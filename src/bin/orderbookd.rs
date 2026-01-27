use std::env;
use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use prost::Message;

use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};

use aws_sdk_s3::primitives::ByteStream;

use hello_rust::engine::order_book::{
    DoneEvent, DoneReason, Event, LevelSnapshot, MarketPhase, Order, OrderBookSnapshot,
    OrderSnapshot, Side, TimeInForce,
};
use hello_rust::markets::{MarketSnapshot, Markets, MarketsSnapshot};
use hello_rust::proto::orderbook as pb;

struct AppState {
    markets: Markets,
    last_sequence: u64,
}

impl AppState {
    fn new() -> Self {
        Self { markets: Markets::new(), last_sequence: 0 }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let stream_name = env::var("JETSTREAM_STREAM").unwrap_or_else(|_| "ORDERBOOK".to_string());
    let orders_subject = env::var("ORDERS_SUBJECT").unwrap_or_else(|_| "orders".to_string());
    let events_subject = env::var("EVENTS_SUBJECT").unwrap_or_else(|_| "events".to_string());
    let deliver_subject =
        env::var("ORDERS_DELIVER_SUBJECT").unwrap_or_else(|_| "orders.deliver".to_string());
    let consumer_name = env::var("ORDERS_CONSUMER").unwrap_or_else(|_| "orderbook".to_string());
    let snapshot_bucket = env::var("SNAPSHOT_BUCKET").ok();
    let snapshot_prefix = env::var("SNAPSHOT_PREFIX").unwrap_or_else(|_| "snapshots".to_string());
    let snapshot_key = env::var("SNAPSHOT_KEY").ok();

    let client = async_nats::connect(nats_url).await?;
    let jetstream = async_nats::jetstream::new(client);

    let s3_client = if snapshot_bucket.is_some() {
        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Some(aws_sdk_s3::Client::new(&aws_config))
    } else {
        None
    };

    let mut state = match (s3_client.as_ref(), snapshot_bucket.as_deref()) {
        (Some(client), Some(bucket)) => {
            load_snapshot(
                client,
                bucket,
                &snapshot_prefix,
                &stream_name,
                snapshot_key.as_deref(),
            )
            .await?
        }
        _ => AppState::new(),
    };

    let stream = jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![orders_subject.clone(), events_subject.clone()],
            ..Default::default()
        })
        .await?;

    let deliver_policy = if state.last_sequence > 0 {
        DeliverPolicy::ByStartSequence {
            start_sequence: state.last_sequence + 1,
        }
    } else {
        DeliverPolicy::All
    };

    // Keep a durable consumer when snapshots are disabled so restarts don't replay the stream.
    let use_durable = snapshot_bucket.is_none();
    let consumer = if use_durable {
        stream
            .get_or_create_consumer(
                &consumer_name,
                jetstream::consumer::push::Config {
                    durable_name: Some(consumer_name.clone()),
                    deliver_subject: deliver_subject.clone(),
                    deliver_policy: deliver_policy.clone(),
                    ack_policy: AckPolicy::Explicit,
                    filter_subject: orders_subject.clone(),
                    ack_wait: Duration::from_secs(30),
                    ..Default::default()
                },
            )
            .await?
    } else {
        stream
            .create_consumer(jetstream::consumer::push::Config {
                name: Some(format!("{}-{}", consumer_name, state.last_sequence)),
                durable_name: None,
                deliver_subject,
                deliver_policy,
                ack_policy: AckPolicy::Explicit,
                filter_subject: orders_subject.clone(),
                ack_wait: Duration::from_secs(30),
                ..Default::default()
            })
            .await?
    };

    let mut messages = consumer.messages().await?;
    let mut shutdown = Box::pin(shutdown_signal());

    println!("listening for orders on {}", orders_subject);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            message = messages.next() => {
                let message = match message {
                    Some(Ok(msg)) => msg,
                    Some(Err(err)) => {
                        eprintln!("jetstream message error: {err}");
                        continue;
                    }
                    None => break,
                };

                let seq = message.info().map(|info| info.stream_sequence).unwrap_or(0);
                let payload = message.message.payload.clone();
                let command = match pb::OrderCommand::decode(payload.as_ref()) {
                    Ok(cmd) => cmd,
                    Err(err) => {
                        eprintln!("invalid order payload: {err}");
                        message.ack().await?;
                        continue;
                    }
                };

                let events = match apply_command(&mut state.markets, command) {
                    Ok(ev) => ev,
                    Err(err) => {
                        eprintln!("invalid order command: {err}");
                        message.ack().await?;
                        continue;
                    }
                };

                publish_events(&jetstream, &events_subject, events).await?;
                message.ack().await?;
                if seq > 0 {
                    state.last_sequence = seq;
                }
            }
        }
    }

    if let (Some(client), Some(bucket)) = (s3_client.as_ref(), snapshot_bucket.as_deref()) {
        let key = save_snapshot(client, bucket, &snapshot_prefix, &stream_name, &state).await?;
        println!("snapshot saved to s3://{}/{}", bucket, key);
    }

    Ok(())
}

fn apply_command(markets: &mut Markets, command: pb::OrderCommand) -> Result<Vec<Event>, String> {
    match command.cmd {
        Some(pb::order_command::Cmd::PlaceLimit(place)) => {
            let side = match side_from_proto(place.side) {
                Some(side) => side,
                None => return Ok(reject(place.id)),
            };
            if place.market_id.is_empty() {
                return Ok(reject(place.id));
            }
            let tif = match time_in_force_from_proto(place.time_in_force) {
                Ok(tif) => tif,
                Err(_) => return Ok(reject(place.id)),
            };
            Ok(markets
                .book_mut(&place.market_id)
                .place_limit_with_tif(
                    Order::new(place.id, place.owner_id, side, place.price, place.qty),
                    tif,
                ))
        }
        Some(pb::order_command::Cmd::PlaceMarket(place)) => {
            let side = match side_from_proto(place.side) {
                Some(side) => side,
                None => return Ok(reject(place.id)),
            };
            if place.market_id.is_empty() {
                return Ok(reject(place.id));
            }
            Ok(markets
                .book_mut(&place.market_id)
                .place_market(Order::market(place.id, place.owner_id, side, place.qty)))
        }
        Some(pb::order_command::Cmd::Cancel(cancel)) => {
            if cancel.market_id.is_empty() {
                return Ok(vec![Event::Done(DoneEvent {
                    order_id: cancel.id,
                    reason: DoneReason::Rejected,
                })]);
            }
            if cancel.qty == 0 {
                Ok(markets.book_mut(&cancel.market_id).cancel(cancel.id))
            } else {
                Ok(markets
                    .book_mut(&cancel.market_id)
                    .cancel_qty(cancel.id, cancel.qty))
            }
        }
        Some(pb::order_command::Cmd::Amend(amend)) => {
            if amend.market_id.is_empty() {
                return Ok(reject(amend.id));
            }
            if amend.price.is_none() && amend.qty.is_none() {
                return Ok(reject(amend.id));
            }
            Ok(markets
                .book_mut(&amend.market_id)
                .amend(amend.id, amend.price, amend.qty))
        }
        Some(pb::order_command::Cmd::SetMarketPhase(set_phase)) => {
            if set_phase.market_id.is_empty() {
                return Ok(vec![]);
            }
            let phase = market_phase_from_proto(set_phase.phase);
            markets.book_mut(&set_phase.market_id).set_phase(phase);
            Ok(vec![])
        }
        Some(pb::order_command::Cmd::RunAuction(run)) => {
            if run.market_id.is_empty() {
                return Ok(vec![]);
            }
            let next_phase = market_phase_from_proto(run.next_phase);
            Ok(markets.book_mut(&run.market_id).run_auction(next_phase))
        }
        None => Err("missing command".to_string()),
    }
}

fn reject(order_id: u64) -> Vec<Event> {
    vec![Event::Done(DoneEvent {
        order_id,
        reason: DoneReason::Rejected,
    })]
}

fn side_from_proto(side: i32) -> Option<Side> {
    match pb::Side::try_from(side) {
        Ok(pb::Side::Bid) => Some(Side::Bid),
        Ok(pb::Side::Ask) => Some(Side::Ask),
        Ok(pb::Side::Unspecified) | Err(_) => None,
    }
}

fn time_in_force_from_proto(value: i32) -> Result<TimeInForce, String> {
    match pb::TimeInForce::try_from(value) {
        Ok(pb::TimeInForce::Unspecified) | Ok(pb::TimeInForce::Gtc) => Ok(TimeInForce::Gtc),
        Ok(pb::TimeInForce::Ioc) => Ok(TimeInForce::Ioc),
        Ok(pb::TimeInForce::Fok) => Ok(TimeInForce::Fok),
        Ok(pb::TimeInForce::PostOnly) => Ok(TimeInForce::PostOnly),
        Err(_) => Err("invalid time_in_force".to_string()),
    }
}

fn time_in_force_to_proto(tif: TimeInForce) -> i32 {
    match tif {
        TimeInForce::Gtc => pb::TimeInForce::Gtc as i32,
        TimeInForce::Ioc => pb::TimeInForce::Ioc as i32,
        TimeInForce::Fok => pb::TimeInForce::Fok as i32,
        TimeInForce::PostOnly => pb::TimeInForce::PostOnly as i32,
    }
}

fn time_in_force_from_snapshot(value: i32) -> TimeInForce {
    match pb::TimeInForce::try_from(value) {
        Ok(pb::TimeInForce::PostOnly) => TimeInForce::PostOnly,
        Ok(pb::TimeInForce::Ioc) => TimeInForce::Ioc,
        Ok(pb::TimeInForce::Fok) => TimeInForce::Fok,
        Ok(pb::TimeInForce::Gtc) | Ok(pb::TimeInForce::Unspecified) | Err(_) => TimeInForce::Gtc,
    }
}

fn market_phase_from_proto(value: i32) -> MarketPhase {
    match pb::MarketPhase::try_from(value) {
        Ok(pb::MarketPhase::Auction) => MarketPhase::Auction,
        Ok(pb::MarketPhase::Closed) => MarketPhase::Closed,
        Ok(pb::MarketPhase::Open) | Ok(pb::MarketPhase::Unspecified) | Err(_) => MarketPhase::Open,
    }
}

fn market_phase_to_proto(phase: MarketPhase) -> i32 {
    match phase {
        MarketPhase::Open => pb::MarketPhase::Open as i32,
        MarketPhase::Auction => pb::MarketPhase::Auction as i32,
        MarketPhase::Closed => pb::MarketPhase::Closed as i32,
    }
}

fn event_to_proto(event: Event) -> pb::Event {
    match event {
        Event::Trade(t) => pb::Event {
            kind: Some(pb::event::Kind::Trade(pb::TradeEvent {
                taker_id: t.taker_id,
                maker_id: t.maker_id,
                price: t.price,
                qty: t.qty,
            })),
        },
        Event::Done(d) => {
            let reason = match d.reason {
                DoneReason::Filled => pb::DoneReason::Filled as i32,
                DoneReason::Canceled => pb::DoneReason::Canceled as i32,
                DoneReason::Rejected => pb::DoneReason::Rejected as i32,
            };
            pb::Event {
                kind: Some(pb::event::Kind::Done(pb::DoneEvent {
                    order_id: d.order_id,
                    reason,
                })),
            }
        }
        Event::Ack(a) => pb::Event {
            kind: Some(pb::event::Kind::Ack(pb::AckEvent {
                order_id: a.order_id,
            })),
        },
        Event::Update(u) => pb::Event {
            kind: Some(pb::event::Kind::Update(pb::UpdateEvent {
                order_id: u.order_id,
                price: u.price,
                qty_remaining: u.qty_remaining,
            })),
        },
    }
}

async fn publish_events(
    jetstream: &jetstream::Context,
    subject: &str,
    events: Vec<Event>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let subject = subject.to_string();
    for event in events {
        let payload = event_to_proto(event).encode_to_vec();
        jetstream
            .publish(subject.clone(), payload.into())
            .await?
            .await?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            eprintln!("ctrl-c handler error: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => {
                eprintln!("signal handler error: {err}");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn load_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    stream_name: &str,
    key_override: Option<&str>,
) -> Result<AppState, Box<dyn Error + Send + Sync>> {
    let key = if let Some(key) = key_override {
        key.to_string()
    } else {
        match latest_snapshot_key(client, bucket, prefix, stream_name).await? {
            Some(key) => key,
            None => return Ok(AppState::new()),
        }
    };

    let obj = client.get_object().bucket(bucket).key(&key).send().await?;
    let data = obj.body.collect().await?;
    let bytes = data.into_bytes();
    let snapshot = pb::StateSnapshot::decode(bytes.as_ref())?;
    let state = state_from_proto(snapshot)?;
    println!("loaded snapshot from s3://{}/{}", bucket, key);
    Ok(state)
}

async fn save_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    stream_name: &str,
    state: &AppState,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let snapshot = state_to_proto(state);
    let payload = snapshot.encode_to_vec();
    let key = snapshot_key(prefix, stream_name, state.last_sequence);
    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(payload))
        .send()
        .await?;
    Ok(key)
}

async fn latest_snapshot_key(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    stream_name: &str,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    let prefix = format!(
        "{}/{}/",
        prefix.trim_end_matches('/'),
        stream_name
    );
    let mut best: Option<(u64, String)> = None;
    let mut continuation: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(&prefix);
        if let Some(token) = continuation {
            req = req.continuation_token(token);
        }
        let resp = req.send().await?;
        if let Some(objects) = resp.contents {
            for obj in objects {
                let Some(key) = obj.key else { continue };
                if let Some(seq) = parse_snapshot_sequence(&key) {
                    match best {
                        Some((best_seq, _)) if best_seq >= seq => {}
                        _ => best = Some((seq, key)),
                    }
                }
            }
        }
        if resp.is_truncated.unwrap_or(false) {
            continuation = resp.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    Ok(best.map(|(_, key)| key))
}

fn snapshot_key(prefix: &str, stream_name: &str, sequence: u64) -> String {
    let prefix = prefix.trim_end_matches('/');
    format!("{}/{}/seq_{:020}.bin", prefix, stream_name, sequence)
}

fn parse_snapshot_sequence(key: &str) -> Option<u64> {
    let file = key.rsplit('/').next()?;
    if !file.starts_with("seq_") || !file.ends_with(".bin") {
        return None;
    }
    let number = &file[4..file.len() - 4];
    number.parse::<u64>().ok()
}

fn state_to_proto(state: &AppState) -> pb::StateSnapshot {
    let snapshot = state.markets.snapshot();
    pb::StateSnapshot {
        last_sequence: state.last_sequence,
        markets: snapshot
            .markets
            .into_iter()
            .map(market_snapshot_to_proto)
            .collect(),
    }
}

fn state_from_proto(snapshot: pb::StateSnapshot) -> Result<AppState, Box<dyn Error + Send + Sync>> {
    let last_sequence = snapshot.last_sequence;
    let markets = snapshot
        .markets
        .into_iter()
        .map(market_snapshot_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = MarketsSnapshot { markets };
    Ok(AppState {
        markets: Markets::from_snapshot(snapshot),
        last_sequence,
    })
}

fn market_snapshot_to_proto(snapshot: MarketSnapshot) -> pb::MarketSnapshot {
    pb::MarketSnapshot {
        market_id: snapshot.market_id,
        book: Some(order_book_snapshot_to_proto(snapshot.book)),
    }
}

fn market_snapshot_from_proto(
    snapshot: pb::MarketSnapshot,
) -> Result<MarketSnapshot, Box<dyn Error + Send + Sync>> {
    let book = snapshot
        .book
        .ok_or_else(|| "missing order book snapshot".to_string())?;
    Ok(MarketSnapshot {
        market_id: snapshot.market_id,
        book: order_book_snapshot_from_proto(book)?,
    })
}

fn order_book_snapshot_to_proto(snapshot: OrderBookSnapshot) -> pb::OrderBookSnapshot {
    pb::OrderBookSnapshot {
        orders: snapshot
            .orders
            .into_iter()
            .map(order_snapshot_to_proto)
            .collect(),
        bids: snapshot
            .bids
            .into_iter()
            .map(level_snapshot_to_proto)
            .collect(),
        asks: snapshot
            .asks
            .into_iter()
            .map(level_snapshot_to_proto)
            .collect(),
        phase: market_phase_to_proto(snapshot.phase),
    }
}

fn order_book_snapshot_from_proto(
    snapshot: pb::OrderBookSnapshot,
) -> Result<OrderBookSnapshot, Box<dyn Error + Send + Sync>> {
    let orders = snapshot
        .orders
        .into_iter()
        .map(order_snapshot_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let bids = snapshot
        .bids
        .into_iter()
        .map(level_snapshot_from_proto)
        .collect();
    let asks = snapshot
        .asks
        .into_iter()
        .map(level_snapshot_from_proto)
        .collect();

    Ok(OrderBookSnapshot {
        orders,
        bids,
        asks,
        phase: market_phase_from_proto(snapshot.phase),
    })
}

fn order_snapshot_to_proto(snapshot: OrderSnapshot) -> pb::OrderSnapshot {
    let side = match snapshot.side {
        Side::Bid => pb::Side::Bid as i32,
        Side::Ask => pb::Side::Ask as i32,
    };
    pb::OrderSnapshot {
        id: snapshot.id,
        price: snapshot.price,
        qty_remaining: snapshot.qty_remaining,
        side,
        live: snapshot.live,
        time_in_force: time_in_force_to_proto(snapshot.time_in_force),
        owner_id: snapshot.owner_id,
    }
}

fn order_snapshot_from_proto(
    snapshot: pb::OrderSnapshot,
) -> Result<OrderSnapshot, Box<dyn Error + Send + Sync>> {
    let side = match pb::Side::try_from(snapshot.side) {
        Ok(pb::Side::Bid) => Side::Bid,
        Ok(pb::Side::Ask) => Side::Ask,
        Ok(pb::Side::Unspecified) | Err(_) => {
            return Err("invalid side in snapshot".into());
        }
    };
    Ok(OrderSnapshot {
        id: snapshot.id,
        price: snapshot.price,
        qty_remaining: snapshot.qty_remaining,
        side,
        live: snapshot.live,
        time_in_force: time_in_force_from_snapshot(snapshot.time_in_force),
        owner_id: snapshot.owner_id,
    })
}

fn level_snapshot_to_proto(snapshot: LevelSnapshot) -> pb::LevelSnapshot {
    pb::LevelSnapshot {
        price: snapshot.price,
        order_ids: snapshot.order_ids,
    }
}

fn level_snapshot_from_proto(snapshot: pb::LevelSnapshot) -> LevelSnapshot {
    LevelSnapshot {
        price: snapshot.price,
        order_ids: snapshot.order_ids,
    }
}
