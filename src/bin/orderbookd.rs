use std::env;
use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use prost::Message;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;

use hello_rust::markets::Markets;
use hello_rust::engine::order_book::{DoneEvent, DoneReason, Event, Order, Side};
use hello_rust::proto::orderbook as pb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let stream_name = env::var("JETSTREAM_STREAM").unwrap_or_else(|_| "ORDERBOOK".to_string());
    let orders_subject = env::var("ORDERS_SUBJECT").unwrap_or_else(|_| "orders".to_string());
    let events_subject = env::var("EVENTS_SUBJECT").unwrap_or_else(|_| "events".to_string());
    let deliver_subject =
        env::var("ORDERS_DELIVER_SUBJECT").unwrap_or_else(|_| "orders.deliver".to_string());
    let consumer_name = env::var("ORDERS_CONSUMER").unwrap_or_else(|_| "orderbook".to_string());

    let client = async_nats::connect(nats_url).await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream = jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![orders_subject.clone(), events_subject.clone()],
            max_messages: 100_000,
            ..Default::default()
        })
        .await?;

    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            jetstream::consumer::push::Config {
                durable_name: Some(consumer_name.clone()),
                deliver_subject,
                ack_policy: AckPolicy::Explicit,
                filter_subject: orders_subject.clone(),
                ack_wait: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .await?;

    let mut messages = consumer.messages().await?;
    let mut markets = Markets::new();

    println!("listening for orders on {}", orders_subject);
    while let Some(message) = messages.next().await {
        let message = match message {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("jetstream message error: {err}");
                continue;
            }
        };

        let payload = message.message.payload.clone();
        let command = match pb::OrderCommand::decode(payload.as_ref()) {
            Ok(cmd) => cmd,
            Err(err) => {
                eprintln!("invalid order payload: {err}");
                message.ack().await?;
                continue;
            }
        };

        let events = match apply_command(&mut markets, command) {
            Ok(ev) => ev,
            Err(err) => {
                eprintln!("invalid order command: {err}");
                message.ack().await?;
                continue;
            }
        };

        publish_events(&jetstream, &events_subject, events).await?;
        message.ack().await?;
    }

    Ok(())
}

fn apply_command(markets: &mut Markets, command: pb::OrderCommand) -> Result<Vec<Event>, String> {
    match command.cmd {
        Some(pb::order_command::Cmd::PlaceLimit(place)) => {
            let side = match pb::Side::try_from(place.side) {
                Ok(pb::Side::Bid) => Side::Bid,
                Ok(pb::Side::Ask) => Side::Ask,
                Ok(pb::Side::Unspecified) | Err(_) => {
                    return Ok(vec![Event::Done(DoneEvent {
                        order_id: place.id,
                        reason: DoneReason::Rejected,
                    })]);
                }
            };
            if place.market_id.is_empty() {
                return Ok(vec![Event::Done(DoneEvent {
                    order_id: place.id,
                    reason: DoneReason::Rejected,
                })]);
            }
            Ok(markets
                .book_mut(&place.market_id)
                .place_limit(Order::new(place.id, side, place.price, place.qty)))
        }
        Some(pb::order_command::Cmd::Cancel(cancel)) => {
            if cancel.market_id.is_empty() {
                return Ok(vec![Event::Done(DoneEvent {
                    order_id: cancel.id,
                    reason: DoneReason::Rejected,
                })]);
            }
            Ok(markets.book_mut(&cancel.market_id).cancel(cancel.id))
        }
        None => Err("missing command".to_string()),
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
