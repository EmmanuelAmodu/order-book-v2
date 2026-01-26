use std::collections::{BTreeMap, VecDeque, HashMap};

pub const TICK_SIZE: f64 = 0.000001;

pub type OrderId = u64;
pub type PriceTick = u32; // integer ticks
pub type Qty = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Bid, Ask }

#[derive(Debug, Clone, Copy)]
pub struct Order {
    id: OrderId,
    price: PriceTick,
    qty_remaining: Qty,
    side: Side,
    live: bool, // tombstone cancel
}

impl Order {
    pub fn new(id: OrderId, side: Side, price: PriceTick, qty: Qty) -> Self {
        Self { id, side, price, qty_remaining: qty, live: true }
    }

    pub fn id(&self) -> OrderId {
        self.id
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn price(&self) -> PriceTick {
        self.price
    }

    pub fn qty_remaining(&self) -> Qty {
        self.qty_remaining
    }

    pub fn live(&self) -> bool {
        self.live
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneReason { Filled, Canceled, Rejected }

#[derive(Debug, Clone, Copy)]
pub struct TradeEvent {
    pub taker_id: OrderId,
    pub maker_id: OrderId,
    pub price: PriceTick,
    pub qty: Qty,
}

#[derive(Debug, Clone, Copy)]
pub struct DoneEvent { pub order_id: OrderId, pub reason: DoneReason }

#[derive(Debug, Clone, Copy)]
pub struct AckEvent { pub order_id: OrderId }

#[derive(Debug, Clone, Copy)]
pub enum Event {
    Trade(TradeEvent),
    Done(DoneEvent),
    Ack(AckEvent),
}

#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub id: OrderId,
    pub price: PriceTick,
    pub qty_remaining: Qty,
    pub side: Side,
    pub live: bool,
}

#[derive(Debug, Clone)]
pub struct LevelSnapshot {
    pub price: PriceTick,
    pub order_ids: Vec<OrderId>,
}

#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub orders: Vec<OrderSnapshot>,
    pub bids: Vec<LevelSnapshot>,
    pub asks: Vec<LevelSnapshot>,
}

pub struct OrderBook {
    bids: BTreeMap<PriceTick, VecDeque<OrderId>>,
    asks: BTreeMap<PriceTick, VecDeque<OrderId>>,
    orders: HashMap<OrderId, Order>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), orders: HashMap::new() }
    }

    #[inline]
    pub fn best_bid_price(&self) -> Option<PriceTick> {
        self.bids.last_key_value().map(|(p, _)| *p)
    }

    #[inline]
    pub fn best_ask_price(&self) -> Option<PriceTick> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    pub fn order(&self, id: OrderId) -> Option<Order> {
        self.orders.get(&id).copied()
    }

    pub fn level_front(&self, side: Side, price: PriceTick) -> Option<OrderId> {
        let book = match side { Side::Bid => &self.bids, Side::Ask => &self.asks };
        book.get(&price).and_then(|level| level.front().copied())
    }

    pub fn level_exists(&self, side: Side, price: PriceTick) -> bool {
        let book = match side { Side::Bid => &self.bids, Side::Ask => &self.asks };
        book.get(&price).map(|level| !level.is_empty()).unwrap_or(false)
    }

    pub fn book_is_empty(&self, side: Side) -> bool {
        match side {
            Side::Bid => self.bids.is_empty(),
            Side::Ask => self.asks.is_empty(),
        }
    }

    pub fn snapshot(&self) -> OrderBookSnapshot {
        let orders = self
            .orders
            .values()
            .map(|o| OrderSnapshot {
                id: o.id,
                price: o.price,
                qty_remaining: o.qty_remaining,
                side: o.side,
                live: o.live,
            })
            .collect();

        let bids = self
            .bids
            .iter()
            .map(|(price, level)| LevelSnapshot {
                price: *price,
                order_ids: level.iter().copied().collect(),
            })
            .collect();

        let asks = self
            .asks
            .iter()
            .map(|(price, level)| LevelSnapshot {
                price: *price,
                order_ids: level.iter().copied().collect(),
            })
            .collect();

        OrderBookSnapshot { orders, bids, asks }
    }

    pub fn from_snapshot(snapshot: OrderBookSnapshot) -> Self {
        let orders = snapshot
            .orders
            .into_iter()
            .map(|o| {
                (
                    o.id,
                    Order {
                        id: o.id,
                        price: o.price,
                        qty_remaining: o.qty_remaining,
                        side: o.side,
                        live: o.live,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        let bids = snapshot
            .bids
            .into_iter()
            .map(|level| (level.price, VecDeque::from(level.order_ids)))
            .collect::<BTreeMap<_, _>>();

        let asks = snapshot
            .asks
            .into_iter()
            .map(|level| (level.price, VecDeque::from(level.order_ids)))
            .collect::<BTreeMap<_, _>>();

        Self { bids, asks, orders }
    }

    // Minimal validation (crossing is OK)
    fn validate_order(&self, o: &Order) -> Result<(), DoneReason> {
        if !o.live { return Err(DoneReason::Rejected); }
        if o.qty_remaining == 0 { return Err(DoneReason::Rejected); }
        // price bounds checks happen before conversion to ticks in a real system
        Ok(())
    }

    pub fn cancel(&mut self, id: OrderId) -> Vec<Event> {
        let mut ev = Vec::new();
        if let Some(o) = self.orders.get_mut(&id) {
            if o.live && o.qty_remaining > 0 {
                o.live = false;
                let (price, side) = (o.price, o.side);
                let book = match side {
                    Side::Bid => &mut self.bids,
                    Side::Ask => &mut self.asks,
                };
                if let Some(level) = book.get_mut(&price) {
                    level.retain(|&order_id| order_id != id);
                    if level.is_empty() {
                        book.remove(&price);
                    }
                }
                ev.push(Event::Done(DoneEvent { order_id: id, reason: DoneReason::Canceled }));
            }
        }
        ev
    }

    pub fn place_limit(&mut self, order: Order) -> Vec<Event> {
        let mut events = Vec::new();

        if self.orders.contains_key(&order.id) {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        if self.validate_order(&order).is_err() {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        // Insert into global store early so maker/taker updates are easy
        self.orders.insert(order.id, order);

        // Match as taker
        events.extend(self.match_order(order.id));

        // If still live with remaining qty, rest it
        let rest_qty = self.orders.get(&order.id).map(|o| (o.live, o.qty_remaining, o.price, o.side));
        if let Some((live, qty_remaining, price, side)) = rest_qty {
            if live && qty_remaining > 0 {
                let book = match side { Side::Bid => &mut self.bids, Side::Ask => &mut self.asks };
                book.entry(price).or_insert_with(VecDeque::new).push_back(order.id);
            } else {
                // fully filled inside match_order likely emitted Done(Filled)
            }
        }

        events.push(Event::Ack(AckEvent { order_id: order.id }));
        events
    }

    fn match_order(&mut self, taker_id: OrderId) -> Vec<Event> {
        let mut events = Vec::new();

        loop {
            // Pull taker snapshot (avoid long borrows)
            let (taker_side, taker_price, taker_live, taker_qty) = match self.orders.get(&taker_id) {
                Some(o) => (o.side, o.price, o.live, o.qty_remaining),
                None => break,
            };
            if !taker_live || taker_qty == 0 { break; }

            // Determine best opposing price and whether it crosses
            let best_opp = match taker_side {
                Side::Bid => self.best_ask_price(),
                Side::Ask => self.best_bid_price(),
            };
            let opp_price = match best_opp {
                Some(p) => p,
                None => break,
            };

            let crosses = match taker_side {
                Side::Bid => opp_price <= taker_price,
                Side::Ask => opp_price >= taker_price,
            };
            if !crosses { break; }

            // Peek a maker id from the opposing best level (skip dead via tombstones)
            let maker_id_opt = {
                let opp_book = match taker_side { Side::Bid => &mut self.asks, Side::Ask => &mut self.bids };
                let level = match opp_book.get_mut(&opp_price) {
                    Some(l) => l,
                    None => continue,
                };

                // Clean dead makers at the front
                while let Some(mid) = level.front().copied() {
                    let live = self.orders.get(&mid).map(|o| o.live && o.qty_remaining > 0).unwrap_or(false);
                    if live { break; }
                    level.pop_front();
                }

                level.front().copied()
            };

            // Remove empty level (do it in a separate short borrow)
            if maker_id_opt.is_none() {
                let opp_book = match taker_side { Side::Bid => &mut self.asks, Side::Ask => &mut self.bids };
                if let Some(level) = opp_book.get(&opp_price) {
                    if level.is_empty() {
                        opp_book.remove(&opp_price);
                    }
                }
                continue; // level was empty or all dead
            }

            let maker_id = maker_id_opt.unwrap();

            // Now do the actual fill
            let (maker_price, fill_qty) = {
                let taker = self.orders.get(&taker_id).unwrap();
                let maker = self.orders.get(&maker_id).unwrap();
                let qty = taker.qty_remaining.min(maker.qty_remaining);
                (maker.price, qty)
            };

            // Apply updates
            let (taker_done, maker_done) = {
                // Update taker
                let taker_done = {
                    let taker = self.orders.get_mut(&taker_id).unwrap();
                    taker.qty_remaining -= fill_qty;
                    if taker.qty_remaining == 0 {
                        taker.live = false;
                        true
                    } else {
                        false
                    }
                };

                // Update maker
                let maker_done = {
                    let maker = self.orders.get_mut(&maker_id).unwrap();
                    maker.qty_remaining -= fill_qty;
                    if maker.qty_remaining == 0 {
                        maker.live = false;
                        true
                    } else {
                        false
                    }
                };

                (taker_done, maker_done)
            };

            events.push(Event::Trade(TradeEvent {
                taker_id,
                maker_id,
                price: maker_price, // resting price
                qty: fill_qty,
            }));

            if maker_done {
                events.push(Event::Done(DoneEvent { order_id: maker_id, reason: DoneReason::Filled }));
                let opp_book = match taker_side { Side::Bid => &mut self.asks, Side::Ask => &mut self.bids };
                if let Some(level) = opp_book.get_mut(&opp_price) {
                    if level.front().copied() == Some(maker_id) {
                        level.pop_front();
                    }
                    if level.is_empty() {
                        opp_book.remove(&opp_price);
                    }
                }
            }
            if taker_done {
                events.push(Event::Done(DoneEvent { order_id: taker_id, reason: DoneReason::Filled }));
                break;
            }
        }

        events
    }
}
