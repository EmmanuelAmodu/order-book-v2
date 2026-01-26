use std::collections::{BTreeMap, VecDeque, HashMap};

pub const TICK_SIZE: f64 = 0.000001;

pub type OrderId = u64;
pub type PriceTick = u32; // integer ticks
pub type Qty = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Bid, Ask }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce { Gtc, Ioc, Fok, PostOnly }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketPhase { Open, Auction, Closed }

#[derive(Debug, Clone, Copy)]
pub struct Order {
    id: OrderId,
    price: PriceTick,
    qty_remaining: Qty,
    side: Side,
    live: bool, // tombstone cancel
    tif: TimeInForce,
}

impl Order {
    pub fn new(id: OrderId, side: Side, price: PriceTick, qty: Qty) -> Self {
        Self { id, side, price, qty_remaining: qty, live: true, tif: TimeInForce::Gtc }
    }

    pub fn market(id: OrderId, side: Side, qty: Qty) -> Self {
        let price = match side {
            Side::Bid => PriceTick::MAX,
            Side::Ask => 0,
        };
        Self { id, side, price, qty_remaining: qty, live: true, tif: TimeInForce::Ioc }
    }

    pub fn with_tif(mut self, tif: TimeInForce) -> Self {
        self.tif = tif;
        self
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

    pub fn time_in_force(&self) -> TimeInForce {
        self.tif
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
pub struct UpdateEvent {
    pub order_id: OrderId,
    pub price: PriceTick,
    pub qty_remaining: Qty,
}

#[derive(Debug, Clone, Copy)]
pub enum Event {
    Trade(TradeEvent),
    Done(DoneEvent),
    Ack(AckEvent),
    Update(UpdateEvent),
}

#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub id: OrderId,
    pub price: PriceTick,
    pub qty_remaining: Qty,
    pub side: Side,
    pub live: bool,
    pub time_in_force: TimeInForce,
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
    pub phase: MarketPhase,
}

pub struct OrderBook {
    bids: BTreeMap<PriceTick, VecDeque<OrderId>>,
    asks: BTreeMap<PriceTick, VecDeque<OrderId>>,
    orders: HashMap<OrderId, Order>,
    phase: MarketPhase,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            phase: MarketPhase::Open,
        }
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
                time_in_force: o.tif,
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

        OrderBookSnapshot { orders, bids, asks, phase: self.phase }
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
                        tif: o.time_in_force,
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

        Self { bids, asks, orders, phase: snapshot.phase }
    }

    // Minimal validation (crossing is OK)
    fn validate_order(&self, o: &Order) -> Result<(), DoneReason> {
        if !o.live { return Err(DoneReason::Rejected); }
        if o.qty_remaining == 0 { return Err(DoneReason::Rejected); }
        // price bounds checks happen before conversion to ticks in a real system
        Ok(())
    }

    pub fn cancel(&mut self, id: OrderId) -> Vec<Event> {
        self.cancel_qty(id, 0)
    }

    pub fn cancel_qty(&mut self, id: OrderId, qty: Qty) -> Vec<Event> {
        let mut ev = Vec::new();
        if let Some(o) = self.orders.get_mut(&id) {
            if o.live && o.qty_remaining > 0 {
                if qty == 0 || qty >= o.qty_remaining {
                    o.live = false;
                    let (price, side) = (o.price, o.side);
                    self.remove_from_level(id, price, side);
                    ev.push(Event::Done(DoneEvent { order_id: id, reason: DoneReason::Canceled }));
                } else {
                    o.qty_remaining -= qty;
                    ev.push(Event::Update(UpdateEvent {
                        order_id: id,
                        price: o.price,
                        qty_remaining: o.qty_remaining,
                    }));
                }
            }
        }
        ev
    }

    pub fn place_limit(&mut self, order: Order) -> Vec<Event> {
        self.place_limit_with_tif(order, TimeInForce::Gtc)
    }

    pub fn place_market(&mut self, order: Order) -> Vec<Event> {
        self.place_limit_with_tif(order, TimeInForce::Ioc)
    }

    pub fn place_limit_with_tif(&mut self, order: Order, tif: TimeInForce) -> Vec<Event> {
        let mut events = Vec::new();

        if self.orders.contains_key(&order.id) {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        if self.validate_order(&order).is_err() {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        let order = order.with_tif(tif);

        if self.phase == MarketPhase::Closed {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        if self.phase == MarketPhase::Auction {
            if matches!(tif, TimeInForce::Ioc | TimeInForce::Fok) {
                events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
                return events;
            }
        }

        if tif == TimeInForce::PostOnly && self.would_cross(&order) {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        if tif == TimeInForce::Fok && !self.can_fully_fill(&order) {
            events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Rejected }));
            return events;
        }

        // Insert into global store early so maker/taker updates are easy
        self.orders.insert(order.id, order);

        // Match as taker in continuous mode
        if self.phase == MarketPhase::Open {
            events.extend(self.match_order(order.id));
        }

        // If still live with remaining qty, rest it
        let rest_qty = self.orders.get(&order.id).map(|o| (o.live, o.qty_remaining, o.price, o.side));
        if let Some((live, qty_remaining, price, side)) = rest_qty {
            if live && qty_remaining > 0 {
                match tif {
                    TimeInForce::Gtc | TimeInForce::PostOnly => {
                        let book = match side { Side::Bid => &mut self.bids, Side::Ask => &mut self.asks };
                        book.entry(price).or_insert_with(VecDeque::new).push_back(order.id);
                    }
                    TimeInForce::Ioc | TimeInForce::Fok => {
                        let order = self.orders.get_mut(&order.id).unwrap();
                        order.live = false;
                        events.push(Event::Done(DoneEvent { order_id: order.id, reason: DoneReason::Canceled }));
                    }
                }
            } else {
                // fully filled inside match_order likely emitted Done(Filled)
            }
        }

        events.push(Event::Ack(AckEvent { order_id: order.id }));
        events
    }

    pub fn amend(
        &mut self,
        id: OrderId,
        new_price: Option<PriceTick>,
        new_qty: Option<Qty>,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        let (current_price, current_qty, side, tif) = match self.orders.get(&id) {
            Some(o) if o.live && o.qty_remaining > 0 => (o.price, o.qty_remaining, o.side, o.tif),
            _ => {
                events.push(Event::Done(DoneEvent { order_id: id, reason: DoneReason::Rejected }));
                return events;
            }
        };

        let price = new_price.unwrap_or(current_price);
        let qty = new_qty.unwrap_or(current_qty);

        if qty == 0 {
            return self.cancel(id);
        }

        if tif == TimeInForce::PostOnly && self.would_cross_at(side, price) {
            events.push(Event::Done(DoneEvent { order_id: id, reason: DoneReason::Rejected }));
            return events;
        }

        let price_changed = price != current_price;
        let qty_increased = qty > current_qty;

        if price_changed || qty_increased {
            self.remove_from_level(id, current_price, side);
        }

        if let Some(o) = self.orders.get_mut(&id) {
            o.price = price;
            o.qty_remaining = qty;
        }

        if self.phase == MarketPhase::Open && price_changed && self.would_cross_at(side, price) {
            events.extend(self.match_order(id));
        }

        let rest = match self.orders.get(&id) {
            Some(o) => o.live && o.qty_remaining > 0,
            None => false,
        };
        if rest && (price_changed || qty_increased) {
            let book = match side { Side::Bid => &mut self.bids, Side::Ask => &mut self.asks };
            book.entry(price).or_insert_with(VecDeque::new).push_back(id);
        }

        if rest && (price_changed || qty != current_qty) {
            if let Some(o) = self.orders.get(&id) {
                events.push(Event::Update(UpdateEvent {
                    order_id: id,
                    price: o.price,
                    qty_remaining: o.qty_remaining,
                }));
            }
        }

        events
    }

    pub fn phase(&self) -> MarketPhase {
        self.phase
    }

    pub fn set_phase(&mut self, phase: MarketPhase) {
        self.phase = phase;
    }

    pub fn run_auction(&mut self, next_phase: MarketPhase) -> Vec<Event> {
        let mut events = Vec::new();
        if self.phase != MarketPhase::Auction {
            return events;
        }

        let Some(clearing_price) = self.auction_clearing_price() else {
            self.phase = next_phase;
            return events;
        };

        events.extend(self.match_at_price(clearing_price));
        self.phase = next_phase;
        events
    }

    fn auction_clearing_price(&self) -> Option<PriceTick> {
        let mut prices: Vec<PriceTick> = self
            .bids
            .keys()
            .chain(self.asks.keys())
            .copied()
            .collect();
        prices.sort_unstable();
        prices.dedup();

        let mut best: Option<(PriceTick, Qty, Qty)> = None;
        for price in prices {
            let bid_qty = self.sum_bids_at_or_above(price);
            let ask_qty = self.sum_asks_at_or_below(price);
            let matched = bid_qty.min(ask_qty);
            if matched == 0 {
                continue;
            }
            let imbalance = if bid_qty > ask_qty {
                bid_qty - ask_qty
            } else {
                ask_qty - bid_qty
            };
            match best {
                None => best = Some((price, matched, imbalance)),
                Some((best_price, best_matched, best_imbalance)) => {
                    if matched > best_matched
                        || (matched == best_matched && imbalance < best_imbalance)
                        || (matched == best_matched && imbalance == best_imbalance && price > best_price)
                    {
                        best = Some((price, matched, imbalance));
                    }
                }
            }
        }

        best.map(|(price, _, _)| price)
    }

    fn sum_bids_at_or_above(&self, price: PriceTick) -> Qty {
        let mut total: Qty = 0;
        for (level_price, level) in self.bids.iter().rev() {
            if *level_price < price {
                break;
            }
            for id in level {
                if let Some(o) = self.orders.get(id) {
                    if o.live && o.qty_remaining > 0 {
                        total += o.qty_remaining;
                    }
                }
            }
        }
        total
    }

    fn sum_asks_at_or_below(&self, price: PriceTick) -> Qty {
        let mut total: Qty = 0;
        for (level_price, level) in &self.asks {
            if *level_price > price {
                break;
            }
            for id in level {
                if let Some(o) = self.orders.get(id) {
                    if o.live && o.qty_remaining > 0 {
                        total += o.qty_remaining;
                    }
                }
            }
        }
        total
    }

    fn match_at_price(&mut self, clearing_price: PriceTick) -> Vec<Event> {
        let mut events = Vec::new();
        let bids = self.auction_order_ids(Side::Bid, clearing_price);
        let asks = self.auction_order_ids(Side::Ask, clearing_price);

        let mut bid_idx = 0;
        let mut ask_idx = 0;
        while bid_idx < bids.len() && ask_idx < asks.len() {
            let bid_id = bids[bid_idx];
            let ask_id = asks[ask_idx];

            let (bid_qty, ask_qty) = {
                let bid = self.orders.get(&bid_id);
                let ask = self.orders.get(&ask_id);
                match (bid, ask) {
                    (Some(b), Some(a)) if b.live && a.live && b.qty_remaining > 0 && a.qty_remaining > 0 => {
                        (b.qty_remaining, a.qty_remaining)
                    }
                    _ => {
                        if bid.is_none() || bid.map(|b| !b.live || b.qty_remaining == 0).unwrap_or(true) {
                            bid_idx += 1;
                        }
                        if ask.is_none() || ask.map(|a| !a.live || a.qty_remaining == 0).unwrap_or(true) {
                            ask_idx += 1;
                        }
                        continue;
                    }
                }
            };

            let fill_qty = bid_qty.min(ask_qty);

            {
                let bid = self.orders.get_mut(&bid_id).unwrap();
                bid.qty_remaining -= fill_qty;
                if bid.qty_remaining == 0 {
                    bid.live = false;
                }
            }
            {
                let ask = self.orders.get_mut(&ask_id).unwrap();
                ask.qty_remaining -= fill_qty;
                if ask.qty_remaining == 0 {
                    ask.live = false;
                }
            }

            events.push(Event::Trade(TradeEvent {
                taker_id: bid_id,
                maker_id: ask_id,
                price: clearing_price,
                qty: fill_qty,
            }));

            if self.orders.get(&bid_id).map(|o| !o.live).unwrap_or(false) {
                events.push(Event::Done(DoneEvent { order_id: bid_id, reason: DoneReason::Filled }));
                let (price, side) = (self.orders.get(&bid_id).map(|o| o.price).unwrap_or(clearing_price), Side::Bid);
                self.remove_from_level(bid_id, price, side);
                bid_idx += 1;
            }
            if self.orders.get(&ask_id).map(|o| !o.live).unwrap_or(false) {
                events.push(Event::Done(DoneEvent { order_id: ask_id, reason: DoneReason::Filled }));
                let (price, side) = (self.orders.get(&ask_id).map(|o| o.price).unwrap_or(clearing_price), Side::Ask);
                self.remove_from_level(ask_id, price, side);
                ask_idx += 1;
            }
        }

        events
    }

    fn auction_order_ids(&self, side: Side, clearing_price: PriceTick) -> Vec<OrderId> {
        let mut ids = Vec::new();
        match side {
            Side::Bid => {
                for (price, level) in self.bids.iter().rev() {
                    if *price < clearing_price {
                        break;
                    }
                    for id in level {
                        ids.push(*id);
                    }
                }
            }
            Side::Ask => {
                for (price, level) in &self.asks {
                    if *price > clearing_price {
                        break;
                    }
                    for id in level {
                        ids.push(*id);
                    }
                }
            }
        }
        ids
    }

    fn remove_from_level(&mut self, id: OrderId, price: PriceTick, side: Side) {
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
    }

    fn would_cross(&self, order: &Order) -> bool {
        self.would_cross_at(order.side, order.price)
    }

    fn would_cross_at(&self, side: Side, price: PriceTick) -> bool {
        let best_opp = match side {
            Side::Bid => self.best_ask_price(),
            Side::Ask => self.best_bid_price(),
        };
        let opp_price = match best_opp {
            Some(price) => price,
            None => return false,
        };
        match side {
            Side::Bid => opp_price <= price,
            Side::Ask => opp_price >= price,
        }
    }

    fn can_fully_fill(&self, order: &Order) -> bool {
        let mut remaining = order.qty_remaining;
        match order.side {
            Side::Bid => {
                for (price, level) in &self.asks {
                    if *price > order.price {
                        break;
                    }
                    for id in level {
                        if let Some(o) = self.orders.get(id) {
                            if o.live && o.qty_remaining > 0 {
                                if o.qty_remaining >= remaining {
                                    return true;
                                }
                                remaining -= o.qty_remaining;
                            }
                        }
                    }
                }
            }
            Side::Ask => {
                for (price, level) in self.bids.iter().rev() {
                    if *price < order.price {
                        break;
                    }
                    for id in level {
                        if let Some(o) = self.orders.get(id) {
                            if o.live && o.qty_remaining > 0 {
                                if o.qty_remaining >= remaining {
                                    return true;
                                }
                                remaining -= o.qty_remaining;
                            }
                        }
                    }
                }
            }
        }
        false
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
