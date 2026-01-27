#[cfg(test)]
mod tests {
    use hello_rust::engine::order_book::*;

    fn o(id: OrderId, side: Side, price: PriceTick, qty: Qty) -> Order {
        Order::new(id, id, side, price, qty)
    }

    fn o_owner(id: OrderId, owner_id: OwnerId, side: Side, price: PriceTick, qty: Qty) -> Order {
        Order::new(id, owner_id, side, price, qty)
    }

    fn trades(events: &[Event]) -> Vec<TradeEvent> {
        events.iter().filter_map(|e| {
            if let Event::Trade(t) = *e { Some(t) } else { None }
        }).collect()
    }

    fn done(events: &[Event]) -> Vec<DoneEvent> {
        events.iter().filter_map(|e| {
            if let Event::Done(d) = *e { Some(d) } else { None }
        }).collect()
    }

    fn updates(events: &[Event]) -> Vec<UpdateEvent> {
        events.iter().filter_map(|e| {
            if let Event::Update(u) = *e { Some(u) } else { None }
        }).collect()
    }

    fn has_done(events: &[Event], id: OrderId, reason: DoneReason) -> bool {
        done(events).into_iter().any(|d| d.order_id == id && d.reason == reason)
    }

    fn has_ack(events: &[Event], id: OrderId) -> bool {
        events.iter().any(|e| {
            if let Event::Ack(a) = *e { a.order_id == id } else { false }
        })
    }

    #[test]
    fn rests_when_no_cross() {
        let mut ob = OrderBook::new();

        // best ask at 110
        ob.place_limit(o(1, Side::Ask, 110, 5));

        // bid at 100 should NOT cross 110 -> should rest
        let ev = ob.place_limit(o(2, Side::Bid, 100, 3));
        assert!(trades(&ev).is_empty());

        assert_eq!(ob.best_bid_price(), Some(100));
        assert_eq!(ob.best_ask_price(), Some(110));

        // ensure bid level contains taker id
        assert_eq!(ob.level_front(Side::Bid, 100), Some(2));
        let ord = ob.order(2).unwrap();
        assert!(ord.live());
        assert_eq!(ord.qty_remaining(), 3);
    }

    #[test]
    fn simple_match_fills_taker_partially_or_fully() {
        let mut ob = OrderBook::new();

        // Maker ask
        ob.place_limit(o(1, Side::Ask, 100, 5));

        // Taker bid crosses
        let ev = ob.place_limit(o(2, Side::Bid, 100, 3));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);

        assert_eq!(ts[0].taker_id, 2);
        assert_eq!(ts[0].maker_id, 1);
        assert_eq!(ts[0].price, 100);
        assert_eq!(ts[0].qty, 3);

        // Maker should have 2 remaining and still be live
        let maker = ob.order(1).unwrap();
        assert!(maker.live());
        assert_eq!(maker.qty_remaining(), 2);

        // Taker filled
        let taker = ob.order(2).unwrap();
        assert!(!taker.live());
        assert_eq!(taker.qty_remaining(), 0);
        assert!(has_done(&ev, 2, DoneReason::Filled));
        assert!(!has_done(&ev, 1, DoneReason::Filled));
    }

    #[test]
    fn partial_fill_maker_stays_at_front_of_level() {
        let mut ob = OrderBook::new();

        // Maker ask qty 10
        ob.place_limit(o(10, Side::Ask, 200, 10));

        // Taker bid qty 3
        let _ev = ob.place_limit(o(11, Side::Bid, 200, 3));

        // Maker should still be at the front of the same level (NOT popped)
        assert_eq!(ob.level_front(Side::Ask, 200), Some(10));

        let maker = ob.order(10).unwrap();
        assert!(maker.live());
        assert_eq!(maker.qty_remaining(), 7);
    }

    #[test]
    fn fifo_within_same_price_level() {
        let mut ob = OrderBook::new();

        // Two asks at same price, time priority: 1 then 2
        ob.place_limit(o(1, Side::Ask, 100, 2));
        ob.place_limit(o(2, Side::Ask, 100, 2));

        // Buy 3 should fill maker 1 fully (2) then maker 2 partially (1)
        let ev = ob.place_limit(o(99, Side::Bid, 100, 3));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 2);

        assert_eq!((ts[0].maker_id, ts[0].qty), (1, 2));
        assert_eq!((ts[1].maker_id, ts[1].qty), (2, 1));

        assert!(has_done(&ev, 1, DoneReason::Filled));
        assert!(!has_done(&ev, 2, DoneReason::Filled));

        let maker2 = ob.order(2).unwrap();
        assert!(maker2.live());
        assert_eq!(maker2.qty_remaining(), 1);

        // Level front should still be maker 2
        assert_eq!(ob.level_front(Side::Ask, 100), Some(2));
    }

    #[test]
    fn sweeps_multiple_price_levels_and_removes_empty_levels() {
        let mut ob = OrderBook::new();

        // Asks at 10 and 11
        ob.place_limit(o(1, Side::Ask, 10, 2));
        ob.place_limit(o(2, Side::Ask, 11, 2));

        // Bid @11 qty 3 should take 2 at 10 and 1 at 11
        let ev = ob.place_limit(o(9, Side::Bid, 11, 3));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 2);

        assert_eq!((ts[0].price, ts[0].qty, ts[0].maker_id), (10, 2, 1));
        assert_eq!((ts[1].price, ts[1].qty, ts[1].maker_id), (11, 1, 2));

        // Level 10 should be removed (fully consumed)
        assert!(!ob.level_exists(Side::Ask, 10));

        // Maker at 11 should have 1 remaining
        let maker2 = ob.order(2).unwrap();
        assert!(maker2.live());
        assert_eq!(maker2.qty_remaining(), 1);

        // Best ask should now be 11
        assert_eq!(ob.best_ask_price(), Some(11));
    }

    #[test]
    fn cancel_tombstone_is_skipped_during_matching() {
        let mut ob = OrderBook::new();

        // Place ask then cancel
        ob.place_limit(o(1, Side::Ask, 100, 5));
        let cev = ob.cancel(1);
        assert!(has_done(&cev, 1, DoneReason::Canceled));

        // Place a new ask at same price
        ob.place_limit(o(2, Side::Ask, 100, 5));

        // Buy should match maker 2 (not canceled maker 1)
        let ev = ob.place_limit(o(9, Side::Bid, 100, 5));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].maker_id, 2);
        assert_eq!(ts[0].qty, 5);

        assert!(has_done(&ev, 2, DoneReason::Filled));
        assert!(!has_done(&ev, 1, DoneReason::Filled)); // already canceled
    }

    #[test]
    fn reject_duplicate_order_id() {
        let mut ob = OrderBook::new();

        let ev1 = ob.place_limit(o(1, Side::Ask, 100, 5));
        assert!(has_ack(&ev1, 1));

        let ev2 = ob.place_limit(o(1, Side::Ask, 110, 5));
        assert!(has_done(&ev2, 1, DoneReason::Rejected));
        assert!(!has_ack(&ev2, 1));

        let ord = ob.order(1).unwrap();
        assert_eq!(ord.price(), 100);
        assert!(ord.live());

        assert_eq!(ob.level_front(Side::Ask, 100), Some(1));
        assert!(!ob.level_exists(Side::Ask, 110));
    }

    #[test]
    fn reject_zero_qty_input() {
        let mut ob = OrderBook::new();

        let ev = ob.place_limit(Order::new(2, 2, Side::Bid, 90, 0));
        assert!(has_done(&ev, 2, DoneReason::Rejected));
        assert!(!has_ack(&ev, 2));
        assert!(ob.order(2).is_none());
        assert!(ob.book_is_empty(Side::Bid));
        assert!(ob.book_is_empty(Side::Ask));
    }

    #[test]
    fn cancel_removes_from_level_and_clears_best_price() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 5));
        assert_eq!(ob.best_ask_price(), Some(100));

        let ev = ob.cancel(1);
        assert!(has_done(&ev, 1, DoneReason::Canceled));

        assert!(!ob.level_exists(Side::Ask, 100));
        assert_eq!(ob.best_ask_price(), None);
        assert!(ob.book_is_empty(Side::Ask));
    }

    #[test]
    fn ioc_cancels_remainder_without_resting() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 2));

        let ev = ob.place_limit_with_tif(o(2, Side::Bid, 100, 5), TimeInForce::Ioc);
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].qty, 2);

        assert!(has_done(&ev, 2, DoneReason::Canceled));
        assert!(has_ack(&ev, 2));
        assert!(ob.best_bid_price().is_none());
        assert!(ob.book_is_empty(Side::Bid));
    }

    #[test]
    fn fok_rejects_when_not_fully_fillable() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 2));

        let ev = ob.place_limit_with_tif(o(2, Side::Bid, 100, 3), TimeInForce::Fok);
        assert!(trades(&ev).is_empty());
        assert!(has_done(&ev, 2, DoneReason::Rejected));
        assert!(!has_ack(&ev, 2));
        assert!(ob.order(2).is_none());
    }

    #[test]
    fn post_only_rejects_crossing_and_rests_when_not_crossing() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 2));

        let ev = ob.place_limit_with_tif(o(2, Side::Bid, 110, 1), TimeInForce::PostOnly);
        assert!(trades(&ev).is_empty());
        assert!(has_done(&ev, 2, DoneReason::Rejected));
        assert!(!has_ack(&ev, 2));
        assert!(ob.order(2).is_none());

        let ev = ob.place_limit_with_tif(o(3, Side::Bid, 90, 1), TimeInForce::PostOnly);
        assert!(has_ack(&ev, 3));
        assert_eq!(ob.best_bid_price(), Some(90));
        assert_eq!(ob.level_front(Side::Bid, 90), Some(3));
    }

    #[test]
    fn market_order_sweeps_and_cancels_remainder() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 2));
        ob.place_limit(o(2, Side::Ask, 101, 2));

        let ev = ob.place_market(Order::market(9, 99, Side::Bid, 5));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].qty, 2);
        assert_eq!(ts[1].qty, 2);
        assert!(has_done(&ev, 9, DoneReason::Canceled));
        assert!(has_ack(&ev, 9));
        assert!(ob.book_is_empty(Side::Ask));
        assert!(ob.book_is_empty(Side::Bid));
    }

    #[test]
    fn partial_cancel_emits_update_and_reduces_qty() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 5));
        let ev = ob.cancel_qty(1, 2);
        let us = updates(&ev);
        assert_eq!(us.len(), 1);
        assert_eq!(us[0].order_id, 1);
        assert_eq!(us[0].qty_remaining, 3);
        assert_eq!(us[0].price, 100);

        let ord = ob.order(1).unwrap();
        assert!(ord.live());
        assert_eq!(ord.qty_remaining(), 3);
        assert_eq!(ob.best_ask_price(), Some(100));
    }

    #[test]
    fn amend_moves_price_and_emits_update() {
        let mut ob = OrderBook::new();

        ob.place_limit(o(1, Side::Ask, 100, 5));
        let ev = ob.amend(1, Some(105), None);
        let us = updates(&ev);
        assert_eq!(us.len(), 1);
        assert_eq!(us[0].price, 105);
        assert_eq!(ob.best_ask_price(), Some(105));
        assert!(!ob.level_exists(Side::Ask, 100));
    }

    #[test]
    fn amend_rejects_crossing_for_post_only() {
        let mut ob = OrderBook::new();

        ob.place_limit_with_tif(o(1, Side::Ask, 110, 5), TimeInForce::PostOnly);
        ob.place_limit(o(2, Side::Bid, 100, 5));

        let ev = ob.amend(1, Some(90), None);
        assert!(has_done(&ev, 1, DoneReason::Rejected));

        let ord = ob.order(1).unwrap();
        assert_eq!(ord.price(), 110);
        assert_eq!(ob.best_ask_price(), Some(110));
    }

    #[test]
    fn auction_defers_matching_and_clears_on_run() {
        let mut ob = OrderBook::new();
        ob.set_phase(MarketPhase::Auction);

        let ev1 = ob.place_limit(o(1, Side::Bid, 110, 5));
        let ev2 = ob.place_limit(o(2, Side::Ask, 100, 5));
        assert!(trades(&ev1).is_empty());
        assert!(trades(&ev2).is_empty());
        assert!(has_ack(&ev1, 1));
        assert!(has_ack(&ev2, 2));

        let ev = ob.run_auction(MarketPhase::Open);
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].price, 110);
        assert!(has_done(&ev, 1, DoneReason::Filled));
        assert!(has_done(&ev, 2, DoneReason::Filled));
        assert_eq!(ob.phase(), MarketPhase::Open);
        assert!(ob.book_is_empty(Side::Bid));
        assert!(ob.book_is_empty(Side::Ask));
    }

    #[test]
    fn closed_rejects_new_orders() {
        let mut ob = OrderBook::new();
        ob.set_phase(MarketPhase::Closed);

        let ev = ob.place_limit(o(1, Side::Bid, 100, 1));
        assert!(has_done(&ev, 1, DoneReason::Rejected));
        assert!(!has_ack(&ev, 1));
        assert!(ob.order(1).is_none());
    }

    #[test]
    fn auction_rejects_ioc() {
        let mut ob = OrderBook::new();
        ob.set_phase(MarketPhase::Auction);

        let ev = ob.place_limit_with_tif(o(1, Side::Bid, 100, 1), TimeInForce::Ioc);
        assert!(has_done(&ev, 1, DoneReason::Rejected));
        assert!(!has_ack(&ev, 1));
        assert!(ob.order(1).is_none());
    }

    #[test]
    fn self_trade_is_prevented() {
        let mut ob = OrderBook::new();

        ob.place_limit(o_owner(1, 42, Side::Ask, 100, 5));
        let ev = ob.place_limit(o_owner(2, 42, Side::Bid, 100, 5));
        assert!(trades(&ev).is_empty());
        assert!(has_done(&ev, 2, DoneReason::Canceled));

        let maker = ob.order(1).unwrap();
        assert!(maker.live());
        assert_eq!(maker.qty_remaining(), 5);
        assert_eq!(ob.best_ask_price(), Some(100));
        assert!(ob.book_is_empty(Side::Bid));
    }
}
