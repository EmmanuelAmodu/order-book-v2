#[cfg(test)]
mod tests {
    use hello_rust::order_book::*;

    fn o(id: OrderId, side: Side, price: PriceTick, qty: Qty) -> Order {
        Order::new(id, side, price, qty)
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

        let ev = ob.place_limit(Order::new(2, Side::Bid, 90, 0));
        assert!(has_done(&ev, 2, DoneReason::Rejected));
        assert!(!has_ack(&ev, 2));
        assert!(ob.order(2).is_none());
        assert!(ob.book_is_empty(Side::Bid));
        assert!(ob.book_is_empty(Side::Ask));
    }
}
