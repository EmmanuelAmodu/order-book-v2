use std::collections::HashMap;
use crate::engine::order_book::OrderBook;

pub struct Markets {
    books: HashMap<String, OrderBook>,
}

impl Markets {
    pub fn new() -> Self {
        Self { books: HashMap::new() }
    }

    pub fn book_mut(&mut self, market_id: &str) -> &mut OrderBook {
        self.books
            .entry(market_id.to_string())
            .or_insert_with(OrderBook::new)
    }
}
