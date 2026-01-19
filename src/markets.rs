use std::collections::HashMap;
use crate::order_book::OrderBook;

pub(crate) struct Markets {
    books: HashMap<String, OrderBook>,
}

impl Markets {
    pub fn new() -> Self {
        Self { books: HashMap::new() }
    }
}
