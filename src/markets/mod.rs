use std::collections::HashMap;
use crate::engine::order_book::{OrderBook, OrderBookSnapshot};

pub struct Markets {
    books: HashMap<String, OrderBook>,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub market_id: String,
    pub book: OrderBookSnapshot,
}

#[derive(Debug, Clone)]
pub struct MarketsSnapshot {
    pub markets: Vec<MarketSnapshot>,
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

    pub fn snapshot(&self) -> MarketsSnapshot {
        let markets = self
            .books
            .iter()
            .map(|(market_id, book)| MarketSnapshot {
                market_id: market_id.clone(),
                book: book.snapshot(),
            })
            .collect();
        MarketsSnapshot { markets }
    }

    pub fn from_snapshot(snapshot: MarketsSnapshot) -> Self {
        let books = snapshot
            .markets
            .into_iter()
            .map(|market| (market.market_id, OrderBook::from_snapshot(market.book)))
            .collect();
        Self { books }
    }
}
