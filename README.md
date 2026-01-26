# Order Book + JetStream

This service consumes protobuf-encoded order commands from NATS JetStream, routes them to a per-market order book, and publishes protobuf-encoded events.

## Subjects and Streams

- Stream name: `ORDERBOOK` (env `JETSTREAM_STREAM`)
- Orders subject: `orders` (env `ORDERS_SUBJECT`)
- Events subject: `events` (env `EVENTS_SUBJECT`)
- Consumer deliver subject: `orders.deliver` (env `ORDERS_DELIVER_SUBJECT`)
- Consumer name: `orderbook` (env `ORDERS_CONSUMER`)

The stream is created (if missing) with subjects `[orders, events]`.

## Snapshot + Order Tape

- Order tape: the JetStream `orders` subject is the append-only tape. The service can replay orders from any snapshot by starting at the snapshot sequence.
- Snapshots are protobuf-encoded `StateSnapshot` messages stored in S3 at:
  - `s3://$SNAPSHOT_BUCKET/$SNAPSHOT_PREFIX/$JETSTREAM_STREAM/seq_{sequence}.bin`
- On shutdown (SIGINT/SIGTERM), the service writes a snapshot.
- On startup, it loads the latest snapshot by default or a specific one if `SNAPSHOT_KEY` is set.

Environment variables:

- `SNAPSHOT_BUCKET` (required to enable snapshots)
- `SNAPSHOT_PREFIX` (default: `snapshots`)
- `SNAPSHOT_KEY` (optional: load a specific snapshot key)

## Message Schema (Protobuf)

Payloads are protobuf messages defined in `proto/orderbook.proto`:

```proto
syntax = "proto3";
package orderbook;

enum Side {
  SIDE_UNSPECIFIED = 0;
  SIDE_BID = 1;
  SIDE_ASK = 2;
}

message PlaceLimit {
  uint64 id = 1;
  Side side = 2;
  uint32 price = 3;
  uint64 qty = 4;
  string market_id = 5;
}

message Cancel {
  uint64 id = 1;
  string market_id = 2;
}

message OrderCommand {
  oneof cmd {
    PlaceLimit place_limit = 1;
    Cancel cancel = 2;
  }
}

enum DoneReason {
  DONE_REASON_UNSPECIFIED = 0;
  DONE_REASON_FILLED = 1;
  DONE_REASON_CANCELED = 2;
  DONE_REASON_REJECTED = 3;
}

message TradeEvent {
  uint64 taker_id = 1;
  uint64 maker_id = 2;
  uint32 price = 3;
  uint64 qty = 4;
}

message DoneEvent {
  uint64 order_id = 1;
  DoneReason reason = 2;
}

message AckEvent {
  uint64 order_id = 1;
}

message Event {
  oneof kind {
    TradeEvent trade = 1;
    DoneEvent done = 2;
    AckEvent ack = 3;
  }
}

message OrderSnapshot {
  uint64 id = 1;
  uint32 price = 2;
  uint64 qty_remaining = 3;
  Side side = 4;
  bool live = 5;
}

message LevelSnapshot {
  uint32 price = 1;
  repeated uint64 order_ids = 2;
}

message OrderBookSnapshot {
  repeated OrderSnapshot orders = 1;
  repeated LevelSnapshot bids = 2;
  repeated LevelSnapshot asks = 3;
}

message MarketSnapshot {
  string market_id = 1;
  OrderBookSnapshot book = 2;
}

message StateSnapshot {
  uint64 last_sequence = 1;
  repeated MarketSnapshot markets = 2;
}
```

## Semantics

- `market_id` is required for all order commands. A per-market order book is created on first use.
- `Ack` means the order was accepted into the engine (not necessarily rested).
- Events are published to the single `events` subject (no market id in the event payload).

## Running

Start NATS with JetStream:

```
nats-server -js
```

Run the service:

```
cargo run
```
