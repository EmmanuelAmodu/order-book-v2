# Order Book + JetStream

This service consumes protobuf-encoded order commands from NATS JetStream, routes them to a per-market order book, and publishes protobuf-encoded events.

## Subjects and Streams

- Stream name: `ORDERBOOK` (env `JETSTREAM_STREAM`)
- Orders subject: `orders` (env `ORDERS_SUBJECT`)
- Events subject: `events` (env `EVENTS_SUBJECT`)
- Consumer deliver subject: `orders.deliver` (env `ORDERS_DELIVER_SUBJECT`)
- Consumer name: `orderbook` (env `ORDERS_CONSUMER`)

The stream is created (if missing) with subjects `[orders, events]`.

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
