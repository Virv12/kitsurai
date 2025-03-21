#!/usr/bin/env bash

TMP=$(mktemp -d)
PEERS=localhost:3000,localhost:3001,localhost:3002,localhost:3003,localhost:3004
cargo run --release --bin kitsurai -- \
  --store-path "$TMP"/store0.db --http-addr localhost:8000 --peer-addr localhost:3000 \
  --discovery-strategy csv --discovery-value $PEERS &
cargo run --release --bin kitsurai -- \
  --store-path "$TMP"/store1.db --http-addr localhost:8001 --peer-addr localhost:3001 \
  --discovery-strategy csv --discovery-value $PEERS &
cargo run --release --bin kitsurai -- \
  --store-path "$TMP"/store2.db --http-addr localhost:8002 --peer-addr localhost:3002 \
  --discovery-strategy csv --discovery-value $PEERS &
cargo run --release --bin kitsurai -- \
  --store-path "$TMP"/store3.db --http-addr localhost:8003 --peer-addr localhost:3003 \
  --discovery-strategy csv --discovery-value $PEERS &
cargo run --release --bin kitsurai -- \
  --store-path "$TMP"/store4.db --http-addr localhost:8004 --peer-addr localhost:3004 \
  --discovery-strategy csv --discovery-value $PEERS &

wait
