#!/usr/bin/env bash
set -e

PEERS=localhost:3000

cargo build --release --bin ktd

TMP=$(mktemp -d)
for PEER in ${PEERS//,/ }; do
    PORT=${PEER##*:}
    echo "Starting node at $PEER"
    RUST_LOG=info ./target/release/ktd \
        --store-path "$TMP"/store$PORT.db \
        --http-addr localhost:$(( 5000 + $PORT )) \
        --rpc-addr $PEER \
        --peers $PEERS \
        --bandwidth 50000 \
        --availability-zone unique-zone &
done

wait
