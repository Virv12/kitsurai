#!/usr/bin/env bash

PEERS=localhost:3000,localhost:3001,localhost:3002,localhost:3003,localhost:3004

cargo build --bin ktd

TMP=$(mktemp -d)
for PEER in ${PEERS//,/ }; do
    PORT=${PEER##*:}
    echo "Starting node at $PEER"
    ./target/debug/ktd \
        --store-path "$TMP"/store$PORT.db \
        --http-addr localhost:$(( 5000 + $PORT )) \
        --rpc-addr $PEER \
        --peers $PEERS &
done

wait
