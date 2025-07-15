#!/usr/bin/env bash
set -e

trap "kill -- -$$" EXIT

PEERS=localhost:3000,localhost:3001,localhost:3002

cargo build --release --bins

ulimit -n 100000

TMP=$(mktemp -d)
for PEER in ${PEERS//,/ }; do
    PORT=${PEER##*:}
    echo "Starting node at $PEER"
    ./target/release/ktd \
        --store-path "$TMP"/store$PORT.db \
        --http-addr localhost:$(( 5000 + $PORT )) \
        --rpc-addr $PEER \
        --bandwidth 100000 \
        --peers $PEERS &
done

sleep 1

TABLE=$(./target/release/ktc table create 100000 1 1 1 | awk '{ print $3 }')
for i in {0..99}; do
    ./target/release/ktc item $TABLE set key-$i value >/dev/null
done

./target/release/ktb $TABLE "key"
