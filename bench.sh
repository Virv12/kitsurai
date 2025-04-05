#!/usr/bin/env bash
set -e

trap "kill -- -$$" EXIT

PEERS=localhost:3000,localhost:3001,localhost:3002

cargo build --release --bins

ulimit -n 10000

TMP=$(mktemp -d)
for PEER in ${PEERS//,/ }; do
    PORT=${PEER##*:}
    echo "Starting node at $PEER"
    ./target/release/ktd \
        --store-path "$TMP"/store$PORT.db \
        --http-addr localhost:$(( 5000 + $PORT )) \
        --rpc-addr $PEER \
        --peers $PEERS &
done

sleep 1

TABLE=$(./target/release/ktc table create 20 1 1 1 | awk '{ print $3 }')
./target/release/ktc item $TABLE set key-1 value-1 >/dev/null

./target/release/ktb $TABLE "key-1"
