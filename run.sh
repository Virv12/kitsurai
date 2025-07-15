#!/usr/bin/env bash

ulimit -n 100000

RUST_LOG=info ./target/release/ktd \
	--peers 192.168.1.168:3000,192.168.1.128:3000,192.168.1.100:3000 \
	--store-path tmp/store.db \
	--http-addr 0.0.0.0:8000 \
	--rpc-addr 192.168.1.168:3000 \
	--bandwidth 100000
