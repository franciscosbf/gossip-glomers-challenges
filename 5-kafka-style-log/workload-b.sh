#!/usr/bin/env sh

go build -o kafka-style-log main.go &&
	maelstrom test -w kafka --bin ./kafka-style-log \
		--node-count 2 --concurrency 2n --time-limit 20 --rate 1000
