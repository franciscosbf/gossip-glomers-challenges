#!/usr/bin/env sh

go build -o grow-only-counter main.go &&
	maelstrom test -w g-counter --bin grow-only-counter \
		--node-count 3 --rate 100 --time-limit 20 --nemesis partition
