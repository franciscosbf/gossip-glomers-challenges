#!/usr/bin/env sh

go build -o broadcast main.go &&
	maelstrom test -w broadcast --bin ./broadcast \
		--node-count 25 --time-limit 20 --rate 100 --latency 100
