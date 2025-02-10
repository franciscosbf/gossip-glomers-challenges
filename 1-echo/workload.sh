#!/usr/bin/env sh

go build -o echo main.go &&
	maelstrom test -w echo --bin ./echo \
		--node-count 1 --time-limit 10
