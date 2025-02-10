#!/usr/bin/env sh

go build -o unique-id-generation main.go &&
	maelstrom test -w unique-ids --bin ./unique-id-generation \
		--time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
