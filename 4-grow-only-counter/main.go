package main

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(n)

	var local atomic.Int64

	n.Handle("init", func(msg maelstrom.Message) error {
		var body struct {
			Type    string    `json:"type"`
			NodeId  *string   `json:"node_id,omitempty"`
			NodeIds *[]string `json:"node_ids,omitempty"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nid := *body.NodeId
		nids := *body.NodeIds

		if err := kv.Write(context.Background(), nid, 0); err != nil {
			return nil
		}

		if nid == "n0" {
			if err := kv.Write(context.Background(), "total", 0); err != nil {
				return err
			}

			go func() {
			retry:
				for {
					time.Sleep(1 * time.Second)
					sum := 0
					for _, n := range nids {
						if value, err := kv.ReadInt(context.Background(), n); err == nil {
							sum += value
						} else {
							goto retry
						}
					}
					kv.Write(context.Background(), "total", sum)
				}
			}()
		}

		go func() {
			for {
				time.Sleep(1 * time.Second)
				kv.Write(context.Background(), nid, local.Load())
			}
		}()

		body.Type = "init_ok"
		body.NodeId = nil
		body.NodeIds = nil

		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		local.Add(int64(delta))

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{}

		total, err := kv.ReadInt(context.Background(), "total")
		if err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["value"] = total

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
