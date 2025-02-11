package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type msgs struct {
	mu     sync.RWMutex
	values []float64
}

func (ms *msgs) insert(msg float64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.values = append(ms.values, msg)
}

func (ms *msgs) get() []float64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return append(make([]float64, len(ms.values)), ms.values...)
}

type network struct {
	topology map[string][]string
}

func (net *network) update(topology map[string][]string) {
	net.topology = topology
}

func (net *network) accessible(node string) []string {
	return net.topology[node]
}

type broadcast struct {
	mu  sync.Mutex
	log map[string][]string
}

func (b *broadcast) broadcasted(message float64, node string) (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v-%v-%v", message, node, id), nil
}

func (b *broadcast) received(broadcasted string, node string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if nodes, ok := b.log[broadcasted]; ok {
		for _, n := range nodes {
			if n == node {
				return true
			}
		}

		b.log[broadcasted] = append(b.log[broadcasted], node)
	} else {
		b.log[broadcasted] = []string{node}
	}

	return false
}

func (b *broadcast) source(message float64, node string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	broadcasted, err := b.broadcasted(message, node)
	if err != nil {
		return "", err
	}

	b.log[broadcasted] = []string{node}

	return broadcasted, nil
}

func main() {
	n := maelstrom.NewNode()

	ms := &msgs{values: []float64{}}
	net := &network{topology: map[string][]string{}}
	b := &broadcast{log: map[string][]string{}}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		type content struct {
			Type        string   `json:"type"`
			Message     *float64 `json:"message,omitempty"`
			Broadcasted *string  `json:"broadcasted,omitempty"`
		}

		var body content

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := n.ID()

		var (
			broadcasted string
			err         error
		)

		if body.Broadcasted != nil {
			if b.received(*body.Broadcasted, id) {
				return nil
			}

			broadcasted = *body.Broadcasted
		} else {
			broadcasted, err = b.source(*body.Message, id)
			if err != nil {
				return err
			}

		}

		message := *body.Message
		ms.insert(message)

		bbody := content{
			Type:        body.Type,
			Message:     body.Message,
			Broadcasted: new(string),
		}
		*bbody.Broadcasted = broadcasted
		for _, an := range net.accessible(id) {
			if b.received(broadcasted, an) {
				continue
			}

			go func(an string) {
				ok := make(chan struct{}, 1)

				for {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					n.RPC(an, bbody, func(msg maelstrom.Message) error {
						select {
						case ok <- struct{}{}:
						default:
						}

						return nil
					})

					select {
					case <-ok:
						return
					case <-ctx.Done():
					}
				}
			}(an)
		}

		body.Type = "broadcast_ok"
		body.Message = nil
		body.Broadcasted = nil

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := ms.get()

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Type     string               `json:"type"`
			Topology *map[string][]string `json:"topology,omitempty"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := *body.Topology
		net.update(topology)

		body.Type = "topology_ok"
		body.Topology = nil

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
