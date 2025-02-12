package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/serialx/hashring"
)

type initMsg struct {
	Type    string   `json:"type"`
	NodeIds []string `json:"node_ids"`
}

type initReply struct {
	Type string `json:"type"`
}

type sendMsg struct {
	Type string  `json:"type"`
	Key  string  `json:"key"`
	Msg  float64 `json:"msg"`
	Node bool    `json:"node"`
}

type sendReply struct {
	Type   string  `json:"type"`
	Offset float64 `json:"offset"`
}

type pollMsg struct {
	Type    string             `json:"type"`
	Offsets map[string]float64 `json:"offsets"`
}

type pollReply struct {
	Type string                  `json:"type"`
	Msgs map[string][][2]float64 `json:"msgs"`
}

type commitOffsetsMsg struct {
	Type    string             `json:"type"`
	Offsets map[string]float64 `json:"offsets"`
	Node    bool               `json:"node"`
}

type commitOffsetsReply struct {
	Type string `json:"type"`
}

type listCommittedOffsetsMsg struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsReply struct {
	Type    string             `json:"type"`
	Offsets map[string]float64 `json:"offsets"`
}

func offsetKey(key string) string {
	return fmt.Sprintf("%v-offset", key)
}

func msgsKey(key string) string {
	return fmt.Sprintf("%v-msgs", key)
}

func committedOffsetKey(key string) string {
	return fmt.Sprintf("%v-committed-offset", key)
}

type nodeSelector struct {
	ring *hashring.HashRing
}

func (ns *nodeSelector) extend(nodes []string) {
	ns.ring = hashring.New(nodes)
}

func (ns *nodeSelector) get(key string) string {
	n, _ := ns.ring.GetNode(key)

	return n
}

type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func (kl *keyLocks) get(key string) *sync.Mutex {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	lock, ok := kl.locks[key]

	if !ok {
		lock = &sync.Mutex{}
		kl.locks[key] = lock
	}

	return lock
}

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewLinKV(n)

	kl := &keyLocks{locks: map[string]*sync.Mutex{}}
	ns := &nodeSelector{ring: hashring.New([]string{})}

	n.Handle("init", func(msg maelstrom.Message) error {
		var (
			init  initMsg
			reply initReply
		)

		if err := json.Unmarshal(msg.Body, &init); err != nil {
			return err
		}

		ns.extend(init.NodeIds)

		reply.Type = "init_ok"

		return n.Reply(msg, reply)
	})

	n.Handle("send", func(msg maelstrom.Message) error {
		var (
			send  sendMsg
			reply sendReply
		)

		if err := json.Unmarshal(msg.Body, &send); err != nil {
			return err
		}

		nid := ns.get(send.Key)
		if send.Node || nid == n.ID() {
			reply.Type = "send_ok"
			mu := kl.get(send.Key)
			mu.Lock()
			defer mu.Unlock()
			mkey := msgsKey(send.Key)
			var msgs [][2]float64
			if err := kv.ReadInto(context.Background(), mkey, &msgs); err != nil {
				if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
					msgs = [][2]float64{}
				} else {
					return err
				}
			}
			offset := float64(len(msgs))
			msgs = append(msgs, [2]float64{offset, send.Msg})
			if err := kv.Write(context.Background(), mkey, msgs); err != nil {
				return err
			}
			reply.Offset = offset
		} else {
			send.Node = true
			msg, err := n.SyncRPC(context.Background(), nid, send)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(msg.Body, &reply); err != nil {
				return err
			}
		}

		return n.Reply(msg, reply)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var (
			poll  pollMsg
			reply pollReply
		)

		if err := json.Unmarshal(msg.Body, &poll); err != nil {
			return err
		}

		reply.Type = "poll_ok"

		reply.Msgs = map[string][][2]float64{}
		for key, offset := range poll.Offsets {
			mkey := msgsKey(key)
			var msgs [][2]float64
			if err := kv.ReadInto(context.Background(), mkey, &msgs); err != nil {
				if err.(*maelstrom.RPCError).Code != maelstrom.KeyDoesNotExist {
					return err
				}
				reply.Msgs[key] = [][2]float64{}
			} else {
				if offset >= float64(len(msgs)) {
					reply.Msgs[key] = [][2]float64{}
				} else {
					reply.Msgs[key] = msgs[int(offset):]
				}
			}
		}

		return n.Reply(msg, reply)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var (
			commitOffsets commitOffsetsMsg
			reply         commitOffsetsReply
		)

		if err := json.Unmarshal(msg.Body, &commitOffsets); err != nil {
			return err
		}

		reply.Type = "commit_offsets_ok"

		for key, offset := range commitOffsets.Offsets {
			nid := ns.get(key)
			if commitOffsets.Node || nid == n.ID() {
				mu := kl.get(key)
				if err := func() error {
					mu.Lock()
					defer mu.Unlock()
					cokey := committedOffsetKey(key)
					coffset, err := kv.ReadInt(context.Background(), cokey)
					if err != nil {
						if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
							coffset = 0
						} else {
							return err
						}
					}
					if offset >= float64(coffset) {
						if err := kv.Write(context.Background(), cokey, int(offset)); err != nil {
							return err
						}
					}
					return nil
				}(); err != nil {
					return err
				}
			} else {
				commitOffsets := commitOffsetsMsg{
					Type:    "commit_offsets",
					Offsets: map[string]float64{key: offset},
					Node:    true,
				}
				msg, err := n.SyncRPC(context.Background(), nid, commitOffsets)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(msg.Body, &reply); err != nil {
					return err
				}
			}
		}

		return n.Reply(msg, reply)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var (
			listCommittedOffsets listCommittedOffsetsMsg
			reply                listCommittedOffsetsReply
		)

		if err := json.Unmarshal(msg.Body, &listCommittedOffsets); err != nil {
			return err
		}

		reply.Type = "list_committed_offsets_ok"

		reply.Offsets = map[string]float64{}
		for _, key := range listCommittedOffsets.Keys {
			cokey := committedOffsetKey(key)
			offset, err := kv.ReadInt(context.Background(), cokey)

			if err == nil {
				reply.Offsets[key] = float64(offset)
				continue
			} else if err.(*maelstrom.RPCError).Code != maelstrom.KeyDoesNotExist {
				return err
			}
		}

		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
