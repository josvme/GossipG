package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Topology struct {
	store map[string]any
	mu    sync.Mutex
}

func (t *Topology) Update(m map[string]any) error {
	t.mu.Lock()
	t.store = m
	t.mu.Unlock()
	return nil
}

type KV struct {
	store []int64
	mu    sync.Mutex
}

func (kv *KV) Put(v int64) error {
	kv.mu.Lock()
	kv.store = append(kv.store, v)
	kv.mu.Unlock()
	return nil
}

func (kv *KV) Get() ([]int64, error) {
	kv.mu.Lock()
	values := kv.store
	kv.mu.Unlock()
	return values, nil
}

func main() {
	n := maelstrom.NewNode()
	kv := KV{
		store: make([]int64, 0),
		mu:    sync.Mutex{},
	}

	topology := Topology{
		store: make(map[string]any),
		mu:    sync.Mutex{},
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// JSON number is of float type
		err := kv.Put(int64(body["message"].(float64)))
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		vals, err := kv.Get()
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": vals})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		err := topology.Update(body)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
