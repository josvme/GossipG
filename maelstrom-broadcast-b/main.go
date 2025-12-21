package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type FullTopology struct {
	store map[string][]string
	mu    sync.Mutex
}

func (t *FullTopology) Update(m map[string]any) error {
	t.mu.Lock()
	t.store = convertFullTopology(m)
	t.mu.Unlock()
	return nil
}

type KV struct {
	store map[int64]int64
	mu    sync.Mutex
}

func (kv *KV) Put(k, v int64) error {
	kv.mu.Lock()
	kv.store[k] = v
	kv.mu.Unlock()
	return nil
}

func (kv *KV) Get() ([]int64, error) {
	kv.mu.Lock()
	results := make([]int64, 0)
	for _, v := range kv.store {
		results = append(results, v)
	}
	kv.mu.Unlock()
	return results, nil
}

func (kv *KV) Contains(k int64) bool {
	kv.mu.Lock()
	_, ok := kv.store[k]
	kv.mu.Unlock()
	return ok
}

func convertFullTopology(m map[string]any) map[string][]string {
	newMap := make(map[string][]string)
	for k, v := range m {
		tmpSlice := make([]string, 0)
		for _, vv := range v.([]interface{}) {
			tmpSlice = append(tmpSlice, vv.(string))
		}
		newMap[k] = tmpSlice
	}
	return newMap
}

func main() {
	n := maelstrom.NewNode()
	kv := KV{
		store: make(map[int64]int64, 0),
		mu:    sync.Mutex{},
	}

	topology := FullTopology{
		store: make(map[string][]string),
		mu:    sync.Mutex{},
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// JSON number is of float type
		uniqueMessage := int64(body["message"].(float64))
		if kv.Contains(uniqueMessage) {
			// Message was already seen, skip it
			return nil
		}
		err := kv.Put(uniqueMessage, uniqueMessage)
		if err != nil {
			return err
		}
		// After storing it in the KV store, send it to all the nodes in our topology
		for _, o := range topology.store[n.ID()] {
			_ = n.Send(o, body)
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
		err := topology.Update(body["topology"].(map[string]any))
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
