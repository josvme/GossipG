package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type FullTopology struct {
	store map[string][]string
	mu    sync.Mutex
}

func (t *FullTopology) Update(m map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.store = convertFullTopology(m)
	return nil
}

func (t *FullTopology) Neighbors(nodeID string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	neighbors, ok := t.store[nodeID]
	if !ok {
		return nil
	}
	return slices.Clone(neighbors)
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
	slices.Sort(results)
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

type SendTo struct {
	dest string
	body any
}

type BackgroundSyncer struct {
	channel  chan SendTo
	node     *maelstrom.Node
	kv       *KV
	topology *FullTopology
}

func (b *BackgroundSyncer) enqueueMsg(v SendTo) {
	b.channel <- v
}

func (b *BackgroundSyncer) sync() {
	batch := make(map[string][]int64)
	mu := sync.Mutex{}
	// We will batch all requests in the last 200 milliseconds.
	// This means we will have fewer messages
	// The time can be fine-tuned w.r.t latency/message count tradeoff
	ticker := time.NewTicker(250 * time.Millisecond)

	go func() {
		for {
			select {
			case v := <-b.channel:
				// If it is a client, skip it
				if strings.HasPrefix(v.dest, "c") {
					continue
				}
				mu.Lock()
				msg, ok := v.body.(map[string]any)
				if ok {
					if m, ok := msg["message"]; ok {
						collectMessage(m, batch, v)
					} else if msgs, ok := msg["messages"]; ok {
						collectMessage(msgs, batch, v)
					}
				}
				mu.Unlock()
			}
		}
	}()

	// Every 100 milliseconds, we will send and clear the batched messages
	for range ticker.C {
		mu.Lock()
		for dest, messages := range batch {
			if len(messages) == 0 {
				continue
			}
			go func(dest string, messages []int64) {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, err := b.node.SyncRPC(ctx, dest, map[string]any{
					"type":     "batch",
					"messages": Unique(messages),
				})
				cancel()
				if err != nil {
					return
				}
				delete(batch, dest)
			}(dest, messages)
		}

		mu.Unlock()
	}
}

func collectMessage(m any, batch map[string][]int64, v SendTo) {
	switch val := m.(type) {
	case int64:
		batch[v.dest] = append(batch[v.dest], val)
	case []int64:
		batch[v.dest] = append(batch[v.dest], val...)
	case []interface{}:
		for _, i := range val {
			if f, ok := i.(float64); ok {
				batch[v.dest] = append(batch[v.dest], int64(f))
			} else if i64, ok := i.(int64); ok {
				batch[v.dest] = append(batch[v.dest], i64)
			}
		}
	}
}

// Save the message to local memory and send it to three hop neighbors
// We choose 3 hops neighbors to reduce the message propagation latency as the furthest node is eight hops away
func (b *BackgroundSyncer) saveAndPropagate(uniqueMessage int64, srcNode string) {
	if b.kv.Contains(uniqueMessage) {
		return
	}
	_ = b.kv.Put(uniqueMessage, uniqueMessage)

	allTwoHopPaths := make([]string, 0, 20)
	for _, o := range b.topology.Neighbors(b.node.ID()) {
		allTwoHopPaths = append(allTwoHopPaths, b.topology.Neighbors(o)...)
	}
	allTwoHopPaths = Unique(allTwoHopPaths)

	allThreeHopPaths := make([]string, 0, 20)
	for _, o := range allTwoHopPaths {
		allThreeHopPaths = append(allThreeHopPaths, b.topology.Neighbors(o)...)
	}
	allThreeHopPaths = Unique(allThreeHopPaths)

	for _, o := range allThreeHopPaths {
		if o == srcNode {
			continue
		}
		if slices.Contains(b.topology.store[b.node.ID()], o) {
			continue
		}
		if strings.HasPrefix(o, "c") {
			continue
		}
		b.enqueueMsg(SendTo{
			dest: o,
			body: map[string]any{"type": "broadcast", "message": uniqueMessage},
		})
	}

	dst, _ := strconv.Atoi(b.node.ID()[1:])
	d := "n" + strconv.Itoa(24-dst)
	b.enqueueMsg(SendTo{
		dest: d,
		body: map[string]any{"type": "broadcast", "message": uniqueMessage},
	})
}

func Unique[T comparable](in []T) []T {
	seen := make(map[T]struct{}, len(in))
	out := make([]T, 0, len(in))

	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	return out
}

func main() {
	n := maelstrom.NewNode()
	kv := KV{
		store: make(map[int64]int64),
		mu:    sync.Mutex{},
	}

	topology := FullTopology{
		store: make(map[string][]string),
		mu:    sync.Mutex{},
	}

	bkSyncer := BackgroundSyncer{
		channel:  make(chan SendTo, 1000),
		node:     n,
		kv:       &kv,
		topology: &topology,
	}

	// Turn on background sync
	go bkSyncer.sync()
	// Turn on heartbeat
	// go bkSyncer.fullSyncHeartBeat()

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		// Skip them, as other nodes will return broadcast_ok
		return nil
	})

	n.Handle("batch", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messages := body["messages"].([]interface{})
		for _, m := range messages {
			uniqueMessage := int64(m.(float64))
			bkSyncer.saveAndPropagate(uniqueMessage, msg.Src)
		}
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		uniqueMessage := int64(body["message"].(float64))
		bkSyncer.saveAndPropagate(uniqueMessage, msg.Src)

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

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": Unique(vals)})
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
