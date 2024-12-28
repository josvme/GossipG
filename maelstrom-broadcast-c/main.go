package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
	"time"
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
	tmpSlice := make([]string, 0)
	for k, v := range m {
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
	for {
		v := <-b.channel
		err := b.node.Send(v.dest, v.body)
		// only retry temporary errors
		if slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
			maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			b.channel <- v
		}
	}
}

// Do full sync every 1 second
func (b *BackgroundSyncer) heartBeat() {
	for range time.Tick(time.Second * 2) {
		b.fullSync()
	}
}

func (b *BackgroundSyncer) fullSync() {
	// Do a full sync once in 1 second
	allVals, _ := b.kv.Get()
	for _, v := range allVals {
		for _, o := range b.topology.store[b.node.ID()] {
			b.enqueueMsg(SendTo{
				dest: o,
				body: map[string]any{"type": "broadcast", "message": v},
			})
		}
	}
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
		channel:  make(chan SendTo, 10000),
		node:     n,
		kv:       &kv,
		topology: &topology,
	}

	// Turn on background sync
	go bkSyncer.sync()
	// Turn on heartbeat
	go bkSyncer.heartBeat()

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		// Skip them, as other nodes will return broadcast_ok
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// JSON is float
		uniqueMessage := int64(body["message"].(float64))
		if kv.Contains(uniqueMessage) {
			// Message was already seen, skip it
			return nil
		}
		err := kv.Put(uniqueMessage, uniqueMessage)
		if err != nil {
			return err
		}
		for _, o := range topology.store[n.ID()] {
			err = n.Send(o, body)
			if slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
				maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
			}, maelstrom.ErrorCode(err)) {
				// We need to retry as failures are temporary
				bkSyncer.enqueueMsg(SendTo{
					dest: o,
					body: body,
				})
			}

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

		// When topology changes we do a full broadcast
		// Not actually needed as these topology changes are not happening
		allVals, _ := kv.Get()
		for _, v := range allVals {
			for _, o := range topology.store[n.ID()] {
				bkSyncer.enqueueMsg(SendTo{
					dest: o,
					body: map[string]any{"type": "broadcast", "message": v},
				})
			}
		}

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}