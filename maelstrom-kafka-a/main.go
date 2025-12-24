package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type UniqueIdGenerator interface {
	Next() int
}

type InMemoryUniqueIdGenerator struct {
	mu sync.RWMutex
	id int
}

func (i *InMemoryUniqueIdGenerator) Next() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.id++
	return i.id
}

type DataStore interface {
	save(key string, offset int, value int)
	get(key string, offset int) [][]int
}

type LogStore struct {
	offsetStore OffsetStore
	dataStore   DataStore
	mu          sync.RWMutex
	id          UniqueIdGenerator
}

type InMemoryDataStore struct {
	mu    sync.RWMutex
	store map[string][][]int
}

func (i *InMemoryDataStore) save(key string, offset int, value int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.store[key] = append(i.store[key], []int{offset, value})
}

func (i *InMemoryDataStore) get(key string, offset int) [][]int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	vals := i.store[key]
	out := make([][]int, 0)
	for _, v := range vals {
		if v[0] >= offset {
			out = append(out, v)
		}
	}
	return out
}

type OffsetStore interface {
	commit(key string, offset int)
	get(key string) int
}

type InMemoryOffsetStore struct {
	store map[string]int
	mu    sync.RWMutex
}

func (o *InMemoryOffsetStore) commit(key string, offset int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.store[key] = offset
}

func (o *InMemoryOffsetStore) get(key string) int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.store[key]
}

func newLogStore() *LogStore {
	return &LogStore{
		offsetStore: &InMemoryOffsetStore{store: make(map[string]int)},
		dataStore:   &InMemoryDataStore{store: make(map[string][][]int)},
		mu:          sync.RWMutex{},
		id:          &InMemoryUniqueIdGenerator{mu: sync.RWMutex{}, id: 0},
	}
}

func main() {
	n := maelstrom.NewNode()
	logs := newLogStore()

	n.Handle("send", func(msg maelstrom.Message) error {
		type Send struct {
			Typ string `json:"type"`
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		var body Send
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]any)
		result["type"] = "send_ok"
		offset := logs.id.Next()
		logs.dataStore.save(body.Key, offset, body.Msg)
		result["offset"] = offset

		return n.Reply(msg, result)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		type Poll struct {
			Typ     string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}

		var body Poll
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]any)
		result["type"] = "poll_ok"
		out := make(map[string][][]int)
		for k, v := range body.Offsets {
			out[k] = logs.dataStore.get(k, v)
		}
		result["msgs"] = out
		return n.Reply(msg, result)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		type CommitOffsets struct {
			Typ     string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}

		var body CommitOffsets
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]any)
		result["type"] = "commit_offsets_ok"

		for k, v := range body.Offsets {
			logs.offsetStore.commit(k, v)
		}
		return n.Reply(msg, result)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		type ListCommittedOffsets struct {
			Typ  string   `json:"type"`
			Keys []string `json:"keys"`
		}
		var body ListCommittedOffsets
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result := make(map[string]any)
		result["type"] = "list_committed_offsets_ok"
		offset := make(map[string]int)
		for _, v := range body.Keys {
			offset[v] = logs.offsetStore.get(v)
		}
		result["offsets"] = offset
		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
