package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sort"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type UniqueIdGenerator interface {
	Next(ctx context.Context, key string) (int, error)
}

type KVUniqueIdGenerator struct {
	kv *maelstrom.KV
}

func (i *KVUniqueIdGenerator) Next(ctx context.Context, key string) (int, error) {
	idKey := "id_" + key
	for {
		val, err := i.kv.ReadInt(ctx, idKey)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return 0, err
		}

		newVal := val + 1
		err = i.kv.CompareAndSwap(ctx, idKey, val, newVal, maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist)
		if err == nil {
			return newVal, nil
		}

		if !slices.Contains([]int{maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist,
			maelstrom.Timeout, maelstrom.TemporarilyUnavailable, maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			return 0, err
		}
	}
}

type DataStore interface {
	save(ctx context.Context, key string, offset int, value int) error
	get(ctx context.Context, key string, offset int) ([][]int, error)
}

type LogStore struct {
	offsetStore OffsetStore
	dataStore   DataStore
	id          UniqueIdGenerator
}

type KVDataStore struct {
	kv *maelstrom.KV
}

func (i *KVDataStore) save(ctx context.Context, key string, offset int, value int) error {
	dataKey := "data_" + key
	for {
		var data [][]int
		err := i.kv.ReadInto(ctx, dataKey, &data)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}

		newData := append(data, []int{offset, value})
		sort.Slice(newData, func(i, j int) bool {
			return newData[i][0] < newData[j][0]
		})
		err = i.kv.CompareAndSwap(ctx, dataKey, data, newData, maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist)
		if err == nil {
			return nil
		}

		if !slices.Contains([]int{maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist,
			maelstrom.Timeout, maelstrom.TemporarilyUnavailable, maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			return err
		}
	}
}

func (i *KVDataStore) get(ctx context.Context, key string, offset int) ([][]int, error) {
	dataKey := "data_" + key
	var vals [][]int
	err := i.kv.ReadInto(ctx, dataKey, &vals)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return [][]int{}, nil
		}
		return nil, err
	}
	out := make([][]int, 0)
	for _, v := range vals {
		if v[0] >= offset {
			out = append(out, v)
		}
	}
	return out, nil
}

type OffsetStore interface {
	commit(ctx context.Context, key string, offset int) error
	get(ctx context.Context, key string) (int, error)
}

type KVOffsetStore struct {
	kv *maelstrom.KV
}

func (o *KVOffsetStore) commit(ctx context.Context, key string, offset int) error {
	for {
		offsets := make(map[string]int)
		err := o.kv.ReadInto(ctx, "offset", &offsets)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}

		newOffsets := make(map[string]int)
		for k, v := range offsets {
			newOffsets[k] = v
		}
		newOffsets[key] = offset

		err = o.kv.CompareAndSwap(ctx, "offset", offsets, newOffsets, maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist)
		if err == nil {
			return nil
		}

		if !slices.Contains([]int{maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist,
			maelstrom.Timeout, maelstrom.TemporarilyUnavailable, maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			return err
		}
	}
}

func (o *KVOffsetStore) get(ctx context.Context, key string) (int, error) {
	var offsets map[string]int
	err := o.kv.ReadInto(ctx, "offset", &offsets)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, nil
		}
		return 0, err
	}
	return offsets[key], nil
}

func newLogStore(kv *maelstrom.KV) *LogStore {
	return &LogStore{
		offsetStore: &KVOffsetStore{kv: kv},
		dataStore:   &KVDataStore{kv: kv},
		id:          &KVUniqueIdGenerator{kv: kv},
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	logs := newLogStore(kv)
	ctx := context.Background()

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
		offset, err := logs.id.Next(ctx, body.Key)
		if err != nil {
			return err
		}
		err = logs.dataStore.save(ctx, body.Key, offset, body.Msg)
		if err != nil {
			return err
		}
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
			msgs, err := logs.dataStore.get(ctx, k, v)
			if err != nil {
				return err
			}
			out[k] = msgs
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
			err := logs.offsetStore.commit(ctx, k, v)
			if err != nil {
				return err
			}
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
		offsets := make(map[string]int)
		for _, v := range body.Keys {
			off, err := logs.offsetStore.get(ctx, v)
			if err != nil {
				return err
			}
			offsets[v] = off
		}
		result["offsets"] = offsets
		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
