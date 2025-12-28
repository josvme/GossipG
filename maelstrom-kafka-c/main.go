package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sort"
	"strconv"

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

func getResponsibleNode(key string, n *maelstrom.Node) string {
	nodes := n.NodeIDs()
	slices.Sort(nodes)
	keyInt, err := strconv.Atoi(key)
	if err != nil {
		return nodes[0]
	}
	return nodes[keyInt%len(nodes)]
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

		if getResponsibleNode(body.Key, n) != n.ID() {
			resp, err := n.SyncRPC(ctx, getResponsibleNode(body.Key, n), map[string]any{
				"type":   "internal",
				"method": "send",
				"key":    body.Key,
				"msg":    body.Msg,
			})
			if err != nil {
				return err
			}
			return n.Reply(msg, resp.Body)
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
			if getResponsibleNode(k, n) == n.ID() {
				msgs, err := logs.dataStore.get(ctx, k, v)
				if err != nil {
					return err
				}
				out[k] = msgs
			} else {
				resp, err := n.SyncRPC(ctx, getResponsibleNode(k, n), map[string]any{
					"type":   "internal",
					"method": "poll",
					"key":    k,
					"offset": v,
				})
				if err != nil {
					return err
				}
				var internalResp struct {
					Msgs [][]int `json:"msgs"`
				}
				if err := json.Unmarshal(resp.Body, &internalResp); err != nil {
					return err
				}
				out[k] = internalResp.Msgs
			}
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
			if getResponsibleNode(k, n) == n.ID() {
				err := logs.offsetStore.commit(ctx, k, v)
				if err != nil {
					return err
				}
			} else {
				_, err := n.SyncRPC(ctx, getResponsibleNode(k, n), map[string]any{
					"type":   "internal",
					"method": "commit_offsets",
					"key":    k,
					"offset": v,
				})
				if err != nil {
					return err
				}
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
			if getResponsibleNode(v, n) == n.ID() {
				off, err := logs.offsetStore.get(ctx, v)
				if err != nil {
					return err
				}
				offsets[v] = off
			} else {
				resp, err := n.SyncRPC(ctx, getResponsibleNode(v, n), map[string]any{
					"type":   "internal",
					"method": "list_committed_offsets",
					"key":    v,
				})
				if err != nil {
					return err
				}
				var internalResp struct {
					Offset int `json:"offset"`
				}
				if err := json.Unmarshal(resp.Body, &internalResp); err != nil {
					return err
				}
				offsets[v] = internalResp.Offset
			}
		}
		result["offsets"] = offsets
		return n.Reply(msg, result)
	})

	n.Handle("internal", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		method := body["method"].(string)
		switch method {
		case "send":
			key := body["key"].(string)
			msgVal := int(body["msg"].(float64))
			offset, err := logs.id.Next(ctx, key)
			if err != nil {
				return err
			}
			err = logs.dataStore.save(ctx, key, offset, msgVal)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
		case "poll":
			key := body["key"].(string)
			offset := int(body["offset"].(float64))
			msgs, err := logs.dataStore.get(ctx, key, offset)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
		case "commit_offsets":
			key := body["key"].(string)
			offset := int(body["offset"].(float64))
			err := logs.offsetStore.commit(ctx, key, offset)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
		case "list_committed_offsets":
			key := body["key"].(string)
			off, err := logs.offsetStore.get(ctx, key)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offset": off})
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
