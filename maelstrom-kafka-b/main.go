package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"strconv"
	"sync"
)

type ILogs interface {
	send(ctx context.Context, key string, msg int) int
	poll(ctx context.Context, offsets map[string]int) map[string][][]int
	commit(ctx context.Context, offsets map[string]int) error
	committedOffset(ctx context.Context, keys []string) map[string]int
}

type Log struct {
	nextOffset      int
	committedOffset int
	name            string
}
type Logs struct {
	store *maelstrom.KV
	mu    sync.Mutex
	index map[string]*Log
}

func (l *Logs) send(ctx context.Context, key string, msg int) int {
	l.mu.Lock()
	_, ok := l.index[key]
	if !ok {
		l.index[key] = &Log{
			nextOffset:      0,
			committedOffset: 0,
			name:            key,
		}
	}
	err := l.store.CompareAndSwap(ctx, key+strconv.Itoa(l.index[key].nextOffset+1), nil, msg, true)

	// retry logic
	for slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
		maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
	}, maelstrom.ErrorCode(err)) {
		err = l.store.CompareAndSwap(ctx, key+strconv.Itoa(l.index[key].nextOffset+1), nil, msg, true)
	}
	l.index[key].nextOffset += 1
	l.mu.Unlock()
	return l.index[key].nextOffset
}

func (l *Logs) poll(ctx context.Context, offsets map[string]int) map[string][][]int {
	results := make(map[string][][]int)
	l.mu.Lock()
	for k, o := range offsets {
		if _, ok := l.index[k]; ok {
			tmp := make([][]int, 0)
			for i := 0; i < 5; i++ {
				v, err := l.store.ReadInt(ctx, k+strconv.Itoa(o+i))
				// retry logic
				for slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
					maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
				}, maelstrom.ErrorCode(err)) {
					v, err = l.store.ReadInt(ctx, k+strconv.Itoa(o+i))
				}
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					break
				}
				tmp = append(tmp, []int{o + i, v})
			}
			results[k] = tmp
		}
	}
	l.mu.Unlock()
	return results
}

func (l *Logs) commit(ctx context.Context, offsets map[string]int) error {
	l.mu.Lock()
	for k, o := range offsets {
		if _, ok := l.index[k]; ok {
			l.index[k].committedOffset = min(l.index[k].nextOffset, o)
		}
	}
	defer l.mu.Unlock()
	return nil
}

func (l *Logs) committedOffset(ctx context.Context, keys []string) map[string]int {
	results := make(map[string]int)
	l.mu.Lock()
	for _, v := range keys {
		if _, ok := l.index[v]; ok {
			results[v] = l.index[v].committedOffset
		}
	}
	l.mu.Unlock()
	return results
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ctx := context.Background()
	var logs ILogs = &Logs{
		store: kv,
		mu:    sync.Mutex{},
		index: make(map[string]*Log),
	}

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
		offset := logs.send(ctx, body.Key, body.Msg)
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
		messages := logs.poll(ctx, body.Offsets)
		result["msgs"] = messages
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
		err := logs.commit(ctx, body.Offsets)
		if err != nil {
			return err
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
		offset := logs.committedOffset(ctx, body.Keys)
		result["offsets"] = offset

		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
