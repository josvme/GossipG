package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type ILog interface {
	append(msg int) int
	get(offset int) [][]int
	commit(offset int) error
	committedOffset() int
}

type ILogs interface {
	send(key string, msg int) int
	poll(offsets map[string]int) map[string][][]int
	commit(offsets map[string]int) error
	committedOffset(keys []string) map[string]int
}

type Logs struct {
	store map[string]ILog
	mu    sync.Mutex
}

func newLogs() ILogs {
	return &Logs{
		store: make(map[string]ILog),
		mu:    sync.Mutex{},
	}
}

func newLog(name string) ILog {
	/**
	 * Zero offset is considered to be start of topic.
	 * But first offset we write will be 1, So if someone asks for offset from 0, we provide from 1.
	 */
	return &Log{
		name:         name,
		nextOffset:   0,
		commitOffset: 0,
		kv:           make(map[int]int),
		mu:           sync.Mutex{},
	}
}

func (l *Logs) send(key string, msg int) int {
	l.mu.Lock()
	_, ok := l.store[key]
	if !ok {
		l.store[key] = newLog(key)
	}
	off := l.store[key].append(msg)
	l.mu.Unlock()
	return off
}

func (l *Logs) poll(offsets map[string]int) map[string][][]int {
	results := make(map[string][][]int)
	l.mu.Lock()
	for k, o := range offsets {
		if _, ok := l.store[k]; ok {
			results[k] = l.store[k].get(o)
		}
	}
	l.mu.Unlock()
	return results
}

func (l *Logs) commit(offsets map[string]int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, o := range offsets {
		if _, ok := l.store[k]; ok {
			err := l.store[k].commit(o)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Logs) committedOffset(keys []string) map[string]int {
	results := make(map[string]int)
	l.mu.Lock()
	for _, v := range keys {
		if _, ok := l.store[v]; ok {
			results[v] = l.store[v].committedOffset()
		}
	}
	l.mu.Unlock()
	return results
}

type Log struct {
	name         string
	nextOffset   int
	commitOffset int
	kv           map[int]int
	mu           sync.Mutex
}

func (l *Log) append(msg int) int {
	l.mu.Lock()
	l.nextOffset += 1
	l.kv[l.nextOffset] = msg
	l.mu.Unlock()
	return l.nextOffset
}

func (l *Log) get(o int) [][]int {
	offset := min(1, o)
	i := 0
	results := make([][]int, 0)
	l.mu.Lock()
	for v, ok := l.kv[i+offset]; ok; {
		results = append(results, []int{offset + i, v})
		i += 1
		if i > 5 {
			break
		}
	}
	l.mu.Unlock()
	return results
}

func (l *Log) commit(offset int) error {
	l.mu.Lock()
	l.commitOffset = min(offset, l.nextOffset)
	l.mu.Unlock()
	return nil
}

func (l *Log) committedOffset() int {
	l.mu.Lock()
	off := l.commitOffset
	l.mu.Unlock()
	return off
}

func main() {
	n := maelstrom.NewNode()
	logs := newLogs()

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
		offset := logs.send(body.Key, body.Msg)
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
		messages := logs.poll(body.Offsets)
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
		err := logs.commit(body.Offsets)
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
		offset := logs.committedOffset(body.Keys)
		result["offsets"] = offset

		return n.Reply(msg, result)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
