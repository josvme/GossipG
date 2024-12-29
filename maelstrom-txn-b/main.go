package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
)

type Store struct {
	mu    sync.RWMutex
	data  map[int]int
	nodes []string
}

func (s *Store) read(k int) any {
	var r any = nil
	if v, ok := s.data[k]; ok {
		r = v
	}
	return r
}

func (s *Store) write(k, v int) {
	s.data[k] = v
}

func (s *Store) processTxns(txs [][3]any) [][3]any {
	s.mu.Lock()
	for _, t := range txs {
		if t[0] == "s" {
			t[2] = s.read(int(t[1].(float64)))
		}
		if t[0] == "w" {
			s.write(int(t[1].(float64)), int(t[2].(float64)))
		}
	}
	s.mu.Unlock()
	return txs
}

func main() {
	n := maelstrom.NewNode()
	neighbourNodes := make([]string, 0)
	ctx := context.Background()

	for _, v := range n.NodeIDs() {
		if v == n.ID() {
			continue
		}
		neighbourNodes = append(neighbourNodes, v)
	}

	s := Store{
		mu:    sync.RWMutex{},
		data:  make(map[int]int),
		nodes: neighbourNodes,
	}

	n.Handle("replicate", func(msg maelstrom.Message) error {
		type Txn struct {
			Typ   string   `json:"type"`
			MsgId int      `json:"msg_id"`
			Txn   [][3]any `json:"txn"`
		}

		var body Txn
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.processTxns(body.Txn)
		body.Typ = "replicate_ok"

		return n.Reply(msg, body)
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		type Txn struct {
			Typ   string   `json:"type"`
			MsgId int      `json:"msg_id"`
			Txn   [][3]any `json:"txn"`
		}

		var body Txn
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// process transaction
		s.processTxns(body.Txn)

		// Replicate to all other nodes
		// We actually don't want to send the reads
		go func() {
			repl := body
			repl.Typ = "replicate"
			for _, node := range neighbourNodes {
				_, err := n.SyncRPC(ctx, node, repl)
				for slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
					maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
				}, maelstrom.ErrorCode(err)) {
					_, err = n.SyncRPC(ctx, node, repl)
				}
			}
		}()
		body.Typ = "txn_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
