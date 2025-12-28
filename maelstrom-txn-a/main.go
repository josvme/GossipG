package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[int]int
}

func (s *Store) processTxns(txs [][3]any) [][3]any {
	for _, t := range txs {
		if t[0] == "r" {
			t[2] = s.read(int(t[1].(float64)))
		}
		if t[0] == "w" {
			s.write(int(t[1].(float64)), int(t[2].(float64)))
		}
	}
	return txs
}

func (s *Store) read(k int) any {
	s.mu.RLock()
	var r any = nil
	if v, ok := s.data[k]; ok {
		r = v
	}
	s.mu.RUnlock()
	return r
}

func (s *Store) write(k, v int) {
	s.mu.Lock()
	s.data[k] = v
	s.mu.Unlock()
}

func main() {
	n := maelstrom.NewNode()
	s := Store{
		mu:   sync.RWMutex{},
		data: make(map[int]int),
	}

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

		s.processTxns(body.Txn)
		body.Typ = "txn_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
