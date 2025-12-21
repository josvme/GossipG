package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Counter struct {
	mu sync.Mutex
	x  uint16
}

func (c *Counter) Up() {
	c.mu.Lock()
	c.x += 1
	c.mu.Unlock()
}

func (c *Counter) V() uint16 {
	c.mu.Lock()
	x := c.x
	c.mu.Unlock()
	return x
}
func main() {
	n := maelstrom.NewNode()
	counter := Counter{
		mu: sync.Mutex{},
		x:  0,
	}

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		// Sort of like Snowflake implementation, but UID size is not formatted properly.
		// We don't have to handle overflows as they will rollover
		id := fmt.Sprintf("%s%010d%08d", n.ID(), time.Now().UnixMilli(), counter.V())
		counter.Up()
		body["id"] = id

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
