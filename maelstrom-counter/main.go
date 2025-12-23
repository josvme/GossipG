package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const C = "Counter"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Read value from a message
		newValue := int(body["delta"].(float64))
		err := updateCounter(kv, ctx, newValue)

		for slices.Contains([]int{maelstrom.PreconditionFailed, maelstrom.KeyDoesNotExist,
			maelstrom.Timeout, maelstrom.TemporarilyUnavailable, maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			// We need to retry as failures are temporary
			err = updateCounter(kv, ctx, newValue)
		}

		resp := make(map[string]any)
		resp["type"] = "add_ok"

		return n.Reply(msg, resp)
	})

	n.Handle("readone", func(msg maelstrom.Message) error {
		value, err := readCounter(kv, ctx)
		for slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
			maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			// We need to retry as failures are temporary
			value, err = readCounter(kv, ctx)
		}

		resp := make(map[string]any)
		resp["type"] = "readone_ok"
		resp["value"] = value
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req := make(map[string]any)
		req["type"] = "readone"

		// Get value from own replica
		value, err := readCounter(kv, ctx)
		for slices.Contains([]int{maelstrom.Timeout, maelstrom.TemporarilyUnavailable,
			maelstrom.Crash, maelstrom.Abort, maelstrom.TxnConflict,
		}, maelstrom.ErrorCode(err)) {
			// We need to retry as failures are temporary
			value, err = readCounter(kv, ctx)
		}

		for _, v := range n.NodeIDs() {
			ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			if v == n.ID() {
				cancel()
				continue
			}
			var j map[string]any
			reply, err := n.SyncRPC(ctx, v, req)
			fmt.Fprintln(os.Stderr, "reply from msg", req, reply)
			if err != nil {
				cancel()
				continue
			}
			if err := json.Unmarshal(reply.Body, &j); err != nil {
				cancel()
				continue
			}
			value = int(math.Max(j["value"].(float64), float64(value)))
			cancel()
		}

		resp := make(map[string]any)
		resp["type"] = "read_ok"
		resp["value"] = value
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func readCounter(kv *maelstrom.KV, ctx context.Context) (int, error) {
	v, err := kv.ReadInt(ctx, C)
	return v, err
}

func updateCounter(kv *maelstrom.KV, ctx context.Context, delta int) error {
	createIfNotExists := false
	from := 0
	readValue, err := kv.ReadInt(ctx, C)
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		// Key doesn't exist, so create it
		createIfNotExists = true
	} else {
		// Key exists, so try CAS on current value
		from = readValue
	}
	err = kv.CompareAndSwap(ctx, C, from, readValue+delta, createIfNotExists)
	return err
}
