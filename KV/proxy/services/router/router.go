package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"sync"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// Node is one SGLang server as the proxy sees it.
// ID is the identifier used across the route table, the health table and the
// pull headers. Two nodes on one machine cannot share a ZMQ port, so each
// node carries its own addresses.
type Node struct {
	ID         string
	KVAddr     string // host:port of the node's KV events PUB socket
	ReplayAddr string // host:port of the node's replay socket (optional, for missed-batch replay)
}

type RouteTable struct {
	routeMapCPU *cpuMap
	routeMapGPU *gpuMap
	nodes       []Node
	// seqMap is only written by Listen's single consumer goroutine, no lock needed
	seqMap map[string]uint64 // tracks the last seen seq kv event per node
}

type cpuMap struct {
	mx    sync.RWMutex
	table map[int64]map[string]struct{} // maps hash to the set of nodes holding it in CPU
}

type gpuMap struct {
	mx    sync.RWMutex
	table map[int64]map[string]struct{} // maps hash to the set of nodes holding it in GPU
}

func NewRouteTableService(nodes []Node) *RouteTable {
	c := make(map[int64]map[string]struct{})
	g := make(map[int64]map[string]struct{})
	seq := make(map[string]uint64)
	return &RouteTable{routeMapCPU: &cpuMap{table: c},
		routeMapGPU: &gpuMap{table: g},
		nodes:       nodes, seqMap: seq}
}

func (table *RouteTable) writeCache(batch *KVEventBatch) {
	// idk what to do with the other params in this batch res
	for _, event := range batch.events {
		switch event.Type {
		case "BlockStored":
			// 	{
			// 		"type": "BlockStored",
			// 		"block_hashes": [int, ...],       # list of int64 hashes
			// 		"parent_block_hash": int | None,  # hash of the preceding block
			// 		"token_ids": [int, ...],          # actual token IDs that fill this block
			// 		"block_size": int,                # tokens per block
			// 		"lora_id": int | None,
			// 		"medium": "GPU" | "CPU_PINNED" | "DISK" | "EXTERNAL" | None,
			// 		"cache_salt": str | None,
			// 		"session_id": str | None,
			// 		"lora_name": str | None,          # only in "dynamo" format
			// 	}
			medium, ok := event.Fields["medium"].(string)
			if !ok {
				continue
			}
			hashes, ok := eventHashes(event)
			if !ok {
				continue
			}
			switch medium {
			case "GPU":
				table.routeMapGPU.mx.Lock()
				addNodeToHashes(table.routeMapGPU.table, hashes, batch.nodeIP)
				table.routeMapGPU.mx.Unlock()
			case "CPU_PINNED":
				table.routeMapCPU.mx.Lock()
				addNodeToHashes(table.routeMapCPU.table, hashes, batch.nodeIP)
				table.routeMapCPU.mx.Unlock()
			default:
				// DISK / EXTERNAL / None are not tracked
				continue
			}
		case "BlockRemoved":
			// 	{
			// 	  "type": "BlockRemoved",
			// 	  "block_hashes": [int, ...],
			// 	  "medium" : "GPU" | "CPU_PINNED" | ... | None,
			// 	}
			medium, ok := event.Fields["medium"].(string)
			if !ok {
				continue
			}
			hashes, ok := eventHashes(event)
			if !ok {
				continue
			}
			switch medium {
			case "GPU":
				table.routeMapGPU.mx.Lock()
				removeNodeFromHashes(table.routeMapGPU.table, hashes, batch.nodeIP)
				table.routeMapGPU.mx.Unlock()
			case "CPU_PINNED":
				table.routeMapCPU.mx.Lock()
				removeNodeFromHashes(table.routeMapCPU.table, hashes, batch.nodeIP)
				table.routeMapCPU.mx.Unlock()
			default:
				continue
			}
		default:
			// 	{"type": "AllBlocksCleared"}
			// remove this node from both tables
			table.routeMapCPU.mx.Lock()
			removeNodeFromAllHashes(table.routeMapCPU.table, batch.nodeIP)
			table.routeMapCPU.mx.Unlock()

			table.routeMapGPU.mx.Lock()
			removeNodeFromAllHashes(table.routeMapGPU.table, batch.nodeIP)
			table.routeMapGPU.mx.Unlock()
		}
	}

}

// eventHashes pulls the block_hashes list out of a raw event. msgpack decodes
// every array as []any, so each element is converted with toInt64.
func eventHashes(event rawKVEvent) ([]int64, bool) {
	raw, ok := event.Fields["block_hashes"].([]any)
	if !ok {
		return nil, false
	}
	hashes := make([]int64, 0, len(raw))
	for _, v := range raw {
		n, ok := toInt64(v)
		if !ok {
			return nil, false
		}
		hashes = append(hashes, n)
	}
	return hashes, true
}

func addNodeToHashes(table map[int64]map[string]struct{}, hashes []int64, node string) {
	for _, hash := range hashes {
		nodes, ok := table[hash]
		if !ok {
			nodes = make(map[string]struct{})
			table[hash] = nodes
		}
		nodes[node] = struct{}{}
	}
}

func removeNodeFromHashes(table map[int64]map[string]struct{}, hashes []int64, node string) {
	for _, hash := range hashes {
		nodes, ok := table[hash]
		if !ok {
			continue
		}
		delete(nodes, node)
		if len(nodes) == 0 {
			delete(table, hash)
		}
	}
}

func removeNodeFromAllHashes(table map[int64]map[string]struct{}, node string) {
	for hash, nodes := range table {
		delete(nodes, node)
		if len(nodes) == 0 {
			delete(table, hash)
		}
	}
}

func (table *RouteTable) ReadCache(ctx context.Context, hash int64) map[string]struct{} {
	// reads the cache for this hash and returns the set of nodes if it exists in the table
	// check both GPU and CPU; only a miss in both is a cache miss

	table.routeMapCPU.mx.RLock()
	node, ok := table.routeMapCPU.table[hash]
	table.routeMapCPU.mx.RUnlock()
	if !ok {
		// check the GPU
		table.routeMapGPU.mx.RLock()
		node, ok = table.routeMapGPU.table[hash]
		table.routeMapGPU.mx.RUnlock()
		if !ok {
			return nil
		}
	}
	// return a copy here since we dont need to modify it anymore
	n := maps.Clone(node)
	return n
}

type rawKVEvent struct {
	Type   string // "BlockStored", "BlockRemoved", "AllBlocksCleared"
	Fields map[string]any
}

type KVEventBatch struct {
	// the batched event
	seq        uint64
	ts         float64
	attnDPRank *int64
	events     []rawKVEvent
	error      error
	nodeIP     string
}

func (table *RouteTable) Listen(ctx context.Context) {
	// one channel that all the per-node listeners write to,
	// and this one routine reads from it
	ch := make(chan *KVEventBatch, 1024) // NOTE: idk about this exact buffer size

	for _, n := range table.nodes {
		// have all of them write to ch
		go listenNode(ctx, n.KVAddr, n.ID, ch)
	}

	for {
		select {
		case event := <-ch:
			if event.error != nil {
				// the per-node listener died. TODO: restart it
				slog.Error("KV event listener failed", "node", event.nodeIP, "error", event.error.Error())
				continue
			}
			prevSeq, seen := table.seqMap[event.nodeIP]
			if seen && event.seq != prevSeq+1 {
				// we need to read the missed batches through the node's replay endpoint
				// TODO: do that
				// but for now, log
				slog.Info("KV event missed", "node", event.nodeIP, "missed_seq", prevSeq+1)
			}
			table.seqMap[event.nodeIP] = event.seq
			table.writeCache(event)
		case <-ctx.Done():
			return
		}
	}
}

func listenNode(ctx context.Context, addr string, node string, ch chan<- *KVEventBatch) {
	// listens to the KV events endpoint of one node
	sock := zmq.NewSub(ctx)
	defer sock.Close()

	if err := sock.SetOption(zmq.OptionSubscribe, ""); err != nil {
		ch <- &KVEventBatch{nodeIP: node, error: fmt.Errorf("zmq subscribe: %w", err)}
		return
	}

	if err := sock.Dial("tcp://" + addr); err != nil {
		ch <- &KVEventBatch{nodeIP: node, error: fmt.Errorf("zmq dial %s: %w", addr, err)}
		return
	}

	for {
		msg, err := sock.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				ch <- &KVEventBatch{nodeIP: node, error: fmt.Errorf("zmq recv: %w", err)}
				return
			}
		}
		if len(msg.Frames) != 3 {
			continue // malformed, skip
		}

		seq := binary.BigEndian.Uint64(msg.Frames[1])
		batch := decodeBatch(seq, msg.Frames[2])
		batch.nodeIP = node
		if batch.error != nil {
			slog.Warn("bad KV event payload", "node", node, "error", batch.error.Error())
			continue // bad msgpack frame, skip and keep going
		}

		select {
		case ch <- batch:
		case <-ctx.Done():
			return
		}
	}

}

func decodeBatch(seq uint64, payload []byte) *KVEventBatch {
	// NOTE: keep array_like=True on the python side
	// array [ts:float64, events:[]any, attn_dp_rank:int|nil]
	var raw []any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		return &KVEventBatch{error: err}
	}
	if len(raw) < 2 {
		err := fmt.Errorf("batch too short: %d fields", len(raw))
		return &KVEventBatch{error: err}
	}

	ts, _ := raw[0].(float64)

	var dpRank *int64
	if len(raw) >= 3 && raw[2] != nil {
		v, ok := toInt64(raw[2])
		if !ok {
			err := fmt.Errorf("couldnt type convert")
			return &KVEventBatch{error: err}
		}
		dpRank = &v
	}

	rawEvents, _ := raw[1].([]any)
	events := make([]rawKVEvent, 0, len(rawEvents))
	for _, re := range rawEvents {
		m, ok := re.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := m["type"].(string)
		delete(m, "type")
		events = append(events, rawKVEvent{Type: typ, Fields: m})
	}

	return &KVEventBatch{seq: seq, ts: ts, attnDPRank: dpRank, events: events, error: nil}
}

func toInt64(v any) (int64, bool) {
	// to change diff int type to 64
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	}
	return 0, false
}
