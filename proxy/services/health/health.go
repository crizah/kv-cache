package health

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"sync"
	"time"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/crizah/KV/proxy/services/node"
)

// Status is the health verdict for a node.
//   - StatusUnhealthy: the node errored or has gone quiet. Never a route
//     target, never a pull source.
//   - StatusLoaded: alive, but over the configured load threshold. Not a
//     route target, but still a valid pull source.
//   - StatusHealthy: alive and under the threshold. Valid route target.
type Status int

const (
	StatusUnhealthy Status = iota
	StatusLoaded
	StatusHealthy
)

func (s Status) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusLoaded:
		return "loaded"
	default:
		return "unhealthy"
	}
}

// staleAfter is how long a table entry stays valid. A node whose last update
// is older than this is treated as unhealthy when read.
const staleAfter = 30 * time.Second

// Config holds the knobs for the load score.
// A node is marked StatusLoaded when EITHER:
//   - running + WaitingWeight×waiting > LoadedThreshold  (catches short prefixes with many batched reqs)
//   - in-flight tokens >= TokenLoadedThreshold           (catches long prefixes where few reqs dominate)
type Config struct {
	WaitingWeight         float64
	LoadedThreshold       float64
	TokenLoadedThreshold  int64
}

func loadScore(running, waiting int64, waitingWeight float64) float64 {
	return float64(running) + waitingWeight*float64(waiting)
}

type HealthTable struct {
	mx              sync.RWMutex
	table           map[string]*NodeInfo
	nodes           []node.Node
	waitingWeight   float64
	loadedThreshold      float64
	tokenLoadedThreshold int64
}

type NodeInfo struct {
	node.Node   // embedded: ID, AgentURL, KVEventsAddr, LoadAddr
	Status      Status
	RunningReqs int64
	WaitingReqs int64
	Tokens      int64
	MaxTokens   int64
	timestamp   time.Time // last time we heard from this node
}

// this only contains node with status StatusHealthy StatusLoaded and all loaded after all status
type AvailableNodes struct {
	SortedNodes []NodeInfo        // healthy and loaded nodes, least loaded first
	NodesMap    map[string]Status // node ID -> status
}

func NewHealthTable(nodes []node.Node, cfg Config) *HealthTable {
	t := make(map[string]*NodeInfo, len(nodes))
	for _, n := range nodes {
		// nodes start unhealthy until their first LoadStat arrives
		t[n.ID] = &NodeInfo{Node: n, Status: StatusUnhealthy}
	}
	return &HealthTable{
		table:                t,
		nodes:                nodes,
		waitingWeight:        cfg.WaitingWeight,
		loadedThreshold:      cfg.LoadedThreshold,
		tokenLoadedThreshold: cfg.TokenLoadedThreshold,
	}
}

// LoadStat is the node's published load, one message per tick.
type LoadStat struct {
	seq         uint64
	RunningReqs int64
	WaitingReqs int64
	Tokens      int64
	MaxTokens   int64
}

// loadEvent is what one node's listener goroutine sends to the shared
// channel: either a stat or the error that killed the listener.
type loadEvent struct {
	node string
	stat *LoadStat
	err  error
}

// CheckHealth runs in the background: every node's listener writes to one
// channel, and this one routine reads from it and updates the table (similar to the kv event listener)
func (table *HealthTable) CheckHealth(ctx context.Context) {
	ch := make(chan loadEvent, 128)

	for _, n := range table.nodes {
		go listenNode(ctx, n.LoadAddr, n.ID, ch)
	}

	for {
		select {
		case ev := <-ch:
			if ev.err != nil {
				// the per-node listener died. TODO: restart it
				slog.Error("load listener failed", "node", ev.node, "error", ev.err.Error())
				table.markUnhealthy(ev.node)
				continue
			}
			table.record(ev.node, ev.stat)
		case <-ctx.Done():
			return
		}
	}
}

// record writes the latest stat for a node and recomputes its status.
func (table *HealthTable) record(node string, stat *LoadStat) {
	table.mx.Lock()
	defer table.mx.Unlock()
	info, ok := table.table[node]
	if !ok {
		return // unknown node, shouldnt happen
	}
	prev := info.Status
	info.RunningReqs = stat.RunningReqs
	info.WaitingReqs = stat.WaitingReqs
	info.Tokens = stat.Tokens
	info.MaxTokens = stat.MaxTokens
	info.Status = StatusHealthy
	score := loadScore(stat.RunningReqs, stat.WaitingReqs, table.waitingWeight)
	if score > table.loadedThreshold {
		info.Status = StatusLoaded
	}
	if info.Status == StatusHealthy && table.tokenLoadedThreshold > 0 && stat.Tokens >= table.tokenLoadedThreshold {
		info.Status = StatusLoaded
	}
	info.timestamp = time.Now()
	slog.Info("health", "node", node, "status", info.Status,
		"running", stat.RunningReqs, "waiting", stat.WaitingReqs,
		"tokens", stat.Tokens, "score", score,
		"changed", prev != info.Status)
}

func (table *HealthTable) LLRoute() *node.RouterRes {
	// gives us the least load node
	table.mx.RLock()
	defer table.mx.RUnlock()

	// just get the one with the least running req
	minLoad := math.MaxInt64
	var n node.Node
	for _, l := range table.table {
		if int(l.RunningReqs) <= minLoad {
			minLoad = int(l.RunningReqs)
			n = l.Node
		}
	}
	return &node.RouterRes{NodeD: n}
}

func (table *HealthTable) markUnhealthy(node string) {
	table.mx.Lock()
	defer table.mx.Unlock()
	info, ok := table.table[node]
	if !ok {
		return
	}
	info.Status = StatusUnhealthy
	info.timestamp = time.Now()
}

// runs on demand: marks stale entries unhealthy, then returns
// every alive node (healthy and loaded) as a status map and sorted least
// loaded first (healthy always come before loaded)
// NOTE: this is the ONLY guy that marks the unhealthy nodes (>5min) as unhealthy lazily
func (table *HealthTable) GetHealthyNodes(ctx context.Context) AvailableNodes {
	table.mx.Lock()
	defer table.mx.Unlock()

	now := time.Now()
	sorted := make([]NodeInfo, 0, len(table.table))
	nodesMap := make(map[string]Status, len(table.table))

	for _, info := range table.table {
		if now.Sub(info.timestamp) > staleAfter {
			info.Status = StatusUnhealthy
		}
		if info.Status == StatusUnhealthy {
			continue
		}
		sorted = append(sorted, *info)
		nodesMap[info.ID] = info.Status
	}

	slices.SortFunc(sorted, func(a, b NodeInfo) int {
		scoreA := loadScore(a.RunningReqs, a.WaitingReqs, table.waitingWeight)
		scoreB := loadScore(b.RunningReqs, b.WaitingReqs, table.waitingWeight)
		return cmp.Compare(scoreA, scoreB)
	})

	return AvailableNodes{SortedNodes: sorted, NodesMap: nodesMap}
}

func listenNode(ctx context.Context, addr string, node string, ch chan<- loadEvent) {
	// listens to the load-publish endpoint of one node
	sock := zmq.NewSub(ctx)
	defer sock.Close()

	if err := sock.SetOption(zmq.OptionSubscribe, ""); err != nil {
		ch <- loadEvent{node: node, err: fmt.Errorf("zmq subscribe: %w", err)}
		return
	}

	if err := sock.Dial("tcp://" + addr); err != nil {
		ch <- loadEvent{node: node, err: fmt.Errorf("zmq dial %s: %w", addr, err)}
		return
	}

	for {
		msg, err := sock.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				ch <- loadEvent{node: node, err: fmt.Errorf("zmq recv: %w", err)}
				return
			}
		}
		if len(msg.Frames) != 3 {
			continue // malformed, skip
		}

		stat, err := decodeLoadStat(msg.Frames[2])
		if err != nil {
			slog.Warn("bad loadstat payload", "node", node, "error", err.Error())
			continue // bad msgpack frame, skip and keep going
		}
		stat.seq = binary.BigEndian.Uint64(msg.Frames[1])

		select {
		case ch <- loadEvent{node: node, stat: stat}:
		case <-ctx.Done():
			return
		}
	}
}

// decodeLoadStat decodes the msgpack LoadStat payload.
// Assumed layout, TODO: VERIFY against SGLang docs:
// ["LoadStat", num_running_reqs, num_waiting_reqs, num_tokens, max_total_num_tokens, attn_dp_rank, ...]
func decodeLoadStat(payload []byte) (*LoadStat, error) {
	var raw []any
	if err := msgpack.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}
	if len(raw) < 5 {
		return nil, fmt.Errorf("loadstat too short: %d fields", len(raw))
	}
	tag, ok := raw[0].(string)
	if !ok || tag != "LoadStat" {
		return nil, fmt.Errorf("unexpected loadstat tag: %v", raw[0])
	}
	running, ok := toInt64(raw[1])
	if !ok {
		return nil, fmt.Errorf("bad num_running_reqs: %v", raw[1])
	}
	waiting, ok := toInt64(raw[2])
	if !ok {
		return nil, fmt.Errorf("bad num_waiting_reqs: %v", raw[2])
	}
	tokens, ok := toInt64(raw[3])
	if !ok {
		return nil, fmt.Errorf("bad num_tokens: %v", raw[3])
	}
	maxTokens, ok := toInt64(raw[4])
	if !ok {
		return nil, fmt.Errorf("bad max_total_num_tokens: %v", raw[4])
	}
	return &LoadStat{RunningReqs: running, WaitingReqs: waiting, Tokens: tokens, MaxTokens: maxTokens}, nil
}

func toInt64(v any) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	}
	return 0, false
}
