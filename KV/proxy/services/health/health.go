package health

import (
	"context"
	"sync"
	"time"
)

type HealthTable struct {
	mx    sync.RWMutex
	table map[string]*NodeInfo
	nodes []string
}

type NodeInfo struct {
	// lol idk stuff
	Healthy   bool
	IP        string
	timestamp time.Time
}

type HealthyNodes struct {
	SortedNodes []NodeInfo
	NodesMap    map[string]struct{}
}

func NewHealthTable(nodes []string) *HealthTable {
	t := make(map[string]*NodeInfo)
	return &HealthTable{table: t, nodes: nodes}
}

func (table *HealthTable) CheckHealth(ctx context.Context) {
	// runs in the background, listens and updates table
	//
	// Design: subscribe to each node's load-publish ZMQ socket (LoadStat:
	// num_running_reqs, num_waiting_reqs, num_tokens, max_total_num_tokens).
	// alive  = saw a stat within the last N seconds
	// loaded = alive but over the configured load threshold
	// GetHealthyNodes sorts by load score, e.g. running + w*waiting.

}
func (table *HealthTable) GetHealthyNodes(ctx context.Context) HealthyNodes {
	// runs on demand, sorts based on algo and returns healthy nodes in sorted order
	// have this handle locks and return a copy
	table.mx.RLock()
	defer table.mx.RUnlock()
	return HealthyNodes{}
}
