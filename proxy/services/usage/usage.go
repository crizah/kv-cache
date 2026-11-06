package usage

import (
	"bufio"
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/crizah/KV/proxy/services/node"
)

const pollInterval = 5 * time.Second

type UsageInfo struct {
	KVCacheMemoryGB       float64
	WeightMemoryGB        float64
	StartupAvailableMemGB float64
	CacheHitRate          float64
	timestamp             time.Time
}

type UsageTable struct {
	mx    sync.RWMutex
	table map[string]*UsageInfo
	nodes []node.Node
}

func NewUsageTable(nodes []node.Node) *UsageTable {
	t := make(map[string]*UsageInfo, len(nodes))
	for _, n := range nodes {
		t[n.ID] = &UsageInfo{}
	}
	return &UsageTable{table: t, nodes: nodes}
}

func (ut *UsageTable) Poll(ctx context.Context) {
	for _, n := range ut.nodes {
		go ut.pollNode(ctx, n)
	}
}

func (ut *UsageTable) Get(nodeID string) (UsageInfo, bool) {
	ut.mx.RLock()
	defer ut.mx.RUnlock()
	info, ok := ut.table[nodeID]
	if !ok {
		return UsageInfo{}, false
	}
	return *info, true
}

func (ut *UsageTable) pollNode(ctx context.Context, n node.Node) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			info, err := scrape(ctx, n.MetricsURL)
			if err != nil {
				slog.Warn("usage scrape failed", "node", n.ID, "error", err)
				continue
			}
			ut.mx.Lock()
			ut.table[n.ID] = info
			ut.mx.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// scrape fetches /metrics and pulls out the four gauges we care about.
// Prometheus text format: lines like `sglang:kv_cache_memory_usage_gb{...} 12.3`
func scrape(ctx context.Context, url string) (*UsageInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	info := &UsageInfo{timestamp: time.Now()}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, val, ok := parseMetricLine(line)
		if !ok {
			continue
		}
		switch name {
		case "sglang:kv_cache_memory_usage_gb":
			info.KVCacheMemoryGB = val
		case "sglang:weight_memory_usage_gb":
			info.WeightMemoryGB = val
		case "sglang:startup_available_gpu_memory_gb":
			info.StartupAvailableMemGB = val
		case "sglang:cache_hit_rate":
			info.CacheHitRate = val
		}
	}
	return info, scanner.Err()
}

// parseMetricLine parses `name{labels} value` or `name value`.
func parseMetricLine(line string) (name string, value float64, ok bool) {
	nameEnd := strings.IndexAny(line, "{ ")
	if nameEnd < 0 {
		return "", 0, false
	}
	name = line[:nameEnd]
	valStart := strings.LastIndexByte(line, ' ')
	if valStart < 0 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(line[valStart+1:]), 64)
	if err != nil {
		return "", 0, false
	}
	return name, v, true
}
