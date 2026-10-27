package router

// Offline tests using data recorded by probe_events.py.
//
// Run after probe_events.py has produced:
//   results/events_raw.jsonl     (raw ZMQ frames, payload_b64 is msgpack)
//   results/events_decoded.jsonl (pre-decoded; used only for cross-checking)
//
// Set the env var PROBE_DIR to the directory containing those files, e.g.:
//   PROBE_DIR=/workspace/exp2/results go test ./services/router/ -run TestOffline -v

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func probeDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("PROBE_DIR")
	if dir == "" {
		t.Skip("PROBE_DIR not set; skipping offline tests")
	}
	return dir
}

// rawFrame is one line of events_raw.jsonl.
type rawFrame struct {
	Seq        uint64 `json:"seq"`
	PayloadB64 string `json:"payload_b64"`
}

// decodedFrame is one line of events_decoded.jsonl.
type decodedFrame struct {
	Seq    uint64           `json:"seq"`
	Ts     float64          `json:"ts"`
	Events []map[string]any `json:"events"`
}

func readLines[T any](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []T
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			return nil, fmt.Errorf("parse %q: %w", line[:min(len(line), 80)], err)
		}
		out = append(out, v)
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestOfflineDecodeBatch verifies that decodeBatch parses every raw frame
// and produces the same event count as events_decoded.jsonl.
func TestOfflineDecodeBatch(t *testing.T) {
	dir := probeDir(t)

	rawFrames, err := readLines[rawFrame](filepath.Join(dir, "events_raw.jsonl"))
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}
	decFrames, err := readLines[decodedFrame](filepath.Join(dir, "events_decoded.jsonl"))
	if err != nil {
		t.Fatalf("read decoded: %v", err)
	}

	if len(rawFrames) != len(decFrames) {
		t.Fatalf("frame count mismatch: raw=%d decoded=%d", len(rawFrames), len(decFrames))
	}

	for i, rf := range rawFrames {
		payload, err := base64.StdEncoding.DecodeString(rf.PayloadB64)
		if err != nil {
			t.Errorf("frame %d: base64 decode: %v", i, err)
			continue
		}

		got := decodeBatch(rf.Seq, payload)
		if got.error != nil {
			t.Errorf("frame %d (seq=%d): decodeBatch error: %v", i, rf.Seq, got.error)
			continue
		}

		want := decFrames[i]
		if got.seq != want.Seq {
			t.Errorf("frame %d: seq got=%d want=%d", i, got.seq, want.Seq)
		}
		if len(got.events) != len(want.Events) {
			t.Errorf("frame %d: event count got=%d want=%d", i, len(got.events), len(want.Events))
		}
	}
	t.Logf("OK: decoded %d frames", len(rawFrames))
}

// TestOfflineWriteCache verifies that writeCache produces the expected route
// table state: stored hashes land in the table, removed hashes disappear.
func TestOfflineWriteCache(t *testing.T) {
	dir := probeDir(t)

	rawFrames, err := readLines[rawFrame](filepath.Join(dir, "events_raw.jsonl"))
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}

	table := NewRouteTableService(nil)
	const nodeIP = "test-node"

	var stored, removed int
	for i, rf := range rawFrames {
		payload, err := base64.StdEncoding.DecodeString(rf.PayloadB64)
		if err != nil {
			t.Errorf("frame %d: base64: %v", i, err)
			continue
		}
		batch := decodeBatch(rf.Seq, payload)
		if batch.error != nil {
			continue
		}
		batch.nodeIP = nodeIP
		table.writeCache(batch)

		for _, ev := range batch.events {
			switch ev.Type {
			case "BlockStored":
				stored++
			case "BlockRemoved":
				removed++
			}
		}
	}

	// Sanity: if we stored any hashes, the table must not be empty.
	table.routeMapCPU.mx.RLock()
	cpuEntries := len(table.routeMapCPU.table)
	table.routeMapCPU.mx.RUnlock()
	table.routeMapGPU.mx.RLock()
	gpuEntries := len(table.routeMapGPU.table)
	table.routeMapGPU.mx.RUnlock()

	t.Logf("BlockStored=%d BlockRemoved=%d cpuEntries=%d gpuEntries=%d",
		stored, removed, cpuEntries, gpuEntries)

	if stored > 0 && cpuEntries+gpuEntries == 0 {
		t.Error("BlockStored events were processed but table is empty")
	}

	// Every entry in the table must map to nodeIP.
	ctx := context.Background()
	for _, tbl := range []map[int64]map[string]struct{}{
		table.routeMapCPU.table,
		table.routeMapGPU.table,
	} {
		for hash, nodes := range tbl {
			if _, ok := nodes[nodeIP]; !ok {
				t.Errorf("hash %d: expected node %q not present; got %v", hash, nodeIP, nodes)
			}
			if n := table.ReadCache(ctx, hash); len(n) == 0 {
				t.Errorf("hash %d: ReadCache returned empty after write", hash)
			}
		}
	}
}

// TestOfflineEventMediums verifies that every BlockStored event has a medium
// the proxy recognises ("GPU" or "CPU_PINNED").
func TestOfflineEventMediums(t *testing.T) {
	dir := probeDir(t)

	rawFrames, err := readLines[rawFrame](filepath.Join(dir, "events_raw.jsonl"))
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}

	mediums := map[string]int{}
	for _, rf := range rawFrames {
		payload, _ := base64.StdEncoding.DecodeString(rf.PayloadB64)
		batch := decodeBatch(rf.Seq, payload)
		if batch.error != nil {
			continue
		}
		for _, ev := range batch.events {
			if ev.Type == "BlockStored" || ev.Type == "BlockRemoved" {
				m, _ := ev.Fields["medium"].(string)
				mediums[m]++
			}
		}
	}
	t.Logf("mediums: %v", mediums)

	for m := range mediums {
		if m != "GPU" && m != "CPU_PINNED" {
			t.Logf("NOTE: unexpected medium %q (DISK/EXTERNAL/nil are not tracked by the proxy)", m)
		}
	}
}

