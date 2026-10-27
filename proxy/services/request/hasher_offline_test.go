package services

// Offline hasher test using data recorded by probe_events.py.
//
// Verifies that the proxy's SHA-256 chain hash reproduces the block_hashes
// values in the KV events for the same token sequence.
//
// For this to hold, SGLang must use the same algorithm:
//   SHA256(parent_digest || tokens_as_little_endian_uint32) → first 8 bytes
//   as big-endian uint64 reinterpreted as int64.
//
// Run:
//   PROBE_DIR=/workspace/exp2/results go test ./services/request/ -run TestHasherOffline -v

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// eventSummary is events_summary.json written by probe_events.py.
type eventSummary struct {
	IDs1024     []int    `json:"ids_1024_token_ids"`
	ChainOK     bool     `json:"chain_ok"`
}

// decodedFrameH mirrors the decoded JSONL for the hasher test.
type decodedFrameH struct {
	Events []map[string]any `json:"events"`
}

func readLinesH[T any](path string) ([]T, error) {
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
			return nil, fmt.Errorf("parse line: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// TestHasherOffline verifies the proxy's hash chain against recorded events.
func TestHasherOffline(t *testing.T) {
	dir := os.Getenv("PROBE_DIR")
	if dir == "" {
		t.Skip("PROBE_DIR not set; skipping offline hasher test")
	}

	// Load token IDs used in probe_events.py
	sumData, err := os.ReadFile(filepath.Join(dir, "events_summary.json"))
	if err != nil {
		t.Fatalf("read events_summary.json: %v", err)
	}
	var summary eventSummary
	if err := json.Unmarshal(sumData, &summary); err != nil {
		t.Fatalf("parse events_summary.json: %v", err)
	}
	if len(summary.IDs1024) == 0 {
		t.Fatal("events_summary.json has no ids_1024_token_ids")
	}

	// Convert []int → []uint32
	tokens := make([]uint32, len(summary.IDs1024))
	for i, v := range summary.IDs1024 {
		tokens[i] = uint32(v)
	}

	// Compute expected hash chain with the proxy's algorithm (pageSize=64).
	const pageSize = 64
	numPages := len(tokens) / pageSize
	expectedHashes := make([]int64, numPages)
	var digest []byte
	for p := range numPages {
		page := tokens[p*pageSize : (p+1)*pageSize]
		digest = hashOnePage(page, digest)
		expectedHashes[p] = int64(binary.BigEndian.Uint64(digest[:8]))
	}

	// Collect block_hashes from recorded events.
	frames, err := readLinesH[decodedFrameH](filepath.Join(dir, "events_decoded.jsonl"))
	if err != nil {
		t.Fatalf("read decoded events: %v", err)
	}

	// Gather all int64 block_hashes from BlockStored events.
	var observedHashes []int64
	for _, fr := range frames {
		for _, ev := range fr.Events {
			if ev["type"] != "BlockStored" {
				continue
			}
			raw, ok := ev["block_hashes"].([]any)
			if !ok {
				continue
			}
			for _, h := range raw {
				switch v := h.(type) {
				case float64:
					observedHashes = append(observedHashes, int64(v))
				case int64:
					observedHashes = append(observedHashes, v)
				case json.Number:
					n, _ := v.Int64()
					observedHashes = append(observedHashes, n)
				}
			}
		}
	}

	t.Logf("proxy computed %d hashes; events contain %d hash values",
		len(expectedHashes), len(observedHashes))

	// Check that every expected hash appears in the observed set.
	observed := make(map[int64]bool, len(observedHashes))
	for _, h := range observedHashes {
		observed[h] = true
	}

	misses := 0
	for i, h := range expectedHashes {
		if !observed[h] {
			t.Errorf("page %d: hash 0x%016x not found in events", i, uint64(h))
			misses++
			if misses >= 5 {
				t.Log("(stopping after 5 misses)")
				break
			}
		}
	}
	if misses > 0 {
		t.Log("Hash mismatch: SGLang may use a different algorithm. " +
			"Compare events_decoded.jsonl block_hashes with the values above.")
		// Print first few for debugging
		if len(expectedHashes) > 0 && len(observedHashes) > 0 {
			t.Logf("Expected[0]: 0x%016x", uint64(expectedHashes[0]))
			t.Logf("Observed[0]: 0x%016x", uint64(observedHashes[0]))
		}
	} else if len(expectedHashes) > 0 {
		t.Logf("All %d proxy hashes matched events. Algorithms agree.", len(expectedHashes))
	}

	// Sanity: expectedHashes must not all be the same (trivial hash bug).
	unique := make(map[int64]bool)
	for _, h := range expectedHashes {
		unique[h] = true
	}
	if len(expectedHashes) > 1 && len(unique) == 1 {
		t.Error("all computed hashes are identical – hashOnePage is likely broken")
	}

	// Also verify no NaN/Inf (shouldn't be possible with int64 but defend).
	for _, h := range expectedHashes {
		if math.IsNaN(float64(h)) {
			t.Error("NaN in computed hashes")
		}
	}
}
