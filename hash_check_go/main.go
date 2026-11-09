// Reads token vectors written by hash_check.py and verifies the Go hash matches.
//
// Usage:
//   python3 hash_check.py write 64          # generates /tmp/hash_vectors.json
//   go run ../hash_check_go/main.go          # reads and checks
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

func hashOnePage(tokens []uint32, priorDigest []byte) []byte {
	h := sha256.New()
	if len(priorDigest) == 32 {
		h.Write(priorDigest)
	}
	buf := make([]byte, len(tokens)*4)
	for i, t := range tokens {
		binary.LittleEndian.PutUint32(buf[i*4:], t)
	}
	h.Write(buf)
	return h.Sum(nil)
}

func toInt64(digest []byte) int64 {
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func main() {
	path := "/tmp/hash_vectors.json"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
		os.Exit(1)
	}

	var v struct {
		PageSize int     `json:"page_size"`
		Tokens   []int64 `json:"tokens"`
		Hashes   []int64 `json:"hashes"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(1)
	}

	tokens := make([]uint32, len(v.Tokens))
	for i, t := range v.Tokens {
		tokens[i] = uint32(t)
	}

	numPages := len(tokens) / v.PageSize
	fmt.Printf("page_size=%d  tokens=%d  pages=%d\n", v.PageSize, len(tokens), numPages)
	fmt.Printf("%5s  %22s  %22s  %10s\n", "page", "python", "go", "match")

	allPass := true
	var digest []byte
	for p := range numPages {
		page := tokens[p*v.PageSize : (p+1)*v.PageSize]
		digest = hashOnePage(page, digest)
		goHash := toInt64(digest)
		pyHash := v.Hashes[p]
		match := goHash == pyHash
		if !match {
			allPass = false
		}
		if !match || p < 3 || p == numPages-1 {
			status := "OK"
			if !match {
				status = "MISMATCH"
			}
			fmt.Printf("%5d  %22d  %22d  %10s\n", p, pyHash, goHash, status)
		}
	}

	if allPass {
		fmt.Println("\nALL PASS — Go and Python hashes agree")
	} else {
		fmt.Println("\nFAILURES DETECTED")
		os.Exit(1)
	}
}
