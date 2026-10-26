package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/crizah/KV/proxy/services/router"
)

type NodeConfig struct {
	ID       string `json:"id"`        // identifier used across route table, health table and pull headers
	HTTP     string `json:"http"`      // host:port of the node's HTTP server
	KVEvents string `json:"kv_events"` // host:port of the node's KV events PUB socket
	Load     string `json:"load"`      // host:port of the node's load-publish socket
}

type Config struct {
	Model         string       `json:"model"`
	PageSize      int          `json:"page_size"` // must equal the servers' --page-size
	ListenAddr    string       `json:"listen_addr"`
	TokenizerPath string       `json:"tokenizer_path"` // full fast tokenizer file (tokenizer.json), not tokenizer_config.json
	Nodes         []NodeConfig `json:"nodes"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.PageSize <= 0 {
		return nil, fmt.Errorf("page_size must be > 0, got %d", cfg.PageSize)
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("config must list at least one node")
	}
	return &cfg, nil
}

func (cfg *Config) routerNodes() []router.Node {
	nodes := make([]router.Node, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes = append(nodes, router.Node{ID: n.ID, KVAddr: n.KVEvents})
	}
	return nodes
}

func (cfg *Config) nodeIDs() []string {
	ids := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}
