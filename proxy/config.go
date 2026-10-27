package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/router"
)

type NodeConfig struct {
	ID       string `json:"id"`        // identifier used across route table, health table and pull headers
	HTTP     string `json:"http"`      // host:port of the node's HTTP server
	KVEvents string `json:"kv_events"` // host:port of the node's KV events PUB socket
	Load     string `json:"load"`      // host:port of the node's load-publish socket
}

type HealthConfig struct {
	WaitingWeight   float64 `json:"waiting_weight"`   // weight of queued requests in the load score
	LoadedThreshold float64 `json:"loaded_threshold"` // load score above which a node is marked loaded
}

// ModelConfig holds the settings that depend on the model. Page size and
// tokenizer differ per model, and so does the transfer-vs-recompute
// break-even point measured in exp-2.
type ModelConfig struct {
	PageSize            int    `json:"page_size"`             // must equal the servers' --page-size
	TokenizerPath       string `json:"tokenizer_path"`        // full fast tokenizer file (tokenizer.json), not tokenizer_config.json
	PullThresholdTokens int    `json:"pull_threshold_tokens"` // matched prefix (tokens) at/above which a cross-node pull beats recompute; 0 means no threshold
}

type Config struct {
	Model      string                 `json:"model"` // active model, must be a key in models
	Models     map[string]ModelConfig `json:"models"`
	ListenAddr string                 `json:"listen_addr"`
	Nodes      []NodeConfig           `json:"nodes"`
	Health     HealthConfig           `json:"health"`
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
	model, ok := cfg.Models[cfg.Model]
	if !ok {
		return nil, fmt.Errorf("model %q is not defined in models", cfg.Model)
	}
	if model.PageSize <= 0 {
		return nil, fmt.Errorf("model %q: page_size must be > 0, got %d", cfg.Model, model.PageSize)
	}
	if model.TokenizerPath == "" {
		return nil, fmt.Errorf("model %q: tokenizer_path is required", cfg.Model)
	}
	if model.PullThresholdTokens < 0 {
		return nil, fmt.Errorf("model %q: pull_threshold_tokens must be >= 0, got %d", cfg.Model, model.PullThresholdTokens)
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("config must list at least one node")
	}
	if cfg.Health.WaitingWeight == 0 {
		cfg.Health.WaitingWeight = 1.0
	}
	if cfg.Health.LoadedThreshold <= 0 {
		cfg.Health.LoadedThreshold = 8
	}
	return &cfg, nil
}

// activeModel returns the settings for the model selected by cfg.Model.
func (cfg *Config) activeModel() ModelConfig {
	return cfg.Models[cfg.Model]
}

func (cfg *Config) routerNodes() []router.Node {
	nodes := make([]router.Node, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes = append(nodes, router.Node{ID: n.ID, KVAddr: n.KVEvents})
	}
	return nodes
}

func (cfg *Config) healthNodes() []health.Node {
	nodes := make([]health.Node, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes = append(nodes, health.Node{ID: n.ID, LoadAddr: n.Load})
	}
	return nodes
}

func (cfg *Config) healthConfig() health.Config {
	return health.Config{
		WaitingWeight:   cfg.Health.WaitingWeight,
		LoadedThreshold: cfg.Health.LoadedThreshold,
	}
}
