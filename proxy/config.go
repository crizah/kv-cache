package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/node"
	services "github.com/crizah/KV/proxy/services/request"
)

type NodeConfig struct {
	ID       string `json:"id"`
	IP       string `json:"ip"`        // empty defaults to 127.0.0.1
	HTTP     string `json:"http"`      // agent HTTP port, e.g. ":9100"
	KVEvents string `json:"kv_events"` // ZMQ KV events port, e.g. ":5557"
	Load     string `json:"load"`      // ZMQ load publisher port, e.g. ":6000"
	Meta     string `json:"meta"`      // agent metadata port (peer-to-peer, not used by proxy)
	SGLang   string `json:"sglang"`    // SGLang internal port (not used by proxy)
}

func (n NodeConfig) toNode() (node.Node, error) {
	if n.IP == "" {
		return node.Node{}, fmt.Errorf("node %q: ip is required", n.ID)
	}
	return node.Node{
		ID:           n.ID,
		AgentURL:     "http://" + n.IP + ":" + n.HTTP,
		KVEventsAddr: n.IP + ":" + n.KVEvents,
		LoadAddr:     n.IP + ":" + n.Load,
		MetricsURL:   "http://" + n.IP + ":" + n.SGLang + "/metrics",
	}, nil
}

type HealthConfig struct {
	WaitingWeight        float64 `json:"waiting_weight"`
	LoadedThreshold      float64 `json:"loaded_threshold"`
	TokenLoadedThreshold int64   `json:"token_loaded_threshold"` // in-flight tokens above which a node is loaded; 0 = disabled
}

// ModelConfig holds the settings that depend on the model. Page size and
// tokenizer differ per model, and so does the transfer-vs-recompute
// break-even point measured in exp-2.
type ModelConfig struct {
	PageSize            int    `json:"page_size"`             // must equal the servers' --page-size
	TokenizerPath       string `json:"tokenizer_path"`        // full fast tokenizer file (tokenizer.json), not tokenizer_config.json
	PullThresholdTokens int    `json:"pull_threshold_tokens"` // matched prefix (tokens) at/above which a cross-node pull beats recompute; 0 means no threshold
}

//
// type RouterType string
//
// const (
// 	KV         RouterType = "kv"
// 	CACHEROUTE RouterType = "CacheRoute"
// 	RR         RouterType = "RR"
// )

type Config struct {
	Model             string                 `json:"model"`
	Models            map[string]ModelConfig `json:"models"`
	ListenAddr        string                 `json:"listen_addr"`
	Router            string                 `json:"router"`
	Nodes             []NodeConfig           `json:"nodes"`
	Health            HealthConfig           `json:"health"`
	RequestTimeoutSec int                    `json:"request_timeout_sec"`
	CpuOffloadGB      int                    `json:"cpu_offload_gb"`
	MemFractionStatic float64                `json:"mem_fraction_static"`
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
	if cfg.RequestTimeoutSec <= 0 {
		cfg.RequestTimeoutSec = 30
	}
	switch cfg.Router {
	case "kv", "rr", "cacheRoute", "ll":
		// valid
	default:
		return nil, fmt.Errorf("router must be one of kv, rr, cacheRoute, ll; got %q", cfg.Router)
	}

	return &cfg, nil
}

func (cfg *Config) activeModel() ModelConfig {
	return cfg.Models[cfg.Model]
}

// nodes builds the shared Node slice from config. All services receive this same slice.
func (cfg *Config) nodes() ([]node.Node, error) {
	ns := make([]node.Node, len(cfg.Nodes))
	for i, n := range cfg.Nodes {
		nd, err := n.toNode()
		if err != nil {
			return nil, err
		}
		ns[i] = nd
	}
	return ns, nil
}

func (cfg *Config) healthConfig() health.Config {
	return health.Config{
		WaitingWeight:        cfg.Health.WaitingWeight,
		LoadedThreshold:      cfg.Health.LoadedThreshold,
		TokenLoadedThreshold: cfg.Health.TokenLoadedThreshold,
	}
}

func (cfg *Config) requestServiceConfig() services.Config {
	m := cfg.activeModel()
	return services.Config{
		Model:               services.Model(cfg.Model),
		PageSize:            m.PageSize,
		TokenizerPath:       m.TokenizerPath,
		PullThresholdTokens: m.PullThresholdTokens,
		Router:              cfg.Router,
	}
}

func (cfg *Config) requestTimeout() time.Duration {
	return time.Duration(cfg.RequestTimeoutSec) * time.Second
}
