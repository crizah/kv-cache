package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/crizah/KV/proxy/handlers"
	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/router"
	"github.com/crizah/KV/proxy/services/usage"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	configPath := os.Getenv("CONFIG_PATH")
	cfg, err := loadConfig(configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nodes, err := cfg.nodes()
	if err != nil {
		slog.Error("build nodes", "error", err)
		os.Exit(1)
	}

	// route table, kept up to date in the background by the nodes' KV events
	routeTable := router.NewRouteTableService(nodes)
	// if its round robin, initliaise that bitch
	if cfg.Router == "rr" {
		routeTable.RRMap.TotalNodes = len(nodes)
		routeTable.RRMap.CurrentIndex = -1
	}
	go routeTable.Listen(ctx)

	// usage table, polled every 5s from each node's /metrics endpoint
	usageTable := usage.NewUsageTable(nodes)
	usageTable.Poll(ctx)

	// health table, kept up to date in the background by the nodes' load publisher
	healthTable := health.NewHealthTable(nodes, cfg.healthConfig())
	go healthTable.CheckHealth(ctx)

	h := handlers.NewHandler(
		cfg.requestServiceConfig(),
		routeTable,
		healthTable,
		nodes,
		cfg.requestTimeout(),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/generate", h.ServeHTTP)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	slog.Info("proxy listening", "addr", cfg.ListenAddr, "nodes", len(cfg.Nodes))
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server", "error", err)
		os.Exit(1)
	}
}
