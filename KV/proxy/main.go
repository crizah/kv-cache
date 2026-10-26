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
)

func main() {
	cfg, err := loadConfig("config.json")
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// route table, kept up to date in the background by the nodes' KV events
	routeTable := router.NewRouteTableService(cfg.routerNodes())
	go routeTable.Listen(ctx)

	// health table, kept up to date in the background by the nodes' load publisher
	healthTable := health.NewHealthTable(cfg.healthNodes(), cfg.healthConfig())
	go healthTable.CheckHealth(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/generate", handlers.ReqHandler)
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
