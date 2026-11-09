package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/node"
	services "github.com/crizah/KV/proxy/services/request"
	"github.com/crizah/KV/proxy/services/router"
)

type Handler struct {
	reqCfg      services.Config
	routeTable  *router.RouteTable
	healthTable *health.HealthTable
	nodes       []node.Node
	timeout     time.Duration
	client      *http.Client
}

func NewHandler(
	reqCfg services.Config,
	rt *router.RouteTable,
	ht *health.HealthTable,
	nodes []node.Node,
	timeout time.Duration,
) *Handler {
	return &Handler{
		reqCfg:      reqCfg,
		routeTable:  rt,
		healthTable: ht,
		nodes:       nodes,
		timeout:     timeout,
		client:      &http.Client{}, // timeout controlled by per-request context
	}
}

type generateReq struct {
	Text string `json:"text"`
}

type fwdResult struct {
	resp *http.Response
	err  error
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req generateReq
	if err := json.Unmarshal(body, &req); err != nil || req.Text == "" {
		slog.Error("handler bad request", "error", err)
		http.Error(w, "invalid request: missing text field", http.StatusBadRequest)
		return
	}

	slog.Info("handler request", "text_len", len(req.Text))

	svc, err := services.NewRequestService(h.reqCfg, req.Text, h.nodes, h.routeTable, h.healthTable)
	if err != nil {
		slog.Error("handler service init failed", "error", err)
		http.Error(w, "service init: "+err.Error(), http.StatusInternalServerError)
		return
	}

	reqCtx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	res, err := svc.HandleReq(reqCtx)
	if err != nil {
		slog.Error("handler routing failed", "error", err)
		http.Error(w, "routing: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	slog.Info("handler routing", "dest", res.NodeD.ID, "pull_from", res.NodeP.ID)

	kvSource := ""
	if res.NodeP.ID != "" {
		kvSource = res.NodeP.ID
	}

	ch := make(chan fwdResult, 1)
	targetURL := res.NodeD.AgentURL + r.URL.RequestURI()
	go func() {
		ch <- forwardReq(reqCtx, h.client, r.Method, targetURL, r.Header, body, kvSource)
	}()

	select {
	case result := <-ch:
		if result.err != nil {
			// TODO: retry with other nodes or buffer here
			http.Error(w, "forward failed: "+result.err.Error(), http.StatusBadGateway)
			return
		}
		defer result.resp.Body.Close()
		for k, vv := range result.resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		if reqID := r.Header.Get("X-Request-Id"); reqID != "" {
			w.Header().Set("X-Request-Id", reqID)
		}
		w.Header().Set("X-Served-By", res.NodeD.ID)
		if res.NodeP.ID != "" {
			w.Header().Set("X-Pulled-From", res.NodeP.ID)
		}
		w.WriteHeader(result.resp.StatusCode)
		io.Copy(w, result.resp.Body) //nolint:errcheck
	case <-reqCtx.Done():
		// TODO: retry with other nodes or buffer here
		http.Error(w, "request timeout or cancelled", http.StatusGatewayTimeout)
	}
}

func forwardReq(ctx context.Context, client *http.Client, method, url string, header http.Header, body []byte, kvSource string) fwdResult {
	fwdReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return fwdResult{err: err}
	}
	fwdReq.Header = header.Clone()
	if kvSource != "" {
		fwdReq.Header.Set("X-Kv-Source", kvSource)
	}
	resp, err := client.Do(fwdReq)
	return fwdResult{resp: resp, err: err}
}
