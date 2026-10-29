package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/crizah/KV/proxy/services/health"
	services "github.com/crizah/KV/proxy/services/request"
	"github.com/crizah/KV/proxy/services/router"
)

type Handler struct {
	reqCfg       services.Config
	routeTable   *router.RouteTable
	healthTable  *health.HealthTable
	nodeHTTP     map[string]string // node ID -> SGLang "host:port"
	nodeAgentURL map[string]string // node ID -> agent metadata base URL
	timeout      time.Duration
	client       *http.Client
}

func NewHandler(
	reqCfg services.Config,
	rt *router.RouteTable,
	ht *health.HealthTable,
	nodeHTTP map[string]string,
	nodeAgentURL map[string]string,
	timeout time.Duration,
) *Handler {
	return &Handler{
		reqCfg:       reqCfg,
		routeTable:   rt,
		healthTable:  ht,
		nodeHTTP:     nodeHTTP,
		nodeAgentURL: nodeAgentURL,
		timeout:      timeout,
		client:       &http.Client{}, // timeout controlled by per-request context
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
		http.Error(w, "invalid request: missing text field", http.StatusBadRequest)
		return
	}

	svc, err := services.NewRequestService(h.reqCfg, req.Text, h.routeTable, h.healthTable)
	if err != nil {
		http.Error(w, "service init: "+err.Error(), http.StatusInternalServerError)
		return
	}

	reqCtx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	res, err := svc.HandleReq(reqCtx)
	if err != nil {
		http.Error(w, "routing: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	kvSource := ""
	if res.NodeP != "" {
		kvSource = h.nodeAgentURL[res.NodeP]
	}

	ch := make(chan fwdResult, 1)
	targetURL := "http://" + h.nodeHTTP[res.NodeD] + r.URL.RequestURI()
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
		w.WriteHeader(result.resp.StatusCode)
		io.Copy(w, result.resp.Body)
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
