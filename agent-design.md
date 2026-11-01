# Agent Design

## Role

The agent sits in front of SGLang. The proxy talks only to the agent. The agent:
1. Receives `/generate` from the proxy
2. Reads `X-Kv-Source` header — if present, triggers a NIXL pull from the producer before forwarding
3. Forwards the request to its local SGLang instance
4. Streams the response back to the proxy

SGLang ports are internal. The agent is the only public-facing HTTP server per node.

## HTTP Endpoints

| Endpoint | Description |
|---|---|
| `POST /generate` | Entry point. Reads `X-Kv-Source`, triggers pull if set, forwards to SGLang |
| `GET /agent` | Returns NIXL agent metadata: `{agent, meta (hex), base, nbytes}` |
| `GET /keys?since=N` | Returns new page records since cursor N: `{records: [{k, p, n}], next}` |

`/agent` and `/keys` are consumed by the peer agent when pulling.

## HiCache Backend (SGLang plugin)

Loaded via `--hicache-storage-backend dynamic` with `extra-config`:
- `interface_v1: 1` — routes SGLang to call `batch_set_v1` / `batch_get_v1` instead of the default copy path
- `self_name` — this node's ID (e.g. `"A"`)
- `meta_port` — port this agent's HTTP server listens on (e.g. `9100`)
- `sglang_port` — internal SGLang HTTP port (e.g. `30000`)

**`batch_set_v1`** — called after prefill. Records the K/V pointer for each new page into `_local_keys`. No data is copied; HiCache already moved it to CPU via `write_through`.

**`batch_exists`** — polls peer's `/keys` endpoint, returns contiguous prefix match count.

**`batch_get_v1`** — does one NIXL READ from the producer's registered host memory into local host memory. Logs result to JSONL for experiment metrics.

## Pull Flow (inside agent HTTP handler)

```
POST /generate + X-Kv-Source: http://<producer-agent>
  │
  ├─ fetch producer's /agent  →  add_remote_agent()
  ├─ fetch producer's /keys   →  update peer key map
  ├─ pin local pages          →  (see Pinning)
  ├─ NIXL READ                →  pages land in local host pool
  └─ forward to SGLang        →  cache hit in L2, prefill skips recompute
```

If `X-Kv-Source` is absent, forward to SGLang directly (cache miss path, SGLang recomputes).

## Response Streaming

After forwarding to SGLang, the agent pipes SGLang's response back to the proxy chunk by chunk as it arrives — no buffering or second copy. The agent is a transparent tunnel for the response body. SGLang's chunked/SSE stream is preserved end-to-end:

```
SGLang → agent (chunk by chunk) → proxy → client
```

## Pinning

While a NIXL READ is in progress, the producer's pages must not be evicted from its CPU pool. Pin mechanism is a TODO — SGLang/HiCache does not expose a pin API today. For the experiment, eviction during transfer is unlikely given pool size vs transfer duration. Noted as a known limitation.
(after reeading the docs, it does look like an external pinning mechanism is not exposed. oh well)

Pull has a configurable timeout. On timeout or NIXL error, the agent forwards to SGLang without the pull — SGLang recomputes.

## SGLang Launch Flags

All values come from `proxy/config.json`

| Flag | Value source | Notes |
|---|---|---|
| `--hicache-write-policy` | hardcoded `write_through` | GPU→CPU copy on every prefill |
| `--page-size` | `models.<model>.page_size` | Must match proxy hasher |
| `--mem-fraction-static` | `nodes.<id>.mem_fraction_static` | GPU pool fraction |
| `--hicache-ratio` | `nodes.<id>.hicache_ratio` | CPU pool as fraction of GPU pool; must be > GPU pool |
| `--kv-events-config` | `nodes.<id>.kv_events` port | ZMQ PUB socket; proxy subscribes for route table updates |
| `--load-publish-endpoint` | `nodes.<id>.load` port | ZMQ PUB socket; proxy subscribes for health |

## Metrics Logging

Every completed pull is appended to `logs/<self_name>_pull.jsonl`:
```json
{"ts": 1234567890.0, "pages": 23, "bytes": 168820736, "lookup_ms": 1.2, "read_ms": 327.5}
```

## Limitations

- Pinning not implemented. A page evicted from the producer mid-transfer causes a failed or corrupt pull; the agent falls back to recompute.
- Both nodes run on the same machine (localhost). Metadata exchange over the network is the same HTTP path but was not tested on a real multi-node setup.
- `X-Kv-Source` resolves the peer dynamically per request, so N-node setups work in principle. The 2-node experiment hardcodes one peer in extra-config as a fallback if the header is absent.
