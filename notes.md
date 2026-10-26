# Notes

Running record of design decisions, open questions, and next steps. Update this file as we go.

## Locked design decisions

- **Pull is directed by the proxy.** On a cache hit on an unhealthy node, the proxy routes the request to a healthy node and sends headers telling it which producer to pull from. Agents never scan peers themselves.
- **Route table reads check both tiers.** `ReadCache` checks the CPU map, then the GPU map. A hash is a miss only if absent from both. Kept as-is.
- **Health comes from SGLang's load publisher** (ZMQ), not an HTTP `/health` endpoint. Liveness and load are separate signals: a node can be alive but too loaded to route to, and a dead node is never a valid pull source.
- **Matched hashes stay internal.** The proxy keeps the matched hash chain and count only for the transfer-vs-recompute threshold experiment. Nothing extra is sent to node B beyond the agreed headers.
- **Nuanced multi-level hash matching** (using intermediate matched hashes when the primary node is unhealthy) is a v2 feature.
- **Chat templates are out of scope.** Proxy accepts raw prompts only (`/generate`, `/v1/completions`). Recorded as a limitation.
- **Page size is a universal config** (`proxy/config.json`), must equal SGLang's `--page-size` on every node.
- **Hasher is verified** against SGLang's C++ path (user verified).
- **Route table is updated only from KV events.** Agents do not return new hashes in responses.

## Known edge cases (parked)

- The exp backend pulls from host (CPU) memory only. A block present only in a node's GPU map cannot be pulled. `write_through` makes this window small. Parked unless pulls fail in practice.

## Waiting on user / SGLang docs

- Exact `--kv-events-config` flag and values, and the load-publish endpoint flag name.
- LoadStat payload layout. The decoder in `proxy/services/health/health.go` (`decodeLoadStat`) currently assumes `["LoadStat", num_running_reqs, num_waiting_reqs, num_tokens, max_total_num_tokens, attn_dp_rank, ...]` over 3 ZMQ frames `[topic][seq][msgpack]`. One function to fix if the docs say otherwise.
- Tuned values for `--mem-fraction-static`, `--hicache-ratio` / `--hicache-size`.
- Page pinning while a peer is reading, plus pull timeout -> recompute fallback.
- How node B obtains the producer's NIXL agent metadata and key->pointer map in real deployment. The exp used shared files in `meta_dir`; over the network this needs a transport. Proxy headers will carry the producer identity, but the NIXL handshake path is undecided.
- `tokenizer.json` (the full fast tokenizer file) must be placed in `proxy/configs/<model>/`. `tokenizer_config.json` alone will not load in the Go tokenizer library.

## Next steps

- Implement `CheckHealth` as a LoadStat subscriber (alive vs loaded split).
- Implement the HTTP handler: parse `/generate`, route, forward, stream the response back.
- Pull-failure path: broadcast and remove the hash from the route table.
- Agent side: read pull headers, add page pinning with timeout, tune launch flags.
- Run the transfer-vs-recompute threshold experiment.

## Change log

- 2026-10-26: Fixed router bugs (nil inner map panic, doubled hash slice, BlockRemoved never removing, AllBlocksCleared locking the wrong mutex and clearing the wrong map, Listen returning after one event, seq never stored). Tokenizer is now created once and shared. Added `config.json` + loader and wired `main.go`.
- 2026-10-26: Health service implemented. Per-node ZMQ listeners (same pattern as the KV listener) feed one channel into `CheckHealth`. Status is an enum: unhealthy (error or >5min stale) / loaded (alive, over threshold) / healthy. Load score = running + waiting_weight * waiting, threshold from `config.json` (`health` section). `GetHealthyNodes` marks stale entries unhealthy while reading, then returns every alive node: `SortedNodes` (healthy + loaded, least loaded first, so healthy are always up front) and `NodesMap` (`map[string]Status` — a missing node is unhealthy).
- 2026-10-26: Router decision updated for the new health snapshot. Cache hit on a healthy holder -> route straight to it. Hit only on loaded holders -> route to the least loaded healthy node with `nodeP` = the least loaded loaded holder (pull path). All holders unhealthy -> treat as a cache miss (a dead node serves no bytes). No healthy node alive at all -> no route (retry TODO stands).
