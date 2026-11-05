# Notes

Running record of design decisions, open questions, and next steps. Update this file as we go.

## Locked design decisions

- **Proxy never talks to SGLang directly.** The proxy talks only to the agent's HTTP server (port 9100/9101). The agent sits in front of SGLang, reads the `X-Kv-Source` header, triggers the NIXL pull if needed, then forwards `/generate` to SGLang internally. SGLang ports (30000/30001) are internal only.
- **Pull is directed by the proxy.** On a cache hit on an unhealthy node, the proxy routes the request to a healthy node and sends headers telling it which producer to pull from. Agents never scan peers themselves.
- **Route table reads check both tiers.** `ReadCache` checks the CPU map, then the GPU map. A hash is a miss only if absent from both. Kept as-is.
- **Health comes from SGLang's load publisher** (ZMQ), not an HTTP `/health` endpoint. Liveness and load are separate signals: a node can be alive but too loaded to route to, and a dead node is never a valid pull source.
- **Matched hashes stay internal.** The proxy keeps the matched hash chain and count only for the transfer-vs-recompute threshold experiment. Nothing extra is sent to node B beyond the agreed headers.
- **Pull threshold is per model.** `pull_threshold_tokens` sits with `page_size` and `tokenizer_path` under `config.json`'s `models` map, selected by `model`. A cross-node pull only happens when the matched prefix is at least that many tokens; below it the proxy sends to the healthiest node and lets it recompute. Qwen3-4B = 11000 tokens from exp-2. `0` disables the threshold (always pull).
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
- How node B obtains the producer's NIXL agent metadata and key->pointer map in real deployment. The exp used shared files in `meta_dir`; over the network this needs a transport. Proxy headers will carry the producer identity, but the NIXL handshake path is undecided (we have a metadata endpoint on our agent server)

## Next steps

 - agent design

## Change log

- 2026-10-26: Fixed router bugs (nil inner map panic, doubled hash slice, BlockRemoved never removing, AllBlocksCleared locking the wrong mutex and clearing the wrong map, Listen returning after one event, seq never stored). Tokenizer is now created once and shared. Added `config.json` + loader and wired `main.go`.
- 2026-10-26: Health service implemented. Per-node ZMQ listeners (same pattern as the KV listener) feed one channel into `CheckHealth`. Status is an enum: unhealthy (error or >5min stale) / loaded (alive, over threshold) / healthy. Load score = running + waiting_weight * waiting, threshold from `config.json` (`health` section). `GetHealthyNodes` marks stale entries unhealthy while reading, then returns every alive node: `SortedNodes` (healthy + loaded, least loaded first, so healthy are always up front) and `NodesMap` (`map[string]Status` — a missing node is unhealthy).
- 2026-10-26: Router decision updated for the new health snapshot. Cache hit on a healthy holder -> route straight to it. Hit only on loaded holders -> route to the least loaded healthy node with `nodeP` = the least loaded loaded holder (pull path). All holders unhealthy -> treat as a cache miss (a dead node serves no bytes). No healthy node alive at all -> no route (retry TODO stands).

- the /agent endpoint is a one time thing between 2 nodes to exchange NIXL metadat. stored in .metadata. 
since this is a one time thing, it will affect our wall time, but the wiring is done in a way to skip this handshake if the metadata exists already.
the actual metadata includes only the pulled nodesID, the NIXL metadata and the CPU mem size we are allowed to read from (this is diff from the flag we pass `--cpu-offload-gb` i think, dont quote me on this, but i think the exact value of this is volatile)

also, TODO: have a startup script that hits this endpoint on every combination of nodes at cluster registration

TODO: have a batch_delete_v1 (have it subscribe to the same zmq path the proxy subscribes to, and this evicts the in memeory key value map on the source) local_keys is exactly hash → (k_ptr, v_ptr, elem_bytes)

okay so a design decision im not super sure on:
- the proxy already contains the hashes and the nodes that contain them via zmq events
- what we are doing is, upon every pull request, we are running a batch_exists. this guys checks a local table on the sourc that contains the actual key:value and tensors where this cache is stored on the CPU
- we then pull based on this mem address we just got
- batch_set_v1 sets this map when we get new cache on CPU

issues:
1. 2 sources of truth (even tho we have recompute fallbacks)
2. eating up mem on CPU
3. the most important one: eviction policy. to have this eviction policy on this second node, we need to have our agent subscribe to the same zmq event that the proxy subscribes to -> decode hash -> evict. not evicting isnt a correctness issue because we only send a req in the first place of the proxy believes this node has it, since proxy's table is updated, only edge cases fall through where we might visit this path, and even then, if the keys dont exist, we do a recomputation. 
4. but its just the 2 sources of truth thats bothering me. on the other hand, if we had the proxy itself save the memory addr of the blocks, thats HEAVY coupling and idk does sglang move shit around? plus, the cache events dont give u the mem addr, ill have to make another call for it so....

for now, i am going with the 2 source of truth design cos() its cleaner. but we need an eviction policy for this to ever work as anything but an exp

- also, make the launch script to start sg lang with ts

```
 --port {sglang_port}
 --page-size {page_size}                        # from models[model].page_size
 --kv-events-zmq-addr {ip}:{kv_events_port}    # proxy subscribes to this
 --load-balance-zmq-addr {ip}:{load_port}       # proxy subscribes to this
 --cpu-offload-gb {N}                           # not in config.json yet
 --mem-fraction-static {N}                      # not in config.json yet
 --hicache-storage-backend dynamic
 --hicache-storage-backend-extra-config '{"self_name": "A", "config_path": "...", "interface_v1": 1}'
 --model {model_path}
```

- also, make sure our routing algo takes into account the current load + the queued load
- so next we just have to make an interface out of the routing logic to have round robin and cacheroute
- and also, we need to add the json tokenisers for our model
