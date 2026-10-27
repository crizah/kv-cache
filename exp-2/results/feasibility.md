# Feasibility: Qwen3-4B pull-vs-recompute (RTX 5060 Ti, GPU 0)

## Verdict table

**Verdict: GO** (break-even at L=8192 is 5.89 Gbps; GO <=10, MAYBE 10-25, NO-GO >25 or never)

| L | KV bytes (MB) | saved_ms | break-even Gbps | pull_loopback_ms | loopback beats recompute? |
|---|---|---|---|---|---|
| 1024 | 151 | 142.9 | 9.54 | 204.0 | no |
| 4096 | 604 | 742.8 | 6.97 | 909.8 | no |
| 8192 | 1208 | 1734.2 | 5.89 | 1581.7 | yes |
| 16384 | 2416 | 4377.3 | 4.60 | 3119.7 | yes |

`saved_ms(L) = median TTFT(L) - median TTFT(128)` (TTFT(128) = 34.1 ms). `pull_loopback_ms = read_ms + h2d_ms + 5`; the 5 ms is an **assumed** fixed overhead, not measured.

## Notes / deviations (read these)

- **UCX_TLS:** plain `UCX_TLS=tcp` is rejected by NIXL's UCX backend when a GPU is present (`Invalid UCX_TLS=tcp ... add cuda_copy`). I used `UCX_TLS=tcp,cuda_copy`; the buffers are pinned host memory, so the data path is still TCP loopback.
- **Loopback NIXL read is slow** (~5.6-6.6 Gbps, 0.70-0.82 GB/s) versus 13-14 GB/s host-to-GPU. It is a best case for the network (no real link), yet it only beats recompute at L>=8192. The break-even Gbps figures are the link speed that must be *sustained end to end*; loopback itself sits near the 8192 break-even (5.89 Gbps).
- **Model:** Qwen3-4B was not on disk and the disk was full. With the user's approval I deleted the Qwen3-8B files under `/workspace/models` to make room and downloaded Qwen3-4B. The instance's default `SGLANG_MODEL` is still Qwen3-8B, so `sglang`/`model-ui` (stopped by me) will fail or re-download if restarted.
- The 5 ms fixed overhead is an assumption. h2d is a single contiguous copy; real paged scatter copies could be slower. Recompute TTFT includes HTTP/stream overhead (TTFT(128) = 34 ms is subtracted as the floor).
- No 16384 OOM; all lengths ran. `saved_ms` has no 2048/512 rows in the tables because NIXL/h2d were run only at 1024/4096/8192/16384 as specified.
- Single run, 10 trials per point, no tuning.

## Measured components (medians)

| L | TTFT ms | read_ms (NIXL tcp loopback) | loopback Gbps | h2d_ms |
|---|---|---|---|---|
| 1024 | 177.0 | 187.7 | 6.4 | 11.3 |
| 4096 | 776.8 | 860.7 | 5.6 | 44.1 |
| 8192 | 1768.3 | 1489.4 | 6.5 | 87.3 |
| 16384 | 4411.4 | 2941.0 | 6.6 | 173.6 |

Recompute TTFT, all lengths (median ms): 128: 34.1, 512: 92.2, 1024: 177.0, 2048: 362.2, 4096: 776.8, 8192: 1768.3, 16384: 4411.4
Recompute throughput L/TTFT: 4096 -> 5273 tok/s, 8192 -> 4633 tok/s

## Modeled links
`pull_ms = bytes*8/(rate*1e9)*1000 + h2d_ms + 5`

| L | Gbps | pull_ms | saved_ms | winner | speedup (recompute/pull or pull/recompute) |
|---|---|---|---|---|---|
| 1024 | 1 | 1224.3 | 142.9 | recompute | 8.57x |
| 1024 | 5 | 257.9 | 142.9 | recompute | 1.80x |
| 1024 | 10 | 137.1 | 142.9 | pull | 1.04x |
| 1024 | 25 | 64.7 | 142.9 | pull | 2.21x |
| 1024 | 100 | 28.4 | 142.9 | pull | 5.03x |
| 4096 | 1 | 4880.9 | 742.8 | recompute | 6.57x |
| 4096 | 5 | 1015.5 | 742.8 | recompute | 1.37x |
| 4096 | 10 | 532.3 | 742.8 | pull | 1.40x |
| 4096 | 25 | 242.4 | 742.8 | pull | 3.06x |
| 4096 | 100 | 97.4 | 742.8 | pull | 7.63x |
| 8192 | 1 | 9756.0 | 1734.2 | recompute | 5.63x |
| 8192 | 5 | 2025.0 | 1734.2 | recompute | 1.17x |
| 8192 | 10 | 1058.7 | 1734.2 | pull | 1.64x |
| 8192 | 25 | 478.8 | 1734.2 | pull | 3.62x |
| 8192 | 100 | 188.9 | 1734.2 | pull | 9.18x |
| 16384 | 1 | 19506.0 | 4377.3 | recompute | 4.46x |
| 16384 | 5 | 4044.1 | 4377.3 | pull | 1.08x |
| 16384 | 10 | 2111.4 | 4377.3 | pull | 2.07x |
| 16384 | 25 | 951.7 | 4377.3 | pull | 4.60x |
| 16384 | 100 | 371.9 | 4377.3 | pull | 11.77x |
