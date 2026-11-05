# RUN.md – exact commands in run order

Two cloud instances in the **same region**, one 24 GB GPU each:

```
Machine A: <A_HOST>   SGLang :30000   meta :8765   kv-events :30200
Machine B: <B_HOST>   SGLang :30001   meta :8766   kv-events :30201
```

Each command below is labelled with the machine it runs on. Replace
`<A_HOST>` / `<B_HOST>` with the instances' **overlay** IPs (see Prereq).
Machines no longer share CPU or memory; all traffic between nodes crosses the
network.

---

## Prereq – Network between the two instances

Two instances on different machines sit on separate bridge networks behind
NAT: private IPs the peer cannot dial, and every port remapped to a random
public number. NIXL/UCX opens its own TCP ports and advertises its own
address, so `-p` mappings cannot carry it. (TUN-based overlays such as
Tailscale/WireGuard are not an option either: the container has no
`CAP_NET_ADMIN`, so no interface can be created.)

Use a **Vast overlay network** instead. Instances in the same physical cluster
share a private virtual LAN where **all ports are open between them**, so the
nodes talk directly and UCX can advertise a routable address. No TUN, no
WireGuard, no Tailscale, no port pinning.

### Create the overlay and the two instances

Find clusters that have offers matching your GPU needs:
```bash
vastai search offers --raw 'cluster_id!=None ' <your filters> | grep -o 'cluster_id: [0-9]*' | sort -u
vastai search offers cluster_id=<CLUSTER_ID> <your filters>
```
Both instances must come from the **same** `cluster_id`. If that cluster only
has one GPU type, take two of that type and say so in the report.

```bash
vastai create overlay <CLUSTER_ID> kvnet
```

### Verify

On each instance (`apt install iproute2` if needed):
```bash
ip -br addr
# eth0 = overlay LAN (10.0.0.x), eth1 = bridge to the internet
nc -vz <peer eth0 IP> <port>      # any port, once something is listening there
```

Use the two `eth0` IPs for every `<A_HOST>` / `<B_HOST>` below. `launch_node.sh`
defaults to `UCX_NET_DEVICES=eth0`, which is what makes UCX advertise the
overlay address instead of the NAT'd one.

Note for the report: this is a private LAN inside one physical cluster, not a
public-internet path. `link_check.sh` measures it.

Template for both instances. Keep `SGLANG_MODEL` and `--download-dir
/workspace/models`: the image downloads the 8B weights at boot, which saves
time. It also auto-starts an SGLang server on `127.0.0.1:18000`; stop that
server once the download completes so the GPU is free, then start the real one
with `launch_node.sh` (Step 1), which reuses the same `/workspace/models` cache.

```bash
vastai create instance <OFFER_ID> --image vastai/sglang:@vastai-automatic-tag \
  --env '-p 1111:1111 -p 7860:7860 -p 8080:8080 -p 8000:8000 -p 8265:8265 -p 10100:10100 -p 10200:10200 -p 30000:30000 -p 8765:8765 -p 30200:30200 -p 51820:51820/udp --cap-add=NET_ADMIN --cap-add=MKNOD -e OPEN_BUTTON_PORT="1111" -e OPEN_BUTTON_TOKEN="1" -e JUPYTER_DIR="/" -e DATA_DIRECTORY="/workspace/" -e PORTAL_CONFIG="localhost:1111:11111:/:Instance Portal|localhost:7860:17860:/:Model UI|localhost:8000:18000:/docs:SGLang API|localhost:8080:18080:/:Jupyter|localhost:8080:8080:/terminals/1:Jupyter Terminal" -e SGLANG_MODEL="Qwen/Qwen3-8B" -e SGLANG_ARGS="--download-dir /workspace/models --host 127.0.0.1 --port 18000" -e AUTO_PARALLEL="true"' \
  --onstart-cmd 'entrypoint.sh' --disk 24 --jupyter --ssh --direct
```

If `mknod` is still denied after that, add `--device=/dev/net/tun` to the same
string. Verify the caps before doing anything else:
```bash
grep CapEff /proc/self/status                    # bit 12 (0x1000) = NET_ADMIN, bit 13 (0x2000) = NET_RAW
mkdir -p /dev/net && mknod /dev/net/tun c 10 200 && chmod 666 /dev/net/tun
ip tuntap add dev t0 mode tun && echo "TUN OK" && ip link del t0
```

**Known failure:** Vast container instances may ignore `--cap-add` and boot with
Docker's default set. If `CapEff` lacks bit 12, `TUNSETIFF` returns "Operation
not permitted" no matter what, and `--device=/dev/net/tun` does not help. In
that case container mode cannot work — use a VM instance or another provider.
Do not attempt userspace networking.
On A, note the **public** UDP port Vast reports for 51820 — the WireGuard
endpoint on B uses that public number, not 51820.
```

Once the model is in `/workspace/models`, stop the auto-started server so it
does not hold VRAM:
```bash
supervisorctl status
sudo supervisorctl stop sglang model-ui    # confirm with nvidia-smi: memory freed
```

Port 30000/8765 mappings are only for your own `curl` checks from outside; the
instances talk over the overlay, not those. Both hosts can use the same port
numbers. Prefer offers in the same city/datacenter (`geolocation`) for a lower
RTT; check `inet_up`/`inet_down` on both offers since that is the pull link.

### Dependencies

```bash
pip install pyzmq msgpack requests
# analyze.py plots (optional):
pip install matplotlib
# link_check.sh throughput test (optional, falls back to NIXL only):
sudo apt-get install -y iperf3 iputils-ping
```

---

## Step 0 – Link check (A → B, real network)

**On A:**
```bash
chmod +x /workspace/exp2/link_check.sh
/workspace/exp2/link_check.sh server
# Optional: override KV bytes/token (default 147456 = Qwen3-8B)
# /workspace/exp2/link_check.sh server 147456
```
It prints this machine's IP(s) and waits.

**On B:**
```bash
/workspace/exp2/link_check.sh client <A_HOST>
```

**Expected:** iperf3 TCP throughput for the tunnel, TCP-connect RTT, and clean
NIXL READ bandwidth for 512 … 16384 token-sized buffers. These numbers are the
real link for the pull path — the modeled-network table in analyze.py is only
for extrapolation beyond it.

---

## Step 1 – Launch SGLang, one server per machine

**On A:**
```bash
chmod +x /workspace/exp2/launch_node.sh
mkdir -p /workspace/exp2/logs /workspace/exp2/results
/workspace/exp2/launch_node.sh A 0 30000 8765 http://<B_HOST>:8766 \
  2>&1 | tee /workspace/exp2/logs/A_server.log
```

**On B** (separate shell/machine):
```bash
/workspace/exp2/launch_node.sh B 0 30001 8766 http://<A_HOST>:8765 \
  2>&1 | tee /workspace/exp2/logs/B_server.log
```

Default model is `Qwen/Qwen3-8B`. No CPU pinning (each node has its own machine).
Wait until both print `Server is ready` (a few minutes for 8B — weights download
on first run).

Verify (from B, or any machine that can reach both):
```bash
curl http://<A_HOST>:30000/health   # → "ok"
curl http://<B_HOST>:30001/health   # → "ok"
curl http://<A_HOST>:8765/agent     # → {"agent": "A-...", ...}
curl http://<B_HOST>:8766/agent     # → {"agent": "B-...", ...}
```

**Troubleshooting – kv-events-config:**
If the server fails with an argument error, check the exact key names:
```bash
/opt/sglang/bin/python -c "
import sglang.srt.server_args as s, inspect
print(inspect.signature(s.ServerArgs.__init__))
" 2>&1 | grep kv_events
```
Adjust `KV_CFG` in `launch_node.sh` accordingly.

---

## Step 2 – Probes

`probe_events.py` and `probe_eviction.py` run **on A** against localhost.
`probe_health.py` runs **on B** against both nodes over the network.

**On A:**
```bash
cd /workspace/exp2

# 2a. KV events (15 s per prompt, ~35 s total)
python probe_events.py --port 30000 --kv-events-port 30200 --out-dir results
# Expected: CPU_PINNED stored: YES    Parent hash chain: OK

# 2b. Eviction (40 × 2K prompts then 60 s collection)
python probe_eviction.py --port 30000 --kv-events-port 30200 --out-dir results
# Expected: BlockRemoved GPU≥1   CPU_PINNED≥1
```

**On B:**
```bash
cd /workspace/exp2

# 2c. Health (polls both nodes; ~25 s)
python probe_health.py \
  --a http://<A_HOST>:30000 --b http://<B_HOST>:30001 \
  --a-meta http://<A_HOST>:8765 --b-meta http://<B_HOST>:8766 \
  --out-dir results
# Expected: All required health fields present.
```

---

## Step 3 – Sweep (~2–4 hours, run on B's machine)

```bash
cd /workspace/exp2
python sweep.py \
  --a http://<A_HOST>:30000 \
  --b http://<B_HOST>:30001 \
  --a-meta http://<A_HOST>:8765 \
  --b-meta http://<B_HOST>:8766 \
  --lengths 128,256,512,1024,2048,4096,8192,16384 \
  --warmup 3 --trials 20 --suffix 128 --seed 42 \
  --out results/sweep.csv
```

**Validity checks during run:**
- `recompute`: `cached_tokens == 0`
- `pull`:      `cached_tokens == L`
- `local`:     `cached_tokens >= L`

Invalid rows land in the CSV with `valid=false` and are excluded from fits.

---

## Step 4 – Interference (~30 min, run on B's machine)

```bash
cd /workspace/exp2
python interference.py \
  --a http://<A_HOST>:30000 \
  --b http://<B_HOST>:30001 \
  --a-meta http://<A_HOST>:8765 \
  --prefix-len 4096 \
  --streams 8 --phase-secs 60 \
  --rates 0,0.5,1,2 \
  --out results/interference.csv
```

Output is labeled: *measured with the two servers on separate machines, pulls crossing the network.*

---

## Step 5 – Go offline tests (local, no GPU needed)

```bash
cd /path/to/proxy

# Event decoder + writeCache
PROBE_DIR=/workspace/exp2/results \
  go test ./services/router/ -run TestOffline -v

# Hasher chain
PROBE_DIR=/workspace/exp2/results \
  go test ./services/request/ -run TestHasherOffline -v
```

Copy `results/` from the cloud to the proxy machine first. If the hasher test
fails, compare `results/events_decoded.jsonl` `block_hashes` with the computed
values printed by the test, then adjust `hashOnePage` in
`services/request/request.go`.

---

## Step 6 – Analysis (local)

```bash
cd /workspace/exp2
python analyze.py \
  --sweep results/sweep.csv \
  --interference results/interference_summary.json \
  --out-dir results/plots
```

Prints:
- Measured median/p95 TTFT per length and condition
- Linear fit (pull) and quadratic fit (recompute) with break-even L*
- **Modeled-network table**: for 1, 5, 10, 25 Gbps nominal rates, replaces
  `read_ms` with `bytes_read / (rate / 8)` and reports L* per rate.
  Labeled *modeled, nominal line rate* – real links run slower.
- TPOT p50/p95/p99 per interference phase with ratio to baseline

---

## Files produced

| File | Description |
|------|-------------|
| `logs/A_args.txt` | Full SGLang command for A |
| `logs/A_pull.jsonl` | Per-pull NIXL metrics (A side) |
| `logs/B_pull.jsonl` | Per-pull NIXL metrics (B side) |
| `results/events_raw.jsonl` | Raw ZMQ frames (base64 payload) |
| `results/events_decoded.jsonl` | Decoded KV events |
| `results/events_summary.json` | Token IDs + summary for Go test |
| `results/eviction_*.jsonl` | Eviction probe frames |
| `results/health.jsonl` | Health snapshots |
| `results/sweep.csv` | TTFT measurements |
| `results/interference_*.csv/json` | Interference measurements |
| `results/plots/ttft_vs_length.png` | Measured + modeled curves plot |