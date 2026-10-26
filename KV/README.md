this repo witll contain the code for the load balancer as well as the code for the node agent
load balancer in Go and agent in Python because thast just easier

exp/ contains the code for the python experiment
hardware specifications
- design choice for the agent and load balance. when doing a p-to-p trasnfer, we should probably not have node b also push to its own cpu and store the same hashes in the global router as well i think. because thats just redundant. 
- also, i think we can compare our system against Hicacjhes existing L3 sharing. Ours shares L2 over netwowrk directly. Hicaches built in system write from L2 into a file (given this is lazily, but i dont think the L1 to L2 can be made lazy. i.e, on demand) and we can read from there. I think we can use this as a basline as well
- request queuing, token-aware load and flow control are out of scope for this prototype"
- also, chat templates is out of scope for this demo

```
GPUs	2x NVIDIA RTX 5060 Ti, 16 GB each (31.9 GB total)
GPU compute	45.6 TFLOPS total
GPU memory bandwidth	368.3 GB/s
Max CUDA	13.1 (driver 590.48.01)
CPU	AMD EPYC 7C13, 64 cores (51.2 of 128 vCPUs allocated)
RAM	103 GB
Disk	T-FORCE TM8FFH004T NVMe, 2,916 MB/s read speed, 24 GB allocated
PCIe	4.0 x8
Network	about 2.3 Gbps up and down
```
```
 SGLang 0.5.20 (`/opt/sglang`) 
 nixl / nixl-cu13  1.4.1 (the doc's standalone test used 1.5.0) 
 UCX  bundled in `nixl_cu13.libs` (`libucp`/`libucs`); `ucx_info` isn't installed, so no version number 
 Python  3.12.3 
 NVIDIA driver | 590.48.01 (CUDA 13.1) |
 PyTorch  2.13.0+cu130 
 ```

###  setup for the throwaway experiment
- `Qwen/Qwen3-0.6B`, 0.6B params.
- Two SGLang servers on one machine, one per GPU. The NIXL transfer ran over local TCP. (the gpus and the cpu memeory pools were seperated, and the CPU cores were not pinned)
- Ran prompt P on Node A, ran a baseline on prompt Q on node B -> 0 cached tokens on node B
- Ran prompt P on node B -> 1472 cached tokens on node B
- The standard NIXL backend for SGLang only comminicates agent to storage for traditional nodes. Ours comminicates agent to agent with NIXL (mirroring communication protocol SGLang has for PD servers)

### Report 

yes, Server B got a prefix cache hit on prompt P`, which only server A had computed. The KV pages reached B through a NIXL READ from A's host (CPU) memory.

#### evidence

Page size 64. P is 1500 tokens, so 23 full pages = 1472 tokens.

 Step 2: `Q` -> B (control) prompt_tokens 1500, `cached_tokens` = **0** 
 Step 3: `P` -> A  A's log: `published 23 pages (no copy)` 
 Step 4: `P` -> B  `cached_tokens` = **1472** (= 23 x 64) 
 Bytes read  168,820,736 = 23 pages x 7,340,032 bytes (K and V for 28 layers per page), in **327.5 ms**
 Output equality B's output for `P` equals A's, character for character (keeping in mind this was a very small model)

B's log lines for step 4:

```
[peer_nixl] batch_exists: 23/23 pages present on peer A
[peer_nixl] NIXL READ from agent A-77cba0e8: pages=23 descs=46 bytes=168820736 in 327.5 ms
```

### Limitations/Caveats/General thots

- current load balancer only runs on linux x86 64. we can probably add support later on for others because its only an issue of the tokeniser liberary. TODO: read architecture and change env var respectively

- Current version of the code doesnt pin any slots. So theoretically, the pages in A could be ovewritten. Pin in actual design
- B's own pages are also published to A (23 pages, from `Q`), since both servers run the same backend. (this is okay for different prompts, but for the same hashes, we shpuld not publish, only have B publish the newly computed prefill values)
- Ran a small as fuck model, so quality isnt something that was tested 
- This was only a test to see if transfer over NIXL, assisted with SQLangs Autocache is evern possible, therefore the 327.5ms time over a local network is not to be taken at face value
- It is probably not a good idea to publish from GPU to CPU on every request, it hinders GPUs performace and is a huge bottleneck, maybe we can do smth about this? because currently what we are doing is regardless of if we will need to transfer the cache, Autocache pushes the cache into L2 (CPU) (granted, it doesnt does it by pages and doesnt push cache existing in L2 redundantly). We use this flag for this `--hicache-write-policy write_through`. Other options for this flag are `write_back` and `write_through_selective`. `write_through_selective`. This guys only copies onto L2 if its a hotspot. Not sure if we can use this for our case tbh and `write_back` only writes upon eviction so nah. If we could find a way to pull to L2 lazily (on demand) that would be great otherwise this seems a bit eek.

If we wanted direct GPU to network transfer, we would need an RNIC. Nxidia's Mellanox and such which idk about this one

- lets talk about the eviction policy. we will follow LRU and much of it on the nodes itself should be handled by HighCache
for the GPU, we need to be set `--mem-fraction-static` limited by the GPU's memory. I think we should have defaults for this handled by Hicache itself. Ill have to check
CPU limits set by `--hicache-ratio` or `--hicache-size`, and it must be larger than the GPU pool

what we do need to make sure of is is WHILE node B is reading from A's cpu, it cant be overwritten evne if the CPU pool is used up

another thing we need to amke sure off is telling the proxy about stale pages. I think w can do this with the heartbeat. Have the agent send the expired hashes to the proxy since the last heartbeat is probably the way to go. we can sync with timestamps, not need for any sort of local storage.

Okay, i think it might already support this. MIGHT. Below doc also has support for cache addition. and they say SGLang also broadcasts event over a ZMQ socket.
https://docs.nvidia.com/dynamo/advanced-customizations/writing-custom-backends/publish-kv-events
the docs do seem a bit muddy but, w can try this at startup
`--kv-events-config '{"publisher": "zmq", "topic": "kv-events"}'` and have our proxy subsribe to it. otherwise, there is always, send with heartbeat

Note: I think we can also use this as one of the baselines for comparison. Dynamos framework for SGLang

### Workings of the throwaway experiment

- `interface_v1: 1` in the extra-config makes HiCache give the page positions to `batch_set_v1` and `batch_get_v1`. Hicache does not do this by default, it give data to copy by default
- After prefill runs, HiCache copies cache from As GPU into As CPU.`batch_set_v1` records the address of each page's K and V regions in the host KV pool. It copies nothing (keeping in mind that the cache itself is already copied via HiCache, we just avoid doing allat again). The whole pool is registered with NIXL in `register_mem_pool_host`.
- Puller, `batch_exists` / `batch_get_v1`: `batch_exists` checks As key map. `batch_get_v1` does one NIXL READ from A's registered memory straight into B's host-pool buffer.
- Metadata exchange: a small JSON file (agent metadata) and an append-only key map in `/exp/meta/`. No KV bytes go through it.

### Files

- `peer_nixl_backend.py`: the agent
- `launch.sh`: run as `./launch.sh A 0 30000 B` and `./launch.sh B 1 30001 A` // name, gpu, port, peer
- `test.py`: request steps (`2`, `3`, `4`) (2 gives prompt P to A. 3 given prompt Q to B (cache miss), 4 gives prompt P to B and we see a cache hit)
- `A.log`, `B.log`: server logs

### Launch commands
in launch.sh
args are name, gpu, port, peer

### alr for the actual load balancer
components
- route table (this will need to have {hash, [node_ips]})
We are gonna keep this as a in memory thing i aint doing allat

- the heartbeat moniter (i wonder if there is an existing library taht takes care of the boilr plate code for me, this stuff is pretty standard)
okay there is Gin Proxy but knowing Gin, NUH HUH
eh ukw we can write it ourselves
i think i can just have the agent expose a /health endpoint
and i think we should also store this information in memory

- the cache evictor (subscribes to the nodes' ZMQ sockets)

- the tokeniser and hasher (ts needs to be lightweight) to tokenise and hash incoming requests (NOTE: make sure the hashers used are uniform throughout)

- the actual routing logic (this will be caliberated to the results of "break point on prefill length where trasnfer becoumes cheaper than recomputation (altough, my results will be driven my a model and will heavily depend on the model size but what are you gonna do))
user req -> tokenise -> hash prefix wise based on SGLangs block size (64 i think?), if the any hash doesnt exist, send to the best node (i think just round robin for this or last used?) -> if hashes do exist, match largest prefix hash to the node ip -> if node is chilling (have this thershold be configurable), send it to her -> if the node is not chilling, find one that is chilling -> send to him with headers {hash, producer_ip, block_id} and such -> the agent then takes care of the rest

also, the route table is updated in background as its subscribed to the event. the agent need not return new hashes along with its response

- also, if we have cache hits on multiple nodes, load is the tie breaker


### the general agent design
we have a general agent design working currently. Need to add a few things on top of it
- the /health endpoint (have it report current load plus queued shit)

we d0onmt need a dedicated endpoint. see sg lang design below

- mechanism to pin the pages in the CPU while they are being read  (NOTE: check how we can do this, because if we cant, a missing or overwritten page needs to be recoumted by B)
Also, add a timeout on the pin, and also on the communication between A and B in general. If it surpasses, have B recompute (current test waits for the computer, we should add a timeout)
- when a pull fails, we broadcast and remove hash from route table. i dont know how reliable ZMQ's socket is, its TCP so its SHOULD be fine

### the sg lang design
- start with `interface_v1: 1`, `--kv-events-config '{"publisher": "zmq", "topic": "kv-events"}'`, `--hicache-write-policy write_through`,  `--mem-fraction-static`, `--hicache-ratio` or `--hicache-size`
- `--kv-events-config '{"publisher":"zmq","endpoint":"tcp://*:5557","topic":"","replay_endpoint":"tcp://*:5558","format":"default"}'` for the zmq socket, publishing kv cache.

```
┌─────────────────┬────────────────┬───────────────────────────────────────────────────┐
│      field      │    default     │                      meaning                      │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ publisher       │ "null"         │ "zmq" to enable, "null" is a no-op                │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ endpoint        │ "tcp://*:5557" │ PUB socket; * = bind-all, concrete host = connect │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ bind            │ null (auto)    │ override bind vs connect                          │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ replay_endpoint │ null           │ optional ROUTER socket for missed-batch replay    │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ buffer_steps    │ 10 000         │ how many past batches to buffer for replay        │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ hwm             │ 100 000        │ ZMQ high-water-mark before drops                  │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ max_queue_size  │ 100 000        │ internal Python queue before drops                │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ topic           │ ""             │ ZMQ topic prefix                                  │
├─────────────────┼────────────────┼───────────────────────────────────────────────────┤
│ format          │ "default"      │ "dynamo" for NVIDIA Dynamo-compatible format      │
└─────────────────┴────────────────┴───────────────────────────────────────────────────┘
```

```
Wire framing (every message is three ZMQ frames):

[topic_bytes] [seq: 8-byte big-endian int64] [msgpack payload]

The payload is a msgpack-encoded KVEventBatch which decodes as a positional array: [ts, events, attn_dp_rank].

Event types — events is a list of tagged msgpack maps, each with a type key:

BlockStored

{
  "type": "BlockStored",
  "block_hashes": [int, ...],       # list of int64 hashes
  "parent_block_hash": int | None,  # hash of the preceding block
  "token_ids": [int, ...],          # actual token IDs that fill this block
  "block_size": int,                # tokens per block
  "lora_id": int | None,
  "medium": "GPU" | "CPU_PINNED" | "DISK" | "EXTERNAL" | None,
  "cache_salt": str | None,
  "session_id": str | None,
  "lora_name": str | None,          # only in "dynamo" format
}

BlockRemoved

{
  "type": "BlockRemoved",
  "block_hashes": [int, ...],
  "medium": "GPU" | "CPU_PINNED" | ... | None,
}

AllBlocksCleared

{"type": "AllBlocksCleared"}

```
okay so these people have a design which eugh

when moving from L1->L2 we get 2 events
1. BlockRemoved with medium="GPU" // its goine from GPU
2. BlockStored with medium="CPU_PINNED" // its now on host

when L2->L1 back
1. BlockStored with medium="GPU" // it's back on GPU
2. (The CPU copy may then emit BlockRemoved with medium="CPU_PINNED" when reclaimed) // they only do this if CPU runs out of its max capacity???

so we cant rely on these events as a whole, we need to track GPU and CPU states independantly and on each request, use a styate machine to figure out where the state of shit is
use transactions when writing double event events (like the ones above)
since we are using write_through, we should see write to GPU and later on see a write to CPU event

NOTE: also keep in mind, the actual kv cache doesnt use any hashing. it searches by tokens only. KEEP THAT IN MIND


- `-load-publish-endpoint tcp://*:6000` for health pretty much
[b"load"] [seq 8B big-endian] [msgpack LoadStat]. The LoadStat payload is a positional array:
["LoadStat", num_running_reqs, num_waiting_reqs, num_tokens, max_total_num_tokens, attn_dp_rank, ...]

Requires `--kv-events-config` to be set with a TCP endpoint

### comparison
compare against SGLangs L3 cache sharing. that would be fun
maybe we can compare against llm-d(we should def do this one, its a very similar design but for vLLM) and a dedicated kv aware scheduling algo (like what was is called cacheroute or smth)
im not a 100% sure on how nvidias Dynamo framework works, but from what i understand, it does a similar routing logic. but the thing is, idek if this method is like worth it all in the first place guys. like, this might just absolutely tank on latency

I think we should do a test for the "break point between latency and recomputation for prefix lengths" experiment first
what does llm-d do? it mush have a similar mechanism
okay so, they have a script that caliberates this shit for ur model, GPU and transport which is neat. we will not be doing that for this run :sob:
But since they have it caliberated dynamically, the experimental comparison should be fair.

obv we wont be able to make this super sophisticated to survive actual production workloads, but this is just a proof and not a prod ready software

- i think we should make the offilne shit first
- and have a startup script i can just run to save on costs
- once we get out agent and proxy working, we can carry out the experiment to get the sweet number. will also need more CPU ram so i can use a larger model
(like 100B+ lets see)
