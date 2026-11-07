Compare SGlangs L3 storage and us
We test metrics on NodeA and NodeB transfers. NodeA churning its own requests, and NodeB pulling
We test for different bytes moved aka diff prefix lengths 
(we can have the test suite 2048,8192,24576) (because i am tired)

Setup being A is 0%, 80% and 100% saturation and its churning its own req

Metrics:
On NodeB:
- TTFT
- effective speed
On NodeA:
- TPOT p95, against its churn baseline GPU utlisation, CPU utilisation

the file storage can just be one of the hosts' folders
launch both servers with `--hicache-storage-backend file` and `--hicache-write-policy write_through`

run with the 25 mps percentage

and also, we should trottle the network transfers for both the paths (tc for NIXL and `--device-read-bps` on the container for the disk). because otherwise, its just not fair. (granted, we should have also added throttle to phase 0. i wanted to i forgot about it guys :sob)

A cant be made warm, because we want to test our ENTIRE communication system, as in, time it takes for A to write to host memory/write to disk storage. because this is an eagre process, we cant warm A because then we loose this time. this time needs to be part of the system.

so, for a given prefix length, and a given saturation point, we run exp 5 (i dont want to spend that much time) times. so total numbers will be 
num_prefixes(3)*num_saturation points_on_a(3)*5 = 45runs

coming back to this time for write (tfw), it has another dimension to it. the tax on A's resources when writing, mainly the GPU, CPU and disk. because these writings are eagre, its important to quantify this work done for pages nobody may ever pull (tbh, this can be a seperate phase)

so, what we can do is, lets call this phase2.1, where for our different prefix lengths, we measure this tfw and tax of nodeA's hardware (no pulling is needed here) and ran for the same above configs of 45runs. we can also measure one run each cell where we dont have a write through and no l3 backend. as a baseline. this is fair. so a total of 45*3=135 runs (us, l3, baseline)

then, we can run our phase2.2, where we follow a similar model as our phase 0, where we warm nodeA with a given L, ask B to pull and measure on B (only the time it takes to pull and add back to our host memory)

so, for every 5Ls per prefix, we warm nodeA, and the same 0, 80, 100 saturation points
- our script sends those L+128_suffix to nodeB, we measure from until ready to pull ttft and effective speed
- l3 also, measures from ready to pull untile ttft and effective speed

we also need to have throttle consistency between the both phases. exact same throttles
actually, we can merge the 2 phases even because thats better
at the end of each cell of phase 2.1, we can also just then and there trigger the phase 2.2 on node B, where we ask B to pull and measure the ttft and effective speed. the only thing with this is that it will require ME to trigger the scripts from different nodes each time since they are on diff nodes altogether. maybe we can come with a way to automate the pull from nodeB


### agent instructions
Write phase-2/run_phase2.py. It runs on the host (stdlib only, no pip installs) and runs unattended for one arm of Phase 2 per invocation. It uses docker exec nodeA|nodeB for anything node-local and HTTP to the SGLang servers (ports from the project config.json; resolve container IPs with docker inspect on kvnet).

Flags: --system {nixl,l3,baseline}, --lengths 2048,8192,24576, --sat 0,80,100, --trials 5, --throttle-gbps (optional), --gpu-a, --gpu-b, --mps-label, --evict {fadvise,dropcaches,none}, --l3-dir, --cont-a, --cont-b, --config, --seed.

Inputs. Documents come from phase-1/data/L_docs_{length}.json, and MSP comes from phase-1/results/msp_{length}.json (msp_req_per_s). Churn rate is sat% × MSP. Inspect phase-1/add_load for its flags. Use docs 0 to 14 as targets (5 per saturation level, never reused within an arm, because A must never have seen a target before). Use docs 15 to 24 as churn documents. If add_load can’t select documents or log per-token gaps with timestamps, add the minimum flags needed. Don’t change its behavior otherwise.

Startup checks. Read /get_server_info on both nodes. Abort with a clear message if the servers don’t match --system: nixl is --hicache-storage-backend dynamic with write-through, l3 is file with write-through, and baseline has no storage backend and no write-through. Save both server-info JSONs, the git commit and all flags in run_meta.json. Measure a floor once: the median TTFT of a 128-token prompt on idle B, over 10 requests. Calibrate the throttle: for nixl, measure the A to B throughput (iperf3 if present, otherwise a small socket test); for l3, run a direct-I/O dd write and read of 2 GB in the L3 folder. Save the results in calibration.json. If --throttle-gbps is set, apply tc qdisc replace dev eth0 root tbf rate Xgbit burst 64mbit latency 50ms on nodeA. Always remove it in a finally block.

Per cell (length × sat). Flush both nodes. Start add_load in the background at the chosen rate, restarting it if it dies. Skip it for sat 0. Warm in for 30 s, then record a 60 s churn-only baseline window. Then run --trials runs, with churn continuing. Stop the churn at the end of the cell and wait for A’s running and queue counts to reach 0 before the next cell.

Per run.

Write window. Send A target_doc + fresh random 128-token suffix (input_ids, max_new_tokens=1, streaming). Record A’s TTFT and end-to-end time.
Shareable time (tfw). Measured from the end of A’s response. Compute the document’s page keys exactly as SGLang’s HiCache does (find the function in the SGLang source, run it with docker exec nodeA python3, and cache the key lists in phase-2/data/keys_{length}.json). For nixl, poll A’s meta /keys?since=N until every key appears. For l3, poll until every key’s file exists in --l3-dir and the sizes stop growing. If you can’t compute the keys, stop and tell the user. Don’t guess. Time out after 10 minutes and mark the run failed.
Evict (l3 only). Run sync, then posix_fadvise(DONTNEED) on the document’s files (or drop_caches if chosen). Record the disk read bytes during step 4 to confirm reads hit the disk.
Pull window (nixl and l3 only). Flush B. Send target_doc + a different fresh 128-token suffix to B (max_new_tokens=1, streaming). Record TTFT, cached_tokens and prompt_tokens. A row is valid only if cached_tokens equals the length. Effective speed (Gbps) is L × kv_bytes_per_token × 8 / (TTFT − floor) / 1e9, with kv_bytes_per_token computed from the model’s config.json. Also record the end-to-end time from A’s request start to B’s first token.
Cleanup. Delete this document’s L3 files (l3). Record total elapsed time.
baseline does steps 1 and 5 only. It doesn’t poll keys or pull, and it only measures A’s tax.

Sampling. During every cell, sample once per second: A’s GPU utilization and memory (nvidia-smi for --gpu-a), A’s container CPU, A’s and B’s network tx and rx bytes, disk read and write bytes for the device behind --l3-dir, and A’s running and waiting counts from /metrics. Compute A’s TPOT p50 and p95 for the churn-only baseline window, the write window and the pull window, from the churn script’s per-token gaps. Fall back to deltas of the Prometheus time_per_output_token histogram if the gaps aren’t logged.

Robustness. Retry a failed run once, then log it and continue. Write results incrementally. Save progress in progress.json so a re-run skips finished cells. Use timeouts on every request. Clean up in finally. Print a table at the end.

Output (to phase-2/results/<system>_<timestamp>/): a per-run CSV (system, length, sat, trial, doc, tfw, A TTFT, B TTFT, cached_tokens, valid, effective_Gbps, end-to-end, throttle), a per-second samples CSV, the raw token gaps, a per-cell summary (medians and p95, valid counts, A’s TPOT against its baseline, mean GPU, CPU, disk and network), calibration.json and run_meta.json.

Rules. Don’t change the parameters above. Keep invalid rows and count them.

cap network at 6gbps

docker run -d --name nodeA --network kvnet --gpus device=GPU_A --shm-size=16g --ulimit memlock=-1 --cap-add NET_ADMIN --device-read-bps /dev/md0:750mb --device-write-bps /dev/md0:750mb -v $HOME/Naween/shaizah/kv-cache:/root/kv-cache -v $HOME/Naween/shaizah/models:/models lmsysorg/sglang:v0.5.19-cu129 sleep infinity
