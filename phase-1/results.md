plan is to run on 25% (and MAYBE also 50%) capacity of h200 pro
we get the saturation point for nodeA on this capacity

- maximum saturation poine (MSP): the highest req rate nodeA can sustain while ttft p95 stays under an SLO and waiting queue doesnt grow (maybe we can have a range for this? but like come on man. lets keep 2s?)

we select a range of prefix lengths (maybe not all the lengths we tested for because i dont want to spend 100 hours) [2k, 8k, 16k, 24k]

and a range of saturations points = [50, 75, 100, 125] percent of MSP (we can tweak these numbers)
and we measure TTFT (p50 and p95), TPOT (p50 and p95), end-to-end latency p95, cache hit rate (cached tokens over prompt tokens)

### setup:
- nodeA is the load bearing node with varying saturation points 
- for each load balancer type, we measure for requests of multiple prefix lengths (lets give each 20 runs?) and multiple saturation points for A 
so a `num_gpu_capacities(total of num_load_balancers(num_prefix_len * 20 * num_of_saturation_points))`. yikes
- run the proxy on unis servers itself, no gpu, on the same docker network as the 2 nodes

### what we need for this
- script to measure nodeA's saturation point:

send workload to A directly (not via a proxy) with Poissons arrival at 1, 2, 3.. req/s for 60secs. the workload is cache hit requests with a 128 token suffix and 256 output tokens. The last passing rate is the MSP. log nodeA's running and waiting counts at each step

- script to incrementally increase the saturation of nodeA :

A load level is a fraction of the MSP used as the offered rate. The load generator needs a --rate flag and nothing more, so no separate script to raise saturation. (wait for a bit after each saturation increase to let proxy cache table catch up, throttled/poissons arrival should take care of it but just in case)

- get the tokeniser for the 32B model
- script to test the kv events and health events are being broadcasted and captured appropriately on the proxy
- change `"loaded_threshold": 8` in the config to running + waiting count at 80% of the MSP and the config for the new model
- finish cacheRoute logic
- make a binary out of the go proxy, dont want to install go on the machines
- also, compare exp2's launch.sh and our lauch.sh
- and also, subscribe to node GPU metrics

### more setup lol

- we cant pin caches (as far as i know)
- we need to have L prompts on nodeA warm. these L prompts are the ones we will use for our measurment for a pull. they have a prefix length of n (for that given run). these exist on nodeA. lets keep L at like 30? 25.

- then, our script adds load to nodeA (based on msp percentage) and we wait for a little bit for all health and kv events to be propogated to proxy. this script needs to KEEP A at the desired saturation level until we get all our numbers. we also need to make sure that the L cached prefixes dont get evicted from A as we send the reqs, since we have no way of cpu pinning. so, the reqs that this loading script sends, should def contain the L prefixes plus random suffix, so A still does cmputation AND we ensure that the L prefixes arent evicted. we should also keep hi-cache-ratio rather large (lets say 300?)

- then for the actual test via the proxy, we will send a mixture. we should contain the L cached prefix along with a couple new prompts as well. for a good distribution. but pull only happens on those L prefixes. and the pull IS what we want to measure, but the randomness adds real data, and increases load on nodeB as nodeA reaches its saturation point. (have a ratio of 2:1 i think for cached:new). with this, the best possible hit rate is about 67% (hehe 6 7) (sorry)

### what the agent needs to write for me

- the saturation finding script:
client: nodeA

for every prefix length [2k, 8k, 16k, 24k], flush nodeA between each run
0. Prepare the L cached documents (store this, we will resuse this guy and this stays on a shared folder, so all containers can read it. dont worry about that, ill mount appropriately)
1. Prepare A. Send each of the L cached documents once, so they’re in its cache.
2. Use realistic traffic. Each request reuses one of those documents, adds a fresh short suffix, and asks for 256 output tokens. This matches the background load you’ll use in the experiment.
3. Start slow. Send requests at 0.25 per second for 60 seconds, with random gaps between them (Poisson arrivals). Don’t wait for one to finish before sending the next.
4. Check each step. It passes if p95 TTFT is under 3 seconds, nearly every request finished, and the waiting queue at the end isn’t bigger than at the start. Read the queue from A’s metrics.
5. Speed up. Rest for 30 seconds so the queue drains, then raise the rate by 25% and repeat. Stop at the first step that fails.
6. Report. The last passing rate is the MSP. Confirm it by running that rate once more, and if it fails, take the step below. Save a table of rate, p95 TTFT, finished over sent, and queue size, plus one MSP number in a file.

have this script be runnable with a flag where we can mention the the prefix length we want to run it for.
have another flag that only generates the L documents, stores it, doesnt do anything to A
and then the entire suite for a prefix length, checks that document where L is to be stored, if it doesnt exist, geneartes it and runs the rest of the steps. otherwise, resues. L is 25 btw

- the load adding script
client: nodeA

runs only after we run the above script
this also has flags for MSP and for prefix length
this maintains a load of x of MSP, where MSP for that prefix length is determined from the above exp. hardcode values for now and comment that these need to be filled up

for that prefix length, it looks is the L document, at startup, warms nodeA with those L documents, and then maintains the x load on nodeA (for x , the script sends 0.x*MSP reqs per second with random gaps) by sending requests of 128 tokens, but reuses the prefixes from L, so as to ensure we dont evict them.

- script to test if the kv events and load events and usage events are being written to the proxy (just a one off test)
client: proxy? im not sure on it

- the actual exp script
client: proxy
sends each req through the proxy and measures timings according to the metrics

again, flag for the prefix length we are testing rn
this script sends 75 total requests, we a 2:1 ratio for cached vs new. the cached requests (50) will get a random 128 token suffix attached. and the new ones will be the same length as prefix_lenght+suffix length

we measure for 
- TTFT(time from sending a request to the first streamed token), 
- end-to-end latency(time from sending to last token),
- TPOT (end-to-end minus TTFT) divided by (output tokens minus 1). With --stream-interval 1, each streamed chunk is one token)
- cache hit rate (cached tokens over prompt tokens) (max, arounf 65%)

Cache hit rate. For every request, read cached_tokens and prompt_tokens from the meta_info in the last streamed chunk of the proxy. Hit rate for a request is cached_tokens / prompt_tokens. Report the mean over all 75 measured requests, and separately over the 50 cached-document requests.

for each request, we also need to record who served it, who we pulled from etc. the script, for every req, sends the req with a X-Request-Id, and our proxy returns headers with X-Request-Id (same as the one we sent), X-Served-By (the node that processed this req) and X-Pulled-From (if we had a pull, this cna be empty)
we log this, so we know which req was processed by which node
