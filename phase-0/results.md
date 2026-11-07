part 0 results
and qwen3-32B (:money) (lol gotta put the unis gpu to use guys)
ran sweep for 25%, 50% and 100% of h200 pro capacity

we measured ttft with breakdown of lookup and read times, measured across diff prefix sizes (each with 20 trails for a good median) for recompute vs pull 

```
--lengths 256,512,1024,2048,3072,4096,6144,8192,12288,16384,20480,24576,28672,32768 --warmup 3 --trials 20 --suffix 128 --seed 42
```
each results has a csv and also log filed in the tar ball
we still have to make sense of the data

largely depends on the hardware guys. i am GUESSING yall dont have a bunch of h200 pros sitting around
and so far, we only ran sweep.py, we should also run interference.py. would be interesting to see but i actually doubt A will show any substantial slow downs, if at all
what can be interesting to see is A's compute with write_through turned off vs on. bet that will have fun numbers

BIG NOTE: this does NOT take into consideration the latency we get from a nodeA writing to RAM with the --write-through. this only measures pure recompute vs transfer over an already written through cache on nodeA's RAM
and also, 
another BiG NOTE: these pull timings are like, best case scenario since I FORGOT TO ADD THROTTLE BRO HOW DID I FORGET TO ADD MF NETWORK THROTTLE. idk man, maybe we will redo this one because tbh these pull times are literally on the same machine over a docker network
