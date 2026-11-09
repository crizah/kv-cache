- the eviction policy thing for batch_set_v1
- our helpleness towards pinning cpu while reading
- only allowing template req /generate and such
- only one pull at a time right now (not only do we not support concurrency on pulls rn, we CANT handle another pull req while a pull is still being processed as it will execute with the wrong fields. its a correctness issue and we will have to set request level locks for it)
- context.Cancel() equivalent on the python agent
- not having an eviction policy for batch_Set_v1 is also a correctness issue, our cache can be stale, give gardbage or even wrong values when overwritten. all fixable with an eviction policy, but we aint doing that rn
- the numbers for the "golden number" we got was over local host network transfer and a relatively smaller model and also coupled to the hardware we ran it on
- this golden number of 5k derived from 0sat 10gbps transfer
so with ANY saturation levels, <5k prefix, recompute is always faster
current design of the experiments dont tell us "at a saturation 80%, is giving to A slower than pulling into B"

the calinerations arent the best and there is an inherent difference between our load adder and our proxy load determiner
