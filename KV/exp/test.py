import requests, json, random, sys, time
from transformers import AutoTokenizer
tok = AutoTokenizer.from_pretrained("Qwen/Qwen3-0.6B")
def mk(seed, n=1500):
    r = random.Random(seed); w = "alpha river stone cloud engine violet market signal garden harbor copper lantern meadow pixel quartz".split()
    ids = []; s = f"Document {seed}: "
    while len(tok(s).input_ids) < n: s += " ".join(r.choice(w) for _ in range(50)) + ". "
    ids = tok(s).input_ids[:n]
    return tok.decode(ids)
P, Q = mk(1), mk(2)
def gen(port, text):
    r = requests.post(f"http://127.0.0.1:{port}/generate", json={"text": text, "sampling_params": {"temperature": 0, "max_new_tokens": 32}}).json()
    m = r["meta_info"]; return r["text"], m["prompt_tokens"], m["cached_tokens"]
step = sys.argv[1]
if step == "2": print("Q->B", gen(30001, Q))
if step == "3": print("P->A", gen(30000, P))
if step == "4": print("P->B", gen(30001, P))
if step == "4a": print("P->A again", gen(30000, P))
