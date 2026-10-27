import os, sys, time, csv, statistics
os.environ["UCX_TLS"] = "tcp,cuda_copy"  # plain "tcp" is rejected by NIXL when a GPU is present
import multiprocessing as mp
import torch
from nixl._api import nixl_agent, nixl_agent_config

PAGE = 64 * 147456
HALF = PAGE // 2
LENS = [1024, 4096, 8192, 16384]
MAXB = (16384 // 64) * PAGE

def descs(base, L):
    n = (L // 64) * 2
    return [(base + i * HALF, HALF, 0) for i in range(n)]

def a_proc(conn):
    buf = torch.empty(MAXB, dtype=torch.uint8).pin_memory()
    buf.fill_(1)
    ag = nixl_agent("A", nixl_agent_config(backends=["UCX"]))
    ag.register_memory([(buf.data_ptr(), MAXB, 0, "")], "DRAM")
    conn.send((ag.get_agent_metadata(), buf.data_ptr()))
    conn.recv()  # done

if __name__ == "__main__":
    mp.set_start_method("spawn")
    pa, ca = mp.Pipe()
    A = mp.Process(target=a_proc, args=(ca,)); A.start()
    meta, abase = pa.recv()
    buf = torch.empty(MAXB, dtype=torch.uint8).pin_memory()
    ag = nixl_agent("B", nixl_agent_config(backends=["UCX"]))
    ag.register_memory([(buf.data_ptr(), MAXB, 0, "")], "DRAM")
    peer = ag.add_remote_agent(meta)
    peer = peer.decode() if isinstance(peer, bytes) else peer
    def read(L):
        src = ag.get_xfer_descs(descs(abase, L), "DRAM")
        dst = ag.get_xfer_descs(descs(buf.data_ptr(), L), "DRAM")
        h = ag.initialize_xfer("READ", dst, src, "A")
        t0 = time.perf_counter()
        st = ag.transfer(h)
        while st not in ("DONE",):
            if st == "ERR": raise RuntimeError("xfer err")
            st = ag.check_xfer_state(h)
        ms = (time.perf_counter() - t0) * 1000
        ag.release_xfer_handle(h)
        return ms
    rows = []
    for L in LENS:
        read(L)  # throwaway
        nbytes = (L // 64) * PAGE
        ts = [read(L) for _ in range(10)]
        for i, t in enumerate(ts): rows.append((L, nbytes, i, t))
        print(f"L={L} bytes={nbytes} median_ms={statistics.median(ts):.2f} median_GB/s={nbytes/1e9/(statistics.median(ts)/1000):.3f}", flush=True)
    with open("results/nixl.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["L", "bytes", "trial", "read_ms"]); w.writerows(rows)
    pa.send("done"); A.join()
