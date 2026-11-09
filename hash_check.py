#!/usr/bin/env python3
"""Verify that SGLang's hash and the proxy's hash agree on random token pages.

Run inside the SGLang container (needs sglang installed):
    docker exec -it nodeA python3 /root/kv-cache/hash_check.py

Or on the host if sglang is importable. It prints PASS/FAIL per test case.
"""

import hashlib
import random
import struct
import sys


# ── Proxy algorithm (re-implemented from Go) ─────────────────────────────────

def proxy_hash_page(tokens: list[int], prior_digest: bytes | None = None) -> bytes:
    """Mirrors proxy's hashOnePage: SHA256(prior_digest? || tokens_as_LE_uint32)."""
    h = hashlib.sha256()
    if prior_digest and len(prior_digest) == 32:
        h.update(prior_digest)
    h.update(struct.pack(f"<{len(tokens)}I", *tokens))
    return h.digest()

def proxy_int64(digest: bytes) -> int:
    """Mirrors proxy's: int64(binary.BigEndian.Uint64(digest[:8]))"""
    val = int.from_bytes(digest[:8], "big", signed=False)
    return val - 2**64 if val >= 2**63 else val

def proxy_hash_chain(token_ids: list[int], page_size: int) -> list[int]:
    num_pages = len(token_ids) // page_size
    digest = None
    result = []
    for p in range(num_pages):
        page = token_ids[p * page_size : (p + 1) * page_size]
        digest = proxy_hash_page(page, digest)
        result.append(proxy_int64(digest))
    return result


# ── SGLang algorithm ──────────────────────────────────────────────────────────

def sglang_hash_to_int64(hex_str: str) -> int:
    """Mirrors hash_str_to_int64 from kv_events.py."""
    val = int(hex_str[:16], 16)
    return val - 2**64 if val >= 2**63 else val

def sglang_hash_chain_native(token_ids: list[int], page_size: int) -> list[int]:
    """Use SGLang's actual C++ hash extension."""
    from array import array as _array
    from sglang.srt.mem_cache.cpp_utils.native_hash import get_native_hash
    raw = _array("I", token_ids)
    hex_strs = get_native_hash(raw, len(token_ids), 1, False, None, page_size)
    return [sglang_hash_to_int64(h) for h in hex_strs]

def sglang_hash_chain_pure(token_ids: list[int], page_size: int) -> list[int]:
    """Pure-Python re-implementation of the C++ hash (uint32, no prior, non-wide)."""
    num_pages = len(token_ids) // page_size
    digest = None
    result = []
    for p in range(num_pages):
        page = token_ids[p * page_size : (p + 1) * page_size]
        h = hashlib.sha256()
        if digest:
            h.update(digest)
        h.update(struct.pack(f"<{len(page)}I", *page))
        digest = h.digest()
        hex_str = digest.hex()
        result.append(sglang_hash_to_int64(hex_str))
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def run(page_size: int = 64, num_tokens: int = 2048, seed: int = 42):
    rng = random.Random(seed)
    # token IDs in uint32 range, matching what a real tokenizer would produce
    tokens = [rng.randint(0, 150000) for _ in range(num_tokens)]

    proxy   = proxy_hash_chain(tokens, page_size)
    pure_sg = sglang_hash_chain_pure(tokens, page_size)

    try:
        native_sg = sglang_hash_chain_native(tokens, page_size)
        native_available = True
    except Exception as e:
        print(f"[warn] SGLang native extension not available: {e}")
        native_sg = None
        native_available = False

    print(f"\npage_size={page_size}  tokens={num_tokens}  pages={num_tokens//page_size}")
    print(f"{'page':>5}  {'proxy':>22}  {'sglang_pure':>22}  {'match_pure':>12}", end="")
    if native_available:
        print(f"  {'sglang_native':>22}  {'match_native':>13}")
    else:
        print()

    all_pass = True
    for i, (p, s) in enumerate(zip(proxy, pure_sg)):
        match_pure = p == s
        all_pass = all_pass and match_pure
        line = f"{i:>5}  {p:>22}  {s:>22}  {'OK' if match_pure else 'MISMATCH':>12}"
        if native_available:
            n = native_sg[i]
            match_native = p == n
            all_pass = all_pass and match_native
            line += f"  {n:>22}  {'OK' if match_native else 'MISMATCH':>13}"
        if not match_pure or (native_available and p != native_sg[i]):
            print(line)  # always print mismatches
        elif i < 3 or i == num_tokens // page_size - 1:
            print(line)  # print first 3 and last for a sanity sample

    print(f"\n{'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
    return all_pass


def write_vectors(path: str, page_size: int = 64, num_tokens: int = 2048, seed: int = 42):
    """Write token IDs and expected hashes to a JSON file for the Go checker."""
    import json
    rng = random.Random(seed)
    tokens = [rng.randint(0, 150000) for _ in range(num_tokens)]
    hashes = proxy_hash_chain(tokens, page_size)
    with open(path, "w") as f:
        json.dump({"page_size": page_size, "tokens": tokens, "hashes": hashes}, f)
    print(f"Wrote {len(tokens)} tokens → {len(hashes)} hashes to {path}")


if __name__ == "__main__":
    import json

    mode      = sys.argv[1] if len(sys.argv) > 1 else "check"
    page_size = int(sys.argv[2]) if len(sys.argv) > 2 else 64

    if mode == "write":
        write_vectors("/tmp/hash_vectors.json", page_size)
    else:
        ok = run(page_size=page_size)
        sys.exit(0 if ok else 1)
