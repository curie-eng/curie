#!/usr/bin/env python3
"""Isolate the two regimes the first run conflated.

R1: pool deep enough to cover the burst -> every conversation finds a warm pod.
R2: arrivals slower than refill        -> the pool never empties.
Both are bounded by the shipped quota of 8 sandbox slots.
"""
import json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_lib import kc, conversation, C, NS, BUNDLE_REF
import load_run as R

def cleanup():
    kc("delete", "sandboxclaim", "-l", "load.curietech.ai=adr0116", "--ignore-not-found")
    kc("delete", "sandboxwarmpool", R.VK_POOL, "--ignore-not-found")
    kc("delete", "sandboxtemplate", R.VK_TEMPLATE, "--ignore-not-found")
    time.sleep(8)

def stats(recs, wall, label):
    oks = [r for r in recs if r["ok"]]
    cl = sorted(r["claim_s"] for r in recs)
    p = lambda q: cl[min(len(cl)-1, int(len(cl)*q))] if cl else 0
    print(f"  {C['b']}-> {len(oks)}/{len(recs)} ok in {wall:.1f}s | "
          f"claim p50 {p(.5):.2f}s p95 {p(.95):.2f}s max {cl[-1]:.2f}s{C['x']}", flush=True)
    return {"label": label, "n": len(recs), "ok": len(oks), "wall_s": wall,
            "p50": p(.5), "p95": p(.95), "max": cl[-1] if cl else 0,
            "claims": [round(r["claim_s"], 2) for r in recs]}

out = {}

# ---- R1: pool depth >= burst size. 4 warm + 4 bound = the whole 8-slot quota.
print(f"{C['b']}[R1] pool deep enough for the burst (4 warm, 4 conversations, 4 concurrent){C['x']}")
cleanup()
if R.make_vk_pool(4):
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=4) as ex:
        recs = [f.result() for f in [ex.submit(conversation, i, R.VK_POOL, None, 110)
                                     for i in range(101, 105)]]
    out["r1_deep_pool"] = stats(recs, time.monotonic()-t0, "pool depth >= burst")

# ---- R2: arrivals spaced wider than refill. Pool never empties.
print(f"\n{C['b']}[R2] arrivals spaced 9s apart, pool 3 (refill keeps up){C['x']}")
cleanup()
if R.make_vk_pool(3):
    t0 = time.monotonic(); recs = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = []
        for i in range(201, 209):
            futs.append(ex.submit(conversation, i, R.VK_POOL, None, 110))
            time.sleep(9)
        recs = [f.result() for f in futs]
    out["r2_steady_arrivals"] = stats(recs, time.monotonic()-t0, "arrivals below refill rate")

cleanup()
print(f"\n{C['b']}=== isolation result ==={C['x']}")
for k, v in out.items():
    print(f"  {v['label']:28} p50 {v['p50']:6.2f}s  p95 {v['p95']:6.2f}s  max {v['max']:6.2f}s  "
          f"({v['ok']}/{v['n']} ok)")
    print(f"    {C['d']}per-claim: {v['claims']}{C['x']}")
json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "isolate.json"), "w"), indent=1)
