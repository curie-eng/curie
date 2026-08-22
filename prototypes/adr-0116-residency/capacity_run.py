#!/usr/bin/env python3
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from capacity_lib import arm, kc, C

N        = int(os.environ.get("N", "14"))
HOLD     = int(os.environ.get("HOLD", "45"))
INTERVAL = float(os.environ.get("INTERVAL", "3"))

q = kc("get", "resourcequota", "curie-sandbox-quota", "-o", "jsonpath={.status.hard}")
print(f"{C['b']}=== does residency decide capacity? ==={C['x']}")
print(f"{C['d']}quota: {q}")
print(f"{N} one-shot conversations, one arriving every {INTERVAL}s.")
print(f"Arm A holds its sandbox {HOLD}s after the turn (routeTtlSeconds 3600, scaled).")
print(f"Arm B releases immediately (decision 5).{C['x']}")

a = arm("[A] shipped behaviour: hold the sandbox after the turn", N, HOLD, INTERVAL,
        f"each conversation keeps its slot for {HOLD}s; 8 slots exist")
b = arm("[B] decision 5: release when the turn ends", N, 0, INTERVAL,
        "each conversation frees its slot immediately")

print(f"\n{C['b']}=== result ==={C['x']}")
print(f"  {'':46}{'served':>9}{'quota-blocked':>15}{'per min':>10}")
for s in (a, b):
    print(f"  {s['label'][:46]:46}{s['ok']}/{s['n']:<6}{s['quota_blocked']:>13}{s['served_per_min']:>10.1f}")
if a["ok"] and b["ok"]:
    print(f"\n  {C['g']}served: {b['ok']}/{b['n']} vs {a['ok']}/{a['n']}"
          f"  |  throughput {b['served_per_min']/max(a['served_per_min'],1e-9):.1f}x{C['x']}")
if a["quota_blocked"] and not b["quota_blocked"]:
    print(f"  {C['g']}the quota wall appears only when the sandbox is held{C['x']}")
json.dump({"arm_a_hold": a, "arm_b_release": b},
          open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"), indent=1)
