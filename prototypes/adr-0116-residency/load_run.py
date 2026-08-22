#!/usr/bin/env python3
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_lib import kc, run_arm, PER_CLAIM_ENV, C, NS, BUNDLE_REF

N     = int(os.environ.get("N", "24"))
CONC  = int(os.environ.get("CONC", "5"))
POOLR = int(os.environ.get("POOLR", "3"))
SHIPPED_POOL = "curie-runner-pool"
VK_POOL, VK_TEMPLATE = "load-vk-pool", "load-vk-runner"

def quota():
    o = kc("get", "resourcequota", "curie-sandbox-quota", "-o", "json")
    if not o: return {}
    d = json.loads(o).get("status", {})
    return {"hard": d.get("hard", {}), "used": d.get("used", {})}

def make_vk_pool(replicas):
    raw = kc("get", "sandboxtemplate", "curie-runner", "-o", "json")
    t = json.loads(raw)
    t["metadata"] = {"name": VK_TEMPLATE, "namespace": NS, "labels": {"load.curietech.ai": "adr0116"}}
    def setenv(c, k, v):
        for e in c.setdefault("env", []):
            if e.get("name") == k:
                e.pop("valueFrom", None); e["value"] = v; return
        c["env"].append({"name": k, "value": v})
    def walk(o):
        if isinstance(o, dict):
            if o.get("name") == "runner" and "image" in o:
                setenv(o, "CURIE_BUNDLE_REF", BUNDLE_REF); setenv(o, "CURIE_PLUGIN_DIR", "/bundles/current")
            if o.get("name") in ("bundle-fetch", "bundle-extract"):
                setenv(o, "CURIE_BUNDLE_REF", BUNDLE_REF)
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for i in o: walk(i)
    walk(t["spec"])
    kc("apply", "-f", "-", inp=json.dumps(t))
    kc("apply", "-f", "-", inp=json.dumps({
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxWarmPool",
        "metadata": {"name": VK_POOL, "namespace": NS, "labels": {"load.curietech.ai": "adr0116"}},
        "spec": {"replicas": replicas, "updateStrategy": {"type": "OnReplenish"},
                 "sandboxTemplateRef": {"name": VK_TEMPLATE}}}))
    t0 = time.monotonic()
    while time.monotonic() - t0 < 300:
        s = kc("get", "sandboxwarmpool", VK_POOL, "-o", "jsonpath={.status.readyReplicas}")
        if s.strip() and int(s) >= replicas:
            print(f"  {C['g']}pool warm: {replicas} pods pre-booted in {time.monotonic()-t0:.0f}s "
                  f"(paid once per version){C['x']}", flush=True)
            return True
        time.sleep(2)
    print(f"  {C['r']}pool never became ready{C['x']}"); return False

def cleanup():
    kc("delete", "sandboxclaim", "-l", "load.curietech.ai=adr0116", "--ignore-not-found")
    kc("delete", "sandboxwarmpool", VK_POOL, "--ignore-not-found")
    kc("delete", "sandboxtemplate", VK_TEMPLATE, "--ignore-not-found")
    time.sleep(8)

q = quota()
print(f"{C['b']}=== ADR-0116 load test: one-shot conversation throughput ==={C['x']}")
print(f"{C['d']}Same budget for both arms. Namespace sandbox quota:")
print(f"  hard {q.get('hard')}")
print(f"Both arms: {N} one-shot conversations, {CONC} concurrent.")
print(f"Arm B spends {POOLR} of the same slots on a warm pool, so it is handicapped.")
print(f"A conversation counts only if a real ACI turn came back with a terminal frame.{C['x']}")

cleanup()
a = run_arm("[A] today: cold create per conversation (per-claim env)",
            N, CONC, SHIPPED_POOL, PER_CLAIM_ENV)
cleanup()

print(f"\n{C['b']}[B] ADR-0116: version-keyed warm pool, env-free claims{C['x']}")
b = None
if make_vk_pool(POOLR):
    b = run_arm("[B] pre-bound: bind from a version-keyed pool (no env)",
                N, CONC, VK_POOL, None)
cleanup()

print(f"\n{C['b']}=== result ==={C['x']}")
hdr = f"  {'':34}{'ok':>7}{'wall':>9}{'conv/min':>10}{'claim p50':>11}{'claim p95':>11}"
print(hdr)
for s in [x for x in (a, b) if x]:
    print(f"  {s['label'][:34]:34}{s['ok']}/{s['n']:<5}{s['wall_s']:8.1f}s"
          f"{s['throughput_per_min']:10.1f}{s['claim_p50']:10.2f}s{s['claim_p95']:10.2f}s")
if a and b and a["throughput_per_min"] > 0:
    print(f"\n  {C['g']}throughput: {b['throughput_per_min']/a['throughput_per_min']:.1f}x"
          f"  |  claim p50: {a['claim_p50']/max(b['claim_p50'],1e-9):.0f}x faster{C['x']}")
json.dump({"arm_a": a, "arm_b": b, "quota": q},
          open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"), indent=1)
print(f"\n{C['d']}written: results.json{C['x']}")
