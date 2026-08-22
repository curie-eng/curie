#!/usr/bin/env python3
"""ADR-0116 demo: what a conversation waits for, and what a slot is spent on."""
import json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probe as P
from probe import kc, time_claim, burner, C
import capacity as CAP

REF = os.environ["BUNDLE_REF"]
SHIPPED = "curie-runner-pool"
VK_POOL, VK_TMPL = "demo-vk-pool", "demo-vk-runner"
ENV = [{"name": "CURIE_BUNDLE_REF", "value": REF},
       {"name": "CURIE_PLUGIN_DIR", "value": "/bundles/current"},
       {"containerName": "bundle-fetch", "name": "CURIE_BUNDLE_REF", "value": REF},
       {"containerName": "bundle-extract", "name": "CURIE_BUNDLE_REF", "value": REF}]
R = {}

def hdr(n, title, sub=""):
    print(f"\n{C['b']}[{n}/5] {title}{C['x']}")
    if sub: print(f"      {C['d']}{sub}{C['x']}", flush=True)

def vk_pool(n=3):
    t = json.loads(kc("get", "sandboxtemplate", "curie-runner", "-o", "json"))
    t["metadata"] = {"name": VK_TMPL, "namespace": "curie", "labels": {"demo": "adr0116"}}
    def setenv(c, k, v):
        for e in c.setdefault("env", []):
            if e.get("name") == k:
                e.clear(); e["name"] = k; e["value"] = v; return
        c["env"].append({"name": k, "value": v})
    def walk(o):
        if isinstance(o, dict):
            if o.get("name") == "runner" and "image" in o:
                setenv(o, "CURIE_BUNDLE_REF", REF); setenv(o, "CURIE_PLUGIN_DIR", "/bundles/current")
            if o.get("name") in ("bundle-fetch", "bundle-extract"):
                setenv(o, "CURIE_BUNDLE_REF", REF)
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for i in o: walk(i)
    walk(t["spec"]); kc("apply", "-f", "-", inp=json.dumps(t))
    kc("apply", "-f", "-", inp=json.dumps({
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxWarmPool",
        "metadata": {"name": VK_POOL, "namespace": "curie", "labels": {"demo": "adr0116"}},
        "spec": {"replicas": n, "updateStrategy": {"type": "OnReplenish"},
                 "sandboxTemplateRef": {"name": VK_TMPL}}}))
    t0 = time.time()
    while time.time() - t0 < 240:
        r = kc("get", "sandboxwarmpool", VK_POOL, "-o", "jsonpath={.status.readyReplicas}")
        if r.strip() and int(r) >= n:
            print(f"      {C['g']}{n} pods pre-fetched, pre-extracted, pre-booted in "
                  f"{time.time()-t0:.0f}s -- paid once per version{C['x']}", flush=True); return True
        time.sleep(2)
    return False

def contend(on, n=18):
    if on:
        kc("apply", "-f", "-", inp=json.dumps(burner(n)))
        print(f"      {C['y']}{n} neighbours requesting 200m each; the critical path "
              f"requests 50m (cpu.weight 29 vs 11){C['x']}", flush=True)
        t0 = time.time()
        while time.time() - t0 < 150:
            r = kc("get", "deploy", "probe-burner", "-o", "jsonpath={.status.readyReplicas}")
            if r.strip() and int(r) >= n: break
            time.sleep(2)
        time.sleep(8); print(f"      {C['d']}node saturated{C['x']}", flush=True)
    else:
        kc("delete", "deploy", "probe-burner", "--ignore-not-found", "--wait=true")
        t0 = time.time()
        while time.time() - t0 < 200:
            if not kc("get", "pods", "-l", "app=probe-burner",
                      "-o", "jsonpath={.items[*].metadata.name}").split(): break
            time.sleep(3)
        print(f"      {C['d']}contention removed{C['x']}", flush=True); time.sleep(5)

def cleanup():
    kc("delete", "sandboxclaim", "-l", "demo=adr0116", "--ignore-not-found")
    kc("delete", "sandboxclaim", "-l", "cap=adr0116", "--ignore-not-found")
    kc("delete", "sandboxwarmpool", VK_POOL, "--ignore-not-found")
    kc("delete", "sandboxtemplate", VK_TMPL, "--ignore-not-found")
    kc("delete", "deploy", "probe-burner", "--ignore-not-found")
    time.sleep(10)

print(f"{C['b']}=== ADR-0116: sandbox residency and pre-binding ==={C['x']}")
print(f"{C['d']}A turn costs 2-9% of one core; almost all of it is waiting on the model.")
print(f"What costs time is STARTING a conversation, and what costs capacity is")
print(f"holding a sandbox after one ENDS. This measures both.")
print(f"Cluster: fake model, no observability stack, 8-slot sandbox quota.{C['x']}")

cleanup()
hdr(1, "Today: a new conversation on a quiet node",
    "the claim carries the bundle ref as per-claim env, so the pod is built from scratch")
R["cold"], _ = time_claim("demo-a", SHIPPED, env=ENV, budget=150)

hdr(2, "The same claim, node under contention", "claimTimeoutSeconds is 90")
contend(True)
R["cold_load"], _ = time_claim("demo-b", SHIPPED, env=ENV, budget=110)
contend(False)

hdr(3, "Decision 3: a warm pool keyed by version",
    "the bundle is baked into the pool's template, not injected per claim")
ok = vk_pool(3)

if ok:
    hdr(4, "A claim carrying no env binds a pre-booted pod")
    R["bind"], _ = time_claim("demo-c", VK_POOL, budget=60)
    print(f"      {C['d']}and under the same contention:{C['x']}", flush=True)
    contend(True)
    R["bind_load"], _ = time_claim("demo-d", VK_POOL, budget=60)
    contend(False)
cleanup()

hdr(5, "Decision 5: what a slot is actually spent on",
    "14 one-shot conversations, one every 3s, into an 8-slot quota")
for label, hold, key in (("holding the sandbox 45s after the turn", 45, "hold"),
                         ("releasing it when the turn ends", 0, "release")):
    print(f"\n      {C['b']}{label}{C['x']}", flush=True)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=14) as ex:
        futs = []
        for i in range(1, 15):
            futs.append(ex.submit(CAP.conversation, i, hold)); time.sleep(3)
        recs = [f.result() for f in futs]
    wall = time.time() - t0
    q = sum(1 for r in recs if r["quota_blocked"])
    served = sum(1 for r in recs if r["ok"])
    R[key] = {"served": served, "quota": q, "per_min": served / wall * 60, "wall": wall}
    col = C['r'] if q else C['g']
    print(f"      -> {served}/14 served, {col}{q} blocked by quota{C['x']}, "
          f"{served/wall*60:.1f}/min", flush=True)
cleanup()

f = lambda v: f"{v:.2f}s" if isinstance(v, float) else ("timed out" if v is None else str(v))
print(f"\n{C['b']}=== result ==={C['x']}")
print(f"  {'starting a conversation':30}{'quiet':>12}{'under contention':>19}")
print(f"  {'  today (cold create)':30}{f(R.get('cold')):>12}{f(R.get('cold_load')):>19}")
print(f"  {'  pre-bound (ADR-0116)':30}{f(R.get('bind')):>12}{f(R.get('bind_load')):>19}")
if R.get("cold") and R.get("bind"):
    print(f"      {C['g']}{R['cold']/R['bind']:.0f}x faster, and the 90s deadline "
          f"stops being reachable{C['x']}")
h, rl = R.get("hold"), R.get("release")
if h and rl:
    print(f"\n  {'spending a slot':30}{'served':>12}{'quota-blocked':>19}{'per min':>10}")
    print(f"  {'  hold 45s after the turn':30}{h['served']}/14{'':>7}"
          f"{C['r']}{h['quota']}{C['x']}{'':>17}{h['per_min']:>7.1f}")
    print(f"  {'  release at end of turn':30}{rl['served']}/14{'':>7}"
          f"{C['g']}{rl['quota']}{C['x']}{'':>17}{rl['per_min']:>7.1f}")
    print(f"      {C['g']}the quota wall appears only when the sandbox is held{C['x']}")
    print(f"      {C['d']}45s is routeTtlSeconds 3600 scaled by 80; the shipped ratio is far wider{C['x']}")
json.dump(R, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"), indent=1)
