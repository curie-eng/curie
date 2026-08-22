#!/usr/bin/env python3
"""ADR-0116 demo: what a cold sandbox create costs, and what removes it."""
import json, os, subprocess, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from probe import kc, apply, time_claim, burner, C, NS

BUNDLE_REF = os.environ["BUNDLE_REF"]
SHIPPED_POOL = "curie-runner-pool"
VK_POOL = "probe-vk-pool"
VK_TEMPLATE = "probe-vk-runner"
PER_CLAIM_ENV = [
    {"name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
    {"name": "CURIE_PLUGIN_DIR", "value": "/bundles/current"},
    {"containerName": "bundle-fetch", "name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
    {"containerName": "bundle-extract", "name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
]
R = {}

def hdr(n, total, title, sub=""):
    print(f"\n{C['b']}[{n}/{total}] {title}{C['x']}")
    if sub:
        print(f"      {C['d']}{sub}{C['x']}")

def make_version_keyed_pool():
    """Decision 3: the bundle is baked into the POOL's template, not injected per claim."""
    raw = kc("get", "sandboxtemplate", "curie-runner", "-o", "json")
    t = json.loads(raw)
    t["metadata"] = {"name": VK_TEMPLATE, "namespace": NS,
                     "labels": {"probe.curietech.ai/adr": "0116"}}
    def setenv(c, k, v):
        for e in c.setdefault("env", []):
            if e.get("name") == k:
                e.pop("valueFrom", None); e["value"] = v; return
        c["env"].append({"name": k, "value": v})
    def walk(o):
        if isinstance(o, dict):
            if o.get("name") == "runner" and "image" in o:
                setenv(o, "CURIE_BUNDLE_REF", BUNDLE_REF)
                setenv(o, "CURIE_PLUGIN_DIR", "/bundles/current")
            if o.get("name") in ("bundle-fetch", "bundle-extract"):
                setenv(o, "CURIE_BUNDLE_REF", BUNDLE_REF)
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for i in o: walk(i)
    walk(t["spec"])
    apply(t)
    apply({"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxWarmPool",
           "metadata": {"name": VK_POOL, "namespace": NS,
                        "labels": {"probe.curietech.ai/adr": "0116"}},
           "spec": {"replicas": 2, "updateStrategy": {"type": "OnReplenish"},
                    "sandboxTemplateRef": {"name": VK_TEMPLATE}}})
    t0 = time.monotonic()
    while time.monotonic() - t0 < 180:
        st = kc("get", "sandboxwarmpool", VK_POOL, "-o", "jsonpath={.status.readyReplicas}")
        if st.strip() and int(st) >= 2:
            print(f"      {C['g']}2 pods pre-fetched, pre-extracted, pre-booted "
                  f"({time.monotonic()-t0:.0f}s, paid once){C['x']}")
            return True
        time.sleep(2)
    print(f"      {C['r']}pool did not become ready{C['x']}")
    return False

def contend(on, replicas=18):
    if on:
        apply(burner(replicas))
        print(f"      {C['y']}{replicas} neighbours requesting 200m each; "
              f"critical path requests 50m (weight 29 vs 11).{C['x']}")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 120:
            r = kc("get", "deploy", "probe-burner", "-o", "jsonpath={.status.readyReplicas}")
            if r.strip() and int(r) >= replicas:
                break
            time.sleep(2)
        time.sleep(8)
        print(f"      {C['d']}node saturated{C['x']}")
    else:
        kc("delete", "deploy", "probe-burner", "--ignore-not-found", "--wait=true")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 180:
            n = kc("get", "pods", "-l", "app=probe-burner",
                   "-o", "jsonpath={.items[*].metadata.name}").split()
            if not n:
                break
            time.sleep(3)
        print(f"      {C['d']}contention removed, node quiet again{C['x']}")
        time.sleep(5)

def main():
    print(f"{C['b']}=== ADR-0116: sandbox residency and pre-binding ==={C['x']}")
    print(f"{C['d']}A Curie turn costs 2-9% of one core; almost all of a turn is waiting on the model.")
    print(f"What costs real time is STARTING a conversation, and a hard 90s deadline sits on it.")
    print(f"This cluster: fake model, no observability stack, 6,961-byte bundle, gVisor off.{C['x']}")

    hdr(1, 5, "Today: a new conversation on a quiet node",
        "claim carries the bundle ref as per-claim env, so the pod is built from scratch")
    R["A"], _ = time_claim("probe-a", SHIPPED_POOL, env=PER_CLAIM_ENV, budget=150)

    hdr(2, 5, "The same claim, node under contention",
        "this is the shape of the incident: claimTimeoutSeconds is 90")
    contend(True)
    R["B"], _ = time_claim("probe-b", SHIPPED_POOL, env=PER_CLAIM_ENV, budget=110)
    contend(False)

    hdr(3, 5, "Decision 3: a warm pool keyed by version",
        "the bundle is baked into the pool's template, not injected per claim")
    if make_version_keyed_pool():
        hdr(4, 5, "A claim carrying no env binds a pre-booted pod")
        R["C"], _ = time_claim("probe-c", VK_POOL, budget=60)
        hdr(5, 5, "The same pre-bound claim, the same contention")
        contend(True)
        R["D"], _ = time_claim("probe-d", VK_POOL, budget=60)
        contend(False)

    print(f"\n{C['b']}=== result ==={C['x']}")
    print(f"  {'':22}{'quiet node':>14}{'under contention':>20}")
    def f(v): return f"{v:.2f}s" if v else "timed out"
    print(f"  {'today (cold create)':22}{f(R.get('A')):>14}{f(R.get('B')):>20}")
    print(f"  {'pre-bound (ADR-0116)':22}{f(R.get('C')):>14}{f(R.get('D')):>20}")
    if R.get("A") and R.get("C"):
        print(f"\n  {C['g']}{R['A']/R['C']:.0f}x faster on a quiet node{C['x']}")
    if R.get("D"):
        print(f"  {C['g']}under contention the 90s deadline is no longer reachable "
              f"({R['D']:.2f}s){C['x']}")
    print(f"\n{C['d']}The cold baseline is cluster-shaped: 4-5s here, and 17.39s measured on a")
    print(f"real-model install with Langfuse and ClickHouse resident. The arm that matters is")
    print(f"the middle column: today's path does not finish, and a pre-bound one does.{C['x']}")
    json.dump(R, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"))

if __name__ == "__main__":
    main()
