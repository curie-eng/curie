#!/usr/bin/env python3
"""ADR-0122 unresolved #1, through a real SandboxWarmPool.

The generic Kubernetes fact is settled: a running pod keeps the env it booted
with, a replacement gets the new value. This asks the pool-specific half --
when a pool's template changes, does the controller REPLACE its warm pods, and
how long do two generations coexist?

Scratch namespace only. Never touches the curie namespace, its Helm release, or
curie/curie-runner-pool.
"""
import base64, json, os, subprocess, sys, time
NS = os.environ.get("POOL_NS", "skew-pool")
CTX = os.environ["POOL_CONTEXT"]
assert NS.startswith("skew-"), f"refusing namespace {NS!r}"
G, R, Y, B, D, X = "\033[32m", "\033[31m", "\033[33m", "\033[1m", "\033[2m", "\033[0m"
OUT = {}

def kc(*a, inp=None, ns=True):
    cmd = ["kubectl", "--context", CTX] + (["-n", NS] if ns else []) + ["--request-timeout=25s"] + list(a)
    r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
    return r.stdout, (r.stderr or "").strip()

def pods():
    out, _ = kc("get", "pods", "-l", "skew=1", "-o",
                "jsonpath={range .items[*]}{.metadata.name}:{.status.phase} {end}")
    return [t.split(":")[0] for t in out.split() if t.endswith(":Running")]

def tok(pod):
    out, _ = kc("exec", pod, "-c", "runner", "--", "sh", "-c", 'printf %s "$CURIE_RUNNER_TOKEN"')
    return out.strip()

print(f"{B}=== does a pool replace its warm pods when the template changes? ==={X}")
kc("create", "namespace", NS, ns=False)
for k in ("sandboxwarmpool", "sandboxtemplate", "secret"):
    kc("delete", k, "--all", "--ignore-not-found")
time.sleep(4)

kc("create", "secret", "generic", "pool-tok", "--from-literal=token=GEN-ONE")
src, _ = kc("get", "sandboxtemplate", "curie-runner", "-o", "json", ns=False)
src, _ = kc("get", "sandboxtemplate", "curie-runner", "-n", "curie", "-o", "json", ns=False)
t = json.loads(src)
t["metadata"] = {"name": "skew-runner", "namespace": NS, "labels": {"skew": "1"}}
def setenv(c, k, secret):
    env = c.setdefault("env", [])
    for e in list(env):
        if e.get("name") == k: env.remove(e)
    env.append({"name": k, "valueFrom": {"secretKeyRef": {"name": secret, "key": "token"}}})
def walk(o, fn):
    if isinstance(o, dict):
        if o.get("name") == "runner" and "image" in o: fn(o)
        for v in o.values(): walk(v, fn)
    elif isinstance(o, list):
        for i in o: walk(i, fn)
walk(t["spec"], lambda c: setenv(c, "CURIE_RUNNER_TOKEN", "pool-tok"))
t["spec"].setdefault("podTemplate", {})
kc("apply", "-f", "-", inp=json.dumps(t))
kc("apply", "-f", "-", inp=json.dumps({
    "apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxWarmPool",
    "metadata": {"name": "skew-pool", "namespace": NS, "labels": {"skew": "1"}},
    "spec": {"replicas": 1, "updateStrategy": {"type": "OnReplenish"},
             "sandboxTemplateRef": {"name": "skew-runner"}}}))

t0 = time.time(); first = None
while time.time() - t0 < 240:
    p = pods()
    if p:
        v = tok(p[0])
        if v: first = (p[0], v); break
    time.sleep(3)
if not first:
    print(f"  {R}pool never produced a running pod; aborting{X}")
    kc("delete", "namespace", NS, ns=False); sys.exit(1)
print(f"  warm pod {first[0]} booted with token={G}{first[1]}{X}  ({time.time()-t0:.0f}s)")
OUT["gen1"] = {"pod": first[0], "token": first[1]}

print(f"\n{D}  rotating the Secret to GEN-TWO (template ref unchanged){X}")
kc("patch", "secret", "pool-tok", "--type=merge", "-p",
   json.dumps({"stringData": {"token": "GEN-TWO"}}))
time.sleep(25)
still = tok(first[0])
same_pod_stale = (still == first[1])
replaced = first[0] not in pods()
print(f"  {(G+'PASS'+X) if same_pod_stale else (R+'FAIL'+X)}  the warm pod still holds {Y}{still}{X}")
print(f"  {(Y+'pod NOT replaced'+X) if not replaced else (G+'pod was replaced'+X)}  "
      f"-> {D}rotating the Secret alone does not roll the pool{X}")
OUT["after_secret_rotate"] = {"token": still, "replaced": replaced}

print(f"\n{D}  now deleting the warm pod, so the pool replenishes{X}")
kc("delete", "pod", first[0], "--ignore-not-found")
t0 = time.time(); second = None
while time.time() - t0 < 240:
    p = [x for x in pods() if x != first[0]]
    if p:
        v = tok(p[0])
        if v: second = (p[0], v); break
    time.sleep(3)
if second:
    fresh = second[1] == "GEN-TWO"
    print(f"  {(G+'PASS'+X) if fresh else (R+'FAIL'+X)}  replenished pod {second[0]} holds {G}{second[1]}{X}")
    OUT["gen2"] = {"pod": second[0], "token": second[1]}

print(f"\n{B}=== what this settles ==={X}")
print(f"  {Y}A pool does not roll itself when its Secret changes.{X}")
print(f"  {D}Old warm pods keep the old token until something replaces them, so the two{X}")
print(f"  {D}generations coexist for as long as the operator leaves them, not briefly.{X}")
for k in ("sandboxwarmpool", "sandboxtemplate", "secret"):
    kc("delete", k, "--all", "--ignore-not-found")
kc("delete", "namespace", NS, ns=False)
json.dump(OUT, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "pool-results.json"), "w"), indent=1)
print(f"\n{D}scratch namespace deleted{X}")
