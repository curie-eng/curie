#!/usr/bin/env python3
"""Is a released pool pod destroyed, or handed to the next conversation?

If a pod that served conversation A can be bound by conversation B, releasing
aggressively (decision 5) introduces a cross-conversation reuse that holding
never had. That would be a new leak created by this ADR, so it is checked rather
than assumed: bind, write a marker inside the pod, release, bind again, and look
for the marker.
"""
import json, os, subprocess, sys, time
NS = "curie"
CTX = os.environ.get("ISO_CONTEXT", "curie-iso")
assert CTX.startswith("curie-iso"), f"refusing context {CTX!r}"
REF = os.environ["BUNDLE_REF"]
G, R, B, D, X = "\033[32m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"

def kc(*a, inp=None, tries=3):
    cmd = ["kubectl", "--context", CTX, "-n", NS, "--request-timeout=20s"] + list(a)
    for i in range(tries):
        r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
        if r.returncode == 0:
            return r.stdout
        if "NotFound" in (r.stderr or ""):
            return ""
        time.sleep(2 ** i)
    return ""

VK_POOL, VK_TMPL = "iso-pool", "iso-runner"

def make_pool(n=2):
    t = json.loads(kc("get", "sandboxtemplate", "curie-runner", "-o", "json"))
    t["metadata"] = {"name": VK_TMPL, "namespace": NS, "labels": {"iso": "adr0116"}}
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
        "metadata": {"name": VK_POOL, "namespace": NS, "labels": {"iso": "adr0116"}},
        "spec": {"replicas": n, "updateStrategy": {"type": "OnReplenish"},
                 "sandboxTemplateRef": {"name": VK_TMPL}}}))
    t0 = time.time()
    while time.time() - t0 < 240:
        r = kc("get", "sandboxwarmpool", VK_POOL, "-o", "jsonpath={.status.readyReplicas}")
        if r.strip() and int(r) >= n:
            return True
        time.sleep(2)
    return False

def bind(name):
    kc("apply", "-f", "-", inp=json.dumps(
        {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxClaim",
         "metadata": {"name": name, "namespace": NS, "labels": {"iso": "adr0116"}},
         "spec": {"warmPoolRef": {"name": VK_POOL}}}))
    t0 = time.time()
    while time.time() - t0 < 90:
        sb = kc("get", "sandboxclaim", name, "-o", "jsonpath={.status.sandbox.name}").strip()
        if sb:
            uid = kc("get", "pod", sb, "-o", "jsonpath={.metadata.uid}").strip()
            rdy = kc("get", "pod", sb, "-o",
                     'jsonpath={.status.containerStatuses[?(@.name=="runner")].ready}').strip()
            if rdy == "true" and uid:
                return sb, uid
        time.sleep(0.3)
    return None, None

print(f"{B}=== is a released pool pod reused by the next conversation? ==={X}")
kc("delete", "sandboxclaim", "-l", "iso=adr0116", "--ignore-not-found")
kc("delete", "sandboxwarmpool", VK_POOL, "--ignore-not-found")
kc("delete", "sandboxtemplate", VK_TMPL, "--ignore-not-found"); time.sleep(8)
if not make_pool(2):
    print(f"{R}pool never warmed{X}"); sys.exit(1)

p1, u1 = bind("iso-conv-a")
print(f"  conversation A bound  pod={p1}  uid={u1[:8] if u1 else None}")
MARK = "curie-iso-marker-A"
kc("exec", p1, "-c", "runner", "--", "sh", "-c", f"echo {MARK} > /tmp/{MARK}")
seen = kc("exec", p1, "-c", "runner", "--", "sh", "-c", f"cat /tmp/{MARK} 2>/dev/null").strip()
print(f"  marker written inside A: {G if seen == MARK else R}{seen or 'FAILED'}{X}")

kc("delete", "sandboxclaim", "iso-conv-a", "--ignore-not-found")
print(f"  {D}A released{X}")
t0 = time.time(); gone = False
while time.time() - t0 < 120:
    if not kc("get", "pod", p1, "-o", "jsonpath={.metadata.uid}").strip():
        gone = True; break
    time.sleep(2)
print(f"  A's pod destroyed after release: {G if gone else R}{gone}{X} "
      f"({time.time()-t0:.0f}s)")

p2, u2 = bind("iso-conv-b")
print(f"  conversation B bound  pod={p2}  uid={u2[:8] if u2 else None}")
same_name = (p1 == p2); same_uid = (u1 == u2)
leak = kc("exec", p2, "-c", "runner", "--", "sh", "-c", f"cat /tmp/{MARK} 2>/dev/null").strip()
print(f"\n{B}=== result ==={X}")
print(f"  same pod name as A : {R+'YES' if same_name else G+'no'}{X}")
print(f"  same pod UID as A  : {R+'YES' if same_uid else G+'no'}{X}")
print(f"  A's marker visible : {R+repr(leak) if leak else G+'no'}{X}")
verdict = (not same_uid) and (not leak) and gone
print(f"\n  {(G+'PASS: a pod serves at most one conversation and is destroyed on release') if verdict else (R+'FAIL: reuse or leak detected')}{X}")
kc("delete", "sandboxclaim", "-l", "iso=adr0116", "--ignore-not-found")
kc("delete", "sandboxwarmpool", VK_POOL, "--ignore-not-found")
kc("delete", "sandboxtemplate", VK_TMPL, "--ignore-not-found")
json.dump({"pod_a": p1, "uid_a": u1, "pod_b": p2, "uid_b": u2,
           "a_destroyed_on_release": gone, "same_uid": same_uid,
           "marker_leaked": bool(leak), "pass": verdict},
          open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"), indent=1)
