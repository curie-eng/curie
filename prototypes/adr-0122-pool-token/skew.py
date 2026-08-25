#!/usr/bin/env python3
"""ADR-0122 unresolved #1 and #2, as generic Kubernetes questions.

#1 Generation skew: after the Secret behind a pod's env is rotated, does the
   running pod keep the old value, and does a replacement get the new one? If
   both are true, a mid-roll pool has two live token generations at once.

#2 Concurrent creation: when two actors race to create the same Secret, what
   does the loser see, and can it recover the winner's value deterministically?

Neither needs Curie. Both are settled by the API's own semantics, so this runs in
a scratch namespace with one busybox Deployment and costs almost nothing.
"""
import base64, hmac, json, os, subprocess, sys, time
NS = os.environ.get("SKEW_NS", "skew-spike")
CTX = os.environ["SKEW_CONTEXT"]
G, R, Y, B, D, X = "\033[32m", "\033[31m", "\033[33m", "\033[1m", "\033[2m", "\033[0m"
OUT = {}

def kc(*a, inp=None, ns=True, check=False):
    cmd = ["kubectl", "--context", CTX] + (["-n", NS] if ns else []) + ["--request-timeout=25s"] + list(a)
    r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
    if check and r.returncode != 0:
        return None, (r.stderr or "").strip()
    return r.stdout, (r.stderr or "").strip()

def secret_value(name, key="token"):
    out, _ = kc("get", "secret", name, "-o", f"jsonpath={{.data.{key}}}")
    return base64.b64decode(out).decode() if out.strip() else None

def pod_env(pod, key):
    out, _ = kc("exec", pod, "--", "sh", "-c", f"printf %s \"${key}\"")
    return out.strip()

# Every check below is an identity comparison, so the spike never needs a token's
# value -- only which planted generation an observation matches. `which` returns a
# name out of this table, so no secret material ever flows into stdout or the
# results file.
_PLANTED = [
    ("actor A's value", "WORKER-ONE-TOKEN"),
    ("actor B's value", "WORKER-TWO-TOKEN"),
    ("the rotated value", "ROTATED-GEN-TWO"),
]

def which(value):
    if value is None:
        return "absent"
    probe = value.encode("utf-8")
    for name, known in _PLANTED:
        if hmac.compare_digest(known.encode("utf-8"), probe):
            return name
    return "an unplanted value"

print(f"{B}=== ADR-0122 leftovers, as Kubernetes questions ==={X}")
kc("create", "namespace", NS, ns=False)
kc("delete", "secret", "pool-token", "--ignore-not-found")
kc("delete", "deploy", "gen-a", "--ignore-not-found")
time.sleep(3)

# ---------- #2 concurrent creation -------------------------------------------
print(f"\n{B}[#2] two actors race to create the same Secret{X}")
body = lambda v: json.dumps({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "pool-token"},
                             "stringData": {"token": v}})
r1 = subprocess.Popen(["kubectl", "--context", CTX, "-n", NS, "create", "-f", "-"],
                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
r2 = subprocess.Popen(["kubectl", "--context", CTX, "-n", NS, "create", "-f", "-"],
                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
o1, e1 = r1.communicate(body("WORKER-ONE-TOKEN"))
o2, e2 = r2.communicate(body("WORKER-TWO-TOKEN"))
winners = [("A", r1.returncode, e1.strip()), ("B", r2.returncode, e2.strip())]
for who, rc, err in winners:
    print(f"    actor {who}: rc={rc}  {(G+'created'+X) if rc==0 else (Y+err[:78]+X)}")
won = [w for w, rc, _ in winners if rc == 0]
lost = [(w, e) for w, rc, e in winners if rc != 0]
canon = secret_value("pool-token")
already = all("AlreadyExists" in e or "already exists" in e for _, e in lost)
print(f"    the surviving value is {G}{which(canon)}{X}")
print(f"    {(G+'PASS'+X) if len(won)==1 else (R+'FAIL'+X)}  exactly one create succeeded ({len(won)})")
print(f"    {(G+'PASS'+X) if already else (R+'FAIL'+X)}  the loser got AlreadyExists, so it can adopt rather than guess")
OUT["concurrent"] = {"winners": len(won), "loser_already_exists": already,
                     "canonical": which(canon)}

# ---------- #1 generation skew ------------------------------------------------
print(f"\n{B}[#1] does a running pod keep its old value when the Secret rotates?{X}")
dep = {"apiVersion": "apps/v1", "kind": "Deployment",
       "metadata": {"name": "gen-a"},
       "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "gen-a"}},
                "template": {"metadata": {"labels": {"app": "gen-a"}},
                             "spec": {"containers": [{
                                 "name": "c", "image": "busybox:1.36",
                                 "command": ["sh", "-c", "sleep 3600"],
                                 "env": [{"name": "TOKEN", "valueFrom": {
                                     "secretKeyRef": {"name": "pool-token", "key": "token"}}}],
                                 "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                                               "limits": {"cpu": "50m", "memory": "32Mi"}}}]}}}}
kc("apply", "-f", "-", inp=json.dumps(dep))
t0 = time.time(); pod = None
while time.time() - t0 < 180:
    out, _ = kc("get", "pods", "-l", "app=gen-a", "-o",
                "jsonpath={range .items[*]}{.metadata.name}:{.status.phase} {end}")
    for tok in out.split():
        n, _, ph = tok.partition(":")
        if ph == "Running":
            pod = n; break
    if pod: break
    time.sleep(2)
if not pod:
    print(f"    {R}pod never ran{X}"); sys.exit(1)
before = pod_env(pod, "TOKEN")
print(f"    pod {pod} booted with {G}{which(before)}{X}")

kc("patch", "secret", "pool-token", "--type=merge", "-p",
   json.dumps({"stringData": {"token": "ROTATED-GEN-TWO"}}))
print(f"    {D}Secret rotated to ROTATED-GEN-TWO{X}")
time.sleep(20)
after = pod_env(pod, "TOKEN")
stale = (after == before)
print(f"    {(G+'PASS'+X) if stale else (R+'FAIL'+X)}  running pod still sees {Y}{which(after)}{X} "
      f"{'(env is read once at start)' if stale else '(unexpectedly updated)'}")

kc("delete", "pod", pod, "--ignore-not-found")
t0 = time.time(); newpod = None
while time.time() - t0 < 180:
    out, _ = kc("get", "pods", "-l", "app=gen-a", "-o",
                "jsonpath={range .items[*]}{.metadata.name}:{.status.phase} {end}")
    for tok in out.split():
        n, _, ph = tok.partition(":")
        if ph == "Running" and n != pod:
            newpod = n; break
    if newpod: break
    time.sleep(2)
newval = pod_env(newpod, "TOKEN") if newpod else None
fresh = (newval == "ROTATED-GEN-TWO")
print(f"    {(G+'PASS'+X) if fresh else (R+'FAIL'+X)}  replacement pod {newpod} sees {G}{which(newval)}{X}")
OUT["skew"] = {"before": which(before), "after_rotate_same_pod": which(after),
               "replacement": which(newval),
               "running_pod_keeps_old": stale, "replacement_gets_new": fresh}

print(f"\n{B}=== what this means for a pool roll ==={X}")
if stale and fresh:
    print(f"  {Y}Mid-roll a pool genuinely holds two live token generations:{X}")
    print(f"    old pods still accept {Y}{which(before)}{X}, "
          f"new pods accept {G}{which(newval)}{X}")
    print(f"  {D}So a binder that reads only the Secret can present the wrong one to an old pod.{X}")
kc("delete", "deploy", "gen-a", "--ignore-not-found")
kc("delete", "secret", "pool-token", "--ignore-not-found")
kc("delete", "namespace", NS, ns=False)
json.dump(OUT, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json"), "w"), indent=1)
print(f"\n{D}scratch namespace deleted; results.json written{X}")
