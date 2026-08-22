#!/usr/bin/env python3
"""ADR-0116 measurement harness. Times the claim -> ready path for each arm."""
import json, os, subprocess, sys, time

NS = "curie"
C = {"g": "\033[32m", "r": "\033[31m", "y": "\033[33m", "b": "\033[1m", "d": "\033[2m", "x": "\033[0m"}

# HARD SAFETY: this kubeconfig also holds production EKS contexts. Every call is
# pinned to an explicit context so a stray kubectl can never reach one of them.
CONTEXT = os.environ.get("DEMO_CONTEXT", "curie-demo")
assert CONTEXT.startswith("curie-demo"), f"refusing to run against context {CONTEXT!r}"

def kc(*a, inp=None, ns=True, tries=6):
    """Run kubectl, retrying transient failures.

    Under the contention arm the burners starve the single node's API server too,
    so a bare call returns empty stdout with the real error on stderr. Swallowing
    that turned an API stall into a JSONDecodeError further down. Retry instead,
    and surface the error when it is not transient."""
    cmd = ["kubectl", "--context", CONTEXT, "--request-timeout=20s"] + (
        ["-n", NS] if ns else []) + list(a)
    last = ""
    for i in range(tries):
        r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
        if r.returncode == 0:
            return r.stdout
        last = (r.stderr or "").strip()
        if "NotFound" in last or "not found" in last:
            return ""
        time.sleep(min(2 ** i, 8))
    print(f"    {C['r']}kubectl failed after {tries} tries: {last[:160]}{C['x']}", flush=True)
    return ""

def apply(obj):
    return kc("apply", "-f", "-", inp=json.dumps(obj))

def claim(name, pool, env=None):
    o = {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxClaim",
         "metadata": {"name": name, "namespace": NS,
                      "labels": {"probe.curietech.ai/adr": "0116"}},
         "spec": {"warmPoolRef": {"name": pool}}}
    if env:
        o["spec"]["env"] = env
    return o

def time_claim(name, pool, env=None, budget=120.0, label=""):
    """Apply a claim and time it to a ready runner. Returns (seconds|None, phases)."""
    kc("delete", "sandboxclaim", name, "--ignore-not-found")
    time.sleep(1.0)
    phases = []
    t0 = time.monotonic()
    apply(claim(name, pool, env))
    def el(): return time.monotonic() - t0
    def mark(s):
        phases.append((el(), s))
        print(f"    {C['d']}{el():6.2f}s{C['x']}  {s}", flush=True)
    mark("claim applied" + (" (with per-claim env)" if env else " (no env)"))
    sandbox = None; seen = set()
    while el() < budget:
        out = kc("get", "sandboxclaim", name, "-o", "json")
        if out:
            st = (json.loads(out).get("status") or {})
            sb = (st.get("sandbox") or {}).get("name")
            if sb and not sandbox:
                sandbox = sb
                mark(f"bound to sandbox {sb}")
            for c in st.get("conditions") or []:
                k = f"{c.get('type')}={c.get('status')}"
                if k not in seen:
                    seen.add(k)
                    r = c.get("reason") or ""
                    mark(f"claim {k} ({r})")
                    if r == "ReconcilerError":
                        mark(f"{C['r']}{(c.get('message') or '')[:120]}{C['x']}")
        if el() > 90 and "deadline" not in seen:
            seen.add("deadline")
            mark(f"{C['r']}claimTimeoutSeconds (90s) EXCEEDED -- in production the worker "
                 f"gives up here; three of these escalate as runner-error{C['x']}")
        if sandbox:
            out = kc("get", "pod", sandbox, "-o", "json")
            if out:
                s = json.loads(out).get("status", {})
                for cs in s.get("containerStatuses") or []:
                    if cs["name"] == "runner" and cs.get("ready"):
                        mark(f"{C['g']}RUNNER READY{C['x']}")
                        return el(), phases
        time.sleep(0.1)
    mark(f"{C['r']}gave up after {budget:.0f}s{C['x']}")
    return None, phases

def burner(replicas, cpu_request="200m"):
    """A neighbour that requests more CPU than the critical path and burns all it can.
    Reproduces the incident's share ratio (200m vs the runner's 50m) under contention."""
    return {"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "probe-burner", "namespace": NS,
                         "labels": {"probe.curietech.ai/adr": "0116"}},
            "spec": {"replicas": replicas,
                     "selector": {"matchLabels": {"app": "probe-burner"}},
                     "template": {"metadata": {"labels": {"app": "probe-burner"}},
                                  "spec": {"containers": [{
                                      "name": "burn", "image": "busybox:1.36",
                                      "command": ["/bin/sh", "-c",
                                                  "while :; do :; done"],
                                      "resources": {"requests": {"cpu": cpu_request, "memory": "16Mi"}}}]}}}}

if __name__ == "__main__":
    print("harness ok")
