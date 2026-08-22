#!/usr/bin/env python3
"""Does residency, not the claim path, decide capacity on the shipped defaults?

The shipped default is routeTtlSeconds 3600 against a quota of 8 sandbox slots,
so a one-shot conversation holds a slot for the hour after its last message.
This scales that down: HOLD seconds instead of 3600, same 8 slots, arrivals every
INTERVAL seconds. The prediction is that slots fill and later arrivals cannot get
a sandbox at all -- the "idle accumulation" failure, not a burst failure.

A conversation counts only when a real ACI turn returned a terminal frame.
"""
import json, os, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

NS = "curie"
CTX = os.environ.get("CAP_CONTEXT", "curie-cap")
assert CTX.startswith("curie-cap"), f"refusing context {CTX!r}"
REF = os.environ["BUNDLE_REF"]
C = {"g": "\033[32m", "r": "\033[31m", "y": "\033[33m", "b": "\033[1m", "d": "\033[2m", "x": "\033[0m"}
_pr = threading.Lock()

def kc(*a, inp=None, tries=4):
    cmd = ["kubectl", "--context", CTX, "-n", NS, "--request-timeout=20s"] + list(a)
    for i in range(tries):
        r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
        if r.returncode == 0:
            return r.stdout
        e = (r.stderr or "")
        if "NotFound" in e or "not found" in e or "already exists" in e:
            return ""
        time.sleep(min(2 ** i, 6))
    return ""

ENV = [{"name": "CURIE_BUNDLE_REF", "value": REF},
       {"name": "CURIE_PLUGIN_DIR", "value": "/bundles/current"},
       {"containerName": "bundle-fetch", "name": "CURIE_BUNDLE_REF", "value": REF},
       {"containerName": "bundle-extract", "name": "CURIE_BUNDLE_REF", "value": REF}]

def turn(pod):
    script = ("import json,urllib.request,sys\n"
              "f={'kind':'event','type':'message','text':'ack','ts':'1','user':'U'}\n"
              "r=urllib.request.Request('http://localhost:8080/v1/event',"
              "data=json.dumps(f).encode(),headers={'Content-Type':'application/json'},method='POST')\n"
              "fin=None\n"
              "try:\n"
              "    for line in urllib.request.urlopen(r,timeout=120):\n"
              "        try:\n"
              "            o=json.loads(line)\n"
              "            if o.get('type')=='final': fin=o\n"
              "        except Exception: pass\n"
              "except Exception as e: print(json.dumps({'err':type(e).__name__})); sys.exit(0)\n"
              "print(json.dumps({'status':(fin or {}).get('status')}))\n")
    out = kc("exec", pod, "-c", "runner", "--", "/app/.venv/bin/python", "-c", script, tries=2)
    for line in reversed(out.strip().splitlines()):
        try:
            d = json.loads(line)
            if "status" in d or "err" in d:
                return d.get("status") == "done", d.get("status") or d.get("err")
        except Exception:
            continue
    return False, "no-reply"

def conversation(i, hold, budget=100):
    name = f"cap-{i:03d}"
    rec = {"i": i, "claim_s": None, "ok": False, "status": None, "quota_blocked": False}
    t0 = time.monotonic()
    kc("apply", "-f", "-", inp=json.dumps(
        {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxClaim",
         "metadata": {"name": name, "namespace": NS, "labels": {"cap": "adr0116"}},
         "spec": {"warmPoolRef": {"name": "curie-runner-pool"}, "env": ENV}}))
    pod = None
    while time.monotonic() - t0 < budget:
        out = kc("get", "sandboxclaim", name, "-o", "json")
        if out:
            try:
                st = json.loads(out).get("status") or {}
            except Exception:
                st = {}
            for c in st.get("conditions") or []:
                m = c.get("message") or ""
                if "exceeded quota" in m:
                    rec["quota_blocked"] = True
                    rec["status"] = "quota-exceeded"
            sb = (st.get("sandbox") or {}).get("name")
            if sb:
                po = kc("get", "pod", sb, "-o", "json")
                if po:
                    try:
                        cs = (json.loads(po).get("status", {}).get("containerStatuses") or [])
                    except Exception:
                        cs = []
                    for c in cs:
                        if c["name"] == "runner" and c.get("ready"):
                            pod = sb
        if pod:
            break
        time.sleep(0.2)
    rec["claim_s"] = time.monotonic() - t0
    if not pod:
        rec["status"] = rec["status"] or "never-ready"
    else:
        rec["ok"], rec["status"] = turn(pod)
        if hold:
            time.sleep(hold)          # the residency window
    kc("delete", "sandboxclaim", name, "--ignore-not-found")
    mark = f"{C['g']}ok{C['x']}" if rec["ok"] else f"{C['r']}{rec['status']}{C['x']}"
    with _pr:
        print(f"    [{i:>2}] claim {rec['claim_s']:6.2f}s  {mark}", flush=True)
    return rec

def arm(label, n, hold, interval, note):
    with _pr:
        print(f"\n{C['b']}{label}{C['x']}\n  {C['d']}{note}{C['x']}", flush=True)
    kc("delete", "sandboxclaim", "-l", "cap=adr0116", "--ignore-not-found"); time.sleep(10)
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = []
        for i in range(1, n + 1):
            futs.append(ex.submit(conversation, i, hold))
            time.sleep(interval)
        recs = [f.result() for f in futs]
    wall = time.monotonic() - t0
    ok = [r for r in recs if r["ok"]]
    q = [r for r in recs if r["quota_blocked"]]
    with _pr:
        print(f"  {C['b']}-> {len(ok)}/{n} served, {len(q)} blocked by quota, wall {wall:.0f}s{C['x']}", flush=True)
    return {"label": label, "n": n, "hold": hold, "ok": len(ok),
            "quota_blocked": len(q), "wall_s": wall,
            "served_per_min": len(ok) / wall * 60 if wall else 0, "records": recs}
