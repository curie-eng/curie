#!/usr/bin/env python3
"""Throughput of one-shot conversations under a fixed sandbox budget.

Both arms get the SAME namespace quota (the shipped 8 sandbox slots) and the
SAME concurrency. Arm B spends part of that budget on a warm pool, so it is
handicapped, not favoured.

Each conversation is the whole round trip: claim -> ready -> POST /v1/event ->
verify a `final` frame -> release. A conversation counts as SUCCESS only if the
reply came back as a terminal frame; a ready pod that never answers is a
failure.
"""
import json, os, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

NS = "curie"
CTX = os.environ.get("LOAD_CONTEXT", "curie-load")
assert CTX.startswith("curie-load"), f"refusing context {CTX!r}"
BUNDLE_REF = os.environ["BUNDLE_REF"]
C = {"g": "\033[32m", "r": "\033[31m", "y": "\033[33m", "b": "\033[1m", "d": "\033[2m", "x": "\033[0m"}
_pr = threading.Lock()

def kc(*a, inp=None, tries=4, timeout="20s"):
    cmd = ["kubectl", "--context", CTX, "-n", NS, f"--request-timeout={timeout}"] + list(a)
    last = ""
    for i in range(tries):
        r = subprocess.run(cmd, capture_output=True, text=True, input=inp)
        if r.returncode == 0:
            return r.stdout
        last = (r.stderr or "").strip()
        if "NotFound" in last or "not found" in last or "already exists" in last:
            return ""
        time.sleep(min(2 ** i, 6))
    return ""

PER_CLAIM_ENV = [
    {"name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
    {"name": "CURIE_PLUGIN_DIR", "value": "/bundles/current"},
    {"containerName": "bundle-fetch", "name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
    {"containerName": "bundle-extract", "name": "CURIE_BUNDLE_REF", "value": BUNDLE_REF},
]

def claim_obj(name, pool, env):
    o = {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxClaim",
         "metadata": {"name": name, "namespace": NS, "labels": {"load.curietech.ai": "adr0116"}},
         "spec": {"warmPoolRef": {"name": pool}}}
    if env:
        o["spec"]["env"] = env
    return o

def turn(pod, text, budget=180):
    """Drive one real ACI turn inside the pod. Returns (ok, status, frames)."""
    script = (
        "import json,urllib.request,urllib.error,sys\n"
        f"f={{'kind':'event','type':'message','text':{text!r},'ts':'1','user':'U_LOAD'}}\n"
        "r=urllib.request.Request('http://localhost:8080/v1/event',"
        "data=json.dumps(f).encode(),headers={'Content-Type':'application/json'},method='POST')\n"
        "n=0;final=None\n"
        "try:\n"
        "    resp=urllib.request.urlopen(r,timeout=%d)\n"
        "    for line in resp:\n"
        "        n+=1\n"
        "        try:\n"
        "            o=json.loads(line)\n"
        "            if o.get('type')=='final': final=o\n"
        "        except Exception: pass\n"
        "except Exception as e:\n"
        "    print(json.dumps({'err':type(e).__name__})); sys.exit(0)\n"
        "print(json.dumps({'frames':n,'status':(final or {}).get('status'),"
        "'text':((final or {}).get('text') or '')[:60]}))\n" % budget
    )
    out = kc("exec", pod, "-c", "runner", "--", "/app/.venv/bin/python", "-c", script,
             tries=2, timeout=f"{budget + 30}s")
    for line in reversed(out.strip().splitlines()):
        try:
            d = json.loads(line)
            if "status" in d or "err" in d:
                return (d.get("status") == "done", d.get("status") or d.get("err"), d.get("frames", 0))
        except Exception:
            continue
    return (False, "no-reply", 0)

def conversation(i, pool, env, ready_budget):
    """One whole one-shot conversation. Returns a record."""
    name = f"load-{i:03d}"
    rec = {"i": i, "claim_s": None, "turn_s": None, "ok": False, "status": None, "deadline_exceeded": False}
    t0 = time.monotonic()
    kc("apply", "-f", "-", inp=json.dumps(claim_obj(name, pool, env)))
    pod = None
    while time.monotonic() - t0 < ready_budget:
        out = kc("get", "sandboxclaim", name, "-o", "json")
        if out:
            try:
                st = json.loads(out).get("status") or {}
            except Exception:
                st = {}
            sb = (st.get("sandbox") or {}).get("name")
            if sb:
                po = kc("get", "pod", sb, "-o", "json")
                if po:
                    try:
                        s = json.loads(po).get("status", {})
                    except Exception:
                        s = {}
                    for cs in s.get("containerStatuses") or []:
                        if cs["name"] == "runner" and cs.get("ready"):
                            pod = sb
                            break
        if pod:
            break
        time.sleep(0.15)
    rec["claim_s"] = time.monotonic() - t0
    if rec["claim_s"] > 90:
        rec["deadline_exceeded"] = True
    if not pod:
        rec["status"] = "never-ready"
        kc("delete", "sandboxclaim", name, "--ignore-not-found")
        return rec
    t1 = time.monotonic()
    ok, status, frames = turn(pod, f"conversation {i}: reply with a one line ack")
    rec["turn_s"] = time.monotonic() - t1
    rec["ok"], rec["status"], rec["frames"] = ok, status, frames
    kc("delete", "sandboxclaim", name, "--ignore-not-found")   # one-shot: release immediately
    return rec

def run_arm(label, n, conc, pool, env, ready_budget=110):
    with _pr:
        print(f"\n{C['b']}{label}{C['x']}")
        print(f"  {C['d']}{n} one-shot conversations, {conc} at a time, "
              f"pool={pool}, per-claim env={'yes' if env else 'no'}{C['x']}", flush=True)
    t0 = time.monotonic()
    recs = []
    with ThreadPoolExecutor(max_workers=conc) as ex:
        futs = [ex.submit(conversation, i, pool, env, ready_budget) for i in range(1, n + 1)]
        done = 0
        for f in futs:
            r = f.result()
            recs.append(r)
            done += 1
            mark = f"{C['g']}ok{C['x']}" if r["ok"] else f"{C['r']}{r['status']}{C['x']}"
            with _pr:
                print(f"    [{done:>2}/{n}] claim {r['claim_s']:6.2f}s  "
                      f"turn {(r['turn_s'] or 0):5.2f}s  {mark}", flush=True)
    wall = time.monotonic() - t0
    oks = [r for r in recs if r["ok"]]
    cl = sorted(r["claim_s"] for r in recs)
    def pct(p):
        return cl[min(len(cl) - 1, int(len(cl) * p))] if cl else 0
    summary = {
        "label": label, "n": n, "conc": conc, "wall_s": wall,
        "ok": len(oks), "failed": n - len(oks),
        "deadline_exceeded": sum(1 for r in recs if r["deadline_exceeded"]),
        "claim_p50": pct(0.50), "claim_p95": pct(0.95), "claim_max": cl[-1] if cl else 0,
        "throughput_per_min": len(oks) / wall * 60 if wall else 0,
        "records": recs,
    }
    with _pr:
        print(f"  {C['b']}-> {len(oks)}/{n} succeeded in {wall:.1f}s "
              f"({summary['throughput_per_min']:.1f} conversations/min), "
              f"claim p50 {summary['claim_p50']:.2f}s p95 {summary['claim_p95']:.2f}s"
              f"{C['x']}", flush=True)
        if summary["deadline_exceeded"]:
            print(f"  {C['r']}   {summary['deadline_exceeded']} exceeded the 90s claim deadline{C['x']}", flush=True)
    return summary
