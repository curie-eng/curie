#!/usr/bin/env bash
#
# Render-assertion test for issue #3831 (ADR 0197). One optional CA bundle,
# codeHostTrust.caBundle.configMapRef, is mounted read-only at one path
# wherever a code host is reached: the api, the worker (which hands the same
# ConfigMap to every publication Job it builds) and every sandbox runner. The
# runner's git and HTTP clients point at it. Unset, nothing renders.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

render() {
  helm template rel "$CHART" -f "$CHART/values-dev.yaml" \
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=false "$@"
}

check() {
  python3 -c '
import sys, yaml

mode = sys.argv[1]
PATH_ = "/etc/curie/code-host-trust/ca.crt"
CLIENT_ENV = ("CURIE_REPO_CA_BUNDLE", "GIT_SSL_CAINFO", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")

def fail(msg):
    sys.stderr.write(f"FAIL: {msg}\n")
    sys.exit(1)

docs = [d for d in yaml.safe_load_all(sys.stdin) if d]

def pod(kind, suffix):
    for d in docs:
        if d.get("kind") != kind or not d["metadata"]["name"].endswith(suffix):
            continue
        if kind == "SandboxTemplate":
            return d["spec"]["podTemplate"]["spec"]
        return d["spec"]["template"]["spec"]
    fail(f"no {kind} named *{suffix} rendered")

def env(container):
    return {e["name"]: e.get("value") for e in container.get("env", []) or []}

def mounted(spec, container):
    vols = [v for v in spec.get("volumes", []) or [] if v["name"] == "code-host-trust"]
    mounts = [m for m in container.get("volumeMounts", []) or [] if m["name"] == "code-host-trust"]
    return vols, mounts

api = pod("Deployment", "-curie-api")
worker = pod("Deployment", "-curie-worker")
sandbox = pod("SandboxTemplate", "-curie-runner")
targets = {
    "api": (api, next(c for c in api["containers"] if c["name"] == "api")),
    "worker": (worker, next(c for c in worker["containers"] if c["name"] == "worker")),
    "runner": (sandbox, next(c for c in sandbox["containers"] if c["name"] == "runner")),
}

for name, (spec, container) in targets.items():
    vols, mounts = mounted(spec, container)
    e = env(container)
    if mode == "unset":
        if vols or mounts:
            fail(f"{name}: unset codeHostTrust still renders the trust volume")
        leaked = [k for k in (*CLIENT_ENV, "CURIE_CODE_HOST_CA_BUNDLE", "CURIE_CODE_HOST_CA_CONFIGMAP") if k in e]
        if leaked:
            fail(f"{name}: unset codeHostTrust still renders {leaked}")
        continue
    if len(vols) != 1 or vols[0]["configMap"]["name"] != "corp-ca":
        fail(f"{name}: expected one code-host-trust volume from ConfigMap corp-ca, got {vols}")
    if vols[0]["configMap"]["items"] != [{"key": "bundle.pem", "path": "ca.crt"}]:
        fail(f"{name}: the configured key must project to ca.crt, got {vols[0]}")
    if len(mounts) != 1 or mounts[0].get("readOnly") is not True:
        fail(f"{name}: the trust bundle must be mounted exactly once, read-only: {mounts}")
    if mounts[0]["mountPath"] != "/etc/curie/code-host-trust":
        fail(f"{name}: unexpected trust mount {mounts[0]}")

if mode == "set":
    runner_env = env(targets["runner"][1])
    for key in CLIENT_ENV:
        if runner_env.get(key) != PATH_:
            fail(f"runner: {key} must point at {PATH_}, got {runner_env.get(key)}")
    api_env = env(targets["api"][1])
    if api_env.get("CURIE_CODE_HOST_CA_BUNDLE") != PATH_:
        fail("api: CURIE_CODE_HOST_CA_BUNDLE must name the mounted bundle")
    worker_env = env(targets["worker"][1])
    if worker_env.get("CURIE_CODE_HOST_CA_CONFIGMAP") != "corp-ca":
        fail("worker: CURIE_CODE_HOST_CA_CONFIGMAP must name the ConfigMap for publication Jobs")
    if worker_env.get("CURIE_CODE_HOST_CA_CONFIGMAP_KEY") != "bundle.pem":
        fail("worker: CURIE_CODE_HOST_CA_CONFIGMAP_KEY must carry the configured key")
    init = next(c for c in sandbox["initContainers"] if c["name"] == "workspace-init")
    if any(k in env(init) for k in CLIENT_ENV):
        fail("workspace-init reaches no code host and must not carry the trust env")
print(f"  ok: codeHostTrust {mode}")
' "$1"
}

render | check unset
render --set codeHostTrust.caBundle.configMapRef.name=corp-ca \
  --set codeHostTrust.caBundle.configMapRef.key=bundle.pem | check set

# Negative control: the check must fail when a mount loses readOnly.
if render --set codeHostTrust.caBundle.configMapRef.name=corp-ca \
  --set codeHostTrust.caBundle.configMapRef.key=bundle.pem \
  | sed 's/readOnly: true/readOnly: false/' | check set >/dev/null 2>&1; then
  echo "FAIL: a writable trust mount passed the check" >&2
  exit 1
fi
echo "  ok: negative control (writable mount) fails the check"

echo "code-host-trust-assertions: all assertions passed"
