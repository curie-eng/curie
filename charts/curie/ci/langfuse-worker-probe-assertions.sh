#!/usr/bin/env bash
#
# Render contract for the langfuse-worker Deployment's probes (#71, #2330).
#
# #71 ("K8s: add liveness probes -- worker and dispatcher have none at all")
# closed on the criterion that *every* Deployment with a persistent process
# carries both a readiness and a liveness probe. `langfuse-worker` was the
# remaining violation: its container block ended at `resources:` with no probe
# key at all, so Kubernetes reported `ready: true` the instant the process
# started, `Deployment.Available` stayed true against a wedged worker, and
# `kubectl rollout status` / `helm --wait` succeeded immediately. That is the
# second, independent defect #2330 names (the first -- the missing
# `wait-for-postgres` gate, sibling of #1853/#2009 -- is pinned by
# langfuse-postgres-readiness-assertions.sh).
#
# The probes' proof surface is the RENDER, not a cluster. With no readinessProbe
# the runtime `WORKER_READY == true` check passes identically before and after
# the change (`minReadySeconds` defaults to 0), so only these assertions can
# falsify the probe contract.
#
# WHY readiness is `/api/ready` and liveness is `tcpSocket`, not an HTTP path:
#
#   From the pinned image `langfuse/langfuse-worker:3.225.5`, both `GET
#   /api/health` and `GET /api/ready` call `checkContainerHealth`, which runs a
#   Prisma `SELECT 1` AND a Redis ping; they differ ONLY in `failOnSigterm`.
#
#   * Readiness -> `/api/ready` (failOnSigterm: true). It exercises the event
#     loop and the stores, and it returns 500 during shutdown so a terminating
#     pod drains. `/api/health` as readiness would keep a terminating pod in
#     service.
#   * Liveness -> `tcpSocket:3030`. An HTTP liveness probe on either endpoint
#     couples the worker's LIFE to Postgres and Valkey. The worker is
#     `replicas: 1` with `strategy: Recreate` and runs Prisma/ClickHouse boot
#     migrations at container start, and init containers do NOT re-run on a
#     liveness restart. So a store blip of roughly five minutes
#     (`failureThreshold: 15` x `periodSeconds: 20`) would kill the only replica
#     and restart it straight into boot migrations against a sick Postgres --
#     reproducing the exact Prisma `P1001` / `BackOff` class this ticket exists
#     to stop, with the readiness gate powerless to help. Chart precedent is
#     against dependency-coupled liveness (`mail-adapter.yaml` splits a static
#     `/healthz` from a store-waiting `/readyz`; the API's `/health` touches no
#     DB; `inference.yaml` uses `tcpSocket`); `langfuse-web` probing
#     `/api/public/health` for both is the exception, not the pattern.
#
#   ACCEPTED TRADE-OFF: `tcpSocket` will not catch a fully wedged Node event
#   loop -- the kernel still accepts on the listen backlog. Detection of that
#   rides on the READINESS probe, which does exercise the event loop and flips
#   `Deployment.Available`. Restart-on-hard-hang (process alive, not listening)
#   is liveness's only job here, deliberately.
#
# `timeoutSeconds` is asserted BY NAME on both probes: the kubelet default is
# 1s, and a 1s timeout against an endpoint that runs a Prisma query plus a Redis
# ping is a restart loop waiting to happen.
#
# The cadence numbers below are the chart DEFAULTS, which now come from
# `langfuse.worker.readinessProbe.*` / `.livenessProbe.*` in values.yaml rather
# than from literals in the template. This script pins those defaults by value.
# The invariant that survives an operator OVERRIDE -- every container's liveness
# failure cutoff strictly outlasting its readiness cutoff -- is pinned separately
# by ci/probe-window-assertions.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

if ! helm template curie "$CHART" --show-only templates/langfuse.yaml >"$TMP/default.yaml"; then
  fail "default render failed"
fi

cat >"$TMP/assert.py" <<'PY'
import sys
import yaml

path = sys.argv[1]
docs = [doc for doc in yaml.safe_load_all(open(path)) if doc]

CONTAINER = "langfuse-worker"
SUFFIX = "-langfuse-worker"

matches = [
    doc
    for doc in docs
    if doc.get("kind") == "Deployment"
    and doc.get("metadata", {}).get("name", "").endswith(SUFFIX)
]
if len(matches) != 1:
    raise SystemExit(f"expected exactly one Deployment ending in {SUFFIX!r}, got {len(matches)}")
spec = matches[0]["spec"]["template"]["spec"]
worker = next((c for c in spec.get("containers", []) if c.get("name") == CONTAINER), None)
if worker is None:
    raise SystemExit(f"no {CONTAINER!r} container in the {SUFFIX} Deployment")

ports = worker.get("ports") or []
declared = [p.get("containerPort") for p in ports]
if 3030 not in declared:
    raise SystemExit(f"{CONTAINER} must declare containerPort 3030, got {declared}")
print("  ok: langfuse-worker declares containerPort 3030")

readiness = worker.get("readinessProbe")
liveness = worker.get("livenessProbe")
if readiness is None:
    raise SystemExit(
        f"{CONTAINER} has NO readinessProbe. Without one Kubernetes reports ready:true as soon as "
        f"the process starts, so Deployment.Available and `kubectl rollout status` report a wedged "
        f"worker as healthy -- the #71 criterion this Deployment was the last to violate (#2330)"
    )
if liveness is None:
    raise SystemExit(
        f"{CONTAINER} has NO livenessProbe. A worker whose process is alive but no longer listening "
        f"is never restarted; #71 requires both a readiness AND a liveness probe on every Deployment "
        f"running a persistent process (#2330)"
    )
print("  ok: langfuse-worker carries BOTH a readinessProbe and a livenessProbe (#71)")

# ------------------------------------------------------------------- readiness
http = readiness.get("httpGet")
if not http:
    raise SystemExit(
        f"readinessProbe must be an httpGet on /api/ready: it is the endpoint that actually "
        f"exercises the event loop and the stores, got {sorted(readiness)}"
    )
if http.get("path") != "/api/ready":
    raise SystemExit(
        f"readinessProbe httpGet.path is {http.get('path')!r}, expected '/api/ready'. Only "
        f"/api/ready sets failOnSigterm, so it is the endpoint that returns 500 during shutdown and "
        f"lets a terminating pod drain; /api/health as readiness would keep a terminating pod in "
        f"service"
    )
if http.get("port") != 3030:
    raise SystemExit(
        f"readinessProbe httpGet.port is {http.get('port')!r}, expected the declared containerPort 3030"
    )
print("  ok: readinessProbe is httpGet /api/ready on port 3030 (SIGTERM-aware, so a terminating pod drains)")

# -------------------------------------------------------------------- liveness
if "httpGet" in liveness:
    raise SystemExit(
        f"livenessProbe must NOT use httpGet (got {liveness['httpGet']!r}). Both /api/health and "
        f"/api/ready run a Prisma SELECT 1 and a Redis ping, so an HTTP liveness probe couples the "
        f"worker's LIFE to Postgres and Valkey. The worker is replicas:1 + Recreate and runs boot "
        f"migrations at container start, and init containers do NOT re-run on a liveness restart -- "
        f"so a ~5 minute store blip would restart the single replica straight into boot migrations "
        f"against a sick Postgres, which is the exact P1001/BackOff class #2330 exists to stop. Use "
        f"tcpSocket:3030"
    )
tcp = liveness.get("tcpSocket")
if not tcp:
    raise SystemExit(
        f"livenessProbe must be tcpSocket:3030 -- the only liveness signal the worker image offers "
        f"that does not couple to Postgres and Valkey (got {sorted(liveness)})"
    )
if tcp.get("port") != 3030:
    raise SystemExit(
        f"livenessProbe tcpSocket.port is {tcp.get('port')!r}, expected the declared containerPort 3030"
    )
print("  ok: livenessProbe is tcpSocket:3030 with no httpGet -- a store blip cannot restart the single replica")

# --------------------------------------------------------------------- cadence
EXPECTED = {
    "readinessProbe": (readiness, {
        "initialDelaySeconds": 20,
        "periodSeconds": 10,
        "timeoutSeconds": 5,
        "failureThreshold": 30,
    }),
    "livenessProbe": (liveness, {
        "initialDelaySeconds": 90,
        "periodSeconds": 20,
        "timeoutSeconds": 5,
        "failureThreshold": 15,
    }),
}
for probe_name, (probe, expected) in EXPECTED.items():
    for field, value in expected.items():
        if field not in probe:
            reason = ""
            if field == "timeoutSeconds":
                reason = (
                    " -- the kubelet default is 1s, an omitted key is invisible in the manifest, and "
                    "1s against an endpoint running a Prisma query plus a Redis ping is a restart loop"
                )
            raise SystemExit(
                f"{probe_name} does not declare {field} explicitly{reason}"
            )
        if probe[field] != value:
            raise SystemExit(
                f"{probe_name}.{field} is {probe[field]!r}, expected {value!r}"
            )
print("  ok: readiness cadence 20/10/5/30 and liveness cadence 90/20/5/15, both with an explicit timeoutSeconds")
PY

python3 "$TMP/assert.py" "$TMP/default.yaml"

# --------------------------------------------------------------- red controls
# Each control mutates a REAL default render and proves the assertion rejects it
# for the INTENDED reason, not an incidental KeyError.
cat >"$TMP/mutate.py" <<'PY'
import sys
import yaml

source, target, mutation = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if doc]
worker = None
for doc in docs:
    if doc.get("kind") != "Deployment":
        continue
    for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
        if container.get("name") == "langfuse-worker":
            worker = container
if worker is None:
    raise SystemExit("no langfuse-worker container to mutate")

if mutation == "drop-readiness":
    worker.pop("readinessProbe", None)
elif mutation == "drop-liveness":
    worker.pop("livenessProbe", None)
elif mutation == "liveness-http-health":
    worker["livenessProbe"] = {
        "httpGet": {"path": "/api/health", "port": 3030},
        "initialDelaySeconds": 90,
        "periodSeconds": 20,
        "timeoutSeconds": 5,
        "failureThreshold": 15,
    }
elif mutation == "readiness-health":
    worker.setdefault("readinessProbe", {}).setdefault("httpGet", {})["path"] = "/api/health"
elif mutation == "drop-liveness-timeout":
    worker.get("livenessProbe", {}).pop("timeoutSeconds", None)
else:
    raise SystemExit(f"unknown mutation {mutation!r}")

with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

assert_rejected() {
  local mutation="$1" needle="$2" label="$3" output rc
  python3 "$TMP/mutate.py" "$TMP/default.yaml" "$TMP/$mutation.yaml" "$mutation"
  set +e
  output="$(python3 "$TMP/assert.py" "$TMP/$mutation.yaml" 2>&1)"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "negative control passed: $label"
  [[ "$output" == *"$needle"* ]] || fail "$label was rejected for an unrelated reason: $output"
  echo "  ok: $label is rejected"
}

assert_rejected drop-readiness "NO readinessProbe" "removing the readinessProbe"
assert_rejected drop-liveness "NO livenessProbe" "removing the livenessProbe"
assert_rejected liveness-http-health "must NOT use httpGet" \
  "swapping liveness to httpGet /api/health (a store-coupled probe that restarts the single replica on a Postgres or Valkey blip)"
assert_rejected readiness-health "keep a terminating pod in service" \
  "swapping readiness to /api/health"
assert_rejected drop-liveness-timeout "livenessProbe does not declare timeoutSeconds explicitly" \
  "dropping the explicit livenessProbe timeoutSeconds (kubelet would default it to 1s)"

echo "PASS: langfuse-worker carries a readiness AND a liveness probe on its :3030 endpoint -- httpGet /api/ready for readiness so a terminating pod drains, tcpSocket for liveness so a store blip cannot restart the single replica into its boot migrations (#71, #2330)"
