#!/usr/bin/env bash
# #4016: an approved Bash gate resumes the ORIGINAL listener after the worker,
# Postgres, and Valkey are all restarted while the approval is pending.
#
# Drives the public CLI against a preinstalled disposable release (CI installs
# it with `curie cluster up --dev --fake-model`). The fake model's default turn
# calls Bash `echo hi`, which the bundle's approval gate holds. The order
# otherwise follows the issue: worker, Postgres, Valkey, still pending, resolve.
# While the approval is pending the script replaces the worker pod(s), rolls
# the Postgres StatefulSet, and STOPS Valkey (scale to 0). The resolve is issued
# inside that Valkey stop window, so the API's inline resume enqueue
# deterministically fails, which is the #4016 failure; the script asserts the
# API logged that failure, so a pass cannot come from a resolve that happened
# to land while Valkey was reachable. Valkey is then started again and the
# script requires:
#   AC1  the original `cluster message` listener exits 0 with finalized=true
#        within RESUME_BOUND_SECONDS of the resolve;
#   AC2  exactly one runner `tool call ... tool=Bash` line for the turn's thread,
#        and an audit row action=resolved, principal_kind=operator,
#        actor=<approver>.
#
# What this script OWNS (and is the only thing its cleanup removes):
#   1. the uniquely named agent it deploys (kill + delete through the CLI);
#   2. the approval its own turn raises (rejected in cleanup if still pending);
#   3. its background listener process;
#   4. its mktemp work dir, including the 0600 operator token file;
#   5. with the red-on-revert knobs only: the api Deployment image change,
#      rolled back to the recorded revision in cleanup.
# What it restarts but never deletes: the release's worker pods and the
# Postgres and Valkey StatefulSets; they are left rolled out and Ready. Valkey
# is scaled to 0 and back to its recorded replica count; cleanup scales it back
# first if the run stops inside the stop window.
# What it NEVER touches: the namespace, the Helm release, the agent-sandbox
# controller, any other agent, and any pod other than the release's worker pods
# and the two StatefulSets' pods.
#
# Environment contract (same as e2e-cluster-rollout-recovery.sh):
#   CURIE_BIN                 executable curie binary (required)
#   CURIE_E2E_LISTEN_HOST     pod-reachable callback host (required)
#   CURIE_E2E_NAMESPACE       release namespace (default curie)
#   CURIE_E2E_RELEASE         Helm release name (default curie)
#   CURIE_E2E_KUBE_CONTEXT    optional; adds --context to every kubectl call and
#                             every `curie cluster` call (a global flag there)
#
# Optional red-on-revert knobs (normal CI leaves these unset):
#   CURIE_E2E_PRE_FIX_API_IMAGE   pre-fix api repository
#   CURIE_E2E_PRE_FIX_API_TAG     pre-fix api tag
# When both are set the api Deployment runs that image for the scenario (the
# fix under test lives in the API's resume reconciler). Against pre-fix images
# this script must fail at the AC1 assertion: the resolve returns approved but
# the resume enqueue that raised during the Valkey stop then waits out the
# resume reconciler's grace (10860 s on the chart), so the listener never
# finalizes within RESUME_BOUND_SECONDS.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${CURIE_E2E_NAMESPACE:-curie}"
RELEASE="${CURIE_E2E_RELEASE:-curie}"
KUBE_CONTEXT="${CURIE_E2E_KUBE_CONTEXT:-}"
PRE_FIX_API_IMAGE="${CURIE_E2E_PRE_FIX_API_IMAGE:-}"
PRE_FIX_API_TAG="${CURIE_E2E_PRE_FIX_API_TAG:-}"

# The chart's curie.fullname with no overrides: the release name when it
# already contains "curie", else "<release>-curie". CI's release is "curie".
if [[ "$RELEASE" == *curie* ]]; then
    FULLNAME="$RELEASE"
else
    FULLNAME="${RELEASE}-curie"
fi
API_DEPLOYMENT="${FULLNAME}-api"
WORKER_DEPLOYMENT="${FULLNAME}-worker"
POSTGRES_STATEFULSET="${FULLNAME}-postgres"
VALKEY_STATEFULSET="${FULLNAME}-valkey"

RESUME_BOUND_SECONDS=300
APPROVAL_APPEAR_BUDGET_SECONDS=180
STILL_PENDING_BUDGET_SECONDS=300
RESOLVE_BUDGET_SECONDS=180
VALKEY_STOP_BUDGET_SECONDS=120
RUNNER_LOG_BUDGET_SECONDS=60
ROLLOUT_TIMEOUT="300s"
# The original listener runs with --timeout-secs 600 (#4016), so restart time
# before the resolve cannot fail it by itself; AC1 is the separate
# RESUME_BOUND_SECONDS bound measured from the resolve.
LISTEN_TIMEOUT_SECONDS=600

RUN_ID="$(date -u +%H%M%S)$$${RANDOM}"
RUN_TAG="$(printf '%s' "$RUN_ID" | tr -cd '0-9' | tail -c 10)"
AGENT="e2e-approval-resume-${RUN_TAG}"
ROUTE="e2e_resume_${RUN_TAG}"
CHANNEL="C0E2E${RUN_TAG}"
APPROVER="U0E2E${RUN_TAG}"
MARKER="curie-4016-${RUN_TAG}"

umask 077
WORKDIR="$(mktemp -d)"
BUNDLE_DIR="$WORKDIR/bundle"
TOKEN_FILE="$WORKDIR/operator-token"
LISTENER_OUT="$WORKDIR/listener.json"
LISTENER_ERR="$WORKDIR/listener.err"

LISTENER_PID=""
AGENT_OWNED=0
APPROVAL_ID=""
APPROVAL_RESOLVED=0
API_PATCHED=0
API_ORIGINAL_REVISION=""
TOKEN=""
VALKEY_ORIGINAL_REPLICAS=""
VALKEY_STOPPED=0
SCRIPT_STARTED_RFC3339="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -z "${CURIE_BIN:-}" || ! -x "${CURIE_BIN:-}" ]]; then
    echo "error: CURIE_BIN must name an executable curie binary" >&2
    exit 1
fi
if [[ -z "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
    echo "error: CURIE_E2E_LISTEN_HOST must name the pod-reachable callback host" >&2
    exit 1
fi
if [[ -n "$PRE_FIX_API_IMAGE" || -n "$PRE_FIX_API_TAG" ]]; then
    if [[ -z "$PRE_FIX_API_IMAGE" || -z "$PRE_FIX_API_TAG" ]]; then
        echo "error: CURIE_E2E_PRE_FIX_API_IMAGE and CURIE_E2E_PRE_FIX_API_TAG must be set together" >&2
        exit 1
    fi
fi
BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"

KUBECTL_CTX=()
CURIE_CTX=()
if [[ -n "$KUBE_CONTEXT" ]]; then
    KUBECTL_CTX=(--context "$KUBE_CONTEXT")
    CURIE_CTX=(--context "$KUBE_CONTEXT")
fi

# `${arr[@]+...}`: bash 3.2 reports an empty array as unbound under `set -u`.
kube() { kubectl ${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"} -n "$NAMESPACE" "$@"; }
curie_cluster() { "$BIN" --json cluster ${CURIE_CTX[@]+"${CURIE_CTX[@]}"} "$@"; }
approvals_cli() {
    curie_cluster approvals "$AGENT" --namespace "$NAMESPACE" --release "$RELEASE" "$@"
}

stop_pid() {
    local pid="$1"
    [[ -n "$pid" ]] || return 0
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

wait_for_pid() {
    local pid="$1" budget="$2" started=$SECONDS
    while kill -0 "$pid" 2>/dev/null; do
        if (( SECONDS - started >= budget )); then return 124; fi
        sleep 0.25
    done
    wait "$pid"
}

# Read the API from INSIDE the api pod, as cli/scripts/upgrade-drill.sh does:
# the audit trail has no CLI verb, and this never materializes the platform key
# in this shell; the container already holds it in $API_KEY.
api_get_in_cluster() {
    local path="$1"
    kube exec -i "deploy/$API_DEPLOYMENT" -c api -- python3 -c '
import json, os, sys, urllib.error, urllib.request
req = urllib.request.Request("http://127.0.0.1:8000" + sys.argv[1], method="GET")
req.add_header("X-API-Key", os.environ["API_KEY"])
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        sys.stdout.write(resp.read().decode())
except urllib.error.HTTPError as err:
    json.dump({"http_status": err.code, "detail": err.read().decode()}, sys.stdout)
' "$path" </dev/null
}

approval_status() {
    api_get_in_cluster "/approvals/$1" 2>/dev/null | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("status") or "")
except Exception:
    print("")
' || true
}

# Pending rows as "id<TAB>route<TAB>granted_tool". Non-zero when the list call
# fails, so callers can tell "no rows" from "API not answering".
pending_rows() {
    local raw
    raw="$(approvals_cli --list 2>/dev/null)" || return 1
    printf '%s' "$raw" | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    raise SystemExit(1)
doc = json.loads(raw.splitlines()[-1])
for row in doc.get("pending") or []:
    print("{}\t{}\t{}".format(row.get("id"), row.get("route") or "", row.get("granted_tool") or ""))
'
}

pods_by_selector_of() {
    # Pod name<TAB>uid<TAB>ready for the workload's own selector.
    local kind="$1" name="$2" selector
    selector="$(kube get "$kind" "$name" -o json | python3 -c '
import json, sys
labels = json.load(sys.stdin)["spec"]["selector"]["matchLabels"]
print(",".join("{}={}".format(k, v) for k, v in sorted(labels.items())))
')"
    kube get pods -l "$selector" -o json | python3 -c '
import json, sys
for pod in json.load(sys.stdin).get("items", []):
    meta = pod["metadata"]
    ready = any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", []))
    gone = bool(meta.get("deletionTimestamp"))
    print("{}\t{}\t{}".format(meta["name"], meta["uid"], "ready" if ready and not gone else "notready"))
'
}

assert_replaced() {
    local label="$1" kind="$2" name="$3" before="$4" after
    after="$(pods_by_selector_of "$kind" "$name")"
    python3 - "$label" "$before" "$after" <<'PY'
import sys
label, before, after = sys.argv[1:]
old = {line.split("\t")[1] for line in before.splitlines() if line.strip()}
rows = [line.split("\t") for line in after.splitlines() if line.strip()]
if not old:
    raise SystemExit(f"error: {label}: no pre-restart pods were recorded")
survivors = [r[0] for r in rows if r[1] in old]
if survivors:
    raise SystemExit(f"error: {label}: pre-restart pod(s) not replaced: {survivors}")
if not any(r[2] == "ready" for r in rows):
    raise SystemExit(f"error: {label}: no Ready replacement pod: {rows}")
print(f"{label}: all {len(old)} pre-restart pod(s) replaced; replacement Ready")
PY
}

assert_release_healthy() {
    kube rollout status "deployment/$API_DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
    kube rollout status "deployment/$WORKER_DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
    kube rollout status "statefulset/$POSTGRES_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
    kube rollout status "statefulset/$VALKEY_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
}

restore_api_deployment() {
    (( API_PATCHED )) || return 0
    [[ -n "$API_ORIGINAL_REVISION" ]] || {
        echo "error: cannot restore api Deployment without its original revision" >&2
        return 1
    }
    kube rollout undo "deployment/$API_DEPLOYMENT" --to-revision="$API_ORIGINAL_REVISION" >/dev/null
    kube rollout status "deployment/$API_DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
    API_PATCHED=0
}

# Scale Valkey back to its recorded replica count and wait for it.
# Checks each step explicitly: cleanup calls this under `set +e`, so errexit
# cannot be relied on. VALKEY_STOPPED clears only after both steps succeed.
start_valkey() {
    (( VALKEY_STOPPED )) || return 0
    if ! kube scale "statefulset/$VALKEY_STATEFULSET" \
        --replicas="$VALKEY_ORIGINAL_REPLICAS" >/dev/null; then
        echo "error: kubectl scale statefulset/$VALKEY_STATEFULSET --replicas=$VALKEY_ORIGINAL_REPLICAS failed" >&2
        return 1
    fi
    if ! kube rollout status "statefulset/$VALKEY_STATEFULSET" \
        --timeout="$ROLLOUT_TIMEOUT" >/dev/null; then
        echo "error: statefulset/$VALKEY_STATEFULSET did not finish rolling out after scaling back" >&2
        return 1
    fi
    VALKEY_STOPPED=0
}

# HTTP status of a failed `curie --json` call. Under --json the centralized
# error handler writes {"error": "...", "fix": ...} to STDOUT (cli/src/exit.rs
# error_json), e.g. "resolving approval failed with 500 Internal Server Error",
# so stdout is read first and stderr is only a fallback. Prints nothing when no
# status is present (transport failure).
cli_http_status() {
    python3 - "$1" "$2" <<'PY'
import json, pathlib, re, sys
pattern = re.compile(r"failed with (\d{3})\b")
def from_stdout(path):
    try:
        lines = pathlib.Path(path).read_text(errors="replace").strip().splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            doc = json.loads(line)
        except ValueError:
            continue
        if isinstance(doc, dict) and isinstance(doc.get("error"), str):
            match = pattern.findall(doc["error"])
            return match[-1] if match else ""
    return None
status = from_stdout(sys.argv[1])
if not status:
    try:
        match = pattern.findall(pathlib.Path(sys.argv[2]).read_text(errors="replace"))
    except OSError:
        match = []
    status = match[-1] if match else ""
print(status)
PY
}

# Both streams of a failed `curie --json` call, for diagnostics.
show_cli_failure() {
    echo "--- stdout ---" >&2
    tail -n 20 "$1" >&2 2>/dev/null || true
    echo "--- stderr ---" >&2
    tail -n 20 "$2" >&2 2>/dev/null || true
}

cleanup() {
    local code=$?
    trap - EXIT INT TERM
    set +e
    stop_pid "$LISTENER_PID"
    # Before anything that needs the stack (approval reject, agent kill/delete).
    if (( VALKEY_STOPPED )); then
        if ! start_valkey; then
            echo "error: cleanup could not restore statefulset/$VALKEY_STATEFULSET to $VALKEY_ORIGINAL_REPLICAS replica(s); it is left stopped" >&2
            [[ "$code" -ne 0 ]] || code=1
        fi
    fi
    # Leave nothing pending that this run raised.
    if [[ -n "$APPROVAL_ID" && "$APPROVAL_RESOLVED" == "0" && -n "$TOKEN" ]]; then
        if [[ "$(approval_status "$APPROVAL_ID")" == "pending" ]]; then
            CURIE_APPROVAL_PRINCIPAL_TOKEN="$TOKEN" approvals_cli \
                --resolve "$APPROVAL_ID" --reject >/dev/null 2>&1 \
                || echo "warning: could not reject owned approval $APPROVAL_ID" >&2
        fi
    fi
    if (( AGENT_OWNED )); then
        curie_cluster kill "$AGENT" --namespace "$NAMESPACE" --release "$RELEASE" --yes \
            >/dev/null 2>&1 || echo "warning: could not kill owned agent $AGENT" >&2
        if ! curie_cluster delete "$AGENT" --namespace "$NAMESPACE" --release "$RELEASE" --yes \
            >"$WORKDIR/delete.json" 2>"$WORKDIR/delete.err"; then
            echo "error: could not delete owned agent $AGENT" >&2
            show_cli_failure "$WORKDIR/delete.json" "$WORKDIR/delete.err"
            [[ "$code" -ne 0 ]] || code=1
        fi
    fi
    if ! restore_api_deployment; then
        echo "error: could not roll the api Deployment back to revision $API_ORIGINAL_REVISION" >&2
        [[ "$code" -ne 0 ]] || code=1
    fi
    TOKEN=""
    rm -rf -- "$WORKDIR"
    exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
    echo "error: $*" >&2
    exit 1
}

dump_diagnostics() {
    echo "--- listener stderr (tail) ---" >&2
    tail -n 40 "$LISTENER_ERR" >&2 2>/dev/null || true
    echo "--- listener stdout ---" >&2
    cat "$LISTENER_OUT" >&2 2>/dev/null || true
    if [[ -n "$APPROVAL_ID" ]]; then
        echo "--- approval $APPROVAL_ID ---" >&2
        api_get_in_cluster "/approvals/$APPROVAL_ID" >&2 2>/dev/null || true
        echo >&2
    fi
}

echo "=== preflight: release workloads for $RELEASE in $NAMESPACE ==="
for workload in "deployment/$API_DEPLOYMENT" "deployment/$WORKER_DEPLOYMENT" \
                "statefulset/$POSTGRES_STATEFULSET" "statefulset/$VALKEY_STATEFULSET"; do
    kube get "$workload" -o name >/dev/null 2>&1 \
        || fail "release workload $workload not found in namespace $NAMESPACE (chart fullname $FULLNAME)"
done

if [[ -n "$PRE_FIX_API_IMAGE" ]]; then
    echo "=== red-on-revert: api runs pre-fix ${PRE_FIX_API_IMAGE}:${PRE_FIX_API_TAG} ==="
    API_ORIGINAL_REVISION="$(kube get deployment "$API_DEPLOYMENT" \
        -o jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}')"
    [[ -n "$API_ORIGINAL_REVISION" ]] || fail "api Deployment has no revision annotation"
    API_PATCHED=1
    kube set image "deployment/$API_DEPLOYMENT" "api=${PRE_FIX_API_IMAGE}:${PRE_FIX_API_TAG}" >/dev/null
    kube rollout status "deployment/$API_DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
fi

echo "=== fixture bundle: Bash gated on route $ROUTE ==="
python3 - "$BUNDLE_DIR" "$AGENT" "$ROUTE" <<'PY'
import json, sys
from pathlib import Path

bundle, name, route = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
(bundle / ".claude-plugin").mkdir(parents=True, exist_ok=True)
(bundle / "skills" / "approval-resume").mkdir(parents=True, exist_ok=True)
manifest = dict(
    name=name,
    version="0.1.0",
    description="Approval resume across worker and store restarts (#4016).",
    systemPrompt="Run the requested shell command with the Bash tool.",
    approvalPolicy=dict(gates=[dict(gate="Bash", route=route)]),
)
(bundle / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest) + "\n")
# No allowed-tools line: a skill that pre-grants Bash defeats the gate and the
# runner refuses to boot.
(bundle / "skills" / "approval-resume" / "SKILL.md").write_text(
    "---\nname: approval-resume\ndescription: Run a gated shell command.\n---\n\n"
    "Run the requested shell command with the Bash tool.\n"
)
(bundle / ".mcp.json").write_text('{"mcpServers":{}}\n')
PY

deploy_agent() {
    curie_cluster deploy --plugin-dir "$BUNDLE_DIR" --agent "$AGENT" \
        --slack-channel "$CHANNEL" --namespace "$NAMESPACE" --release "$RELEASE" \
        --chart "$REPO_ROOT/charts/curie"
}

echo "=== first deploy: creates $AGENT; refused while route $ROUTE is unbound ==="
if deploy_agent >"$WORKDIR/deploy1.json" 2>"$WORKDIR/deploy1.err"; then
    # The agent exists either way; a pass here only means the refusal moved.
    AGENT_OWNED=1
    echo "note: first deploy succeeded before the route was bound"
else
    # Tolerate ONLY the unbound-route refusal: the agent must now exist and the
    # refusal must name this route.
    if approvals_cli --list >/dev/null 2>&1; then
        AGENT_OWNED=1
    fi
    # With --json the refusal is the JSON error object on stdout; without it,
    # the same text is on stderr. Read both.
    if (( ! AGENT_OWNED )) || ! cat "$WORKDIR/deploy1.json" "$WORKDIR/deploy1.err" \
        | grep -qF "$ROUTE"; then
        tail -n 40 "$WORKDIR/deploy1.json" "$WORKDIR/deploy1.err" >&2
        fail "first deploy failed for a reason other than the unbound route $ROUTE"
    fi
fi

echo "=== bind route $ROUTE -> $CHANNEL, approvers users:$APPROVER ==="
approvals_cli --route-resolution "$ROUTE=$CHANNEL" \
    --route-approvers "$ROUTE=users:$APPROVER" >/dev/null
echo "=== second deploy: must succeed ==="
deploy_agent >"$WORKDIR/deploy2.json" 2>"$WORKDIR/deploy2.err" || {
    show_cli_failure "$WORKDIR/deploy2.json" "$WORKDIR/deploy2.err"
    fail "deploy with the bound route failed"
}

echo "=== mint operator principal for $APPROVER (token never printed) ==="
approvals_cli --mint-operator-principal "$APPROVER" >"$TOKEN_FILE.mint" 2>"$WORKDIR/mint.err" || {
    # On failure the mint's stdout is the error object, which holds no token.
    show_cli_failure "$TOKEN_FILE.mint" "$WORKDIR/mint.err"
    fail "operator principal mint failed"
}
python3 - "$TOKEN_FILE.mint" "$TOKEN_FILE" <<'PY'
import json, os, pathlib, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
token = json.loads(raw.splitlines()[-1]).get("operator_principal", {}).get("token")
if not isinstance(token, str) or not token:
    raise SystemExit("error: operator principal response omitted its token")
fd = os.open(sys.argv[2], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as fh:
    fh.write(token)
PY
rm -f -- "$TOKEN_FILE.mint"
TOKEN="$(<"$TOKEN_FILE")"
[[ -n "$TOKEN" ]] || fail "operator principal token is empty"

echo "=== start the original listener and wait for the gated Bash approval ==="
BEFORE_IDS="$(pending_rows | cut -f1 | sort)" || fail "could not list pending approvals before the turn"
curie_cluster message --namespace "$NAMESPACE" --release "$RELEASE" \
    --channel "$CHANNEL" --listen-host "$CURIE_E2E_LISTEN_HOST" \
    --timeout-secs "$LISTEN_TIMEOUT_SECONDS" "run echo $MARKER" \
    >"$LISTENER_OUT" 2>"$LISTENER_ERR" &
LISTENER_PID=$!

started=$SECONDS
NEW_ROWS=""
while (( SECONDS - started < APPROVAL_APPEAR_BUDGET_SECONDS )); do
    if rows="$(pending_rows)"; then
        NEW_ROWS="$(python3 - "$BEFORE_IDS" "$rows" "$ROUTE" <<'PY'
import sys
before = set(sys.argv[1].split())
route = sys.argv[3]
for line in sys.argv[2].splitlines():
    parts = line.split("\t")
    if len(parts) == 3 and parts[0] not in before and parts[1] == route:
        print(line)
PY
)"
        [[ -n "$NEW_ROWS" ]] && break
    fi
    if ! kill -0 "$LISTENER_PID" 2>/dev/null; then
        dump_diagnostics
        fail "listener exited before the gated Bash call raised an approval"
    fi
    sleep 2
done
[[ -n "$NEW_ROWS" ]] || {
    dump_diagnostics
    fail "no pending approval on route $ROUTE within ${APPROVAL_APPEAR_BUDGET_SECONDS}s"
}
[[ "$(printf '%s\n' "$NEW_ROWS" | wc -l | tr -d ' ')" == "1" ]] \
    || fail "expected exactly one new pending approval on $ROUTE, got: $NEW_ROWS"
IFS=$'\t' read -r APPROVAL_ID _route GRANTED_TOOL <<<"$NEW_ROWS"
[[ "$GRANTED_TOOL" == "Bash" ]] \
    || fail "pending approval $APPROVAL_ID gates '$GRANTED_TOOL', not Bash"
echo "approval $APPROVAL_ID pending on $ROUTE for Bash"

echo "=== restart worker pod(s) ==="
WORKER_BEFORE="$(pods_by_selector_of deployment "$WORKER_DEPLOYMENT")"
kube delete pod -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker" \
    --wait=true --timeout=120s >/dev/null
kube rollout status "deployment/$WORKER_DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
assert_replaced "worker" deployment "$WORKER_DEPLOYMENT" "$WORKER_BEFORE"

echo "=== restart statefulset/$POSTGRES_STATEFULSET ==="
POSTGRES_BEFORE="$(pods_by_selector_of statefulset "$POSTGRES_STATEFULSET")"
kube rollout restart "statefulset/$POSTGRES_STATEFULSET" >/dev/null
kube rollout status "statefulset/$POSTGRES_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
assert_replaced "$POSTGRES_STATEFULSET" statefulset "$POSTGRES_STATEFULSET" "$POSTGRES_BEFORE"

echo "=== stop statefulset/$VALKEY_STATEFULSET (resolve happens inside this window) ==="
VALKEY_ORIGINAL_REPLICAS="$(kube get statefulset "$VALKEY_STATEFULSET" -o jsonpath='{.spec.replicas}')"
[[ "$VALKEY_ORIGINAL_REPLICAS" =~ ^[1-9][0-9]*$ ]] \
    || fail "statefulset/$VALKEY_STATEFULSET has spec.replicas '$VALKEY_ORIGINAL_REPLICAS'; expected a running Valkey"
VALKEY_BEFORE="$(pods_by_selector_of statefulset "$VALKEY_STATEFULSET")"
VALKEY_STOPPED=1
kube scale "statefulset/$VALKEY_STATEFULSET" --replicas=0 >/dev/null
started=$SECONDS
while [[ -n "$(pods_by_selector_of statefulset "$VALKEY_STATEFULSET")" ]]; do
    (( SECONDS - started < VALKEY_STOP_BUDGET_SECONDS )) \
        || fail "Valkey pods still present ${VALKEY_STOP_BUDGET_SECONDS}s after scaling to 0"
    sleep 1
done
echo "Valkey stopped (was $VALKEY_ORIGINAL_REPLICAS replica(s))"

echo "=== approval $APPROVAL_ID must still be pending after the restarts ==="
started=$SECONDS
still_pending=0
while (( SECONDS - started < STILL_PENDING_BUDGET_SECONDS )); do
    # The list can fail transiently while the stores come back; retry.
    if rows="$(pending_rows)" && printf '%s\n' "$rows" | cut -f1 | grep -Fxq "$APPROVAL_ID"; then
        still_pending=1
        break
    fi
    sleep 3
done
(( still_pending )) || {
    dump_diagnostics
    fail "approval $APPROVAL_ID is not pending ${STILL_PENDING_BUDGET_SECONDS}s after the restarts"
}

echo "=== resolve $APPROVAL_ID as operator $APPROVER ==="
started=$SECONDS
while true; do
    if CURIE_APPROVAL_PRINCIPAL_TOKEN="$TOKEN" approvals_cli --resolve "$APPROVAL_ID" \
        >"$WORKDIR/resolve.json" 2>"$WORKDIR/resolve.err"; then
        status="$(python3 - "$WORKDIR/resolve.json" <<'PY'
import json, pathlib, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
print(json.loads(raw.splitlines()[-1]).get("resolved", {}).get("status", ""))
PY
)"
        [[ "$status" == "approved" ]] || fail "resolve returned status '$status', not approved"
        break
    fi
    http="$(cli_http_status "$WORKDIR/resolve.json" "$WORKDIR/resolve.err")"
    if [[ "$http" == "409" ]]; then
        # Already resolved, e.g. an earlier ambiguous attempt landed. Counts
        # only if the row really is approved.
        [[ "$(approval_status "$APPROVAL_ID")" == "approved" ]] || {
            show_cli_failure "$WORKDIR/resolve.json" "$WORKDIR/resolve.err"
            fail "resolve answered 409 but approval $APPROVAL_ID is not approved"
        }
        break
    fi
    if [[ -n "$http" && "$http" != 5* ]]; then
        show_cli_failure "$WORKDIR/resolve.json" "$WORKDIR/resolve.err"
        fail "resolve refused with HTTP $http"
    fi
    if (( SECONDS - started >= RESOLVE_BUDGET_SECONDS )); then
        show_cli_failure "$WORKDIR/resolve.json" "$WORKDIR/resolve.err"
        fail "resolve kept failing on transport/5xx for ${RESOLVE_BUDGET_SECONDS}s"
    fi
    echo "resolve attempt failed (${http:-transport}); retrying"
    sleep 5
done
RESOLVED_AT=$SECONDS
APPROVAL_RESOLVED=1
echo "approval $APPROVAL_ID approved while Valkey was stopped"

echo "=== start statefulset/$VALKEY_STATEFULSET again ==="
start_valkey
assert_replaced "$VALKEY_STATEFULSET" statefulset "$VALKEY_STATEFULSET" "$VALKEY_BEFORE"

echo "=== inline enqueue failed during the Valkey stop ==="
# approvals.py logs "approval <id> <status> by <actor>; resume enqueue failed,
# reconciler will retry" when the inline XADD raises. Read every api pod
# (current and previous container) so a multi-replica or restarted api is
# still covered.
inline_failed=0
while read -r api_pod; do
    [[ -n "$api_pod" ]] || continue
    for prev in "" "--previous"; do
        # Drain the log stream so an early match cannot cause a SIGPIPE false refusal.
        if kube logs "$api_pod" -c api --since-time="$SCRIPT_STARTED_RFC3339" $prev 2>/dev/null \
            | grep -F "approval $APPROVAL_ID " | grep -F "resume enqueue failed" >/dev/null; then
            inline_failed=1
        fi
    done
done < <(pods_by_selector_of deployment "$API_DEPLOYMENT" | cut -f1)
(( inline_failed )) || {
    dump_diagnostics
    fail "scenario did not exercise the failed inline enqueue (Valkey was reachable at resolve time)"
}
echo "API logged the failed inline resume enqueue for $APPROVAL_ID"

echo "=== AC1: original listener finalizes within ${RESUME_BOUND_SECONDS}s of the resolve ==="
listener_status=0
wait_for_pid "$LISTENER_PID" "$RESUME_BOUND_SECONDS" || listener_status=$?
RESUME_SECONDS=$((SECONDS - RESOLVED_AT))
if (( listener_status == 124 )); then
    dump_diagnostics
    fail "AC1: original listener did not exit within ${RESUME_BOUND_SECONDS}s of the resolve"
fi
LISTENER_PID=""
if (( listener_status != 0 )); then
    dump_diagnostics
    fail "AC1: original listener exited $listener_status, not 0"
fi
THREAD="$(python3 - "$LISTENER_OUT" <<'PY'
import json, pathlib, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
try:
    doc = json.loads(raw.splitlines()[-1])
except Exception as exc:
    raise SystemExit(f"error: AC1: unparseable listener JSON: {exc}: {raw[:500]}")
if doc.get("finalized") is not True:
    raise SystemExit(f"error: AC1: listener output is not finalized: {doc}")
thread = doc.get("thread")
if not isinstance(thread, str) or not thread:
    raise SystemExit(f"error: AC1: listener output names no thread: {doc}")
print(thread)
PY
)" || { dump_diagnostics; exit 1; }
(( RESUME_SECONDS <= RESUME_BOUND_SECONDS )) \
    || fail "AC1: listener finalized ${RESUME_SECONDS}s after the resolve, above ${RESUME_BOUND_SECONDS}s"
echo "AC1 pass: listener exit 0, finalized, ${RESUME_SECONDS}s after the resolve (thread $THREAD)"

echo "=== AC2a: exactly one Bash execution for thread $THREAD ==="
runner_bash_count() {
    local pod total=0 n logfile="$WORKDIR/runner.log"
    while read -r pod; do
        [[ -n "$pod" ]] || continue
        # A pod that is still pending or already gone has no readable log.
        kube logs "$pod" -c runner >"$logfile" 2>/dev/null || continue
        n="$(python3 - "$CHANNEL:$THREAD" "$logfile" <<'PY'
import re, sys
needle = re.compile(r"tool call session=\S*" + re.escape(sys.argv[1]) + r" tool=Bash\b")
with open(sys.argv[2], errors="replace") as fh:
    print(sum(1 for line in fh if needle.search(line)))
PY
)"
        total=$((total + n))
    done < <(kube get pods -o json | python3 -c '
import json, sys
for pod in json.load(sys.stdin).get("items", []):
    if any(c.get("name") == "runner" for c in pod.get("spec", {}).get("containers", [])):
        print(pod["metadata"]["name"])
')
    printf '%s\n' "$total"
}
started=$SECONDS
BASH_COUNT=0
while (( SECONDS - started < RUNNER_LOG_BUDGET_SECONDS )); do
    BASH_COUNT="$(runner_bash_count)"
    (( BASH_COUNT >= 1 )) && break
    sleep 3
done
# One more read after any first sighting so a late duplicate is counted too.
sleep 3
BASH_COUNT="$(runner_bash_count)"
[[ "$BASH_COUNT" == "1" ]] \
    || fail "AC2: expected exactly 1 runner 'tool call ... tool=Bash' line for thread $THREAD, found $BASH_COUNT"
echo "AC2a pass: one Bash execution for the approved call"

echo "=== AC2b: audit records the operator resolution ==="
api_get_in_cluster "/approvals/$APPROVAL_ID/audit" >"$WORKDIR/audit.json"
python3 - "$WORKDIR/audit.json" "$APPROVER" <<'PY' || { dump_diagnostics; exit 1; }
import json, pathlib, sys
doc = json.loads(pathlib.Path(sys.argv[1]).read_text())
rows = doc if isinstance(doc, list) else []
if not rows:
    raise SystemExit("error: AC2: approval audit is empty or unreadable: " + json.dumps(doc)[:400])
hits = [r for r in rows if r.get("action") == "resolved"
        and r.get("principal_kind") == "operator" and r.get("actor") == sys.argv[2]]
if not hits:
    raise SystemExit("error: AC2: no audit row action=resolved principal_kind=operator actor="
                     + sys.argv[2] + ": " + json.dumps(
                         [{k: r.get(k) for k in ("action", "principal_kind", "actor")} for r in rows]))
print("AC2b pass: audit row resolved by operator " + sys.argv[2])
PY

echo "=== release workloads rolled out and Ready ==="
assert_release_healthy
echo "#4016 APPROVAL RESUME ACROSS WORKER/POSTGRES/VALKEY RESTARTS PASS"
