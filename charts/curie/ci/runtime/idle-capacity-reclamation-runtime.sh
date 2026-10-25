#!/usr/bin/env bash
# #4368: completed retained routes free quota capacity without losing history.
# fix-pin-provision: candidate-stack
# The caller supplies the candidate CLI, context, run ID, and loaded images.
# Each Fix pin phase provisions its own product before invoking this scenario.
# This scenario owns only its release and the resources installed by it.
# Prerequisites: kubectl, Helm, Python 3 with PyYAML, setsid, and network access
# to the chart backing-service images.
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }
for variable in CURIE_BIN CURIE_SCENARIO_CONTEXT CURIE_SCENARIO_RUN_ID \
    CURIE_SCENARIO_API_IMAGE CURIE_SCENARIO_WORKER_IMAGE CURIE_SCENARIO_RUNNER_IMAGE; do
    [[ -n "${!variable:-}" ]] || fail "$variable is required"
done
[[ -f "$CURIE_BIN" && -x "$CURIE_BIN" ]] || fail "CURIE_BIN must name an executable curie binary"
command -v python3 >/dev/null || fail "required executable is unavailable: python3"
# Validate all inputs, including context existence, before invoking any
# infrastructure tool. Read the same kubeconfig files the caller's tools use.
python3 - <<'PY'
import os
import pathlib
import re
import sys


def fail(variable, reason):
    print(f"FAIL: {variable} {reason}", file=sys.stderr)
    raise SystemExit(1)


context = os.environ["CURIE_SCENARIO_CONTEXT"]
if re.search(r"[\s\x00-\x1f\x7f]", context) or context.startswith("-"):
    fail("CURIE_SCENARIO_CONTEXT", "must name one kubeconfig context")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,24}", os.environ["CURIE_SCENARIO_RUN_ID"]):
    fail("CURIE_SCENARIO_RUN_ID", "must match ^[a-z0-9][a-z0-9-]{0,24}$")
component = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
host_component = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
host = rf"(?:{host_component}(?:\.{host_component})*|\[[A-Fa-f0-9:]+\])(?::[0-9]+)?"
repository = rf"(?:{host}/)?{component}(?:/{component})*"
for variable in ("CURIE_SCENARIO_API_IMAGE", "CURIE_SCENARIO_WORKER_IMAGE", "CURIE_SCENARIO_RUNNER_IMAGE"):
    image = os.environ[variable]
    name, separator, tag = image.rpartition(":")
    if not separator or not re.fullmatch(repository, name) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        fail(variable, "must be a repository:tag image reference")
try:
    import yaml
except ImportError:
    fail("CURIE_SCENARIO_CONTEXT", "validation requires Python 3 with PyYAML")
kubeconfig = os.environ.get("KUBECONFIG") or str(pathlib.Path.home() / ".kube/config")
contexts = {}
for filename in dict.fromkeys(path for path in kubeconfig.split(os.pathsep) if path):
    path = pathlib.Path(filename)
    if not path.exists():
        continue
    try:
        config = yaml.safe_load(path.read_text()) or {}
        entries = config.get("contexts", [])
        if not isinstance(entries, list):
            fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig contexts")
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig contexts")
            contexts.setdefault(entry["name"], entry.get("context"))
    except (OSError, ValueError, AttributeError, yaml.YAMLError):
        fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig")
if not isinstance(contexts.get(context), dict):
    fail("CURIE_SCENARIO_CONTEXT", "is absent from the caller's kubeconfig")
PY
for command in kubectl helm setsid; do
    command -v "$command" >/dev/null || fail "required executable is unavailable: $command"
done
BIN="$(python3 -c 'import os; print(os.path.abspath(os.environ["CURIE_BIN"]))')"
RUN_ID="$CURIE_SCENARIO_RUN_ID"
CONTEXT="$CURIE_SCENARIO_CONTEXT"
NAMESPACE="test-4368-idle-capacity-$CURIE_SCENARIO_RUN_ID"
RELEASE="idlecap-$CURIE_SCENARIO_RUN_ID"
FULLNAME="$RELEASE-curie"
PUBLICATION_NAMESPACE="${NAMESPACE}-${FULLNAME}-publication"
PUBLICATION_NAMESPACE="${PUBLICATION_NAMESPACE:0:63}"
PUBLICATION_NAMESPACE="${PUBLICATION_NAMESPACE%-}"
AGENT="acme-capacity-bot"
INSTALL_STARTED=0
DRIVER_PID=""
# Never forward the invoking shell's model, GitHub, Slack, or API credentials.
# The fresh release uses only the published development defaults.
unset CURIE_CREDENTIALS ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL
unset CURIE_GITHUB_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_BOT_TOKEN SLACK_APP_TOKEN
unset CURIE_API_URL CURIE_API_KEY CURIE_VALKEY_PASSWORD CURIE_NAMESPACE CURIE_STREAM
unset CURIE_MODEL_BASE_URL CURIE_FAKE_MODEL CURIE_FAKE_SCRIPT CURIE_FAKE_SCRIPT_FILE

kc() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
CLUSTER_OBJECTS=(
    namespace/agent-sandbox-system
    clusterrole/agent-sandbox-controller
    clusterrole/agent-sandbox-controller-extensions
    clusterrole/agent-sandbox-controller-networkpolicies-read
    clusterrolebinding/agent-sandbox-controller
    clusterrolebinding/agent-sandbox-controller-extensions
    clusterrolebinding/agent-sandbox-controller-networkpolicies-read
    priorityclass/curie-platform
    priorityclass/curie-sandbox
)
# An occupied scenario namespace would make its release and data unowned too.
for object in "namespace/$NAMESPACE" "${CLUSTER_OBJECTS[@]}"; do
    existing="$(kc get "$object" --ignore-not-found -o name)" || fail "could not inventory $object"
    [[ -z "$existing" ]] || fail "refusing to install while $object exists"
done
ABSENT_CRDS=()
for crd in sandboxes.agents.x-k8s.io sandboxclaims.extensions.agents.x-k8s.io \
    sandboxtemplates.extensions.agents.x-k8s.io sandboxwarmpools.extensions.agents.x-k8s.io; do
    existing="$(kc get "crd/$crd" --ignore-not-found -o name)" || fail "could not inventory CRD $crd"
    [[ -n "$existing" ]] || ABSENT_CRDS+=("$crd")
done
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-idle-capacity-"))')"
export CURIE_CONFIG_DIR="$WORKDIR/curie-config"
cleanup() {
    local status=$? cleanup_status=0 remaining object crd
    trap - EXIT INT TERM
    set +e
    if [[ -n "$DRIVER_PID" ]]; then
        kill -TERM -- "-$DRIVER_PID" 2>/dev/null || true
        sleep 1
        kill -KILL -- "-$DRIVER_PID" 2>/dev/null || true
        wait "$DRIVER_PID" 2>/dev/null || true
    fi
    if (( status != 0 )); then
        if (( INSTALL_STARTED )); then
            for component in worker api; do
                kc logs "deployment/$FULLNAME-$component" --tail=250 --request-timeout=10s \
                    >"$WORKDIR/failure-$component.log" 2>&1 || true
            done
        fi
        if ! python3 - "$WORKDIR" "$REPO_ROOT" "$RUN_ID" <<'PY_DIAGNOSTIC'
import getpass, os, pathlib, re, sys
source = pathlib.Path(sys.argv[1])
slug = re.sub(r"[^A-Za-z0-9_.-]", "-", pathlib.Path(sys.argv[2]).name)
destination = pathlib.Path("/tmp") / getpass.getuser() / slug / "idle-capacity-reclamation" / sys.argv[3]
destination.mkdir(parents=True, mode=0o700, exist_ok=False)
os.chmod(destination, 0o700)
for pattern in ("*.log", "*.json"):
    for evidence in source.glob(pattern):
        if evidence.is_file() and not evidence.is_symlink():
            target = destination / evidence.name
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(evidence.read_bytes()[-65536:])
print("OK: bounded failure diagnostics retained privately")
PY_DIAGNOSTIC
        then
            cleanup_status=1
        fi
    fi
    if (( INSTALL_STARTED )); then
        # Let the live controller release finalizers before Helm stops it.
        kc delete sandboxclaims --all --ignore-not-found --wait=true --timeout=90s \
            >"$WORKDIR/delete-claims.log" 2>&1 \
            || { echo "FAIL: could not delete owned sandbox claims" >&2; cleanup_status=1; }
        kc delete sandboxes --all --ignore-not-found --wait=true --timeout=90s \
            >"$WORKDIR/delete-sandboxes.log" 2>&1 \
            || { echo "FAIL: could not delete owned sandboxes" >&2; cleanup_status=1; }
        helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NAMESPACE" \
            --ignore-not-found --wait --timeout 60s >"$WORKDIR/uninstall.log" 2>&1 \
            || { echo "FAIL: could not uninstall the owned release" >&2; cleanup_status=1; }
        # Drain owned pods before namespace deletion so the namespace
        # controller does not defer reconciliation for their termination grace.
        kc delete pods --all --ignore-not-found --wait=true --timeout=60s >"$WORKDIR/delete-pods.log" 2>&1 \
            || { echo "FAIL: could not drain pods in the owned namespace" >&2; cleanup_status=1; }
        kc delete namespace "$NAMESPACE" --ignore-not-found --wait=false \
            >"$WORKDIR/delete-namespace.log" 2>&1 \
            || { echo "FAIL: could not delete the owned namespace" >&2; cleanup_status=1; }
        kc wait --for=delete "namespace/$NAMESPACE" --timeout=180s >"$WORKDIR/wait-namespace.log" 2>&1 \
            || { echo "FAIL: owned namespace deletion did not complete" >&2; cleanup_status=1; }
        kc wait --for=delete namespace/agent-sandbox-system --timeout=180s >"$WORKDIR/wait-controller.log" 2>&1 \
            || { echo "FAIL: controller namespace deletion did not complete" >&2; cleanup_status=1; }
        kc wait --for=delete "namespace/$PUBLICATION_NAMESPACE" --timeout=180s >"$WORKDIR/wait-publication.log" 2>&1 \
            || { echo "FAIL: publication namespace deletion did not complete" >&2; cleanup_status=1; }
        for crd in "${ABSENT_CRDS[@]}"; do
            kc delete "crd/$crd" --ignore-not-found --wait=false >"$WORKDIR/delete-crd.log" 2>&1 \
                || { echo "FAIL: could not delete owned CRD $crd" >&2; cleanup_status=1; }
            kc wait --for=delete "crd/$crd" --timeout=60s >"$WORKDIR/wait-crd.log" 2>&1 \
                || { echo "FAIL: owned CRD $crd deletion did not complete" >&2; cleanup_status=1; }
        done
        for object in "namespace/$NAMESPACE" "namespace/$PUBLICATION_NAMESPACE" "${CLUSTER_OBJECTS[@]}" \
            "validatingadmissionpolicy/$FULLNAME-runner-resources" \
            "validatingadmissionpolicybinding/$FULLNAME-runner-resources" "${ABSENT_CRDS[@]/#/crd/}"; do
            if ! remaining="$(kc get "$object" --ignore-not-found -o name 2>"$WORKDIR/verify-objects.log")"; then
                echo "FAIL: could not verify teardown of $object" >&2
                cleanup_status=1
            elif [[ -n "$remaining" ]]; then
                echo "FAIL: owned object $object remains" >&2
                cleanup_status=1
            fi
        done
        if ! remaining="$(helm --kube-context "$CONTEXT" list -n "$NAMESPACE" --all --short \
            --filter "^$RELEASE$" 2>"$WORKDIR/verify-release.log")"; then
            echo "FAIL: could not verify release teardown" >&2
            cleanup_status=1
        elif [[ -n "$remaining" ]]; then
            echo "FAIL: owned release remains" >&2
            cleanup_status=1
        fi
    fi
    rm -rf -- "$WORKDIR" || cleanup_status=1
    if (( cleanup_status )); then
        exit 1
    fi
    echo "OK: quota scenario release, namespaces, and cluster-scoped objects are gone"
    if (( status == 0 )); then
        echo "PASS: clean and stale-route six-turn bursts finalized under quota two; reclaimed history resumed"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Keep normal route retention and claim deadlines; only sandbox pod quota is reduced.
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set "api.image.repository=${CURIE_SCENARIO_API_IMAGE%:*}" \
    --set "api.image.tag=${CURIE_SCENARIO_API_IMAGE##*:}" --set api.image.pullPolicy=Never
    --set "worker.image.repository=${CURIE_SCENARIO_WORKER_IMAGE%:*}" \
    --set "worker.image.tag=${CURIE_SCENARIO_WORKER_IMAGE##*:}" --set worker.image.pullPolicy=Never
    --set "agentSandbox.runner.image=${CURIE_SCENARIO_RUNNER_IMAGE%:*}" \
    --set "agentSandbox.runner.tag=${CURIE_SCENARIO_RUNNER_IMAGE##*:}"
    --set agentSandbox.runner.imagePullPolicy=Never --set agentSandbox.runner.prewarm.imagePullPolicy=Never
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=true
    --set agentSandbox.platformPool.replicas=0 --set agentSandbox.runner.fakeModel=true
    --set worker.replicas=1 --set resourceQuota.hard.sandboxPodCount=2
    --set security.gvisor.mode=off
    --set langfuse.deploy=false --set langfuse.host=langfuse.example.com
    --set clickhouse.deploy=false --set otelCollector.deploy=false --set otelCollector.telemetryDisabled=true
    --set ui.deploy=false --set dispatcher.deploy=false --set mailAdapter.deploy=false
)
# Keep the caller's working directory so relative KUBECONFIG paths retain
# their meaning. Register ownership before the CLI can partially install.
INSTALL_STARTED=1
"$BIN" cluster --context "$CONTEXT" up --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --dev --fake-model --no-expose "${sets[@]}" \
    >"$WORKDIR/install.log" 2>&1 || fail "candidate cluster install failed"
for component in api worker; do
    image_variable="CURIE_SCENARIO_${component^^}_IMAGE"
    kc rollout status "deployment/$FULLNAME-$component" --timeout=300s >/dev/null
    [[ "$(kc get "deployment/$FULLNAME-$component" -o jsonpath='{.spec.template.spec.containers[0].image}')" == "${!image_variable}" ]] \
        || fail "the $component Deployment is not using the candidate image"
done
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_CLAIM_TIMEOUT_SECONDS")].value}')" == 90 ]] \
    || fail "worker claim timeout must retain its normal 90-second value"
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_ROUTE_TTL_SECONDS")].value}')" == 3600 ]] \
    || fail "worker route retention must retain its normal 3600-second value"
"$BIN" init "$AGENT" --dir "$WORKDIR/bundle" >"$WORKDIR/init.log" 2>&1
"$BIN" --json cluster --context "$CONTEXT" deploy --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --plugin-dir "$WORKDIR/bundle" --agent "$AGENT" \
    --slack-channel C0EXAMPLE1 >"$WORKDIR/deploy.json" 2>"$WORKDIR/deploy.log" \
    || fail "candidate agent deployment failed"
python3 - "$WORKDIR/deploy.json" <<'PY'
import json, pathlib, sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload["deployment"]["status"] == "active", "candidate deployment is not active"
PY
kc rollout status "deployment/$FULLNAME-worker" --timeout=300s >/dev/null

# The driver captures credentials and arbitrary tool output privately. Only
# fixed assertions and numeric observations can reach the public test log.
cat >"$WORKDIR/driver.py" <<'PY'
import calendar
from datetime import datetime
import json
import os
import pathlib
import subprocess
import sys
import time

root, binary, context, namespace, release, fullname, agent, chart = sys.argv[1:]
root = pathlib.Path(root)
sys.path.insert(0, str(pathlib.Path(chart) / "ci" / "runtime"))
from quota_burst_diagnostics import emit_summary

kube = ["kubectl", "--context", context, "-n", namespace]
message = [binary, "--json", "cluster", "--context", context, "message", "--namespace", namespace,
           "--release", release, "--chart", chart, "--agent", agent, "--channel", "C0EXAMPLE1", "--timeout-secs", "300"]
children = []


def run(args, *, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    verb = " ".join(args[5:8]) if len(args) > 5 else "command"
    detail = ""
    for line in reversed((result.stderr or "").splitlines()):
        text = line.strip()
        if not text:
            continue
        cleaned = []
        for character in text:
            cleaned.append(character if character.isalnum() or character in " _.:=" else " ")
        detail = " ".join("".join(cleaned).split())[:160]
        break
    assert result.returncode == 0, (
        "an owned runtime observation or setup command failed: "
        + verb + " exit " + str(result.returncode)
        + ((" detail " + detail) if detail else "")
    )
    return result.stdout


def get(kind, *args):
    return json.loads(run([*kube, "get", kind, *args, "-o", "json"]))


# Probe the real ownership store and authenticated runner status from inside
# the worker's network. Tokens never leave captured, private subprocess output.
probe_code = r'''
import concurrent.futures, json, urllib.request
import redis
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.types import RouteRecord
config = WorkerConfig()
client = redis.Redis(**{**config.valkey_client_kwargs(), "decode_responses": False,
                       "socket_timeout": 5, "socket_connect_timeout": 5})
routes = []
for key in client.scan_iter(match="curie:sandbox:route:*"):
    raw = client.get(key)
    if raw is not None:
        record = RouteRecord.from_json(raw.decode())
        routes.append({"key": key.decode(), "raw": raw.decode(), "record": json.loads(raw),
                       "expires": client.pexpiretime(key), "status": None})
def status(route):
    handle = RouteRecord.from_json(route["raw"]).handle
    request = urllib.request.Request(handle.base_url + "/v1/status",
                                    headers={"Authorization": "Bearer " + handle.token})
    try:
        with urllib.request.urlopen(request, timeout=0.6) as response:
            route["status"] = json.load(response)
    except Exception:
        pass
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
    list(pool.map(status, routes))
waits = []
for key in client.scan_iter(match=config.key_prefix + ":capacity_wait:record:*"):
    fields = client.hgetall(key)
    if fields:
        waits.append({"first_wait_ms": int(fields.get(b"first_wait_ms", 0)),
                      "state": fields.get(b"state", b"").decode()})
print(json.dumps({"routes": routes, "waits": waits}))
'''


def probe():
    return json.loads(run([*kube, "exec", "deployment/" + fullname + "-worker", "--", "python3", "-c", probe_code]))


def counts_against_quota(pod, now):
    # Kubernetes v1.31 QuotaV1Pod also excludes pods past their deletion grace.
    # https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/quota/v1/evaluator/core/pods.go
    if pod.get("status", {}).get("phase") in {"Failed", "Succeeded"}:
        return False
    metadata = pod["metadata"]
    deleted = metadata.get("deletionTimestamp")
    grace = metadata.get("deletionGracePeriodSeconds")
    if deleted is not None and grace is not None:
        if now > datetime.fromisoformat(deleted).timestamp() + grace:
            return False
    return True


def observe():
    quota = get("resourcequota", fullname + "-sandbox-quota")
    assert quota["spec"]["hard"]["pods"] == "2", "sandbox pod quota changed from two"
    quota_used = int(quota.get("status", {}).get("used", {}).get("pods", 0))
    assert quota_used <= 2, "observed quota usage exceeded two"
    priority = quota["spec"]["scopeSelector"]["matchExpressions"][0]["values"][0]
    pods = {pod["metadata"]["uid"]: pod for pod in get("pods")["items"]
            if pod["spec"].get("priorityClassName") == priority}
    # Preserve every observed UID below for reclamation and active-route checks.
    now = time.time()
    chargeable = [pod for pod in pods.values() if counts_against_quota(pod, now)]
    if len(chargeable) > 2:
        counts = {phase: 0 for phase in ("Pending", "Running", "Succeeded", "Failed", "Unknown", "other")}
        for pod in pods.values():
            phase = pod.get("status", {}).get("phase")
            counts[phase if phase in counts else "other"] += 1
        print("Quota pod diagnostic: " + json.dumps({"chargeable": len(chargeable),
              "total": len(pods), "quota_used": quota_used, "phase_counts": counts}, sort_keys=True), file=sys.stderr)
    assert len(chargeable) <= 2, "observed more than two chargeable sandbox pods"
    return pods, probe()


def thread():
    stamp = time.time_ns()
    return f"{stamp // 10**9}.{stamp % 10**9 // 1000:06d}"


def start(name, text, thread_id=None):
    thread_id = thread_id or thread()
    output = root / (name + ".json")
    error = root / (name + ".log")
    with output.open("w") as stdout, error.open("w") as stderr:
        process = subprocess.Popen([*message, "--thread", thread_id, text], stdout=stdout, stderr=stderr)
    turn = {"process": process, "thread": thread_id, "text": text, "output": output,
            "started": time.monotonic(), "finished": None, "name": name, "error": error}
    children.append(process)
    return turn


def finished(turn):
    if turn["finished"] is not None:
        return True
    if turn["process"].poll() is None:
        return False
    turn["finished"] = time.monotonic()
    try:
        payload = json.loads(turn["output"].read_text())
    except (OSError, ValueError):
        payload = None
    successful = (turn["process"].returncode == 0 and isinstance(payload, dict)
                  and payload.get("finalized") is True and payload.get("timed_out") is not True
                  and payload.get("failed") is not True and isinstance(payload.get("reply"), str)
                  and bool(payload["reply"].strip()))
    if not successful:
        diagnostic = {"phase": turn["name"], "cli_exit": turn["process"].returncode,
                      "elapsed_seconds": round(turn["finished"] - turn["started"], 2),
                      "json_object": isinstance(payload, dict)}
        if isinstance(payload, dict):
            for field in ("finalized", "timed_out", "failed"):
                value = payload.get(field)
                diagnostic[field] = value if isinstance(value, bool) else None
            classes = {"workspace-error", "runner-error", "runner-timeout", "budget-exceeded",
                       "model-credential-rejected", "history-persistence-error", "agent-busy",
                       "sandbox-capacity", "capacity-wait-expired", "delivery-exhausted"}
            failure_class = payload.get("failure_class")
            diagnostic["failure_class"] = failure_class if isinstance(failure_class, str) and failure_class in classes else "unrecognized-or-absent"
            diagnostic["reply_present"] = isinstance(payload.get("reply"), str) and bool(payload["reply"].strip())
            diagnostic["error_present"] = isinstance(payload.get("error"), str)
        error_text = turn["error"].read_text(errors="replace").lower()
        # Fixed categories expose the failing boundary without echoing a
        # message, hostname, credential, identifier, or arbitrary reply.
        diagnostic["stderr_categories"] = [category for category, needle in (
            ("chart-resolution", "chart"), ("http-error", "http"),
            ("agent-listing", "listing agents"), ("port-forward", "port-forward"),
            ("connection-refused", "connection refused"), ("unauthorized", "401"),
            ("forbidden", "403"), ("timeout", "timed out"),
            ("argument-error", "unexpected argument"), ("enqueue", "enqueue"),
        ) if needle in error_text]
        print("Listener diagnostic: " + json.dumps(diagnostic, sort_keys=True), file=sys.stderr)
    assert turn["process"].returncode == 0, "a quota-pressure CLI listener failed"
    assert turn["finished"] - turn["started"] < 300, "a listener reached its unchanged 300-second timeout"
    assert isinstance(payload, dict), "a quota-pressure CLI listener produced no JSON object"
    assert payload.get("finalized") is True, "a quota-pressure turn did not finalize"
    assert payload.get("timed_out") is not True, "a quota-pressure listener timed out"
    assert payload.get("failed") is not True, "a quota-pressure turn failed"
    assert isinstance(payload.get("reply"), str) and bool(payload["reply"].strip()), "a quota-pressure turn had no delivered reply"
    return True


def matches(route, turn):
    return route["record"]["thread_key"].endswith(turn["thread"])


def safe(route):
    status = route["status"] or {}
    return (status.get("turn_active") is False and status.get("history_durable") is True
            and status.get("status") in {"done", "idle-awaiting-input"})


def batch(phase, count, *, progress=True, stale=None, expect_pressure=False):
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    phase_started_ms = int(time.time() * 1000)
    turns = [start(f"{phase}-{index}", ("[fake:progress-demo] " if progress else "")
                   + f"held-open capacity probe {phase} {index}") for index in range(count)]
    seen_pods, seen_routes, eligible_claims, active_claims = {}, {}, set(), set()
    parked = False
    deadline = time.monotonic() + 320
    while True:
        completed = [finished(turn) for turn in turns]
        pods, snapshot = observe()
        seen_pods.update(pods)
        for route in snapshot["routes"]:
            seen_routes[route["key"]] = route
            if safe(route):
                eligible_claims.add(route["record"]["claim_name"])
            if (route["status"] or {}).get("turn_active") is True:
                active_claims.add(route["record"]["claim_name"])
        # Retained wait records survive wake, so even a fast pressure repair
        # must demonstrate an actual durable parked event rather than infer it.
        parked |= any(wait["first_wait_ms"] >= phase_started_ms for wait in snapshot["waits"])
        if stale:
            by_key = {route["key"]: route for route in snapshot["routes"]}
            for key, old in stale.items():
                assert key in by_key and by_key[key]["raw"] == old["raw"], "an unreadable old route was deleted or changed"
                assert by_key[key]["status"] is None, "a stale old route unexpectedly became reachable"
        # A previously observed active runner may disappear only after it was
        # observed safely idle or its own real CLI turn had successfully ended.
        # Pods are snapshotted before routes: a just-created active route may
        # legitimately arrive before its Pod has ever entered a snapshot.
        # The Kubernetes substrate addresses the runner Pod by sandbox_name
        # (SandboxSubstrate.pod_termination); retain its first observed UID so
        # a same-name replacement does not conceal an actual disappearance.
        for old in seen_routes.values():
            record = old["record"]
            claim = record["claim_name"]
            observed_uid = next((uid for uid, pod in seen_pods.items()
                                 if pod["metadata"]["name"] == record["sandbox_name"]), None)
            if claim not in active_claims or observed_uid is None or observed_uid in pods:
                continue
            own_turn = next((turn for turn in turns if matches(old, turn)), None)
            # Reply delivery can trail DONE and deletion by a few network hops.
            # Confirm that real completion rather than treating a missed idle
            # polling sample as proof that an active turn was destroyed.
            if claim not in eligible_claims and own_turn is not None:
                reply_deadline = time.monotonic() + 5
                while not finished(own_turn) and time.monotonic() < reply_deadline:
                    time.sleep(0.1)
            own_finished = own_turn is not None and finished(own_turn)
            assert claim in eligible_claims or own_finished, "an observed active runner disappeared before completing"
        if all(completed):
            break
        assert time.monotonic() < deadline, "quota-pressure burst exceeded its bounded runtime"
        time.sleep(0.2)
    if expect_pressure:
        logs = run([*kube, "logs", "deployment/" + fullname + "-worker", "--since-time=" + started])
        assert parked and "capacity wait parked" in logs, "the burst never parked an event for capacity"
        assert "idle route reclamation freed sandbox capacity" in logs, "no actual pressure reclamation was observed"
        assert len(seen_pods) > 2, "quota pressure did not replace retained runner pods"
        assert active_claims, "the burst never observed an active runner"
        print(f"OK: {phase}: {count} successful 300-second listeners, parked events, {len(seen_pods)} runner UIDs, quota <= 2")
    return turns, seen_routes, seen_pods


def clear_owned_sandboxes():
    # This fresh namespace contains only this selector's release and runners.
    # Keep Valkey affinity records intact to reproduce a preceding cleanup phase.
    run([*kube, "delete", "sandboxclaims", "--all", "--wait=true", "--timeout=90s"], timeout=100)
    run([*kube, "delete", "sandboxes", "--all", "--wait=true", "--timeout=90s"], timeout=100)
    deadline = time.monotonic() + 90
    while True:
        pods, _ = observe()
        if not pods:
            break
        assert time.monotonic() < deadline, "owned sandbox pods survived the prior-phase cleanup"
        time.sleep(0.5)


def history_followup(turns, records, pods):
    current = {route["key"] for route in probe()["routes"]}
    reclaimed = next(((turn, route) for turn in turns for route in records.values()
                      if matches(route, turn) and route["key"] not in current), None)
    assert reclaimed is not None, "no completed thread was actually reclaimed"
    old_turn, old_route = reclaimed
    original_uids = {uid for uid, pod in pods.items() if pod["metadata"]["name"] == old_route["record"]["sandbox_name"]}
    followup = start("history-followup", "quota reclamation history followup", old_turn["thread"])
    deadline = time.monotonic() + 320
    while not finished(followup):
        observe()
        assert time.monotonic() < deadline, "reclaimed-thread history followup exceeded its bounded runtime"
        time.sleep(0.2)
    live_pods, snapshot = observe()
    new_route = next(route for route in snapshot["routes"] if matches(route, followup))
    assert new_route["record"]["claim_name"] != old_route["record"]["claim_name"], "followup reused the reclaimed claim"
    pod = next(pod for pod in live_pods.values() if pod["metadata"]["name"] == new_route["record"]["sandbox_name"])
    assert pod["metadata"]["uid"] not in original_uids, "followup reused the reclaimed pod UID"
    code = r'''
import json, os, sys, urllib.request
from aci_protocol import BootEnv
url = os.environ[BootEnv.env_key("history_ref")]
token = os.environ[BootEnv.env_key("history_token")]
request = urllib.request.Request(url, headers={"X-API-Key": token})
with urllib.request.urlopen(request, timeout=10) as response:
    value = json.load(response)["value"]
users = [record.get("user") for record in value if isinstance(record, dict) and "user" in record]
assert len(users) == 2 and users[0] == sys.argv[1] and users[1] == sys.argv[2], "reclaimed-thread transcript lost or duplicated a turn"
'''
    run([*kube, "exec", pod["metadata"]["name"], "--", "python3", "-c", code, old_turn["text"], followup["text"]])
    logs = run([*kube, "logs", pod["metadata"]["name"]])
    assert "history resume cache observed" in logs, "replacement runner did not actually resume prior history"
    assert safe(new_route), "replacement runner did not persist the followup history durably"
    print("OK: a reclaimed completed thread booted a new runner, resumed history, and retained both turns exactly once")


def burst_sixteen():
    # @spec QUOTA-TIMEOUT-DIAGNOSTIC-1/2: observations never update the verdict sample.
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    turns = [start(f"burst16-{index}", "held-open capacity probe burst16 " + str(index))
             for index in range(16)]
    def quota_pods():
        quota = get("resourcequota", fullname + "-sandbox-quota")
        assert quota["spec"]["hard"]["pods"] == "2", "sandbox pod quota changed from two"
        quota_used = int(quota.get("status", {}).get("used", {}).get("pods", 0))
        assert quota_used <= 2, "observed quota usage exceeded two"
        priority = quota["spec"]["scopeSelector"]["matchExpressions"][0]["values"][0]
        now = time.time()
        pods = [pod for pod in get("pods")["items"]
                if pod["spec"].get("priorityClassName") == priority
                and counts_against_quota(pod, now)]
        assert len(pods) <= 2, "observed more than two chargeable sandbox pods"

    deadline = time.monotonic() + 320
    diagnostic_origin = deadline - 320
    prior_completed_count = None
    prior_sample_time = None
    last_observation_seconds = 0
    max_observation_seconds = 0
    try:
        while True:
            completed = [finished(turn) for turn in turns]
            prior_completed_count = sum(completed)
            try:
                prior_sample_time = observation_started = time.monotonic()
            except Exception:
                prior_sample_time = observation_started = None
            try:
                quota_pods()
            finally:
                try:
                    last_observation_seconds = time.monotonic() - observation_started
                    max_observation_seconds = max(max_observation_seconds, last_observation_seconds)
                except Exception:
                    last_observation_seconds = max_observation_seconds = None
            if all(completed):
                break
            assert time.monotonic() < deadline, "sixteen-thread burst exceeded its bounded runtime"
            time.sleep(0.2)
    finally:
        elapsed_seconds = prior_sample_age_seconds = returncodes = None
        try:
            sampled_at = time.monotonic()
            elapsed_seconds = sampled_at - diagnostic_origin
            prior_sample_age_seconds = (None if prior_sample_time is None
                                        else sampled_at - prior_sample_time)
            returncodes = [turn["process"].poll() for turn in turns]
        except Exception:
            returncodes = None
        try:
            emit_summary(
                elapsed_seconds=elapsed_seconds,
                prior_completed_count=prior_completed_count,
                prior_sample_age_seconds=prior_sample_age_seconds,
                last_observation_seconds=last_observation_seconds,
                max_observation_seconds=max_observation_seconds,
                returncodes=returncodes,
            )
        except Exception:
            pass
    last = None
    for _attempt in range(5):
        try:
            observe()
            break
        except AssertionError as error:
            last = error
            if "command failed" not in str(error):
                raise
            time.sleep(2)
    else:
        raise last
    logs = ""
    last = None
    for _attempt in range(5):
        try:
            logs = run([*kube, "logs", "deployment/" + fullname + "-worker", "--since-time=" + started], timeout=60)
            break
        except AssertionError as error:
            last = error
            time.sleep(2)
    else:
        raise last
    parks, wakes = {}, {}
    saw_capacity_text = False
    for line in logs.splitlines():
        if "capacity wait parked" not in line and "capacity wait woke" not in line:
            continue
        saw_capacity_text = True
        start_json = line.find("{")
        end_json = line.rfind("}")
        if start_json < 0 or end_json <= start_json:
            raise AssertionError("malformed capacity wait log")
        try:
            payload = json.loads(line[start_json:end_json + 1])
        except json.JSONDecodeError as error:
            raise AssertionError("malformed capacity wait log") from error
        if not isinstance(payload, dict):
            raise AssertionError("malformed capacity wait log")
        stamp = payload.get("timestamp")
        message = payload.get("message")
        if not isinstance(stamp, str) or not isinstance(message, str) or "T" not in stamp:
            raise AssertionError("malformed capacity wait timestamp")
        parked_line = message.startswith("capacity wait parked event_id=")
        woke_line = message.startswith("capacity wait woke event_id=")
        if parked_line == woke_line:
            raise AssertionError("malformed capacity wait event id")
        event_id = message.split("event_id=", 1)[1].split()[0]
        if not event_id or any(character.isspace() for character in event_id):
            raise AssertionError("malformed capacity wait event id")
        whole, _, fraction = stamp.partition(".")
        try:
            seconds = calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S"))
        except ValueError as error:
            raise AssertionError("malformed capacity wait timestamp") from error
        if fraction:
            digits = ""
            for character in fraction:
                if character.isdigit():
                    digits += character
                else:
                    break
            if not digits:
                raise AssertionError("malformed capacity wait timestamp")
            seconds += int(digits[:6]) / 1_000_000
        bucket = parks if parked_line else wakes
        bucket.setdefault(event_id, []).append(seconds)
    if saw_capacity_text and not parks:
        raise AssertionError("malformed capacity wait log")
    assert parks, "sixteen-thread burst recorded no park"
    unmatched = 0
    longest = 0.0
    for event_id, park_times in parks.items():
        wake_times = wakes.get(event_id, [])
        cursor = 0
        for park_time in park_times:
            while cursor < len(wake_times) and wake_times[cursor] < park_time:
                cursor += 1
            if cursor >= len(wake_times):
                unmatched += 1
                continue
            longest = max(longest, wake_times[cursor] - park_time)
            cursor += 1
    assert unmatched == 0, "a park had no later wake"
    assert longest <= 12, "a park to wake interval exceeded 12 seconds"
    print("parked_turn_burst_16")
    print("finalized_16")
    print("park_observed")
    print("unmatched_parks_0")
    print("max_park_wake_s")


try:
    clean_turns, clean_routes, clean_pods = batch("clean", 6, expect_pressure=True)
    history_followup(clean_turns, clean_routes, clean_pods)
    clear_owned_sandboxes()
    # Two completed routes survived the clean phase. A second real pair and
    # cleanup leave at least four unreadable routes, without raising the quota.
    batch("prior-phase", 2, progress=False)
    clear_owned_sandboxes()
    stale = {route["key"]: route for route in probe()["routes"]}
    assert len(stale) >= 4, "the prior phase did not retain at least four stale affinity records"
    assert all(route["status"] is None for route in stale.values()), "a prior-phase route is still readable"
    batch("safe-idle-prime", 1, progress=False)
    snapshot = probe()["routes"]
    reachable = [route for route in snapshot if route["key"] not in stale and safe(route)]
    assert reachable, "no reachable completed candidate was primed after stale cleanup"
    assert max(route["expires"] for route in stale.values()) < min(route["expires"] for route in reachable), "old unreadable routes do not precede the safe idle candidate"
    batch("stale", 6, stale=stale, expect_pressure=True)
    print("OK: at least four unreadable older routes remained intact ahead of safely reclaimed completed routes")
    clear_owned_sandboxes()
    burst_sixteen()
except AssertionError as error:
    print("FAIL: " + str(error), file=sys.stderr)
    sys.exit(1)
except Exception as error:
    print("FAIL: runtime driver error class " + type(error).__name__, file=sys.stderr)
    sys.exit(1)
finally:
    for process in children:
        if process.poll() is None:
            process.terminate()
    for process in children:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
PY
setsid python3 "$WORKDIR/driver.py" "$WORKDIR" "$BIN" "$CONTEXT" "$NAMESPACE" "$RELEASE" "$FULLNAME" "$AGENT" "$REPO_ROOT/charts/curie" &
DRIVER_PID=$!
driver_status=0
wait "$DRIVER_PID" || driver_status=$?
# Reap only this driver's own process group, including CLI port-forwards.
kill -TERM -- "-$DRIVER_PID" 2>/dev/null || true
sleep 1
kill -KILL -- "-$DRIVER_PID" 2>/dev/null || true
DRIVER_PID=""
[[ "$driver_status" == 0 ]] || fail "idle capacity reclamation runtime assertions failed"
