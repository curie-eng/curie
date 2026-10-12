#!/usr/bin/env bash
# #4368: completed retained routes free quota capacity without losing history.
# Each invocation builds this checkout, owns a fresh kind cluster, and tears it
# down. This also works when verify-fix-pin reverses only the product changes.
# Prerequisites: Docker, kind, kubectl, Helm, Cargo, Python 3, setsid, and network
# access to the pinned build dependencies and chart backing-service images.
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }
for command in docker kind kubectl helm cargo python3 setsid; do
    command -v "$command" >/dev/null || fail "required executable is unavailable: $command"
done
# Use the caller-selected Docker engine for builds, kind, and owned cleanup.
docker info >/dev/null 2>&1 || fail "Docker is unavailable"

RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
CLUSTER="acme-idle-capacity-$RUN_ID"
CONTEXT="kind-$CLUSTER"
NAMESPACE="test-4368-idle-capacity"
RELEASE="acme-capacity"
FULLNAME="$RELEASE-curie"
AGENT="acme-capacity-bot"
TAG="idle-capacity-$RUN_ID"
OWNED_KIND=0
DRIVER_PID=""
IMAGES=()
# Keep build artifacts outside every Docker build context.
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-idle-capacity-"))')"
export KUBECONFIG="$WORKDIR/kubeconfig"
export CURIE_CONFIG_DIR="$WORKDIR/curie-config"
# Never forward the invoking shell's model, GitHub, Slack, or API credentials.
# The fresh release uses only the published development defaults.
unset CURIE_CREDENTIALS ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL
unset CURIE_GITHUB_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_BOT_TOKEN SLACK_APP_TOKEN
unset CURIE_API_URL CURIE_API_KEY CURIE_VALKEY_PASSWORD CURIE_NAMESPACE CURIE_STREAM
unset CURIE_MODEL_BASE_URL CURIE_FAKE_MODEL CURIE_FAKE_SCRIPT CURIE_FAKE_SCRIPT_FILE

kc() { kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
cleanup() {
    local status=$? cleanup_status=0 remaining image
    trap - EXIT INT TERM
    set +e
    if [[ -n "$DRIVER_PID" ]]; then
        kill -TERM -- "-$DRIVER_PID" 2>/dev/null || true
        sleep 1
        kill -KILL -- "-$DRIVER_PID" 2>/dev/null || true
        wait "$DRIVER_PID" 2>/dev/null || true
    fi
    if (( status != 0 )); then
        if (( OWNED_KIND )); then
            for component in worker api; do
                kc logs "deployment/$FULLNAME-$component" --tail=250 --request-timeout=10s \
                    >"$WORKDIR/failure-$component.log" 2>&1 || true
            done
        fi
        python3 - "$WORKDIR" "$REPO_ROOT" "$RUN_ID" <<'PY_DIAGNOSTIC'
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
        [[ $? == 0 ]] || cleanup_status=1
    fi
    if (( OWNED_KIND )); then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || cleanup_status=1
        remaining="$(kind get clusters 2>/dev/null)" || cleanup_status=1
        if [[ $'\n'"$remaining"$'\n' == *$'\n'"$CLUSTER"$'\n'* ]]; then
            echo "FAIL: owned kind cluster still exists" >&2
            cleanup_status=1
        fi
        remaining="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER")" || cleanup_status=1
        if [[ -n "$remaining" ]]; then
            echo "FAIL: owned kind node containers remain" >&2
            cleanup_status=1
        fi
    fi
    for image in "${IMAGES[@]}"; do
        if docker image inspect "$image" >/dev/null 2>&1; then
            docker image rm "$image" >/dev/null 2>&1 || cleanup_status=1
        fi
        if docker image inspect "$image" >/dev/null 2>&1; then
            echo "FAIL: an owned candidate image tag remains" >&2
            cleanup_status=1
        fi
    done
    rm -rf -- "$WORKDIR" || cleanup_status=1
    if (( cleanup_status )); then
        exit 1
    fi
    echo "OK: owned kind cluster, node containers, and candidate tags are gone"
    if (( status == 0 )); then
        echo "PASS: clean and stale-route six-turn bursts finalized under quota two; reclaimed history resumed"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

clusters="$(kind get clusters)" || fail "could not inventory kind clusters"
[[ $'\n'"$clusters"$'\n' != *$'\n'"$CLUSTER"$'\n'* ]] || fail "generated kind cluster name is already occupied"
[[ -z "$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER")" ]] || fail "generated cluster name has existing containers"

echo "Building the candidate CLI and uniquely tagged API, worker, and runner images"
# Rebuild from the active checkout even when CURIE_BIN names a prebuilt binary:
# the reversed Fix pin run must execute its own product, including the CLI.
cargo build --locked --manifest-path "$REPO_ROOT/cli/Cargo.toml" \
    --target-dir "$WORKDIR/cargo" >"$WORKDIR/cargo.log" 2>&1 || fail "candidate CLI build failed"
BIN="$WORKDIR/cargo/debug/curie"
for component in api worker runner; do
    image="curie-$component:$TAG"
    IMAGES+=("$image")
    [[ "$component" == runner ]] && dockerfile="runner/Dockerfile" || dockerfile="apps/$component/Dockerfile"
    docker build --file "$REPO_ROOT/$dockerfile" --tag "$image" "$REPO_ROOT" \
        >"$WORKDIR/build-$component.log" 2>&1 || fail "candidate $component image build failed"
done

# Match CI's enforcing CNI and Kubernetes version on this owned cluster.
cat >"$WORKDIR/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  disableDefaultCNI: true
  podSubnet: "192.168.0.0/16"
nodes:
  - role: control-plane
YAML
python3 - "$WORKDIR/calico.yaml" <<'PY'
import hashlib, pathlib, sys, urllib.request
url = "https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml"
with urllib.request.urlopen(url, timeout=30) as response:
    manifest = response.read()
assert hashlib.sha256(manifest).hexdigest() == "be59408bf990e96276f631d2f9285c2a0f9802194c0ad1cecdb6d9c52623a1c8", "Calico manifest digest mismatch"
pathlib.Path(sys.argv[1]).write_bytes(manifest)
PY
# Register ownership before creation so a partial kind startup is cleaned too.
OWNED_KIND=1
kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" \
    --image kindest/node:v1.31.0 --config "$WORKDIR/kind.yaml" \
    >"$WORKDIR/kind.log" 2>&1 || fail "owned kind cluster creation failed"
[[ "$(kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" config current-context)" == "$CONTEXT" ]] \
    || fail "private kubeconfig does not select the owned cluster"
kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" apply -f "$WORKDIR/calico.yaml" \
    >"$WORKDIR/calico.log" 2>&1 || fail "owned cluster Calico install failed"
kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" -n kube-system \
    rollout status daemonset/calico-node --timeout=300s >/dev/null
kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" \
    wait --for=condition=Ready node --all --timeout=300s >/dev/null
kind load docker-image --name "$CLUSTER" "${IMAGES[@]}" >"$WORKDIR/load.log" 2>&1 \
    || fail "candidate image import failed"

# Keep normal route retention and claim deadlines; only sandbox pod quota is reduced.
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set api.image.repository=curie-api --set "api.image.tag=$TAG" --set api.image.pullPolicy=Never
    --set worker.image.repository=curie-worker --set "worker.image.tag=$TAG" --set worker.image.pullPolicy=Never
    --set agentSandbox.runner.image=curie-runner --set "agentSandbox.runner.tag=$TAG"
    --set agentSandbox.runner.imagePullPolicy=Never --set agentSandbox.runner.prewarm.imagePullPolicy=Never
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=true
    --set agentSandbox.platformPool.replicas=0 --set agentSandbox.runner.fakeModel=true
    --set worker.replicas=1 --set resourceQuota.hard.sandboxPodCount=2
    --set security.gvisor.mode=off
    --set langfuse.deploy=false --set langfuse.host=langfuse.example.com
    --set clickhouse.deploy=false --set otelCollector.deploy=false --set otelCollector.telemetryDisabled=true
    --set ui.deploy=false --set dispatcher.deploy=false --set mailAdapter.deploy=false
)
cd "$WORKDIR"
"$BIN" cluster --context "$CONTEXT" up --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --dev --fake-model --no-expose "${sets[@]}" \
    >"$WORKDIR/install.log" 2>&1 || fail "candidate cluster install failed"
for component in api worker; do
    kc rollout status "deployment/$FULLNAME-$component" --timeout=300s >/dev/null
    [[ "$(kc get "deployment/$FULLNAME-$component" -o jsonpath='{.spec.template.spec.containers[0].image}')" == "curie-$component:$TAG" ]] \
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
import json
import os
import pathlib
import subprocess
import sys
import time

root, binary, context, namespace, release, fullname, agent, chart = sys.argv[1:]
root = pathlib.Path(root)
kube = ["kubectl", "--kubeconfig", os.environ["KUBECONFIG"], "--context", context, "-n", namespace]
message = [binary, "--json", "cluster", "--context", context, "message", "--namespace", namespace,
           "--release", release, "--chart", chart, "--agent", agent, "--channel", "C0EXAMPLE1", "--timeout-secs", "300"]
children = []


def run(args, *, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, "an owned runtime observation or setup command failed"
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


def observe():
    quota = get("resourcequota", fullname + "-sandbox-quota")
    assert quota["spec"]["hard"]["pods"] == "2", "sandbox pod quota changed from two"
    quota_used = int(quota.get("status", {}).get("used", {}).get("pods", 0))
    assert quota_used <= 2, "observed quota usage exceeded two"
    priority = quota["spec"]["scopeSelector"]["matchExpressions"][0]["values"][0]
    pods = {pod["metadata"]["uid"]: pod for pod in get("pods")["items"]
            if pod["spec"].get("priorityClassName") == priority}
    # Kubernetes' compute quota `pods` counts only nonterminal Pods; Failed
    # and Succeeded objects can remain after they stop consuming quota.
    # https://kubernetes.io/docs/concepts/policy/resource-quotas/#compute-resource-quota
    # Preserve every observed UID below for reclamation and active-route checks.
    chargeable = [pod for pod in pods.values()
                  if pod.get("status", {}).get("phase") not in {"Failed", "Succeeded"}]
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
