#!/usr/bin/env bash
# #4367: a real repository-selection HTTP 500 reaches the failed turn reply.
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
docker info >/dev/null 2>&1 || fail "Docker is unavailable"

RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
CLUSTER="acme-workspace-diagnostic-$RUN_ID"
CONTEXT="kind-$CLUSTER"
NAMESPACE="test-4367-workspace-diagnostic"
RELEASE="acme-workspace"
FULLNAME="$RELEASE-curie"
AGENT="acme-workspace-bot"
TAG="workspace-diagnostic-$RUN_ID"
GATE="workspace-cold-start"
OWNED_KIND=0
POSTGRES_REPLICAS=""
POSTGRES_STOPPED=0
MESSAGE_PID=""
IMAGES=()
# Keep build artifacts outside every Docker build context.
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-workspace-diagnostic-"))')"
export KUBECONFIG="$WORKDIR/kubeconfig"
export CURIE_CONFIG_DIR="$WORKDIR/curie-config"
# Never forward the invoking shell's model, GitHub, Slack, or API credentials.
# The fresh release uses only the published development defaults.
unset CURIE_CREDENTIALS ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL
unset CURIE_GITHUB_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_BOT_TOKEN SLACK_APP_TOKEN
unset CURIE_API_URL CURIE_API_KEY CURIE_VALKEY_PASSWORD CURIE_NAMESPACE CURIE_STREAM
unset CURIE_MODEL_BASE_URL CURIE_FAKE_MODEL CURIE_FAKE_SCRIPT CURIE_FAKE_SCRIPT_FILE

kc() { kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
stop_message() {
    [[ -n "$MESSAGE_PID" ]] || return 0
    # The CLI and its port-forwards are in this invocation's own process group.
    kill -TERM -- "-$MESSAGE_PID" 2>/dev/null || true
    sleep 1
    kill -KILL -- "-$MESSAGE_PID" 2>/dev/null || true
    wait "$MESSAGE_PID" 2>/dev/null || true
    MESSAGE_PID=""
}
restore_postgres() {
    (( POSTGRES_STOPPED )) || return 0
    kc scale "statefulset/$FULLNAME-postgres" --replicas="$POSTGRES_REPLICAS" >/dev/null || return 1
    kc rollout status "statefulset/$FULLNAME-postgres" --timeout=180s >/dev/null || return 1
    [[ "$(kc get "statefulset/$FULLNAME-postgres" -o jsonpath='{.spec.replicas}')" == "$POSTGRES_REPLICAS" ]] || return 1
    POSTGRES_STOPPED=0
}
cleanup() {
    local status=$? cleanup_status=0 remaining image
    trap - EXIT INT TERM
    set +e
    stop_message
    if (( POSTGRES_STOPPED )); then
        restore_postgres || { echo "FAIL: could not restore recorded PostgreSQL replicas" >&2; cleanup_status=1; }
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
        echo "PASS: repository-selection HTTP 500 reached workspace-error finalized=false; restored PostgreSQL finalized the next turn"
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

# Keep the API endpoint serving through this test's bounded database outage.
# /ready still probes the real database; /health remains process-local. These
# test-only cadences preserve the required liveness-after-readiness ordering.
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set api.image.repository=curie-api --set "api.image.tag=$TAG" --set api.image.pullPolicy=Never
    --set worker.image.repository=curie-worker --set "worker.image.tag=$TAG" --set worker.image.pullPolicy=Never
    --set agentSandbox.runner.image=curie-runner --set "agentSandbox.runner.tag=$TAG"
    --set agentSandbox.runner.imagePullPolicy=Never --set agentSandbox.runner.prewarm.imagePullPolicy=Never
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=true
    --set agentSandbox.platformPool.replicas=0 --set agentSandbox.runner.fakeModel=true
    --set worker.replicas=1 --set agentSandbox.runner.workspace.enabled=true
    --set api.readinessProbe.failureThreshold=120 --set api.livenessProbe.failureThreshold=60
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
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_WORKSPACE_ENABLED")].value}')" == true ]] \
    || fail "real workspace selection must be enabled"
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

# A test-only init container makes the cold-start boundary observable before
# the worker's ordinary claim deadline. The runner, workspace selection, API,
# database, stream transport, and reply delivery are otherwise unmodified.
kc create configmap "$GATE" --from-literal=release=hold >/dev/null
kc get sandboxtemplates -o json >"$WORKDIR/templates.json"
python3 - "$WORKDIR/templates.json" "$WORKDIR" "curie-runner:$TAG" "$GATE" <<'PY'
import json, pathlib, sys
templates = json.loads(pathlib.Path(sys.argv[1]).read_text())["items"]
root, image, gate = pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4]
assert templates, "candidate installed no SandboxTemplates"
for index, template in enumerate(templates):
    spec = template["spec"]["podTemplate"]["spec"]
    assert any(container["image"] == image for container in spec["containers"]), "template does not use candidate runner"
    original = {key: spec.get(key, []) for key in ("initContainers", "volumes")}
    (root / f"template-{index}.original.json").write_text(json.dumps({"spec": {"podTemplate": {"spec": original}}}))
    spec = dict(original)
    spec["volumes"] = [*spec["volumes"], {"name": gate, "configMap": {"name": gate}}]
    spec["initContainers"] = [{
        "name": gate, "image": image, "imagePullPolicy": "Never",
        "command": ["/bin/sh", "-ec", "until grep -qx open /cold-start/release; do sleep 0.2; done"],
        "volumeMounts": [{"name": gate, "mountPath": "/cold-start", "readOnly": True}],
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"}},
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"cpu": "50m", "memory": "32Mi"}},
    }, *spec["initContainers"]]
    (root / f"template-{index}.patch.json").write_text(json.dumps({"spec": {"podTemplate": {"spec": spec}}}))
    with (root / "template-names").open("a") as names:
        names.write(template["metadata"]["name"] + "\n")
PY
index=0
while IFS= read -r template; do
    kc patch sandboxtemplate "$template" --type=merge --patch-file "$WORKDIR/template-$index.patch.json" >/dev/null
    index=$((index + 1))
done <"$WORKDIR/template-names"

start_turn() {
    local phase=$1 thread
    thread="$(python3 -c 'import time; stamp=time.time_ns(); print(f"{stamp // 10**9}.{stamp % 10**9 // 10**3:06}")')"
    setsid "$BIN" --json cluster --context "$CONTEXT" message \
        --namespace "$NAMESPACE" --release "$RELEASE" --agent "$AGENT" --channel C0EXAMPLE1 \
        --chart "$REPO_ROOT/charts/curie" \
        --thread "$thread" --timeout-secs 300 "Reply with a short workspace diagnostic check." \
        >"$WORKDIR/$phase.json" 2>"$WORKDIR/$phase.log" &
    MESSAGE_PID=$!
}
finish_turn() {
    local deadline=$((SECONDS + 330)) status=0
    while kill -0 "$MESSAGE_PID" 2>/dev/null; do
        (( SECONDS < deadline )) || fail "candidate CLI turn exceeded the bounded runtime"
        sleep 0.5
    done
    wait "$MESSAGE_PID" || status=$?
    # Reap any owned port-forward that survived the CLI's normal RAII cleanup.
    stop_message
    TURN_STATUS=$status
}

echo "Starting a cold runner, then stopping this release's PostgreSQL"
start_turn outage
deadline=$((SECONDS + 60))
while :; do
    kc get pods -o json >"$WORKDIR/pods.json"
    if python3 - "$WORKDIR/pods.json" "$GATE" <<'PY'
import json, pathlib, sys
pods = json.loads(pathlib.Path(sys.argv[1]).read_text())["items"]
observed = any(status["name"] == sys.argv[2] and "running" in status.get("state", {})
               for pod in pods for status in pod.get("status", {}).get("initContainerStatuses", []))
raise SystemExit(0 if observed else 1)
PY
    then break; fi
    kill -0 "$MESSAGE_PID" 2>/dev/null || fail "turn ended before an owned cold runner started"
    (( SECONDS < deadline )) || fail "no owned runner reached the cold-start gate"
    sleep 0.25
done
POSTGRES_REPLICAS="$(kc get "statefulset/$FULLNAME-postgres" -o jsonpath='{.spec.replicas}')"
[[ "$POSTGRES_REPLICAS" =~ ^[1-9][0-9]*$ ]] || fail "PostgreSQL did not have a positive replica count"
POSTGRES_STOPPED=1
kc scale "statefulset/$FULLNAME-postgres" --replicas=0 >/dev/null
kc wait --for=delete "pod/$FULLNAME-postgres-0" --timeout=60s >/dev/null
finish_turn
# A failed product turn must be a CLI failure with a structured failed reply.
[[ "$TURN_STATUS" == 1 ]] || fail "outage turn did not report the CLI's product-failure exit status"
python3 - "$WORKDIR/outage.json" <<'PY'
import json, pathlib, sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("failed") is True, "outage turn was not failed"
assert payload.get("finalized") is False, "outage turn incorrectly finalized"
assert payload.get("failure_class") == "workspace-error", "wrong failure classification"
reply = payload.get("reply", "")
assert "repository-selection" in reply and "HTTP 500" in reply, "failed reply omitted repository-selection HTTP 500"
print("OK: failed CLI reply names repository-selection HTTP 500 with workspace-error finalized=false")
PY
kc logs "deployment/$FULLNAME-worker" --tail=-1 >"$WORKDIR/worker.log"
python3 - "$WORKDIR/worker.log" <<'PY'
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
assert any("workspace start failed" in line and "stage=repository-selection" in line and "HTTP 500" in line
           for line in lines), "worker did not observe a real repository-selection HTTP 500"
print("OK: the real worker observed repository-selection API HTTP 500")
PY

echo "Restoring the recorded PostgreSQL replica count and cold-start templates"
restore_postgres || fail "PostgreSQL did not recover at its recorded replica count"
# The test's long readiness tolerance keeps Deployment.Available true during
# the outage. Probe the real API route instead of trusting that retained flag.
deadline=$((SECONDS + 90))
until kc exec "deployment/$FULLNAME-api" -- python3 -c \
    'import json, urllib.request; response=urllib.request.urlopen("http://127.0.0.1:8000/ready", timeout=5); assert response.status == 200 and json.load(response) == {"status": "ok"}' \
    >"$WORKDIR/recovery-readiness.log" 2>&1; do
    (( SECONDS < deadline )) || fail "the real API readiness endpoint did not confirm PostgreSQL recovery"
    sleep 1
done
echo "OK: the real API readiness endpoint confirmed restored database access"
index=0
while IFS= read -r template; do
    kc patch sandboxtemplate "$template" --type=merge --patch-file "$WORKDIR/template-$index.original.json" >/dev/null
    index=$((index + 1))
done <"$WORKDIR/template-names"
kc patch configmap "$GATE" --type=merge -p '{"data":{"release":"open"}}' >/dev/null
start_turn recovery
finish_turn
if [[ "$TURN_STATUS" != 0 ]]; then
    # Emit only fixed metadata. Arbitrary replies, API errors, identifiers, and
    # stderr remain private and are removed by this invocation's cleanup.
    python3 - "$WORKDIR/recovery.json" "$TURN_STATUS" <<'PY'
import json, pathlib, sys
try:
    payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
except (OSError, ValueError):
    payload = None
summary = {"phase": "recovery", "cli_exit": int(sys.argv[2]), "json_object": isinstance(payload, dict)}
if isinstance(payload, dict):
    for name in ("failed", "finalized", "timed_out"):
        value = payload.get(name)
        summary[name] = value if isinstance(value, bool) else None
    known_classes = {"workspace-error", "runner-error", "runner-timeout", "budget-exceeded",
                     "model-credential-rejected", "history-persistence-error", "agent-busy"}
    failure_class = payload.get("failure_class")
    summary["failure_class"] = failure_class if isinstance(failure_class, str) and failure_class in known_classes else "unrecognized-or-absent"
    reply = payload.get("reply")
    summary["reply_present"] = isinstance(reply, str) and bool(reply.strip())
    summary["reply_repository_selection"] = isinstance(reply, str) and "repository-selection" in reply
    summary["reply_http_500"] = isinstance(reply, str) and "HTTP 500" in reply
    error = payload.get("error")
    summary["cli_error"] = isinstance(error, str)
    summary["error_agent_listing"] = isinstance(error, str) and "listing agents" in error
    summary["error_http_500"] = isinstance(error, str) and "500" in error
print("Recovery diagnostic: " + json.dumps(summary, sort_keys=True))
PY
fi
[[ "$TURN_STATUS" == 0 ]] || fail "recovery CLI turn failed"
python3 - "$WORKDIR/recovery.json" <<'PY'
import json, pathlib, sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("finalized") is True, "recovery turn did not finalize"
assert payload.get("failed") is not True, "recovery turn was failed"
assert str(payload.get("reply") or "").strip(), "recovery turn had no reply"
print("OK: the subsequent real CLI turn finalized with a reply after PostgreSQL recovery")
PY
