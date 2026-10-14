#!/usr/bin/env bash
# #4367: a real repository-selection HTTP 500 reaches the failed turn reply.
# fix-pin-provision: candidate-stack
# The caller supplies the candidate CLI, context, run ID, and loaded images.
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
CURIE_BIN="$(python3 -c 'import os; print(os.path.abspath(os.environ["CURIE_BIN"]))')"
CONTEXT="$CURIE_SCENARIO_CONTEXT"
NAMESPACE="test-4367-wsdiag-$CURIE_SCENARIO_RUN_ID"
RELEASE="wsdiag-$CURIE_SCENARIO_RUN_ID"
FULLNAME="$RELEASE-curie"
AGENT="acme-workspace-bot"
GATE="workspace-cold-start"
POSTGRES_REPLICAS=""
POSTGRES_STOPPED=0
MESSAGE_PID=""
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
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-workspace-diagnostic-"))')"
export CURIE_CONFIG_DIR="$WORKDIR/curie-config"
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
    local status=$? cleanup_status=0 remaining object crd
    trap - EXIT INT TERM
    set +e
    stop_message
    if (( POSTGRES_STOPPED )); then
        restore_postgres || { echo "FAIL: could not restore recorded PostgreSQL replicas" >&2; cleanup_status=1; }
    fi
    helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NAMESPACE" \
        --ignore-not-found --wait --timeout 60s >"$WORKDIR/uninstall.log" 2>&1 \
        || { echo "FAIL: could not uninstall the owned release" >&2; cleanup_status=1; }
    # The namespace controller estimates remaining pod termination grace and
    # can defer reconciliation after those pods are gone. Drain owned pods
    # first, retaining their normal grace period, before namespace deletion.
    kc delete pods --all --ignore-not-found --wait=true --timeout=60s >"$WORKDIR/delete-pods.log" 2>&1 \
        || { echo "FAIL: could not drain pods in the owned namespace" >&2; cleanup_status=1; }
    kc delete namespace "$NAMESPACE" --ignore-not-found --wait=false >"$WORKDIR/delete-namespace.log" 2>&1 \
        || { echo "FAIL: could not delete the owned namespace" >&2; cleanup_status=1; }
    kc wait --for=delete "namespace/$NAMESPACE" --timeout=60s >"$WORKDIR/wait-namespace.log" 2>&1 \
        || { echo "FAIL: owned namespace deletion did not complete" >&2; cleanup_status=1; }
    kc wait --for=delete namespace/agent-sandbox-system --timeout=60s >"$WORKDIR/wait-controller.log" 2>&1 \
        || { echo "FAIL: controller namespace deletion did not complete" >&2; cleanup_status=1; }
    for crd in "${ABSENT_CRDS[@]}"; do
        kc delete "crd/$crd" --ignore-not-found --wait=false >"$WORKDIR/delete-crd.log" 2>&1 \
            || { echo "FAIL: could not delete owned CRD $crd" >&2; cleanup_status=1; }
        kc wait --for=delete "crd/$crd" --timeout=60s >"$WORKDIR/wait-crd.log" 2>&1 \
            || { echo "FAIL: owned CRD $crd deletion did not complete" >&2; cleanup_status=1; }
    done
    for object in "namespace/$NAMESPACE" "${CLUSTER_OBJECTS[@]}" "${ABSENT_CRDS[@]/#/crd/}"; do
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
    rm -rf -- "$WORKDIR" || cleanup_status=1
    if (( cleanup_status )); then
        exit 1
    fi
    echo "OK: owned release, namespaces, controller resources, priority classes, and new CRDs are gone"
    if (( status == 0 )); then
        echo "PASS: repository-selection HTTP 500 reached workspace-error finalized=false; restored PostgreSQL finalized the next turn"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Keep the API endpoint serving through this test's bounded database outage.
# /ready still probes the real database; /health remains process-local. These
# test-only cadences preserve the required liveness-after-readiness ordering.
# A values file preserves caller-supplied tags as strings, including numeric
# tags that Helm's --set would otherwise coerce.
python3 - "$WORKDIR/images.yaml" <<'PY'
import os, pathlib, sys, yaml
images = {component: os.environ[f"CURIE_SCENARIO_{component.upper()}_IMAGE"].rsplit(":", 1)
          for component in ("api", "worker", "runner")}
values = {component: {"image": {"repository": images[component][0], "tag": images[component][1]}}
          for component in ("api", "worker")}
values["agentSandbox"] = {"runner": {"image": images["runner"][0], "tag": images["runner"][1]}}
pathlib.Path(sys.argv[1]).write_text(yaml.safe_dump(values))
PY
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set api.image.pullPolicy=Never --set worker.image.pullPolicy=Never
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
# Keep the caller's working directory so relative KUBECONFIG paths retain
# their meaning. CLI state and every scenario artifact use private paths.
"$CURIE_BIN" cluster --context "$CONTEXT" up --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --values-file "$WORKDIR/images.yaml" \
    --dev --fake-model --no-expose "${sets[@]}" \
    >"$WORKDIR/install.log" 2>&1 || fail "candidate cluster install failed"
for component in api worker; do
    image_variable="CURIE_SCENARIO_${component^^}_IMAGE"
    kc rollout status "deployment/$FULLNAME-$component" --timeout=300s >/dev/null
    [[ "$(kc get "deployment/$FULLNAME-$component" -o jsonpath='{.spec.template.spec.containers[0].image}')" == "${!image_variable}" ]] \
        || fail "the $component Deployment is not using the candidate image"
done
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_CLAIM_TIMEOUT_SECONDS")].value}')" == 90 ]] \
    || fail "worker claim timeout must retain its normal 90-second value"
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_WORKSPACE_ENABLED")].value}')" == true ]] \
    || fail "real workspace selection must be enabled"
"$CURIE_BIN" init "$AGENT" --dir "$WORKDIR/bundle" >"$WORKDIR/init.log" 2>&1
"$CURIE_BIN" --json cluster --context "$CONTEXT" deploy --namespace "$NAMESPACE" --release "$RELEASE" \
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
python3 - "$WORKDIR/templates.json" "$WORKDIR" "$CURIE_SCENARIO_RUNNER_IMAGE" "$GATE" <<'PY'
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
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "16Mi"},
                      "limits": {"cpu": "50m", "memory": "32Mi", "ephemeral-storage": "32Mi"}},
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
    setsid "$CURIE_BIN" --json cluster --context "$CONTEXT" message \
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
