#!/usr/bin/env bash
# #4889: a factory agent's real sandbox keeps a 1280Mi workspace write that the
# standard envelope evicts.
# fix-pin-provision: candidate-stack
# The caller supplies the candidate CLI, context, run ID, and loaded images.
# This scenario owns only its release and the resources installed by it.
#
# Phases, strictly sequential so the 8Gi factory request is never reserved
# twice (one factory sandbox fills the default sandbox requests.memory quota):
#   1. Install the repository chart through `curie cluster up`, selecting the
#      factory profile for agent acme-fp with
#      agentSandbox.agentResourceProfiles.acme-fp=factory. factory.enabled is
#      deliberately not used: it requires a tracker and code host whose
#      credential Secret the API mounts non-optionally, and it starts poll intake
#      against a forge this cluster cannot reach. The derived selection
#      (factory.enabled plus factory.sandboxEgress.<agent>) is pinned equal to
#      this opt-in envelope by the render assertions in
#      sandbox-emptydir-sizelimit-assertions.sh.
#   2. The live SandboxTemplate the chart generated for acme-fp carries the
#      full factory envelope; the control agent acme-sp has no per-agent
#      template and uses the generic (standard) one.
#   3. Free-memory precheck: some node must have at least 8Gi of allocatable
#      memory left after every non-terminated pod's memory request.
#   4. One real CLI turn for acme-fp creates a SandboxClaim through the worker
#      and controller. That runner pod carries the factory envelope; 1280Mi of
#      random data written under /workspace survives at least 90 seconds of
#      repeated checks on the same pod (Running, not evicted, no restart).
#   5. The acme-fp claim is deleted, and its sandbox and pod are verified gone.
#   6. Only then one real CLI turn for acme-sp; the same write evicts that pod
#      with a kubelet message naming the workspace volume and its 1Gi limit.
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
# Short owned names keep every per-agent pool name within 63 characters.
NAMESPACE="test-4889-profile-$CURIE_SCENARIO_RUN_ID"
RELEASE="frp-$CURIE_SCENARIO_RUN_ID"
FULLNAME="$RELEASE-curie"
FACTORY_AGENT="acme-fp"
CONTROL_AGENT="acme-sp"
FACTORY_TEMPLATE="$FULLNAME-agent-$FACTORY_AGENT-runner"
GENERIC_TEMPLATE="$FULLNAME-runner"
# 1280Mi: above the standard 1Gi workspace, far below the factory 15Gi one.
FILL_BYTES=1342177280
FILL_PATH=/workspace/.curie-4889-fill.bin
HOLD_SECONDS=90
HOLD_INTERVAL=5
EVICTION_WAIT_SECONDS=240
FACTORY_MEMORY_BYTES=$((8 * 1024 * 1024 * 1024))
MESSAGE_PID=""
INSTALL_STARTED=0
# Never forward the invoking shell's model, GitHub, Slack, or API credentials.
# The fresh release uses only the published development defaults.
unset CURIE_CREDENTIALS ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL
unset CURIE_GITHUB_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_BOT_TOKEN SLACK_APP_TOKEN
unset CURIE_API_URL CURIE_API_KEY CURIE_VALKEY_PASSWORD CURIE_NAMESPACE CURIE_STREAM
unset CURIE_MODEL_BASE_URL CURIE_FAKE_MODEL CURIE_FAKE_SCRIPT CURIE_FAKE_SCRIPT_FILE

# Every progress line carries the seconds elapsed since this script started, so
# the CI log shows the length of the retention hold directly.
log() { printf '[+%4ds] %s\n' "$SECONDS" "$*"; }
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
WORKDIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="curie-factory-resource-profile-"))')"
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
cleanup() {
    local status=$? cleanup_status=0 remaining object crd
    trap - EXIT INT TERM
    set +e
    if [[ -n "${CONTROL_POD:-}" ]]; then
        control_diagnostics || log "control diagnostics could not be completed"
    fi
    stop_message
    if (( INSTALL_STARTED )); then
        # Let the live controller release finalizers before Helm stops it.
        kc delete sandboxclaims --all --ignore-not-found --wait=true --timeout=90s \
            >"$WORKDIR/delete-claims.log" 2>&1 \
            || { echo "FAIL: could not delete owned sandbox claims" >&2; cleanup_status=1; }
        kc delete sandboxes --all --ignore-not-found --wait=true --timeout=90s \
            >"$WORKDIR/delete-sandboxes.log" 2>&1 \
            || { echo "FAIL: could not delete owned sandboxes" >&2; cleanup_status=1; }
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
    kc wait --for=delete "namespace/$NAMESPACE" --timeout=120s >"$WORKDIR/wait-namespace.log" 2>&1 \
        || { echo "FAIL: owned namespace deletion did not complete" >&2; cleanup_status=1; }
    kc wait --for=delete namespace/agent-sandbox-system --timeout=120s >"$WORKDIR/wait-controller.log" 2>&1 \
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
    echo "OK: owned release, namespaces, sandboxes, controller resources, priority classes, and new CRDs are gone"
    if (( status == 0 )); then
        echo "PASS: the factory sandbox kept 1280Mi in /workspace for ${HOLD_SECONDS}s or more on one Running pod; the standard sandbox was evicted for exceeding its 1Gi workspace"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
# No profile field is overridden: the envelope under test is the chart's own.
sets=(
    --set "fullnameOverride=$FULLNAME"
    --set api.image.pullPolicy=Never --set worker.image.pullPolicy=Never
    --set agentSandbox.runner.imagePullPolicy=Never --set agentSandbox.runner.prewarm.imagePullPolicy=Never
    --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=true
    --set agentSandbox.platformPool.replicas=0 --set agentSandbox.runner.fakeModel=true
    --set worker.replicas=1 --set agentSandbox.runner.workspace.enabled=true
    --set security.gvisor.mode=off
    --set langfuse.deploy=false --set langfuse.host=langfuse.example.com
    --set clickhouse.deploy=false --set otelCollector.deploy=false --set otelCollector.telemetryDisabled=true
    --set ui.deploy=false --set dispatcher.deploy=false --set mailAdapter.deploy=false
    --set "agentSandbox.agentResourceProfiles.$FACTORY_AGENT=factory"
)
log "Installing the candidate chart with agentResourceProfiles.$FACTORY_AGENT=factory"
INSTALL_STARTED=1
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
[[ "$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_WORKSPACE_ENABLED")].value}')" == true ]] \
    || fail "real workspace selection must be enabled"
pools="$(kc get deployment "$FULLNAME-worker" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CURIE_AGENT_SANDBOX_POOLS")].value}')"
[[ ",$pools," == *",$FACTORY_AGENT,"* ]] || fail "the worker does not route $FACTORY_AGENT to its own sandbox pool"
[[ ",$pools," != *",$CONTROL_AGENT,"* ]] || fail "the control agent $CONTROL_AGENT unexpectedly has its own sandbox pool"
log "OK: the worker routes $FACTORY_AGENT to its own pool and $CONTROL_AGENT to the generic one"

# The envelope checker reads a pod spec (a live Pod or a SandboxTemplate's
# podTemplate) and asserts every container's resources, the workspace volume,
# and each writable path volume keyed by its runner mount path.
cat >"$WORKDIR/envelope.py" <<'PY'
import json
import pathlib
import re
import sys

UNITS = {"": 1, "k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
         "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}
PROFILES = {
    "factory": {
        "resources": {"requests": {"cpu": "50m", "memory": "8Gi", "ephemeral-storage": "10Gi"},
                      "limits": {"cpu": "2", "memory": "8Gi", "ephemeral-storage": "20Gi"}},
        "workspace": "15Gi", "/home/runner": "15Gi", "/tmp": "4Gi",
    },
    "standard": {
        "resources": {"requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
                      "limits": {"cpu": "1", "memory": "768Mi", "ephemeral-storage": "4Gi"}},
        "workspace": "1Gi", "/home/runner": "512Mi", "/tmp": "512Mi",
    },
}


def quantity(value, key):
    text = str(value)
    if key == "cpu":
        if text.endswith("m"):
            return int(text[:-1])
        return int(float(text) * 1000)
    match = re.fullmatch(r"([0-9]+)(Ki|Mi|Gi|Ti|k|M|G|T)?", text)
    assert match, f"unparsed quantity {text!r} for {key}"
    return int(match.group(1)) * UNITS[match.group(2) or ""]


def same(actual, expected, key):
    return actual is not None and quantity(actual, key) == quantity(expected, key)


source, profile, label = sys.argv[1:4]
document = json.loads(pathlib.Path(source).read_text())
spec = document["spec"]["podTemplate"]["spec"] if document.get("kind") == "SandboxTemplate" else document["spec"]
want = PROFILES[profile]
containers = [*spec.get("initContainers", []), *spec.get("containers", [])]
runner = next((c for c in spec.get("containers", []) if c["name"] == "runner"), None)
assert runner is not None, f"{label}: no runner container"
problems = []
for container in containers:
    resources = container.get("resources") or {}
    # Only the chart's own containers carry the envelope; a container the
    # chart did not render would have no runner envelope to compare.
    for side, fields in want["resources"].items():
        for key, expected in fields.items():
            actual = (resources.get(side) or {}).get(key)
            if not same(actual, expected, key):
                problems.append(f"container {container['name']} {side}.{key}={actual} (want {expected})")
volumes = {volume["name"]: volume for volume in spec.get("volumes", [])}
workspace = (volumes.get("workspace") or {}).get("emptyDir") or {}
if not same(workspace.get("sizeLimit"), want["workspace"], "memory"):
    problems.append(f"workspace sizeLimit={workspace.get('sizeLimit')} (want {want['workspace']})")
mounts = {mount["mountPath"]: mount["name"] for mount in runner.get("volumeMounts", [])
          if mount["name"].startswith("writable-")}
for path in ("/home/runner", "/tmp"):
    name = mounts.get(path)
    size = ((volumes.get(name) or {}).get("emptyDir") or {}).get("sizeLimit") if name else None
    if not same(size, want[path], "memory"):
        problems.append(f"writable {path} sizeLimit={size} (want {want[path]})")
assert not problems, f"{label} does not carry the {profile} envelope: " + "; ".join(problems)
print(f"OK: {label} carries the {profile} envelope (memory {want['resources']['requests']['memory']}/"
      f"{want['resources']['limits']['memory']}, cpu {want['resources']['requests']['cpu']}/"
      f"{want['resources']['limits']['cpu']}, ephemeral-storage {want['resources']['requests']['ephemeral-storage']}/"
      f"{want['resources']['limits']['ephemeral-storage']}, workspace {want['workspace']}, "
      f"home {want['/home/runner']}, tmp {want['/tmp']}) on {len(containers)} containers")
PY

# The live objects are what the chart generated, read back from the API server.
kc get sandboxtemplate "$FACTORY_TEMPLATE" -o json >"$WORKDIR/factory-template.json" \
    || fail "the chart generated no SandboxTemplate for $FACTORY_AGENT"
python3 "$WORKDIR/envelope.py" "$WORKDIR/factory-template.json" factory "SandboxTemplate $FACTORY_TEMPLATE"
kc get sandboxtemplate "$GENERIC_TEMPLATE" -o json >"$WORKDIR/generic-template.json" \
    || fail "the chart generated no generic SandboxTemplate"
python3 "$WORKDIR/envelope.py" "$WORKDIR/generic-template.json" standard "SandboxTemplate $GENERIC_TEMPLATE"
[[ -z "$(kc get sandboxtemplate "$FULLNAME-agent-$CONTROL_AGENT-runner" --ignore-not-found -o name)" ]] \
    || fail "the control agent $CONTROL_AGENT unexpectedly has its own SandboxTemplate"

for agent in "$FACTORY_AGENT" "$CONTROL_AGENT"; do
    "$CURIE_BIN" init "$agent" --dir "$WORKDIR/bundle-$agent" >"$WORKDIR/init-$agent.log" 2>&1 \
        || fail "could not initialize the $agent bundle"
done
"$CURIE_BIN" --json cluster --context "$CONTEXT" deploy --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --plugin-dir "$WORKDIR/bundle-$FACTORY_AGENT" --agent "$FACTORY_AGENT" \
    --slack-channel C0EXAMPLE1 >"$WORKDIR/deploy-$FACTORY_AGENT.json" 2>"$WORKDIR/deploy-$FACTORY_AGENT.log" \
    || fail "factory agent deployment failed"
"$CURIE_BIN" --json cluster --context "$CONTEXT" deploy --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" --plugin-dir "$WORKDIR/bundle-$CONTROL_AGENT" --agent "$CONTROL_AGENT" \
    --slack-channel C0EXAMPLE2 >"$WORKDIR/deploy-$CONTROL_AGENT.json" 2>"$WORKDIR/deploy-$CONTROL_AGENT.log" \
    || fail "control agent deployment failed"
python3 - "$WORKDIR/deploy-$FACTORY_AGENT.json" "$WORKDIR/deploy-$CONTROL_AGENT.json" <<'PY'
import json, pathlib, sys
for path in sys.argv[1:]:
    payload = json.loads(pathlib.Path(path).read_text())
    assert payload["deployment"]["status"] == "active", "candidate deployment is not active"
PY
kc rollout status "deployment/$FULLNAME-worker" --timeout=300s >/dev/null
log "OK: both agents deployed through the candidate CLI"

# Free-memory precheck, after the platform is running so its requests count.
kc get resourcequota "$FULLNAME-sandbox-quota" -o json >"$WORKDIR/quota.json"
kubectl --context "$CONTEXT" get nodes -o json >"$WORKDIR/nodes.json"
kubectl --context "$CONTEXT" get pods -A -o json >"$WORKDIR/all-pods.json"
python3 - "$WORKDIR/quota.json" "$WORKDIR/nodes.json" "$WORKDIR/all-pods.json" "$FACTORY_MEMORY_BYTES" <<'PY'
import json, pathlib, re, sys

UNITS = {"": 1, "k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
         "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}


def memory(text):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|k|M|G|T)?", str(text))
    assert match, f"unparsed memory quantity {text!r}"
    return int(float(match.group(1)) * UNITS[match.group(2) or ""])


def gib(value):
    return f"{value / 2**30:.2f}Gi"


quota, nodes, pods = (json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:4])
need = int(sys.argv[4])
hard = quota["spec"]["hard"].get("requests.memory")
assert hard is not None and memory(hard) >= need, (
    f"the sandbox ResourceQuota requests.memory {hard} cannot admit one {gib(need)} factory sandbox")
requested = {}
for pod in pods["items"]:
    if pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}:
        continue
    node = pod["spec"].get("nodeName")
    if not node:
        continue
    total = 0
    for container in pod["spec"].get("containers", []):
        total += memory(((container.get("resources") or {}).get("requests") or {}).get("memory", "0"))
    init = max((memory(((c.get("resources") or {}).get("requests") or {}).get("memory", "0"))
                for c in pod["spec"].get("initContainers", [])), default=0)
    requested[node] = requested.get(node, 0) + max(total, init)
free = {}
for node in nodes["items"]:
    if node["spec"].get("unschedulable"):
        continue
    name = node["metadata"]["name"]
    free[name] = memory(node["status"]["allocatable"]["memory"]) - requested.get(name, 0)
best = max(free.values(), default=0)
if best < need:
    print(f"FAIL: free-memory precheck: no schedulable node has {gib(need)} of unrequested allocatable memory "
          f"for one factory sandbox (largest is {gib(best)}). The factory profile requests 8Gi per sandbox; "
          "run this scenario on a node with more memory or fewer concurrent workloads.", file=sys.stderr)
    raise SystemExit(1)
print(f"OK: free-memory precheck: largest unrequested allocatable memory on a node is {gib(best)} "
      f"(need {gib(need)}); sandbox quota requests.memory {hard}")
PY

TURN_STATUS=0
start_turn() {
    local agent=$1 channel=$2 thread
    thread="$(python3 -c 'import time; stamp=time.time_ns(); print(f"{stamp // 10**9}.{stamp % 10**9 // 10**3:06}")')"
    setsid "$CURIE_BIN" --json cluster --context "$CONTEXT" message \
        --namespace "$NAMESPACE" --release "$RELEASE" --agent "$agent" --channel "$channel" \
        --chart "$REPO_ROOT/charts/curie" \
        --thread "$thread" --timeout-secs 300 "Reply with a short sandbox envelope check." \
        >"$WORKDIR/turn-$agent.json" 2>"$WORKDIR/turn-$agent.log" &
    MESSAGE_PID=$!
}
finish_turn() {
    local agent=$1 deadline=$((SECONDS + 330)) status=0
    while kill -0 "$MESSAGE_PID" 2>/dev/null; do
        (( SECONDS < deadline )) || fail "the $agent CLI turn exceeded the bounded runtime"
        sleep 0.5
    done
    wait "$MESSAGE_PID" || status=$?
    # Reap any owned port-forward that survived the CLI's normal RAII cleanup.
    stop_message
    TURN_STATUS=$status
    [[ "$TURN_STATUS" == 0 ]] || fail "the $agent CLI turn failed (exit $TURN_STATUS)"
    python3 - "$WORKDIR/turn-$agent.json" "$agent" <<'PY'
import json, pathlib, sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("finalized") is True, f"the {sys.argv[2]} turn did not finalize"
assert payload.get("failed") is not True, f"the {sys.argv[2]} turn failed"
PY
}
# Prints "<claim> <pod>" for the single claim the worker created for an agent.
claimed_pod() {
    local agent=$1 deadline=$((SECONDS + 120)) found
    while :; do
        kc get sandboxclaims -l "curietech.ai/agent=$agent" -o json >"$WORKDIR/claims-$agent.json"
        if found="$(python3 - "$WORKDIR/claims-$agent.json" <<'PY'
import json, pathlib, sys
claims = [claim for claim in json.loads(pathlib.Path(sys.argv[1]).read_text())["items"]
          if not claim["metadata"].get("deletionTimestamp")]
assert len(claims) <= 1, "more than one live claim for one agent"
if not claims:
    raise SystemExit(3)
sandbox = ((claims[0].get("status") or {}).get("sandbox") or {}).get("name")
if not sandbox:
    raise SystemExit(3)
print(claims[0]["metadata"]["name"], sandbox)
PY
)"; then
            echo "$found"
            return 0
        fi
        (( SECONDS < deadline )) || fail "no bound SandboxClaim appeared for $agent"
        sleep 2
    done
}
# Writes FILL_BYTES of random data and fsyncs it. Exits nonzero if the pod is
# killed mid-write, which the control phase tolerates.
fill_workspace() {
    local pod=$1
    kc exec "$pod" -c runner -- python3 -c "
import os
path, size, chunk = '$FILL_PATH', $FILL_BYTES, 1 << 20
with open(path, 'wb') as handle:
    for _ in range(size // chunk):
        handle.write(os.urandom(chunk))
        handle.flush()
        os.fsync(handle.fileno())
        os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
print(os.stat(path).st_size)
"
}
write_control_workspace() {
    CONTROL_WRITE_STATUS=0
    fill_workspace "$CONTROL_POD" >"$WORKDIR/control-write.log" 2>&1 || CONTROL_WRITE_STATUS=$?
    log "control write exit status: $CONTROL_WRITE_STATUS"
    cat "$WORKDIR/control-write.log"
    printf '\n'
    if (( CONTROL_WRITE_STATUS != 0 )); then
        log "the control write ended with a nonzero exit (tolerated: the pod may be evicted mid-write)"
    fi
}
pod_state() {
    local pod=$1
    kc get pod "$pod" -o json >"$WORKDIR/pod-$pod.json" || return 1
    python3 - "$WORKDIR/pod-$pod.json" <<'PY'
import json, pathlib, sys
pod = json.loads(pathlib.Path(sys.argv[1]).read_text())
status = pod.get("status", {})
restarts = next((c.get("restartCount", 0) for c in status.get("containerStatuses", []) if c["name"] == "runner"), -1)
deleting = "deleting" if pod["metadata"].get("deletionTimestamp") else "live"
print(pod["metadata"]["uid"], status.get("phase", "Unknown"), status.get("reason") or "-", deleting, restarts)
PY
}
control_diagnostics() {
    local state="" current_uid="" pod_status=0 workspace_status=not-attempted events_status=not-attempted
    local file_bytes=""
    log "Collecting original control pod diagnostics before cleanup"
    state="$(pod_state "$CONTROL_POD" 2>/dev/null)" || pod_status=$?
    read -r current_uid _ <<<"$state"
    if [[ -n "${CONTROL_UID:-}" && "$current_uid" == "$CONTROL_UID" ]]; then
        workspace_status=0
        file_bytes="$(kc exec "$CONTROL_POD" -c runner -- python3 -c \
            "import os; print(os.stat('$FILL_PATH').st_size)" 2>/dev/null)" || workspace_status=$?
    fi
    if [[ -n "${CONTROL_UID:-}" ]]; then
        events_status=0
        kc get events --field-selector "involvedObject.uid=$CONTROL_UID" -o json \
            >"$WORKDIR/control-diagnostic-events.json" 2>/dev/null || events_status=$?
    fi
    python3 - "$WORKDIR" "$CONTROL_POD" "${CONTROL_UID:-}" "$pod_status" \
        "$workspace_status" "$file_bytes" "$events_status" "${CONTROL_WRITE_STATUS:-not-run}" <<'PY'
import json
import pathlib
import sys

workdir, pod_name, uid, pod_status, workspace_status, file_bytes, events_status, write_status = sys.argv[1:]


def read(name):
    try:
        return json.loads((pathlib.Path(workdir) / name).read_text())
    except (OSError, ValueError):
        return {}


def exit_status(value):
    return int(value) if value.isdigit() else None


def termination(value):
    return {key: value[key] for key in ("reason", "exitCode", "signal", "finishedAt") if key in value}


pod = None
snapshots = [(f"pod-{pod_name}.json", "live")] if pod_status == "0" else []
snapshots += [("control-status.json", "last-observed"), ("control-pod.json", "initial")]
for name, source in snapshots:
    document = read(name)
    if not uid or document.get("metadata", {}).get("uid") != uid:
        continue
    status = document.get("status", {})
    pod = {key: status.get(key) for key in ("phase", "reason", "message")}
    pod.update(uid=uid, source=source, deletionTimestamp=document["metadata"].get("deletionTimestamp"))
    pod["containers"] = [
        {"name": container.get("name"), "kind": kind, "restartCount": container.get("restartCount", 0),
         "waitingReason": container.get("state", {}).get("waiting", {}).get("reason"),
         "terminated": termination(container.get("state", {}).get("terminated", {})),
         "lastTerminated": termination(container.get("lastState", {}).get("terminated", {}))}
        for kind in ("containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses")
        for container in status.get(kind, [])
    ]
    break
events = None
if events_status == "0":
    events = [
        {key: event.get(key) for key in ("type", "reason", "message", "count", "firstTimestamp", "lastTimestamp")}
        for event in read("control-diagnostic-events.json").get("items", [])
        if uid and event.get("involvedObject", {}).get("uid") == uid
    ]
print("control diagnostics: " + json.dumps({
    "original_uid": uid or None, "pod": pod, "pod_read_exit_status": exit_status(pod_status),
    "write_exit_status": exit_status(write_status),
    "workspace_file_bytes": int(file_bytes) if workspace_status == "0" and file_bytes.isdigit() else None,
    "workspace_read_exit_status": exit_status(workspace_status),
    "events": events, "events_read_exit_status": exit_status(events_status),
}))
PY
}

# Phase 4: the factory agent's sandbox, alone.
log "Starting one real turn for $FACTORY_AGENT (the control agent has no claim yet)"
start_turn "$FACTORY_AGENT" C0EXAMPLE1
finish_turn "$FACTORY_AGENT"
[[ -z "$(kc get sandboxclaims -l "curietech.ai/agent=$CONTROL_AGENT" -o name)" ]] \
    || fail "a control claim exists during the factory phase"
found="$(claimed_pod "$FACTORY_AGENT")" || fail "no bound SandboxClaim for $FACTORY_AGENT"
read -r FACTORY_CLAIM FACTORY_POD <<<"$found"
kc wait --for=condition=Ready "pod/$FACTORY_POD" --timeout=180s >/dev/null \
    || fail "the factory runner pod did not become Ready"
[[ "$(kc get pod "$FACTORY_POD" -o jsonpath='{.metadata.labels.curietech\.ai/agent}')" == "$FACTORY_AGENT" ]] \
    || fail "the factory runner pod was not created from the $FACTORY_AGENT template"
kc get pod "$FACTORY_POD" -o json >"$WORKDIR/factory-pod.json"
python3 "$WORKDIR/envelope.py" "$WORKDIR/factory-pod.json" factory "factory runner pod"
state="$(pod_state "$FACTORY_POD")" || fail "could not read the factory runner pod"
read -r FACTORY_UID phase reason deleting FACTORY_RESTARTS <<<"$state"
[[ "$phase" == Running && "$deleting" == live ]] || fail "the factory runner pod is not Running before the write"
log "Writing $FILL_BYTES bytes (1280Mi) to $FILL_PATH in the factory runner"
written="$(fill_workspace "$FACTORY_POD")" || fail "the 1280Mi write failed in the factory sandbox"
[[ "$written" == "$FILL_BYTES" ]] || fail "the factory write produced $written bytes, not $FILL_BYTES"
log "OK: wrote 1280Mi; holding for at least ${HOLD_SECONDS}s with a check every ${HOLD_INTERVAL}s"
hold_started=$SECONDS
checks=0
min_checks=$((HOLD_SECONDS / HOLD_INTERVAL + 1))
while :; do
    state="$(pod_state "$FACTORY_POD")" || fail "the factory runner pod disappeared during the hold"
    read -r uid phase reason deleting restarts <<<"$state"
    [[ "$uid" == "$FACTORY_UID" ]] || fail "the factory runner pod was replaced during the hold"
    [[ "$phase" == Running ]] || fail "the factory runner pod left Running during the hold (phase $phase, reason $reason)"
    [[ "$reason" != Evicted && "$deleting" == live ]] || fail "the factory runner pod was evicted or deleted during the hold"
    [[ "$restarts" == "$FACTORY_RESTARTS" ]] || fail "the factory runner container restarted during the hold"
    [[ -z "$(kc get events --field-selector "involvedObject.uid=$FACTORY_UID,reason=Evicted" -o name)" ]] \
        || fail "an Evicted event names the factory runner pod"
    size="$(kc exec "$FACTORY_POD" -c runner -- python3 -c "import os; print(os.stat('$FILL_PATH').st_size)")" \
        || fail "could not read the factory workspace file during the hold"
    [[ "$size" == "$FILL_BYTES" ]] || fail "the factory workspace file is $size bytes, not $FILL_BYTES"
    checks=$((checks + 1))
    held=$((SECONDS - hold_started))
    log "hold check $checks: ${held}s, pod Running, not evicted, restarts $restarts, file $size bytes"
    if (( held >= HOLD_SECONDS && checks >= min_checks )); then
        break
    fi
    sleep "$HOLD_INTERVAL"
done
log "OK: the factory sandbox kept 1280Mi in /workspace for ${held}s across $checks checks on one Running pod"

# Phase 5: release the factory sandbox before the control claim exists.
log "Deleting the factory claim and waiting for its sandbox and pod to go"
kc delete sandboxclaim "$FACTORY_CLAIM" --wait=true --timeout=120s >/dev/null \
    || fail "could not delete the factory SandboxClaim"
kc wait --for=delete "pod/$FACTORY_POD" --timeout=120s >/dev/null 2>&1 || true
[[ -z "$(kc get "pod/$FACTORY_POD" --ignore-not-found -o name)" ]] || fail "the factory runner pod survived its claim"
deadline=$((SECONDS + 120))
until [[ -z "$(kc get "sandbox/$FACTORY_POD" --ignore-not-found -o name)" ]]; do
    (( SECONDS < deadline )) || fail "the factory Sandbox survived its claim"
    sleep 2
done
[[ -z "$(kc get pods -l "curietech.ai/agent=$FACTORY_AGENT" -o name)" ]] || fail "a factory runner pod remains"
[[ -z "$(kc get sandboxclaims -l "curietech.ai/agent=$FACTORY_AGENT" -o name)" ]] || fail "a factory claim remains"
log "OK: the factory claim, sandbox, and pod are gone"

# Phase 6: the standard control sandbox, alone.
log "Starting one real turn for $CONTROL_AGENT"
start_turn "$CONTROL_AGENT" C0EXAMPLE2
finish_turn "$CONTROL_AGENT"
found="$(claimed_pod "$CONTROL_AGENT")" || fail "no bound SandboxClaim for $CONTROL_AGENT"
read -r CONTROL_CLAIM CONTROL_POD <<<"$found"
CONTROL_UID="$(kc get pod "$CONTROL_POD" -o jsonpath='{.metadata.uid}' 2>/dev/null)" || CONTROL_UID=""
kc wait --for=condition=Ready "pod/$CONTROL_POD" --timeout=180s >/dev/null \
    || fail "the control runner pod did not become Ready"
kc get pod "$CONTROL_POD" -o json >"$WORKDIR/control-pod.json"
python3 "$WORKDIR/envelope.py" "$WORKDIR/control-pod.json" standard "control runner pod"
state="$(pod_state "$CONTROL_POD")" || fail "could not read the control runner pod"
read -r CONTROL_UID phase reason deleting restarts <<<"$state"
[[ "$phase" == Running ]] || fail "the control runner pod is not Running before the write"
log "Writing 1280Mi to $FILL_PATH in the control runner (claim $CONTROL_CLAIM); an exit from eviction is expected"
write_control_workspace
# The kubelet measures emptyDir usage periodically, so poll the pod status and
# its events (which outlive a pod the controller replaces) within a bound.
deadline=$((SECONDS + EVICTION_WAIT_SECONDS))
eviction=""
while :; do
    if kc get pod "$CONTROL_POD" -o json >"$WORKDIR/control-status.json" 2>/dev/null; then
        eviction="$(python3 - "$WORKDIR/control-status.json" "$CONTROL_UID" <<'PY'
import json, pathlib, sys
pod = json.loads(pathlib.Path(sys.argv[1]).read_text())
status = pod.get("status", {})
if pod["metadata"]["uid"] == sys.argv[2] and status.get("reason") == "Evicted":
    print(" ".join(str(status.get("message", "")).split()))
PY
)"
    fi
    if [[ -z "$eviction" ]]; then
        kc get events --field-selector "involvedObject.uid=$CONTROL_UID,reason=Evicted" -o json \
            >"$WORKDIR/control-events.json" 2>/dev/null || true
        if [[ -s "$WORKDIR/control-events.json" ]]; then
            eviction="$(python3 - "$WORKDIR/control-events.json" <<'PY'
import json, pathlib, sys
items = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("items", [])
if items:
    print(" ".join(str(items[-1].get("message", "")).split()))
PY
)"
        fi
    fi
    if [[ -n "$eviction" ]]; then
        break
    fi
    (( SECONDS < deadline )) || fail "the control runner pod was not evicted within ${EVICTION_WAIT_SECONDS}s of the 1280Mi write"
    sleep 2
done
log "control eviction: $eviction"
[[ "$eviction" == *'EmptyDir volume "workspace"'* ]] || fail "the eviction message does not name the workspace volume"
[[ "$eviction" == *'"1Gi"'* ]] || fail "the eviction message does not name the 1Gi workspace limit"
log "OK: the standard sandbox was evicted for exceeding its 1Gi workspace"
