#!/usr/bin/env bash
# #4717: rotating a hosted connector credential rolls that connector only.
# fix-pin-provision: candidate-stack
# The caller supplies the candidate CLI, context, run id, and loaded images.
# This scenario owns one namespace and one Helm release, then removes them.
set -euo pipefail
umask 077

fail() { echo "FAIL: $*" >&2; exit 1; }

for variable in CURIE_BIN CURIE_SCENARIO_CONTEXT CURIE_SCENARIO_RUN_ID \
    CURIE_SCENARIO_API_IMAGE CURIE_SCENARIO_WORKER_IMAGE CURIE_SCENARIO_RUNNER_IMAGE; do
    [[ -n "${!variable:-}" ]] || fail "$variable is required"
done
[[ -f "$CURIE_BIN" && -x "$CURIE_BIN" ]] || fail "CURIE_BIN must name an executable curie binary"
command -v python3 >/dev/null || fail "required executable is unavailable: python3"
python3 - <<'PY'
import os, pathlib, re, sys
def fail(variable, reason):
    print(f"FAIL: {variable} {reason}", file=sys.stderr)
    raise SystemExit(1)
context = os.environ["CURIE_SCENARIO_CONTEXT"]
if re.search(r"[\s\x00-\x1f\x7f]", context) or context.startswith("-"):
    fail("CURIE_SCENARIO_CONTEXT", "must name one kubeconfig context")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", os.environ["CURIE_SCENARIO_RUN_ID"]):
    fail("CURIE_SCENARIO_RUN_ID", "must be a short dns token")
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
    config = yaml.safe_load(path.read_text()) or {}
    entries = config.get("contexts", [])
    if not isinstance(entries, list):
        fail("CURIE_SCENARIO_CONTEXT", "cannot read the caller's kubeconfig contexts")
    for entry in entries:
        if isinstance(entry, dict) and isinstance(entry.get("name"), str):
            contexts.setdefault(entry["name"], entry.get("context"))
if not isinstance(contexts.get(context), dict):
    fail("CURIE_SCENARIO_CONTEXT", "is absent from the caller's kubeconfig")
PY
for command in kubectl helm docker kind; do
    command -v "$command" >/dev/null || fail "required executable is unavailable: $command"
done

CONTEXT="$CURIE_SCENARIO_CONTEXT"
RUN_ID="$CURIE_SCENARIO_RUN_ID"
NAMESPACE="rot-$RUN_ID"
RELEASE="rot-$RUN_ID"
FULLNAME="$RELEASE-curie"
AGENT="acme-bot"
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
WORKDIR="$(mktemp -d)"
CONNECTOR_DIGEST="busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
CONNECTOR_IMAGE="busybox:1.36.1"
API_PID=""
OWNED=0

kc() { kubectl --context "$CONTEXT" "$@"; }
kcn() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }

sanitize() {
    python3 - "$1" <<'PY'
import os, pathlib, re, sys
path = pathlib.Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
text = path.read_text(errors="replace")
for key in ("ROTATE_TOKEN", "ROTATE_FILE", "ROTATE_SETTING", "OTHER_TOKEN", "API_KEY", "CURIE_API_KEY"):
    value = os.environ.get(key, "")
    if value:
        text = text.replace(value, "[redacted]")
text = re.sub(r"curie\.dev/connector-secret-digest\"?\s*[:=]\s*\"?[0-9a-f]{64}", "curie.dev/connector-secret-digest=[redacted]", text)
text = re.sub(r"\b[0-9a-f]{64}\b", "[redacted]", text)
sys.stdout.write(text[-4000:])
PY
}

cleanup() {
    local status=$?
    if [[ -n "$API_PID" ]]; then
        kill "$API_PID" >/dev/null 2>&1 || true
    fi
    if (( OWNED )); then
        "$CURIE_BIN" cluster --context "$CONTEXT" down \
            --namespace "$NAMESPACE" --release "$RELEASE" --yes \
            >"$WORKDIR/down.out" 2>"$WORKDIR/down.err" || true
        kc delete namespace "$NAMESPACE" --wait=true --timeout=180s \
            >"$WORKDIR/ns-delete.out" 2>"$WORKDIR/ns-delete.err" || true
        if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
            echo "FAIL: owned namespace remains" >&2
            status=1
        fi
    fi
    rm -rf "$WORKDIR"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
    fail "namespace $NAMESPACE already exists"
fi

export CONNECTOR_IMAGE
python3 - "$WORKDIR" <<PY
import os, pathlib
root = pathlib.Path(${WORKDIR@Q})
plugin = root / "plugin"
(plugin / ".claude-plugin").mkdir(parents=True)
(plugin / "skills" / "rotate").mkdir(parents=True)
(plugin / ".claude-plugin" / "plugin.json").write_text(
    '{"name":"acme-rotate","version":"0.0.1","description":"Rotation fixture."}\n'
)
(plugin / "skills" / "rotate" / "SKILL.md").write_text(
    "---\nname: rotate\ndescription: Reports that the fixture is installed.\n---\n\nReport ready.\n"
)
image = os.environ["CONNECTOR_IMAGE"]
(plugin / "connectors.yaml").write_text(f"""connectors:
  rotate:
    image: {image}
    port: 8080
    args: ["sh", "-c", "httpd -f -p 8080 -h /etc"]
    secrets: [ROTATE_TOKEN]
    config: [ROTATE_SETTING]
    secret_files:
      ROTATE_FILE: /secrets/rotate-file
  other:
    image: {image}
    port: 8080
    args: ["sh", "-c", "httpd -f -p 8080 -h /etc"]
    secrets: [OTHER_TOKEN]
""")
PY
# Generate sentinels after the bundle exists. Values stay out of argv.
SENTINEL_FILE="$WORKDIR/sentinels.env"
python3 - "$SENTINEL_FILE" <<'PY'
import pathlib, secrets, sys
path = pathlib.Path(sys.argv[1])
lines = []
for name in ("ROTATE_TOKEN", "ROTATE_FILE", "ROTATE_SETTING", "OTHER_TOKEN"):
    lines.append(f"{name}={secrets.token_hex(16)}")
path.write_text("\n".join(lines) + "\n")
os_chmod = __import__("os").chmod
os_chmod(path, 0o600)
PY
# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$SENTINEL_FILE"
set +a
export CONNECTOR_IMAGE

CLUSTER="${CONTEXT#kind-}"
# A one-line build drops the multi-arch index. kind and ctr both reject that
# index when only one platform was pulled.
printf 'FROM %s\n' "$CONNECTOR_DIGEST" | docker build --platform linux/amd64 \
    --tag "$CONNECTOR_IMAGE" - >"$WORKDIR/pull.log" 2>&1 || {
    sanitize "$WORKDIR/pull.log"
    fail "could not pull the connector image"
}
docker save "$CONNECTOR_IMAGE" | docker exec -i "${CLUSTER}-control-plane" \
    ctr --namespace=k8s.io images import - >"$WORKDIR/load.log" 2>&1 || {
    sanitize "$WORKDIR/load.log"
    fail "could not load the connector image"
}

python3 -c 'import secrets; print(secrets.token_hex(24), end="")' >"$WORKDIR/api-key"
chmod 600 "$WORKDIR/api-key"
API_KEY="$(cat "$WORKDIR/api-key")"
export API_KEY

OWNED=1
"$CURIE_BIN" cluster --context "$CONTEXT" up \
    --namespace "$NAMESPACE" \
    --release "$RELEASE" \
    --chart "$CHART" \
    --dev \
    --fake-model \
    --no-expose \
    --set security.gvisor.mode=off \
    --set dispatcher.deploy=false \
    --set ui.deploy=false \
    --set langfuse.deploy=false \
    --set langfuse.host=langfuse.example.com \
    --set clickhouse.deploy=false \
    --set mailAdapter.deploy=false \
    --set otelCollector.deploy=false \
    --set otelCollector.telemetryDisabled=true \
    --set "api.apiKey=${API_KEY}" \
    --set worker.connectorReconciler.enabled=true \
    --set worker.connectorReconciler.intervalSeconds=1 \
    --set api.image.repository="${CURIE_SCENARIO_API_IMAGE%:*}" \
    --set "api.image.tag=${CURIE_SCENARIO_API_IMAGE##*:}" \
    --set api.image.pullPolicy=Never \
    --set worker.image.repository="${CURIE_SCENARIO_WORKER_IMAGE%:*}" \
    --set "worker.image.tag=${CURIE_SCENARIO_WORKER_IMAGE##*:}" \
    --set worker.image.pullPolicy=Never \
    --set agentSandbox.runner.image="${CURIE_SCENARIO_RUNNER_IMAGE%:*}" \
    --set "agentSandbox.runner.tag=${CURIE_SCENARIO_RUNNER_IMAGE##*:}" \
    --set agentSandbox.runner.imagePullPolicy=Never \
    --set agentSandbox.runner.prewarm.imagePullPolicy=Never \
    >"$WORKDIR/up.out" 2>"$WORKDIR/up.err" || {
        sanitize "$WORKDIR/up.err"
        fail "cluster up failed"
    }
kcn rollout status "deploy/${FULLNAME}-api" --timeout=300s >/dev/null
kcn rollout status "deploy/${FULLNAME}-worker" --timeout=300s >/dev/null

kcn port-forward "svc/${FULLNAME}-api" ":8000" >"$WORKDIR/forward.log" 2>&1 &
API_PID=$!
API_PORT=""
for _ in $(seq 1 60); do
    API_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) .*/\1/p' "$WORKDIR/forward.log" | head -1)"
    if [[ -n "$API_PORT" ]] && curl -fsS -o /dev/null "http://127.0.0.1:${API_PORT}/health"; then
        break
    fi
    sleep 1
done
[[ -n "$API_PORT" ]] || { sanitize "$WORKDIR/forward.log"; fail "API port-forward did not become ready"; }

deploy() {
    local label="$1"
    # Pass the release key explicitly. A loopback --api-url would replace the
    # chart's default dev key with a stored local key.
    env CURIE_API_KEY="$API_KEY" "$CURIE_BIN" --json cluster --context "$CONTEXT" deploy \
        --plugin-dir "$WORKDIR/plugin" \
        --agent "$AGENT" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --api-url "http://127.0.0.1:${API_PORT}" \
        >"$WORKDIR/deploy-$label.json" 2>"$WORKDIR/deploy-$label.err" || {
            sanitize "$WORKDIR/deploy-$label.err"
            sanitize "$WORKDIR/deploy-$label.json"
            fail "cluster deploy failed"
        }
}

pod_field() {
    local connector="$1" field="$2"
    kcn get pod -l "app.kubernetes.io/name=${RELEASE}-${AGENT}-mcp-${connector}" \
        -o "jsonpath=${field}"
}

expect_hash() {
    local name="$1"
    python3 - "$name" <<'PY'
import hashlib, os, sys
print(hashlib.sha256(os.environ[sys.argv[1]].encode()).hexdigest())
PY
}

container_hash() {
    local connector="$1" mode="$2"
    local pod
    pod="$(pod_field "$connector" '{.items[0].metadata.name}')"
    [[ -n "$pod" ]] || fail "no pod for $connector"
    if [[ "$mode" == file ]]; then
        kcn exec "$pod" -- sha256sum /secrets/rotate-file | awk '{print $1}'
    else
        kcn exec "$pod" -- sh -c "printf %s \"\$$mode\" | sha256sum" | awk '{print $1}'
    fi
}

assert_content() {
    local connector="$1" mode="$2" name="$3"
    local got expected
    got="$(container_hash "$connector" "$mode")"
    expected="$(expect_hash "$name")"
    if [[ "$got" != "$expected" ]]; then
        fail "connector $connector did not observe the rotated $mode input"
    fi
}

snapshot() {
    local connector="$1"
    printf '%s %s\n' \
        "$(pod_field "$connector" '{.items[0].metadata.uid}')" \
        "$(pod_field "$connector" '{.items[0].metadata.labels.pod-template-hash}')"
}

deploy initial
A_OLD="$(snapshot rotate)"
B_OLD="$(snapshot other)"
[[ -n "${A_OLD%% *}" && -n "${B_OLD%% *}" ]] || fail "initial pods were not recorded"
assert_content rotate ROTATE_TOKEN ROTATE_TOKEN
assert_content rotate ROTATE_SETTING ROTATE_SETTING
assert_content rotate file ROTATE_FILE
assert_content other OTHER_TOKEN OTHER_TOKEN

rotate_one() {
    local name="$1" mode="$2" label="$3"
    python3 - "$SENTINEL_FILE" "$name" <<'PY'
import pathlib, secrets, sys
path, name = sys.argv[1], sys.argv[2]
lines = pathlib.Path(path).read_text().splitlines()
out = []
for line in lines:
    key, _, _value = line.partition("=")
    if key == name:
        out.append(f"{key}={secrets.token_hex(16)}")
    else:
        out.append(line)
pathlib.Path(path).write_text("\n".join(out) + "\n")
PY
    set -a
    # shellcheck source=/dev/null
    source "$SENTINEL_FILE"
    set +a
    deploy "$label"
    local a_new b_new
    a_new="$(snapshot rotate)"
    b_new="$(snapshot other)"
    if [[ "$a_new" == "$A_OLD" ]]; then
        fail "connector A did not roll after $label"
    fi
    if [[ "$b_new" != "$B_OLD" ]]; then
        fail "connector B rolled after $label"
    fi
    assert_content rotate "$mode" "$name"
    A_OLD="$a_new"
}

rotate_one ROTATE_TOKEN ROTATE_TOKEN token
rotate_one ROTATE_SETTING ROTATE_SETTING setting
rotate_one ROTATE_FILE file file

deploy unchanged
A_NEW="$(snapshot rotate)"
B_NEW="$(snapshot other)"
if [[ "$A_NEW" != "$A_OLD" || "$B_NEW" != "$B_OLD" ]]; then
    fail "an unchanged deploy rolled a connector"
fi

DEPLOY="deploy/${RELEASE}-${AGENT}-mcp-rotate"
fingerprint() {
    kcn get "$DEPLOY" -o jsonpath='{.spec.template.metadata.annotations.curie\.dev/connector-secret-digest}'
}
wait_hash() {
    local i
    for i in $(seq 1 60); do
        if [[ -n "$(kcn get "$DEPLOY" -o jsonpath='{.metadata.annotations.curie\.dev/connector-hash}' 2>/dev/null || true)" ]]; then
            return 0
        fi
        sleep 1
    done
    fail "the worker did not adopt the connector deployment"
}
wait_hash
BEFORE_FP="$(fingerprint)"
BEFORE_UID="$(pod_field rotate '{.items[0].metadata.uid}')"
kcn label "$DEPLOY" app.kubernetes.io/component=edited --overwrite >/dev/null
restored=0
for _ in $(seq 1 60); do
    current="$(kcn get "$DEPLOY" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/component}')"
    if [[ "$current" == "mcp-connector" ]]; then
        restored=1
        break
    fi
    sleep 1
done
(( restored )) || fail "the worker did not restore the connector label"
AFTER_FP="$(fingerprint)"
AFTER_UID="$(pod_field rotate '{.items[0].metadata.uid}')"
if [[ "$AFTER_FP" != "$BEFORE_FP" || "$AFTER_UID" != "$BEFORE_UID" ]]; then
    fail "the reconciler apply changed the connector fingerprint or pod"
fi
sleep 5
if [[ "$(fingerprint)" != "$BEFORE_FP" || "$(pod_field rotate '{.items[0].metadata.uid}')" != "$BEFORE_UID" ]]; then
    fail "the connector fingerprint or pod was not stable"
fi
echo "connector rotation rolled only the consumer"
