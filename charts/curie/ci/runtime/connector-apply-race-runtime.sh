#!/usr/bin/env bash
# #5131: cluster deploy survives the connector reconciler creating the same
# object between the CLI's read and its create.
# fix-pin-provision: candidate-stack
# The caller supplies the candidate CLI, context, run id, and loaded images.
# This scenario owns a hold namespace and a cluster scoped admission webhook;
# the caller owns the installed workload release.
# The webhook forces the interleaving: the CLI's create of one connector
# Service is held in admission until the reconciler's create of it commits.
set -euo pipefail
umask 077

fail() { echo "FAIL: $*" >&2; exit 1; }

for variable in CURIE_BIN CURIE_SCENARIO_CONTEXT CURIE_SCENARIO_RUN_ID \
    CURIE_SCENARIO_API_IMAGE CURIE_SCENARIO_WORKER_IMAGE CURIE_SCENARIO_RUNNER_IMAGE \
    CURIE_SCENARIO_NAMESPACE CURIE_SCENARIO_RELEASE; do
    [[ -n "${!variable:-}" ]] || fail "$variable is required"
done
[[ -f "$CURIE_BIN" && -x "$CURIE_BIN" ]] || fail "CURIE_BIN must name an executable curie binary"
command -v python3 >/dev/null || fail "required executable is unavailable: python3"
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
for variable, maximum in (("CURIE_SCENARIO_NAMESPACE", 63), ("CURIE_SCENARIO_RELEASE", 53)):
    value = os.environ[variable]
    if len(value) > maximum or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", value):
        fail(variable, f"must be a DNS label of at most {maximum} characters")
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
for command in kubectl docker openssl curl; do
    command -v "$command" >/dev/null || fail "required executable is unavailable: $command"
done

CONTEXT="$CURIE_SCENARIO_CONTEXT"
RUN_ID="$CURIE_SCENARIO_RUN_ID"
NAMESPACE="$CURIE_SCENARIO_NAMESPACE"
RELEASE="$CURIE_SCENARIO_RELEASE"
FULLNAME="$RELEASE-curie"
AGENT="acme-race"
OWNER_SELECTOR="curie.dev/connector-owner=$AGENT"
HOLD_NS="race-$RUN_ID-hold"
WEBHOOK_CONFIG="connector-apply-race-$RUN_ID"
HELD_SERVICE="${RELEASE}-${AGENT}-mcp-rotate-direct"
LABELLED_SERVICE="${RELEASE}-${AGENT}-mcp-rotate"
PROBE_SERVICE="${RELEASE}-hold-probe"
UNLABELLED="${RELEASE}-unlabelled"
HAND_LABEL_KEY="example.com/operator-note"
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKDIR="$(mktemp -d)"
CONNECTOR_DIGEST="busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
CONNECTOR_IMAGE="busybox:1.36.1"
API_PID=""
AGENT_TOUCHED=0
HOLD_OWNED=0

kc() { kubectl --context "$CONTEXT" "$@"; }
kcn() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }

sanitize() {
    python3 - "$1" <<'PY'
import os, pathlib, re, sys
path = pathlib.Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
text = path.read_text(errors="replace")
for key in ("ROTATE_TOKEN", "OTHER_TOKEN", "API_KEY", "CURIE_API_KEY"):
    value = os.environ.get(key, "")
    if value:
        text = text.replace(value, "[redacted]")
text = re.sub(r"curie\.dev/connector-secret-digest\"?\s*[:=]\s*\"?[0-9a-f]{64}", "curie.dev/connector-secret-digest=[redacted]", text)
text = re.sub(r"\b[0-9a-f]{64}\b", "[redacted]", text)
sys.stdout.write(text[-4000:])
PY
}

remove_hold() {
    local remaining=0
    kc delete validatingwebhookconfiguration "$WEBHOOK_CONFIG" --ignore-not-found --wait=true --timeout=60s \
        >"$WORKDIR/vwc-delete.out" 2>"$WORKDIR/vwc-delete.err" || remaining=1
    local webhook
    if ! webhook="$(kc get validatingwebhookconfiguration "$WEBHOOK_CONFIG" --ignore-not-found -o name \
        2>"$WORKDIR/vwc-verify.err")"; then
        echo "FAIL: could not verify owned ValidatingWebhookConfiguration deletion" >&2
        remaining=1
    elif [[ -n "$webhook" ]]; then
        echo "FAIL: owned ValidatingWebhookConfiguration remains" >&2
        remaining=1
    fi
    (( remaining == 0 )) || return 1
    kc delete namespace "$HOLD_NS" --ignore-not-found --wait=false \
        >"$WORKDIR/hold-delete.out" 2>"$WORKDIR/hold-delete.err" || return 1
    HOLD_OWNED=0
    return 0
}

cleanup() {
    local status=$?
    # The webhook goes first: failurePolicy Fail must never outlive its pod.
    if (( HOLD_OWNED )); then
        remove_hold || status=1
    fi
    if (( AGENT_TOUCHED )); then
        env CURIE_API_KEY="$API_KEY" "$CURIE_BIN" cluster --context "$CONTEXT" delete "$AGENT" --yes \
            --namespace "$NAMESPACE" --release "$RELEASE" --api-url "http://127.0.0.1:$API_PORT" \
            >"$WORKDIR/delete-agent.out" 2>"$WORKDIR/delete-agent.err" \
            || { echo "FAIL: could not remove the scenario connector agent" >&2; status=1; }
        kcn delete deployment,service,networkpolicy,secret -l "curie.dev/connector-owner=$AGENT" \
            --ignore-not-found --wait=true --timeout=90s >"$WORKDIR/delete-connectors.out" 2>"$WORKDIR/delete-connectors.err" \
            || { echo "FAIL: could not remove the scenario connector objects" >&2; status=1; }
    fi
    if [[ -n "$API_PID" ]]; then
        kill "$API_PID" >/dev/null 2>&1 || true
        wait "$API_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKDIR"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for owned in "namespace $HOLD_NS" \
    "validatingwebhookconfiguration $WEBHOOK_CONFIG"; do
    # shellcheck disable=SC2086
    if kc get $owned >/dev/null 2>&1; then
        fail "$owned already exists"
    fi
done

write_bundle() {
    local with_extra="$1"
    python3 - "$WORKDIR" "$with_extra" <<'PY'
import os, pathlib, sys
root, with_extra = pathlib.Path(sys.argv[1]), sys.argv[2] == "1"
plugin = root / "plugin"
(plugin / ".claude-plugin").mkdir(parents=True, exist_ok=True)
(plugin / "skills" / "race").mkdir(parents=True, exist_ok=True)
(plugin / ".claude-plugin" / "plugin.json").write_text(
    '{"name":"acme-race","version":"0.0.1","description":"Connector apply race fixture."}\n'
)
(plugin / "skills" / "race" / "SKILL.md").write_text(
    "---\nname: race\ndescription: Reports that the fixture is installed.\n---\n\nReport ready.\n"
)
image = os.environ["CONNECTOR_IMAGE"]
server = f"""    image: {image}
    port: 8080
    args: ["sh", "-c", "httpd -f -p 8080 -h /etc"]
"""
text = "connectors:\n"
text += "  rotate:\n" + server + "    secrets: [ROTATE_TOKEN]\n"
text += "  other:\n" + server + "    secrets: [OTHER_TOKEN]\n"
if with_extra:
    text += "  extra:\n" + server
(plugin / "connectors.yaml").write_text(text)
PY
}

export CONNECTOR_IMAGE
write_bundle 0
# Generate sentinels after the bundle exists. Values stay out of argv.
SENTINEL_FILE="$WORKDIR/sentinels.env"
python3 - "$SENTINEL_FILE" <<'PY'
import os, pathlib, secrets, sys
path = pathlib.Path(sys.argv[1])
path.write_text("".join(f"{name}={secrets.token_hex(16)}\n" for name in ("ROTATE_TOKEN", "OTHER_TOKEN")))
os.chmod(path, 0o600)
PY
set -a
# shellcheck source=/dev/null
source "$SENTINEL_FILE"
set +a

CLUSTER="${CONTEXT#kind-}"
# Keep the connector image in Docker's single-platform image format for import.
# The second connector scenario reuses the first scenario's conversion.
source_digest="$(docker image inspect "$CONNECTOR_IMAGE" \
    --format '{{index .Config.Labels "curie.dev/connector-source"}}' 2>/dev/null || true)"
if [[ "$source_digest" != "$CONNECTOR_DIGEST" ]]; then
    printf 'FROM %s\nLABEL curie.dev/connector-source=%s\n' "$CONNECTOR_DIGEST" "$CONNECTOR_DIGEST" \
        | docker build --platform linux/amd64 --tag "$CONNECTOR_IMAGE" - >"$WORKDIR/pull.log" 2>&1 || {
        sanitize "$WORKDIR/pull.log"
        fail "could not pull the connector image"
    }
fi
docker save "$CONNECTOR_IMAGE" | docker exec -i "${CLUSTER}-control-plane" \
    ctr --namespace=k8s.io images import - >"$WORKDIR/load.log" 2>&1 || {
    sanitize "$WORKDIR/load.log"
    fail "could not load the connector image"
}

kcn get secret "$FULLNAME-secrets" -o jsonpath='{.data.apiKey}' \
    | python3 -c 'import base64, sys; key=base64.b64decode(sys.stdin.read(), validate=True); assert key.strip(), "installed API key is empty"; sys.stdout.buffer.write(key)' \
        >"$WORKDIR/api-key" || fail "could not read the installed API key"
API_KEY="$(cat "$WORKDIR/api-key")"
export API_KEY

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
    AGENT_TOUCHED=1
    # Pass the release key explicitly. A loopback --api-url would replace the
    # chart's default dev key with a stored local key.
    env "CURIE_API_KEY=$API_KEY" "$CURIE_BIN" --json cluster --context "$CONTEXT" deploy \
        --plugin-dir "$WORKDIR/plugin" \
        --agent "$AGENT" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --api-url "http://127.0.0.1:${API_PORT}" \
        >"$WORKDIR/deploy-$label.json" 2>"$WORKDIR/deploy-$label.err"
}

deploy_or_fail() {
    local label="$1"
    deploy "$label" || {
        sanitize "$WORKDIR/deploy-$label.err"
        sanitize "$WORKDIR/deploy-$label.json"
        fail "cluster deploy ($label) failed"
    }
}

# Sorted field managers of one object. kubectl hides managedFields by default.
managers() {
    kcn get "$@" --show-managed-fields -o json | python3 -c '
import json, sys
fields = json.load(sys.stdin)["metadata"].get("managedFields") or []
print(" ".join(sorted({entry.get("manager", "") for entry in fields})))
'
}

resource_version() {
    kcn get "$@" -o jsonpath='{.metadata.resourceVersion}'
}

cluster_ip() {
    kcn get service "$HELD_SERVICE" -o jsonpath='{.spec.clusterIP}'
}

hand_label() {
    kcn get service "$LABELLED_SERVICE" -o jsonpath='{.metadata.labels.example\.com/operator-note}'
}

assert_ready() {
    local connector="$1"
    kcn rollout status "deploy/${RELEASE}-${AGENT}-mcp-${connector}" --timeout=120s \
        >/dev/null 2>"$WORKDIR/rollout-$connector.err" || {
        sanitize "$WORKDIR/rollout-$connector.err"
        fail "connector $connector is not Ready"
    }
}

# Phase A: objects without the connector owner label, which no prune or
# server-side apply may touch.
kcn create configmap "$UNLABELLED" --from-literal=note=kept >/dev/null
kcn create service clusterip "$UNLABELLED" --tcp=80:80 >/dev/null
UNLABELLED_CM_RV="$(resource_version configmap "$UNLABELLED")"
UNLABELLED_SVC_RV="$(resource_version service "$UNLABELLED")"
[[ -n "$UNLABELLED_CM_RV" && -n "$UNLABELLED_SVC_RV" ]] || fail "unlabelled objects were not recorded"

# The hold webhook. It runs as the API image's python3 in its own namespace,
# outside the chart's NetworkPolicies, and may only read Services here.
CLI_USER="$(kc auth whoami -o jsonpath='{.status.userInfo.username}')"
[[ "$CLI_USER" =~ ^[A-Za-z0-9@:._-]+$ ]] || fail "harness: could not read the CLI's Kubernetes user name"
export CLI_USER NAMESPACE HOLD_NS HELD_SERVICE PROBE_SERVICE WEBHOOK_CONFIG CURIE_SCENARIO_API_IMAGE

cat >"$WORKDIR/hold_webhook.py" <<'PY'
"""Two-sided hold on the CREATE of one connector Service.

A CREATE by the CLI user is held until the Service exists, which only another
writer can make true. A CREATE by anyone else (the connector reconciler) is
held until the CLI's CREATE has arrived. Together they force: CLI create in
admission, reconciler create commits, CLI create released. Each hold is bounded
and every request is allowed; this webhook never denies.
"""
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BOUND = 15.0
CLI_USER = os.environ["CLI_USER"]
PROBE = os.environ["PROBE_SERVICE"]
SA = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "https://kubernetes.default.svc"
cli_arrived = threading.Event()


def managers_of(metadata):
    return sorted({entry.get("manager", "") for entry in metadata.get("managedFields") or []})


def live_managers(namespace, name):
    with open(f"{SA}/token", encoding="utf-8") as handle:
        token = handle.read().strip()
    context = ssl.create_default_context(cafile=f"{SA}/ca.crt")
    request = urllib.request.Request(
        f"{API}/api/v1/namespaces/{namespace}/services/{name}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=5) as response:
            return managers_of(json.load(response)["metadata"])
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        review = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        request = review["request"]
        metadata = (request.get("object") or {}).get("metadata") or {}
        name = metadata.get("name") or request.get("name", "")
        namespace = request.get("namespace", "")
        user = (request.get("userInfo") or {}).get("username", "")
        start = time.monotonic()
        record = {"name": name, "user": user, "dry_run": bool(request.get("dryRun"))}
        if name == PROBE:
            record.update(branch="probe", met=True, managers=[])
        elif user == CLI_USER:
            cli_arrived.set()
            seen, error = None, None
            while time.monotonic() - start < BOUND:
                try:
                    seen = live_managers(namespace, name)
                except Exception as exc:  # keep holding; the log names it
                    error = f"{type(exc).__name__}: {exc}"
                if seen is not None:
                    break
                time.sleep(0.1)
            record.update(branch="cli", met=seen is not None, managers=seen or [], error=error)
        else:
            met = cli_arrived.wait(BOUND)
            record.update(branch="other", met=met, managers=managers_of(metadata))
        record["outcome"] = "condition met" if record["met"] else "bound expired"
        record["waited"] = round(time.monotonic() - start, 2)
        print("HOLD " + json.dumps(record, sort_keys=True), flush=True)
        body = json.dumps({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {"uid": request["uid"], "allowed": True},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


server = ThreadingHTTPServer(("0.0.0.0", 8443), Handler)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
server.socket = tls.wrap_socket(server.socket, server_side=True)
print("hold webhook ready", flush=True)
server.serve_forever()
PY

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=connector-apply-race-hold" \
    -addext "subjectAltName=DNS:hold.${HOLD_NS}.svc" \
    -keyout "$WORKDIR/tls.key" -out "$WORKDIR/tls.crt" >"$WORKDIR/openssl.log" 2>&1 || {
    sanitize "$WORKDIR/openssl.log"
    fail "harness: could not create the hold webhook certificate"
}

python3 - "$WORKDIR/tls.crt" "$WORKDIR/hold.json" "$WORKDIR/vwc.json" <<'PY'
import base64, json, os, pathlib, sys
cert, workload_out, webhook_out = (pathlib.Path(arg) for arg in sys.argv[1:4])
env = os.environ
hold_ns, namespace = env["HOLD_NS"], env["NAMESPACE"]
labels = {"app": "hold"}
workload = {"apiVersion": "v1", "kind": "List", "items": [
    {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "hold", "namespace": hold_ns}},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
     "metadata": {"name": "connector-apply-race-hold", "namespace": namespace},
     "rules": [{"apiGroups": [""], "resources": ["services"], "verbs": ["get"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
     "metadata": {"name": "connector-apply-race-hold", "namespace": namespace},
     "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "connector-apply-race-hold"},
     "subjects": [{"kind": "ServiceAccount", "name": "hold", "namespace": hold_ns}]},
    {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "hold", "namespace": hold_ns},
     "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {
         "metadata": {"labels": labels},
         "spec": {
             "serviceAccountName": "hold",
             "terminationGracePeriodSeconds": 1,
             "containers": [{
                 "name": "hold",
                 "image": env["CURIE_SCENARIO_API_IMAGE"],
                 "imagePullPolicy": "Never",
                 "command": ["python3", "-u", "/src/hold_webhook.py"],
                 "env": [
                     {"name": "CLI_USER", "value": env["CLI_USER"]},
                     {"name": "PROBE_SERVICE", "value": env["PROBE_SERVICE"]},
                 ],
                 "ports": [{"containerPort": 8443}],
                 "readinessProbe": {"tcpSocket": {"port": 8443}, "periodSeconds": 1},
                 "volumeMounts": [
                     {"name": "tls", "mountPath": "/tls", "readOnly": True},
                     {"name": "src", "mountPath": "/src", "readOnly": True},
                 ],
             }],
             "volumes": [
                 {"name": "tls", "secret": {"secretName": "hold-tls", "defaultMode": 0o444}},
                 {"name": "src", "configMap": {"name": "hold-src", "defaultMode": 0o444}},
             ],
         }}}},
    {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "hold", "namespace": hold_ns},
     "spec": {"selector": labels, "ports": [{"port": 443, "targetPort": 8443}]}},
]}
# The probe name exists only so the script can prove the API server reaches
# the webhook before the real deploy. It is created with --dry-run=server.
expression = (
    f'object.metadata.name == "{env["HELD_SERVICE"]}"'
    f' || object.metadata.name == "{env["PROBE_SERVICE"]}"'
)
webhook = {
    "apiVersion": "admissionregistration.k8s.io/v1",
    "kind": "ValidatingWebhookConfiguration",
    "metadata": {"name": env["WEBHOOK_CONFIG"]},
    "webhooks": [{
        "name": "hold.connector-apply-race.example.com",
        "admissionReviewVersions": ["v1"],
        "sideEffects": "None",
        "failurePolicy": "Fail",
        "timeoutSeconds": 25,
        "clientConfig": {
            "caBundle": base64.b64encode(cert.read_bytes()).decode(),
            "service": {"name": "hold", "namespace": hold_ns, "path": "/validate", "port": 443},
        },
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
        "rules": [{"operations": ["CREATE"], "apiGroups": [""], "apiVersions": ["v1"],
                   "resources": ["services"], "scope": "Namespaced"}],
        "matchConditions": [{"name": "held-connector-service", "expression": expression}],
    }],
}
workload_out.write_text(json.dumps(workload))
webhook_out.write_text(json.dumps(webhook))
PY

HOLD_OWNED=1
kc create namespace "$HOLD_NS" >/dev/null
kc -n "$HOLD_NS" create secret tls hold-tls \
    --cert "$WORKDIR/tls.crt" --key "$WORKDIR/tls.key" >/dev/null
kc -n "$HOLD_NS" create configmap hold-src \
    --from-file=hold_webhook.py="$WORKDIR/hold_webhook.py" >/dev/null
kc apply -f "$WORKDIR/hold.json" >"$WORKDIR/hold-apply.log" 2>&1 || {
    sanitize "$WORKDIR/hold-apply.log"
    fail "harness: could not install the hold webhook workload"
}
kc -n "$HOLD_NS" rollout status deploy/hold --timeout=180s >/dev/null 2>"$WORKDIR/hold-rollout.err" || {
    sanitize "$WORKDIR/hold-rollout.err"
    fail "harness: the hold webhook did not become ready"
}
kc apply -f "$WORKDIR/vwc.json" >"$WORKDIR/vwc-apply.log" 2>&1 || {
    sanitize "$WORKDIR/vwc-apply.log"
    fail "harness: could not register the hold webhook"
}

hold_log() {
    kc -n "$HOLD_NS" logs deploy/hold --tail=-1 2>/dev/null || true
}

# Prove the API server reaches the webhook before the deploy relies on it.
probe_ok=0
for _ in $(seq 1 60); do
    if kcn create service clusterip "$PROBE_SERVICE" --tcp=80:80 --dry-run=server \
        >/dev/null 2>"$WORKDIR/probe.err" && [[ "$(hold_log)" == *'"branch": "probe"'* ]]; then
        probe_ok=1
        break
    fi
    sleep 1
done
(( probe_ok )) || { sanitize "$WORKDIR/probe.err"; fail "harness: the API server never reached the hold webhook"; }

hold_records() {
    hold_log | python3 -c '
import json, sys
for line in sys.stdin:
    if line.startswith("HOLD "):
        record = json.loads(line[5:])
        if record.get("branch") != "probe":
            print(json.dumps(record, sort_keys=True))
'
}

# Phase B: the first deploy, with the CLI's create of the held Service forced
# to land after the reconciler's.
if ! deploy initial; then
    sanitize "$WORKDIR/deploy-initial.err"
    sanitize "$WORKDIR/deploy-initial.json"
    echo "hold webhook records:" >&2
    hold_records >&2 || true
    if grep -q "AlreadyExists" "$WORKDIR/deploy-initial.err" "$WORKDIR/deploy-initial.json"; then
        fail "first cluster deploy failed with AlreadyExists after the reconciler created a connector object mid-apply"
    fi
    fail "first cluster deploy failed under the forced interleaving"
fi

hold_records >"$WORKDIR/hold-records.json"
python3 - "$WORKDIR/hold-records.json" <<'PY' || { cat "$WORKDIR/hold-records.json" >&2; exit 1; }
import json, sys
records = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
cli = [r for r in records if r["branch"] == "cli"]
other = [r for r in records if r["branch"] == "other"]
if not any(r["met"] and "curie-connector-reconciler" in r["managers"] for r in cli):
    print("FAIL: harness: the CLI create was never held until the reconciler's create of the Service committed", file=sys.stderr)
    raise SystemExit(1)
if not any(r["met"] for r in other):
    print("FAIL: harness: no other writer's create waited for the CLI create", file=sys.stderr)
    raise SystemExit(1)
print(f"forced interleaving observed: {len(cli)} held CLI create(s), {len(other)} held reconciler create(s)")
PY
remove_hold || fail "harness: the hold webhook could not be removed"

direct_managers="$(managers service "$HELD_SERVICE")"
for manager in curie-cli-connectors curie-connector-reconciler; do
    [[ " $direct_managers " == *" $manager "* ]] || \
        fail "service $HELD_SERVICE lacks field manager $manager (has: $direct_managers)"
done

kcn get deployment,service,networkpolicy,secret -l "$OWNER_SELECTOR" --show-managed-fields -o json \
    | python3 -c '
import collections, json, sys
items = json.load(sys.stdin).get("items") or []
counts = collections.Counter(item["kind"] for item in items)
problems = []
for kind, least in (("Deployment", 2), ("Service", 2), ("NetworkPolicy", 2), ("Secret", 1)):
    if counts[kind] < least:
        problems.append(f"expected at least {least} owned {kind}, found {counts[kind]}")
for item in items:
    meta = item["metadata"]
    found = sorted({entry.get("manager", "") for entry in meta.get("managedFields") or []})
    if "curie-cli-connectors" not in found:
        problems.append(f"{item['kind']}/{meta['name']} lacks manager curie-cli-connectors (has: {found})")
    annotations = meta.get("annotations") or {}
    if item["kind"] == "Secret" and "kubectl.kubernetes.io/last-applied-configuration" in annotations:
        problems.append(f"Secret/{meta['name']} carries a last-applied-configuration copy")
for problem in problems:
    print(f"FAIL: {problem}", file=sys.stderr)
raise SystemExit(1 if problems else 0)
' || fail "connector objects are not written by the CLI server-side apply"

kcn get sandboxtemplates.extensions.agents.x-k8s.io --show-managed-fields -o json \
    | python3 -c '
import json, sys
release, agent = sys.argv[1], sys.argv[2]
items = json.load(sys.stdin).get("items") or []
def name(item):
    return item["metadata"]["name"]
generic = [i for i in items if name(i).endswith("-runner") and "-agent-" not in name(i)
           and (i["metadata"].get("labels") or {}).get("app.kubernetes.io/instance") == release]
if len(generic) != 1:
    print(f"FAIL: expected one generic runner SandboxTemplate, found {len(generic)}", file=sys.stderr)
    raise SystemExit(1)
clone_name = name(generic[0])[: -len("-runner")] + f"-agent-{agent}-runner"
clone = [i for i in items if name(i) == clone_name]
if not clone:
    print(f"FAIL: per-agent SandboxTemplate {clone_name} is missing", file=sys.stderr)
    raise SystemExit(1)
found = sorted({e.get("manager", "") for e in clone[0]["metadata"].get("managedFields") or []})
if "curie-cli-connectors" in found:
    print(f"FAIL: per-agent SandboxTemplate {clone_name} was written by the connector server-side apply (managers: {found})", file=sys.stderr)
    raise SystemExit(1)
labels = clone[0]["metadata"].get("labels") or {}
if labels.get("app.kubernetes.io/managed-by") == "curie-cli":
    if "kubectl-client-side-apply" not in found:
        print(f"FAIL: CLI-cloned SandboxTemplate {clone_name} is not a client-side apply (managers: {found})", file=sys.stderr)
        raise SystemExit(1)
    print(f"sandbox template {clone_name}: owner is the CLI clone (managers: {found})")
else:
    print(f"sandbox template {clone_name}: owner is helm, the CLI left it alone (managers: {found})")
' "$RELEASE" "$AGENT" || fail "the connector server-side apply wrote the sandbox template"

CLUSTER_IP="$(cluster_ip)"
[[ -n "$CLUSTER_IP" ]] || fail "service $HELD_SERVICE has no clusterIP"
kcn label service "$LABELLED_SERVICE" "$HAND_LABEL_KEY=kept" >/dev/null

# Phase C: add a connector with no secrets.
write_bundle 1
deploy_or_fail add-extra
assert_ready extra
[[ "$(cluster_ip)" == "$CLUSTER_IP" ]] || fail "service $HELD_SERVICE changed clusterIP after adding a connector"
[[ "$(hand_label)" == kept ]] || fail "the hand label on $LABELLED_SERVICE was removed after adding a connector"

# Phase D: remove it again.
write_bundle 0
deploy_or_fail remove-extra
pruned=0
for _ in $(seq 1 60); do
    listed="$(kcn get deployment,service,networkpolicy -o name)" || fail "could not list connector objects"
    if [[ "$listed" != *"/${RELEASE}-${AGENT}-mcp-extra"* ]]; then
        pruned=1
        break
    fi
    sleep 1
done
(( pruned )) || fail "the removed connector's objects were not pruned"
[[ "$(cluster_ip)" == "$CLUSTER_IP" ]] || fail "service $HELD_SERVICE changed clusterIP after removing a connector"
[[ "$(hand_label)" == kept ]] || fail "the hand label on $LABELLED_SERVICE was removed after removing a connector"
assert_ready rotate
assert_ready other

# Phase E: nothing outside the owned connector set moved, and no connector pod
# ever started ahead of its Secret.
[[ "$(resource_version configmap "$UNLABELLED")" == "$UNLABELLED_CM_RV" ]] || \
    fail "the unlabelled ConfigMap was modified"
[[ "$(resource_version service "$UNLABELLED")" == "$UNLABELLED_SVC_RV" ]] || \
    fail "the unlabelled Service was modified"
kcn get events -o json | python3 -c '
import json, re, sys
pattern = re.compile(r"secret.*not found|couldn.t find key", re.IGNORECASE)
bad = [
    event for event in json.load(sys.stdin).get("items") or []
    if event.get("reason") in ("Failed", "CreateContainerConfigError")
    and pattern.search(event.get("message") or "")
]
for event in bad:
    involved = event.get("involvedObject") or {}
    reason, kind, name = event.get("reason"), involved.get("kind"), involved.get("name")
    print(f"FAIL: {reason} on {kind}/{name}: missing secret", file=sys.stderr)
raise SystemExit(1 if bad else 0)
' || fail "a connector pod reported a missing Secret"

echo "connector apply converged through a concurrent reconciler create"
