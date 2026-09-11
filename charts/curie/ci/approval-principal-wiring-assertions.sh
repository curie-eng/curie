#!/usr/bin/env bash
# Render assertions for authenticated Slack approval principals (#1531).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "ASSERTION FAILED: $1" >&2; exit 1; }

dev_render="$TMP/dev-render"
sealed_render="$TMP/sealed-render"
mkdir -p "$dev_render" "$sealed_render"
helm template curie "$CHART" --output-dir "$dev_render" \
  -f "$CHART/values-dev.yaml" \
  --set dispatcher.slack.appToken=xapp-assert \
  --set dispatcher.slack.botToken=xoxb-assert >/dev/null
helm template curie "$CHART" --output-dir "$sealed_render" \
  --set dispatcher.slack.appToken=xapp-assert \
  --set dispatcher.slack.botToken=xoxb-assert >/dev/null

python3 - "$dev_render" "$sealed_render" <<'PY'
import pathlib
import sys
import yaml

def load(root):
    docs = []
    for path in pathlib.Path(root).rglob("*.yaml"):
        with path.open() as stream:
            docs.extend(doc for doc in yaml.safe_load_all(stream) if doc)
    return docs

dev_docs = load(sys.argv[1])
docs = load(sys.argv[2])

secret = next(
    doc for doc in dev_docs
    if doc.get("kind") == "Secret" and doc.get("metadata", {}).get("name") == "curie-secrets"
)
data = secret.get("stringData") or {}
attester = data.get("approvalChatAttesterSecret")
if not attester:
    raise SystemExit("chart Secret has no approvalChatAttesterSecret")
if attester == data.get("apiKey"):
    raise SystemExit("approvalChatAttesterSecret equals apiKey")

deployments = {
    doc["metadata"]["name"]: doc
    for doc in docs
    if doc.get("kind") == "Deployment"
}

def refs(name):
    pod = deployments[name]["spec"]["template"]["spec"]
    env = pod["containers"][0].get("env") or []
    return {entry["name"]: entry for entry in env}

env_name = "CURIE_APPROVAL_CHAT_ATTESTER_SECRET"
for deployment in ("curie-api", "curie-dispatcher"):
    entry = refs(deployment).get(env_name)
    expected = {
        "name": env_name,
        "valueFrom": {
            "secretKeyRef": {
                "name": "curie-secrets",
                "key": "approvalChatAttesterSecret",
            }
        },
    }
    if entry != expected:
        raise SystemExit(f"{deployment} has wrong {env_name} wiring: {entry!r}")

for deployment in ("curie-worker", "curie-ui"):
    if env_name in refs(deployment):
        raise SystemExit(f"{deployment} must not receive {env_name}")

# The shipped dev attester literal must never reach a DEFAULT (sealed) render.
# values.yaml carries it only as the curie.managedSecret comparison default, so
# a bare install generates a random secret instead; values-dev.yaml emits it on
# purpose via allowDevDefaults and is deliberately not checked here.
DEV_ATTESTER_LITERAL = "curie-dev-approval-chat-attester"
sealed_secrets = [doc for doc in docs if doc.get("kind") == "Secret"]
if not sealed_secrets:
    raise SystemExit("sealed render produced no Secret; the dev-literal assertion is vacuous")
for doc in sealed_secrets:
    if DEV_ATTESTER_LITERAL in yaml.safe_dump(doc):
        name = doc.get("metadata", {}).get("name")
        raise SystemExit(f"sealed render Secret {name} ships the dev attester default")

# --- Full-render sweep -------------------------------------------------
# The checks above read only containers[0].env of four named Deployments and
# only kind==Secret for the literal. Kubernetes injects secrets by several
# other routes, so the claim "the sealed render never ships the attester to
# anything but api/dispatcher, and never ships the dev literal" is only as
# strong as a sweep over EVERY rendered document and EVERY pod spec.
import base64

ALLOWED_ATTESTER_WORKLOADS = {"curie-api", "curie-dispatcher"}
LITERAL_B64 = base64.b64encode(DEV_ATTESTER_LITERAL.encode()).decode()

# 1. The literal (or its base64 encoding) anywhere in the sealed render --
#    ConfigMap, plain env value, annotation, Secret data -- is a leak.
for doc in docs:
    dumped = yaml.safe_dump(doc)
    kind = doc.get("kind")
    name = doc.get("metadata", {}).get("name")
    if DEV_ATTESTER_LITERAL in dumped:
        raise SystemExit(f"sealed render {kind}/{name} ships the dev attester literal")
    if LITERAL_B64 in dumped:
        raise SystemExit(f"sealed render {kind}/{name} ships the base64 dev attester literal")
    # base64 `data:` would hide the plaintext from the scan above, so decode it.
    if kind == "Secret":
        for key, value in (doc.get("data") or {}).items():
            try:
                decoded = base64.b64decode(str(value), validate=True).decode("utf-8", "replace")
            except Exception:
                continue
            if DEV_ATTESTER_LITERAL in decoded:
                raise SystemExit(
                    f"sealed render Secret {name} data.{key} decodes to the dev attester literal"
                )

# 2. Every pod spec in the render -- containers, initContainers and
#    ephemeralContainers of any Deployment/StatefulSet/DaemonSet/Job/CronJob
#    or bare Pod -- not just containers[0] of four names.
def pod_specs(doc):
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        yield spec
    elif kind == "CronJob":
        job = ((spec.get("jobTemplate") or {}).get("spec") or {})
        yield ((job.get("template") or {}).get("spec") or {})
    elif kind in ("Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"):
        yield ((spec.get("template") or {}).get("spec") or {})

EXPECTED_SOURCE = {
    "name": "curie-secrets",
    "key": "approvalChatAttesterSecret",
}
for doc in docs:
    name = doc.get("metadata", {}).get("name")
    kind = doc.get("kind")
    for pod in pod_specs(doc):
        if not pod:
            continue
        containers = []
        for field in ("containers", "initContainers", "ephemeralContainers"):
            containers.extend(pod.get(field) or [])
        for container in containers:
            label = f"{kind}/{name} container {container.get('name')}"
            for entry in container.get("env") or []:
                if entry.get("name") != env_name:
                    continue
                if name not in ALLOWED_ATTESTER_WORKLOADS:
                    raise SystemExit(f"{label} must not receive {env_name}")
                source = ((entry.get("valueFrom") or {}).get("secretKeyRef") or {})
                if entry.get("value") is not None or source != EXPECTED_SOURCE:
                    raise SystemExit(
                        f"{label} sources {env_name} from {entry!r}, not "
                        f"secretKeyRef {EXPECTED_SOURCE}"
                    )
            # envFrom of curie-secrets hands the whole Secret, attester
            # included, to a workload no named-env check would ever see.
            for source in container.get("envFrom") or []:
                ref = (source.get("secretRef") or {}).get("name")
                if ref == "curie-secrets" and name not in ALLOWED_ATTESTER_WORKLOADS:
                    raise SystemExit(
                        f"{label} envFrom secretRef curie-secrets, which carries "
                        f"{env_name}; only {sorted(ALLOWED_ATTESTER_WORKLOADS)} may"
                    )

# 3. The sealed curie-secrets object itself must carry a real, generated,
#    independent attester -- present, non-empty, distinct from apiKey, and
#    not the published dev literal.
sealed_secret = next(
    (
        doc for doc in docs
        if doc.get("kind") == "Secret" and doc.get("metadata", {}).get("name") == "curie-secrets"
    ),
    None,
)
if sealed_secret is None:
    raise SystemExit("sealed render has no curie-secrets Secret; the sealed assertions are vacuous")
sealed_values = dict(sealed_secret.get("stringData") or {})
for key, value in (sealed_secret.get("data") or {}).items():
    try:
        sealed_values.setdefault(key, base64.b64decode(str(value), validate=True).decode())
    except Exception:
        sealed_values.setdefault(key, str(value))
sealed_attester = sealed_values.get("approvalChatAttesterSecret")
if not sealed_attester:
    raise SystemExit("sealed curie-secrets has no non-empty approvalChatAttesterSecret")
if sealed_attester == sealed_values.get("apiKey"):
    raise SystemExit("sealed approvalChatAttesterSecret equals apiKey")
# Backstop only: the whole-render literal sweep above already covers this
# object, so this line is redundant by construction rather than independently
# reachable. It is kept so the sealed Secret states its own invariant.
if sealed_attester == DEV_ATTESTER_LITERAL:
    raise SystemExit("sealed approvalChatAttesterSecret is the published dev literal")

sandboxes = [doc for doc in docs if doc.get("kind") == "SandboxTemplate"]
if not sandboxes:
    raise SystemExit("sealed render produced no SandboxTemplate; runner isolation assertion is vacuous")
for sandbox in sandboxes:
    if env_name in yaml.safe_dump(sandbox):
        raise SystemExit("runner SandboxTemplate must not receive chat attestation secret")
PY

# Even an explicit operator override may not collapse the independent trust
# domains onto one key. Refusal output intentionally names only values paths.
if helm template curie "$CHART" \
  --set api.apiKey=shared-assertion-value \
  --set api.approvalChatAttesterSecret=shared-assertion-value \
  >"$TMP/equal.out" 2>"$TMP/equal.err"; then
  fail "equal API and chat attester secrets rendered successfully"
fi
grep -q "must differ" "$TMP/equal.err" \
  || fail "equal-secret refusal did not provide a non-secret recovery hint"
if grep -q "shared-assertion-value" "$TMP/equal.err"; then
  fail "equal-secret refusal leaked the credential value"
fi

echo "OK: approval principal secret wiring render assertions passed"
