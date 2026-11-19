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

for caller in ("api", "worker"):
    caller_env = refs("curie-" + caller)
    if "SLACK_BOT_TOKEN" in caller_env or "CURIE_CHANNEL_GATEWAY_TOKEN" not in caller_env:
        raise SystemExit("application must use the gateway caller credential")
if "SLACK_BOT_TOKEN" not in refs("curie-channel-gateway"):
    raise SystemExit("gateway has no provider credential")

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

for deployment in ("curie-worker", "curie-ui", "curie-channel-gateway"):
    if env_name in refs(deployment):
        raise SystemExit(f"{deployment} must not receive {env_name}")

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
