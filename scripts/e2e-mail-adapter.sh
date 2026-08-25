#!/usr/bin/env bash
# Real AgentMail + Kubernetes acceptance for #1515. This is intentionally
# fail-closed: inability to create and delete isolated inboxes is an unmet
# external E2E prerequisite, never a skip or a green result.
set -euo pipefail

CONTEXT="k8"
IMAGE="${CURIE_MAIL_E2E_IMAGE:-}"
KEEP=0
SECRET_NAMESPACE="monitoring"
SECRET_NAME="agentmail-api"
SECRET_KEY="password"

usage() {
  echo "Usage: scripts/e2e-mail-adapter.sh --context k8 --image <candidate-image> [--keep]" >&2
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --context) CONTEXT="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[[ "$CONTEXT" == "k8" ]] || { echo "FAIL: this harness is pinned to context k8" >&2; exit 1; }
[[ -n "$IMAGE" ]] || { echo "FAIL: --image (or CURIE_MAIL_E2E_IMAGE) is required" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
RUN_ID="$(date -u +%Y%m%d%H%M%S)-${RANDOM}"
NAMESPACE="curie-mail-e2e-${RUN_ID,,}"
RELEASE="curie-mail-e2e"
OWNED_LABEL="curie-mail-e2e/owned"
TMP="$(mktemp -d /tmp/curie-mail-e2e.XXXXXX)"
chmod 700 "$TMP"
KEY_FILE="$TMP/agentmail-key"
API_KEY_FILE="$TMP/curie-api-key"
TOKEN_FILE="$TMP/channel-token"
VALUES_FILE="$TMP/values.json"
PF_PIDS=()
INBOX_ID_FILES=()

banner() { printf '\n== %s ==\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

delete_inbox() {
  local id_file="$1"
  [[ -s "$id_file" && -s "$KEY_FILE" ]] || return 0
  curl --silent --show-error --max-time 20 -o /dev/null -X DELETE \
    -H "Authorization: Bearer $(<"$KEY_FILE")" \
    "https://api.agentmail.to/v0/inboxes/$(<"$id_file")" || true
}

namespace_is_owned() {
  [[ "$(kubectl --context "$CONTEXT" get namespace "$NAMESPACE" -o "jsonpath={.metadata.labels['curie-mail-e2e/owned']}" 2>/dev/null || true)" == "true" ]]
}

cleanup() {
  local rc=$?
  for pid in "${PF_PIDS[@]}"; do kill "$pid" >/dev/null 2>&1 || true; done
  for id_file in "${INBOX_ID_FILES[@]}"; do delete_inbox "$id_file"; done
  if [[ "$KEEP" -eq 0 ]] && namespace_is_owned; then
    helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NAMESPACE" --no-hooks >/dev/null 2>&1 || true
    kubectl --context "$CONTEXT" delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP"
  return "$rc"
}
trap cleanup EXIT INT TERM

[[ "$(kubectl config current-context)" == "$CONTEXT" ]] \
  || fail "current kube context is not k8; select it explicitly before running"
kubectl --context "$CONTEXT" get secret "$SECRET_NAME" -n "$SECRET_NAMESPACE" >/dev/null \
  || fail "external E2E unmet: Kubernetes Secret $SECRET_NAMESPACE/$SECRET_NAME is absent"
umask 077
kubectl --context "$CONTEXT" get secret "$SECRET_NAME" -n "$SECRET_NAMESPACE" \
  -o "jsonpath={.data.${SECRET_KEY}}" | base64 -d >"$KEY_FILE"
[[ -s "$KEY_FILE" ]] || fail "external E2E unmet: AgentMail credential key is empty"

json_field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
for part in sys.argv[2].split("."):
    value = value[part]
print(value, end="")
PY
}

create_inbox() {
  local role="$1"
  local response="$TMP/create-$role.json"
  local status id_file email_file
  id_file="$TMP/$role-id"
  email_file="$TMP/$role-email"
  status="$(curl --silent --show-error --max-time 30 -o "$response" -w '%{http_code}' \
    -X POST -H "Authorization: Bearer $(<"$KEY_FILE")" \
    -H 'Content-Type: application/json' \
    --data "{\"client_id\":\"curie-mail-e2e-${RUN_ID}-${role}\",\"metadata\":{\"owner\":\"curie-mail-e2e\",\"run\":\"${RUN_ID}\"}}" \
    https://api.agentmail.to/v0/inboxes)"
  case "$status" in
    200|201) ;;
    401|403) fail "external E2E unmet: AgentMail key lacks inbox_create permission" ;;
    *) fail "external E2E unmet: disposable inbox creation returned HTTP $status" ;;
  esac
  json_field "$response" inbox_id >"$id_file"
  json_field "$response" email >"$email_file"
  [[ -s "$id_file" && -s "$email_file" ]] || fail "external E2E unmet: create response lacked isolated inbox identity"
  INBOX_ID_FILES+=("$id_file")
}

banner "Prove disposable AgentMail fixture capability"
create_inbox adapter
create_inbox allowed
create_inbox denied
[[ "$(<"$TMP/adapter-id")" != "$(<"$TMP/allowed-id")" ]] \
  || fail "external E2E unmet: disposable inbox isolation probe returned duplicate ids"

AGENTMAIL_CIDRS_JSON="$(getent ahostsv4 api.agentmail.to | awk '{print $1"/32"}' | sort -u | python3 -c 'import json,sys; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))')"
[[ "$AGENTMAIL_CIDRS_JSON" != "[]" ]] || fail "could not resolve AgentMail HTTPS CIDRs"

kubectl --context "$CONTEXT" create namespace "$NAMESPACE" >/dev/null
kubectl --context "$CONTEXT" label namespace "$NAMESPACE" "$OWNED_LABEL=true" --overwrite >/dev/null

write_values() {
  local deploy_mail="$1" token_file="${2:-}"
  KEY_FILE="$KEY_FILE" TOKEN_FILE="$token_file" VALUES_FILE="$VALUES_FILE" \
    ADAPTER_EMAIL_FILE="$TMP/adapter-email" ALLOWED_EMAIL_FILE="$TMP/allowed-email" \
    AGENTMAIL_CIDRS_JSON="$AGENTMAIL_CIDRS_JSON" IMAGE="$IMAGE" DEPLOY_MAIL="$deploy_mail" \
    python3 - <<'PY'
import json, os

image = os.environ["IMAGE"]
image_values = {"repository": image, "tag": "", "digest": "", "pullPolicy": "Always"}
if "@sha256:" in image:
    image_values["repository"], digest = image.split("@", 1)
    image_values["digest"] = digest
elif ":" in image.rsplit("/", 1)[-1]:
    image_values["repository"], image_values["tag"] = image.rsplit(":", 1)

token = ""
token_file = os.environ.get("TOKEN_FILE")
if token_file:
    token = open(token_file).read()

values = {
    "priorityClasses": {"platform": {"create": False}, "sandbox": {"create": False}},
    "agentSandbox": {"controller": {"deploy": False}, "runner": {"fakeModel": True}},
    "security": {"gvisor": {"mode": "off"}},
    "mailAdapter": {
        "deploy": os.environ["DEPLOY_MAIL"] == "true",
        "image": image_values,
        "inbox": open(os.environ["ADAPTER_EMAIL_FILE"]).read(),
        "allowedSenders": [open(os.environ["ALLOWED_EMAIL_FILE"]).read()],
        "channelToken": token,
        "egressSecret": "e2e-adapter-secret-" + os.path.basename(os.environ["VALUES_FILE"]),
        "agentmail": {
            "apiKey": open(os.environ["KEY_FILE"]).read(),
            "baseUrl": "https://api.agentmail.to/v0",
            "httpsCidrs": json.loads(os.environ["AGENTMAIL_CIDRS_JSON"]),
        },
        "persistence": {"size": "1Gi", "storageClass": "", "existingClaim": ""},
    },
}
with open(os.environ["VALUES_FILE"], "w") as handle:
    json.dump(values, handle)
PY
  chmod 600 "$VALUES_FILE"
}

banner "Install isolated fake-model Curie release"
write_values false
helm --kube-context "$CONTEXT" upgrade --install "$RELEASE" "$CHART" \
  -n "$NAMESPACE" -f "$VALUES_FILE" --wait --timeout 20m >/dev/null

kubectl --context "$CONTEXT" wait -n "$NAMESPACE" --for=condition=available \
  deployment/"$RELEASE-api" deployment/"$RELEASE-worker" --timeout=10m >/dev/null
kubectl --context "$CONTEXT" get secret "$RELEASE-secrets" -n "$NAMESPACE" \
  -o jsonpath='{.data.apiKey}' | base64 -d >"$API_KEY_FILE"
[[ -s "$API_KEY_FILE" ]] || fail "isolated Curie API key was not generated"

API_PORT=$((28000 + RANDOM % 1000))
kubectl --context "$CONTEXT" port-forward -n "$NAMESPACE" service/"$RELEASE-api" "$API_PORT:8000" \
  >"$TMP/api-port-forward.log" 2>&1 &
PF_PIDS+=("$!")
for _ in $(seq 1 100); do
  curl --silent --fail "http://127.0.0.1:$API_PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done
curl --silent --fail "http://127.0.0.1:$API_PORT/health" >/dev/null \
  || fail "isolated Curie API never became reachable"

api_post() {
  local path="$1" body_file="$2" out_file="$3"
  curl --silent --show-error --fail-with-body -o "$out_file" \
    -H "X-API-Key: $(<"$API_KEY_FILE")" -H 'Content-Type: application/json' \
    --data-binary "@$body_file" "http://127.0.0.1:$API_PORT$path" >/dev/null
}

python3 - "$TMP/agent.json" "$TMP/adapter-email" <<'PY'
import json, sys
email = open(sys.argv[2]).read()
json.dump({
    "name": "acme-mail-e2e",
    "channel": {
        "kind": "email", "address": email,
        "endpoint": "http://curie-mail-e2e-mail-adapter:8080", "adapter": "mail-adapter",
    },
}, open(sys.argv[1], "w"))
PY
api_post /agents "$TMP/agent.json" "$TMP/agent-response.json"
python3 - "$TMP/token-request.json" "$TMP/adapter-email" <<'PY'
import json, sys
json.dump({"kind": "email", "address": open(sys.argv[2]).read(), "ttl_s": 3600}, open(sys.argv[1], "w"))
PY
api_post /channels/token "$TMP/token-request.json" "$TMP/token-response.json"
json_field "$TMP/token-response.json" token >"$TOKEN_FILE"
[[ -s "$TOKEN_FILE" ]] || fail "channel token mint returned no token"

banner "Enable the candidate mail adapter on durable RWO state"
write_values true "$TOKEN_FILE"
helm --kube-context "$CONTEXT" upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" \
  -f "$VALUES_FILE" --wait --timeout 15m >/dev/null
kubectl --context "$CONTEXT" wait -n "$NAMESPACE" --for=condition=available \
  deployment/"$RELEASE-mail-adapter" --timeout=5m >/dev/null

banner "Prove mail-only NetworkPolicy enforcement and pod hardening"
kubectl --context "$CONTEXT" get deployment "$RELEASE-mail-adapter" -n "$NAMESPACE" -o json \
  >"$TMP/mail-deployment.json"
python3 - "$TMP/mail-deployment.json" "$TMP/network-probe.json" "$NAMESPACE" <<'PY'
import json, sys
deployment = json.load(open(sys.argv[1]))
labels = deployment["spec"]["template"]["metadata"]["labels"]
probe = {
    "apiVersion": "v1", "kind": "Pod",
    "metadata": {"name": "mail-egress-probe", "namespace": sys.argv[3], "labels": labels},
    "spec": {
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "containers": [{
            "name": "probe", "image": "curlimages/curl:8.12.1",
            "command": ["sh", "-c", "sleep 600"],
            "securityContext": {
                "runAsNonRoot": True, "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }],
    },
}
json.dump(probe, open(sys.argv[2], "w"))
PY
kubectl --context "$CONTEXT" apply -f "$TMP/network-probe.json" >/dev/null
kubectl --context "$CONTEXT" wait -n "$NAMESPACE" --for=condition=Ready \
  pod/mail-egress-probe --timeout=3m >/dev/null
kubectl --context "$CONTEXT" exec -n "$NAMESPACE" mail-egress-probe -- \
  curl --silent --output /dev/null --connect-timeout 5 https://api.agentmail.to/v0/inboxes \
  || fail "mail NetworkPolicy blocked its configured AgentMail HTTPS CIDRs"
if kubectl --context "$CONTEXT" exec -n "$NAMESPACE" mail-egress-probe -- \
  curl --silent --output /dev/null --connect-timeout 5 https://example.com; then
  fail "mail NetworkPolicy permitted a non-configured HTTPS destination"
fi
python3 - "$TMP/mail-deployment.json" <<'PY'
import json, sys
pod = json.load(open(sys.argv[1]))["spec"]["template"]["spec"]
container = pod["containers"][0]
assert pod.get("automountServiceAccountToken") is False
assert container.get("securityContext", {}).get("readOnlyRootFilesystem") is True
assert container.get("readinessProbe", {}).get("httpGet", {}).get("path") == "/readyz"
assert container.get("livenessProbe", {}).get("httpGet", {}).get("path") == "/healthz"
assert not any(env.get("name") == "CURIE_API_KEY" for env in container.get("env", []))
PY

send_mail() {
  local from_role="$1"
  local correlation="$2"
  local response="$TMP/send-$correlation.json"
  python3 - "$TMP/send-body.json" "$TMP/adapter-email" "$correlation" <<'PY'
import json, sys
json.dump({"to": open(sys.argv[2]).read(), "subject": sys.argv[3], "text": "Reply with the deterministic plumbing result."}, open(sys.argv[1], "w"))
PY
  local status
  status="$(curl --silent --show-error --max-time 30 -o "$response" -w '%{http_code}' \
    -X POST -H "Authorization: Bearer $(<"$KEY_FILE")" -H 'Content-Type: application/json' \
    --data-binary "@$TMP/send-body.json" \
    "https://api.agentmail.to/v0/inboxes/$(<"$TMP/$from_role-id")/messages/send")"
  [[ "$status" == "200" ]] || fail "AgentMail send returned HTTP $status"
  json_field "$response" thread_id >"$TMP/thread-$correlation"
}

thread_has_marker() {
  local correlation="$1"
  local out="$TMP/thread.json"
  local status
  status="$(curl --silent --show-error --max-time 20 -o "$out" -w '%{http_code}' \
    -H "Authorization: Bearer $(<"$KEY_FILE")" \
    "https://api.agentmail.to/v0/inboxes/$(<"$TMP/adapter-id")/threads/$(<"$TMP/thread-$correlation")")"
  [[ "$status" == "200" ]] || return 1
  python3 - "$out" <<'PY'
import json, sys
messages = json.load(open(sys.argv[1])).get("messages", [])
raise SystemExit(0 if any("X-Curie-Event:" in str(m.get(k, "")) for m in messages for k in ("text", "extracted_text", "preview")) else 1)
PY
}

wait_for_marker() {
  local correlation="$1"
  for _ in $(seq 1 180); do thread_has_marker "$correlation" && return 0; sleep 1; done
  return 1
}

banner "Positive: real mail produces one Curie turn and one threaded reply"
send_mail allowed "positive-$RUN_ID"
wait_for_marker "positive-$RUN_ID" || fail "positive mail produced no correlated threaded reply"

banner "Negative: disallowed sender and malformed authenticated egress"
send_mail denied "denied-$RUN_ID"
sleep 10
if thread_has_marker "denied-$RUN_ID"; then fail "disallowed sender produced a reply"; fi

MAIL_PORT=$((29000 + RANDOM % 500))
kubectl --context "$CONTEXT" port-forward -n "$NAMESPACE" service/"$RELEASE-mail-adapter" "$MAIL_PORT:8080" \
  >"$TMP/mail-port-forward.log" 2>&1 &
PF_PIDS+=("$!")
sleep 1
malformed_status="$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
  -H 'X-Curie-Adapter-Secret: e2e-adapter-secret-values.json' \
  -H 'Content-Type: application/json' --data '{}' "http://127.0.0.1:$MAIL_PORT/")"
case "$malformed_status" in 2*) fail "authenticated malformed egress was accepted" ;; esac

banner "Recovery: 401 pending survives token Secret rotation and Recreate"
printf '%s' 'chn.invalid-e2e-token' >"$TMP/invalid-token"
write_values true "$TMP/invalid-token"
helm --kube-context "$CONTEXT" upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" \
  -f "$VALUES_FILE" --wait --timeout 10m >/dev/null
send_mail allowed "recovery-$RUN_ID"
sleep 5
if thread_has_marker "recovery-$RUN_ID"; then fail "invalid scoped token unexpectedly produced a reply"; fi
write_values true "$TOKEN_FILE"
helm --kube-context "$CONTEXT" upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" \
  -f "$VALUES_FILE" --wait --timeout 10m >/dev/null
wait_for_marker "recovery-$RUN_ID" || fail "pending 401 delivery did not recover after token rotation"

banner "PASS: real AgentMail positive, negative, malformed-wire, and Recreate recovery"
