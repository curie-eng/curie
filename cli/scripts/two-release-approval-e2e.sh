#!/usr/bin/env bash
# Two Helm releases on one kind cluster, one Slack app, owner-only approval
# without retry-until-acked (#2307).
#
# Phases (CURIE_TWO_RELEASE_PHASE, or the first argument):
#   prereqs  Helm/API one-shot path is always ready. Slack tokens are not
#            a helm prereq. Missing tokens only document that the live
#            envelope row will be BLOCKED; ready=true so path-triggered CI
#            still stands up two Helm releases. CURIE_TWO_RELEASE_REQUIRED=1
#            is live-envelope-required mode: missing tokens fail-close that
#            row, not the helm path.
#   run      Install two releases on a job-owned kind cluster and prove
#            owner-only without looping deliveries. Refuses unless
#            CURIE_TWO_RELEASE_ALLOW_LIVE=1 so a laptop invocation cannot
#            touch Slack or a cluster.
#
# Sequence (one shot each; never retry until some release succeeds):
#   1. Create the approval on owner release A.
#   2. Observe consumer B once via API GET (404 not found). No resolve /
#      reject / mutate.
#   3. Observe owner A once via API GET (still pending).
#   4. POST resolve to B once (404 approval not found); A still pending.
#   5. POST resolve to A once. 200/201 resolved is PASS; 401/403 because a
#      human/operator principal is required is BLOCKED (do not impersonate
#      Slack). B-miss stays PASS either way.
# Helm two-release always writes a 0600 Slack values file so both dispatcher
# Deployments exist (curie.dispatcher.enabled requires appToken AND botToken).
# Missing dedicated Slack app: BLOCKED live envelope row, not PASS. Dummy
# fixture tokens make the dispatcher backoff; that is OK.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PHASE="${CURIE_TWO_RELEASE_PHASE:-${1:-}}"
KIND_CLUSTER=curie-2307-two-release
NS_A=curie-2307-owner
NS_B=curie-2307-consumer
RELEASE_A=owner2307
RELEASE_B=consumer2307
CHART="$ROOT/charts/curie"
NOGVISOR="$CHART/values-e2e-nogvisor.yaml"
CONSUMER_OVERLAY="$CHART/values-e2e-two-release-consumer.yaml"
JOB_LABEL_KEY="curie.example.com/two-release-job"
JOB_LABEL_VALUE="2307"
API_KEY="${CURIE_API_KEY:-}"
OWNED_KIND=0
OWNED_HELM=0
PF_A_PID=""
PF_B_PID=""
SLACK_VALUES=""
API_VALUES=""
PREV_CONTEXT=""
KEEP="${CURIE_TWO_RELEASE_KEEP:-0}"
FORCE="${CURIE_TWO_RELEASE_FORCE:-0}"

if [[ -n "${CURIE_TWO_RELEASE_KIND_CLUSTER:-}" ]]; then
  KIND_CLUSTER="$CURIE_TWO_RELEASE_KIND_CLUSTER"
fi

if [[ -z "$PHASE" ]]; then
  if [[ "${CURIE_TWO_RELEASE_ALLOW_LIVE:-}" == "1" ]]; then
    PHASE=run
  else
    PHASE=prereqs
  fi
fi

log() { printf '%s\n' "$*" >&2; }

die() {
  log "error: $*"
  exit 1
}

write_summary() {
  local body="$1"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$body" >>"$GITHUB_STEP_SUMMARY"
  fi
  printf '%s\n' "$body" >&2
}

write_output() {
  local key="$1"
  local value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "$key" "$value" >>"$GITHUB_OUTPUT"
  fi
}

missing_live_envelope_creds() {
  local missing=()
  [[ -n "${CI_SLACK_APP_TOKEN:-}" ]] || missing+=("CI_SLACK_APP_TOKEN (dedicated CI Slack app token)")
  [[ -n "${CI_SLACK_BOT_TOKEN:-}" ]] || missing+=("CI_SLACK_BOT_TOKEN (dedicated CI Slack bot token)")
  if ((${#missing[@]})); then
    printf '%s\n' "${missing[@]}"
  fi
}

phase_prereqs() {
  local missing
  missing="$(missing_live_envelope_creds || true)"
  if [[ -n "$missing" ]]; then
    if [[ "${CURIE_TWO_RELEASE_REQUIRED:-}" == "1" ]]; then
      write_summary "$(cat <<EOF
### Two-release approval e2e live envelope BLOCKED (required mode)

Helm/API one-shot still proceeds. Live Slack envelope observation was
required and is fail-closed. Missing credential(s):

$(printf '%s\n' "$missing" | sed 's/^/- /')

Provision a dedicated CI Slack app (app token and bot token) to observe
the live envelope. Do not use an unknown local bot token.
EOF
)"
      write_output ready true
      write_output skip_reason "live envelope BLOCKED: missing dedicated CI Slack app"
      echo "two-release-approval-e2e: helm prereqs ready; live envelope required but missing" >&2
      exit 0
    fi
    write_summary "$(cat <<EOF
### Two-release approval e2e prerequisites ready (Helm/API one-shot)

Slack tokens are absent; that does not skip the Helm two-release cluster
job. The default path is Helm install + create-on-A + one-shot B miss +
one-shot A pending. Live Slack owner-only envelope stays BLOCKED without
those tokens. Do not use an unknown local bot token.

Missing live-envelope credential(s):

$(printf '%s\n' "$missing" | sed 's/^/- /')
EOF
)"
    write_output ready true
    write_output skip_reason "live envelope BLOCKED: missing dedicated CI Slack app"
    echo "two-release-approval-e2e: helm prereqs ready; live envelope BLOCKED (tokens missing)" >&2
    exit 0
  fi
  write_summary "### Two-release approval e2e prerequisites ready

Dedicated CI Slack app tokens are present. The cluster job installs two
Helm releases for the API one-shot path and may observe a live envelope."
  write_output ready true
  echo "two-release-approval-e2e: prerequisites ready" >&2
}

refuse_foreign_cluster_name() {
  case "$KIND_CLUSTER" in
    dark-factory|curie-2589-resume|curie-pilot3-verify|curie-sre-demo|curie-e2e)
      die "refusing another job's kind cluster name $KIND_CLUSTER"
      ;;
  esac
}

fullname() {
  local release="$1"
  printf '%s-curie' "$release"
}

shared_helm_sets() {
  cat <<'EOF'
langfuse.deploy=false
langfuse.host=langfuse.example.com
clickhouse.deploy=false
otelCollector.deploy=false
otelCollector.telemetryDisabled=true
rustfs.deploy=false
rustfs.host=s3.example.com
ui.deploy=false
mailAdapter.deploy=false
inference.deploy=false
worker.deploy=false
agentSandbox.deploy=false
preflights.avxCheck.enabled=false
preflights.networkPolicyProbe.enabled=false
preflights.controllerReady.enabled=false
postgres.persistence.enabled=false
valkey.persistence.enabled=false
rustfs.persistence.enabled=false
EOF
}

image_sets() {
  local api_image="${CURIE_API_IMAGE:-}"
  local dispatcher_image="${CURIE_DISPATCHER_IMAGE:-}"
  if [[ -n "$api_image" ]]; then
    printf 'api.image.repository=%s\n' "${api_image%:*}"
    printf 'api.image.tag=%s\n' "${api_image##*:}"
    printf 'api.image.pullPolicy=Never\n'
  fi
  if [[ -n "$dispatcher_image" ]]; then
    printf 'dispatcher.image.repository=%s\n' "${dispatcher_image%:*}"
    printf 'dispatcher.image.tag=%s\n' "${dispatcher_image##*:}"
    printf 'dispatcher.image.pullPolicy=Never\n'
  fi
}

helm_set_args() {
  local line
  local args=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && args+=(--set "$line")
  done < <(shared_helm_sets; image_sets)
  printf '%s\n' "${args[@]}"
}

write_slack_values() {
  SLACK_VALUES="$(mktemp)"
  chmod 600 "$SLACK_VALUES"
  python3 - "$SLACK_VALUES" <<'PY'
import json, os, pathlib, sys
path = pathlib.Path(sys.argv[1])
# CI tokens when present; otherwise non-connecting placeholders so
# curie.dispatcher.enabled still deploys. Never real Slack, never helm argv.
app = os.environ.get("CI_SLACK_APP_TOKEN") or "xapp-fixture-2307"
bot = os.environ.get("CI_SLACK_BOT_TOKEN") or "xoxb-fixture-2307"
path.write_text(
    "dispatcher:\n"
    "  slack:\n"
    f"    appToken: {json.dumps(app)}\n"
    f"    botToken: {json.dumps(bot)}\n"
)
PY
}

write_api_values() {
  if [[ -z "$API_KEY" ]]; then
    API_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
  fi
  local attester
  attester="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
  while [[ "$attester" == "$API_KEY" ]]; do
    attester="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
  done
  API_VALUES="$(mktemp)"
  chmod 600 "$API_VALUES"
  # Keys stay in this 0600 file, never on the helm argv.
  CURIE_API_KEY="$API_KEY" CURIE_APPROVAL_CHAT_ATTESTER_SECRET="$attester" \
    python3 - "$API_VALUES" <<'PY'
import json, os, pathlib, sys
path = pathlib.Path(sys.argv[1])
api_key = os.environ["CURIE_API_KEY"]
attester = os.environ["CURIE_APPROVAL_CHAT_ATTESTER_SECRET"]
if api_key == attester:
    raise SystemExit("api.apiKey and api.approvalChatAttesterSecret must differ")
path.write_text(
    "security:\n"
    "  allowDevDefaults: true\n"
    "api:\n"
    f"  apiKey: {json.dumps(api_key)}\n"
    f"  approvalChatAttesterSecret: {json.dumps(attester)}\n"
)
PY
}

install_owner_release() {
  local sets=()
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] && sets+=("$line")
  done < <(helm_set_args)
  helm install "$RELEASE_A" "$CHART" \
    --namespace "$NS_A" \
    --wait --timeout 15m \
    -f "$NOGVISOR" \
    -f "$API_VALUES" \
    -f "$SLACK_VALUES" \
    "${sets[@]}"
}

install_consumer_release() {
  local sets=()
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] && sets+=("$line")
  done < <(helm_set_args)
  helm install "$RELEASE_B" "$CHART" \
    --namespace "$NS_B" \
    --skip-crds \
    --wait --timeout 15m \
    -f "$NOGVISOR" \
    -f "$CONSUMER_OVERLAY" \
    -f "$API_VALUES" \
    -f "$SLACK_VALUES" \
    "${sets[@]}"
}

create_owned_namespace() {
  local ns="$1"
  kubectl create namespace "$ns"
  kubectl label namespace "$ns" "${JOB_LABEL_KEY}=${JOB_LABEL_VALUE}" --overwrite >/dev/null
}

cluster_is_job_owned() {
  local kubeconfig found
  kubeconfig="$(mktemp)"
  if ! kind export kubeconfig --name "$KIND_CLUSTER" --kubeconfig "$kubeconfig" >/dev/null 2>&1; then
    rm -f "$kubeconfig"
    return 1
  fi
  found="$(kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    get nodes -l "${JOB_LABEL_KEY}=${JOB_LABEL_VALUE}" -o name 2>/dev/null || true)"
  rm -f "$kubeconfig"
  [[ -n "$found" ]]
}

label_kind_cluster() {
  kubectl label nodes --all "${JOB_LABEL_KEY}=${JOB_LABEL_VALUE}" --overwrite >/dev/null
}

ensure_kind_cluster() {
  refuse_foreign_cluster_name
  if kind get clusters 2>/dev/null | grep -Fxq "$KIND_CLUSTER"; then
    if [[ "$FORCE" != "1" ]]; then
      die "kind cluster $KIND_CLUSTER already exists; set CURIE_TWO_RELEASE_FORCE=1 to recreate this job-owned cluster only"
    fi
    if ! cluster_is_job_owned; then
      die "kind cluster $KIND_CLUSTER exists but is not labeled ${JOB_LABEL_KEY}=${JOB_LABEL_VALUE}; refusing to delete"
    fi
    log "recreating leftover kind cluster $KIND_CLUSTER"
    kind delete cluster --name "$KIND_CLUSTER"
  fi
  kind create cluster --name "$KIND_CLUSTER" --wait 120s
  OWNED_KIND=1
  label_kind_cluster
  if [[ -n "${CURIE_API_IMAGE:-}" ]]; then
    kind load docker-image "$CURIE_API_IMAGE" --name "$KIND_CLUSTER"
  fi
  if [[ -n "${CURIE_DISPATCHER_IMAGE:-}" ]]; then
    kind load docker-image "$CURIE_DISPATCHER_IMAGE" --name "$KIND_CLUSTER"
  fi
}

start_port_forward() {
  local ns="$1" svc="$2" local_port="$3"
  kubectl --namespace "$ns" port-forward "svc/${svc}" "${local_port}:8000" >/dev/null 2>&1 &
  printf '%s' "$!"
}

wait_http() {
  local url="$1"
  local i
  for i in $(seq 1 60); do
    if curl -fsS -o /dev/null --max-time 2 "$url"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

api_json() {
  local url="$1" method="$2" body="${3:-}"
  python3 - "$url" "$method" "$body" <<'PY'
import json, os, sys, urllib.error, urllib.request
url, method, body = sys.argv[1], sys.argv[2], sys.argv[3]
req = urllib.request.Request(
    url,
    data=body.encode() if body else None,
    method=method,
    headers={
        "X-API-Key": os.environ["CURIE_API_KEY"],
        "Content-Type": "application/json",
        "Accept": "application/json",
    },
)
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read().decode()
        status = resp.status
except urllib.error.HTTPError as exc:
    payload = exc.read().decode()
    status = exc.code
except urllib.error.URLError as exc:
    print(json.dumps({"status": 0, "body": str(exc.reason)}))
    sys.exit(0)
try:
    parsed = json.loads(payload) if payload else None
except json.JSONDecodeError:
    parsed = payload
print(json.dumps({"status": status, "body": parsed}))
PY
}

create_approval_on_a() {
  local url="$1"
  api_json "$url/approvals" POST "$(python3 - <<'PY'
import json, uuid
print(json.dumps({
    "conversation_id": "C0EXAMPLE1",
    "author": "U0EXAMPLE1",
    "summary": "two-release owner-only fixture",
    "reply_kind": "slack",
    "reply_channel": "C0EXAMPLE1",
    "reply_placeholder": "p-2307",
    "dedupe_key": uuid.uuid4().hex,
}))
PY
)"
}

# One delivery/observation of the consumer. Do not wrap this in a retry.
observe_consumer_once() {
  local owner_url="$1" consumer_url="$2" approval_id="$3"
  local miss owner_after
  miss="$(api_json "$consumer_url/approvals/${approval_id}" GET)"
  owner_after="$(api_json "$owner_url/approvals/${approval_id}" GET)"
  python3 - "$miss" "$owner_after" <<'PY'
import json, sys
miss = json.loads(sys.argv[1])
owner = json.loads(sys.argv[2])
detail = ""
body = miss.get("body")
if isinstance(body, dict):
    detail = str(body.get("detail") or "")
if miss.get("status") != 404 or detail.strip().casefold() != "approval not found":
    raise SystemExit(
        f"consumer one-shot must 404 approval not found; got {miss}"
    )
if owner.get("status") != 200:
    raise SystemExit(f"owner row missing after consumer one-shot: {owner}")
status = (owner.get("body") or {}).get("status") if isinstance(owner.get("body"), dict) else None
if status != "pending":
    raise SystemExit(
        f"consumer one-shot must not resolve/reject/mutate; owner status={status}"
    )
print("consumer one-shot: 404 ownership miss, owner row still pending")
PY
}

# One delivery/observation of the owner. Do not wrap this in a retry.
observe_owner_once() {
  local owner_url="$1" approval_id="$2"
  local got
  got="$(api_json "$owner_url/approvals/${approval_id}" GET)"
  python3 - "$got" <<'PY'
import json, sys
got = json.loads(sys.argv[1])
if got.get("status") != 200:
    raise SystemExit(f"owner one-shot GET failed: {got}")
status = (got.get("body") or {}).get("status") if isinstance(got.get("body"), dict) else None
if status != "pending":
    raise SystemExit(f"owner one-shot expected pending before live resolve; status={status}")
print("owner one-shot: row still pending on A")
PY
}

# One POST /resolve on the consumer. Do not wrap this in a retry.
resolve_consumer_once() {
  local owner_url="$1" consumer_url="$2" approval_id="$3"
  local miss owner_after
  miss="$(api_json "$consumer_url/approvals/${approval_id}/resolve" POST '{"decision":"approved"}')"
  owner_after="$(api_json "$owner_url/approvals/${approval_id}" GET)"
  python3 - "$miss" "$owner_after" <<'PY'
import json, sys
miss = json.loads(sys.argv[1])
owner = json.loads(sys.argv[2])
detail = ""
body = miss.get("body")
if isinstance(body, dict):
    detail = str(body.get("detail") or "")
if miss.get("status") != 404 or detail.strip().casefold() != "approval not found":
    raise SystemExit(
        f"consumer one-shot resolve must 404 approval not found; got {miss}"
    )
if owner.get("status") != 200:
    raise SystemExit(f"owner row missing after consumer resolve-once: {owner}")
status = (owner.get("body") or {}).get("status") if isinstance(owner.get("body"), dict) else None
if status != "pending":
    raise SystemExit(
        f"consumer resolve-once must not mutate owner; owner status={status}"
    )
print("consumer one-shot resolve: 404 ownership miss, owner row still pending")
PY
}

# One POST /resolve on the owner. Do not wrap this in a retry.
# 200/201 resolved -> PASS. 401/403 principal required -> BLOCKED (do not
# impersonate Slack). Unexpected statuses fail closed.
resolve_owner_once() {
  local owner_url="$1" approval_id="$2"
  local got
  got="$(api_json "$owner_url/approvals/${approval_id}/resolve" POST '{"decision":"approved"}')"
  python3 - "$got" <<'PY'
import json, sys
got = json.loads(sys.argv[1])
status = got.get("status")
body = got.get("body")
row_status = body.get("status") if isinstance(body, dict) else None
if status in (200, 201):
    if row_status not in ("approved", "rejected"):
        raise SystemExit(f"owner resolve-once HTTP {status} but not resolved: {got}")
    print("PASS\tresolved")
    raise SystemExit(0)
if status in (401, 403):
    print("BLOCKED\thuman/operator principal required")
    raise SystemExit(0)
raise SystemExit(f"owner resolve-once unexpected response: {got}")
PY
}

snapshot_consumer_logs_once() {
  local deploy
  deploy="$(fullname "$RELEASE_B")-dispatcher"
  kubectl --namespace "$NS_B" logs "deploy/${deploy}" --tail=200 2>/dev/null || true
}

stop_port_forward() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local status=$?
  stop_port_forward "$PF_A_PID"
  stop_port_forward "$PF_B_PID"
  if [[ -n "$SLACK_VALUES" && -f "$SLACK_VALUES" ]]; then
    rm -f "$SLACK_VALUES"
  fi
  if [[ -n "$API_VALUES" && -f "$API_VALUES" ]]; then
    rm -f "$API_VALUES"
  fi
  if [[ "$KEEP" == "1" ]]; then
    log "keeping owned resources (kind=$KIND_CLUSTER ns=$NS_A,$NS_B releases=$RELEASE_A,$RELEASE_B)"
    return 0
  fi
  if (( OWNED_HELM )); then
    log "uninstalling recorded releases $RELEASE_A/$NS_A and $RELEASE_B/$NS_B"
    helm uninstall "$RELEASE_A" --namespace "$NS_A" --wait --timeout 180s >/dev/null 2>&1 || true
    helm uninstall "$RELEASE_B" --namespace "$NS_B" --wait --timeout 180s >/dev/null 2>&1 || true
    kubectl delete namespace "$NS_A" --wait=true --timeout=180s >/dev/null 2>&1 || true
    kubectl delete namespace "$NS_B" --wait=true --timeout=180s >/dev/null 2>&1 || true
  fi
  if (( OWNED_KIND )); then
    log "deleting kind cluster $KIND_CLUSTER"
    kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
  fi
  if [[ -n "${PREV_CONTEXT:-}" ]]; then
    kubectl config use-context "$PREV_CONTEXT" >/dev/null 2>&1 || true
  fi
  if (( status != 0 )); then
    log "cleanup finished after failure (exit $status)"
  fi
}

phase_run() {
  if [[ "${CURIE_TWO_RELEASE_ALLOW_LIVE:-}" != "1" ]]; then
    echo "PHASE=run refuses to start without CURIE_TWO_RELEASE_ALLOW_LIVE=1 (this guard keeps a laptop invocation from touching Slack or a cluster)." >&2
    exit 1
  fi
  refuse_foreign_cluster_name
  PREV_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
  trap cleanup EXIT

  ensure_kind_cluster
  create_owned_namespace "$NS_A"
  create_owned_namespace "$NS_B"
  OWNED_HELM=1
  write_api_values
  write_slack_values
  install_owner_release
  install_consumer_release

  local svc_a svc_b disp_a disp_b
  svc_a="$(fullname "$RELEASE_A")-api"
  svc_b="$(fullname "$RELEASE_B")-api"
  disp_a="$(fullname "$RELEASE_A")-dispatcher"
  disp_b="$(fullname "$RELEASE_B")-dispatcher"
  kubectl --namespace "$NS_A" rollout status "deploy/${svc_a}" --timeout=300s
  kubectl --namespace "$NS_B" rollout status "deploy/${svc_b}" --timeout=300s
  kubectl --namespace "$NS_A" rollout status "deploy/${disp_a}" --timeout=300s
  kubectl --namespace "$NS_B" rollout status "deploy/${disp_b}" --timeout=300s

  PF_A_PID="$(start_port_forward "$NS_A" "$svc_a" 18080)"
  PF_B_PID="$(start_port_forward "$NS_B" "$svc_b" 18081)"
  export CURIE_API_KEY="$API_KEY"
  wait_http "http://127.0.0.1:18080/health" || die "owner API did not become reachable"
  wait_http "http://127.0.0.1:18081/health" || die "consumer API did not become reachable"

  local created approval_id
  created="$(create_approval_on_a "http://127.0.0.1:18080")"
  approval_id="$(python3 - "$created" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
status = d.get("status")
if status not in (200, 201):
    raise SystemExit("create approval on A failed: " + sys.argv[1])
body = d.get("body") or {}
print(body["id"])
PY
)"
  log "created approval $approval_id on owner release A"

  observe_consumer_once "http://127.0.0.1:18080" "http://127.0.0.1:18081" "$approval_id"
  local consumer_logs
  consumer_logs="$(snapshot_consumer_logs_once)"
  observe_owner_once "http://127.0.0.1:18080" "$approval_id"
  resolve_consumer_once "http://127.0.0.1:18080" "http://127.0.0.1:18081" "$approval_id"
  local owner_resolve a_resolve_row a_resolve_reason
  owner_resolve="$(resolve_owner_once "http://127.0.0.1:18080" "$approval_id")"
  a_resolve_row="${owner_resolve%%$'\t'*}"
  a_resolve_reason="${owner_resolve#*$'\t'}"

  local live_row="BLOCKED"
  local live_reason="dedicated CI Slack app credentials are not in repo secrets; live ownership observation unavailable. Do not use unknown local bot token."
  if [[ -n "${CI_SLACK_APP_TOKEN:-}" && -n "${CI_SLACK_BOT_TOKEN:-}" ]]; then
    live_reason="actual authenticated Slack button interaction unavailable; a human card click cannot be impersonated. Operator resolve is not the owner-only Slack proof."
    if printf '%s\n' "$consumer_logs" | grep -q "may be owned by another Curie release"; then
      log "consumer dispatcher logged an ownership miss on the one-shot observation"
    else
      log "consumer dispatcher did not log an ownership miss on this one-shot (no envelope was injected)"
    fi
  fi

  write_summary "$(cat <<EOF
### Two-release approval e2e

- helm two-release (owner CRDs, consumer --skip-crds + values-e2e-two-release-consumer.yaml): PASS
- create approval on A: PASS ($approval_id)
- API isolation: PASS
- API isolation one-shot B (404 approval not found): PASS
- API isolation one-shot A (still pending): PASS
- B one-shot resolve miss (404 approval not found): PASS
- A resolve-once: ${a_resolve_row} (${a_resolve_reason})
- deployed dispatcher envelope ownership: BLOCKED (no one-shot envelope was delivered)
- live Slack owner-only envelope: ${live_row} (${live_reason})
EOF
)"
  echo "two-release-approval-e2e: helm two-release PASS; API isolation PASS; B one-shot resolve miss PASS; A resolve $a_resolve_row; live Slack envelope $live_row" >&2
}

case "$PHASE" in
  prereqs) phase_prereqs ;;
  run) phase_run ;;
  *)
    echo "usage: two-release-approval-e2e.sh {prereqs|run}" >&2
    exit 2
    ;;
esac
