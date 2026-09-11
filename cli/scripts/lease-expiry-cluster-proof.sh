#!/usr/bin/env bash
# Disposable two-worker cluster proof for lease-expiry reclaim (#2453 / #2433).
#
# Stands up a task-owned kind cluster, installs Curie with two worker replicas
# and reclaim_min_idle_ms at its default 900000, drives three concurrent test
# Slack mentions into a handler failure, and records placeholder edits, XPENDING
# delivery increments, the no-lease 900 s backstop control, and SIGKILL takeover.
#
# Refuses the permanent soak (namespace/release `curie`, namespace `default`).
# Never shortens reclaim_min_idle_ms. Never messages a human channel. Identifiers
# in public output are redacted.
#
# Usage:
#   curie dev lease-expiry-cluster-proof [--force] [--keep] [--json]
#   bash cli/scripts/lease-expiry-cluster-proof.sh --self-test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SELF_TEST=0
FORCE=0
KEEP=0
JSON=0
SKIP_INSTALL=0
BIN="${CURIE_BIN:-}"
NAMESPACE="${CURIE_E2E_NAMESPACE:-acme-2453}"
RELEASE="${CURIE_E2E_RELEASE:-t2453}"
KIND_CLUSTER="${CURIE_E2E_KIND_CLUSTER:-curie-t2453}"
WORKER_IMAGE="${CURIE_E2E_WORKER_IMAGE:-ghcr.io/curie-eng/curie-worker:0.8.7}"
API_IMAGE="${CURIE_E2E_API_IMAGE:-ghcr.io/curie-eng/curie-api:0.8.7}"
DISPATCHER_IMAGE="${CURIE_E2E_DISPATCHER_IMAGE:-ghcr.io/curie-eng/curie-dispatcher:0.8.7}"
RUNNER_IMAGE="${CURIE_E2E_RUNNER_IMAGE:-ghcr.io/curie-eng/curie-runner:0.8.7}"
UI_IMAGE="${CURIE_E2E_UI_IMAGE:-ghcr.io/curie-eng/curie-ui:0.8.7}"
KUBECONFIG_FILE="${CURIE_E2E_KUBECONFIG:-$REPO_ROOT/.projects/kubeconfig-t2453}"
EVIDENCE_DIR="${CURIE_E2E_EVIDENCE_DIR:-$REPO_ROOT/.projects/2453-evidence}"
PLUGIN_DIR="${CURIE_E2E_PLUGIN_DIR:-$REPO_ROOT/examples/coder}"
AGENT_NAME="${CURIE_E2E_AGENT:-acme-2453-bot}"
STREAM="curie:runs"
GROUP="curie-workers"
LEASE_TTL_S=45
OBSERVE_S="${CURIE_E2E_OBSERVE_S:-120}"
PLACEHOLDER_WAIT_S="${CURIE_E2E_PLACEHOLDER_WAIT_S:-90}"
TURN_NOT_STARTED_TEXT="I ran into a problem and could not finish this request, so if no answer appears here shortly, please send it again."
PLACEHOLDER_TEXT="On it. Working on your request."
# Connected-transport cluster message posts an ellipsis placeholder (ADR-0078).
CONNECTED_PLACEHOLDER=$'\u2026'
CANDIDATE=""
OWNED_KIND=0
OWNED_HELM=0
export KUBECONFIG="$KUBECONFIG_FILE"

log() { printf '%s\n' "$*" >&2; }

die() {
    log "error: $*"
    exit 1
}

usage() {
    cat <<'EOF' >&2
usage: lease-expiry-cluster-proof.sh [--force] [--keep] [--json] [--self-test]
EOF
}

is_soak_namespace() {
    local ns="${1:-}"
    [[ "$ns" == "curie" || "$ns" == "default" ]]
}

is_soak_release() {
    local rel="${1:-}"
    [[ "$rel" == "curie" ]]
}

refuse_soak() {
    local ns="${1:-}" rel="${2:-}"
    if is_soak_namespace "$ns"; then
        die "refusing soak namespace '$ns' (permanent soak / shared default). Use a task-owned CURIE_E2E_NAMESPACE."
    fi
    if is_soak_release "$rel"; then
        die "refusing soak release '$rel'. Use a task-owned CURIE_E2E_RELEASE."
    fi
}

# A helm --set that names reclaim_min_idle would silently invalidate the default
# 900000 pin this issue requires. The worker config has no env alias for that
# field; still refuse any operator set that mentions it.
forbids_reclaim_override() {
    local arg
    for arg in "$@"; do
        if [[ "$arg" == *reclaim_min_idle* || "$arg" == *RECLAIM_MIN_IDLE* || "$arg" == *lease_expired_idle* || "$arg" == *LEASE_EXPIRED_IDLE* ]]; then
            return 1
        fi
    done
    return 0
}

slack_credentials_present() {
    [[ -n "${SLACK_BOT_TOKEN:-}" && -n "${SLACK_APP_TOKEN:-}" && -n "${SLACK_TEST_CHANNEL:-}" ]]
}

require_slack() {
    if ! slack_credentials_present; then
        die "missing Slack credentials; set SLACK_BOT_TOKEN, SLACK_APP_TOKEN, and SLACK_TEST_CHANNEL to the authorized test route"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --force) FORCE=1; shift ;;
            --keep) KEEP=1; shift ;;
            --json) JSON=1; shift ;;
            --self-test) SELF_TEST=1; shift ;;
            --skip-install) SKIP_INSTALL=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown argument: $1" ;;
        esac
    done
}

run_self_test() {
    local failed=0
    if is_soak_namespace "curie" && is_soak_namespace "default" && ! is_soak_namespace "acme-2453"; then
        log "soak namespace curie refused"
        log "soak namespace default refused"
    else
        log "self-test: soak namespace helper is wrong"
        failed=1
    fi
    if is_soak_release "curie" && ! is_soak_release "t2453"; then
        log "soak release curie refused"
    else
        log "self-test: soak release helper is wrong"
        failed=1
    fi
    if forbids_reclaim_override "--set" "worker.replicas=2" \
        && ! forbids_reclaim_override "--set" "worker.extraEnv[0].name=CURIE_RECLAIM_MIN_IDLE_MS" \
        && ! forbids_reclaim_override "--set" "reclaim_min_idle_ms=1000"; then
        log "reclaim_min_idle override refused"
        log "default reclaim_min_idle_ms 900000 preserved"
    else
        log "self-test: reclaim override helper is wrong"
        failed=1
    fi
    (
        unset SLACK_BOT_TOKEN SLACK_APP_TOKEN SLACK_TEST_CHANNEL || true
        if slack_credentials_present; then
            log "self-test: missing slack helper is wrong"
            exit 1
        fi
        log "missing slack credentials refused"
    ) || failed=1
    (( failed == 0 )) || die "self-test failed"
    log "self-test passed"
    if (( JSON )); then
        printf '%s\n' '{"status":"self-test","issue":2453,"reclaim_min_idle_ms":900000}'
    fi
}

resolve_bin() {
    if [[ -n "$BIN" && -x "$BIN" ]]; then
        return 0
    fi
    if [[ -x "$REPO_ROOT/cli/target/debug/curie" ]]; then
        BIN="$REPO_ROOT/cli/target/debug/curie"
        return 0
    fi
    if command -v curie >/dev/null 2>&1; then
        BIN="$(command -v curie)"
        return 0
    fi
    die "CURIE_BIN must name an executable curie built from this checkout"
}

candidate_identity() {
    CANDIDATE="$(git -C "$REPO_ROOT" rev-parse HEAD)"
}

redact() {
    python3 -c '
import re, sys
text = sys.stdin.read()
text = re.sub(r"xoxb-[A-Za-z0-9-]+", "xoxb-REDACTED", text)
text = re.sub(r"xapp-[A-Za-z0-9-]+", "xapp-REDACTED", text)
text = re.sub(r"\bC[A-Z0-9]{8,}\b", "C0EXAMPLE1", text)
text = re.sub(r"\bU[A-Z0-9]{8,}\b", "U0EXAMPLE1", text)
text = re.sub(r"\bT[A-Z0-9]{8,}\b", "T0EXAMPLE1", text)
text = re.sub(r"\bB[A-Z0-9]{8,}\b", "B0EXAMPLE1", text)
text = re.sub(r"\b[0-9]{10}\.[0-9]{3,}\b", "TS_REDACTED", text)
sys.stdout.write(text)
'
}

kubectl_ns() {
    kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" "$@"
}

helm_ns() {
    helm --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" "$@"
}

fullname() {
    # Release name does not contain "curie", so Helm fullname is <release>-curie.
    printf '%s-curie' "$RELEASE"
}

cleanup() {
    local status=$?
    if (( status != 0 )); then
        dump_diagnostics || true
    fi
    if (( KEEP )); then
        log "keeping owned resources (kind=$KIND_CLUSTER ns=$NAMESPACE release=$RELEASE)"
        return 0
    fi
    if (( OWNED_HELM )); then
        log "uninstalling release $RELEASE in $NAMESPACE"
        helm_ns uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
        kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
    fi
    if (( OWNED_KIND )); then
        log "deleting kind cluster $KIND_CLUSTER"
        kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
    fi
    if (( status != 0 )); then
        log "cleanup finished after failure (exit $status)"
    fi
}

slack_api() {
    python3 - "$@" <<'PY'
import json, os, sys, urllib.parse, urllib.request

method = sys.argv[1]
token = os.environ["SLACK_BOT_TOKEN"]
params = {}
for raw in sys.argv[2:]:
    key, _, value = raw.partition("=")
    params[key] = value
data = urllib.parse.urlencode(params).encode()
req = urllib.request.Request(
    f"https://slack.com/api/{method}",
    data=data,
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    },
    method="POST",
)
with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read().decode())
if not payload.get("ok"):
    print(json.dumps({"ok": False, "error": payload.get("error")}), file=sys.stderr)
    sys.exit(1)
print(json.dumps(payload))
PY
}

post_mentions() {
    # Bolt IgnoringSelfEvents drops a self-authored app_mention, so a single
    # test-bot token cannot drive the ingest listener. The connected-transport
    # cluster message path posts a real Slack placeholder and enqueues the same
    # QueuedTurn shape a mention would (#770 / the #2454 host-process observation)
    # without messaging a human.
    local i payload ts
    MENTION_TS=()
    for i in 1 2 3; do
        payload="$("$BIN" --json cluster message \
            --namespace "$NAMESPACE" \
            --release "$RELEASE" \
            --channel "$SLACK_TEST_CHANNEL" \
            "2453-proof-${i} concurrent lease-expiry pin, please ignore")"
        ts="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("thread") or "")' <<<"$payload")"
        [[ -n "$ts" ]] || die "cluster message did not return thread ts: $(printf '%s' "$payload" | redact)"
        MENTION_TS+=("$ts")
        log "enqueued connected-transport turn $i"
    done
}

thread_snapshot() {
    local ts="$1"
    slack_api conversations.replies \
        "channel=${SLACK_TEST_CHANNEL}" \
        "ts=${ts}" \
        "inclusive=true"
}

wait_placeholders() {
    local deadline=$((SECONDS + PLACEHOLDER_WAIT_S))
    local ts snap texts
    while (( SECONDS < deadline )); do
        local ready=0
        for ts in "${MENTION_TS[@]}"; do
            snap="$(thread_snapshot "$ts")"
            texts="$(python3 -c 'import json,sys; msgs=json.load(sys.stdin).get("messages") or []; print("\n".join(m.get("text") or "" for m in msgs))' <<<"$snap")"
            if grep -Fq "$PLACEHOLDER_TEXT" <<<"$texts" \
                || grep -Fq "$CONNECTED_PLACEHOLDER" <<<"$texts" \
                || grep -Fq "$TURN_NOT_STARTED_TEXT" <<<"$texts"; then
                ready=$((ready + 1))
            fi
        done
        if (( ready == 3 )); then
            log "observed placeholders on all three threads"
            return 0
        fi
        sleep 2
    done
    die "did not observe three placeholders within ${PLACEHOLDER_WAIT_S}s"
}

assert_edited_in_place() {
    local ts snap
    for ts in "${MENTION_TS[@]}"; do
        snap="$(thread_snapshot "$ts")"
        TURN_NOT_STARTED_TEXT="$TURN_NOT_STARTED_TEXT" PLACEHOLDER_TEXT="$PLACEHOLDER_TEXT" \
        CONNECTED_PLACEHOLDER="$CONNECTED_PLACEHOLDER" \
            python3 -c '
import json, os, sys
notice = os.environ["TURN_NOT_STARTED_TEXT"]
placeholders = {os.environ["PLACEHOLDER_TEXT"], os.environ["CONNECTED_PLACEHOLDER"]}
data = json.load(sys.stdin)
msgs = data.get("messages") or []
texts = [m.get("text") or "" for m in msgs]
print("thread_message_count", len(msgs), file=sys.stderr)
for t in texts:
    print("thread_text_len", len(t), file=sys.stderr)
if len(msgs) < 1 or len(msgs) > 2:
    raise SystemExit(f"thread has {len(msgs)} messages, want 1 or 2 with no extras")
if notice not in texts:
    raise SystemExit("placeholder was not edited to CURIE_TURN_NOT_STARTED_TEXT")
if any(p in texts for p in placeholders if p):
    raise SystemExit("original placeholder text is still present")
print("ok")
' <<<"$snap" >/dev/null
        log "thread edited in place with no extra messages"
    done
}

valkey_cmd() {
    local pod
    pod="$(kubectl_ns get pod -l app.kubernetes.io/component=valkey -o jsonpath='{.items[0].metadata.name}')"
    [[ -n "$pod" ]] || die "valkey pod not found"
    # The password is already in the container env; do not read the Secret here.
    kubectl_ns exec "$pod" -- sh -c 'valkey-cli -a "$VALKEY_PASSWORD" --no-auth-warning "$@"' sh "$@"
}

xpending_dump() {
    valkey_cmd XPENDING "$STREAM" "$GROUP" - + 100
}

scale_workers() {
    local replicas="$1"
    kubectl_ns scale "deploy/$(fullname)-worker" --replicas="$replicas"
    if (( replicas > 0 )); then
        kubectl_ns rollout status "deploy/$(fullname)-worker" --timeout=180s
    else
        kubectl_ns wait --for=delete pod -l app.kubernetes.io/component=worker --timeout=120s || true
    fi
}

break_postgres() {
    mkdir -p "$EVIDENCE_DIR"
    local svc
    svc="$(fullname)-postgres"
    kubectl_ns get svc "$svc" -o yaml >"$EVIDENCE_DIR/postgres-svc.yaml"
    kubectl_ns delete svc "$svc" --wait=true
    local i
    for i in $(seq 1 30); do
        if ! kubectl_ns get svc "$svc" >/dev/null 2>&1; then
            log "deleted postgres Service $svc so BindingResolver.resolve fails immediately (DNS)"
            return 0
        fi
        sleep 1
    done
    die "postgres Service $svc still present after delete"
}

restore_postgres() {
    if [[ -f "$EVIDENCE_DIR/postgres-svc.yaml" ]]; then
        kubectl_ns apply -f "$EVIDENCE_DIR/postgres-svc.yaml" >/dev/null
        log "restored postgres Service"
    fi
}

dump_diagnostics() {
    log "dumping diagnostics"
    local ts pod
    for ts in "${MENTION_TS[@]:-}"; do
        log "thread $ts"
        thread_snapshot "$ts" 2>/dev/null | redact >&2 || true
    done
    kubectl_ns get pods -o wide 2>/dev/null | redact >&2 || true
    for pod in $(kubectl_ns get pod -l app.kubernetes.io/component=worker -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
        log "worker logs $pod"
        kubectl_ns logs "$pod" --tail=120 2>/dev/null | redact >&2 || true
    done
    xpending_dump 2>/dev/null | redact >&2 || true
}

ensure_kind() {
    mkdir -p "$(dirname "$KUBECONFIG_FILE")" "$EVIDENCE_DIR"
    if kind get clusters 2>/dev/null | grep -Fxq "$KIND_CLUSTER"; then
        log "recreating leftover kind cluster $KIND_CLUSTER"
        kind delete cluster --name "$KIND_CLUSTER"
    fi
    kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG_FILE"
    OWNED_KIND=1
}

load_images() {
    local img
    for img in "$WORKER_IMAGE" "$API_IMAGE" "$DISPATCHER_IMAGE" "$RUNNER_IMAGE" "$UI_IMAGE"; do
        if ! docker image inspect "$img" >/dev/null 2>&1; then
            log "pulling $img"
            docker pull "$img"
        fi
        log "kind load $img"
        kind load docker-image "$img" --name "$KIND_CLUSTER"
    done
}

cluster_up() {
    local worker_tag api_tag dispatcher_tag runner_tag ui_tag
    worker_tag="${WORKER_IMAGE##*:}"
    api_tag="${API_IMAGE##*:}"
    dispatcher_tag="${DISPATCHER_IMAGE##*:}"
    runner_tag="${RUNNER_IMAGE##*:}"
    ui_tag="${UI_IMAGE##*:}"
    local sets=(
        --set "security.gvisor.mode=off"
        --set "worker.replicas=2"
        --set "worker.image.repository=ghcr.io/curie-eng/curie-worker"
        --set "worker.image.tag=${worker_tag}"
        --set "worker.image.pullPolicy=IfNotPresent"
        --set "api.image.repository=ghcr.io/curie-eng/curie-api"
        --set "api.image.tag=${api_tag}"
        --set "api.image.pullPolicy=IfNotPresent"
        --set "dispatcher.image.repository=ghcr.io/curie-eng/curie-dispatcher"
        --set "dispatcher.image.tag=${dispatcher_tag}"
        --set "dispatcher.image.pullPolicy=IfNotPresent"
        --set "ui.image.repository=ghcr.io/curie-eng/curie-ui"
        --set "ui.image.tag=${ui_tag}"
        --set "ui.image.pullPolicy=IfNotPresent"
        --set "agentSandbox.runner.image=ghcr.io/curie-eng/curie-runner"
        --set "agentSandbox.runner.tag=${runner_tag}"
        --set "agentSandbox.runner.imagePullPolicy=IfNotPresent"
        --set "agentSandbox.runner.prewarm.imagePullPolicy=IfNotPresent"
        --set "langfuse.deploy=false"
        --set "langfuse.host=langfuse.example.com"
        --set "clickhouse.deploy=false"
        --set "ui.deploy=false"
        --set "mailAdapter.deploy=false"
        --set "otelCollector.deploy=false"
        --set "otelCollector.telemetryDisabled=true"
    )
    forbids_reclaim_override "${sets[@]}" || die "internal error: helm set would override reclaim timing"
    log "curie cluster up --namespace $NAMESPACE --release $RELEASE (worker $WORKER_IMAGE)"
    "$BIN" cluster up \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$REPO_ROOT/charts/curie" \
        --dev \
        --fake-model \
        --no-expose \
        "${sets[@]}"
    OWNED_HELM=1
}

connect_slack_and_deploy() {
    log "connecting test Slack route via cluster comms"
    "$BIN" cluster comms --slack \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$REPO_ROOT/charts/curie"
    kubectl_ns rollout status "deploy/$(fullname)-dispatcher" --timeout=180s
    log "deploying $AGENT_NAME from $PLUGIN_DIR"
    "$BIN" cluster deploy \
        --plugin-dir "$PLUGIN_DIR" \
        --agent "$AGENT_NAME" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$REPO_ROOT/charts/curie" \
        --slack-channel "$SLACK_TEST_CHANNEL"
}

observe_xpending() {
    local start="$SECONDS"
    local dump
    log "observing XPENDING for ${OBSERVE_S}s (lease TTL ${LEASE_TTL_S}s)"
    while (( SECONDS - start < OBSERVE_S )); do
        dump="$(xpending_dump 2>/dev/null || true)"
        printf '%s\n' "t+$((SECONDS - start))s XPENDING:" | redact >&2
        printf '%s\n' "$dump" | redact >&2
        printf '%s\n' "$dump" >>"$EVIDENCE_DIR/xpending.log"
        sleep 15
    done
}

inject_nolease_and_watch() {
    local before after payload
    payload='{"event_id":"2453-nolease","conversation_id":"2453-nolease","author":"U0EXAMPLE1","text":"nolease control","reply_handle":{"kind":"slack","channel":"C0EXAMPLE1","placeholder":null},"received_at":"2026-09-09T00:00:00Z","source":"slack"}'
    log "scaling workers to 0 so XREADGROUP can park the no-lease row"
    scale_workers 0
    valkey_cmd XGROUP CREATE "$STREAM" "$GROUP" 0 MKSTREAM >/dev/null 2>&1 || true
    valkey_cmd XADD "$STREAM" "*" payload "$payload" >/dev/null
    valkey_cmd XREADGROUP GROUP "$GROUP" pre-lease-2453 COUNT 1 STREAMS "$STREAM" ">" >/dev/null
    before="$(xpending_dump)"
    log "xpending after no-lease inject:"
    printf '%s\n' "$before" | redact >&2
    if ! grep -q "pre-lease-2453" <<<"$before"; then
        die "XREADGROUP did not park the no-lease row on pre-lease-2453"
    fi
    log "injected no-lease PEL row owned by pre-lease-2453; restoring two workers"
    scale_workers 2
    sleep "$OBSERVE_S"
    after="$(xpending_dump)"
    log "xpending after no-lease observe:"
    printf '%s\n' "$after" | redact >&2
    if ! grep -q "pre-lease-2453" <<<"$after"; then
        die "no-lease row left the PEL before the 900s backstop"
    fi
    log "no-lease row still pending after ${OBSERVE_S}s (backstop is 900s)"
    { printf '%s\n' "$before"; printf '%s\n' "$after"; } | redact >>"$EVIDENCE_DIR/nolease.log"
}

sigkill_takeover() {
    local dump consumer pod start
    dump="$(xpending_dump)"
    consumer="$(printf '%s\n' "$dump" | grep -E 'curie-worker-' | head -1 || true)"
    if [[ -n "$consumer" ]]; then
        pod="${consumer%-*}"
    else
        pod="$(kubectl_ns get pod -l app.kubernetes.io/component=worker -o jsonpath='{.items[0].metadata.name}')"
    fi
    [[ -n "$pod" ]] || die "no worker pod to SIGKILL"
    log "SIGKILL worker pod $pod (PEL consumer ${consumer:-none}, --grace-period=0)"
    kubectl_ns delete pod "$pod" --force --grace-period=0
    start="$SECONDS"
    while (( SECONDS - start < OBSERVE_S )); do
        dump="$(xpending_dump 2>/dev/null || true)"
        printf '%s\n' "sigkill t+$((SECONDS - start))s XPENDING:" | redact >&2
        printf '%s\n' "$dump" | redact >&2
        printf '%s\n' "$dump" >>"$EVIDENCE_DIR/sigkill-xpending.log"
        sleep 15
    done
}

write_evidence() {
    local worker_id
    worker_id="$(docker image inspect "$WORKER_IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null || echo "$WORKER_IMAGE")"
    cat >"$EVIDENCE_DIR/summary.json" <<EOF
{
  "issue": 2453,
  "commit": "$CANDIDATE",
  "worker_image": "$WORKER_IMAGE",
  "worker_digest": "$worker_id",
  "kind_cluster": "$KIND_CLUSTER",
  "namespace": "$NAMESPACE",
  "release": "$RELEASE",
  "reclaim_min_idle_ms": 900000,
  "lease_ttl_s": $LEASE_TTL_S,
  "observe_s": $OBSERVE_S,
  "mentions": 3
}
EOF
    if (( JSON )); then
        cat "$EVIDENCE_DIR/summary.json"
    fi
}

run_live() {
    refuse_soak "$NAMESPACE" "$RELEASE"
    require_slack
    resolve_bin
    candidate_identity
    mkdir -p "$EVIDENCE_DIR"
    trap cleanup EXIT
    if (( SKIP_INSTALL )); then
        [[ -f "$KUBECONFIG_FILE" ]] || die "skip-install requires $KUBECONFIG_FILE"
        OWNED_KIND=1
        OWNED_HELM=1
        log "skipping kind/helm install; using existing $KIND_CLUSTER $NAMESPACE/$RELEASE"
    else
        ensure_kind
        load_images
        cluster_up
        connect_slack_and_deploy
    fi
    scale_workers 0
    post_mentions
    wait_placeholders
    log "placeholders observed; breaking postgres"
    break_postgres
    scale_workers 2
    local deadline=$((SECONDS + PLACEHOLDER_WAIT_S))
    local ok=0
    while (( SECONDS < deadline )); do
        if assert_edited_in_place; then
            ok=1
            break
        fi
        sleep 3
    done
    if (( ! ok )); then
        dump_diagnostics || true
        assert_edited_in_place
    fi
    # SIGKILL while the failed-turn rows are still pending with times_delivered=1
    # so the surviving replica's lease-expiry pass is the takeover, not a later
    # empty PEL. The no-lease control is injected after that snapshot.
    sigkill_takeover
    inject_nolease_and_watch
    restore_postgres
    write_evidence
    log "cluster proof observations recorded under $EVIDENCE_DIR"
}

main() {
    parse_args "$@"
    if (( SELF_TEST )); then
        run_self_test
        return 0
    fi
    run_live
}

main "$@"
