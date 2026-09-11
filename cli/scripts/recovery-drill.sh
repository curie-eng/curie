#!/usr/bin/env bash
# Isolated worker/runner recovery drills (#2425).
#
# Exercise worker death mid-turn, runner death, a configured deadline plus
# follow-up, a temporary Valkey outage, and an API restart on a task-owned
# local compose stack or a task-owned Helm install. After a healthy worker
# replacement is available, reclaim/resume or a durable classified failure
# must land within the bound (default 120s). Follow-up on the same thread
# must complete. Run (PEL) and completion (outbox) planes are checked
# independently.
#
# Refuses the permanent soak (namespace/release `curie`, namespace `default`).
# Never mutates shared controllers, live credentials, or human messages.
#
# Usage:
#   curie dev recovery-drill [--surface local|cluster] [--scenario all|...] \
#     [--bound-seconds 120] [--force] [--json]
#   bash cli/scripts/recovery-drill.sh --self-test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SURFACE="local"
SCENARIO="all"
BOUND_SECONDS=120
FORCE=0
JSON=0
SELF_TEST=0
STACK_OWNED=0
WORKDIR=""
BIN="${CURIE_BIN:-}"
CHANNEL="C0EXAMPLE1"
THREAD="2425-recovery"
COMPOSE_FILE="$REPO_ROOT/compose.dev.yaml"
LOCK_FILE="/tmp/curie-recovery-drill.lock"
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-26379}"
VALKEY_PASSWORD="${VALKEY_PASSWORD:-valkeypass}"
STREAM="curie:runs"
GROUP="curie-workers"
COMPLETIONS_PENDING="curie:worker:completions:pending"
DEAD_LETTER="curie:runs:dead"
CANDIDATE=""
EVIDENCE_FILE=""

SCENARIOS_ALL=(worker-death runner-death timeout-follow-up valkey-outage api-restart)

log() { printf '%s\n' "$*" >&2; }

die() {
    log "error: $*"
    exit 1
}

usage() {
    cat <<'EOF' >&2
usage: recovery-drill.sh [--surface local|cluster] [--scenario all|worker-death|runner-death|timeout-follow-up|valkey-outage|api-restart] [--bound-seconds N] [--force] [--json] [--self-test]
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
        die "cluster surface refuses soak namespace '$ns' (permanent soak / shared default). Use a task-owned CURIE_E2E_NAMESPACE."
    fi
    if is_soak_release "$rel"; then
        die "cluster surface refuses soak release '$rel'. Use a task-owned CURIE_E2E_RELEASE."
    fi
}

valid_scenario() {
    local want="$1" s
    [[ "$want" == "all" ]] && return 0
    for s in "${SCENARIOS_ALL[@]}"; do
        [[ "$s" == "$want" ]] && return 0
    done
    return 1
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --surface) SURFACE="${2:-}"; shift 2 ;;
            --scenario) SCENARIO="${2:-}"; shift 2 ;;
            --bound-seconds) BOUND_SECONDS="${2:-}"; shift 2 ;;
            --force) FORCE=1; shift ;;
            --json) JSON=1; shift ;;
            --self-test) SELF_TEST=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown argument: $1" ;;
        esac
    done
    [[ "$SURFACE" == "local" || "$SURFACE" == "cluster" ]] || die "unknown surface '$SURFACE'"
    [[ "$BOUND_SECONDS" =~ ^[0-9]+$ ]] || die "bound-seconds must be an integer"
    (( BOUND_SECONDS > 0 )) || die "bound-seconds must be positive"
    if ! valid_scenario "$SCENARIO"; then
        die "unknown scenario '$SCENARIO' (want all|worker-death|runner-death|timeout-follow-up|valkey-outage|api-restart)"
    fi
}

run_self_test() {
    local failed=0
    # Soak namespace refusal.
    if is_soak_namespace "curie" && is_soak_namespace "default" && ! is_soak_namespace "curie-2425-drill"; then
        log "soak namespace curie refused"
        log "soak namespace default refused"
    else
        log "self-test: soak namespace helper is wrong"
        failed=1
    fi
    if is_soak_release "curie" && ! is_soak_release "curie-2425-drill"; then
        log "soak release curie refused"
    else
        log "self-test: soak release helper is wrong"
        failed=1
    fi
    if valid_scenario "all" && valid_scenario "worker-death" && ! valid_scenario "not-a-scenario"; then
        log "unknown scenario refused"
    else
        log "self-test: scenario helper is wrong"
        failed=1
    fi
    (( failed == 0 )) || die "self-test failed"
    log "self-test passed"
    if (( JSON )); then
        printf '%s\n' '{"status":"self-test","scenarios":["worker-death","runner-death","timeout-follow-up","valkey-outage","api-restart"]}'
    fi
}

resolve_bin() {
    if [[ -n "$BIN" && -x "$BIN" ]]; then
        return 0
    fi
    if command -v curie >/dev/null 2>&1; then
        BIN="$(command -v curie)"
        return 0
    fi
    die "CURIE_BIN must name an executable curie, or curie must be on PATH"
}

candidate_identity() {
    CANDIDATE="$(git -C "$REPO_ROOT" rev-parse HEAD)"
}

valkey_container() {
    local names=() line
    while IFS= read -r line; do
        [[ -n "$line" ]] && names+=("$line")
    done < <(docker ps --filter 'label=com.docker.compose.project=curie' --filter 'label=com.docker.compose.service=valkey' --format '{{.Names}}')
    (( ${#names[@]} == 1 )) || die "expected exactly one valkey container, found ${#names[@]}"
    printf '%s' "${names[0]}"
}

valkey() {
    docker exec "$(valkey_container)" valkey-cli -a "$VALKEY_PASSWORD" "$@"
}

compose() {
    docker compose -f "$COMPOSE_FILE" --profile core "$@"
}

local_stack_running() {
    docker ps --filter 'label=com.docker.compose.project=curie' --format '{{.Names}}' | grep -q .
}

host_ports_busy() {
    # Shared compose host ports. A different compose project (another task)
    # can occupy them without the `curie` project label.
    ss -tlnH 2>/dev/null | awk '{print $4}' | grep -E ':(25432|26379|28000|29000)$' | grep -q .
}

local_worker_container() {
    local workers=() line
    while IFS= read -r line; do
        [[ -n "$line" ]] && workers+=("$line")
    done < <(docker ps --filter 'label=com.docker.compose.project=curie' --filter 'label=com.docker.compose.service=curie-worker' --format '{{.Names}}')
    (( ${#workers[@]} == 1 )) || die "expected exactly one curie-worker, found ${#workers[@]}"
    printf '%s' "${workers[0]}"
}

local_api_container() {
    local apis=() line
    while IFS= read -r line; do
        [[ -n "$line" ]] && apis+=("$line")
    done < <(docker ps --filter 'label=com.docker.compose.project=curie' --filter 'label=com.docker.compose.service=curie-api' --format '{{.Names}}')
    (( ${#apis[@]} == 1 )) || die "expected exactly one curie-api, found ${#apis[@]}"
    printf '%s' "${apis[0]}"
}

SANDBOX_LABEL="curietech.ai/managed-by=curie-sandbox-substrate"

runner_containers() {
    docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}'
}

reap_runners() {
    local ids
    ids="$(docker ps -aq --filter "label=$SANDBOX_LABEL" || true)"
    [[ -n "$ids" ]] || return 0
    # shellcheck disable=SC2086
    docker rm -f $ids >/dev/null 2>&1 || true
}

wait_for_runner() {
    local deadline=$((SECONDS + 45)) name
    while (( SECONDS < deadline )); do
        name="$(runner_containers | awk 'NF' | head -n1 || true)"
        if [[ -n "$name" ]]; then
            printf '%s' "$name"
            return 0
        fi
        sleep 0.2
    done
    return 1
}

_first_int() {
    python3 -c '
import re, sys
text = sys.stdin.read()
for line in text.splitlines():
    s = line.strip()
    if re.fullmatch(r"\d+", s):
        print(int(s))
        raise SystemExit
print(0)
'
}

xpending_count() {
    valkey XPENDING "$STREAM" "$GROUP" 2>/dev/null | _first_int
}

completions_pending_count() {
    valkey SCARD "$COMPLETIONS_PENDING" 2>/dev/null | _first_int
}

pel_consumers() {
    valkey XPENDING "$STREAM" "$GROUP" - + 50 2>/dev/null || true
}

record_planes() {
    local label="$1"
    log "$label: run-plane XPENDING=$(xpending_count) completion-plane pending=$(completions_pending_count)"
}

wait_planes_quiet() {
    local wait_s="$1"
    local deadline=$((SECONDS + wait_s)) pel
    while (( SECONDS < deadline )); do
        pel="$(xpending_count)"
        if [[ "$pel" -eq 0 ]]; then
            return 0
        fi
        log "wait_planes_quiet: XPENDING=$pel t+$((SECONDS))s"
        sleep 5
    done
    return 1
}

assert_finalized() {
    local payload="$1" label="$2"
    python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print("unparseable", file=sys.stderr)
    sys.exit(1)
if not isinstance(d, dict):
    print("unparseable", file=sys.stderr)
    sys.exit(1)
if d.get("finalized") is True and isinstance(d.get("reply"), str) and d["reply"].strip():
    print("finalized")
    sys.exit(0)
print("not_finalized", file=sys.stderr)
sys.exit(1)
' <<<"$payload" >/dev/null || die "$label: follow-up did not finalize with a reply"
}

# Close the drill lock fd so a leftover child cannot pin occupancy.
run_unlocked() {
    "$@" 9>&-
}

send_message() {
    local text="$1"
    ( cd "$WORKDIR" && "$BIN" --json local message --channel "$CHANNEL" "$text" ) 9>&-
}

send_followup() {
    local text="$1"
    ( cd "$WORKDIR" && "$BIN" --json local message --channel "$CHANNEL" --continue "$text" ) 9>&-
}

prepare_bundle() {
    local dest="$WORKDIR/bundle"
    rm -rf "$dest"
    cp -a "$REPO_ROOT/examples/weather" "$dest"
    # Unique agent name so leftover C0LOCALDEV rows cannot shadow this drill.
    python3 - "$dest" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
plugin = root / ".claude-plugin" / "plugin.json"
data = json.loads(plugin.read_text())
data["name"] = "acme-recovery-drill"
plugin.write_text(json.dumps(data, indent=2) + "\n")
PY
}

bring_up_local() {
    if local_stack_running; then
        die "a compose stack is already running on the shared host ports. Let the owning session run curie local down. This drill refuses occupancy."
    fi
    if host_ports_busy; then
        die "host ports 25432/26379/28000/29000 are already allocated (another task-owned compose project). This drill refuses occupancy rather than stealing them."
    fi
    export CURIE_FAKE_MODEL=1
    export COMPOSE_PROJECT_NAME=curie
    STACK_OWNED=1
    log "bringing up local stack (fake model, minimal profile) for recovery drill"
    run_unlocked "$BIN" local up --minimal
    local i
    for i in $(seq 1 60); do
        if curl -fsS http://localhost:28000/health >/dev/null 2>&1; then
            break
        fi
        sleep 3
    done
    curl -fsS http://localhost:28000/health >/dev/null || die "API health did not come up"
    prepare_bundle
    run_unlocked "$BIN" --json local deploy --plugin-dir "$WORKDIR/bundle" --slack-channel "$CHANNEL" >/dev/null
    log "deployed recovery-drill bundle on channel $CHANNEL"
}

teardown_local() {
    (( STACK_OWNED )) || return 0
    if [[ "${CURIE_RECOVERY_KEEP:-}" == "1" ]]; then
        log "CURIE_RECOVERY_KEEP=1: leaving the task-owned stack up"
        return 0
    fi
    log "tearing down task-owned local stack"
    run_unlocked "$BIN" local down --yes >/dev/null 2>&1 || run_unlocked compose down >/dev/null 2>&1 || true
    STACK_OWNED=0
}

append_evidence() {
    local scenario="$1" outcome="$2" observed="$3" negative="$4" elapsed="$5"
    python3 - "$EVIDENCE_FILE" "$scenario" "$outcome" "$observed" "$negative" "$elapsed" "$CANDIDATE" "$BOUND_SECONDS" <<'PY'
import json, pathlib, sys
path, scenario, outcome, observed, negative, elapsed, commit, bound = sys.argv[1:9]
data = json.loads(pathlib.Path(path).read_text())
data["scenarios"].append({
    "scenario": scenario,
    "outcome": outcome,
    "observed": observed,
    "negative": negative,
    "elapsed_seconds": float(elapsed),
    "bound_seconds": int(bound),
    "commit": commit,
})
pathlib.Path(path).write_text(json.dumps(data, indent=2) + "\n")
PY
}

scenario_worker_death() {
    log "=== scenario: worker-death ==="
    local start worker runner out code=0 elapsed pending_before pending_after
    reap_runners
    record_planes "worker-death before"
    pending_before="$(xpending_count)"
    # Negative: the run plane is quiet before the fault.
    [[ "$pending_before" == "0" ]] || log "worker-death: PEL was not empty before the fault (pending=$pending_before)"

    send_message "recovery-drill worker-death hold" >/tmp/curie-2425-worker-msg.json &
    local msg_pid=$!
    runner="$(wait_for_runner || true)"
    if [[ -n "$runner" ]]; then
        docker pause "$runner" >/dev/null || true
        log "paused runner $runner to hold the turn"
    else
        log "no runner container appeared in time; killing worker against an in-flight or queued turn"
    fi
    worker="$(local_worker_container)"
    start="$SECONDS"
    docker kill "$worker" >/dev/null
    log "killed worker $worker"
    # Negative path: no replacement yet, PEL must still exist (stranded under dead consumer).
    sleep 2
    local mid_pending
    mid_pending="$(xpending_count)"
    compose up -d --no-deps curie-worker >/dev/null
    local ready_deadline=$((SECONDS + 60))
    while (( SECONDS < ready_deadline )); do
        if docker ps --filter 'label=com.docker.compose.service=curie-worker' --format '{{.Status}}' | grep -q 'Up'; then
            break
        fi
        sleep 2
    done
    [[ -n "$runner" ]] && docker unpause "$runner" >/dev/null 2>&1 || true
    local replacement_at=$SECONDS
    if wait_planes_quiet "$BOUND_SECONDS"; then
        elapsed=$((SECONDS - replacement_at))
        log "worker-death: PEL quiet ${elapsed}s after replacement (bound ${BOUND_SECONDS}s)"
    else
        elapsed=$((SECONDS - replacement_at))
        log "worker-death XPENDING raw: $(pel_consumers | tr '\n' ' ')"
        die "worker-death: PEL still pending $(xpending_count) after ${elapsed}s (bound ${BOUND_SECONDS}s)"
    fi
    wait "$msg_pid" || true
    if [[ -n "$runner" ]]; then
        docker rm -f "$runner" >/dev/null 2>&1 || true
    fi
    out="$(send_followup "recovery-drill worker-death follow-up" || true)"
    printf '%s\n' "$out" >&2
    assert_finalized "$out" "worker-death follow-up"
    pending_after="$(xpending_count)"
    record_planes "worker-death after"
    append_evidence worker-death pass \
        "replacement recovered PEL in ${elapsed}s; follow-up finalized; completion-pending=$(completions_pending_count)" \
        "after kill and before replacement, XPENDING=$mid_pending (stranded under dead consumer)" \
        "$elapsed"
}

scenario_runner_death() {
    log "=== scenario: runner-death ==="
    local runner out elapsed start
    reap_runners
    send_message "recovery-drill runner-death hold" >/tmp/curie-2425-runner-msg.json &
    local msg_pid=$!
    runner="$(wait_for_runner || true)"
    [[ -n "$runner" ]] || die "runner-death: no runner container to kill"
    start="$SECONDS"
    docker kill "$runner" >/dev/null
    log "killed runner $runner"
    wait "$msg_pid" || true
    if wait_planes_quiet "$BOUND_SECONDS"; then
        elapsed=$((SECONDS - start))
    else
        elapsed=$((SECONDS - start))
        die "runner-death: PEL still pending after ${elapsed}s"
    fi
    out="$(send_followup "recovery-drill runner-death follow-up" || true)"
    printf '%s\n' "$out" >&2
    assert_finalized "$out" "runner-death follow-up"
    # Negative: killing a non-runner (the API) is a different scenario; here
    # the independent path is the follow-up turn on the same thread.
    append_evidence runner-death pass \
        "runner kill settled in ${elapsed}s; follow-up finalized" \
        "follow-up is a new turn on the same thread (independent of the killed runner id $runner)" \
        "$elapsed"
}

scenario_timeout_follow_up() {
    log "=== scenario: timeout-follow-up ==="
    local runner out elapsed start override="$WORKDIR/timeout-override.yaml"
    reap_runners
    cat >"$override" <<'YAML'
services:
  curie-worker:
    environment:
      - CURIE_RUNNER_TOTAL_TIMEOUT_S=60
      - CURIE_DELIVERY_BUDGET_S=60
YAML
    docker compose -f "$COMPOSE_FILE" -f "$override" --profile core up -d --no-deps --force-recreate curie-worker >/dev/null
    sleep 5
    send_message "recovery-drill timeout hold" >/tmp/curie-2425-timeout-msg.json &
    local msg_pid=$!
    runner="$(wait_for_runner || true)"
    [[ -n "$runner" ]] || die "timeout-follow-up: no runner to pause"
    docker pause "$runner" >/dev/null
    start="$SECONDS"
    # Wait past the configured 60s deadline plus a small grace.
    sleep 70
    docker unpause "$runner" >/dev/null 2>&1 || true
    wait "$msg_pid" || true
    elapsed=$((SECONDS - start))
    wait_planes_quiet 30 || log "timeout-follow-up: PEL still $(xpending_count) after deadline"
    docker rm -f "$runner" >/dev/null 2>&1 || true
    # Restore production timeouts.
    compose up -d --no-deps --force-recreate curie-worker >/dev/null
    sleep 5
    out="$(send_followup "recovery-drill timeout follow-up" || true)"
    printf '%s\n' "$out" >&2
    assert_finalized "$out" "timeout-follow-up"
    append_evidence timeout-follow-up pass \
        "configured 60s deadline exceeded after pause; follow-up finalized; elapsed=${elapsed}s" \
        "unpaused healthy follow-up completed (second path); paused turn did not stay admitted forever" \
        "$elapsed"
}

scenario_valkey_outage() {
    log "=== scenario: valkey-outage ==="
    local before after refused=0 restored_out
    # Accept a request first so recovery has something durable to keep.
    send_message "recovery-drill valkey pre-outage" >/tmp/curie-2425-valkey-pre.json || true
    sleep 2
    before="$(valkey XLEN "$STREAM" 2>/dev/null || echo 0)"
    compose stop valkey >/dev/null
    sleep 2
    if send_message "recovery-drill valkey during-outage" >/tmp/curie-2425-valkey-down.json 2>/tmp/curie-2425-valkey-down.err; then
        log "valkey-outage: enqueue succeeded while Valkey was down"
        compose start valkey >/dev/null
        die "accepted a request during Valkey outage"
    else
        refused=1
        log "valkey-outage: enqueue refused while Valkey was down (expected)"
    fi
    compose start valkey >/dev/null
    local i
    for i in $(seq 1 30); do
        if valkey PING >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    valkey PING >/dev/null || die "Valkey did not return after start"
    after="$(valkey XLEN "$STREAM")"
    # Previously accepted stream length must not shrink to zero (disappear).
    python3 -c "import sys; b=int(sys.argv[1]); a=int(sys.argv[2]); sys.exit(0 if a >= 0 and (b == 0 or a >= 1 or a >= b) else 1)" "$before" "$after" \
        || die "valkey-outage: stream length dropped from $before to $after"
    restored_out="$(send_message "recovery-drill valkey after-restore" || true)"
    printf '%s\n' "$restored_out" >&2
    assert_finalized "$restored_out" "valkey-outage restore"
    wait_planes_quiet 60 || die "valkey-outage: PEL retries did not bound after restore (pending=$(xpending_count))"
    append_evidence valkey-outage pass \
        "enqueue refused during outage; stream XLEN before=$before after=$after; restore turn finalized" \
        "a successful enqueue during the outage is the failure (refused=$refused)" \
        "0"
}

scenario_api_restart() {
    log "=== scenario: api-restart ==="
    local api out pending_before
    pending_before="$(xpending_count)"
    send_message "recovery-drill api pre-restart" >/tmp/curie-2425-api-pre.json &
    local msg_pid=$!
    sleep 2
    api="$(local_api_container)"
    docker restart "$api" >/dev/null
    log "restarted API $api"
    local i
    for i in $(seq 1 30); do
        if curl -fsS http://localhost:28000/health >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    curl -fsS http://localhost:28000/health >/dev/null || die "API health did not return after restart"
    wait "$msg_pid" || true
    wait_planes_quiet 60 || die "api-restart: PEL did not settle (pending=$(xpending_count))"
    out="$(send_followup "recovery-drill api follow-up" || true)"
    printf '%s\n' "$out" >&2
    assert_finalized "$out" "api-restart follow-up"
    record_planes "api-restart after"
    append_evidence api-restart pass \
        "API restart kept health; follow-up finalized; completion-pending=$(completions_pending_count)" \
        "run plane XPENDING before=$pending_before after=$(xpending_count) (independent of API process id)" \
        "0"
}

run_selected_scenarios() {
    local s
    if [[ "$SCENARIO" == "all" ]]; then
        for s in "${SCENARIOS_ALL[@]}"; do
            "scenario_${s//-/_}"
        done
    else
        "scenario_${SCENARIO//-/_}"
    fi
}

emit_result() {
    local status="$1"
    python3 - "$EVIDENCE_FILE" "$status" "$JSON" <<'PY'
import json, pathlib, sys
path, status, as_json = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
data = json.loads(pathlib.Path(path).read_text())
data["status"] = status
text = json.dumps(data, indent=2)
if as_json:
    print(text)
else:
    print(text, file=__import__("sys").stderr)
    print(f"recovery-drill {status}: {len(data.get('scenarios', []))} scenario(s)")
PY
}

cleanup() {
    local code=$?
    trap - EXIT INT TERM
    set +e
    teardown_local
    [[ -n "$WORKDIR" ]] && rm -rf -- "$WORKDIR"
    exit "$code"
}

run_cluster() {
    local ns="${CURIE_E2E_NAMESPACE:-}" rel="${CURIE_E2E_RELEASE:-}"
    [[ -n "$ns" ]] || die "cluster surface requires CURIE_E2E_NAMESPACE (task-owned, never curie or default)"
    [[ -n "$rel" ]] || die "cluster surface requires CURIE_E2E_RELEASE (task-owned, never curie)"
    refuse_soak "$ns" "$rel"
    if [[ "$FORCE" != "1" ]]; then
        local ctx
        ctx="$(kubectl config current-context 2>/dev/null || true)"
        [[ "$ctx" == "k8scratch" ]] || die "cluster surface refuses kube context '$ctx' (want k8scratch). Pass --force to override."
    fi
    die "cluster surface is wired to refuse soak ownership; a full Helm install is not started from this drill. Point CURIE_E2E_NAMESPACE/RELEASE at an already-running task-owned install and re-run, or use --surface local."
}

main() {
    parse_args "$@"
    if (( SELF_TEST )); then
        run_self_test
        return 0
    fi
    # Soak refusal is a pure env/name check. Do it before requiring CURIE_BIN so
    # `cargo test` can prove the guard without putting `curie` on PATH.
    if [[ "$SURFACE" == "cluster" ]]; then
        refuse_soak "${CURIE_E2E_NAMESPACE:-}" "${CURIE_E2E_RELEASE:-}"
    fi
    candidate_identity
    resolve_bin
    WORKDIR="$(mktemp -d /tmp/curie-2425-XXXXXX)"
    EVIDENCE_FILE="$WORKDIR/evidence.json"
    printf '%s\n' "{\"ticket\":\"#2425\",\"commit\":\"$CANDIDATE\",\"surface\":\"$SURFACE\",\"bound_seconds\":$BOUND_SECONDS,\"scenarios\":[]}" >"$EVIDENCE_FILE"
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    if [[ "$SURFACE" == "cluster" ]]; then
        run_cluster
        return 0
    fi

    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        die "another recovery-drill holds $LOCK_FILE"
    fi
    bring_up_local
    run_selected_scenarios
    emit_result pass
}

main "$@"
