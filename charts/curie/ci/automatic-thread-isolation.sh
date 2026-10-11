#!/usr/bin/env bash
#
# Kind regression for automatic thread allocation (#4311).
#
# Three overlapping `curie cluster message` processes that omit `--thread` must
# finalize into three distinct conversations, each with exactly one execution
# and one completion. An explicit `--thread` must continue only the
# conversation it names. Everything runs on a kind cluster this script creates
# and deletes; nothing outside it is read or touched.
#
# Why the wall clock is pinned. The defect is two processes reading the same
# wall-clock value and deriving the same thread ts from it. Left to chance, the
# overlap happens rarely, so a red-on-revert run would only fail sometimes. The
# three message processes therefore run with a tiny LD_PRELOAD shim that
# answers CLOCK_REALTIME with one fixed instant for any process named `curie`
# (kubectl and every other child keep the real clock; monotonic time is never
# touched, so every timeout still runs on real time). That reproduces the
# observed condition exactly: identical clock reads in independent processes.
# Every other part of the path is real: the CLI built from this checkout, its
# own port-forwards, Valkey, the worker, the runner sandbox and the scripted
# fake model, with no Slack and no paid provider.
#
# Receipts asserted per conversation:
#   execution  one SandboxClaim per conversation. The worker names each claim's
#              `curietech.ai/thread-hash` label after sha256(thread key), so
#              three conversations are three distinct hashes with one claim
#              each, and a shared conversation shows up as fewer hashes.
#   completion each process's own `--json` result is finalized with the
#              scripted reply, and the API holds one terminal relay record per
#              message.
#
# Cleanup is registered before anything is created and runs on success and on
# failure. It uninstalls the release, deletes the namespace, deletes the kind
# cluster, kills every process the script started, and then ASSERTS that the
# release's cluster-scoped objects, the namespace, its volumes, the kind node
# container and those processes are all gone, and that every kind cluster that
# existed before the run is still there. A cleanup assertion failure fails the
# run even when the regression itself passed.
#
# Usage (no arguments; run from anywhere):
#   bash charts/curie/ci/automatic-thread-isolation.sh
#
# Needs docker, kind, kubectl, helm, cargo, a C compiler and python3. The
# product images are the published ones for the chart's appVersion, pulled
# anonymously from GHCR; the CLI is built from the checkout the script lives in.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
RUN_ID="$(printf '%s%04x%04x' "$(date +%s | tail -c 6)" "$((RANDOM))" "$((RANDOM))")"
KIND_CLUSTER="ati-${RUN_ID}"
CONTEXT="kind-${KIND_CLUSTER}"
NAMESPACE="ati-${RUN_ID}"
RELEASE="ati${RUN_ID}"
FULLNAME="${RELEASE}-curie"
AGENT="ati-bot"
CONCURRENT=3
MESSAGE_TIMEOUT_SECS=420
SCRIPTED_REPLY="all done"
WORKDIR=""
KUBECONFIG_FILE=""
OWNED_KIND=0
OWNED_RELEASE=0
SPAWNED_PIDS=()
PREEXISTING_KIND=""

log() { printf '[automatic-thread-isolation] %s\n' "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

for tool in docker kind kubectl helm cargo cc curl python3; do
    command -v "$tool" >/dev/null 2>&1 || fail "missing required tool: $tool"
done

kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" "$@"; }
kcn() { kc -n "$NAMESPACE" "$@"; }

# Every absence query below fails closed: a query that errors returns
# nonzero, and the caller counts that as "not proven gone". An empty answer
# from a failed command must never read as an empty cluster.

# Cluster-scoped objects this release created, by the chart's instance label.
release_cluster_scoped() {
    local kinds="clusterroles,clusterrolebindings,priorityclasses,validatingadmissionpolicies,validatingadmissionpolicybindings,mutatingwebhookconfigurations,validatingwebhookconfigurations"
    kc get "$kinds" -l "app.kubernetes.io/instance=${RELEASE}" -o name
}

# Volumes still bound to the owned namespace.
namespace_volumes() {
    local rows
    rows="$(kc get pv -o jsonpath='{range .items[*]}{.metadata.name} {.spec.claimRef.namespace}{"\n"}{end}')" \
        || return 1
    awk -v ns="$NAMESPACE" '$2 == ns {print $1}' <<<"$rows"
}

# Kind clusters on this Docker daemon, one per line.
kind_clusters() {
    local out
    out="$(kind get clusters 2>&1)" || { printf '%s\n' "$out" >&2; return 1; }
    grep -v '^No kind clusters found' <<<"$out" || true
}

# Node containers (running or not) carrying a kind cluster's label.
kind_node_containers() {
    docker ps -aq --filter "label=io.x-k8s.kind.cluster=$1"
}

dump_diagnostics() {
    [[ -n "$KUBECONFIG_FILE" && -f "$KUBECONFIG_FILE" ]] || return 0
    log "diagnostics: pods"
    kcn get pods -o wide >&2 2>/dev/null || true
    log "diagnostics: sandbox claims"
    kcn get sandboxclaims.extensions.agents.x-k8s.io -L curietech.ai/thread-hash >&2 2>/dev/null || true
    log "diagnostics: recent events"
    kcn get events --sort-by=.lastTimestamp 2>/dev/null | tail -30 >&2 || true
    log "diagnostics: worker log tail"
    kcn logs "deploy/${FULLNAME}-worker" --tail=80 >&2 2>/dev/null || true
    local f
    for f in "$WORKDIR"/message-*.err; do
        [[ -f "$f" ]] || continue
        log "diagnostics: $(basename "$f") tail"
        tail -20 "$f" >&2 || true
    done
}

cleanup() {
    local status=$?
    trap - EXIT
    set +e
    if (( status != 0 )); then
        dump_diagnostics
    fi
    local cleanup_failed=0 pid

    # Spawned processes first, so no port-forward outlives the cluster.
    for pid in "${SPAWNED_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            pkill -TERM -P "$pid" 2>/dev/null
            kill -TERM "$pid" 2>/dev/null
        fi
    done
    for pid in "${SPAWNED_PIDS[@]}"; do
        for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        if kill -0 "$pid" 2>/dev/null; then
            pkill -KILL -P "$pid" 2>/dev/null
            kill -KILL "$pid" 2>/dev/null
        fi
    done

    if (( OWNED_RELEASE )); then
        log "cleanup: uninstalling release $RELEASE"
        helm --kubeconfig "$KUBECONFIG_FILE" --kube-context "$CONTEXT" -n "$NAMESPACE" \
            uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1
        log "cleanup: deleting namespace $NAMESPACE"
        kc delete namespace "$NAMESPACE" --wait=true --timeout=240s >/dev/null 2>&1
        local leftover
        if ! leftover="$(release_cluster_scoped)"; then
            log "cleanup assertion: could not list release cluster-scoped objects"
            cleanup_failed=1
        elif [[ -n "$leftover" ]]; then
            log "cleanup assertion: release cluster-scoped objects remain: $leftover"
            cleanup_failed=1
        fi
        if ! leftover="$(kc get namespace "$NAMESPACE" --ignore-not-found -o name)"; then
            log "cleanup assertion: could not query namespace $NAMESPACE"
            cleanup_failed=1
        elif [[ -n "$leftover" ]]; then
            log "cleanup assertion: namespace $NAMESPACE still exists"
            cleanup_failed=1
        fi
        if ! leftover="$(namespace_volumes)"; then
            log "cleanup assertion: could not list persistent volumes"
            cleanup_failed=1
        elif [[ -n "$leftover" ]]; then
            log "cleanup assertion: volumes still bound to $NAMESPACE: $leftover"
            cleanup_failed=1
        fi
    fi

    # Other owners' clusters still present right before our delete; only
    # these can be attributed to this run if they then disappear.
    local survivors="" name clusters nodes
    if ! clusters="$(kind_clusters)"; then
        log "cleanup assertion: could not list kind clusters before deleting ours"
        cleanup_failed=1
    elif [[ -n "$PREEXISTING_KIND" ]]; then
        # An empty snapshot must stay empty: `grep -f` on an empty pattern
        # list would select every line, including this run's own cluster.
        survivors="$(grep -Fxf <(printf '%s\n' "$PREEXISTING_KIND") <<<"$clusters" || true)"
    fi

    if (( OWNED_KIND )); then
        log "cleanup: deleting kind cluster $KIND_CLUSTER"
        for _ in 1 2 3; do
            kind delete cluster --name "$KIND_CLUSTER" >&2 && break
            sleep 5
        done
        # A node container kind no longer tracks (a failed create) is still ours.
        nodes="$(kind_node_containers "$KIND_CLUSTER")" && [[ -n "$nodes" ]] && \
            docker rm -f $nodes >/dev/null
        if ! clusters="$(kind_clusters)"; then
            log "cleanup assertion: could not list kind clusters"
            cleanup_failed=1
        elif grep -Fxq "$KIND_CLUSTER" <<<"$clusters"; then
            log "cleanup assertion: kind cluster $KIND_CLUSTER still exists"
            cleanup_failed=1
        fi
        if ! nodes="$(kind_node_containers "$KIND_CLUSTER")"; then
            log "cleanup assertion: could not list kind node containers"
            cleanup_failed=1
        elif [[ -n "$nodes" ]]; then
            log "cleanup assertion: kind node containers for $KIND_CLUSTER remain"
            cleanup_failed=1
        fi
    fi

    for pid in "${SPAWNED_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log "cleanup assertion: spawned process $pid is still running"
            cleanup_failed=1
        fi
    done
    if pgrep -af "port-forward" 2>/dev/null | grep -Fq -- "$NAMESPACE"; then
        log "cleanup assertion: a port-forward into $NAMESPACE is still running"
        cleanup_failed=1
    fi

    # Unrelated kind clusters (another run's intake identity) are preserved.
    if [[ -n "$survivors" ]]; then
        if ! clusters="$(kind_clusters)"; then
            log "cleanup assertion: could not list kind clusters after cleanup"
            cleanup_failed=1
        else
            while IFS= read -r name; do
                [[ -n "$name" ]] || continue
                if ! grep -Fxq "$name" <<<"$clusters"; then
                    log "cleanup assertion: unrelated kind cluster $name disappeared during cleanup"
                    cleanup_failed=1
                fi
            done <<<"$survivors"
        fi
    fi

    [[ -n "$WORKDIR" ]] && rm -rf "$WORKDIR"

    if (( cleanup_failed )); then
        log "FAIL: cleanup left owned resources behind"
        exit 1
    fi
    log "cleanup verified: release, namespace, volumes, cluster and processes are gone"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

PREEXISTING_KIND="$(kind_clusters)" || fail "cannot list kind clusters on this Docker daemon"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/automatic-thread-isolation.XXXXXX")"
KUBECONFIG_FILE="$WORKDIR/kubeconfig"
export KUBECONFIG="$KUBECONFIG_FILE"
cd "$REPO_ROOT"

# --- the CLI under test, built from this checkout -------------------------
log "building the curie CLI from $REPO_ROOT"
cargo build --release --locked --quiet --manifest-path "$REPO_ROOT/cli/Cargo.toml"
TARGET_DIR="$(cargo metadata --format-version 1 --no-deps --manifest-path "$REPO_ROOT/cli/Cargo.toml" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
[[ -x "$TARGET_DIR/release/curie" ]] || fail "cargo build did not produce $TARGET_DIR/release/curie"
# A private copy, so a concurrent build into a shared target cannot swap the
# binary under test mid-run.
BIN="$WORKDIR/curie"
cp "$TARGET_DIR/release/curie" "$BIN"

# --- the clock pin --------------------------------------------------------
cat >"$WORKDIR/pin-realtime.c" <<'C'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Answer CLOCK_REALTIME with one fixed instant, for processes named curie only. */
int clock_gettime(clockid_t id, struct timespec *ts) {
    static int (*real)(clockid_t, struct timespec *);
    if (!real) real = (int (*)(clockid_t, struct timespec *))dlsym(RTLD_NEXT, "clock_gettime");
    int rc = real(id, ts);
    const char *pin = getenv("ATI_PINNED_REALTIME");
    if (rc == 0 && pin && (id == CLOCK_REALTIME || id == CLOCK_REALTIME_COARSE)
        && strcmp(program_invocation_short_name, "curie") == 0) {
        char *dot = NULL;
        ts->tv_sec = (time_t)strtoll(pin, &dot, 10);
        ts->tv_nsec = (dot && *dot == '.') ? strtol(dot + 1, NULL, 10) : 0;
    }
    return rc;
}
C
cc -shared -fPIC -O2 -o "$WORKDIR/pin-realtime.so" "$WORKDIR/pin-realtime.c" -ldl
PINNED_REALTIME="$(date +%s).123456000"

# --- an owned kind cluster ------------------------------------------------
# Refuse a name that already exists before claiming ownership: a failed create
# of someone else's cluster must never lead cleanup to delete it.
if grep -Fxq "$KIND_CLUSTER" <<<"$PREEXISTING_KIND"; then
    fail "kind cluster $KIND_CLUSTER already exists and belongs to someone else"
fi
log "creating kind cluster $KIND_CLUSTER"
OWNED_KIND=1
# One retry: a cold node occasionally misses kubeadm's kubelet health window.
if ! kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 180s >/dev/null; then
    log "kind create failed once; deleting the partial cluster and retrying"
    kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
    kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 180s >/dev/null
fi

APP_VERSION="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}[[:space:]]*$/\1/p' "$CHART/Chart.yaml")"
[[ -n "$APP_VERSION" ]] || fail "could not read appVersion from $CHART/Chart.yaml"
for component in api worker runner; do
    image="ghcr.io/curie-eng/curie-${component}:${APP_VERSION}"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        log "pulling $image"
        docker pull --quiet "$image" >/dev/null
    fi
    log "loading $image into $KIND_CLUSTER"
    kind load docker-image "$image" --name "$KIND_CLUSTER" >/dev/null
done

# --- the release, scripted model, no Slack --------------------------------
log "installing release $RELEASE into namespace $NAMESPACE"
OWNED_RELEASE=1
"$BIN" cluster --context "$CONTEXT" up \
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
    --set "api.image.repository=ghcr.io/curie-eng/curie-api" \
    --set "api.image.tag=${APP_VERSION}" \
    --set api.image.pullPolicy=IfNotPresent \
    --set "worker.image.repository=ghcr.io/curie-eng/curie-worker" \
    --set "worker.image.tag=${APP_VERSION}" \
    --set worker.image.pullPolicy=IfNotPresent \
    --set "agentSandbox.runner.image=ghcr.io/curie-eng/curie-runner" \
    --set "agentSandbox.runner.tag=${APP_VERSION}" \
    --set agentSandbox.runner.imagePullPolicy=IfNotPresent \
    --set agentSandbox.runner.prewarm.imagePullPolicy=IfNotPresent \
    >"$WORKDIR/up.out" 2>"$WORKDIR/up.err" || {
        tail -40 "$WORKDIR/up.err" >&2
        fail "curie cluster up failed"
    }
kcn rollout status "deploy/${FULLNAME}-api" --timeout=300s >/dev/null
kcn rollout status "deploy/${FULLNAME}-worker" --timeout=300s >/dev/null

# The release is installed with --no-expose and no UI, so the deploy reaches
# the API through this script's own port-forward, which cleanup kills.
log "deploying agent $AGENT"
# kubectl picks the local port itself, so no probed port can be taken first.
kcn port-forward "svc/${FULLNAME}-api" ":8000" >"$WORKDIR/api-forward.log" 2>&1 &
SPAWNED_PIDS+=("$!")
API_LOCAL_PORT=""
for _ in $(seq 1 60); do
    API_LOCAL_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) .*/\1/p' "$WORKDIR/api-forward.log" | head -1)"
    [[ -n "$API_LOCAL_PORT" ]] && curl -fsS -o /dev/null "http://127.0.0.1:${API_LOCAL_PORT}/health" 2>/dev/null && break
    sleep 1
done
[[ -n "$API_LOCAL_PORT" ]] || { cat "$WORKDIR/api-forward.log" >&2; fail "the API port-forward never reported its port"; }
curl -fsS -o /dev/null "http://127.0.0.1:${API_LOCAL_PORT}/health" || fail "the API port-forward never answered"
# The release's own API key, read the way `cluster message` reads it: from the
# release-labelled Secret that carries `apiKey`. It goes into the deploy's
# environment only and is never printed or put on a command line.
release_api_key() {
    kcn get secret -l "app.kubernetes.io/instance=${RELEASE}" -o json | python3 -c '
import base64, json, sys
for item in json.load(sys.stdin)["items"]:
    value = (item.get("data") or {}).get("apiKey")
    if value:
        sys.stdout.write(base64.b64decode(value).decode())
        break
else:
    raise SystemExit("no release Secret carries apiKey")
'
}
cp -a "$REPO_ROOT/examples/coder" "$WORKDIR/coder"
CURIE_API_KEY="$(release_api_key)" "$BIN" --json cluster --context "$CONTEXT" deploy \
    --plugin-dir "$WORKDIR/coder" \
    --agent "$AGENT" \
    --namespace "$NAMESPACE" \
    --release "$RELEASE" \
    --api-url "http://127.0.0.1:${API_LOCAL_PORT}" \
    >"$WORKDIR/deploy.json" 2>"$WORKDIR/deploy.err" || {
        tail -40 "$WORKDIR/deploy.err" >&2
        head -c 4000 "$WORKDIR/deploy.json" >&2
        fail "curie cluster deploy failed"
    }


claims_by_hash() {
    kcn get sandboxclaims.extensions.agents.x-k8s.io \
        -l curietech.ai/managed-by=curie-sandbox-substrate \
        -o jsonpath='{range .items[*]}{.metadata.labels.curietech\.ai/thread-hash}{"\n"}{end}' \
        | sed '/^$/d' | sort | uniq -c | awk '{print $2, $1}'
}

terminal_relay_records() {
    local pod
    pod="$(kcn get pod -l app.kubernetes.io/component=valkey -o jsonpath='{.items[0].metadata.name}')"
    [[ -n "$pod" ]] || fail "no valkey pod"
    # The password is already in the container env; the Secret is never read.
    kcn exec "$pod" -- sh -c \
        'valkey-cli -a "$VALKEY_PASSWORD" --no-auth-warning --scan --pattern "curie:cluster-message-replies:*:terminal"' \
        | sed '/^$/d' | sort -u | wc -l | tr -d ' '
}

# The worker's thread-to-sandbox routes, one per conversation it executed:
# `curie:sandbox:route:<thread key>` holds JSON naming the claim, with a TTL
# the worker refreshes whenever it runs a turn on that thread. Printed as
# "<thread ts> <absolute expiry ms> <route json>", one line per route. The
# absolute expiry (PEXPIRETIME) only moves when the route is refreshed, unlike
# the remaining TTL, which falls while the next turn runs.
route_snapshot() {
    local pod
    pod="$(kcn get pod -l app.kubernetes.io/component=valkey -o jsonpath='{.items[0].metadata.name}')"
    [[ -n "$pod" ]] || fail "no valkey pod"
    kcn exec "$pod" -- sh -c '
        cli() { valkey-cli -a "$VALKEY_PASSWORD" --no-auth-warning "$@"; }
        for key in $(cli --scan --pattern "curie:sandbox:route:*"); do
            printf "%s %s %s\n" "${key##*:}" "$(cli PEXPIRETIME "$key")" "$(cli GET "$key")"
        done' | sed '/^$/d' | sort
}

# --- three overlapping processes, no --thread, one pinned clock ----------
[[ -z "$(claims_by_hash)" ]] || fail "sandbox claims exist before the first message"
log "starting $CONCURRENT overlapping cluster message processes without --thread"
MESSAGE_PIDS=()
for i in $(seq 1 "$CONCURRENT"); do
    ATI_PINNED_REALTIME="$PINNED_REALTIME" LD_PRELOAD="$WORKDIR/pin-realtime.so" \
        "$BIN" --json cluster --context "$CONTEXT" message "automatic thread isolation $i" \
        --agent "$AGENT" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --timeout-secs "$MESSAGE_TIMEOUT_SECS" \
        >"$WORKDIR/message-$i.json" 2>"$WORKDIR/message-$i.err" &
    MESSAGE_PIDS+=("$!")
    SPAWNED_PIDS+=("$!")
done
failed=0
for i in $(seq 1 "$CONCURRENT"); do
    wait "${MESSAGE_PIDS[$((i - 1))]}" || { log "message process $i exited nonzero"; failed=1; }
done
(( failed == 0 )) || fail "a concurrent cluster message process failed"

python3 - "$SCRIPTED_REPLY" "$WORKDIR"/message-*.json <<'PY' >"$WORKDIR/threads"
import json
import sys

expected = sys.argv[1]
threads = []
for path in sys.argv[2:]:
    with open(path) as handle:
        result = json.load(handle)
    if result.get("finalized") is not True:
        raise SystemExit(f"{path}: turn did not finalize: {result}")
    if expected not in (result.get("reply") or ""):
        raise SystemExit(f"{path}: reply lacks the scripted model's {expected!r}: {result.get('reply')!r}")
    threads.append(result["thread"])
for thread in threads:
    print(thread)
PY
mapfile -t THREADS <"$WORKDIR/threads"
distinct="$(printf '%s\n' "${THREADS[@]}" | sort -u | wc -l | tr -d ' ')"
log "conversations returned: ${THREADS[*]}"
(( distinct == CONCURRENT )) || \
    fail "$CONCURRENT processes without --thread shared conversations: only $distinct distinct"

# Each conversation executed exactly once: one claim per thread hash, and as
# many hashes as conversations.
mapfile -t CLAIMS < <(claims_by_hash)
log "sandbox claims by thread hash: ${CLAIMS[*]:-none}"
(( ${#CLAIMS[@]} == CONCURRENT )) || \
    fail "expected $CONCURRENT executed conversations, observed ${#CLAIMS[@]} thread hashes"
for row in "${CLAIMS[@]}"; do
    [[ "${row##* }" == 1 ]] || fail "a conversation executed more than once: $row"
done

# Each returned conversation is the one the worker routed and executed, each
# on its own claim, and the worker routed no other conversation.
# A turn's last refresh can trail its delivered reply, so the baseline is the
# first snapshot that stays identical across one more read.
route_snapshot >"$WORKDIR/routes.before"
for _ in $(seq 1 20); do
    sleep 3
    route_snapshot >"$WORKDIR/routes.settle"
    cmp -s "$WORKDIR/routes.before" "$WORKDIR/routes.settle" && break
    mv "$WORKDIR/routes.settle" "$WORKDIR/routes.before"
done
cmp -s "$WORKDIR/routes.before" "$WORKDIR/routes.settle" || fail "the worker's routes never settled after the three turns"
python3 - "$WORKDIR/routes.before" "${THREADS[@]}" <<'PY'
import json
import sys

rows = [line.split(" ", 2) for line in open(sys.argv[1]).read().splitlines()]
routed = {ts: json.loads(value).get("claim_name") for ts, _, value in rows}
expected = set(sys.argv[2:])
if set(routed) != expected or len(rows) != len(expected):
    raise SystemExit(f"routes {sorted(routed)} do not match the returned conversations {sorted(expected)}")
if not all(routed.values()) or len(set(routed.values())) != len(expected):
    raise SystemExit(f"each conversation needs its own claim: {routed}")
PY

# Each message completed exactly once.
records="$(terminal_relay_records)"
(( records == CONCURRENT )) || fail "expected $CONCURRENT terminal relay records, observed $records"

# --- explicit --thread continues only the supplied conversation ----------
CONTINUED="${THREADS[0]}"
log "continuing conversation $CONTINUED with an explicit --thread"
"$BIN" --json cluster --context "$CONTEXT" message "automatic thread isolation continued" \
    --agent "$AGENT" \
    --namespace "$NAMESPACE" \
    --release "$RELEASE" \
    --thread "$CONTINUED" \
    --timeout-secs "$MESSAGE_TIMEOUT_SECS" \
    >"$WORKDIR/continue.json" 2>"$WORKDIR/message-continue.err" &
SPAWNED_PIDS+=("$!")
wait "$!" || fail "the explicit --thread continuation failed"
python3 - "$CONTINUED" "$SCRIPTED_REPLY" "$WORKDIR/continue.json" <<'PY'
import json
import sys

continued, expected, path = sys.argv[1:]
result = json.load(open(path))
if result.get("finalized") is not True or expected not in (result.get("reply") or ""):
    raise SystemExit(f"continuation did not finalize with the scripted reply: {result}")
if result.get("thread") != continued:
    raise SystemExit(f"continuation answered on {result.get('thread')!r}, not {continued!r}")
PY
mapfile -t AFTER < <(claims_by_hash)
log "sandbox claims after continuation: ${AFTER[*]:-none}"
# A conversation may be re-claimed under its own hash (a new generation) or
# released, but the continuation must not add a hash or a second claim.
if ! python3 - "${CLAIMS[@]}" -- "${AFTER[@]}" <<'PY'; then
import sys

args = sys.argv[1:]
split = args.index("--")
before = dict(row.split() for row in args[:split])
after = dict(row.split() for row in args[split + 1:])
if not set(after) <= set(before) or any(count != "1" for count in after.values()):
    raise SystemExit(1)
PY
    fail "the continuation opened or re-executed another conversation: before=${CLAIMS[*]} after=${AFTER[*]}"
fi
# The continuation ran on the conversation it named: that route still names a
# claim for the same thread hash (the worker may re-claim a released sandbox
# as a new generation) and the new turn refreshed it. Every other route is
# either untouched or released, and no new conversation was routed.
route_snapshot >"$WORKDIR/routes.after"
python3 - "$WORKDIR/routes.before" "$WORKDIR/routes.after" "$CONTINUED" <<'PY'
import json
import sys


def load(path):
    rows = [line.split(" ", 2) for line in open(path).read().splitlines()]
    return {ts: (int(expiry), value) for ts, expiry, value in rows}


before, after, continued = load(sys.argv[1]), load(sys.argv[2]), sys.argv[3]
if not set(after) <= set(before):
    raise SystemExit(f"the continuation routed a new conversation: {sorted(set(after) - set(before))}")
if continued not in after:
    raise SystemExit(f"the continued conversation {continued} has no route after its turn")


def thread_hash(value):
    # Claim names are <prefix>-<sha256(thread key)[:10]>-<generation nonce>.
    return json.loads(value)["claim_name"].rsplit("-", 2)[1]


if thread_hash(after[continued][1]) != thread_hash(before[continued][1]):
    raise SystemExit("the continued conversation ran on another conversation's claim")
if after[continued][0] <= before[continued][0]:
    raise SystemExit(
        f"the continued conversation's route was not refreshed: expiry {before[continued][0]} -> {after[continued][0]} ms"
    )
for ts in before:
    if ts != continued and ts in after and after[ts] != before[ts]:
        raise SystemExit(f"conversation {ts} changed although the continuation named {continued}")
PY
records="$(terminal_relay_records)"
(( records == CONCURRENT + 1 )) || \
    fail "expected $((CONCURRENT + 1)) terminal relay records after continuation, observed $records"

log "PASS: $CONCURRENT overlapping processes got $CONCURRENT conversations; --thread continued only its own"
