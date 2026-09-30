#!/usr/bin/env bash
# Bounded synthetic stable-install restore drill for #2427.
#
# Uses existing export/restore mechanisms (pg_dump, aws s3 sync, SQLite file
# copy, Valkey RDB). Does not invent Valkey replay, RPO/RTO, or a production
# backup product. Tears down only the compose project this script created.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# util-linux flock, with its exit statuses, on hosts that ship none (a stock Mac).
GNU_PROCESS="$REPO_ROOT/cli/scripts/gnu-process.py"
INVENTORY="$REPO_ROOT/tools/restore-drill/restore_inventory.py"
COMPOSE_FILE="$REPO_ROOT/compose.dev.yaml"
AWS_CLI_IMAGE="amazon/aws-cli:2.32.6"
CHECK_BACKUP=""
SUPPLIED_CONFIG=""
NEGATIVE=""
WORKDIR=""
SRC_PROJECT=""
DST_PROJECT=""
OWNED_PROJECTS=()
LOCK_FD=""
RESTORE_STARTED_AT=""
RESTORE_ELAPSED=""

usage() {
    cat >&2 <<'EOF'
usage: restore-drill.sh [--check-backup DIR --supplied-config FILE] [--negative COMPONENT]

  --check-backup DIR       Validate a backup directory and refuse if incomplete.
  --supplied-config FILE   JSON object of separately supplied key names to values.
  --negative COMPONENT     Omit a required component (postgres, bundles,
                           mail-adapter-state, valkey) and expect refusal.

With no arguments, run the disposable compose drill: seed, backup, destroy the
task-owned source, restore onto a distinct target, verify, then a fake-model
round trip. Always runs the negative control against a copy of the backup.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check-backup)
            CHECK_BACKUP="$2"
            shift 2
            ;;
        --supplied-config)
            SUPPLIED_CONFIG="$2"
            shift 2
            ;;
        --negative)
            NEGATIVE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument $1" >&2
            usage
            exit 2
            ;;
    esac
done

inventory() {
    (cd "$REPO_ROOT" && uv run python "$INVENTORY" "$@")
}

emit_json() {
    python3 -c 'import json,sys; json.dump(json.loads(sys.argv[1]), sys.stdout, indent=2, sort_keys=True); print()' "$1"
}

run_check() {
    local backup="$1" supplied="$2"
    inventory check "$backup" --supplied-config "$supplied"
}

if [[ -n "$CHECK_BACKUP" ]]; then
    if [[ -z "$SUPPLIED_CONFIG" ]]; then
        echo "error: --check-backup requires --supplied-config" >&2
        exit 2
    fi
    if [[ -n "$NEGATIVE" ]]; then
        WORKDIR="$(mktemp -d)"
        cp -a "$CHECK_BACKUP" "$WORKDIR/backup"
        inventory omit "$WORKDIR/backup" "$NEGATIVE"
        if run_check "$WORKDIR/backup" "$SUPPLIED_CONFIG"; then
            echo "error: negative control expected restore to refuse after omitting $NEGATIVE" >&2
            rm -rf "$WORKDIR"
            exit 1
        fi
        rm -rf "$WORKDIR"
        echo "negative control refused omitted component $NEGATIVE" >&2
        exit 0
    fi
    run_check "$CHECK_BACKUP" "$SUPPLIED_CONFIG"
    exit $?
fi

port_busy() {
    local port="$1"
    python3 - "$port" <<'PY'
import socket, sys
port = int(sys.argv[1])
sock = socket.socket()
sock.settimeout(0.2)
try:
    sock.connect(("127.0.0.1", port))
except OSError:
    sys.exit(0)
else:
    sock.close()
    sys.exit(1)
PY
}

require_free_ports() {
    local port
    for port in 28000 25432 26379 29000; do
        if ! port_busy "$port"; then
            echo "error: host port $port is occupied; stop the owner of the existing Curie stack before this drill" >&2
            echo "fix: this drill uses the compose.dev.yaml host ports and will not touch another install" >&2
            exit 1
        fi
    done
}

ensure_runner_network() {
    docker network inspect "$RUNNER_NETWORK" >/dev/null 2>&1 || docker network create "$RUNNER_NETWORK" >/dev/null
}

write_compose_override() {
    cat >"$WORKDIR/compose.override.yaml" <<EOF
networks:
  curie_runner:
    name: ${RUNNER_NETWORK}
services:
  curie-worker:
    environment:
      - CURIE_DOCKER_NETWORK=${RUNNER_NETWORK}
      - CURIE_FAKE_MODEL=1
EOF
}

compose() {
    local project="$1"
    shift
    OTEL_EXPORTER_OTLP_ENDPOINT= docker compose -p "$project" \
        -f "$COMPOSE_FILE" -f "$WORKDIR/compose.override.yaml" --profile core "$@"
}

cleanup() {
    local project
    if [[ -n "${BUNDLE:-}" && -f "$BUNDLE/.curie/compose.connectors.yaml" ]]; then
        echo "cleanup: connector compose for the drill bundle" >&2
        docker compose -f "$BUNDLE/.curie/compose.connectors.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    for project in "${OWNED_PROJECTS[@]+"${OWNED_PROJECTS[@]}"}"; do
        echo "cleanup: docker compose -p $project down -v" >&2
        compose "$project" down -v --remove-orphans >/dev/null 2>&1 || true
    done
    if [[ -n "$WORKDIR" && -d "$WORKDIR" ]]; then
        rm -rf "$WORKDIR" 2>/dev/null || docker run --rm -v "$WORKDIR:/work" busybox rm -rf /work
    fi
    if [[ -n "${RUNNER_NETWORK:-}" ]]; then
        docker network rm "$RUNNER_NETWORK" >/dev/null 2>&1 || true
    fi
}

wait_for_api() {
    local i
    for i in $(seq 1 90); do
        if curl -fsS http://localhost:28000/health >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "error: API did not become healthy on localhost:28000" >&2
    return 1
}

s3_sync() {
    local project="$1" direction="$2" stage="$3"
    local network="${project}_default"
    local from to
    mkdir -p "$stage"
    if [[ "$direction" = export ]]; then
        from="s3://curie-bundles"
        to="/stage"
    else
        from="/stage"
        to="s3://curie-bundles"
    fi
    # The aws-cli image's entrypoint is `aws`, so override it. Credentials stay
    # in the container environment; this script never prints them.
    docker run --rm --network "$network" --entrypoint /bin/sh \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp \
        -e AWS_ACCESS_KEY_ID=rustfs \
        -e AWS_SECRET_ACCESS_KEY=rustfssecret \
        -e AWS_DEFAULT_REGION=us-east-1 \
        -e AWS_EC2_METADATA_DISABLED=true \
        -v "$stage:/stage" \
        "$AWS_CLI_IMAGE" \
        -ec "aws configure set default.s3.addressing_style path
aws --endpoint-url http://rustfs:9000 s3 sync '$from' '$to' --only-show-errors"
}

backup_postgres() {
    local project="$1" dest="$2"
    mkdir -p "$(dirname "$dest")"
    compose "$project" exec -T postgres pg_dump -U postgres -Fc postgres >"$dest"
}

restore_postgres() {
    local project="$1" src="$2"
    compose "$project" exec -T postgres psql -U postgres -c \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='postgres' AND pid <> pg_backend_pid();" \
        >/dev/null
    compose "$project" exec -T postgres pg_restore -U postgres -d postgres --clean --if-exists --no-owner <"$src"
}

backup_valkey() {
    local project="$1" dest="$2"
    mkdir -p "$(dirname "$dest")"
    compose "$project" exec -T valkey valkey-cli -a valkeypass --no-auth-warning SAVE >/dev/null
    compose "$project" cp valkey:/data/dump.rdb "$dest"
}

restore_valkey() {
    local project="$1" src="$2"
    compose "$project" stop valkey >/dev/null
    compose "$project" cp "$src" valkey:/data/dump.rdb
    compose "$project" start valkey >/dev/null
}

resolve_bin() {
    if [[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN:-}" ]]; then
        BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"
        return
    fi
    local target
    target="$(
        cd "$REPO_ROOT/cli" && cargo metadata --format-version 1 --no-deps --offline \
            | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])'
    )"
    (cd "$REPO_ROOT/cli" && cargo build --quiet)
    BIN="$target/debug/curie"
    if [[ ! -x "$BIN" ]]; then
        echo "error: failed to build curie at $BIN" >&2
        exit 1
    fi
}

json_field() {
    python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(d[sys.argv[1]])' "$1"
}

nested_field() {
    python3 -c 'import json,sys; d=json.loads(sys.stdin.read())
keys=sys.argv[1].split(".")
for k in keys:
    d=d[k]
print(d)' "$1"
}

exec 9>"/tmp/curie-restore-drill.lock"
if ! "$GNU_PROCESS" flock -n 9; then
    echo "error: another restore drill is already running" >&2
    exit 1
fi
LOCK_FD=9

trap cleanup EXIT

require_free_ports
resolve_bin
WORKDIR="$(mktemp -d /tmp/curie-restore-drill.XXXXXX)"
chmod 700 "$WORKDIR"
RUNNER_NETWORK="curie-r2427-runner-$$"
export CURIE_FAKE_MODEL=1
ensure_runner_network
write_compose_override
BACKUP="$WORKDIR/backup"
SUPPLIED="$WORKDIR/supplied.json"
REPORT="$WORKDIR/report.json"
CANDIDATE="$(git -C "$REPO_ROOT" rev-parse HEAD)"
SRC_PROJECT="curie-r2427-src-$$"
DST_PROJECT="curie-r2427-dst-$$"

cat >"$SUPPLIED" <<'EOF'
{
  "POSTGRES_PASSWORD": "postgres",
  "VALKEY_PASSWORD": "valkeypass",
  "S3_ACCESS_KEY": "rustfs",
  "S3_SECRET_KEY": "rustfssecret",
  "API_KEY": "curie-dev-key"
}
EOF

echo "=== source install $SRC_PROJECT ===" >&2
OWNED_PROJECTS+=("$SRC_PROJECT")
compose "$SRC_PROJECT" up -d --wait --wait-timeout 300 \
    postgres valkey rustfs rustfs-perms
compose "$SRC_PROJECT" run --rm rustfs-init
compose "$SRC_PROJECT" up -d --wait --wait-timeout 300 \
    curie-migrate curie-api curie-worker
wait_for_api

echo "=== deploy synthetic agent ===" >&2
BUNDLE="$WORKDIR/bundle"
cp -a "$REPO_ROOT/examples/weather" "$BUNDLE"
rm -f "$BUNDLE/connectors.yaml"
DEPLOY_JSON="$("$BIN" --json local deploy --plugin-dir "$BUNDLE")"
printf '%s\n' "$DEPLOY_JSON" >&2
AGENT_ID="$(printf '%s' "$DEPLOY_JSON" | nested_field agent.id 2>/dev/null || true)"
if [[ -z "$AGENT_ID" ]]; then
    AGENT_ID="$(printf '%s' "$DEPLOY_JSON" | json_field agent_id)"
fi
AGENT_NAME="$(printf '%s' "$DEPLOY_JSON" | nested_field agent.name 2>/dev/null || true)"
if [[ -z "$AGENT_NAME" ]]; then
    AGENT_NAME="$(printf '%s' "$DEPLOY_JSON" | json_field agent_name)"
fi
BUNDLE_SHA="$(printf '%s' "$DEPLOY_JSON" | nested_field bundle.sha256 2>/dev/null || true)"
if [[ -z "$BUNDLE_SHA" ]]; then
    BUNDLE_SHA="$(printf '%s' "$DEPLOY_JSON" | json_field bundle_sha256)"
fi
VERSION_ID="$(printf '%s' "$DEPLOY_JSON" | nested_field version.id)"

echo "=== seed mail adapter sqlite ===" >&2
inventory seed-mail "$BACKUP/mail-adapter/state.sqlite3" >/dev/null

echo "=== backup supported state ===" >&2
backup_postgres "$SRC_PROJECT" "$BACKUP/postgres/curie.dump"
s3_sync "$SRC_PROJECT" export "$BACKUP/bundles"
backup_valkey "$SRC_PROJECT" "$BACKUP/valkey/dump.rdb"
inventory write-manifest "$BACKUP" --candidate "$CANDIDATE" >/dev/null
run_check "$BACKUP" "$SUPPLIED" >/dev/null
echo "backup accepted" >&2

echo "=== negative control: omit ${NEGATIVE:-bundles} ===" >&2
cp -a "$BACKUP" "$WORKDIR/backup-negative"
inventory omit "$WORKDIR/backup-negative" "${NEGATIVE:-bundles}"
if run_check "$WORKDIR/backup-negative" "$SUPPLIED" >"$WORKDIR/negative.json"; then
    echo "error: incomplete backup was accepted" >&2
    exit 1
fi
python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); assert p.get("error") and p.get("fix")' "$WORKDIR/negative.json"
echo "negative control refused omitted ${NEGATIVE:-bundles}" >&2

echo "=== destroy source install ===" >&2
compose "$SRC_PROJECT" down -v --remove-orphans
OWNED_PROJECTS=()

echo "=== restore onto distinct target $DST_PROJECT ===" >&2
RESTORE_STARTED_AT="$(date -u +%s)"
OWNED_PROJECTS+=("$DST_PROJECT")
compose "$DST_PROJECT" up -d --wait --wait-timeout 300 \
    postgres valkey rustfs rustfs-perms
compose "$DST_PROJECT" run --rm rustfs-init
restore_postgres "$DST_PROJECT" "$BACKUP/postgres/curie.dump"
s3_sync "$DST_PROJECT" import "$BACKUP/bundles"
restore_valkey "$DST_PROJECT" "$BACKUP/valkey/dump.rdb"
mkdir -p "$WORKDIR/restored-mail"
cp -a "$BACKUP/mail-adapter/state.sqlite3" "$WORKDIR/restored-mail/state.sqlite3"
inventory verify-mail "$WORKDIR/restored-mail/state.sqlite3" >/dev/null
compose "$DST_PROJECT" up -d --wait --wait-timeout 300 curie-migrate curie-api curie-worker
wait_for_api
RESTORE_ELAPSED="$(( $(date -u +%s) - RESTORE_STARTED_AT ))"

echo "=== verify records and bundle digest ===" >&2
VERSIONS_JSON="$("$BIN" --json local versions "$AGENT_NAME")"
printf '%s\n' "$VERSIONS_JSON" >&2
python3 - "$VERSIONS_JSON" "$BUNDLE_SHA" "$VERSION_ID" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
want_sha, want_id = sys.argv[2], sys.argv[3]
versions = payload.get("versions") or []
match = next((row for row in versions if row.get("id") == want_id), None)
if match is None:
    raise SystemExit(f"restored versions do not include {want_id}")
got = match.get("bundle_sha256")
if got != want_sha:
    raise SystemExit(f"bundle digest mismatch: restored {got} expected {want_sha}")
print(f"restored version {want_id} digest {got}")
PY

echo "=== post-restore fake-model round trip ===" >&2
# local message's one-shot dispatcher always joins `curie_default`. Attach this
# drill's Valkey there so enqueue reaches the restored stream without renaming
# the task-owned compose project to `curie` (which would `down -v` someone
# else's retained volumes). Refuse if that network already has other containers.
valkey_container="$(compose "$DST_PROJECT" ps -q valkey)"
if docker network inspect curie_default >/dev/null 2>&1; then
    others="$(docker network inspect curie_default -f '{{len .Containers}}')"
    if [[ "$others" != "0" ]]; then
        echo "error: network curie_default already has containers; will not attach this drill's Valkey" >&2
        echo "fix: stop the owner of project curie, or re-run when host ports 28000/25432 are free" >&2
        exit 1
    fi
else
    docker network create curie_default >/dev/null
fi
docker network connect --alias valkey curie_default "$valkey_container"
MESSAGE_JSON="$("$BIN" --json local message --channel C0LOCALDEV --timeout-secs 180 "restore drill ping" || true)"
docker network disconnect curie_default "$valkey_container" >/dev/null 2>&1 || true
printf '%s\n' "$MESSAGE_JSON" >&2
python3 -c '
import json, sys
payload = json.loads(sys.argv[1])
if payload.get("finalized") is True and str(payload.get("reply") or "").strip():
    sys.exit(0)
raise SystemExit("post-restore round trip did not finalize a reply")
' "$MESSAGE_JSON"

python3 - "$REPORT" "$CANDIDATE" "$RESTORE_ELAPSED" "$BUNDLE_SHA" "$WORKDIR/negative.json" <<'PY'
import json, sys
report_path, candidate, elapsed, digest, negative_path = sys.argv[1:6]
negative = json.load(open(negative_path))
json.dump(
    {
        "ok": True,
        "issue": "2427",
        "candidate": candidate,
        "recovered_point": {"bundle_sha256": digest, "manifest": "backup/MANIFEST.json"},
        "elapsed_restore_seconds": int(elapsed),
        "data_loss": "none within the declared backup set; Valkey stream/PEL replay is not a Curie contract",
        "valkey_replay_contract": "missing",
        "rpo_rto_claimed": False,
        "recurring_production_backup_established": False,
        "negative_control": negative,
        "round_trip": {"mode": "fake", "finalized": True},
    },
    open(report_path, "w"),
    indent=2,
    sort_keys=True,
)
PY
cat "$REPORT"
echo "restore drill passed in ${RESTORE_ELAPSED}s" >&2
