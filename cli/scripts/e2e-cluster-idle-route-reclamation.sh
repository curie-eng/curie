#!/usr/bin/env bash
# Issue 2714: real cluster quota recovery, restart ordering, durable history,
# and fail closed runner reachability.
#
# The release is installed by the caller in the exact throwaway namespace. The
# cluster ladder runs first. This script then clears ladder routes through the
# public reset surface and drives only public cluster deploy, message, and reset
# commands for product behavior. kubectl is limited to observation, the required
# worker restart, and the bounded NetworkPolicy negative.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# GNU timeout and util-linux setsid, with their exit statuses, on hosts that
# ship neither (a stock Mac).
GNU_PROCESS="$REPO_ROOT/cli/scripts/gnu-process.py"
KUBE_CONTEXT="${CURIE_E2E_KUBE_CONTEXT:-k8}"
NAMESPACE="${CURIE_E2E_NAMESPACE:-test-2714-idle-route-reclamation}"
RELEASE="${CURIE_E2E_RELEASE:-curie}"
AGENT="${CURIE_E2E_AGENT:-weather}"
MESSAGE_TIMEOUT_SECONDS="${CURIE_E2E_MESSAGE_TIMEOUT_SECONDS:-240}"
WAIT_SECONDS="${CURIE_E2E_WAIT_SECONDS:-180}"
ROUTE_PREFIX="curie:sandbox:route:"
FILLER_LABEL_NAME="curie-e2e-idle-route-reclamation"
FILLER_LABEL_VALUE="filler"
DENY_POLICY="${RELEASE}-idle-route-reclamation-deny-fillers"
CAPACITY_LOG_PATTERN="sandbox capacity exhausted for event"
RECLAIM_LOG_PATTERN="idle route reclamation freed sandbox capacity; retrying event"
METRIC_NAME="curie.sandbox.lifecycle"
METRIC_DEBUG_EXPORTER="${CURIE_E2E_METRIC_DEBUG_EXPORTER:-debug/pressure}"
QUOTA_RESOURCE="${CURIE_E2E_QUOTA_RESOURCE:-pods}"
WORKDIR="$(mktemp -d)"
REAL_KUBECTL=""
BIN=""
WORKER_DEPLOYMENT=""
VALKEY_STATEFULSET=""
OTEL_DEPLOYMENT=""
OTEL_CONFIGMAP=""
RESOURCE_QUOTA=""
RUNNER_TEMPLATE=""
RUNNER_INGRESS_POLICY=""
ALLOW_POLICY_SAVED=0
ALLOW_POLICY_REMOVED=0
DENY_POLICY_CREATED=0
QUOTA_WATCH_PID=""
VICTIM_CLAIM_WATCH_PID=""
VICTIM_SANDBOX_WATCH_PID=""
QUOTA_WATCH_RAW=""
VICTIM_CLAIM_WATCH_RAW=""
VICTIM_SANDBOX_WATCH_RAW=""
QUOTA_WATCH_ERROR=""
VICTIM_CLAIM_WATCH_ERROR=""
VICTIM_SANDBOX_WATCH_ERROR=""
ORIGINAL_ALLOW_FILE="$WORKDIR/runner-ingress.original.json"
RESTORE_ALLOW_FILE="$WORKDIR/runner-ingress.restore.json"
FILLER_PODS=()
PADDING_KEY_PREFIX=""

die() {
    echo "error: $*" >&2
    exit 1
}

case "$QUOTA_RESOURCE" in
    pods)
        QUOTA_HARD="2"
        QUOTA_EMPTY="0"
        QUOTA_ONE="1"
        QUOTA_FULL="2"
        QUOTA_OTHER_RESOURCE=""
        QUOTA_OTHER_HARD=""
        RUNNER_CPU_LIMIT=""
        ;;
    limits.cpu)
        QUOTA_HARD="2"
        QUOTA_EMPTY="0"
        QUOTA_ONE="1"
        QUOTA_FULL="2"
        QUOTA_OTHER_RESOURCE="pods"
        QUOTA_OTHER_HARD="50"
        RUNNER_CPU_LIMIT="1"
        ;;
    *) die "CURIE_E2E_QUOTA_RESOURCE must be pods or limits.cpu, found $QUOTA_RESOURCE" ;;
esac

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

stop_pid() {
    local pid="$1" attempt
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM -- "-$pid" 2>/dev/null || true
        for ((attempt=0; attempt<20; attempt++)); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL -- "-$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
}

print_watch_diagnostic() {
    local description="$1" raw_file="$2" error_file="$3"
    [[ -s "$raw_file" || -s "$error_file" ]] || return 0
    echo "watch diagnostic $description raw=$(tail -c 500 "$raw_file" 2>/dev/null || true) stderr=$(tail -c 500 "$error_file" 2>/dev/null || true)" >&2
}

kube() {
    "$REAL_KUBECTL" --context "$KUBE_CONTEXT" "$@"
}

timestamp_utc() {
    date -u +%Y-%m-%dT%H:%M:%S.%NZ
}

one_resource() {
    local kind="$1" selector="$2" description="$3"
    kube -n "$NAMESPACE" get "$kind" -l "$selector" -o json | python3 -c '
import json,sys
description=sys.argv[1]
names=[item["metadata"]["name"] for item in json.load(sys.stdin).get("items",[])]
if len(names) != 1:
    raise SystemExit(f"expected one {description}, found {names}")
print(names[0])
' "$description"
}

statefulset_owner_for_component() {
    local component="$1" description="$2" statefulset
    statefulset="$(kube -n "$NAMESPACE" get pods \
        -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=$component" \
        -o json | python3 -c '
import json,sys
component,description=sys.argv[1:]
pods=[]
for pod in json.load(sys.stdin).get("items",[]):
    meta=pod.get("metadata",{})
    if meta.get("deletionTimestamp"):
        continue
    owners=[owner for owner in meta.get("ownerReferences",[]) if owner.get("controller") is True]
    if len(owners)==1 and owners[0].get("kind")=="StatefulSet":
        pods.append((meta.get("name"),owners[0].get("name")))
if len(pods)!=1 or not pods[0][1]:
    raise SystemExit(f"expected one nonterminating {description} pod with a StatefulSet owner, found {pods}")
print(pods[0][1])
' "$component" "$description")" || return 1
    kube -n "$NAMESPACE" get statefulset "$statefulset" -o json | python3 -c '
import json,sys
component,release,description=sys.argv[1:]
selector=json.load(sys.stdin).get("spec",{}).get("selector",{}).get("matchLabels",{})
if selector.get("app.kubernetes.io/component")!=component or selector.get("app.kubernetes.io/instance")!=release:
    raise SystemExit(f"{description} StatefulSet selector does not identify release component {release}/{component}: {selector}")
' "$component" "$RELEASE" "$description" || return 1
    printf '%s\n' "$statefulset"
}

deployment_for_component_selector() {
    local component="$1" description="$2"
    kube -n "$NAMESPACE" get deployments \
        -l "app.kubernetes.io/instance=$RELEASE" -o json | python3 -c '
import json,sys
component,release,description=sys.argv[1:]
names=[]
for deployment in json.load(sys.stdin).get("items",[]):
    selector=deployment.get("spec",{}).get("selector",{}).get("matchLabels",{})
    if selector.get("app.kubernetes.io/component")==component and selector.get("app.kubernetes.io/instance")==release:
        names.append(deployment.get("metadata",{}).get("name"))
if len(names)!=1 or not names[0]:
    raise SystemExit(f"expected one {description} Deployment selected by release component labels, found {names}")
print(names[0])
' "$component" "$RELEASE" "$description"
}

worker_pod() {
    kube -n "$NAMESPACE" get pods \
        -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker" \
        -o json | python3 -c '
import json,sys
ready=[]
for pod in json.load(sys.stdin).get("items",[]):
    meta=pod.get("metadata",{})
    if meta.get("deletionTimestamp"):
        continue
    conditions=pod.get("status",{}).get("conditions",[])
    if any(c.get("type")=="Ready" and c.get("status")=="True" for c in conditions):
        ready.append(meta["name"])
if len(ready) != 1:
    raise SystemExit(f"expected one Ready worker pod, found {ready}")
print(ready[0])
'
}

valkey_json() {
    # Authentication expands only inside the Valkey pod and never reaches host
    # argv or output.
    # shellcheck disable=SC2016
    kube -n "$NAMESPACE" exec "statefulset/$VALKEY_STATEFULSET" -- \
        sh -c 'REDISCLI_AUTH="$VALKEY_PASSWORD" exec valkey-cli --json "$@"' sh "$@"
}

seed_unrelated_valkey_keys() {
    local count="$1" result
    PADDING_KEY_PREFIX="curie:e2e:2714:padding:$(python3 -c 'import secrets; print(secrets.token_hex(16))'):"
    result="$(valkey_json EVAL '
local count=tonumber(ARGV[1])
for index=1,count do
  redis.call("SET",ARGV[2]..index,"padding","EX",ARGV[3])
end
return count
' 0 "$count" "$PADDING_KEY_PREFIX" 1800 | python3 -c 'import json,sys; print(int(json.load(sys.stdin)))')"
    [[ "$result" == "$count" ]] || die "Valkey padding seed returned $result, expected $count"
    echo "seeded $count unrelated owned Valkey keys before quota reclamation"
}

delete_unrelated_valkey_keys() {
    local deleted remaining
    [[ -n "$PADDING_KEY_PREFIX" && -n "$VALKEY_STATEFULSET" ]] || return 0
    deleted="$(valkey_json EVAL '
local cursor="0"
local deleted=0
repeat
  local scan=redis.call("SCAN",cursor,"MATCH",ARGV[1].."*","COUNT",1000)
  cursor=scan[1]
  local keys=scan[2]
  if #keys > 0 then
    deleted=deleted+redis.call("DEL",unpack(keys))
  end
until cursor == "0"
return deleted
' 0 "$PADDING_KEY_PREFIX" | python3 -c 'import json,sys; print(int(json.load(sys.stdin)))')" || return 1
    remaining="$(valkey_json EVAL '
local cursor="0"
local found=0
repeat
  local scan=redis.call("SCAN",cursor,"MATCH",ARGV[1].."*","COUNT",1000)
  cursor=scan[1]
  found=found+#scan[2]
until cursor == "0"
return found
' 0 "$PADDING_KEY_PREFIX" | python3 -c 'import json,sys; print(int(json.load(sys.stdin)))')" || return 1
    [[ "$remaining" == "0" ]] || {
        echo "error: $remaining owned Valkey padding keys remain after deleting $deleted" >&2
        return 1
    }
    echo "deleted $deleted owned Valkey padding keys"
    PADDING_KEY_PREFIX=""
}

route_keys() {
    valkey_json KEYS "${ROUTE_PREFIX}*" | python3 -c '
import json,sys
for value in sorted(json.load(sys.stdin)):
    print(value)
'
}

route_key_for_thread() {
    local thread="$1"
    route_keys | python3 -c '
import sys
thread=sys.argv[1]
matches=[line.strip() for line in sys.stdin if line.strip().endswith(":"+thread)]
if len(matches) != 1:
    raise SystemExit(f"expected one route ending in {thread!r}, found {matches}")
print(matches[0])
' "$thread"
}

route_record() {
    local route_key="$1"
    valkey_json GET "$route_key" | python3 -c '
import json,sys
outer=json.load(sys.stdin)
if not isinstance(outer,str):
    raise SystemExit("route is absent or not a string")
value=json.loads(outer)
if not isinstance(value,dict):
    raise SystemExit("route record is not an object")
print(json.dumps(value,sort_keys=True,separators=(",",":")))
'
}

route_field() {
    local route_key="$1" field="$2"
    route_record "$route_key" | python3 -c '
import json,sys
value=json.load(sys.stdin).get(sys.argv[1])
if value is None or value == "":
    raise SystemExit(f"route field {sys.argv[1]} is absent")
print(value)
' "$field"
}

route_expiry_ms() {
    local route_key="$1"
    valkey_json PEXPIRETIME "$route_key" | python3 -c '
import json,sys
value=int(json.load(sys.stdin))
if value <= 0:
    raise SystemExit(f"route has invalid absolute expiry {value}")
print(value)
'
}

resource_quota_used() {
    kube -n "$NAMESPACE" get resourcequota "$RESOURCE_QUOTA" \
        -o json | python3 -c '
import json,sys
resource=sys.argv[1]
value=json.load(sys.stdin).get("status",{}).get("used",{}).get(resource)
if value is None:
    raise SystemExit(f"ResourceQuota status.used has no {resource}")
print(value)
' "$QUOTA_RESOURCE"
}

wait_quota_usage() {
    local expected="$1" started=$SECONDS actual
    while (( SECONDS - started < WAIT_SECONDS )); do
        actual="$(resource_quota_used 2>/dev/null || true)"
        if [[ "$actual" == "$expected" ]]; then
            return 0
        fi
        sleep 0.25
    done
    die "ResourceQuota used.$QUOTA_RESOURCE did not become $expected"
}

assert_nonpressure_quota_headroom() {
    [[ -n "$QUOTA_OTHER_RESOURCE" ]] || return 0
    kube -n "$NAMESPACE" get resourcequota "$RESOURCE_QUOTA" -o json | python3 -c '
from decimal import Decimal,InvalidOperation
import json,sys
resource,hard=sys.argv[1:]
try:
    value=json.load(sys.stdin)
    used=Decimal(str(value.get("status",{}).get("used",{}).get(resource)))
    limit=Decimal(hard)
    status_hard=Decimal(str(value.get("status",{}).get("hard",{}).get(resource)))
except (InvalidOperation,ValueError) as exc:
    raise SystemExit(f"ResourceQuota {resource} headroom is not numeric: {exc}")
if status_hard != limit:
    raise SystemExit(f"ResourceQuota {resource} status.hard={status_hard} differs from configured hard={limit}")
if used + 1 > limit:
    raise SystemExit(f"ResourceQuota {resource} has insufficient nonpressure headroom: used={used} required=1 hard={limit}")
print(f"ResourceQuota {resource} nonpressure headroom: used={used} required=1 hard={limit}")
' "$QUOTA_OTHER_RESOURCE" "$QUOTA_OTHER_HARD"
}

start_quota_watch() {
    local destination="$1" snapshot="$2" error_file="$3" raw_file="$4"
    local resource_version used status_hard spec_hard template
    kube -n "$NAMESPACE" get resourcequota "$RESOURCE_QUOTA" -o json >"$snapshot"
    read -r resource_version used status_hard spec_hard < <(python3 - "$snapshot" "$QUOTA_RESOURCE" "$QUOTA_FULL" "$QUOTA_HARD" <<'PY'
import json,pathlib,sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
resource,expected_used,expected_hard=sys.argv[2:]
resource_version=value.get("metadata",{}).get("resourceVersion")
used=value.get("status",{}).get("used",{}).get(resource)
status_hard=value.get("status",{}).get("hard",{}).get(resource)
spec_hard=value.get("spec",{}).get("hard",{}).get(resource)
if not resource_version or str(used) != expected_used or str(status_hard) != expected_hard or str(spec_hard) != expected_hard:
    raise SystemExit(
        f"quota watch must start from {resource} used={expected_used} and hard={expected_hard} with a resourceVersion, "
        f"found resourceVersion={resource_version!r} used={used!r} status.hard={status_hard!r} spec.hard={spec_hard!r}"
    )
print(resource_version,used,status_hard,spec_hard)
PY
)
    printf 'SNAPSHOT\t%s\t%s\t%s\t%s\t%s\n' "$(timestamp_utc)" "$resource_version" "$used" "$status_hard" "$spec_hard" >"$destination"
    template="{{.type}}|{{.object.metadata.resourceVersion}}|{{index .object.status.used \"$QUOTA_RESOURCE\"}}|{{index .object.status.hard \"$QUOTA_RESOURCE\"}}|{{index .object.spec.hard \"$QUOTA_RESOURCE\"}}{{\"\\n\"}}"
    : >"$raw_file"
    "$GNU_PROCESS" setsid bash -c '
        set -o pipefail
        "$8" timeout --foreground "$1s" "$2" --context "$3" -n "$4" get resourcequota "$5" --watch --output-watch-events -o go-template="$6" |
            tee -a "$7" |
            while IFS= read -r line; do
                IFS="|" read -r event_type event_resource_version event_used event_status_hard event_spec_hard extra <<<"$line"
                if [[ -n "$event_type" && -n "$event_resource_version" && -n "$event_used" && -n "$event_status_hard" && -n "$event_spec_hard" && -z "$extra" ]]; then
                    printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$event_type" "$event_resource_version" "$event_used" "$event_status_hard" "$event_spec_hard"
                else
                    printf "%s\tMALFORMED\t%s\n" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$line"
                fi
            done
    ' bash "$WAIT_SECONDS" "$REAL_KUBECTL" "$KUBE_CONTEXT" "$NAMESPACE" "$RESOURCE_QUOTA" "$template" "$raw_file" "$GNU_PROCESS" >>"$destination" 2>>"$error_file" &
    QUOTA_WATCH_PID=$!
}

start_exact_resource_watch() {
    local kind="$1" name="$2" destination="$3" snapshot="$4" error_file="$5" raw_file="$6"
    local resource_version uid
    kube -n "$NAMESPACE" get "$kind" "$name" -o json >"$snapshot"
    read -r resource_version uid < <(python3 - "$snapshot" <<'PY'
import json,pathlib,sys
metadata=json.loads(pathlib.Path(sys.argv[1]).read_text()).get("metadata",{})
resource_version=metadata.get("resourceVersion")
uid=metadata.get("uid")
if not resource_version or not uid:
    raise SystemExit("watched resource has no resourceVersion or uid")
print(resource_version,uid)
PY
)
    printf 'SNAPSHOT\t%s\t%s\t%s\n' "$(timestamp_utc)" "$resource_version" "$uid" >"$destination"
    : >"$raw_file"
    "$GNU_PROCESS" setsid bash -c '
        set -o pipefail
        "$9" timeout --foreground "$1s" "$2" --context "$3" -n "$4" get "$5" "$6" --watch --output-watch-events -o "$7" |
            tee -a "$8" |
            while IFS= read -r line; do
                IFS="|" read -r event_type event_resource_version event_uid extra <<<"$line"
                if [[ -n "$event_type" && -n "$event_resource_version" && -n "$event_uid" && -z "$extra" ]]; then
                    printf "%s\t%s\t%s\t%s\n" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$event_type" "$event_resource_version" "$event_uid"
                else
                    printf "%s\tMALFORMED\t%s\n" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$line"
                fi
            done
    ' bash "$WAIT_SECONDS" "$REAL_KUBECTL" "$KUBE_CONTEXT" "$NAMESPACE" "$kind" "$name" 'jsonpath={.type}{"|"}{.object.metadata.resourceVersion}{"|"}{.object.metadata.uid}{"\n"}' "$raw_file" "$GNU_PROCESS" >>"$destination" 2>>"$error_file" &
    printf '%s\n' "$!"
}

wait_quota_watch_ready() {
    local destination="$1" error_file="$2" pid="$3" started=$SECONDS observed status
    while (( SECONDS - started < WAIT_SECONDS )); do
        if observed="$(python3 - "$destination" "$QUOTA_RESOURCE" "$QUOTA_FULL" "$QUOTA_HARD" <<'PY'
import pathlib,sys
resource,full,hard=sys.argv[2:]
rows=[line.split("\t") for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
snapshots=[row for row in rows if len(row)==6 and row[0]=="SNAPSHOT"]
for row in rows:
    if row[0]=="SNAPSHOT":
        if len(row)!=6:
            raise SystemExit(2)
    elif len(row)!=6 or row[1] not in {"ADDED","MODIFIED"} or not all(row[2:]):
        raise SystemExit(2)
if len(snapshots)!=1:
    raise SystemExit(1)
if snapshots[0][3:] != [full,hard,hard]:
    raise SystemExit(2)
events=[row for row in rows if row and row[0]!="SNAPSHOT"]
if not events:
    raise SystemExit(1)
first=events[0]
if len(first)!=6 or first[1]!="ADDED" or not first[2] or first[3:] != [full,hard,hard]:
    raise SystemExit(2)
print(first[2])
PY
)"; then
            [[ ! -s "$error_file" ]] || die "ResourceQuota watch wrote stderr before readiness: $(tail -c 500 "$error_file")"
            kill -0 "$pid" 2>/dev/null || die "ResourceQuota watch exited after its initial ADDED event"
            printf '%s\n' "$observed"
            return 0
        else
            status=$?
        fi
        (( status == 2 )) && die "ResourceQuota watch did not produce a valid initial ADDED event"
        [[ ! -s "$error_file" ]] || die "ResourceQuota watch wrote stderr before readiness: $(tail -c 500 "$error_file")"
        kill -0 "$pid" 2>/dev/null || die "ResourceQuota watch exited before its initial ADDED event"
        sleep 0.1
    done
    die "ResourceQuota watch did not become ready with an initial ADDED event"
}

wait_exact_resource_watch_ready() {
    local destination="$1" error_file="$2" pid="$3" description="$4" started=$SECONDS observed status
    while (( SECONDS - started < WAIT_SECONDS )); do
        if observed="$(python3 - "$destination" <<'PY'
import pathlib,sys
rows=[line.split("\t") for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
snapshots=[row for row in rows if len(row)==4 and row[0]=="SNAPSHOT" and row[3]]
for row in rows:
    if row[0]=="SNAPSHOT":
        if len(row)!=4 or not row[3]:
            raise SystemExit(2)
    elif len(row)!=4 or row[1] not in {"ADDED","MODIFIED","DELETED"} or not row[2] or not row[3]:
        raise SystemExit(2)
if len(snapshots)!=1:
    raise SystemExit(1)
events=[row for row in rows if row and row[0]!="SNAPSHOT"]
if not events:
    raise SystemExit(1)
first=events[0]
if len(first)!=4 or first[1]!="ADDED" or not first[2] or first[3]!=snapshots[0][3]:
    raise SystemExit(2)
print(first[2])
PY
)"; then
            [[ ! -s "$error_file" ]] || die "$description watch wrote stderr before readiness: $(tail -c 500 "$error_file")"
            kill -0 "$pid" 2>/dev/null || die "$description watch exited after its initial ADDED event"
            printf '%s\n' "$observed"
            return 0
        else
            status=$?
        fi
        (( status == 2 )) && die "$description watch did not produce a valid initial ADDED event"
        [[ ! -s "$error_file" ]] || die "$description watch wrote stderr before readiness: $(tail -c 500 "$error_file")"
        kill -0 "$pid" 2>/dev/null || die "$description watch exited before its initial ADDED event"
        sleep 0.1
    done
    die "$description watch did not become ready with an initial ADDED event"
}

wait_watch_deleted() {
    local destination="$1" error_file="$2" description="$3" started=$SECONDS
    local observed status
    while (( SECONDS - started < WAIT_SECONDS )); do
        if observed="$(python3 - "$destination" <<'PY'
import pathlib,sys
rows=[line.split("\t") for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
snapshots=[row for row in rows if len(row)==4 and row[0]=="SNAPSHOT" and row[3]]
for row in rows:
    if row[0]=="SNAPSHOT":
        if len(row)!=4 or not row[3]:
            raise SystemExit(2)
    elif len(row)!=4 or row[1] not in {"ADDED","MODIFIED","DELETED"} or not row[2] or not row[3]:
        raise SystemExit(2)
if len(snapshots)!=1:
    raise SystemExit(2)
expected_uid=snapshots[0][3]
for fields in rows:
    if len(fields)==4 and fields[1]=="DELETED" and fields[2] and fields[3]==expected_uid:
        print(f"{fields[0]}\t{fields[2]}\t{expected_uid}\t{fields[3]}")
        raise SystemExit(0)
raise SystemExit(1)
PY
)"; then
            [[ ! -s "$error_file" ]] || die "$description watch wrote stderr: $(tail -c 500 "$error_file")"
            printf '%s\n' "$observed"
            return 0
        else
            status=$?
        fi
        (( status == 2 )) && die "$description watch recorded malformed evidence"
        [[ ! -s "$error_file" ]] || die "$description watch wrote stderr: $(tail -c 500 "$error_file")"
        sleep 0.25
    done
    die "$description watch did not observe a DELETED event: $(tail -c 500 "$error_file" 2>/dev/null || true)"
}

assert_quota_watch_2_1_2() {
    local destination="$1" error_file="$2"
    [[ ! -s "$error_file" ]] || die "ResourceQuota watch wrote stderr: $(tail -c 500 "$error_file")"
    python3 - "$destination" "$error_file" "$QUOTA_RESOURCE" "$QUOTA_FULL" "$QUOTA_ONE" "$QUOTA_HARD" <<'PY'
import pathlib,sys
resource,full,one,hard=sys.argv[3:]
rows=[]
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    fields=line.split("\t")
    if fields[0]=="SNAPSHOT":
        if len(fields)!=6:
            raise SystemExit(f"ResourceQuota watch recorded malformed snapshot evidence: {fields!r}")
        rows.append((fields[1],"SNAPSHOT",fields[2],fields[3],fields[4],fields[5]))
    elif len(fields)==6 and fields[1] in {"ADDED","MODIFIED"} and all(fields[2:]):
        rows.append((fields[0],fields[1],fields[2],fields[3],fields[4],fields[5]))
    else:
        raise SystemExit(f"ResourceQuota watch recorded malformed or unexpected evidence: {fields!r}")
if len(rows)<2 or rows[0][1]!="SNAPSHOT" or rows[0][3:] != (full,hard,hard):
    raise SystemExit(f"ResourceQuota watch did not start at {resource} used={full} status.hard={hard} spec.hard={hard}: {rows}")
start=rows[1]
if start[1]!="ADDED" or start[3:] != (full,hard,hard):
    raise SystemExit(f"ResourceQuota watch did not observe a valid initial ADDED event: {rows}")
drop_index=next((index for index,row in enumerate(rows[2:],2) if row[3:]==(one,hard,hard)),None)
rebound_index=next((index for index,row in enumerate(rows[2:],2) if drop_index is not None and index>drop_index and row[3:]==(full,hard,hard)),None)
drop=rows[drop_index] if drop_index is not None else None
rebound=rows[rebound_index] if rebound_index is not None else None
if drop is None or rebound is None or len({start[2],drop[2],rebound[2]}) != 3:
    error=pathlib.Path(sys.argv[2]).read_text(errors="replace")[-500:]
    raise SystemExit(f"ResourceQuota API watch did not observe {resource} stream ordered {full} then {one} then {full} with matching hard values and distinct resourceVersions: rows={rows} watch_stderr={error!r}")
print(f"{start[2]}\t{drop[2]}\t{rebound[2]}\t{drop[0]}")
PY
}

record_evidence() {
    local destination="$1" event="$2" detail="$3"
    printf '%s\t%s\n' "$event" "$detail" >>"$destination"
}

assert_activation_evidence() {
    local destination="$1"
    python3 - "$destination" <<'PY'
import pathlib,sys
events={}
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    fields=line.split("\t",1)
    if len(fields) != 2:
        raise SystemExit(f"malformed activation evidence row: {line!r}")
    event,detail=fields
    if event in events:
        raise SystemExit(f"duplicate activation evidence event {event!r}")
    events[event]=detail
required=(
    "worker-log-order",
    "victim-claim-deleted",
    "victim-sandbox-deleted",
    "victim-pod-gone",
    "quota-watch-2-1-2",
    "quota-headroom-observed",
    "rejected-sandbox-observation",
    "retry-claim-bound-ready",
    "fresh-metric-one",
)
missing=[event for event in required if event not in events]
if missing:
    raise SystemExit(f"activation evidence is missing {missing}")
if len(events) != len(required):
    raise SystemExit(f"activation evidence has unexpected rows: {sorted(events)}")
print("activation evidence contains the nine required proof rows")
PY
}

observation_elapsed_seconds() {
    python3 - "$1" "$2" <<'PY'
from datetime import datetime
import sys
start=datetime.fromisoformat(sys.argv[1].replace("Z","+00:00"))
end=datetime.fromisoformat(sys.argv[2].replace("Z","+00:00"))
print(f"{(end-start).total_seconds():.3f}")
PY
}

additional_sandbox_state() {
    kube -n "$NAMESPACE" get sandboxes -o json | python3 -c '
import json,sys
known=set(sys.argv[1:])
observed=[]
for item in json.load(sys.stdin).get("items",[]):
    metadata=item.get("metadata",{})
    name=metadata.get("name")
    if name and name not in known:
        observed.append({
            "name":name,
            "uid":metadata.get("uid"),
            "creationTimestamp":metadata.get("creationTimestamp"),
            "deletionTimestamp":metadata.get("deletionTimestamp"),
            "status":item.get("status",{}),
        })
print(json.dumps(sorted(observed,key=lambda row:row["name"]),sort_keys=True,separators=(",",":")))
' "$@"
}

assert_worker_quota_authorization() {
    local service_account="$1" action resource namespace expected actual rc
    local subject="system:serviceaccount:$NAMESPACE:$service_account"
    assert_quota_authorization() {
        action="$1"
        resource="$2"
        namespace="$3"
        expected="$4"
        if actual="$(kube -n "$namespace" auth can-i "$action" "$resource" --as="$subject" 2>&1)"; then
            rc=0
        else
            rc=$?
        fi
        if [[ "$expected" == "yes" ]]; then
            [[ "$rc" == "0" && "$actual" == "yes" ]] || \
                die "worker service account quota authorization for namespace=$namespace $action $resource returned status=$rc answer=$actual, expected status=0 answer=yes"
        else
            [[ "$rc" == "1" && "$actual" == "no" ]] || \
                die "worker service account quota authorization for namespace=$namespace $action $resource returned status=$rc answer=$actual, expected status=1 answer=no"
        fi
    }
    assert_quota_authorization get "resourcequotas/$RESOURCE_QUOTA" "$NAMESPACE" yes
    for action in list watch create update patch delete deletecollection; do
        assert_quota_authorization "$action" resourcequotas "$NAMESPACE" no
    done
    for action in update patch delete; do
        assert_quota_authorization "$action" "resourcequotas/$RESOURCE_QUOTA" "$NAMESPACE" no
    done
    for action in get list; do
        if [[ "$action" == "get" ]]; then
            resource="resourcequotas/$RESOURCE_QUOTA"
        else
            resource="resourcequotas"
        fi
        assert_quota_authorization "$action" "$resource" default no
    done
}

assert_worker_exact_quota_get() {
    local worker
    worker="$(worker_pod)"
    kube -n "$NAMESPACE" exec "$worker" -c worker -- python -c '
import json,os,ssl,sys,urllib.request
namespace,name=sys.argv[1:]
token=open("/var/run/secrets/kubernetes.io/serviceaccount/token").read().strip()
context=ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
request=urllib.request.Request(
    f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/resourcequotas/{name}",
    headers={"Authorization":f"Bearer {token}"},
)
with urllib.request.urlopen(request,context=context,timeout=5) as response:
    value=json.load(response)
if value.get("metadata",{}).get("name") != name or value.get("metadata",{}).get("namespace") != namespace:
    raise SystemExit("worker service account exact ResourceQuota GET returned a different object")
print("worker service account exact ResourceQuota GET succeeded")
' "$NAMESPACE" "$RESOURCE_QUOTA"
}

claim_sandbox() {
    local claim="$1"
    kube -n "$NAMESPACE" get sandboxclaim "$claim" \
        -o jsonpath='{.status.sandbox.name}'
}

wait_claim_bound() {
    local claim="$1" started=$SECONDS sandbox
    while (( SECONDS - started < WAIT_SECONDS )); do
        sandbox="$(claim_sandbox "$claim" 2>/dev/null || true)"
        if [[ -n "$sandbox" ]] && kube -n "$NAMESPACE" get sandbox "$sandbox" \
            >/dev/null 2>&1 && kube -n "$NAMESPACE" get pod "$sandbox" -o json | python3 -c '
import json,sys
pod=json.load(sys.stdin)
conditions=pod.get("status",{}).get("conditions",[])
raise SystemExit(0 if any(c.get("type")=="Ready" and c.get("status")=="True" for c in conditions) else 1)
'; then
            printf '%s\n' "$sandbox"
            return 0
        fi
        sleep 0.25
    done
    die "SandboxClaim $claim did not bind a Ready sandbox"
}

runner_status() {
    local pod="$1"
    # Both credentials stay inside the runner. The result contains only status.
    kube -n "$NAMESPACE" exec "$pod" -c runner -- python -c '
import json,os,urllib.request
port=os.environ.get("CURIE_RUNNER_PORT","8080")
token=os.environ.get("CURIE_RUNNER_TOKEN","")
if not token:
    raise SystemExit("CURIE_RUNNER_TOKEN is absent")
request=urllib.request.Request(
    f"http://127.0.0.1:{port}/v1/status",
    headers={"Authorization":f"Bearer {token}"},
)
with urllib.request.urlopen(request,timeout=3) as response:
    print(json.dumps(json.load(response),sort_keys=True,separators=(",",":")))
'
}

wait_runner_idle_durable() {
    local pod="$1" started=$SECONDS status
    while (( SECONDS - started < WAIT_SECONDS )); do
        status="$(runner_status "$pod" 2>/dev/null || true)"
        if [[ -n "$status" ]] && printf '%s' "$status" | python3 -c '
import json,sys
value=json.load(sys.stdin)
safe=value.get("status") in {"done","idle-awaiting-input"}
raise SystemExit(0 if safe and value.get("turn_active") is False and value.get("history_durable") is True else 1)
'; then
            printf '%s\n' "$status"
            return 0
        fi
        sleep 0.25
    done
    die "runner pod $pod did not report authenticated idle durable status"
}

wait_resource_gone() {
    local kind="$1" name="$2" started=$SECONDS
    while (( SECONDS - started < WAIT_SECONDS )); do
        if [[ -z "$(kube -n "$NAMESPACE" get "$kind" "$name" --ignore-not-found -o name)" ]]; then
            return 0
        fi
        sleep 0.25
    done
    die "$kind $name did not disappear"
}

wait_no_resources() {
    local kind="$1" started=$SECONDS names
    while (( SECONDS - started < WAIT_SECONDS )); do
        names="$(kube -n "$NAMESPACE" get "$kind" -o name)"
        [[ -z "$names" ]] && return 0
        sleep 0.25
    done
    die "preexisting $kind remained after route reset: $names"
}

wait_route_gone() {
    local route_key="$1" started=$SECONDS exists
    while (( SECONDS - started < WAIT_SECONDS )); do
        exists="$(valkey_json EXISTS "$route_key" | python3 -c 'import json,sys; print(int(json.load(sys.stdin)))')"
        [[ "$exists" == "0" ]] && return 0
        sleep 0.25
    done
    die "route $route_key did not disappear"
}

message_json_field() {
    local path="$1" field="$2"
    python3 - "$path" "$field" <<'PY'
import json,pathlib,sys
path,field=sys.argv[1:]
value=json.loads(pathlib.Path(path).read_text())
item=value.get(field)
if item is None or item == "":
    raise SystemExit(f"message output has no {field}: {value}")
print(item)
PY
}

assert_finalized_no_capacity() {
    local label="$1" path="$2"
    python3 - "$label" "$path" <<'PY'
import json,pathlib,sys
label,path=sys.argv[1:]
value=json.loads(pathlib.Path(path).read_text())
reply=value.get("reply")
if value.get("finalized") is not True or not isinstance(reply,str) or not reply.strip():
    raise SystemExit(f"{label}: no finalized nonempty reply: {value}")
lower=reply.lower()
if "capacity" in lower or "try again shortly" in lower:
    raise SystemExit(f"{label}: reply carried a capacity refusal: {reply}")
print(f"{label}: finalized without capacity refusal")
PY
}

assert_corrected_refusal() {
    local path="$1" quota_name="$2"
    python3 - "$path" "$quota_name" <<'PY'
import json,pathlib,sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
reply=value.get("reply")
if value.get("finalized") is not True or not isinstance(reply,str):
    raise SystemExit(f"negative trigger did not finalize: {value}")
lower=reply.lower()
if "capacity" not in lower or "try again" not in lower:
    raise SystemExit(f"negative trigger did not carry the corrected capacity guidance: {reply}")
for forbidden in ("quota", "resource", "requested", "used", "hard", sys.argv[2].lower()):
    if forbidden and forbidden in lower:
        raise SystemExit(f"negative trigger leaked operator capacity detail {forbidden!r}: {reply}")
if "finishes" in lower or "will finish" in lower:
    raise SystemExit(f"negative trigger promised another conversation would finish: {reply}")
print("negative trigger returned corrected operator safe capacity guidance")
PY
}

run_message() {
    local label="$1" prompt="$2" output="$3" thread="${4:-}"
    local args=(--json cluster message "$prompt" --namespace "$NAMESPACE" --release "$RELEASE")
    args+=(--listen-host "$CURIE_E2E_LISTEN_HOST" --timeout-secs "$MESSAGE_TIMEOUT_SECONDS")
    if [[ -n "$thread" ]]; then
        args+=(--thread "$thread")
    fi
    if ! "$GNU_PROCESS" timeout "$((MESSAGE_TIMEOUT_SECONDS + 30))" "$BIN" "${args[@]}" \
        >"$output" 2>"${output%.json}.err"; then
        cat "${output%.json}.err" >&2 || true
        cat "$output" >&2 || true
        die "$label cluster message failed"
    fi
}

reset_thread() {
    local route_key="$1" output="$2" thread_key
    thread_key="${route_key#"$ROUTE_PREFIX"}"
    [[ "$thread_key" != "$route_key" && -n "$thread_key" ]] || \
        die "route key does not carry the expected prefix: $route_key"
    if ! "$GNU_PROCESS" timeout 90 "$BIN" --json cluster reset-thread "$AGENT" \
        --thread-key "$thread_key" --namespace "$NAMESPACE" --release "$RELEASE" \
        --yes >"$output" 2>"${output%.json}.err"; then
        cat "${output%.json}.err" >&2 || true
        die "public reset failed for $route_key"
    fi
    python3 - "$output" <<'PY'
import json,pathlib,sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
if value.get("requested") is not True:
    raise SystemExit(f"reset was not accepted: {value}")
PY
}

reset_and_wait_gone() {
    local thread_key="$1" claim="$2" sandbox="$3" slug="$4"
    reset_thread "$thread_key" "$WORKDIR/reset-$slug.json"
    wait_route_gone "$thread_key"
    wait_resource_gone sandboxclaim "$claim"
    wait_resource_gone sandbox "$sandbox"
    wait_resource_gone pod "$sandbox"
}

worker_logs_since() {
    local since="$1"
    kube -n "$NAMESPACE" logs "deployment/$WORKER_DEPLOYMENT" -c worker \
        --since-time="$since" --timestamps
}

wait_worker_log() {
    local since="$1" pattern="$2" started=$SECONDS
    while (( SECONDS - started < WAIT_SECONDS )); do
        if worker_logs_since "$since" 2>/dev/null | grep -Fq "$pattern"; then
            return 0
        fi
        sleep 0.5
    done
    die "worker log did not contain $pattern"
}

assert_capacity_log_order() {
    local since="$1" destination="$2"
    worker_logs_since "$since" >"$destination"
    python3 - "$destination" "$CAPACITY_LOG_PATTERN" "$RECLAIM_LOG_PATTERN" \
        "$QUOTA_RESOURCE" "$QUOTA_ONE" "$QUOTA_FULL" "$QUOTA_HARD" <<'PY'
import ast,json,pathlib,re,sys
capacity_pattern,reclaim_pattern=sys.argv[2:4]
rows=[]
for line in pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines():
    kubectl_stamp,separator,encoded=line.partition(" ")
    if not kubectl_stamp.endswith("Z") or not separator:
        if capacity_pattern in line or reclaim_pattern in line:
            raise SystemExit("relevant worker log row has no kubectl timestamp prefix")
        continue
    payload=encoded
    try:
        envelope=json.loads(payload)
    except json.JSONDecodeError as exc:
        if capacity_pattern in line or reclaim_pattern in line:
            raise SystemExit(f"relevant worker log row is not a valid structured JSON envelope: {exc}")
        continue
    if not isinstance(envelope,dict):
        if capacity_pattern in line or reclaim_pattern in line:
            raise SystemExit("relevant worker log row is not a JSON object")
        continue
    message=envelope.get("message")
    if not isinstance(message,str):
        if capacity_pattern in line or reclaim_pattern in line:
            raise SystemExit("relevant worker log row has no string message")
        continue
    if capacity_pattern not in message and reclaim_pattern not in message:
        continue
    rows.append((kubectl_stamp,message))
capacity=[row for row in rows if capacity_pattern in row[1]]
reclaimed=[row for row in rows if reclaim_pattern in row[1]]
if not capacity or not reclaimed:
    raise SystemExit("capacity accounting warning did not precede successful reclamation log")
expected_resource,requested,used,hard=sys.argv[4:]
expected_requested={expected_resource:requested}
expected_used={expected_resource:used}
expected_hard={expected_resource:hard}
for _,payload in capacity:
    if len(payload) > 16384:
        raise SystemExit("capacity accounting warning is too large to parse safely")
    maps=re.search(r" requested=(\{.*?\}) used=(\{.*?\}) hard=(\{.*\})$",payload)
    if not maps:
        raise SystemExit(f"capacity accounting warning did not contain complete requested, used, and hard maps: {payload!r}")
    try:
        actual=tuple(ast.literal_eval(group) for group in maps.groups())
    except (SyntaxError,ValueError,MemoryError,RecursionError) as exc:
        raise SystemExit(f"capacity accounting warning maps are not literal dictionaries: {exc}")
    if actual != (expected_requested,expected_used,expected_hard):
        raise SystemExit(
            "capacity accounting warning did not report the exact rejected resource maps: "
            f"requested={actual[0]!r} used={actual[1]!r} hard={actual[2]!r}"
        )
if len(reclaimed) != 1:
    raise SystemExit(f"expected one successful reclamation log, found {len(reclaimed)}")
if not any(stamp < reclaimed[0][0] for stamp,_ in capacity):
    raise SystemExit("capacity accounting warning did not precede successful reclamation log")
if not reclaimed[0][1].split(sys.argv[3],1)[1].strip():
    raise SystemExit("successful reclamation log did not name its retried event")
print(max(stamp for stamp,_ in capacity if stamp < reclaimed[0][0]))
PY
}

collector_logs_since() {
    local since="$1"
    kube -n "$NAMESPACE" logs "deployment/$OTEL_DEPLOYMENT" -c otel-collector \
        --since-time="$since" --timestamps
}

latest_reclaimed_metric_value() {
    local since="$1"
    collector_logs_since "$since" 2>/dev/null | python3 -c '
import re,sys

text=sys.stdin.read()
text=re.sub(r"(?m)^\d{4}-\d{2}-\d{2}T[^\s]+[ \t]+","",text)
metric_name=sys.argv[1]
points=[]
for metric in re.split(r"(?m)^[ \t]*Metric #\d+\n",text)[1:]:
    descriptor=metric.split("NumberDataPoints #",1)[0]
    if not re.search(r"(?m)^[ \t]*(?:->[ \t]*)?Name:[ \t]*"+re.escape(metric_name)+r"[ \t]*$",descriptor):
        continue
    if not re.search(r"(?m)^[ \t]*(?:->[ \t]*)?DataType:[ \t]*Sum[ \t]*$",descriptor):
        continue
    if not re.search(r"(?m)^[ \t]*(?:->[ \t]*)?AggregationTemporality:[ \t]*Cumulative[ \t]*$",descriptor):
        continue
    for point in re.split(r"(?m)^[ \t]*NumberDataPoints #\d+\n",metric)[1:]:
        point=re.split(r"(?m)^[ \t]*Metric #\d+\n",point,1)[0]
        attributes=set(re.findall(r"(?m)^\s*->\s*([^\n]+)\s*$",point))
        if "operation: Str(reclaim)" not in attributes:
            continue
        if "outcome: Str(reclaimed)" not in attributes:
            continue
        values=re.findall(r"(?m)^\s*Value:\s*([0-9]+(?:\.0+)?)\s*$",point)
        if len(values) != 1:
            continue
        points.append(values[0])
if not points:
    raise SystemExit(1)
print(points[-1])
' "$METRIC_NAME"
}

wait_reclaimed_metric_value() {
    local since="$1" expected="$2" evidence_file="$3" event="${4:-}" started=$SECONDS actual
    while (( SECONDS - started < WAIT_SECONDS )); do
        actual="$(latest_reclaimed_metric_value "$since" 2>/dev/null || true)"
        if [[ "$actual" == "$expected" ]]; then
            if [[ -n "$event" ]]; then
                record_evidence "$evidence_file" "$event" \
                    "$METRIC_NAME=$actual operation=reclaim outcome=reclaimed"
            fi
            echo "$METRIC_NAME operation=reclaim outcome=reclaimed cumulative_value=$actual observed"
            return 0
        fi
        sleep 1
    done
    die "$METRIC_NAME exact reclaim metric cumulative value $expected was not exported"
}

assert_history_boot_log() {
    local pod="$1"
    kube -n "$NAMESPACE" logs "$pod" -c runner | python3 -c '
import re,sys
matches=re.findall(r"history loaded session=\S+ records=(\d+) messages=(\d+) compacted=(\S+)",sys.stdin.read())
if not matches:
    raise SystemExit("replacement runner has no history loaded log")
records,messages,_=matches[-1]
if int(records) < 1 or int(messages) < 2:
    raise SystemExit(f"replacement runner loaded too little history: records={records} messages={messages}")
print(f"replacement runner boot loaded records={records} messages={messages}")
'
}

assert_history_ref_and_nonce() {
    local pod="$1" expected_ref="$2" nonce="$3"
    kube -n "$NAMESPACE" exec "$pod" -c runner -- sh -ec '
        test -n "$CURIE_HISTORY_REF"
        test "$CURIE_HISTORY_REF" = "$1"
    ' sh "$expected_ref"
    # The state token and reference remain inside the runner. Only the assertion
    # result and transcript record count leave the pod.
    kube -n "$NAMESPACE" exec "$pod" -c runner -- python -c '
import json,os,sys,urllib.request
ref=os.environ.get("CURIE_HISTORY_REF","")
token=os.environ.get("CURIE_HISTORY_TOKEN","")
if not ref or not token:
    raise SystemExit("history reference or token is absent")
request=urllib.request.Request(ref,headers={"X-API-Key":token})
with urllib.request.urlopen(request,timeout=5) as response:
    payload=json.load(response)
value=payload.get("value")
if not isinstance(value,list):
    raise SystemExit("transcript value is not an array")
if sys.argv[1] not in json.dumps(value,sort_keys=True):
    raise SystemExit("planted nonce is absent from transcript")
print(f"authenticated transcript contains planted nonce across {len(value)} record(s)")
' "$nonce"
}

filler_claim_identity() {
    kube -n "$NAMESPACE" get sandboxclaim "$1" "$2" \
        -o json | python3 -c '
import json,sys
rows=[]
for item in json.load(sys.stdin).get("items",[]):
    meta=item.get("metadata",{})
    rows.append((meta.get("name"),meta.get("uid"),meta.get("deletionTimestamp"),item.get("status",{}).get("sandbox",{}).get("name")))
if sorted(row[0] for row in rows) != sorted(sys.argv[1:]):
    raise SystemExit(f"filler claims differ from expected names: {rows}")
print(json.dumps(sorted(rows),separators=(",",":")))
' "$1" "$2"
}

runner_pod_ip() {
    kube -n "$NAMESPACE" get pod "$1" -o jsonpath='{.status.podIP}'
}

probe_runner_from_worker() {
    local pod_ip="$1" expected="$2" worker
    worker="$(worker_pod)"
    kube -n "$NAMESPACE" exec "$worker" -c worker -- python -c '
import socket,sys,urllib.error,urllib.request
url=f"http://{sys.argv[1]}:8080/status"
expected=sys.argv[2]
try:
    with urllib.request.urlopen(url,timeout=2) as response:
        response.read(1)
        reachable=True
except urllib.error.HTTPError as exc:
    exc.read(1)
    reachable=True
except (urllib.error.URLError,TimeoutError,socket.timeout,OSError):
    reachable=False
if expected == "reachable" and not reachable:
    raise SystemExit("runner was not reachable")
if expected == "unreachable" and reachable:
    raise SystemExit("runner returned an HTTP response")
' "$pod_ip" "$expected"
}

wait_fillers_unreachable() {
    local started=$SECONDS all_blocked pod ip
    while (( SECONDS - started < 30 )); do
        all_blocked=1
        for pod in "${FILLER_PODS[@]}"; do
            ip="$(runner_pod_ip "$pod")"
            if ! probe_runner_from_worker "$ip" unreachable 2>/dev/null; then
                all_blocked=0
                break
            fi
        done
        (( all_blocked )) && return 0
        sleep 0.5
    done
    die "NetworkPolicy did not make every filler runner unreachable from the worker"
}

worker_established_runner_connections() {
    local worker
    worker="$(worker_pod)"
    kube -n "$NAMESPACE" exec "$worker" -c worker -- python -c '
import re,sys
expected_port=f"{int(sys.argv[1]):04X}"
total=0
for path,remote_header,address_length in (
    ("/proc/net/tcp", "rem_address", 8),
    ("/proc/net/tcp6", "remote_address", 32),
):
    try:
        lines=open(path,encoding="ascii").read().splitlines()
    except OSError as exc:
        raise SystemExit(f"cannot read {path}: {exc}") from exc
    if not lines:
        raise SystemExit(f"{path} is empty")
    header=lines[0].split()
    if header[:4] != ["sl", "local_address", remote_header, "st"]:
        raise SystemExit(f"unexpected {path} header")
    for line in lines[1:]:
        fields=line.split()
        if len(fields) < 4:
            raise SystemExit(f"malformed {path} row")
        remote,state=fields[2],fields[3]
        if not re.fullmatch(rf"[0-9A-Fa-f]{{{address_length}}}:[0-9A-Fa-f]{{4}}",remote):
            raise SystemExit(f"malformed {path} remote endpoint")
        if not re.fullmatch(r"[0-9A-Fa-f]{2}",state):
            raise SystemExit(f"malformed {path} state")
        if remote.rsplit(":",1)[1].upper() == expected_port and state == "01":
            total += 1
print(total)
' 8080
}

wait_worker_runner_connections_drained() {
    local started=$SECONDS established
    # The worker keeps an aiohttp connection pool. A policy blocks new dials but
    # an established pooled TCP connection can still yield a valid status reply.
    while (( SECONDS - started < 45 )); do
        established="$(worker_established_runner_connections)"
        [[ "$established" =~ ^[0-9]+$ ]] || die "worker runner connection observation was not an integer"
        if (( established == 0 )); then
            echo "worker has no established outbound TCP connections to runner port 8080"
            return 0
        fi
        sleep 0.5
    done
    die "worker retained established outbound TCP connections to runner port 8080"
}

assert_fillers_reachable() {
    local pod ip
    for pod in "${FILLER_PODS[@]}"; do
        [[ -n "$(kube -n "$NAMESPACE" get pod "$pod" --ignore-not-found -o name)" ]] || continue
        ip="$(runner_pod_ip "$pod")"
        probe_runner_from_worker "$ip" reachable
    done
}

wait_fillers_reachable() {
    local started=$SECONDS
    while (( SECONDS - started < 30 )); do
        if assert_fillers_reachable 2>/dev/null; then
            return 0
        fi
        sleep 0.5
    done
    echo "error: worker reachability did not return for every filler runner" >&2
    return 1
}

canonical_policy() {
    python3 - "$1" <<'PY'
import json,pathlib,sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
meta=value.get("metadata",{})
keep={key:meta[key] for key in ("name","namespace","labels","annotations","ownerReferences","finalizers") if key in meta}
print(json.dumps({"apiVersion":value.get("apiVersion"),"kind":value.get("kind"),"metadata":keep,"spec":value.get("spec")},sort_keys=True,separators=(",",":")))
PY
}

restore_runner_ingress() {
    local expected actual live_file="$WORKDIR/runner-ingress.live.json"
    if (( DENY_POLICY_CREATED )); then
        if ! kube -n "$NAMESPACE" delete networkpolicy "$DENY_POLICY" \
            --ignore-not-found --wait=true >/dev/null; then
            echo "error: could not remove filler deny policy $DENY_POLICY" >&2
            return 1
        fi
        DENY_POLICY_CREATED=0
    fi
    if (( ALLOW_POLICY_REMOVED )); then
        if ! python3 - "$ORIGINAL_ALLOW_FILE" "$RESTORE_ALLOW_FILE" <<'PY'
import json,pathlib,sys
source,destination=sys.argv[1:]
value=json.loads(pathlib.Path(source).read_text())
value.pop("status",None)
meta=value.get("metadata",{})
for key in ("creationTimestamp","generation","managedFields","resourceVersion","selfLink","uid"):
    meta.pop(key,None)
pathlib.Path(destination).write_text(json.dumps(value,sort_keys=True,separators=(",",":")))
PY
        then
            echo "error: could not prepare the saved runner ingress policy" >&2
            return 1
        fi
        if ! kube -n "$NAMESPACE" create -f "$RESTORE_ALLOW_FILE" >/dev/null 2>&1; then
            if ! kube -n "$NAMESPACE" get networkpolicy "$RUNNER_INGRESS_POLICY" \
                -o json >"$live_file"; then
                echo "error: could not restore runner ingress policy $RUNNER_INGRESS_POLICY" >&2
                return 1
            fi
            expected="$(canonical_policy "$ORIGINAL_ALLOW_FILE")" || return 1
            actual="$(canonical_policy "$live_file")" || return 1
            if [[ "$actual" != "$expected" ]]; then
                echo "error: runner ingress policy restore collided with a different object" >&2
                return 1
            fi
        fi
        ALLOW_POLICY_REMOVED=0
    fi
    if (( ALLOW_POLICY_SAVED )); then
        if ! kube -n "$NAMESPACE" get networkpolicy "$RUNNER_INGRESS_POLICY" \
            -o json >"$live_file"; then
            echo "error: restored runner ingress policy is absent" >&2
            return 1
        fi
        expected="$(canonical_policy "$ORIGINAL_ALLOW_FILE")" || return 1
        actual="$(canonical_policy "$live_file")" || return 1
        [[ "$actual" == "$expected" ]] || {
            echo "error: runner ingress policy did not restore exactly" >&2
            return 1
        }
        wait_fillers_reachable || return 1
        ALLOW_POLICY_SAVED=0
        echo "runner ingress allow policy restored exactly and worker reachability returned"
    fi
}

cleanup() {
    local code=$? cleanup_failed=0 pod
    trap - EXIT INT TERM
    set +e
    stop_pid "$QUOTA_WATCH_PID"
    stop_pid "$VICTIM_CLAIM_WATCH_PID"
    stop_pid "$VICTIM_SANDBOX_WATCH_PID"
    if (( code != 0 )); then
        print_watch_diagnostic quota "$QUOTA_WATCH_RAW" "$QUOTA_WATCH_ERROR"
        print_watch_diagnostic victim-claim "$VICTIM_CLAIM_WATCH_RAW" "$VICTIM_CLAIM_WATCH_ERROR"
        print_watch_diagnostic victim-sandbox "$VICTIM_SANDBOX_WATCH_RAW" "$VICTIM_SANDBOX_WATCH_ERROR"
    fi
    restore_runner_ingress || cleanup_failed=1
    delete_unrelated_valkey_keys || cleanup_failed=1
    # Empty when a run fails before the fillers exist, which bash 3.2 refuses
    # to expand under `set -u` without the `+` guard.
    for pod in ${FILLER_PODS[@]+"${FILLER_PODS[@]}"}; do
        kube -n "$NAMESPACE" label pod "$pod" "$FILLER_LABEL_NAME-" \
            --overwrite >/dev/null 2>&1 || true
    done
    if (( cleanup_failed == 0 )); then
        rm -rf -- "$WORKDIR"
    else
        echo "error: preserved cleanup evidence and saved policy under $WORKDIR" >&2
    fi
    if (( code == 0 && cleanup_failed != 0 )); then
        code=1
    fi
    exit "$code"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

require_command kubectl
require_command helm
require_command python3
REAL_KUBECTL="$(command -v kubectl)"
[[ "$NAMESPACE" == "test-2714-idle-route-reclamation" ]] || \
    die "CURIE_E2E_NAMESPACE must be the exact throwaway namespace test-2714-idle-route-reclamation"
[[ "$KUBE_CONTEXT" == "k8" ]] || die "CURIE_E2E_KUBE_CONTEXT must be k8"
[[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN:-}" ]] || \
    die "CURIE_BIN must name an executable candidate curie binary"
[[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]] || \
    die "CURIE_E2E_LISTEN_HOST must name the pod reachable callback host"
BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"

# Force every kubectl subprocess spawned by the public CLI onto the authorized
# homelab context. The wrapper adds an explicit argv flag, even when the CLI does
# not expose a context flag of its own.
mkdir -p "$WORKDIR/bin"
cat >"$WORKDIR/bin/kubectl" <<'SH'
#!/bin/sh
exec "${CURIE_E2E_REAL_KUBECTL:?}" --context "${CURIE_E2E_KUBE_CONTEXT:?}" "$@"
SH
chmod 0700 "$WORKDIR/bin/kubectl"
export CURIE_E2E_REAL_KUBECTL="$REAL_KUBECTL"
export CURIE_E2E_KUBE_CONTEXT="$KUBE_CONTEXT"
export PATH="$WORKDIR/bin:$PATH"

echo "=== fail closed cluster and release preconditions ==="
kube get namespace "$NAMESPACE" -o json | python3 -c '
import json,sys
value=json.load(sys.stdin)
if value.get("status",{}).get("phase") != "Active":
    raise SystemExit("throwaway namespace is not Active")
'
helm --kube-context "$KUBE_CONTEXT" -n "$NAMESPACE" status "$RELEASE" >/dev/null
kube -n agent-sandbox-system rollout status deployment/agent-sandbox-controller \
    --timeout=180s >/dev/null

WORKER_DEPLOYMENT="$(one_resource deployment \
    "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker" \
    "worker Deployment")"
VALKEY_STATEFULSET="$(statefulset_owner_for_component valkey "Valkey")"
OTEL_DEPLOYMENT="$(deployment_for_component_selector otel-collector "OTel Collector")"
RESOURCE_QUOTA="$(one_resource resourcequota \
    "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=tenant-capacity" \
    "sandbox ResourceQuota")"
RUNNER_TEMPLATE="$(one_resource sandboxtemplate \
    "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=agent-sandbox" \
    "runner SandboxTemplate")"
RUNNER_INGRESS_POLICY="$(kube -n "$NAMESPACE" get networkpolicy \
    -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=agent-sandbox" \
    -o json | python3 -c '
import json,sys
names=[item["metadata"]["name"] for item in json.load(sys.stdin).get("items",[]) if item["metadata"]["name"].endswith("-runner-ingress")]
if len(names) != 1:
    raise SystemExit(f"expected one runner ingress allow policy, found {names}")
print(names[0])
')"
OTEL_CONFIGMAP="$(kube -n "$NAMESPACE" get deployment "$OTEL_DEPLOYMENT" \
    -o json | python3 -c '
import json,sys
value=json.load(sys.stdin)
names=[]
for volume in value.get("spec",{}).get("template",{}).get("spec",{}).get("volumes",[]):
    config=volume.get("configMap") or {}
    if volume.get("name")=="config" and config.get("name"):
        names.append(config["name"])
if len(names)!=1:
    raise SystemExit(f"expected one collector config volume, found {names}")
print(names[0])
')"

kube -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" --timeout=180s >/dev/null
kube -n "$NAMESPACE" rollout status "statefulset/$VALKEY_STATEFULSET" --timeout=180s >/dev/null
kube -n "$NAMESPACE" rollout status "deployment/$OTEL_DEPLOYMENT" --timeout=180s >/dev/null

kube -n "$NAMESPACE" get deployment "$WORKER_DEPLOYMENT" -o json | python3 -c '
import json,sys
value=json.load(sys.stdin)
spec=value.get("spec",{})
if spec.get("replicas") != 1 or value.get("status",{}).get("readyReplicas") != 1:
    raise SystemExit("proof requires exactly one Ready worker replica")
env={row.get("name"):row.get("value") for c in spec.get("template",{}).get("spec",{}).get("containers",[]) if c.get("name")=="worker" for row in c.get("env",[])}
if env.get("CURIE_FAKE_MODEL") != "1":
    raise SystemExit("worker is not configured for fake model")
claim=float(env.get("CURIE_CLAIM_TIMEOUT_SECONDS","0"))
budget=float(env.get("CURIE_DELIVERY_BUDGET_S","0"))
ttl=float(env.get("CURIE_ROUTE_TTL_SECONDS","0"))
if budget < 120 + claim:
    raise SystemExit(f"delivery budget {budget} does not leave a conservative pressure proof window beyond claim timeout {claim}")
if ttl < 900:
    raise SystemExit(f"route TTL {ttl} is too short for this proof")
'

WORKER_SERVICE_ACCOUNT="$(kube -n "$NAMESPACE" get deployment "$WORKER_DEPLOYMENT" -o json | python3 -c '
import json,sys
name=json.load(sys.stdin).get("spec",{}).get("template",{}).get("spec",{}).get("serviceAccountName")
if not name:
    raise SystemExit("worker Deployment has no serviceAccountName")
print(name)
')"

kube -n "$NAMESPACE" get resourcequota "$RESOURCE_QUOTA" -o json | python3 -c '
import json,sys
value=json.load(sys.stdin)
resource,hard,other_resource,other_hard=sys.argv[1:]
actual=value.get("spec",{}).get("hard",{})
if str(actual.get(resource)) != hard:
    raise SystemExit(f"sandbox ResourceQuota spec.hard.{resource} is {actual.get(resource)!r}, expected {hard}")
if other_resource and str(actual.get(other_resource)) != other_hard:
    raise SystemExit(f"sandbox ResourceQuota spec.hard.{other_resource} is {actual.get(other_resource)!r}, expected {other_hard}")
' "$QUOTA_RESOURCE" "$QUOTA_HARD" "$QUOTA_OTHER_RESOURCE" "$QUOTA_OTHER_HARD"
assert_worker_quota_authorization "$WORKER_SERVICE_ACCOUNT"
assert_worker_exact_quota_get
kube -n "$NAMESPACE" get sandboxwarmpools \
    -l "app.kubernetes.io/instance=$RELEASE" -o json | python3 -c '
import json,sys
items=json.load(sys.stdin).get("items",[])
if len(items)!=1:
    raise SystemExit(f"expected one SandboxWarmPool, found {len(items)}")
replicas=items[0].get("spec",{}).get("replicas")
if replicas != 0:
    raise SystemExit(f"SandboxWarmPool replicas is {replicas}, expected 0")
'
kube -n "$NAMESPACE" get sandboxtemplate "$RUNNER_TEMPLATE" -o json | python3 -c '
import json,sys
value=json.load(sys.stdin)
expected_cpu=sys.argv[1]
containers=value.get("spec",{}).get("podTemplate",{}).get("spec",{}).get("containers",[])
runner=next((c for c in containers if c.get("name")=="runner"),None)
if runner is None:
    raise SystemExit("runner container is absent from SandboxTemplate")
env={row.get("name"):row.get("value") for row in runner.get("env",[])}
if env.get("CURIE_FAKE_MODEL") != "1":
    raise SystemExit("runner SandboxTemplate is not fake model")
if env.get("CURIE_RUNNER_PORT") != "8080":
    raise SystemExit(f"runner port is {env.get('CURIE_RUNNER_PORT')!r}, expected 8080")
if expected_cpu and str(runner.get("resources",{}).get("limits",{}).get("cpu")) != expected_cpu:
    raise SystemExit(f"runner cpu limit is {runner.get('resources',{}).get('limits',{}).get('cpu')!r}, expected {expected_cpu}")
' "$RUNNER_CPU_LIMIT"
kube -n "$NAMESPACE" get configmap "$OTEL_CONFIGMAP" -o json | python3 -c '
import json,re,sys
value=json.load(sys.stdin)
lines="\n".join(value.get("data",{}).values()).splitlines()
name=sys.argv[1]
def child_block(parent,indent,key):
    prefix=" " * indent + key + ":"
    for index,line in enumerate(parent):
        if line.rstrip()==prefix:
            block=[]
            for child in parent[index+1:]:
                if child and len(child)-len(child.lstrip(" "))<=indent:
                    break
                block.append(child)
            return block
    return []
service=child_block(lines,0,"service")
pipelines=child_block(service,2,"pipelines")
metrics=child_block(pipelines,4,"metrics")
exporter_line=next((line for line in metrics if re.fullmatch(r" {6}exporters:[ \t]*\[[^]]+\][ \t]*",line)),None)
match=re.fullmatch(r" {6}exporters:[ \t]*\[([^]]+)\][ \t]*",exporter_line or "")
if not match or name not in [item.strip() for item in match.group(1).split(",")]:
    raise SystemExit(f"collector metrics pipeline does not export to {name}")
exporters=child_block(lines,0,"exporters")
detail=child_block(exporters,2,name)
if not detail or not any(re.fullmatch(r" {4}verbosity:[ \t]*detailed[ \t]*",line) for line in detail):
    raise SystemExit(f"collector exporter {name} must use detailed verbosity")
' "$METRIC_DEBUG_EXPORTER"

dispatcher_ready="$(kube -n "$NAMESPACE" get deployment \
    -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=dispatcher" \
    -o json | python3 -c 'import json,sys; print(sum(int(i.get("status",{}).get("readyReplicas",0) or 0) for i in json.load(sys.stdin).get("items",[])))')"
[[ "$dispatcher_ready" == "0" ]] || die "proof requires the disconnected cluster message relay"

echo "preconditions: context=$KUBE_CONTEXT namespace=$NAMESPACE quotaResource=$QUOTA_RESOURCE quotaHard=$QUOTA_HARD warmPool=0 fakeModel=1"

echo "=== deploy the weather bundle through the public cluster CLI ==="
cp -a "$REPO_ROOT/examples/weather" "$WORKDIR/weather"
"$BIN" --json cluster deploy --plugin-dir "$WORKDIR/weather" \
    --namespace "$NAMESPACE" --release "$RELEASE" >"$WORKDIR/deploy.json"
python3 - "$WORKDIR/deploy.json" <<'PY'
import json,pathlib,sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
if value.get("deployment",{}).get("status") != "active":
    raise SystemExit(f"deployment is not active: {value}")
PY

echo "=== release routes left by the required cluster ladder ==="
# A read loop, not mapfile: bash 3.2, which macOS ships, has no mapfile.
preexisting_routes=()
while IFS= read -r route_key; do
    preexisting_routes+=("$route_key")
done < <(route_keys)
for index in "${!preexisting_routes[@]}"; do
    reset_thread "${preexisting_routes[$index]}" "$WORKDIR/reset-preexisting-$index.json"
    wait_route_gone "${preexisting_routes[$index]}"
done
wait_no_resources sandboxclaims
wait_no_resources sandboxes
wait_quota_usage "$QUOTA_EMPTY"

NONCE="idle-route-2714-$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
echo "=== plant durable history on the oldest route ==="
run_message "oldest route" "Remember this exact continuity marker: $NONCE" \
    "$WORKDIR/oldest.json"
assert_finalized_no_capacity "oldest route" "$WORKDIR/oldest.json"
A_THREAD="$(message_json_field "$WORKDIR/oldest.json" thread)"
A_ROUTE="$(route_key_for_thread "$A_THREAD")"
A_RECORD="$(route_record "$A_ROUTE")"
A_CLAIM="$(printf '%s' "$A_RECORD" | python3 -c 'import json,sys; print(json.load(sys.stdin)["claim_name"])')"
A_SANDBOX="$(wait_claim_bound "$A_CLAIM")"
A_HISTORY_REF="$(printf '%s' "$A_RECORD" | python3 -c 'import json,sys; print(json.load(sys.stdin)["history_ref"])')"
[[ -n "$A_HISTORY_REF" && "$A_HISTORY_REF" != "None" ]] || die "oldest route has no history reference"
wait_runner_idle_durable "$A_SANDBOX" >/dev/null
A_EXPIRY="$(route_expiry_ms "$A_ROUTE")"

sleep 1
echo "=== fill the second quota slot with a newer completed route ==="
run_message "newer filler" "Complete a second idle route for quota pressure." \
    "$WORKDIR/newer.json"
assert_finalized_no_capacity "newer filler" "$WORKDIR/newer.json"
B_THREAD="$(message_json_field "$WORKDIR/newer.json" thread)"
B_ROUTE="$(route_key_for_thread "$B_THREAD")"
B_CLAIM="$(route_field "$B_ROUTE" claim_name)"
B_SANDBOX="$(wait_claim_bound "$B_CLAIM")"
wait_runner_idle_durable "$B_SANDBOX" >/dev/null
B_EXPIRY="$(route_expiry_ms "$B_ROUTE")"
(( A_EXPIRY < B_EXPIRY )) || die "oldest route does not have the earlier absolute expiry"
wait_quota_usage "$QUOTA_FULL"
assert_nonpressure_quota_headroom

echo "=== restart the worker and preserve absolute route order ==="
OLD_WORKER="$(worker_pod)"
kube -n "$NAMESPACE" rollout restart "deployment/$WORKER_DEPLOYMENT" >/dev/null
kube -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" --timeout=180s >/dev/null
NEW_WORKER="$(worker_pod)"
[[ "$NEW_WORKER" != "$OLD_WORKER" ]] || die "worker restart did not replace the pod"
[[ "$(route_expiry_ms "$A_ROUTE")" == "$A_EXPIRY" ]] || die "oldest route expiry changed across worker restart"
[[ "$(route_expiry_ms "$B_ROUTE")" == "$B_EXPIRY" ]] || die "newer route expiry changed across worker restart"
[[ "$(route_field "$A_ROUTE" history_ref)" == "$A_HISTORY_REF" ]] || die "history reference changed across worker restart"
seed_unrelated_valkey_keys 5001

echo "=== trigger real quota rejection and reclaim the oldest idle route ==="
POSITIVE_SINCE="$(timestamp_utc)"
QUOTA_WATCH_FILE="$WORKDIR/quota-watch.tsv"
QUOTA_WATCH_SNAPSHOT="$WORKDIR/quota-watch-start.json"
QUOTA_WATCH_ERROR="$WORKDIR/quota-watch.err"
QUOTA_WATCH_RAW="$WORKDIR/quota-watch.raw"
VICTIM_CLAIM_WATCH_FILE="$WORKDIR/victim-claim-watch.tsv"
VICTIM_CLAIM_WATCH_SNAPSHOT="$WORKDIR/victim-claim-watch-start.json"
VICTIM_CLAIM_WATCH_ERROR="$WORKDIR/victim-claim-watch.err"
VICTIM_CLAIM_WATCH_RAW="$WORKDIR/victim-claim-watch.raw"
VICTIM_SANDBOX_WATCH_FILE="$WORKDIR/victim-sandbox-watch.tsv"
VICTIM_SANDBOX_WATCH_SNAPSHOT="$WORKDIR/victim-sandbox-watch-start.json"
VICTIM_SANDBOX_WATCH_ERROR="$WORKDIR/victim-sandbox-watch.err"
VICTIM_SANDBOX_WATCH_RAW="$WORKDIR/victim-sandbox-watch.raw"
ACTIVATION_EVIDENCE="$WORKDIR/positive-activation.tsv"
start_quota_watch "$QUOTA_WATCH_FILE" "$QUOTA_WATCH_SNAPSHOT" "$QUOTA_WATCH_ERROR" "$QUOTA_WATCH_RAW"
VICTIM_CLAIM_WATCH_PID="$(start_exact_resource_watch sandboxclaim "$A_CLAIM" \
    "$VICTIM_CLAIM_WATCH_FILE" "$VICTIM_CLAIM_WATCH_SNAPSHOT" "$VICTIM_CLAIM_WATCH_ERROR" "$VICTIM_CLAIM_WATCH_RAW")"
VICTIM_SANDBOX_WATCH_PID="$(start_exact_resource_watch sandbox "$A_SANDBOX" \
    "$VICTIM_SANDBOX_WATCH_FILE" "$VICTIM_SANDBOX_WATCH_SNAPSHOT" "$VICTIM_SANDBOX_WATCH_ERROR" "$VICTIM_SANDBOX_WATCH_RAW")"
QUOTA_WATCH_READY_RESOURCE_VERSION="$(wait_quota_watch_ready "$QUOTA_WATCH_FILE" "$QUOTA_WATCH_ERROR" "$QUOTA_WATCH_PID")"
VICTIM_CLAIM_WATCH_READY_RESOURCE_VERSION="$(wait_exact_resource_watch_ready "$VICTIM_CLAIM_WATCH_FILE" "$VICTIM_CLAIM_WATCH_ERROR" "$VICTIM_CLAIM_WATCH_PID" "victim SandboxClaim")"
VICTIM_SANDBOX_WATCH_READY_RESOURCE_VERSION="$(wait_exact_resource_watch_ready "$VICTIM_SANDBOX_WATCH_FILE" "$VICTIM_SANDBOX_WATCH_ERROR" "$VICTIM_SANDBOX_WATCH_PID" "victim Sandbox")"
echo "watch readiness: quotaResourceVersion=$QUOTA_WATCH_READY_RESOURCE_VERSION claimResourceVersion=$VICTIM_CLAIM_WATCH_READY_RESOURCE_VERSION sandboxResourceVersion=$VICTIM_SANDBOX_WATCH_READY_RESOURCE_VERSION"
run_message "quota recovery" "Start only after recovering one idle quota slot." \
    "$WORKDIR/recovered.json"
assert_finalized_no_capacity "quota recovery" "$WORKDIR/recovered.json"
POSITIVE_METRIC_SINCE="$(timestamp_utc)"
C_THREAD="$(message_json_field "$WORKDIR/recovered.json" thread)"
C_ROUTE="$(route_key_for_thread "$C_THREAD")"
C_CLAIM="$(route_field "$C_ROUTE" claim_name)"
wait_worker_log "$POSITIVE_SINCE" "$CAPACITY_LOG_PATTERN"
wait_worker_log "$POSITIVE_SINCE" "$RECLAIM_LOG_PATTERN"
assert_capacity_log_order "$POSITIVE_SINCE" "$WORKDIR/positive-worker.log" >/dev/null
record_evidence "$ACTIVATION_EVIDENCE" worker-log-order "capacity warning precedes exactly one reclaim line"
wait_resource_gone sandboxclaim "$A_CLAIM"
wait_resource_gone sandbox "$A_SANDBOX"
wait_resource_gone pod "$A_SANDBOX"
wait_route_gone "$A_ROUTE"
record_evidence "$ACTIVATION_EVIDENCE" victim-pod-gone "pod=$A_SANDBOX"
read -r CLAIM_DELETED_OBSERVED_AT CLAIM_DELETED_RESOURCE_VERSION CLAIM_SNAPSHOT_UID CLAIM_DELETED_UID < <(
    wait_watch_deleted "$VICTIM_CLAIM_WATCH_FILE" "$VICTIM_CLAIM_WATCH_ERROR" "victim SandboxClaim"
)
read -r SANDBOX_DELETED_OBSERVED_AT SANDBOX_DELETED_RESOURCE_VERSION SANDBOX_SNAPSHOT_UID SANDBOX_DELETED_UID < <(
    wait_watch_deleted "$VICTIM_SANDBOX_WATCH_FILE" "$VICTIM_SANDBOX_WATCH_ERROR" "victim Sandbox"
)
[[ "$CLAIM_SNAPSHOT_UID" == "$CLAIM_DELETED_UID" ]] || die "victim SandboxClaim delete UID differs from its snapshot"
[[ "$SANDBOX_SNAPSHOT_UID" == "$SANDBOX_DELETED_UID" ]] || die "victim Sandbox delete UID differs from its snapshot"
record_evidence "$ACTIVATION_EVIDENCE" victim-claim-deleted \
    "resourceVersion=$CLAIM_DELETED_RESOURCE_VERSION snapshotUid=$CLAIM_SNAPSHOT_UID deletedUid=$CLAIM_DELETED_UID"
record_evidence "$ACTIVATION_EVIDENCE" victim-sandbox-deleted \
    "resourceVersion=$SANDBOX_DELETED_RESOURCE_VERSION snapshotUid=$SANDBOX_SNAPSHOT_UID deletedUid=$SANDBOX_DELETED_UID"
stop_pid "$VICTIM_CLAIM_WATCH_PID"
VICTIM_CLAIM_WATCH_PID=""
stop_pid "$VICTIM_SANDBOX_WATCH_PID"
VICTIM_SANDBOX_WATCH_PID=""
[[ -n "$(kube -n "$NAMESPACE" get sandboxclaim "$B_CLAIM" --ignore-not-found -o name)" ]] || \
    die "newer route was reclaimed instead of the oldest route"
wait_quota_usage "$QUOTA_FULL"
stop_pid "$QUOTA_WATCH_PID"
QUOTA_WATCH_PID=""
read -r QUOTA_START_RESOURCE_VERSION QUOTA_DROP_RESOURCE_VERSION QUOTA_REBOUND_RESOURCE_VERSION QUOTA_HEADROOM_OBSERVED_AT < <(
    assert_quota_watch_2_1_2 "$QUOTA_WATCH_FILE" "$QUOTA_WATCH_ERROR"
)
record_evidence "$ACTIVATION_EVIDENCE" quota-watch-2-1-2 \
    "resource=$QUOTA_RESOURCE resourceVersions=$QUOTA_START_RESOURCE_VERSION,$QUOTA_DROP_RESOURCE_VERSION,$QUOTA_REBOUND_RESOURCE_VERSION"
RESOURCES_GONE_OBSERVED_AT="$CLAIM_DELETED_OBSERVED_AT"
if [[ "$SANDBOX_DELETED_OBSERVED_AT" > "$RESOURCES_GONE_OBSERVED_AT" ]]; then
    RESOURCES_GONE_OBSERVED_AT="$SANDBOX_DELETED_OBSERVED_AT"
fi
HEADROOM_OBSERVATION_SECONDS="$(observation_elapsed_seconds "$RESOURCES_GONE_OBSERVED_AT" "$QUOTA_HEADROOM_OBSERVED_AT")"
record_evidence "$ACTIVATION_EVIDENCE" quota-headroom-observed \
    "resource=$QUOTA_RESOURCE used=$QUOTA_ONE requested=$QUOTA_ONE hard=$QUOTA_HARD resourcesGoneObservedAt=$RESOURCES_GONE_OBSERVED_AT headroomObservedAt=$QUOTA_HEADROOM_OBSERVED_AT observationSeconds=$HEADROOM_OBSERVATION_SECONDS"
echo "quota headroom observation: resource=$QUOTA_RESOURCE resourcesGoneObservedAt=$RESOURCES_GONE_OBSERVED_AT headroomObservedAt=$QUOTA_HEADROOM_OBSERVED_AT observationSeconds=$HEADROOM_OBSERVATION_SECONDS"
C_SANDBOX="$(wait_claim_bound "$C_CLAIM")"
C_CLAIM_RESOURCE_VERSION="$(kube -n "$NAMESPACE" get sandboxclaim "$C_CLAIM" -o jsonpath='{.metadata.resourceVersion}')"
[[ -n "$C_CLAIM_RESOURCE_VERSION" ]] || die "replacement SandboxClaim has no resourceVersion"
record_evidence "$ACTIVATION_EVIDENCE" retry-claim-bound-ready "resourceVersion=$C_CLAIM_RESOURCE_VERSION pod=$C_SANDBOX"
REJECTED_SANDBOX_STATE="$(additional_sandbox_state "$B_SANDBOX" "$C_SANDBOX")"
record_evidence "$ACTIVATION_EVIDENCE" rejected-sandbox-observation \
    "additionalSandboxesAfterRetry=$REJECTED_SANDBOX_STATE"
wait_runner_idle_durable "$C_SANDBOX" >/dev/null
# Source order emits this metric only after the retry returns. The focused fix
# pin asserts that guard with an opened retry and a deleted victim. Receipt time
# is deliberately not used to order the metric against Kubernetes watches.
wait_reclaimed_metric_value "$POSITIVE_METRIC_SINCE" 1 "$ACTIVATION_EVIDENCE" fresh-metric-one
assert_activation_evidence "$ACTIVATION_EVIDENCE"
echo "activation evidence records required rows without cross clock ordering"

echo "=== reset the trigger route before revisiting reclaimed history ==="
reset_and_wait_gone "$C_ROUTE" "$C_CLAIM" "$C_SANDBOX" trigger
wait_quota_usage "$QUOTA_ONE"

echo "=== revisit the reclaimed thread through a real replacement runner ==="
run_message "history revisit" "Use the durable conversation and confirm continuity." \
    "$WORKDIR/revisit.json" "$A_THREAD"
assert_finalized_no_capacity "history revisit" "$WORKDIR/revisit.json"
A_REPLACEMENT_ROUTE="$(route_key_for_thread "$A_THREAD")"
A_REPLACEMENT_CLAIM="$(route_field "$A_REPLACEMENT_ROUTE" claim_name)"
A_REPLACEMENT_SANDBOX="$(wait_claim_bound "$A_REPLACEMENT_CLAIM")"
[[ "$A_REPLACEMENT_CLAIM" != "$A_CLAIM" ]] || die "history revisit reused the reclaimed claim"
[[ "$(route_field "$A_REPLACEMENT_ROUTE" history_ref)" == "$A_HISTORY_REF" ]] || \
    die "replacement route changed the exact history reference"
assert_history_ref_and_nonce "$A_REPLACEMENT_SANDBOX" "$A_HISTORY_REF" "$NONCE"
assert_history_boot_log "$A_REPLACEMENT_SANDBOX"
wait_runner_idle_durable "$A_REPLACEMENT_SANDBOX" >/dev/null
wait_quota_usage "$QUOTA_FULL"

echo "=== release positive routes before the isolated negative ==="
reset_and_wait_gone "$A_REPLACEMENT_ROUTE" "$A_REPLACEMENT_CLAIM" \
    "$A_REPLACEMENT_SANDBOX" history
reset_and_wait_gone "$B_ROUTE" "$B_CLAIM" "$B_SANDBOX" newer
wait_quota_usage "$QUOTA_EMPTY"

echo "=== create two completed filler routes for the network negative ==="
run_message "negative filler one" "Complete filler route one." "$WORKDIR/filler-one.json"
F1_THREAD="$(message_json_field "$WORKDIR/filler-one.json" thread)"
F1_ROUTE="$(route_key_for_thread "$F1_THREAD")"
F1_CLAIM="$(route_field "$F1_ROUTE" claim_name)"
F1_SANDBOX="$(wait_claim_bound "$F1_CLAIM")"
wait_runner_idle_durable "$F1_SANDBOX" >/dev/null
sleep 1
run_message "negative filler two" "Complete filler route two." "$WORKDIR/filler-two.json"
F2_THREAD="$(message_json_field "$WORKDIR/filler-two.json" thread)"
F2_ROUTE="$(route_key_for_thread "$F2_THREAD")"
F2_CLAIM="$(route_field "$F2_ROUTE" claim_name)"
F2_SANDBOX="$(wait_claim_bound "$F2_CLAIM")"
wait_runner_idle_durable "$F2_SANDBOX" >/dev/null
wait_quota_usage "$QUOTA_FULL"
assert_nonpressure_quota_headroom
FILLER_PODS=("$F1_SANDBOX" "$F2_SANDBOX")
for pod in "${FILLER_PODS[@]}"; do
    kube -n "$NAMESPACE" label pod "$pod" \
        "$FILLER_LABEL_NAME=$FILLER_LABEL_VALUE" --overwrite >/dev/null
done
kube -n "$NAMESPACE" get pods -l "$FILLER_LABEL_NAME=$FILLER_LABEL_VALUE" \
    -o json | python3 -c '
import json,sys
actual=sorted(item["metadata"]["name"] for item in json.load(sys.stdin).get("items",[]))
expected=sorted(sys.argv[1:])
if actual != expected:
    raise SystemExit(f"filler label selected {actual}, expected only {expected}")
' "${FILLER_PODS[@]}"
assert_fillers_reachable

echo "=== replace the additive allow policy with a filler only deny policy ==="
kube -n "$NAMESPACE" get networkpolicy "$RUNNER_INGRESS_POLICY" -o json \
    >"$ORIGINAL_ALLOW_FILE"
ALLOW_POLICY_SAVED=1
kube -n "$NAMESPACE" delete networkpolicy "$RUNNER_INGRESS_POLICY" --wait=true >/dev/null
ALLOW_POLICY_REMOVED=1
kube -n "$NAMESPACE" apply -f - >/dev/null <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: $DENY_POLICY
  namespace: $NAMESPACE
spec:
  podSelector:
    matchLabels:
      $FILLER_LABEL_NAME: $FILLER_LABEL_VALUE
  policyTypes:
    - Ingress
YAML
DENY_POLICY_CREATED=1
wait_fillers_unreachable
wait_worker_runner_connections_drained
wait_fillers_unreachable
echo "worker received no HTTP response from either filler runner"

NEGATIVE_IDENTITY_BEFORE="$(filler_claim_identity "$F1_CLAIM" "$F2_CLAIM")"
NEGATIVE_SINCE="$(timestamp_utc)"
echo "=== unreachable candidates fail closed without deletion ==="
run_message "negative trigger" "Prove unreachable idle candidates fail closed." \
    "$WORKDIR/negative.json"
assert_corrected_refusal "$WORKDIR/negative.json" "$RESOURCE_QUOTA"
NEGATIVE_METRIC_SINCE="$(timestamp_utc)"
NEGATIVE_IDENTITY_AFTER="$(filler_claim_identity "$F1_CLAIM" "$F2_CLAIM")"
[[ "$NEGATIVE_IDENTITY_AFTER" == "$NEGATIVE_IDENTITY_BEFORE" ]] || \
    die "negative trigger changed a filler claim identity"
wait_quota_usage "$QUOTA_FULL"
[[ "$(route_field "$F1_ROUTE" claim_name)" == "$F1_CLAIM" ]] || die "filler route one changed"
[[ "$(route_field "$F2_ROUTE" claim_name)" == "$F2_CLAIM" ]] || die "filler route two changed"
NEGATIVE_THREAD="$(message_json_field "$WORKDIR/negative.json" thread)"
if route_key_for_thread "$NEGATIVE_THREAD" >/dev/null 2>&1; then
    die "negative trigger retained a route after refusal"
fi
wait_worker_log "$NEGATIVE_SINCE" "$CAPACITY_LOG_PATTERN"
assert_capacity_log_order "$POSITIVE_SINCE" "$WORKDIR/all-worker.log"
wait_reclaimed_metric_value "$NEGATIVE_METRIC_SINCE" 1 "$WORKDIR/negative-metric.tsv"
echo "negative trigger preserved both fillers and a fresh cumulative reclaim metric remained one"

echo "=== restore exact network policy and prove reachability ==="
restore_runner_ingress

echo "=== release filler routes through the public reset surface ==="
reset_and_wait_gone "$F1_ROUTE" "$F1_CLAIM" "$F1_SANDBOX" filler-one
reset_and_wait_gone "$F2_ROUTE" "$F2_CLAIM" "$F2_SANDBOX" filler-two
wait_quota_usage "$QUOTA_EMPTY"

echo "ISSUE 2714 CLUSTER IDLE ROUTE RECLAMATION PASS resource=$QUOTA_RESOURCE"
