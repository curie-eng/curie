#!/usr/bin/env bash
# Prove a credential free cluster redeploy clears both secret surfaces and
# still starts an execution (#3853).
#
# Requires a release already installed by `curie cluster up --dev --fake-model`
# and the released `curie` binary in CURIE_BIN. CURIE_E2E_LISTEN_HOST is the
# pod reachable host for `cluster message` on a loopback API server.
set -euo pipefail

BIN="${CURIE_BIN:-curie}"
NAMESPACE="${CURIE_NAMESPACE:-curie}"
RELEASE="${CURIE_RELEASE:-curie}"
ns_rel=(--namespace "$NAMESPACE" --release "$RELEASE")

workdir="$(mktemp -d)"
pf_pid=""
cleanup() {
  if [[ -n "$pf_pid" ]]; then
    kill "$pf_pid" 2>/dev/null || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

"$BIN" init acme-cred --dir "$workdir/bundle" >/dev/null

export ACME_TOKEN_A=placeholder-a
export ACME_TOKEN_B=placeholder-b
export ACME_TOKEN_KEEP=placeholder-keep

deploy() {
  local agent="$1"
  local channel="$2"
  shift 2
  "$BIN" --json cluster deploy "${ns_rel[@]}" \
    --plugin-dir "$workdir/bundle" \
    --agent "$agent" \
    --slack-channel "$channel" \
    "$@"
}

echo "seed acme-cred with ACME_TOKEN_A"
deploy acme-cred C0EXAMPLE1 --secret ACME_TOKEN_A >/dev/null
echo "rotate acme-cred to ACME_TOKEN_B"
deploy acme-cred C0EXAMPLE1 --secret ACME_TOKEN_B >/dev/null
echo "seed unrelated acme-keep"
deploy acme-keep C0EXAMPLE2 --secret ACME_TOKEN_KEEP >/dev/null
echo "redeploy acme-cred with no connector secret"
deploy acme-cred C0EXAMPLE1 >/dev/null

kubectl -n "$NAMESPACE" rollout status deploy/curie-api --timeout=300s
kubectl -n "$NAMESPACE" rollout status deploy/curie-worker --timeout=300s

api_key="$(kubectl -n "$NAMESPACE" get secret "${RELEASE}-secrets" -o jsonpath='{.data.apiKey}' | base64 -d)"
kubectl -n "$NAMESPACE" port-forward --address 127.0.0.1 "svc/${RELEASE}-api" 18080:8000 >/dev/null &
pf_pid=$!
ready=0
for _ in $(seq 1 30); do
  if curl -fsS -H "X-API-Key: ${api_key}" http://127.0.0.1:18080/agents >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  echo "stale secret metadata: API port-forward did not answer" >&2
  exit 1
fi
export CURIE_STALE_SECRET_API_KEY="$api_key"
unset api_key

python3 - "$NAMESPACE" "$RELEASE" <<'PY'
import json, os, subprocess, sys, urllib.request

namespace, release = sys.argv[1:3]
request = urllib.request.Request(
    "http://127.0.0.1:18080/agents",
    headers={"X-API-Key": os.environ["CURIE_STALE_SECRET_API_KEY"]},
)
with urllib.request.urlopen(request, timeout=30) as response:
    agents = json.load(response)
by_name = {agent["name"]: agent.get("secrets") or [] for agent in agents}
if by_name.get("acme-cred") != []:
    raise SystemExit("acme-cred API secret names are %s" % by_name.get("acme-cred"))
if by_name.get("acme-keep") != ["ACME_TOKEN_KEEP"]:
    raise SystemExit("acme-keep API secret names are %s" % by_name.get("acme-keep"))

raw = subprocess.check_output(
    ["helm", "get", "values", release, "-n", namespace, "-o", "json"],
    text=True,
)
values = json.loads(raw)
bound = (
    values.get("agentSandbox", {}).get("connectorSecrets", {})
    if isinstance(values, dict)
    else {}
)
cred = bound.get("acme-cred") or {}
keep = bound.get("acme-keep") or {}
if cred:
    raise SystemExit("acme-cred Helm binding still has %s" % sorted(cred))
if sorted(keep) != ["ACME_TOKEN_KEEP"]:
    raise SystemExit("acme-keep Helm binding names are %s" % sorted(keep))
print("both surfaces cleared for acme-cred and retained for acme-keep")
PY

msg_args=(
  --json cluster message
  "${ns_rel[@]}"
  --agent acme-cred
  --channel C0EXAMPLE1
  --timeout-secs 300
  "reply with the word ready"
)
if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
  msg_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
fi
out="$("$BIN" "${msg_args[@]}")"
printf '%s\n' "$out" | python3 -c '
import json, sys
payload = json.loads(sys.stdin.read())
if payload.get("finalized") is not True or not str(payload.get("reply") or "").strip():
    raise SystemExit("execution did not finalize with a reply")
print("execution finalized")
'
