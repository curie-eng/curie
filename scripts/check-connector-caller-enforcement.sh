#!/usr/bin/env bash
# Assert every hosted connector's caller proxy ENFORCES (ADR-0168 decision 7).
#
# Run after check-netpol-enforcement.sh on the same cluster. That gate proves
# the CNI evaluates NetworkPolicy at all, which the denial in leg 3 needs before
# it means anything. Per connector Service:
#
#   1. The Service lands on the caller proxy. A sandbox-labelled probe with no
#      token gets HTTP 403 from the Service port: an answer, so the path is
#      open, and a refusal, so the proxy answered and not the server.
#   2. The server listens on its own port inside the pod, proved over loopback
#      from the proxy container, so leg 3 cannot pass on a missing listener.
#   3. The same probe dialling the pod IP at the server's port is denied. The
#      rendered policies open only the proxy's port, and they match the pod
#      port after the Service DNAT, which is what leg 1 lands on.
#   4. A token for an agent the connector admits reaches the server: the
#      answer is neither the proxy's 403 nor its 502, so the runner's header,
#      the key pair and the Host the proxy forwards all line up.
#   5. A token for an agent the connector does not admit gets the 403 that
#      says `not_admitted`.
#
# Then, across the namespace, the grant store credential (#5550):
#
#   6. The Valkey password, read from the Secret behind the API's own
#      VALKEY_PASSWORD and never printed, is not empty and appears in no
#      Deployment, no Pod and no ReplicaSet with replicas above zero, and every
#      connector's caller proxy reads it by secretKeyRef from that Secret and
#      key. A superseded ReplicaSet at zero replicas that still holds the old
#      literal is reported, not failed: only rotating the password clears it.
#   7. Inside each connector's caller proxy container, the production config
#      loader (ProxyConfig.from_env) spends one fresh grant id twice against
#      the real Valkey: the first spend succeeds, the second is refused, and
#      the grant key exists in the Valkey pod.
#   8. The same spend with the password variable overridden to a wrong value,
#      for that exec only, spends nothing and writes no key.
#
# A connector's direct Service (`<name>-direct`) fronts the same pods on the
# server's port, which only an operator policy opens, so connectors are listed
# by their name label and each is probed once.
#
# Legs 4 and 5 mint with CURIE_CALLER_SIGNING_KEY, the seed the install was
# given, inside CURIE_CALLER_MINT_IMAGE (default curie-worker:local), with the
# primitive the worker signs with. The seed reaches the container by name, never
# on an argv.
#
# CI runs this in the kind lane, the e2e-ladder-cluster job of
# .github/workflows/ci.yaml, which seeds the key pair. That lane runs for pull
# requests against main, pushes to main and dispatched runs, the nightly one
# included; pull requests against next and pushes to next omit it.
set -euo pipefail

NS="${1:-curie}"
RELEASE="${2:-curie}"
APP="${3:-curie}"
PROBE_IMAGE="${CURIE_NETPOL_PROBE_IMAGE:-curlimages/curl:8.10.1}"
MINT_IMAGE="${CURIE_CALLER_MINT_IMAGE:-curie-worker:local}"
PROBE_POD="caller-probe-sandbox"
OTHER_AGENT_POD="caller-probe-other-agent"

fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
  kubectl -n "$NS" delete pod "$PROBE_POD" "$OTHER_AGENT_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== connector caller proxy enforcement (namespace=$NS release=$RELEASE app=$APP) =="

[ -n "${CURIE_CALLER_SIGNING_KEY:-}" ] \
  || fail "CURIE_CALLER_SIGNING_KEY is not set, so no admitted caller can be proved to get through"

# A token naming $1, valid for ten minutes.
mint() {
  docker run --rm -e CURIE_CALLER_SIGNING_KEY "$MINT_IMAGE" python -c '
import os, sys, time
from curie_worker.caller_token import mint
print(mint(os.environ["CURIE_CALLER_SIGNING_KEY"], agent=sys.argv[1], exp=int(time.time()) + 600))
' "$1"
}

# Every connector is listed by the name label its Services share, so a direct
# Service is not taken for a second connector.
CONNECTORS=()
while IFS= read -r svc; do
  [ -n "$svc" ] && CONNECTORS+=("$svc")
done < <(
  kubectl -n "$NS" get svc -l app.kubernetes.io/part-of="$RELEASE" \
    -o jsonpath='{range .items[*]}{.metadata.labels.app\.kubernetes\.io/name}{"\n"}{end}' \
    2>/dev/null | grep -- "-mcp-" | sort -u || true
)
(( ${#CONNECTORS[@]} > 0 )) \
  || fail "found 0 connector Services in $NS; the caller proxy check would be vacuous"

# `<release>-<agent>-mcp-<connector>`. The positive probe wears that agent so
# ingress selects it. A second probe wears a different agent and must not connect.
owning_agent() {
  local name="$1"
  local prefix="${RELEASE}-"
  local rest agent
  case "$name" in
    "$prefix"*) rest="${name#"$prefix"}" ;;
    *) return 1 ;;
  esac
  case "$rest" in
    *-mcp-*) agent="${rest%%-mcp-*}" ;;
    *) return 1 ;;
  esac
  [ -n "$agent" ] || return 1
  printf '%s' "$agent"
}

FIRST_AGENT=""
for svc in "${CONNECTORS[@]}"; do
  if FIRST_AGENT="$(owning_agent "$svc")"; then
    break
  fi
  FIRST_AGENT=""
done

kubectl -n "$NS" delete pod "$PROBE_POD" "$OTHER_AGENT_POD" --ignore-not-found --wait=true --timeout=90s \
  >/dev/null 2>&1 || true
# The labels Rail 1 and the connector policies select, so the probe is treated
# exactly as a sandbox is. curietech.ai/agent is the owning agent when the
# object name carries one.
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $PROBE_POD
  labels:
    app.kubernetes.io/name: $APP
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
    curietech.ai/agent: "${FIRST_AGENT:-unset}"
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
YAML
kubectl -n "$NS" wait --for=condition=Ready "pod/$PROBE_POD" --timeout=180s >/dev/null \
  || fail "the sandbox-labelled probe pod did not become ready"
CURRENT_AGENT="${FIRST_AGENT}"
OTHER_AGENT_VALUE=""

ADMITTED_LEGS=0
for svc in "${CONNECTORS[@]}"; do
  kubectl -n "$NS" rollout status "deployment/$svc" --timeout=180s >/dev/null 2>&1 \
    || fail "connector Deployment $svc did not become ready"

  PORT="$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.ports[0].port}')"
  AGENT="$(owning_agent "$svc" || true)"
  if [ -n "$AGENT" ] && [ "$AGENT" != "$CURRENT_AGENT" ]; then
    kubectl -n "$NS" label pod "$PROBE_POD" "curietech.ai/agent=${AGENT}" --overwrite >/dev/null
    CURRENT_AGENT="$AGENT"
  fi
  if [ -n "$AGENT" ]; then
    other="not-${AGENT}"
    if [ "$other" != "$OTHER_AGENT_VALUE" ]; then
      OTHER_AGENT_VALUE="$other"
      kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $OTHER_AGENT_POD
  labels:
    app.kubernetes.io/name: $APP
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
    curietech.ai/agent: ${other}
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
YAML
      kubectl -n "$NS" wait --for=condition=Ready "pod/$OTHER_AGENT_POD" --timeout=180s >/dev/null \
        || fail "the other-agent probe did not become ready"
    fi
    if kubectl -n "$NS" exec "$OTHER_AGENT_POD" -- \
         curl -s -m 8 -o /dev/null "http://${svc}:${PORT}/" 2>/dev/null; then
      fail "a sandbox wearing curietech.ai/agent=${OTHER_AGENT_VALUE} reached connector Service $svc:$PORT; ingress is not limited to the owning agent"
    fi
    echo "  ok  other-agent probe cannot reach connector $svc:$PORT"
  fi
  TARGET="$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.ports[0].targetPort}')"
  [ "$TARGET" = "caller" ] \
    || fail "connector Service $svc targets '$TARGET', not the caller proxy; does the API hold a caller public key?"

  CODE="$(kubectl -n "$NS" exec "$PROBE_POD" -- \
    curl -s -m 10 -o /dev/null -w '%{http_code}' "http://${svc}:${PORT}/mcp" 2>/dev/null || true)"
  [ "$CODE" = "403" ] \
    || fail "a tokenless call to $svc:$PORT answered '$CODE', not the caller proxy's 403"
  echo "  ok  $svc:$PORT lands on the caller proxy, which refuses a tokenless call"

  POD_IP="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name="$svc" \
    -o jsonpath='{.items[0].status.podIP}')"
  SERVER_PORT="$(kubectl -n "$NS" get deployment "$svc" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="server")].ports[0].containerPort}')"
  [ -n "$POD_IP" ] && [ -n "$SERVER_PORT" ] \
    || fail "could not read the pod IP and the server port of $svc"

  kubectl -n "$NS" exec "deployment/$svc" -c caller-proxy -- \
    python -c "import socket; socket.create_connection(('127.0.0.1', $SERVER_PORT), 5).close()" \
    >/dev/null 2>&1 \
    || fail "$svc's server does not listen on $SERVER_PORT inside its pod, so a denial from outside would prove nothing"
  echo "  ok  $svc's server listens on $SERVER_PORT inside its pod"

  if kubectl -n "$NS" exec "$PROBE_POD" -- \
       curl -s -m 8 -o /dev/null "http://${POD_IP}:${SERVER_PORT}/mcp" >/dev/null 2>&1; then
    fail "a sandbox reached $svc's server port $SERVER_PORT directly, past the caller proxy"
  fi
  echo "  ok  a sandbox cannot reach $svc's server port $SERVER_PORT directly"

  ADMITS="$(kubectl -n "$NS" get deployment "$svc" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="caller-proxy")].env[?(@.name=="CURIE_CALLER_PROXY_ADMITS")].value}')"
  read -r ADMITTED OUTSIDER < <(python3 -c '
import json, sys
names = json.loads(sys.argv[1] or "[]")
outsider = next(n for n in ("caller-gate-outsider", "caller-gate-outsider-2") if n not in names)
print(names[0] if names else "-", outsider)
' "$ADMITS") || fail "could not read the admits list of $svc: '$ADMITS'"
  if [ "$ADMITTED" = "-" ]; then
    echo "  --  $svc admits no agent, so no admitted caller can be sent"
    continue
  fi

  TOKEN="$(mint "$ADMITTED")" || fail "could not mint a caller token in $MINT_IMAGE"
  CODE="$(kubectl -n "$NS" exec "$PROBE_POD" -- \
    curl -s -m 10 -o /dev/null -w '%{http_code}' -H "X-Curie-Caller: $TOKEN" \
    "http://${svc}:${PORT}/mcp" 2>/dev/null || true)"
  case "$CODE" in
    403|502|000|"") fail "an admitted caller token for $ADMITTED did not reach the server behind $svc: '$CODE'" ;;
  esac
  echo "  ok  $svc admits a caller token for $ADMITTED, and the server answered $CODE"

  TOKEN="$(mint "$OUTSIDER")" || fail "could not mint a caller token in $MINT_IMAGE"
  ANSWER="$(kubectl -n "$NS" exec "$PROBE_POD" -- \
    curl -s -m 10 -w '\n%{http_code}' -H "X-Curie-Caller: $TOKEN" \
    "http://${svc}:${PORT}/mcp" 2>/dev/null || true)"
  CODE="${ANSWER##*$'\n'}"
  BODY="${ANSWER%$'\n'*}"
  if [ "$CODE" != "403" ] || ! grep -Eq '"curie_caller": ?"not_admitted"' <<<"$BODY"; then
    fail "a caller token for $OUTSIDER, whom $svc does not admit, got '$CODE' and not the not_admitted refusal"
  fi
  echo "  ok  $svc refuses an agent it does not admit"
  ADMITTED_LEGS=$((ADMITTED_LEGS + 1))
done

(( ADMITTED_LEGS > 0 )) \
  || fail "no connector admits an agent, so no admitted caller was proved to get through"

# -- leg 6 --------------------------------------------------------------------
# The password lives in a shell variable and reaches python3 only through its
# environment. It is never echoed and never on an argv.
API_DEPLOYMENT="$(kubectl -n "$NS" get deployment \
  -l "app.kubernetes.io/component=api,app.kubernetes.io/instance=$RELEASE" \
  -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
[ -n "$API_DEPLOYMENT" ] && [ "${API_DEPLOYMENT// /}" = "$API_DEPLOYMENT" ] \
  || fail "leg 6: expected one API Deployment for release $RELEASE in $NS, found '${API_DEPLOYMENT}'"
VALKEY_REF="$(kubectl -n "$NS" get deployment "$API_DEPLOYMENT" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="api")].env[?(@.name=="VALKEY_PASSWORD")].valueFrom.secretKeyRef.name}{" "}{.spec.template.spec.containers[?(@.name=="api")].env[?(@.name=="VALKEY_PASSWORD")].valueFrom.secretKeyRef.key}' \
  2>/dev/null || true)"
read -r VALKEY_SECRET VALKEY_KEY <<<"$VALKEY_REF" || true
[ -n "${VALKEY_SECRET:-}" ] && [ -n "${VALKEY_KEY:-}" ] \
  || fail "leg 6: the API Deployment $API_DEPLOYMENT reads VALKEY_PASSWORD from no Secret"
SECRET_JSON="$(kubectl -n "$NS" get secret "$VALKEY_SECRET" -o json 2>/dev/null)" \
  || fail "leg 6: could not read Secret $VALKEY_SECRET"
GRANT_STORE_PASSWORD="$(python3 -c '
import base64, json, sys
data = json.load(sys.stdin).get("data") or {}
sys.stdout.write(base64.b64decode(data.get(sys.argv[1], "")).decode())
' "$VALKEY_KEY" <<<"$SECRET_JSON")" \
  || fail "leg 6: could not decode key $VALKEY_KEY of Secret $VALKEY_SECRET"
unset SECRET_JSON
[ -n "$GRANT_STORE_PASSWORD" ] \
  || fail "leg 6: key $VALKEY_KEY of Secret $VALKEY_SECRET is empty, so no literal could be detected"
WORKLOADS="$(kubectl -n "$NS" get deployments,pods,replicasets -o json 2>/dev/null)" \
  || fail "leg 6: could not list the Deployments, Pods and ReplicaSets in $NS"
LEG6="$(GRANT_STORE_PASSWORD="$GRANT_STORE_PASSWORD" python3 -c '
import json, os, sys
from urllib.parse import quote

password = os.environ["GRANT_STORE_PASSWORD"]
secret, key = sys.argv[1], sys.argv[2]
needles = {password, json.dumps(password)[1:-1], quote(password, safe="")}
proxies = 0
for item in json.load(sys.stdin).get("items") or []:
    kind = item.get("kind", "?")
    name = item.get("metadata", {}).get("name", "?")
    texts = (json.dumps(item), json.dumps(item, ensure_ascii=False))
    if any(needle in text for needle in needles for text in texts):
        replicas = (item.get("spec") or {}).get("replicas")
        if kind == "ReplicaSet" and not replicas:
            print(f"INFO {name}")
        else:
            print(f"FAIL {kind}/{name} holds the Valkey password")
    if kind != "Deployment":
        continue
    containers = item["spec"]["template"]["spec"].get("containers") or []
    for container in containers:
        if container.get("name") != "caller-proxy":
            continue
        proxies += 1
        entry = next(
            (e for e in container.get("env") or []
             if e.get("name") == "CURIE_CALLER_PROXY_GRANT_STORE_PASSWORD"),
            None,
        )
        ref = ((entry or {}).get("valueFrom") or {}).get("secretKeyRef") or {}
        if ref.get("name") != secret or ref.get("key") != key:
            print(f"FAIL Deployment/{name} caller-proxy does not read "
                  f"CURIE_CALLER_PROXY_GRANT_STORE_PASSWORD from Secret {secret} key {key}")
        else:
            print(f"OK Deployment/{name} caller-proxy reads the password from Secret {secret} key {key}")
if not proxies:
    print("FAIL no Deployment carries a caller-proxy container")
' "$VALKEY_SECRET" "$VALKEY_KEY" <<<"$WORKLOADS")" \
  || fail "leg 6: could not scan the workloads in $NS"
unset WORKLOADS
LEG6_FAILED=0
while IFS= read -r line; do
  case "$line" in
    "FAIL "*) echo "FAIL: leg 6: ${line#FAIL }" >&2; LEG6_FAILED=1 ;;
    "INFO "*) echo "  --  leg 6: superseded ReplicaSet ${line#INFO } still holds the old literal (replicas 0; rotate the Valkey password)" ;;
    "OK "*) echo "  ok  leg 6: ${line#OK }" ;;
  esac
done <<<"$LEG6"
(( LEG6_FAILED == 0 )) \
  || fail "leg 6: the Valkey password is readable in a workload spec, or a caller proxy does not read it by reference"
echo "  ok  leg 6: no Deployment, Pod or live ReplicaSet in $NS holds the Valkey password"

# -- legs 7 and 8 -------------------------------------------------------------
VALKEY_POD="$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/component=valkey,app.kubernetes.io/instance=$RELEASE" \
  -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
VALKEY_POD="${VALKEY_POD%% *}"
[ -n "$VALKEY_POD" ] \
  || fail "leg 7: found no Valkey pod for release $RELEASE in $NS, so a spent grant cannot be confirmed"

# argv: the grant id, how many spends, then an optional password override that
# applies to this exec only.
SPEND_CODE='
import os, sys
from curie_connector_proxy.server import GRANT_STORE_PASSWORD_ENV, ProxyConfig

env = dict(os.environ)
if len(sys.argv) > 3:
    env[GRANT_STORE_PASSWORD_ENV] = sys.argv[3]
config = ProxyConfig.from_env(env)
if config.grant_store is None:
    raise SystemExit("no grant store is configured")
for _ in range(int(sys.argv[2])):
    print(config.grant_store.spend(sys.argv[1], 300))
'

grant_exists() {
  kubectl -n "$NS" exec "$VALKEY_POD" -c valkey -- \
    sh -c 'valkey-cli --no-auth-warning -a "$VALKEY_PASSWORD" exists "connector-grant:$1"' _ "$1" \
    2>/dev/null || true
}

fresh_uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

for svc in "${CONNECTORS[@]}"; do
  GRANT_ID="$(fresh_uuid)"
  SPENT="$(kubectl -n "$NS" exec "deployment/$svc" -c caller-proxy -- \
    python -c "$SPEND_CODE" "$GRANT_ID" 2 2>/dev/null || true)"
  [ "$SPENT" = $'True\nFalse' ] \
    || fail "leg 7: $svc's caller proxy spent a fresh grant as '${SPENT//$'\n'/,}', not True then False"
  EXISTS="$(grant_exists "$GRANT_ID")"
  [ "$EXISTS" = "1" ] \
    || fail "leg 7: the grant $svc's caller proxy spent is not in Valkey pod $VALKEY_POD (exists answered '$EXISTS')"
  echo "  ok  leg 7: $svc's caller proxy spends a fresh grant once against Valkey, and the key exists"

  GRANT_ID="$(fresh_uuid)"
  WRONG="wrong-$(fresh_uuid)"
  SPENT="$(kubectl -n "$NS" exec "deployment/$svc" -c caller-proxy -- \
    python -c "$SPEND_CODE" "$GRANT_ID" 1 "$WRONG" 2>/dev/null || true)"
  [ "$SPENT" = "False" ] \
    || fail "leg 8: $svc's caller proxy with a wrong password spent as '${SPENT//$'\n'/,}', not False"
  EXISTS="$(grant_exists "$GRANT_ID")"
  [ "$EXISTS" = "0" ] \
    || fail "leg 8: a spend with a wrong password left a key in Valkey pod $VALKEY_POD (exists answered '$EXISTS')"
  echo "  ok  leg 8: $svc's caller proxy with a wrong password spends nothing"
done
unset GRANT_STORE_PASSWORD

echo "== every connector's caller proxy enforces =="
