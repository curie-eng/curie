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

fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
  kubectl -n "$NS" delete pod "$PROBE_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
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

kubectl -n "$NS" delete pod "$PROBE_POD" --ignore-not-found --wait=true --timeout=90s \
  >/dev/null 2>&1 || true
# The labels Rail 1 and the connector policies select, so the probe is treated
# exactly as a sandbox is.
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $PROBE_POD
  labels:
    app.kubernetes.io/name: $APP
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
YAML
kubectl -n "$NS" wait --for=condition=Ready "pod/$PROBE_POD" --timeout=180s >/dev/null \
  || fail "the sandbox-labelled probe pod did not become ready"

ADMITTED_LEGS=0
for svc in "${CONNECTORS[@]}"; do
  kubectl -n "$NS" rollout status "deployment/$svc" --timeout=180s >/dev/null 2>&1 \
    || fail "connector Deployment $svc did not become ready"

  PORT="$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.ports[0].port}')"
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

echo "== every connector's caller proxy enforces =="
