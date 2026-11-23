#!/usr/bin/env bash
#
# Render assertions for operator-named connector ServiceAccounts
# (docs/interfaces/connector-host/INTERFACE.md, "A connector's own
# ServiceAccount"). Four properties:
#
#   (a) A stock install hands the API CURIE_CONNECTOR_SERVICE_ACCOUNTS as `[]`,
#       and no other component receives it.
#   (b) A supplied connectorServiceAccounts list reaches the API as the same
#       list in JSON.
#   (c) The chart schema refuses an entry the API would refuse at boot: a name
#       that is not a ServiceAccount name, a missing key, an unknown key and an
#       empty agent.
#   (d) Naming a ServiceAccount grants the worker nothing: with the reconciler
#       on, the worker Role is the same with and without a mapping, and names
#       no serviceaccounts resource.
set -euo pipefail

# NOTE: read variables with a herestring, never `printf ... | cmd`; see
# sealing-key-assertions.sh for the pipefail trap this avoids.

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILED=0

fail() {
    echo "FAIL: $1" >&2
    FAILED=1
}

render() {
    helm template t "$1" -n t "${@:2}"
}

# Prints "<component> <value>" for every env entry named
# CURIE_CONNECTOR_SERVICE_ACCOUNTS, one line each.
holders() {
    python3 -c '
import sys, yaml

for doc in yaml.safe_load_all(sys.stdin):
    if not doc:
        continue
    component = (doc.get("metadata", {}).get("labels") or {}).get("app.kubernetes.io/component", "-")
    def walk(node):
        if isinstance(node, dict):
            for entry in node.get("env") or []:
                if isinstance(entry, dict) and entry.get("name") == "CURIE_CONNECTOR_SERVICE_ACCOUNTS":
                    print(component, entry.get("value", "<no literal value>"))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(doc)
'
}

# Prints the worker Role's rules as canonical JSON.
worker_role() {
    python3 -c '
import json, sys, yaml

for doc in yaml.safe_load_all(sys.stdin):
    if doc and doc.get("kind") == "Role" and doc["metadata"]["name"] == "t-curie-worker":
        print(json.dumps(doc.get("rules"), sort_keys=True))
'
}

VALUES="$(mktemp)"
BAD="$(mktemp)"
trap 'rm -f "$VALUES" "$BAD"' EXIT

cat >"$VALUES" <<'YAML'
worker:
  connectorReconciler:
    enabled: true
connectorServiceAccounts:
  - agent: acme-bot
    connector: storage
    serviceAccountName: acme-bot-storage
  - agent: acme-dev
    connector: storage
    serviceAccountName: acme.dev-storage
YAML

# -- (a) ----------------------------------------------------------------------
DEFAULT="$(render "$CHART")"
HOLDERS="$(holders <<<"$DEFAULT")"
if [[ "$HOLDERS" != "api []" ]]; then
    fail "a stock install handed CURIE_CONNECTOR_SERVICE_ACCOUNTS to: ${HOLDERS:-nothing}"
else
    echo "ok: a stock install hands the API an empty list and nothing else the setting"
fi

# -- (b) ----------------------------------------------------------------------
MAPPED="$(render "$CHART" -f "$VALUES")"
HOLDERS="$(holders <<<"$MAPPED")"
COMPONENT="${HOLDERS%% *}"
VALUE="${HOLDERS#* }"
EXPECTED='[{"agent":"acme-bot","connector":"storage","serviceAccountName":"acme-bot-storage"},{"agent":"acme-dev","connector":"storage","serviceAccountName":"acme.dev-storage"}]'
if [[ "$COMPONENT" != "api" || "$(wc -l <<<"$HOLDERS" | tr -d ' ')" != "1" ]]; then
    fail "a supplied list reached: ${HOLDERS:-nothing}"
elif ! python3 -c 'import json, sys; sys.exit(json.loads(sys.argv[1]) != json.loads(sys.argv[2]))' \
    "$VALUE" "$EXPECTED"; then
    fail "the API received $VALUE, not $EXPECTED"
else
    echo "ok: a supplied list reaches the API alone, unchanged"
fi

# -- (c) ----------------------------------------------------------------------
refused() {
    local label="$1" entry="$2"
    printf 'connectorServiceAccounts:\n%s\n' "$entry" >"$BAD"
    if render "$CHART" -f "$BAD" >/dev/null 2>&1; then
        fail "the chart rendered an entry with $label"
    else
        echo "ok: the chart refuses an entry with $label"
    fi
}
refused "an uppercase ServiceAccount name" \
    '  - {agent: acme-bot, connector: storage, serviceAccountName: Acme-Storage}'
refused "an underscore in the ServiceAccount name" \
    '  - {agent: acme-bot, connector: storage, serviceAccountName: acme_storage}'
refused "no serviceAccountName" \
    '  - {agent: acme-bot, connector: storage}'
refused "an unknown key" \
    '  - {agent: acme-bot, connector: storage, serviceAccountName: acme-storage, namespace: acme}'
refused "an empty agent" \
    '  - {agent: "", connector: storage, serviceAccountName: acme-storage}'

# -- (d) ----------------------------------------------------------------------
WITHOUT="$(worker_role <<<"$(render "$CHART" --set worker.connectorReconciler.enabled=true)")"
WITH="$(worker_role <<<"$MAPPED")"
if [[ -z "$WITH" ]]; then
    fail "no worker Role rendered with the reconciler on"
elif [[ "$WITH" != "$WITHOUT" ]]; then
    fail "naming a connector ServiceAccount changed the worker Role"
elif grep -q 'serviceaccounts' <<<"$WITH"; then
    fail "the worker Role names serviceaccounts"
else
    echo "ok: naming a connector ServiceAccount grants the worker nothing"
fi

exit "$FAILED"
