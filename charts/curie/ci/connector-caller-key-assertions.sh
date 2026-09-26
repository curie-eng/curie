#!/usr/bin/env bash
#
# Render assertions for the connector caller key pair (ADR-0168 decision 7).
#
# The worker signs each sandbox's caller token with a key from
# connectorCaller.existingSecret. Four properties:
#
#   (a) A stock install renders no caller key anywhere.
#   (b) The key names alone change nothing: until an operator names a Secret,
#       the render is the stock render.
#   (c) With the Secret named, the signing key reaches exactly one workload,
#       the worker, by secretKeyRef to that Secret and key. A sandbox holding
#       it could mint a token naming any agent.
#   (d) The named Secret is referenced exactly once in the whole render: the
#       worker's signing-key env entry from (c). A second reference would mean
#       another workload, an envFrom, a volume, or the public key reached
#       something -- nothing verifies a token in this release, and a reference
#       to a key nobody reads is one more thing to keep in step.
#
# Pass a second chart directory to also prove the stock render matches it,
# ignoring the per-render random secrets:
#
#   bash charts/curie/ci/connector-caller-key-assertions.sh <baseline-chart-dir>
set -euo pipefail

# NOTE: read variables with a herestring, never `printf ... | cmd`; see
# sealing-key-assertions.sh for the pipefail trap this avoids.

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE="${1:-}"
FAILED=0

fail() {
    echo "FAIL: $1" >&2
    FAILED=1
}

render() {
    helm template t "$1" -n t "${@:2}"
}

# Every render mints fresh random Secret values and an installation id, so two
# renders are compared with those taken out.
normalized() {
    python3 -c '
import json, sys, yaml

def scrub(node):
    if isinstance(node, dict):
        if node.get("name") == "CURIE_INSTALLATION_ID" and "value" in node:
            node = {**node, "value": ""}
        return {key: scrub(value) for key, value in node.items()}
    if isinstance(node, list):
        return [scrub(item) for item in node]
    return node

docs = [doc for doc in yaml.safe_load_all(sys.stdin) if doc and doc.get("kind") != "Secret"]
print(json.dumps([scrub(doc) for doc in docs], sort_keys=True))
'
}

DEFAULT="$(render "$CHART")"

# -- (a) ----------------------------------------------------------------------
if grep -q 'CURIE_CONNECTOR_CALLER_SIGNING_KEY' <<<"$DEFAULT"; then
    fail "a stock install rendered the connector caller signing key"
else
    echo "ok: a stock install renders no caller key"
fi

# -- (b) ----------------------------------------------------------------------
RENAMED="$(render "$CHART" --set connectorCaller.signingKeyKey=other \
    --set connectorCaller.verifyKeyKey=other-public)"
if [[ "$(normalized <<<"$RENAMED")" != "$(normalized <<<"$DEFAULT")" ]]; then
    fail "the caller key names changed the render with no Secret named"
else
    echo "ok: the key names alone change nothing"
fi

# -- (c) ----------------------------------------------------------------------
SUPPLIED="$(render "$CHART" --set connectorCaller.existingSecret=acme-connector-caller)"
if ! python3 -c '
import sys, yaml

holders = []
for doc in yaml.safe_load_all(sys.stdin):
    if not doc:
        continue
    def walk(node):
        if isinstance(node, dict):
            for entry in node.get("env") or []:
                if isinstance(entry, dict) and entry.get("name") == "CURIE_CONNECTOR_CALLER_SIGNING_KEY":
                    holders.append((doc, node.get("name"), entry))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(doc)

assert len(holders) == 1, f"expected one holder of the signing key, found {len(holders)}"
doc, container, entry = holders[0]
labels = doc["metadata"].get("labels") or {}
assert labels.get("app.kubernetes.io/component") == "worker", labels
assert container == "worker", container
assert entry["valueFrom"] == {
    "secretKeyRef": {"name": "acme-connector-caller", "key": "signingKey"}
}, entry
' <<<"$SUPPLIED"; then
    fail "the signing key did not reach exactly the worker, by reference"
else
    echo "ok: the worker alone receives the signing key, by reference"
fi

# -- (d) ----------------------------------------------------------------------
NAME_COUNT="$(grep -o 'acme-connector-caller' <<<"$SUPPLIED" | wc -l | tr -d ' ')"
if [[ "$NAME_COUNT" != "1" ]]; then
    fail "the Secret name acme-connector-caller appeared $NAME_COUNT times in the render, not exactly once"
else
    echo "ok: the Secret name appears exactly once in the render"
fi

# -- baseline -----------------------------------------------------------------
if [[ -n "$BASELINE" ]]; then
    if [[ "$(normalized <<<"$(render "$BASELINE")")" != "$(normalized <<<"$DEFAULT")" ]]; then
        fail "the stock render differs from the baseline chart at $BASELINE"
    else
        echo "ok: the stock render matches the baseline chart"
    fi
fi

exit "$FAILED"
