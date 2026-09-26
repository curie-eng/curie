#!/usr/bin/env bash
#
# Render assertions for the connector caller key pair (ADR-0168 decision 7).
#
# The worker signs each sandbox's caller token, and the API renders the public
# half into every hosted connector's caller proxy. Five properties:
#
#   (a) A stock install references the signing key from the worker alone and
#       the public key from the API alone, both from the release Secret, which
#       holds them empty. Empty mints no token and renders no proxy.
#   (b) Values supplied inline land in the release Secret and nowhere else: the
#       signing value appears exactly once in the whole render.
#   (c) With connectorCaller.existingSecret named, the worker reads the signing
#       key and the API reads the public key from that Secret, and the name
#       appears exactly twice in the render. A sandbox, the dispatcher or the
#       API holding the signing key could mint a token naming any agent.
#   (d) The previous public key reaches the API alone, as a plain value.
#   (e) The proxy image is the worker's own image, digest pin included.
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

# Prints "<component> <container> <secret name or -> <key or value>" for every
# env entry named $1, one line each.
holders() {
    python3 -c '
import sys, yaml

wanted = sys.argv[1]
for doc in yaml.safe_load_all(sys.stdin):
    if not doc:
        continue
    component = (doc.get("metadata", {}).get("labels") or {}).get("app.kubernetes.io/component", "-")
    def walk(node):
        if isinstance(node, dict):
            for entry in node.get("env") or []:
                if isinstance(entry, dict) and entry.get("name") == wanted:
                    ref = (entry.get("valueFrom") or {}).get("secretKeyRef")
                    if ref:
                        print(component, node.get("name"), ref["name"], ref["key"])
                    else:
                        print(component, node.get("name"), "-", entry.get("value", ""))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(doc)
' "$1"
}

secret_value() {
    python3 -c '
import base64, sys, yaml
for doc in yaml.safe_load_all(sys.stdin):
    if doc and doc.get("kind") == "Secret" and doc["metadata"]["name"] == "t-curie-secrets":
        data = doc.get("stringData") or {}
        if sys.argv[1] in data:
            print(data[sys.argv[1]])
        else:
            print(base64.b64decode((doc.get("data") or {})[sys.argv[1]]).decode())
' "$1"
}

DEFAULT="$(render "$CHART")"

# -- (a) ----------------------------------------------------------------------
SIGNING="$(holders CURIE_CONNECTOR_CALLER_SIGNING_KEY <<<"$DEFAULT")"
PUBLIC="$(holders CURIE_CONNECTOR_CALLER_PUBLIC_KEY <<<"$DEFAULT")"
if [[ "$SIGNING" != "worker worker t-curie-secrets connectorCallerSigningKey" ]]; then
    fail "a stock install gave the signing key to: ${SIGNING:-nothing}"
elif [[ "$PUBLIC" != "api api t-curie-secrets connectorCallerVerifyKey" ]]; then
    fail "a stock install gave the public key to: ${PUBLIC:-nothing}"
elif [[ -n "$(secret_value connectorCallerSigningKey <<<"$DEFAULT")$(secret_value connectorCallerVerifyKey <<<"$DEFAULT")" ]]; then
    fail "a stock install rendered a caller key value into the release Secret"
else
    echo "ok: a stock install references the signing key from the worker and the public key from the API, both empty"
fi

# -- (b) ----------------------------------------------------------------------
INLINE="$(render "$CHART" --set connectorCaller.signingKey=SIGNING-SENTINEL \
    --set connectorCaller.verifyKey=VERIFY-SENTINEL)"
if [[ "$(grep -o 'SIGNING-SENTINEL' <<<"$INLINE" | wc -l | tr -d ' ')" != "1" ]]; then
    fail "the inline signing key appeared outside the release Secret"
elif [[ "$(secret_value connectorCallerSigningKey <<<"$INLINE")" != "SIGNING-SENTINEL" ]]; then
    fail "the inline signing key did not reach the release Secret"
elif [[ "$(secret_value connectorCallerVerifyKey <<<"$INLINE")" != "VERIFY-SENTINEL" ]]; then
    fail "the inline public key did not reach the release Secret"
else
    echo "ok: inline caller keys land in the release Secret and nowhere else"
fi

# -- (c) ----------------------------------------------------------------------
SUPPLIED="$(render "$CHART" --set connectorCaller.existingSecret=acme-connector-caller)"
SIGNING="$(holders CURIE_CONNECTOR_CALLER_SIGNING_KEY <<<"$SUPPLIED")"
PUBLIC="$(holders CURIE_CONNECTOR_CALLER_PUBLIC_KEY <<<"$SUPPLIED")"
NAME_COUNT="$(grep -o 'acme-connector-caller' <<<"$SUPPLIED" | wc -l | tr -d ' ')"
if [[ "$SIGNING" != "worker worker acme-connector-caller signingKey" ]]; then
    fail "with a named Secret the signing key went to: ${SIGNING:-nothing}"
elif [[ "$PUBLIC" != "api api acme-connector-caller verifyKey" ]]; then
    fail "with a named Secret the public key went to: ${PUBLIC:-nothing}"
elif [[ "$NAME_COUNT" != "2" ]]; then
    fail "the Secret name acme-connector-caller appeared $NAME_COUNT times in the render, not exactly twice"
else
    echo "ok: a named Secret feeds the worker's signing key and the API's public key, and nothing else"
fi

# -- (d) ----------------------------------------------------------------------
PREVIOUS="$(holders CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY <<<"$(render "$CHART" \
    --set connectorCaller.previousVerifyKey=PREVIOUS-SENTINEL)")"
if [[ "$PREVIOUS" != "api api - PREVIOUS-SENTINEL" ]]; then
    fail "the previous public key went to: ${PREVIOUS:-nothing}"
else
    echo "ok: the previous public key reaches the API alone"
fi

# -- (e) ----------------------------------------------------------------------
image_pair() {
    python3 -c '
import sys, yaml
proxy = worker = None
for doc in yaml.safe_load_all(sys.stdin):
    if not doc or doc.get("kind") != "Deployment":
        continue
    component = doc["metadata"].get("labels", {}).get("app.kubernetes.io/component")
    for container in doc["spec"]["template"]["spec"]["containers"]:
        if component == "worker" and container["name"] == "worker":
            worker = container["image"]
        for entry in container.get("env") or []:
            if entry.get("name") == "CURIE_CONNECTOR_PROXY_IMAGE":
                proxy = entry["value"]
print(proxy == worker and worker is not None, proxy, worker)
'
}
for pin in "" "--set worker.image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000"; do
    # shellcheck disable=SC2086
    read -r SAME PROXY WORKER <<<"$(image_pair <<<"$(render "$CHART" $pin)")"
    if [[ "$SAME" != "True" ]]; then
        fail "the proxy image $PROXY is not the worker image $WORKER (${pin:-default})"
    else
        echo "ok: the proxy image is the worker image (${pin:-default})"
    fi
done

exit "$FAILED"
