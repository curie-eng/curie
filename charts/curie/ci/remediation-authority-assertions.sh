#!/usr/bin/env bash
# Render assertions for the remediation authority key (ADR 0231, #4231).
#
# Binding and arming a remediation policy need CURIE_REMEDIATION_AUTHORITY_KEY,
# a credential the platform API key cannot derive. It is only independent if no
# platform-key holder receives it, so this proves with `helm template` that:
#   (a) the dev render (values-dev.yaml) and the sealed render both carry a
#       nonblank remediationAuthorityKey in curie-secrets, distinct from apiKey,
#       and the sealed render generates it rather than shipping the dev default;
#   (b) an explicit api.remediationAuthorityKey is honored verbatim and never
#       inlined anywhere but the Secret;
#   (c) the env var reaches the API Deployment's api container through a
#       secretKeyRef, and no other workload (any pod spec in any document,
#       containers, initContainers or ephemeralContainers, CRD templates and
#       ConfigMap-embedded templates included) names the variable or the secret
#       key, and no pod spec mounts curie-secrets whole by envFrom or a volume;
#   (d) a value equal to api.apiKey refuses to render without printing it;
#   (e) an explicit blank or whitespace value refuses to render.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "ASSERTION FAILED: $1" >&2; exit 1; }

SLACK=(--set dispatcher.slack.appToken=xapp-assert --set dispatcher.slack.botToken=xoxb-assert)
EXPLICIT="assert-remediation-authority-0231-value"

helm template curie "$CHART" -f "$CHART/values-dev.yaml" "${SLACK[@]}" \
  >"$TMP/dev.yaml" || fail "dev render failed"
helm template curie "$CHART" "${SLACK[@]}" \
  >"$TMP/sealed.yaml" || fail "sealed render failed"
helm template curie "$CHART" -f "$CHART/values-dev.yaml" "${SLACK[@]}" \
  --set "api.remediationAuthorityKey=$EXPLICIT" \
  >"$TMP/dev-explicit.yaml" || fail "dev render with an explicit key failed"
helm template curie "$CHART" "${SLACK[@]}" \
  --set "api.remediationAuthorityKey=$EXPLICIT" \
  >"$TMP/sealed-explicit.yaml" || fail "sealed render with an explicit key failed"

python3 - "$TMP" "$EXPLICIT" <<'PY'
import pathlib
import sys

import yaml

TMP = pathlib.Path(sys.argv[1])
EXPLICIT = sys.argv[2]
ENV = "CURIE_REMEDIATION_AUTHORITY_KEY"
KEY = "remediationAuthorityKey"
SECRET = "curie-secrets"
API = "curie-api"
API_CONTAINER = "api"
DEV_DEFAULT = "curie-dev-remediation-authority"


def fail(message):
    raise SystemExit(f"ASSERTION FAILED: {message}")


def load(name):
    with (TMP / name).open() as stream:
        return [doc for doc in yaml.safe_load_all(stream) if doc]


def ident(doc):
    return f"{doc.get('kind')}/{doc.get('metadata', {}).get('name')}"


def secret_data(docs, render):
    matches = [
        doc for doc in docs
        if doc.get("kind") == "Secret" and doc.get("metadata", {}).get("name") == SECRET
    ]
    if len(matches) != 1:
        fail(f"{render}: expected exactly one {SECRET} Secret, found {len(matches)}")
    return matches[0].get("stringData") or {}


def pod_specs(node, path=""):
    """Every pod spec anywhere in a document: any mapping with a containers list."""
    if isinstance(node, dict):
        if isinstance(node.get("containers"), list):
            yield path, node
        for key, value in node.items():
            yield from pod_specs(value, f"{path}/{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from pod_specs(value, f"{path}[{index}]")


def all_containers(spec):
    for field in ("containers", "initContainers", "ephemeralContainers"):
        for container in spec.get(field) or []:
            yield field, container


def mounts_secret_whole(spec):
    """envFrom or a volume (plain or projected) that would expose curie-secrets."""
    found = []
    for field, container in all_containers(spec):
        for source in container.get("envFrom") or []:
            if (source.get("secretRef") or {}).get("name") == SECRET:
                found.append(f"{field}:{container.get('name')} envFrom")
    for volume in spec.get("volumes") or []:
        if (volume.get("secret") or {}).get("secretName") == SECRET:
            found.append(f"volume {volume.get('name')}")
        for source in (volume.get("projected") or {}).get("sources") or []:
            if (source.get("secret") or {}).get("name") == SECRET:
                found.append(f"projected volume {volume.get('name')}")
    return found


def check_render(render, docs, *, expect_value=None):
    data = secret_data(docs, render)
    value = data.get(KEY)
    if value is None or not str(value).strip():
        fail(f"{render}: {SECRET} has no nonblank {KEY}")
    if value == data.get("apiKey"):
        fail(f"{render}: {KEY} equals apiKey")
    if expect_value is not None and value != expect_value:
        fail(f"{render}: an explicit api.remediationAuthorityKey was not honored verbatim")

    # (c) Exactly the API container receives the variable, by secretKeyRef.
    expected = {"name": ENV, "valueFrom": {"secretKeyRef": {"name": SECRET, "key": KEY}}}
    api_docs = [d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == API]
    if len(api_docs) != 1:
        fail(f"{render}: expected one {API} Deployment, found {len(api_docs)}")
    api_spec = api_docs[0]["spec"]["template"]["spec"]
    receivers = []
    for field, container in all_containers(api_spec):
        for entry in container.get("env") or []:
            ref = ((entry.get("valueFrom") or {}).get("secretKeyRef") or {})
            if entry.get("name") == ENV or ref.get("key") == KEY:
                receivers.append((field, container.get("name"), entry))
    if [(f, n) for f, n, _ in receivers] != [("containers", API_CONTAINER)]:
        fail(f"{render}: {ENV} must reach only the {API} '{API_CONTAINER}' container, got "
             f"{[(f, n) for f, n, _ in receivers]}")
    if receivers[0][2] != expected:
        fail(f"{render}: {API} {ENV} wiring is not a secretKeyRef to {SECRET}/{KEY}: "
             f"{ {k: v for k, v in receivers[0][2].items() if k != 'value'} }")

    # No pod spec anywhere, the API included, takes curie-secrets whole.
    for doc in docs:
        for path, spec in pod_specs(doc):
            whole = mounts_secret_whole(spec)
            if whole:
                fail(f"{render}: {ident(doc)}{path} mounts {SECRET} whole ({whole}); that "
                     f"would hand {KEY} to every container in it")

    # No other document names the variable or the key, structurally or as text
    # (a pod template carried inside a ConfigMap or CRD string counts).
    for doc in docs:
        if ident(doc) in (f"Secret/{SECRET}", f"Deployment/{API}"):
            continue
        for path, spec in pod_specs(doc):
            for field, container in all_containers(spec):
                for entry in container.get("env") or []:
                    ref = ((entry.get("valueFrom") or {}).get("secretKeyRef") or {})
                    if entry.get("name") == ENV or ref.get("key") == KEY:
                        fail(f"{render}: {ident(doc)}{path} {field}:{container.get('name')} "
                             f"receives the remediation authority")
        text = yaml.safe_dump(doc)
        if ENV in text or KEY in text:
            fail(f"{render}: {ident(doc)} names {ENV} or {KEY}; only {API} may receive it")
        if value in text:
            fail(f"{render}: {ident(doc)} carries the remediation authority value inline")

    # The API Deployment references the key; it never inlines the value.
    if value in yaml.safe_dump(api_docs[0]):
        fail(f"{render}: {API} inlines the remediation authority value")
    return value


dev = load("dev.yaml")
sealed = load("sealed.yaml")
if not [d for d in sealed if d.get("kind") == "SandboxTemplate"]:
    fail("sealed render produced no SandboxTemplate; the runner isolation check is vacuous")

check_render("dev", dev)
generated = check_render("sealed", sealed)
if generated == DEV_DEFAULT:
    fail("sealed render shipped the published dev default remediation authority")
if len(generated) < 32:
    fail("sealed render's generated remediation authority is shorter than 32 characters")

check_render("dev-explicit", load("dev-explicit.yaml"), expect_value=EXPLICIT)
check_render("sealed-explicit", load("sealed-explicit.yaml"), expect_value=EXPLICIT)
PY

# A second sealed render generates a fresh value: generation is not a constant.
helm template curie "$CHART" "${SLACK[@]}" >"$TMP/sealed-2.yaml" || fail "second sealed render failed"
python3 - "$TMP/sealed.yaml" "$TMP/sealed-2.yaml" <<'PY'
import sys

import yaml


def value(path):
    with open(path) as stream:
        for doc in yaml.safe_load_all(stream):
            if doc and doc.get("kind") == "Secret" and doc["metadata"]["name"] == "curie-secrets":
                return doc["stringData"]["remediationAuthorityKey"]
    raise SystemExit("ASSERTION FAILED: no curie-secrets Secret")


if value(sys.argv[1]) == value(sys.argv[2]):
    raise SystemExit("ASSERTION FAILED: two sealed renders produced the same remediation authority")
PY

# (d) The authority may never collapse onto the platform key, even explicitly.
# The refusal names only values paths, never the credential.
expect_refused() {
  local label="$1" leak="$2"
  shift 2
  if helm template curie "$CHART" "${SLACK[@]}" "$@" >"$TMP/refused.out" 2>"$TMP/refused.err"; then
    fail "$label rendered successfully"
  fi
  grep -q "remediationAuthorityKey" "$TMP/refused.err" \
    || fail "$label refusal does not name api.remediationAuthorityKey"
  if [ -n "$leak" ] && grep -qF -- "$leak" "$TMP/refused.out" "$TMP/refused.err"; then
    fail "$label refusal leaked the credential value"
  fi
}

SHARED="shared-remediation-assertion-value"
expect_refused "equal API and remediation authority keys" "$SHARED" \
  --set "api.apiKey=$SHARED" --set "api.remediationAuthorityKey=$SHARED"
expect_refused "equal API and remediation authority keys (dev values)" "$SHARED" \
  -f "$CHART/values-dev.yaml" --set "api.apiKey=$SHARED" --set "api.remediationAuthorityKey=$SHARED"
expect_refused "a remediation authority equal to the dev API key" "curie-dev-key" \
  -f "$CHART/values-dev.yaml" --set "api.remediationAuthorityKey=curie-dev-key"

# (e) An explicit blank or whitespace value is a misconfiguration, not "unset".
for blank in "" "   "; do
  expect_refused "a blank remediation authority (sealed, '$blank')" "" \
    --set-string "api.remediationAuthorityKey=$blank"
  expect_refused "a blank remediation authority (dev values, '$blank')" "" \
    -f "$CHART/values-dev.yaml" --set-string "api.remediationAuthorityKey=$blank"
done

echo "OK: remediation authority render assertions passed"
