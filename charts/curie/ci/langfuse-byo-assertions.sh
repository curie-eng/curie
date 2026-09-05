#!/usr/bin/env bash
set -euo pipefail

# Issue #1505: langfuse.deploy selects one shared endpoint for the API, worker,
# and OTel Collector. A chart-owned install must ignore langfuse.host; BYO mode
# must require it, route every consumer to it, and leave no dead in-chart
# Langfuse resources or Service hostname behind.

# Issue #2314: that shared endpoint had its `http://` scheme hardcoded at every
# consumer, so a BYO Langfuse behind TLS was unreachable. The scheme now comes
# from the `curie.langfuse.url` helper -- explicit `langfuse.scheme` wins,
# otherwise it derives (https on port 443, http elsewhere), and an invalid
# value fails the render rather than silently emitting cleartext.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"

cleanup() {
  [[ -n "${TMP:-}" && -d "$TMP" ]] && rm -rf -- "$TMP"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() {
  local chart="$1"
  local name="$2"
  shift 2
  helm template curie "$chart" --output-dir "$TMP/$name" "$@" >/dev/null
}

CHECKER="$TMP/check-render.py"
cat > "$CHECKER" <<'PY'
import pathlib
import sys

import yaml


def die(message):
    raise SystemExit(message)


def load_documents(root):
    root = pathlib.Path(root)
    loaded = []
    for path in sorted(root.rglob("*.yaml")):
        for index, document in enumerate(yaml.safe_load_all(path.read_text()), start=1):
            if isinstance(document, dict):
                loaded.append((path, index, document))
    if not loaded:
        die(f"{root}: Helm wrote no YAML documents")
    return root, loaded


def one_resource(documents, kind, name):
    matches = [
        document
        for _, _, document in documents
        if document.get("kind") == kind
        and document.get("metadata", {}).get("name") == name
    ]
    if len(matches) != 1:
        die(f"expected exactly one {kind}/{name}, found {len(matches)}")
    return matches[0]


def env_value(deployment, variable):
    entries = [
        entry
        for container in deployment.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
        for entry in container.get("env", [])
        if entry.get("name") == variable
    ]
    resource = deployment.get("metadata", {}).get("name")
    if len(entries) != 1:
        die(
            f"Deployment/{resource}: expected exactly one {variable} entry, "
            f"found {len(entries)}"
        )
    if set(entries[0]) != {"name", "value"}:
        die(
            f"Deployment/{resource}: {variable} must be one literal value, "
            f"got {entries[0]!r}"
        )
    return entries[0]["value"]


def assert_endpoints(documents, expected_base):
    for deployment_name in ("curie-api", "curie-worker"):
        deployment = one_resource(documents, "Deployment", deployment_name)
        actual = env_value(deployment, "LANGFUSE_HOST")
        if actual != expected_base:
            die(
                f"Deployment/{deployment_name}: LANGFUSE_HOST is {actual!r}, "
                f"expected {expected_base!r}"
            )

    config_map = one_resource(documents, "ConfigMap", "curie-otel-collector")
    raw_config = config_map.get("data", {}).get("collector-config.yaml")
    if not isinstance(raw_config, str) or not raw_config.strip():
        die("ConfigMap/curie-otel-collector: collector-config.yaml is missing")
    config = yaml.safe_load(raw_config)
    try:
        actual_endpoint = config["exporters"]["otlphttp/langfuse"]["endpoint"]
    except (KeyError, TypeError) as exc:
        die(
            "ConfigMap/curie-otel-collector: exporters.otlphttp/langfuse.endpoint "
            f"is missing ({exc})"
        )
    expected_endpoint = f"{expected_base}/api/public/otel"
    if actual_endpoint != expected_endpoint:
        die(
            "ConfigMap/curie-otel-collector: Langfuse endpoint is "
            f"{actual_endpoint!r}, expected {expected_endpoint!r}"
        )


def scalar_residue(value, needle, path="$"):
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            found.extend(scalar_residue(child, needle, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(scalar_residue(child, needle, f"{path}[{index}]"))
    elif isinstance(value, str) and needle in value:
        found.append(path)
    return found


command, root_arg, expected_base = sys.argv[1:4]
root, documents = load_documents(root_arg)

if command == "internal":
    assert_endpoints(documents, expected_base)
    one_resource(documents, "Service", "curie-langfuse-web")
    one_resource(documents, "Deployment", "curie-langfuse-web")
    one_resource(documents, "Deployment", "curie-langfuse-worker")
elif command == "external":
    assert_endpoints(documents, expected_base)
    forbidden = {
        ("Service", "curie-langfuse-web"),
        ("Deployment", "curie-langfuse-web"),
        ("Deployment", "curie-langfuse-worker"),
        ("Job", "curie-langfuse-model-pricing"),
    }
    present = sorted(
        (document.get("kind"), document.get("metadata", {}).get("name"))
        for _, _, document in documents
        if (document.get("kind"), document.get("metadata", {}).get("name"))
        in forbidden
    )
    if present:
        die(f"BYO render still contains chart-owned Langfuse resources: {present!r}")

    forbidden_files = {
        "langfuse.yaml",
        "langfuse-model-pricing.yaml",
    }
    rendered_files = sorted(
        str(path.relative_to(root))
        for path in root.rglob("*.yaml")
        if path.name in forbidden_files
    )
    if rendered_files:
        die(f"BYO render wrote gated Langfuse template files: {rendered_files!r}")

    residue = []
    for path, index, document in documents:
        for value_path in scalar_residue(document, "curie-langfuse-web"):
            residue.append(f"{path.relative_to(root)} document {index} {value_path}")
    if residue:
        die(
            "BYO render retains the internal Service hostname curie-langfuse-web "
            f"at {residue!r}"
        )
elif command == "harness":
    api = one_resource(documents, "Deployment", "curie-api")
    actual = env_value(api, "LANGFUSE_HOST")
    if actual != expected_base:
        die(
            f"Deployment/curie-api: LANGFUSE_HOST is {actual!r}, "
            f"expected {expected_base!r}"
        )
    langfuse_services = [
        document.get("metadata", {}).get("name")
        for _, _, document in documents
        if document.get("kind") == "Service"
        and document.get("metadata", {}).get("name") == "curie-langfuse-web"
    ]
    if langfuse_services:
        die(
            "trimmed harness render still contains the chart-owned Langfuse "
            f"Service: {langfuse_services!r}"
        )
else:
    die(f"unknown checker command {command!r}")
PY

echo "=== BYO Langfuse assertion 1: chart-owned mode ignores an external host override ==="
render "$CHART" internal --set-string langfuse.host=ignored.example.com
python3 "$CHECKER" internal "$TMP/internal" "http://curie-langfuse-web:3000"
echo "  ok: API, worker, and collector use the chart-owned Langfuse Service"

echo "=== BYO Langfuse assertion 2: an external TLS endpoint on 443 reaches every consumer over https ==="
render "$CHART" external \
  --set langfuse.deploy=false \
  --set-string langfuse.host=langfuse.example.com \
  --set langfuse.web.service.port=443
python3 "$CHECKER" external "$TMP/external" "https://langfuse.example.com:443"
echo "  ok: the derived https endpoint is shared and no chart-owned Langfuse residue remains"

echo "=== BYO Langfuse assertion 2b: an explicit langfuse.scheme=https wins on a non-443 port ==="
render "$CHART" external-explicit \
  --set langfuse.deploy=false \
  --set-string langfuse.host=langfuse.example.com \
  --set-string langfuse.scheme=https \
  --set langfuse.web.service.port=4319
python3 "$CHECKER" external "$TMP/external-explicit" "https://langfuse.example.com:4319"
echo "  ok: TLS on an arbitrary port is reachable without touching the port"

echo "=== BYO Langfuse assertion 2c: a cleartext BYO endpoint is unchanged ==="
render "$CHART" external-cleartext \
  --set langfuse.deploy=false \
  --set-string langfuse.host=langfuse.example.com \
  --set langfuse.web.service.port=4319
python3 "$CHECKER" external "$TMP/external-cleartext" "http://langfuse.example.com:4319"
echo "  ok: a non-443 BYO port still derives http, so existing installs do not move"

expect_scheme_failure() {
  local name="$1"
  shift
  local stderr="$TMP/$name.stderr"
  if helm template curie "$CHART" --output-dir "$TMP/$name" "$@" \
    >/dev/null 2>"$stderr"; then
    fail "$name: an invalid langfuse.scheme rendered instead of failing closed"
  fi
  if ! grep -qF "langfuse.scheme" "$stderr"; then
    fail "$name: render failed without naming langfuse.scheme: $(<"$stderr")"
  fi
}

echo "=== BYO Langfuse assertion 2d: an unsupported langfuse.scheme fails the render ==="
expect_scheme_failure bad-scheme \
  --set langfuse.deploy=false \
  --set-string langfuse.host=langfuse.example.com \
  --set-string langfuse.scheme=ftp
echo "  ok: langfuse.scheme=ftp is rejected by name instead of falling back to cleartext"

echo "=== BYO Langfuse assertion 3: trimmed runtime harness supplies its own Langfuse host ==="
render "$CHART" harness \
  -f "$CHART/values-e2e-nogvisor.yaml" \
  -f "$CHART/values-e2e-harness.yaml" \
  --set api.deploy=true \
  --set api.replicas=0 \
  --set api.service.type=NodePort \
  --set api.service.nodePort=30181 \
  --set-string postgres.host=postgres.example.com \
  --set-string valkey.host=valkey.example.com
python3 "$CHECKER" harness "$TMP/harness" "http://langfuse.example.com:3000"
echo "  ok: the API uses the harness Langfuse endpoint without a chart-owned Service"

expect_host_failure() {
  local name="$1"
  shift
  local stderr="$TMP/$name.stderr"
  if helm template curie "$CHART" --output-dir "$TMP/$name" "$@" \
    >/dev/null 2>"$stderr"; then
    fail "$name: langfuse.deploy=false rendered without a non-empty langfuse.host"
  fi
  if ! grep -qF "langfuse.host" "$stderr"; then
    fail "$name: render failed without naming langfuse.host: $(<"$stderr")"
  fi
}

echo "=== BYO Langfuse assertion 4: missing and empty external hosts fail closed ==="
expect_host_failure missing-host --set langfuse.deploy=false
expect_host_failure empty-host \
  --set langfuse.deploy=false \
  --set-string 'langfuse.host='
echo "  ok: both rejected renders name langfuse.host"

echo "=== BYO Langfuse assertion 5: reverting the shared host helper is detected ==="
MUTANT="$TMP/mutant-chart"
cp -a "$CHART" "$MUTANT"
python3 - "$MUTANT/templates/_helpers.tpl" <<'PY'
import pathlib
import re
import sys


path = pathlib.Path(sys.argv[1])
text = path.read_text()
tokens = list(re.finditer(r"{{-?\s*(.*?)\s*-?}}", text, re.DOTALL))
start = None
end = None
depth = 0

for token in tokens:
    directive = token.group(1).strip()
    if start is None:
        if re.fullmatch(r'define\s+"curie\.langfuse\.webHost"', directive):
            start = token.start()
            depth = 1
        continue

    keyword = directive.split(None, 1)[0] if directive else ""
    if keyword in {"block", "define", "if", "range", "with"}:
        depth += 1
    elif keyword == "end":
        depth -= 1
        if depth == 0:
            end = token.end()
            break

if start is None or end is None:
    sys.exit(
        "negative control could not locate the complete "
        "curie.langfuse.webHost helper block"
    )

old_unconditional = """{{- define "curie.langfuse.webHost" -}}
{{- printf "%s-langfuse-web" (include "curie.fullname" .) -}}
{{- end -}}"""
current = text[start:end]
if current == old_unconditional:
    sys.exit(
        "negative control found the old unconditional curie.langfuse.webHost "
        "already present; there is no deploy-aware helper to revert"
    )

path.write_text(text[:start] + old_unconditional + text[end:])
PY

render "$MUTANT" mutant-external \
  --set langfuse.deploy=false \
  --set-string langfuse.host=langfuse.example.com \
  --set langfuse.web.service.port=4319

mutation_error=""
if mutation_error="$(
  python3 "$CHECKER" external "$TMP/mutant-external" \
    "http://langfuse.example.com:4319" 2>&1
)"; then
  fail "negative control did not fire: the old unconditional Service helper passed the BYO endpoint contract"
fi
if [[ "$mutation_error" != *"curie-langfuse-web"* ]]; then
  fail "negative control failed for an unexpected reason: $mutation_error"
fi
echo "  ok: the external-consumer checker rejects the reverted helper"

echo "BYO Langfuse host assertions passed"
