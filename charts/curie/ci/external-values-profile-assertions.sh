#!/usr/bin/env bash
#
# Render-assertion test for issue #2662.
#
# values-external.yaml is the checked-in canonical overlay for operator-managed
# PostgreSQL, Valkey, ClickHouse, and S3-compatible object storage. It must
# stay a non-secret values file that uses existing deploy / host / TLS /
# identity / existingSecret fields, disable every chart-owned backing store,
# and render consumers onto those external endpoints and Secret references.
#
# Asserts:
#
#   1. The overlay itself carries no password, secretKey, or token values.
#   2. helm lint and helm template succeed with -f values-external.yaml.
#   3. No chart-owned postgres / valkey / clickhouse / rustfs Service,
#      StatefulSet, or rustfs-init Job renders.
#   4. Per-consumer env names the overlay hosts, TLS settings, buckets, and
#      existingSecret references. The chart Secret must not back those store
#      credentials.
#   5. Rail 1 renders runner-allow-object-store for the overlay CIDR and does
#      not render runner-allow-rustfs.
#   6. NEGATIVE CONTROL: helm template --set postgres.deploy=false without a
#      host exits non-zero and names postgres.host. Dropping postgres.host
#      from the overlay fails the same way.
#
# These are render results only, not proof that external services are healthy.
#
# Every render goes through --output-dir, never a stdout pipe: piping helm
# template in this environment silently truncates a large render at exit 0
# with empty stderr. Structural checks go through PyYAML rather than grep.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
OVERLAY="$CHART/values-external.yaml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "=== 1: overlay contains no credential values ==="
python3 - "$OVERLAY" <<'PY' || fail "overlay credential scan failed"
import sys
from pathlib import Path

import yaml

path = Path(sys.argv[1])
text = path.read_text()
forbidden_substrings = (
    "password:",
    "secretKey:",
    "privateKey:",
    "apiKey:",
    "token:",
)
for needle in forbidden_substrings:
    if needle in text:
        raise SystemExit(f"{path}: contains {needle!r}; credentials stay in Secrets")

doc = yaml.safe_load(text)
if not isinstance(doc, dict):
    raise SystemExit(f"{path}: expected a mapping")

required_false = ("postgres", "valkey", "clickhouse", "rustfs")
for store in required_false:
    block = doc.get(store)
    if not isinstance(block, dict) or block.get("deploy") is not False:
        raise SystemExit(f"{path}: {store}.deploy must be false")
    if not block.get("host"):
        raise SystemExit(f"{path}: {store}.host must be set")
    if not block.get("existingSecret"):
        raise SystemExit(f"{path}: {store}.existingSecret must be set")

rustfs = doc["rustfs"]
if rustfs.get("auth", {}).get("secretKey"):
    raise SystemExit(f"{path}: rustfs.auth.secretKey must not be set")
egress = rustfs.get("egress") or []
if not egress:
    raise SystemExit(f"{path}: rustfs.egress must name a narrow CIDR")

print("  overlay has no credential values and names every store: OK")
PY

echo "=== 2: helm lint -f values-external.yaml ==="
helm lint "$CHART" -f "$OVERLAY" >/dev/null \
  || fail "helm lint -f values-external.yaml failed"
echo "  helm lint: OK"

echo "=== 2b: helm template -f values-external.yaml ==="
RENDER_DIR="$TMP/profile"
mkdir -p "$RENDER_DIR"
helm template acme "$CHART" --namespace acme -f "$OVERLAY" \
  --output-dir "$RENDER_DIR" >/dev/null \
  || fail "helm template -f values-external.yaml failed"
TEMPLATES="$RENDER_DIR/curie/templates"
echo "  helm template: OK"

echo "=== 6: negative missing host ==="
set +e
MISSING_HOST_ERR="$TMP/missing-host.err"
helm template acme "$CHART" --namespace acme --set postgres.deploy=false \
  >"$TMP/missing-host.out" 2>"$MISSING_HOST_ERR"
MISSING_RC=$?
set -e
if [[ "$MISSING_RC" -eq 0 ]]; then
  fail "postgres.deploy=false without host rendered successfully"
fi
if ! grep -q 'postgres.host' "$MISSING_HOST_ERR"; then
  fail "postgres.deploy=false without host did not name postgres.host; stderr: $(cat "$MISSING_HOST_ERR")"
fi
echo "  missing postgres.host is refused and names postgres.host: OK"

set +e
DROP_HOST_ERR="$TMP/drop-host.err"
helm template acme "$CHART" --namespace acme -f "$OVERLAY" --set postgres.host= \
  >"$TMP/drop-host.out" 2>"$DROP_HOST_ERR"
DROP_RC=$?
set -e
if [[ "$DROP_RC" -eq 0 ]]; then
  fail "values-external.yaml with postgres.host cleared rendered successfully"
fi
if ! grep -q 'postgres.host' "$DROP_HOST_ERR"; then
  fail "cleared postgres.host did not name postgres.host; stderr: $(cat "$DROP_HOST_ERR")"
fi
echo "  overlay with postgres.host cleared is refused: OK"

echo "=== 3-5: rendered endpoints, secrets, and disabled stores ==="
TEMPLATES="$TEMPLATES" python3 <<'PY' || fail "profile render assertions failed"
import os
import pathlib
import sys

import yaml

ROOT = pathlib.Path(os.environ["TEMPLATES"])
CHART_SECRET = "acme-curie-secrets"
PG_CREDS = "acme-postgres"
VALKEY_CREDS = "acme-valkey"
CH_CREDS = "acme-clickhouse"
S3_CREDS = "acme-s3"
PG_HOST = "postgres.example.com"
EXPECTED_VALKEY = "valkey.example.com"
CH_HOST = "clickhouse.example.com"
S3_ENDPOINT = "https://s3.example.com:443"
S3_CIDR = "192.0.2.10/32"

failures = []


def die_later(message):
    failures.append(message)


def load_all():
    docs = []
    if not ROOT.is_dir():
        die_later(f"{ROOT}: helm wrote no templates directory")
        return docs
    for path in sorted(ROOT.rglob("*")):
        if path.suffix not in {".yaml", ".yml"}:
            continue
        text = path.read_text()
        if not text.strip():
            continue
        for document in yaml.safe_load_all(text):
            if isinstance(document, dict):
                docs.append((path, document))
    if not docs:
        die_later(f"{ROOT}: Helm wrote no YAML documents")
    return docs


def walk_containers(obj, acc):
    if isinstance(obj, dict):
        for key in ("containers", "initContainers"):
            items = obj.get(key)
            if isinstance(items, list):
                acc.extend(items)
        for value in obj.values():
            walk_containers(value, acc)
    elif isinstance(obj, list):
        for item in obj:
            walk_containers(item, acc)


def env_entries(docs, container_name, env_name):
    matches = []
    for path, document in docs:
        containers = []
        walk_containers(document, containers)
        for container in containers:
            if not isinstance(container, dict) or container.get("name") != container_name:
                continue
            for entry in container.get("env") or []:
                if entry.get("name") == env_name:
                    matches.append((path, document, entry))
    return matches


def require_literal(docs, container, env_name, expected, aid):
    matches = env_entries(docs, container, env_name)
    if len(matches) != 1:
        die_later(
            f"[{aid}] {container}/{env_name}: expected exactly 1 entry, found {len(matches)}"
        )
        return
    entry = matches[0][2]
    if "valueFrom" in entry:
        die_later(f"[{aid}] {container}/{env_name}: expected a literal value, got valueFrom")
        return
    actual = entry.get("value")
    if actual != expected:
        die_later(
            f"[{aid}] {container}/{env_name}: value {actual!r}, expected {expected!r}"
        )


def require_contains(docs, container, env_name, needle, aid):
    matches = env_entries(docs, container, env_name)
    if len(matches) != 1:
        die_later(
            f"[{aid}] {container}/{env_name}: expected exactly 1 entry, found {len(matches)}"
        )
        return
    actual = matches[0][2].get("value") or ""
    if needle not in actual:
        die_later(
            f"[{aid}] {container}/{env_name}: {actual!r} does not contain {needle!r}"
        )


def require_secret(docs, container, env_name, secret_name, key, aid):
    matches = env_entries(docs, container, env_name)
    if len(matches) != 1:
        die_later(
            f"[{aid}] {container}/{env_name}: expected exactly 1 entry, found {len(matches)}"
        )
        return
    ref = (matches[0][2].get("valueFrom") or {}).get("secretKeyRef") or {}
    if ref.get("name") != secret_name:
        die_later(
            f"[{aid}] {container}/{env_name}: secretKeyRef.name {ref.get('name')!r}, "
            f"expected {secret_name!r}"
        )
    if ref.get("key") != key:
        die_later(
            f"[{aid}] {container}/{env_name}: secretKeyRef.key {ref.get('key')!r}, "
            f"expected {key!r}"
        )
    if ref.get("name") == CHART_SECRET:
        die_later(
            f"[{aid}] {container}/{env_name}: still references chart Secret {CHART_SECRET}"
        )


def named(docs, kind, name):
    return [
        document
        for _, document in docs
        if document.get("kind") == kind
        and document.get("metadata", {}).get("name") == name
    ]


docs = load_all()

owned = [
    ("Service", "acme-curie-postgres"),
    ("StatefulSet", "acme-curie-postgres"),
    ("Service", "acme-curie-valkey"),
    ("StatefulSet", "acme-curie-valkey"),
    ("Service", "acme-curie-clickhouse"),
    ("StatefulSet", "acme-curie-clickhouse"),
    ("Service", "acme-curie-rustfs"),
    ("StatefulSet", "acme-curie-rustfs"),
    ("Job", "acme-curie-rustfs-init"),
]
for kind, name in owned:
    found = named(docs, kind, name)
    if found:
        die_later(f"[3] chart-owned {kind}/{name} still rendered ({len(found)})")

for filename in ("postgres.yaml", "valkey.yaml", "clickhouse.yaml", "rustfs.yaml"):
    path = ROOT / filename
    if path.is_file() and path.stat().st_size > 0:
        die_later(f"[3] {filename} still has content under deploy=false")

# Postgres consumers: migrate init, api, worker, both Langfuse deployments.
for container in ("migrate", "api", "worker"):
    require_secret(docs, container, "POSTGRES_PASSWORD", PG_CREDS, "postgresPassword", "pg")
    require_contains(docs, container, "DATABASE_URL", PG_HOST, "pg")
    require_contains(docs, container, "DATABASE_URL", "?ssl=require", "pg-tls")
    require_literal(docs, container, "DB_SCHEMA", "curie", "pg-schema")
for container in ("langfuse-web", "langfuse-worker"):
    require_secret(docs, container, "POSTGRES_PASSWORD", PG_CREDS, "postgresPassword", "pg")
    require_contains(docs, container, "DATABASE_URL", PG_HOST, "pg")
    require_contains(
        docs, container, "DATABASE_URL", "sslmode=require", "pg-tls"
    )

# Valkey consumers: api, worker, drain jobs, both Langfuse deployments.
# Dispatcher is omitted: curie.dispatcher.enabled is false on a token-less
# default install, so that Deployment does not render.
for container in ("api", "worker"):
    require_literal(docs, container, "VALKEY_HOST", EXPECTED_VALKEY, "vk")
    require_secret(docs, container, "VALKEY_PASSWORD", VALKEY_CREDS, "valkeyPassword", "vk")
    require_literal(docs, container, "VALKEY_TLS", "true", "vk-tls")
for container in ("upgrade-drain", "upgrade-drain-release"):
    require_literal(docs, container, "VALKEY_HOST", EXPECTED_VALKEY, "vd")
    require_secret(docs, container, "VALKEY_PASSWORD", VALKEY_CREDS, "valkeyPassword", "vd")
for container in ("langfuse-web", "langfuse-worker"):
    require_literal(docs, container, "REDIS_HOST", EXPECTED_VALKEY, "vk")
    require_secret(docs, container, "REDIS_AUTH", VALKEY_CREDS, "valkeyPassword", "vk")
    require_literal(docs, container, "REDIS_TLS_ENABLED", "true", "vk-tls")

# ClickHouse is a Langfuse store.
for container in ("langfuse-web", "langfuse-worker"):
    require_contains(docs, container, "CLICKHOUSE_URL", CH_HOST, "ch")
    require_contains(docs, container, "CLICKHOUSE_URL", "https://", "ch-tls")
    require_secret(
        docs, container, "CLICKHOUSE_PASSWORD", CH_CREDS, "clickhousePassword", "ch"
    )
    require_literal(docs, container, "CLICKHOUSE_CLUSTER_ENABLED", "false", "ch")
    require_literal(docs, container, "CLICKHOUSE_USER", "default", "ch")
    require_literal(docs, container, "LANGFUSE_S3_EVENT_UPLOAD_BUCKET", "langfuse", "bk")
    require_literal(docs, container, "LANGFUSE_S3_MEDIA_UPLOAD_BUCKET", "langfuse", "bk")
    require_literal(docs, container, "LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT", S3_ENDPOINT, "s3")
    require_secret(
        docs,
        container,
        "LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY",
        S3_CREDS,
        "rustfsSecretKey",
        "s3",
    )
    require_secret(
        docs,
        container,
        "LANGFUSE_S3_MEDIA_UPLOAD_SECRET_ACCESS_KEY",
        S3_CREDS,
        "rustfsSecretKey",
        "s3",
    )

require_literal(docs, "api", "S3_ENDPOINT_URL", S3_ENDPOINT, "s3")
require_literal(docs, "worker", "S3_ENDPOINT_URL", S3_ENDPOINT, "s3")
require_literal(docs, "api", "S3_ACCESS_KEY", "acme", "s3")
require_literal(docs, "worker", "S3_ACCESS_KEY", "acme", "s3")
require_secret(docs, "api", "S3_SECRET_KEY", S3_CREDS, "rustfsSecretKey", "s3")
require_secret(docs, "worker", "S3_SECRET_KEY", S3_CREDS, "rustfsSecretKey", "s3")
require_literal(docs, "api", "BUNDLE_BUCKET", "curie-bundles", "bk")
require_literal(docs, "worker", "BUNDLE_BUCKET", "curie-bundles", "bk")
require_literal(docs, "worker", "CURIE_WORKSPACE_BUCKET", "curie-workspaces", "bk")
require_literal(docs, "bundle-fetch", "S3_ENDPOINT", S3_ENDPOINT, "s3")
require_secret(docs, "bundle-fetch", "S3_SECRET_KEY", S3_CREDS, "rustfsSecretKey", "s3")
require_literal(docs, "bundle-fetch", "BUNDLE_BUCKET", "curie-bundles", "bk")

object_store = named(docs, "NetworkPolicy", "acme-curie-runner-allow-object-store")
if len(object_store) != 1:
    die_later(
        f"[5] expected 1 NetworkPolicy/acme-curie-runner-allow-object-store, "
        f"found {len(object_store)}"
    )
else:
    cidrs = []
    for rule in object_store[0].get("spec", {}).get("egress") or []:
        for peer in rule.get("to") or []:
            block = peer.get("ipBlock") or {}
            if "cidr" in block:
                cidrs.append(block["cidr"])
    if S3_CIDR not in cidrs:
        die_later(f"[5] runner-allow-object-store cidrs {cidrs!r} missing {S3_CIDR}")

if named(docs, "NetworkPolicy", "acme-curie-runner-allow-rustfs"):
    die_later("[5] runner-allow-rustfs still rendered on the BYO overlay")

if failures:
    for item in failures:
        print(f"FAIL: {item}", file=sys.stderr)
    raise SystemExit(1)

print("  chart-owned stores absent; endpoints and Secret refs match overlay: OK")
PY

echo "external-values-profile-assertions: PASS"
