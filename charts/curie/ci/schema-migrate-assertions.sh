#!/usr/bin/env bash
#
# Schema migrations run in one Helm hook Job (#2300), not in every API pod.
# These assertions prove the Job is the migrator and the API init is a wait.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

helm template t "$CHART" --namespace default > "$TMP/default.yaml"
helm template t "$CHART" --namespace default --set api.migrate.enabled=false > "$TMP/disabled.yaml"
helm template t "$CHART" --namespace default --set api.deploy=false --set ui.apiBaseUrl=https://api.example.com > "$TMP/no-api.yaml"
helm template t "$CHART" --namespace default --set api.migrate.forwardOnly=true > "$TMP/forward.yaml"
helm template t "$CHART" --namespace default --is-upgrade > "$TMP/upgrade.yaml"
helm template t "$CHART" --namespace default --is-upgrade --set worker.upgradeDrain.enabled=false > "$TMP/no-drain.yaml"
helm template t "$CHART" --namespace default --is-upgrade --set worker.deploy=false > "$TMP/no-worker.yaml"
helm template t "$CHART" --namespace default --is-upgrade \
  --set valkey.deploy=false --set valkey.host=valkey.example.com --set valkey.port=6380 \
  --set valkey.tls=true --set valkey.existingSecret=acme-valkey > "$TMP/byo.yaml"

CHECK="$TMP/check-schema-migrate.py"
cat > "$CHECK" <<'PY'
import sys

import yaml


def fail(message):
    raise SystemExit(message)


def docs(path):
    return [doc for doc in yaml.safe_load_all(open(path)) if isinstance(doc, dict)]


def jobs(path, name_suffix):
    found = []
    for doc in docs(path):
        if doc.get("kind") != "Job":
            continue
        name = doc.get("metadata", {}).get("name", "")
        if name.endswith(name_suffix):
            found.append(doc)
    return found


def api_inits(path):
    found = []
    for doc in docs(path):
        if doc.get("kind") != "Deployment":
            continue
        labels = doc.get("metadata", {}).get("labels", {})
        if labels.get("app.kubernetes.io/component") != "api":
            continue
        inits = (
            doc.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("initContainers")
            or []
        )
        found.extend(inits)
    return found


default, disabled, no_api, forward, upgrade, no_drain, no_worker, byo = sys.argv[1:9]

migrate_jobs = jobs(default, "-schema-migrate")
if len(migrate_jobs) != 1:
    fail(f"expected one schema-migrate Job, found {len(migrate_jobs)}")
job = migrate_jobs[0]
annotations = job.get("metadata", {}).get("annotations", {})
if annotations.get("helm.sh/hook") != "post-install,pre-upgrade":
    fail(f"schema-migrate hook phases are {annotations.get('helm.sh/hook')!r}")
if annotations.get("helm.sh/hook-weight") != "-5":
    fail("schema-migrate must run after the drain gate (weight -10)")
policy = annotations.get("helm.sh/hook-delete-policy", "")
if "hook-failed" in policy:
    fail("failed migrate Job logs must be kept; hook-failed would delete them")
if job.get("spec", {}).get("backoffLimit") != 3:
    fail("schema-migrate backoffLimit must be 3 so crash retry can resume")

container = job["spec"]["template"]["spec"]["containers"][0]
script = " ".join(container.get("args") or [])
if "python -m curie_api.schema_compat upgrade" not in script:
    fail("schema-migrate Job must exec python -m curie_api.schema_compat upgrade")
if "import curie_api.schema_compat" not in script:
    fail("schema-migrate Job must probe for schema_compat before calling it")
if "alembic -c alembic.ini upgrade head" not in script:
    fail(
        "schema-migrate Job must fall back to Alembic on a pre-#2300 API image "
        "(released-upgrade --reset-then-reuse-values keeps the old digest)"
    )
if script.index("import curie_api.schema_compat") > script.index("attempt=1"):
    fail("current-image migrate must enter its Python supervisor before shell readiness")
env = {item["name"]: item.get("value") for item in container.get("env", [])}
if env.get("CURIE_SCHEMA_FORWARD_ONLY") != "false":
    fail(f"default forward-only must be false, got {env.get('CURIE_SCHEMA_FORWARD_ONLY')!r}")

inits = api_inits(default)
names = [item.get("name") for item in inits]
if "migrate" in names:
    fail("API Deployment must not keep a migrate init container")
if names != ["schema-wait"]:
    fail(f"API init containers must be exactly schema-wait, got {names!r}")
wait_script = " ".join((inits[0].get("args") or []))
if "alembic" in wait_script:
    fail("API schema-wait init must not invoke alembic")
if "python -m curie_api.schema_compat wait" not in wait_script:
    fail("API schema-wait init must exec python -m curie_api.schema_compat wait")
if "import curie_api.schema_compat" not in wait_script:
    fail("API schema-wait init must probe for schema_compat before calling it")

if jobs(disabled, "-schema-migrate"):
    fail("api.migrate.enabled=false must omit the schema-migrate Job")
if any(item.get("name") == "schema-wait" for item in api_inits(disabled)):
    fail("api.migrate.enabled=false must omit the schema-wait init")

if jobs(no_api, "-schema-migrate"):
    fail("api.deploy=false must omit the schema-migrate Job")

fwd_jobs = jobs(forward, "-schema-migrate")
if len(fwd_jobs) != 1:
    fail("forwardOnly=true must still render the schema-migrate Job")
fwd_env = {
    item["name"]: item.get("value")
    for item in fwd_jobs[0]["spec"]["template"]["spec"]["containers"][0].get("env", [])
}
if fwd_env.get("CURIE_SCHEMA_FORWARD_ONLY") != "true":
    fail("api.migrate.forwardOnly=true must set CURIE_SCHEMA_FORWARD_ONLY=true")

pause_names = {
    "VALKEY_HOST", "VALKEY_PORT", "VALKEY_PASSWORD", "VALKEY_TLS",
    "CURIE_INSTALLATION_ID", "CURIE_UPGRADE_REVISION",
    "CURIE_UPGRADE_LEGACY_QUIESCE", "KEY_PREFIX",
}


def container_env(job):
    entries = job["spec"]["template"]["spec"]["containers"][0].get("env", [])
    names = [item["name"] for item in entries]
    if len(names) != len(set(names)):
        fail("schema-migrate pause wiring must not duplicate an env name")
    return {item["name"]: item for item in entries}


for path in (default, forward, no_drain, no_worker):
    items = container_env(jobs(path, "-schema-migrate")[0])
    if pause_names.intersection(items):
        fail("install, disabled drain and absent worker must omit every pause env")


def assert_pause_wiring(path):
    migrate = container_env(jobs(path, "-schema-migrate")[0])
    drain = container_env(jobs(path, "-upgrade-drain")[0])
    missing = pause_names.difference(migrate)
    if missing:
        fail(f"upgrade schema-migrate is missing pause env: {sorted(missing)!r}")
    for name in pause_names - {"KEY_PREFIX"}:
        if migrate[name] != drain[name]:
            fail(f"upgrade schema-migrate and drain disagree on {name}")
    if migrate["KEY_PREFIX"].get("value") != "curie:worker":
        fail("upgrade schema-migrate KEY_PREFIX must match the existing drain default")
    return migrate


assert_pause_wiring(upgrade)
byo_env = assert_pause_wiring(byo)
if byo_env["VALKEY_HOST"].get("value") != "valkey.example.com":
    fail("upgrade schema-migrate must honor the existing BYO Valkey host")
if byo_env["VALKEY_PORT"].get("value") != "6380":
    fail("upgrade schema-migrate must honor the existing BYO Valkey port")
if byo_env["VALKEY_TLS"].get("value") != "true":
    fail("upgrade schema-migrate must preserve BYO Valkey TLS")
if byo_env["VALKEY_PASSWORD"].get("valueFrom", {}).get("secretKeyRef") != {
    "name": "acme-valkey", "key": "valkeyPassword",
}:
    fail("upgrade schema-migrate must source BYO Valkey auth from its existing Secret")

print("OK: schema-migrate is the only migrator; pause wiring is upgrade-only and matches drain")
PY

python3 "$CHECK" "$TMP/default.yaml" "$TMP/disabled.yaml" "$TMP/no-api.yaml" \
  "$TMP/forward.yaml" "$TMP/upgrade.yaml" "$TMP/no-drain.yaml" "$TMP/no-worker.yaml" "$TMP/byo.yaml"

python3 - "$TMP/upgrade.yaml" "$TMP/missing-revision.yaml" <<'PY'
import pathlib
import sys
import yaml

docs = list(yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text()))
for doc in docs:
    if isinstance(doc, dict) and doc.get("kind") == "Job" and doc["metadata"]["name"].endswith("-schema-migrate"):
        container = doc["spec"]["template"]["spec"]["containers"][0]
        container["env"] = [item for item in container["env"] if item["name"] != "CURIE_UPGRADE_REVISION"]
pathlib.Path(sys.argv[2]).write_text(yaml.safe_dump_all(docs))
PY
if python3 "$CHECK" "$TMP/default.yaml" "$TMP/disabled.yaml" "$TMP/no-api.yaml" \
    "$TMP/forward.yaml" "$TMP/missing-revision.yaml" "$TMP/no-drain.yaml" \
    "$TMP/no-worker.yaml" "$TMP/byo.yaml" > "$TMP/negative.log" 2>&1; then
  echo "FAIL: missing-revision negative control passed the pause wiring assertion" >&2
  exit 1
fi
negative_output="$(cat "$TMP/negative.log")"
if [[ "$negative_output" != *"missing pause env"*"CURIE_UPGRADE_REVISION"* ]]; then
  cat "$TMP/negative.log" >&2
  exit 1
fi
echo "OK: missing revision is rejected by the same pause wiring assertion"
