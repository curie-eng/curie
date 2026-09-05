#!/usr/bin/env bash
#
# Schema migrations run in one Helm hook Job (#2300), not in every API pod.
# These assertions prove the Job is the migrator and the API init is a wait.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

helm template t "$CHART" > "$TMP/default.yaml"
helm template t "$CHART" --set api.migrate.enabled=false > "$TMP/disabled.yaml"
helm template t "$CHART" --set api.deploy=false \
  --set ui.apiBaseUrl=https://api.example.com > "$TMP/no-api.yaml"
helm template t "$CHART" --set api.migrate.forwardOnly=true > "$TMP/forward.yaml"

python3 - "$TMP/default.yaml" "$TMP/disabled.yaml" "$TMP/no-api.yaml" "$TMP/forward.yaml" <<'PY'
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


default, disabled, no_api, forward = sys.argv[1:5]

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

print("OK: schema-migrate Job is the only migrator; API init waits")
PY
