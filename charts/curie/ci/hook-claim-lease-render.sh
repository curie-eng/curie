#!/usr/bin/env bash
#
# The API and worker deployments render one delivery budget and one hook claim
# lease. Chart defaults keep the lease unset. Compose resolves the same pair
# from compose.dev.yaml. An unset lease is null or absent. An empty string is
# not a default local up.
#
# Render with helm template --output-dir. Do not pipe helm template. A stdout
# pipe has been observed to truncate while still exiting 0.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

cat > "$TMP/check_helm.py" <<'PY'
import sys
import yaml

api_manifest, worker_manifest, budget, lease, case = sys.argv[1:6]


def die(message):
    sys.stderr.write("ASSERTION FAILED: %s: %s\n" % (case, message))
    sys.exit(1)


def containers(manifest, path_label):
    with open(manifest) as handle:
        docs = [doc for doc in yaml.safe_load_all(handle) if isinstance(doc, dict)]
    deployments = [doc for doc in docs if doc.get("kind") == "Deployment"]
    if len(deployments) != 1:
        die("%s: expected one Deployment, found %d" % (path_label, len(deployments)))
    spec = ((deployments[0].get("spec") or {}).get("template") or {}).get("spec") or {}
    found = list(spec.get("containers") or [])
    found.extend(spec.get("initContainers") or [])
    return found


def env_map(container):
    mapped = {}
    for entry in container.get("env") or []:
        if not isinstance(entry, dict) or "name" not in entry:
            continue
        if "value" in entry:
            mapped[entry["name"]] = entry["value"]
        else:
            mapped[entry["name"]] = "<valueFrom>"
    return mapped


def observe(manifest, path_label, required_name):
    named = [item for item in containers(manifest, path_label) if item.get("name") == required_name]
    if len(named) != 1:
        die("%s: container %r appears %d times" % (path_label, required_name, len(named)))
    observed = []
    for container in containers(manifest, path_label):
        env = env_map(container)
        carries = "CURIE_DELIVERY_BUDGET_S" in env or "CURIE_HOOK_CLAIM_LEASE_S" in env
        if container.get("name") != required_name and not carries:
            continue
        if "CURIE_DELIVERY_BUDGET_S" not in env:
            die("%s container %s is missing CURIE_DELIVERY_BUDGET_S" % (path_label, container.get("name")))
        budget_value = env["CURIE_DELIVERY_BUDGET_S"]
        if "CURIE_HOOK_CLAIM_LEASE_S" in env:
            # Helm may render the seconds as an int or a quoted string.
            lease_value = str(env["CURIE_HOOK_CLAIM_LEASE_S"])
        else:
            lease_value = "<absent>"
        observed.append((container.get("name"), str(budget_value), lease_value))
    return observed


api = observe(api_manifest, "api", "api")
worker = observe(worker_manifest, "worker", "worker")
for name, seen_budget, seen_lease in api + worker:
    if seen_budget != budget:
        die("container %s CURIE_DELIVERY_BUDGET_S is %r, expected %r" % (name, seen_budget, budget))
    if lease == "absent":
        if seen_lease != "<absent>":
            die("container %s has CURIE_HOOK_CLAIM_LEASE_S=%r; expected it absent" % (name, seen_lease))
    elif seen_lease != lease:
        die("container %s CURIE_HOOK_CLAIM_LEASE_S is %r, expected %r" % (name, seen_lease, lease))
PY

cat > "$TMP/check_compose.py" <<'PY'
import json
import os
import subprocess
import sys

import yaml

repo = sys.argv[1]
budget_name = "CURIE_DELIVERY_BUDGET_S"
lease_name = "CURIE_HOOK_CLAIM_LEASE_S"
expected_raw = "${CURIE_DELIVERY_BUDGET_S:-600}"
services = ("curie-api", "curie-worker")


def die(message):
    sys.stderr.write("ASSERTION FAILED: %s\n" % message)
    sys.exit(1)


def environment(service):
    raw = service.get("environment") or {}
    if isinstance(raw, list):
        out = {}
        for entry in raw:
            name, separator, value = str(entry).partition("=")
            out[name] = value if separator else None
        return out
    if isinstance(raw, dict):
        return dict(raw)
    die("environment is %r" % type(raw).__name__)


document = yaml.safe_load(open(repo + "/compose.dev.yaml"))
for service in services:
    env = environment(document["services"][service])
    if budget_name not in env:
        die("raw %s is missing %s" % (service, budget_name))
    if env[budget_name] != expected_raw:
        die(
            "raw %s %s is %r, expected %r"
            % (service, budget_name, env[budget_name], expected_raw)
        )


def run_config(extra):
    env = {
        name: value
        for name, value in os.environ.items()
        if name not in (budget_name, lease_name)
    }
    env.update(extra)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "--profile",
            "full",
            "-f",
            repo + "/compose.dev.yaml",
            "config",
            "--format",
            "json",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        die("docker compose config failed: %s" % result.stderr.strip())
    return json.loads(result.stdout)["services"]


def lease_of(env):
    if lease_name not in env:
        return ("absent", None)
    return ("present", env[lease_name])


def budget_of(env, service, case):
    if budget_name not in env:
        die("%s: %s is missing %s" % (case, service, budget_name))
    return str(env[budget_name])


def check_unset():
    rendered = run_config({})
    for service in services:
        env = environment(rendered[service])
        if budget_of(env, service, "compose-unset") != "600":
            die("compose-unset: %s budget is %r, expected 600" % (service, budget_of(env, service, "compose-unset")))
        state, value = lease_of(env)
        if state == "present" and value is not None:
            die("compose-unset: %s lease is %r; expected null or absent" % (service, value))


def check_empty():
    rendered = run_config({lease_name: ""})
    observed = []
    for service in services:
        env = environment(rendered[service])
        if budget_of(env, service, "compose-empty-lease") != "600":
            die(
                "compose-empty-lease: %s budget is %r, expected 600"
                % (service, budget_of(env, service, "compose-empty-lease"))
            )
        state, value = lease_of(env)
        if state == "present" and value not in (None, ""):
            die("compose-empty-lease: %s lease is %r; expected null, absent, or empty" % (service, value))
        observed.append((state, value))
    if observed[0] != observed[1]:
        die("compose-empty-lease: curie-api and curie-worker disagree: %r" % (observed,))


def check_override():
    rendered = run_config({budget_name: "1800", lease_name: "7200"})
    for service in services:
        env = environment(rendered[service])
        if budget_of(env, service, "compose-override") != "1800":
            die("compose-override: %s budget is %r, expected 1800" % (service, budget_of(env, service, "compose-override")))
        state, value = lease_of(env)
        if state != "present" or str(value) != "7200":
            die("compose-override: %s lease is %r, expected 7200" % (service, value))


check_unset()
check_empty()
check_override()
PY

render() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  if ! helm template curie "$CHART" --output-dir "$out" "$@" >/dev/null; then
    fail "$name: helm template failed"
  fi
  local api="$out/curie/templates/api.yaml"
  local worker="$out/curie/templates/worker.yaml"
  [ -f "$api" ] || fail "$name: api.yaml did not render"
  [ -f "$worker" ] || fail "$name: worker.yaml did not render"
  python3 "$TMP/check_helm.py" "$api" "$worker" "$BUDGET_EXPECT" "$LEASE_EXPECT" "$name"
}

BUDGET_EXPECT=10800
LEASE_EXPECT=absent
render chart-defaults

BUDGET_EXPECT=1800
LEASE_EXPECT=absent
# The chart refuses a runner ceiling above the delivery budget. Lower both.
render budget-override \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800

BUDGET_EXPECT=1800
LEASE_EXPECT=7200
render budget-and-lease \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.hookClaimLeaseSeconds=7200

cat > "$TMP/null-lease.yaml" <<'YAML'
worker:
  hookClaimLeaseSeconds: null
YAML

BUDGET_EXPECT=10800
LEASE_EXPECT=absent
render null-lease -f "$TMP/null-lease.yaml"

python3 "$TMP/check_compose.py" "$REPO_ROOT"
