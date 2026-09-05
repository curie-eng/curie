#!/usr/bin/env bash
#
# The pre-upgrade drain gate must be wired the way issue #2010 needs it, and
# must refuse the two configurations that would make it silently useless.
#
# Every assertion here corresponds to a way the gate stops protecting anything
# while still rendering perfectly valid YAML:
#
#   * a `pre-install` hook would fail every fresh install against a Valkey that
#     does not exist yet;
#   * a non-zero backoffLimit would re-quiesce the fleet and re-wait the whole
#     timeout on a refusal, turning one postponed upgrade into minutes of a
#     cluster that is not claiming;
#   * `hook-failed` in the delete policy would destroy the only log naming which
#     deliveries held the upgrade back;
#   * a quiesce TTL that does not outlast the wait lapses mid-drain, so the
#     replicas resume claiming into the roll AND the gate still reports success;
#   * a wait shorter than the delivery budget refuses upgrades over turns that
#     are still inside the budget ADR-0131 already promised them, which is a
#     gate that gets switched off in its first week.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

helm template t "$CHART" > "$TMP/default.yaml"
helm template t "$CHART" --set worker.upgradeDrain.enabled=false > "$TMP/disabled.yaml"
helm template t "$CHART" --set worker.deploy=false > "$TMP/no-worker.yaml"

# --- the two render-time refusals -------------------------------------------
#
# Asserted by EXECUTING the rejected configuration, not by reading the helper:
# a guard that has never been seen refusing is a guard nobody has tested.

assert_render_fails() {
  local label="$1" expected="$2"
  shift 2
  local out
  if out="$(helm template t "$CHART" "$@" 2>&1)"; then
    echo "FAIL: $label rendered successfully; expected a render-time refusal" >&2
    exit 1
  fi
  if ! printf '%s' "$out" | grep -qF "$expected"; then
    echo "FAIL: $label was refused, but not for the expected reason." >&2
    echo "  expected to find: $expected" >&2
    echo "  actual output:" >&2
    printf '%s\n' "$out" | sed 's/^/    /' >&2
    exit 1
  fi
  echo "OK: $label is refused at render time"
}

assert_render_fails \
  "a quiesce TTL that does not outlast the drain wait" \
  "must be strictly greater than worker.upgradeDrain.timeoutSeconds" \
  --set worker.upgradeDrain.timeoutSeconds=900 \
  --set worker.upgradeDrain.quiesceTtlSeconds=900

# The cross-family relationship is DERIVED, not refused: raising the delivery
# budget is a decision made for unrelated reasons, and failing the render for a
# value the operator never touched would break configurations valid today. Both
# ends of the derivation are rendered so the assertion checks the relationship
# rather than a constant.
helm template t "$CHART" \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 > "$TMP/raised.yaml"
helm template t "$CHART" \
  --set worker.deliveryBudgetSeconds=60 \
  --set worker.runnerTotalTimeoutSeconds=60 \
  --set worker.deliveryShutdownReserveSeconds=0 \
  --set worker.upgradeDrain.timeoutSeconds=120 \
  --set worker.upgradeDrain.quiesceTtlSeconds=300 > "$TMP/small.yaml"

python3 - "$TMP/default.yaml" "$TMP/disabled.yaml" "$TMP/no-worker.yaml" "$TMP/small.yaml" "$TMP/raised.yaml" <<'PY'
import sys

import yaml

default_path, disabled_path, no_worker_path, small_path, raised_path = sys.argv[1:6]

DRAIN = "upgrade-drain"
RELEASE = "upgrade-drain-release"


def load(path):
    with open(path) as handle:
        return [doc for doc in yaml.safe_load_all(handle) if doc]


def jobs_by_component(docs):
    out = {}
    for doc in docs:
        if doc.get("kind") != "Job":
            continue
        component = (doc.get("metadata") or {}).get("labels", {}).get(
            "app.kubernetes.io/component"
        )
        if component in (DRAIN, RELEASE):
            out.setdefault(component, []).append(doc)
    return out


failures = []


def check(condition, message):
    if not condition:
        failures.append(message)


# --- the gate is absent unless it is asked for -------------------------------
for path, label in ((disabled_path, "upgradeDrain.enabled=false"), (no_worker_path, "worker.deploy=false")):
    present = jobs_by_component(load(path))
    check(not present, f"{label} still rendered {sorted(present)}")

# --- the default render ------------------------------------------------------
jobs = jobs_by_component(load(default_path))
for component in (DRAIN, RELEASE):
    check(
        len(jobs.get(component, [])) == 1,
        f"expected exactly one {component} Job, found {len(jobs.get(component, []))}",
    )

if not failures:
    drain = jobs[DRAIN][0]
    release = jobs[RELEASE][0]

    drain_ann = (drain.get("metadata") or {}).get("annotations", {})
    release_ann = (release.get("metadata") or {}).get("annotations", {})

    # The hook phases. `pre-upgrade` ONLY: a fresh install has nothing in flight
    # and no Valkey to ask, so `pre-install` would fail every first install.
    check(
        drain_ann.get("helm.sh/hook") == "pre-upgrade",
        f"drain hook is {drain_ann.get('helm.sh/hook')!r}, expected exactly 'pre-upgrade'",
    )
    check(
        release_ann.get("helm.sh/hook") == "post-upgrade",
        f"release hook is {release_ann.get('helm.sh/hook')!r}, expected exactly 'post-upgrade'",
    )
    # The failed gate Job must survive: its log is the only place an operator
    # can read WHICH deliveries held the upgrade back.
    check(
        "hook-failed" not in drain_ann.get("helm.sh/hook-delete-policy", ""),
        "the drain Job is auto-deleted on failure, destroying the refusal's evidence",
    )
    check(
        "before-hook-creation" in drain_ann.get("helm.sh/hook-delete-policy", ""),
        "the drain Job is not cleared before the next attempt",
    )

    drain_spec = drain.get("spec") or {}
    release_spec = release.get("spec") or {}
    # A refusal is a decision, not a transient error.
    check(
        drain_spec.get("backoffLimit") == 0,
        f"drain backoffLimit is {drain_spec.get('backoffLimit')!r}, expected 0: "
        "retrying a refusal re-quiesces the fleet and re-waits the whole timeout",
    )
    # Clearing the flag IS idempotent and worth retrying.
    check(
        (release_spec.get("backoffLimit") or 0) > 0,
        f"release backoffLimit is {release_spec.get('backoffLimit')!r}, expected > 0",
    )
    # The Job-level ceiling must sit ABOVE the gate's own wait, or Kubernetes
    # kills the gate before it can answer and every upgrade fails.
    check(
        (drain_spec.get("activeDeadlineSeconds") or 0) > 900,
        f"drain activeDeadlineSeconds is {drain_spec.get('activeDeadlineSeconds')!r}, "
        "expected greater than the 900s default drain wait",
    )

    for component, doc, mode in ((DRAIN, drain, "drain"), (RELEASE, release, "release")):
        pod = (doc.get("spec") or {}).get("template", {}).get("spec", {})
        check(
            pod.get("restartPolicy") == "Never",
            f"{component} restartPolicy is {pod.get('restartPolicy')!r}, expected 'Never'",
        )
        containers = pod.get("containers") or []
        check(len(containers) == 1, f"{component} has {len(containers)} containers, expected 1")
        if not containers:
            continue
        container = containers[0]
        # The WORKER image: the gate reads the worker's own key namespace and
        # lease keys through WorkerConfig, so a different image would be a
        # second copy of that layout free to drift.
        check(
            "curie-worker" in (container.get("image") or ""),
            f"{component} image is {container.get('image')!r}, expected the worker image",
        )
        check(
            container.get("command")
            == ["python", "-m", "curie_worker.upgrade_drain", "--mode", mode],
            f"{component} command is {container.get('command')!r}",
        )
        env = {e["name"]: e for e in container.get("env", []) if isinstance(e, dict)}
        for required in ("VALKEY_HOST", "VALKEY_PORT", "VALKEY_PASSWORD"):
            check(required in env, f"{component} is missing {required}")
        # Both Jobs build the same WorkerConfig, whose validator refuses a
        # quiesce TTL that does not outlast the wait. A release Job missing
        # these would construct a config the gate could not.
        check(
            env.get("CURIE_UPGRADE_DRAIN_TIMEOUT_S", {}).get("value") == "900",
            f"{component} CURIE_UPGRADE_DRAIN_TIMEOUT_S is "
            f"{env.get('CURIE_UPGRADE_DRAIN_TIMEOUT_S', {}).get('value')!r}, expected '900'",
        )
        check(
            env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "1800",
            f"{component} CURIE_UPGRADE_QUIESCE_TTL_S is "
            f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}, expected '1800'",
        )

    drain_env = {
        e["name"]: e
        for e in (drain["spec"]["template"]["spec"]["containers"][0].get("env") or [])
        if isinstance(e, dict)
    }
    check(
        drain_env.get("CURIE_UPGRADE_DRAIN_POLL_INTERVAL_S", {}).get("value") == "5",
        "the drain Job does not carry the configured poll interval",
    )

# --- the derived clocks ------------------------------------------------------


def drain_env(path, label):
    jobs = jobs_by_component(load(path))
    if len(jobs.get(DRAIN, [])) != 1:
        failures.append(f"the {label} render produced no drain Job")
        return None
    return {
        e["name"]: e
        for e in (jobs[DRAIN][0]["spec"]["template"]["spec"]["containers"][0].get("env") or [])
        if isinstance(e, dict)
    }


# A wait already above the budget is left alone: the floor is a floor, not an
# override, so an operator who asks for longer keeps it.
env = drain_env(small_path, "smaller-budget")
if env is not None:
    check(
        env.get("CURIE_UPGRADE_DRAIN_TIMEOUT_S", {}).get("value") == "120",
        "a drain wait already above the budget was not left at the configured value: "
        f"{env.get('CURIE_UPGRADE_DRAIN_TIMEOUT_S', {}).get('value')!r}",
    )
    check(
        env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "300",
        "a quiesce TTL already above the wait was not left at the configured value: "
        f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}",
    )

# Raising deliveryBudgetSeconds to its 1800s maximum, with the grace ADR-0131
# requires, must still render -- and must carry the gate up with it rather than
# leaving a 900s wait that would refuse every upgrade during ordinary traffic.
# 1800 + 60 reserve = 1860, and the quiesce TTL is derived above that.
env = drain_env(raised_path, "raised-budget")
if env is not None:
    check(
        env.get("CURIE_UPGRADE_DRAIN_TIMEOUT_S", {}).get("value") == "1860",
        "raising the delivery budget did not raise the effective drain wait: "
        f"{env.get('CURIE_UPGRADE_DRAIN_TIMEOUT_S', {}).get('value')!r}, expected '1860'",
    )
    check(
        env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "1920",
        "the quiesce TTL was not derived above the raised wait, so the worker "
        "would refuse it at boot: "
        f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}, expected '1920'",
    )

if failures:
    print("FAIL: upgrade drain gate render assertions failed", file=sys.stderr)
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    raise SystemExit(1)

print("OK: upgrade drain gate render assertions passed")
PY

# Opt-in transactional recovery must retain all three phase witnesses.
python3 "$SCRIPT_DIR/upgrade-recovery-assertions.py" "$CHART"
