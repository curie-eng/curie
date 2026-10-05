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
#   * a roll-hold quiesce TTL longer than the drain wait strands a paused
#     fleet for longer than the upgrade could ever have waited (#3127): the
#     marker is a renewed lease while waiting, and the hold written after a
#     clean drain is capped at the effective wait;
#   * a wait shorter than the delivery budget refuses upgrades over turns that
#     are still inside the budget ADR-0131 already promised them, which is a
#     gate that gets switched off in its first week.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

helm template t "$CHART" > "$TMP/default.yaml"
helm template t "$CHART" > "$TMP/fresh-second.yaml"
helm template t "$CHART" --is-upgrade > "$TMP/client-upgrade.yaml"
# A model credential only reaches the worker on a real-model render (#2640).
helm template t "$CHART" \
  --set agentSandbox.runner.fakeModel=false \
  --set-string postgres.existingSecret=acme-postgres-credentials \
  --set-string valkey.existingSecret=acme-valkey-credentials \
  --set-string agentSandbox.runner.credentialsExistingSecret=acme-model-credentials \
  --set-string worker.adapterCredentialsExistingSecret=acme-adapter-credentials \
  > "$TMP/byo-credentials.yaml"
helm template t "$CHART" --set worker.upgradeDrain.enabled=false > "$TMP/disabled.yaml"
helm template t "$CHART" --set worker.deploy=false > "$TMP/no-worker.yaml"
helm template t "$CHART" \
  --set-string worker.upgradeDrain.resources.requests.memory=512Mi \
  --set-string worker.upgradeDrain.resources.limits.memory=768Mi \
  > "$TMP/custom-memory.yaml"

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

# #3127: a quiesce TTL at or below the wait is no longer refused. The wait is
# held by a renewed lease, so the roll hold may be shorter than the wait.
helm template t "$CHART" \
  --set worker.upgradeDrain.timeoutSeconds=900 \
  --set worker.upgradeDrain.quiesceTtlSeconds=900 > "$TMP/equal-ttl.yaml"
helm template t "$CHART" \
  --set worker.upgradeDrain.timeoutSeconds=900 \
  --set worker.upgradeDrain.quiesceTtlSeconds=600 > "$TMP/short-ttl.yaml"

# The cross-family relationship is DERIVED, not refused: raising the delivery
# budget is a decision made for unrelated reasons, and failing the render for a
# value the operator never touched would break configurations valid today. Both
# ends of the derivation are rendered so the assertion checks the relationship
# rather than a constant.
helm template t "$CHART" \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=2400 > "$TMP/raised.yaml"
helm template t "$CHART" \
  --set worker.deliveryBudgetSeconds=60 \
  --set worker.runnerTotalTimeoutSeconds=60 \
  --set worker.deliveryShutdownReserveSeconds=0 \
  --set worker.upgradeDrain.timeoutSeconds=120 \
  --set worker.upgradeDrain.quiesceTtlSeconds=300 > "$TMP/small.yaml"

python3 - "$TMP/default.yaml" "$TMP/disabled.yaml" "$TMP/no-worker.yaml" "$TMP/small.yaml" "$TMP/raised.yaml" "$TMP/equal-ttl.yaml" "$TMP/short-ttl.yaml" "$TMP/custom-memory.yaml" <<'PY'
import sys

import yaml

default_path, disabled_path, no_worker_path, small_path, raised_path = sys.argv[1:6]
equal_ttl_path, short_ttl_path = sys.argv[6:8]
custom_memory_path = sys.argv[8]

DRAIN = "upgrade-drain"
RELEASE = "upgrade-drain-release"
ATTEST = "upgrade-drain-attest"


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
        if component in (DRAIN, RELEASE, ATTEST):
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
for component in (DRAIN, RELEASE, ATTEST):
    check(
        len(jobs.get(component, [])) == 1,
        f"expected exactly one {component} Job, found {len(jobs.get(component, []))}",
    )

# All hook modes import the worker dependency tree. Keep their shared sizing
# and the operator override wired through the rendered containers.
for path, label, request_memory, limit_memory in (
    (default_path, "default", "384Mi", "384Mi"),
    (custom_memory_path, "custom-memory", "512Mi", "768Mi"),
):
    rendered_jobs = jobs_by_component(load(path))
    for component in (DRAIN, RELEASE, ATTEST):
        matching = rendered_jobs.get(component, [])
        if len(matching) != 1:
            check(False, f"the {label} render lacks exactly one {component} Job")
            continue
        pod = (matching[0].get("spec") or {}).get("template", {}).get("spec", {})
        containers = pod.get("containers") or []
        if len(containers) != 1:
            check(False, f"the {label} {component} Job lacks exactly one container")
            continue
        resources = containers[0].get("resources") or {}
        for key, expected_memory, expected_cpu in (
            ("requests", request_memory, "10m"),
            ("limits", limit_memory, "200m"),
        ):
            configured = resources.get(key) or {}
            check(
                configured.get("memory") == expected_memory,
                f"the {label} {component} {key}.memory is {configured.get('memory')!r}, "
                f"expected {expected_memory!r}",
            )
            check(
                configured.get("cpu") == expected_cpu,
                f"the {label} {component} {key}.cpu is {configured.get('cpu')!r}, "
                f"expected {expected_cpu!r}",
            )

if not failures:
    drain = jobs[DRAIN][0]
    release = jobs[RELEASE][0]
    attest = jobs[ATTEST][0]

    drain_ann = (drain.get("metadata") or {}).get("annotations", {})
    release_ann = (release.get("metadata") or {}).get("annotations", {})
    attest_ann = (attest.get("metadata") or {}).get("annotations") or {}

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
    # Attest is the second pre-upgrade refusal. It runs only after a recorded
    # drain, so Helm must not treat deleting the drain Job as success.
    check(
        attest_ann.get("helm.sh/hook") == "pre-upgrade",
        f"attest hook is {attest_ann.get('helm.sh/hook')!r}, expected exactly 'pre-upgrade'",
    )
    check(
        attest_ann.get("helm.sh/hook-weight") == "-9",
        f"attest hook weight is {attest_ann.get('helm.sh/hook-weight')!r}, expected '-9'",
    )
    attest_delete = attest_ann.get("helm.sh/hook-delete-policy", "")
    check(
        "before-hook-creation" in attest_delete,
        "the attest Job is not cleared before the next attempt",
    )
    check(
        "hook-failed" not in attest_delete,
        "the attest Job is auto-deleted on failure, destroying the refusal's evidence",
    )
    check(
        "curie.ai/minimum-helm-timeout-seconds" not in attest_ann,
        "the attest Job carries curie.ai/minimum-helm-timeout-seconds; "
        "that annotation belongs to the drain Job only",
    )
    attest_spec = attest.get("spec") or {}
    check(
        attest_spec.get("backoffLimit") == 0,
        f"attest backoffLimit is {attest_spec.get('backoffLimit')!r}, expected 0",
    )
    # The deadline starts at Job creation, so it has to cover the worker image
    # pull. 60s timed out after a clean drain and left the roll hold set.
    check(
        attest_spec.get("activeDeadlineSeconds") == 300,
        f"attest activeDeadlineSeconds is {attest_spec.get('activeDeadlineSeconds')!r}, "
        "expected 300 so a slow image pull cannot fail attest after a clean drain",
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
    # kills the gate before it can answer and every upgrade fails. 900 is the
    # configured timeout floor, not the effective wait: the delivery budget
    # plus reserve raises that wait above the floor.
    check(
        (drain_spec.get("activeDeadlineSeconds") or 0) > 900,
        f"drain activeDeadlineSeconds is {drain_spec.get('activeDeadlineSeconds')!r}, "
        "expected greater than 900",
    )

    for component, doc, mode in (
        (DRAIN, drain, "drain"),
        (RELEASE, release, "release"),
        (ATTEST, attest, "attest"),
    ):
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
            == [
                "python",
                "-m",
                "curie_worker.upgrade_drain",
                "--mode",
                mode,
                "--installation-id-observed=true",
            ],
            f"{component} command does not pass the observed installation identity",
        )
        env = {e["name"]: e for e in container.get("env", []) if isinstance(e, dict)}
        for required in ("VALKEY_HOST", "VALKEY_PORT", "VALKEY_PASSWORD"):
            check(required in env, f"{component} is missing {required}")
        # Both Jobs build the same WorkerConfig. The roll hold is capped at the
        # effective wait (#3127): min(quiesceTtlSeconds 1800, wait 10860) = 1800.
        check(
            env.get("CURIE_UPGRADE_DRAIN_TIMEOUT_S", {}).get("value") == "10860",
            f"{component} CURIE_UPGRADE_DRAIN_TIMEOUT_S is "
            f"{env.get('CURIE_UPGRADE_DRAIN_TIMEOUT_S', {}).get('value')!r}, expected '10860'",
        )
        check(
            env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "1800",
            f"{component} CURIE_UPGRADE_QUIESCE_TTL_S is "
            f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}, expected '1800' "
            "(min of quiesceTtlSeconds and the effective drain wait)",
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
        env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "120",
        "a quiesce TTL above the wait was not capped at the wait (expected '120'): "
        f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}",
    )

# Raising deliveryBudgetSeconds to 1800s and raising the worker
# grace must still render, with the gate raised to cover the delivery budget.
# 1800 + 60 reserve = 1860; the roll hold is min(1800, 1860) = 1800.
env = drain_env(raised_path, "raised-budget")
if env is not None:
    check(
        env.get("CURIE_UPGRADE_DRAIN_TIMEOUT_S", {}).get("value") == "1860",
        "raising the delivery budget did not raise the effective drain wait: "
        f"{env.get('CURIE_UPGRADE_DRAIN_TIMEOUT_S', {}).get('value')!r}, expected '1860'",
    )
    check(
        env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == "1800",
        "the roll hold was not min(quiesceTtlSeconds, effective wait): "
        f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}, expected '1800'",
    )


# The published minimum includes the actual drain Job deadline, the rendered
# worker grace, and 60 seconds for scheduling and Helm operations.
for path, label, expected_wait, expected_grace, expected_minimum in (
    (default_path, "default", 10860, 10860, 21900),
    (raised_path, "raised-budget-and-grace", 1860, 2400, 4440),
):
    docs = load(path)
    jobs = jobs_by_component(docs)
    workers = [
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and (doc.get("metadata") or {}).get("labels", {}).get("app.kubernetes.io/component")
        == "worker"
    ]
    if len(jobs.get(DRAIN, [])) != 1 or len(workers) != 1:
        failures.append(f"the {label} render lacks one drain Job or worker Deployment")
        continue
    job = jobs[DRAIN][0]
    deadline = (job.get("spec") or {}).get("activeDeadlineSeconds")
    grace = ((workers[0].get("spec") or {}).get("template") or {}).get("spec", {}).get(
        "terminationGracePeriodSeconds"
    )
    minimum = (job.get("metadata") or {}).get("annotations", {}).get(
        "curie.ai/minimum-helm-timeout-seconds"
    )
    check(
        deadline == expected_wait + 120,
        f"the {label} drain Job deadline is {deadline!r}, expected {expected_wait + 120}",
    )
    check(grace == expected_grace, f"the {label} worker grace is {grace!r}, expected {expected_grace}")
    check(
        isinstance(minimum, str) and minimum.isdecimal(),
        f"the {label} minimum Helm timeout annotation is not a decimal string: {minimum!r}",
    )
    if isinstance(minimum, str) and minimum.isdecimal():
        check(
            int(minimum) == expected_minimum,
            f"the {label} minimum Helm timeout is {minimum}, expected {expected_minimum}",
        )
        if isinstance(deadline, int) and isinstance(grace, int):
            check(
                int(minimum) == deadline + grace + 60,
                f"the {label} minimum Helm timeout does not cover the drain Job deadline, "
                "worker grace, and 60 second margin",
            )

# A configured hold below the wait renders and is kept; an equal one too.
for path, label, expected in (
    (short_ttl_path, "short-ttl", "600"),
    (equal_ttl_path, "equal-ttl", "900"),
):
    env = drain_env(path, label)
    if env is not None:
        check(
            env.get("CURIE_UPGRADE_QUIESCE_TTL_S", {}).get("value") == expected,
            f"the {label} render's CURIE_UPGRADE_QUIESCE_TTL_S is "
            f"{env.get('CURIE_UPGRADE_QUIESCE_TTL_S', {}).get('value')!r}, "
            f"expected {expected!r}",
        )

# The invariant on every render: the hold never outlasts the drain wait.
for path, label in (
    (default_path, "default"),
    (small_path, "smaller-budget"),
    (raised_path, "raised-budget"),
    (equal_ttl_path, "equal-ttl"),
    (short_ttl_path, "short-ttl"),
):
    env = drain_env(path, label)
    if env is None:
        continue
    try:
        ttl = float(env["CURIE_UPGRADE_QUIESCE_TTL_S"]["value"])
        wait = float(env["CURIE_UPGRADE_DRAIN_TIMEOUT_S"]["value"])
    except (KeyError, TypeError, ValueError):
        failures.append(f"the {label} render lacks a numeric quiesce TTL or wait")
        continue
    check(
        ttl <= wait,
        f"the {label} render holds the marker {ttl}s, longer than the {wait}s wait",
    )

if failures:
    print("FAIL: upgrade drain gate render assertions failed", file=sys.stderr)
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    raise SystemExit(1)

print("OK: upgrade drain gate render assertions passed")
PY

# The installation identity is one render-scoped value shared by the managed
# Secret, the long-running worker and both lifecycle hooks. Keep these checks in
# the real Helm consumer: independently calling a random helper from each
# template would look plausible in source and still split the release across
# three different Valkey keys.
python3 - \
  "$TMP/default.yaml" \
  "$TMP/fresh-second.yaml" \
  "$TMP/client-upgrade.yaml" \
  "$TMP/byo-credentials.yaml" <<'PY'
import pathlib
import sys

import yaml

default_path, fresh_second_path, client_upgrade_path, byo_path = map(
    pathlib.Path, sys.argv[1:5]
)

DRAIN = "upgrade-drain"
RELEASE = "upgrade-drain-release"
ATTEST = "upgrade-drain-attest"


def load(path):
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def one(docs, *, kind, component=None):
    matches = []
    for doc in docs:
        if doc.get("kind") != kind:
            continue
        labels = (doc.get("metadata") or {}).get("labels") or {}
        if component is not None and labels.get("app.kubernetes.io/component") != component:
            continue
        matches.append(doc)
    assert len(matches) == 1, (
        f"expected one {kind} for component {component!r}, found {len(matches)}"
    )
    return matches[0]


def managed_secret(docs):
    # Exactly one release Secret owns the installation identity. Selected by
    # that key rather than by count: the chart also owns the runner token
    # Secret (#3821), which carries no installationId.
    secrets = [
        doc
        for doc in docs
        if doc.get("kind") == "Secret"
        and ((doc.get("metadata") or {}).get("labels") or {}).get(
            "app.kubernetes.io/instance"
        )
        == "t"
        and "installationId" in (doc.get("stringData") or {})
    ]
    assert len(secrets) == 1, (
        f"expected one release Secret carrying installationId, found {len(secrets)}"
    )
    secret = secrets[0]
    installation_id = (secret.get("stringData") or {}).get("installationId")
    assert isinstance(installation_id, str) and installation_id.strip(), (
        "managed Secret has no nonblank stringData.installationId"
    )
    return secret, installation_id


def container_env(doc):
    containers = ((doc.get("spec") or {}).get("template") or {}).get("spec", {}).get(
        "containers"
    ) or []
    assert len(containers) == 1, "expected exactly one container"
    return containers[0], containers[0].get("env") or []


def unique_env(env, name):
    entries = [entry for entry in env if entry.get("name") == name]
    assert len(entries) == 1, f"expected exactly one {name} entry, found {len(entries)}"
    return entries[0]


def assert_identity_render(docs, *, observed, legacy):
    secret, installation_id = managed_secret(docs)
    managed_name = secret["metadata"]["name"]

    worker = one(docs, kind="Deployment", component="worker")
    _, worker_env = container_env(worker)
    worker_identity = unique_env(worker_env, "CURIE_INSTALLATION_ID")
    ref = (worker_identity.get("valueFrom") or {}).get("secretKeyRef") or {}
    assert ref.get("name") == managed_name, (
        "worker installation identity does not reference the release-managed Secret"
    )
    assert ref.get("key") == "installationId", (
        "worker installation identity does not reference the installationId key"
    )
    assert ref.get("optional", False) is False, (
        "worker installation identity Secret reference is optional"
    )
    assert "value" not in worker_identity, "worker installation identity was inlined"

    revisions = []
    identities = []
    legacy_values = []
    for component, mode in ((DRAIN, "drain"), (RELEASE, "release"), (ATTEST, "attest")):
        hook = one(docs, kind="Job", component=component)
        container, env = container_env(hook)
        assert container.get("command") == [
            "python",
            "-m",
            "curie_worker.upgrade_drain",
            "--mode",
            mode,
            f"--installation-id-observed={str(observed).lower()}",
        ], f"{component} does not carry the expected observed-identity argument"

        identity = unique_env(env, "CURIE_INSTALLATION_ID")
        revision = unique_env(env, "CURIE_UPGRADE_REVISION")
        legacy_entry = unique_env(env, "CURIE_UPGRADE_LEGACY_QUIESCE")
        assert set(identity) == {"name", "value"}, (
            f"{component} installation identity is not a render-time literal"
        )
        assert set(revision) == {"name", "value"}, (
            f"{component} upgrade revision is not a render-time literal"
        )
        assert set(legacy_entry) == {"name", "value"}, (
            f"{component} legacy compatibility bit is not a render-time literal"
        )
        assert isinstance(revision["value"], str) and revision["value"].isdecimal(), (
            f"{component} upgrade revision is not a decimal integer string"
        )
        assert int(revision["value"]) > 0, f"{component} upgrade revision is not positive"
        identities.append(identity["value"])
        revisions.append(revision["value"])
        legacy_values.append(legacy_entry["value"])

    assert identities == [installation_id, installation_id, installation_id], (
        "managed Secret and hook installation identities do not match within one render"
    )
    assert len(set(revisions)) == 1, (
        "drain, release, and attest hooks carry different revisions"
    )
    assert legacy_values == [legacy, legacy, legacy], (
        "drain, release, and attest hooks disagree on legacy compatibility"
    )
    return installation_id, managed_name, worker_env


default_docs = load(default_path)
first_id, _, _ = assert_identity_render(default_docs, observed=True, legacy="false")
second_id, _, _ = assert_identity_render(
    load(fresh_second_path), observed=True, legacy="false"
)
assert first_id != second_id, "two fresh installs reused one installation identity"

# `helm template --is-upgrade` has no live lookup result. It must remain a valid
# client-side render while marking the hook as unobserved so an executing drain
# can refuse before touching Valkey or allowing a rollout.
assert_identity_render(load(client_upgrade_path), observed=False, legacy="true")

# A store/model/adapter credential may come from an operator-owned Secret, but
# the installation identity always belongs to this release's managed Secret.
_, managed_name, byo_worker_env = assert_identity_render(
    load(byo_path), observed=True, legacy="false"
)
expected_credential_refs = {
    "POSTGRES_PASSWORD": "acme-postgres-credentials",
    "VALKEY_PASSWORD": "acme-valkey-credentials",
    "CURIE_CREDENTIALS": "acme-model-credentials",
    "CURIE_ADAPTER_CREDENTIALS": "acme-adapter-credentials",
    "CURIE_API_KEY": managed_name,
}
for env_name, expected_secret in expected_credential_refs.items():
    entry = unique_env(byo_worker_env, env_name)
    actual_secret = (
        ((entry.get("valueFrom") or {}).get("secretKeyRef") or {}).get("name")
    )
    assert actual_secret == expected_secret, (
        f"{env_name} did not use its expected credential Secret"
    )
installation_ref = (
    (
        unique_env(byo_worker_env, "CURIE_INSTALLATION_ID").get("valueFrom") or {}
    ).get("secretKeyRef")
    or {}
)
assert installation_ref.get("name") == managed_name, (
    "a BYO credential Secret substituted for the managed installation identity"
)

print(
    "OK: one memoized installation identity reaches the managed Secret, worker and "
    "the drain, release, and attest hooks; fresh installs rotate it; client-only "
    "upgrades are marked unobserved; BYO credential Secrets cannot replace it"
)
PY
