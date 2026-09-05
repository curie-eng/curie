#!/usr/bin/env bash
#
# Render contract: on every rendered container that carries BOTH probes, the
# livenessProbe's earliest failure cutoff must be strictly LATER than the
# readinessProbe's.
#
# WHY. The two probes have different jobs. Readiness says "this container may
# legitimately take this long to become useful -- keep it out of the Service
# until then." Liveness says "past this point the process is wedged -- kill it."
# When the liveness cutoff lands INSIDE the window readiness was configured to
# tolerate, the kubelet restarts a container that is still doing exactly what
# readiness was sized to wait for, and the restart re-enters the same slow boot:
#
#   * postgres replaying WAL after an unclean shutdown (`pg_isready` returns
#     non-zero for the whole of recovery),
#   * langfuse-web / langfuse-worker running Prisma and ClickHouse boot
#     migrations at container start (init containers do NOT re-run on a liveness
#     restart, so the restart lands straight back in the migration),
#   * api warming up before `/health` answers.
#
# The failure is invisible in review and invisible at install time: the manifest
# is valid, `helm install` is green, and the damage shows up minutes later as a
# `Killing` event and a BackOff loop under load. So the proof surface is the
# RENDER, and this script is it.
#
# The four containers that were broken on origin/main when this assertion was
# written, readiness cutoff vs liveness cutoff:
#
#   postgres         60 s vs  40 s   (and no explicit liveness timeoutSeconds,
#                                     so the kubelet's 1 s default applies to a
#                                     `pg_isready` exec that can exceed 1 s on a
#                                     contended node during WAL replay -- every
#                                     such timeout counts as a probe failure, so
#                                     the effective cutoff is even shorter)
#   langfuse-web    310 s vs 190 s
#   langfuse-worker 310 s vs 190 s
#   api             120 s vs 105 s
#
# WHAT THIS PINS. Deliberately the CLASS, not those four names. This script
# walks every object carrying a pod spec in both renders -- Deployment,
# StatefulSet, DaemonSet, Job, and CronJob's spec.template.spec; a bare Pod's
# own spec (templates/security-probe.yaml); SandboxTemplate's
# spec.podTemplate.spec (templates/agent-sandbox.yaml); and, generically, any
# other kind exposing the conventional spec.template.spec shape -- and checks
# every container that has both probes, so a fifth violating container added
# later fails CI with no list here to update -- the lesson
# `charts/curie/CLAUDE.md` already records for the `curie.env.otel` membership
# boundary (#2331). There is no allowlist and no named-container table below
# on purpose.
#
# THE FORMULA, matching the chart's own precedent at `templates/dispatcher.yaml`
# lines 3-9:
#
#     earliest_failure_cutoff = initialDelaySeconds + (failureThreshold - 1) * periodSeconds
#
# Omitted keys take the kubelet's defaults -- `initialDelaySeconds: 0`,
# `periodSeconds: 10`, `failureThreshold: 3`, `timeoutSeconds: 1` -- rather than
# raising a KeyError. Three of the four broken blocks above omit at least one of
# these keys, and a crash on a missing key reads as a script bug and gets the
# assertion weakened instead of the chart fixed.
#
# TWO SUPPORTING RULES.
#
#   1. Every livenessProbe must declare `timeoutSeconds` explicitly. The
#      kubelet's 1 s default is invisible in the manifest and is the silent half
#      of the postgres case above.
#   2. `postgres` liveness `timeoutSeconds` must be at least 5. `pg_isready`
#      under WAL replay on a contended node is not a 1 s operation.
#
# NOT CHECKED, deliberately: containers carrying only one probe (the invariant
# is a relation BETWEEN two probes -- `agentSandbox.runner` has readiness and no
# liveness by design), and `startupProbe`, which defers readiness and liveness
# equally and so leaves the comparison unchanged. Containers behind a
# `deploy: false` toggle (`dispatcher`, `mail-adapter`, `inference`) render in
# neither of the two renders below and are therefore outside this script's
# reach; widening to profiled renders is a follow-up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

if ! helm template curie "$CHART" >"$TMP/default.yaml"; then
  fail "default render failed"
fi
if ! helm template curie "$CHART" -f "$CHART/values-dev.yaml" >"$TMP/dev.yaml"; then
  fail "values-dev.yaml render failed"
fi

# ------------------------------------------------------------------ the checker
# ONE program, reused by every positive and negative run below: it takes a
# rendered manifest path plus a label, and it collects ALL problems across the
# whole render before raising, so a run reports every violating container at
# once instead of hiding the second one behind the first.
cat >"$TMP/check.py" <<'PY'
import sys
import yaml

# Kubelet defaults for omitted probe keys (k8s core/v1 Probe).
KUBELET_DEFAULTS = {
    "initialDelaySeconds": 0,
    "periodSeconds": 10,
    "failureThreshold": 3,
    "timeoutSeconds": 1,
}
FORMULA = (
    "formula: earliest_failure_cutoff = initialDelaySeconds + "
    "(failureThreshold - 1) * periodSeconds, with kubelet defaults "
    "initialDelaySeconds=0 periodSeconds=10 failureThreshold=3 timeoutSeconds=1 "
    "applied to omitted keys"
)
POD_TEMPLATE_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "Job")

problems = []


def cadence(probe, where):
    """Probe cadence with kubelet defaults filled in for omitted keys."""
    resolved = {}
    for key, default in KUBELET_DEFAULTS.items():
        value = probe.get(key, default)
        try:
            resolved[key] = int(value)
        except (TypeError, ValueError):
            problems.append(
                f"[{label}] {where}: {key} is {value!r}, which is not an integer; "
                f"cannot compute a cutoff from it"
            )
            resolved[key] = default
    return resolved


def cutoff(c):
    return c["initialDelaySeconds"] + (c["failureThreshold"] - 1) * c["periodSeconds"]


def cadence_str(c):
    return (
        f"{c['initialDelaySeconds']}/{c['periodSeconds']}/"
        f"{c['timeoutSeconds']}/{c['failureThreshold']}"
    )


def pod_templates(doc):
    kind = doc.get("kind")
    name = doc.get("metadata", {}).get("name", "<unnamed>")
    if kind in POD_TEMPLATE_KINDS:
        template = doc.get("spec", {}).get("template")
    elif kind == "CronJob":
        template = (
            doc.get("spec", {}).get("jobTemplate", {}).get("spec", {}).get("template")
        )
    elif kind == "Pod":
        # A bare Pod (templates/security-probe.yaml) has no wrapping
        # template -- the document's own `spec` IS the pod spec, so hand the
        # whole document through: callers read `template["spec"]`, which is
        # exactly this Pod's spec.
        template = doc
    elif kind == "SandboxTemplate":
        # extensions.agents.x-k8s.io SandboxTemplate (templates/agent-sandbox.yaml)
        # carries its pod spec at spec.podTemplate.spec, not spec.template.spec,
        # so `podTemplate` itself is the "template" callers expect.
        template = doc.get("spec", {}).get("podTemplate")
    else:
        # Generic fallback: any other object carrying a conventional pod
        # template at spec.template.spec (the same shape Deployment /
        # StatefulSet / DaemonSet / Job use) is walked too, so a future
        # workload kind is covered with no name added here.
        template = doc.get("spec", {}).get("template")
    if not isinstance(template, dict):
        return []
    return [(kind, name, template)]


if __name__ == "__main__":
    render, label = sys.argv[1], sys.argv[2]

    docs = [doc for doc in yaml.safe_load_all(open(render)) if isinstance(doc, dict)]
    if not docs:
        raise SystemExit(f"[{label}] {render} rendered no documents at all")

    checked = 0
    liveness_seen = 0
    for doc in docs:
        for kind, name, template in pod_templates(doc):
            spec = template.get("spec") or {}
            containers = list(spec.get("containers") or []) + list(
                spec.get("initContainers") or []
            )
            for container in containers:
                if not isinstance(container, dict):
                    continue
                cname = container.get("name", "<unnamed>")
                where = f"{kind}/{name} container={cname}"
                readiness = container.get("readinessProbe")
                liveness = container.get("livenessProbe")

                if isinstance(liveness, dict):
                    liveness_seen += 1
                    # Rule 1: the kubelet's 1s timeoutSeconds default is invisible in
                    # the manifest, and each timed-out probe counts as a failure.
                    if "timeoutSeconds" not in liveness:
                        problems.append(
                            f"[{label}] {where}: livenessProbe does not declare "
                            f"timeoutSeconds explicitly -- the kubelet then applies its 1s "
                            f"default, which is invisible in the manifest, and every timed-out "
                            f"probe counts as a failure, so the container's real failure "
                            f"cutoff is shorter than the numbers on the page suggest"
                        )
                    # Rule 2: pg_isready during WAL replay on a contended node is not
                    # a 1s operation.
                    elif cname == "postgres":
                        timeout = liveness.get("timeoutSeconds")
                        if not isinstance(timeout, int) or timeout < 5:
                            problems.append(
                                f"[{label}] {where}: postgres liveness timeoutSeconds must be "
                                f"at least 5, got {timeout!r} -- the liveness handler is an "
                                f"exec of pg_isready, which under WAL replay on a contended "
                                f"node routinely exceeds a short timeout, and every timeout "
                                f"counts as a probe failure"
                            )

                # The ordering invariant is a relation BETWEEN two probes: a
                # container with only one, or neither, is skipped silently.
                if not isinstance(readiness, dict) or not isinstance(liveness, dict):
                    continue

                rcad = cadence(readiness, f"{where} readinessProbe")
                lcad = cadence(liveness, f"{where} livenessProbe")
                rcut = cutoff(rcad)
                lcut = cutoff(lcad)
                checked += 1

                if lcut > rcut:
                    print(f"  ok: {kind}/{name}/{cname} ready={rcut}s live={lcut}s")
                    continue

                problems.append(
                    f"[{label}] {where}: liveness cutoff must exceed readiness cutoff, "
                    f"got liveness {lcut}s vs readiness {rcut}s. "
                    f"readiness i/p/t/f={cadence_str(rcad)}, "
                    f"liveness i/p/t/f={cadence_str(lcad)}. {FORMULA}. "
                    f"A liveness probe that gives up before readiness has stopped "
                    f"tolerating a slow boot restarts a container that is still "
                    f"legitimately booting, and the restart re-enters the same boot path"
                )

    print(
        f"  {label}: {checked} container(s) carry both probes and were checked for "
        f"ordering; {liveness_seen} livenessProbe(s) checked for an explicit timeoutSeconds"
    )

    if problems:
        raise SystemExit(
            f"{len(problems)} probe-window problem(s) in the {label} render:\n  - "
            + "\n  - ".join(problems)
        )
PY

python3 "$TMP/check.py" "$TMP/default.yaml" "default"
python3 "$TMP/check.py" "$TMP/dev.yaml" "values-dev"

# --------------------------------------------------------------- red controls
# Each control mutates a REAL default render and proves the checker rejects it
# for the INTENDED reason (needle-matched), not an incidental KeyError. Every
# mutation asserts its target container was actually found, so a container
# rename cannot quietly turn a control into a no-op.
cat >"$TMP/mutate.py" <<'PY'
import os
import sys
import yaml

sys.path.insert(0, os.path.dirname(__file__))
from check import pod_templates

source, target, mutation = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if isinstance(doc, dict)]


def containers():
    for doc in docs:
        for _kind, _name, template in pod_templates(doc):
            spec = template.get("spec") or {}
            for container in list(spec.get("containers") or []) + list(
                spec.get("initContainers") or []
            ):
                if isinstance(container, dict):
                    yield container


def find(name, probe):
    hits = [c for c in containers() if c.get("name") == name and probe in c]
    if not hits:
        raise SystemExit(
            f"mutation {mutation!r} found no container named {name!r} carrying a {probe} "
            f"in the render -- the red control would silently prove nothing, so this is "
            f"a hard failure, not a skip"
        )
    return hits


if mutation == "web-liveness-kubelet-defaults":
    # Cutoff collapses to the kubelet default 0 + (3-1)*10 = 20s, far inside
    # langfuse-web's readiness tolerance.
    for c in find("langfuse-web", "livenessProbe"):
        for key in ("initialDelaySeconds", "periodSeconds", "failureThreshold"):
            c["livenessProbe"].pop(key, None)
elif mutation == "api-drop-liveness-timeout":
    for c in find("api", "livenessProbe"):
        c["livenessProbe"].pop("timeoutSeconds", None)
elif mutation == "ui-readiness-threshold":
    for c in find("ui", "readinessProbe"):
        c["readinessProbe"]["failureThreshold"] = 1000
elif mutation == "postgres-liveness-timeout-1":
    for c in find("postgres", "livenessProbe"):
        c["livenessProbe"]["timeoutSeconds"] = 1
else:
    raise SystemExit(f"unknown mutation {mutation!r}")

with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

# assert_rejected <render> <label> <description> <needle>...
# Fails loudly if the checker PASSES, and fails loudly if it goes red without
# every needle -- a non-zero exit on its own proves nothing here, because on an
# unfixed chart every render is red for several reasons at once.
assert_rejected() {
  local render="$1" label="$2" description="$3"
  shift 3
  local output rc needle
  set +e
  output="$(python3 "$TMP/check.py" "$render" "$label" 2>&1)"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "negative control passed: $description"
  for needle in "$@"; do
    [[ "$output" == *"$needle"* ]] \
      || fail "$description was rejected without the expected reason ($needle): $output"
  done
  echo "  ok: $description is rejected"
}

assert_mutation_rejected() {
  local mutation="$1" description="$2"
  shift 2
  python3 "$TMP/mutate.py" "$TMP/default.yaml" "$TMP/$mutation.yaml" "$mutation"
  assert_rejected "$TMP/$mutation.yaml" "red:$mutation" "$description" "$@"
}

assert_mutation_rejected web-liveness-kubelet-defaults \
  "letting langfuse-web's liveness cadence fall back to the kubelet defaults (cutoff 20s, far inside its readiness tolerance)" \
  "container=langfuse-web" "liveness cutoff must exceed readiness cutoff"

assert_mutation_rejected api-drop-liveness-timeout \
  "dropping the api livenessProbe timeoutSeconds (the kubelet would silently apply 1s)" \
  "container=api" "does not declare timeoutSeconds explicitly"

assert_mutation_rejected ui-readiness-threshold \
  "widening the ui readiness failureThreshold to 1000 so its readiness tolerance outruns liveness" \
  "container=ui" "liveness cutoff must exceed readiness cutoff"

assert_mutation_rejected postgres-liveness-timeout-1 \
  "setting the postgres liveness timeoutSeconds to 1 (pg_isready under WAL replay is not a 1s operation)" \
  "container=postgres" "postgres liveness timeoutSeconds must be at least 5"

# ------------------------------------------------- values-override red control
# This one does NOT mutate a render -- it renders with overrides and asserts
# the OVERRIDES reach the manifest. It is what proves EACH of the four
# values-driven liveness blocks actually READS its own values key, rather than
# carrying a hardcoded, ordered set of numbers that happens to satisfy the
# invariant on its own: a single postgres-only override cannot tell "reads
# values" apart from "coincidentally ordered hardcoded numbers" on
# langfuse-web, langfuse-worker, or api, so this renders ONE chart with all
# four overrides at once and requires the checker to go red naming every one
# of the four containers by name.
#
# NOTE: on the pre-fix templates these values keys do not exist yet, so the
# overrides are inert and the render is red anyway from the shipped cadence.
# This control therefore cannot distinguish anything until the template edits
# land -- which is intended. The whole script is the failing-test contract: it
# is expected RED against the unmodified chart and green only once the
# cadences are values-driven and correctly ordered.
if ! helm template curie "$CHART" \
  --set postgres.livenessProbe.failureThreshold=3 \
  --set postgres.livenessProbe.timeoutSeconds=5 \
  --set langfuse.web.livenessProbe.failureThreshold=1 \
  --set langfuse.worker.livenessProbe.failureThreshold=1 \
  --set api.livenessProbe.failureThreshold=1 \
  >"$TMP/values-override.yaml"; then
  fail "values-override render failed"
fi
assert_rejected "$TMP/values-override.yaml" "red:values-override" \
  "overriding postgres, langfuse-web, langfuse-worker and api's livenessProbe values keys at once (proves each template reads its own values key)" \
  "container=postgres" "container=langfuse-web" "container=langfuse-worker" "container=api" \
  "liveness cutoff must exceed readiness cutoff"

echo "PASS: in both the default and values-dev renders, every object carrying a pod spec (Deployment/StatefulSet/DaemonSet/Job/CronJob, a bare Pod, SandboxTemplate, or any kind exposing spec.template.spec) has every container carrying BOTH probes checked, and each such container has a liveness failure cutoff strictly later than its readiness cutoff, every livenessProbe declares timeoutSeconds explicitly, and postgres liveness allows pg_isready at least 5s -- so the kubelet cannot restart a container that is still inside the boot window readiness was sized to tolerate"
