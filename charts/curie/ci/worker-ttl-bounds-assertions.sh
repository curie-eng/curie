#!/usr/bin/env bash
#
# Render-assertion test for the worker TTL/timeout bounds (issue #1388).
#
# `worker.routeTtlSeconds: 0` is valid YAML, renders cleanly, installs green and
# passes readiness -- and then fails on the FIRST message, because it reaches
# Valkey as `SET ... NX EX 0`, which raises `invalid expire time in 'set'
# command`. That exception is not classified by the kernel, so the turn hangs,
# the entry is re-delivered to dead-letter, and every attempt leaks a sandbox.
# `values.schema.json` makes helm refuse the value at install/template time so it
# never reaches worker env at all. Twelve assertions:
#
#   (a) POSITIVE, defaults: the render SUCCEEDS and the worker Deployment
#       carries the three env vars at their shipped defaults.
#   (b) POSITIVE, valid overrides: legitimate tuning (300 / 7200 / 45, the
#       literals #1382's own tests use) still reaches worker env. This is the
#       assertion that catches a schema so strict it refuses real operation.
#   (c) NEGATIVE, zero: each of the three knobs at 0 fails the render.
#   (d) NEGATIVE, negative: -5 fails the render, for each of the three knobs.
#   (e) NEGATIVE, over the documented maximum: MAX + 1 fails the render, for
#       each of the three knobs. MAX (31536000, one year in seconds) is
#       defined once below and every executable assertion below derives from
#       it; this header states the literal once and does not restate it.
#   (f) NEGATIVE, non-integer TTL: 3600.5 fails the render for routeTtlSeconds
#       and suspendedRouteTtlSeconds, proving `type: integer` is doing work. A
#       YAML float renders through `| quote` as "4.47597e+06" for larger values
#       -- the exact class documented at ci/github-app-credential-assertions.sh:11.
#       claimTimeoutSeconds is `type: number`, not `integer`, so the same
#       fractional shape is LEGAL there; (f) also pins that 45.5 renders and
#       reaches worker env unrefused, so the integer/number split stays real.
#   (g) BLAST RADIUS: every values combination helm-ci already exercises still
#       lints and renders clean with the schema present. A chart-root
#       values.schema.json is validated against the WHOLE coalesced values tree
#       on lint/template/install/upgrade, so an over-specified schema breaks
#       every install. This is the regression net for that.
#   (h) POSITIVE, at max: MAX renders, for each of the three knobs. The
#       accept side of the boundary. Without it a schema `maximum` that
#       drifted BELOW MAX would still satisfy (e) while the worker's
#       Python bound still accepted the value --
#       the cross-language divergence the paired literal exists to prevent. The
#       Python twin is test_substrate_config_accepts_exactly_the_documented_maximum
#       in apps/worker/tests/test_run.py.
#   (i) OBSERVED-BEHAVIOR PIN, explicit null: `--set worker.routeTtlSeconds=null`
#       exits 0 and renders CURIE_ROUTE_TTL_SECONDS PRESENT WITH AN EMPTY VALUE
#       (a bare `value:`, which the API server hands the container as "").
#       Helm drops nil keys during values coalescing, which happens BEFORE schema
#       validation, so no `type`, `exclusiveMinimum` or `not: {type: null}` can
#       ever reach this case. This path is closed only by the worker loader's
#       empty-string refusal, asserted from the other end by the "" case of
#       test_substrate_config_refuses_an_unparseable_ttl_naming_the_env_var. (i)
#       and that case are one seam read from two ends; deleting either removes
#       half of the only gate on the null path.
#   (j) ADR-0131 RELATIONSHIP, positive and negative: the rendered
#       terminationGracePeriodSeconds must cover the rendered
#       CURIE_DELIVERY_BUDGET_S + CURIE_DELIVERY_SHUTDOWN_RESERVE_S. The
#       positive raises budget and grace together and must still render+pass;
#       the negative raises the budget while leaving grace at its pre-ADR-0131
#       value of 1800 and must be REFUSED BY THE RENDER ITSELF, by the
#       `curie.worker.validateDrainBudget` guard in _helpers.tpl.
#       That guard is the only fence an operator actually hits: JSON Schema
#       cannot express cross-field arithmetic, so values.schema.json accepts
#       grace=1800, `helm upgrade` used to succeed, and the worker then
#       CrashLoopBackOffed because its `WorkerConfig` check raises before
#       `asyncio.run` and no supervisor can catch it -- a silent breaking
#       upgrade for any install that overrides grace. The fixed-value
#       assertion in (a) only ever sees 1860 at the default; this is the half
#       that actually exercises the inequality the ADR mandates, and the only
#       assertion that demonstrates the guard REJECTING something.
#   (k) RUNNER CEILING, positive and negative: the shipped worker env contains
#       CURIE_RUNNER_TOTAL_TIMEOUT_S=600 exactly once; 1700 under an 1800-second
#       delivery budget renders and reaches the worker; equality at 600, a
#       fractional 0.5-second ceiling, and the inclusive 1800 maximum render
#       with coherent delivery budgets. The schema refuses 0 and 1800.1,
#       proving the exclusive lower bound and inclusive upper bound. Finally,
#       the relationship guard refuses the individually schema-valid combination
#       runnerTotalTimeoutSeconds=1700 / deliveryBudgetSeconds=600 and names
#       both keys, values, inequality, and corrective actions in chart-owned
#       output. This is the cross-field negative JSON Schema cannot express.
#   (m) RETAINED extraEnv TIMEOUT, positive: a v0.8.4-era worker.extraEnv
#       override of CURIE_RUNNER_TOTAL_TIMEOUT_S=1700 must not duplicate the
#       first-class env. The rendered worker keeps exactly one copy at the
#       first-class default (600) and still emits a non-colliding extraEnv
#       entry. This is the 2026-09-04 soak: retained extraEnv plus first-class
#       timeout made Kubernetes reject the worker patch (#2097).
# SCHEMA WORDING IS NOT ASSERTED, AND MUST NOT BECOME ASSERTED. Every negative
# below EXCEPT the relationship negatives in (j) and (k) checks only (1)
# that helm exited non-zero and (2) that the captured output contains the bare
# knob name. The schema-bound negatives in (k) follow this generic rule too.
# The failure text comes from helm's own bundled JSON-Schema validator, whose
# wording changed between the CI-pinned helm and current helm while the
# pass/fail outcomes stayed identical:
#
#   helm 3.16.4 (the CI pin, .github/workflows/helm-ci.yaml:40):
#     - worker.routeTtlSeconds: Must be greater than 0
#     - worker.routeTtlSeconds: Invalid type. Expected: integer, given: string
#   helm 3.20.0:
#     - at '/worker/routeTtlSeconds': exclusiveMinimum: got 0, want 0
#     - at '/worker/routeTtlSeconds': got string, want integer
#
# The bare knob name is the only token common to both. Do NOT "improve" these
# into greps for `Must be greater than`, `exclusiveMinimum`, `Invalid type`,
# `want integer`, or the `- <path>: ` prefix shape: a wording-locked assertion
# passes on the author's machine and fails in CI, or the reverse, for a reason
# that has nothing to do with the chart.
#
# The relationship negatives in (j), (k), and (l) are the deliberate
# exceptions: their messages are CHART-OWNED text from `fail` in _helpers.tpl,
# not helm's validator, so they cannot drift with the helm version and
# asserting them is what proves each guard -- rather than some unrelated
# template error -- is what refused the render. They check stable
# operands/actions, not full sentences.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }
render() { helm template curie "$CHART" "$@" 2>&1; }

# Reads the worker Deployment's env list by NAME out of a render, rather than
# grepping: a value moving between containers, or a second container growing a
# same-named var, is invisible to a grep and caught here.
WORKER_ENV_PY="$(
  cat <<'PY'
import sys, yaml
want = sys.argv[2:]
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
deploys = [
    d for d in docs
    if d.get("kind") == "Deployment"
    and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/component") == "worker"
]
if len(deploys) != 1:
    raise SystemExit(f"expected exactly one worker Deployment, rendered {len(deploys)}")
containers = deploys[0]["spec"]["template"]["spec"]["containers"]
env = {}
counts = {}
for c in containers:
    for e in c.get("env", []):
        name = e["name"]
        counts[name] = counts.get(name, 0) + 1
        # Preserve this harness's historical last-value semantics for every
        # pre-existing env assertion. Only the newly introduced runner ceiling
        # has an exactly-once contract below.
        env[name] = e.get("value")
for pair in want:
    name, _, expected = pair.partition("=")
    if name not in env:
        raise SystemExit(f"{name} is not in the worker env (got {sorted(env)})")
    if name == "CURIE_RUNNER_TOTAL_TIMEOUT_S" and counts[name] != 1:
        raise SystemExit(
            f"{name} occurs {counts[name]} times in the worker env, expected exactly once"
        )
    got = env[name]
    if expected == "":
        # "present with an empty value". `nil | quote` emits a bare `value:`,
        # which parses as null and which Kubernetes hands the container as an
        # empty string; both spellings are the same thing to the worker.
        if got not in (None, ""):
            raise SystemExit(f"{name} rendered {got!r}, expected it present and empty")
    elif got != expected:
        raise SystemExit(f"{name} rendered {got!r}, expected {expected!r}")
PY
)"

WORKER_TERMINATION_GRACE_PY="$(
  cat <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
deploys = [
    d for d in docs
    if d.get("kind") == "Deployment"
    and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/component") == "worker"
]
if len(deploys) != 1:
    raise SystemExit(f"expected exactly one worker Deployment, rendered {len(deploys)}")
got = deploys[0]["spec"]["template"]["spec"].get("terminationGracePeriodSeconds")
if type(got) is not int or got != 1860:
    raise SystemExit(
        "worker terminationGracePeriodSeconds rendered "
        f"{got!r}, expected integer 1860"
    )
PY
)"

# ADR-0131 relationship: rendered grace must be >= rendered delivery budget +
# rendered shutdown reserve. The fixed-value check above catches a values-file
# drift; THIS catches an operator who raises the budget (or leaves an old grace
# override in place) without raising the grace to match -- the actual
# ADR-0131 misconfiguration, and the one the fixed check cannot see.
WORKER_GRACE_COVERS_BUDGET_PY="$(
  cat <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
deploys = [
    d for d in docs
    if d.get("kind") == "Deployment"
    and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/component") == "worker"
]
if len(deploys) != 1:
    raise SystemExit(f"expected exactly one worker Deployment, rendered {len(deploys)}")
spec = deploys[0]["spec"]["template"]["spec"]
grace = spec.get("terminationGracePeriodSeconds")
env = {e["name"]: e.get("value") for c in spec["containers"] for e in c.get("env", [])}
budget = int(env["CURIE_DELIVERY_BUDGET_S"])
reserve = int(env["CURIE_DELIVERY_SHUTDOWN_RESERVE_S"])
required = budget + reserve
if type(grace) is not int or grace < required:
    raise SystemExit(
        f"terminationGracePeriodSeconds ({grace!r}) does not cover "
        f"CURIE_DELIVERY_BUDGET_S + CURIE_DELIVERY_SHUTDOWN_RESERVE_S "
        f"({budget} + {reserve} = {required}): a draining worker would be "
        "SIGKILLed before it could settle"
    )
PY
)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Renders into a file and asserts the named env vars. Fails the assertion when
# the render itself fails, so a broken schema can never make a content check
# pass vacuously.
assert_env() {
  local letter="$1" out="$TMP/$1.yaml"
  shift
  local -a sets=()
  while [ "$1" != "--" ]; do sets+=("$1"); shift; done
  shift
  # `${sets[@]+"${sets[@]}"}` rather than a bare `"${sets[@]}"`: expanding an
  # EMPTY array under `set -u` is an "unbound variable" error until bash 4.4, and
  # macOS ships 3.2 as /bin/bash. Assertion (a) passes no --set at all, so `sets`
  # is empty on the very first call -- and because the EXIT trap's own status
  # replaces the script's in 3.2, the abort surfaced as exit 0 and chart-check
  # reported PASS for a run that asserted NOTHING.
  if ! helm template curie "$CHART" -s templates/worker.yaml ${sets[@]+"${sets[@]}"} >"$out" 2>&1; then
    fail "$letter" "the render FAILED; it must succeed
  $(head -5 "$out")"
  fi
  local msg
  if ! msg="$(python3 -c "$WORKER_ENV_PY" "$out" "$@" 2>&1)"; then
    fail "$letter" "$msg"
  fi
}

# Asserts a render fails AND that its output names the knob. Captured rather
# than piped: helm exits non-zero here by design, and under `set -o pipefail`
# a pipeline would fail even when the check succeeds.
assert_refused() {
  local letter="$1" knob="$2"
  shift 2
  local out
  if out="$(render "$@" 2>&1)"; then
    fail "$letter" "helm accepted $* -- the schema must refuse it"
  fi
  grep -q "$knob" <<<"$out" \
    || fail "$letter" "the refusal of $* does not name $knob
  $(head -5 <<<"$out")"
}

# (a) The DEFAULT render must SUCCEED before anything else is asserted. A schema
#     with a syntax error fails every render, which would make every negative
#     below pass for the wrong reason -- the exact trap
#     ci/api-ingress-assertions.sh:31-34 documents having fallen into. The
#     negatives below render the WHOLE chart (via `render()`, no `-s`), so the
#     guard must cover that same render shape, not just the worker template.
DEFAULT_RENDER="$TMP/default.yaml"
if ! helm template curie "$CHART" >"$DEFAULT_RENDER" 2>&1; then
  fail a "the full-chart default render FAILED; every negative below would pass for the wrong reason"
fi
if ! msg="$(python3 -c "$WORKER_TERMINATION_GRACE_PY" "$DEFAULT_RENDER" 2>&1)"; then
  fail a "$msg"
fi
if ! msg="$(python3 -c "$WORKER_GRACE_COVERS_BUDGET_PY" "$DEFAULT_RENDER" 2>&1)"; then
  fail a "$msg"
fi
assert_env a -- \
  CURIE_CLAIM_TIMEOUT_SECONDS=90 \
  CURIE_ROUTE_TTL_SECONDS=3600 \
  CURIE_SUSPENDED_ROUTE_TTL_SECONDS=86400 \
  CURIE_DELIVERY_BUDGET_S=600 \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=600 \
  CURIE_DELIVERY_LEASE_TTL_S=45 \
  CURIE_DELIVERY_LEASE_HEARTBEAT_S=10 \
  CURIE_DELIVERY_SHUTDOWN_RESERVE_S=60 \
  CURIE_TERMINATION_GRACE_PERIOD_S=1860

# (b)
assert_env b \
  --set worker.routeTtlSeconds=300 \
  --set worker.suspendedRouteTtlSeconds=7200 \
  --set worker.claimTimeoutSeconds=45 \
  -- \
  CURIE_CLAIM_TIMEOUT_SECONDS=45 \
  CURIE_ROUTE_TTL_SECONDS=300 \
  CURIE_SUSPENDED_ROUTE_TTL_SECONDS=7200

# The documented ceiling, one year in seconds. Defined once; every executable
# assertion below that touches the bound (c/d/e via the loop, h) derives from
# this variable rather than restating the literal.
MAX=31536000
OVER_MAX=$((MAX + 1))

# (c), (d), (e). All three knobs refuse 0, -5 and OVER_MAX identically, so the
# per-knob repetition carries no information; the loop keeps the coverage and
# makes an added knob a one-word edit. (f) below is NOT folded in with them,
# because its behaviour genuinely differs per knob.
for knob in routeTtlSeconds suspendedRouteTtlSeconds claimTimeoutSeconds; do
  assert_refused c "$knob" --set "worker.$knob=0"
  assert_refused d "$knob" --set "worker.$knob=-5"
  assert_refused e "$knob" --set "worker.$knob=$OVER_MAX"
done

# (f) `--set-json`, not `--set`: helm's `--set` parser hands 3600.5 to the
# schema as the STRING "3600.5", which every type here (integer or number)
# refuses -- that would pass this assertion even if `type: integer` were
# edited away, since a string never satisfies `type: number` either.
# `--set-json` delivers a genuine JSON number, so the refusal is actually
# about `type: integer` and not about `--set`'s own string coercion.
assert_refused f routeTtlSeconds --set-json worker.routeTtlSeconds=3600.5
assert_refused f suspendedRouteTtlSeconds --set-json worker.suspendedRouteTtlSeconds=3600.5
# claimTimeoutSeconds is `type: number`, so the same fractional JSON number is
# legal there.
assert_env f --set-json worker.claimTimeoutSeconds=45.5 -- CURIE_CLAIM_TIMEOUT_SECONDS=45.5

# (g) BLAST RADIUS.
helm lint "$CHART" >/dev/null 2>&1 \
  || fail g "helm lint on defaults broke with the schema present"
helm lint "$CHART" -f "$CHART/values-dev.yaml" >/dev/null 2>&1 \
  || fail g "helm lint -f values-dev.yaml broke with the schema present"
helm lint "$CHART" -f "$CHART/values-dev.yaml" -f "$CHART/values-e2e-nogvisor.yaml" \
  >/dev/null 2>&1 \
  || fail g "helm lint -f values-dev.yaml -f values-e2e-nogvisor.yaml broke"
helm template curie "$CHART" -f "$CHART/values-e2e-harness.yaml" >/dev/null 2>&1 \
  || fail g "helm template -f values-e2e-harness.yaml broke with the schema present"
# api.githubAppId is an integer in values.yaml and a STRING when the CLI sets it
# (cli/src/github_app.rs:52 uses --set-string). Typing it either way breaks one
# of the two paths, so the schema must leave it untyped; this proves it did.
helm template curie "$CHART" \
  --set-string api.githubAppId=1234567 \
  --set api.githubAppPrivateKey=X >/dev/null 2>&1 \
  || fail g "the --set-string api.githubAppId path broke; the schema over-typed it"

# (h) One chart key paired with the env var it must reach, so the maximum
# appears once per side of the render rather than six times.
for pair in \
  routeTtlSeconds:CURIE_ROUTE_TTL_SECONDS \
  suspendedRouteTtlSeconds:CURIE_SUSPENDED_ROUTE_TTL_SECONDS \
  claimTimeoutSeconds:CURIE_CLAIM_TIMEOUT_SECONDS; do
  assert_env h --set "worker.${pair%%:*}=$MAX" -- "${pair##*:}=$MAX"
done

# (i)
assert_env i --set worker.routeTtlSeconds=null -- CURIE_ROUTE_TTL_SECONDS=

# (j) ADR-0131 grace/budget relationship, POSITIVE and NEGATIVE. This is the
# important half of the assertion, not a formality -- a render assertion that
# only ever sees the default values is vacuous. The negative reproduces the
# actual ADR-0131 misconfiguration: an operator raises the delivery budget to
# 1800 but leaves an old terminationGracePeriodSeconds override (1800, the
# pre-ADR-0131 value) in place, so grace no longer covers budget + reserve
# (1800 + 60 = 1860 > 1800). That combination used to render clean and kill the
# worker at boot; it is now refused at render time by
# `curie.worker.validateDrainBudget`, so the negative asserts the REFUSAL and
# its message rather than inspecting a rendered manifest.
J_POSITIVE="$TMP/j-positive.yaml"
if ! helm template curie "$CHART" \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 \
  >"$J_POSITIVE" 2>&1; then
  fail j "the render FAILED on a raised budget with a matching grace; it must succeed
  $(head -5 "$J_POSITIVE")"
fi
if ! msg="$(python3 -c "$WORKER_GRACE_COVERS_BUDGET_PY" "$J_POSITIVE" 2>&1)"; then
  fail j "$msg"
fi

# The NEGATIVE control. Captured, not piped: helm exits non-zero by design here
# and `set -o pipefail` would abort the script on the very outcome being
# asserted.
J_NEGATIVE_OUT=""
if J_NEGATIVE_OUT="$(helm template curie "$CHART" \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1800 2>&1)"; then
  fail j "helm ACCEPTED deliveryBudgetSeconds=1800 with terminationGracePeriodSeconds=1800 (grace 1800 < required 1860). That render must be refused: it upgrades clean and then CrashLoopBackOffs the worker on the boot validator"
fi
# The three key names and the arithmetic, checked one token at a time so the
# assertion says WHICH part of the message went missing. Chart-owned wording,
# not helm's -- see the header note above.
for token in \
  "worker.terminationGracePeriodSeconds" \
  "worker.deliveryBudgetSeconds" \
  "worker.deliveryShutdownReserveSeconds" \
  "(1800) + " \
  "(60) = 1860" \
  "ADR-0131"; do
  grep -qF "$token" <<<"$J_NEGATIVE_OUT" \
    || fail j "the refusal does not mention $token; an operator reading it during an upgrade cannot tell which value to raise
  $(head -3 <<<"$J_NEGATIVE_OUT")"
done

# (k) Runner per-request ceiling. The default is asserted in (a), including
# exactly-once env placement. Exercise an operational override, equality, a
# legal fractional ceiling, and the inclusive schema maximum with compatible
# delivery budgets. The 1800 case also needs the matching ADR-0131 termination
# grace.
assert_env k \
  --set worker.runnerTotalTimeoutSeconds=1700 \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 \
  -- \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=1700 \
  CURIE_DELIVERY_BUDGET_S=1800
assert_env k \
  --set worker.runnerTotalTimeoutSeconds=600 \
  --set worker.deliveryBudgetSeconds=600 \
  -- \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=600 \
  CURIE_DELIVERY_BUDGET_S=600
assert_env k \
  --set-json worker.runnerTotalTimeoutSeconds=0.5 \
  --set worker.deliveryBudgetSeconds=60 \
  -- \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=0.5 \
  CURIE_DELIVERY_BUDGET_S=60
assert_env k \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 \
  -- \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=1800 \
  CURIE_DELIVERY_BUDGET_S=1800

# Disable both resource templates so these failures isolate JSON Schema rather
# than either cross-field guard. Use --set-json for 1800.1 so the schema sees a
# genuine number and the refusal proves the inclusive maximum.
assert_refused k runnerTotalTimeoutSeconds \
  --set worker.deploy=false \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set worker.runnerTotalTimeoutSeconds=0
assert_refused k runnerTotalTimeoutSeconds \
  --set worker.deploy=false \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set-json worker.runnerTotalTimeoutSeconds=1800.1

# Individually valid scalar values, relationally invalid together. Capture the
# result directly so Helm's required non-zero status is the asserted outcome.
K_RELATIONSHIP_OUT=""
if K_RELATIONSHIP_OUT="$(helm template curie "$CHART" \
  --set worker.runnerTotalTimeoutSeconds=1700 \
  --set worker.deliveryBudgetSeconds=600 2>&1)"; then
  fail k "helm ACCEPTED worker.runnerTotalTimeoutSeconds=1700 above worker.deliveryBudgetSeconds=600; the render must refuse a runner ceiling that exceeds the overall delivery budget"
fi
for token in \
  "worker.runnerTotalTimeoutSeconds" \
  "(1700)" \
  "worker.deliveryBudgetSeconds" \
  "(600)" \
  "<=" \
  "lower" \
  "raise"; do
  grep -qF "$token" <<<"$K_RELATIONSHIP_OUT" \
    || fail k "the runner/delivery refusal does not mention $token; an operator cannot identify and correct the invalid timeout relationship
  $(head -3 <<<"$K_RELATIONSHIP_OUT")"
done

# (m) Retained extraEnv must not duplicate the first-class runner timeout.
# --set-json so Helm sees a real list rather than a stringified overlay.
assert_env m \
  --set-json 'worker.extraEnv=[{"name":"CURIE_RUNNER_TOTAL_TIMEOUT_S","value":"1700"},{"name":"CURIE_UPGRADE_FIXTURE","value":"kept"}]' \
  -- \
  CURIE_RUNNER_TOTAL_TIMEOUT_S=600 \
  CURIE_UPGRADE_FIXTURE=kept

echo "worker-ttl-bounds-assertions: all twelve assertions passed"
