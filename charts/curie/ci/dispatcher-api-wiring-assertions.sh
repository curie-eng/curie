#!/usr/bin/env bash
#
# Render-assertion test for the dispatcher's platform-API wiring (#442) and the
# UI's API upstream (#2316). Proves, with `helm template` alone (no cluster),
# that the dispatcher Deployment is told where the API is and how to authenticate
# to it, and that the UI's nginx CURIE_API_TARGET uses the same helper. Unwired,
# the dispatcher falls back to its code default http://localhost:8000, which
# inside its own pod is the dispatcher itself, and every Slack Approve click
# dead-ends with only a warning. The UI's probes hit nginx `/` rather than the
# upstream, so an unwired BYO install stays Ready while every /api/ request
# fails at the proxy -- that path must fail the render closed.
#
#   1. Default install renders CURIE_API_URL as the in-chart API Service
#      (http://<fullname>-api:<api.service.port>), asserted as a VALUE.
#   2. The port tracks .Values.api.service.port and is not hardcoded.
#   3. dispatcher.apiBaseUrl overrides verbatim (the BYO / api.deploy=false path).
#   4. CURIE_API_KEY arrives by secretKeyRef to the chart Secret's `apiKey`
#      key, never as an inline literal (which would land the credential in
#      `helm get manifest` and in any rendered artifact CI uploads).
#   5. The app preflight timeout defaults to 120s, renders exactly once, and an
#      operator override reaches CURIE_API_PREFLIGHT_TIMEOUT_SECONDS.
#   6. A dispatcher-only startup probe uses the same heartbeat command and its
#      earliest fast-failure cutoff is strictly later than the app deadline.
#      The existing readiness/liveness probes remain unchanged, including on the
#      worker, which must not receive a startup probe.
#   7. A token-less install still renders no dispatcher at all (unchanged gate).
#   8. Default install renders the UI's CURIE_API_TARGET as the in-chart API
#      Service; the port tracks .Values.api.service.port.
#   9. ui.apiBaseUrl overrides CURIE_API_TARGET verbatim when api.deploy=false
#      (the BYO path). An empty override with api.deploy=false fails the render
#      and names ui.apiBaseUrl. ui.deploy=false still renders the rest of the
#      chart without that override.
#
# NOTE ON `--output-dir`: the sibling scripts in this directory capture
# `helm template` through command substitution. Do NOT copy that here, and do not
# "fix" this script back to a pipe. In this environment `helm template` into a
# stdout pipe has been observed to truncate silently at ~41 lines while still
# exiting 0, which turns a rendered-fine env var into a reported-absent FALSE
# NEGATIVE (and could equally report one present by luck). Rendering to a
# directory and reading the written file is the only trustworthy form here.
#
# Fails loudly, naming the assertion. Runnable locally and from CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

# The dispatcher template is gated on both Slack tokens being set
# (curie.dispatcher.enabled), so every render that expects a dispatcher must
# supply them.
TOKENS=(
  --set dispatcher.slack.appToken=xapp-assert
  --set dispatcher.slack.botToken=xoxb-assert
)

# Render to a directory and echo the path of the dispatcher Deployment manifest.
render_dispatcher() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template curie "$CHART" --output-dir "$out" "$@" >/dev/null
  local manifest="$out/curie/templates/dispatcher.yaml"
  [ -f "$manifest" ] || fail "$name: dispatcher.yaml did not render at all"
  echo "$manifest"
}

# Same --output-dir convention for the UI Deployment. Do not capture helm
# template through a pipe; see the header note.
render_ui() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template curie "$CHART" --output-dir "$out" "$@" >/dev/null
  local manifest="$out/curie/templates/ui.yaml"
  [ -f "$manifest" ] || fail "$name: ui.yaml did not render at all"
  echo "$manifest"
}

# Read one env entry out of the rendered dispatcher container structurally, via
# PyYAML -- the same convention as read_key() in render-assertions.sh and the
# ASSERT_PY pass in controller-rbac-assertions.sh. Deliberately NOT grep/awk over
# the text: a line-oriented state machine silently mis-reads a requoted value, a
# reordered key, or a `valueFrom` that renders before `value`, and it fails as a
# FALSE PASS -- exactly the class this script's header warns about.
#
#   env_value  <manifest> <name>   -> the entry's `value`, empty if absent
#   env_field  <manifest> <name> <dotted-path>
#                                  -> a nested field (e.g. valueFrom.secretKeyRef.key)
#   env_has    <manifest> <name> <dotted-path>
#                                  -> exit 0 if the path exists, 1 if not
ENV_PY="$TMP/env.py"
cat > "$ENV_PY" <<'PY'
import sys, yaml

manifest, name = sys.argv[1], sys.argv[2]
path = sys.argv[3] if len(sys.argv) > 3 else "value"

with open(manifest) as f:
    docs = [d for d in yaml.safe_load_all(f) if d]

entries = [
    e
    for d in docs
    if d.get("kind") == "Deployment"
    for c in ((d.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers") or []
    for e in (c.get("env") or [])
    if e.get("name") == name
]
if not entries:
    sys.exit(1)
if len(entries) > 1:
    sys.stderr.write("env %r appears %d times in the dispatcher\n" % (name, len(entries)))
    sys.exit(2)

node = entries[0]
for part in path.split("."):
    if not isinstance(node, dict) or part not in node:
        sys.exit(1)
    node = node[part]
sys.stdout.write(str(node))
PY

env_value() { python3 "$ENV_PY" "$1" "$2" || true; }
env_field() { python3 "$ENV_PY" "$1" "$2" "$3" || true; }
env_has() { python3 "$ENV_PY" "$1" "$2" "$3" >/dev/null; }

# Structural workload reader for probe and env cardinality assertions. Every
# lookup resolves the named Deployment container first, so a value copied into
# an annotation/comment or a sibling container cannot create a false pass.
WORKLOAD_PY="$TMP/workload.py"
cat > "$WORKLOAD_PY" <<'PY'
import json
import sys
import yaml

manifest, container_name, operation = sys.argv[1:4]

with open(manifest) as f:
    docs = [d for d in yaml.safe_load_all(f) if d]

deployments = [d for d in docs if d.get("kind") == "Deployment"]
if len(deployments) != 1:
    sys.stderr.write("expected exactly one Deployment, found %d\n" % len(deployments))
    sys.exit(2)

containers = (
    (((deployments[0].get("spec") or {}).get("template") or {}).get("spec") or {})
    .get("containers")
    or []
)
containers = [c for c in containers if c.get("name") == container_name]
if len(containers) != 1:
    sys.stderr.write(
        "container %r appears %d times in Deployment\n"
        % (container_name, len(containers))
    )
    sys.exit(2)
container = containers[0]

if operation == "env-count":
    name = sys.argv[4]
    print(sum(1 for entry in container.get("env") or [] if entry.get("name") == name))
    sys.exit(0)

if operation == "env-value":
    name = sys.argv[4]
    entries = [entry for entry in container.get("env") or [] if entry.get("name") == name]
    if len(entries) != 1 or "value" not in entries[0]:
        sys.exit(1)
    print(entries[0]["value"], end="")
    sys.exit(0)

probe_name = sys.argv[4]
if probe_name not in container:
    sys.exit(1)
node = container[probe_name]

if operation == "probe-has":
    sys.exit(0)
if operation == "probe-json":
    print(json.dumps(node, sort_keys=True, separators=(",", ":")), end="")
    sys.exit(0)
if operation == "probe-field":
    for part in sys.argv[5].split("."):
        if not isinstance(node, dict) or part not in node:
            sys.exit(1)
        node = node[part]
    if isinstance(node, (dict, list)):
        print(json.dumps(node, sort_keys=True, separators=(",", ":")), end="")
    else:
        print(node, end="")
    sys.exit(0)

sys.stderr.write("unknown operation %r\n" % operation)
sys.exit(2)
PY

env_count() { python3 "$WORKLOAD_PY" "$1" "$2" env-count "$3"; }
workload_env_value() { python3 "$WORKLOAD_PY" "$1" "$2" env-value "$3"; }
probe_has() { python3 "$WORKLOAD_PY" "$1" "$2" probe-has "$3" >/dev/null; }
probe_json() { python3 "$WORKLOAD_PY" "$1" "$2" probe-json "$3"; }
probe_field() { python3 "$WORKLOAD_PY" "$1" "$2" probe-field "$3" "$4"; }

assert_probe_field() {
  local manifest="$1" container="$2" probe="$3" field="$4" expected="$5" label="$6"
  local actual
  actual="$(probe_field "$manifest" "$container" "$probe" "$field")" \
    || fail "$label: $probe.$field is absent"
  [ "$actual" = "$expected" ] \
    || fail "$label: $probe.$field is '$actual', expected '$expected'"
}

# Return success only when Kubernetes cannot declare startup failed before the
# app exhausts the API-health budget, the fresh discovery/Slack budget, and a
# final already-started two-second Slack call. Kept as one reusable predicate so
# the under-budget negative control below proves the assertion is falsifiable.
probe_budget_is_safe() {
  local manifest="$1" container="$2"
  local app_timeout initial_delay period failure_threshold
  app_timeout="$(workload_env_value "$manifest" "$container" CURIE_API_PREFLIGHT_TIMEOUT_SECONDS)" || return 1
  initial_delay="$(probe_field "$manifest" "$container" startupProbe initialDelaySeconds)" || return 1
  period="$(probe_field "$manifest" "$container" startupProbe periodSeconds)" || return 1
  failure_threshold="$(probe_field "$manifest" "$container" startupProbe failureThreshold)" || return 1
  python3 - "$app_timeout" "$initial_delay" "$period" "$failure_threshold" <<'PY'
import sys

app_timeout, initial_delay, period, failure_threshold = map(float, sys.argv[1:])
earliest_failure = initial_delay + (failure_threshold - 1) * period
application_envelope = 2 * app_timeout + 2
sys.exit(0 if earliest_failure > application_envelope else 1)
PY
}

# 1: default install renders the in-chart API Service name and port as a value.
# This render is reused by assertion 4 below (identical arguments), so it is
# deliberately kept in its own variable rather than re-rendered.
default_manifest="$(render_dispatcher default "${TOKENS[@]}")"
actual="$(env_value "$default_manifest" CURIE_API_URL)"
[ -n "$actual" ] \
  || fail "default install: dispatcher has no CURIE_API_URL env value; it will fall back to http://localhost:8000 (itself) and Slack approval clicks will dead-end"
[ "$actual" = "http://curie-api:8000" ] \
  || fail "default install: CURIE_API_URL is '$actual', expected 'http://curie-api:8000' (the in-chart API Service)"

# 2: the port comes from .Values.api.service.port, not a hardcoded 8000.
manifest="$(render_dispatcher port --set api.service.port=9999 "${TOKENS[@]}")"
actual="$(env_value "$manifest" CURIE_API_URL)"
[ "$actual" = "http://curie-api:9999" ] \
  || fail "api.service.port=9999: CURIE_API_URL is '$actual', expected 'http://curie-api:9999' (the port is hardcoded in the template instead of read from .Values.api.service.port)"

# 3: BYO override renders verbatim (the api.deploy=false answer).
manifest="$(render_dispatcher byo --set dispatcher.apiBaseUrl=http://byo-api.example:8080 "${TOKENS[@]}")"
actual="$(env_value "$manifest" CURIE_API_URL)"
[ "$actual" = "http://byo-api.example:8080" ] \
  || fail "dispatcher.apiBaseUrl override: CURIE_API_URL is '$actual', expected the verbatim override 'http://byo-api.example:8080'"

# 4: the API key arrives by reference to the chart Secret, never inline. Reuses
# assertion 1's default render -- the arguments are identical, so a second render
# would only pay another full chart template for the same bytes.
env_has "$default_manifest" CURIE_API_KEY name \
  || fail "default install: dispatcher has no CURIE_API_KEY env; approval resolve calls will be rejected by the API"
env_has "$default_manifest" CURIE_API_KEY valueFrom.secretKeyRef \
  || fail "CURIE_API_KEY is not a secretKeyRef; an inline value would put the shared API key into 'helm get manifest' output"
actual="$(env_field "$default_manifest" CURIE_API_KEY valueFrom.secretKeyRef.name)"
[ "$actual" = "curie-secrets" ] \
  || fail "CURIE_API_KEY secretKeyRef names Secret '$actual', expected the chart Secret 'curie-secrets'"
actual="$(env_field "$default_manifest" CURIE_API_KEY valueFrom.secretKeyRef.key)"
[ "$actual" = "apiKey" ] \
  || fail "CURIE_API_KEY secretKeyRef uses key '$actual', expected the chart Secret's existing 'apiKey' key (the same key api.yaml consumes as API_KEY; a new key would let the two sides drift)"
if env_has "$default_manifest" CURIE_API_KEY value; then
  fail "CURIE_API_KEY renders an inline literal value; the credential must come from the Secret by reference only"
fi

# 5: the API preflight budget is explicit, singular, and operator-configurable.
actual="$(env_count "$default_manifest" dispatcher CURIE_API_PREFLIGHT_TIMEOUT_SECONDS)"
[ "$actual" = "1" ] \
  || fail "default install: CURIE_API_PREFLIGHT_TIMEOUT_SECONDS appears $actual times, expected exactly one chart-owned env entry"
actual="$(workload_env_value "$default_manifest" dispatcher CURIE_API_PREFLIGHT_TIMEOUT_SECONDS)"
[ "$actual" = "120" ] \
  || fail "default install: CURIE_API_PREFLIGHT_TIMEOUT_SECONDS is '$actual', expected the 120s delayed-readiness budget"

override_manifest="$(render_dispatcher timeout-override --set dispatcher.apiPreflightTimeoutSeconds=125 "${TOKENS[@]}")"
actual="$(env_count "$override_manifest" dispatcher CURIE_API_PREFLIGHT_TIMEOUT_SECONDS)"
[ "$actual" = "1" ] \
  || fail "operator timeout override: CURIE_API_PREFLIGHT_TIMEOUT_SECONDS appears $actual times, expected exactly one env entry"
actual="$(workload_env_value "$override_manifest" dispatcher CURIE_API_PREFLIGHT_TIMEOUT_SECONDS)"
[ "$actual" = "125" ] \
  || fail "dispatcher.apiPreflightTimeoutSeconds=125 rendered '$actual', so the operator override does not reach the app"

# 6: startup defers the heartbeat probe's fast-failure authority until after the
# app's two bounded phases. Kubernetes counts the first failed sample at t=0, so the
# earliest terminal failure is initialDelay + (failureThreshold - 1) * period:
# 0 + (27 - 1) * 10 = 260s, strictly beyond the maximum 242s app envelope.
probe_has "$default_manifest" dispatcher startupProbe \
  || fail "default install: dispatcher has no startupProbe, so readiness/liveness can restart it during normal API warm-up"
assert_probe_field "$default_manifest" dispatcher startupProbe initialDelaySeconds 0 "dispatcher startup gate"
assert_probe_field "$default_manifest" dispatcher startupProbe periodSeconds 10 "dispatcher startup gate"
assert_probe_field "$default_manifest" dispatcher startupProbe timeoutSeconds 5 "dispatcher startup gate"
assert_probe_field "$default_manifest" dispatcher startupProbe failureThreshold 27 "dispatcher startup gate"

startup_cutoff=$((
  $(probe_field "$default_manifest" dispatcher startupProbe initialDelaySeconds) +
  ($(probe_field "$default_manifest" dispatcher startupProbe failureThreshold) - 1) *
  $(probe_field "$default_manifest" dispatcher startupProbe periodSeconds)
))
[ "$startup_cutoff" = "260" ] \
  || fail "dispatcher startupProbe earliest fast-failure cutoff is ${startup_cutoff}s, expected 260s"
probe_budget_is_safe "$default_manifest" dispatcher \
  || fail "dispatcher startupProbe can fail before the default app startup envelope is exhausted"
probe_budget_is_safe "$override_manifest" dispatcher \
  || fail "dispatcher startupProbe can fail before the operator's 125s phase override exhausts the full startup envelope"

# Startup must use the exact heartbeat check already proven by readiness and
# liveness; a new command or path would create an untested second liveness seam.
startup_command="$(probe_field "$default_manifest" dispatcher startupProbe exec.command)"
readiness_command="$(probe_field "$default_manifest" dispatcher readinessProbe exec.command)"
liveness_command="$(probe_field "$default_manifest" dispatcher livenessProbe exec.command)"
[ "$startup_command" = "$readiness_command" ] \
  || fail "dispatcher startupProbe command differs from the existing readiness heartbeat command"
[ "$startup_command" = "$liveness_command" ] \
  || fail "dispatcher startupProbe command differs from the existing liveness heartbeat command"

# Pin the existing heartbeat behavior on both workloads. Comparing the complete
# parsed probe objects catches command, timing, and threshold drift, while the
# explicit fields make failures actionable.
assert_probe_field "$default_manifest" dispatcher readinessProbe initialDelaySeconds 10 "dispatcher readiness"
assert_probe_field "$default_manifest" dispatcher readinessProbe periodSeconds 10 "dispatcher readiness"
assert_probe_field "$default_manifest" dispatcher readinessProbe timeoutSeconds 5 "dispatcher readiness"
assert_probe_field "$default_manifest" dispatcher readinessProbe failureThreshold 3 "dispatcher readiness"
assert_probe_field "$default_manifest" dispatcher livenessProbe initialDelaySeconds 30 "dispatcher liveness"
assert_probe_field "$default_manifest" dispatcher livenessProbe periodSeconds 15 "dispatcher liveness"
assert_probe_field "$default_manifest" dispatcher livenessProbe timeoutSeconds 5 "dispatcher liveness"
assert_probe_field "$default_manifest" dispatcher livenessProbe failureThreshold 4 "dispatcher liveness"

worker_manifest="$TMP/default/curie/templates/worker.yaml"
[ -f "$worker_manifest" ] || fail "default install: worker.yaml did not render"
if probe_has "$worker_manifest" worker startupProbe; then
  fail "worker gained a startupProbe; issue #2203 is dispatcher-only and worker probe behavior must remain unchanged"
fi
[ "$(probe_json "$worker_manifest" worker readinessProbe)" = "$(probe_json "$default_manifest" dispatcher readinessProbe)" ] \
  || fail "worker readinessProbe drifted from the existing shared dispatcher heartbeat probe"
[ "$(probe_json "$worker_manifest" worker livenessProbe)" = "$(probe_json "$default_manifest" dispatcher livenessProbe)" ] \
  || fail "worker livenessProbe drifted from the existing shared dispatcher heartbeat probe"

# Falsifiable negative control: equality is unsafe because Kubernetes may make
# its terminal startup decision at the same instant the app is entitled to use
# its final budget. The chart must refuse this operator combination and name the
# setting plus the startup-probe constraint that rejected it.
under_budget_out="$TMP/under-budget"
mkdir -p "$under_budget_out"
if helm template curie "$CHART" --output-dir "$under_budget_out" \
  --set dispatcher.apiPreflightTimeoutSeconds=129 "${TOKENS[@]}" \
  >"$TMP/under-budget.stdout" 2>"$TMP/under-budget.stderr"; then
  fail "dispatcher.apiPreflightTimeoutSeconds=129 rendered a 260s app envelope against a 260s startup probe cutoff; the chart must reject an unsafe equal budget"
fi
under_budget_stderr="$(<"$TMP/under-budget.stderr")"
[[ "$under_budget_stderr" == *"dispatcher.apiPreflightTimeoutSeconds"* ]] \
  || fail "unsafe timeout render failure did not name dispatcher.apiPreflightTimeoutSeconds: $under_budget_stderr"
if [[ "$under_budget_stderr" != *"startupProbe"* && "$under_budget_stderr" != *"startup probe"* ]]; then
  fail "unsafe timeout render failure did not name the startup probe: $under_budget_stderr"
fi
if [[ "$under_budget_stderr" != *"cutoff"* && "$under_budget_stderr" != *"budget"* ]]; then
  fail "unsafe timeout render failure did not explain the startup probe cutoff/budget: $under_budget_stderr"
fi

# 7: unchanged gate -- no Slack tokens, no dispatcher.
out="$TMP/tokenless"
mkdir -p "$out"
helm template curie "$CHART" --output-dir "$out" >/dev/null
if [ -f "$out/curie/templates/dispatcher.yaml" ]; then
  fail "a token-less default install rendered a dispatcher Deployment; the curie.dispatcher.enabled gate regressed"
fi

# 8: default install renders the UI's CURIE_API_TARGET as the in-chart API
# Service, and the port comes from .Values.api.service.port. Reuses assertion
# 1's default render -- the arguments are identical.
default_ui_manifest="$TMP/default/curie/templates/ui.yaml"
[ -f "$default_ui_manifest" ] \
  || fail "default install: ui.yaml did not render"
actual="$(env_value "$default_ui_manifest" CURIE_API_TARGET)"
[ -n "$actual" ] \
  || fail "default install: UI has no CURIE_API_TARGET env value; nginx will proxy /api/ to the image default"
[ "$actual" = "http://curie-api:8000" ] \
  || fail "default install: UI CURIE_API_TARGET is '$actual', expected 'http://curie-api:8000' (the in-chart API Service)"

ui_port_manifest="$(render_ui ui-port --set api.service.port=9999)"
actual="$(env_value "$ui_port_manifest" CURIE_API_TARGET)"
[ "$actual" = "http://curie-api:9999" ] \
  || fail "api.service.port=9999: UI CURIE_API_TARGET is '$actual', expected 'http://curie-api:9999' (the port is hardcoded in the template instead of read from .Values.api.service.port)"

# 9: BYO override renders verbatim on the api.deploy=false path, and an empty
# override fails the render closed naming ui.apiBaseUrl. ui.deploy=false is the
# gated-off sibling: the rest of the chart still renders without the override.
ui_byo_manifest="$(render_ui byo-ui \
  --set api.deploy=false \
  --set ui.apiBaseUrl=http://byo-api.example:8080)"
actual="$(env_value "$ui_byo_manifest" CURIE_API_TARGET)"
[ "$actual" = "http://byo-api.example:8080" ] \
  || fail "ui.apiBaseUrl override: CURIE_API_TARGET is '$actual', expected the verbatim override 'http://byo-api.example:8080'"

byo_ui_missing="$TMP/byo-ui-missing"
mkdir -p "$byo_ui_missing"
if helm template curie "$CHART" --output-dir "$byo_ui_missing" \
  --set api.deploy=false \
  >"$TMP/byo-ui-missing.stdout" 2>"$TMP/byo-ui-missing.stderr"; then
  fail "api.deploy=false with empty ui.apiBaseUrl rendered; the chart must fail closed naming ui.apiBaseUrl"
fi
byo_ui_missing_stderr="$(<"$TMP/byo-ui-missing.stderr")"
[[ "$byo_ui_missing_stderr" == *"ui.apiBaseUrl"* ]] \
  || fail "api.deploy=false empty-override render failure did not name ui.apiBaseUrl: $byo_ui_missing_stderr"

ui_off="$TMP/ui-off"
mkdir -p "$ui_off"
helm template curie "$CHART" --output-dir "$ui_off" \
  --set api.deploy=false \
  --set ui.deploy=false >/dev/null
if [ -f "$ui_off/curie/templates/ui.yaml" ]; then
  fail "ui.deploy=false still rendered ui.yaml"
fi

echo "OK: dispatcher platform-API wiring, delayed-readiness, and UI API-target render assertions passed"
