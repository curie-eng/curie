#!/usr/bin/env bash
#
# Fast render contract for the Langfuse Postgres readiness gate on BOTH
# Langfuse Deployments (#1853, #2330).
#
# #1853 filed the crash-loop against the Langfuse *web* pod and its fix keyed
# the gate under `langfuse.web.postgresReadiness` -- a per-Deployment values
# path. `langfuse-worker` runs the same Prisma boot migrations against the same
# Postgres, so it reproduced the identical symptom (Prisma `P1001`,
# `reason: Error`, namespace `BackOff`) with no gate to hold it back: that is
# #2330. The gate is now chart-level (`langfuse.postgresReadiness`, the shape
# `langfuse.clickhouseReadiness` already had for #2009) and rendered from the
# shared `curie.langfuse.postgresGate` helper on both Deployments, ordered
# before the ClickHouse gate on both.
#
# `langfuse.web.postgresReadiness` survives as a DEPRECATED ALIAS, honoured on
# BOTH Deployments. It is deliberately not refused: `helm upgrade
# --reuse-values` replays an operator's stored user-supplied values blob, so a
# render `fail` would abort upgrades (including `curie cluster comms`) on
# exactly the 0.8.5 releases that need the gate. The safety property asserted
# here is that a stored legacy `enabled: false` or a legacy tuning value does
# not silently become web-only.
#
# Every render assertion below iterates both `-langfuse-web` and
# `-langfuse-worker`, and both red negative controls are armed once PER
# Deployment: a mutant that only strips the web init stays green against a
# worker-only regression, which is the exact hole #2330 fell through.
#
# Two renders exist solely to catch worker-specific defects that the default
# render cannot, because `langfuse.web.containerSecurityContext` and
# `langfuse.worker.containerSecurityContext` are byte-identical in values.yaml:
# `worker-uid` proves the helper reads the WORKER's context rather than web's,
# and `root-worker-security` proves the 1001 non-root floor (#351: both Langfuse
# images declare a NAMED user, so `runAsNonRoot` needs a numeric uid) still
# applies on the worker. `langfuse-runasuser-assertions.sh` stays web-only; these
# two renders are what makes that deferral safe.
#
# The executable live delayed-start regression is nested under ci/runtime so
# chart-check keeps discovering only the fast top-level assertion scripts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# The readiness init defaults to postgres.image (langfuse.web.postgresReadiness
# .image is empty by default), so read the pin out of values.yaml instead of
# repeating it here -- a literal would silently drift from the #2319 pin.
POSTGRES_IMAGE="$(python3 -c '
import sys

import yaml

values = yaml.safe_load(open(sys.argv[1])) or {}
image = ((values.get("postgres") or {}).get("image") or "").strip()
if not image:
    raise SystemExit("values.yaml postgres.image is empty")
print(image)
' "$CHART/values.yaml")"

render() {
  local name="$1"
  shift
  if ! helm template curie "$CHART" --show-only templates/langfuse.yaml "$@" >"$TMP/$name.yaml"; then
    fail "$name render failed"
  fi
}

render default
render disabled --set langfuse.postgresReadiness.enabled=false
render both-disabled \
  --set langfuse.postgresReadiness.enabled=false \
  --set langfuse.clickhouseReadiness.enabled=false
render blank-web-security --set-json 'langfuse.web.containerSecurityContext=null'
render root-web-security \
  --set langfuse.web.containerSecurityContext.runAsNonRoot=false \
  --set langfuse.web.containerSecurityContext.runAsUser=0
# Worker-only securityContext renders. The two defaults are byte-identical, so
# these are the ONLY renders that can catch a helper wired to web's context.
render worker-uid --set langfuse.worker.containerSecurityContext.runAsUser=1002
render root-worker-security \
  --set langfuse.worker.containerSecurityContext.runAsNonRoot=false \
  --set langfuse.worker.containerSecurityContext.runAsUser=0
render tuned \
  --set-string langfuse.postgresReadiness.image=registry.example.com/postgres-readiness:test \
  --set langfuse.postgresReadiness.attempts=7 \
  --set langfuse.postgresReadiness.intervalSeconds=3 \
  --set langfuse.postgresReadiness.probeTimeoutSeconds=4
render canonical-tuned --set langfuse.postgresReadiness.attempts=7
# Deprecated-alias renders (#2330 AC4). Both must SUCCEED -- there is no
# legacy-refusal control, by design.
render legacy-disabled --set langfuse.web.postgresReadiness.enabled=false
render legacy-tuned --set langfuse.web.postgresReadiness.attempts=7

cat >"$TMP/assert.py" <<'PY'
import sys
import yaml

path, mode, expected_image, attempts, interval, timeout = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(path)) if doc]

# (Deployment name suffix, application container name, short key used by the
# per-mode tables below). Every assertion runs for BOTH.
DEPLOYMENTS = (
    ("-langfuse-web", "langfuse-web", "web"),
    ("-langfuse-worker", "langfuse-worker", "worker"),
)

# How the init container's securityContext is expected to relate to its OWN
# application container's, per render mode:
#   match       -- identical to the app container's (the #351 class)
#   floor-blank -- app container has no securityContext; the init still floors
#   floor-root  -- app container was overridden to root; the init still floors
SC_POLICY = {
    "default": {"web": "match", "worker": "match"},
    "tuned": {"web": "match", "worker": "match"},
    "canonical-tuned": {"web": "match", "worker": "match"},
    "legacy-tuned": {"web": "match", "worker": "match"},
    "worker-uid": {"web": "match", "worker": "match"},
    "blank-web-security": {"web": "floor-blank", "worker": "match"},
    "root-web-security": {"web": "floor-root", "worker": "match"},
    "root-worker-security": {"web": "match", "worker": "floor-root"},
}

# Exact uid expectations. `worker-uid` is the render that proves the gate reads
# the per-Deployment context: web must stay 1001 while the worker moves to 1002.
EXACT_UID = {
    ("worker-uid", "web"): 1001,
    ("worker-uid", "worker"): 1002,
    ("blank-web-security", "web"): 1001,
    ("root-web-security", "web"): 1001,
    ("root-worker-security", "worker"): 1001,
}

NO_GATE_MODES = ("disabled", "legacy-disabled")


def deployment_with_container(suffix, container_name):
    matches = []
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        if not doc.get("metadata", {}).get("name", "").endswith(suffix):
            continue
        containers = doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if any(container.get("name") == container_name for container in containers):
            matches.append(doc)
    if len(matches) != 1:
        raise SystemExit(
            f"expected one Deployment ending in {suffix!r} containing {container_name!r}, got {len(matches)}"
        )
    return matches[0]


def check_absent(suffix, container_name):
    spec = deployment_with_container(suffix, container_name)["spec"]["template"]["spec"]
    inits = spec.get("initContainers", [])
    waits = [item for item in inits if item.get("name") == "wait-for-postgres"]
    if waits:
        raise SystemExit(
            f"{suffix}: wait-for-postgres rendered while the Postgres readiness gate is disabled "
            f"(got initContainers {[item.get('name') for item in inits]})"
        )
    if mode == "both-disabled" and "initContainers" in spec:
        raise SystemExit(
            f"{suffix}: initContainers key still rendered with BOTH readiness gates disabled "
            f"(got {[item.get('name') for item in inits]})"
        )


def check_gate(suffix, container_name, key):
    dep = deployment_with_container(suffix, container_name)
    spec = dep["spec"]["template"]["spec"]
    inits = spec.get("initContainers", [])
    names = [item.get("name") for item in inits]
    waits = [item for item in inits if item.get("name") == "wait-for-postgres"]
    if len(waits) != 1:
        raise SystemExit(
            f"{suffix}: expected exactly one wait-for-postgres init container, got {len(waits)} "
            f"(initContainers {names}). The worker runs the same Prisma boot migrations against the "
            f"same Postgres as the web pod, so it needs the same gate (#2330)"
        )
    wait = waits[0]
    if not names or names[0] != "wait-for-postgres":
        raise SystemExit(f"{suffix}: wait-for-postgres must be the FIRST init container, got {names}")
    if "wait-for-clickhouse" in names and names.index("wait-for-postgres") > names.index("wait-for-clickhouse"):
        raise SystemExit(
            f"{suffix}: wait-for-postgres must precede wait-for-clickhouse, got {names}"
        )
    if wait.get("image") != expected_image:
        raise SystemExit(
            f"{suffix}: wait-for-postgres image is {wait.get('image')!r}, expected {expected_image!r}"
        )

    app = next(item for item in spec["containers"] if item.get("name") == container_name)
    wait_sc = wait.get("securityContext") or {}
    app_sc = app.get("securityContext") or {}
    policy = SC_POLICY[mode][key]
    if policy == "match" and wait_sc != app_sc:
        raise SystemExit(
            f"{suffix}: wait-for-postgres securityContext differs from {container_name}: "
            f"{wait_sc!r} != {app_sc!r}"
        )
    uid = wait_sc.get("runAsUser")
    if type(uid) is not int or uid < 1:
        raise SystemExit(
            f"{suffix}: wait-for-postgres runAsUser must be a numeric non-root uid, got {uid!r} (#351)"
        )
    if wait_sc.get("runAsNonRoot") is not True:
        raise SystemExit(f"{suffix}: wait-for-postgres must retain runAsNonRoot=true")
    expected_uid = EXACT_UID.get((mode, key))
    if expected_uid is not None and uid != expected_uid:
        raise SystemExit(
            f"{suffix}: wait-for-postgres runAsUser is {uid!r}, expected {expected_uid!r} -- the gate "
            f"must derive its securityContext from the {key} Deployment's own containerSecurityContext"
        )
    if policy == "floor-blank" and app_sc:
        raise SystemExit(
            f"{suffix}: expected a blank {container_name} securityContext in this render, got {app_sc!r}"
        )
    if policy == "floor-root" and (
        app_sc.get("runAsUser") != 0 or app_sc.get("runAsNonRoot") is not False
    ):
        raise SystemExit(
            f"{suffix}: expected a root {container_name} override in this render, got {app_sc!r}"
        )

    env_items = wait.get("env", [])
    env = {item.get("name"): item for item in env_items}
    expected_names = {
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_DATABASE",
        "POSTGRES_READINESS_ATTEMPTS",
        "POSTGRES_READINESS_INTERVAL_SECONDS",
        "POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS",
    }
    if set(env) != expected_names:
        raise SystemExit(
            f"{suffix}: wait-for-postgres env names are {sorted(env)}, expected {sorted(expected_names)}"
        )
    for name, item in env.items():
        if "valueFrom" in item:
            raise SystemExit(
                f"{suffix}: credential-free readiness env {name} unexpectedly uses valueFrom"
            )
        upper = name.upper()
        if any(token in upper for token in ("PASSWORD", "TOKEN", "SECRET", "CREDENTIAL")):
            raise SystemExit(f"{suffix}: credential-like env {name} reached wait-for-postgres")

    expected_tuning = {
        "POSTGRES_READINESS_ATTEMPTS": attempts,
        "POSTGRES_READINESS_INTERVAL_SECONDS": interval,
        "POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS": timeout,
    }
    for name, expected in expected_tuning.items():
        actual = str(env[name].get("value"))
        if actual != expected:
            raise SystemExit(f"{suffix}: {name} rendered {actual!r}, expected {expected!r}")

    command = "\n".join(str(part) for part in wait.get("command", []))
    for needle in (
        "pg_isready",
        'while [ "$attempt" -le "$POSTGRES_READINESS_ATTEMPTS" ]',
        "Postgres readiness exhausted",
        "exit 1",
    ):
        if needle not in command:
            raise SystemExit(
                f"{suffix}: wait-for-postgres command is missing bounded-gate contract {needle!r}"
            )


if mode in NO_GATE_MODES or mode == "both-disabled":
    for suffix, container_name, _key in DEPLOYMENTS:
        check_absent(suffix, container_name)
    print(f"  ok: {mode} removes the readiness init from BOTH Langfuse Deployments")
    raise SystemExit(0)

for suffix, container_name, key in DEPLOYMENTS:
    check_gate(suffix, container_name, key)
print(
    f"  ok: {mode} both Deployments gate first on wait-for-postgres (before wait-for-clickhouse), "
    f"credential-free, hardened per-Deployment, image={expected_image}, tuning={attempts}/{interval}/{timeout}"
)
PY

python3 "$TMP/assert.py" "$TMP/default.yaml" default "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/tuned.yaml" tuned registry.example.com/postgres-readiness:test 7 3 4
python3 "$TMP/assert.py" "$TMP/blank-web-security.yaml" blank-web-security "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/root-web-security.yaml" root-web-security "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/worker-uid.yaml" worker-uid "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/root-worker-security.yaml" root-worker-security "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/disabled.yaml" disabled ignored 0 0 0
python3 "$TMP/assert.py" "$TMP/both-disabled.yaml" both-disabled ignored 0 0 0

# AC4 -- the deprecated `langfuse.web.postgresReadiness` alias. Both of these
# renders must SUCCEED and must reach BOTH Deployments; a legacy value that
# silently stayed web-only would leave the worker ungated (or, worse, gated
# after an operator had explicitly turned the gate off).
python3 "$TMP/assert.py" "$TMP/legacy-disabled.yaml" legacy-disabled ignored 0 0 0
python3 "$TMP/assert.py" "$TMP/legacy-tuned.yaml" legacy-tuned "$POSTGRES_IMAGE" 7 2 2
python3 "$TMP/assert.py" "$TMP/canonical-tuned.yaml" canonical-tuned "$POSTGRES_IMAGE" 7 2 2
echo "  ok: the deprecated langfuse.web.postgresReadiness alias reaches BOTH Deployments and merges over the canonical defaults"

# The validation `fail` names the key path the OPERATOR used, so the grep has to
# move with it: a single hardcoded string would silently pass against the wrong
# message for one of the two paths.
assert_refused() {
  local keypath="$1" field="$2" flag="$3" value="$4" output rc
  set +e
  output="$(helm template curie "$CHART" --show-only templates/langfuse.yaml \
    "$flag" "${keypath}.${field}=${value}" 2>&1)"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "invalid ${keypath}.${field}=${value} rendered successfully"
  [[ "$output" == *"${keypath}.${field}"* ]] || \
    fail "invalid ${keypath}.${field}=${value} failed without naming the knob the operator set: $output"
}

for field in attempts intervalSeconds probeTimeoutSeconds; do
  assert_refused langfuse.postgresReadiness "$field" --set 0
  assert_refused langfuse.postgresReadiness "$field" --set -1
  assert_refused langfuse.postgresReadiness "$field" --set-json 2.5
done
echo "  ok: every bounded tuning knob rejects zero, negative, and fractional values"

for field in attempts intervalSeconds probeTimeoutSeconds; do
  assert_refused langfuse.web.postgresReadiness "$field" --set 0
  assert_refused langfuse.web.postgresReadiness "$field" --set -1
  assert_refused langfuse.web.postgresReadiness "$field" --set-json 2.5
done
echo "  ok: an invalid value supplied through the deprecated alias is refused, naming the alias path"

# Red-on-revert controls: remove the init from a real default render, then prove
# the same consumer assertion rejects it for the intended reason. Each control
# is armed ONCE PER DEPLOYMENT -- a mutant that only strips the web init stays
# green against a worker-only regression, which is #2330 itself.
cat >"$TMP/strip-init.py" <<'PY'
import sys
import yaml

source, target, container_name = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if doc]
mutated = False
for doc in docs:
    if doc.get("kind") != "Deployment":
        continue
    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    if any(item.get("name") == container_name for item in spec.get("containers", [])):
        spec["initContainers"] = [
            item for item in spec.get("initContainers", [])
            if item.get("name") != "wait-for-postgres"
        ]
        mutated = True
if not mutated:
    raise SystemExit(f"no Deployment containing {container_name!r} to mutate")
with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

cat >"$TMP/unbound-init.py" <<'PY'
import sys
import yaml

source, target, container_name = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if doc]
mutated = False
for doc in docs:
    if doc.get("kind") != "Deployment":
        continue
    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    if not any(item.get("name") == container_name for item in spec.get("containers", [])):
        continue
    for item in spec.get("initContainers", []):
        if item.get("name") == "wait-for-postgres":
            item["command"][-1] = item["command"][-1].replace(
                'while [ "$attempt" -le "$POSTGRES_READINESS_ATTEMPTS" ]; do',
                "while true; do",
            )
            mutated = True
if not mutated:
    raise SystemExit(f"no wait-for-postgres under {container_name!r} to mutate")
with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

assert_mutant_rejected() {
  local label="$1" mutant="$2" suffix="$3" needle="$4" output rc
  set +e
  output="$(python3 "$TMP/assert.py" "$mutant" default "$POSTGRES_IMAGE" 60 2 2 2>&1)"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "negative control passed: $label"
  [[ "$output" == *"$needle"* ]] || fail "$label was rejected for an unrelated reason: $output"
  [[ "$output" == *"$suffix"* ]] || fail "$label was rejected without naming $suffix: $output"
}

for target in "langfuse-web:-langfuse-web" "langfuse-worker:-langfuse-worker"; do
  container_name="${target%%:*}"
  suffix="${target##*:}"
  python3 "$TMP/strip-init.py" "$TMP/default.yaml" "$TMP/missing-init-$container_name.yaml" "$container_name"
  assert_mutant_rejected "removing wait-for-postgres from $container_name" \
    "$TMP/missing-init-$container_name.yaml" "$suffix" "wait-for-postgres"
  echo "  ok: removing the rendered readiness init from $suffix is rejected"

  python3 "$TMP/unbound-init.py" "$TMP/default.yaml" "$TMP/unbounded-init-$container_name.yaml" "$container_name"
  assert_mutant_rejected "unbounding the readiness loop on $container_name" \
    "$TMP/unbounded-init-$container_name.yaml" "$suffix" "bounded-gate contract"
  echo "  ok: stripping the loop bound from the rendered init on $suffix is rejected"
done

echo "PASS: Langfuse web AND worker Postgres readiness render contract, the deprecated langfuse.web.postgresReadiness alias, and per-Deployment red negative controls (#1853, #2330)"
