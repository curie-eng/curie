#!/usr/bin/env bash
#
# Render-assertion tests for the chart's rendered output.
#
# Issue #195 (auto-generate strong per-release chart credentials), Assertions
# 1-5. Proves three things about the chart's credential Secret:
#
#   1. A SEALED render (security.allowDevDefaults defaults false, `lookup` empty
#      offline under `helm template`) GENERATES a strong random value for each of
#      the eleven chart-owned secret keys instead of shipping the published dev
#      default. The generated langfuseEncryptionKey is 64 lowercase-hex chars,
#      and the two Langfuse init credentials are 32 alphanumeric chars.
#   2. The DEV overlay (values-dev.yaml sets allowDevDefaults=true) keeps the
#      deterministic published defaults, so the dev/e2e path renders unchanged.
#   3. An explicit `--set` override that differs from the published default is
#      honored on the sealed path (override wins over generation).
#   4. The OTel Basic auth header uses the resolved Langfuse project secret
#      unless the operator supplies an explicit header override.
#
# Issue #1569 extends this contract to the two Langfuse init credentials and
# adds an executed negative control for their published values.
#
# Issue #488 (freeze the boot env in the contract), Assertions 6-7. The Helm
# template cannot import the frozen contract crate, so its hand-typed boot-env
# names have no compiler holding them to it. Assertion 6 renders the runner
# container and holds its env names to the generated key export; Assertion 7 is
# the negative control proving Assertion 6 can fail.
#
# Issue #1530 (runner sandbox API egress), Assertion 11.
#
# Issue #1109/#1124 (the API's outbound GitHub credential), Assertion 12 and its
# negative control. api.githubToken is the one OPTIONAL credential in the
# Secret, so it is a deliberate plain pass-through rather than a
# curie.managedSecret: empty must render empty ("no GitHub credential, public
# repos only") and an explicit value must render verbatim. The negative control
# routes it through the generating helper and requires the assert to fire.
#
# Runnable locally (from anywhere) and from CI. Fails loudly, naming the key.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"

# The eleven chart-owned secret keys, each with its published dev default.
KEYS=(
  postgresPassword
  valkeyPassword
  clickhousePassword
  rustfsSecretKey
  langfuseSalt
  langfuseEncryptionKey
  langfuseNextauthSecret
  langfuseInitProjectSecretKey
  langfuseInitUserPassword
  apiKey
  githubWebhookSecret
)
declare -A DEFAULTS=(
  [postgresPassword]="postgres"
  [valkeyPassword]="valkeypass"
  [clickhousePassword]="clickhouse"
  [rustfsSecretKey]="rustfssecret"
  [langfuseSalt]="dev-salt-change-me"
  [langfuseEncryptionKey]="0000000000000000000000000000000000000000000000000000000000000000"
  [langfuseNextauthSecret]="dev-nextauth-secret-change-me"
  [langfuseInitProjectSecretKey]="sk-lf-curie-dev"
  [langfuseInitUserPassword]="curie-dev-password"
  [apiKey]="curie-dev-key"
  [githubWebhookSecret]="dev-webhook-secret"
)

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

SEALED="$TMP/sealed.yaml"
DEV="$TMP/dev.yaml"

echo "=== Rendering sealed chart (allowDevDefaults default false) ==="
helm template "$CHART" --show-only templates/secrets.yaml > "$SEALED"

echo "=== Rendering dev overlay (allowDevDefaults=true) ==="
helm template "$CHART" -f "$CHART/values-dev.yaml" --show-only templates/secrets.yaml > "$DEV"

# Read one stringData key from a rendered Secret via PyYAML (robust vs grep/awk).
read_key() {
  # $1 = rendered secret YAML file, $2 = key
  python3 -c '
import sys, yaml
path, key = sys.argv[1], sys.argv[2]
doc = yaml.safe_load(open(path))
sd = (doc or {}).get("stringData", {})
if key not in sd:
    sys.stderr.write("stringData is missing key %r\n" % key)
    sys.exit(3)
sys.stdout.write(str(sd[key]))
' "$1" "$2"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

check_generated_key() {
  # $1 = rendered Secret, $2 = key, $3 = render label
  local out="$1" key="$2" label="$3" val def
  val="$(read_key "$out" "$key")"
  def="${DEFAULTS[$key]}"
  if [[ "$val" == "$def" ]]; then
    echo "$label still emits the published dev default for '$key' (value == '$def'); expected a generated value." >&2
    return 1
  fi
  echo "  ok: $key was generated (not the published default)"
}

echo "=== Assertion 1a: sealed render GENERATES (no published default) ==="
for key in "${KEYS[@]}"; do
  check_generated_key "$SEALED" "$key" "sealed render" \
    || fail "sealed render did not generate '$key'; see the message above."
done

echo "=== Assertion 1b negative control: published Langfuse init values FAIL the sealed detector ==="
for key in langfuseInitProjectSecretKey langfuseInitUserPassword; do
  negative_output=""
  if negative_output="$(check_generated_key "$DEV" "$key" "development negative control" 2>&1)"; then
    fail "negative control did not fire: published '$key' passed the sealed generation detector."
  fi
  if [[ "$negative_output" != *"published dev default for '$key'"* ]]; then
    fail "negative control for '$key' failed for an unexpected reason: $negative_output"
  fi
  echo "  ok: published $key is rejected (the detector can fail)"
done

# Langfuse accepts this generated shape: its env schema defines
# LANGFUSE_INIT_PROJECT_SECRET_KEY as an optional string, without an `sk`
# prefix constraint, and initialization passes it through as the predefined
# project secret key. See https://github.com/langfuse/langfuse/blob/bf78bcefd23f43bbd8d04c263f31b70ca2f1ec29/web/src/env.mjs and
# https://github.com/langfuse/langfuse/blob/bf78bcefd23f43bbd8d04c263f31b70ca2f1ec29/web/src/initialize.ts.
echo "=== Assertion 1c: sealed Langfuse init credentials are 32 alphanumeric chars ==="
for key in langfuseInitProjectSecretKey langfuseInitUserPassword; do
  val="$(read_key "$SEALED" "$key")"
  if [[ ! "$val" =~ ^[[:alnum:]]{32}$ ]]; then
    fail "sealed $key must match ^[[:alnum:]]{32}$; got '${val}' (length ${#val})."
  fi
  echo "  ok: $key is 32 alphanumeric chars"
done

echo "=== Assertion 2: sealed langfuseEncryptionKey is 64 lowercase-hex chars ==="
enc="$(read_key "$SEALED" langfuseEncryptionKey)"
if [[ ! "$enc" =~ ^[0-9a-f]{64}$ ]]; then
  fail "sealed langfuseEncryptionKey must match ^[0-9a-f]{64}$; got '${enc}' (length ${#enc})."
fi
echo "  ok: langfuseEncryptionKey is 64 lowercase-hex chars"

echo "=== Assertion 3: dev overlay keeps the deterministic published defaults ==="
for key in "${KEYS[@]}"; do
  val="$(read_key "$DEV" "$key")"
  def="${DEFAULTS[$key]}"
  if [[ "$val" != "$def" ]]; then
    fail "dev overlay must keep the published default for '$key'; expected '$def', got '$val'."
  fi
  echo "  ok: $key == published default (deterministic dev path)"
done

echo "=== Assertion 4a: explicit override wins on the sealed path ==="
# On the sealed path (no allowDevDefaults, empty offline `lookup`), an operator
# `--set` that differs from the published default must be honored verbatim rather
# than generated -- this proves the override branch sits ahead of generation.
OVERRIDE="$TMP/override.yaml"
helm template "$CHART" \
  --set api.apiKey=override-sentinel-xyz \
  --set langfuse.init.projectSecretKey=override-project-secret-1569 \
  --set langfuse.init.userPassword=override-user-password-1569 \
  --show-only templates/secrets.yaml > "$OVERRIDE"
got="$(read_key "$OVERRIDE" apiKey)"
if [[ "$got" != "override-sentinel-xyz" ]]; then
  fail "explicit --set api.apiKey override must be honored on the sealed path; expected 'override-sentinel-xyz', got '$got'."
fi
echo "  ok: explicit apiKey override honored (override wins over generation)"
got="$(read_key "$OVERRIDE" langfuseInitProjectSecretKey)"
if [[ "$got" != "override-project-secret-1569" ]]; then
  fail "explicit --set langfuse.init.projectSecretKey override must be honored on the sealed path; expected 'override-project-secret-1569', got '$got'."
fi
echo "  ok: explicit Langfuse init project secret override honored"
got="$(read_key "$OVERRIDE" langfuseInitUserPassword)"
if [[ "$got" != "override-user-password-1569" ]]; then
  fail "explicit --set langfuse.init.userPassword override must be honored on the sealed path; expected 'override-user-password-1569', got '$got'."
fi
echo "  ok: explicit Langfuse init user password override honored"

assert_otlp_auth_agrees() {
  # $1 = rendered Secret, $2 = expected public key, $3 = render label
  local out="$1" public_key="$2" label="$3" project_secret got expected
  project_secret="$(read_key "$out" langfuseInitProjectSecretKey)"
  got="$(read_key "$out" otlpAuthHeader)"
  expected="Basic $(printf '%s' "$public_key:$project_secret" | base64 | tr -d '\n')"
  if [[ "$got" != "$expected" ]]; then
    fail "$label OTel Basic auth must use the rendered langfuseInitProjectSecretKey; expected '$expected', got '$got'."
  fi
  echo "  ok: $label OTel Basic auth agrees with the rendered project secret"
}

echo "=== Assertion 4b: default OTel auth uses the resolved Langfuse project secret ==="
assert_otlp_auth_agrees "$SEALED" "pk-lf-curie-dev" "sealed render"
assert_otlp_auth_agrees "$OVERRIDE" "pk-lf-curie-dev" "credential override render"

OTLP_OVERRIDE="$TMP/otlp-override.yaml"
helm template "$CHART" \
  --set-string 'otelCollector.otlpAuthHeader=Basic explicit-otel-sentinel-1569' \
  --show-only templates/secrets.yaml > "$OTLP_OVERRIDE"
got="$(read_key "$OTLP_OVERRIDE" otlpAuthHeader)"
if [[ "$got" != "Basic explicit-otel-sentinel-1569" ]]; then
  fail "explicit --set otelCollector.otlpAuthHeader must be honored; expected 'Basic explicit-otel-sentinel-1569', got '$got'."
fi
echo "  ok: explicit OTel auth override honored"

echo "=== Assertion 5: quoted \"false\" does NOT disable generation (fail closed) ==="
# Go templates treat any non-empty string as truthy, so a quoted
# `security.allowDevDefaults="false"` (easily produced by --set or values-file
# quoting) must NOT read as truthy and ship the published dev default. Only the
# literal `true` opts into defaults; every other value falls through to
# generation. Assert both the bareword and the quoted-string spellings still
# generate apiKey (not the published `curie-dev-key`).
for spelling in "security.allowDevDefaults=false" 'security.allowDevDefaults="false"'; do
  FALSY="$TMP/falsy.yaml"
  helm template "$CHART" --set "$spelling" \
    --show-only templates/secrets.yaml > "$FALSY"
  got="$(read_key "$FALSY" apiKey)"
  if [[ "$got" == "curie-dev-key" ]]; then
    fail "allowDevDefaults '$spelling' must NOT ship the published default; apiKey generated expected, got 'curie-dev-key' (fail-OPEN regression)."
  fi
  echo "  ok: --set $spelling still generates apiKey (fail closed)"
done

echo "=== Assertion 6: runner env names are declared boot-env keys (#488) ==="
# The chart is the one boot-env producer that cannot import the frozen contract:
# a Helm template has no way to reference curie_aci_protocol, so its env names
# are hand-typed YAML. This render-assert is the only thing holding them to the
# contract, which is why it exists and why Assertion 7 proves it can fail.
#
# The expected key list is READ FROM the generated crate, never hand-copied here:
# a copy would be a third drift site and would defeat the point of the pin.
#
# Subset, never equality (issue #488, edge case 4): a warm/unbound pod
# legitimately carries only a few boot vars (CURIE_SESSION_ID is baked as
# `warm-unbound`, budget/tokens arrive per-claim from the worker), so requiring
# every exported key to be present would fail every render. What we can hold is
# the inverse: no runner env name that LOOKS like a boot key may be absent from
# the contract. That catches the real regression, a typo or a rename that the
# runner then silently never reads.
KEY_SRC="$REPO_ROOT/packages/aci-protocol/generated/rust/src/lib.rs"
[[ -f "$KEY_SRC" ]] || fail "generated key source not found at $KEY_SRC; run the contract codegen first."

ENV_CHECK="$TMP/check_runner_env.py"
cat > "$ENV_CHECK" <<'PYEOF'
"""Assert a rendered SandboxTemplate's runner env names are declared boot keys.

argv: <rendered-dir> <generated-rust-lib.rs>
Exits 0 on pass, 1 naming the offending key(s) on failure.
"""
import pathlib
import re
import sys

import yaml

# Env names in these namespaces are boot-env contract keys and must be declared.
# Anything else the runner container carries (HOME, and operator free-form
# extraEnv on a non-default render) is out of scope by design: extraEnv is
# operator-supplied and the contract does not govern it (issue #488, edge case 6).
CONTRACT_PREFIXES = ("CURIE_", "OTEL_EXPORTER_OTLP_", "ANTHROPIC_")

rendered, key_src = sys.argv[1], sys.argv[2]

# The exported list, parsed out of the generated `env_keys` module. Scoped to
# that module so an unrelated string constant elsewhere in the crate cannot
# widen the allowed set.
text = pathlib.Path(key_src).read_text()
module = re.search(r"pub mod env_keys \{(.*?)\n\}", text, re.S)
if not module:
    sys.stderr.write(f"no `pub mod env_keys` block found in {key_src}\n")
    sys.exit(1)
declared = set(re.findall(r'pub const [A-Z0-9_]+: &str = "([A-Z0-9_]+)"', module.group(1)))
if not declared:
    sys.stderr.write(f"`env_keys` in {key_src} exported no keys; the pin would be vacuous\n")
    sys.exit(1)

# Scope to the RUNNER container only (issue #488, edge case 5): the bundle-fetch
# and bundle-extract init containers declare their own CURIE_BUNDLE_REF, which
# is init-container env, not the runner's boot env.
found = {}
def walk(node, path):
    if isinstance(node, dict):
        if node.get("name") == "runner" and "image" in node:
            for entry in node.get("env", []) or []:
                found.setdefault(entry["name"], path)
        for key, value in node.items():
            walk(value, f"{path}/{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            walk(value, f"{path}[{i}]")

for path in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(path.read_text()):
        if isinstance(doc, dict) and doc.get("kind") == "SandboxTemplate":
            walk(doc, str(path))

if not found:
    sys.stderr.write("found no runner container env in the rendered SandboxTemplate; "
                     "the subset assert would pass vacuously\n")
    sys.exit(1)

# Non-vacuity floor: the render must actually carry boot env. Without this a
# template that dropped its whole env block would sail through the subset check.
contract_names = {n for n in found if n.startswith(CONTRACT_PREFIXES)}
if "CURIE_SESSION_ID" not in contract_names or len(contract_names) < 4:
    sys.stderr.write(
        "runner env does not carry a plausible boot env (expected CURIE_SESSION_ID "
        f"and 4+ contract-namespaced names); got {sorted(contract_names)}\n")
    sys.exit(1)

undeclared = sorted(n for n in contract_names if n not in declared)
if undeclared:
    for name in undeclared:
        sys.stderr.write(
            f"runner container env '{name}' (at {found[name]}) is NOT a declared "
            "boot-env key in aci_protocol.session.BootEnv. Either it is a typo of a "
            "real key, or the chart is inventing an env var the runner never reads.\n")
    sys.exit(1)

print(f"  ok: {len(contract_names)} runner boot-env names all declared: "
      f"{', '.join(sorted(contract_names))}")
PYEOF

# Render one chart dir's SandboxTemplate and check it. Returns nonzero (rather
# than exiting) so the negative control can assert the failure.
check_runner_env() {
  # $1 = chart dir, $2 = label, rest = extra helm args
  local chart="$1" label="$2"
  shift 2
  local out
  out="$(mktemp -d -p "$TMP")"
  # --output-dir, not a stdout pipe: a piped `helm template` can truncate at
  # exit 0, which would silently turn this assert into a false negative. No
  # --show-only either (it does not compose with --output-dir); the extractor
  # selects the SandboxTemplate by kind.
  helm template "$chart" --output-dir "$out" "$@" > /dev/null
  echo "  render: $label"
  python3 "$ENV_CHECK" "$out" "$KEY_SRC"
}

# Default render: fakeModel + a baked model + OTel, which is every literal on
# the default path.
check_runner_env "$CHART" "default values" \
  || fail "default render carries a runner env name that is not a declared boot-env key."
# Widened render: reaches the conditional literals the default branches past
# (CURIE_CREDENTIALS, and the inference-branch ANTHROPIC_BASE_URL/CURIE_MODEL).
check_runner_env "$CHART" "credentials + in-cluster inference" \
  --set agentSandbox.runner.credentials=dummy \
  --set inference.deploy=true \
  || fail "widened render carries a runner env name that is not a declared boot-env key."

echo "=== Assertion 7: negative control -- a misspelled runner env name FAILS ==="
# Mandatory: an assert that has never been shown failing is not a pin. Mutate a
# TEMP COPY of the chart (never the real template) and require the check to
# reject it, naming the bad key.
MUTANT="$TMP/mutant"
cp -a "$CHART" "$MUTANT"
python3 - "$MUTANT/templates/agent-sandbox.yaml" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
text = p.read_text()
old, new = "- name: CURIE_SANDBOX_ID", "- name: CURIE_SANBOX_ID"
if old not in text:
    sys.stderr.write(f"negative control could not find {old!r} to mutate\n")
    sys.exit(1)
p.write_text(text.replace(old, new, 1))
PYEOF
if check_runner_env "$MUTANT" "mutant (CURIE_SANDBOX_ID -> CURIE_SANBOX_ID)" 2>&1; then
  fail "negative control did not fire: a misspelled 'CURIE_SANBOX_ID' passed the boot-env assert, so Assertion 6 is not actually pinning anything."
fi
echo "  ok: misspelled runner env name is rejected (the assert can fail)"

echo "=== Assertion 8: priorityClassName on every control-plane pod + the sandbox controller + the sandbox (ADR-0059 decision 5, #759, #816) ==="
# The control plane (worker, api, dispatcher, data tier: postgres, valkey,
# clickhouse, rustfs) must outrank sandbox pods for node-pressure eviction, so
# the components that supervise, drain, and reclaim a sandbox are never
# themselves preferred for eviction over the sandboxes they manage. Render with
# the dispatcher enabled (it needs both Slack tokens to render at all) so every
# control-plane pod is present in one pass.
#
# The vendored agent-sandbox controller (#816) is control plane too: it
# reconciles sandbox claims/releases, so it must also outrank the sandbox pods
# it manages for node-pressure eviction. It is a static Deployment named
# exactly `agent-sandbox-controller`, not prefixed by the chart fullname, so it
# cannot be matched by the suffix table below and needs its own exact-name
# lookup. This is also the tripwire for the template-layer string-injection
# anchor silently drifting on a future upstream controller bump: if the anchor
# stops matching, the field silently stops being set, and only an assert that
# actually reads the rendered priorityClassName back out would catch it.
PRIO_OUT="$(mktemp -d -p "$TMP")"
helm template "$CHART" --output-dir "$PRIO_OUT" \
  --set dispatcher.slack.appToken=xapp-render-assert \
  --set dispatcher.slack.botToken=xoxb-render-assert \
  > /dev/null

PRIO_CHECK="$TMP/check_priority_class.py"
cat > "$PRIO_CHECK" <<'PYEOF'
"""Assert priorityClassName on every control-plane pod template, the vendored
agent-sandbox controller Deployment, and the sandbox pod template.

argv: <rendered-dir> <expected-platform-name> <expected-sandbox-name>
Exits 0 on pass, 1 naming the offending workload on failure.
"""
import pathlib
import sys

import yaml

rendered, platform_name, sandbox_name = sys.argv[1], sys.argv[2], sys.argv[3]

# Deployment/StatefulSet name suffix -> expected priorityClassName. The data
# tier (postgres/valkey/clickhouse/rustfs) and the three first-party services
# (worker, api, dispatcher) are all control plane per ADR-0059 decision 5;
# langfuse/ui/inference/otel are deliberately out of scope (not named in the
# decision).
EXPECTED = {
    "-worker": platform_name,
    "-api": platform_name,
    "-dispatcher": platform_name,
    "-postgres": platform_name,
    "-valkey": platform_name,
    "-clickhouse": platform_name,
    "-rustfs": platform_name,
}

# Exact name, not a suffix match: `agent-sandbox-controller-extensions` and
# `release-name-curie-preflight-controller` also exist in the render, and a
# loose `-controller` suffix would silently match the wrong object.
CONTROLLER_NAME = "agent-sandbox-controller"

found = {}
controller_found = None
sandbox_found = []
for path in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(path.read_text()):
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind")
        if kind in ("Deployment", "StatefulSet"):
            name = doc.get("metadata", {}).get("name", "")
            spec = (
                doc.get("spec", {})
                .get("template", {})
                .get("spec", {})
            )
            for suffix in EXPECTED:
                if name.endswith(suffix):
                    found[suffix] = (name, spec.get("priorityClassName"))
            if kind == "Deployment" and name == CONTROLLER_NAME:
                controller_found = (name, spec.get("priorityClassName"))
        elif kind == "SandboxTemplate":
            spec = doc.get("spec", {}).get("podTemplate", {}).get("spec", {})
            sandbox_found.append((doc.get("metadata", {}).get("name", ""), spec.get("priorityClassName")))

missing = sorted(set(EXPECTED) - set(found))
if missing:
    sys.stderr.write(f"render is missing expected control-plane workload(s): {missing}\n")
    sys.exit(1)

mismatched = [
    (suffix, name, got, EXPECTED[suffix])
    for suffix, (name, got) in found.items()
    if got != EXPECTED[suffix]
]
if mismatched:
    for suffix, name, got, want in mismatched:
        sys.stderr.write(
            f"workload '{name}' (matched by suffix '{suffix}') has "
            f"priorityClassName={got!r}, expected {want!r}\n")
    sys.exit(1)

if controller_found is None:
    sys.stderr.write(
        f"found no Deployment named exactly {CONTROLLER_NAME!r} in the render; "
        "the agent-sandbox controller priorityClassName assert would pass vacuously\n")
    sys.exit(1)

controller_name, controller_got = controller_found
if controller_got != platform_name:
    sys.stderr.write(
        f"controller Deployment '{controller_name}' has "
        f"priorityClassName={controller_got!r}, expected {platform_name!r}\n")
    sys.exit(1)

if not sandbox_found:
    sys.stderr.write("found no SandboxTemplate in the render; the sandbox assert would pass vacuously\n")
    sys.exit(1)

sandbox_mismatched = [(n, got) for n, got in sandbox_found if got != sandbox_name]
if sandbox_mismatched:
    for name, got in sandbox_mismatched:
        sys.stderr.write(
            f"SandboxTemplate '{name}' has priorityClassName={got!r}, expected {sandbox_name!r}\n")
    sys.exit(1)

print(f"  ok: {len(found)} control-plane workloads and the agent-sandbox controller carry "
      f"priorityClassName={platform_name!r}; SandboxTemplate carries priorityClassName={sandbox_name!r}")
PYEOF

python3 "$PRIO_CHECK" "$PRIO_OUT" "curie-platform" "curie-sandbox" \
  || fail "default render did not set the expected priorityClassName on every control-plane pod, the agent-sandbox controller, and the sandbox."

echo "=== Assertion 9: priorityClassName names are operator-overridable (additive values, #759) ==="
PRIO_OVERRIDE_OUT="$(mktemp -d -p "$TMP")"
helm template "$CHART" --output-dir "$PRIO_OVERRIDE_OUT" \
  --set dispatcher.slack.appToken=xapp-render-assert \
  --set dispatcher.slack.botToken=xoxb-render-assert \
  --set priorityClasses.platform.name=custom-platform-class \
  --set priorityClasses.sandbox.name=custom-sandbox-class \
  > /dev/null
python3 "$PRIO_CHECK" "$PRIO_OVERRIDE_OUT" "custom-platform-class" "custom-sandbox-class" \
  || fail "overriding priorityClasses.platform.name/sandbox.name did not propagate to priorityClassName on the rendered pods."
echo "  ok: overriding priorityClasses.platform.name/sandbox.name propagates to every control-plane pod, the agent-sandbox controller, and the sandbox"

echo "=== Assertion 10: SandboxTemplate opts the controller out of its own permissive NetworkPolicy when Rail 1 is on (#765) ==="
# NetworkPolicy allows are additive across objects that select the same pods --
# there is no way for the chart's own restrictive Rail 1 policies to narrow
# what a separate, broader policy already permits. Left unset, the vendored
# agent-sandbox controller's default "Managed" behavior reconciles its OWN
# shared NetworkPolicy per SandboxTemplate with a built-in Secure Default
# egress rule (public internet minus RFC1918/link-local), which silently
# re-opens exactly the egress Rail 1's default-deny + allowlist were meant to
# close (issue #765 packet-level evidence: a non-allowlisted host was
# reachable from a real sandbox pod). spec.networkPolicyManagement: Unmanaged
# tells the controller to skip creating that policy for this template
# entirely, leaving Rail 1 as the only NetworkPolicy selecting these pods.
NP_CHECK="$TMP/check_network_policy_management.py"
cat > "$NP_CHECK" <<'PYEOF'
"""Assert the rendered SandboxTemplate's spec.networkPolicyManagement.

argv: <rendered-dir> <expected-value-or-"absent">
Exits 0 on pass, 1 naming the mismatch on failure.
"""
import pathlib
import sys

import yaml

rendered, expected = sys.argv[1], sys.argv[2]

found = []
for path in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(path.read_text()):
        if isinstance(doc, dict) and doc.get("kind") == "SandboxTemplate":
            spec = doc.get("spec", {}) or {}
            found.append((doc.get("metadata", {}).get("name", ""), spec.get("networkPolicyManagement")))

if not found:
    sys.stderr.write("found no SandboxTemplate in the render; the assert would pass vacuously\n")
    sys.exit(1)

for name, got in found:
    want = None if expected == "absent" else expected
    if got != want:
        sys.stderr.write(
            f"SandboxTemplate '{name}' has spec.networkPolicyManagement={got!r}, expected {want!r}\n")
        sys.exit(1)

print(f"  ok: {len(found)} SandboxTemplate(s) carry spec.networkPolicyManagement={expected!r}")
PYEOF

NP_ON_OUT="$(mktemp -d -p "$TMP")"
helm template "$CHART" --output-dir "$NP_ON_OUT" \
  --set agentSandbox.runner.image=curie-runner \
  --set agentSandbox.runner.tag=latest \
  --set agentSandbox.runner.imagePullPolicy=Never \
  > /dev/null
python3 "$NP_CHECK" "$NP_ON_OUT" "Unmanaged" \
  || fail "default render (Rail 1 on) did not set spec.networkPolicyManagement: Unmanaged on the runner SandboxTemplate."
echo "  ok: default render (security.networkPolicy.enabled=true) sets networkPolicyManagement: Unmanaged"

NP_OFF_OUT="$(mktemp -d -p "$TMP")"
helm template "$CHART" --output-dir "$NP_OFF_OUT" \
  --set agentSandbox.runner.image=curie-runner \
  --set agentSandbox.runner.tag=latest \
  --set agentSandbox.runner.imagePullPolicy=Never \
  --set security.networkPolicy.enabled=false \
  > /dev/null
python3 "$NP_CHECK" "$NP_OFF_OUT" "absent" \
  || fail "with security.networkPolicy.enabled=false (Rail 1 off), spec.networkPolicyManagement should be left unset (default Managed) so the controller's own baseline policy still applies, but it was set."
echo "  ok: with Rail 1 off, networkPolicyManagement is left unset (falls back to the controller's own Managed default rather than nothing)"

echo "=== Assertion 11: runner sandbox reaches only this release API on TCP 8000 (#1530) ==="
RUNNER_API_OUT="$(mktemp -d -p "$TMP")"
helm template runner-api-render "$CHART" --namespace runner-api-namespace \
  --output-dir "$RUNNER_API_OUT" > /dev/null

RUNNER_API_CHECK="$TMP/check_runner_api_egress.py"
cat > "$RUNNER_API_CHECK" <<'PYEOF'
import pathlib
import sys

import yaml

rendered, expected_state = sys.argv[1], sys.argv[2]
expected_name = "runner-api-render-curie-runner-allow-api"
expected_default_deny_name = "runner-api-render-curie-runner-default-deny-egress"
expected_spec = {
    "podSelector": {
        "matchLabels": {
            "app.kubernetes.io/name": "curie",
            "app.kubernetes.io/instance": "runner-api-render",
            "app.kubernetes.io/component": "runner-sandbox",
        },
    },
    "policyTypes": ["Egress"],
    "egress": [{
        "to": [{
            "namespaceSelector": {
                "matchLabels": {
                    "kubernetes.io/metadata.name": "runner-api-namespace",
                },
            },
            "podSelector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "curie",
                    "app.kubernetes.io/instance": "runner-api-render",
                    "app.kubernetes.io/component": "api",
                },
            },
        }],
        "ports": [{"protocol": "TCP", "port": 8000}],
    }],
}

policies = []
for path in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(path.read_text()):
        if isinstance(doc, dict) and doc.get("kind") == "NetworkPolicy":
            policies.append(doc)

matches = [
    policy for policy in policies
    if policy.get("metadata", {}).get("name") == expected_name
]
default_deny_matches = [
    policy for policy in policies
    if policy.get("metadata", {}).get("name") == expected_default_deny_name
]

if expected_state == "present":
    if len(matches) != 1:
        sys.stderr.write(
            f"expected exactly one NetworkPolicy named {expected_name!r}; found {len(matches)}\n")
        sys.exit(1)
    got = matches[0].get("spec")
    if got != expected_spec:
        sys.stderr.write(
            f"NetworkPolicy {expected_name!r} has spec={got!r}; expected {expected_spec!r}\n")
        sys.exit(1)
    print("  ok: runner sandbox API egress is release scoped and TCP 8000 only")
elif expected_state == "absent":
    if matches:
        sys.stderr.write(
            f"api.deploy=false still renders NetworkPolicy {expected_name!r}\n")
        sys.exit(1)
    if len(default_deny_matches) != 1:
        sys.stderr.write(
            f"api.deploy=false render must retain exactly one NetworkPolicy named "
            f"{expected_default_deny_name!r}; found {len(default_deny_matches)}\n")
        sys.exit(1)
    print("  ok: api.deploy=false removes the runner sandbox API egress allowance")
else:
    sys.stderr.write(f"unknown expected state {expected_state!r}\n")
    sys.exit(2)
PYEOF

python3 "$RUNNER_API_CHECK" "$RUNNER_API_OUT" present \
  || fail "default render is missing the release scoped runner sandbox API egress allowance."

RUNNER_API_OFF_OUT="$(mktemp -d -p "$TMP")"
helm template runner-api-render "$CHART" --namespace runner-api-namespace \
  --set api.deploy=false \
  --output-dir "$RUNNER_API_OFF_OUT" > /dev/null
python3 "$RUNNER_API_CHECK" "$RUNNER_API_OFF_OUT" absent \
  || fail "api.deploy=false did not remove the runner sandbox API egress allowance."

echo "=== Assertion 12a: api.githubToken is passed through, never generated (#1109, #1124) ==="
# The one OPTIONAL credential in this Secret. curie.managedSecret GENERATES when
# the value equals its default, which for an optional token means 32 characters
# of noise sent to GitHub as a bearer token, failing auth in a way that reads
# like a permissions problem rather than a missing credential (#1109 shipped that
# and reverted it). Empty must stay EMPTY -- empty means "no GitHub credential,
# public repos only" -- and an explicit value must render verbatim on the sealed
# path, which is what makes `curie cluster up --github-token` reach the API pod.
#
# Returns nonzero rather than exiting, so the negative control below can assert
# the failure. Same shape as check_runner_env for Assertions 6/7, except it
# takes an ALREADY-RENDERED Secret rather than a chart dir: the sealed render is
# the one captured once at the top of this script and shared with Assertions 1
# and 2, so only the negative control (a mutated chart copy) renders its own.
check_github_token_empty() {
  # $1 = rendered secrets.yaml, $2 = label
  local out="$1" label="$2" got
  if ! got="$(read_key "$out" githubToken)"; then
    echo "githubToken is missing from the rendered Secret entirely; the pass-through assert would be vacuous." >&2
    return 1
  fi
  if [[ -n "$got" ]]; then
    echo "sealed render must leave githubToken EMPTY (empty means 'no GitHub credential, public repos only'); got '${got}' -- api.githubToken has been routed through a generating helper (#1109 regression)." >&2
    return 1
  fi
  echo "  ok: $label leaves githubToken empty (not generated)"
}

check_github_token_empty "$SEALED" "sealed render" \
  || fail "sealed render does not leave api.githubToken empty; see the message above."

GHT="$TMP/githubtoken.yaml"
SENTINEL="ghp-render-sentinel-1124" # gitleaks:allow -- fake render sentinel, not a real token
helm template "$CHART" --set api.githubToken="$SENTINEL" \
  --show-only templates/secrets.yaml > "$GHT"
got="$(read_key "$GHT" githubToken)"
if [[ "$got" != "$SENTINEL" ]]; then
  fail "an explicit api.githubToken must render verbatim; expected '$SENTINEL', got '$got'."
fi
echo "  ok: explicit githubToken renders verbatim"

for key in "${KEYS[@]}"; do
  [[ "$key" == "githubToken" ]] && fail "githubToken must NOT be in KEYS: that list asserts a value is GENERATED on the sealed path, which is the exact #1109 regression."
done
echo "  ok: githubToken is not in the generated-key list"

echo "=== Assertion 12b negative control: routing githubToken through curie.managedSecret FAILS ==="
# Mandatory, per Assertion 7's convention: an assert that has never been shown
# failing is not a pin, and the three checks above all pass at base. Mutate a
# TEMP COPY of the chart (never the real template) into exactly the #1109
# regression and require the check to reject it, naming the key.
GHT_MUTANT="$TMP/mutant-githubtoken"
cp -a "$CHART" "$GHT_MUTANT"
python3 - "$GHT_MUTANT/templates/secrets.yaml" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
text = p.read_text()
old = '  githubToken: {{ .Values.api.githubToken | default "" | quote }}'
new = ('  githubToken: {{ include "curie.managedSecret" (dict "root" . "key" "githubToken" '
       '"value" .Values.api.githubToken "default" "" "hex" false "existingData" $existingSecret) | quote }}')
if old not in text:
    sys.stderr.write(f"negative control could not find {old!r} to mutate\n")
    sys.exit(1)
p.write_text(text.replace(old, new, 1))
PYEOF
GHT_MUTANT_RENDER="$TMP/mutant-githubtoken.yaml"
helm template "$GHT_MUTANT" --show-only templates/secrets.yaml > "$GHT_MUTANT_RENDER"
if check_github_token_empty "$GHT_MUTANT_RENDER" "mutant (githubToken via curie.managedSecret)" 2>&1; then
  fail "negative control did not fire: githubToken routed through curie.managedSecret still rendered empty, so Assertion 12 is not actually pinning anything."
fi
echo "  ok: a generated githubToken is rejected (the assert can fail)"

echo "=== Assertion 12c: worker API URL and key wiring (#1529, #1578) ==="
WORKER_API_CHECK="$TMP/check_worker_api.py"
cat > "$WORKER_API_CHECK" <<'PYEOF'
import sys

import yaml

manifest, secret_manifest, expected_url, key_source, *key_ref = sys.argv[1:]
docs = [
    doc
    for path in (manifest, secret_manifest)
    for doc in yaml.safe_load_all(open(path))
    if doc
]
workers = [
    doc
    for doc in docs
    if doc.get("kind") == "Deployment"
    and doc.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "worker"
]
if len(workers) != 1:
    raise SystemExit(f"expected one worker Deployment, found {len(workers)}")

containers = workers[0]["spec"]["template"]["spec"].get("containers", [])
if len(containers) != 1 or containers[0].get("name") != "worker":
    raise SystemExit("expected one worker container")

def only_entry(name):
    entries = [entry for entry in containers[0].get("env", []) if entry.get("name") == name]
    if len(entries) != 1:
        raise SystemExit(f"{name} appears {len(entries)} times")
    return entries[0]

url_entry = only_entry("CURIE_API_URL")
if url_entry.get("value") != expected_url:
    raise SystemExit(
        f"CURIE_API_URL is {url_entry.get('value')!r}, expected {expected_url!r}"
    )

key_entry = only_entry("CURIE_API_KEY")

if key_source == "chart":
    chart_secrets = [
        doc
        for doc in docs
        if doc.get("kind") == "Secret"
        and "apiKey" in (doc.get("stringData") or {})
    ]
    if len(chart_secrets) != 1:
        raise SystemExit(
            f"expected one chart Secret containing apiKey, found {len(chart_secrets)}"
        )
    expected_key_entry = {
        "name": "CURIE_API_KEY",
        "valueFrom": {
            "secretKeyRef": {
                "name": chart_secrets[0].get("metadata", {}).get("name"),
                "key": "apiKey",
            }
        },
    }
elif key_source == "operator":
    if len(key_ref) != 2:
        raise SystemExit("operator key assertion requires a Secret name and key")
    expected_key_entry = {
        "name": "CURIE_API_KEY",
        "valueFrom": {
            "secretKeyRef": {"name": key_ref[0], "key": key_ref[1]}
        },
    }
else:
    raise SystemExit(f"unknown key source {key_source!r}")

if key_entry != expected_key_entry:
    raise SystemExit(
        "CURIE_API_KEY does not match its expected Secret reference"
    )
PYEOF

assert_worker_api() {
  local name="$1" release="$2" expected_url="$3" key_source="$4"
  shift 4
  local key_args=()
  if [[ "$key_source" == "operator" ]]; then
    key_args=("$1" "$2")
    shift 2
  fi
  local out="$TMP/worker_api_url_$name"
  mkdir -p "$out"
  helm template "$release" "$CHART" --output-dir "$out" "$@" >/dev/null
  local manifest="$out/curie/templates/worker.yaml"
  local secret_manifest="$out/curie/templates/secrets.yaml"
  [[ -f "$manifest" ]] || fail "$name: worker.yaml did not render"
  [[ -f "$secret_manifest" ]] || fail "$name: secrets.yaml did not render"
  local error
  if ! error="$(python3 "$WORKER_API_CHECK" "$manifest" "$secret_manifest" "$expected_url" "$key_source" "${key_args[@]}" 2>&1)"; then
    fail "$name: $error"
  fi
}

assert_worker_api default curie http://curie-api:8000 chart
assert_worker_api enabled curie http://curie-api:8000 chart --set worker.connectorReconciler.enabled=true
assert_worker_api release other http://other-curie-api:8000 chart
assert_worker_api port curie http://curie-api:9999 chart --set api.service.port=9999

WORKER_API_BYO="$TMP/worker_api_byo.yaml"
cat > "$WORKER_API_BYO" <<'EOF'
api:
  deploy: false
dispatcher:
  apiBaseUrl: https://byo-api.example
EOF
assert_worker_api byo curie https://byo-api.example chart -f "$WORKER_API_BYO"

WORKER_API_OVERRIDE="$TMP/worker_api_override.yaml"
cat > "$WORKER_API_OVERRIDE" <<'EOF'
dispatcher:
  apiBaseUrl: https://byo-api.example
worker:
  connectorReconciler:
    enabled: true
  extraEnv:
    - name: CURIE_API_URL
      value: http://operator.example:9000
EOF
assert_worker_api override curie http://operator.example:9000 chart -f "$WORKER_API_OVERRIDE"

WORKER_API_KEY_OVERRIDE="$TMP/worker_api_key_override.yaml"
cat > "$WORKER_API_KEY_OVERRIDE" <<'EOF'
worker:
  extraEnv:
    - name: CURIE_API_KEY
      valueFrom:
        secretKeyRef:
          name: operatorapisecret
          key: apiKey
EOF
assert_worker_api key_override curie http://curie-api:8000 operator operatorapisecret apiKey -f "$WORKER_API_KEY_OVERRIDE"
echo "  ok: default and connector enabled renders use one chart API key reference; operator key override renders once without a chart duplicate"

echo "=== Assertion 13: security probe uses the configured RustFS port (#1507) ==="
SECURITY_PROBE_PORT="$TMP/security_probe_port.yaml"
helm template curie "$CHART" \
  --set rustfs.port=9100 \
  --show-only templates/security-probe.yaml > "$SECURITY_PROBE_PORT"
python3 - "$SECURITY_PROBE_PORT" <<'PYEOF'
import sys

import yaml

manifest = sys.argv[1]
docs = [doc for doc in yaml.safe_load_all(open(manifest)) if doc]
probes = []
for doc in docs:
    if doc.get("kind") != "Job":
        continue
    containers = doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    probe_containers = [container for container in containers if container.get("name") == "probe"]
    if probe_containers:
        probes.extend(probe_containers)

if len(probes) != 1:
    raise SystemExit(f"expected exactly one security probe container, found {len(probes)}")

entries = [
    entry
    for entry in probes[0].get("env", [])
    if entry.get("name") == "DATATIER_TARGETS"
]
if len(entries) != 1:
    raise SystemExit(f"DATATIER_TARGETS appears {len(entries)} times in the security probe")

targets = entries[0].get("value", "").split()
expected = "curie-rustfs:9100"
forbidden = "curie-rustfs:9000"
failures = []
if expected not in targets:
    failures.append(
        f"must include the configured RustFS target {expected!r}"
    )
if forbidden in targets:
    failures.append(
        f"must exclude the default RustFS target {forbidden!r} after rustfs.port=9100"
    )
if failures:
    raise SystemExit(f"DATATIER_TARGETS {' and '.join(failures)}; got {targets!r}")

print(f"  ok: DATATIER_TARGETS includes {expected!r} and excludes {forbidden!r}")
PYEOF

echo "=== Assertion 14: durable three-signal OTel Collector contract (#1818, #1819) ==="
bash "$SCRIPT_DIR/otel-collector-durability-assertions.sh"

echo
echo "PASS: sealed render generates strong values for all 11 keys (encryptionKey 64-hex and Langfuse init credentials 32 alphanumeric); dev overlay keeps published defaults; explicit credential and OTel overrides win on the sealed path; default OTel Basic auth uses the resolved Langfuse project secret; every runner boot-env name is a declared contract key (proven by a failing negative control); every control-plane pod, the agent-sandbox controller, and the sandbox render with the expected priorityClassName, including under operator override; the runner SandboxTemplate opts the controller out of its own permissive NetworkPolicy whenever Rail 1 is on, and leaves it to the controller's default when Rail 1 is off; api.githubToken stays a plain pass-through (empty renders empty, an explicit value renders verbatim, and it is never generated), proven by a failing negative control; the worker renders exactly one API URL plus exactly one correctly sourced API key in default, connector enabled, release name, configured port, BYO API, and operator override cases; the security probe uses the configured RustFS port in DATATIER_TARGETS; and the Collector render carries durable bounded delivery for traces, logs, and metrics."
