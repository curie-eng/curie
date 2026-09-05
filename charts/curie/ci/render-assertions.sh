#!/usr/bin/env bash
#
# Render-assertion tests for the chart's rendered output.
#
# Issue #195 (auto-generate strong per-release chart credentials), Assertions
# 1-5. Proves three things about the chart's credential Secret:
#
#   1. A SEALED render (security.allowDevDefaults defaults false, `lookup` empty
#      offline under `helm template`) GENERATES a strong random value for each of
#      the twelve chart-owned secret keys instead of shipping the published dev
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
# Issue #1762 (quiet API migration startup), Assertion 14 and its negative
# control. The migration init container must wait for Postgres with bounded,
# quiet retries before preserving the original Alembic upgrade command.
#
# Issue #2323 (NOTES app-service image tags), Assertion 15 and its negative
# control. NOTES.txt must print the same image reference the corresponding
# Deployment renders for api, dispatcher, worker, and ui. A rendered
# reference that ends in a bare colon is refused, because docker/crictl
# resolve `repo:` to `:latest`, which is a different image from the
# appVersion tag the pods run.
#
# Runnable locally (from anywhere) and from CI. Fails loudly, naming the key.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"

# The twelve chart-owned secret keys, each with its published dev default.
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
  approvalChatAttesterSecret
  githubWebhookSecret
)
# The published dev default for each of those keys. A `case` lookup rather than
# `declare -A`: associative arrays are bash 4+, and macOS ships bash 3.2 (the
# last GPLv2 release) as /bin/bash, which `#!/usr/bin/env bash` finds unless the
# contributor installed a newer one. Under `set -u` the unsupported `declare -A`
# made the first key look like an unbound variable, so `curie dev chart-check`
# failed on every stock Mac with "postgresPassword: unbound variable" -- an error
# that named nothing real and pointed at no fix. The other twenty-one assertion
# scripts already run on 3.2; this was the only one that did not.
default_for() {
  case "$1" in
    postgresPassword)             printf '%s\n' "postgres" ;;
    valkeyPassword)               printf '%s\n' "valkeypass" ;;
    clickhousePassword)           printf '%s\n' "clickhouse" ;;
    rustfsSecretKey)              printf '%s\n' "rustfssecret" ;;
    langfuseSalt)                 printf '%s\n' "dev-salt-change-me" ;;
    langfuseEncryptionKey)        printf '%s\n' "0000000000000000000000000000000000000000000000000000000000000000" ;;
    langfuseNextauthSecret)       printf '%s\n' "dev-nextauth-secret-change-me" ;;
    langfuseInitProjectSecretKey) printf '%s\n' "sk-lf-curie-dev" ;;
    langfuseInitUserPassword)     printf '%s\n' "curie-dev-password" ;;
    apiKey)                       printf '%s\n' "curie-dev-key" ;;
    approvalChatAttesterSecret)   printf '%s\n' "curie-dev-approval-chat-attester" ;;
    githubWebhookSecret)          printf '%s\n' "dev-webhook-secret" ;;
    *) return 1 ;;
  esac
}

# Prove the lookup is total over KEYS here, at startup, rather than relying on a
# failure at the point of use. `default_for` is called inside a command
# substitution, where a nonzero exit cannot abort the script the way the old
# `set -u` array miss did -- and one call site sits under `||`, which suspends
# `set -e` for the whole function body. Checking up front keeps the old property
# that a key with no published default is a hard error, not a silent empty
# string that makes the generation detector pass by accident.
for key in "${KEYS[@]}"; do
  default_for "$key" >/dev/null \
    || { echo "FAIL: default_for() has no published default for key '$key'" >&2; exit 1; }
done

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
  def="$(default_for "$key")"
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
  def="$(default_for "$key")"
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
  --set api.approvalChatAttesterSecret=override-attester-2194 \
  --set langfuse.init.projectSecretKey=override-project-secret-1569 \
  --set langfuse.init.userPassword=override-user-password-1569 \
  --show-only templates/secrets.yaml > "$OVERRIDE"
got="$(read_key "$OVERRIDE" apiKey)"
if [[ "$got" != "override-sentinel-xyz" ]]; then
  fail "explicit --set api.apiKey override must be honored on the sealed path; expected 'override-sentinel-xyz', got '$got'."
fi
echo "  ok: explicit apiKey override honored (override wins over generation)"
got_attester="$(read_key "$OVERRIDE" approvalChatAttesterSecret)"
if [[ "$got_attester" != "override-attester-2194" ]]; then
  fail "explicit --set api.approvalChatAttesterSecret override must be honored verbatim; expected 'override-attester-2194', got '$got_attester'."
fi
if [[ "$got_attester" == "$got" ]]; then
  fail "explicit api.approvalChatAttesterSecret override must remain distinct from api.apiKey; both rendered '$got'."
fi
echo "  ok: explicit approval chat attester override honored and distinct from apiKey"
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

echo "=== Assertion 4b: a legacy retained nil attester generates independently (#2194) ==="
# A v0.8.2 release has no api.approvalChatAttesterSecret. On an upgrade that
# replays the old release config, Helm represents the absent new key as nil.
# Preserve an ordinary retained override in the fixture so this exercises the
# legacy values shape rather than a one-key synthetic probe.
LEGACY_ATTESTER_VALUES="$TMP/legacy-attester-values.yaml"
cat > "$LEGACY_ATTESTER_VALUES" <<'YAMLEOF'
api:
  apiKey: legacy-retained
  approvalChatAttesterSecret: null
global:
  storageClass: legacy-retained-storage-class-2194
YAMLEOF
LEGACY_ATTESTER="$TMP/legacy-attester.yaml"
helm template legacy-attester "$CHART" \
  -f "$LEGACY_ATTESTER_VALUES" \
  --show-only templates/secrets.yaml > "$LEGACY_ATTESTER"
legacy_api_key="$(read_key "$LEGACY_ATTESTER" apiKey)"
legacy_attester="$(read_key "$LEGACY_ATTESTER" approvalChatAttesterSecret)"
if [[ -z "${legacy_attester//[[:space:]]/}" ]]; then
  fail "a legacy retained nil api.approvalChatAttesterSecret rendered blank; the managed Secret must generate a nonblank attester (#2194)."
fi
if [[ "$legacy_attester" == "$legacy_api_key" ]]; then
  fail "a legacy retained nil api.approvalChatAttesterSecret resolved to apiKey; the attester must use an independent trust domain (#2194)."
fi
echo "  ok: legacy retained nil attester resolves nonblank and differs from apiKey"

echo "=== Assertion 4c: explicit blank attesters are rejected (#2194) ==="
for label in empty whitespace; do
  EXPLICIT_BLANK_VALUES="$TMP/explicit-blank-attester-$label.yaml"
  if [[ "$label" == "empty" ]]; then
    printf '%s\n' 'api:' '  approvalChatAttesterSecret: ""' > "$EXPLICIT_BLANK_VALUES"
  else
    printf '%s\n' 'api:' '  approvalChatAttesterSecret: "   "' > "$EXPLICIT_BLANK_VALUES"
  fi
  explicit_blank_output=""
  if explicit_blank_output="$(helm template explicit-blank-attester "$CHART" \
      -f "$EXPLICIT_BLANK_VALUES" --show-only templates/secrets.yaml 2>&1)"; then
    fail "explicit $label api.approvalChatAttesterSecret rendered successfully; values validation must reject blank credentials (#2194)."
  fi
  if [[ "$explicit_blank_output" != *"approvalChatAttesterSecret"* ]]; then
    fail "explicit $label attester was rejected for an unexpected reason: $explicit_blank_output"
  fi
  echo "  ok: explicit $label attester is rejected by the chart values contract"
done

echo "=== Assertion 4d negative control: nil-unsafe managedSecret FAILS (#2194) ==="
# Replace only the managedSecret helper in a temporary chart copy with its
# pre-fix body. The same legacy-values assertion above must then observe the
# zero-byte attester that escaped the v0.8.2 -> v0.8.3 upgrade.
ATTESTER_MUTANT="$TMP/mutant-managed-secret-nil"
cp -a "$CHART" "$ATTESTER_MUTANT"
python3 - "$ATTESTER_MUTANT/templates/_helpers.tpl" <<'PYEOF'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
start_marker = '{{- define "curie.managedSecret" -}}'
end_marker = '{{/* ---- Shared first-party-app environment fragments ---- */}}'
start = text.find(start_marker)
end = text.find(end_marker, start)
if start == -1 or end == -1:
    sys.stderr.write("negative control could not find curie.managedSecret to mutate\n")
    sys.exit(1)
old_body = r'''{{- define "curie.managedSecret" -}}
{{- if eq (toString .root.Values.security.allowDevDefaults) "true" -}}{{/* string-coercion safety -- a quoted "false" must not read as truthy and silently ship a published default (fail closed to generation). */}}
{{- .value -}}
{{- else if ne (toString .value) (toString .default) -}}
{{- .value -}}
{{- else if hasKey .existingData .key -}}
{{- index .existingData .key | b64dec -}}
{{- else if .hex -}}
{{- randAlphaNum 32 | sha256sum -}}
{{- else -}}
{{- randAlphaNum 32 -}}
{{- end -}}
{{- end -}}'''
path.write_text(text[:start] + old_body + "\n\n" + text[end:])
PYEOF
ATTESTER_MUTANT_OUT="$TMP/mutant-managed-secret-nil.yaml"
helm template legacy-attester "$ATTESTER_MUTANT" \
  -f "$LEGACY_ATTESTER_VALUES" \
  --show-only templates/secrets.yaml > "$ATTESTER_MUTANT_OUT"
mutant_attester="$(read_key "$ATTESTER_MUTANT_OUT" approvalChatAttesterSecret)"
if [[ -n "$mutant_attester" ]]; then
  fail "nil-unsafe managedSecret negative control did not reproduce the exact blank attester; got a nonblank value."
fi
echo "  ok: restoring the nil-unsafe helper reproduces the blank legacy attester"

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

echo "=== Assertion 4e: default OTel auth uses the resolved Langfuse project secret ==="
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
  --set inference.persistence.enabled=true \
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
  --set ui.deploy=false \
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

echo "=== Placement assertion 1: every rendered pod surface receives its placement class ==="
PLACEMENT_VALUES="$TMP/placement-values.yaml"
cat > "$PLACEMENT_VALUES" <<'YAMLEOF'
placement:
  platform:
    podLabels:
      example.com/fargate-profile: platform
    annotations:
      example.com/placement: platform
    nodeSelector:
      example.com/node-class: platform
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: platform
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: example.com/node-class
                  operator: In
                  values: [platform]
  data:
    podLabels:
      example.com/fargate-profile: data
    annotations:
      example.com/placement: data
    nodeSelector:
      example.com/node-class: data
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: data
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: example.com/node-class
                  operator: In
                  values: [data]
  sandbox:
    podLabels:
      example.com/fargate-profile: sandbox
    annotations:
      example.com/placement: sandbox
    nodeSelector:
      example.com/node-class: sandbox
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: sandbox
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: example.com/node-class
                  operator: In
                  values: [sandbox]
  hooks:
    podLabels:
      example.com/fargate-profile: hooks
    annotations:
      example.com/placement: hooks
    nodeSelector:
      example.com/node-class: hooks
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: hooks
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: example.com/node-class
                  operator: In
                  values: [hooks]
  controller:
    podLabels:
      example.com/fargate-profile: controller
    annotations:
      example.com/placement: controller
    nodeSelector:
      example.com/node-class: controller
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: controller
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: example.com/node-class
                  operator: In
                  values: [controller]
YAMLEOF

PLACEMENT_CHECK="$TMP/check_placement.py"
cat > "$PLACEMENT_CHECK" <<'PYEOF'
"""Assert placement classes on every chart rendered pod surface.

argv: <rendered-dir> <populated|default|platform-only>
Exits 0 on pass, 1 naming the missing, extra, or mismatched surface on failure.
"""
import pathlib
import sys

import yaml

rendered, mode = sys.argv[1], sys.argv[2]
prefix = "placement-render-curie"

expected = {
    ("Deployment", f"{prefix}-api"): "platform",
    ("Deployment", f"{prefix}-dispatcher"): "platform",
    ("Deployment", f"{prefix}-worker"): "platform",
    ("Deployment", f"{prefix}-ui"): "platform",
    ("Deployment", f"{prefix}-inference"): "platform",
    ("Deployment", f"{prefix}-otel-collector"): "platform",
    ("Deployment", f"{prefix}-langfuse-web"): "platform",
    ("Deployment", f"{prefix}-langfuse-worker"): "platform",
    ("Deployment", f"{prefix}-mail-adapter"): "platform",
    ("StatefulSet", f"{prefix}-postgres"): "data",
    ("StatefulSet", f"{prefix}-valkey"): "data",
    ("StatefulSet", f"{prefix}-clickhouse"): "data",
    ("StatefulSet", f"{prefix}-rustfs"): "data",
    ("SandboxTemplate", f"{prefix}-runner"): "sandbox",
    ("DaemonSet", f"{prefix}-runner-prewarm"): "sandbox",
    ("Job", f"{prefix}-rustfs-init"): "hooks",
    ("Job", f"{prefix}-preflight-avx"): "hooks",
    ("Job", f"{prefix}-preflight-gvisor"): "hooks",
    ("Job", f"{prefix}-preflight-controller"): "hooks",
    ("Job", f"{prefix}-netpol-probe"): "hooks",
    ("Job", f"{prefix}-security-probe"): "hooks",
    ("Job", f"{prefix}-langfuse-model-pricing"): "hooks",
    # The pre-upgrade drain gate and its post-upgrade release (issue #2010).
    ("Job", f"{prefix}-upgrade-drain"): "hooks",
    ("Job", f"{prefix}-upgrade-drain-release"): "hooks",
    # The single schema upgrade phase (#2300).
    ("Job", f"{prefix}-schema-migrate"): "hooks",
    ("Job", f"{prefix}-grafana-token-updater"): "hooks",
    ("Job", f"{prefix}-grafana-token-cleanup"): "hooks",
    ("Job", f"{prefix}-mail-persistence-preflight"): "hooks",
    ("Pod", f"{prefix}-security-probe-hardening"): "hooks",
    ("Deployment", "agent-sandbox-controller"): "controller",
}


def pod_surface(doc):
    kind = doc.get("kind")
    if kind in {"Deployment", "StatefulSet", "DaemonSet", "Job"}:
        return doc.get("spec", {}).get("template", {}) or {}
    if kind == "Pod":
        return {"metadata": doc.get("metadata", {}) or {}, "spec": doc.get("spec", {}) or {}}
    if kind == "SandboxTemplate":
        return doc.get("spec", {}).get("podTemplate", {}) or {}
    return None


surfaces = {}
for path in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(path.read_text()):
        if not isinstance(doc, dict):
            continue
        surface = pod_surface(doc)
        if surface is None:
            continue
        key = (doc.get("kind"), doc.get("metadata", {}).get("name"))
        if key in surfaces:
            sys.stderr.write(f"duplicate rendered pod surface {key!r}\n")
            sys.exit(1)
        surfaces[key] = surface

missing = sorted(set(expected) - set(surfaces))
unexpected = sorted(set(surfaces) - set(expected))
if missing or unexpected:
    if missing:
        sys.stderr.write(f"render is missing expected pod surfaces: {missing!r}\n")
    if unexpected:
        sys.stderr.write(f"render has unclassified pod surfaces: {unexpected!r}\n")
    sys.exit(1)


def wanted_placement(class_name):
    return {
        "label": class_name,
        "annotation": class_name,
        "nodeSelector": {"example.com/node-class": class_name},
        "tolerations": [{
            "key": "example.com/dedicated",
            "operator": "Equal",
            "value": class_name,
            "effect": "NoSchedule",
        }],
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{
                        "matchExpressions": [{
                            "key": "example.com/node-class",
                            "operator": "In",
                            "values": [class_name],
                        }],
                    }],
                },
            },
        },
    }


def assert_populated(key, surface, class_name):
    metadata = surface.get("metadata", {}) or {}
    spec = surface.get("spec", {}) or {}
    labels = metadata.get("labels", {}) or {}
    annotations = metadata.get("annotations", {}) or {}
    want = wanted_placement(class_name)
    got = {
        "label": labels.get("example.com/fargate-profile"),
        "annotation": annotations.get("example.com/placement"),
        "nodeSelector": spec.get("nodeSelector"),
        "tolerations": spec.get("tolerations"),
        "affinity": spec.get("affinity"),
    }
    if got != want:
        sys.stderr.write(
            f"pod surface {key!r} has placement {got!r}, expected class "
            f"{class_name!r} with placement {want!r}\n"
        )
        sys.exit(1)


if mode == "populated":
    for key, class_name in expected.items():
        assert_populated(key, surfaces[key], class_name)
    print(f"  ok: all {len(expected)} pod surfaces carry their exact populated placement class")
elif mode == "default":
    for key, surface in surfaces.items():
        metadata = surface.get("metadata", {}) or {}
        labels = metadata.get("labels", {}) or {}
        annotations = metadata.get("annotations", {}) or {}
        spec = surface.get("spec", {}) or {}
        if "example.com/fargate-profile" in labels:
            sys.stderr.write(f"default pod surface {key!r} unexpectedly has the placement marker label\n")
            sys.exit(1)
        if "example.com/placement" in annotations:
            sys.stderr.write(f"default pod surface {key!r} unexpectedly has the placement marker annotation\n")
            sys.exit(1)
        present = [field for field in ("nodeSelector", "tolerations", "affinity") if field in spec]
        if present:
            sys.stderr.write(
                f"default pod surface {key!r} unexpectedly has placement fields {present!r}\n"
            )
            sys.exit(1)
    print(f"  ok: all {len(expected)} pod surfaces omit placement markers and scheduling fields by default")
elif mode == "platform-only":
    marker = "example.com/fargate-profile"
    for key, class_name in expected.items():
        labels = surfaces[key].get("metadata", {}).get("labels", {}) or {}
        got = labels.get(marker)
        if class_name == "platform" and got != "platform":
            sys.stderr.write(
                f"platform pod surface {key!r} has {marker}={got!r}, expected 'platform'\n"
            )
            sys.exit(1)
        if class_name != "platform" and marker in labels:
            sys.stderr.write(
                f"unselected {class_name} pod surface {key!r} leaked platform marker {got!r}\n"
            )
            sys.exit(1)
    print("  ok: the platform marker reaches only platform pods and is absent from data, sandbox, hooks, and controller pods")
else:
    sys.stderr.write(f"unknown placement check mode {mode!r}\n")
    sys.exit(2)
PYEOF

# Enable every conditional pod surface so the expected inventory is exhaustive.
# mailAdapter.deploy also needs a narrow provider CIDR (fail-closed egress) and
# an existingClaim so the persistence preflight Job actually renders. The Job
# name is mail-persistence-preflight, not mail-adapter-persistence.
PLACEMENT_HELM_ARGS=(
  --set dispatcher.slack.appToken=xapp-placement-render
  --set dispatcher.slack.botToken=xoxb-placement-render
  --set inference.deploy=true
  --set inference.persistence.enabled=true
  --set security.gvisor.mode=require
  --set mailAdapter.deploy=true
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24'
  --set mailAdapter.persistence.existingClaim=placement-render-mail-state
  --set grafanaConnector.enabled=true
)

PLACEMENT_OUT="$(mktemp -d -p "$TMP")"
helm template placement-render "$CHART" --output-dir "$PLACEMENT_OUT" \
  -f "$PLACEMENT_VALUES" "${PLACEMENT_HELM_ARGS[@]}" > /dev/null
python3 "$PLACEMENT_CHECK" "$PLACEMENT_OUT" populated \
  || fail "populated placement classes did not reach every exact rendered pod surface."

echo "=== Placement assertion 2: empty placement defaults preserve all pod surfaces ==="
PLACEMENT_DEFAULT_OUT="$(mktemp -d -p "$TMP")"
helm template placement-render "$CHART" --output-dir "$PLACEMENT_DEFAULT_OUT" \
  "${PLACEMENT_HELM_ARGS[@]}" > /dev/null
python3 "$PLACEMENT_CHECK" "$PLACEMENT_DEFAULT_OUT" default \
  || fail "empty placement defaults changed rendered pod metadata or scheduling fields."

echo "=== Placement assertion 3: a platform-only marker does not leak across classes ==="
PLACEMENT_PLATFORM_ONLY="$TMP/placement-platform-only.yaml"
cat > "$PLACEMENT_PLATFORM_ONLY" <<'YAMLEOF'
placement:
  platform:
    podLabels:
      example.com/fargate-profile: platform
YAMLEOF
PLACEMENT_PLATFORM_ONLY_OUT="$(mktemp -d -p "$TMP")"
helm template placement-render "$CHART" --output-dir "$PLACEMENT_PLATFORM_ONLY_OUT" \
  -f "$PLACEMENT_PLATFORM_ONLY" "${PLACEMENT_HELM_ARGS[@]}" > /dev/null
python3 "$PLACEMENT_CHECK" "$PLACEMENT_PLATFORM_ONLY_OUT" platform-only \
  || fail "the platform-only marker was absent from a platform pod or leaked into another placement class."

echo "=== Placement assertion 4: a legacy release's retained placement: null still renders (#2008) ==="
# `helm upgrade --reuse-values` replays the STORED release config as this
# upgrade's user-supplied values -- it is not a merge over the chart's current
# defaults. Helm's values coalescing then deletes any top-level key whose
# replayed value is YAML null outright, so a release created before placement
# classes existed, which stored `placement: null`, hands that null straight
# back in on every future upgrade. `.Values.placement` is then nil and every
# `.Values.placement.<class>` lookup in the templates panics with a nil
# pointer template error (issue #2008). A live `helm upgrade --dry-run
# --reuse-values` reproduction needs an actual prior Helm release, which this
# chart-only CI has no cluster or release history to provide -- rendering a
# fixture that reproduces the exact coalesced shape (`placement: null`
# alongside an ordinary retained setting, so this reads as a real retained
# values file and not a one-key probe) is the faithful, cluster-free stand-in
# for the same upgrade path.
PLACEMENT_LEGACY_NULL="$TMP/placement-legacy-null.yaml"
cat > "$PLACEMENT_LEGACY_NULL" <<'YAMLEOF'
placement: null
global:
  storageClass: gp3-legacy
YAMLEOF

PLACEMENT_LEGACY_NULL_OUT="$(mktemp -d -p "$TMP")"
PLACEMENT_LEGACY_NULL_ERR="$TMP/placement-legacy-null.err"
# set -euo pipefail would otherwise kill the script silently at this line on
# the very failure we are testing for; capture stderr and check the exit
# status explicitly so a broken render reports a useful message instead.
if ! helm template placement-render "$CHART" --output-dir "$PLACEMENT_LEGACY_NULL_OUT" \
  -f "$PLACEMENT_LEGACY_NULL" "${PLACEMENT_HELM_ARGS[@]}" \
  > /dev/null 2>"$PLACEMENT_LEGACY_NULL_ERR"; then
  fail "a legacy release's retained 'placement: null' failed to render (#2008): $(cat "$PLACEMENT_LEGACY_NULL_ERR")"
fi
python3 "$PLACEMENT_CHECK" "$PLACEMENT_LEGACY_NULL_OUT" default \
  || fail "a legacy release's retained 'placement: null' rendered but did not degrade to the chart's empty placement defaults (#2008): every expected pod surface must still be present and none may carry a placement marker or scheduling field."

echo "=== Placement assertion 4 negative control: reverting the nil-safe placement accessor FAILS ==="
# Mandatory, per Assertion 7's convention: an assert that has never been shown
# failing is not a pin. Mutate a TEMP COPY of the chart's _helpers.tpl and
# require the legacy-null render to fail again.
#
# The #2008 fix has not landed on this branch yet, so this mutation cannot
# target known-existing text the way Assertion 7's and 12b's negative
# controls do. It instead locates its mutation target -- the nil-safe
# placement accessor in _helpers.tpl -- by NAME: the `curie.placement.class`
# define the fix is expected to introduce. It mutates that define's body back
# into a raw, nil-UNSAFE `.Values.placement.<class>`-style dereference by
# stripping this chart's own established `| default dict` nil-guard idiom
# (see _helpers.tpl:766) from inside it. If the define cannot be found, or is
# found but does not use that idiom, the mutation script fails loudly naming
# what it could not find rather than silently no-op'ing, so a renamed or
# differently-shaped fix does not leave this negative control quietly
# pinning nothing.
PLACEMENT_MUTANT="$TMP/mutant-placement"
cp -a "$CHART" "$PLACEMENT_MUTANT"
python3 - "$PLACEMENT_MUTANT/templates/_helpers.tpl" <<'PYEOF'
import pathlib
import re
import sys

p = pathlib.Path(sys.argv[1])
text = p.read_text()

block_re = re.compile(
    r'{{-?\s*define "curie\.placement\.class"\s*-?}}.*?{{-?\s*end\s*-?}}',
    re.DOTALL,
)
m = block_re.search(text)
if not m:
    sys.stderr.write(
        "negative control could not find a 'curie.placement.class' define in "
        "_helpers.tpl to mutate. That is the nil-safe placement accessor "
        "Assertion 4 (#2008) expects the fix to introduce; if the fix named "
        "it something else, update this negative control's anchor to match.\n"
    )
    sys.exit(1)

block = m.group(0)
if "| default dict" not in block:
    sys.stderr.write(
        "negative control found 'curie.placement.class' but no '| default "
        "dict' nil-guard idiom (this chart's established pattern, see "
        "_helpers.tpl:766) inside it to strip. Update this negative control "
        "to match however the #2008 fix actually guards against nil "
        ".Values.placement.\n"
    )
    sys.exit(1)

mutated_block = block.replace("| default dict", "", 1)
p.write_text(text.replace(block, mutated_block, 1))
PYEOF

PLACEMENT_MUTANT_OUT="$(mktemp -d -p "$TMP")"
if helm template placement-render "$PLACEMENT_MUTANT" --output-dir "$PLACEMENT_MUTANT_OUT" \
  -f "$PLACEMENT_LEGACY_NULL" "${PLACEMENT_HELM_ARGS[@]}" > /dev/null 2>&1; then
  fail "negative control did not fire: a legacy release's retained 'placement: null' still rendered after stripping the nil-guard from curie.placement.class, so Placement assertion 4 (#2008) is not actually pinning anything."
fi
echo "  ok: stripping the nil-guard from curie.placement.class makes the legacy-null render fail again (the assert can fail)"

echo "=== Placement assertion 5: a malformed placement class is refused, not silently dropped (#2008) ==="
# The #2008 nil tolerance routes every class lookup through
# `curie.placement.class`, which hands its result to the three consumers as
# YAML. Helm's `fromYaml` returns an ERROR MAP rather than raising on a non-map
# document, so without a kind refusal in that helper a malformed class renders
# clean with `.podLabels`/`.annotations`/`.nodeSelector` all resolving to
# nothing -- every scheduling constraint the operator asked for silently
# dropped, and workloads free to land on unintended nodes. The pre-#2008
# templates aborted on this shape; these asserts pin that the fix stayed
# fail-CLOSED on it while only the nil case became tolerant.
PLACEMENT_MALFORMED_CLASS="$TMP/placement-malformed-class.yaml"
cat > "$PLACEMENT_MALFORMED_CLASS" <<'YAMLEOF'
placement:
  platform: spot
YAMLEOF

PLACEMENT_MALFORMED_CLASS_OUT="$(mktemp -d -p "$TMP")"
PLACEMENT_MALFORMED_CLASS_ERR="$TMP/placement-malformed-class.err"
# Same `set -euo pipefail` care as assertion 4: the expected outcome here is a
# FAILING render, so check the exit status explicitly instead of letting the
# non-zero status kill the script.
if helm template placement-render "$CHART" --output-dir "$PLACEMENT_MALFORMED_CLASS_OUT" \
  -f "$PLACEMENT_MALFORMED_CLASS" "${PLACEMENT_HELM_ARGS[@]}" \
  > /dev/null 2>"$PLACEMENT_MALFORMED_CLASS_ERR"; then
  fail "a malformed placement class ('placement.platform: spot', a scalar where a map of placement fields belongs) rendered successfully and silently dropped its scheduling constraints -- that is the #2008 fail-open regression: the chart must refuse the shape, not quietly schedule the workload anywhere."
fi
grep -q 'placement\.platform' "$PLACEMENT_MALFORMED_CLASS_ERR" \
  || fail "a malformed placement class was refused, but the error never names the offending class ('placement.platform'), so the refusal is an incidental crash rather than an actionable message: $(cat "$PLACEMENT_MALFORMED_CLASS_ERR")"

PLACEMENT_MALFORMED_TOP="$TMP/placement-malformed-top.yaml"
cat > "$PLACEMENT_MALFORMED_TOP" <<'YAMLEOF'
placement: spot
YAMLEOF

PLACEMENT_MALFORMED_TOP_OUT="$(mktemp -d -p "$TMP")"
PLACEMENT_MALFORMED_TOP_ERR="$TMP/placement-malformed-top.err"
if helm template placement-render "$CHART" --output-dir "$PLACEMENT_MALFORMED_TOP_OUT" \
  -f "$PLACEMENT_MALFORMED_TOP" "${PLACEMENT_HELM_ARGS[@]}" \
  > /dev/null 2>"$PLACEMENT_MALFORMED_TOP_ERR"; then
  fail "a malformed top-level 'placement: spot' rendered successfully and silently dropped every placement class -- the chart must refuse a non-map placement, not quietly schedule every workload anywhere (#2008 fail-open regression)."
fi
grep -q 'placement' "$PLACEMENT_MALFORMED_TOP_ERR" \
  || fail "a malformed top-level placement was refused, but the error never mentions 'placement', so the refusal is an incidental crash rather than an actionable message: $(cat "$PLACEMENT_MALFORMED_TOP_ERR")"

# Positive control: the refusal must be narrow. The legacy nil path from
# assertion 4 -- the whole point of #2008 -- must still render.
PLACEMENT_REFUSAL_CONTROL_OUT="$(mktemp -d -p "$TMP")"
PLACEMENT_REFUSAL_CONTROL_ERR="$TMP/placement-refusal-control.err"
if ! helm template placement-render "$CHART" --output-dir "$PLACEMENT_REFUSAL_CONTROL_OUT" \
  -f "$PLACEMENT_LEGACY_NULL" "${PLACEMENT_HELM_ARGS[@]}" \
  > /dev/null 2>"$PLACEMENT_REFUSAL_CONTROL_ERR"; then
  fail "the malformed-placement refusal is too broad: a legacy release's retained 'placement: null' no longer renders, so the fail-closed guard swallowed the #2008 nil tolerance it was supposed to preserve: $(cat "$PLACEMENT_REFUSAL_CONTROL_ERR")"
fi
echo "  ok: malformed placement values are refused by name, and legacy 'placement: null' still renders"

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
  # `${a[@]+"${a[@]}"}` rather than a bare `"${key_args[@]}"`: expanding an EMPTY
  # array under `set -u` is an "unbound variable" error until bash 4.4, and macOS
  # ships 3.2. key_args is empty for every non-operator key source, which is most
  # of the call sites below.
  if ! error="$(python3 "$WORKER_API_CHECK" "$manifest" "$secret_manifest" "$expected_url" "$key_source" ${key_args[@]+"${key_args[@]}"} 2>&1)"; then
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
  # Rail 1 needs a CIDR peer for a BYO API (#2317); this fixture only cares
  # about the worker's CURIE_API_URL.
  egress:
    - cidr: 192.0.2.41/32
      ports: [{ protocol: TCP, port: 443 }]
dispatcher:
  apiBaseUrl: https://byo-api.example
ui:
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

echo "=== Assertion 14: API schema-wait init waits quietly for Postgres then the upgrade phase (#2300) ==="
API_MIGRATE_OUT="$TMP/api_migrate"
helm template curie "$CHART" --output-dir "$API_MIGRATE_OUT" >/dev/null
API_MIGRATE_RENDER="$API_MIGRATE_OUT/curie/templates/api.yaml"
[[ -f "$API_MIGRATE_RENDER" ]] || fail "api.yaml did not render"

API_MIGRATE_CHECK="$TMP/check_api_migrate_wait.py"
cat > "$API_MIGRATE_CHECK" <<'PYEOF'
"""Execute the rendered API schema-wait init command with fake dependencies."""

import os
import pathlib
import subprocess
import sys
import tempfile

import yaml


def fail(message):
    raise SystemExit(message)


def migrate_container(manifest):
    matches = []
    for doc in yaml.safe_load_all(pathlib.Path(manifest).read_text()):
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        containers = (
            doc.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("initContainers", [])
        )
        matches.extend(item for item in containers if item.get("name") == "schema-wait")
        alembic = [item for item in containers if item.get("name") == "migrate"]
        if alembic:
            fail("API init must not run a migrate container; Alembic belongs on the upgrade Job")
    if len(matches) != 1:
        fail(f"expected exactly one schema-wait init container, found {len(matches)}")
    return matches[0]


def shell_process(container):
    process = list(container.get("command") or []) + list(container.get("args") or [])
    if len(process) < 3 or pathlib.Path(process[0]).name not in {"sh", "bash"}:
        fail(
            "schema-wait init container must use a shell readiness wait before "
            "the schema probe; a direct Alembic command is not retry-safe"
        )
    if process[1] != "-c":
        fail("schema-wait init container shell command must use -c")
    script = process[2]
    if "alembic" in script:
        fail("schema-wait init must not invoke Alembic; migrations belong on the upgrade Job")
    wait = "exec python -m curie_api.schema_compat wait"
    if wait not in script:
        fail(f"schema-wait init command must preserve {wait!r}")
    if not any(token in script[: script.index(wait)] for token in ("python", "python3")):
        fail("schema-wait init command has no supported Postgres readiness probe before the wait")
    env_names = {item.get("name") for item in container.get("env", [])}
    if "DATABASE_URL" not in env_names:
        fail("schema-wait init container must receive DATABASE_URL from the Postgres environment")
    return process


def write_program(path, text):
    path.write_text("#!/bin/sh\n" + text)
    path.chmod(0o755)


def run_case(process, readiness_failures):
    with tempfile.TemporaryDirectory() as temp:
        root = pathlib.Path(temp)
        fake_bin = root / "bin"
        fake_bin.mkdir()
        (fake_bin / "python").symlink_to(sys.executable)
        fake_modules = root / "modules"
        fake_modules.mkdir()
        attempts = root / "readiness-attempts"
        wait_calls = root / "wait-calls"

        write_program(fake_bin / "sleep", "exit 0\n")
        compat_pkg = fake_modules / "curie_api"
        compat_pkg.mkdir()
        (compat_pkg / "__init__.py").write_text("")
        (compat_pkg / "schema_compat.py").write_text(
            "import os, pathlib, sys\n"
            "pathlib.Path(os.environ['WAIT_CALLS']).write_text(' '.join(sys.argv[1:]) + '\\n')\n"
        )
        (fake_modules / "asyncpg.py").write_text(
            """\
import os
import pathlib


class InvalidPasswordError(Exception):
    pass


class Connection:
    async def close(self):
        pass


async def connect(database_url, timeout):
    attempts = pathlib.Path(os.environ["READINESS_ATTEMPTS"])
    lines = attempts.read_text().splitlines() if attempts.exists() else []
    count = int(lines[-1]) + 1 if lines else 1
    with attempts.open("a") as stream:
        stream.write(f"{count}\\n")
    if count <= int(os.environ["READINESS_FAILURES"]):
        raise InvalidPasswordError("asyncpg-password-sentinel-must-not-leak")
    return Connection()
"""
        )

        env = os.environ.copy()
        env.update(
            {
                "WAIT_CALLS": str(wait_calls),
                "DATABASE_URL": (
                    "postgresql+asyncpg://curie:EXAMPLE_NOT_A_SECRET@postgres:5432/curie"
                ),
                "PATH": f"{fake_bin}:{env['PATH']}",
                "PYTHONPATH": (
                    f"{fake_modules}{os.pathsep}{env['PYTHONPATH']}"
                    if env.get("PYTHONPATH")
                    else str(fake_modules)
                ),
                "READINESS_ATTEMPTS": str(attempts),
                "READINESS_FAILURES": str(readiness_failures),
            }
        )
        try:
            result = subprocess.run(
                process,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            fail("migrate readiness wait did not terminate within 60 seconds with fake sleep")

        attempt_lines = attempts.read_text().splitlines() if attempts.exists() else []
        calls = wait_calls.read_text().splitlines() if wait_calls.exists() else []
        return result, attempt_lines, calls


container = migrate_container(sys.argv[1])
process = shell_process(container)

ready, ready_attempts, ready_calls = run_case(process, 0)
if ready.returncode != 0:
    fail(f"immediate readiness exited {ready.returncode}: {ready.stdout}{ready.stderr}")
if len(ready_attempts) != 1:
    fail(f"immediate readiness ran the probe {len(ready_attempts)} times, expected once")
if ready_calls != ["wait"]:
    fail(f"immediate readiness did not invoke schema_compat wait: {ready_calls!r}")

delayed, delayed_attempts, delayed_calls = run_case(process, 2)
if delayed.returncode != 0:
    fail(f"delayed readiness exited {delayed.returncode}: {delayed.stdout}{delayed.stderr}")
if len(delayed_attempts) != 3:
    fail(f"delayed readiness ran the probe {len(delayed_attempts)} times, expected three")
if delayed_calls != ["wait"]:
    fail(f"delayed readiness did not invoke schema_compat wait: {delayed_calls!r}")
delayed_output = [
    line.strip()
    for line in (delayed.stdout + delayed.stderr).splitlines()
    if line.strip()
]
if delayed_output != ["Waiting for Postgres readiness"]:
    fail(f"delayed readiness must log one concise wait line: {delayed_output!r}")

exhausted, exhausted_attempts, exhausted_calls = run_case(process, 60)
if exhausted.returncode == 0:
    fail("readiness exhaustion must exit nonzero so the init container can restart")
if len(exhausted_attempts) != 60:
    fail(f"readiness exhaustion must make exactly 60 attempts; observed {len(exhausted_attempts)}")
if exhausted_calls:
    fail(f"readiness exhaustion must not invoke schema wait; got {exhausted_calls!r}")

output_lines = [
    line.strip()
    for line in (exhausted.stdout + exhausted.stderr).splitlines()
    if line.strip()
]
if not output_lines:
    fail("readiness exhaustion must emit one concise terminal message")
if len(output_lines) > 2:
    fail(f"readiness exhaustion emitted {len(output_lines)} lines, expected at most 2")
if output_lines[0] != "Waiting for Postgres readiness":
    fail(f"first readiness failure must emit one concise wait message: {output_lines!r}")
lower_output = " ".join(output_lines).lower()
if "postgres" not in lower_output or not any(
    word in lower_output for word in ("ready", "wait", "timeout", "unavailable")
):
    fail(f"readiness exhaustion message must explain the Postgres wait: {output_lines!r}")
if "InvalidPasswordError" not in " ".join(output_lines):
    fail(f"readiness exhaustion must retain the final probe error class: {output_lines!r}")
if "traceback" in lower_output or "sqlalchemy.exc" in lower_output:
    fail(f"readiness exhaustion emitted a traceback: {output_lines!r}")
if any(
    secret in lower_output
    for secret in (
        "example_not_a_secret",
        "postgresql+asyncpg://",
        "asyncpg-password-sentinel-must-not-leak",
    )
):
    fail(f"readiness exhaustion exposed database credentials: {output_lines!r}")

print(
    "  ok: immediate and delayed readiness run schema_compat wait; bounded "
    "exhaustion stays concise, exits nonzero, and never invokes the wait"
)
PYEOF

python3 "$API_MIGRATE_CHECK" "$API_MIGRATE_RENDER" \
  || fail "API migrate init command does not implement the bounded quiet readiness contract."

echo "=== Assertion 14 negative control: changed readiness bound FAILS ==="
API_MIGRATE_BOUND_MUTANT="$TMP/mutant-api-migrate-bound"
cp -a "$CHART" "$API_MIGRATE_BOUND_MUTANT"
python3 - "$API_MIGRATE_BOUND_MUTANT/templates/api.yaml" <<'PYEOF'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
old = "              max_attempts=60\n"
if text.count(old) != 1:
    sys.stderr.write("bound negative control could not find the readiness bound\n")
    sys.exit(1)
path.write_text(text.replace(old, "              max_attempts=4\n", 1))
PYEOF
API_MIGRATE_BOUND_MUTANT_OUT="$TMP/api_migrate_bound_mutant"
helm template curie "$API_MIGRATE_BOUND_MUTANT" \
  --output-dir "$API_MIGRATE_BOUND_MUTANT_OUT" >/dev/null
API_MIGRATE_BOUND_MUTANT_RENDER="$API_MIGRATE_BOUND_MUTANT_OUT/curie/templates/api.yaml"
[[ -f "$API_MIGRATE_BOUND_MUTANT_RENDER" ]] || fail "bound mutant api.yaml did not render"
api_migrate_bound_negative_output=""
if api_migrate_bound_negative_output="$(python3 "$API_MIGRATE_CHECK" "$API_MIGRATE_BOUND_MUTANT_RENDER" 2>&1)"; then
  fail "negative control did not fire: a changed readiness bound passed the contract."
fi
if [[ "$api_migrate_bound_negative_output" != *"must make exactly 60 attempts"* ]]; then
  fail "readiness-bound negative control failed unexpectedly: $api_migrate_bound_negative_output"
fi
echo "  ok: a changed readiness bound is rejected (the boundedness assert can fail)"

echo "=== Assertion 14 negative control: direct Alembic on the API init FAILS ==="
# Mutate a temporary chart copy to the pre-#2300 command and require the same
# rendered-command checker to reject it.
API_MIGRATE_MUTANT="$TMP/mutant-api-migrate"
cp -a "$CHART" "$API_MIGRATE_MUTANT"
python3 - "$API_MIGRATE_MUTANT/templates/api.yaml" <<'PYEOF'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
start_marker = '          command: ["/bin/sh", "-c"]\n'
end_marker = "          env:\n"
start = text.find(start_marker)
end = text.find(end_marker, start)
if start == -1 or end == -1:
    sys.stderr.write("negative control could not find the migration readiness command\n")
    sys.exit(1)
direct_command = '          command: ["alembic", "-c", "alembic.ini", "upgrade", "head"]\n'
path.write_text(text[:start] + direct_command + text[end:])
PYEOF
API_MIGRATE_MUTANT_OUT="$TMP/api_migrate_mutant"
helm template curie "$API_MIGRATE_MUTANT" --output-dir "$API_MIGRATE_MUTANT_OUT" >/dev/null
API_MIGRATE_MUTANT_RENDER="$API_MIGRATE_MUTANT_OUT/curie/templates/api.yaml"
[[ -f "$API_MIGRATE_MUTANT_RENDER" ]] || fail "mutant api.yaml did not render"
api_migrate_negative_output=""
if api_migrate_negative_output="$(python3 "$API_MIGRATE_CHECK" "$API_MIGRATE_MUTANT_RENDER" 2>&1)"; then
  fail "negative control did not fire: a direct Alembic API init passed the schema-wait contract."
fi
if [[ "$api_migrate_negative_output" != *"direct Alembic command is not retry-safe"* ]]; then
  fail "direct-Alembic negative control failed unexpectedly: $api_migrate_negative_output"
fi
echo "  ok: a direct Alembic API init is rejected (the assert can fail)"

echo "=== Assertion 15: NOTES app-service images match Deployments and never end in a bare colon (#2323) ==="
# helm template does not emit NOTES.txt. Render it through tpl so the
# operator-facing image refs are asserted on the same consumer path as
# `helm install`. Same probe as clickhouse-langfuse-pin-assertions.sh.
NOTES_IMAGE_CHART="$TMP/notes-image-chart"
cp -a "$CHART" "$NOTES_IMAGE_CHART"
cp "$CHART/templates/NOTES.txt" "$NOTES_IMAGE_CHART/NOTES.txt"
cat >"$NOTES_IMAGE_CHART/templates/notes-image-check.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-image-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF

NOTES_IMAGE_CHECK="$TMP/check_notes_app_images.py"
cat >"$NOTES_IMAGE_CHECK" <<'PYEOF'
"""Assert NOTES app-service image refs match Deployment images and refuse a bare colon.

argv: <notes-configmap.yaml> <rendered-dir> <svc> [<svc> ...]
Exits 0 on pass, 1 naming the offending service or image on failure.
"""
import pathlib
import re
import sys

import yaml

notes_path, rendered_dir, *services = sys.argv[1:]
if not services:
    sys.stderr.write("expected at least one service name\n")
    sys.exit(1)

notes_doc = yaml.safe_load(pathlib.Path(notes_path).read_text())
notes = ((notes_doc or {}).get("data") or {}).get("notes") or ""
found = {}
for match in re.finditer(
    r"^  - (api|dispatcher|worker|ui) \(([^)]*)\)",
    notes,
    flags=re.MULTILINE,
):
    found[match.group(1)] = match.group(2).rstrip()

for svc in services:
    image = found.get(svc)
    if image is None:
        sys.stderr.write(
            "NOTES image reference for %r is missing from the rendered notes\n" % svc
        )
        sys.exit(1)
    if image.endswith(":"):
        sys.stderr.write(
            "NOTES image reference for %r ends in a bare colon: %r\n" % (svc, image)
        )
        sys.exit(1)

    deploy_path = pathlib.Path(rendered_dir) / "curie" / "templates" / ("%s.yaml" % svc)
    if not deploy_path.is_file():
        sys.stderr.write("Deployment render is missing: %s\n" % deploy_path)
        sys.exit(1)
    deploy_image = None
    for doc in yaml.safe_load_all(deploy_path.read_text()) or []:
        if not doc or doc.get("kind") != "Deployment":
            continue
        containers = (
            ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
        ).get("containers") or []
        for container in containers:
            if container.get("name") == svc:
                deploy_image = container.get("image")
                break
        if deploy_image is not None:
            break
    if not deploy_image:
        sys.stderr.write(
            "Deployment %s has no container named %r\n" % (deploy_path, svc)
        )
        sys.exit(1)
    if str(deploy_image).endswith(":"):
        sys.stderr.write(
            "Deployment image for %r ends in a bare colon: %r\n" % (svc, deploy_image)
        )
        sys.exit(1)
    if image != deploy_image:
        sys.stderr.write(
            "NOTES image for %r is %r but Deployment renders %r\n"
            % (svc, image, deploy_image)
        )
        sys.exit(1)

print("ok: NOTES images match Deployments for %s" % ", ".join(services))
PYEOF

render_notes_images() {
  local dest="$1"
  shift
  helm template curie "$NOTES_IMAGE_CHART" \
    --show-only templates/notes-image-check.yaml \
    "$@" >"$dest"
}

NOTES_IMAGE_DEFAULT="$TMP/notes-images-default.yaml"
NOTES_IMAGE_DEFAULT_OUT="$TMP/notes-images-default-out"
mkdir -p "$NOTES_IMAGE_DEFAULT_OUT"
helm template curie "$CHART" --output-dir "$NOTES_IMAGE_DEFAULT_OUT" >/dev/null
render_notes_images "$NOTES_IMAGE_DEFAULT"
python3 "$NOTES_IMAGE_CHECK" "$NOTES_IMAGE_DEFAULT" "$NOTES_IMAGE_DEFAULT_OUT" \
  api worker ui \
  || fail "default NOTES app-service images must match the corresponding Deployments and must not end in a bare colon."
echo "  ok: default NOTES api/worker/ui images match Deployments (no Slack tokens)"

NOTES_IMAGE_SLACK="$TMP/notes-images-slack.yaml"
NOTES_IMAGE_SLACK_OUT="$TMP/notes-images-slack-out"
mkdir -p "$NOTES_IMAGE_SLACK_OUT"
helm template curie "$CHART" --output-dir "$NOTES_IMAGE_SLACK_OUT" \
  --set dispatcher.slack.appToken=xapp-render-assert \
  --set dispatcher.slack.botToken=xoxb-render-assert \
  >/dev/null
render_notes_images "$NOTES_IMAGE_SLACK" \
  --set dispatcher.slack.appToken=xapp-render-assert \
  --set dispatcher.slack.botToken=xoxb-render-assert
python3 "$NOTES_IMAGE_CHECK" "$NOTES_IMAGE_SLACK" "$NOTES_IMAGE_SLACK_OUT" \
  api dispatcher worker ui \
  || fail "Slack-enabled NOTES app-service images must match the corresponding Deployments and must not end in a bare colon."
echo "  ok: Slack-enabled NOTES api/dispatcher/worker/ui images match Deployments"

NOTES_IMAGE_PIN="$TMP/notes-images-pin.yaml"
NOTES_IMAGE_PIN_OUT="$TMP/notes-images-pin-out"
mkdir -p "$NOTES_IMAGE_PIN_OUT"
helm template curie "$CHART" --output-dir "$NOTES_IMAGE_PIN_OUT" \
  --set api.image.tag=pin-2323 >/dev/null
render_notes_images "$NOTES_IMAGE_PIN" --set api.image.tag=pin-2323
python3 "$NOTES_IMAGE_CHECK" "$NOTES_IMAGE_PIN" "$NOTES_IMAGE_PIN_OUT" \
  api worker ui \
  || fail "explicit api.image.tag must appear in both NOTES and the API Deployment."
if ! grep -q 'curie-api:pin-2323' "$NOTES_IMAGE_PIN"; then
  fail "explicit api.image.tag=pin-2323 did not appear in rendered NOTES."
fi
echo "  ok: explicit api.image.tag=pin-2323 is printed in NOTES and the API Deployment"

echo "=== Assertion 15 negative control: a bare-colon NOTES image FAILS ==="
NOTES_IMAGE_MUTANT="$TMP/mutant-notes-image"
cp -a "$NOTES_IMAGE_CHART" "$NOTES_IMAGE_MUTANT"
python3 - "$NOTES_IMAGE_MUTANT/templates/NOTES.txt" <<'PYEOF'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
services = ("api", "dispatcher", "worker", "ui")
replaced = 0
for svc in services:
    include = (
        '{{ include "curie.image" (dict "repository" .Values.%s.image.repository'
        ' "tag" .Values.%s.image.tag "digest" .Values.%s.image.digest'
        ' "defaultTag" .Chart.AppVersion) }}' % (svc, svc, svc)
    )
    old = "{{ .Values.%s.image.repository }}:{{ .Values.%s.image.tag }}" % (svc, svc)
    if include in text:
        text = text.replace(include, old)
        replaced += 1
    elif old in text:
        replaced += 1
    else:
        sys.stderr.write("negative control could not find the %s image expression\n" % svc)
        sys.exit(1)
if replaced != 4:
    sys.stderr.write("negative control expected to rewrite 4 image expressions\n")
    sys.exit(1)
path.write_text(text)
PYEOF
cp "$NOTES_IMAGE_MUTANT/templates/NOTES.txt" "$NOTES_IMAGE_MUTANT/NOTES.txt"
NOTES_IMAGE_MUTANT_RENDER="$TMP/notes-images-mutant.yaml"
NOTES_IMAGE_MUTANT_OUT="$TMP/notes-images-mutant-out"
mkdir -p "$NOTES_IMAGE_MUTANT_OUT"
helm template curie "$CHART" --output-dir "$NOTES_IMAGE_MUTANT_OUT" >/dev/null
helm template curie "$NOTES_IMAGE_MUTANT" \
  --show-only templates/notes-image-check.yaml \
  >"$NOTES_IMAGE_MUTANT_RENDER"
notes_image_negative_output=""
if notes_image_negative_output="$(python3 "$NOTES_IMAGE_CHECK" "$NOTES_IMAGE_MUTANT_RENDER" "$NOTES_IMAGE_MUTANT_OUT" api worker ui 2>&1)"; then
  fail "negative control did not fire: NOTES interpolated from image.tag still passed the bare-colon assert."
fi
if [[ "$notes_image_negative_output" != *"ends in a bare colon"* ]]; then
  fail "bare-colon negative control failed unexpectedly: $notes_image_negative_output"
fi
echo "  ok: a NOTES image interpolated from empty image.tag is rejected (the assert can fail)"

echo
echo "PASS: sealed render generates strong values for all 12 keys (encryptionKey 64-hex and Langfuse init credentials 32 alphanumeric); dev overlay keeps published defaults; explicit credential and OTel overrides win on the sealed path; default OTel Basic auth uses the resolved Langfuse project secret; every runner boot-env name is a declared contract key (proven by a failing negative control); every control-plane pod, the agent-sandbox controller, and the sandbox render with the expected priorityClassName, including under operator override; the runner SandboxTemplate opts the controller out of its own permissive NetworkPolicy whenever Rail 1 is on, and leaves it to the controller's default when Rail 1 is off; api.githubToken stays a plain pass-through (empty renders empty, an explicit value renders verbatim, and it is never generated), proven by a failing negative control; every rendered pod surface receives its exact placement class while empty defaults omit placement fields and a platform-only label does not leak across classes; the worker renders exactly one API URL plus exactly one correctly sourced API key in default, connector enabled, release name, configured port, BYO API, and operator override cases; the security probe uses the configured RustFS port in DATATIER_TARGETS; and the API schema-wait init command waits quietly with bounded retries before the upgrade-phase wait, with readiness exhaustion proven to exit nonzero without invoking schema_compat wait; and NOTES prints the same app-service image references the corresponding Deployments render, refusing a bare trailing colon (proven by a failing negative control)."
