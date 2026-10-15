#!/usr/bin/env bash
#
# Render-assertion test for the GitHub App credential (ADR-0092).
#
# Two properties, both learned the hard way on a live cluster:
#
#   (a) The App ID must reach the API as the DIGITS. helm's `--set` parses a
#       bare number and a --reuse-values round trip turns it into a float64, so
#       app id 1234567 renders as "1.234567e+06", the JWT's `iss` claim is wrong,
#       and every GitHub call answers 401. The chart must validate the ID and
#       quote its decimal digits, and the CLI must use --set-string.
#   (b) A BYO `githubAppExistingSecret` must make the chart REFERENCE a Secret
#       rather than carry the key, so the PEM never enters helm's stored values.
#       Without it the key is copied into every retained release revision (10 by
#       default) and `helm get values` can print it.
#
# Accepted App ID forms, invalid inputs, and Secret references are asserted.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }
VALUES_FILE="$(mktemp)"
trap 'rm -f "$VALUES_FILE"' EXIT

render() { helm template credential-check "$CHART" "$@" -s templates/api.yaml -s templates/secrets.yaml; }

check_reference() {
  python3 - "$1" "$2" "$3" <<'PYREF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.argv[1]) if d]
try:
    deployments = [d for d in docs if d['kind'] == 'Deployment' and d['metadata']['labels'].get('app.kubernetes.io/component') == 'api']
    assert len(deployments) == 1, 'expected one API Deployment'
    container = next(c for c in deployments[0]['spec']['template']['spec']['containers'] if c['name'] == 'api')
    entries = [e for e in container['env'] if e['name'] == 'GITHUB_APP_PRIVATE_KEY']
    assert len(entries) == 1, 'expected exactly one GITHUB_APP_PRIVATE_KEY env'
    entry = entries[0]
    assert 'value' not in entry, 'private key must reference a Secret'
    reference = entry['valueFrom']['secretKeyRef']
    name = sys.argv[2]
    if name == 'managed':
        secrets = [d for d in docs if d['kind'] == 'Secret' and 'githubAppPrivateKey' in d.get('stringData', {})]
        assert len(secrets) == 1, 'expected one rendered managed credential Secret'
        name = secrets[0]['metadata']['name']
    assert reference['name'] == name, f"Secret name {reference['name']!r} != {name!r}"
    assert reference['key'] == sys.argv[3], f"Secret key {reference['key']!r} != {sys.argv[3]!r}"
except (AssertionError, KeyError, StopIteration, TypeError) as exc:
    print(f'FAIL [credential reference] {exc}', file=sys.stderr)
    sys.exit(1)
PYREF
}

check_app_id() {
  local case_name="$1" expected="$2"
  shift 2
  local out
  out="$(render "$@")" || fail "$case_name" "valid App ID failed to render"
  python3 - "$case_name" "$expected" "$out" <<'PYID'
import sys, yaml
case_name, expected, rendered = sys.argv[1:]
try:
    deployments = [d for d in yaml.safe_load_all(rendered) if d and d.get('kind') == 'Deployment'
                   and d['metadata']['labels'].get('app.kubernetes.io/component') == 'api']
    assert len(deployments) == 1, 'expected one API Deployment'
    container = next(c for c in deployments[0]['spec']['template']['spec']['containers'] if c['name'] == 'api')
    entries = [e for e in container['env'] if e['name'] == 'GITHUB_APP_ID']
    assert len(entries) == 1, 'expected exactly one GITHUB_APP_ID env'
    value = entries[0].get('value')
    assert isinstance(value, str), f'App ID must be a quoted string, got {value!r}'
    assert value == expected, f'GITHUB_APP_ID rendered as {value!r}, expected {expected!r}'
except (AssertionError, KeyError, StopIteration, TypeError) as exc:
    print(f'FAIL [{case_name}] {exc}', file=sys.stderr)
    sys.exit(1)
PYID
}

reject_app_id() {
  local case_name="$1"
  shift
  local error
  if error="$(render "$@" 2>&1)"; then
    fail "$case_name" "invalid api.githubAppId rendered successfully"
  fi
  [[ "$error" == *"api.githubAppId"* ]] || fail "$case_name" "rejection must name api.githubAppId"
  [[ "$error" == *"empty string or a positive integer of 1 to 18 decimal digits"* ]] || \
    fail "$case_name" "rejection must describe the accepted App ID forms"
}

# The default and an explicitly empty string remain the exact empty string.
check_app_id default-empty ""
check_app_id explicit-empty "" --set-string api.githubAppId=

# Canonical strings preserve every decimal digit, including the upper bound.
check_app_id string-id 1234567 --set-string api.githubAppId=1234567 --set api.githubAppPrivateKey=X
check_app_id string-minimum 1 --set-string api.githubAppId=1
check_app_id string-precise-large 123456789012345678 --set-string api.githubAppId=123456789012345678
check_app_id string-maximum 999999999999999999 --set-string api.githubAppId=999999999999999999

# Integer inputs and retained float64 values render with decimal int64 digits.
check_app_id numeric-id 1234567 --set api.githubAppId=1234567
check_app_id numeric-minimum 1 --set api.githubAppId=1
check_app_id numeric-maximum 999999999999999999 --set api.githubAppId=999999999999999999
cat >"$VALUES_FILE" <<'EOF'
api:
  githubAppId: 4475970
EOF
check_app_id unquoted-values-id 4475970 -f "$VALUES_FILE"
cat >"$VALUES_FILE" <<'EOF'
api:
  githubAppId: 1234567.0
EOF
check_app_id retained-float-id 1234567 -f "$VALUES_FILE"
cat >"$VALUES_FILE" <<'EOF'
api:
  githubAppId: 1.234567e+06
EOF
check_app_id retained-scientific-id 1234567 -f "$VALUES_FILE"
cat >"$VALUES_FILE" <<'EOF'
api:
  githubAppId: 999999999999999872.0
EOF
check_app_id float-below-maximum 999999999999999872 -f "$VALUES_FILE"

# Noncanonical strings must fail before a credential can reach the API.
reject_app_id alphabetic-string --set-string api.githubAppId=abc
reject_app_id leading-zero-string --set-string api.githubAppId=01234567
reject_app_id scientific-string --set-string api.githubAppId=1.234567e+06
reject_app_id whitespace-string --set-string 'api.githubAppId= 123 '
reject_app_id zero-string --set-string api.githubAppId=0
reject_app_id negative-string --set-string api.githubAppId=-1
reject_app_id fractional-string --set-string api.githubAppId=1.5
reject_app_id plus-prefixed-string --set-string api.githubAppId=+123
reject_app_id string-above-maximum --set-string api.githubAppId=1000000000000000000
reject_app_id nineteen-digit-string --set-string api.githubAppId=1234567890123456789

# Only positive whole numeric values inside the decimal bound are accepted.
reject_app_id numeric-zero --set api.githubAppId=0
reject_app_id numeric-negative --set api.githubAppId=-5
reject_app_id numeric-above-maximum --set api.githubAppId=1000000000000000000
reject_app_id twenty-digit-bare-value --set api.githubAppId=12345678901234567890
while IFS='|' read -r case_name value; do
  cat >"$VALUES_FILE" <<EOF
api:
  githubAppId: $value
EOF
  reject_app_id "$case_name" -f "$VALUES_FILE"
done <<'EOF'
numeric-fraction|1.5
float-zero|0.0
float-negative|-1.0
float-rounded-maximum|999999999999999999.0
float-above-maximum|1.0e+18
boolean-true|true
boolean-false|false
list|[1234567]
empty-list|[]
map|{id: 1234567}
empty-map|{}
EOF

# (d) Default path references the managed Secret rendered in the same release.
OUT="$(render --set api.githubAppPrivateKey=X)"
check_reference "$OUT" managed githubAppPrivateKey

# (e) BYO name and key must belong to GITHUB_APP_PRIVATE_KEY, not another env.
OUT="$(render --set api.githubAppExistingSecret=example-app-key)"
check_reference "$OUT" example-app-key privateKey
OUT="$(render --set api.githubAppExistingSecret=example-app-key \
  --set api.githubAppExistingSecretKey=customPemKey)"
check_reference "$OUT" example-app-key customPemKey

# (f) BYO wins even with a stale inline key; retain the custom key selection.
OUT="$(render --set api.githubAppExistingSecret=example-app-key \
  --set api.githubAppExistingSecretKey=customPemKey --set api.githubAppPrivateKey=STALE)"
check_reference "$OUT" example-app-key customPemKey

echo "github-app-credential-assertions: all assertions passed"
