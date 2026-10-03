#!/usr/bin/env bash
#
# Render-assertion test for the GitHub App credential (ADR-0092).
#
# Two properties, both learned the hard way on a live cluster:
#
#   (a) The App ID must reach the API as the DIGITS. helm's `--set` parses a
#       bare number and a --reuse-values round trip turns it into a float64, so
#       app id 1234567 renders as "1.234567e+06", the JWT's `iss` claim is wrong,
#       and every GitHub call answers 401. The chart must quote whatever it is
#       given rather than emit a bare number, and the CLI must use --set-string.
#   (b) A BYO `githubAppExistingSecret` must make the chart REFERENCE a Secret
#       rather than carry the key, so the PEM never enters helm's stored values.
#       Without it the key is copied into every retained release revision (10 by
#       default) and `helm get values` can print it.
#
# Six assertions.
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

# (a) The App ID is emitted as a quoted string, never a bare number.
OUT="$(render --set-string api.githubAppId=1234567 --set api.githubAppPrivateKey=X)"
grep -q 'name: GITHUB_APP_ID' <<<"$OUT" || fail a "GITHUB_APP_ID is missing"
python3 - "$OUT" <<'PY' || exit 1
import sys, yaml
doc = [d for d in yaml.safe_load_all(sys.argv[1]) if d and d.get("kind") == "Deployment"][0]
env = {e["name"]: e for e in doc["spec"]["template"]["spec"]["containers"][0]["env"]}
value = env["GITHUB_APP_ID"].get("value")
if value != "1234567":
    print(f"FAIL [a] GITHUB_APP_ID rendered as {value!r}, expected '1234567'. "
          "A float here becomes '1.234567e+06' and every GitHub call 401s.",
          file=sys.stderr)
    sys.exit(1)
PY

# (b) An unquoted values file App ID renders as the exact decimal string.
cat >"$VALUES_FILE" <<'EOF'
api:
  githubAppId: 4475970
EOF
OUT="$(render -f "$VALUES_FILE")"
python3 - "$OUT" <<'PY' || exit 1
import sys, yaml
doc = [d for d in yaml.safe_load_all(sys.argv[1]) if d and d.get("kind") == "Deployment"][0]
env = {e["name"]: e for e in doc["spec"]["template"]["spec"]["containers"][0]["env"]}
value = env["GITHUB_APP_ID"].get("value")
if value != "4475970":
    print(f"FAIL [b] GITHUB_APP_ID rendered as {value!r}, expected '4475970'. "
          "An unquoted values file must preserve its decimal digits.",
          file=sys.stderr)
    sys.exit(1)
PY

# (c) Absent config renders the exact empty string, not a broken reference.
OUT="$(render)"
python3 - "$OUT" <<'PY' || exit 1
import sys, yaml
doc = [d for d in yaml.safe_load_all(sys.argv[1]) if d and d.get("kind") == "Deployment"][0]
env = {e["name"]: e for e in doc["spec"]["template"]["spec"]["containers"][0]["env"]}
value = env["GITHUB_APP_ID"].get("value")
if value != "":
    print(f"FAIL [c] GITHUB_APP_ID rendered as {value!r}, expected empty string.",
          file=sys.stderr)
    sys.exit(1)
PY

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

echo "github-app-credential-assertions: all six assertions passed"
