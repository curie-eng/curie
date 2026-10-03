#!/usr/bin/env bash
# Real Helm assertions for opt-in API ingress routing, TLS and Service identity.
set -euo pipefail
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL [$1] $2" >&2; exit 1; }
render() { helm template identity-check "$CHART" "$@" >"$TMP/render.yaml"; }
check() {
  python3 - "$TMP/render.yaml" "$1" <<'PY'
import sys, yaml
from pathlib import Path
case = sys.argv[2]
docs = [d for d in yaml.safe_load_all(Path(sys.argv[1]).read_text()) if d]
def one(kind, component=None):
    found = [d for d in docs if d['kind'] == kind and (component is None or d.get('metadata', {}).get('labels', {}).get('app.kubernetes.io/component') == component)]
    assert len(found) == 1, f'expected one {kind}/{component}, got {len(found)}'
    return found[0]
try:
    if case == 'default':
        assert not any(d['kind'] == 'Ingress' for d in docs), 'ingress enabled by default'
    else:
        ingress = one('Ingress')
        spec = ingress['spec']
        rules = spec['rules']
        assert len(rules) == 1 and rules[0]['host'] == 'api.example.com', 'rule host mismatch'
        paths = rules[0]['http']['paths']
        assert len(paths) == 1, 'expected one API path'
        path = paths[0]
        assert path['path'] == '/api' and path['pathType'] == 'Prefix', 'path or pathType mismatch'
        assert spec['ingressClassName'] == 'example-controller', 'ingress class mismatch'
        assert ingress['metadata']['annotations']['cert-manager.io/cluster-issuer'] == 'example-issuer', 'cluster issuer mismatch'
        service = one('Service', 'api')
        backend = path['backend']['service']
        assert backend['name'] == service['metadata']['name'], 'backend does not point at rendered API Service'
        assert backend['port']['number'] == 9999, 'configured backend port lost'
        assert any(p['port'] == backend['port']['number'] for p in service['spec']['ports']), 'backend port absent from API Service'
        if case == 'disabled':
            assert 'tls' not in spec, 'disabled TLS block still rendered'
        else:
            tls = spec['tls']
            assert len(tls) == 1 and tls[0]['hosts'] == [rules[0]['host']], 'TLS host mismatch'
            if case == 'secret':
                assert tls[0]['secretName'] == 'example-cert', 'certificate Secret mismatch'
            else:
                assert 'secretName' not in tls[0], 'empty certificate Secret must be omitted'
except (AssertionError, KeyError, TypeError) as exc:
    print(f'FAIL [{case}] {exc}', file=sys.stderr)
    sys.exit(1)
PY
}
render
check default
BASE=(--set api.ingress.enabled=true --set api.ingress.host=api.example.com
  --set api.ingress.path=/api --set api.ingress.pathType=Prefix
  --set api.ingress.className=example-controller --set api.service.port=9999
  --set-string 'api.ingress.annotations.cert-manager\.io/cluster-issuer=example-issuer')
# Missing host must fail even with TLS disabled: no hostless catch-all rule.
for tls in true false; do
  if helm template identity-check "$CHART" --set api.ingress.enabled=true \
      --set api.ingress.tls.enabled="$tls" >"$TMP/refused" 2>&1; then
    fail host "missing host accepted with tls.enabled=$tls"
  fi
  grep -q 'api.ingress.host is required' "$TMP/refused" || fail host "missing readable host refusal"
done
render "${BASE[@]}"
check enabled
render "${BASE[@]}" --set api.ingress.tls.enabled=false
check disabled
render "${BASE[@]}" --set api.ingress.tls.secretName=example-cert
check secret
# Helm coalesces tls: {} with defaults; tls: null deletes the parent tree.
for value in '{}' null; do
  printf 'api:\n  ingress:\n    tls: %s\n' "$value" >"$TMP/tls.yaml"
  render "${BASE[@]}" -f "$TMP/tls.yaml"
  if [[ "$value" == null ]]; then check disabled; else check enabled; fi
done

# Check the API container environment, not an unrelated workload or a
# similarly named value elsewhere in the rendered chart.
render -s templates/api.yaml
python3 - "$TMP/render.yaml" "$CHART/values.yaml" <<'PY' || exit 1
import sys
from pathlib import Path

import yaml

deployments = [
    document for document in yaml.safe_load_all(Path(sys.argv[1]).read_text())
    if document and document.get("kind") == "Deployment"
]
if len(deployments) != 1:
    raise SystemExit(f"FAIL [g] expected one API Deployment, found {len(deployments)}")
containers = deployments[0]["spec"]["template"]["spec"]["containers"]
api = [container for container in containers if container["name"] == "api"]
if len(api) != 1:
    raise SystemExit(f"FAIL [g] expected one API container, found {len(api)}")
values = [
    entry.get("value") for entry in api[0]["env"]
    if entry["name"] == "FORWARDED_ALLOW_IPS"
]
if values != [""]:
    raise SystemExit(f"FAIL [g] FORWARDED_ALLOW_IPS rendered {values!r}, expected one empty value")
with open(sys.argv[2]) as stream:
    chart_values = yaml.safe_load(stream)
if "forwardedAllowIps" in chart_values["api"]:
    raise SystemExit("FAIL [g] api.forwardedAllowIps exposes forbidden proxy trust configuration")
PY

# Both narrow and universal trust overrides must fail before deployment.
for configured in '192.0.2.10' '*' '0.0.0.0/0'; do
  if render --set-string 'api.extraEnv[0].name=FORWARDED_ALLOW_IPS' \
      --set-string "api.extraEnv[0].value=$configured" \
      -s templates/api.yaml 2>"$TMP/refused"; then
    fail h "api.extraEnv accepted FORWARDED_ALLOW_IPS=$configured"
  fi
  grep -q 'api.extraEnv contains chart-owned environment variable FORWARDED_ALLOW_IPS' "$TMP/refused" \
    || fail h "the rejection of $configured did not name the reserved environment"
done

echo "api-ingress-assertions: all routing, identity, TLS and proxy trust assertions passed"
