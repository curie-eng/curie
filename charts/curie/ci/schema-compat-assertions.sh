#!/usr/bin/env bash
# The packaged schema authority must render intact before any cluster mutation.
set -euo pipefail
chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT
helm template acme-bot "$chart_dir" > "$test_dir/default.yaml"
helm template acme-bot "$chart_dir" --set api.deploy=false --set ui.apiBaseUrl=http://api.example.com > "$test_dir/no-api.yaml"
helm template acme-bot "$chart_dir" --set api.image.tag=fixture-custom > "$test_dir/override.yaml"
helm package "$chart_dir" --destination "$test_dir" >/dev/null
helm template acme-bot "$test_dir"/*.tgz > "$test_dir/packaged.yaml"
python3 - "$test_dir" "$chart_dir" <<'PY'
import json
import pathlib
import sys
import yaml

root, chart = map(pathlib.Path, sys.argv[1:])
authority = json.loads((chart / 'files/schema-compat.json').read_text())
version = yaml.safe_load((chart / 'Chart.yaml').read_text())['appVersion']
for name in ['default', 'packaged', 'override', 'no-api']:
    docs = [doc for doc in yaml.safe_load_all((root / f'{name}.yaml').read_text()) if isinstance(doc, dict)]
    matches = [doc for doc in docs if doc.get('metadata', {}).get('labels', {}).get('app.kubernetes.io/component') == 'schema-compat']
    if name == 'no-api':
        assert matches == [], 'BYO API must not publish this chart as the serving schema authority'
        continue
    assert len(matches) == 1
    resource = matches[0]
    assert resource['kind'] == 'ConfigMap'
    assert resource['metadata']['name'] == 'acme-bot-curie-schema-compat'
    assert 'helm.sh/hook' not in resource['metadata'].get('annotations', {})
    assert resource['data']['application-version'] == version
    assert json.loads(resource['data']['compatibility.json']) == authority
    api = next(doc for doc in docs if doc.get('kind') == 'Deployment' and doc['metadata'].get('labels', {}).get('app.kubernetes.io/component') == 'api')
    container = next(item for item in api['spec']['template']['spec']['containers'] if item['name'] == 'api')
    assert resource['data']['api-image'] == container['image']
PY
cp -R "$chart_dir" "$test_dir/broken"
for damage in missing invalid; do
  if [[ "$damage" == missing ]]; then
    rm "$test_dir/broken/files/schema-compat.json"
  else
    echo 'not valid JSON' > "$test_dir/broken/files/schema-compat.json"
  fi
  if helm template acme-bot "$test_dir/broken" > "$test_dir/$damage.yaml" 2> "$test_dir/$damage.err"; then
    echo "schema metadata render accepted $damage packaged authority" >&2
    exit 1
  fi
  python3 -c 'import pathlib,sys; error=pathlib.Path(sys.argv[1]).read_text().lower(); assert "schema compatibility metadata" in error or "json" in error' "$test_dir/$damage.err"
done
printf '%s\n' 'Schema metadata render, artifact package and refusal assertions passed.'
