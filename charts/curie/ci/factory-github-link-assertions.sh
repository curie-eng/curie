#!/usr/bin/env bash
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check() {
  python3 -c '
import sys, yaml
configured = sys.argv[1] == "configured"
docs = [doc for doc in yaml.safe_load_all(sys.stdin) if doc]
api = next(doc for doc in docs if doc.get("kind") == "Deployment" and doc["metadata"]["name"].endswith("-curie-api"))
env = {item["name"]: item for item in api["spec"]["template"]["spec"]["containers"][0]["env"]}
client = env["FACTORY_GITHUB_APP_CLIENT_ID"]
assert client == {"name": "FACTORY_GITHUB_APP_CLIENT_ID", "value": "Iv1.example" if configured else ""}, client
secret = env.get("FACTORY_GITHUB_APP_CLIENT_SECRET")
if configured:
    assert secret == {"name": "FACTORY_GITHUB_APP_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": "acme-github-oauth", "key": "oauthSecret"}}}, secret
else:
    assert secret is None, secret
for doc in docs:
    if doc.get("kind") == "Secret":
        assert "oauthSecret" not in doc.get("data", {}), doc["metadata"]["name"]
        assert "oauthSecret" not in doc.get("stringData", {}), doc["metadata"]["name"]
' "$1"
}

helm template rel "$CHART" -f "$CHART/values-dev.yaml" | check defaults
helm template rel "$CHART" -f "$CHART/values-dev.yaml" \
  --set-string factory.githubAppClientId=Iv1.example \
  --set-string factory.githubAppClientSecretExistingSecret=acme-github-oauth \
  --set-string factory.githubAppClientSecretExistingSecretKey=oauthSecret | check configured
echo "OK GitHub link client credentials use an existing Secret"
