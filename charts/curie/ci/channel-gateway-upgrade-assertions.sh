#!/usr/bin/env bash
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
VALUES="$CHART/ci/fixtures/slack-pre-gateway-values.yaml"
helm template acme "$CHART" -f "$VALUES" > "$WORK/install.yaml"
helm template acme "$CHART" -f "$VALUES" --is-upgrade > "$WORK/upgrade.yaml"
helm template acme "$CHART" > "$WORK/no-slack.yaml"
if helm template acme "$CHART" -f "$VALUES" --set channelGateway.deploy=false > "$WORK/refused.yaml" 2> "$WORK/refused.err"; then
  echo "FAIL: explicit gateway disable accepted Slack without a URL" >&2
  exit 1
fi
grep -q "set channelGateway.deploy=true or channelGateway.url" "$WORK/refused.err"
python3 - "$WORK" "$CHART" "$VALUES" <<'PYTHON'
import pathlib
import shutil
import subprocess
import sys
import yaml

work = pathlib.Path(sys.argv[1])
for name in ("install", "upgrade"):
    objects = [item for item in yaml.safe_load_all((work / (name + ".yaml")).read_text()) if item]
    deployments = {item["metadata"]["name"]: item for item in objects if item["kind"] == "Deployment"}
    secret = next(item for item in objects if item["metadata"]["name"] == "acme-curie-channel-gateway-tls")
    assert secret["kind"] == "Secret" and set(secret["data"]) == {"ca.crt", "tls.crt", "tls.key"}
    for component in ("api", "worker", "channel-gateway"):
        pod = deployments["acme-curie-" + component]["spec"]["template"]["spec"]
        env = {entry["name"]: entry for entry in pod["containers"][0]["env"]}
        assert pod["enableServiceLinks"] is False
        if component == "channel-gateway":
            assert env["SLACK_BOT_TOKEN"]["valueFrom"]["secretKeyRef"] == {"name": "acme-slack", "key": "bot"}
        else:
            assert not {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN", "SLACK_USER_TOKEN"} & env.keys()
            assert env["CURIE_CHANNEL_GATEWAY_URL"]["value"] == "https://acme-curie-channel-gateway:8081"
            assert env["CURIE_CHANNEL_GATEWAY_CA_FILE"]["value"] == "/var/run/curie/gateway-tls/ca.crt"
            assert any(volume.get("secret", {}).get("secretName") == "acme-curie-channel-gateway-tls" for volume in pod["volumes"])
no_slack = list(yaml.safe_load_all((work / "no-slack.yaml").read_text()))
assert not any(item and item["metadata"].get("name", "").startswith("acme-curie-channel-gateway") for item in no_slack)
chart = pathlib.Path(sys.argv[2])
retained_chart = work / "retained-chart"
shutil.copytree(chart, retained_chart)
values = yaml.safe_load((chart / "values.yaml").read_text())
del values["channelGateway"]
(retained_chart / "values.yaml").write_text(yaml.safe_dump(values))

def gateway_objects(source):
    result = subprocess.run([
        "helm", "template", "acme", str(source), "-f", sys.argv[3],
        "--set", "security.allowDevDefaults=true",
        "--set", "channelGateway.tlsSecret=acme-gateway-tls",
    ], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    names = {"acme-curie-api", "acme-curie-worker", "acme-curie-channel-gateway",
             "acme-curie-channel-gateway-replay"}
    return [item for item in yaml.safe_load_all(result.stdout)
            if item and item["metadata"]["name"] in names]

assert gateway_objects(chart) == gateway_objects(retained_chart), "retained gateway defaults differ"
for malformed in (None, "resources: [invalid"):
    path = retained_chart / "files/channel-gateway-defaults.yaml"
    if malformed is None:
        path.unlink()
    else:
        path.write_text(malformed)
    result = subprocess.run(["helm", "template", "acme", str(retained_chart)],
                            capture_output=True, text=True)
    assert result.returncode != 0, "missing or malformed gateway defaults were accepted"
    assert "restore the chart defaults file" in result.stderr

print("PASS: unchanged pre-gateway Slack values render on install and upgrade; TLS, token custody and retained defaults are wired")
PYTHON
