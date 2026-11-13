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
import base64
import hashlib
import pathlib
from datetime import datetime, timezone
import shutil
import subprocess
import sys
import yaml

work = pathlib.Path(sys.argv[1])
checksums = []
for name in ("install", "upgrade"):
    objects = [item for item in yaml.safe_load_all((work / (name + ".yaml")).read_text()) if item]
    deployments = {item["metadata"]["name"]: item for item in objects if item["kind"] == "Deployment"}
    secret = next(item for item in objects if item["metadata"]["name"] == "acme-curie-channel-gateway-tls")
    assert secret["kind"] == "Secret" and set(secret["data"]) == {"ca.crt", "tls.crt", "tls.key"}
    dates = []
    for key in ("ca.crt", "tls.crt"):
        result = subprocess.run(["openssl", "x509", "-noout", "-enddate"],
                                input=base64.b64decode(secret["data"][key]), capture_output=True, check=True)
        expiry = datetime.strptime(result.stdout.decode().strip().split("=", 1)[1], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        assert (expiry - datetime.now(timezone.utc)).days >= 3649
        dates.append(expiry)
    assert abs((dates[0] - dates[1]).total_seconds()) < 60
    hashes = {deployments["acme-curie-" + component]["spec"]["template"]["metadata"]["annotations"]["checksum/gateway-tls"]
              for component in ("api", "worker", "channel-gateway")}
    expected = hashlib.sha256(("acme-curie-channel-gateway-tls\n" + yaml.safe_dump(secret["data"], sort_keys=True).rstrip()).encode()).hexdigest()
    assert hashes == {expected}, "TLS Secret and all rollout hashes must share the same data"
    checksums.append(expected)
    for component in ("api", "worker", "channel-gateway"):
        pod = deployments["acme-curie-" + component]["spec"]["template"]["spec"]
        env = {entry["name"]: entry for entry in pod["containers"][0]["env"]}
        assert pod["enableServiceLinks"] is False
        if component == "channel-gateway":
            assert next(v["secret"] for v in pod["volumes"] if v["name"] == "tls")["defaultMode"] == 0o400
            assert env["SLACK_BOT_TOKEN"]["valueFrom"]["secretKeyRef"] == {"name": "acme-slack", "key": "bot"}
        else:
            assert not {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN", "SLACK_USER_TOKEN"} & env.keys()
            assert env["CURIE_CHANNEL_GATEWAY_URL"]["value"] == "https://acme-curie-channel-gateway:8081"
            assert env["CURIE_CHANNEL_GATEWAY_CA_FILE"]["value"] == "/var/run/curie/gateway-tls/ca.crt"
            assert any(volume.get("secret", {}).get("secretName") == "acme-curie-channel-gateway-tls" for volume in pod["volumes"])
assert checksums[0] != checksums[1], "regenerated TLS data must change rollout checksums"
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

supplied = gateway_objects(chart)
for deployment in supplied:
    if deployment["kind"] == "Deployment" and deployment["metadata"]["name"] in {"acme-curie-api", "acme-curie-worker"}:
        volume = next(v["secret"] for v in deployment["spec"]["template"]["spec"]["volumes"] if v["name"] == "gateway-tls")
        assert volume["optional"] is True and volume["items"] == [{"key": "ca.crt", "path": "ca.crt"}]
assert supplied == gateway_objects(retained_chart), "retained gateway defaults differ"
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


def gateway_env(*sets):
    args = ["helm", "template", "acme", sys.argv[2], "-f", sys.argv[3]]
    for value in sets:
        args += ["--set", value]
    rendered = subprocess.run(args, capture_output=True, text=True, check=True).stdout
    gateway = next(item for item in yaml.safe_load_all(rendered)
                   if item and item["kind"] == "Deployment"
                   and item["metadata"]["name"] == "acme-curie-channel-gateway")
    return {entry["name"]: entry for entry in gateway["spec"]["template"]["spec"]["containers"][0]["env"]}


# @spec GATEWAY-USER-EMAIL-6: the identity caller exists only when an operator Secret is named.
off = gateway_env()
assert "CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN" not in off
assert "CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S" not in off
on = gateway_env("channelGateway.identityLookup.existingSecret=idp-gateway",
                 "channelGateway.identityLookup.tokenKey=caller")
assert on["CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN"]["valueFrom"] == {
    "secretKeyRef": {"name": "idp-gateway", "key": "caller"}}
assert on["CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S"]["value"] == "5"
for bad in ("channelGateway.identityLookup.timeoutSeconds=0",
            "channelGateway.identityLookup.tokenKey="):
    result = subprocess.run(["helm", "template", "acme", sys.argv[2], "-f", sys.argv[3],
                             "--set", "channelGateway.identityLookup.existingSecret=idp-gateway",
                             "--set", bad], capture_output=True, text=True)
    assert result.returncode != 0, bad + " was accepted"

print("PASS: unchanged pre-gateway Slack values render on install and upgrade; TLS, token custody and retained defaults are wired")
PYTHON
