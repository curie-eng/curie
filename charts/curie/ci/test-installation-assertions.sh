#!/usr/bin/env bash
# Render-assertion test for the test installation declaration (ADR 0202
# decision 1, #4133).
#
# `testInstallation.enabled` (default false) and `testInstallation.drivers`
# render as CURIE_TEST_INSTALLATION_ENABLED and CURIE_TEST_INSTALLATION_DRIVERS
# into both the API and the dispatcher, and only from the chart. The chart
# refuses to render the declaration on when api.environment is prod, when a
# published default secret (API key, worker token, approval attester secret)
# is in use, or when a driver has no channel.
#
# Assertions:
#   (a) Default render: both services carry "false" and "[]".
#   (b) values.yaml declares the defaults; the schema types enabled boolean,
#       so a quoted "true" cannot turn it on; `testInstallation=null` (a
#       --reuse-values release from before the key) renders the default.
#   (c) On, with real secrets: both services carry "true" and the same
#       normalized drivers JSON, and that exact string parses through the REAL
#       API Settings and DispatcherConfig into the same drivers.
#   (d) On with api.environment prod (any case or padding) fails, naming it.
#   (e) On with each published default secret fails, naming its value key;
#       a sealed install (no allowDevDefaults) generates them and renders.
#   (f) A driver with no channel fails, and so does every other malformed
#       entry; with values.schema.json removed, the template `fail` alone
#       still refuses a driver with no channel (the schema is not the only
#       gate).
#   (g) extraEnv on the API or the dispatcher may not set either variable.
#
# Runnable locally (from anywhere) and from CI. Render-only: no cluster.
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

python3 - "$CHART" "$WORK" <<'PY'
import json
import pathlib
import shutil
import subprocess
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
work = pathlib.Path(sys.argv[2])

ENABLED = "CURIE_TEST_INSTALLATION_ENABLED"
DRIVERS = "CURIE_TEST_INSTALLATION_DRIVERS"
# Placeholder ids only (AGENTS.md: never a real Slack id).
DRIVER = {"channel_id": "C0EXAMPLE1", "bot_id": "B0EXAMPLE1", "bot_user_id": "U0EXAMPLE1"}
SIBLING = {
    "channel_id": "G0EXAMPLE2",
    "bot_id": "B0EXAMPLE2",
    "bot_user_id": "U0EXAMPLE2",
    "agent": "acme-tester",
}
REAL_SECRETS = {
    "api": {"apiKey": "example-api-key", "approvalChatAttesterSecret": "example-attester"},
    "worker": {"internalWorkerToken": "example-worker-token"},
}

# The dispatcher renders only with the default identity's tokens.
base = {
    "dispatcher": {"slack": {"appToken": "xapp-example", "botToken": "xoxb-example"}},
    "agentSandbox": {"controller": {"deploy": False}},
    "worker": {"publication": {"enabled": False}},
}

count = 0


def merge(left, right):
    out = dict(left)
    for key, value in right.items():
        out[key] = merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


def render(*overlays, args=(), dev=True, source=None):
    """One helm template into its own output dir; returns (result, output)."""
    global count
    count += 1
    values = work / f"values-{count}.yaml"
    merged = base
    for overlay in overlays:
        merged = merge(merged, overlay)
    values.write_text(yaml.safe_dump(merged))
    output = work / str(count)
    command = ["helm", "template", "acme", str(source or chart), "--output-dir", str(output)]
    if dev:
        command += ["-f", str((source or chart) / "values-dev.yaml")]
    command += ["-f", str(values), *args]
    return subprocess.run(command, text=True, capture_output=True), output


def ok(*overlays, **kwargs):
    result, output = render(*overlays, **kwargs)
    assert result.returncode == 0, f"expected a successful render, got:\n{result.stderr}"
    return output


def refuse(label, *overlays, expected, **kwargs):
    result, _ = render(*overlays, **kwargs)
    assert result.returncode != 0, f"{label}: the render SUCCEEDED; it must be refused"
    for text in expected:
        assert text in result.stderr, (
            f"{label}: expected stderr to contain {text!r}\n--- stderr ---\n{result.stderr}"
        )


def env(output, filename, container):
    docs = yaml.safe_load_all((output / "curie/templates" / filename).read_text())
    for doc in docs:
        if not doc or doc.get("kind") != "Deployment":
            continue
        for c in doc["spec"]["template"]["spec"]["containers"]:
            if c["name"] == container:
                found = {}
                for e in c.get("env") or []:
                    if e["name"] in (ENABLED, DRIVERS):
                        assert e["name"] not in found, f"{filename} repeats {e['name']}"
                        assert "value" in e, f"{filename} renders {e['name']} without a literal value"
                        found[e["name"]] = e["value"]
                return found
    raise AssertionError(f"no {container} container in {filename}")


def pair(output):
    api = env(output, "api.yaml", "api")
    dispatcher = env(output, "dispatcher.yaml", "dispatcher")
    assert api == dispatcher, f"the API and the dispatcher disagree: api={api} dispatcher={dispatcher}"
    return api


def on(drivers=(DRIVER,), **extra):
    return merge({"testInstallation": {"enabled": True, "drivers": list(drivers)}}, extra)


# (a) Default: off and empty, the same in both.
default = pair(ok())
assert default == {ENABLED: "false", DRIVERS: "[]"}, f"(a) default renders {default!r}"

# (b) Declared defaults, typed boolean, quoted refused, nil coalesced away.
values = yaml.safe_load((chart / "values.yaml").read_text())
assert values.get("testInstallation") == {"enabled": False, "drivers": []}, (
    f"(b) values.yaml declares testInstallation as {values.get('testInstallation')!r}"
)
schema = json.loads((chart / "values.schema.json").read_text())
enabled_schema = schema["properties"]["testInstallation"]["properties"]["enabled"]
assert enabled_schema.get("type") == "boolean", f"(b) schema types enabled as {enabled_schema!r}"
refuse("(b) quoted true", REAL_SECRETS, args=("--set-string", "testInstallation.enabled=true"),
       expected=["testInstallation"])
assert pair(ok(args=("--set", "testInstallation=null"))) == default, "(b) testInstallation=null"

# (c) On: one value, the same normalized JSON in both.
enabled_output = ok(REAL_SECRETS, on((DRIVER, SIBLING)))
enabled = pair(enabled_output)
assert enabled[ENABLED] == "true", f"(c) enabled renders {enabled!r}"
assert json.loads(enabled[DRIVERS]) == [DRIVER, SIBLING], f"(c) drivers render {enabled!r}"
(work / "rendered.json").write_text(json.dumps(enabled))
(work / "expected.json").write_text(json.dumps([DRIVER, SIBLING]))

# (d) The prod tripwire.
for environment in ("prod", " PROD "):
    refuse(f"(d) api.environment={environment!r}", REAL_SECRETS, on(),
           {"api": {"environment": environment}}, expected=["api.environment"])
ok(REAL_SECRETS, {"api": {"environment": "prod"}})  # control: prod alone renders

# (e) Each published default secret.
for key, overlay in (
    ("api.apiKey", {"api": {"apiKey": "curie-dev-key"}}),
    ("worker.internalWorkerToken", {"worker": {"internalWorkerToken": "curie-dev-worker-token"}}),
    ("api.approvalChatAttesterSecret",
     {"api": {"approvalChatAttesterSecret": "curie-dev-approval-chat-attester"}}),
):
    refuse(f"(e) {key}", REAL_SECRETS, overlay, on(), expected=[key])
# values-dev.yaml alone keeps all three published defaults.
refuse("(e) values-dev.yaml defaults", on(),
       expected=["api.apiKey", "worker.internalWorkerToken", "api.approvalChatAttesterSecret"])
# A sealed install generates each one, so it renders.
ok(on(), dev=False)

# (f) Malformed drivers.
no_channel = {k: v for k, v in DRIVER.items() if k != "channel_id"}
refuse("(f) no channel", REAL_SECRETS, on((no_channel,)), expected=["channel_id"])
refuse("(f) blank channel", REAL_SECRETS, on(({**DRIVER, "channel_id": ""},)), expected=["channel_id"])
for label, entry, field in (
    ("dm channel", {**DRIVER, "channel_id": "D0EXAMPLE1"}, "channel_id"),
    ("user as bot", {**DRIVER, "bot_id": "U0EXAMPLE1"}, "bot_id"),
    ("bot as user", {**DRIVER, "bot_user_id": "B0EXAMPLE1"}, "bot_user_id"),
    ("blank agent", {**DRIVER, "agent": ""}, "agent"),
    ("unknown key", {**DRIVER, "note": "x"}, "note"),
):
    refuse(f"(f) {label}", REAL_SECRETS, on((entry,)), expected=[field])
# Malformed drivers are refused with the declaration off too.
refuse("(f) off, no channel", {"testInstallation": {"drivers": [no_channel]}}, expected=["channel_id"])

# The template `fail` is a gate of its own: without the schema it still refuses.
schemaless = work / "schemaless"
shutil.copytree(chart, schemaless / "curie")
(schemaless / "curie" / "values.schema.json").unlink()
refuse("(f) schemaless no channel", REAL_SECRETS, on((no_channel,)), source=schemaless / "curie",
       expected=["testInstallation.drivers[0]", "channel"])
for key, overlay in (
    ("api.apiKey", {"api": {"apiKey": "curie-dev-key"}}),
    ("worker.internalWorkerToken", {"worker": {"internalWorkerToken": "curie-dev-worker-token"}}),
    ("api.approvalChatAttesterSecret", {"api": {"approvalChatAttesterSecret": "curie-dev-approval-chat-attester"}}),
):
    refuse(f"(f) schemaless {key}", REAL_SECRETS, overlay, on(), source=schemaless / "curie",
           expected=[key, "published default"])
refuse("(f) schemaless prod", REAL_SECRETS, on(), {"api": {"environment": "prod"}},
       source=schemaless / "curie", expected=["api.environment"])

# (g) The setting comes only from the chart.
for workload in ("api", "dispatcher"):
    for name in (ENABLED, DRIVERS):
        refuse(f"(g) {workload}.extraEnv {name}",
               {workload: {"extraEnv": [{"name": name, "value": "true"}]}},
               expected=[f"{workload}.extraEnv", name])

print(f"OK: {count} Helm renders")
PY

# Round-trip the EXACT rendered strings through the REAL API Settings and
# DispatcherConfig. `uv run` starts from the repo root: both apps are
# workspace members.
(
  cd "$REPO_ROOT"
  uv run --python 3.13 python - "$WORK/rendered.json" "$WORK/expected.json" "$CHART" <<'PY'
import json
import os
import sys
from pathlib import Path

from jsonschema import Draft7Validator

# The schema must independently refuse the three unsafe declarations, rather
# than merely leave every semantic refusal to Helm's template helper.
schema = json.loads((Path(sys.argv[3]) / "values.schema.json").read_text())
validator = Draft7Validator(schema)
safe = {
    "testInstallation": {"enabled": True, "drivers": []},
    "security": {"allowDevDefaults": True},
    "api": {"environment": "dev", "apiKey": "example-api-key", "approvalChatAttesterSecret": "example-attester"},
    "worker": {"internalWorkerToken": "example-worker-token"},
}
validator.validate(safe)
sealed = json.loads(json.dumps(safe))
sealed["security"]["allowDevDefaults"] = False
sealed["api"]["apiKey"] = "curie-dev-key"
sealed["api"]["approvalChatAttesterSecret"] = "curie-dev-approval-chat-attester"
sealed["worker"]["internalWorkerToken"] = "curie-dev-worker-token"
validator.validate(sealed)
for area, field, value in (
    ("api", "environment", "prod"),
    ("api", "environment", " PROD "),
    ("api", "apiKey", "curie-dev-key"),
    ("api", "approvalChatAttesterSecret", "curie-dev-approval-chat-attester"),
    ("worker", "internalWorkerToken", "curie-dev-worker-token"),
):
    candidate = json.loads(json.dumps(safe))
    candidate[area][field] = value
    assert not validator.is_valid(candidate), f"schema accepts unsafe {area}.{field} with declaration on"
    candidate["testInstallation"]["enabled"] = False
    validator.validate(candidate)

for key in [key for key in os.environ if key.startswith(("CURIE_", "API_KEY"))]:
    del os.environ[key]

rendered = json.load(open(sys.argv[1]))
expected = json.load(open(sys.argv[2]))
os.environ.update(rendered)
os.environ["API_KEY"] = "example-api-key"
os.environ["CURIE_API_KEY"] = "example-api-key"
os.environ["CURIE_APPROVAL_CHAT_ATTESTER_SECRET"] = "example-attester-secret"
os.environ["CURIE_INTERNAL_WORKER_TOKEN"] = "example-worker-token"

from curie_api.config import Settings
from curie_dispatcher.config import DispatcherConfig
from pydantic import ValidationError


def entries(drivers):
    return [
        {k: v for k, v in vars(d).items() if v is not None} for d in drivers
    ]


api = Settings(_env_file=None)
dispatcher = DispatcherConfig()
for name, config in (("API", api), ("dispatcher", dispatcher)):
    assert config.test_installation_enabled is True, f"{name} did not read the declaration on"
    assert entries(config.test_installation_drivers) == expected, (
        f"{name} parsed {config.test_installation_drivers!r}, expected {expected!r}"
    )

# The same parsers refuse the published defaults with the declaration on.
os.environ["API_KEY"] = os.environ["CURIE_API_KEY"] = "curie-dev-key"
for name, build in (("API", lambda: Settings(_env_file=None)), ("dispatcher", DispatcherConfig)):
    try:
        build()
    except ValidationError as exc:
        assert "CURIE_TEST_INSTALLATION_ENABLED" in str(exc), f"{name}: {exc}"
    else:
        raise AssertionError(f"the {name} booted on the published API key with the declaration on")

print("OK: the rendered declaration parses through the real API and dispatcher settings")
PY
)
echo "test-installation-assertions: all assertions passed"
