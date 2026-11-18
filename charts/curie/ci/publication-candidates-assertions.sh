#!/usr/bin/env bash
#
# Render assertions for publication candidate retention (#5428).
#
# The chart default is on. A default install with workspaces on renders the
# switch true for the API and the worker. Turning workspaces off still renders,
# and both workloads see the switch off. An explicit false stays off. A zero
# byte cap fails the render.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$CHART" <<'PY'
import pathlib
import subprocess
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
repo = chart.parents[1]
SWITCH = "CURIE_PUBLICATION_CANDIDATES_ENABLED"
CAP = "CURIE_PUBLICATION_CANDIDATE_MAX_BYTES"


def fail(case: str, message: str) -> None:
    print(f"FAIL [{case}] {message}", file=sys.stderr)
    sys.exit(1)


def render(case: str, *args: str, expect_success: bool = True) -> str:
    result = subprocess.run(
        [
            "helm",
            "template",
            "curie",
            str(chart),
            "--show-only",
            "templates/api.yaml",
            "--show-only",
            "templates/worker.yaml",
            *args,
        ],
        capture_output=True,
        text=True,
    )
    if expect_success and result.returncode != 0:
        fail(case, f"helm template failed:\n{result.stderr.strip()[-1500:]}")
    if not expect_success and result.returncode == 0:
        fail(case, "an invalid candidate configuration rendered successfully")
    if not expect_success:
        return result.stderr
    return result.stdout


def environment(case: str, output: str, component: str) -> dict[str, str]:
    deployments = [
        document
        for document in yaml.safe_load_all(output)
        if document
        and document.get("kind") == "Deployment"
        and document["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]
    if len(deployments) != 1:
        fail(case, f"expected one {component} Deployment, found {len(deployments)}")
    containers = deployments[0]["spec"]["template"]["spec"]["containers"]
    named = [container for container in containers if container["name"] == component]
    if len(named) != 1:
        fail(case, f"expected one {component} container, found {len(named)}")
    env: dict[str, str] = {}
    for entry in named[0].get("env", []):
        if entry["name"] in env:
            fail(case, f"{component} renders {entry['name']} twice")
        if "value" in entry:
            env[entry["name"]] = str(entry["value"])
    return env


def assert_switch(case: str, output: str, expected: str) -> None:
    for component in ("api", "worker"):
        env = environment(case, output, component)
        if SWITCH not in env:
            fail(case, f"{component} does not render {SWITCH}")
        if env[SWITCH] != expected:
            fail(case, f"{component} {SWITCH} is {env[SWITCH]!r}, expected {expected!r}")
        if expected == "true":
            if component == "api" and env.get(CAP) != "1073741824":
                fail(case, f"api {CAP} is {env.get(CAP)!r}")
        elif component == "api" and CAP in env:
            fail(case, f"api renders {CAP} while candidates are off")


values = yaml.safe_load((chart / "values.yaml").read_text())
candidates = values["worker"]["workspace"]["publicationCandidates"]
if candidates.get("enabled") is not True:
    fail("values", f"chart default enabled is {candidates.get('enabled')!r}")
if candidates.get("maxBytes") != 1073741824:
    fail("values", f"chart default maxBytes is {candidates.get('maxBytes')!r}")

schema = yaml.safe_load((chart / "values.schema.json").read_text())
# The schema is JSON. yaml.safe_load accepts it.
enabled_schema = schema["properties"]["worker"]["properties"]["workspace"]["properties"][
    "publicationCandidates"
]["properties"]["enabled"]
if enabled_schema.get("default") is not True:
    fail("schema", f"values.schema.json default is {enabled_schema.get('default')!r}")

ops = (repo / "docs" / "operations.md").read_text()
marker = "With the switch off, a failed publication settles"
index = ops.find(marker)
start = ops.rfind("Publication recovery needs retained candidates", 0, index)
if start < 0 or index < 0:
    fail("docs", "the retention paragraph is missing")
paragraph = " ".join(ops[start : ops.find("\n\n", index)].split())
if "on by default" not in paragraph:
    fail("docs", "the retention paragraph does not say the switch is on by default")
if "Set it to false to turn retention off" not in paragraph:
    fail("docs", "the retention paragraph does not say how to turn the switch off")
if "nothing is recovered." not in paragraph:
    fail("docs", "the retention paragraph no longer ends on the unrecovered settlement")

assert_switch("workspaces-on", render("workspaces-on"), "true")
assert_switch(
    "workspaces-off",
    render(
        "workspaces-off",
        "--set",
        "agentSandbox.runner.workspace.enabled=false",
    ),
    "false",
)
assert_switch(
    "explicit-off",
    render(
        "explicit-off",
        "--set",
        "worker.workspace.publicationCandidates.enabled=false",
    ),
    "false",
)
bad = render(
    "bad-cap",
    "--set",
    "worker.workspace.publicationCandidates.maxBytes=0",
    expect_success=False,
)
if "maxBytes" not in bad:
    fail("bad-cap", f"the refusal did not name maxBytes:\n{bad.strip()[-800:]}")

print("PASS: publication candidates default on, render off without workspaces, and refuse a bad cap")
PY
