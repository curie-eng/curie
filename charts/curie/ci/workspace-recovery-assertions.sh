#!/usr/bin/env bash
#
# Render assertions for workspace recovery capture (ADR 0251, #5143).
#
# worker.workspace.recovery.enabled is off by default. When an operator turns it
# on, the worker and the API must both see the switch, the API must see the
# installation byte cap, and the API's archive limit must equal the worker's,
# because the ledger refuses a reservation whose declared base limit differs
# from its own. Turning it on without a positive integer cap must fail the
# render rather than start an API that refuses to boot.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$CHART" <<'PY'
import pathlib
import subprocess
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
SWITCH = "CURIE_WORKSPACE_RECOVERY_ENABLED"
CAP = "CURIE_WORKSPACE_RECOVERY_MAX_BYTES"
ARCHIVE = "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES"


def fail(case: str, message: str) -> None:
    print(f"FAIL [{case}] {message}", file=sys.stderr)
    sys.exit(1)


def render(case: str, *args: str, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
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
        fail(case, "an invalid recovery configuration rendered successfully")
    return result


def environment(case: str, output: str, component: str) -> dict[str, dict[str, object]]:
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
    env: dict[str, dict[str, object]] = {}
    for entry in named[0].get("env", []):
        if entry["name"] in env:
            fail(case, f"{component} renders {entry['name']} twice")
        env[entry["name"]] = entry
    return env


def value(case: str, env: dict[str, dict[str, object]], name: str, component: str) -> str | None:
    entry = env.get(name)
    if entry is None:
        return None
    if "valueFrom" in entry or not isinstance(entry.get("value"), str):
        fail(case, f"{component} {name} is not a literal string value: {entry!r}")
    return str(entry["value"])


def assert_disabled(case: str, output: str) -> None:
    for component in ("worker", "api"):
        env = environment(case, output, component)
        switch = value(case, env, SWITCH, component)
        if switch not in (None, "false"):
            fail(case, f"{component} {SWITCH} renders {switch!r} while recovery is off")
    api = environment(case, output, "api")
    cap = value(case, api, CAP, "api")
    if cap not in (None, ""):
        fail(case, f"api {CAP} renders {cap!r} while recovery is off")


def assert_enabled(case: str, output: str, cap: str, archive: str) -> None:
    worker = environment(case, output, "worker")
    api = environment(case, output, "api")
    expected = {
        ("worker", SWITCH): "true",
        ("worker", ARCHIVE): archive,
        ("api", SWITCH): "true",
        ("api", CAP): cap,
        ("api", ARCHIVE): archive,
    }
    for (component, name), wanted in expected.items():
        env = worker if component == "worker" else api
        got = value(case, env, name, component)
        if got != wanted:
            fail(case, f"{component} {name} renders {got!r}, expected {wanted!r}")
    if value(case, api, ARCHIVE, "api") != value(case, worker, ARCHIVE, "worker"):
        fail(case, "the API archive limit differs from the worker's")


DEFAULT_ARCHIVE = str(
    yaml.safe_load((chart / "values.yaml").read_text())["worker"]["workspace"]["maxArchiveBytes"]
)
TEN_GIB = "10737418240"

# 1. The default release renders recovery off in both workloads.
assert_disabled("default", render("default").stdout)

# 2. A cap without the switch is still off.
assert_disabled(
    "cap-without-switch",
    render("cap-without-switch", "--set", f"worker.workspace.recovery.maxBytes={TEN_GIB}").stdout,
)

# 3. Enabled with a cap renders all four entries with the operator's values.
assert_enabled(
    "enabled",
    render(
        "enabled",
        "--set",
        "worker.workspace.recovery.enabled=true",
        "--set",
        f"worker.workspace.recovery.maxBytes={TEN_GIB}",
    ).stdout,
    TEN_GIB,
    DEFAULT_ARCHIVE,
)

# 4. The API archive limit follows an overridden worker limit, not a default.
assert_enabled(
    "enabled-archive-override",
    render(
        "enabled-archive-override",
        "--set",
        "worker.workspace.recovery.enabled=true",
        "--set",
        "worker.workspace.recovery.maxBytes=2147483648",
        "--set",
        "worker.workspace.maxArchiveBytes=134217728",
    ).stdout,
    "2147483648",
    "134217728",
)

# 5. Enabled without a positive integer cap refuses to render.
for case, extra in (
    ("enabled-no-cap", ()),
    ("enabled-null-cap", ("--set", "worker.workspace.recovery.maxBytes=null")),
    ("enabled-zero-cap", ("--set", "worker.workspace.recovery.maxBytes=0")),
    ("enabled-negative-cap", ("--set", "worker.workspace.recovery.maxBytes=-1")),
    ("enabled-string-cap", ("--set-string", "worker.workspace.recovery.maxBytes=1024")),
    ("enabled-fractional-cap", ("--set-json", "worker.workspace.recovery.maxBytes=1.5")),
):
    failure = render(
        case,
        "--set",
        "worker.workspace.recovery.enabled=true",
        *extra,
        expect_success=False,
    )
    diagnostic = failure.stdout + failure.stderr
    if "maxBytes" not in diagnostic:
        fail(case, f"the refusal does not name worker.workspace.recovery.maxBytes:\n{diagnostic}")

# 6. The chart owns these names: an extraEnv duplicate is refused.
for case, args in (
    ("worker-extra-env", ("--set", f"worker.extraEnv[0].name={SWITCH}")),
    ("api-extra-env-switch", ("--set", f"api.extraEnv[0].name={SWITCH}")),
    ("api-extra-env-cap", ("--set", f"api.extraEnv[0].name={CAP}")),
):
    component = case.split("-", 1)[0]
    render(
        case,
        "--set",
        "worker.workspace.recovery.enabled=true",
        "--set",
        f"worker.workspace.recovery.maxBytes={TEN_GIB}",
        *args,
        "--set-string",
        f"{component}.extraEnv[0].value=conflict",
        expect_success=False,
    )

print("workspace-recovery-assertions: default, enabled, cap refusal and reserved-env cases passed")
PY
