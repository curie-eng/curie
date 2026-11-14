#!/usr/bin/env bash
# Render assertions for factory.requesterCodingInstructions.apply (ADR 0278, #5592).
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$CHART" <<'PY'
import pathlib
import subprocess
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
NAME = "CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY"


def render(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", "template", "curie", str(chart), "--show-only", "templates/api.yaml", *args],
        capture_output=True,
        text=True,
    )


def rendered(*args: str) -> object:
    result = render(*args)
    assert result.returncode == 0, result.stderr
    for document in yaml.safe_load_all(result.stdout):
        if document and document.get("kind") == "Deployment":
            for container in document["spec"]["template"]["spec"]["containers"]:
                if container["name"] == "api":
                    values = [e for e in container.get("env", []) if e["name"] == NAME]
                    assert len(values) == 1, f"{NAME} rendered {len(values)} times"
                    return values[0].get("value")
    raise AssertionError("no API container rendered")


# The switch ships on with the enforcing worker and runner.
assert rendered() == "true", rendered()
# An operator can turn it off; new WorkItems then record explicit absence.
assert rendered("--set", "factory.requesterCodingInstructions.apply=false") == "false"
# A retained release whose values predate the key still renders the default.
assert rendered("--set-json", "factory.requesterCodingInstructions=null") == "true"
# The schema types the switch.
refused = render("--set-string", "factory.requesterCodingInstructions.apply=yes")
assert refused.returncode != 0, "a non-boolean switch rendered"
# The env name is reserved, so an extraEnv copy cannot shadow it.
reserved = (chart / "files" / "reserved-env.yaml").read_text(encoding="utf-8")
assert NAME in reserved, "the switch's env name is not reserved"
print("requester-instructions-assertions: default, override, retained values and schema passed")
PY
