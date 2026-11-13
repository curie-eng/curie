"""Fixed PATH observations in a credential-free copy of the selected template."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

CATALOG_VERSION = 1
CATALOG = (
    "bash",
    "sh",
    "git",
    "curl",
    "wget",
    "jq",
    "make",
    "gcc",
    "cc",
    "python3",
    "pip",
    "uv",
    "pytest",
    "ruff",
    "node",
    "npm",
    "npx",
    "pnpm",
    "yarn",
    "tsc",
    "cargo",
    "rustc",
    "rustfmt",
    "go",
    "java",
    "mvn",
    "gradle",
    "ruby",
    "bundle",
    "php",
    "composer",
    "dotnet",
    "docker",
    "kubectl",
    "helm",
    "terraform",
    "psql",
    "sqlite3",
    "redis-cli",
    "valkey-cli",
    "gh",
    "shellcheck",
)
PROBE_PREFIX = "curie-tool-observer-"


def digest(value: object) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


@dataclass(frozen=True, repr=False)
class ProbeSelection:
    pool: str
    template: str
    spec: dict[str, Any] = field(compare=False)
    identity: str


@dataclass(frozen=True)
class ProbeResult:
    present: frozenset[str]
    image_digest: str = field(repr=False)
    platform: str
    user_id: int


def probe_script() -> str:
    # POSIX command and printf are shell builtins. The lookup output is discarded.
    return (
        "for name in " + " ".join(CATALOG) + "; do "
        'if command -v "$name" >/dev/null 2>&1; then value=1; else value=0; fi; '
        'printf "%s=%s\\n" "$name" "$value"; done'
    )


def probe_template(selection: ProbeSelection, claim: str) -> dict[str, Any]:
    """Retain scheduling and hardening, with only the image's default environment."""
    if not claim.startswith(PROBE_PREFIX):
        raise ValueError("invalid observation claim")
    spec = copy.deepcopy(selection.spec)
    pod = spec["podTemplate"]["spec"]
    runner = next(c for c in pod["containers"] if c["name"] == "runner")
    for key in (
        "env",
        "envFrom",
        "volumeMounts",
        "volumeDevices",
        "lifecycle",
        "readinessProbe",
        "livenessProbe",
        "startupProbe",
        "workingDir",
    ):
        runner.pop(key, None)
    runner["command"] = ["/bin/sh", "-c", probe_script()]
    runner.pop("args", None)
    pod["containers"] = [runner]
    for key in ("initContainers", "ephemeralContainers", "volumes"):
        pod.pop(key, None)
    pod["automountServiceAccountToken"] = False
    pod["enableServiceLinks"] = False
    pod["restartPolicy"] = "Never"
    pod["activeDeadlineSeconds"] = 120
    spec["podTemplate"].setdefault("metadata", {}).setdefault("labels", {})[
        "curietech.ai/sandbox-claim"
    ] = claim
    return spec


def parse_probe(output: str) -> frozenset[str]:
    lines = output.splitlines()
    if len(lines) != len(CATALOG):
        raise ValueError("incomplete tool observation")
    present = set()
    for expected, line in zip(CATALOG, lines, strict=True):
        if line not in (f"{expected}=0", f"{expected}=1"):
            raise ValueError("invalid tool observation")
        if line.endswith("=1"):
            present.add(expected)
    return frozenset(present)


def reported_digest(image_id: str) -> str:
    match = re.search(r"(?:^|[@/])(sha256:[0-9a-f]{64})$", image_id)
    if match is None:
        raise ValueError("runner image identity unavailable")
    return match[1]
