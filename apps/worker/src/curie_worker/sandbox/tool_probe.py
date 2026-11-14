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
    declared_digests: frozenset[str] = field(repr=False)


@dataclass(frozen=True)
class ProbeResult:
    present: frozenset[str]
    image_digest: str = field(repr=False)
    platform: str
    user_id: int


def probe_script() -> str:
    # POSIX command and printf are shell builtins. The lookup output is discarded.
    return (
        "probe_system=$(uname -s) && probe_machine=$(uname -m) || exit 1; "
        'printf "platform_os=%s\\nplatform_arch=%s\\n" "$probe_system" "$probe_machine"; '
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


def parse_probe(output: str) -> tuple[frozenset[str], str]:
    lines = output.splitlines()
    if len(lines) != len(CATALOG) + 2:
        raise ValueError("incomplete tool observation")
    architecture = {
        "platform_arch=x86_64": "amd64",
        "platform_arch=aarch64": "arm64",
    }.get(lines[1])
    if lines[0] != "platform_os=Linux" or architecture is None:
        raise ValueError("sandbox platform unavailable")
    present = set()
    for expected, line in zip(CATALOG, lines[2:], strict=True):
        if line not in (f"{expected}=0", f"{expected}=1"):
            raise ValueError("invalid tool observation")
        if line.endswith("=1"):
            present.add(expected)
    return frozenset(present), f"linux/{architecture}"


def reported_digest(image_id: str) -> str:
    match = re.search(r"(?:^|[@/])(sha256:[0-9a-f]{64})$", image_id)
    if match is None:
        raise ValueError("runner image identity unavailable")
    return match[1]


def pinned_runner_digest(selection: ProbeSelection) -> str | None:
    runner = next(
        c for c in selection.spec["podTemplate"]["spec"]["containers"] if c["name"] == "runner"
    )
    match = re.search(r"@(sha256:[0-9a-f]{64})$", runner["image"])
    return None if match is None else match[1]
