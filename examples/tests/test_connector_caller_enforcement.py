"""Black box coverage for the connector caller proxy enforcement gate.

ADR-0168 decision 7. The gate runs against an enforcing cluster in CI; here a
fake kubectl answers as that cluster would, so each leg can be shown failing
on its own.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check-connector-caller-enforcement.sh"
SERVICE = "curie-weather-mcp-netpol-probe"
POD_IP = "192.0.2.20"

_FAKE = """#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
with open(os.environ["FAKE_KUBECTL_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(args) + "\\n")
env = os.environ.get

if "delete" in args or "apply" in args or "wait" in args or "rollout" in args:
    raise SystemExit(0)
if "get" in args and "svc" in args:
    if "-l" in args:
        print(env("FAKE_CONNECTORS", ""))
        raise SystemExit(0)
    jsonpath = args[-1]
    print(env("FAKE_TARGET_PORT", "caller") if "targetPort" in jsonpath else "8000", end="")
    raise SystemExit(0)
if "get" in args and "pod" in args:
    print("192.0.2.20", end="")
    raise SystemExit(0)
if "get" in args and "deployment" in args:
    print("8000", end="")
    raise SystemExit(0)
if "exec" in args:
    command = args[args.index("--") + 1 :]
    if command[0] == "python":
        raise SystemExit(0 if env("FAKE_SERVER_LISTENS", "1") == "1" else 1)
    url = command[-1]
    if url.startswith("http://192.0.2.20:"):
        raise SystemExit(0 if env("FAKE_DIRECT_REACHABLE") == "1" else 28)
    print(env("FAKE_CODE", "403"), end="")
    raise SystemExit(0)
raise SystemExit(90)
"""


def _run(tmp_path: Path, **fake: str) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(_FAKE, encoding="utf-8")
    kubectl.chmod(0o755)
    log = tmp_path / "kubectl.jsonl"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "FAKE_KUBECTL_LOG": str(log),
        "FAKE_CONNECTORS": SERVICE,
        **fake,
    }
    result = subprocess.run(
        ["bash", str(SCRIPT), "curie", "curie", "curie"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    return result, calls


def _direct_dials(calls: list[list[str]]) -> list[list[str]]:
    return [c for c in calls if "exec" in c and any(a.startswith(f"http://{POD_IP}:") for a in c)]


# @spec ADR-0168 d7
def test_an_enforcing_proxy_passes_every_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "lands on the caller proxy" in result.stdout
    assert "cannot reach" in result.stdout
    assert len(_direct_dials(calls)) == 1


# @spec ADR-0168 d7
def test_no_connector_service_is_fatal(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_CONNECTORS="")
    assert result.returncode != 0
    assert "vacuous" in result.stderr


# @spec ADR-0168 d7
def test_a_service_still_targeting_the_server_fails(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_TARGET_PORT="http")
    assert result.returncode != 0
    assert "not the caller proxy" in result.stderr


# @spec ADR-0168 d7
@pytest.mark.parametrize("code", ["200", "000", "401"])
def test_a_tokenless_call_anything_but_the_proxy_answers_fails(tmp_path: Path, code: str) -> None:
    result, _ = _run(tmp_path, FAKE_CODE=code)
    assert result.returncode != 0
    assert "not the caller proxy's 403" in result.stderr


# @spec ADR-0168 d7
def test_a_server_that_does_not_listen_stops_before_the_direct_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, FAKE_SERVER_LISTENS="0")
    assert result.returncode != 0
    assert "would prove nothing" in result.stderr
    assert _direct_dials(calls) == []


# @spec ADR-0168 d7
def test_a_sandbox_reaching_the_server_port_directly_fails(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_DIRECT_REACHABLE="1")
    assert result.returncode != 0
    assert "past the caller proxy" in result.stderr
