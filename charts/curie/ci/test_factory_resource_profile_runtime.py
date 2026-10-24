"""Exercise control write logging and diagnostics through the real shell helpers."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("runtime") / "factory-resource-profile-runtime.sh"
UID = "original-control-uid"
SECRET = "example-env-value-that-must-stay-private"

TOOLS = r"""
kubectl() {
    shift 4
    printf 'boundary %s %s\n' "$1" "${2:-}" >&3
    case "$1 $2" in
        'get pod')
            printf '%s\n' "$PROFILE_TEST_POD"
            return "$PROFILE_TEST_POD_STATUS"
            ;;
        'get events')
            [[ "$*" == *"involvedObject.uid=$CONTROL_UID"* ]] || return 91
            printf '%s\n' "$PROFILE_TEST_EVENTS"
            return "$PROFILE_TEST_EVENTS_STATUS"
            ;;
        'exec pod-control')
            if [[ "${*: -1}" == *os.fsync* ]]; then
                printf '%s' "$PROFILE_TEST_WRITE_STDOUT"
                printf '%s' "$PROFILE_TEST_WRITE_STDERR" >&2
                return "$PROFILE_TEST_WRITE_STATUS"
            fi
            printf '%s\n' 1342177280
            return "$PROFILE_TEST_WORKSPACE_STATUS"
            ;;
        delete*|wait*) return 0 ;;
        get*) return 0 ;;
    esac
    return 92
}
helm() { printf 'boundary helm %s\n' "$*" >&3; }
"""


def shell_function(source: str, name: str) -> str:
    start = re.search(rf"^{name}\(\) \{{", source, re.MULTILINE)
    if start is None:
        raise AssertionError(f"missing shell helper: {name}")
    first_line = source[start.start() :].splitlines()[0]
    if first_line.endswith("}"):
        return first_line
    lines = []
    delimiter = None
    for line in source[start.start() :].splitlines():
        lines.append(line)
        if delimiter is not None:
            if line == delimiter:
                delimiter = None
            continue
        heredoc = re.search(r"(?<!<)<<(?!<)\s*['\"]?([A-Za-z_]\w*)['\"]?", line)
        if heredoc is not None:
            delimiter = heredoc.group(1)
        elif line == "}":
            return "\n".join(lines)
    raise AssertionError(f"unterminated shell helper: {name}")


def run_control(
    tmp_path: Path,
    *,
    write_status: int = 0,
    scenario_status: int = 0,
    pod_uid: str = UID,
    pod_status: int = 0,
    workspace_status: int = 0,
    events_status: int = 0,
) -> subprocess.CompletedProcess[str]:
    pod = {
        "metadata": {"name": "pod-control", "uid": pod_uid},
        "spec": {"containers": [{"env": [{"name": "TOKEN", "value": SECRET}]}]},
        "status": {
            "phase": "Failed",
            "reason": "Evicted",
            "message": 'EmptyDir volume "workspace" exceeds "1Gi"',
            "containerStatuses": [
                {
                    "name": "runner",
                    "restartCount": 2,
                    "state": {"terminated": {"reason": "Error", "exitCode": 137}},
                    "lastState": {"terminated": {"reason": "OOMKilled", "exitCode": 137}},
                }
            ],
        },
    }
    events = {
        "items": [
            {"involvedObject": {"uid": UID}, "reason": "Evicted", "message": "workspace limit"},
            {"involvedObject": {"uid": "replacement-uid"}, "message": SECRET},
        ]
    }
    environment = os.environ.copy()
    environment.update(
        WORKDIR=str(tmp_path),
        PROFILE_TEST_POD=json.dumps(pod),
        PROFILE_TEST_EVENTS=json.dumps(events),
        PROFILE_TEST_POD_STATUS=str(pod_status),
        PROFILE_TEST_WRITE_STATUS=str(write_status),
        PROFILE_TEST_WORKSPACE_STATUS=str(workspace_status),
        PROFILE_TEST_EVENTS_STATUS=str(events_status),
        PROFILE_TEST_WRITE_STDOUT="write stdout\n\n",
        PROFILE_TEST_WRITE_STDERR="write stderr\n",
    )
    helpers = "\n".join(
        shell_function(SCRIPT.read_text(), name)
        for name in (
            "log", "kc", "fill_workspace", "pod_state", "write_control_workspace",
            "control_diagnostics", "stop_message", "cleanup",
        )
    )
    command = (
        "set -euo pipefail\nexec 3>&1\n"
        + helpers + "\n" + TOOLS
        + f"""
CONTEXT=kind-acme-test
NAMESPACE=test-profile
RELEASE=profile
CONTROL_POD=pod-control
CONTROL_UID={UID}
FILL_BYTES=1342177280
FILL_PATH=/workspace/.curie-4889-fill.bin
HOLD_SECONDS=90
MESSAGE_PID=""
INSTALL_STARTED=1
ABSENT_CRDS=()
CLUSTER_OBJECTS=()
trap cleanup EXIT
write_control_workspace
printf 'write returned to caller\n'
exit {scenario_status}
"""
    )
    return subprocess.run(
        ["bash", "-c", command], env=environment, text=True,
        capture_output=True, check=False, timeout=10,
    )


def diagnostic(completed: subprocess.CompletedProcess[str]) -> dict:
    lines = [
        line for line in completed.stdout.splitlines() if line.startswith("control diagnostics: ")
    ]
    assert len(lines) == 1, completed.stdout + completed.stderr
    return json.loads(lines[0].removeprefix("control diagnostics: "))


@pytest.mark.parametrize("write_status", [0, 23, 137])
@pytest.mark.parametrize("scenario_status", [0, 1])
def test_write_status_output_and_diagnostics_precede_cleanup(
    tmp_path: Path, write_status: int, scenario_status: int
) -> None:
    completed = run_control(tmp_path, write_status=write_status, scenario_status=scenario_status)
    output = completed.stdout
    assert completed.returncode == scenario_status, output + completed.stderr
    assert f"control write exit status: {write_status}" in output
    assert "write stdout\n\nwrite stderr\n" in output
    assert output.index("write stderr") < output.index("write returned to caller")
    summary = diagnostic(completed)
    assert summary["write_exit_status"] == write_status
    assert summary["pod"]["uid"] == UID
    assert summary["pod"]["phase"] == "Failed"
    assert summary["pod"]["reason"] == "Evicted"
    runner = summary["pod"]["containers"][0]
    assert runner["restartCount"] == 2
    assert runner["terminated"] == {"reason": "Error", "exitCode": 137}
    assert runner["lastTerminated"] == {"reason": "OOMKilled", "exitCode": 137}
    assert summary["workspace_file_bytes"] == 1342177280
    assert [event["reason"] for event in summary["events"]] == ["Evicted"]
    assert SECRET not in output + completed.stderr
    assert output.index("control diagnostics: ") < output.index("boundary delete sandboxclaims")
    assert output.index("control diagnostics: ") < output.index("boundary helm")
    assert not tmp_path.exists()


@pytest.mark.parametrize("pod_uid", [UID, "replacement-uid"])
def test_unavailable_or_replaced_original_pod_keeps_uid_scoped_events(
    tmp_path: Path, pod_uid: str
) -> None:
    completed = run_control(
        tmp_path, scenario_status=1, pod_uid=pod_uid,
        pod_status=1 if pod_uid == UID else 0,
    )
    assert completed.returncode == 1, completed.stdout + completed.stderr
    summary = diagnostic(completed)
    assert summary["pod"] is None
    assert summary["workspace_file_bytes"] is None
    assert [event["reason"] for event in summary["events"]] == ["Evicted"]
    assert SECRET not in completed.stdout + completed.stderr
    assert completed.stdout.count("boundary exec pod-control") == 1
    assert not tmp_path.exists()


def test_diagnostic_read_failures_preserve_scenario_status_and_cleanup(tmp_path: Path) -> None:
    completed = run_control(tmp_path, workspace_status=42, events_status=43)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    summary = diagnostic(completed)
    assert summary["workspace_file_bytes"] is None
    assert summary["workspace_read_exit_status"] == 42
    assert summary["events"] is None
    assert summary["events_read_exit_status"] == 43
    assert "boundary delete namespace" in completed.stdout
    assert not tmp_path.exists()
