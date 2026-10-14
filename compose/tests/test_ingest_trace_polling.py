"""Execute exact trace polling with delayed candidate CLI read results."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from compose.tests.test_local_observability_ladder import LADDER_PATH, _shell_function

TRACE_ID = "a" * 32


def _trace(*operations: str) -> dict[str, object]:
    return {
        "trace": {"id": TRACE_ID, "input": "private-input-canary"},
        "tree": [{"name": operation, "type": "SPAN", "children": [],
                  "output": "private-output-canary"} for operation in operations],
        "approval_decision": None,
    }


def _query(
    tmp_path: Path, reads: list[tuple[int, dict[str, object]]], *, mode: str = "observe",
) -> tuple[subprocess.CompletedProcess[str], int]:
    source = LADDER_PATH.read_text()
    response_file = tmp_path / "responses.json"
    response_file.write_text(json.dumps(reads))
    count_file = tmp_path / "count"
    fake_bin = tmp_path / "curie"
    fake_bin.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib\n"
        "count_file = pathlib.Path(os.environ['FAKE_COUNT'])\n"
        "count = int(count_file.read_text()) if count_file.exists() else 0\n"
        "count_file.write_text(str(count + 1))\n"
        "responses = json.loads(pathlib.Path(os.environ['FAKE_RESPONSES']).read_text())\n"
        "code, body = responses[min(count, len(responses) - 1)]\n"
        "print(json.dumps(body))\n"
        "raise SystemExit(code)\n"
    )
    fake_bin.chmod(0o700)
    script = "set -u\n"
    script += _shell_function(source, "sanitize_exact_trace_read")
    script += _shell_function(source, "query_exact_seed_trace")
    script += f'''
WORKDIR="$1"
BIN="$2"
OBSERVABILITY_POLL_ATTEMPTS=3
OBSERVABILITY_POLL_INTERVAL_SECONDS=0
query_exact_seed_trace local {TRACE_ID} "curie.queue.enqueue,agent.run,curie.reply.post" "" "$3"
'''
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(tmp_path), str(fake_bin), mode],
        env={**os.environ, "FAKE_COUNT": str(count_file),
             "FAKE_RESPONSES": str(response_file)},
        text=True, capture_output=True, check=False,
    )
    assert "private-" not in result.stdout + result.stderr
    assert list(tmp_path.glob("exact-trace.*")) == []
    assert list(tmp_path.glob("safe-trace.*")) == []
    return result, int(count_file.read_text())


def test_observe_trace_query_recovers_from_exit_three_then_complete_membership(
    tmp_path: Path,
) -> None:
    # Exit 3 is the actual candidate CLI backend query failure contract; its
    # stdout is error evidence, never an authenticated proof of trace absence.
    result, count = _query(tmp_path, [
        (3, {"error": "backend query failed", "fix": "retry exact id"}),
        (0, _trace("curie.queue.enqueue")),
        (0, _trace("curie.queue.enqueue", "agent.run", "curie.reply.post")),
    ])

    assert result.returncode == 0, result.stderr
    assert count == 3
    assert json.loads(result.stdout)["operation"] == [
        "agent.run", "curie.queue.enqueue", "curie.reply.post",
    ]


def test_observe_trace_query_preserves_valid_evidence_after_not_found_reads(
    tmp_path: Path,
) -> None:
    result, count = _query(tmp_path, [
        (0, _trace("curie.queue.enqueue")),
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
    ])

    assert result.returncode == 0, result.stderr
    assert count == 3
    evidence = json.loads(result.stdout)
    assert evidence["trace_id"] == TRACE_ID
    assert evidence["observation_count"] == 1
    assert evidence["operation"] == ["curie.queue.enqueue"]
    assert evidence["observation_type"] == ["SPAN"]


def test_observe_trace_query_preserves_last_valid_evidence_after_exit_three(
    tmp_path: Path,
) -> None:
    result, count = _query(tmp_path, [
        (0, _trace("curie.queue.enqueue")),
        (0, _trace("curie.queue.enqueue", "agent.run")),
        (3, {"error": "backend query failed", "fix": "retry exact id"}),
    ])

    assert result.returncode == 0, result.stderr
    assert count == 3
    evidence = json.loads(result.stdout)
    assert evidence["trace_id"] == TRACE_ID
    assert evidence["observation_count"] == 2
    assert evidence["operation"] == ["agent.run", "curie.queue.enqueue"]
    assert evidence["observation_type"] == ["SPAN"]


@pytest.mark.parametrize("mode", ["observe", "present"])
def test_trace_query_persistent_errors_fail_without_absence_evidence(
    tmp_path: Path, mode: str,
) -> None:
    result, count = _query(tmp_path, [
        (3, {"error": "backend query failed", "fix": "retry exact id"}),
    ], mode=mode)

    assert result.returncode == 1
    assert count == 3
    assert result.stdout == ""
    assert "query-error" in result.stderr


def test_observe_trace_query_error_then_absence_fails_without_valid_evidence(
    tmp_path: Path,
) -> None:
    result, count = _query(tmp_path, [
        (3, {"error": "backend query failed", "fix": "retry exact id"}),
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
    ])

    assert result.returncode == 1
    assert count == 3
    assert result.stdout == ""
    assert "query-error" in result.stderr


def test_absent_trace_query_refuses_query_error_before_retry(tmp_path: Path) -> None:
    result, count = _query(tmp_path, [
        (3, {"error": "backend query failed", "fix": "retry exact id"}),
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
    ], mode="absent")

    assert result.returncode == 1
    assert count == 1
    assert "unexpected failure" in result.stderr


@pytest.mark.parametrize("mode", ["observe", "absent"])
def test_trace_query_stable_absence_remains_valid(tmp_path: Path, mode: str) -> None:
    result, count = _query(tmp_path, [
        (1, {"error": "exact trace not found", "fix": "retry exact id"}),
    ], mode=mode)

    assert result.returncode == 0, result.stderr
    assert count == 3
    if mode == "observe":
        assert json.loads(result.stdout)["observation_count"] == 0
    else:
        assert "remained not-found" in result.stdout


def test_observe_trace_query_refuses_malformed_success(tmp_path: Path) -> None:
    result, count = _query(tmp_path, [(0, {"trace": {}, "tree": []})])

    assert result.returncode == 1
    assert count == 1
    assert "malformed" in result.stderr
