"""Harness errors never carry a private value: the surface is removed, not scrubbed.

Covers ``identity_target.run`` and ``Target.exec_python`` failures, the
``Evidence.hold_private`` short-value refusal, ``identity_scenarios.run`` on an
unexpected exception, ``TicketScenario.restore`` and the two failed-PATCH
messages. Synthetic values only.
"""

from __future__ import annotations

import contextlib
import re
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import identity_scenarios
import identity_scenarios_attestation as att
import identity_scenarios_requester as req_helpers
import identity_target
import pytest
from identity_e2e import HarnessError
from identity_scenarios import Evidence, StepFailed, TicketScenario

HARNESS = Path(__file__).resolve().parents[1]
SUBJECT = "identity-e2e-alice"
PYDANTIC = f"1 validation error\n  Input should be valid [input_value='{SUBJECT}', input_type=str]"
SCAN = ("identity_target.py", "identity_scenarios.py", "identity_scenarios_attestation.py")


def failing(rc: int = 3) -> Any:
    def fake(argv: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            list(argv), rc, stdout=f"out {SUBJECT}\n", stderr=PYDANTIC
        )

    return fake


@pytest.fixture(autouse=True)
def no_log_dir() -> Any:
    # Tolerates the setter's absence so only the tests that need it fail on it.
    setter = getattr(identity_target, "set_private_log_dir", lambda path: None)
    setter(None)
    yield
    setter(None)


def target() -> identity_target.Target:
    return identity_target.Target(
        context="ctx", namespace="ns", release="rel", curie_bin="curie", chart=HARNESS
    )


# --- identity_target.run / exec_python -----------------------------------------------


def test_run_failure_carries_only_the_command_and_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", failing(3))
    with pytest.raises(HarnessError) as caught:
        identity_target.run(["kubectl", "-n", "ns", "exec", "pod", "--", SUBJECT])
    assert str(caught.value) == "kubectl -n ns exec exited 3"


def test_exec_python_failure_carries_only_the_workload_and_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", failing(7))
    with pytest.raises(HarnessError) as caught:
        target().exec_python("deployment/api", "api", "print(1)", {"subject": SUBJECT})
    assert str(caught.value) == "python in deployment/api/api exited 7"
    assert SUBJECT not in str(caught.value)


def test_exec_python_writes_the_output_tail_privately_and_replaces_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    identity_target.set_private_log_dir(tmp_path)
    monkeypatch.setattr(subprocess, "run", failing(7))
    for _ in range(2):
        with pytest.raises(HarnessError):
            target().exec_python("deployment/api", "api", "print(1)", {})
    written = tmp_path / "last-exec-error.txt"
    assert stat.S_IMODE(written.stat().st_mode) == 0o600
    text = written.read_text()
    assert SUBJECT in text
    assert "input_value=" in text
    assert text.count("validation error") == 1
    assert [p.name for p in tmp_path.iterdir()] == ["last-exec-error.txt"]


def test_exec_python_failure_without_a_log_dir_drops_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(subprocess, "run", failing(7))
    with pytest.raises(HarnessError):
        target().exec_python("deployment/api", "api", "print(1)", {})
    assert list(tmp_path.iterdir()) == []


# --- _quietly ------------------------------------------------------------------------


def test_quietly_logs_the_exception_type_only(capsys: pytest.CaptureFixture[str]) -> None:
    def call() -> None:
        raise HarnessError(f"sql failed for {SUBJECT}")

    assert req_helpers._quietly(call) is None
    captured = capsys.readouterr()
    assert captured.err.strip().endswith("restore failed: HarnessError")
    assert len(captured.err.strip().splitlines()) == 1
    assert SUBJECT not in captured.err
    assert captured.out == ""


def test_quietly_success_returns_the_value_silently(capsys: pytest.CaptureFixture[str]) -> None:
    assert req_helpers._quietly(lambda: 5) == 5
    assert capsys.readouterr().err == ""


def test_quietly_propagates_other_exceptions() -> None:
    def call() -> None:
        raise KeyError(SUBJECT)

    with pytest.raises(KeyError):
        req_helpers._quietly(call)


# --- scrub_text is gone --------------------------------------------------------------


def test_scrub_text_is_removed_and_uncalled() -> None:
    assert not hasattr(Evidence, "scrub_text")
    for path in sorted(HARNESS.glob("*.py")):
        assert not re.search(r"\.scrub_text\(", path.read_text()), path.name
    for name in SCAN:
        assert "def scrub_text" not in (HARNESS / name).read_text(), name


# --- hold_private --------------------------------------------------------------------


def test_hold_private_refuses_a_short_string_without_echoing_it() -> None:
    with pytest.raises(HarnessError) as caught:
        Evidence("zz").hold_private(["abc1234"])
    assert "shorter than 8 characters" in str(caught.value)
    assert "abc1234" not in str(caught.value)


def test_hold_private_ignores_none_and_non_strings_and_holds_long_values() -> None:
    evidence = Evidence("zz")
    evidence.hold_private([None, 5, b"abc", SUBJECT])
    evidence.data["note"] = f"seen {SUBJECT}"
    assert SUBJECT not in str(evidence.finish())


# --- identity_scenarios.run ----------------------------------------------------------


def test_run_records_an_unexpected_exception_by_type_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def scenario(harness: Any, args: Any, evidence: Evidence) -> None:
        raise KeyError(SUBJECT)

    monkeypatch.setitem(identity_scenarios.SCENARIOS, "zz-keyerror", scenario)
    data = identity_scenarios.run("zz-keyerror", SimpleNamespace(), SimpleNamespace())
    assert data["aborted"] == "KeyError"
    assert data["passed"] is False
    assert SUBJECT not in str(data)


# --- TicketScenario.restore ----------------------------------------------------------


def ticket_scenario(api_error: bool = False) -> TicketScenario:
    scenario = object.__new__(TicketScenario)
    evidence = Evidence("zz-ticket")
    scenario.evidence = evidence

    @contextlib.contextmanager
    def api() -> Any:
        if api_error:
            raise HarnessError(f"port-forward failed {SUBJECT}")
        yield object()

    scenario.harness = SimpleNamespace(api=api)
    scenario.changes = {
        "routes": True,
        "flag": False,
        "deployments": False,
        "ended": False,
        "approver_role": True,
    }

    def boom(*args: Any) -> None:
        raise HarnessError(f"answered 500: {SUBJECT}")

    for name in ("_drop_registration", "_restore_routes", "_verify_restored", "_restore_role"):
        setattr(scenario, name, boom)
    return scenario


@pytest.mark.parametrize("api_error", [False, True])
def test_restore_records_exception_type_names_only(api_error: bool) -> None:
    scenario = ticket_scenario(api_error)
    scenario.restore()
    data = scenario.evidence.data
    errors = [item["error"] for item in data["restore"]["failed"]]
    assert errors
    assert set(errors) == {"HarnessError"}
    assert data["aborted"].startswith("restore incomplete:")
    assert SUBJECT not in str(data)


# --- failed-PATCH messages -----------------------------------------------------------

BODY = {"detail": {"code": "flag_refused", "echo": SUBJECT}, "input": SUBJECT}


class FakeApi:
    def __init__(self, body: Any) -> None:
        self.body = body

    def ok(self, method: str, path: str) -> dict[str, Any]:
        return {
            "connector_requester_identity": False,
            "connector_requester_identity_version": 1,
        }

    def call(self, method: str, path: str, payload: Any = None) -> tuple[int, Any]:
        return 422, self.body


def test_requester_flag_patch_failure_names_status_and_code_only() -> None:
    with pytest.raises(StepFailed) as caught:
        identity_scenarios.set_requester_flag(FakeApi(BODY), "agent-id", True, what="flagging")
    message = str(caught.value)
    assert "422" in message
    assert "flag_refused" in message
    assert SUBJECT not in message


def test_requester_flag_patch_failure_with_string_detail_keeps_the_detail() -> None:
    body = {"detail": "version_conflict", "input": SUBJECT}
    with pytest.raises(StepFailed) as caught:
        identity_scenarios.set_requester_flag(FakeApi(body), "agent-id", True, what="flagging")
    assert "version_conflict" in str(caught.value)
    assert SUBJECT not in str(caught.value)


def test_attestation_approval_tools_patch_failure_names_status_and_code_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Harness:
        data: dict[str, Any] = {
            "keys": {"caller_signing": "k"},
            "helm_previous": {"connectorCaller.signingKey": "x"},
            "tenant_id": "00000000-0000-4000-8000-000000000001",
        }
        state = SimpleNamespace(directory=Path("/nonexistent"))
        target = SimpleNamespace(
            kubectl_json=lambda *a: {},
            component=lambda name: name,
            # The approver-role migration answers as the live pod script does.
            pod_python=lambda *a, **k: {"action": "unchanged", "legacy_state": "migrated"},
        )

        @contextlib.contextmanager
        def api(self) -> Any:
            yield FakeApi(BODY)

    monkeypatch.setattr(att, "require_install_ready", lambda *a, **k: None)
    monkeypatch.setattr(att.req_helpers, "require_reference_connector", lambda *a, **k: {})
    monkeypatch.setattr(att.req_helpers, "roster_groups", lambda *a, **k: [att.AUTHORITY_GROUP])
    monkeypatch.setattr(att, "reconciler_setting", lambda *a: {})
    monkeypatch.setattr(att, "reconciler_on", lambda *a: False)
    monkeypatch.setattr(att, "requester_flag", lambda *a: (True, 1))
    monkeypatch.setattr(att, "set_requester_flag", lambda *a, **k: (True, 1))
    monkeypatch.setattr(att.req_helpers, "set_approver_role", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(att, "stored_approval_tools", lambda *a: [])
    with pytest.raises(StepFailed) as caught:
        att.setup(
            Harness(),  # type: ignore[arg-type]
            Evidence("zz-att"),
            {},
            agent="agent-a",
            agent_id="id-1",
            approver="bob@example.test",
            restore={},
            state={},
        )
    message = str(caught.value)
    assert "422" in message
    assert "flag_refused" in message
    assert SUBJECT not in message
