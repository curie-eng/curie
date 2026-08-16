"""The conformance kit's console front door (#1516).

The CLI is the surface a vendor quotes in a README, so the two properties that
matter most are here: the exit code follows ``automated_floor`` and nothing
short of a full strict pass exits 0, and the JSON payload carries the verdict
next to the list of what no machine checked.

The kit is also handed a real egress secret, so it must never write one into a
report. That is asserted rather than promised.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml
from channel_protocol.conformance.cli import main
from conformance_stubs import (
    StubAdapter,
    conformant_stub,
    free_port,
    non_conformant_stub,
    random_secret,
)

_KIND = "email"
_ADDRESS = "agent@example.test"
_QUERY_TOKEN = "querysecretvalue"


@pytest.fixture
def secret() -> str:
    return random_secret()


@contextmanager
def _running(stub: StubAdapter) -> Iterator[StubAdapter]:
    stub.start()
    try:
        yield stub
    finally:
        stub.stop()


def _write_profile(tmp_path: Path) -> Path:
    profile = {
        "version": "1.0",
        "kind": _KIND,
        "address": {
            "description": "The mailbox this adapter owns.",
            "pattern": r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
            "example": _ADDRESS,
        },
        "credentials": {
            "egress": "conformance-stub",
            "egress_secret_env": "CURIE_EGRESS_SECRET",
            "ingress_token_env": "CURIE_INGRESS_TOKEN",
        },
        "conformance": {"wire_version": "1.0"},
    }
    path = tmp_path / "adapter.yaml"
    path.write_text(yaml.safe_dump(profile), encoding="utf-8")
    return path


def _write_secret(tmp_path: Path, secret: str) -> Path:
    path = tmp_path / "egress-secret"
    path.write_text(secret, encoding="utf-8")
    return path


def _argv(
    tmp_path: Path, stub: StubAdapter, secret: str, *extra: str, query: bool = False
) -> list[str]:
    endpoint = stub.endpoint + (f"?trace={_QUERY_TOKEN}" if query else "")
    return [
        "--profile",
        str(_write_profile(tmp_path)),
        "--endpoint",
        endpoint,
        "--secret-file",
        str(_write_secret(tmp_path, secret)),
        *extra,
    ]


def _payload(captured: str) -> dict[str, Any]:
    decoded: dict[str, Any] = json.loads(captured)
    return decoded


def test_cli_exits_non_zero_on_any_fail(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with _running(
        non_conformant_stub("redirects", secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        code = main(_argv(tmp_path, stub, secret, "--json"))

    assert code != 0
    assert _payload(capsys.readouterr().out)["automated_floor"] == "fail"


def test_cli_exits_non_zero_on_any_not_run_in_strict_mode(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """A partial run is not a pass, and the exit code has to say so.

    This is the second front door of RULING 3. An exit code that cannot tell a
    full run from a partial one is exactly what lets a README claim conformance
    off an egress only invocation.
    """

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        code = main(_argv(tmp_path, stub, secret, "--json"))
        payload = _payload(capsys.readouterr().out)

    assert code != 0
    assert payload["automated_floor"] == "fail"
    statuses = {result["rule"]: result["status"] for result in payload["results"]}
    assert statuses[4] == "pass"
    assert statuses[5] == "pass"
    assert {statuses[1], statuses[2], statuses[7]} == {"not_run"}


def test_cli_process_exit_code_follows_the_verdict(tmp_path: Path, secret: str) -> None:
    """Asserted against a real PROCESS, because CI reads the exit status and
    not a return value."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys;"
                "from channel_protocol.conformance.cli import main;"
                "sys.exit(main(sys.argv[1:]))",
                *_argv(tmp_path, stub, secret, "--json"),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

    assert completed.returncode != 0, completed.stdout + completed.stderr
    assert _payload(completed.stdout)["automated_floor"] == "fail"


def test_diagnostic_mode_never_reports_conformant_and_stamps_its_mode(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Diagnostic mode exists so an author can see partial results while
    building, and it must never be mistakable for a strict run."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(_argv(tmp_path, stub, secret, "--json", "--mode", "diagnostic"))
        payload = _payload(capsys.readouterr().out)

    assert payload["mode"] == "diagnostic"
    assert payload["automated_floor"] == "fail"


def test_json_payload_carries_verdict_and_counts(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """All four top level fields ship in v1. Under ADR-0101 they cannot be
    added later without a version bump, so they are here now."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(_argv(tmp_path, stub, secret, "--json"))
        payload = _payload(capsys.readouterr().out)

    assert set(payload) >= {
        "automated_floor",
        "manual_review_required",
        "mode",
        "counts",
        "results",
    }
    assert set(payload["counts"]) == {"pass", "fail", "not_run"}
    assert any(item["clause"] == "3c" for item in payload["manual_review_required"])


def test_no_conformant_field_in_the_payload(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """A field literally named ``conformant`` is quotable as whole floor
    conformance while clause 3c was never asserted. The name was the hole, so
    the name is gone, at every depth of the payload."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(_argv(tmp_path, stub, secret, "--json"))
        payload = _payload(capsys.readouterr().out)

    assert "conformant" not in _all_keys(payload)


def _all_keys(payload: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            found.add(str(key))
            found |= _all_keys(value)
    elif isinstance(payload, list):
        for item in payload:
            found |= _all_keys(item)
    return found


def test_the_human_render_prints_the_manual_review_beside_the_verdict(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """A passing run still has to show what was not checked, or the verdict
    reads as more than it covers."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(_argv(tmp_path, stub, secret))
        rendered = capsys.readouterr().out

    assert "automated_floor" in rendered
    assert "3c" in rendered


def test_cli_report_never_contains_the_secret_or_the_endpoint_query(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The kit is pointed at a live adapter with a real egress secret and an
    endpoint whose query may itself be a credential. Neither may reach a report
    a vendor pastes into an issue."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(_argv(tmp_path, stub, secret, "--json", query=True))
        json_output = capsys.readouterr()
        main(_argv(tmp_path, stub, secret, query=True))
        human_output = capsys.readouterr()

    for stream in (
        json_output.out,
        json_output.err,
        human_output.out,
        human_output.err,
    ):
        assert secret not in stream
        assert _QUERY_TOKEN not in stream


def test_cli_refuses_a_profile_named_env_var_as_a_secret_source(
    tmp_path: Path,
    secret: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A third party authored file must never choose which of an operator's
    secrets gets read and where it is sent.

    ``credentials.egress_secret_env`` documents what the ADAPTER reads. The
    profile's value is exported here and the run must still refuse, rather than
    quietly resolving it and posting the result at the profile's endpoint.
    """

    monkeypatch.setenv("CURIE_EGRESS_SECRET", secret)
    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        code = main(
            [
                "--profile",
                str(_write_profile(tmp_path)),
                "--endpoint",
                stub.endpoint,
                "--json",
            ]
        )
        captured = capsys.readouterr()

    assert code != 0
    assert '"automated_floor": "pass"' not in captured.out
    assert "--secret-file" in captured.out + captured.err


def test_cli_has_no_secret_env_flag_at_all(capsys: pytest.CaptureFixture[str]) -> None:
    """RULING 4 is structural: the flag does not exist, so it cannot be used."""

    with pytest.raises(SystemExit):
        main(["--help"])
    assert "--secret-env" not in capsys.readouterr().out


def test_cli_accepts_the_secret_on_stdin(
    tmp_path: Path,
    secret: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The second operator supplied source, so a secret never has to touch disk."""

    monkeypatch.setattr(sys, "stdin", io.StringIO(secret + "\n"))
    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        main(
            [
                "--profile",
                str(_write_profile(tmp_path)),
                "--endpoint",
                stub.endpoint,
                "--secret-stdin",
                "--json",
            ]
        )
        payload = _payload(capsys.readouterr().out)

    statuses = {result["rule"]: result["status"] for result in payload["results"]}
    assert statuses[4] == "pass", payload
    assert statuses[5] == "pass", payload


def test_cli_loads_a_driver_from_a_module_attr_spec(
    tmp_path: Path,
    secret: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The vendor's front door: with a driver supplied, a correct adapter
    reaches a strict pass and the command exits 0."""

    state_path = tmp_path / "stub-state.json"
    port = free_port()
    monkeypatch.setenv("STUB_PORT", str(port))
    monkeypatch.setenv("STUB_SECRET", secret)
    monkeypatch.setenv("STUB_STATE", str(state_path))
    monkeypatch.setenv("STUB_BEHAVIOR", "conformant")

    code = main(
        [
            "--profile",
            str(_write_profile(tmp_path)),
            "--endpoint",
            f"http://127.0.0.1:{port}/curie",
            "--secret-file",
            str(_write_secret(tmp_path, secret)),
            "--driver",
            "conformance_stubs:subprocess_driver_from_env",
            "--json",
        ]
    )
    payload = _payload(capsys.readouterr().out)

    assert code == 0, payload
    assert payload["automated_floor"] == "pass"


def test_cli_refuses_a_driver_spec_it_cannot_resolve(
    tmp_path: Path, secret: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """A typo'd driver spec must be a loud usage error, never a silent run with
    rules 1, 2 and 7 quietly ``not_run``."""

    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        code = main(
            _argv(
                tmp_path,
                stub,
                secret,
                "--driver",
                "conformance_stubs:no_such_driver_factory",
                "--json",
            )
        )
        captured = capsys.readouterr()

    assert code != 0
    assert "no_such_driver_factory" in captured.out + captured.err
