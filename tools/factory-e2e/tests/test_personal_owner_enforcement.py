"""Hermetic contract for the owner-binding enforcement proof driver (#5378).

The live path needs a throwaway cluster and a short-lived personal grant, so it
is proved by running ``personal_owner_enforcement.py run``. These tests pin
only the pure parts: flag and input validation (a missing prerequisite is a
refusal, never a skip), the ledger's reservation math, native transcript usage
counting, the judge's pass, fail, missing and not-met verdicts for every
criterion, sanitization of every output, argv and environment, and the
finalizer's ordering. Nothing here touches a cluster, Docker or a network, and
every value is an obvious synthetic sentinel.
"""

from __future__ import annotations

import base64
import json
import os
import sqlite3
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import personal_owner_enforcement as driver
import personal_owner_enforcement_support as poe
import personal_profile_support as pps
import pytest
import tls_code_host as tch

CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
GRANT = "synthetic-owner-grant-sentinel-not-a-token"
HEADER = "synthetic-hosted-header-sentinel-not-a-token"
T0 = 1_800_000_000.0


# --------------------------------------------------------------------------
# Fixtures built in the test body
# --------------------------------------------------------------------------


def _private(path: Path, text: str, mode: int = 0o600) -> Path:
    path.write_text(text)
    path.chmod(mode)
    return path


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "state"
    state.mkdir(parents=True)
    state.chmod(0o700)
    return state


def _input(tmp_path: Path, **overrides: Any) -> Path:
    kubeconfig = _private(tmp_path / "kubeconfig", "apiVersion: v1\n")
    curie = tmp_path / "curie"
    curie.write_text("#!/bin/sh\n")
    curie.chmod(0o700)
    body: dict[str, Any] = {
        "kubeconfig": str(kubeconfig),
        "curie_bin": str(curie),
        "runner_api_host": "10.0.0.5",
    }
    body.update(overrides)
    body = {key: value for key, value in body.items() if value is not None}
    return _private(tmp_path / "input.json", json.dumps(body))


def _flag_values(tmp_path: Path, **overrides: str) -> dict[str, str]:
    values = {
        "candidate": CANDIDATE,
        "context": "fixture-context",
        "namespace": "acme-owner-proof",
        "docker_context": "other-context",
        "registry": "127.0.0.1:5005",
        "state_dir": str(_state(tmp_path)),
        "run_id": "acme-run-1",
    }
    values.update(overrides)
    return values


def _refused(code: str, call: Callable[[], Any]) -> pps.Refusal:
    with pytest.raises(pps.Refusal) as caught:
        call()
    assert caught.value.code == code
    return caught.value


def _live_ledger(tmp_path: Path, run_id: str = "acme-run-1") -> poe.OwnerLedger:
    ledger = poe.OwnerLedger.open(_state(tmp_path), run_id, create=True)
    ledger.begin(CANDIDATE, "live", T0)
    ledger.start_live(T0, T0 + poe.GRANT_MIN_LIFETIME + 60)
    return ledger


def _row(seq: int, **overrides: Any) -> poe.SessionRow:
    values: dict[str, Any] = {
        "seq": seq,
        "slot": poe.SLOT_NAMES[seq - 1],
        "settled": True,
        "known": True,
        "output_tokens": 300,
        "assistant_turns": 2,
        "elapsed": 60.0,
    }
    values.update(overrides)
    return poe.SessionRow(**values)


def _assistant(message_id: str, tokens: int | None, *, uuid: str, **extra: Any) -> dict[str, Any]:
    usage = {} if tokens is None else {"input_tokens": 10, "output_tokens": tokens}
    return {
        "type": "assistant",
        "uuid": uuid,
        "message": {
            "id": message_id,
            "model": poe.MODEL,
            "usage": usage,
            "content": [{"type": "text", "text": "PROOF-READ-OK"}],
        },
        **extra,
    }


def _jsonl(*records: dict[str, Any]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


# --------------------------------------------------------------------------
# Flags, input file, grant file, expiry: refusals, never skips
# --------------------------------------------------------------------------


def test_flags_accept_the_caller_targets(tmp_path: Path) -> None:
    flags = poe.require_flags(**_flag_values(tmp_path))
    assert flags.registry_port == 5005
    assert flags.grant_namespace == "acme-owner-proof-grants"
    assert flags.release != "curie" and flags.release == poe.release_name("acme-run-1")


@pytest.mark.parametrize(
    ("override", "code"),
    [
        ({"candidate": "abc123"}, "candidate_invalid"),
        ({"namespace": "a" * 43}, "namespace_invalid"),
        ({"namespace": "Acme"}, "namespace_invalid"),
        ({"registry": "10.0.0.1:5005"}, "registry_invalid"),
        ({"registry": "127.0.0.1:notaport"}, "registry_invalid"),
        ({"context": ""}, "context_invalid"),
        ({"docker_context": "two words"}, "docker_context_invalid"),
        ({"run_id": "bad run"}, "arguments_invalid"),
        ({"state_dir": "relative/state"}, "state_dir_unsafe"),
    ],
)
def test_flags_refuse_unsafe_values(tmp_path: Path, override: dict[str, str], code: str) -> None:
    _refused(code, lambda: poe.require_flags(**_flag_values(tmp_path, **override)))


def test_state_dir_must_be_private(tmp_path: Path) -> None:
    values = _flag_values(tmp_path)
    Path(values["state_dir"]).chmod(0o755)
    _refused("state_dir_unsafe", lambda: poe.require_flags(**values))


def test_input_file_round_trip_hides_its_repr(tmp_path: Path) -> None:
    inputs = poe.load_input_file(_input(tmp_path))
    assert inputs.runner_api_host == "10.0.0.5"
    assert inputs.grant_probe_cli == "claude"
    assert repr(inputs) == "<input file>"


@pytest.mark.parametrize(
    "overrides",
    [
        {"runner_api_host": "127.0.0.1"},
        {"runner_api_host": "0.0.0.0"},
        {"runner_api_host": "runner.example.com"},
        {"kubeconfig": "relative/kubeconfig"},
        {"curie_bin": None},
        {"extra_key": "x"},
        {"grant_probe_cli": "claude; rm"},
    ],
)
def test_input_file_refuses_bad_shapes(tmp_path: Path, overrides: dict[str, Any]) -> None:
    path = _input(tmp_path, **overrides)
    _refused("input_file_invalid", lambda: poe.load_input_file(path))


def test_input_file_must_be_private_and_absolute(tmp_path: Path) -> None:
    path = _input(tmp_path)
    path.chmod(0o644)
    _refused("input_file_invalid", lambda: poe.load_input_file(path))
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    _refused("input_file_invalid", lambda: poe.load_input_file(link))
    _refused("input_file_invalid", lambda: poe.load_input_file(Path("input.json")))


def test_grant_file_is_checked_by_stat_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    grant = _private(tmp_path / "grant", "\x00not-ascii-and-never-read\n")

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("the grant file was opened")

    monkeypatch.setattr(os, "open", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    poe.stat_grant_file(grant)


def test_grant_file_refusals(tmp_path: Path) -> None:
    _refused("grant_missing", lambda: poe.stat_grant_file(None))
    _refused("grant_missing", lambda: poe.stat_grant_file(tmp_path / "absent"))
    empty = _private(tmp_path / "empty", "")
    _refused("grant_missing", lambda: poe.stat_grant_file(empty))
    shared = _private(tmp_path / "shared", GRANT, 0o644)
    _refused("grant_unsafe", lambda: poe.stat_grant_file(shared))
    big = _private(tmp_path / "big", "x" * 5000)
    _refused("grant_unsafe", lambda: poe.stat_grant_file(big))
    link = tmp_path / "link"
    link.symlink_to(_private(tmp_path / "real", GRANT))
    _refused("grant_unsafe", lambda: poe.stat_grant_file(link))


def test_expiry_must_cover_live_window_and_cleanup() -> None:
    assert poe.GRANT_MIN_LIFETIME == 1620
    _refused("expiry_unknown", lambda: poe.require_expiry("", T0))
    _refused("expiry_unknown", lambda: poe.require_expiry("tomorrow", T0))
    _refused("lifetime_insufficient", lambda: poe.require_expiry(str(int(T0) + 1619), T0))
    assert poe.require_expiry(str(int(T0) + 1620), T0) == T0 + 1620


def _main(argv: list[str], environ: dict[str, str] | None = None) -> int:
    return driver.main(argv, environ=environ or {"PATH": "/usr/bin", "HOME": "/nonexistent"})


def _cluster_args(tmp_path: Path, command: str, **overrides: str) -> list[str]:
    values = {
        "--candidate": CANDIDATE,
        "--context": "fixture-context",
        "--namespace": "acme-owner-proof",
        "--docker-context": "other-context",
        "--registry": "127.0.0.1:5005",
        "--input-file": str(_input(tmp_path)),
        "--state-dir": str(_state(tmp_path)),
        "--run-id": "acme-run-1",
        "--grant-file": str(tmp_path / "missing-grant"),
        "--grant-expires-at": str(int(T0)),
    }
    values.update(overrides)
    return [command, *[item for pair in values.items() for item in pair]]


def test_preflight_missing_grant_is_a_refusal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _main(_cluster_args(tmp_path, "preflight")) == poe.EXIT_REFUSED
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "grant_missing"


def test_preflight_refuses_ambient_service_credentials(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    environ = {"PATH": "/usr/bin", "ANTHROPIC_API_KEY": GRANT}
    assert _main(_cluster_args(tmp_path, "preflight"), environ) == poe.EXIT_REFUSED
    out = capsys.readouterr().out
    assert json.loads(out)["error"] == "service_credentials"
    assert GRANT not in out


def test_preflight_refuses_an_unsafe_input_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    args = _cluster_args(tmp_path, "preflight", **{"--input-file": str(tmp_path / "absent")})
    assert _main(args) == poe.EXIT_REFUSED
    assert json.loads(capsys.readouterr().out)["error"] == "input_file_invalid"


def test_run_requires_grant_flags(tmp_path: Path) -> None:
    args = [item for item in _cluster_args(tmp_path, "run") if item]
    without = args[: args.index("--grant-file")]
    with pytest.raises(SystemExit) as stopped:
        _main(without)
    assert stopped.value.code == 2


# --------------------------------------------------------------------------
# The ceiling and reservation math
# --------------------------------------------------------------------------


def test_ceiling_splits_evenly_and_fits() -> None:
    assert len(poe.SESSION_PLAN) == poe.MAX_SESSIONS == 8
    assert poe.SESSION_OUTPUT_TOKENS * poe.MAX_SESSIONS == poe.MAX_OUTPUT_TOKENS == 8192
    assert (
        poe.SESSION_ASSISTANT_TURNS_CEILING * poe.MAX_SESSIONS
        == poe.TOTAL_ASSISTANT_TURNS_CEILING
        == 32
    )
    assert poe.SESSION_SECONDS * poe.MAX_SESSIONS <= poe.LIVE_SECONDS == 1440
    assert poe.CLEANUP_SECONDS == 180 and poe.PAID_RETRIES == 0
    assert poe.MODEL == "claude-sonnet-5-5"
    assert poe.shares_fit()
    assert [slot.generation for slot in poe.SESSION_PLAN] == [1, 1, 1, 1, 1, 1, 2, 3]


@pytest.mark.parametrize(
    "overrides",
    [
        {"session_tokens": poe.SESSION_OUTPUT_TOKENS + 1},
        {"session_turns": poe.SESSION_ASSISTANT_TURNS_CEILING + 1},
        {"session_seconds": poe.SESSION_SECONDS + 1},
        {"sessions": poe.MAX_SESSIONS + 1},
    ],
)
def test_shares_that_do_not_fit_are_refused(overrides: dict[str, Any]) -> None:
    assert not poe.shares_fit(**overrides)


def _decide(rows: list[poe.SessionRow], **overrides: Any) -> poe.Reservation:
    values: dict[str, Any] = {
        "slot": poe.SLOT_NAMES[len(rows)] if len(rows) < poe.MAX_SESSIONS else "rejected",
        "now": T0,
        "live_deadline": T0 + poe.LIVE_SECONDS,
        "grant_expires_at": T0 + poe.GRANT_MIN_LIFETIME,
    }
    values.update(overrides)
    return poe.reservation_decision(rows, **values)


def test_first_reservation_takes_one_even_share() -> None:
    first = _decide([])
    assert (first.seq, first.tokens, first.turns) == (1, 1024, 4)
    assert first.deadline == T0 + poe.SESSION_SECONDS


def test_reservation_deadline_never_passes_the_live_window() -> None:
    late = _decide([], now=T0 + poe.LIVE_SECONDS - 100)
    assert late.deadline == T0 + poe.LIVE_SECONDS
    _refused("live_window_exhausted", lambda: _decide([], now=T0 + poe.LIVE_SECONDS - 80))
    _refused("live_window_exhausted", lambda: _decide([], now=T0 + poe.LIVE_SECONDS))
    _refused("live_window_exhausted", lambda: _decide([], live_deadline=None))


def test_unknown_or_unsettled_usage_exhausts_the_allowance() -> None:
    _refused("usage_unknown", lambda: _decide([_row(1, known=False, output_tokens=None)]))
    _refused("usage_unknown", lambda: _decide([_row(1, settled=False)]))
    assert poe.spent([_row(1, known=False)]) == (
        poe.MAX_OUTPUT_TOKENS,
        poe.TOTAL_ASSISTANT_TURNS_CEILING,
    )
    assert poe.spent([_row(1, unreserved=True)]) == (
        poe.MAX_OUTPUT_TOKENS,
        poe.TOTAL_ASSISTANT_TURNS_CEILING,
    )
    assert poe.spent([_row(1, settled=False)]) == (
        poe.SESSION_OUTPUT_TOKENS,
        poe.SESSION_ASSISTANT_TURNS_CEILING,
    )
    assert poe.spent([_row(1), _row(2, output_tokens=0, assistant_turns=0)]) == (300, 2)


def test_unreserved_start_or_overrun_fences_every_start() -> None:
    _refused("budget_exceeded", lambda: _decide([_row(1, unreserved=True)]))
    _refused("budget_exceeded", lambda: _decide([_row(1, output_tokens=1025)]))
    _refused("budget_exceeded", lambda: _decide([_row(1, assistant_turns=5)]))
    _refused("budget_exceeded", lambda: _decide([_row(1, elapsed=181.0)]))


def test_eight_sessions_exhaust_the_plan_and_slots_never_repeat() -> None:
    rows = [_row(seq, output_tokens=1024) for seq in range(1, 8)]
    eighth = _decide(rows)
    assert eighth.seq == 8 and eighth.slot == "revalidated"
    rows.append(_row(8))
    for slot in poe.SLOT_NAMES:
        with pytest.raises(pps.Refusal) as caught:
            _decide(rows, slot=slot)
        assert caught.value.code == "paid_retry_refused"
    _refused("paid_retry_refused", lambda: _decide([_row(1)], slot=poe.SLOT_NAMES[0]))
    _refused("paid_retry_refused", lambda: _decide([], slot="diagnostic-rerun"))


def test_reservation_needs_grant_lifetime_through_cleanup() -> None:
    short = T0 + poe.LIVE_SECONDS + poe.CLEANUP_SECONDS - 1
    _refused("lifetime_insufficient", lambda: _decide([], grant_expires_at=short))
    _refused("lifetime_insufficient", lambda: _decide([], grant_expires_at=None))


# --------------------------------------------------------------------------
# The private ledger
# --------------------------------------------------------------------------


def test_ledger_is_private_and_one_run_per_id(tmp_path: Path) -> None:
    state = _state(tmp_path)
    ledger = poe.OwnerLedger.open(state, "acme-run-1", create=True)
    assert (state / poe.LEDGER_FILE).stat().st_mode & 0o777 == 0o600
    ledger.begin(CANDIDATE, "live", T0)
    _refused("paid_retry_refused", lambda: ledger.begin(CANDIDATE, "live", T0))
    rehearsal = poe.OwnerLedger.open(state, "acme-rehearsal", create=True)
    rehearsal.begin(CANDIDATE, "rehearse", T0)
    _refused("rehearsal_run_id", lambda: rehearsal.begin(CANDIDATE, "live", T0))
    _refused("ledger_missing", lambda: rehearsal.reserve("process-interrupted", T0))
    _refused("ledger_missing", lambda: poe.OwnerLedger.open(state, "unknown", create=False))
    (state / poe.LEDGER_FILE).chmod(0o644)
    _refused("ledger_unsafe", lambda: poe.OwnerLedger.open(state, "acme-run-1", create=False))


def test_ledger_reserves_settles_and_stops_on_unknown_usage(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    first = ledger.reserve("process-interrupted", T0 + 1)
    _refused("usage_unknown", lambda: ledger.reserve("process-recovered", T0 + 2))
    ledger.mark_started(first.seq, "pod:cli", T0 + 3)
    ledger.settle(first.seq, poe.SessionUsage(200, 1, True), 40.0, T0 + 40)
    ledger.settle(first.seq, poe.SessionUsage(150, 1, True), 41.0, T0 + 41)
    assert ledger.sessions()[0]["output_tokens"] == 200
    second = ledger.reserve("process-recovered", T0 + 42)
    ledger.settle(second.seq, poe.SessionUsage.unknown(), None, T0 + 60)
    _refused("usage_unknown", lambda: ledger.reserve("pod-interrupted", T0 + 61))


def test_ledger_unreserved_start_is_recorded_and_fences(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    ledger.record_unreserved("pod:cli", T0 + 5)
    assert ledger.sessions()[0]["unreserved"] == 1
    _refused("budget_exceeded", lambda: ledger.reserve("process-interrupted", T0 + 6))


# --------------------------------------------------------------------------
# Native transcript usage
# --------------------------------------------------------------------------


def test_usage_counts_each_message_once_at_its_largest_count() -> None:
    data = _jsonl(
        {"type": "user", "uuid": "u1"},
        _assistant("msg-1", 40, uuid="a1"),
        _assistant("msg-1", 90, uuid="a2"),
        _assistant("msg-2", 30, uuid="a3"),
    )
    usage = poe.transcript_usage(data)
    assert (usage.known, usage.output_tokens, usage.assistant_turns) == (True, 120, 2)
    assert usage.models == (poe.MODEL,)


def test_usage_excludes_only_exactly_matched_seeds() -> None:
    seed = _assistant("seed-1", None, uuid="s1")
    data = _jsonl(seed, _assistant("msg-1", 25, uuid="a1"))
    usage = poe.transcript_usage(data, [seed])
    assert (usage.known, usage.output_tokens, usage.assistant_turns) == (True, 25, 1)
    changed = {**seed, "message": {**seed["message"], "id": "seed-changed"}}
    assert not poe.transcript_usage(_jsonl(changed), [seed]).known
    assert not poe.transcript_usage(_jsonl(seed)).known


def test_seed_only_or_empty_capture_is_not_measured_zero_usage() -> None:
    seed = _assistant("seed-1", None, uuid="s1")
    assert not poe.transcript_usage(_jsonl(seed), [seed]).known
    assert not poe.transcript_usage(b"").known


def _native_refusal_fixture() -> dict[str, Any]:
    # Observed from SDK-bundled CLI 2.1.317 in an empty HOME with an invalid
    # OAuth token, 2026-11-12. The native debug response exposes the provider
    # type; the stream's authentication_failed is the CLI's classification.
    return json.loads((Path(__file__).parent / "fixtures/native_auth_refusal.json").read_text())


def _aci_refusal() -> list[dict[str, Any]]:
    # The error/final pair consumed by the enrollment driver's AC5 live_session.
    return [
        {"epoch": "example-epoch", "kind": "error", "classification": "authentication-required"},
        {"epoch": "example-epoch", "kind": "final", "status": "classified-failure"},
    ]


def test_rejection_uses_observed_provider_debug_response_and_aci_terminal() -> None:
    sample = _native_refusal_fixture()
    reading = poe.native_refusal(sample["native_debug_stderr"].encode(), _aci_refusal())
    assert reading["http_status"] == 401
    assert reading["provider_error_type"] == "authentication_error"
    assert reading["source"] == "native_debug_stderr"
    assert reading["aci_classification"] == "authentication-required"
    assert sample["native_stream"][0]["error"] == "authentication_failed"


@pytest.mark.parametrize(
    "kind",
    [
        "empty",
        "stream_only",
        "truncated",
        "no_aci",
        "no_final",
        "different_epoch",
        "no_epoch",
        "conflicting",
    ],
)
def test_rejection_capture_fails_with_a_named_reason(kind: str) -> None:
    sample = _native_refusal_fixture()
    log = sample["native_debug_stderr"].encode()
    events = _aci_refusal()
    reason = "rejection_capture_missing_provider_response"
    if kind == "empty":
        log = b""
    elif kind == "stream_only":
        log = _jsonl(*sample["native_stream"])
    elif kind == "truncated":
        log = log[: log.index(b"{") + 1]
    elif kind == "no_aci":
        events = []
        reason = "rejection_capture_missing_aci_terminal"
    elif kind == "no_final":
        events = events[:1]
        reason = "rejection_capture_missing_aci_terminal"
    elif kind == "different_epoch":
        events[1]["epoch"] = "different-epoch"
        reason = "rejection_capture_missing_aci_terminal"
    elif kind == "no_epoch":
        for event in events:
            event["epoch"] = None
        reason = "rejection_capture_missing_aci_terminal"
    else:
        log += b'API error: 500 {"error":{"type":"api_error"}}\n'
        reason = "rejection_capture_conflicting_provider_responses"
    with pytest.raises(poe.StepFailed, match=reason):
        poe.native_refusal(log, events)


@pytest.mark.parametrize(
    "data",
    [
        _jsonl(_assistant("msg-1", 10, uuid="a1"))[:-1],
        b"{not json}\n",
        _jsonl(_assistant("msg-1", None, uuid="a1")),
        _jsonl({"type": "assistant", "uuid": "a1", "message": "flat"}),
    ],
)
def test_unreadable_or_unterminated_usage_is_unknown(data: bytes) -> None:
    assert poe.transcript_usage(data) == poe.SessionUsage.unknown()


def test_provider_rejection_is_classified_and_scrubbed() -> None:
    scrubber = pps.Scrubber([pps.HeldSecret(GRANT)])
    record = {
        "type": "assistant",
        "uuid": "x1",
        "error": "authentication_failed",
        "message": {
            "id": "synthetic-1",
            "model": poe.SYNTHETIC_MODEL,
            "content": [
                {
                    "type": "text",
                    "text": f'API Error: 401 {{"type":"error","error":{{"type":'
                    f'"authentication_error","message":"bad {GRANT}"}}}}',
                }
            ],
        },
    }
    usage = poe.transcript_usage(_jsonl(record), scrub=scrubber.scrub)
    assert (usage.known, usage.output_tokens, usage.assistant_turns) == (True, 0, 0)
    (error,) = usage.error_classes
    assert error["http_status"] == 401
    assert error["provider_error_type"] == "authentication_error"
    assert error["cli_error"] == "authentication_failed"
    assert GRANT not in json.dumps(error)


def test_tool_results_reads_text_blocks() -> None:
    data = _jsonl(
        {
            "type": "user",
            "message": {
                "content": [
                    {"type": "tool_result", "content": "CHALLENGE-ABC"},
                    {"type": "tool_result", "content": [{"type": "text", "text": "PUBLIC"}]},
                ]
            },
        }
    )
    assert poe.tool_results(data) == ["CHALLENGE-ABC", "PUBLIC"]


# --------------------------------------------------------------------------
# The judge
# --------------------------------------------------------------------------


def _record_all(ledger: poe.OwnerLedger, criterion: str) -> None:
    for key, value in poe.EXPECTED[criterion].items():
        ledger.record(criterion, key, value, T0)


OBSERVED = [name for name in poe.CRITERIA if name not in {"budget", "cleanup"}]


@pytest.mark.parametrize("criterion", OBSERVED)
def test_judge_passes_fails_and_reports_missing(tmp_path: Path, criterion: str) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, criterion)
    if criterion == "restart":
        ledger.record("restart", "pod.workspace_nonce_restored", True, T0)
    assert poe.judge(ledger, criterion)["verdict"] == "pass"
    key, expected = next(iter(poe.EXPECTED[criterion].items()))
    ledger.record(criterion, key, {"unexpected": expected}, T0)
    verdict = poe.judge(ledger, criterion)
    assert verdict["verdict"] == "fail"
    check = next(c for c in verdict["criteria"][0]["checks"] if c["key"] == key)
    assert check["expected"] == expected and check["status"] == "fail"
    ledger.db.execute("DELETE FROM observations WHERE key = ?", (key,))
    assert poe.judge(ledger, criterion)["verdict"] == "missing"


def test_pod_workspace_restoration_is_not_met_naming_the_blocker(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "restart")
    ledger.record("restart", "pod.workspace_nonce_restored", False, T0)
    verdict = poe.judge(ledger, "restart")
    assert verdict["verdict"] == "not_met"
    check = next(
        c for c in verdict["criteria"][0]["checks"] if c["key"] == "pod.workspace_nonce_restored"
    )
    assert check["blocked_by"] == "#5541" and check["upstream"] == "#5092"
    assert poe.exit_code("not_met") == poe.EXIT_NOT_MET


def test_process_recovery_allows_a_fresh_pod_and_requires_interruption_proof(
    tmp_path: Path,
) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "restart")
    for key in ("same_pod", "container_restarted"):
        ledger.record("restart", f"process.{key}", False, T0)
    result = poe.judge(ledger, "restart")
    assert result["verdict"] == "pass"
    checks = {row["key"]: row for row in result["criteria"][0]["checks"]}
    assert checks["process.same_pod"]["status"] == "informational"
    assert checks["process.container_restarted"]["observed"] is False
    for key in ("interruption_process_only", "old_epoch_confirmed_before_bind"):
        ledger.record("restart", f"process.{key}", False, T0)
        assert poe.judge(ledger, "restart")["verdict"] == "fail"
        ledger.record("restart", f"process.{key}", True, T0)
        ledger.db.execute(
            "DELETE FROM observations WHERE run_id=? AND key=?", (ledger.run_id, f"process.{key}")
        )
        assert poe.judge(ledger, "restart")["verdict"] == "missing"
        ledger.record("restart", f"process.{key}", True, T0)


@pytest.mark.parametrize(
    "bad",
    [
        {"action": "delete_pod"},
        {"driver_pod_delete_calls": 1},
        {"native_identity_matched": False},
        {"native_gone": False},
        {"confirmed_epoch": 6},
        {"old_epoch": None},
        {"old_claim_still_bound_to_request": False},
        {"successor_claim_observed": True},
    ],
)
def test_process_epoch_confirmation_rejects_unproved_or_late_absence(bad: dict[str, Any]) -> None:
    facts = {
        "action": "signal_processes",
        "driver_pod_delete_calls": 0,
        "native_identity_matched": True,
        "native_gone": True,
        "old_epoch": 7,
        "confirmed_epoch": 7,
        "old_claim_still_bound_to_request": True,
        "successor_claim_observed": False,
    }
    assert all(poe.process_interruption_checks(facts).values())
    assert not poe.process_interruption_checks({**facts, **bad})["old_epoch_confirmed_before_bind"]


def test_item_usage_preserves_unknown_settlement_and_charges_full_share(tmp_path: Path) -> None:
    first = _live_ledger(tmp_path)
    a = first.reserve(poe.SLOT_NAMES[0], T0)
    first.settle(a.seq, poe.SessionUsage(60, 1, True), 6, T0 + 6)
    b = first.reserve(poe.SLOT_NAMES[1], T0 + 7)
    first.mark_started(b.seq, "synthetic-native-2", T0 + 8)
    first.finish("failed", T0 + 9)
    second = poe.OwnerLedger(first.db, "acme-run-2", first.scrub)
    second.begin(CANDIDATE, "live", T0 + 10)
    second.start_live(T0 + 10, T0 + poe.GRANT_MIN_LIFETIME + 60)
    second.configure_item([first.run_id], 16384, T0 + 10)
    usage = second.item_usage()
    assert usage["charged_output_tokens"] == 1084
    assert [s["basis"] for s in usage["sessions"]] == ["native_transcript", "full_reservation"]
    assert usage["sessions"][1]["measured_output_tokens"] is None
    assert first.sessions()[1]["settled_at"] is None
    assert first.sessions()[1]["known"] is None
    second.reserve(poe.SLOT_NAMES[0], T0 + 10)
    second.check_spend({1: poe.SessionUsage(200, 1, True)}, T0 + 11)
    assert second.observations("budget")["item.usage"]["charged_output_tokens"] == 1284
    second.configure_item([first.run_id], 1283, T0 + 12)
    _refused(
        "budget_exceeded", lambda: second.check_spend({1: poe.SessionUsage(200, 1, True)}, T0 + 13)
    )
    assert second.observations("budget")["spend_guard.breach"] == "item_output_tokens"


def test_item_cap_refuses_a_reservation_before_start(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    ledger.configure_item([], poe.SESSION_OUTPUT_TOKENS - 1, T0)
    _refused("budget_exceeded", lambda: ledger.reserve(poe.SLOT_NAMES[0], T0))
    assert ledger.sessions() == []


def test_item_cap_counts_qualification_probe_output(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    ledger.record("budget", "grant_probe.outcome", {"output_tokens": 4}, T0)
    ledger.configure_item([], poe.SESSION_OUTPUT_TOKENS, T0)
    assert ledger.item_usage()["charged_output_tokens"] == 4
    _refused("budget_exceeded", lambda: ledger.reserve(poe.SLOT_NAMES[0], T0))


@pytest.mark.parametrize("kind", poe.RESTART_KINDS)
@pytest.mark.parametrize(
    "key", ["committed_head_restored", "committed_files_restored", "old_runtime_gone"]
)
def test_committed_state_and_old_runtime_are_hard_requirements(
    tmp_path: Path, kind: str, key: str
) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "restart")
    ledger.record("restart", "pod.workspace_nonce_restored", False, T0)
    ledger.record("restart", f"{kind}.{key}", False, T0)
    verdict = poe.judge(ledger, "restart")
    assert verdict["verdict"] == "fail"
    check = next(c for c in verdict["criteria"][0]["checks"] if c["key"] == f"{kind}.{key}")
    assert check["status"] == "fail" and "blocked_by" not in check


def test_process_and_pod_uncommitted_workspace_gaps_are_observations(
    tmp_path: Path,
) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "restart")
    for kind in ("process", "pod"):
        ledger.record("restart", f"{kind}.workspace_nonce_restored", False, T0)
    ledger.record("restart", "process.same_pod", False, T0)
    ledger.record("restart", "process.container_restarted", False, T0)
    ledger.record("restart", "worker.workspace_nonce_retained_informational", False, T0)
    for kind in poe.RESTART_KINDS:
        ledger.record("restart", f"{kind}.git_status_before", ["?? .proof-nonce"], T0)
        ledger.record("restart", f"{kind}.git_status_after", [], T0)
    result = poe.judge(ledger, "restart")
    assert result["verdict"] == "not_met"
    checks = {c["key"]: c for c in result["criteria"][0]["checks"]}
    for kind in ("process", "pod"):
        check = checks[f"{kind}.workspace_nonce_restored"]
        assert check["status"] == "not_met"
        assert check["blocked_by"] == "#5541" and check["upstream"] == "#5092"
    assert checks["process.same_pod"]["status"] == "informational"
    assert checks["process.container_restarted"]["status"] == "informational"
    assert poe.exit_code("missing") == poe.exit_code("fail") == poe.EXIT_FAIL


@pytest.mark.parametrize(
    "key",
    [
        *[
            f"{kind}.{field}"
            for kind in poe.RESTART_KINDS
            for field in ("history_identity", "fresh_native_process", "committed_files_restored")
        ],
        "pod.new_pod",
        "worker.worker_restarted",
        "process.interruption_process_only",
        "process.old_epoch_confirmed_before_bind",
    ],
)
def test_audit_keeps_explicit_recovery_requirements_blocking(tmp_path: Path, key: str) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "restart")
    for kind in ("process", "pod"):
        ledger.record("restart", f"{kind}.workspace_nonce_restored", False, T0)
    ledger.record("restart", key, False, T0)
    result = poe.judge(ledger, "restart")
    assert result["verdict"] == "fail"
    check = next(c for c in result["criteria"][0]["checks"] if c["key"] == key)
    assert check["status"] == "fail" and "blocked_by" not in check


def _budget_ledger(tmp_path: Path, **session: Any) -> poe.OwnerLedger:
    ledger = _live_ledger(tmp_path)
    for index, slot in enumerate(poe.SLOT_NAMES):
        reservation = ledger.reserve(slot, T0 + 10 * (index + 1))
        usage = poe.SessionUsage(
            session.get("tokens", 300),
            session.get("turns", 2),
            session.get("known", True),
            models=(poe.MODEL,),
        )
        if session.get("tokens", 300) == 0:
            usage = poe.SessionUsage(
                0, 0, True, error_classes=({"http_status": 401, "first_line": ""},)
            )
        ledger.settle(reservation.seq, usage, 60.0, T0 + 10 * (index + 1) + 5)
        if not usage.known:
            break
    ledger.record("budget", "grant_probe.ok", True, T0)
    ledger.mark_cleanup(started=T0 + 1000)
    return ledger


def test_budget_judge_pass_fail_and_missing(tmp_path: Path) -> None:
    assert poe.judge(_budget_ledger(tmp_path / "a"), "budget")["verdict"] == "pass"
    assert poe.judge(_budget_ledger(tmp_path / "z", tokens=0), "budget")["verdict"] == "pass"
    unknown = poe.judge(_budget_ledger(tmp_path / "b", known=False), "budget")
    assert unknown["verdict"] == "fail"
    ledger = _live_ledger(tmp_path / "c")
    ledger.record_unreserved("pod:cli", T0 + 1)
    assert poe.judge(ledger, "budget")["verdict"] == "fail"
    missing_probe = _budget_ledger(tmp_path / "d")
    missing_probe.db.execute("DELETE FROM observations WHERE key = 'grant_probe.ok'")
    assert poe.judge(missing_probe, "budget")["verdict"] == "fail"


def _cleanup_ledger(tmp_path: Path, mode: str = "live") -> poe.OwnerLedger:
    ledger = poe.OwnerLedger.open(_state(tmp_path), "acme-run-1", create=True)
    ledger.begin(CANDIDATE, mode, T0)
    for kind in poe.CLEANUP_KINDS:
        ledger.record_cleanup(
            kind, "ref", "ok" if kind == "foreign_untouched" else "absent", "", T0
        )
    ledger.record_cleanup("step", "fence", "ok", "", T0)
    ledger.mark_cleanup(started=T0, finished=T0 + 120)
    return ledger


def test_cleanup_judge_pass_fail_and_missing(tmp_path: Path) -> None:
    assert poe.judge(_cleanup_ledger(tmp_path / "a"), "cleanup")["verdict"] == "pass"
    present = _cleanup_ledger(tmp_path / "b")
    present.record_cleanup("pvs", "volume", "present", "", T0)
    assert poe.judge(present, "cleanup")["verdict"] == "fail"
    slow = _cleanup_ledger(tmp_path / "c")
    slow.mark_cleanup(finished=T0 + poe.CLEANUP_SECONDS + 1)
    assert poe.judge(slow, "cleanup")["verdict"] == "fail"
    gap = _cleanup_ledger(tmp_path / "d")
    gap.db.execute("DELETE FROM cleanup WHERE kind = 'registry'")
    assert poe.judge(gap, "cleanup")["verdict"] == "missing"
    failed = _cleanup_ledger(tmp_path / "e")
    failed.record_cleanup("step", "registry", "failed", "StepFailed", T0)
    assert poe.judge(failed, "cleanup")["verdict"] == "fail"


def test_rehearsal_ledger_is_judged_only_on_cleanup(tmp_path: Path) -> None:
    ledger = _cleanup_ledger(tmp_path, "rehearse")
    _refused("rehearsal_ledger", lambda: poe.judge(ledger, "selection"))
    _refused("rehearsal_ledger", lambda: poe.judge(ledger, "all"))
    assert poe.judge(ledger, "cleanup")["verdict"] == "pass"
    _refused("criterion_invalid", lambda: poe.judge(ledger, "everything"))


def test_judge_command_exit_codes_and_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "selection")
    _record_all(ledger, "restart")
    ledger.record("restart", "pod.workspace_nonce_restored", False, T0)
    ledger.close()
    state = str(tmp_path / "state")
    base = ["judge", "--state-dir", state, "--run-id", "acme-run-1"]
    assert _main([*base, "--criterion", "selection"]) == poe.EXIT_PASS
    assert json.loads(capsys.readouterr().out)["verdict"] == "pass"
    assert _main([*base, "--criterion", "restart"]) == poe.EXIT_NOT_MET
    capsys.readouterr()
    assert _main([*base, "--criterion", "disconnect"]) == poe.EXIT_FAIL
    assert json.loads(capsys.readouterr().out)["verdict"] == "missing"
    other = "fedcba9876543210fedcba9876543210fedcba98"
    assert _main([*base, "--criterion", "selection", "--candidate", other]) == poe.EXIT_REFUSED
    assert json.loads(capsys.readouterr().out)["error"] == "candidate_mismatch"


# --------------------------------------------------------------------------
# Sanitization: no held value in any output, argv, environment or error
# --------------------------------------------------------------------------


def test_output_scrubs_held_values_and_their_encodings(capsys: pytest.CaptureFixture[str]) -> None:
    scrubber = pps.Scrubber([pps.HeldSecret(GRANT), pps.HeldSecret(HEADER)])
    out = driver.Output(scrubber, pps.TokenScan(scrubber, ("stdout", "stderr")))
    encoded = base64.b64encode(HEADER.encode()).decode()
    out.emit({"grant": GRANT, "header": encoded})
    out.progress(f"saw {GRANT}")
    captured = capsys.readouterr()
    for text in (captured.out, captured.err):
        assert GRANT not in text and HEADER not in text and encoded not in text
    assert poe.sanitized_json({"value": GRANT}, scrubber) == '{"value": "<redacted>"}'


def test_ledger_rows_are_scrubbed(tmp_path: Path) -> None:
    scrubber = pps.Scrubber([pps.HeldSecret(GRANT)])
    ledger = poe.OwnerLedger.open(_state(tmp_path), "acme-run-1", create=True, scrub=scrubber.scrub)
    ledger.begin(CANDIDATE, "live", T0)
    ledger.record("budget", "grant_probe.outcome", {"first_line": f"Bearer {GRANT}"}, T0)
    ledger.record_cleanup("step", "fence", "failed", f"detail {GRANT}", T0)
    ledger.close()
    assert GRANT.encode() not in (tmp_path / "state" / poe.LEDGER_FILE).read_bytes()


def test_argv_builders_name_the_context_and_carry_no_value(tmp_path: Path) -> None:
    kubectl = poe.kubectl_argv("fixture-context", "acme-owner-proof", "get", "pods")
    assert kubectl[:3] == ["kubectl", "--context", "fixture-context"]
    assert poe.kubectl_argv("fixture-context", None, "get", "--raw=/readyz")[1:3] == [
        "--context",
        "fixture-context",
    ]
    assert poe.docker_argv("other-context", "info")[:3] == ["docker", "--context", "other-context"]
    for argv in (kubectl, pps.grant_probe_argv("/usr/bin/claude")):
        assert all(GRANT not in arg and HEADER not in arg for arg in argv)


def test_child_environment_is_an_allowlist_of_names_and_paths(tmp_path: Path) -> None:
    flags = poe.require_flags(**_flag_values(tmp_path))
    inputs = poe.load_input_file(_input(tmp_path))
    environ = {
        "PATH": "/usr/bin",
        "HOME": "/home/acme",
        "CLAUDE_CODE_OAUTH_TOKEN": GRANT,
        "SENTINEL_TOKEN": HEADER,
        "AWS_PROFILE": "acme",
    }
    env = poe.child_env(environ, flags=flags, inputs=inputs, evidence_dir=tmp_path / "evidence")
    assert set(env) <= {
        *poe.CHILD_ENV_NAMES,
        "PYTHONDONTWRITEBYTECODE",
        "DOCKER_CONTEXT",
        "KUBECONFIG",
        "THROWAWAY_CONTEXT",
        "CURIE_PERSONAL_TEST_NAMESPACE",
        "CURIE_BIN",
        "CURIE_PERSONAL_RUNNER_API_HOST",
        "CURIE_PERSONAL_EVIDENCE_DIR",
        "CURIE_PERSONAL_PROOF_STATE_DIR",
    }
    assert all(GRANT not in value and HEADER not in value for value in env.values())
    assert env["THROWAWAY_CONTEXT"] == "fixture-context"


def test_forwarded_child_argv_carries_paths_never_values(tmp_path: Path) -> None:
    grant = _private(tmp_path / "grant", GRANT)
    args = driver.build_parser().parse_args(
        _cluster_args(tmp_path, "run", **{"--grant-file": str(grant)})
    )
    argv = driver.forwarded(args)
    assert str(grant) in argv
    assert all(GRANT not in arg for arg in argv)


def test_refusal_payload_names_codes_only() -> None:
    payload = poe.Refusal("rbac_denied", "create secrets").payload()
    assert payload["error"] == "rbac_denied"
    assert "create secrets" in payload["fix"]
    assert poe.Refusal("grant_unsafe").payload()["fix"] == pps.FIXES["grant_unsafe"]


def test_in_pod_programs_compile() -> None:
    for name in (
        "HOLD_SCRIPT",
        "LAUNCH_SCRIPT",
        "LAUNCH_RELEASE_PROGRAM",
        "SCAN_SCRIPT",
        "SENTINEL_SERVER",
        "SWEEP_PROGRAM",
        "READ_PROGRAM",
        "RELEASE_PROGRAM",
        "WORKSPACE_PROGRAM",
        "STUB_TREE_PROGRAM",
        "STUB_BOOT",
        "INTERRUPT_PROGRAM",
        "KILL_PROGRAM",
        "HISTORY_IDS_PROGRAM",
    ):
        compile(getattr(driver, name), name, "exec")
        assert GRANT not in getattr(driver, name)


# --------------------------------------------------------------------------
# The code host stub and the repository workspace
# --------------------------------------------------------------------------

IMAGE = "localhost:5005/acme-runner@sha256:" + "0" * 64


def test_chart_workspace_init_is_the_candidate_charts_program() -> None:
    program = poe.chart_workspace_init(driver.CHART_SANDBOX.read_text(), poe.CHART_WORKSPACE_VALUES)
    assert "{{" not in program and "}}" not in program
    assert 'pathlib.Path("/workspace")' in program
    assert "workspace-fetch: archive digest mismatch" in program
    assert "checkout origin is not credential-free" in program


def test_chart_workspace_init_refuses_a_missing_or_unrendered_block() -> None:
    with pytest.raises(ValueError):
        poe.chart_workspace_init("containers:\n  - name: runner\n", poe.CHART_WORKSPACE_VALUES)
    template = (
        "        - name: workspace-init\n"
        "          command:\n"
        "            - python\n"
        "            - -c\n"
        "            - |\n"
        '              ref = "CURIE_WORKSPACE_REF"\n'
        "              limit = {{ .Values.unknown }}\n"
        "          env:\n"
    )
    with pytest.raises(ValueError):
        poe.chart_workspace_init(template, poe.CHART_WORKSPACE_VALUES)
    rendered = poe.chart_workspace_init(
        template.replace("{{ .Values.unknown }}", "{{ $wf.fetchTimeoutSeconds }}"),
        poe.CHART_WORKSPACE_VALUES,
    )
    assert rendered == 'ref = "CURIE_WORKSPACE_REF"\nlimit = 45\n'


def _source_template() -> dict[str, Any]:
    common = {"image": IMAGE, "imagePullPolicy": "Always", "securityContext": {"a": 1}}
    return {
        "podTemplate": {
            "spec": {
                "volumes": [{"name": "tmp", "emptyDir": {}}],
                "initContainers": [
                    {**common, "name": "bundle-fetch", "env": []},
                    {**common, "name": "bundle-extract", "env": []},
                ],
                "containers": [
                    {**common, "name": "runner", "env": [{"name": "HOME", "value": "/h"}]},
                    {**common, "name": "capture", "env": []},
                ],
            }
        }
    }


def test_workspace_template_adds_the_charts_workspace_and_code_host_trust() -> None:
    source = _source_template()
    spec = poe.workspace_template(
        source, program="print('stage')\n", ca_config_map="code-host-ca", ca_key="ca.crt"
    )
    assert source == _source_template()
    pod = spec["podTemplate"]["spec"]
    assert [item["name"] for item in pod["initContainers"]] == [
        "bundle-fetch",
        "bundle-extract",
        "workspace-init",
    ]
    init = pod["initContainers"][-1]
    assert init["image"] == IMAGE and init["command"] == ["python", "-c", "print('stage')\n"]
    assert init["env"] == [{"name": name, "value": ""} for name in poe.WORKSPACE_INIT_ENV]
    volumes = {item["name"]: item for item in pod["volumes"]}
    assert volumes["workspace"] == {"name": "workspace", "emptyDir": {"sizeLimit": "1Gi"}}
    assert volumes["code-host-trust"]["configMap"] == {
        "name": "code-host-ca",
        "items": [{"key": "ca.crt", "path": "ca.crt"}],
    }
    runner = pod["containers"][0]
    mounts = {item["mountPath"]: item for item in runner["volumeMounts"]}
    assert mounts["/workspace"]["name"] == "workspace"
    assert mounts["/etc/curie/code-host-trust"]["readOnly"] is True
    env = {item["name"]: item["value"] for item in runner["env"]}
    for name in poe.CODE_HOST_TRUST_ENV:
        assert env[name] == "/etc/curie/code-host-trust/ca.crt"
    assert env["GIT_CONFIG_KEY_0"] == "safe.directory"
    assert env["GIT_CONFIG_VALUE_0"] == "/workspace"
    assert "volumeMounts" not in pod["containers"][1]
    with pytest.raises(ValueError):
        poe.workspace_template(spec, program="pass\n", ca_config_map="c", ca_key="k")


def test_code_host_manifests_seed_the_recorded_stub() -> None:
    base = tch.stub_manifests("acme-owner-proof", IMAGE, driver.STUB_PORT)
    manifests = poe.code_host_manifests(
        base,
        image=IMAGE,
        port=driver.STUB_PORT,
        seed_files=driver.SEED_FILES,
        issue_body=driver.SEED_ISSUE,
        boot=driver.STUB_BOOT,
        labels={"curie-proof-owner": "acme-run"},
    )
    by_kind = {item["kind"]: item for item in manifests}
    assert all(item["metadata"]["labels"]["curie-proof-owner"] == "acme-run" for item in manifests)
    container = by_kind["Pod"]["spec"]["containers"][0]
    assert container["image"] == IMAGE and container["imagePullPolicy"] == "Always"
    assert container["command"] == ["python3", "-c", driver.STUB_BOOT]
    args = container["args"]
    assert args[args.index("--seed-tree") + 1] == "/work/tree"
    assert args[args.index("--issue-file") + 1] == "/work/issue.md"
    assert args[args.index("--host") + 1] == "github-stub.acme-owner-proof.svc.cluster.local"
    assert container["readinessProbe"]["tcpSocket"]["port"] == driver.STUB_PORT
    seed = next(v for v in by_kind["Pod"]["spec"]["volumes"] if v["name"] == "seed")
    paths = {item["path"] for item in seed["configMap"]["items"]}
    assert paths == {"tree/README.md", "tree/example.py", "issue.md"}
    data = by_kind["ConfigMap"]["data"]
    assert sorted(value for key, value in data.items() if key.startswith("seed-")) == sorted(
        [*driver.SEED_FILES.values(), driver.SEED_ISSUE]
    )
    assert base[1]["spec"]["containers"][0]["image"] == IMAGE
    assert "--seed-tree" not in base[1]["spec"]["containers"][0]["args"]
    for bad in ({"../escape": "x"}, {".hidden": "x"}, {}):
        with pytest.raises(ValueError):
            poe.code_host_manifests(
                base,
                image=IMAGE,
                port=driver.STUB_PORT,
                seed_files=bad,
                issue_body="",
                boot="",
                labels={},
            )


def test_committed_restoration_compares_both_sides_with_the_stub() -> None:
    source = {"head": "a" * 40, "files": {"README.md": "1" * 64}}
    same = {"head": "a" * 40, "files": {"README.md": "1" * 64}, "nonce": "absent"}
    assert poe.committed_restoration(source, same, same) == {
        "committed_head_restored": True,
        "committed_files_restored": True,
    }
    moved = {**same, "head": "b" * 40}
    edited = {**same, "files": {"README.md": "2" * 64}}
    assert poe.committed_restoration(source, same, moved)["committed_head_restored"] is False
    assert poe.committed_restoration(source, edited, same)["committed_files_restored"] is False
    empty = poe.committed_restoration({}, {}, {})
    assert empty == {"committed_head_restored": False, "committed_files_restored": False}


# --------------------------------------------------------------------------
# Runner reachability and the request each observation reads
# --------------------------------------------------------------------------

NAMESPACE = "acme-owner-proof"
SUFFIX = f".{NAMESPACE}.svc.cluster.local"


def _pod(name: str, uid: str, *, phase: str = "Running", **meta: Any) -> dict[str, Any]:
    return {"metadata": {"name": name, "uid": uid, **meta}, "status": {"phase": phase}}


def test_runner_loopback_is_deterministic_and_never_the_driver_loopback() -> None:
    import ipaddress

    names = [f"acme-thread-{index}{SUFFIX}" for index in range(200)]
    addresses = [poe.runner_loopback(name) for name in names]
    assert addresses == [poe.runner_loopback(name.upper() + ".") for name in names]
    for address in addresses:
        parsed = ipaddress.ip_address(address)
        assert parsed.is_loopback and address != "127.0.0.1"
        assert int(address.split(".")[1]) >= 64
    assert poe.runner_suffix(NAMESPACE) == SUFFIX


def test_runner_port_is_the_workers_default() -> None:
    import dataclasses

    from curie_worker.sandbox.types import SubstrateConfig

    field = next(f for f in dataclasses.fields(SubstrateConfig) if f.name == "runner_port")
    assert field.default == poe.RUNNER_PORT


def test_runner_forwards_follow_each_sandboxes_running_pod() -> None:
    sandboxes = [
        {"metadata": {"name": "acme-a"}, "status": {"serviceFQDN": f"acme-a{SUFFIX}"}},
        {"metadata": {"name": "acme-b"}, "status": {}},
        {"metadata": {"name": "acme-c"}, "status": {"serviceFQDN": f"acme-c{SUFFIX}"}},
        {"metadata": {"name": "acme-d"}, "status": {"serviceFQDN": f"acme-d{SUFFIX}"}},
        {"metadata": {"name": "acme-e"}, "status": {"serviceFQDN": "acme-e.elsewhere.svc"}},
    ]
    pods = [
        _pod("acme-a", "uid-a"),
        # Before serviceFQDN is published the Service is named for the Sandbox.
        _pod("acme-b", "uid-b"),
        # Adopted: the Pod names its Sandbox by owner reference.
        _pod("acme-warm-1", "uid-c", ownerReferences=[{"kind": "Sandbox", "name": "acme-c"}]),
        _pod("acme-d", "uid-d", deletionTimestamp="2026-01-01T00:00:00Z"),
        _pod("acme-e", "uid-e"),
    ]
    found = poe.runner_forwards(sandboxes, pods, NAMESPACE)
    assert sorted(found) == [f"acme-a{SUFFIX}", f"acme-b{SUFFIX}", f"acme-c{SUFFIX}"]
    assert found[f"acme-a{SUFFIX}"] == {
        "address": poe.runner_loopback(f"acme-a{SUFFIX}"),
        "pod": "acme-a",
        "uid": "uid-a",
    }
    assert found[f"acme-c{SUFFIX}"]["pod"] == "acme-warm-1"
    pending = [_pod("acme-a", "uid-a", phase="Pending")]
    assert poe.runner_forwards(sandboxes[:1], pending, NAMESPACE) == {}


def test_runner_forwards_refuse_two_names_on_one_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(poe, "runner_loopback", lambda _name: "127.64.0.1")
    sandboxes = [{"metadata": {"name": name}, "status": {}} for name in ("acme-a", "acme-b")]
    pods = [_pod("acme-a", "uid-a"), _pod("acme-b", "uid-b")]
    with pytest.raises(ValueError, match="share one loopback"):
        poe.runner_forwards(sandboxes, pods, NAMESPACE)


def test_worker_resolver_maps_only_runner_names_to_their_loopback() -> None:
    import socket

    namespace: dict[str, Any] = {}
    exec(compile(driver.RESOLVER_DEFS, "resolver", "exec"), namespace)  # noqa: S102 - the program
    calls: list[tuple[Any, ...]] = []

    def resolve(*args: Any) -> list[Any]:
        calls.append(args)
        return []

    getaddrinfo = namespace["_resolver"](SUFFIX, resolve)
    getaddrinfo(f"acme-a{SUFFIX}", 8080, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, 0)
    getaddrinfo(f"acme-a{SUFFIX}.".encode(), 8080)
    getaddrinfo("127.0.0.1", 5432, socket.AF_UNSPEC, socket.SOCK_STREAM)
    getaddrinfo("acme-a.elsewhere.svc.cluster.local", 8080)
    address = poe.runner_loopback(f"acme-a{SUFFIX}")
    assert calls[0] == (address, 8080, socket.AF_INET, socket.SOCK_STREAM, 0, 0)
    assert calls[1][:3] == (address, 8080, socket.AF_INET)
    assert calls[2] == ("127.0.0.1", 5432, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, 0)
    assert calls[3][0] == "acme-a.elsewhere.svc.cluster.local"
    assert namespace["runner_loopback"](f"acme-b{SUFFIX}") == poe.runner_loopback(f"acme-b{SUFFIX}")


def test_worker_program_runs_the_worker_module_behind_the_resolver() -> None:
    compile(driver.WORKER_PROGRAM, "worker", "exec")
    assert driver.WORKER_PROGRAM.startswith(driver.RESOLVER_DEFS)
    assert 'runpy.run_module("curie_worker", run_name="__main__"' in driver.WORKER_PROGRAM
    assert "socket.getaddrinfo = _resolver(sys.argv[1], socket.getaddrinfo)" in (
        driver.WORKER_PROGRAM
    )


def test_rbac_covers_the_runner_forwards() -> None:
    checks = driver.rbac_checks(NAMESPACE, f"{NAMESPACE}-grant")
    assert ("create", "pods/portforward", NAMESPACE) in checks
    assert ("list", "sandboxes.agents.x-k8s.io", NAMESPACE) in checks


def test_history_restored_is_an_ordered_prefix() -> None:
    assert poe.history_restored(["m"], ["m"])
    # A turn that finished before the interruption appends after the marker.
    assert poe.history_restored(["m"], ["m", "t1"])
    assert not poe.history_restored(["m"], [])
    assert not poe.history_restored(["m"], ["other"])
    assert not poe.history_restored(["m", "t1"], ["t1", "m"])
    assert not poe.history_restored([], [])
    assert not poe.history_restored(["m"], None)


def test_worker_launched_needs_the_workers_start_on_the_observed_claim() -> None:
    worker = "acme-proof-worker"
    running = {"started_at": T0, "runtime_claim_name": "acme-claim", "terminal_at": None}
    settled = {**running, "terminal_at": T0 + 5}

    def launched(row: Any, owner: str | None, *, rehearsal: bool = False) -> bool:
        return poe.worker_launched(
            row,
            claim="acme-claim",
            captured_owner=owner,
            worker_owner=worker,
            rehearsal=rehearsal,
        )

    assert launched(running, worker)
    assert not launched(running, "acme-proof-launcher")
    assert not launched({**running, "started_at": None}, worker)
    assert not launched({**running, "runtime_claim_name": "acme-other"}, worker)
    assert not launched(None, worker)
    # Terminal settlement cleared the owner: only a rehearsal accepts that.
    assert launched(settled, None, rehearsal=True)
    assert not launched(settled, None)
    assert not launched(running, None, rehearsal=True)


def test_retained_request_is_the_started_request_on_the_claim() -> None:
    rows: list[dict[str, Any]] = [
        {"id": "r1", "runtime_claim_name": "acme-a", "started_at": T0, "status": "failed"},
        {"id": "r2", "runtime_claim_name": "acme-b", "started_at": T0, "status": "running"},
        {"id": "r3", "runtime_claim_name": None, "started_at": None, "status": "waiting"},
    ]
    assert poe.retained_request(rows, "acme-b") == rows[1]
    assert poe.retained_request(rows, "acme-a") == rows[0]
    # A never-started request holds no claim, so it is never the target.
    assert poe.retained_request(rows, "") is None
    assert poe.retained_request(rows[2:], "acme-b") is None
    assert [row["id"] for row in poe.active_requests(rows)] == ["r2", "r3"]
    assert poe.active_requests([{"status": "cancellation_requested"}]) != []
    assert poe.active_requests([{"status": s} for s in ("completed", "failed", "expired")]) == []


# --------------------------------------------------------------------------
# The finalizer
# --------------------------------------------------------------------------


def test_finalizer_runs_every_step_newest_first() -> None:
    ran: list[str] = []
    recorded: list[tuple[str, bool, str]] = []
    finalizer = poe.Finalizer(lambda name, ok, detail: recorded.append((name, ok, detail)))

    def failing(message: str) -> Callable[[], None]:
        def step() -> None:
            ran.append(message)
            raise poe.StepFailed(f"{message} failed")

        return step

    def leaking() -> None:
        ran.append("leaking")
        raise RuntimeError(f"carries {GRANT}")

    finalizer.register("first", lambda: ran.append("first"))
    finalizer.register("second", failing("second"))
    finalizer.register("third", leaking)
    assert finalizer.run() == [("third", False), ("second", False), ("first", True)]
    assert ran == ["leaking", "second", "first"]
    assert recorded[0] == ("third", False, "RuntimeError")
    assert recorded[1] == ("second", False, "second failed")
    assert finalizer.run() == []


def test_teardown_order_fences_first_and_proves_absence_last() -> None:
    order = driver.TEARDOWN_ORDER
    assert order[0] == "fence" and order[-1] == "absence"
    assert order.index("inventory") < order.index("proof-cleanup") < order.index("registry")
    ran: list[str] = []
    finalizer = poe.Finalizer()

    def step(name: str) -> Callable[[], None]:
        return lambda: ran.append(name)

    for name in reversed(order):
        finalizer.register(name, step(name))
    finalizer.run()
    assert tuple(ran) == order


def test_vanished_is_only_a_not_found() -> None:
    class Status(Exception):
        def __init__(self, status: int) -> None:
            super().__init__("synthetic")
            self.status = status

    assert poe.vanished(Status(404))
    assert not poe.vanished(Status(403))
    assert not poe.vanished(RuntimeError("synthetic"))


def test_failure_site_names_class_and_functions_never_values() -> None:
    marker = "synthetic-value-never-in-a-site"

    def capture_pod() -> None:
        raise LookupError(marker)

    def revalidated() -> None:
        capture_pod()

    try:
        revalidated()
    except LookupError as exc:
        site = poe.failure_site(exc, Path(__file__).resolve().parents[3])
    assert site.startswith("LookupError in capture_pod (test_personal_owner_enforcement.py:")
    assert marker not in site
    try:
        raise ValueError(marker)
    except ValueError as exc:
        assert poe.failure_site(exc, Path("/nonexistent-root")) == "ValueError"


# --------------------------------------------------------------------------
# Publication on: one session, at most two queries
# --------------------------------------------------------------------------


def _check_of(verdict: dict[str, Any], key: str) -> dict[str, Any]:
    checks = [check for item in verdict["criteria"] for check in item["checks"]]
    return next(check for check in checks if check["key"] == key)


def test_session_ceiling_is_one_constant_over_every_query(tmp_path: Path) -> None:
    assert poe.SESSION_QUERIES * poe.SESSION_TURNS == poe.SESSION_ASSISTANT_TURNS_CEILING == 4
    ledger = _live_ledger(tmp_path)
    reservation = ledger.reserve(poe.SLOT_NAMES[0], T0 + 10)
    ledger.settle(reservation.seq, poe.SessionUsage(600, 4, True, models=(poe.MODEL,)), 60, T0 + 20)
    ledger.record("budget", "grant_probe.ok", True, T0)
    verdict = poe.judge(ledger, "budget")
    assert _check_of(verdict, "session.process-interrupted.assistant_turns")["observed"] == 4
    assert _check_of(verdict, "every_session_within_share")["observed"] is True
    ledger.settle(reservation.seq, poe.SessionUsage(700, 5, True, models=(poe.MODEL,)), 61, T0 + 21)
    verdict = poe.judge(ledger, "budget")
    assert _check_of(verdict, "session.process-interrupted.assistant_turns")["status"] == "fail"
    _refused("budget_exceeded", lambda: ledger.reserve(poe.SLOT_NAMES[1], T0 + 30))


def test_paid_objectives_answer_the_continuation_without_a_tool() -> None:
    for objective in (driver.PAIR_OBJECTIVE, driver.HOSTED_OBJECTIVE):
        assert objective.endswith(driver.CONTINUATION_ANSWER)
        assert "`No changes needed:`" in objective and "call no tool" in objective
    assert driver.REHEARSE_PAIR_OBJECTIVE == "[fake:progress-demo]"


def test_hold_script_holds_read_once_per_process_and_every_stop() -> None:
    script = driver.HOLD_SCRIPT
    compile(script, "hold", "exec")
    assert 'once = root / f"held-{mode}-{pid}-{start}"' in script
    assert 'if mode == "read" and once.exists():' in script
    # Stop has no once marker: each query's Stop is harvested and released.
    assert 'mode == "stop"' not in script


# Proof-driver guards use the actual scenario methods with synthetic observations.
def _proof() -> Any:
    cls = driver.owner_proof_class(driver.load_enrollment())
    proof = cls.__new__(cls)
    proof.disk_meter = None
    proof.disk_failure = None
    proof.cleanup_budget = None
    return proof


def test_process_interruption_records_absence_and_epoch_before_recovery() -> None:
    proof = _proof()
    proof.paid = True
    proof.driver_pod_deletions = []
    trace: list[str] = []
    proof.owner_claims = lambda: trace.append("claims") or ["acme-claim"]
    proof.requests_of = lambda _: (
        trace.append("epoch")
        or [{"id": "acme-request", "runtime_epoch": 7, "runtime_claim_name": "acme-claim"}]
    )
    proof.observe = lambda *args: None

    def execute(pod: str, program: str, pid: str, start: str, **kwargs: Any) -> tuple[int, bytes]:
        assert (pod, program, pid, start) == ("acme-pod", driver.INTERRUPT_PROGRAM, "42", "200")
        trace.append("processes_signalled")
        return 0, json.dumps(
            {"action": "signal_processes", "native_identity_matched": True, "native_gone": True}
        ).encode()

    proof.pod_exec = execute
    track = driver.Track(
        "process-interrupted",
        1,
        pod="acme-pod",
        claim="acme-claim",
        cli="acme-uid:42:200",
        work_item_id="acme-work-item",
        request_id="acme-request",
        runtime_epoch=7,
    )
    facts = proof.interrupt("process", track)
    assert trace == ["claims", "processes_signalled", "epoch", "claims"]
    assert proof.driver_pod_deletions == []
    assert all(poe.process_interruption_checks(facts).values())


def test_worker_observer_persists_real_transport_frames_before_consumption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    from types import SimpleNamespace

    from aci_protocol import ErrorEvent, Final, SessionStatus
    from curie_worker.runner_client import TurnStream

    monkeypatch.setattr(TurnStream, "__aiter__", TurnStream.__aiter__)
    scope: dict[str, Any] = {}
    exec(driver.WORKER_EVENTS_PROGRAM, scope)
    target = tmp_path / "worker-events.sqlite3"
    scope["observe_runner_events"](str(target))
    messages = [
        ErrorEvent(message="provider refused", classification="authentication-required"),
        Final(text="authentication required", status=SessionStatus.CLASSIFIED_FAILURE),
    ]

    class Content:
        async def __aiter__(self) -> Any:
            for message in messages:
                yield (message.model_dump_json() + "\n").encode()

    async def consume() -> list[Any]:
        stream = TurnStream(
            SimpleNamespace(content=Content()),
            10,
            asyncio.get_running_loop().time() + 10,
            "example-epoch",
        )
        observed = []
        async for frame in stream:
            with sqlite3.connect(target) as db:
                count = db.execute("SELECT count(*) FROM events").fetchone()[0]
            assert count == len(observed) + 1
            observed.append(frame)
        return observed

    observed = asyncio.run(consume())
    assert [frame.model_dump() for frame in observed] == [m.model_dump() for m in messages]
    with sqlite3.connect(target) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute("SELECT * FROM events")]
    assert (
        poe.native_refusal(_native_refusal_fixture()["native_debug_stderr"].encode(), rows)[
            "aci_classification"
        ]
        == "authentication-required"
    )


@pytest.mark.parametrize("mode", ["full", "remaining"])
def test_phase_scope_keeps_every_remaining_phase(tmp_path: Path, mode: str) -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.ctx = SimpleNamespace(phases=mode)
    order: list[str] = []
    proof.snapshot_foreign = lambda: None
    proof.say = lambda _: None
    proof.step = order.append
    for method in (
        "setup",
        "prepare_owner",
        "deploy_sentinel",
        "enroll",
        "prepare_remaining",
        "rejected",
        "revalidated",
        "disconnect",
        "final_audit",
    ):
        setattr(proof, method, lambda: None)
    pairs: list[str] = []
    proof.pair = lambda kind, _: pairs.append(kind)
    proof.execute()
    assert order[-5:] == ["rejected", "revalidated", "disconnect", "audit", "completed"]
    assert pairs == ([] if mode == "remaining" else ["process", "pod", "worker"])
    assert ("remaining-generation" in order) == (mode == "remaining")


@pytest.mark.parametrize("changed_product", [False, True])
def test_scope_reuses_only_proven_criteria_with_unchanged_product(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_product: bool,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()

    git("init", "-q", "-b", "fixture")
    source_file = repo / "product.py"
    source_file.write_text("x = 1\n")
    git("add", ".")
    git("-c", "user.name=Example", "-c", "user.email=example@example.com", "commit", "-qm", "base")
    source_sha = git("rev-parse", "HEAD")
    if changed_product:
        source_file.write_text("x = 2\n")
    else:
        change = repo / "tools/factory-e2e/README.md"
        change.parent.mkdir(parents=True)
        change.write_text("proof change\n")
    git("add", ".")
    git(
        "-c", "user.name=Example", "-c", "user.email=example@example.com", "commit", "-qm", "change"
    )
    candidate = git("rev-parse", "HEAD")
    monkeypatch.setattr(driver, "REPO_ROOT", repo)
    state = _state(tmp_path)
    prior = poe.OwnerLedger.open(state, "prior", create=True)
    prior.begin(source_sha, "live", T0)
    for criterion in ("selection", "restart", "fencing", "authentication-required"):
        _record_all(prior, criterion)
    prior.record("restart", "process.workspace_nonce_restored", False, T0)
    prior.finish("failed", T0 + 1)
    current = poe.OwnerLedger(prior.db, "current", prior.scrub)
    current.begin(candidate, "live", T0 + 2)
    if changed_product:
        with pytest.raises(poe.StepFailed, match="remaining_scope_product_changed"):
            driver.reuse_prior_proof(current, candidate, "prior")
    else:
        driver.reuse_prior_proof(current, candidate, "prior")
        result = poe.judge(current, "restart")
        assert result["verdict"] == "not_met"
        assert result["candidate"] == candidate
        assert result["criteria"][0]["source_candidate"] == source_sha
        assert result["criteria"][0]["source_run_id"] == "prior"
        assert poe.judge(current, "fencing")["verdict"] == "pass"
        assert poe.judge(current, "authentication-required")["verdict"] == "missing"
        rehearsal = poe.OwnerLedger(prior.db, "rehearsal", prior.scrub)
        rehearsal.begin(candidate, "rehearse", T0 + 2)
        driver.reuse_prior_proof(rehearsal, candidate, "prior")
        inherited = next(
            c for c in poe.judge_rehearsal(rehearsal)["criteria"] if c["criterion"] == "restart"
        )
        epoch = next(
            c for c in inherited["checks"] if c["key"] == "process.old_epoch_confirmed_before_bind"
        )
        assert epoch["status"] == "pass"
        assert inherited["source_candidate"] == source_sha
        prior.record("selection", "claim.label.owner_principal_matches", False, T0)
        with pytest.raises(poe.StepFailed, match="remaining_scope_missing_selection_proof"):
            driver.reuse_prior_proof(current, candidate, "prior")


@pytest.mark.parametrize(
    "finished,status,reason",
    [(False, None, "pod_log_capture_missing_eof"), (True, 1, "pod_log_capture_transport_failed")],
)
def test_live_capture_requires_complete_successful_streams(
    tmp_path: Path,
    finished: bool,
    status: int | None,
    reason: str,
) -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.paid = True
    proof.captures = []
    log = tmp_path / "runner.log"
    log.write_bytes(b"partial capture\n")
    proof.pod_log_streams = {
        "example-pod": [(log, SimpleNamespace(poll=lambda: status, returncode=status))]
    }
    proof.wait_soft = lambda predicate, **kwargs: predicate() if finished else None
    with pytest.raises(poe.StepFailed, match=reason):
        proof.collect_completed_logs()


def test_live_capture_keeps_complete_successful_streams(tmp_path: Path) -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.captures = []
    log = tmp_path / "runner.log"
    log.write_bytes(b"complete capture\n")
    proof.pod_log_streams = {
        "example-pod": [(log, SimpleNamespace(poll=lambda: 0, returncode=0))]
    }
    proof.wait_soft = lambda predicate, **kwargs: predicate()
    proof.collect_completed_logs()
    assert proof.captures == [b"complete capture\n"]


def test_debug_capture_refuses_every_grant_except_the_invalid_control() -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.namespace = "acme-proof"
    proof.invalid = pps.HeldSecret("synthetic-invalid-control")
    ref = SimpleNamespace(name="example-token", key="grant")
    env = SimpleNamespace(
        name="CLAUDE_CODE_OAUTH_TOKEN", value_from=SimpleNamespace(secret_key_ref=ref)
    )
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[SimpleNamespace(name="runner", env=[env])])
    )
    token = SimpleNamespace(data={"grant": base64.b64encode(b"synthetic-invalid-control").decode()})
    proof.core = SimpleNamespace(
        read_namespaced_pod=lambda *a, **kw: pod, read_namespaced_secret=lambda *a, **kw: token
    )
    proof.enable_refusal_capture("example-pod")
    token.data["grant"] = base64.b64encode(GRANT.encode()).decode()
    with pytest.raises(poe.StepFailed, match="rejection_capture_refuses_debug_for_a_real_grant"):
        proof.enable_refusal_capture("example-pod")


@pytest.mark.parametrize(
    "prior_runs,cap,expected",
    [
        (["acme-run-1"], 16384, "ceiling_invalid"),
        (["missing"], 16384, "usage_unknown"),
        ([], 0, "ceiling_invalid"),
    ],
)
def test_item_accounting_refuses_invalid_history(
    tmp_path: Path,
    prior_runs: list[str],
    cap: int,
    expected: str,
) -> None:
    ledger = _live_ledger(tmp_path)
    _refused(expected, lambda: ledger.configure_item(prior_runs, cap, T0))


@pytest.mark.parametrize(
    "tokens,turns,expected",
    [
        (0, 0, None),
        (512, 2, None),
        (600, 3, "session_assistant_turns"),
        (800, 2, "session_output_tokens"),
    ],
)
def test_query_gate_checks_the_continuation_before_it_starts(
    tokens: int, turns: int, expected: str | None
) -> None:
    row = _row(1, settled=False)
    assert (
        poe.next_query_refusal([row], seq=1, session=poe.SessionUsage(tokens, turns, True))
        == expected
    )
    assert poe.next_query_refusal([row], seq=None, session=None) == "unreserved"
    assert poe.next_query_refusal([row], seq=1, session=None) == "usage_unknown"


def test_token_guard_takes_precedence_over_turn_counts(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    r = ledger.reserve(poe.SLOT_NAMES[0], T0)
    reading = ledger.check_spend({r.seq: poe.SessionUsage(8193, 1, True)}, T0 + 1)
    assert reading.breach == "output_tokens"
    assert ledger.observations("budget")["spend_guard.breach"] == "output_tokens"
    # A second reading must neither count settled usage twice nor lose an increase.
    ledger.settle(r.seq, poe.SessionUsage(400, 2, True), 1, T0 + 1)
    assert (
        poe.spend_reading(ledger.rows(), {r.seq: poe.SessionUsage(512, 2, True)}).output_tokens
        == 512
    )


def test_stop_refuses_an_over_budget_continuation_and_allows_one_that_fits() -> None:
    from types import SimpleNamespace

    proof = _proof()
    track = driver.Track("process-recovered", 1, usage=poe.SessionUsage(512, 2, True))
    proof.paid = True
    proof.ctx = SimpleNamespace(ledger=SimpleNamespace(rows=lambda: [_row(1, settled=False)]))
    released: list[str] = []
    proof.release_hold = lambda _track, mode: released.append(mode)

    def refuse(_pod: str, _ref: str, _track: Any, reason: str) -> None:
        raise poe.StepFailed(reason)

    proof.refuse_query = refuse
    proof.release_stop(track, 1)
    assert released == ["stop"]
    track.usage = poe.SessionUsage(700, 3, True)
    with pytest.raises(poe.StepFailed, match="session_assistant_turns"):
        proof.release_stop(track, 1)
    assert released == ["stop"]


@pytest.mark.parametrize("slot", [poe.SLOT_NAMES[0], "rejected"])
def test_native_launch_is_reserved_durably_before_release(tmp_path: Path, slot: str) -> None:
    from types import SimpleNamespace

    ledger = _live_ledger(tmp_path)
    r = ledger.reserve(slot, T0)
    proof = _proof()
    proof.paid, proof.pending = True, [r]
    proof.ctx = SimpleNamespace(ledger=ledger)
    captures: list[str] = []
    proof.start_log_capture = lambda *args: captures.append("logs") or tmp_path / "native.log"
    proof.enable_refusal_capture = lambda *args: captures.append("invalid-control-only")
    proof.workspace_state = lambda *args: {}
    proof.live_deadline = T0 + 1440
    proof.native = {"uid:123:456": {"pod": "acme-pod", "uid": "uid"}}
    proof.tracks = [driver.Track(r.slot, r.seq)]
    released: list[str] = []

    def pod_exec(pod: str, program: str, *args: str) -> None:
        assert ledger.sessions()[0]["native_ref"] == "uid:123:456"
        assert captures == (["logs", "invalid-control-only"] if slot == "rejected" else ["logs"])
        assert program == driver.LAUNCH_RELEASE_PROGRAM
        assert args == (("123", "456", "refusal") if slot == "rejected" else ("123", "456"))
        released.append(pod)

    proof.pod_exec = pod_exec
    proof.on_native_start("uid:123:456", T0 + 1)
    assert released == ["acme-pod"] and proof.pending == []
    proof.kill_cli = lambda *args: True
    proof.fence = lambda: None
    proof.native["uid:789:456"] = {"pod": "acme-pod", "uid": "uid"}
    with pytest.raises(poe.StepFailed, match="without a reservation"):
        proof.on_native_start("uid:789:456", T0 + 2)
    assert released == ["acme-pod"]


@pytest.mark.parametrize("allowed", [False, True])
def test_launch_wrapper_refuses_before_native_exec_and_accepts_one_permit(
    tmp_path: Path, allowed: bool
) -> None:
    import subprocess
    import sys

    marker = tmp_path / "native-ran"
    native = tmp_path / "claude-native"
    native.write_text(f"#!/bin/sh\nprintf ran > {marker}\n")
    native.chmod(0o755)
    script = driver.LAUNCH_SCRIPT.replace(repr(driver.WORK_DIR), repr(str(tmp_path)))
    script = script.replace(f"+ {driver.HOLD_SECONDS}", "+ 0")
    if allowed:
        script = script.replace(
            'hold = root / "holding-launch"',
            'permit.write_text("go")\nhold = root / "holding-launch"',
        )
    wrapper = tmp_path / "claude"
    wrapper.write_text(script)
    result = subprocess.run(
        [sys.executable, str(wrapper), "--print"], capture_output=True, timeout=5, check=False
    )
    assert result.returncode == (0 if allowed else 2)
    assert marker.exists() is allowed
    assert list(tmp_path.glob("launch-*")) == []


def test_expiry_deletes_the_pod_without_harvesting_even_when_kill_fails() -> None:
    proof = _proof()
    deleted: list[str] = []
    proof.delete_runner_pod = deleted.append
    track = driver.Track("process-interrupted", 1, pod="acme-pod", cli="uid:1:2", transcript="t")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("synthetic transport failure")

    proof.kill_cli = fail
    with pytest.raises(RuntimeError):
        proof.expire(track)
    assert deleted == ["acme-pod"]
    proof.kill_cli = lambda *args, **kwargs: True
    proof.exec_unlogged = fail
    proof.expire(track)
    assert deleted == ["acme-pod", "acme-pod"]


def test_cleanup_has_one_budget_and_preserves_absence_time() -> None:
    import signal

    budget = poe.CleanupBudget(T0)
    assert budget.wait_bound(T0 + 100) == 50
    assert budget.wait_bound(T0 + 170) == 0 and budget.remaining(T0 + 170) == 10
    with pytest.raises(poe.StepFailed, match="budget was exhausted"):
        with driver.cleanup_time_limit(0):
            pytest.fail("expired cleanup ran")
    with pytest.raises(poe.StepFailed, match="budget was exhausted"):
        with driver.cleanup_time_limit(30):
            signal.raise_signal(signal.SIGALRM)
    with driver.cleanup_time_limit(30):
        assert True
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0


@pytest.mark.parametrize(
    "uid,label,expected",
    [
        ("owned", "acme", True),
        ("foreign", "acme", False),
        ("owned", "other", False),
        (None, "acme", False),
    ],
)
def test_failure_fence_preserves_a_foreign_namespace(
    uid: str | None, label: str, expected: bool
) -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.namespace, proof.namespace_uid, proof.ownership = "acme-proof", uid, "acme"
    proof.core = SimpleNamespace(
        read_namespace=lambda *a, **k: SimpleNamespace(
            metadata=SimpleNamespace(uid="owned", labels={poe.OWNERSHIP_LABEL: label})
        )
    )
    deleted: list[str] = []
    proof.stop_worker = lambda: None
    proof.runner_pods = lambda: []
    proof.custom = SimpleNamespace(
        list_namespaced_custom_object=lambda *a, **k: {
            "items": [{"metadata": {"name": "acme-claim"}}]
        },
        delete_namespaced_custom_object=lambda *a, **k: deleted.append(a[4]),
    )
    proof.fence()
    assert deleted == (["acme-claim"] if expected else [])


@pytest.mark.parametrize(
    "key",
    [
        "bash.grant_found",
        "audit.grant_in_captures",
        "audit.invalid_grant_in_captures",
        "audit.captures_complete",
    ],
)
def test_confidentiality_judge_requires_grant_containment_and_complete_captures(
    tmp_path: Path, key: str
) -> None:
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "hosted-header")
    assert poe.judge(ledger, "hosted-header")["verdict"] == "pass"
    ledger.record("hosted-header", key, not poe.EXPECTED["hosted-header"][key], T0)
    assert _check_of(poe.judge(ledger, "hosted-header"), key)["status"] == "fail"


def test_service_fallback_includes_unlabelled_claims_and_orphan_pods() -> None:
    keys = {"mode": "m", "tenant": "t", "principal": "p"}
    owner = {"tenant": "acme", "principal": "owner"}
    personal = {"m": "personal", "t": "acme", "p": "owner"}
    assert (
        poe.fallback_runtimes(
            {"good": personal}, {"pod": "good"}, observed_claims={}, labels=keys, owner=owner
        )
        == []
    )
    assert poe.fallback_runtimes(
        {"service": {}},
        {"pod": "service", "orphan": "missing"},
        observed_claims={},
        labels=keys,
        owner=owner,
    ) == ["claim/service", "pod/orphan", "pod/pod"]


@pytest.mark.parametrize("kind", ["process", "pod"])
def test_recovery_boundary_matches_the_new_claim_for_the_active_request(kind: str) -> None:
    proof = _proof()
    thread = "acme-thread"
    from curie_worker.sandbox.types import SubstrateConfig

    config = SubstrateConfig(namespace="acme-proof", warm_pool="acme-pool")
    original, resumed = config.claim_name_for(thread, "a"), config.claim_name_for(thread, "b")
    row = {
        "id": "r",
        "started_at": T0,
        "runtime_claim_name": original,
        "runtime_owner": "acme-worker",
        "runtime_epoch": 7,
        "status": "running",
    }
    proof.paid, proof.tracks = False, []
    proof.work_items = {"issue": {"id": "w", "conversation_id": thread}}
    proof.requests_of = lambda _: [row]
    proof.pod_states = {
        "new": {"claim": resumed, "pod": "acme-pod", "uid": "new", "restarts": 1, "ready": True}
    }
    proof.wait_soft = lambda predicate, **kwargs: predicate()
    found = proof.boundary(f"{kind}-recovered", "read", work_item_id="w")
    assert found is not None and found.claim == resumed and found.request_id == "r"
    row["status"] = "completed"
    assert proof.boundary(f"{kind}-recovered", "read", work_item_id="w") is None


def test_process_absence_is_observed_without_a_recovered_session() -> None:
    proof = _proof()
    proof.paid = True
    proof.live_clis = lambda: set()
    a = driver.Track("process-interrupted", 1, pod_uid="old", restart_count=0, cli="old:1:2")
    pod = {
        "metadata": {"uid": "old"},
        "status": {"containerStatuses": [{"name": "runner", "restartCount": 1}]},
    }
    proof.runner_pods = lambda: [pod]
    assert proof.old_runtime_gone("process", a, None, {})
    pod["status"]["containerStatuses"][0]["restartCount"] = 0
    assert not proof.old_runtime_gone("process", a, None, {})
    proof.runner_pods = lambda: []
    assert proof.old_runtime_gone("process", a, None, {})


def test_missing_waiting_request_is_missing_evidence(tmp_path: Path) -> None:
    assert poe.waiting_request_started([], "r") is None
    assert poe.waiting_request_started([{"id": "r", "started_at": None}], "r") is False
    assert poe.waiting_request_started([{"id": "r", "started_at": T0}], "r") is True
    ledger = _live_ledger(tmp_path)
    _record_all(ledger, "disconnect")
    ledger.record("disconnect", "disconnect.waiting_request_started", None, T0)
    assert poe.judge(ledger, "disconnect")["verdict"] == "fail"


def test_rehearsal_judge_preserves_provider_only_readings_without_calling_them_proof(
    tmp_path: Path,
) -> None:
    ledger = poe.OwnerLedger.open(_state(tmp_path), "acme-rehearse", create=True)
    ledger.begin(CANDIDATE, "rehearse", T0)
    for name in OBSERVED:
        _record_all(ledger, name)
    ledger.record("authentication-required", "rejected.http_status", None, T0)
    ledger.record("hosted-header", "sentinel.challenge_returned", False, T0)
    result = poe.judge(ledger, "rehearsal")
    for key in ("rejected.http_status", "sentinel.challenge_returned", "per_response_output_cap"):
        assert _check_of(result, key)["status"] == "rehearsal_only"
    ledger.record("disconnect", "disconnect.waiting_request_started", None, T0)
    assert (
        _check_of(poe.judge(ledger, "rehearsal"), "disconnect.waiting_request_started")["status"]
        == "fail"
    )


def test_sentinel_missing_counts_are_never_invented_zeroes() -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.command = lambda *a, **k: SimpleNamespace(returncode=1, stdout=b"")
    proof.context, proof.namespace, proof.env = "fixture", "acme-proof", {}
    proof.pod_exec = lambda *a, **k: (1, b"")
    assert proof.sentinel_counts() == dict.fromkeys(("authorized", "unauthorized", "tool_calls"))


@pytest.mark.parametrize("missing_sentinel_capture", [False, True])
def test_final_audit_scans_both_grants_and_the_hosted_header(
    tmp_path: Path, missing_sentinel_capture: bool
) -> None:
    from types import SimpleNamespace

    proof = _proof()
    proof.paid = True
    log = tmp_path / "runner.log"
    log.write_bytes(b"completed stream\n")
    proof.pod_log_streams = {"example-pod": [(log, SimpleNamespace(poll=lambda: 0, returncode=0))]}
    proof.wait_soft = lambda predicate, **kwargs: predicate()
    proof.captures_missing, proof.captures = 0, [GRANT.encode(), b"synthetic-invalid-grant"]
    proof.grant, proof.invalid = pps.HeldSecret(GRANT), pps.HeldSecret("synthetic-invalid-grant")
    proof.sentinel_token = pps.HeldSecret(HEADER)
    proof.namespace, proof.grant_namespace = "acme-proof", "acme-grants"
    proof.custom = SimpleNamespace(list_namespaced_custom_object=lambda *a, **k: {})
    proof.core = SimpleNamespace(list_namespaced_event=lambda *a, **k: {})
    proof.api_client = SimpleNamespace(sanitize_for_serialization=lambda x: x)

    def capture_pod(_pod: str) -> None:
        proof.captures_missing += int(missing_sentinel_capture)

    proof.capture_pod, proof.check_stub = capture_pod, lambda: None
    ledger = _live_ledger(tmp_path)
    proof.observe = lambda c, k, v: ledger.record(c, k, v, T0)
    proof.final_audit()
    found = ledger.observations("hosted-header")
    assert found["audit.grant_in_captures"] is True
    assert found["audit.invalid_grant_in_captures"] is True
    assert found["audit.header_in_captures"] is False
    assert found["audit.captures_complete"] is (not missing_sentinel_capture)


def test_watchdog_expires_a_session_without_a_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    proof = _proof()
    events = iter([False, True])
    proof.watchdog_stop = SimpleNamespace(wait=lambda _: next(events))
    proof.live_expired, proof.live_deadline = False, T0 + 100
    proof.tracks = [driver.Track("process-interrupted", 1, cli="uid:1:2", deadline=T0 - 1)]
    expired: list[str] = []
    proof.expire = lambda t: expired.append(t.slot)
    monkeypatch.setattr(driver.time, "time", lambda: T0)
    proof.watch()
    assert expired == ["process-interrupted"]
    assert proof.tracks[0].expired_at == T0


def test_live_grant_is_loaded_after_unpaid_setup_not_in_the_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    enrollment = driver.load_enrollment()
    monkeypatch.setattr(enrollment.EnrollmentProof, "__init__", lambda *args: None)
    cls = driver.owner_proof_class(enrollment)
    monkeypatch.setattr(cls, "sync_scrubber", lambda _: None)
    opened: list[str] = []

    def load(path: Path) -> pps.HeldSecret:
        opened.append(str(path))
        return pps.HeldSecret(GRANT)

    monkeypatch.setattr(pps, "load_grant", load)
    ctx = SimpleNamespace(
        mode="run", flags=SimpleNamespace(release="acme-proof"), grant_file="/private/acme-grant"
    )
    proof = cls(ctx)
    assert opened == []
    proof.grant = pps.HeldSecret("synthetic-unpaid-grant")
    proof.load_live_grant()
    assert opened == ["/private/acme-grant"] and proof.grant.reveal() == GRANT


def test_a_terminating_pod_keeps_its_observed_personal_claim_provenance() -> None:
    keys = {"mode": "m", "tenant": "t", "principal": "p"}
    owner = {"tenant": "acme", "principal": "owner"}
    seen = {"deleted": {"m": "personal", "t": "acme", "p": "owner"}}
    assert (
        poe.fallback_runtimes(
            {}, {"terminating": "deleted"}, observed_claims=seen, labels=keys, owner=owner
        )
        == []
    )
    assert poe.fallback_runtimes(
        {}, {"unknown": "unseen"}, observed_claims=seen, labels=keys, owner=owner
    ) == ["pod/unknown"]
    assert poe.fallback_runtimes(
        {"deleted": {}}, {"pod": "deleted"}, observed_claims=seen, labels=keys, owner=owner
    ) == ["claim/deleted", "pod/pod"]


@pytest.mark.parametrize("paid", [False, True])
def test_a_failed_live_criterion_stops_before_a_later_session(tmp_path: Path, paid: bool) -> None:
    from types import SimpleNamespace

    proof = _proof()
    ledger = _live_ledger(tmp_path)
    proof.ctx, proof.paid = SimpleNamespace(ledger=ledger), paid
    proof.observe("selection", "claim.label.owner_tenant_matches", True)
    if paid:
        with pytest.raises(poe.StepFailed, match="selection.claim.label.owner_tenant_matches"):
            proof.observe("selection", "claim.label.owner_tenant_matches", False)
    else:
        proof.observe("selection", "claim.label.owner_tenant_matches", False)
    assert ledger.observations("selection")["claim.label.owner_tenant_matches"] is False
    proof.observe("restart", "pod.workspace_nonce_restored", False)
    proof.observe("restart", "process.workspace_nonce_restored", False)
    proof.observe("restart", "informational", "recorded")


def test_disk_start_override_is_explicit_and_forwarded(tmp_path: Path) -> None:
    args = driver.build_parser().parse_args(
        _cluster_args(tmp_path, "run") + ["--disk-start-percent", "85", "--disk-stop-percent", "88"]
    )
    flags = driver.flags_of(args)
    assert (flags.disk_start_percent, flags.disk_stop_percent) == (85, 88)
    child = driver.forwarded(args)
    assert child[child.index("--disk-start-percent") + 1] == "85"
    _refused(
        "disk_limits_invalid",
        lambda: poe.require_flags(
            **_flag_values(tmp_path / "invalid"), disk_start_percent=88, disk_stop_percent=85
        ),
    )


def test_disk_meter_samples_peak_and_stops_at_the_exact_limit() -> None:
    values = iter([85, 87, 88, 83])
    meter = poe.DiskMeter(lambda: next(values), 85, 88)
    assert meter.sample(initial=True) == 85
    assert meter.sample() == 87
    with pytest.raises(poe.StepFailed, match="disk usage"):
        meter.sample()
    assert meter.sample(final=True) == 83
    assert meter.snapshot() == {
        "start_limit_percent": 85,
        "stop_limit_percent": 88,
        "before_percent": 85,
        "peak_percent": 88,
        "after_percent": 83,
    }
    with pytest.raises(poe.StepFailed):
        poe.DiskMeter(lambda: 86, 85, 88).sample(initial=True)


def test_disk_stop_interrupts_the_runtime_before_cancelling_owned_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proof = _proof()
    proof.tracks = [driver.Track("process-interrupted", 1, cli="pod:1:2")]
    called: list[str] = []
    proof.kill_worker = lambda: called.append("worker")
    proof.expire = lambda _: called.append("runtime")
    monkeypatch.setattr(driver.os, "getpgrp", lambda: 123)
    monkeypatch.setattr(driver.os, "getpid", lambda: 123)
    monkeypatch.setattr(driver.os, "killpg", lambda *args: called.append("commands"))
    proof.stop_for_disk()
    assert called == ["worker", "runtime", "commands"]


def test_every_session_checks_disk_before_reserving_and_records_the_peak(tmp_path: Path) -> None:
    from types import SimpleNamespace

    proof = _proof()
    ledger = _live_ledger(tmp_path)
    proof.ctx, proof.paid = SimpleNamespace(ledger=ledger), True
    proof.pending, proof.tracks, proof.reserved_at = [], [], {}
    values = iter([87, 88])
    proof.disk_meter = poe.DiskMeter(lambda: next(values), 85, 88)
    stopped: list[bool] = []
    proof.stop_for_disk = lambda: stopped.append(True)
    proof.reserve(poe.SLOT_NAMES[0])
    with pytest.raises(poe.StepFailed):
        proof.reserve(poe.SLOT_NAMES[1])
    assert len(ledger.sessions()) == 1 and stopped == [True]
    assert poe.disk_receipt(ledger)["peak_percent"] == 88


def test_watchdog_checks_disk_every_thirty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    proof = _proof()
    clock = [0]

    def wait(_seconds: float) -> bool:
        clock[0] += 30
        return clock[0] > 90

    proof.watchdog_stop = SimpleNamespace(wait=wait)
    proof.live_expired, proof.live_deadline, proof.tracks = False, None, []
    observed: list[int] = []

    def read() -> int:
        observed.append(clock[0])
        return 83

    proof.disk_meter = poe.DiskMeter(read, 85, 88)
    monkeypatch.setattr(driver.time, "monotonic", lambda: clock[0])
    proof.watch()
    assert observed == [30, 60, 90]
