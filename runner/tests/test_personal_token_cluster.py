"""Exact-image custody proof, gated by CURIE_PERSONAL_LIVE=1.

The caller supplies the scratch Docker context, throwaway Kubernetes context,
fresh namespace and source-built CURIE_BIN. CURIE_PERSONAL_PROOF_PHASE=synthetic
exercises all unpaid observations without a grant. The default live phase also
requires a privately loaded CLAUDE_CODE_OAUTH_TOKEN, its stat-only private input
and a stable CURIE_PERSONAL_PROOF_RUN_ID for the durable two-session ledger.
No live failure is retried. Local stdio MCP inheritance remains unproven.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import anyio
import pytest
from personal_token_cluster_support import (
    MODEL,
    BudgetLedger,
    ClusterProof,
    HeldSecret,
    ProofCheckError,
    live_observations,
    live_transcript_identity,
    native_transcript_records,
    validate_inputs,
)

__tracebackhide__ = True
_GRANT = HeldSecret(
    os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", "")
    if os.environ.get("CURIE_PERSONAL_LIVE") == "1"
    else ""
)


def test_live_failure_diagnostics_preserve_safe_model_and_usage_observations() -> None:
    grant = HeldSecret("claude-private-synthetic-grant")
    events = [
        {"type": "error", "message": "private error " + grant.reveal()},
        {"type": "final", "status": "classified-failure", "output_tokens": 0},
    ]
    observed = {
        "assistant_messages": [
            {"message": {"model": "<synthetic>"}},
            {"message": {"model": "claude-sonnet-5-5-20261101"}},
            {"message": {"model": grant.reveal()}},
        ],
        "usage": [{"models": [{"output_tokens": 17}]}],
    }
    result = live_observations(events, observed, 1.25, grant)
    encoded = json.dumps(result)
    exposed = grant.reveal() in encoded or "private error" in encoded
    assert not exposed
    assert result == {
        "models": ["<synthetic>", "claude-sonnet-5-5-20261101"],
        "expected_model": MODEL,
        "synthetic_assistant_records": 0,
        "unreported_model_entries": 1,
        "final_status": "classified-failure",
        "final_output_tokens": 0,
        "reported_output_tokens": 17,
        "usage_report_count": 1,
        "error_event_count": 1,
        "elapsed_seconds": 1.25,
    }


@pytest.mark.parametrize("rejected", [False, True])
def test_native_transcript_model_identity_distinguishes_cli_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rejected: bool,
) -> None:
    from aci_protocol import Event
    from curie_runner.adapter import ClaudeAgentSession
    from curie_runner.otel import RunTracer
    from curie_runner.session import SessionRunner
    from curie_runner.side_effects import SideEffectClassifier
    from personal_auth_support import (
        OWNER_GRANT,
        REVIEWER_MODEL,
        TURN_TIMEOUT_S,
        ErrorReply,
        TextReply,
        installed,
        wire_session,
    )

    def responder(request):
        return ErrorReply(400, "invalid_request_error") if rejected else TextReply("public control")

    with installed(tmp_path, monkeypatch, responder) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        wiring = wire_session(
            installation, reviewer_model=REVIEWER_MODEL, implementer_model=MODEL, max_turns=4,
        )
        session = ClaudeAgentSession(wiring.options)
        runner = SessionRunner(
            held_secrets=frozenset({OWNER_GRANT}), session_factory=lambda: session,
            ceiling=0, max_usd_per_day=None, tracer=RunTracer(None),
            classifier=SideEffectClassifier(), trace_name="curie-run:personal-custody",
            session_id="session-PLACEHOLDER", model=MODEL, tool_access=wiring.access,
        )

        async def scenario():
            lines = []
            try:
                with anyio.fail_after(TURN_TIMEOUT_S):
                    await runner.start()
                    async for line in runner.run_turn(
                        Event(
                            type="message", text="Return the public control.",
                            user="U0EXAMPLE1", ts="1",
                        )
                    ):
                        lines.append(json.loads(line))
                    assert session._personal_config is not None
                    files = {
                        str(path): path.read_bytes()
                        for path in session._personal_config.claude.rglob("*.jsonl")
                    }
                    return native_transcript_records(files), lines
            finally:
                await runner.close()

        observed, events = anyio.run(scenario)
        requested = {
            request.body.get("model") for request in installation.proxy.requests
            if isinstance(request.body.get("model"), str)
        }
        assert requested == {MODEL}
        if rejected:
            # Observed on the pinned native CLI: API errors are local assistant
            # records with model <synthetic>, rather than provider messages.
            assert observed["synthetic_assistant_records"] > 0
            assert not observed["assistant_messages"]
            with pytest.raises(ProofCheckError, match="did not identify assistant turns"):
                live_transcript_identity(observed)
            assert any(event.get("type") == "error" for event in events)
        else:
            turns, models = live_transcript_identity(observed)
            assert len(turns) == 1 and models == {MODEL}
            assert any(
                event.get("type") == "final" and event.get("status") == "done" for event in events
            )


def test_live_transcript_identity_retains_wrong_model_refusal() -> None:
    def record(model: str, identifier: str) -> dict:
        return {"type": "assistant", "message": {"id": identifier, "model": model}}

    records = [record(MODEL, "provider"), record("<synthetic>", "local-error")]
    path = "/home/runner/.claude/projects/session.jsonl"
    files = {path: b"\n".join(json.dumps(r).encode() for r in records)}
    files["/tmp/unrelated.jsonl"] = json.dumps(record("claude-unrelated", "other")).encode()
    observed = native_transcript_records(files)
    assert live_transcript_identity(observed) == ({"provider"}, {MODEL})
    files[path] += b"\n" + json.dumps(record("claude-wrong", "wrong")).encode()
    with pytest.raises(ProofCheckError, match="unexpected model"):
        live_transcript_identity(native_transcript_records(files))


def test_diagnostic_retry_preserves_unknown_reservation_and_stops_at_two_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_STATE_DIR", str(tmp_path))
    original = BudgetLedger("test-budget")
    assert original.start("first-candidate") == (1, 1024)
    with pytest.raises(ProofCheckError, match="no remaining"):
        original.remaining()
    original.db.close()
    retry = BudgetLedger("test-budget", diagnostic_retry=True)
    try:
        assert retry.remaining() == 512
        assert retry.start("retry-candidate") == (2, 512)
        usage = retry.finish(2, 123, 2, 10)
        assert usage["output_tokens"] == 123
        assert usage["unknown_reserved_output_tokens"] == 1024
        assert usage["accounted_output_tokens"] == 1147
        with pytest.raises(ProofCheckError, match="two-session"):
            retry.start("third-candidate")
    finally:
        retry.db.close()


def test_diagnostic_retry_refuses_usage_above_its_session_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_STATE_DIR", str(tmp_path))
    ledger = BudgetLedger("test-session-cap", diagnostic_retry=True)
    try:
        sequence, allowance = ledger.start("candidate")
        assert allowance == 512
        with pytest.raises(ProofCheckError, match="exceeded its output allowance"):
            ledger.finish(sequence, 513, 1, 1)
    finally:
        ledger.db.close()


@pytest.fixture
def personal_cluster(request: pytest.FixtureRequest) -> Iterator[ClusterProof]:
    __tracebackhide__ = True
    if os.environ.get("CURIE_PERSONAL_LIVE") != "1":
        _GRANT.clear()
        pytest.skip("set CURIE_PERSONAL_LIVE=1 for the owned cluster proof")
    phase = os.environ.get("CURIE_PERSONAL_PROOF_PHASE", "live")
    validate_inputs(_GRANT, phase)
    proof = ClusterProof(_GRANT, phase)

    def finish() -> None:
        __tracebackhide__ = True
        try:
            proof.cleanup()
        finally:
            reporter = request.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_line(json.dumps(proof.evidence, sort_keys=True))

    request.addfinalizer(finish)
    try:
        proof.setup()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        proof.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        raise RuntimeError(
            "personal cluster proof setup failed; private details withheld"
        ) from None
    yield proof


def test_personal_token_custody_on_exact_candidate(personal_cluster: ClusterProof) -> None:
    __tracebackhide__ = True
    try:
        personal_cluster.run()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        personal_cluster.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        raise AssertionError(
            "personal cluster custody proof failed; private details withheld"
        ) from None
