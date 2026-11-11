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
import subprocess
from collections.abc import Iterator
from dataclasses import replace
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
    transcript_entry_metadata,
    validate_inputs,
)

__tracebackhide__ = True
_GRANT = HeldSecret(
    os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", "")
    if os.environ.get("CURIE_PERSONAL_LIVE") == "1"
    else ""
)


def test_rebuilding_candidate_has_distinct_configs_and_digest_pinned_pods() -> None:
    # An unpaid cluster reproduction observed two manifests sharing a config;
    # CRI returned the earlier repo digest for both requested digest references.
    builds: list[bytes] = []
    published: list[str] = []
    for owner in ("a" * 24, "b" * 24):
        proof = object.__new__(ClusterProof)
        proof.namespace = "test-candidate-image"
        proof.docker_context = "scratch"
        proof.cli = "curie"
        proof.candidate = "c" * 40
        proof.ownership = owner
        proof.tags = []
        proof.evidence = {}
        digest = "sha256:" + owner[0] * 64

        def command(
            argv: object, *, candidate: str = proof.candidate, **kwargs: object,
        ) -> subprocess.CompletedProcess[bytes]:
            if argv == ["git", "rev-parse", "HEAD"]:
                output = candidate.encode()
            elif argv == ["git", "status", "--porcelain", "--untracked-files=all"]:
                output = b""
            elif "df -P" in str(argv):
                output = (
                    b"Filesystem Blocks Used Available Capacity Mounted\n"
                    b"/dev/test 100 40 60 40% /\n"
                )
            else:
                output = b"25100"
            return subprocess.CompletedProcess([], 0, output, b"")

        def docker(
            *args: str, candidate: str = proof.candidate, manifest: str = digest, **kwargs: object,
        ) -> subprocess.CompletedProcess[bytes]:
            output = b""
            status = 0
            if args[:2] == ("context", "inspect"):
                output = b'"ssh://builder.example.com"'
            elif args[0] == "info":
                output = b"/data"
            elif kwargs.get("check") is False:
                status = 1
            elif args[-1] == "{{json .Mounts}}":
                output = b"[]"
            elif args[-1] == '{{index .Config.Labels "org.opencontainers.image.revision"}}':
                output = candidate.encode()
            elif args[-1] == "{{json .RepoDigests}}":
                output = json.dumps([published[-1].rsplit(":", 1)[0] + "@" + manifest]).encode()
            elif args[0] == "push":
                published.append(args[1])
            elif args[0] == "build":
                data = kwargs["data"]
                assert isinstance(data, bytes)
                builds.append(data)
            return subprocess.CompletedProcess([], status, output, b"")

        proof.command = command  # type: ignore[method-assign]
        proof.docker = docker  # type: ignore[method-assign]
        proof.build_image()
        labels = dict(line.removeprefix("LABEL ").split("=", 1)
                      for line in builds[-1].decode().splitlines() if line.startswith("LABEL "))
        assert labels.get("curie-proof-owner") == owner
        assert labels["org.opencontainers.image.revision"] == proof.candidate
        spec = proof.pod_spec()
        for container in [*spec["containers"], *spec["initContainers"]]:
            assert container["image"] == "localhost:25100/curie-runner-candidate@" + digest
    assert builds[0] != builds[1]
    assert published[0] != published[1]


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
        return ErrorReply(
            401, "authentication_error", message="OAuth token revoked " + OWNER_GRANT,
        ) if rejected else TextReply("public control")

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
            entry = next(
                item for item in transcript_entry_metadata(observed, turn=1)
                if item["model"] == "<synthetic>"
            )
            error = entry["error_class"]
            assert error["http_status"] == 401
            # The pinned CLI humanizes 401 and drops the provider JSON type;
            # its own classification remains distinct from that missing field.
            assert error["provider_error_type"] is None
            assert error["cli_error_type"] == "authentication_failed"
            assert error["first_error_line"].startswith("Failed to authenticate. API Error: 401")
            assert "OAuth token revoked" in error["first_error_line"]
            assert OWNER_GRANT not in json.dumps(entry)
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


@pytest.mark.parametrize("mutation", [None, "seeded-content", "new-model"])
def test_native_replay_identity_requires_seed_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str | None,
) -> None:
    from aci_protocol import Event
    from curie_runner.adapter import ClaudeAgentSession, build_structured_resume
    from curie_runner.history import ConversationMessage
    from curie_runner.otel import RunTracer
    from curie_runner.session import SessionRunner
    from curie_runner.side_effects import SideEffectClassifier
    from personal_auth_support import (
        OWNER_GRANT,
        TURN_TIMEOUT_S,
        TextReply,
        installed,
        wire_session,
    )

    with installed(tmp_path, monkeypatch, lambda _: TextReply("public response")) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        wiring = wire_session(installation, reviewer_model=MODEL, implementer_model=MODEL)
        resume = build_structured_resume(
            (
                ConversationMessage(role="user", content="Earlier request"),
                ConversationMessage(role="assistant", content="Earlier answer",
                                    assistant_group="a" * 64),
            ),
            curie_session_id="replay-provenance-control", cwd=str(installation.workspace),
        )
        session = ClaudeAgentSession(replace(
            wiring.options, resume=resume.resume, session_id=None,
            session_store=resume.session_store, session_store_flush="eager",
        ))
        runner = SessionRunner(
            held_secrets=frozenset({OWNER_GRANT}), session_factory=lambda: session,
            ceiling=0, max_usd_per_day=None, tracer=RunTracer(None),
            classifier=SideEffectClassifier(), trace_name="curie-run:replay-provenance",
            session_id="session-PLACEHOLDER", model=MODEL, tool_access=wiring.access,
        )

        async def scenario():
            assert resume.session_store is not None
            seeds = await resume.session_store.load(resume.session_key)
            assert seeds
            try:
                with anyio.fail_after(TURN_TIMEOUT_S):
                    await runner.start()
                    async for _line in runner.run_turn(Event(
                        type="message", text="Return the public response.",
                        user="U0EXAMPLE1", ts="1",
                    )):
                        pass
                    assert session._personal_config is not None
                    entries = await resume.session_store.load(resume.session_key)
                    assert entries
                    observed = native_transcript_records({
                        "/home/runner/.claude/projects/replay.jsonl": b"\n".join(
                            json.dumps(entry).encode() for entry in entries
                        ),
                    })
                    observed["seeded_records"] = seeds
                    return observed
            finally:
                await runner.close()

        observed = anyio.run(scenario)
        seeded, generated = observed["assistant_messages"]
        # Observed with the pinned native CLI: portable replay has neither
        # provider model nor usage, while the new fake-provider response has both.
        assert "model" not in seeded["message"] and "usage" not in seeded["message"]
        assert generated["message"]["model"] == MODEL
        assert {
            r.body["model"] for r in installation.proxy.requests if "model" in r.body
        } == {MODEL}
        if mutation == "seeded-content":
            seeded["message"]["content"] = "Changed history"
            with pytest.raises(ProofCheckError, match="seeded transcript record changed"):
                live_transcript_identity(observed)
        elif mutation == "new-model":
            generated["message"].pop("model")
            with pytest.raises(ProofCheckError, match="unexpected model"):
                live_transcript_identity(observed)
        else:
            assert live_transcript_identity(observed) == ({generated["message"]["id"]}, {MODEL})


def test_missing_model_requires_the_exact_seeded_record() -> None:
    seed = {"type": "assistant", "uuid": "prior", "message": {"id": "old", "content": "old"}}
    response = {"type": "assistant", "uuid": "new", "message": {"id": "new", "model": MODEL}}
    observed = {"messages": [seed, response], "assistant_messages": [seed, response]}
    with pytest.raises(ProofCheckError, match="unexpected model"):
        live_transcript_identity(observed)
    observed["seeded_records"] = [{**seed, "uuid": "another"}]
    with pytest.raises(ProofCheckError, match="unexpected model"):
        live_transcript_identity(observed)


def test_transcript_metadata_keeps_only_identity_usage_and_turn() -> None:
    seed = {"type": "assistant", "uuid": "prior", "message": {"content": "private history"}}
    response = {"type": "assistant", "uuid": "new", "message": {
        "model": MODEL, "content": "private response",
        "usage": {"input_tokens": 2, "output_tokens": 3, "private": "private usage"},
    }}
    observed = {"messages": [seed, response], "seeded_records": [seed]}
    first = transcript_entry_metadata(observed, turn=1)
    assert first == [
        {"index": 0, "record_type": "assistant", "turn": 0, "provenance": "seeded",
         "model": None, "usage": None},
        {"index": 1, "record_type": "assistant", "turn": 1,
         "provenance": "new-response", "model": MODEL,
         "usage": {"input_tokens": 2, "output_tokens": 3}},
    ]
    observed["messages"].append({"type": "assistant", "uuid": "next", "message": {}})
    second = transcript_entry_metadata(observed, turn=2, prior=first)
    assert second[:2] == first and second[-1]["turn"] == 2
    assert second[-1]["model"] is None and second[-1]["provenance"] == "new-response"
    assert "private" not in json.dumps(second)


def test_zero_usage_error_metadata_redacts_held_values_and_omits_later_lines() -> None:
    private = "PRIVATE-PLACEHOLDER-installation-value"
    # Provider error JSON: https://platform.claude.com/docs/en/api/errors
    record = {"type": "assistant", "message": {
        "model": MODEL, "usage": {"input_tokens": 0, "output_tokens": 0},
        "content": [{"type": "text", "text":
                     'API Error: 403 {"error":{"type":"permission_error","message":"'
                     + private + '"}}\nPrivate later line'}],
    }}
    entry, = transcript_entry_metadata(
        {"messages": [record]}, turn=2, held_values=[private],
    )
    assert entry["error_class"]["http_status"] == 403
    assert entry["error_class"]["provider_error_type"] == "permission_error"
    assert "REDACTED" in entry["error_class"]["first_error_line"]
    assert private not in json.dumps(entry) and "Private later line" not in json.dumps(entry)
    record["message"]["content"] = [{"type": "text", "text": "Private conversation text"}]
    entry, = transcript_entry_metadata({"messages": [record]}, turn=2)
    assert entry["error_class"] == {
        "http_status": None, "provider_error_type": None,
        "cli_error_type": None, "first_error_line": None,
    }
    assert "Private conversation text" not in json.dumps(entry)


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
        with pytest.raises(ProofCheckError, match="2-session"):
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
