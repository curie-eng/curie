"""Worker-side adoption over the existing ACI Event (ADR-0116 d2, ADR-0122).

The worker never learns success from an HTTP 200: a tolerant consumer that
ignores ``adoption_credential`` serves the turn anyway. Success is the runner's
``adoption_applied: true`` ack on every frame of the adopting turn, and the
recovery path for a lost response is ``GET /v1/status`` with the conversation
credential, whose attested ``session_id`` is what proves the binding (identity
only: never that the lost turn completed or is safe to repeat).

Pure HTTP: the server is the REAL runner app in bootstrap mode over a fake
model session (no Valkey, no cluster). Legacy and malformed consumers are the
only doubles, and they exist to prove a missing or non-boolean ack is never
counted as adoption.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from aci_protocol import Event, Final, SessionStatus, TextDelta
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.fake import FakeModelSession
from curie_runner.history import ConversationReplay, NullTranscriptStore
from curie_runner.server import bind_status_attestation
from curie_runner.session import SessionRunner
from curie_worker.runner_client import (
    AdoptionRefused,
    RunnerClient,
    RunnerError,
    RunnerStreamTimeout,
)

_BOOT = "pool-bootstrap-credential-0123456789"
_CONV = "conversation-credential-A-0123456789"
_CONV_B = "conversation-credential-B-0123456789"
_SESSION = "agent-acme-thread-C0EXAMPLE1-1700000000.000100"
_SESSION_B = "agent-acme-thread-C0EXAMPLE1-1700000000.000200"
_HISTORY = "http://api.example.com/agents/acme/state/transcript/thread-1"
_SECRETS = (_BOOT, _CONV, _CONV_B)


def _event(text: str = "hello") -> Event:
    return Event(type="message", text=text, user="U1", ts="1700000000.000100")


class _Binder:
    """The runner's ConversationBinder: records what was bound, loads nothing."""

    def __init__(self) -> None:
        self.loads: list[tuple[str, str | None]] = []

    async def load(
        self, session_id: str, history_ref: str | None
    ) -> tuple[Any, ConversationReplay, Any]:
        self.loads.append((session_id, history_ref))
        return NullTranscriptStore(), ConversationReplay(), None

    def rebind(self, session_id: str, replay: ConversationReplay) -> None:
        return None


@dataclass
class _Runner:
    server: TestServer
    runner: SessionRunner
    binder: _Binder
    sessions: list[FakeModelSession]

    @property
    def base_url(self) -> str:
        return str(self.server.make_url("")).rstrip("/")


@asynccontextmanager
async def _real_runner(
    *, token: str | None = None, bootstrap: str | None = _BOOT,
    model_factory: Callable[[], FakeModelSession] = FakeModelSession,
) -> AsyncIterator[_Runner]:
    """The real runner app: bootstrap mode by default, per-claim when ``token`` is set."""

    sessions: list[FakeModelSession] = []

    def factory() -> FakeModelSession:
        fake = model_factory()
        sessions.append(fake)
        return fake

    binder = _Binder()
    runner = SessionRunner(
        session_factory=factory,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:warm-unbound",
        session_id="warm-unbound",
        conversation_binder=binder,
    )
    await runner.start()
    # What ``__main__`` binds at boot: the attestation ``/v1/status`` reports.
    bind_status_attestation(runner, session_id="warm-unbound", sandbox_id="sbx-pool-1", cwd=None)
    app = create_app(runner, token=token, bootstrap_token=bootstrap)
    server = TestServer(app)
    await server.start_server()
    try:
        yield _Runner(server=server, runner=runner, binder=binder, sessions=sessions)
    finally:
        await server.close()


class _LegacyRunner:
    """A tolerant pre-adoption consumer: ignores the field, serves the turn, no ack."""

    def __init__(self, *, ack_value: object = None) -> None:
        self.app = web.Application()
        self.app.add_routes([web.post("/v1/event", self._event)])
        self.bodies: list[dict[str, Any]] = []
        self.ack_value = ack_value

    async def _event(self, request: web.Request) -> web.StreamResponse:
        self.bodies.append(await request.json())
        resp = web.StreamResponse(status=200, headers={"Content-Type": "application/x-ndjson"})
        await resp.prepare(request)
        for frame in (TextDelta(text="x"), Final(text="ok", status=SessionStatus.DONE)):
            payload = frame.model_dump()
            if self.ack_value is not None:
                payload["adoption_applied"] = self.ack_value
            resp_line = __import__("json").dumps(payload) + "\n"
            await resp.write(resp_line.encode("utf-8"))
        await resp.write_eof()
        return resp


async def _drain(turn: Any) -> list[Any]:
    frames = []
    async with turn:
        async for frame in turn:
            frames.append(frame)
    return frames


def _assert_no_secret(*payloads: str) -> None:
    for payload in payloads:
        for secret in _SECRETS:
            assert secret not in payload


# --- the happy path against the real runner ------------------------------------


class _TimeoutAdoptionSession(FakeModelSession):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.stopped = asyncio.Event()

    async def receive_turn(self) -> AsyncIterator[Any]:
        if len(self.queries) == 1:
            self.entered.set()
            await self.stopped.wait()
            # The SDK exception-on-interrupt path must retain both timeout
            # classification and the adopting turn's outbound acknowledgement.
            raise RuntimeError("synthetic model stop")
        async for frame in super().receive_turn():
            yield frame

    async def interrupt(self) -> None:
        await super().interrupt()
        self.stopped.set()

    async def close(self) -> None:
        self.stopped.set()
        await super().close()


@pytest.mark.parametrize("automatic_timeout", [False, True])
def test_adopting_timeout_uses_retired_binding_and_preserves_ack(
    automatic_timeout: bool,
) -> None:
    async def go() -> None:
        async with _real_runner(model_factory=_TimeoutAdoptionSession) as rt:
            client = RunnerClient(total_timeout_s=0.2 if automatic_timeout else 2.0)
            try:
                turn = await client.start_adopting_turn(
                    rt.base_url, _event(), bootstrap_token=_BOOT,
                    conversation_token=_CONV, session_id=_SESSION, history_ref=_HISTORY,
                )
                model = rt.sessions[-1]
                assert isinstance(model, _TimeoutAdoptionSession)
                await asyncio.wait_for(model.entered.wait(), timeout=1.0)
                epoch = turn._response.headers["X-Curie-Turn-Epoch"]  # noqa: SLF001
                async with ClientSession() as control:
                    for refused_token in (_BOOT, _CONV_B):
                        async with control.post(
                            rt.base_url + "/v1/timeout",
                            headers={"Authorization": f"Bearer {refused_token}",
                                     "X-Curie-Turn-Epoch": epoch},
                        ) as response:
                            assert response.status == 401
                    assert model.interrupts == 0
                    if automatic_timeout:
                        with pytest.raises(RunnerStreamTimeout):
                            await _drain(turn)
                        assert model.interrupts == 1
                    else:
                        async with control.post(
                            rt.base_url + "/v1/timeout",
                            headers={"Authorization": f"Bearer {_CONV}",
                                     "X-Curie-Turn-Epoch": epoch},
                        ) as response:
                            assert response.status == 200
                        frames = await _drain(turn)
                        assert isinstance(frames[-1], Final)
                        assert frames[-1].status is SessionStatus.CLASSIFIED_FAILURE
                        assert frames[-1].text == "run timed out"
                        assert all(frame.adoption_applied is True for frame in frames)
                        assert turn.adoption_acked
                assert rt.runner.status is SessionStatus.CLASSIFIED_FAILURE
                recovered = await _drain(await client.start_turn(
                    rt.base_url, _event("healthy subsequent turn"), _CONV,
                ))
                assert isinstance(recovered[-1], Final)
                assert recovered[-1].status is SessionStatus.DONE
                assert all(frame.adoption_applied is None for frame in recovered)
            finally:
                await client.close()

    asyncio.run(go())


def test_adopting_turn_is_acked_then_the_conversation_credential_gates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _real_runner() as rt:
            client = RunnerClient(total_timeout_s=30.0)
            try:
                turn = await client.start_adopting_turn(
                    rt.base_url,
                    _event(),
                    bootstrap_token=_BOOT,
                    conversation_token=_CONV,
                    session_id=_SESSION,
                    history_ref=_HISTORY,
                )
                # Before any frame is read the ack is unknown, never assumed.
                assert turn.adoption_ack_observed is None
                assert turn.adoption_acked is False
                frames = await _drain(turn)
                assert frames and isinstance(frames[-1], Final)
                assert turn.adoption_acked is True
                assert all(frame.adoption_applied is True for frame in frames)
                # The runner really bound the conversation named on the frame.
                assert rt.binder.loads == [(_SESSION, _HISTORY)]
                assert rt.runner.session_id == _SESSION
                assert rt.sessions[-1].queries == ["hello"]

                # The bootstrap is retired: refused as a plain wrong token.
                with pytest.raises(AdoptionRefused) as refused:
                    await client.start_adopting_turn(
                        rt.base_url,
                        _event(),
                        bootstrap_token=_BOOT,
                        conversation_token=_CONV_B,
                        session_id=_SESSION_B,
                        history_ref=_HISTORY,
                    )
                assert refused.value.status == 401
                # The conversation credential now gates the ordinary routes.
                status = await client.status(rt.base_url, token=_CONV)
                assert status["session_id"] == _SESSION
                follow = await client.start_turn(
                    rt.base_url,
                    _event("again").model_copy(update={"session_id": _SESSION}),
                    token=_CONV,
                )
                later = await _drain(follow)
                # A later turn is not an adoption and claims no ack.
                assert follow.adoption_acked is False
                assert all(frame.adoption_applied is None for frame in later)
                assert await client.adoption_applied(
                    rt.base_url, conversation_token=_CONV, session_id=_SESSION
                )
            finally:
                await client.close()

    asyncio.run(go())
    _assert_no_secret(*(record.getMessage() for record in caplog.records))


def test_adopting_event_carries_identity_and_credential_on_the_wire() -> None:
    """The existing Event is the transport: no header, no new route."""

    legacy = _LegacyRunner()

    async def go() -> None:
        server = TestServer(legacy.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            turn = await client.start_adopting_turn(
                base,
                _event(),
                bootstrap_token=_BOOT,
                conversation_token=_CONV,
                session_id=_SESSION,
                history_ref=_HISTORY,
            )
            await _drain(turn)
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())
    body = legacy.bodies[0]
    assert body["adoption_credential"] == _CONV
    assert body["session_id"] == _SESSION
    assert body["history_ref"] == _HISTORY
    assert body["kind"] == "event" and body["text"] == "hello"


# --- a 200 is never proof: legacy and malformed acks ----------------------------


def test_legacy_consumer_without_ack_is_not_adopted() -> None:
    legacy = _LegacyRunner()

    async def go() -> None:
        server = TestServer(legacy.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            turn = await client.start_adopting_turn(
                base,
                _event(),
                bootstrap_token=_BOOT,
                conversation_token=_CONV,
                session_id=_SESSION,
                history_ref=None,
            )
            frames = await _drain(turn)
            assert isinstance(frames[-1], Final)  # the turn itself succeeded
            assert turn.adoption_ack_observed is False
            assert turn.adoption_acked is False  # ...and that is not adoption
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_explicit_false_ack_is_not_adopted() -> None:
    declined = _LegacyRunner(ack_value=False)

    async def go() -> None:
        server = TestServer(declined.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            turn = await client.start_adopting_turn(
                base,
                _event(),
                bootstrap_token=_BOOT,
                conversation_token=_CONV,
                session_id=_SESSION,
                history_ref=None,
            )
            await _drain(turn)
            assert turn.adoption_acked is False
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


@pytest.mark.parametrize("ack_value", ["true", 1])
def test_non_boolean_ack_is_a_stream_error_not_adoption(ack_value: object) -> None:
    malformed = _LegacyRunner(ack_value=ack_value)

    async def go() -> None:
        server = TestServer(malformed.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            turn = await client.start_adopting_turn(
                base,
                _event(),
                bootstrap_token=_BOOT,
                conversation_token=_CONV,
                session_id=_SESSION,
                history_ref=None,
            )
            with pytest.raises(ValueError):
                await _drain(turn)
            assert turn.adoption_acked is False
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


# --- refusals never leak the credential -----------------------------------------


def test_refusals_carry_status_and_no_credential(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _real_runner() as rt:
            client = RunnerClient(total_timeout_s=30.0)
            try:
                # Wrong bootstrap.
                with pytest.raises(AdoptionRefused) as wrong:
                    await client.start_adopting_turn(
                        rt.base_url,
                        _event(),
                        bootstrap_token="not-the-pool-bootstrap",
                        conversation_token=_CONV,
                        session_id=_SESSION,
                        history_ref=None,
                    )
                assert wrong.value.status == 401
                _assert_no_secret(str(wrong.value))
                # Bootstrap presented as the new credential.
                with pytest.raises(AdoptionRefused) as same:
                    await client.start_adopting_turn(
                        rt.base_url,
                        _event(),
                        bootstrap_token=_BOOT,
                        conversation_token=_BOOT,
                        session_id=_SESSION,
                        history_ref=None,
                    )
                assert same.value.status == 400
                _assert_no_secret(str(same.value))
                # Nothing was bound by either refusal.
                assert rt.binder.loads == []
                assert rt.runner.session_id == "warm-unbound"
                assert not await client.adoption_applied(
                    rt.base_url, conversation_token=_CONV, session_id=_SESSION
                )
            finally:
                await client.close()
        async with _real_runner(token="per-claim-token-0123456789", bootstrap=None) as cold:
            client = RunnerClient(total_timeout_s=30.0)
            try:
                # A cold (per-claim) runner is never adoptable, whoever asks.
                with pytest.raises(AdoptionRefused) as not_adoptable:
                    await client.start_adopting_turn(
                        cold.base_url,
                        _event(),
                        bootstrap_token="per-claim-token-0123456789",
                        conversation_token=_CONV,
                        session_id=_SESSION,
                        history_ref=None,
                    )
                assert not_adoptable.value.status == 409
                assert cold.sessions[0].queries == []
            finally:
                await client.close()

    asyncio.run(go())
    _assert_no_secret(*(record.getMessage() for record in caplog.records))


# --- lost-response recovery through the runner's attestation --------------------


def test_recovery_probe_distinguishes_applied_from_still_adoptable() -> None:
    async def go() -> None:
        async with _real_runner() as rt:
            client = RunnerClient(total_timeout_s=30.0)
            try:
                # Not applied yet: the conversation credential is a wrong token
                # (401). That is "not confirmed on this pod", not proof the
                # bootstrap is adoptable; here the same pod really is unbound.
                assert (
                    await client.adoption_applied(
                        rt.base_url, conversation_token=_CONV, session_id=_SESSION
                    )
                    is False
                )
                # Apply, simulating the lost response by never reading the body.
                turn = await client.start_adopting_turn(
                    rt.base_url,
                    _event(),
                    bootstrap_token=_BOOT,
                    conversation_token=_CONV,
                    session_id=_SESSION,
                    history_ref=_HISTORY,
                )
                turn.close()
                # Let the runner finish the turn we abandoned.
                for _ in range(200):
                    if rt.runner.session_id == _SESSION and not rt.runner._turn_open:  # noqa: SLF001
                        break
                    await asyncio.sleep(0.01)
                # The attestation proves the binding without reopening the
                # bootstrap or sending a second adopting event.
                assert (
                    await client.adoption_applied(
                        rt.base_url, conversation_token=_CONV, session_id=_SESSION
                    )
                    is True
                )
                # A retry of the adopting request after recovery is refused, so a
                # duplicate first turn cannot be started under the bootstrap.
                with pytest.raises(AdoptionRefused) as again:
                    await client.start_adopting_turn(
                        rt.base_url,
                        _event(),
                        bootstrap_token=_BOOT,
                        conversation_token=_CONV,
                        session_id=_SESSION,
                        history_ref=_HISTORY,
                    )
                assert again.value.status == 401
                # The credential attesting a DIFFERENT conversation is a refusal,
                # never "applied": the worker must not route this thread there.
                with pytest.raises(RunnerError):
                    await client.adoption_applied(
                        rt.base_url, conversation_token=_CONV, session_id=_SESSION_B
                    )
            finally:
                await client.close()

    asyncio.run(go())


def test_non_refusal_failure_is_scrubbed_and_not_an_adoption_refusal() -> None:
    """A 5xx that echoes the body is neither a refusal nor a credential leak."""

    app = web.Application()

    async def broken(request: web.Request) -> web.Response:
        # A non-conforming server that echoes the request back on failure.
        return web.Response(status=503, text=await request.text())

    app.add_routes([web.post("/v1/event", broken)])

    async def go() -> None:
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            with pytest.raises(RunnerError) as failure:
                await client.start_adopting_turn(
                    base,
                    _event(),
                    bootstrap_token=_BOOT,
                    conversation_token=_CONV,
                    session_id=_SESSION,
                    history_ref=None,
                )
            assert not isinstance(failure.value, AdoptionRefused)
            assert "503" in str(failure.value)
            _assert_no_secret(str(failure.value))
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_adoption_authority_is_established_only_by_a_realizing_bootstrap_runner() -> None:
    """Root correction 6: authority first, through the status route, never by version."""

    async def go() -> None:
        client = RunnerClient(total_timeout_s=30.0)
        try:
            async with _real_runner() as warm:
                assert await client.adoption_authority(warm.base_url, bootstrap_token=_BOOT) is True
                # Establishing authority started no turn and bound nothing.
                assert warm.sessions[0].queries == [] and warm.runner.session_id == "warm-unbound"
                # The wrong pool credential establishes nothing (401 -> False).
                assert (
                    await client.adoption_authority(warm.base_url, bootstrap_token="other-pool")
                    is False
                )
                # After adoption the bootstrap is retired: authority is gone too.
                turn = await client.start_adopting_turn(
                    warm.base_url,
                    _event(),
                    bootstrap_token=_BOOT,
                    conversation_token=_CONV,
                    session_id=_SESSION,
                    history_ref=None,
                )
                await _drain(turn)
                assert (
                    await client.adoption_authority(warm.base_url, bootstrap_token=_BOOT) is False
                )
            async with _real_runner(token="per-claim-token-0123456789", bootstrap=None) as cold:
                # A cold per-claim runner (even presented with its own token) is not an adopter.
                assert (
                    await client.adoption_authority(
                        cold.base_url, bootstrap_token="per-claim-token-0123456789"
                    )
                    is False
                )
            async with _real_runner(token=None, bootstrap=None) as open_runner:
                # An OLD or open runner answers 200 on the probe: refused, so no
                # adopting event (and no model turn) is ever sent to it.
                assert (
                    await client.adoption_authority(open_runner.base_url, bootstrap_token=_BOOT)
                    is False
                )
                assert open_runner.sessions[0].queries == []
        finally:
            await client.close()

    asyncio.run(go())


def test_adoption_authority_rejects_a_forged_403_without_the_fixed_reason() -> None:
    app = web.Application()

    async def status(_: web.Request) -> web.Response:
        return web.json_response({"error": "forbidden"}, status=403)

    app.add_routes([web.get("/v1/status", status)])

    async def go() -> None:
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            assert await client.adoption_authority(base, bootstrap_token=_BOOT) is False
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_recovery_probe_refuses_a_legacy_attestation_without_session_id() -> None:
    """A runner that answers 200 without attesting a session cannot be trusted as bound."""

    app = web.Application()

    async def status(_: web.Request) -> web.Response:
        return web.json_response({"status": "idle-awaiting-input", "turn_active": False})

    app.add_routes([web.get("/v1/status", status)])

    async def go() -> None:
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            base = str(server.make_url("")).rstrip("/")
            with pytest.raises(RunnerError):
                await client.adoption_applied(base, conversation_token=_CONV, session_id=_SESSION)
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())
