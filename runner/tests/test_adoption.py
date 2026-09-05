"""Warm-pool adoption over the authenticated ACI Event (ADR-0116 d2, ADR-0122).

A bootstrap-mode runner accepts its pool credential for exactly one thing: an
adopting ``POST /v1/event`` carrying ``adoption_credential`` and ``session_id``.
That call binds the conversation (history, session, attestation) BEFORE the
credential swaps, acks with ``adoption_applied: true`` on every frame of that
turn, and retires the bootstrap. Everything else under the bootstrap refuses,
a failed or malformed adoption is inert, concurrent adoptions have exactly one
winner, and the per-claim (cold) and open (CLI/fake) modes are unchanged.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

import anyio
import pytest
from aci_protocol import SessionStatus, parse_ndjson
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.__main__ import adoptable_history_ref
from curie_runner.adoption import AdoptionRefused, CredentialAuthority, CredentialMode, Principal
from curie_runner.fake import FakeModelSession
from curie_runner.history import (
    ConversationMessage,
    ConversationReplay,
    HistoryError,
    NullTranscriptStore,
)
from curie_runner.server import AUTHORITY, bind_status_attestation
from curie_runner.session import ConversationBindingError, SessionRunner

_BOOT = "pool-bootstrap-credential-0123456789"
_CONV = "conversation-credential-A-0123456789"
_CONV_B = "conversation-credential-B-0123456789"
_SESSION = "agent-acme-thread-C0EXAMPLE1-1700000000.000100"
_SESSION_B = "agent-acme-thread-C0EXAMPLE1-1700000000.000200"
_HISTORY = "http://api.example.com/agents/acme/state/transcript/thread-1"
_SECRETS = (_BOOT, _CONV, _CONV_B)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _frame(**overrides: Any) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "kind": "event",
        "type": "message",
        "text": "hello",
        "user": "U1",
        "ts": "1700000000.000100",
    }
    frame.update(overrides)
    return frame


def _adopting(
    credential: str = _CONV, session_id: str | None = _SESSION, **extra: Any
) -> dict[str, Any]:
    frame = _frame(adoption_credential=credential, history_ref=_HISTORY, **extra)
    if session_id is not None:
        frame["session_id"] = session_id
    return frame


class RecordingBinder:
    """A ConversationBinder double: records calls, optionally fails or dawdles."""

    def __init__(
        self,
        *,
        fail: bool = False,
        delay_s: float = 0.0,
        replay: ConversationReplay | None = None,
    ) -> None:
        self.loads: list[tuple[str, str | None]] = []
        self.rebinds: list[tuple[str, ConversationReplay]] = []
        self.fail = fail
        self.delay_s = delay_s
        self.replay = replay or ConversationReplay()
        self.store: Any = NullTranscriptStore()
        self.summary: Any = None

    async def load(
        self, session_id: str, history_ref: str | None
    ) -> tuple[Any, ConversationReplay, Any]:
        self.loads.append((session_id, history_ref))
        if self.delay_s:
            await anyio.sleep(self.delay_s)
        if self.fail:
            raise HistoryError("configured structured history could not be loaded")
        return self.store, self.replay, self.summary

    def rebind(self, session_id: str, replay: ConversationReplay) -> None:
        self.rebinds.append((session_id, replay))


class _RefusingSession(FakeModelSession):
    """A replacement session whose connect fails after the object exists."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def connect(self) -> None:
        raise RuntimeError("model session refused to connect")

    async def close(self) -> None:
        self.closed = True
        await super().close()


def _runner(
    binder: RecordingBinder | None,
    *,
    factory_fail_after: int | None = None,
    connect_fail_after: int | None = None,
    boot_replay: ConversationReplay | None = None,
) -> tuple[SessionRunner, list[FakeModelSession]]:
    sessions: list[FakeModelSession] = []

    def factory() -> FakeModelSession:
        if factory_fail_after is not None and len(sessions) >= factory_fail_after:
            raise RuntimeError("model session refused to connect")
        fake: FakeModelSession = (
            _RefusingSession()
            if connect_fail_after is not None and len(sessions) >= connect_fail_after
            else FakeModelSession()
        )
        sessions.append(fake)
        return fake

    runner = SessionRunner(
        session_factory=factory,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:warm-unbound",
        session_id="warm-unbound",
        conversation_binder=binder,
        boot_replay=boot_replay,
    )
    bind_status_attestation(runner, session_id="warm-unbound", sandbox_id="sbx-pool-1", cwd=None)
    return runner, sessions


def _events(text: str) -> list[Any]:
    return parse_ndjson(text)


def _assert_no_secret(*payloads: str) -> None:
    for payload in payloads:
        for secret in _SECRETS:
            assert secret not in payload


# --- the authority on its own ------------------------------------------------


def test_authority_modes_and_principals() -> None:
    open_mode = CredentialAuthority()
    assert open_mode.mode is CredentialMode.OPEN
    assert open_mode.gated is False
    assert open_mode.authenticate("anything") is Principal.NONE

    per_claim = CredentialAuthority(token="per-claim-token", bootstrap_token=_BOOT)
    assert per_claim.mode is CredentialMode.PER_CLAIM
    assert per_claim.adoptable is False
    assert per_claim.authenticate("per-claim-token") is Principal.CONVERSATION
    # A per-claim pod was bound at boot; the bootstrap is never accepted there.
    assert per_claim.authenticate(_BOOT) is Principal.NONE

    bootstrap = CredentialAuthority(bootstrap_token=_BOOT)
    assert bootstrap.mode is CredentialMode.BOOTSTRAP
    assert bootstrap.adoptable is True
    assert bootstrap.authenticate(_BOOT) is Principal.BOOTSTRAP
    assert bootstrap.authenticate(_CONV) is Principal.NONE
    assert bootstrap.authenticate(None) is Principal.NONE
    # Empty strings are "not configured", never an enforce-on-empty state.
    assert CredentialAuthority(token="", bootstrap_token="").gated is False


# --- bootstrap mode: refusals before adoption --------------------------------


def test_bootstrap_without_credential_is_refused_and_starts_no_turn() -> None:
    binder = RecordingBinder()
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        app = create_app(runner, bootstrap_token=_BOOT)
        async with TestClient(TestServer(app)) as client:
            unauth = await client.post("/v1/event", json=_frame())
            assert unauth.status == 401
            wrong = await client.post("/v1/event", json=_frame(), headers=_bearer(_CONV))
            assert wrong.status == 401
            plain = await client.post("/v1/event", json=_frame(), headers=_bearer(_BOOT))
            assert plain.status == 403
            body = await plain.json()
            assert body["error"] == "bootstrap credential permits adoption only"
            # Even a frame that names the conversation is refused without the credential.
            named = await client.post(
                "/v1/event", json=_frame(session_id=_SESSION), headers=_bearer(_BOOT)
            )
            assert named.status == 403
        assert sessions[0].queries == []
        assert binder.loads == []
        assert runner.session_id == "warm-unbound"

    anyio.run(go)


def test_bootstrap_is_refused_on_every_other_gated_route() -> None:
    runner, _ = _runner(RecordingBinder())

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            for method, path, payload in (
                ("get", "/v1/status", None),
                ("post", "/v1/steer", _frame()),
                ("post", "/v1/interrupt", {"kind": "interrupt", "reason": "x"}),
                ("post", "/v1/reset", None),
                ("post", "/v1/snapshot", None),
            ):
                resp = await client.request(
                    method.upper(), path, json=payload, headers=_bearer(_BOOT)
                )
                assert resp.status == 403, path
                assert (await resp.json())["error"] == "bootstrap credential permits adoption only"
            # The probe routes stay open for the chart's probes.
            assert (await client.get("/healthz")).status == 200
            assert (await client.get("/status")).status == 200

    anyio.run(go)


# --- bootstrap mode: adoption --------------------------------------------------


def test_adoption_binds_acks_and_retires_the_bootstrap(caplog: pytest.LogCaptureFixture) -> None:
    replay = ConversationReplay(
        messages=(ConversationMessage(role="user", content="earlier question"),),
        source_turns=1,
    )
    binder = RecordingBinder(replay=replay)
    runner, sessions = _runner(binder)
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        await runner.start()
        boot_session = sessions[0]
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            text = await resp.text()
            assert resp.status == 200, text
            events = _events(text)
            assert events[-1].type == "final"
            assert events[-1].status == SessionStatus.DONE
            # Every frame of the adopting turn carries the ack, the final included.
            assert events and all(event.adoption_applied is True for event in events)

            # The conversation was actually bound, not merely accepted:
            assert binder.loads == [(_SESSION, _HISTORY)]
            assert binder.rebinds == [(_SESSION, replay)]
            assert runner.session_id == _SESSION
            # ...on a replacement model session; the boot session is closed and
            # the turn ran against the bound one.
            assert len(sessions) == 2
            assert boot_session.connected is False
            assert boot_session.queries == []
            assert sessions[1].queries == ["hello"]

            # The bootstrap is retired: it is now just a wrong token everywhere.
            stale = await client.post("/v1/event", json=_frame(), headers=_bearer(_BOOT))
            assert stale.status == 401
            stale_status = await client.get("/v1/status", headers=_bearer(_BOOT))
            assert stale_status.status == 401
            readopt = await client.post(
                "/v1/event", json=_adopting(_CONV_B), headers=_bearer(_BOOT)
            )
            assert readopt.status == 401

            # The conversation credential gates every control route and the
            # attestation reports the ADOPTED conversation.
            status = await client.get("/v1/status", headers=_bearer(_CONV))
            assert status.status == 200
            attested = await status.json()
            assert attested["session_id"] == _SESSION
            assert attested["sandbox_id"] == "sbx-pool-1"
            follow_up = await client.post(
                "/v1/event", json=_frame(text="again", session_id=_SESSION), headers=_bearer(_CONV)
            )
            follow_text = await follow_up.text()
            assert follow_up.status == 200
            # A later turn is not an adoption: no ack is claimed for it.
            assert all(event.adoption_applied is None for event in _events(follow_text))
            reset = await client.post("/v1/reset", headers=_bearer(_CONV))
            assert reset.status == 200
            # A reset after adoption still serves the bound conversation.
            assert runner.session_id == _SESSION
            _assert_no_secret(text, follow_text, await status.text())
        _assert_no_secret(*(record.getMessage() for record in caplog.records))

    anyio.run(go)


def test_adoption_credential_is_required_on_every_route_after_binding() -> None:
    runner, _ = _runner(RecordingBinder())

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            assert (
                await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            ).status == 200
            for method, path, payload in (
                ("get", "/v1/status", None),
                ("post", "/v1/steer", _frame()),
                ("post", "/v1/interrupt", {"kind": "interrupt", "reason": "x"}),
                ("post", "/v1/reset", None),
                ("post", "/v1/snapshot", None),
                ("post", "/v1/event", _frame()),
            ):
                missing = await client.request(method.upper(), path, json=payload)
                assert missing.status == 401, path
                wrong = await client.request(
                    method.upper(), path, json=payload, headers=_bearer(_CONV_B)
                )
                assert wrong.status == 401, path
            # Positive control: the bound credential reaches the route logic.
            steer = await client.post("/v1/steer", json=_frame(), headers=_bearer(_CONV))
            assert steer.status == 409  # no live turn, the ordinary finish-race answer
            snapshot = await client.post("/v1/snapshot", headers=_bearer(_CONV))
            assert snapshot.status == 409  # no managed workspace on this fake runner

    anyio.run(go)


def test_cross_conversation_and_re_adoption_are_refused_after_binding() -> None:
    binder = RecordingBinder()
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            assert (
                await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            ).status == 200
            queries_after_adoption = list(sessions[-1].queries)
            # The bound credential presented with another conversation's identity.
            other = await client.post(
                "/v1/event", json=_frame(session_id=_SESSION_B), headers=_bearer(_CONV)
            )
            assert other.status == 409
            assert (await other.json())["error"] == (
                "event names a conversation this runner is not bound to"
            )
            # The bound credential trying to adopt again (same or other session).
            again = await client.post(
                "/v1/event", json=_adopting(_CONV_B, _SESSION_B), headers=_bearer(_CONV)
            )
            assert again.status == 409
            assert (await again.json())["error"] == "runner is already bound to a conversation"
            same = await client.post(
                "/v1/event", json=_adopting(_CONV, _SESSION), headers=_bearer(_CONV)
            )
            assert same.status == 409
            # None of those reached the model, and the binding did not move.
            assert sessions[-1].queries == queries_after_adoption
            assert binder.loads == [(_SESSION, _HISTORY)]
            assert runner.session_id == _SESSION
            # The bound conversation still works with its own credential.
            ok = await client.post(
                "/v1/event", json=_frame(session_id=_SESSION), headers=_bearer(_CONV)
            )
            assert ok.status == 200

    anyio.run(go)


@pytest.mark.parametrize(
    ("frame", "status", "error"),
    [
        (_adopting(session_id=None), 400, "adoption requires a session_id"),
        (_adopting(session_id=""), 400, "adoption requires a session_id"),
        (_adopting(credential=_BOOT), 400, "adoption credential must differ from the bootstrap"),
        (_adopting(credential=""), 400, None),
        (_adopting(credential="   "), 400, None),
        (_adopting(credential="x" * 4097), 400, None),
        ({**_adopting(), "adoption_credential": 12345}, 400, None),
        ({**_adopting(), "adoption_credential": {"token": _CONV}}, 400, None),
    ],
)
def test_malformed_or_partial_adoption_is_inert(
    frame: dict[str, Any], status: int, error: str | None
) -> None:
    binder = RecordingBinder()
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=frame, headers=_bearer(_BOOT))
            body = await resp.text()
            assert resp.status == status, body
            if error is not None:
                assert (await resp.json())["error"] == error
            _assert_no_secret(body)
            # Nothing was applied: no history load, no session swap, no model
            # call, no credential change, and the boot identity still stands.
            assert binder.loads == []
            assert binder.rebinds == []
            assert len(sessions) == 1
            assert sessions[0].queries == []
            assert runner.session_id == "warm-unbound"
            assert (
                await client.post("/v1/event", json=_frame(), headers=_bearer(_CONV))
            ).status == 401
            # The bootstrap remains adoptable afterwards: a well-formed retry wins.
            retry = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert retry.status == 200, await retry.text()
            assert runner.session_id == _SESSION

    anyio.run(go)


def test_failed_history_load_leaves_runner_unbound_and_bootstrap_adoptable() -> None:
    binder = RecordingBinder(fail=True)
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert resp.status == 503
            body = await resp.text()
            assert (await resp.json())["error"] == "conversation could not be bound"
            _assert_no_secret(body)
            assert binder.loads == [(_SESSION, _HISTORY)]
            assert binder.rebinds == []
            assert len(sessions) == 1 and sessions[0].queries == []
            assert runner.session_id == "warm-unbound"
            # The presented credential was NOT installed...
            assert (await client.get("/v1/status", headers=_bearer(_CONV))).status == 401
            # ...and the bootstrap still adopts once the store recovers.
            binder.fail = False
            retry = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert retry.status == 200, await retry.text()
            assert runner.session_id == _SESSION
            assert (await client.get("/v1/status", headers=_bearer(_CONV))).status == 200

    anyio.run(go)


def test_failed_session_connect_restores_boot_factory_and_stays_unbound() -> None:
    binder = RecordingBinder()
    # The boot session is the first factory call; the adoption's replacement
    # session (second call) refuses to connect.
    runner, sessions = _runner(binder, factory_fail_after=1)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert resp.status == 503
            # rebind was applied then rolled back to the boot identity.
            assert [session_id for session_id, _ in binder.rebinds] == [_SESSION, "warm-unbound"]
            assert runner.session_id == "warm-unbound"
            assert sessions[0].connected is True
            assert (await client.get("/v1/status", headers=_bearer(_CONV))).status == 401
            # Still adoptable.
            assert (
                await client.post("/v1/event", json=_frame(), headers=_bearer(_BOOT))
            ).status == 403

    anyio.run(go)


def test_failed_connect_restores_the_real_boot_replay_and_closes_the_replacement() -> None:
    boot = ConversationReplay(
        messages=(ConversationMessage(role="user", content="boot history"),), source_turns=1
    )
    binder = RecordingBinder()
    # The boot session is the first factory call; the replacement (second)
    # exists but refuses to connect.
    runner, sessions = _runner(binder, connect_fail_after=1, boot_replay=boot)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert resp.status == 503
            # Rolled back to the ACTUAL boot replay, not an empty one.
            assert binder.rebinds[-1] == ("warm-unbound", boot)
            refused = sessions[1]
            assert isinstance(refused, _RefusingSession) and refused.closed is True
            assert sessions[0].connected is True
            assert runner.session_id == "warm-unbound"
            assert (await client.get("/v1/status", headers=_bearer(_CONV))).status == 401

    anyio.run(go)


def test_compaction_summary_is_appended_only_after_the_binding_is_applied() -> None:
    class RecordingStore(NullTranscriptStore):
        def __init__(self) -> None:
            self.appended: list[Any] = []

        async def append(self, record: Any) -> None:
            self.appended.append(record)

    failing = RecordingBinder()
    failing.store, failing.summary = RecordingStore(), object()
    runner, _ = _runner(failing, connect_fail_after=1)

    async def go_failed() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            assert (
                await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            ).status == 503
        # A refused adoption wrote nothing durable.
        assert failing.store.appended == []

    anyio.run(go_failed)

    ok = RecordingBinder()
    marker = object()
    ok.store, ok.summary = RecordingStore(), marker
    runner_ok, _ = _runner(ok)

    async def go_ok() -> None:
        await runner_ok.start()
        async with TestClient(TestServer(create_app(runner_ok, bootstrap_token=_BOOT))) as client:
            assert (
                await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            ).status == 200
        # The summary is the first durable write, ahead of the adopting turn's own record.
        assert ok.store.appended[0] is marker
        assert len(ok.store.appended) == 2

    anyio.run(go_ok)


def test_cancelled_adoption_completes_atomically_instead_of_splitting() -> None:
    """A request cancelled mid-bind ends bound-and-swapped, never half-adopted."""

    binder = RecordingBinder(delay_s=0.1)
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        app = create_app(runner, bootstrap_token=_BOOT)
        authority = app[AUTHORITY]
        adopting = asyncio.create_task(
            authority.adopt(_CONV, _SESSION, _HISTORY, bind=runner.bind_conversation)
        )
        await asyncio.sleep(0.02)  # inside the binder's history load
        adopting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await adopting
        # The shielded transition still ran to its consistent end.
        for _ in range(100):
            if authority.binding is not None:
                break
            await asyncio.sleep(0.01)
        assert authority.binding is not None
        assert authority.binding.session_id == _SESSION
        assert runner.session_id == _SESSION
        assert authority.authenticate(_CONV) is Principal.CONVERSATION
        assert authority.authenticate(_BOOT) is Principal.NONE
        assert len(sessions) == 2 and sessions[0].connected is False
        async with TestClient(TestServer(app)) as client:
            assert (await client.get("/v1/status", headers=_bearer(_CONV))).status == 200
            assert (await client.get("/v1/status", headers=_bearer(_BOOT))).status == 401
            again = await client.post(
                "/v1/event", json=_adopting(_CONV_B, _SESSION_B), headers=_bearer(_BOOT)
            )
            assert again.status == 401

    asyncio.run(go())


def test_bind_conversation_rolls_back_on_cancellation_during_connect() -> None:
    """Below the authority, a cancelled connect leaves the boot factory intact."""

    class HangingSession(FakeModelSession):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        async def connect(self) -> None:
            await asyncio.sleep(10)

        async def close(self) -> None:
            self.closed = True

    hanging: list[HangingSession] = []
    binder = RecordingBinder()
    sessions: list[FakeModelSession] = []

    def factory() -> FakeModelSession:
        if sessions:
            session = HangingSession()
            hanging.append(session)
            sessions.append(session)
            return session
        boot = FakeModelSession()
        sessions.append(boot)
        return boot

    runner = SessionRunner(
        session_factory=factory,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:warm-unbound",
        session_id="warm-unbound",
        conversation_binder=binder,
    )

    async def go() -> None:
        await runner.start()
        task = asyncio.create_task(runner.bind_conversation(_SESSION, _HISTORY))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [session_id for session_id, _ in binder.rebinds] == [_SESSION, "warm-unbound"]
        assert hanging and hanging[0].closed is True
        assert runner.session_id == "warm-unbound"
        assert sessions[0].connected is True

    asyncio.run(go())


def test_adoptable_history_ref_is_bound_to_the_pods_state_authority() -> None:
    base = "http://api.example.com/agents/acme/state"
    env = {"CURIE_STATE_URL": base}
    assert adoptable_history_ref(None, env) is None
    assert adoptable_history_ref("", env) is None
    assert (
        adoptable_history_ref(f"{base}/transcript/thread-1", env) == f"{base}/transcript/thread-1"
    )
    # The worker quotes the thread key with safe="" (binding.py), so a Slack
    # key's ":" and a nested "/" arrive percent-encoded and must pass AS SENT.
    for key in ("C0EXAMPLE1:1700000000.000100", "slack/C0EXAMPLE1/1700000000.000100"):
        quoted = f"{base}/transcript/{quote(key, safe='')}"
        assert adoptable_history_ref(quoted, env) == quoted
    assert adoptable_history_ref(f"{base}/transcript/thread-1", {"CURIE_STATE_URL": base + "/"})
    for outside in (
        "http://api.example.com/agents/other/state/transcript/thread-1",
        "http://evil.example.com/agents/acme/state/transcript/thread-1",
        "https://api.example.com/agents/acme/state/transcript/thread-1",
        "http://api.example.com:8080/agents/acme/state/transcript/thread-1",
        f"{base}x/transcript/thread-1",
        base,
        # Same host, prefix intact as a string, but dot segments escape the
        # namespace once normalized: refused on the PARSED form.
        f"{base}/../other/state/transcript/thread-1",
        f"{base}/transcript/%2e%2e/%2e%2e/other/state/transcript/thread-1",
        f"{base}/transcript/.%2E/../other/state/transcript/thread-1",
        f"{base}/transcript/./thread-1",
        f"{base}//transcript/thread-1",
        f"{base}/transcript/",
        # Userinfo, query and fragment are never part of a transcript key.
        "http://user:pw@api.example.com/agents/acme/state/transcript/thread-1",
        f"{base}/transcript/thread-1?x=1",
        f"{base}/transcript/thread-1#frag",
        "not a url",
    ):
        with pytest.raises(HistoryError):
            adoptable_history_ref(outside, env)
    # No state authority configured: no caller-chosen ref is admitted at all.
    with pytest.raises(HistoryError):
        adoptable_history_ref(f"{base}/transcript/thread-1", {})


def test_concurrent_adoptions_have_exactly_one_winner() -> None:
    binder = RecordingBinder(delay_s=0.05)
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            results: dict[str, tuple[int, str]] = {}

            async def attempt(name: str, credential: str, session_id: str) -> None:
                resp = await client.post(
                    "/v1/event", json=_adopting(credential, session_id), headers=_bearer(_BOOT)
                )
                results[name] = (resp.status, await resp.text())

            async with anyio.create_task_group() as tg:
                tg.start_soon(attempt, "a", _CONV, _SESSION)
                tg.start_soon(attempt, "b", _CONV_B, _SESSION_B)

            statuses = sorted(status for status, _ in results.values())
            assert statuses == [200, 409], results
            winner = next(name for name, (status, _) in results.items() if status == 200)
            loser = "b" if winner == "a" else "a"
            assert "already bound" in results[loser][1]
            won_credential, won_session = (
                (_CONV, _SESSION) if winner == "a" else (_CONV_B, _SESSION_B)
            )
            lost_credential = _CONV_B if winner == "a" else _CONV
            # Exactly one binding was applied, to the winner.
            assert binder.loads == [(won_session, _HISTORY)]
            assert runner.session_id == won_session
            assert all(event.adoption_applied is True for event in _events(results[winner][1]))
            # Only the winner's credential works; the loser's and the bootstrap do not.
            assert (await client.get("/v1/status", headers=_bearer(won_credential))).status == 200
            assert (await client.get("/v1/status", headers=_bearer(lost_credential))).status == 401
            assert (await client.get("/v1/status", headers=_bearer(_BOOT))).status == 401
            # Exactly one model turn ran.
            assert sum(len(session.queries) for session in sessions) == 1

    anyio.run(go)


# --- per-claim (cold) and open modes are unchanged -----------------------------


def test_per_claim_runner_refuses_adoption_and_keeps_serving_legacy_frames() -> None:
    binder = RecordingBinder()
    runner, sessions = _runner(binder)
    token = "per-claim-token-0123456789"

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=token))) as client:
            # A per-claim pod was bound at boot: it is not adoptable, whoever asks.
            adopt = await client.post("/v1/event", json=_adopting(), headers=_bearer(token))
            assert adopt.status == 409
            assert (await adopt.json())["error"] == "runner is not adoptable"
            assert binder.loads == [] and binder.rebinds == []
            assert sessions[0].queries == []
            # The bootstrap is never a credential here.
            assert (
                await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            ).status == 401
            # Legacy omission of session identity still serves the turn, without an ack.
            legacy = await client.post("/v1/event", json=_frame(), headers=_bearer(token))
            legacy_text = await legacy.text()
            assert legacy.status == 200
            assert all(event.adoption_applied is None for event in _events(legacy_text))
            # An explicit matching session id serves; a different one refuses.
            assert (
                await client.post(
                    "/v1/event", json=_frame(session_id="warm-unbound"), headers=_bearer(token)
                )
            ).status == 200
            mismatch = await client.post(
                "/v1/event", json=_frame(session_id=_SESSION_B), headers=_bearer(token)
            )
            assert mismatch.status == 409
            assert runner.session_id == "warm-unbound"

    anyio.run(go)


def test_open_runner_refuses_adoption_and_stays_pass_through() -> None:
    binder = RecordingBinder()
    runner, sessions = _runner(binder)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            adopt = await client.post("/v1/event", json=_adopting())
            assert adopt.status == 409
            assert binder.loads == [] and sessions[0].queries == []
            plain = await client.post("/v1/event", json=_frame())
            assert plain.status == 200
            assert all(event.adoption_applied is None for event in _events(await plain.text()))
            # Open mode never enforced identity; a session id on the frame is inert.
            assert (
                await client.post("/v1/event", json=_frame(session_id=_SESSION_B))
            ).status == 200

    anyio.run(go)


@pytest.mark.parametrize("mode", ["open", "per-claim", "bootstrap-bound"])
def test_steer_never_carries_a_credential(mode: str) -> None:
    runner, _ = _runner(RecordingBinder())
    token = "per-claim-token-0123456789"

    async def go() -> None:
        await runner.start()
        if mode == "open":
            app, headers = create_app(runner), {}
        elif mode == "per-claim":
            app, headers = create_app(runner, token=token), _bearer(token)
        else:
            app, headers = create_app(runner, bootstrap_token=_BOOT), _bearer(_CONV)
        async with TestClient(TestServer(app)) as client:
            if mode == "bootstrap-bound":
                assert (
                    await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
                ).status == 200
            # Hold a live turn open so the refusal is provably not the 409 finish race.
            runner._turn_open = True  # noqa: SLF001 - simulate a live turn
            resp = await client.post(
                "/v1/steer", json=_frame(adoption_credential=_CONV_B), headers=headers
            )
            assert resp.status == 400
            assert (await resp.json())["error"] == "steer must not carry an adoption credential"
            runner._turn_open = False  # noqa: SLF001

    anyio.run(go)


def test_authority_never_installs_an_empty_credential() -> None:
    """Defense in depth below the wire layer: an empty bearer must never authenticate."""

    async def go() -> None:
        authority = CredentialAuthority(bootstrap_token=_BOOT)
        calls: list[str] = []

        async def bind(session_id: str, history_ref: str | None) -> None:
            calls.append(session_id)

        for empty in ("", "   "):
            with pytest.raises(AdoptionRefused) as refused:
                await authority.adopt(empty, _SESSION, None, bind=bind)
            assert refused.value.status == 400
        assert calls == []
        assert authority.adoptable is True
        assert authority.authenticate("") is Principal.NONE

    anyio.run(go)


def test_runner_without_binder_cannot_bind() -> None:
    runner, _ = _runner(None)

    async def go() -> None:
        await runner.start()
        with pytest.raises(ConversationBindingError):
            await runner.bind_conversation(_SESSION, _HISTORY)
        async with TestClient(TestServer(create_app(runner, bootstrap_token=_BOOT))) as client:
            resp = await client.post("/v1/event", json=_adopting(), headers=_bearer(_BOOT))
            assert resp.status == 503
            assert runner.session_id == "warm-unbound"

    anyio.run(go)
