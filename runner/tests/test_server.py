"""The aiohttp ACI channel: health, status, event stream, interrupt, steer."""

import json
from pathlib import Path

import anyio
from aci_protocol import SessionStatus, parse_ndjson
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.__main__ import build_runner
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_TURN_EPOCH_HEADER = "X-Curie-Turn-Epoch"


class _EpochControlledSession:
    """Two no-result turns whose SDK wait is released only by interrupt."""

    def __init__(self) -> None:
        self.entered = [anyio.Event(), anyio.Event()]
        self.release = [anyio.Event(), anyio.Event()]
        self.turn = -1
        self.interrupts = 0

    async def connect(self) -> None: ...

    async def query(self, _text: str) -> None:
        self.turn += 1

    async def receive_turn(self):
        turn = self.turn
        self.entered[turn].set()
        await self.release[turn].wait()
        if False:  # pragma: no cover - retain the async-generator shape
            yield None

    async def interrupt(self) -> None:
        self.interrupts += 1
        self.release[self.turn].set()

    async def close(self) -> None:
        for release in self.release:
            release.set()


def _runner() -> tuple[SessionRunner, FakeModelSession]:
    fake = FakeModelSession()
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    return runner, fake


def _boot_runner(
    tmp_path: Path, *, managed_workspace: bool
) -> tuple[SessionRunner, Path | None]:
    """Build the actual fake-model boot path with claim identity and workspace state."""

    plugin_dir = tmp_path / "bundle"
    manifest_dir = plugin_dir / ".claude-plugin"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    workspace_path: Path | None = None
    if managed_workspace:
        workspace_path = tmp_path / "workspace"
        (workspace_path / ".git").mkdir(parents=True)
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "session-acme-workspace",
            "CURIE_SANDBOX_ID": "sandbox-acme-workspace",
            "CURIE_BUDGET": (
                '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
            ),
        }
    )
    return (
        build_runner(
            config,
            fake_model=True,
            workspace_path=workspace_path,
        ),
        workspace_path,
    )


def test_healthz_status_and_event_round_trip() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            health = await client.get("/healthz")
            assert health.status == 200
            assert (await health.json())["ok"] is True

            status = await client.get("/status")
            body = await status.json()
            assert body["status"] == SessionStatus.IDLE_AWAITING_INPUT.value
            assert body["ready"] is True
            assert body["history_durable"] is True

            frame = {"kind": "event", "type": "message", "text": "hi", "user": "U", "ts": "1"}
            resp = await client.post("/v1/event", json=frame)
            assert resp.status == 200
            assert resp.headers["Content-Type"].startswith("application/x-ndjson")
            events = parse_ndjson(await resp.text())
            assert events[-1].type == "final"
            assert events[-1].status == SessionStatus.DONE

    anyio.run(go)


def test_event_header_uses_explicit_parent_and_missing_or_malformed_is_safe_root() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    trace_id = "1234567890abcdef1234567890abcdef"
    parent_span_id = "1234567890abcdef"
    traceparent = f"00-{trace_id}-{parent_span_id}-01"

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            inherited = await client.post(
                "/v1/event",
                json=_EVENT_FRAME,
                headers={"traceparent": traceparent},
            )
            assert inherited.status == 200
            missing = await client.post("/v1/event", json=_EVENT_FRAME)
            assert missing.status == 200
            malformed = await client.post(
                "/v1/event",
                json=_EVENT_FRAME,
                headers={"traceparent": "not-a-traceparent"},
            )
            assert malformed.status == 200

    anyio.run(go)

    roots = [span for span in exporter.get_finished_spans() if span.name == "agent.run"]
    assert len(roots) == 3
    inherited, missing, malformed = roots
    assert inherited.context is not None
    assert missing.context is not None
    assert malformed.context is not None
    assert inherited.context.trace_id == int(trace_id, 16)
    assert inherited.parent is not None
    assert inherited.parent.span_id == int(parent_span_id, 16)
    assert missing.parent is None
    assert malformed.parent is None
    assert len(
        {
            inherited.context.trace_id,
            missing.context.trace_id,
            malformed.context.trace_id,
        }
    ) == 3


def test_event_rejects_non_event_frame() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            resp = await client.post("/v1/event", json={"kind": "interrupt", "reason": "x"})
            assert resp.status == 400

    anyio.run(go)


def test_interrupt_endpoint_acks() -> None:
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            resp = await client.post("/v1/interrupt", json={"kind": "interrupt", "reason": "stop"})
            assert resp.status == 200
            assert (await resp.json())["ok"] is True
            assert fake.interrupts >= 1

    anyio.run(go)


def test_steer_takes_an_event_frame_and_conflicts_without_a_turn() -> None:
    runner, _ = _runner()
    steer_frame = {"kind": "event", "type": "message", "text": "do X", "user": "U", "ts": "2"}

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            # A steer is an ACI event frame; with no live turn it has nowhere to
            # land -> 409, so F1 falls back to opening a fresh /v1/event.
            resp = await client.post("/v1/steer", json=steer_frame)
            assert resp.status == 409
            # A non-event frame on the steer endpoint is a 400.
            bad = await client.post("/v1/steer", json={"kind": "interrupt", "reason": "x"})
            assert bad.status == 400

    anyio.run(go)


def test_reset_endpoint_starts_a_fresh_session() -> None:
    # The factory hands out a NEW fake each call, so a reset is observable: the
    # runner's live session object changes identity after /v1/reset (#550).
    sessions: list[FakeModelSession] = []

    def factory() -> FakeModelSession:
        fake = FakeModelSession()
        sessions.append(fake)
        return fake

    runner = SessionRunner(
        session_factory=factory,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )

    async def go() -> None:
        await runner.start()
        first = runner._session  # noqa: SLF001 - identity check is the assertion
        async with TestClient(TestServer(create_app(runner))) as client:
            resp = await client.post("/v1/reset")
            assert resp.status == 200
            assert (await resp.json())["ok"] is True
            # A fresh session replaced the original one; the old one was closed
            # and the new one connected. (Asserted inside the client context: the
            # app cleanup closes the live session on exit.)
            second = runner._session  # noqa: SLF001
            assert second is not first
            assert first.connected is False
            assert second is not None and second.connected is True
            assert runner.status == SessionStatus.IDLE_AWAITING_INPUT

    anyio.run(go)


def test_reset_is_refused_while_a_turn_is_active() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            # Simulate a live turn: reset must 409 rather than tear the session
            # down under an open /v1/event stream.
            runner._turn_open = True  # noqa: SLF001
            resp = await client.post("/v1/reset")
            assert resp.status == 409

    anyio.run(go)


def test_reset_requires_auth_when_a_token_is_configured() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            unauth = await client.post("/v1/reset")
            assert unauth.status == 401
            ok = await client.post("/v1/reset", headers=_AUTH)
            assert ok.status == 200

    anyio.run(go)


def test_control_status_requires_auth_while_probe_status_remains_open() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            probe = await client.get("/status")
            assert probe.status == 200
            unauthenticated = await client.get("/v1/status")
            assert unauthenticated.status == 401
            authenticated = await client.get("/v1/status", headers=_AUTH)
            assert authenticated.status == 200
            assert (await authenticated.json())["history_durable"] is True

    anyio.run(go)


def test_authenticated_status_attests_actual_mounted_workspace_boot(tmp_path: Path) -> None:
    runner, workspace_path = _boot_runner(tmp_path, managed_workspace=True)
    assert workspace_path is not None

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            # Boot identity and filesystem placement are control-plane facts. The
            # unauthenticated probe remains useful without disclosing either.
            probe = await client.get("/status")
            probe_body = await probe.json()
            assert probe.status == 200
            assert "session_id" not in probe_body
            assert "sandbox_id" not in probe_body
            assert "managed_workspace" not in probe_body
            assert "cwd" not in probe_body

            unauthenticated = await client.get("/v1/status")
            assert unauthenticated.status == 401
            authenticated = await client.get("/v1/status", headers=_AUTH)
            body = await authenticated.json()
            assert authenticated.status == 200
            # Preserve the pre-attestation status DTO while adding enough
            # runner-observed state for a worker to reject a wrong replacement.
            assert body["status"] == SessionStatus.IDLE_AWAITING_INPUT.value
            assert body["ready"] is True
            assert body["turn_active"] is False
            assert body["history_durable"] is True
            assert {
                "session_id": "session-acme-workspace",
                "sandbox_id": "sandbox-acme-workspace",
                "managed_workspace": True,
                "cwd": str(workspace_path),
            }.items() <= body.items()

    anyio.run(go)


def test_authenticated_status_attests_unmounted_boot_without_cwd(tmp_path: Path) -> None:
    runner, workspace_path = _boot_runner(tmp_path, managed_workspace=False)
    assert workspace_path is None

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            authenticated = await client.get("/v1/status", headers=_AUTH)
            body = await authenticated.json()
            assert authenticated.status == 200
            assert body["status"] == SessionStatus.IDLE_AWAITING_INPUT.value
            assert body["ready"] is True
            assert body["turn_active"] is False
            assert body["history_durable"] is True
            assert {
                "session_id": "session-acme-workspace",
                "sandbox_id": "sandbox-acme-workspace",
                "managed_workspace": False,
                "cwd": None,
            }.items() <= body.items()

    anyio.run(go)


_TOKEN = "test-token-xyz"
_AUTH = {"Authorization": f"Bearer {_TOKEN}"}
_EVENT_FRAME = {"kind": "event", "type": "message", "text": "hi", "user": "U", "ts": "1"}


def test_event_without_auth_header_is_401() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.post("/v1/event", json=_EVENT_FRAME)
            assert resp.status == 401
            assert "error" in await resp.json()

    anyio.run(go)


def test_event_with_wrong_token_is_401() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.post(
                "/v1/event", json=_EVENT_FRAME, headers={"Authorization": "Bearer wrong"}
            )
            assert resp.status == 401

    anyio.run(go)


def test_event_with_malformed_header_is_401() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            # The raw token with no Bearer scheme is not a valid credential.
            resp = await client.post(
                "/v1/event", json=_EVENT_FRAME, headers={"Authorization": _TOKEN}
            )
            assert resp.status == 401

    anyio.run(go)


def test_event_with_correct_token_proceeds() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.post("/v1/event", json=_EVENT_FRAME, headers=_AUTH)
            assert resp.status == 200
            events = parse_ndjson(await resp.text())
            assert events[-1].type == "final"
            assert events[-1].status == SessionStatus.DONE

    anyio.run(go)


def test_steer_with_correct_token_and_no_turn_is_409_not_401() -> None:
    runner, _ = _runner()
    steer_frame = {"kind": "event", "type": "message", "text": "do X", "user": "U", "ts": "2"}

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            # Auth must not disturb the steer contract: a valid token with no live
            # turn still returns 409, not 401.
            resp = await client.post("/v1/steer", json=steer_frame, headers=_AUTH)
            assert resp.status == 409

    anyio.run(go)


def test_interrupt_requires_auth_and_proceeds_with_token() -> None:
    runner, fake = _runner()
    interrupt_frame = {"kind": "interrupt", "reason": "stop"}

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            unauth = await client.post("/v1/interrupt", json=interrupt_frame)
            assert unauth.status == 401

            resp = await client.post("/v1/interrupt", json=interrupt_frame, headers=_AUTH)
            assert resp.status == 200
            assert (await resp.json())["ok"] is True
            assert fake.interrupts >= 1

    anyio.run(go)


def test_timeout_route_auth_epoch_validation_and_turn_isolation() -> None:
    """Only the authenticated current epoch may label and stop its own turn."""

    async def go() -> None:
        session = _EpochControlledSession()
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            first = await client.post("/v1/event", json=_EVENT_FRAME, headers=_AUTH)
            first_epoch = first.headers[_TURN_EPOCH_HEADER]
            assert 32 <= len(first_epoch) <= 256
            assert all(
                character.isalnum() or character in "-_"
                for character in first_epoch
            )
            await session.entered[0].wait()

            unauthenticated = await client.post(
                "/v1/timeout", headers={_TURN_EPOCH_HEADER: first_epoch}
            )
            assert unauthenticated.status == 401
            wrong_bearer = await client.post(
                "/v1/timeout",
                headers={
                    "Authorization": "Bearer wrong-token-PLACEHOLDER",
                    _TURN_EPOCH_HEADER: first_epoch,
                },
            )
            assert wrong_bearer.status == 401
            assert session.interrupts == 0

            missing = await client.post("/v1/timeout", headers=_AUTH)
            malformed = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: "not*an*epoch"},
            )
            oversized = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: "A" * 1025},
            )
            assert missing.status == 400
            assert malformed.status == 400
            assert oversized.status == 400

            guessed_epoch = (
                ("A" if first_epoch[0] != "A" else "B") + first_epoch[1:]
            )
            spoofed = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: guessed_epoch},
            )
            assert spoofed.status == 409
            assert session.interrupts == 0

            accepted = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: first_epoch},
            )
            assert accepted.status == 200
            assert (await accepted.json())["ok"] is True
            assert session.interrupts == 1
            first_body = await first.text()
            assert first_epoch not in first_body

            replayed = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: first_epoch},
            )
            assert replayed.status == 409
            assert session.interrupts == 1

            second = await client.post("/v1/event", json=_EVENT_FRAME, headers=_AUTH)
            second_epoch = second.headers[_TURN_EPOCH_HEADER]
            assert second_epoch != first_epoch
            await session.entered[1].wait()

            stale = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: first_epoch},
            )
            spoofed_retry = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: guessed_epoch},
            )
            assert stale.status == 409
            assert spoofed_retry.status == 409
            assert session.interrupts == 1

            accepted_retry = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: second_epoch},
            )
            assert accepted_retry.status == 200
            await second.text()
            assert session.interrupts == 2

            delayed_replay = await client.post(
                "/v1/timeout",
                headers={**_AUTH, _TURN_EPOCH_HEADER: second_epoch},
            )
            assert delayed_replay.status == 409
            assert session.interrupts == 2

    anyio.run(go)


def test_steer_without_auth_header_is_401() -> None:
    runner, _ = _runner()
    steer_frame = {"kind": "event", "type": "message", "text": "do X", "user": "U", "ts": "2"}

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.post("/v1/steer", json=steer_frame)
            assert resp.status == 401
            assert "error" in await resp.json()

    anyio.run(go)


def test_empty_token_passes_through() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        # An empty token is a falsy token: create_app must not gate, so a
        # header-less POST proceeds rather than 401-ing on an unusable token.
        async with TestClient(TestServer(create_app(runner, token=""))) as client:
            resp = await client.post("/v1/event", json=_EVENT_FRAME)
            assert resp.status == 200

    anyio.run(go)


def test_healthz_never_gated() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.get("/healthz")
            assert resp.status == 200

    anyio.run(go)


def test_status_never_gated() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            resp = await client.get("/status")
            assert resp.status == 200

    anyio.run(go)


def test_no_token_configured_passes_through() -> None:
    runner, _ = _runner()

    async def go() -> None:
        await runner.start()
        # An app built with token=None does not gate: a header-less POST proceeds.
        async with TestClient(TestServer(create_app(runner, token=None))) as client:
            resp = await client.post("/v1/event", json=_EVENT_FRAME)
            assert resp.status == 200

    anyio.run(go)
