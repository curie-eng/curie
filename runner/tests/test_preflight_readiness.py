"""The runner listens while the declared verification preflight runs (#4777).

A declared check (installs plus tests, up to 600 s each) used to run inside
``build_runner`` before the aiohttp app existed, so ``GET /healthz`` had no
listener, the chart readiness probe failed, and the worker's 90 s claim timed
out. These tests drive the production boot (``__main__._serve``) in a thread,
serve the app it hands to ``web.run_app`` on a real socket in the test loop,
and talk to it over real HTTP. Only external boundaries are faked: the model
session (``boot.ClaudeAgentSession``), the progress API (an in-test aiohttp
server) and the declared check (a shell script held at a release barrier).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import pathlib
import signal
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import anyio
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, parse_ndjson_line
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import __main__ as boot
from curie_runner.progress import PROGRESS_TOKEN_ENV, PROGRESS_URL_ENV

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_MODEL = "example-provider/units-model"
_PROGRESS_TOKEN = "sbx.example-progress-token.signature"
_RUNNER_TOKEN = "runner-readiness-token"
_AUTH = {"Authorization": f"Bearer {_RUNNER_TOKEN}"}
_ANSWER = "The conversion is in place."
_BLOCKED_PREFIX = (
    "Could not complete: in-sandbox verification is unavailable before "
    "implementation, so no model round ran."
)
_HOLD_COMMAND = "hold-check"
# One 30 s watchdog bounds both sides of the barrier: the test gives up waiting
# for the check to enter it, and the barrier's own shell loop exits on its own
# after the same time (0.05 s polls), so a lost test never leaves a process
# behind for long.
_WATCHDOG_S = 30.0
_ENTER_WATCHDOG_S = _WATCHDOG_S
_HOLD_POLLS = int(_WATCHDOG_S / 0.05)
_HEALTHZ_WITHIN_S = 5.0


def _hold_record(exit_status: int) -> dict[str, Any]:
    return {
        "check": "hold",
        "command": _HOLD_COMMAND,
        "outcome": "passed" if exit_status == 0 else "failed",
        "exit_status": exit_status,
        "missing_binaries": [],
        "blocked_services": [],
    }


def _assistant(text: str) -> AssistantMessage:
    # Same shape as ``_assistant`` in runner/tests/test_false_completion.py.
    return AssistantMessage(content=[TextBlock(text=text)], model="fake-model", usage=None)


def _result(text: str) -> ResultMessage:
    # Same shape as ``_result`` in runner/tests/test_false_completion.py.
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage={"input_tokens": 5, "output_tokens": 3},
    )


@dataclass
class _Model:
    """What the model double saw: each construction, connect and query."""

    constructed: int = 0
    connected: int = 0
    queries: int = 0


@dataclass
class _Harness:
    tmp: Path
    entered: Path
    release: Path
    pidfile: Path
    client: aiohttp.ClientSession
    events: list[str] = field(default_factory=list)
    verification: list[tuple[dict[str, Any], int]] = field(default_factory=list)
    requests: list[str] = field(default_factory=list)
    model: _Model = field(default_factory=_Model)
    captured: list[web.Application] = field(default_factory=list)
    captured_event: threading.Event = field(default_factory=threading.Event)
    thread_errors: list[BaseException] = field(default_factory=list)
    thread: threading.Thread | None = None
    app_runner: web.AppRunner | None = None
    cleaned: bool = False
    base_url: str | None = None

    def _thread_failure(self) -> str:
        return "; ".join(f"{type(exc).__name__}: {exc}" for exc in self.thread_errors)

    async def serve_if_captured(self) -> bool:
        """Serve the app ``_serve`` handed to ``run_app``, once, on a real port."""

        if self.app_runner is None and self.captured_event.is_set():
            app_runner = web.AppRunner(self.captured[0])
            await app_runner.setup()
            self.app_runner = app_runner
            site = web.TCPSite(app_runner, "127.0.0.1", 0)
            await site.start()
            host, port = app_runner.addresses[0][:2]
            self.base_url = f"http://{host}:{port}"
        return self.app_runner is not None

    async def wait_served(self, timeout: float = 30.0) -> None:
        with anyio.move_on_after(timeout):
            while not await self.serve_if_captured():
                if self.thread_errors:
                    break
                await asyncio.sleep(0.05)
        assert self.app_runner is not None, (
            "the runner never handed an app to run_app "
            f"(boot error: {self._thread_failure() or 'none'})"
        )

    async def wait_entered(self) -> None:
        """Wait for the check to sit in its barrier, serving the app meanwhile."""

        with anyio.move_on_after(_ENTER_WATCHDOG_S):
            while not self.entered.exists():
                await self.serve_if_captured()
                if self.thread_errors and not self.entered.exists():
                    break
                await asyncio.sleep(0.05)
        assert self.entered.exists(), (
            f"the declared check never entered its barrier within {_ENTER_WATCHDOG_S:g} s "
            f"(boot error: {self._thread_failure() or 'none'})"
        )

    def pid(self) -> int:
        return int(self.pidfile.read_text(encoding="utf-8").strip())

    async def status_of(self, path: str) -> int | None:
        assert self.base_url is not None
        try:
            async with self.client.get(
                self.base_url + path, timeout=aiohttp.ClientTimeout(total=1)
            ) as resp:
                await resp.read()
                return resp.status
        except (aiohttp.ClientError, TimeoutError):
            return None

    async def runner_status(self) -> dict[str, Any]:
        assert self.base_url is not None
        async with self.client.get(self.base_url + "/status") as resp:
            assert resp.status == 200
            body: dict[str, Any] = await resp.json()
            return body

    async def wait_ready(self, timeout: float = 20.0) -> None:
        with anyio.move_on_after(timeout):
            while not (await self.runner_status())["ready"]:
                await asyncio.sleep(0.05)
        assert (await self.runner_status())["ready"] is True, "runner never became ready"

    async def turn(self, text: str) -> list[Any]:
        """POST one ACI event and parse the whole NDJSON response."""

        assert self.base_url is not None
        frame = {"kind": "event", "type": "message", "text": text, "user": "U-issue", "ts": "1.0"}
        async with self.client.post(self.base_url + "/v1/event", json=frame, headers=_AUTH) as resp:
            body = await resp.text()
            assert resp.status == 200, body
        return [parse_ndjson_line(line) for line in body.splitlines() if line.strip()]


def _workspace_path_shim(workspace: Path) -> Callable[..., Path]:
    """``_serve`` hardcodes ``Path("/workspace")``; point only that call at tmp."""

    def shim(*args: Any, **kwargs: Any) -> Path:
        if args == ("/workspace",) and not kwargs:
            return workspace
        return pathlib.Path(*args, **kwargs)

    return shim


def _declare(root: Path, checks: list[dict[str, Any]]) -> tuple[Path, Path]:
    """A plugin with ``verification/checks.json`` and a git checkout.

    Built like ``_declare`` in runner/tests/test_factory_preflight.py.
    """

    plugin = root / "plugin"
    manifest = plugin / ".claude-plugin"
    manifest.mkdir(parents=True)
    (manifest / "plugin.json").write_text(
        json.dumps({"name": "preflight-readiness", "version": "0.1.0"}), encoding="utf-8"
    )
    declared = plugin / "verification" / "checks.json"
    declared.parent.mkdir(parents=True)
    declared.write_text(json.dumps({"checks": checks}), encoding="utf-8")
    workspace = root / "workspace"
    (workspace / ".git").mkdir(parents=True)
    return plugin, workspace


def _barrier(h: _Harness, exit_status: int) -> str:
    """Shell lines that wait for the release file, then exit with ``exit_status``."""

    return (
        "i=0\n"
        f"while [ ! -e '{h.release}' ]; do\n"
        "  i=$((i + 1))\n"
        f'  if [ "$i" -ge {_HOLD_POLLS} ]; then exit 70; fi\n'
        "  sleep 0.05\n"
        "done\n"
        f"exit {exit_status}\n"
    )


def _hold_check(bindir: Path, h: _Harness, exit_status: int, body: str | None = None) -> None:
    """A check that records its pid, enters the barrier, and waits for release.

    ``body`` replaces everything after the pid line when a test needs a
    different check shape.
    """

    bindir.mkdir(exist_ok=True)
    script = bindir / _HOLD_COMMAND
    if body is None:
        body = f": > '{h.entered}'\n" + _barrier(h, exit_status)
    script.write_text(
        f"#!/bin/sh\necho $$ > '{h.pidfile}'\n{body}",
        encoding="utf-8",
    )
    script.chmod(0o755)


def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # No ambient credential, history, memory, telemetry or mode may reach boot.
    for name in list(os.environ):
        if name.startswith(("CURIE_", "OTEL_", "ANTHROPIC_", "CLAUDE_")):
            monkeypatch.delenv(name, raising=False)


@asynccontextmanager
async def _harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    exit_status: int = 0,
    checks: list[dict[str, Any]] | None = None,
    report_status: int = 201,
    fake_model: bool = False,
    script_body: Callable[[_Harness], str] | None = None,
) -> AsyncIterator[_Harness]:
    """Boot ``_serve`` in a thread against an in-test progress API; clean up all."""

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as client:
        h = _Harness(
            tmp=tmp_path,
            entered=tmp_path / "check-entered",
            release=tmp_path / "check-release",
            pidfile=tmp_path / "check-pid",
            client=client,
        )
        bindir = tmp_path / "bin"
        _hold_check(bindir, h, exit_status, None if script_body is None else script_body(h))
        plugin, workspace = _declare(
            tmp_path,
            checks
            if checks is not None
            else [{"id": "hold", "paths": ["**"], "command": [_HOLD_COMMAND]}],
        )

        @web.middleware
        async def seen(request: web.Request, handler: Any) -> web.StreamResponse:
            h.requests.append(request.path)
            response: web.StreamResponse = await handler(request)
            return response

        async def verification(request: web.Request) -> web.Response:
            h.verification.append((await request.json(), report_status))
            h.events.append(f"verification:{report_status}")
            return web.json_response({"recorded": report_status == 201}, status=report_status)

        async def usage(request: web.Request) -> web.Response:
            await request.read()
            return web.json_response({"recorded": True}, status=201)

        progress = web.Application(middlewares=[seen])
        progress.router.add_post("/v1/work-item-progress/readiness/verification", verification)
        progress.router.add_post("/v1/work-item-progress/readiness/usage", usage)

        class ModelDouble:
            """Records each construction, connect and query; answers one text turn."""

            def __init__(self, options: Any, *_args: Any, **_kwargs: Any) -> None:
                self.options = options
                h.model.constructed += 1
                h.events.append("model:constructed")

            async def connect(self) -> None:
                h.model.connected += 1
                h.events.append("model:connected")

            async def query(self, _text: str) -> None:
                h.model.queries += 1
                h.events.append("model:query")

            async def receive_turn(self) -> AsyncIterator[Any]:
                yield _assistant(_ANSWER)
                yield _result(_ANSWER)

            async def interrupt(self) -> None:
                return None

            async def close(self) -> None:
                return None

        def capture(app: web.Application, *_args: Any, **_kwargs: Any) -> None:
            h.captured.append(app)
            h.captured_event.set()

        async with TestServer(progress) as server:
            _isolate_env(monkeypatch)
            monkeypatch.setenv("PATH", os.pathsep.join((str(bindir), "/usr/bin", "/bin")))
            monkeypatch.setenv("CURIE_PLUGIN_DIR", str(plugin))
            monkeypatch.setenv("CURIE_SESSION_ID", "s-preflight-readiness")
            monkeypatch.setenv("CURIE_SANDBOX_ID", "b-preflight-readiness")
            monkeypatch.setenv("CURIE_BUDGET", _BUDGET)
            monkeypatch.setenv("CURIE_MODEL", _MODEL)
            monkeypatch.setenv("CURIE_RUNNER_TOKEN", _RUNNER_TOKEN)
            monkeypatch.setenv(
                PROGRESS_URL_ENV, str(server.make_url("/v1/work-item-progress/readiness"))
            )
            monkeypatch.setenv(PROGRESS_TOKEN_ENV, _PROGRESS_TOKEN)
            if fake_model:
                monkeypatch.setenv("CURIE_FAKE_MODEL", "1")
            monkeypatch.setattr(boot, "Path", _workspace_path_shim(workspace))
            monkeypatch.setattr(boot, "ClaudeAgentSession", ModelDouble)
            monkeypatch.setattr(boot.web, "run_app", capture)

            def serve() -> None:
                try:
                    boot._serve()
                except BaseException as exc:  # noqa: BLE001 - surfaced by the waits
                    h.thread_errors.append(exc)

            h.thread = threading.Thread(target=serve, name="curie-serve", daemon=True)
            h.thread.start()
            try:
                yield h
            finally:
                # Always release, so a reversed boot blocked in the check returns.
                h.release.touch()
                loop = asyncio.get_running_loop()
                deadline = loop.time() + 30
                while h.thread.is_alive() and loop.time() < deadline:
                    await asyncio.sleep(0.05)
                if h.app_runner is not None and not h.cleaned:
                    with anyio.fail_after(30):
                        await h.app_runner.cleanup()
                    h.cleaned = True


def _finals(outbound: list[Any]) -> list[Final]:
    return [event for event in outbound if isinstance(event, Final)]


def _process_gone(pid: int) -> bool:
    """Gone, or a zombie some other process owns (not ours to reap)."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return True
    state, ppid = stat.rsplit(")", 1)[1].split()[:2]
    return state == "Z" and int(ppid) != os.getpid()


# --- the fix pin --------------------------------------------------------------------


@pytest.mark.parametrize("exit_status", [0, 1], ids=["check-exits-0", "check-exits-1"])
def test_http_ready_while_declared_check_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_status: int
) -> None:
    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch, exit_status=exit_status) as h:
            await h.wait_entered()

            # The readiness probe: a listener answering 200 while the check is
            # still held. A boot that runs the check before the app exists hands
            # nothing to run_app, so the observed status stays None.
            loop = asyncio.get_running_loop()
            healthz_status: int | None = None
            deadline = loop.time() + _HEALTHZ_WITHIN_S
            while loop.time() < deadline:
                if await h.serve_if_captured():
                    healthz_status = await h.status_of("/healthz")
                    if healthz_status == 200:
                        break
                await asyncio.sleep(0.05)
            assert healthz_status == 200, (
                f"GET /healthz answered {healthz_status} while the declared check ran; "
                "the listener must bind before the preflight finishes"
            )
            assert not h.release.exists()
            assert not _process_gone(h.pid()), "the check must still be running"
            assert (await h.runner_status())["ready"] is False
            assert h.verification == []
            assert h.model.constructed == 0

            h.release.touch()
            await h.wait_ready()

            assert [body for body, _ in h.verification] == [_hold_record(exit_status)]
            # Executable route: a failed check does not block the turn.
            outbound = await h.turn("Add a kilometres-to-miles conversion.")
            finals = _finals(outbound)
            assert len(finals) == 1 and outbound[-1] is finals[0], outbound
            assert not finals[0].text.startswith("Could not complete:"), finals[0].text
            assert finals[0].status is SessionStatus.DONE, finals[0]
            assert h.model.constructed == 1
            assert h.model.queries == 1

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


# --- an early turn shares the one initialization ------------------------------------


def test_early_turn_waits_for_shared_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch) as h:
            await h.wait_entered()
            await h.wait_served(timeout=_HEALTHZ_WITHIN_S)

            early = asyncio.create_task(h.turn("Start on the issue."))
            await asyncio.sleep(0.5)
            assert not early.done(), "the early turn must wait for the preflight"
            assert h.model == _Model(), h.events
            assert h.verification == []

            h.release.touch()
            with anyio.fail_after(30):
                outbound = await early

            finals = _finals(outbound)
            assert len(finals) == 1 and outbound[-1] is finals[0], outbound
            assert _ANSWER in finals[0].text, finals[0].text
            # Every report was accepted before the model was built.
            assert h.events == [
                "verification:201",
                "model:constructed",
                "model:connected",
                "model:query",
            ]
            assert h.model == _Model(constructed=1, connected=1, queries=1)

            second = await h.turn("Continue.")
            assert len(_finals(second)) == 1 and isinstance(second[-1], Final), second
            assert h.model == _Model(constructed=1, connected=1, queries=2)
            assert len(h.verification) == 1, "the preflight must not run again"

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


# --- initialization outcomes ---------------------------------------------------------


def test_early_turn_after_unavailable_undelegated_check_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # #3873 routing survives the move: a check that cannot run here and is not
    # delegated still answers offline, with no model.
    async def scenario() -> None:
        missing = [{"id": "absent", "paths": ["**"], "command": ["no-such-curie-check"]}]
        async with _harness(tmp_path, monkeypatch, checks=missing) as h:
            await h.wait_served()
            outbound = await h.turn("Start on the issue.")

            finals = _finals(outbound)
            assert len(finals) == 1 and outbound[-1] is finals[0], outbound
            assert finals[0].text.startswith(_BLOCKED_PREFIX), finals[0].text
            assert [body["outcome"] for body, _ in h.verification] == ["unavailable"]
            assert h.model.constructed == 0

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


def test_rejected_verification_report_fails_every_turn_without_a_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch, report_status=409) as h:
            await h.wait_entered()
            await h.wait_served(timeout=_HEALTHZ_WITHIN_S)

            # Both turns are in flight on the shared initialization before the
            # check is released, so neither reads an already-settled outcome.
            first = asyncio.create_task(h.turn("Start on the issue."))
            second = asyncio.create_task(h.turn("Try again."))
            await asyncio.sleep(0.5)
            assert not first.done() and not second.done()
            assert h.verification == []
            assert h.model.constructed == 0
            h.release.touch()
            with anyio.fail_after(30):
                outbounds = await asyncio.gather(first, second)

            for outbound in outbounds:
                finals = _finals(outbound)
                assert len(finals) == 1 and outbound[-1] is finals[0], outbound
                assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE, finals[0]
                assert any(
                    isinstance(event, ErrorEvent) and event.classification == "runner-error"
                    for event in outbound
                ), outbound
            assert [status for _, status in h.verification] == [409]
            assert h.model.constructed == 0
            assert (await h.runner_status())["ready"] is False
            assert await h.status_of("/healthz") == 200

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


def test_fake_model_runs_no_declared_check_and_calls_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # CURIE_FAKE_MODEL must stay offline: no check, no progress POST, no model.
    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch, fake_model=True) as h:
            await h.wait_served()
            assert await h.status_of("/healthz") == 200
            await h.wait_ready()
            await asyncio.sleep(0.3)

            assert not h.entered.exists()
            assert not h.pidfile.exists()
            assert h.requests == []
            assert h.model == _Model()

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


def test_cleanup_while_check_held_kills_the_check_and_leaves_no_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch) as h:
            await h.wait_entered()
            assert h.app_runner is not None, "no app was served while the declared check ran"
            pid = h.pid()
            assert not _process_gone(pid)

            # A thread-backed subprocess.run would wait here for the child.
            with anyio.fail_after(5):
                await h.app_runner.cleanup()
            h.cleaned = True
            assert not h.release.exists(), "cleanup must not depend on the barrier"

            loop = asyncio.get_running_loop()
            deadline = loop.time() + 2
            while not _process_gone(pid) and loop.time() < deadline:
                await asyncio.sleep(0.05)
            assert _process_gone(pid), f"check process {pid} survived cleanup"

            def pending() -> list[asyncio.Task[Any]]:
                current = asyncio.current_task()
                return [t for t in asyncio.all_tasks() if t is not current and not t.done()]

            deadline = loop.time() + 1
            while pending() and loop.time() < deadline:
                await asyncio.sleep(0.05)
            assert pending() == [], pending()
            assert h.model.constructed == 0

    async def bounded() -> None:
        with anyio.fail_after(120):
            await scenario()

    asyncio.run(bounded())


# --- a descendant that outlives the check's leader -----------------------------------


def _descendant_body(leader_exits: bool) -> Callable[[_Harness], str]:
    """Background a long sleep that inherits the check's output pipes.

    Its stdout and stderr are deliberately not redirected, so the pipes stay
    open after the leader is gone and initialization keeps draining them.
    """

    def body(h: _Harness) -> str:
        descendant = h.tmp / "descendant-pid"
        lines = f"sleep 120 &\necho $! > '{descendant}'\n: > '{h.entered}'\n"
        return lines + ("exit 0\n" if leader_exits else _barrier(h, 0))

    return body


@pytest.mark.parametrize("leader_exits", [True, False], ids=["leader-exits", "leader-held"])
def test_cleanup_kills_a_descendant_holding_the_check_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, leader_exits: bool
) -> None:
    descendant_file = tmp_path / "descendant-pid"

    async def scenario() -> None:
        async with _harness(tmp_path, monkeypatch, script_body=_descendant_body(leader_exits)) as h:
            await h.wait_entered()
            await h.wait_served(timeout=_HEALTHZ_WITHIN_S)
            descendant = int(descendant_file.read_text(encoding="utf-8").strip())
            leader = h.pid()

            loop = asyncio.get_running_loop()
            if leader_exits:
                # The leader is gone (or a zombie awaiting its reap); only the
                # descendant still holds the pipes.
                deadline = loop.time() + 5
                while loop.time() < deadline:
                    try:
                        state = Path(f"/proc/{leader}/stat").read_text(encoding="utf-8")
                    except FileNotFoundError:
                        break
                    if state.rsplit(")", 1)[1].split()[0] == "Z":
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError(f"check leader {leader} never exited")
            await asyncio.sleep(0.5)
            # Initialization is still waiting on the open pipes.
            assert not _process_gone(descendant), "the descendant must still be running"
            assert h.verification == []
            assert (await h.runner_status())["ready"] is False
            assert h.model.constructed == 0

            assert h.app_runner is not None
            with anyio.fail_after(5):
                await h.app_runner.cleanup()
            h.cleaned = True
            assert not h.release.exists(), "cleanup must not depend on the barrier"

            deadline = loop.time() + 2
            while loop.time() < deadline and not (
                _process_gone(descendant) and _process_gone(leader)
            ):
                await asyncio.sleep(0.05)
            assert _process_gone(descendant), (
                f"descendant {descendant} of the check survived cleanup"
            )
            assert _process_gone(leader), f"check leader {leader} was not reaped"
            assert h.model.constructed == 0

    async def bounded() -> None:
        try:
            with anyio.fail_after(120):
                await scenario()
        finally:
            # A regression must not leave the sleep behind for two minutes.
            if descendant_file.exists():
                with contextlib.suppress(ProcessLookupError, ValueError):
                    os.kill(
                        int(descendant_file.read_text(encoding="utf-8").strip()),
                        signal.SIGKILL,
                    )

    asyncio.run(bounded())
