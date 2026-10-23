"""On-demand attachment client exercised against a streaming platform fixture."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tracemalloc
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.attachment_access import (
    AttachmentAccess,
    AttachmentAccessError,
    AttachmentAccessTurn,
)
from curie_runner.platform_slack.capability import url_origin


def event(base: str, token: str = "ata.synthetic-capability") -> Event:
    return Event(
        type="message",
        text="files",
        user="U",
        ts="1",
        attachment_access={
            "url": base + "/attachment-access",
            "token": token,
        },
    )


class Platform:
    def __init__(self, size: int = 33 * 1024 * 1024 + 7) -> None:
        self.size = size
        self.reads = 0
        self.requests: list[str] = []
        digest = hashlib.sha256()
        for remaining in range(0, size, 64 * 1024):
            digest.update(b"x" * min(64 * 1024, size - remaining))
        self.digest = digest.hexdigest()
        self.status = 200
        self.bad_completion = False
        self.on_completion: Callable[[], None] | None = None
        self.name = "report.bin"
        self.slow = False
        self.partial = False
        self.drop_completion = False
        self.invalid_page: Any = None
        self.redirect: str | None = None
        self.started = anyio.Event()
        self.disconnected = anyio.Event()

    def descriptor(self) -> dict[str, Any]:
        return {
            "id": "ref-1",
            "name": self.name,
            "mime_type": "application/octet-stream",
            "size_bytes": self.size,
            "sha256": self.digest if self.reads else None,
            "current": True,
        }

    async def handle(self, request: web.Request) -> web.StreamResponse:
        self.requests.append(request.path.rsplit("/", 1)[-1])
        assert request.headers.get("X-Curie-Attachment-Access", "").startswith("ata.")
        if self.status != 200:
            return web.json_response({"detail": "denied"}, status=self.status)
        op = self.requests[-1]
        if self.redirect is not None:
            raise web.HTTPFound(self.redirect)
        if op == "list" and self.invalid_page is not None:
            return web.json_response(self.invalid_page)
        if op == "list":
            return web.json_response({"files": [self.descriptor()], "next_cursor": None})
        if op == "authorize":
            return web.json_response(self.descriptor())
        if op == "completion":
            if self.on_completion is not None:
                self.on_completion()
            if self.drop_completion:
                self.drop_completion = False
                assert request.transport is not None
                request.transport.close()
                return web.Response()
            return web.json_response(
                {
                    "id": "ref-1",
                    "sha256": "0" * 64 if self.bad_completion else self.digest,
                    "size_bytes": self.size,
                    "status": "complete",
                }
            )
        assert op == "fetch"
        self.reads += 1
        response = web.StreamResponse(headers={"X-Curie-Transfer-ID": "transfer-1"})
        await response.prepare(request)
        self.started.set()
        remaining = self.size // 2 if self.partial else self.size
        try:
            while remaining:
                chunk = b"x" * min(64 * 1024, remaining)
                await response.write(chunk)
                remaining -= len(chunk)
                if self.slow:
                    await anyio.sleep(0.01)
            await response.write_eof()
        except (ConnectionResetError, anyio.get_cancelled_exc_class()):
            self.disconnected.set()
        return response


@pytest.fixture
async def rig(
    tmp_path: Path,
) -> AsyncIterator[tuple[Platform, AttachmentAccess, AttachmentAccessTurn, str]]:
    platform = Platform()
    app = web.Application()
    app.router.add_post("/attachment-access/{operation}", platform.handle)
    async with TestServer(app) as server:
        base = str(server.make_url("")).rstrip("/")
        turn = AttachmentAccessTurn(trusted_origin=url_origin(base))
        turn.begin(event(base))
        try:
            yield platform, AttachmentAccess(turn, tmp_path), turn, base
        finally:
            turn.end()
            shutil.rmtree(tmp_path)


@pytest.mark.anyio
async def test_stream_above_old_limit_and_verified_cache(rig: Any) -> None:
    platform, access, _, _ = rig
    listing = await access.list({})
    assert listing["files"][0]["cached"] is False
    assert platform.reads == 0
    first, second = await access.fetch({"id": "ref-1"}), await access.fetch({"id": "ref-1"})
    assert first == second
    assert Path(first["path"]).is_absolute()
    assert Path(first["path"]).stat().st_size == platform.size
    assert platform.reads == 1
    assert platform.requests.count("authorize") == 2
    assert (await access.list({}))["files"][0]["cached"] is True


@pytest.mark.anyio
async def test_bad_completion_never_publishes(rig: Any, tmp_path: Path) -> None:
    platform, access, _, _ = rig
    platform.bad_completion = True
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_cache_revalidates_live_authority_and_local_bytes(rig: Any) -> None:
    platform, access, _, _ = rig
    result = await access.fetch({"id": "ref-1"})
    platform.status = 403
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    platform.status = 200
    Path(result["path"]).write_bytes(b"tampered")
    assert (await access.fetch({"id": "ref-1"}))["size_bytes"] == platform.size
    assert platform.reads == 2


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["../escape", "/tmp/escape", "a/b", "a\\b", ".", "\u0000bad"])
async def test_untrusted_names_refused_before_body(rig: Any, name: str) -> None:
    platform, access, _, _ = rig
    platform.name = name
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert platform.reads == 0


@pytest.mark.anyio
async def test_parallel_same_id_serializes_provider_read(rig: Any) -> None:
    platform, access, _, _ = rig
    results: list[dict[str, Any]] = []

    async def fetch() -> None:
        results.append(await access.fetch({"id": "ref-1"}))

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(fetch)
        tasks.start_soon(fetch)
    assert results[0] == results[1]
    assert platform.reads == 1


@pytest.mark.anyio
async def test_steer_renews_opener_and_old_end_cannot_revoke_successor(rig: Any) -> None:
    _, access, turn, base = rig
    opener = turn.begin(event(base))
    turn.steer(event(base))
    assert (await access.list({}))["files"]
    successor = turn.begin(event(base, "ata.successor"))
    turn.end(opener)
    assert turn.current() is successor
    turn.steer(Event(type="message", text="no cap", user="U", ts="2"))
    with pytest.raises(AttachmentAccessError):
        await access.list({})


@pytest.mark.anyio
async def test_turn_end_cancels_stream_and_cleans_partial(rig: Any, tmp_path: Path) -> None:
    platform, access, turn, _ = rig
    platform.slow = True
    errors: list[Exception] = []

    async def fetch() -> None:
        try:
            await access.fetch({"id": "ref-1"})
        except AttachmentAccessError as exc:
            errors.append(exc)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(fetch)
        await platform.started.wait()
        turn.end()
    assert errors
    assert list(tmp_path.iterdir()) == []
    with anyio.fail_after(2):
        await platform.disconnected.wait()


@pytest.mark.anyio
async def test_untrusted_origin_has_no_network(rig: Any) -> None:
    platform, access, turn, _ = rig
    turn.begin(event("http://untrusted.example.com"))
    with pytest.raises(AttachmentAccessError):
        await access.list({})
    assert platform.requests == []


def test_boot_mounts_policy_governed_tools_and_fake_is_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from curie_runner.__main__ import build_runner
    from curie_runner.approval import is_platform_owned_tool
    from runner_boot_session import initialized_session_sync
    from test_channel_read import _config, _patch_sdk
    from test_connectors import _published_live_tool_names

    _patch_sdk(monkeypatch)
    config = replace(
        _config(
            monkeypatch,
            tmp_path,
            grant=False,
            policy={"allow": ["*/list_attachments"], "deny": ["*/fetch_attachment"]},
        ),
        attachment_access_enabled=True,
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    runner = build_runner(config, attachments_path=cache)
    options = initialized_session_sync(runner).options
    published = _published_live_tool_names(options.mcp_servers)
    names = {"mcp__curie__list_attachments", "mcp__curie__fetch_attachment"}
    assert names <= published
    assert "mcp__curie__fetch_attachment" in options.disallowed_tools
    assert "mcp__curie__list_attachments" not in options.disallowed_tools
    assert options.allowed_tools == []
    assert runner.enforces_attachment_access
    assert all(not is_platform_owned_tool(name, state_server_mounted=True) for name in names)
    fake = build_runner(config, fake_model=True, attachments_path=cache)
    assert not fake.enforces_attachment_access


def test_lazy_activation_requires_writable_cache_and_trusted_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from curie_runner.__main__ import build_runner
    from test_channel_read import _config, _patch_sdk

    _patch_sdk(monkeypatch)
    config = replace(_config(monkeypatch, tmp_path, grant=False), attachment_access_enabled=True)
    with pytest.raises(ValueError, match="attachments"):
        build_runner(config)


@pytest.mark.anyio
async def test_session_lifecycle_and_status_use_actual_boot_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from aci_protocol import ATTACHMENT_ACCESS_STATUS_FIELD
    from aiohttp.test_utils import TestClient
    from curie_runner import create_app
    from curie_runner.__main__ import build_runner
    from test_channel_read import _call_server, _config, _patch_sdk, _until

    platform = Platform(17)
    app = web.Application()
    app.router.add_post("/attachment-access/{operation}", platform.handle)
    async with TestServer(app) as server:
        base = str(server.make_url("")).rstrip("/")
        sessions = _patch_sdk(monkeypatch)
        config = replace(
            _config(monkeypatch, tmp_path, api_base=base, grant=False),
            attachment_access_enabled=True,
        )
        cache = tmp_path / "cache"
        cache.mkdir()
        runner = await anyio.to_thread.run_sync(
            lambda: build_runner(config, attachments_path=cache)
        )
        async with TestClient(TestServer(create_app(runner))) as client:
            assert (await (await client.get("/status")).json())[
                ATTACHMENT_ACCESS_STATUS_FIELD
            ] is True
            await _until(lambda: bool(sessions))
            sdk = sessions[-1]
            tools = sdk.options.mcp_servers["curie"]["instance"]
            assert (await _call_server(tools, "list_attachments", {}))["is_error"]

            terminal_seen, release_terminal = anyio.Event(), anyio.Event()

            async def consume() -> None:
                async for line in runner.run_turn(event(base)):
                    if json.loads(line).get("type") == "final":
                        terminal_seen.set()
                        await release_terminal.wait()

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(consume)
                await _until(lambda: bool(sdk.queries))
                assert "remote references" in sdk.queries[-1]
                assert "on disk at" not in sdk.queries[-1]
                assert "ata.synthetic-capability" not in sdk.queries[-1]
                assert not (await _call_server(tools, "list_attachments", {}))["is_error"]
                assert await runner.steer("again", event=event(base))
                assert not (await _call_server(tools, "fetch_attachment", {"id": "ref-1"}))[
                    "is_error"
                ]
                sdk.finish()
                await terminal_seen.wait()
                try:
                    assert (await _call_server(tools, "list_attachments", {}))["is_error"]
                finally:
                    release_terminal.set()
            assert (await _call_server(tools, "list_attachments", {}))["is_error"]
            assert platform.reads == 1


@pytest.mark.anyio
async def test_missing_opener_stale_end_and_null_steer_revocation_then_valid_renewal(
    rig: Any,
) -> None:
    _, access, turn, base = rig
    old = turn.begin(Event(type="message", text="none", user="U", ts="2"))
    successor = turn.begin(event(base))
    turn.end(old)
    assert turn.current() is successor
    turn.steer(None)
    with pytest.raises(AttachmentAccessError):
        await access.list({})
    turn.steer(event(base))
    assert (await access.list({}))["files"]


def test_side_effects_keep_fetch_conservative() -> None:
    from curie_runner.side_effects import SideEffectClassifier

    classifier = SideEffectClassifier()
    assert not classifier.is_side_effecting("mcp__curie__list_attachments")
    assert classifier.is_side_effecting("mcp__curie__fetch_attachment")
    assert classifier.is_side_effecting("mcp__curie__fetch_attachment_extra")


@pytest.mark.anyio
async def test_long_safe_utf8_filename_does_not_exceed_filesystem_limit(rig: Any) -> None:
    platform, access, _, _ = rig
    platform.name = chr(0xE9) * 125 + ".pdf"
    result = await access.fetch({"id": "ref-1"})
    assert Path(result["path"]).suffix == ".pdf"
    assert len(Path(result["path"]).name.encode()) <= 255
    assert result["size_bytes"] == platform.size


@pytest.mark.anyio
@pytest.mark.parametrize("name", [".hidden", "a\x7fb", chr(0xE9) * 128])
async def test_hidden_control_or_overlong_utf8_names_refused(rig: Any, name: str) -> None:
    platform, access, _, _ = rig
    platform.name = name
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert platform.reads == 0


@pytest.mark.anyio
async def test_partial_eof_is_not_completion(rig: Any, tmp_path: Path) -> None:
    platform, access, _, _ = rig
    platform.partial = True
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_lost_completion_response_retries_only_status(rig: Any) -> None:
    platform, access, _, _ = rig
    platform.drop_completion = True
    result = await access.fetch({"id": "ref-1"})
    assert result["size_bytes"] == platform.size
    assert platform.reads == 1
    assert platform.requests.count("completion") == 2


@pytest.mark.anyio
async def test_cache_symlink_is_never_followed_or_overwritten(rig: Any, tmp_path: Path) -> None:
    _, access, _, _ = rig
    first = await access.fetch({"id": "ref-1"})
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.write_bytes(b"private unrelated bytes")
    try:
        Path(first["path"]).unlink()
        Path(first["path"]).symlink_to(outside)
        second = await access.fetch({"id": "ref-1"})
        assert second["path"] != first["path"]
        assert outside.read_bytes() == b"private unrelated bytes"
        assert not Path(second["path"]).is_symlink()
    finally:
        outside.unlink()


@pytest.mark.anyio
async def test_root_symlink_refused_before_content(rig: Any, tmp_path: Path) -> None:
    platform, access, _, _ = rig
    destination = tmp_path / "actual"
    destination.mkdir()
    link = tmp_path / "link"
    link.symlink_to(destination)
    access.root = link
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert platform.reads == 0
    assert list(destination.iterdir()) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "page",
    [
        {"files": [{}], "next_cursor": None},
        {"files": [], "next_cursor": 42},
        {"files": [], "next_cursor": "x" * (256 * 1024)},
        {"files": [{}] * 101, "next_cursor": None},
    ],
)
async def test_bounded_malformed_catalogs_fail_closed(rig: Any, page: Any) -> None:
    platform, access, _, _ = rig
    platform.invalid_page = page
    with pytest.raises(AttachmentAccessError):
        await access.list({})
    assert platform.reads == 0


@pytest.mark.anyio
async def test_redirect_never_forwards_capability(rig: Any) -> None:
    platform, access, _, base = rig
    platform.redirect = base + "/untrusted"
    with pytest.raises(AttachmentAccessError):
        await access.list({})
    assert platform.requests == ["list"]


@pytest.mark.anyio
async def test_finite_transfer_timeout_cleans_staging(
    rig: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import curie_runner.attachment_access as module

    platform, access, _, _ = rig
    platform.slow = True
    monkeypatch.setattr(module, "TRANSFER_SECONDS", 0.05)
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_proxy_environment_does_not_control_platform_access(
    rig: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, access, _, _ = rig
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setenv("NO_PROXY", "")
    assert (await access.list({}))["files"]


@pytest.mark.anyio
async def test_streaming_buffer_stays_bounded_above_old_file_limit(rig: Any) -> None:
    _, access, _, _ = rig
    tracemalloc.start()
    try:
        result = await access.fetch({"id": "ref-1"})
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result["size_bytes"] > 32 * 1024 * 1024
    assert peak < 8 * 1024 * 1024


@pytest.mark.anyio
async def test_selected_transfer_above_old_thread_budget_has_no_byte_cutoff(tmp_path: Path) -> None:
    platform = Platform(257 * 1024 * 1024 + 1)
    app = web.Application()
    app.router.add_post("/attachment-access/{operation}", platform.handle)
    async with TestServer(app) as server:
        base = str(server.make_url("")).rstrip("/")
        turn = AttachmentAccessTurn(trusted_origin=url_origin(base))
        turn.begin(event(base))
        try:
            access = AttachmentAccess(turn, tmp_path)
            result = await access.fetch({"id": "ref-1"})
            assert result["size_bytes"] == platform.size
            assert Path(result["path"]).stat().st_size == platform.size
        finally:
            turn.end()
            shutil.rmtree(tmp_path)


@pytest.mark.anyio
async def test_actual_filesystem_capacity_failure_leaves_no_path(rig: Any, tmp_path: Path) -> None:
    _, _, _, base = rig
    # Limit only the owned child process's regular-file capacity. The real
    # stream and real filesystem fail after 1 MiB; no storage API is mocked.
    script = """
import signal, resource, sys
from pathlib import Path
import anyio
from aci_protocol import Event
from curie_runner.attachment_access import (
    AttachmentAccess, AttachmentAccessError, AttachmentAccessTurn,
)
from curie_runner.platform_slack.capability import url_origin
async def main():
    root, base = Path(sys.argv[1]), sys.argv[2]
    turn = AttachmentAccessTurn(trusted_origin=url_origin(base))
    turn.begin(Event(type='message', text='files', user='U', ts='1', attachment_access={
        'url': base + '/attachment-access', 'token': 'ata.synthetic-capability'}))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024 * 1024, 1024 * 1024))
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    try:
        await AttachmentAccess(turn, root).fetch({'id': 'ref-1'})
    except AttachmentAccessError:
        assert list(root.iterdir()) == []
        print('capacity-failure-clean')
    else:
        raise AssertionError('storage exhaustion accepted a file')
anyio.run(main)
"""
    result = await anyio.run_process([sys.executable, "-c", script, str(tmp_path), base])
    assert result.stdout.strip() == b"capacity-failure-clean"


def test_policy_and_operator_gates_intercept_attachment_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from curie_runner.__main__ import build_runner
    from runner_boot_session import initialized_session_sync
    from test_channel_read import _config, _patch_sdk
    from test_tool_policy_enforcement import _interception_reason

    _patch_sdk(monkeypatch)
    config = replace(
        _config(monkeypatch, tmp_path, grant=False, policy={"allow": ["*/list_attachments"]}),
        attachment_access_enabled=True,
        approval_required_tools=("mcp__curie__list_attachments",),
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    runner = build_runner(config, attachments_path=cache)
    initialized_session_sync(runner)
    gate = runner._approval_gate  # noqa: SLF001 -- exercise the gate boot actually installed
    assert gate is not None
    for interceptor in ("hook", "callback"):
        assert "approval" in _interception_reason(gate, "mcp__curie__list_attachments", interceptor)
        assert "not permitted for this agent" in _interception_reason(
            gate, "mcp__curie__fetch_attachment", interceptor
        )


def test_readonly_turn_can_list_but_cannot_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from aci_protocol import ToolAccess
    from claude_agent_sdk.types import (
        PermissionResultAllow,
        PermissionResultDeny,
        ToolPermissionContext,
    )
    from curie_runner.__main__ import build_runner
    from runner_boot_session import initialized_session_sync
    from test_channel_read import _config, _patch_sdk

    _patch_sdk(monkeypatch)
    config = replace(
        _config(monkeypatch, tmp_path, grant=False, policy={"allow": ["*/*"]}),
        attachment_access_enabled=True,
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    runner = build_runner(config, attachments_path=cache)
    options = initialized_session_sync(runner).options
    runner._tool_access.begin(ToolAccess.READ_ONLY)  # noqa: SLF001 -- arm actual boot callback front
    for name, expected in (
        ("list_attachments", PermissionResultAllow),
        ("fetch_attachment", PermissionResultDeny),
    ):
        result = anyio.run(options.can_use_tool, "mcp__curie__" + name, {}, ToolPermissionContext())
        assert isinstance(result, expected)


@pytest.mark.anyio
async def test_staging_symlink_replacement_cannot_publish(rig: Any, tmp_path: Path) -> None:
    platform, access, _, _ = rig
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.write_bytes(b"unrelated")

    def replace_staging() -> None:
        staged = next(tmp_path.glob(".attachment-*"))
        staged.unlink()
        staged.symlink_to(outside)

    platform.on_completion = replace_staging
    try:
        with pytest.raises(AttachmentAccessError):
            await access.fetch({"id": "ref-1"})
        assert list(tmp_path.iterdir()) == []
        assert outside.read_bytes() == b"unrelated"
    finally:
        outside.unlink()


@pytest.mark.anyio
async def test_malformed_unicode_metadata_is_a_safe_tool_failure(rig: Any) -> None:
    platform, access, _, _ = rig
    platform.name = "\ud800.pdf"
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert platform.reads == 0


def test_boot_feature_flag_uses_the_optional_contract_parser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_runner import RunnerConfig
    from test_connectors import _boot_env

    env = _boot_env(monkeypatch, tmp_path, "attachments")
    assert not RunnerConfig.from_env(env).attachment_access_enabled
    assert RunnerConfig.from_env(
        env | {"CURIE_ATTACHMENT_ACCESS_ENABLED": "1"}
    ).attachment_access_enabled
    assert not RunnerConfig.from_env(
        env | {"CURIE_ATTACHMENT_ACCESS_ENABLED": "0"}
    ).attachment_access_enabled


@pytest.mark.anyio
async def test_final_name_collision_preserves_existing_file(
    rig: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    import curie_runner.attachment_access as module

    _, access, _, _ = rig
    suffix = "c" * 32
    monkeypatch.setattr(module.uuid, "uuid4", lambda: SimpleNamespace(hex=suffix))
    existing = tmp_path / (hashlib.sha256(b"ref-1").hexdigest() + "-" + suffix + ".bin")
    existing.write_bytes(b"unrelated existing file")
    with pytest.raises(AttachmentAccessError):
        await access.fetch({"id": "ref-1"})
    assert existing.read_bytes() == b"unrelated existing file"
    assert list(tmp_path.iterdir()) == [existing]


@pytest.mark.anyio
async def test_fifo_cache_replacement_cannot_block_turn_deadline(rig: Any, tmp_path: Path) -> None:
    _, _, _, base = rig
    script = """
import os, sys
from pathlib import Path
import anyio
from aci_protocol import Event
from curie_runner.attachment_access import AttachmentAccess, AttachmentAccessTurn
from curie_runner.platform_slack.capability import url_origin
async def main():
    root, base = Path(sys.argv[1]), sys.argv[2]
    turn = AttachmentAccessTurn(trusted_origin=url_origin(base))
    turn.begin(Event(type='message', text='files', user='U', ts='1', attachment_access={
        'url': base + '/attachment-access', 'token': 'ata.synthetic-capability'}))
    access = AttachmentAccess(turn, root)
    result = await access.fetch({'id': 'ref-1'})
    path = Path(result['path'])
    path.unlink()
    os.mkfifo(path)
    second = await access.fetch({'id': 'ref-1'})
    assert second['path'] != result['path']
    print('fifo-refused')
anyio.run(main)
"""
    with anyio.fail_after(5):
        result = await anyio.run_process([sys.executable, "-c", script, str(tmp_path), base])
    assert result.stdout.strip() == b"fifo-refused"


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["list", "fetch", "download"])
@pytest.mark.parametrize("stop", ["deadline", "turn_end"])
async def test_local_cache_hash_stops_with_original_operation_authority(
    rig: Any,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    stop: str,
) -> None:
    import threading
    import time

    import curie_runner.attachment_access as module

    platform, access, turn, _ = rig
    await access.fetch({"id": "ref-1"})
    existing = set(access.root.iterdir())
    if operation == "download":
        # A new cache client exercises final verification after a real body
        # transfer as well as the existing-cache read paths.
        access = AttachmentAccess(turn, access.root)
    real_fdopen = module.os.fdopen
    started = threading.Event()
    reads: list[int] = []

    class SlowLocalReader:
        """Delay bounded reads of the actual cached file, never substitute its bytes."""

        def __init__(self, stream: Any) -> None:
            self.stream = stream

        def __enter__(self) -> SlowLocalReader:
            self.stream.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self.stream.__exit__(*args)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.stream, name)

        def read(self, size: int) -> bytes:
            started.set()
            reads.append(1)
            time.sleep(0.004)
            return self.stream.read(min(size, 64 * 1024))

        def readinto(self, buffer: Any) -> int:
            started.set()
            reads.append(1)
            time.sleep(0.004)
            return self.stream.readinto(memoryview(buffer)[: 64 * 1024])

    def controlled_reader(fd: int, mode: str) -> Any:
        stream = real_fdopen(fd, mode)
        return SlowLocalReader(stream) if mode == "rb" else stream

    monkeypatch.setattr(module.os, "fdopen", controlled_reader)
    if stop == "deadline":
        monkeypatch.setattr(module, "TRANSFER_SECONDS", 0.3 if operation == "download" else 0.08)
    failures: list[AttachmentAccessError] = []
    started_at = time.monotonic()

    async def use_cache() -> None:
        try:
            if operation != "list":
                await access.fetch({"id": "ref-1"})
            else:
                await access.list({})
        except AttachmentAccessError as exc:
            failures.append(exc)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(use_cache)
        if stop == "turn_end":
            with anyio.fail_after(2):
                while not started.is_set():
                    await anyio.sleep(0.001)
            turn.end()
    assert failures, "A retired or expired operation must not return a cached path/catalog"
    assert started.is_set(), "The cancellation must exercise actual disk verification"
    assert time.monotonic() - started_at < 0.9
    assert len(reads) < 128, "Cancellation must stop the disk hash before consuming the whole file"
    assert set(access.root.iterdir()) == existing, "Cancelled publication must remove its new file"
    assert platform.reads == (2 if operation == "download" else 1), (
        "Verification cancellation must never trigger a provider retry"
    )
