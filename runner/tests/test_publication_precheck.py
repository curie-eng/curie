"""Publication refusals stay inside the active turn and share the SDK call identity."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import which
from typing import Any

import anyio
import pytest
from aci_protocol import Event
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import PermissionResultDeny, ToolPermissionContext
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import ApprovalGate, build_approval_hook, build_can_use_tool
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME
from starlette.requests import Request
from starlette.responses import StreamingResponse

from .test_hosted_mcp_approval_catalog import (
    _ProviderCapture,
    _sdk_env,
    _serve,
    _sse,
    _text_frames,
    _tool_use_frames,
)

REPO = "acme-corp/acme-bot"
TITLE = "Update documentation"
BODY = "Explain the requested documentation changes.\n"
CAPABILITY = "ppc.test.publicationcapability"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def workspace(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "remote", "add", "origin", f"https://github.com/{REPO}.git")
    (repo / "README.md").write_text("Original documentation.\n")
    _git(repo, "add", "README.md")
    _git(
        repo,
        "-c",
        "user.name=Curie Test",
        "-c",
        "user.email=curie@example.com",
        "commit",
        "--quiet",
        "-m",
        "Initial fixture",
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _context(head: str, url: str, *, capability: str = CAPABILITY) -> dict[str, Any]:
    return {
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "deployment_id": "22222222-2222-4222-8222-222222222222",
        "work_item_id": "33333333-3333-4333-8333-333333333333",
        "execution_request_id": "44444444-4444-4444-8444-444444444444",
        "lineage_id": "55555555-5555-4555-8555-555555555555",
        "runtime_epoch": 2,
        "lineage_version": 3,
        "conversation_id": "slack:T0EXAMPLE1:C0EXAMPLE1:123.45",
        "queued_event_id": "publicationevent1",
        "expected_head": head,
        "precheck_url": url,
        "capability": capability,
        "observed_title": TITLE,
        "observed_body_sha256": hashlib.sha256(BODY.encode()).hexdigest(),
        "observed_at": "2026-09-25T10:00:00Z",
    }


def _event(context: dict[str, Any] | None) -> Event:
    return Event.model_validate(
        {
            "type": "message",
            "text": "Publish the requested changes.",
            "user": "U0EXAMPLE1",
            "ts": "123.45",
            "publication_context": context,
        }
    )


def _success() -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="publicationtest",
        result="The request was handled.",
        usage={"input_tokens": 20, "output_tokens": 8},
    )


@asynccontextmanager
async def _api(
    replies: list[tuple[int, dict[str, str]]],
) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
    calls: list[dict[str, Any]] = []

    async def compare(request: web.Request) -> web.Response:
        calls.append(
            {
                "capability": request.headers.get("X-Curie-Publication-Precheck"),
                "body": await request.json(),
            }
        )
        index = min(len(calls) - 1, len(replies) - 1)
        status, body = replies[index]
        return web.json_response(body, status=status)

    app = web.Application()
    app.router.add_post("/publications/precheck", compare)
    server = TestServer(app)
    await server.start_server()
    try:
        yield str(server.make_url("/publications/precheck")), calls
    finally:
        await server.close()


async def _boot(
    tmp_path: Path,
    repo: Path,
    api_url: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    sdk_env: dict[str, str] | None = None,
) -> SessionRunner:
    bundle = tmp_path / "bundle"
    (bundle / ".claude-plugin").mkdir(parents=True)
    (bundle / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "publicationtest", "version": "0.1.0", "description": "test"})
    )
    monkeypatch.setenv("CURIE_STATE_URL", api_url.replace("/publications/precheck", "/state"))
    monkeypatch.setenv("CURIE_STATE_TOKEN", "teststatecapability")
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(bundle),
            "CURIE_SESSION_ID": "publicationtest",
            "CURIE_SANDBOX_ID": "publicationbox",
            "CURIE_BUDGET": '{"max_output_tokens_per_run":10000,"max_usd_per_day":1.0}',
        }
    )
    # Production boot performs synchronous capability discovery with anyio.run.
    # Keep that boot outside this test server's running event loop.
    return await anyio.to_thread.run_sync(
        lambda: build_runner(
            config, workspace_path=repo, fake_model=False, sdk_env=sdk_env
        )
    )


class _PublicationModel(FakeModelSession):
    """An external model that can receive the stream before its SDK callbacks."""

    def __init__(
        self,
        gate: ApprovalGate,
        proposals: list[tuple[str, dict[str, Any]]],
        *,
        stream_first: bool = True,
        before_call: Callable[[int], None] | None = None,
    ) -> None:
        super().__init__()
        self.gate = gate
        self.proposals = proposals
        self.stream_first = stream_first
        self.before_call = before_call
        self.permissions: list[PermissionResultDeny] = []
        self.hooks: list[dict[str, Any]] = []
        self.pending_after_stream: list[bool] = []
        self.tool_results: list[ToolResultBlock] = []

    async def receive_turn(self) -> AsyncIterator[Any]:
        permission = build_can_use_tool(self.gate)
        hook = build_approval_hook(self.gate)["PreToolUse"][0].hooks[0]
        for index, (call_id, proposal) in enumerate(self.proposals):
            if self.before_call is not None:
                self.before_call(index)
            message = AssistantMessage(
                content=[ToolUseBlock(id=call_id, name=PLATFORM_PUBLISH_TOOL_NAME, input=proposal)],
                model="externaltestmodel",
            )
            if self.stream_first:
                yield message
                self.pending_after_stream.append(self.gate.pending_summary is not None)
            # The SDK declares these two ID fields in ToolPermissionContext and
            # HookCallback (claude_agent_sdk/types.py). Stream identity comes
            # from the corresponding SDK ToolUseBlock.id.
            hook_result, decision = await asyncio.gather(
                hook(
                    {"tool_name": PLATFORM_PUBLISH_TOOL_NAME, "tool_input": proposal},
                    call_id,
                    {},
                ),
                permission(
                    PLATFORM_PUBLISH_TOOL_NAME,
                    proposal,
                    ToolPermissionContext(tool_use_id=call_id),
                ),
            )
            assert isinstance(decision, PermissionResultDeny)
            self.hooks.append(hook_result)
            self.permissions.append(decision)
            if not self.stream_first:
                yield message
            result = ToolResultBlock(tool_use_id=call_id, content=decision.message, is_error=True)
            self.tool_results.append(result)
            yield UserMessage(content=[result])
            if decision.interrupt:
                return
        yield _success()


async def _run(
    runner: SessionRunner, model: FakeModelSession, event: Event
) -> list[dict[str, Any]]:
    runner._factory = lambda: model
    await runner.start()
    try:
        return [json.loads(line) async for line in runner.run_turn(event)]
    finally:
        await runner.close()


def _assert_refusal(
    model: _PublicationModel, gate: ApprovalGate, frames: list[dict[str, Any]], reason: str
) -> None:
    assert reason in model.permissions[0].message
    assert model.permissions[0].interrupt is False
    hook = model.hooks[0]
    assert hook["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert reason in hook["hookSpecificOutput"]["permissionDecisionReason"]
    assert hook.get("continue_", True) is True
    assert not hook.get("stopReason")
    assert model.tool_results[0].is_error is True
    assert frames[-1]["status"] == "done"
    assert not any(frame["type"] == "error" for frame in frames)
    assert gate.pending_summary is None
    assert gate.pending_granted_tool is None
    assert gate.publication_title is None
    assert gate.publication_body is None
    assert gate.pending_halt is False
    assert gate.grant_tool is None


@pytest.mark.parametrize("stream_first", [True, False])
def test_empty_diff_refusal_shares_one_decision_and_finishes_done(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stream_first: bool,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(
                gate,
                [("publish1", {"title": f"  {TITLE}  ", "body": BODY})],
                stream_first=stream_first,
            )
            frames = await _run(runner, model, _event(_context(head, url)))
            _assert_refusal(model, gate, frames, "no_change")
            assert len(calls) == 1
            assert calls[0]["capability"] == CAPABILITY
            assert calls[0]["body"]["proposed_title"] == TITLE
            assert calls[0]["body"]["proposed_body"] == BODY
            assert (
                calls[0]["body"]["observed_body_sha256"]
                == hashlib.sha256(BODY.encode()).hexdigest()
            )
            if stream_first:
                assert model.pending_after_stream == [False]
            assert CAPABILITY not in json.dumps(frames)
            assert CAPABILITY not in caplog.text
            assert all(CAPABILITY not in query for query in model.queries)

    anyio.run(go)


@pytest.mark.parametrize("body", [None, "", " \n\t"])
def test_blank_body_refuses_before_snapshot_or_remote_read(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    body: str | None,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(503, {"detail": "precheck_unavailable"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            # Boot sees a managed workspace; capture would now fail. A body
            # refusal must win without trying to classify this as an empty diff.
            (repo / ".git").rename(repo / "gitmetadata")
            proposal = {"title": TITLE}
            if body is not None:
                proposal["body"] = body
            model = _PublicationModel(gate, [("blankbody", proposal)])
            frames = await _run(runner, model, _event(_context(head, url)))
            _assert_refusal(model, gate, frames, "body_required")
            assert calls == []

    anyio.run(go)


@pytest.mark.parametrize("initial_body", [BODY, " \n"])
def test_refused_proposal_can_be_corrected_with_a_file_change_in_the_same_turn(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    initial_body: str,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None

            def correct(index: int) -> None:
                if index == 1:
                    assert gate.pending_summary is None
                    assert gate.pending_halt is False
                    (repo / "README.md").write_text("Corrected documentation.\n")

            model = _PublicationModel(
                gate,
                [
                    ("refused", {"title": TITLE, "body": initial_body}),
                    ("corrected", {"title": "Publish corrected documentation", "body": BODY}),
                ],
                before_call=correct,
            )
            frames = await _run(runner, model, _event(_context(head, url)))
            assert len(model.queries) == 1
            assert len(model.permissions) == 2
            assert model.permissions[0].interrupt is False
            assert model.permissions[1].interrupt is True
            assert frames[-1]["status"] == "awaiting-approval"
            assert frames[-1]["approval_granted_tool"] == PLATFORM_PUBLISH_TOOL_NAME
            assert gate.publication_title == "Publish corrected documentation"
            assert gate.publication_body == BODY
            assert len(calls) == (1 if initial_body == BODY else 0)

    anyio.run(go)


@pytest.mark.parametrize(
    ("status", "reply", "reason"),
    [
        (200, {"result": "metadata_changed"}, "metadata_only_unsupported"),
        (409, {"detail": {"code": "stale_context"}}, "stale_context"),
        (503, {"detail": "precheck_unavailable"}, "precheck_unavailable"),
    ],
)
def test_remote_refusal_is_actionable_and_does_not_park_the_turn(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    reply: dict[str, str],
    reason: str,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(status, reply)]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(gate, [("remote", {"title": TITLE, "body": BODY})])
            frames = await _run(runner, model, _event(_context(head, url)))
            _assert_refusal(model, gate, frames, reason)
            assert len(calls) == 1
            if reason == "metadata_only_unsupported":
                assert "file" in model.permissions[0].message.casefold()

    anyio.run(go)


def test_file_change_does_not_contact_unavailable_metadata_provider(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        repo, head = workspace
        (repo / "README.md").write_text("Actual working tree change.\n")
        async with _api([(503, {"detail": "precheck_unavailable"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(gate, [("files", {"title": TITLE, "body": BODY})])
            frames = await _run(runner, model, _event(_context(head, url)))
            assert frames[-1]["status"] == "awaiting-approval"
            assert gate.publication_title == TITLE
            assert gate.publication_body == BODY
            assert calls == []

    anyio.run(go)


def test_identical_arguments_with_a_new_call_id_require_fresh_truth(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        repo, head = workspace
        replies = [
            (200, {"result": "unchanged"}),
            (409, {"detail": {"code": "stale_context"}}),
        ]
        async with _api(replies) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(
                gate,
                [(call_id, {"title": TITLE, "body": BODY}) for call_id in ("first", "later")],
            )
            frames = await _run(runner, model, _event(_context(head, url)))
            assert len(calls) == 2
            assert "no_change" in model.permissions[0].message
            assert "stale_context" in model.permissions[1].message
            assert all(not decision.interrupt for decision in model.permissions)
            assert frames[-1]["status"] == "done"
            assert gate.pending_summary is None

    anyio.run(go)


def test_precheck_budget_counts_shared_calls_once_and_does_not_block_file_correction(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None

            def correct(index: int) -> None:
                if index == 6:
                    assert gate.pending_summary is None
                    (repo / "README.md").write_text("Working tree correction after refusals.\n")

            model = _PublicationModel(
                gate,
                [(f"call{index}", {"title": TITLE, "body": BODY}) for index in range(7)],
                before_call=correct,
            )
            frames = await _run(runner, model, _event(_context(head, url)))
            assert len(calls) == 5
            assert len(model.permissions) == 7
            assert all("no_change" in decision.message for decision in model.permissions[:5])
            assert "rate" in model.permissions[5].message.casefold()
            assert all(not decision.interrupt for decision in model.permissions[:6])
            assert model.permissions[6].interrupt is True
            assert frames[-1]["status"] == "awaiting-approval"

    anyio.run(go)


def test_local_commits_cannot_be_mistaken_for_an_unchanged_publication(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        repo, head = workspace
        (repo / "README.md").write_text("Local commit which is outside the working tree patch.\n")
        _git(repo, "add", "README.md")
        _git(
            repo,
            "-c",
            "user.name=Curie Test",
            "-c",
            "user.email=curie@example.com",
            "commit",
            "--quiet",
            "-m",
            "Local authored commit",
        )
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(gate, [("localcommit", {"title": TITLE, "body": BODY})])
            frames = await _run(runner, model, _event(_context(head, url)))
            assert model.permissions[0].interrupt is False
            assert "head" in model.permissions[0].message.casefold()
            assert frames[-1]["status"] == "done"
            assert gate.pending_summary is None
            assert gate.pending_halt is False
            assert calls == []

    anyio.run(go)


def test_warm_turn_replaces_context_and_drops_it_when_next_event_has_none(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        repo, head = workspace
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            runner = await _boot(tmp_path, repo, url, monkeypatch)
            gate = runner._approval_gate
            assert gate is not None
            model = _PublicationModel(gate, [("reusedid", {"title": TITLE, "body": BODY})])
            runner._factory = lambda: model
            await runner.start()
            try:
                first = [
                    json.loads(line) async for line in runner.run_turn(_event(_context(head, url)))
                ]
                fresh = _context(head, url, capability="ppc.fresh.publicationcapability")
                fresh["runtime_epoch"] = 3
                second = [json.loads(line) async for line in runner.run_turn(_event(fresh))]
                third = [json.loads(line) async for line in runner.run_turn(_event(None))]
            finally:
                await runner.close()
            assert first[-1]["status"] == second[-1]["status"] == "done"
            assert third[-1]["status"] == "awaiting-approval"
            assert len(model.queries) == 3
            assert [call["capability"] for call in calls] == [CAPABILITY, fresh["capability"]]
            assert gate.publication_title == TITLE

    anyio.run(go)


def test_fake_permission_callback_receives_the_stream_tool_use_id() -> None:
    async def go() -> None:
        seen: list[str | None] = []

        async def refuse(_name: str, _arguments: dict[str, Any], context: ToolPermissionContext):
            seen.append(context.tool_use_id)
            return PermissionResultDeny(message="no_change: change a file first", interrupt=False)

        block = ToolUseBlock(
            id="publicationcallidentity",
            name=PLATFORM_PUBLISH_TOOL_NAME,
            input={"title": TITLE, "body": BODY},
        )
        model = FakeModelSession(
            lambda: [AssistantMessage(content=[block], model="fake"), _success()],
            can_use_tool=refuse,
        )
        await model.connect()
        try:
            await model.query("Publish changes")
            messages = [message async for message in model.receive_turn()]
        finally:
            await model.close()
        assert seen == [block.id]
        assert isinstance(messages[-1], ResultMessage)

    anyio.run(go)


@pytest.mark.skipif(which("claude") is None, reason="local Claude CLI is unavailable")
def test_real_sdk_publication_call_shares_stream_hook_and_permission_identity(
    tmp_path: Path,
    workspace: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PublicationProvider(_ProviderCapture):
        def __init__(self) -> None:
            super().__init__()
            self.message_count = 0
            self.offered_publication = False

        async def respond(self, request: Request) -> StreamingResponse:
            self.message_count += 1
            names = self._names(await request.json())
            self.tool_lists.append(names)
            # Setup requests may arrive before the platform tool is available.
            # Offer one call only after the real request advertises that tool.
            if PLATFORM_PUBLISH_TOOL_NAME in names and not self.offered_publication:
                self.offered_publication = True
                frames = _tool_use_frames(
                    PLATFORM_PUBLISH_TOOL_NAME, {"title": TITLE, "body": BODY}
                )
            else:
                frames = _text_frames("The publication request was refused.")
            return StreamingResponse(_sse(frames), media_type="text/event-stream")

    stream_ids: list[str] = []
    hook_ids: list[str | None] = []
    permission_ids: list[str | None] = []
    hook_results: list[dict[str, Any]] = []
    permission_results: list[PermissionResultDeny] = []
    order: list[str] = []

    real_build_hook = boot.build_approval_hook
    real_build_permission = boot.build_can_use_tool

    def observe_hook(gate: ApprovalGate):
        matchers = real_build_hook(gate)
        callback = matchers["PreToolUse"][0].hooks[0]

        async def capture(hook_input: Any, tool_use_id: str | None, context: Any):
            result = await callback(hook_input, tool_use_id, context)
            tool_name = (
                hook_input.get("tool_name")
                if isinstance(hook_input, dict)
                else getattr(hook_input, "tool_name", None)
            )
            if tool_name != PLATFORM_PUBLISH_TOOL_NAME:
                return result
            order.append("hook")
            hook_ids.append(tool_use_id)
            hook_results.append(result)
            # Let the SDK reach can_use_tool after the real hook has made its
            # decision so all three SDK identifiers are observable in one call.
            return {}

        matchers["PreToolUse"][0].hooks[0] = capture
        return matchers

    def observe_permission(gate: ApprovalGate):
        callback = real_build_permission(gate)

        async def capture(name: str, args: dict[str, Any], context: ToolPermissionContext):
            result = await callback(name, args, context)
            if name == PLATFORM_PUBLISH_TOOL_NAME:
                order.append("permission")
                permission_ids.append(context.tool_use_id)
                assert isinstance(result, PermissionResultDeny)
                permission_results.append(result)
            return result

        return capture

    class ObservingSession(boot.ClaudeAgentSession):
        def receive_turn(self):
            upstream = super().receive_turn()

            async def observe():
                async for message in upstream:
                    if isinstance(message, AssistantMessage):
                        for block in message.content:
                            if (
                                isinstance(block, ToolUseBlock)
                                and block.name == PLATFORM_PUBLISH_TOOL_NAME
                            ):
                                order.append("stream")
                                stream_ids.append(block.id)
                    yield message

            return observe()

    monkeypatch.setattr(boot, "build_approval_hook", observe_hook)
    monkeypatch.setattr(boot, "build_can_use_tool", observe_permission)
    monkeypatch.setattr(boot, "ClaudeAgentSession", ObservingSession)

    async def go() -> None:
        repo, head = workspace
        provider = PublicationProvider()
        async with _api([(200, {"result": "unchanged"})]) as (url, calls):
            with _serve(provider.app()) as provider_url:
                runner = await _boot(
                    tmp_path,
                    repo,
                    url,
                    monkeypatch,
                    sdk_env=_sdk_env(provider_url, tmp_path / "claude-publication"),
                )
                gate = runner._approval_gate
                assert gate is not None
                await runner.start()
                try:
                    with anyio.fail_after(45):
                        frames = [
                            json.loads(line)
                            async for line in runner.run_turn(_event(_context(head, url)))
                        ]
                finally:
                    await runner.close()

        assert provider.message_count >= 2
        assert provider.offered_publication is True
        assert stream_ids == ["toolu_loopback_1"]
        assert hook_ids == stream_ids
        assert permission_ids == stream_ids
        assert order == ["stream", "hook", "permission"]
        assert "no_change" in hook_results[0]["hookSpecificOutput"][
            "permissionDecisionReason"
        ]
        assert "no_change" in permission_results[0].message
        assert permission_results[0].interrupt is False
        assert len(calls) == 1
        assert calls[0]["body"]["proposed_title"] == TITLE
        assert calls[0]["body"]["proposed_body"] == BODY
        assert frames[-1]["status"] == "done"
        assert not any(frame["type"] == "error" for frame in frames)
        assert gate.pending_summary is None
        assert gate.pending_granted_tool is None
        assert gate.pending_halt is False

    anyio.run(go)
