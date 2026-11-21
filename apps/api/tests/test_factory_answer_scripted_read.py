"""A scripted SDK turn reads an admitted answer from the TLS tracker fixture."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from secrets import token_urlsafe
from types import ModuleType
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import ClaudeAgentOptions, ToolResultBlock, UserMessage, query
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_runner.harness.claude.approval import build_approval_server
from curie_runner.issue_read import CAPABILITY_HEADER, build_issue_tool
from curie_runner.subprocess_env import model_credential_names
from fastapi.testclient import TestClient
from forge_fakes.github import REPO, _issue_event, _post, factory_env
from forge_fakes.github_comments import _rows
from test_factory_terminus import _start_running
from test_factory_tracker_answer_intake import _finish, _mint, _post_questions

ROOT = Path(__file__).resolve().parents[3]
ISSUE_NUMBER = 3815
PROMPT = "Read the factory issue."
ANSWER = "Retain the header row.\n\nKeep the existing filename."
TOOL = "mcp__curie__get_issue"


def _load(name: str, path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


async def _run_tool(client: TestClient, token: str, endpoint: str, workspace: Path) -> Any:
    async def forward(request: web.Request) -> web.Response:
        response = await asyncio.to_thread(
            client.post,
            request.path,
            json=await request.json(),
            headers={CAPABILITY_HEADER: request.headers[CAPABILITY_HEADER]},
        )
        return web.Response(body=response.content, status=response.status_code)

    app = web.Application()
    read_path = "/work-items/issue-read"
    app.router.add_post(read_path, forward)
    async with TestServer(app) as server, asyncio.timeout(30):
        tool = build_issue_tool(str(server.make_url(read_path)), token)
        options = ClaudeAgentOptions(
            cwd=workspace,
            setting_sources=[],
            model="claude-sonnet-4-6",
            mcp_servers={"curie": build_approval_server(issue_tool=tool)},
            strict_mcp_config=True,
            tools=[],
            allowed_tools=[TOOL],
            max_turns=2,
            env={
                "ANTHROPIC_BASE_URL": endpoint,
                "ANTHROPIC_AUTH_TOKEN": token_urlsafe(16),
                "CLAUDE_CONFIG_DIR": str(workspace / "cli"),
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                "ENABLE_TOOL_SEARCH": "false",
            },
        )
        results = []
        async for message in query(prompt=PROMPT, options=options):
            if isinstance(message, UserMessage) and isinstance(message.content, list):
                results.extend(
                    block for block in message.content if isinstance(block, ToolResultBlock)
                )
    [result] = results
    assert not result.is_error
    assert isinstance(result.content, list)
    [content] = result.content
    return json.loads(content["text"])


def test_scripted_turn_reads_only_the_admitted_answer_as_authority(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for key in model_credential_names(os.environ) | frozenset(
        key for key in os.environ if key.startswith("CLAUDE_CODE_USE_")
    ):
        monkeypatch.delenv(key, raising=False)
    github = _load("factory_answer_github_stub", "tools/github-stub/github_stub.py")
    scripted = _load("factory_answer_model_script", "tools/model-script/model_script.py")
    request_key = scripted.request_key

    def fixture_key(request: dict[str, Any]) -> tuple[str, int] | None:
        # The SDK prepends environment reminders with the run's path and clock.
        key = request_key(request)
        if key is not None and key[0].endswith(f"\n\n{PROMPT}"):
            return PROMPT, key[1]
        return key

    scripted.request_key = fixture_key
    fixture_dir = ROOT / "tools/factory-e2e/fixtures"
    recording = ROOT / "tools/github-stub/recordings/curie-pr-3400.json"
    stub = github.GithubStub(
        tmp_path / "github",
        json.loads(recording.read_text()),
        seed_tree=fixture_dir / "unitconv",
        issue_file=fixture_dir / "unitconv-issue.md",
    )
    transcript = tmp_path / "model.json"
    exchanges = []
    for index, (content, stop) in enumerate(
        (
            (
                [{"type": "tool_use", "id": "toolu_scripted_read", "name": TOOL, "input": {}}],
                "tool_use",
            ),
            ([{"type": "text", "text": "Issue read complete."}], "end_turn"),
        )
    ):
        exchanges.append(
            {
                "lane_key": PROMPT,
                "ordinal": index,
                "message": {"id": f"msg_scripted_{index}", "content": content, "stop_reason": stop},
            }
        )
    scripted.dump_transcript(transcript, exchanges)
    model = scripted.ModelScript(transcript)
    try:
        stub.start()
        model.start()
        pem = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
        config = factory_env(
            tmp_path / "app",
            intake="webhook",
            api_url=f"{stub.base_url}/api/v3",
            label="factory",
            private_key=pem,
        )
        config.update(
            CURIE_CODE_HOST_CA_BUNDLE=str(stub.ca_file),
            INTERNAL_WORKER_TOKEN=token_urlsafe(16),
            GITHUB_CLONE_BASE=stub.base_url,
            GITHUB_APP_PRIVATE_KEY=pem,
        )
        for key, value in config.items():
            monkeypatch.setenv(key, value)
        get_settings.cache_clear()
        with TestClient(create_app()) as client:
            created = client.post(
                "/agents",
                headers={"X-API-Key": get_settings().api_key},
                json={
                    "name": "acme-factory-answers",
                    "repo_full_name": REPO,
                    "channel": {"kind": "github", "address": REPO},
                },
            )
            assert created.status_code == 201, created.text
            admitted = _post(
                client,
                "issues",
                _issue_event(
                    "labeled",
                    ISSUE_NUMBER,
                    sender=github.HUMAN,
                    label={"name": "factory"},
                ),
            )
            assert admitted.status_code == 200, admitted.text
            assert admitted.json().get("status") == "factory_admitted", admitted.text
            [asking] = _rows("SELECT id FROM curie.execution_requests")
            epoch = _start_running(client, asking["id"])
            asked = _post_questions(client, _mint(client, asking["id"]))
            assert asked.status_code == 200, asked.text
            _finish(client, asking["id"], epoch)
            writer = stub.append_comment(ISSUE_NUMBER, ANSWER, author=github.HUMAN)
            reader = stub.add_user({"id": 6620, "login": "reader", "type": "User"}, "read")
            shadow = stub.append_comment(ISSUE_NUMBER, ANSWER, author=reader)
            for comment in (writer, shadow):
                response = _post(
                    client,
                    "issue_comment",
                    _issue_event(
                        "created",
                        ISSUE_NUMBER,
                        sender=comment["user"],
                        comment=comment,
                    ),
                )
                assert response.status_code == 200, response.text
            [attempt] = _rows("SELECT id FROM curie.execution_requests WHERE sequence = 2")
            _start_running(client, attempt["id"])
            result = asyncio.run(
                _run_tool(
                    client,
                    _mint(client, attempt["id"]),
                    model.base_url,
                    tmp_path,
                )
            )
            assert result["answer_attempt"] is True
            assert result["admitted_answers"] == [
                {
                    "comment_id": str(writer["id"]),
                    "author_id": str(github.HUMAN["id"]),
                    "text": ANSWER,
                }
            ]
            assert result["thread_context"].count(ANSWER) == 2
    finally:
        try:
            model.close()
        finally:
            stub.close()
            get_settings.cache_clear()
