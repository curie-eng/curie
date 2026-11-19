"""LIVE: the dark factory proposes a learned rule only for general feedback (#5796).

ADR 0287 decision 11's graded check, with a falsifiable control. Each case
boots a fresh real runner session with the dark-factory bundle on a real model
through OpenRouter, the factory's issue read capability pointed at a local
stand-in for the platform API. The stand-in answers the completion contract as
an implementation contract offering a rule proposal (``rule_proposal: true``),
answers the issue read with a small fixed issue, and records every request to
``/work-items/rule-proposal``, answering ``{"status": "candidate"}``.

One revision turn is sent, framed as the factory frames reviewer feedback on
its pull request. The grade is the real tool call and the stand-in's record,
never the answer text:

1. ``general``: feedback that states a rule for the codebase. Passes only when
   the turn's tool trajectory (its ``tool_note`` frames) holds ``propose_rule``
   and the stand-in recorded exactly one POST whose JSON body has exactly the
   keys ``rule`` and ``reason``.
2. ``one_off`` (the control): feedback asking for a one-off change. Passes
   only when the trajectory holds no ``propose_rule`` and nothing was posted.

Skipped unless ``CURIE_E2E_LIVE=1``. Under it, ``OPENROUTER_API_KEY`` is
required and ``OPENROUTER_MODEL`` picks the model (default
``anthropic/claude-sonnet-4.5``). Cost is bounded per session by a low turn
cap and a small USD budget; the code-changing and subagent tools are removed
from the catalogue, so a turn only reads and decides.
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import pytest
from aci_protocol import Event, Final, ToolNote, parse_ndjson_line
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import __main__ as boot
from curie_runner.config import RunnerConfig
from curie_runner.issue_read import CAPABILITY_HEADER, ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV

_LIVE_REQUESTED = os.environ.get("CURIE_E2E_LIVE") == "1"
_OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
_DEFAULT_MODEL = "anthropic/claude-sonnet-4.5"

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_SOURCE = REPO_ROOT / "examples" / "dark-factory"

CAPABILITY = "wir.live-rule-proposal.signature"
ISSUE_READ_PATH = "/work-items/issue-read"
CONTRACT_PATH = "/work-items/completion-contract"
RULE_PATH = "/work-items/rule-proposal"

# The cost bounds of one session: the SDK stops at this spend or turn count.
SESSION_USD_BUDGET = 0.75
MAX_TURNS = 12
TURN_TIMEOUT_SECONDS = 600

GENERAL_FEEDBACK = (
    "Use the shared parse_timestamp helper in cli/src/time.rs; every CLI flag that "
    "takes a time goes through it."
)
ONE_OFF_FEEDBACK = "Rename this variable to start_at."

REPOSITORY = "example-org/example-cli"
ISSUE_URL = f"https://github.com/{REPOSITORY}/issues/12"
PR_URL = f"https://github.com/{REPOSITORY}/pull/14"
PULL_REQUEST_FOLLOW_UP_MARKER = "Curie pull request follow-up."

ISSUE_TICKET = (
    "# Add a --since flag to the list command (#12)\n\n"
    "The `list` command should accept `--since <time>` and show only entries "
    "created at or after that time.\n\n"
    "## Acceptance criteria\n\n"
    "1. `list --since 2026-01-01T00:00:00Z` shows only entries created at or after it.\n"
    "2. An unparseable time is refused with a clear error.\n"
)
PULL_REQUEST_BODY = (
    "Add a --since flag to the list command.\n\nCloses #12\n\n"
    "## Decisions\n\n1. The flag parses its value in cli/src/list.rs.\n"
)

# The workspace the bundle's review gate fingerprints: a tiny checkout whose
# files make both pieces of feedback plausible.
WORKSPACE_FILES = {
    "cli/src/time.rs": (
        "/// Parses every time a CLI flag takes.\n"
        "pub fn parse_timestamp(value: &str) -> Result<i64, String> {\n"
        '    value.parse::<i64>().map_err(|_| format!("not a time: {value}"))\n'
        "}\n"
    ),
    "cli/src/list.rs": (
        "pub fn since(value: &str) -> Result<i64, String> {\n"
        "    let since_time = value.parse::<i64>().map_err(|e| e.to_string())?;\n"
        "    Ok(since_time)\n"
        "}\n"
    ),
}


class _PlatformStub:
    """The platform routes a revision run reaches, recording rule proposals."""

    def __init__(self) -> None:
        self.contract_calls = 0
        self.rule_posts: list[tuple[Any, str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def contract(request: web.Request) -> web.Response:
            await request.read()
            self.contract_calls += 1
            # No execution binding: the start waits on no requester envelope.
            return web.json_response(
                {
                    "completion_contract": "implementation",
                    "result_kinds": [],
                    "session_usd_allowance": None,
                    "execution": None,
                    "rule_proposal": True,
                }
            )

        async def issue(request: web.Request) -> web.Response:
            await request.read()
            deadline = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=2)
            return web.json_response(
                {
                    "url": ISSUE_URL,
                    "ticket": ISSUE_TICKET,
                    "execution_deadline": deadline.isoformat(timespec="seconds"),
                    "execution_seconds": 7200,
                }
            )

        async def rule(request: web.Request) -> web.Response:
            raw = await request.read()
            try:
                body: Any = json.loads(raw)
            except ValueError:
                body = None
            self.rule_posts.append((body, request.headers.get(CAPABILITY_HEADER)))
            return web.json_response({"status": "candidate"})

        app.router.add_post(CONTRACT_PATH, contract)
        app.router.add_post(ISSUE_READ_PATH, issue)
        app.router.add_post(RULE_PATH, rule)
        return app


def _workspace(root: Path) -> Path:
    workspace = root / "workspace"
    for name, text in WORKSPACE_FILES.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    git = [
        "git",
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "commit.gpgsign=false",
    ]
    for command in (
        [*git, "init", "-q", "-b", "main"],
        [*git, "add", "-A"],
        [*git, "commit", "-q", "-m", "fixture"],
    ):
        subprocess.run(command, cwd=workspace, check=True, timeout=60)
    return workspace


def _revision_message(feedback: str) -> str:
    """The revision turn as the factory frames reviewer feedback, plus the cost bound."""

    provenance = {
        "event": "pull_request_review_comment",
        "url": f"{PR_URL}#discussion_r1001",
        "sender": "example-reviewer",
        "body": feedback,
        "path": "cli/src/list.rs",
        "line": 2,
        "review_id": 2001,
        "pull_request_body": PULL_REQUEST_BODY,
    }
    return (
        f"{PR_URL}\n\n{PULL_REQUEST_FOLLOW_UP_MARKER}\n"
        "Human GitHub review feedback on this work item's pull request follows as JSON. "
        "Answer from the pull request body's Decisions section and the current diff first. "
        "Start the revision pipeline only when the comment asks for a change. "
        "The feedback may ask a question or request a change. Answer questions directly and "
        "revise the same pull request when a change is needed. The pull_request_body, when "
        "present, records the last successfully published description.\n"
        + json.dumps(provenance, ensure_ascii=False)
        + "\n\nThis session is a short rehearsal of the start of the revision: the code "
        "change itself happens in a later session. Read the issue and the feedback and do "
        "what the skill asks of a revision before any code changes, but do not edit "
        "files, run commands, start reviewers or publish. Then end your turn with one "
        "sentence saying what the revision will change."
    )


def _require_credential() -> None:
    if not _OPENROUTER_KEY:
        pytest.fail(
            "CURIE_E2E_LIVE=1 rule proposal proof requires OPENROUTER_API_KEY "
            "(and optionally OPENROUTER_MODEL)"
        )


def _drive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, feedback: str
) -> tuple[list[ToolNote], Final, _PlatformStub]:
    from curie_runner.sdk_auth import CREDENTIALS_ENV, resolve_model_credential

    _require_credential()
    stub = _PlatformStub()
    bundle = tmp_path / "dark-factory"
    shutil.copytree(BUNDLE_SOURCE, bundle)
    workspace = _workspace(tmp_path)
    model = os.environ.get("OPENROUTER_MODEL", _DEFAULT_MODEL)
    sdk_env: dict[str, str] = {CREDENTIALS_ENV: _OPENROUTER_KEY}
    resolve_model_credential(sdk_env)
    sdk_env.update(
        {
            "DARK_FACTORY_WORKSPACE": str(workspace),
            "DARK_FACTORY_STATE_DIR": str(tmp_path / "review-state"),
            "DARK_FACTORY_PROGRESS_LOG": str(tmp_path / "review-progress.log"),
        }
    )
    session_id = f"rule-proposal-{uuid4()}"
    # The CLI and the bundle's hook work in the fixture checkout.
    monkeypatch.chdir(workspace)

    async def go() -> tuple[list[ToolNote], Final]:
        async with TestServer(stub.app(), host="127.0.0.1") as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, CAPABILITY)
            config = RunnerConfig.from_env(
                {
                    "CURIE_PLUGIN_DIR": str(bundle),
                    "CURIE_SESSION_ID": session_id,
                    "CURIE_SANDBOX_ID": f"sandbox-{session_id}",
                    "CURIE_MODEL": model,
                    "CURIE_MAX_TURNS": str(MAX_TURNS),
                    "CURIE_BUDGET": json.dumps(
                        {
                            "max_output_tokens_per_run": 20000,
                            "max_usd_per_day": SESSION_USD_BUDGET,
                        }
                    ),
                    # A turn only reads and decides: no subagent, no edit, no
                    # command, so the session stays cheap and the checkout
                    # unchanged.
                    "CURIE_DISALLOWED_TOOLS": "Agent,Task,Bash,Edit,Write,MultiEdit,NotebookEdit",
                }
            )
            runner = await anyio.to_thread.run_sync(
                lambda: boot.build_runner(config, sdk_env=sdk_env)
            )
            await runner.start()
            notes: list[ToolNote] = []
            final: Final | None = None
            try:
                with anyio.fail_after(TURN_TIMEOUT_SECONDS):
                    async for line in runner.run_turn(
                        Event(
                            type="message",
                            text=_revision_message(feedback),
                            user="U0EXAMPLE1",
                            ts="1.0",
                        )
                    ):
                        parsed = parse_ndjson_line(line)
                        if isinstance(parsed, ToolNote):
                            notes.append(parsed)
                        elif isinstance(parsed, Final):
                            final = parsed
            finally:
                await runner.close()
            assert final is not None, "the real turn emitted no terminal final"
            return notes, final

    notes, final = anyio.run(go)
    trajectory = [note.tool for note in notes]
    print(
        f"rule proposal live model={model} trajectory={trajectory} "
        f"status={final.status.value} input_tokens={final.input_tokens} "
        f"output_tokens={final.output_tokens} posts={len(stub.rule_posts)}"
    )
    assert stub.contract_calls == 1, "the boot did not read the completion contract once"
    # A provider error before any tool runs would pass the control vacuously.
    assert not (final.text or "").startswith("API Error"), final.text
    return notes, final, stub


def _called_propose_rule(notes: list[ToolNote]) -> bool:
    return any((note.tool or "").endswith("__propose_rule") for note in notes)


@pytest.mark.skipif(not _LIVE_REQUESTED, reason="set CURIE_E2E_LIVE=1 for the live rule proposal")
@pytest.mark.parametrize(
    ("feedback", "proposes"),
    [
        pytest.param(GENERAL_FEEDBACK, True, id="general"),
        pytest.param(ONE_OFF_FEEDBACK, False, id="one_off"),
    ],
)
def test_live_revision_proposes_a_rule_only_for_general_feedback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, feedback: str, proposes: bool
) -> None:
    notes, _final, stub = _drive(tmp_path, monkeypatch, feedback)
    trajectory = [note.tool for note in notes]
    if not proposes:
        # The control must have run the factory, not stopped before any tool.
        assert notes, "the control turn made no tool call at all"
        assert not _called_propose_rule(notes), trajectory
        assert stub.rule_posts == []
        return
    assert _called_propose_rule(notes), trajectory
    assert len(stub.rule_posts) == 1, len(stub.rule_posts)
    body, capability = stub.rule_posts[0]
    assert isinstance(body, dict)
    assert set(body) == {"rule", "reason"}
    assert all(isinstance(value, str) and value.strip() for value in body.values())
    assert capability == CAPABILITY
