"""Requester coding instructions reach the runner before the first turn (ADR 0278, #5592).

The worker posts a version 1 context envelope; the runner validates it against
the execution binding from its completion contract, acknowledges it, and only
then builds the model session. Every case pairs a positive control with a
negative one.

Prompt placement is checked two ways. The fresh and resumed turn cases compose
the system prompt with ``_compose_system_prompt``, the function the runner's
boot uses, because a resumed turn from a native checkpoint needs a second boot
with a transcript store that the boot path cannot be handed directly. The boot
cases then drive the real ``build_runner`` behind ``create_app``: the envelope
and the turn both arrive over HTTP with the per-claim bearer, the completion
contract comes from a local API, and the real Claude Code CLI talks to a local
stand-in Messages API. No credential and no network: the API key is a
placeholder and every URL is loopback.

Section bytes are always compared against ``requester_section`` output, never
against hand-written prompt prose.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Final, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from curie_internal.requester_coding_instructions import MAX_BYTES, text_sha256
from curie_runner import RunnerConfig, RunTracer, SideEffectClassifier, create_app
from curie_runner import requester_context as rc
from curie_runner.__main__ import _compose_system_prompt, build_runner
from curie_runner.adapter import (
    ClaudeAgentSession,
    build_options,
    build_structured_resume,
    model_message_to_conversation,
)
from curie_runner.fake import FakeModelSession
from curie_runner.final_answer import FINAL_ANSWER_HEADER
from curie_runner.history import ConversationMessage, HarnessReplayState
from curie_runner.issue_read import ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.redact import install_stdout_redaction
from curie_runner.requester_context import (
    BLOCKED_EXPLANATION,
    CLOSE_PREFIX,
    CONTEXT_PATH,
    MAX_ENVELOPE_BYTES,
    OPEN_PREFIX,
    Applied,
    EnvelopeRefused,
    ExecutionBinding,
    Failed,
    NoneSelected,
    RequesterContextGate,
    acknowledgment,
    attach_gate,
    build_subagent_guard,
    close_line,
    open_line,
    parse_envelope,
    private_fragments,
    requester_section,
    subagent_prompt_refusal,
)
from curie_runner.session import SessionRunner
from personal_auth_support import requires_cli

from .test_resumed_turn_system_prompt import _BODIES, _messages, _system_text

TENANT = "0b6f6a52-2d4e-4f0a-9a43-3f1d8f6c1a01"
WORK_ITEM = "5d0e3c9b-8a7f-4b62-9e15-6c2a7b9d4e02"
EXECUTION_REQUEST = "9a1c2e4f-6b8d-4e0a-8c3f-1d5e7a9b2c03"
PRINCIPAL = "3e7f9a1b-2c4d-4e6f-8a0b-1c3d5e7f9a04"
OTHER_ID = "7f8e9d0c-1b2a-4c3d-9e8f-0a1b2c3d4e05"
FENCE = 3
BINDING = ExecutionBinding(
    tenant_id=TENANT,
    work_item_id=WORK_ITEM,
    execution_request_id=EXECUTION_REQUEST,
    execution_fence=FENCE,
)
TEXT = (
    "Prefer small focused commits with clear messages.\n"
    "Name each test after the behavior it proves.\n"
    "Keep module imports grouped and sorted."
)
LINES = tuple(TEXT.splitlines())
DIGEST = text_sha256(TEXT)
TOKEN = "per-claim-bearer-placeholder"
BEARER = {"Authorization": f"Bearer {TOKEN}"}
MODEL = "claude-sonnet-5"
BUNDLE_PROMPT = "You implement one issue in the mounted repository."


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _selected(text: str = TEXT, **overrides: Any) -> dict[str, Any]:
    selection: dict[str, Any] = {
        "kind": "selected",
        "principal_id": PRINCIPAL,
        "revision": 1,
        "sha256": _sha(text),
        "format_version": 1,
        "text": text,
    }
    selection.update(overrides)
    return selection


def _envelope(selection: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "envelope_version": 1,
        "tenant_id": TENANT,
        "work_item_id": WORK_ITEM,
        "execution_request_id": EXECUTION_REQUEST,
        "execution_fence": FENCE,
        "selection": _selected() if selection is None else selection,
    }
    envelope.update(overrides)
    return envelope


def _raw(envelope: Any) -> bytes:
    return json.dumps(envelope).encode("utf-8")


VALID = _raw(_envelope())
ABSENT = _raw(_envelope({"kind": "absent", "reason": "no_profile"}))


def _refusal(raw: bytes) -> str:
    with pytest.raises(EnvelopeRefused) as refused:
        parse_envelope(raw, BINDING)
    return refused.value.reason


# --- 1. parse_envelope ---------------------------------------------------------------


def test_a_valid_selected_envelope_is_applied_and_an_absent_one_is_none_selected() -> None:
    assert parse_envelope(VALID, BINDING) == Applied(
        selection_id=WORK_ITEM, sha256=DIGEST, text=TEXT
    )
    assert parse_envelope(ABSENT, BINDING) == NoneSelected(reason="no_profile")
    # Negative: an absent selection with an unknown reason is not a valid absence.
    unknown = _raw(_envelope({"kind": "absent", "reason": "not_a_reason"}))
    assert _refusal(unknown) == "malformed_envelope"


_REFUSALS: tuple[tuple[str, str, Callable[[], bytes]], ...] = (
    (
        "too_large",
        "envelope-over-bound",
        lambda: _raw(_envelope(padding="x" * MAX_ENVELOPE_BYTES)),
    ),
    ("too_large", "text-over-bound", lambda: _raw(_envelope(_selected("a" * (MAX_BYTES + 1))))),
    ("malformed_envelope", "not-json", lambda: b'{"envelope_version": 1'),
    ("malformed_envelope", "not-utf8", lambda: b"\xff\xfe{}"),
    ("malformed_envelope", "not-an-object", lambda: _raw([_envelope()])),
    (
        "malformed_envelope",
        "text-not-a-string",
        lambda: _raw(_envelope({**_selected(), "text": 7})),
    ),
    (
        "malformed_envelope",
        "missing-principal",
        lambda: _raw(_envelope({k: v for k, v in _selected().items() if k != "principal_id"})),
    ),
    ("unsupported_version", "version-2", lambda: _raw(_envelope(envelope_version=2))),
    ("unsupported_version", "version-string", lambda: _raw(_envelope(envelope_version="1"))),
    ("unsupported_version", "version-bool", lambda: _raw(_envelope(envelope_version=True))),
    ("wrong_execution", "tenant", lambda: _raw(_envelope(tenant_id=OTHER_ID))),
    ("wrong_execution", "work-item", lambda: _raw(_envelope(work_item_id=OTHER_ID))),
    ("wrong_execution", "request", lambda: _raw(_envelope(execution_request_id=OTHER_ID))),
    ("wrong_fence", "older-fence", lambda: _raw(_envelope(execution_fence=FENCE - 1))),
    ("wrong_fence", "fence-string", lambda: _raw(_envelope(execution_fence=str(FENCE)))),
    ("unsupported_format", "format-2", lambda: _raw(_envelope(_selected(format_version=2)))),
    (
        "digest_mismatch",
        "other-text-digest",
        lambda: _raw(_envelope(_selected(sha256=_sha(TEXT + " ")))),
    ),
    (
        "digest_mismatch",
        "uppercase-digest",
        lambda: _raw(_envelope(_selected(sha256=DIGEST.upper()))),
    ),
)


@pytest.mark.parametrize(
    ("reason", "build"),
    [pytest.param(reason, build, id=f"{reason}-{name}") for reason, name, build in _REFUSALS],
)
def test_each_refusal_reason_is_raised_and_the_valid_control_is_applied(
    reason: str, build: Callable[[], bytes]
) -> None:
    assert isinstance(parse_envelope(VALID, BINDING), Applied)
    assert _refusal(build()) == reason


def test_the_text_bound_is_utf8_bytes_not_characters() -> None:
    exact = "a" * MAX_BYTES
    assert parse_envelope(_raw(_envelope(_selected(exact))), BINDING) == Applied(
        selection_id=WORK_ITEM, sha256=_sha(exact), text=exact
    )
    assert _refusal(_raw(_envelope(_selected("a" * (MAX_BYTES + 1))))) == "too_large"

    # Two bytes per character: exactly at the bound is accepted ...
    two_byte_exact = "é" * (MAX_BYTES // 2)
    assert len(two_byte_exact.encode("utf-8")) == MAX_BYTES
    assert isinstance(parse_envelope(_raw(_envelope(_selected(two_byte_exact))), BINDING), Applied)
    # ... and one character more is under the limit in characters but over it in bytes.
    two_byte_over = "é" * (MAX_BYTES // 2 + 1)
    assert len(two_byte_over) < MAX_BYTES < len(two_byte_over.encode("utf-8"))
    assert _refusal(_raw(_envelope(_selected(two_byte_over)))) == "too_large"


# --- 2. acknowledgment ---------------------------------------------------------------


def _ack(outcome: str, **extra: Any) -> dict[str, Any]:
    return {
        "envelope_version": 1,
        "work_item_id": WORK_ITEM,
        "execution_request_id": EXECUTION_REQUEST,
        "execution_fence": FENCE,
        "outcome": outcome,
        **extra,
    }


def test_acknowledgment_shapes_echo_the_binding_and_fence() -> None:
    applied = acknowledgment(BINDING, Applied(selection_id=WORK_ITEM, sha256=DIGEST, text=TEXT))
    assert applied == {"acknowledgment": _ack("applied", sha256=DIGEST)}

    none = acknowledgment(BINDING, NoneSelected(reason="no_profile"))
    assert none == {"acknowledgment": _ack("none")}
    assert "sha256" not in none["acknowledgment"]
    assert "reason" not in none

    failed = acknowledgment(BINDING, Failed("digest_mismatch"))
    assert failed == {"acknowledgment": _ack("failed"), "reason": "digest_mismatch"}

    # No acknowledgment carries the text, whatever the outcome.
    for body in (applied, none, failed):
        assert TEXT not in json.dumps(body)


# --- 3. RequesterContextGate ---------------------------------------------------------


def test_delivery_before_bind_is_not_expected_and_after_bind_is_applied() -> None:
    async def go() -> None:
        unbound = RequesterContextGate()
        assert await unbound.deliver(VALID, bind_deadline_s=0.05) == (
            409,
            {"error": "requester_context_not_expected"},
        )
        assert unbound.outcome is None

        # An envelope posted while the start is still in its preflight waits
        # for the binding, by default far longer than the start waits for it.
        assert rc.BIND_DEADLINE_SECONDS > rc.DELIVERY_DEADLINE_SECONDS
        early = RequesterContextGate()
        posted = anyio.Event()
        answers: list[tuple[int, dict[str, Any]]] = []

        async def post() -> None:
            posted.set()
            answers.append(await early.deliver(VALID))

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(post)
            await posted.wait()
            await anyio.wait_all_tasks_blocked()
            assert answers == []
            early.bind(BINDING)
        assert answers == [(200, {"acknowledgment": _ack("applied", sha256=DIGEST)})]

        bound = RequesterContextGate()
        bound.bind(BINDING)
        assert await bound.deliver(VALID, bind_deadline_s=0.05) == (
            200,
            {"acknowledgment": _ack("applied", sha256=DIGEST)},
        )
        assert bound.applied == Applied(selection_id=WORK_ITEM, sha256=DIGEST, text=TEXT)

    anyio.run(go)


def test_bind_none_expects_nothing() -> None:
    async def go() -> None:
        gate = RequesterContextGate()
        gate.bind(None)
        assert gate.bound
        assert await gate.deliver(VALID) == (409, {"error": "requester_context_not_expected"})
        assert gate.outcome is None and gate.applied is None

        control = RequesterContextGate()
        control.bind(BINDING)
        status, _ = await control.deliver(VALID)
        assert status == 200

    anyio.run(go)


def test_identical_bytes_are_idempotent_and_different_bytes_are_refused() -> None:
    async def go() -> None:
        gate = RequesterContextGate()
        gate.bind(BINDING)
        first = await gate.deliver(VALID)
        assert first == (200, {"acknowledgment": _ack("applied", sha256=DIGEST)})
        assert await gate.deliver(VALID) == first

        conflict = (409, {"error": "requester_context_already_delivered"})
        assert await gate.deliver(ABSENT) == conflict
        # Same JSON value, different bytes: still a different delivery.
        assert await gate.deliver(VALID + b" ") == conflict
        assert await gate.deliver(VALID, oversized=True) == conflict
        assert gate.applied == Applied(selection_id=WORK_ITEM, sha256=DIGEST, text=TEXT)

        # A refused delivery is decided too: its own bytes repeat, others conflict.
        refused = RequesterContextGate()
        refused.bind(BINDING)
        bad = _raw(_envelope(execution_fence=FENCE + 1))
        failed = (200, {"acknowledgment": _ack("failed"), "reason": "wrong_fence"})
        assert await refused.deliver(bad) == failed
        assert await refused.deliver(bad) == failed
        assert await refused.deliver(VALID) == conflict
        assert refused.outcome == Failed("wrong_fence")

    anyio.run(go)


def test_wait_without_delivery_is_missing_envelope() -> None:
    async def go() -> None:
        gate = RequesterContextGate()
        gate.bind(BINDING)
        started = time.monotonic()
        assert await gate.wait(deadline_s=0.05) == Failed("missing_envelope")
        assert time.monotonic() - started < 5
        # A late envelope cannot replace the decided outcome; it learns the
        # reason, so the request fails with it.
        assert await gate.deliver(VALID) == (
            200,
            {"acknowledgment": _ack("failed"), "reason": "missing_envelope"},
        )
        assert gate.outcome == Failed("missing_envelope")

        control = RequesterContextGate()
        control.bind(BINDING)
        await control.deliver(VALID)
        assert await control.wait(deadline_s=0.05) == Applied(
            selection_id=WORK_ITEM, sha256=DIGEST, text=TEXT
        )

    anyio.run(go)


def test_wait_returns_once_the_envelope_arrives() -> None:
    async def go() -> None:
        gate = RequesterContextGate()
        gate.bind(BINDING)
        outcomes: list[Any] = []

        async def waiter() -> None:
            outcomes.append(await gate.wait(deadline_s=30))

        with anyio.fail_after(10):
            async with anyio.create_task_group() as group:
                group.start_soon(waiter)
                await anyio.sleep(0)
                assert outcomes == []
                await gate.deliver(ABSENT)
        assert outcomes == [NoneSelected(reason="no_profile")]

    anyio.run(go)


# --- 4. The server route -------------------------------------------------------------


def _session_runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )


async def _post_context(
    app: web.Application, body: Any, headers: dict[str, str] | None = None
) -> tuple[int, str]:
    async with TestClient(TestServer(app)) as client:
        response = await client.post(CONTEXT_PATH, data=body, headers=headers or {})
        return response.status, await response.text()


def _bound_gate() -> RequesterContextGate:
    gate = RequesterContextGate()
    gate.bind(BINDING)
    return gate


def test_the_route_requires_the_per_claim_bearer() -> None:
    async def go() -> None:
        assert CONTEXT_PATH == "/v1/requester-context"
        missing = _bound_gate()
        status, _ = await _post_context(
            create_app(_session_runner(), token=TOKEN, requester_context=missing), VALID
        )
        assert status == 401
        assert missing.outcome is None

        wrong = _bound_gate()
        status, _ = await _post_context(
            create_app(_session_runner(), token=TOKEN, requester_context=wrong),
            VALID,
            {"Authorization": "Bearer not-the-claim-token"},
        )
        assert status == 401
        assert wrong.outcome is None

        right = _bound_gate()
        status, text = await _post_context(
            create_app(_session_runner(), token=TOKEN, requester_context=right), VALID, BEARER
        )
        assert status == 200
        assert json.loads(text) == {"acknowledgment": _ack("applied", sha256=DIGEST)}
        assert right.applied is not None

    anyio.run(go)


def test_the_route_answers_404_without_a_gate_and_serves_an_attached_one() -> None:
    async def go() -> None:
        status, text = await _post_context(
            create_app(_session_runner(), token=TOKEN), VALID, BEARER
        )
        assert status == 404
        assert json.loads(text) == {"error": "requester_context_not_served"}

        runner = _session_runner()
        gate = _bound_gate()
        attach_gate(runner, gate)
        status, _ = await _post_context(create_app(runner, token=TOKEN), VALID, BEARER)
        assert status == 200
        assert gate.applied is not None

    anyio.run(go)


def test_an_oversized_body_is_a_failed_too_large_acknowledgment() -> None:
    async def chunked() -> AsyncIterator[bytes]:
        # No content length: the route stops reading past the bound.
        for _ in range(3):
            yield b" " * (MAX_ENVELOPE_BYTES // 2 + 1)

    async def go() -> None:
        too_large = {"acknowledgment": _ack("failed"), "reason": "too_large"}
        for body in (b" " * (MAX_ENVELOPE_BYTES + 1), chunked()):
            gate = _bound_gate()
            status, text = await _post_context(
                create_app(_session_runner(), token=TOKEN, requester_context=gate), body, BEARER
            )
            assert (status, json.loads(text)) == (200, too_large)
            assert gate.outcome == Failed("too_large")

        # Control: a body at the bound is read and decided on its content.
        padded = VALID + b" " * (MAX_ENVELOPE_BYTES - len(VALID))
        assert len(padded) == MAX_ENVELOPE_BYTES
        gate = _bound_gate()
        status, text = await _post_context(
            create_app(_session_runner(), token=TOKEN, requester_context=gate), padded, BEARER
        )
        assert (status, json.loads(text)) == (
            200,
            {"acknowledgment": _ack("applied", sha256=DIGEST)},
        )

    anyio.run(go)


def test_the_route_never_echoes_the_instruction_text() -> None:
    async def go() -> None:
        bodies = []
        for raw in (VALID, _raw(_envelope(_selected(sha256=_sha("other text"))))):
            gate = _bound_gate()
            status, text = await _post_context(
                create_app(_session_runner(), token=TOKEN, requester_context=gate), raw, BEARER
            )
            assert status == 200
            bodies.append(text)
        applied, refused = bodies
        assert json.loads(applied)["acknowledgment"]["outcome"] == "applied"
        assert json.loads(refused)["reason"] == "digest_mismatch"
        for body in bodies:
            assert TEXT not in body
            for line in LINES:
                assert line not in body

    anyio.run(go)


# --- 5. Redaction ----------------------------------------------------------------------


@contextmanager
def _redacting_root() -> Iterator[io.StringIO]:
    """A real root handler with the runner's stdout redaction pass installed."""

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    root.addHandler(handler)
    saved = {item: list(item.filters) for item in root.handlers}
    try:
        install_stdout_redaction()
        yield stream
    finally:
        root.removeHandler(handler)
        for item, filters in saved.items():
            item.filters[:] = filters


def _logged(*messages: str) -> str:
    with _redacting_root() as stream:
        logger = logging.getLogger("curie_runner.test_requester_instruction_context")
        logger.setLevel(logging.INFO)
        for message in messages:
            logger.info("%s", message)
    return stream.getvalue()


def test_an_applied_delivery_redacts_the_text_its_lines_and_the_section() -> None:
    section = requester_section(WORK_ITEM, DIGEST, TEXT)
    unrelated = "An unrelated status line for the requester context log."

    # Negative control: before any delivery nothing is held.
    before = _logged(TEXT, section)
    assert TEXT in before and section in before

    async def deliver(raw: bytes) -> RequesterContextGate:
        gate = _bound_gate()
        await gate.deliver(raw)
        return gate

    # A refused delivery holds nothing either.
    refused = anyio.run(deliver, _raw(_envelope(_selected(sha256=_sha("other text")))))
    assert refused.applied is None
    assert TEXT in _logged(TEXT)

    gate = anyio.run(deliver, VALID)
    assert gate.applied is not None
    assert gate.applied.section() == section
    assert set(private_fragments(gate.applied)) >= {TEXT, section, *LINES}

    after = _logged(TEXT, section, *LINES, unrelated)
    assert TEXT not in after
    assert section not in after
    for line in LINES:
        assert line not in after
    assert "[REDACTED:held_secret]" in after
    # Negative control: a different string is not touched.
    assert unrelated in after


# --- 6. Subagent guard -------------------------------------------------------------------


def _applied_gate() -> RequesterContextGate:
    gate = _bound_gate()
    anyio.run(gate.deliver, VALID)
    assert gate.applied is not None
    return gate


def _guard(gate: RequesterContextGate) -> Callable[..., Any]:
    matchers = build_subagent_guard(gate)["PreToolUse"]
    assert len(matchers) == 1
    assert matchers[0].matcher == "Agent|Task"
    hook: Callable[..., Any] = matchers[0].hooks[0]
    return hook


def _decide(hook: Callable[..., Any], hook_input: Any) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        result: dict[str, Any] = await hook(hook_input, "toolu_example", None)
        return result

    return anyio.run(go)


def _denied(result: dict[str, Any]) -> bool:
    output = result.get("hookSpecificOutput") or {}
    return output.get("permissionDecision") == "deny"


ORDINARY = "Review the diff in this worktree against the issue's acceptance criteria."


@pytest.mark.parametrize("tool", ["Agent", "Task"])
@pytest.mark.parametrize(
    "carried",
    [
        pytest.param(open_line(WORK_ITEM, DIGEST), id="open-line"),
        pytest.param(OPEN_PREFIX, id="open-prefix"),
        pytest.param(close_line(DIGEST), id="close-line"),
        pytest.param(CLOSE_PREFIX, id="close-prefix"),
        pytest.param(TEXT, id="exact-text"),
        pytest.param(requester_section(WORK_ITEM, DIGEST, TEXT), id="whole-section"),
    ],
)
def test_a_subagent_prompt_carrying_the_section_is_denied(tool: str, carried: str) -> None:
    hook = _guard(_applied_gate())
    allowed = _decide(hook, {"tool_name": tool, "tool_input": {"prompt": ORDINARY}})
    assert allowed == {}
    denied = _decide(
        hook, {"tool_name": tool, "tool_input": {"prompt": f"{ORDINARY}\n\n{carried}\n"}}
    )
    assert _denied(denied)
    assert denied["hookSpecificOutput"]["hookEventName"] == "PreToolUse"


def test_the_refusal_needs_an_applied_outcome_and_reads_every_field() -> None:
    applied = _applied_gate().applied
    carrying = {"prompt": f"{ORDINARY} {TEXT}"}
    assert subagent_prompt_refusal(carrying, applied) is not None
    assert subagent_prompt_refusal({"prompt": ORDINARY}, applied) is None
    # Without an applied outcome nothing is pinned, so nothing is refused.
    assert subagent_prompt_refusal(carrying, None) is None
    unapplied = RequesterContextGate()
    unapplied.bind(BINDING)
    assert _decide(_guard(unapplied), {"tool_name": "Agent", "tool_input": carrying}) == {}
    # Every string in the input is read, at any depth and beside a string
    # ``prompt``, so the text or a delimiter carried in another field is refused.
    for field in (
        {"description": open_line(WORK_ITEM, DIGEST)},
        {"description": close_line(DIGEST)},
        {"prompt": ORDINARY, "description": TEXT},
        {"prompt": ORDINARY, "context": {"notes": [TEXT]}},
    ):
        assert subagent_prompt_refusal(field, applied) is not None, field
    assert subagent_prompt_refusal({"prompt": ORDINARY, "description": ORDINARY}, applied) is None
    assert subagent_prompt_refusal({"prompt": ORDINARY, "count": 3}, applied) is None


def test_the_guard_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = _applied_gate()
    hook = _guard(gate)
    ordinary = {"tool_name": "Agent", "tool_input": {"prompt": ORDINARY}}
    assert _decide(hook, ordinary) == {}

    for broken in (
        {"tool_name": "Agent", "tool_input": "not a mapping"},
        {"tool_name": "Agent", "tool_input": [ORDINARY]},
        {"tool_name": "Agent"},
        "not a mapping either",
    ):
        assert _denied(_decide(hook, broken)), broken

    def boom(*_args: Any) -> str | None:
        raise RuntimeError("guard internals failed")

    with monkeypatch.context() as patched:
        patched.setattr(rc, "subagent_prompt_refusal", boom)
        assert _denied(_decide(hook, ordinary))
    assert _decide(hook, ordinary) == {}

    class _RaisingGate(RequesterContextGate):
        @property
        def applied(self) -> Applied | None:
            raise RuntimeError("gate unreadable")

    assert _denied(_decide(_guard(_RaisingGate()), ordinary))


def test_the_guard_decides_well_under_the_hook_deadline() -> None:
    # The CLI waits at most 60 s for a hook; the guard must decide far sooner even
    # on the largest pinned text and a large prompt that does not carry it.
    text = ("Prefer explicit names over abbreviations in every module.\n" * 300)[:MAX_BYTES]
    text = text.rstrip("\n")
    gate = _bound_gate()
    anyio.run(gate.deliver, _raw(_envelope(_selected(text))))
    assert gate.applied is not None
    hook = _guard(gate)
    large = "Review the change carefully. " * 40_000
    started = time.monotonic()

    async def go() -> tuple[dict[str, Any], dict[str, Any]]:
        with anyio.fail_after(5):
            allowed = await hook(
                {"tool_name": "Task", "tool_input": {"prompt": large}}, "toolu_example", None
            )
            denied = await hook(
                {"tool_name": "Task", "tool_input": {"prompt": large + text}},
                "toolu_example",
                None,
            )
        return allowed, denied

    allowed, denied = anyio.run(go)
    assert time.monotonic() - started < 5
    assert allowed == {}
    assert _denied(denied)


# --- 7. System prompt placement (fresh and resumed turns) ------------------------------


async def _record_other(request: web.Request) -> web.Response:
    request.app[_OTHER].append(f"{request.method} {request.path}")
    return web.json_response({"input_tokens": 1})


_OTHER = web.AppKey("other", list[str])


def _stand_in() -> web.Application:
    app = web.Application()
    app[_BODIES] = []
    app[_OTHER] = []
    app.router.add_post("/v1/messages", _messages)
    app.router.add_route("*", "/{tail:.*}", _record_other)
    return app


async def _sdk_turn(
    *,
    base_url: str,
    cwd: str,
    system_prompt: str,
    prior: tuple[ConversationMessage, ...],
    replay: HarnessReplayState | None,
    query: str,
) -> tuple[list[ConversationMessage], HarnessReplayState | None]:
    resume = build_structured_resume(
        prior,
        curie_session_id="curie-requester-context",
        cwd=cwd,
        harness_replay=replay,
        system_prompt=system_prompt,
    )
    options = build_options(
        plugins=[],
        model=MODEL,
        system_prompt=system_prompt,
        max_turns=2,
        max_budget_usd=None,
        resume=resume.resume,
        session_id=resume.session_id,
        session_store=resume.session_store,
        cwd=cwd,
        env={
            "ANTHROPIC_API_KEY": "sk-ant-placeholder",
            "ANTHROPIC_BASE_URL": base_url,
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
        },
    )
    session = ClaudeAgentSession(options)
    await session.connect()
    try:
        await session.query(query)
        with anyio.fail_after(90):
            messages = [
                projected
                for message in [m async for m in session.receive_turn()]
                if (projected := model_message_to_conversation(message)) is not None
            ]
        exported = await session.export_replay_state()
    finally:
        await session.close()
    return messages, exported


def _delimited(system: str) -> str:
    """The bytes from the open delimiter through the end of the close line."""

    start = system.index(OPEN_PREFIX)
    end = system.index(">>>", system.index(CLOSE_PREFIX)) + len(">>>")
    return system[start:end]


@requires_cli
def test_the_section_follows_the_bundle_prompt_on_fresh_and_resumed_turns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    applied = parse_envelope(VALID, BINDING)
    assert isinstance(applied, Applied)
    section = requester_section(WORK_ITEM, DIGEST, TEXT)
    assert applied.section() == section
    prompt = _compose_system_prompt(
        BUNDLE_PROMPT, None, model=MODEL, requester_preamble=applied.section()
    )
    assert prompt is not None

    async def scenario() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        app = _stand_in()
        async with TestServer(app, host="127.0.0.1") as server:
            base_url = str(server.make_url("")).rstrip("/")
            first_reply, checkpoint = await _sdk_turn(
                base_url=base_url,
                cwd=str(cwd),
                system_prompt=prompt,
                prior=(),
                replay=None,
                query="start the issue",
            )
            assert checkpoint is not None and checkpoint.kind == "checkpoint"
            fresh = list(app[_BODIES])
            await _sdk_turn(
                base_url=base_url,
                cwd=str(cwd),
                system_prompt=prompt,
                prior=(ConversationMessage(role="user", content="start the issue"), *first_reply),
                replay=checkpoint,
                query="continue the issue",
            )
            return fresh, list(app[_BODIES][len(fresh) :])

    fresh, resumed = anyio.run(scenario)
    assert fresh, "the fresh turn made no provider request"
    assert resumed, "the resumed turn made no provider request"
    for body in (*fresh, *resumed):
        system = _system_text(body)
        assert section in system
        assert system.index(BUNDLE_PROMPT) < system.index(section)
        assert system.count(OPEN_PREFIX) == 1
    assert _delimited(_system_text(fresh[-1])) == _delimited(_system_text(resumed[-1])) == section


@requires_cli
def test_a_turn_without_an_applied_outcome_carries_no_delimiter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    without = _compose_system_prompt(BUNDLE_PROMPT, None, model=MODEL)
    absent = _compose_system_prompt(BUNDLE_PROMPT, None, model=MODEL, requester_preamble=None)
    with_section = _compose_system_prompt(
        BUNDLE_PROMPT,
        None,
        model=MODEL,
        requester_preamble=requester_section(WORK_ITEM, DIGEST, TEXT),
    )
    assert without is not None and absent is not None and with_section is not None
    assert absent.encode("utf-8") == without.encode("utf-8")
    assert OPEN_PREFIX not in absent and CLOSE_PREFIX not in absent
    # Control: the same composition with the section does carry it.
    assert OPEN_PREFIX in with_section and with_section.startswith(without)

    async def scenario() -> list[dict[str, Any]]:
        app = _stand_in()
        async with TestServer(app, host="127.0.0.1") as server:
            await _sdk_turn(
                base_url=str(server.make_url("")).rstrip("/"),
                cwd=str(cwd),
                system_prompt=absent,
                prior=(),
                replay=None,
                query="start the issue",
            )
            return list(app[_BODIES])

    bodies = anyio.run(scenario)
    assert bodies, "the turn made no provider request"
    for body in bodies:
        system = _system_text(body)
        assert BUNDLE_PROMPT in system
        assert OPEN_PREFIX not in system and CLOSE_PREFIX not in system
        assert TEXT not in system


# --- 8. The real boot: a Failed outcome sends zero model requests ------------------------

_CAPABILITY = "wir.example-capability.signature"
_PROBE = McpToolCapabilityProbe(
    complete=True,
    has_potential_write_tool=True,
    tool_count=1,
    observed_tools=frozenset(),
    readonly_tools=frozenset(),
)


def _contract_api() -> web.Application:
    async def contract(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "completion_contract": "implementation",
                "result_kinds": [],
                "session_usd_allowance": None,
                "execution": {
                    "tenant_id": TENANT,
                    "work_item_id": WORK_ITEM,
                    "execution_request_id": EXECUTION_REQUEST,
                    "execution_fence": FENCE,
                },
            }
        )

    app = web.Application()
    app.router.add_post("/work-items/completion-contract", contract)
    return app


def _boot_config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "implementer", "version": "0.1.0", "description": "t"}),
        encoding="utf-8",
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin),
            "CURIE_SESSION_ID": "s-requester-context",
            "CURIE_SANDBOX_ID": "b-requester-context",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 64, "max_usd_per_day": 1.0}',
            "CURIE_MODEL": MODEL,
        }
    )


def _boot_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: bytes
) -> tuple[dict[str, Any], Final, list[dict[str, Any]], list[str]]:
    """Boot the real runner, deliver ``raw`` and run one turn, all over HTTP."""

    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    config = _boot_config(tmp_path)

    async def go() -> tuple[dict[str, Any], Final, list[dict[str, Any]], list[str]]:
        provider = _stand_in()
        async with (
            TestServer(_contract_api(), host="127.0.0.1") as api,
            TestServer(provider, host="127.0.0.1") as model,
        ):
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(api.make_url("/work-items/issue-read")))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, _CAPABILITY)
            runner = build_runner(
                config,
                sdk_env={
                    "ANTHROPIC_BASE_URL": str(model.make_url("")).rstrip("/"),
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_AUTH_TOKEN": "",
                    "CLAUDE_CODE_OAUTH_TOKEN": "",
                    "CLAUDE_CONFIG_DIR": str(tmp_path / "claude-config"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                    "HTTP_PROXY": "",
                    "HTTPS_PROXY": "",
                    "ALL_PROXY": "",
                    "NO_PROXY": "127.0.0.1,localhost",
                },
                mcp_capability=_PROBE,
            )
            async with TestClient(TestServer(create_app(runner, token=TOKEN))) as client:
                with anyio.fail_after(120):
                    delivered = await client.post(CONTEXT_PATH, data=raw, headers=BEARER)
                    assert delivered.status == 200
                    ack: dict[str, Any] = await delivered.json()
                    turn = await client.post(
                        "/v1/event",
                        json={
                            "kind": "event",
                            "type": "message",
                            "text": "start the issue",
                            "user": "U0EXAMPLE",
                            "ts": "1",
                        },
                        headers={**BEARER, FINAL_ANSWER_HEADER: "1"},
                    )
                    assert turn.status == 200
                    events = parse_ndjson(await turn.text())
            finals = [event for event in events if isinstance(event, Final)]
            assert len(finals) == 1
            return ack, finals[0], list(provider[_BODIES]), list(provider[_OTHER])

    return anyio.run(go)


@requires_cli
def test_an_applied_boot_reaches_the_model_with_the_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ack, final, bodies, _ = _boot_turn(tmp_path, monkeypatch, VALID)
    assert ack == {"acknowledgment": _ack("applied", sha256=DIGEST)}
    assert bodies, "an applied boot made no provider request"
    section = requester_section(WORK_ITEM, DIGEST, TEXT)
    assert any(section in _system_text(body) for body in bodies)
    assert final.text != BLOCKED_EXPLANATION


@requires_cli
def test_a_none_selected_boot_reaches_the_model_without_a_delimiter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ack, final, bodies, _ = _boot_turn(tmp_path, monkeypatch, ABSENT)
    assert ack == {"acknowledgment": _ack("none")}
    assert bodies, "a none-selected boot made no provider request"
    for body in bodies:
        system = _system_text(body)
        assert OPEN_PREFIX not in system and CLOSE_PREFIX not in system
    assert final.text != BLOCKED_EXPLANATION


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        pytest.param(
            _raw(_envelope(_selected(sha256=_sha("other text")))),
            "digest_mismatch",
            id="digest-mismatch",
        ),
        pytest.param(_raw(_envelope(execution_fence=FENCE + 1)), "wrong_fence", id="wrong-fence"),
        pytest.param(b"not json", "malformed_envelope", id="malformed"),
    ],
)
def test_a_failed_boot_sends_zero_model_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: bytes, reason: str
) -> None:
    ack, final, bodies, other = _boot_turn(tmp_path, monkeypatch, raw)
    assert ack == {"acknowledgment": _ack("failed"), "reason": reason}
    assert bodies == []
    assert other == []
    assert final.text == BLOCKED_EXPLANATION


# --- Reuse: one runner process, one execution, one selection ---------------------


def _snapshot(system_prompt: str) -> dict[str, Any]:
    """The prompt the bundled CLI records and would restore (see test_history.py)."""

    return {
        "type": "attachment",
        "uuid": "snapshot-1",
        "timestamp": "1970-01-01T00:00:00.000Z",
        "attachment": {
            "type": "prompt_snapshot",
            "systemPrompt": [system_prompt],
            "reminderFold": False,
        },
    }


_PRIOR = (
    ConversationMessage(role="user", content="start the work"),
    ConversationMessage(role="assistant", content=[{"type": "text", "text": "started"}]),
)
_PRIOR_ENTRIES = (
    {
        "type": "user",
        "uuid": "entry-1",
        "timestamp": "1970-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "start the work"},
    },
    {
        "type": "assistant",
        "uuid": "entry-2",
        "timestamp": "1970-01-01T00:00:00.000Z",
        "message": {"role": "assistant", "content": _PRIOR[1].content},
    },
)


def _prompt_for(selection_id: str | None) -> str:
    prompt = _compose_system_prompt(
        "You implement factory tickets.",
        None,
        model=None,
        requester_preamble=(
            requester_section(selection_id, DIGEST, TEXT) if selection_id is not None else None
        ),
    )
    assert prompt is not None
    return prompt


@pytest.mark.parametrize(
    ("recorded", "restored"),
    [
        # The control: the same selection's checkpoint is restored.
        (WORK_ITEM, True),
        # Another WorkItem's selection of the very same text: identity, not text.
        (OTHER_ID, False),
        # A checkpoint recorded with no selection under a selected boot.
        (None, False),
    ],
)
def test_a_checkpoint_is_restored_only_under_its_own_selection(
    tmp_path: Path, recorded: str | None, restored: bool
) -> None:
    checkpoint = HarnessReplayState(
        harness="claude",
        kind="checkpoint",
        entries=(*_PRIOR_ENTRIES, _snapshot(_prompt_for(recorded))),
    )

    resume = build_structured_resume(
        _PRIOR,
        curie_session_id="curie-thread-requester",
        cwd=str(tmp_path),
        harness_replay=checkpoint,
        system_prompt=_prompt_for(WORK_ITEM),
    )

    assert resume.session_store is not None
    entries = anyio.run(resume.session_store.load, resume.session_key)
    assert entries is not None
    assert (entries == list(checkpoint.entries)) is restored
    if not restored:
        # The portable conversation is replayed under this boot's own prompt.
        assert [entry["message"] for entry in entries] == [m.to_dict() for m in _PRIOR]


def test_a_decided_runner_never_serves_a_second_execution_its_section() -> None:
    # A runner process is bound to the one execution its completion contract
    # names. Another request's envelope on the same process is refused, so that
    # request fails before model work and never runs under this section.
    gate = _applied_gate()
    other = _raw(_envelope(execution_request_id=OTHER_ID))

    status, body = anyio.run(gate.deliver, other)

    assert status == 409 and body == {"error": "requester_context_already_delivered"}
    assert gate.applied is not None and gate.applied.selection_id == WORK_ITEM
    # The control: the same execution's identical bytes are acknowledged again.
    assert anyio.run(gate.deliver, VALID)[0] == 200
    # A fresh process for the other execution applies its own selection.
    fresh = RequesterContextGate()
    fresh.bind(
        ExecutionBinding(
            tenant_id=TENANT,
            work_item_id=WORK_ITEM,
            execution_request_id=OTHER_ID,
            execution_fence=FENCE,
        )
    )
    assert anyio.run(fresh.deliver, other)[0] == 200
    assert fresh.applied is not None
