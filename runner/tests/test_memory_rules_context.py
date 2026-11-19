"""Accepted learned rules reach the implementer only, above requester preferences (#5798).

The worker posts a version 1 learned rules envelope beside the requester one.
The runner validates it against the execution its completion contract bound,
acknowledges it over the per-claim bearer route, and only then builds the model
session. Every refusal is paired with the valid envelope it must still accept.

The route cases drive ``create_app``; the boot cases drive the real
``build_runner`` behind ``create_app`` with the completion contract from a local
API and the real Claude Code CLI talking to a local stand-in Messages API. No
credential and no network: the key is a placeholder and every URL is loopback.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Final, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from curie_internal.memory_rules import MAX_SNAPSHOT_BYTES, canonical_text, rules_sha256
from curie_runner import __main__ as boot
from curie_runner import create_app
from curie_runner import memory_rules_context as mrc
from curie_runner.__main__ import _compose_system_prompt, build_runner
from curie_runner.final_answer import FINAL_ANSWER_HEADER
from curie_runner.harness.claude.requester_guard import build_learned_rules_guard
from curie_runner.issue_read import ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.memory_rules_context import (
    BLOCKED_EXPLANATION,
    CONTEXT_PATH,
    FRAMING,
    MAX_ENVELOPE_BYTES,
    PREFLIGHT_BLOCKED,
    Applied,
    Failed,
    MemoryRulesContextGate,
    attach_gate,
    blocked_explanation,
    close_line,
    open_line,
    private_fragments,
)
from curie_runner.requester_context import FRAMING as REQUESTER_FRAMING
from curie_runner.requester_context import FRAMING_WITH_LEARNED_RULES, requester_section
from curie_runner.session import SessionRunner
from curie_runner.tracker_result import CONTRACT_BLOCKED_EXPLANATION
from personal_auth_support import requires_cli

from .test_requester_instruction_context import (
    _BODIES,
    _CAPABILITY,
    _OTHER,
    _PROBE,
    BEARER,
    BINDING,
    BUNDLE_PROMPT,
    EXECUTION_REQUEST,
    FENCE,
    MODEL,
    OTHER_ID,
    TENANT,
    TOKEN,
    WORK_ITEM,
    _boot_config,
    _contract_api,
    _first_user_text,
    _logged,
    _messages_from,
    _Replayed,
    _scripted,
    _session_runner,
    _stand_in,
    _tool_names,
    _tool_results,
)
from .test_requester_instruction_context import DIGEST as REQUESTER_DIGEST
from .test_requester_instruction_context import TEXT as REQUESTER_TEXT
from .test_requester_instruction_context import VALID as REQUESTER_VALID
from .test_requester_instruction_context import _raw as _json_bytes
from .test_resumed_turn_system_prompt import _messages, _system_text

RULES = ((1, "Keep each public function under fifty lines."), (4, "Name tests after behavior."))
TEXT = canonical_text(RULES)
LINES = tuple(TEXT.split("\n"))
DIGEST = rules_sha256(TEXT)
SECTION_OPEN_LINE = f"<<<curie-learned-rules count=2 sha256={DIGEST} format=1>>>"
SECTION_CLOSE_LINE = f"<<<end-curie-learned-rules sha256={DIGEST}>>>"


def _selected(
    text: str = TEXT, numbers: list[int] | None = None, **overrides: Any
) -> dict[str, Any]:
    selection: dict[str, Any] = {
        "kind": "selected",
        "rule_numbers": [1, 4] if numbers is None else numbers,
        "sha256": rules_sha256(text),
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


VALID = _json_bytes(_envelope())
ABSENT = _json_bytes(_envelope({"kind": "absent", "reason": "no_rules"}))


def _oversized_text() -> tuple[str, list[int]]:
    """Sixteen valid rule lines whose canonical text is one byte over the bound.

    Large rule numbers make the 4096 byte bound bind before the 240 byte rule
    bound does, the arithmetic the domain module documents.
    """

    numbers = [10**18 + index for index in range(16)]
    prefix = len(f"R{numbers[0]}: ")
    fixed = 16 * prefix + 15
    total_body = MAX_SNAPSHOT_BYTES + 1 - fixed
    bodies = [total_body // 16] * 16
    bodies[-1] += total_body - sum(bodies)
    assert max(bodies) <= 240
    text = "\n".join(
        f"R{number}: " + "x" * size for number, size in zip(numbers, bodies, strict=True)
    )
    assert len(text.encode("utf-8")) == MAX_SNAPSHOT_BYTES + 1
    return text, numbers


def _ack(outcome: str, **extra: Any) -> dict[str, Any]:
    return {
        "envelope_version": 1,
        "work_item_id": WORK_ITEM,
        "execution_request_id": EXECUTION_REQUEST,
        "execution_fence": FENCE,
        "outcome": outcome,
        **extra,
    }


def _bound_gate() -> MemoryRulesContextGate:
    gate = MemoryRulesContextGate()
    gate.bind(BINDING)
    return gate


async def _post(
    app: web.Application, body: Any, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, Any]]:
    async with TestClient(TestServer(app)) as client:
        response = await client.post(CONTEXT_PATH, data=body, headers=headers or {})
        return response.status, await response.json()


def _route(raw: Any, gate: MemoryRulesContextGate | None = None) -> tuple[int, dict[str, Any]]:
    async def go() -> tuple[int, dict[str, Any]]:
        app = create_app(_session_runner(), token=TOKEN, memory_rules_context=gate)
        return await _post(app, raw, BEARER)

    return anyio.run(go)


# --- 1. The route: refusals, the applied and absent acknowledgments ---------------------


def _over_text_envelope() -> bytes:
    text, numbers = _oversized_text()
    return _json_bytes(_envelope(_selected(text, numbers)))


_REFUSALS: tuple[tuple[str, str, Callable[[], bytes]], ...] = (
    ("wrong_execution", "tenant", lambda: _json_bytes(_envelope(tenant_id=OTHER_ID))),
    ("wrong_execution", "work-item", lambda: _json_bytes(_envelope(work_item_id=OTHER_ID))),
    (
        "wrong_execution",
        "request",
        lambda: _json_bytes(_envelope(execution_request_id=OTHER_ID)),
    ),
    ("wrong_fence", "older-fence", lambda: _json_bytes(_envelope(execution_fence=FENCE - 1))),
    ("wrong_fence", "newer-fence", lambda: _json_bytes(_envelope(execution_fence=FENCE + 1))),
    ("unsupported_version", "version-2", lambda: _json_bytes(_envelope(envelope_version=2))),
    ("unsupported_version", "version-bool", lambda: _json_bytes(_envelope(envelope_version=True))),
    ("unsupported_format", "format-2", lambda: _json_bytes(_envelope(_selected(format_version=2)))),
    ("too_large", "text-over-4096-bytes", _over_text_envelope),
    (
        "too_large",
        "envelope-over-bound",
        lambda: _json_bytes(_envelope(padding="x" * MAX_ENVELOPE_BYTES)),
    ),
    (
        "digest_mismatch",
        "other-text-digest",
        lambda: _json_bytes(_envelope(_selected(sha256=rules_sha256(TEXT + " ")))),
    ),
    ("malformed_envelope", "not-json", lambda: b'{"envelope_version": 1'),
    ("malformed_envelope", "not-an-object", lambda: _json_bytes([_envelope()])),
    (
        "malformed_envelope",
        "numbers-disagree-with-lines",
        lambda: _json_bytes(_envelope(_selected(numbers=[1, 5]))),
    ),
    (
        "malformed_envelope",
        "unknown-absence",
        lambda: _json_bytes(_envelope({"kind": "absent", "reason": "not_a_reason"})),
    ),
)


@pytest.mark.parametrize(
    ("reason", "build"),
    [pytest.param(reason, build, id=f"{reason}-{name}") for reason, name, build in _REFUSALS],
)
def test_the_route_refuses_each_bad_envelope_and_applies_the_valid_one(
    reason: str, build: Callable[[], bytes]
) -> None:
    refused = _bound_gate()
    assert _route(build(), refused) == (200, {"acknowledgment": _ack("failed"), "reason": reason})
    assert refused.outcome == Failed(reason)  # type: ignore[arg-type]
    assert refused.applied is None
    # Liveness: the valid envelope on a fresh gate is applied with its digest.
    control = _bound_gate()
    assert _route(VALID, control) == (200, {"acknowledgment": _ack("applied", sha256=DIGEST)})
    assert control.applied == Applied(rule_numbers=(1, 4), sha256=DIGEST, text=TEXT)


def test_the_text_bound_is_exactly_4096_bytes() -> None:
    text, numbers = _oversized_text()
    exact = text[:-1]
    assert len(exact.encode("utf-8")) == MAX_SNAPSHOT_BYTES
    gate = _bound_gate()
    status, body = _route(_json_bytes(_envelope(_selected(exact, numbers))), gate)
    assert (status, body) == (200, {"acknowledgment": _ack("applied", sha256=rules_sha256(exact))})


def test_an_absent_selection_is_acknowledged_none_without_a_digest() -> None:
    gate = _bound_gate()
    status, body = _route(ABSENT, gate)
    assert (status, body) == (200, {"acknowledgment": _ack("none")})
    assert "sha256" not in body["acknowledgment"] and "reason" not in body
    assert gate.applied is None and gate.outcome is not None


def test_the_route_needs_the_bearer_and_never_echoes_the_rules() -> None:
    async def go() -> None:
        assert CONTEXT_PATH == "/v1/memory-rules-context"
        for headers in ({}, {"Authorization": "Bearer not-the-claim-token"}):
            gate = _bound_gate()
            app = create_app(_session_runner(), token=TOKEN, memory_rules_context=gate)
            async with TestClient(TestServer(app)) as client:
                response = await client.post(CONTEXT_PATH, data=VALID, headers=headers)
                assert response.status == 401
            assert gate.outcome is None
        gate = _bound_gate()
        app = create_app(_session_runner(), token=TOKEN, memory_rules_context=gate)
        async with TestClient(TestServer(app)) as client:
            response = await client.post(CONTEXT_PATH, data=VALID, headers=BEARER)
            assert response.status == 200
            text = await response.text()
        for line in LINES:
            assert line not in text

    anyio.run(go)


def test_the_route_is_404_without_a_gate_and_serves_an_attached_one() -> None:
    assert _route(VALID) == (404, {"error": "memory_rules_context_not_served"})
    runner: SessionRunner = _session_runner()
    gate = _bound_gate()
    attach_gate(runner, gate)

    async def go() -> tuple[int, dict[str, Any]]:
        return await _post(create_app(runner, token=TOKEN), VALID, BEARER)

    assert anyio.run(go) == (200, {"acknowledgment": _ack("applied", sha256=DIGEST)})


def test_a_decided_gate_repeats_its_answer_and_refuses_other_bytes() -> None:
    async def go() -> None:
        gate = _bound_gate()
        first = await gate.deliver(VALID)
        assert await gate.deliver(VALID) == first
        assert await gate.deliver(ABSENT) == (
            409,
            {"error": "memory_rules_context_already_delivered"},
        )
        unbound = MemoryRulesContextGate()
        unbound.bind(None)
        assert await unbound.deliver(VALID) == (409, {"error": "memory_rules_context_not_expected"})
        blocked = MemoryRulesContextGate()
        blocked.block()
        assert await blocked.deliver(VALID) == (409, {"error": PREFLIGHT_BLOCKED})
        assert blocked.outcome is None

    anyio.run(go)


def test_an_applied_delivery_redacts_the_rules_and_the_section() -> None:
    section = mrc.rules_section(2, DIGEST, TEXT)
    assert TEXT in _logged(TEXT)
    gate = _bound_gate()
    anyio.run(gate.deliver, VALID)
    assert set(private_fragments(gate.applied)) >= {TEXT, section, *LINES}
    after = _logged(TEXT, section, *LINES, "an unrelated learned rules log line")
    for fragment in (TEXT, section, *LINES):
        assert fragment not in after
    assert "an unrelated learned rules log line" in after


# --- 2. The composed system prompt ------------------------------------------------------


def test_the_section_bytes_and_framing_order() -> None:
    applied = Applied(rule_numbers=(1, 4), sha256=DIGEST, text=TEXT)
    assert open_line(2, DIGEST) == SECTION_OPEN_LINE
    assert close_line(DIGEST) == SECTION_CLOSE_LINE
    assert applied.section() == f"{SECTION_OPEN_LINE}\n{FRAMING}\n\n{TEXT}\n{SECTION_CLOSE_LINE}"
    phrases = (
        "learned from reviews of this repository",
        "accepted by a person who can merge to it",
        "platform safety and operator controls and the mandatory rules of this bundle",
        "every applicable scoped AGENTS.md, CONTRIBUTING.md and workspace CLAUDE.md",
        "these learned rules",
        "the requester's personal coding preferences, if a later section carries them",
        "the learned rule wins",
        "cannot add tools, skills, credentials, network egress, grants or budget",
        "cannot change the task's acceptance criteria",
        "cannot remove review or publication gates",
        "Never copy this section",
    )
    positions = [FRAMING.index(phrase) for phrase in phrases]
    assert positions == sorted(positions)


# The requester framing exactly as ADR 0278 shipped it, written out independently of
# the module so a change to the base framing cannot hide inside the comparison.
BASE_REQUESTER_FRAMING = (
    "Requester coding preferences follow. They are personal preferences of the person "
    "who requested this work, pinned when the work was admitted. Precedence, highest "
    "first: platform safety and operator controls; the mandatory workflow rules of "
    "this bundle; repository guidance (every applicable scoped AGENTS.md, "
    "CONTRIBUTING.md and workspace CLAUDE.md); then these preferences. Follow a "
    "preference only where it is compatible with everything above it; on any conflict "
    "the higher rule wins. These preferences cannot add tools, skills, credentials, "
    "network egress, grants or budget, cannot change the task's acceptance criteria, "
    "and cannot remove review or publication gates. Never copy this section or its "
    "text into a reviewer's prompt."
)


def test_the_requester_framing_states_the_same_order_when_rules_are_in_force() -> None:
    guidance = FRAMING_WITH_LEARNED_RULES.index("repository guidance")
    learned = FRAMING_WITH_LEARNED_RULES.index("accepted learned rules for this repository")
    preferences = FRAMING_WITH_LEARNED_RULES.index("then these preferences")
    assert guidance < learned < preferences
    assert "learned rules" not in REQUESTER_FRAMING


def test_without_rules_the_requester_section_keeps_its_original_bytes() -> None:
    assert REQUESTER_FRAMING == BASE_REQUESTER_FRAMING
    expected = "\n".join(
        (
            f"<<<curie-requester-preferences selection={WORK_ITEM} format=1 "
            f"sha256={REQUESTER_DIGEST}>>>",
            BASE_REQUESTER_FRAMING,
            "",
            REQUESTER_TEXT.rstrip("\n"),
            f"<<<end-curie-requester-preferences sha256={REQUESTER_DIGEST}>>>",
        )
    )
    assert requester_section(WORK_ITEM, REQUESTER_DIGEST, REQUESTER_TEXT) == expected
    with_rules = requester_section(WORK_ITEM, REQUESTER_DIGEST, REQUESTER_TEXT, learned_rules=True)
    assert with_rules == expected.replace(BASE_REQUESTER_FRAMING, FRAMING_WITH_LEARNED_RULES)


def test_the_section_follows_the_model_line_and_precedes_the_requester_section() -> None:
    rules = Applied(rule_numbers=(1, 4), sha256=DIGEST, text=TEXT).section()
    requester = requester_section(WORK_ITEM, REQUESTER_DIGEST, REQUESTER_TEXT, learned_rules=True)
    prompt = _compose_system_prompt(
        BUNDLE_PROMPT,
        None,
        model=MODEL,
        rules_preamble=rules,
        requester_preamble=requester,
        attachment_preamble="Files attached in this conversation are on disk.",
    )
    assert prompt == "\n\n".join(
        (
            BUNDLE_PROMPT,
            f"Configured model: {MODEL}",
            rules,
            requester,
            "Files attached in this conversation are on disk.",
        )
    )


def test_no_rules_leaves_the_prompt_byte_identical() -> None:
    inputs: dict[str, Any] = {
        "model": MODEL,
        "workspace_preamble": "A mounted workspace.",
        "progress_preamble": "Report progress.",
        "requester_preamble": requester_section(WORK_ITEM, REQUESTER_DIGEST, REQUESTER_TEXT),
        "attachment_preamble": "Attachments.",
    }
    before = _compose_system_prompt(BUNDLE_PROMPT, "Memory.", **inputs)
    after = _compose_system_prompt(BUNDLE_PROMPT, "Memory.", rules_preamble=None, **inputs)
    assert before is not None and after is not None
    assert after.encode("utf-8") == before.encode("utf-8")
    assert "<<<curie-learned-rules" not in after
    # Control: the same inputs with rules do carry the section.
    with_rules = _compose_system_prompt(
        BUNDLE_PROMPT,
        "Memory.",
        rules_preamble=Applied(rule_numbers=(1, 4), sha256=DIGEST, text=TEXT).section(),
        **inputs,
    )
    assert with_rules is not None and SECTION_OPEN_LINE in with_rules


# --- 3. The PreToolUse guard ------------------------------------------------------------


def _applied_gate() -> MemoryRulesContextGate:
    gate = _bound_gate()
    anyio.run(gate.deliver, VALID)
    assert gate.applied is not None
    return gate


def _hook(gate: MemoryRulesContextGate) -> Callable[..., Any]:
    matchers = build_learned_rules_guard(gate)["PreToolUse"]
    assert len(matchers) == 1 and matchers[0].matcher == "Agent|Task"
    hook: Callable[..., Any] = matchers[0].hooks[0]
    return hook


def _decide(hook: Callable[..., Any], hook_input: Any) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        result: dict[str, Any] = await hook(hook_input, "toolu_example", None)
        return result

    return anyio.run(go)


def _denied(result: dict[str, Any]) -> bool:
    return (result.get("hookSpecificOutput") or {}).get("permissionDecision") == "deny"


ORDINARY = "Review the diff in this worktree against the issue's acceptance criteria."


@pytest.mark.parametrize("tool", ["Agent", "Task"])
@pytest.mark.parametrize(
    "carried",
    [
        pytest.param(SECTION_OPEN_LINE, id="open-line"),
        pytest.param("<<<curie-learned-rules", id="open-delimiter"),
        pytest.param(SECTION_CLOSE_LINE, id="close-line"),
        pytest.param("<<<end-curie-learned-rules", id="close-delimiter"),
        pytest.param(LINES[0], id="rule-line-1"),
        pytest.param(LINES[1], id="rule-line-4"),
    ],
)
def test_the_guard_refuses_a_subagent_prompt_carrying_the_rules(tool: str, carried: str) -> None:
    hook = _hook(_applied_gate())
    assert _decide(hook, {"tool_name": tool, "tool_input": {"prompt": ORDINARY}}) == {}
    denied = _decide(hook, {"tool_name": tool, "tool_input": {"prompt": f"{ORDINARY}\n{carried}"}})
    assert _denied(denied)
    assert denied["hookSpecificOutput"]["hookEventName"] == "PreToolUse"


@pytest.mark.parametrize(
    "prompt",
    [
        pytest.param(ORDINARY, id="ordinary"),
        pytest.param(f"{ORDINARY} R1: check the error paths too.", id="other-r1-text"),
        pytest.param(f"{ORDINARY} R4: and R1: are reviewer labels here.", id="bare-labels"),
        pytest.param(f"{ORDINARY} Name tests after behavior", id="rule-body-without-line"),
    ],
)
def test_the_guard_passes_a_subagent_prompt_without_the_rules(prompt: str) -> None:
    hook = _hook(_applied_gate())
    assert _decide(hook, {"tool_name": "Agent", "tool_input": {"prompt": prompt}}) == {}


def test_the_guard_abstains_without_applied_rules_and_fails_closed() -> None:
    unapplied = _bound_gate()
    carrying = {"tool_name": "Agent", "tool_input": {"prompt": LINES[0]}}
    assert _decide(_hook(unapplied), carrying) == {}
    hook = _hook(_applied_gate())
    assert _denied(_decide(hook, {"tool_name": "Agent", "tool_input": "not a mapping"}))
    assert _denied(_decide(hook, "not a mapping either"))
    assert _denied(_decide(hook, carrying))


# --- 4. The real boot -------------------------------------------------------------------


@dataclass
class _Boot:
    requester_ack: dict[str, Any]
    rules_status: int
    rules_ack: dict[str, Any]
    final: Final
    bodies: list[dict[str, Any]]
    other: list[str]


def _boot_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rules_raw: bytes | None,
    *,
    requester_raw: bytes = REQUESTER_VALID,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]] = _messages,
    contract_refused: bool = False,
) -> _Boot:
    """Boot the real runner, deliver both envelopes and run one turn, all over HTTP."""

    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    config = _boot_config(tmp_path)

    async def go() -> _Boot:
        provider = _stand_in(handler)
        async with (
            TestServer(_contract_api(contract_refused), host="127.0.0.1") as api,
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
                    requester = await client.post(
                        "/v1/requester-context", data=requester_raw, headers=BEARER
                    )
                    requester_ack: dict[str, Any] = await requester.json()
                    rules_ack: dict[str, Any] = {}
                    rules_status = 0
                    if rules_raw is not None:
                        rules = await client.post(CONTEXT_PATH, data=rules_raw, headers=BEARER)
                        rules_ack = await rules.json()
                        rules_status = rules.status
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
            return _Boot(
                requester_ack,
                rules_status,
                rules_ack,
                finals[0],
                list(provider[_BODIES]),
                list(provider[_OTHER]),
            )

    return anyio.run(go)


@pytest.mark.parametrize(
    ("build", "reason"),
    [
        pytest.param(
            lambda: _json_bytes(_envelope(execution_request_id=OTHER_ID)),
            "wrong_execution",
            id="other-execution",
        ),
        pytest.param(
            lambda: _json_bytes(_envelope(execution_fence=FENCE + 1)),
            "wrong_fence",
            id="other-fence",
        ),
        pytest.param(
            lambda: _json_bytes(_envelope(envelope_version=2)), "unsupported_version", id="version"
        ),
        pytest.param(_over_text_envelope, "too_large", id="text-over-4096-bytes"),
        pytest.param(
            lambda: _json_bytes(_envelope(_selected(sha256=rules_sha256("other")))),
            "digest_mismatch",
            id="digest-mismatch",
        ),
        pytest.param(lambda: b"not json", "malformed_envelope", id="malformed"),
    ],
)
def test_a_refused_learned_rules_envelope_starts_no_model_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, build: Callable[[], bytes], reason: str
) -> None:
    booted = _boot_run(tmp_path, monkeypatch, build())
    # The requester envelope was applied: the learned rules alone block the start.
    assert booted.requester_ack["acknowledgment"]["outcome"] == "applied"
    assert (booted.rules_status, booted.rules_ack) == (
        200,
        {"acknowledgment": _ack("failed"), "reason": reason},
    )
    assert booted.bodies == []
    assert booted.other == []
    assert booted.final.text == blocked_explanation(reason)


def test_a_boot_whose_worker_never_delivers_the_rules_names_the_missing_envelope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An older worker never posts the envelope; the runner waits its deadline, then the
    # start answers offline with the specific reason, not the generic verification text.
    monkeypatch.setattr("curie_runner.memory_rules_context.DELIVERY_DEADLINE_SECONDS", 0.2)
    booted = _boot_run(tmp_path, monkeypatch, None)
    assert booted.rules_status == 0
    assert booted.bodies == []
    assert booted.final.text == blocked_explanation("missing_envelope")
    assert "missing_envelope" in booted.final.text


def test_a_preflight_blocked_boot_acknowledges_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    booted = _boot_run(tmp_path, monkeypatch, VALID, contract_refused=True)
    assert (booted.rules_status, booted.rules_ack) == (409, {"error": PREFLIGHT_BLOCKED})
    assert booted.final.text == CONTRACT_BLOCKED_EXPLANATION
    assert booted.bodies == []


@requires_cli
def test_an_applied_boot_carries_the_section_between_model_line_and_requester_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    booted = _boot_run(tmp_path, monkeypatch, VALID)
    assert booted.rules_ack == {"acknowledgment": _ack("applied", sha256=DIGEST)}
    assert booted.bodies, "an applied boot made no provider request"
    system = _system_text(booted.bodies[0])
    rules = Applied(rule_numbers=(1, 4), sha256=DIGEST, text=TEXT).section()
    requester = requester_section(WORK_ITEM, REQUESTER_DIGEST, REQUESTER_TEXT, learned_rules=True)
    assert system.count(SECTION_OPEN_LINE) == 1
    assert f"Configured model: {MODEL}\n\n{rules}\n\n{requester}" in system
    assert BLOCKED_EXPLANATION not in booted.final.text


@requires_cli
def test_an_absent_boot_reaches_the_model_without_the_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    booted = _boot_run(tmp_path, monkeypatch, ABSENT)
    assert booted.rules_ack == {"acknowledgment": _ack("none")}
    assert booted.bodies
    for body in booted.bodies:
        system = _system_text(body)
        assert "<<<curie-learned-rules" not in system and "<<<end-curie-learned-rules" not in system
    assert BLOCKED_EXPLANATION not in booted.final.text


CLEAN_REVIEW = "REVIEW-CONTROL-8c2d Review the diff against the acceptance criteria."
DIRTY_REVIEW = f"Review the diff. Also hold it to this:\n{LINES[1]}"


def _reviewer_script(
    reviewers: list[str],
) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    """The main session calls a contaminated then a clean reviewer; every request
    that is not of the main conversation is a reviewer's and is kept whole."""

    main = _scripted(
        [
            (
                "Agent",
                "toolu_dirty",
                {
                    "description": "review",
                    "prompt": DIRTY_REVIEW,
                    "subagent_type": "general-purpose",
                },
            ),
            (
                "Agent",
                "toolu_clean",
                {
                    "description": "review",
                    "prompt": CLEAN_REVIEW,
                    "subagent_type": "general-purpose",
                },
            ),
        ]
    )

    async def respond(request: web.Request) -> web.StreamResponse:
        body = await request.json()
        if "start the issue" not in _first_user_text(body):
            reviewers.append(json.dumps(body))
            return await _messages_from(request, body)
        return await main(_Replayed(request, body))  # type: ignore[arg-type]

    return respond


def _carries_rules(dumped: str) -> bool:
    return "curie-learned-rules" in dumped or any(
        json.dumps(line)[1:-1] in dumped for line in LINES
    )


@requires_cli
@pytest.mark.parametrize("mounted", [True, False], ids=["guard-mounted", "guard-removed"])
def test_reviewer_sessions_never_receive_the_learned_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mounted: bool
) -> None:
    if not mounted:
        # Control: without the production mount the same call reaches a reviewer.
        monkeypatch.setattr(boot, "build_learned_rules_guard", lambda _gate: {})
    reviewers: list[str] = []

    booted = _boot_run(tmp_path, monkeypatch, VALID, handler=_reviewer_script(reviewers))

    assert booted.rules_ack == {"acknowledgment": _ack("applied", sha256=DIGEST)}
    # The implementer's own prompt carries the section ...
    assert SECTION_OPEN_LINE in _system_text(booted.bodies[0])
    assert "Agent" in _tool_names(booted.bodies[0])
    results = _tool_results(booted.bodies[-1])
    # ... the clean reviewer runs in both boots, composed without the section.
    clean = [body for body in reviewers if "REVIEW-CONTROL-8c2d" in body]
    assert clean and not any(_carries_rules(body) for body in clean)
    assert not results["toolu_clean"].get("is_error")
    leaked = [body for body in reviewers if _carries_rules(body)]
    if mounted:
        assert results["toolu_dirty"].get("is_error") is True
        assert leaked == []
    else:
        assert leaked, "the unguarded control never delivered a rule line to a reviewer"
