"""The runner carries a connector's approval ticket to its gated call (ADR 0288).

A hosted connector's preparation tool returns a signed ``cat1`` ticket that the
model must copy into the gated tool's top-level ``approval_ticket``. These tests
prove the runner records the connector's exact ticket when the model's copy is
damaged, puts the grant's exact ticket back on the approved call, and leaves the
call alone in every case the rule does not cover. Tickets are signed with the
synthetic fixture key from ``tests/vectors/approver-ticket.json`` and checked
with the production verifier, ``curie_internal.approver_ticket.verify_preparation``,
the same function the API's ``verify_ticket`` calls.
"""

from __future__ import annotations

import base64
import hashlib
import json
import tracemalloc
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event, SessionStatus, parse_ndjson
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import (
    PermissionResultAllow,
    PermissionResultDeny,
    PreToolUseHookSpecificOutput,
    ToolPermissionContext,
)
from curie_internal.approver_ticket import (
    TicketError,
    execution_arguments_sha256,
    sign_ticket,
    verify_preparation,
)
from curie_runner import __main__ as boot
from curie_runner import ticket_carry
from curie_runner.__main__ import build_runner
from curie_runner.approval import ApprovalGate
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.approval import build_approval_hook, build_can_use_tool
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.ticket_carry import TicketCarry
from nacl.signing import SigningKey
from plugin_format import TOOL_POLICY_ENFORCEMENT, ToolPolicy
from runner_boot_session import initialized_session_sync

from .test_connectors import HOSTED, _bundle, _CapturedSession, _config_for

_VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "tests" / "vectors" / "approver-ticket.json").read_text(
        encoding="utf-8"
    )
)
_SIGNING_KEY = SigningKey(hashlib.sha256(_VECTORS["fixture_seed_label"].encode("utf-8")).digest())
_KEY_ID = "selector-test-1"
_NOW = 1_700_000_100
# The all-zero tenant id is the default single-tenant value. It appears three
# times in a ticket and encodes to long repeated base64 runs, the part a model
# most often drops when it copies the ticket.
_ZERO = "00000000-0000-0000-0000-000000000000"
_AGENT = "00000000-0000-4000-8000-000000000002"
_SERVER = "access"
_PREPARE = f"mcp__{_SERVER}__request_grant"
_GATED = f"mcp__{_SERVER}__apply_grant"
_ARGS = {"resource": "example-repo", "level": "write", "note": "café"}


def _principal(subject: str) -> dict[str, str]:
    return {"tenant_id": _ZERO, "issuer": "https://id.example.com", "subject": subject}


def _ticket(
    arguments: dict[str, Any],
    *,
    connector: str = _SERVER,
    tool: str = "apply_grant",
    nonce_seed: str = "one",
    issued_at: int = _NOW - 10,
) -> str:
    """A synthetic signed ticket bound to ``arguments`` (minus any ticket)."""

    nonce = base64.urlsafe_b64encode(hashlib.sha256(nonce_seed.encode()).digest())
    return sign_ticket(
        {
            "version": 1,
            "purpose": "approval_request",
            "tenant_id": _ZERO,
            "agent_id": _AGENT,
            "connector": connector,
            "tool": tool,
            "route": "access",
            "key_id": _KEY_ID,
            "requester": _principal("requester"),
            "nonce": nonce.rstrip(b"=").decode("ascii"),
            "issued_at": issued_at,
            "expires_at": issued_at + 300,
            "arguments_sha256": execution_arguments_sha256(arguments),
            "approvers": [_principal("approver")],
        },
        signing_key=_SIGNING_KEY,
    )


def _damaged(ticket: str) -> str:
    """The ticket with one 68-character piece of a repeated run dropped."""

    start = ticket.index("MDAwMDAw")
    damaged = ticket[:start] + ticket[start + 68 :]
    assert damaged != ticket and len(damaged) == len(ticket) - 68
    return damaged


def _verifies(arguments: dict[str, Any]) -> bool:
    """Whether the API's own preparation check would accept these arguments."""

    try:
        verify_preparation(
            arguments["approval_ticket"],
            public_key=_SIGNING_KEY.verify_key,
            key_id=_KEY_ID,
            now=_NOW,
            arguments_sha256=execution_arguments_sha256(arguments),
        )
    except TicketError:
        return False
    return True


def _reply(*tickets: str) -> list[dict[str, str]]:
    """A preparation tool's MCP reply, as content blocks holding JSON text."""

    return [
        {
            "type": "text",
            "text": json.dumps(
                {"apply_grant_arguments": [{**_ARGS, "approval_ticket": t} for t in tickets]}
            ),
        }
    ]


def _nested(depth: int, value: Any) -> Any:
    for _ in range(depth):
        value = [value]
    return value


def _gate(*, proxied: frozenset[str] = frozenset({_SERVER}), **kw: Any) -> ApprovalGate:
    gate = ApprovalGate(
        required=frozenset({_GATED}),
        route_by_tool={_GATED: "access"},
        ticket_carry=TicketCarry(),
        **kw,
    )
    gate.proxied_connector_servers = proxied
    return gate


def _callbacks(gate: ApprovalGate) -> tuple[Any, Any]:
    """The production PreToolUse and PostToolUse callbacks for ``gate``."""

    hooks = build_approval_hook(gate)
    (pre,) = hooks["PreToolUse"]
    (post,) = hooks["PostToolUse"]
    assert pre.matcher is None and post.matcher is None
    return pre.hooks[0], post.hooks[0]


def _observe(gate: ApprovalGate, tool: str, response: Any) -> dict[str, Any]:
    _, post = _callbacks(gate)
    return anyio.run(post, {"tool_name": tool, "tool_response": response}, "t0", {"signal": None})


def _decide(gate: ApprovalGate, tool: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    pre, _ = _callbacks(gate)
    return anyio.run(pre, {"tool_name": tool, "tool_input": tool_input}, "t1", {"signal": None})


# --- creation: the recorded approval carries the connector's exact ticket ----------


def test_a_damaged_copy_is_recorded_with_the_connectors_exact_ticket() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    assert _observe(gate, _PREPARE, _reply(ticket)) == {}

    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert not _verifies(model_call), "the damaged copy is what the API refuses today"
    output = _decide(gate, _GATED, model_call)

    assert output["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert gate.pending_granted_tool == _GATED
    assert gate.pending_granted_arguments == {**_ARGS, "approval_ticket": ticket}
    assert _verifies(gate.pending_granted_arguments)


@pytest.mark.parametrize("case", _VECTORS["positive"], ids=lambda c: c["name"])
def test_the_runner_digest_agrees_with_every_shared_ticket_vector(case: dict[str, Any]) -> None:
    # Cross-artifact contract: the shared vectors are signed for their own
    # arguments with the signer's digest. The runner restores a source only when
    # its digest of the call equals the ticket's, so its canonical rule equals
    # the signer's and the API's on every vector, Unicode and numbers included.
    connector, tool = case["payload"]["connector"], case["payload"]["tool"]
    gate = _gate(proxied=frozenset({connector}))
    gated = f"mcp__{connector}__{tool}"
    gate.required = frozenset({gated})
    _observe(gate, f"mcp__{connector}__prepare", {"ticket": case["ticket"]})

    payload, _, signature = case["ticket"].rpartition(".")
    damaged = f"{payload[:40]}{payload[44:]}.{signature}"
    model_call = {**case["arguments"], "approval_ticket": damaged}
    _decide(gate, gated, model_call)

    assert gate.pending_granted_arguments == {
        **case["arguments"],
        "approval_ticket": case["ticket"],
    }


def test_the_session_final_carries_the_exact_ticket_on_the_fake_tier() -> None:
    # The value the worker sends to the API as granted_arguments is the final's
    # approval_granted_arguments. Drive a whole turn: preparation result,
    # PostToolUse observation, then the gated call with a damaged copy.
    ticket = _ticket(_ARGS)
    gate = _gate()
    _, post = _callbacks(gate)
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id="p1", name=_PREPARE, input={"resource": "example-repo"})],
            model="m",
        ),
        UserMessage(content=[ToolResultBlock(tool_use_id="p1", content=_reply(ticket))]),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id="g1", name=_GATED, input={**_ARGS, "approval_ticket": _damaged(ticket)}
                )
            ],
            model="m",
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="",
        ),
    ]
    session = FakeModelSession(
        lambda: script, can_use_tool=build_can_use_tool(gate), post_tool_use_hook=post
    )
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="test",
        session_id="s-1",
        approval_gate=gate,
    )

    async def go() -> list[Any]:
        await runner.start()
        lines = [
            line
            async for line in runner.run_turn(Event(type="message", text="go", user="U1", ts="1"))
        ]
        return parse_ndjson("".join(lines))

    final = anyio.run(go)[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == _GATED
    assert final.approval_granted_arguments == {**_ARGS, "approval_ticket": ticket}
    assert _verifies(final.approval_granted_arguments)


# --- creation: every case outside the rule leaves the model's call alone -----------


@pytest.mark.parametrize("how", ["disallowed", "after-an-interrupting-denial"])
def test_the_fake_never_observes_a_result_for_a_call_that_never_ran(how: str) -> None:
    # The real CLI runs PostToolUse only for a call that executed. A call the
    # fake stopped never ran, so a result scripted for it in a later turn must
    # not reach the hook either.
    observed: list[str | None] = []

    async def post(_hook_input: Any, tool_use_id: str | None, _context: Any) -> dict[str, Any]:
        observed.append(tool_use_id)
        return {}

    stopped = ToolUseBlock(id="x1", name=_PREPARE, input={})
    options: dict[str, Any]
    if how == "disallowed":
        first = [AssistantMessage(content=[stopped], model="m")]
        options = {"disallowed_tools": [_PREPARE]}
    else:
        denied = ToolUseBlock(id="d1", name="mcp__other__deny", input={})
        first = [AssistantMessage(content=[denied, stopped], model="m")]

        async def deny_first(name: str, _input: Any, _context: Any) -> Any:
            if name == "mcp__other__deny":
                return PermissionResultDeny(message="no", interrupt=True)
            return PermissionResultAllow()

        options = {"can_use_tool": deny_first}
    result = ToolResultBlock(tool_use_id="x1", content=_reply(_ticket(_ARGS)))
    later = [UserMessage(content=[result])]
    scripts = iter([first, later])
    session = FakeModelSession(lambda: next(scripts), post_tool_use_hook=post, **options)

    async def run() -> None:
        for text in ("first turn", "second turn"):
            await session.query(text)
            async for _ in session.receive_turn():
                pass

    anyio.run(run)
    assert observed == []


def _held_arguments(gate: ApprovalGate, model_call: dict[str, Any]) -> dict[str, Any] | None:
    _decide(gate, _GATED, model_call)
    return gate.pending_granted_arguments


def test_two_tickets_for_the_same_details_each_restore_their_own_copy() -> None:
    first, second = _ticket(_ARGS, nonce_seed="a"), _ticket(_ARGS, nonce_seed="b")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(first))
    _observe(gate, _PREPARE, _reply(second))
    for source in (first, second):
        gate.reset()
        restored = {**_ARGS, "approval_ticket": source}
        assert _held_arguments(gate, {**_ARGS, "approval_ticket": _damaged(source)}) == restored


def test_a_copy_matching_two_remembered_tickets_is_left_unchanged() -> None:
    # A reply that also carries a ticket reusing the source's signature (a
    # forgery or a corrupted echo that still parses) leaves two possible
    # sources for the same evidence. Neither is chosen.
    ticket = _ticket(_ARGS)
    other = _ticket({**_ARGS, "resource": "other"}, nonce_seed="z")
    forged = f"{other.rpartition('.')[0]}.{ticket.rpartition('.')[2]}"
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket, forged))
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_no_remembered_ticket_leaves_the_call_unchanged() -> None:
    gate = _gate()
    model_call = {**_ARGS, "approval_ticket": _damaged(_ticket(_ARGS))}
    assert _held_arguments(gate, model_call) == model_call


def test_changed_details_do_not_match_the_ticket_for_other_details() -> None:
    gate = _gate()
    _observe(gate, _PREPARE, _reply(_ticket(_ARGS)))
    changed = {**_ARGS, "level": "admin", "approval_ticket": _damaged(_ticket(_ARGS))}
    assert _held_arguments(gate, changed) == changed


def test_a_copy_of_one_request_is_never_swapped_for_another_requests_ticket() -> None:
    # Ticket A was prepared for details X and ticket B for details Y. A call
    # with Y and a damaged copy of A names A as its source, and A does not fit
    # Y, so nothing is restored, even though B would fit.
    details_x, details_y = _ARGS, {**_ARGS, "resource": "other-repo"}
    first, second = _ticket(details_x, nonce_seed="a"), _ticket(details_y, nonce_seed="b")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(first))
    _observe(gate, _PREPARE, [{"type": "text", "text": second}])
    for copy in (_damaged(first), "", "cat1.x.y"):
        gate.reset()
        model_call = {**details_y, "approval_ticket": copy}
        assert _held_arguments(gate, model_call) == model_call


def test_a_call_without_a_ticket_argument_is_not_given_one() -> None:
    gate = _gate()
    _observe(gate, _PREPARE, _reply(_ticket(_ARGS)))
    assert _held_arguments(gate, dict(_ARGS)) == _ARGS


def test_a_ticket_from_a_server_that_is_not_a_hosted_connector_is_ignored() -> None:
    gate = _gate(proxied=frozenset({"other"}))
    assert gate.observe_tool_result(_PREPARE, _reply(_ticket(_ARGS))) == 0
    model_call = {**_ARGS, "approval_ticket": _damaged(_ticket(_ARGS))}
    assert _held_arguments(gate, model_call) == model_call


def test_a_gated_call_to_a_server_that_is_not_a_hosted_connector_is_never_rewritten() -> None:
    # Defense in depth behind the observer's own check: even a carry that holds
    # a matching ticket is not consulted for a server outside the proxied set.
    gate = _gate(proxied=frozenset({"other"}))
    ticket = _ticket(_ARGS)
    assert gate.ticket_carry.observe(_PREPARE, _reply(ticket), servers=frozenset({_SERVER})) == 1
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_a_ticket_naming_another_connector_is_not_remembered_from_this_one() -> None:
    # The "other" connector echoes a ticket that names "access". Only access
    # itself may be the source of an access ticket.
    gate = _gate(proxied=frozenset({_SERVER, "other"}))
    assert gate.observe_tool_result("mcp__other__echo", _reply(_ticket(_ARGS))) == 0
    model_call = {**_ARGS, "approval_ticket": _damaged(_ticket(_ARGS))}
    assert _held_arguments(gate, model_call) == model_call


def test_an_echo_naming_another_connector_still_makes_a_copy_ambiguous() -> None:
    # One hosted connector's reply carries its own ticket and an echo that names
    # another connector but reuses the ticket's signature. The echo is never
    # chosen, but the copy could come from it, so neither is restored.
    ticket = _ticket(_ARGS)
    other = _ticket(_ARGS, connector="other", nonce_seed="z")
    echo = f"{other.rpartition('.')[0]}.{ticket.rpartition('.')[2]}"
    gate = _gate(proxied=frozenset({_SERVER, "other"}))
    assert gate.observe_tool_result(_PREPARE, _reply(ticket, echo)) == 1
    model_call = {**_ARGS, "approval_ticket": _damaged(echo)}
    assert _damaged(echo).rpartition(".")[2] == ticket.rpartition(".")[2]
    assert _held_arguments(gate, model_call) == model_call


def test_a_ticket_first_seen_as_an_echo_becomes_a_source_when_its_connector_returns_it() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate(proxied=frozenset({_SERVER, "other"}))
    assert gate.observe_tool_result("mcp__other__echo", _reply(ticket)) == 0
    assert gate.observe_tool_result(_PREPARE, _reply(ticket)) == 1
    assert len(gate.ticket_carry._entries) == 1
    restored = {**_ARGS, "approval_ticket": ticket}
    assert _held_arguments(gate, {**_ARGS, "approval_ticket": _damaged(ticket)}) == restored


def test_a_ticket_for_another_tool_does_not_match() -> None:
    other_tool = _ticket(_ARGS, tool="revoke_grant")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(other_tool))
    model_call = {**_ARGS, "approval_ticket": _damaged(other_tool)}
    assert _held_arguments(gate, model_call) == model_call


def test_freshness_is_left_to_the_api() -> None:
    # The carry restores the source whatever its claimed expiry; the API then
    # answers preparation_expired, which the requester is told to ask again for.
    stale = _ticket(_ARGS, issued_at=_NOW - 400)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(stale))
    restored = _held_arguments(gate, {**_ARGS, "approval_ticket": _damaged(stale)})
    assert restored == {**_ARGS, "approval_ticket": stale}


def test_malformed_and_plugin_sources_are_ignored() -> None:
    gate = _gate(proxied=frozenset({_SERVER, "plugin_b_access"}))
    ticket = _ticket(_ARGS)
    assert gate.observe_tool_result(_PREPARE, {"t": "cat1.bm90LWpzb24.c2ln"}) == 0
    assert gate.observe_tool_result(_PREPARE, {"t": _damaged(ticket)}) == 0
    assert gate.observe_tool_result("mcp__plugin_b_access__x", _reply(ticket)) == 0
    assert gate.observe_tool_result("Bash", _reply(ticket)) == 0
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_a_copy_with_a_shortened_signature_gets_its_identical_payload_back() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    payload, _, signature = ticket.rpartition(".")
    damaged = f"{payload}.{signature[:-4]}"
    assert _held_arguments(gate, {**_ARGS, "approval_ticket": damaged}) == {
        **_ARGS,
        "approval_ticket": ticket,
    }


def test_a_copy_keeps_its_source_when_its_damaged_signature_gains_a_dot() -> None:
    # The evidence is the copy's ends: it starts with the source's exact
    # ``cat1.<payload>.``, whatever follows that prefix.
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    payload, _, signature = ticket.rpartition(".")
    damaged = f"{payload}.{signature[:10]}.{signature[10:]}"
    assert _held_arguments(gate, {**_ARGS, "approval_ticket": damaged}) == {
        **_ARGS,
        "approval_ticket": ticket,
    }


def test_the_evidence_is_anchored_at_the_copys_ends() -> None:
    # The source's payload or signature appearing elsewhere in the copy, not
    # as its exact prefix or suffix, is no evidence: the call is left alone.
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    payload, _, signature = ticket.rpartition(".")
    body = payload.removeprefix("cat1.")
    for copy in (f"cat1.x{body}.{signature[:-4]}", f"{payload[:-4]}.{signature}x"):
        gate.reset()
        model_call = {**_ARGS, "approval_ticket": copy}
        assert _held_arguments(gate, model_call) == model_call


def test_a_copy_that_still_parses_is_never_replaced() -> None:
    # Same-length signature damage parses like a different ticket would, and an
    # intact ticket the carry never saw parses too. Neither is replaced.
    seen, unseen = _ticket(_ARGS, nonce_seed="b"), _ticket(_ARGS, nonce_seed="a")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(seen))
    payload, _, signature = seen.rpartition(".")
    for copy in (unseen, f"{payload}.AAAA{signature[4:]}"):
        gate.reset()
        model_call = {**_ARGS, "approval_ticket": copy}
        assert _held_arguments(gate, model_call) == model_call


def test_forgetting_a_ticket_stops_the_carry_for_the_conversation() -> None:
    # A forgotten string could be the second possible source of a later copy,
    # so after the memory bound is reached nothing is restored at all.
    first = _ticket(_ARGS, nonce_seed="a")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(first))
    for index in range(32):
        other = {**_ARGS, "resource": f"other-{index}"}
        _observe(gate, _PREPARE, [{"type": "text", "text": _ticket(other, nonce_seed=str(index))}])
    second = _ticket(_ARGS, nonce_seed="b")
    _observe(gate, _PREPARE, _reply(second))
    assert all(entry.token != first for entry in gate.ticket_carry._entries)

    for copy in (first, _damaged(first), _damaged(second)):
        gate.reset()
        model_call = {**_ARGS, "approval_ticket": copy}
        assert _held_arguments(gate, model_call) == model_call


@pytest.mark.parametrize(
    "reply",
    [["x" * 300_000], [*range(20_000)], _nested(40, "deep"), ["cat1. " * 70]],
    ids=["characters", "nodes", "depth", "candidates"],
)
def test_a_reply_read_only_in_part_stops_the_carry(reply: Any) -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    _observe(gate, _PREPARE, reply)
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_a_ticket_in_a_key_or_after_a_malformed_prefix_is_found() -> None:
    ticket = _ticket(_ARGS)
    for reply in ({ticket: 0}, [f"cat1.x.{ticket}"], [f"see cat1.cat1.{ticket} here"]):
        gate = _gate()
        assert gate.observe_tool_result(_PREPARE, reply) == 1, reply


def test_a_token_cut_by_the_character_bound_is_not_remembered() -> None:
    # The reply really carries the ticket plus one more character, so the
    # complete token is not a ticket. A window that ends right after the ticket
    # must not manufacture it.
    ticket = _ticket(_ARGS)
    gate = _gate()
    padding = "x" * (ticket_carry.MAX_SCAN_CHARS - len(ticket))
    assert gate.observe_tool_result(_PREPARE, [padding + ticket + "A"]) == 0
    assert gate.observe_tool_result(_PREPARE, [padding[:-1] + ticket + " "]) == 1


def test_the_scan_stops_at_its_depth_and_candidate_bounds() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    assert gate.observe_tool_result(_PREPARE, _nested(40, ticket)) == 0
    assert gate.observe_tool_result(_PREPARE, ["cat1. " * 64 + ticket]) == 0
    assert gate.observe_tool_result(_PREPARE, ["cat1. " * 10 + ticket]) == 1


def test_the_scan_stops_at_its_node_bound() -> None:
    # A ticket past the node bound is not read: the reply was read only in part,
    # which can only lose a source, never invent one.
    ticket = _ticket(_ARGS)
    gate = _gate()
    assert gate.observe_tool_result(_PREPARE, [*range(20_000), ticket]) == 0
    assert gate.observe_tool_result(_PREPARE, [*range(100), ticket]) == 1


def test_a_copy_must_look_like_a_ticket_and_fit_its_size() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    signature = ticket.rpartition(".")[2]
    huge = "cat1." + "A" * 20_000 + "." + signature
    for copy in (signature, "x." + signature, huge):
        gate.reset()
        model_call = {**_ARGS, "approval_ticket": copy}
        assert _held_arguments(gate, model_call) == model_call


def test_evidence_shared_with_another_tools_ticket_is_ambiguous() -> None:
    # A forged string for another tool reuses the source's signature. The
    # evidence names two remembered strings, so neither is chosen, even though
    # only one is for the called tool.
    ticket = _ticket(_ARGS)
    other = _ticket(_ARGS, tool="revoke_grant", nonce_seed="z")
    forged = f"{other.rpartition('.')[0]}.{ticket.rpartition('.')[2]}"
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket, forged))
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_the_carry_also_runs_for_a_policy_approval_required_tool() -> None:
    ticket = _ticket(_ARGS)
    gate = ApprovalGate(
        tool_policy=ToolPolicy(
            enforcement=TOOL_POLICY_ENFORCEMENT, approvalRequired=[f"{_SERVER}/apply_grant"]
        ),
        mcp_servers=set(),
        connector_servers={_SERVER},
    )
    gate.proxied_connector_servers = frozenset({_SERVER})
    gate.observe_tool_result(_PREPARE, _reply(ticket))
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": _damaged(ticket)})
    assert output["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert gate.pending_granted_arguments == {**_ARGS, "approval_ticket": ticket}


def test_the_scan_of_a_wide_reply_costs_bounded_memory() -> None:
    gate = _gate()
    wide = [list(range(1_000_000))]
    tracemalloc.start()
    try:
        gate.observe_tool_result(_PREPARE, wide)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 1_000_000


def test_an_ungated_connector_call_is_never_rewritten() -> None:
    # A repairable copy for an ungated tool: the hook abstains, so the call runs
    # with exactly what the model wrote and nothing is recorded.
    ticket = _ticket(_ARGS, tool="record")
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    output = _decide(gate, f"mcp__{_SERVER}__record", model_call)
    assert output == {}
    assert gate.pending_granted_arguments is None
    result = anyio.run(
        build_can_use_tool(gate),
        f"mcp__{_SERVER}__record",
        model_call,
        ToolPermissionContext(tool_use_id="u1"),
    )
    assert isinstance(result, PermissionResultAllow) and result.updated_input is None


def test_the_observer_never_raises_and_never_changes_a_result() -> None:
    gate = _gate()

    class Exploding(dict[str, Any]):
        def get(self, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("synthetic accessor failure")

    for hook_input in (
        {},
        {"tool_name": 3},
        {"tool_name": _PREPARE, "tool_response": object()},
        Exploding(),
    ):
        _, post = _callbacks(gate)
        assert anyio.run(post, hook_input, None, {"signal": None}) == {}


class _Exploding(dict[str, Any]):
    def get(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("synthetic accessor failure")


@pytest.mark.parametrize(
    "hook_input",
    [
        {"tool_name": _PREPARE, "tool_response": [object()]},
        _Exploding(),
        {"tool_name": _PREPARE},
        {"tool_response": _reply(_ticket(_ARGS, nonce_seed="hidden"))},
    ],
    ids=["unreadable-value", "raising-accessor", "missing-reply", "missing-tool-name"],
)
def test_a_failed_observation_stops_the_carry(hook_input: Any) -> None:
    # The reply that could not be read may hold a second possible source, so a
    # copy of a ticket remembered earlier is no longer restored or repaired.
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, _reply(ticket))
    _, post = _callbacks(gate)
    assert anyio.run(post, hook_input, None, {"signal": None}) == {}
    model_call = {**_ARGS, "approval_ticket": _damaged(ticket)}
    assert _held_arguments(gate, model_call) == model_call


def test_json_scalars_do_not_stop_the_carry() -> None:
    ticket = _ticket(_ARGS)
    gate = _gate()
    _observe(gate, _PREPARE, [None, True, 3, 2.5, {"n": None}, *_reply(ticket)])
    restored = {**_ARGS, "approval_ticket": ticket}
    assert _held_arguments(gate, {**_ARGS, "approval_ticket": _damaged(ticket)}) == restored


def _post_tool_use_names(hooks: dict[str, Any]) -> list[str]:
    """The PostToolUse callbacks by name. The question stop (#5785) is always there."""

    return [hook.__name__ for matcher in hooks.get("PostToolUse", []) for hook in matcher.hooks]


def test_no_observer_is_registered_without_a_hosted_connector() -> None:
    hooks = build_approval_hook(_gate(proxied=frozenset()))
    assert _post_tool_use_names(hooks) == ["questions_stop"]


@pytest.mark.parametrize(("caller_token", "observed"), [("cct.a.b", True), (None, False)])
def test_the_boot_registers_the_observer_only_for_proxied_connectors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caller_token: str | None, observed: bool
) -> None:
    # Wiring pin: the observer exists in the real SDK options exactly when the
    # boot handed the gate a proxied connector before building the hooks.
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    root = _bundle(tmp_path, HOSTED)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": "b",
                "version": "0.1.0",
                "description": "t",
                "approvalPolicy": {
                    "gates": [{"gate": "mcp__grafana__update_dashboard", "route": "ops"}]
                },
            }
        ),
        encoding="utf-8",
    )
    config = _config_for(
        root, release="curie", agent="acme-dev", namespace="curie", caller_token=caller_token
    )

    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        return McpToolCapabilityProbe(complete=True, has_potential_write_tool=True, tool_count=1)

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    runner = build_runner(config, fake_model=False)
    options = initialized_session_sync(runner).options
    assert ("ticket_observer" in _post_tool_use_names(options.hooks)) is observed
    if observed:
        gate = runner._approval_gate
        assert gate is not None
        (matcher,) = options.hooks["PostToolUse"]
        reply = {"t": _ticket(_ARGS, connector="grafana", tool="update_dashboard")}
        anyio.run(
            matcher.hooks[0],
            {"tool_name": "mcp__grafana__prepare", "tool_response": reply},
            None,
            {"signal": None},
        )
        assert len(gate.ticket_carry._entries) == 1


# --- the approved call: the grant's exact ticket reaches the connector -------------


def _granted_gate(ticket: str) -> ApprovalGate:
    return _gate(grant_tool=_GATED, grant_arguments={**_ARGS, "approval_ticket": ticket})


def test_the_approved_call_runs_with_the_grants_exact_ticket() -> None:
    ticket = _ticket(_ARGS)
    gate = _granted_gate(ticket)
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": _damaged(ticket)})

    decision = output["hookSpecificOutput"]
    assert decision["permissionDecision"] == "allow"
    assert decision["updatedInput"] == {**_ARGS, "approval_ticket": ticket}
    assert set(decision) <= set(PreToolUseHookSpecificOutput.__annotations__)
    assert gate.grant_tool is None, "the one-shot grant is spent"


def test_the_permission_callback_returns_the_same_updated_input() -> None:
    ticket = _ticket(_ARGS)
    gate = _granted_gate(ticket)
    result = anyio.run(
        build_can_use_tool(gate),
        _GATED,
        {**_ARGS, "approval_ticket": _damaged(ticket)},
        ToolPermissionContext(tool_use_id="g2"),
    )
    assert isinstance(result, PermissionResultAllow)
    assert result.updated_input == {**_ARGS, "approval_ticket": ticket}


def test_an_exact_approved_call_is_allowed_without_rewriting() -> None:
    ticket = _ticket(_ARGS)
    gate = _granted_gate(ticket)
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": ticket})
    assert output["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert "updatedInput" not in output["hookSpecificOutput"]


@pytest.mark.parametrize(
    "stored",
    ["reference.42", _ticket(_ARGS, tool="revoke_grant")],
    ids=["not-a-ticket", "a-ticket-for-another-tool"],
)
def test_the_grant_is_repaired_only_from_a_stored_ticket_for_this_tool(stored: str) -> None:
    gate = _granted_gate(stored)
    copy = "cat1.typo.42" if stored == "reference.42" else _damaged(stored)
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": copy})
    assert output["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert gate.grant_tool == _GATED


def _seen_since_resume(kind: str, ticket: str) -> Any:
    """One PostToolUse input the resumed runner observes before the approved call."""

    if kind == "failed":
        return _Exploding()
    if kind == "unrelated-echo":
        return {"tool_name": _PREPARE, "tool_response": _reply(_ticket(_ARGS, connector="other"))}
    # A string that reuses the granted ticket's signature, so the model's copy
    # is also a damaged copy of it: a forged ticket for other details from the
    # same connector, or an echo of a ticket naming another connector.
    other = (
        _ticket({**_ARGS, "resource": "other"}, nonce_seed="z")
        if kind == "shared-evidence"
        else _ticket(_ARGS, connector="other", nonce_seed="z")
    )
    shared = f"{other.rpartition('.')[0]}.{ticket.rpartition('.')[2]}"
    return {"tool_name": _PREPARE, "tool_response": _reply(shared)}


@pytest.mark.parametrize(
    "kind", ["shared-evidence", "echo-sharing-the-evidence", "unrelated-echo", "failed"]
)
def test_the_grant_repair_reads_no_runner_memory(kind: str) -> None:
    # ADR 0288 decision 2: the grant's stored arguments are the only trust
    # anchor. The repaired call is the approved call byte for byte, so nothing
    # the runner observed since the resume (and nothing it failed to observe)
    # changes the repair.
    ticket = _ticket(_ARGS)
    gate = _gate(
        proxied=frozenset({_SERVER, "other"}),
        grant_tool=_GATED,
        grant_arguments={**_ARGS, "approval_ticket": ticket},
    )
    _, post = _callbacks(gate)
    assert anyio.run(post, _seen_since_resume(kind, ticket), None, {"signal": None}) == {}
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": _damaged(ticket)})
    assert output["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert output["hookSpecificOutput"]["updatedInput"] == {**_ARGS, "approval_ticket": ticket}
    assert gate.grant_tool is None


def test_the_grant_is_not_repaired_for_a_tool_outside_the_hosted_connectors() -> None:
    # An ordinary gated tool may have its own argument named approval_ticket.
    # It is compared like any other argument: a different value is refused and
    # the grant stays available for the exact call.
    ticket = _ticket(_ARGS)
    gate = _gate(
        proxied=frozenset({"other"}),
        grant_tool=_GATED,
        grant_arguments={**_ARGS, "approval_ticket": ticket},
    )
    output = _decide(gate, _GATED, {**_ARGS, "approval_ticket": _damaged(ticket)})
    assert output["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert gate.grant_tool == _GATED
    result = anyio.run(
        build_can_use_tool(gate),
        _GATED,
        {**_ARGS, "approval_ticket": _damaged(ticket)},
        ToolPermissionContext(tool_use_id="g4"),
    )
    assert isinstance(result, PermissionResultDeny)
    assert gate.grant_tool == _GATED


@pytest.mark.parametrize(
    "model_call",
    [
        {**_ARGS, "level": "admin", "approval_ticket": _damaged(_ticket(_ARGS))},
        dict(_ARGS),
        {**_ARGS, "approval_ticket": None},
        {**_ARGS, "approval_ticket": _ticket(_ARGS, nonce_seed="other")},
    ],
    ids=["different-details", "ticket-omitted", "ticket-null", "a-different-complete-ticket"],
)
def test_other_differences_still_meet_the_grant_mismatch_refusal(
    model_call: dict[str, Any],
) -> None:
    ticket = _ticket(_ARGS)
    gate = _granted_gate(ticket)
    output = _decide(gate, _GATED, model_call)
    assert output["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert (
        "covers only the approved details"
        in output["hookSpecificOutput"]["permissionDecisionReason"]
    )
    assert gate.grant_tool == _GATED, "the grant stays unspent for the exact call"
    result = anyio.run(
        build_can_use_tool(gate), _GATED, model_call, ToolPermissionContext(tool_use_id="g3")
    )
    assert isinstance(result, PermissionResultDeny)
