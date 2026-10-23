"""Run authority comes only from the verified caller token (ADR 0239, AC3).

"Bundle, identity and request contract" 2: action execution requires run and
work_item, which arrive only as the token's paired claims; ADR 0178's deadline
bound stands, so a handle never outlives the token's (deadline-capped) exp.
The proxy takes the invocation's run and work item from the verified token
alone (``shared.run_context``), refuses a tool in the policy's ``run_tools``
without them, and never reads headers, arguments or the session for them. The
shared server receives the verified pair through the resolver (plan R1-1).

``tests/vectors/connector-shared-caller.json`` freezes the cases. They are
driven twice: through the pure decision (``caller.decide_shared`` then
``run_context``) with the frozen tokens, and through the real proxy, fixture
MCP server and resolver socket with tokens minted for a live member. The live
token keeps each case's claims except identity: the installation becomes the
application's (or stays blank, or becomes another installation, as the case's
claim relates to its ``installation``), the agent becomes the member, and exp
keeps the case's distance from ``now``.
"""

from __future__ import annotations

import inspect
import json
import re
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _resolver_harness import Fleet, Member
from _shared_connector_support import random_installation
from _shared_proxy_harness import (
    ALIAS_HEADER,
    CALLER_HEADER,
    CALLER_TOKEN_VECTOR_FILE,
    SHARED_VECTOR_FILE,
    ProxyReply,
    SharedProxy,
    Worlds,
    refusal_table,
    shared_vector,
    wait_until,
)
from curie_connector_proxy import caller

TOP_KEYS = {
    "comment",
    "signed_by",
    "header",
    "alias_header",
    "jsonrpc_error_code",
    "cases",
    "refusals",
}
CASE_KEYS = {
    "name",
    "why",
    "claims",
    "token",
    "now",
    "installation",
    "tool",
    "run_tools",
    "spoof",
    "deadline",
    "expect",
}
CLAIM_KEYS = {"installation", "agent_id", "agent", "exp"}
PAIR_KEYS = {"run", "work_item"}
SPOOF_KEYS = {"headers", "arguments"}
ADMITTED_KEYS = {"outcome", "context"}
CONTEXT_KEYS = {"run", "work_item", "expires_at"}
REFUSED_KEYS = {"outcome", "status", "curie_caller", "curie_shared"}
REFUSAL_ROW_KEYS = {"code", "status", "curie_caller"}
WHOLE_TOKEN = re.compile(r"(?:cct2|ccg|csg)\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
TOKEN_REFUSALS = {"missing", "invalid", "expired", "not_admitted"}

CASES: list[dict[str, Any]] = shared_vector()["cases"]


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _token(segments: object) -> str:
    """A frozen token, stored as its "."-separated segments, joined back."""

    assert isinstance(segments, list) and all(isinstance(part, str) for part in segments)
    assert len(segments) == 3, segments
    return ".".join(segments)


def _signer() -> dict[str, Any]:
    corpus = json.loads(CALLER_TOKEN_VECTOR_FILE.read_text(encoding="utf-8"))
    # The first seed of the caller-token corpus, as the comment says.
    signer: dict[str, Any] = corpus["vectors"][0]
    assert signer["name"] == shared_vector()["signed_by"]
    return signer


def _admitted(reply: ProxyReply) -> dict[str, Any]:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    return result


# =============================================================================
# The vector itself
# =============================================================================


def test_run_vector_carries_only_known_keys() -> None:
    document = shared_vector()
    assert set(document) == TOP_KEYS
    assert document["header"] == CALLER_HEADER == caller.HEADER
    assert document["alias_header"] == ALIAS_HEADER
    assert document["jsonrpc_error_code"] == -32000
    comment = document["comment"]
    for reader in ("test_shared_proxy_run_authority.py", "_shared_proxy_harness.py"):
        assert reader in comment, reader
    names = [case["name"] for case in document["cases"]]
    assert len(names) == len(set(names))
    for case in document["cases"]:
        assert set(case) == CASE_KEYS, case["name"]
        claims = case["claims"]
        assert set(claims) in (CLAIM_KEYS, CLAIM_KEYS | PAIR_KEYS), case["name"]
        assert set(case["spoof"]) == SPOOF_KEYS, case["name"]
        assert isinstance(case["run_tools"], list), case["name"]
        assert case["deadline"] is None or isinstance(case["deadline"], int), case["name"]
        expect = case["expect"]
        if expect["outcome"] == "admitted":
            assert set(expect) == ADMITTED_KEYS, case["name"]
            assert set(expect["context"]) == CONTEXT_KEYS, case["name"]
        else:
            assert expect["outcome"] == "refused", case["name"]
            assert set(expect) == REFUSED_KEYS, case["name"]
    for row in document["refusals"]:
        assert set(row) == REFUSAL_ROW_KEYS, row
    codes = [row["code"] for row in document["refusals"]]
    assert len(codes) == len(set(codes))
    # Every refused case names a code in the table, with the table's status and value.
    table = refusal_table()
    for case in document["cases"]:
        expect = case["expect"]
        if expect["outcome"] == "refused":
            assert table[expect["curie_shared"]] == (expect["status"], expect["curie_caller"])


def test_run_vector_stores_no_whole_token() -> None:
    text = SHARED_VECTOR_FILE.read_text(encoding="utf-8")
    assert WHOLE_TOKEN.search(text) is None, "a whole token string is in the vector"
    for case in shared_vector()["cases"]:
        segments = case["token"]
        assert isinstance(segments, list) and len(segments) == 3, case["name"]
        assert segments[0] == "cct2", case["name"]


def test_run_vector_tokens_are_the_minters_bytes_under_the_first_seed() -> None:
    from curie_worker.caller_token import mint

    signer = _signer()
    keys = (caller.public_key(str(signer["public"])),)
    for case in CASES:
        claims = case["claims"]
        minted = mint(
            str(signer["seed"]),
            installation=claims["installation"],
            agent_id=claims["agent_id"],
            agent=claims["agent"],
            exp=claims["exp"],
            run=claims.get("run"),
            work_item=claims.get("work_item"),
        )
        assert minted == _token(case["token"]), case["name"]
        carried = caller.claims(keys, minted)
        assert carried is not None, case["name"]
        assert (carried.installation, carried.agent_id, carried.agent, carried.exp) == (
            claims["installation"],
            claims["agent_id"],
            claims["agent"],
            claims["exp"],
        )
        assert (carried.run, carried.work_item) == (claims.get("run"), claims.get("work_item"))


# =============================================================================
# The pure decision
# =============================================================================


def test_run_context_reads_only_the_decision() -> None:
    from curie_connector_proxy import shared

    parameters = inspect.signature(shared.run_context).parameters
    assert list(parameters) == ["decision", "requires_run"]
    assert parameters["requires_run"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_run_vector_case_through_the_pure_decision(case: dict[str, Any]) -> None:
    from curie_connector_proxy import shared

    keys = (caller.public_key(str(_signer()["public"])),)
    decision = caller.decide_shared(
        keys, _token(case["token"]), installation=case["installation"], now=case["now"]
    )
    expect = case["expect"]
    if decision.refusal is not None:
        assert expect["outcome"] == "refused", (case["name"], decision.refusal)
        assert expect["curie_shared"] in TOKEN_REFUSALS
        assert decision.refusal == expect["curie_shared"], case["name"]
        assert shared.REFUSALS[decision.refusal] == (expect["status"], expect["curie_caller"])
        return
    requires_run = case["tool"] in case["run_tools"]
    if expect["outcome"] == "refused":
        with pytest.raises(shared.SharedRefusal) as refused:
            shared.run_context(decision, requires_run=requires_run)
        assert refused.value.code == expect["curie_shared"] == "run_required"
        assert str(refused.value) == "run_required"
        assert shared.REFUSALS["run_required"] == (expect["status"], expect["curie_caller"])
        return
    context = expect["context"]
    assert shared.run_context(decision, requires_run=requires_run) == (
        context["run"],
        context["work_item"],
    )
    # The handle's expiry is the token's exp: never past a run deadline.
    assert decision.exp == context["expires_at"] == case["claims"]["exp"]
    if case["deadline"] is not None:
        assert context["expires_at"] <= case["deadline"]


# =============================================================================
# Through the real proxy, server and resolver
# =============================================================================


def _live_token(world: SharedProxy, member: Member, case: dict[str, Any]) -> tuple[str, int]:
    claims = case["claims"]
    if claims["installation"] == "":
        installation = ""
    elif claims["installation"] == case["installation"]:
        installation = world.app.installation
    else:
        installation = random_installation()
    exp = int(time.time()) + (claims["exp"] - case["now"])
    token = world.token(
        member,
        installation=installation,
        exp=exp,
        run=claims.get("run"),
        work_item=claims.get("work_item"),
    )
    return token, exp


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_run_vector_case_through_the_real_proxy(worlds: Worlds, case: dict[str, Any]) -> None:
    world = worlds(run_tools=tuple(case["run_tools"]))
    member = world.member()
    token, exp = _live_token(world, member, case)
    before = world.observe()
    reply = world.call(
        member,
        case["tool"],
        case["spoof"]["arguments"],
        tokens=[token],
        extra=list(case["spoof"]["headers"].items()),
    )
    expect = case["expect"]
    if expect["outcome"] == "refused":
        assert reply.status == expect["status"], reply.text
        world.assert_refused(reply, expect["curie_shared"], before)
        return
    result = _admitted(reply)
    context = expect["context"]
    assert result["binding"] == member.binding_id
    assert result["authorization_sha256"] == world.digest(member)
    # expires_at keeps the case's relation to exp (equal to it).
    assert result["expires_at"] == exp + (context["expires_at"] - case["claims"]["exp"])
    if case["tool"] == "run_task":
        assert (result["run"], result["work_item"]) == (context["run"], context["work_item"])
    seen = world.fixture.since(before.upstream)
    for request in seen:
        assert not {"x-curie-run", "x-curie-work-item"} & request.names


def test_run_a_run_tool_without_a_pair_never_reaches_the_server(worlds: Worlds) -> None:
    world = worlds(run_tools=("run_task",))
    member = world.member()
    before = world.observe()
    world.assert_refused(world.call(member, "run_task"), "run_required", before)
    # Liveness: the same member with a paired token is admitted with that pair.
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    reply = world.call(member, "run_task", token_options={"run": run, "work_item": work_item})
    result = _admitted(reply)
    assert (result["run"], result["work_item"]) == (run, work_item)


def test_run_an_ordinary_tool_passes_exactly_the_tokens_pair_or_none(worlds: Worlds) -> None:
    """Without run_tools, run_task is ordinary: the server sees the token's pair or none."""

    world = worlds()
    member = world.member()
    spoof = [
        ("X-Curie-Run", str(uuid.uuid4())),
        ("X-Curie-Work-Item", str(uuid.uuid4())),
    ]
    unpaired = _admitted(world.call(member, "run_task", extra=spoof))
    assert (unpaired["run"], unpaired["work_item"]) == (None, None)
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    paired = _admitted(
        world.call(
            member,
            "run_task",
            {"run": spoof[0][1], "work_item": spoof[1][1]},
            extra=spoof,
            token_options={"run": run, "work_item": work_item},
        )
    )
    assert (paired["run"], paired["work_item"]) == (run, work_item)


def test_run_the_registry_context_carries_exactly_the_tokens_pair(worlds: Worlds) -> None:
    world = worlds(run_tools=("hold",))
    member = world.member()
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    exp = int(time.time()) + 1100
    hold, future = world.start_hold(
        member,
        "pair",
        token_options={"run": run, "work_item": work_item, "exp": exp},
        extra=[("X-Curie-Run", str(uuid.uuid4())), ("X-Curie-Work-Item", str(uuid.uuid4()))],
    )
    assert hold.handle is not None
    context = world.context(hold.handle)
    assert (context.run, context.work_item, context.expires_at) == (run, work_item, exp)
    reply = world.finish_hold(hold, future)
    assert _admitted(reply)["second"]["binding"] == member.binding_id


def test_run_a_paired_session_cannot_lend_its_pair_to_an_unpaired_call(worlds: Worlds) -> None:
    world = worlds(run_tools=("run_task",))
    member = world.member()
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    paired = {"run": run, "work_item": work_item}
    session = world.initialize(member, token_options=paired)
    # Liveness: the paired opener's own run call is admitted with its pair.
    opened = _admitted(world.call(member, "run_task", session=session, token_options=paired))
    assert (opened["run"], opened["work_item"]) == (run, work_item)
    # An unpaired token on the same session, with the pair in headers and
    # arguments, still has no pair.
    before = world.observe()
    reply = world.call(
        member,
        "run_task",
        {"run": run, "work_item": work_item},
        session=session,
        extra=[("X-Curie-Run", run), ("X-Curie-Work-Item", work_item)],
    )
    world.assert_refused(reply, "run_required", before)
    # The same unpaired token is admitted for an ordinary tool on that session.
    assert _admitted(world.call(member, "whoami", session=session))["binding"] == (
        member.binding_id
    )


def test_run_the_handle_expires_with_the_token(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    exp = int(time.time()) + 15
    hold, future = world.start_hold(member, "expiry", token_options={"exp": exp})
    assert hold.first is not None and hold.first["expires_at"] == exp
    wait_until(lambda: time.time() >= exp + 1, "the token's exp to pass", timeout=60)
    reply = world.finish_hold(hold, future)
    outcome = _admitted(reply)
    assert outcome["second"] == {"refused": "handle_expired", "status": 401}
    assert world.provider.calls("/v1/hold/second") == []
    world.wait_for_invocations(0)
