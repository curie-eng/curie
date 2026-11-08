"""ADR 0271 decision 7: the caller proxy attests a spent grant to the connector.

``X-Curie-Grant-Verified`` reaches a connector only from its own proxy, once,
carrying the canonical UUID ``jti`` of the grant this very request spent. Every
inbound copy is forged and dropped on every path. A gated tool keeps its strict
grant rule. An ungated tool is attested only when the presented grant verifies
and names this connector, this tool and these canonical arguments; any other
grant is the runner's static header riding along on an unrelated call, which
forwards unchanged and spends nothing. A batch is never attested, and a spent
grant is never replayable, even after an upstream that never answered.

Driven over real sockets with the helpers of ``test_connector_proxy.py``. The
one upstream added here reads a request whole and hangs up without answering.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, server
from multidict import CIMultiDict

from .test_connector_proxy import (
    _BEFORE_EXPIRY,
    _CONNECTOR,
    _GATED_TOOLS,
    _GRANT_SEED,
    _MERGE_CALL,
    _REFUSAL,
    _REVIEW_ROUTE,
    _TOKEN,
    _VECTOR,
    MemoryGrantStore,
    _caller_headers,
    _canonical,
    _gated,
    _grant_required_body,
    _mint_grant,
    _run,
    _serving,
    _Upstream,
)

# The literal the connector-host interface freezes. Spelled out here, not read
# from ``caller``, so every behavior below fails on its own behavior.
_VERIFIED = "X-Curie-Grant-Verified"
# Not the corpus key: a grant signed with it never verifies at the proxy.
_OTHER_SEED = base64.b64encode(bytes(range(1, 33))).decode("ascii")

# An execution tool absent from the proxy's gated list (ADR 0271 decision 7:
# the protected tool refuses a missing attestation even when ungated).
_PROTECTED = "execute_attested"
_ARGS: dict[str, Any] = {"note": "ship", "n": 1}


def _call(name: str, arguments: bytes, *, request_id: int = 1) -> bytes:
    return (
        b'{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"%s","arguments":%s}}'
        % (request_id, name.encode("ascii"), arguments)
    )


# Spelled unlike its canonical text, so a match is on canonical arguments.
_PROTECTED_CALL = _call(_PROTECTED, b'{ "note": "ship", "n": 1 }')
_OTHER_ARGS_CALL = _call(_PROTECTED, b'{"note":"other","n":1}')
_UNRELATED_CALL = _call("record_effect", b'{"note":"ship","n":1}', request_id=3)
_TOOLS_LIST = b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}'


def _batch(*items: bytes) -> bytes:
    return b"[" + b",".join(items) + b"]"


def _jti() -> str:
    return str(uuid.uuid4())


def _grant(
    tool: str,
    arguments: dict[str, Any],
    *,
    jti: str,
    agent: str = "acme-dev",
    connector: str = _CONNECTOR,
    exp: int = int(_VECTOR["exp"]),
    seed: str = _GRANT_SEED,
) -> str:
    """``_mint_grant`` with every claim open to a case."""

    from curie_worker.connector_grant import mint

    return mint(
        seed,
        agent=agent,
        connector=connector,
        tool=tool,
        args=_canonical(arguments),
        exp=exp,
        jti=jti,
    )


class _Ledger(MemoryGrantStore):
    """``MemoryGrantStore`` that keeps every spend it was asked for and every one it stored."""

    def __init__(self, *already: str) -> None:
        super().__init__()
        for jti in already:
            super().spend(jti, 1)
        self.asked: list[str] = []
        self.spent: list[str] = []

    def spend(self, jti: str, ttl: int) -> bool:
        self.asked.append(jti)
        stored = super().spend(jti, ttl)
        if stored:
            self.spent.append(jti)
        return stored


def _admitted(*grants: str, token: str = _TOKEN) -> list[tuple[str, str]]:
    pairs = [*_caller_headers().items(), ("Content-Type", "application/json")]
    if token != _TOKEN:
        pairs[0] = (caller.HEADER, token)
    return pairs + [(server.GRANT_HEADER, grant) for grant in grants]


def _forged(value: str) -> list[tuple[str, str]]:
    """Inbound attestations in three letter cases, repeated, one a copy of ``value``."""

    other = _jti()
    return [
        (_VERIFIED, value),
        (_VERIFIED.lower(), value),
        (_VERIFIED.upper(), other),
        (_VERIFIED, other),
    ]


def _verified(headers: Any) -> list[str]:
    """Every attestation value the connector received, in any letter case."""

    wanted = _VERIFIED.lower()
    return [value for name, value in headers.items() if name.lower() == wanted]


async def _send(
    proxy: TestServer,
    pairs: list[tuple[str, str]],
    body: bytes | None,
    *,
    method: str = "POST",
    path: str = "/mcp",
) -> tuple[int, bytes]:
    """Send ``pairs`` exactly as listed, letter case and repeats included.

    Session headers, not request headers: aiohttp's per-request merge lets a
    name in a new letter case replace the earlier copies, so three casings
    would never reach the proxy. Session headers go on the wire as given.
    """

    async with aiohttp.ClientSession(headers=CIMultiDict(pairs)) as client:
        async with client.request(method, proxy.make_url(path), data=body) as answer:
            return answer.status, await answer.read()


def _assert_grant_required(status: int, answer: bytes) -> None:
    assert status == _REFUSAL["status"]
    assert json.loads(answer) == _grant_required_body()


def _route_token() -> str:
    from curie_worker import caller_token

    return caller_token.mint_route(
        str(_VECTOR["seed"]), agent="acme-dev", connector=_CONNECTOR, exp=int(_VECTOR["exp"])
    )


def _decisions(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "curie_connector_proxy" and record.getMessage().startswith("caller=")
    ]


async def _attests_once(
    proxy: TestServer, upstream: Any, grant: str, body: bytes, jti: str
) -> None:
    """Liveness: ``grant`` on its own single call is forwarded with one attestation."""

    before = len(upstream.seen)
    status, _ = await _send(proxy, [*_admitted(grant), *_forged(jti)], body)
    assert status == 201
    assert len(upstream.seen) == before + 1
    seen = upstream.seen[-1]
    assert seen["body"] == body
    assert _verified(seen["headers"]) == [jti]


# @spec REQUEST-APPROVAL-7
def test_the_verified_grant_header_name_is_the_one_the_interface_freezes() -> None:
    assert caller.GRANT_VERIFIED_HEADER == _VERIFIED == "X-Curie-Grant-Verified"


# --------------------------------------------------------------------------- #
# Inbound copies are forged: dropped on every path, never a refusal.
# --------------------------------------------------------------------------- #


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("path", ["cct_post", "cct_get", "grant_less_tools_call", "declared_route"])
def test_every_inbound_verified_header_is_dropped_on_an_admitted_path(path: str) -> None:
    forged = _forged(_jti())

    async def go() -> None:
        # Not vacuous: sent straight to a connector, every forged copy arrives.
        direct = _Upstream()
        bare = TestServer(direct.app)
        await bare.start_server()
        try:
            assert (await _send(bare, forged, b"{}"))[0] == 201
        finally:
            await bare.close()
        assert sorted(_verified(direct.seen[0]["headers"])) == sorted(v for _n, v in forged)
        async with _serving(
            gated_tools=_GATED_TOOLS,
            connector=_CONNECTOR,
            grant_store=_Ledger(),
            routes=(_REVIEW_ROUTE,),
        ) as (upstream, proxy):
            if path == "cct_get":
                status, _ = await _send(proxy, [*_admitted(), *forged], None, method="GET")
            elif path == "declared_route":
                status, _ = await _send(
                    proxy,
                    [*_admitted(token=_route_token()), *forged],
                    b"{}",
                    path=_REVIEW_ROUTE.path,
                )
            else:
                body = b"{}" if path == "cct_post" else _PROTECTED_CALL
                status, _ = await _send(proxy, [*_admitted(), *forged], body)
            # Liveness: a forged attestation is dropped, not refused.
            assert status == 201
            [seen] = upstream.seen
            assert _verified(seen["headers"]) == []
            for _name, value in forged:
                assert value not in seen["headers"].values()

    _run(go)


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("refusal", ["no_caller_token", "gated_call_without_a_grant"])
def test_a_refused_request_carrying_forged_attestations_reaches_nothing(refusal: str) -> None:
    forged = _forged(_jti())
    pairs = [("Content-Type", "application/json"), *forged]
    if refusal == "gated_call_without_a_grant":
        pairs.insert(0, (caller.HEADER, _TOKEN))

    async def go() -> None:
        async with _gated(grant_store=_Ledger()) as (upstream, proxy):
            status, _ = await _send(proxy, pairs, _MERGE_CALL)
            assert status == server.REFUSAL_STATUS
            assert upstream.seen == []
            # Liveness: the same forgeries beside an admitted, ungated call
            # give way to nothing.
            status, _ = await _send(proxy, [*_admitted(), *forged], _UNRELATED_CALL)
            assert status == 201
            [seen] = upstream.seen
            assert _verified(seen["headers"]) == []

    _run(go)


# --------------------------------------------------------------------------- #
# A gated tool: strict grant, one spend, one attestation equal to the jti.
# --------------------------------------------------------------------------- #


# @spec REQUEST-APPROVAL-7
def test_a_spent_gated_grant_reaches_the_connector_as_one_verified_header_equal_to_its_jti() -> (
    None
):
    jti = _jti()
    grant = _mint_grant("merge_pull_request", {"n": 1}, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, _ = await _send(proxy, [*_admitted(grant), *_forged(jti)], _MERGE_CALL)
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == _MERGE_CALL
            assert _verified(seen["headers"]) == [jti]
            assert server.GRANT_HEADER not in seen["headers"]
            assert caller.HEADER not in seen["headers"]
            assert ledger.spent == [jti]

    _run(go)


_GATED_REFUSALS = {
    "missing": server.GRANT_HEADER_MISSING,
    "repeated": server.GRANT_HEADER_REPEATED,
    "bad_signature": server.GRANT_INVALID,
    "expired": server.GRANT_EXPIRED,
    "another_agent": server.GRANT_AGENT_MISMATCH,
    "another_connector": server.GRANT_CONNECTOR_MISMATCH,
    "another_tool": server.GRANT_TOOL_MISMATCH,
    "other_arguments": server.GRANT_ARGUMENTS_MISMATCH,
    "replayed": server.GRANT_NOT_SPENT,
    "no_store": server.GRANT_NO_STORE,
    "non_uuid_jti": server.GRANT_INVALID,
    "uppercase_uuid_jti": server.GRANT_INVALID,
}


def _presented(case: str, tool: str, arguments: dict[str, Any], jti: str) -> list[str]:
    """The grant headers one refusal case presents for a call to ``tool``."""

    if case == "missing":
        return []
    claims: dict[str, Any] = {}
    if case == "bad_signature":
        claims["seed"] = _OTHER_SEED
    elif case == "expired":
        claims["exp"] = _BEFORE_EXPIRY  # the proxy's clock: exp <= now
    elif case == "another_agent":
        claims["agent"] = "someone-else"
    elif case == "another_connector":
        claims["connector"] = "jira"
    elif case == "another_tool":
        tool = "other_tool"
    elif case == "other_arguments":
        arguments = {**arguments, "n": 2}
    elif case == "non_uuid_jti":
        jti = "jti-1"
    elif case == "uppercase_uuid_jti":
        jti = jti.upper()
    grant = _grant(tool, arguments, jti=jti, **claims)
    return [grant, grant] if case == "repeated" else [grant]


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("case", list(_GATED_REFUSALS))
def test_a_refused_gated_call_reaches_nothing_and_spends_nothing(
    case: str, caplog: pytest.LogCaptureFixture
) -> None:
    jti = _jti()
    grants = _presented(case, "merge_pull_request", {"n": 1}, jti)
    ledger = _Ledger(jti) if case == "replayed" else _Ledger()
    fresh_jti = _jti()
    fresh = _mint_grant("merge_pull_request", {"n": 1}, jti=fresh_jti)

    async def go() -> None:
        store = None if case == "no_store" else ledger
        async with _gated(grant_store=store) as (upstream, proxy):
            status, answer = await _send(proxy, [*_admitted(*grants), *_forged(jti)], _MERGE_CALL)
            _assert_grant_required(status, answer)
            assert upstream.seen == []
        # Only a replay asks the store, and nothing new is stored.
        assert ledger.asked == ([jti] if case == "replayed" else [])
        assert ledger.spent == []
        # Liveness: a fresh, valid grant for the same call is attested once.
        async with _gated(grant_store=_Ledger()) as (upstream, proxy):
            await _attests_once(proxy, upstream, fresh, _MERGE_CALL, fresh_jti)

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    refused = _decisions(caplog)[0]
    assert f"outcome={caller.GRANT_REQUIRED}" in refused.split()
    assert f"grant={_GATED_REFUSALS[case]}" in refused.split()


# --------------------------------------------------------------------------- #
# An ungated tool: a grant that matches this call is spent and attested.
# --------------------------------------------------------------------------- #


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("gated_tools", [(), _GATED_TOOLS], ids=["no_gated_list", "a_gated_list"])
def test_an_ungated_call_matching_its_grant_is_spent_and_attested_once(
    gated_tools: tuple[str, ...],
) -> None:
    jti = _jti()
    grant = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with _serving(gated_tools=gated_tools, connector=_CONNECTOR, grant_store=ledger) as (
            upstream,
            proxy,
        ):
            status, _ = await _send(proxy, [*_admitted(grant), *_forged(jti)], _PROTECTED_CALL)
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == _PROTECTED_CALL
            assert _verified(seen["headers"]) == [jti]
            assert server.GRANT_HEADER not in seen["headers"]
            assert caller.HEADER not in seen["headers"]
            assert ledger.spent == [jti]
            # The attempt it authorized is spent: the same call again refuses.
            status, answer = await _send(proxy, _admitted(grant), _PROTECTED_CALL)
            _assert_grant_required(status, answer)
            assert len(upstream.seen) == 1

    _run(go)


_UNGATED_MATCH_REFUSALS = {
    "expired": server.GRANT_EXPIRED,
    "already_spent": server.GRANT_NOT_SPENT,
    "another_agent": server.GRANT_AGENT_MISMATCH,
    "no_store": server.GRANT_NO_STORE,
    "non_uuid_jti": server.GRANT_INVALID,
    "uppercase_uuid_jti": server.GRANT_INVALID,
}


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("case", list(_UNGATED_MATCH_REFUSALS))
def test_a_matching_grant_that_cannot_be_spent_refuses_the_ungated_call(
    case: str, caplog: pytest.LogCaptureFixture
) -> None:
    jti = _jti()
    [grant] = _presented(case, _PROTECTED, _ARGS, jti)
    ledger = _Ledger(jti) if case == "already_spent" else _Ledger()
    fresh_jti = _jti()
    fresh = _mint_grant(_PROTECTED, _ARGS, jti=fresh_jti)

    async def go() -> None:
        store = None if case == "no_store" else ledger
        async with _gated(grant_store=store) as (upstream, proxy):
            status, answer = await _send(proxy, [*_admitted(grant), *_forged(jti)], _PROTECTED_CALL)
            _assert_grant_required(status, answer)
            assert upstream.seen == []
        assert ledger.asked == ([jti] if case == "already_spent" else [])
        assert ledger.spent == []
        # Liveness: a fresh grant for this agent, unexpired, spendable.
        async with _gated(grant_store=_Ledger()) as (upstream, proxy):
            await _attests_once(proxy, upstream, fresh, _PROTECTED_CALL, fresh_jti)

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    refused = _decisions(caplog)[0]
    assert f"grant={_UNGATED_MATCH_REFUSALS[case]}" in refused.split()


_NON_MATCHES = [
    "another_tool",
    "other_arguments",
    "another_connectors_grant",
    "bad_signature",
    "not_a_grant",
    "not_a_tools_call",
    "invalid_json",
]


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("case", _NON_MATCHES)
def test_a_grant_that_does_not_match_the_ungated_call_rides_along_unspent(case: str) -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    presented, body = {
        # The liveness case: the runner's static grant on an unrelated call.
        "another_tool": (static, _UNRELATED_CALL),
        "other_arguments": (static, _OTHER_ARGS_CALL),
        # Same tool name and arguments on another hosted connector.
        "another_connectors_grant": (
            _grant(_PROTECTED, _ARGS, jti=jti, connector="jira"),
            _PROTECTED_CALL,
        ),
        "bad_signature": (_grant(_PROTECTED, _ARGS, jti=jti, seed=_OTHER_SEED), _PROTECTED_CALL),
        "not_a_grant": ("ccg.not.signed", _PROTECTED_CALL),
        "not_a_tools_call": (static, _TOOLS_LIST),
        "invalid_json": (static, _PROTECTED_CALL[:-1]),
    }[case]
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, _ = await _send(proxy, [*_admitted(presented), *_forged(jti)], body)
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == body
            assert _verified(seen["headers"]) == []
            assert server.GRANT_HEADER not in seen["headers"]
            assert ledger.asked == []
            # Nothing was spent: the genuine grant (same jti) still attests
            # its own call exactly once.
            await _attests_once(proxy, upstream, static, _PROTECTED_CALL, jti)
            assert ledger.spent == [jti]

    _run(go)


# @spec REQUEST-APPROVAL-7
def test_the_static_grant_rides_unrelated_calls_before_and_after_its_one_spend() -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            assert (await _send(proxy, _admitted(static), _UNRELATED_CALL))[0] == 201
            await _attests_once(proxy, upstream, static, _PROTECTED_CALL, jti)
            # Warm reuse after the spend: unrelated calls still pass unattested.
            assert (await _send(proxy, _admitted(static), _UNRELATED_CALL))[0] == 201
            _assert_grant_required(*await _send(proxy, _admitted(static), _PROTECTED_CALL))
            assert (await _send(proxy, _admitted(static), _UNRELATED_CALL))[0] == 201
            assert [_verified(seen["headers"]) for seen in upstream.seen] == [[], [jti], [], []]
            assert [seen["body"] for seen in upstream.seen] == [
                _UNRELATED_CALL,
                _PROTECTED_CALL,
                _UNRELATED_CALL,
                _UNRELATED_CALL,
            ]
        assert ledger.spent == [jti]
        assert ledger.asked == [jti, jti]

    _run(go)


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("second", ["the_same_grant", "an_unrelated_grant"])
def test_a_matching_grant_beside_a_second_grant_header_is_refused_unspent(
    second: str, caplog: pytest.LogCaptureFixture
) -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    other = (
        static if second == "the_same_grant" else _mint_grant("record_effect", _ARGS, jti=_jti())
    )
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, answer = await _send(proxy, _admitted(static, other), _PROTECTED_CALL)
            _assert_grant_required(status, answer)
            assert upstream.seen == []
            assert ledger.asked == []
            # Liveness: the one grant alone is attested.
            await _attests_once(proxy, upstream, static, _PROTECTED_CALL, jti)

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    assert f"grant={server.GRANT_HEADER_REPEATED}" in _decisions(caplog)[0].split()


# @spec REQUEST-APPROVAL-7
def test_two_grant_headers_matching_nothing_forward_the_ungated_call_unattested() -> None:
    first = _mint_grant("record_effect", _ARGS, jti=_jti())
    second = _mint_grant(_PROTECTED, {"note": "other", "n": 1}, jti=_jti())
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, _ = await _send(proxy, _admitted(first, second), _PROTECTED_CALL)
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == _PROTECTED_CALL
            assert _verified(seen["headers"]) == []
            assert server.GRANT_HEADER not in seen["headers"]
            assert ledger.asked == []

    _run(go)


# --------------------------------------------------------------------------- #
# Batches are never attested.
# --------------------------------------------------------------------------- #

_REFUSED_BATCHES = {
    "a_gated_call_beside_another": (
        "merge_pull_request",
        {"n": 1},
        _MERGE_CALL,
        _batch(_TOOLS_LIST, _MERGE_CALL),
    ),
    "one_matching_ungated_call": (_PROTECTED, _ARGS, _PROTECTED_CALL, _batch(_PROTECTED_CALL)),
    "a_matching_ungated_call_beside_another": (
        _PROTECTED,
        _ARGS,
        _PROTECTED_CALL,
        _batch(_UNRELATED_CALL, _PROTECTED_CALL),
    ),
}


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("case", list(_REFUSED_BATCHES))
def test_a_batch_carrying_the_call_a_grant_names_is_refused_before_any_spend(
    case: str, caplog: pytest.LogCaptureFixture
) -> None:
    tool, arguments, single, batch = _REFUSED_BATCHES[case]
    jti = _jti()
    grant = _mint_grant(tool, arguments, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, answer = await _send(proxy, [*_admitted(grant), *_forged(jti)], batch)
            _assert_grant_required(status, answer)
            assert upstream.seen == []
            assert ledger.asked == []
            # Liveness: the same grant on the one call it names, alone.
            await _attests_once(proxy, upstream, grant, single, jti)
            assert ledger.spent == [jti]

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    assert "grant=batched_call" in _decisions(caplog)[0].split()


# @spec REQUEST-APPROVAL-7
def test_a_batch_of_unrelated_calls_with_the_static_grant_forwards_unattested_and_unspent() -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    batch = _batch(_TOOLS_LIST, _UNRELATED_CALL, _OTHER_ARGS_CALL)
    ledger = _Ledger()

    async def go() -> None:
        async with _gated(grant_store=ledger) as (upstream, proxy):
            status, _ = await _send(proxy, [*_admitted(static), *_forged(jti)], batch)
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == batch
            assert _verified(seen["headers"]) == []
            assert server.GRANT_HEADER not in seen["headers"]
            assert ledger.asked == []
            await _attests_once(proxy, upstream, static, _PROTECTED_CALL, jti)

    _run(go)


# --------------------------------------------------------------------------- #
# A route and a non-POST carry no tool call, so never an attestation.
# --------------------------------------------------------------------------- #


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("path", ["declared_route", "get"])
def test_a_route_or_a_get_beside_a_matching_grant_is_neither_attested_nor_spent(path: str) -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with _serving(
            gated_tools=_GATED_TOOLS,
            connector=_CONNECTOR,
            grant_store=ledger,
            routes=(_REVIEW_ROUTE,),
        ) as (upstream, proxy):
            if path == "declared_route":
                pairs = [*_admitted(static, token=_route_token()), *_forged(jti)]
                status, _ = await _send(proxy, pairs, _PROTECTED_CALL, path=_REVIEW_ROUTE.path)
            else:
                pairs = [*_admitted(static), *_forged(jti)]
                status, _ = await _send(proxy, pairs, None, method="GET")
            assert status == 201
            [seen] = upstream.seen
            assert _verified(seen["headers"]) == []
            assert server.GRANT_HEADER not in seen["headers"]
            assert ledger.asked == []
            # Liveness: the same grant on /mcp, as a cct tools/call, attests.
            await _attests_once(proxy, upstream, static, _PROTECTED_CALL, jti)

    _run(go)


# --------------------------------------------------------------------------- #
# An uncertain upstream outcome never makes a spent grant replayable.
# --------------------------------------------------------------------------- #


class _HangUp:
    """A connector server that reads each request whole, then closes without a byte."""

    def __init__(self) -> None:
        self.requests: list[bytes] = []
        self.port = 0

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            length = 0
            for line in head.decode("latin-1").split("\r\n")[1:]:
                name, _, value = line.partition(":")
                if name.strip().lower() == "content-length":
                    length = int(value.strip())
            self.requests.append(await reader.readexactly(length))
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()


@asynccontextmanager
async def _hanging_up() -> AsyncIterator[_HangUp]:
    hang = _HangUp()
    listener = await asyncio.start_server(hang.serve, "127.0.0.1", 0)
    hang.port = listener.sockets[0].getsockname()[1]
    try:
        yield hang
    finally:
        listener.close()
        await listener.wait_closed()


# @spec REQUEST-APPROVAL-7
@pytest.mark.parametrize("tool", ["gated", "ungated"])
def test_an_upstream_that_never_answers_leaves_the_grant_spent_and_unreplayable(
    tool: str,
) -> None:
    name, arguments, body = (
        ("merge_pull_request", {"n": 1}, _MERGE_CALL)
        if tool == "gated"
        else (_PROTECTED, _ARGS, _PROTECTED_CALL)
    )
    jti = _jti()
    grant = _mint_grant(name, arguments, jti=jti)
    ledger = _Ledger()

    async def go() -> None:
        async with (
            _hanging_up() as hang,
            _gated(upstream_port=hang.port, grant_store=ledger) as (_unused, proxy),
        ):
            status, _ = await _send(proxy, _admitted(grant), body)
            assert status == 502
            assert hang.requests == [body]
            assert ledger.spent == [jti]
            # The identical request again: spent, refused, never resent.
            _assert_grant_required(*await _send(proxy, _admitted(grant), body))
            assert hang.requests == [body]
            assert ledger.spent == [jti]

    _run(go)


# --------------------------------------------------------------------------- #
# The decision log says an attestation was set, never what it was.
# --------------------------------------------------------------------------- #


# @spec REQUEST-APPROVAL-7
def test_the_decision_log_says_attested_present_only_on_an_attested_admit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    jti = _jti()
    static = _mint_grant(_PROTECTED, _ARGS, jti=jti)
    merge_jti = _jti()
    merge = _mint_grant("merge_pull_request", {"n": 1}, jti=merge_jti)

    async def go() -> None:
        async with _gated(grant_store=_Ledger()) as (upstream, proxy):
            for pairs, body, expected in [
                (_admitted(static), _UNRELATED_CALL, 201),
                (_admitted(static), _PROTECTED_CALL, 201),
                (_admitted(static), _PROTECTED_CALL, _REFUSAL["status"]),
                (_admitted(merge), _MERGE_CALL, 201),
                (_admitted(), _UNRELATED_CALL, 201),
            ]:
                assert (await _send(proxy, pairs, body))[0] == expected
            # Not vacuous: the connector did receive what the log must not carry.
            assert [_verified(seen["headers"]) for seen in upstream.seen] == [
                [],
                [jti],
                [merge_jti],
                [],
            ]

    with caplog.at_level(logging.DEBUG):
        _run(go)
    lines = _decisions(caplog)
    assert len(lines) == 5
    for index, line in enumerate(lines):
        words = line.split()
        if index in (1, 3):
            assert "outcome=admitted" in words
            assert words.count("attested=present") == 1
        else:
            assert "attested=" not in line
    for text in (jti, merge_jti):
        assert text not in caplog.text
        assert text.split("-")[-1] not in caplog.text
