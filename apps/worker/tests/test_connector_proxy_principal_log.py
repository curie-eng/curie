"""The caller proxy's decision log says whether an admitted cct named a requester.

ADR 0270 decision 2. The per-request decision line (``caller=... outcome=...``)
gains ``principal=present`` for an admitted ``cct`` that carried a verified
principal and ``principal=absent`` for an admitted agent-only ``cct``. A
refused request and a ``ccr`` route request carry no principal field at all,
and no principal value (issuer, subject, tenant id) is ever logged, by any
logger, on any path.

Driven over real sockets like ``test_connector_proxy.py``: a real aiohttp
upstream stands in for the connector server, tokens come from the shared
vectors in ``tests/vectors/connector-caller-token.json`` and
``connector-caller-refusal.json``, and fresh ones are minted by the worker's own
``caller_token`` under the vectors' seed, so mint and verify are the two real
halves.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, server
from curie_worker import caller_token

_ROOT = Path(__file__).resolve().parents[3]
_TOKENS = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-token.json").read_text(encoding="utf-8")
)
_REFUSAL = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-refusal.json").read_text(encoding="utf-8")
)
_VECTOR = next(v for v in _TOKENS["vectors"] if v["name"] == "a_plain_agent_name")
_SEED = str(_VECTOR["seed"])
_PUBLIC = str(_VECTOR["public"])
_EXP = int(_VECTOR["exp"])
_NOW = _EXP - 10
_AGENT = "acme-dev"
_CONNECTOR = "reviewer"
_REVIEW = server.RouteConfig(
    path="/v1/review", methods=frozenset({"POST"}), max_body_bytes=1024 * 1024
)
_LOGGER = "curie_connector_proxy"

# A requester no vector uses, so a value found in a log can only have come from
# the token this test presented.
_MINTED_PRINCIPAL = caller_token.CallerPrincipal(
    tenant_id="55555555-5555-4555-8555-555555555555",
    issuer="https://issuer.principal-log.example.test",
    subject="principal-log-subject-0001",
)


def _signed_here(vector: dict[str, Any]) -> bool:
    return vector["public"] == _PUBLIC and int(vector["exp"]) == _EXP


_PRINCIPAL_VECTORS = [
    v for v in _TOKENS["vectors"] if "principal" in v and v["agent"] == _AGENT and _signed_here(v)
]
_AGENT_ONLY_VECTORS = [
    v
    for v in _TOKENS["vectors"]
    if "principal" not in v and v["agent"] == _AGENT and _signed_here(v)
]


def _payload(token: str) -> list[tuple[str, object]]:
    """Every key/value pair of a token's signed payload, duplicates included."""

    segment = token.split(".")[1]
    raw = base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    pairs: list[tuple[str, object]] = []

    def keep(items: list[tuple[str, object]]) -> dict[str, object]:
        pairs.extend(items)
        return dict(items)

    json.loads(raw, object_pairs_hook=keep)
    return pairs


def _strings(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def _principal_values(token: str) -> set[str]:
    """The text a token's principal carries, however malformed, worth searching a log for."""

    # The hook sees every object at every depth, so a duplicated key inside the
    # principal contributes both of its values.
    found: set[str] = set()
    for key, value in _payload(token):
        if key in {"principal", "issuer", "subject", "tenant_id", "role"}:
            found.update(text for text in _strings(value) if len(text) >= 4)
    return found


_CANONICAL_VALUES = {
    text for vector in _PRINCIPAL_VECTORS for text in _strings(vector["principal"])
} | {_MINTED_PRINCIPAL.tenant_id, _MINTED_PRINCIPAL.issuer, _MINTED_PRINCIPAL.subject}


class _Upstream:
    """A connector server that records every request it is sent."""

    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        self.app = web.Application()
        self.app.router.add_route("*", "/{tail:.*}", self._echo)

    async def _echo(self, request: web.Request) -> web.Response:
        self.seen.append(
            {
                "method": request.method,
                "path": request.raw_path,
                "headers": request.headers.copy(),
                "body": await request.read(),
            }
        )
        return web.Response(status=201, body=b'{"ok":true}', content_type="application/json")


@asynccontextmanager
async def _serving(
    *,
    admits: frozenset[str] = frozenset({_AGENT}),
    routes: tuple[server.RouteConfig, ...] = (),
) -> AsyncIterator[tuple[_Upstream, TestServer]]:
    upstream = _Upstream()
    upstream_server = TestServer(upstream.app, handler_cancellation=True)
    await upstream_server.start_server(auto_decompress=False)
    extra: dict[str, Any] = {}
    if routes:
        extra["connector"] = _CONNECTOR
        extra["routes"] = routes
    config = server.ProxyConfig(
        listen_port=8480,
        upstream_port=upstream_server.port or 0,
        public_keys=(caller.public_key(_PUBLIC),),
        admits=admits,
        **extra,
    )
    proxy = TestServer(server.make_app(config, clock=lambda: _NOW), handler_cancellation=True)
    await proxy.start_server()
    try:
        yield upstream, proxy
    finally:
        await proxy.close()
        await upstream_server.close()


def _run(body: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(body())


async def _send(
    proxy: TestServer, headers: list[tuple[str, str]], *, path: str = "/mcp"
) -> int:
    async with aiohttp.ClientSession() as client:
        async with client.post(proxy.make_url(path), data=b"{}", headers=headers) as answer:
            await answer.read()
            return answer.status


def _decision_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == _LOGGER and record.getMessage().startswith("caller=")
    ]


def _principal_fields(line: str) -> list[str]:
    return [field for field in line.split() if field.startswith("principal=")]


def _assert_no_principal_value_logged(
    caplog: pytest.LogCaptureFixture, tokens: list[str]
) -> None:
    """No principal value, and no piece of the bearer, in any record of any logger."""

    forbidden = set(_CANONICAL_VALUES)
    for token in tokens:
        forbidden |= _principal_values(token)
    messages = [record.getMessage() for record in caplog.records]
    texts = [*messages, caplog.text]
    for value in sorted(forbidden):
        for text in texts:
            assert value not in text, "a principal value reached a log record"
    for token in tokens:
        _prefix, payload, signature = token.split(".")
        for text in texts:
            assert payload not in text, "a caller token payload reached a log record"
            assert signature not in text, "a caller token signature reached a log record"


def _principal_minted() -> str:
    return caller_token.mint(_SEED, agent=_AGENT, exp=_EXP, principal=_MINTED_PRINCIPAL)


def _agent_only_minted() -> str:
    return caller_token.mint(_SEED, agent=_AGENT, exp=_EXP)


# --------------------------------------------------------------------------- #
# Admitted cct: present or absent, exactly once
# --------------------------------------------------------------------------- #


def test_the_vectors_this_file_reads_are_still_there() -> None:
    # Guards against a vector rename turning the parametrized cases below into
    # an empty, vacuously green set.
    assert {v["name"] for v in _PRINCIPAL_VECTORS} >= {
        "a_human_requester",
        "the_same_requester_on_a_work_item_run",
    }
    assert {v["name"] for v in _AGENT_ONLY_VECTORS} >= {"a_plain_agent_name", "a_work_item_run"}
    assert _REFUSAL["principal_refusals"]
    assert _TOKENS["tampered_principals"]
    assert _TOKENS["malformed_principals"]


def _admitted_line(caplog: pytest.LogCaptureFixture, token: str) -> str:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status = await _send(proxy, [(caller.HEADER, token)])
            assert status == 201
            assert len(upstream.seen) == 1

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"caller={_AGENT}" in line.split()
    assert "outcome=admitted" in line.split()
    return line


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("vector", _PRINCIPAL_VECTORS, ids=lambda v: v["name"])
def test_an_admitted_principal_bearing_cct_logs_principal_present(
    vector: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    token = str(vector["minted"])
    line = _admitted_line(caplog, token)
    assert _principal_fields(line) == ["principal=present"]
    _assert_no_principal_value_logged(caplog, [token])


# @spec CONNECTOR-REQUESTER-2
def test_a_freshly_minted_requester_logs_principal_present_and_none_of_its_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = _principal_minted()
    line = _admitted_line(caplog, token)
    assert _principal_fields(line) == ["principal=present"]
    for value in (
        _MINTED_PRINCIPAL.tenant_id,
        _MINTED_PRINCIPAL.issuer,
        _MINTED_PRINCIPAL.subject,
    ):
        assert value not in caplog.text
        assert all(value not in record.getMessage() for record in caplog.records)
    _assert_no_principal_value_logged(caplog, [token])


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("vector", _AGENT_ONLY_VECTORS, ids=lambda v: v["name"])
def test_an_admitted_agent_only_cct_logs_principal_absent(
    vector: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    token = str(vector["minted"])
    line = _admitted_line(caplog, token)
    assert _principal_fields(line) == ["principal=absent"]
    _assert_no_principal_value_logged(caplog, [token])


# @spec CONNECTOR-REQUESTER-2
def test_a_freshly_minted_agent_only_cct_logs_principal_absent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = _agent_only_minted()
    line = _admitted_line(caplog, token)
    assert _principal_fields(line) == ["principal=absent"]


# @spec CONNECTOR-REQUESTER-2
def test_each_admitted_request_gets_its_own_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # One proxy, two callers in turn: the field follows each request's own
    # token, never a previous request's.
    present = _principal_minted()
    absent = _agent_only_minted()

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            assert await _send(proxy, [(caller.HEADER, present)]) == 201
            assert await _send(proxy, [(caller.HEADER, absent)]) == 201
            assert await _send(proxy, [(caller.HEADER, present)]) == 201
            assert len(upstream.seen) == 3

    with caplog.at_level(logging.DEBUG):
        _run(go)
    lines = _decision_lines(caplog)
    assert [_principal_fields(line) for line in lines] == [
        ["principal=present"],
        ["principal=absent"],
        ["principal=present"],
    ]
    _assert_no_principal_value_logged(caplog, [present, absent])


# --------------------------------------------------------------------------- #
# Refusals: no principal field, whatever the token carried
# --------------------------------------------------------------------------- #


def _refusal_cases() -> list[Any]:
    cases: list[Any] = []
    for entry in _REFUSAL["principal_refusals"]:
        cases.append(
            pytest.param(
                [(caller.HEADER, str(entry["token"]))],
                str(entry["refusal"]),
                id=f"principal_refusal-{entry['name']}",
            )
        )
    for entry in _TOKENS["tampered_principals"]:
        cases.append(
            pytest.param(
                [(caller.HEADER, str(entry["minted"]))],
                caller.INVALID,
                id=f"tampered-{entry['name']}",
            )
        )
    for entry in _TOKENS["malformed_principals"]:
        cases.append(
            pytest.param(
                [(caller.HEADER, str(entry["minted"]))],
                caller.INVALID,
                id=f"malformed-{entry['name']}",
            )
        )
    cases.append(pytest.param([], caller.MISSING, id="no_caller_token"))
    return cases


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize(("headers", "refusal"), _refusal_cases())
def test_a_refused_cct_logs_no_principal_field(
    headers: list[tuple[str, str]], refusal: str, caplog: pytest.LogCaptureFixture
) -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            assert await _send(proxy, headers) == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"outcome={refusal}" in line.split()
    assert _principal_fields(line) == []
    assert "principal" not in line
    _assert_no_principal_value_logged(caplog, [value for _name, value in headers])


# @spec CONNECTOR-REQUESTER-2
def test_an_expired_agent_only_cct_logs_no_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = caller_token.mint(_SEED, agent=_AGENT, exp=_NOW - 100)

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            assert await _send(proxy, [(caller.HEADER, token)]) == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"outcome={caller.EXPIRED}" in line.split()
    assert "principal" not in line


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("principal", [True, False], ids=["with_requester", "agent_only"])
def test_a_cct_the_connector_does_not_admit_logs_no_principal_field(
    principal: bool, caplog: pytest.LogCaptureFixture
) -> None:
    token = _principal_minted() if principal else _agent_only_minted()

    async def go() -> None:
        async with _serving(admits=frozenset({"someone-else"})) as (upstream, proxy):
            assert await _send(proxy, [(caller.HEADER, token)]) == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"caller={_AGENT}" in line.split()
    assert f"outcome={caller.NOT_ADMITTED}" in line.split()
    assert "principal" not in line
    _assert_no_principal_value_logged(caplog, [token])


# @spec CONNECTOR-REQUESTER-2
def test_two_principal_bearing_caller_headers_log_no_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = _principal_minted()

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status = await _send(proxy, [(caller.HEADER, token), (caller.HEADER, token)])
            assert status == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"outcome={caller.INVALID}" in line.split()
    assert "principal" not in line
    _assert_no_principal_value_logged(caplog, [token])


# --------------------------------------------------------------------------- #
# Routes: a ccr never carries a requester, so its line names none
# --------------------------------------------------------------------------- #


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("run", [False, True], ids=["agent_only", "work_item_run"])
def test_an_admitted_route_token_logs_no_principal_field(
    run: bool, caplog: pytest.LogCaptureFixture
) -> None:
    claims: dict[str, str] = {}
    if run:
        claims = {
            "run": "11111111-2222-4333-8444-555555555555",
            "work_item": "66666666-7777-4888-8999-aaaaaaaaaaaa",
        }
    token = caller_token.mint_route(
        _SEED, agent=_AGENT, connector=_CONNECTOR, exp=_EXP, **claims
    )

    async def go() -> None:
        async with _serving(routes=(_REVIEW,)) as (upstream, proxy):
            status = await _send(proxy, [(caller.HEADER, token)], path=_REVIEW.path)
            assert status == 201
            assert [seen["path"] for seen in upstream.seen] == [_REVIEW.path]

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"caller={_AGENT}" in line.split()
    assert "outcome=admitted" in line.split()
    assert f"route={_REVIEW.path!r}" in line.split()
    assert _principal_fields(line) == []
    assert "principal" not in line


# @spec CONNECTOR-REQUESTER-2
def test_a_refused_route_token_logs_no_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = caller_token.mint_route(
        _SEED, agent=_AGENT, connector=_CONNECTOR, exp=_NOW - 100
    )

    async def go() -> None:
        async with _serving(routes=(_REVIEW,)) as (upstream, proxy):
            status = await _send(proxy, [(caller.HEADER, token)], path=_REVIEW.path)
            assert status == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"outcome={caller.EXPIRED}" in line.split()
    assert "principal" not in line


# @spec CONNECTOR-REQUESTER-2
def test_a_principal_bearing_cct_on_a_route_logs_no_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A route admits only a ccr: the requester a cct carries cannot ride onto
    # a route, and the refusal names no requester either.
    token = _principal_minted()

    async def go() -> None:
        async with _serving(routes=(_REVIEW,)) as (upstream, proxy):
            status = await _send(proxy, [(caller.HEADER, token)], path=_REVIEW.path)
            assert status == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    [line] = _decision_lines(caplog)
    assert f"outcome={caller.INVALID}" in line.split()
    assert "principal" not in line
    _assert_no_principal_value_logged(caplog, [token])


# @spec CONNECTOR-REQUESTER-2
def test_a_cct_beside_a_declared_route_still_logs_its_principal_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Declaring routes changes nothing for the cct paths: /mcp on a connector
    # with routes still logs present or absent.
    present = _principal_minted()
    absent = _agent_only_minted()

    async def go() -> None:
        async with _serving(routes=(_REVIEW,)) as (upstream, proxy):
            assert await _send(proxy, [(caller.HEADER, present)]) == 201
            assert await _send(proxy, [(caller.HEADER, absent)]) == 201
            assert len(upstream.seen) == 2

    with caplog.at_level(logging.DEBUG):
        _run(go)
    lines = _decision_lines(caplog)
    assert [_principal_fields(line) for line in lines] == [
        ["principal=present"],
        ["principal=absent"],
    ]
    for line in lines:
        assert not any(field.startswith("route=") for field in line.split())
    _assert_no_principal_value_logged(caplog, [present, absent])
