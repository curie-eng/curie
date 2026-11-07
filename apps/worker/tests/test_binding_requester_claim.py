"""The binding mints the verified requester into the cct and reads the opt-in strictly.

ADR 0270 decisions 1 and 2. ``boot_env`` signs ``principal`` into the
connector caller token (``cct``) only; every ``ccr`` route token stays an
agent/run credential. ``connector_requester_identity_for`` reads the operator
flag from the REAL compose Postgres (never mocked) and fails closed on any
stored state that is not a boolean. A read that fails (the SELECT raising) is
the retryable ``TurnAuthorityUnavailable``, never that terminal refusal, so a
transient database fault cannot drop an accepted turn.

@spec CONNECTOR-REQUESTER-1 CONNECTOR-REQUESTER-2.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker import binding as binding_module
from curie_worker import caller_token
from curie_worker.binding import SANDBOX_TOKEN_TTL_SECONDS, BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.identity_authority import TurnAuthorityUnavailable
from curie_worker.protected_binding import (
    ProtectedBindingResolver,
    ProtectedMediatedBindingResolver,
)
from nacl.signing import SigningKey
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")
_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_THREAD = "thread-1"
_SCOPED = {"connector_release": "curie", "connector_namespace": "curie"}
_CALLER_ENV = "CURIE_CONNECTOR_CALLER_TOKEN"
_ROUTES_ENV = "CURIE_CONNECTOR_ROUTE_TOKENS"
TENANT = "33333333-3333-4333-8333-333333333333"
ISSUER = "https://identity.example.test/directory"
SUBJECT = "SyntheticRequesterSubject"
RUN = "11111111-1111-4111-8111-111111111111"
WORK_ITEM = "22222222-2222-4222-8222-222222222222"
OTHER_TENANT = "44444444-4444-4444-8444-444444444444"
# ADR 0270 decision 3: a requester-bearing cct lives at most one hour from mint.
REQUESTER_TTL = 3600
# The digest domain the route record's requester fact is computed under.
DIGEST_DOMAIN = b"curie-cct-principal-v1\n"
# A frozen wall clock, far from the real one so no assertion passes by accident.
T = 1_900_000_000


def _resolved() -> ResolvedDeployment:
    """@spec CONNECTOR-REQUESTER-2."""
    return ResolvedDeployment(
        agent_name="test-agent",
        agent_id=_AGENT,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def _seed() -> str:
    """@spec CONNECTOR-REQUESTER-2: a fresh worker Ed25519 seed."""
    return base64.b64encode(bytes(SigningKey.generate())).decode()


def _principal(**changes: str) -> Any:
    """@spec CONNECTOR-REQUESTER-2: the worker-side canonical principal."""
    cls = getattr(caller_token, "CallerPrincipal", None)
    assert cls is not None, "caller_token has no CallerPrincipal"
    return cls(**{"tenant_id": TENANT, "issuer": ISSUER, "subject": SUBJECT, **changes})


def _boot(seed: str | None, **kwargs: Any) -> dict[str, str]:
    """A real resolver's boot_env; it makes no engine call. @spec CONNECTOR-REQUESTER-2."""
    resolver = BindingResolver.__new__(BindingResolver)
    config = WorkerConfig(**_SCOPED) if seed is None else WorkerConfig(
        connector_caller_signing_key=seed, **_SCOPED,
    )
    resolver._config = config  # type: ignore[attr-defined]
    return resolver.boot_env(_resolved(), _THREAD, **kwargs)


def _b64url_decode(segment: str) -> bytes:
    """@spec CONNECTOR-REQUESTER-2."""
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _verified_claims(seed: str, token: str, prefix: str) -> dict[str, Any]:
    """Signature-checked claims of one caller token. @spec CONNECTOR-REQUESTER-2."""
    head, payload, signature = token.split(".")
    assert head == prefix
    SigningKey(base64.b64decode(seed)).verify_key.verify(
        f"{head}.{payload}".encode("ascii"), _b64url_decode(signature),
    )
    claims: dict[str, Any] = json.loads(_b64url_decode(payload))
    return claims


# -- boot_env -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "subject", [SUBJECT, "Zoë.Requester@Example.Test"], ids=["ascii", "non-ascii"],
)
def test_an_opted_in_boot_signs_the_principal_into_the_cct(subject: str) -> None:
    """@spec CONNECTOR-REQUESTER-2: complete principal, same key, same agent and expiry."""
    seed = _seed()
    principal = _principal(subject=subject)
    before = int(time.time())
    env = _boot(seed, caller_principal=principal)
    after = int(time.time())
    token = env[_CALLER_ENV]
    claims = _verified_claims(seed, token, "cct")
    exp = claims["exp"]
    assert isinstance(exp, int)
    # ADR 0270 decision 3: a requester-bearing cct lives one hour from mint.
    assert before + REQUESTER_TTL <= exp <= after + REQUESTER_TTL
    assert claims == {
        "agent": "test-agent", "exp": exp,
        "principal": {"issuer": ISSUER, "subject": subject, "tenant_id": TENANT},
    }
    # The boot mints exactly what the shared minter mints for these claims.
    assert token == caller_token.mint(seed, agent="test-agent", exp=exp, principal=principal)


@pytest.mark.parametrize("explicit", [False, True], ids=["omitted", "explicit-none"])
def test_a_boot_without_a_principal_keeps_todays_agent_only_cct(explicit: bool) -> None:
    """@spec CONNECTOR-REQUESTER-2: a principal-less token keeps today's closed claim set."""
    seed = _seed()
    env = _boot(seed, **({"caller_principal": None} if explicit else {}))
    token = env[_CALLER_ENV]
    claims = _verified_claims(seed, token, "cct")
    assert set(claims) == {"agent", "exp"} and claims["agent"] == "test-agent"
    assert token == caller_token.mint(seed, agent="test-agent", exp=claims["exp"])


def test_a_work_item_boot_carries_the_run_pair_and_the_principal() -> None:
    """@spec CONNECTOR-REQUESTER-2: run/work_item keep their meaning beside the principal."""
    seed = _seed()
    ceiling = int(time.time()) + 90
    env = _boot(
        seed, caller_run=RUN, caller_work_item=WORK_ITEM, caller_exp_ceiling=ceiling,
        caller_principal=_principal(),
    )
    claims = _verified_claims(seed, env[_CALLER_ENV], "cct")
    assert claims == {
        "agent": "test-agent", "exp": ceiling, "run": RUN, "work_item": WORK_ITEM,
        "principal": {"issuer": ISSUER, "subject": SUBJECT, "tenant_id": TENANT},
    }


@pytest.mark.parametrize("run_pair", [False, True], ids=["session", "work-item"])
def test_every_ccr_route_token_stays_principal_less(run_pair: bool) -> None:
    """@spec CONNECTOR-REQUESTER-2: route tokens carry no human authority."""
    seed = _seed()
    pair: dict[str, Any] = (
        {"caller_run": RUN, "caller_work_item": WORK_ITEM} if run_pair else {}
    )
    env = _boot(
        seed, route_connectors=frozenset({"reviewer", "uploads"}),
        caller_principal=_principal(), **pair,
    )
    caller = _verified_claims(seed, env[_CALLER_ENV], "cct")
    assert caller["principal"] == {"issuer": ISSUER, "subject": SUBJECT, "tenant_id": TENANT}
    tokens = json.loads(env[_ROUTES_ENV])
    assert sorted(tokens) == ["reviewer", "uploads"]
    for connector, token in tokens.items():
        claims = _verified_claims(seed, token, "ccr")
        assert "principal" not in claims
        expected = {"agent": "test-agent", "exp": claims["exp"], "connector": connector}
        if run_pair:
            expected.update(run=RUN, work_item=WORK_ITEM)
        assert claims == expected


def test_a_principal_never_mints_a_token_where_today_none_is_minted() -> None:
    """@spec CONNECTOR-REQUESTER-2: no key or no connector scope, no caller token at all."""
    keyless = _boot(None, caller_principal=_principal())
    assert _CALLER_ENV not in keyless
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(connector_caller_signing_key=_seed())  # type: ignore[attr-defined]
    unscoped = resolver.boot_env(_resolved(), _THREAD, caller_principal=_principal())
    assert _CALLER_ENV not in unscoped
    assert not any(SUBJECT in value for value in {**keyless, **unscoped}.values())


@pytest.mark.parametrize(
    "cls", [BindingResolver, ProtectedBindingResolver, ProtectedMediatedBindingResolver],
)
def test_every_kernel_binding_accepts_the_principal_and_reads_the_flag(cls: type) -> None:
    """@spec CONNECTOR-REQUESTER-1: the kernel's boot call never raises TypeError on a binding."""
    parameter = inspect.signature(cls.boot_env).parameters.get("caller_principal")
    assert parameter is not None, f"{cls.__name__}.boot_env has no caller_principal"
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is None
    reader = getattr(cls, "connector_requester_identity_for", None)
    assert callable(reader) and inspect.iscoroutinefunction(reader), (
        f"{cls.__name__} has no async connector_requester_identity_for"
    )


# -- ADR 0270 decision 3: the requester fact and the one-hour requester cct -------------


def _principal_digest(value: Any) -> str:
    """The route record's requester fact, computed independently of the worker.

    ``sha256(domain + compact sorted-key ASCII JSON of the principal value)``.
    @spec CONNECTOR-REQUESTER-3.
    """
    canonical = json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
    return "sha256:" + hashlib.sha256(DIGEST_DOMAIN + canonical.encode("ascii")).hexdigest()


def _b64url(raw: bytes) -> str:
    """@spec CONNECTOR-REQUESTER-3."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _signed_cct(seed: str, body: dict[str, Any], *, sort_keys: bool = True) -> str:
    """A cct over any payload, signed with the test key: shapes the minter refuses.

    @spec CONNECTOR-REQUESTER-3.
    """
    payload = json.dumps(body, separators=(",", ":"), sort_keys=sort_keys).encode("ascii")
    head = f"cct.{_b64url(payload)}"
    signature = SigningKey(base64.b64decode(seed)).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


def _unsigned_facts(token: str) -> tuple[str, int | None]:
    """``caller_token.unsigned_facts``, resolved at call time. @spec CONNECTOR-REQUESTER-3."""
    reader = getattr(caller_token, "unsigned_facts", None)
    assert callable(reader), "caller_token has no unsigned_facts"
    facts: tuple[str, int | None] = reader(token)
    return facts


_ALICE_FIELDS = {"issuer": ISSUER, "subject": SUBJECT, "tenant_id": TENANT}
# Every principal value the proxy would refuse. Any principal key, whatever its
# shape, is a requester fact (a digest), never principal-less.
_MALFORMED_PRINCIPALS: dict[str, Any] = {
    "partial": {"issuer": ISSUER, "subject": SUBJECT},
    "unknown-key": {**_ALICE_FIELDS, "role": "admin"},
    # TENANT has no hex letters, so upper() would leave it valid; this one has some.
    "uppercase-tenant": {**_ALICE_FIELDS, "tenant_id": "abcdef01-2345-4678-89ab-cdef01234567".upper()},
    "empty-subject": {**_ALICE_FIELDS, "subject": ""},
    "typed-subject": {**_ALICE_FIELDS, "subject": 7},
    "string": "SyntheticRequesterSubject",
    "null": None,
    "list": [],
}


def test_unsigned_facts_digest_tracks_each_principal_field() -> None:
    """@spec CONNECTOR-REQUESTER-3: equality of the digest is equality of the principal."""
    seed = _seed()
    first = caller_token.mint(seed, agent="test-agent", exp=T + 3600, principal=_principal())
    digest, exp = _unsigned_facts(first)
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", digest), digest
    assert (digest, exp) == (_principal_digest(_ALICE_FIELDS), T + 3600)
    # The same principal twice is equal, whatever else the token carries: a
    # claim-time re-mint and a deadline re-sign keep the recorded fact.
    again = caller_token.mint(
        seed,
        agent="test-agent",
        exp=T + 1200,
        run=RUN,
        work_item=WORK_ITEM,
        principal=_principal(),
    )
    assert _unsigned_facts(again) == (digest, T + 1200)
    # The digest is over the canonical re-serialization, not the signed bytes.
    reordered = _signed_cct(
        seed,
        {
            "principal": {"tenant_id": TENANT, "subject": SUBJECT, "issuer": ISSUER},
            "exp": T + 3600,
            "agent": "test-agent",
        },
        sort_keys=False,
    )
    assert _unsigned_facts(reordered) == (digest, T + 3600)
    # Each field alone changes it.
    for field, value in (
        ("tenant_id", OTHER_TENANT),
        ("issuer", f"{ISSUER}/other"),
        ("subject", f"{SUBJECT}2"),
    ):
        changed = caller_token.mint(
            seed,
            agent="test-agent",
            exp=T + 3600,
            principal=_principal(**{field: value}),
        )
        other, other_exp = _unsigned_facts(changed)
        assert other_exp == T + 3600
        assert other == _principal_digest({**_ALICE_FIELDS, field: value})
        assert other != digest, field
    # ASCII JSON: a non-ASCII subject is hashed escaped, never as raw UTF-8.
    zoe = "Zoë.Requester@Example.Test"
    escaped = _unsigned_facts(
        caller_token.mint(seed, agent="test-agent", exp=T, principal=_principal(subject=zoe))
    )[0]
    assert escaped == _principal_digest({**_ALICE_FIELDS, "subject": zoe})
    raw_utf8 = json.dumps(
        {**_ALICE_FIELDS, "subject": zoe},
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    assert escaped != "sha256:" + hashlib.sha256(DIGEST_DOMAIN + raw_utf8).hexdigest()
    # No principal key, or a payload that cannot be read, is principal-less.
    plain = caller_token.mint(seed, agent="test-agent", exp=T + 86400)
    assert _unsigned_facts(plain) == ("none", T + 86400)
    paired = caller_token.mint(seed, agent="test-agent", exp=T + 90, run=RUN, work_item=WORK_ITEM)
    assert _unsigned_facts(paired) == ("none", T + 90)
    for unreadable in (
        "cct.payload.signature",
        "cct.present",
        "",
        "cct." + _b64url(b"[1]") + ".sig",
        "cct." + _b64url(b'{"principal":') + ".sig",
    ):
        assert _unsigned_facts(unreadable) == ("none", None), unreadable


@pytest.mark.parametrize("shape", sorted(_MALFORMED_PRINCIPALS))
def test_unsigned_facts_gives_any_principal_key_a_digest(shape: str) -> None:
    """@spec CONNECTOR-REQUESTER-3: a malformed principal is a requester, never "none".

    The proxy refuses it, so treating it as principal-less would let a
    malformed requester adopt a principal-less sandbox.
    """
    seed = _seed()
    value = _MALFORMED_PRINCIPALS[shape]
    token = _signed_cct(seed, {"agent": "test-agent", "exp": T + 3600, "principal": value})
    digest, exp = _unsigned_facts(token)
    assert digest == _principal_digest(value)
    assert digest not in {"none", "absent", _principal_digest(_ALICE_FIELDS)}
    assert exp == T + 3600


def test_unsigned_facts_reads_exp_as_the_route_expiry_read_does() -> None:
    """@spec CONNECTOR-REQUESTER-3: a non-integer or missing exp is unknown, never coerced."""
    seed = _seed()
    for exp_value in ("1900000000", 1.5, True, None):
        body: dict[str, Any] = {"agent": "test-agent", "principal": _ALICE_FIELDS}
        if exp_value is not None:
            body["exp"] = exp_value
        assert _unsigned_facts(_signed_cct(seed, body)) == (
            _principal_digest(_ALICE_FIELDS),
            None,
        ), exp_value


def test_principal_of_reads_exactly_the_three_principal_fields() -> None:
    """@spec CONNECTOR-REQUESTER-3: the re-sign and re-mint principal, or a refusal.

    No principal key is None. Anything but exactly the three valid strings
    raises ValueError, so no caller can fall back to a principal-less or
    longer token, and the refusal never carries the values.
    """
    reader = getattr(caller_token, "principal_of", None)
    assert callable(reader), "caller_token has no principal_of"
    seed = _seed()
    token = caller_token.mint(
        seed,
        agent="test-agent",
        exp=T + 3600,
        run=RUN,
        work_item=WORK_ITEM,
        principal=_principal(),
    )
    assert reader(token) == _principal()
    assert reader(caller_token.mint(seed, agent="test-agent", exp=T + 3600)) is None
    for shape, value in sorted(_MALFORMED_PRINCIPALS.items()):
        malformed = _signed_cct(seed, {"agent": "test-agent", "exp": T + 3600, "principal": value})
        with pytest.raises(ValueError) as caught:
            reader(malformed)
        assert SUBJECT not in str(caught.value), shape


@pytest.mark.parametrize(
    ("with_principal", "ceiling", "cct_exp", "ccr_exp"),
    [
        (True, None, T + 3600, T + 86400),
        (False, None, T + 86400, T + 86400),
        (True, T + 600, T + 600, T + 600),
        (True, T + 7200, T + 3600, T + 7200),
    ],
    ids=["principal", "none", "principal-ceiling-600", "principal-ceiling-7200"],
)
def test_requester_cct_lifetimes(
    monkeypatch: pytest.MonkeyPatch,
    with_principal: bool,
    ceiling: int | None,
    cct_exp: int,
    ccr_exp: int,
) -> None:
    """@spec CONNECTOR-REQUESTER-3: one hour for a requester cct; every ccr keeps its own.

    A ccr is an agent/run credential (ADR 0270 decision 2), so it keeps the
    24 hour or ceiling expiry; only the principal-bearing cct is capped at
    mint time plus one hour, and never lengthened past the ceiling.
    """
    assert getattr(binding_module, "REQUESTER_CALLER_TOKEN_TTL_SECONDS", None) == REQUESTER_TTL
    assert SANDBOX_TOKEN_TTL_SECONDS == 86400
    # A fractional clock pins the integer truncation of "now".
    monkeypatch.setattr(binding_module.time, "time", lambda: T + 0.75)
    seed = _seed()
    kwargs: dict[str, Any] = {"route_connectors": frozenset({"reviewer", "uploads"})}
    if with_principal:
        kwargs["caller_principal"] = _principal()
    if ceiling is not None:
        kwargs["caller_exp_ceiling"] = ceiling
    env = _boot(seed, **kwargs)
    claims = _verified_claims(seed, env[_CALLER_ENV], "cct")
    assert claims["exp"] == cct_exp
    assert ("principal" in claims) is with_principal
    expected_cct = caller_token.mint(
        seed,
        agent="test-agent",
        exp=cct_exp,
        principal=_principal() if with_principal else None,
    )
    # Principal-less: byte for byte what is minted today.
    assert env[_CALLER_ENV] == expected_cct
    tokens = json.loads(env[_ROUTES_ENV])
    assert sorted(tokens) == ["reviewer", "uploads"]
    for connector, token in tokens.items():
        route = _verified_claims(seed, token, "ccr")
        assert route["exp"] == ccr_exp, connector
        assert "principal" not in route
        assert token == caller_token.mint_route(
            seed,
            agent="test-agent",
            connector=connector,
            exp=ccr_exp,
        )


# -- connector_requester_identity_for against real Postgres --------------------------------


def _state_invalid() -> type[Exception]:
    """@spec CONNECTOR-REQUESTER-1: exported from curie_worker.binding."""
    cls = getattr(binding_module, "RequesterIdentityStateInvalid", None)
    assert cls is not None, "curie_worker.binding has no RequesterIdentityStateInvalid"
    assert issubclass(cls, Exception)
    return cls


@asynccontextmanager
async def _engine() -> AsyncIterator[AsyncEngine]:
    """@spec CONNECTOR-REQUESTER-1: the real compose Postgres."""
    engine = create_async_engine(_DB_URL)
    try:
        await pg_connect_or_skip(engine)
        yield engine
    finally:
        await engine.dispose()


@asynccontextmanager
async def _agent(engine: AsyncEngine) -> AsyncIterator[uuid.UUID]:
    """One default-tenant agent row, every column at its default. @spec CONNECTOR-REQUESTER-1."""
    agent_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"requester-{agent_id.hex[:12]}"},
        )
    try:
        yield agent_id
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id},
            )


async def _set_flag(engine: AsyncEngine, schema: str, agent_id: uuid.UUID, value: Any) -> None:
    """@spec CONNECTOR-REQUESTER-1: the operator column, written directly."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"UPDATE {schema}.agents SET connector_requester_identity = :value "
                "WHERE id = :id"
            ),
            {"value": value, "id": agent_id},
        )


def _run(body: Callable[[AsyncEngine], Awaitable[None]]) -> None:
    """@spec CONNECTOR-REQUESTER-1."""

    async def go() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        async with _engine() as engine:
            await body(engine)

    asyncio.run(go())


def test_a_missing_agent_row_reads_as_not_opted_in() -> None:
    """@spec CONNECTOR-REQUESTER-1: absence is off, never an error."""

    async def body(engine: AsyncEngine) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        resolver = BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA))
        assert await resolver.connector_requester_identity_for(uuid.uuid4()) is False

    _run(body)


def test_the_stored_flag_is_read_fresh_on_every_call() -> None:
    """@spec CONNECTOR-REQUESTER-1: default false, then exactly what the operator stored."""

    async def body(engine: AsyncEngine) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        resolver = BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA))
        async with _agent(engine) as agent_id:
            assert await resolver.connector_requester_identity_for(agent_id) is False
            await _set_flag(engine, _SCHEMA, agent_id, True)
            assert await resolver.connector_requester_identity_for(agent_id) is True
            await _set_flag(engine, _SCHEMA, agent_id, False)
            assert await resolver.connector_requester_identity_for(agent_id) is False

    _run(body)


# Each damage turns a readable copy of the real agents table into a stored value
# the column's NOT NULL BOOLEAN contract forbids. Reading one is a terminal refusal.
_DAMAGE = {
    "null": [
        "ALTER TABLE {s}.agents ALTER COLUMN connector_requester_identity DROP NOT NULL",
        "UPDATE {s}.agents SET connector_requester_identity = NULL",
    ],
    "text": [
        "ALTER TABLE {s}.agents ALTER COLUMN connector_requester_identity DROP DEFAULT",
        "ALTER TABLE {s}.agents ALTER COLUMN connector_requester_identity "
        "TYPE text USING 'true'",
    ],
    "integer": [
        "ALTER TABLE {s}.agents ALTER COLUMN connector_requester_identity DROP DEFAULT",
        "ALTER TABLE {s}.agents ALTER COLUMN connector_requester_identity "
        "TYPE integer USING 1",
    ],
}
# A pre-migration schema makes the SELECT itself raise (a DBAPIError): a failed
# read, not a stored value, so it is the retryable unavailable answer.
_READ_ERROR = ["ALTER TABLE {s}.agents DROP COLUMN connector_requester_identity"]


async def _damaged_read_raises(
    engine: AsyncEngine, statements: list[str], raised: type[Exception], never: type[Exception],
) -> None:
    """Read a damaged throwaway copy of the agent row; assert exactly what it raises.

    The copy lives in a throwaway schema (CREATE TABLE ... LIKE), so no curie
    object or production invariant is touched. The same copy first reads a
    stored true, so the outcome is the damage and not the copy.
    @spec CONNECTOR-REQUESTER-1.
    """
    schema = f"requester_flag_{uuid.uuid4().hex[:12]}"
    async with _agent(engine) as agent_id:
        try:
            async with engine.begin() as conn:
                await conn.execute(text(f"CREATE SCHEMA {schema}"))
                await conn.execute(text(
                    f"CREATE TABLE {schema}.agents "
                    f"(LIKE {_SCHEMA}.agents INCLUDING DEFAULTS)"
                ))
                await conn.execute(
                    text(
                        f"INSERT INTO {schema}.agents "
                        f"SELECT * FROM {_SCHEMA}.agents WHERE id = :id"
                    ),
                    {"id": agent_id},
                )
            await _set_flag(engine, schema, agent_id, True)
            resolver = BindingResolver(engine, WorkerConfig(db_schema=schema))
            assert await resolver.connector_requester_identity_for(agent_id) is True
            async with engine.begin() as conn:
                for statement in statements:
                    await conn.execute(text(statement.format(s=schema)))
            # Fresh connections, so no statement prepared before the damage is
            # reused and the reader sees the damaged column itself.
            await engine.dispose()
            with pytest.raises(raised) as caught:
                await resolver.connector_requester_identity_for(agent_id)
            assert not isinstance(caught.value, never)
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


@pytest.mark.parametrize("damage", sorted(_DAMAGE))
def test_any_state_but_a_stored_boolean_fails_closed(damage: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: NULL or a non-bool stored value is a terminal refusal."""
    invalid = _state_invalid()

    async def body(engine: AsyncEngine) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await _damaged_read_raises(engine, _DAMAGE[damage], invalid, TurnAuthorityUnavailable)

    _run(body)


def test_a_failed_select_is_retryable_unavailable_never_a_refusal() -> None:
    """@spec CONNECTOR-REQUESTER-1: a read error is redelivered, not refused as invalid state."""
    invalid = _state_invalid()

    async def body(engine: AsyncEngine) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await _damaged_read_raises(engine, _READ_ERROR, TurnAuthorityUnavailable, invalid)

    _run(body)


def test_an_unreachable_database_is_retryable_unavailable_never_a_refusal() -> None:
    """@spec CONNECTOR-REQUESTER-1: a refused connect (asyncpg's bare OSError) is unavailable.

    Nothing listens on port 1, so the read fails before any SQL is sent. This
    needs no Postgres: the failure is the point.
    """
    invalid = _state_invalid()

    async def go() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        engine = create_async_engine(
            "postgresql+asyncpg://postgres:postgres@127.0.0.1:1/postgres",
            connect_args={"timeout": 5},
        )
        try:
            resolver = BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA))
            with pytest.raises(TurnAuthorityUnavailable) as caught:
                await resolver.connector_requester_identity_for(uuid.uuid4())
            assert not isinstance(caught.value, invalid)
        finally:
            await engine.dispose()

    asyncio.run(go())


async def _terminate_backends(engine: AsyncEngine, application_name: str) -> None:
    """Kill every server session of one reader, waiting for each to exit.

    @spec CONNECTOR-REQUESTER-1: a real lost connection, not a raised double.
    """
    async with engine.begin() as conn:
        killed = (
            await conn.execute(
                text(
                    # The timeout form waits until each backend has exited
                    # (PostgreSQL 14+), so no reap poll is needed.
                    "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity "
                    "WHERE application_name = :name"
                ),
                {"name": application_name},
            )
        ).scalars().all()
    assert killed and all(killed), "the reader had no live session to lose"


def test_a_lost_connection_is_unavailable_and_the_next_read_recovers() -> None:
    """@spec CONNECTOR-REQUESTER-1: a transient fault is retryable; the retry reads the flag.

    The resolver's pooled connection is terminated server side between two
    reads. The read that lands on the dead connection is unavailable, never
    the terminal invalid-state refusal, and the next read answers the stored
    value again with no operator action.
    """
    invalid = _state_invalid()
    application_name = f"requester-flag-{uuid.uuid4().hex[:12]}"

    async def body(engine: AsyncEngine) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        reader = create_async_engine(
            _DB_URL, connect_args={"server_settings": {"application_name": application_name}},
        )
        try:
            async with _agent(engine) as agent_id:
                await _set_flag(engine, _SCHEMA, agent_id, True)
                resolver = BindingResolver(reader, WorkerConfig(db_schema=_SCHEMA))
                assert await resolver.connector_requester_identity_for(agent_id) is True
                await _terminate_backends(engine, application_name)
                with pytest.raises(TurnAuthorityUnavailable) as caught:
                    await resolver.connector_requester_identity_for(agent_id)
                assert not isinstance(caught.value, invalid)
                assert await resolver.connector_requester_identity_for(agent_id) is True
        finally:
            await reader.dispose()

    _run(body)
