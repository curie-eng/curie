"""The shared approval grant, ``csg`` (ADR 0239, AC5).

"Shared membership" 7: the explicitly selected shared approval-grant wire
binds agent binding, application, canonical tool, alias, argument digest and
policy generation; shared invocations never accept the dedicated approval
wire; shared grants stay single-use in real Valkey and cannot be laundered
through a second alias. Plan R1-2 adds that a handle pins the binding the
proxy admitted, so an account replacement committed during an approved (or
any in-flight) call cannot redirect it to the new account.

The minter and verifier are checked on the wire bytes. Through the real
proxy, every refusal is the one ``grant_required`` body with nothing reaching
the server, the resolver or the provider, and a refused grant is never burned:
its Valkey key ``connector-shared-grant:<jti>`` stays absent.
``apps/worker/tests/test_connector_proxy.py`` (the dedicated ``ccg`` suite)
is unchanged.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _resolver_harness import Fleet
from _shared_proxy_harness import (
    GRANT_HEADER,
    GRANT_KEY_PREFIX,
    ProxyReply,
    SharedProxy,
    Worlds,
    closed_port,
)
from curie_connector_proxy import caller
from curie_test_support.valkey import connect_or_skip
from nacl.signing import SigningKey

BOTH_ALIASES = [{"alias": "primary", "tools": None}, {"alias": "secondary", "tools": None}]
GRANT_CLAIMS = {
    "application",
    "binding",
    "alias",
    "tool",
    "args_sha256",
    "policy_generation",
    "exp",
    "jti",
}


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _seed(signing: SigningKey) -> str:
    return base64.b64encode(bytes(signing)).decode("ascii")


def _claims(**changes: Any) -> dict[str, Any]:
    claims: dict[str, Any] = {
        "application": str(uuid.uuid4()),
        "binding": str(uuid.uuid4()),
        "alias": "primary",
        "tool": "create_item",
        "args_sha256": hashlib.sha256(b'{"title":"x"}').hexdigest(),
        "policy_generation": 3,
        "exp": 1790000000,
        "jti": f"jti-{uuid.uuid4().hex}",
    }
    claims.update(changes)
    return claims


def _signed(signing: SigningKey, payload: bytes, prefix: str = "csg") -> str:
    """A token over arbitrary payload bytes, signed the way the wire signs."""

    signing_input = f"{prefix}.{_b64url(payload)}"
    signature = signing.sign(signing_input.encode("ascii")).signature
    return f"{signing_input}.{_b64url(signature)}"


def _canonical(claims: dict[str, Any]) -> bytes:
    return json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("ascii")


def _admitted(reply: ProxyReply) -> dict[str, Any]:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    return result


# =============================================================================
# The wire
# =============================================================================


def test_grant_minter_wire_shape() -> None:
    from curie_worker import connector_grant

    signing = SigningKey.generate()
    keys = (signing.verify_key,)
    claims = _claims()
    token = connector_grant.mint_shared(_seed(signing), **claims)
    assert token.isascii()
    prefix, payload, signature = token.split(".")
    assert prefix == "csg" == connector_grant.SHARED_PREFIX == caller.SHARED_GRANT_PREFIX
    raw = _unb64url(payload)
    assert raw == _canonical(claims)
    assert set(json.loads(raw)) == GRANT_CLAIMS
    # Unpadded base64url, one spelling.
    assert _b64url(raw) == payload and "=" not in token
    # The signature covers the ASCII prefix and payload.
    signing.verify_key.verify(f"csg.{payload}".encode("ascii"), _unb64url(signature))
    assert token == _signed(signing, raw)
    # Deterministic.
    assert connector_grant.mint_shared(_seed(signing), **claims) == token
    assert caller.verify_shared_grant(keys, token) == caller.SharedGrant(**claims)
    assert {"mint_shared", "SHARED_PREFIX", "mint", "PREFIX"} <= set(connector_grant.__all__)


def test_grant_the_dedicated_wire_is_unchanged_and_separate() -> None:
    from curie_worker import caller_token, connector_grant

    signing = SigningKey.generate()
    keys = (signing.verify_key,)
    dedicated_claims = {
        "agent": "acme-dev",
        "connector": "github",
        "tool": "merge_pull_request",
        "args": '{"n":1}',
        "exp": 1790000000,
        "jti": "jti-dedicated",
    }
    dedicated = connector_grant.mint(_seed(signing), **dedicated_claims)
    assert connector_grant.PREFIX == "ccg"
    assert dedicated == _signed(signing, _canonical(dedicated_claims), "ccg")
    assert caller.verify(keys, dedicated) == caller.Grant(**dedicated_claims)
    shared_token = connector_grant.mint_shared(_seed(signing), **_claims())
    # Neither wire is read as the other.
    assert caller.verify(keys, shared_token) is None
    assert caller.verify_shared_grant(keys, dedicated) is None
    caller_token_text = caller_token.mint(
        _seed(signing),
        installation="Zq8mX2vN7kP4tR9wL3sB6yH1cF5gJ0dA",
        agent_id=str(uuid.uuid4()),
        agent="acme-dev",
        exp=1790000000,
    )
    assert caller.verify_shared_grant(keys, caller_token_text) is None
    # Another key verifies nothing.
    assert caller.verify_shared_grant((SigningKey.generate().verify_key,), shared_token) is None


def _malformed_payloads(claims: dict[str, Any]) -> list[tuple[str, bytes]]:
    """Signed payloads the caller-strict parse and the exact claim rules refuse."""

    def changed(**values: Any) -> bytes:
        return _canonical({**claims, **values})

    canonical = _canonical(claims)
    non_ascii = json.dumps(
        {**claims, "tool": "créer"}, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    ).encode("utf-8")
    return [
        ("an extra claim", changed(agent="acme-dev")),
        ("a missing claim", _canonical({k: v for k, v in claims.items() if k != "jti"})),
        ("a duplicate key", canonical[:-1] + b',"jti":"jti-duplicate"}'),
        ("a negative exp", changed(exp=-1)),
        ("a float exp", changed(exp=1790000000.5)),
        (
            "a NaN generation",
            canonical.replace(b'"policy_generation":3', b'"policy_generation":NaN'),
        ),
        ("generation zero", changed(policy_generation=0)),
        ("an uppercase application", changed(application=claims["application"].upper())),
        ("a binding that is no uuid", changed(binding="binding-1")),
        ("an invalid alias", changed(alias="Bad_Alias")),
        ("an empty tool", changed(tool="")),
        ("a tool too long", changed(tool="t" * 129)),
        ("an uppercase digest", changed(args_sha256=claims["args_sha256"].upper())),
        ("a short digest", changed(args_sha256=claims["args_sha256"][:63])),
        ("an empty jti", changed(jti="")),
        ("a jti too long", changed(jti="j" * 129)),
        ("a jti with a control character", changed(jti="jti\n1")),
        ("a non-ASCII payload", non_ascii),
        ("a JSON array", b"[]"),
    ]


def test_grant_verifier_refuses_every_malformed_payload() -> None:
    signing = SigningKey.generate()
    keys = (signing.verify_key,)
    claims = _claims()
    # Liveness: the canonical payload, signed by hand, verifies.
    assert caller.verify_shared_grant(keys, _signed(signing, _canonical(claims))) == (
        caller.SharedGrant(**claims)
    )
    malformed = _malformed_payloads(claims)
    assert b'"policy_generation":NaN' in dict(malformed)["a NaN generation"]
    for label, payload_bytes in malformed:
        token = _signed(signing, payload_bytes)
        assert caller.verify_shared_grant(keys, token) is None, label
    good = _signed(signing, _canonical(claims))
    prefix, payload, signature = good.split(".")
    tampered = _b64url(_canonical({**claims, "alias": "secondary"}))
    assert caller.verify_shared_grant(keys, f"{prefix}.{tampered}.{signature}") is None
    assert caller.verify_shared_grant(keys, f"ccg.{payload}.{signature}") is None
    assert caller.verify_shared_grant(keys, _signed(signing, _canonical(claims), "ccg")) is None
    assert caller.verify_shared_grant(keys, "csg.not.valid") is None
    assert caller.verify_shared_grant(keys, good + ".extra") is None


_MINT_REFUSALS: list[tuple[str, dict[str, Any]]] = [
    ("application", {"application": "not-a-uuid"}),
    ("binding", {"binding": str(uuid.uuid4()).upper()}),
    ("alias", {"alias": "-primary"}),
    ("tool", {"tool": ""}),
    ("args_sha256", {"args_sha256": "0" * 63}),
    ("policy_generation", {"policy_generation": 0}),
    ("exp", {"exp": -5}),
    ("jti", {"jti": ""}),
    ("jti-non-ascii", {"jti": "jtï"}),
]


@pytest.mark.parametrize(
    "change", [change for _, change in _MINT_REFUSALS], ids=[name for name, _ in _MINT_REFUSALS]
)
def test_grant_minter_refuses_a_claim_the_verifier_would(change: dict[str, Any]) -> None:
    from curie_worker.connector_grant import mint_shared

    seed = _seed(SigningKey.generate())
    mint_shared(seed, **_claims())  # liveness
    with pytest.raises(ValueError):
        mint_shared(seed, **_claims(**change))


# =============================================================================
# Through the real proxy and real Valkey
# =============================================================================


def test_grant_a_valid_grant_is_admitted_once_and_spent_in_valkey(worlds: Worlds) -> None:
    world = worlds(gated_tools=("create_item",))
    member = world.member()
    arguments = {"title": "first", "labels": ["b", "a"], "body": "é"}
    token, jti = world.grant(member, "create_item", arguments)
    client = connect_or_skip()
    key = GRANT_KEY_PREFIX + jti
    try:
        assert client.exists(key) == 0
        before = world.observe()
        reply = world.call(member, "create_item", arguments, grants=[token])
        result = _admitted(reply)
        assert result["binding"] == member.binding_id
        assert result["provider_path"] == "/v1/created"
        assert result["authorization_sha256"] == world.digest(member)
        (seen,) = world.fixture.since(before.upstream)
        assert seen.handle is not None
        assert GRANT_HEADER.lower() not in seen.names
        assert client.exists(key) == 1
        ttl = client.ttl(key)
        # SET NX EX max(exp - now, 1), with exp 300 seconds after the mint.
        assert 0 < ttl <= 300, ttl
        # A replay is refused and never reaches the server.
        before = world.observe()
        replay = world.call(member, "create_item", arguments, grants=[token])
        world.assert_refused(replay, "grant_required", before)
        # An ungated tool needs no grant.
        assert _admitted(world.call(member, "whoami"))["binding"] == member.binding_id
    finally:
        client.delete(key)
        client.close()


def _with_other_key(world: SharedProxy, mint: Any) -> str:
    saved, world.seed = world.seed, _seed(SigningKey.generate())
    try:
        token: str = mint()
    finally:
        world.seed = saved
    return token


def test_grant_every_mismatch_is_refused_and_never_burned(worlds: Worlds) -> None:
    world = worlds(gated_tools=("create_item",))
    member = world.member(aliases=BOTH_ALIASES)
    arguments = {"title": "x"}
    generation = world.generation()

    def grant(**changes: Any) -> tuple[str, str]:
        tool = changes.pop("tool", "create_item")
        args = changes.pop("arguments", arguments)
        return world.grant(member, tool, args, **changes)

    first, first_jti = grant()
    second, second_jti = grant()
    cases: list[tuple[str, list[str], list[str]]] = [
        ("the dedicated ccg wire", [world.dedicated_grant(member, "create_item", arguments)], []),
        ("no grant header", [], []),
        ("two grant headers", [first, second], [first_jti, second_jti]),
        ("garbage", ["csg.not.valid"], []),
    ]
    for label, changes in [
        ("another alias", {"alias": "secondary"}),
        ("another application", {"application": str(uuid.uuid4())}),
        ("another binding", {"binding": str(uuid.uuid4())}),
        ("another tool", {"tool": "hold"}),
        ("another argument digest", {"arguments": {"title": "y"}}),
        ("a later generation", {"policy_generation": generation + 1}),
        ("an earlier generation", {"policy_generation": max(generation - 1, 1)}),
        ("expired", {"exp": int(time.time()) - 1}),
    ]:
        if label == "an earlier generation" and generation == 1:
            continue
        token, jti = grant(**changes)
        cases.append((label, [token], [jti]))
    other_token = _with_other_key(world, lambda: grant()[0])
    cases.append(("another signing key", [other_token], []))

    client = connect_or_skip()
    try:
        for label, grants, jtis in cases:
            before = world.observe()
            reply = world.call(member, "create_item", arguments, grants=grants)
            try:
                world.assert_refused(reply, "grant_required", before)
            except AssertionError as error:
                raise AssertionError(f"{label}: {error}") from None
            for jti in jtis:
                assert client.exists(GRANT_KEY_PREFIX + jti) == 0, f"{label} burned its grant"
        # Liveness: an untouched matching grant is admitted after all of them.
        reply = world.call(member, "create_item", arguments, grants=[first])
        assert _admitted(reply)["binding"] == member.binding_id
        assert client.exists(GRANT_KEY_PREFIX + first_jti) == 1
    finally:
        client.delete(GRANT_KEY_PREFIX + first_jti, GRANT_KEY_PREFIX + second_jti)
        client.close()


def test_grant_cannot_be_laundered_through_another_alias(worlds: Worlds) -> None:
    world = worlds(gated_tools=("create_item",))
    member = world.member(aliases=BOTH_ALIASES)
    arguments = {"title": "launder"}
    token, jti = world.grant(member, "create_item", arguments, alias="primary")
    client = connect_or_skip()
    key = GRANT_KEY_PREFIX + jti
    try:
        before = world.observe()
        reply = world.call(member, "create_item", arguments, grants=[token], alias="secondary")
        world.assert_refused(reply, "grant_required", before)
        assert client.exists(key) == 0
        # Still spendable, once, on the alias it names.
        reply = world.call(member, "create_item", arguments, grants=[token], alias="primary")
        assert _admitted(reply)["binding"] == member.binding_id
        assert client.exists(key) == 1
        for alias in ("secondary", "primary"):
            before = world.observe()
            reply = world.call(member, "create_item", arguments, grants=[token], alias=alias)
            world.assert_refused(reply, "grant_required", before)
    finally:
        client.delete(key)
        client.close()


def test_grant_an_unavailable_store_refuses(worlds: Worlds) -> None:
    world = worlds(
        gated_tools=("create_item",), grant_store_url=f"redis://127.0.0.1:{closed_port()}/0"
    )
    member = world.member()
    arguments = {"title": "no store"}
    token, _ = world.grant(member, "create_item", arguments)
    before = world.observe()
    reply = world.call(member, "create_item", arguments, grants=[token])
    world.assert_refused(reply, "grant_required", before)
    # Liveness: the store is only consulted for gated tools.
    assert _admitted(world.call(member, "whoami"))["binding"] == member.binding_id


def test_grant_an_encoded_body_cannot_hide_a_gated_call(worlds: Worlds) -> None:
    """CR1-1: an encoded gated call is refused, and refusing it spends no grant."""

    world = worlds(gated_tools=("create_item",))
    member = world.member()
    arguments = {"title": "encoded"}
    token, jti = world.grant(member, "create_item", arguments)
    client = connect_or_skip()
    key = GRANT_KEY_PREFIX + jti
    try:
        for grants in ([], [token]):
            before = world.observe()
            reply = world.post_encoded(
                member, world.call_message("create_item", arguments), "gzip", grants=grants
            )
            world.assert_refused(reply, "body_encoding_unsupported", before)
            assert client.exists(key) == 0
        # Liveness: the same grant on the unencoded call is admitted once.
        reply = world.call(member, "create_item", arguments, grants=[token])
        assert _admitted(reply)["binding"] == member.binding_id
        assert client.exists(key) == 1
    finally:
        client.delete(key)
        client.close()


def test_grant_a_call_refused_for_capacity_spends_no_grant(worlds: Worlds) -> None:
    """CR1-2: invocations_full refuses before the single-use grant is spent."""

    world = worlds(gated_tools=("create_item",), max_invocations=1)
    holder, member = world.member(), world.member()
    arguments = {"title": "capacity"}
    token, jti = world.grant(member, "create_item", arguments)
    client = connect_or_skip()
    key = GRANT_KEY_PREFIX + jti
    try:
        hold, future = world.start_hold(holder, "capacity")
        before = world.observe()
        reply = world.call(member, "create_item", arguments, grants=[token])
        world.assert_refused(reply, "invocations_full", before)
        assert client.exists(key) == 0, "a call refused for capacity burned its grant"
        outcome = _admitted(world.finish_hold(hold, future))
        assert outcome["second"]["binding"] == holder.binding_id
        world.wait_for_invocations(0)
        # Capacity is back: the same grant is admitted once, then spent.
        reply = world.call(member, "create_item", arguments, grants=[token])
        assert _admitted(reply)["binding"] == member.binding_id
        assert client.exists(key) == 1
        before = world.observe()
        replay = world.call(member, "create_item", arguments, grants=[token])
        world.assert_refused(replay, "grant_required", before)
    finally:
        client.delete(key)
        client.close()


def test_grant_a_spend_that_cannot_reach_its_worker_thread_refuses_closed(
    worlds: Worlds,
) -> None:
    """The spend's executor hand-off failing is a failed spend: 403, never a 500."""

    from curie_connector_proxy import shared

    world = worlds(gated_tools=("create_item",))
    member = world.member()
    arguments = {"title": "no thread"}
    token, jti = world.grant(member, "create_item", arguments)
    client = connect_or_skip()
    key = GRANT_KEY_PREFIX + jti

    def no_thread(*args: object, **kwargs: object) -> Any:
        raise RuntimeError("can't start new thread")

    try:
        # Patched on the module object shared.py calls through, for this request
        # only; the proxy's event loop thread sees it, which is the point.
        # Restored by hand: the store fixture's monkeypatch must not be undone.
        original = shared.asyncio.to_thread
        shared.asyncio.to_thread = no_thread  # type: ignore[assignment]
        try:
            before = world.observe()
            reply = world.call(member, "create_item", arguments, grants=[token])
        finally:
            shared.asyncio.to_thread = original
        world.assert_refused(reply, "grant_required", before)
        assert reply.json["error"]["data"] == {
            "curie_caller": "grant_required",
            "curie_shared": "grant_required",
        }
        assert client.exists(key) == 0
        # Liveness: restored, the same grant is admitted once.
        reply = world.call(member, "create_item", arguments, grants=[token])
        assert _admitted(reply)["binding"] == member.binding_id
        assert client.exists(key) == 1
    finally:
        client.delete(key)
        client.close()


# =============================================================================
# R1-2: a handle pins the admitted binding
# =============================================================================


def test_grant_an_account_replacement_during_an_approved_call_refuses_binding_mismatch(
    worlds: Worlds,
) -> None:
    world = worlds(gated_tools=("hold",))
    member = world.member()
    arguments = {"key": "approved", "resolve_first": False}
    token, jti = world.grant(member, "hold", arguments)
    client = connect_or_skip()
    try:
        hold, future = world.start_hold(
            member, "approved", arguments={"resolve_first": False}, grants=[token]
        )
        old_binding, old_text = world.replace_account(member)
        assert member.binding_id != old_binding
        new_digest = world.digest(member)
        outcome = _admitted(world.finish_hold(hold, future))
        assert outcome["first"] is None
        assert outcome["second"] == {"refused": "binding_mismatch", "status": 403}
        assert world.provider.calls("/v1/hold/second") == []
        seen = [request.authorization_sha256 for request in world.provider.requests]
        assert new_digest not in seen
        # Liveness: a new call after the replacement is served on the new account.
        result = _admitted(world.call(member, "whoami"))
        assert result["binding"] == member.binding_id and result["account"] == member.account
        assert result["authorization_sha256"] == new_digest
        assert old_text != member.text
    finally:
        client.delete(GRANT_KEY_PREFIX + jti)
        client.close()


def test_grant_an_in_flight_call_across_a_replacement_refuses_binding_mismatch(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member()
    old_digest = world.digest(member)
    hold, future = world.start_hold(member, "in-flight")
    assert hold.first is not None and hold.first["binding"] == member.binding_id
    assert hold.first["authorization_sha256"] == old_digest
    old_binding, _ = world.replace_account(member)
    new_digest = world.digest(member)
    outcome = _admitted(world.finish_hold(hold, future))
    assert outcome["first"]["binding"] == old_binding
    assert outcome["second"] == {"refused": "binding_mismatch", "status": 403}
    assert new_digest not in [request.authorization_sha256 for request in world.provider.requests]
