"""@spec PROTECTED-HOOK-SOURCE-4 PROTECTED-HOOK-SOURCE-9."""

from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import importlib.util
import json
from types import ModuleType

import pytest
from curie_api import hook_signing

API_KEY = "placeholder-platform-key"
AGENT = "12345678-1234-5678-9abc-def012345678"
OTHER_AGENT = "12345678-1234-5678-9abc-def012345679"
HOOK = "issues:résumé"
TIMESTAMP = "1800000000"
DELIVERY_ID = "support-probe-1"
BODY = b'{ "tool_access": "read-only", "text": "\\u00e9" }\n'


def _source() -> ModuleType:
    """@spec PROTECTED-HOOK-SOURCE-4 PROTECTED-HOOK-SOURCE-9."""
    name = "curie_api.hook_source_signing"
    assert importlib.util.find_spec(name) is not None, (
        "Protected sources need scoped key derivation and purpose-specific support signatures"
    )
    return importlib.import_module(name)


def _key_oracle(agent: str = AGENT, hook: str = HOOK, generation: int = 7) -> str:
    """@spec PROTECTED-HOOK-SOURCE-4."""
    material = json.dumps(
        ["curie.hook.source.v1", agent, hook, generation],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    digest = hmac.new(API_KEY.encode(), material, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _signature_oracle(
    secret: str,
    *,
    support: bool,
    timestamp: str = TIMESTAMP,
    delivery_id: str = DELIVERY_ID,
    hook: str = HOOK,
    tool_access: str | None = "read-only",
    body: bytes = BODY,
) -> str:
    """@spec PROTECTED-HOOK-SOURCE-4 PROTECTED-HOOK-SOURCE-9."""
    context = json.dumps([hook, tool_access], ensure_ascii=True, separators=(",", ":")).encode(
        "ascii"
    )
    material = (
        b"curie.hook.delivery.v2\n"
        + f"{timestamp}.{delivery_id}.{len(context)}:".encode("ascii")
        + context
        + body
    )
    if support:
        material = b"curie.hook.support.v1\n" + material
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def test_scoped_derivation_matches_independent_known_ascii_json_vector() -> None:
    """@spec PROTECTED-HOOK-SOURCE-4."""
    expected = "EdlEWU8iDK4UZoNBq3rTtq67d6tuB4JRG71PmFZyQfo"
    assert _key_oracle() == expected
    derive = _source().derive
    assert derive(API_KEY, agent_id=AGENT, hook=HOOK, generation=7) == expected
    assert derive(API_KEY, agent_id=AGENT, hook=HOOK, generation=7) == expected
    assert derive(API_KEY, agent_id=AGENT.upper(), hook=HOOK, generation=7) == expected
    assert derive(API_KEY, agent_id="{" + AGENT + "}", hook=HOOK, generation=7) == expected
    assert derive(API_KEY, agent_id=AGENT.replace("-", ""), hook=HOOK, generation=7) == expected


def test_scoped_keys_separate_agents_hooks_generations_and_platform_keys() -> None:
    """@spec PROTECTED-HOOK-SOURCE-4 PROTECTED-HOOK-SOURCE-5."""
    derive = _source().derive
    scopes = [(AGENT, HOOK, 7), (OTHER_AGENT, HOOK, 7), (AGENT, "other", 7), (AGENT, HOOK, 8)]
    keys = [
        derive(API_KEY, agent_id=agent, hook=hook, generation=gen) for agent, hook, gen in scopes
    ]
    assert len(set(keys)) == len(scopes)
    for key, (agent, hook, gen) in zip(keys, scopes, strict=True):
        assert key == _key_oracle(agent, hook, gen)
    assert derive("other-platform-key", agent_id=AGENT, hook=HOOK, generation=7) not in keys
    assert hook_signing.derive(API_KEY, agent_id=AGENT, generation=7) not in keys


@pytest.mark.parametrize("access", [None, "read-only"])
def test_scoped_secret_preserves_delivery_v2_and_refuses_other_source_keys(
    access: str | None,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-4."""
    derive = _source().derive
    key = derive(API_KEY, agent_id=AGENT, hook=HOOK, generation=7)
    args = dict(
        timestamp=TIMESTAMP, delivery_id=DELIVERY_ID, hook=HOOK, tool_access=access, body=BODY
    )
    header = _signature_oracle(key, support=False, **args)
    assert hook_signing.sign(key, **args) == header
    assert hook_signing.verify(key, header=header, now=int(TIMESTAMP), **args)
    bad_keys = [
        hook_signing.derive(API_KEY, agent_id=AGENT, generation=7),
        derive(API_KEY, agent_id=OTHER_AGENT, hook=HOOK, generation=7),
        derive(API_KEY, agent_id=AGENT, hook="other", generation=7),
        derive(API_KEY, agent_id=AGENT, hook=HOOK, generation=8),
    ]
    for bad_key in bad_keys:
        bad_header = _signature_oracle(bad_key, support=False, **args)
        assert not hook_signing.verify(key, header=bad_header, now=int(TIMESTAMP), **args)
        assert not hook_signing.verify(bad_key, header=header, now=int(TIMESTAMP), **args)


@pytest.mark.parametrize("access", [None, "read-only"])
@pytest.mark.parametrize("delivery_id", [DELIVERY_ID, "", "support", "ordinary-delivery"])
def test_support_exact_raw_material_and_mutual_delivery_replay_refusal(
    access: str | None, delivery_id: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    key = _key_oracle()
    args = dict(
        timestamp=TIMESTAMP, delivery_id=delivery_id, hook=HOOK, tool_access=access, body=BODY
    )
    support = _signature_oracle(key, support=True, **args)
    delivery = _signature_oracle(key, support=False, **args)
    assert source.sign_support(key, **args) == support
    assert source.verify_support(key, header=support, now=int(TIMESTAMP), **args)
    assert hook_signing.sign(key, **args) == delivery
    assert hook_signing.verify(key, header=delivery, now=int(TIMESTAMP), **args)
    assert not source.verify_support(key, header=delivery, now=int(TIMESTAMP), **args)
    assert not hook_signing.verify(key, header=support, now=int(TIMESTAMP), **args)


@pytest.mark.parametrize(
    "change",
    [
        {"body": BODY.replace(b" ", b"")},
        {"body": BODY + b"\n"},
        {"hook": "other"},
        {"tool_access": None},
        {"delivery_id": "other-delivery"},
        {"timestamp": "1800000001"},
    ],
)
def test_support_signature_binds_every_delivery_field(change: dict[str, object]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    key = _key_oracle()
    args = dict(
        timestamp=TIMESTAMP, delivery_id=DELIVERY_ID, hook=HOOK, tool_access="read-only", body=BODY
    )
    header = _signature_oracle(key, support=True)
    assert not source.verify_support(key, header=header, now=int(TIMESTAMP), **(args | change))
    assert not source.verify_support("other-source-key", header=header, now=int(TIMESTAMP), **args)


@pytest.mark.parametrize(
    "offset,accepted", [(-301, False), (-300, True), (0, True), (300, True), (301, False)]
)
def test_support_timestamp_window_matches_existing_delivery(offset: int, accepted: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    key = _key_oracle()
    timestamp = str(int(TIMESTAMP) + offset)
    args = dict(
        timestamp=timestamp, delivery_id=DELIVERY_ID, hook=HOOK, tool_access=None, body=BODY
    )
    for support, verify in [(True, source.verify_support), (False, hook_signing.verify)]:
        header = _signature_oracle(key, support=support, **args)
        assert verify(key, header=header, now=int(TIMESTAMP), **args) is accepted


@pytest.mark.parametrize(
    "timestamp",
    [
        None,
        "",
        "-1800000000",
        "+1800000000",
        " 1800000000",
        "1800000000 ",
        "1_800000000",
        "１８００００００００",
        "1800000000.0",
        "1" * 13,
        "1" * 5000,
    ],
    ids=[
        "missing",
        "empty",
        "negative",
        "positive-sign",
        "leading-space",
        "trailing-space",
        "underscore",
        "non-ascii",
        "decimal",
        "13-digits",
        "5000-digits",
    ],
)
def test_support_refuses_malformed_timestamps_without_raising(timestamp: str | None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    key = _key_oracle()
    args = dict(
        timestamp=timestamp, delivery_id=DELIVERY_ID, hook=HOOK, tool_access=None, body=BODY
    )
    for support, verify in [(True, source.verify_support), (False, hook_signing.verify)]:
        header = _signature_oracle(key, support=support, tool_access=None)
        assert not verify(key, header=header, now=int(TIMESTAMP), **args)


@pytest.mark.parametrize("header", [None, "", "sha1=bad", "sha256=bad", "sha256=" + "0" * 64])
def test_support_refuses_malformed_signature_headers_like_delivery(header: str | None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    args = dict(
        timestamp=TIMESTAMP, delivery_id=DELIVERY_ID, hook=HOOK, tool_access=None, body=BODY
    )
    for verify in [source.verify_support, hook_signing.verify]:
        assert not verify(_key_oracle(), header=header, now=int(TIMESTAMP), **args)


@pytest.mark.parametrize("delivery_id", ["a.b", ".", "1800000000.extra"])
def test_support_dotted_delivery_id_refused_and_signer_raises(delivery_id: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    source = _source()
    key = _key_oracle()
    args = dict(
        timestamp=TIMESTAMP, delivery_id=delivery_id, hook=HOOK, tool_access=None, body=BODY
    )
    for support, verify in [(True, source.verify_support), (False, hook_signing.verify)]:
        header = _signature_oracle(key, support=support, **args)
        assert not verify(key, header=header, now=int(TIMESTAMP), **args)
    for sign in [source.sign_support, hook_signing.sign]:
        with pytest.raises(ValueError):
            sign(key, **args)


def test_support_non_ascii_signature_header_refuses_without_exception() -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    assert not _source().verify_support(
        _key_oracle(),
        timestamp=TIMESTAMP,
        delivery_id=DELIVERY_ID,
        hook=HOOK,
        tool_access=None,
        body=BODY,
        header="sha256=é",
        now=int(TIMESTAMP),
    )
