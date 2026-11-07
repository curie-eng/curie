"""Unit tests of the ADR 0274 wake signing module against the frozen vectors.

Pure: no database, Valkey or app. The module is the unit under test, so it is
imported here; the vectors and the independent reference in
``_hook_wake_support`` are the oracle.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest
from _hook_wake_support import (
    VECTORS,
    minimum_reservation_s,
    reference_dedupe_key,
    reference_key,
    reference_material,
    reference_scope,
    reference_signature,
)
from curie_api import hook_signing, hook_wake_signing

CORPUS = json.loads(VECTORS.read_text())
WAKE_CASES = CORPUS["vectors"] + CORPUS["negative_cases"]
ORIGINAL = CORPUS["vectors"][0]
AGENT = "00000000-0000-4000-8000-000000000001"


def _request(case: dict[str, Any]) -> dict[str, Any]:
    return dict(
        timestamp=case["timestamp"],
        delivery_id=case["delivery_id"],
        agent_id=case["agent_id"],
        hook=case["hook"],
        body=case["body"].encode("utf-8"),
    )


def _derive(case: dict[str, Any]) -> str:
    return hook_wake_signing.derive_key(
        case["installation_key"],
        agent_id=case["agent_id"],
        hook=case["hook"],
        generation=case["generation"],
    )


def test_constants_match_the_frozen_contract() -> None:
    assert hook_wake_signing.WAKE_PREFIX == b"curie.hook.wake.v1"
    assert hook_wake_signing.KEY_LABEL == "curie.hook.wake.key.v1"
    assert hook_wake_signing.SCOPE_LABEL == b"curie.hook.wake.scope.v1"
    assert hook_wake_signing.DEDUPE_NAMESPACE == "curie:hook-wake:v1"
    assert hook_wake_signing.MAX_GENERATION == 2**63 - 1
    assert hook_wake_signing.DEDUPE_CLOCK_SKEW_MARGIN_S == 30
    assert hook_wake_signing.TOLERANCE_S == hook_signing.TOLERANCE_S == 300
    assert hook_wake_signing.MAX_BODY == b"{}"


@pytest.mark.parametrize("case", WAKE_CASES, ids=lambda case: case["id"])
def test_derived_key_and_scope_reproduce_every_vector(case: dict[str, Any]) -> None:
    expected = case["expected"]
    key = _derive(case)
    assert key == expected["derived_key"]
    assert key == reference_key(
        case["installation_key"], case["agent_id"], case["hook"], case["generation"]
    )
    assert (
        hook_wake_signing.installation_scope(case["installation_key"])
        == (expected["installation_scope"])
    )


@pytest.mark.parametrize(
    "case",
    [case for case in WAKE_CASES if case["prefix"] == "curie.hook.wake.v1"],
    ids=lambda case: case["id"],
)
def test_material_and_signature_reproduce_wake_prefix_vectors(case: dict[str, Any]) -> None:
    expected = case["expected"]
    key = _derive(case)
    material = hook_wake_signing.material(**_request(case))
    assert material.hex() == expected["material_hex"]
    assert material == reference_material(
        case["timestamp"],
        case["delivery_id"],
        case["agent_id"],
        case["hook"],
        case["body"].encode("utf-8"),
    )
    assert hook_wake_signing.sign(key, **_request(case)) == expected["signature"]
    assert hook_wake_signing.verify(
        key,
        **_request(case),
        header=expected["signature"],
        now=float(int(case["timestamp"])),
    )


@pytest.mark.parametrize(
    "case",
    [case for case in WAKE_CASES if case["prefix"] != "curie.hook.wake.v1"],
    ids=lambda case: case["id"],
)
def test_other_purpose_prefixes_never_authenticate_a_wake(case: dict[str, Any]) -> None:
    expected = case["expected"]
    key = _derive(case)
    assert hook_wake_signing.material(**_request(case)).hex() != expected["material_hex"]
    assert hook_wake_signing.sign(key, **_request(case)) != expected["signature"]
    assert not hook_wake_signing.verify(
        key,
        **_request(case),
        header=expected["signature"],
        now=float(int(case["timestamp"])),
    )


@pytest.mark.parametrize("case", CORPUS["negative_cases"], ids=lambda case: case["id"])
def test_negative_signatures_fail_the_original_request(case: dict[str, Any]) -> None:
    original_key = ORIGINAL["expected"]["derived_key"]
    assert not hook_wake_signing.verify(
        original_key,
        **_request(ORIGINAL),
        header=case["expected"]["signature"],
        now=float(int(ORIGINAL["timestamp"])),
    )
    # The negative case's own key over the original request is also refused,
    # unless it is the very same key (body and prefix cases differ elsewhere).
    if case["expected"]["derived_key"] != original_key:
        assert not hook_wake_signing.verify(
            case["expected"]["derived_key"],
            **_request(ORIGINAL),
            header=ORIGINAL["expected"]["signature"],
            now=float(int(ORIGINAL["timestamp"])),
        )


def test_leading_zero_timestamp_is_signed_verbatim() -> None:
    zero = CORPUS["vectors"][1]
    assert zero["timestamp"].startswith("0")
    key = _derive(zero)
    signature = hook_wake_signing.sign(key, **_request(zero))
    assert signature == zero["expected"]["signature"]
    assert signature != ORIGINAL["expected"]["signature"]
    assert hook_wake_signing.verify(
        key, **_request(zero), header=signature, now=float(int(zero["timestamp"]))
    )
    # The unpadded spelling's signature does not authenticate the padded one.
    assert not hook_wake_signing.verify(
        key,
        **_request(zero),
        header=ORIGINAL["expected"]["signature"],
        now=float(int(zero["timestamp"])),
    )


@pytest.mark.parametrize("case", CORPUS["scope_vectors"], ids=lambda case: case["expected_scope"])
def test_installation_scope_vectors(case: dict[str, Any]) -> None:
    scope = hook_wake_signing.installation_scope(case["installation_key"])
    assert scope == case["expected_scope"] == reference_scope(case["installation_key"])
    assert scope != hashlib.sha256(case["installation_key"].encode()).hexdigest()


def test_dedupe_key_format() -> None:
    api_key = ORIGINAL["installation_key"]
    key = hook_wake_signing.dedupe_key(
        api_key, agent_id=AGENT, hook="queue-ready", generation=7, delivery_id="example-delivery"
    )
    scope = ORIGINAL["expected"]["installation_scope"]
    assert key == f"curie:hook-wake:v1:{scope}:{AGENT}:queue-ready:7:example-delivery"
    assert key == reference_dedupe_key(api_key, AGENT, "queue-ready", 7, "example-delivery")
    assert api_key not in key
    assert hashlib.sha256(api_key.encode()).hexdigest() not in key
    other_generation = hook_wake_signing.dedupe_key(
        api_key, agent_id=AGENT, hook="queue-ready", generation=8, delivery_id="example-delivery"
    )
    other_hook = hook_wake_signing.dedupe_key(
        api_key, agent_id=AGENT, hook="another-queue", generation=7, delivery_id="example-delivery"
    )
    assert len({key, other_generation, other_hook}) == 3


@pytest.mark.parametrize(
    ("timestamp", "now", "expected"),
    [
        ("1794000000", 1794000000.0, 331),
        ("1794000000", 1794000300.0, 31),
        ("1794000000", 1793999700.0, 631),
        ("1794000000", 1794999999.0, 31),
        ("1794000000", 1794000000.5, 330),
        ("01794000000", 1794000000.0, 331),
    ],
)
def test_dedupe_ttl_boundaries(timestamp: str, now: float, expected: int) -> None:
    ttl = hook_wake_signing.dedupe_ttl_s(timestamp, now)
    assert ttl == expected
    assert ttl == minimum_reservation_s(timestamp, now) + 30
    # The reservation always outlives the signed timestamp plus the window.
    assert now + ttl > int(timestamp) + 300


@pytest.mark.parametrize("generation", [0, -1, True, False, 2**63, 1.0, "1", None])
def test_derive_key_refuses_out_of_range_generation(generation: Any) -> None:
    with pytest.raises(ValueError):
        hook_wake_signing.derive_key(
            "example-installation-signing-key",
            agent_id=AGENT,
            hook="queue-ready",
            generation=generation,
        )


@pytest.mark.parametrize("generation", [1, 2, 2**31, 2**63 - 1])
def test_derive_key_accepts_the_contract_range(generation: int) -> None:
    key = hook_wake_signing.derive_key(
        "example-installation-signing-key",
        agent_id=AGENT,
        hook="queue-ready",
        generation=generation,
    )
    assert key == reference_key(
        "example-installation-signing-key", AGENT, "queue-ready", generation
    )
    assert "=" not in key


def test_canonical_agent_id_accepts_only_lowercase_hyphenated() -> None:
    lettered = "abcdef00-0000-4000-8000-000000000001"
    assert hook_wake_signing.canonical_agent_id(AGENT) == AGENT
    assert hook_wake_signing.canonical_agent_id(lettered) == lettered
    for raw in (
        lettered.upper(),
        "ABCDEF00-0000-4000-8000-000000000001",
        "Abcdef00-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-00000000000A",
        "{" + AGENT + "}",
        AGENT.replace("-", ""),
        "urn:uuid:" + AGENT,
        " " + AGENT,
        AGENT + "\n",
        AGENT[:-1],
        "",
        "not-a-uuid",
    ):
        assert hook_wake_signing.canonical_agent_id(raw) is None, raw


GOOD_SIGNATURE = "sha256=" + "0" * 64


@pytest.mark.parametrize(
    ("timestamp", "delivery_id", "signature", "expected"),
    [
        ("1", "a", GOOD_SIGNATURE, True),
        ("9" * 12, "A_z-0" * 25 + "abc", GOOD_SIGNATURE, True),
        ("001794000000", "x" * 128, GOOD_SIGNATURE, True),
        ("", "a", GOOD_SIGNATURE, False),
        ("1" * 13, "a", GOOD_SIGNATURE, False),
        ("+1794000000", "a", GOOD_SIGNATURE, False),
        ("-1794000000", "a", GOOD_SIGNATURE, False),
        ("17940 00000", "a", GOOD_SIGNATURE, False),
        (" 1794000000", "a", GOOD_SIGNATURE, False),
        ("١٢٣", "a", GOOD_SIGNATURE, False),
        ("1794000000", "", GOOD_SIGNATURE, False),
        ("1794000000", "x" * 129, GOOD_SIGNATURE, False),
        ("1794000000", "a.b", GOOD_SIGNATURE, False),
        ("1794000000", "a b", GOOD_SIGNATURE, False),
        ("1794000000", "café", GOOD_SIGNATURE, False),
        ("1794000000", "a", "sha256=" + "A" * 64, False),
        ("1794000000", "a", "0" * 64, False),
        ("1794000000", "a", "sha256=" + "0" * 63, False),
        ("1794000000", "a", "sha256=" + "0" * 65, False),
        ("1794000000", "a", "SHA256=" + "0" * 64, False),
        ("1794000000", "a", "sha256=" + "٠" * 64, False),
    ],
)
def test_well_formed_headers(
    timestamp: str, delivery_id: str, signature: str, expected: bool
) -> None:
    assert hook_wake_signing.well_formed(timestamp, delivery_id, signature) is expected


@pytest.mark.parametrize(
    ("offset", "expected"),
    [(0, True), (300, True), (-300, True), (301, False), (-301, False), (299.5, True)],
)
def test_in_window_is_inclusive_on_both_sides(offset: float, expected: bool) -> None:
    timestamp = "1794000000"
    assert hook_wake_signing.in_window(timestamp, float(1794000000 + offset)) is expected


def test_verify_refuses_stale_future_and_malformed_inputs() -> None:
    key = ORIGINAL["expected"]["derived_key"]
    signature = ORIGINAL["expected"]["signature"]
    stamp = int(ORIGINAL["timestamp"])
    request = _request(ORIGINAL)
    assert hook_wake_signing.verify(key, **request, header=signature, now=stamp + 300.0)
    assert hook_wake_signing.verify(key, **request, header=signature, now=stamp - 300.0)
    assert not hook_wake_signing.verify(key, **request, header=signature, now=stamp + 301.0)
    assert not hook_wake_signing.verify(key, **request, header=signature, now=stamp - 301.0)
    assert not hook_wake_signing.verify(
        key, **request, header=signature.upper().replace("SHA256=", "sha256="), now=stamp
    )
    assert not hook_wake_signing.verify(key, **request, header=signature[7:], now=stamp)
    assert not hook_wake_signing.verify(
        key, **{**request, "timestamp": "1" * 13}, header=signature, now=stamp
    )
    assert not hook_wake_signing.verify(
        key, **{**request, "delivery_id": "a.b"}, header=signature, now=stamp
    )
    assert not hook_wake_signing.verify(
        key, **{**request, "body": b"{ }"}, header=signature, now=stamp
    )
    assert not hook_wake_signing.verify(
        reference_key("another-installation-key", AGENT, "queue-ready", 1),
        **request,
        header=signature,
        now=stamp,
    )


def test_module_signature_matches_the_independent_reference() -> None:
    key = reference_key("example-installation-signing-key", AGENT, "another-queue", 3)
    request = dict(
        timestamp="0000000042",
        delivery_id="Delivery_with-Mixed_09",
        agent_id=AGENT,
        hook="another-queue",
        body=b"{}",
    )
    assert hook_wake_signing.sign(key, **request) == reference_signature(
        key, "0000000042", "Delivery_with-Mixed_09", AGENT, "another-queue", b"{}"
    )
