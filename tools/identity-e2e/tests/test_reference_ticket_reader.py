"""The reference connector's ticket reader, held to the product over the shared vectors.

``human_restricted_connector`` is standard library only (it ships inline as a
connectors.yaml env value), so it carries its own Ed25519 verify, strict ticket
reader and argument digest. ``read_ticket`` and ``execution_arguments_sha256``
must agree with ``curie_internal.approver_ticket`` over
tests/vectors/approver-ticket.json and tests/vectors/action-canonical-arguments.json,
and ``ed25519_verify`` with PyNaCl (libsodium).

``read_ticket`` checks the signature, the closed shape and the canonical bytes,
nothing else. Key id, scope, requester and digest are the tool's checks, and no
connector reapplies the preparation deadline (ADR 0271 decision 7), so the
corpus's ``preparation_expired``, ``key_mismatch`` and ``arguments_mismatch``
tickets all read. A connector reads the ticket the arguments carry, as the API
does, so the corpus's missing, null and integer carried tickets refuse too.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import human_restricted_connector as hrc
import pytest
from curie_internal import approver_ticket
from identity_target import REPO
from nacl.exceptions import CryptoError
from nacl.signing import SigningKey, VerifyKey

VECTORS = REPO / "tests" / "vectors"
CORPUS = json.loads((VECTORS / "approver-ticket.json").read_text("utf-8"))
CANONICAL = json.loads((VECTORS / "action-canonical-arguments.json").read_text("utf-8"))
PUBLIC_KEY = base64.b64decode(CORPUS["public_key"])
FIXTURE_KEY = SigningKey(hashlib.sha256(CORPUS["fixture_seed_label"].encode("utf-8")).digest())
ROTATED_KEY = SigningKey(
    hashlib.sha256(CORPUS["rotation_fixture_seed_label"].encode("utf-8")).digest()
)
CODES = {"invalid_ticket", "preparation_expired", "key_mismatch", "arguments_mismatch"}
TICKET_SECTIONS = ("positive", "negative", "untrusted_key_id", "capacity_cases")

# RFC 8032 section 5.1: the field prime and the base point's prime order.
P = 2**255 - 19
L = 2**252 + 27742317777372353535851937790883648493
# The neutral point (x = 0, y = 1): its canonical spelling and two others that a
# lenient decoder reads as the same point. RFC 8032 decoding refuses both: y must
# be below p, and x = 0 with the sign bit set fails.
NEUTRAL = (1).to_bytes(32, "little")
NEUTRAL_PAST_THE_PRIME = (P + 1).to_bytes(32, "little")
NEUTRAL_NEGATIVE_ZERO = (1 | 1 << 255).to_bytes(32, "little")


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def name(case: dict[str, Any]) -> str:
    return case["name"]


def carried(case: dict[str, Any]) -> object:
    """What a connector reads: the ticket the arguments carry."""

    return case["arguments"].get("approval_ticket")


def signed_claims(token: str) -> dict[str, Any]:
    return json.loads(unb64(token.split(".")[1]))


def negatives(*codes: str) -> list[dict[str, Any]]:
    return [case for case in CORPUS["negative"] if case["code"] in codes]


def nacl_verify(public_key: bytes, message: bytes, signature: bytes) -> bool:
    try:
        VerifyKey(public_key).verify(message, signature)
    except (CryptoError, ValueError, TypeError):
        return False
    return True


def signed(message: bytes = b"cat1.synthetic-message") -> tuple[bytes, bytes, bytes]:
    return bytes(FIXTURE_KEY.verify_key), message, FIXTURE_KEY.sign(message).signature


# --- the corpus itself ---------------------------------------------------------------


def test_the_corpus_key_is_its_fixture_seed_key() -> None:
    assert bytes(FIXTURE_KEY.verify_key) == PUBLIC_KEY
    assert bytes(ROTATED_KEY.verify_key) != PUBLIC_KEY


def test_the_corpus_refusal_codes_are_the_ones_this_file_sorts() -> None:
    """A new code must be taught here: read by the reader, or refused by it."""

    assert CORPUS["positive"] and CORPUS["capacity_cases"] and CORPUS["untrusted_key_id"]
    assert {case["code"] for case in CORPUS["negative"]} == CODES


# --- read_ticket ---------------------------------------------------------------------


@pytest.mark.parametrize("case", CORPUS["positive"] + CORPUS["capacity_cases"], ids=name)
def test_read_ticket_returns_the_claims_of_every_valid_vector(case: dict[str, Any]) -> None:
    assert hrc.read_ticket(carried(case), PUBLIC_KEY) == case["payload"]


@pytest.mark.parametrize("case", negatives("invalid_ticket"), ids=name)
def test_read_ticket_refuses_every_invalid_ticket_vector(case: dict[str, Any]) -> None:
    assert hrc.read_ticket(carried(case), PUBLIC_KEY) is None


@pytest.mark.parametrize("case", negatives("preparation_expired"), ids=name)
def test_read_ticket_never_checks_the_preparation_window(case: dict[str, Any]) -> None:
    """ADR 0271 decision 7: an approval answered after five minutes still executes."""

    assert hrc.read_ticket(carried(case), PUBLIC_KEY) == signed_claims(case["ticket"])


@pytest.mark.parametrize("case", negatives("key_mismatch", "arguments_mismatch"), ids=name)
def test_read_ticket_leaves_the_key_id_and_the_digest_to_the_tool(case: dict[str, Any]) -> None:
    """The tool refuses these as ticket_scope_mismatch and ticket_arguments_mismatch."""

    assert hrc.read_ticket(carried(case), PUBLIC_KEY) == signed_claims(case["ticket"])


@pytest.mark.parametrize("case", CORPUS["untrusted_key_id"], ids=name)
def test_read_ticket_trusts_the_given_key_never_the_ticket_key_id(case: dict[str, Any]) -> None:
    claims = hrc.read_ticket(case["ticket"], base64.b64decode(case["public_key"]))
    if "code" in case or "verification_code" in case:
        assert claims is None
    else:
        assert claims is not None and claims["key_id"] == case["key_id"]
        assert claims == case.get("payload", claims)


def test_read_ticket_refuses_a_valid_ticket_under_any_other_key() -> None:
    ticket = carried(CORPUS["positive"][0])
    assert hrc.read_ticket(ticket, PUBLIC_KEY) is not None
    rotated = bytes(ROTATED_KEY.verify_key)
    for key in (rotated, b"", PUBLIC_KEY[:31], PUBLIC_KEY + b"\0", PUBLIC_KEY * 2):
        assert hrc.read_ticket(ticket, key) is None


def test_read_ticket_refuses_a_valid_ticket_whose_s_is_past_the_group_order() -> None:
    ticket = carried(CORPUS["positive"][0])
    assert isinstance(ticket, str)
    prefix, payload, signature = ticket.split(".")
    raw = unb64(signature)
    high = raw[:32] + (int.from_bytes(raw[32:], "little") + L).to_bytes(32, "little")
    assert hrc.read_ticket(f"{prefix}.{payload}.{b64url(high)}", PUBLIC_KEY) is None


def test_read_ticket_never_raises_on_a_value_that_is_not_a_ticket_string() -> None:
    ticket = carried(CORPUS["positive"][0])
    assert isinstance(ticket, str)
    for value in (None, 42, True, 1.5, "", ticket.encode("ascii"), [ticket], {"t": ticket}):
        assert hrc.read_ticket(value, PUBLIC_KEY) is None


# --- execution_arguments_sha256 ------------------------------------------------------


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param(case["arguments"], id=f"{section}-{case['name']}")
        for section in TICKET_SECTIONS
        for case in CORPUS[section]
    ],
)
def test_the_argument_digest_matches_the_product_on_every_ticket_vector(
    arguments: dict[str, Any],
) -> None:
    expected = approver_ticket.execution_arguments_sha256(arguments)
    assert hrc.execution_arguments_sha256(arguments) == expected


@pytest.mark.parametrize("case", CORPUS["positive"], ids=name)
def test_the_argument_digest_is_the_one_each_valid_ticket_signs(case: dict[str, Any]) -> None:
    assert hrc.execution_arguments_sha256(case["arguments"]) == case["payload"]["arguments_sha256"]


def _canonical_objects() -> list[Any]:
    """Every JSON object in the canonical-arguments vector, with its frozen digest if any."""

    found = [
        pytest.param(case["arguments"], case["sha256"], id=f"vectors-{case['name']}")
        for case in CANONICAL["vectors"]
    ]
    found += [
        pytest.param(
            {"target": case["target"], "prior_state": case["prior_state"]},
            case["sha256"],
            id=f"restore-{case['name']}",
        )
        for case in CANONICAL["restore"]
    ]
    for case in CANONICAL["non_canonical_texts"]:
        parsed = json.loads(case["text"])
        if isinstance(parsed, dict):
            found.append(pytest.param(parsed, None, id=f"non_canonical-{case['name']}"))
    return found


@pytest.mark.parametrize(("arguments", "frozen"), _canonical_objects())
def test_the_argument_digest_matches_the_product_on_the_canonical_arguments_vector(
    arguments: dict[str, Any], frozen: str | None
) -> None:
    digest = hrc.execution_arguments_sha256(arguments)
    assert digest == approver_ticket.execution_arguments_sha256(arguments)
    if frozen is not None and "approval_ticket" not in arguments:
        assert digest == frozen


def test_the_argument_digest_omits_only_the_top_level_ticket() -> None:
    plain = {"note": "synthetic", "nested": {"approval_ticket": "execution data"}}
    digest = hrc.execution_arguments_sha256(plain)
    assert digest == approver_ticket.execution_arguments_sha256(plain)
    assert hrc.execution_arguments_sha256({**plain, "approval_ticket": "cat1.any"}) == digest
    assert hrc.execution_arguments_sha256({**plain, "nested": {}}) != digest


# --- ed25519_verify ------------------------------------------------------------------


def _corpus_signatures() -> list[Any]:
    """Each distinct (key, message, signature) a corpus ticket carries in three segments."""

    seen: dict[tuple[bytes, bytes, bytes], str] = {}
    for section in TICKET_SECTIONS:
        for case in CORPUS[section]:
            try:
                prefix, payload, signature = case["ticket"].split(".")
                message, raw = (prefix + "." + payload).encode("ascii"), unb64(signature)
            except (AttributeError, KeyError, ValueError):
                continue
            key = base64.b64decode(case.get("public_key", CORPUS["public_key"]))
            seen.setdefault((key, message, raw), f"{section}-{case['name']}")
    return [pytest.param(*triple, id=case_id) for triple, case_id in seen.items()]


@pytest.mark.parametrize(("public_key", "message", "signature"), _corpus_signatures())
def test_ed25519_verify_agrees_with_pynacl_on_every_corpus_signature(
    public_key: bytes, message: bytes, signature: bytes
) -> None:
    expected = nacl_verify(public_key, message, signature)
    assert hrc.ed25519_verify(public_key, message, signature) is expected


@pytest.mark.parametrize("n", range(4))
def test_ed25519_verify_accepts_fresh_signatures(n: int) -> None:
    key = SigningKey(hashlib.sha256(f"identity-e2e synthetic ed25519 seed {n}".encode()).digest())
    public = bytes(key.verify_key)
    for message in (b"", b"x", b"cat1." + b"A" * 4096):
        signature = key.sign(message).signature
        assert nacl_verify(public, message, signature)
        assert hrc.ed25519_verify(public, message, signature) is True


@pytest.mark.parametrize("index", [0, 31, 32, 63])
def test_ed25519_verify_refuses_a_tampered_signature(index: int) -> None:
    public, message, signature = signed()
    assert hrc.ed25519_verify(public, message, signature) is True
    tampered = bytearray(signature)
    tampered[index] ^= 0x01
    assert not nacl_verify(public, message, bytes(tampered))
    assert hrc.ed25519_verify(public, message, bytes(tampered)) is False


def test_ed25519_verify_refuses_a_tampered_message() -> None:
    public, message, signature = signed()
    assert hrc.ed25519_verify(public, message, signature) is True
    for changed in (message.upper(), message + b".", message[:-1], b""):
        assert not nacl_verify(public, changed, signature)
        assert hrc.ed25519_verify(public, changed, signature) is False


@pytest.mark.parametrize("multiple", [1, 2])
def test_ed25519_verify_refuses_a_non_canonical_s(multiple: int) -> None:
    """S + kL names the same scalar, so only an S < L check refuses it."""

    public, message, signature = signed()
    s = int.from_bytes(signature[32:], "little") + multiple * L
    high = signature[:32] + s.to_bytes(32, "little")
    assert hrc.ed25519_verify(public, message, signature) is True
    assert not nacl_verify(public, message, high)
    assert hrc.ed25519_verify(public, message, high) is False


def test_ed25519_verify_refuses_wrong_lengths_without_raising() -> None:
    public, message, signature = signed()
    for key in (b"", public[:31], public + b"\0"):
        assert not nacl_verify(key, message, signature)
        assert hrc.ed25519_verify(key, message, signature) is False
    for wrong in (b"", signature[:63], signature + b"\0"):
        assert not nacl_verify(public, message, wrong)
        assert hrc.ed25519_verify(public, message, wrong) is False


def _neutral_r(r: bytes, message: bytes) -> tuple[bytes, bytes, bytes]:
    """A fixture-key signature whose R is ``r``, a spelling of the neutral point.

    With R neutral, [S]B = R + [k]A holds for S = k * a mod L, so a verifier
    that decodes R leniently accepts it.
    """

    digest = hashlib.sha512(bytes(FIXTURE_KEY)).digest()
    a = int.from_bytes(digest[:32], "little") & ((1 << 254) - 8) | (1 << 254)
    public = bytes(FIXTURE_KEY.verify_key)
    k = int.from_bytes(hashlib.sha512(r + public + message).digest(), "little") % L
    return public, message, r + (k * a % L).to_bytes(32, "little")


# With A neutral, R neutral and S = 0 hold for every message under a lenient decoder.
NON_CANONICAL_POINTS: dict[str, tuple[bytes, bytes, bytes]] = {
    "a key spelled past the field prime": (NEUTRAL_PAST_THE_PRIME, b"any", NEUTRAL + bytes(32)),
    "a key spelled as negative zero": (NEUTRAL_NEGATIVE_ZERO, b"any", NEUTRAL + bytes(32)),
    "an R spelled past the field prime": _neutral_r(NEUTRAL_PAST_THE_PRIME, b"cat1.synthetic"),
    "an R spelled as negative zero": _neutral_r(NEUTRAL_NEGATIVE_ZERO, b"cat1.synthetic"),
}


@pytest.mark.parametrize(
    ("public_key", "message", "signature"),
    NON_CANONICAL_POINTS.values(),
    ids=list(NON_CANONICAL_POINTS),
)
def test_ed25519_verify_refuses_a_non_canonical_point_encoding(
    public_key: bytes, message: bytes, signature: bytes
) -> None:
    assert int.from_bytes(NEUTRAL_PAST_THE_PRIME, "little") >= P
    assert not nacl_verify(public_key, message, signature)
    assert hrc.ed25519_verify(public_key, message, signature) is False
