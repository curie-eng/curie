"""Wire contract for the shared scoped sandbox token (#410, #3833).

The worker mints and the API verifies one shared HMAC signed capability.
An independent reference implementation pins its encoding and malformed claim
refusals without relying on a round trip through the same implementation.
"""

import base64
import hashlib
import hmac
import json

from curie_internal.sandbox_token import mint, verify

KEY = "curie-dev-key"
AGENT = "00000000-0000-0000-0000-000000000001"
EXP = 1893456000  # 2030-01-01, the golden known-answer exp
FAR_FUTURE = 4102444800  # 2100-01-01, comfortably valid at test time
PAST = 1000000000  # 2001, comfortably expired at test time


def _sign(api_key: str, payload_obj: object) -> str:
    """Independent reimplementation of the signing wire format from scratch.

    Deliberately does NOT call the module under test: it rebuilds the exact
    payload-segment + HMAC-SHA256 signature encoding so a test can assert the
    module produces this byte sequence, and can forge validly-signed tokens with
    arbitrary (even malformed) payloads to probe verify's claim checks.
    """
    payload_json = json.dumps(
        payload_obj, separators=(",", ":"), sort_keys=True
    ).encode()
    payload_seg = base64.urlsafe_b64encode(payload_json).rstrip(b"=").decode()
    signing_input = f"sbx.{payload_seg}"
    sig = hmac.new(api_key.encode(), signing_input.encode(), hashlib.sha256).digest()
    sig_seg = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    return f"sbx.{payload_seg}.{sig_seg}"


def _reference_token(
    api_key: str,
    agent: str,
    scope: str,
    exp: int,
    claims: dict[str, str | None] | None = None,
) -> str:
    payload: dict[str, object] = {"agent": agent, "scope": scope, "exp": exp}
    if claims is not None:
        payload.update(claims)
    return _sign(api_key, payload)


def _decode():  # noqa: ANN202 - resolved lazily so the older tests still collect
    from curie_internal.sandbox_token import decode

    return decode


def test_roundtrip_true_for_matching_claims_and_future_exp() -> None:
    token = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE)
    assert verify(token, KEY, agent=AGENT, scope="state") is True


def test_roundtrip_false_when_exp_is_in_the_past() -> None:
    token = mint(KEY, agent=AGENT, scope="state", exp=PAST)
    assert verify(token, KEY, agent=AGENT, scope="state") is False


def test_three_claim_token_wire_format_unchanged() -> None:
    # The independent known answer pins the deterministic encoding. The caller
    # supplies the absolute expiration rather than relying on a clock.
    token = mint(KEY, agent=AGENT, scope="state", exp=EXP)
    assert token == _reference_token(KEY, AGENT, "state", EXP)
    assert token.startswith("sbx.")
    assert len(token.split(".")) == 3


LONG_LIVED_CLAIMS: dict[str, str | None] = {
    "binding": "slack:T1/C1",
    "memory": "read",
}
PER_TURN_CLAIMS: dict[str, str | None] = {
    "binding": "slack:T1/C1",
    "memory": "write",
    "sender": "U123",
    "turn": "evt-0001",
}


def test_mint_with_claims_matches_independent_reference_wire_format() -> None:
    # Known answers for the two ADR-0188 credential shapes: the long-lived
    # read credential and the per-turn write credential. claims=None is
    # today's three-claim token, byte for byte.
    plain = mint(KEY, agent=AGENT, scope="state", exp=EXP, claims=None)
    assert plain == _reference_token(KEY, AGENT, "state", EXP)
    long_lived = mint(KEY, agent=AGENT, scope="state", exp=EXP, claims=LONG_LIVED_CLAIMS)
    assert long_lived == _reference_token(KEY, AGENT, "state", EXP, LONG_LIVED_CLAIMS)
    per_turn = mint(KEY, agent=AGENT, scope="state", exp=EXP, claims=PER_TURN_CLAIMS)
    assert per_turn == _reference_token(KEY, AGENT, "state", EXP, PER_TURN_CLAIMS)
    # A null binding is carried as JSON null, not dropped.
    unbound: dict[str, str | None] = {"binding": None, "memory": "read"}
    token = mint(KEY, agent=AGENT, scope="state", exp=EXP, claims=unbound)
    assert token == _reference_token(KEY, AGENT, "state", EXP, unbound)
    payload_seg = token.split(".")[1]
    raw = base64.urlsafe_b64decode(payload_seg + "=" * (-len(payload_seg) % 4))
    assert json.loads(raw) == {
        "agent": AGENT,
        "scope": "state",
        "exp": EXP,
        "binding": None,
        "memory": "read",
    }
    # Claimed tokens still verify as plain scope="state" tokens, so an old api
    # (which ignores unknown claims) accepts them.
    far = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE, claims=PER_TURN_CLAIMS)
    assert verify(far, KEY, agent=AGENT, scope="state") is True


def test_decode_returns_verified_claims() -> None:
    decode = _decode()
    token = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE, claims=PER_TURN_CLAIMS)
    assert decode(token, KEY, agent=AGENT, scope="state") == {
        "agent": AGENT,
        "scope": "state",
        "exp": FAR_FUTURE,
        **PER_TURN_CLAIMS,
    }
    plain = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE)
    assert decode(plain, KEY, agent=AGENT, scope="state") == {
        "agent": AGENT,
        "scope": "state",
        "exp": FAR_FUTURE,
    }
    # now= is honoured the same way verify honours it.
    assert decode(plain, KEY, agent=AGENT, scope="state", now=FAR_FUTURE) is None
    assert decode(plain, KEY, agent=AGENT, scope="state", now=FAR_FUTURE - 1) is not None


def test_decode_rejection_matrix_matches_verify() -> None:
    decode = _decode()
    other_agent = "99999999-9999-9999-9999-999999999999"
    valid = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE, claims=PER_TURN_CLAIMS)
    parts = valid.split(".")
    sig_seg = parts[2]
    flipped_char = "A" if sig_seg[-1] != "A" else "B"
    expired = mint(KEY, agent=AGENT, scope="state", exp=PAST, claims=PER_TURN_CLAIMS)
    broad = mint(KEY, agent=AGENT, scope="state-admin", exp=FAR_FUTURE)
    wrong_key = mint("some-other-key", agent=AGENT, scope="state", exp=FAR_FUTURE)
    cases: list[tuple[str, str, str]] = [
        (valid, AGENT, "state"),
        (valid, other_agent, "state"),
        (valid, AGENT, "state-admin"),
        (expired, AGENT, "state"),
        (broad, AGENT, "state"),
        (f"{parts[0]}.{parts[1]}.{sig_seg[:-1]}{flipped_char}", AGENT, "state"),
        (wrong_key, AGENT, "state"),
        (_sign(KEY, {"agent": AGENT, "scope": "state"}), AGENT, "state"),
        (_sign(KEY, {"scope": "state", "exp": FAR_FUTURE}), AGENT, "state"),
        (_sign(KEY, {"agent": AGENT, "exp": FAR_FUTURE}), AGENT, "state"),
        (_sign(KEY, {"agent": AGENT, "scope": "state", "exp": True}), AGENT, "state"),
        (_sign(KEY, [AGENT, "state", FAR_FUTURE]), AGENT, "state"),
        (_sign(KEY, 42), AGENT, "state"),
        ("", AGENT, "state"),
        ("sbx.notbase64!.x", AGENT, "state"),
        ("sbx.onlytwo", AGENT, "state"),
        ("sbx.a.b.c", AGENT, "state"),
        ("nope.abc.def", AGENT, "state"),
    ]
    for token, agent, scope in cases:
        ok = verify(token, KEY, agent=agent, scope=scope)
        decoded = decode(token, KEY, agent=agent, scope=scope)
        assert (decoded is not None) is ok, (token, agent, scope)
    assert decode(valid, KEY, agent=AGENT, scope="state") is not None


def test_mint_refuses_overriding_reserved_claims() -> None:
    for reserved in ("agent", "scope", "exp"):
        try:
            mint(KEY, agent=AGENT, scope="state", exp=EXP, claims={reserved: "x"})
        except ValueError:
            continue
        raise AssertionError(f"mint accepted a claims override of {reserved!r}")


def test_verify_rejection_matrix_returns_false_and_never_raises() -> None:
    other_agent = "99999999-9999-9999-9999-999999999999"
    valid = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE)
    parts = valid.split(".")

    # Expired exp.
    expired = mint(KEY, agent=AGENT, scope="state", exp=PAST)
    assert verify(expired, KEY, agent=AGENT, scope="state") is False

    # Wrong agent claim: a token minted for AGENT does not verify for another.
    assert verify(valid, KEY, agent=other_agent, scope="state") is False

    # Scope that merely CONTAINS "state" must NOT satisfy an exact "state"
    # requirement, and the reverse (both directions catch substring matching).
    broad = mint(KEY, agent=AGENT, scope="state-admin", exp=FAR_FUTURE)
    assert verify(broad, KEY, agent=AGENT, scope="state") is False
    notstate = mint(KEY, agent=AGENT, scope="notstate", exp=FAR_FUTURE)
    assert verify(notstate, KEY, agent=AGENT, scope="state") is False
    assert verify(valid, KEY, agent=AGENT, scope="state-admin") is False

    # Tampered signature: flip a char in the sig segment.
    sig_seg = parts[2]
    flipped_char = "A" if sig_seg[-1] != "A" else "B"
    tampered_sig = f"{parts[0]}.{parts[1]}.{sig_seg[:-1]}{flipped_char}"
    assert verify(tampered_sig, KEY, agent=AGENT, scope="state") is False

    # Tampered payload: swap in a different payload but keep the old signature.
    forged_payload = json.dumps(
        {"agent": AGENT, "scope": "state", "exp": FAR_FUTURE + 1},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    forged_seg = base64.urlsafe_b64encode(forged_payload).rstrip(b"=").decode()
    tampered_payload = f"{parts[0]}.{forged_seg}.{parts[2]}"
    assert verify(tampered_payload, KEY, agent=AGENT, scope="state") is False

    # Token signed with a DIFFERENT api_key.
    wrong_key_token = mint("some-other-key", agent=AGENT, scope="state", exp=FAR_FUTURE)
    assert verify(wrong_key_token, KEY, agent=AGENT, scope="state") is False

    # Missing "exp": must fail closed, never be treated as never-expiring.
    missing_exp = _sign(KEY, {"agent": AGENT, "scope": "state"})
    assert verify(missing_exp, KEY, agent=AGENT, scope="state") is False

    # Missing "agent".
    missing_agent = _sign(KEY, {"scope": "state", "exp": FAR_FUTURE})
    assert verify(missing_agent, KEY, agent=AGENT, scope="state") is False

    # Missing "scope".
    missing_scope = _sign(KEY, {"agent": AGENT, "exp": FAR_FUTURE})
    assert verify(missing_scope, KEY, agent=AGENT, scope="state") is False

    # Non-dict JSON payload (a signed array, and a signed number).
    array_payload = _sign(KEY, [AGENT, "state", FAR_FUTURE])
    assert verify(array_payload, KEY, agent=AGENT, scope="state") is False
    number_payload = _sign(KEY, 42)
    assert verify(number_payload, KEY, agent=AGENT, scope="state") is False

    # Garbage / malformed strings: none may raise, all return False.
    malformed = [
        "",
        "sbx.notbase64!.x",
        "sbx.onlytwo",
        "sbx.a.b.c",
        "nope.abc.def",
    ]
    for token in malformed:
        assert verify(token, KEY, agent=AGENT, scope="state") is False
