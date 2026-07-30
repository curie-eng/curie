"""Wire-format contract for the scoped sandbox token (#410, api copy).

The token is a minimal HMAC-signed capability minted by the worker and verified
by the api, so a sandboxed agent gets a token that authorizes ONLY its own state
namespace and nothing else. The module lives byte-identical in both apps; the
token-module test bodies below are identical to
apps/worker/tests/binding/test_sandbox_token.py except the import line.

Pure stdlib, nothing mocked. An independent reference reimplementation of the
wire format pins the exact encoding so the two copies cannot silently drift into
a shape that still round-trips against itself but not against the other app.
"""

import base64
import hashlib
import hmac
import json
from pathlib import Path

from curie_api.sandbox_token import mint, verify

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
    payload_json = json.dumps(payload_obj, separators=(",", ":"), sort_keys=True).encode()
    payload_seg = base64.urlsafe_b64encode(payload_json).rstrip(b"=").decode()
    signing_input = f"sbx.{payload_seg}"
    sig = hmac.new(api_key.encode(), signing_input.encode(), hashlib.sha256).digest()
    sig_seg = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    return f"sbx.{payload_seg}.{sig_seg}"


def _reference_token(api_key: str, agent: str, scope: str, exp: int) -> str:
    return _sign(api_key, {"agent": agent, "scope": scope, "exp": exp})


def test_roundtrip_true_for_matching_claims_and_future_exp() -> None:
    token = mint(KEY, agent=AGENT, scope="state", exp=FAR_FUTURE)
    assert verify(token, KEY, agent=AGENT, scope="state") is True


def test_roundtrip_false_when_exp_is_in_the_past() -> None:
    token = mint(KEY, agent=AGENT, scope="state", exp=PAST)
    assert verify(token, KEY, agent=AGENT, scope="state") is False


def test_mint_matches_independent_reference_wire_format() -> None:
    # Golden known-answer: pins the exact deterministic encoding, so the two
    # byte-identical copies cannot drift into a self-consistent-but-incompatible
    # shape. mint takes no clock; the caller passes the absolute exp.
    token = mint(KEY, agent=AGENT, scope="state", exp=EXP)
    assert token == _reference_token(KEY, AGENT, "state", EXP)
    assert token.startswith("sbx.")
    assert len(token.split(".")) == 3


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


def test_the_two_module_copies_are_byte_identical() -> None:
    # Anti-drift gate: the worker mints and the api verifies, so the two copies
    # MUST agree on the wire format to the byte. Resolve both paths relative to
    # this test file (parents[3] is the worktree root) rather than hardcoding.
    root = Path(__file__).resolve().parents[3]
    api_src = root / "apps" / "api" / "src" / "curie_api" / "sandbox_token.py"
    worker_src = root / "apps" / "worker" / "src" / "curie_worker" / "sandbox_token.py"
    assert api_src.read_bytes() == worker_src.read_bytes()
