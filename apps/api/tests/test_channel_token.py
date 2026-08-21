"""Wire-format contract for the scoped channel ingress token (ADR-0096 phase 2).

T-C1. The `chn` token is the credential an ingress adapter presents at
`POST /channels/turns`. It is a SIBLING of `sandbox_token`, never a widening of
it (plan D6): `sandbox_token.mint` binds an `agent` claim, and an adapter is
bound to a BINDING, not to an agent. Spike S3's rule -- "if the claim set must
grow, escalate rather than widen the primitive" -- is why this module exists at
all, so a test here also pins that `sandbox_token`'s own claim set did NOT grow.

The claims are `{channel_id, generation, scope, exp}` (plan D5, EB-C1):
deliberately NOT `(kind, address)`. `crud.update_channel_binding` mutates the
binding row in place and `delete_agent` frees the pair for reuse, so a token
claiming the pair goes live again against a NEW owner (E12). The row id plus a
rebind counter is what makes a rebind observable to a credential minted before
it -- and it is what makes T-C8 and T-C9 fail under the rejected design.

Pure stdlib, nothing mocked, no database. An independent reimplementation of the
signing format pins the exact encoding rather than round-tripping the module
against itself.
"""

import base64
import hashlib
import hmac
import inspect
import json
from typing import Any

import pytest
from curie_api import sandbox_token
from curie_api.channel_token import CHANNEL_ENQUEUE_SCOPE, mint, verify

KEY = "curie-dev-key"
OTHER_KEY = "some-other-platform-key"
# The binding row's id (`agent_channels.id`), which is what the token claims.
CHANNEL_ID = "11111111-1111-1111-1111-111111111111"
OTHER_CHANNEL_ID = "22222222-2222-2222-2222-222222222222"
GENERATION = 3
EXP = 1893456000  # 2030-01-01, the golden known-answer exp
FAR_FUTURE = 4102444800  # 2100-01-01, comfortably valid at test time
PAST = 1000000000  # 2001, comfortably expired at test time


def _sign(api_key: str, payload_obj: object) -> str:
    """Independent reimplementation of the `chn` signing wire format.

    Deliberately does NOT call the module under test: it rebuilds the exact
    payload-segment + HMAC-SHA256 encoding, so a test can assert the module
    produces this byte sequence and can forge validly-signed tokens carrying
    arbitrary (even malformed) payloads to probe `verify`'s claim checks.
    """

    payload_json = json.dumps(
        payload_obj, separators=(",", ":"), sort_keys=True
    ).encode()
    payload_seg = base64.urlsafe_b64encode(payload_json).rstrip(b"=").decode()
    signing_input = f"chn.{payload_seg}"
    sig = hmac.new(api_key.encode(), signing_input.encode(), hashlib.sha256).digest()
    sig_seg = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    return f"chn.{payload_seg}.{sig_seg}"


def _reference_token(
    api_key: str, channel_id: str, generation: int, scope: str, exp: int
) -> str:
    return _sign(
        api_key,
        {
            "channel_id": channel_id,
            "generation": generation,
            "scope": scope,
            "exp": exp,
        },
    )


def _valid(**overrides: Any) -> str:
    fields: dict[str, Any] = {
        "channel_id": CHANNEL_ID,
        "generation": GENERATION,
        "scope": CHANNEL_ENQUEUE_SCOPE,
        "exp": FAR_FUTURE,
    }
    fields.update(overrides)
    return mint(KEY, **fields)


def _verify(token: Any, **overrides: Any) -> bool:
    """Verify against the default claim set, with per-case overrides.

    The verifying key is always ``KEY``; the wrong-key cases call ``verify``
    directly so the deviation is visible at the call site.
    """

    fields: dict[str, Any] = {
        "channel_id": CHANNEL_ID,
        "generation": GENERATION,
        "scope": CHANNEL_ENQUEUE_SCOPE,
    }
    fields.update(overrides)
    return verify(token, KEY, **fields)


def test_the_enqueue_scope_string_is_the_frozen_contract() -> None:
    # The scope literal is mirrored at the adapter's mint site and at the
    # ingress dependency; a byte-identical string on both sides IS the contract,
    # exactly as `state`/`state.app` are for the sandbox token.
    assert CHANNEL_ENQUEUE_SCOPE == "channel.enqueue"


def test_roundtrip_true_for_matching_claims_and_future_exp() -> None:
    assert _verify(_valid()) is True


def test_mint_matches_an_independent_reference_wire_format() -> None:
    # Golden known-answer: pins the deterministic encoding AND the exact claim
    # SET. A mint that quietly added `kind`/`address` claims (the rejected D5
    # alternative) produces a different payload segment and fails here.
    token = mint(
        KEY,
        channel_id=CHANNEL_ID,
        generation=GENERATION,
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=EXP,
    )
    assert token == _reference_token(
        KEY, CHANNEL_ID, GENERATION, CHANNEL_ENQUEUE_SCOPE, EXP
    )
    assert token.startswith("chn.")
    assert len(token.split(".")) == 3


def test_the_claims_are_channel_id_and_generation_never_kind_and_address() -> None:
    """Plan D5. The decision-proving assertion for T-C8/T-C9's mechanism.

    Two bindings can hold the same `(kind, address)` pair over time -- delete and
    recreate, or move away and back -- so a token claiming the pair survives a
    change of owner. Decoding the payload here states the claim set as a fact of
    the wire, so a later "just add kind so verify can double-check" change fails
    a test that names why it is wrong.
    """

    payload_seg = _valid().split(".")[1]
    pad = "=" * (-len(payload_seg) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload_seg + pad))
    assert set(claims) == {"channel_id", "generation", "scope", "exp"}
    assert claims["channel_id"] == CHANNEL_ID
    assert claims["generation"] == GENERATION


@pytest.mark.parametrize(
    ("case", "token_kwargs", "verify_kwargs"),
    [
        # Expired: the whole point of `exp`.
        ("expired", {"exp": PAST}, {}),
        # A token minted for one binding never verifies for another -- the
        # containment T-C3 exercises over HTTP.
        ("wrong channel_id", {}, {"channel_id": OTHER_CHANNEL_ID}),
        # STALE GENERATION: minted before a rebind, presented after one.
        ("stale generation", {"generation": GENERATION}, {"generation": GENERATION + 1}),
        # A generation that ran BACKWARDS is equally refused: the comparison is
        # equality, not "at least", so a rolled-back row cannot revive a token.
        ("future generation", {"generation": GENERATION + 1}, {}),
        # Wrong scope: an enqueue token is not a state token.
        ("wrong scope", {"scope": "state"}, {}),
        ("scope not requested", {}, {"scope": "state"}),
    ],
)
def test_verify_rejects_each_claim_mismatch(
    case: str, token_kwargs: dict[str, Any], verify_kwargs: dict[str, Any]
) -> None:
    assert _verify(_valid(**token_kwargs), **verify_kwargs) is False, case


def test_verify_rejects_a_tampered_or_foreign_signature() -> None:
    token = _valid()
    prefix, payload_seg, sig_seg = token.split(".")

    # Flipped signature byte.
    flipped = sig_seg[:-1] + ("A" if sig_seg[-1] != "A" else "B")
    assert _verify(f"{prefix}.{payload_seg}.{flipped}") is False

    # Signed by a DIFFERENT key: the signature is well-formed, just not ours.
    foreign = _sign(
        OTHER_KEY,
        {
            "channel_id": CHANNEL_ID,
            "generation": GENERATION,
            "scope": CHANNEL_ENQUEUE_SCOPE,
            "exp": FAR_FUTURE,
        },
    )
    assert _verify(foreign) is False

    # Right token, wrong verifying key.
    assert verify(
        token,
        OTHER_KEY,
        channel_id=CHANNEL_ID,
        generation=GENERATION,
        scope=CHANNEL_ENQUEUE_SCOPE,
    ) is False

    # Claims rewritten under a signature that no longer covers them.
    escalated = _sign(
        OTHER_KEY,
        {
            "channel_id": OTHER_CHANNEL_ID,
            "generation": GENERATION,
            "scope": CHANNEL_ENQUEUE_SCOPE,
            "exp": FAR_FUTURE,
        },
    ).split(".")[1]
    assert _verify(f"{prefix}.{escalated}.{sig_seg}") is False


@pytest.mark.parametrize(
    "malformed",
    [
        "",
        "chn",
        "chn.",
        "chn.abc",
        "a.b.c",
        "chn.!!!.???",
        "chn.abc.def.ghi",
        None,
        12345,
        b"chn.abc.def",
    ],
)
def test_verify_returns_false_and_never_raises_on_malformed_input(
    malformed: Any,
) -> None:
    # The contract `sandbox_token.verify` documents and the ingress dependency
    # relies on: a failure is a plain False, so the caller answers 401 rather
    # than 500. A crash here is an unauthenticated remote DoS on the API.
    assert _verify(malformed) is False


def test_verify_rejects_a_validly_signed_token_with_a_broken_payload() -> None:
    # Every one of these is signed with the REAL key: only `verify`'s own claim
    # decoding stands between them and an accepted request.
    SCOPE = CHANNEL_ENQUEUE_SCOPE
    for payload in (
        {"channel_id": CHANNEL_ID, "generation": GENERATION, "scope": SCOPE},
        {"channel_id": CHANNEL_ID, "generation": GENERATION, "scope": SCOPE, "exp": "9"},
        {"channel_id": CHANNEL_ID, "generation": GENERATION, "scope": SCOPE, "exp": True},
        {"channel_id": CHANNEL_ID, "scope": SCOPE, "exp": FAR_FUTURE},
        {"channel_id": None, "generation": GENERATION, "scope": SCOPE, "exp": FAR_FUTURE},
        ["not", "a", "mapping"],
        "not a mapping either",
    ):
        assert _verify(_sign(KEY, payload)) is False, payload


def test_exp_is_strictly_in_the_future() -> None:
    token = _valid(exp=EXP)
    assert _verify(token, now=EXP - 1) is True
    # Equal is expired, not valid: the boundary an off-by-one makes wrong for a
    # full clock tick.
    assert _verify(token, now=EXP) is False
    assert _verify(token, now=EXP + 1) is False


def test_a_channel_token_is_not_a_platform_key_and_not_a_sandbox_token() -> None:
    """Cross-credential containment (plan D6, §6.2's protected set).

    Three separate confusions, each of which would hand a compromised adapter a
    credential it was never granted: presenting a `chn` token where the platform
    key is expected, presenting it where a sandbox token is expected, and the
    reverse.
    """

    channel = _valid()
    assert channel != KEY
    assert not hmac.compare_digest(channel, KEY)

    agent = "00000000-0000-0000-0000-000000000001"
    sandbox = sandbox_token.mint(KEY, agent=agent, scope="state", exp=FAR_FUTURE)

    # A channel token does not authenticate as a sandbox token ...
    assert sandbox_token.verify(channel, KEY, agent=agent, scope="state") is False
    assert sandbox_token.verify(channel, KEY, agent=CHANNEL_ID, scope="state") is False
    # ... and a sandbox token does not authenticate as a channel token, even
    # when its `agent` claim happens to equal the channel id.
    assert _verify(sandbox) is False
    assert (
        verify(
            sandbox_token.mint(
                KEY, agent=CHANNEL_ID, scope=CHANNEL_ENQUEUE_SCOPE, exp=FAR_FUTURE
            ),
            KEY,
            channel_id=CHANNEL_ID,
            generation=GENERATION,
            scope=CHANNEL_ENQUEUE_SCOPE,
        )
        is False
    )


def test_sandbox_token_was_not_widened_to_carry_a_binding() -> None:
    """Plan D6, the rule that made this module exist rather than an extra claim.

    The cheap way to ship ingress auth is an optional `channel_id=None` on
    `sandbox_token.mint`; that is exactly the widening spike S3 forbade, and it
    would break the byte-identical api/worker twin test in a way that is easy to
    "fix" by copying the drift across. Asserting the signature keeps the
    decision, not just its current consequence.
    """

    assert list(inspect.signature(sandbox_token.mint).parameters) == [
        "api_key",
        "agent",
        "scope",
        "exp",
    ]
    assert list(inspect.signature(sandbox_token.verify).parameters) == [
        "token",
        "api_key",
        "agent",
        "scope",
        "now",
    ]
