"""The caller proxy's token check (ADR-0168 decision 7, ADR 0270 decision 2).

It reads the same frozen corpus the worker's minter is pinned to
(tests/vectors/connector-caller-token.json), so the minter and the verifier
cannot drift apart. The agent-only refusal cases are derived from the accept
vectors here rather than frozen. The principal refusals are frozen in the
corpus (`malformed_principals`, `tampered_principals`), because a duplicate
key can only be written down as the exact text that was signed.
"""

from __future__ import annotations

import base64
import binascii
import json
import random
import string
import tomllib
import unicodedata
from pathlib import Path
from typing import Any

import pytest
from curie_connector_proxy import caller
from nacl.signing import SigningKey, VerifyKey

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
_CORPUS = json.loads(_VECTOR.read_text(encoding="utf-8"))
_VECTORS: list[dict[str, Any]] = _CORPUS["vectors"]
_BY_NAME = {str(v["name"]): v for v in _VECTORS}
_MALFORMED: list[dict[str, Any]] = _CORPUS["malformed_principals"]
_TAMPERED: list[dict[str, Any]] = _CORPUS["tampered_principals"]
_PRINCIPAL_VECTORS = [v for v in _VECTORS if "principal" in v]
_OTHER_SEED = {
    "a_plain_agent_name": "the_same_claims_under_another_seed",
    "the_same_claims_under_another_seed": "a_plain_agent_name",
    "a_stored_name_outside_the_bundle_shape_is_carried_verbatim": (
        "the_same_claims_under_another_seed"
    ),
    "a_work_item_run": "the_same_claims_under_another_seed",
    "the_same_work_item_under_another_run": "the_same_claims_under_another_seed",
    "a_human_requester": "the_same_claims_under_another_seed",
    "the_same_requester_on_a_work_item_run": "the_same_claims_under_another_seed",
    "another_subject_under_the_same_issuer": "the_same_claims_under_another_seed",
    "a_non_ascii_subject_is_carried_escaped": "the_same_claims_under_another_seed",
    "a_principal_at_its_field_bounds": "the_same_claims_under_another_seed",
    "a_control_character_is_carried_escaped": "the_same_claims_under_another_seed",
}
_MALFORMED_BY_NAME = {str(e["name"]): e for e in _MALFORMED}
# Every malformed and tampered principal is signed with the first vector's seed.
_SIGNER = _VECTORS[0]


def _key(vector: dict[str, object]) -> list[VerifyKey]:
    return [caller.public_key(str(vector["public"]))]


def _exp(vector: dict[str, object]) -> int:
    return int(str(vector["exp"]))


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decide(
    vector: dict[str, object], token: str, *, admits: set[str], now: int
) -> caller.Decision:
    return caller.decide(_key(vector), token, admits=frozenset(admits), now=now)


def _principal(vector: dict[str, Any]) -> caller.Principal | None:
    fields = vector.get("principal")
    if fields is None:
        return None
    return caller.Principal(
        tenant_id=str(fields["tenant_id"]),
        issuer=str(fields["issuer"]),
        subject=str(fields["subject"]),
    )


# @spec ADR-0168 d7
def test_the_prefix_and_header_match_the_minter_and_the_runner() -> None:
    from curie_worker import caller_token

    assert caller.PREFIX == _CORPUS["prefix"] == caller_token.PREFIX
    assert caller.HEADER == "X-Curie-Caller"


# @spec CONNECTOR-REQUESTER-4
def test_the_principal_header_is_the_one_the_corpus_names() -> None:
    assert caller.PRINCIPAL_HEADER == _CORPUS["forged_principal_headers"]["header"]
    assert caller.PRINCIPAL_HEADER == "X-Curie-Principal"


def test_every_vector_has_a_counterpart_under_another_seed() -> None:
    assert set(_OTHER_SEED) == set(_BY_NAME)
    for name, other in _OTHER_SEED.items():
        assert _BY_NAME[name]["public"] != _BY_NAME[other]["public"], name


# @spec ADR-0168 d7
# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_every_frozen_token_is_admitted_before_it_expires(vector: dict[str, object]) -> None:
    decision = _decide(
        vector, str(vector["minted"]), admits={str(vector["agent"])}, now=_exp(vector) - 1
    )
    assert decision == caller.Decision(
        agent=str(vector["agent"]),
        refusal=None,
        run=str(vector["run"]) if "run" in vector else None,
        work_item=str(vector["work_item"]) if "work_item" in vector else None,
        principal=_principal(vector),
    )
    assert decision.admitted
    # An agent-only token names no human, and a principal-bearing one names
    # exactly the signed one.
    assert decision.principal == _principal(vector)
    assert caller.claims(_key(vector), str(vector["minted"])) is not None


# @spec CONNECTOR-REQUESTER-2
def test_the_principal_corpus_is_not_vacuous() -> None:
    assert len(_PRINCIPAL_VECTORS) >= 4
    assert any("run" in v for v in _PRINCIPAL_VECTORS)
    human = _BY_NAME["a_human_requester"]
    other = _BY_NAME["another_subject_under_the_same_issuer"]
    # Two humans who differ only in subject come back as two principals.
    assert _principal(human) != _principal(other)
    assert {k: v for k, v in human["principal"].items() if k != "subject"} == {
        k: v for k, v in other["principal"].items() if k != "subject"
    }
    now = _exp(human) - 1
    first = _decide(human, str(human["minted"]), admits={"acme-dev"}, now=now)
    second = _decide(other, str(other["minted"]), admits={"acme-dev"}, now=now)
    assert first.principal is not None and second.principal is not None
    assert first.principal.subject != second.principal.subject


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("entry", _MALFORMED, ids=lambda e: str(e["name"]))
def test_a_malformed_principal_is_invalid(entry: dict[str, Any]) -> None:
    # Signed by the configured key (the minter's tests check that), so the
    # refusal is the claims', and the same key admits a well-formed principal.
    token = str(entry["minted"])
    decision = _decide(_SIGNER, token, admits={"acme-dev"}, now=_exp(_SIGNER) - 1)
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID), entry["name"]
    assert decision.principal is None
    assert caller.claims(_key(_SIGNER), token) is None


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("entry", _TAMPERED, ids=lambda e: str(e["name"]))
def test_a_tampered_principal_is_invalid(entry: dict[str, Any]) -> None:
    token = str(entry["minted"])
    decision = _decide(_SIGNER, token, admits={"acme-dev"}, now=_exp(_SIGNER) - 1)
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID), entry["name"]
    assert decision.principal is None
    assert caller.claims(_key(_SIGNER), token) is None


# @spec CONNECTOR-REQUESTER-2
def test_the_key_that_refuses_the_malformed_principals_admits_a_well_formed_one() -> None:
    human = _BY_NAME["a_human_requester"]
    assert human["public"] == _SIGNER["public"]
    decision = _decide(_SIGNER, str(human["minted"]), admits={"acme-dev"}, now=_exp(human) - 1)
    assert decision.admitted
    assert decision.principal == _principal(human)


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("offset", [0, 1])
@pytest.mark.parametrize("vector", _PRINCIPAL_VECTORS, ids=lambda v: str(v["name"]))
def test_an_expired_principal_token_names_no_principal(vector: dict[str, Any], offset: int) -> None:
    decision = _decide(
        vector, str(vector["minted"]), admits={"acme-dev"}, now=_exp(vector) + offset
    )
    assert decision == caller.Decision(
        agent=str(vector["agent"]),
        refusal=caller.EXPIRED,
        run=vector.get("run"),
        work_item=vector.get("work_item"),
    )
    assert decision.principal is None


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("vector", _PRINCIPAL_VECTORS, ids=lambda v: str(v["name"]))
def test_a_principal_never_carries_an_agent_past_admission(vector: dict[str, Any]) -> None:
    decision = _decide(vector, str(vector["minted"]), admits={"someone-else"}, now=_exp(vector) - 1)
    assert decision == caller.Decision(
        agent=str(vector["agent"]),
        refusal=caller.NOT_ADMITTED,
        run=vector.get("run"),
        work_item=vector.get("work_item"),
    )
    assert decision.principal is None


def _signed_route(claims: dict[str, object]) -> str:
    seed = base64.b64decode(str(_SIGNER["seed"]))
    payload = json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("ascii")
    head = f"ccr.{_b64url(payload)}"
    signature = SigningKey(seed).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


# @spec CONNECTOR-REQUESTER-2
def test_a_route_token_carrying_a_principal_is_invalid() -> None:
    # ADR 0270 decision 2: ccr keeps its closed claims; human authority on a
    # route would need its own amendment.
    human = _BY_NAME["a_human_requester"]
    base: dict[str, object] = {"agent": "acme-dev", "connector": "reviewer", "exp": _exp(human)}
    keys = _key(_SIGNER)
    now = _exp(human) - 1
    # Liveness: the same claims without the principal are admitted.
    plain = caller.decide_route(
        keys, _signed_route(base), connector="reviewer", admits=frozenset({"acme-dev"}), now=now
    )
    assert plain == caller.Decision(agent="acme-dev", refusal=None)
    for extra in (
        {"principal": human["principal"]},
        {
            "principal": human["principal"],
            "run": "11111111-1111-4111-8111-111111111111",
            "work_item": "22222222-2222-4222-8222-222222222222",
        },
        {"principal": None},
    ):
        decision = caller.decide_route(
            keys,
            _signed_route({**base, **extra}),
            connector="reviewer",
            admits=frozenset({"acme-dev"}),
            now=now,
        )
        assert decision == caller.Decision(agent=None, refusal=caller.INVALID), extra
        assert decision.principal is None


# @spec CONNECTOR-REQUESTER-2
def test_a_principal_at_its_field_bounds_crosses_from_minter_to_verifier() -> None:
    from curie_worker import caller_token

    principal = caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444",
        issuer="https://idp.example.test/" + "a" * (2048 - 25),
        subject="s" * 256,
    )
    token = caller_token.mint(
        str(_SIGNER["seed"]), agent="acme-dev", exp=_exp(_SIGNER), principal=principal
    )
    decision = _decide(_SIGNER, token, admits={"acme-dev"}, now=_exp(_SIGNER) - 1)
    assert decision.admitted
    assert decision.principal == caller.Principal(
        tenant_id=principal.tenant_id, issuer=principal.issuer, subject=principal.subject
    )


def _refused(entry: dict[str, Any]) -> bool:
    token = str(entry["minted"])
    decision = _decide(_SIGNER, token, admits={"acme-dev"}, now=_exp(_SIGNER) - 1)
    return decision == caller.Decision(agent=None, refusal=caller.INVALID)


# @spec CONNECTOR-REQUESTER-2
def test_the_field_bounds_admit_at_the_bound_and_refuse_one_past_it() -> None:
    # The bounds are identity_context ContextCanonicalV1's, pinned through the
    # corpus: exactly 2048 and 256 characters admit, one more refuses.
    vector = _BY_NAME["a_principal_at_its_field_bounds"]
    fields = vector["principal"]
    assert (len(fields["issuer"]), len(fields["subject"])) == (2048, 256)
    decision = _decide(vector, str(vector["minted"]), admits={"acme-dev"}, now=_exp(vector) - 1)
    assert decision.admitted
    assert decision.principal == _principal(vector)
    for name, field, length in (
        ("an_issuer_of_2049_characters", "issuer", 2049),
        ("a_subject_of_257_characters", "subject", 257),
    ):
        entry = _MALFORMED_BY_NAME[name]
        assert len(json.loads(str(entry["payload"]))["principal"][field]) == length
        assert _refused(entry), name


# @spec CONNECTOR-REQUESTER-2
def test_a_control_character_is_admitted_and_exposed_exactly() -> None:
    # A principal valid for HTTP identity is valid in the token, and identity
    # admits control characters; the subject comes back verbatim, not cleaned.
    vector = _BY_NAME["a_control_character_is_carried_escaped"]
    subject = str(vector["principal"]["subject"])
    assert any(unicodedata.category(char) == "Cc" for char in subject)
    decision = _decide(vector, str(vector["minted"]), admits={"acme-dev"}, now=_exp(vector) - 1)
    assert decision.admitted
    assert decision.principal is not None
    assert decision.principal.subject == subject


# @spec CONNECTOR-REQUESTER-2
def test_a_lone_surrogate_is_refused() -> None:
    # Identity text must encode to UTF-8; a lone surrogate escape decodes to a
    # string that does not.
    entry = _MALFORMED_BY_NAME["a_lone_surrogate_in_subject"]
    subject = json.loads(str(entry["payload"]))["principal"]["subject"]
    with pytest.raises(UnicodeEncodeError):
        subject.encode("utf-8")
    assert _refused(entry)


# @spec ADR-0168 d7
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_a_token_signed_under_another_seed_is_invalid(vector: dict[str, object]) -> None:
    # The first two vectors share their payload, so this is the case that
    # catches a verifier which checks the claims and skips the signature.
    other = _BY_NAME[_OTHER_SEED[str(vector["name"])]]
    decision = caller.decide(
        _key(other),
        str(vector["minted"]),
        admits=frozenset({str(vector["agent"])}),
        now=_exp(vector) - 1,
    )
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID)


# @spec ADR-0168 d7
def test_either_configured_key_admits_so_a_rotation_overlaps() -> None:
    first, second = _BY_NAME["a_plain_agent_name"], _BY_NAME["the_same_claims_under_another_seed"]
    keys = [caller.public_key(str(first["public"])), caller.public_key(str(second["public"]))]
    for vector in (first, second):
        decision = caller.decide(
            keys, str(vector["minted"]), admits=frozenset({"acme-dev"}), now=_exp(vector) - 1
        )
        assert decision.admitted


# @spec ADR-0168 d7
@pytest.mark.parametrize("offset", [0, 1])
def test_a_token_at_or_past_its_expiry_is_expired(offset: int) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = _decide(
        vector, str(vector["minted"]), admits={"acme-dev"}, now=_exp(vector) + offset
    )
    assert decision == caller.Decision(agent="acme-dev", refusal=caller.EXPIRED)


# @spec ADR-0168 d7
def test_an_agent_off_the_list_is_not_admitted_and_the_match_is_exact() -> None:
    vector = _BY_NAME["a_stored_name_outside_the_bundle_shape_is_carried_verbatim"]
    now = _exp(vector) - 1
    for admits in ({"acme-dev"}, {"acme café"}, {"Acme Cafe"}, {"ACME CAFÉ"}, set()):
        decision = _decide(vector, str(vector["minted"]), admits=admits, now=now)
        assert decision == caller.Decision(agent="Acme Café", refusal=caller.NOT_ADMITTED)


# @spec ADR-0168 d7
@pytest.mark.parametrize("token", [None, ""])
def test_no_token_is_missing(token: str | None) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = caller.decide(
        _key(vector),
        token,
        admits=frozenset({"acme-dev"}),
        now=_exp(vector) - 1,
    )
    assert decision == caller.Decision(agent=None, refusal=caller.MISSING)


def _tampered() -> list[tuple[str, str]]:
    vector = _BY_NAME["a_plain_agent_name"]
    minted = str(vector["minted"])
    prefix, payload, signature = minted.split(".")
    seed = base64.b64decode(str(vector["seed"]))

    def signed(claims: bytes, *, head: str = "cct") -> str:
        segment = _b64url(claims)
        sig = SigningKey(seed).sign(f"{head}.{segment}".encode("ascii")).signature
        return f"{head}.{segment}.{_b64url(sig)}"

    other_agent = _b64url(b'{"agent":"other-agent","exp":1790000000}')
    later = _b64url(b'{"agent":"acme-dev","exp":1890000000}')
    # Standard alphabet where the url-safe one is required: the frozen
    # signature contains `_` and `-`, so this changes the spelling only.
    standard_signature = signature.replace("-", "+").replace("_", "/")
    assert standard_signature != signature
    # A non-canonical last character: same decoded bytes, different text.
    last = signature[-1]
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    sloppy = signature[:-1] + alphabet[alphabet.index(last) + 1]
    assert base64.urlsafe_b64decode(sloppy + "==") == base64.urlsafe_b64decode(signature + "==")
    return [
        ("another_agent_under_the_old_signature", f"{prefix}.{other_agent}.{signature}"),
        ("a_later_exp_under_the_old_signature", f"{prefix}.{later}.{signature}"),
        ("the_state_token_prefix", f"sbx.{payload}.{signature}"),
        ("an_uppercase_prefix", f"CCT.{payload}.{signature}"),
        ("a_missing_segment", f"{prefix}.{payload}"),
        ("an_extra_segment", f"{minted}.x"),
        ("a_padded_payload", f"{prefix}.{payload}=.{signature}"),
        ("a_padded_signature", f"{prefix}.{payload}.{signature}=="),
        ("the_standard_alphabet", f"{prefix}.{payload}.{standard_signature}"),
        ("a_non_canonical_signature", f"{prefix}.{payload}.{sloppy}"),
        ("a_truncated_signature", f"{prefix}.{payload}.{signature[:-4]}"),
        ("not_ascii", f"{prefix}.{payload}.{signature[:-1]}é"),
        ("far_too_long", f"{prefix}.{payload}.{signature}" + "A" * 5000),
        ("exp_as_a_bool", signed(b'{"agent":"acme-dev","exp":true}')),
        ("exp_as_a_string", signed(b'{"agent":"acme-dev","exp":"1890000000"}')),
        ("exp_as_a_float", signed(b'{"agent":"acme-dev","exp":1890000000.0}')),
        ("agent_not_a_string", signed(b'{"agent":7,"exp":1890000000}')),
        ("an_empty_agent", signed(b'{"agent":"","exp":1890000000}')),
        ("an_extra_claim", signed(b'{"agent":"acme-dev","aud":"x","exp":1890000000}')),
        # ADR 0270 decision 2: a duplicate key anywhere in the signed payload
        # refuses, so a first-wins and a last-wins reader cannot disagree.
        (
            "a_duplicate_agent_key",
            signed(b'{"agent":"other-agent","agent":"acme-dev","exp":1890000000}'),
        ),
        ("a_duplicate_exp_key", signed(b'{"agent":"acme-dev","exp":1,"exp":1890000000}')),
        (
            "only_run",
            signed(
                b'{"agent":"acme-dev","exp":1790000000,'
                b'"run":"11111111-1111-4111-8111-111111111111"}'
            ),
        ),
        (
            "only_work_item",
            signed(
                b'{"agent":"acme-dev","exp":1790000000,'
                b'"work_item":"22222222-2222-4222-8222-222222222222"}'
            ),
        ),
        (
            "an_uppercase_run",
            signed(
                b'{"agent":"acme-dev","exp":1790000000,'
                b'"run":"AAAAAAAA-1111-4111-8111-111111111111",'
                b'"work_item":"22222222-2222-4222-8222-222222222222"}'
            ),
        ),
        (
            "a_null_run",
            signed(
                b'{"agent":"acme-dev","exp":1790000000,"run":null,'
                b'"work_item":"22222222-2222-4222-8222-222222222222"}'
            ),
        ),
        ("a_missing_claim", signed(b'{"agent":"acme-dev"}')),
        ("not_an_object", signed(b'["acme-dev",1890000000]')),
        ("not_json", signed(b"acme-dev")),
        (
            "a_signed_but_foreign_prefix",
            signed(b'{"agent":"acme-dev","exp":1890000000}', head="cct2"),
        ),
    ]


# @spec ADR-0168 d7
@pytest.mark.parametrize(("name", "token"), _tampered(), ids=[n for n, _ in _tampered()])
def test_a_tampered_token_is_invalid(name: str, token: str) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = _decide(vector, token, admits={"acme-dev", "other-agent"}, now=_exp(vector) - 1)
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID), name


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    "text",
    [
        "not base64!",
        base64.b64encode(b"short").decode(),
        "",
        # Url-safe spelling of a key whose standard spelling has `+` or `/`.
        str(_BY_NAME["a_plain_agent_name"]["public"]).replace("/", "_"),
    ],
)
def test_a_public_key_that_is_not_32_standard_base64_bytes_is_refused(text: str) -> None:
    with pytest.raises(ValueError):
        caller.public_key(text)


# @spec ADR-0168 d7
def test_the_worker_wheel_ships_the_proxy_package() -> None:
    project = tomllib.loads(
        (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    packages = project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    assert "src/curie_connector_proxy" in packages


# --------------------------------------------------------------------------- #
# ADR 0270 decision 4: the X-Curie-Principal value the proxy sets, and the
# bounded strict reader a connector uses on it. Both read the corpus sections
# `principal_headers` and `malformed_principal_headers`.
# --------------------------------------------------------------------------- #

_HEADER_CASES: list[dict[str, Any]] = _CORPUS["principal_headers"]["cases"]
_HEADER_BY_NAME = {str(c["name"]): c for c in _HEADER_CASES}
_BAD_HEADERS: list[dict[str, Any]] = _CORPUS["malformed_principal_headers"]["cases"]
_BAD_HEADER_BY_NAME = {str(c["name"]): str(c["value"]) for c in _BAD_HEADERS}
_HUMAN_HEADER = "the_human_requester_header"


def _header_principal(case: dict[str, Any]) -> caller.Principal:
    fields = case["principal"]
    return caller.Principal(
        tenant_id=str(fields["tenant_id"]),
        issuer=str(fields["issuer"]),
        subject=str(fields["subject"]),
    )


# @spec CONNECTOR-REQUESTER-4
@pytest.mark.parametrize("case", _HEADER_CASES, ids=lambda c: str(c["name"]))
def test_every_principal_header_round_trips(case: dict[str, Any]) -> None:
    principal = _header_principal(case)
    assert caller.principal_header_value(principal) == case["header"]
    assert caller.read_principal_header(str(case["header"])) == principal


# @spec CONNECTOR-REQUESTER-4
@pytest.mark.parametrize("case", _BAD_HEADERS, ids=lambda c: str(c["name"]))
def test_every_malformed_principal_header_is_refused(case: dict[str, Any]) -> None:
    assert caller.read_principal_header(str(case["value"])) is None


# Each second spelling beside the genuine header it was made from: a lenient
# reader would read the same requester from both.
_SPELLINGS = [
    ("a_padded_header", _HUMAN_HEADER),
    ("a_header_in_the_standard_alphabet", "a_header_using_both_url_safe_characters"),
    ("a_header_with_stray_trailing_bits", _HUMAN_HEADER),
    ("a_header_with_a_non_ascii_character", _HUMAN_HEADER),
    ("a_header_with_inner_whitespace", _HUMAN_HEADER),
    ("a_header_with_a_trailing_newline", _HUMAN_HEADER),
    ("a_header_with_a_byte_order_mark", _HUMAN_HEADER),
    ("a_header_with_unsorted_keys", _HUMAN_HEADER),
    ("a_header_of_spaced_json", _HUMAN_HEADER),
    ("a_header_with_trailing_whitespace_in_its_json", _HUMAN_HEADER),
    ("a_header_with_an_escaped_solidus", _HUMAN_HEADER),
    ("a_header_with_escaped_non_ascii", "a_non_ascii_issuer_and_subject_header"),
]


# @spec CONNECTOR-REQUESTER-4
@pytest.mark.parametrize(("refused", "genuine"), _SPELLINGS, ids=[r for r, _ in _SPELLINGS])
def test_a_second_spelling_of_a_genuine_principal_is_refused(refused: str, genuine: str) -> None:
    case = _HEADER_BY_NAME[genuine]
    # Liveness: the one spelling reads back as the requester.
    assert caller.read_principal_header(str(case["header"])) == _header_principal(case)
    assert caller.read_principal_header(_BAD_HEADER_BY_NAME[refused]) is None


def _counting_decoder(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count base64 decodes: every `base64` decoder ends in `binascii.a2b_base64`."""

    calls: list[int] = []
    real = binascii.a2b_base64

    def counting(*args: Any, **kwargs: Any) -> bytes:
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(binascii, "a2b_base64", counting)
    return calls


# @spec CONNECTOR-REQUESTER-4
def test_one_character_past_the_length_bound_is_refused_before_decoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bound = caller.PRINCIPAL_HEADER_MAX_CHARS
    assert type(bound) is int
    genuine = _HEADER_BY_NAME[_HUMAN_HEADER]
    text = str(genuine["header"])
    over = (text * (bound // len(text) + 2))[: bound + 1]
    assert len(over) == bound + 1
    # The corpus's past-the-bound case is past this reader's bound too.
    past = _BAD_HEADER_BY_NAME["a_header_past_the_length_bound"]
    assert len(past) > bound
    calls = _counting_decoder(monkeypatch)
    assert caller.read_principal_header(over) is None
    assert caller.read_principal_header(past) is None
    assert calls == []
    # Liveness, and proof the count sees this reader's decoding at all.
    assert caller.read_principal_header(text) == _header_principal(genuine)
    assert calls


# @spec CONNECTOR-REQUESTER-4
def test_the_longest_ascii_principal_is_within_the_length_bound() -> None:
    case = _HEADER_BY_NAME["a_principal_at_its_field_bounds_header"]
    principal = _header_principal(case)
    assert (len(principal.issuer), len(principal.subject)) == (2048, 256)
    assert len(str(case["header"])) <= caller.PRINCIPAL_HEADER_MAX_CHARS
    assert caller.read_principal_header(str(case["header"])) == principal
    # One past either field bound is refused by the field bound, not the length.
    for name in (
        "a_header_with_an_issuer_of_2049_characters",
        "a_header_with_a_subject_of_257_characters",
    ):
        value = _BAD_HEADER_BY_NAME[name]
        assert len(value) <= caller.PRINCIPAL_HEADER_MAX_CHARS, name
        assert caller.read_principal_header(value) is None, name


# @spec CONNECTOR-REQUESTER-4
def test_the_widest_principal_a_caller_token_carries_is_readable() -> None:
    # Non-ASCII text is wider in the header than its character count, so a
    # bound sized to the longest ASCII principal would refuse a requester the
    # proxy forwards. This one fills a cct to its 4096-character bound.
    from curie_worker import caller_token

    principal = caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444",
        issuer="https://idp.example.test/" + "a" * (2048 - 25),
        subject="\U0001d11e" * 52 + "u" * (256 - 52),
    )
    token = caller_token.mint(
        str(_SIGNER["seed"]), agent="acme-dev", exp=_exp(_SIGNER), principal=principal
    )
    assert len(token) <= caller_token.MAX_TOKEN_CHARS
    decision = _decide(_SIGNER, token, admits={"acme-dev"}, now=_exp(_SIGNER) - 1)
    assert decision.principal is not None
    header = caller.principal_header_value(decision.principal)
    longest_ascii = str(_HEADER_BY_NAME["a_principal_at_its_field_bounds_header"]["header"])
    assert len(header) > len(longest_ascii)
    assert len(header) <= caller.PRINCIPAL_HEADER_MAX_CHARS
    assert caller.read_principal_header(header) == decision.principal


def _junk() -> list[str]:
    rng = random.Random(270)
    url_safe = string.ascii_letters + string.digits + "-_"
    values = [
        "",
        "=",
        "==",
        "A",
        "AA",
        "AAA",
        "AAAA",
        "-",
        "_",
        "....",
        " ",
        "\t",
        "\r\n",
        "\x00",
        "\ud800",
        "\udfff\ud800",
        "é" * 8,
        "\U0001d11e",
        "cct.",
        _b64url(b"{}"),
        _b64url(b"[]"),
        _b64url(b"true"),
        _b64url(("[" * 1500 + "]" * 1500).encode("ascii")),
        _b64url(('{"a":' * 500 + "1" + "}" * 500).encode("ascii")),
        _b64url(b'{"issuer":"\\u12","subject":"s","tenant_id":"t"}'),
        _b64url(b"9" * 3000),
        _b64url(b'{"issuer":1e999,"subject":-0,"tenant_id":[]}'),
        _b64url(
            b'{"issuer":{"issuer":"x"},"subject":"s",'
            b'"tenant_id":"44444444-4444-4444-8444-444444444444"}'
        ),
    ]
    for _ in range(150):
        size = rng.randrange(0, 96)
        values.append("".join(rng.choice(url_safe) for _ in range(size)))
        values.append(_b64url(rng.randbytes(size)))
        values.append("".join(chr(rng.randrange(0x110000)) for _ in range(rng.randrange(12))))
    return values


# @spec CONNECTOR-REQUESTER-4
def test_the_principal_header_reader_never_raises_on_junk() -> None:
    for value in _junk():
        assert caller.read_principal_header(value) is None, repr(value[:40])
    # Liveness: the same reader admits a genuine header.
    case = _HEADER_BY_NAME[_HUMAN_HEADER]
    assert caller.read_principal_header(str(case["header"])) == _header_principal(case)
