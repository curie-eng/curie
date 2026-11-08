"""The connector caller token the worker mints (ADR-0168 decision 7, ADR 0270 decision 2).

The wire is frozen in tests/vectors/connector-caller-token.json, because the
verifier that checks it holds only the public key and must accept exactly what
this module signs.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import unicodedata
from pathlib import Path
from typing import Any

import pytest
from curie_worker import caller_token
from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
)
_FILE_KEYS = {
    "comment",
    "prefix",
    "vectors",
    "malformed_principals",
    "tampered_principals",
    "forged_principal_headers",
}
_VECTOR_KEYS = {"name", "why", "seed", "public", "agent", "exp", "minted"}
_RUN_KEYS = {"run", "work_item"}
_PRINCIPAL_KEY = "principal"
_PRINCIPAL_FIELDS = {"tenant_id", "issuer", "subject"}
_MALFORMED_KEYS = {"name", "why", "payload", "minted"}
_TAMPERED_KEYS = {"name", "why", "field", "minted"}
_FORGED_KEYS = {"header", "cases"}
_FORGED_CASE_KEYS = {"name", "why", "token_kind", "inbound"}
_HUMAN = "a_human_requester"


def _corpus() -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(_VECTOR.read_text(encoding="utf-8"))
    return parsed


def _vectors() -> list[dict[str, Any]]:
    vectors = _corpus()["vectors"]
    assert isinstance(vectors, list) and vectors
    return vectors


def _malformed() -> list[dict[str, Any]]:
    entries = _corpus()["malformed_principals"]
    assert isinstance(entries, list) and entries
    return entries


def _tampered() -> list[dict[str, Any]]:
    entries = _corpus()["tampered_principals"]
    assert isinstance(entries, list) and entries
    return entries


def _by_name(name: str) -> dict[str, Any]:
    return next(v for v in _vectors() if v["name"] == name)


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class _DuplicateKey(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise _DuplicateKey
    return dict(pairs)


def _strict_loads(text: str | bytes) -> Any:
    """``json.loads`` that refuses a duplicate key at any depth."""

    return json.loads(text, object_pairs_hook=_strict_object)


def test_the_vector_carries_only_known_keys() -> None:
    corpus = _corpus()
    assert set(corpus) == _FILE_KEYS
    for vector in _vectors():
        keys = set(vector)
        assert _VECTOR_KEYS <= keys, vector.get("name")
        assert keys <= _VECTOR_KEYS | _RUN_KEYS | {_PRINCIPAL_KEY}, vector.get("name")
        assert ("run" in keys) == ("work_item" in keys)
        if _PRINCIPAL_KEY in keys:
            principal = vector[_PRINCIPAL_KEY]
            assert isinstance(principal, dict), vector["name"]
            assert set(principal) == _PRINCIPAL_FIELDS, vector["name"]
            assert all(isinstance(value, str) for value in principal.values()), vector["name"]
    for entry in _malformed():
        assert set(entry) == _MALFORMED_KEYS, entry.get("name")
    for entry in _tampered():
        assert set(entry) == _TAMPERED_KEYS, entry.get("name")
        assert entry["field"] in _PRINCIPAL_FIELDS | {_PRINCIPAL_KEY}, entry["name"]
    forged = corpus["forged_principal_headers"]
    assert set(forged) == _FORGED_KEYS
    assert forged["header"] == "X-Curie-Principal"
    assert forged["cases"]
    for case in forged["cases"]:
        assert set(case) == _FORGED_CASE_KEYS, case.get("name")
        assert case["token_kind"] in {"cct", "ccr"}, case["name"]
        assert case["inbound"], case["name"]
        for pair in case["inbound"]:
            assert isinstance(pair, list) and len(pair) == 2, case["name"]
            assert all(isinstance(part, str) and part for part in pair), case["name"]
            # Every forged name is the principal header in some letter case.
            assert pair[0].lower() == forged["header"].lower(), case["name"]


def test_every_corpus_entry_has_its_own_name() -> None:
    corpus = _corpus()
    names = [
        str(entry["name"])
        for entry in (
            *_vectors(),
            *_malformed(),
            *_tampered(),
            *corpus["forged_principal_headers"]["cases"],
        )
    ]
    assert len(names) == len(set(names))


def test_the_prefix_is_the_one_the_worker_signs_under() -> None:
    assert _corpus()["prefix"] == caller_token.PREFIX


def _principal_kwargs(vector: dict[str, Any]) -> dict[str, Any]:
    if _PRINCIPAL_KEY not in vector:
        return {}
    return {"principal": caller_token.CallerPrincipal(**vector[_PRINCIPAL_KEY])}


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_the_worker_mints_exactly_the_frozen_token(vector: dict[str, object]) -> None:
    extra: dict[str, Any] = (
        {"run": str(vector["run"]), "work_item": str(vector["work_item"])}
        if "run" in vector
        else {}
    )
    # An agent-only vector passes no principal at all, so the call is today's.
    extra.update(_principal_kwargs(vector))
    minted = caller_token.mint(
        str(vector["seed"]),
        agent=str(vector["agent"]),
        exp=int(str(vector["exp"])),
        **extra,
    )
    assert minted == vector["minted"]


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_every_frozen_token_verifies_under_its_public_key(vector: dict[str, object]) -> None:
    # The corpus has to be right before a verifier is written against it, so
    # the signature and the claims are checked here with the primitive itself.
    seed = base64.b64decode(str(vector["seed"]))
    public = base64.b64decode(str(vector["public"]))
    assert bytes(SigningKey(seed).verify_key) == public

    prefix, payload, signature = str(vector["minted"]).split(".")
    assert prefix == _corpus()["prefix"]
    VerifyKey(public).verify(f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature))
    expected = {"agent": vector["agent"], "exp": vector["exp"]}
    if "run" in vector:
        expected["run"] = vector["run"]
        expected["work_item"] = vector["work_item"]
    if _PRINCIPAL_KEY in vector:
        expected[_PRINCIPAL_KEY] = vector[_PRINCIPAL_KEY]
    assert _strict_loads(_b64url_decode(payload)) == expected
    # One spelling: compact, sorted at every level, ASCII escapes.
    assert _b64url_decode(payload) == json.dumps(
        expected, separators=(",", ":"), sort_keys=True
    ).encode("ascii")


def test_the_non_ascii_subject_is_signed_as_an_escape() -> None:
    vector = _by_name("a_non_ascii_subject_is_carried_escaped")
    subject = str(vector[_PRINCIPAL_KEY]["subject"])
    assert not subject.isascii()
    payload = _b64url_decode(str(vector["minted"]).split(".")[1])
    assert subject.encode("utf-8") not in payload
    assert json.dumps(subject).encode("ascii") in payload


def test_the_control_character_subject_is_signed_as_an_escape() -> None:
    # The identity bounds admit a control character, so the token carries it;
    # the ASCII escapes keep the raw byte out of the signed payload.
    vector = _by_name("a_control_character_is_carried_escaped")
    subject = str(vector[_PRINCIPAL_KEY]["subject"])
    controls = [char for char in subject if unicodedata.category(char) == "Cc"]
    assert controls
    payload = _b64url_decode(str(vector["minted"]).split(".")[1])
    for char in controls:
        assert char.encode("ascii") not in payload
    assert json.dumps(subject).encode("ascii") in payload
    assert _strict_loads(payload)[_PRINCIPAL_KEY]["subject"] == subject


def test_the_field_bounds_are_pinned_at_and_one_past_each_bound() -> None:
    # The minter must reproduce the at-bound vector byte for byte (the
    # parametrized mint test) and refuse to build each one-past principal.
    at_bounds = _by_name("a_principal_at_its_field_bounds")[_PRINCIPAL_KEY]
    assert (len(at_bounds["issuer"]), len(at_bounds["subject"])) == (2048, 256)
    assert len(str(_by_name("a_principal_at_its_field_bounds")["minted"])) <= 4096
    past = {name: fields for name, fields in _three_field_principals()}
    assert len(past["an_issuer_of_2049_characters"]["issuer"]) == 2049
    assert len(past["a_subject_of_257_characters"]["subject"]) == 257
    for name in ("an_issuer_of_2049_characters", "a_subject_of_257_characters"):
        with pytest.raises(ValueError):
            caller_token.CallerPrincipal(**past[name])


@pytest.mark.parametrize("entry", _malformed(), ids=lambda e: str(e["name"]))
def test_every_malformed_principal_is_validly_signed_so_only_its_claims_refuse_it(
    entry: dict[str, Any],
) -> None:
    # The corpus has to be right before a verifier is written against it: each
    # token is `cct.<payload>.<signature>` over the exact payload text, signed
    # by the first vector's seed and under the verifier's size bound, so a
    # verifier that refuses it can only be refusing the claims.
    first = _vectors()[0]
    text = str(entry["payload"])
    assert text.isascii(), entry["name"]
    prefix, payload, signature = str(entry["minted"]).split(".")
    assert prefix == _corpus()["prefix"]
    assert payload == _b64url(text.encode("ascii"))
    public = base64.b64decode(str(first["public"]))
    VerifyKey(public).verify(f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature))
    # The verifier's size bound, written out so this corpus check stands alone.
    assert len(str(entry["minted"])) <= 4096
    parsed = json.loads(text)
    assert isinstance(parsed, dict) and "principal" in text, entry["name"]
    assert (parsed["agent"], parsed["exp"]) == (first["agent"], first["exp"])


@pytest.mark.parametrize("entry", _tampered(), ids=lambda e: str(e["name"]))
def test_every_tampered_principal_changes_one_field_under_the_original_signature(
    entry: dict[str, Any],
) -> None:
    human = _by_name(_HUMAN)
    original_prefix, original_payload, original_signature = str(human["minted"]).split(".")
    prefix, payload, signature = str(entry["minted"]).split(".")
    assert (prefix, signature) == (original_prefix, original_signature)
    assert payload != original_payload
    claims = _strict_loads(_b64url_decode(payload))
    original = _strict_loads(_b64url_decode(original_payload))
    # Canonical text, so only the signature can refuse it.
    assert _b64url_decode(payload) == json.dumps(
        claims, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    field = entry["field"]
    if field == _PRINCIPAL_KEY:
        assert _PRINCIPAL_KEY not in claims
        assert claims == {k: v for k, v in original.items() if k != _PRINCIPAL_KEY}
    else:
        changed = {
            key
            for key in _PRINCIPAL_FIELDS
            if claims[_PRINCIPAL_KEY][key] != original[_PRINCIPAL_KEY][key]
        }
        assert changed == {field}, entry["name"]
        assert {k: v for k, v in claims.items() if k != _PRINCIPAL_KEY} == {
            k: v for k, v in original.items() if k != _PRINCIPAL_KEY
        }
    public = base64.b64decode(str(human["public"]))
    with pytest.raises(BadSignatureError):
        VerifyKey(public).verify(f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature))


def _three_field_principals() -> list[tuple[str, dict[str, Any]]]:
    """Malformed entries whose principal is an object with exactly the three
    field names and no duplicate key: refused for a value, not the shape."""

    found: list[tuple[str, dict[str, Any]]] = []
    for entry in _malformed():
        try:
            parsed = _strict_loads(str(entry["payload"]))
        except _DuplicateKey:
            continue
        principal = parsed.get(_PRINCIPAL_KEY)
        if isinstance(principal, dict) and set(principal) == _PRINCIPAL_FIELDS:
            found.append((str(entry["name"]), principal))
    return found


def test_the_value_refusals_are_not_vacuous() -> None:
    names = {name for name, _ in _three_field_principals()}
    assert {
        "an_uppercase_tenant",
        "a_tenant_that_is_not_a_uuid",
        "an_empty_issuer",
        "an_empty_subject",
        "an_issuer_of_2049_characters",
        "a_subject_of_257_characters",
        "a_lone_surrogate_in_subject",
    } <= names


@pytest.mark.parametrize(
    ("name", "fields"), _three_field_principals(), ids=[n for n, _ in _three_field_principals()]
)
def test_a_principal_the_verifier_would_refuse_cannot_be_built(
    name: str, fields: dict[str, Any]
) -> None:
    # ADR 0270 decision 2: the minter refuses before boot what the verifier
    # would refuse after it, and the refusal never carries the identity.
    with pytest.raises(ValueError) as refused:
        caller_token.CallerPrincipal(**fields)
    message = str(refused.value)
    for value in fields.values():
        if isinstance(value, str) and value:
            assert value not in message, name


def test_every_vector_principal_can_be_built_and_is_frozen() -> None:
    principals = [v[_PRINCIPAL_KEY] for v in _vectors() if _PRINCIPAL_KEY in v]
    assert principals
    for fields in principals:
        built = caller_token.CallerPrincipal(**fields)
        assert (built.tenant_id, built.issuer, built.subject) == (
            fields["tenant_id"],
            fields["issuer"],
            fields["subject"],
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            built.subject = "user-0009"  # type: ignore[misc]


def test_a_principal_at_its_field_bounds_can_be_built() -> None:
    caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444",
        issuer="i" * 2048,
        subject="s" * 256,
    )
    caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444", issuer="i", subject="s"
    )


def _accepts(build: Any) -> bool:
    try:
        build()
    except ValueError:  # pydantic's ValidationError is a ValueError
        return False
    return True


def test_the_principal_bounds_are_the_canonical_identity_bounds() -> None:
    # A principal valid for HTTP identity is valid in the token: CallerPrincipal
    # accepts exactly what identity_context's ContextCanonicalV1 (issuer,
    # subject) and canonical UUID (tenant) accept, sampled at each boundary.
    from curie_internal import identity_context
    from pydantic import TypeAdapter

    tenant = "44444444-4444-4444-8444-444444444444"
    text_samples = [
        ("i" * 2048, "s" * 256, True),
        ("é" * 2048, "é" * 256, True),
        ("i", "s", True),
        ("i" * 2049, "s", False),
        ("i", "s" * 257, False),
        ("", "s", False),
        ("i", "", False),
        ("i", "user\t0001", True),
        ("https://idp.example.test\n", "s", True),
        ("i", "\x00", True),
        ("i", "\x7f\x85", True),
        ("i", "user-0001\ud800", False),
        ("\udfff", "s", False),
    ]
    for issuer, subject, expected in text_samples:
        canonical = _accepts(
            lambda i=issuer, s=subject: identity_context.ContextCanonicalV1(issuer=i, subject=s)
        )
        caller = _accepts(
            lambda i=issuer, s=subject: caller_token.CallerPrincipal(
                tenant_id=tenant, issuer=i, subject=s
            )
        )
        assert canonical == expected, (len(issuer), len(subject))
        assert caller == canonical, (len(issuer), len(subject))
    canonical_uuid = TypeAdapter(identity_context.CanonicalUUID)
    for tenant_id, expected in [
        (tenant, True),
        ("00000000-0000-0000-0000-000000000000", True),
        ("AAAAAAAA-4444-4444-8444-444444444444", False),
        ("44444444444444448444444444444444", False),
        ("{44444444-4444-4444-8444-444444444444}", False),
        ("44444444-4444-4444-8444-444444444444\n", False),
        ("tenant-0001", False),
        ("", False),
    ]:
        canonical = _accepts(lambda t=tenant_id: canonical_uuid.validate_python(t))
        caller = _accepts(
            lambda t=tenant_id: caller_token.CallerPrincipal(tenant_id=t, issuer="i", subject="s")
        )
        assert canonical == expected, tenant_id
        assert caller == canonical, tenant_id


def test_a_token_past_the_verifier_bound_is_refused_without_echoing_the_principal() -> None:
    # Every field is inside its own bound, but the ASCII escapes (six bytes
    # per character) push the token past what the verifier reads. ADR 0270
    # decision 2: refused before boot, not minted for the verifier to drop.
    seed = str(_vectors()[0]["seed"])
    issuer = "https://idp.example.test/" + "é" * 2000
    subject = "é" * 256
    assert len(issuer) <= 2048 and len(subject) <= 256
    principal = caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444", issuer=issuer, subject=subject
    )
    with pytest.raises(ValueError) as refused:
        caller_token.mint(seed, agent="acme-dev", exp=1790000000, principal=principal)
    message = str(refused.value)
    assert issuer not in message
    assert subject not in message
    assert "é" * 8 not in message
    assert "\\u00e9" not in message


def test_the_minter_bounds_a_token_at_the_verifiers_4096_characters() -> None:
    assert caller_token.MAX_TOKEN_CHARS == 4096


def test_a_principal_at_its_field_bounds_still_mints_under_the_token_bound() -> None:
    seed = str(_vectors()[0]["seed"])
    principal = caller_token.CallerPrincipal(
        tenant_id="44444444-4444-4444-8444-444444444444",
        issuer="https://idp.example.test/" + "a" * (2048 - 25),
        subject="s" * 256,
    )
    token = caller_token.mint(
        seed,
        agent="acme-dev",
        exp=1790000000,
        run="11111111-1111-4111-8111-111111111111",
        work_item="22222222-2222-4222-8222-222222222222",
        principal=principal,
    )
    assert len(token) <= caller_token.MAX_TOKEN_CHARS
    claims = _strict_loads(_b64url_decode(token.split(".")[1]))
    assert claims[_PRINCIPAL_KEY] == {
        "tenant_id": principal.tenant_id,
        "issuer": principal.issuer,
        "subject": principal.subject,
    }


def test_a_route_token_never_takes_a_principal() -> None:
    # ADR 0270 decision 2: a ccr stays an agent/run credential without human
    # authority, so its minter has no way to be handed one.
    seed = str(_vectors()[0]["seed"])
    principal = caller_token.CallerPrincipal(**_by_name(_HUMAN)[_PRINCIPAL_KEY])
    with pytest.raises(TypeError):
        caller_token.mint_route(  # type: ignore[call-arg]
            seed, agent="acme-dev", connector="reviewer", exp=1790000000, principal=principal
        )


def test_an_omitted_principal_keeps_the_agent_only_bytes() -> None:
    plain = _by_name("a_plain_agent_name")
    assert (
        caller_token.mint(str(plain["seed"]), agent="acme-dev", exp=1790000000, principal=None)
        == plain["minted"]
    )


def test_one_of_the_run_pair_is_refused_without_echoing_it() -> None:
    seed = str(_vectors()[0]["seed"])
    run = "11111111-1111-4111-8111-111111111111"
    work_item = "22222222-2222-4222-8222-222222222222"
    with pytest.raises(ValueError, match="present together") as caught:
        caller_token.mint(seed, agent="acme-dev", exp=1790000000, run=run)
    assert run not in str(caught.value)
    with pytest.raises(ValueError, match="present together"):
        caller_token.mint(seed, agent="acme-dev", exp=1790000000, work_item=work_item)


def test_a_key_that_is_not_a_32_byte_seed_is_refused() -> None:
    for text in ("not base64!", base64.b64encode(b"short").decode(), ""):
        with pytest.raises(ValueError):
            caller_token.signing_key(text)


def test_the_refusal_never_echoes_the_key() -> None:
    text = base64.b64encode(b"x" * 31).decode()
    with pytest.raises(ValueError) as refused:
        caller_token.signing_key(text)
    assert text not in str(refused.value)


def test_surrounding_whitespace_in_a_mounted_key_is_ignored() -> None:
    # A Secret written with `echo` carries a trailing newline.
    seed = str(_vectors()[0]["seed"])
    assert caller_token.mint(seed + "\n", agent="acme-dev", exp=1790000000) == _vectors()[0][
        "minted"
    ]


def test_a_damaged_key_that_would_repair_into_a_different_key_is_refused() -> None:
    # Without `validate=True`, `base64.b64decode` silently discards any
    # character outside the base64 alphabet instead of refusing the input. A
    # Secret damaged in transit could then "repair" into a 32-byte seed that
    # decodes cleanly -- just not the one anybody wrote down. This seed's
    # first character is replaced with `-` (not base64) and a different,
    # still-valid character is inserted right after; dropping the `-` and
    # keeping that character still decodes to 32 bytes, but to different
    # bytes than the original seed (a base64 symbol is a bijection onto its
    # 6 bits, so swapping the leading symbol always changes the decode).
    original = base64.b64encode(bytes(SigningKey.generate())).decode()
    replacement = "A" if original[0] != "A" else "B"
    damaged = "-" + replacement + original[1:]
    assert len(base64.b64decode(damaged, validate=False)) == 32
    assert base64.b64decode(damaged, validate=False) != base64.b64decode(original)

    with pytest.raises(ValueError):
        caller_token.signing_key(damaged)
