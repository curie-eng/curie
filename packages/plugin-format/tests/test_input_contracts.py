"""Generic input compatibility is strict, byte-bound and explicitly enforced."""

import hashlib
import json
from pathlib import Path

import pytest
from plugin_format import validate_bundle
from plugin_format.input_contracts import (
    INPUT_CONTRACT_ENFORCEMENT,
    InputContractsInvalid,
    parse_input_contracts,
    parse_input_envelope,
    read_input_contracts,
    require_input_contract,
)

CONTRACT = "example.specification/v1"
DEPLOYMENT = "11111111-1111-4111-8111-111111111111"
BUNDLE = "a" * 64
BODY = "## Problem\nPreserve café and exact newlines.\n"


def declaration(**overrides: object) -> dict[str, object]:
    return {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "accepts": [CONTRACT],
        "required": True,
        **overrides,
    }


def envelope(body: str = BODY, **overrides: object) -> str:
    metadata = {
        "contract": CONTRACT,
        "deployment": DEPLOYMENT,
        "bundle_sha256": BUNDLE,
        "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        **overrides,
    }
    return "<!-- curie-input@1 " + json.dumps(metadata, separators=(",", ":")) + " -->\n" + body


def bundle(root: Path, manifest: dict[str, object]) -> Path:
    (root / ".claude-plugin").mkdir()
    (root / ".claude-plugin/plugin.json").write_text(
        json.dumps({"name": "demo", **manifest}), encoding="utf-8"
    )
    return root


def test_declaration_parses_without_factory_specific_semantics() -> None:
    parsed = parse_input_contracts(declaration(accepts=[CONTRACT, "other.vendor/v22"]))
    assert parsed.interface == INPUT_CONTRACT_ENFORCEMENT
    assert set(parsed.accepts) == {CONTRACT, "other.vendor/v22"}
    assert parsed.required is True


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        "v1",
        {},
        declaration(required=False),
        declaration(required=1),
        declaration(interface="curie/input-contracts@2"),
        declaration(accepts=[]),
        declaration(accepts=[CONTRACT] * 33),
        declaration(accepts=[CONTRACT, CONTRACT]),
        declaration(accepts=[None]),
        declaration(accepts="v1"),
        declaration(accepts=[""]),
        declaration(accepts=["has spaces"]),
        declaration(accepts=["non-ascii-é"]),
        declaration(accepts=["x" * 201]),
        declaration(unrecognized="ignored?"),
    ],
)
def test_invalid_declarations_fail_closed(raw: object) -> None:
    with pytest.raises(InputContractsInvalid):
        parse_input_contracts(raw)


def test_absent_declaration_preserves_legacy_bundle_behavior(tmp_path: Path) -> None:
    root = bundle(tmp_path, {})
    assert read_input_contracts(root, enforcements=frozenset()) is None
    assert validate_bundle(root).valid


def test_declared_contract_requires_explicit_consumer_handshake(tmp_path: Path) -> None:
    root = bundle(tmp_path, {"inputContracts": declaration()})
    with pytest.raises(InputContractsInvalid):
        read_input_contracts(root, enforcements=frozenset())
    parsed = read_input_contracts(root, enforcements=frozenset({INPUT_CONTRACT_ENFORCEMENT}))
    assert parsed is not None and CONTRACT in parsed.accepts
    result = validate_bundle(root)
    assert not result.valid
    assert "input_contracts.unenforceable" in {issue.code for issue in result.errors}
    result = validate_bundle(root, enforces_input_contracts=INPUT_CONTRACT_ENFORCEMENT)
    assert result.valid, result.errors


def test_explicit_null_declaration_is_invalid_not_legacy(tmp_path: Path) -> None:
    root = bundle(tmp_path, {"inputContracts": None})
    with pytest.raises(InputContractsInvalid):
        read_input_contracts(root, enforcements=frozenset({INPUT_CONTRACT_ENFORCEMENT}))
    result = validate_bundle(root, enforces_input_contracts=INPUT_CONTRACT_ENFORCEMENT)
    assert not result.valid
    assert "input_contracts.invalid" in {issue.code for issue in result.errors}


def test_envelope_returns_exact_body_and_identity() -> None:
    parsed = parse_input_envelope(envelope())
    assert parsed.body == BODY
    assert parsed.contract == CONTRACT
    assert str(parsed.deployment) == DEPLOYMENT
    assert parsed.bundle_sha256 == BUNDLE
    assert parsed.body_sha256 == hashlib.sha256(BODY.encode()).hexdigest()


@pytest.mark.parametrize("body", ["", "\n", "é\r\n", "## Problem\ntext\n\n"])
def test_valid_envelope_hashes_exact_utf8_remainder(body: str) -> None:
    assert parse_input_envelope(envelope(body)).body == body


@pytest.mark.parametrize(
    "mutation",
    [
        lambda text: "\n" + text,
        lambda text: "prefix " + text,
        lambda text: "\ufeff" + text,
        lambda text: text.replace("curie-input@1", "curie-input@2", 1),
        lambda text: text.replace(" -->\n", " --!>\n", 1),
        lambda text: text.replace(" -->\n", " -->\r\n", 1),
        lambda text: text.replace(" -->\n", " -->", 1),
        lambda text: text + "edited\n",
        lambda text: text.replace("café", "cafe"),
        lambda text: text.replace('"contract":', '"contract":"injected","contract":', 1),
    ],
)
def test_edited_malformed_or_nonleading_envelopes_are_refused(mutation) -> None:
    with pytest.raises(InputContractsInvalid):
        parse_input_envelope(mutation(envelope()))


@pytest.mark.parametrize(
    "overrides",
    [
        {"contract": ""},
        {"contract": "bad id"},
        {"contract": "é"},
        {"deployment": "not-a-uuid"},
        {"bundle_sha256": "g" * 64},
        {"bundle_sha256": "a" * 63},
        {"body_sha256": "0" * 64},
        {"body_sha256": None},
        {"unknown": True},
        {"contract": "x" * 2050},
    ],
)
def test_invalid_envelope_fields_are_refused(overrides: dict[str, object]) -> None:
    with pytest.raises(InputContractsInvalid):
        parse_input_envelope(envelope(**overrides))


def test_accepted_contract_and_exact_deployment_bundle_are_required() -> None:
    parsed = require_input_contract(
        envelope(),
        parse_input_contracts(declaration()),
        deployment_id=DEPLOYMENT,
        bundle_sha256=BUNDLE,
    )
    assert parsed.body == BODY


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": "example.specification/v2"},
        {"deployment": "22222222-2222-4222-8222-222222222222"},
        {"bundle_sha256": "b" * 64},
    ],
)
def test_correctly_hashed_but_incompatible_input_is_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(InputContractsInvalid):
        require_input_contract(
            envelope(**changes),
            parse_input_contracts(declaration()),
            deployment_id=DEPLOYMENT,
            bundle_sha256=BUNDLE,
        )


def test_standalone_bundle_validation_does_not_claim_managed_input_enforcement(
    tmp_path: Path,
) -> None:
    root = bundle(tmp_path, {"inputContracts": declaration()})
    assert not validate_bundle(root).valid
    local = validate_bundle(root, input_contract_context="standalone")
    assert local.valid, local.errors


def test_unknown_input_contract_context_is_not_silently_standalone(tmp_path: Path) -> None:
    root = bundle(tmp_path, {"inputContracts": declaration()})
    with pytest.raises(ValueError):
        validate_bundle(root, input_contract_context="unknown-context")
