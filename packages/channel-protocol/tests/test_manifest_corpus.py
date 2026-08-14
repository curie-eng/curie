"""The three consumer corpus for the adapter binding profile (#1516).

The same committed file is read by the Pydantic models here, by the exported
JSON Schema, and by the Rust CLI (``cli/tests/adapter_manifest.rs``). Every
invalid case is rejected by the Pydantic model. The exported schema rejects
tier 1 and tier 2 only.

Tier 3 is an ADMITTED PAIRED VALIDATOR: JSON Schema has no keyword for "the
Rust regex crate can compile this string" and no extension both validators
would honor, so the rule lives in a Python validator and a Rust one, with this
corpus as the drift gate between them. The tier 3 branch below therefore
asserts the INVERSE, that the schema accepts the case. Without that inverted
assertion a future reader would assume the rule exports, and the Rust half
could quietly disappear.
"""

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from channel_protocol.manifest import ProfileVersionError, load_profile
from channel_protocol.manifest_schema import build_schema
from pydantic import ValidationError

_CORPUS: dict[str, Any] = json.loads(
    (Path(__file__).parents[1] / "schema" / "adapter-profile.corpus.json").read_text(
        encoding="utf-8"
    )
)
_ROOT_MODEL = "AdapterProfile"


def _schema_validator() -> jsonschema.protocols.Validator:
    schema = build_schema()
    root = {
        "$schema": schema["$schema"],
        "$ref": f"#/$defs/{_ROOT_MODEL}",
        "$defs": schema["$defs"],
    }
    return jsonschema.Draft202012Validator(root)


def _referenced_models(property_schema: dict[str, Any]) -> list[str]:
    """Model names a property's schema can resolve to, direct or through a union."""

    names = []
    ref = property_schema.get("$ref")
    if isinstance(ref, str):
        names.append(ref.rsplit("/", 1)[-1])
    for member in list(property_schema.get("anyOf", [])) + list(property_schema.get("oneOf", [])):
        names.extend(_referenced_models(member))
    items = property_schema.get("items")
    if isinstance(items, dict):
        names.extend(_referenced_models(items))
    return names


def _mark_exercised(
    defs: dict[str, Any], model: str, value: dict[str, Any], seen: set[str]
) -> None:
    properties = defs[model].get("properties", {})
    for name, raw in value.items():
        if name not in properties:
            continue
        seen.add(f"{model}.{name}")
        if not isinstance(raw, dict):
            continue
        for child in _referenced_models(properties[name]):
            if child in defs:
                _mark_exercised(defs, child, raw, seen)


def test_every_invalid_case_is_rejected_at_its_declared_tier() -> None:
    validator = _schema_validator()
    for case in _CORPUS["invalid"]:
        tier = case["tier"]
        assert tier in (1, 2, 3), f"unknown tier {tier}: {case['why']}"
        with pytest.raises((ValidationError, ProfileVersionError)):
            load_profile(case["profile"])
        caught_by_schema = not validator.is_valid(case["profile"])
        if tier == 3:
            assert not caught_by_schema, (
                "a tier 3 case is caught by the exported schema, so its tier tag is "
                f"wrong and the rule does not need a paired validator: {case['why']}"
            )
        else:
            assert caught_by_schema, (
                f"a tier {tier} case is not caught by the exported schema, so the Rust "
                f"CLI does not get the rule for free: {case['why']}"
            )


def test_corpus_covers_every_schema_property() -> None:
    """A property no valid case carries is a property the Rust mirror can omit.

    The Rust struct uses ``deny_unknown_fields``, so a field the corpus never
    sends can be missing on that side and every cross language test stays green.
    """

    defs = build_schema()["$defs"]
    seen: set[str] = set()
    for profile in _CORPUS["valid"]:
        _mark_exercised(defs, _ROOT_MODEL, profile, seen)

    declared = {
        f"{model}.{name}"
        for model, definition in defs.items()
        for name in definition.get("properties", {})
    }
    missing = sorted(declared - seen)
    assert not missing, f"no valid corpus case exercises: {', '.join(missing)}"


def test_valid_profiles_round_trip_compared_values() -> None:
    """Values are COMPARED, not merely parsed.

    A consumer that returns a constant instead of carrying the parsed value
    through passes a parse only check on every case. The corpus therefore
    varies the values across cases, and this asserts the variation survived.
    """

    egress_values = set()
    patterns = set()
    for profile in _CORPUS["valid"]:
        parsed = load_profile(profile)
        assert parsed.credentials.egress == profile["credentials"]["egress"]
        assert parsed.address.pattern == profile["address"]["pattern"]
        assert parsed.endpoint == profile.get("endpoint")
        egress_values.add(profile["credentials"]["egress"])
        patterns.add(profile["address"]["pattern"])

    assert len(egress_values) >= 2, "every valid case carries the same egress identity"
    assert len(patterns) >= 2, "every valid case carries the same address pattern"
    assert any("endpoint" not in profile for profile in _CORPUS["valid"]), (
        "no valid case omits endpoint, so its optionality is never exercised"
    )
    assert any("_" in profile["kind"] for profile in _CORPUS["valid"]), (
        "no valid case carries an underscore slug, which the API accepts"
    )
