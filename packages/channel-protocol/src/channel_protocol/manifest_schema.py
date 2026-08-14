"""Export the committed adapter binding profile JSON Schema and its shape baseline.

Two committed artifacts, two different jobs, and confusing them is how a
breaking profile change lands quietly:

* ``schema/adapter-profile.schema.json`` is the DRIFT gate. It proves the
  committed schema still matches the models. It says nothing about whether a
  change was compatible or whether it earned a version bump.
* ``schema/adapter-profile.baseline.json`` is the BUMP gate. It records the
  canonical validation shape at the current ``PROFILE_VERSION``, so a change
  that must bump cannot land at an unchanged version. Refreshing it is legal
  only in the commit that also bumps ``PROFILE_VERSION``, and
  ``tests/test_manifest_previous_shape.py`` asserts that pairing.

The change class table, which decides the bump:

* Add an OPTIONAL property: minor (1.0 to 1.1). The schema is closed, so a
  consumer on 1.0 rejects the new payload.
* Add a REQUIRED property, remove or rename one, change a type, tighten a
  ``pattern``, or make an optional property required: major (1.0 to 2.0). Each
  invalidates a file a conforming author had already written.
* Loosen a ``pattern``, widen an ``enum``, relax a bound: minor. Same closed
  schema reasoning as an added optional property.
* Edit a ``description``, a ``title`` or a ``$comment``: no bump. No shape
  change, which is why the baseline excludes both prose keywords.

Consumer acceptance is same major, less or equal minor
(``channel_protocol.manifest.check_version``).

``schema_export.py`` is deliberately NOT edited to carry this. The profile is a
build time declaration, not a wire message; a separate ``$id`` lets the two
version independently, and then each drift test names which contract broke.
"""

import json
import sys
from pathlib import Path
from typing import Any

from pydantic.json_schema import models_json_schema

from .manifest import (
    PROFILE_VERSION,
    AdapterConformance,
    AdapterCredentials,
    AdapterProfile,
    AddressShape,
)

SCHEMA_ID = "https://curietech.ai/schemas/adapter-profile.schema.json"

# Every model the drift gate (tests/test_manifest_schema_compat.py) can see. A
# model that is never listed here is invisible to it, so the committed schema
# would stay "current" while the profile it documents lost half its surface.
_MODELS = (
    AdapterProfile,
    AddressShape,
    AdapterCredentials,
    AdapterConformance,
)

# The keywords the bump gate records, per Section 2.1 of the profile's
# compatibility policy. `description` and `title` are excluded on purpose: a
# gate that reds on prose becomes noise, and noise gets refreshed reflexively,
# which is exactly how a real shape change gets waved through. The composition
# keywords are included so a property that changes which model it resolves to
# is recorded as the shape change it is.
_CANONICAL_KEYWORDS = frozenset(
    {
        "$ref",
        "additionalProperties",
        "allOf",
        "anyOf",
        "const",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "items",
        "maxItems",
        "maxLength",
        "maximum",
        "minItems",
        "minLength",
        "minimum",
        "oneOf",
        "pattern",
        "prefixItems",
        "required",
        "type",
    }
)


def schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "schema" / "adapter-profile.schema.json"


def baseline_path() -> Path:
    return Path(__file__).resolve().parents[2] / "schema" / "adapter-profile.baseline.json"


def build_schema() -> dict[str, Any]:
    _, top = models_json_schema(
        [(model, "validation") for model in _MODELS],
        ref_template="#/$defs/{model}",
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Curie Adapter Manifest v1.0",
        "manifestVersion": PROFILE_VERSION,
        **top,
    }


def render_schema() -> str:
    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"


def write_schema() -> Path:
    path = schema_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_schema(), encoding="utf-8")
    return path


def _canonical_constraint(node: Any) -> Any:
    """Keep only the keywords that decide whether a payload validates."""

    if isinstance(node, list):
        return [_canonical_constraint(member) for member in node]
    if not isinstance(node, dict):
        return node
    reduced: dict[str, Any] = {}
    for keyword in sorted(node):
        if keyword not in _CANONICAL_KEYWORDS:
            continue
        value = node[keyword]
        if keyword == "enum" and isinstance(value, list):
            reduced[keyword] = sorted(value, key=repr)
        else:
            reduced[keyword] = _canonical_constraint(value)
    return reduced


def canonical_shape(schema: dict[str, Any]) -> dict[str, Any]:
    """The validation shape of every exported model, prose stripped out.

    Deeper than a names and required list on purpose: a names only baseline
    stays green on exactly the changes the change class table says must bump,
    namely a tightened pattern, a changed type, a narrowed enum, a changed
    format or a moved bound.
    """

    return {
        name: {
            "required": sorted(definition.get("required", [])),
            "additionalProperties": definition.get("additionalProperties", True),
            "properties": {
                prop: _canonical_constraint(subschema)
                for prop, subschema in sorted(definition.get("properties", {}).items())
            },
        }
        for name, definition in sorted(schema.get("$defs", {}).items())
    }


def build_baseline() -> dict[str, Any]:
    return {"version": PROFILE_VERSION, "models": canonical_shape(build_schema())}


def render_baseline() -> str:
    return json.dumps(build_baseline(), indent=2, sort_keys=True) + "\n"


def write_baseline() -> Path:
    path = baseline_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_baseline(), encoding="utf-8")
    return path


if __name__ == "__main__":
    if "--write-baseline" in sys.argv[1:]:
        print(f"wrote {write_baseline()}")
    else:
        print(f"wrote {write_schema()}")
