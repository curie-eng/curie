"""The API's promote-a-trace eval-case DTOs must conform to the frozen eval-case
schema the worker eval runner enforces (#500).

`EvalCaseOut` / `GraderOut` (`schemas.py`) are linked to the worker's frozen
`EvalCase` / `Grader` (committed at `apps/worker/schema/eval-cases.schema.json`)
only by a docstring. Nothing asserted that a case the promote endpoint mints
actually validates against that schema, so the API could drift -- e.g. a new
`GraderKind` added to the schema but not to `GraderOut`'s hard-coded literal, or
a field added on one side only. This gate validates emitted cases against the
committed schema and pins the field sets, so drift fails loudly.

The API package deliberately does not import the worker, so the schema is read
from its committed file by path, not imported.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api.schemas import EvalCaseOut, GraderOut
from jsonschema import Draft202012Validator

_SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "apps" / "worker" / "schema" / "eval-cases.schema.json"
)


def _schema() -> dict:
    return json.loads(_SCHEMA_PATH.read_text())


def _validator_for(def_name: str) -> Draft202012Validator:
    schema = _schema()
    # Validate an instance against one $def while keeping the sibling $defs
    # resolvable (Grader references GraderKind, EvalCase references Grader).
    return Draft202012Validator({"$ref": f"#/$defs/{def_name}", "$defs": schema["$defs"]})


@pytest.mark.parametrize("kind", ["exact", "contains", "regex", "tool_called"])
def test_emitted_eval_case_validates_against_the_frozen_schema(kind: str) -> None:
    case = EvalCaseOut(
        id="c1",
        input="what is the weather?",
        grader=GraderOut(kind=kind, expected="sunny", case_sensitive=False),
    )
    errors = sorted(_validator_for("EvalCase").iter_errors(case.model_dump()), key=str)
    messages = [e.message for e in errors]
    assert not errors, f"promote-emitted EvalCase failed the frozen schema: {messages}"


def test_grader_kinds_match_the_schema_enum() -> None:
    # A new grader kind added to the worker schema but not to GraderOut's literal
    # (or vice versa) is a silent divergence -- the API would mint a kind the
    # runner rejects, or reject a kind the runner accepts.
    schema_kinds = set(_schema()["$defs"]["GraderKind"]["enum"])
    api_kinds = set(GraderOut.model_fields["kind"].annotation.__args__)
    assert api_kinds == schema_kinds, (
        f"GraderOut.kind literals {sorted(api_kinds)} diverge from the schema "
        f"GraderKind enum {sorted(schema_kinds)}"
    )


def test_expect_status_values_match_the_schema_enum() -> None:
    # A terminal status added to the worker schema's ExpectedStatus but not to
    # EvalCaseOut's literal (or vice versa) is a silent divergence -- the promote
    # endpoint would mint a status the runner rejects, or reject one it accepts.
    schema_statuses = set(_schema()["$defs"]["ExpectedStatus"]["enum"])
    api_statuses = set(EvalCaseOut.model_fields["expect_status"].annotation.__args__)
    assert api_statuses == schema_statuses, (
        f"EvalCaseOut.expect_status literals {sorted(api_statuses)} diverge from the "
        f"schema ExpectedStatus enum {sorted(schema_statuses)}"
    )


def test_eval_case_field_names_match_the_schema() -> None:
    schema = _schema()
    for model, def_name in ((EvalCaseOut, "EvalCase"), (GraderOut, "Grader")):
        schema_props = set(schema["$defs"][def_name]["properties"])
        model_fields = set(model.model_fields)
        assert model_fields == schema_props, (
            f"{model.__name__} fields {sorted(model_fields)} diverge from schema "
            f"{def_name} properties {sorted(schema_props)}"
        )
