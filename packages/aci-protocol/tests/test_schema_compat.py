"""The artifact-sync gate: committed schema and generated Rust match the models.

These tests check *artifact sync* only: if a model changes without regenerating
the committed schema or Rust, they fail. Regenerate with
``scripts/check-contracts.sh`` (or the two module entry points) and commit the
result. They do not, and cannot, judge backward compatibility -- a model change
regenerates both sides and stays green. Compatibility is a policy defined by the
semver change-class table (packages/CLAUDE.md); an *unbumped* wire change is
caught separately by the wire-lock gate in ``tests/test_wire_lock.py``.

The generated TypeScript compiles under tsc in CI (it needs a Node toolchain, so
it is not regenerated here); its input is the same committed schema this gate
pins, so a drifted schema is caught here before TypeScript can diverge.
"""

from pathlib import Path

from aci_protocol.rust_export import crate_dir, render_rust
from aci_protocol.schema_export import build_schema, render_schema, schema_path


def test_committed_json_schema_is_current() -> None:
    committed = schema_path().read_text(encoding="utf-8")
    assert render_schema() == committed, (
        "aci-protocol JSON Schema is stale; run scripts/check-contracts.sh and commit"
    )


def test_committed_rust_is_current() -> None:
    committed = (crate_dir() / "src" / "lib.rs").read_text(encoding="utf-8")
    assert render_rust() == committed, (
        "generated Rust is stale; run scripts/check-contracts.sh and commit"
    )


def test_reply_placeholders_are_required_nullable_strings() -> None:
    definitions = build_schema()["$defs"]
    reply_handle = definitions["ReplyHandle"]
    approval_request = definitions["ApprovalRequest"]

    assert "placeholder" in reply_handle["required"]
    assert "reply_placeholder" in approval_request["required"]

    handle_placeholder = reply_handle["properties"]["placeholder"]
    approval_placeholder = approval_request["properties"]["reply_placeholder"]
    assert "anyOf" in handle_placeholder
    assert "anyOf" in approval_placeholder

    handle_variants = handle_placeholder["anyOf"]
    approval_variants = approval_placeholder["anyOf"]
    assert {variant["type"] for variant in handle_variants} == {"null", "string"}
    assert {variant["type"] for variant in approval_variants} == {"null", "string"}

    handle_string = next(variant for variant in handle_variants if variant["type"] == "string")
    approval_string = next(
        variant for variant in approval_variants if variant["type"] == "string"
    )
    assert "minLength" not in handle_string
    assert approval_string["minLength"] == 1


def test_publication_context_is_an_optional_event_field_with_required_contents() -> None:
    schema = build_schema()
    assert schema["protocolVersion"] == "0.5.2"

    definitions = schema["$defs"]
    event = definitions["Event"]
    assert "publication_context" not in event["required"]
    variants = event["properties"]["publication_context"]["anyOf"]
    assert {"$ref": "#/$defs/PublicationContext"} in variants
    assert {"type": "null"} in variants

    context = definitions["PublicationContext"]
    assert set(context["required"]) == {
        "agent_id",
        "deployment_id",
        "work_item_id",
        "execution_request_id",
        "runtime_epoch",
        "conversation_id",
        "lineage_id",
        "lineage_version",
        "expected_head",
        "queued_event_id",
        "precheck_url",
        "capability",
        "observed_title",
        "observed_body_sha256",
        "observed_at",
    }


def test_publication_context_is_present_in_both_generated_language_artifacts() -> None:
    typescript = (
        Path(__file__).resolve().parents[1] / "generated" / "ts" / "aci-protocol.ts"
    ).read_text(encoding="utf-8")
    rust = (crate_dir() / "src" / "lib.rs").read_text(encoding="utf-8")

    assert "export interface PublicationContext {" in typescript
    assert "publication_context?:" in typescript
    assert "pub struct PublicationContext {" in rust
    assert "publication_context: Option<PublicationContext>" in rust
