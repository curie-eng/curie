"""The artifact-sync gate: committed schema, Rust and .env.example match the models.

These tests check *artifact sync* only: if a model changes without regenerating
the committed schema, Rust or .env.example block, they fail. Regenerate with
``scripts/check-contracts.sh`` (or the three module entry points) and commit the
result. They do not, and cannot, judge backward compatibility -- a model change
regenerates both sides and stays green. Compatibility is a policy defined by the
semver change-class table (packages/CLAUDE.md); an *unbumped* wire change is
caught separately by the wire-lock gate in ``tests/test_wire_lock.py``.

The generated TypeScript compiles under tsc in CI (it needs a Node toolchain, so
it is not regenerated here); its input is the same committed schema this gate
pins, so a drifted schema is caught here before TypeScript can diverge.
"""

from aci_protocol.env_example_export import env_example_path, render_env_example
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


def test_committed_env_example_is_current() -> None:
    committed = env_example_path().read_text(encoding="utf-8")
    assert render_env_example(committed) == committed, (
        ".env.example boot env block is stale; run scripts/check-contracts.sh and commit"
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
    assert schema["protocolVersion"] == "0.5.20"

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


def test_memory_writes_is_an_optional_nullable_boolean_boot_env_field() -> None:
    boot_env = build_schema()["$defs"]["BootEnv"]
    assert "memory_writes" not in boot_env.get("required", [])
    field = boot_env["properties"]["memory_writes"]
    assert {variant["type"] for variant in field["anyOf"]} == {"boolean", "null"}
    assert field["default"] is None
    assert field["env"] == "CURIE_MEMORY_WRITES"
    assert field["producer"] == ["worker"]


def test_channel_bound_is_optional_on_boot_env_and_absent_from_session_config() -> None:
    definitions = build_schema()["$defs"]
    boot_env = definitions["BootEnv"]
    assert "channel_bound" in boot_env["properties"]
    assert "channel_bound" not in boot_env.get("required", [])
    assert "channel_bound" not in definitions["SessionConfig"]["properties"]


def test_attachments_manifest_is_an_optional_boot_env_string() -> None:
    """ADR 0205: a runner-local boot input outside the frozen SessionConfig."""
    definitions = build_schema()["$defs"]
    boot_env = definitions["BootEnv"]
    assert "attachments_manifest" not in boot_env.get("required", [])
    field = boot_env["properties"]["attachments_manifest"]
    assert {variant["type"] for variant in field["anyOf"]} == {"string", "null"}
    assert field["default"] is None
    assert field["env"] == "CURIE_ATTACHMENTS_MANIFEST"
    assert field["producer"] == ["kernel", "substrate"]
    assert "attachments_manifest" not in definitions["SessionConfig"]["properties"]
