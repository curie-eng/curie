"""Exporter regressions: the version guard must survive the Literal removal.

Decision 5 -- the highest-risk silent failure in this change. Both exporters
detect the version field by introspecting the single-valued Literal. Once
``version`` becomes a plain ``str``, that introspection stops matching and the
guard vanishes green: the schema quietly makes ``version`` optional, and the Rust
reader quietly stops checking it. These tests assert the rendered artifacts, not
the private helpers, because the rendered artifact is the real contract.
"""

import re

from aci_protocol import ADOPTION_CREDENTIAL_MAX_CHARS, BootEnv
from aci_protocol.rust_export import render_rust
from aci_protocol.schema_export import build_schema


def test_version_field_is_required_in_the_exported_schema() -> None:
    final = build_schema()["$defs"]["Final"]
    assert "version" in final["required"]
    # It is no longer a const; it is a semver-pattern string. A const value would
    # re-pin the exact version and defeat the whole compatibility range.
    assert "const" not in final["properties"]["version"]


def test_event_session_context_fields_are_optional_nullable_strings() -> None:
    event = build_schema()["$defs"]["Event"]
    for field in ("session_id", "history_ref"):
        assert field not in event["required"]
        prop = event["properties"][field]
        assert "anyOf" in prop
        assert {variant["type"] for variant in prop["anyOf"]} == {"string", "null"}


def test_event_adoption_credential_is_optional_nullable_string() -> None:
    event = build_schema()["$defs"]["Event"]
    assert "adoption_credential" not in event["required"]
    prop = event["properties"]["adoption_credential"]
    assert "anyOf" in prop
    assert {variant["type"] for variant in prop["anyOf"]} == {"string", "null"}
    string_variant = next(variant for variant in prop["anyOf"] if variant["type"] == "string")
    assert string_variant["minLength"] == 1
    assert string_variant["maxLength"] == ADOPTION_CREDENTIAL_MAX_CHARS
    assert string_variant["pattern"] == r".*\S.*"


def test_outbound_adoption_applied_is_optional_nullable_bool() -> None:
    final = build_schema()["$defs"]["Final"]
    assert "adoption_applied" not in final["required"]
    prop = final["properties"]["adoption_applied"]
    assert "anyOf" in prop
    assert {variant["type"] for variant in prop["anyOf"]} == {"boolean", "null"}


def test_generated_rust_guards_the_version() -> None:
    rust = render_rust()
    lines = rust.splitlines()
    version_fields = [i for i, line in enumerate(lines) if line.strip() == "version: String,"]
    assert version_fields, "no version field found in the generated Rust"
    for i in version_fields:
        preceding = lines[i - 1].strip()
        # The compatibility deserializer must decorate every version field, and
        # #[serde(default)] must NOT -- a defaulted version is the silent gutting.
        assert preceding == '#[serde(deserialize_with = "require_compatible_protocol_version")]', (
            f"version field is not guarded; preceding line was {preceding!r}"
        )


def test_generated_rust_requires_nullable_reply_placeholder_key() -> None:
    rust = render_rust()
    reply_handle = re.search(r"pub struct ReplyHandle \{\n(.*?)\n\}", rust, re.DOTALL)
    assert reply_handle, "no ReplyHandle struct in the generated Rust"
    assert (
        '#[serde(deserialize_with = "deserialize_required_nullable")]\n'
        "    pub placeholder: Option<String>,"
    ) in reply_handle.group(1)


def test_generated_rust_carries_reply_handle_behavior_tests() -> None:
    rust = render_rust()
    assert "fn reply_handle_accepts_explicit_null_placeholder()" in rust
    assert "fn reply_handle_rejects_omitted_placeholder()" in rust


# ---------------------------------------------------------------------------
# BootEnv export (#488). The env key (CURIE_RUNNER_TOKEN) is not the field
# name (runner_token), so the mapping has to ride in the schema for codegen to
# emit constants the Rust CLI and the chart assert can pin against.
# ---------------------------------------------------------------------------

_ENV_KEYS_MODULE_RE = re.compile(r"pub mod env_keys \{\n(.*?)\n\}", re.DOTALL)


def _env_keys_module() -> str:
    match = _ENV_KEYS_MODULE_RE.search(render_rust())
    assert match, "no `pub mod env_keys` module in the generated Rust"
    return match.group(1)


def test_boot_env_is_in_the_exported_schema() -> None:
    boot = build_schema()["$defs"]["BootEnv"]
    # The frozen SessionConfig is composed as a field, not flattened in.
    assert "session" in boot["properties"]
    assert "session" in boot["required"]


def test_the_schema_carries_the_env_key_for_every_boot_field() -> None:
    """Decision 2: the key mapping is machine-readable, so codegen can emit it.

    Without the key in the schema, the Rust constants and the chart assert have
    to retype the literals -- the third declaration site this change exists to
    prevent.
    """
    props = build_schema()["$defs"]["BootEnv"]["properties"]
    declared = {prop["env"] for prop in props.values() if "env" in prop}
    # `session` nests SessionConfig rather than carrying a key of its own.
    assert declared, "no field in the exported BootEnv carries an `env` key"
    assert declared <= set(BootEnv.env_keys())


def test_the_schema_carries_the_producers_alongside_every_env_key() -> None:
    """Producer ownership is part of the exported contract, not just runtime.

    The boot env has four producers and one consumer; the tag is what lets a
    render surface be checked against the keys it is allowed to write. An
    untagged key exported to Rust would be a key nobody owns.

    ``producer`` is a COLLECTION: a key may have several producers (the chart
    sets ANTHROPIC_BASE_URL as a fallback and the worker overrides it), so a
    single scalar tag cannot express the tree as it actually is.
    """
    props = build_schema()["$defs"]["BootEnv"]["properties"]
    keyed = [prop for prop in props.values() if "env" in prop]
    assert keyed, "no field in the exported BootEnv carries an `env` key"
    for prop in keyed:
        producers = prop.get("producer")
        assert isinstance(producers, list) and producers, (
            f"{prop['env']} carries no producer list: {producers!r}"
        )
        assert set(producers) <= {"worker", "kernel", "substrate", "operator"}, (
            f"{prop['env']} carries an unknown producer: {producers!r}"
        )


def test_the_schema_forbids_the_worker_from_owning_sandbox_identity_or_port() -> None:
    """The anti-clobber finding, pinned at the exported-contract level too.

    A `producer: worker` tag on either key would license exactly the render the
    worker golden forbids (envVarsInjectionPolicy: Overrides, values.yaml:789).
    """
    for key in ("CURIE_SANDBOX_ID", "CURIE_RUNNER_PORT"):
        assert key not in BootEnv.env_keys(producer="worker")
        assert key in BootEnv.env_keys(producer="substrate")


def test_generated_rust_exports_a_const_per_declared_env_key() -> None:
    module = _env_keys_module()
    for key in BootEnv.env_keys():
        assert f'pub const {key}: &str = "{key}";' in module, (
            f"{key} has no constant in the generated env_keys module"
        )


def test_generated_env_keys_module_declares_nothing_undeclared() -> None:
    """A stale constant left behind by a removed field is a dead pin."""
    emitted = re.findall(r"pub const (\w+): &str", _env_keys_module())
    assert sorted(emitted) == sorted(BootEnv.env_keys())


def test_generated_env_keys_module_is_sorted() -> None:
    """Ordering must not depend on dict iteration, or the drift gate flaps.

    check-contracts.sh regenerates and runs `git diff --exit-code`; a const
    module ordered by field-declaration order would churn on every reorder.
    """
    emitted = re.findall(r"pub const (\w+): &str", _env_keys_module())
    assert emitted == sorted(emitted)


def test_generated_env_keys_module_is_deterministic() -> None:
    assert render_rust() == render_rust()


def test_generated_env_keys_are_flattened_to_include_session_and_otel_keys() -> None:
    """The chart bakes CURIE_RUNNER_PORT and the OTel keys into the runner
    container itself (agent-sandbox.yaml:430, 433, 435). A const list missing
    the nested SessionConfig/OTel keys fails a default render's subset assert.
    """
    module = _env_keys_module()
    for key in (
        "CURIE_PLUGIN_DIR",
        "CURIE_SESSION_ID",
        "CURIE_SANDBOX_ID",
        "CURIE_BUDGET",
        "CURIE_MEMORY_REF",
        "CURIE_CREDENTIALS",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "CURIE_RUNNER_PORT",
    ):
        assert f'pub const {key}: &str = "{key}";' in module, (
            f"{key} is reachable in the runner container but absent from env_keys"
        )
