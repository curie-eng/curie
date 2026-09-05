"""Bootstrap boot-env contract (#1492, ADR-0116 decision 2, ADR-0122).

This package declares the key and its admission bounds. It does not implement
bootstrap mode, adoption, retirement, pools, or any runtime authority check; a
declared boot field is not those behaviors (see the field's docstring for the
consumer authority policy the realizing runner enforces).
"""

from __future__ import annotations

import json
import re

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    READER_CONTEXT,
    RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS,
    BootEnv,
    Budget,
    SessionConfig,
    is_compatible,
)
from aci_protocol.env_example_export import render_block
from aci_protocol.rust_export import render_rust
from aci_protocol.schema_export import build_schema
from pydantic import ValidationError

# Distinctive fixture material, not a live credential. Every redaction test
# asserts this exact string never reaches a repr, str, or error surface.
_TOKEN = "runner-bootstrap-token-fixture-PLACEHOLDER"
_KEY = "CURIE_RUNNER_BOOTSTRAP_TOKEN"
_FIELD = "runner_bootstrap_token"

_BUDGET = Budget(max_output_tokens_per_run=4096, max_usd_per_day=5.0)

# A cold, bound pod's env as the worker + substrate write it today: a
# per-conversation CURIE_RUNNER_TOKEN and no bootstrap key at all.
_COLD_ENV: dict[str, str] = {
    "CURIE_PLUGIN_DIR": "/plugins/bundle",
    "CURIE_SESSION_ID": "sess-abc",
    "CURIE_SANDBOX_ID": "curie-sandbox-abc123",
    "CURIE_BUDGET": _BUDGET.model_dump_json(),
    "CURIE_RUNNER_PORT": "8080",
    "CURIE_RUNNER_TOKEN": "rt-per-conversation-fixture",
}


def _session() -> SessionConfig:
    return SessionConfig(
        plugin_dir="/plugins/bundle",
        session_id="sess-abc",
        sandbox_id="curie-sandbox-abc123",
        budget=_BUDGET,
    )


def _warm_env(**overrides: str) -> dict[str, str]:
    """A warm pool pod's env: substrate keys only, no per-conversation token."""
    env = {k: v for k, v in _COLD_ENV.items() if k != "CURIE_RUNNER_TOKEN"}
    env[_KEY] = _TOKEN
    env.update(overrides)
    return env


def _surfaces(exc: BaseException) -> str:
    """Every text a caller could log or interpolate from a raised error."""
    parts = [str(exc), repr(exc), f"{exc}"]
    if isinstance(exc, ValidationError):
        parts.append(repr(exc.errors()))
        parts.append(exc.json())
    return "\n".join(parts)


# --- Compatibility: omitted, empty, and the cold path are byte-identical -----


def test_omitted_key_parses_to_none_and_renders_nothing() -> None:
    boot = BootEnv.from_env(_COLD_ENV)
    assert boot.runner_bootstrap_token is None
    assert boot.runner_token == "rt-per-conversation-fixture"
    assert _KEY not in boot.to_env()


def test_declared_but_empty_fails_closed_unlike_every_other_boot_var() -> None:
    """Deliberately NOT the ``_str_or_none`` rule ``runner_token`` follows.

    ``CURIE_RUNNER_TOKEN`` treats empty as unset so a local/fake sandbox never
    presents an empty bearer. The bootstrap key has no such producer: it comes
    only from a pool Secret, so an empty value is a mis-rendered ``secretKeyRef``,
    and decoding it as "neither token present" would boot a warm pod whose
    gated routes pass through. Absent stays the compatible legacy case.
    """
    with pytest.raises(ValidationError) as exc:
        BootEnv.from_env(_warm_env(**{_KEY: ""}))
    assert [err["loc"] for err in exc.value.errors()] == [(_FIELD,)]
    assert "malformed runner bootstrap token" in str(exc.value)
    assert BootEnv.from_env(_COLD_ENV | {"CURIE_RUNNER_TOKEN": ""}).runner_token is None


def test_cold_path_with_both_keys_parses_both_verbatim() -> None:
    """The contract carries both; precedence is the consumer's policy.

    ``runner_token`` present means the bound per-conversation mode and the
    bootstrap is never admitted. That policy is documented on the field and
    realized by the runner, not by this parse, which must not silently drop
    either value (dropping the bootstrap here would hide a misconfigured pool
    from the consumer that has to refuse it).
    """
    boot = BootEnv.from_env(_COLD_ENV | {_KEY: _TOKEN})
    assert boot.runner_token == "rt-per-conversation-fixture"
    assert boot.runner_bootstrap_token == _TOKEN


def test_default_construction_is_none() -> None:
    assert BootEnv(session=_session()).runner_bootstrap_token is None


# --- The new env round trip ---------------------------------------------------


def test_warm_env_roundtrips_through_from_env_and_to_env() -> None:
    boot = BootEnv.from_env(_warm_env())
    assert boot.runner_bootstrap_token == _TOKEN
    assert boot.runner_token is None
    rendered = boot.to_env()
    assert rendered[_KEY] == _TOKEN
    assert "CURIE_RUNNER_TOKEN" not in rendered
    assert BootEnv.from_env(rendered) == boot


def test_env_key_accessor_names_the_key() -> None:
    assert BootEnv.env_key(_FIELD) == _KEY
    assert _KEY in BootEnv.env_keys()


def test_the_worker_render_surface_never_emits_the_bootstrap_key() -> None:
    """Producer is the substrate (per-pool Secret), never the per-claim render."""
    env = BootEnv.render_worker(
        plugin_dir="/plugins/bundle",
        session_id="sess-abc",
        budget=_BUDGET,
        memory_ref="s3://memory/agent",
        history_ref="s3://history/thread",
        runner_token="rt-per-conversation-fixture",
    )
    assert _KEY not in env
    assert _KEY not in BootEnv.env_keys(producer="worker")
    assert _KEY not in BootEnv.env_keys(producer="kernel")
    assert _KEY not in BootEnv.env_keys(producer="operator")
    assert _KEY in BootEnv.env_keys(producer="substrate")


# --- Secrecy: repr, str, and every validation-error surface -------------------


def test_repr_and_str_omit_the_token() -> None:
    boot = BootEnv.from_env(_warm_env())
    assert _TOKEN not in repr(boot)
    assert _TOKEN not in str(boot)
    assert _FIELD not in repr(boot)


@pytest.mark.parametrize(
    "raw",
    (
        " ",
        "   ",
        "\t\n",
        " ",
        _TOKEN + "x" * (RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS - len(_TOKEN) + 1),
    ),
    ids=("space", "spaces", "tab-newline", "nbsp", "oversize"),
)
def test_present_but_malformed_env_value_fails_closed_without_echo(raw: str) -> None:
    """A malformed present value must refuse the boot, not degrade to tokenless.

    Degrading would turn a mis-rendered pool Secret into an unauthenticated
    warm pod. The material is never attached to the error.
    """
    with pytest.raises(ValidationError) as exc:
        BootEnv.from_env(_warm_env(**{_KEY: raw}))
    rendered = _surfaces(exc.value)
    assert _TOKEN not in rendered
    assert raw.strip() == "" or raw not in rendered
    assert "malformed runner bootstrap token" in rendered
    assert [err["loc"] for err in exc.value.errors()] == [(_FIELD,)]


def test_oversize_by_one_is_rejected_and_at_the_bound_is_admitted() -> None:
    at_bound = "b" * RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS
    assert BootEnv.from_env(_warm_env(**{_KEY: at_bound})).runner_bootstrap_token == at_bound
    with pytest.raises(ValidationError):
        BootEnv.from_env(_warm_env(**{_KEY: at_bound + "b"}))


@pytest.mark.parametrize(
    "value",
    (
        1,
        True,
        b"bytes",
        [_TOKEN],
        {"nested": _TOKEN},
        "",
        "   ",
        _TOKEN + "x" * (RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS - len(_TOKEN) + 1),
    ),
    ids=("int", "bool", "bytes", "list", "dict", "empty", "blank", "oversize"),
)
def test_direct_construction_rejects_malformed_values_without_echo(value: object) -> None:
    """The model path (a producer building the union, or a JSON decode)."""
    with pytest.raises(ValidationError) as exc:
        BootEnv(session=_session(), runner_bootstrap_token=value)  # type: ignore[arg-type]
    rendered = _surfaces(exc.value)
    assert _TOKEN not in rendered
    assert "malformed runner bootstrap token" in rendered
    assert [err["loc"] for err in exc.value.errors()] == [(_FIELD,)]


@pytest.mark.parametrize(
    "value",
    (
        _TOKEN + "x" * (RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS - len(_TOKEN) + 1),
        [_TOKEN],
        {"nested": _TOKEN},
        4711,
        "",
        "   ",
    ),
    ids=("oversize", "list", "dict", "int", "empty", "blank"),
)
def test_model_validate_json_redacts_a_malformed_token(value: object) -> None:
    raw = json.dumps({"session": _session().model_dump(), _FIELD: value})
    with pytest.raises(ValidationError) as exc:
        BootEnv.model_validate_json(raw)
    rendered = _surfaces(exc.value)
    assert _TOKEN not in rendered
    assert "4711" not in rendered
    assert "malformed runner bootstrap token" in rendered
    assert [err["loc"] for err in exc.value.errors()] == [(_FIELD,)]


def test_model_validate_json_explicit_null_is_none() -> None:
    raw = json.dumps({"session": _session().model_dump(), _FIELD: None})
    assert BootEnv.model_validate_json(raw).runner_bootstrap_token is None


def _context_chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    cur: BaseException | None = exc
    while cur is not None:
        out.append(cur)
        cur = cur.__context__ or cur.__cause__
    return out[1:]


@pytest.mark.parametrize(
    "build",
    (
        lambda: BootEnv.model_validate_json(
            '{"session": {}, "' + _FIELD + '": "' + _TOKEN + '", bad}'
        ),
        lambda: BootEnv.model_validate_json(
            json.dumps({"session": {"plugin_dir": "/p"}, _FIELD: _TOKEN})
        ),
        lambda: BootEnv(session=_session(), runner_bootstrap_token=[_TOKEN]),  # type: ignore[arg-type]
        lambda: BootEnv.from_env(
            _warm_env(**{_KEY: _TOKEN + "x" * RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS})
        ),
    ),
    ids=("invalid-json", "unrelated-error", "ctor-list", "env-oversize"),
)
def test_no_chained_exception_retains_the_material(build: object) -> None:
    """``raise ... from None`` only hides the chain; the original error would
    still hang off ``__context__``. The redacted error is raised outside the
    except block so an introspector that ignores ``__suppress_context__``
    finds nothing behind it."""
    with pytest.raises(ValidationError) as exc:
        build()  # type: ignore[operator]
    for chained in _context_chain(exc.value):
        assert _TOKEN not in _surfaces(chained)
    assert _context_chain(exc.value) == []


def test_invalid_json_never_echoes_the_raw_input() -> None:
    raw = '{"session": {}, "' + _FIELD + '": "' + _TOKEN + '", bad}'
    with pytest.raises(ValidationError) as exc:
        BootEnv.model_validate_json(raw)
    assert _TOKEN not in _surfaces(exc.value)


def test_unrelated_errors_keep_their_diagnosis_but_lose_the_material() -> None:
    """A missing session field is still reported as such; the token is scrubbed."""
    with pytest.raises(ValidationError) as exc:
        BootEnv.model_validate({"session": {"plugin_dir": "/p"}, _FIELD: _TOKEN})
    rendered = _surfaces(exc.value)
    assert "session_id" in rendered
    assert _TOKEN not in rendered


def test_a_well_formed_token_with_an_unrelated_error_is_not_misdiagnosed() -> None:
    with pytest.raises(ValidationError) as exc:
        BootEnv.model_validate({"session": {"plugin_dir": "/p"}, _FIELD: _TOKEN})
    assert "malformed runner bootstrap token" not in str(exc.value)


# --- Old-consumer compatibility and the version class --------------------------


def test_reader_context_tolerates_fields_a_consumer_does_not_model() -> None:
    """The strict-producer/tolerant-consumer rule that makes an optional field a patch."""
    payload = {"session": _session().model_dump(), "field_from_the_future": 1}
    assert BootEnv.model_validate(payload, context=READER_CONTEXT).runner_bootstrap_token is None
    with pytest.raises(ValidationError):
        BootEnv.model_validate(payload)


def test_protocol_version_is_the_compatible_patch_over_0_4_5() -> None:
    assert PROTOCOL_VERSION == "0.4.6"
    assert is_compatible("0.4.5", PROTOCOL_VERSION)
    assert is_compatible(PROTOCOL_VERSION, "0.4.5")
    assert not is_compatible("0.5.0", PROTOCOL_VERSION)


def test_schema_declares_an_optional_bounded_nullable_string() -> None:
    boot = build_schema()["$defs"]["BootEnv"]
    prop = boot["properties"][_FIELD]
    assert _FIELD not in boot.get("required", [])
    assert prop["default"] is None
    assert prop["env"] == _KEY
    assert prop["producer"] == ["substrate"]
    string_branch = next(b for b in prop["anyOf"] if b.get("type") == "string")
    assert string_branch["minLength"] == 1
    assert string_branch["maxLength"] == RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS
    assert re.compile(string_branch["pattern"]).search("a")
    assert not re.compile(string_branch["pattern"]).search("   ")
    assert {"type": "null"} in prop["anyOf"]


# --- Exporter completeness ----------------------------------------------------


def test_env_example_block_documents_the_key_as_substrate_owned() -> None:
    line = next(line for line in render_block().splitlines() if _KEY in line)
    assert line.endswith("producers: substrate")


def test_generated_rust_carries_the_constant_and_redacts_the_field() -> None:
    rust = render_rust()
    assert f'pub const {_KEY}: &str = "{_KEY}";' in rust
    assert 'deserialize_with = "deserialize_runner_bootstrap_token"' in rust
    assert "impl std::fmt::Debug for BootEnv" in rust
    assert (
        '"runner_bootstrap_token",\n                &self.runner_bootstrap_token'
        '.as_ref().map(|_| "<redacted>")'
    ) in rust
    assert "malformed runner bootstrap token" in rust
    assert f"{RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS}" in rust
    # Non-strings are decoded through serde_json::Value so a scalar cannot be
    # echoed by serde's own "invalid type: integer `...`" error.
    assert "Option::<serde_json::Value>::deserialize(deserializer)" in rust
