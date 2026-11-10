"""New source administrative DTO grammar, @spec PROTECTED-HOOK-SOURCE-3."""

import importlib
import importlib.util
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError


def dto_module():
    """Expected missing-feature red, @spec PROTECTED-HOOK-SOURCE-3."""
    name = "curie_api.hook_source_policy_schemas"
    assert importlib.util.find_spec(name) is not None, "source administrative DTOs not implemented"
    return importlib.import_module(name)


def write_request():
    """Anonymous protected request, @spec PROTECTED-HOOK-SOURCE-3."""
    return {
        "expected_generation": "0",
        "operation_id": "22222222-2222-4222-8222-222222222222",
        "runtime_id": "33333333-3333-4333-8333-333333333333",
        "qualification_id": "44444444-4444-4444-8444-444444444444",
        "bundle_digest": "a" * 64,
    }


def no_row():
    """Absent-policy output, @spec PROTECTED-HOOK-SOURCE-3."""
    return {
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "hook": "incident-alert",
        "generation": "0",
        "mode": "ordinary",
        "tool_access": None,
        "runtime_id": None,
        "qualification_id": None,
        "bundle_digest": None,
        "legacy_generation": "17",
        "activation": "closed",
        "updated_at": None,
    }


@pytest.mark.parametrize("generation", ["0", "1", "9007199254740993", "9223372036854775807"])
def test_write_and_mutation_preserve_exact_decimal_strings(generation):
    """No JSON number precision loss, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = {**write_request(), "expected_generation": generation}
    assert module.HookSourcePolicyWrite.model_validate(value).model_dump(mode="json") == value
    mutation = {key: value[key] for key in ("expected_generation", "operation_id")}
    assert (
        module.HookSourcePolicyMutation.model_validate(mutation).model_dump(mode="json") == mutation
    )


@pytest.mark.parametrize(
    "bad",
    [
        0,
        True,
        1.0,
        None,
        "",
        "00",
        "01",
        "+1",
        "-1",
        " 1",
        "1 ",
        "1e2",
        "1.0",
        "١",
        "9223372036854775808",
    ],
)
def test_requests_refuse_noncanonical_generation_without_coercion(bad):
    """Strict request and query value grammar, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = {**write_request(), "expected_generation": bad}
    with pytest.raises(ValidationError):
        module.HookSourcePolicyWrite.model_validate(value)
    with pytest.raises(ValidationError):
        module.HookSourcePolicyMutation.model_validate(
            {"expected_generation": bad, "operation_id": value["operation_id"]}
        )


@pytest.mark.parametrize(
    "field,bad",
    [
        ("operation_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("operation_id", "22222222222242228222222222222222"),
        ("runtime_id", 3),
        ("runtime_id", None),
        ("qualification_id", "not-a-uuid"),
        ("qualification_id", True),
        ("bundle_digest", "sha256:" + "a" * 64),
        ("bundle_digest", "A" * 64),
        ("bundle_digest", "a" * 63),
        ("bundle_digest", 1),
    ],
)
def test_write_refuses_malformed_identity_and_bundle(field, bad):
    """Canonical IDs and bare digest, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    with pytest.raises(ValidationError):
        module.HookSourcePolicyWrite.model_validate({**write_request(), field: bad})


@pytest.mark.parametrize(
    "extra", ["tool_access", "mode", "secret", "consumer_credential", "activation"]
)
def test_request_does_not_accept_writable_authority_or_credentials(extra):
    """No implicit wider policy, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    with pytest.raises(ValidationError):
        module.HookSourcePolicyWrite.model_validate({**write_request(), extra: "example-value"})
    with pytest.raises(ValidationError):
        module.HookSourcePolicyMutation.model_validate(
            {
                "expected_generation": "0",
                "operation_id": write_request()["operation_id"],
                extra: "example-value",
            }
        )


@pytest.mark.parametrize(
    "field",
    ["expected_generation", "operation_id", "runtime_id", "qualification_id", "bundle_digest"],
)
def test_write_requires_every_declared_field(field):
    """No fallback configuration, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = write_request()
    del value[field]
    with pytest.raises(ValidationError):
        module.HookSourcePolicyWrite.model_validate(value)


@pytest.mark.parametrize("reason", [None, "pending_history"])
def test_no_row_output_preserves_closed_generation_zero(reason):
    """History is distinguishable without fake authority, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = {**no_row(), "refusal_reason": reason}
    output = module.HookSourcePolicyOut.model_validate(value).model_dump(mode="json")
    assert output == value
    assert not ({"operation_id", "secret", "consumer_credential"} & output.keys())


def test_row_output_serializes_utc_and_large_decimal_generation():
    """Audit time cannot introduce local offset, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = {
        **no_row(),
        "generation": "9007199254740993",
        "mode": "protected",
        "tool_access": "read-only",
        "runtime_id": write_request()["runtime_id"],
        "qualification_id": write_request()["qualification_id"],
        "bundle_digest": "a" * 64,
        "updated_at": datetime(2026, 10, 3, 9, 0, tzinfo=timezone(timedelta(hours=9))),
        "activation": "closed",
    }
    output = module.HookSourcePolicyOut.model_validate(value).model_dump(mode="json")
    assert output["generation"] == "9007199254740993"
    assert output["legacy_generation"] == "17"
    assert output["updated_at"] in ("2026-10-03T00:00:00Z", "2026-10-03T00:00:00+00:00")


@pytest.mark.parametrize("generation", ["1", "9223372036854775807"])
def test_scoped_secret_output_keeps_generation_string(generation):
    """New DTO does not expose numeric generation, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    value = {
        "agent_id": no_row()["agent_id"],
        "hook": "incident-alert",
        "generation": generation,
        "secret": "anonymous-source-key",
    }
    assert module.HookSourceSecretOut.model_validate(value).model_dump(mode="json") == value


@pytest.mark.parametrize("generation", ["0", 1, True, "01", "9223372036854775808"])
def test_scoped_secret_refuses_zero_or_noncanonical_generation(generation):
    """Source credentials name committed generations, @spec PROTECTED-HOOK-SOURCE-3."""
    module = dto_module()
    with pytest.raises(ValidationError):
        module.HookSourceSecretOut.model_validate(
            {
                "agent_id": no_row()["agent_id"],
                "hook": "incident-alert",
                "generation": generation,
                "secret": "anonymous-source-key",
            }
        )
