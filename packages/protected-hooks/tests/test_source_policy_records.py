"""Pure source decision encodings, @spec PROTECTED-HOOK-SOURCE-6/10."""

import copy
import hashlib
import importlib
import importlib.util

import pytest


def records_module():
    """Expected missing-feature red, @spec PROTECTED-HOOK-SOURCE-6/10."""
    name = "curie_protected_hooks.source_policy_records"
    assert importlib.util.find_spec(name) is not None, "source policy records not implemented"
    return importlib.import_module(name)


def target():
    """Anonymous desired configuration, @spec PROTECTED-HOOK-SOURCE-10."""
    return {
        "mode": "protected",
        "tool_access": "read-only",
        "runtime_id": "33333333-3333-4333-8333-333333333333",
        "qualification_id": "44444444-4444-4444-8444-444444444444",
        "bundle_digest": "a" * 64,
    }


def policy():
    """Committed wire snapshot, @spec PROTECTED-HOOK-SOURCE-6."""
    return {
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "hook": "incident-alert",
        "generation": "9007199254740993",
        "operation_id": "22222222-2222-4222-8222-222222222222",
        **target(),
        "legacy_generation": "17",
    }


@pytest.mark.parametrize("ordinary", [False, True])
def test_intent_independent_literal_vector(ordinary):
    """Exact target bytes, @spec PROTECTED-HOOK-SOURCE-10."""
    module = records_module()
    value = target()
    if ordinary:
        value = dict.fromkeys(value)
        value["mode"] = "ordinary"
        wire = (
            b'{"bundle_digest":null,"mode":"ordinary","qualification_id":null,'
            b'"runtime_id":null,"tool_access":null}'
        )
    else:
        wire = (
            b'{"bundle_digest":"'
            + b"a" * 64
            + b'","mode":"protected","qualification_id":"44444444-4444-4444-8444-444444444444",'
            b'"runtime_id":"33333333-3333-4333-8333-333333333333","tool_access":"read-only"}'
        )
    before = copy.deepcopy(value)
    assert module.target_intent_sha256(value) == hashlib.sha256(wire).hexdigest()
    assert value == before
    assert (
        module.target_intent_sha256(dict(reversed(list(value.items()))))
        == hashlib.sha256(wire).hexdigest()
    )


def test_fingerprint_independent_literal_vector():
    """Ten decision fields only, @spec PROTECTED-HOOK-SOURCE-6."""
    module = records_module()
    value = policy()
    wire = (
        b'{"agent_id":"11111111-1111-4111-8111-111111111111","bundle_digest":"'
        + b"a" * 64
        + b'","generation":"9007199254740993","hook":"incident-alert","legacy_generation":"17",'
        b'"mode":"protected","operation_id":"22222222-2222-4222-8222-222222222222",'
        b'"qualification_id":"44444444-4444-4444-8444-444444444444",'
        b'"runtime_id":"33333333-3333-4333-8333-333333333333","tool_access":"read-only"}'
    )
    before = copy.deepcopy(value)
    assert module.policy_fingerprint(value) == hashlib.sha256(wire).hexdigest()
    assert value == before
    for audit in ("2026-10-03T00:00:00Z", "2026-10-04T00:00:00Z"):
        assert (
            module.policy_fingerprint({**value, "updated_at": audit})
            == hashlib.sha256(wire).hexdigest()
        )
    assert (
        module.policy_fingerprint(dict(reversed(list(value.items()))))
        == hashlib.sha256(wire).hexdigest()
    )


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("agent_id", "55555555-5555-4555-8555-555555555555"),
        ("hook", "other-hook"),
        ("generation", "9007199254740994"),
        ("operation_id", "66666666-6666-4666-8666-666666666666"),
        ("runtime_id", "77777777-7777-4777-8777-777777777777"),
        ("qualification_id", "88888888-8888-4888-8888-888888888888"),
        ("bundle_digest", "b" * 64),
        ("legacy_generation", "18"),
    ],
)
def test_each_binding_changes_fingerprint(field, replacement):
    """No omitted authority binding, @spec PROTECTED-HOOK-SOURCE-6."""
    module = records_module()
    original = policy()
    assert module.policy_fingerprint(original) != module.policy_fingerprint(
        {**original, field: replacement}
    )


def test_ordinary_tombstone_and_maximum_generation():
    """Explicit nulls and full BIGINT range, @spec PROTECTED-HOOK-SOURCE-6/10."""
    module = records_module()
    ordinary = {
        "mode": "ordinary",
        "tool_access": None,
        "runtime_id": None,
        "qualification_id": None,
        "bundle_digest": None,
    }
    value = {**policy(), **ordinary, "generation": "9223372036854775807", "legacy_generation": "0"}
    assert module.policy_fingerprint(value) != module.policy_fingerprint(policy())
    assert module.target_intent_sha256(ordinary) != module.target_intent_sha256(target())


@pytest.mark.parametrize(
    "field,bad",
    [
        ("generation", "0"),
        ("generation", "01"),
        ("generation", "+1"),
        ("generation", " 1"),
        ("generation", "1e2"),
        ("generation", "9223372036854775808"),
        ("generation", 1),
        ("generation", True),
        ("generation", 1.0),
        ("legacy_generation", "-1"),
        ("legacy_generation", 17),
        ("agent_id", "11111111111141118111111111111111"),
        ("operation_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("hook", "Alert"),
        ("hook", "x" * 64),
    ],
)
def test_fingerprint_rejects_noncanonical_bindings(field, bad):
    """No lossy identity/counter coercion, @spec PROTECTED-HOOK-SOURCE-6."""
    module = records_module()
    with pytest.raises(module.SourcePolicyRecordInvalid):
        module.policy_fingerprint({**policy(), field: bad})


@pytest.mark.parametrize(
    "field,bad",
    [
        ("mode", "unknown"),
        ("mode", True),
        ("tool_access", "read-write"),
        ("tool_access", None),
        ("runtime_id", None),
        ("qualification_id", 4),
        ("runtime_id", "33333333333343338333333333333333"),
        ("bundle_digest", "A" * 64),
        ("bundle_digest", "sha256:" + "a" * 64),
        ("bundle_digest", "a" * 63),
    ],
)
def test_target_rejects_invalid_protected_configuration(field, bad):
    """Strict readonly intent grammar, @spec PROTECTED-HOOK-SOURCE-10."""
    module = records_module()
    value = {**target(), field: bad}
    with pytest.raises(module.SourcePolicyRecordInvalid):
        module.target_intent_sha256(value)
    with pytest.raises(module.SourcePolicyRecordInvalid):
        module.policy_fingerprint({**policy(), **value})


@pytest.mark.parametrize(
    "field", ["tool_access", "runtime_id", "qualification_id", "bundle_digest"]
)
def test_ordinary_requires_explicit_null_policy_references(field):
    """Tombstone cannot retain authority, @spec PROTECTED-HOOK-SOURCE-10."""
    module = records_module()
    value = dict.fromkeys(target())
    value["mode"] = "ordinary"
    value[field] = target()[field]
    with pytest.raises(module.SourcePolicyRecordInvalid):
        module.target_intent_sha256(value)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("target", "mode"),
        ("target", "bundle_digest"),
        ("policy", "operation_id"),
        ("policy", "legacy_generation"),
    ],
)
def test_missing_decision_fields_are_not_defaulted(kind, field):
    """Explicit nulls remain mandatory, @spec PROTECTED-HOOK-SOURCE-6/10."""
    module = records_module()
    value = target() if kind == "target" else policy()
    del value[field]
    function = module.target_intent_sha256 if kind == "target" else module.policy_fingerprint
    with pytest.raises(module.SourcePolicyRecordInvalid):
        function(value)


@pytest.mark.parametrize("extra", ["expected_generation", "operation_id", "method", "updated_at"])
def test_intent_rejects_transport_or_audit_fields(extra):
    """Only target configuration is encodable, @spec PROTECTED-HOOK-SOURCE-10."""
    module = records_module()
    with pytest.raises(module.SourcePolicyRecordInvalid):
        module.target_intent_sha256({**target(), extra: "sensitive-example-value"})


@pytest.mark.parametrize("extra", ["activation", "readiness", "current_time", "secret"])
def test_fingerprint_rejects_nondecision_extras_safely(extra):
    """No accidental secret or volatile input, @spec PROTECTED-HOOK-SOURCE-6."""
    module = records_module()
    with pytest.raises(module.SourcePolicyRecordInvalid) as caught:
        module.policy_fingerprint({**policy(), extra: "sensitive-example-value"})
    assert "sensitive-example-value" not in str(caught.value)
