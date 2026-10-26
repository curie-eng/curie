"""Independent pure role matching; no caller permission or provider truth proof.

@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-3
@spec AGENT-ROLE-LAYER-V1-4 AGENT-ROLE-LAYER-V1-5 AGENT-ROLE-LAYER-V1-6
"""

from __future__ import annotations

import copy
import importlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "tests/vectors/http-identity-v1.json").read_text())
NOW = datetime.fromisoformat(VECTORS["now"])
OTHER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
AGENT = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
INVALID_POLICY = "invalid_agent_role_configuration"
FACT_CODES = (
    "invalid_response",
    "unsupported_version",
    "scope_mismatch",
    "identity_stale",
    "membership_stale",
    "unavailable",
)


def api() -> ModuleType:
    """@spec AGENT-ROLE-LAYER-V1-1: absence fails in the test, not collection."""
    name = "curie_api.agent_role_evaluation"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "pure role filter evaluation is not implemented"
        raise
    for symbol in ("RoleFilterContext", "RoleFilterLayerResult", "evaluate_role_filter_layer"):
        assert hasattr(module, symbol), f"role evaluation entry point missing: {symbol}"
    return module


def facts_api() -> ModuleType:
    """@spec AGENT-ROLE-LAYER-V1-1."""
    return importlib.import_module("curie_api.schemas.http_identity")


def roles_api() -> ModuleType:
    """@spec AGENT-ROLE-LAYER-V1-1."""
    return importlib.import_module("curie_api.schemas.agent_access")


def changed(value: Any, path: str, replacement: Any) -> Any:
    """@spec AGENT-ROLE-LAYER-V1-4: change synthetic input, not expected output."""
    result = copy.deepcopy(value)
    target = result
    keys = path.split(".")
    for key in keys[:-1]:
        target = target[int(key)] if isinstance(target, list) else target[key]
    target[keys[-1]] = replacement
    return result


def capture(
    name: str = "resolved-human-active",
    *,
    role: str = "invocation",
    request_changes: dict[str, Any] | None = None,
    response_changes: dict[str, Any] | None = None,
    limits: dict[str, int] | None = None,
) -> Any:
    """@spec AGENT-ROLE-LAYER-V1-1: all ordinary facts cross the actual T1 boundary."""
    module = facts_api()
    request = copy.deepcopy(VECTORS["request"])
    request["purpose"] = "approval" if role == "approver" else "invocation"
    for path, replacement in (request_changes or {}).items():
        request = changed(request, path, replacement)
    response = copy.deepcopy(next(c["response"] for c in VECTORS["cases"] if c["name"] == name))
    for field in ("request_id", "purpose", "authority_version", "scope"):
        response[field] = copy.deepcopy(request[field])
    for path, replacement in (response_changes or {}).items():
        response = changed(response, path, replacement)
    value = module.validate_lookup(
        json.dumps(response, ensure_ascii=False).encode(),
        module.LookupRequestV1.model_validate(request),
        module.FreshnessLimits.model_validate(dict(VECTORS["limits"], **(limits or {}))),
        now=NOW,
    )
    assert isinstance(value, module.ValidatedFacts), value
    return value


def context(facts: Any, role: str = "invocation") -> Any:
    """@spec AGENT-ROLE-LAYER-V1-1."""
    return api().RoleFilterContext(
        tenant_id=facts.request.scope.tenant_id,
        agent_id=AGENT,
        role=role,
        request=facts.request,
    )


def selectors(facts: Any) -> list[dict[str, Any]]:
    """@spec AGENT-ROLE-LAYER-V1-4: literal selector fixtures, never a matcher."""
    sender = facts.request.scope.sender
    issuer = facts.request.scope.expected_issuer
    return [
        {
            "kind": "surface_account",
            "provider": sender.provider,
            "namespace": sender.namespace.model_dump(),
            "native_id": sender.native_id,
        },
        {"kind": "issuer_subject", "issuer": issuer, "subject": "SampleSubject"},
        {"kind": "issuer_group", "group": {"issuer": issuer, "id": "SampleGroup"}},
    ]


def policy(
    facts: Any,
    entries: list[dict[str, Any]] | None,
    *,
    role: str = "invocation",
    migrated: bool = False,
    missing: bool = False,
) -> Any:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-5: use bounded production parsing."""
    config: dict[str, Any] = {
        "schema_version": 1,
        "legacy_state": "migrated" if migrated else "retained",
        "filter": None if entries is None else {"entries": entries},
    }
    if migrated:
        config["raw_account_revocation"] = "separate_removal_required"
    value = {
        "schema_version": 1,
        "tenant_id": facts.request.scope.tenant_id,
        "agent_id": AGENT,
        "version": 1,
        "roles": {} if missing else {role: config},
    }
    parser = importlib.import_module("curie_api.agent_role_filter")
    parsed = parser.parse_agent_role_policy_v1(json.dumps(value).encode())
    assert isinstance(parsed, roles_api().AgentRolePolicyV1), parsed
    return parsed


def evaluate(policy_value: Any, facts: Any, *, role: str = "invocation", **kwargs: Any) -> Any:
    """@spec AGENT-ROLE-LAYER-V1-1: call only the pure production boundary."""
    if "context" not in kwargs:
        kwargs["context"] = context(facts, role)
    kwargs.setdefault("now", NOW)
    return api().evaluate_role_filter_layer(policy_value, facts, **kwargs)


def expect(value: Any, status: str, reason: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-6: match never means allow."""
    assert isinstance(value, api().RoleFilterLayerResult)
    assert (value.status, value.reason, value.permission) == (status, reason, "not_evaluated")
    assert not hasattr(value, "allowed")
    with pytest.raises(TypeError, match="^role_filter_layer_is_not_permission$"):
        bool(value)


@pytest.mark.parametrize("name", [c["name"] for c in VECTORS["cases"] if c["expected"] == "facts"])
def test_normal_fixtures_are_real_t1_outputs(name: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-1: fixture errors cannot masquerade as missing matching."""
    assert capture(name).outcome in {"resolved", "inactive", "unresolved"}


@pytest.mark.parametrize("role", ["invocation", "approver"])
@pytest.mark.parametrize(
    ("configuration", "status", "reason"),
    [
        ("absent", "no_match", "not_configured"),
        ("no_roles", "no_match", "not_configured"),
        ("other_role", "no_match", "not_configured"),
        ("null", "match", "unrestricted"),
        ("empty", "no_match", "empty_filter"),
        ("raw", "match", "exact_selector"),
        ("subject", "match", "exact_selector"),
        ("group", "match", "exact_selector"),
        ("miss", "no_match", "no_selector_match"),
    ],
)
def test_active_role_states(role: str, configuration: str, status: str, reason: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-4 AGENT-ROLE-LAYER-V1-5: omission is not null."""
    facts = capture(role=role)
    entries = selectors(facts)
    configurations = {
        "absent": None,
        "no_roles": policy(facts, [], missing=True),
        "other_role": policy(
            facts, entries, role="approver" if role == "invocation" else "invocation"
        ),
        "null": policy(facts, None, role=role),
        "empty": policy(facts, [], role=role),
        "raw": policy(facts, entries[:1], role=role),
        "subject": policy(facts, entries[1:2], role=role),
        "group": policy(facts, entries[2:], role=role),
        "miss": policy(facts, [dict(entries[1], subject="AnotherSubject")], role=role),
    }
    expect(evaluate(configurations[configuration], facts, role=role), status, reason)


@pytest.mark.parametrize("role", ["invocation", "approver"])
def test_any_match_has_no_selector_preference_or_cross_role_help(role: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-4 AGENT-ROLE-LAYER-V1-5: first miss cannot shadow a match."""
    facts = capture(role=role)
    entries = selectors(facts)
    miss = dict(entries[1], subject="AnotherSubject")
    for ordered in ([miss, *entries], [*reversed(entries), miss], [miss, entries[2]]):
        expect(
            evaluate(policy(facts, ordered, role=role), facts, role=role), "match", "exact_selector"
        )
    candidate = policy(facts, [miss], role=role)
    other = policy(facts, entries, role="approver" if role == "invocation" else "invocation")
    candidate = candidate.model_copy(
        update={
            "roles": roles_api().AgentRolesV1.model_validate(
                {
                    **candidate.roles.model_dump(),
                    **other.roles.model_dump(),
                }
            )
        }
    )
    expect(evaluate(candidate, facts, role=role), "no_match", "no_selector_match")


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        ("provider", "Sample-chat"),
        ("provider", "sample-chat "),
        ("namespace.id", OTHER),
        ("namespace.authority", "OtherAuthority"),
        ("namespace.kind", "Workspace"),
        ("namespace.key", "exampleworkspace"),
        ("native_id", "sampleaccount"),
        ("native_id", "SampleAccount "),
        ("native_id", "SampleAccount\N{SNOWMAN}"),
        ("namespace.key", "ExampleWorkspac\N{LATIN SMALL LETTER E WITH ACUTE}"),
    ],
)
def test_every_raw_tuple_component_is_exact(path: str, replacement: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-4: never normalize an account or namespace lifetime."""
    facts = capture()
    raw = selectors(facts)[0]
    expect(evaluate(policy(facts, [raw]), facts), "match", "exact_selector")
    expect(
        evaluate(policy(facts, [changed(raw, path, replacement)]), facts),
        "no_match",
        "no_selector_match",
    )


@pytest.mark.parametrize(
    ("index", "path", "replacement"),
    [
        (1, "issuer", "https://identity.example.test/directory/"),
        (1, "issuer", "https://Identity.example.test/directory"),
        (1, "subject", "samplesubject"),
        (1, "subject", "SampleSubject "),
        (1, "subject", "SampleSubject\N{SNOWMAN}"),
        (2, "group.issuer", "https://identity.example.test/other"),
        (2, "group.id", "samplegroup"),
        (2, "group.id", "SampleGroup "),
    ],
)
def test_subject_and_group_compare_both_exact_components(
    index: int, path: str, replacement: str
) -> None:
    """@spec AGENT-ROLE-LAYER-V1-4: issuer and identifier are inseparable."""
    facts = capture()
    selector = selectors(facts)[index]
    expect(evaluate(policy(facts, [selector]), facts), "match", "exact_selector")
    expect(
        evaluate(policy(facts, [changed(selector, path, replacement)]), facts),
        "no_match",
        "no_selector_match",
    )


@pytest.mark.parametrize("index", [0, 1, 2])
def test_unicode_canonical_equivalence_is_not_identifier_equality(index: int) -> None:
    """@spec AGENT-ROLE-LAYER-V1-4: normalization must not merge distinct accounts."""
    composed = "Caf\N{LATIN SMALL LETTER E WITH ACUTE}"
    decomposed = "Cafe\N{COMBINING ACUTE ACCENT}"
    facts = capture(
        request_changes={"scope.sender.native_id": composed},
        response_changes={"identity.subject": composed, "membership.groups.0.id": composed},
    )
    selector = selectors(facts)[index]
    path = ["native_id", "subject", "group.id"][index]
    exact = changed(selector, path, composed)
    expect(evaluate(policy(facts, [exact]), facts), "match", "exact_selector")
    distinct = changed(selector, path, decomposed)
    expect(evaluate(policy(facts, [distinct]), facts), "no_match", "no_selector_match")


def test_selector_kinds_do_not_infer_aliases_from_shared_text() -> None:
    """@spec AGENT-ROLE-LAYER-V1-4: raw, subject and group text are separate domains."""
    facts = capture()
    raw, subject, group = selectors(facts)
    for selector in (
        dict(raw, native_id="SampleSubject"),
        dict(subject, subject="SampleAccount"),
        changed(group, "group.id", "SampleSubject"),
    ):
        expect(evaluate(policy(facts, [selector]), facts), "no_match", "no_selector_match")


@pytest.mark.parametrize("field", ["channel_identity_id", "provider_installation_id"])
def test_receiver_ids_are_capture_scope_not_raw_account_identity(field: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3 AGENT-ROLE-LAYER-V1-4."""
    original = capture()
    new_capture = capture(request_changes={f"scope.sender.{field}": OTHER})
    candidate = policy(original, selectors(original)[:1])
    expect(evaluate(candidate, new_capture), "match", "exact_selector")
    expect(evaluate(candidate, new_capture, context=context(original)), "refusal", "scope_mismatch")


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        ("request_id", OTHER),
        ("authority_version", 8),
        ("purpose", "approval"),
        ("scope.tenant_id", OTHER),
        ("scope.expected_issuer", "https://identity.example.test/other"),
        ("scope.sender.provider", "another-chat"),
        ("scope.sender.native_id", "OtherAccount"),
        ("scope.sender.namespace.id", OTHER),
        ("scope.sender.namespace.authority", "Other"),
        ("scope.sender.namespace.kind", "Other"),
        ("scope.sender.namespace.key", "Other"),
    ],
)
def test_complete_request_mismatch_precedes_matching(path: str, replacement: Any) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: all captured request fields must correspond."""
    facts = capture()
    request = facts_api().LookupRequestV1.model_validate(
        changed(facts.request.model_dump(), path, replacement)
    )
    ctx = context(facts).model_copy(update={"request": request})
    expect(evaluate(policy(facts, None), facts, context=ctx), "refusal", "scope_mismatch")


@pytest.mark.parametrize("field", ["tenant_id", "agent_id"])
def test_policy_and_context_scope_are_exact(field: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3 AGENT-ROLE-LAYER-V1-4: tenant is part of raw scope."""
    facts = capture()
    candidate = policy(facts, selectors(facts)[:1]).model_copy(update={field: OTHER})
    expect(evaluate(candidate, facts), "refusal", "scope_mismatch")
    ctx = context(facts).model_copy(update={field: OTHER})
    expect(evaluate(policy(facts, None), facts, context=ctx), "refusal", "scope_mismatch")


@pytest.mark.parametrize("role", ["invocation", "approver"])
def test_role_must_agree_with_lookup_purpose(role: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-3."""
    facts = capture(role="approver" if role == "invocation" else "invocation")
    expect(evaluate(policy(facts, None, role=role), facts, role=role), "refusal", "scope_mismatch")


@pytest.mark.parametrize("role", ["invocation", "approver"])
@pytest.mark.parametrize("state", ["disabled", "revoked"])
def test_inactive_human_requires_selected_migrated_exact_raw(role: str, state: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-5: identity/subject/group cannot replace migrated raw."""
    facts = capture(f"inactive-human-{state}", role=role)
    entries = selectors(facts)
    expect(
        evaluate(policy(facts, entries[:1], role=role, migrated=True), facts, role=role),
        "match",
        "exact_raw",
    )
    alternatives = [
        None,
        policy(facts, [], missing=True),
        policy(facts, None, role=role, migrated=True),
        policy(facts, [], role=role, migrated=True),
        policy(facts, entries[:1], role=role),
        policy(facts, entries[1:], role=role, migrated=True),
        policy(facts, [dict(entries[0], native_id="OtherAccount")], role=role, migrated=True),
        policy(
            facts,
            entries[:1],
            role="approver" if role == "invocation" else "invocation",
            migrated=True,
        ),
    ]
    for candidate in alternatives:
        expect(evaluate(candidate, facts, role=role), "refusal", "inactive_requires_migrated_raw")


@pytest.mark.parametrize("role", ["invocation", "approver"])
@pytest.mark.parametrize("kind", ["human", "unknown"])
@pytest.mark.parametrize("migrated", [False, True])
def test_unresolved_raw_detection_never_supplies_independent_human_proof(
    role: str, kind: str, migrated: bool
) -> None:
    """@spec AGENT-ROLE-LAYER-V1-5 AGENT-ROLE-LAYER-V1-6: no synthetic human proof input."""
    facts = capture(f"unresolved-{kind}", role=role)
    entries = selectors(facts)
    expect(
        evaluate(policy(facts, entries[:1], role=role, migrated=migrated), facts, role=role),
        "refusal",
        "independent_human_required",
    )
    alternatives = [
        None,
        policy(facts, [], missing=True),
        policy(facts, None, role=role),
        policy(facts, [], role=role),
        policy(facts, entries[1:], role=role),
        policy(facts, [dict(entries[0], native_id="OtherAccount")], role=role),
        policy(facts, entries[:1], role="approver" if role == "invocation" else "invocation"),
    ]
    for candidate in alternatives:
        expect(evaluate(candidate, facts, role=role), "refusal", "unresolved_requires_raw")


@pytest.mark.parametrize(
    "name",
    [
        "resolved-service-active",
        "resolved-agent-active",
        "inactive-service-disabled",
        "inactive-agent-revoked",
        "unresolved-service",
        "unresolved-agent",
    ],
)
def test_service_and_agent_kind_refuse_before_null_or_migrated_raw(name: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3 AGENT-ROLE-LAYER-V1-5."""
    facts = capture(name)
    for candidate in (
        None,
        policy(facts, None),
        policy(facts, selectors(facts)[:1], migrated=True),
    ):
        expect(evaluate(candidate, facts), "refusal", "forbidden_kind")


@pytest.mark.parametrize("code", FACT_CODES)
@pytest.mark.parametrize("role", ["invocation", "approver"])
def test_existing_facts_refusals_never_fall_back_to_absence_null_or_raw(
    code: str, role: str
) -> None:
    """@spec AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-3 AGENT-ROLE-LAYER-V1-6."""
    good = capture(role=role)
    refusal = facts_api().FactsRefusal(code=code)
    for candidate in (
        None,
        policy(good, None, role=role),
        policy(good, selectors(good)[:1], role=role),
    ):
        result = api().evaluate_role_filter_layer(
            candidate, refusal, context=context(good, role), now=NOW
        )
        expect(result, "refusal", code)
    expect(evaluate(policy(good, None, role=role), good, role=role), "match", "unrestricted")
    expect(
        api().evaluate_role_filter_layer(None, refusal, context=context(good, role), now=NOW),
        "refusal",
        code,
    )


@pytest.mark.parametrize("which", ["identity", "membership"])
def test_exact_expiry_equality_is_usable_and_one_microsecond_excess_is_stale(which: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: membership freshness is independent of identity."""
    facts = capture(limits={"max_identity_age_ms": 5000 if which == "membership" else 2000})
    endpoint = datetime(2030, 1, 1, 12, 0, 3 if which == "membership" else 2, tzinfo=UTC)
    candidate = policy(facts, None)
    expect(evaluate(candidate, facts, now=endpoint), "match", "unrestricted")
    expect(
        evaluate(candidate, facts, now=endpoint + timedelta(microseconds=1)),
        "refusal",
        f"{which}_stale",
    )


@pytest.mark.parametrize("which", ["identity", "membership"])
@pytest.mark.parametrize(
    ("stamp", "at_boundary", "fraction"),
    [
        ("2030-01-01T11:59:59.99999999Z", "refusal", Fraction(-1, 100_000_000)),
        ("2030-01-01T12:00:00.00000001Z", "match", Fraction(1, 100_000_000)),
    ],
)
def test_fraction_expiry_never_rounds_to_a_microsecond(
    which: str, stamp: str, at_boundary: str, fraction: Fraction
) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: catch float conversion and rounding expiry upward."""
    observation = (
        "identity_observation.as_of" if which == "identity" else "membership.observation.as_of"
    )
    limits = (
        {"max_identity_age_ms": 5000, "max_membership_age_ms": 2000}
        if which == "membership"
        else {}
    )
    facts = capture(response_changes={observation: stamp}, limits=limits)
    assert getattr(facts, f"{which}_expires_at") == Fraction(1893499202) + fraction
    boundary = datetime(2030, 1, 1, 12, 0, 2, tzinfo=UTC)
    candidate = policy(facts, None)
    expect(
        evaluate(candidate, facts, now=boundary - timedelta(microseconds=1)),
        "match",
        "unrestricted",
    )
    expect(
        evaluate(candidate, facts, now=boundary),
        at_boundary,
        f"{which}_stale" if at_boundary == "refusal" else "unrestricted",
    )
    expect(
        evaluate(candidate, facts, now=boundary + timedelta(microseconds=1)),
        "refusal",
        f"{which}_stale",
    )


@pytest.mark.parametrize("name", ["inactive-human-disabled", "unresolved-human"])
def test_nonresolved_expiry_is_checked_before_raw_outcome(name: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3 AGENT-ROLE-LAYER-V1-5."""
    facts = capture(name)
    candidate = policy(facts, selectors(facts)[:1], migrated=True)
    expect(evaluate(candidate, facts, now=NOW + timedelta(seconds=2)), "refusal", "identity_stale")


@pytest.mark.parametrize(
    "clock",
    [
        None,
        1,
        "2030-01-01T12:00:01Z",
        NOW.replace(tzinfo=None),
        NOW.astimezone(timezone(timedelta(hours=1))),
    ],
)
def test_invalid_clock_precedes_policy_and_facts_refusals(clock: Any) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: never read a fallback wall clock."""
    good = capture()
    refusal = facts_api().FactsRefusal(code="unavailable")
    bad_policy = roles_api().RoleFilterRefusal(code=INVALID_POLICY)
    expect(
        api().evaluate_role_filter_layer(bad_policy, refusal, context=context(good), now=clock),
        "refusal",
        "invalid_context",
    )


@pytest.mark.parametrize("argument", ["policy", "facts", "context"])
@pytest.mark.parametrize("value", [{}, b"{}", True, "SYNTHETIC_ARGUMENT_MARKER"])
def test_unsupported_outer_types_are_not_an_alternative_public_contract(
    argument: str, value: Any
) -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-3."""
    facts = capture()
    inputs = {"policy": policy(facts, None), "facts": facts, "context": context(facts), "now": NOW}
    inputs[argument] = value
    expect(api().evaluate_role_filter_layer(**inputs), "refusal", "invalid_context")


@pytest.mark.parametrize("code", [INVALID_POLICY, "unsupported_agent_role_configuration_version"])
def test_policy_refusal_precedes_facts_refusal(code: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-3."""
    facts = capture()
    expect(
        api().evaluate_role_filter_layer(
            roles_api().RoleFilterRefusal(code=code),
            facts_api().FactsRefusal(code="unavailable"),
            context=context(facts),
            now=NOW,
        ),
        "refusal",
        code,
    )


@pytest.mark.parametrize("target", ["policy", "facts"])
@pytest.mark.parametrize("bad_code", ["SYNTHETIC_DIAGNOSTIC_MARKER", None, True])
def test_constructed_refusals_cannot_inject_reason_codes(target: str, bad_code: Any) -> None:
    """@spec AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-3."""
    facts = capture()
    candidate = policy(facts, None)
    if target == "policy":
        candidate = roles_api().RoleFilterRefusal.model_construct(code=bad_code)
    else:
        facts = facts_api().FactsRefusal.model_construct(code=bad_code)
    result = api().evaluate_role_filter_layer(candidate, facts, context=context(capture()), now=NOW)
    expect(result, "refusal", INVALID_POLICY if target == "policy" else "invalid_facts")
    assert "SYNTHETIC_DIAGNOSTIC_MARKER" not in repr(result)


def test_mixed_error_precedence_never_reaches_matching() -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: each adjacent guard beats downstream failures."""
    facts = capture()
    candidate = policy(facts, None)
    invalid_facts = facts.model_copy(update={"identity": None})
    wrong_scope = context(facts).model_copy(update={"agent_id": OTHER})
    expect(evaluate(candidate, invalid_facts, context=wrong_scope), "refusal", "invalid_facts")
    expect(
        evaluate(candidate, facts, context=wrong_scope, now=NOW + timedelta(seconds=9)),
        "refusal",
        "scope_mismatch",
    )
    expect(evaluate(candidate, facts, now=NOW + timedelta(seconds=9)), "refusal", "identity_stale")
    service = capture(
        "resolved-service-active",
        limits={"max_identity_age_ms": 5000, "max_membership_age_ms": 1000},
    )
    expect(
        evaluate(policy(service, None), service, now=NOW + timedelta(microseconds=1)),
        "refusal",
        "membership_stale",
    )
    malformed_policy = candidate.model_copy(update={"version": 0})
    expect(evaluate(malformed_policy, invalid_facts), "refusal", INVALID_POLICY)
    malformed_ctx = context(facts).model_copy(update={"role": "approval"})
    expect(
        evaluate(malformed_policy, invalid_facts, context=malformed_ctx),
        "refusal",
        "invalid_context",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("identity", None),
        ("identity_observation", None),
        ("identity_expires_at", None),
        ("membership_expires_at", None),
        ("membership_observation", None),
        ("groups", None),
        ("groups", []),
        ("sender_kind", "unknown"),
        ("sender_kind", "service"),
        ("outcome", "unresolved"),
        ("outcome", "inactive"),
    ],
)
def test_malformed_resolved_facts_refuse_before_null(field: str, value: Any) -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-3: no missing-component fallback."""
    facts = capture()
    expect(
        evaluate(policy(facts, None), facts.model_copy(update={field: value})),
        "refusal",
        "invalid_facts",
    )


@pytest.mark.parametrize("name", ["inactive-human-disabled", "unresolved-human"])
@pytest.mark.parametrize("field", ["groups", "membership_observation", "membership_expires_at"])
def test_nonresolved_facts_cannot_carry_membership_components(name: str, field: str) -> None:
    """@spec AGENT-ROLE-LAYER-V1-3."""
    active = capture()
    facts = capture(name)
    malformed = facts.model_copy(update={field: getattr(active, field)})
    expect(evaluate(policy(facts, None), malformed), "refusal", "invalid_facts")


def test_contradictory_identity_and_group_components_are_not_normal_fixtures() -> None:
    """@spec AGENT-ROLE-LAYER-V1-3: ordinary defensive coherence, not forgery detection."""
    facts = capture()
    for update in (
        {"account_status": "disabled"},
        {"kind": "service"},
        {"issuer": "https://identity.example.test/other"},
    ):
        malformed = facts.model_copy(update={"identity": facts.identity.model_copy(update=update)})
        expect(evaluate(policy(facts, None), malformed), "refusal", "invalid_facts")
    group = facts.groups[0].model_copy(update={"issuer": "https://identity.example.test/other"})
    expect(
        evaluate(policy(facts, None), facts.model_copy(update={"groups": (group,)})),
        "refusal",
        "invalid_facts",
    )
    inactive = capture("inactive-human-disabled")
    expect(
        evaluate(policy(inactive, None), inactive.model_copy(update={"identity": facts.identity})),
        "refusal",
        "invalid_facts",
    )
    unresolved = capture("unresolved-human")
    expect(
        evaluate(
            policy(unresolved, None), unresolved.model_copy(update={"identity": facts.identity})
        ),
        "refusal",
        "invalid_facts",
    )


def test_context_revalidation_forbids_proof_permission_and_nested_mutations() -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-6: no human-proof escape hatch."""
    facts = capture()
    cls = api().RoleFilterContext
    valid = context(facts)
    for extra in ("human_verified", "allowed", "permission", "active", "ready"):
        with pytest.raises(ValueError):
            cls.model_validate(dict(valid.model_dump(), **{extra: True}))
    for update in (
        {"tenant_id": "invalid"},
        {"agent_id": OTHER.upper()},
        {"role": "approval"},
        {"request": None},
    ):
        malformed = valid.model_copy(update=update)
        expect(
            evaluate(policy(facts, None), facts, context=malformed), "refusal", "invalid_context"
        )
    malformed = context(capture())
    object.__setattr__(malformed.request.scope.sender.namespace, "id", "invalid")
    expect(evaluate(policy(facts, None), facts, context=malformed), "refusal", "invalid_context")
    with pytest.raises((ValueError, AttributeError, TypeError)):
        valid.role = "approver"


def test_mutated_policy_is_detached_revalidated_and_never_cached() -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-3: nested lists remain untrusted."""
    facts = capture()
    candidate = policy(facts, selectors(facts)[1:2])
    first = evaluate(candidate, facts)
    expect(first, "match", "exact_selector")
    candidate.roles.invocation.filter.entries.clear()
    expect(evaluate(candidate, facts), "no_match", "empty_filter")
    expect(first, "match", "exact_selector")
    candidate.roles.invocation.filter.entries.append(
        roles_api().IssuerSubjectSelectorV1.model_construct(
            kind="issuer_subject", issuer="https://identity.example.test/directory", subject=True
        )
    )
    expect(evaluate(candidate, facts), "refusal", INVALID_POLICY)
    duplicate = policy(facts, selectors(facts)[:1])
    duplicate.roles.invocation.filter.entries.append(duplicate.roles.invocation.filter.entries[0])
    expect(evaluate(duplicate, facts), "refusal", INVALID_POLICY)
    oversize = policy(facts, selectors(facts)[1:2])
    object.__setattr__(oversize.roles.invocation.filter.entries[0], "subject", "x" * 65536)
    expect(evaluate(oversize, facts), "refusal", INVALID_POLICY)
    expect(
        evaluate(policy(facts, None).model_copy(update={"version": 0}), facts),
        "refusal",
        INVALID_POLICY,
    )


def test_results_are_frozen_constant_and_do_not_retain_synthetic_identity() -> None:
    """@spec AGENT-ROLE-LAYER-V1-2 AGENT-ROLE-LAYER-V1-6."""
    marker = "SYNTHETIC_IDENTITY_DETAIL_MARKER"
    facts = capture(
        request_changes={"scope.sender.native_id": marker},
        response_changes={"identity.subject": marker},
    )
    candidate = policy(facts, selectors(facts)[:1])
    results = [
        evaluate(candidate, facts),
        evaluate(None, facts),
        evaluate(candidate, facts, now=NOW + timedelta(seconds=9)),
    ]
    for result in results:
        assert result.permission == "not_evaluated"
        for rendered in (str(result), repr(result)):
            assert marker not in rendered
            assert facts.request.scope.tenant_id not in rendered
            assert "request_id" not in rendered
            assert len(rendered) < 250
        for field, value in (("status", "match"), ("reason", marker), ("permission", "allowed")):
            with pytest.raises((ValueError, AttributeError, TypeError)):
                setattr(result, field, value)
        with pytest.raises(TypeError, match="^role_filter_layer_is_not_permission$"):
            bool(result)
    with pytest.raises(TypeError):
        api().evaluate_role_filter_layer(
            candidate, facts, context=context(facts), now=NOW, human_verified=True
        )


def test_module_import_and_repeated_calls_have_no_runtime_or_external_io() -> None:
    """@spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-6: executable isolation only."""
    api()
    script = r"""
import builtins
import importlib.abc
import io
import json
import os
import socket
import sys
import time
from datetime import datetime
forbidden = ("curie_api.main", "curie_api.app", "curie_api.db", "curie_api.config",
             "curie_api.models", "curie_api.admission", "curie_api.agent_access",
             "curie_api.routers", "curie_api.team_authorization", "sqlalchemy",
             "asyncpg", "httpx", "requests", "redis", "boto3", "kubernetes", "curie_worker")
class RuntimeForbidden(importlib.abc.MetaPathFinder):
    # @spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-6
    def find_spec(self, fullname, path=None, target=None):
        # @spec AGENT-ROLE-LAYER-V1-1
        if any(fullname == p or fullname.startswith(p + ".") for p in forbidden):
            raise AssertionError("role layer imported runtime: " + fullname)
sys.meta_path.insert(0, RuntimeForbidden())
from curie_api.agent_role_evaluation import RoleFilterContext, evaluate_role_filter_layer
from curie_api.agent_role_filter import parse_agent_role_policy_v1
from curie_api.schemas.http_identity import LookupRequestV1, FreshnessLimits, validate_lookup
vectors, agent = json.loads(sys.argv[1]), sys.argv[2]
now = datetime.fromisoformat(vectors["now"])
request = LookupRequestV1.model_validate(vectors["request"])
facts = validate_lookup(json.dumps(vectors["cases"][0]["response"]).encode(), request,
                        FreshnessLimits.model_validate(vectors["limits"]), now=now)
policy = parse_agent_role_policy_v1(json.dumps({"schema_version": 1,
    "tenant_id": request.scope.tenant_id, "agent_id": agent, "version": 1,
    "roles": {"invocation": {"schema_version": 1, "legacy_state": "retained",
                              "filter": None}}}).encode())
context = RoleFilterContext(tenant_id=request.scope.tenant_id, agent_id=agent,
                            role="invocation", request=request)
def no_io(*args, **kwargs):
    # @spec AGENT-ROLE-LAYER-V1-1 AGENT-ROLE-LAYER-V1-3
    raise AssertionError("pure role calculation attempted IO or read a clock")
builtins.open = io.open = os.open = no_io
socket.socket = socket.create_connection = socket.getaddrinfo = no_io
time.time = time.monotonic = no_io
for _ in range(2):
    result = evaluate_role_filter_layer(policy, facts, context=context, now=now)
    assert (result.status, result.reason, result.permission) == (
        "match", "unrestricted", "not_evaluated")
    absent = evaluate_role_filter_layer(None, facts, context=context, now=now)
    assert (absent.status, absent.reason, absent.permission) == (
        "no_match", "not_configured", "not_evaluated")
assert not any(n == p or n.startswith(p + ".") for n in sys.modules for p in forbidden)
print("pure role import and calculation passed")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(VECTORS), AGENT],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "pure role import and calculation passed"
