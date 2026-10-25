"""Independent pure representations; no live authorization or migration proof.

@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-3
@spec AGENT-ROLE-FILTER-V1-4 AGENT-ROLE-FILTER-V1-5 AGENT-ROLE-FILTER-V1-6
"""

from __future__ import annotations

import copy
import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "tests/vectors/agent-role-filter-v1.json").read_text())
INVALID = "invalid_agent_role_configuration"
UNSUPPORTED = "unsupported_agent_role_configuration_version"
OTHER_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def api() -> ModuleType:
    """@spec AGENT-ROLE-FILTER-V1-1: missing behavior fails inside tests."""
    name = "curie_api.agent_role_filter"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "pure agent role filter parser/readback is not implemented"
        raise
    for symbol in (
        "parse_agent_role_policy_v1",
        "parse_agent_role_change_v1",
        "render_agent_role_policy_v1",
    ):
        assert hasattr(module, symbol), f"agent role filter entry point missing: {symbol}"
    return module


def models() -> ModuleType:
    """@spec AGENT-ROLE-FILTER-V1-1."""
    module = importlib.import_module("curie_api.schemas.agent_access")
    for symbol in (
        "SurfaceAccountSelectorV1",
        "SelectorV1",
        "RoleConfigurationV1",
        "AgentRoleChangeV1",
        "IssuerSubjectSelectorV1",
        "IssuerGroupSelectorV1",
        "RoleFilterV1",
        "RetainedRoleConfigurationV1",
        "MigratedRoleConfigurationV1",
        "AgentRolesV1",
        "AgentRolePolicyV1",
        "SetRoleFilterV1",
        "MigrateRoleV1",
        "RoleFilterRefusal",
    ):
        assert hasattr(module, symbol), f"agent role filter model missing: {symbol}"
    return module


def policy() -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-2."""
    return copy.deepcopy(VECTORS["cases"][0]["input"])


def change() -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-4."""
    return copy.deepcopy(next(c["input"] for c in VECTORS["cases"] if c["target"] == "change"))


def encoded(value: Any) -> bytes:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()


def changed(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    result = copy.deepcopy(value)
    target = result
    keys = path.split(".")
    for key in keys[:-1]:
        target = target[int(key)] if isinstance(target, list) else target[key]
    target[keys[-1]] = replacement
    return result


def parse(value: Any, target: str = "policy") -> Any:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    return getattr(api(), f"parse_agent_role_{target}_v1")(
        encoded(value) if isinstance(value, dict) else value
    )


def refuse(result: Any, code: str = INVALID) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    assert isinstance(result, models().RoleFilterRefusal)
    assert result.model_dump(mode="json") == {"code": code}
    assert len(str(result)) < 200
    assert len(repr(result)) < 200
    assert getattr(result, "__cause__", None) is None
    assert getattr(result, "__context__", None) is None


def readback(value: dict[str, Any]) -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    result = parse(value)
    assert isinstance(result, models().AgentRolePolicyV1)
    rendered = api().render_agent_role_policy_v1(result)
    assert isinstance(rendered, bytes)
    assert (
        json.dumps(json.loads(rendered), ensure_ascii=False, separators=(",", ":")).encode()
        == rendered
    )
    reparsed = parse(rendered)
    assert isinstance(reparsed, models().AgentRolePolicyV1)
    assert api().render_agent_role_policy_v1(reparsed) == rendered
    return json.loads(rendered)


@pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: c["name"])
def test_independent_decoded_vectors(case: dict[str, Any]) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-3 AGENT-ROLE-FILTER-V1-6."""
    result = parse(case["input"], case["target"])
    if case["expected"] != "valid":
        refuse(result, case["expected"])
    elif case["target"] == "policy":
        assert readback(case["input"]) == case["input"]
    else:
        assert not isinstance(result, models().RoleFilterRefusal)
        assert result.model_dump(mode="json") == case["input"]


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        ("native_id", "sampleaccount"),
        ("native_id", "SampleAccount "),
        ("native_id", "SampleAccount\N{SNOWMAN}"),
        ("provider", "sample-chat"),
        ("namespace.id", OTHER_UUID),
    ],
)
def test_exact_selectors_and_namespace_lifetime(path: str, replacement: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2: distinct exact selectors survive readback."""
    candidate = policy()
    entries = candidate["roles"]["invocation"]["filter"]["entries"]
    entries.append(changed(entries[0], path, replacement))
    assert readback(candidate) == candidate


@pytest.mark.parametrize(
    "path", ["provider", "namespace.authority", "namespace.kind", "namespace.key"]
)
@pytest.mark.parametrize("target", ["policy", "change"])
def test_namespace_descriptor_consistency_within_filter(path: str, target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-6."""
    candidate = policy() if target == "policy" else change()
    filt = candidate["roles"]["invocation"]["filter"] if target == "policy" else candidate["filter"]
    raw = filt["entries"][0]
    filt["entries"] = [raw, changed(raw, path, "OtherDescriptor")]
    refuse(parse(candidate, target))
    filt["entries"][1]["namespace"]["id"] = OTHER_UUID
    assert not isinstance(parse(candidate, target), models().RoleFilterRefusal)


def test_namespace_consistency_across_roles_and_cross_role_reuse() -> None:
    """@spec AGENT-ROLE-FILTER-V1-3 AGENT-ROLE-FILTER-V1-6."""
    candidate = policy()
    candidate["roles"]["approver"] = copy.deepcopy(candidate["roles"]["invocation"])
    assert readback(candidate) == candidate
    candidate["roles"]["approver"]["filter"]["entries"][0]["namespace"]["key"] = "Substituted"
    refuse(parse(candidate))


@pytest.mark.parametrize("index", [0, 1, 2])
def test_exact_duplicates_refuse_for_each_selector_kind(index: int) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = policy()
    entries = candidate["roles"]["invocation"]["filter"]["entries"]
    entries.append(copy.deepcopy(entries[index]))
    refuse(parse(candidate))


@pytest.mark.parametrize(
    ("index", "path", "value"),
    [
        (1, "issuer", "https://identity.example.test/Exact%2fIssuer"),
        (1, "subject", "samplesubject"),
        (2, "group.id", "samplegroup"),
        (2, "group.issuer", "https://Identity.example.test/Other"),
    ],
)
def test_distinct_issuer_and_group_strings_are_never_normalized(
    index: int, path: str, value: str
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-6."""
    candidate = policy()
    entries = candidate["roles"]["invocation"]["filter"]["entries"]
    entries.append(changed(entries[index], path, value))
    assert readback(candidate) == candidate


@pytest.mark.parametrize("role", ["invocation", "approver"])
def test_missing_null_empty_and_role_independence(role: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-3."""
    candidate = policy()
    config = candidate["roles"].pop("invocation")
    assert readback(candidate)["roles"] == {}
    candidate["roles"][role] = config
    for filt in [None, {"entries": []}, config["filter"]]:
        candidate["roles"][role] = dict(config, filter=filt)
        assert readback(candidate) == candidate
        assert set(readback(candidate)["roles"]) == {role}
    candidate["roles"][role] = None
    refuse(parse(candidate))
    refuse(parse(changed(policy(), "roles", None)))
    refuse(parse(None))


@pytest.mark.parametrize("target", ["policy", "change"])
@pytest.mark.parametrize(
    "field",
    [
        "tenant_id",
        "channel_identity_id",
        "provider_installation_id",
        "channel_id",
        "binding_id",
        "generation",
        "credential_id",
        "principal_id",
        "profile_email",
        "display_name",
    ],
)
def test_selectors_forbid_receiver_local_principal_and_profile_fields(
    target: str, field: str
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2."""
    candidate = policy() if target == "policy" else change()
    prefix = "roles.invocation.filter.entries.0." if target == "policy" else "filter.entries.0."
    refuse(parse(changed(candidate, prefix + field, OTHER_UUID), target))


@pytest.mark.parametrize(
    "path",
    [
        "",
        "roles",
        "roles.invocation",
        "roles.invocation.filter",
        "roles.invocation.filter.entries.0",
        "roles.invocation.filter.entries.0.namespace",
        "roles.invocation.filter.entries.1",
        "roles.invocation.filter.entries.2.group",
    ],
)
@pytest.mark.parametrize("field", ["permitted", "human_proof", "ready", "active", "authority_mode"])
def test_permission_proof_and_activation_extras_refuse(path: str, field: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-5."""
    refuse(parse(changed(policy(), (path + "." if path else "") + field, True)))


@pytest.mark.parametrize("role", ["invocation", "approver"])
def test_migration_intent_and_disclosure_union(role: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-3 AGENT-ROLE-FILTER-V1-4: intent only."""
    candidate = dict(change(), role=role)
    assert parse(candidate, "change").model_dump(mode="json") == candidate
    candidate["operation"] = "migrate_role"
    refuse(parse(candidate, "change"))
    candidate["raw_account_revocation"] = "separate_removal_required"
    assert parse(candidate, "change").model_dump(mode="json") == candidate
    for value in [None, True, "Separate_removal_required", "bypass", ""]:
        refuse(parse(dict(candidate, raw_account_revocation=value), "change"))
    candidate["operation"] = "set_filter"
    refuse(parse(candidate, "change"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("operation", "reverse_migration"),
        ("operation", "reset"),
        ("role", "approval"),
        ("role", ["invocation", "approver"]),
        ("roles", {}),
        ("legacy_state", "retained"),
        ("invocation_team_id", None),
        ("approver_team_id", None),
        ("clear_bindings", True),
        ("clear_routes", True),
        ("authority_mode", "http"),
        ("allowed_callers", None),
    ],
)
def test_change_is_one_role_intent_without_legacy_or_reset_overrides(
    field: str, value: Any
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-4."""
    refuse(parse(changed(change(), field, value), "change"))


def test_legacy_write_schema_compatibility_without_transition_simulation() -> None:
    """@spec AGENT-ROLE-FILTER-V1-4: legacy schema unchanged, enforcement deferred."""
    legacy = importlib.import_module("curie_api.schemas.agent_access").AgentAccessPolicyWrite
    value = {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0}
    assert legacy.model_validate(value).model_dump(mode="json") == value
    assert legacy.model_validate(dict(value, expected_version=10**100)).expected_version == 10**100
    with pytest.raises(ValueError):
        legacy.model_validate(dict(value, roles={}))
    assert set(legacy.model_fields) == set(value)


@pytest.mark.parametrize(
    ("target", "field", "accepted"),
    [("policy", "version", [1, 2147483647]), ("change", "expected_version", [0, 1, 2147483647])],
)
def test_existing_counter_shape_without_cas(target: str, field: str, accepted: list[int]) -> None:
    """@spec AGENT-ROLE-FILTER-V1-4."""
    base = policy() if target == "policy" else change()
    for value in accepted:
        candidate = changed(base, field, value)
        result = parse(candidate, target)
        assert getattr(result, field) == value
        assert parse(candidate, target).model_dump(mode="json") == result.model_dump(mode="json")
    for value in [-1, 2147483648, True, False, "1", 1.0, None]:
        refuse(parse(changed(base, field, value), target))


@pytest.mark.parametrize("target", ["policy", "change"])
@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{",
        b"[]",
        b"null",
        b"true",
        b"42",
        b"{} garbage",
        b"{}{}",
        b"\xef\xbb\xbf{}",
        b"\xff",
        b'{"schema_version":1,"x":"\xed\xa0\x80"}',
        b'{"schema_version":2,"x":"\\ud800"}',
        b'{"schema_version":2,"x":"\\udfff"}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
        b'{"schema_version":2,"nested":{"n":NaN}}',
        b'{"schema_version":2,"schema_version":1}',
    ],
)
def test_strict_bounded_parse_rejects_malformed_json_before_version(
    target: str, raw: bytes
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    refuse(parse(raw, target))


@pytest.mark.parametrize("target", ["policy", "change"])
@pytest.mark.parametrize("value", [None, "{}", bytearray(b"{}"), memoryview(b"{}"), 1, [], True])
def test_parser_is_a_bytes_boundary(target: str, value: Any) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    refuse(parse(value, target))


@pytest.mark.parametrize("target", ["policy", "change"])
@pytest.mark.parametrize(
    "needle",
    [
        b'"schema_version":1',
        b'"entries":[',
        b'"kind":"surface_account"',
        b'"authority":""',
        b'"subject":"SampleSubject"',
        b'"id":"SampleGroup"',
    ],
)
def test_duplicate_object_keys_at_every_depth(target: str, needle: bytes) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    raw = encoded(policy() if target == "policy" else change())
    assert needle in raw
    if needle == b'"entries":[':
        raw = raw.replace(needle, b'"entries":[],"entries":[', 1)
    else:
        raw = raw.replace(needle, needle + b"," + needle, 1)
    refuse(parse(raw, target))


@pytest.mark.parametrize("target", ["policy", "change"])
def test_byte_limit_includes_whitespace_and_multibyte_text(target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = policy() if target == "policy" else change()
    prefix = (
        "roles.invocation.filter.entries.1.subject"
        if target == "policy"
        else "filter.entries.1.subject"
    )
    candidate = changed(candidate, prefix, "Sample\N{SNOWMAN}")
    raw = json.dumps(candidate, ensure_ascii=False).encode()
    exact = raw + b" " * (65536 - len(raw))
    assert len(exact) == 65536
    assert not isinstance(parse(exact, target), models().RoleFilterRefusal)
    refuse(parse(exact + b" ", target))


@pytest.mark.parametrize("target", ["policy", "change"])
def test_entries_128_129_and_order(target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = policy() if target == "policy" else change()
    path = "roles.invocation.filter.entries" if target == "policy" else "filter.entries"
    entries = [
        {"kind": "issuer_subject", "issuer": "https://identity.example.test", "subject": str(i)}
        for i in range(128)
    ]
    candidate = changed(candidate, path, entries)
    result = parse(candidate, target)
    assert not isinstance(result, models().RoleFilterRefusal)
    if target == "policy":
        assert readback(candidate) == candidate
    else:
        assert result.model_dump(mode="json") == candidate
    refuse(parse(changed(candidate, path, entries + [dict(entries[0], subject="128")]), target))
    refuse(parse(changed(candidate, path, entries + [entries[0]]), target))


@pytest.mark.parametrize(
    ("target", "field"),
    [
        ("policy", "schema_version"),
        ("change", "schema_version"),
        ("policy", "counter"),
        ("change", "counter"),
        ("policy", "nested_schema"),
    ],
)
@pytest.mark.parametrize("token", [b"1.0", b"1e0", b"true", b'"1"'])
def test_integer_json_tokens_are_strict(target: str, field: str, token: bytes) -> None:
    """@spec AGENT-ROLE-FILTER-V1-4 AGENT-ROLE-FILTER-V1-6."""
    candidate = policy() if target == "policy" else change()
    raw = encoded(candidate)
    if field == "nested_schema":
        needle = b'"roles":{"invocation":{"schema_version":1'
        raw = raw.replace(needle, b'"roles":{"invocation":{"schema_version":' + token, 1)
    else:
        name = (
            ("version" if target == "policy" else "expected_version")
            if field == "counter"
            else field
        )
        needle = encoded(name) + b":" + str(candidate[name]).encode()
        raw = raw.replace(needle, encoded(name) + b":" + token, 1)
    refuse(parse(raw, target))


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    ("root_version", "root_extra", "invocation", "approver", "expected"),
    [
        (2, True, None, None, UNSUPPORTED),
        (0, True, None, None, UNSUPPORTED),
        (-1, True, None, None, UNSUPPORTED),
        (True, False, None, None, INVALID),
        (1, True, {"schema_version": 2}, {}, INVALID),
        (1, False, {"schema_version": 2, "extra": True}, {"schema_version": None}, UNSUPPORTED),
        (1, False, {"schema_version": 0, "extra": True}, {}, UNSUPPORTED),
        (1, False, {"schema_version": -1}, {}, UNSUPPORTED),
        (1, False, None, {"schema_version": 2}, INVALID),
        (
            1,
            False,
            {"schema_version": 1, "legacy_state": "retained", "filter": {}},
            {"schema_version": 2},
            INVALID,
        ),
        (1, False, {"schema_version": None}, {"schema_version": 2}, INVALID),
    ],
)
def test_deterministic_mixed_error_precedence(
    reverse: bool,
    root_version: Any,
    root_extra: bool,
    invocation: Any,
    approver: Any,
    expected: str,
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6: insertion order cannot reorder refusal stages."""
    candidate = policy()
    candidate["schema_version"] = root_version
    candidate["roles"] = {"invocation": invocation, "approver": approver}
    if root_extra:
        candidate["extra"] = "synthetic-extra"
    if reverse:
        candidate = dict(reversed(list(candidate.items())))
        candidate["roles"] = dict(reversed(list(candidate["roles"].items())))
    refuse(parse(candidate), expected)


@pytest.mark.parametrize(
    "path", ["tenant_id", "agent_id", "version", "roles", "roles.unrecognized"]
)
def test_envelope_errors_precede_unsupported_nested_roles(path: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = changed(policy(), "roles.invocation.schema_version", 2)
    refuse(parse(changed(candidate, path, None)))


@pytest.mark.parametrize("target", ["policy", "change"])
def test_large_integer_tokens_respect_existing_counter_domain(target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-4 AGENT-ROLE-FILTER-V1-6."""
    before = sys.get_int_max_str_digits()
    giant = b"9" * 5000
    candidate = policy() if target == "policy" else change()
    raw = encoded(candidate)
    versioned = raw.replace(b'"schema_version":1', b'"schema_version":' + giant, 1)
    assert len(versioned) < 65536
    refuse(parse(versioned, target), UNSUPPORTED)
    field = "version" if target == "policy" else "expected_version"
    counter = raw.replace(
        encoded(field) + b":" + str(candidate[field]).encode(), encoded(field) + b":" + giant, 1
    )
    assert len(counter) < 65536
    refuse(parse(counter, target))
    assert sys.get_int_max_str_digits() == before


def test_readback_revalidates_and_preserves_presence() -> None:
    """@spec AGENT-ROLE-FILTER-V1-3 AGENT-ROLE-FILTER-V1-6."""
    module = models()
    parsed = parse(policy())
    with pytest.raises(ValueError):
        parsed.version = 8
    mutated = parse(policy())
    mutated.roles.invocation.filter.entries.append(mutated.roles.invocation.filter.entries[0])
    refuse(api().render_agent_role_policy_v1(mutated))
    constructed = module.AgentRolePolicyV1.model_construct(**dict(policy(), version=True))
    refuse(api().render_agent_role_policy_v1(constructed))
    role = module.RetainedRoleConfigurationV1.model_construct(
        schema_version=True, legacy_state="retained", filter=None
    )
    roles = module.AgentRolesV1.model_construct(invocation=role)
    constructed = parsed.model_copy(update={"roles": roles})
    refuse(api().render_agent_role_policy_v1(constructed))
    impossible = parsed.model_copy(update={"version": 10**4999})
    before = sys.get_int_max_str_digits()
    refuse(api().render_agent_role_policy_v1(impossible))
    assert sys.get_int_max_str_digits() == before
    oversize = parse(policy())
    subject = oversize.roles.invocation.filter.entries[1]
    object.__setattr__(subject, "subject", "x" * 65536)
    refuse(api().render_agent_role_policy_v1(oversize))
    nested = parse(policy())
    object.__setattr__(nested.roles.invocation.filter.entries[0].namespace, "id", "not-a-uuid")
    refuse(api().render_agent_role_policy_v1(nested))


def test_unicode_readback_is_compact_and_has_no_new_identifier_ceiling() -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-6."""
    candidate = changed(
        policy(), "roles.invocation.filter.entries.1.subject", "\N{SNOWMAN}" + "x" * 4096
    )
    rendered = api().render_agent_role_policy_v1(parse(candidate))
    assert isinstance(rendered, bytes)
    assert "\N{SNOWMAN}".encode() in rendered
    assert b"\\u2603" not in rendered
    assert json.loads(rendered) == candidate


def test_code_only_refusals_never_echo_input_details() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    marker = "SYNTHETIC_PRIVATE_DETAIL_SENTINEL"
    for target in ["policy", "change"]:
        candidate = policy() if target == "policy" else change()
        candidate["secret_field_path"] = marker
        result = parse(candidate, target)
        refuse(result)
        for rendered in [str(result), repr(result), result.model_dump_json()]:
            assert marker not in rendered
            assert "secret_field_path" not in rendered
            assert "tenant_id" not in rendered
            assert candidate["tenant_id"] not in rendered


def test_import_has_no_runtime_initialization() -> None:
    """@spec AGENT-ROLE-FILTER-V1-1: no app, DB, provider, worker or external IO."""
    api()
    script = r"""
import builtins
import importlib.abc
import io
import json
import os
import socket
import sys
forbidden = ("curie_api.main", "curie_api.app", "curie_api.db", "curie_api.config",
             "curie_api.models", "curie_api.agent_access", "curie_api.routers",
             "curie_api.team_authorization", "sqlalchemy", "asyncpg", "httpx",
             "requests", "redis", "boto3", "kubernetes", "curie_worker")
class RuntimeForbidden(importlib.abc.MetaPathFinder):
    # @spec AGENT-ROLE-FILTER-V1-1
    def find_spec(self, fullname, path=None, target=None):
        # @spec AGENT-ROLE-FILTER-V1-1
        if any(fullname == p or fullname.startswith(p + ".") for p in forbidden):
            raise AssertionError("pure role filter imported runtime: " + fullname)
sys.meta_path.insert(0, RuntimeForbidden())
from curie_api.agent_role_filter import (
    parse_agent_role_policy_v1, parse_agent_role_change_v1, render_agent_role_policy_v1,
)

def no_io(*args, **kwargs):
    # @spec AGENT-ROLE-FILTER-V1-1
    raise AssertionError("pure role filter attempted external IO")
builtins.open = io.open = os.open = no_io
socket.socket = socket.create_connection = socket.getaddrinfo = no_io
policy, change = sys.argv[1:]
for _ in range(2):
    parsed = parse_agent_role_policy_v1(policy.encode())
    assert json.loads(render_agent_role_policy_v1(parsed)) == json.loads(policy)
    assert parse_agent_role_change_v1(change.encode()).model_dump(mode="json") == json.loads(change)
assert not any(n == p or n.startswith(p + ".") for n in sys.modules for p in forbidden)
print("pure role filter import and repeated parsing passed")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(policy()), json.dumps(change())],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "pure role filter import and repeated parsing passed"


@pytest.mark.parametrize(
    ("target", "path"),
    [
        ("policy", path)
        for path in [
            "schema_version",
            "tenant_id",
            "agent_id",
            "version",
            "roles",
            "roles.invocation.schema_version",
            "roles.invocation.legacy_state",
            "roles.invocation.filter",
            "roles.invocation.filter.entries",
            "roles.invocation.filter.entries.0.kind",
            "roles.invocation.filter.entries.0.provider",
            "roles.invocation.filter.entries.0.native_id",
            "roles.invocation.filter.entries.0.namespace",
            "roles.invocation.filter.entries.0.namespace.id",
            "roles.invocation.filter.entries.0.namespace.authority",
            "roles.invocation.filter.entries.0.namespace.kind",
            "roles.invocation.filter.entries.0.namespace.key",
            "roles.invocation.filter.entries.1.issuer",
            "roles.invocation.filter.entries.1.subject",
            "roles.invocation.filter.entries.2.group",
            "roles.invocation.filter.entries.2.group.issuer",
            "roles.invocation.filter.entries.2.group.id",
        ]
    ]
    + [
        ("change", path)
        for path in [
            "schema_version",
            "tenant_id",
            "agent_id",
            "expected_version",
            "role",
            "operation",
            "filter",
        ]
    ],
)
def test_required_members_are_never_defaulted(target: str, path: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-3 AGENT-ROLE-FILTER-V1-4."""
    candidate = policy() if target == "policy" else change()
    keys = path.split(".")
    node = candidate
    for key in keys[:-1]:
        node = node[int(key)] if isinstance(node, list) else node[key]
    del node[keys[-1]]
    refuse(parse(candidate, target))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("tenant_id", OTHER_UUID.upper()),
        ("tenant_id", OTHER_UUID.replace("-", "")),
        ("agent_id", "not-a-uuid"),
        ("roles.invocation.filter.entries", {}),
        ("roles.invocation.filter.entries.0.kind", "Surface_account"),
        ("roles.invocation.filter.entries.0.provider", ""),
        ("roles.invocation.filter.entries.0.provider", True),
        ("roles.invocation.filter.entries.0.native_id", ""),
        ("roles.invocation.filter.entries.0.native_id", 1),
        ("roles.invocation.filter.entries.0.namespace.authority", None),
        ("roles.invocation.filter.entries.0.namespace.kind", ""),
        ("roles.invocation.filter.entries.0.namespace.key", ""),
        ("roles.invocation.filter.entries.0.namespace.id", OTHER_UUID.upper()),
        ("roles.invocation.filter.entries.1.subject", ""),
        ("roles.invocation.filter.entries.1.subject", False),
        ("roles.invocation.filter.entries.1.issuer", "https://user@identity.example.test"),
        ("roles.invocation.filter.entries.1.issuer", "https://identity.example.test?q=1"),
        ("roles.invocation.filter.entries.1.issuer", " https://identity.example.test"),
        ("roles.invocation.filter.entries.2.group.id", ""),
    ],
)
def test_strict_selector_and_scope_grammars(path: str, value: Any) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-6."""
    refuse(parse(changed(policy(), path, value)))


@pytest.mark.parametrize(
    "issuer",
    [
        "HTTPS://Identity.example.test/Exact/../Issuer",
        "https://identity.example.test/empty?",
        "https://identity.example.test/empty#",
        "https://identity.example.test/" + "x" * 4096,
    ],
)
def test_imported_issuer_semantics_remain_exact(issuer: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2: no configuration-specific URL hardening."""
    candidate = changed(policy(), "roles.invocation.filter.entries.1.issuer", issuer)
    assert readback(candidate) == candidate


def test_approver_version_and_cross_role_consistency_follow_full_invocation_validation() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = policy()
    candidate["roles"]["approver"] = copy.deepcopy(candidate["roles"]["invocation"])
    candidate["roles"]["approver"]["schema_version"] = 2
    candidate["roles"]["approver"]["filter"]["entries"][0]["namespace"]["key"] = "Contradiction"
    refuse(parse(candidate), UNSUPPORTED)
    candidate["roles"]["invocation"]["filter"]["entries"].append(
        copy.deepcopy(candidate["roles"]["invocation"]["filter"]["entries"][0])
    )
    refuse(parse(candidate))
    raw = encoded(policy()).replace(
        b'"roles":{"invocation":{"schema_version":1',
        b'"roles":{"invocation":{"schema_version":' + b"9" * 5000,
        1,
    )
    refuse(parse(raw), UNSUPPORTED)


def test_models_revalidate_internal_instances_and_reject_constructed_nested_inputs() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    module = models()
    valid = parse(policy())
    assert isinstance(module.AgentRolePolicyV1.model_validate(valid), module.AgentRolePolicyV1)
    bad_entry = module.SurfaceAccountSelectorV1.model_construct(
        **dict(policy()["roles"]["invocation"]["filter"]["entries"][0], native_id=True)
    )
    with pytest.raises(ValueError):
        module.SurfaceAccountSelectorV1.model_validate(bad_entry)
    filt = module.RoleFilterV1.model_construct(entries=[bad_entry])
    role = module.RetainedRoleConfigurationV1.model_construct(
        schema_version=1, legacy_state="retained", filter=filt
    )
    roles = module.AgentRolesV1.model_construct(invocation=role)
    forged = valid.model_copy(update={"roles": roles})
    with pytest.raises(ValueError):
        module.AgentRolePolicyV1.model_validate(forged)
    refuse(api().render_agent_role_policy_v1(forged))
    valid.roles.invocation.filter.entries.append(valid.roles.invocation.filter.entries[0])
    with pytest.raises(ValueError):
        module.AgentRolePolicyV1.model_validate(valid)


def test_rendered_byte_ceiling_is_applied_to_mutated_models() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    candidate = changed(policy(), "roles.invocation.filter.entries.1.subject", "x")
    raw = encoded(candidate)
    subject = "x" * (65536 - len(raw) + 1)
    candidate = changed(candidate, "roles.invocation.filter.entries.1.subject", subject)
    assert len(encoded(candidate)) == 65536
    parsed = parse(candidate)
    output = api().render_agent_role_policy_v1(parsed)
    assert isinstance(output, bytes)
    assert len(output) == 65536
    object.__setattr__(parsed.roles.invocation.filter.entries[1], "subject", subject + "x")
    refuse(api().render_agent_role_policy_v1(parsed))


def test_selector_kinds_do_not_infer_aliases_between_people() -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-6."""
    candidate = policy()
    entries = candidate["roles"]["invocation"]["filter"]["entries"]
    entries[0]["native_id"] = entries[1]["subject"] = entries[2]["group"]["id"] = "SharedExactText"
    assert readback(candidate) == candidate
