"""Pure synthetic configuration conformance, without runtime qualification.

@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-2
@spec HTTP-AUTHORITY-CONFIG-V1-3 HTTP-AUTHORITY-CONFIG-V1-4
@spec HTTP-AUTHORITY-CONFIG-V1-5 HTTP-AUTHORITY-CONFIG-V1-6
@spec HTTP-AUTHORITY-CONFIG-V1-7
"""

from __future__ import annotations

import copy
import importlib
import json
import subprocess
import sys
from collections import UserDict
from types import MappingProxyType, ModuleType
from typing import Any
from uuid import UUID

import pytest
from curie_api.schemas.directory import DirectoryConfigIn
from pydantic import ValidationError

TENANT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
INVALID = "invalid_authority_configuration"
UNSUPPORTED = "unsupported_authority_configuration_version"
MODES = ("disabled", "local_projection", "http")


def api() -> ModuleType:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6: missing code fails in test bodies."""
    name = "curie_api.schemas.http_authority"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "Pure authority configuration module is not implemented"
        raise
    for symbol in ("AuthorityConfigV1", "AuthorityConfigRefusal", "validate_authority_config_v1"):
        assert hasattr(module, symbol), (
            f"Pure authority configuration entry point missing: {symbol}"
        )
    return module


def configuration(mode: str = "http") -> dict[str, Any]:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1: independent synthetic candidate."""
    selections = {
        "disabled": {"mode": "disabled"},
        "local_projection": {
            "mode": "local_projection",
            "issuer": "https://Identity.example/Exact%2fIssuer",
            "governed_domains": ["a.example", "z.example"],
            "managed_by": "Acme.Directory_1",
        },
        "http": {
            "mode": "http",
            "issuer": "https://Identity.example/Exact%2fIssuer",
            "lookup_url": "https://Lookup.example:443/custom/%2fOperation",
            "credential_ref": {"id": "Acme_Credential-1", "generation": 13},
            "max_identity_age_ms": 101,
            "max_membership_age_ms": 202,
            "max_clock_skew_ms": 0,
        },
    }
    return {"schema_version": 1, "tenant_id": TENANT, "version": 7, "selection": selections[mode]}


def changed(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1: change one decoded input member."""
    result = copy.deepcopy(value)
    target = result
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = replacement
    return result


def validate(value: Any) -> Any:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6: exercise the actual pure boundary."""
    return api().validate_authority_config_v1(value)


def readback(value: Any) -> dict[str, Any]:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6: successful intentional readback."""
    assert isinstance(value, api().AuthorityConfigV1)
    return value.model_dump(mode="json")


def refuse(value: Any, code: str = INVALID) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6: code-only refusal, never a raw error."""
    assert isinstance(value, api().AuthorityConfigRefusal)
    assert value.code == code
    assert value.model_dump(mode="json") == {"code": code}


@pytest.mark.parametrize("mode", MODES)
def test_exact_selected_representation_has_no_readiness_or_activation_claim(mode: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-6 HTTP-AUTHORITY-CONFIG-V1-7."""
    candidate = configuration(mode)
    before = copy.deepcopy(candidate)
    assert readback(validate(candidate)) == before
    assert candidate == before


@pytest.mark.parametrize("wrapper", [dict, UserDict, MappingProxyType])
def test_decoded_mapping_is_not_a_json_or_bytes_parser(wrapper: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6."""
    candidate = configuration()
    assert readback(validate(wrapper(candidate))) == candidate


@pytest.mark.parametrize("value", [None, [], (), "{}", b"{}", 1, True])
def test_nonmapping_including_absence_is_refused_without_inventing_disabled(value: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-2 HTTP-AUTHORITY-CONFIG-V1-6."""
    refuse(validate(value))


@pytest.mark.parametrize(
    ("version", "code"),
    [
        (0, UNSUPPORTED),
        (2, UNSUPPORTED),
        (-1, UNSUPPORTED),
        (10**100, UNSUPPORTED),
        (True, INVALID),
        (False, INVALID),
        (1.0, INVALID),
        ("1", INVALID),
        (None, INVALID),
    ],
)
def test_top_schema_version_precedes_other_invalid_shape(version: Any, code: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-2 HTTP-AUTHORITY-CONFIG-V1-6."""
    refuse(
        validate({"schema_version": version, "selection": "invalid", "secret": "sentinel"}), code
    )


def test_missing_schema_version_and_supported_version_with_bad_union_are_invalid() -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6."""
    refuse(validate({}))
    refuse(validate({"schema_version": 1}))
    refuse(validate(changed(configuration(), "selection.mode", "unknown")))


@pytest.mark.parametrize("mode", ["HTTP", " http", "local", 1, True, b"http"])
def test_mode_discriminator_is_an_exact_string(mode: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    refuse(validate(changed(configuration(), "selection.mode", mode)))


@pytest.mark.parametrize("path", ["selection", "selection.credential_ref"])
@pytest.mark.parametrize("value", [[], ["id", "Acme"], "http", 1, False])
def test_nested_objects_cannot_be_replaced_by_scalars_or_sequences(path: str, value: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-4."""
    refuse(validate(changed(configuration(), path, value)))


REQUIRED_PATHS = (
    [
        (mode, path)
        for mode in MODES
        for path in ("schema_version", "tenant_id", "version", "selection", "selection.mode")
    ]
    + [
        (mode, "selection." + field)
        for mode in ("local_projection", "http")
        for field in ("issuer",)
    ]
    + [("local_projection", "selection." + field) for field in ("governed_domains", "managed_by")]
    + [
        ("http", "selection." + field)
        for field in (
            "lookup_url",
            "credential_ref",
            "credential_ref.id",
            "credential_ref.generation",
            "max_identity_age_ms",
            "max_membership_age_ms",
            "max_clock_skew_ms",
        )
    ]
)


@pytest.mark.parametrize(("mode", "path"), REQUIRED_PATHS)
@pytest.mark.parametrize("missing", [True, False], ids=["missing", "null"])
def test_every_mode_member_is_required_and_nonnull(mode: str, path: str, missing: bool) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-4 HTTP-AUTHORITY-CONFIG-V1-5."""
    candidate = configuration(mode)
    target = candidate
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[part]
    if missing:
        del target[parts[-1]]
    else:
        target[parts[-1]] = None
    refuse(validate(candidate))


@pytest.mark.parametrize(
    ("mode", "field", "value"),
    [
        ("disabled", field, value)
        for field, value in configuration()["selection"].items()
        if field != "mode"
    ]
    + [("disabled", "governed_domains", []), ("disabled", "managed_by", "Acme")]
    + [
        ("local_projection", field, value)
        for field, value in configuration()["selection"].items()
        if field not in ("mode", "issuer")
    ]
    + [("http", "governed_domains", []), ("http", "managed_by", "Acme")],
)
def test_modes_refuse_fields_owned_by_another_mode(mode: str, field: str, value: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    refuse(validate(changed(configuration(mode), "selection." + field, value)))


@pytest.mark.parametrize("path", ["", "selection", "selection.credential_ref"])
@pytest.mark.parametrize(
    "field",
    [
        "unexpected",
        "token",
        "credential",
        "secret",
        "token_file",
        "credential_path",
        "ready",
        "active",
        "expected_authority_version",
        "principal_id",
        "groups",
        "sender",
    ],
)
def test_unknown_secret_path_policy_and_activation_fields_never_enter_readback(
    path: str, field: str
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-4 HTTP-AUTHORITY-CONFIG-V1-6."""
    candidate = configuration()
    target = candidate
    for part in path.split(".") if path else []:
        target = target[part]
    target[field] = "synthetic-invalid-extra"
    refuse(validate(candidate))


@pytest.mark.parametrize("path", ["version", "selection.credential_ref.generation"])
@pytest.mark.parametrize("value", [0, -1, True, False, "1", 1.0])
def test_authority_version_and_credential_generation_are_strict_positive_integers(
    path: str, value: Any
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-2 HTTP-AUTHORITY-CONFIG-V1-4."""
    refuse(validate(changed(configuration(), path, value)))


def test_version_ownership_stays_distinct_from_schema_and_secret_generation() -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-2 HTTP-AUTHORITY-CONFIG-V1-4."""
    candidate = configuration()
    candidate["version"] = 1
    candidate["selection"]["credential_ref"]["generation"] = 10**100
    for _ in range(2):
        assert readback(validate(candidate)) == candidate
    refuse(validate(changed(candidate, "expected_version", 0)))
    refuse(validate(changed(candidate, "authority_version", 7)))
    refuse(validate(changed(candidate, "identity_epoch", 7)))
    # The existing directory administration create sentinel stays on its owner.
    directory = DirectoryConfigIn(
        issuer="https://identity.example",
        governed_domains=[],
        managed_by="Acme",
        expected_version=0,
    )
    assert directory.expected_version == 0
    with pytest.raises(ValidationError):
        DirectoryConfigIn(**directory.model_dump() | {"expected_version": -1})


@pytest.mark.parametrize(
    "value",
    [
        TENANT.upper(),
        TENANT.replace("-", ""),
        "{" + TENANT + "}",
        " " + TENANT,
        TENANT + " ",
        "not-a-uuid",
        UUID(TENANT),
        TENANT.encode(),
        1,
        True,
    ],
)
def test_tenant_is_only_an_exact_canonical_uuid_string(value: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    refuse(validate(changed(configuration(), "tenant_id", value)))


@pytest.mark.parametrize("mode", ["local_projection", "http"])
@pytest.mark.parametrize(
    "issuer",
    [
        "https://Identity.example/Exact%2fIssuer",
        "HTTPS://Identity.example/Exact/../Issuer",
        "https://identity.example/empty?",
        "https://identity.example/empty#",
    ],
)
def test_issuer_reuses_exact_t1_semantics_without_lookup_url_hardening(
    mode: str, issuer: str
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 HTTP-AUTHORITY-CONFIG-V1-3."""
    candidate = changed(configuration(mode), "selection.issuer", issuer)
    assert readback(validate(candidate)) == candidate


@pytest.mark.parametrize("mode", ["local_projection", "http"])
@pytest.mark.parametrize(
    "issuer",
    [
        "http://identity.example",
        "https:///missing",
        "https://@identity.example",
        "https://user:pass@identity.example",
        "https://identity.example?q=1",
        "https://identity.example#fragment",
        " https://identity.example",
        1,
        b"https://identity.example",
    ],
)
def test_configuration_rejects_invalid_or_coerced_issuer(mode: str, issuer: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    refuse(validate(changed(configuration(mode), "selection.issuer", issuer)))


@pytest.mark.parametrize("mode", ["local_projection", "http"])
def test_configuration_issuer_ceiling_is_2048_characters(mode: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    issuer = "https://identity.example/" + "x" * (2048 - len("https://identity.example/"))
    candidate = changed(configuration(mode), "selection.issuer", issuer)
    assert readback(validate(candidate)) == candidate
    refuse(validate(changed(candidate, "selection.issuer", issuer + "x")))


@pytest.mark.parametrize(
    ("domains", "expected"),
    [
        (["z.example", "a.example"], ["a.example", "z.example"]),
        (("z.example", "a.example"), ["a.example", "z.example"]),
        ({"z.example", "a.example"}, ["a.example", "z.example"]),
        ([], []),
        (["a" * 63 + ".example"], ["a" * 63 + ".example"]),
        ([".".join(["a" * 63] * 3 + ["a" * 61])], [".".join(["a" * 63] * 3 + ["a" * 61])]),
    ],
)
def test_local_domains_inherit_directory_collection_acceptance_and_sorted_order(
    domains: Any, expected: list[str]
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1: characterize the existing grammar owner."""
    selection = configuration("local_projection")["selection"]
    directory = DirectoryConfigIn(
        issuer=selection["issuer"],
        governed_domains=domains,
        managed_by=selection["managed_by"],
        expected_version=0,
    )
    assert directory.governed_domains == expected
    candidate = changed(configuration("local_projection"), "selection.governed_domains", domains)
    assert readback(validate(candidate))["selection"]["governed_domains"] == expected


@pytest.mark.parametrize(
    "domains",
    [
        ["UPPER.example"],
        ["a.example", "a.example"],
        ["-bad.example"],
        ["bad-.example"],
        ["a..example"],
        [".example"],
        ["example."],
        ["localhost"],
        ["a_b.example"],
        ["caf\N{LATIN SMALL LETTER E WITH ACUTE}.example"],
        ["a" * 64 + ".example"],
        [".".join(["a" * 63] * 4)],
        [f"a{i}.example" for i in range(101)],
        [1],
        "a.example",
    ],
)
def test_local_domains_preserve_existing_directory_grammar_rejections(domains: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    selection = configuration("local_projection")["selection"]
    with pytest.raises(ValidationError):
        DirectoryConfigIn(
            issuer=selection["issuer"],
            governed_domains=domains,
            managed_by=selection["managed_by"],
            expected_version=0,
        )
    refuse(
        validate(changed(configuration("local_projection"), "selection.governed_domains", domains))
    )


def test_new_contract_domain_strings_are_strict_without_changing_directory_coercion() -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    directory = DirectoryConfigIn(
        issuer="https://identity.example",
        governed_domains=[b"a.example"],
        managed_by="Acme",
        expected_version=0,
    )
    assert directory.governed_domains == ["a.example"]
    refuse(
        validate(
            changed(configuration("local_projection"), "selection.governed_domains", [b"a.example"])
        )
    )


@pytest.mark.parametrize(
    ("managed_by", "accepted"),
    [
        ("A", True),
        ("a._-Z9", True),
        ("A" * 128, True),
        ("", False),
        ("A" * 129, False),
        ("_first", False),
        ("a/b", False),
        ("a b", False),
        ("a\n", False),
        (1, False),
        (b"Acme", False),
    ],
)
def test_local_managed_by_reuses_directory_validation(managed_by: Any, accepted: bool) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    kwargs = {
        "issuer": "https://identity.example",
        "governed_domains": [],
        "managed_by": managed_by,
        "expected_version": 0,
    }
    candidate = changed(configuration("local_projection"), "selection.managed_by", managed_by)
    if accepted:
        assert DirectoryConfigIn(**kwargs).managed_by == managed_by
        assert readback(validate(candidate))["selection"]["managed_by"] == managed_by
    else:
        with pytest.raises(ValidationError):
            DirectoryConfigIn(**kwargs)
        refuse(validate(candidate))


@pytest.mark.parametrize(
    "url",
    [
        "https://Lookup.example",
        "https://Lookup.example/",
        "HTTPS://Lookup.example/Exact%2fPath",
        "https://Lookup.example:1/custom/../operation",
        "https://Lookup.example:65535/%3f%23",
        "https://[2001:db8::1]:443/lookup",
        "https://127.0.0.1/private",
        "https://localhost/custom",
    ],
)
def test_lookup_is_the_exact_https_operation_url_without_network_or_ssrf_qualification(
    url: str,
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-3."""
    candidate = changed(configuration(), "selection.lookup_url", url)
    assert readback(validate(candidate)) == candidate


@pytest.mark.parametrize(
    "url",
    [
        "",
        "/lookup",
        "//identity.example/lookup",
        "http://identity.example/lookup",
        "https:///lookup",
        "https://",
        "https://user@identity.example/lookup",
        "https://user:pass@identity.example/lookup",
        "https://@identity.example/lookup",
        "https://identity.example/lookup?",
        "https://identity.example/lookup?q=1",
        "https://identity.example/lookup#",
        "https://identity.example/lookup#fragment",
        " https://identity.example/lookup",
        "https://identity.example/lookup ",
        "https://identity.example:0/lookup",
        "https://identity.example:65536/lookup",
        "https://identity.example:-1/lookup",
        "https://identity.example:abc/lookup",
        "https://identity.example:/lookup",
        "https://identity.example:1.0/lookup",
        "https://identity.example:+443/lookup",
        "https://identity.example:٤٤٣/lookup",
        "https://[not-ipv6]/lookup",
        "https://identity.example\\lookup",
        1,
        b"https://identity.example",
    ],
)
def test_lookup_rejects_unsafe_or_nonabsolute_or_coerced_url_shapes(url: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-3."""
    refuse(validate(changed(configuration(), "selection.lookup_url", url)))


@pytest.mark.parametrize("control", [chr(n) for n in range(32)] + [chr(127)])
def test_lookup_rejects_ascii_controls_before_a_url_parser_can_strip_them(control: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-3."""
    refuse(
        validate(
            changed(
                configuration(),
                "selection.lookup_url",
                "https://identity.example/a" + control + "b",
            )
        )
    )


@pytest.mark.parametrize("registry_id", ["A", "a" * 64, "Acme_Z9-credential", "1-_"])
def test_registry_reference_accepts_only_metadata_with_exact_case(registry_id: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-4."""
    candidate = changed(configuration(), "selection.credential_ref.id", registry_id)
    assert readback(validate(candidate)) == candidate


@pytest.mark.parametrize(
    "registry_id",
    [
        "",
        "a" * 65,
        "_first",
        "-first",
        " a",
        "a ",
        "a\n",
        "a/b",
        "a.b",
        "a:b",
        "https://secret.example/key",
        "/tmp/synthetic-key",
        "../key",
        "${KEY}",
        "$(id)",
        "a;id",
        "a\\b",
        "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        1,
        b"Acme",
    ],
)
def test_registry_reference_rejects_paths_urls_interpolation_and_coercion(registry_id: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-4."""
    refuse(validate(changed(configuration(), "selection.credential_ref.id", registry_id)))


@pytest.mark.parametrize(
    ("identity", "membership", "skew"),
    [(1, 10**100, 0), (10**100, 1, 10**100)],
)
def test_freshness_values_are_independent_explicit_inputs_without_an_added_ceiling(
    identity: int, membership: int, skew: int
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-5."""
    candidate = configuration()
    candidate["selection"].update(
        max_identity_age_ms=identity, max_membership_age_ms=membership, max_clock_skew_ms=skew
    )
    assert readback(validate(candidate)) == candidate


@pytest.mark.parametrize(
    "field", ["max_identity_age_ms", "max_membership_age_ms", "max_clock_skew_ms"]
)
@pytest.mark.parametrize("value", [-1, True, False, "1", 1.0])
def test_freshness_rejects_negative_or_coerced_inputs(field: str, value: Any) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-5."""
    refuse(validate(changed(configuration(), "selection." + field, value)))


@pytest.mark.parametrize("field", ["max_identity_age_ms", "max_membership_age_ms"])
def test_zero_age_is_not_filled_from_the_other_age_or_skew(field: str) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-5."""
    refuse(validate(changed(configuration(), "selection." + field, 0)))


@pytest.mark.parametrize(
    ("path", "marker"),
    [
        ("selection.lookup_url", "https://URL_SENTINEL.example/?secret=SECRET_SENTINEL"),
        ("selection.issuer", "https://URL_SENTINEL.example/?secret=SECRET_SENTINEL"),
        ("selection.credential_ref.id", "/tmp/PATH_SENTINEL/SECRET_SENTINEL"),
        ("secret", "SECRET_SENTINEL"),
        ("selection.credential_ref.token_file", "/tmp/PATH_SENTINEL/SECRET_SENTINEL"),
    ],
)
def test_refusal_str_repr_and_serialized_output_do_not_echo_invalid_secret_url_or_path(
    path: str,
    marker: str,
) -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-6."""
    candidate = changed(configuration(), path, marker)
    result = validate(candidate)
    refuse(result)
    rendered = (str(result), repr(result), json.dumps(result.model_dump(mode="json")))
    for output in rendered:
        assert marker not in output
        assert "SECRET_SENTINEL" not in output
        assert "URL_SENTINEL" not in output
        assert "PATH_SENTINEL" not in output
        assert "selection" not in output
        assert "lookup_url" not in output
        assert len(output) < 256


def test_pure_import_and_validation_require_no_runtime_or_external_io() -> None:
    """@spec HTTP-AUTHORITY-CONFIG-V1-2 HTTP-AUTHORITY-CONFIG-V1-4 HTTP-AUTHORITY-CONFIG-V1-7."""
    api()  # Intended missing-module failure remains a test assertion, never collection failure.
    script = r"""
import builtins
import importlib.abc
import io
import json
import os
import socket
import sys

forbidden = (
    "curie_api.main", "curie_api.app", "curie_api.db", "curie_api.config",
    "curie_api.models", "curie_api.directory_service", "curie_api.routers",
    "curie_api.forges", "curie_api.hook_signing", "sqlalchemy", "asyncpg",
    "httpx", "requests", "redis", "boto3", "kubernetes",
)
class RuntimeForbidden(importlib.abc.MetaPathFinder):
    # @spec HTTP-AUTHORITY-CONFIG-V1-7
    def find_spec(self, fullname, path=None, target=None):
        # @spec HTTP-AUTHORITY-CONFIG-V1-7
        if any(fullname == name or fullname.startswith(name + ".") for name in forbidden):
            raise AssertionError("pure configuration imported runtime: " + fullname)
sys.meta_path.insert(0, RuntimeForbidden())
from curie_api.schemas.http_authority import AuthorityConfigV1, validate_authority_config_v1

def no_io(*args, **kwargs):
    # @spec HTTP-AUTHORITY-CONFIG-V1-7
    raise AssertionError("pure configuration attempted external IO")
builtins.open = io.open = os.open = no_io
socket.socket = socket.create_connection = socket.getaddrinfo = no_io
candidates = json.loads(sys.argv[1])
for candidate in candidates:
    before = json.dumps(candidate, sort_keys=True)
    for _ in range(2):
        result = validate_authority_config_v1(candidate)
        assert isinstance(result, AuthorityConfigV1)
        assert result.model_dump(mode="json") == candidate
    assert json.dumps(candidate, sort_keys=True) == before
assert not any(name == prefix or name.startswith(prefix + ".")
               for name in sys.modules for prefix in forbidden)
print("pure import and repeated validation passed; no runtime activation")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps([configuration(mode) for mode in MODES])],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        result.stdout.strip() == "pure import and repeated validation passed; no runtime activation"
    )
