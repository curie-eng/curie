"""Pure explicit G v2 and B/G/M binding, @spec COMMISSIONING-SERIES-3."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
from typing import Any

import pytest

from .test_authority_records import example_records
from .test_commissioning_bootstrap import encoded
from .test_commissioning_series_bootstrap import series_bootstrap_value

ROLES = (
    "enqueue",
    "worker",
    "verifier",
    "issuer",
    "control",
    "candidate_enqueue",
    "candidate_worker",
    "qualification_publisher",
    "control_reader",
    "source_writer",
    "observer",
    "evidence_observer",
)


def module() -> Any:
    name = "curie_protected_hooks.commissioning_series_guard"
    assert importlib.util.find_spec(name) is not None, "series guard v2 not implemented"
    return importlib.import_module(name)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def tuple_values() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    from curie_protected_hooks.commissioning_evidence_acl import evidence_observer_acl_rules

    b = series_bootstrap_value()
    b["entry_points"] = {
        "production": "composition/protected-production-v1",
        "commissioning": "composition/protected-commissioning-v1",
    }
    credentials = {
        **b["production_credentials"],
        "issuer": b["issuer_identity"],
        "control": b["control_identity"],
        "candidate_enqueue": b["candidate_credentials"]["enqueue"],
        "candidate_worker": b["candidate_credentials"]["worker"],
        "qualification_publisher": b["qualification_publisher_identity"],
        "control_reader": b["control_reader_identity"],
        "source_writer": b["source_writer_identity"],
        "evidence_observer": b["evidence_observer_identity"],
        "observer": {"id": "commissioning/example-acl-observer", "generation": "1"},
    }
    g = {
        "schema_version": 2,
        "kind": "commissioning_guard_config",
        "bootstrap_digest": sha(encoded(b)),
        "admission_policy_sha256": "a" * 64,
        "rbac_sha256": "b" * 64,
        "network_policy_sha256": "c" * 64,
        "broker_acl": {
            role: {"credential": credentials[role], "rules_sha256": "d" * 64} for role in ROLES
        },
    }
    g["broker_acl"]["evidence_observer"]["rules_sha256"] = sha(
        encoded(list(evidence_observer_acl_rules()))
    )
    m = copy.deepcopy(example_records()["manifest"])
    m["guard_identity"]["config_sha256"] = sha(encoded(g))
    return b, g, m


def test_explicit_series_guard_and_tuple_bind_without_runtime_authority() -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

    api = module()
    b, g, m = tuple_values()
    assert api.parse_series_guard_config(encoded(g)) == g
    assert (
        api.verify_series_tuple(
            parse_series_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m))
        )
        is None
    )
    fresh = api.parse_series_guard_config(encoded(g))
    fresh["broker_acl"]["evidence_observer"]["credential"]["id"] = "changed"
    assert api.parse_series_guard_config(encoded(g)) == g


@pytest.mark.parametrize("role", ROLES)
def test_guard_principal_inventory_is_closed_and_complete(role: str) -> None:
    api = module()
    _, g, _ = tuple_values()
    del g["broker_acl"][role]
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_guard_config(encoded(g))
    _, g, _ = tuple_values()
    g["broker_acl"]["extra"] = copy.deepcopy(g["broker_acl"]["issuer"])
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_guard_config(encoded(g))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda g: g.update(schema_version=1),
        lambda g: g.update(schema_version=2.0),
        lambda g: g.update(schema_version=True),
        lambda g: g.update(kind="other"),
        lambda g: g.update(unexpected="untrusted-private-marker"),
        lambda g: g["broker_acl"]["evidence_observer"].update(extra="value"),
        lambda g: g["broker_acl"]["evidence_observer"].update(rules_sha256="not-a-digest"),
    ],
)
def test_malformed_or_cross_version_guard_is_refused(mutation) -> None:
    api = module()
    _, g, _ = tuple_values()
    mutation(g)
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        api.parse_series_guard_config(encoded(g))
    assert "untrusted-private-marker" not in str(error.value)


@pytest.mark.parametrize("field", ["runtime_id", "runtime_generation", "execution_config_digest"])
def test_manifest_binding_rejects_a_different_runtime_or_configuration(field: str) -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

    api = module()
    b, g, m = tuple_values()
    replacements = {
        "runtime_id": "99999999-9999-4999-8999-999999999999",
        "runtime_generation": "9",
        "execution_config_digest": "9" * 64,
    }
    m[field] = replacements[field]
    with pytest.raises(api.CommissioningRecordInvalid):
        api.verify_series_tuple(
            parse_series_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m))
        )


@pytest.mark.parametrize("role", ROLES)
def test_each_guard_credential_must_match_its_predeclared_identity(role: str) -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

    api = module()
    b, g, m = tuple_values()
    g["broker_acl"][role]["credential"] = copy.deepcopy(b["mediator_identity"])
    m["guard_identity"]["config_sha256"] = sha(encoded(g))
    with pytest.raises(api.CommissioningRecordInvalid):
        api.verify_series_tuple(
            parse_series_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m))
        )


@pytest.mark.parametrize(
    "changed",
    ["bootstrap", "guard-hash", "evidence-rules", "production-credentials", "entry-point"],
)
def test_valid_record_rebindings_cannot_bypass_tuple_checks(changed: str) -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

    api = module()
    b, g, m = tuple_values()
    if changed == "bootstrap":
        g["bootstrap_digest"] = "9" * 64
    elif changed == "guard-hash":
        m["guard_identity"]["config_sha256"] = "9" * 64
    elif changed == "evidence-rules":
        g["broker_acl"]["evidence_observer"]["rules_sha256"] = "9" * 64
    elif changed == "production-credentials":
        m["credential_refs"]["enqueue"]["generation"] = "99"
    else:
        b["entry_points"]["commissioning"] = "composition/unreviewed"
        g["bootstrap_digest"] = sha(encoded(b))
    if changed != "guard-hash":
        m["guard_identity"]["config_sha256"] = sha(encoded(g))
    with pytest.raises(api.CommissioningRecordInvalid):
        api.verify_series_tuple(
            parse_series_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m))
        )


def test_version_one_guard_and_binding_still_refuse_series_records() -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_provisioner import (
        ProvisionerRefused,
        parse_guard_config,
        verify_tuple,
    )
    from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

    b, g, m = tuple_values()
    with pytest.raises(ProvisionerRefused):
        parse_guard_config(encoded(g))
    with pytest.raises(ProvisionerRefused):
        verify_tuple(parse_series_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m)))


@pytest.mark.parametrize(
    "raw", [b"", b"\xff", b"null", b"[]", b" " * 16385, b'{"schema_version":NaN}']
)
def test_guard_rejects_unreadable_malformed_or_oversized_bytes(raw: bytes) -> None:
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_guard_config(raw)


def test_duplicate_guard_member_is_not_normalized() -> None:
    api = module()
    _, g, _ = tuple_values()
    raw = encoded(g)[:-1] + b',"bootstrap_digest":"' + b"9" * 64 + b'"}'
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_guard_config(raw)


def test_series_binding_does_not_accept_a_version_one_bootstrap() -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap

    api = module()
    b, g, m = tuple_values()
    del b["evidence_observer_identity"]
    b["schema_version"] = 1
    with pytest.raises(api.CommissioningRecordInvalid):
        api.verify_series_tuple(
            parse_commissioning_bootstrap(encoded(b)), encoded(g), parse_manifest(encoded(m))
        )
