"""Schema2 identity separation is a structural claim, not effective RBAC proof."""

from __future__ import annotations

import hashlib
import itertools
from dataclasses import FrozenInstanceError
from typing import Any

import pytest
from curie_worker import commissioning_control_profile as cp

from .protected_configuration_vectors import canonical
from .test_commissioning_control_profile import CA, profile_value, projection

_PRINCIPALS = ("production_mediator", "commissioning_mediator", "provisioner", "guard_reader")


def value() -> dict[str, Any]:
    data = profile_value()
    data["schema_version"] = 2
    data["slot_policy"] = "host-custodied-cold-sdk-v1"
    del data["principals"]["mediator"]
    data["principals"].update(
        production_mediator="system:serviceaccount:acme-control:production-mediator",
        commissioning_mediator="system:serviceaccount:acme-control:commissioning-mediator",
        guard_reader="system:serviceaccount:acme-control:guard-reader",
    )
    return data


def parser() -> Any:
    result = getattr(cp, "parse_control_profile_v2", None)
    assert callable(result), "distinct v2 control parser is missing"
    return result


def test_v2_preserves_four_roles_without_granting_v1_render_or_binding_authority() -> None:
    data = value()
    raw = canonical(data)
    record = parser()(raw)
    assert type(record) is cp.ControlProfileV2
    assert record.canonical_bytes == raw
    assert record.digest == hashlib.sha256(raw).hexdigest()
    observed = record.as_dict()
    observed["principals"].clear()
    assert record.as_dict() == data
    with pytest.raises((FrozenInstanceError, AttributeError)):
        record._canonical_bytes = b"{}"
    with pytest.raises(cp.ControlProfileInvalid):
        cp.parse_control_profile(raw)
    with pytest.raises(cp.ControlProfileInvalid):
        parser()(canonical(profile_value()))
    with pytest.raises(cp.ControlProfileInvalid):
        cp.validate_control_profile_binding(
            record, admission_policy_sha256=record.digest, projection=projection()
        )
    with pytest.raises(cp.ControlProfileInvalid):
        cp.render_control_objects(record, projection=projection(), server_ca_pem=CA)


@pytest.mark.parametrize("left,right", list(itertools.combinations(_PRINCIPALS, 2)))
def test_all_four_principals_must_be_distinct(left: str, right: str) -> None:
    data = value()
    data["principals"][left] = data["principals"][right]
    with pytest.raises(cp.ControlProfileInvalid):
        parser()(canonical(data))


@pytest.mark.parametrize("role", _PRINCIPALS)
@pytest.mark.parametrize(
    "user",
    [
        "system:anonymous",
        "system:apiserver",
        "system:kube-scheduler",
        "system:kube-controller-manager",
        "system:node:acme-node",
        "system:masters",
        "system:serviceaccount:kube-system:unlisted-controller",
        "system:serviceaccount:bad",
        "",
    ],
)
def test_noncontroller_roles_cannot_use_reserved_or_system_identities(role: str, user: str) -> None:
    data = value()
    data["principals"][role] = user
    with pytest.raises(cp.ControlProfileInvalid):
        parser()(canonical(data))


@pytest.mark.parametrize("version", [1, 3, True, 2.0, "2", None])
def test_version_is_exactly_integer_two(version: Any) -> None:
    data = value()
    data["schema_version"] = version
    with pytest.raises(cp.ControlProfileInvalid):
        parser()(canonical(data))


@pytest.mark.parametrize(
    "mutation",
    [
        "old-policy",
        "old-mediator",
        "missing-role",
        "extra-role",
        "unknown-minor",
        "unsorted-minors",
        "repeated-controller",
        "missing-controller",
        "guard-image",
        "guard-timeout",
        "same-namespace",
        "bad-projection",
        "extra-top",
    ],
)
def test_v2_preserves_closed_profile_and_controller_rules(mutation: str) -> None:
    data = value()
    if mutation == "old-policy":
        data["slot_policy"] = "launch-id-names-v1"
    elif mutation == "old-mediator":
        data["principals"]["mediator"] = "acme-old-mediator"
    elif mutation == "missing-role":
        del data["principals"]["guard_reader"]
    elif mutation == "extra-role":
        data["principals"]["extra"] = "acme-extra"
    elif mutation == "unknown-minor":
        data["kubernetes_minors"] = ["1.33"]
    elif mutation == "unsorted-minors":
        data["kubernetes_minors"] = ["1.35", "1.34"]
    elif mutation == "repeated-controller":
        data["principals"]["pod_binders"] *= 2
    elif mutation == "missing-controller":
        data["principals"]["pod_binders"] = []
    elif mutation == "guard-image":
        data["guard"]["image"] = "registry.example.com/acme-guard:latest"
    elif mutation == "guard-timeout":
        data["guard"]["timeout_seconds"] = True
    elif mutation == "same-namespace":
        data["control_namespace"] = data["workload_namespace"]
    elif mutation == "bad-projection":
        data["projection_digest"] = "bad"
    elif mutation == "extra-top":
        data["unknown"] = True
    with pytest.raises(cp.ControlProfileInvalid):
        parser()(canonical(data))


def test_empty_status_controllers_and_user_principals_remain_structurally_valid() -> None:
    data = value()
    data["principals"]["pod_status_controllers"] = []
    for role in _PRINCIPALS:
        data["principals"][role] = "acme-" + role
    assert parser()(canonical(data)).as_dict() == data


@pytest.mark.parametrize("bad", [b"{}", b"null", b"[]", b"\xff", b"NaN", b"1.0", b"x" * 16385])
def test_errors_remain_input_free_and_detached(bad: bytes) -> None:
    with pytest.raises(cp.ControlProfileInvalid) as err:
        parser()(bad)
    assert str(err.value) == "Invalid protected control profile"
    assert err.value.__cause__ is None and err.value.__context__ is None


def test_noncanonical_duplicate_mutable_and_direct_invalid_records_refuse() -> None:
    raw = canonical(value())
    for bad in [b" " + raw, raw + b"\n", b'{"schema_version":2,' + raw[1:], bytearray(raw)]:
        with pytest.raises(cp.ControlProfileInvalid):
            parser()(bad)
    cls = getattr(cp, "ControlProfileV2", None)
    assert cls is not None
    with pytest.raises(cp.ControlProfileInvalid):
        cls(b"{}")
