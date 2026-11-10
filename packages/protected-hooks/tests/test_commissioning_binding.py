"""Pure commissioning binding validation, @spec PROTECTED-COMMISSIONING-1/2."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Callable
from typing import Any

import pytest

from .test_authority_records import example_records
from .test_commissioning_bootstrap import bootstrap_value, encoded
from .test_commissioning_records import module, permit_value

Mutation = Callable[[dict[str, Any]], None]


def arguments(
    permit: dict[str, Any] | None = None,
    bootstrap: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Records rebound by digest after any mutation, @spec PROTECTED-COMMISSIONING-1."""
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap

    api = module()
    m = example_records()["manifest"] if manifest is None else manifest
    b = bootstrap_value() if bootstrap is None else bootstrap
    p = permit_value() if permit is None else permit
    return {
        "permit": api.parse_commissioning_permit(encoded(p)),
        "manifest": parse_manifest(encoded(m)),
        "bootstrap": parse_commissioning_bootstrap(encoded(b)),
        "broker": api.BrokerObservation(identity=copy.deepcopy(m["broker_identity"]), time_ms=1000),
        "trusted_issuer": copy.deepcopy(b["issuer_identity"]),
        "trusted_control": copy.deepcopy(b["control_identity"]),
        "max_permit_ms": 1000,
    }


def validate(args: dict[str, Any]) -> None:
    permit = args.pop("permit")
    assert module().validate_commissioning_binding(permit, **args) is None


def refuses(args: dict[str, Any]) -> None:
    with pytest.raises(module().CommissioningRecordInvalid):
        validate(args)


@pytest.mark.parametrize("now", [1000, 1999])
def test_exact_binding_accepts_issued_through_last_millisecond(now: int) -> None:
    args = arguments()
    args["broker"] = module().BrokerObservation(identity=args["broker"].identity, time_ms=now)
    validate(args)


@pytest.mark.parametrize("now", [0, 999, 2000, 2001])
def test_broker_time_outside_the_permit_interval_refuses(now: int) -> None:
    args = arguments()
    args["broker"] = module().BrokerObservation(identity=args["broker"].identity, time_ms=now)
    refuses(args)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("runtime_id", "99999999-9999-4999-8999-999999999999"),
        ("runtime_generation", "2"),
        ("manifest_digest", "0" * 64),
        ("bootstrap_digest", "0" * 64),
        ("execution_config_digest", "0" * 64),
    ],
)
def test_each_permit_scalar_binding_mismatch_refuses_alone(field: str, replacement: str) -> None:
    p = permit_value()
    p[field] = replacement
    refuses(arguments(permit=p))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p["broker_identity"].update(run_id="0" * 40),
        lambda p: p["broker_identity"].update(instance_id="99999999-9999-4999-8999-999999999999"),
        lambda p: p["broker_identity"]["endpoint"].update(port=6381),
        lambda p: p["broker_identity"].update(tls_spki_sha256="0" * 64),
        lambda p: p["guard_identity"].update(revision="4"),
        lambda p: p["guard_identity"].update(config_sha256="0" * 64),
        lambda p: p["guard_identity"].update(control_id="99999999-9999-4999-8999-999999999999"),
    ],
)
def test_permit_broker_and_guard_identity_must_equal_the_manifest(mutation: Mutation) -> None:
    p = permit_value()
    mutation(p)
    refuses(arguments(permit=p))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda b: b["broker_identity"].update(run_id="0" * 40),
        lambda b: b["broker_identity"].update(instance_id="99999999-9999-4999-8999-999999999999"),
        lambda b: b["broker_identity"]["endpoint"].update(host="other.example.test"),
    ],
)
def test_broker_observation_must_equal_the_manifest_including_run_id(mutation: Any) -> None:
    """A restarted broker refuses a retained permit, @spec PROTECTED-COMMISSIONING-1."""
    args = arguments()
    observed = {"broker_identity": args["broker"].identity}
    mutation(observed)
    args["broker"] = module().BrokerObservation(identity=observed["broker_identity"], time_ms=1000)
    refuses(args)


@pytest.mark.parametrize(
    "observation",
    [
        {"identity": {}, "time_ms": 1000},
        {"identity": None, "time_ms": 1000},
        {"identity": "broker", "time_ms": 1000},
        {"time_ms": True},
        {"time_ms": -1},
        {"time_ms": 9007199254740992},
        {"time_ms": "1000"},
        {"time_ms": 1000.0},
    ],
)
def test_malformed_broker_observation_refuses(observation: dict[str, Any]) -> None:
    api = module()
    identity = example_records()["manifest"]["broker_identity"]
    with pytest.raises(api.CommissioningRecordInvalid):
        api.BrokerObservation(**({"identity": identity} | observation))


def test_broker_observation_is_an_immutable_copy() -> None:
    api = module()
    identity = example_records()["manifest"]["broker_identity"]
    observation = api.BrokerObservation(identity=identity, time_ms=1000)
    identity["run_id"] = "0" * 40
    observation.identity["run_id"] = "1" * 40
    assert observation.identity == example_records()["manifest"]["broker_identity"]
    assert observation.run_id == "b" * 40
    assert "b" * 40 not in repr(observation)


def bound(bootstrap: dict[str, Any], permit: dict[str, Any] | None = None) -> dict[str, Any]:
    """Rebind a permit to a mutated B so only the B mutation is under test."""
    p = permit_value() if permit is None else permit
    p["bootstrap_digest"] = hashlib.sha256(encoded(bootstrap)).hexdigest()
    return arguments(permit=p, bootstrap=bootstrap)


def test_rebinding_helper_accepts_an_unchanged_bootstrap() -> None:
    validate(bound(bootstrap_value()))


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("runtime_id", "99999999-9999-4999-8999-999999999999"),
        ("runtime_generation", "2"),
        ("execution_config_digest", "0" * 64),
    ],
)
def test_bootstrap_runtime_must_equal_the_final_manifest(field: str, replacement: str) -> None:
    b = bootstrap_value()
    b[field] = replacement
    refuses(bound(b))


@pytest.mark.parametrize("role", ["enqueue", "worker", "verifier"])
def test_bootstrap_production_credentials_must_equal_manifest_credential_refs(role: str) -> None:
    b = bootstrap_value()
    b["production_credentials"][role]["generation"] = "99"
    refuses(bound(b))


@pytest.mark.parametrize("role", ["enqueue", "worker"])
def test_permit_candidate_credentials_must_equal_the_bootstraps(role: str) -> None:
    b = bootstrap_value()
    b["candidate_credentials"][role]["generation"] = "99"
    refuses(bound(b))


@pytest.mark.parametrize("field", ["issuer", "control"])
@pytest.mark.parametrize("where", ["permit", "bootstrap", "trusted"])
def test_issuer_and_control_must_equal_the_trusted_references(field: str, where: str) -> None:
    other = {"id": f"commissioning/other-{field}", "generation": "1"}
    if where == "permit":
        p = permit_value()
        p[f"{field}_identity"] = other
        args = arguments(permit=p)
    elif where == "bootstrap":
        b = bootstrap_value()
        b[f"{field}_identity"] = other
        args = bound(b)
    else:
        args = arguments()
        args[f"trusted_{field}"] = other
    refuses(args)


@pytest.mark.parametrize(
    "trusted",
    [
        {"id": "commissioning/example-issuer", "generation": 1},
        {"id": "commissioning/example-issuer", "generation": "1", "extra": "x"},
        {"id": "commissioning/example-issuer"},
        "commissioning/example-issuer",
        None,
    ],
)
def test_malformed_trusted_reference_refuses(trusted: Any) -> None:
    args = arguments()
    args["trusted_issuer"] = trusted
    refuses(args)


@pytest.mark.parametrize("candidate", ["issuer", "control", "enqueue", "worker"])
@pytest.mark.parametrize("production", ["enqueue", "worker", "verifier"])
def test_permit_credentials_never_alias_production_at_any_generation(
    candidate: str, production: str
) -> None:
    """A trusted reference cannot launder an alias, @spec PROTECTED-COMMISSIONING-1."""
    m = example_records()["manifest"]
    alias = {"id": m["credential_refs"][production]["id"], "generation": "999"}
    p = permit_value()
    if candidate in ("issuer", "control"):
        p[f"{candidate}_identity"] = alias
    else:
        p["candidate_credentials"][candidate] = alias
    args = arguments(permit=p)
    if candidate in ("issuer", "control"):
        args[f"trusted_{candidate}"] = copy.deepcopy(alias)
    refuses(args)


@pytest.mark.parametrize("candidate", ["issuer", "control", "enqueue", "worker"])
def test_bootstrap_refuses_commissioning_alias_of_production(candidate: str) -> None:
    """B cannot predeclare the alias the binding refuses, @spec PROTECTED-COMMISSIONING-2."""
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap

    b = bootstrap_value()
    alias = {"id": b["production_credentials"]["verifier"]["id"], "generation": "999"}
    if candidate in ("issuer", "control"):
        b[f"{candidate}_identity"] = alias
    else:
        b["candidate_credentials"][candidate] = alias
    with pytest.raises(module().CommissioningRecordInvalid):
        parse_commissioning_bootstrap(encoded(b))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p["sources"][0].update(generation="2"),
        lambda p: p["sources"][0].update(policy_fingerprint="0" * 64),
        lambda p: p["sources"][0].update(source_id="99999999-9999-4999-8999-999999999999"),
        lambda p: p.update(targets=["fixture/example-outside"]),
    ],
)
def test_permit_scope_must_be_within_the_bootstrap_scope(mutation: Mutation) -> None:
    p = permit_value()
    mutation(p)
    refuses(arguments(permit=p))


def test_permit_may_use_the_whole_bootstrap_scope() -> None:
    b = bootstrap_value()
    p = permit_value()
    p["sources"] = copy.deepcopy(b["source_scope"])
    p["targets"] = list(b["target_scope"])
    validate(arguments(permit=p))


@pytest.mark.parametrize(
    ("trusted", "declared", "accepts"),
    [
        (1000, "1000", True),
        (999, "1000", False),
        (1000, "999", False),
        (5000, "1000", True),
    ],
)
def test_permit_duration_is_bounded_by_trusted_and_bootstrap_maxima(
    trusted: int, declared: str, accepts: bool
) -> None:
    b = bootstrap_value()
    b["max_permit_ms"] = declared
    args = bound(b)
    args["max_permit_ms"] = trusted
    if accepts:
        validate(args)
    else:
        refuses(args)


@pytest.mark.parametrize("trusted", [0, -1, True, 1000.0, "1000", None])
def test_malformed_trusted_maximum_refuses(trusted: Any) -> None:
    args = arguments()
    args["max_permit_ms"] = trusted
    refuses(args)


@pytest.mark.parametrize("field", ["permit", "manifest", "bootstrap", "broker"])
def test_untyped_inputs_refuse(field: str) -> None:
    args = arguments()
    args[field] = encoded(permit_value()) if field != "broker" else {"time_ms": 1000}
    refuses(args)


def test_binding_signature_is_keyword_only_after_the_permit() -> None:
    args = arguments()
    with pytest.raises(TypeError):
        module().validate_commissioning_binding(args["permit"], args["manifest"], args["bootstrap"])
