"""Pure provisioner core: pinned inputs and B, G, M hash order, @spec PROTECTED-COMMISSIONING-2."""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import importlib
import importlib.util
import json
from collections.abc import Callable
from typing import Any

import pytest

from .test_authority_records import example_records
from .test_commissioning_bootstrap import bootstrap_value

RUNNER = "registry.example.test/runner@sha256:" + "e" * 64
WORKER = "registry.example.test/worker@sha256:" + "d" * 64
PRINCIPALS = (
    "enqueue",
    "worker",
    "verifier",
    "issuer",
    "control",
    "candidate_enqueue",
    "candidate_worker",
    "qualification_publisher",
    "observer",
)
CANDIDATES = ("candidate_enqueue", "candidate_worker")
OBSERVER = {"id": "commissioning/example-observer", "generation": "1"}


def minimal_rules(**changes: tuple[Any, ...]) -> dict[str, tuple[Any, ...]]:
    """Valid stand-in rules; candidates carry the design decision 9 placeholder."""
    rules: dict[str, tuple[Any, ...]] = {
        name: ("-@all", "+acl|whoami") + (("~{lane}:*",) if name in CANDIDATES else ())
        for name in PRINCIPALS
    }
    rules.update(changes)
    return rules


Mutation = Callable[[dict[str, Any]], None]


def _set(path: tuple[Any, ...], value: Any) -> Mutation:
    def mutate(target: dict[str, Any]) -> None:
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return mutate


def module() -> Any:
    name = "curie_protected_hooks.commissioning_provisioner"
    assert importlib.util.find_spec(name) is not None, "provisioner core not implemented"
    return importlib.import_module(name)


def canonical(value: Any) -> bytes:
    """Independent canonical byte oracle, @spec PROTECTED-COMMISSIONING-2."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def substrate() -> dict[str, str]:
    return {
        "authority_domain_id": "55555555-5555-4555-8555-555555555555",
        "launch_identity": "launch/example-v1",
    }


def projection_value() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "format": "kubernetes-cold-pod-v1",
        **substrate(),
        "namespace": "owned-runner",
        "image": {"reference": RUNNER, "digest": "sha256:" + "e" * 64},
        "embedded_bundle": {
            "object_identity": "bundle/example-v1",
            "sha256": "f" * 64,
            "path": "/bundles/current",
        },
        "delivery_slots": {
            "schema_version": 1,
            "execution_identity": "verified-delivery-v1",
            "history_binding": "protected-history-v1",
            "runner_token": "fresh-owned-launch-v1",
            "history_token": "scoped-history-credential-v1",
            "workload_identity": "control-plane-cold-create-v1",
            "dial_identity": "owned-workload-endpoint-v1",
        },
    }


def execution_value(projection: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "runner": {"fake_model": False, "model": "example-model"},
        "policy": {"tool_access": "read-only"},
        "launch": {
            "runner_image_digest": "sha256:" + "e" * 64,
            "substrate": {
                "kind": "kubernetes",
                **substrate(),
                "launch_config_sha256": sha(projection),
            },
        },
        "bindings": {"qualified_connectors": []},
    }


def mapping() -> dict[str, Any]:
    return {
        "os": "linux",
        "architecture": "amd64",
        "variant": None,
        "runtime_profile": "runtime/example",
        "pinned_image_reference": RUNNER,
        "oci_digest": "sha256:" + "e" * 64,
        "observed_image_id": "sha256:" + "9" * 64,
        "image_config_id": None,
    }


def profile_fields(**changes: Any) -> dict[str, Any]:
    m = example_records()["manifest"]
    b = bootstrap_value()
    projection = canonical(projection_value())
    fields: dict[str, Any] = {
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "qualification_id": m["qualification_id"],
        "broker_identity": copy.deepcopy(m["broker_identity"]),
        "guard_control_id": m["guard_identity"]["control_id"],
        "guard_revision": m["guard_identity"]["revision"],
        "worker_image": WORKER,
        "runner_image": RUNNER,
        "bundle_digest": copy.deepcopy(m["bundle_digest"]),
        "projection": projection,
        "execution_config": canonical(execution_value(projection)),
        "platform_mappings": [mapping()],
        "enforcement_evidence_ref": "sha256:" + "7" * 64,
        "production_credentials": copy.deepcopy(m["credential_refs"]),
        "candidate_credentials": copy.deepcopy(b["candidate_credentials"]),
        "issuer_identity": copy.deepcopy(b["issuer_identity"]),
        "control_identity": copy.deepcopy(b["control_identity"]),
        "qualification_publisher_identity": copy.deepcopy(b["qualification_publisher_identity"]),
        "observer_identity": copy.deepcopy(OBSERVER),
        "mediator_identity": copy.deepcopy(b["mediator_identity"]),
        "gateway_identity": copy.deepcopy(b["gateway_identity"]),
        "source_scope": copy.deepcopy(b["source_scope"]),
        "target_scope": list(b["target_scope"]),
        "max_permit_ms": "1000",
        "admission_policy_sha256": "a1" * 32,
        "rbac_sha256": "b2" * 32,
        "network_policy_sha256": "c3" * 32,
    }
    fields.update(changes)
    return fields


def profile(**changes: Any) -> Any:
    return module().ProvisionerProfile(**profile_fields(**changes))


class Recipes:
    """Stand-in for the broker ACL recipe seam; the real recipes are measured elsewhere."""

    def __init__(self, rules: dict[str, tuple[str, ...]] | None = None) -> None:
        self.calls: list[str] = []
        self._rules = rules or {
            name: (
                "-@all",
                "resetkeys",
                "+acl|whoami",
                "%R~protected:example:" + name + (":{lane}" if name in CANDIDATES else "") + ":*",
            )
            for name in PRINCIPALS
        }

    def rules(self, principal: str) -> tuple[str, ...]:
        self.calls.append(principal)
        return self._rules[principal]


def built(**changes: Any) -> Any:
    return module().construct(profile(**changes), acl=Recipes())


def test_entry_points_are_predeclared_distinct_opaque_refs() -> None:
    api = module()
    assert api.PRODUCTION_ENTRY_POINT != api.COMMISSIONING_ENTRY_POINT
    result = built()
    assert result.bootstrap.as_dict()["entry_points"] == {
        "production": api.PRODUCTION_ENTRY_POINT,
        "commissioning": api.COMMISSIONING_ENTRY_POINT,
    }
    assert set(api.ENTRY_POINT_COMPOSITIONS) == {
        api.PRODUCTION_ENTRY_POINT,
        api.COMMISSIONING_ENTRY_POINT,
    }
    # The production entry point names exactly the existing composition.
    assert api.ENTRY_POINT_COMPOSITIONS[api.PRODUCTION_ENTRY_POINT] == (
        "curie_api.protected_runtime_files:load_ingress",
        "curie_worker.protected_entry:open_runtime",
    )


def test_profile_has_no_entry_point_selection_member() -> None:
    names = {f.name for f in dataclasses.fields(module().ProvisionerProfile)}
    assert not names & {"entry_points", "entry_point", "context", "kubeconfig"}


def test_bootstrap_carries_the_pinned_inputs_and_nothing_downstream_of_it() -> None:
    result = built()
    b = result.bootstrap.as_dict()
    fields = profile_fields()
    assert b["execution_config_digest"] == sha(fields["execution_config"])
    for key in (
        "runtime_id",
        "runtime_generation",
        "production_credentials",
        "candidate_credentials",
        "issuer_identity",
        "control_identity",
        "qualification_publisher_identity",
        "mediator_identity",
        "gateway_identity",
        "source_scope",
        "target_scope",
        "max_permit_ms",
    ):
        assert b[key] == fields[key]
    raw = result.bootstrap.canonical_bytes
    for later in (result.guard_digest, result.manifest.digest, result.measurement.digest):
        assert later.encode() not in raw


def test_hash_order_b_then_g_then_m() -> None:
    result = built()
    guard = json.loads(result.guard_config)
    assert guard["bootstrap_digest"] == result.bootstrap.digest
    assert result.guard_digest == sha(result.guard_config)
    m = result.manifest.as_dict()
    assert m["guard_identity"] == {
        "control_id": profile_fields()["guard_control_id"],
        "revision": profile_fields()["guard_revision"],
        "config_sha256": result.guard_digest,
    }
    assert result.manifest.digest.encode() not in result.guard_config


def test_every_bootstrap_change_reaches_g_and_m() -> None:
    first = built()
    second = built(max_permit_ms="2000")
    assert first.bootstrap.digest != second.bootstrap.digest
    assert first.guard_digest != second.guard_digest
    assert first.manifest.digest != second.manifest.digest


def test_manifest_pins_images_projection_and_execution_config() -> None:
    result = built()
    m = result.manifest.as_dict()
    fields = profile_fields()
    assert m["worker_image_digest"] == "sha256:" + "d" * 64
    assert m["runner_image_digest"] == "sha256:" + "e" * 64
    assert m["bundle_digest"] == fields["bundle_digest"]
    assert m["execution_config_digest"] == sha(fields["execution_config"])
    assert m["substrate"] == {
        "kind": "kubernetes",
        **substrate(),
        "launch_config_sha256": sha(fields["projection"]),
    }
    assert m["credential_refs"] == fields["production_credentials"]
    assert m["broker_identity"] == fields["broker_identity"]
    assert m["qualification_id"] == fields["qualification_id"]


def test_guard_hashes_each_principals_rules_bound_to_its_credential() -> None:
    recipes = Recipes()
    result = module().construct(profile(), acl=recipes)
    guard = json.loads(result.guard_config)
    assert sorted(recipes.calls) == sorted(PRINCIPALS)
    fields = profile_fields()
    credentials = {
        **fields["production_credentials"],
        "issuer": fields["issuer_identity"],
        "control": fields["control_identity"],
        "qualification_publisher": fields["qualification_publisher_identity"],
        "candidate_enqueue": fields["candidate_credentials"]["enqueue"],
        "candidate_worker": fields["candidate_credentials"]["worker"],
        "observer": OBSERVER,
    }
    assert set(guard["broker_acl"]) == set(PRINCIPALS)
    for name in PRINCIPALS:
        rules = list(recipes.rules(name))
        assert guard["broker_acl"][name] == {
            "credential": credentials[name],
            "rules_sha256": sha(canonical(rules)),
        }
    for key in ("admission_policy_sha256", "rbac_sha256", "network_policy_sha256"):
        assert guard[key] == fields[key]


def test_guard_config_is_canonical_and_round_trips() -> None:
    api = module()
    result = built()
    assert result.guard_config == canonical(json.loads(result.guard_config))
    parsed = api.parse_guard_config(result.guard_config)
    assert parsed == json.loads(result.guard_config)


@pytest.mark.parametrize(
    "rule",
    [
        "on",
        "off",
        ">secret",
        "<secret",
        "#" + "a" * 64,
        "!" + "a" * 64,
        "nopass",
        "resetpass",
        "(+get on)",
        "ON",
        "Off",
        "NoPass",
        "RESETPASS",
        "reset",
        "Reset",
        "(+get RESET)",
        "",
        " ",
        "+get\n",
    ],
)
def test_guard_refuses_rules_carrying_enabled_state_or_passwords(rule: str) -> None:
    api = module()
    rules = minimal_rules(issuer=("-@all", rule))
    with pytest.raises(api.ProvisionerRefused) as raised:
        api.construct(profile(), acl=Recipes(rules))
    assert "secret" not in str(raised.value)


@pytest.mark.parametrize("value", [(), ["-@all"], ("-@all", 7), None])
def test_guard_refuses_malformed_rule_recipes(value: Any) -> None:
    api = module()
    rules: dict[str, Any] = minimal_rules()
    rules["issuer"] = value
    with pytest.raises(api.ProvisionerRefused):
        api.construct(profile(), acl=Recipes(rules))


def MergedRecipes(qualification_id: str) -> Any:
    """The merged recipes G hashes; candidates as design decision 9 templates."""
    from curie_protected_hooks.commissioning_acl import GuardRecipes

    return GuardRecipes(qualification_id)


def test_guard_accepts_the_merged_recipes() -> None:
    recipes = MergedRecipes(profile_fields()["qualification_id"])
    # The production worker and candidate worker carry the long measured command selector.
    assert max(len(rule) for name in PRINCIPALS for rule in recipes.rules(name)) > 512
    guard = json.loads(module().construct(profile(), acl=recipes).guard_config)
    for name in PRINCIPALS:
        assert guard["broker_acl"][name]["rules_sha256"] == sha(
            canonical(list(recipes.rules(name)))
        )


def test_guard_hashes_candidate_templates_not_a_campaigns_lane() -> None:
    """Design decision 9: G is built before any permit."""
    from curie_protected_hooks.commissioning_acl import (
        commissioning_acl_rules,
        commissioning_acl_template,
    )

    from .test_commissioning_records import permit_value

    api = module()
    recipes = MergedRecipes(profile_fields()["qualification_id"])
    guard = json.loads(api.construct(profile(), acl=recipes).guard_config)
    for name in CANDIDATES:
        template = list(commissioning_acl_template(name))
        assert guard["broker_acl"][name]["rules_sha256"] == sha(canonical(template))
    permit = permit_value()
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    record = parse_commissioning_permit(json.dumps(permit).encode())
    for name in CANDIDATES:
        instantiated = minimal_rules(**{name: commissioning_acl_rules(name, permit=record)})
        with pytest.raises(api.ProvisionerRefused):
            api.construct(profile(), acl=Recipes(instantiated))
        with pytest.raises(api.ProvisionerRefused):
            api.construct(profile(), acl=Recipes(minimal_rules(**{name: ("-@all",)})))
    with pytest.raises(api.ProvisionerRefused):
        api.construct(profile(), acl=Recipes(minimal_rules(issuer=("-@all", "~{lane}:*"))))


def test_guard_binds_the_acl_observer_outside_b() -> None:
    """Design decision 7: G hashes the observer's rules; B names no observer."""
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

    api = module()
    assert api.GUARD_PRINCIPALS == (*api.BROKER_PRINCIPALS, "observer")
    assert "observer" not in api.BROKER_PRINCIPALS
    result = api.construct(profile(), acl=MergedRecipes(profile_fields()["qualification_id"]))
    entry = json.loads(result.guard_config)["broker_acl"]["observer"]
    assert entry == {
        "credential": OBSERVER,
        "rules_sha256": sha(canonical(list(commissioning_acl_rules("observer")))),
    }
    assert "observer" not in json.dumps(result.bootstrap.as_dict())
    fields = profile_fields()
    for taken in (
        fields["issuer_identity"],
        fields["production_credentials"]["verifier"],
        fields["candidate_credentials"]["worker"],
        fields["qualification_publisher_identity"],
        fields["mediator_identity"],
        fields["gateway_identity"],
    ):
        with pytest.raises(api.ProvisionerRefused):
            api.construct(profile(observer_identity=dict(taken)), acl=Recipes())
    for bad in ({"id": "x"}, {"id": "commissioning/o", "generation": 1}, None):
        with pytest.raises(api.ProvisionerRefused):
            api.construct(profile(observer_identity=bad), acl=Recipes())


def test_verify_tuple_refuses_an_observer_that_b_names() -> None:
    api = module()
    result = built()
    value = json.loads(result.guard_config)
    value["broker_acl"]["observer"]["credential"] = profile_fields()["issuer_identity"]
    guard = canonical(value)
    m = result.manifest.as_dict()
    m["guard_identity"]["config_sha256"] = sha(guard)
    from curie_protected_hooks.authority_records import parse_manifest

    manifest = parse_manifest(canonical(m))
    with pytest.raises(api.ProvisionerRefused):
        api.verify_tuple(result.bootstrap, guard, manifest)
    value["broker_acl"]["observer"]["credential"] = OBSERVER
    guard = canonical(value)
    m["guard_identity"]["config_sha256"] = sha(guard)
    api.verify_tuple(result.bootstrap, guard, parse_manifest(canonical(m)))


def test_guard_bounds_each_rule_and_each_principals_rule_set() -> None:
    api = module()
    rules = minimal_rules()
    longest = "(+get ~" + "k" * 4088 + ")"
    assert len(longest) == 4096
    rules["worker"] = ("-@all", longest)
    api.construct(profile(), acl=Recipes(rules))
    rules["worker"] = ("-@all", longest[:-1] + "k)")
    with pytest.raises(api.ProvisionerRefused):
        api.construct(profile(), acl=Recipes(rules))
    rules["worker"] = (longest,) * 4
    api.construct(profile(), acl=Recipes(rules))
    rules["worker"] = (longest,) * 4 + ("-@all",)
    with pytest.raises(api.ProvisionerRefused):
        api.construct(profile(), acl=Recipes(rules))


def test_measurement_file_binds_the_final_manifest_and_parses() -> None:
    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement

    result = built()
    name, raw = result.measurement_file
    assert name == result.measurement.digest + ".json"
    assert sha(raw) == result.measurement.digest
    value = parse_artifact_measurement(raw).as_dict()
    m = result.manifest.as_dict()
    assert value["manifest_digest"] == result.manifest.digest
    for key in ("execution_config_digest", "substrate", "guard_identity"):
        assert value[key] == m[key]
    assert value["launch_projection"] == {
        "schema_version": 1,
        "format": "kubernetes-cold-pod-v1",
        "sha256": m["substrate"]["launch_config_sha256"],
    }
    assert value["platform_mappings"] == [mapping()]
    assert value["enforcement_evidence_ref"] == "sha256:" + "7" * 64
    assert result.measurement_record_id == "sha256:" + result.measurement.digest


def test_construction_is_deterministic() -> None:
    first, second = built(), built()
    assert first.bootstrap.canonical_bytes == second.bootstrap.canonical_bytes
    assert first.guard_config == second.guard_config
    assert first.manifest.canonical_bytes == second.manifest.canonical_bytes
    assert first.measurement_file == second.measurement_file


def test_verify_tuple_accepts_the_constructed_tuple() -> None:
    result = built()
    module().verify_tuple(result.bootstrap, result.guard_config, result.manifest)


def test_verify_tuple_refuses_mixed_tuples() -> None:
    api = module()
    first, second = built(), built(max_permit_ms="2000")
    for b, g, m in (
        (second.bootstrap, first.guard_config, first.manifest),
        (first.bootstrap, second.guard_config, first.manifest),
        (first.bootstrap, first.guard_config, second.manifest),
    ):
        with pytest.raises(api.ProvisionerRefused):
            api.verify_tuple(b, g, m)


@pytest.mark.parametrize(
    "mutation",
    [
        _set(("runtime_generation",), "8"),
        _set(("execution_config_digest",), "0" * 64),
        _set(("credential_refs", "verifier", "generation"), "99"),
    ],
)
def test_verify_tuple_refuses_a_manifest_that_keeps_g_but_leaves_b(mutation: Any) -> None:
    from curie_protected_hooks.authority_records import parse_manifest

    api = module()
    result = built()
    value = result.manifest.as_dict()
    mutation(value)
    with pytest.raises(api.ProvisionerRefused):
        api.verify_tuple(result.bootstrap, result.guard_config, parse_manifest(canonical(value)))


def test_verify_tuple_refuses_wrong_types_and_noncanonical_guard() -> None:
    api = module()
    result = built()
    spaced = json.dumps(json.loads(result.guard_config), indent=1).encode()
    for b, g, m in (
        (result.bootstrap.as_dict(), result.guard_config, result.manifest),
        (result.bootstrap, result.guard_config.decode(), result.manifest),
        (result.bootstrap, spaced, result.manifest),
        (result.bootstrap, result.guard_config, result.manifest.as_dict()),
    ):
        with pytest.raises(api.ProvisionerRefused):
            api.verify_tuple(b, g, m)


@pytest.mark.parametrize(
    "mutation",
    [
        _set(("extra",), 1),
        _set(("schema_version",), 2),
        _set(("kind",), "guard"),
        _set(("bootstrap_digest",), "A" * 64),
        _set(("rbac_sha256",), "x"),
        _set(("broker_acl", "extra"), {"credential": {"id": "x", "generation": "1"}}),
        _set(("broker_acl", "issuer", "rules_sha256"), "1"),
        _set(("broker_acl", "issuer", "enabled"), True),
        _set(("broker_acl", "issuer", "credential", "generation"), 1),
    ],
)
def test_parse_guard_config_is_closed(mutation: Mutation) -> None:
    api = module()
    value = json.loads(built().guard_config)
    mutation(value)
    with pytest.raises(api.ProvisionerRefused):
        api.parse_guard_config(canonical(value))


def test_parse_guard_config_refuses_duplicates_and_floats() -> None:
    api = module()
    raw = built().guard_config
    with pytest.raises(api.ProvisionerRefused):
        api.parse_guard_config(raw[:-1] + b',"kind":"x"}')
    with pytest.raises(api.ProvisionerRefused):
        api.parse_guard_config(raw.replace(b'"schema_version":1', b'"schema_version":1.0'))


@pytest.mark.parametrize(
    "image",
    [
        "registry.example.test/runner:latest",
        "registry.example.test/runner@sha256:" + "E" * 64,
        "registry.example.test/runner@sha256:" + "e" * 63,
        "@sha256:" + "e" * 64,
        "a@b@sha256:" + "e" * 64,
        "registry.example.test/run ner@sha256:" + "e" * 64,
    ],
)
def test_images_must_be_pinned_by_digest(image: str) -> None:
    api = module()
    for field in ("worker_image", "runner_image"):
        with pytest.raises(api.ProvisionerRefused):
            built(**{field: image})


def _bound(projection: bytes) -> dict[str, bytes]:
    """P with an E that binds its digest, so only the P check can refuse."""
    return {"projection": projection, "execution_config": canonical(execution_value(projection))}


def _projection(change: Mutation) -> dict[str, bytes]:
    value = projection_value()
    change(value)
    return _bound(canonical(value))


def _execution(change: Mutation) -> bytes:
    value = execution_value(canonical(projection_value()))
    change(value)
    return canonical(value)


@pytest.mark.parametrize(
    "changes",
    [
        _projection(_set(("format",), "docker-cold-v1")),
        _projection(_set(("schema_version",), True)),
        _projection(_set(("schema_version",), 2)),
        _projection(_set(("image", "digest"), "sha256:" + "0" * 64)),
        _projection(_set(("image", "reference"), WORKER)),
        _projection(_set(("embedded_bundle", "sha256"), "0" * 64)),
        {"projection": canonical({**projection_value(), "launch_identity": "launch/other"})},
        _projection(_set(("delivery_slots", "runner_token"), "reused")),
        _bound(canonical(projection_value()) + b" "),
        _bound(json.dumps(projection_value(), indent=1).encode()),
        {
            "execution_config": _execution(
                _set(("launch", "runner_image_digest"), "sha256:" + "0" * 64)
            )
        },
        {"execution_config": _execution(_set(("launch", "substrate", "kind"), "docker"))},
        {
            "execution_config": _execution(
                _set(("launch", "substrate", "launch_config_sha256"), "0" * 64)
            )
        },
        {"execution_config": _execution(_set(("bindings", "qualified_connectors"), ["c"]))},
        {"execution_config": _execution(_set(("runner", "fake_model"), True))},
        {"execution_config": _execution(_set(("policy", "tool_access"), "read-write"))},
        {"execution_config": b"[]"},
        {"execution_config": b'{"a":1.5}'},
        {
            "execution_config": json.dumps(
                execution_value(canonical(projection_value())), indent=1
            ).encode()
        },
    ],
)
def test_projection_and_execution_config_must_match_the_pinned_profile(
    changes: dict[str, Any],
) -> None:
    api = module()
    with pytest.raises(api.ProvisionerRefused):
        built(**changes)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [
            {
                **mapping(),
                "oci_digest": "sha256:" + "0" * 64,
                "pinned_image_reference": "registry.example.test/runner@sha256:" + "0" * 64,
            }
        ],
        [{**mapping(), "pinned_image_reference": WORKER, "oci_digest": "sha256:" + "d" * 64}],
        [mapping(), mapping()],
    ],
)
def test_platform_mappings_must_name_the_pinned_runner_image(rows: list[Any]) -> None:
    api = module()
    with pytest.raises(api.ProvisionerRefused):
        built(platform_mappings=rows)


def test_platform_mappings_are_sorted_into_the_measurement() -> None:
    arm = {**mapping(), "architecture": "arm64", "observed_image_id": "sha256:" + "8" * 64}
    result = built(platform_mappings=[arm, mapping()])
    rows = json.loads(result.measurement_file[1])["platform_mappings"]
    assert [row["architecture"] for row in rows] == ["amd64", "arm64"]


@pytest.mark.parametrize(
    "changes",
    [
        {
            "candidate_credentials": {
                "enqueue": {"id": "credential/example-enqueue", "generation": "1"},
                "worker": {"id": "commissioning/example-worker", "generation": "2"},
            }
        },
        {"issuer_identity": {"id": "commissioning/example-control", "generation": "1"}},
        {"runtime_id": "not-a-uuid"},
        {"guard_revision": 3},
        {"guard_control_id": "x"},
        {"broker_identity": {}},
        {"qualification_id": "x"},
        {"enforcement_evidence_ref": "7" * 64},
        {"admission_policy_sha256": "x"},
        {"target_scope": []},
    ],
)
def test_invalid_profiles_refuse_with_one_redacted_error(changes: dict[str, Any]) -> None:
    api = module()
    with pytest.raises(api.ProvisionerRefused) as raised:
        built(**changes)
    assert str(raised.value) == "Commissioning provisioner refused"


def test_profile_and_result_reprs_hide_contents() -> None:
    result = built()
    assert "example" not in repr(profile())
    assert "example" not in repr(result)


def test_profile_copies_mutable_inputs() -> None:
    fields = profile_fields()
    value = module().ProvisionerProfile(**fields)
    fields["source_scope"].clear()
    fields["platform_mappings"][0]["architecture"] = "arm64"
    fields["production_credentials"]["enqueue"]["generation"] = "99"
    result = module().construct(value, acl=Recipes())
    assert result.bootstrap.as_dict()["source_scope"] == profile_fields()["source_scope"]
    rows = json.loads(result.measurement_file[1])["platform_mappings"]
    assert rows == [mapping()]
    assert result.manifest.as_dict()["credential_refs"]["enqueue"]["generation"] == "4"
