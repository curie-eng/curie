"""Per-agent runner resource override on the claim path (#3209).

``prepare_resources_claim`` is an in-memory stand-in for the Kubernetes writes.
It does not talk to a cluster. ``templates`` maps a SandboxTemplate name to that
object's spec (``podTemplate`` lives on the spec). ``warm_pools`` maps a
SandboxWarmPool name to that object's spec (``replicas`` and
``sandboxTemplateRef`` live on the spec). Dict keys are the object names.
Kubernetes metadata is not stored.

``pool`` is the chart pool the claim would already use: the generic
``{prefix}-runner-pool``, or the per-agent ``{prefix}-agent-{agent}-runner-pool``
when that is the pool ``claim_warm_pool`` selects. The source spec is
``templates[pool with the trailing "-pool" removed]``, which is the chart
template paired with that pool. A missing key fails. The generic template is
not a fallback when the per-agent pool was selected.

The worker-owned names use the generic prefix, the same cut as
``agent_warm_pool_name``: strip ``-runner-pool``, and also strip a trailing
``-agent-{agent}`` when ``pool`` is already the per-agent chart pool. The
template name is ``{prefix}-agent-{agent}-resources`` and the pool name is
``{prefix}-agent-{agent}-resources-pool``.

A set override replaces ``resources`` on every container and init container
with that object and nothing else. Every other spec field is a copy of the
source. The source object is not replaced and is not mutated. The recorded
warm pool spec is exactly ``replicas: 0`` and a ``sandboxTemplateRef`` whose
name is the new template. No other field is written there.

Validation failures and a pool that does not end in ``-runner-pool`` raise
``ValueError`` before any write. ``resources_object_name`` is the same suffix
gate the client applies to a name before it is recorded.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from typing import Any

import pytest
from curie_worker.binding import _RESOLVE_AGENT_SQL, _RESOLVE_SQL
from curie_worker.sandbox.docker import RunnerHardening
from curie_worker.sandbox.resources import (
    docker_limit_args,
    prepare_resources_claim,
    resources_object_name,
)

_AGENT = "acme-a"
_OVERRIDE: dict[str, Any] = {
    "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
    "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
}
_CHART_RESOURCES: dict[str, Any] = {
    "requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
    "limits": {"cpu": "1", "memory": "768Mi", "ephemeral-storage": "4Gi"},
    # Not a ResourceRequirements field. A merge would keep it; a replacement
    # drops it. The recorded resources must equal the override exactly.
    "claims": [{"name": "drop-me"}],
}


def _spec(marker: str) -> dict[str, Any]:
    resources = copy.deepcopy(_CHART_RESOURCES)
    return {
        "service": True,
        "envVarsInjectionPolicy": "Overrides",
        "networkPolicyManagement": "Unmanaged",
        "podTemplate": {
            "metadata": {"labels": {"curietech.ai/marker": marker}},
            "spec": {
                "runtimeClassName": "gvisor",
                "priorityClassName": "curie-sandbox",
                "volumes": [{"name": "bundles", "emptyDir": {"sizeLimit": "2Gi"}}],
                "containers": [
                    {
                        "name": "runner",
                        "image": "example.com/runner:dev",
                        "env": [{"name": "CURIE_MODEL", "value": marker}],
                        "resources": copy.deepcopy(resources),
                        "volumeMounts": [{"name": "bundles", "mountPath": "/bundles"}],
                    }
                ],
                "initContainers": [
                    {
                        "name": "bundle-fetch",
                        "image": "example.com/aws-cli:dev",
                        "command": ["/bin/sh", "-c", "echo fetch"],
                        "resources": copy.deepcopy(resources),
                    },
                    {
                        "name": "bundle-extract",
                        "image": "example.com/busybox:dev",
                        "command": ["/bin/sh", "-c", "echo extract"],
                        "resources": copy.deepcopy(resources),
                    },
                ],
            },
        },
    }


def _chart_pool(template_name: str) -> dict[str, Any]:
    return {
        "replicas": 1,
        "updateStrategy": {"type": "Recreate"},
        "sandboxTemplateRef": {"name": template_name},
    }


def _with_resources(spec: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    copied = copy.deepcopy(spec)
    pod = copied["podTemplate"]["spec"]
    for container in pod["containers"]:
        container["resources"] = copy.deepcopy(resources)
    for container in pod["initContainers"]:
        container["resources"] = copy.deepcopy(resources)
    return copied


def _keys(value: object) -> Iterator[str]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from _keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _keys(item)


def _ids(store: dict[str, Any]) -> dict[str, int]:
    return {name: id(value) for name, value in store.items()}


def test_null_override_returns_the_chart_pool_and_writes_nothing() -> None:
    source_name = "curie-runner"
    templates = {source_name: _spec("generic"), "decoy": _spec("decoy")}
    warm_pools = {source_name + "-pool": _chart_pool(source_name)}
    before_templates = copy.deepcopy(templates)
    before_pools = copy.deepcopy(warm_pools)
    template_ids = _ids(templates)
    pool_ids = _ids(warm_pools)

    for pool in ("curie-runner-pool", "curie-agent-acme-a-runner-pool"):
        assert prepare_resources_claim(pool, _AGENT, None, templates, warm_pools) == pool

    assert templates == before_templates
    assert warm_pools == before_pools
    assert _ids(templates) == template_ids
    assert _ids(warm_pools) == pool_ids


@pytest.mark.parametrize(
    ("pool", "source_name", "owned_template", "owned_pool"),
    [
        (
            "curie-runner-pool",
            "curie-runner",
            "curie-agent-acme-a-resources",
            "curie-agent-acme-a-resources-pool",
        ),
        (
            "curie-agent-acme-a-runner-pool",
            "curie-agent-acme-a-runner",
            "curie-agent-acme-a-resources",
            "curie-agent-acme-a-resources-pool",
        ),
        (
            "curie-g1-runner-pool",
            "curie-g1-runner",
            "curie-g1-agent-acme-a-resources",
            "curie-g1-agent-acme-a-resources-pool",
        ),
        (
            "curie-g1-agent-acme-a-runner-pool",
            "curie-g1-agent-acme-a-runner",
            "curie-g1-agent-acme-a-resources",
            "curie-g1-agent-acme-a-resources-pool",
        ),
    ],
)
def test_set_override_copies_the_selected_chart_template(
    pool: str,
    source_name: str,
    owned_template: str,
    owned_pool: str,
) -> None:
    source = _spec(source_name)
    decoy = _spec("decoy")
    templates: dict[str, Any] = {source_name: source, "decoy": decoy}
    chart_pool = _chart_pool(source_name)
    warm_pools: dict[str, Any] = {pool: chart_pool}
    source_snapshot = copy.deepcopy(source)
    decoy_snapshot = copy.deepcopy(decoy)
    chart_snapshot = copy.deepcopy(chart_pool)

    got = prepare_resources_claim(pool, _AGENT, copy.deepcopy(_OVERRIDE), templates, warm_pools)

    assert got == owned_pool
    assert templates[source_name] is source
    assert templates[source_name] == source_snapshot
    assert templates["decoy"] is decoy
    assert templates["decoy"] == decoy_snapshot
    assert set(templates) == {source_name, "decoy", owned_template}
    assert templates[owned_template] == _with_resources(source_snapshot, _OVERRIDE)
    for container in templates[owned_template]["podTemplate"]["spec"]["containers"]:
        assert container["resources"] == _OVERRIDE
        assert set(container["resources"]) == {"requests", "limits"}
    for container in templates[owned_template]["podTemplate"]["spec"]["initContainers"]:
        assert container["resources"] == _OVERRIDE
        assert set(container["resources"]) == {"requests", "limits"}
    assert warm_pools[pool] is chart_pool
    assert warm_pools[pool] == chart_snapshot
    assert set(warm_pools) == {pool, owned_pool}
    assert warm_pools[owned_pool] == {
        "replicas": 0,
        "sandboxTemplateRef": {"name": owned_template},
    }


@pytest.mark.parametrize(
    "resources",
    [
        {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
        },
        {
            "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"},
        },
        {
            "requests": {
                "cpu": "500m",
                "memory": "1Gi",
                "ephemeral-storage": "1Gi",
                "gpu": "1",
            },
            "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
        },
    ],
)
def test_resources_outside_the_six_key_shape_write_nothing(resources: dict[str, Any]) -> None:
    source_name = "curie-runner"
    templates: dict[str, Any] = {source_name: _spec("generic")}
    warm_pools: dict[str, Any] = {"curie-runner-pool": _chart_pool(source_name)}
    before_templates = copy.deepcopy(templates)
    before_pools = copy.deepcopy(warm_pools)
    template_ids = _ids(templates)
    pool_ids = _ids(warm_pools)

    with pytest.raises(ValueError):
        prepare_resources_claim(
            "curie-runner-pool", _AGENT, resources, templates, warm_pools
        )

    assert templates == before_templates
    assert warm_pools == before_pools
    assert _ids(templates) == template_ids
    assert _ids(warm_pools) == pool_ids


def test_pool_without_runner_pool_suffix_writes_nothing() -> None:
    templates: dict[str, Any] = {"custom-pool": _spec("custom"), "curie-runner": _spec("generic")}
    warm_pools: dict[str, Any] = {"custom-pool": _chart_pool("custom-pool")}
    before_templates = copy.deepcopy(templates)
    before_pools = copy.deepcopy(warm_pools)
    template_ids = _ids(templates)
    pool_ids = _ids(warm_pools)

    with pytest.raises(ValueError):
        prepare_resources_claim("custom-pool", _AGENT, _OVERRIDE, templates, warm_pools)

    assert templates == before_templates
    assert warm_pools == before_pools
    assert _ids(templates) == template_ids
    assert _ids(warm_pools) == pool_ids


def test_missing_source_template_names_that_template_and_writes_nothing() -> None:
    templates: dict[str, Any] = {"curie-runner": _spec("generic")}
    warm_pools: dict[str, Any] = {}
    before_templates = copy.deepcopy(templates)
    template_ids = _ids(templates)

    with pytest.raises(ValueError, match=r"curie-agent-acme-a-runner(?!-pool)"):
        prepare_resources_claim(
            "curie-agent-acme-a-runner-pool",
            _AGENT,
            _OVERRIDE,
            templates,
            warm_pools,
        )

    assert templates == before_templates
    assert warm_pools == {}
    assert _ids(templates) == template_ids


@pytest.mark.parametrize(
    ("kind", "name"),
    [
        ("template", "curie-agent-acme-a-resources"),
        ("warmpool", "curie-agent-acme-a-resources-pool"),
        ("template", "curie-g1-agent-acme-a-resources"),
        ("warmpool", "curie-g1-agent-acme-a-resources-pool"),
    ],
)
def test_resources_object_name_returns_an_owned_suffix(kind: str, name: str) -> None:
    assert resources_object_name(kind, name) == name


@pytest.mark.parametrize(
    ("kind", "name"),
    [
        ("template", "curie-runner"),
        ("template", "curie-agent-acme-a-runner"),
        ("template", "curie-agent-acme-a-resources-pool"),
        ("template", "curie-agent-acme-a-resources-extra"),
        ("warmpool", "curie-runner-pool"),
        ("warmpool", "curie-agent-acme-a-runner-pool"),
        ("warmpool", "curie-agent-acme-a-resources"),
        ("warmpool", "curie-agent-acme-a-resources-pool-extra"),
        ("sandbox", "curie-agent-acme-a-resources"),
    ],
)
def test_resources_object_name_refuses_a_name_outside_its_suffix(kind: str, name: str) -> None:
    with pytest.raises(ValueError):
        resources_object_name(kind, name)


def test_second_override_updates_the_worker_template_and_records_no_sandbox() -> None:
    pool = "curie-agent-acme-a-runner-pool"
    source_name = "curie-agent-acme-a-runner"
    generic_name = "curie-runner"
    source = _spec("per-agent")
    generic = _spec("generic")
    templates: dict[str, Any] = {source_name: source, generic_name: generic}
    chart_pool = _chart_pool(source_name)
    warm_pools: dict[str, Any] = {pool: chart_pool, "curie-runner-pool": _chart_pool(generic_name)}
    first = copy.deepcopy(_OVERRIDE)
    second = copy.deepcopy(_OVERRIDE)
    second["limits"] = {**second["limits"], "cpu": "2"}
    owned_template = "curie-agent-acme-a-resources"
    owned_pool = "curie-agent-acme-a-resources-pool"

    assert (
        prepare_resources_claim(pool, _AGENT, first, templates, warm_pools) == owned_pool
    )
    assert templates[owned_template] == _with_resources(source, first)

    assert (
        prepare_resources_claim(pool, _AGENT, second, templates, warm_pools) == owned_pool
    )

    assert templates[owned_template] == _with_resources(source, second)
    assert templates[source_name] is source
    assert templates[generic_name] is generic
    assert templates[source_name] == _spec("per-agent")
    assert templates[generic_name] == _spec("generic")
    assert set(templates) == {source_name, generic_name, owned_template}
    assert warm_pools[pool] is chart_pool
    assert warm_pools[pool]["replicas"] == 1
    assert set(warm_pools) == {pool, "curie-runner-pool", owned_pool}
    assert warm_pools[owned_pool] == {
        "replicas": 0,
        "sandboxTemplateRef": {"name": owned_template},
    }
    assert "sandbox" not in set(_keys(templates))
    assert "sandbox" not in set(_keys(warm_pools))


def test_model_settings_for_selects_runner_resources() -> None:
    import inspect

    from curie_worker.binding import BindingResolver

    source = inspect.getsource(BindingResolver.model_settings_for)
    assert "runner_resources" in source


def test_runner_resources_are_read_apart_from_deployment_resolution() -> None:
    # Resolution SQL stays runnable on schemas that predate the column.
    # The claim path reads the override through runner_resources_for.
    import inspect

    from curie_worker.binding import BindingResolver

    assert "a.runner_resources" not in _RESOLVE_SQL
    assert "a.runner_resources" not in _RESOLVE_AGENT_SQL
    source = inspect.getsource(BindingResolver.runner_resources_for)
    assert "runner_resources" in source


def test_docker_limit_args_uses_the_hardening_defaults_when_resources_are_absent() -> None:
    # RunnerHardening.memory_limit is the docker flag "768m" for the chart's
    # 768Mi limit. A null override must pass those defaults through unchanged.
    hardening = RunnerHardening()
    assert hardening.memory_limit == "768m"
    assert hardening.cpu_limit == "1"
    assert docker_limit_args(None, hardening.memory_limit, hardening.cpu_limit) == [
        "--memory",
        "768m",
        "--cpus",
        "1",
    ]
    assert hardening.memory_limit == "768m"
    assert hardening.cpu_limit == "1"
    assert RunnerHardening().memory_limit == "768m"
    assert RunnerHardening().cpu_limit == "1"


def test_docker_limit_args_converts_kubernetes_limits() -> None:
    # docker's RAMInBytes parser (what --memory uses) treats m as mebibytes and
    # g as gibibytes, base 1024. That is the same quantity as Kubernetes Mi and
    # Gi, so 768Mi becomes the flag 768m (the RunnerHardening default's unit)
    # and 1Gi becomes 1g, not a raw byte count and not the decimal SI unit.
    # --cpus is a CPU count: Kubernetes 500m is 0.5, and a whole core stays
    # the digit string from the limit. Requests and ephemeral-storage are not
    # docker flags.
    mebi = {
        "requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
        "limits": {"cpu": "500m", "memory": "768Mi", "ephemeral-storage": "4Gi"},
    }
    gibi = {
        "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
        "limits": {"cpu": "2", "memory": "1Gi", "ephemeral-storage": "4Gi"},
    }
    assert docker_limit_args(mebi, "768m", "1") == ["--memory", "768m", "--cpus", "0.5"]
    assert docker_limit_args(gibi, "768m", "1") == ["--memory", "1g", "--cpus", "2"]
    kibi = {
        "requests": {"cpu": "50m", "memory": "128Ki", "ephemeral-storage": "1Ki"},
        "limits": {"cpu": "1", "memory": "512Ki", "ephemeral-storage": "2Ki"},
    }
    bare = {
        "requests": {"cpu": "50m", "memory": "1024", "ephemeral-storage": "2048"},
        "limits": {"cpu": "1", "memory": "4096", "ephemeral-storage": "8192"},
    }
    tebi = {
        "requests": {"cpu": "50m", "memory": "1Ti", "ephemeral-storage": "1Ti"},
        "limits": {"cpu": "1", "memory": "1Ti", "ephemeral-storage": "1Ti"},
    }
    assert docker_limit_args(kibi, "768m", "1") == ["--memory", "512k", "--cpus", "1"]
    assert docker_limit_args(bare, "768m", "1") == ["--memory", "4096", "--cpus", "1"]
    assert docker_limit_args(tebi, "768m", "1") == [
        "--memory",
        str(1024**4),
        "--cpus",
        "1",
    ]
