"""Per-agent runner resource override applied at the next claim.

A null override keeps the chart pool. A set override is copied onto a
worker-owned SandboxTemplate and a replicas-0 warm pool. The shared template
is never written. Existing sandboxes are not in these dicts and are not patched.
"""

from __future__ import annotations

import copy
from typing import Any

_RUNNER_POOL_SUFFIX = "-runner-pool"
_POOL_SUFFIX = "-pool"
_DIMENSIONS = ("cpu", "memory", "ephemeral-storage")


def resources_object_name(kind: str, name: str) -> str:
    """Return ``name`` when it is a worker-owned resources object of ``kind``.

    ``template`` names end in ``-resources`` and not ``-resources-pool``.
    ``warmpool`` names end in ``-resources-pool``. Anything else is refused
    before a write, including the shared chart template.
    """

    if kind == "template" and name.endswith("-resources"):
        return name
    if kind == "warmpool" and name.endswith("-resources-pool"):
        return name
    raise ValueError(f"{kind} name {name!r} is not a worker-owned resources object")


def docker_limit_args(
    resources: dict[str, Any] | None,
    default_memory: str,
    default_cpus: str,
) -> list[str]:
    """``docker run`` memory and cpu flags for one claim.

    ``None`` passes the hardening defaults through. A set override converts
    ``limits.memory`` and ``limits.cpu`` only. Docker's ``--memory`` parser
    treats ``m`` as mebibytes and ``g`` as gibibytes, which is the Kubernetes
    ``Mi`` and ``Gi`` quantity. ``--cpus`` is a CPU count, so ``500m`` is
    ``0.5``.
    """

    if resources is None:
        return ["--memory", default_memory, "--cpus", default_cpus]
    limits = resources["limits"]
    return [
        "--memory",
        _docker_memory(str(limits["memory"])),
        "--cpus",
        _docker_cpu(str(limits["cpu"])),
    ]


def prepare_resources_claim(
    pool: str,
    agent_name: str,
    resources: dict[str, Any] | None,
    templates: dict[str, Any],
    warm_pools: dict[str, Any],
) -> str:
    """Return the pool to claim, copying a resources template when set.

    ``resources`` None returns ``pool`` and does not touch either dict. A set
    value is validated before any write. The source spec is the chart template
    paired with ``pool`` (the pool name without the trailing ``-pool``).
    """

    if resources is None:
        return pool
    _validate_resources(resources)
    if not pool.endswith(_RUNNER_POOL_SUFFIX):
        raise ValueError(f"pool {pool!r} does not end in {_RUNNER_POOL_SUFFIX}")
    source_name = pool[: -len(_POOL_SUFFIX)]
    if source_name not in templates:
        raise ValueError(f"source template {source_name} is missing")
    owned_template, owned_pool = _owned_names(pool, agent_name)
    resources_object_name("template", owned_template)
    resources_object_name("warmpool", owned_pool)
    copied = copy.deepcopy(templates[source_name])
    _replace_resources(copied, resources)
    templates[owned_template] = copied
    warm_pools[owned_pool] = {
        "replicas": 0,
        "sandboxTemplateRef": {"name": owned_template},
    }
    return owned_pool


def _owned_names(pool: str, agent_name: str) -> tuple[str, str]:
    stem = pool[: -len(_RUNNER_POOL_SUFFIX)]
    agent_suffix = f"-agent-{agent_name}"
    prefix = stem[: -len(agent_suffix)] if stem.endswith(agent_suffix) else stem
    template = f"{prefix}-agent-{agent_name}-resources"
    return template, f"{template}-pool"


def _validate_resources(resources: dict[str, Any]) -> None:
    if set(resources) != {"requests", "limits"}:
        raise ValueError("runner resources must be exactly requests and limits")
    for side in ("requests", "limits"):
        block = resources[side]
        if not isinstance(block, dict) or set(block) != set(_DIMENSIONS):
            raise ValueError(f"runner resources {side} must be cpu, memory, and ephemeral-storage")


def _replace_resources(spec: dict[str, Any], resources: dict[str, Any]) -> None:
    block = {
        "requests": {key: resources["requests"][key] for key in _DIMENSIONS},
        "limits": {key: resources["limits"][key] for key in _DIMENSIONS},
    }
    pod = spec["podTemplate"]["spec"]
    for container in pod.get("containers", []):
        container["resources"] = copy.deepcopy(block)
    for container in pod.get("initContainers", []):
        container["resources"] = copy.deepcopy(block)


def _docker_memory(quantity: str) -> str:
    # docker's RAMInBytes parser treats k, m, and g as binary kibibytes,
    # mebibytes, and gibibytes. That matches Kubernetes Ki, Mi, and Gi.
    # A bare number is bytes. Ti is passed as a byte count.
    if quantity.endswith("Ki"):
        return f"{quantity[:-2]}k"
    if quantity.endswith("Mi"):
        return f"{quantity[:-2]}m"
    if quantity.endswith("Gi"):
        return f"{quantity[:-2]}g"
    if quantity.endswith("Ti"):
        return str(int(quantity[:-2]) * 1024**4)
    if quantity.isdigit():
        return quantity
    raise ValueError(f"unsupported memory quantity {quantity!r}")


def _docker_cpu(quantity: str) -> str:
    if quantity.endswith("m"):
        millicores = int(quantity[:-1])
        whole, remainder = divmod(millicores, 1000)
        if remainder == 0:
            return str(whole)
        return f"{millicores / 1000:.3f}".rstrip("0").rstrip(".")
    return quantity
