"""Stable low risk service resources shared by Curie processes."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from functools import cache

from opentelemetry.sdk.resources import Resource


def deployment_environment(env: Mapping[str, str]) -> str | None:
    """Resolve the standard deployment environment resource attribute."""

    for item in env.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        key, separator, value = item.partition("=")
        if separator and key.strip() == "deployment.environment.name":
            return value.strip() or None
    return env.get("CURIE_DEPLOYMENT_ENVIRONMENT") or None


@cache
def service_instance_id(service_name: str) -> str:
    """Return one anonymous instance identifier for this process and service."""

    return f"{service_name}-{uuid.uuid4().hex}"


def build_resource(
    service_name: str,
    *,
    service_version: str,
    service_instance_id: str,
    deployment_environment: str | None = None,
) -> Resource:
    """Build Curie's closed service resource without workload correlation IDs."""

    if not service_name or not service_version or not service_instance_id:
        raise ValueError("service identity values must be nonempty")
    attributes: dict[str, str] = {
        "service.namespace": "curie",
        "service.name": service_name,
        "service.version": service_version,
        "service.instance.id": service_instance_id,
    }
    if deployment_environment and deployment_environment.strip():
        attributes["deployment.environment.name"] = deployment_environment.strip()
    return Resource(attributes)
