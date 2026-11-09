"""Endpoint resolution shared by hosted MCP clients and protected route callers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from curie_internal.connector_endpoints import ConnectorEndpointBinding
from plugin_format.connector_render import mcp_entry, service_dns, unhosted_mcp_entry
from plugin_format.connectors import ConnectorSpec


class EndpointResolver(Protocol):
    def mcp(self, connector: str, spec: ConnectorSpec) -> dict[str, Any]: ...

    def base_url(self, connector: str, spec: ConnectorSpec) -> str: ...


@dataclass(frozen=True)
class KubernetesEndpointResolver:
    release: str
    agent: str
    namespace: str

    def mcp(self, connector: str, spec: ConnectorSpec) -> dict[str, Any]:
        return mcp_entry(self.release, self.agent, self.namespace, connector, spec)

    def base_url(self, connector: str, spec: ConnectorSpec) -> str:
        host = service_dns(self.release, self.agent, connector, self.namespace)
        return f"http://{host}:{spec.port}"


@dataclass(frozen=True)
class StaticEndpointResolver:
    binding: ConnectorEndpointBinding

    def mcp(self, connector: str, spec: ConnectorSpec) -> dict[str, Any]:
        # Reuse the declared client-auth rendering without deriving a Service name.
        endpoint = self.binding.endpoint(connector)
        entry = unhosted_mcp_entry(spec.model_copy(update={"unhosted_url": endpoint.mcp_url}))
        assert entry is not None
        return entry

    def base_url(self, connector: str, spec: ConnectorSpec) -> str:
        return self.binding.endpoint(connector).base_url


def endpoint_resolver(
    *,
    release: str | None,
    agent: str | None,
    namespace: str | None,
    endpoints: ConnectorEndpointBinding | None,
) -> EndpointResolver | None:
    if endpoints is not None:
        if agent != endpoints.agent:
            raise ValueError("connector endpoint identity does not match authenticated agent")
        return StaticEndpointResolver(endpoints)
    if release and agent and namespace:
        return KubernetesEndpointResolver(release, agent, namespace)
    return None
