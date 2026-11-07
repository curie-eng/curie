"""Trusted hosted connector deployment configuration, version 1."""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, field_validator

ENDPOINTS_ENV = "CURIE_CONNECTOR_ENDPOINTS"
BINDING_ENV = "CURIE_HOSTED_CONNECTOR_ENDPOINTS"
UPLOADS_ENV = "CURIE_CONNECTOR_UPLOAD_ENDPOINTS"


class ConnectorEndpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    base_url: str
    upload_url: str | None = None

    @field_validator("base_url", "upload_url")
    @classmethod
    def valid_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or any(c.isspace() or ord(c) < 32 for c in value)
        ):
            raise ValueError(
                "connector endpoint requires an absolute HTTP(S) URL without credentials"
            )
        # Force validation of a malformed or out-of-range port.
        _ = parsed.port
        return value.rstrip("/")

    @property
    def mcp_url(self) -> str:
        return f"{self.base_url}/mcp"


class ConnectorEndpointBinding(BaseModel):
    """Worker projection for one authenticated agent, never bundle input."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    version: Literal[1]
    agent: str
    connectors: dict[str, ConnectorEndpoint]

    def endpoint(self, connector: str) -> ConnectorEndpoint:
        try:
            return self.connectors[connector]
        except KeyError:
            raise ValueError(f"missing hosted connector endpoint: {connector}") from None


class StaticConnectorEndpoints(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    version: Literal[1]
    agents: dict[str, dict[str, ConnectorEndpoint]]

    def bind(self, authenticated_agent: str) -> ConnectorEndpointBinding:
        if authenticated_agent not in self.agents:
            raise ValueError("missing static connector endpoints for authenticated agent")
        return ConnectorEndpointBinding(
            version=1, agent=authenticated_agent, connectors=self.agents[authenticated_agent]
        )
