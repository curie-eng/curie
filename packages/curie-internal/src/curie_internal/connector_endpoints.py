"""Trusted hosted connector deployment configuration, version 1."""

from __future__ import annotations

import ipaddress
import json
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, BeforeValidator, ConfigDict, field_validator, model_validator

ENDPOINTS_ENV = "CURIE_CONNECTOR_ENDPOINTS"
BINDING_ENV = "CURIE_HOSTED_CONNECTOR_ENDPOINTS"
UPLOADS_ENV = "CURIE_CONNECTOR_UPLOAD_ENDPOINTS"
CA_FILE_ENV = "CURIE_CONNECTOR_CA_FILE"


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate connector endpoint JSON key")
        result[key] = value
    return result


def parse_endpoint_json(value: Any) -> Any:
    """Decode deployment JSON before Pydantic can discard duplicate keys."""

    if not isinstance(value, str):
        return value
    parsed = json.loads(value, object_pairs_hook=_unique_object)
    if not isinstance(parsed, dict):
        raise ValueError("connector endpoint JSON must be an object")
    return parsed


def _strict_version(value: Any) -> Any:
    if type(value) is not int:
        raise ValueError("endpoint schema version must be an integer")
    return value


SchemaVersion = Annotated[Literal[1], BeforeValidator(_strict_version)]


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class ConnectorEndpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, hide_input_in_errors=True)

    base_url: str
    upload_url: str | None = None
    insecure_http: bool = False

    @field_validator("base_url", "upload_url")
    @classmethod
    def valid_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            if (
                "?" in value
                or "#" in value
                or "\\" in value
                or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
            ):
                raise ValueError
            parsed = urlsplit(value)
            host, port = parsed.hostname, parsed.port
            if (
                parsed.scheme not in {"http", "https"}
                or not host
                or not host.isascii()
                or "%" in host
                or parsed.username is not None
                or parsed.password is not None
                or parsed.netloc.endswith(":")
                or port == 0
            ):
                raise ValueError
            authority = f"[{host}]" if ":" in host else host
            if port is not None:
                authority += f":{port}"
            return urlunsplit((parsed.scheme, authority, parsed.path.rstrip("/"), "", ""))
        except ValueError:
            raise ValueError("invalid connector endpoint HTTP(S) URL") from None

    @model_validator(mode="after")
    def require_secure_transport(self) -> ConnectorEndpoint:
        for url in (self.base_url, self.upload_url):
            if url is None:
                continue
            parsed = urlsplit(url)
            if parsed.scheme == "http" and not _loopback(parsed.hostname or ""):
                if not self.insecure_http:
                    raise ValueError(
                        "non-loopback HTTP connector endpoints require insecure_http=true"
                    )
        return self

    @property
    def mcp_url(self) -> str:
        return f"{self.base_url}/mcp"


class ConnectorEndpointBinding(BaseModel):
    """Worker projection for one authenticated agent, never bundle input."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, hide_input_in_errors=True)

    version: SchemaVersion
    agent: str
    connectors: dict[str, ConnectorEndpoint]

    def endpoint(self, connector: str) -> ConnectorEndpoint:
        try:
            return self.connectors[connector]
        except KeyError:
            raise ValueError(f"missing hosted connector endpoint: {connector}") from None


class StaticConnectorEndpoints(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, hide_input_in_errors=True)

    version: SchemaVersion
    agents: dict[str, dict[str, ConnectorEndpoint]]

    def bind(self, authenticated_agent: str) -> ConnectorEndpointBinding:
        if authenticated_agent not in self.agents:
            raise ValueError("missing static connector endpoints for authenticated agent")
        return ConnectorEndpointBinding(
            version=1, agent=authenticated_agent, connectors=self.agents[authenticated_agent]
        )
