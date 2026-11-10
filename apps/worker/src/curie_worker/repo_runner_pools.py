"""Schema and reader for the CURIE_REPO_RUNNER_POOLS worker input (ADR 0216).

The worker parses this at startup; the sandbox substrate selects a
repository's pool from it. Pool names are opaque: chart rendering owns the
agent and digest identity behind each name.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from ipaddress import ip_network
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StrictBool,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from .sandbox.daemon_config import ToolDaemonConfig

REPO_RUNNER_POOLS_ENV = "CURIE_REPO_RUNNER_POOLS"
_IMAGE_DIGEST = re.compile(r"@(sha256:[0-9a-f]{64})$")

Digest = Annotated[StrictStr, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
RepositoryPath = Annotated[
    StrictStr,
    Field(pattern=r"^[A-Za-z0-9_][A-Za-z0-9._~-]*(?:/[A-Za-z0-9_][A-Za-z0-9._~-]*)+$"),
]
PoolName = Annotated[
    StrictStr, Field(max_length=63, pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class EgressPort(_Model):
    """One TCP destination port, matching the chart's existing egress shape."""

    protocol: Literal["TCP"] = "TCP"
    port: Annotated[StrictInt, Field(ge=1, le=65535)]


class EgressRule(_Model):
    """A destination CIDR and its allowed TCP ports."""

    cidr: Annotated[StrictStr, Field(pattern=r"^[^\s/]+/[0-9]{1,3}$")]
    ports: tuple[EgressPort, ...] = Field(min_length=1)

    @field_validator("cidr")
    @classmethod
    def check_cidr(cls, value: str) -> str:
        ip_network(value)
        return value


class RepoRunnerPool(_Model):
    """An opaque pool with index, amd64 and arm64 image identities in order."""

    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"docker": {"const": True}}},
            "then": {"required": ["daemon"], "properties": {"daemon": {"type": "object"}}},
            "else": {"not": {"required": ["daemon"]}},
        }
    )

    pool: PoolName
    digests: tuple[Digest, Digest, Digest]
    base: Digest
    docker: StrictBool
    egress: tuple[EgressRule, ...] = ()
    daemon: ToolDaemonConfig | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="before")
    @classmethod
    def check_daemon_presence(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            if value.get("docker") is True and value.get("daemon") is None:
                raise ValueError("Docker-enabled pools require a daemon object")
            if value.get("docker") is False and "daemon" in value:
                raise ValueError("Docker-disabled pools forbid daemon configuration")
        return value


class RepoRunnerPools(RootModel[dict[RepositoryPath, RepoRunnerPool]]):
    """The repository binding paths carried by worker repository names."""

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)


def parse_repo_runner_pools(raw: str | None) -> RepoRunnerPools:
    """Parse the env value; blank is empty, anything invalid fails closed."""

    if raw is None or not raw.strip():
        return RepoRunnerPools({})
    try:
        return RepoRunnerPools.model_validate_json(raw)
    except ValidationError as exc:
        first = exc.errors(include_input=False, include_context=False, include_url=False)[0]
        category = "invalid JSON" if first["type"] == "json_invalid" else "invalid configuration"
        raise ValueError(
            f"{REPO_RUNNER_POOLS_ENV} is not a valid repository runner pool map: "
            f"{exc.error_count()} error(s), {category}"
        ) from None


def platform_runner_digest(image: str | None) -> str | None:
    """The ``sha256:`` digest pinned on a runner image reference, or None."""

    match = _IMAGE_DIGEST.search(image or "")
    return match.group(1) if match else None


def build_schema() -> dict[str, Any]:
    """Export the pool input as a JSON Schema document."""

    schema = RepoRunnerPools.model_json_schema()
    schema["propertyNames"] = TypeAdapter(RepositoryPath).json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://curietech.ai/schemas/repo-runner-pools.schema.json"
    return schema


def render_schema() -> str:
    """Render the canonical schema with sorted keys and a trailing newline."""

    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"
