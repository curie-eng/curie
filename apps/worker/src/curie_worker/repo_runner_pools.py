"""Pure schema for the deferred CURIE_REPO_RUNNER_POOLS worker input.

No environment reader or pool selection exists here. Pool names are opaque:
later chart rendering owns the agent and digest identity behind each name.
"""

from __future__ import annotations

import json
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
    field_validator,
)

Digest = Annotated[StrictStr, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
RepositoryPath = Annotated[
    StrictStr,
    Field(pattern=r"^[A-Za-z0-9_][A-Za-z0-9._~-]*(?:/[A-Za-z0-9_][A-Za-z0-9._~-]*)+$"),
]
PoolName = Annotated[
    StrictStr, Field(max_length=63, pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


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

    pool: PoolName
    digests: tuple[Digest, Digest, Digest]
    base: Digest
    docker: StrictBool
    egress: tuple[EgressRule, ...] = ()


class RepoRunnerPools(RootModel[dict[RepositoryPath, RepoRunnerPool]]):
    """The repository binding paths carried by worker repository names."""

    model_config = ConfigDict(frozen=True)


def build_schema() -> dict[str, Any]:
    """Export the pool input without adding a runtime consumer."""

    schema = RepoRunnerPools.model_json_schema()
    schema["propertyNames"] = TypeAdapter(RepositoryPath).json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://curietech.ai/schemas/repo-runner-pools.schema.json"
    return schema


def render_schema() -> str:
    """Render the canonical schema with sorted keys and a trailing newline."""

    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"
