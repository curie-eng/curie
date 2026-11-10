"""Pure factory runner, Docker and tool artifact contracts (ADRs 0216 and 0217).

These declarations validate operator input and export schemas. They neither
build images nor read manifests, environment variables or repository pools.
"""

from __future__ import annotations

from ipaddress import ip_network
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

CatalogTool = Literal["uv", "rust", "pnpm", "build-essential", "docker-cli"]
Platform = Literal["linux/amd64", "linux/arm64"]
CATALOG_TOOLS: tuple[CatalogTool, ...] = (
    "uv",
    "rust",
    "pnpm",
    "build-essential",
    "docker-cli",
)

Digest = Annotated[StrictStr, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
PinnedImage = Annotated[StrictStr, Field(pattern=r"^[^@\s]+@sha256:[0-9a-f]{64}$")]
NonBlank = Annotated[StrictStr, Field(min_length=1, pattern=r"\S")]


def _executable(command: tuple[str, ...]) -> tuple[str, ...]:
    if not command[0].strip():
        raise ValueError("a probe command must start with a non-blank executable")
    return command


Command = Annotated[
    tuple[StrictStr, ...],
    Field(min_length=1, json_schema_extra={"prefixItems": [{"type": "string", "pattern": r"\S"}]}),
    AfterValidator(_executable),
]
CachePath = Annotated[
    StrictStr,
    Field(
        pattern=r"^/home/runner(?:/[^\s/]+)*$",
        json_schema_extra={"not": {"pattern": r"(?:^|/)\.\.(?:/|$)"}},
    ),
]
CacheVariable = Annotated[StrictStr, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
RegistryHost = Annotated[
    StrictStr, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9.-]*(?::[0-9]{1,5})?$")
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ToolProbe(_Model):
    """A named executable added by an optional repository runner Dockerfile."""

    name: NonBlank
    command: Command


class RunnerConfig(_Model):
    """The tools and immutable image identities of a repository runner."""

    tools: tuple[CatalogTool, ...] = Field(json_schema_extra={"uniqueItems": True})
    image: PinnedImage
    base: Digest
    platform_digests: dict[Platform, Digest] = Field(
        min_length=2,
        max_length=2,
        json_schema_extra={"required": ["linux/amd64", "linux/arm64"]},
    )
    dockerfile: NonBlank | None = None
    probes: tuple[ToolProbe, ...] = ()

    @model_validator(mode="after")
    def check_unique_tools_and_probes(self) -> Self:
        if len(set(self.tools)) != len(self.tools):
            raise ValueError("runner tools must not contain duplicate catalog names")
        names = [probe.name for probe in self.probes]
        if len(set(names)) != len(names):
            raise ValueError("runner probes must have unique names")
        return self


class DockerEgressPort(_Model):
    """One TCP destination admitted by a Docker egress declaration."""

    protocol: Literal["TCP"] = "TCP"
    port: Annotated[StrictInt, Field(ge=1, le=65535)]


class DockerEgressRule(_Model):
    """A destination network and the TCP ports allowed within it."""

    cidr: Annotated[StrictStr, Field(pattern=r"^[^\s/]+/[0-9]{1,3}$")]
    ports: tuple[DockerEgressPort, ...] = Field(min_length=1)

    @field_validator("cidr")
    @classmethod
    def check_cidr(cls, value: str) -> str:
        ip_network(value)
        return value


class DockerConfig(_Model):
    """Operator Docker opt-in; a disabled declaration grants no egress."""

    enabled: StrictBool
    egress_allowlist: tuple[DockerEgressRule, ...] = ()


class ManifestTool(_Model):
    """A tool whose executable and version were observed during image build."""

    name: NonBlank
    version: NonBlank
    path: NonBlank
    source: Literal["catalog", "probe"]


class ToolManifest(_Model):
    """The future CLI-produced, runner-read tool manifest carried in an image."""

    schema_version: Literal[1]
    tools: tuple[ManifestTool, ...]
    docker_cli: StrictBool

    @field_validator("schema_version", mode="before")
    @classmethod
    def check_schema_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be integer 1")
        return value


class ToolCatalogEntry(_Model):
    """Pinned platform layers, writable caches and the catalog version probe."""

    name: CatalogTool
    platforms: tuple[Platform, ...] = Field(
        min_length=1, json_schema_extra={"uniqueItems": True}
    )
    source_digests: dict[Platform, PinnedImage] = Field(min_length=1)
    cache_env: dict[CacheVariable, CachePath]
    registry_hosts: tuple[RegistryHost, ...]
    probe: Command

    @model_validator(mode="after")
    def check_platforms_and_caches(self) -> Self:
        if len(set(self.platforms)) != len(self.platforms):
            raise ValueError("catalog platforms must be unique")
        if set(self.source_digests) != set(self.platforms):
            raise ValueError("source_digests must pin exactly the declared platforms")
        if any(".." in PurePosixPath(path).parts for path in self.cache_env.values()):
            raise ValueError("catalog cache paths must stay under the writable /home/runner")
        return self


def manifest_schema() -> dict[str, Any]:
    """Export the tool manifest as a Draft 2020-12 JSON Schema."""

    schema = ToolManifest.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://curietech.ai/schemas/tool-manifest.schema.json"
    return schema


def catalog_entry_schema() -> dict[str, Any]:
    """Export one catalog entry as a Draft 2020-12 JSON Schema."""

    schema = ToolCatalogEntry.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://curietech.ai/schemas/tool-catalog-entry.schema.json"
    schema["allOf"] = [
        {
            "if": {"properties": {"platforms": {"contains": {"const": platform}}}},
            "then": {"properties": {"source_digests": {"required": [platform]}}},
            "else": {"properties": {"source_digests": {"not": {"required": [platform]}}}},
        }
        for platform in ("linux/amd64", "linux/arm64")
    ]
    return schema
