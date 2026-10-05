import base64
import binascii
import re
import uuid
from datetime import datetime
from typing import Literal

from aci_protocol.turn import SLACK_KIND, route_identity
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from ..forges import types as forge_types
from ..forges.paths import REPOSITORY_FULL_NAME_PATTERN, valid_repository_path
from ..identities import refuse_undeclared
from .channels import (
    BUILTIN_CLUSTER_MESSAGE_ADAPTER,
    CHANNEL_KIND,
    validate_channel_binding,
    validate_channel_endpoint,
)


class PublicationCreate(BaseModel):
    """Trusted snapshot facts used to atomically create approval + publication."""

    deployment_id: uuid.UUID
    conversation_id: str = Field(min_length=1)
    reply_conversation_id: str | None = Field(default=None, min_length=1)
    repo_full_name: str = Field(pattern=REPOSITORY_FULL_NAME_PATTERN)
    author: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    reply_kind: str = Field(min_length=1)
    reply_channel: str = Field(min_length=1)
    reply_placeholder: str | None = None
    reply_endpoint: str | None = None
    reply_adapter: str | None = None
    dedupe_key: str = Field(min_length=1)
    review_origin_key: str | None = Field(default=None, min_length=1, max_length=180)
    route: str | None = None
    base_sha: str
    work_item_request_id: uuid.UUID | None = None
    work_item_runtime_epoch: int | None = Field(default=None, ge=1)
    observed_title: str | None = Field(default=None, max_length=256)
    observed_body_sha256: str | None = Field(default=None, pattern=r"[0-9a-f]{64}")
    observed_lineage_id: uuid.UUID | None = None
    observed_lineage_version: int | None = Field(default=None, ge=1)
    patch_b64: str
    changed_paths: list[str] = Field(max_length=4096)
    expires_in_seconds: int | None = Field(default=None, ge=1)
    title: str | None = Field(default=None, max_length=256)
    body: str | None = Field(default=None, max_length=65_536)

    @field_validator("repo_full_name")
    @classmethod
    def _canonical_publication_repo(cls, value: str) -> str:
        if not valid_repository_path(forge_types.GITHUB, value):
            raise ValueError("repo_full_name must be one canonical owner/repository name")
        return value

    @field_validator("base_sha")
    @classmethod
    def _full_base_sha(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            raise ValueError("base_sha must be one full 40-character hexadecimal commit id")
        return value.lower()

    @model_validator(mode="after")
    def _valid_reply_route(self) -> "PublicationCreate":
        validate_channel_binding(self.reply_kind, self.reply_channel)

        # The built-in relay is a platform-set sentinel (the worker's own
        # disconnected-message consumer), not an identity or an egress
        # credential, on ANY kind including slack -- so it is exempt from the
        # kind-aware identity check below, exactly as it was exempt from the
        # both-or-neither check before this kind split existed.
        builtin_relay = self.reply_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER
        slack = self.reply_kind == SLACK_KIND

        if builtin_relay:
            if self.reply_endpoint is not None:
                raise ValueError(
                    "the built-in cluster-message publication reply route must not set an endpoint"
                )
        elif slack:
            # For Slack, reply_adapter is the identity with or without an
            # endpoint; an endpoint is the CLI stub's per-turn origin (#19).
            identity = route_identity(self.reply_kind, self.reply_adapter)
            refuse_undeclared(self.reply_kind, identity)
            self.__dict__["reply_adapter"] = identity
        elif (self.reply_endpoint is None) != (self.reply_adapter is None):
            raise ValueError("publication reply route must set endpoint and adapter together")

        if self.reply_adapter is not None and not CHANNEL_KIND.match(self.reply_adapter):
            raise ValueError("publication reply adapter must be a lowercase slug")
        if self.reply_endpoint is not None:
            validate_channel_endpoint(self.reply_endpoint)
        return self

    @field_validator("changed_paths")
    @classmethod
    def _safe_changed_paths(cls, value: list[str]) -> list[str]:
        for path in value:
            parts = path.split("/")
            folded = tuple(part.casefold() for part in parts)
            if folded[:2] == (".github", "workflows"):
                raise ValueError("GitHub workflow changes cannot be published by this capability")
            if folded[:1] == (".github",):
                raise ValueError("GitHub metadata changes cannot be published by this capability")
            if (
                not path
                or path.startswith("/")
                or parts[0].casefold() == ".git"
                or any(part in ("", ".", "..") for part in parts)
            ):
                raise ValueError("changed_paths must contain safe repository-relative paths")
        return value

    def decoded_patch(self) -> bytes:
        try:
            return base64.b64decode(self.patch_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("patch_b64 must be canonical base64") from exc


class ReviewRevisionReserve(BaseModel):
    repository_id: int = Field(gt=0, strict=True)
    pr_number: int = Field(gt=0, strict=True)
    expected_lineage_version: int = Field(ge=1, strict=True)
    origin_key: str = Field(min_length=1, max_length=180)


class ReviewRevisionOut(BaseModel):
    revision_id: uuid.UUID
    lineage_id: uuid.UUID
    agent_id: uuid.UUID
    conversation_id: str
    reply_conversation_id: str
    binding_id: uuid.UUID
    binding_generation: int
    repository_id: int
    installation_id: int
    pr_node_id: str
    base_ref: str
    repo_full_name: str
    pr_number: int
    branch: str
    base_sha: str
    expected_head_sha: str
    lineage_version: int
    revision_number: int
    version: int
    status: Literal["reserved", "consumed", "cancelled"]


class ReviewRevisionCancel(BaseModel):
    origin_key: str = Field(min_length=1, max_length=180)
    expected_version: int = Field(ge=1, strict=True)


class PublicationContextMint(BaseModel):
    """Trusted worker identity for a running factory execution."""

    model_config = ConfigDict(extra="forbid")

    deployment_id: uuid.UUID
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    runtime_epoch: int = Field(gt=0, strict=True)
    queued_event_id: str = Field(min_length=1, max_length=1024)


class PublicationPrecheck(BaseModel):
    """Observed metadata and a proposal, with no caller selected resource."""

    model_config = ConfigDict(extra="forbid")

    observed_title: str = Field(max_length=256)
    observed_body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: AwareDatetime
    proposed_title: str = Field(min_length=1, max_length=256)
    proposed_body: str = Field(min_length=1, max_length=65_536)

    @field_validator("observed_title", "proposed_title", "proposed_body")
    @classmethod
    def _utf8_metadata(cls, value: str) -> str:
        value.encode("utf-8")
        return value

    @field_validator("proposed_title", "proposed_body")
    @classmethod
    def _nonblank_metadata(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("publication title and body must be nonblank")
        return value


class PublicationPrecheckResult(BaseModel):
    result: Literal["unchanged", "metadata_changed"]


class PublicationLineageAdvance(BaseModel):
    """Exact compare-and-set facts for one publication revision outcome."""

    expected_version: int = Field(ge=1)
    expected_head_sha: str | None
    # The worker's claimed publication version and lease owner fence a stale
    # worker out after another worker reclaims the publication.
    expected_publication_version: int = Field(ge=1)
    lease_owner: str = Field(min_length=1, max_length=255)
    state: Literal["open", "merged", "closed"] = "open"
    pr_number: int = Field(gt=0)
    pr_url: str = Field(min_length=1, max_length=2048)
    head_sha: str
    metadata_updated_at: AwareDatetime | None

    @field_validator("expected_head_sha", "head_sha")
    @classmethod
    def _full_commit_sha(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            raise ValueError("lineage head must be one full 40-character commit id")
        return value.lower()


class PublicationLineageOut(BaseModel):
    """Credential-free pull-request lineage facts safe for the worker."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    deployment_id: uuid.UUID
    conversation_id: str
    repo_full_name: str
    base_sha: str
    branch: str
    pr_number: int | None
    pr_url: str | None
    head_sha: str | None
    state: Literal["open", "merged", "closed"] = Field(validation_alias="status")
    version: int
    latest_revision: int
    # This is intentionally only a boolean. The worker needs to know whether a
    # fenced replacement would race an unresolved revision, but must not learn
    # that revision's identifier or private patch state.
    has_pending_revision: bool = False
    # True while a terminal publication outcome has not yet been acknowledged
    # by the durable transcript outbox. No private patch or error text crosses
    # this read seam.
    has_pending_outcome: bool = False
    # Monotonic within one lineage. The worker stores this on its route so a
    # headless denial/failure causes exactly one cold history rehydrate even
    # though the Git head itself did not move.
    visible_outcome_revision: int = Field(default=0, ge=0)


class PublicationOut(BaseModel):
    """Patch-free publication metadata safe for operator and worker reads."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    approval_id: uuid.UUID
    deployment_id: uuid.UUID
    lineage_id: uuid.UUID | None
    revision_number: int | None
    expected_prior_head: str | None
    lineage_base_sha: str | None
    lineage_head_sha: str | None
    lineage_state: Literal["open", "merged", "closed"] | None
    lineage_version: int | None
    branch: str | None
    pr_number: int | None
    pr_url: str | None
    repo_full_name: str
    status: str
    open_as_draft: bool = False
    branch_prefix: str | None = None
    version: int
    base_sha: str
    changed_paths: list[str]
    title: str
    body: str
    reply_kind: str
    reply_channel: str
    reply_placeholder: str | None
    reply_endpoint: str | None
    reply_adapter: str | None
    result_url: str | None
    error: str | None
    created_at: datetime
    updated_at: datetime
    terminal_at: datetime | None
