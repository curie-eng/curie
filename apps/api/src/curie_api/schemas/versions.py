import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from ..models import GIT_FLOW_CREATED_BY
from .common import validate_optional_commit_sha


class VersionCreate(BaseModel):
    version_label: str
    bundle_ref: str | None = None
    commit_sha: str | None = None
    created_by: str

    _check_commit_sha = field_validator("commit_sha")(validate_optional_commit_sha)

    @field_validator("created_by")
    @classmethod
    def reject_internal_provenance(cls, value: str) -> str:
        if value == GIT_FLOW_CREATED_BY:
            raise ValueError("created_by is reserved for internal Git flow versions")
        return value


class VersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version_label: str
    bundle_ref: str | None
    bundle_sha256: str | None
    commit_sha: str | None
    created_by: str
    created_at: datetime


class BundleOut(BaseModel):
    """Result of storing a bundle for a version."""

    version_id: uuid.UUID
    bundle_ref: str
    bundle_sha256: str
    size_bytes: int


class BundleFile(BaseModel):
    """One text file inside a stored bundle (path relative to the bundle root)."""

    path: str
    content: str


class BundleFiles(BaseModel):
    """The readable text surfaces of a version's stored bundle."""

    files: list[BundleFile]
