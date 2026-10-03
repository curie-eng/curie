from pydantic import BaseModel, Field, field_validator

from ..workspace_policy import valid_repository_name


class RepositoryCredentialOut(BaseModel):
    """One server-derived Git credential returned only to the trusted worker."""

    repo_full_name: str
    clone_url: str
    authorization_header: str
    revision: str | None = None


class WorkspaceCredentialOut(RepositoryCredentialOut):
    """The workspace clone credential, plus the base a factory WorkItem froze.

    The worker clones ``base_branch`` and pins ``base_commit`` (ADR 0186). Both
    are None outside the factory and on legacy WorkItems.
    """

    base_branch: str | None = None
    base_commit: str | None = None


class WorkspaceSelectionRequest(BaseModel):
    conversation_id: str = Field(min_length=1)
    author: str = Field(min_length=1)
    repo_full_name: str | None = None

    @field_validator("repo_full_name")
    @classmethod
    def _canonical_repo(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not valid_repository_name(value):
            raise ValueError("repo_full_name must be one canonical owner/repository name")
        return value


class WorkspaceSelectionOut(BaseModel):
    repo_full_name: str | None
    revision: str | None = None


class WorkspaceCredentialRequest(BaseModel):
    conversation_id: str = Field(min_length=1)
