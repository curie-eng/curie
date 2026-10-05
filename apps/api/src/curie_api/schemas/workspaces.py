from pydantic import BaseModel, Field, ValidationInfo, field_validator

from ..forges import types as forge_types
from ..forges.config import CodeHostKind
from ..forges.paths import valid_repository_path
from ..forges.types import CredentialHeader


class RepositoryCredentialOut(BaseModel):
    """One server-derived Git credential returned only to the trusted worker.

    ``origin``, ``header_form`` and ``ca_bundle_ref`` are the code host
    transport facts (ADR 0197). Each defaults to today's GitHub behavior: the
    configured GitHub host, an ``Authorization: Basic`` header, and the public
    trust store.
    """

    repo_full_name: str
    clone_url: str
    authorization_header: str
    revision: str | None = None
    # The origin (scheme, host and optional base path) this credential
    # authenticates to. None is the configured GitHub host.
    origin: str | None = None
    # Which header git sends the credential in. GitLab refuses a Bearer
    # header, so the form travels with the credential instead of being assumed.
    header_form: CredentialHeader = CredentialHeader.AUTHORIZATION_BASIC
    # A reference to a PEM CA bundle for a self-managed code host, mounted
    # where the clone runs. A reference, never certificate bytes. None trusts
    # the public store, as today.
    ca_bundle_ref: str | None = None


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
    # The code host kind ``repo_full_name`` is a path on (ADR 0197). Declared
    # before the path so its validator can read it. GitHub keeps exactly
    # owner/name; a kind that nests groups accepts a deeper path.
    repository_kind: CodeHostKind = "github"
    repo_full_name: str | None = None

    @field_validator("repo_full_name")
    @classmethod
    def _canonical_repo(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None:
            return None
        kind = info.data.get("repository_kind")
        if kind is None:
            # The kind itself failed validation and is already reported.
            return value
        if not valid_repository_path(kind, value):
            if kind == forge_types.GITHUB:
                raise ValueError("repo_full_name must be one canonical owner/repository name")
            raise ValueError(
                f"repo_full_name must be a {kind} repository path of two or more segments"
            )
        return value


class WorkspaceSelectionOut(BaseModel):
    repo_full_name: str | None
    revision: str | None = None


class WorkspaceCredentialRequest(BaseModel):
    conversation_id: str = Field(min_length=1)
