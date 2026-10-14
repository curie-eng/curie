"""Issue a repository credential to the trusted worker through CodeHost (ADR 0197).

The workspace clone and the publication push both receive the same facts as
data: the clean clone URL, the header value git sends and the header form that
names it, the origin the header is scoped to, and a reference to the CA bundle
mounted where the git command runs. The worker derives none of them; it holds
no forge code (ADR 0197, "Two ports" item 6).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .forges.hosts import code_host_for, forge_code_host, repository_ref
from .forges.ports import CodeHost
from .forges.types import GITHUB, CredentialHeader, CredentialScope, RepositoryRef
from .models import ThreadPublicationLineage, WorkItem


@dataclass(frozen=True)
class IssuedCredential:
    clone_url: str
    authorization_header: str
    origin: str
    header_form: CredentialHeader
    ca_bundle_ref: str | None


async def thread_repository(
    session: AsyncSession,
    settings: Settings,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
) -> RepositoryRef:
    """The repository a thread works on, with its code host identity.

    A factory WorkItem on the thread froze its repository's code host kind,
    host and project id at admission, so the thread's repository is that one
    (the newest WorkItem when the thread had several). A thread with none is
    a path on the install's own GitHub, as every non-factory thread has been.
    """

    item = await session.scalar(
        select(WorkItem)
        .where(
            WorkItem.agent_id == agent_id,
            WorkItem.conversation_id == conversation_id,
            func.lower(WorkItem.repository_path) == repo_full_name.casefold(),
        )
        .order_by(WorkItem.created_at.desc(), WorkItem.id.desc())
        .limit(1)
    )
    if item is None:
        return repository_ref(settings, path=repo_full_name, project_id=None)
    return replace(item.repository, path=repo_full_name)


async def lineage_repository(
    session: AsyncSession, settings: Settings, lineage: ThreadPublicationLineage
) -> RepositoryRef:
    """The repository a publication lineage is on: its verified code host
    identity once an advance recorded one, else its thread's (`thread_repository`)."""

    return lineage.repository or await thread_repository(
        session,
        settings,
        agent_id=lineage.agent_id,
        conversation_id=lineage.conversation_id,
        repo_full_name=lineage.repo_full_name,
    )


async def issue_repository_credential(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    repo_full_name: str,
    project_id: int | str | None,
    scope: CredentialScope,
) -> IssuedCredential:
    """The install's own GitHub credential for one stored repository.

    Raises `curie_api.forges.errors.ForgeError` when the code host cannot issue
    one. The header value is what follows the header name in
    ``Credential.git_header()``, so GitHub's stays exactly ``Basic <base64>``.
    """

    code_host = code_host_for(settings, client)
    repository = repository_ref(settings, path=repo_full_name, project_id=project_id)
    return await _issue(settings, code_host, repository, scope)


async def issue_stored_credential(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    repository: RepositoryRef,
    scope: CredentialScope,
) -> IssuedCredential:
    """The credential of the code host a stored repository names.

    A GitHub repository's comes from the install's GitHub
    (`issue_repository_credential`); any other from its forge registry entry.
    """

    if repository.kind == GITHUB:
        return await issue_repository_credential(
            settings,
            client,
            repo_full_name=repository.path,
            project_id=repository.project_id,
            scope=scope,
        )
    code_host = forge_code_host(settings, client, repository)
    return await _issue(settings, code_host, repository, scope)


async def _issue(
    settings: Settings, code_host: CodeHost, repository: RepositoryRef, scope: CredentialScope
) -> IssuedCredential:
    credential = await code_host.credential(repository, scope)
    _, _, value = credential.git_header().partition(": ")
    return IssuedCredential(
        clone_url=f"{credential.origin}/{repository.path}.git",
        authorization_header=value,
        origin=credential.origin,
        header_form=credential.header,
        ca_bundle_ref=settings.code_host_ca_bundle.strip() or None,
    )
