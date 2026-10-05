"""Build the code host the factory core talks to (ADR 0197, "Two ports").

The one place a forge-neutral caller turns settings into a `CodeHost`. Today
every repository lives on the GitHub the settings name, so ``code_host_for``
returns the GitHub adapter; the typed binding config replaces these settings
later in #3831 without changing any caller.
"""

from __future__ import annotations

from urllib.parse import urlsplit

import httpx

from curie_api.config import Settings
from curie_api.forges import types
from curie_api.forges.errors import NotFound
from curie_api.forges.github.code_host import PATH_ID_PREFIX, GitHubCodeHost
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import PullRequestRef, RepositoryRef


def code_host_for(settings: Settings, client: httpx.AsyncClient) -> CodeHost:
    """The code host for this install's repositories."""

    return GitHubCodeHost(settings, client)


def repository_ref(settings: Settings, *, path: str, project_id: int | str | None) -> RepositoryRef:
    """A reference to a stored repository without a remote read.

    A stored row that predates its captured immutable id is addressed by path:
    its ``project_id`` is ``path:<owner/name>``, which `resolve_repository`
    accepts and which never equals a captured id.
    """

    host = urlsplit(settings.github_html_base).hostname or "github.com"
    identity = str(project_id) if project_id is not None else f"{PATH_ID_PREFIX}{path}"
    return RepositoryRef(types.GITHUB, host, identity, path)


def pull_request_ref(
    settings: Settings, *, path: str, project_id: int | str | None, number: int
) -> PullRequestRef:
    return PullRequestRef(repository_ref(settings, path=path, project_id=project_id), str(number))


async def resolve_stored(code_host: CodeHost, repository: RepositoryRef) -> RepositoryRef:
    """Read a stored repository now, refusing one whose immutable id changed.

    A stored GitHub row carries its path, so it resolves through that path and
    the answer's id must equal the stored one (ADR 0197 identity rule 2). A
    row without a captured id takes the id GitHub reports.
    """

    resolved = await code_host.resolve_repository(f"{PATH_ID_PREFIX}{repository.path}")
    stored = repository.project_id
    if not stored.startswith(PATH_ID_PREFIX) and resolved.project_id != stored:
        raise NotFound("repository_mismatch")
    return resolved
