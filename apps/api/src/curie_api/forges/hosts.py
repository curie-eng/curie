"""Build the code host a caller talks to (ADR 0197, "Two ports").

Two named constructors, and neither falls back to the other (#4397).
``factory_code_host`` is the factory's: the GitHub code host entry of the forge
registry, minting with that entry's App. ``code_host_for`` is the non-factory
publication and workspace paths': the install's own GitHub settings, so
publication works with no factory config at all.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

import httpx

from curie_api.config import Settings
from curie_api.forges import types
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.github.code_host import PATH_ID_PREFIX, GitHubCodeHost
from curie_api.forges.github.connection import GitHubCodeHostConnection, GitHubTrackerConnection
from curie_api.forges.ports import CodeHost
from curie_api.forges.registry import ForgeRegistry, forges_registry
from curie_api.forges.types import PullRequestRef, RepositoryRef, TrackerIssueRef
from curie_api.github_app import GitHubAuth, GitHubCredentials, credentials_for_auth


def code_host_for(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    installations: Mapping[str, int] | None = None,
) -> CodeHost:
    """The install's own GitHub as a code host, for the non-factory publication
    and workspace paths. It mints with the install's GitHub settings.

    ``installations`` maps a repository path to the App installation stored for
    it, which fences the reads that mint for that repository.
    """

    auth = GitHubAuth.from_settings(settings)
    return GitHubCodeHost(
        client,
        api_url=auth.api_url,
        html_base=settings.github_html_base,
        timeout_s=auth.timeout_s,
        credentials=credentials_for_auth(auth),
        installations=installations,
    )


def factory_code_host(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    *,
    installations: Mapping[str, int] | None = None,
) -> GitHubCodeHost:
    """The factory's code host: the forge registry's GitHub code host entry.

    Refused ``unavailable`` when no GitHub code host is configured.
    """

    return factory_code_host_entry(registry).adapter(client, installations=installations)


def factory_credentials(registry: ForgeRegistry) -> GitHubCredentials:
    """The factory's GitHub App resolver: the GitHub code host entry's, which
    every factory mint uses (tracker reads included; both entries name one App)."""

    return factory_code_host_entry(registry).credentials.credentials


def factory_tracker(registry: ForgeRegistry) -> GitHubTrackerConnection:
    """The factory's GitHub tracker entry. Refused ``unavailable`` when none."""

    connection = registry.github_tracker()
    if connection is None:
        raise Unavailable("tracker_unconfigured")
    return connection


def factory_repository(
    registry: ForgeRegistry, *, path: str, project_id: int | str
) -> RepositoryRef:
    """A repository on the factory's GitHub code host, by its stored id and path."""

    host = factory_code_host_entry(registry).config.host
    return RepositoryRef(types.GITHUB, host, str(project_id), path)


def factory_code_host_entry(registry: ForgeRegistry) -> GitHubCodeHostConnection:
    """The factory's GitHub code host entry. Refused ``unavailable`` when none."""

    connection = registry.github_code_host()
    if connection is None:
        raise Unavailable("code_host_unconfigured")
    return connection


def github_host(settings: Settings) -> str:
    """The identity host of the configured GitHub: its HTML base's authority.

    Migration 0103 backfills stored rows with the same derivation.
    """

    return urlsplit(settings.github_html_base).netloc.lower() or "github.com"


def repository_ref(settings: Settings, *, path: str, project_id: int | str | None) -> RepositoryRef:
    """A reference to a stored repository without a remote read.

    A stored row that predates its captured immutable id is addressed by path:
    its ``project_id`` is ``path:<owner/name>``, which `resolve_repository`
    accepts and which never equals a captured id.
    """

    identity = str(project_id) if project_id is not None else f"{PATH_ID_PREFIX}{path}"
    return RepositoryRef(types.GITHUB, github_host(settings), identity, path)


def github_issue_ref(
    settings: Settings, *, repository_id: int, issue_number: int
) -> TrackerIssueRef:
    """A GitHub issue: scoped by its repository's immutable id (ADR 0197)."""

    return TrackerIssueRef(
        types.GITHUB, github_host(settings), str(repository_id), str(issue_number)
    )


def issue_url(settings: Settings, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
    """The tracker's link to one issue. Makes no request.

    A GitHub issue is on its own repository, so ``repository`` supplies the path.
    Any other tracker links it through `forges.trackers.issue_link`.
    """

    if issue.kind != types.GITHUB:
        from curie_api.forges.trackers import issue_link

        return issue_link(forges_registry(settings), issue, repository)
    from curie_api.forges.github.tracker import issue_url as github_issue_url

    return github_issue_url(settings.github_html_base, repository.path, issue)


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
