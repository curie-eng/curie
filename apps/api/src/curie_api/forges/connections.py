"""What the forge registry builds per config entry (ADR 0197, #4397).

A connection is one configured tracker or code host. It holds the entry's
config and credential and yields the adapters the factory core talks to
through `curie_api.forges.ports`.

A tracker connection is scoped one of two ways (#4358 plan D13). A native
tracker (GitHub, later GitLab) yields a tracker per repository, because its
issues live in the repository the binding names. A tracker-only kind (Jira
Cloud) yields a tracker per binding, because one site serves every repository
the binding lists. Both refinements are runtime checkable, so the registry can
refuse the wrong one by name instead of failing on a missing method.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

import httpx

from curie_api.forges.config import BindingConfig, CodeHostConfig, TrackerConfig
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.types import RepositoryRef, TrackerIssueRef


class TrackerConnection(Protocol):
    """Every configured tracker: its config entry."""

    config: TrackerConfig


@runtime_checkable
class RepositoryTrackerConnection(TrackerConnection, Protocol):
    """A tracker whose issues live in one repository (GitHub)."""

    def for_repository(
        self,
        client: httpx.AsyncClient,
        *,
        repo_full_name: str,
        repository_id: int,
        token: Any,
        label: str | None = None,
    ) -> Tracker:
        """The tracker on one repository. ``label`` replaces the entry's label."""
        ...


@runtime_checkable
class BindingTrackerConnection(TrackerConnection, Protocol):
    """A tracker one binding scopes (Jira Cloud): its scope id and start
    authority come from the binding, not from a repository."""

    def for_binding(
        self, client: httpx.AsyncClient, binding: BindingConfig, *, label: str | None = None
    ) -> Tracker:
        """The tracker on ``binding``'s scope. ``label`` replaces the entry's label."""
        ...

    def issue_url(self, issue: TrackerIssueRef) -> str:
        """The web link to ``issue`` with no request; `NotFound` when this
        tracker cannot name it."""
        ...


class CodeHostConnection(Protocol):
    config: CodeHostConfig

    @property
    def credentials(self) -> Any:
        """The provider that mints this code host's credentials."""
        ...

    def adapter(
        self,
        client: httpx.AsyncClient,
        *,
        installations: Mapping[str, int] | None = None,
        repository: RepositoryRef | None = None,
    ) -> CodeHost:
        """The code host adapter. ``installations`` maps a repository path to
        the App installation stored for it. With ``repository``, its
        ``marked_comments`` are the status comments on that repository's pull
        requests and review threads."""
        ...

    async def installation_for(self, repository: RepositoryRef) -> int | None:
        """The App installation this code host has on ``repository`` now, the
        fence a WorkItem records (#4358 plan D14 item 2). None on a kind with no
        installations; `Unavailable` when it cannot be answered."""
        ...

    async def installation_token(
        self,
        repository: RepositoryRef,
        installation_id: int,
        *,
        timeout_s: float | None = None,
    ) -> str:
        """A token minted only for the recorded ``installation_id`` on
        ``repository`` (plan D14 item 2a). ``timeout_s`` caps the provider's own
        timeout. A refused or changed installation is `Unavailable`."""
        ...
