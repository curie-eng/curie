"""What the forge registry builds per config entry (ADR 0197, #4397).

A connection is one configured tracker or code host. It holds the entry's
config and credential and yields the per-repository adapters the factory
core talks to through `curie_api.forges.ports`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import httpx

from curie_api.forges.config import CodeHostConfig, TrackerConfig
from curie_api.forges.ports import CodeHost, Tracker


class TrackerConnection(Protocol):
    config: TrackerConfig

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


class CodeHostConnection(Protocol):
    config: CodeHostConfig

    @property
    def credentials(self) -> Any:
        """The provider that mints this code host's credentials."""
        ...

    def adapter(
        self, client: httpx.AsyncClient, *, installations: Mapping[str, int] | None = None
    ) -> CodeHost:
        """The code host adapter. ``installations`` maps a repository path to
        the App installation stored for it."""
        ...
