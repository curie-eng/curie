"""Operator authorization policy for runtime repository workspaces."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .forges import types as forge_types
from .forges.paths import valid_repository_path

if TYPE_CHECKING:
    from .config import Settings
    from .forges.types import RepositoryRef

_OWNER_WILDCARD = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/\*$")


def valid_allowlist_entry(entry: str) -> bool:
    """A ``GITHUB_REPO_ALLOWLIST`` entry: one GitHub owner/name, or owner/*.

    The allowlist is GitHub's, so its exact entries follow the GitHub path rule
    in ``curie_api.forges.paths``.
    """

    return bool(
        valid_repository_path(forge_types.GITHUB, entry) or _OWNER_WILDCARD.fullmatch(entry)
    )


def repository_is_allowed(repo_full_name: str, allowlist: tuple[str, ...]) -> bool:
    """Match exact owner/repository or owner-wide owner/* entries."""

    repo = repo_full_name.casefold()
    owner = repo.split("/", 1)[0]
    return any(entry.casefold() == repo or entry.casefold() == f"{owner}/*" for entry in allowlist)


def repository_allowed(settings: Settings, repository: RepositoryRef) -> bool:
    """Whether the install authorizes work on ``repository``, by its code host.

    ``GITHUB_REPO_ALLOWLIST`` is GitHub's, so a GitHub repository is held to
    it. On any other code host the forge config is the authority: a binding
    must list the repository (its project id and path) on an entry of that
    kind and host. A GitHub owner/* entry never admits another host's path.
    """

    if repository.kind == forge_types.GITHUB:
        return repository_is_allowed(repository.path, settings.github_repo_allowlist)
    config = settings.forges
    return any(
        (entry := config.code_host(repo.code_host)).kind == repository.kind
        and entry.host == repository.host
        and repo.project_id == repository.project_id
        and repo.path == repository.path
        for binding in config.bindings
        for repo in binding.repos
    )


def credential_mode(*, app_id: str, app_private_key: str, token: str) -> str:
    if app_id and app_private_key:
        return "github_app"
    if token:
        return "raw_token_fallback"
    return "anonymous"
