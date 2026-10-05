"""Operator authorization policy for runtime repository workspaces."""

from __future__ import annotations

import re

from .forges import types as forge_types
from .forges.paths import valid_repository_path

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
    return any(
        entry.casefold() == repo or entry.casefold() == f"{owner}/*"
        for entry in allowlist
    )


def credential_mode(*, app_id: str, app_private_key: str, token: str) -> str:
    if app_id and app_private_key:
        return "github_app"
    if token:
        return "raw_token_fallback"
    return "anonymous"
