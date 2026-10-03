"""Strict GitHub repository binding validation and URL path construction."""

import re
from collections.abc import Mapping
from typing import Annotated
from urllib.parse import quote

from pydantic import AfterValidator, StringConstraints

_OWNER_RE = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
_REPOSITORY_RE = re.compile(r"[A-Za-z0-9._-]+")
_SCHEMA_RE = r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*/[A-Za-z0-9._-]+$"


class InvalidRepoFullName(ValueError):
    """A repository binding is not one valid GitHub ``owner/name`` pair."""


def normalize_repo_full_name(value: str) -> str:
    """Validate one repository binding and preserve its spelling exactly."""

    if not isinstance(value, str) or value.count("/") != 1:
        raise InvalidRepoFullName(
            "repo_full_name must be exactly one GitHub owner/name pair"
        )

    owner, repository = value.split("/", 1)
    if (
        not 1 <= len(owner) <= 39
        or _OWNER_RE.fullmatch(owner) is None
        or not 1 <= len(repository) <= 100
        or repository in {".", ".."}
        or _REPOSITORY_RE.fullmatch(repository) is None
    ):
        raise InvalidRepoFullName(
            "repo_full_name must be exactly one GitHub owner/name pair"
        )
    return value


RepoFullName = Annotated[
    str,
    StringConstraints(min_length=3, max_length=140, pattern=_SCHEMA_RE),
    AfterValidator(normalize_repo_full_name),
]


def repo_url_path(repo_full_name: str) -> str:
    """Return an encoded two segment path for a validated repository binding."""

    owner, repository = normalize_repo_full_name(repo_full_name).split("/", 1)
    return f"{quote(owner, safe='')}/{quote(repository, safe='')}"


def entry_for_repo[V](entries: Mapping[str, V], repo_full_name: str) -> V | None:
    """The first entry keyed by ``repo_full_name``, matched case-insensitively."""

    wanted = repo_full_name.casefold()
    for name, value in entries.items():
        if name.casefold() == wanted:
            return value
    return None
