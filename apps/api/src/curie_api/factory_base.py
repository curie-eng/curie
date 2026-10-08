"""Resolve the base branch a factory ticket starts from and targets (#3095, ADR 0186).

The deployment allows bases per repository (``GITHUB_FACTORY_BASES``). A ticket
picks one with a single ``base:<branch>`` label; with none it gets the
deployment default, or the repository default branch. Two distinct labels, an
unallowed branch, or a branch the repository does not have are refused with
one marked issue comment, never substituted.

GitHub REST:
https://docs.github.com/en/rest/branches/branches#get-a-branch
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

import httpx

from .config import Settings
from .factory_notices import NOT_IMPLEMENTABLE_LABEL, code_span, upsert_issue_notice
from .github_factory_events import BASE_LABEL_PREFIX
from .github_review_events import FeedbackUnavailable
from .github_review_truth import github_headers
from .repo_full_name import entry_for_repo

logger = logging.getLogger(__name__)

REFUSAL_MARKER = "<!-- curie-factory-base-refusal -->"
_SHA = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class BaseChoice:
    """The branch the labels and the deployment select, before it is read."""

    branch: str
    source: Literal["label", "default"]


@dataclass(frozen=True)
class ResolvedBase:
    branch: str
    source: Literal["label", "default"]
    commit: str


@dataclass(frozen=True)
class BaseRefusal:
    code: Literal["base_conflict", "base_not_allowed", "base_missing"]
    reason: str


def bases_for(settings: Settings, repo_full_name: str) -> dict[str, Any] | None:
    """The deployment's entry for one repository, matched case-insensitively."""

    return entry_for_repo(settings.github_factory_bases, repo_full_name)


def _named(labels: set[str]) -> set[str]:
    return {name[len(BASE_LABEL_PREFIX) :] for name in labels if name.startswith(BASE_LABEL_PREFIX)}


def _default(entry: dict[str, Any] | None, default_branch: str | None) -> str | None:
    if entry is not None and entry.get("default_base") is not None:
        return str(entry["default_base"])
    return default_branch


def _single_base(
    named: set[str], entry: dict[str, Any] | None, default_branch: str | None
) -> str | None:
    """The one labelled branch, else the default. ``named`` has at most one."""

    return next(iter(named)) if named else _default(entry, default_branch)


def choose_base(
    labels: set[str], entry: dict[str, Any] | None, default_branch: str | None
) -> BaseChoice | BaseRefusal:
    """Apply the precedence: one label, else the default. Reads nothing."""

    named = _named(labels)
    if len(named) > 1:
        listed = ", ".join(code_span(BASE_LABEL_PREFIX + name) for name in sorted(named))
        return BaseRefusal("base_conflict", f"the issue has more than one base label ({listed})")
    branch = _single_base(named, entry, default_branch)
    if branch is None:
        # The repository read carried no default branch; ask again later.
        raise FeedbackUnavailable("repository_unavailable")
    choice = BaseChoice(branch, "label" if named else "default")
    allowed = entry["bases"] if entry is not None else [default_branch]
    if choice.branch not in allowed:
        return BaseRefusal(
            "base_not_allowed",
            f"base {code_span(choice.branch)} is not an allowed base for this deployment",
        )
    return choice


def label_disagreement(
    labels: set[str],
    entry: dict[str, Any] | None,
    default_branch: str | None,
    recorded: str,
) -> str | None:
    """The branch the labels now name when it is not the recorded base.

    The recorded base is kept regardless; this is only what the status comment
    reports. Two labels name no single branch, so they report nothing.
    """

    named = _named(labels)
    if len(named) > 1:
        return None
    branch = _single_base(named, entry, default_branch)
    return branch if branch is not None and branch != recorded else None


async def read_base_commit(
    client: httpx.AsyncClient,
    *,
    api: str,
    token: str,
    repo_path: str,
    branch: str,
) -> str | None:
    """The branch head commit, or None when the repository has no such branch."""

    try:
        response = await client.get(
            f"{api}{repo_path}/branches/{quote(branch, safe='/')}",
            headers=github_headers(token),
            follow_redirects=False,
        )
    except httpx.HTTPError:
        raise FeedbackUnavailable("base_unavailable") from None
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise FeedbackUnavailable("base_unavailable")
    try:
        payload = response.json()
    except ValueError:
        raise FeedbackUnavailable("base_unavailable") from None
    commit = payload.get("commit") if isinstance(payload, dict) else None
    sha = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(sha, str) or _SHA.fullmatch(sha) is None:
        raise FeedbackUnavailable("base_unavailable")
    return sha


async def resolve_base(
    client: httpx.AsyncClient,
    *,
    settings: Settings,
    token: str,
    repo_full_name: str,
    repo_path: str,
    labels: set[str],
    default_branch: str | None,
) -> ResolvedBase | BaseRefusal:
    """Choose the base and read its head commit. A missing branch is refused."""

    choice = choose_base(labels, bases_for(settings, repo_full_name), default_branch)
    if isinstance(choice, BaseRefusal):
        return choice
    commit = await read_base_commit(
        client,
        api=settings.github_api_url.rstrip("/"),
        token=token,
        repo_path=repo_path,
        branch=choice.branch,
    )
    if commit is None:
        return BaseRefusal(
            "base_missing", f"base {code_span(choice.branch)} does not exist in the repository"
        )
    return ResolvedBase(choice.branch, choice.source, commit)


def refusal_body(refusal: BaseRefusal) -> str:
    return (
        f"Curie did not start this issue: {refusal.reason}. Fix the `base:` label or the"
        " deployment's allowed bases; Curie will pick the issue up again on its next pass."
        f"\n\n{REFUSAL_MARKER}\n"
    )


async def comment_refusal(
    client: httpx.AsyncClient,
    *,
    settings: Settings,
    token: str,
    repo_path: str,
    issue_number: int,
    refusal: BaseRefusal,
) -> None:
    """Keep the one refusal comment current and label the issue rejected.

    The comment is edited, never duplicated. The rejection label joins it once
    the comment stands (ADR 0199 decisions 5.1 and 5.4); the state label pass
    removes the label when the issue is later admitted.
    """

    api = settings.github_api_url.rstrip("/")
    outcome = await upsert_issue_notice(
        client,
        api=api,
        repo_path=repo_path,
        headers=github_headers(token),
        issue_number=issue_number,
        marker=REFUSAL_MARKER,
        body=refusal_body(refusal),
        app_id=settings.github_app_id,
    )
    if outcome == "unavailable":
        raise FeedbackUnavailable("base_refusal_unavailable")
    try:
        added = await client.post(
            f"{api}{repo_path}/issues/{issue_number}/labels",
            headers=github_headers(token),
            json={"labels": [NOT_IMPLEMENTABLE_LABEL]},
            follow_redirects=False,
        )
    except httpx.HTTPError:
        raise FeedbackUnavailable("base_refusal_unavailable") from None
    if added.status_code in {401, 403, 404}:
        # A refused write matches how _sync_labels treats one: logged, and the
        # refusal stands so the ticket is not retried for the label alone.
        logger.warning(
            "factory base rejection label refused",
            extra={"issue_number": issue_number, "status": added.status_code},
        )
    elif added.status_code not in {200, 201}:
        raise FeedbackUnavailable("base_refusal_unavailable")
