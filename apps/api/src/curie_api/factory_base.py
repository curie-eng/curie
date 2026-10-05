"""Resolve the base branch a factory ticket starts from and targets (#3095, ADR 0186).

The deployment allows bases per repository (``GITHUB_FACTORY_BASES``). A ticket
picks one with a single ``base:<branch>`` label; with none it gets the
deployment default, or the repository default branch. Two distinct labels, an
unallowed branch, or a branch the repository does not have are refused with
one marked issue comment, never substituted.

The branch read and the comment go through the forge ports: the code host's
``branch_head`` and the tracker's `MarkedComments`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from .config import Settings
from .factory_notices import code_span
from .forges.errors import ForgeError
from .forges.ports import CodeHost, MarkedComments
from .forges.types import ReplyTarget, RepositoryRef, TrackerIssueRef
from .github_factory_events import BASE_LABEL_PREFIX
from .github_review_events import FeedbackUnavailable
from .repo_full_name import entry_for_repo

# The core-owned marker; the tracker's comment adapter decides how to embed it.
REFUSAL_MARKER = "curie-factory-base-refusal"


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


async def resolve_base(
    *,
    settings: Settings,
    repo_full_name: str,
    labels: set[str],
    default_branch: str | None,
    code_host: CodeHost,
    repository: RepositoryRef,
) -> ResolvedBase | BaseRefusal:
    """Choose the base and read its head commit. A missing branch is refused.

    The code host answers the branch's head commit, None when ``repository``
    has no such branch, and raises a `ForgeError` when it cannot answer now.
    """

    choice = choose_base(labels, bases_for(settings, repo_full_name), default_branch)
    if isinstance(choice, BaseRefusal):
        return choice
    try:
        commit = await code_host.branch_head(repository, choice.branch)
    except ForgeError:
        raise FeedbackUnavailable("base_unavailable") from None
    if commit is None:
        return BaseRefusal(
            "base_missing", f"base {code_span(choice.branch)} does not exist in the repository"
        )
    return ResolvedBase(choice.branch, choice.source, commit)


def refusal_body(refusal: BaseRefusal) -> str:
    """The refusal comment's text; the comment adapter appends the marker."""

    return (
        f"Curie did not start this issue: {refusal.reason}. Fix the `base:` label or the"
        " deployment's allowed bases; Curie will pick the issue up again on its next pass."
    )


async def comment_refusal(
    comments: MarkedComments, issue: TrackerIssueRef, refusal: BaseRefusal
) -> None:
    """Keep the one refusal comment current. It is edited, never duplicated."""

    try:
        await comments.upsert_marked(
            ReplyTarget.on_issue(issue), REFUSAL_MARKER, refusal_body(refusal)
        )
    except ForgeError:
        raise FeedbackUnavailable("base_refusal_unavailable") from None
