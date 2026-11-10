"""Choose the repository a ticket runs against, at admission (ADR 0197 identity 3).

A native tracker's repository is its own project, so labels and components
play no part. A tracker-only binding (Jira) picks, in order of precedence:

1. exactly one ``repo:<alias>`` label, which must name an alias on the binding;
2. else the aliases the ticket's components map to, when they agree on one;
3. else the binding's ``default_repo``.

The label wins because a person put it on this ticket to choose, while a
component map is a standing rule. More than one ``repo:`` label, an unknown
alias, or components mapping to different aliases with no label to decide are
refused with one comment, never guessed, as an unknown ``base:`` label is
(`curie_api.factory_base`). The choice is frozen on the WorkItem by the caller.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from typing import Literal

from curie_api.factory_notices import code_span as _code
from curie_api.forges.config import BindingConfig, RepositoryBindingConfig, TrackerConfig

REPO_LABEL_PREFIX = "repo:"
REFUSAL_MARKER = "<!-- curie-factory-repo-refusal -->"


@dataclass(frozen=True)
class Refusal:
    code: Literal["repo_conflict", "repo_unknown", "repo_component_conflict"]
    reason: str


def _listed(names: Iterable[str]) -> str:
    return ", ".join(_code(name) for name in sorted(names))


def resolve_repository(
    binding: BindingConfig,
    *,
    tracker: TrackerConfig,
    labels: Collection[str],
    components: Collection[str],
) -> RepositoryBindingConfig | Refusal:
    """The repository ``binding`` runs this ticket against, or why it will not.

    ``tracker`` is the binding's tracker: whether it is native decides whether
    labels and components are read at all. Reads nothing remote.
    """

    if binding.tracker != tracker.name:
        raise ValueError(f"binding {binding.name!r} is not bound to tracker {tracker.name!r}")
    if tracker.native:
        (repo,) = binding.repos
        return repo
    aliases = [repo.alias for repo in binding.repos]
    named = {
        label[len(REPO_LABEL_PREFIX) :] for label in labels if label.startswith(REPO_LABEL_PREFIX)
    }
    if len(named) > 1:
        listed = _listed(REPO_LABEL_PREFIX + name for name in named)
        return Refusal("repo_conflict", f"the issue has more than one repository label ({listed})")
    if named:
        (alias,) = named
        chosen = binding.repo(alias)
        if chosen is None:
            return Refusal(
                "repo_unknown",
                f"repository {_code(alias)} is not one of this binding's repositories"
                f" ({_listed(aliases)})",
            )
        return chosen
    mapped = {binding.components[name] for name in components if name in binding.components}
    if len(mapped) > 1:
        return Refusal(
            "repo_component_conflict",
            f"the issue's components map to more than one repository ({_listed(mapped)})",
        )
    fallback = next(iter(mapped)) if mapped else binding.default_repo
    # ForgesConfig requires a default_repo here and BindingConfig validates
    # every alias it maps to, so neither branch is reachable from a loaded config.
    resolved = binding.repo(fallback) if fallback is not None else None
    if resolved is None:
        raise ValueError(f"binding {binding.name!r} has no default repository")
    return resolved


def refusal_body(refusal: Refusal) -> str:
    return (
        f"Curie did not start this issue: {refusal.reason}. Fix the `repo:` label or the"
        " issue's components; Curie will pick the issue up again on its next pass."
        f"\n\n{REFUSAL_MARKER}\n"
    )
