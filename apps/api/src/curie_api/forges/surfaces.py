"""Tracker surfaces: what a ``github=``, ``gitlab=`` or ``jira=`` binding serves,
and the label that starts factory work through it (#4361, #4397 decisions 3
and 10).

A tracker surface is a channel binding whose kind names a tracker location.
``resolve_surface`` answers which configured tracker serves that location; a
location no configured tracker serves is refused ``surface_unconfigured``
before it is stored. ``effective_trigger_label`` is the only reader of a
surface's trigger label: the surface's own label, or the configured label of
its tracker when it has none.

GitHub needs no configured binding: a ``github=owner/name`` surface alone makes
its agent serve the repository (ADR 0214), and the one GitHub tracker serves it.
A GitLab project path is served by the binding whose GitLab tracker lists it,
and a Jira surface names a binding with a Jira Cloud tracker. In a build that
installs neither kind no such tracker can be configured, so every ``gitlab=``
and ``jira=`` surface is unconfigured.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.forges import types
from curie_api.forges.config import BindingConfig, TrackerConfig
from curie_api.forges.errors import NotFound
from curie_api.forges.registry import ForgeRegistry
from curie_api.forges.types import TrackerIssueRef
from curie_api.models import AgentChannel

# A tracker surface kind -> the tracker kind that serves it.
_TRACKER_KIND = {
    "github": types.GITHUB,
    "gitlab": types.GITLAB,
    "jira": types.JIRA_CLOUD,
}
TRACKER_SURFACE_KINDS = frozenset(_TRACKER_KIND)
# A tracker kind -> the surface kind that names its locations (#4358 plan D14).
SURFACE_KIND = {tracker: surface for surface, tracker in _TRACKER_KIND.items()}
# What a surface's address names: the repository its issues live in (a native
# tracker), or the config binding that scopes a tracker-only kind.
_ADDRESSED_BY: dict[str, Literal["repository", "binding"]] = {
    "github": "repository",
    "gitlab": "repository",
    "jira": "binding",
}


class SurfaceUnconfigured(ValueError):
    """No configured tracker serves this surface's location."""

    code = "surface_unconfigured"

    def __init__(self, kind: str, address: str, reason: str) -> None:
        super().__init__(f"{self.code}: {kind}:{address} {reason}")
        self.kind = kind
        self.address = address


class Surface(Protocol):
    """What a stored binding and a binding write both carry."""

    @property
    def kind(self) -> str: ...

    @property
    def address(self) -> str: ...

    @property
    def trigger_label(self) -> str | None: ...


@dataclass(frozen=True)
class SurfaceTarget:
    """The tracker serving one surface, the config binding that names its
    location (None for GitHub), and the location itself."""

    tracker: TrackerConfig | None
    binding_name: str | None
    location: str


def resolve_surface(registry: ForgeRegistry, kind: str, address: str) -> SurfaceTarget:
    """The tracker that serves the ``kind`` surface at ``address``.

    ``github`` always resolves, with the configured GitHub tracker (None when the
    install configures none, so a non-factory install still binds repositories).
    ``gitlab`` and ``jira`` resolve only to a configured binding, and are
    refused ``SurfaceUnconfigured`` otherwise.

    Raises:
        ValueError: ``kind`` is not a tracker surface kind.
        SurfaceUnconfigured: no configured tracker serves the location.
    """

    tracker_kind = _TRACKER_KIND.get(kind)
    if tracker_kind is None:
        raise ValueError(f"{kind!r} is not a tracker surface kind")
    config = registry.config
    if kind == "github":
        tracker = next((t for t in config.trackers if t.kind == tracker_kind), None)
        return SurfaceTarget(tracker, None, address)
    for binding in config.bindings:
        tracker = config.tracker(binding.tracker)
        if tracker.kind != tracker_kind:
            continue
        if kind == "jira" and binding.name == address:
            return SurfaceTarget(tracker, binding.name, address)
        if kind == "gitlab" and any(repo.path == address for repo in binding.repos):
            return SurfaceTarget(tracker, binding.name, address)
    reason = (
        "names no configured binding with a Jira Cloud tracker"
        if kind == "jira"
        else "is not a project any configured GitLab tracker binding lists"
    )
    raise SurfaceUnconfigured(kind, address, reason)


def effective_trigger_label(binding: Surface, registry: ForgeRegistry) -> str | None:
    """The label that starts factory work through ``binding``.

    Its own ``trigger_label``, else the configured label of the tracker serving
    it. None when it is not a tracker surface, or when no configured tracker
    serves it any more (its config was removed after it was bound), since then
    no label can start work through it.
    """

    if binding.trigger_label is not None:
        return binding.trigger_label
    if binding.kind not in TRACKER_SURFACE_KINDS:
        return None
    try:
        tracker = resolve_surface(registry, binding.kind, binding.address).tracker
    except SurfaceUnconfigured:
        return None
    return None if tracker is None else tracker.label


def surface_location(
    registry: ForgeRegistry, issue: TrackerIssueRef, repository_path: str
) -> tuple[str, str]:
    """The surface kind and address an issue's replies and surfaces live on.

    A repository-addressed kind (GitHub, GitLab) is located by the repository
    the issue lives in, ``repository_path``; a binding-addressed kind (Jira) by
    the config binding serving the issue's tracker scope.

    Raises:
        NotFound: the issue's kind is not a tracker surface kind (``tracker``),
            or no configured binding serves its scope (``binding``).
    """

    surface = SURFACE_KIND.get(issue.kind)
    if surface is None:
        raise NotFound("tracker")
    if _ADDRESSED_BY[surface] == "repository":
        return surface, repository_path
    from curie_api.forges.trackers import binding_for_scope

    binding = binding_for_scope(registry, issue)
    if binding is None:
        raise NotFound("binding")
    return surface, binding.name


def binding_location(binding: BindingConfig, tracker: TrackerConfig) -> tuple[str, str]:
    """The surface kind and address that serve ``binding`` on ``tracker``.

    A binding-addressed kind is addressed by the binding's name; a
    repository-addressed kind by the binding's one repository path.
    """

    surface = SURFACE_KIND[tracker.kind]
    if _ADDRESSED_BY[surface] == "binding":
        return surface, binding.name
    if len(binding.repos) != 1:
        raise ValueError(f"binding {binding.name!r} lists more than one {surface} repository")
    return surface, binding.repos[0].path


async def bound_labels(
    session: AsyncSession, registry: ForgeRegistry, kind: str, address: str
) -> dict[str, list[AgentChannel]]:
    """The ``kind`` surfaces at ``address``, grouped by effective trigger label
    (#4361 decision 14).

    A label that maps to two surfaces is a run time collision (the tracker
    label was changed to equal an explicit label): it is still one label to
    list, and admission under it refuses.
    """

    rows = await session.scalars(
        select(AgentChannel)
        .where(AgentChannel.kind == kind, AgentChannel.address == address)
        .order_by(AgentChannel.agent_id)
    )
    grouped: dict[str, list[AgentChannel]] = {}
    for row in rows:
        label = effective_trigger_label(row, registry)
        if label is not None:
            grouped.setdefault(label, []).append(row)
    return grouped


async def agent_binding(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> AgentChannel | None:
    """The agent's ``kind`` surface at ``address``, if it still holds one.

    One at most: `crud.channels.refuse_label_overlap` keeps an agent to one
    surface per location.
    """

    found: AgentChannel | None = await session.scalar(
        select(AgentChannel)
        .where(
            AgentChannel.agent_id == agent_id,
            AgentChannel.kind == kind,
            AgentChannel.address == address,
        )
        .limit(1)
    )
    return found
