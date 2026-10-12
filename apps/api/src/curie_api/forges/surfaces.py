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

from dataclasses import dataclass
from typing import Protocol

from curie_api.forges import types
from curie_api.forges.config import TrackerConfig
from curie_api.forges.registry import ForgeRegistry

# A tracker surface kind -> the tracker kind that serves it.
_TRACKER_KIND = {
    "github": types.GITHUB,
    "gitlab": types.GITLAB,
    "jira": types.JIRA_CLOUD,
}
TRACKER_SURFACE_KINDS = frozenset(_TRACKER_KIND)


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
