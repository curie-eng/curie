"""Entry-point discovery for harness contributions (ADR-0060).

Modeled on Databricks' Omnigent (Apache 2.0): a harness registers a Python
entry point under ``ENTRY_POINT_GROUP`` whose value points at a
``get_contribution() -> HarnessContribution`` callable. The first two
fail-closed guard rules are ported from Omnigent verbatim, both evaluated on
entry-point *metadata* -- before ``ep.load()`` ever executes a line of the
plugin's code, so an adversarial or merely-broken plugin cannot forge its way
past a check by being expensive or side-effecting to import:

1. **Flat package path refusal.** A registered ``value`` must dot into a real
   package (``curie_runner.harness.claude:get_contribution``), not a bare
   top-level module (``claude:get_contribution``). A flat path is exactly the
   shape most likely to collide with something else importable on the path,
   and it is a namespacing smell a legitimate harness package never has a
   reason to produce.
2. **Built-in name collision refusal, on the metadata key.** A registered
   *key* that matches a name in ``BUILTIN_HARNESS_CANONICAL_PATHS`` (which
   covers the built-in's aliases, not just its declared name) is refused
   unless its ``value`` is that name's own canonical path.

The metadata key is only what a plugin *advertises*, though: the keys it
actually claims are the ``name`` and ``aliases`` of the contribution it
returns. So two further rules run after ``ep.load()``, against the keys the
loaded contribution really claims, still attributing the offending ``ep``:

3. **Built-in name collision refusal, on every claimed key.** Each of
   ``contribution.name`` and every entry in ``contribution.aliases`` is
   checked against ``BUILTIN_HARNESS_CANONICAL_PATHS`` under the originating
   ``ep.value``, so a plugin registered as ``evil`` cannot capture ``claude``
   by declaring it as an alias.
4. **Duplicate key refusal.** A key already claimed by an *earlier
   contribution* is refused, so two plugins colliding on one name are not
   silently resolved by distribution scan order. Keys are deduplicated within
   a single contribution first, so a manifest listing its own name among its
   aliases is an authoring quirk, not a boot failure.

Both post-load rules are dict lookups, so they are only as trustworthy as the
key's type. Every claimed key is therefore refused up front unless it is
exactly a ``str``: an object that hashes like ``"claude"`` while controlling
its own ``__eq__`` would otherwise pass both checks and still be handed back
by ``resolve_harness("claude")``.

Silently shadowing the Claude harness is the single worst failure this
registry could produce, so an ambiguous registration is refused rather than
resolved by load order. Every refusal is fail-closed by design: an ambiguous
or malformed registry entry is worse than a missing harness.
"""

from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import EntryPoint
from importlib.metadata import entry_points as _iter_entry_points

from .contribution import HarnessContribution

ENTRY_POINT_GROUP = "curie.harness"

# Canonical "module:attr" path for each built-in harness's own self-registration,
# keyed by every name that harness claims -- its declared name and each of its
# aliases, since an alias is just as load-bearing a key as the name (ADR-0060
# documents `executor: harness: claude-sdk` as a real declarative form).
# A third-party entry point may not claim one of these keys under any other path.
_CLAUDE_CANONICAL_PATH = "curie_runner.harness.claude:get_contribution"
BUILTIN_HARNESS_CANONICAL_PATHS: dict[str, str] = {
    "claude": _CLAUDE_CANONICAL_PATH,
    "claude-sdk": _CLAUDE_CANONICAL_PATH,
    "claude-code": _CLAUDE_CANONICAL_PATH,
}

# The harness selected when none is named (``CURIE_HARNESS`` unset): the
# built-in Claude harness. Single source shared by the runner config and the
# boot path so the default name is declared exactly once.
DEFAULT_HARNESS = "claude"


class FlatHarnessPackageError(RuntimeError):
    """Raised when a harness entry point registers a flat (unpackaged) module path."""


class HarnessNameCollisionError(RuntimeError):
    """Raised when a harness entry point claims a key that is already spoken for.

    Covers both a key belonging to a built-in harness (its declared name or one
    of its aliases) and a key another contribution has already registered.
    """


class MalformedHarnessContributionError(RuntimeError):
    """Raised when a loaded contribution declares a key that is not a plain ``str``.

    ``HarnessContribution.name`` and ``aliases`` are only annotated, never
    enforced, and every guard here is a dict lookup. An object that hashes like
    a built-in's name while controlling its own ``__eq__`` would slip past those
    lookups and still be returned by ``resolve_harness``, so a claimed key that
    is not exactly a ``str`` is refused before it is looked up or stored.
    """


class UnknownHarnessError(RuntimeError):
    """Raised when a requested harness name (or alias) is not registered."""


class UnsupportedHarnessError(RuntimeError):
    """Raised when runner boot selects a harness Curie does not support."""


def _check_guards(ep: EntryPoint) -> None:
    module_path = ep.value.split(":", 1)[0]
    if "." not in module_path:
        raise FlatHarnessPackageError(
            f"harness entry point {ep.name!r} registers a flat package path "
            f"{ep.value!r}; a harness must live in a real package, not a "
            "top-level module."
        )
    _check_builtin_key(ep, ep.name)


def _ep_label(ep: EntryPoint) -> str:
    """Identify an entry point in an operator-facing error message."""
    return f"{ep.name!r} ({ep.value!r})"


def _check_key_type(ep: EntryPoint, key: object) -> None:
    # ``type(key) is not str`` rather than ``isinstance``: a ``str`` subclass can
    # override ``__eq__``/``__hash__`` and so still steer the dict lookups below.
    if type(key) is not str:
        raise MalformedHarnessContributionError(
            f"harness entry point {_ep_label(ep)} claims key "
            f"{key!r} of type {type(key).__name__!r}; a harness key must be a "
            "plain str."
        )


def _check_builtin_key(ep: EntryPoint, key: str) -> None:
    canonical = BUILTIN_HARNESS_CANONICAL_PATHS.get(key)
    if canonical is not None and ep.value != canonical:
        raise HarnessNameCollisionError(
            f"harness entry point {ep.name!r} claims built-in harness name "
            f"{key!r} but registers {ep.value!r}, not the built-in's canonical "
            f"path {canonical!r}."
        )


def discover_contributions(
    *, entry_points: Iterable[EntryPoint] | None = None
) -> dict[str, HarnessContribution]:
    """Discover every registered harness, keyed by its declared name and aliases.

    Guard failures raise rather than silently skipping the offending entry
    point: a malformed or colliding registration is a configuration error the
    operator must fix, not a harness that quietly fails to appear.
    """

    eps = (
        entry_points
        if entry_points is not None
        else _iter_entry_points(group=ENTRY_POINT_GROUP)
    )
    contributions: dict[str, HarnessContribution] = {}
    claimed_by: dict[str, str] = {}
    for ep in eps:
        _check_guards(ep)
        get_contribution = ep.load()
        contribution = get_contribution()
        # Snapshot every claimed key exactly once, before any of them is typed,
        # sorted, hashed, or stored. ``name``/``aliases`` are unenforced
        # attributes on a plugin-supplied object, so re-reading them would let an
        # ``aliases`` that yields plain strings on its first iteration and a
        # hostile ``__hash__``/``__eq__`` key on its second pass the type guard
        # and still be registered. Nothing below may re-read the contribution.
        claimed_name = contribution.name
        claimed_aliases = tuple(contribution.aliases)
        _check_key_type(ep, claimed_name)
        for alias in claimed_aliases:
            _check_key_type(ep, alias)
        # sorted() is load bearing, not cosmetic: frozenset iteration is hash
        # ordered, so without it *which* colliding key gets reported would vary
        # run to run -- exactly the load-order nondeterminism this module exists
        # to remove. It runs after the type checks above, since sorting a
        # sequence holding a non-str would raise its own unrelated TypeError.
        # dict.fromkeys dedupes within one contribution, so a manifest
        # that lists its own name among its aliases is not a self-collision;
        # duplicates are refused only across contributions.
        for key in dict.fromkeys((claimed_name, *sorted(claimed_aliases))):
            _check_builtin_key(ep, key)
            owner = claimed_by.get(key)
            if owner is not None:
                raise HarnessNameCollisionError(
                    f"harness entry point {_ep_label(ep)} claims "
                    f"{key!r}, which is already registered by {owner}."
                )
            contributions[key] = contribution
            claimed_by[key] = _ep_label(ep)
    return contributions


def resolve_harness(
    name: str, *, contributions: dict[str, HarnessContribution] | None = None
) -> HarnessContribution:
    """Resolve a harness by its declared name or one of its aliases."""

    available = contributions if contributions is not None else discover_contributions()
    try:
        return available[name]
    except KeyError:
        raise UnknownHarnessError(
            f"no harness registered under {name!r} (known: {sorted(available)})"
        ) from None
