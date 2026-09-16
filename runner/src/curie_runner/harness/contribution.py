"""The harness contribution manifest (ADR-0060).

A harness is not the ``ModelSession`` class (``adapter.py``) alone -- it is
everything the rest of the runner assumes about the engine that class drives:
what the image must install, which credential shapes it accepts, which of its
tools are safe to retry, which env vars carry a model override, and how a
mounted bundle becomes that engine's native session config. Today those facts
are scattered across ``sdk_auth.py``, ``side_effects.py``, and ``plugin.py`` as
free functions and module-level constants with no single name tying them
together. ``HarnessContribution`` is that name: one declared object per
harness, discovered via ``registry.py``.

These are plain, unfrozen dataclasses (matching ``config.py``'s
``RunnerConfig``), not a ``packages/`` contract type. Freezing this shape with
tri-language codegen is a deliberate later choice once a second harness has
exercised it, not an accident of where the file lives.
"""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class InstallSpec:
    """What the runner image must install for this harness to run."""

    packages: tuple[str, ...] = ()
    notes: str | None = None


@dataclass(frozen=True)
class AuthSpec:
    """Which credential shapes this harness's auth resolution accepts."""

    credential_env_keys: tuple[str, ...]
    oauth_token_prefix: str | None


@dataclass(frozen=True)
class BundleCompileResult:
    """A mounted bundle translated into this harness's native session config."""

    plugins: list[Any]
    system_prompt: str | None


@dataclass(frozen=True)
class HarnessContribution:
    """Everything the runner needs to know about one harness, declared once."""

    name: str
    image: str
    install: InstallSpec
    auth: AuthSpec
    readonly_tools: frozenset[str]
    model_override_env_keys: tuple[str, ...]
    build_spawn_env: Callable[[MutableMapping[str, str]], dict[str, str] | None]
    compile_bundle: Callable[[str | None], BundleCompileResult]
    # A harness must opt in only when it can consume Curie's ordered portable
    # role/content prefix. False means recovered history is refused explicitly;
    # rendering it into the system prompt is never a fallback (ADR-0119).
    supports_structured_replay: bool = False
    aliases: frozenset[str] = field(default_factory=frozenset)
    labels: frozenset[str] = field(default_factory=frozenset)
