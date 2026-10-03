"""The active harness contribution manifest (ADR 0140).

The runner ships one supported Claude harness. This local dataclass declares
the hooks it consumes at boot and the identity used by the registry. A concrete
second harness can shape the interface if one is added.
"""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class BundleCompileResult:
    """A mounted bundle translated into this harness's native session config."""

    plugins: list[Any]
    system_prompt: str | None


@dataclass(frozen=True)
class HarnessContribution:
    """The active hooks and capabilities consumed by the current runner."""

    name: str
    readonly_tools: frozenset[str]
    build_spawn_env: Callable[[MutableMapping[str, str]], dict[str, str] | None]
    compile_bundle: Callable[[str | None], BundleCompileResult]
    # A harness must opt in only when it can consume Curie's ordered portable
    # role/content prefix. False means recovered history is refused explicitly;
    # rendering it into the system prompt is never a fallback (ADR-0119).
    supports_structured_replay: bool = False
    aliases: frozenset[str] = field(default_factory=frozenset)
