"""The supported Claude harness contribution manifest (ADR 0140).

Wraps the existing side effect classification, credential resolution and bundle
compilation. Registry guards retain the canonical name and aliases, while runner
boot resolves this contribution directly.
"""

from __future__ import annotations

from ... import sdk_auth, side_effects
from ...plugin import load_bundle_system_prompt, load_plugins
from ..contribution import BundleCompileResult, HarnessContribution


def _compile_bundle(plugin_dir: str | None) -> BundleCompileResult:
    return BundleCompileResult(
        plugins=load_plugins(plugin_dir),
        system_prompt=load_bundle_system_prompt(plugin_dir),
    )


CLAUDE_CONTRIBUTION = HarnessContribution(
    name="claude",
    aliases=frozenset({"claude-sdk", "claude-code"}),
    readonly_tools=side_effects.CLAUDE_READONLY_TOOLS,
    build_spawn_env=sdk_auth.resolve_sdk_env,
    compile_bundle=_compile_bundle,
    supports_structured_replay=True,
)


def get_contribution() -> HarnessContribution:
    return CLAUDE_CONTRIBUTION
