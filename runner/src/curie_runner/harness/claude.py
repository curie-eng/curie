"""The Claude harness's own contribution manifest (ADR-0060).

Wraps the already-landed ``side_effects.py``, ``sdk_auth.py``, and
``plugin.py`` -- this module declares no new behavior, it just names what
already exists so a second harness has something to register alongside. This
is the built-in registered under ``registry.BUILTIN_HARNESS_CANONICAL_PATHS``,
so any third-party entry point claiming the name ``"claude"`` under a
different module path is refused by the registry's guard rules.
"""

from __future__ import annotations

from .. import sdk_auth, side_effects
from ..plugin import load_bundle_system_prompt, load_plugins
from .contribution import AuthSpec, BundleCompileResult, HarnessContribution, InstallSpec


def _compile_bundle(plugin_dir: str | None) -> BundleCompileResult:
    return BundleCompileResult(
        plugins=load_plugins(plugin_dir),
        system_prompt=load_bundle_system_prompt(plugin_dir),
    )


CLAUDE_CONTRIBUTION = HarnessContribution(
    name="claude",
    aliases=frozenset({"claude-sdk", "claude-code"}),
    image="curie-runner",
    install=InstallSpec(
        packages=("claude-agent-sdk",),
        notes=(
            "includes its bundled runtime and runs as a non-root user; "
            "see runner/Dockerfile"
        ),
    ),
    auth=AuthSpec(
        credential_env_keys=sdk_auth.DEFAULT_CREDENTIAL_ENV_KEYS,
        oauth_token_prefix=sdk_auth.OAUTH_TOKEN_PREFIX,
    ),
    readonly_tools=side_effects.CLAUDE_READONLY_TOOLS,
    model_override_env_keys=(
        sdk_auth.MODEL_BASE_URL_ENV,
        sdk_auth.API_BACKEND_ENV,
        sdk_auth.MODEL_ENV_KEY_ENV,
    ),
    build_spawn_env=sdk_auth.resolve_sdk_env,
    compile_bundle=_compile_bundle,
    supports_structured_replay=True,
)


def get_contribution() -> HarnessContribution:
    return CLAUDE_CONTRIBUTION
