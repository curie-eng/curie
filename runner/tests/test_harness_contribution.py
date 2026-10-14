"""The Claude harness's contribution wraps the existing modules, verbatim.

Every assertion here checks equivalence (or identity) against the module the
contribution wraps, not new behavior -- the contribution is a name for facts
that already exist in ``side_effects.py``, ``sdk_auth.py``, and ``plugin.py``.
"""

from pathlib import Path

import pytest
from curie_runner import CLAUDE_READONLY_TOOLS, load_bundle_system_prompt, load_plugins
from curie_runner.harness import get_contribution
from curie_runner.sdk_auth import (
    API_BACKEND_ENV,
    CREDENTIALS_ENV,
    UnsupportedApiBackendError,
    resolve_sdk_env,
)

_FIXTURES = Path(__file__).resolve().parents[2] / "packages/plugin-format/tests/fixtures"


def test_claude_contribution_wraps_readonly_tools() -> None:
    assert get_contribution().readonly_tools is CLAUDE_READONLY_TOOLS


def test_build_spawn_env_matches_resolve_sdk_env_plain_credential() -> None:
    build_spawn_env = get_contribution().build_spawn_env
    env_a = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}
    env_b = dict(env_a)
    assert build_spawn_env(env_a) == resolve_sdk_env(env_b)
    assert env_a == env_b


def test_build_spawn_env_matches_resolve_sdk_env_chat_completions_refusal() -> None:
    build_spawn_env = get_contribution().build_spawn_env
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER", API_BACKEND_ENV: "chat_completions"}
    with pytest.raises(UnsupportedApiBackendError):
        build_spawn_env(dict(env))
    with pytest.raises(UnsupportedApiBackendError):
        resolve_sdk_env(dict(env))


def test_compile_bundle_matches_existing_loaders() -> None:
    bundle = _FIXTURES / "valid_bundle"
    result = get_contribution().compile_bundle(str(bundle))
    assert result.plugins == load_plugins(str(bundle))
    assert result.system_prompt == load_bundle_system_prompt(str(bundle))


def test_compile_bundle_no_plugin_dir() -> None:
    result = get_contribution().compile_bundle(None)
    assert result.plugins == []
    assert result.system_prompt is None
