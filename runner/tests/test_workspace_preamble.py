"""Mounted-workspace facts in the runner system prompt (#2658)."""

import re
from pathlib import Path

from curie_runner import RunnerConfig
from curie_runner.__main__ import build_runner
from curie_runner.adapter import ClaudeAgentSession


def _plugin_config(tmp_path) -> RunnerConfig:
    plugin_dir = tmp_path / ".claude-plugin"
    plugin_dir.mkdir()
    (plugin_dir / "plugin.json").write_text(
        '{"name": "workspacepreamble", "version": "0.1.0", "description": "test"}'
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-workspace",
            "CURIE_SANDBOX_ID": "b-workspace",
            "CURIE_BUDGET": (
                '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 1.0}'
            ),
            "CURIE_MODEL": "z-ai/glm-5.2",
        }
    )


def _real_session_prompt(tmp_path, **build_kw) -> str | None:
    runner = build_runner(_plugin_config(tmp_path), fake_model=False, **build_kw)
    session = runner._factory()
    assert isinstance(session, ClaudeAgentSession)
    return session._options.system_prompt


def _assert_mounted_workspace_facts(prompt: str) -> None:
    lowered = prompt.casefold()
    assert "/workspace" in prompt
    assert "already" in lowered
    assert re.search(r"do\s+not", prompt, flags=re.IGNORECASE)
    assert "clone" in lowered
    assert "unavailable" in lowered or "unreachable" in lowered


def _assert_mounted_workspace_facts_absent(prompt: str | None) -> None:
    text = prompt or ""
    lowered = text.casefold()
    assert "/workspace" not in lowered
    assert "git clone" not in lowered
    assert "github.com" not in lowered


def test_build_runner_forwards_mounted_workspace_facts_to_session_prompt(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    (workspace / ".git").mkdir(parents=True)

    prompt = _real_session_prompt(tmp_path, workspace_path=workspace)

    assert prompt is not None
    _assert_mounted_workspace_facts(prompt)
    assert str(workspace) not in prompt
    assert "Configured model: z-ai/glm-5.2" in prompt


def test_build_runner_omits_workspace_facts_without_workspace_path(tmp_path) -> None:
    prompt = _real_session_prompt(tmp_path)
    _assert_mounted_workspace_facts_absent(prompt)


def test_build_runner_omits_workspace_facts_when_workspace_has_no_git(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    prompt = _real_session_prompt(tmp_path, workspace_path=workspace)
    _assert_mounted_workspace_facts_absent(prompt)


def test_format_workspace_preamble_none_is_none() -> None:
    from curie_runner.__main__ import format_workspace_preamble

    assert format_workspace_preamble(None) is None


def test_workspace_preamble_forbids_clone_and_fetch_in_one_sentence() -> None:
    # Kills: dropping the negation, or splitting clone/fetch across sentences so
    # a neighbouring "do not push" line can stand in. Span is [^.] so it is
    # sentence-local, not a file-wide bag of words.
    from curie_runner.__main__ import format_workspace_preamble

    dummy = Path("dummy-workspace")
    preamble = format_workspace_preamble(dummy)
    assert preamble is not None
    assert re.search(
        r"(?:do\s+not|must\s+not|never)[^.]{0,120}"
        r"(?:git\s+)?clone[^.]{0,80}(?:git\s+)?fetch",
        preamble,
        flags=re.IGNORECASE,
    ), "one sentence must forbid clone and fetch; deleting it must fail this test"
    assert "/workspace" in preamble
    assert str(dummy) not in preamble


def test_workspace_preamble_forbids_a_substitute_test_runner() -> None:
    # Issue #2901: an agent with no index and no preinstalled pytest wrote its
    # own shim and treated that as the repository's checks.
    from curie_runner.__main__ import format_workspace_preamble

    preamble = format_workspace_preamble(Path("dummy-workspace"))
    assert preamble is not None
    assert re.search(
        r"do\s+not\s+write\s+a\s+substitute\s+test\s+runner\s+or\s+shim",
        preamble,
        flags=re.IGNORECASE,
    ), preamble
    assert "--no-index" in preamble
    assert "in-sandbox verification is unavailable" in preamble
    assert "Do not request publication." in preamble
    assert "only after a person publishes" in preamble
    assert "/workspace" in preamble


def test_compose_system_prompt_joins_workspace_between_memory_and_bundle() -> None:
    from curie_runner.__main__ import _compose_system_prompt

    assert (
        _compose_system_prompt(
            "BASE",
            "MEM",
            model="z-ai/glm-5.2",
            workspace_preamble="WS",
        )
        == "MEM\n\nWS\n\nBASE\n\nConfigured model: z-ai/glm-5.2"
    )
