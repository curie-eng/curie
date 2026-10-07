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
            "CURIE_BUDGET": ('{"max_output_tokens_per_run": 1000, "max_usd_per_day": 1.0}'),
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
    assert "No factory verification preflight result is available" in preamble
    assert "Do not claim that the check passed" in preamble
    assert "matching required check" in preamble
    assert "/workspace" in preamble
    assert "Verification command:" not in preamble
    assert "pytest" not in preamble


# --- declared verification checks (#3521) ------------------------------------------

_PYTHON_COMMAND = "uv run pytest unitconv/tests -q"
_RUST_COMMAND = "cargo test --locked"


def _check(
    check_id: str,
    paths: list[str],
    command: str,
    *,
    outcome: str = "passed",
    delegated_to: str | None = None,
) -> dict[str, object]:
    return {
        "id": check_id,
        "paths": paths,
        "command": command,
        "installed": False,
        "outcome": outcome,
        "exit_status": 0 if outcome == "passed" else None,
        "missing_binaries": [] if outcome == "passed" else ["uv"],
        "blocked_services": [],
        "report_status": 201,
        "delegated_to": delegated_to,
    }


def _verification(
    *checks: dict[str, object],
    source: str | None = "bundle",
    lockfile_installs: bool = False,
    unreadable: str | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "source": source if checks else None,
        "lockfile_installs": lockfile_installs,
        "unreadable": unreadable,
        "checks": list(checks),
    }
    if not checks:
        result["report_status"] = 201
    return result


_PYTHON = _check("python", ["**/*.py", "pyproject.toml"], _PYTHON_COMMAND)
_RUST = _check("rust", ["**/*.rs", "Cargo.lock"], _RUST_COMMAND)


def _preamble(verification: dict[str, object]) -> str:
    from curie_runner.__main__ import format_workspace_preamble

    preamble = format_workspace_preamble(Path("dummy-workspace"), verification)
    assert preamble is not None
    return preamble


def _check_line(preamble: str, check_id: str, command: str) -> str:
    lines = [line for line in preamble.splitlines() if check_id in line and command in line]
    assert lines, preamble
    return "\n".join(lines)


def test_preamble_with_no_declared_check_says_so_and_names_no_suite() -> None:
    preamble = _preamble(_verification())

    assert "No verification check was declared" in preamble
    assert re.search(r"do\s+not\s+invent\s+a\s+check", preamble, flags=re.IGNORECASE)
    assert "pytest" not in preamble
    assert "Verification command:" not in preamble
    assert "Run only the repository's documented focused check command" not in preamble


def test_preamble_names_an_unreadable_repository_declaration() -> None:
    preamble = _preamble(_verification(unreadable=".curie/verification.json is not valid JSON"))

    assert "No verification check was declared" in preamble
    assert "unreadable" in preamble.casefold()
    assert ".curie/verification.json" in preamble


def test_preamble_lists_each_declared_check_with_its_paths_and_command() -> None:
    preamble = _preamble(_verification(_PYTHON, _RUST))

    python_line = _check_line(preamble, "python", _PYTHON_COMMAND)
    assert "**/*.py" in python_line
    assert "pyproject.toml" in python_line
    rust_line = _check_line(preamble, "rust", _RUST_COMMAND)
    assert "**/*.rs" in rust_line
    assert "Cargo.lock" in rust_line
    assert "from the bundle" in preamble
    assert re.search(
        r"run\s+only\s+the\s+check\s+whose\s+paths\s+match",
        preamble,
        flags=re.IGNORECASE,
    ), preamble
    assert "no check was declared for that area" in preamble
    assert "Verification command:" not in preamble


def test_preamble_names_the_repository_as_the_declaring_source() -> None:
    preamble = _preamble(_verification(_RUST, source="repository"))

    assert "from the repository" in preamble
    _check_line(preamble, "rust", _RUST_COMMAND)


def test_python_only_declaration_does_not_present_python_as_the_check_for_rust() -> None:
    preamble = _preamble(_verification(_PYTHON))

    _check_line(preamble, "python", _PYTHON_COMMAND)
    assert "no check was declared for that area" in preamble
    assert re.search(r"do\s+not\s+run\s+an\s+unrelated\s+check", preamble, flags=re.IGNORECASE), (
        preamble
    )
    assert "Run only the repository's documented focused check command" not in preamble
    assert "Verification command:" not in preamble


def test_unavailable_check_result_is_scoped_to_that_check() -> None:
    # #3873: an unavailable check that reaches the prompt is always delegated;
    # an undelegated one stops the run before the model.
    unavailable = _check(
        "python",
        ["**/*.py"],
        _PYTHON_COMMAND,
        outcome="unavailable",
        delegated_to="unit-tests",
    )

    preamble = _preamble(_verification(unavailable, _RUST))

    assert "Missing binaries: uv" in preamble
    assert "in-sandbox verification is unavailable" in preamble
    python_line = _check_line(preamble, "python", _PYTHON_COMMAND)
    assert "unavailable" in python_line
    assert "unavailable" not in _check_line(preamble, "rust", _RUST_COMMAND)
    instruction = next(line for line in preamble.splitlines() if line.startswith("- Check python:"))
    assert "delegat" in instruction.casefold()
    assert "CI is pending proof" in instruction
    assert "do not publish and the work item cannot succeed" not in preamble
    assert "If no matching required check exists" not in preamble
    fenced = re.search(r"```json\n(.*?)\n```", preamble, flags=re.DOTALL)
    assert fenced is not None
    outside = preamble[: fenced.start()] + preamble[fenced.end() :]
    assert '"delegated_to":"unit-tests"' in fenced.group(1)
    assert "unit-tests" not in outside


def test_check_data_carries_delegated_to_only_when_set() -> None:
    import json

    from curie_runner.__main__ import _format_check_data

    delegated = json.loads(
        _format_check_data(
            _check(
                "python",
                ["**/*.py"],
                _PYTHON_COMMAND,
                outcome="unavailable",
                delegated_to="unit-tests",
            )
        )
    )
    undelegated = json.loads(_format_check_data(_RUST))

    assert delegated["delegated_to"] == "unit-tests"
    assert "delegated_to" not in undelegated


def test_lockfile_installs_allow_only_the_declared_install_commands() -> None:
    preamble = _preamble(_verification(_PYTHON, lockfile_installs=True))

    assert (
        "Only the declared lockfile-pinned install commands may contact a package registry"
        in preamble
    )


def test_without_lockfile_installs_the_no_index_rule_stays() -> None:
    preamble = _preamble(_verification(_PYTHON))

    assert "--no-index" in preamble
    assert "lockfile-pinned install commands may contact" not in preamble


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


def test_declared_checks_are_rendered_as_fenced_data() -> None:
    preamble = _preamble(_verification(_PYTHON, _RUST))

    fenced = re.search(r"```json\n(.*?)\n```", preamble, flags=re.DOTALL)
    assert fenced is not None, preamble
    block = fenced.group(1)
    assert _PYTHON_COMMAND in block and _RUST_COMMAND in block
    assert "**/*.rs" in block
    before = preamble[: fenced.start()]
    assert re.search(r"data,?\s+not\s+instructions", before, flags=re.IGNORECASE)
