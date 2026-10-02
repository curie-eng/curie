"""The trusted shell boundary precedes every untrusted Bash startup file.

The installed SDK0.2.159 bundles CLI2.1.281. Its supported
CLAUDE_CODE_SHELL override must execute the platform launcher before snapshots;
see https://code.claude.com/docs/en/env-vars and test_shell_env_real_cli.py.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from curie_runner import subprocess_env


def test_sdk_shell_env_pins_trusted_launcher_and_interpreter() -> None:
    values = subprocess_env.sdk_shell_env()
    launcher = Path(values["CLAUDE_CODE_SHELL"])
    assert launcher == Path(subprocess_env.__file__).with_name("curie_bash.sh")
    assert launcher.is_file() and os.access(launcher, os.X_OK)
    assert values["CURIE_SHELL_PYTHON"] == sys.executable


@pytest.mark.parametrize("exists", [False, True])
def test_sdk_shell_env_rejects_missing_or_nonexecutable_launcher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
) -> None:
    launcher = tmp_path / "unavailable-shell"
    if exists:
        launcher.write_text("#!/bin/sh\nexit 0\n")
        launcher.chmod(0o600)
    monkeypatch.setattr(subprocess_env, "BASH_SHELL_LAUNCHER", launcher, raising=False)
    with pytest.raises((FileNotFoundError, PermissionError, RuntimeError)):
        subprocess_env.sdk_shell_env()


def test_cli_parent_cannot_take_shell_authority_from_input_env() -> None:
    parent = subprocess_env.cli_parent_env(
        {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
            "CLAUDE_CODE_SHELL": "/workspace/untrusted-shell",
            "CURIE_SHELL_PYTHON": "/workspace/untrusted-python",
        }
    )
    assert parent["CLAUDE_CODE_SHELL"] == str(
        Path(subprocess_env.__file__).with_name("curie_bash.sh")
    )
    assert parent["CURIE_SHELL_PYTHON"] == sys.executable
    assert parent["ANTHROPIC_API_KEY"] == "sk-ant-PLACEHOLDER"


def test_installed_launcher_scrubs_before_startup_and_ignores_python_injection(
    tmp_path: Path,
) -> None:
    launch = subprocess_env.sdk_shell_env()
    poison = tmp_path / "sitecustomize.py"
    poison.write_text(
        "import os\n"
        "open(os.environ['ACME_POISON_LOG'],'w').write("
        "os.environ.get('ANTHROPIC_API_KEY','absent'))\n"
    )
    (tmp_path / "curie_runner").mkdir()
    (tmp_path / "curie_runner/__init__.py").write_text("")
    (tmp_path / "curie_runner/shell_launcher.py").write_text(
        "raise RuntimeError('untrusted CWD package executed')\n"
    )
    startup = tmp_path / "startup.sh"
    startup.write_text(
        'env > "$ACME_STARTUP_LOG"\nexport ACME_STARTUP_MARKER=read-before-command\n'
    )
    source = (
        os.environ
        | launch
        | {
            "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
            "CURIE_RUNNER_TOKEN": "runner-sentinel",
            "CURIE_STATE_TOKEN": "sbx.example-state",
            "CURIE_MODEL_ENV_KEY": "ACME_PROVIDER_KEY",
            "ACME_PROVIDER_KEY": "provider-sentinel",
            "STDIO_TOKEN": "connector-sentinel",
            "PYTHONPATH": str(tmp_path),
            "BASH_ENV": str(startup),
            "ACME_POISON_LOG": str(tmp_path / "poison.log"),
            "ACME_STARTUP_LOG": str(tmp_path / "startup.log"),
        }
    )
    result = subprocess.run(
        [
            launch["CLAUDE_CODE_SHELL"],
            "-c",
            'printf "%s\\n" "$ACME_STARTUP_MARKER" "$0" "$1"; env; bash -c env',
            "acme",
            "argument with spaces",
        ],
        env=source,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "read-before-command\nacme\nargument with spaces\n" in result.stdout
    assert "STDIO_TOKEN=connector-sentinel" in result.stdout
    startup_text = (tmp_path / "startup.log").read_text()
    assert "STDIO_TOKEN=connector-sentinel" in startup_text
    assert not (tmp_path / "poison.log").exists()
    for sentinel in (
        "sk-ant-PLACEHOLDER",
        "runner-sentinel",
        "sbx.example-state",
        "provider-sentinel",
    ):
        assert sentinel not in result.stdout + startup_text
    exit_result = subprocess.run(
        [launch["CLAUDE_CODE_SHELL"], "-c", "exit 37"], env=source, capture_output=True, timeout=15
    )
    assert exit_result.returncode == 37
    signal_result = subprocess.run(
        [launch["CLAUDE_CODE_SHELL"], "-c", "kill -TERM $$"],
        env=source,
        capture_output=True,
        timeout=15,
    )
    assert signal_result.returncode == -signal.SIGTERM


@pytest.mark.parametrize("missing", [False, True])
def test_sdk_connect_pins_options_and_fails_closed_before_external_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: bool,
) -> None:
    import anyio
    from claude_agent_sdk import ClaudeSDKClient
    from curie_runner.adapter import ClaudeAgentSession, build_options

    captured: list[dict[str, str]] = []

    async def connect(client: ClaudeSDKClient) -> None:
        captured.append(dict(options.env))

    monkeypatch.setattr(ClaudeSDKClient, "connect", connect)
    options = build_options(
        plugins=[],
        model="claude-sonnet-5",
        system_prompt="hello",
        max_turns=1,
        max_budget_usd=None,
        resume=None,
        env={
            "CLAUDE_CODE_SHELL": "/workspace/untrusted-shell",
            "CURIE_SHELL_PYTHON": "/workspace/untrusted-python",
        },
    )
    if missing:
        monkeypatch.setattr(
            subprocess_env, "BASH_SHELL_LAUNCHER", tmp_path / "missing", raising=False
        )
        with pytest.raises((FileNotFoundError, PermissionError, RuntimeError)):
            anyio.run(ClaudeAgentSession(options).connect)
        assert not captured
    else:
        anyio.run(ClaudeAgentSession(options).connect)
        assert len(captured) == 1
        assert captured[0]["CLAUDE_CODE_SHELL"] == str(
            Path(subprocess_env.__file__).with_name("curie_bash.sh")
        )
        assert captured[0]["CURIE_SHELL_PYTHON"] == sys.executable


@pytest.mark.parametrize("mode", ["broken", "timeout"])
def test_executable_but_unusable_shell_fails_closed_before_sdk_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    import time

    import anyio
    from claude_agent_sdk import ClaudeSDKClient
    from curie_runner.adapter import ClaudeAgentSession, build_options

    launcher = tmp_path / "unusable-shell"
    launcher.write_text(
        "#!/bin/sh\nprintf 'arbitrary-child-sentinel' >&2\nexit 37\n"
        if mode == "broken"
        else "#!/bin/sh\nexec sleep 30\n"
    )
    launcher.chmod(0o755)
    monkeypatch.setattr(subprocess_env, "BASH_SHELL_LAUNCHER", launcher)
    calls: list[bool] = []

    async def connect(client: ClaudeSDKClient) -> None:
        calls.append(True)

    monkeypatch.setattr(ClaudeSDKClient, "connect", connect)
    options = build_options(
        plugins=[],
        model="claude-sonnet-5",
        system_prompt="hello",
        max_turns=1,
        max_budget_usd=None,
        resume=None,
        env={},
    )
    started = time.monotonic()
    with pytest.raises((RuntimeError, OSError)) as failure:
        anyio.run(ClaudeAgentSession(options).connect)
    assert time.monotonic() - started < 10, "shell validation must be bounded before SDK spawn"
    assert not calls, "SDK would silently fall back to an unguarded shell"
    assert "arbitrary-child-sentinel" not in str(failure.value)
