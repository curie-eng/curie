"""Platform credentials stay in the runner and stay out of shell and hook env."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from curie_runner.subprocess_env import (
    BASH_CREDENTIAL_PRELUDE,
    proc_dumpable_library,
    release_platform_credentials,
    shell_and_hook_env,
)


@pytest.fixture
def real_compiler() -> str:
    compiler = shutil.which("gcc")
    if compiler is None:
        raise RuntimeError("the source runner requires a working C compiler")
    return compiler


def test_source_compiler_does_not_receive_personal_credentials(
    tmp_path: Path, real_compiler: str,
) -> None:
    capture = tmp_path / "compiler-environment.json"
    wrapper = tmp_path / "gcc"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        f"pathlib.Path({str(capture)!r}).write_text(json.dumps({{\n"
        " 'grant': os.environ.get('CLAUDE_CODE_OAUTH_TOKEN'),\n"
        " 'connector': os.environ.get('ACME_CONNECTOR_VALUE'),\n"
        "}))\n"
        f"os.execv({real_compiler!r}, [{real_compiler!r}, *sys.argv[1:]])\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    grant = "synthetic-personal-compiler-sentinel"
    library = Path(proc_dumpable_library({
        "PATH": str(tmp_path) + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"),
        "CLAUDE_CODE_OAUTH_TOKEN": grant,
        "ACME_CONNECTOR_VALUE": "public-compiler-control",
    }))
    try:
        observed = capture.read_text(encoding="utf-8")
        grant_exposed = grant in observed
        assert not grant_exposed
        assert json.loads(observed) == {"grant": None, "connector": "public-compiler-control"}
        assert library.is_file()
    finally:
        library.unlink()
        library.parent.rmdir()


def test_release_drops_platform_tokens_and_keeps_the_cli_model_key() -> None:
    env = {
        "PATH": "/usr/bin",
        "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
        "CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER",
        "CURIE_RUNNER_TOKEN": "runner-sentinel",
        "CURIE_STATE_TOKEN": "sbx.example-state",
        "CURIE_MEMORY_TOKEN": "sbx.example-memory",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.example-caller",
        "CURIE_MODEL": "claude-sonnet-5",
        "STDIO_TOKEN": "stdio-secret",
    }

    release_platform_credentials(env)

    assert env["ANTHROPIC_API_KEY"] == "sk-ant-PLACEHOLDER"
    assert env["CURIE_MODEL"] == "claude-sonnet-5"
    assert env["PATH"] == "/usr/bin"
    assert env["STDIO_TOKEN"] == "stdio-secret"
    assert "CURIE_CREDENTIALS" not in env
    joined = "\n".join(f"{key}={value}" for key, value in env.items())
    assert "runner-sentinel" not in joined
    assert "sbx.example-state" not in joined
    assert "sbx.example-memory" not in joined
    assert "cct.example-caller" not in joined


def test_shell_and_hook_env_also_drops_the_model_key() -> None:
    source = {
        "PATH": "/usr/bin",
        "HOME": "/home/curie",
        "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
        "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat-PLACEHOLDER",
        "CURIE_RUNNER_TOKEN": "runner-sentinel",
        "CURIE_STATE_TOKEN": "sbx.example-state",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.example-caller",
        "CURIE_MODEL": "claude-sonnet-5",
        "STDIO_TOKEN": "stdio-secret",
    }

    child = shell_and_hook_env(source, extra={"CLAUDE_PLUGIN_ROOT": "/bundle"})
    rendered = "\n".join(f"{key}={value}" for key, value in child.items())

    assert child["PATH"] == "/usr/bin"
    assert child["HOME"] == "/home/curie"
    assert child["CURIE_MODEL"] == "claude-sonnet-5"
    assert child["STDIO_TOKEN"] == "stdio-secret"
    assert child["CLAUDE_PLUGIN_ROOT"] == "/bundle"
    assert "ANTHROPIC_API_KEY" not in child
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in child
    assert "runner-sentinel" not in rendered
    assert "sbx.example-state" not in rendered
    assert "cct.example-caller" not in rendered


@pytest.mark.parametrize("name", ("CURIE_STATE_TOKEN1", "CURIE_STATE_TOKEN_1"))
def test_indexed_credentials_leave_public_digit_values_usable(name: str) -> None:
    child = shell_and_hook_env(
        {
            name: "indexed-platform-credential-placeholder",
            "CURIE_RUNNER_ALLOW_TOKENLESS": "1",
            "ACME_PUBLIC_VALUE": "public/v1",
        }
    )
    observed = subprocess.run(
        [sys.executable, "-c", "import json,os; print(json.dumps(dict(os.environ)))"],
        env=child,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(observed.stdout)
    assert payload["ACME_PUBLIC_VALUE"] == "public/v1"
    assert name not in payload
    exposed = "indexed-platform-credential-placeholder" in observed.stdout
    assert not exposed


def test_bash_prelude_unsets_platform_credentials_before_the_command() -> None:
    assert BASH_CREDENTIAL_PRELUDE.is_file()
    env = os.environ.copy()
    env.update(
        {
            "BASH_ENV": str(BASH_CREDENTIAL_PRELUDE),
            "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
            "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat-PLACEHOLDER",
            "CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER",
            "CURIE_RUNNER_TOKEN": "runner-sentinel",
            "CURIE_STATE_TOKEN": "sbx.example-state",
            "CURIE_CONNECTOR_CALLER_TOKEN": "cct.example-caller",
            "CURIE_MODEL_ENV_KEY": "ACME_PROVIDER_KEY",
            "ACME_PROVIDER_KEY": "provider-sentinel",
            "CURIE_MODEL": "claude-sonnet-5",
            "STDIO_TOKEN": "stdio-secret",
        }
    )
    completed = subprocess.run(
        ["bash", "-c", "env"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    rendered = completed.stdout
    assert "CURIE_MODEL=claude-sonnet-5" in rendered
    assert "STDIO_TOKEN=stdio-secret" in rendered
    for sentinel in (
        "sk-ant-PLACEHOLDER",
        "sk-ant-oat-PLACEHOLDER",
        "runner-sentinel",
        "sbx.example-state",
        "cct.example-caller",
        "provider-sentinel",
    ):
        assert sentinel not in rendered


def test_an_env_override_cannot_restore_a_declared_provider_credential() -> None:
    source = {
        "PATH": "/usr/bin",
        "CURIE_MODEL_ENV_KEY": "ACME_PROVIDER_KEY",
        "ACME_PROVIDER_KEY": "provider-sentinel",
    }

    hidden = shell_and_hook_env(source, extra={"CURIE_MODEL_ENV_KEY": ""})
    renamed = shell_and_hook_env(source, extra={"CURIE_MODEL_ENV_KEY": "OTHER_KEY"})

    assert "ACME_PROVIDER_KEY" not in hidden
    assert "provider-sentinel" not in hidden.values()
    assert "ACME_PROVIDER_KEY" not in renamed
    assert "provider-sentinel" not in renamed.values()


def test_a_declared_provider_credential_name_is_not_a_shell_variable() -> None:
    source = {
        "PATH": "/usr/bin",
        "CURIE_MODEL_ENV_KEY": "ACME_PROVIDER_KEY",
        "ACME_PROVIDER_KEY": "provider-sentinel",
        "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
    }

    child = shell_and_hook_env(source)

    assert "ACME_PROVIDER_KEY" not in child
    assert "provider-sentinel" not in "\n".join(child.values())
    assert child["CURIE_MODEL_ENV_KEY"] == "ACME_PROVIDER_KEY"
    assert child["PATH"] == "/usr/bin"


@pytest.mark.parametrize("launcher", [False, True])
def test_bundle_shell_can_read_upload_file_while_platform_tokens_stay_hidden(
    tmp_path: Path, launcher: bool
) -> None:
    path = tmp_path / "uploads.json"
    path.write_text('{"reviewer":"ccu.test.signature"}')
    path.chmod(0o400)
    source = {
        "PATH": os.environ["PATH"],
        "CURIE_CONNECTOR_UPLOAD_TOKENS_FILE": str(path),
        "CURIE_RUNNER_TOKEN": "runner-sentinel",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.test.signature",
        "CURIE_CONNECTOR_UPLOAD_TOKENS": "raw-token-sentinel",
        "CURIE_OTHER_TOKEN_FILE": "other-token-sentinel",
        "BASH_ENV": str(BASH_CREDENTIAL_PRELUDE),
    }
    command = (
        'test -z "${CURIE_RUNNER_TOKEN-}${CURIE_CONNECTOR_CALLER_TOKEN-}'
        '${CURIE_CONNECTOR_UPLOAD_TOKENS-}${CURIE_OTHER_TOKEN_FILE-}" '
        '&& cat "$CURIE_CONNECTOR_UPLOAD_TOKENS_FILE"'
    )
    if launcher:
        argv = [sys.executable, "-I", "-m", "curie_runner.shell_launcher", "-c", command]
        env = source
    else:
        release_platform_credentials(source)
        argv = ["bash", "-c", command]
        env = shell_and_hook_env(source)
    completed = subprocess.run(argv, env=env, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == path.read_text()


class _EnvSnapshotSession:
    """An SDK session stand-in that records what a hook, a shell and Bash would see.

    ``query`` runs while the turn is live, after the runner has taken the
    turn's memory credential, so its snapshots are the env any subprocess
    spawned during that turn would inherit.
    """

    snapshots: list[dict[str, str]] = []

    def __init__(self, memory_turn: object = None) -> None:
        self.bash_env: str = ""
        self.memory_turn = memory_turn
        self.write_token_during_turn: str | None = None

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        from curie_runner.subprocess_env import cli_parent_env

        if self.memory_turn is not None:
            self.write_token_during_turn = self.memory_turn.write_token  # type: ignore[attr-defined]
        type(self).snapshots = [
            dict(os.environ),
            shell_and_hook_env(os.environ, extra={"CLAUDE_PLUGIN_ROOT": "/bundle"}),
            cli_parent_env(os.environ),
        ]
        env = cli_parent_env(os.environ)
        completed = subprocess.run(
            ["bash", "-c", "env"], check=True, capture_output=True, text=True, env=env
        )
        self.bash_env = completed.stdout

    async def receive_turn(self):  # type: ignore[no-untyped-def]
        if False:  # pragma: no cover - retain the async-generator shape
            yield None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None


def test_turn_memory_token_never_reaches_env_hooks_or_bash() -> None:
    # ADR-0188 decision 5 / MEMORY-TOKEN-3: the per-turn memory credential
    # rides the ACI Event into the runner's MemoryTurn and nowhere else -- not
    # os.environ, not the hook/shell env, not the CLI parent env, not Bash.
    import anyio
    from aci_protocol import Event
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.memory_facts import MemoryTurn
    from curie_runner.session import SessionRunner

    token = "sbx.turn-memory-sentinel.sig"
    memory_turn = MemoryTurn()
    session = _EnvSnapshotSession(memory_turn)
    runner = SessionRunner(
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        max_usd_per_day=1.0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        memory_turn=memory_turn,
    )

    async def go() -> None:
        await runner.start()
        try:
            event = Event(type="message", text="hi", user="U123", ts="1", memory_token=token)
            async for _line in runner.run_turn(event):
                pass
        finally:
            await runner.close()

    anyio.run(go)

    # While the turn ran, the runner held the credential for its tools (so the
    # checks below are not vacuous)...
    assert session.write_token_during_turn == token
    # ...and no env a subprocess spawned at that moment would get carries it,
    # under any name.
    assert len(_EnvSnapshotSession.snapshots) == 3
    for env in _EnvSnapshotSession.snapshots:
        assert token not in "\n".join(f"{k}={v}" for k, v in env.items())
    assert session.bash_env, "bash never ran"
    assert token not in session.bash_env
    assert token not in "\n".join(f"{k}={v}" for k, v in os.environ.items())
