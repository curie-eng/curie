"""Platform credentials stay in the runner and stay out of shell and hook env."""

from __future__ import annotations

import os
import subprocess

from curie_runner.subprocess_env import (
    BASH_CREDENTIAL_PRELUDE,
    release_platform_credentials,
    shell_and_hook_env,
)


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
