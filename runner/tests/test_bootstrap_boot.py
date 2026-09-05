"""Bootstrap mode at boot (ADR-0122 via the BootEnv ``runner_bootstrap_token``).

Two activation gates, both proven by execution rather than by reading:

- the boot path selects the credential mode from the two BootEnv credentials
  exactly as the contract documents (per-claim wins; bootstrap alone is
  bootstrap mode; neither is the open legacy boot);
- the bootstrap credential is removed from the process environment before any
  child can be spawned, proven with an EXECUTING child: a real subprocess that
  reports whether it can see the key, with a positive control (a needed token
  stays visible) and a negative control (without the scrub the child does see
  the bootstrap, so the assertion is not vacuous).
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from aci_protocol import BootEnv
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.__main__ import build_app_for, retire_bootstrap_from_process_env
from curie_runner.adoption import CredentialMode
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.server import AUTHORITY
from curie_runner.session import SessionRunner

BOOTSTRAP_ENV = BootEnv.env_key("runner_bootstrap_token")
HISTORY_TOKEN_ENV = BootEnv.env_key("history_token")
_BOOT = "pool-bootstrap-credential-0123456789"

_BASE = {
    "CURIE_PLUGIN_DIR": "/unused",
    "CURIE_SESSION_ID": "warm-unbound",
    "CURIE_SANDBOX_ID": "sbx-pool-1",
    "CURIE_BUDGET": '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 5.0}',
}


def _runner() -> SessionRunner:
    return SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:warm-unbound",
        session_id="warm-unbound",
    )


def test_config_reads_the_bootstrap_token_and_keeps_it_out_of_repr() -> None:
    config = RunnerConfig.from_env(dict(_BASE, **{BOOTSTRAP_ENV: _BOOT}))
    assert config.runner_bootstrap_token == _BOOT
    assert _BOOT not in repr(config)
    assert RunnerConfig.from_env(dict(_BASE)).runner_bootstrap_token is None
    # A declared-but-empty value is a mis-rendered Secret and fails closed.
    with pytest.raises(Exception, match="runner_bootstrap_token"):
        RunnerConfig.from_env(dict(_BASE, **{BOOTSTRAP_ENV: ""}))


@pytest.mark.parametrize(
    ("env", "mode"),
    [
        ({BOOTSTRAP_ENV: _BOOT}, CredentialMode.BOOTSTRAP),
        ({BOOTSTRAP_ENV: _BOOT, "CURIE_RUNNER_TOKEN": "per-claim-token"}, CredentialMode.PER_CLAIM),
        ({"CURIE_RUNNER_TOKEN": "per-claim-token"}, CredentialMode.PER_CLAIM),
        ({}, CredentialMode.OPEN),
    ],
)
def test_boot_selects_the_credential_mode_from_boot_env(
    env: dict[str, str], mode: CredentialMode
) -> None:
    config = RunnerConfig.from_env(dict(_BASE, **env))
    app = build_app_for(config, _runner(), None)
    assert app[AUTHORITY].mode is mode
    assert app[AUTHORITY].adoptable is (mode is CredentialMode.BOOTSTRAP)


def _child_sees(key: str) -> bool:
    """Run a REAL child process and report whether ``key`` is in its environment."""

    completed = subprocess.run(
        [sys.executable, "-c", f"import os, sys; sys.exit(0 if {key!r} in os.environ else 7)"],
        check=False,
    )
    assert completed.returncode in (0, 7), completed.returncode
    return completed.returncode == 0


def test_bootstrap_is_removed_from_the_executing_child_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(BOOTSTRAP_ENV, _BOOT)
    monkeypatch.setenv(HISTORY_TOKEN_ENV, "history-token-stays")
    # Negative control: before the scrub, a child DOES inherit the bootstrap,
    # so the assertion below cannot pass by accident.
    assert _child_sees(BOOTSTRAP_ENV) is True

    assert retire_bootstrap_from_process_env(os.environ) is True

    # The bootstrap is gone from every child spawned from now on...
    assert _child_sees(BOOTSTRAP_ENV) is False
    assert BOOTSTRAP_ENV not in os.environ
    # ...while a credential the runner legitimately needs (positive control)
    # is still inherited.
    assert _child_sees(HISTORY_TOKEN_ENV) is True
    # Idempotent and honest about absence.
    assert retire_bootstrap_from_process_env(os.environ) is False


def test_mcp_capability_probe_env_cannot_see_the_bootstrap_after_the_scrub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The probe merges ``os.environ`` at call time; after the scrub it is clean."""

    monkeypatch.setenv(BOOTSTRAP_ENV, _BOOT)
    retire_bootstrap_from_process_env(os.environ)
    merged = {**os.environ, **{}}
    assert BOOTSTRAP_ENV not in merged
    assert _BOOT not in "".join(merged.values())
