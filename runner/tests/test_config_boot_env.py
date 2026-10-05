"""RunnerConfig consumes the declared BootEnv, with today's exact semantics.

#488 makes ``BootEnv.from_env`` the runner's single parse of the boot env and
deletes ``RunnerConfig.from_env``'s bare ``CURIE_*`` literals. The config it
produces from a given env must not change, so this module pins the parse
per-knob against TODAY's behavior.

The parse tolerance is deliberately NON-uniform and each knob keeps what it has:
``CURIE_MAX_TURNS`` and ``CURIE_RUNNER_PORT`` RAISE on garbage (a bare
``int()`` today), while the history-window knobs DEGRADE to their default on
garbage and on a nonpositive value. Unifying them would be a behavior change
wearing a consistency costume, so the asymmetry is pinned on both sides.

The table below also pins the deliberate production defaults and aliases that
predate #488 (harness default, history_ref independence, token blank-is-unset).
"""

from __future__ import annotations

import dataclasses

import pytest
from curie_runner import RunnerConfig
from pydantic import ValidationError

_BASE = {
    "CURIE_PLUGIN_DIR": "/bundle",
    "CURIE_SESSION_ID": "sess-1",
    "CURIE_SANDBOX_ID": "sbx-1",
    "CURIE_BUDGET": '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 5.0}',
}


def _field_names() -> set[str]:
    return {f.name for f in dataclasses.fields(RunnerConfig)}


def test_full_boot_env_parses_to_the_same_config() -> None:
    """Every knob a real bound claim sets, read back off one config."""

    env = dict(
        _BASE,
        CURIE_MEMORY_REF="http://api/agents/a/state/memory",
        CURIE_CREDENTIALS="cred-1",
        CURIE_HISTORY_REF="http://api/agents/a/state/transcript/t",
        CURIE_RUNNER_TOKEN="tok-1",
        CURIE_MODEL="agent-pinned",
        CURIE_APPROVAL_REQUIRED_TOOLS="Bash, Write ,",
        CURIE_APPROVAL_GRANT_TOOL="  Bash  ",
        CURIE_APPROVAL_RESUMED_KIND="  policy  ",
        CURIE_MAX_TURNS="7",
        CURIE_RUNNER_PORT="9090",
    )

    config = RunnerConfig.from_env(env)

    assert config.session.plugin_dir == "/bundle"
    assert config.session.session_id == "sess-1"
    assert config.session.sandbox_id == "sbx-1"
    assert config.session.memory_ref == "http://api/agents/a/state/memory"
    assert config.session.credentials_ref == "cred-1"
    assert config.ceiling == 1000
    assert config.max_usd_per_day == 5.0
    assert config.history_ref == "http://api/agents/a/state/transcript/t"
    assert config.runner_token == "tok-1"
    assert config.model == "agent-pinned"
    # Comma-joined names: stripped, blanks dropped.
    assert config.approval_required_tools == ["Bash", "Write"]
    # The approval markers DO strip today; the token/ref knobs do not.
    assert config.approval_grant_tool == "Bash"
    assert config.approval_resumed_kind == "policy"
    assert config.max_turns == 7
    assert config.port == 9090


# (env overrides on _BASE, RunnerConfig attribute, expected value). Each row is
# a deliberate production default or alias; changing one is a behavior change.
_KNOB_TABLE = [
    pytest.param({}, "ceiling", 1000, id="budget-ceiling"),
    pytest.param({}, "max_usd_per_day", 5.0, id="budget-usd-per-day"),
    pytest.param({}, "max_turns", 20, id="max-turns-default"),
    pytest.param({}, "port", 8080, id="port-default"),
    pytest.param({}, "history_ref", None, id="history-ref-default"),
    pytest.param({}, "approval_required_tools", None, id="approval-tools-default"),
    pytest.param({}, "approval_grant_tool", None, id="approval-grant-default"),
    pytest.param({}, "approval_resumed_kind", None, id="approval-resumed-default"),
    # CURIE_HARNESS is a runner-local knob; unset or blank selects built-in Claude.
    pytest.param({}, "harness", "claude", id="harness-default-claude"),
    pytest.param({"CURIE_HARNESS": "claude-code"}, "harness", "claude-code", id="harness-read"),
    pytest.param({"CURIE_HARNESS": "  opencode "}, "harness", "opencode", id="harness-stripped"),
    pytest.param({"CURIE_HARNESS": "   "}, "harness", "claude", id="harness-blank-falls-back"),
    # A memory ref is an externalized-memory pointer, not an SDK resume id, so it
    # must not become the rehydrate ref.
    pytest.param(
        {"CURIE_MEMORY_REF": "s3://mem/thread"}, "history_ref", None, id="history-ref-not-memory"
    ),
    pytest.param(
        {"CURIE_MEMORY_REF": "s3://mem", "CURIE_HISTORY_REF": "s3://hist"},
        "history_ref",
        "s3://hist",
        id="history-ref-explicit-wins",
    ),
    pytest.param({"CURIE_RUNNER_TOKEN": "abc123"}, "runner_token", "abc123", id="token-read"),
    pytest.param({}, "runner_token", None, id="token-absent"),
    # An empty value must read as unset rather than as an unusable token (#63);
    # the process entrypoint then refuses to boot on it (#3821), not the parse.
    pytest.param({"CURIE_RUNNER_TOKEN": ""}, "runner_token", None, id="token-empty-is-unset"),
    # CURIE_RUNNER_ALLOW_TOKENLESS is a runner-local dev knob (#3821), not a
    # BootEnv key. Only 1/true (any case, trimmed) turns it on: unlike
    # CURIE_FAKE_MODEL, "yes" is off, so nobody harmonizes the two later.
    pytest.param({}, "allow_tokenless", False, id="allow-tokenless-default-off"),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": "1"}, "allow_tokenless", True, id="allow-tokenless-1"
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": "true"}, "allow_tokenless", True, id="allow-tokenless-true"
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": " TRUE "},
        "allow_tokenless",
        True,
        id="allow-tokenless-true-any-case-trimmed",
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": "0"}, "allow_tokenless", False, id="allow-tokenless-0"
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": "yes"}, "allow_tokenless", False, id="allow-tokenless-yes"
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": "false"},
        "allow_tokenless",
        False,
        id="allow-tokenless-false",
    ),
    pytest.param(
        {"CURIE_RUNNER_ALLOW_TOKENLESS": ""}, "allow_tokenless", False, id="allow-tokenless-empty"
    ),
]


@pytest.mark.parametrize(("overrides", "attr", "expected"), _KNOB_TABLE)
def test_boot_env_knob_defaults_and_aliases(
    overrides: dict[str, str], attr: str, expected: object
) -> None:
    assert getattr(RunnerConfig.from_env(dict(_BASE, **overrides)), attr) == expected


def test_approval_markers_blank_is_unset() -> None:
    env = dict(_BASE, CURIE_APPROVAL_GRANT_TOOL="   ", CURIE_APPROVAL_RESUMED_KIND="")
    config = RunnerConfig.from_env(env)

    # A blank grant must never read as a tool named "" that the gate then lets
    # through; a blank resumed-kind must not claim a policy resume happened.
    assert config.approval_grant_tool is None
    assert config.approval_resumed_kind is None


def test_approval_required_tools_all_blank_is_no_gates() -> None:
    config = RunnerConfig.from_env(dict(_BASE, CURIE_APPROVAL_REQUIRED_TOOLS=" , , "))

    assert not config.approval_required_tools


def test_max_turns_raises_on_garbage() -> None:
    # Today's bare int(): an operator typo fails the boot loudly rather than
    # silently running with a different turn cap.
    with pytest.raises(ValueError):
        RunnerConfig.from_env(dict(_BASE, CURIE_MAX_TURNS="lots"))


def test_runner_port_raises_on_garbage() -> None:
    with pytest.raises(ValueError):
        RunnerConfig.from_env(dict(_BASE, CURIE_RUNNER_PORT="eighty"))


def test_history_window_comes_through_the_declared_surface() -> None:
    """The window is an operator knob on the boot env, not a bare os.environ read.

    Reading it off the process env at the call site meant the value could not be
    seen, tested, or overridden through the config the rest of the boot uses.
    """

    config = RunnerConfig.from_env(
        dict(_BASE, CURIE_HISTORY_MAX_TURNS="4", CURIE_HISTORY_MAX_BYTES="2048")
    )

    assert config.history_max_turns == 4
    assert config.history_max_bytes == 2048


@pytest.mark.parametrize("raw", ["", "   ", "twelve", "0", "-3", "1.5"])
def test_history_window_degrades_rather_than_raising(raw: str) -> None:
    """A typo in an operator's extraEnv must not become a boot crash.

    A nonpositive window is rejected the same way: max_turns=0 slices every turn
    and a nonpositive byte budget can never be met, so both are meaningless.
    None hands the consumer its own default.
    """

    config = RunnerConfig.from_env(
        dict(_BASE, CURIE_HISTORY_MAX_TURNS=raw, CURIE_HISTORY_MAX_BYTES=raw)
    )

    assert config.history_max_turns is None
    assert config.history_max_bytes is None


def test_memory_fact_limit_defaults_to_200_when_unset() -> None:
    """Unset, each memory holds and shows at most 200 facts, as before (#3624)."""

    from curie_runner.memory_facts import MAX_FACTS_PER_MEMORY

    assert MAX_FACTS_PER_MEMORY == 200
    assert RunnerConfig.from_env(dict(_BASE)).memory_max_facts == 200


@pytest.mark.parametrize("raw", ["3", "250"])
def test_memory_fact_limit_comes_through_the_declared_surface(raw: str) -> None:
    config = RunnerConfig.from_env(dict(_BASE, CURIE_MEMORY_MAX_FACTS=raw))

    assert config.memory_max_facts == int(raw)


@pytest.mark.parametrize("raw", ["", "   ", "lots", "0", "-3", "1.5"])
def test_memory_fact_limit_falls_back_to_200_rather_than_raising(raw: str) -> None:
    """A typo or a limit of zero or less must not crash boot; the default applies."""

    config = RunnerConfig.from_env(dict(_BASE, CURIE_MEMORY_MAX_FACTS=raw))

    assert config.memory_max_facts == 200


def test_repository_trust_comes_through_the_declared_surface() -> None:
    """ADR 0197: the code host origin and deep path reach the snapshot check."""

    config = RunnerConfig.from_env(
        dict(
            _BASE,
            CURIE_REPO_ORIGIN="https://gitlab.example.com",
            CURIE_REPO_PATH="platform/team/infra",
        )
    )

    assert config.repo_origin == "https://gitlab.example.com"
    assert config.repo_path == "platform/team/infra"


@pytest.mark.parametrize("raw", [None, ""])
def test_repository_trust_is_absent_on_a_github_boot(raw: str | None) -> None:
    """Unset or blank is a GitHub boot: the configured host and owner/name."""

    env = dict(_BASE)
    if raw is not None:
        env |= {"CURIE_REPO_ORIGIN": raw, "CURIE_REPO_PATH": raw}
    config = RunnerConfig.from_env(env)

    assert config.repo_origin is None
    assert config.repo_path is None


def test_malformed_budget_still_raises() -> None:
    with pytest.raises(ValidationError):
        RunnerConfig.from_env(dict(_BASE, CURIE_BUDGET="{not json"))


def test_system_prompt_is_no_longer_a_config_surface() -> None:
    """The bundle is the declared system-prompt surface and always wins (AC5).

    CURIE_SYSTEM_PROMPT let an operator env silently replace the prompt
    versioned with the agent, which is unauditable: the bundle says one thing and
    the sandbox runs another.
    """

    assert "system_prompt" not in _field_names()

    config = RunnerConfig.from_env(dict(_BASE, CURIE_SYSTEM_PROMPT="you-are-overridden"))

    # The env value must reach no field at all: if it lands anywhere on the
    # config, some boot path can still prefer it over the bundle's prompt.
    assert "you-are-overridden" not in repr(dataclasses.astuple(config))


def test_idempotent_tools_is_no_longer_a_config_surface() -> None:
    """The env override was never wired to a consumer (AC5).

    An unwired widening knob on a deny-by-default classifier is worse than none:
    it reads as an escape hatch that silently does nothing.
    """

    assert "idempotent_tools" not in _field_names()

    config = RunnerConfig.from_env(dict(_BASE, CURIE_IDEMPOTENT_TOOLS="Read,Bash"))

    assert "Read" not in repr(dataclasses.astuple(config))
