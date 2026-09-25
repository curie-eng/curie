"""The runner model + credentials passthrough layered onto every boot env.

Pure unit tests (no DB / Valkey): both the runs binding and the eval consumer
must forward CURIE_FAKE_MODEL and CURIE_CREDENTIALS from the worker config
into the sandbox boot env, and must omit them when the config leaves them unset.
"""

from __future__ import annotations

import uuid

import pytest
from curie_worker.binding import (
    APPROVAL_REQUIRED_ENV,
    BASE_URL_ENV,
    CREDENTIALS_ENV,
    FAKE_MODEL_ENV,
    MODEL_ENV,
    THINKING_ENV,
    BindingResolver,
    ResolvedDeployment,
    apply_model_env,
)
from curie_worker.config import WorkerConfig
from curie_worker.eval.stream import EvalJob, EvalStreamConsumer
from curie_worker.sandbox_token import verify


def _resolved(model: str | None = None, thinking: str | None = None) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_id=uuid.uuid4(),
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        model=model,
        thinking=thinking,
    )


def test_apply_model_env_forwards_fake_and_credentials_when_set() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(fake_model=True, credentials="tok-abc"))
    assert env[FAKE_MODEL_ENV] == "1"
    assert env[CREDENTIALS_ENV] == "tok-abc"


def test_apply_model_env_omits_both_when_unset() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())  # defaults: fake_model=False, credentials=""
    assert FAKE_MODEL_ENV not in env
    assert CREDENTIALS_ENV not in env


def test_apply_model_env_leaves_sdk_creds_absent_without_credentials() -> None:
    # No BYO credential -> the SDK vars are neither added nor blanked, so the
    # legacy ambient-token real-model path still flows the operator's token.
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in env
    assert "ANTHROPIC_API_KEY" not in env


def test_apply_model_env_forwards_base_url_and_model_without_fake_flag() -> None:
    base_url_env, model_env = BASE_URL_ENV, MODEL_ENV
    assert base_url_env == "ANTHROPIC_BASE_URL"
    assert model_env == "CURIE_MODEL"

    env: dict[str, str] = {}
    apply_model_env(
        env,
        WorkerConfig(model_base_url="http://ollama:11434", model="qwen3:4b"),
    )

    assert env[base_url_env] == "http://ollama:11434"
    assert env[model_env] == "qwen3:4b"
    assert FAKE_MODEL_ENV not in env


def test_apply_model_env_forwards_model_without_base_url() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(credentials="sk-or-xyz", model="z-ai/glm-5.2"))
    assert env[MODEL_ENV] == "z-ai/glm-5.2"
    assert env[CREDENTIALS_ENV] == "sk-or-xyz"
    assert BASE_URL_ENV not in env


def test_apply_model_env_forwards_base_url_without_model() -> None:
    base_url_env, model_env = BASE_URL_ENV, MODEL_ENV

    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(model_base_url="http://ollama:11434"))

    assert env[base_url_env] == "http://ollama:11434"
    assert model_env not in env


def test_apply_model_env_omits_base_url_and_model_by_default() -> None:
    base_url_env, model_env = BASE_URL_ENV, MODEL_ENV

    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())

    assert base_url_env not in env
    assert model_env not in env


def test_worker_config_reads_local_model_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_MODEL_BASE_URL", "http://x:1")
    monkeypatch.setenv("CURIE_MODEL", "qwen3:4b")
    config = WorkerConfig()

    assert config.model_base_url == "http://x:1"
    assert config.model == "qwen3:4b"


def test_binding_boot_env_carries_fake_and_credentials() -> None:
    # No engine call is made by boot_env, so a bare resolver suffices.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(fake_model=True, credentials="cred-1")  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "thread-1")
    assert env[FAKE_MODEL_ENV] == "1"
    assert env[CREDENTIALS_ENV] == "cred-1"

    # Fake model off + no credentials -> neither leaks into the boot env.
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    env = resolver.boot_env(_resolved(), "thread-1")
    assert FAKE_MODEL_ENV not in env
    assert CREDENTIALS_ENV not in env


def test_eval_boot_env_carries_fake_and_credentials() -> None:
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(fake_model=True, credentials="cred-eval"),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = EvalJob(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        sha="deadbeef",
        suite="smoke",
        bundle_ref="bundles/x.zip",
        requested_at="2026-07-05T00:00:00+00:00",
    )
    env = consumer._boot_env(item, None, None, model=None)
    assert env[FAKE_MODEL_ENV] == "1"
    assert env[CREDENTIALS_ENV] == "cred-eval"


# --- Per-agent model selection (#254) -----------------------------------------


def test_apply_model_env_model_override_wins_over_config() -> None:
    env: dict[str, str] = {}
    apply_model_env(
        env, WorkerConfig(model="platform-default"), model_override="agent-glm-5"
    )
    assert env[MODEL_ENV] == "agent-glm-5"


def test_apply_model_env_model_override_none_falls_back_to_config() -> None:
    env: dict[str, str] = {}
    apply_model_env(
        env, WorkerConfig(model="platform-default"), model_override=None
    )
    assert env[MODEL_ENV] == "platform-default"


def test_apply_model_env_model_override_without_config_model() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(), model_override="agent-kimi")
    assert env[MODEL_ENV] == "agent-kimi"


def test_binding_boot_env_forwards_per_agent_model() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(model="platform-default")  # type: ignore[attr-defined]

    # A pinned per-agent model overrides the worker default.
    env = resolver.boot_env(_resolved(model="agent-deepseek-v4"), "thread-1")
    assert env[MODEL_ENV] == "agent-deepseek-v4"

    # No per-agent model -> the platform default still applies.
    env = resolver.boot_env(_resolved(model=None), "thread-1")
    assert env[MODEL_ENV] == "platform-default"


# --- Operator-scoped model wire declaration (#514) -----------------------------
#
# The runner reads CURIE_MODEL_API_BACKEND (the endpoint's wire protocol) and
# CURIE_MODEL_ENV_KEY (which env var carries the credential). The runner boot
# env is closed-world -- only what apply_model_env emits is there -- so without a
# producer here both vars are unreachable and the feature is dead config. Both
# mirror model_base_url's chain exactly: WorkerConfig field -> apply_model_env ->
# boot env, emitted only when non-empty.
#
# The env-var names are the cross-package contract with the runner; asserted by
# their literal strings so this file never depends on a constant that only exists
# after the feature lands (which would break collection of the whole module).
API_BACKEND_ENV = "CURIE_MODEL_API_BACKEND"
MODEL_ENV_KEY_ENV = "CURIE_MODEL_ENV_KEY"


def test_binding_exports_the_new_model_env_constants() -> None:
    # The emission sites must go through module constants, like BASE_URL_ENV does,
    # rather than inline literals scattered across the two lanes.
    from curie_worker import binding

    assert binding.API_BACKEND_ENV == API_BACKEND_ENV
    assert binding.MODEL_ENV_KEY_ENV == MODEL_ENV_KEY_ENV


def test_apply_model_env_forwards_api_backend_and_env_key_when_set() -> None:
    env: dict[str, str] = {}
    apply_model_env(
        env,
        WorkerConfig(
            model_api_backend="messages",
            model_env_key='["ANTHROPIC_AUTH_TOKEN"]',
        ),
    )

    assert env[API_BACKEND_ENV] == "messages"
    assert env[MODEL_ENV_KEY_ENV] == '["ANTHROPIC_AUTH_TOKEN"]'


def test_apply_model_env_omits_api_backend_and_env_key_by_default() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())

    assert API_BACKEND_ENV not in env
    assert MODEL_ENV_KEY_ENV not in env


def test_apply_model_env_omits_api_backend_and_env_key_when_empty_string() -> None:
    # ABSENT, not present-and-empty. The runner treats "" as "not declared" and
    # defaults, so emitting an empty var would work by accident today -- but it
    # also makes the boot env lie about what was configured, and any consumer that
    # checks presence rather than truthiness would then see a declaration that was
    # never made.
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(model_api_backend="", model_env_key=""))

    assert API_BACKEND_ENV not in env
    assert MODEL_ENV_KEY_ENV not in env


def test_apply_model_env_composes_new_vars_with_the_existing_emission() -> None:
    # The two additions must not disturb the fake_model / credentials / base_url /
    # model emission they sit next to.
    env: dict[str, str] = {}
    apply_model_env(
        env,
        WorkerConfig(
            fake_model=True,
            credentials="cred-compose",
            model_base_url="http://ollama:11434",
            model="qwen3:4b",
            model_api_backend="messages",
            model_env_key="MY_PROVIDER_KEY",
        ),
    )

    assert env[FAKE_MODEL_ENV] == "1"
    assert env[CREDENTIALS_ENV] == "cred-compose"
    assert env[BASE_URL_ENV] == "http://ollama:11434"
    assert env[MODEL_ENV] == "qwen3:4b"
    assert env[API_BACKEND_ENV] == "messages"
    assert env[MODEL_ENV_KEY_ENV] == "MY_PROVIDER_KEY"


def test_binding_boot_env_carries_api_backend_and_env_key() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        model_api_backend="messages", model_env_key="MY_PROVIDER_KEY"
    )

    env = resolver.boot_env(_resolved(), "thread-1")
    assert env[API_BACKEND_ENV] == "messages"
    assert env[MODEL_ENV_KEY_ENV] == "MY_PROVIDER_KEY"


def test_eval_boot_env_carries_api_backend_and_env_key() -> None:
    # Both lanes boot the runner through apply_model_env, so the eval consumer
    # gets the declaration too rather than silently dialing a different endpoint.
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(
            model_api_backend="messages", model_env_key="MY_PROVIDER_KEY"
        ),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = EvalJob(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        sha="deadbeef",
        suite="smoke",
        bundle_ref="bundles/x.zip",
        requested_at="2026-07-05T00:00:00+00:00",
    )

    env = consumer._boot_env(item, None, None, model=None)
    assert env[API_BACKEND_ENV] == "messages"
    assert env[MODEL_ENV_KEY_ENV] == "MY_PROVIDER_KEY"


# --- Operator scope is the security property, not a style choice (#514) --------
#
# Both vars are OPERATOR scope: they come from WorkerConfig and NEVER from the
# per-agent row. CURIE_MODEL is per-agent (#254) precisely because a model id is
# inert. These two are not: model_env_key names which env var the runner reads a
# credential OUT of, and it is read from the same boot env that holds the scoped
# state tokens (ADR-0033) and the agent's connector secrets. A per-agent override
# would let anyone who can write an agent row aim the runner at those. The tests
# below fail if someone later plumbs either one through the agent row.


def test_apply_model_env_has_no_per_agent_override_params() -> None:
    # Signature pin, still exact. Exactly TWO per-agent parameters are allowed
    # here, each authorized by name: model_override (#254, the per-agent model)
    # and thinking_override (#1182, ADR-0098, the per-agent thinking depth).
    #
    # The regression this guards has not changed shape: a NEW *_override
    # parameter would mean an agent row can aim the credential read or redeclare
    # the wire protocol. Thinking depth is neither -- it is the same
    # capability-versus-cost axis as the model itself, which is why ADR-0098
    # placed it beside model rather than beside api_backend. Widening this set
    # again needs the same kind of argument, on the record.
    #
    # The teeth are in the two tests below, which are unchanged and still pass:
    # api_backend and env_key never reach ResolvedDeployment and can never be
    # supplied per agent.
    import inspect

    params = set(inspect.signature(apply_model_env).parameters)
    assert params == {"env", "config", "model_override", "thinking_override"}


def test_resolved_deployment_carries_no_api_backend_or_env_key_field() -> None:
    # The agent row is the other half of the same seam: if the columns never reach
    # ResolvedDeployment, boot_env has nothing per-agent to forward.
    fields = set(ResolvedDeployment.model_fields)
    assert "model_api_backend" not in fields
    assert "model_env_key" not in fields
    assert "api_backend" not in fields
    assert "env_key" not in fields


def test_binding_boot_env_api_backend_is_not_overridable_per_agent() -> None:
    # Operator config is the sole source: an agent row (here carrying its own
    # pinned model, the one thing it MAY override) cannot change or supply either
    # declaration.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        model_api_backend="messages", model_env_key="MY_PROVIDER_KEY"
    )

    env = resolver.boot_env(_resolved(model="agent-pinned-model"), "thread-1")
    assert env[MODEL_ENV] == "agent-pinned-model"  # per-agent model still wins
    assert env[API_BACKEND_ENV] == "messages"  # operator scope, unchanged
    assert env[MODEL_ENV_KEY_ENV] == "MY_PROVIDER_KEY"

    # With the operator config silent, no agent row can conjure either var.
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    env = resolver.boot_env(_resolved(model="agent-pinned-model"), "thread-1")
    assert API_BACKEND_ENV not in env
    assert MODEL_ENV_KEY_ENV not in env


# --- Per-sandbox runner token minting (issue #63) -----------------------------
# The env-var name is the cross-package contract with the runner; asserted by its
# literal string so this test file never depends on a constant that only exists
# after the feature lands (which would break collection of the whole module).
RUNNER_TOKEN_ENV = "CURIE_RUNNER_TOKEN"


def test_binding_boot_env_mints_runner_token() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "thread-1")
    assert env.get(RUNNER_TOKEN_ENV), "boot_env must mint a non-empty runner token"


def test_binding_boot_env_token_is_unique_per_claim() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]

    first = resolver.boot_env(_resolved(), "thread-1")[RUNNER_TOKEN_ENV]
    second = resolver.boot_env(_resolved(), "thread-1")[RUNNER_TOKEN_ENV]
    assert first != second  # a fresh token is minted per claim


def test_binding_boot_env_token_survives_credentials() -> None:
    # Regression guard on the PR #109 apply_model_env credential pop: it strips
    # ambient SDK creds when CURIE_CREDENTIALS is set, but must not eat the
    # runner token (which is not a model credential).
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(fake_model=True, credentials="cred-1")  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "thread-1")
    assert env.get(RUNNER_TOKEN_ENV), "the runner token must survive credential selection"
    assert env[CREDENTIALS_ENV] == "cred-1"


# --- Conversation-history ref delivery (#20, ADR-0029) ------------------------
# The env-var names are the cross-package contract with the runner; asserted by
# their literal strings so this file never depends on a constant that only exists
# after the feature lands.
HISTORY_REF_ENV = "CURIE_HISTORY_REF"
HISTORY_TOKEN_ENV = "CURIE_HISTORY_TOKEN"


def test_binding_boot_env_sets_per_thread_transcript_ref() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    config = WorkerConfig()
    resolver._config = config  # type: ignore[attr-defined]

    resolved = _resolved()
    env = resolver.boot_env(resolved, "1720000000.000100")
    base = config.api_base_url.rstrip("/")
    assert env[HISTORY_REF_ENV] == (
        f"{base}/agents/{resolved.agent_id}/state/transcript/1720000000.000100"
    )
    # The history token is a scoped state token bound to this agent
    # (ADR-0033, #410), never the raw platform key.
    assert env[HISTORY_TOKEN_ENV] != config.api_key
    assert verify(
        env[HISTORY_TOKEN_ENV],
        config.api_key,
        agent=str(resolved.agent_id),
        scope="state",
    )


def test_binding_boot_env_history_ref_is_deterministic_per_thread() -> None:
    # Every claim for a thread yields the same ref, so a fresh, a restarted, and a
    # resumed sandbox all rehydrate the same transcript (#20) with no special path.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]

    resolved = _resolved()
    first = resolver.boot_env(resolved, "t-1")[HISTORY_REF_ENV]
    second = resolver.boot_env(resolved, "t-1")[HISTORY_REF_ENV]
    assert first == second


def test_binding_boot_env_url_encodes_thread_key() -> None:
    # A thread key with reserved characters must not escape the transcript key
    # path; it is percent-encoded (safe="") so slashes cannot inject a new path.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "weird/../key")
    assert "/state/transcript/weird%2F..%2Fkey" in env[HISTORY_REF_ENV]


def test_binding_boot_env_omits_history_token_without_api_key() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key="")  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "t-1")
    assert HISTORY_TOKEN_ENV not in env


def test_binding_boot_env_carries_approval_required_tools() -> None:
    # Permission gates (#245): the agent's approval-required tool names travel
    # to the runner as a comma-joined env var; absent config injects nothing.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]

    gated = _resolved()
    gated = gated.model_copy(
        update={"approval_required_tools": ["Bash", "mcp__github__create_issue"]}
    )
    env = resolver.boot_env(gated, "thread-1")
    assert env[APPROVAL_REQUIRED_ENV] == "Bash,mcp__github__create_issue"

    env = resolver.boot_env(_resolved(), "thread-1")
    assert APPROVAL_REQUIRED_ENV not in env


# --- Opt-in false-completion check (#517, #669) --------------------------------
#
# The runner (runner/src/curie_runner/config.py) reads CURIE_FALSE_COMPLETION_
# CHECK directly (never through the frozen BootEnv contract, since the check is
# observe-only and authority-free). Before #669 nothing set that var in any
# deployed compose/k8s sandbox, so the #588 check was unreachable outside a
# hand-run local runner. Mirrors model_api_backend/model_env_key's chain exactly
# (#514): WorkerConfig field -> boot env, emitted only when truthy, operator scope
# only (no per-agent override), and both the runs binding and the eval lane must
# agree.
FALSE_COMPLETION_CHECK_ENV = "CURIE_FALSE_COMPLETION_CHECK"


def test_worker_config_reads_false_completion_check_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert WorkerConfig().false_completion_check is False
    monkeypatch.setenv("CURIE_FALSE_COMPLETION_CHECK", "1")
    assert WorkerConfig().false_completion_check is True


def test_apply_model_env_forwards_false_completion_check_when_set() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(false_completion_check=True))
    assert env[FALSE_COMPLETION_CHECK_ENV] == "1"


def test_apply_model_env_omits_false_completion_check_by_default() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())
    assert FALSE_COMPLETION_CHECK_ENV not in env


def test_binding_boot_env_carries_false_completion_check() -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(false_completion_check=True)  # type: ignore[attr-defined]

    env = resolver.boot_env(_resolved(), "thread-1")
    assert env[FALSE_COMPLETION_CHECK_ENV] == "1"

    # Default off preserves current behavior: unset until explicitly enabled.
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    env = resolver.boot_env(_resolved(), "thread-1")
    assert FALSE_COMPLETION_CHECK_ENV not in env


def test_eval_boot_env_carries_false_completion_check() -> None:
    # Both lanes boot the runner through apply_model_env for this knob, so the
    # eval consumer gets the same observe-only check rather than silently
    # running with a different boot posture than a bound run.
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(false_completion_check=True),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = EvalJob(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        sha="deadbeef",
        suite="smoke",
        bundle_ref="bundles/x.zip",
        requested_at="2026-07-05T00:00:00+00:00",
    )

    env = consumer._boot_env(item, None, None, model=None)
    assert env[FALSE_COMPLETION_CHECK_ENV] == "1"


def test_resolved_deployment_carries_no_false_completion_check_field() -> None:
    # Operator scope is the security property, not a style choice (#514's
    # precedent): a per-agent row must never be able to enable an observe-only
    # runner behavior for itself. If this field ever reaches ResolvedDeployment,
    # boot_env would have a per-agent value to forward.
    fields = set(ResolvedDeployment.model_fields)
    assert "false_completion_check" not in fields


# --- thinking depth: the two operator layers (#1182, ADR-0098) ----------------
# Mirrors the model tests directly, because the precedence IS the model's: the
# agent row wins, then the platform default, then nothing at all. The third case
# is the load-bearing one -- "nothing at all" is what keeps an unconfigured
# install behaving exactly as it did before this feature existed.


def test_thinking_unset_at_both_layers_writes_nothing() -> None:
    # Not "adaptive", not an empty string: the key must be ABSENT, so the runner
    # omits the SDK option and the model's own default stands.
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig())
    assert THINKING_ENV not in env


def test_thinking_platform_default_is_forwarded() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(thinking="adaptive"))
    assert env[THINKING_ENV] == "adaptive"


def test_thinking_per_agent_value_wins_over_the_platform_default() -> None:
    env: dict[str, str] = {}
    apply_model_env(env, WorkerConfig(thinking="adaptive"), thinking_override="disabled")
    assert env[THINKING_ENV] == "disabled"


def test_boot_env_applies_the_same_precedence_on_the_runs_lane() -> None:
    # The eval lane goes through apply_model_env; the runs lane goes through
    # boot_env. They are the sibling paths that must not drift, so pin the runs
    # lane against the same three cases.
    resolver = BindingResolver.__new__(BindingResolver)

    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    assert THINKING_ENV not in resolver.boot_env(_resolved(), "thread-1")

    resolver._config = WorkerConfig(thinking="enabled:2000")  # type: ignore[attr-defined]
    assert resolver.boot_env(_resolved(), "thread-1")[THINKING_ENV] == "enabled:2000"

    env = resolver.boot_env(_resolved(thinking="disabled"), "thread-1")
    assert env[THINKING_ENV] == "disabled"


def test_an_empty_per_agent_thinking_does_not_emit_an_empty_key() -> None:
    # A cleared column reads as "" through some paths; an empty value is "unset",
    # never a value, or the runner would parse "" and the operator would see a
    # knob that does nothing.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    assert THINKING_ENV not in resolver.boot_env(_resolved(thinking=""), "thread-1")
