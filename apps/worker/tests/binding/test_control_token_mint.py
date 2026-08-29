"""Who the worker mints a fleet-control token for -- and who it never does.

ADR-0133. The API refuses a control token that names the wrong agent, so this
is the second, independent half of the same guarantee: no other sandbox is
handed one in the first place. Both halves are asserted because each is a
different failure. If only the API checked, every sandbox would carry a
fleet-reaching credential that happened to be rejected today. If only the worker
checked, a mint-site bug would be a privilege escalation.

No mocks and no DB, matching ``test_boot_env_golden``: ``boot_env`` makes no
engine call, so a bare resolver over a real WorkerConfig runs the real path.
"""

from __future__ import annotations

import uuid

from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.sandbox_token import verify

_CONTROL_AGENT = uuid.UUID("44444444-4444-4444-8444-444444444444")
_OTHER_AGENT = uuid.UUID("55555555-5555-4555-8555-555555555555")
_THREAD = "thread-1"
_KEY = "curie-dev-key"


def _resolved(agent_id: uuid.UUID, agent_name: str) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name=agent_name,
        agent_id=agent_id,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def _boot_env(config: WorkerConfig, resolved: ResolvedDeployment) -> dict[str, str]:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = config  # type: ignore[attr-defined]
    return resolver.boot_env(resolved, _THREAD)


def test_no_control_pair_when_no_operator_named_a_control_agent() -> None:
    """The default. Every agent on a stock install boots without the pair, so
    the feature is off until someone turns it on."""

    env = _boot_env(WorkerConfig(), _resolved(_CONTROL_AGENT, "curie-control"))
    assert "CURIE_CONTROL_TOKEN" not in env
    assert "CURIE_CONTROL_URL" not in env


def test_the_named_control_agent_gets_a_control_scoped_token() -> None:
    config = WorkerConfig(control_agent="curie-control")
    env = _boot_env(config, _resolved(_CONTROL_AGENT, "curie-control"))

    assert env["CURIE_CONTROL_URL"] == "http://localhost:8000/fleet"
    token = env["CURIE_CONTROL_TOKEN"]
    assert verify(token, _KEY, agent=str(_CONTROL_AGENT), scope="control") is True


def test_every_other_agent_gets_nothing_even_when_the_feature_is_on() -> None:
    """The property the whole design rests on: not a permission another agent
    fails at call time, but a credential it is never issued."""

    config = WorkerConfig(control_agent="curie-control")
    env = _boot_env(config, _resolved(_OTHER_AGENT, "sre-bot"))
    assert "CURIE_CONTROL_TOKEN" not in env
    assert "CURIE_CONTROL_URL" not in env


def test_the_control_token_is_not_a_state_token_and_vice_versa() -> None:
    """Scope is inside the signed payload, so the three credentials a control
    agent holds are mutually non-substitutable. A state token presented to the
    fleet plane fails, and the control token cannot reach the state store."""

    config = WorkerConfig(control_agent="curie-control")
    env = _boot_env(config, _resolved(_CONTROL_AGENT, "curie-control"))

    control = env["CURIE_CONTROL_TOKEN"]
    broad_state = env["CURIE_MEMORY_TOKEN"]
    app_state = env["CURIE_STATE_TOKEN"]

    assert len({control, broad_state, app_state}) == 3

    agent = str(_CONTROL_AGENT)
    assert verify(control, _KEY, agent=agent, scope="state") is False
    assert verify(control, _KEY, agent=agent, scope="state.app") is False
    assert verify(broad_state, _KEY, agent=agent, scope="control") is False
    assert verify(app_state, _KEY, agent=agent, scope="control") is False


def test_the_control_token_is_bound_to_the_control_agents_id() -> None:
    """Binding, not just well-formedness: the same token fails for any other
    agent, which is what stops it being useful if it leaks."""

    config = WorkerConfig(control_agent="curie-control")
    env = _boot_env(config, _resolved(_CONTROL_AGENT, "curie-control"))
    token = env["CURIE_CONTROL_TOKEN"]
    assert verify(token, _KEY, agent=str(_OTHER_AGENT), scope="control") is False


def test_a_name_collision_is_the_whole_test_for_privilege() -> None:
    """Privilege follows the agent NAME the platform resolved at deploy.

    An agent whose name differs by a character gets nothing -- there is no
    prefix, suffix, or case-insensitive match to trip over.
    """

    config = WorkerConfig(control_agent="curie-control")
    for impostor in ("curie-control-dev", "Curie-Control", "curie-contro", " curie-control"):
        env = _boot_env(config, _resolved(_OTHER_AGENT, impostor))
        assert "CURIE_CONTROL_TOKEN" not in env, impostor


def test_no_control_token_without_a_platform_key_to_sign_with() -> None:
    """The fake/local no-key path, matching how the state tokens behave: nothing
    to sign with means nothing is minted, rather than an unsigned credential."""

    config = WorkerConfig(control_agent="curie-control", api_key="")
    env = _boot_env(config, _resolved(_CONTROL_AGENT, "curie-control"))
    assert "CURIE_CONTROL_TOKEN" not in env
