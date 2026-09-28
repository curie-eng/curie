"""Per-agent warm-pool routing for connector-secret delivery (#1488).

The chart renders ``{fullname}-agent-{agent}-runner-pool`` next to the generic
``{fullname}-runner-pool``. Claims that carry connector secrets must name the
per-agent pool so the bound pod inherits that template's secretKeyRef and
``curietech.ai/agent`` label. Claims without secrets stay on the generic pool.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from curie_worker.binding import CONNECTOR_SECRET_KEYS_ENV, inject_connector_secrets
from curie_worker.sandbox import SandboxSubstrate, SubstrateConfig
from curie_worker.sandbox import types as sandbox_types
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.types import AGENT_LABEL, agent_warm_pool_name

from .conftest import FakeSandboxClient


def _substrate(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    agent_pools: frozenset[str] = frozenset({"acme-a", "acme-b"}),
) -> SandboxSubstrate:
    # agent_pools stands in for CURIE_AGENT_SANDBOX_POOLS (every per-agent pool
    # the chart rendered) and CURIE_AGENT_CONNECTOR_SECRET_POOLS (the ones whose
    # template carries connector secrets); here every pool carries them.
    return SandboxSubstrate(
        fake_k8s,
        affinity,
        replace(
            config,
            warm_pool="curie-runner-pool",
            agent_pools=agent_pools,
            connector_secret_pools=agent_pools,
        ),
    )


def test_agent_warm_pool_name_matches_chart_template() -> None:
    # charts/curie/templates/agent-sandbox.yaml:
    #   generic: {fullname}-runner-pool
    #   per-agent: {fullname}-agent-{agent}-runner-pool
    assert agent_warm_pool_name("curie-runner-pool", None) == "curie-runner-pool"
    assert agent_warm_pool_name("curie-runner-pool", "") == "curie-runner-pool"
    assert (
        agent_warm_pool_name("curie-runner-pool", "acme-a") == "curie-agent-acme-a-runner-pool"
    )
    assert (
        agent_warm_pool_name("curie-g1-runner-pool", "acme-b")
        == "curie-g1-agent-acme-b-runner-pool"
    )


def test_agent_warm_pool_name_leaves_unrecognized_base_alone() -> None:
    # An operator override that does not use the chart suffix is not rewritten.
    assert agent_warm_pool_name("custom-pool", "acme-a") == "custom-pool"


def test_claim_with_connector_secrets_targets_the_per_agent_pool(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    env: dict[str, str] = {"CURIE_BUDGET": "{}"}
    inject_connector_secrets(
        env, {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_secret"}, agent_label="acme-a"
    )
    handle = _substrate(fake_k8s, affinity, config).claim(
        "T-secret", env=env, agent_name="acme-a"
    )
    claim = fake_k8s.claims[handle.claim_name]
    assert claim.pool == "curie-agent-acme-a-runner-pool"
    assert claim.labels[AGENT_LABEL] == "acme-a"
    assert "additionalPodMetadata" not in claim.labels
    assert CONNECTOR_SECRET_KEYS_ENV in env


def test_claim_without_connector_secrets_stays_on_the_generic_pool(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    handle = _substrate(fake_k8s, affinity, config, agent_pools=frozenset()).claim(
        "T-generic", env={"CURIE_BUDGET": "{}"}, agent_name="acme-a"
    )
    claim = fake_k8s.claims[handle.claim_name]
    assert claim.pool == "curie-runner-pool"
    # The agent label still lands on the claim object so rotation can find it,
    # but the generic template has no per-agent secretKeyRef.
    assert claim.labels[AGENT_LABEL] == "acme-a"


def test_two_agents_with_the_same_secret_name_target_distinct_pools(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    for agent in ("acme-a", "acme-b"):
        env: dict[str, str] = {"CURIE_BUDGET": "{}"}
        inject_connector_secrets(
            env, {"GITHUB_PERSONAL_ACCESS_TOKEN": f"{agent}-sentinel"}, agent_label=agent
        )
        handle = substrate.claim(f"T-{agent}", env=env, agent_name=agent)
        assert fake_k8s.claims[handle.claim_name].pool == f"curie-agent-{agent}-runner-pool"
        assert fake_k8s.claims[handle.claim_name].labels[AGENT_LABEL] == agent


def test_registry_egress_agent_without_secrets_targets_its_per_agent_pool(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # #3083: agentSandbox.registryEgress.<agent> renders a NetworkPolicy that
    # selects curietech.ai/agent=<agent> pods, which only the per-agent template
    # labels. The chart names those agents in CURIE_AGENT_SANDBOX_POOLS.
    substrate = SandboxSubstrate(
        fake_k8s,
        affinity,
        replace(config, warm_pool="curie-runner-pool", agent_pools=frozenset({"factory"})),
    )
    declared = substrate.claim("T-reg", env={"CURIE_BUDGET": "{}"}, agent_name="factory")
    other = substrate.claim("T-other", env={"CURIE_BUDGET": "{}"}, agent_name="acme-a")
    assert fake_k8s.claims[declared.claim_name].pool == "curie-agent-factory-runner-pool"
    assert fake_k8s.claims[other.claim_name].pool == "curie-runner-pool"


def test_connector_secrets_without_a_rendered_pool_fail_fast_naming_pool_and_agent(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # #2943: an agent deployed by the CLI whose connectors.yaml declares secrets
    # carries the marker, but the chart rendered no pool for it. The claim used
    # to name that pool and wait out ClaimTimeoutError three times.
    env: dict[str, str] = {"CURIE_BUDGET": "{}"}
    inject_connector_secrets(
        env, {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_secret"}, agent_label="cli-bot"
    )
    substrate = _substrate(fake_k8s, affinity, config, agent_pools=frozenset())
    with pytest.raises(sandbox_types.MissingAgentPoolError) as raised:
        substrate.claim("T-cli", env=env, agent_name="cli-bot")
    message = str(raised.value)
    assert "curie-agent-cli-bot-runner-pool" in message
    assert "cli-bot" in message
    assert "agentSandbox.connectorSecrets" in message
    assert raised.value.agent_name == "cli-bot"
    assert raised.value.pool == "curie-agent-cli-bot-runner-pool"
    assert fake_k8s.claims == {}


def test_connector_secrets_with_an_operator_pool_override_are_refused(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # A non-chart pool name has no derivable per-agent sibling, and the generic
    # pool never receives connector secret values (they are stripped from the
    # claim env), so the claim is refused rather than run without its secrets.
    env: dict[str, str] = {"CURIE_BUDGET": "{}"}
    inject_connector_secrets(
        env, {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_secret"}, agent_label="cli-bot"
    )
    substrate = SandboxSubstrate(
        fake_k8s, affinity, replace(config, warm_pool="custom-pool", agent_pools=frozenset())
    )
    with pytest.raises(sandbox_types.MissingAgentPoolError) as raised:
        substrate.claim("T-custom", env=env, agent_name="cli-bot")
    assert "cli-bot" in str(raised.value)
    assert "custom-pool" in str(raised.value)
    assert fake_k8s.claims == {}


def test_connector_secrets_on_a_registry_only_pool_are_refused(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # A registryEgress pool exists but its template carries no connector
    # secretKeyRef, so a bundle declaring secrets must not land there.
    env: dict[str, str] = {"CURIE_BUDGET": "{}"}
    inject_connector_secrets(
        env, {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_secret"}, agent_label="factory"
    )
    substrate = SandboxSubstrate(
        fake_k8s,
        affinity,
        replace(config, warm_pool="curie-runner-pool", agent_pools=frozenset({"factory"})),
    )
    with pytest.raises(sandbox_types.MissingAgentPoolError) as raised:
        substrate.claim("T-reg-secret", env=env, agent_name="factory")
    assert "curie-agent-factory-runner-pool" in str(raised.value)
    assert fake_k8s.claims == {}


def test_connector_secrets_with_a_custom_base_pool_are_refused_even_when_listed(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # With a non-chart base pool the per-agent name cannot be derived, so the
    # claim would land on the base pool, which carries no connector secrets.
    env: dict[str, str] = {"CURIE_BUDGET": "{}"}
    inject_connector_secrets(
        env, {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_secret"}, agent_label="acme-a"
    )
    listed = frozenset({"acme-a"})
    substrate = SandboxSubstrate(
        fake_k8s,
        affinity,
        replace(
            config,
            warm_pool="custom-pool",
            agent_pools=listed,
            connector_secret_pools=listed,
        ),
    )
    with pytest.raises(sandbox_types.MissingAgentPoolError):
        substrate.claim("T-custom-listed", env=env, agent_name="acme-a")
    assert fake_k8s.claims == {}
