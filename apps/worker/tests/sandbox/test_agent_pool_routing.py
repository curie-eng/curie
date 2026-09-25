"""Per-agent warm-pool routing for connector-secret delivery (#1488).

The chart renders ``{fullname}-agent-{agent}-runner-pool`` next to the generic
``{fullname}-runner-pool``. Claims that carry connector secrets must name the
per-agent pool so the bound pod inherits that template's secretKeyRef and
``curietech.ai/agent`` label. Claims without secrets stay on the generic pool.
"""

from __future__ import annotations

from dataclasses import replace

from curie_worker.binding import CONNECTOR_SECRET_KEYS_ENV, inject_connector_secrets
from curie_worker.sandbox import SandboxSubstrate, SubstrateConfig
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.types import AGENT_LABEL, agent_warm_pool_name

from .conftest import FakeSandboxClient


def _substrate(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> SandboxSubstrate:
    return SandboxSubstrate(fake_k8s, affinity, replace(config, warm_pool="curie-runner-pool"))


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
    handle = _substrate(fake_k8s, affinity, config).claim(
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
