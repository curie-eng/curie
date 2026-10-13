"""Repository runner pool routing (ADR 0216 decision 6).

``CURIE_REPO_RUNNER_POOLS`` binds a repository to a prebuilt runner pool. A
fresh claim for that repository names the entry's pool, the route remembers
which image it booted (digests and Docker flag), and a retained route whose
image no longer matches the current policy is retired for a fresh claim. A
missing pool or a runner built on another platform runner is refused before any
Kubernetes write. A repository without an entry routes exactly as before.

Everything is driven through the public ``SandboxSubstrate`` API against the
in-memory Kubernetes fake and the real Valkey affinity store.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace

import pytest
import redis
from curie_worker import sandbox as sandbox_package
from curie_worker.repo_runner_pools import RepoRunnerPool
from curie_worker.sandbox import SandboxSubstrate, SubstrateConfig
from curie_worker.sandbox import types as sandbox_types
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.types import KubeTransientError, MissingAgentPoolError, RouteRecord

from .conftest import PoolProbingFakeSandboxClient

INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")
OTHER = "sha256:" + "e" * 64
DIGESTS = (INDEX, AMD64, ARM64)
REPO = "acme-corp/api"
POOL = "acme-api-runner"
BASE_POOL = "curie-runner-pool"


def _entry(
    pool: str = POOL,
    *,
    digests: tuple[str, str, str] = DIGESTS,
    base: str = BASE,
    docker: bool = False,
) -> RepoRunnerPool:
    return RepoRunnerPool.model_validate(
        {"pool": pool, "digests": list(digests), "base": base, "docker": docker}
    )


def _substrate(
    k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    pools: Mapping[str, RepoRunnerPool],
    *,
    platform: str | None = BASE,
    agent_pools: frozenset[str] = frozenset(),
) -> SandboxSubstrate:
    return SandboxSubstrate(
        k8s,
        affinity,
        replace(
            config,
            warm_pool=BASE_POOL,
            agent_pools=agent_pools,
            repo_runner_pools=pools,
            platform_runner_digest=platform,
        ),
    )


def _assert_nothing_written(
    k8s: PoolProbingFakeSandboxClient, affinity: AffinityStore, thread_key: str
) -> None:
    assert k8s.created == []
    assert k8s.claims == {}
    assert affinity.get(thread_key) is None


# -- fresh routing ------------------------------------------------------------


def test_refusal_is_exported_from_the_sandbox_package() -> None:
    assert sandbox_package.RepoRunnerPoolRefused is sandbox_types.RepoRunnerPoolRefused
    assert issubclass(sandbox_types.RepoRunnerPoolRefused, MissingAgentPoolError)


def test_repo_with_an_entry_claims_its_pool_and_records_the_image(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    # The entry outranks the per-agent pool the agent would otherwise get.
    substrate = _substrate(
        probing_k8s,
        affinity,
        config,
        {REPO: _entry(docker=True)},
        agent_pools=frozenset({"factory"}),
    )
    handle = substrate.claim("T-fresh", workspace_repo=REPO, agent_name="factory")

    assert probing_k8s.claims[handle.claim_name].pool == POOL
    assert POOL in probing_k8s.warm_pool_probes
    assert handle.runner_digests == DIGESTS
    assert isinstance(handle.runner_digests, tuple)
    assert handle.runner_docker is True
    stored = affinity.get("T-fresh")
    assert stored is not None
    assert stored.handle == handle
    assert isinstance(stored.handle.runner_digests, tuple)


def test_repo_key_matches_case_insensitively(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    substrate = _substrate(probing_k8s, affinity, config, {"Acme-Corp/API": _entry()})
    handle = substrate.claim("T-case", workspace_repo="acme-corp/api")
    assert probing_k8s.claims[handle.claim_name].pool == POOL
    assert handle.runner_digests == DIGESTS


def test_existing_pool_with_matching_base_is_claimed(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    # Liveness counterpart of both refusals: the same entry with its pool
    # rendered and its base equal to this installation's runner is served.
    probing_k8s.warm_pools = frozenset({BASE_POOL, POOL})
    handle = _substrate(probing_k8s, affinity, config, {REPO: _entry()}).claim(
        "T-live", workspace_repo=REPO
    )
    assert probing_k8s.claims[handle.claim_name].pool == POOL
    assert handle.runner_docker is False


# -- refusals -----------------------------------------------------------------


def test_missing_pool_is_refused_before_any_kubernetes_write(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    probing_k8s.warm_pools = frozenset({BASE_POOL})
    substrate = _substrate(probing_k8s, affinity, config, {REPO: _entry()})
    minted: list[Mapping[str, str]] = []

    def _mint(env: Mapping[str, str]) -> dict[str, str]:
        minted.append(env)
        return dict(env)

    substrate.set_boot_credential_minter(_mint)
    with pytest.raises(sandbox_types.RepoRunnerPoolRefused) as raised:
        substrate.claim("T-missing", env={"CURIE_BUDGET": "{}"}, workspace_repo=REPO)

    refusal = raised.value
    assert isinstance(refusal, MissingAgentPoolError)
    assert refusal.reason == "missing_pool"
    assert refusal.repo == REPO
    assert refusal.pool == POOL
    message = str(refusal)
    assert REPO in message
    assert POOL in message
    assert "missing_pool" in message
    assert f"curie cluster factory --repo {REPO}" in message
    assert minted == []
    _assert_nothing_written(probing_k8s, affinity, "T-missing")


@pytest.mark.parametrize("platform", [OTHER, None], ids=["other-runner", "unpinned-runner"])
def test_stale_base_is_refused_before_any_kubernetes_write(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    platform: str | None,
) -> None:
    # The pool exists; only the base it was built on is wrong (or this
    # installation's runner image carries no digest to compare against).
    substrate = _substrate(probing_k8s, affinity, config, {REPO: _entry()}, platform=platform)
    with pytest.raises(sandbox_types.RepoRunnerPoolRefused) as raised:
        substrate.claim("T-stale", workspace_repo=REPO)

    refusal = raised.value
    assert isinstance(refusal, MissingAgentPoolError)
    assert refusal.reason == "stale_base"
    assert refusal.repo == REPO
    assert refusal.pool == POOL
    message = str(refusal)
    assert REPO in message
    assert "stale_base" in message
    assert f"curie cluster factory --repo {REPO}" in message
    _assert_nothing_written(probing_k8s, affinity, "T-stale")


def test_unreadable_pool_is_transient_not_a_refusal(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    probing_k8s.warm_pool_error = RuntimeError("apiserver 503")
    substrate = _substrate(probing_k8s, affinity, config, {REPO: _entry()})
    with pytest.raises(KubeTransientError) as raised:
        substrate.claim("T-probe", workspace_repo=REPO)
    assert not isinstance(raised.value, MissingAgentPoolError)
    _assert_nothing_written(probing_k8s, affinity, "T-probe")


def test_refusal_on_a_retained_route_leaves_the_route_untouched(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    first = _substrate(probing_k8s, affinity, config, {REPO: _entry()}).claim(
        "T-keep", workspace_repo=REPO
    )
    rebuilt = {REPO: _entry(digests=(INDEX, OTHER, ARM64))}
    probing_k8s.warm_pools = frozenset({BASE_POOL})
    with pytest.raises(sandbox_types.RepoRunnerPoolRefused):
        _substrate(probing_k8s, affinity, config, rebuilt).claim("T-keep", workspace_repo=REPO)
    assert first.claim_name in probing_k8s.claims
    assert probing_k8s.created == [first.claim_name]
    stored = affinity.get("T-keep")
    assert stored is not None and stored.handle.claim_name == first.claim_name


# -- no entry: today's routing ------------------------------------------------


def test_repo_without_an_entry_routes_as_before(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    # Only the generic pool, the declared agent's pool, and the bound
    # repository's pool exist. The unpinned platform image only matters to a
    # repository with an entry.
    probing_k8s.warm_pools = frozenset({BASE_POOL, "curie-agent-factory-runner-pool", POOL})
    substrate = _substrate(
        probing_k8s,
        affinity,
        config,
        {REPO: _entry()},
        platform=None,
        agent_pools=frozenset({"factory"}),
    )
    declared = substrate.claim("T-agent", workspace_repo="acme-corp/web", agent_name="factory")
    other = substrate.claim("T-other", workspace_repo="acme-corp/web", agent_name="acme-a")
    bare = substrate.claim("T-bare")

    assert probing_k8s.claims[declared.claim_name].pool == "curie-agent-factory-runner-pool"
    assert probing_k8s.claims[other.claim_name].pool == BASE_POOL
    assert probing_k8s.claims[bare.claim_name].pool == BASE_POOL
    for handle in (declared, other, bare):
        assert handle.runner_digests is None
        assert handle.runner_docker is False


# -- reuse versus re-claim ----------------------------------------------------


def test_unchanged_policy_reuses_the_route(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    pools = {REPO: _entry(docker=True)}
    first = _substrate(probing_k8s, affinity, config, pools).claim("T-reuse", workspace_repo=REPO)
    again = _substrate(probing_k8s, affinity, config, pools).claim("T-reuse", workspace_repo=REPO)
    assert again == first
    assert probing_k8s.created == [first.claim_name]
    assert probing_k8s.deleted == []


@pytest.mark.parametrize(
    ("first_pools", "second_pools", "second_repo", "expected_pool", "expected"),
    [
        pytest.param(
            {REPO: _entry(), "acme-corp/web": _entry("acme-web-runner", docker=True)},
            {REPO: _entry(), "acme-corp/web": _entry("acme-web-runner", docker=True)},
            "acme-corp/web",
            "acme-web-runner",
            (DIGESTS, True),
            id="repo-changed",
        ),
        pytest.param(
            {REPO: _entry()},
            {REPO: _entry(digests=(INDEX, OTHER, ARM64))},
            REPO,
            POOL,
            ((INDEX, OTHER, ARM64), False),
            id="digest-changed",
        ),
        pytest.param(
            {REPO: _entry()},
            {REPO: _entry(docker=True)},
            REPO,
            POOL,
            (DIGESTS, True),
            id="docker-changed",
        ),
        pytest.param(
            {},
            {REPO: _entry()},
            REPO,
            POOL,
            (DIGESTS, False),
            id="entry-added",
        ),
        pytest.param(
            {REPO: _entry()},
            {},
            REPO,
            BASE_POOL,
            (None, False),
            id="entry-removed",
        ),
    ],
)
def test_policy_change_retires_the_route_and_claims_fresh(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    first_pools: Mapping[str, RepoRunnerPool],
    second_pools: Mapping[str, RepoRunnerPool],
    second_repo: str,
    expected_pool: str,
    expected: tuple[tuple[str, ...] | None, bool],
) -> None:
    # The derived per-agent probe must not pick anything up: only these exist.
    probing_k8s.warm_pools = frozenset({BASE_POOL, POOL, "acme-web-runner"})
    first = _substrate(probing_k8s, affinity, config, first_pools).claim(
        "T-change", workspace_repo=REPO
    )
    second = _substrate(probing_k8s, affinity, config, second_pools).claim(
        "T-change", workspace_repo=second_repo
    )

    assert second.claim_name != first.claim_name
    assert first.claim_name not in probing_k8s.claims
    assert first.claim_name in probing_k8s.deleted
    assert probing_k8s.claims[second.claim_name].pool == expected_pool
    assert (second.runner_digests, second.runner_docker) == expected
    stored = affinity.get("T-change")
    assert stored is not None and stored.handle.claim_name == second.claim_name


def test_route_for_another_repository_is_never_reused_even_without_entries(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    # ADR 0216 decision 6: the repository must match before any reuse, with or
    # without a pool entry. A caller naming no repository still adopts.
    probing_k8s.warm_pools = frozenset({BASE_POOL})
    substrate = _substrate(probing_k8s, affinity, config, {})
    first = substrate.claim("T-other", workspace_repo=REPO)
    assert substrate.claim("T-other", workspace_repo=REPO.upper()) == first
    assert substrate.claim("T-other") == first

    second = substrate.claim("T-other", workspace_repo="acme-corp/web")

    assert second.claim_name != first.claim_name
    assert first.claim_name in probing_k8s.deleted
    assert second.workspace_repo == "acme-corp/web"


def test_adopt_retires_a_route_whose_image_changed(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    first = _substrate(probing_k8s, affinity, config, {REPO: _entry()}).claim(
        "T-adopt", workspace_repo=REPO
    )
    rebuilt = _substrate(
        probing_k8s, affinity, config, {REPO: _entry(digests=(INDEX, AMD64, OTHER))}
    )
    assert rebuilt.adopt("T-adopt") is None
    assert first.claim_name not in probing_k8s.claims
    assert first.claim_name in probing_k8s.deleted
    assert affinity.get("T-adopt") is None


def test_adopt_keeps_a_route_whose_image_is_unchanged(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> None:
    pools = {REPO: _entry()}
    first = _substrate(probing_k8s, affinity, config, pools).claim("T-keep", workspace_repo=REPO)
    assert _substrate(probing_k8s, affinity, config, pools).adopt("T-keep") == first
    assert first.claim_name in probing_k8s.claims
    assert probing_k8s.deleted == []


# -- route records ------------------------------------------------------------


def test_legacy_route_without_runner_fields_is_reused_when_no_entry_applies(
    probing_k8s: PoolProbingFakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    # A route a pre-upgrade worker wrote: no runner_digests, no runner_docker.
    probing_k8s.create_claim("legacy-claim", pool=BASE_POOL)
    legacy = {
        "thread_key": "T-legacy",
        "claim_name": "legacy-claim",
        "sandbox_name": "sbx-legacy-claim",
        "namespace": "test-ns",
        "service_fqdn": "sbx-legacy-claim.test-ns.svc.cluster.local",
        "port": 8080,
        "session_id": "sess",
        "workspace_repo": "acme-corp/web",
        "state": "live",
    }
    redis_client.set(f"{key_prefix}:route:T-legacy", json.dumps(legacy), ex=60)

    stored = affinity.get("T-legacy")
    assert stored is not None
    assert stored.handle.runner_digests is None
    assert stored.handle.runner_docker is False

    substrate = _substrate(probing_k8s, affinity, config, {REPO: _entry()})
    handle = substrate.claim("T-legacy", workspace_repo="acme-corp/web")
    assert handle.claim_name == "legacy-claim"
    assert substrate.adopt("T-legacy") == handle
    assert probing_k8s.created == ["legacy-claim"]
    assert probing_k8s.deleted == []


def test_route_record_round_trip_keeps_digests_a_tuple() -> None:
    handle = sandbox_types.SandboxHandle(
        thread_key="t",
        claim_name="c",
        sandbox_name="s",
        namespace="n",
        service_fqdn="s.n.svc.cluster.local",
        port=8080,
        session_id="sess",
        runner_digests=DIGESTS,
        runner_docker=True,
    )
    restored = RouteRecord.from_json(RouteRecord(handle=handle).to_json()).handle
    assert restored == handle
    assert restored.runner_digests == DIGESTS
    assert isinstance(restored.runner_digests, tuple)
    assert restored.runner_docker is True
