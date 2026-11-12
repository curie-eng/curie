"""One owner, one native runtime, and no claim crosses owners or modes (#5378, ADR 0268).

The substrate is real over real Valkey; only the Kubernetes control plane is
faked (``FakeSandboxClient``, and ``_FakeApi`` under the real Kubernetes
client). ``_Owner`` stands in for the kernel's side of a personal create: it
records the order the substrate asks for the owner lease, the grant and the
fence, and can refuse or lose the lease on demand. Every grant is a placeholder.
"""

from __future__ import annotations

import copy
import ssl
import time
from dataclasses import dataclass, field, replace
from typing import Any

import pytest
import redis
from curie_worker.sandbox import AffinityStore, SandboxSubstrate, SubstrateConfig
from curie_worker.sandbox.static import StaticPoolSandboxClient, StaticShellSlot
from curie_worker.sandbox.substrate import HISTORY_ENV, SESSION_ENV
from curie_worker.sandbox.types import (
    AUTHENTICATION_REQUIRED,
    ClaimProvenance,
    OwnerBusy,
    PersonalLaunchRefused,
    PersonalModeUnsupported,
    ProvenanceMismatch,
    RouteRecord,
    SandboxHandle,
    SandboxView,
)

from .conftest import FakeSandboxClient, _FakeBundleStore, _RecordingDocker
from .test_k8s_claim import _client, _FakeApi, _seed_chart
from .test_personal_token_claim import _CLAIM, _POOL, _personal_env, _source_spec

THREAD = "slack:C0EXAMPLE1:1700000000.000001"
OTHER_THREAD = "slack:C0EXAMPLE1:1700000000.000002"
TENANT = "00000000-0000-4000-8000-0000000000a1"
PRINCIPAL = "00000000-0000-4000-8000-0000000000b1"
OTHER_PRINCIPAL = "00000000-0000-4000-8000-0000000000b2"
GENERATION = 3
GRANT = "sk-ant-oat01-PLACEHOLDER-substrate-grant"
GRANT_ENV = {"CURIE_MODEL_AUTH_MODE": "personal", "CLAUDE_CODE_OAUTH_TOKEN": GRANT}
OWNER_LABELS = (
    "curietech.ai/model-auth-mode",
    "curietech.ai/owner-tenant",
    "curietech.ai/owner-principal",
    "curietech.ai/enrollment-generation",
)


def _provenance(*, principal: str = PRINCIPAL, generation: int = GENERATION) -> ClaimProvenance:
    return ClaimProvenance(
        owner_tenant_id=TENANT, owner_principal_id=principal, enrollment_generation=generation
    )


@dataclass
class _Owner:
    """The kernel's half of one personal create (``sandbox.types.PersonalCreate``)."""

    provenance: ClaimProvenance = field(default_factory=_provenance)
    steps: list[str] = field(default_factory=list)
    refusal: PersonalLaunchRefused | None = None
    busy: bool = False
    lose_lease: bool = False
    # The fence's fresh authority check refuses (after any predecessor teardown).
    fence_refusal: PersonalLaunchRefused | None = None

    def begin(self) -> None:
        self.steps.append("begin")
        if self.busy:
            raise OwnerBusy()

    def grant_env(self) -> dict[str, str]:
        self.steps.append("grant")
        if self.refusal is not None:
            raise self.refusal
        return dict(GRANT_ENV)

    def fence(self) -> None:
        self.steps.append("fence")
        if self.fence_refusal is not None:
            raise self.fence_refusal
        if self.lose_lease:
            raise OwnerBusy()


def _owner_labels(claim_labels: dict[str, str]) -> dict[str, str]:
    return {name: claim_labels[name] for name in OWNER_LABELS if name in claim_labels}


def _substrate(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> SandboxSubstrate:
    return SandboxSubstrate(fake_k8s, affinity, config)


def test_a_personal_claim_records_its_owner_and_its_grant_reaches_only_that_create(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    owner = _Owner()
    caller_env = {"CURIE_RUNNER_TOKEN": "runner-token-placeholder"}

    handle = substrate.claim(THREAD, env=caller_env, personal=owner)

    [claim] = fake_k8s.claims.values()
    assert _owner_labels(claim.labels) == {
        "curietech.ai/model-auth-mode": "personal",
        "curietech.ai/owner-tenant": TENANT,
        "curietech.ai/owner-principal": PRINCIPAL,
        "curietech.ai/enrollment-generation": str(GENERATION),
    }
    assert {name: claim.env[name] for name in GRANT_ENV} == GRANT_ENV
    # Lease, then the grant (after the owner inventory), then the fence, then the write.
    assert owner.steps == ["begin", "grant", "fence"]
    assert caller_env == {"CURIE_RUNNER_TOKEN": "runner-token-placeholder"}
    assert GRANT not in repr(handle)
    assert GRANT not in RouteRecord(handle=handle).to_json()
    assert all(GRANT not in value for value in claim.labels.values())

    # The same owner's follow-up reuses its live runtime without a second grant read.
    again = _Owner()
    assert substrate.claim(THREAD, personal=again) == handle
    assert again.steps == []
    assert substrate.adopt(THREAD, provenance=_provenance()) == handle
    assert fake_k8s.created == [claim.name]


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (_provenance(generation=GENERATION - 1), _provenance()),
        (_provenance(principal=OTHER_PRINCIPAL), _provenance()),
        (None, _provenance()),
        (_provenance(), None),
    ],
    ids=["older-generation", "another-owner", "unknown-provenance", "service-after-personal"],
)
def test_a_retained_runtime_of_another_owner_generation_or_mode_is_replaced_not_reused(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    first: ClaimProvenance | None,
    second: ClaimProvenance | None,
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    retained = substrate.claim(THREAD, personal=None if first is None else _Owner(provenance=first))
    # Liveness: the retained runtime serves its own provenance.
    assert substrate.adopt(THREAD, provenance=first) == retained

    # Neither adopted nor evicted by a request of another provenance...
    assert substrate.adopt(THREAD, provenance=second) is None
    assert substrate.adopt(THREAD, provenance=first) == retained

    # ...and never reused by its claim: retired, and replaced by its own runtime.
    replacement = substrate.claim(
        THREAD, personal=None if second is None else _Owner(provenance=second)
    )

    assert replacement.claim_name != retained.claim_name
    assert retained.claim_name not in fake_k8s.claims
    assert retained.claim_name in fake_k8s.deleted
    labels = _owner_labels(fake_k8s.claims[replacement.claim_name].labels)
    assert labels == ({} if second is None else _owner_labels(second.labels()))
    assert substrate.adopt(THREAD, provenance=second) == replacement
    assert substrate.adopt(THREAD, provenance=first) is None


@dataclass
class _RacingClient(FakeSandboxClient):
    """Another worker wins the thread's route while this create is in flight."""

    winner: ClaimProvenance | None = None
    affinity: AffinityStore | None = None
    config: SubstrateConfig | None = None
    raced: bool = False

    def create_claim(self, name: str, **kwargs: object) -> None:  # type: ignore[override]
        if not self.raced:
            self.raced = True
            assert self.affinity is not None and self.config is not None
            won = self.config.claim_name_for(THREAD, "winner")
            labels = {} if self.winner is None else self.winner.labels()
            super().create_claim(
                won, pool="test-pool", deadline=time.monotonic() + 30, labels=labels
            )
            provenance = self.winner
            handle = SandboxHandle(
                thread_key=THREAD,
                claim_name=won,
                sandbox_name=f"sbx-{won}",
                namespace="test-ns",
                service_fqdn=f"sbx-{won}.test-ns.svc.cluster.local",
                port=8080,
                session_id="thread-winner",
                model_auth_mode=None if provenance is None else provenance.model_auth_mode,
                owner_tenant_id=None if provenance is None else provenance.owner_tenant_id,
                owner_principal_id=None if provenance is None else provenance.owner_principal_id,
                enrollment_generation=(
                    None if provenance is None else provenance.enrollment_generation
                ),
            )
            assert self.affinity.put_if_absent(THREAD, RouteRecord(handle=handle), 60)
        super().create_claim(name, **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("winner", "loser"),
    [
        (_provenance(generation=GENERATION - 1), _provenance()),
        (None, _provenance()),
        (_provenance(), None),
    ],
    ids=["older-generation-winner", "service-winner", "personal-winner-of-a-service-claim"],
)
def test_a_concurrent_route_winner_of_another_provenance_is_refused_not_adopted(
    affinity: AffinityStore,
    config: SubstrateConfig,
    winner: ClaimProvenance | None,
    loser: ClaimProvenance | None,
) -> None:
    client = _RacingClient(winner=winner, affinity=affinity, config=config)
    substrate = SandboxSubstrate(client, affinity, config)

    with pytest.raises(ProvenanceMismatch) as raised:
        substrate.claim(THREAD, personal=None if loser is None else _Owner(provenance=loser))

    assert raised.value.code == "owner_provenance_mismatch"
    winner_claim = config.claim_name_for(THREAD, "winner")
    # This request's own claim is retired; the winner's runtime is untouched.
    assert set(client.claims) == {winner_claim}
    assert substrate.adopt(THREAD, provenance=winner) is not None


def test_a_concurrent_route_winner_of_the_same_owner_is_adopted(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    client = _RacingClient(winner=_provenance(), affinity=affinity, config=config)
    substrate = SandboxSubstrate(client, affinity, config)

    handle = substrate.claim(THREAD, personal=_Owner())

    assert handle.claim_name == config.claim_name_for(THREAD, "winner")
    assert set(client.claims) == {handle.claim_name}


def test_another_threads_runtime_makes_the_owner_busy_before_any_grant_read(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    first = substrate.claim(THREAD, personal=_Owner())
    # Suspended or live, the other thread's claim holds the owner's one slot.
    substrate.suspend(THREAD, history_ref=None)

    blocked = _Owner()
    with pytest.raises(OwnerBusy):
        substrate.claim(OTHER_THREAD, personal=blocked)

    assert blocked.steps == ["begin"]
    assert set(fake_k8s.claims) == {first.claim_name}
    # Liveness: another owner on that thread is not blocked by this one.
    other = _Owner(provenance=_provenance(principal=OTHER_PRINCIPAL))
    created = substrate.claim(OTHER_THREAD, personal=other)
    assert other.steps == ["begin", "grant", "fence"]
    assert set(fake_k8s.claims) == {first.claim_name, created.claim_name}


def test_a_contended_owner_lease_creates_nothing(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    owner = _Owner(busy=True)

    with pytest.raises(OwnerBusy):
        substrate.claim(THREAD, personal=owner)

    assert owner.steps == ["begin"]
    assert fake_k8s.created == []


def test_a_lost_owner_lease_refuses_with_no_create_write(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    owner = _Owner(lose_lease=True)

    with pytest.raises(OwnerBusy):
        substrate.claim(THREAD, personal=owner)

    assert owner.steps == ["begin", "grant", "fence"]
    assert [op.verb for op in fake_k8s.ops if op.verb == "create_claim"] == []
    assert fake_k8s.claims == {}


def test_a_personal_handoff_retires_its_predecessor_and_sees_it_gone_before_creating(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    old = substrate.claim(THREAD, personal=_Owner())
    fake_k8s.ops.clear()

    new = substrate.handoff(
        THREAD,
        expected=old,
        env={"CURIE_RUNNER_TOKEN": "runner-token-placeholder"},
        workspace_repo=None,
        personal=_Owner(),
    )

    verbs = [
        (op.verb, op.target) for op in fake_k8s.ops if op.verb in {"create_claim", "delete_claim"}
    ]
    # Retire-then-create: never two native runtimes of one owner at once.
    assert verbs[0] == ("delete_claim", old.claim_name)
    assert verbs.index(("create_claim", new.claim_name)) > 0
    assert set(fake_k8s.claims) == {new.claim_name}
    assert substrate.adopt(THREAD, provenance=_provenance()) == new


def test_a_grant_refusal_creates_nothing_and_keeps_the_predecessor(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = _substrate(fake_k8s, affinity, config)
    old = substrate.claim(THREAD, personal=_Owner())
    refused = _Owner(
        refusal=PersonalLaunchRefused("custody_missing", classification=AUTHENTICATION_REQUIRED)
    )

    with pytest.raises(PersonalLaunchRefused) as raised:
        substrate.handoff(THREAD, expected=old, env={}, workspace_repo=None, personal=refused)

    assert raised.value.authentication_required
    assert refused.steps == ["begin", "grant"]
    assert set(fake_k8s.claims) == {old.claim_name}
    assert substrate.adopt(THREAD, provenance=_provenance()) == old


def test_docker_refuses_a_personal_create_before_any_container_call(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    docker = _RecordingDocker(
        image="curie-runner", network="acme-stack", bundle_store=_FakeBundleStore()
    )
    owner = _Owner()

    with pytest.raises(PersonalModeUnsupported) as raised:
        SandboxSubstrate(docker, affinity, config).claim(THREAD, personal=owner)
    with pytest.raises(PersonalModeUnsupported):
        docker.create_claim(
            "claim-direct", pool="pool", env=dict(GRANT_ENV), deadline=time.monotonic() + 30
        )

    assert raised.value.code == "personal_mode_requires_kubernetes"
    assert owner.steps == []
    assert docker.calls == []


def test_a_static_pool_refuses_a_personal_create_before_any_lease(
    redis_client: redis.Redis, key_prefix: str, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    static = StaticPoolSandboxClient(
        slots=[StaticShellSlot("s0", "localhost", 1, "pool")],
        redis_client=redis_client,
        control_token="c" * 40,
        ssl_context=ssl.create_default_context(),
        bundle_store=_FakeBundleStore(),
        key_prefix=key_prefix,
    )
    owner = _Owner()

    with pytest.raises(PersonalModeUnsupported):
        SandboxSubstrate(static, affinity, config).claim(THREAD, personal=owner)
    with pytest.raises(PersonalModeUnsupported):
        static.create_claim(
            "claim-direct", pool="pool", env=dict(GRANT_ENV), deadline=time.monotonic() + 30
        )

    assert owner.steps == []
    assert list(redis_client.scan_iter(f"{key_prefix}:static*")) == []


def test_kubernetes_refuses_owner_labels_without_the_personal_template() -> None:
    api = _FakeApi()
    _seed_chart(api, spec=_source_spec())
    pristine = copy.deepcopy(api.objects)
    labels = _provenance().labels()

    with pytest.raises(ValueError, match="personal mode env"):
        _client(api).create_claim(
            _CLAIM,
            pool=_POOL,
            env={"CURIE_RUNNER_TOKEN": "runner-token-placeholder"},
            labels=labels,
            deadline=time.monotonic() + 300,
        )
    assert api.objects == pristine

    # Liveness: with the personal pair the claim is created, labelled, and the
    # grant is only in the claim's own tokens Secret.
    _client(api).create_claim(
        _CLAIM, pool=_POOL, env=_personal_env(), labels=labels, deadline=time.monotonic() + 300
    )
    claim = api.objects[("sandboxclaims", _CLAIM)]
    assert {name: claim["metadata"]["labels"][name] for name in OWNER_LABELS} == labels
    assert "personal-grant-placeholder" not in str(claim)
    assert (
        api.secrets[f"{_CLAIM}-tokens"]["stringData"]["CLAUDE_CODE_OAUTH_TOKEN"]
        == "personal-grant-placeholder"
    )


# --- Session identity, refusals and teardown across handoff and resume (r1) ------

_RETAINED_ENV = {
    SESSION_ENV: "session-of-the-retained-runtime",
    HISTORY_ENV: "https://state.example/transcript/retained",
}
_SUSPENDED_HISTORY = "https://state.example/transcript/suspended"
_FRESH_ENV = {"CURIE_RUNNER_TOKEN": "runner-token-placeholder"}

_PROVENANCE_PAIRS = [
    pytest.param(_provenance(), _provenance(), True, id="same-owner"),
    pytest.param(None, None, True, id="service"),
    pytest.param(
        _provenance(generation=GENERATION - 1), _provenance(), False, id="older-generation"
    ),
    pytest.param(_provenance(principal=OTHER_PRINCIPAL), _provenance(), False, id="another-owner"),
    pytest.param(None, _provenance(), False, id="personal-after-service-or-unknown"),
    pytest.param(_provenance(), None, False, id="service-after-personal"),
]


def _as(provenance: ClaimProvenance | None) -> _Owner | None:
    return None if provenance is None else _Owner(provenance=provenance)


@pytest.mark.parametrize(("first", "second", "inherits"), _PROVENANCE_PAIRS)
def test_a_handoff_hands_on_the_session_only_within_one_provenance(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    first: ClaimProvenance | None,
    second: ClaimProvenance | None,
    inherits: bool,
) -> None:
    """An idle runtime replaced through ``handoff`` passes its session and
    history on only to a runtime of its own provenance. Another owner,
    generation or mode, or unknown provenance, gets a new session identity,
    as ``claim()`` gives it, and never boots with the retained transcript."""

    substrate = _substrate(fake_k8s, affinity, config)
    retained = substrate.claim(THREAD, env=dict(_RETAINED_ENV), personal=_as(first))
    assert (retained.session_id, retained.history_ref) == tuple(_RETAINED_ENV.values())

    new = substrate.handoff(
        THREAD, expected=retained, env=dict(_FRESH_ENV), workspace_repo=None, personal=_as(second)
    )

    boot = fake_k8s.claims[new.claim_name].env
    if inherits:
        assert (new.session_id, new.history_ref) == (retained.session_id, retained.history_ref)
        assert (boot[SESSION_ENV], boot[HISTORY_ENV]) == tuple(_RETAINED_ENV.values())
    else:
        assert new.session_id != retained.session_id
        assert new.history_ref is None
        assert SESSION_ENV not in boot and HISTORY_ENV not in boot
    # Either way the route names the replacement and the retained runtime is gone.
    assert substrate.adopt(THREAD, provenance=second) == new
    assert set(fake_k8s.claims) == {new.claim_name}


@pytest.mark.parametrize(("first", "second", "inherits"), _PROVENANCE_PAIRS)
def test_a_resume_hands_on_the_session_only_within_one_provenance(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    first: ClaimProvenance | None,
    second: ClaimProvenance | None,
    inherits: bool,
) -> None:
    """A suspended runtime resumes its session and history only for a request
    of its own provenance. A plain service turn on a thread holding a
    suspended personal claim, and a personal request on a service or
    unknown-provenance one, boot a new session instead."""

    substrate = _substrate(fake_k8s, affinity, config)
    retained = substrate.claim(THREAD, env=dict(_RETAINED_ENV), personal=_as(first))
    substrate.suspend(THREAD, history_ref=_SUSPENDED_HISTORY)

    resumed = substrate.resume(THREAD, env=dict(_FRESH_ENV), personal=_as(second))

    boot = fake_k8s.claims[resumed.claim_name].env
    if inherits:
        assert (resumed.session_id, resumed.history_ref) == (
            retained.session_id,
            _SUSPENDED_HISTORY,
        )
        assert (boot[SESSION_ENV], boot[HISTORY_ENV]) == (retained.session_id, _SUSPENDED_HISTORY)
    else:
        assert resumed.session_id != retained.session_id
        assert resumed.history_ref is None
        assert SESSION_ENV not in boot and HISTORY_ENV not in boot
    assert set(fake_k8s.claims) == {resumed.claim_name}
    assert substrate.adopt(THREAD, provenance=second) == resumed


@pytest.mark.parametrize(
    ("step", "code", "retired"),
    [
        pytest.param("busy", None, False, id="owner-busy"),
        pytest.param("refusal", "custody_missing", False, id="grant-refused"),
        pytest.param("refusal", "enrollment_inactive", False, id="authorization-refused"),
        pytest.param(
            "fence_refusal", "enrollment_generation_mismatch", True, id="refused-after-teardown"
        ),
    ],
)
def test_a_refused_personal_resume_keeps_the_suspended_route_for_a_later_resume(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    step: str,
    code: str | None,
    retired: bool,
) -> None:
    """A personal resume takes the owner slot and reads the grant before it
    deletes anything. Refused there, the suspended claim and its route stay
    exactly as they were; refused by the fence after the teardown, the claim
    is gone but the route (session and history) stays. Either way the thread
    resumes its own session once the owner is authorized again."""

    substrate = _substrate(fake_k8s, affinity, config)
    retained = substrate.claim(THREAD, env=dict(_RETAINED_ENV), personal=_Owner())
    substrate.suspend(THREAD, history_ref=_SUSPENDED_HISTORY)
    suspended = affinity.get(THREAD)
    refusing: dict[str, Any] = (
        {"busy": True}
        if code is None
        else {step: PersonalLaunchRefused(code, classification=AUTHENTICATION_REQUIRED)}
    )

    with pytest.raises((OwnerBusy, PersonalLaunchRefused)):
        substrate.resume(THREAD, env=dict(_FRESH_ENV), personal=_Owner(**refusing))

    assert fake_k8s.created == [retained.claim_name]
    assert affinity.get(THREAD) == suspended
    assert (retained.claim_name in fake_k8s.claims) is not retired

    # Liveness: authorized again, the same thread resumes its own session.
    resumed = substrate.resume(THREAD, env=dict(_FRESH_ENV), personal=_Owner())
    assert (resumed.session_id, resumed.history_ref) == (retained.session_id, _SUSPENDED_HISTORY)
    assert set(fake_k8s.claims) == {resumed.claim_name}


@dataclass
class _LingeringClient(FakeSandboxClient):
    """Kubernetes deletion as it is: a deleted claim is gone at once while its
    sandbox stays for ``linger_reads`` more reads (None: for good). Every
    claim create records the sandboxes still lingering then in ``overlaps``."""

    linger_reads: int | None = 3
    lingering: dict[str, int | None] = field(default_factory=dict)
    overlaps: list[str] = field(default_factory=list)

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        claim = self.claims.get(name)
        sandbox = None if claim is None else self.sandboxes.get(claim.sandbox_name)
        super().delete_claim(name, request_timeout_seconds=request_timeout_seconds)
        if sandbox is not None:
            self.sandboxes[sandbox.name] = sandbox
            self.lingering[sandbox.name] = self.linger_reads

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        if name in self.lingering:
            left = self.lingering[name]
            if left is not None and left <= 0:
                del self.lingering[name]
                self.sandboxes.pop(name, None)
            elif left is not None:
                self.lingering[name] = left - 1
        return super().get_sandbox(name, request_timeout_seconds=request_timeout_seconds)

    def create_claim(self, name: str, **kwargs: Any) -> None:
        self.overlaps.extend(sorted(self.lingering))
        super().create_claim(name, **kwargs)


def _replace_by_resume(substrate: SandboxSubstrate, client: _LingeringClient) -> SandboxHandle:
    substrate.claim(THREAD, personal=_Owner())
    substrate.suspend(THREAD, history_ref=_SUSPENDED_HISTORY)
    return substrate.resume(THREAD, env=dict(_FRESH_ENV), personal=_Owner())


def _replace_by_claim(substrate: SandboxSubstrate, client: _LingeringClient) -> SandboxHandle:
    # The owner's runtime of an older generation is retired by the next claim.
    substrate.claim(THREAD, personal=_Owner(provenance=_provenance(generation=GENERATION - 1)))
    return substrate.claim(THREAD, personal=_Owner())


def _replace_after_adopt(substrate: SandboxSubstrate, client: _LingeringClient) -> SandboxHandle:
    # Its claim stops binding: adopt refuses the stale route, the claim replaces it.
    old = substrate.claim(THREAD, personal=_Owner())
    client.claims[old.claim_name].ready = False
    assert substrate.adopt(THREAD, provenance=_provenance()) is None
    return substrate.claim(THREAD, personal=_Owner())


@pytest.mark.parametrize(
    "replace_runtime",
    [
        pytest.param(_replace_by_resume, id="resume"),
        pytest.param(_replace_by_claim, id="stale-route-eviction"),
        pytest.param(_replace_after_adopt, id="adopt-then-claim"),
    ],
)
@pytest.mark.parametrize("linger_reads", [3, None], ids=["sandbox-goes", "sandbox-stays"])
def test_a_predecessor_whose_claim_goes_before_its_sandbox_never_overlaps_the_create(
    affinity: AffinityStore,
    config: SubstrateConfig,
    replace_runtime: Any,
    linger_reads: int | None,
) -> None:
    """ADR 0268 decision 2. The owner's earlier runtime on this thread is
    deleted while its sandbox outlives its claim. The successor is created
    only after both are observed gone (the create waits), or not at all when
    the sandbox stays past the bound (``OwnerBusy``): never two at once."""

    client = _LingeringClient(linger_reads=linger_reads)
    bounded = replace(config, release_gone_timeout_seconds=0.2)
    substrate = SandboxSubstrate(client, affinity, bounded)

    if linger_reads is None:
        with pytest.raises(OwnerBusy):
            replace_runtime(substrate, client)
        assert len(client.created) == 1
    else:
        new = replace_runtime(substrate, client)
        # Liveness: the successor exists, routed, and its predecessor is gone.
        assert len(client.created) == 2
        assert set(client.claims) == {new.claim_name}
        assert set(client.sandboxes) == {new.sandbox_name}
        assert substrate.adopt(THREAD, provenance=_provenance()) == new
    assert client.overlaps == []
