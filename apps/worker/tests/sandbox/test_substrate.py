"""SandboxSubstrate lifecycle logic against real Valkey + the in-memory
cluster fake (the K8s control plane is the one external service faked here;
the real cluster path is the e2e in test_e2e_k8scratch.py)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest
from aci_protocol import BootEnv
from curie_worker.sandbox import (
    AffinityStore,
    CapacityExhaustedError,
    ClaimTimeoutError,
    ClaimView,
    NoRouteError,
    QuotaRejection,
    RouteRecord,
    RouteState,
    SandboxHandle,
    SandboxSubstrate,
    SandboxView,
    SubstrateConfig,
    SuspendedThreadError,
)
from curie_worker.sandbox.k8s import _claim_view

from .conftest import FakeClaim, FakeSandbox, FakeSandboxClient

# Named from the boot contract, NOT re-imported from the module under test:
# asserting against the substrate's own constant passes under any rename on
# either side, so it proves the resume env agrees with itself rather than with
# the runner that has to read it (#488).
HISTORY_ENV = BootEnv.env_key("history_ref")
SESSION_ENV = BootEnv.env_key("session_id")


@pytest.fixture
def substrate(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> SandboxSubstrate:
    return SandboxSubstrate(fake_k8s, affinity, config)


def test_claim_binds_and_routes_thread(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    handle = substrate.claim("1700000000.000100")

    assert handle.sandbox_name.startswith("sbx-curie-thread-")
    assert handle.service_fqdn.endswith(".svc.cluster.local")
    assert handle.base_url == f"http://{handle.service_fqdn}:8080"
    assert fake_k8s.claims[handle.claim_name].env == {}

    # Same thread claims again -> same binding, no second claim created.
    again = substrate.claim("1700000000.000100")
    assert again == handle
    assert len(fake_k8s.created) == 1

    # A different thread gets a different sandbox (no cross-talk).
    other = substrate.claim("1700000000.000999")
    assert other.sandbox_name != handle.sandbox_name


def test_lookup_returns_none_when_sandbox_gone(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    handle = substrate.claim("T1")
    assert substrate.lookup("T1") == handle

    # Cluster-side deletion out from under the route (node loss, manual kill).
    fake_k8s.sandboxes.pop(handle.sandbox_name)
    assert substrate.lookup("T1") is None


def test_adopt_reuses_only_a_ready_route_and_never_cold_claims(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    """Workspace callers must reprepare rather than bind a stale workspace ref."""

    handle = substrate.claim("T1")
    created_before_adoption = list(fake_k8s.created)

    assert substrate.adopt("T1") == handle
    assert fake_k8s.created == created_before_adoption

    fake_k8s.sandboxes.pop(handle.sandbox_name)

    assert substrate.adopt("T1") is None
    assert fake_k8s.created == created_before_adoption


def test_claim_timeout_cleans_up_claim(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    fake_k8s.bind_ready = False
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    with pytest.raises(ClaimTimeoutError):
        substrate.claim("T1")
    # The unbound claim is not leaked and no route was recorded.
    assert fake_k8s.deleted == fake_k8s.created
    assert affinity.get("T1") is None


def test_quota_rejection_fails_promptly_and_cleans_up_claim(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    """#1534: a persisting ResourceQuota rejection is terminal at the next
    poll, not after claim_timeout_seconds. One extra poll is the debounce so a
    single-blip rejection can still bind (see
    test_transient_quota_rejection_can_clear_before_claim_binds)."""

    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="8",
        hard="8",
    )
    fake_k8s.quota_rejection = rejection
    original_get_claim = fake_k8s.get_claim
    poll_count = 0

    def get_claim_with_updated_rejection(name: str) -> ClaimView | None:
        nonlocal poll_count
        view = original_get_claim(name)
        if view is None:
            return None
        poll_count += 1
        if poll_count == 1:
            return replace(
                view,
                quota_rejection=replace(rejection, used="7"),
            )
        return view

    fake_k8s.get_claim = get_claim_with_updated_rejection  # type: ignore[method-assign]
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    started = time.monotonic()
    with pytest.raises(CapacityExhaustedError) as excinfo:
        substrate.claim("T1")
    elapsed = time.monotonic() - started

    assert elapsed < 20 * config.poll_interval_seconds
    assert excinfo.value.rejection == rejection
    assert excinfo.value.rejection.quota_name == "curie-sandbox-quota"
    assert excinfo.value.rejection.resource == "limits.cpu"
    assert excinfo.value.rejection.requested == "1"
    assert excinfo.value.rejection.used == "8"
    assert excinfo.value.rejection.hard == "8"
    assert poll_count >= 2
    assert fake_k8s.deleted == fake_k8s.created
    assert affinity.get("T1") is None


def test_transient_quota_rejection_can_clear_before_claim_binds(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="8",
        hard="8",
    )
    original_get_claim = fake_k8s.get_claim
    poll_count = 0

    def get_claim_after_transient_rejection(name: str) -> ClaimView | None:
        nonlocal poll_count
        view = original_get_claim(name)
        if view is None:
            return None
        poll_count += 1
        if poll_count == 1:
            return replace(
                view,
                ready=False,
                sandbox_name=None,
                quota_rejection=rejection,
                ready_reason="ReconcilerError",
                ready_message="temporary ResourceQuota rejection",
            )
        return view

    fake_k8s.get_claim = get_claim_after_transient_rejection  # type: ignore[method-assign]
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    handle = substrate.claim("T1")

    assert poll_count >= 2
    assert handle.sandbox_name in fake_k8s.sandboxes
    assert affinity.get("T1") is not None


def test_later_non_quota_condition_replaces_earlier_quota_evidence(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="8",
        hard="8",
    )
    fake_k8s.bind_ready = False
    fake_k8s.ready_reason = "ReconcilerError"
    fake_k8s.ready_message = "later nonquota condition"
    original_get_claim = fake_k8s.get_claim
    poll_count = 0

    def get_claim_after_quota_rejection(name: str) -> ClaimView | None:
        nonlocal poll_count
        view = original_get_claim(name)
        if view is None:
            return None
        poll_count += 1
        if poll_count == 1:
            return replace(
                view,
                quota_rejection=rejection,
                ready_message="earlier quota rejection",
            )
        return view

    fake_k8s.get_claim = get_claim_after_quota_rejection  # type: ignore[method-assign]
    short_config = replace(config, claim_timeout_seconds=0.05)
    substrate = SandboxSubstrate(fake_k8s, affinity, short_config)

    with pytest.raises(ClaimTimeoutError) as excinfo:
        substrate.claim("T1")

    message = str(excinfo.value)
    assert poll_count >= 2
    assert "later nonquota condition" in message
    assert "earlier quota rejection" not in message


def test_lost_race_adopts_winner_and_retires_loser(
    substrate: SandboxSubstrate,
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
) -> None:
    # A competing worker recorded a route for T1 between our create and put.
    winner_handle = SandboxHandle(
        thread_key="T1",
        claim_name="claim-winner",
        sandbox_name="sbx-claim-winner",
        namespace="test-ns",
        service_fqdn="sbx-claim-winner.test-ns.svc.cluster.local",
        port=8080,
        session_id="sess-w",
    )
    original_create = fake_k8s.create_claim
    # The winner's sandbox really exists (adoption requires a live winner).
    fake_k8s.claims["claim-winner"] = FakeClaim(
        name="claim-winner", env={}, labels={}, sandbox_name="sbx-claim-winner"
    )
    fake_k8s.sandboxes["sbx-claim-winner"] = FakeSandbox(
        name="sbx-claim-winner",
        service_fqdn="sbx-claim-winner.test-ns.svc.cluster.local",
    )

    def create_then_lose(name: str, **kwargs: object) -> None:
        original_create(name, **kwargs)  # type: ignore[arg-type]
        affinity.put_if_absent("T1", RouteRecord(handle=winner_handle), ttl_seconds=60)

    fake_k8s.create_claim = create_then_lose  # type: ignore[method-assign]

    handle = substrate.claim("T1")
    assert handle == winner_handle
    # The loser's claim was deleted, not leaked; the winner's was kept.
    assert "claim-winner" not in fake_k8s.deleted
    assert len(fake_k8s.deleted) == 1


def test_suspend_resume_rehydrates_from_history(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient, affinity: AffinityStore
) -> None:
    first = substrate.claim("T1")
    substrate.suspend("T1", history_ref="sdk-session-abc")

    # Suspended: mode flipped, route no longer live.
    assert fake_k8s.sandboxes[first.sandbox_name].operating_mode == "Suspended"
    record = affinity.get("T1")
    assert record is not None and record.state is RouteState.SUSPENDED
    assert substrate.lookup("T1") is None
    # A claim() while suspended must not silently fork a second live session
    # for the thread without the history; the kernel resumes explicitly.

    resumed = substrate.resume("T1")
    assert resumed.claim_name != first.claim_name
    assert resumed.session_id == first.session_id
    assert resumed.history_ref == "sdk-session-abc"
    # The new claim injects the rehydrate env for the replacement runner.
    env = fake_k8s.claims[resumed.claim_name].env
    assert env[HISTORY_ENV] == "sdk-session-abc"
    assert env[SESSION_ENV] == first.session_id
    # Old claim retired; route is live again on the new claim.
    assert first.claim_name in fake_k8s.deleted
    assert substrate.lookup("T1") == resumed


def test_suspend_and_resume_require_route(substrate: SandboxSubstrate) -> None:
    with pytest.raises(NoRouteError):
        substrate.suspend("nope", history_ref=None)
    with pytest.raises(NoRouteError):
        substrate.resume("nope")


def test_release_deletes_claim_and_route(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient, affinity: AffinityStore
) -> None:
    handle = substrate.claim("T1")
    assert substrate.release("T1")
    assert handle.claim_name not in fake_k8s.claims
    assert handle.sandbox_name not in fake_k8s.sandboxes
    assert affinity.get("T1") is None
    assert not substrate.release("T1")


def test_reap_orphans_deletes_unrouted_claims(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient, affinity: AffinityStore
) -> None:
    live = substrate.claim("T-live")
    orphan = substrate.claim("T-orphan")
    # The orphan's route expires (simulated by guarded delete), its claim stays.
    affinity.delete_if_claim("T-orphan", orphan.claim_name)
    # An idle thread's route expires long after its claim was created, so the
    # orphan is aged well past the reaper's bind-window grace. Without this the
    # claim is milliseconds old, which is indistinguishable from a claim still
    # binding, and "no route" alone does not mean "litter".
    fake_k8s.claims[orphan.claim_name].created_at = datetime.now(UTC) - timedelta(seconds=33.0)
    # The survivor is aged past the grace too, so its survival can only be
    # explained by its live route. Left milliseconds old it would be spared by
    # AGE, and dropping the live-route exclusion entirely would still pass
    # here while deleting a genuinely routed claim in production.
    fake_k8s.claims[live.claim_name].created_at = datetime.now(UTC) - timedelta(seconds=33.0)

    reaped = substrate.reap_orphans()
    assert reaped == [orphan.claim_name]
    assert orphan.claim_name not in fake_k8s.claims
    assert live.claim_name in fake_k8s.claims
    assert live.claim_name not in fake_k8s.deleted
    # Reap is idempotent.
    assert substrate.reap_orphans() == []


@dataclass
class _ReapDuringBindClient(FakeSandboxClient):
    """A fake that runs one reaper tick from inside the bind window.

    ``_await_bound`` issues its first ``get_claim`` poll immediately after
    ``create_claim`` returns, which is precisely the window in which
    ``_claim_fresh`` has not written the thread's route yet. Reaping from that
    poll reproduces "the periodic maintenance tick landed mid-claim" -- the
    production race -- synchronously: no threads, no sleeps, no wall clock.
    """

    substrate: SandboxSubstrate | None = None
    reaped: bool = False
    reap_results: list[list[str]] = field(default_factory=list)

    def get_claim(self, name: str) -> ClaimView | None:
        if self.substrate is not None and not self.reaped:
            # Flagged before reaping: reap_orphans() calls list_claims(), which
            # re-enters get_claim on this same fake.
            self.reaped = True
            self.reap_results.append(self.substrate.reap_orphans())
            view = super().get_claim(name)  # the claim has not bound yet
            claim = self.claims.get(name)
            if claim is not None:
                claim.ready = True  # the controller binds right after the tick
            return view
        return super().get_claim(name)


def test_reap_orphans_spares_an_in_flight_claim(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # A claim that is still binding has no route yet, so "no live route names
    # it" is ambiguous, not proof of litter. Reaping it deletes a live sandbox
    # out from under the blocked creator, which then polls a claim that is gone
    # until its deadline and reports a bind timeout.
    fake_k8s = _ReapDuringBindClient(bind_ready=False)
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    fake_k8s.substrate = substrate

    handle = substrate.claim("T1")

    assert fake_k8s.reap_results == [[]]  # the tick found nothing reapable
    assert handle.claim_name in fake_k8s.claims  # the claim survived
    # Never deleted at all, as opposed to deleted and re-created.
    assert handle.claim_name not in fake_k8s.deleted


@dataclass
class _ReapAfterBindClient(FakeSandboxClient):
    """A fake that runs one reaper tick AFTER the claim has bound, while the
    substrate is still waiting for the sandbox's dial target.

    ``_claim_fresh`` writes the thread's route only once both phases finish, so
    between ``_await_bound`` returning and ``_await_service_fqdn`` succeeding
    the claim is READY and routeless at the same time. This fake reaps from the
    first ``get_sandbox`` poll and withholds the serviceFQDN on that same poll,
    which is that window exactly: the claim reports ready, the sandbox exists,
    and no route names it yet.
    """

    substrate: SandboxSubstrate | None = None
    reaped: bool = False
    reap_results: list[list[str]] = field(default_factory=list)
    ready_at_reap: list[bool] = field(default_factory=list)

    def get_sandbox(self, name: str) -> SandboxView | None:
        view = super().get_sandbox(name)
        if view is None or self.substrate is None or self.reaped:
            return view
        self.reaped = True
        self.ready_at_reap = [claim.ready for claim in self.claims.values()]
        self.reap_results.append(self.substrate.reap_orphans())
        # The dial target arrives on a later poll, so this one hands back a
        # bound sandbox with no address, keeping the claim routeless.
        return SandboxView(
            name=view.name,
            ready=view.ready,
            service_fqdn=None,
            operating_mode=view.operating_mode,
            port=view.port,
        )


def test_reap_orphans_spares_a_bound_claim_awaiting_its_route(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # The spare must not hinge on the claim being unbound. A claim that has
    # already bound but is still waiting on its service address, or on the
    # route write that follows it, is equally live and equally routeless;
    # reaping it kills a running sandbox out from under its blocked creator.
    fake_k8s = _ReapAfterBindClient()
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    fake_k8s.substrate = substrate

    handle = substrate.claim("T1")

    # The tick really did run against a BOUND claim, not an unbound one.
    assert fake_k8s.ready_at_reap == [True]
    assert fake_k8s.reap_results == [[]]  # the tick found nothing reapable
    assert handle.claim_name in fake_k8s.claims  # the claim survived
    # Never deleted at all, as opposed to deleted and re-created.
    assert handle.claim_name not in fake_k8s.deleted


def test_reap_orphans_still_deletes_a_claim_older_than_the_grace(
    substrate: SandboxSubstrate,
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = substrate.claim("T-old")
    boundary = substrate.claim("T-boundary")
    affinity.delete_if_claim("T-old", old.claim_name)
    affinity.delete_if_claim("T-boundary", boundary.claim_name)

    # The reaper's wall-clock reference is pinned so that "exactly at the
    # grace" is a real boundary rather than a race against test execution time.
    frozen = datetime.now(UTC)

    class _FrozenClock:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return frozen

    monkeypatch.setattr("curie_worker.sandbox.substrate.datetime", _FrozenClock)

    # The grace is the config fixture's claim_timeout_seconds (2.0) plus the
    # reaper's fixed margin (30.0), spelled as literals: importing the module
    # under test's own constant would keep this green under any margin value,
    # including 0, which is the defect itself.
    fake_k8s.claims[old.claim_name].created_at = frozen - timedelta(seconds=33.0)
    fake_k8s.claims[boundary.claim_name].created_at = frozen - timedelta(seconds=32.0)

    assert substrate.reap_orphans() == [old.claim_name]
    assert old.claim_name not in fake_k8s.claims
    # Exactly at the grace is spared: ties go to the creator.
    assert boundary.claim_name in fake_k8s.claims


def test_reap_grace_scales_with_the_configured_claim_timeout(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # The grace is claim_timeout_seconds PLUS a fixed margin, so raising the
    # configured timeout must spare an older claim. Every other reaper test
    # runs at the one fixture timeout, where a cutoff that hardcodes the
    # resulting grace and ignores claim_timeout_seconds is indistinguishable
    # from the real rule. Two substrates differing only in that value, against
    # one age between their two graces, is what separates them: 2.0 gives a
    # grace of 32.0 and 10.0 gives 40.0, and the claim is aged 35.0. Literals
    # throughout, because importing the substrate's own margin constant would
    # stay green under any value it is given, including one that drops the
    # scaling.
    patient_config = replace(config, claim_timeout_seconds=10.0)
    age = timedelta(seconds=35.0)

    strict_k8s = FakeSandboxClient()
    strict = SandboxSubstrate(strict_k8s, affinity, config)
    strict_claim = strict.claim("T-strict")
    affinity.delete_if_claim("T-strict", strict_claim.claim_name)
    strict_k8s.claims[strict_claim.claim_name].created_at = datetime.now(UTC) - age

    patient_k8s = FakeSandboxClient()
    patient = SandboxSubstrate(patient_k8s, affinity, patient_config)
    patient_claim = patient.claim("T-patient")
    affinity.delete_if_claim("T-patient", patient_claim.claim_name)
    patient_k8s.claims[patient_claim.claim_name].created_at = datetime.now(UTC) - age

    # Past the short config's grace of 32.0: litter, reaped.
    assert strict.reap_orphans() == [strict_claim.claim_name]
    assert strict_claim.claim_name not in strict_k8s.claims
    # The very same age, inside the long config's grace of 40.0: a creator can
    # still be waiting on it, so it is spared.
    assert patient.reap_orphans() == []
    assert patient_claim.claim_name in patient_k8s.claims
    assert patient_claim.claim_name not in patient_k8s.deleted


def test_reap_orphans_skips_and_warns_on_a_claim_of_unknown_age(
    substrate: SandboxSubstrate,
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    orphan = substrate.claim("T-unknown")
    affinity.delete_if_claim("T-unknown", orphan.claim_name)
    # An adapter that cannot report a creation instant: a parse defect, a
    # malformed cluster answer, a future third adapter.
    fake_k8s.claims[orphan.claim_name].created_at = None

    # A second orphan, plainly reapable, created AFTER the unknown-age one so
    # the sweep reaches it only by continuing past that claim. One unreadable
    # claim must cost exactly itself; skipping the rest of the sweep would let
    # a single malformed answer stall reaping fleet-wide.
    stale = substrate.claim("T-stale")
    affinity.delete_if_claim("T-stale", stale.claim_name)
    fake_k8s.claims[stale.claim_name].created_at = datetime.now(UTC) - timedelta(seconds=33.0)

    with caplog.at_level(logging.WARNING, logger="curie_worker.sandbox.substrate"):
        assert substrate.reap_orphans() == [stale.claim_name]

    # Unknown age is never reaped: an unreaped claim is recoverable litter, a
    # wrongly reaped one is a killed live sandbox.
    assert orphan.claim_name in fake_k8s.claims
    assert stale.claim_name not in fake_k8s.claims
    # And the skip is observable. Without a signal, one adapter bug turns
    # orphan reaping off entirely and nothing anywhere reports it. Asserted on
    # the claim name, not the sentence, so a reworded message still passes
    # while a deleted or downgraded log line does not.
    warned = [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and orphan.claim_name in record.getMessage()
    ]
    assert warned, "an unknown-age skip must name the claim at WARNING"


def test_claim_rebinds_when_sandbox_died_under_live_route(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient, affinity: AffinityStore
) -> None:
    first = substrate.claim("T1")
    # Cluster-side death out from under the route (node loss, manual kill):
    # the stale route must not win the re-claim race and hand back a dead
    # handle, and the stale claim must be retired.
    fake_k8s.sandboxes.pop(first.sandbox_name)

    second = substrate.claim("T1")
    assert second.claim_name != first.claim_name
    assert second.sandbox_name in fake_k8s.sandboxes
    assert first.claim_name in fake_k8s.deleted
    assert substrate.lookup("T1") == second


def test_claim_race_never_adopts_dead_winner(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient, affinity: AffinityStore
) -> None:
    # A competing route lands mid-claim but its sandbox is already gone; the
    # claimer must clear the stale route and bind fresh, never return a handle
    # to a nonexistent sandbox.
    dead = SandboxHandle(
        thread_key="T1",
        claim_name="claim-dead",
        sandbox_name="sbx-claim-dead",
        namespace="test-ns",
        service_fqdn="sbx-claim-dead.test-ns.svc.cluster.local",
        port=8080,
        session_id="sess-d",
    )
    original_create = fake_k8s.create_claim
    injected = False

    def create_then_race(name: str, **kwargs: object) -> None:
        nonlocal injected
        original_create(name, **kwargs)  # type: ignore[arg-type]
        if not injected:
            injected = True
            affinity.put_if_absent("T1", RouteRecord(handle=dead), ttl_seconds=60)

    fake_k8s.create_claim = create_then_race  # type: ignore[method-assign]

    handle = substrate.claim("T1")
    assert handle.sandbox_name in fake_k8s.sandboxes
    assert "claim-dead" in fake_k8s.deleted
    assert substrate.lookup("T1") == handle


class _SlowBindNoFqdnClient(FakeSandboxClient):
    """A fake whose claim binds only after a wall-clock threshold and whose
    bound sandbox never gets a serviceFQDN.

    The bind readiness is TIME-based (measured against ``time.monotonic()``),
    not iteration-count-based, so poll jitter cannot starve phase 1 -- the claim
    reports ready once ``bind_after_seconds`` of real time has elapsed since the
    first create_claim, regardless of how many polls happened. serviceFQDN is
    always empty, so phase 2 (await_service_fqdn) can only ever time out.
    """

    bind_after_seconds: float = 1.2
    _bind_deadline: float | None = None

    def create_claim(self, name: str, **kwargs: object) -> None:
        if self._bind_deadline is None:
            self._bind_deadline = time.monotonic() + self.bind_after_seconds
        super().create_claim(name, **kwargs)  # type: ignore[arg-type]

    def get_claim(self, name: str) -> ClaimView | None:
        claim = self.claims.get(name)
        if claim is None:
            return None
        ready = self._bind_deadline is not None and time.monotonic() >= self._bind_deadline
        return ClaimView(
            name=claim.name,
            ready=ready,
            sandbox_name=claim.sandbox_name if ready else None,
            created_at=claim.created_at,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
        )

    def get_sandbox(self, name: str) -> SandboxView | None:
        view = super().get_sandbox(name)
        if view is None:
            return None
        return SandboxView(
            name=view.name,
            ready=view.ready,
            service_fqdn="",
            operating_mode=view.operating_mode,
        )


def test_claim_budget_is_end_to_end_across_bind_and_fqdn(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    # The bind + FQDN phases must share ONE end-to-end deadline equal to
    # claim_timeout_seconds (2.0s), not a fresh 2.0s each. Bind takes ~1.2s of
    # wall clock, then FQDN never arrives. Under a shared budget the whole claim
    # aborts at ~2.0s; under per-phase budgets it runs ~1.2 + 2.0 = ~3.2s.
    # We assert < 2.6 (0.6s of slack over the 2.0 target) so CI jitter cannot
    # flip a correctly-budgeted run, while ~3.2s of per-phase behavior stays red.
    fake_k8s = _SlowBindNoFqdnClient()
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    started = time.monotonic()
    with pytest.raises(ClaimTimeoutError):
        substrate.claim("T1")
    elapsed = time.monotonic() - started

    assert elapsed < 2.6
    # The unbound claim is not leaked despite the timeout.
    assert fake_k8s.deleted == fake_k8s.created


def test_claim_timeout_error_names_the_budget(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    fake_k8s.bind_ready = False
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    with pytest.raises(ClaimTimeoutError) as excinfo:
        substrate.claim("T1")
    # The error message names the configured budget so the signature change
    # (one shared deadline) does not silently drop the timeout value.
    message = str(excinfo.value)
    assert str(config.claim_timeout_seconds) in message
    assert "no ready condition was observed" in message.lower()
    assert "pod was created" not in message.lower()
    assert "cpu-saturated" not in message.lower()
    assert "kubectl top" not in message.lower()


def test_non_quota_reconciler_error_stays_on_slow_bind_path(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    view = _claim_view(
        {
            "metadata": {"name": "curie-thread-example"},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "False",
                        "reason": "ReconcilerError",
                        "message": (
                            'Error seen: pods "curie-thread-example" is forbidden: User '
                            '"system:serviceaccount:curie1572:worker" cannot create resource '
                            '"pods"'
                        ),
                    }
                ]
            },
        }
    )
    assert view.quota_rejection is None
    fake_k8s.bind_ready = False
    fake_k8s.quota_rejection = view.quota_rejection
    fake_k8s.ready_reason = view.ready_reason
    fake_k8s.ready_message = view.ready_message
    original_get_claim = fake_k8s.get_claim
    poll_count = 0

    def get_claim_with_updated_condition(name: str) -> ClaimView | None:
        nonlocal poll_count
        current = original_get_claim(name)
        if current is None:
            return None
        poll_count += 1
        if poll_count == 1:
            return replace(
                current,
                ready_reason="Provisioning",
                ready_message="earlier condition",
            )
        return current

    fake_k8s.get_claim = get_claim_with_updated_condition  # type: ignore[method-assign]
    short_config = replace(config, claim_timeout_seconds=0.05)
    substrate = SandboxSubstrate(fake_k8s, affinity, short_config)

    with pytest.raises(ClaimTimeoutError) as excinfo:
        substrate.claim("T1")

    message = str(excinfo.value)
    assert "ReconcilerError" in message
    assert view.ready_message is not None
    assert view.ready_message in message
    assert "earlier condition" not in message
    assert poll_count >= 2
    assert "pod was created" not in message.lower()
    assert "cpu-saturated" not in message.lower()
    assert "kubectl top" not in message.lower()
    assert fake_k8s.deleted == fake_k8s.created


def test_claim_on_suspended_route_refuses_to_fork(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    substrate.claim("T1")
    substrate.suspend("T1", history_ref="h-1")
    # The kernel must resume explicitly; a plain claim on a suspended thread
    # would silently fork a second session without the history.
    with pytest.raises(SuspendedThreadError):
        substrate.claim("T1")
    resumed = substrate.resume("T1")
    assert substrate.lookup("T1") == resumed


# --- Per-sandbox runner token (issue #63) -------------------------------------
# The env-var name is the cross-package contract with the runner; asserted by its
# literal string.
RUNNER_TOKEN_ENV = "CURIE_RUNNER_TOKEN"


def test_resume_mints_fresh_runner_token(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    # A resume creates a new claim; the old token died with the old claim, so the
    # new claim env must carry a freshly minted, non-empty runner token.
    substrate.claim("T1")
    substrate.suspend("T1", history_ref="h-1")
    resumed = substrate.resume("T1")

    env = fake_k8s.claims[resumed.claim_name].env
    assert env.get(RUNNER_TOKEN_ENV), "resume must mint a fresh runner token into the claim env"


def test_claim_handle_carries_env_runner_token(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    # The token in the claim env and the token on the returned handle must be the
    # same value, so claim-time and call-time always agree.
    handle = substrate.claim("T1", env={RUNNER_TOKEN_ENV: "tok-19"})

    assert handle.token == "tok-19"
    assert fake_k8s.claims[handle.claim_name].env[RUNNER_TOKEN_ENV] == "tok-19"


def test_resume_merges_caller_boot_env(
    substrate: SandboxSubstrate, fake_k8s: FakeSandboxClient
) -> None:
    # The approval resume path (#244): a suspended pod is gone (ADR-0003), so
    # the replacement must boot with the same bound env a fresh claim gets
    # (bundle ref, budget) or it comes up generic. Session identity and the
    # recorded history ref are preserved on top of the caller env.
    substrate.claim("T1")
    substrate.suspend("T1", history_ref="h-42")

    boot_env = {
        "CURIE_BUNDLE_REF": "bundles/agent-v7.tgz",
        "CURIE_BUDGET": '{"max_output_tokens_per_run": 1, "max_usd_per_day": 1.0}',
        RUNNER_TOKEN_ENV: "tok-fresh",
    }
    resumed = substrate.resume("T1", env=boot_env)

    env = fake_k8s.claims[resumed.claim_name].env
    assert env["CURIE_BUNDLE_REF"] == "bundles/agent-v7.tgz"
    assert env["CURIE_BUDGET"] == boot_env["CURIE_BUDGET"]
    # A caller-minted token is kept (binding.boot_env mints one per claim).
    assert env[RUNNER_TOKEN_ENV] == "tok-fresh"
    # Session identity and the recorded history ref are preserved.
    assert env[SESSION_ENV] == resumed.session_id
    assert env[HISTORY_ENV] == "h-42"
    # The caller's mapping is not mutated.
    assert SESSION_ENV not in boot_env


# --- Bind/serviceFQDN poll backoff -------------------------------------------
# These run on a SIMULATED clock: the substrate's ``time`` module is swapped for
# a fake whose ``sleep`` advances a virtual counter instead of waiting, so a 10s
# cold boot is asserted exactly and instantly. Wall clock would make the cadence
# assertions jitter-dependent and the suite ten seconds slower per case. The
# reaper tests above swap ``substrate.datetime`` the same way.


class _VirtualClock:
    """A monotonic clock whose ``sleep`` advances time rather than passing it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _production_poll_config(key_prefix: str) -> SubstrateConfig:
    """The SHIPPED poll defaults, on a per-test Valkey prefix.

    The ``config`` fixture runs a 5ms interval and a 2s budget to keep the other
    tests fast; asserting the cadence against that would prove nothing about
    what operators actually run.
    """

    return SubstrateConfig(
        namespace="test-ns",
        warm_pool="test-pool",
        key_prefix=key_prefix,
        claim_timeout_seconds=90.0,
    )


class _VirtualBindClient(FakeSandboxClient):
    """Binds its claim once the VIRTUAL clock passes ``bind_after_seconds``.

    Every ``get_claim`` records the virtual instant it was made, which is the
    poll cadence the backoff exists to shape.
    """

    def __init__(self, clock: _VirtualClock, bind_after_seconds: float) -> None:
        super().__init__()
        self.clock = clock
        self.bind_after_seconds = bind_after_seconds
        self.poll_times: list[float] = []

    def get_claim(self, name: str) -> ClaimView | None:
        claim = self.claims.get(name)
        if claim is None:
            return None
        self.poll_times.append(self.clock.now)
        ready = self.clock.now >= self.bind_after_seconds
        return ClaimView(
            name=claim.name,
            ready=ready,
            sandbox_name=claim.sandbox_name if ready else None,
            created_at=claim.created_at,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
        )


class _VirtualFqdnClient(FakeSandboxClient):
    """Binds immediately, but publishes no serviceFQDN until the virtual clock
    passes ``fqdn_after_seconds`` -- the phase-2 shape of a cold boot."""

    def __init__(self, clock: _VirtualClock, fqdn_after_seconds: float) -> None:
        super().__init__()
        self.clock = clock
        self.fqdn_after_seconds = fqdn_after_seconds
        self.sandbox_polls: list[float] = []

    def get_sandbox(self, name: str) -> SandboxView | None:
        view = super().get_sandbox(name)
        if view is None:
            return None
        self.sandbox_polls.append(self.clock.now)
        if self.clock.now >= self.fqdn_after_seconds:
            return view
        return SandboxView(
            name=view.name,
            ready=view.ready,
            service_fqdn="",
            operating_mode=view.operating_mode,
            port=view.port,
        )


def test_bind_polling_backs_off_over_a_cold_boot(
    affinity: AffinityStore, key_prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A cold create (pod scheduling, init containers, runner boot) takes ~10s.
    # The old fixed 50ms loop asks the apiserver about the claim ~200 times over
    # that window, per claim; the backoff must bring that down to tens while
    # still actually polling.
    clock = _VirtualClock()
    monkeypatch.setattr("curie_worker.sandbox.substrate.time", clock)
    fake_k8s = _VirtualBindClient(clock, bind_after_seconds=10.0)
    substrate = SandboxSubstrate(fake_k8s, affinity, _production_poll_config(key_prefix))

    handle = substrate.claim("T1")

    assert handle.sandbox_name in fake_k8s.sandboxes
    assert len(fake_k8s.poll_times) <= 40
    # The lower bound keeps the upper one honest: a loop that stopped polling
    # altogether would also satisfy "at most 40".
    assert len(fake_k8s.poll_times) >= 10


def test_warm_bind_polls_at_the_unchanged_fast_interval(
    affinity: AffinityStore, key_prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The warm-pool path is the one the backoff must not tax. A bind at 200ms is
    # inside the fast window, so the poll instants are exactly the ones the old
    # fixed loop produced and the claim binds on the very same poll.
    clock = _VirtualClock()
    monkeypatch.setattr("curie_worker.sandbox.substrate.time", clock)
    fake_k8s = _VirtualBindClient(clock, bind_after_seconds=0.2)
    substrate = SandboxSubstrate(fake_k8s, affinity, _production_poll_config(key_prefix))

    handle = substrate.claim("T1")

    assert fake_k8s.poll_times == pytest.approx([0.0, 0.05, 0.10, 0.15, 0.20])
    assert handle.sandbox_name in fake_k8s.sandboxes


def test_service_fqdn_polling_backs_off_over_a_cold_boot(
    affinity: AffinityStore, key_prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Phase 2 has its own wait loop and therefore its own call volume: a bound
    # sandbox whose address takes 10s to publish must not cost ~200 get_sandbox
    # calls either.
    clock = _VirtualClock()
    monkeypatch.setattr("curie_worker.sandbox.substrate.time", clock)
    fake_k8s = _VirtualFqdnClient(clock, fqdn_after_seconds=10.0)
    substrate = SandboxSubstrate(fake_k8s, affinity, _production_poll_config(key_prefix))

    handle = substrate.claim("T1")

    assert handle.service_fqdn.endswith(".svc.cluster.local")
    assert len(fake_k8s.sandbox_polls) <= 40
    assert len(fake_k8s.sandbox_polls) >= 10


def test_backoff_is_capped_and_never_overshoots_the_claim_budget(
    affinity: AffinityStore, key_prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Two bounds that together stop the backoff from turning into a stall: no
    # single sleep exceeds the cap, and the last sleep is clamped to whatever is
    # left of the shared deadline, so a claim that never binds still times out
    # at claim_timeout_seconds rather than a cap-length overshoot past it.
    clock = _VirtualClock()
    monkeypatch.setattr("curie_worker.sandbox.substrate.time", clock)
    # 90.0s lands the backoff grid (6 x 0.05, then 0.1 + 0.2 + 0.4, then 0.5
    # steps) exactly on the deadline, so the clamp would never fire and the
    # test would pass even with the ``min(..., deadline - now)`` clamp
    # deleted. 90.3 leaves a 0.3s remainder, smaller than
    # poll_interval_max_seconds, so only the clamp can produce the final sleep.
    config = replace(_production_poll_config(key_prefix), claim_timeout_seconds=90.3)
    fake_k8s = _VirtualBindClient(clock, bind_after_seconds=float("inf"))
    substrate = SandboxSubstrate(fake_k8s, affinity, config)

    with pytest.raises(ClaimTimeoutError):
        substrate.claim("T1")

    assert max(clock.sleeps) <= config.poll_interval_max_seconds
    # The clamp is what produced this final sleep: an unclamped backoff would
    # have kept it at the cap instead of shortening it.
    assert clock.sleeps[-1] < config.poll_interval_max_seconds
    assert clock.now == pytest.approx(config.claim_timeout_seconds)
    # The claim that never bound is still cleaned up.
    assert fake_k8s.deleted == fake_k8s.created


class _VirtualBindThenFqdnClient(FakeSandboxClient):
    """Binds its claim at ``bind_after_seconds`` and only then starts
    publishing serviceFQDN, from ``fqdn_after_seconds``.

    Records both poll streams (``poll_times`` for get_claim, ``sandbox_polls``
    for get_sandbox, matching ``_VirtualBindClient`` and ``_VirtualFqdnClient``
    above) so a test can check the FQDN loop's own cadence rather than only
    the combined outcome. A standalone subclass rather than multiple
    inheritance from those two: both override ``__init__`` with a zero-arg
    ``super().__init__()``, and combining them would route that call into the
    sibling's ``__init__``, which needs its own positional args.
    """

    def __init__(
        self, clock: _VirtualClock, bind_after_seconds: float, fqdn_after_seconds: float
    ) -> None:
        super().__init__()
        self.clock = clock
        self.bind_after_seconds = bind_after_seconds
        self.fqdn_after_seconds = fqdn_after_seconds
        self.poll_times: list[float] = []
        self.sandbox_polls: list[float] = []

    def get_claim(self, name: str) -> ClaimView | None:
        claim = self.claims.get(name)
        if claim is None:
            return None
        self.poll_times.append(self.clock.now)
        ready = self.clock.now >= self.bind_after_seconds
        return ClaimView(
            name=claim.name,
            ready=ready,
            sandbox_name=claim.sandbox_name if ready else None,
            created_at=claim.created_at,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
        )

    def get_sandbox(self, name: str) -> SandboxView | None:
        view = super().get_sandbox(name)
        if view is None:
            return None
        self.sandbox_polls.append(self.clock.now)
        if self.clock.now >= self.fqdn_after_seconds:
            return view
        return SandboxView(
            name=view.name,
            ready=view.ready,
            service_fqdn="",
            operating_mode=view.operating_mode,
            port=view.port,
        )


def test_service_fqdn_polling_restarts_fast_after_a_cold_bind(
    affinity: AffinityStore, key_prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The bind phase backs off all the way to the 500ms cap over its 10s cold
    # boot. The FQDN phase must not inherit that backed-off interval: its own
    # generator starts fresh, so its first sleep is the fast 50ms interval, not
    # a leftover 500ms from phase 1.
    clock = _VirtualClock()
    monkeypatch.setattr("curie_worker.sandbox.substrate.time", clock)
    fake_k8s = _VirtualBindThenFqdnClient(
        clock, bind_after_seconds=10.0, fqdn_after_seconds=10.05
    )
    substrate = SandboxSubstrate(fake_k8s, affinity, _production_poll_config(key_prefix))

    handle = substrate.claim("T1")

    assert fake_k8s.sandbox_polls == pytest.approx([10.0, 10.05])
    assert handle.service_fqdn.endswith(".svc.cluster.local")
