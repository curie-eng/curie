"""Actual execution gates with real Valkey and a labeled synthetic external core.

Opaque capsules here exercise transport ownership, not MAC/current-policy proof.
@spec HTTP-INVOCATION-EXECUTION-18.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import threading
import uuid
from typing import Any

import pytest
from aci_protocol import Attachment, Final, SessionStatus, TextDelta
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1
from curie_worker import workspace as workspace_module
from curie_worker.binding import HttpIdentityBinding
from curie_worker.capacity_wait import WAIT_REPLY_REF_FIELD
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.identity_authority import HttpTurnAuthorityClient, TurnAuthorityUnavailable
from curie_worker.kernel.core import Kernel
from curie_worker.sandbox import SuspendedThreadError

from .. import test_workspace as workspace_tests
from ..queue_fixtures import qevent, wait_until
from ..test_sibling_wiring import built as built
from .test_attachment_claim import _FakeAttachmentLane, _WorkspaceProbe
from .test_binding_integration import StubBinding, _resolved

CAPSULE = "curie-context-v1.e30." + "A" * 43
ORIGINAL_HASH = hashlib.sha256(b"synthetic original queued bytes").hexdigest()


def require_execution_interface() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-18: missing ABI fails inside test bodies."""
    assert "identity_authority" in inspect.signature(Kernel).parameters, (
        "Kernel has no identity_authority collaborator"
    )
    parameters = inspect.signature(Kernel.process_event).parameters
    assert "http_identity_context" in parameters, "Kernel loses the opaque context"
    assert "original_payload_sha256" in parameters, "Kernel loses the original wire digest"


class Binding(StubBinding):
    """Synthetic binding service; the actual SQL classification has separate real-PG proof.

    @spec HTTP-INVOCATION-EXECUTION-18.
    """

    def __init__(self, *, required: bool = True, fault: str | None = None) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        self.identity = HttpIdentityBinding(
            tenant_id=uuid.uuid4(), agent_id=uuid.uuid4(), binding_id=uuid.uuid4(),
            binding_generation=7, http_identity_required=required,
        )
        self.resolved = _resolved(self.identity.agent_id, bundle=None)
        super().__init__({("slack", "C1"): self.resolved})
        self.fault = fault
        self.boots: list[str] = []
        self.mints: list[dict[str, Any]] = []
        self.classifications: list[tuple[Any, ...]] = []

    async def identity_binding(self, *route: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        self.classifications.append(route)
        if self.fault == "error":
            raise RuntimeError("synthetic binding service unavailable")
        if self.fault == "malformed":
            return {"http_identity_required": False}
        if self.fault == "absent":
            return None
        return self.identity

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        """@spec HTTP-INVOCATION-EXECUTION-18: count real kernel preparation before claim."""
        self.boots.append(thread_key)
        return {
            **super().boot_env(resolved, thread_key, **kwargs),
            "CURIE_SESSION_ID": "session:" + thread_key,
            "CURIE_HISTORY_REF": "history:" + thread_key,
        }

    def turn_memory_token(self, _resolved: Any, **kwargs: Any) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-18: synthetic external capability issuer."""
        self.mints.append(kwargs)
        return "synthetic-memory-capability"

    async def close_turn_memory(self, *_args: Any, **_kwargs: Any) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18: synthetic external capability settlement."""


class Core:
    """Typed synthetic external observations; never substitutes a datastore response.

    @spec HTTP-INVOCATION-EXECUTION-18.
    """

    def __init__(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        self.requests: list[TurnAuthorityRequestV1] = []
        self.effects: list[tuple[str, tuple[int, ...]]] = []
        self.refuse: str | None = None
        self.unavailable = False
        self.pause_event: str | None = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.observe: Any = None

    async def check(self, request: TurnAuthorityRequestV1) -> TurnAuthorityResponseV1:
        """@spec HTTP-INVOCATION-EXECUTION-18: external core contract, not an offline permit."""
        assert type(request) is TurnAuthorityRequestV1
        self.requests.append(request)
        if self.observe is not None:
            self.effects.append((request.stage, self.observe()))
        if request.event_id == self.pause_event and request.stage == "prepare":
            self.entered.set()
            await self.release.wait()
        if self.unavailable:
            raise TurnAuthorityUnavailable()
        unsupported_resume = request.approval_id is not None
        decision = "refuse" if request.stage == self.refuse or unsupported_resume else "continue"
        return TurnAuthorityResponseV1(
            schema_version=1, request_id=request.request_id, stage=request.stage,
            event_id=request.event_id,
            context_sha256=hashlib.sha256(request.context.encode("ascii")).hexdigest(),
            scope=request.scope, decision=decision,
            reason=("unsupported_resume" if unsupported_resume else
                    "caller_not_allowed" if decision == "refuse" else "current_authority"),
        )


async def process(
    h: Any, event: Any, *, context: Any = CAPSULE, digest: Any = ORIGINAL_HASH,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-18."""
    await h.kernel.process_event(
        event, http_identity_context=context, original_payload_sha256=digest,
    )


def expected_scope(binding: Binding) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-18: derive expectations from the binding owner."""
    return {
        "tenant_id": str(binding.identity.tenant_id),
        "agent_id": str(binding.identity.agent_id),
        "binding_id": str(binding.identity.binding_id),
        "binding_generation": 7,
    }


@pytest.mark.parametrize("configured", [False, True], ids=["unqualified-generic", "explicit-local"])
def test_unselected_legacy_control_still_claims_runs_and_completes(make_harness, configured: bool):
    """@spec HTTP-INVOCATION-EXECUTION-18: two real harness controls, no fabricated HTTP proof."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding = Binding(required=False) if configured else None
        async with make_harness(binding=binding) as h:
            event = qevent("legacy control", thread="legacy-control")
            await h.kernel.process_event(event)
            assert h.runner.opened == [event.text]
            assert len(h.fake_k8s.claim_envs) == 1
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


def test_adopted_binding_without_context_is_terminal_before_preparation(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: provoke the old worker's real unauthorized boot."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding = Binding()
        async with make_harness(binding=binding) as h:
            event = qevent("missing provenance", thread="missing-context")
            await h.kernel.process_event(event)
            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == [] and binding.boots == [] and binding.mints == []
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["missing", "error", "malformed"])
def test_configured_unreadable_classifier_cannot_select_legacy(make_harness, fault: str):
    """@spec HTTP-INVOCATION-EXECUTION-18: actual old generic fallback is unsafe."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding = Binding(fault=fault)
        if fault == "missing":
            binding.identity_binding = None
        async with make_harness(binding=binding) as h:
            with pytest.raises(TurnAuthorityUnavailable, match="^identity_authority_unavailable$"):
                await h.kernel.process_event(qevent("unreadable classification"))
            assert h.runner.opened == [] and h.fake_k8s.claim_envs == []
            assert binding.boots == [] and binding.mints == [] and h.sink.events == []
    asyncio.run(run())


@pytest.mark.parametrize("required", [True, False], ids=["http", "changed-to-local"])
def test_recognized_context_checks_each_execution_boundary_before_privilege(
    make_harness, required: bool,
):
    """@spec HTTP-INVOCATION-EXECUTION-18: context cannot turn local after admission."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(required=required), Core()
        async with make_harness(binding=binding, identity_authority=core) as h:
            core.observe = lambda: (
                len(binding.boots), len(h.fake_k8s.claim_envs), len(binding.mints),
            )
            event = qevent("checked execution")
            await process(h, event)
            stages = [request.stage for request in core.requests]
            assert stages[0] == "prepare" and stages[-1] == "start"
            assert stages.index("prepare") < stages.index("claim_or_resume") < stages.index("start")
            assert core.effects[0] == ("prepare", (0, 0, 0))
            assert ("claim_or_resume", (1, 0, 0)) in core.effects
            assert ("start", (1, 1, 0)) in core.effects
            assert len({request.request_id for request in core.requests}) == len(core.requests)
            for request in core.requests:
                assert request.context == CAPSULE and request.payload_sha256 == ORIGINAL_HASH
                assert request.event_id == event.event_id and request.approval_id is None
                assert request.scope.model_dump() == expected_scope(binding)
            assert h.runner.opened == [event.text] and len(binding.mints) == 1
    asyncio.run(run())


@pytest.mark.parametrize(("stage", "boots", "claims"), [
    ("prepare", 0, 0), ("claim_or_resume", 1, 0), ("start", 1, 1),
])
def test_refusal_prevents_the_next_real_execution_effect(make_harness, stage, boots, claims):
    """@spec HTTP-INVOCATION-EXECUTION-18: no capability or runner call follows refusal."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        core.refuse = stage
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("refused execution")
            await process(h, event)
            assert core.requests[-1].stage == stage
            assert len(binding.boots) == boots and len(h.fake_k8s.claim_envs) == claims
            assert binding.mints == [] and h.runner.opened == [] and h.runner.steers == []
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


@pytest.mark.parametrize("owner", ["generic", "missing-client", "binding-gone"])
def test_context_requires_a_concrete_current_owner_and_client(make_harness, owner: str):
    """@spec HTTP-INVOCATION-EXECUTION-18: recognized work never takes a generic path."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding = None if owner == "generic" else Binding(
            fault="absent" if owner == "binding-gone" else None,
        )
        core = None if owner == "missing-client" else Core()
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("recognized but unqualified")
            if owner == "binding-gone":
                await process(h, event)
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
            else:
                with pytest.raises(TurnAuthorityUnavailable):
                    await process(h, event)
            assert h.runner.opened == [] and h.fake_k8s.claim_envs == [] and h.sink.events == []
            if core is not None:
                assert core.requests == []
    asyncio.run(run())


def test_preparation_pause_cannot_salvage_a_revoked_claim(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: revoke during actual file preparation."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        entered, release = threading.Event(), threading.Event()
        lane = _FakeAttachmentLane(resolve_entered=entered, resolve_gate=release)
        async with make_harness(binding=binding, identity_authority=core) as h:
            h.kernel._attachments = lane
            event = qevent("fetch then refuse", attachments=[Attachment(id="F1", name="file.txt")])
            task = asyncio.create_task(process(h, event))
            try:
                assert await asyncio.to_thread(entered.wait, 3), "file preparation did not start"
                core.refuse = "claim_or_resume"
            finally:
                release.set()
                await task
            assert lane.resolve_calls and lane.discard_calls
            assert h.fake_k8s.claim_envs == [] and binding.mints == [] and h.runner.opened == []
            assert core.requests[-1].stage == "claim_or_resume"
    asyncio.run(run())


def test_live_steer_refusal_precedes_capability_renewal_and_runner_input(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        async with make_harness(binding=binding, identity_authority=core) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            opener = asyncio.create_task(process(h, qevent("first", thread="steer")))
            try:
                await wait_until(lambda: h.runner.turn_active)
                minted = len(binding.mints)
                requests = len(h.runner.steer_headers)
                core.refuse = "steer"
                await process(h, qevent("forbidden followup", thread="steer"))
                assert h.runner.opened == ["first"] and h.runner.steers == []
                assert len(h.runner.steer_headers) == requests
                assert len(binding.mints) == minted
            finally:
                hold.set()
                await opener
    asyncio.run(run())


def test_idle_409_fallback_rechecks_start_before_new_capabilities(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: the shared start gate owns fallback too."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        async with make_harness(binding=binding, identity_authority=core) as h:
            await process(h, qevent("completed opener", thread="fallback"))
            minted, probes = len(binding.mints), len(h.runner.steer_headers)
            core.refuse = "start"
            await process(h, qevent("refused fallback", thread="fallback"))
            assert len(h.runner.steer_headers) == probes + 1
            assert h.runner.opened == ["completed opener"] and len(binding.mints) == minted
            assert core.requests[-1].stage == "start"
    asyncio.run(run())


def test_attachment_handoff_reaches_the_same_start_refusal_gate(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: a replacement sandbox cannot skip the common gate."""
    require_execution_interface()

    class AuthenticatedBinding(Binding):
        """@spec HTTP-INVOCATION-EXECUTION-18: satisfy retained-runner handoff authentication."""

        def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
            """@spec HTTP-INVOCATION-EXECUTION-18."""
            return {
                **super().boot_env(resolved, thread_key, **kwargs),
                "CURIE_RUNNER_TOKEN": "synthetic-http-attachment-runner-token",
            }

    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = AuthenticatedBinding(), Core()
        binding.resolved.deployment_id = uuid.uuid4()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2,
        ) as h:
            h.kernel._attachments = _FakeAttachmentLane()
            h.kernel._workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            await process(h, qevent("first", thread="file-handoff"))
            minted = len(binding.mints)
            core.refuse = "start"
            await process(h, qevent(
                "refused file", thread="file-handoff",
                attachments=[Attachment(id="F2", name="file.txt")],
            ))
            assert len(h.fake_k8s.claim_envs) == 2, "the test did not reach an actual handoff"
            assert [text for runner in h.runners.values() for text in runner.opened] == ["first"]
            assert len(binding.mints) == minted and core.requests[-1].stage == "start"
    asyncio.run(run())


def test_approval_resume_parser_cannot_mint_an_http_approval_grant(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: original context remains subject to refusal."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        approval = str(uuid.uuid4())
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("approved system message", event_id=f"approval-{approval}-resolved")
            await process(h, event)
            assert core.requests[0].approval_id == approval
            assert core.requests[0].context == CAPSULE
            assert core.requests[0].stage == "prepare"
            assert binding.boots == [] and binding.mints == []
            assert h.fake_k8s.claim_envs == [] and h.runner.opened == [] and h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


@pytest.mark.parametrize("transition", ["claim", "resume", "handoff"])
def test_real_workspace_coordinator_rechecks_after_prepare_before_each_allocation(
    make_harness, tmp_path, transition: str,
):
    """@spec HTTP-INVOCATION-EXECUTION-18: actual coordinator, synthetic repository/object ports."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        async with make_harness() as h:
            thread = "1700000000.000100"
            old = None
            if transition != "claim":
                old = await asyncio.to_thread(h.substrate.claim, thread, env={})
            if transition == "resume":
                await asyncio.to_thread(
                    h.substrate.suspend, thread, history_ref="synthetic-history",
                )
            baseline_claims = len(h.fake_k8s.claim_envs)
            preparer, _commands, objects = workspace_tests._preparer(workspace_module, tmp_path)
            original_prepare = preparer.prepare
            revoked = False
            guarded = []

            def prepare(**kwargs: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-18: revoke after real archive preparation."""
                nonlocal revoked
                prepared = original_prepare(**kwargs)
                revoked = transition != "resume"
                return prepared

            def current_guard() -> None:
                """@spec HTTP-INVOCATION-EXECUTION-18: synthetic external authority changes."""
                nonlocal revoked
                guarded.append(revoked)
                if revoked:
                    raise TurnAuthorityUnavailable()
                # The first claim now reports the actual suspended route. A resume
                # must sample again rather than reusing this completed observation.
                revoked = True

            preparer.prepare = prepare
            coordinator = workspace_module.WorkspaceClaimCoordinator(
                preparer=preparer, substrate=h.substrate, suspended_error=SuspendedThreadError,
            )
            arguments = dict(
                thread_key=thread, deployment_id=uuid.uuid4(), repo_full_name="acme-corp/acme-bot",
            )
            if transition == "handoff":
                arguments["replace_handle"] = old
            # Exercise the actual old coordinator, too, instead of counting its
            # absent optional parameter as the behavior proof.
            if "revalidate_before_claim" in inspect.signature(
                coordinator.claim_or_resume_with_handle,
            ).parameters:
                arguments["revalidate_before_claim"] = current_guard
            try:
                refused = False
                try:
                    await asyncio.to_thread(coordinator.claim_or_resume_with_handle, **arguments)
                except TurnAuthorityUnavailable:
                    refused = True
                assert len(h.fake_k8s.claim_envs) == baseline_claims
                assert refused and guarded == ([False, True] if transition == "resume" else [True])
                assert objects.objects == {}, "refused staged archive or ownership was orphaned"
                assert h.substrate.lookup(thread) == (old if transition == "handoff" else None)
            finally:
                await asyncio.to_thread(coordinator.release, thread)
                await asyncio.to_thread(h.substrate.release, thread)
    asyncio.run(run())


def test_cancelled_and_concurrent_carry_does_not_leak_into_an_unselected_event(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: two live tasks plus a subsequent plain local turn."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2,
        ) as h:
            first = qevent("cancelled", thread="carry-one")
            second = qevent("concurrent", thread="carry-two")
            core.pause_event = first.event_id
            task = asyncio.create_task(process(h, first, context="opaque-first", digest="1" * 64))
            try:
                await asyncio.wait_for(core.entered.wait(), 2)
                await process(h, second, context="opaque-second", digest="2" * 64)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                core.release.set()
                await asyncio.gather(task, return_exceptions=True)
            assert any(
                r.event_id == first.event_id and r.context == "opaque-first" for r in core.requests
            )
            second_requests = [r for r in core.requests if r.event_id == second.event_id]
            assert second_requests and all(
                r.context == "opaque-second" and r.payload_sha256 == "2" * 64
                for r in second_requests
            )
            before = len(core.requests)
            binding.identity = binding.identity.model_copy(update={"http_identity_required": False})
            await h.kernel.process_event(qevent("plain local", thread="carry-three"))
            assert len(core.requests) == before
            opened = [text for runner in h.runners.values() for text in runner.opened]
            assert "plain local" in opened and "cancelled" not in opened
    asyncio.run(run())


async def pending_entry(
    h: Any, consumer: Consumer, fields: dict[str, str],
) -> tuple[str, dict[str, str]]:
    """@spec HTTP-INVOCATION-EXECUTION-18: actual stream/PEL ownership."""
    await consumer.ensure_group()
    await h.async_redis.xadd(h.config.stream, fields)
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1,
    )
    entry_id, observed = rows[0][1][0]
    return entry_id, dict(observed)


def test_consumer_keeps_original_hash_and_all_fields_across_capacity_placeholder_rewrite(
    make_harness, monkeypatch: pytest.MonkeyPatch,
):
    """@spec HTTP-INVOCATION-EXECUTION-18: actual park/wake, no reconstruction of payload bytes."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        async with make_harness() as h:
            event = qevent("original unicode café", placeholder=None)
            raw = " \n" + json.dumps(
                event.model_dump(mode="json"), ensure_ascii=False, indent=1,
            ) + "\n"
            fields = {"payload": raw, "curie_identity_context_v1": CAPSULE,
                      "synthetic-extra": "preserved", "traceparent": "synthetic-malformed-trace"}
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
            )
            entry_id, observed = await pending_entry(h, consumer, fields)
            lease = await leases.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer=h.config.consumer_name,
            )
            assert lease is not None
            try:
                record = await consumer._waits.park(entry_id, observed, event.event_id, lease)
                notice = await consumer._waits.claim_notice(event.event_id, record.generation)
                assert notice is not None
                await consumer._waits.finish_notice(
                    event.event_id, record.generation, notice,
                    delivered=True, reply_ref="synthetic-queued-placeholder",
                )
            finally:
                await leases.release(
                    h.config.stream, h.config.consumer_group, entry_id,
                    owner=lease.owner, resume_event_id=None,
                )
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            assert await consumer._waits.wake_due() == 1
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1,
            )
            new_id, awakened = rows[0][1][0]
            assert all(awakened[key] == value for key, value in fields.items())
            assert awakened[WAIT_REPLY_REF_FIELD] == "synthetic-queued-placeholder"
            seen = []
            original = h.kernel.process_event

            async def observed_process(turn: Any, **kwargs: Any) -> None:
                """@spec HTTP-INVOCATION-EXECUTION-18: delegate the actual kernel unchanged."""
                seen.append((turn, kwargs))
                await original(turn, **kwargs)

            monkeypatch.setattr(h.kernel, "process_event", observed_process)
            await consumer._handle(new_id, dict(awakened))
            assert len(seen) == 1
            turn, kwargs = seen[0]
            assert turn.reply_handle.placeholder == "synthetic-queued-placeholder"
            assert kwargs.get("http_identity_context") == CAPSULE
            assert kwargs.get("original_payload_sha256") == hashlib.sha256(raw.encode()).hexdigest()
            assert kwargs["original_payload_sha256"] != hashlib.sha256(
                turn.model_dump_json().encode(),
            ).hexdigest()
    asyncio.run(run())


def test_unavailable_http_delivery_stays_pending_until_the_existing_cap_dead_letters(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: actual PEL count and XADD-before-XACK owner."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        core = Core()
        core.unavailable = True
        async with make_harness(
            binding=Binding(), identity_authority=core, max_delivery=2, reclaim_min_idle_ms=1,
        ) as h:
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
            )
            raw = qevent("unavailable current authority").model_dump_json()
            fields = {"payload": raw, "curie_identity_context_v1": CAPSULE}
            entry_id, observed = await pending_entry(h, consumer, fields)
            dead_stream = h.config.dead_letter_stream_name()
            try:
                await consumer._handle(entry_id, observed)
                assert (
                    await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                )["pending"] == 1
                assert len(core.requests) == 1 and h.runner.opened == []
                assert h.fake_k8s.claim_envs == [] and h.sink.events == []
                await asyncio.sleep(0.02)
                await h.async_redis.xclaim(
                    h.config.stream, h.config.consumer_group, h.config.consumer_name,
                    min_idle_time=1, message_ids=[entry_id],
                )
                await consumer._handle(entry_id, observed)
                assert len(core.requests) == 2
                await asyncio.sleep(0.02)
                assert entry_id in await consumer._dead_letter_over_cap()
                assert (
                    await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                )["pending"] == 0
                dead = await h.async_redis.xrange(dead_stream)
                assert len(dead) == 1
                assert all(dead[0][1][key] == value for key, value in fields.items())
                assert len(core.requests) == 2 and h.runner.opened == [] and h.sink.events == []
            finally:
                await h.async_redis.delete(dead_stream)
    asyncio.run(run())


@pytest.mark.parametrize("defect", ["non-ascii", "oversized", "missing-digest", "invalid-digest"])
def test_invalid_recognized_carrier_is_terminal_before_any_execution_effect(make_harness, defect):
    """@spec HTTP-INVOCATION-EXECUTION-18: malformed recognized work cannot become legacy."""
    require_execution_interface()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = Binding(), Core()
        context, digest = CAPSULE, ORIGINAL_HASH
        if defect == "non-ascii":
            context = "curie-context-v1.é." + "A" * 43
        elif defect == "oversized":
            context = CAPSULE + "A" * (16385 - len(CAPSULE))
            assert len(context) == 16385
        elif defect == "missing-digest":
            digest = None
        else:
            digest = "A" * 64
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("invalid recognized carrier")
            await process(h, event, context=context, digest=digest)
            assert binding.boots == [] and binding.mints == []
            assert h.fake_k8s.claim_envs == [] and h.runner.opened == [] and h.runner.steers == []
            assert core.requests == [] and h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


def test_normal_worker_factory_supplies_the_current_authority_client(built):
    """@spec HTTP-INVOCATION-EXECUTION-18: actual build/Kernel, blocked external constructors."""
    kwargs = built(
        {}, internal_worker_token="synthetic-current-check-worker-token",
        api_base_url="http://synthetic-core.invalid",
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1:1/synthetic",
        valkey_host="127.0.0.1", valkey_port=1,
    )
    assert isinstance(kwargs.get("identity_authority"), HttpTurnAuthorityClient), (
        "normal worker build leaves HTTP current authority unwired"
    )


def test_retained_attachment_rechecks_authority_after_fetch_before_handoff(make_harness):
    """@spec HTTP-INVOCATION-EXECUTION-18: a completed fetch cannot retain an earlier grant."""
    require_execution_interface()

    class AuthenticatedBinding(Binding):
        """@spec HTTP-INVOCATION-EXECUTION-18: qualify this retained-runner handoff."""

        def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
            """@spec HTTP-INVOCATION-EXECUTION-18."""
            return {
                **super().boot_env(resolved, thread_key, **kwargs),
                "CURIE_RUNNER_TOKEN": "synthetic-http-attachment-runner-token",
            }

    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        binding, core = AuthenticatedBinding(), Core()
        binding.resolved.deployment_id = uuid.uuid4()
        entered, release = threading.Event(), threading.Event()
        lane = _FakeAttachmentLane(resolve_entered=entered, resolve_gate=release)
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2,
        ) as h:
            h.kernel._attachments = lane
            h.kernel._workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            await process(h, qevent("retained opener", thread="post-fetch-handoff"))
            original = h.substrate.lookup("slack:C1:post-fetch-handoff")
            assert original is not None and original.token
            assert len(h.fake_k8s.claim_envs) == 1
            minted = len(binding.mints)
            task = asyncio.create_task(process(h, qevent(
                "revoked while fetching", thread="post-fetch-handoff",
                attachments=[Attachment(id="F3", name="file.txt")],
            )))
            try:
                assert await asyncio.to_thread(entered.wait, 3), "retained fetch did not start"
                core.refuse = "claim_or_resume"
            finally:
                release.set()
                await task
            assert len(h.fake_k8s.claim_envs) == 1, "revoked fetch created a replacement claim"
            assert h.substrate.lookup("slack:C1:post-fetch-handoff") == original
            assert [text for runner in h.runners.values() for text in runner.opened] == [
                "retained opener",
            ]
            assert len(binding.mints) == minted
            assert core.requests[-1].stage == "claim_or_resume"
    asyncio.run(run())
