"""The worker retains the verified requester per delivery and mints it at boot.

ADR 0270 decisions 1 and 2. ``identity.check`` accepts the version 2 current
answer, binds its requester to the delivery's checked scope, and refuses a
requester that changes between stages of one delivery. Immediately before the
targeted ``boot_env``, an HTTP-context turn reads the operator flag and passes
the retained requester as the caller principal only when the agent opted in.
A malformed stored flag refuses terminally; a flag read that fails is the
retryable unavailable path, so the delivery stays pending for redelivery.

The core double replays the typed answers the API route emits; it is the
external core seam, not a datastore. The kernel harness runs real Valkey.

@spec CONNECTOR-REQUESTER-1 CONNECTOR-REQUESTER-2 HTTP-INVOCATION-EXECUTION-18.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityScopeV1
from curie_worker import binding as binding_module
from curie_worker import caller_token
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.identity_authority import TurnAuthorityUnavailable
from curie_worker.kernel import identity

from ..queue_fixtures import qevent
from .test_binding_integration import _resolved
from .test_hook_runs import _event as _cron_event
from .test_http_identity_authority import (
    CAPSULE,
    ORIGINAL_HASH,
    Binding,
    Core,
    _active_deployments,
    pending_entry,
    process,
    require_execution_interface,
)

TENANT = "11111111-1111-4111-8111-111111111111"
OTHER_TENANT = "33333333-3333-4333-8333-333333333333"
SCOPE = {
    "tenant_id": TENANT,
    "agent_id": "22222222-2222-4222-8222-222222222222",
    "binding_id": "44444444-4444-4444-8444-444444444444",
    "binding_generation": 7,
}
ISSUER = "https://identity.example.test/directory"
ALICE = {"tenant_id": TENANT, "issuer": ISSUER, "subject": "SyntheticAliceSubject"}
BOB = {"tenant_id": TENANT, "issuer": ISSUER, "subject": "SyntheticBobSubject"}
_REASONS = {
    "continue": "current_authority",
    "refuse": "caller_not_allowed",
    "unavailable": "authority_unavailable",
}


def contract() -> Any:
    """@spec CONNECTOR-REQUESTER-1: a missing V2 contract fails inside the test body."""
    module = importlib.import_module("curie_internal.identity_authority")
    for symbol in ("TurnRequesterV1", "TurnAuthorityResponseV2"):
        assert hasattr(module, symbol), f"turn authority contract missing {symbol}"
    return module


def require_requester_interface() -> None:
    """@spec CONNECTOR-REQUESTER-1: the kernel accessor and carry field exist."""
    assert callable(getattr(identity, "requester", None)), "kernel identity has no requester()"
    assert "requester" in identity.HttpTurnCarry.__dataclass_fields__, (
        "HttpTurnCarry does not retain a requester"
    )


def answer(
    request: TurnAuthorityRequestV1, *, version: int, requester: dict[str, str] | None = None,
    decision: str = "continue",
) -> Any:
    """The typed answer the API emits for this exact request. @spec CONNECTOR-REQUESTER-1."""
    module = contract()
    fields: dict[str, Any] = dict(
        request_id=request.request_id, stage=request.stage, event_id=request.event_id,
        context_sha256=hashlib.sha256(request.context.encode("ascii")).hexdigest(),
        scope=request.scope, decision=decision, reason=_REASONS[decision],
    )
    if version == 1:
        return module.TurnAuthorityResponseV1(schema_version=1, **fields)
    return module.TurnAuthorityResponseV2(
        schema_version=2,
        requester=None if requester is None else module.TurnRequesterV1(**requester),
        **fields,
    )


class ScriptedCore:
    """One scripted answer per check, in order. @spec CONNECTOR-REQUESTER-1."""

    def __init__(self, *answers: tuple[int, dict[str, str] | None, str]) -> None:
        """@spec CONNECTOR-REQUESTER-1: (version, requester, decision) per check."""
        self.answers = list(answers)
        self.requests: list[TurnAuthorityRequestV1] = []

    async def check(self, request: TurnAuthorityRequestV1) -> Any:
        """@spec CONNECTOR-REQUESTER-1."""
        self.requests.append(request)
        version, requester, decision = self.answers.pop(0)
        return answer(request, version=version, requester=requester, decision=decision)


def dumped(value: Any) -> dict[str, str] | None:
    """@spec CONNECTOR-REQUESTER-1."""
    return None if value is None else value.model_dump(mode="json")


def delivery(core: Any, body: Any) -> None:
    """Run ``body(carry, kernel)`` inside one live delivery carry.

    ``identity.check`` reads only the carry and the kernel's current-authority
    collaborator. @spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-18.
    """

    async def go() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18: the owner is this running task."""
        carry = identity.HttpTurnCarry(
            qevent("requester delivery"), CAPSULE, ORIGINAL_HASH,
            scope=TurnAuthorityScopeV1.model_validate(SCOPE),
        )
        token = identity.CURRENT.set(carry)
        try:
            await body(carry, SimpleNamespace(_identity_authority=core))
        finally:
            carry.active = False
            identity.CURRENT.reset(token)

    asyncio.run(go())


# -- identity.check -------------------------------------------------------------------------


def test_a_v2_requester_is_retained_and_exposed_for_the_live_delivery_only() -> None:
    """@spec CONNECTOR-REQUESTER-1: same-requester stages continue; requester() reads it."""
    require_requester_interface()
    core = ScriptedCore((2, ALICE, "continue"), (2, ALICE, "continue"), (2, ALICE, "continue"))
    assert identity.requester() is None

    async def body(carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        assert identity.requester() is None
        for stage in ("prepare", "claim_or_resume", "start"):
            await identity.check(kernel, stage)
            assert dumped(identity.requester()) == ALICE
            assert dumped(carry.requester) == ALICE
        carry.active = False
        assert identity.requester() is None

    delivery(core, body)
    assert [request.stage for request in core.requests] == ["prepare", "claim_or_resume", "start"]
    assert identity.requester() is None


def test_a_v2_null_requester_is_retained_as_absence() -> None:
    """@spec CONNECTOR-REQUESTER-1: no verified human, no requester, the turn continues."""
    require_requester_interface()
    core = ScriptedCore((2, None, "continue"), (2, None, "continue"))

    async def body(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await identity.check(kernel, "prepare")
        assert identity.requester() is None
        await identity.check(kernel, "start")
        assert identity.requester() is None

    delivery(core, body)
    assert len(core.requests) == 2


def test_a_v1_only_delivery_never_retains_a_requester() -> None:
    """@spec CONNECTOR-REQUESTER-1: an agent not opted in keeps today's answers."""
    require_requester_interface()
    core = ScriptedCore((1, None, "continue"), (1, None, "continue"), (1, None, "continue"))

    async def body(carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        for stage in ("prepare", "claim_or_resume", "start"):
            await identity.check(kernel, stage)
            assert identity.requester() is None
        assert carry.requester is None

    delivery(core, body)


@pytest.mark.parametrize(
    ("first", "second"),
    [(ALICE, BOB), (ALICE, None), (None, ALICE)],
    ids=["a-then-b", "a-then-null", "null-then-a"],
)
def test_a_requester_that_changes_within_one_delivery_refuses(
    first: dict[str, str] | None, second: dict[str, str] | None,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: an active execution keeps one requester."""
    require_requester_interface()
    core = ScriptedCore((2, first, "continue"), (2, second, "continue"))

    async def body(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await identity.check(kernel, "prepare")
        assert dumped(identity.requester()) == first
        with pytest.raises(identity.HttpIdentityRefused):
            await identity.check(kernel, "start")
        assert dumped(identity.requester()) == first

    delivery(core, body)
    assert [request.stage for request in core.requests] == ["prepare", "start"]


@pytest.mark.parametrize("first", [ALICE, None], ids=["human", "no-human"])
def test_a_v1_continue_after_a_v2_answer_refuses(first: dict[str, str] | None) -> None:
    """@spec CONNECTOR-REQUESTER-1: a mixed-version API replica cannot drop the requester."""
    require_requester_interface()
    core = ScriptedCore((2, first, "continue"), (1, None, "continue"))

    async def body(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await identity.check(kernel, "prepare")
        with pytest.raises(identity.HttpIdentityRefused):
            await identity.check(kernel, "claim_or_resume")

    delivery(core, body)


@pytest.mark.parametrize(
    ("decision", "raised"),
    [("refuse", identity.HttpIdentityRefused), ("unavailable", TurnAuthorityUnavailable)],
)
def test_a_v2_refusal_or_unavailability_retains_nothing(decision: str, raised: type) -> None:
    """@spec CONNECTOR-REQUESTER-1: refused or unavailable answers carry no projection."""
    require_requester_interface()
    core = ScriptedCore((2, None, decision))

    async def body(carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        with pytest.raises(raised):
            await identity.check(kernel, "prepare")
        assert identity.requester() is None and carry.requester is None

    delivery(core, body)


def test_a_requester_bound_to_another_tenant_never_continues() -> None:
    """@spec CONNECTOR-REQUESTER-1: the worker checks the association before using it.

    The contract already refuses this shape, so the double forges an
    unvalidated instance: the kernel must fail closed whichever check fires.
    """
    require_requester_interface()
    module = contract()

    class ForgingCore:
        """@spec CONNECTOR-REQUESTER-1: an answer that skipped validation."""

        async def check(self, request: TurnAuthorityRequestV1) -> Any:
            """@spec CONNECTOR-REQUESTER-1."""
            valid = answer(request, version=2, requester=ALICE)
            forged = module.TurnRequesterV1.model_construct(**{**ALICE, "tenant_id": OTHER_TENANT})
            return module.TurnAuthorityResponseV2.model_construct(
                **{**dict(valid), "requester": forged},
            )

    async def body(carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        with pytest.raises((identity.HttpIdentityRefused, TurnAuthorityUnavailable)):
            await identity.check(kernel, "prepare")
        assert identity.requester() is None and carry.requester is None

    delivery(ForgingCore(), body)

    liveness = ScriptedCore((2, ALICE, "continue"))

    async def live(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1: the realistic bound requester continues."""
        await identity.check(kernel, "prepare")
        assert dumped(identity.requester()) == ALICE

    delivery(liveness, live)


def test_retention_is_per_delivery_so_the_next_delivery_may_name_another_requester() -> None:
    """@spec CONNECTOR-REQUESTER-1: handoff happens between executions, never inside one."""
    require_requester_interface()

    async def first(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await identity.check(kernel, "prepare")
        assert dumped(identity.requester()) == ALICE

    async def second(_carry: Any, kernel: Any) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        await identity.check(kernel, "prepare")
        assert dumped(identity.requester()) == BOB

    delivery(ScriptedCore((2, ALICE, "continue")), first)
    delivery(ScriptedCore((2, BOB, "continue")), second)


# -- Boot wiring in the kernel lifecycle ---------------------------------------------------


def invalid_state() -> BaseException:
    """The binding's fail-closed flag state error. @spec CONNECTOR-REQUESTER-1."""
    cls = getattr(binding_module, "RequesterIdentityStateInvalid", None)
    assert cls is not None, "curie_worker.binding has no RequesterIdentityStateInvalid"
    assert issubclass(cls, Exception)
    try:
        return cls()
    except TypeError:
        return cls("requester_identity_state_invalid")


class RequesterBinding(Binding):
    """The kernel identity double plus the operator flag reader and boot recording.

    @spec CONNECTOR-REQUESTER-1 CONNECTOR-REQUESTER-2.
    """

    def __init__(
        self, *, flag: bool | BaseException = False, required: bool = True,
        mint_refusal: bool = False,
    ) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        super().__init__(required=required)
        self.flag = flag
        self.mint_refusal = mint_refusal
        self.flag_reads: list[uuid.UUID] = []
        self.boot_kwargs: list[dict[str, Any]] = []

    async def connector_requester_identity_for(self, agent_id: uuid.UUID) -> bool:
        """@spec CONNECTOR-REQUESTER-1: the stored operator flag, or its read failure."""
        self.flag_reads.append(agent_id)
        if isinstance(self.flag, BaseException):
            raise self.flag
        return self.flag

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        """@spec CONNECTOR-REQUESTER-2: record the exact principal the mint receives."""
        self.boot_kwargs.append(dict(kwargs))
        if self.mint_refusal and kwargs.get("caller_principal") is not None:
            # caller_token.mint refuses a token over the verifier bound with ValueError.
            raise ValueError("caller token exceeds the verifier bound")
        return super().boot_env(resolved, thread_key, **kwargs)

    def requester_wire(self, subject: str = "SyntheticAliceSubject") -> dict[str, str]:
        """A requester bound to this binding's own checked tenant. @spec CONNECTOR-REQUESTER-1."""
        return {"tenant_id": str(self.identity.tenant_id), "issuer": ISSUER, "subject": subject}


class RequesterCore(Core):
    """The kernel identity core double, answering V2 like an opted-in API route.

    @spec CONNECTOR-REQUESTER-1.
    """

    def __init__(self, requester: dict[str, str] | None, *, version: int = 2) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        super().__init__()
        self.requester = requester
        self.version = version

    async def check(self, request: TurnAuthorityRequestV1) -> Any:
        """@spec CONNECTOR-REQUESTER-1: refusals stay V1, as the API answers them."""
        base = await super().check(request)
        if self.version == 1 or base.decision != "continue":
            return base
        return answer(request, version=2, requester=self.requester)


def principal_for(requester: dict[str, str]) -> Any:
    """The worker principal the cct must carry. @spec CONNECTOR-REQUESTER-2."""
    cls = getattr(caller_token, "CallerPrincipal", None)
    assert cls is not None, "caller_token has no CallerPrincipal"
    return cls(
        tenant_id=requester["tenant_id"], issuer=requester["issuer"],
        subject=requester["subject"],
    )


def assert_refused_before_boot(h: Any, binding: RequesterBinding) -> None:
    """No boot, no claim, no runner input, a terminal refusal. @spec CONNECTOR-REQUESTER-1."""
    assert binding.boots == [] and h.fake_k8s.claim_envs == []
    assert h.runner.opened == [] and h.runner.steers == [] and binding.mints == []


def test_an_opted_in_http_turn_boots_with_the_retained_requester_as_caller_principal(
    make_harness,
):
    """@spec CONNECTOR-REQUESTER-1 CONNECTOR-REQUESTER-2: flag on, verified human, signed."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-2."""
        binding = RequesterBinding(flag=True)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("opted-in requester")
            await process(h, event)
            assert binding.flag_reads and set(binding.flag_reads) == {binding.identity.agent_id}
            assert len(binding.boot_kwargs) == 1
            assert binding.boot_kwargs[0]["caller_principal"] == principal_for(
                binding.requester_wire(),
            )
            assert len(h.fake_k8s.claim_envs) == 1 and h.runner.opened == [event.text]

    asyncio.run(run())


@pytest.mark.parametrize("answer_version", [2, 1], ids=["v2-null-requester", "v1-answer"])
def test_an_opted_in_turn_without_a_verified_human_boots_principal_less(
    make_harness, answer_version: int,
):
    """@spec CONNECTOR-REQUESTER-1: no requester claim, the agent-only token still boots."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=True)
        core = RequesterCore(None, version=answer_version)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("opted-in without a human")
            await process(h, event)
            assert len(binding.boot_kwargs) == 1
            assert binding.boot_kwargs[0].get("caller_principal") is None
            assert len(h.fake_k8s.claim_envs) == 1 and h.runner.opened == [event.text]

    asyncio.run(run())


def test_an_agent_not_opted_in_boots_principal_less(make_harness):
    """@spec CONNECTOR-REQUESTER-1: flag off and V1 answers keep today's boot."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=False)
        core = RequesterCore(None, version=1)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("not opted in")
            await process(h, event)
            assert len(binding.boot_kwargs) == 1
            assert binding.boot_kwargs[0].get("caller_principal") is None
            assert len(h.fake_k8s.claim_envs) == 1 and h.runner.opened == [event.text]

    asyncio.run(run())


def test_a_requester_retained_for_an_agent_not_opted_in_refuses_before_boot(make_harness):
    """@spec CONNECTOR-REQUESTER-1: an inconsistent flag and answer fail closed."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=False)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("requester without opt-in")
            await process(h, event)
            assert binding.flag_reads and set(binding.flag_reads) == {binding.identity.agent_id}
            assert binding.boot_kwargs == []
            assert_refused_before_boot(h, binding)
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(run())


@pytest.mark.parametrize(
    "answer_version,human",
    [(2, True), (2, False), (1, False)],
    ids=["v2-human", "v2-no-human", "v1"],
)
def test_an_invalid_requester_identity_state_refuses_before_boot(
    make_harness, answer_version: int, human: bool,
):
    """@spec CONNECTOR-REQUESTER-1: a malformed stored opt-in fails closed at use, terminally."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=invalid_state())
        core = RequesterCore(binding.requester_wire() if human else None, version=answer_version)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("invalid requester state")
            await process(h, event)
            assert binding.flag_reads and set(binding.flag_reads) == {binding.identity.agent_id}
            assert binding.boot_kwargs == []
            assert_refused_before_boot(h, binding)
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(run())


@pytest.mark.parametrize(
    "answer_version,human",
    [(2, True), (2, False), (1, False)],
    ids=["v2-human", "v2-no-human", "v1"],
)
def test_a_failed_flag_read_is_unavailable_not_a_terminal_refusal(
    make_harness, answer_version: int, human: bool,
):
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-18: a read fault is retryable.

    The binding reports a failed read as ``TurnAuthorityUnavailable``. The
    kernel raises it to the consumer like every other unavailable answer: no
    boot, no claim, no reply and no done marker, whether or not the agent
    would have read as opted in (the "v1" case is today's flag-off agent).
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=TurnAuthorityUnavailable())
        core = RequesterCore(binding.requester_wire() if human else None, version=answer_version)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("unreadable requester flag")
            with pytest.raises(TurnAuthorityUnavailable, match="^identity_authority_unavailable$"):
                await process(h, event)
            assert binding.flag_reads and set(binding.flag_reads) == {binding.identity.agent_id}
            assert binding.boot_kwargs == [] and binding.boots == []
            assert h.fake_k8s.claim_envs == [] and binding.mints == []
            assert h.runner.opened == [] and h.runner.steers == [] and h.sink.events == []
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(run())


@pytest.mark.parametrize(
    "recovered,answer_version,human",
    [(True, 2, True), (False, 1, False)],
    ids=["opted-in-human", "not-opted-in"],
)
def test_a_flag_read_failure_stays_pending_and_the_redelivery_runs_once(
    make_harness, recovered: bool, answer_version: int, human: bool,
):
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-18: a transient fault drops nothing.

    The actual consumer handles one stream entry. The first delivery's flag
    read fails: the entry stays in the PEL with no claim and no done marker.
    The read then recovers, the entry is reclaimed and redelivered, and the
    turn executes exactly once with the principal the recovered flag allows.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=TurnAuthorityUnavailable())
        core = RequesterCore(binding.requester_wire() if human else None, version=answer_version)
        async with make_harness(
            binding=binding, identity_authority=core, reclaim_min_idle_ms=1,
        ) as h:
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
            )
            event = qevent("flag read recovers")
            fields = {"payload": event.model_dump_json(), "curie_identity_context_v1": CAPSULE}
            entry_id, observed = await pending_entry(h, consumer, fields)

            await consumer._handle(entry_id, observed)
            assert len(binding.flag_reads) == 1
            assert binding.boot_kwargs == [] and binding.boots == []
            assert h.fake_k8s.claim_envs == [] and binding.mints == []
            assert h.runner.opened == [] and h.sink.events == []
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            assert (
                await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            )["pending"] == 1

            binding.flag = recovered
            # Idle time zero claims the pending entry at once; the test needs
            # the redelivery, not a reclaim delay.
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, h.config.consumer_name,
                min_idle_time=0, message_ids=[entry_id],
            )
            await consumer._handle(entry_id, observed)
            assert len(binding.flag_reads) == 2
            assert set(binding.flag_reads) == {binding.identity.agent_id}
            assert len(binding.boot_kwargs) == 1 and len(binding.boots) == 1
            expected = principal_for(binding.requester_wire()) if human else None
            assert binding.boot_kwargs[0].get("caller_principal") == expected
            assert len(h.fake_k8s.claim_envs) == 1 and h.runner.opened == [event.text]
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert (
                await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            )["pending"] == 0

    asyncio.run(run())


def test_a_principal_the_mint_refuses_refuses_the_turn_before_the_claim(make_harness):
    """@spec CONNECTOR-REQUESTER-2: an oversized projection refuses before boot (plan item 10)."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-2."""
        binding = RequesterBinding(flag=True, mint_refusal=True)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("oversized requester")
            await process(h, event)
            assert len(binding.boot_kwargs) == 1
            assert binding.boot_kwargs[0]["caller_principal"] == principal_for(
                binding.requester_wire(),
            )
            assert_refused_before_boot(h, binding)
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(run())


def test_a_legacy_turn_never_reads_the_flag_or_passes_a_principal(make_harness):
    """@spec CONNECTOR-REQUESTER-1: no HTTP context, no requester transport."""
    require_requester_interface()

    async def run() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        binding = RequesterBinding(flag=True, required=False)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = qevent("legacy turn", thread="legacy-requester")
            await h.kernel.process_event(event)
            assert core.requests == [] and binding.flag_reads == []
            assert len(binding.boot_kwargs) == 1
            assert binding.boot_kwargs[0].get("caller_principal") is None
            assert len(h.fake_k8s.claim_envs) == 1 and h.runner.opened == [event.text]

    asyncio.run(run())


def test_a_scheduled_turn_on_an_http_binding_never_reads_the_flag_or_passes_a_principal(
    make_harness, make_hook_run,
):
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-23: a schedule acquires no human."""
    require_requester_interface()

    async def run_case() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        async with make_hook_run() as run, _active_deployments(run) as deploy:
            await deploy(run.version_id)
            binding = RequesterBinding(flag=True)
            binding.identity = binding.identity.model_copy(update={"agent_id": run.agent_id})
            binding.resolved = _resolved(run.agent_id, bundle=None)
            binding._by_route[("slack", "C1")] = binding.resolved
            core = RequesterCore(binding.requester_wire())
            async with make_harness(
                binding=binding, identity_authority=core, hook_runs=run.recorder(),
            ) as h:
                h.runner.default_script = [Final(text="complete", status=SessionStatus.DONE)]
                event = _cron_event(hook_run=run.ref)
                await h.kernel.process_event(event)
                assert h.runner.opened == [event.text]
                assert core.requests == [] and binding.flag_reads == []
                assert len(binding.boot_kwargs) == 1
                assert binding.boot_kwargs[0].get("caller_principal") is None

    asyncio.run(run_case())
