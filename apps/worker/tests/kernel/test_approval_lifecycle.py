"""Approval-gate lifecycle tests (#244, ADR-0010): suspend on pending, resume
on resolve, durable across a worker restart.

Same harness discipline as the other kernel suites: real Valkey, the real
substrate over a fake Kubernetes client, an in-process fake ACI runner. Only
Slack, the model, and the approval API (a recording fake at the
``ApprovalCreator`` seam) are faked.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import aiohttp
import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol import ConfirmIntent
from channel_protocol.reply import ReplyAck, ReplyEvent
from curie_worker.approvals import (
    ApprovalBackendError,
    ApprovalRequest,
    CreatedApproval,
    SettledApproval,
)
from curie_worker.reply_sink import TargetRoute
from curie_worker.sandbox.types import RouteState

DONE = SessionStatus.DONE
AWAITING = SessionStatus.AWAITING_APPROVAL


class RecordingApprovals:
    """An ApprovalCreator fake that records requests and mints stable ids."""

    def __init__(self, *, fail: bool = False) -> None:
        self.requests: list[ApprovalRequest] = []
        self.create_calls = 0
        self.fail = fail

    async def create(self, request: ApprovalRequest) -> CreatedApproval:
        self.create_calls += 1
        if self.fail:
            raise ApprovalBackendError("approval API unavailable")
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


class RecordingReader:
    """An ApprovalReader fake: hands back settled records in order, records the reads.

    Takes one or more responses and returns them in turn, the last one repeating
    for every further read, so ``RecordingReader(record)`` is a fixed record and
    ``RecordingReader(None, record)`` is a first read that came back empty and
    then recovered.

    That leading ``None`` is what a transient failure actually looks like at this
    seam (#1199): ``ApprovalReader.get`` never raises -- on an ``httpx.HTTPError``
    or a 503 it logs and hands back ``None`` -- so a blip is indistinguishable at
    the call site from "no such record", and raising here would exercise a path
    the production reader cannot produce.

    Separate from ``RecordingApprovals`` for the same reason the kernel takes two
    parameters (#1084): most tests need only the create half, and a combined fake
    would make every one of them carry a read they never exercise.
    """

    def __init__(self, *records: SettledApproval | None) -> None:
        # Loud here rather than an IndexError on the first ``get``: with no
        # records there is nothing to hand back, and ``RecordingReader(None)``
        # is how you spell "the read came back empty".
        assert records, "RecordingReader needs at least one record; use RecordingReader(None)"
        self.records = records
        self.reads: list[str] = []

    async def get(self, approval_id: str) -> SettledApproval | None:
        self.reads.append(approval_id)
        return self.records[min(len(self.reads), len(self.records)) - 1]


def _qevent(
    text: str,
    *,
    thread: str = "th-appr",
    event_id: str | None = None,
    placeholder: str | None = "p-1",
    endpoint: str | None = None,
    kind: str = "slack",
    adapter: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind=kind,
            channel="C1",
            placeholder=placeholder,
            endpoint=endpoint,
            adapter=adapter,
        ),
        received_at="2026-07-14T00:00:00+00:00",
    )


def _thread_key(thread: str, *, kind: str = "slack") -> str:
    return f"{kind}:C1:{thread}"


def _awaiting_script(summary: str) -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(text="Requesting sign-off", status=AWAITING, approval_summary=summary),
    ]


# The settled record a resolved approval reads back as: the one verdict the
# card-stamping tests below all pin their assertions to.
_APPROVED = SettledApproval(
    status="approved", resolved_by="U9", resolution_note="approved for Q3"
)


async def _pause_awaiting_approval(h, thread: str) -> None:
    """Run a turn to the approval pause on ``thread``.

    The live card is posted and its location remembered -- the worker is the only
    component that knows where it went -- which is the precondition every
    card-settling test below starts from, hence the exists-assert on the ref.
    """

    h.runner.default_script = _awaiting_script("Refund order 42")
    await h.kernel.process_event(_qevent("refund?", thread=thread))
    assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))


async def _peek_card_ref(h, thread: str) -> dict | None:
    """The remembered card ref, read WITHOUT consuming it.

    The store's own read is a GETDEL, so a test that wants to inspect a surviving
    ref goes to the key directly.
    """

    raw = await h.async_redis.get(h.config.approval_card_key(_thread_key(thread)))
    return None if raw is None else json.loads(raw)


def test_awaiting_approval_creates_record_and_suspends(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            ev = _qevent("please discount", event_id="ev-appr-1")
            await h.kernel.process_event(ev)

            # The durable record was created with the turn's identity: the
            # dedupe key is the event id and the reply handle rides along so a
            # resolution can resume into the same placeholder.
            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.summary == "Give ACME a 20% discount"
            assert req.dedupe_key == "ev-appr-1"
            assert req.conversation_id == ev.conversation_id
            assert req.reply_channel == "C1"
            assert req.reply_placeholder == "p-1"
            assert req.author == "U1"

            # The sandbox was suspended and the route flipped to SUSPENDED.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Suspended"]
            record = h.substrate._affinity.get(_thread_key(ev.conversation_id))
            assert record is not None and record.state is RouteState.SUSPENDED

            # The placeholder carries the pending notice with the record id,
            # and the event is done (no retry loop).
            assert h.sink.last_text is not None
            assert "Awaiting approval (appr-1)" in h.sink.last_text
            assert "Give ACME a 20% discount" in h.sink.last_text
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_the_created_record_carries_the_turns_kind_and_adapter(make_harness) -> None:
    """T-A12, worker half / AC2 (plan EB-A17, finding 2).

    The durable record is what the RESUME is rebuilt from, days later, possibly
    after the binding moved (T-A8). So the kind and the egress-credential
    selector have to be copied off THIS turn's reply handle at creation time --
    the one moment both facts are known and true.

    Mutation this catches: leave `kernel.py`'s `ApprovalRequest(...)` producer
    unchanged. Everything still compiles, the approval is still created, the
    Slack lane is unaffected, and only a resumed EMAIL turn -- the rarest path in
    the system -- reveals the loss.

    Both fields are asserted on the same request rather than in two tests,
    because dropping either one alone produces the same silent shape.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Send the quote to ACME")
            ev = _qevent(
                "send it",
                event_id="ev-appr-kind",
                kind="email",
                adapter="agentmail-sandbox",
            )
            await h.kernel.process_event(ev)

            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.reply_kind == "email"
            assert req.reply_adapter == "agentmail-sandbox"
            # The pre-existing reply-handle fields are still copied, so a
            # producer that replaced the block rather than extending it fails.
            assert req.reply_channel == "C1"
            assert req.reply_placeholder == "p-1"

    asyncio.run(go())


def test_a_slack_turns_record_carries_slack_and_no_adapter(make_harness) -> None:
    """T-A12, the sibling lane. Slack legitimately has no adapter (its route is
    the worker's configured origin, D4.4), so its record must persist NULL rather
    than borrow the email lane's slug or a placeholder string -- a fabricated
    adapter would send the platform's egress credential for somebody else's
    adapter on resume.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("discount?", event_id="ev-appr-slack"))

            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.reply_kind == "slack"
            assert req.reply_adapter is None

    asyncio.run(go())


def test_null_placeholder_turn_reaches_an_approval_and_persists_its_own_ref(
    make_harness,
) -> None:
    """A placeholder-less turn can pause for approval like any other (ADR-0079).

    The reverse of what this test asserted before the placeholder-less path
    landed: the kernel used to refuse the turn outright, so a channel that
    preposts nothing could not reach an approval gate at all.

    The record must carry the ref the turn DELIVERED on, not the null the wire
    carried. Those differ here, and persisting the null would send the approval's
    outcome to a second message beside the request it answers.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            event = _qevent(
                "please discount",
                placeholder=None,
                endpoint="http://adapter.example.test/",
                kind="email",
                adapter="agentmail",
            )

            await h.kernel.process_event(event)

            assert h.runner.opened == ["please discount"]
            assert approvals.create_calls == 1
            req = approvals.requests[0]
            # The routing pair and egress selector still come off the wire...
            assert req.reply_kind == "email"
            assert req.reply_adapter == "agentmail"
            # ...but the reply ref is the one this turn minted by delivering.
            assert req.reply_placeholder is not None
            assert req.reply_placeholder == h.sink.text_posts[0][1]

    asyncio.run(go())


def test_no_edit_placeholderless_approval_uses_one_minted_ref(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals,
            slack_no_edit_streaming=True,
        ) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            thread = "th_no_edit_approval"
            event = _qevent(
                "please discount",
                thread=thread,
                placeholder=None,
            )

            await h.kernel.process_event(event)

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            request = approvals.requests[0]
            assert request.reply_kind == "slack"
            assert request.reply_endpoint is None
            assert request.reply_adapter is None
            assert request.reply_placeholder == minted
            assert h.sink.updates
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert "Awaiting approval (appr-1)" in h.sink.updates[-1][2]

            pending_update_count = len(h.sink.updates)
            h.runner.default_script = [
                Final(text="Discount applied.", status=DONE),
            ]
            resolution = _qevent(
                "[approval resolved] approved by U9",
                thread=thread,
                event_id="approval-appr-1-resolved",
                placeholder=request.reply_placeholder,
            )

            await h.kernel.process_event(resolution)

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            assert len(h.sink.updates) == pending_update_count + 1
            assert h.sink.updates[-1][1] == minted
            assert h.sink.updates[-1][2] == "Discount applied."

    asyncio.run(go())


def test_no_edit_placeholderless_approval_refuses_a_missing_minted_ref(
    make_harness,
) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals,
            slack_no_edit_streaming=True,
        ) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            event = _qevent(
                "please discount",
                thread="th_no_edit_missing_ref",
                placeholder=None,
            )
            original_emit = h.sink.emit
            missing_ref_deliveries = 0

            async def omit_precreation_ref(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                nonlocal missing_ref_deliveries
                if (
                    reply_event.target.reply_ref is None
                    and getattr(reply_event, "text", None) == "Requesting sign-off"
                ):
                    missing_ref_deliveries += 1
                    return ReplyAck(ref=None)
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = omit_precreation_ref

            with pytest.raises(RuntimeError, match="reply ref"):
                await h.kernel.process_event(event)

            assert missing_ref_deliveries == 1
            assert approvals.create_calls == 0
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_stream_minted_ref_survives_a_booting_delivery_failure(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            booting = h.config.booting_text
            original_emit = h.sink.emit
            booting_failures = 0

            async def fail_booting_once(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                nonlocal booting_failures
                if getattr(event, "text", None) == booting and booting_failures == 0:
                    booting_failures += 1
                    raise RuntimeError("injected booting delivery failure")
                return await original_emit(
                    event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = fail_booting_once
            event = _qevent(
                "please discount",
                placeholder=None,
                endpoint="http://adapter.example.test/",
                kind="email",
                adapter="agentmail",
            )

            await h.kernel.process_event(event)

            assert booting_failures == 1
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            assert approvals.requests[0].reply_placeholder == minted
            assert len(h.sink.updates) >= 2, h.sink.updates
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert "Awaiting approval (appr-1)" in h.sink.updates[-1][2]

    asyncio.run(go())


def test_multiparagraph_summary_yields_a_single_block_parseable_notice(
    make_harness,
) -> None:
    """A model-authored multi-paragraph summary must not break the CLI notice
    parse (#817).

    The notice is a control string the CLI splits on blank lines, requiring the
    marker-leading block (cli/src/chat.rs parse_approval_id, the #766
    keep-alive). A blank line inside the summary would strand the resumed reply
    (or, on the route-bound path, report the raw notice as a false success). The
    kernel collapses the interpolated summary to one logical line, so the notice
    stays a single block whose trailing ``\\n\\n``-split segment starts with the
    marker -- while the durable record keeps the original summary."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            summary = "First paragraph of the summary.\n\nSecond paragraph.\nThird line."
            h.runner.default_script = _awaiting_script(summary)
            ev = _qevent("please discount", event_id="ev-appr-multi")
            await h.kernel.process_event(ev)

            # The durable record keeps the original multi-paragraph summary; only
            # the notice display is collapsed.
            assert len(approvals.requests) == 1
            assert approvals.requests[0].summary == summary

            # The placeholder notice is a single logical block: splitting on the
            # blank-line delimiter, the trailing block is the marker-leading
            # notice, exactly what the CLI parser anchors on.
            text = h.sink.last_text
            assert text is not None
            blocks = text.split("\n\n")
            notice = blocks[-1]
            assert notice.startswith("Awaiting approval (appr-1)")
            assert "The session is paused" in notice
            # The summary's own blank line is gone; it reads as one line.
            summary_line, _, _ = notice.partition("\n")
            assert "First paragraph of the summary." in summary_line
            assert "Second paragraph." in summary_line
            assert "Third line." in summary_line

    asyncio.run(go())


def test_pending_state_survives_worker_restart_and_resumes_on_resolve(
    make_harness,
) -> None:
    """The epic's acceptance shape: suspend, replace every worker-side object
    (a fresh harness over the same Valkey routes), then deliver the resolution
    turn and watch the session resume and complete."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-restart"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Refund order 42")
            await h.kernel.process_event(_qevent("refund?", thread=thread))
            record = h.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.SUSPENDED

        # "Restart": a brand-new kernel/substrate/runner (nothing in-process
        # survives) over the same Valkey affinity keys. The suspended route is
        # still there because it lives in Valkey, not worker memory.
        async with make_harness(approvals=approvals) as h2:
            record = h2.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.SUSPENDED

            # The resolution turn (what the API enqueues on resolve): the
            # kernel must resume the suspended thread, boot a replacement
            # sandbox WITH the bound boot env, and run the turn to done.
            h2.runner.default_script = [
                Final(text="Refund processed.", status=DONE)
            ]
            resume_turn = _qevent(
                "[approval resolved] approved by U9", thread=thread, event_id="ev-resolve-1"
            )
            await h2.kernel.process_event(resume_turn)

            # The suspended claim was retired and a fresh one created; the
            # route is LIVE again and the reply landed.
            record = h2.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.LIVE
            assert h2.sink.last_text == "Refund processed."
            assert h2.runner.opened == ["[approval resolved] approved by U9"]

    asyncio.run(go())


def test_resume_injects_boot_env_into_replacement_claim(make_harness) -> None:
    """The dormant-path fix: a resume must boot the replacement sandbox with
    the same bound env a fresh claim gets (bundle ref, budget), not a generic
    env -- the suspended pod is gone (ADR-0003) and env is all a boot has."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            thread = "th-envmerge"
            h.runner.default_script = _awaiting_script("Ship it")
            await h.kernel.process_event(_qevent("ship?", thread=thread))

            h.runner.default_script = [Final(text="Shipped.", status=DONE)]
            boot_env = {
                "CURIE_BUNDLE_REF": "bundles/agent-v7.tgz",
                "CURIE_BUDGET": '{"max_output_tokens_per_run": 1, "max_usd_per_day": 1.0}',
            }
            handle = await h.kernel._claim_or_resume(_thread_key(thread), boot_env)
            assert handle is not None

            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env["CURIE_BUNDLE_REF"] == "bundles/agent-v7.tgz"
            assert "CURIE_BUDGET" in resumed_env
            # The substrate still guarantees session identity and a fresh
            # runner token on the replacement claim.
            assert resumed_env.get("CURIE_SESSION_ID")
            assert resumed_env.get("CURIE_RUNNER_TOKEN")

    asyncio.run(go())


class GrantBinding:
    """A binding stand-in that answers approval_grant_tool by event id (#430).

    resolve/boot_env behave like the routed double; approval_grant_tool returns
    the granted tool ONLY for the one resume event id it was configured with,
    mirroring the worker's real derivation from durable approval state.
    """

    def __init__(self, *, grant_event_id: str, grant_tool: str) -> None:
        self.grant_event_id = grant_event_id
        self.grant_tool = grant_tool
        self.agent_id = uuid.uuid4()

    async def resolve(self, kind: str, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    def packs_for(self, resolved):  # noqa: ANN001, ANN201
        from curie_worker.behaviorpacks import BehaviorPacks

        return BehaviorPacks.from_config(None)

    def budget_for(self, resolved):  # noqa: ANN001, ANN201
        from aci_protocol import Budget

        return Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0)

    def boot_env(self, resolved, thread_key):  # noqa: ANN001, ANN201
        return {"CURIE_SESSION_ID": f"s-{thread_key}"}

    async def approval_grant_tool(self, event_id: str, agent_id):  # noqa: ANN001, ANN201
        return self.grant_tool if event_id == self.grant_event_id else None


def test_resume_claim_injects_approval_grant_tool_env(make_harness) -> None:
    """#430: a resume claim for an approved permission-gate approval injects
    CURIE_APPROVAL_GRANT_TOOL into the boot env passed to the replacement
    claim; a fresh (non-approval) mention injects nothing (the gate re-arms)."""

    async def go() -> None:
        from curie_api.resumequeue import resume_event_id

        grant_event = resume_event_id(uuid.uuid4())
        binding = GrantBinding(
            grant_event_id=grant_event, grant_tool="mcp__github__create_issue"
        )
        async with make_harness(binding=binding) as h:
            # The resume turn carries the approval resume event id -> the grant
            # for the approved tool lands in the boot env of the fresh claim.
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "proceed with the approved action",
                    thread="th-grant",
                    event_id=grant_event,
                )
            )
            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env.get("CURIE_APPROVAL_GRANT_TOOL") == "mcp__github__create_issue"

            # A fresh, unrelated mention has a different event id -> no grant env
            # (re-armed), so an adopted/warm follow-up cannot inherit an allowance.
            await h.kernel.process_event(
                _qevent("hello there", thread="th-fresh", event_id="ev-fresh-1")
            )
            fresh_env = h.fake_k8s.claim_envs[-1]
            assert fresh_env is not None
            assert "CURIE_APPROVAL_GRANT_TOOL" not in fresh_env

    asyncio.run(go())


def test_no_backend_escalates_instead_of_stranding(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:  # no approvals client wired
            h.runner.default_script = _awaiting_script("Anything")
            ev = _qevent("gate this")
            await h.kernel.process_event(ev)

            assert h.sink.last_text is not None
            assert "no approval backend" in h.sink.last_text
            # Not suspended: a pause nothing could resume would strand the thread.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_backend_failure_escalates_and_does_not_suspend(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals(fail=True)
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Anything")
            ev = _qevent("gate this")
            await h.kernel.process_event(ev)

            assert h.sink.last_text is not None
            assert "could not be created" in h.sink.last_text
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_unknown_gate_kind_escalates_instead_of_stranding_the_turn(make_harness) -> None:
    """#492/#544: ``gate_kind`` is authority-bearing, so the shared wire model
    rejects an unrecognized value rather than degrading it to None (which would
    route it through the prefix fallback and silently widen authority).

    The ACI ``final`` frame types the field as a bare ``str``, so a runner can
    emit anything and the rejection lands at the worker, at construction. Before
    the model was shared this same value was rejected by the API with a 422,
    surfacing as ``ApprovalBackendError`` and escalating; the local raise must
    escalate identically. If it escaped ``_pause_for_approval`` the consumer
    would leave the entry pending, redeliver it until the delivery cap, and
    dead-letter it -- a full LLM re-run per redelivery and silence for the user.
    The done marker is the proof it did not: it is only written once the turn is
    terminally handled."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = [
                TextDelta(text="Requesting sign-off"),
                Final(
                    text="Requesting sign-off",
                    status=AWAITING,
                    approval_summary="Anything",
                    approval_gate_kind="not-a-real-gate",
                ),
            ]
            ev = _qevent("gate this", thread="th-bad-gate")
            await h.kernel.process_event(ev)

            # Escalated to a human, exactly as the 422 path did.
            assert h.sink.last_text is not None
            assert "could not be created" in h.sink.last_text
            # No record was created from the rejected payload.
            assert approvals.requests == []
            # Not suspended: a session no resolution could ever wake.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            # Terminally handled, so the entry is acked rather than redelivered.
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_pause_emits_a_confirm_intent_for_the_approval_card(make_harness) -> None:
    """#246, ADR-0020: pausing emits a channel-neutral Confirm intent into the
    approval's thread whose confirm/cancel actions carry the record id (the Slack
    adapter renders it into Block Kit buttons -- see test_slack_sink.py), alongside
    the placeholder notice. The kernel never builds Block Kit itself."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            ev = _qevent("please discount", thread="th-card")
            await h.kernel.process_event(ev)

            assert len(h.sink.posts) == 1
            channel, message, requested_by, thread_ts, _endpoint = h.sink.posts[0]
            assert channel == "C1"
            assert thread_ts == "th-card"
            assert requested_by == "U1"
            # The mandatory text fallback carries the summary; the adapter derives
            # the "Approval required: ..." card fallback from it below the seam.
            assert message.text == "Give ACME a 20% discount"
            # A Confirm intent, not buttons: the record id rides both actions so a
            # click resolves exactly this approval.
            intent = message.interaction
            assert isinstance(intent, ConfirmIntent)
            assert intent.id == "appr-1"
            assert intent.confirm.value == "appr-1"
            assert intent.cancel.value == "appr-1"
            assert (intent.confirm.label, intent.cancel.label) == ("Approve", "Reject")
            # #1053: the decision may carry a reason. Asserted at the KERNEL,
            # not only at the renderer, because this is the field that decides
            # whether the real product path offers a note at all -- a renderer
            # test alone would stay green with the kernel emitting the default.
            #
            # #1076: and asserted UNCONDITIONALLY, because the value being the
            # same for every card is itself the decision. Nothing here reads
            # config, so if a toggle is ever added this assertion is where the
            # change surfaces, rather than the always-on behavior quietly
            # becoming sometimes-on.
            assert intent.allow_free_text is True

    asyncio.run(go())


def test_every_posted_card_carries_the_note_variant(make_harness) -> None:
    """#1076: always-on is the decision, so pin it across card SHAPES.

    ``allow_free_text`` is set at one call site with no config behind it, which
    is easy to read as an accident of that call site. This drives the two shapes
    that differ -- the in-thread card of an UNROUTED approval and the top-level
    card of a ROUTED one -- and asserts both carry it. A change that made it
    conditional on the routing branch would fail here rather than surfacing only
    as a UX difference nobody tests.
    """

    async def go() -> None:
        # Unrouted: the card joins the requesting thread.
        async with make_harness(approvals=RecordingApprovals()) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("please discount", thread="th-unrouted"))
            unrouted = h.sink.posts[0]

        # Routed: the card posts top-level in the bound channel.
        binding = RoutedBinding({"finance": {"channel": "C_FINANCE"}})
        async with make_harness(approvals=RecordingApprovals(), binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Approve the invoice", "finance")
            await h.kernel.process_event(_qevent("please invoice", thread="th-routed"))
            routed = h.sink.posts[0]

        # The two really are different shapes, or this test proves nothing.
        assert unrouted[3] == "th-unrouted", "the unrouted card must be in-thread"
        assert routed[3] is None, "the routed card must be top-level"

        for label, (channel, message, _by, _ts, _endpoint) in (
            ("unrouted", unrouted),
            ("routed", routed),
        ):
            intent = message.interaction
            assert isinstance(intent, ConfirmIntent)
            assert intent.allow_free_text is True, (
                f"the {label} card for {channel} came without the note variant; "
                "every card carries it (#1076)"
            )

    asyncio.run(go())


def test_escalation_paths_post_no_card(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:  # no approvals backend wired
            h.runner.default_script = _awaiting_script("Anything")
            await h.kernel.process_event(_qevent("gate this"))
            assert h.sink.posts == []

    asyncio.run(go())


def _awaiting_routed_script(summary: str, route: str) -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=AWAITING,
            approval_summary=summary,
            approval_route=route,
        ),
    ]


class RoutedBinding:
    """A minimal binding stand-in: one channel -> one agent with route bindings."""

    def __init__(self, routes: dict | None) -> None:
        self.routes = routes
        self.agent_id = uuid.uuid4()

    async def resolve(self, kind: str, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
            approval_routes=self.routes,
        )

    def packs_for(self, resolved):  # noqa: ANN001, ANN201
        from curie_worker.behaviorpacks import BehaviorPacks

        return BehaviorPacks.from_config(None)

    def budget_for(self, resolved):  # noqa: ANN001, ANN201
        from aci_protocol import Budget

        return Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0)

    def boot_env(self, resolved, thread_key):  # noqa: ANN001, ANN201
        return {"CURIE_SESSION_ID": f"s-{thread_key}"}


def test_routed_approval_cards_go_to_the_bound_channel(make_harness) -> None:
    """#247: the manifest route resolves through the agent's bindings; the card
    lands in the bound channel (top-level, no foreign thread) and the record
    carries route + card_channel so the authorizer counts THAT channel. #451:
    the triggering turn has no per-turn endpoint (a Slack-triggered turn), so
    the card also rides the worker's default Slack transport (``None``)."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": {"channel": "C_MGRS"}})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script(
                "Discount for ACME", "managers"
            )
            await h.kernel.process_event(_qevent("discount?", thread="th-routed"))

            req = approvals.requests[0]
            assert req.route == "managers"
            assert req.card_channel == "C_MGRS"
            # Card posted to the bound channel, top-level (no thread there).
            channel, message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C_MGRS"
            assert thread_ts is None
            assert isinstance(message.interaction, ConfirmIntent)
            assert endpoint is None

    asyncio.run(go())


def test_unbound_route_escalates_instead_of_routing_to_the_requesting_channel(
    make_harness,
) -> None:
    """(19, #544 Decision B / AC2) A named but UNBOUND route escalates loudly:
    no approval is created and no card is posted, so authority never widens to
    the requesting channel. This deliberately REVERSES #247's silent
    channel-fallback (the behavior this test used to assert) -- the fallback was
    the same silent widening from the other end.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding(None)  # agent has no bindings at all
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Anything", "managers")
            ev = _qevent("gate", thread="th-unbound")
            await h.kernel.process_event(ev)

            # No approval was created for the unresolvable route ...
            assert approvals.requests == []
            # ... and no card was posted anywhere (never widened to a channel).
            assert h.sink.posts == []
            # The human-visible escalation names the unbound route.
            assert h.sink.last_text is not None
            assert "managers" in h.sink.last_text
            # The event is terminally handled (done), not left to retry.
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_routeless_approval_keeps_prior_behavior(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Plain request")
            await h.kernel.process_event(_qevent("gate", thread="th-plain"))

            req = approvals.requests[0]
            assert req.route is None
            assert req.card_channel == "C1"

    asyncio.run(go())


# --- Card transport follows the card's channel, not the trigger (#451) --------

_CLI_STUB = "http://localhost:8155"


def test_routed_card_ignores_the_triggering_turns_endpoint(make_harness) -> None:
    """#451: the card's channel is policy (the manifest route binding), so its
    transport must be too. A CLI-triggered turn carries a local stub endpoint;
    delivering a route-bound card through it posts the card at the stub instead
    of the real Slack workspace, so the bound channel never sees it. ``None``
    means the worker's default Slack transport."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": {"channel": "C_MGRS"}})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script(
                "Discount for ACME", "managers"
            )
            await h.kernel.process_event(
                _qevent("discount?", thread="th-cli-routed", endpoint=_CLI_STUB)
            )

            channel, _message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C_MGRS"
            assert thread_ts is None
            assert endpoint is None

    asyncio.run(go())


def test_card_routed_to_requesting_channel_keeps_the_trigger_endpoint(
    make_harness,
) -> None:
    """The inverse of the routed case: when the route binds back to the channel
    that asked, the card belongs to that conversation -- it threads under it and
    rides the same transport the trigger arrived on, so a CLI-stub turn's card
    stays at the stub."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": {"channel": "C1"}})  # the requesting channel
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Ship it", "managers")
            await h.kernel.process_event(
                _qevent("ship?", thread="th-self-routed", endpoint=_CLI_STUB)
            )

            channel, _message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C1"
            assert thread_ts == "th-self-routed"
            assert endpoint == _CLI_STUB

    asyncio.run(go())


# --- Expired-approval card teardown (#419) ------------------------------------


def _resume_turn(
    text: str, *, thread: str, approval_id: str, author: str
) -> QueuedTurn:
    """The API's approval resume turn: the deterministic ``approval-<id>-resolved``
    event id both the resolve and expiry paths stamp, replayed into the same
    placeholder. The expiry path authors it as "system"; a resolve names the
    resolver."""

    return QueuedTurn(
        event_id=f"approval-{approval_id}-resolved",
        conversation_id=thread,
        author=author,
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1", endpoint=None),
        received_at="2026-07-14T00:00:00+00:00",
    )


def test_expiry_resume_disables_the_approval_card(make_harness) -> None:
    """#419: an EXPIRED approval's resume turn (author "system", enqueued by the
    #412 sweeper or a past-SLA resolve attempt) disables the live card in place --
    buttons gone, an expiry line in their stead -- mirroring the resolved-card
    edit, since no click will ever arrive to do it."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-expire-card"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("please discount", thread=thread))

            # The live card was posted and its location remembered, because an
            # expiry (unlike a resolve) carries no click to locate the card.
            assert len(h.sink.posts) == 1
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))
            card_ts = "posted-1"  # the FakeSink's returned ts for the first post

            # The expiry resume turn the sweeper enqueues (author "system").
            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",
                )
            )

            # The card was edited in place: same ts, and a channel-neutral message
            # carrying the remembered summary. The buttonless expired-card render
            # (no actions block, an expiry line) is the adapter's job below the
            # seam -- asserted in test_slack_sink.py.
            assert len(h.sink.card_updates) == 1
            channel, ts, message, endpoint, settled = h.sink.card_updates[0]
            # Expiry means nobody decided, so the settled outcome carries no
            # decision and the adapter renders the expired form (#1084).
            assert settled is not None and settled.decision is None
            assert (channel, ts) == ("C1", card_ts)
            assert endpoint is None
            assert message.text == "Give ACME a 20% discount"

            # The memory was consumed (GETDEL), so a redelivery no-ops.
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

            # The continuation still streamed into the placeholder.
            assert h.sink.last_text == "Acknowledged the expiry."

    asyncio.run(go())


def test_resolve_resume_stamps_the_card_from_the_record(make_harness) -> None:
    """#1084: a RESOLVE resume settles the card, it no longer leaves it live.

    This asserted the opposite until #1084, on the premise that "the dispatcher
    already did from the click". That holds only when there WAS a click: a
    resolution through ``POST /approvals/{id}/resolve`` or ``curie <tier>
    approvals --resolve`` never touches Slack, so the card kept its buttons and
    every later click earned a 409. The worker is the only component that still
    knows where the card is, so settling it belongs here.

    The verdict comes from the durable record, not from the platform-authored
    resume prose -- that sentence is written for a model, and rebuilding a
    decision out of it by regex is how the card would start lying after a
    wording change.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-resolve-card"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # The card was settled, with the outcome read off the record.
            assert len(h.sink.card_updates) == 1
            _channel, _ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert message.text == "Refund order 42", "the summary must survive"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            assert settled.note == "approved for Q3"
            # And the requester the live card named is carried into the rebuild,
            # so the settled card is not missing a line the original had.
            # "U1" is the author `_qevent` stamps on the triggering turn, which
            # is exactly what `approval_card` rendered as "Requested by".
            assert settled.requested_by == "U1"

            # The read was keyed off the resume turn's deterministic event id.
            assert reader.reads == ["appr-1"]

            # The memory is still consumed, so a later approval cannot collide.
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_a_resolve_resume_leaves_the_card_alone_when_the_record_cannot_be_read(
    make_harness,
) -> None:
    """No record, no stamp (#1084).

    The kernel would have to invent a decision to render anything, and a card
    stating a verdict nobody confirmed is worse than one still showing buttons:
    the buttons are at least honest about the platform not having told Slack
    yet, and the next click gets the real answer from the API.

    #1199 flipped the key assertion below: the ref must SURVIVE a pass that
    stamped nothing.
    """

    async def go() -> None:
        reader = RecordingReader(None)
        thread = "th-unreadable-record"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.card_updates == []
            # #1199: the ref SURVIVES, because nothing was stamped. It is the
            # only pointer the platform still holds to the posted card, and a
            # failed record read is routinely transient (a 503, a connection
            # blip), so consuming it here would permanently strand a card with
            # live-looking Approve/Reject buttons that no later pass can settle.
            # The ref is only spent once a stamp is actually attempted.
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_a_resolve_resume_with_no_reader_configured_still_resumes(make_harness) -> None:
    """Progressive enhancement, per ADR-0020: a deployment with nothing to read
    the record with settles no card and continues the run normally. The stamp is
    an enrichment on a decision that already happened."""

    async def go() -> None:
        thread = "th-no-reader"
        async with make_harness(approvals=RecordingApprovals()) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.card_updates == []
            # The run itself continued: the resumed reply was delivered.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_resolve_authored_by_a_system_named_actor_does_not_expire_the_card(
    make_harness,
) -> None:
    """#419 hardening: the expiry-vs-resolve discriminator is the platform text
    marker, NOT the author. A resolver whose identity is literally "system" (the
    codebase's reserved machine-actor name) must not get its RESOLVED card wrongly
    stamped expired -- the ``[approval resolved]`` text keeps it off the expiry
    path."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-system-resolver"
        async with make_harness(approvals=approvals) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by system",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",  # a resolver literally named "system"
                )
            )

            # Author is "system" but the text says RESOLVED: the card is left to
            # the dispatcher, never stamped expired by the worker. That is the
            # load-bearing assertion here.
            assert h.sink.card_updates == []
            # #1199: no reader is configured, so no stamp was even attempted, so
            # the ref survives. It is the only pointer to the posted card, and
            # spending it on a pass that settled nothing would strand a card
            # whose buttons still look live. (This asserted the opposite until
            # #1199, when the pop moved behind the record read.)
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


# --- A transient record read must not destroy the card ref (#1199) ------------


def test_a_transient_record_read_leaves_the_ref_for_a_later_pass(make_harness) -> None:
    """#1199: the ref is spent only when a stamp is actually attempted.

    ``ApprovalCardStore.pop`` is a GETDEL, so it is a one-shot destruction of the
    only pointer the platform still holds to the posted Slack card. And
    ``ApprovalReader.get`` never raises: on a transient ``httpx.HTTPError`` or a
    503 it logs and returns ``None``. Popping before that read therefore lets a
    momentary API blip permanently strand the card -- buttons still live, every
    later click earning a 409, and no pass able to re-drive the stamp because the
    ref is gone. Reading the record first makes the failure recoverable: the ref
    outlives the bad pass, and the next pass settles the card -- but only when
    that same delivery is reclaimed. There is no unconditional later pass: an
    otherwise-healthy turn goes on to write the done marker and every redelivery
    is then skipped, so the surviving ref is revisited only when the delivery
    ALSO failed to reach ``mark_done`` and the entry is reclaimed (ADR-0039,
    #505) -- which is exactly the state pass 2 below reproduces.

    THE MUTATION THIS CATCHES: move the ``pop`` back above the record read in
    ``Kernel._finalize_settled_card``. The first pass then destroys the ref, so
    the second pass finds ``ref is None`` and ``card_updates`` stays empty.
    """

    async def go() -> None:
        # The blip, then the recovery: one ``None`` read, then the real record.
        reader = RecordingReader(None, _APPROVED)
        thread = "th-transient-read"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            resume = _resume_turn(
                "[approval resolved] approved by U9",
                thread=thread,
                approval_id="appr-1",
                author="U9",
            )

            # Pass 1: the record read comes back None (the blip).
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(resume)

            # Nothing was stamped, which is correct: the kernel must not guess a
            # verdict. But the ref SURVIVES, which is the whole point of #1199.
            assert h.sink.card_updates == []
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

            # Pass 2 is the bounded-reclaim redelivery (ADR-0039, #505): the
            # worker died before ``mark_done``, so the entry is redelivered and
            # the kernel's done short-circuit does not fire. Clearing the done key
            # reproduces that crash-before-done state; it is not a test shortcut
            # around the idempotence guard, it IS the state reclaim delivers.
            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            # The recovered pass settles the card, with the outcome read off the
            # now-reachable record.
            assert len(h.sink.card_updates) == 1
            _channel, _ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert message.text == "Refund order 42", "the summary must survive"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            assert settled.note == "approved for Q3"
            # "U1" is the author `_qevent` stamps on the triggering turn, which is
            # what the live card rendered as "Requested by"; it rides the ref.
            assert settled.requested_by == "U1"

            # And NOW the ref is consumed, because a stamp was made.
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_a_redelivery_after_a_successful_stamp_still_finds_nothing(make_harness) -> None:
    """#1199 must not weaken idempotence: one stamp per card, ever.

    Deferring the pop behind the record read is only safe if the pop still
    happens exactly once on the path that stamps. A genuine redelivery of an
    already-settled resume turn re-reads a record that is still resolved, so
    without the GETDEL it would re-edit the card on every reclaim pass.

    THE MUTATION THIS CATCHES: dropping the pop (or making it a plain GET) while
    moving the read earlier -- the redelivery then produces a second
    ``card_updates`` entry.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-redelivered-stamp"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            resume = _resume_turn(
                "[approval resolved] approved by U9",
                thread=thread,
                approval_id="appr-1",
                author="U9",
            )

            # The healthy pass: the card is settled and the ref is spent.
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(resume)
            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

            # The redelivery, in the same crash-before-done shape the reclaim loop
            # delivers, so the done short-circuit cannot mask the assertion.
            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            # Nothing further happened to the card: exactly one stamp across both
            # passes, and the ref stays absent.
            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_a_ref_belonging_to_another_approval_is_never_stamped(make_harness) -> None:
    """#1199: a popped ref is stamped only when it is THIS approval's card.

    The ref is keyed by thread alone, so nothing in it tied the card to the
    approval whose verdict is about to be written onto it. Under the old
    pop-first order that pairing was correct by construction -- the pop was the
    first I/O after the entry guard. Reading the record first opens a window (a
    full HTTP round trip on a 30s client) in which another worker can pause the
    SAME thread for a new approval and overwrite the ref: ``remember`` is a plain
    overwriting SET, and this method holds no cross-worker lock. Stamping then
    rewrites approval B's card -- one whose Approve/Reject buttons are live for a
    still-pending request -- with approval A's verdict, and a human reads a
    pending governance request as already approved. The same corruption arrives
    by a second route: a ref left behind by a permanently unstampable pass
    lingers for its 14-day TTL and is popped by a LATER approval's resume.

    So the ref carries its approval id and a mismatch refuses to stamp, leaving
    the mismatched card live with its buttons rather than having it state a
    verdict about a different approval. The refusal also puts the ref back, so
    the approval it really belongs to can still settle its own card -- pinned by
    ``test_a_mismatched_ref_is_put_back_so_its_own_approval_can_settle`` below.

    The reader here is healthy, so the pairing check is the ONLY thing between
    this turn and a stamp -- that is what makes the test prove the pairing check
    rather than some earlier guard.

    THE MUTATION THIS CATCHES: delete the approval-id comparison from
    ``Kernel._finalize_settled_card``. Approval B's live card is then rewritten
    with approval A's "approved by U9" and ``card_updates`` stops being empty.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-mismatched-ref"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            # ``RecordingApprovals`` mints ids by request count, so this thread's
            # remembered ref belongs to approval "appr-1".
            await _pause_awaiting_approval(h, thread)

            # A resume turn for a DIFFERENT approval on the same thread, which is
            # what the overwrite window (and the stale-ref window) produce.
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-99",
                    author="U9",
                )
            )

            # The record read happened and came back healthy, so nothing earlier
            # than the pairing check can explain the refusal below.
            assert reader.reads == ["appr-99"]
            # The wrong card was NOT stamped. This is the corruption prevented:
            # approval appr-1's card still shows its live buttons instead of
            # claiming appr-99's verdict.
            assert h.sink.card_updates == []
            # And the resume itself completed normally -- the whole method is
            # best-effort and must never fail a resume.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_a_mismatched_ref_is_put_back_so_its_own_approval_can_settle(
    make_harness,
) -> None:
    """#1199: a refusal must not destroy the OTHER approval's only pointer.

    The popped ref belongs to a DIFFERENT, still-pending approval, so dropping
    it on the floor strands that approval's card with live Approve/Reject
    buttons that nothing will ever settle -- the #1199 harm class, reintroduced
    through the refusal instead of through the record read. Putting the ref back
    keeps the pairing honest AND keeps the card settleable: the approval the ref
    actually belongs to pops it on its own resume, pairs, and stamps.

    The put-back must not clobber a newer ref, so it is conditional: the pop
    already closed the overwrite window, and the only way a conditional write
    loses is that something newer arrived on the thread -- in which case not
    writing is the correct outcome.

    THE MUTATION THIS CATCHES: delete the put-back from the refusal path in
    ``Kernel._finalize_settled_card``. The key is then gone after the mismatched
    pass, so approval appr-1's own resume finds no ref and ``card_updates``
    stays empty forever.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-putback-ref"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            # The pause posts appr-1's card and remembers where it lives.
            await _pause_awaiting_approval(h, thread)

            # A resume for a DIFFERENT approval pops appr-1's ref and refuses.
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-99",
                    author="U9",
                )
            )
            assert h.sink.card_updates == []

            # The ref survives the refusal AND still describes appr-1's card --
            # not merely surviving bytes, the same channel/ts/summary the live
            # card was posted at, so a later stamp lands on the right message.
            restored = await _peek_card_ref(h, thread)
            assert restored is not None, "the mismatched ref was destroyed, not put back"
            assert restored["approval_id"] == "appr-1"
            assert restored["summary"] == "Refund order 42"
            assert (restored["channel"], restored["ts"]) == ("C1", "posted-1")

            # And that is what the put-back is FOR: appr-1's own resume now pops
            # its ref, pairs, and settles its card. Without the put-back this
            # half is unreachable.
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert len(h.sink.card_updates) == 1
            _channel, ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert ts == "posted-1", "the stamp must land on appr-1's own card"
            assert message.text == "Refund order 42"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            # The stamp spent the ref, so idempotence is unchanged by the
            # put-back: one stamp per card, ever.
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_an_expiry_resume_for_another_approval_stamps_nothing_and_keeps_the_ref(
    make_harness,
) -> None:
    """#1199: the pairing check and its put-back apply on the EXPIRY branch too.

    The two branches differ only in whether they read the record, so the pop,
    the pairing check and the put-back are written once and cover both -- a
    check present on one path only is a wrong-card stamp on the other. The
    expiry branch is where the residual bites hardest: a resolve leaves one heal
    path (a human clicking the live buttons drives the dispatcher's payload-side
    stamp), while an expiry has no click at all, so a destroyed ref there is
    precisely the permanently-live card #419 exists to remove.

    THE MUTATION THIS CATCHES: gate the pairing check (or its put-back) to the
    resolve branch only -- e.g. ``if outcome is not None and ref.approval_id
    != ...``. The expiry resume for appr-99 then stamps appr-1's card expired,
    so ``card_updates`` stops being empty; gating only the put-back destroys the
    ref, so the key stops existing.
    """

    async def go() -> None:
        thread = "th-expiry-mismatched-ref"
        # No reader is wired on purpose: the expiry branch reads no record, so
        # nothing earlier than the pairing check can explain the refusal.
        async with make_harness(approvals=RecordingApprovals()) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("please discount", thread=thread))
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

            # The #412 sweeper's expiry resume, but for a DIFFERENT approval --
            # the shape the cross-worker overwrite window and a stale ref both
            # produce.
            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-99",
                    author="system",
                )
            )

            # appr-1's card was NOT disabled by appr-99's expiry: its request is
            # still pending and its buttons still mean something.
            assert h.sink.card_updates == []
            # And its ref survives, so appr-1's own terminal transition can still
            # settle the card. On this branch there is no click to heal it.
            surviving = await _peek_card_ref(h, thread)
            assert surviving is not None, "the expiry refusal destroyed the other approval's ref"
            assert surviving["approval_id"] == "appr-1"
            # The resume itself still completed: the teardown is best-effort.
            assert h.sink.last_text == "Acknowledged the expiry."

    asyncio.run(go())


def test_a_ref_remembered_before_the_pairing_existed_still_stamps(
    make_harness,
) -> None:
    """#1199: an entry written by a pre-upgrade worker carries no approval id.

    Refs live in Valkey for 14 days, so a deploy of the pairing check meets a
    population of entries with no ``approval_id`` key at all. An empty id means
    "cannot be paired", never "must be refused": refusing would strand EVERY
    card in flight at the rollout hour, live buttons and all, with no click on
    the CLI/API-resolve path to heal them. The window is one-shot and
    unrepeatable -- by the time the symptom is noticed the refs are gone -- so
    it gets exactly one chance to be right.

    The legacy payload is written straight into Valkey rather than produced by
    the harness, because the current ``remember`` cannot emit it: that shape no
    longer exists in this codebase, only in a running deployment's Valkey.

    THE MUTATION THIS CATCHES: drop the empty-id exemption from the pairing
    check in ``Kernel._finalize_settled_card`` (compare the ids unconditionally).
    Every legacy ref then mismatches and no pre-upgrade card is ever stamped.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-legacy-ref"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            # Overwrite the entry with the exact shape the pre-#1199 worker
            # wrote: same keys, no ``approval_id``.
            await h.async_redis.set(
                h.config.approval_card_key(_thread_key(thread)),
                json.dumps(
                    {
                        "channel": "C1",
                        "ts": "posted-1",
                        "summary": "Refund order 42",
                        "endpoint": None,
                        "requested_by": "U1",
                    }
                ),
            )

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # The legacy card IS stamped, exactly as it would have been before
            # the pairing existed -- summary, requester and verdict all intact.
            assert len(h.sink.card_updates) == 1
            channel, ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert (channel, ts) == ("C1", "posted-1")
            assert message.text == "Refund order 42"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            assert settled.requested_by == "U1"
            # And the ref was consumed, because a stamp was made.
            assert not await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))

    asyncio.run(go())


def test_a_present_but_null_approval_id_is_corrupt_not_pre_upgrade(
    make_harness,
) -> None:
    """#1199: the pre-upgrade exemption belongs to an ABSENT key, not a falsy one.

    The test above pins the exemption the rollout needs; this one pins its
    boundary. "Written before the field existed" is a statement about the key
    being ABSENT. A payload that carries ``"approval_id": null`` is a different
    fact entirely -- a shape-drifted or partially-populated entry, the natural
    output of any writer that models the field as optional, or of anyone with
    write access to ``curie:approval-card:<thread>`` -- and the pre-#1199 worker
    could not have produced it, because the key it wrote had no such member.

    Treating the two alike is what makes the exemption unbounded: an empty id
    short-circuits the pairing check entirely, so the entry becomes a guaranteed
    stamp on whatever channel/ts it names, with whatever verdict the resuming
    approval carries. ``pop`` already has the right answer for an entry it
    cannot trust -- return None and let the click path heal the card -- and a
    present-but-falsy id is exactly that case, not the rollout case.

    THE MUTATION THIS CATCHES: keeping ``str(data.get("approval_id") or "")`` in
    ``ApprovalCardStore.pop``. The ``or`` coalesces ``null`` (and ``0``,
    ``false``, ``[]``) to ``""``, which is indistinguishable from a pre-upgrade
    entry, so the ref pops, takes the exemption, and stamps appr-1's verdict on a
    card nothing has paired it to.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-null-id-ref"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)

            # Every field ``remember`` writes, populated -- only the id is null.
            # Written straight into Valkey because ``remember`` cannot emit this
            # shape (and, per the store-level test below, must never learn to).
            await h.async_redis.set(
                h.config.approval_card_key(_thread_key(thread)),
                json.dumps(
                    {
                        "channel": "C1",
                        "ts": "posted-1",
                        "summary": "Refund order 42",
                        "endpoint": None,
                        "requested_by": "U1",
                        "approval_id": None,
                    }
                ),
            )

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # Nothing was stamped: the entry is corrupt, so there is no ref, so
            # there is no card the kernel can claim to be settling.
            assert h.sink.card_updates == []
            # And the refusal is the ENTRY's doing, not an unreadable record: the
            # reader is healthy and was read for this very approval, so the only
            # thing standing between that verdict and a stamp is the null id.
            assert reader.reads == ["appr-1"]
            # The run itself continued -- the teardown is best-effort.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_the_remembered_approval_id_matches_the_resume_events_approval_id() -> None:
    """#1199: the two sides of the pairing must agree on the id STRING.

    The check is a raw string comparison across the api/worker seam. The worker
    remembers ``str(created.id)`` at the pause; the resuming id is the middle of
    the API's ``resume_event_id(approval.id)``. Every other test in this file
    fakes BOTH sides (``RecordingApprovals`` mints ``appr-<n>`` and
    ``_resume_turn`` interpolates whatever string it is handed), so a drift in
    either representation -- hex without dashes, a case change, a non-canonical
    UUID rendering -- would refuse every card settle on every thread fleet-wide
    while the whole suite stayed green, with a single ``logger.info`` as the
    only signal.

    This drives a real ``uuid.UUID`` through the API's builder and the worker's
    parser, which is why it needs no harness: the seam IS the unit.

    THE MUTATION THIS CATCHES: either side changing its id formatting -- e.g.
    ``resumequeue.resume_event_id`` emitting ``approval_id.hex``, or the
    worker's parser normalizing the middle it recovers.
    """

    from curie_api.resumequeue import resume_event_id
    from curie_worker.kernel import _approval_id_from_resume_event

    approval_id = uuid.UUID("6f1c8b3e-9c2a-4f5d-8a71-2b3c4d5e6f70")
    assert _approval_id_from_resume_event(resume_event_id(approval_id)) == str(
        approval_id
    )
    # Not a property of this one literal: a freshly minted id round-trips too.
    minted = uuid.uuid4()
    assert _approval_id_from_resume_event(resume_event_id(minted)) == str(minted)


def test_a_resolve_with_no_reader_leaves_the_ref_and_stamps_nothing(
    make_harness,
) -> None:
    """#1199: a PERMANENTLY unstampable resolve pass leaves the ref behind.

    "No reader configured" can never recover within this deployment, yet the
    #1199 ordering treats it like a transient blip: the record read returns
    nothing, so the pop is never reached and the ref survives to its TTL. That is
    the behavior the pairing check above has to be safe against, and it deserves
    its own test -- it was pinned only as a side-assertion on
    ``test_resolve_authored_by_a_system_named_actor_does_not_expire_the_card``,
    whose real subject is the expiry-vs-resolve discriminator, so a change to
    this ordering would have surfaced as a failure in a test named after
    something else entirely.

    THE MUTATION THIS CATCHES: move the ``pop`` back above the record read in
    ``Kernel._finalize_settled_card`` (or give the no-reader arm its own eager
    pop). The ref is then consumed by a pass that stamped nothing.
    """

    async def go() -> None:
        thread = "th-permanent-no-reader"
        async with make_harness(approvals=RecordingApprovals()) as h:  # no reader wired
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # Nothing was stamped: with no reader there is no verdict to state,
            # and guessing one is worse than a card that still shows buttons.
            assert h.sink.card_updates == []
            # The ref survives, because the pop is claimed only when a stamp is
            # actually attempted.
            assert await h.async_redis.exists(h.config.approval_card_key(_thread_key(thread)))
            # The run continued regardless: the stamp is an enrichment.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_restore_declines_to_clobber_a_newer_card_but_still_puts_a_ref_back(
    make_harness,
) -> None:
    """#1199: ``ApprovalCardStore.restore`` writes CONDITIONALLY, and it writes.

    Deliberately a store-level test rather than a ``Kernel.process_event`` drive,
    unlike every other test in this file: the property under test is the store's
    write MODE, not a kernel decision, and the losing case needs a newer
    ``remember`` landing between a ``pop`` and its put-back -- a window the kernel
    cannot be made to open from the outside without inventing a concurrency
    scenario it does not actually produce. The store is where the race is
    constructible, so that is where it is pinned. Real Valkey throughout (the
    harness's own store); a fake would be pinning the mock's write mode, not
    Valkey's ``SET NX``.

    The put-back exists because ``_finalize_settled_card`` has to pop before it
    can tell whose ref it holds, so a ref belonging to a different, still-pending
    approval has to go back. It is conditional because the ONLY way that write
    loses is that a newer ``remember`` landed on the thread in between -- the pop
    already emptied the key -- and then the newer entry is the one that must
    survive. An unconditional put-back resurrects the older card's ref over it,
    which is the wrong-card stamp this branch exists to close.

    THE MUTATIONS THIS CATCHES:
    -- flipping ``_write``'s ``nx`` default to True/False, or dropping ``nx=True``
       from ``restore``: appr-A's stale ref clobbers appr-B's live one, and the
       surviving entry describes the wrong card.
    -- making ``restore`` a no-op (or deleting its write): the second half below
       finds nothing to pop back, so the mismatched ref is destroyed after all.
    """

    async def go() -> None:
        thread = "th-restore-conditional"
        async with make_harness() as h:
            store = h.card_store

            # 1. Approval A's card is remembered, then popped exactly as
            # ``_finalize_settled_card`` pops it -- the key is now empty and the
            # caller holds A's ref.
            await store.remember(
                thread,
                channel="C-alpha",
                ts="ts-alpha",
                summary="Refund order 42",
                endpoint=None,
                approval_id="appr-A",
                requested_by="U1",
            )
            ref_a = await store.pop(thread)
            assert ref_a is not None and ref_a.approval_id == "appr-A"

            # 2. A NEWER approval lands on the same thread inside the window --
            # a different card, in a different channel, at a different ts.
            await store.remember(
                thread,
                channel="C-beta",
                ts="ts-beta",
                summary="Delete the prod bucket",
                endpoint=None,
                approval_id="appr-B",
                requested_by="U2",
            )

            # 3. The put-back of A's ref must decline rather than overwrite B.
            await store.restore(thread, ref_a)

            surviving = await store.pop(thread)
            assert surviving is not None, "the entry vanished; nothing is stampable"
            assert surviving.approval_id == "appr-B", "A's stale ref clobbered the newer card"
            assert (surviving.channel, surviving.ts) == ("C-beta", "ts-beta")
            assert surviving.summary == "Delete the prod bucket"

            # And the companion, without which a ``restore`` that wrote NOTHING
            # would satisfy every assertion above: with nothing newer in the
            # window, the put-back does put the ref back, whole.
            await store.remember(
                thread,
                channel="C-gamma",
                ts="ts-gamma",
                summary="Rotate the signing key",
                endpoint=None,
                approval_id="appr-C",
                requested_by="U3",
            )
            ref_c = await store.pop(thread)
            assert ref_c is not None and ref_c.approval_id == "appr-C"
            assert await store.pop(thread) is None, "the pop must leave the key empty"

            await store.restore(thread, ref_c)
            assert await store.pop(thread) == ref_c

    asyncio.run(go())


def test_remember_refuses_to_write_an_empty_approval_id(make_harness) -> None:
    """#1199: today's code must be INCAPABLE of writing the compat sentinel.

    The exemption for an unpairable ref is only defensible while ``""`` provably
    means "written before the field existed". Every writer that can still emit it
    keeps the exemption alive past the rollout it was granted for, and a
    persisted empty id is not a benign one -- it is an entry the pairing check
    waves through, aimed at whatever channel/ts it names. Refusing at the write
    turns an unguardable entry into a loud failure at the moment it is produced,
    instead of a silent stamp days later on someone else's card.

    Deliberately a store-level test rather than a ``Kernel.process_event`` drive,
    for the same reason as the ``restore`` test above: the input is not
    constructible from the outside. The kernel remembers ``str(created.id)`` off
    the approvals API response, and every creator in this suite mints a real id,
    so reaching this guard through the kernel would mean inventing a fake that
    returns ``{"id": ""}`` -- pinning the fake's shape, not the store's contract.
    The guard lives on ``remember``, so that is where it is pinned.

    THE MUTATION THIS CATCHES: deleting the empty-id guard from
    ``ApprovalCardStore.remember``. The call then succeeds and leaves an entry in
    Valkey whose id the pairing check cannot use, for the full 14-day TTL.
    """

    async def go() -> None:
        thread = "th-remember-empty-id"
        async with make_harness() as h:
            with pytest.raises(ValueError, match="approval_id"):
                await h.card_store.remember(
                    thread,
                    channel="C1",
                    ts="posted-1",
                    summary="Refund order 42",
                    endpoint=None,
                    approval_id="",
                    requested_by="U1",
                )

            # Refused, not written-then-regretted: nothing reached the key, so
            # there is no unpairable entry to pop and no partial write to reason
            # about later.
            assert not await h.async_redis.exists(h.config.approval_card_key(thread))
            assert await h.card_store.pop(thread) is None

    asyncio.run(go())


# --- Best-effort resume reply when the CLI stub endpoint is dead (#708) --------


def test_resume_reply_best_effort_completes_offline_when_endpoint_is_dead(
    make_harness,
) -> None:
    """AC-708-1/2 (#708, PRIMARY): a resolved approval's resume turn whose per-turn
    reply endpoint is the now-dead CLI stub, delivered on a worker with NO distinct
    default transport (the pure-offline local loop), must still COMPLETE -- the
    granted tool executes exactly once and the turn reaches terminal ACK -- instead
    of dead-lettering because the reply cannot be delivered.

    Today the reply-delivery ``update`` raises the aiohttp transport error
    ``_with_transport_fallback`` re-raises when there is no distinct default (#530
    only rescues the has-default case), so ``process_event`` escapes; the consumer
    then leaves the entry pending, redelivers it to the delivery cap, and
    dead-letters it (#505) -- a full re-run per redelivery and the resolved approval
    never completes. The done marker is the proof it did NOT: it is written only
    once the turn is terminally handled (the consumer then acks it).

    The fix makes a resume turn's reply best-effort: the kernel gates the new
    ``best_effort_unreachable`` flag on ``_is_approval_resume(event_id)`` for the
    reply-delivery ``update`` calls (streaming edits + final reply). The resume
    ``event_id`` shape (``approval-<uuid>-resolved``) is authored by
    ``resumequeue.resume_event_id`` -- the format authority the worker recognizer
    keys off across the api/worker seam.
    """

    async def go() -> None:
        from curie_api.resumequeue import resume_event_id

        grant_event = resume_event_id(uuid.uuid4())
        binding = GrantBinding(
            grant_event_id=grant_event, grant_tool="mcp__github__create_issue"
        )
        async with make_harness(binding=binding) as h:
            # Offline local loop: the reply endpoint (the CLI stub) is dead, and the
            # worker sink has NO distinct default transport to fall back to.
            h.sink.dead_endpoints.add(_CLI_STUB)

            # The granted tool runs to done in the runner during the resume turn.
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            resume_turn = QueuedTurn(
                event_id=grant_event,
                conversation_id="th-offline-resume",
                author="U9",
                text="[approval resolved] approved by U9",
                reply_handle=ReplyHandle(
                    kind="slack", channel="C1", placeholder="p-1", endpoint=_CLI_STUB
                ),
                received_at="2026-07-14T00:00:00+00:00",
            )

            # Must NOT raise: a dead reply endpoint on a resume turn no longer
            # dead-letters the resolved approval.
            await h.kernel.process_event(resume_turn)

            # Terminal ACK, not dead-letter.
            assert await h.async_redis.exists(h.config.done_key(grant_event))

            # The granted tool executed exactly once: the resume turn opened a
            # single runner turn, and that claim carried the one-shot #430 grant.
            assert h.runner.opened == ["[approval resolved] approved by U9"]
            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert (
                resumed_env.get("CURIE_APPROVAL_GRANT_TOOL")
                == "mcp__github__create_issue"
            )

    asyncio.run(go())


def test_normal_turn_reply_stays_loud_when_endpoint_is_dead(make_harness) -> None:
    """AC-708-4 (#708): the best-effort swallow is scoped to resume turns. A NORMAL
    (non ``approval-<uuid>-resolved``) turn hitting the same dead endpoint + no
    distinct default must STILL fail loudly -- a fresh local turn whose stub crashed
    mid-turn is a genuine failure that must surface, not silently complete. Extends
    ``test_no_fallback_when_no_default_is_configured``'s intent to the kernel.

    The transport error propagates out of ``process_event`` (leaving the entry
    pending for reclaim), so the turn is NOT marked done -- the inverse of the
    resume case above."""

    async def go() -> None:
        async with make_harness() as h:  # no binding -> a plain, non-resume turn
            h.sink.dead_endpoints.add(_CLI_STUB)
            h.runner.default_script = [Final(text="done", status=DONE)]
            ev = _qevent(
                "hello",
                thread="th-normal-dead",
                event_id="ev-normal-1",  # not the resume shape
                endpoint=_CLI_STUB,
            )

            with pytest.raises(aiohttp.ClientError):
                await h.kernel.process_event(ev)

            # Not silently completed: no done marker was written.
            assert not await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())
