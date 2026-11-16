"""Factory retries across the worker, the API routes, SQL and the stream (#5424, ADR 0269).

Every test drives the real consumer, the real kernel, the real dispatch client
over HTTP, the real API routes and the real reconciler pass. What the worker
reported is read off its finish requests, and what the policy decided is read
from SQL.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import httpx
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, SideEffectFlag, parse_ndjson_line
from curie_worker.kernel import attempt, delivery, failures
from curie_worker.kernel import work_items as kernel_work_items
from curie_worker.sandbox.types import OwnerBusy, SandboxTermination
from curie_worker.workitem_dispatch import WorkItemConflict

from apps.worker.tests.factory_retry_fixtures import (
    WAIT_S,
    FactoryRetryCase,
    Item,
    clean_failure,
    factory_retry,
    factory_retry_database,
    frame_line,
    load_vectors,
)
from apps.worker.tests.queue_fixtures import wait_until

__all__ = ["factory_retry", "factory_retry_database"]

VECTORS = load_vectors()
CASES: dict[str, dict[str, Any]] = {case["id"]: case for case in VECTORS["cases"]}
WORKER_CASES: list[dict[str, Any]] = [case for case in VECTORS["cases"] if case.get("worker")]
MALFORMED: list[dict[str, Any]] = VECTORS["malformed_hints"]
TRANSIENT = {"cause": "model_unreachable", "retry_safety": "no_effects", "retry_condition": None}
# The runner stream budget for the tests that let a stalled turn time out.
SHORT_STREAM = {"runner_total_timeout_s": 1.0}


def _ended_failed() -> Final:
    return Final(text="The run failed.", status=SessionStatus.CLASSIFIED_FAILURE)


# -- AC1: three attempts, shared across causes, exactly once ---------------------------------


def test_model_unreachable_chain_runs_three_attempts_through_http_ndjson_sql_and_streams(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        runners = await case.real_runners(4)
        work_item_id = case.item.work_item_id
        request_id = case.item.request_id
        for attempt_number in (1, 2, 3):
            await case.publish_wake(request_id)
            finish = await case.finished(request_id)
            assert finish.facts == TRANSIENT
            await case.settle()
            successor = await case.successor(request_id)
            if attempt_number == 3:
                assert successor is None
            else:
                assert successor is not None and successor["status"] == "waiting", successor
                request_id = successor["id"]
        chain = await case.requests(work_item_id)
        assert [(row["status"], row["terminal_cause"]) for row in chain] == [
            ("failed", "model_unreachable")
        ] * 3
        assert await case.used(work_item_id) == 2
        assert len({row["runtime_claim_name"] for row in chain}) == 3
        assert len({row["runtime_sandbox_name"] for row in chain}) == 3
        # Three sandboxes each ran one real runner turn; the spare was never claimed.
        assert sorted(runner.turns for runner in runners) == [0, 1, 1, 1]
        assert case.h.runner.opened == []
        assert [call.status for call in case.calls_to("start")] == [200, 200, 200]
        published = case.stream_event_ids()
        await case.reconcile()
        await case.settle()
        assert case.stream_event_ids() == published
        assert len(await case.requests(work_item_id)) == 3

    case.run(exercise())


def test_mixed_owner_loss_and_transient_chain_never_starts_a_fourth_attempt(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        first = case.item
        case.script(end="stall")
        await case.publish_wake(first.request_id)
        await case.held()
        await case.lose_owner(first.request_id)
        await case.record_teardown(first.request_id)
        await case.settle()
        second = await case.successor(first.request_id)
        assert second is not None and second["owner_lost_retry"] is True, second
        assert await case.used(first.work_item_id) == 1
        assert not [
            call for call in case.calls_to("finish", first.request_id) if call.status == 200
        ]

        case.script(*clean_failure())
        await case.publish_wake(second["id"])
        assert (await case.finished(second["id"])).facts == TRANSIENT
        await case.settle()
        third = await case.successor(second["id"])
        assert third is not None and third["owner_lost_retry"] is False, third
        assert await case.used(first.work_item_id) == 2

        case.script(*clean_failure())
        await case.publish_wake(third["id"])
        assert (await case.finished(third["id"])).facts == TRANSIENT
        await case.settle()
        assert await case.successor(third["id"]) is None
        chain = await case.requests(first.work_item_id)
        assert [(row["status"], row["terminal_cause"]) for row in chain] == [
            ("failed", "owner_lost"),
            ("failed", "model_unreachable"),
            ("failed", "model_unreachable"),
        ]
        assert await case.used(first.work_item_id) == 2
        assert len(case.h.runner.opened) == 3
        published = case.stream_event_ids()
        await case.reconcile()
        await case.settle()
        assert case.stream_event_ids() == published
        assert len(case.h.runner.opened) == 3
        assert len(await case.requests(first.work_item_id)) == 3

    case.run(exercise())


def test_redelivered_execute_wake_of_a_finished_request_starts_nothing(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.script(*clean_failure())
        event_id = await case.publish_wake(item.request_id)
        await case.finished(item.request_id)
        await case.settle()
        before = await case.requests(item.work_item_id)
        opened = len(case.h.runner.opened)
        calls = len(case.calls)

        case.republish(event_id)
        await case.settle()
        assert len(case.h.runner.opened) == opened
        assert [call.route for call in case.calls[calls:] if call.status == 200] == []
        assert await case.requests(item.work_item_id) == before
        assert await case.used(item.work_item_id) == 1
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["status"] == "waiting", successor

        # Liveness: the successor's own wake does start a turn on this stack.
        case.script(*clean_failure())
        await case.publish_wake(successor["id"])
        await case.finished(successor["id"])
        await case.settle()
        assert len(case.h.runner.opened) == opened + 1

    case.run(exercise())


def test_lost_finish_response_resends_the_same_facts_and_admits_one_successor(
    factory_retry: FactoryRetryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = factory_retry

    async def no_wait(_delay: float) -> None:
        await asyncio.sleep(0)

    # The kernel's settlement retry waits out a bounded delay first; skip the wait.
    monkeypatch.setattr(kernel_work_items, "_settle_sleep", no_wait)

    async def lose_the_answer(item: Item, fault: Exception) -> None:
        case.script(*clean_failure())
        case.finish_fault = fault
        await case.publish_wake(item.request_id)
        await wait_until(
            lambda: len(case.calls_to("finish", item.request_id)) == 2,
            "the finish to be sent again",
            timeout=WAIT_S,
        )
        await case.settle()
        assert case.finish_fault is None
        first, second = case.calls_to("finish", item.request_id)
        assert (first.status, second.status, second.code) == (200, 409, "not_running")
        assert second.body == first.body
        assert first.facts == TRANSIENT
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        assert await case.used(item.work_item_id) == 1
        assert len(await case.requests(item.work_item_id)) == 2

    async def exercise() -> None:
        # The connection died after the API settled: the client replays the request.
        await lose_the_answer(case.item, httpx.ReadError("example connection reset"))
        # The answer arrived unreadable: the kernel's settlement retry sends it again.
        released = len(case.h.fake_k8s.deleted_claims)
        await lose_the_answer(await case.admit(), httpx.DecodingError("example unreadable answer"))
        await wait_until(
            lambda: len(case.h.fake_k8s.deleted_claims) > released,
            "the settled run to release its sandbox",
            timeout=WAIT_S,
        )

    case.run(exercise())


def test_stale_runtime_epoch_finish_is_refused_and_the_live_owner_settles(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.script(end="stall", tail=clean_failure())
        await case.publish_wake(item.request_id)
        await case.held()
        epoch = int((await case.row(item.request_id))["runtime_epoch"])
        with pytest.raises(WorkItemConflict) as refused:
            await case.dispatch.finish(
                item.request_id,
                runtime_epoch=epoch + 1,
                outcome="failed",
                cause="model_unreachable",
                detail=None,
                ci_fix_round=None,
                retry_safety="no_effects",
                retry_condition=None,
                exhausted_volume=None,
            )
        assert refused.value.code == "stale_owner"
        row = await case.row(item.request_id)
        assert (row["status"], row["runtime_epoch"]) == ("running", epoch)
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

        case.release_turn()
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.body["runtime_epoch"] == epoch
        assert finish.facts == TRANSIENT
        assert [(call.status, call.code) for call in case.calls_to("finish", item.request_id)] == [
            (409, "stale_owner"),
            (200, None),
        ]
        assert await case.successor(item.request_id) is not None
        assert await case.used(item.work_item_id) == 1

    case.run(exercise())


# -- AC2: a fresh request, never a resumed one, and only after a clean observed end ----------


def test_successor_starts_on_a_fresh_sandbox_with_fresh_deadlines_and_the_old_request_unchanged(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.script(*clean_failure())
        await case.publish_wake(item.request_id)
        await case.finished(item.request_id)
        await case.settle()
        before = await case.row(item.request_id)
        waiting = await case.successor(item.request_id)
        assert waiting is not None

        case.script(*clean_failure())
        await case.publish_wake(waiting["id"])
        await case.finished(waiting["id"])
        await case.settle()
        assert await case.row(item.request_id) == before
        ran = await case.row(waiting["id"])
        assert ran["id"] != before["id"]
        assert ran["retry_of_request_id"] == before["id"]
        assert ran["sequence"] == before["sequence"] + 1
        assert ran["runtime_claim_name"] != before["runtime_claim_name"]
        assert ran["runtime_sandbox_name"] != before["runtime_sandbox_name"]
        assert ran["wait_deadline"] > before["wait_deadline"]
        assert ran["started_at"] > before["started_at"]
        assert ran["execution_deadline"] > before["execution_deadline"]
        # The first attempt's sandbox was deleted; the second never ran on it.
        assert before["runtime_claim_name"] in case.h.fake_k8s.deleted_claims

    case.run(exercise())


def test_flagged_turn_reports_flagged_and_admits_nothing(factory_retry: FactoryRetryCase) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.script(SideEffectFlag(tool="deploy"), *clean_failure())
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.facts == {**TRANSIENT, "retry_safety": "flagged"}
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0
        await case.control_admits_one()

    case.run(exercise())


def test_uncertain_continuation_reports_uncertain_and_admits_nothing(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        done = Final(text="The work is ready.", status=SessionStatus.DONE)
        item = case.item
        # The turn ends without publishing and the runner refuses its continuation.
        case.script(done, fail_next_open=True)
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.facts == {
            "cause": "runner_error",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }
        assert len(case.h.runner.opened) == 2
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

        # Liveness: a continuation the runner ran and ended cleanly is observed, and retries.
        control = await case.admit()
        case.script(done)
        case.script(*clean_failure())
        await case.publish_wake(control.request_id)
        finish = await case.finished(control.request_id)
        await case.settle()
        assert finish.facts == TRANSIENT
        assert len(case.h.runner.opened) == 4
        assert await case.successor(control.request_id) is not None
        assert await case.used(control.work_item_id) == 1

    case.run(exercise())


def test_approval_resume_and_ci_fix_turns_report_uncertain_when_unflagged(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        case.script_default(*clean_failure())
        resumed = case.item
        fixed = await case.admit()
        for item, turn in (
            (resumed, case.approval_resume_turn(resumed)),
            (fixed, case.ci_fix_turn(fixed)),
        ):
            opened = len(case.h.runner.opened)
            epoch = await case.start_directly(item)
            case.enqueue(turn)
            finish = await case.finished(item.request_id)
            await case.settle()
            assert finish.body["runtime_epoch"] == epoch
            assert finish.facts == {**TRANSIENT, "retry_safety": "uncertain"}
            assert len(case.h.runner.opened) > opened
            assert await case.successor(item.request_id) is None
            assert await case.used(item.work_item_id) == 0
        # Liveness: the same clean failure on a run's own execute turn admits one.
        await case.control_admits_one()

    case.run(exercise())


def test_refusal_finishes_without_a_turn_report_uncertain(factory_retry: FactoryRetryCase) -> None:
    case = factory_retry
    refused = {"cause": "runner_escalated", "retry_safety": "uncertain", "retry_condition": None}

    async def exercise() -> None:
        unbound = case.item
        stopped = await case.admit()

        # The request's channel binding is gone when its approval resume arrives.
        epoch = await case.start_directly(unbound)
        await case.execute(
            "UPDATE curie.agent_channels SET address = 'C0EXAMPLE2' WHERE agent_id = :agent",
            {"agent": case.agent_id},
        )
        case.enqueue(case.approval_resume_turn(unbound))
        finish = await case.finished(unbound.request_id)
        await case.settle()
        await case.execute(
            "UPDATE curie.agent_channels SET address = 'C0EXAMPLE1' WHERE agent_id = :agent",
            {"agent": case.agent_id},
        )
        assert finish.body["runtime_epoch"] == epoch
        assert finish.facts == refused

        # The agent has no active deployment when its CI fix round arrives.
        epoch = await case.start_directly(stopped)
        await case.execute(
            "UPDATE curie.deployments SET status = 'stopped' WHERE id = :id",
            {"id": case.deployment_id},
        )
        case.enqueue(case.ci_fix_turn(stopped))
        finish = await case.finished(stopped.request_id)
        await case.settle()
        await case.execute(
            "UPDATE curie.deployments SET status = 'active' WHERE id = :id",
            {"id": case.deployment_id},
        )
        assert finish.body["runtime_epoch"] == epoch
        assert finish.facts == refused

        # Neither refusal claimed a sandbox or opened a runner turn.
        assert case.h.fake_k8s.claim_envs == []
        assert case.h.runner.opened == []
        for item in (unbound, stopped):
            assert (await case.row(item.request_id))["status"] == "failed"
            assert await case.successor(item.request_id) is None
            assert await case.used(item.work_item_id) == 0
        await case.control_admits_one()

    case.run(exercise())


def test_owner_loss_successor_is_admitted_after_observed_teardown_and_is_marked_distinct(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        ordinary = await case.control_admits_one(case.item)
        assert ordinary["owner_lost_retry"] is False

        lost = await case.admit()
        case.script(end="stall")
        await case.publish_wake(lost.request_id)
        await case.held()
        await case.lose_owner(lost.request_id)
        # The runtime is gone, but until that is recorded there is no successor.
        row = await case.row(lost.request_id)
        assert (row["status"], row["terminal_cause"]) == ("cancellation_requested", "owner_lost")
        assert row["runtime_claim_name"] in case.h.fake_k8s.deleted_claims
        assert await case.successor(lost.request_id) is None
        assert await case.used(lost.work_item_id) == 0

        await case.record_teardown(lost.request_id)
        await case.settle()
        row = await case.row(lost.request_id)
        assert (row["status"], row["terminal_cause"]) == ("failed", "owner_lost")
        successor = await case.successor(lost.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        assert successor["owner_lost_retry"] is True
        assert await case.used(lost.work_item_id) == 1
        assert not [call for call in case.calls_to("finish", lost.request_id) if call.status == 200]

    case.run(exercise())


def test_scheduled_and_blocked_successors_are_not_dispatched_before_release(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    # Only a probed provider scope can release a block (#5424).
    blocking = CASES["model_credit_exhausted_on_a_probed_scope_is_blocked_on_credit"]

    async def exercise() -> None:
        retried = case.item
        case.script(*clean_failure())
        await case.publish_wake(retried.request_id)
        await case.finished(retried.request_id)
        await case.settle()
        scheduled = await case.successor(retried.request_id)
        assert scheduled is not None and scheduled["blocked_on"] is None, scheduled

        faulted = await case.admit()
        case.script(frame_line(blocking["worker"]["error_frame"]), _ended_failed())
        await case.publish_wake(faulted.request_id)
        finish = await case.finished(faulted.request_id)
        await case.settle()
        assert finish.facts == blocking["finish"]
        blocked = await case.successor(faulted.request_id)
        assert blocked is not None, finish
        assert blocked["blocked_on"] == blocking["expected"]["blocked_on"]

        opened = len(case.h.runner.opened)
        await case.reconcile()
        await case.settle()
        for waiting in (scheduled, blocked):
            row = await case.row(waiting["id"])
            assert (row["status"], row["published_generation"]) == ("waiting", None)
            assert case.wakes_for(waiting["id"]) == []
        assert len(case.h.runner.opened) == opened

        # Both are past their due time now. Only the typed block keeps one asleep.
        await case.make_due(blocked["id"])
        case.script(*clean_failure())
        await case.publish_wake(scheduled["id"])
        await case.finished(scheduled["id"])
        await case.settle()
        assert len(case.h.runner.opened) == opened + 1
        row = await case.row(blocked["id"])
        assert (row["status"], row["published_generation"]) == ("waiting", None)
        assert case.wakes_for(blocked["id"]) == []
        assert case.calls_to("acquire", blocked["id"]) == []

    case.run(exercise())


# -- AC3: one attempt per started factory execute turn, and nothing else changes -------------


def test_started_factory_execute_failure_opens_exactly_one_runner_turn(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        assert case.h.config.max_attempts == 3
        # Every turn fails the same way, so a retry inside the delivery would open more.
        case.script_default(*clean_failure())
        item = case.item
        await case.publish_wake(item.request_id)
        await case.finished(item.request_id)
        await case.settle()
        assert len(case.h.runner.opened) == 1
        assert [call.status for call in case.calls_to("start", item.request_id)] == [200]
        assert [call.status for call in case.calls_to("finish", item.request_id)] == [200]
        assert (await case.row(item.request_id))["execution_attempts"] == 1
        assert await case.successor(item.request_id) is not None

    case.run(exercise())


def test_unstarted_run_still_defers_and_retries_as_before(factory_retry: FactoryRetryCase) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.issuer.fault = "lookalike"
        await case.publish_wake(item.request_id)
        await wait_until(
            lambda: any(call.status == 200 for call in case.calls_to("defer", item.request_id)),
            "the unstarted request to defer",
            timeout=WAIT_S,
        )
        await case.settle()
        # The workspace was tried once for each attempt the kernel allows a delivery.
        assert case.issuer.redemptions == case.h.config.max_attempts == 3
        (deferral,) = case.calls_to("defer", item.request_id)
        assert deferral.body["capacity"] is False
        row = await case.row(item.request_id)
        assert row["status"] == "waiting"
        assert (row["start_deferrals"], row["capacity_deferrals"]) == (1, 0)
        assert row["execution_attempts"] == 0
        assert case.calls_to("start", item.request_id) == []
        assert case.calls_to("finish", item.request_id) == []
        assert case.h.runner.opened == []
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

        # Liveness: the same request starts once its workspace can be prepared.
        case.issuer.fault = "normal"
        case.script(*clean_failure())
        await case.publish_wake(item.request_id)
        await case.finished(item.request_id)
        await case.settle()
        assert (await case.row(item.request_id))["execution_attempts"] == 1
        assert len(case.h.runner.opened) == 1

    case.run(exercise())


def test_interactive_turn_keeps_in_delivery_retries(factory_retry: FactoryRetryCase) -> None:
    case = factory_retry

    async def exercise() -> None:
        case.script_default(*clean_failure())
        case.enqueue(await case.interactive_turn())
        await case.settle()
        assert len(case.h.runner.opened) == case.h.config.max_attempts == 3
        # No dispatch route was called: the turn belongs to no WorkItem request.
        assert case.calls == []
        assert (await case.row(case.item.request_id))["status"] == "waiting"

    case.run(exercise())


def test_ci_fix_turn_keeps_kernel_retry_policy_and_its_round(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        epoch = await case.start_directly(item)
        case.script(*clean_failure())
        case.script(Final(text="The checks are fixed.", status=SessionStatus.DONE))
        case.enqueue(case.ci_fix_turn(item, round_=2))
        finish = await case.finished(item.request_id)
        await case.settle()
        # The failed attempt was retried inside the delivery, as before.
        assert len(case.h.runner.opened) == 2
        assert finish.body["runtime_epoch"] == epoch
        assert finish.body["ci_fix_round"] == 2
        assert finish.facts == {
            "cause": "ci_fix_unpublished",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }
        assert [call.status for call in case.calls_to("finish", item.request_id)] == [200]
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

    case.run(exercise())


def test_capacity_refusal_after_start_keeps_the_request_running_and_retries_inside_the_delivery(
    factory_retry: FactoryRetryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        cap_token = case.h.kernel._cap_caller_token_to_deadline
        refusals: list[dict[str, Any]] = []

        async def refuse_the_owner_slot_once(*args: object, **kwargs: object) -> object:
            if not refusals:
                row = await case.row(item.request_id)
                refusals.append(
                    {
                        "status": row["status"],
                        "epoch": row["runtime_epoch"],
                        "opened": len(case.h.runner.opened),
                    }
                )
                raise OwnerBusy
            return await cap_token(*args, **kwargs)

        monkeypatch.setattr(
            case.h.kernel, "_cap_caller_token_to_deadline", refuse_the_owner_slot_once
        )
        case.script(end="stall", tail=clean_failure())
        await case.publish_wake(item.request_id)
        await case.held()
        # The refusal came after the SQL start and before any runner turn.
        (refusal,) = refusals
        assert (refusal["status"], refusal["opened"]) == ("running", 0)
        # The turn that follows it runs on the same request, inside the same delivery.
        row = await case.row(item.request_id)
        assert (row["status"], row["runtime_epoch"]) == ("running", refusal["epoch"])
        assert len(case.h.runner.opened) == 1
        assert [call.status for call in case.calls_to("start", item.request_id)] == [200]
        assert case.calls_to("finish", item.request_id) == []
        assert case.calls_to("defer", item.request_id) == []
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

        case.release_turn()
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.body["runtime_epoch"] == refusal["epoch"]
        assert finish.body["cause"] == "model_unreachable"
        assert len(case.h.runner.opened) == 1
        assert len(case.wakes_for(item.request_id)) == 1

    case.run(exercise())


# -- AC4: the cause, the safety fact and the carrier, as the shared vectors state them -------


@pytest.mark.parametrize("vector", WORKER_CASES, ids=[case["id"] for case in WORKER_CASES])
def test_worker_classification_matches_every_vector(vector: dict[str, Any]) -> None:
    worker = vector["worker"]
    acc = delivery._StreamAccumulator()
    if worker["error_frame"] is not None:
        frame = parse_ndjson_line(frame_line(worker["error_frame"]).text)
        # An error frame is applied to the accumulator alone.
        asyncio.run(
            attempt._apply_frame(cast(Any, None), frame, acc, cast(Any, None), cast(Any, None))
        )
        assert acc.classification == worker["classification"]
    end = worker["turn_end"]
    ended = end == "final" or (
        end in {"accepted_timeout", "observed_termination"} and not acc.saw_error_event
    )
    assert ended is worker["runner_ended"]
    turn = failures.TurnOutcome(
        terminal_ok=False,
        saw_side_effect=worker["saw_side_effect"],
        execution_uncertain=worker["execution_uncertain"],
        classification=acc.classification or worker["classification"],
        runner_ended=ended,
        retry_condition=acc.retry_condition,
    )
    condition = turn.retry_condition
    assert {
        "cause": failures._escalation_cause(turn),
        "retry_safety": failures.retry_safety(
            turn,
            marker_flagged=worker["marker_flagged"],
            own_execute_turn=worker["own_execute_turn"],
        ),
        "retry_condition": None if condition is None else condition.model_dump(mode="json"),
    } == vector["finish"]


def test_valid_transient_carrier_reaches_the_finish_body_unchanged(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    carriers = (
        CASES["model_unreachable_transient_service_carrier_retries"],
        CASES["model_unreachable_carrier_with_unknown_extra_key_still_retries"],
    )

    async def exercise() -> None:
        item = case.item
        for vector in carriers:
            case.script(frame_line(vector["worker"]["error_frame"]), _ended_failed())
            await case.publish_wake(item.request_id)
            finish = await case.finished(item.request_id)
            await case.settle()
            carrier = vector["finish"]["retry_condition"]
            assert carrier is not None
            assert finish.facts == vector["finish"]
            row = await case.row(item.request_id)
            assert row["retry_safety"] == vector["finish"]["retry_safety"]
            assert row["retry_condition_reason"] == carrier["reason"]
            admitted = await case.successor(item.request_id) is not None
            assert admitted is vector["expected"]["successor"]
            item = await case.admit()

    case.run(exercise())


@pytest.mark.parametrize("hint", MALFORMED, ids=[hint["id"] for hint in MALFORMED])
def test_malformed_hint_frame_stops_a_factory_turn_as_uncertain(
    factory_retry: FactoryRetryCase, hint: dict[str, Any]
) -> None:
    case = factory_retry

    async def exercise() -> None:
        item = case.item
        case.script(frame_line(hint["error_frame"]), _ended_failed())
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.facts == {
            "cause": "unclassified",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }
        assert len(case.h.runner.opened) == 1
        assert (await case.row(item.request_id))["status"] == "failed"
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0
        # Liveness: the same failure with a frame the worker can decode admits one.
        await case.control_admits_one()

    case.run(exercise())


@pytest.mark.parametrize("factory_retry", [{"max_delivery": 2}], indirect=True)
@pytest.mark.parametrize("kind", ["interactive", "ci_fix", "approval_resume"])
def test_malformed_frame_on_an_interactive_turn_still_escapes_to_the_consumer(
    factory_retry: FactoryRetryCase, kind: str
) -> None:
    case = factory_retry

    async def exercise() -> None:
        case.script_default(frame_line(MALFORMED[0]["error_frame"]), _ended_failed())
        if kind == "interactive":
            turn = await case.interactive_turn()
        else:
            await case.start_directly(case.item)
            turn = (
                case.ci_fix_turn(case.item)
                if kind == "ci_fix"
                else case.approval_resume_turn(case.item)
            )
        case.enqueue(turn)
        dead_letters = case.h.config.dead_letter_stream_name()
        await wait_until(
            lambda: turn.event_id in case.stream_event_ids(dead_letters),
            "the undecodable turn to reach the dead letter stream",
            timeout=WAIT_S,
        )
        await case.settle()
        # At most one open for each delivery: the frame error left the kernel as
        # raised. Classified as a runner error it would retry inside a delivery.
        assert 1 <= len(case.h.runner.opened) <= case.h.config.max_delivery == 2
        assert case.redis.exists(case.h.config.done_key(turn.event_id)) == 0
        assert case.calls_to("finish") == []
        if kind != "interactive":
            assert (await case.row(case.item.request_id))["status"] == "running"

        # Liveness: on a run's own execute turn the same frame settles the request.
        control = case.item if kind == "interactive" else await case.admit()
        await case.publish_wake(control.request_id)
        finish = await case.finished(control.request_id)
        await case.settle()
        assert finish.facts == {
            "cause": "unclassified",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }
        assert case.stream_event_ids(dead_letters) == [turn.event_id]

    case.run(exercise())


@pytest.mark.parametrize("factory_retry", [SHORT_STREAM], indirect=True)
def test_error_frame_then_accepted_timeout_reports_uncertain_and_admits_nothing(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    limited = ErrorEvent(message="Example provider error.", classification="rate-limit")

    async def exercise() -> None:
        item = case.item
        # The runner reports the error, then stalls until its timeout stop is accepted.
        case.script(limited, end="stall")
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert case.h.runner.timeout_calls == 1
        assert finish.facts == {
            "cause": "model_rate_limited",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

        # Liveness: the same error frame followed by the runner's own end admits one.
        control = await case.admit()
        case.script(limited, _ended_failed())
        await case.publish_wake(control.request_id)
        finish = await case.finished(control.request_id)
        await case.settle()
        assert finish.facts == {
            "cause": "model_rate_limited",
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        assert case.h.runner.timeout_calls == 1
        assert await case.successor(control.request_id) is not None
        assert await case.used(control.work_item_id) == 1

    case.run(exercise())


@pytest.mark.parametrize("factory_retry", [SHORT_STREAM], indirect=True)
def test_accepted_timeout_and_observed_termination_without_an_error_frame_settle_terminal(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    stopped = CASES["runner_timeout_accepted_stop_is_terminal"]
    terminated = CASES["sandbox_terminated_observed_termination_is_terminal"]
    unconfirmed = CASES["runner_timeout_unconfirmed_reports_uncertain"]

    async def ends(item: Item) -> dict[str, Any]:
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        hold = case.h.runner.hold
        if isinstance(hold, asyncio.Event):
            # A stop the runner refused leaves its turn open; end it for the next one.
            hold.set()
        await case.settle()
        return finish.facts

    async def settled_terminal(item: Item) -> None:
        # Nothing could release a wait on these, so none is admitted (#5424).
        assert (await case.row(item.request_id))["status"] == "failed"
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

    async def exercise() -> None:
        # The runner accepted the timeout stop: the end was observed.
        timed_out = case.item
        case.script(end="stall")
        assert await ends(timed_out) == stopped["finish"]
        assert stopped["expected"]["successor"] is False
        await settled_terminal(timed_out)

        # The connection dropped and the control plane reported the pod terminated.
        deleted = await case.admit()
        case.h.fake_k8s.termination = SandboxTermination("Deleted")
        case.script(end="drop")
        facts = await ends(deleted)
        case.h.fake_k8s.termination = None
        assert facts == terminated["finish"]
        assert terminated["expected"]["successor"] is False
        await settled_terminal(deleted)

        # The runner refused the timeout stop: nothing shows that the turn ended.
        refused = await case.admit()
        case.script(end="stall", timeout_status=409)
        assert await ends(refused) == unconfirmed["finish"]

        # The connection dropped and no termination was observed.
        dropped = await case.admit()
        case.script(end="drop")
        assert await ends(dropped) == {
            "cause": "runner_error",
            "retry_safety": "uncertain",
            "retry_condition": None,
        }

        for item in (refused, dropped):
            assert (await case.row(item.request_id))["status"] == "failed"
            assert await case.successor(item.request_id) is None
            assert await case.used(item.work_item_id) == 0
        assert len(case.h.runner.opened) == 4
        assert case.h.runner.timeout_calls == 2

    case.run(exercise())
