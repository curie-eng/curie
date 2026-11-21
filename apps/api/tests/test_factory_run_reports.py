"""Operator diagnostics survive the factory's terminal transcript cleanup (#5601)."""

# ruff: noqa: F811 (the imported fixture is requested by the tests)

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from curie_api.factory_run_reports import sweep_expired
from curie_api.langfuse import LangfuseClient
from curie_api.models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryRunReport,
    Publication,
    ThreadTranscript,
    WorkItem,
)
from curie_api.workitems.lifecycle import settle_terminal
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from test_workitem_outcomes import (  # noqa: F401
    PR_URL,
    WORKER_HEADERS,
    _admit,
    _agent,
    _cancel,
    _complete,
    _completed,
    _deadline_90_then_elapse,
    _detail,
    _facts,
    _finish,
    _open_pr,
    _publish,
    _resolve,
    _terminate,
    stack,
    with_session,
)


def _report(
    client: TestClient, headers: dict[str, str], item_id: uuid.UUID, run_id: uuid.UUID
) -> Any:
    if isinstance(client.app.state.langfuse, LangfuseClient):

        class EmptyTraces:
            async def list_factory_traces(
                self, session_id: str, started_at: datetime, terminal_at: datetime
            ):
                return [], "missing"

        client.app.state.langfuse = EmptyTraces()
    return client.get(f"/work-items/{item_id}/runs/{run_id}/report", headers=headers)


def test_langfuse_lookup_is_session_and_execution_bounded() -> None:
    pages = [
        {
            "data": [
                {"name": "curie-run:wanted", "sessionId": "wanted", "id": "trace-1"},
                {"name": "curie-run:wanted", "sessionId": "other", "id": "wrong"},
                {"name": "other", "sessionId": "wanted", "id": "wrong-tool"},
            ],
            "meta": {"totalPages": 2},
        },
        {
            "data": [
                {"name": "curie-run:wanted", "sessionId": "wanted", "id": "trace-1"},
                {"name": "curie-run:wanted", "sessionId": "wanted", "id": "trace-2"},
            ],
            "meta": {"totalPages": 2},
        },
    ]
    seen: list[dict[str, Any]] = []

    async def get(path: str, params: dict[str, Any]) -> dict[str, Any]:
        assert path == "/api/public/traces"
        seen.append(dict(params))
        return pages.pop(0)

    client = object.__new__(LangfuseClient)
    client._get = get  # type: ignore[method-assign]
    start = datetime(2026, 11, 12, 12, tzinfo=UTC)
    trace_ids, status = asyncio.run(
        client.list_factory_traces("wanted", start, start + timedelta(minutes=5))
    )
    assert (trace_ids, status) == (["trace-1", "trace-2"], "time_matched")
    assert seen[0]["sessionId"] == "wanted"
    assert seen[0]["fromTimestamp"] == start.isoformat()
    assert "toTimestamp" in seen[0]
    assert seen[1]["page"] == 2


def test_report_uses_langfuse_trace_ids_without_copying_observations(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    _finish(seeded.request_id, seeded.runtime_epoch, "failed", "no_pull_request")

    async def creation_time(session: AsyncSession) -> datetime:
        request = await session.get(ExecutionRequest, seeded.request_id)
        assert request is not None and request.started_at is not None
        assert request.created_at < request.started_at
        return request.created_at

    created_at = with_session(creation_time)

    class Traces:
        async def list_factory_traces(
            self, session_id: str, started_at: datetime, terminal_at: datetime
        ):
            assert str(agent["agent_id"]) in session_id
            assert started_at == created_at
            assert started_at <= terminal_at
            return ["trace-current"], "time_matched"

    stack.app.state.langfuse = Traces()
    report = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id).json()
    assert report["trace_ids"] == ["trace-current"]
    assert report["sources"]["traces"] == "time_matched"
    assert "transcript" not in report["sources"]
    assert all(event["kind"] != "conversation" for event in report["events"])


def test_terminal_report_retains_redacted_session_after_transcript_expiry(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)

    async def seed(session: AsyncSession) -> None:
        item = await session.get(WorkItem, seeded.work_item_id)
        assert item is not None
        session.add(
            ThreadTranscript(
                agent_id=item.agent_id,
                binding_scope=None,
                thread_key=item.conversation_id,
                value=[
                    {
                        "type": "turn",
                        "ts": datetime.now(UTC).isoformat(),
                        "user": "Implement the ticket",
                        "assistant": (
                            "Five blocking findings remain. X-API-Key: SECRET_EXAMPLE_VALUE "
                            "Authorization: OPAQUE_PRIVATE_789\n"
                            "Authorization: Bearer OPAQUE_BEARER_123\n"
                            "Authorization: Basic c2VjcmV0\n"
                            "password: private words here"
                        ),
                        "messages": [
                            {
                                "role": "assistant",
                                "content": [
                                    {
                                        "type": "tool_use",
                                        "name": "Bash",
                                        "input": {
                                            "headers": {"Authorization": "OPAQUE_PRIVATE_123"}
                                        },
                                    },
                                    {
                                        "type": "tool_result",
                                        "content": "Authorization: OPAQUE_PRIVATE_456",
                                    },
                                ],
                            }
                        ],
                        "status": "done",
                    }
                ],
            )
        )
        session.add(
            ExecutionRequestPhaseReport(
                execution_request_id=seeded.request_id,
                phase="review",
                note="Five blocking findings remain",
                loop_round=1,
            )
        )
        await session.commit()

    with_session(seed)
    _finish(seeded.request_id, seeded.runtime_epoch, "failed", "no_pull_request")

    async def transcript_absent(session: AsyncSession) -> bool:
        item = await session.get(WorkItem, seeded.work_item_id)
        assert item is not None
        return (
            await session.scalar(
                select(ThreadTranscript.id).where(
                    ThreadTranscript.agent_id == item.agent_id,
                    ThreadTranscript.thread_key == item.conversation_id,
                )
            )
            is None
        )

    assert with_session(transcript_absent)
    response = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id)
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["execution_request_id"] == str(seeded.request_id)
    assert report["terminal_cause"] == "no_pull_request"
    assert "transcript" not in report["sources"]
    assert all(event["kind"] != "conversation" for event in report["events"])
    assert report["sources"]["traces"] == "missing"
    assert "SECRET_EXAMPLE_VALUE" not in json.dumps(report)
    assert "OPAQUE_PRIVATE_123" not in json.dumps(report)
    assert "OPAQUE_PRIVATE_456" not in json.dumps(report)
    assert "OPAQUE_PRIVATE_789" not in json.dumps(report)
    assert "OPAQUE_BEARER_123" not in json.dumps(report)
    assert "c2VjcmV0" not in json.dumps(report)
    assert "private words here" not in json.dumps(report)
    assert "words here" not in json.dumps(report)
    assert report["analysis"]["immediate_cause"] == "no_pull_request"
    assert "underlying reason is not asserted" in report["analysis"]["summary"]
    assert report["analysis"]["evidence"] == []
    assert _detail(stack, auth_headers, seeded.work_item_id)["state"] == "failed"
    assert any(
        event["kind"] == "phase" and event["phase"] == "review" for event in report["events"]
    )
    assert response.headers["cache-control"] == "no-store"

    async def stored_has_no_credential(session: AsyncSession) -> bool:
        row = await session.get(FactoryRunReport, seeded.request_id)
        assert row is not None
        stored = json.dumps(row.report)
        return all(
            secret not in stored
            for secret in ("OPAQUE_PRIVATE", "OPAQUE_BEARER", "c2VjcmV0", "words here")
        )

    assert with_session(stored_has_no_credential)


def test_report_absence_and_auth_are_explicit(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    _finish(seeded.request_id, seeded.runtime_epoch, "failed", "no_pull_request")

    report = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id)
    assert report.status_code == 200, report.text
    by_run_id = stack.get(f"/work-items/runs/{seeded.request_id}/report", headers=auth_headers)
    assert by_run_id.status_code == 200, by_run_id.text
    assert by_run_id.json() == report.json()
    assert report.json()["sources"]["traces"] in ("missing", "unavailable")
    assert report.json()["analysis"]["evidence"] == []

    refused = stack.get(f"/work-items/{seeded.work_item_id}/runs/{seeded.request_id}/report")
    assert refused.status_code == 401
    worker = _report(stack, WORKER_HEADERS, seeded.work_item_id, seeded.request_id)
    assert worker.status_code == 401
    worker_by_id = stack.get(f"/work-items/runs/{seeded.request_id}/report", headers=WORKER_HEADERS)
    assert worker_by_id.status_code == 401
    unknown = _report(stack, auth_headers, seeded.work_item_id, uuid.uuid4())
    assert unknown.status_code == 404


def test_repeated_settlement_preserves_the_first_report(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    _finish(seeded.request_id, seeded.runtime_epoch, "failed", "no_pull_request")
    first = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id).json()

    async def settle_again(session: AsyncSession) -> None:
        item = await session.get(WorkItem, seeded.work_item_id)
        request = await session.get(ExecutionRequest, seeded.request_id)
        assert item is not None and request is not None
        await settle_terminal(session, item, request, detail="later changed detail")
        await session.commit()

    with_session(settle_again)
    second = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id).json()
    assert second == first


def test_expired_report_is_unavailable_and_is_reaped(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    _finish(seeded.request_id, seeded.runtime_epoch, "failed", "no_pull_request")

    async def expire(session: AsyncSession) -> int:
        report = await session.get(FactoryRunReport, seeded.request_id)
        assert report is not None
        report.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
        deleted = await sweep_expired(session)
        await session.commit()
        return deleted

    assert with_session(expire) == 1
    response = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id)
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "report_unavailable"


def test_published_report_retains_publication_reference(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)

    response = _report(stack, auth_headers, seeded.work_item_id, seeded.request_id)
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["sources"]["publications"] == "complete"
    assert any(
        event["kind"] == "publication" and event["reference_url"] == PR_URL
        for event in report["events"]
    )
    assert report["sources"]["artifacts"] == "complete"

    async def publication_times(session: AsyncSession) -> tuple[str, str]:
        row = await session.get(Publication, uuid.UUID(publication["id"]))
        assert row is not None and row.terminal_at is not None
        return row.created_at.isoformat(), row.terminal_at.isoformat()

    created_at, terminal_at = with_session(publication_times)
    attempt = next(event for event in report["events"] if event["text"] == "Publication requested")
    result = next(event for event in report["events"] if event["reference_url"] == PR_URL)
    assert datetime.fromisoformat(attempt["at"]) == datetime.fromisoformat(created_at).replace(
        tzinfo=UTC
    )
    assert datetime.fromisoformat(result["at"]) == datetime.fromisoformat(terminal_at).replace(
        tzinfo=UTC
    )


def test_cancelled_and_legacy_requests_distinguish_capture_from_absence(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _cancel(seeded.work_item_id, facts.request_id)
    response = _report(stack, auth_headers, seeded.work_item_id, facts.request_id)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"

    async def remove_capture(session: AsyncSession) -> None:
        row = await session.get(FactoryRunReport, facts.request_id)
        assert row is not None
        await session.delete(row)
        await session.commit()

    with_session(remove_capture)
    absent = _report(stack, auth_headers, seeded.work_item_id, facts.request_id)
    assert absent.status_code == 404
    assert absent.json()["detail"]["code"] == "report_unavailable"


def test_expired_execution_retains_a_report_with_cancellation_evidence(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    facts, seeded = _deadline_90_then_elapse(stack, auth_headers)
    _terminate(facts.request_id)

    response = _report(stack, auth_headers, seeded.work_item_id, facts.request_id)
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["status"] == "expired"
    assert any(event["id"] == "cancellation_requested" for event in report["events"])
    assert _detail(stack, auth_headers, seeded.work_item_id)["state"] == "expired"
