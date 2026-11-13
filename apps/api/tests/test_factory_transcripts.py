"""A factory transcript remains inspectable after its runner has gone (#5608)."""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.factory_transcripts import expire_events
from curie_api.models import ExecutionRequest, FactoryTranscriptEvent
from curie_internal import sandbox_token
from forge_fakes.github_comments import admitted, comments  # noqa: F401 - fixtures
from sqlalchemy import update
from test_factory_terminus import _finish_unpublished, _label, _request, _start_running

pytestmark = pytest.mark.usefixtures("clean_db")


def _write_token(request_id: uuid.UUID) -> str:
    return sandbox_token.mint(
        get_settings().api_key,
        agent=str(request_id),
        scope="work_item.transcript.write",
        exp=int(time.time()) + 3600,
    )


def _event(boot_id: uuid.UUID, sequence: int, *, text: str = "hello") -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "boot_id": str(boot_id),
        "sequence": sequence,
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "message",
        "content": {"role": "assistant", "blocks": [{"type": "text", "text": text}]},
    }


def test_transcript_write_is_request_bound_ordered_and_idempotent(admitted: Any) -> None:  # noqa: F811
    client, github, _comments = admitted
    _label(client, github, 5608)
    request_id = _request(5608)["id"]
    _start_running(client, request_id)
    boot_id = uuid.uuid4()
    first = _event(boot_id, 1, text="first")
    second = _event(boot_id, 2, text="second")
    path = f"/v1/work-item-transcripts/{request_id}/events"
    headers = {"X-API-Key": _write_token(request_id)}

    for event in (first, first, second):
        response = client.post(path, headers=headers, json=event)
        assert response.status_code == 201, response.text

    conflicting = {**first, "content": {"role": "assistant", "content": "changed"}}
    assert client.post(path, headers=headers, json=conflicting).status_code == 409
    reused_sequence = _event(boot_id, 2, text="different event")
    assert client.post(path, headers=headers, json=reused_sequence).status_code == 409

    read = client.get(
        f"/work-items/runs/{request_id}/transcript?limit=1",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert read.status_code == 200, read.text
    page = read.json()
    assert page["status"] == "incomplete"
    assert [row["content"]["blocks"][0]["text"] for row in page["events"]] == ["first"]
    next_page = client.get(
        f"/work-items/runs/{request_id}/transcript?limit=10&after={page['next_cursor']}",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert next_page.status_code == 200
    assert [row["content"]["blocks"][0]["text"] for row in next_page.json()["events"]] == [
        "second"
    ]
    assert client.post(
        path, headers={"X-API-Key": _write_token(uuid.uuid4())}, json=_event(boot_id, 3)
    ).status_code == 401
    assert client.post(
        path, headers={"X-API-Key": get_settings().api_key}, json=_event(boot_id, 3)
    ).status_code == 401
    assert client.get(f"/work-items/runs/{request_id}/transcript").status_code == 401
    assert client.get(
        f"/work-items/runs/{request_id}/transcript?agent_id={uuid.uuid4()}",
        headers={"X-API-Key": get_settings().api_key},
    ).status_code == 404

    secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
    third = _event(boot_id, 3, text=secret)
    assert client.post(path, headers=headers, json=third).status_code == 201
    finished = _event(boot_id, 4)
    finished["kind"] = "capture_complete"
    finished["content"] = {"status": "done"}
    assert client.post(path, headers=headers, json=finished).status_code == 201
    settled = _finish_unpublished(client, request_id, cause="no_pull_request", detail="done")
    assert settled.status_code == 200, settled.text
    retained = client.get(
        f"/work-items/runs/{request_id}/transcript",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert retained.status_code == 200, retained.text
    assert retained.json()["status"] == "complete"
    assert retained.json()["redacted"] is True
    assert secret not in retained.text

    async def expire_archive() -> int:
        async with client.app.state.sessionmaker() as session:
            old = datetime.now(UTC) - timedelta(days=31)
            await session.execute(
                update(FactoryTranscriptEvent)
                .where(FactoryTranscriptEvent.execution_request_id == request_id)
                .values(expires_at=old + timedelta(days=30))
            )
            await session.commit()
            return await expire_events(session)

    assert client.portal.call(expire_archive) == 4
    swept = client.get(
        f"/work-items/runs/{request_id}/transcript",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert swept.status_code == 200
    assert swept.json()["status"] == "unavailable"
    assert swept.json()["events"] == []

    async def expire_request() -> None:
        async with client.app.state.sessionmaker() as session:
            await session.execute(
                update(ExecutionRequest)
                .where(ExecutionRequest.id == request_id)
                .values(transcript_expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            await session.commit()

    client.portal.call(expire_request)
    assert client.post(path, headers=headers, json=_event(boot_id, 5)).status_code == 410
    expired = client.get(
        f"/work-items/runs/{request_id}/transcript",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert expired.json()["status"] == "expired"
    assert expired.json()["events"] == []


def test_retention_is_fixed_at_first_event(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    client, github, _comments = admitted
    _label(client, github, 5609)
    request_id = _request(5609)["id"]
    _start_running(client, request_id)
    settings = get_settings()
    monkeypatch.setattr(settings, "factory_transcript_retention_days", 7)
    boot_id = uuid.uuid4()
    path = f"/v1/work-item-transcripts/{request_id}/events"
    headers = {"X-API-Key": _write_token(request_id)}
    assert client.post(path, headers=headers, json=_event(boot_id, 1)).status_code == 201
    first = client.get(
        f"/work-items/runs/{request_id}/transcript",
        headers={"X-API-Key": settings.api_key},
    ).json()["expires_at"]
    assert timedelta(days=6) < datetime.fromisoformat(first) - datetime.now(UTC) < timedelta(days=8)

    monkeypatch.setattr(settings, "factory_transcript_retention_days", 14)
    assert client.post(path, headers=headers, json=_event(boot_id, 2)).status_code == 201
    second = client.get(
        f"/work-items/runs/{request_id}/transcript",
        headers={"X-API-Key": settings.api_key},
    ).json()["expires_at"]
    assert second == first
