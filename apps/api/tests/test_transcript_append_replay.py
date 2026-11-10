"""#5366: a transcript append replays a record already stored under its ``record_id``.

The runner retries a turn's transcript append when the first attempt's outcome
is unknown (a 5xx, a connection cut after the request was sent). That attempt
may have committed, so the retry must not store the turn twice. Each runner turn
record carries a ``record_id``; when an item with the same nonempty
``record_id`` is already stored, the append returns the stored entry unchanged
with 200 and no version bump, checked before the per-thread cap. Items without a
``record_id`` (the worker's publication outcome record, the factory base
reconcile) append every time, as before.

Real router, real Postgres (the disposable-DB conftest), nothing mocked.
"""

from __future__ import annotations

import uuid
from typing import Any

from curie_api.config import get_settings
from curie_api.transcripts import json_size


def _agent(client: Any, headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={
            "name": f"transcript-replay-{uuid.uuid4().hex[:6]}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE6"},
        },
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _turn(record_id: str | None, user: str, *, pad: int = 0) -> dict[str, Any]:
    item: dict[str, Any] = {
        "type": "turn",
        "user": user,
        "assistant": "done" + "x" * pad,
        "ts": "2026-11-04T00:00:00+00:00",
        "status": "done",
    }
    if record_id is not None:
        item["record_id"] = record_id
    return item


def test_the_same_record_id_appended_twice_is_stored_once(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-replay"
    item = _turn("r-1", "first question")

    first = client.post(f"{url}/append", json={"item": item}, headers=auth_headers)
    assert first.status_code == 200, first.text
    replay = client.post(f"{url}/append", json={"item": item}, headers=auth_headers)
    assert replay.status_code == 200, replay.text

    assert replay.json()["value"] == [item]
    assert replay.json()["version"] == first.json()["version"]
    stored = client.get(url, headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["value"] == [item]
    assert stored.json()["version"] == first.json()["version"]


def test_a_replay_after_later_records_returns_the_log_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-replay-later"
    first = _turn("r-1", "first question")
    second = _turn("r-2", "second question")

    for item in (first, second):
        appended = client.post(f"{url}/append", json={"item": item}, headers=auth_headers)
        assert appended.status_code == 200, appended.text
    version = appended.json()["version"]

    replay = client.post(f"{url}/append", json={"item": first}, headers=auth_headers)
    assert replay.status_code == 200, replay.text
    assert replay.json()["value"] == [first, second]
    assert replay.json()["version"] == version


def test_different_record_ids_both_append_in_order(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-two-ids"
    first = _turn("r-1", "first question")
    second = _turn("r-2", "second question")

    one = client.post(f"{url}/append", json={"item": first}, headers=auth_headers)
    assert one.status_code == 200, one.text
    two = client.post(f"{url}/append", json={"item": second}, headers=auth_headers)
    assert two.status_code == 200, two.text

    assert two.json()["value"] == [first, second]
    assert two.json()["version"] == one.json()["version"] + 1
    assert client.get(url, headers=auth_headers).json()["value"] == [first, second]


def test_items_without_a_record_id_append_every_time(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The worker's publication outcome append carries no ``record_id``."""
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-no-ids"
    outcome = {
        "user": "Platform publication outcome",
        "assistant": "Published pull request #41.",
        "ts": "2026-11-04T00:00:00+00:00",
    }
    # An empty id is no id: it never matches a stored item.
    blank = _turn("", "blank id")

    versions = []
    for item in (outcome, outcome, blank, blank):
        appended = client.post(f"{url}/append", json={"item": item}, headers=auth_headers)
        assert appended.status_code == 200, appended.text
        versions.append(appended.json()["version"])

    assert client.get(url, headers=auth_headers).json()["value"] == [
        outcome,
        outcome,
        blank,
        blank,
    ]
    assert versions == sorted(set(versions))
    assert len(set(versions)) == 4


def test_a_replay_on_a_full_transcript_is_200_while_a_new_record_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The replay check runs before the cap: a stored record is not a new byte."""
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-full"
    stored_item = _turn("r-1", "first question", pad=300)
    new_item = _turn("r-2", "second question", pad=300)
    settings = get_settings()
    # Room for the stored record plus 100 bytes: too little for either record
    # a second time.
    settings.transcript_max_thread_bytes = json_size([stored_item]) + 100
    try:
        initial = client.post(f"{url}/append", json={"item": stored_item}, headers=auth_headers)
        assert initial.status_code == 200, initial.text

        refused = client.post(
            f"{url}/append",
            json={"item": new_item, "reserve_bytes": 50},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text

        replay = client.post(
            f"{url}/append",
            json={"item": stored_item, "reserve_bytes": 50},
            headers=auth_headers,
        )
        assert replay.status_code == 200, replay.text
        assert replay.json()["value"] == [stored_item]
        assert replay.json()["version"] == initial.json()["version"]

        stored = client.get(url, headers=auth_headers).json()
        assert stored["value"] == [stored_item]
        assert stored["version"] == initial.json()["version"]
    finally:
        get_settings.cache_clear()
