"""Bounded, authenticated reply storage for disconnected cluster messages.

The worker writes channel-protocol reply events through a private credential;
the CLI reads only the UUID bucket it minted through the platform credential.
These tests use the real compose Valkey.  They intentionally never replace it
with an in-memory fake: ordering, expiry, atomic dedupe and concurrent bucket
isolation are the behavior under test.
"""

import json
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import quote

import pytest
import redis
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient

INTERNAL_ROUTE = "/v1/internal/cluster-message-replies"
PUBLIC_ROUTE = "/cluster-message-replies"
RELAY_KEY_GLOB = "curie:cluster-message-replies:*"


def _worker_headers(secret: str | None = None) -> dict[str, str]:
    if secret is None:
        secret = get_settings().internal_worker_token
    return {"X-Curie-Adapter-Secret": secret}


def _platform_headers(key: str | None = None) -> dict[str, str]:
    if key is None:
        key = get_settings().api_key
    return {"X-API-Key": key}


def _new_ref() -> str:
    return str(uuid.uuid4())


def _target(reply_ref: str) -> dict[str, Any]:
    return {
        "kind": "slack",
        "address": "C0EXAMPLE1",
        "conversation_id": "thread-example",
        "reply_ref": reply_ref,
    }


def _status(reply_ref: str, status: str = "Working") -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "turn.status",
        "target": _target(reply_ref),
        "status": status,
    }


def _update(reply_ref: str, text: str = "the answer") -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "reply.update",
        "target": _target(reply_ref),
        "text": text,
    }


def _post(reply_ref: str) -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "reply.post",
        "target": _target(reply_ref),
        "message": {"version": "1.0", "text": "Approve this command?"},
        "requested_by": "U0EXAMPLE1",
    }


def _completed(
    reply_ref: str,
    *,
    event_id: str = "EvSIM-example",
    outcome: str = "delivered",
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "turn.completed",
        "target": _target(reply_ref),
        "event_id": event_id,
        "outcome": outcome,
    }


def _write(client: TestClient, reply_ref: str, event: dict[str, Any]) -> Any:
    return client.post(
        f"{INTERNAL_ROUTE}/{reply_ref}",
        json=event,
        headers=_worker_headers(),
    )


def _read(client: TestClient, reply_ref: str, *, after: int = 0) -> Any:
    return client.get(
        f"{PUBLIC_ROUTE}/{reply_ref}",
        params={"after": after},
        headers=_platform_headers(),
    )


@pytest.fixture
def relay_client(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    valkey_client: redis.Redis,
) -> Iterator[TestClient]:
    """Build after bounded-store overrides, as production reads them at boot."""

    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_TTL_S", "60")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_EVENTS", "100")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_BYTES", str(64 * 1024))
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            yield client
    finally:
        get_settings.cache_clear()


@pytest.fixture
def valkey_client() -> Iterator[redis.Redis]:
    """Real Valkey plus narrow cleanup of keys this test created."""

    client = connect_or_skip(decode_responses=True)
    before = set(client.scan_iter(RELAY_KEY_GLOB))
    try:
        yield client
    finally:
        created = set(client.scan_iter(RELAY_KEY_GLOB)) - before
        if created:
            client.delete(*created)
        client.close()


def test_worker_post_and_platform_get_use_distinct_credentials_and_ack_the_ref(
    relay_client: TestClient,
) -> None:
    reply_ref = _new_ref()
    event = _post(reply_ref)

    written = _write(relay_client, reply_ref, event)
    assert written.status_code == 200, written.text
    assert written.json() == {"ref": reply_ref}

    page = _read(relay_client, reply_ref)
    assert page.status_code == 200, page.text
    assert page.json() == {
        "events": [event],
        "next_cursor": 1,
        "terminal": False,
    }


@pytest.mark.parametrize(
    "credential_case",
    [
        "missing",
        "wrong-worker",
        "platform-only",
        "platform-and-wrong-worker",
    ],
)
def test_worker_post_refuses_missing_wrong_or_platform_only_credentials_without_writing(
    credential_case: str,
    relay_client: TestClient,
    valkey_client: redis.Redis,
) -> None:
    reply_ref = _new_ref()
    before = set(valkey_client.scan_iter(RELAY_KEY_GLOB))
    headers = {
        "missing": {},
        "wrong-worker": _worker_headers("wrong-worker-secret"),
        "platform-only": _platform_headers(),
        "platform-and-wrong-worker": {
            **_platform_headers(),
            **_worker_headers("wrong-worker-secret"),
        },
    }[credential_case]

    refused = relay_client.post(
        f"{INTERNAL_ROUTE}/{reply_ref}", json=_update(reply_ref), headers=headers
    )

    assert refused.status_code == 401, refused.text
    assert set(valkey_client.scan_iter(RELAY_KEY_GLOB)) == before


@pytest.mark.parametrize(
    "credential_case",
    [
        "missing",
        "wrong-platform",
        "worker-only",
    ],
)
def test_platform_get_refuses_missing_wrong_or_worker_only_credentials(
    credential_case: str, relay_client: TestClient
) -> None:
    reply_ref = _new_ref()
    headers = {
        "missing": {},
        "wrong-platform": _platform_headers("wrong-platform-key"),
        "worker-only": _worker_headers(),
    }[credential_case]
    refused = relay_client.get(f"{PUBLIC_ROUTE}/{reply_ref}", headers=headers)
    assert refused.status_code == 401, refused.text


def test_no_route_enumerates_cluster_message_reply_buckets(
    relay_client: TestClient,
) -> None:
    public_list = relay_client.get(PUBLIC_ROUTE, headers=_platform_headers())
    internal_list = relay_client.get(INTERNAL_ROUTE, headers=_worker_headers())

    assert public_list.status_code == 404, public_list.text
    assert internal_list.status_code == 404, internal_list.text


def test_platform_get_reports_an_unwritten_valid_ref_as_absent(
    relay_client: TestClient,
) -> None:
    absent = _read(relay_client, _new_ref())
    assert absent.status_code == 404, absent.text


@pytest.mark.parametrize(
    "reply_ref",
    [
        "http://evil.example",
        "../",
        "a/b",
        "550E8400-E29B-41D4-A716-446655440000",
        # A version 1 UUID: well formed, and not the canonical form this route
        # accepts, which is the whole point of the case.
        #
        # It was `str(uuid.uuid1())`, evaluated at COLLECTION time (#2235). That
        # gave the case a different node id on every run, so no `Fix pin:`
        # selector could ever name it, and under xdist each worker collects at
        # its own moment and pytest refuses the mismatch outright: "Different
        # tests were collected between gw0 and gw3". A literal proves exactly
        # the same thing about the route and stays nameable.
        "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    ],
)
def test_noncanonical_refs_are_refused_before_any_valkey_write(
    reply_ref: str,
    relay_client: TestClient,
    valkey_client: redis.Redis,
) -> None:
    before = set(valkey_client.scan_iter(RELAY_KEY_GLOB))
    encoded = quote(reply_ref, safe="")

    refused = relay_client.post(
        f"{INTERNAL_ROUTE}/{encoded}",
        json=_update(reply_ref),
        headers=_worker_headers(),
    )

    assert refused.status_code in (404, 422), refused.text
    assert set(valkey_client.scan_iter(RELAY_KEY_GLOB)) == before


def test_body_ref_must_equal_the_validated_path_ref(
    relay_client: TestClient, valkey_client: redis.Redis
) -> None:
    path_ref = _new_ref()
    other_ref = _new_ref()
    before = set(valkey_client.scan_iter(RELAY_KEY_GLOB))

    refused = _write(relay_client, path_ref, _update(other_ref))

    assert refused.status_code == 409, refused.text
    assert set(valkey_client.scan_iter(RELAY_KEY_GLOB)) == before


def test_cursor_reads_preserve_order_and_approval_resume_uses_the_same_ref(
    relay_client: TestClient,
) -> None:
    reply_ref = _new_ref()
    initial = [
        _status(reply_ref),
        _update(reply_ref, "I need approval"),
        _completed(reply_ref, event_id="EvSIM-first", outcome="awaiting-approval"),
    ]
    for event in initial:
        response = _write(relay_client, reply_ref, event)
        assert response.status_code == 200, response.text
        assert response.json() == {"ref": reply_ref}

    first_page = _read(relay_client, reply_ref)
    assert first_page.status_code == 200, first_page.text
    assert first_page.json() == {
        "events": initial,
        "next_cursor": 3,
        # Awaiting approval is a pause, not the CLI poller's terminal result.
        "terminal": False,
    }

    resumed = [
        _update(reply_ref, "Approved and complete"),
        _completed(reply_ref, event_id="approval-example-resolved"),
    ]
    for event in resumed:
        response = _write(relay_client, reply_ref, event)
        assert response.status_code == 200, response.text

    second_page = _read(relay_client, reply_ref, after=3)
    assert second_page.status_code == 200, second_page.text
    assert second_page.json() == {
        "events": resumed,
        "next_cursor": 5,
        "terminal": True,
    }


@pytest.mark.parametrize("event_kind", ["update", "completed"])
def test_an_exact_worker_retry_is_deduplicated(
    event_kind: str, relay_client: TestClient
) -> None:
    reply_ref = _new_ref()
    event = (
        _update(reply_ref, "one durable update")
        if event_kind == "update"
        else _completed(reply_ref, event_id="EvSIM-retried")
    )

    first = _write(relay_client, reply_ref, event)
    retry = _write(relay_client, reply_ref, event)
    assert first.status_code == retry.status_code == 200
    assert first.json() == retry.json() == {"ref": reply_ref}

    page = _read(relay_client, reply_ref)
    assert page.status_code == 200, page.text
    assert page.json()["events"] == [event]
    assert page.json()["next_cursor"] == 1


def test_concurrent_refs_cannot_cross_read_or_advance_each_others_cursor(
    relay_client: TestClient,
) -> None:
    left_ref = _new_ref()
    right_ref = _new_ref()
    expected = {
        left_ref: [
            _update(left_ref, "left-only"),
            _completed(left_ref, event_id="EvSIM-left"),
        ],
        right_ref: [
            _update(right_ref, "right-only"),
            _completed(right_ref, event_id="EvSIM-right"),
        ],
    }

    def send_ref(item: tuple[str, list[dict[str, Any]]]) -> list[int]:
        reply_ref, events = item
        return [_write(relay_client, reply_ref, event).status_code for event in events]

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(send_ref, expected.items())) == [[200, 200], [200, 200]]

    left = _read(relay_client, left_ref).json()
    right = _read(relay_client, right_ref).json()
    assert left == {"events": expected[left_ref], "next_cursor": 2, "terminal": True}
    assert right == {
        "events": expected[right_ref],
        "next_cursor": 2,
        "terminal": True,
    }
    assert left_ref not in json.dumps(right)
    assert right_ref not in json.dumps(left)


def test_event_count_bound_refuses_growth_without_discarding_prior_results(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    valkey_client: redis.Redis,
) -> None:
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_TTL_S", "60")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_EVENTS", "2")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_BYTES", str(64 * 1024))
    get_settings.cache_clear()
    reply_ref = _new_ref()
    try:
        with TestClient(create_app()) as client:
            first = _status(reply_ref, "one")
            second = _update(reply_ref, "two")
            assert _write(client, reply_ref, first).status_code == 200
            assert _write(client, reply_ref, second).status_code == 200

            retry = _write(client, reply_ref, second)
            assert retry.status_code == 200, retry.text

            overflow = _write(client, reply_ref, _update(reply_ref, "three"))
            assert overflow.status_code == 409, overflow.text

            page = _read(client, reply_ref)
            assert page.status_code == 200, page.text
            assert page.json()["events"] == [first, second]
            assert page.json()["next_cursor"] == 2
    finally:
        get_settings.cache_clear()


def test_aggregate_byte_bound_refuses_the_crossing_event_without_partial_write(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    valkey_client: redis.Redis,
) -> None:
    reply_ref = _new_ref()
    first = _update(reply_ref, "a" * 120)
    second = _update(reply_ref, "b" * 120)
    first_bytes = len(json.dumps(first).encode())
    second_bytes = len(json.dumps(second).encode())
    limit = max(first_bytes, second_bytes) + 32
    assert first_bytes < limit and second_bytes < limit
    assert first_bytes + second_bytes > limit

    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_TTL_S", "60")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_EVENTS", "100")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_BYTES", str(limit))
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            assert _write(client, reply_ref, first).status_code == 200
            overflow = _write(client, reply_ref, second)
            assert overflow.status_code == 413, overflow.text

            page = _read(client, reply_ref)
            assert page.status_code == 200, page.text
            assert page.json()["events"] == [first]
            assert page.json()["next_cursor"] == 1
    finally:
        get_settings.cache_clear()


def test_reply_bucket_expires_in_real_valkey(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    valkey_client: redis.Redis,
) -> None:
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_TTL_S", "1")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_EVENTS", "100")
    monkeypatch.setenv("CLUSTER_MESSAGE_REPLIES_MAX_BYTES", str(64 * 1024))
    get_settings.cache_clear()
    reply_ref = _new_ref()
    before = set(valkey_client.scan_iter(RELAY_KEY_GLOB))
    try:
        with TestClient(create_app()) as client:
            assert _write(client, reply_ref, _update(reply_ref)).status_code == 200
            assert _read(client, reply_ref).json()["events"]

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                if not (set(valkey_client.scan_iter(RELAY_KEY_GLOB)) - before):
                    break
                time.sleep(0.05)

            assert set(valkey_client.scan_iter(RELAY_KEY_GLOB)) == before
            expired = _read(client, reply_ref)
            assert expired.status_code == 404, expired.text
    finally:
        get_settings.cache_clear()
