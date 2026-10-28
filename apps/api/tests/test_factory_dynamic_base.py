"""Current tracker selection is pinned once at the execution acquisition boundary."""

# ruff: noqa: F811

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import Settings, get_settings
from forge_fakes.github import LABEL, REPO, _issue_event, _post, _signature
from test_github_factory_base import (  # noqa: F401
    MAIN_SHA,
    NEXT_SHA,
    WORKER,
    _acquire,
    _acquire_body,
    _execute,
    _execution,
    _label,
    _work_item,
    base_app,
)
from test_github_factory_ingress import _rows

pytestmark = pytest.mark.usefixtures("clean_db")


def _due(number: int) -> None:
    _execute(
        "UPDATE curie.execution_requests SET base_settle_until = clock_timestamp(), "
        "dispatch_not_before = clock_timestamp(), published_generation = dispatch_generation "
        "WHERE id = :id",
        {"id": _execution(number)["id"]},
    )


def _start(client: Any, number: int, grant: Any) -> Any:
    row = _execution(number)
    return client.post(
        f"/v1/internal/work-items/requests/{row['id']}/start",
        headers=WORKER,
        json={
            "owner": "factory-base-owner",
            "generation": grant.json()["generation"],
            "claim_name": "claim-factory-base",
            "sandbox_name": "sandbox-factory-base",
            "context": grant.json()["context"],
        },
    )


def test_admission_does_not_read_or_resolve_a_branch_head(base_app: Any) -> None:
    client, api = base_app

    response = _label(client, api, 32001, "base:next")

    assert response.json()["status"] == "factory_admitted", response.text
    assert api.branch_requests == []
    item = _work_item(32001)
    assert item is not None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (None, None, None)
    assert _execution(32001)["acquire_owner"] is None


def test_default_settle_interval_is_sixty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FACTORY_BASE_LABEL_SETTLE_S", raising=False)

    assert Settings(_env_file=None).factory_base_label_settle_s == 60


@pytest.mark.parametrize("seconds", [0, 2, 60])
def test_settle_uses_database_time_and_cannot_grant_early(
    base_app: Any, monkeypatch: pytest.MonkeyPatch, seconds: int
) -> None:
    client, api = base_app
    monkeypatch.setenv("FACTORY_BASE_LABEL_SETTLE_S", str(seconds))
    get_settings.cache_clear()
    before = _rows("SELECT clock_timestamp() AS now")[0]["now"]

    assert _label(client, api, 32002).json()["status"] == "factory_admitted"

    after = _rows("SELECT clock_timestamp() AS now")[0]["now"]
    row = _execution(32002)
    assert seconds <= (row["base_settle_until"] - before).total_seconds()
    assert (row["base_settle_until"] - after).total_seconds() <= seconds
    if seconds:
        early = _acquire(client, 32002)
        assert early.status_code == 409, early.text
        assert _execution(32002)["acquire_owner"] is None
        assert _work_item(32002)["base_commit"] is None
        assert api.branch_requests == []
        _due(32002)

    ready = _acquire(client, 32002)

    assert ready.status_code == 200, ready.text
    assert _work_item(32002)["base_commit"] == MAIN_SHA


def test_delayed_base_label_is_read_when_settle_finishes(
    base_app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = base_app
    monkeypatch.setenv("FACTORY_BASE_LABEL_SETTLE_S", "60")
    get_settings.cache_clear()
    assert _label(client, api, 32003).json()["status"] == "factory_admitted"
    api.labels = [LABEL, "base:next"]
    api.branches["next"] = "4" * 40
    _due(32003)

    claimed = _acquire(client, 32003)

    assert claimed.status_code == 200, claimed.text
    item = _work_item(32003)
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "next",
        "label",
        "4" * 40,
    )
    assert api.branch_requests == ["next"]
    (pin,) = _rows(
        "SELECT base_resolved_request_id FROM curie.work_items WHERE id = :id",
        {"id": item["id"]},
    )
    assert pin["base_resolved_request_id"] == _execution(32003)["id"]


def test_duplicate_delivery_does_not_restart_the_settle_interval(
    base_app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = base_app
    monkeypatch.setenv("FACTORY_BASE_LABEL_SETTLE_S", "60")
    get_settings.cache_clear()
    api.issue_number = 32004
    api.labels = [LABEL]
    api.advance_label_event(32004)
    event = _issue_event("labeled", 32004, label={"name": LABEL})
    delivery = str(uuid.uuid4())
    assert _post(client, "issues", event, delivery=delivery).status_code == 200
    before = _execution(32004)

    assert _post(client, "issues", event, delivery=delivery).status_code == 200

    after = _execution(32004)
    assert (after["id"], after["base_settle_until"]) == (before["id"], before["base_settle_until"])
    assert api.branch_requests == []
    _due(32004)
    assert _acquire(client, 32004).status_code == 200


def test_held_acquisition_renewal_never_repins_the_request(base_app: Any) -> None:
    client, api = base_app
    assert _label(client, api, 32005, "base:next").json()["status"] == "factory_admitted"
    first = _acquire(client, 32005)
    assert first.status_code == 200, first.text
    before = _work_item(32005)
    api.branches["next"] = "5" * 40
    api.labels = [LABEL, "base:main"]
    api.branch_requests.clear()

    renewed = _acquire(client, 32005)

    assert renewed.status_code == 200, renewed.text
    after = _work_item(32005)
    assert (after["base_branch"], after["base_commit"]) == (before["base_branch"], NEXT_SHA)
    assert api.branch_requests == []
    assert _start(client, 32005, renewed).status_code == 200


def test_capacity_redelivery_preserves_the_request_pin(base_app: Any) -> None:
    client, api = base_app
    assert _label(client, api, 32006, "base:next").json()["status"] == "factory_admitted"
    first = _acquire(client, 32006)
    assert first.status_code == 200, first.text
    row = _execution(32006)
    deferred = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/defer",
        headers=WORKER,
        json={
            "owner": "factory-base-owner",
            "generation": first.json()["generation"],
            "reason": "capacity",
            "capacity": True,
        },
    )
    assert deferred.status_code == 200, deferred.text
    api.branches["next"] = "6" * 40
    api.branch_requests.clear()
    _due(32006)

    redelivered = _acquire(client, 32006)

    assert redelivered.status_code == 200, redelivered.text
    assert _execution(32006)["id"] == row["id"]
    assert _work_item(32006)["base_commit"] == NEXT_SHA
    assert api.branch_requests == []


def test_superseding_a_blocked_external_read_cannot_pin_or_start_the_old_request(
    base_app: Any,
) -> None:
    client, api = base_app
    assert _label(client, api, 32007, "base:next").json()["status"] == "factory_admitted"
    old = _execution(32007)
    payload = _acquire_body(32007)

    async def race() -> None:
        entered, release = asyncio.Event(), asyncio.Event()

        async def forge(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/repos/{REPO}/git/ref/heads/next":
                entered.set()
                await release.wait()
            return api.handle(request)

        original = client.app.state.http_client
        async with httpx.AsyncClient(transport=httpx.MockTransport(forge)) as external:
            client.app.state.http_client = external
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
                ) as http:
                    claiming = asyncio.create_task(
                        http.post(
                            f"/v1/internal/work-items/requests/{old['id']}/acquire",
                            headers=WORKER,
                            json=payload,
                        )
                    )
                    try:
                        await asyncio.wait_for(entered.wait(), 5)
                        api.labels = [LABEL, "base:main"]
                        api.advance_label_event(32007)
                        body = json.dumps(
                            _issue_event("labeled", 32007, label={"name": LABEL})
                        ).encode()
                        changed = await asyncio.wait_for(
                            http.post(
                                "/github/webhook",
                                content=body,
                                headers={
                                    "X-GitHub-Delivery": str(uuid.uuid4()),
                                    "X-GitHub-Event": "issues",
                                    "X-Hub-Signature-256": _signature(body),
                                    "Content-Type": "application/json",
                                },
                            ),
                            5,
                        )
                        assert changed.status_code == 200, changed.text
                    finally:
                        release.set()
                    stale = await asyncio.wait_for(claiming, 5)
                    assert stale.status_code == 409, stale.text
            finally:
                client.app.state.http_client = original

    client.portal.call(race)

    current = _execution(32007)
    assert current["id"] != old["id"]
    assert _work_item(32007)["base_commit"] is None
    assert _acquire(client, 32007).status_code == 200
    assert _work_item(32007)["base_commit"] == MAIN_SHA
    (stale,) = _rows(
        "SELECT status, acquire_owner FROM curie.execution_requests WHERE id = :id",
        {"id": old["id"]},
    )
    assert stale == {"status": "cancelled", "acquire_owner": None}


@pytest.mark.parametrize("label", ["base:next", "base:release-1", "base:main"])
def test_a_base_only_label_observation_fences_a_blocked_first_acquisition(
    base_app: Any, label: str
) -> None:
    client, api = base_app
    number = 32008
    assert _label(client, api, number, "base:main").json()["status"] == "factory_admitted"
    original = _execution(number)
    payload = _acquire_body(number)

    async def race() -> httpx.Response:
        entered, release = asyncio.Event(), asyncio.Event()

        async def forge(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/repos/{REPO}/git/ref/heads/main":
                entered.set()
                await release.wait()
            return api.handle(request)

        previous = client.app.state.http_client
        async with httpx.AsyncClient(transport=httpx.MockTransport(forge)) as external:
            client.app.state.http_client = external
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
                ) as http:
                    claiming = asyncio.create_task(
                        http.post(
                            f"/v1/internal/work-items/requests/{original['id']}/acquire",
                            headers=WORKER,
                            json=payload,
                        )
                    )
                    try:
                        await asyncio.wait_for(entered.wait(), 5)
                        api.labels = [LABEL, label]
                        body = json.dumps(
                            _issue_event("labeled", number, label={"name": label})
                        ).encode()
                        observed = await asyncio.wait_for(
                            http.post(
                                "/github/webhook",
                                content=body,
                                headers={
                                    "X-GitHub-Delivery": str(uuid.uuid4()),
                                    "X-GitHub-Event": "issues",
                                    "X-Hub-Signature-256": _signature(body),
                                    "Content-Type": "application/json",
                                },
                            ),
                            5,
                        )
                        assert observed.status_code == 200, observed.text
                    finally:
                        release.set()
                    return await asyncio.wait_for(claiming, 5)
            finally:
                client.app.state.http_client = previous

    first = client.portal.call(race)

    current = _execution(number)
    assert current["id"] == original["id"]
    assert current["status"] == "waiting"
    if label != "base:main":
        assert first.status_code == 409, first.text
    else:
        assert first.status_code in {200, 409}, first.text
    if first.status_code == 409:
        assert current["acquire_owner"] is None
        item = _work_item(number)
        assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
            None,
            None,
            None,
        )
    _due(number)

    latest = _acquire(client, number)

    if label == "base:release-1":
        assert latest.status_code == 409, latest.text
        assert _execution(number)["id"] == original["id"]
        assert _execution(number)["acquire_owner"] is None
        assert _work_item(number)["base_commit"] is None
        api.labels = [LABEL, "base:next"]
        _due(number)
        latest = _acquire(client, number)
    assert latest.status_code == 200, latest.text
    assert _execution(number)["id"] == original["id"]
    expected = ("main", MAIN_SHA) if label == "base:main" else ("next", NEXT_SHA)
    item = _work_item(number)
    assert (item["base_branch"], item["base_commit"]) == expected
