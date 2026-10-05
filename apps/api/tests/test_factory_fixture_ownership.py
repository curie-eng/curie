"""Factory fixture ownership: apps/api/README.md#factory-test-isolation."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import REPO, GitHubAPI  # noqa: E402
from forge_fakes.github_comments import (  # noqa: E402
    _clear_ci_keys,
    admitted,  # noqa: F401
    comments,  # noqa: F401
)
from test_factory_terminus import _label, _request  # noqa: E402

pytestmark = pytest.mark.usefixtures("clean_db")


def test_independent_factory_fixtures_do_not_share_timeline_event_identity() -> None:
    """@spec apps/api/README.md#factory-test-isolation"""
    first, second = GitHubAPI(), GitHubAPI()
    request = httpx.Request("GET", f"https://api.github.com/repos/{REPO}/issues/9701/events")
    initial = first.handle(request).json()[0]["id"]
    assert first.handle(request).json()[0]["id"] == initial
    assert second.handle(request).json()[0]["id"] != initial
    first.advance_label_event(9701)
    assert first.handle(request).json()[0]["id"] > initial


def _keys(request_id: str) -> list[str]:
    return [
        f"curie:work-item:ci:{request_id}:2",
        f"curie:work-item:ci:{request_id}:2:enqueued",
        f"curie:work-item:ci-rerun:{request_id}:example-head",
        f"curie:work-item:ci-rerun:{request_id}:example-head:lock",
    ]


@pytest.fixture
def records(admitted: Any) -> Iterator[tuple[redis.Redis, list[str], list[str]]]:  # noqa: F811
    """@spec apps/api/README.md#factory-test-isolation"""
    client, github, _sink = admitted
    number = 98291
    _label(client, github, number)
    owned = _keys(str(_request(number)["id"]))
    foreign = _keys(str(uuid.uuid4()))
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        for key in owned + foreign:
            valkey.set(key, "fixture-record", ex=60)
        yield valkey, owned, foreign
    finally:
        # These exact keys were seeded by this fixture, including the simulated
        # sibling. Never sweep a namespace during assertion recovery.
        valkey.delete(*(owned + foreign))
        valkey.close()


def test_factory_cleanup_preserves_other_requests_ci_records(
    records: tuple[redis.Redis, list[str], list[str]],
) -> None:
    """@spec apps/api/README.md#factory-test-isolation"""
    valkey, _owned, foreign = records
    _clear_ci_keys()
    assert all(valkey.get(key) == b"fixture-record" for key in foreign)


def test_factory_cleanup_removes_its_requests_rerun_decisions_and_locks(
    records: tuple[redis.Redis, list[str], list[str]],
) -> None:
    """@spec apps/api/README.md#factory-test-isolation"""
    valkey, owned, _foreign = records
    _clear_ci_keys()
    assert all(not valkey.exists(key) for key in owned)
