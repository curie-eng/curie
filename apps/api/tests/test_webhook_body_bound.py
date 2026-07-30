"""The GitHub webhook bounds its request body before authentication (#633).

A directly exposed webhook must not let an unauthenticated oversized request
buffer an unbounded body in memory. These drive the real endpoint (mounted on a
minimal app with the infra deps stubbed, so no DB is needed) and assert the
bound fires ahead of auth, holds for a lying/absent Content-Length and a
streamed body, and does not disturb the valid/invalid signature outcomes.
"""

import hashlib
import hmac
from collections.abc import Iterator
from typing import Any

import pytest
from curie_api.config import Settings
from curie_api.deps import get_eval_queue, get_session, get_store
from curie_api.routers import github as github_router
from curie_api.schemas import WebhookResult
from fastapi import FastAPI
from fastapi.testclient import TestClient

SECRET = "webhook-test-secret"
MAX = 512  # a small bound so tests need not send megabytes


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setattr(
        github_router,
        "get_settings",
        lambda: Settings(github_webhook_secret=SECRET, github_webhook_max_body_bytes=MAX),
    )

    async def _fake_push(*_args: Any, **_kwargs: Any) -> WebhookResult:
        # A signed push that clears the size gate reaches process_push; stub it
        # so the endpoint test needs no Postgres/clone.
        return WebhookResult(status="deployed")

    monkeypatch.setattr(github_router, "process_push", _fake_push)

    app = FastAPI()
    app.include_router(github_router.router)
    app.dependency_overrides[get_session] = lambda: None
    app.dependency_overrides[get_store] = lambda: None
    app.dependency_overrides[get_eval_queue] = lambda: None
    with TestClient(app) as test_client:
        yield test_client


def _post(client: TestClient, body: bytes, *, event: str, sign: bool) -> Any:
    headers = {"X-GitHub-Event": event, "Content-Type": "application/json"}
    if sign:
        headers["X-Hub-Signature-256"] = _sign(body)
    return client.post("/github/webhook", content=body, headers=headers)


def test_accepts_a_body_at_the_limit(client: TestClient) -> None:
    # A body of exactly the bound is accepted (ping short-circuits before the DB
    # path), proving the gate rejects only what is strictly over the limit and a
    # valid signature still works.
    body = b"x" * MAX
    resp = _post(client, body, event="ping", sign=True)
    assert resp.status_code == 200
    assert resp.json()["status"] == "pong"


def test_rejects_a_body_over_the_limit(client: TestClient) -> None:
    body = b"x" * (MAX + 1)
    # Even a body carrying a valid signature is refused: the bound fires before
    # authentication is consulted.
    resp = _post(client, body, event="push", sign=True)
    assert resp.status_code == 413


def test_rejects_a_streamed_oversize_body_without_content_length(
    client: TestClient,
) -> None:
    # A chunked request sends no Content-Length, so the fast-path header check
    # cannot catch it; the streamed accumulation must. httpx sends chunked when
    # content is an iterator.
    def _chunks() -> Iterator[bytes]:
        for _ in range(4):
            yield b"y" * 200  # 800 bytes total, over MAX

    resp = client.post(
        "/github/webhook",
        content=_chunks(),
        headers={"X-GitHub-Event": "push", "Content-Type": "application/json"},
    )
    assert resp.status_code == 413


def test_oversize_is_rejected_before_authentication(client: TestClient) -> None:
    # No signature header at all, oversized body: still 413, not 401. The size
    # gate precedes the signature check by design.
    resp = _post(client, b"z" * (MAX + 50), event="push", sign=False)
    assert resp.status_code == 413


def test_within_limit_invalid_signature_fails_closed(client: TestClient) -> None:
    body = b'{"ref":"refs/heads/dev"}'
    resp = client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": "sha256=" + "0" * 64,
            "Content-Type": "application/json",
        },
    )
    assert resp.status_code == 401


def test_within_limit_missing_signature_fails_closed(client: TestClient) -> None:
    resp = _post(client, b'{"ref":"refs/heads/dev"}', event="push", sign=False)
    assert resp.status_code == 401


def test_within_limit_valid_signature_proceeds(client: TestClient) -> None:
    # A normal signed push under the bound clears both gates and reaches
    # process_push (stubbed), i.e. it enqueues in the real path.
    body = b'{"ref":"refs/heads/dev","after":"a","repository":{}}'
    resp = _post(client, body, event="push", sign=True)
    assert resp.status_code == 200
    assert resp.json()["status"] == "deployed"
