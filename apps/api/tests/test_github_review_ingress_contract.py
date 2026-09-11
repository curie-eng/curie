"""Transport contract for authenticated GitHub review-feedback ingress."""

import hashlib
import hmac

import pytest
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from httpx import Response

# Event names come from GitHub's webhook event catalog:
# https://docs.github.com/en/webhooks/webhook-events-and-payloads
REVIEW_EVENTS = (
    "issue_comment",
    "pull_request_review_comment",
    "pull_request_review",
)
MALFORMED_JSON = b'{"action":'


def _signature(body: bytes) -> str:
    """Sign bytes using GitHub's documented HMAC-SHA256 delivery contract.

    https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
    """

    secret = get_settings().github_webhook_secret.encode()
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _post(
    client: TestClient, event: str, *, signature: str | None = None
) -> Response:
    return client.post(
        "/github/webhook",
        content=MALFORMED_JSON,
        headers={
            "X-GitHub-Delivery": "00000000-0000-4000-8000-000000000001",
            "X-GitHub-Event": event,
            "X-Hub-Signature-256": signature or _signature(MALFORMED_JSON),
            "Content-Type": "application/json",
        },
    )


@pytest.mark.parametrize("event", REVIEW_EVENTS)
def test_signed_review_event_rejects_malformed_json(
    client: TestClient, event: str
) -> None:
    response = _post(client, event)

    assert response.status_code == 400, response.text


def test_review_event_rejects_bad_signature_before_parsing(client: TestClient) -> None:
    response = _post(client, "issue_comment", signature="sha256=" + "0" * 64)

    assert response.status_code == 401, response.text


def test_unrelated_event_remains_ignored_without_parsing(client: TestClient) -> None:
    response = _post(client, "issues")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"
