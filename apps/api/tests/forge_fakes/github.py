"""GitHub REST and webhook fakes shared by the factory intake suites.

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from typing import Any

import httpx
from curie_api.config import get_settings
from curie_api.github_app import GitHubInstallationRefused
from fastapi.testclient import TestClient

REPO = "acme-corp/acme-bot"

REPO_ID = 4401
INSTALLATION_ID = 5501
SENDER_ID = 6601
SENDER = "octocat"
LABEL = "factory"
MENTION = "curie"

BASE_SHA = "0" * 39 + "1"

_ENV = {
    "GITHUB_FACTORY_INGRESS_ENABLED": "true",
    "GITHUB_FACTORY_LABEL": LABEL,
    "GITHUB_FACTORY_MENTION": MENTION,
    "GITHUB_REVIEW_INGRESS_ENABLED": "false",
    "GITHUB_APP_ID": "51",
    "GITHUB_APP_PRIVATE_KEY": "example-private-key",
    "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
    "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
    "GITHUB_TOKEN": "",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "RESUME_RECONCILER_ENABLED": "false",
    "APPROVAL_SWEEP_INTERVAL_S": "0",
    "DEAD_LETTER_WATCH_INTERVAL_S": "0",
}


class GitHubAPI:
    """In-process stand-in for the GitHub REST reads the intake verifier makes."""

    def __init__(self) -> None:
        self.issue_state = "open"
        self.labels = [LABEL]
        self.permission = "write"
        self.permission_requests: list[httpx.Request] = []
        self.permission_user_id = SENDER_ID
        self.comment_body: str | None = None
        self.comment_app: dict[str, Any] | None = None
        self.repository_id = REPO_ID
        self.issue_number = 0
        # @spec apps/api/README.md#factory-test-isolation
        # Timeline events retain identity inside this stand-in while independent
        # fixtures cannot address each other's request-derived Valkey keys.
        self.label_event_base = uuid.uuid4().int >> 80
        self.label_event_ids: dict[int, int] = {}

    def advance_label_event(self, number: int) -> None:
        """Record a newer labeled timeline event. A relabel webhook reads it."""

        current = self.label_event_ids.get(number, self.label_event_base + number)
        self.label_event_ids[number] = current + 1

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}":
            return httpx.Response(
                200,
                json={"id": self.repository_id, "full_name": REPO, "default_branch": "main"},
            )
        if path.startswith(f"/repos/{REPO}/branches/"):
            # Admission resolves the base and reads its commit (#3095).
            name = path.removeprefix(f"/repos/{REPO}/branches/")
            return httpx.Response(200, json={"name": name, "commit": {"sha": BASE_SHA}})
        if path.startswith(f"/repos/{REPO}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "id": comment_id,
                    "body": self.comment_body,
                    "user": {"id": SENDER_ID, "login": SENDER, "type": "User"},
                    "performed_via_github_app": self.comment_app,
                    "issue_url": (
                        f"https://api.github.com/repos/{REPO}/issues/{self.issue_number}"
                    ),
                },
            )
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            event_id = self.label_event_ids.get(number, self.label_event_base + number)
            return httpx.Response(
                200,
                json=[
                    {
                        "id": event_id,
                        "event": "labeled",
                        "label": {"name": LABEL},
                        "actor": {"id": SENDER_ID, "login": SENDER, "type": "User"},
                        "created_at": "2026-09-01T00:00:00Z",
                    }
                ],
            )
        if path.startswith(f"/repos/{REPO}/issues/"):
            number = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "number": number,
                    "state": self.issue_state,
                    "labels": [{"name": name} for name in self.labels],
                },
            )
        if path == f"/repos/{REPO}/collaborators/{SENDER}/permission":
            self.permission_requests.append(request)
            return httpx.Response(
                200,
                json={
                    "permission": self.permission,
                    "user": {"id": self.permission_user_id, "login": SENDER},
                },
            )
        return httpx.Response(404, json={"message": "missing fixture"})


class _Credentials:
    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id != INSTALLATION_ID:
            raise GitHubInstallationRefused("installation was not rediscovered")
        return "fixture-installation-token"


def _signature(body: bytes) -> str:
    secret = get_settings().github_webhook_secret.encode()
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _post(
    client: TestClient,
    event: str,
    payload: dict[str, Any],
    *,
    delivery: str | None = None,
    signature: str | None = None,
) -> httpx.Response:
    body = json.dumps(payload).encode()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": delivery or str(uuid.uuid4()),
            "X-GitHub-Event": event,
            "X-Hub-Signature-256": signature if signature is not None else _signature(body),
            "Content-Type": "application/json",
        },
    )


def _sender(
    sender_type: str = "User", login: str = SENDER, sender_id: int = SENDER_ID
) -> dict[str, Any]:
    return {"id": sender_id, "login": login, "type": sender_type}


def _issue_event(action: str, number: int, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": action,
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _sender(),
        "issue": {
            "number": number,
            "state": "open",
            "body": "do not store this issue body",
        },
    }
    payload.update(extra)
    return payload


def _comment_event(number: int, body: str, comment_id: int) -> dict[str, Any]:
    return _issue_event(
        "created",
        number,
        comment={
            "id": comment_id,
            "body": body,
            "user": _sender(),
            "performed_via_github_app": None,
        },
    )
