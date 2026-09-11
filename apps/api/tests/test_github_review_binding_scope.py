"""A Slack channel binding must not serialize independent review conversations."""

import base64
import copy
import time
import uuid

import httpx
import pytest
from channel_protocol import scoped_conversation_id
from curie_api import approval_principal
from curie_api.config import get_settings
from curie_api.github_review_events import parse_feedback

from apps.api.tests.test_github_review_events import (
    HEAD,
    REPO,
    post_review,
    review_rows,
)
from apps.api.tests.test_github_review_events import review_app_key as review_app_key
from apps.api.tests.test_github_review_events import review_stack as review_stack


def _other_verified_lineage(client, truth, auth_headers):
    """Actual producer APIs; fixed fixture attestation is not a human click."""
    deployment = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0][
        "deployment_id"
    ]
    conversation = "1700000000.000051"
    worker_headers = {"X-Curie-Worker-Token": "fixture-review-worker-token"}
    selected = client.post(
        f"/v1/internal/workspaces/{deployment}/selection",
        headers=worker_headers,
        json={"conversation_id": scoped_conversation_id("slack", "C0EXAMPLE1", conversation),
              "author": "U0REQUEST1", "repo_full_name": REPO},
    )
    assert selected.status_code == 200, selected.text
    created = client.post("/v1/internal/publications", headers=worker_headers, json={
        "deployment_id": str(deployment), "conversation_id": conversation,
        "repo_full_name": REPO, "author": "U0REQUEST1", "summary": "Second thread fixture",
        "reply_kind": "slack", "reply_channel": "C0EXAMPLE1",
        "reply_placeholder": "1700000000.000052", "dedupe_key": str(uuid.uuid4()),
        "base_sha": HEAD, "patch_b64": base64.b64encode(b"diff --git a/a b/a\n").decode(),
        "changed_paths": ["a"], "expires_in_seconds": 600,
    })
    assert created.status_code == 201, created.text
    publication = created.json()
    truth.pr.update(number=18, node_id="PR_example_18", html_url=f"https://github.com/{REPO}/pull/18")
    truth.pr["head"]["ref"] = publication["branch"]
    principal = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject="U0REQUEST1", kind="chat", actor_channel="C0EXAMPLE1",
        approval_id=publication["approval_id"], scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    resolved = client.post(
        f"/approvals/{publication['approval_id']}/resolve", json={"decision": "approved"},
        headers={**auth_headers, "X-Curie-Approval-Principal": principal},
    )
    assert resolved.status_code == 200, resolved.text
    advanced = client.patch(
        f"/v1/internal/publications/{publication['id']}/lineage",
        headers=worker_headers,
        json={"expected_version": 1, "expected_head_sha": None, "state": "open",
              "pr_number": 18, "pr_url": f"https://github.com/{REPO}/pull/18", "head_sha": HEAD},
    )
    assert advanced.status_code == 200, advanced.text
    review_rows("UPDATE curie.publications SET outcome_history_ready_at=now(), "
                "result_reported_at=now() WHERE id=:id", {"id": publication["id"]})
    return conversation


@pytest.mark.parametrize("other_conversation", [False, True])
def test_concurrent_feedback_is_durable_for_same_and_independent_thread_lineages(
    review_stack, auth_headers, other_conversation,
):
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    original_http = client.app.state.http_client

    def handle(request):
        # Both response shapes are documented by GitHub REST Pull requests/Get.
        if request.url.path == f"/repos/{REPO}/pulls/18":
            truth.calls.append(request.url.path)
            return httpx.Response(200, json=truth.pr)
        return truth.handle(request)

    injected = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client.app.state.http_client = injected
    delivery = str(uuid.uuid4())
    second_id = None
    try:
        if other_conversation:
            expected_conversation = _other_verified_lineage(client, truth, auth_headers)
            assert valkey.xlen(stream) == 1  # Producer setup never erases or wakes the queue.
            truth.payload["issue"]["number"] = 18
            truth.payload["issue"]["pull_request"].update(
                html_url=f"https://github.com/{REPO}/pull/18",
                url=f"https://api.github.com/repos/{REPO}/pulls/18",
            )
        else:
            expected_conversation = "1700000000.000001"
        number = 18 if other_conversation else 17
        truth.payload["comment"].update(
            id=72, html_url=f"https://github.com/{REPO}/pull/{number}#issuecomment-72"
        )
        truth.comment = copy.deepcopy(truth.payload["comment"])
        truth.comment["issue_url"] = f"https://api.github.com/repos/{REPO}/issues/{number}"
        truth.comment["pull_request_url"] = f"https://api.github.com/repos/{REPO}/pulls/{number}"
        second_id = parse_feedback("issue_comment", truth.payload, delivery).event_id
        result = post_review(client, truth, delivery=delivery)
        assert result.status_code == 200, result.text
        assert result.json()["status"] == "feedback_queued", result.text
        rows = review_rows(
            "SELECT lineage_id,binding_id,status,turn FROM curie.github_review_feedback"
        )
        assert len(rows) == 2 and all(row["status"] == "queued" for row in rows)
        assert len({row["binding_id"] for row in rows}) == 1
        assert len({row["lineage_id"] for row in rows}) == (2 if other_conversation else 1)
        assert {row["turn"]["conversation_id"] for row in rows} == {
            "1700000000.000001", expected_conversation
        }
        assert valkey.xlen(stream) == 2
        assert review_rows("SELECT id FROM curie.publication_review_reservations") == []
    finally:
        client.app.state.http_client = original_http
        try:
            client.portal.call(injected.aclose)
        finally:
            if second_id is not None:
                valkey.delete(f"curie:github-review:{second_id}")
