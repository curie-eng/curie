"""GitHub sender fixtures through the feedback-normalization boundary.

Payload families and action names are documented at
https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
and the pull_request_review / pull_request_review_comment sections. These
fixtures prove parsing and refusal only; HMAC, current GitHub truth and durable
lineage authorization are exercised separately through the actual HTTP ingress.
"""

import asyncio
import base64
import copy
import hashlib
import hmac
import json
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace

import httpx
import pytest
from channel_protocol import scoped_conversation_id
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api import approval_principal
from curie_api.config import Settings, get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.github_review_events import FeedbackIgnored, parse_feedback
from curie_api.github_review_truth import BoundReviewLineage, verify_feedback_truth
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import literal, text
from sqlalchemy.ext.asyncio import create_async_engine

DELIVERY = str(uuid.UUID(int=1))
REPO = "acme-corp/acme-bot"
HEAD = "a" * 40


def feedback_payload(event: str = "issue_comment") -> dict:
    user = {"id": 41, "login": "example-reviewer", "type": "User"}
    feedback = {
        "id": 71,
        "body": "Please add a regression test before updating this PR.",
        "user": user,
        "created_at": "2026-09-05T01:00:00Z",
        "updated_at": "2026-09-05T01:00:00Z",
        "performed_via_github_app": None,
        "author_association": "MEMBER",
    }
    result = {
        "action": "created",
        "installation": {"id": 11},
        "repository": {"id": 21, "full_name": REPO},
        "sender": copy.deepcopy(user),
    }
    pr = {
        "number": 17,
        "state": "open",
        "html_url": f"https://github.com/{REPO}/pull/17",
        "head": {"sha": HEAD, "ref": "curie/example", "repo": {"id": 21, "full_name": REPO}},
        "base": {"repo": {"id": 21, "full_name": REPO}},
    }
    if event == "issue_comment":
        result["issue"] = {
            "number": 17,
            "state": "open",
            "pull_request": {
                "html_url": pr["html_url"],
                "url": f"https://api.github.com/repos/{REPO}/pulls/17",
            },
        }
        feedback["html_url"] = f"{pr['html_url']}#issuecomment-71"
        result["comment"] = feedback
    elif event == "pull_request_review_comment":
        feedback.update(
            {
                "html_url": f"{pr['html_url']}#discussion_r71",
                "commit_id": HEAD,
                "path": "src/example.py",
                "line": 12,
                "pull_request_review_id": 81,
            }
        )
        result.update({"pull_request": pr, "comment": feedback})
    else:
        feedback.update(
            {
                "html_url": f"{pr['html_url']}#pullrequestreview-71",
                "commit_id": HEAD,
                "state": "changes_requested",
                "submitted_at": "2026-09-05T01:00:00Z",
            }
        )
        result.update({"action": "submitted", "pull_request": pr, "review": feedback})
    return result


@pytest.mark.parametrize(
    "event", ["issue_comment", "pull_request_review_comment", "pull_request_review"]
)
def test_each_human_review_family_retains_canonical_identity_and_provenance(event: str) -> None:
    result = parse_feedback(event, feedback_payload(event), DELIVERY)
    assert result.repo_full_name == REPO
    assert result.pr_number == 17
    assert result.sender_id == 41 and result.sender_login == "example-reviewer"
    assert result.feedback_id == 71 and result.installation_id == 11
    assert result.body == "Please add a regression test before updating this PR."
    assert result.url.startswith(f"https://github.com/{REPO}/pull/17#")
    assert (
        result.event_id
        == parse_feedback(event, feedback_payload(event), str(uuid.UUID(int=2))).event_id
    )
    if event == "pull_request_review_comment":
        assert result.path == "src/example.py" and result.line == 12 and result.review_id == 81


@pytest.mark.parametrize("action", ["edited", "deleted", "dismissed"])
def test_non_creation_actions_are_observably_ignored(action: str) -> None:
    payload = feedback_payload()
    payload["action"] = action
    with pytest.raises(FeedbackIgnored, match="unsupported_action"):
        parse_feedback("issue_comment", payload, DELIVERY)


@pytest.mark.parametrize(
    "mutation,reason",
    [
        (lambda p: p.pop("installation"), "invalid_installation"),
        (lambda p: p["installation"].update(id=True), "invalid_installation"),
        (lambda p: p["repository"].update(full_name="acme-corp/../other"), "invalid_repository"),
        (lambda p: p["sender"].update(id=42), "sender_mismatch"),
        (lambda p: p["sender"].update(type="Bot"), "non_human_sender"),
        (lambda p: p["sender"].update(login="ghost"), "non_human_sender"),
        (lambda p: p["comment"]["user"].update(type="Bot"), "non_human_sender"),
        (lambda p: p["comment"].update(performed_via_github_app={"id": 51}), "app_authored"),
        (lambda p: p["issue"].pop("pull_request"), "not_pull_request"),
        (lambda p: p["issue"].update(state="closed"), "terminal_pull_request"),
        (lambda p: p["comment"].update(body="  "), "empty_feedback"),
        (
            lambda p: p["comment"].update(html_url="https://evil.example.com/private-sentinel"),
            "invalid_feedback_url",
        ),
        (lambda p: p["comment"].update(updated_at="2026-09-05T01:00:01Z"), "edited_feedback"),
    ],
)
def test_invalid_or_non_human_feedback_cannot_be_normalized(mutation, reason: str) -> None:
    payload = feedback_payload()
    mutation(payload)
    with pytest.raises(FeedbackIgnored, match=reason) as caught:
        parse_feedback("issue_comment", payload, DELIVERY)
    assert "private-sentinel" not in str(caught.value)


def test_delivery_header_must_be_a_real_uuid() -> None:
    with pytest.raises(FeedbackIgnored, match="invalid_delivery"):
        parse_feedback("issue_comment", feedback_payload(), "not-a-delivery-private-sentinel")


def test_app_reinstallation_does_not_change_feedback_execution_identity() -> None:
    payload = feedback_payload()
    original = parse_feedback("issue_comment", payload, DELIVERY)
    payload["installation"]["id"] = 12
    replacement = parse_feedback("issue_comment", payload, str(uuid.UUID(int=3)))
    assert replacement.event_id == original.event_id
    assert replacement.installation_id != original.installation_id


@pytest.mark.parametrize(
    "association", [None, "NONE", "FIRST_TIME_CONTRIBUTOR", "FIRST_TIMER", [], "member"]
)
def test_drive_by_or_malformed_sender_association_cannot_authorize_feedback(association) -> None:
    payload = feedback_payload()
    payload["comment"]["author_association"] = association
    with pytest.raises(FeedbackIgnored, match="unauthorized_association"):
        parse_feedback("issue_comment", payload, DELIVERY)


@pytest.mark.parametrize("association", ["OWNER", "MEMBER", "COLLABORATOR"])
def test_signed_member_associations_are_retained_and_verified(association: str) -> None:
    payload = feedback_payload()
    payload["comment"]["author_association"] = association
    assert parse_feedback("issue_comment", payload, DELIVERY).author_association == association


def test_review_ingress_is_disabled_by_default_and_refuses_incomplete_enablement() -> None:
    from pydantic import ValidationError

    assert Settings().github_review_ingress_enabled is False
    with pytest.raises(ValidationError, match="GitHub review ingress"):
        Settings(github_review_ingress_enabled=True)


@pytest.mark.parametrize("state", [None, 1, {}, [], "approved", "dismissed"])
def test_non_actionable_or_malformed_review_state_has_a_redacted_refusal(state) -> None:
    payload = feedback_payload("pull_request_review")
    payload["review"]["state"] = state
    with pytest.raises(FeedbackIgnored, match="non_actionable_review"):
        parse_feedback("pull_request_review", payload, DELIVERY)


class GitHubTruth:
    """Only GitHub HTTP is replaced; signer/resolver/verifier execute normally.

    REST comment identities, issue_url and pull_request_url follow:
    https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
    https://docs.github.com/en/rest/pulls/comments#get-a-review-comment-for-a-pull-request
    https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
    """

    def __init__(self, event: str, key: str):
        self.payload = feedback_payload(event)
        self.feedback = parse_feedback(event, self.payload, DELIVERY)
        self.settings = Settings(
            github_app_id="51",
            github_app_private_key=key,
            github_token="fixture-pat-must-not-be-used",
        )
        self.lineage = BoundReviewLineage(
            REPO, 17, "curie/example", HEAD, 21, 11, "PR_example_17", "main"
        )
        self.calls: list[str] = []
        self.installation = {"id": 11}
        self.installation_status = 200
        self.repo = {"id": 21, "full_name": REPO}
        self.pr = copy.deepcopy(feedback_payload("pull_request_review")["pull_request"])
        self.pr["merged"] = False
        self.pr["node_id"] = "PR_example_17"
        self.pr["base"]["ref"] = "main"
        self.comment = copy.deepcopy(self.payload.get("comment", self.payload.get("review")))
        self.comment["issue_url"] = f"https://api.github.com/repos/{REPO}/issues/17"
        self.comment["pull_request_url"] = f"https://api.github.com/repos/{REPO}/pulls/17"
        self.feedback_status = 200

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request.url.path)
        assert "fixture-pat" not in request.headers.get("Authorization", "")
        if request.url.path.endswith("/installation"):
            return httpx.Response(self.installation_status, json=self.installation)
        if request.url.path.endswith("/access_tokens"):
            return httpx.Response(
                201,
                json={
                    "token": "fixture-app-token-private-sentinel",
                    "expires_at": "2999-01-01T00:00:00Z",
                },
            )
        assert request.headers["Authorization"] == "Bearer fixture-app-token-private-sentinel"
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=self.repo)
        if request.url.path == f"/repos/{REPO}/pulls/17":
            return httpx.Response(200, json=self.pr)
        return httpx.Response(
            self.feedback_status,
            json=self.comment,
            headers={"Location": "https://evil.example.com/private-sentinel"},
        )


@pytest.fixture(scope="module")
def review_app_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


async def verify_truth(truth: GitHubTruth, monkeypatch: pytest.MonkeyPatch) -> str:
    real_client = httpx.Client
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(truth.handle)),
    )
    _RESOLVERS.clear()
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(truth.handle), follow_redirects=True
        ) as client:
            return await verify_feedback_truth(
                truth.feedback,
                truth.lineage,
                settings=truth.settings,
                client=client,
            )
    finally:
        _RESOLVERS.clear()


@pytest.mark.parametrize(
    "changed",
    [
        {"repository_id": 999},
        {"installation_id": 999},
        {"pr_node_id": "PR_other_identity"},
        {"base_ref": "another-base"},
    ],
)
def test_review_truth_must_match_immutable_persisted_authority(
    changed: dict, review_app_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    truth = GitHubTruth("issue_comment", review_app_key)
    truth.lineage = replace(truth.lineage, **changed)
    with pytest.raises(FeedbackIgnored):
        asyncio.run(verify_truth(truth, monkeypatch))


@pytest.mark.parametrize(
    "event", ["issue_comment", "pull_request_review_comment", "pull_request_review"]
)
def test_current_app_repo_pr_and_human_feedback_must_independently_agree(
    event: str,
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth = GitHubTruth(event, review_app_key)
    assert asyncio.run(verify_truth(truth, monkeypatch)) == HEAD
    assert truth.calls[0] == f"/repos/{REPO}/installation"
    assert f"/repos/{REPO}" in truth.calls
    assert f"/repos/{REPO}/pulls/17" in truth.calls


@pytest.mark.parametrize(
    "mutation,reason",
    [
        (lambda t: t.installation.update(id=12), "installation_unverified"),
        (lambda t: setattr(t, "installation_status", 302), "installation_unverified"),
        (lambda t: t.repo.update(id=22), "repository_mismatch"),
        (lambda t: t.pr["head"].update(ref="other-branch"), "pull_request_mismatch"),
        (lambda t: t.pr["head"].update(sha="b" * 40), "stale_feedback_head"),
        (lambda t: t.pr["base"]["repo"].update(id=22), "repository_mismatch"),
        (lambda t: t.pr.update(state="closed", merged=True), "terminal_pull_request"),
        (
            lambda t: t.comment.update(body="forged-current-body-private-sentinel"),
            "feedback_changed",
        ),
        (lambda t: t.comment["user"].update(id=42), "feedback_changed"),
        (lambda t: t.comment.update(updated_at="2026-09-05T01:00:01Z"), "edited_feedback"),
        (
            lambda t: t.comment.update(issue_url=f"https://api.github.com/repos/{REPO}/issues/18"),
            "feedback_target_mismatch",
        ),
        (lambda t: setattr(t, "feedback_status", 404), "feedback_unavailable"),
        (lambda t: setattr(t, "feedback_status", 401), "feedback_unavailable"),
        (lambda t: setattr(t, "feedback_status", 302), "feedback_unavailable"),
    ],
)
def test_signed_claims_cannot_override_current_github_authority(
    mutation,
    reason: str,
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth = GitHubTruth("issue_comment", review_app_key)
    mutation(truth)
    with pytest.raises(FeedbackIgnored, match=reason) as caught:
        asyncio.run(verify_truth(truth, monkeypatch))
    assert "private-sentinel" not in str(caught.value)
    assert not any("private-sentinel" in path for path in truth.calls)


def test_a_user_pat_is_not_product_app_installation_proof(
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth = GitHubTruth("issue_comment", review_app_key)
    truth.settings = Settings(github_token="fixture-pat-must-not-be-used")
    with pytest.raises(FeedbackIgnored, match="installation_unverified"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert truth.calls == []


def test_webhook_repo_cannot_select_a_different_persisted_lineage(
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth = GitHubTruth("issue_comment", review_app_key)
    truth.lineage = replace(truth.lineage, repo_full_name="acme-corp/other-bot")
    with pytest.raises(FeedbackIgnored, match="lineage_mismatch"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert truth.calls == []


@pytest.mark.parametrize("surface", ["installation", "feedback"])
@pytest.mark.parametrize("status", [401, 403, 404, 429, 503])
def test_current_authority_outage_is_retryable_and_recovers(
    surface: str,
    status: int,
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api.github_review_events import FeedbackUnavailable

    truth = GitHubTruth("issue_comment", review_app_key)
    # GitHub hides private resources behind 404 and uses 403 for rate limits.
    # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api
    setattr(truth, f"{surface}_status", status)
    with pytest.raises(FeedbackUnavailable):
        asyncio.run(verify_truth(truth, monkeypatch))
    setattr(truth, f"{surface}_status", 200)
    assert asyncio.run(verify_truth(truth, monkeypatch)) == HEAD


def test_verified_reinstallation_retires_old_cached_token_before_any_repo_read(
    review_app_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api.github_app import GitHubCredentials

    truth = GitHubTruth("issue_comment", review_app_key)
    requests: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/access_tokens"):
            installation = request.url.path.split("/")[-2]
            return httpx.Response(
                201,
                json={
                    "token": f"fixture-installation-{installation}",
                    "expires_at": "2999-01-01T00:00:00Z",
                },
            )
        return truth.handle(request)

    real_client = httpx.Client
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(github)),
    )
    credentials = GitHubCredentials(truth.settings)
    assert credentials.token_for_verified_installation(REPO, 11) == "fixture-installation-11"
    # An unchanged installation may reuse its cache; a different installation
    # must mint a new token even when the previous token has not yet expired.
    assert credentials.token_for_verified_installation(REPO, 11) == "fixture-installation-11"
    truth.installation["id"] = 12
    assert credentials.token_for_verified_installation(REPO, 12) == "fixture-installation-12"
    assert [r.url.path for r in requests if r.method == "POST"] == [
        "/app/installations/11/access_tokens",
        "/app/installations/12/access_tokens",
    ]


def review_rows(statement: str, parameters: dict | None = None) -> list[dict]:
    async def execute() -> list[dict]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), parameters or {})
                return [dict(row) for row in result.mappings()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(execute())


@pytest.fixture
def review_stack(
    clean_db: None,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    review_app_key: str,
) -> Iterator[tuple[TestClient, GitHubTruth, object, str]]:
    """Real migrated Postgres/Valkey/API, with only GitHub HTTP replaced.

    The producer captures App identity through its actual approval/advance API.
    The fixed example attestation is test setup, not a real human Slack click;
    publication GitHub HTTP and worker outcome-history delivery remain fixtures.
    """
    event = getattr(request, "param", "issue_comment")
    truth = GitHubTruth(event, review_app_key)
    stream = f"test:curie:github-review:{uuid.uuid4().hex}"
    for key, value in {
        "RUNS_STREAM": stream,
        "KEY_PREFIX": f"{stream}:worker",
        "INTERNAL_WORKER_TOKEN": "fixture-review-worker-token",
        "GITHUB_WEBHOOK_SECRET": "fixture-review-webhook-secret",
        "GITHUB_REVIEW_INGRESS_ENABLED": "true",
        "GITHUB_APP_ID": "51",
        "GITHUB_APP_PRIVATE_KEY": review_app_key,
        "GITHUB_TOKEN": "",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_REVIEW_RECONCILER_INTERVAL_S": "0",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "RESUME_RECONCILER_ENABLED": "false",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _RESOLVERS.clear()
    valkey = connect_or_skip(decode_responses=True)
    with ExitStack() as owned:
        owned.callback(get_settings.cache_clear)
        owned.callback(_RESOLVERS.clear)
        owned.callback(valkey.close)
        def cleanup_worker_markers() -> None:
            keys = list(valkey.scan_iter(match=f"{stream}:worker:*"))
            if keys:
                valkey.delete(*keys)

        owned.callback(cleanup_worker_markers)
        owned.callback(
            valkey.delete,
            stream,
            f"{stream}:dead",
            f"curie:github-review:{truth.feedback.event_id}",
        )
        client = owned.enter_context(TestClient(create_app()))
        real_client = httpx.Client
        monkeypatch.setattr(
            "curie_api.github_app.httpx.Client",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(truth.handle)),
        )
        external = httpx.AsyncClient(transport=httpx.MockTransport(truth.handle))
        owned.callback(client.portal.call, external.aclose)
        client.app.state.http_client = external
        auth = {"X-API-Key": get_settings().api_key}
        agent = client.post(
            "/agents",
            headers=auth,
            json={
                "name": f"acme-review-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
            },
        )
        assert agent.status_code == 201, agent.text
        agent_id = agent.json()["id"]
        version = client.post(
            f"/agents/{agent_id}/versions",
            headers=auth,
            json={"version_label": "fixture", "created_by": "operator"},
        )
        assert version.status_code == 201, version.text
        deployment = client.post(
            "/deployments",
            headers=auth,
            json={
                "agent_id": agent_id,
                "version_id": version.json()["id"],
                "environment": "dev",
            },
        )
        assert deployment.status_code == 201, deployment.text
        selected = client.post(
            f"/v1/internal/workspaces/{deployment.json()['id']}/selection",
            headers={"X-Curie-Worker-Token": "fixture-review-worker-token"},
            json={
                "conversation_id": scoped_conversation_id(
                    "slack", "C0EXAMPLE1", "1700000000.000001"
                ),
                "author": "U0REQUEST1",
                "repo_full_name": REPO,
            },
        )
        assert selected.status_code == 200, selected.text
        publication = client.post(
            "/v1/internal/publications",
            headers={
                "X-Curie-Worker-Token": "fixture-review-worker-token",
            },
            json={
                "deployment_id": deployment.json()["id"],
                "conversation_id": "1700000000.000001",
                "repo_full_name": REPO,
                "author": "U0REQUEST1",
                "summary": "Fixture publication",
                "reply_kind": "slack",
                "reply_channel": "C0EXAMPLE1",
                "reply_placeholder": "1700000000.000002",
                "dedupe_key": f"fixture-{uuid.uuid4()}",
                "base_sha": HEAD,
                "patch_b64": base64.b64encode(b"diff --git a/a b/a\n").decode(),
                "changed_paths": ["a"],
                "expires_in_seconds": 600,
            },
        )
        assert publication.status_code == 201, publication.text
        branch = publication.json()["branch"]
        truth.pr["head"]["ref"] = branch
        principal = approval_principal.mint(
            get_settings().approval_chat_attester_secret,
            subject="U0REQUEST1",
            kind="chat",
            actor_channel="C0EXAMPLE1",
            approval_id=publication.json()["approval_id"],
            scope=approval_principal.APPROVE_SCOPE,
            exp=int(time.time()) + 60,
        )
        resolved = client.post(
            f"/approvals/{publication.json()['approval_id']}/resolve",
            json={"decision": "approved"},
            headers={**auth, "X-Curie-Approval-Principal": principal},
        )
        assert resolved.status_code == 200, resolved.text
        advanced = client.patch(
            f"/v1/internal/publications/{publication.json()['id']}/lineage",
            headers={"X-Curie-Worker-Token": "fixture-review-worker-token"},
            json={
                "expected_version": 1,
                "expected_head_sha": None,
                "state": "open",
                "pr_number": 17,
                "pr_url": f"https://github.com/{REPO}/pull/17",
                "head_sha": HEAD,
            },
        )
        assert advanced.status_code == 200, advanced.text
        review_rows(
            "UPDATE curie.publications SET outcome_history_ready_at=now(), "
            "result_reported_at=now(), terminal_at=now() WHERE id=:id",
            {"id": publication.json()["id"]},
        )
        # Remove only this fixture's synthetic approval-resume input, before
        # admitting any review. This test does not execute that earlier turn.
        valkey.delete(stream)
        assert review_rows(
            "SELECT github_repository_id, github_installation_id, github_pr_node_id, base_ref "
            "FROM curie.thread_publication_lineages"
        ) == [{
            "github_repository_id": 21,
            "github_installation_id": 11,
            "github_pr_node_id": "PR_example_17",
            "base_ref": "main",
        }]
        truth.calls.clear()  # Attribute subsequent reads to the ingress under test.
        yield client, truth, valkey, stream


def post_review(
    client: TestClient,
    truth: GitHubTruth,
    *,
    delivery: str = DELIVERY,
    signature: str | None = None,
) -> httpx.Response:
    body = json.dumps(truth.payload).encode()
    signature = (
        signature
        or "sha256="
        + hmac.new(
            b"fixture-review-webhook-secret",
            body,
            hashlib.sha256,
        ).hexdigest()
    )
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": truth.feedback.event,
            "X-GitHub-Delivery": delivery,
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )


@pytest.mark.parametrize(
    "review_stack",
    [
        "issue_comment",
        "pull_request_review_comment",
        "pull_request_review",
    ],
    indirect=True,
)
def test_real_ingress_persists_and_enqueues_exactly_one_honest_bound_turn(review_stack) -> None:
    from aci_protocol import parse_queued_turn

    client, truth, valkey, stream = review_stack
    first = post_review(client, truth)
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "feedback_queued"
    for delivery in (DELIVERY, str(uuid.UUID(int=2))):
        duplicate = post_review(client, truth, delivery=delivery)
        assert duplicate.status_code == 200, duplicate.text
        assert duplicate.json()["status"] == "feedback_duplicate"
    entries = valkey.xrange(stream)
    assert len(entries) == 1
    turn = parse_queued_turn(entries[0][1]["payload"])
    assert turn.event_id == truth.feedback.event_id
    assert turn.conversation_id == "1700000000.000001"
    assert turn.reply_handle.kind == "slack" and turn.reply_handle.channel == "C0EXAMPLE1"
    assert turn.reply_handle.placeholder is None
    assert turn.author == "github:41:example-reviewer"
    assert not turn.source.is_job
    assert truth.feedback.body in turn.text and truth.feedback.url in turn.text
    assert "fixture-review-worker-token" not in entries[0][1]["payload"]
    assert "fixture-app-token" not in entries[0][1]["payload"]
    rows = review_rows("SELECT status, stream_id, version FROM curie.github_review_feedback")
    assert rows == [{"status": "queued", "stream_id": entries[0][0], "version": 2}]


def test_invalid_hmac_cannot_read_github_persist_or_enqueue(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    response = post_review(client, truth, signature="sha256=invalid")
    assert response.status_code == 401
    assert truth.calls == []
    assert valkey.xlen(stream) == 0
    assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []


def test_disabled_review_ingress_does_not_read_authority_or_enqueue(
    review_stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, truth, valkey, stream = review_stack
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    get_settings.cache_clear()
    response = post_review(client, truth)
    assert response.status_code == 200 and response.json()["status"] == "feedback_disabled"
    assert truth.calls == []
    assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []
    assert valkey.xlen(stream) == 0


def test_binding_quota_refuses_new_feedback_and_keeps_an_observable_row(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    client.app.state.github_review_reconciler._settings.channel_binding_backlog_limit = 0
    response = post_review(client, truth)
    assert response.status_code == 200 and response.json()["status"] == "feedback_refused"
    assert valkey.xlen(stream) == 0
    rows = review_rows("SELECT status,error_code FROM curie.github_review_feedback")
    assert rows == [{"status": "refused", "error_code": "binding_backlog_quota"}]
    assert post_review(client, truth).json()["status"] == "feedback_duplicate"
    assert valkey.xlen(stream) == 0


@pytest.mark.parametrize("permission_case", [
    "legacy", "allowed", "revoked", "wrong_id", "bad_login", "http404", "http403", "timeout",
    "reserve_unavailable", "http403_cap", "reserve_cap",
])
def test_real_review_consumer_waits_for_active_turn_and_revalidates_before_new_turn(
    review_stack, tmp_path, permission_case,
) -> None:
    """Actual API/DB/Valkey/consumer/RunnerClient; GitHub and ACI peer are fixtures.

    An existing managed route is a fixture premise, not a claim of a real
    checkout or Kubernetes recovery. This asserts durable review deferral and
    exact reservation origin; GitHub publication and approval remain separate.
    """
    import uvicorn
    from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
    from aci_protocol.s3 import build_s3_client
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.approvals import ApprovalClient
    from curie_worker.binding import BindingResolver
    from curie_worker.consumer import Consumer
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from curie_worker.workspace import (
        SubprocessCommands,
        WorkspaceClaimCoordinator,
        WorkspaceCredentialClient,
        WorkspaceLimits,
        WorkspaceObjectStore,
        WorkspacePreparer,
    )

    from apps.worker.tests.kernel.conftest import kernel_harness, make_config

    client, truth, valkey, stream = review_stack
    permission = {"permission": "write", "role_name": "write", "user": {"id": 41}}
    permission_status = 200
    permission_timeout = False
    reservation_fault_installed = False
    reservation_fault_name = f"review_reservation_{uuid.uuid4().hex}"
    permission_path = f"/repos/{REPO}/collaborators/example-reviewer/permission"
    lineage_authorization = "Basic " + base64.b64encode(
        b"x-access-token:fixture-app-token-private-sentinel"
    ).decode()
    if permission_case != "legacy":
        from dataclasses import replace

        truth.payload["comment"]["author_association"] = "CONTRIBUTOR"
        truth.comment["author_association"] = "CONTRIBUTOR"
        truth.feedback = replace(truth.feedback, author_association="CONTRIBUTOR")

    def github_permission(request):
        # Observed with a scoped App installation token on GitHub REST,
        # 2026-09-05: Basic x-access-token and Bearer both read the same private
        # PR (200); an invalid Basic credential is refused (404). The existing
        # lineage reader uses Basic; review verification still requires Bearer.
        if (
            request.url.path == f"/repos/{REPO}/pulls/17"
            and request.headers.get("Authorization") == lineage_authorization
        ):
            truth.calls.append(request.url.path)
            return httpx.Response(200, json=truth.pr)
        if request.url.path != permission_path:
            return truth.handle(request)
        truth.calls.append(request.url.path)
        assert request.headers["Authorization"] == "Bearer fixture-app-token-private-sentinel"
        if permission_timeout:
            raise httpx.ReadTimeout("synthetic permission timeout", request=request)
        return httpx.Response(permission_status, json=permission)

    settings = get_settings()
    names = {"stream": stream, "group": f"{stream}:group",
             "prefix": settings.worker_key_prefix, "sandbox_prefix": f"{stream}:sandbox"}
    original = QueuedTurn(
        event_id=f"ordinary-{uuid.uuid4()}", conversation_id="1700000000.000001",
        author="U0REQUEST1", text="Continue the existing work.",
        reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder=None),
        received_at="2026-09-05T01:00:00+00:00",
    )
    thread_key = scoped_conversation_id("slack", "C0EXAMPLE1", original.conversation_id)
    bucket = f"review-consumer-{uuid.uuid4().hex}"
    objects_client = build_s3_client(
        endpoint_url=settings.s3_endpoint_url, access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key, region=settings.s3_region,
    )
    objects_client.create_bucket(Bucket=bucket)

    def install_reservation_fault() -> None:
        nonlocal reservation_fault_installed
        # Real task-owned database fault at the reserve sibling, after the live
        # API verifier succeeds. The API/DB/Valkey are never replaced by doubles.
        # clean_db depends on _disposable_db: this function exists only in that
        # run's freshly created DB. The runtime owner also drops its own volume
        # after a killed pytest. Names and the origin predicate are task scoped.
        reservation_fault_installed = True
        review_rows(f"""
            CREATE FUNCTION curie.{reservation_fault_name}()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'task-owned reservation failure'; END $$
        """)
        origin = str(literal(truth.feedback.event_id).compile(
            compile_kwargs={"literal_binds": True}
        ))
        review_rows(f"""
            CREATE TRIGGER {reservation_fault_name}
            BEFORE INSERT ON curie.publication_review_reservations
            FOR EACH ROW WHEN (NEW.origin_key = {origin})
            EXECUTE FUNCTION curie.{reservation_fault_name}()
        """)

    def remove_reservation_fault() -> None:
        nonlocal reservation_fault_installed
        if reservation_fault_installed:
            review_rows(
                f"DROP TRIGGER IF EXISTS {reservation_fault_name} "
                "ON curie.publication_review_reservations"
            )
            review_rows(f"DROP FUNCTION IF EXISTS curie.{reservation_fault_name}()")
            reservation_fault_installed = False

    async def exercise() -> None:
        nonlocal permission_status, permission_timeout
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(16)
        api_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        server = uvicorn.Server(
            uvicorn.Config(client.app, lifespan="off", log_level="critical", access_log=False)
        )
        server_task = asyncio.create_task(server.serve(sockets=[sock]))
        engine = create_async_engine(settings.database_url)
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    await asyncio.sleep(0.01)
            async with httpx.AsyncClient() as http:
                publication_client = ApprovalClient(
                    api_base_url=api_url, api_key="", client=http,
                    worker_token="fixture-review-worker-token", read_timeout_s=2.0,
                )
                lineage = (await asyncio.to_thread(
                    review_rows, "SELECT deployment_id FROM curie.thread_publication_lineages"
                ))[0]
                lineage_response = await http.get(
                    f"{api_url}/v1/internal/publications/lineage",
                    params={
                        "deployment_id": str(lineage["deployment_id"]),
                        "conversation_id": thread_key,
                        "repo_full_name": REPO,
                    },
                    headers={"X-Curie-Worker-Token": "fixture-review-worker-token"},
                )
                assert lineage_response.status_code == 200, lineage_response.text
                assert lineage_response.json()["head_sha"] == HEAD
                assert lineage_response.json()["conversation_id"] == thread_key

                def workspace(substrate):
                    return WorkspaceClaimCoordinator(
                        preparer=WorkspacePreparer(
                            credentials=WorkspaceCredentialClient(
                                api_url=api_url, worker_token="fixture-review-worker-token"
                            ),
                            commands=SubprocessCommands(),
                            objects=WorkspaceObjectStore(client=objects_client, bucket=bucket),
                            scratch_root=tmp_path, limits=WorkspaceLimits(),
                        ),
                        substrate=substrate,
                    )

                async with kernel_harness(
                    names, valkey, binding=BindingResolver(engine, make_config(names)),
                    publication_creator=publication_client, workspace_factory=workspace,
                    max_delivery=2, reclaim_min_idle_ms=1,
                ) as h:
                    await asyncio.to_thread(
                        h.substrate.claim, thread_key,
                        env={"CURIE_RUNNER_TOKEN": "example-review-route-token"},
                        workspace_repo=REPO, workspace_materialized_head=HEAD,
                        publication_visible_outcome_revision=1,
                    )
                    consumer = Consumer(
                        redis=h.async_redis, kernel=h.kernel, config=h.config,
                        leases=DeliveryLeaseStore(h.async_redis, h.config),
                    )
                    await consumer.ensure_group()
                    hold = asyncio.Event()
                    h.runner.hold = hold
                    h.runner.default_script = [TextDelta(text="working")]
                    h.runner.tail = [Final(text="original complete", status=SessionStatus.DONE)]

                    async def dispatch_new():
                        rows = await h.async_redis.xreadgroup(
                            h.config.consumer_group, h.config.consumer_name,
                            {stream: ">"}, count=1,
                        )
                        assert rows
                        entry_id, fields = rows[0][1][0]
                        await consumer._dispatch(entry_id, fields)
                        return entry_id, fields

                    async def handler_done(entry_id):
                        async with asyncio.timeout(10):
                            while entry_id in consumer._inflight_ids:
                                await asyncio.sleep(0.01)

                    try:
                        await h.async_redis.xadd(stream, to_stream_fields(original))
                        first_id, _ = await dispatch_new()
                        async with asyncio.timeout(10):
                            while not h.runner.turn_active:
                                assert first_id in consumer._inflight_ids, (
                                    "initial ordinary delivery ended before a model turn opened"
                                )
                                await asyncio.sleep(0.01)
                        ordinary_steer = original.model_copy(update={
                            "event_id": f"ordinary-{uuid.uuid4()}", "text": "Also cover the edge."
                        })
                        await h.async_redis.xadd(stream, to_stream_fields(ordinary_steer))
                        steer_id, _ = await dispatch_new()
                        await handler_done(steer_id)
                        assert h.runner.steers == [ordinary_steer.text]
                        admitted = await asyncio.to_thread(post_review, client, truth)
                        assert admitted.json()["status"] == "feedback_queued", admitted.text
                        review_id, review_fields = await dispatch_new()
                        await handler_done(review_id)
                        assert h.runner.opened == [original.text]
                        assert h.runner.steers == [ordinary_steer.text]
                        assert await asyncio.to_thread(
                            review_rows, "SELECT id FROM curie.publication_review_reservations"
                        ) == []
                        pending = await h.async_redis.xpending_range(
                            stream, h.config.consumer_group, review_id, review_id, 1
                        )
                        assert len(pending) == 1
                        hold.set()
                        await handler_done(first_id)
                        h.runner.hold = None
                        h.runner.tail = []
                        h.runner.default_script = [
                            Final(text="review complete", status=SessionStatus.DONE)
                        ]
                        if permission_case == "revoked":
                            permission["permission"] = "read"
                        elif permission_case == "wrong_id":
                            permission["user"] = {"id": 42}
                        elif permission_case == "bad_login":
                            truth.comment["user"]["login"] = "wrong/path"
                        elif permission_case.startswith("http"):
                            permission_status = int(permission_case[4:].removesuffix("_cap"))
                        elif permission_case == "timeout":
                            permission_timeout = True
                        elif permission_case.startswith("reserve"):
                            await asyncio.to_thread(install_reservation_fault)
                        truth.calls.clear()
                        await consumer._dispatch(review_id, review_fields)
                        await handler_done(review_id)
                        if permission_case in {"revoked", "wrong_id", "bad_login"}:
                            assert h.runner.opened == [original.text]
                            assert h.runner.steers == [ordinary_steer.text]
                            assert await asyncio.to_thread(
                                review_rows, "SELECT id FROM curie.publication_review_reservations"
                            ) == []
                            assert await h.async_redis.xpending_range(
                                stream, h.config.consumer_group, review_id, review_id, 1
                            ) == []
                            reconciler = client.app.state.github_review_reconciler
                            assert await reconciler.reconcile_terminal() == 1
                            code = {
                                "revoked": "sender_permission_refused",
                                "wrong_id": "sender_permission_identity_mismatch",
                                "bad_login": "non_human_sender",
                            }[permission_case]
                            assert await asyncio.to_thread(
                                review_rows,
                                "SELECT status,error_code FROM curie.github_review_feedback",
                            ) == [{"status": "refused", "error_code": code}]
                            return
                        if permission_case in {
                            "http404", "http403", "timeout", "reserve_unavailable",
                            "http403_cap", "reserve_cap",
                        }:
                            assert h.runner.opened == [original.text]
                            assert len(await h.async_redis.xpending_range(
                                stream, h.config.consumer_group, review_id, review_id, 1
                            )) == 1
                            assert await asyncio.to_thread(
                                review_rows, "SELECT id FROM curie.publication_review_reservations"
                            ) == []
                            assert await asyncio.to_thread(
                                review_rows,
                                "SELECT status,error_code FROM curie.github_review_feedback",
                            ) == [{"status": "queued", "error_code": None}]
                            # A transient authority read is not terminal. It is
                            # still bounded by the existing real PEL delivery cap.
                            assert await consumer._dead_letter_over_cap() == set()
                            if permission_case.endswith("_cap"):
                                await h.async_redis.xclaim(
                                    stream, h.config.consumer_group, h.config.consumer_name,
                                    min_idle_time=0, message_ids=[review_id], idle=1000,
                                )
                                assert await consumer._dead_letter_over_cap() == {review_id}
                                assert await h.async_redis.xpending_range(
                                    stream, h.config.consumer_group, review_id, review_id, 1
                                ) == []
                                dead = await h.async_redis.xrange(
                                    h.config.dead_letter_stream_name()
                                )
                                assert len(dead) == 1
                                assert dead[0][1]["dl_original_id"] == review_id
                                assert json.loads(dead[0][1]["payload"]) == json.loads(
                                    review_fields["payload"]
                                )
                                reconciler = client.app.state.github_review_reconciler
                                assert await reconciler.reconcile_terminal() == 1
                                assert await asyncio.to_thread(
                                    review_rows,
                                    "SELECT status,error_code FROM curie.github_review_feedback",
                                ) == [{
                                    "status": "dead_lettered",
                                    "error_code": "delivery_dead_lettered",
                                }]
                                assert h.runner.opened == [original.text]
                                return
                            permission_status, permission_timeout = 200, False
                            await asyncio.to_thread(remove_reservation_fault)
                            truth.calls.clear()
                            await consumer._dispatch(review_id, review_fields)
                            await handler_done(review_id)
                        assert len(h.runner.opened) == 2
                        assert h.runner.opened[1] == json.loads(review_fields["payload"])["text"]
                        assert h.runner.steers == [ordinary_steer.text]
                        assert f"/repos/{REPO}/pulls/17" in truth.calls
                        if permission_case == "legacy":
                            assert permission_path not in truth.calls
                        else:
                            assert permission_path in truth.calls
                        assert await h.async_redis.xpending_range(
                            stream, h.config.consumer_group, review_id, review_id, 1
                        ) == []
                        assert await asyncio.to_thread(
                            review_rows,
                            "SELECT origin_key,status FROM curie.publication_review_reservations",
                        ) == [{"origin_key": truth.feedback.event_id, "status": "reserved"}]
                        reconciler = client.app.state.github_review_reconciler
                        assert await reconciler.reconcile_terminal() == 1
                    finally:
                        hold.set()
                        await asyncio.gather(*list(consumer._inflight), return_exceptions=True)
                        await asyncio.to_thread(remove_reservation_fault)
        finally:
            await engine.dispose()
            server.should_exit = True
            try:
                await asyncio.wait_for(server_task, 5)
            finally:
                if not server_task.done():
                    server_task.cancel()
                    await asyncio.gather(server_task, return_exceptions=True)
                sock.close()

    original_github = client.app.state.http_client
    injected = httpx.AsyncClient(transport=httpx.MockTransport(github_permission))
    client.app.state.http_client = injected
    try:
        client.portal.call(exercise)
    finally:
        client.app.state.http_client = original_github

        def cleanup_sandbox_keys():
            keys = list(valkey.scan_iter(match=f"{stream}:sandbox*"))
            if keys:
                valkey.delete(*keys)

        with ExitStack() as cleanup:
            cleanup.callback(client.portal.call, injected.aclose)
            cleanup.callback(cleanup_sandbox_keys)
            cleanup.callback(objects_client.close)
            cleanup.callback(objects_client.delete_bucket, Bucket=bucket)
            contents = objects_client.list_objects_v2(Bucket=bucket).get("Contents", [])
            for obj in contents:
                cleanup.callback(objects_client.delete_object, Bucket=bucket, Key=obj["Key"])


def test_real_enqueue_refusal_backs_off_then_recovers_without_second_quota(review_stack) -> None:
    import redis.asyncio as redis

    client, truth, valkey, stream = review_stack
    username = f"review-test-{uuid.uuid4().hex}"
    password = "fixture-owned-valkey-acl-token"
    # Only this task-created ACL identity loses XADD. The backing server and
    # other clients remain healthy; no Postgres/Valkey service is mocked.
    valkey.execute_command("ACL", "SETUSER", username, "on", f">{password}", "~*", "+@all", "-xadd")
    restricted = redis.Redis.from_url(
        str(httpx.URL(get_settings().valkey_dsn()).copy_with(username=username, password=password)),
        decode_responses=True,
    )
    reconciler = client.app.state.github_review_reconciler
    original = reconciler._valkey
    reconciler._valkey = restricted
    try:
        assert client.portal.call(restricted.ping) is True
        response = post_review(client, truth)
        assert response.json()["status"] == "feedback_waiting"
        row = review_rows(
            "SELECT enqueue_attempts,quota_taken,next_attempt_at FROM curie.github_review_feedback"
        )[0]
        assert row["enqueue_attempts"] == 1 and row["quota_taken"] is True
        assert row["next_attempt_at"] is not None and valkey.xlen(stream) == 0
        assert client.portal.call(reconciler.reconcile_once) == 0
        assert (
            review_rows("SELECT enqueue_attempts FROM curie.github_review_feedback")[0][
                "enqueue_attempts"
            ]
            == 1
        )
        # Queue several real scans behind one DB connection. They can all read
        # the due candidate before the first locked attempt updates its retry
        # deadline; later locks must recheck that deadline rather than burn it.
        review_rows("UPDATE curie.github_review_feedback SET next_attempt_at=NULL")

        async def competing_passes() -> list[int]:
            from curie_api.github_review_store import GitHubReviewReconciler
            from sqlalchemy.ext.asyncio import async_sessionmaker

            engine = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
            try:
                contender = GitHubReviewReconciler(
                    async_sessionmaker(engine, expire_on_commit=False), restricted, get_settings()
                )
                return await asyncio.gather(*(contender.reconcile_once() for _ in range(4)))
            finally:
                await engine.dispose()

        assert client.portal.call(competing_passes) == [0, 0, 0, 0]
        assert (
            review_rows("SELECT enqueue_attempts FROM curie.github_review_feedback")[0][
                "enqueue_attempts"
            ]
            == 2
        )
        valkey.execute_command("ACL", "SETUSER", username, "+xadd")
        review_rows(
            "UPDATE curie.github_review_feedback SET next_attempt_at=now()-interval '1 second'"
        )
        assert client.portal.call(reconciler.reconcile_once) == 1
        assert valkey.xlen(stream) == 1
        assert post_review(client, truth).json()["status"] == "feedback_duplicate"
        assert valkey.xlen(stream) == 1
    finally:
        reconciler._valkey = original
        client.portal.call(restricted.aclose)
        valkey.execute_command("ACL", "DELUSER", username)


def test_signed_feedback_with_wrong_current_head_cannot_create_a_turn(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    truth.pr["head"]["sha"] = "b" * 40
    response = post_review(client, truth)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "feedback_ignored"
    assert response.json()["errors"] == [{"code": "stale_feedback_head"}]
    assert valkey.xlen(stream) == 0
    assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []


def test_before_model_revalidation_rejects_edit_and_never_accepts_platform_key(
    review_stack,
) -> None:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = json.loads(valkey.xrange(stream)[0][1]["payload"])
    deployment = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0]
    payload = {"turn": turn, "deployment_id": str(deployment["deployment_id"])}
    path = f"/v1/internal/github/reviews/{truth.feedback.event_id}/verify"
    unauthenticated = client.post(path, json=payload, headers={"X-API-Key": get_settings().api_key})
    assert unauthenticated.status_code == 401
    headers = {"X-Curie-Worker-Token": "fixture-review-worker-token"}
    verified = client.post(path, json=payload, headers=headers)
    assert verified.status_code == 200, verified.text
    assert verified.json()["head_sha"] == HEAD
    assert verified.json()["sender"] == "github:41:example-reviewer"
    truth.comment["body"] = "edited-private-sentinel"
    rejected = client.post(path, json=payload, headers=headers)
    assert rejected.status_code == 409
    assert "private-sentinel" not in rejected.text
    assert valkey.xlen(stream) == 1


def test_production_worker_client_uses_actual_api_without_github_or_slack_authority(
    review_stack,
) -> None:
    from aci_protocol import parse_queued_turn
    from curie_worker.approvals import ApprovalBackendError, ApprovalClient

    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = parse_queued_turn(valkey.xrange(stream)[0][1]["payload"])
    deployment_id = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0][
        "deployment_id"
    ]

    async def exercise() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=client.app),
            base_url="http://api.example.test",
        ) as http:
            worker = ApprovalClient(
                api_base_url="http://api.example.test",
                api_key="",
                client=http,
                read_timeout_s=2,
                worker_token="fixture-review-worker-token",
            )
            verified = await worker.verify_review_feedback(turn, deployment_id)
            assert verified.head_sha == HEAD
            assert verified.sender == "github:41:example-reviewer"
            assert truth.feedback.url in verified.receipt
            assert verified.origin_key == turn.event_id
            assert verified.reservation_id is None
            reserved = await worker.reserve_review_feedback(turn, deployment_id, verified)
            assert reserved == await worker.reserve_review_feedback(turn, deployment_id, verified)
            assert await asyncio.to_thread(
                review_rows, "SELECT origin_key FROM curie.publication_review_reservations"
            ) == [
                {"origin_key": turn.event_id}
            ]
            truth.feedback_status = 404
            with pytest.raises(ApprovalBackendError):
                await worker.verify_review_feedback(turn, deployment_id)

    client.portal.call(exercise)


def test_review_verification_uses_its_own_budget_over_actual_api_http(review_stack) -> None:
    """The production card-read budget is too short for fresh GitHub reads.

    Uvicorn serves the actual API with real Postgres/Valkey; only external GitHub
    responses are controlled. ASGITransport does not enforce HTTP timeouts, so
    this regression deliberately uses a loopback socket and a delayed provider.
    """
    import uvicorn
    from aci_protocol import parse_queued_turn
    from curie_worker.approvals import ApprovalBackendError, ApprovalClient

    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = parse_queued_turn(valkey.xrange(stream)[0][1]["payload"])
    deployment_id = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0][
        "deployment_id"
    ]

    async def exercise() -> None:
        async def delayed_github(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/comments/71"):
                await asyncio.sleep(2.2)
            return truth.handle(request)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(16)
        server = uvicorn.Server(
            uvicorn.Config(client.app, lifespan="off", log_level="critical", access_log=False)
        )
        task = asyncio.create_task(server.serve(sockets=[sock]))
        previous = client.app.state.http_client
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    await asyncio.sleep(0.01)
            async with (
                httpx.AsyncClient(transport=httpx.MockTransport(delayed_github)) as github,
                httpx.AsyncClient() as http,
            ):
                client.app.state.http_client = github
                worker = ApprovalClient(
                    api_base_url=f"http://127.0.0.1:{sock.getsockname()[1]}",
                    api_key="",
                    client=http,
                    read_timeout_s=2.0,
                    worker_token="fixture-review-worker-token",
                )
                assert (await worker.verify_review_feedback(turn, deployment_id)).head_sha == HEAD
                # A mixed-token rollout is infrastructure unavailability and
                # cannot become a terminal policy refusal that ACKs the turn.
                wrong_token = ApprovalClient(
                    api_base_url=f"http://127.0.0.1:{sock.getsockname()[1]}",
                    api_key="",
                    client=http,
                    read_timeout_s=2.0,
                    worker_token="fixture-old-worker-token",
                )
                with pytest.raises(ApprovalBackendError):
                    await wrong_token.verify_review_feedback(turn, deployment_id)
        finally:
            client.app.state.http_client = previous
            server.should_exit = True
            try:
                await asyncio.wait_for(task, 5)
            finally:
                sock.close()

    client.portal.call(exercise)


def test_real_queue_receipt_survives_database_commit_failure_without_second_turn(
    review_stack,
) -> None:
    from sqlalchemy.exc import DBAPIError

    client, truth, valkey, stream = review_stack
    # Task-owned disposable DB fault: the real outbox XADD succeeds, then the
    # real transaction fails while flushing its queued mark. No service mocked.
    review_rows("""
        CREATE FUNCTION curie.review_test_reject_queued() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.status = 'queued' THEN RAISE EXCEPTION 'task-owned queued-mark failure'; END IF;
          RETURN NEW;
        END $$
    """)
    review_rows("""
        CREATE TRIGGER review_test_reject_queued BEFORE UPDATE ON curie.github_review_feedback
        FOR EACH ROW EXECUTE FUNCTION curie.review_test_reject_queued()
    """)
    try:
        with pytest.raises(DBAPIError):
            post_review(client, truth)
        assert valkey.xlen(stream) == 1
        assert review_rows("SELECT status, version FROM curie.github_review_feedback") == [
            {"status": "waiting", "version": 1}
        ]
    finally:
        review_rows("DROP TRIGGER review_test_reject_queued ON curie.github_review_feedback")
        review_rows("DROP FUNCTION curie.review_test_reject_queued()")
    assert client.portal.call(client.app.state.github_review_reconciler.reconcile_once) == 1
    assert client.portal.call(client.app.state.github_review_reconciler.reconcile_once) == 0
    assert valkey.xlen(stream) == 1
    assert review_rows("SELECT status, version FROM curie.github_review_feedback") == [
        {"status": "queued", "version": 2}
    ]


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("UPDATE curie.agent_channels SET generation=generation+1",
         "binding_no_longer_authorized"),
        ("UPDATE curie.thread_publication_lineages SET version=version+1",
         "binding_or_lineage_changed"),
    ],
)
def test_before_model_recheck_refuses_stale_binding_or_lineage_version(
    review_stack, mutation, code
) -> None:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = json.loads(valkey.xrange(stream)[0][1]["payload"])
    deployment_id = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0][
        "deployment_id"
    ]
    review_rows(mutation)
    response = client.post(
        f"/v1/internal/github/reviews/{truth.feedback.event_id}/verify",
        json={"turn": turn, "deployment_id": str(deployment_id)},
        headers={"X-Curie-Worker-Token": "fixture-review-worker-token"},
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == code
    assert valkey.xlen(stream) == 1


@pytest.mark.parametrize(
    ("holder_kind", "mutation", "refusal"),
    [
        ("delivery", None, None),
        ("mutator", None, None),
        ("delivery", "UPDATE curie.agent_channels SET generation=generation+1",
         "binding_no_longer_authorized"),
        ("delivery", "UPDATE curie.thread_publication_lineages SET version=version+1",
         "binding_or_lineage_changed"),
    ],
    ids=["audit-reference", "competing-mutator", "stale-binding", "stale-lineage"],
)
def test_review_outbox_recovers_under_delivery_reference_without_racing_mutators(
    review_stack, holder_kind, mutation, refusal
) -> None:
    client, truth, valkey, stream = review_stack
    # Admit through the real signed HTTP route while the fixture stream has
    # the wrong type. Actual XADD fails, leaving the committed outbox eligible
    # for recovery after removing this task-owned fault; neither store is mocked.
    valkey.set(stream, "fixture-wrong-stream-type")
    response = post_review(client, truth)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "feedback_waiting"
    assert review_rows("SELECT status,error_code FROM curie.github_review_feedback") == [
        {"status": "waiting", "error_code": "enqueue_unavailable"}
    ]
    assert valkey.get(stream) == "fixture-wrong-stream-type"
    valkey.delete(stream)
    review_rows("UPDATE curie.github_review_feedback SET next_attempt_at=NULL")
    if mutation is not None:
        review_rows(mutation)
    event_id = truth.feedback.event_id
    reconciler = client.app.state.github_review_reconciler

    async def recover_with_holder() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as holder, holder.begin() as transaction:
                if holder_kind == "delivery":
                    # A real child FK reference holds KEY SHARE on its parent
                    # until commit, just like settling a concurrent audit row.
                    # It must not prevent the outbox's non-key state update.
                    await holder.execute(text("""
                        INSERT INTO curie.github_review_deliveries (
                          delivery_id,event_kind,action,body_sha256,sender_type,
                          author_association,status,event_id
                        ) SELECT :delivery,event_kind,action,body_sha256,sender_type,
                          author_association,status,event_id
                          FROM curie.github_review_deliveries WHERE delivery_id=:original
                    """), {"delivery": uuid.uuid4(), "original": uuid.UUID(DELIVERY)})
                    assert await holder.scalar(text(
                        "SELECT count(*) FROM curie.github_review_deliveries WHERE event_id=:event"
                    ), {"event": event_id}) == 2
                else:
                    assert await holder.scalar(text(
                        "SELECT event_id FROM curie.github_review_feedback "
                        "WHERE event_id=:event FOR NO KEY UPDATE"
                    ), {"event": event_id}) == event_id
                async with asyncio.timeout(5):
                    enqueued = await reconciler.reconcile_once(event_id)
                if holder_kind == "mutator":
                    assert enqueued == 0
                    assert valkey.xlen(stream) == 0
                    assert await holder.scalar(text(
                        "SELECT status FROM curie.github_review_feedback WHERE event_id=:event"
                    ), {"event": event_id}) == "waiting"
                else:
                    assert enqueued == (0 if refusal else 1)
                    assert await holder.scalar(text(
                        "SELECT status FROM curie.github_review_feedback WHERE event_id=:event"
                    ), {"event": event_id}) == ("refused" if refusal else "queued")
                # Explicit release is part of the control: a real competing
                # mutator must block this pass, then allow the next one through.
                await transaction.rollback()
            assert await reconciler.reconcile_once(event_id) == (
                1 if holder_kind == "mutator" else 0
            )
        finally:
            await engine.dispose()

    client.portal.call(recover_with_holder)
    rows = review_rows(
        "SELECT event_id,status,error_code,stream_id FROM curie.github_review_feedback"
    )
    assert len(rows) == 1
    assert rows[0]["event_id"] == event_id
    assert rows[0]["status"] == ("refused" if refusal else "queued")
    assert rows[0]["error_code"] == refusal
    assert valkey.xlen(stream) == (0 if refusal else 1)
    if refusal:
        assert rows[0]["stream_id"] is None
    else:
        receipt, fields = valkey.xrange(stream)[0]
        assert rows[0]["stream_id"] == receipt
        assert json.loads(fields["payload"])["event_id"] == event_id
        assert post_review(client, truth).json()["status"] == "feedback_duplicate"
        assert valkey.xlen(stream) == 1


def test_concurrent_distinct_delivery_headers_for_one_feedback_enqueue_once(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    barrier = threading.Barrier(4)

    def deliver(index: int) -> tuple[int, str]:
        barrier.wait(timeout=10)
        response = post_review(client, truth, delivery=str(uuid.UUID(int=100 + index)))
        return response.status_code, response.json()["status"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(deliver, range(4)))
    assert sorted(results) == [(200, "feedback_duplicate")] * 3 + [(200, "feedback_queued")]
    assert valkey.xlen(stream) == 1
    assert len(review_rows("SELECT event_id FROM curie.github_review_feedback")) == 1


def test_legacy_name_only_lineage_cannot_override_verified_github_owner(
    review_stack,
) -> None:
    client, truth, valkey, stream = review_stack
    review_rows(
        """
        INSERT INTO curie.thread_publication_lineages (
          id,agent_id,deployment_id,conversation_id,repo_full_name,base_sha,branch,
          pr_number,pr_url,head_sha,status,version,latest_revision
        ) SELECT :id,agent_id,deployment_id,'other-conversation',repo_full_name,base_sha,
          'curie/other-conversation',pr_number,pr_url,head_sha,status,version,latest_revision
          FROM curie.thread_publication_lineages
    """,
        {"id": uuid.uuid4()},
    )
    response = post_review(client, truth)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "feedback_queued", response.text
    queued = json.loads(valkey.xrange(stream)[0][1]["payload"])
    assert queued["conversation_id"] == "1700000000.000001"
    assert valkey.xlen(stream) == 1
    # A name/number-only historical row cannot become review authority when the
    # verified owner is no longer open. Keep the valid queued receipt untouched.
    review_rows("UPDATE curie.thread_publication_lineages SET status='closed' "
                "WHERE github_repository_id IS NOT NULL")
    truth.payload["comment"].update(
        id=72, html_url=f"https://github.com/{REPO}/pull/17#issuecomment-72"
    )
    truth.comment.update(truth.payload["comment"])
    truth.calls.clear()
    refused = post_review(client, truth, delivery=str(uuid.uuid4()))
    assert refused.json()["errors"] == [{"code": "lineage_absent_or_ambiguous"}]
    assert truth.calls == []
    assert valkey.xlen(stream) == 1


def test_forged_slack_principal_on_queued_github_feedback_is_refused_by_actual_api(
    review_stack,
) -> None:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = json.loads(valkey.xrange(stream)[0][1]["payload"])
    turn["author"] = "U0REQUEST1"
    deployment_id = review_rows("SELECT deployment_id FROM curie.thread_publication_lineages")[0][
        "deployment_id"
    ]
    response = client.post(
        f"/v1/internal/github/reviews/{truth.feedback.event_id}/verify",
        json={"turn": turn, "deployment_id": str(deployment_id)},
        headers={"X-Curie-Worker-Token": "fixture-review-worker-token"},
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "feedback_turn_mismatch"
    assert valkey.xlen(stream) == 1


@pytest.mark.parametrize("action", ["edited", "deleted"])
def test_signed_unactionable_review_is_durably_audited_without_effects(
    review_stack, action
) -> None:
    client, truth, valkey, stream = review_stack
    truth.payload["action"] = action
    truth.payload["comment"]["body"] = "AUDIT_PRIVATE_BODY_SENTINEL"
    first = post_review(client, truth)
    assert first.status_code == 200 and first.json()["status"] == "feedback_ignored"
    assert post_review(client, truth).json() == first.json()
    audits = review_rows(
        "SELECT delivery_id,event_kind,action,status,reason,body_sha256,version "
        "FROM curie.github_review_deliveries"
    )
    assert len(audits) == 1 and audits[0]["status"] == "ignored"
    assert audits[0]["event_kind"] == "issue_comment" and audits[0]["action"] == action
    assert (
        audits[0]["body_sha256"] == hashlib.sha256(json.dumps(truth.payload).encode()).hexdigest()
    )
    assert "AUDIT_PRIVATE_BODY_SENTINEL" not in str(audits)
    assert truth.calls == [] and valkey.xlen(stream) == 0
    assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []


def test_reused_delivery_bytes_conflict_even_after_semantic_alias(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    first = post_review(client, truth)
    assert first.json()["status"] == "feedback_queued"
    alias = str(uuid.uuid4())
    assert post_review(client, truth, delivery=alias).json()["status"] == "feedback_duplicate"
    truth.payload["comment"]["body"] = "Changed bytes under a previously adopted delivery alias"
    for delivery in (DELIVERY, alias):
        refused = post_review(client, truth, delivery=delivery)
        assert refused.status_code == 200
        assert refused.json()["errors"] == [{"code": "delivery_identity_conflict"}]
    assert valkey.xlen(stream) == 1
    audits = review_rows(
        "SELECT status,replay_conflicts,version FROM curie.github_review_deliveries "
        "ORDER BY delivery_id"
    )
    assert len(audits) == 2
    assert all(row == {"status": "accepted", "replay_conflicts": 1, "version": 3} for row in audits)


def test_ignored_delivery_cannot_be_promoted_by_changed_signed_bytes(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    truth.payload["action"] = "edited"
    assert post_review(client, truth).json()["status"] == "feedback_ignored"
    truth.payload["action"] = "created"
    refused = post_review(client, truth)
    assert refused.json()["errors"] == [{"code": "delivery_identity_conflict"}]
    assert truth.calls == [] and valkey.xlen(stream) == 0
    assert review_rows("SELECT status,replay_conflicts FROM curie.github_review_deliveries") == [
        {"status": "ignored", "replay_conflicts": 1}
    ]


def test_invalid_hmac_and_missing_delivery_header_create_no_audit(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth, signature="sha256=invalid").status_code == 401
    assert post_review(client, truth, delivery="").status_code == 400
    assert truth.calls == [] and valkey.xlen(stream) == 0
    assert review_rows("SELECT delivery_id FROM curie.github_review_deliveries") == []


def test_repository_id_cannot_overflow_a_durable_receipt() -> None:
    payload = feedback_payload()
    payload["repository"]["id"] = 2**63
    with pytest.raises(FeedbackIgnored):
        parse_feedback("issue_comment", payload, DELIVERY)


def test_concurrent_conflicting_delivery_replays_preserve_canonical_receipt(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    original = review_rows("SELECT body_sha256 FROM curie.github_review_deliveries")[0][
        "body_sha256"
    ]
    truth.payload["comment"]["body"] = "Changed signed replay, never another instruction"
    with ThreadPoolExecutor(max_workers=4) as pool:
        replies = list(pool.map(lambda _: post_review(client, truth), range(4)))
    assert all(
        reply.json()["errors"] == [{"code": "delivery_identity_conflict"}] for reply in replies
    )
    assert review_rows(
        "SELECT body_sha256,replay_conflicts,version FROM curie.github_review_deliveries"
    ) == [{"body_sha256": original, "replay_conflicts": 4, "version": 6}]
    assert valkey.xlen(stream) == 1


def test_retryable_github_read_is_audited_and_explicit_redelivery_recovers(review_stack) -> None:
    client, truth, valkey, stream = review_stack
    truth.feedback_status = 503
    unavailable = post_review(client, truth)
    assert unavailable.status_code == 503
    assert review_rows("SELECT status,reason FROM curie.github_review_deliveries") == [
        {"status": "retryable", "reason": "feedback_unavailable"}
    ]
    assert valkey.xlen(stream) == 0
    assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []
    # GitHub does not automatically redeliver failed webhooks. This explicitly
    # drives the authenticated redelivery surface, not an assumed background retry.
    # https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries
    truth.feedback_status = 200
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    assert post_review(client, truth).json()["status"] == "feedback_duplicate"
    assert valkey.xlen(stream) == 1
    assert review_rows("SELECT status,reason,version FROM curie.github_review_deliveries") == [
        {"status": "accepted", "reason": None, "version": 3}
    ]


def test_receipt_and_feedback_admission_commit_atomically(review_stack) -> None:
    from sqlalchemy.exc import DBAPIError

    client, truth, valkey, stream = review_stack
    review_rows("""
        CREATE FUNCTION curie.review_test_reject_admission() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.status = 'accepted' THEN
            RAISE EXCEPTION 'task-owned admission-mark failure';
          END IF;
          RETURN NEW;
        END $$
    """)
    review_rows("""
        CREATE TRIGGER review_test_reject_admission BEFORE UPDATE ON curie.github_review_deliveries
        FOR EACH ROW EXECUTE FUNCTION curie.review_test_reject_admission()
    """)
    try:
        with pytest.raises(DBAPIError):
            post_review(client, truth)
        assert review_rows("SELECT event_id FROM curie.github_review_feedback") == []
        assert review_rows("SELECT delivery_id FROM curie.github_review_deliveries") == []
        assert valkey.xlen(stream) == 0
    finally:
        review_rows("DROP TRIGGER review_test_reject_admission ON curie.github_review_deliveries")
        review_rows("DROP FUNCTION curie.review_test_reject_admission()")
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    assert valkey.xlen(stream) == 1


def _review_reserve_request(review_stack) -> tuple[str, dict, dict]:
    client, truth, valkey, stream = review_stack
    assert post_review(client, truth).json()["status"] == "feedback_queued"
    turn = json.loads(valkey.xrange(stream)[0][1]["payload"])
    lineage = review_rows("SELECT deployment_id,version FROM curie.thread_publication_lineages")[0]
    headers = {"X-Curie-Worker-Token": "fixture-review-worker-token"}
    payload = {"turn": turn, "deployment_id": str(lineage["deployment_id"])}
    base = f"/v1/internal/github/reviews/{turn['event_id']}"
    verified = client.post(base + "/verify", json=payload, headers=headers)
    assert verified.status_code == 200, verified.text
    assert review_rows("SELECT id FROM curie.publication_review_reservations") == []
    payload.update(expected_lineage_version=lineage["version"], expected_head_sha=HEAD)
    return base, payload, headers


@pytest.mark.parametrize("mutation", ["author", "head", "version", "token"])
def test_review_reservation_rejects_unverified_origin_without_effects(review_stack, mutation):
    client, _, valkey, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    if mutation == "author":
        payload["turn"]["author"] = "U0REQUEST1"
    elif mutation == "head":
        payload["expected_head_sha"] = "d" * 40
    elif mutation == "version":
        payload["expected_lineage_version"] += 1
    else:
        headers = {"X-Curie-Worker-Token": "example-wrong-worker-token"}
    result = client.post(base + "/reserve", json=payload, headers=headers)
    assert result.status_code == (401 if mutation == "token" else 409), result.text
    assert review_rows("SELECT id FROM curie.publication_review_reservations") == []
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [{"status": "queued"}]
    assert valkey.xlen(stream) == 1


def test_review_reservation_is_one_atomic_origin_after_concurrent_replays(review_stack):
    client, _, valkey, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(
            lambda _: client.post(base + "/reserve", json=payload, headers=headers), range(4)
        ))
    assert all(response.status_code == 200 for response in responses)
    ids = {response.json()["reservation_id"] for response in responses}
    assert len(ids) == 1
    rows = review_rows("SELECT origin_key,status FROM curie.publication_review_reservations")
    assert rows == [{"origin_key": payload["turn"]["event_id"], "status": "reserved"}]
    assert review_rows("SELECT status,version FROM curie.github_review_feedback") == [
        {"status": "reserved", "version": 3}
    ]
    assert valkey.xlen(stream) == 1


def _write_actual_fenced_review_terminal(client, event_id: str, stream: str) -> None:
    from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget, TurnCompleted
    from curie_worker.config import WorkerConfig
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from curie_worker.markers import CompletionRecord, Markers
    from curie_worker.reply_sink import TargetRoute
    from redis.exceptions import ResponseError

    async def settle() -> None:
        valkey = client.app.state.github_review_reconciler._valkey
        config = WorkerConfig(key_prefix=get_settings().worker_key_prefix)
        group = "review-observer-test"
        try:
            await valkey.xgroup_create(stream, group, id="0")
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise
        pending = await valkey.xreadgroup(group, "fixture", {stream: "0"}, count=1)
        if not pending or not pending[0][1]:
            pending = await valkey.xreadgroup(group, "fixture", {stream: ">"}, count=1)
        assert len(pending[0][1]) == 1
        entry_id = pending[0][1][0][0]
        store = DeliveryLeaseStore(valkey, config)
        lease = await store.acquire(stream, group, entry_id, consumer="fixture")
        try:
            assert await Markers(valkey, config).settle_fenced(
                event_id,
                CompletionRecord(
                    event_id=event_id,
                    event=TurnCompleted(
                        version=REPLY_WIRE_VERSION,
                        event="turn.completed",
                        target=ReplyTarget(
                            kind="slack", address="C0EXAMPLE1",
                            conversation_id="1700000000.000001",
                            reply_ref=None,
                        ),
                        event_id=event_id,
                        outcome="delivered",
                    ),
                    route=TargetRoute(),
                    created_at=time.time(),
                ),
                stream=stream,
                group=lease.group,
                entry_id=entry_id,
                owner=lease.owner,
                generation=lease.generation,
            ) is not None
        finally:
            await store.release(stream, lease.group, entry_id, owner=lease.owner)

    client.portal.call(settle)


@pytest.mark.parametrize("reserve", [False, True])
def test_review_cleanup_requires_own_fenced_terminal_and_keeps_origin_tombstone(
    review_stack, reserve
) -> None:
    client, truth, _, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    if reserve:
        response = client.post(base + "/reserve", json=payload, headers=headers)
        assert response.status_code == 200, response.text
    reconciler = client.app.state.github_review_reconciler
    assert client.portal.call(reconciler.reconcile_terminal) == 0
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "reserved" if reserve else "queued"}
    ]
    # A different event's real fenced marker cannot settle this origin.
    _write_actual_fenced_review_terminal(client, truth.feedback.event_id + "-other", stream)
    assert client.portal.call(reconciler.reconcile_terminal) == 0
    _write_actual_fenced_review_terminal(client, truth.feedback.event_id, stream)
    assert client.portal.call(reconciler.reconcile_terminal) == 1
    assert client.portal.call(reconciler.reconcile_terminal) == 0
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [{"status": "settled"}]
    if reserve:
        assert review_rows("SELECT status FROM curie.publication_review_reservations") == [
            {"status": "cancelled"}
        ]
    replay = client.post(base + "/reserve", json=payload, headers=headers)
    assert replay.status_code == 409
    assert replay.json()["detail"]["code"] == "feedback_not_executable"
    assert post_review(client, truth).json()["status"] == "feedback_duplicate"


@pytest.mark.parametrize("trim", [False, True])
def test_review_graveyard_cursor_survives_restart_and_never_treats_absence_as_terminal(
    review_stack, trim,
) -> None:
    from curie_api.github_review_store import GitHubReviewReconciler

    client, truth, valkey, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    assert client.post(base + "/reserve", json=payload, headers=headers).status_code == 200
    reconciler = client.app.state.github_review_reconciler
    dead = get_settings().dead_letter_stream_name()
    original_id = valkey.xrange(stream)[0][0]
    # Every irrelevant row is valid but names another original delivery; a
    # later matching original ID with a different canonical turn also refuses.
    for _ in range(129):
        valkey.xadd(dead, {"dl_original_id": "1-0", "payload": json.dumps(payload["turn"])})
    wrong_turn = dict(payload["turn"], author="U0REQUEST1")
    valkey.xadd(dead, {"dl_original_id": original_id, "payload": json.dumps(wrong_turn)})
    valkey.xadd(dead, {"dl_original_id": original_id, "payload": json.dumps(payload["turn"])})
    assert client.portal.call(reconciler.reconcile_terminal) == 0
    cursor = review_rows("SELECT terminal_scan_cursor FROM curie.github_review_feedback")[0][
        "terminal_scan_cursor"
    ]
    assert cursor == valkey.xrange(dead, count=128)[-1][0]
    restarted = GitHubReviewReconciler(
        reconciler._sessionmaker, reconciler._valkey, reconciler._settings
    )
    if not trim:
        assert client.portal.call(restarted.reconcile_terminal) == 1
        assert review_rows("SELECT status FROM curie.github_review_feedback") == [
            {"status": "dead_lettered"}
        ]
        return
    # A trimmed graveyard or missing original stream entry is unknown; neither
    # permits cancelling the one reserved revision.
    valkey.xtrim(dead, maxlen=0, approximate=False)
    valkey.xdel(stream, original_id)
    assert client.portal.call(restarted.reconcile_terminal) == 0
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "reserved"}
    ]
    assert review_rows("SELECT status FROM curie.publication_review_reservations") == [
        {"status": "reserved"}
    ]


def test_actual_consumer_dead_letter_settles_only_the_matching_review_origin(review_stack):
    from curie_worker.consumer import Consumer
    from curie_worker.delivery_lease import DeliveryLeaseStore

    from apps.worker.tests.kernel.conftest import kernel_harness

    client, truth, valkey, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    assert client.post(base + "/reserve", json=payload, headers=headers).status_code == 200
    names = {"stream": stream, "group": f"{stream}:dead-group",
             "prefix": get_settings().worker_key_prefix, "sandbox_prefix": f"{stream}:sandbox"}

    async def exercise():
        async with kernel_harness(
            names, valkey, max_delivery=2, reclaim_min_idle_ms=1,
            delivery_lease_ttl_s=1.0, delivery_lease_heartbeat_s=0.3,
        ) as h:
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases
            )
            # The running consumer group predates this queued event. Production
            # ensure_group intentionally skips historical backlog on first boot.
            await h.async_redis.xgroup_create(stream, h.config.consumer_group, id="0")
            await consumer.ensure_group()
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {stream: ">"}, count=1
            )
            entry_id, fields = rows[0][1][0]
            await h.async_redis.xclaim(
                stream, h.config.consumer_group, h.config.consumer_name,
                min_idle_time=0, message_ids=[entry_id], idle=1000,
            )
            async with consumer._delivery_lease(entry_id, fields) as stale:
                assert stale is not None
                assert await leases.release(
                    stream, h.config.consumer_group, entry_id, owner=stale.owner
                )
                await h.async_redis.xclaim(
                    stream, h.config.consumer_group, "replacement",
                    min_idle_time=0, message_ids=[entry_id],
                )
                current = await leases.acquire(
                    stream, h.config.consumer_group, entry_id, consumer="replacement"
                )
                try:
                    assert current.generation == stale.generation + 1
                    await asyncio.wait_for(stale.lost.wait(), 2)
                    await consumer._dead_letter(
                        entry_id, fields, reason="max-delivery", delivery_count=2
                    )
                    assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0
                    assert await client.app.state.github_review_reconciler.reconcile_terminal() == 0
                finally:
                    await leases.release(
                        stream, h.config.consumer_group, entry_id, owner=current.owner
                    )
            await h.async_redis.xclaim(
                stream, h.config.consumer_group, h.config.consumer_name,
                min_idle_time=0, message_ids=[entry_id], idle=1000,
            )
            assert await consumer._dead_letter_over_cap() == {entry_id}
            assert await h.async_redis.xpending_range(
                stream, h.config.consumer_group, entry_id, entry_id, 1
            ) == []
            assert await client.app.state.github_review_reconciler.reconcile_terminal() == 1
            assert h.runner.opened == []

    client.portal.call(exercise)
    assert review_rows("SELECT status,error_code FROM curie.github_review_feedback") == [
        {"status": "dead_lettered", "error_code": "delivery_dead_lettered"}
    ]
    assert review_rows("SELECT origin_key,status FROM curie.publication_review_reservations") == [
        {"origin_key": truth.feedback.event_id, "status": "cancelled"}
    ]


def test_concurrent_review_observers_settle_once_and_malformed_match_keeps_cursor(review_stack):
    from curie_api.github_review_store import GitHubReviewReconciler

    client, _, valkey, stream = review_stack
    _, payload, _ = _review_reserve_request(review_stack)
    reconciler = client.app.state.github_review_reconciler
    dead = get_settings().dead_letter_stream_name()
    original_id = valkey.xrange(stream)[0][0]
    valkey.xadd(dead, {"dl_original_id": "1-0", "payload": "irrelevant"})
    malformed = valkey.xadd(dead, {"dl_original_id": original_id, "payload": "broken-json"})
    with pytest.raises(ValueError, match="unreadable"):
        client.portal.call(reconciler.reconcile_terminal)
    assert review_rows("SELECT terminal_scan_cursor,status FROM curie.github_review_feedback") == [
        {"terminal_scan_cursor": None, "status": "queued"}
    ]
    valkey.xdel(dead, malformed)
    valkey.xadd(dead, {"dl_original_id": original_id, "payload": json.dumps(payload["turn"])})

    async def competing():
        observers = [GitHubReviewReconciler(
            reconciler._sessionmaker, reconciler._valkey, reconciler._settings
        ) for _ in range(4)]
        return await asyncio.gather(*(observer.reconcile_terminal() for observer in observers))

    assert sum(client.portal.call(competing)) == 1
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "dead_lettered"}
    ]


@pytest.mark.parametrize("stale_verifier", [False, True])
def test_review_terminal_observer_never_cancels_consumed_publication_or_reuses_approval(
    review_stack, stale_verifier,
):
    client, truth, _, stream = review_stack
    base, payload, headers = _review_reserve_request(review_stack)
    reserved = client.post(base + "/reserve", json=payload, headers=headers)
    assert reserved.status_code == 200, reserved.text
    original_approvals = review_rows("SELECT id FROM curie.approvals")
    publication = client.post("/v1/internal/publications", headers=headers, json={
        "deployment_id": payload["deployment_id"],
        "conversation_id": scoped_conversation_id(
            "slack", "C0EXAMPLE1", payload["turn"]["conversation_id"]
        ),
        "repo_full_name": REPO, "author": payload["turn"]["author"],
        "summary": "Review revision fixture", "reply_kind": "slack",
        "reply_channel": "C0EXAMPLE1",
        "reply_conversation_id": payload["turn"]["conversation_id"],
        "reply_placeholder": "1700000000.000003",
        "dedupe_key": f"review-publication-{uuid.uuid4()}",
        "review_origin_key": truth.feedback.event_id,
        "base_sha": HEAD, "patch_b64": base64.b64encode(b"diff --git a/a b/a\n").decode(),
        "changed_paths": ["a"], "expires_in_seconds": 600,
    })
    assert publication.status_code == 201, publication.text
    assert publication.json()["id"] == reserved.json()["reservation_id"]
    assert publication.json()["approval_id"] not in {str(row["id"]) for row in original_approvals}
    if stale_verifier:
        refused = client.post(base + "/verify", json={
            "turn": payload["turn"], "deployment_id": payload["deployment_id"],
        }, headers=headers)
        assert refused.status_code == 409
        assert refused.json()["detail"]["code"] == "feedback_revision_not_executable"
    _write_actual_fenced_review_terminal(client, truth.feedback.event_id, stream)
    assert client.portal.call(client.app.state.github_review_reconciler.reconcile_terminal) == 1
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "settled"}
    ]
    assert review_rows("SELECT status FROM curie.publication_review_reservations") == [
        {"status": "consumed"}
    ]
    assert review_rows("SELECT status FROM curie.publications WHERE id=:id", {
        "id": publication.json()["id"]
    }) == [{"status": "pending"}]
    assert review_rows("SELECT status FROM curie.approvals WHERE id=:id", {
        "id": publication.json()["approval_id"]
    }) == [{"status": "pending"}]
