"""App-retrieved effective permission is independent of comment association."""

import asyncio
from dataclasses import replace

import httpx
import pytest
from curie_api.github_app import _RESOLVERS
from curie_api.github_review_events import FeedbackIgnored, FeedbackUnavailable, parse_feedback
from curie_api.github_review_truth import verify_feedback_truth

from apps.api.tests.test_github_review_events import (
    DELIVERY,
    HEAD,
    REPO,
    GitHubTruth,
    feedback_payload,
    verify_truth,
)
from apps.api.tests.test_github_review_events import review_app_key as review_app_key

PERMISSION_PATH = f"/repos/{REPO}/collaborators/example-reviewer/permission"


class PermissionTruth(GitHubTruth):
    def __init__(self, event, key, association="CONTRIBUTOR"):
        super().__init__(event, key)
        source = "review" if event == "pull_request_review" else "comment"
        self.payload[source]["author_association"] = association
        self.comment["author_association"] = association
        self.feedback = replace(self.feedback, author_association=association)
        # GitHub's documented permission endpoint returns effective legacy
        # permission separately from role_name; maintain maps to write.
        # https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
        self.permission = {
            "permission": "admin", "role_name": "admin",
            "user": {"id": 41, "login": "example-reviewer", "type": "User"},
        }
        self.permission_status = 200
        self.permission_timeout = False

    def handle(self, request):
        if request.url.path != PERMISSION_PATH:
            return super().handle(request)
        self.calls.append(request.url.path)
        assert request.headers["Authorization"] == "Bearer fixture-app-token-private-sentinel"
        if self.permission_timeout:
            raise httpx.ReadTimeout("synthetic permission timeout", request=request)
        return httpx.Response(self.permission_status, json=self.permission)


def test_contributor_parsing_retains_a_claim_without_granting_permission():
    payload = feedback_payload()
    payload["comment"]["author_association"] = "CONTRIBUTOR"
    assert parse_feedback("issue_comment", payload, DELIVERY).author_association == "CONTRIBUTOR"


@pytest.mark.parametrize("event", [
    "issue_comment", "pull_request_review_comment", "pull_request_review",
])
@pytest.mark.parametrize("permission", ["write", "admin"])
def test_contributor_requires_effective_app_permission_for_each_feedback_family(
    review_app_key, monkeypatch, event, permission,
):
    truth = PermissionTruth(event, review_app_key)
    truth.permission.update(permission=permission, role_name="custom-example-role")
    assert asyncio.run(verify_truth(truth, monkeypatch)) == HEAD
    assert truth.calls.count(PERMISSION_PATH) == 1
    assert truth.calls.index(f"/repos/{REPO}") < truth.calls.index(PERMISSION_PATH)


@pytest.mark.parametrize("association", ["OWNER", "MEMBER", "COLLABORATOR"])
def test_existing_association_policy_keeps_its_independent_positive_path(
    review_app_key, monkeypatch, association,
):
    truth = PermissionTruth("issue_comment", review_app_key, association)
    truth.permission_status = 403
    assert asyncio.run(verify_truth(truth, monkeypatch)) == HEAD
    assert PERMISSION_PATH not in truth.calls


@pytest.mark.parametrize("permission", ["read", "none", "maintain", None, True, [], {}])
def test_contributor_cannot_gain_authority_from_role_name_or_unproved_permission(
    review_app_key, monkeypatch, permission,
):
    truth = PermissionTruth("issue_comment", review_app_key)
    truth.permission.update(permission=permission, role_name="admin")
    with pytest.raises(FeedbackIgnored, match="sender_permission_refused"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert truth.calls.count(PERMISSION_PATH) == 1


@pytest.mark.parametrize("user", [None, [], {}, {"id": 42}, {"id": True}])
def test_permission_response_must_name_the_exact_fetched_immutable_user(
    review_app_key, monkeypatch, user,
):
    truth = PermissionTruth("issue_comment", review_app_key)
    truth.permission["user"] = user
    with pytest.raises(FeedbackIgnored, match="sender_permission_identity_mismatch"):
        asyncio.run(verify_truth(truth, monkeypatch))


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_unknown_permission_is_retryable_even_after_repository_access_was_proven(
    review_app_key, monkeypatch, status,
):
    truth = PermissionTruth("issue_comment", review_app_key)
    truth.permission_status = status
    with pytest.raises(FeedbackUnavailable, match="sender_permission_unavailable"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert f"/repos/{REPO}" in truth.calls and PERMISSION_PATH in truth.calls


def test_permission_timeout_never_grants_contributor_authority(review_app_key, monkeypatch):
    truth = PermissionTruth("issue_comment", review_app_key)
    truth.permission_timeout = True
    with pytest.raises(FeedbackUnavailable, match="sender_permission_unavailable"):
        asyncio.run(verify_truth(truth, monkeypatch))


@pytest.mark.parametrize("signed,fetched", [
    ("CONTRIBUTOR", "MEMBER"), ("MEMBER", "CONTRIBUTOR"),
])
def test_association_drift_never_selects_an_easier_authorization_path(
    review_app_key, monkeypatch, signed, fetched,
):
    truth = PermissionTruth("issue_comment", review_app_key, signed)
    truth.comment["author_association"] = fetched
    with pytest.raises(FeedbackIgnored, match="feedback_changed"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert PERMISSION_PATH not in truth.calls


def test_fetched_login_is_validated_before_it_can_address_permission_endpoint(
    review_app_key, monkeypatch,
):
    truth = PermissionTruth("issue_comment", review_app_key)
    truth.comment["user"]["login"] = "wrong/path"
    with pytest.raises(FeedbackIgnored, match="non_human_sender"):
        asyncio.run(verify_truth(truth, monkeypatch))
    assert PERMISSION_PATH not in truth.calls


def test_permission_revocation_is_observed_within_the_same_verifier_client(
    review_app_key, monkeypatch,
):
    truth = PermissionTruth("issue_comment", review_app_key)
    real_client = httpx.Client
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(truth.handle)),
    )

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(truth.handle)) as client:
            async def verify():
                return await verify_feedback_truth(
                    truth.feedback, truth.lineage, settings=truth.settings, client=client
                )
            assert await verify() == HEAD
            truth.permission["permission"] = "read"
            with pytest.raises(FeedbackIgnored, match="sender_permission_refused"):
                await verify()
            truth.permission["permission"] = "write"
            assert await verify() == HEAD
            assert truth.calls.count(PERMISSION_PATH) == 3

    _RESOLVERS.clear()
    try:
        asyncio.run(exercise())
    finally:
        _RESOLVERS.clear()
