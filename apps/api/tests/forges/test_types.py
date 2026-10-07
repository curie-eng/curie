"""Value-type rules the ports rely on: identity equality, CI on the exact head,
reply target shapes and credential headers."""

from __future__ import annotations

import base64
from datetime import UTC, datetime

import pytest
from curie_api.forges.types import (
    Actor,
    CheckState,
    CiRollup,
    Credential,
    CredentialExpiry,
    CredentialHeader,
    CredentialScope,
    NormalizedCheck,
    PullRequestRef,
    ReplyTarget,
    RepositoryRef,
    RollupState,
    TrackerIssueRef,
)

# A placeholder, never a real credential.
PLACEHOLDER = "example-token"

HEAD = "b" * 40
OLD = "a" * 40


def test_a_moved_jira_issue_keeps_its_identity() -> None:
    before = TrackerIssueRef("jira_cloud", "acme.atlassian.example", "site-1", "10042", "PROJ-7")
    after = TrackerIssueRef("jira_cloud", "acme.atlassian.example", "site-1", "10042", "OTHER-3")
    assert before == after and hash(before) == hash(after)
    assert before != TrackerIssueRef("jira_cloud", "acme.atlassian.example", "site-1", "10043")


def test_a_renamed_repository_keeps_its_identity() -> None:
    old = RepositoryRef("gitlab", "gitlab.example", "77", "group/sub/old")
    assert old == RepositoryRef("gitlab", "gitlab.example", "77", "group/sub/new")
    assert old != RepositoryRef("gitlab", "gitlab.example", "78", "group/sub/old")


def test_empty_identifiers_are_refused() -> None:
    with pytest.raises(ValueError):
        TrackerIssueRef("github", "github.example", "", "1")
    with pytest.raises(ValueError):
        Actor("", "someone")


def test_the_rollup_drops_checks_for_another_head() -> None:
    checks = (
        NormalizedCheck("unit", CheckState.SUCCESS, OLD, "Unit"),
        NormalizedCheck("lint", CheckState.PENDING, HEAD, "Lint"),
    )
    rollup = CiRollup.on_head(HEAD, checks)
    assert rollup.state is RollupState.PENDING
    assert [check.key for check in rollup.checks] == ["lint"]
    assert CiRollup.on_head(HEAD, checks[:1]).state is RollupState.NONE


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        ((CheckState.SUCCESS, CheckState.NEUTRAL, CheckState.SKIPPED), RollupState.SUCCESS),
        ((CheckState.SUCCESS, CheckState.PENDING), RollupState.PENDING),
        ((CheckState.PENDING, CheckState.FAILURE), RollupState.FAILURE),
        ((CheckState.SUCCESS, CheckState.CANCELLED), RollupState.FAILURE),
    ],
)
def test_the_rollup_verdict(states: tuple[CheckState, ...], expected: RollupState) -> None:
    checks = tuple(NormalizedCheck(f"c{n}", state, HEAD, f"C{n}") for n, state in enumerate(states))
    assert CiRollup.on_head(HEAD, checks).state is expected


def test_reply_targets_carry_exactly_their_references() -> None:
    issue = TrackerIssueRef("github", "github.example", "4401", "12")
    pull = PullRequestRef(RepositoryRef("github", "github.example", "4401", "acme/bot"), "3")
    assert ReplyTarget.on_thread(pull, "99").thread_id == "99"
    assert ReplyTarget.on_issue(issue).issue == issue
    with pytest.raises(ValueError):
        ReplyTarget("issue", pull_request=pull)
    with pytest.raises(ValueError):
        ReplyTarget("review_thread", pull_request=pull)


def test_credential_headers_and_expiry() -> None:
    basic = Credential(
        origin="https://github.example",
        header=CredentialHeader.AUTHORIZATION_BASIC,
        secret=PLACEHOLDER,
        scope=CredentialScope.PUSH,
        expiry=CredentialExpiry.KNOWN,
        expires_at=datetime(2026, 1, 1, tzinfo=UTC),
        username="x-access-token",
    )
    encoded = base64.b64encode(b"x-access-token:example-token").decode()
    assert basic.git_header() == f"Authorization: Basic {encoded}"
    assert "example-token" not in repr(basic)
    private = Credential(
        origin="https://gitlab.example",
        header=CredentialHeader.PRIVATE_TOKEN,
        secret=PLACEHOLDER,
        scope=CredentialScope.CLONE,
        expiry=CredentialExpiry.UNKNOWN,
    )
    assert private.git_header() == "PRIVATE-TOKEN: example-token"
    with pytest.raises(ValueError):
        Credential(
            origin="https://gitlab.example",
            header=CredentialHeader.AUTHORIZATION_BEARER,
            secret=PLACEHOLDER,
            scope=CredentialScope.CLONE,
            expiry=CredentialExpiry.KNOWN,
        )
