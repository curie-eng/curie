"""Workspace repository paths and transport facts are keyed on the kind (ADR 0197).

GitHub keeps exactly ``owner/name`` everywhere it did before; a code host kind
that nests groups accepts a deeper path. The rule has one home,
``curie_api.forges.paths``.
"""

from __future__ import annotations

import pytest
from curie_api import workspace_policy
from curie_api.forges.paths import valid_repository_path
from curie_api.forges.types import CredentialHeader
from curie_api.schemas.workspaces import (
    RepositoryCredentialOut,
    WorkspaceCredentialOut,
    WorkspaceSelectionRequest,
)
from pydantic import ValidationError

_BASE = {"conversation_id": "thread-1", "author": "U0REQUEST1"}
_DEEP = "platform/team/infra"


def test_selection_defaults_to_github_and_keeps_owner_name() -> None:
    request = WorkspaceSelectionRequest(**_BASE, repo_full_name="acme-corp/acme-bot")

    assert request.repository_kind == "github"
    assert request.repo_full_name == "acme-corp/acme-bot"


@pytest.mark.parametrize("kind", [None, "github"])
@pytest.mark.parametrize("path", [_DEEP, "acme-corp", "acme-corp/acme-bot.", "-acme/bot"])
def test_github_selection_refuses_anything_but_owner_name(kind: str | None, path: str) -> None:
    extra = {} if kind is None else {"repository_kind": kind}

    with pytest.raises(
        ValidationError, match="repo_full_name must be one canonical owner/repository name"
    ):
        WorkspaceSelectionRequest(**_BASE, **extra, repo_full_name=path)


@pytest.mark.parametrize("kind", ["gitlab", "bitbucket_dc"])
def test_a_nesting_kind_accepts_a_deep_path(kind: str) -> None:
    request = WorkspaceSelectionRequest(**_BASE, repository_kind=kind, repo_full_name=_DEEP)

    assert request.repo_full_name == _DEEP


@pytest.mark.parametrize("path", ["infra", "platform/../infra", "platform//infra", "a b/c"])
def test_a_nesting_kind_still_refuses_a_path_that_is_not_one(path: str) -> None:
    with pytest.raises(ValidationError, match="gitlab repository path of two or more segments"):
        WorkspaceSelectionRequest(**_BASE, repository_kind="gitlab", repo_full_name=path)


def test_selection_refuses_a_kind_that_is_not_a_code_host() -> None:
    with pytest.raises(ValidationError, match="repository_kind"):
        WorkspaceSelectionRequest(**_BASE, repository_kind="jira_cloud", repo_full_name=_DEEP)


def test_selection_without_a_repository_needs_no_path_check() -> None:
    request = WorkspaceSelectionRequest(**_BASE, repository_kind="gitlab")

    assert request.repo_full_name is None


def test_a_credential_always_names_its_origin_and_defaults_to_github_transport() -> None:
    for model in (RepositoryCredentialOut, WorkspaceCredentialOut):
        with pytest.raises(ValidationError, match="origin"):
            model(
                repo_full_name="acme-corp/acme-bot",
                clone_url="https://github.com/acme-corp/acme-bot.git",
                authorization_header="Basic abc",
            )
        credential = model(
            repo_full_name="acme-corp/acme-bot",
            clone_url="https://github.com/acme-corp/acme-bot.git",
            authorization_header="Basic abc",
            origin="https://github.com",
        )

        assert credential.header_form is CredentialHeader.AUTHORIZATION_BASIC
        assert credential.ca_bundle_ref is None


def test_credential_carries_a_self_managed_origin_header_form_and_ca_reference() -> None:
    credential = RepositoryCredentialOut(
        repo_full_name=_DEEP,
        clone_url=f"https://gitlab.example.com/{_DEEP}.git",
        authorization_header="glpat-redacted",
        origin="https://gitlab.example.com",
        header_form="private_token",
        ca_bundle_ref="/etc/curie/ca/forge.pem",
    )

    assert credential.model_dump(include={"origin", "header_form", "ca_bundle_ref"}) == {
        "origin": "https://gitlab.example.com",
        "header_form": CredentialHeader.PRIVATE_TOKEN,
        "ca_bundle_ref": "/etc/curie/ca/forge.pem",
    }


def test_workspace_policy_has_no_second_repository_name_rule() -> None:
    """The owner/name rule lives only in forges.paths; the policy routes through it."""

    assert not hasattr(workspace_policy, "valid_repository_name")
    assert not hasattr(workspace_policy, "REPOSITORY_FULL_NAME_PATTERN")


@pytest.mark.parametrize(
    ("entry", "valid"),
    [
        ("acme-corp/acme-bot", True),
        ("acme-corp/*", True),
        (_DEEP, False),
        ("platform/team/*", False),
        ("acme-corp/*/extra", False),
    ],
)
def test_the_github_allowlist_keeps_owner_name_and_owner_wildcards(
    entry: str, valid: bool
) -> None:
    assert workspace_policy.valid_allowlist_entry(entry) is valid
    if "*" not in entry:
        assert valid_repository_path("github", entry) is valid
