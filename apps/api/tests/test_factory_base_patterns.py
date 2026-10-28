"""Factory base policy through the binding parser, chooser and remote resolver."""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.factory_base import BaseChoice, BaseRefusal, ResolvedBase, choose_base, resolve_base
from curie_api.forges.config import RepositoryBindingConfig
from curie_api.forges.memory import InMemoryCodeHost
from curie_api.github_review_events import FeedbackUnavailable
from forge_fakes.gitlab import GitLabAPI, gitlab_code_host
from pydantic import ValidationError


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _policy(
    bases: tuple[str, ...] = (), *, default_base: str | None = None
) -> RepositoryBindingConfig:
    return RepositoryBindingConfig.model_validate(
        {
            "alias": "example",
            "code_host": "example-code",
            "project_id": "4401",
            "path": "acme-corp/acme-bot",
            "bases": bases,
            "default_base": default_base,
        }
    )


@pytest.mark.parametrize(
    ("patterns", "branch"),
    [
        (("main", "next"), "next"),
        (("release/*",), "release/1.0"),
        (("release/*",), "release/stable/1.0"),
        (("release/?",), "release/a"),
        (("release/[0-9]*",), "release/42"),
        (("*",), "feature/new-parser"),
        (("a" * 40,), "a" * 40),
        (("refs/heads/topic",), "refs/heads/topic"),
    ],
)
def test_policy_selects_an_exact_or_glob_matching_literal(
    patterns: tuple[str, ...], branch: str
) -> None:
    policy = _policy(patterns)

    assert choose_base({f"base:{branch}", "factory"}, policy, "main") == BaseChoice(
        branch, "label"
    )


@pytest.mark.parametrize(
    ("patterns", "branch"),
    [
        (("main", "next"), "release/1.0"),
        (("release/*",), "Release/1.0"),
        (("Release/*",), "release/1.0"),
        (("MAIN",), "main"),
        (("release/?",), "release/ab"),
        (("release/[0-9]*",), "release/stable"),
    ],
)
def test_policy_refuses_disallowed_or_wrong_case_labels(
    patterns: tuple[str, ...], branch: str
) -> None:
    chosen = choose_base({f"base:{branch}"}, _policy(patterns), "main")

    assert isinstance(chosen, BaseRefusal)
    assert chosen.code == "base_not_allowed"


@pytest.mark.parametrize(
    ("patterns", "branch"),
    [
        (("release/*",), "release/1.0"),
        (("*",), "release/1.0"),
        (("*",), "a" * 40),
        (("refs/*",), "refs/heads/topic"),
    ],
)
def test_default_is_a_literal_branch_matched_by_the_policy(
    patterns: tuple[str, ...], branch: str
) -> None:
    policy = _policy(patterns, default_base=branch)

    assert policy.default_base == branch
    assert choose_base({"factory"}, policy, "main") == BaseChoice(branch, "default")


def test_repository_default_must_match_a_nonempty_policy_without_an_override() -> None:
    policy = _policy(("release/*",))

    chosen = choose_base(set(), policy, "main")

    assert isinstance(chosen, BaseRefusal)
    assert chosen.code == "base_not_allowed"
    assert choose_base(set(), policy, "release/2.0") == BaseChoice("release/2.0", "default")


@pytest.mark.parametrize("policy", [None, _policy()])
def test_empty_policy_allows_only_the_repository_default(
    policy: RepositoryBindingConfig | None,
) -> None:
    assert choose_base(set(), policy, "main") == BaseChoice("main", "default")
    assert choose_base({"base:main"}, policy, "main") == BaseChoice("main", "label")
    refused = choose_base({"base:next"}, policy, "main")
    assert isinstance(refused, BaseRefusal)
    assert refused.code == "base_not_allowed"


@pytest.mark.parametrize(
    ("patterns", "default"),
    [
        (("release/*",), "main"),
        (("release/*",), "Release/1.0"),
        (("*",), "release/*"),
        (("*",), "release/../main"),
        ((), "main"),
    ],
)
def test_binding_refuses_a_default_outside_policy_or_without_a_literal_branch(
    patterns: tuple[str, ...], default: str
) -> None:
    with pytest.raises(ValidationError):
        _policy(patterns, default_base=default)


@pytest.mark.parametrize(
    "patterns",
    [
        ("",),
        (" ",),
        ("release/* ",),
        ("release/[",),
        ("release/[]",),
        ("release/[!]",),
        ("-release/*",),
        ("release/../*",),
        ("release/*", "release/*"),
        ("main", "main"),
    ],
)
def test_binding_refuses_malformed_or_duplicate_patterns(patterns: tuple[str, ...]) -> None:
    with pytest.raises(ValidationError):
        _policy(patterns)


INVALID_BRANCHES = (
    "",
    " ",
    "-main",
    "/main",
    "main/",
    "release//stable",
    "release/../stable",
    "release/.hidden",
    "release/stable.lock",
    "main.",
    "@",
    "main@{1}",
    "main~1",
    "main^",
    "main:name",
    "release/?",
    "release/*",
    "release/[12]",
    "release\\stable",
    "main\x01",
    "main\x7f",
)


@pytest.mark.parametrize("branch", INVALID_BRANCHES)
def test_wildcard_never_authorizes_an_invalid_branch_literal(branch: str) -> None:
    refused = choose_base({f"base:{branch}"}, _policy(("*",)), "main")

    assert isinstance(refused, BaseRefusal)
    assert choose_base({"base:release/1.0"}, _policy(("*",)), "main") == BaseChoice(
        "release/1.0", "label"
    )


def test_wildcard_does_not_resolve_conflicting_labels() -> None:
    policy = _policy(("*",))
    refused = choose_base({"base:main", "base:release/1.0"}, policy, "main")

    assert isinstance(refused, BaseRefusal)
    assert refused.code == "base_conflict"
    assert choose_base({"base:release/1.0"}, policy, "main") == BaseChoice(
        "release/1.0", "label"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("patterns", [("release/*",), ("*",)])
async def test_resolver_requires_the_selected_remote_branch_and_never_falls_back(
    patterns: tuple[str, ...],
) -> None:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme-corp/acme-bot")
    default_head = host.push(repository, "main")
    policy = _policy(patterns)
    arguments: dict[str, Any] = {
        "policy": policy,
        "labels": {"base:release/1.0"},
        "default_branch": "main",
        "code_host": host,
        "repository": repository,
    }

    missing = await resolve_base(**arguments)

    assert isinstance(missing, BaseRefusal)
    assert missing.code == "base_missing"
    head = host.push(repository, "release/1.0")
    assert head != default_head
    assert await resolve_base(**arguments) == ResolvedBase("release/1.0", "label", head)


@pytest.mark.anyio
@pytest.mark.parametrize("branch", ["a" * 40, "A" * 64, "refs/heads/topic", "refs/tags/v1.0"])
@pytest.mark.parametrize("wildcard", [False, True], ids=["exact", "wildcard"])
async def test_revision_shaped_names_resolve_only_as_existing_literal_heads(
    branch: str, wildcard: bool
) -> None:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme-corp/acme-bot")
    host.push(repository, "main")
    arguments: dict[str, Any] = {
        "policy": _policy(("*",) if wildcard else (branch,)),
        "labels": {f"base:{branch}"},
        "default_branch": "main",
        "code_host": host,
        "repository": repository,
    }

    missing = await resolve_base(**arguments)

    assert isinstance(missing, BaseRefusal)
    assert missing.code == "base_missing"
    head = host.push(repository, branch)
    assert await resolve_base(**arguments) == ResolvedBase(branch, "label", head)


@pytest.mark.anyio
async def test_remote_outage_stays_retryable_and_a_healthy_read_resolves_the_same_branch() -> None:
    fake = GitLabAPI()
    head = fake.push("release/1.0")
    path = f"/projects/{fake.project_id}/repository/branches/release%2F1.0"
    fake.script(503, method="GET", path=path)
    async with fake.client() as client:
        host = gitlab_code_host(fake, client=client)
        arguments: dict[str, Any] = {
            "policy": _policy(("*",)),
            "labels": {"base:release/1.0"},
            "default_branch": "main",
            "code_host": host,
            "repository": fake.repository(),
        }

        with pytest.raises(FeedbackUnavailable) as unavailable:
            await resolve_base(**arguments)

        assert str(unavailable.value) == "base_unavailable"
        assert await resolve_base(**arguments) == ResolvedBase("release/1.0", "label", head)

    assert len(fake.calls("GET", path)) == 2
    assert not fake.calls("GET", f"/projects/{fake.project_id}/repository/branches/main")


def test_an_unreadable_repository_default_cannot_invent_a_branch() -> None:
    with pytest.raises(FeedbackUnavailable):
        choose_base(set(), _policy(("*",)), None)
