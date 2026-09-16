"""GitHub identity response guards; real publication persistence is tested separately.

REST shapes: https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
and https://docs.github.com/en/rest/repos/repos#get-a-repository.
"""

import copy

import pytest
from curie_api.publication_authority import AuthorityRefused, validated_identity


def identity_payloads() -> tuple[dict, dict]:
    repo = {"id": 9001, "full_name": "acme-corp/acme-bot"}
    pr = {
        "number": 123,
        "node_id": "PR_example_123",
        "html_url": "https://github.com/acme-corp/acme-bot/pull/123",
        "state": "open",
        "merged": False,
        "head": {"sha": "a" * 40, "ref": "curie/publication-example", "repo": copy.deepcopy(repo)},
        "base": {"repo": copy.deepcopy(repo), "ref": "main"},
    }
    return repo, pr


def validate(repo: dict, pr: dict):
    return validated_identity(
        repo,
        pr,
        installation_id=41,
        repo_full_name="acme-corp/acme-bot",
        pr_number=123,
        branch="curie/publication-example",
        head_sha="a" * 40,
        state="open",
    )


def test_verified_identity_requires_matching_repo_and_pr() -> None:
    repo, pr = identity_payloads()
    proof = validate(repo, pr)
    assert (proof.repository_id, proof.installation_id, proof.pr_node_id) == (
        9001,
        41,
        "PR_example_123",
    )


@pytest.mark.parametrize(
    "path,value",
    [
        (("id",), True),
        (("id",), 0),
        (("full_name",), "acme-corp/other"),
        (("pr", "number"), True),
        (("pr", "node_id"), ""),
        (("pr", "state"), "closed"),
        (("pr", "head", "sha"), "b" * 40),
        (("pr", "head", "ref"), "other"),
        (("pr", "head", "repo", "id"), 9002),
        (("pr", "base", "repo", "id"), 9002),
        (("pr", "html_url"), "https://github.com/acme-corp/other/pull/123"),
    ],
)
def test_identity_guard_refuses_changed_provider_truth(
    path: tuple[str, ...], value: object
) -> None:
    repo, pr = identity_payloads()
    target = pr if path[0] == "pr" else repo
    parts = path[1:] if path[0] == "pr" else path
    for component in parts[:-1]:
        target = target[component]
    target[parts[-1]] = value
    with pytest.raises(AuthorityRefused):
        validate(repo, pr)


def test_same_commit_hex_case_matches_existing_lineage_reader() -> None:
    repo, pr = identity_payloads()
    pr["head"]["sha"] = ("a" * 40).upper()
    assert validate(repo, pr).repository_id == 9001


def test_provider_identity_cannot_overflow_bigint_authority() -> None:
    repo, pr = identity_payloads()
    repo["id"] = pr["head"]["repo"]["id"] = pr["base"]["repo"]["id"] = 2**63
    with pytest.raises(AuthorityRefused):
        validate(repo, pr)
