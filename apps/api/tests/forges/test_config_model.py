"""The forge config model (ADR 0197): every refusal is shown on a violating
document, and a realistic neighbour of it is shown to load."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any, get_args

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import CODE_HOST_ONLY, NATIVE_FORGES, TRACKER_ONLY
from curie_api.forges.config import (
    AccountAllowlist,
    CodeHostKind,
    ForgesConfig,
    GroupAuthority,
    StaticTokenCredentialConfig,
    TrackerKind,
    WriteCheck,
)
from curie_api.forges.paths import valid_repository_path
from forge_fakes import config_samples as samples
from pydantic import ValidationError


def _load(document: dict[str, Any]) -> ForgesConfig:
    return ForgesConfig.model_validate(document)


def _refused(document: dict[str, Any], match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        _load(document)


def _edit(document: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    edited = copy.deepcopy(document)
    *parents, leaf = path.split(".")
    node: Any = edited
    for part in parents:
        node = node[int(part)] if part.isdigit() else node[part]
    node[int(leaf) if leaf.isdigit() else leaf] = value
    return edited


# Today's factory ---------------------------------------------------------------


def test_todays_single_github_repository_factory_loads() -> None:
    config = _load(samples.github_document())

    (tracker,) = config.trackers
    (binding,) = config.bindings
    (repo,) = binding.repos
    assert (tracker.label, tracker.mention, tracker.poll_interval_s) == ("curie", "curie-bot", 45)
    assert isinstance(binding.start_authority, WriteCheck)
    assert binding.tracker_scope_id == repo.project_id == samples.GITHUB_REPO_ID
    assert (repo.path, repo.bases, repo.default_base) == ("acme-corp/api", ("main", "next"), "next")
    assert repo.ci.required is not None
    assert repo.ci.required.key == "python-ci"
    assert repo.ci.required.paths == ("apps/api", "packages")
    assert repo.ci.required.pending_key_prefix == "python-ci / shard"
    assert repo.ci.metadata_rerun_keys == ("pr-body", "status:docs")
    assert config.card_base_url == "https://curie.example.com"
    assert config.tracker(binding.tracker) is tracker
    assert config.code_host(repo.code_host).kind == "github"


def test_several_github_repositories_share_one_app_and_tracker() -> None:
    document = samples.github_document()
    document["bindings"].append(samples.github_binding("acme-web", "812345679", "acme-corp/web"))
    assert [b.name for b in _load(document).bindings] == ["acme-api", "acme-web"]


def test_a_repository_without_bases_or_ci_loads_with_the_default_branch_only() -> None:
    document = samples.github_document()
    repo = document["bindings"][0]["repos"][0]
    for field in ("bases", "default_base", "ci"):
        del repo[field]
    loaded = _load(document).bindings[0].repos[0]
    assert loaded.bases == () and loaded.default_base is None and loaded.ci.required is None


def test_the_secret_never_appears_in_repr() -> None:
    config = _load(samples.github_document())
    assert "BEGIN KEY" not in repr(config)


def test_configurable_kinds_are_the_registered_ones() -> None:
    assert set(get_args(CodeHostKind)) == (NATIVE_FORGES | CODE_HOST_ONLY) - {types.MEMORY}
    assert set(get_args(TrackerKind)) == (NATIVE_FORGES | TRACKER_ONLY) - {
        types.MEMORY,
        types.MEMORY_TRACKER_ONLY,
    }


def test_an_unregistered_or_in_memory_kind_is_refused() -> None:
    _refused(_edit(samples.github_document(), "code_hosts.0.kind", "memory"), "kind")
    _refused(_edit(samples.github_document(), "trackers.0.kind", "bitbucket_cloud"), "kind")


# Names and references -----------------------------------------------------------


@pytest.mark.parametrize("section", ["code_hosts", "trackers", "bindings"])
def test_duplicate_names_are_refused(section: str) -> None:
    document = samples.github_document()
    document[section].append(copy.deepcopy(document[section][0]))
    _refused(document, f"{section[:-1].replace('_', ' ')} names are not unique")


def test_unknown_tracker_and_code_host_references_are_refused() -> None:
    _refused(_edit(samples.github_document(), "bindings.0.tracker", "jira"), "unknown tracker")
    _refused(
        _edit(samples.github_document(), "bindings.0.repos.0.code_host", "gitlab"),
        "unknown code host 'gitlab'",
    )


@pytest.mark.parametrize("bad", ["API", "repo:web", "web app", "-web", ""])
def test_an_alias_a_repo_label_cannot_carry_is_refused(bad: str) -> None:
    _refused(_edit(samples.github_document(), "bindings.0.repos.0.alias", bad), "alias")


def test_a_lowercase_dotted_alias_loads() -> None:
    _load(_edit(samples.github_document(), "bindings.0.repos.0.alias", "api.v2"))


def test_duplicate_aliases_in_a_binding_are_refused() -> None:
    repos = samples.jira_binding()["repos"]
    repos[1]["alias"] = "web"
    _refused(samples.jira_document(repos=repos), "repeats aliases")


def test_one_repository_under_two_aliases_is_refused() -> None:
    repos = samples.jira_binding()["repos"]
    repos.append({**repos[0], "alias": "web2"})
    _refused(samples.jira_document(repos=repos), "one repository under two aliases")


def test_a_default_repo_outside_the_binding_is_refused() -> None:
    _refused(samples.jira_document(default_repo="mobile"), "default_repo 'mobile'")
    assert _load(samples.jira_document(default_repo="infra")).bindings[0].default_repo == "infra"


def test_a_component_mapped_to_an_unknown_alias_is_refused() -> None:
    _refused(samples.jira_document(components={"Mobile": "mobile"}), "Mobile->mobile")


def test_two_bindings_on_one_tracker_scope_are_refused() -> None:
    document = samples.github_document()
    document["bindings"].append(samples.github_binding("acme-api-2", path="acme-corp/api-2"))
    _refused(document, "claim the same tracker scope")


# Pairing --------------------------------------------------------------------------


def test_a_github_tracker_bound_to_a_gitlab_repository_is_refused() -> None:
    document = samples.github_document()
    document["code_hosts"] = [samples.gitlab_code_host("github")]
    document["bindings"][0]["repos"][0]["path"] = "acme/platform/api"
    _refused(document, "a github tracker pairs only with a github code host")


def test_a_github_tracker_bound_to_another_github_host_is_refused() -> None:
    document = samples.github_document()
    document["code_hosts"] = [samples.github_code_host(host="ghe.example")]
    _refused(document, "pairs only with a code host on github.com")
    document["trackers"] = [samples.github_tracker(host="ghe.example")]
    assert _load(document).code_hosts[0].api_url == "https://ghe.example/api/v3"


def test_a_gitlab_tracker_pairs_with_its_own_gitlab() -> None:
    document = {
        "code_hosts": [samples.gitlab_code_host()],
        "trackers": [
            {
                **samples.jira_tracker("gitlab-issues"),
                "kind": "gitlab",
                "host": "gitlab.example",
                "api_url": "https://gitlab.example/api/v4",
                "credential": samples.static_token(),
                "mention": "curie-bot",
            }
        ],
        "bindings": [
            {
                "name": "infra",
                "tracker": "gitlab-issues",
                "tracker_scope_id": "4242",
                "repos": [
                    {
                        "alias": "infra",
                        "code_host": "gitlab",
                        "project_id": "4242",
                        "path": "acme/platform/infra",
                    }
                ],
            }
        ],
    }
    assert isinstance(_load(document).bindings[0].start_authority, WriteCheck)
    document["code_hosts"] = [{**samples.bitbucket_code_host(), "name": "gitlab"}]
    document["bindings"][0]["repos"][0]["path"] = "acme/infra"
    _refused(document, "a gitlab tracker pairs only with a gitlab code host")


def test_jira_pairs_with_any_code_host() -> None:
    config = _load(samples.jira_document())
    assert {config.code_host(repo.code_host).kind for repo in config.bindings[0].repos} == {
        "bitbucket_cloud",
        "gitlab",
    }
    document = samples.jira_document()
    document["code_hosts"].append(samples.github_code_host())
    repos = document["bindings"][0]["repos"]
    repos.append(
        {"alias": "api", "code_host": "github", "project_id": "1", "path": "acme-corp/api"}
    )
    _load(document)


def test_a_native_binding_lists_exactly_its_own_repository() -> None:
    document = samples.github_document()
    repos = document["bindings"][0]["repos"]
    repos.append({**repos[0], "alias": "web", "project_id": "999", "path": "acme-corp/web"})
    _refused(document, "binds exactly its own repository")


def test_a_native_scope_other_than_its_repository_is_refused() -> None:
    document = _edit(samples.github_document(), "bindings.0.tracker_scope_id", "1")
    _refused(document, "tracker_scope_id must equal project_id")


def test_a_native_binding_has_no_component_map() -> None:
    document = _edit(samples.github_document(), "bindings.0.components", {"Backend": "api"})
    _refused(document, "has no component map")


# Start authority ----------------------------------------------------------------


@pytest.mark.parametrize(
    "authority",
    [{"type": "allowlist", "accounts": ["12345"]}, {"type": "group", "group_id": "g-1"}],
)
def test_a_native_tracker_keeps_its_write_check(authority: dict[str, Any]) -> None:
    document = _edit(samples.github_document(), "bindings.0.start_authority", authority)
    _refused(document, "admits by its write check only")


def test_a_jira_binding_without_an_allowlist_or_group_is_refused() -> None:
    document = samples.jira_document()
    del document["bindings"][0]["start_authority"]
    _refused(document, "start authority must be an allowlist or a group")
    _refused(
        samples.jira_document(start_authority={"type": "write_check"}),
        "start authority must be an allowlist or a group",
    )


def test_a_jira_binding_loads_with_an_allowlist_or_a_group() -> None:
    allowlisted = _load(samples.jira_document()).bindings[0].start_authority
    assert isinstance(allowlisted, AccountAllowlist)
    assert allowlisted.accounts == {samples.JIRA_STARTER}
    grouped = _load(
        samples.jira_document(start_authority={"type": "group", "group_id": "starters-id"})
    )
    authority = grouped.bindings[0].start_authority
    assert isinstance(authority, GroupAuthority) and authority.group_id == "starters-id"


@pytest.mark.parametrize("accounts", [[], ["dev@example.com"], [" "]])
def test_an_allowlist_of_no_accounts_or_of_emails_is_refused(accounts: list[str]) -> None:
    _refused(
        samples.jira_document(start_authority={"type": "allowlist", "accounts": accounts}),
        "allowlist",
    )


def test_a_jira_binding_must_name_a_default_repo() -> None:
    document = samples.jira_document()
    del document["bindings"][0]["default_repo"]
    _refused(document, "must name a default_repo")


# Review feedback allowlist ------------------------------------------------------


def test_a_feedback_allowlist_for_a_host_the_binding_does_not_use_is_refused() -> None:
    _refused(
        samples.jira_document(review_feedback_allowlist={"github": ["1"]}),
        "names code hosts it binds no repo on",
    )
    _refused(
        samples.jira_document(review_feedback_allowlist={"bitbucket": []}),
        "must name account ids",
    )
    loaded = _load(samples.jira_document(review_feedback_allowlist={"gitlab": ["77"]}))
    assert loaded.bindings[0].review_feedback_allowlist == {"gitlab": frozenset({"77"})}


# Paths, bases, CI -----------------------------------------------------------------


@pytest.mark.parametrize("path", ["acme-corp", "acme-corp/api/extra", "acme corp/api", "/api"])
def test_a_github_path_is_exactly_owner_and_name(path: str) -> None:
    _refused(_edit(samples.github_document(), "bindings.0.repos.0.path", path), "is not owner/name")


@pytest.mark.parametrize("path", ["infra", "acme//infra", "acme/../infra", "acme/infra/"])
def test_a_nested_path_needs_two_real_segments(path: str) -> None:
    _refused(
        _edit(samples.jira_document(), "bindings.0.repos.1.path", path), "two or more segments"
    )


def test_the_path_rule_is_per_kind() -> None:
    assert valid_repository_path("gitlab", "acme/platform/team/infra")
    assert valid_repository_path("bitbucket_dc", "PROJ/repo.name")
    assert valid_repository_path("github", "acme-corp/api.js")
    assert not valid_repository_path("github", "acme/platform/infra")


def test_bases_must_be_unique_branches_holding_the_default() -> None:
    document = samples.github_document()
    _refused(_edit(document, "bindings.0.repos.0.bases", ["main", "main"]), "unique branch names")
    _refused(_edit(document, "bindings.0.repos.0.bases", ["main", "-x"]), "unique branch names")
    _refused(_edit(document, "bindings.0.repos.0.default_base", "dev"), "one of its bases")
    _refused(_edit(document, "bindings.0.repos.0.bases", []), "one of its bases")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("paths", []),
        ("paths", ["/apps"]),
        ("paths", ["apps/"]),
        ("key", " "),
        ("pending_key_prefix", ""),
    ],
)
def test_a_malformed_required_check_is_refused(field: str, value: Any) -> None:
    document = _edit(samples.github_document(), f"bindings.0.repos.0.ci.required.{field}", value)
    _refused(document, field)


def test_duplicate_metadata_rerun_keys_are_refused() -> None:
    document = _edit(
        samples.github_document(), "bindings.0.repos.0.ci.metadata_rerun_keys", ["a", "a"]
    )
    _refused(document, "metadata_rerun_keys")


# Trackers, hosts and credentials ----------------------------------------------------


@pytest.mark.parametrize("label", ["", "two words", "x" * 51])
def test_an_intake_label_a_tracker_cannot_hold_is_refused(label: str) -> None:
    _refused(_edit(samples.github_document(), "trackers.0.label", label), "label")


def test_a_github_mention_must_be_a_login_and_a_jira_one_an_account() -> None:
    _refused(_edit(samples.github_document(), "trackers.0.mention", "@curie bot"), "mention")
    _refused(_edit(samples.jira_document(), "trackers.0.mention", "two words"), "mention")


@pytest.mark.parametrize("interval", [0, -1, "inf"])
def test_a_poll_interval_must_be_positive_and_finite(interval: Any) -> None:
    _refused(_edit(samples.github_document(), "trackers.0.poll_interval_s", interval), "poll")


@pytest.mark.parametrize(
    "url",
    [
        "http://api.github.example",
        "ftp://api.github.example",
        "https://user:pw@api.github.example",
        "https://api.github.example?x=1",
    ],
)
def test_an_api_url_that_is_not_plain_https_is_refused(url: str) -> None:
    _refused(_edit(samples.github_document(), "code_hosts.0.api_url", url), "api_url")


def test_a_loopback_http_api_url_loads_for_local_forges() -> None:
    document = _edit(samples.github_document(), "code_hosts.0.api_url", "http://localhost:8080/")
    assert _load(document).code_hosts[0].api_url == "http://localhost:8080"


@pytest.mark.parametrize("host", ["https://github.com", "github.com/api", "git hub.com"])
def test_a_host_must_be_a_bare_hostname(host: str) -> None:
    _refused(_edit(samples.github_document(), "code_hosts.0.host", host), "bare hostname")


def test_a_host_is_compared_case_insensitively() -> None:
    document = _edit(samples.github_document(), "code_hosts.0.host", "GitHub.com")
    assert _load(document).code_hosts[0].host == "github.com"


def test_a_card_base_url_must_be_https() -> None:
    _refused(_edit(samples.github_document(), "card_base_url", "http://curie.example"), "api_url")
    assert _load(_edit(samples.github_document(), "card_base_url", "")).card_base_url == ""


def test_a_reminting_credential_is_refused_on_a_forge_that_cannot_use_it() -> None:
    _refused(
        _edit(samples.jira_document(), "code_hosts.1.credential", samples.github_app()),
        "github_app credential cannot authenticate to gitlab",
    )
    oauth = samples.jira_tracker()["credential"]
    _refused(
        _edit(samples.github_document(), "trackers.0.credential", oauth),
        "oauth_client_credentials credential cannot authenticate to github",
    )
    _load(_edit(samples.github_document(), "trackers.0.credential", samples.static_token()))


def test_a_static_token_has_one_expiry_declaration() -> None:
    both = samples.static_token(expires_at="2027-01-31T00:00:00Z", never_expires=True)
    _refused(_edit(samples.jira_document(), "code_hosts.1.credential", both), "not both")
    naive = samples.static_token(expires_at="2027-01-31T00:00:00")
    _refused(_edit(samples.jira_document(), "code_hosts.1.credential", naive), "timezone")
    blank = samples.static_token(token=" ")
    _refused(_edit(samples.jira_document(), "code_hosts.1.credential", blank), "non-empty")
    loaded = _load(samples.jira_document()).code_host("gitlab").credential
    assert isinstance(loaded, StaticTokenCredentialConfig)
    assert loaded.expires_at == datetime(2027, 1, 31, tzinfo=UTC)


def test_an_unknown_field_is_refused() -> None:
    _refused(_edit(samples.github_document(), "trackers.0.labels", ["x"]), "labels")
