"""Realistic forge config documents the config, resolution and authority tests load.

Each builder returns a plain dict, the shape an operator's values file renders
to, so tests go through ``ForgesConfig.model_validate`` exactly as a loader does.
All ids and hosts are fictional.
"""

from __future__ import annotations

from typing import Any

GITHUB_REPO_ID = "812345678"
JIRA_SITE_ID = "a1b2c3d4-0000-4000-8000-000000000001"
JIRA_STARTER = "557058:aaaaaaaa-0000-4000-8000-000000000001"


# Placeholders, never real credentials.
PLACEHOLDER_APP_KEY = "-----BEGIN KEY-----x"
PLACEHOLDER_CLIENT = "secret-1"

def github_app() -> dict[str, Any]:
    return {"type": "github_app", "app_id": "123456", "private_key": PLACEHOLDER_APP_KEY}


def static_token(**extra: Any) -> dict[str, Any]:
    return {"type": "static_token", "token": "glpat-example", **extra}


def github_code_host(name: str = "github", host: str = "github.com") -> dict[str, Any]:
    api = "https://api.github.com" if host == "github.com" else f"https://{host}/api/v3"
    return {
        "name": name,
        "kind": "github",
        "host": host,
        "api_url": api,
        "credential": github_app(),
    }


def github_tracker(name: str = "github-issues", host: str = "github.com") -> dict[str, Any]:
    api = "https://api.github.com" if host == "github.com" else f"https://{host}/api/v3"
    return {
        "name": name,
        "kind": "github",
        "host": host,
        "api_url": api,
        "credential": github_app(),
        "label": "curie",
        "mention": "curie-bot",
        "poll_interval_s": 45,
    }


def github_binding(
    name: str = "acme-api", project_id: str = GITHUB_REPO_ID, path: str = "acme-corp/api"
) -> dict[str, Any]:
    """Today's single-repository factory: one repo, its own issues, write check."""

    return {
        "name": name,
        "tracker": "github-issues",
        "tracker_scope_id": project_id,
        "repos": [
            {
                "alias": "api",
                "code_host": "github",
                "project_id": project_id,
                "path": path,
                "bases": ["main", "next"],
                "default_base": "next",
                "ci": {
                    "required": {
                        "key": "python-ci",
                        "paths": ["apps/api", "packages"],
                        "pending_key_prefix": "python-ci / shard",
                    },
                    "metadata_rerun_keys": ["pr-body", "status:docs"],
                },
            }
        ],
    }


def github_document() -> dict[str, Any]:
    return {
        "code_hosts": [github_code_host()],
        "trackers": [github_tracker()],
        "bindings": [github_binding()],
        "card_base_url": "https://curie.example.com/",
    }


def jira_tracker(name: str = "jira") -> dict[str, Any]:
    return {
        "name": name,
        "kind": "jira_cloud",
        "host": "acme.atlassian.example",
        "api_url": "https://api.atlassian.example/ex/jira/site",
        "credential": {
            "type": "oauth_client_credentials",
            "client_id": "client-1",
            "client_secret": PLACEHOLDER_CLIENT,
        },
        "label": "curie",
        "mention": "712020:bbbbbbbb-0000-4000-8000-000000000002",
    }


def bitbucket_code_host(name: str = "bitbucket") -> dict[str, Any]:
    return {
        "name": name,
        "kind": "bitbucket_cloud",
        "host": "bitbucket.example",
        "api_url": "https://api.bitbucket.example/2.0",
        "credential": static_token(username="x-token-auth"),
    }


def gitlab_code_host(name: str = "gitlab") -> dict[str, Any]:
    return {
        "name": name,
        "kind": "gitlab",
        "host": "gitlab.example",
        "api_url": "https://gitlab.example/api/v4",
        "credential": static_token(expires_at="2027-01-31T00:00:00Z"),
    }


def jira_binding(**overrides: Any) -> dict[str, Any]:
    """A Jira site bound to a web repo on Bitbucket and a nested GitLab project."""

    binding: dict[str, Any] = {
        "name": "acme-jira",
        "tracker": "jira",
        "tracker_scope_id": JIRA_SITE_ID,
        "repos": [
            {
                "alias": "web",
                "code_host": "bitbucket",
                "project_id": "{6f1c0000-0000-4000-8000-000000000003}",
                "path": "acme/web",
            },
            {
                "alias": "infra",
                "code_host": "gitlab",
                "project_id": "4242",
                "path": "acme/platform/infra",
            },
        ],
        "default_repo": "web",
        "components": {"Frontend": "web", "Platform": "infra"},
        "start_authority": {"type": "allowlist", "accounts": [JIRA_STARTER]},
        "review_feedback_allowlist": {"bitbucket": ["{reviewer-uuid-1}"]},
    }
    binding.update(overrides)
    return binding


def jira_document(**binding_overrides: Any) -> dict[str, Any]:
    return {
        "code_hosts": [bitbucket_code_host(), gitlab_code_host()],
        "trackers": [jira_tracker()],
        "bindings": [jira_binding(**binding_overrides)],
    }
