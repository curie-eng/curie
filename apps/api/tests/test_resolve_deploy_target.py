"""Resolving a named deploy target (ADR-0089, #1166).

The endpoint exists so there is exactly ONE parser for deploy.yaml. A second
implementation in the CLI could disagree with this one about the same file, and
the file's whole job is to be unambiguous about where a deploy lands -- a
disagreement routes the deploy somewhere the author did not intend and reports
success.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client() -> TestClient:
    """A client with NO database fixture, deliberately.

    This endpoint is a pure function of the text posted to it. Building the
    client without the db fixture is the assertion: if resolution ever grew a
    database dependency, this module would stop importing.
    """

    from curie_api.main import create_app

    return TestClient(create_app())


@pytest.fixture(scope="module")
def auth_headers() -> dict[str, str]:
    from curie_api.config import get_settings

    return {"X-API-Key": get_settings().api_key}


REAL = (
    "targets:\n"
    "  dev:\n"
    "    agent: acme-dev\n"
    "    env: dev\n"
    "    slack_channel: C000000A01\n"
    "  prod:\n"
    "    agent: acme-bot\n"
    "    env: prod\n"
    "    slack_channel: C000000A02\n"
)


def _resolve(client: TestClient, headers: dict, content: str, target: str):
    return client.post(
        "/deploy-targets/resolve", json={"content": content, "target": target}, headers=headers
    )


def test_resolves_the_named_target(client: TestClient, auth_headers: dict) -> None:
    r = _resolve(client, auth_headers, REAL, "prod")
    assert r.status_code == 200, r.text
    assert r.json() == {"agent": "acme-bot", "env": "prod", "slack_channel": "C000000A02"}


def test_each_target_resolves_differently(client: TestClient, auth_headers: dict) -> None:
    dev = _resolve(client, auth_headers, REAL, "dev").json()
    prod = _resolve(client, auth_headers, REAL, "prod").json()
    assert dev["agent"] != prod["agent"]
    assert dev["env"] == "dev" and prod["env"] == "prod"


def test_an_unknown_target_404s_and_lists_what_exists(
    client: TestClient, auth_headers: dict
) -> None:
    # Naming a target that is not declared must not fall back to a default --
    # that would deploy somewhere the caller did not ask for.
    r = _resolve(client, auth_headers, REAL, "staging")
    assert r.status_code == 404
    assert "dev, prod" in r.json()["detail"]


def test_a_validation_error_is_returned_not_swallowed(
    client: TestClient, auth_headers: dict
) -> None:
    # Each of these describes a deploy that would otherwise SUCCEED against the
    # wrong agent, environment, or channel.
    r = _resolve(client, auth_headers, "targets:\n  p:\n    env: staging\n", "p")
    assert r.status_code == 400
    assert "deploy.bad_env" in r.json()["detail"]


@pytest.mark.parametrize("endpoint", ["resolve", "list"])
def test_documentation_placeholder_channel_is_rejected(
    client: TestClient, auth_headers: dict, endpoint: str
) -> None:
    content = (
        "targets:\n"
        "  dev:\n"
        "    agent: acme-dev\n"
        "    env: dev\n"
        "    slack_channel: C0EXAMPLE1\n"
    )
    r = client.post(
        f"/deploy-targets/{endpoint}",
        json={"content": content, "target": "dev"},
        headers=auth_headers,
    )
    assert r.status_code == 400, r.text
    assert "deploy.placeholder_channel" in r.json()["detail"]
    assert "C0EXAMPLE1" in r.json()["detail"]


@pytest.mark.parametrize(
    "content",
    [
        "targets:\n  p:\n    env: prod\n",
        "targets:\n  p:\n    agent: null\n    env: prod\n",
    ],
    ids=["omitted", "explicit_null"],
)
def test_resolve_and_list_name_a_target_with_no_agent(
    client: TestClient, auth_headers: dict, content: str
) -> None:
    resolved = _resolve(client, auth_headers, content, "p")
    listed = client.post(
        "/deploy-targets/list",
        json={"content": content, "target": "p"},
        headers=auth_headers,
    )
    assert resolved.status_code == listed.status_code == 400
    assert resolved.json()["detail"] == listed.json()["detail"]
    detail = resolved.json()["detail"]
    assert "deploy.missing_agent" in detail
    assert "targets.p" in detail
    assert "None" not in detail


def test_unparseable_yaml_is_a_400_naming_the_problem(
    client: TestClient, auth_headers: dict
) -> None:
    r = _resolve(client, auth_headers, "targets:\n  p:\n   env: [unclosed\n", "p")
    assert r.status_code == 400
    assert "unparseable" in r.json()["detail"]


def test_explicit_mapping_tag_on_a_scalar_is_a_400(
    client: TestClient, auth_headers: dict
) -> None:
    r = _resolve(client, auth_headers, "targets: !!map foo\n", "prod")
    assert r.status_code == 400
    assert "unparseable" in r.json()["detail"]


def test_duplicate_target_name_is_a_400_naming_the_duplicate(
    client: TestClient, auth_headers: dict
) -> None:
    content = (
        "targets:\n"
        "  prod:\n"
        "    agent: first\n"
        "    env: prod\n"
        "    slack_channel: C0EXAMPLE1\n"
        "  prod:\n"
        "    agent: second\n"
        "    env: prod\n"
        "    slack_channel: C0EXAMPLE2\n"
    )
    r = _resolve(client, auth_headers, content, "prod")
    assert r.status_code == 400, r.text
    assert "deploy.duplicate_target" in r.json()["detail"]
    assert "prod" in r.json()["detail"]


# --------------------------------------------------------------------------- #
# Listing every target (onboarding)
# --------------------------------------------------------------------------- #
_TWO_TARGETS = """
targets:
  prod: { agent: my-bot,     env: prod, slack_channel: C000000A01 }
  dev:  { agent: my-bot-dev, env: dev,  slack_channel: C000000A02 }
"""


def test_list_returns_every_target(client: TestClient, auth_headers: dict) -> None:
    # Onboarding a repository means deploying ALL its targets. A caller that
    # had to parse deploy.yaml to enumerate them would be the second parser
    # ADR-0089 put this endpoint here to prevent.
    r = client.post(
        "/deploy-targets/list",
        json={"content": _TWO_TARGETS, "target": "ignored"},
        headers=auth_headers,
    )
    assert r.status_code == 200
    assert {t["agent"] for t in r.json()["targets"]} == {"my-bot", "my-bot-dev"}


def test_list_orders_dev_before_prod(client: TestClient, auth_headers: dict) -> None:
    # A sequential onboarding run that fails part-way must leave prod BEHIND,
    # not ahead of a dev that never landed. The file declares prod first, so
    # this is ordering rather than luck.
    r = client.post(
        "/deploy-targets/list", json={"content": _TWO_TARGETS, "target": ""}, headers=auth_headers
    )
    assert [t["env"] for t in r.json()["targets"]] == ["dev", "prod"]


def test_list_carries_the_declared_name(client: TestClient, auth_headers: dict) -> None:
    r = client.post(
        "/deploy-targets/list", json={"content": _TWO_TARGETS, "target": ""}, headers=auth_headers
    )
    assert {t["name"] for t in r.json()["targets"]} == {"dev", "prod"}


def test_list_and_resolve_agree_on_an_invalid_file(client: TestClient, auth_headers: dict) -> None:
    # Both go through one parser. If they diverged, a file could list cleanly
    # and resolve as broken -- a second parser by the back door.
    bad = "targets: {dev: {agent: x, env: nonsense, slack_channel: C000000B01}}"
    a = client.post(
        "/deploy-targets/list", json={"content": bad, "target": "dev"}, headers=auth_headers
    )
    b = client.post(
        "/deploy-targets/resolve", json={"content": bad, "target": "dev"}, headers=auth_headers
    )
    assert a.status_code == b.status_code == 400
    assert a.json()["detail"] == b.json()["detail"]


def test_list_of_an_empty_file_is_empty_not_an_error(
    client: TestClient, auth_headers: dict
) -> None:
    # Nothing declared is a legitimate state for a bundle that predates
    # deploy.yaml; the caller decides what to do about it.
    r = client.post(
        "/deploy-targets/list", json={"content": "targets: {}", "target": ""}, headers=auth_headers
    )
    assert r.status_code == 200
    assert r.json()["targets"] == []
