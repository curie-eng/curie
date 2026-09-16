"""`POST /git-flow/routing-check`: can this repository's pushes still route? (#1221)

Migration 0018 (ADR-0091) dropped the unique index on `agents.repo_full_name`,
so binding a SECOND agent to a repository is legal -- and, with no declared
targets, silently flips every future push for the agent that was ALREADY bound
from "deploys" to "rejected". The regression test below is the two-agent case;
the rest fence it so the warning cannot become noise on the configuration
ADR-0091 actually asks operators to adopt.

Real Postgres round-trip (the disposable-DB conftest), because the answer is a
function of the rows: a pure-function test could not catch a lookup that stopped
seeing a sibling agent.
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.config import get_settings

REPO = "octo/routing-check"

# The ADR-0091 intended configuration: two agents, one target each. Nothing here
# is ambiguous, so nothing should warn.
DECLARED = """
targets:
  dev:
    agent: routing-dev
    env: dev
    slack_channel: C000000A01
  prod:
    agent: routing-prod
    env: prod
    slack_channel: C000000A02
"""


def _bind(client: Any, headers: dict[str, str], name: str, address: str, repo: str = REPO) -> None:
    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": address},
            "repo_full_name": repo,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text


def _check(client: Any, headers: dict[str, str], content: str | None = None) -> Any:
    body: dict[str, Any] = {"repo_full_name": REPO}
    if content is not None:
        body["content"] = content
    return client.post("/git-flow/routing-check", json=body, headers=headers)


def test_one_bound_agent_with_no_deploy_yaml_still_routes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The pre-#1221 status quo, and the state every single-agent repository is
    # in: one agent, no deploy.yaml, and pushes that deploy.
    _bind(client, auth_headers, "routing-solo", "C0EXAMPLE3")

    r = _check(client, auth_headers)
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["resolvable"] is True
    assert payload["agent_count"] == 1
    assert payload["agents"] == ["routing-solo"]
    assert payload["unresolvable"] == []


def test_binding_a_second_agent_makes_every_push_unroutable(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """THE regression test for #1221.

    Binding the second agent does not break only the new one: with nothing to
    say which agent a branch deploys to, the resolver rejects, so the agent that
    was already working stops deploying too. That is the silence this endpoint
    exists to end.
    """

    _bind(client, auth_headers, "routing-dev", "C0EXAMPLE3")
    _bind(client, auth_headers, "routing-prod", "C0EXAMPLE4")

    r = _check(client, auth_headers)
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["resolvable"] is False
    assert payload["agent_count"] == 2
    assert sorted(payload["agents"]) == ["routing-dev", "routing-prod"]

    problems = {p["environment"]: p for p in payload["unresolvable"]}
    assert sorted(problems) == ["dev", "prod"], payload
    for problem in problems.values():
        assert problem["code"] == "deploy.no_targets"
        # The resolver's own words, so the caller never restates the rule.
        assert "2 agents are built from this repository" in problem["message"]


def test_two_agents_with_declared_targets_do_not_warn(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The negative case, and the reason this reports the RESOLVER's answer
    # rather than an agent count: several agents per repository is what ADR-0091
    # made possible, and a warning on the supported shape is pure noise.
    _bind(client, auth_headers, "routing-dev", "C0EXAMPLE3")
    _bind(client, auth_headers, "routing-prod", "C0EXAMPLE4")

    r = _check(client, auth_headers, DECLARED)
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["resolvable"] is True, payload
    assert payload["unresolvable"] == []
    assert payload["agent_count"] == 2


def test_an_empty_targets_map_is_the_same_as_no_deploy_yaml(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #1210: what decides the fallback is whether a target is DECLARED, not
    # whether the file exists. `curie init` scaffolds an empty map, so this is
    # the ordinary case rather than a corner one.
    _bind(client, auth_headers, "routing-dev", "C0EXAMPLE3")
    _bind(client, auth_headers, "routing-prod", "C0EXAMPLE4")

    r = _check(client, auth_headers, "targets: {}\n")
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["resolvable"] is False
    codes = {p["code"] for p in payload["unresolvable"]}
    assert codes == {"deploy.no_targets"}
    assert all("empty `targets:` map" in p["message"] for p in payload["unresolvable"])


def test_an_unbound_repository_is_reported_resolvable(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Nothing bound is not a routing FAULT -- it is the state before the first
    # deploy. This endpoint reports routing, not binding existence.
    r = _check(client, auth_headers)
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["agent_count"] == 0
    assert payload["agents"] == []
    assert payload["resolvable"] is True


def test_the_endpoint_requires_the_platform_api_key(client: Any, clean_db: None) -> None:
    # Same auth as every other router (apps/api/CLAUDE.md): one shared key.
    assert client.post("/git-flow/routing-check", json={"repo_full_name": REPO}).status_code == 401
    wrong = client.post(
        "/git-flow/routing-check",
        json={"repo_full_name": REPO},
        headers={"X-API-Key": "wrong"},
    )
    assert wrong.status_code == 401


def test_malformed_deploy_yaml_is_the_same_400_as_the_resolver(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Proof that this endpoint shares `/deploy-targets/resolve`'s parser rather
    # than carrying a second one: an identical file must fail identically. A
    # divergence here is ADR-0089's forbidden second parser reappearing.
    bad = "targets:\n  p:\n   env: [unclosed\n"
    mine = _check(client, auth_headers, bad)
    theirs = client.post(
        "/deploy-targets/resolve", json={"content": bad, "target": "p"}, headers=auth_headers
    )
    assert mine.status_code == theirs.status_code == 400, mine.text
    assert mine.json()["detail"] == theirs.json()["detail"]
    assert "unparseable" in mine.json()["detail"]


def test_a_same_branch_configuration_only_reports_the_reachable_environment(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable environment must not be warned about (#1221).

    ``gitflow.environment_for_ref`` compares a ref against ``dev_branch``
    FIRST, so when both deploy branches are the same branch every push to it
    becomes `dev` and NO push can ever reach the prod resolver. Reporting a
    prod problem here would warn an operator about a push their configuration
    makes impossible -- the client/server divergence this endpoint exists to
    remove. `get_settings` is `lru_cache`-d, so the cache must be cleared after
    the env change (and on teardown) or the override silently does nothing.
    """

    monkeypatch.setenv("DEV_BRANCH", "main")
    monkeypatch.setenv("PROD_BRANCH", "main")
    get_settings.cache_clear()
    try:
        # Two agents and no declared targets: the resolver rejects for BOTH
        # environments, so prod would appear here if it were still evaluated.
        _bind(client, auth_headers, "routing-dev", "C0EXAMPLE5")
        _bind(client, auth_headers, "routing-prod", "C0EXAMPLE6")

        r = _check(client, auth_headers)
        assert r.status_code == 200, r.text
        payload = r.json()
        assert payload["resolvable"] is False, payload
        assert [p["environment"] for p in payload["unresolvable"]] == ["dev"], payload
        assert payload["unresolvable"][0]["code"] == "deploy.no_targets"
    finally:
        get_settings.cache_clear()
