"""The recorded base across relabels, refusals and finalized comments (#3095, ADR 0186).

Review round 2: a relabel must not move the base under a running execution, a
refused readmission must not write a base, and an ignored relabel must reach a
finalized status comment.

GitHub issue label event identities follow:
https://docs.github.com/en/rest/issues/events
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from forge_fakes.github import LABEL, REPO_ID, _issue_event, _post
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: F401  (fixtures)
from test_factory_status_comment import _marked
from test_factory_terminus import (  # noqa: F401  (fixtures)
    REPO,
    _observe_termination,
    _published_issue,
    _reconcile,
    _start_running,
)
from test_github_factory_ingress import _code

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}
TRAIN = {REPO: {"bases": ["main", "next"], "default_base": "main"}}


def _train(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_FACTORY_BASES", json.dumps(TRAIN))
    get_settings.cache_clear()


def _labelled(client: Any, github: Any, number: int, *labels: str) -> Any:
    github.issue_number = number
    github.labels = [LABEL, *labels]
    github.advance_label_event(number)
    return _post(
        client,
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        delivery=str(uuid.uuid4()),
    )


def _item(number: int) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT id, agent_id, conversation_id, base_branch, base_source, base_commit, "
        "base_label_ignored FROM curie.work_items "
        "WHERE github_repository_id = :repo AND github_issue_number = :number",
        {"repo": REPO_ID, "number": number},
    )
    return row


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.status FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.github_issue_number = :number ORDER BY r.sequence",
        {"number": number},
    )


def _redeemed_base(client: Any, number: int, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Any]:
    """What credential redemption returns for the WorkItem's conversation."""

    monkeypatch.setattr(
        "curie_api.forges.github.code_host.resolve_repository_credential",
        lambda _repo, _settings: (
            f"https://github.com/{REPO}.git",
            "Basic eC1hY2Nlc3MtdG9rZW46Zml4dHVyZQ==",  # x-access-token:fixture
        ),
    )
    item = _item(number)
    headers = {"X-API-Key": get_settings().api_key}
    version = client.post(
        f"/agents/{item['agent_id']}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "operator"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        json={
            "agent_id": str(item["agent_id"]),
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text
    deployment_id = deployment.json()["id"]
    selected = client.post(
        f"/v1/internal/workspaces/{deployment_id}/selection",
        json={
            "conversation_id": item["conversation_id"],
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER,
    )
    assert selected.status_code == 200, selected.text
    issued = client.post(
        f"/v1/internal/workspaces/{deployment_id}/credential",
        json={"conversation_id": item["conversation_id"]},
        headers=WORKER,
    )
    assert issued.status_code == 200, issued.text
    return issued.json()["base_branch"], issued.json()["base_commit"]


def test_a_relabel_during_a_running_execution_keeps_its_base_until_the_replacement_admits(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch)
    number = 30961
    assert _labelled(client, github, number, "base:next").json()["status"] == "factory_admitted"
    before = _item(number)
    assert (before["base_branch"], before["base_source"]) == ("next", "label")
    old_id = _requests(number)[0]["id"]
    _start_running(old_id)

    again = _labelled(client, github, number)

    assert again.json()["status"] == "factory_readmit_pending", again.text
    during = _item(number)
    assert (during["base_branch"], during["base_source"], during["base_commit"]) == (
        "next",
        "label",
        before["base_commit"],
    )
    assert _redeemed_base(client, number, monkeypatch) == ("next", before["base_commit"])

    _observe_termination(client, old_id)
    _reconcile()

    assert [row["status"] for row in _requests(number)] == ["cancelled", "waiting"]
    after = _item(number)
    assert (after["base_branch"], after["base_source"]) == ("main", "default")
    assert after["base_label_ignored"] is None


def test_a_readmission_refused_for_identity_mismatch_leaves_the_base_unchanged(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch)
    number = 30962
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    before = _item(number)
    assert (before["base_branch"], before["base_source"]) == ("main", "default")

    # The repository's binding moves to another agent.
    headers = {"X-API-Key": get_settings().api_key}
    moved = client.patch(
        f"/agents/{before['agent_id']}/channels",
        params={"kind": "github", "address": REPO},
        headers=headers,
        json={"kind": "github", "address": "acme-corp/elsewhere"},
    )
    assert moved.status_code == 200, moved.text
    created = client.post(
        "/agents",
        headers=headers,
        json={
            "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
            "repo_full_name": REPO,
            "channel": {"kind": "github", "address": REPO},
        },
    )
    assert created.status_code == 201, created.text

    refused = _labelled(client, github, number, "base:next")

    assert _code(refused) == "identity_mismatch"
    after = _item(number)
    assert after["agent_id"] == before["agent_id"]
    assert (
        after["base_branch"],
        after["base_source"],
        after["base_commit"],
        after["base_label_ignored"],
    ) == (
        before["base_branch"],
        before["base_source"],
        before["base_commit"],
        before["base_label_ignored"],
    )


def test_an_ignored_relabel_re_renders_a_finalized_status_comment(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, github, sink = admitted
    _train(monkeypatch)
    sink.by_path = True
    number, _pr, first = _published_issue(client, github, sink)
    (comment,) = _marked(sink, first["id"])
    assert "Base: `main` (deployment default)" in comment["body"]
    assert "Label now says" not in comment["body"]

    github.labels = [LABEL, "base:next"]
    changed = _post(client, "issues", _issue_event("labeled", number, label={"name": "base:next"}))
    assert _code(changed) == "base_label_recorded"
    sink.requests.clear()

    _reconcile()

    (comment,) = _marked(sink, first["id"])
    assert (
        "Base: `main` (deployment default) "
        "Label now says `base:next`; the recorded base is kept."
    ) in comment["body"]
    assert any(method == "PATCH" for method, _path, _body in sink.requests)
