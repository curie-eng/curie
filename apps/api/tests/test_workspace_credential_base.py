"""The workspace credential carries a factory WorkItem's recorded base (#3095, ADR 0186).

The worker clones `base_branch` and pins `base_commit`, so the sandbox starts
from the base frozen at admission rather than the repository default branch.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_workspace_control_plane import (  # noqa: F401  (fixture)
    REPO,
    WORKER_HEADERS,
    _create_agent_version,
    _deploy,
    worker_client,
)

BASE_COMMIT = "c" * 40


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _select(client: TestClient, deployment_id: str, conversation_id: str) -> None:
    selected = client.post(
        f"/v1/internal/workspaces/{deployment_id}/selection",
        json={"conversation_id": conversation_id, "author": "U0REQUEST1", "repo_full_name": REPO},
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text


def _work_item(agent_id: str, conversation_id: str, number: int, **base: Any) -> None:
    columns = "".join(f", {name}" for name in base)
    values = "".join(f", :{name}" for name in base)
    _execute(
        "INSERT INTO curie.work_items (id, tracker_kind, tracker_host, tracker_scope_id, "
        "tracker_issue_id, code_host_kind, code_host_host, repository_project_id, "
        f"repository_path, code_host_installation_id, agent_id, conversation_id{columns}) "
        "VALUES (:id, 'github', 'github.com', '4401', :number, 'github', 'github.com', "
        f"'4401', :repo, 5501, :agent, :conversation{values})",
        {
            "id": uuid.uuid4(),
            "number": str(number),
            "agent": agent_id,
            "repo": REPO,
            "conversation": conversation_id,
            **base,
        },
    )


def _redeem(client: TestClient, deployment_id: str, conversation_id: str) -> dict[str, Any]:
    response = client.post(
        f"/v1/internal/workspaces/{deployment_id}/credential",
        json={"conversation_id": conversation_id},
        headers=WORKER_HEADERS,
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


def test_a_factory_conversation_redeems_its_recorded_base(
    worker_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    _select(worker_client, deployment["id"], "issue-3095")
    _work_item(
        agent_id,
        "issue-3095",
        3095,
        base_branch="next",
        base_source="label",
        base_commit=BASE_COMMIT,
    )

    issued = _redeem(worker_client, deployment["id"], "issue-3095")

    assert issued["base_branch"] == "next"
    assert issued["base_commit"] == BASE_COMMIT
    assert issued["repo_full_name"] == REPO


def test_a_non_factory_or_legacy_conversation_redeems_no_base(
    worker_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    _select(worker_client, deployment["id"], "slack-thread-1")
    _select(worker_client, deployment["id"], "issue-3096")
    _work_item(agent_id, "issue-3096", 3096)

    plain = _redeem(worker_client, deployment["id"], "slack-thread-1")
    legacy = _redeem(worker_client, deployment["id"], "issue-3096")

    for issued in (plain, legacy):
        assert "base_branch" in issued
        assert "base_commit" in issued
        assert issued["base_branch"] is None
        assert issued["base_commit"] is None
