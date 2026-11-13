"""GitHub REST shapes from the documented issue comment and permission endpoints.

https://docs.github.com/en/rest/issues/comments
https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
Only GitHub HTTP is substituted; sessions and observations use real Postgres.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
from curie_api.config import Settings, get_settings
from curie_api.factory_facts_schema import REQUEST_MARKER
from curie_api.factory_poll_intake import poll_once
from curie_api.models import Agent, AgentChannel
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from forge_fakes.github import LABEL, REPO


@dataclass
class FactsWorld:
    sessions: async_sessionmaker[Any]
    engine: AsyncEngine
    settings: Settings
    github: Any
    agent_id: uuid.UUID
    observation_reads: list[str] = field(default_factory=list)
    fail_write: bool = False
    lose_write: bool = False
    comment_reads: int = 0
    mutate_on_read: Any = None
    permissions: dict[str, str] = field(default_factory=dict)
    permission_failure: bool = False
    pull_requests: set[int] = field(default_factory=set)

    def add(
        self,
        identifier: int = 101,
        *,
        number: int = 42,
        label: str = LABEL,
        user: Any = None,
        app: Any = None,
        executables: list[str] | None = None,
        services: list[str] | None = None,
    ) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "trigger_label": label,
            "executables": executables if executables is not None else ["uv", "g++", "python3.13"],
            "services": services if services is not None else ["postgres"],
        }
        comment = self.github.add_issue_comment(
            number,
            identifier,
            "@curie-factory-bot\n\n" + REQUEST_MARKER + "\n```json\n" + json.dumps(value) + "\n```",
        )
        comment["user"] = user if user is not None else {"id": 77, "login": "dana", "type": "User"}
        comment["performed_via_github_app"] = app
        self.github._etags["issue-comments"] = (
            f'"comments-{len(self.github.comments)}-{identifier}"'
        )
        return comment

    def handle(self, request: httpx.Request) -> httpx.Response:
        root = f"/repos/{REPO}"
        path = request.url.path
        if (
            request.method == "GET"
            and path.startswith(root + "/issues/")
            and path.removeprefix(root + "/issues/").isdigit()
        ):
            number = int(path.rsplit("/", 1)[1])
            self.github.calls.append(request)
            issue: dict[str, Any] = {"number": number, "state": "open", "labels": []}
            if number in self.pull_requests:
                issue["pull_request"] = {"url": f"https://api.github.com{root}/pulls/{number}"}
            return httpx.Response(200, json=issue)
        if "/collaborators/" in path:
            self.github.calls.append(request)
            if self.permission_failure:
                return httpx.Response(503)
            login = path.split("/")[-2]
            user = next(
                (
                    c["user"]
                    for c in self.github.comments.values()
                    if c.get("user", {}).get("login") == login
                ),
                {"id": 77},
            )
            role = self.permissions.get(login, "write")
            permission = "write" if role == "maintain" else "read" if role == "triage" else role
            return httpx.Response(
                200, json={"permission": permission, "role_name": role, "user": user}
            )
        if path.startswith(root + "/issues/comments/") and request.method == "GET":
            self.comment_reads += 1
            if self.mutate_on_read is not None:
                self.mutate_on_read(self)
        if path.endswith("/comments") and request.method == "POST":
            if self.fail_write:
                return httpx.Response(503)
            result = self.github.handle(request)
            if self.lose_write:
                self.lose_write = False
                raise httpx.ReadError("lost fixture response", request=request)
            return result
        return self.github.handle(request)

    async def poll(self) -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(self.handle)) as client:
            await poll_once(self.sessions, self.settings, client)

    def cycle(self) -> None:
        asyncio.run(self.poll())

    def answers(self, number: int = 42) -> list[dict[str, Any]]:
        return [
            c
            for c in self.github.comments.values()
            if c.get("performed_via_github_app", {}) == {"id": 51, "slug": "curie"}
            and c["issue_url"].endswith(f"/issues/{number}")
            and "<!-- curie-factory-facts:v1 " in c["body"]
        ]

    async def rows(self, table: str) -> list[Any]:
        assert table in {
            "factory_facts_requests",
            "work_items",
            "execution_requests",
            "factory_poll_cursors",
            "publication_review_reservations",
            "remediation_reservations",
        }
        async with self.sessions() as session:
            return list((await session.execute(text(f"SELECT * FROM curie.{table}"))).mappings())


@pytest.fixture
def facts_world(monkeypatch: pytest.MonkeyPatch, tmp_path: Any, clean_db: None) -> Any:
    from test_factory_poll_intake import PollGitHub, _patch_credentials, _poll_env

    for key, value in _poll_env(tmp_path, "poll").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CURIE_INSTALLATION_ID", f"facts-{uuid.uuid4().hex}")
    get_settings.cache_clear()
    _patch_credentials(monkeypatch)
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    agent_id = uuid.uuid4()
    world = FactsWorld(sessions, engine, get_settings(), PollGitHub(), agent_id)

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def observe_queries(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: Any
    ) -> None:
        if "factory_fact_observations" in statement and statement.lstrip().upper().startswith(
            "SELECT"
        ):
            world.observation_reads.append(statement)

    async def seed() -> None:
        async with sessions() as session:
            await session.execute(text("TRUNCATE curie.factory_facts_requests"))
            session.add(
                Agent(id=agent_id, name=f"acme-facts-{agent_id.hex[:8]}", max_usd_per_day=12345.67)
            )
            await session.flush()
            session.add(AgentChannel(agent_id=agent_id, kind="github", address=REPO))
            await session.commit()

    asyncio.run(seed())
    try:
        yield world
    finally:
        asyncio.run(engine.dispose())
        get_settings.cache_clear()
