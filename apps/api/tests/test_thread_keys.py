"""The API builds the worker's thread key in one place (ADR-0168 decision 4)."""

from __future__ import annotations

import asyncio
import io
import tokenize
import uuid
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from channel_protocol import scoped_conversation_id
from curie_api.config import get_settings
from curie_api.threadkeys import (
    pre_identity_thread_key,
    route_thread_key,
    route_thread_key_matches,
)
from curie_api.workitem_dispatch import admit, readmit
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ADDRESS = "agent@example.test"
ADAPTER = "agentmail-sandbox"
ENDPOINT = "http://curie-mail-adapter:8080/"
THREAD = "thread/9"
OLD_KEY = scoped_conversation_id("email", ADDRESS, THREAD)
NEW_KEY = scoped_conversation_id("email", ADDRESS, THREAD, identity=ADAPTER)
SLACK_TS = "1700000000.000100"


def test_route_thread_key_resolves_the_route_identity() -> None:
    assert (
        route_thread_key("slack", None, "C0EXAMPLE1", SLACK_TS) == f"slack:C0EXAMPLE1:{SLACK_TS}"
    )
    assert (
        route_thread_key("slack", "default", "C0EXAMPLE1", SLACK_TS)
        == f"slack:C0EXAMPLE1:{SLACK_TS}"
    )
    assert (
        route_thread_key("slack", "second-bot", "C0EXAMPLE1", SLACK_TS)
        == f"slack:second-bot:C0EXAMPLE1:{SLACK_TS}"
    )
    assert route_thread_key("email", ADAPTER, ADDRESS, THREAD) == NEW_KEY
    assert route_thread_key("email", None, ADDRESS, THREAD) == OLD_KEY


def test_only_a_named_non_slack_route_has_a_pre_identity_key() -> None:
    assert pre_identity_thread_key("email", ADAPTER, ADDRESS, THREAD) == OLD_KEY
    assert pre_identity_thread_key("email", None, ADDRESS, THREAD) is None
    assert pre_identity_thread_key("slack", None, "C0EXAMPLE1", SLACK_TS) is None
    assert pre_identity_thread_key("slack", "second-bot", "C0EXAMPLE1", SLACK_TS) is None


def test_a_stored_key_matches_its_route_in_either_form_but_no_other() -> None:
    assert route_thread_key_matches("email", ADAPTER, ADDRESS, THREAD, NEW_KEY)
    assert route_thread_key_matches("email", ADAPTER, ADDRESS, THREAD, OLD_KEY)
    assert not route_thread_key_matches("email", "other-inbox", ADDRESS, THREAD, NEW_KEY)
    # A named Slack identity must never accept the default app's thread.
    assert not route_thread_key_matches(
        "slack", "second-bot", "C0EXAMPLE1", SLACK_TS, f"slack:C0EXAMPLE1:{SLACK_TS}"
    )


def test_only_threadkeys_builds_a_thread_key_in_the_api() -> None:
    """Names in code only: a comment or docstring naming the builder is fine."""
    src = Path(__file__).resolve().parents[1] / "src" / "curie_api"
    offenders = []
    for path in sorted(src.rglob("*.py")):
        if path.name == "threadkeys.py":
            continue
        tokens = tokenize.generate_tokens(io.StringIO(path.read_text()).readline)
        if any(t.type == tokenize.NAME and t.string == "scoped_conversation_id" for t in tokens):
            offenders.append(path.relative_to(src).as_posix())
    assert offenders == []


# --- admission stores the worker's key -----------------------------------------


def _with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        # `expire_on_commit=False`, matching `db.py`'s own session factory: a
        # bare `AsyncSession(engine)` expires every attribute on commit, and
        # `readmit`'s own internal commit then makes its later `work_item.id`
        # read (`workitem_dispatch.py::readmit`) refuse outside a greenlet.
        maker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with maker() as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _mail_agent(session: AsyncSession) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text("INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)"),
        {"id": agent_id, "name": f"acme-mail-{agent_id.hex[:8]}", "repo": "acme-corp/acme-bot"},
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter, endpoint) "
            "VALUES (:id, :agent_id, 'email', :address, :adapter, :endpoint)"
        ),
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "address": ADDRESS,
            "adapter": ADAPTER,
            "endpoint": ENDPOINT,
        },
    )
    await session.commit()
    return agent_id


def _facts(agent_id: uuid.UUID, **overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "agent_id": agent_id,
        "kind": "email",
        "address": ADDRESS,
        "reply_conversation_id": THREAD,
        "repo_full_name": "acme-corp/acme-bot",
        "github_repository_id": 101,
        "github_issue_number": 3104,
        "github_installation_id": 202,
        "objective": "Implement the admitted work item",
        "requester": "U0REQUEST1",
        "request_id": uuid.uuid4(),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_admission_keys_a_mail_work_item_by_its_identity(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _mail_agent(session)
        admitted = await admit(session, _facts(agent_id))
        stored = await session.scalar(
            text("SELECT conversation_id FROM curie.work_items WHERE id = :id"),
            {"id": admitted.work_item.id},
        )
        assert stored == NEW_KEY

    _with_session(body)


# --- readmission accepts the pre-identity key ------------------------------------
#
# `WorkItem.conversation_id` is immutable once written -- migration 0046's
# `enforce_work_items_update_invariants` trigger refuses any UPDATE that
# touches it (pinned by `test_migration_0046_work_items.py`), so a work item
# "admitted before the identity" cannot be built by admitting one under the
# current code and then rewriting its key. It is seeded directly under the
# old key instead: the row shape a pre-decision-4 admission actually left
# behind, since admission itself can no longer produce one.


async def _legacy_mail_work_item(session: AsyncSession) -> tuple[uuid.UUID, uuid.UUID, int]:
    agent_id = await _mail_agent(session)
    facts = _facts(agent_id)
    work_item_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.work_items "
            "(id, github_repository_id, github_issue_number, github_installation_id, "
            "agent_id, repo_full_name, conversation_id) "
            "VALUES (:id, :repo_id, :issue, :install, :agent_id, :repo, :conversation_id)"
        ),
        {
            "id": work_item_id,
            "repo_id": facts.github_repository_id,
            "issue": facts.github_issue_number,
            "install": facts.github_installation_id,
            "agent_id": agent_id,
            "repo": facts.repo_full_name,
            "conversation_id": OLD_KEY,
        },
    )
    await session.commit()
    version = await session.scalar(
        text("SELECT version FROM curie.work_items WHERE id = :id"), {"id": work_item_id}
    )
    assert version is not None
    return agent_id, work_item_id, version


def test_readmit_accepts_a_work_item_admitted_before_the_identity(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, _work_item_id, _version = await _legacy_mail_work_item(session)
        again = await readmit(session, _facts(agent_id))
        assert getattr(again, "code", None) != "identity_mismatch", again

    _with_session(body)
