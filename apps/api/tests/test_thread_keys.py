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
from curie_api import crud
from curie_api.config import get_settings
from curie_api.threadkeys import (
    pre_identity_thread_key,
    route_thread_key,
    route_thread_key_matches,
)
from curie_api.workitem_dispatch import (
    admit,
    cancel,
    claim_terminate_publishes,
    readmit,
    running_for_conversation,
)
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


# --- pre-identity work items still fence their thread ---------------------------


def test_a_continuation_finds_a_work_item_keyed_before_the_identity(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        await _legacy_mail_work_item(session)
        state, _ = await running_for_conversation(session, NEW_KEY)
        # Waiting, not running: found, so "ended", never "absent".
        assert state == "ended"

    _with_session(body)


def test_a_cancelled_work_item_keyed_before_the_identity_still_fences_publication(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, work_item_id, version = await _legacy_mail_work_item(session)
        cancelled = await cancel(
            session, work_item_id=work_item_id, expected_version=version
        )
        assert getattr(cancelled, "work_item", None) is not None, cancelled
        conflict = await crud.publication_cancellation_conflict(
            session, agent_id=agent_id, conversation_id=NEW_KEY
        )
        assert conflict is not None

    _with_session(body)


def test_a_named_slack_key_never_finds_the_default_apps_work_item(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = uuid.uuid4()
        await session.execute(
            text("INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :n, :r)"),
            {"id": agent_id, "n": f"acme-slack-{agent_id.hex[:8]}", "r": "acme-corp/acme-bot"},
        )
        await session.execute(
            text(
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
                "VALUES (:id, :a, 'slack', 'C0EXAMPLE1')"
            ),
            {"id": uuid.uuid4(), "a": agent_id},
        )
        await session.commit()
        await admit(
            session,
            _facts(agent_id, kind="slack", address="C0EXAMPLE1", reply_conversation_id=SLACK_TS),
        )
        named = scoped_conversation_id("slack", "C0EXAMPLE1", SLACK_TS, identity="second-bot")
        state, _ = await running_for_conversation(session, named)
        assert state == "absent"

    _with_session(body)


def test_a_continuation_does_not_find_a_legacy_work_item_under_a_different_current_adapter(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        await _legacy_mail_work_item(session)
        # The pair's binding is bound under ADAPTER, not this one: the old
        # key can only be ITS route's, so a lookup implying another route's
        # identity must not adopt it.
        other = scoped_conversation_id("email", ADDRESS, THREAD, identity="other-inbox")
        state, _ = await running_for_conversation(session, other)
        assert state == "absent"

    _with_session(body)


def test_a_cancelled_legacy_work_item_still_fences_publication_after_its_binding_is_deleted(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, work_item_id, version = await _legacy_mail_work_item(session)
        cancelled = await cancel(
            session, work_item_id=work_item_id, expected_version=version
        )
        assert getattr(cancelled, "work_item", None) is not None, cancelled
        # `thread_key_forms`'s single-binding guard would refuse to adopt the
        # old key once the binding it names is gone; the refusal lookup must
        # not depend on that guard, or the fence fails open right when a
        # cancelled work item's credential most needs refusing.
        await session.execute(
            text("DELETE FROM curie.agent_channels WHERE agent_id = :id"), {"id": agent_id}
        )
        await session.commit()
        conflict = await crud.publication_cancellation_conflict(
            session, agent_id=agent_id, conversation_id=NEW_KEY
        )
        assert conflict is not None

    _with_session(body)


# --- the terminate wake for a legacy work item keys its execute wake's thread ---


def test_a_legacy_mail_work_items_terminate_wake_keys_its_execute_wakes_thread(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, _work_item_id, _version = await _legacy_mail_work_item(session)
        readmitted = await readmit(session, _facts(agent_id))
        assert getattr(readmitted, "code", None) != "identity_mismatch", readmitted
        assert readmitted.request is not None
        request_id = readmitted.request.id
        # The reconciler's execute wake would resolve THIS request's live
        # binding and key its thread `NEW_KEY` (`_execute_turn`,
        # `load_execute_wake`); simulate it having run and needing to stop.
        await session.execute(
            text(
                "UPDATE curie.execution_requests e SET "
                "status = 'cancellation_requested', "
                "started_at = s.ts, "
                "execution_deadline = s.ts + interval '1800 seconds', "
                "execution_attempts = 1, "
                "terminal_cause = 'owner_lost', "
                "runtime_owner = NULL, "
                "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                "version = version + 1 "
                "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                "WHERE e.id = :id"
            ),
            {"id": request_id},
        )
        await session.commit()
        published = await claim_terminate_publishes(session, retry_seconds=0, limit=10)
        (item,) = [p for p in published if p.request_id == request_id]
        assert item.reply_adapter == ADAPTER
        assert (
            route_thread_key(
                item.reply_kind, item.reply_adapter, item.reply_address, item.reply_conversation_id
            )
            == NEW_KEY
        )

    _with_session(body)


# --- the other two work-item lookups also try the pre-identity key -------------


def test_refuse_fenced_work_item_still_fences_a_cancelled_legacy_work_item(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, work_item_id, version = await _legacy_mail_work_item(session)
        cancelled = await cancel(
            session, work_item_id=work_item_id, expected_version=version
        )
        assert getattr(cancelled, "work_item", None) is not None, cancelled
        with pytest.raises(crud.PublicationLineageConflict) as caught:
            await crud._refuse_fenced_work_item(
                session,
                agent_id=agent_id,
                conversation_id=NEW_KEY,
                request_id=None,
                runtime_epoch=None,
            )
        assert caught.value.code == "publication.work_item_cancelled"

    _with_session(body)


async def _bare_lineage(session: AsyncSession, agent_id: uuid.UUID) -> uuid.UUID:
    """A lineage row that exists only to give `publication_lineage_id` a valid FK target."""

    version_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
            "VALUES (:id, :agent_id, 'v1', 'test')"
        ),
        {"id": version_id, "agent_id": agent_id},
    )
    deployment_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.deployments (id, agent_id, version_id, environment) "
            "VALUES (:id, :agent_id, :version_id, 'prod')"
        ),
        {"id": deployment_id, "agent_id": agent_id, "version_id": version_id},
    )
    lineage_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.thread_publication_lineages "
            "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch) "
            "VALUES (:id, :agent_id, :deployment_id, :conversation_id, :repo, :base_sha, :branch)"
        ),
        {
            "id": lineage_id,
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conversation_id": f"placeholder-{lineage_id.hex[:8]}",
            "repo": "acme-corp/acme-bot",
            "base_sha": "a" * 40,
            "branch": f"curie/bind-{lineage_id.hex[:8]}",
        },
    )
    await session.commit()
    return lineage_id


def test_bind_running_work_item_lineage_finds_a_legacy_work_items_running_request(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, work_item_id, _version = await _legacy_mail_work_item(session)
        readmitted = await readmit(session, _facts(agent_id))
        assert getattr(readmitted, "code", None) != "identity_mismatch", readmitted
        assert readmitted.request is not None
        request_id = readmitted.request.id
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "status = 'running', started_at = clock_timestamp(), "
                "execution_deadline = clock_timestamp() + interval '1800 seconds', "
                "execution_attempts = 1, "
                "version = version + 1 "
                "WHERE id = :id"
            ),
            {"id": request_id},
        )
        await session.commit()
        lineage_id = await _bare_lineage(session, agent_id)
        await crud._bind_running_work_item_lineage(
            session, agent_id=agent_id, conversation_id=NEW_KEY, lineage_id=lineage_id
        )
        await session.commit()
        bound = await session.scalar(
            text("SELECT publication_lineage_id FROM curie.work_items WHERE id = :id"),
            {"id": work_item_id},
        )
        assert bound == lineage_id

    _with_session(body)


def test_bind_running_work_item_lineage_does_not_adopt_a_legacy_key_under_another_adapter(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id, work_item_id, _version = await _legacy_mail_work_item(session)
        readmitted = await readmit(session, _facts(agent_id))
        assert getattr(readmitted, "code", None) != "identity_mismatch", readmitted
        assert readmitted.request is not None
        request_id = readmitted.request.id
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "status = 'running', started_at = clock_timestamp(), "
                "execution_deadline = clock_timestamp() + interval '1800 seconds', "
                "execution_attempts = 1, "
                "version = version + 1 "
                "WHERE id = :id"
            ),
            {"id": request_id},
        )
        await session.commit()
        lineage_id = await _bare_lineage(session, agent_id)
        # The pair's binding is bound under ADAPTER, not this one: the old
        # key can only be ITS route's, so a write implying another route's
        # identity must not adopt it.
        other = scoped_conversation_id("email", ADDRESS, THREAD, identity="other-inbox")
        await crud._bind_running_work_item_lineage(
            session, agent_id=agent_id, conversation_id=other, lineage_id=lineage_id
        )
        await session.commit()
        bound = await session.scalar(
            text("SELECT publication_lineage_id FROM curie.work_items WHERE id = :id"),
            {"id": work_item_id},
        )
        assert bound is None

    _with_session(body)
