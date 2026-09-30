"""The two review-lineage checks accept a lineage keyed before the identity
(ADR-0168 decision 4), and refuse one whose route no longer matches.

`crud._require_review_binding` (the review-revision path) and
`github_review_store.review_context` (the webhook path) both rebuild the
lineage's original binding's thread key through `route_thread_key_matches`,
which already tries both the current and the pre-identity form. The cases
here seed a lineage under its pre-identity key and assert that both checks
still accept it, and that each refuses a lineage whose route has since been
rebound to a different identity.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from channel_protocol import scoped_conversation_id
from curie_api import crud
from curie_api.config import get_settings
from curie_api.github_review_events import FeedbackIgnored, UnverifiedFeedback
from curie_api.github_review_store import review_context
from curie_api.models import (
    Agent,
    AgentChannel,
    AgentVersion,
    Deployment,
    Environment,
    ThreadPublicationLineage,
    ThreadWorkspace,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
MAIL_ADDRESS = "review-agent@example.test"
MAIL_ADAPTER = "acme-review-mail"
MAIL_ENDPOINT = "http://acme-review-mail-adapter:8080/"
MAIL_THREAD = "review-thread/1"
SLACK_ADDRESS = "C0EXAMPLE1"
SLACK_TS = "1700000000.000200"


def _with_session[T](body):
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with maker() as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _review_fixture(
    session: AsyncSession,
    *,
    kind: str,
    address: str,
    binding_adapter: str | None,
    binding_endpoint: str | None,
    lineage_conversation_id: str,
    reply_conversation_id: str,
    github: bool = False,
) -> tuple[ThreadPublicationLineage, AgentChannel]:
    """An agent, a channel, a workspace and an open lineage on that thread."""

    agent_id = uuid.uuid4()
    session.add(Agent(id=agent_id, name=f"acme-review-{agent_id.hex[:8]}", repo_full_name=REPO))
    await session.flush()
    version_id = uuid.uuid4()
    session.add(
        AgentVersion(id=version_id, agent_id=agent_id, version_label="v1", created_by="test")
    )
    await session.flush()
    deployment_id = uuid.uuid4()
    session.add(
        Deployment(
            id=deployment_id,
            agent_id=agent_id,
            version_id=version_id,
            environment=Environment.prod,
            status="active",
        )
    )
    binding = AgentChannel(
        id=uuid.uuid4(),
        agent_id=agent_id,
        kind=kind,
        address=address,
        adapter=binding_adapter,
        endpoint=binding_endpoint,
        generation=0,
    )
    session.add(binding)
    session.add(
        ThreadWorkspace(
            id=uuid.uuid4(),
            agent_id=agent_id,
            conversation_id=lineage_conversation_id,
            repo_full_name=REPO,
            selected_by="U0REQUEST1",
        )
    )
    await session.flush()
    github_fields: dict[str, Any] = (
        {
            "github_repository_id": 9101,
            "github_installation_id": 41,
            "github_pr_node_id": "PR_example_review",
            "base_ref": "main",
            "pr_number": 71,
            "pr_url": f"https://github.com/{REPO}/pull/71",
            "head_sha": "b" * 40,
        }
        if github
        else {}
    )
    lineage = ThreadPublicationLineage(
        id=uuid.uuid4(),
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id=lineage_conversation_id,
        repo_full_name=REPO,
        base_sha="a" * 40,
        branch=f"curie/review-{uuid.uuid4().hex[:8]}",
        binding_id=binding.id,
        binding_generation=0,
        reply_conversation_id=reply_conversation_id,
        **github_fields,
    )
    session.add(lineage)
    await session.commit()
    await session.refresh(lineage)
    await session.refresh(binding)
    return lineage, binding


# --- crud._require_review_binding (the review-revision path) --------------------


def test_require_review_binding_accepts_a_mail_lineage_keyed_before_the_identity(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        old_key = scoped_conversation_id("email", MAIL_ADDRESS, MAIL_THREAD)
        lineage, binding = await _review_fixture(
            session,
            kind="email",
            address=MAIL_ADDRESS,
            binding_adapter=MAIL_ADAPTER,
            binding_endpoint=MAIL_ENDPOINT,
            lineage_conversation_id=old_key,
            reply_conversation_id=MAIL_THREAD,
        )
        resolved = await crud._require_review_binding(session, lineage)
        assert resolved.id == binding.id

    _with_session(body)


def test_require_review_binding_refuses_a_bare_slack_lineage_after_a_named_rebind(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        bare_key = scoped_conversation_id("slack", SLACK_ADDRESS, SLACK_TS)
        lineage, _binding = await _review_fixture(
            session,
            kind="slack",
            address=SLACK_ADDRESS,
            # A rebind onto a named identity (decision 3): not the default
            # the lineage's bare key was captured under.
            binding_adapter="second-bot",
            binding_endpoint=None,
            lineage_conversation_id=bare_key,
            reply_conversation_id=SLACK_TS,
        )
        with pytest.raises(crud.PublicationLineageConflict) as caught:
            await crud._require_review_binding(session, lineage)
        assert caught.value.code == "publication.review_ineligible"

    _with_session(body)


# --- github_review_store.review_context (the webhook path) ----------------------


def _feedback(**overrides: Any) -> UnverifiedFeedback:
    values: dict[str, Any] = {
        "delivery_id": uuid.uuid4(),
        "event": "issue_comment",
        "installation_id": 41,
        "repository_id": 9101,
        "repo_full_name": REPO,
        "pr_number": 71,
        "feedback_id": 1,
        "sender_id": 1,
        "sender_login": "example-reviewer",
        "body": "Looks fine.",
        "url": f"https://github.com/{REPO}/pull/71#issuecomment-1",
        "created_at": datetime.now(UTC),
        "head_sha": "b" * 40,
        "commit_sha": None,
        "author_association": "MEMBER",
    }
    values.update(overrides)
    return UnverifiedFeedback(**values)


def test_review_context_accepts_a_mail_lineage_keyed_before_the_identity(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        old_key = scoped_conversation_id("email", MAIL_ADDRESS, MAIL_THREAD)
        _lineage, binding = await _review_fixture(
            session,
            kind="email",
            address=MAIL_ADDRESS,
            binding_adapter=MAIL_ADAPTER,
            binding_endpoint=MAIL_ENDPOINT,
            lineage_conversation_id=old_key,
            reply_conversation_id=MAIL_THREAD,
            github=True,
        )
        context = await review_context(session, _feedback(), get_settings())
        assert context.binding.id == binding.id

    _with_session(body)


def test_review_context_refuses_a_bare_slack_lineage_after_a_named_rebind(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        bare_key = scoped_conversation_id("slack", SLACK_ADDRESS, SLACK_TS)
        await _review_fixture(
            session,
            kind="slack",
            address=SLACK_ADDRESS,
            binding_adapter="second-bot",
            binding_endpoint=None,
            lineage_conversation_id=bare_key,
            reply_conversation_id=SLACK_TS,
            github=True,
        )
        with pytest.raises(FeedbackIgnored) as caught:
            await review_context(session, _feedback(), get_settings())
        assert getattr(caught.value, "code", None) == "binding_no_longer_authorized"

    _with_session(body)
