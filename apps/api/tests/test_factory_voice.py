"""Voice drafts, source refresh, and confirmed steering share a real PostgreSQL transaction."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

import pytest
from curie_api import factory_voice_retention
from curie_api.config import get_settings
from curie_api.factory_confirmed_steering import (
    ConfirmedText,
    GitHubSteeringSource,
    QueuedSteering,
    RefusedSteering,
    SlackSteeringSource,
)
from curie_api.factory_voice import build_voice_draft_service, expire_voice_drafts
from curie_api.factory_voice_contract import (
    Action,
    Actor,
    Confirmed,
    CurrentSource,
    Draft,
    Edit,
    EditAccepted,
    GitHubConversation,
    Pending,
    Refused,
    Rejected,
    SlackThread,
    VoiceDraftService,
    VoiceMedia,
    VoiceSource,
)
from curie_api.main import create_app
from curie_api.models import DEFAULT_TENANT_ID, Agent, FactoryVoiceDraft, WorkItem
from factory_run_support import _run
from factory_voice_fakes import FixtureVoiceRecognizer
from sqlalchemy import Connection, Engine, delete, event, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool

AUDIO = b"fixture media from the authorized source"
BODY_HASH = hashlib.sha256(b"original comment").hexdigest()
REPOSITORY_PATH = "acme-corp/acme-bot"
TEXT = "Keep the existing timeout. Do not change it."


@pytest.fixture(autouse=True)
def clean_voice_storage(clean_db: None) -> None:
    async def clear(sessions: async_sessionmaker[AsyncSession]) -> None:
        async with sessions.begin() as session:
            await session.execute(delete(FactoryVoiceDraft))

    _run(clear)


def media(data: bytes = AUDIO) -> VoiceMedia:
    async def chunks() -> AsyncIterator[bytes]:
        yield data

    return VoiceMedia(declared_type="audio/wav", chunks=chunks())


class Recognizer(FixtureVoiceRecognizer):
    def __init__(self) -> None:
        super().__init__({AUDIO: TEXT})
        self.calls = 0

    async def recognize(self, declared_type: str, data: bytes) -> str | Refused:
        self.calls += 1
        return await super().recognize(declared_type, data)


class Provider:
    def __init__(self, source: VoiceSource, sessions: async_sessionmaker[AsyncSession]) -> None:
        audience = source.audience
        self.source = source
        self.sessions = sessions
        steering: GitHubSteeringSource | SlackSteeringSource
        if isinstance(audience, GitHubConversation):
            steering = GitHubSteeringSource(
                host=audience.host,
                installation_id=audience.installation_id,
                repository_id=audience.repository_id,
                conversation_kind=audience.kind,
                number=audience.number,
                source_comment_id=int(source.message_id),
                confirmation_comment_id=8765,
                actor_id=int(source.actor.provider_user_id),
            )
        else:
            assert source.actor.principal_id is not None
            steering = SlackSteeringSource(
                channel_identity=audience.channel_identity,
                channel=audience.channel_id,
                root_ts=audience.root_ts,
                message_ts=source.message_id,
                file_id=source.attachment_id,
                author=source.actor.provider_user_id,
                principal_id=source.actor.principal_id,
                confirmation_delivery_id="EvEXAMPLE01",
            )
        self.current: CurrentSource | Refused = CurrentSource(
            source=source, media_sha256=hashlib.sha256(AUDIO).hexdigest(), steering_source=steering
        )
        self.outcomes: list[Draft | Refused] = []
        self.bytes = AUDIO
        self.reads = 0
        self.admissions: list[ConfirmedText] = []
        self.refuse_admission = False

    async def media(self, source: VoiceSource) -> VoiceMedia:
        assert source == self.source
        return media(self.bytes)

    async def deliver(self, source: VoiceSource, draft_id: UUID, outcome: Draft | Refused) -> None:
        assert source == self.source
        self.outcomes.append(outcome)

    async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
        assert source == self.source
        self.reads += 1
        return self.current

    async def admit(
        self, session: AsyncSession, feedback: ConfirmedText
    ) -> QueuedSteering | RefusedSteering:
        self.admissions.append(feedback)
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == feedback.work_item_id)
            .values(next_sequence=WorkItem.next_sequence + 1)
        )
        if self.refuse_admission:
            return RefusedSteering(reason="not_authorized")
        return QueuedSteering(
            steering_id=feedback.steering_id, execution_request_id=None, replayed=False
        )

    async def decide(
        self, service: VoiceDraftService, code: str, action: Action
    ) -> EditAccepted | Confirmed | Rejected | Refused:
        return await service.decide(
            code=code,
            tenant_id=self.source.tenant_id,
            audience=self.source.audience,
            actor=self.source.actor,
            action=action,
            feedback=self,
        )

    async def sequence(self) -> int:
        async with self.sessions() as session:
            return (
                await session.execute(
                    select(WorkItem.next_sequence).where(WorkItem.id == self.source.work_item_id)
                )
            ).scalar_one()


async def setup_voice(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[VoiceDraftService, Provider, Recognizer]:
    async with sessions.begin() as session:
        agent_id = (
            await session.execute(insert(Agent).values(name="voice-example").returning(Agent.id))
        ).scalar_one()
        item = WorkItem(
            tenant_id=DEFAULT_TENANT_ID,
            agent_id=agent_id,
            conversation_id="voice-example",
            tracker_kind="github",
            tracker_host="github.com",
            tracker_issue_id="42",
            tracker_scope_id="19",
            code_host_kind="github",
            code_host_host="github.com",
            repository_project_id="19",
            repository_path=REPOSITORY_PATH,
            code_host_installation_id=12,
        )
        session.add(item)
        await session.flush()
        source = VoiceSource(
            tenant_id=DEFAULT_TENANT_ID,
            work_item_id=item.id,
            message_id="7654",
            audience=GitHubConversation(
                host="github.com", installation_id=12, repository_id=19, kind="issue", number=42
            ),
            actor=Actor(provider_user_id="81", principal_id=None),
            attachment_id="01e26b6e-84cd-4bc4-abbb-9b555ca67762",
            content_sha256=BODY_HASH,
        )
    recognizer = Recognizer()
    service = build_voice_draft_service(
        get_settings().model_copy(update={"factory_voice_enabled": True}),
        sessions,
        recognizer=recognizer,
    )
    return service, Provider(source, sessions), recognizer


async def draft(service: VoiceDraftService, provider: Provider) -> Draft:
    pending = await service.submit(provider.source, media())
    assert isinstance(pending, Pending)
    surface: Literal["github", "slack"] = (
        "github" if isinstance(provider.source.audience, GitHubConversation) else "slack"
    )
    assert await service.process_pending(surface, provider)
    assert await service.process_pending(surface, provider)
    assert len(provider.outcomes) == 1
    first = provider.outcomes[0]
    assert isinstance(first, Draft)
    assert (first.draft_id, first.transcript, first.expires_at) == (
        pending.draft_id,
        TEXT,
        pending.expires_at,
    )
    return first


def test_durable_confirm_scopes_replay_and_applies_once(clean_db: None) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, recognizer = await setup_voice(sessions)
        a, b = await asyncio.gather(*(service.submit(provider.source, media()) for _ in range(2)))
        assert isinstance(a, Pending) and a == b
        assert recognizer.calls == 0
        first = await draft(service, provider)
        assert recognizer.calls == 1
        assert not await service.process_pending("github", provider)
        assert await provider.sequence() == 1
        for edit in ["", " " * 10, "a" * 65537]:
            assert await provider.decide(service, first.code, Edit(text=edit)) == Refused(
                reason="invalid_text"
            )
        assert isinstance(first.audience, GitHubConversation)
        reference = f"{REPOSITORY_PATH}#{first.audience.number}"
        edited = f"  Keep the timeout at 30 on {reference}.\nDo not change it."
        assert await provider.decide(service, first.code, Edit(text=edited)) == EditAccepted(
            draft_id=first.draft_id
        )
        assert provider.outcomes == [first]
        assert await provider.decide(service, first.code, Edit(text=edited)) == Refused(
            reason="stale_version"
        )
        assert await provider.decide(service, replace(first, version=2).code, "confirm") == Refused(
            reason="unavailable"
        )
        assert await service.process_pending("github", provider)
        second = provider.outcomes[-1]
        assert isinstance(second, Draft) and second.version == 2
        assert second.transcript == edited and second.expires_at == first.expires_at
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="stale_version"
        )
        assert await provider.sequence() == 1
        results = await asyncio.gather(
            *(provider.decide(service, second.code, "confirm") for _ in range(2))
        )
        assert len(provider.admissions) == 1
        command = provider.admissions[0]
        assert command.text == edited and command.source_content_sha256 == BODY_HASH
        assert (
            command.work_item_id == provider.source.work_item_id and command.draft_id == a.draft_id
        )
        assert sorted(r.replayed for r in results if isinstance(r, Confirmed)) == [False, True]
        assert all(
            isinstance(r, Confirmed) and r.admission_id == command.steering_id for r in results
        )
        assert await provider.sequence() == 2
        assert await service.submit(provider.source, media()) == Refused(reason="closed")
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="stale_version"
        )
        assert await service.decide(
            code=second.code,
            tenant_id=DEFAULT_TENANT_ID,
            audience=provider.source.audience,
            actor=Actor(provider_user_id="82", principal_id=None),
            action="confirm",
            feedback=provider,
        ) == Refused(reason="forbidden")
        async with sessions() as session:
            row = await session.get(FactoryVoiceDraft, a.draft_id)
            assert row is not None
            assert row.source is None and row.media_sha256 is None and row.transcript is None

    _run(go)


@pytest.mark.parametrize("change", ["bytes", "actor", "body", "work", "audience", "attachment"])
def test_source_replacement_invalidates_even_after_original_is_replayed(
    clean_db: None,
    change: str,
) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        first = await draft(service, provider)
        source = provider.source
        assert isinstance(source.audience, GitHubConversation)
        changed = {
            "actor": replace(source, actor=Actor(provider_user_id="82", principal_id=None)),
            "body": replace(source, content_sha256="a" * 64),
            "work": replace(source, work_item_id=uuid4()),
            "audience": replace(source, audience=replace(source.audience, number=43)),
            "attachment": replace(source, attachment_id="b" * 64),
            "bytes": source,
        }[change]
        assert await service.submit(
            changed, media(b"different" if change == "bytes" else AUDIO)
        ) == Refused(reason="source_changed")
        assert isinstance(await service.submit(source, media()), Refused)
        assert isinstance(await provider.decide(service, first.code, "confirm"), Refused)
        assert await provider.sequence() == 1

    _run(go)


def test_refused_reads_and_policy_handoffs_admit_zero_work(clean_db: None) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        first = await draft(service, provider)
        original = provider.current
        assert isinstance(original, CurrentSource)
        provider.current = Refused(reason="unavailable")
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="unavailable"
        )
        provider.current = original
        provider.refuse_admission = True
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="admission_refused"
        )
        assert await provider.sequence() == 1
        assert isinstance(await provider.decide(service, first.code, "reject"), Rejected)
        assert await provider.decide(service, first.code, "confirm") == Refused(reason="closed")
        assert await provider.sequence() == 1

    _run(go)


def test_confirm_verifies_all_explicit_targets(clean_db: None) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        current = await draft(service, provider)
        assert isinstance(current.audience, GitHubConversation)
        for wrong in (
            "fix #41",
            "issue 41 and issue 42",
            f"acme-corp/other#{current.audience.number}",
            "https://other.example.com/acme-corp/acme-bot/issues/42",
            "issue number forty",
            "PR 42",
            "XYZ-42",
        ):
            assert await provider.decide(service, current.code, Edit(text=wrong)) == EditAccepted(
                draft_id=current.draft_id
            )
            assert await service.process_pending("github", provider)
            changed = provider.outcomes[-1]
            assert isinstance(changed, Draft) and changed.transcript == wrong
            current = changed
            assert await provider.decide(service, current.code, "confirm") == Refused(
                reason="target_mismatch"
            )
        assert await provider.sequence() == 1
        assert not provider.admissions

    _run(go)


def test_expiry_purges_pending_and_blocks_a_stale_lease(clean_db: None) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        pending = await service.submit(provider.source, media())
        assert isinstance(pending, Pending)
        started, release = asyncio.Event(), asyncio.Event()

        class PausedReader(Provider):
            async def media(self, source: VoiceSource) -> VoiceMedia:
                started.set()
                await release.wait()
                return media()

        paused = PausedReader(provider.source, sessions)
        async with asyncio.timeout(10):
            old = asyncio.create_task(service.process_pending("github", paused))
            await started.wait()
            past = datetime.now(UTC) - timedelta(seconds=1)
            async with sessions.begin() as session:
                await session.execute(
                    update(FactoryVoiceDraft)
                    .where(FactoryVoiceDraft.id == pending.draft_id)
                    .values(lease_until=past)
                )
            assert await service.process_pending("github", provider)
            release.set()
            assert await old
        async with sessions.begin() as session:
            await session.execute(
                update(FactoryVoiceDraft)
                .where(FactoryVoiceDraft.id == pending.draft_id)
                .values(created_at=past - timedelta(minutes=15), expires_at=past)
            )
            assert await expire_voice_drafts(session) >= 1
        assert not await service.process_pending("github", provider)
        assert not provider.outcomes and not paused.outcomes
        assert await service.submit(provider.source, media()) == Refused(reason="expired")
        assert await provider.sequence() == 1

    _run(go)


def test_slack_decision_needs_matching_actor_audience_and_confirmation_evidence() -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, github, _ = await setup_voice(sessions)
        audience = SlackThread(
            channel_identity="default",
            team_id="TEXAMPLE",
            app_id="AEXAMPLE",
            channel_id="C0EXAMPLE1",
            root_ts="100.000001",
        )
        source = replace(
            github.source,
            audience=audience,
            attachment_id="FEXAMPLE",
            actor=Actor(provider_user_id="UEXAMPLE", principal_id=uuid4()),
            message_id="100.000002",
        )
        provider = Provider(source, sessions)
        assert await service.submit(
            replace(source, actor=replace(source.actor, principal_id=None)), media()
        ) == Refused(reason="invalid_source")
        first = await draft(service, provider)
        for tenant, actor, room in [
            (uuid4(), source.actor, audience),
            (source.tenant_id, replace(source.actor, principal_id=uuid4()), audience),
            (source.tenant_id, source.actor, replace(audience, root_ts="101.000001")),
        ]:
            assert await service.decide(
                code=first.code,
                action=Edit(text="different"),
                actor=actor,
                audience=room,
                tenant_id=tenant,
                feedback=provider,
            ) == Refused(reason="forbidden")
        assert provider.reads == 0
        original = provider.current
        assert isinstance(original, CurrentSource)
        steering = original.steering_source
        assert isinstance(steering, SlackSteeringSource)
        for altered in [
            replace(steering, message_ts=audience.root_ts),
            replace(steering, principal_id=uuid4()),
            replace(steering, confirmation_delivery_id=source.message_id),
        ]:
            provider.current = replace(original, steering_source=altered)
            assert await provider.decide(service, first.code, "confirm") == Refused(
                reason="forbidden"
            )
        assert not provider.admissions
        provider.current = original
        assert isinstance(await provider.decide(service, first.code, "confirm"), Confirmed)
        assert provider.admissions[0].source == original.steering_source

    _run(go)


@pytest.mark.parametrize("change", ["deleted", "digest", "rebound"])
def test_confirm_checks_current_provider_source(change: str) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        first = await draft(service, provider)
        current = provider.current
        assert isinstance(current, CurrentSource)
        steering = current.steering_source
        assert isinstance(steering, GitHubSteeringSource)
        provider.current = replace(
            current,
            steering_source=replace(steering, confirmation_comment_id=steering.source_comment_id),
        )
        assert await provider.decide(service, first.code, "confirm") == Refused(reason="forbidden")
        changed: dict[str, CurrentSource | Refused] = {
            "deleted": Refused(reason="source_changed"),
            "digest": replace(current, media_sha256="b" * 64),
            "rebound": replace(current, source=replace(provider.source, work_item_id=uuid4())),
        }
        provider.current = changed[change]
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="source_changed"
        )
        provider.current = current
        assert await provider.decide(service, first.code, "confirm") == Refused(
            reason="source_changed"
        )
        assert not provider.admissions and await provider.sequence() == 1

    _run(go)


@pytest.mark.parametrize("stage", ["reread", "admit"])
def test_clock_crossing_the_deadline_rolls_back_work_and_purges(stage: str) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        first = await draft(service, provider)

        class Delayed(Provider):
            async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
                async with sessions.begin() as session:
                    await session.execute(
                        update(FactoryVoiceDraft)
                        .where(FactoryVoiceDraft.id == first.draft_id)
                        .values(
                            expires_at=text(
                                "clock_timestamp() - interval '1 second'"
                                if stage == "reread"
                                else "clock_timestamp() + interval '1 second'"
                            )
                        )
                    )
                return await super().reread(source)

            async def admit(
                self, session: AsyncSession, feedback: ConfirmedText
            ) -> QueuedSteering | RefusedSteering:
                decision = await super().admit(session, feedback)
                await session.execute(
                    select(text("pg_sleep_until(expires_at)"))
                    .select_from(FactoryVoiceDraft)
                    .where(FactoryVoiceDraft.id == first.draft_id)
                )
                return decision

        delayed = Delayed(provider.source, sessions)
        async with asyncio.timeout(10):
            assert await delayed.decide(service, first.code, "confirm") == Refused(reason="expired")
        assert len(delayed.admissions) == (1 if stage == "admit" else 0)
        assert await delayed.sequence() == 1
        async with sessions() as session:
            row = await session.get(FactoryVoiceDraft, first.draft_id)
            assert row is not None and row.admission_id is None
            assert row.source is None and row.media_sha256 is None and row.transcript is None

    _run(go)


@pytest.mark.parametrize("edited", [False, True])
def test_restarted_pump_retries_delivery_without_recognizing_twice(edited: bool) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, recognizer = await setup_voice(sessions)
        pending = await service.submit(provider.source, media())
        assert isinstance(pending, Pending)
        assert not await service.process_pending("slack", provider)
        assert await service.process_pending("github", provider)

        if edited:
            assert await service.process_pending("github", provider)
            original = provider.outcomes.pop()
            assert isinstance(original, Draft)
            assert await provider.decide(
                service, original.code, Edit(text="Use Retry.")
            ) == EditAccepted(draft_id=pending.draft_id)

        class LostAck(Provider):
            async def deliver(
                self, source: VoiceSource, draft_id: UUID, outcome: Draft | Refused
            ) -> None:
                await super().deliver(source, draft_id, outcome)
                raise ConnectionError

        lost = LostAck(provider.source, sessions)
        assert await service.process_pending("github", lost)
        assert not await service.process_pending("github", provider)
        async with sessions.begin() as session:
            await session.execute(
                update(FactoryVoiceDraft)
                .where(FactoryVoiceDraft.id == pending.draft_id)
                .values(retry_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        restarted = build_voice_draft_service(
            get_settings().model_copy(update={"factory_voice_enabled": True}),
            sessions,
            recognizer=recognizer,
        )
        assert await restarted.process_pending("github", provider)
        assert provider.outcomes == lost.outcomes and len(provider.outcomes) == 1
        assert recognizer.calls == 1 and await provider.sequence() == 1
        first = provider.outcomes[0]
        assert isinstance(first, Draft) and first.transcript == ("Use Retry." if edited else TEXT)
        assert first.version == (2 if edited else 1) and first.expires_at == pending.expires_at
        assert isinstance(await provider.decide(restarted, first.code, "confirm"), Confirmed)
        assert provider.admissions[0].text == first.transcript
        replay = await provider.decide(restarted, first.code, "confirm")
        assert isinstance(replay, Confirmed) and replay.replayed
        assert len(provider.admissions) == 1

    _run(go)


@pytest.mark.parametrize("after_recognition", [False, True])
def test_changed_media_delivers_only_a_refusal(after_recognition: bool) -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, recognizer = await setup_voice(sessions)
        pending = await service.submit(provider.source, media())
        assert isinstance(pending, Pending)
        if after_recognition:
            assert await service.process_pending("github", provider)
        provider.bytes = b"changed"
        assert await service.process_pending("github", provider)
        assert await service.process_pending("github", provider)
        assert provider.outcomes == [Refused(reason="source_changed")]
        assert await service.submit(provider.source, media()) == Refused(reason="source_changed")
        assert recognizer.calls == int(after_recognition) and await provider.sequence() == 1

    _run(go)


def test_invalid_sources_and_disabled_intake_do_not_open_media() -> None:
    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        source = provider.source
        assert isinstance(source.audience, GitHubConversation)
        opened: list[bool] = []

        async def stream() -> AsyncIterator[bytes]:
            opened.append(True)
            yield AUDIO

        unread = VoiceMedia(declared_type="audio/wav", chunks=stream())
        for invalid in (
            replace(source, attachment_id="https://example.com/upload?signature=example"),
            replace(source, content_sha256="raw comment body"),
            replace(source, audience=replace(source.audience, host="https://example.com")),
            replace(source, message_id="007"),
            replace(source, actor=replace(source.actor, provider_user_id="login")),
        ):
            assert await service.submit(invalid, unread) == Refused(reason="invalid_source")
        disabled = build_voice_draft_service(
            get_settings().model_copy(update={"factory_voice_enabled": False}),
            sessions,
            recognizer=FixtureVoiceRecognizer({AUDIO: TEXT}),
        )
        assert await disabled.submit(source, unread) == Refused(reason="disabled")
        assert not opened and await provider.sequence() == 1
        assert not await service.process_pending("github", provider)

    _run(go)


@pytest.mark.parametrize(
    ("enabled", "pending"),
    [(False, False), (False, True), (True, False)],
    ids=["disabled-empty", "disabled-draining", "enabled-empty"],
)
def test_lifespan_drains_voice_retention(
    enabled: bool, pending: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "factory_voice_enabled", enabled)
    monkeypatch.setattr(factory_voice_retention, "SWEEP_INTERVAL", 0.01)

    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        first = None
        if pending:
            service, provider, _ = await setup_voice(sessions)
            first = await draft(service, provider)

        app = create_app()
        async with app.router.lifespan_context(app):
            task = app.state.voice_retention_task
            sweeps: asyncio.Queue[Engine] = asyncio.Queue()

            def observe(conn: Connection) -> None:
                if asyncio.current_task() is task:
                    sweeps.put_nowait(conn.engine)

            event.listen(Engine, "begin", observe)
            try:
                async with asyncio.timeout(10):
                    engine = await sweeps.get()
                    pool = engine.pool
                    assert isinstance(pool, AsyncAdaptedQueuePool)
                    assert pool.size() == 1 and pool._max_overflow == 0
                    if first is not None:
                        assert await sweeps.get() is engine
                        async with sessions.begin() as session:
                            row = await session.get_one(FactoryVoiceDraft, first.draft_id)
                            assert row.transcript == TEXT
                            row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
                    if enabled:
                        assert await sweeps.get() is engine
                        assert not task.done() and engine.pool is pool
                    else:
                        await asyncio.shield(task)
                        assert engine.pool is not pool and pool.checkedin() == 0
                        if first is None:
                            assert sweeps.empty()
                        else:
                            async with sessions() as session:
                                row = await session.get_one(FactoryVoiceDraft, first.draft_id)
                                assert row.status == "expired"
                                assert row.source is None and row.transcript is None
            finally:
                event.remove(Engine, "begin", observe)
        assert task.done() and pool.checkedin() == 0

    _run(go)
