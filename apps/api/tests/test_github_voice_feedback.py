"""Local replay of GitHub voice comments against the signed webhook and factory poller."""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_confirmed_steering import ConfirmedText, QueuedSteering, RefusedSteering
from curie_api.factory_voice_contract import Actor, Draft, GitHubConversation, Refused, VoiceSource
from curie_api.forges.github import voice
from curie_api.forges.types import PullRequestRef, ReplyTarget
from curie_api.github_factory import lock_issue
from curie_api.github_factory_review import (
    PLAIN_FEEDBACK_CAP,
    admit_review_revision,
    bound_lineage,
    conversation_owner,
)
from curie_api.github_review_events import FeedbackIgnored
from curie_api.models import DEFAULT_TENANT_ID
from curie_test_support.static_shell import wait_until
from forge_fakes.github import INSTALLATION_ID, MENTION, REPO, REPO_ID, SENDER_ID, post_delivery
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_github_factory_ingress import _code, _rows
from test_github_factory_review import _execute
from test_github_plain_feedback import (
    REVIEWER,
    SECOND,
    Factory,
    _all_requests,
    _lineage,
    _requests,
    factory_github,
    running_factory,
)
from test_github_voice_media import BODY, UPLOAD

pytestmark = pytest.mark.usefixtures("clean_db", "valkey")


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Factory]:
    monkeypatch.setenv("FACTORY_VOICE_ENABLED", "true")
    github = factory_github(monkeypatch, tmp_path)
    try:
        with running_factory(github, new_agent=True) as running:
            yield running
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("on_issue", [True, False], ids=["issue", "pr"])
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("private", "voice_private_repository"),
        ("foreign", "voice_invalid_media"),
        ("ambiguous", "voice_invalid_media"),
        ("changed", "voice_source_changed"),
        ("deleted", "voice_unavailable"),
        ("writer_removed", "voice_forbidden"),
        ("wrong_conversation", "voice_source_changed"),
        ("closed_pr", "terminal_pull_request"),
        ("bad_command", "voice_invalid_text"),
        ("prefixed_command", "voice_invalid_text"),
    ],
)
def test_signed_voice_comment_refuses_without_a_revision(
    factory: Factory, on_issue: bool, change: str, reason: str
) -> None:
    pull = factory.pull()
    number = pull.issue if on_issue else pull.number
    body = BODY
    if change == "foreign":
        body = BODY.replace("github.com", "media.example.com")
    elif change == "ambiguous":
        body = f"{UPLOAD}\n{UPLOAD}"
    elif change == "bad_command":
        body = "confirm abcd1234ef01.2 extra"
    elif change == "prefixed_command":
        body = f"@{MENTION} confirm abcd1234ef01.2 extra"
    posted = factory.github.add_conversation_comment(number, REVIEWER, body)
    if change == "private":
        factory.github.visibility = "private"
    elif change == "changed":
        factory.github.edit_comment(posted.item["id"], BODY + " ")
    elif change == "deleted":
        factory.github.comments = [
            c for c in factory.github.comments if c["id"] != posted.item["id"]
        ]
    elif change == "writer_removed":
        factory.github.grant(REVIEWER, "read")
    elif change == "wrong_conversation":
        factory.github.comments[-1]["issue_url"] = f"https://api.github.com/repos/{REPO}/issues/999"
    elif change == "closed_pr":
        factory.github.pulls[pull.number]["state"] = "closed"

    response = post_delivery(factory.client, posted.deliveries[0])
    if change == "deleted":
        assert response.status_code == 503
        assert response.json()["detail"]["code"] == reason
    else:
        assert response.status_code == 200
        assert _code(response) == reason
    assert len(_requests(pull.issue)) == _all_requests() == 1
    assert _lineage(pull)["plain_feedback_revisions"] == 0


@pytest.mark.parametrize("on_issue", [True, False], ids=["issue", "pr"])
def test_no_voice_revision_before_a_factory_publication(factory: Factory, on_issue: bool) -> None:
    issue, _first = factory.work_item()
    number = issue if on_issue else factory.github.seed_pull("unowned", "main")
    posted = factory.github.add_conversation_comment(number, REVIEWER, BODY)
    response = factory.post(posted.deliveries[0])
    assert _code(response) == "lineage_unbound"
    assert _all_requests() == 1


@pytest.mark.parametrize(
    ("body", "ordinary_feedback"),
    [
        (BODY, False),
        (f"![poster](https://media.example.com/poster.png)\n{BODY}", False),
        (BODY.replace("github.com", "media.example.com"), False),
        ("[Recording.wav](https://github.com/user-attachments/assets/example)", False),
        ("edit abcd1234ef01.2 @acme-factory please change the label", False),
        ("confirm", True),
        (f"@{MENTION} confirm abcd1234ef01.2", False),
        ("@example-reviewer confirm abcd1234ef01.2", True),
        ("[image](https://github.com/user-attachments/assets/example)", True),
        ("[notes.pdf](https://github.com/user-attachments/files/1234/notes.pdf)", True),
        (f"Please document this example URL: {UPLOAD}", True),
    ],
)
def test_poll_distinguishes_voice_from_ordinary_pr_feedback(
    factory: Factory, body: str, ordinary_feedback: bool
) -> None:
    pull = factory.pull()
    factory.comment(pull, body)
    factory.poll()
    assert len(_requests(pull.issue)) == (2 if ordinary_feedback else 1)
    assert _lineage(pull)["plain_feedback_revisions"] == int(ordinary_feedback)


def test_webhook_preserves_ordinary_feedback_with_an_audio_example(factory: Factory) -> None:
    pull = factory.pull()
    posted = factory.comment(pull, f"Please document this example URL: {UPLOAD}")
    assert factory.post(posted.deliveries[0]).json()["status"] == "factory_admitted"
    assert len(_requests(pull.issue)) == 2


@pytest.mark.parametrize("mode", ["commit", "rollback", "cap"])
def test_confirmed_text_and_cap_share_the_callers_transaction(factory: Factory, mode: str) -> None:
    pull = factory.pull(complete=False)
    if mode == "cap":
        _execute(
            "UPDATE curie.thread_publication_lineages "
            "SET plain_feedback_revisions=:cap WHERE id=:id",
            {"cap": PLAIN_FEEDBACK_CAP, "id": pull.lineage_id},
        )
    request_id = uuid.uuid4()
    writes = factory.github.writes

    async def submit() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                owner = await conversation_owner(session, get_settings(), REPO_ID, pull.number)
                assert owner is not None
                item, lineage = owner
                await lock_issue(session, item.tracker_issue)
                url = f"https://github.com/{REPO}/pull/{pull.number}#issuecomment-71"
                result = await admit_review_revision(
                    session,
                    settings=get_settings(),
                    work_item=item,
                    lineage_id=lineage.id,
                    bound=bound_lineage(lineage),
                    provenance={"body": "Change the button label to Retry", "url": url},
                    url=url,
                    reply_target=ReplyTarget.on_pull_request(
                        PullRequestRef(item.repository, str(pull.number))
                    ),
                    request_id=request_id,
                    requester="github:41:example-reviewer",
                    plain=True,
                )
                if mode == "cap":
                    assert result.errors == [{"code": "plain_feedback_cap"}]
                else:
                    assert result.status == "factory_queued"
                if mode == "rollback":
                    await session.rollback()
                else:
                    await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(submit())
    assert factory.github.writes == writes
    rows = _requests(pull.issue)
    if mode == "commit":
        assert [r["id"] for r in rows[1:]] == [request_id]
        assert rows[1]["work_item_id"] == pull.work_item_id
        assert rows[1]["reply_target_pr_number"] == str(pull.number)
        assert _lineage(pull)["plain_feedback_revisions"] == 1
    else:
        assert len(rows) == 1
        assert _lineage(pull)["plain_feedback_revisions"] == (
            PLAIN_FEEDBACK_CAP if mode == "cap" else 0
        )


def test_disabled_voice_never_fetches_comment_or_attachment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("FACTORY_VOICE_ENABLED", raising=False)
    github = factory_github(monkeypatch, tmp_path)
    try:
        with running_factory(github, new_agent=True) as running:
            pull = running.pull()
            posted = running.comment(pull, BODY)
            before = len(github.http_requests)
            assert _code(running.post(posted.deliveries[0])) == "voice_disabled"
            assert github.http_requests[before:] == []
            assert len(_requests(pull.issue)) == 1
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("kind", ["issue", "pull_request"])
def test_processing_reopens_the_exact_recording_and_displays_only_at_its_source(
    factory: Factory,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    pull = factory.pull()
    number = pull.issue if kind == "issue" else pull.number
    posted = factory.github.add_conversation_comment(number, REVIEWER, BODY)
    source = VoiceSource(
        tenant_id=DEFAULT_TENANT_ID,
        work_item_id=pull.work_item_id,
        audience=GitHubConversation(
            host="github.com",
            installation_id=INSTALLATION_ID,
            repository_id=REPO_ID,
            kind=kind,
            number=number,
        ),
        actor=Actor(provider_user_id=str(SENDER_ID), principal_id=None),
        message_id=str(posted.item["id"]),
        content_sha256=hashlib.sha256(BODY.encode()).hexdigest(),
        attachment_id=hashlib.sha256(UPLOAD.encode()).hexdigest(),
    )
    recording = Path(__file__).resolve().parents[3] / "tests/vectors/factory-voice/retry-label.wav"
    audio = recording.read_bytes()

    def download(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == UPLOAD
        assert "authorization" not in request.headers
        return httpx.Response(
            200, headers={"Content-Type": "audio/wav"}, stream=httpx.ByteStream(audio)
        )

    monkeypatch.setattr(voice, "AsyncHTTPTransport", lambda: httpx.MockTransport(download))
    runtime = factory.client.app.state.github_voice
    draft = Draft(
        draft_id=uuid.uuid4(),
        audience=source.audience,
        transcript="Change the button label to Retry.",
        version=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )

    async def process() -> None:
        reopened = await runtime.media(source)
        assert not isinstance(reopened, Refused)
        assert b"".join([chunk async for chunk in reopened.chunks]) == audio
        await runtime.deliver(source, draft.draft_id, draft)
        await runtime.deliver(source, draft.draft_id, draft)

    factory.client.portal.call(process)
    prompts = [c for c in factory.github.comments if draft.code in c["body"]]
    assert len(prompts) == 1
    assert prompts[0]["_number"] == number
    assert draft.transcript in prompts[0]["body"] and f"confirm {draft.code}" in prompts[0]["body"]
    assert len(_requests(pull.issue)) == 1
    decision = factory.github.add_conversation_comment(number, REVIEWER, f"confirm {draft.code}")

    async def handoff(rollback: bool = False, substitute: bool = False):
        async with runtime.sessions() as session:
            target = await runtime.target(session, source.audience)
            port = voice.GitHubVoiceFeedback(
                runtime,
                target,
                source.actor,
                REVIEWER["login"],
                decision.item["id"],
                hashlib.sha256(decision.item["body"].encode()).hexdigest(),
                draft.code,
                True,
            )
            current = await port.reread(source)
            assert not isinstance(current, Refused)
            assert current.media_sha256 == hashlib.sha256(audio).hexdigest()
            command = ConfirmedText(
                draft_id=draft.draft_id,
                work_item_id=pull.work_item_id,
                source=current.steering_source,
                source_content_sha256=source.content_sha256,
                text="Different feedback" if substitute else draft.transcript,
            )
            result = await port.admit(session, command)
            if rollback:
                await session.rollback()
            else:
                await session.commit()
            return result

    assert factory.client.portal.call(handoff, False, True) == RefusedSteering(
        reason="source_changed"
    )
    assert isinstance(factory.client.portal.call(handoff, True), QueuedSteering)
    assert len(_requests(pull.issue)) == 1
    assert _lineage(pull)["plain_feedback_revisions"] == 0
    admitted = factory.client.portal.call(handoff)
    assert isinstance(admitted, QueuedSteering) and not admitted.replayed
    replay = factory.client.portal.call(handoff)
    assert replay == QueuedSteering(
        steering_id=admitted.steering_id,
        execution_request_id=admitted.execution_request_id,
        replayed=True,
    )
    before, revision = _requests(pull.issue)
    assert before["work_item_id"] == revision["work_item_id"] == pull.work_item_id
    assert revision["id"] == admitted.execution_request_id
    assert revision["reply_target_kind"] == kind
    assert draft.transcript in revision["objective"] and UPLOAD not in revision["objective"]
    assert _lineage(pull)["plain_feedback_revisions"] == 1
    factory.github.edit_comment(posted.item["id"], BODY + "withdrawn")
    assert factory.client.portal.call(runtime.media, source) == Refused(reason="source_changed")
    with pytest.raises(FeedbackIgnored, match="source_changed"):
        factory.client.portal.call(runtime.deliver, source, draft.draft_id, draft)
    assert len(_requests(pull.issue)) == 2


@dataclass
class Recording:
    data: bytes
    status: int = 200

    def fetch(self, request: httpx.Request) -> httpx.Response:
        assert str(request.url) == UPLOAD and "authorization" not in request.headers
        return httpx.Response(
            self.status, headers={"Content-Type": "audio/wav"}, stream=httpx.ByteStream(self.data)
        )


@pytest.fixture
def spoken_factory(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    from curie_api import factory_voice

    audio = (
        Path(__file__).resolve().parents[3] / "tests/vectors/factory-voice/retry-label.wav"
    ).read_bytes()
    recording = Recording(audio)

    class FixtureRecognizer:
        async def available(self) -> bool:
            return True

        async def recognize(self, declared_type: str, data: bytes) -> str:
            assert declared_type == "audio/wav" and data == audio
            return "Change the button label to Retry."

    monkeypatch.setattr(
        factory_voice,
        "build_voice_draft_service",
        partial(factory_voice.build_voice_draft_service, recognizer=FixtureRecognizer()),
    )
    monkeypatch.setattr(voice, "AsyncHTTPTransport", lambda: httpx.MockTransport(recording.fetch))
    _execute("DELETE FROM curie.factory_voice_drafts", {})
    factory: Factory = request.getfixturevalue("factory")
    return factory, recording


def _displayed_code(factory: Factory, number: int, version: int) -> str:
    expression = re.compile(rf"`confirm ([0-9a-f]{{12}}\.{version})`")
    matches = [
        match[1]
        for comment in factory.github.comments
        if comment["_number"] == number
        for match in expression.finditer(comment["body"])
    ]
    assert len(matches) == 1
    return matches[0]


def _submit_recording(factory: Factory, number: int):
    posted = factory.github.add_conversation_comment(number, REVIEWER, BODY)
    response = factory.post(posted.deliveries[0])
    assert response.json()["status"] == "voice_pending"
    wait_until(
        lambda: bool(_rows("SELECT id FROM curie.factory_voice_drafts WHERE status='ready'")),
        12,
        "the API voice processor to publish the transcript",
    )
    assert _all_requests() == 1
    return posted, _displayed_code(factory, number, 1)


@pytest.mark.parametrize("on_issue", [True, False], ids=["issue", "pr"])
def test_signed_confirmation_admits_one_edited_revision_on_the_same_work_item(
    spoken_factory,
    on_issue: bool,
) -> None:
    factory, _recording = spoken_factory
    pull = factory.pull()
    number = pull.issue if on_issue else pull.number
    posted, code = _submit_recording(factory, number)
    prompt = [c for c in factory.github.comments if code in c["body"]]
    assert len(prompt) == 1 and prompt[0]["_number"] == number
    assert "Change the button label to Retry." in prompt[0]["body"]
    text = "Change the button label to Retry. Keep the existing timeout."
    edit = factory.github.add_conversation_comment(number, REVIEWER, f"edit {code} {text}")
    assert factory.post(edit.deliveries[0]).json()["status"] == "voice_drafted"
    current = _displayed_code(factory, number, 2)
    assert current != code and current.partition(".")[0] == code.partition(".")[0]
    old = factory.github.add_conversation_comment(number, REVIEWER, f"confirm {code}")
    assert _code(factory.post(old.deliveries[0])) == "voice_stale_version"
    assert _all_requests() == 1
    decision = factory.github.add_conversation_comment(number, REVIEWER, f"confirm {current}")
    assert factory.post(decision.deliveries[0]).json()["status"] == "voice_confirmed"
    duplicate = factory.github.add_conversation_comment(number, REVIEWER, f"confirm {current}")
    assert factory.post(duplicate.deliveries[0]).json()["status"] == "factory_duplicate"
    assert factory.post(decision.deliveries[0]).json()["status"] == "factory_duplicate"
    assert factory.post(posted.deliveries[0]).json()["status"] == "factory_duplicate"
    factory.poll()
    first, revision = _requests(pull.issue)
    assert first["work_item_id"] == revision["work_item_id"] == pull.work_item_id
    assert text in revision["objective"] and UPLOAD not in revision["objective"]
    assert revision["reply_target_kind"] == ("issue" if on_issue else "pull_request")
    assert _lineage(pull)["plain_feedback_revisions"] == 1
    (receipt,) = _rows(
        "SELECT status,admission_id,disposition,transcript FROM curie.factory_voice_drafts"
    )
    assert receipt == {
        "status": "confirmed",
        "admission_id": revision["id"],
        "disposition": "queued",
        "transcript": None,
    }


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("wrong_actor", "voice_forbidden"),
        ("wrong_audience", "voice_forbidden"),
        ("edited", "voice_source_changed"),
        ("deleted", "voice_unavailable"),
        ("bytes", "voice_source_changed"),
        ("inaccessible", "voice_unavailable"),
        ("expired", "voice_expired"),
        ("rejected", "voice_closed"),
        ("cap", "voice_admission_refused"),
        ("wrong_target", "voice_target_mismatch"),
        ("withdrawn_decision", "voice_source_changed"),
    ],
)
def test_signed_confirmation_refusals_create_zero_revisions(
    spoken_factory, change: str, reason: str
) -> None:
    factory, recording = spoken_factory
    pull = factory.pull()
    number = pull.number
    posted, code = _submit_recording(factory, number)
    if change == "edited":
        factory.github.edit_comment(posted.item["id"], BODY + " ")
    elif change == "deleted":
        factory.github.comments = [
            c for c in factory.github.comments if c["id"] != posted.item["id"]
        ]
    elif change == "bytes":
        recording.data = recording.data[:-2] + b"\x01\x02"
    elif change == "inaccessible":
        recording.status = 404
    elif change == "wrong_audience":
        number = pull.issue
    elif change == "expired":
        _execute(
            "UPDATE curie.factory_voice_drafts SET created_at=now()-interval '20 minutes', "
            "expires_at=now()-interval '1 second'",
            {},
        )
    elif change == "rejected":
        reject = factory.github.add_conversation_comment(number, REVIEWER, f"reject {code}")
        assert factory.post(reject.deliveries[0]).json()["status"] == "voice_rejected"
    elif change == "cap":
        _execute(
            "UPDATE curie.thread_publication_lineages "
            "SET plain_feedback_revisions=:cap WHERE id=:id",
            {"cap": PLAIN_FEEDBACK_CAP, "id": pull.lineage_id},
        )
    elif change == "wrong_target":
        edit = factory.github.add_conversation_comment(
            number, REVIEWER, f"edit {code} fix issue #9999"
        )
        assert factory.post(edit.deliveries[0]).json()["status"] == "voice_drafted"
        code = _displayed_code(factory, number, 2)
    actor = SECOND if change == "wrong_actor" else REVIEWER
    decision = factory.github.add_conversation_comment(number, actor, f"confirm {code}")
    if change == "withdrawn_decision":
        factory.github.edit_comment(decision.item["id"], "Withdrawn")
    response = post_delivery(factory.client, decision.deliveries[0])
    assert _code(response) == reason, response.text
    assert _all_requests() == len(_requests(pull.issue)) == 1
    assert _lineage(pull)["plain_feedback_revisions"] == (
        PLAIN_FEEDBACK_CAP if change == "cap" else 0
    )
    assert (
        _rows("SELECT admission_id FROM curie.factory_voice_drafts WHERE admission_id IS NOT NULL")
        == []
    )
