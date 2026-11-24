"""The API accepts only the native receipt bound to the worker's delivery."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
import test_factory_slack_runtime as runtime
from curie_api.factory_slack_voice import (
    SlackVoice,
    VoiceUnavailable,
    delivery_message,
    verified_request,
)
from curie_api.factory_voice_contract import Actor, Draft, Refused, SlackThread, VoiceSource
from curie_api.schemas.workitems import FactorySlackSteerIn
from curie_internal.slack_mention_receipt import SlackMentionReceipt

steering = runtime.steering


def clip() -> SlackMentionReceipt:
    return SlackMentionReceipt(
        version=3,
        delivery_id="event-example",
        channel_identity="example",
        channel="C0EXAMPLE1",
        root_ts="1800000000.000001",
        message_ts="1800000000.000002",
        user_id="U0EXAMPLE1",
        evidence={
            "delivery": "app_mention",
            "user_id": "U0EXAMPLE1",
            "event_user_team": "T0EXAMPLE1",
        },
        body="",
        api_app_id="A0EXAMPLE1",
        team_id="T0EXAMPLE1",
        voice_file_id="F0EXAMPLE1",
        content_sha256="a" * 64,
    )


def payload(receipt: SlackMentionReceipt) -> FactorySlackSteerIn:
    return FactorySlackSteerIn(
        delivery_id=receipt.delivery_id,
        channel_identity=receipt.channel_identity,
        channel=receipt.channel,
        root_ts=receipt.root_ts,
        author=receipt.user_id,
        receipt=receipt.model_dump(mode="json"),
    )


def test_only_the_exact_unexpired_verified_receipt_supplies_voice_evidence() -> None:
    stored = clip()
    data = payload(stored)
    assert verified_request(stored, data) is not None
    assert verified_request(None, data) is None
    for key in ("delivery_id", "channel_identity", "channel", "root_ts", "author"):
        assert verified_request(stored, data.model_copy(update={key: "elsewhere"})) is None
    for key in ("voice_file_id", "api_app_id", "team_id", "message_ts", "content_sha256"):
        altered = {**data.receipt, key: "replaced"}
        assert verified_request(stored, data.model_copy(update={"receipt": altered})) is None


def source() -> VoiceSource:
    audience = SlackThread(
        channel_identity="example",
        team_id="T0EXAMPLE1",
        app_id="A0EXAMPLE1",
        channel_id="C0EXAMPLE1",
        root_ts="1800000000.000001",
    )
    return VoiceSource(
        tenant_id=uuid4(),
        work_item_id=uuid4(),
        audience=audience,
        message_id="1800000000.000002",
        actor=Actor(provider_user_id="U0EXAMPLE1", principal_id=uuid4()),
        attachment_id="F0EXAMPLE1",
        content_sha256="a" * 64,
    )


def test_draft_display_uses_the_exact_audience_version_and_deadline() -> None:
    original = source()
    assert isinstance(original.audience, SlackThread)
    now = datetime.now(UTC)
    draft = Draft(
        draft_id=uuid4(),
        audience=original.audience,
        transcript="Keep the timeout.\nChange the label.",
        version=2,
        expires_at=now + timedelta(minutes=1),
    )
    response = delivery_message(original, draft.draft_id, draft, now)
    assert isinstance(response, str)
    assert draft.transcript in response
    for verb in ("confirm", "reject", "edit"):
        assert f"{verb} {draft.code}" in response
    assert delivery_message(original, uuid4(), draft, now) == Refused(reason="invalid_source")
    assert delivery_message(original, draft.draft_id, draft, draft.expires_at) == Refused(
        reason="expired"
    )
    for key in ("channel_identity", "channel_id", "team_id", "app_id", "root_ts"):
        elsewhere = replace(original.audience, **{key: "other"})
        assert delivery_message(
            replace(original, audience=elsewhere), draft.draft_id, draft, now
        ) == Refused(reason="forbidden")


@pytest.mark.parametrize("reason", ["no_speech", "source_changed"])
def test_recognition_refusal_exposes_only_its_code(reason: str) -> None:
    original = source()
    refusal = Refused(reason=reason)
    assert (
        delivery_message(original, uuid4(), refusal, datetime.now(UTC))
        == f"Voice feedback refused ({reason})."
    )


def decision_receipt() -> SlackMentionReceipt:
    fields = clip().model_dump(mode="json", exclude={"voice_file_id", "content_sha256"})
    fields.update(
        version=1,
        bot_user_id="U0EXAMPLE9",
        body="<@U0EXAMPLE9> edit abcd1234ef01.2 https://github.com/acme-corp/acme-bot/issues/1",
    )
    return SlackMentionReceipt.model_validate(fields)


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_voice_receipts_cannot_request_ordinary_steering_or_kickoff() -> None:
    import httpx
    from curie_api.config import get_settings
    from curie_api.factory_slack import steer
    from curie_api.factory_slack_kickoff import kickoff
    from curie_api.schemas.workitems import FactorySlackKickoffIn
    from sqlalchemy.ext.asyncio import AsyncSession

    async with AsyncSession() as session, httpx.AsyncClient() as client:
        for receipt in (clip(), decision_receipt()):
            data = payload(receipt)
            assert (
                await steer(session, data, settings=get_settings(), client=client)
            ).outcome == "receipt_mismatch"
            result = await kickoff(
                session,
                FactorySlackKickoffIn.model_validate(data.model_dump()),
                settings=get_settings(),
                client=client,
            )
            assert result.outcome == "receipt_mismatch"
        assert not session.in_transaction()


@dataclass
class VoiceThread:
    writer: runtime.Person
    run: runtime.SteerRun
    receipt: SlackMentionReceipt
    original: VoiceSource
    adapter: SlackVoice
    posts: list[dict[str, Any]]
    provider_calls: list[str]
    message_files: list[dict[str, Any]]
    stack: runtime.SteeringStack

    def send(self, stored, path="voice"):
        from curie_api.config import get_settings

        response = self.stack.client.post(
            f"/v1/internal/factory-slack/{path}",
            headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
            json=payload(stored).model_dump(),
        )
        assert response.status_code == 200, response.text
        assert response.headers["Cache-Control"] == "no-store"
        return response.json()

    def reply(self, command, *, actor=None, root=None):
        from curie_channel_gateway.slack.identities import delivery_key
        from curie_internal.slack_mention_receipt import receipt_key, write_receipt

        author = (actor or self.writer).slack_user
        base = decision_receipt().model_dump(mode="json")
        base.update(
            user_id=author,
            root_ts=root or self.run.root_ts,
            channel_identity=self.stack.identity,
            message_ts=f"1800100500.{uuid4().int % 1000000:06d}",
            delivery_id=delivery_key(f"Ev{uuid4().hex}", self.stack.identity),
            evidence={**self.receipt.evidence, "user_id": author},
            body=f"<@{runtime.FACTORY_BOT_USER}> {command}",
            bot_user_id=runtime.FACTORY_BOT_USER,
        )
        candidate = SlackMentionReceipt.model_validate(base)
        self.stack.base.doomed_keys.append(receipt_key(candidate.delivery_id))
        assert write_receipt(self.stack.valkey, candidate, ttl_s=90)
        return candidate

    def pump(self):
        assert self.stack.client.portal.call(
            self.adapter.service.process_pending, "slack", self.adapter
        )

    def display_draft(self):
        import re

        assert self.send(self.receipt)["outcome"] == "pending"
        assert self.posts == []
        self.pump()
        self.pump()
        (post,) = self.posts
        assert TRANSCRIPT in post["text"]
        assert post["channel"] == self.receipt.channel and post["thread_ts"] == self.run.root_ts
        (code,) = re.findall(r"^confirm ([0-9a-f]{12}\.1)$", post["text"], re.M)
        return code


TRANSCRIPT = "Change the button label to Retry."


@pytest.fixture
def voice_thread(steering, monkeypatch):
    import hashlib
    import json
    from contextlib import asynccontextmanager
    from pathlib import Path
    from time import time_ns

    import httpx
    from aiohttp.test_utils import TestServer
    from curie_api.config import get_settings
    from curie_api.factory_voice import build_voice_draft_service
    from curie_api.factory_voice_bindings import source_key
    from curie_api.models import DEFAULT_TENANT_ID
    from curie_channel_gateway.read_transport import (
        GatewayClient,
        GatewayProviders,
        gateway_application,
    )
    from curie_channel_gateway.request_replay import RequestReplayStore
    from curie_channel_gateway.slack.files import FileResponse
    from curie_channel_gateway.slack.identities import delivery_key
    from curie_channel_gateway.slack.voice import SlackVoiceGateway
    from curie_internal.slack_mention_receipt import receipt_key, voice_source_key, write_receipt
    from factory_voice_fakes import FixtureVoiceRecognizer
    from test_factory_voice_authority import _enable_public_mode

    writer = steering.person()
    run = steering.open_steered_run()
    _enable_public_mode(steering)
    receipt = clip()
    addressed = f"<@{runtime.FACTORY_BOT_USER}>"
    seconds, micros = divmod(time_ns() // 1000, 1_000_000)
    receipt = receipt.model_copy(
        update={
            "channel_identity": steering.identity,
            "root_ts": run.root_ts,
            "user_id": writer.slack_user,
            "delivery_id": delivery_key(f"Ev{uuid4().hex}", steering.identity),
            "message_ts": f"{seconds}.{micros:06d}",
            "content_sha256": hashlib.sha256(addressed.encode()).hexdigest(),
            "evidence": {
                "delivery": "app_mention",
                "user_id": writer.slack_user,
                "event_user_team": runtime.TEAM,
            },
        }
    )
    assert write_receipt(steering.valkey, receipt, ttl_s=90)
    steering.base.doomed_keys.extend(
        [
            receipt_key(receipt.delivery_id),
            voice_source_key(
                steering.identity,
                runtime.TEAM,
                receipt.api_app_id,
                receipt.channel,
                receipt.root_ts,
                receipt.message_ts,
            ),
        ]
    )
    audience = SlackThread(
        channel_identity=steering.identity,
        team_id=runtime.TEAM,
        app_id=receipt.api_app_id,
        channel_id=receipt.channel,
        root_ts=run.root_ts,
    )
    original = VoiceSource(
        tenant_id=DEFAULT_TENANT_ID,
        work_item_id=run.work_item_id,
        audience=audience,
        actor=Actor(provider_user_id=writer.slack_user, principal_id=writer.principal_id),
        attachment_id=receipt.voice_file_id,
        message_id=receipt.message_ts,
        content_sha256=receipt.content_sha256,
    )
    vectors = Path(__file__).resolve().parents[3] / "tests/vectors/factory-voice"
    audio = (vectors / "retry-label.m4a").read_bytes()
    key = source_key(original)
    steering.base.owned.callback(
        runtime._rows, "DELETE FROM curie.factory_voice_drafts WHERE source_key=:key", {"key": key}
    )
    file = dict(
        id=original.attachment_id,
        mode="hosted",
        mimetype="audio/mp4",
        subtype="slack_audio",
        user=writer.slack_user,
        is_external=False,
        size=len(audio),
    )
    message_files = [file]
    posts = []
    provider_calls = []

    def slack(request):
        method = request.url.path.rsplit("/", 1)[1]
        provider_calls.append(method)
        if method == "auth.test":
            answer = dict(
                team_id=runtime.TEAM, bot_id="B0EXAMPLE1", user_id=runtime.FACTORY_BOT_USER
            )
        elif method == "bots.info":
            answer = dict(bot=dict(id="B0EXAMPLE1", app_id=audience.app_id))
        elif method == "conversations.replies":
            message = dict(
                type="message",
                subtype="file_share",
                thread_ts=run.root_ts,
                ts=original.message_id,
                user=writer.slack_user,
                text=addressed,
                files=message_files,
            )
            answer = dict(messages=[message])
        else:
            assert method == "chat.postMessage"
            posts.append(json.loads(request.content))
            answer = dict(
                channel=audience.channel_id,
                ts="1800000000.000003",
                message=dict(thread_ts=run.root_ts),
            )
        return httpx.Response(200, json={"ok": True, **answer})

    def slack_file(**request):
        url = "https://files.slack.com/example-clip"
        if request["url"] == url:
            return FileResponse(200, {"Content-Type": "audio/mp4"}, iter([audio]))
        assert request["url"].endswith(f"files.info?file={original.attachment_id}")
        response = json.dumps({"ok": True, "file": {**file, "url_private_download": url}})
        return FileResponse(200, {}, iter([response.encode()]))

    @asynccontextmanager
    async def enabled_api():
        settings = get_settings().model_copy(update={"factory_voice_enabled": True})
        state = steering.client.app.state
        service = build_voice_draft_service(
            settings, state.sessionmaker, recognizer=FixtureVoiceRecognizer({audio: TRANSCRIPT})
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(slack)) as provider:
            ports = GatewayProviders(
                voice=SlackVoiceGateway(
                    provider, {steering.identity: "example"}, file_transport=slack_file
                )
            )
            app = gateway_application(
                tokens={"api": "example-api", "worker": "example-worker"},
                providers=ports,
                replay=RequestReplayStore(":memory:"),
                allow_insecure_http=True,
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                gateway = GatewayClient(
                    http, str(server.make_url("")), "example-api", allow_insecure_http=True
                )
                adapter = SlackVoice(
                    settings, state.sessionmaker, state.valkey, state.http_client, gateway, service
                )
                with monkeypatch.context() as patch:
                    patch.setattr(state, "factory_slack_voice", adapter)
                    yield adapter

    with steering.client.portal.wrap_async_context_manager(enabled_api()) as adapter:
        yield VoiceThread(
            writer, run, receipt, original, adapter, posts, provider_calls, message_files, steering
        )


def test_enabled_voice_route_commits_only_confirmed_feedback(
    steering, voice_thread: VoiceThread
) -> None:
    import re

    from curie_api.factory_voice_bindings import source_key

    writer, run, receipt = voice_thread.writer, voice_thread.run, voice_thread.receipt
    posts = voice_thread.posts
    other = steering.person()
    edited = "Keep the existing timeout. Change the button label to Retry."

    assert voice_thread.send(receipt)["outcome"] == "pending"
    code = voice_thread.display_draft()
    assert (
        voice_thread.send(voice_thread.reply(f"confirm {code}", actor=other))["reason"]
        == "forbidden"
    )
    assert (
        voice_thread.send(voice_thread.reply(f"confirm {code}", root="1800000009.000009"))["reason"]
        == "forbidden"
    )
    edit = voice_thread.reply(f"edit {code} {edited}")
    for wrong_route in ("steer", "kickoff"):
        assert voice_thread.send(edit, wrong_route)["outcome"] == "receipt_mismatch"
    assert voice_thread.send(edit)["outcome"] == "draft"
    voice_thread.pump()
    assert len(posts) == 2 and edited in posts[1]["text"]
    (new_code,) = re.findall(r"^confirm ([0-9a-f]{12}\.2)$", posts[1]["text"], re.M)
    assert voice_thread.send(voice_thread.reply(f"confirm {code}"))["reason"] == "stale_version"
    assert steering.revisions(run) == []
    assert (
        runtime._rows(
            "SELECT steering_id FROM curie.factory_steering_ledger WHERE work_item_id=:id",
            {"id": run.work_item_id},
        )
        == []
    )
    confirmation = voice_thread.reply(f"confirm {new_code}")
    assert voice_thread.send(confirmation) == {
        "outcome": "confirmed",
        "disposition": "queued",
        "replayed": False,
    }
    assert voice_thread.send(voice_thread.reply(f"confirm {new_code}")) == {
        "outcome": "confirmed",
        "disposition": "queued",
        "replayed": True,
    }
    (admitted,) = steering.revisions(run)
    (saved,) = runtime._rows(
        "SELECT d.status, d.transcript, d.admission_id, l.steering_id, l.accepted_text, "
        "l.source_facts FROM curie.factory_voice_drafts d "
        "JOIN curie.factory_steering_ledger l ON l.steering_id = d.admission_id "
        "WHERE d.source_key = :key AND l.work_item_id = :item",
        {"key": source_key(voice_thread.original), "item": run.work_item_id},
    )
    assert saved["status"] == "confirmed" and saved["transcript"] is None
    assert saved["admission_id"] == saved["steering_id"] == admitted
    assert saved["accepted_text"] == edited
    assert saved["source_facts"]["principal_id"] == str(writer.principal_id)
    assert saved["source_facts"]["publication"]["pr_number"] == run.pr


def test_removing_the_clip_after_draft_delivery_refuses_confirmation(
    voice_thread: VoiceThread,
) -> None:
    from curie_api.factory_voice_bindings import source_key

    code = voice_thread.display_draft()
    voice_thread.message_files.clear()
    result = voice_thread.send(voice_thread.reply(f"confirm {code}"))
    assert (result["outcome"], result["reason"]) == ("refused", "source_changed")
    assert voice_thread.stack.revisions(voice_thread.run) == []
    assert not runtime._rows(
        "SELECT steering_id FROM curie.factory_steering_ledger WHERE work_item_id=:id",
        {"id": voice_thread.run.work_item_id},
    )
    assert runtime._rows(
        "SELECT status, admission_id FROM curie.factory_voice_drafts WHERE source_key=:key",
        {"key": source_key(voice_thread.original)},
    ) == [{"status": "changed", "admission_id": None}]


def test_draft_delivery_requires_current_actor_and_code_host_authority(
    steering, voice_thread: VoiceThread
) -> None:
    original, adapter = voice_thread.original, voice_thread.adapter
    provider_calls = voice_thread.provider_calls
    draft = Draft(
        draft_id=uuid4(),
        audience=original.audience,
        version=1,
        transcript=TRANSCRIPT,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    steering.client.portal.call(adapter.deliver, original, draft.draft_id, draft)
    (post,) = voice_thread.posts
    assert post["channel"] == voice_thread.receipt.channel
    assert post["thread_ts"] == voice_thread.run.root_ts
    assert draft.transcript in post["text"] and draft.code in post["text"]
    seen = list(provider_calls)
    forged = replace(original, actor=replace(original.actor, principal_id=uuid4()))
    with pytest.raises(VoiceUnavailable):
        steering.client.portal.call(adapter.deliver, forged, draft.draft_id, draft)
    steering.revoke(voice_thread.writer.github_link_ids[0])
    with pytest.raises(VoiceUnavailable):
        steering.client.portal.call(adapter.deliver, original, draft.draft_id, draft)
    assert provider_calls == seen

    assert steering.revisions(voice_thread.run) == []


def test_voice_route_requires_the_worker_credential_and_redacts_invalid_requests() -> None:
    from curie_api.config import get_settings
    from curie_api.routers.factory_slack import internal_router
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.state.factory_slack_voice = None
    app.include_router(internal_router)
    data = payload(clip()).model_dump()
    endpoint = "/v1/internal/factory-slack/voice"
    with TestClient(app) as client:
        assert client.post(endpoint, json=data).status_code == 401
        worker = {"X-Curie-Worker-Token": get_settings().internal_worker_token}
        disabled = client.post(endpoint, headers=worker, json=data)
        assert disabled.status_code == 200
        assert disabled.json()["reason"] == "disabled"
        invalid = client.post(endpoint, headers=worker, json={"receipt": {"body": "private draft"}})
        assert invalid.status_code == 422
        assert invalid.json() == {"detail": {"code": "invalid_source"}}
        for response in (disabled, invalid):
            assert response.headers["Cache-Control"] == "no-store"
