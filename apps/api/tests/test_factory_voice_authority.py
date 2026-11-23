"""The Slack clip author must still have steering authority before each media read."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace

import httpx
import pytest
import test_factory_slack_runtime as runtime
from curie_api.config import get_settings
from curie_api.factory_confirmed_steering import (
    ConfirmedText,
    QueuedSteering,
    RefusedSteering,
    SlackSteeringSource,
)
from curie_api.factory_slack import (
    SlackVoiceAuthorization,
    admit_confirmed_text,
    authorize_voice_source,
)
from curie_api.identity.slack import SlackEvidence
from curie_api.schemas.workitems import FactorySlackSteerIn
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

steering = runtime.steering


def _source(
    *, author: str = "U0EXAMPLE1", identity: str = "default", root: str = "100.001"
) -> tuple[FactorySlackSteerIn, SlackEvidence]:
    evidence = SlackEvidence(
        delivery="app_mention",
        user_id=author,
        event_user_team=runtime.TEAM,
        event_team=runtime.TEAM,
        is_ext_shared_channel=False,
    )
    data = FactorySlackSteerIn(
        delivery_id="EvEXAMPLE1",
        channel_identity=identity,
        channel=runtime.CHANNEL,
        root_ts=root,
        author=author,
        receipt={
            "version": 3,
            "delivery_id": "EvEXAMPLE1",
            "channel_identity": identity,
            "channel": runtime.CHANNEL,
            "root_ts": root,
            "message_ts": "1800000999.000001",
            "user_id": author,
            "voice_file_id": "F0EXAMPLE1",
            "team_id": runtime.TEAM,
            "api_app_id": "A0EXAMPLE1",
            "content_sha256": "a" * 64,
            "body": "",
            "evidence": evidence.model_dump(),
        },
    )
    return data, evidence


def _enable_public_mode(stack: runtime.SteeringStack) -> None:
    from curie_api.factory_slack_public import reserve_public_root

    bound = stack.client.post(
        f"/agents/{stack.agent_id}/channels",
        headers=stack.platform,
        json={"kind": "slack", "address": runtime.OTHER_CHANNEL, "adapter": stack.identity},
    )
    assert bound.status_code == 201
    from test_factory_public_notifications import configure_public

    with pytest.MonkeyPatch.context() as patch:
        result = configure_public(stack, patch, runtime.OTHER_CHANNEL)
        assert result.is_success, result.text

    async def reserve(engine: AsyncEngine) -> None:
        async with AsyncSession(engine) as session, session.begin():
            for request in stack_requests:
                await reserve_public_root(session, request["work_item_id"], request["sequence"])

    stack_requests = stack.requests()
    runtime._db(reserve)


def _authorize(
    stack: runtime.SteeringStack, person: runtime.Person, root: str
) -> SlackVoiceAuthorization | RefusedSteering:
    data, evidence = _source(author=person.slack_user, identity=stack.identity, root=root)

    async def go(engine: AsyncEngine) -> SlackVoiceAuthorization | RefusedSteering:
        async with (
            AsyncSession(engine) as session,
            httpx.AsyncClient(transport=runtime.GitHubTransport(stack.github)) as client,
        ):
            return await authorize_voice_source(
                session, data, evidence, settings=get_settings(), client=client
            )

    return runtime._db(go)


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("message_ts", "100.001"),
        ("version", 2),
        ("version", 1),
        ("driver_bot_id", "B0EXAMPLE1"),
        ("channel_identity", "another"),
        ("channel", "C0EXAMPLE2"),
        ("root_ts", "200.002"),
        ("user_id", "U0EXAMPLE2"),
        ("delivery_id", "EvEXAMPLE2"),
        ("voice_file_id", None),
        ("content_sha256", ""),
        ("body", "unconfirmed"),
        ("evidence", {"delivery": "app_mention", "user_id": "U0EXAMPLE1"}),
    ],
)
async def test_unbound_voice_receipt_refused_before_database_or_provider(
    field: str, value: object
) -> None:
    data, evidence = _source()
    data.receipt[field] = value

    def unexpected(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"unexpected provider request {request.method}")

    async with AsyncSession() as session, httpx.AsyncClient(
        transport=httpx.MockTransport(unexpected)
    ) as client:
        result = await authorize_voice_source(
            session, data, evidence, settings=get_settings(), client=client
        )
    assert result == RefusedSteering(reason="receipt_mismatch")


def test_voice_reread_checks_permissions_before_and_after_publication(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    writer = stack.person()
    reader = stack.person(permission="read")
    request_id, item_id = stack.admit(next(runtime._ISSUES))
    epoch = stack.start(request_id)
    assert stack.enable_destination().status_code == 200
    stack.drain()
    (thread,) = stack.threads(item_id)
    root = thread["root_ts"]
    known = {row["id"] for row in stack.requests()}
    assert _authorize(stack, writer, root) == RefusedSteering(reason="no_pull_request")
    _enable_public_mode(stack)
    prepublication = _authorize(stack, writer, root)
    assert isinstance(prepublication, SlackVoiceAuthorization)
    assert (prepublication.work_item_id, prepublication.principal_id) == (
        item_id,
        writer.principal_id,
    )
    assert _authorize(stack, reader, root) == RefusedSteering(reason="not_authorized")
    stack.finish(request_id, epoch)
    reads = list(stack.github.paths)
    assert _authorize(stack, writer, root) == RefusedSteering(reason="no_pull_request")
    assert stack.github.paths == reads

    number = next(runtime._PULLS)
    _, branch = stack.own_pull_request(item_id, number)
    stack.github.open_pull(number, branch)
    assert isinstance(_authorize(stack, writer, root), SlackVoiceAuthorization)
    stack.github.pulls[number]["head"]["ref"] = "another-branch"
    assert _authorize(stack, writer, root) == RefusedSteering(reason="pull_request_branch_mismatch")
    stack.github.pulls[number]["head"]["ref"] = branch
    stack.github.pulls[number]["state"] = "closed"
    assert _authorize(stack, writer, root) == RefusedSteering(reason="pull_request_not_open")
    assert {row["id"] for row in stack.requests()} == known


def test_only_the_current_personal_owner_may_read_the_voice_source(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    owner = stack.person()
    other = stack.person()
    runtime._enroll(owner.principal_id, generation=3)
    run = stack.open_steered_run(owner=runtime._owner(owner.principal_id, generation=3))
    known = {row["id"] for row in stack.requests()}
    _enable_public_mode(stack)
    assert isinstance(_authorize(stack, owner, run.root_ts), SlackVoiceAuthorization)
    reads = list(stack.github.paths)
    assert _authorize(stack, other, run.root_ts) == RefusedSteering(reason="owner_mismatch")
    runtime._set_enrollment(owner.principal_id, generation=4, active=True)
    assert _authorize(stack, owner, run.root_ts) == RefusedSteering(
        reason="enrollment_generation_mismatch"
    )
    runtime._set_enrollment(owner.principal_id, generation=4, active=False)
    assert _authorize(stack, owner, run.root_ts) == RefusedSteering(reason="enrollment_inactive")
    assert stack.github.paths == reads
    assert {row["id"] for row in stack.requests()} == known


def _confirmed(
    stack: runtime.SteeringStack,
    writer: runtime.Person,
    root: str,
) -> tuple[SlackVoiceAuthorization, ConfirmedText, FactorySlackSteerIn]:
    authority = _authorize(stack, writer, root)
    assert isinstance(authority, SlackVoiceAuthorization)
    source, evidence = _source(author=writer.slack_user, identity=stack.identity, root=root)
    draft = uuid.uuid4()
    confirmation = source.model_copy(
        update={
            "delivery_id": "EvEXAMPLE2",
            "receipt": {
                "version": 1,
                "channel_identity": stack.identity,
                "channel": runtime.CHANNEL,
                "root_ts": root,
                "message_ts": "1800000999.000002",
                "delivery_id": "EvEXAMPLE2",
                "user_id": writer.slack_user,
                "evidence": evidence.model_dump(),
                "team_id": runtime.TEAM,
                "api_app_id": "A0EXAMPLE1",
                "bot_user_id": runtime.FACTORY_BOT_USER,
                "body": f"<@{runtime.FACTORY_BOT_USER}> confirm {draft.hex[:12]}.1",
            },
        }
    )
    command = ConfirmedText(
        draft_id=draft,
        work_item_id=authority.work_item_id,
        text="Preserve these exact words.\n",
        source_content_sha256=source.receipt["content_sha256"],
        source=SlackSteeringSource(
            channel_identity=stack.identity,
            channel=runtime.CHANNEL,
            root_ts=root,
            message_ts=source.receipt["message_ts"],
            file_id=source.receipt["voice_file_id"],
            author=writer.slack_user,
            principal_id=writer.principal_id,
            confirmation_delivery_id=confirmation.delivery_id,
        ),
    )
    return authority, command, confirmation


def test_concurrent_confirmations_replay_exactly_once_and_caller_rollback_is_atomic(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    run = stack.open_steered_run()
    _enable_public_mode(stack)
    authority, command, confirmation = _confirmed(stack, stack.person(), run.root_ts)
    assert isinstance(command.source, SlackSteeringSource)
    fresh = confirmation.model_copy(
        update={
            "delivery_id": "EvEXAMPLE3",
            "receipt": {**confirmation.receipt, "delivery_id": "EvEXAMPLE3"},
        }
    )
    repeated = replace(
        command, source=replace(command.source, confirmation_delivery_id=fresh.delivery_id)
    )
    reads = list(stack.github.paths)

    async def go(engine: AsyncEngine) -> None:
        async def confirm(session: AsyncSession, cmd: ConfirmedText, receipt: FactorySlackSteerIn):
            return await admit_confirmed_text(
                session, cmd, settings=get_settings(), authority=authority, confirmation=receipt
            )

        async with AsyncSession(engine) as session:
            result = await confirm(session, command, confirmation)
            assert result == QueuedSteering(
                steering_id=command.steering_id,
                execution_request_id=command.steering_id,
                replayed=False,
            )
            await session.rollback()
            for table, column in (
                ("factory_steering_ledger", "steering_id"),
                ("execution_requests", "id"),
            ):
                assert (
                    await session.scalar(
                        text(f"SELECT count(*) FROM curie.{table} WHERE {column} = :id"),
                        {"id": command.steering_id},
                    )
                    == 0
                )

        async def race(cmd: ConfirmedText, receipt: FactorySlackSteerIn):
            async with AsyncSession(engine) as session:
                answer = await confirm(session, cmd, receipt)
                await session.commit()
                return answer

        answers = await asyncio.gather(race(command, confirmation), race(repeated, fresh))
        assert set(answers) == {
            QueuedSteering(
                steering_id=command.steering_id,
                execution_request_id=command.steering_id,
                replayed=replay,
            )
            for replay in (False, True)
        }
        async with AsyncSession(engine) as session:
            assert await confirm(
                session, replace(command, text="altered"), confirmation
            ) == RefusedSteering(reason="source_changed")
            assert await confirm(
                session, replace(command, work_item_id=uuid.uuid4()), confirmation
            ) == RefusedSteering(reason="target_mismatch")
            for key, value in (
                ("team_id", "T0EXAMPLE9"),
                ("api_app_id", "A0EXAMPLE9"),
                ("message_ts", authority.source.message_ts),
                ("user_id", "U0EXAMPLE9"),
                ("body", f"<@{runtime.FACTORY_BOT_USER}> reject {command.draft_id.hex[:12]}.1"),
            ):
                changed = confirmation.model_copy(
                    update={"receipt": {**confirmation.receipt, key: value}}
                )
                assert await confirm(session, command, changed) == RefusedSteering(
                    reason="source_changed"
                )
            row = (
                await session.execute(
                    text(
                        "SELECT accepted_text, state, first_delivery_id "
                        "FROM curie.factory_steering_ledger WHERE work_item_id=:id"
                    ),
                    {"id": run.work_item_id},
                )
            ).one()
            assert row[:2] == (command.text, "queued")
            assert row.first_delivery_id in {confirmation.delivery_id, fresh.delivery_id}

    runtime._db(go)
    assert stack.github.paths == reads
    assert stack.revisions(run) == [command.steering_id]


def test_ledger_write_error_rolls_back_revision_and_keeps_confirmation_retryable(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    run = stack.open_steered_run()
    _enable_public_mode(stack)
    authority, command, receipt = _confirmed(stack, stack.person(), run.root_ts)
    # Fail just the ledger INSERT, after revision creation, in this WorkItem's transaction.
    fn = f"curie.reject_guidance_{stack.tag}"
    remove = f"DROP FUNCTION IF EXISTS {fn}() CASCADE"
    stack.base.owned.callback(runtime._rows, remove)
    runtime._rows(
        f"CREATE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        f"IF NEW.work_item_id='{run.work_item_id}' THEN RAISE EXCEPTION 'refused'; "
        "END IF; RETURN NEW; END $$"
    )
    runtime._rows(
        f"CREATE TRIGGER reject_guidance_{stack.tag} BEFORE INSERT "
        f"ON curie.factory_steering_ledger FOR EACH ROW EXECUTE FUNCTION {fn}()"
    )

    async def confirm(engine: AsyncEngine):
        async with AsyncSession(engine) as session, session.begin():
            return await admit_confirmed_text(
                session, command, settings=get_settings(), authority=authority, confirmation=receipt
            )

    with pytest.raises(DBAPIError):
        runtime._db(confirm)
    assert stack.revisions(run) == []
    assert (
        runtime._rows(
            "SELECT steering_id FROM curie.factory_steering_ledger WHERE work_item_id=:id",
            {"id": run.work_item_id},
        )
        == []
    )
    runtime._rows(remove)
    assert runtime._db(confirm) == QueuedSteering(
        steering_id=command.steering_id, execution_request_id=command.steering_id, replayed=False
    )


def test_prepublication_guidance_rechecks_publication_before_commit_without_provider_io(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    writer = stack.person()
    request, item = stack.admit(next(runtime._ISSUES))
    stack.start(request)
    assert stack.enable_destination().status_code == 200
    stack.drain()
    (thread,) = stack.threads(item)
    _enable_public_mode(stack)
    authority, command, receipt = _confirmed(stack, writer, thread["root_ts"])
    pr = next(runtime._PULLS)
    _, branch = stack.own_pull_request(item, pr)
    stack.github.open_pull(pr, branch)
    reads = list(stack.github.paths)

    async def confirm(engine: AsyncEngine):
        async with AsyncSession(engine) as session, session.begin():
            return await admit_confirmed_text(
                session, command, settings=get_settings(), authority=authority, confirmation=receipt
            )

    assert runtime._db(confirm) == RefusedSteering(reason="authority_changed")
    assert stack.github.paths == reads
    assert len(stack.requests()) == 1
    assert (
        runtime._rows(
            "SELECT steering_id FROM curie.factory_steering_ledger WHERE work_item_id=:id",
            {"id": item},
        )
        == []
    )


def test_ordinary_steering_queues_eight_entries_then_only_accepts_replay_or_a_published_revision(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    writer = stack.person()
    other = stack.person()
    viewer = stack.person(permission="read")
    request, item = stack.admit(next(runtime._ISSUES))
    stack.start(request)
    assert stack.enable_destination().status_code == 200
    stack.drain()
    (thread,) = stack.threads(item)
    root = thread["root_ts"]
    _enable_public_mode(stack)
    denied = stack.steer_queued(viewer.slack_user, "view only", thread_ts=root)
    runtime._api_refused(denied, "not_authorized")
    first = None
    for index in range(8):
        author = writer if index % 2 else other
        result = stack.steer_queued(author.slack_user, f"change {index}", thread_ts=root)
        assert result.reply and result.reply["outcome"] == "queued"
        assert result.reply["request_id"] is None and result.new_requests == []
        if first is None:
            first = result.delivery
    assert first is not None
    receipt = json.loads(stack.valkey.get(runtime._receipt_key(first.delivery_id)))
    repeated = stack.client.post(
        runtime.STEER_PATH,
        headers=stack.internal,
        json={
            "delivery_id": first.delivery_id,
            "channel_identity": receipt["channel_identity"],
            "channel": receipt["channel"],
            "root_ts": root,
            "author": receipt["user_id"],
            "receipt": receipt,
        },
    )
    assert repeated.status_code == 200
    assert repeated.json()["outcome"] == "queued" and repeated.json()["replayed"] is True
    runtime._api_refused(
        stack.steer_queued(writer.slack_user, "overflow", thread_ts=root), "guidance_queue_full"
    )
    rows = runtime._rows(
        "SELECT state,accepted_text,execution_request_id FROM curie.factory_steering_ledger "
        "WHERE work_item_id=:id ORDER BY created_at, steering_id",
        {"id": item},
    )
    assert len(rows) == 8 and all(
        row["state"] == "queued" and row["execution_request_id"] is None for row in rows
    )
    assert rows[0]["accepted_text"] == first.text
    pr = next(runtime._PULLS)
    _, branch = stack.own_pull_request(item, pr)
    stack.github.open_pull(pr, branch)
    post = stack.steer_queued(other.slack_user, "on the same branch", thread_ts=root)
    assert post.reply and post.reply["outcome"] == "queued"
    (revision,) = post.new_requests
    assert revision["work_item_id"] == item
    assert revision["reply_target_pr_number"] == str(pr)
    assert str(revision["id"]) == post.reply["request_id"]
    assert revision["objective"].count(post.delivery.text) == 1
    assert len(stack.requests()) == 2


def test_preledger_revision_replay_compares_the_committed_actor_and_full_body(
    steering: runtime.SteeringStack,
) -> None:
    stack = steering
    run = stack.open_steered_run()
    writer = stack.person()
    result = stack.steer_queued(
        writer.slack_user, "Retain this exact detail.", thread_ts=run.root_ts
    )
    runtime._admitted(result)
    (revision,) = result.new_requests
    assert (
        runtime._rows(
            "SELECT steering_id FROM curie.factory_steering_ledger WHERE work_item_id=:id",
            {"id": run.work_item_id},
        )
        == []
    )
    _enable_public_mode(stack)
    receipt = json.loads(stack.valkey.get(runtime._receipt_key(result.delivery.delivery_id)))

    def replay(body: str) -> dict:
        answer = stack.client.post(
            runtime.STEER_PATH,
            headers=stack.internal,
            json={
                "delivery_id": result.delivery.delivery_id,
                "channel_identity": receipt["channel_identity"],
                "channel": receipt["channel"],
                "root_ts": run.root_ts,
                "author": writer.slack_user,
                "receipt": {**receipt, "body": body},
            },
        )
        assert answer.status_code == 200
        return answer.json()

    assert replay("modified")["outcome"] == "source_changed"
    old = replay(result.delivery.text)
    assert (
        old["outcome"] == "queued" and old["replayed"] and old["request_id"] == str(revision["id"])
    )
    assert stack.revisions(run) == [revision["id"]]
    (row,) = runtime._rows(
        "SELECT state, objective_embedded, applied_at FROM curie.factory_steering_ledger "
        "WHERE steering_id=:id",
        {"id": revision["id"]},
    )
    assert row == {"state": "queued", "objective_embedded": True, "applied_at": None}


def test_public_guidance_settles_the_placeholder_and_publishes_one_queued_status(
    steering: runtime.SteeringStack,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_factory_public_notifications import configure_public, drain_public

    stack = steering
    configured = configure_public(stack, monkeypatch, runtime.CHANNEL)
    assert configured.is_success, configured.text
    stack.admit(next(runtime._ISSUES))
    drain_public(stack)
    (posted,) = stack.slack.roots(stack.factory_token)
    root = posted.response["ts"]
    writer = stack.person()
    marker = "Only the execution should see my guidance."
    result = stack.steer_queued(writer.slack_user, marker, thread_ts=root)
    assert result.reply and result.reply["outcome"] == "queued"
    assert result.reply["pull_request_shareable"] is False
    assert "pull_request_url" not in result.reply
    assert result.delivery.placeholder is not None
    assert [edit.text for edit in result.edits] == ["Received."]
    drain_public(stack)
    drain_public(stack)
    records = runtime._rows(
        "SELECT posted_ref FROM curie.factory_slack_public_posts WHERE kind = 'steering_queued'"
    )
    (record,) = records
    assert record["posted_ref"]
    messages = stack.slack.thread_posts(stack.factory_token, root)
    (queued,) = [post for post in messages if post.response.get("ts") == record["posted_ref"]]
    assert "queued" in queued.text.lower()
    assert all(marker not in post.haystack for post in messages)
