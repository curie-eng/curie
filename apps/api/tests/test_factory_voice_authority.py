"""The Slack clip author must still have steering authority before each media read."""

from __future__ import annotations

import httpx
import pytest
import test_factory_slack_runtime as runtime
from curie_api.config import get_settings
from curie_api.factory_confirmed_steering import RefusedSteering
from curie_api.factory_slack import SlackVoiceAuthorization, authorize_voice_source
from curie_api.identity.slack import SlackEvidence
from curie_api.schemas.workitems import FactorySlackSteerIn
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


def _authorize(
    stack: runtime.SteeringStack, person: runtime.Person, root: str
) -> SlackVoiceAuthorization | RefusedSteering:
    data, evidence = _source(author=person.slack_user, identity=stack.identity, root=root)

    async def go(engine: AsyncEngine) -> SlackVoiceAuthorization | RefusedSteering:
        async with AsyncSession(engine) as session, httpx.AsyncClient(
            transport=runtime.GitHubTransport(stack.github)
        ) as client:
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
