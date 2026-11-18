"""Declared-driver kickoff and progress-card replies (#5380 AC2, ADR 0268, ADR 0202).

``test_every_api_outcome_has_a_worker_headline`` reads the outcome literals
either route can answer and requires a worker headline for each.
``test_a_declared_driver_kickoff_resolves_its_principal_and_steering_refuses_its_receipt``
admits a marked root mention from a declared test driver as its prelinked
principal, then refuses that driver's marked reply locally
``steering_evidence_missing`` without asking the steer route. A person's reply
in that root still reaches the steer route. The progress-card tests hand an
admitted kickoff's placeholder to the card and send no reply text, omit the
placeholder on a steer request, keep the reply when kickoff is refused, and
do not render admitted kickoff text.
``test_connect_plan_private_kickoff_settles_placeholder`` sends a private
connect-plan notice for a linked or unlinked sender, with and without OIDC,
and settles the public placeholder without the outcome code.

The boundary is ``test_factory_slack_runtime``'s steering stack: the real API
(``create_app()`` on uvicorn over a disposable migrated Postgres, real Valkey
and RustFS), the dispatcher's Bolt app driven through ``SocketModeHandler``,
and the worker as ``run.build`` composes it, consumed one stream entry at a
time through its real ``Consumer``. The kernel holds the production
``FactorySteeringClient``. Only external services are stand-ins: the Slack Web
API (``FakeSlack``), GitHub REST (``GitHubTruth``) and the Kubernetes API
(``KubernetesApi``).

Event shapes follow https://docs.slack.dev/reference/events/app_mention. A
bot-authored mention follows the shape observed over Socket Mode on a real
test workspace, 2026-11-12: ``user`` is the sending bot's own bot user,
``bot_id`` and ``bot_profile`` are set, ``team`` is the workspace, and there is
no ``user_team``; the envelope carries ``authorizations`` and
``is_ext_shared_channel: false``. A typed issue URL arrives wrapped as
``<URL>``. Enrollment headers are seeded with raw SQL, standing in for #5377's
enrollment writes. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import uuid
from collections.abc import Callable
from typing import Any, cast, get_args

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.models import DEFAULT_TENANT_ID
from curie_channel_gateway.slack.identities import delivery_key, minted_adapter
from curie_channel_gateway.slack.marked_actions import MARK
from curie_internal.driver_declaration import DeclaredDriver
from curie_internal.slack_mention_receipt import kickoff_request_id
from curie_worker import factory_steering as steering_module

from apps.api.tests import test_factory_slack_runtime as runtime
from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    APP_TOKEN,
    CHANNEL,
    FACTORY_BOT_ID,
    FACTORY_BOT_USER,
    PLACEHOLDER,
    REPO,
    SIBLING_BOT_ID,
    SIBLING_BOT_USER,
    TEAM,
    Attempt,
    Delivery,
    OwnerTuple,
    SteeringStack,
    SteeringWorker,
    _assert_no_sandbox,
    _no_false_nothing_queued,
    _receipt_key,
    _rows,
    _seed_principal,
    owner_tuples,
)
from apps.api.tests.test_factory_slack_runtime import steering as steering

KICKOFF_PATH = steering_module.KICKOFF_PATH
STEER_PATH = steering_module.STEER_PATH
KICKOFF_EVIDENCE_MISSING = steering_module.KICKOFF_EVIDENCE_MISSING
# The owner's enrollment generation. Not 1, so a tuple that names a default
# generation instead of the enrolled one is caught.
GENERATION = 2
DRIVER_BOT_ID = "B0EXAMPLE5"
DRIVER_BOT_USER = "U0EXAMPLE5"
DRIVER_APP_ID = "A0EXAMPLE5"
OTHER_USER = "U0EXAMPLE6"


@pytest.mark.parametrize(
    "linked,oidc", [(False, True), (True, True), (False, False), (True, False)]
)
def test_connect_plan_private_kickoff_settles_placeholder(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch, linked: bool, oidc: bool
) -> None:
    from apps.api.tests.factory_slack_kickoff_harness import (
        kick,
        linking,
        next_issue,
        report_auth_test,
    )
    from apps.api.tests.test_identity_connect import connect_plan_settings

    stack = steering
    report_auth_test(stack)
    connect_plan_settings(monkeypatch, oidc=oidc)
    user = stack.person().slack_user if linked else OTHER_USER
    start = len(stack.slack.calls)
    attempt = kick(stack, user, linking(next_issue()))
    code = "enrollment_inactive" if linked else "sender_unverified"
    assert attempt.answer["outcome"] == code
    private = [c for c in stack.slack.calls[start:] if c.method == "chat.postEphemeral" and c.ok]
    assert len(private) == 1
    post = private[0]
    assert post.params["user"] == user and post.token == stack.factory_token
    assert post.params["thread_ts"] == attempt.delivery.message_ts
    assert f"Code: {code}" not in post.text
    assert "Connect your Claude plan" in post.haystack if oidc else "operator" in post.text
    blocks = post.params.get("blocks", [])
    if isinstance(blocks, str):
        blocks = json.loads(blocks)
    assert [
        element["text"]
        for block in blocks
        if block.get("type") == "context"
        for element in block.get("elements", [])
    ].count(f"Code: {code}") == 1
    buttons = [
        element
        for block in blocks
        for element in block.get("elements", [])
        if element.get("type") == "button"
    ]
    assert len(buttons) == int(oidc)
    if oidc:
        assert buttons[0]["text"]["text"] == "Connect your Claude plan"
        assert buttons[0]["url"] == attempt.answer["plan_url" if linked else "connect_url"]
    public = "Not started. I sent you a private message about what to do next."
    assert [c.text for c in attempt.edits] == [public]
    assert attempt.posts == []
    assert all(code not in c.haystack for c in attempt.edits)
    assert all("/identity/connect/" not in c.haystack for c in attempt.edits)
    _assert_no_sandbox(stack)


_ISSUES = itertools.count(6401)
_TS = itertools.count(1)


# --- small views and inputs --------------------------------------------------------


def _issue_url(number: int, repo: str = REPO) -> str:
    return f"https://github.com/{repo}/issues/{number}"


def _linking(number: int, repo: str = REPO, *, lead: str = "Please start") -> str:
    """Message text naming one issue, as Slack wraps a typed URL."""

    return f"{lead} <{_issue_url(number, repo)}>"


def _root_ts() -> str:
    return f"1800000600.{next(_TS):06d}"


def _event_id(stack: SteeringStack) -> str:
    return f"Ev0{stack.tag.upper()}{uuid.uuid4().hex[:8].upper()}"


def _answers(stack: SteeringStack, path: str) -> list[tuple[int, Any]]:
    return [(status, body) for seen, status, body in stack.api_hop.exchanges if seen == path]


def _expected_request(stack: SteeringStack, delivery: Delivery) -> uuid.UUID:
    return kickoff_request_id(minted_adapter(stack.identity), delivery.delivery_id)


def _enroll(principal_id: uuid.UUID, *, generation: int = GENERATION) -> None:
    """An active enrollment header, standing in for #5377's enrollment writes."""

    _rows(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, true, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": DEFAULT_TENANT_ID,
            "principal": principal_id,
            "generation": generation,
        },
    )


def _personal(principal_id: uuid.UUID) -> OwnerTuple:
    return (DEFAULT_TENANT_ID, "personal", principal_id, GENERATION)


def _threads_at(root_ts: str) -> list[dict[str, Any]]:
    return _rows(
        "SELECT work_item_id, channel_identity, channel, root_ts, first_sequence, origin, "
        "card_ts, requester_slack_user "
        "FROM curie.factory_slack_threads WHERE root_ts = :root",
        {"root": root_ts},
    )


def _placeholder(stack: SteeringStack, since: int, thread_ts: str) -> str | None:
    posted = [
        call
        for call in stack.slack.calls[since:]
        if call.method == "chat.postMessage"
        and call.ok
        and call.text == PLACEHOLDER
        and call.params.get("thread_ts") == thread_ts
    ]
    return str(posted[-1].response["ts"]) if posted else None


def _bot_event(*, bot_id: str, bot_user: str, app_id: str, body: str) -> dict[str, Any]:
    """A bot-authored top-level mention, in the shape observed over Socket Mode."""

    ts = _root_ts()
    return {
        "type": "app_mention",
        "app_id": app_id,
        "bot_id": bot_id,
        "bot_profile": {
            "app_id": app_id,
            "deleted": False,
            "icons": {},
            "id": bot_id,
            "name": "acme-test-sender",
            "team_id": TEAM,
            "updated": 1800000000,
            "user_id": bot_user,
        },
        "channel": CHANNEL,
        "event_ts": ts,
        "team": TEAM,
        "text": f"<@{FACTORY_BOT_USER}> {body}",
        "ts": ts,
        "user": bot_user,
    }


def _deliver_envelope(stack: SteeringStack, event: dict[str, Any]) -> Delivery:
    """``SteeringStack._deliver`` with the whole envelope Slack sends a bot's mention.

    Observed over Socket Mode on a real test workspace, 2026-11-12: the payload
    carries ``authorizations`` (one entry, ``is_bot`` true, no enterprise) and
    ``is_ext_shared_channel: false``, which the sender evidence of a mention
    with no ``user_team`` is judged by.
    """

    from curie_channel_gateway.slack.app import build_app
    from curie_channel_gateway.slack.identities import SlackIdentityCredentials
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    from slack_bolt.authorization import AuthorizeResult
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.web import WebClient

    def authorize(**_kwargs: Any) -> AuthorizeResult:
        return AuthorizeResult(
            enterprise_id=None,
            team_id=TEAM,
            bot_token=stack.factory_token,
            bot_id=FACTORY_BOT_ID,
            bot_user_id=FACTORY_BOT_USER,
        )

    app = build_app(
        stack.dispatcher_config,
        web_client=WebClient(token=stack.factory_token, base_url=stack.slack.base_url),
        redis_client=stack.valkey,
        identity=SlackIdentityCredentials(
            name=stack.identity, app_token=APP_TOKEN, bot_token=stack.factory_token
        ),
        authorize=authorize,
        identity_bots={SIBLING_BOT_ID: SIBLING_BOT_USER},
    )
    errors: list[BaseException] = []

    @app.error
    def _collect(error: Exception) -> None:
        errors.append(error)

    event_id = _event_id(stack)
    delivery_id = delivery_key(event_id, stack.identity)
    stack.base.doomed_keys.append(_receipt_key(delivery_id))
    since = len(stack.slack.calls)
    SocketModeHandler(app, app_token=APP_TOKEN).handle(
        runtime._SocketClient(),
        SocketModeRequest(
            type="events_api",
            envelope_id=f"env-{uuid.uuid4().hex[:8]}",
            payload={
                "type": "event_callback",
                "api_app_id": "A0EXAMPLE1",
                "authorizations": [
                    {
                        "enterprise_id": None,
                        "is_bot": True,
                        "is_enterprise_install": False,
                        "team_id": TEAM,
                        "user_id": FACTORY_BOT_USER,
                    }
                ],
                "event": event,
                "event_context": "4-EXAMPLE",
                "event_id": event_id,
                "event_time": 1800000000,
                "is_ext_shared_channel": False,
                "team_id": TEAM,
            },
        ),
    )
    app.listener_runner.listener_executor.shutdown(wait=True)
    assert errors == []
    thread = str(event.get("thread_ts") or event["ts"])
    return Delivery(
        event_id, delivery_id, event["ts"], _placeholder(stack, since, thread), event["text"]
    )


def _attempt(
    stack: SteeringStack, delivery: Delivery, known: set[Any], answers: int, calls: int
) -> Attempt:
    new = [row for row in stack.requests() if row["id"] not in known]
    edits = [
        call
        for call in stack.slack.calls[calls:]
        if call.method == "chat.update"
        and delivery.placeholder is not None
        and call.params.get("ts") == delivery.placeholder
    ]
    return Attempt(delivery, _answers(stack, KICKOFF_PATH)[answers:], new, edits)


def _kick(
    stack: SteeringStack,
    deliver: Callable[[], Delivery],
    *,
    worker: SteeringWorker | None = None,
    before_consume: Callable[[Delivery], object] | None = None,
) -> Attempt:
    """One native delivery, then one consume by the worker, as one attempt."""

    known = {row["id"] for row in stack.requests()}
    answers = len(_answers(stack, KICKOFF_PATH))
    calls = len(stack.slack.calls)
    delivery = deliver()
    if before_consume is not None:
        before_consume(delivery)
    assert stack.consume(worker) is not None, "the dispatcher enqueued nothing"
    return _attempt(stack, delivery, known, answers, calls)


def _kicked_off(
    stack: SteeringStack, attempt: Attempt, *, owner: uuid.UUID, number: int, root_ts: str
) -> dict[str, Any]:
    """One admitted kickoff: the request, its personal WorkItem and its rooted thread."""

    reply = attempt.reply
    assert reply is not None and reply["outcome"] == "admitted", attempt.replies
    assert reply.get("replayed") is not True, reply
    expected = _expected_request(stack, attempt.delivery)
    assert reply["request_id"] == str(expected), reply
    assert reply["issue_url"] == _issue_url(number), reply
    (request,) = attempt.new_requests
    assert request["id"] == expected, request
    assert _issue_url(number) in request["objective"], request
    assert "Please start" not in request["objective"], request
    work_item_id = request["work_item_id"]
    assert str(work_item_id) == reply["work_item_id"], reply
    assert owner_tuples(work_item_id) == {
        work_item_id: _personal(owner),
        expected: _personal(owner),
    }
    (thread,) = _threads_at(root_ts)
    assert thread["work_item_id"] == work_item_id, thread
    assert (thread["channel_identity"], thread["channel"], thread["origin"]) == (
        stack.identity,
        CHANNEL,
        "kickoff",
    )
    # The placeholder is the run's progress card (#5715): the admitted turn
    # neither edits it nor posts a reply, and the thread stores its ts and
    # the sender the card's notify replies mention.
    assert attempt.delivery.placeholder is not None
    assert attempt.edits == [], [call.text for call in attempt.edits]
    assert stack.slack.thread_posts(stack.factory_token, root_ts) == []
    assert thread["card_ts"] == attempt.delivery.placeholder, thread
    assert request["requester"] == f"slack:{thread['requester_slack_user']}", thread
    assert stack.is_terminal(attempt.delivery.delivery_id)
    return request


# --- the parity gate ---------------------------------------------------------------


def test_every_api_outcome_has_a_worker_headline() -> None:
    """Every outcome either route can answer has a worker headline, named in its reply.

    Reads the outcome Literals of ``curie_api.schemas.workitems``, so an outcome
    added to a route without a worker headline fails here instead of reaching
    a person as the generic fallback.
    """

    from curie_api.schemas.workitems import FactorySlackKickoffOutcome, FactorySlackSteerOutcome

    steer = set(get_args(FactorySlackSteerOutcome))
    kickoff = set(get_args(FactorySlackKickoffOutcome))
    assert steer and kickoff
    assert steer - set(steering_module._HEADLINES) == set()
    assert kickoff - set(steering_module._KICKOFF_HEADLINES) == {"admitted"}
    local = {
        steering_module.KICKOFF_EVIDENCE_MISSING,
        steering_module.KICKOFF_UNCONFIGURED,
        steering_module.KICKOFF_BUDGET_EXPIRED,
    }
    assert local <= set(steering_module._KICKOFF_HEADLINES)
    # An admitted kickoff has no reply text: its answer is the progress card.
    refusals = (kickoff | local) - {"admitted"}
    assert set(steering_module._KICKOFF_HEADLINES) <= refusals
    for outcome in refusals:
        reply = steering_module.render_kickoff_reply(outcome, reason="example_reason")
        assert f"Outcome: {outcome} (example_reason)" in reply, reply
        _no_false_nothing_queued([reply])


def test_a_declared_driver_kickoff_resolves_its_principal_and_steering_refuses_its_receipt(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ADR 0202: a declared test driver kicks off as its prelinked principal, never steers.

    The installation is a test installation at the dispatcher and the API,
    declaring one driver bot in this channel. The driver's marked root mention
    (the observed bot shape: no ``user_team``) admits a run owned by the
    principal its bot user is linked to. Its marked reply in that run's root
    carries a version 2 receipt, which steering refuses locally
    ``steering_evidence_missing`` without asking the steer route. Liveness: a
    person's reply there reaches the steer route.
    """

    stack = steering
    driver = DeclaredDriver(channel_id=CHANNEL, bot_id=DRIVER_BOT_ID, bot_user_id=DRIVER_BOT_USER)
    settings = get_settings()
    monkeypatch.setattr(settings, "test_installation_enabled", True)
    monkeypatch.setattr(settings, "test_installation_drivers", (driver,))
    stack.dispatcher_config = stack.dispatcher_config.model_copy(
        update={"test_installation_enabled": True, "test_installation_drivers": (driver,)}
    )
    # What the dispatcher's preflight reports of its own auth.test, which the
    # sender evidence of a mention with no user_team is judged against.
    reported = stack.client.post(
        "/identity/slack-reports",
        headers=stack.platform,
        json={
            "name": stack.identity,
            "team_id": TEAM,
            "enterprise_id": None,
            "enterprise_id_present": True,
            "is_enterprise_install": False,
        },
    )
    assert reported.status_code == 200, reported.text
    principal_id, _link = _seed_principal(DRIVER_BOT_USER)
    _enroll(principal_id)
    number = next(_ISSUES)

    started = _kick(
        stack,
        lambda: _deliver_envelope(
            stack,
            _bot_event(
                bot_id=DRIVER_BOT_ID,
                bot_user=DRIVER_BOT_USER,
                app_id=DRIVER_APP_ID,
                body=f"{MARK} {_linking(number)}",
            ),
        ),
    )
    receipt = json.loads(str(stack.valkey.get(_receipt_key(started.delivery.delivery_id))))
    assert (receipt["version"], receipt["driver_bot_id"]) == (2, DRIVER_BOT_ID), receipt
    request = _kicked_off(
        stack,
        started,
        owner=principal_id,
        number=number,
        root_ts=started.delivery.message_ts,
    )
    assert request["requester"] == f"slack:{DRIVER_BOT_USER}", request

    root = started.delivery.message_ts
    reply_event = _bot_event(
        bot_id=DRIVER_BOT_ID,
        bot_user=DRIVER_BOT_USER,
        app_id=DRIVER_APP_ID,
        body=f"{MARK} Please also rename the helper.",
    )
    reply_event.update({"thread_ts": root, "parent_user_id": DRIVER_BOT_USER})
    calls = len(stack.slack.calls)
    replied = _deliver_envelope(stack, reply_event)
    assert stack.consume() is not None
    receipt = json.loads(str(stack.valkey.get(_receipt_key(replied.delivery_id))))
    assert receipt["version"] == 2, receipt
    assert _answers(stack, STEER_PATH) == [], "a driver's receipt reached the steer route"
    edits = [
        call.text
        for call in stack.slack.calls[calls:]
        if call.method == "chat.update" and call.params.get("ts") == replied.placeholder
    ]
    assert edits and f"Outcome: {steering_module.EVIDENCE_MISSING}" in edits[-1], edits
    assert [row["id"] for row in stack.requests()] == [request["id"]]

    writer = stack.person()
    runtime._api_refused(
        stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=root),
        "lineage_closed",
    )
    _assert_no_sandbox(stack)


# --- #5715: the admitted kickoff hands its placeholder to the progress card ---------
#
# Pure: the production client over an in-memory HTTP transport, with the stored
# receipt read stubbed. The mapped read and the database are never touched.

_CARD_IDENTITY = "default"
_CARD_ROOT = "1800000900.000100"
_CARD_PLACEHOLDER = "1800000900.000200"


def _card_turn(placeholder: str | None) -> Any:
    from aci_protocol import QueuedTurn, ReplyHandle

    handle = ReplyHandle(kind="slack", channel=CHANNEL, placeholder=placeholder)
    qevent = QueuedTurn(
        event_id="EvCARD-1",
        conversation_id=_CARD_ROOT,
        author="U0EXAMPLE9",
        text=_linking(7),
        reply_handle=handle,
        received_at="2026-11-16T00:00:00+00:00",
    )
    return steering_module._Turn(qevent, handle, _CARD_IDENTITY, _CARD_ROOT)


def _card_receipt(turn: Any, *, thread_reply: bool = False) -> Any:
    from curie_internal.slack_mention_receipt import SlackMentionReceipt

    return SlackMentionReceipt(
        version=1,
        delivery_id=turn.qevent.event_id,
        channel_identity=turn.identity,
        channel=CHANNEL,
        root_ts=_CARD_ROOT,
        message_ts="1800000900.000300" if thread_reply else _CARD_ROOT,
        user_id=turn.qevent.author,
        body=turn.qevent.text,
        evidence={},
    )


def _card_client(
    answer: dict[str, Any], sent: list[tuple[str, dict[str, Any]]]
) -> steering_module.FactorySteeringClient:
    def handle(request: httpx.Request) -> httpx.Response:
        sent.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json=answer)

    return steering_module.FactorySteeringClient(
        engine=cast(Any, None),
        schema="curie",
        redis=cast(Any, None),
        api_base_url="http://api.invalid",
        worker_token="example-worker-token",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        intent_key=lambda event_id: f"intent:{event_id}",
        replies=runtime.SlackReplyAdapter(
            "example-token", base_url="https://slack.example.com/api/"
        ),
    )


def _card_decide(
    monkeypatch: pytest.MonkeyPatch,
    answer: dict[str, Any],
    *,
    placeholder: str | None,
    steer: bool = False,
    receipt: bool = True,
) -> tuple[steering_module.SteeringReply | None, list[tuple[str, dict[str, Any]]]]:
    turn = _card_turn(placeholder)
    stored = _card_receipt(turn, thread_reply=steer) if receipt else None

    async def read_receipt(_redis: Any, _delivery: str) -> Any:
        return stored

    async def record_intent() -> None:
        return None

    monkeypatch.setattr(steering_module, "read_receipt", read_receipt)
    sent: list[tuple[str, dict[str, Any]]] = []
    client = _card_client(answer, sent)

    async def run() -> steering_module.SteeringReply | None:
        if steer:
            return await client._steer(turn, deadline=None, record_intent=record_intent)
        return await client._kickoff(turn, mapped=False, deadline=None, record_intent=record_intent)

    return asyncio.run(run()), sent


_ADMITTED = {
    "outcome": "admitted",
    "request_id": "00000000-0000-4000-8000-000000000001",
    "work_item_id": "00000000-0000-4000-8000-000000000002",
    "issue_url": _issue_url(7),
}


@pytest.mark.parametrize("replayed", [False, True])
@pytest.mark.parametrize("placeholder", [_CARD_PLACEHOLDER, None])
def test_progress_card_admitted_kickoff_sends_its_placeholder_and_has_no_reply_text(
    monkeypatch: pytest.MonkeyPatch, placeholder: str | None, replayed: bool
) -> None:
    """An admitted kickoff, fresh or replayed, is answered by the card, not a reply.

    The kickoff request carries the turn's placeholder ts, null when the
    dispatcher posted none, so the API can store it as the run's card.
    """

    reply, sent = _card_decide(
        monkeypatch, {**_ADMITTED, "replayed": replayed}, placeholder=placeholder
    )
    ((path, body),) = sent
    assert path == KICKOFF_PATH
    assert "placeholder_ts" in body and body["placeholder_ts"] == placeholder, body
    assert reply is not None and reply.admitted and reply.progress_card, reply
    assert reply.text == "", reply.text
    assert reply.request_id == _ADMITTED["request_id"], reply


def test_progress_card_steer_request_carries_no_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The steer route's input forbids extra fields, and a revision keeps its reply."""

    answer = {"outcome": "admitted", "request_id": _ADMITTED["request_id"]}
    reply, sent = _card_decide(monkeypatch, answer, placeholder=_CARD_PLACEHOLDER, steer=True)
    ((path, body),) = sent
    assert path == STEER_PATH
    assert "placeholder_ts" not in body, body
    assert reply is not None and reply.admitted and not reply.progress_card, reply
    assert reply.text == steering_module.render_reply("admitted"), reply.text


@pytest.mark.parametrize("outcome", ["issue_closed", "thread_conflict", "admission_refused"])
def test_progress_card_kickoff_refusals_keep_their_reply(
    monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    """A refused kickoff still replies with its headline and outcome code."""

    reply, _sent = _card_decide(
        monkeypatch, {"outcome": outcome, "reason": "example_reason"}, placeholder=None
    )
    assert reply is not None and not reply.admitted and not reply.progress_card, reply
    assert reply.text == steering_module.render_kickoff_reply(outcome, reason="example_reason")
    assert f"Outcome: {outcome} (example_reason)" in reply.text, reply.text


def test_progress_card_local_kickoff_refusal_keeps_its_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A kickoff with no stored receipt is refused locally, with no API call and its reply."""

    reply, sent = _card_decide(monkeypatch, _ADMITTED, placeholder=_CARD_PLACEHOLDER, receipt=False)
    assert sent == []
    assert reply is not None and reply.outcome == KICKOFF_EVIDENCE_MISSING, reply
    assert not reply.progress_card
    assert f"Outcome: {KICKOFF_EVIDENCE_MISSING}" in reply.text, reply.text


def test_progress_card_admitted_kickoff_has_no_rendered_reply() -> None:
    """Nothing renders admitted kickoff text: the card replaces it (decision 9)."""

    with pytest.raises(ValueError, match="progress card"):
        steering_module.render_kickoff_reply("admitted")
