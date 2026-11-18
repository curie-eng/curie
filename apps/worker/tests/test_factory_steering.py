"""Declared-driver kickoff and kickoff reply headlines (#5380 AC2, ADR 0268, ADR 0202).

``test_every_api_outcome_has_a_worker_headline`` reads the kickoff and steer
outcome literals and requires a worker headline for each, including the local
refusals. ``test_a_declared_driver_kickoff_resolves_its_principal_and_steering_refuses_its_receipt``
admits a marked root mention from a declared test driver as its prelinked
principal, then refuses that driver's marked reply locally
``steering_evidence_missing`` without asking the steer route. A person's reply
in that root still reaches the steer route.

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

import itertools
import json
import uuid
from collections.abc import Callable
from typing import Any, get_args

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
# The owner's enrollment generation. Not 1, so a tuple that names a default
# generation instead of the enrolled one is caught.
GENERATION = 2
DRIVER_BOT_ID = "B0EXAMPLE5"
DRIVER_BOT_USER = "U0EXAMPLE5"
DRIVER_APP_ID = "A0EXAMPLE5"

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
        "SELECT work_item_id, channel_identity, channel, root_ts, first_sequence, origin "
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
    assert attempt.edits, "the placeholder was not edited with the reply"
    assert attempt.edits[-1].text == steering_module.render_kickoff_reply(
        "admitted", issue_url=_issue_url(number)
    ), attempt.edits[-1].text
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
    assert kickoff - set(steering_module._KICKOFF_HEADLINES) == set()
    local = {
        steering_module.KICKOFF_EVIDENCE_MISSING,
        steering_module.KICKOFF_UNCONFIGURED,
        steering_module.KICKOFF_BUDGET_EXPIRED,
    }
    assert local <= set(steering_module._KICKOFF_HEADLINES)
    for outcome in kickoff | local:
        reply = steering_module.render_kickoff_reply(outcome, reason="example_reason")
        assert f"Outcome: {outcome} (example_reason)" in reply, reply
        if outcome != "admitted":
            _no_false_nothing_queued([reply])
    admitted = steering_module.render_kickoff_reply("admitted", issue_url=_issue_url(7))
    assert _issue_url(7) in admitted and "posted in this thread" in admitted, admitted


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
