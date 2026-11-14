"""Slack issue-link kickoff at the worker's steering seam (#5380 AC2, ADR 0268).

``FactorySteeringClient.steer`` is the kernel's one question before any binding,
claim or personal installation guard. It now also starts a factory run: a
Slack mention at a channel root, or in a thread no factory run maps, whose
text links an issue and whose receiving agent holds a GitHub surface asks the
API's kickoff route, and that route's answer is the reply. A mapped root still
steers, except for the kickoff delivery's own redelivery, which replays. Every
other turn takes the ordinary path, and so does a candidate the API answers
``not_factory`` and a candidate on a worker without its internal token.

The boundary is ``test_factory_slack_runtime``'s steering stack: the real API
(``create_app()`` on uvicorn over a disposable migrated Postgres, real Valkey
and RustFS), the dispatcher's Bolt app driven through ``SocketModeHandler``,
and the worker as ``run.build`` composes it, consumed one stream entry at a
time through its real ``Consumer``. The kernel holds the production
``FactorySteeringClient``. Only external services are stand-ins: the Slack Web
API (``FakeSlack``), GitHub REST (``GitHubTruth``) and the Kubernetes API
(``KubernetesApi``). An ordinary turn is observed reaching the sandbox
substrate; with the stand-in accepting claims it hands the message to the
claimed sandbox's runner address, the runner stand-in, which answers 503.

Event shapes follow https://docs.slack.dev/reference/events/app_mention. A
bot-authored mention follows the shape observed over Socket Mode on a real
test workspace, 2026-11-12: ``user`` is the sending bot's own bot user,
``bot_id`` and ``bot_profile`` are set, ``team`` is the workspace, and there is
no ``user_team``; the envelope carries ``authorizations`` and
``is_ext_shared_channel: false``. A typed issue URL arrives wrapped as
``<URL>``. Enrollment headers are seeded with raw SQL, standing in for #5377's
enrollment writes. Placeholder identifiers only.

Behavior tests fail with the kickoff source reverted. Regression guards, which
pass before and after: the mapped-root steering test and the ordinary-path
test (each fails if the kickoff branch is widened). The direct message test's
direct message half is a guard of the same kind; its liveness half is not.
"""

from __future__ import annotations

import html
import itertools
import json
import uuid
from collections.abc import Callable
from typing import Any, get_args

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.models import DEFAULT_TENANT_ID
from curie_channel_gateway.slack.identities import delivery_key, minted_adapter
from curie_channel_gateway.slack.marked_actions import MARK
from curie_internal.driver_declaration import DeclaredDriver
from curie_internal.issue_links import find_issue_links
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
    CHART_POOL,
    CHART_TEMPLATE,
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
    Person,
    SteeringStack,
    SteeringWorker,
    _assert_no_sandbox,
    _carry_http_identity,
    _no_false_nothing_queued,
    _reached_sandbox,
    _receipt_from_another_app,
    _receipt_key,
    _rows,
    _seed_principal,
    _set_mapping_readable,
    _spend_budget,
    installation_mode,
    owner_tuples,
    runnable_requests,
)
from apps.api.tests.test_factory_slack_runtime import steering as steering

KICKOFF_PATH = steering_module.KICKOFF_PATH
STEER_PATH = steering_module.STEER_PATH
KICKOFF_EVIDENCE_MISSING = steering_module.KICKOFF_EVIDENCE_MISSING
# The owner's enrollment generation. Not 1, so a tuple that names a default
# generation instead of the enrolled one is caught.
GENERATION = 2
OTHER_REPO = "acme-corp/acme-elsewhere"
DM_CHANNEL = "D0EXAMPLE1"
ALERT_BOT_ID = "B0EXAMPLE7"
ALERT_BOT_USER = "U0EXAMPLE7"
DRIVER_BOT_ID = "B0EXAMPLE5"
DRIVER_BOT_USER = "U0EXAMPLE5"
DRIVER_APP_ID = "A0EXAMPLE5"
OTHER_USER = "U0EXAMPLE6"

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


def _thread_ts() -> str:
    """A thread no factory run maps."""

    return f"1800000700.{next(_TS):06d}"


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


def _sender(stack: SteeringStack) -> Person:
    """A Slack-linked, enrolled person with no code host link (kickoff needs none)."""

    person = stack.person(github=False)
    _enroll(person.principal_id)
    return person


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


def _deliver_event(stack: SteeringStack, event: dict[str, Any]) -> Delivery:
    """One native delivery of ``event`` through the dispatcher's Bolt app."""

    event_id = _event_id(stack)
    delivery_id = delivery_key(event_id, stack.identity)
    stack.base.doomed_keys.append(_receipt_key(delivery_id))
    since = len(stack.slack.calls)
    stack._deliver(event, event_id)
    thread = str(event.get("thread_ts") or event["ts"])
    return Delivery(
        event_id, delivery_id, event["ts"], _placeholder(stack, since, thread), event["text"]
    )


def _root_mention(
    stack: SteeringStack, user: str, body: str, *, channel: str = CHANNEL
) -> Delivery:
    """A person's top-level ``app_mention``; the root is the message's own ts."""

    ts = _root_ts()
    return _deliver_event(
        stack,
        {
            "type": "app_mention",
            "channel": channel,
            "text": f"<@{FACTORY_BOT_USER}> {body}",
            "ts": ts,
            "event_ts": ts,
            "team": TEAM,
            "user": user,
            "user_team": TEAM,
        },
    )


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


def _kick_root(stack: SteeringStack, user: str, body: str, **consume: Any) -> Attempt:
    return _kick(stack, lambda: _root_mention(stack, user, body), **consume)


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


def _refused_locally(attempt: Attempt, code: str = KICKOFF_EVIDENCE_MISSING) -> None:
    assert attempt.replies == [], "the turn reached the kickoff route"
    assert attempt.new_requests == [], attempt.new_requests
    assert attempt.edits, "the placeholder was not edited with the refusal"
    assert f"Outcome: {code}" in attempt.edits[-1].text, attempt.edits[-1].text
    _no_false_nothing_queued([call.text for call in attempt.edits])


def _went_ordinary(stack: SteeringStack, attempt: Attempt, since: int) -> None:
    """Steering returned None: no kickoff decision, and the turn met the substrate."""

    assert attempt.replies == [], "an ordinary turn reached the kickoff route"
    assert attempt.new_requests == [], attempt.new_requests
    assert _reached_sandbox(stack, since), stack.k8s.requests
    assert _answers(stack, STEER_PATH) == []
    texts = [call.text for call in attempt.edits]
    assert not any("Outcome:" in text_ for text_ in texts), texts


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


# --- AC2: kickoff ---------------------------------------------------------------------


def test_root_and_unmapped_thread_issue_links_kick_off_before_any_sandbox(
    steering: SteeringStack,
) -> None:
    """A root mention and an unmapped thread reply each admit one personal run.

    The sender's owner tuple is on the WorkItem and the request, the thread is
    recorded with origin ``kickoff`` at the root the message started, the
    reply names the issue, and nothing reaches the sandbox substrate. The
    first run's progress then posts into that root, with no root post owed.
    """

    stack = steering
    sender = _sender(stack)

    root_number = next(_ISSUES)
    root = _kick_root(stack, sender.slack_user, _linking(root_number))
    request = _kicked_off(
        stack,
        root,
        owner=sender.principal_id,
        number=root_number,
        root_ts=root.delivery.message_ts,
    )
    assert request["requester"] == f"slack:{sender.slack_user}", request
    assert stack.pending() == 0

    thread_number = next(_ISSUES)
    thread_ts = _thread_ts()
    reply = _kick(
        stack,
        lambda: stack.mention(sender.slack_user, _linking(thread_number), thread_ts=thread_ts),
    )
    _kicked_off(stack, reply, owner=sender.principal_id, number=thread_number, root_ts=thread_ts)
    assert stack.pending() == 0
    assert _answers(stack, STEER_PATH) == []
    _assert_no_sandbox(stack)
    assert stack.valkey.exists(stack.steering_sent_key(root.delivery.delivery_id)) == 1

    # The run's progress posts in the thread the message started.
    stack.start(request["id"])
    stack.report(request["id"], "plan", "Planned from the Slack kickoff")
    stack.drain()
    texts = [
        call.haystack
        for call in stack.slack.thread_posts(stack.factory_token, root.delivery.message_ts)
    ]
    assert len(texts) == 1 and "Planned from the Slack kickoff" in texts[0], texts
    assert stack.slack.roots(stack.factory_token) == []
    assert _rows("SELECT id FROM curie.factory_slack_posts WHERE kind = 'root'") == []


def test_a_mapped_root_with_an_issue_link_still_steers_through_its_authorization(
    steering: SteeringStack,
) -> None:
    """Regression guard: a link in a factory run's root is steering, judged as steering.

    A writer's mention admits a revision of the same run and an unauthorized
    reader's is refused ``not_authorized``; the kickoff route is never asked
    and no second run or thread appears.
    """

    stack = steering
    writer = stack.person()
    reader = stack.person(permission="read")
    run = stack.open_steered_run()
    threads = len(stack.threads())

    admitted = stack.steer(
        writer.slack_user, _linking(next(_ISSUES), lead="Please also handle"), thread_ts=run.root_ts
    )
    runtime._admitted(admitted)
    (revision,) = admitted.new_requests
    assert revision["work_item_id"] == run.work_item_id, revision
    runtime._api_refused(
        stack.steer(reader.slack_user, _linking(next(_ISSUES), lead="Also"), thread_ts=run.root_ts),
        "not_authorized",
    )
    assert _answers(stack, KICKOFF_PATH) == []
    assert len(stack.threads()) == threads
    _assert_no_sandbox(stack)


def test_the_kickoff_delivery_redelivered_after_its_root_is_mapped_replays_and_never_steers(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost kickoff answer: the redelivery finds its own request on the mapped root.

    Attempt 1's admission commits and the answer is lost after the API
    answered, so the delivery stays pending with the unconfirmed reply and no
    resend invitation. Attempt 2 sees a mapped root holding this delivery's
    kickoff request, replays through kickoff (never the steer route, which
    would refuse it ``lineage_closed``: the new run has no pull request), and
    settles once with one request.
    Liveness: a new mention in that root steers.
    """

    stack = steering
    sender = _sender(stack)
    writer = stack.person()
    number = next(_ISSUES)

    # The hop drops answers of the path its module names; it names kickoff here.
    with monkeypatch.context() as patch:
        patch.setattr(runtime, "STEER_PATH", KICKOFF_PATH)
        stack.api_hop.drop_next_answer()
        lost = _kick_root(stack, sender.slack_user, _linking(number))
    delivery = lost.delivery
    expected = _expected_request(stack, delivery)
    ((status, answer),) = stack.api_hop.dropped
    assert status == 200 and answer["outcome"] == "admitted", answer
    assert answer["request_id"] == str(expected), answer
    assert lost.replies == [], "the worker read the answer the hop dropped"
    assert [row["id"] for row in lost.new_requests] == [expected]
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == steering_module.KICKOFF_UNCONFIRMED_TEXT, texts
    assert stack.resend_invitations() == []
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    (thread,) = _threads_at(delivery.message_ts)
    assert thread["origin"] == "kickoff", thread

    answers = len(_answers(stack, KICKOFF_PATH))
    assert stack.reclaim() == 1
    ((status, replayed),) = _answers(stack, KICKOFF_PATH)[answers:]
    assert status == 200, replayed
    assert replayed["outcome"] == "admitted" and replayed.get("replayed") is True, replayed
    assert replayed["request_id"] == str(expected), replayed
    assert _answers(stack, STEER_PATH) == [], "the kickoff redelivery was steered"
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == steering_module.render_kickoff_reply(
        "admitted", issue_url=_issue_url(number), replayed=True
    ), texts
    _no_false_nothing_queued(texts)
    assert stack.is_terminal(delivery.delivery_id)
    assert stack.pending() == 0
    assert [row["id"] for row in stack.requests()] == [expected]
    assert len(_threads_at(delivery.message_ts)) == 1
    assert stack.resend_invitations() == []

    # Liveness: another delivery in the now mapped root is steering.
    runtime._api_refused(
        stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=delivery.message_ts),
        "lineage_closed",
    )
    _assert_no_sandbox(stack)


def test_unrelated_chat_and_a_non_factory_agent_sent_a_github_url_take_the_ordinary_path(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard: what is not a kickoff candidate is ordinary chat, as before.

    None of these asks the kickoff route. Unrelated chat, a link quoted inert
    from a thread's root context, and an issue link sent to an agent without a
    GitHub surface reach the sandbox substrate (the last one's receipt is never
    read); a repository link that is not issue-shaped gets the workspace
    selection's own answer, as before. On a personal installation the ordinary
    guard still refuses ordinary chat ``personal_owner_required``.
    """

    stack = steering
    sender = _sender(stack)

    since = len(stack.k8s.requests)
    chat = _kick_root(stack, sender.slack_user, "What does this service do?")
    _went_ordinary(stack, chat, since)
    assert stack.valkey.exists(_receipt_key(chat.delivery.delivery_id)) == 0
    assert stack.valkey.exists(stack.steering_sent_key(chat.delivery.delivery_id)) == 0

    # A repository URL meets the ordinary path's workspace selection, which on
    # this worker (workspaces off) answers before any sandbox claim, as before.
    from curie_worker.workspace import WORKSPACES_DISABLED_REFUSAL

    since = len(stack.k8s.requests)
    repository = _kick_root(stack, sender.slack_user, f"See <https://github.com/{REPO}>")
    assert repository.replies == [], "a repository link reached the kickoff route"
    assert repository.new_requests == [], repository.new_requests
    assert stack.valkey.exists(stack.steering_sent_key(repository.delivery.delivery_id)) == 0
    texts = [call.text for call in repository.edits]
    assert texts and WORKSPACES_DISABLED_REFUSAL in texts[-1], texts
    assert not any("Outcome:" in text_ for text_ in texts), texts
    assert stack.k8s.requests[since:] == []
    assert _threads_at(repository.delivery.message_ts) == []

    # The bot-rooted thread's root names an issue; the reply does not. The
    # dispatcher quotes the root (spec slack-alert-followup-context), escaped
    # with every slash as ``&#x2F;`` (``thread_context.render_prior_reply``), so
    # the quoted link is inert and the turn is not a kickoff candidate.
    quoted_thread = _thread_ts()
    quoted_url = _issue_url(next(_ISSUES))
    answer = stack.slack._answer

    def quoting(method: str, params: dict[str, Any]) -> dict[str, Any]:
        # https://docs.slack.dev/reference/methods/conversations.replies
        if method == "conversations.replies" and params.get("ts") == quoted_thread:
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": quoted_thread,
                        "bot_id": FACTORY_BOT_ID,
                        "text": f"Alert for <{quoted_url}>",
                    }
                ],
                "has_more": False,
            }
        return answer(method, params)

    def queued_with_the_quoted_root(_delivery: Delivery) -> None:
        _entry, fields = stack.valkey.xrange(stack.names["stream"])[-1]
        queued = json.loads(fields["payload"])["text"]
        assert html.escape(quoted_url, quote=False).replace("/", "&#x2F;") in queued, (
            "the queued turn does not quote the root"
        )
        assert find_issue_links(queued) == ()

    monkeypatch.setattr(stack.slack, "_answer", quoting)
    since = len(stack.k8s.requests)
    quoted = _kick(
        stack,
        lambda: stack.mention(
            sender.slack_user, "What does this alert mean?", thread_ts=quoted_thread
        ),
        before_consume=queued_with_the_quoted_root,
    )
    _went_ordinary(stack, quoted, since)
    assert stack.valkey.exists(_receipt_key(quoted.delivery.delivery_id)) == 1

    # Without a GitHub surface the agent serves no repository for the factory.
    removed = stack.client.delete(
        f"/agents/{stack.agent_id}/channels",
        params={"kind": "github", "address": REPO},
        headers=stack.platform,
    )
    assert removed.status_code == 204, removed.text
    since = len(stack.k8s.requests)
    unserved = _kick_root(stack, sender.slack_user, _linking(next(_ISSUES)))
    _went_ordinary(stack, unserved, since)
    key = _receipt_key(unserved.delivery.delivery_id)
    assert stack.valkey.exists(key) == 1
    assert stack.valkey_hop.touched(key) == []

    # A personal installation: the kickoff branch returns None, then the guard refuses.
    installation_mode(monkeypatch, "personal")
    personal = stack.worker_process()
    since = len(stack.k8s.requests)
    guarded = _kick_root(stack, sender.slack_user, "What does this service do?", worker=personal)
    assert guarded.replies == [], guarded.replies
    assert guarded.edits, "the ordinary guard did not answer"
    assert "personal_owner_required" in guarded.edits[-1].text, guarded.edits[-1].text
    assert stack.k8s.requests[since:] == []
    assert runnable_requests() == set()
    assert stack.requests() == []


def test_not_factory_and_a_tokenless_worker_take_the_ordinary_path(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A candidate the API answers ``not_factory``, or cannot be asked about, is ordinary chat.

    Accepted change 4: the worker cannot see the installation's factory
    switch or tracker configuration, so it asks and takes the ordinary path on
    ``not_factory``. On a factory-disabled installation the issue-link mention
    reaches the substrate and, the cluster refusing its sandbox, gets the
    generic not-started notice: the intent marker recorded before the request
    was cleared, so nothing reads the turn as a possibly admitted one. A link
    to a repository the agent does not serve, on a factory installation, is
    handed to the claimed sandbox's runner. A worker without its internal
    token asks nothing and never reads the receipt.
    """

    stack = steering
    sender = _sender(stack)

    monkeypatch.setenv("FACTORY_ENABLED", "false")
    get_settings.cache_clear()
    since = len(stack.k8s.requests)
    disabled = _kick_root(stack, sender.slack_user, _linking(next(_ISSUES)))
    ((status, answer),) = disabled.replies
    assert status == 200 and answer["outcome"] == "not_factory", answer
    assert disabled.new_requests == []
    assert _reached_sandbox(stack, since), stack.k8s.requests
    texts = stack.placeholder_texts(disabled.delivery.placeholder)
    assert texts[-1] == stack.not_started(), texts
    assert stack.valkey.exists(stack.steering_sent_key(disabled.delivery.delivery_id)) == 0
    assert _threads_at(disabled.delivery.message_ts) == []

    tokenless = stack.worker_process(token=False)
    since = len(stack.k8s.requests)
    quiet = _kick_root(stack, sender.slack_user, _linking(next(_ISSUES)), worker=tokenless)
    assert quiet.replies == [] and quiet.new_requests == []
    assert _reached_sandbox(stack, since), stack.k8s.requests
    key = _receipt_key(quiet.delivery.delivery_id)
    assert stack.valkey.exists(key) == 1
    assert stack.valkey_hop.touched(key) == []

    monkeypatch.setenv("FACTORY_ENABLED", "true")
    get_settings.cache_clear()
    stack.k8s.accept(pool=CHART_POOL, template=CHART_TEMPLATE, service_fqdn="127.0.0.1")
    since = len(stack.k8s.requests)
    body = _linking(next(_ISSUES), OTHER_REPO)
    unserved = _kick_root(stack, sender.slack_user, body)
    ((status, answer),) = unserved.replies
    assert status == 200 and answer["outcome"] == "not_factory", answer
    assert unserved.new_requests == []
    assert _reached_sandbox(stack, since), stack.k8s.requests
    delivered = [
        payload
        for method, path, payload in stack.k8s.sandbox_address.requests
        if method == "POST" and path in ("/v1/steer", "/v1/event")
    ]
    assert delivered, stack.k8s.sandbox_address.requests
    assert all(OTHER_REPO in str(payload.get("text")) for payload in delivered), delivered
    assert stack.valkey.exists(stack.steering_sent_key(unserved.delivery.delivery_id)) == 0
    assert stack.requests() == []
    assert stack.threads() == []


def test_missing_tampered_and_cross_identity_receipts_refuse_before_model_work(
    steering: SteeringStack,
) -> None:
    """A candidate without its own receipt is refused locally, never ordinary chat.

    The receipt is deleted, unreadable, names another receiving app, another
    sender or another root: each is refused ``kickoff_evidence_missing`` with
    no kickoff request, no admission and no sandbox. Liveness: an untouched
    mention admits.
    """

    stack = steering
    sender = _sender(stack)
    number = next(_ISSUES)

    def rewrite(change: Callable[[dict[str, Any]], None]) -> Callable[[Delivery], object]:
        def apply(delivery: Delivery) -> None:
            key = _receipt_key(delivery.delivery_id)
            stored = stack.valkey.get(key)
            assert stored is not None, "the dispatcher wrote no root receipt"
            receipt = json.loads(stored)
            change(receipt)
            stack.valkey.set(key, json.dumps(receipt), keepttl=True)

        return apply

    def other_sender(receipt: dict[str, Any]) -> None:
        receipt["user_id"] = OTHER_USER
        receipt["evidence"]["user_id"] = OTHER_USER

    def other_root(receipt: dict[str, Any]) -> None:
        receipt["root_ts"] = _thread_ts()

    cases: list[Callable[[Delivery], object]] = [
        lambda d: stack.valkey.delete(_receipt_key(d.delivery_id)),
        lambda d: stack.valkey.set(_receipt_key(d.delivery_id), "not a receipt", keepttl=True),
        lambda d: _receipt_from_another_app(stack, d),
        rewrite(other_sender),
        rewrite(other_root),
    ]
    for case in cases:
        attempt = _kick_root(stack, sender.slack_user, _linking(number), before_consume=case)
        _refused_locally(attempt)
        assert stack.pending() == 0
        assert _threads_at(attempt.delivery.message_ts) == []
    _assert_no_sandbox(stack)
    assert stack.requests() == []

    live = _kick_root(stack, sender.slack_user, _linking(number))
    _kicked_off(
        stack, live, owner=sender.principal_id, number=number, root_ts=live.delivery.message_ts
    )
    _assert_no_sandbox(stack)


def test_bots_without_receipts_and_http_selected_turns_refuse_kickoff(
    steering: SteeringStack,
) -> None:
    """A bot's mention carries no receipt, and an HTTP selected turn no native evidence.

    An undeclared alert app's root mention (a turn since #2006) and a sibling
    identity's bot in an unmapped thread are refused ``kickoff_evidence_missing``
    with no kickoff request. An HTTP selected turn is held by the #5444
    identity layer before steering is consulted: no kickoff request, no
    admission, still pending. Liveness: a person's mention admits.
    """

    stack = steering
    sender = _sender(stack)
    number = next(_ISSUES)

    alert = _kick(
        stack,
        lambda: _deliver_event(
            stack,
            _bot_event(
                bot_id=ALERT_BOT_ID,
                bot_user=ALERT_BOT_USER,
                app_id="A0EXAMPLE7",
                body=_linking(number, lead="Alert for"),
            ),
        ),
    )
    assert stack.valkey.exists(_receipt_key(alert.delivery.delivery_id)) == 0
    _refused_locally(alert)

    sibling = _kick(
        stack,
        lambda: stack.mention(
            None, _linking(number, lead="Look at"), thread_ts=_thread_ts(), bot_id=SIBLING_BOT_ID
        ),
    )
    _refused_locally(sibling)
    assert stack.pending() == 0

    carried = _kick_root(
        stack,
        sender.slack_user,
        _linking(number),
        before_consume=lambda _delivery: _carry_http_identity(stack),
    )
    assert carried.replies == [] and carried.new_requests == []
    assert not stack.is_terminal(carried.delivery.delivery_id)
    assert stack.pending() == 1
    _assert_no_sandbox(stack)
    assert stack.requests() == []

    live = _kick_root(stack, sender.slack_user, _linking(number))
    _kicked_off(
        stack, live, owner=sender.principal_id, number=number, root_ts=live.delivery.message_ts
    )
    _assert_no_sandbox(stack)


def test_a_direct_message_with_an_issue_link_stays_ordinary(steering: SteeringStack) -> None:
    """A direct message never kicks off, even to a factory agent bound there.

    The agent is bound in a direct message conversation and holds its GitHub
    surface, so only the channel shape keeps the turn ordinary: no kickoff
    request, the substrate reached. Liveness: the same link at a channel root
    admits.
    """

    stack = steering
    sender = _sender(stack)
    number = next(_ISSUES)
    bound = stack.client.post(
        f"/agents/{stack.agent_id}/channels",
        headers=stack.platform,
        json={"kind": "slack", "address": DM_CHANNEL, "adapter": stack.identity},
    )
    assert bound.status_code == 201, bound.text

    since = len(stack.k8s.requests)
    ts = _root_ts()
    direct = _kick(
        stack,
        lambda: _deliver_event(
            stack,
            {
                "type": "message",
                "channel": DM_CHANNEL,
                "channel_type": "im",
                "user": sender.slack_user,
                "user_team": TEAM,
                "team": TEAM,
                "text": _linking(number),
                "ts": ts,
                "event_ts": ts,
            },
        ),
    )
    _went_ordinary(stack, direct, since)
    assert stack.valkey.exists(_receipt_key(direct.delivery.delivery_id)) == 0

    live = _kick_root(stack, sender.slack_user, _linking(number))
    _kicked_off(
        stack, live, owner=sender.principal_id, number=number, root_ts=live.delivery.message_ts
    )


def test_a_worker_without_its_token_or_budget_decides_kickoff_locally(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Neither a tokenless worker nor a spent delivery budget asks the kickoff route.

    A tokenless worker leaves an unmapped candidate to ordinary chat (accepted
    change 4), but refuses the replay of a kickoff whose admission committed
    ``kickoff_unconfigured``, never ordinary chat and never claiming nothing
    started. A candidate whose delivery budget is spent is refused
    ``kickoff_budget_expired`` with no GitHub read. Liveness: the same mention
    with its full budget admits.
    """

    from curie_worker.kernel import constants as kernel_constants

    stack = steering
    sender = _sender(stack)
    tokenless = stack.worker_process(token=False)

    # A kickoff whose answer was lost, redelivered to the tokenless worker.
    with monkeypatch.context() as patch:
        patch.setattr(runtime, "STEER_PATH", KICKOFF_PATH)
        stack.api_hop.drop_next_answer()
        lost = _kick_root(stack, sender.slack_user, _linking(next(_ISSUES)))
    expected = _expected_request(stack, lost.delivery)
    assert [row["id"] for row in lost.new_requests] == [expected]
    answers = len(_answers(stack, KICKOFF_PATH))
    calls = len(stack.slack.calls)
    assert stack.reclaim(tokenless) == 1
    assert _answers(stack, KICKOFF_PATH)[answers:] == [], "the tokenless worker asked the API"
    edits = [
        call.text
        for call in stack.slack.calls[calls:]
        if call.method == "chat.update" and call.params.get("ts") == lost.delivery.placeholder
    ]
    assert edits and "Outcome: kickoff_unconfigured" in edits[-1], edits
    _no_false_nothing_queued(edits)
    assert stack.is_terminal(lost.delivery.delivery_id)
    assert [row["id"] for row in stack.requests()] == [expected]

    remaining_ms = int(kernel_constants._MIN_ATTEMPT_BUDGET_S * 1000) - 2000
    assert remaining_ms > 0
    github_reads = len(stack.github.paths)
    number = next(_ISSUES)
    spent = _kick_root(
        stack,
        sender.slack_user,
        _linking(number),
        before_consume=lambda _delivery: _spend_budget(stack, remaining_ms),
    )
    _refused_locally(spent, steering_module.KICKOFF_BUDGET_EXPIRED)
    assert stack.github.paths[github_reads:] == []
    assert stack.pending() == 0

    live = _kick_root(stack, sender.slack_user, _linking(number))
    _kicked_off(
        stack, live, owner=sender.principal_id, number=number, root_ts=live.delivery.message_ts
    )
    _assert_no_sandbox(stack)

    # Last, since its failed ordinary turn stays pending: the tokenless worker
    # leaves an unmapped candidate to ordinary chat and never reads its receipt.
    ordinary = _kick_root(stack, sender.slack_user, _linking(next(_ISSUES)), worker=tokenless)
    assert ordinary.replies == [] and ordinary.new_requests == []
    assert _reached_sandbox(stack, 0), stack.k8s.requests
    key = _receipt_key(ordinary.delivery.delivery_id)
    assert stack.valkey.exists(key) == 1
    assert stack.valkey_hop.touched(key) == []


def test_an_unreadable_decision_stays_pending_and_admits_once_on_redelivery(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mapping that cannot be read, or a code host outage, fails closed and is bounded.

    The worker's mapping read refused: no kickoff request, no intent marker, no
    notice, still pending. The API's GitHub reads failing: the route answers
    503, the reply says the request is unconfirmed without inviting a resend,
    still pending. Each admits exactly once when the consumer's reclaim
    redelivers it after the fault clears.
    """

    stack = steering
    sender = _sender(stack)

    number = next(_ISSUES)
    _set_mapping_readable(stack, False)
    try:
        unread = _kick_root(stack, sender.slack_user, _linking(number))
    finally:
        _set_mapping_readable(stack, True)
    assert unread.replies == [] and unread.new_requests == []
    assert unread.edits == [], [call.text for call in unread.edits]
    assert stack.valkey.exists(stack.steering_sent_key(unread.delivery.delivery_id)) == 0
    assert stack.resend_invitations() == []
    assert stack.pending() == 1
    _redelivered_once(stack, unread, number)

    number = next(_ISSUES)
    healthy = stack.github.handle

    def outage(request: httpx.Request) -> httpx.Response:
        # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api
        if request.url.path.startswith(f"/repos/{REPO}"):
            return httpx.Response(503, json={"message": "Service Unavailable"})
        return healthy(request)

    with monkeypatch.context() as patch:
        patch.setattr(stack.github, "handle", outage)
        down = _kick_root(stack, sender.slack_user, _linking(number))
    ((status, answer),) = down.replies
    assert status == 503, (status, answer)
    assert down.new_requests == []
    texts = stack.placeholder_texts(down.delivery.placeholder)
    assert texts[-1] == steering_module.KICKOFF_UNCONFIRMED_TEXT, texts
    assert stack.valkey.exists(stack.steering_sent_key(down.delivery.delivery_id)) == 1
    assert stack.resend_invitations() == []
    assert stack.pending() == 1
    assert not stack.is_terminal(down.delivery.delivery_id)
    _redelivered_once(stack, down, number)
    _assert_no_sandbox(stack)


def _redelivered_once(stack: SteeringStack, first: Attempt, number: int) -> None:
    """The consumer's reclaim admits the pending delivery, once, not as a replay."""

    known = {row["id"] for row in stack.requests()}
    answers = len(_answers(stack, KICKOFF_PATH))
    assert stack.reclaim() == 1
    ((status, admitted),) = _answers(stack, KICKOFF_PATH)[answers:]
    assert status == 200 and admitted["outcome"] == "admitted", admitted
    assert admitted.get("replayed") is not True, admitted
    expected = _expected_request(stack, first.delivery)
    assert [row["id"] for row in stack.requests() if row["id"] not in known] == [expected]
    texts = stack.placeholder_texts(first.delivery.placeholder)
    assert texts[-1] == steering_module.render_kickoff_reply(
        "admitted", issue_url=_issue_url(number)
    ), texts
    assert stack.is_terminal(first.delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.resend_invitations() == []


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
