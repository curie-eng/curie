"""The approval journey, composed across three apps and driven end to end.

One real path from the card to the kernel, with every seam BETWEEN the three
apps -- Slack interaction handling, dispatcher, HTTP, API, Postgres, Valkey,
worker -- run for real rather than scripted. The stand-ins that remain are all
at the edges (the runner and Slack itself) and are named in the block below.
The chain: the real
``approval_card(allow_free_text=True)`` render -> the real Bolt
``SocketModeHandler`` -> the real ``this_release_owns_action`` ownership probe ->
the real ``views_open`` note modal -> the real ``view_submission`` handler -> the
real ``ApprovalResolveClient`` over real loopback HTTP -> the real ``curie_api``
app with its real lifespan entered -> the real principal verifier and the real
authorizer -> real Postgres -> the real ``ResumeQueue.enqueue`` -> a real Valkey
stream -> the real worker ``Consumer`` and ``Kernel``.

HONEST LABELLING -- what this module does NOT prove:

    This module proves API/worker/dispatcher behavior only. The runner is the
    existing in-process ``FakeRunner`` fake (apps/worker/tests/kernel/conftest.py)
    and Slack is the existing ``FakeSocketClient`` + mocked ``WebClient``. It does
    not prove real-runner or real-Slack behavior. Stage 3 connects the real
    runner. Three smaller stand-ins sit beside them, all on the Slack/kernel
    edges rather than in the journey itself: ``_authorize`` (Bolt's workspace
    authorization lookup), ``JourneyRecorder`` (the action-ledger recorder) and
    ``_RecordingApprovals`` (the kernel's ``ApprovalCreator``, used only where a
    test needs the render the kernel asks for).

Two consequences of that worth stating where the assertions are, not only in a
plan: "the card settled" here is a ``chat_update`` call recorded on a
``MagicMock``, which proves the dispatcher ASKED Slack to stamp the card, not
that Slack rendered anything; and every tool call the ledger records is a frame
the fake runner emitted, not a tool that ran.

Placement: this lives under ``apps/worker/tests/kernel/`` because the
``make_harness`` fixture (real ``Kernel``, real ``SandboxSubstrate`` over
``FakeK8s``, real ``RunnerClient``, the fake runner server) is ~150 lines of
assembly reachable only from a test under this directory -- pytest resolves
conftests by directory, and ``--import-mode=importlib`` with no ``__init__.py``
means one test module cannot import another's helpers. The API half (a
disposable migrated database plus a uvicorn server) is the cheap half, so that
is the half that moved. ``apps/worker/tests`` is in ``testpaths``, so CI
collects this with no ``pyproject.toml`` edit.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import os
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_api.resumequeue import resume_event_id
from curie_dispatcher.app import build_app
from curie_dispatcher.approval_actions import (
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    NOTE_MODAL_CALLBACK_ID,
    REJECT_ACTION_ID,
    REJECT_NOTE_ACTION_ID,
)
from curie_dispatcher.approval_principal import mint_chat_principal
from curie_dispatcher.config import DispatcherConfig
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.approvals import CreatedApproval
from curie_worker.blocks import approval_card
from curie_worker.consumer import Consumer
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DONE = SessionStatus.DONE


def _module_from_path(path: Path, name: str) -> Any:
    """Import one file as a module, by path.

    ``--import-mode=importlib`` with no ``__init__.py`` means a test module
    cannot import a sibling (or a conftest) by name: pytest registers each
    conftest in ``sys.modules`` under a mangled, unstable key. Fixtures are found
    by pytest regardless, but plain module-level HELPERS are not, so the two
    helper sources this module needs are loaded explicitly. Already-imported
    modules are reused so the conftest is not executed a second time.
    """

    for module in list(sys.modules.values()):
        if getattr(module, "__file__", None) and Path(module.__file__).resolve() == path:
            return module
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_REPO_ROOT = Path(__file__).resolve().parents[4]
_KERNEL_CONFTEST = _module_from_path(
    Path(__file__).resolve().parent / "conftest.py", "_journey_kernel_conftest"
)
composed_api_server = _KERNEL_CONFTEST.composed_api_server
gated_call = _KERNEL_CONFTEST.gated_call
_JOURNEY_ATTESTER_VALUE = _KERNEL_CONFTEST._JOURNEY_ATTESTER_VALUE


# --- Cross-root import of the dispatcher's Socket Mode fakes -----------------
#
# ``apps/dispatcher/tests/conftest.py`` holds ``_authorize``, ``FakeSocketClient``
# and ``deliver_until_acked``. Under ``--import-mode=importlib`` with no
# ``__init__.py``, a module under ``apps/worker/tests/kernel/`` cannot reach it
# by name. It is loaded by file path instead of being redeclared, which is the
# plan's first-choice option: a local copy of ``FakeSocketClient`` would fork the
# ``ack_payloads`` capture (dispatcher conftest:43-53), and this module's AC3
# assertions are ENTIRELY carried by that ack body -- the note path renders a
# refusal into the view ack, not into an ephemeral.


_DISPATCHER_CONFTEST = _REPO_ROOT / "apps" / "dispatcher" / "tests" / "conftest.py"
assert _DISPATCHER_CONFTEST.is_file(), f"dispatcher conftest not found at {_DISPATCHER_CONFTEST}"
_DISPATCHER = _module_from_path(_DISPATCHER_CONFTEST, "_journey_dispatcher_conftest")
FakeSocketClient = _DISPATCHER.FakeSocketClient
deliver_until_acked = _DISPATCHER.deliver_until_acked
_authorize = _DISPATCHER._authorize


def test_the_imported_dispatcher_fakes_are_the_real_ones() -> None:
    """Parity guard on the cross-root import (R9).

    The point of loading the dispatcher's conftest by path rather than copying
    it is that a change to the ack capture cannot silently fork. If Bolt's ack
    body stops being recorded, every AC3 assertion in this module would go
    vacuous, so the capture is pinned here explicitly.
    """

    sock = FakeSocketClient()

    class _Response:
        envelope_id = "env-parity"
        payload = {"response_action": "errors", "errors": {"note": "nope"}}

    sock.send_socket_mode_response(_Response())

    assert sock.acked_envelope_ids == ["env-parity"]
    assert sock.ack_payload_for("env-parity") == {
        "response_action": "errors",
        "errors": {"note": "nope"},
    }


# --- Module preconditions ----------------------------------------------------
#
# A skip is a failure for this module: it is the whole point of the PR. The
# guard is deliberately SPECIFIC. ``CI_REQUIRE_VALKEY_TESTS`` only makes an
# UNREACHABLE store fail instead of skip; it says nothing about the WRONG store,
# and a reachable default ``localhost:26379`` would run happily against a shared
# stack where a pre-existing stream entry can make an assertion pass for the
# wrong reason.


def test_the_isolated_pilot_stack_is_what_we_are_running_against() -> None:
    assert os.environ.get("CI_REQUIRE_VALKEY_TESTS"), (
        "CI_REQUIRE_VALKEY_TESTS must be set: without it an unreachable Valkey "
        "SKIPS instead of failing, and this module's whole claim is that the "
        "journey actually ran."
    )
    assert os.environ.get("TEST_VALKEY_PORT") == "36379", (
        "this module must run against the isolated pilot stack's Valkey on "
        f"36379, not {os.environ.get('TEST_VALKEY_PORT')!r}; the constants are "
        "frozen at import (curie_test_support/valkey.py:19-21), so no fixture "
        "can retarget the worker afterwards."
    )
    assert "VALKEY_URL" not in os.environ, (
        "VALKEY_URL wins outright over the host/port parts (config.py:404-406), "
        "so with it set the API would write the resume to a different store "
        "than the worker reads."
    )
    assert VALKEY_PORT == 36379


# --- Shared journey scaffolding ----------------------------------------------

_APPROVERS_CHANNEL = "C0EXAMPLE1"
_OTHER_CHANNEL = "C0EXAMPLE2"
_APPROVER = "U0EXAMPLE1"
_SECOND_APPROVER = "U0EXAMPLE2"
_OUTSIDER = "U0EXAMPLE3"
_NOTE_TEXT = "checked the invoice, this is fine"

_DB_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")


def _dispatcher_config(stream: str) -> DispatcherConfig:
    """A dispatcher config whose attester secret matches the API's.

    Both halves must sign and verify with the SAME dedicated credential, and it
    must differ from the platform key -- ``approval_auth.py:92-97`` refuses a
    chat token outright when the two are equal, and ``config.py:430-431`` refuses
    the configuration at boot.
    """

    return DispatcherConfig(
        slack_app_token="xapp-test",
        slack_bot_token="xoxb-test",
        valkey_host=VALKEY_HOST,
        valkey_port=VALKEY_PORT,
        valkey_password=VALKEY_PW,
        stream=stream,
        dedupe_prefix=f"test:curie:dedupe:{uuid.uuid4().hex}:",
        dedupe_ttl_seconds=60,
        placeholder_text="Working on it.",
        approval_chat_attester_secret=_JOURNEY_ATTESTER_VALUE,
    )


def _build_dispatcher(
    config: DispatcherConfig, redis_client: redis.Redis, resolver: Any
) -> tuple[Any, WebClient]:
    """The dispatcher app with the REAL resolver wired in.

    The only fakes are Slack's: ``web_client`` is a mock and the socket is
    ``FakeSocketClient``. ``resolver=`` is the real ``ApprovalResolveClient``
    talking real HTTP to the composed API, where every other test in the repo
    passes a ``ScriptedResolver``.
    """

    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": "555.000"})  # type: ignore[method-assign]
    web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.views_open = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        resolver=resolver,
    )
    return app, web_client


def _drain(app: Any, *, timeout: float = 30.0) -> None:
    """Wait for every post-ack listener body to finish, ONCE, at the end, BOUNDED.

    ``shutdown(wait=True)`` is terminal: Bolt's listener executor cannot accept
    another envelope afterwards ("cannot schedule new futures after shutdown").
    The dispatcher's own tests drive one envelope per app so they never notice;
    this module drives a click AND a submission through the same app, so the
    drain has to be the last thing each test does. Mid-test, wait on the
    observable the next step needs instead -- see ``_wait_for``.

    ``shutdown(wait=True)`` takes no deadline, so it is run on a helper thread
    that IS joined with one: a listener body wedged on the loopback API would
    otherwise hang the whole suite with nothing in the report to find it by.
    """

    executor = app.listener_runner.listener_executor
    closer = threading.Thread(target=lambda: executor.shutdown(wait=True), name="journey-drain")
    closer.start()
    closer.join(timeout=timeout)
    assert not closer.is_alive(), (
        f"the Bolt listener executor did not drain within {timeout}s; a post-ack "
        "listener body is still running (most likely wedged on the loopback API)"
    )


def _wait_for(pred: Callable[[], bool], *, what: str, timeout: float = 20.0) -> None:
    """Bounded sync wait for a post-ack listener side effect.

    Bolt returns from ``handle`` as soon as the listener acks and keeps running
    the rest of the body on its executor, so the ``views_open`` call and the card
    stamp land slightly after. Every wait in this module carries a deadline and a
    message: the Bolt executor, the threaded uvicorn server and the AC5 threads
    are all real hang surfaces, and a hang reports nothing.
    """

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


def _card(approval_id: str, *, allow_free_text: bool) -> dict[str, Any]:
    """The REAL rendered card, as a Slack message, exactly as the kernel renders it.

    ``allow_free_text=True`` is what ``kernel.py:3816`` passes unconditionally,
    so that render is the only one production posts.
    """

    fallback, blocks = approval_card(
        approval_id=approval_id,
        summary="Scale the payments deployment to 10 replicas",
        requested_by="U0EXAMPLE4",
        allow_free_text=allow_free_text,
    )
    return {"ts": "1700.0042", "thread_ts": "1700.0001", "text": fallback, "blocks": blocks}


def _buttons(card: dict[str, Any]) -> list[dict[str, Any]]:
    actions = [b for b in card["blocks"] if b["type"] == "actions"]
    assert len(actions) == 1, f"expected exactly one actions block, got {len(actions)}"
    return list(actions[0]["elements"])


def _click(
    envelope_id: str,
    *,
    card: dict[str, Any],
    button: dict[str, Any],
    user: str,
    channel: str,
) -> SocketModeRequest:
    """A block_actions envelope whose action id and value are DERIVED.

    ``button`` is an element read straight out of the rendered card above, so
    nothing here is a literal a renderer produced: a change to the action ids or
    to the ``value`` the buttons carry moves this click with it.
    """

    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": card["ts"]},
            "channel": {"id": channel},
            "message": card,
            "actions": [
                {
                    "type": "button",
                    "action_id": button["action_id"],
                    "action_ts": "2.0",
                    "value": button["value"],
                }
            ],
        },
    )


def _view_submission(
    envelope_id: str, *, private_metadata: str, user: str, note: str | None
) -> SocketModeRequest:
    """A view_submission envelope built from the CAPTURED modal metadata.

    ``private_metadata`` is read back off the real ``views_open`` call arguments,
    never hand-built: it is what carries the approval id, channel, card ts and
    decision across a submission that has no channel or message of its own
    (approval_actions.py:56-58).
    """

    state: dict[str, Any] = {"values": {}}
    if note is not None:
        state["values"] = {"note": {"note-input": {"type": "plain_text_input", "value": note}}}
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "view_submission",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "trigger_id": f"trig-{envelope_id}",
            "view": {
                "id": f"V-{envelope_id}",
                "type": "modal",
                "callback_id": NOTE_MODAL_CALLBACK_ID,
                "private_metadata": private_metadata,
                "state": state,
                "hash": "1",
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
            },
        },
    )


class _Api:
    """Thin read/write helper over the composed API, using the platform key.

    Deliberately NOT a stand-in for anything: every call here is a real HTTP
    request to the real routes (``POST /approvals``, ``GET /approvals/{id}``,
    ``GET /approvals/{id}/audit``), which is how the test observes the same
    surface an operator would.
    """

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base = base_url
        self._headers = {"X-API-Key": api_key}
        self._client = httpx.Client(timeout=10.0)

    def close(self) -> None:
        self._client.close()

    def create_approval(self, **overrides: Any) -> dict[str, Any]:
        body: dict[str, Any] = {
            "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
            "author": "U0EXAMPLE4",
            "summary": "Scale the payments deployment to 10 replicas",
            "reply_kind": "slack",
            "reply_channel": _APPROVERS_CHANNEL,
            "reply_placeholder": "p-1",
            "dedupe_key": f"ev-{uuid.uuid4().hex}",
            "card_channel": _APPROVERS_CHANNEL,
        }
        body.update(overrides)
        response = self._client.post(f"{self._base}/approvals", json=body, headers=self._headers)
        assert response.status_code == 201, response.text
        return dict(response.json())

    def approval(self, approval_id: str) -> dict[str, Any]:
        response = self._client.get(f"{self._base}/approvals/{approval_id}", headers=self._headers)
        assert response.status_code == 200, response.text
        return dict(response.json())

    def audit(self, approval_id: str) -> list[dict[str, Any]]:
        response = self._client.get(
            f"{self._base}/approvals/{approval_id}/audit", headers=self._headers
        )
        assert response.status_code == 200, response.text
        return list(response.json())

    def audit_status(self, approval_id: str) -> int:
        return self._client.get(
            f"{self._base}/approvals/{approval_id}/audit", headers=self._headers
        ).status_code


@pytest.fixture
def api(approval_api_server: Any, approval_api_env: Any) -> Any:
    helper = _Api(approval_api_server.base_url, approval_api_env.api_key)
    yield helper
    helper.close()


async def _seed_agent_with_route(
    *, agent_id: uuid.UUID, route: str, approvers: dict[str, Any]
) -> None:
    """Seed a real ``agents`` row carrying a real approval ROUTE BINDING.

    Without the binding, ``SlackApproverSetSelector`` returns ``UnboundRoute``
    (approvers.py:150), whose ``UnboundRouteBinding`` refuses EVERYONE -- so an
    AC3 refusal test would pass green without ``ExplicitUsers.contains`` ever
    running. Every refusal assertion below therefore names the authorizer it
    expects and asserts it is not that one.

    Shape follows apps/worker/tests/binding/test_approval_grant.py:49-72.
    """

    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    f"INSERT INTO {_DB_SCHEMA}.agents (id, name, approval_routes) "
                    "VALUES (:id, :name, CAST(:routes AS jsonb))"
                ),
                {
                    "id": agent_id,
                    "name": f"agent-{agent_id.hex[:8]}",
                    "routes": json.dumps({route: {"approvers": approvers}}),
                },
            )
            await conn.execute(
                text(
                    f"INSERT INTO {_DB_SCHEMA}.agent_channels (id, agent_id, kind, address) "
                    "VALUES (:id, :agent_id, 'slack', :address)"
                ),
                {
                    "id": uuid.uuid4(),
                    "agent_id": agent_id,
                    "address": f"C{agent_id.hex[:8].upper()}",
                },
            )
    finally:
        await engine.dispose()


async def _stream_entries(h: Any) -> list[QueuedTurn]:
    """Every entry on the REAL runs stream, decoded back into ``QueuedTurn``.

    Read from the stream the API's real ``ResumeQueue.enqueue`` xadded to
    (resumequeue.py:266). Hand-building a ``QueuedTurn`` and feeding it to
    ``kernel.process_event`` would satisfy the event-id assertion while the API
    had xadded nothing at all, which is why this module never does that.
    """

    rows = await h.async_redis.xrange(h.config.stream, "-", "+")
    return [QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD]) for _id, fields in rows]


async def _wait_until(pred: Callable[[], bool], *, what: str, timeout: float = 20.0) -> None:
    """Bounded wait. Every wait in this module has a deadline and a message:
    the threaded uvicorn server and the threaded AC5 clients make a hang a real
    failure mode, and a hang reports nothing."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _consume_the_resume(
    h: Any, *, consumer: Consumer, event_id: str, expect_text: str
) -> None:
    """Run the REAL consumer until it finishes the entry the API wrote.

    The wait is on the terminal ``turn.completed`` for THAT event id, not on the
    delivered text: the kernel appends the ledger's "What I changed" summary to a
    turn that touched the world, so the reply text is a superset of what the
    runner said. Waiting on the completion keeps the wait honest about which turn
    finished, and the text is asserted separately below.
    """

    task = asyncio.create_task(consumer.run())
    try:
        await _wait_until(
            lambda: any(c.event_id == event_id for c in h.sink.completions),
            what=f"the kernel to complete the resumed turn {event_id}",
        )
    finally:
        consumer.request_stop()
        # Bounded: ``read_block_ms=100`` usually ends this promptly, but a stuck
        # handler would hang the suite silently. The shield-free wait_for
        # cancels the task, and the cancellation is then awaited so the loop is
        # not left with a pending task.
        try:
            await asyncio.wait_for(task, timeout=30.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            raise AssertionError(
                "the consumer did not stop within 30s of request_stop(); a "
                "turn handler is still running"
            ) from None

    completed = [c for c in h.sink.completions if c.event_id == event_id]
    assert len(completed) == 1, completed
    assert completed[0].outcome == "delivered"
    assert (h.sink.last_text or "").startswith(expect_text), h.sink.updates


# --- The render the kernel actually asks for ---------------------------------


class _RecordingApprovals:
    """An ``ApprovalCreator`` fake that records requests and mints stable ids.

    Redeclared from apps/worker/tests/kernel/test_approval_lifecycle.py:66 --
    that module cannot be imported from here (importlib mode, no
    ``__init__.py``) and is not in this PR's file list.
    """

    def __init__(self) -> None:
        self.requests: list[Any] = []

    async def create(self, request: Any) -> CreatedApproval:
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


def _awaiting_script(summary: str) -> list[Any]:
    """A turn that ends awaiting approval (test_approval_lifecycle.py:145)."""

    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary=summary,
        ),
    ]


def _qevent(text: str, *, thread: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author="U0EXAMPLE4",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel=_APPROVERS_CHANNEL, placeholder="p-1"),
        received_at="2026-09-11T00:00:00+00:00",
        source=TurnSource.SLACK,
    )


def test_the_kernel_asks_for_the_note_render_so_that_is_the_live_card(
    make_harness: Any,
) -> None:
    """Ties ``allow_free_text`` to what the KERNEL passes, not to a test literal.

    Every other test in this module renders the card by calling
    ``approval_card(allow_free_text=True)`` itself, which would keep passing if
    ``kernel.py:3816`` flipped to ``False`` -- the note listeners would still
    work, and production would simply stop posting cards that reach them. So the
    coupling is asserted here once, at the seam where the kernel states the
    semantic intent (``ConfirmIntent.allow_free_text``, ADR-0020: the interaction
    is the port, the widget is the adapter), and the card is then rendered from
    the value the kernel actually emitted rather than from a constant.

    Driving the emit path further than this is stage 1's territory; this is the
    one fact about it the rest of the module depends on.
    """

    async def go() -> None:
        async with make_harness(approvals=_RecordingApprovals()) as h:
            h.runner.default_script = _awaiting_script("Scale the payments deployment")
            await h.kernel.process_event(_qevent("please approve", thread="th-render"))

            assert len(h.sink.posts) == 1, h.sink.posts
            _channel, message, _requested_by, _thread_ts, _endpoint = h.sink.posts[0]
            intent = message.interaction
            assert intent is not None and intent.kind == "confirm"
            assert intent.allow_free_text is True, (
                "the kernel passes allow_free_text=True UNCONDITIONALLY "
                "(kernel.py:3816, #1076), so the note action ids are the only "
                "pair production posts; if this is False, every note-path test "
                "in this module is testing a card nothing renders"
            )

            _fallback, blocks = approval_card(
                approval_id=intent.id,
                summary=intent.prompt,
                requested_by="U0EXAMPLE4",
                allow_free_text=intent.allow_free_text,
            )
            elements = [b for b in blocks if b["type"] == "actions"][0]["elements"]
            assert [e["action_id"] for e in elements] == [
                APPROVE_NOTE_ACTION_ID,
                REJECT_NOTE_ACTION_ID,
            ]
            assert [e["value"] for e in elements] == [intent.id, intent.id]

    asyncio.run(go())


# --- AC1: one pending record, one actionable card, no side effect yet --------


def test_ac1_a_created_approval_is_pending_actionable_and_has_done_nothing(
    make_harness: Any, api: Any, journey_recorder: Any
) -> None:
    """The starting state, asserted rather than assumed.

    The ``recorded == []`` assertion is a CONTROL, and it is only worth
    something because the same recorder counts to exactly one in AC2 under a
    script that does contain a gated call: the runner script here deliberately
    contains none, so the ledger is empty for a stated reason.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]
        assert created["status"] == "pending"

        card = _card(approval_id, allow_free_text=True)
        buttons = _buttons(card)

        # The action ids are IMPORTED, never literals: this is the positive
        # proof that the render production posts is the note render.
        assert [b["action_id"] for b in buttons] == [
            APPROVE_NOTE_ACTION_ID,
            REJECT_NOTE_ACTION_ID,
        ]
        assert [b["value"] for b in buttons] == [approval_id, approval_id]

        async with make_harness(actions=journey_recorder) as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]
            assert await h.async_redis.xlen(h.config.stream) == 0
            assert journey_recorder.recorded == []

    asyncio.run(go())


# --- AC2: the live NOTE journey, end to end ---------------------------------


def test_ac2_the_note_click_journey_resolves_wakes_and_settles(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
    approval_api_server: Any,
) -> None:
    """The whole live path, in order, with every hop real except the two named fakes.

    The card is rendered with ``allow_free_text=True`` because that is what the
    kernel passes unconditionally (kernel.py:3816), so the note pair is the ONLY
    pair production posts. A test that captured ``APPROVE_ACTION_ID`` here would
    be testing a path production never renders.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]

        card = _card(approval_id, allow_free_text=True)
        approve = _buttons(card)[0]
        assert approve["action_id"] == APPROVE_NOTE_ACTION_ID

        async with make_harness(actions=journey_recorder) as h:
            # The resumed turn's script carries exactly ONE gated tool call, so
            # "exactly one side effect" is mutation-sensitive rather than an
            # assertion about an empty list that can never fail.
            h.runner.default_script = [
                *gated_call("toolu_journey_01"),
                Final(text="scaled the deployment", status=DONE),
            ]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            # BEFORE the resolve: the group offset has to exist when the entry
            # lands, or the consumer never sees it.
            await consumer.ensure_group()

            resolver = composed_resolver_factory()
            config = _dispatcher_config(h.config.stream)
            app, web_client = _build_dispatcher(config, sync_redis, resolver)
            web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
                return_value={"messages": [card]}
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            # (a) The note click: a REAL GET /approvals/{id} on the loopback
            # server answers the ownership probe (approval_actions.py:159-167),
            # then the modal opens.
            click_sock = FakeSocketClient()
            handler.handle(
                click_sock,
                _click(
                    "env-note-click",
                    card=card,
                    button=approve,
                    user=_APPROVER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )
            _wait_for(
                lambda: bool(web_client.views_open.call_args),
                what="the note modal to be opened after the click",
            )
            assert click_sock.acked_envelope_ids == ["env-note-click"]
            web_client.views_open.assert_called_once()
            view = web_client.views_open.call_args.kwargs["view"]
            assert view["callback_id"] == NOTE_MODAL_CALLBACK_ID
            # Nothing has resolved yet: the click only opens the dialog.
            assert api.approval(approval_id)["status"] == "pending"
            assert await h.async_redis.xlen(h.config.stream) == 0

            # (b) The submission, built from the CAPTURED private_metadata.
            submit_sock = FakeSocketClient()
            handler.handle(
                submit_sock,
                _view_submission(
                    "env-note-submit",
                    private_metadata=view["private_metadata"],
                    user=_APPROVER,
                    note=_NOTE_TEXT,
                ),
            )
            _wait_for(
                lambda: bool(web_client.chat_update.call_args),
                what="the resolved card to be stamped after the submission",
            )
            _drain(app)
            assert submit_sock.acked_envelope_ids == ["env-note-submit"]
            # An empty ack body is the ACCEPTED submission: a refusal would carry
            # ``response_action: errors`` (handlers.py:637-645).
            assert submit_sock.ack_payload_for("env-note-submit") is None

            # (c) The audit trail, from the real authorizer.
            entries = api.audit(approval_id)
            resolved = [e for e in entries if e["action"] == "resolved"]
            assert len(resolved) == 1, entries
            assert resolved[0]["actor"] == _APPROVER
            assert resolved[0]["authorized"] is True
            assert resolved[0]["authorizer"] == "ChannelMembershipAuthorizer"
            assert resolved[0]["authorizer"] != "UnboundRouteBinding"
            assert resolved[0]["principal_kind"] == "chat"
            assert resolved[0]["actor_channel"] == _APPROVERS_CHANNEL

            # (d) The durable row.
            row = api.approval(approval_id)
            assert row["status"] == "approved"
            assert row["resolved_by"] == _APPROVER

            # (e) The REAL stream entry the REAL ResumeQueue.enqueue wrote.
            turns = await _stream_entries(h)
            assert len(turns) == 1, f"expected exactly one resume turn, got {turns}"
            assert turns[0].event_id == resume_event_id(approval_id)
            assert _NOTE_TEXT in turns[0].text

            # (f) The real consumer consumes THAT entry and acks it.
            await _consume_the_resume(
                h,
                consumer=consumer,
                event_id=turns[0].event_id,
                expect_text="scaled the deployment",
            )
            pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert pending["pending"] == 0
            # The kernel classified it as an approval resume, which is what
            # routes it down the settle path rather than treating it as a fresh
            # mention (kernel.py:3263).
            assert h.kernel._is_approval_resume(turns[0].event_id) is True

            # (g) Exactly one ledger record, from the one gated call.
            assert len(journey_recorder.recorded) == 1, journey_recorder.recorded
            assert journey_recorder.recorded[0]["frame"].call_id == "toolu_journey_01"
            assert journey_recorder.recorded[0]["event_id"] == turns[0].event_id

            # (h) The card was asked to settle. This is a ``chat_update`` call on
            # a MagicMock -- it proves the dispatcher asked Slack to stamp the
            # card, NOT that Slack rendered anything.
            web_client.chat_update.assert_called_once()
            update = web_client.chat_update.call_args.kwargs
            assert update["channel"] == _APPROVERS_CHANNEL
            assert update["ts"] == card["ts"]
            assert all(b["type"] != "actions" for b in update["blocks"])
            assert f"Approved by <@{_APPROVER}>" in update["text"]

    asyncio.run(go())


def test_ac2_variant_the_immediate_render_is_the_older_card_migration_path(
    make_harness: Any, api: Any, composed_resolver_factory: Any, sync_redis: redis.Redis
) -> None:
    """EXPLICITLY LABELLED VARIANT: ``allow_free_text=False`` is NOT what the
    kernel posts.

    ``kernel.py:3816`` passes ``allow_free_text=True`` unconditionally, so the
    immediate ``APPROVE_ACTION_ID``/``REJECT_ACTION_ID`` pair is the pre-#1059
    migration entry point for cards posted by an OLDER release, kept live so
    those cards still resolve. It is a supported variant, not a second live
    behavior, and it is the only case in this module where no ownership probe
    and no modal are involved.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]

        card = _card(approval_id, allow_free_text=False)
        buttons = _buttons(card)
        assert [b["action_id"] for b in buttons] == [APPROVE_ACTION_ID, REJECT_ACTION_ID]

        async with make_harness() as h:
            resolver = composed_resolver_factory()
            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream), sync_redis, resolver
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            sock = FakeSocketClient()
            handler.handle(
                sock,
                _click(
                    "env-immediate",
                    card=card,
                    button=buttons[0],
                    user=_APPROVER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )
            _drain(app)

            assert sock.acked_envelope_ids == ["env-immediate"]
            # No modal on this path: the click resolves directly.
            web_client.views_open.assert_not_called()
            assert api.approval(approval_id)["status"] == "approved"
            assert api.approval(approval_id)["resolved_by"] == _APPROVER

    asyncio.run(go())


# --- AC3: the real authorizer refuses, and we pin WHY -----------------------


def _refusal_assertions(
    *,
    api: Any,
    approval_id: str,
    sock: Any,
    envelope_id: str,
    web_client: WebClient,
    expected_authorizer: str,
    actor: str,
) -> None:
    """Everything an authorizer REFUSAL must look like, pinned together.

    Three discriminators, because "not 200" is satisfied by several very
    different outcomes:

    * the audit row exists, is ``authorized=false``, and names the authorizer
      that actually ran -- and is NOT ``UnboundRouteBinding``, which refuses
      everyone and would make this pass without the intended set running at all;
    * the refusal was rendered into the VIEW ACK. On the note path a 403 posts
      NO ephemeral: ``_render_outcome`` only logs for a 403, and the refusal
      reaches the approver through ``response_action: errors`` on the ack,
      because they are standing in an open modal where an ephemeral would post
      behind them (approval_actions.py:697-706, handlers.py:637-645);
    * the reason rendered is the API's OWN string, compared against the audit
      row's ``reason`` rather than a literal copied into this file.

    An ownership DECLINE has none of these (no ack at all, no audit row), so
    this set cannot be satisfied by a 404.
    """

    entries = api.audit(approval_id)
    denied = [e for e in entries if e["action"] == "denied"]
    assert len(denied) == 1, entries
    assert denied[0]["actor"] == actor
    assert denied[0]["authorized"] is False
    assert denied[0]["authorizer"] == expected_authorizer
    assert denied[0]["authorizer"] != "UnboundRouteBinding", (
        "the route binding was not seeded, so UnboundRouteBinding refused "
        "everyone and this test would have passed without the intended "
        "approver set ever running"
    )

    payload = sock.ack_payload_for(envelope_id)
    assert payload is not None, "the submission was never acked"
    assert payload["response_action"] == "errors"
    rendered = payload["errors"]["note"]
    assert rendered == denied[0]["reason"], (
        "the approver must be shown the API's own refusal reason, not a "
        f"substituted literal: rendered {rendered!r} vs audit {denied[0]['reason']!r}"
    )

    assert api.approval(approval_id)["status"] == "pending"
    # The note path refuses IN VIEW; an ephemeral here would mean the immediate
    # path ran instead.
    web_client.chat_postEphemeral.assert_not_called()


def test_ac3_an_unlisted_actor_is_refused_by_the_explicit_user_list(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
) -> None:
    """An ``ExplicitUsers``-bound route refuses an actor who is not on the list.

    The actor clicks from the approvals channel itself, so channel membership
    cannot be what refuses them: the only thing left is the explicit list.
    """

    async def go() -> None:
        agent_id = uuid.uuid4()
        await _seed_agent_with_route(
            agent_id=agent_id, route="payments", approvers={"users": [_APPROVER]}
        )
        created = api.create_approval(agent_id=str(agent_id), route="payments")
        approval_id = created["id"]

        card = _card(approval_id, allow_free_text=True)
        approve = _buttons(card)[0]

        async with make_harness(actions=journey_recorder) as h:
            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream),
                sync_redis,
                composed_resolver_factory(),
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            handler.handle(
                FakeSocketClient(),
                _click(
                    "env-unlisted-click",
                    card=card,
                    button=approve,
                    user=_OUTSIDER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )
            _wait_for(
                lambda: bool(web_client.views_open.call_args),
                what="the note modal to be opened after the click",
            )
            view = web_client.views_open.call_args.kwargs["view"]

            sock = FakeSocketClient()
            handler.handle(
                sock,
                _view_submission(
                    "env-unlisted-submit",
                    private_metadata=view["private_metadata"],
                    user=_OUTSIDER,
                    note=None,
                ),
            )
            _drain(app)

            _refusal_assertions(
                api=api,
                approval_id=approval_id,
                sock=sock,
                envelope_id="env-unlisted-submit",
                web_client=web_client,
                expected_authorizer="ExplicitUserListAuthorizer",
                actor=_OUTSIDER,
            )
            assert await h.async_redis.xlen(h.config.stream) == 0
            assert journey_recorder.recorded == []

    asyncio.run(go())


def test_ac3_an_actor_clicking_from_another_channel_is_refused(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
) -> None:
    """A ``SlackChannelMembers`` route refuses a click from a DIFFERENT channel.

    The channel is non-empty and different on purpose. ``slack_approvers.py:73-82``
    refuses on ``not actor_channel`` as well, so a click carrying no channel
    would pass this test for the missing-channel reason instead of the
    wrong-channel one -- and the actor here is the approver who WOULD be
    authorized from the right room, so the channel is the only variable.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]

        card = _card(approval_id, allow_free_text=True)
        approve = _buttons(card)[0]

        async with make_harness(actions=journey_recorder) as h:
            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream),
                sync_redis,
                composed_resolver_factory(),
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            handler.handle(
                FakeSocketClient(),
                _click(
                    "env-wrong-channel-click",
                    card=card,
                    button=approve,
                    user=_APPROVER,
                    channel=_OTHER_CHANNEL,
                ),
            )
            _wait_for(
                lambda: bool(web_client.views_open.call_args),
                what="the note modal to be opened after the click",
            )
            view = web_client.views_open.call_args.kwargs["view"]
            assert json.loads(view["private_metadata"])["channel"] == _OTHER_CHANNEL

            sock = FakeSocketClient()
            handler.handle(
                sock,
                _view_submission(
                    "env-wrong-channel-submit",
                    private_metadata=view["private_metadata"],
                    user=_APPROVER,
                    note=None,
                ),
            )
            _drain(app)

            _refusal_assertions(
                api=api,
                approval_id=approval_id,
                sock=sock,
                envelope_id="env-wrong-channel-submit",
                web_client=web_client,
                expected_authorizer="ChannelMembershipAuthorizer",
                actor=_APPROVER,
            )
            assert api.audit(approval_id)[0]["actor_channel"] == _OTHER_CHANNEL
            assert await h.async_redis.xlen(h.config.stream) == 0
            assert journey_recorder.recorded == []

    asyncio.run(go())


def test_ac3_control_an_unknown_approval_is_declined_not_refused(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    sync_redis: redis.Redis,
) -> None:
    """The OTHER negative shape, so the AC3 tests above cannot pass as this one.

    A note click on an approval this release's database does not have is an
    OWNERSHIP decline, not an authorization refusal: the envelope is left
    UNACKED so Slack retries it on another connection of the same app
    (approval_actions.py:140-156), and there is no ephemeral, no audit row and no
    modal. With a single connection, ``deliver_until_acked`` therefore returns
    None rather than hanging.
    """

    async def go() -> None:
        unknown_id = str(uuid.uuid4())
        card = _card(unknown_id, allow_free_text=True)
        approve = _buttons(card)[0]

        async with make_harness() as h:
            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream),
                sync_redis,
                composed_resolver_factory(),
            )
            sock = FakeSocketClient()
            acked_by = deliver_until_acked(
                [(SocketModeHandler(app, app_token="xapp-test"), sock, app)],
                _click(
                    "env-unknown",
                    card=card,
                    button=approve,
                    user=_APPROVER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )

            assert acked_by is None, "an unowned envelope must be left for Slack to retry"
            assert sock.acked_envelope_ids == []
            web_client.views_open.assert_not_called()
            web_client.chat_postEphemeral.assert_not_called()
            web_client.chat_update.assert_not_called()
            # No record, so no audit trail either: the API returns the frozen
            # row-miss 404 that IS the ownership signal.
            assert api.audit_status(unknown_id) == 404
            assert await h.async_redis.xlen(h.config.stream) == 0

    asyncio.run(go())


# --- AC5: one winner, whether the clicks are replayed or genuinely concurrent -


def test_ac5_a_replayed_submission_loses_and_is_told_who_won(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
) -> None:
    """The same captured submission delivered twice resolves exactly once."""

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]
        card = _card(approval_id, allow_free_text=True)
        approve = _buttons(card)[0]

        async with make_harness(actions=journey_recorder) as h:
            h.runner.default_script = [
                *gated_call("toolu_replay_01"),
                Final(text="scaled once", status=DONE),
            ]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream),
                sync_redis,
                composed_resolver_factory(),
            )
            web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
                return_value={"messages": [card]}
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            handler.handle(
                FakeSocketClient(),
                _click(
                    "env-replay-click",
                    card=card,
                    button=approve,
                    user=_APPROVER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )
            _wait_for(
                lambda: bool(web_client.views_open.call_args),
                what="the note modal to be opened after the click",
            )
            metadata = web_client.views_open.call_args.kwargs["view"]["private_metadata"]

            first = FakeSocketClient()
            handler.handle(
                first,
                _view_submission(
                    "env-replay-1", private_metadata=metadata, user=_APPROVER, note=None
                ),
            )
            second = FakeSocketClient()
            handler.handle(
                second,
                _view_submission(
                    "env-replay-2", private_metadata=metadata, user=_SECOND_APPROVER, note=None
                ),
            )
            _drain(app)

            assert first.ack_payload_for("env-replay-1") is None
            loser = second.ack_payload_for("env-replay-2")
            assert loser is not None and loser["response_action"] == "errors"
            # NAMING the winner, not merely non-200: a 404 ownership miss is
            # also not-200 and would render a completely different sentence.
            assert _APPROVER in loser["errors"]["note"], loser

            row = api.approval(approval_id)
            assert row["status"] == "approved"
            assert row["resolved_by"] == _APPROVER
            assert [e["action"] for e in api.audit(approval_id)] == ["resolved", "race_lost"]

            turns = await _stream_entries(h)
            assert len(turns) == 1, f"a second enqueue would double-wake the session: {turns}"
            assert turns[0].event_id == resume_event_id(approval_id)

            await _consume_the_resume(
                h, consumer=consumer, event_id=turns[0].event_id, expect_text="scaled once"
            )
            assert len(journey_recorder.recorded) == 1, journey_recorder.recorded

    asyncio.run(go())


def test_ac5_two_genuinely_concurrent_resolvers_produce_one_winner(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
) -> None:
    """Two threads, two clients, one barrier, and a proof they OVERLAPPED.

    This is the assertion the loopback-HTTP seam buys. Each thread holds its own
    real ``ApprovalResolveClient`` with its own ``httpx.Client``, and the real
    uvicorn server serves both at once, so the two resolves are genuinely in
    flight together rather than being serialized by a single test portal. The
    overlap is measured, not assumed: if the intervals did not intersect on a
    given run the test reports SKIPPED rather than silently passing as a proof
    of concurrency it degenerated into the replay case above. Every other
    assertion here is unaffected either way, which is why that check is last.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]

        async with make_harness(actions=journey_recorder) as h:
            h.runner.default_script = [
                *gated_call("toolu_race_01"),
                Final(text="scaled once", status=DONE),
            ]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            barrier = threading.Barrier(2)
            results: dict[str, Any] = {}
            intervals: dict[str, tuple[float, float]] = {}

            def _resolve(actor: str) -> None:
                resolver = composed_resolver_factory()
                try:
                    barrier.wait(timeout=20)
                    start = time.monotonic()
                    outcome = resolver.resolve(
                        approval_id,
                        decision="approved",
                        attested_user=actor,
                        attested_channel=_APPROVERS_CHANNEL,
                    )
                    intervals[actor] = (start, time.monotonic())
                    results[actor] = outcome
                finally:
                    # The production default ``httpx.Client`` is created inside
                    # the resolver (approval_actions.py:251-262) and nothing else
                    # owns it -- there is no ``close()`` on the class -- so two
                    # per test leak two connection pools otherwise.
                    resolver._client.close()

            threads = [
                threading.Thread(target=_resolve, args=(actor,), name=f"resolve-{actor}")
                for actor in (_APPROVER, _SECOND_APPROVER)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
                assert not thread.is_alive(), "a concurrent resolver thread hung"

            assert set(results) == {_APPROVER, _SECOND_APPROVER}, results
            codes = sorted(outcome.status_code for outcome in results.values())
            assert codes == [200, 409], results

            winner = next(a for a, o in results.items() if o.status_code == 200)
            loser_actor = next(a for a, o in results.items() if o.status_code == 409)
            loser = results[loser_actor]

            # The 409 BODY names a resolver, in the API's own sentence. This is
            # deliberately not "assert not 200": a 404 ownership miss and a 410
            # expiry are also not-200 and mean entirely different things, and the
            # dispatcher renders each of them differently.
            assert loser.detail.startswith("already resolved by "), loser.detail
            named = loser.detail[len("already resolved by ") :].split(" ", 1)[0]
            assert named in (_APPROVER, _SECOND_APPROVER), loser.detail

            # FINDING, recorded here rather than bent around. The plan asserts
            # "the 409 body names the WINNER". That holds when the loser's
            # request reads the row after the winner committed -- which is what
            # the replayed-submission test above pins -- but NOT reliably under
            # genuine concurrency, which is the case only this seam can produce.
            #
            # Cause: the resolve-once CAS is an ORM-enabled UPDATE
            # (crud.claim_approval_resolution, crud.py:1672) whose default
            # ``synchronize_session="auto"`` evaluates the criteria against the
            # session's identity map and applies the new values to any matching
            # in-memory object. The loser's session read the row while it was
            # still ``pending`` (routers/approvals.py:191) and the sessionmaker
            # sets ``expire_on_commit=False`` (db.py:31), so its in-memory copy
            # still matches, is locally stamped with the LOSER's own subject, and
            # the 409's re-read (``crud.get_approval``) hands that same identity-
            # mapped object back. The loser is then told it lost to itself.
            #
            # So the assertion that stands is the one above -- the body names ONE
            # OF THE TWO PARTIES, in the API's own sentence -- and the DATABASE
            # is asserted separately below for who actually won. Do not tighten
            # the check above to ``named == winner`` until the
            # synchronize_session behavior is fixed; do not loosen it to "not
            # 200" either. (A third assertion spelling out ``named == winner or
            # named == loser_actor`` used to sit here; those are exactly the two
            # ids the line above already admits, so it could not fail.)

            # The durable record is unambiguous even when the 409 sentence is
            # not: exactly one actor won, and it is the one that got the 200.
            row = api.approval(approval_id)
            assert row["status"] == "approved" and row["resolved_by"] == winner
            # Counted, not ordered: which of the two audit rows commits first is
            # genuinely nondeterministic here, unlike the replayed case above.
            actions = sorted(e["action"] for e in api.audit(approval_id))
            assert actions == ["race_lost", "resolved"], actions
            turns = await _stream_entries(h)
            assert len(turns) == 1, f"exactly one wake per approval: {turns}"
            assert turns[0].event_id == resume_event_id(approval_id)

            await _consume_the_resume(
                h, consumer=consumer, event_id=turns[0].event_id, expect_text="scaled once"
            )
            assert len(journey_recorder.recorded) == 1, journey_recorder.recorded

            # The honesty check on the CONCURRENCY claim specifically, and it is
            # last on purpose: every assertion above holds whether the two
            # requests overlapped or degenerated into the replay case, so none of
            # them is lost by the outcome here.
            #
            # It is a SKIP rather than a failure because it is not deterministic:
            # ``barrier.wait`` releases both threads together, but the OS can
            # still let one thread run the whole loopback round trip before the
            # other records its start, and that is a scheduling accident, not a
            # regression. What it must never do is claim concurrency it did not
            # observe, so the run is reported as skipped rather than passed.
            (a_start, a_end), (b_start, b_end) = (
                intervals[_APPROVER],
                intervals[_SECOND_APPROVER],
            )
            if not (a_start < b_end and b_start < a_end):
                pytest.skip(
                    "the two resolve requests did not overlap in time, so this "
                    "run proved the one-winner property but NOTHING about "
                    f"concurrency: {intervals}"
                )

    asyncio.run(go())


# --- AC6: the real verifier refuses four forgeries ---------------------------


def _resolve_with_principal(*, base_url: str, token: str, approval_id: str) -> httpx.Response:
    """POST one forged principal at the real resolve route, header PRESENT.

    The header being present is the whole point. ``approval_auth.py:80-81``
    raises the SAME 401 body for a request that carries no credential at all, so
    a test asserting only "401" could be landing on the missing-credential
    branch and never reaching ``verify_claims``.

    The asserts below are a construction check on this helper, not the
    discriminator: a request built with the header will always have it. The real
    discriminator lives inside each forgery test, which re-sends the identical
    envelope through THIS helper with a genuine token and shows it is accepted
    (200). That is what makes the 401 attributable to the forgery and to nothing
    else.
    """

    headers = {"X-Curie-Approval-Principal": token}
    with httpx.Client(timeout=10.0) as client:
        request = client.build_request(
            "POST",
            f"{base_url}/approvals/{approval_id}/resolve",
            json={"decision": "approved"},
            headers=headers,
        )
        assert "X-Curie-Approval-Principal" in request.headers, (
            "the principal header must actually be on the wire, or this 401 is "
            "the missing-credential branch and the verifier never ran"
        )
        assert request.headers["X-Curie-Approval-Principal"] == token
        return client.send(request)


def test_ac6_control_a_valid_principal_on_the_same_route_resolves(
    api: Any, approval_api_server: Any, approval_api_env: Any
) -> None:
    """The positive control the four forgery cases lean on.

    Same route, same header name, same request shape: only the token differs. If
    this did not return 200, every "refused" assertion below would be consistent
    with the route simply being unreachable or the header being ignored.
    """

    created = api.create_approval()
    approval_id = created["id"]
    token = mint_chat_principal(
        approval_api_env.attester_secret,
        subject=_APPROVER,
        actor_channel=_APPROVERS_CHANNEL,
        approval_id=approval_id,
    )

    response = _resolve_with_principal(
        base_url=approval_api_server.base_url, token=token, approval_id=approval_id
    )

    assert response.status_code == 200, response.text
    assert api.approval(approval_id)["status"] == "approved"


@pytest.mark.parametrize("forgery", ["tampered", "expired", "mismatched_approval", "wrong_secret"])
def test_ac6_the_real_verifier_refuses_a_forged_principal(
    make_harness: Any,
    api: Any,
    approval_api_server: Any,
    approval_api_env: Any,
    forgery: str,
) -> None:
    """Four forgeries, each refused by ``verify_claims`` rather than by absence.

    * tampered -- one byte of a validly minted token flipped
      (approval_principal.py:133, ``hmac.compare_digest``);
    * expired -- minted at ``now - 61``. ``mint_chat_principal`` sets
      ``exp = issued_at + 60``, so ``now - 1`` would still be VALID and the test
      would pass for no reason (approval_principal.py:165);
    * mismatched_approval -- a token minted for approval A submitted against
      approval B, with BOTH rows present so the refusal cannot be a 404
      (approval_principal.py:172);
    * wrong_secret -- signed with the platform ``api_key`` instead of the
      attester secret, claiming an authorized actor.
    """

    async def go() -> None:
        target = api.create_approval()
        approval_id = target["id"]
        other = api.create_approval()

        attester = approval_api_env.attester_secret
        if forgery == "tampered":
            valid = mint_chat_principal(
                attester,
                subject=_APPROVER,
                actor_channel=_APPROVERS_CHANNEL,
                approval_id=approval_id,
            )
            head, _, signature = valid.rpartition(".")
            flipped = ("A" if signature[0] != "A" else "B") + signature[1:]
            token = f"{head}.{flipped}"
        elif forgery == "expired":
            token = mint_chat_principal(
                attester,
                subject=_APPROVER,
                actor_channel=_APPROVERS_CHANNEL,
                approval_id=approval_id,
                now=int(time.time()) - 61,
            )
        elif forgery == "mismatched_approval":
            token = mint_chat_principal(
                attester,
                subject=_APPROVER,
                actor_channel=_APPROVERS_CHANNEL,
                approval_id=other["id"],
            )
        else:
            token = mint_chat_principal(
                approval_api_env.api_key,
                subject=_APPROVER,
                actor_channel=_APPROVERS_CHANNEL,
                approval_id=approval_id,
            )

        response = _resolve_with_principal(
            base_url=approval_api_server.base_url, token=token, approval_id=approval_id
        )

        assert response.status_code == 401, response.text
        assert api.approval(approval_id)["status"] == "pending"
        assert api.approval(other["id"])["status"] == "pending"
        # A forged identity must never reach the authorization stage, so there
        # is no audit row naming the actor it claimed to be.
        assert [e for e in api.audit(approval_id) if e["actor"] == _APPROVER] == []

        async with make_harness() as h:
            assert await h.async_redis.xlen(h.config.stream) == 0

            # THE DISCRIMINATOR, local to this case. ``verify_claims`` and the
            # missing-credential branch raise the SAME 401 body ("missing or
            # invalid approval principal", approval_auth.py:31-36, 80-81,
            # 112-113), so "401" alone is also consistent with the header being
            # dropped or ignored outright. Send the IDENTICAL envelope -- same
            # route, same approval, same header name, same helper -- differing
            # only in that the token is correctly signed, unexpired and bound to
            # THIS approval, and show it is accepted. That pins the refusal above
            # on the forgery rather than on absence, within this test rather than
            # by leaning on a sibling one.
            genuine = _resolve_with_principal(
                base_url=approval_api_server.base_url,
                token=mint_chat_principal(
                    attester,
                    subject=_APPROVER,
                    actor_channel=_APPROVERS_CHANNEL,
                    approval_id=approval_id,
                ),
                approval_id=approval_id,
            )
            assert genuine.status_code == 200, genuine.text
            assert api.approval(approval_id)["status"] == "approved"
            # And the accepted resolve is what puts an entry on the stream the
            # forged ones left empty, so the ``xlen == 0`` above is a real
            # observation of this stream rather than of a stream nothing ever
            # writes to.
            assert await h.async_redis.xlen(h.config.stream) == 1

    asyncio.run(go())


# --- AC-SEC3: the composed seam leaves no listening surface ------------------


def test_ac_sec3_the_composed_port_refuses_connections_after_teardown(
    approval_api_db: str, approval_api_env: Any
) -> None:
    """The loopback server exists only inside the fixture's lifetime.

    It uses ``composed_api_server`` directly rather than the fixture because the
    observation has to happen AFTER teardown, which a test still inside the
    fixture cannot make. A closed-client assertion would prove nothing -- it is
    true of any closed client; a refused TCP connection to the port the server
    was really bound to is the structural claim.
    """

    with composed_api_server() as api:
        port = api.port
        with httpx.Client(timeout=10.0) as client:
            assert client.get(f"{api.base_url}/config").status_code == 200

    with httpx.Client(timeout=5.0) as client, pytest.raises(httpx.ConnectError):
        # ``/health`` (main.py:328), not ``/healthz``: an unrouted path would
        # make this step vacuous, since a 404 proves the server is alive just as
        # well as a 200 does, and only the ConnectError is the claim.
        client.get(f"http://127.0.0.1:{port}/health")
