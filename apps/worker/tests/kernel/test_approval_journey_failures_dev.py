"""The failure half of the approval journey (D3) -- the seven cases stage 2 deferred.

Stage 2 (``test_approval_journey_dev.py``) proved the journey's SUCCESS paths and
five refusals (duplicate click, concurrent click, tampered / expired / mismatched
principal, wrong channel, unauthorized actor). None of those are re-implemented
here. This module adds SIX of the seven cases it deferred, each driven through
the same real API / worker / dispatcher / Postgres / Valkey composition, and each
carrying a POSITIVE control plus a comment naming the exact mutation that must
break it. The seventh -- note-modal validation failure -- is not provable in this
stage and is NOT claimed as covered; see the UNMET PROOF note below, and case 4,
which characterises the shipped behavior instead.

Every case is driven honestly, which for the two background loops means
something specific. ``approval_api_env`` DISABLES the expiry sweeper and the
resume reconciler (conftest.py, ``APPROVAL_SWEEP_INTERVAL_S=0`` /
``RESUME_RECONCILER_ENABLED=false``) because a live loop can flip a row or
enqueue a second resume mid-test. So the two cases about those loops invoke the
PRODUCTION code exactly once, directly, using the LIVE ``app.state`` objects the
running server's lifespan composed (its sessionmaker, its ``ResumeQueue``, and
for case 3 the ``ResumeReconciler`` instance itself), driven on the server's own
event loop. What those cases do NOT do is assert ``x is module.x`` for a name
they imported themselves: that check cannot fail and proves nothing. The pins
they carry instead walk the chain the app walks at runtime -- ``lifespan``'s own
bytecode still naming the scheduled callable, ``curie_api.main`` resolving that
global to the production module's object, and the loop's bytecode still naming
the function under test -- so a rename or a wrapper anywhere on that chain
breaks one of them rather than leaving a stale local copy passing.

UNMET PROOF: note-modal *validation* failure cannot be proven, because no
server-side note validation exists. ``build_note_modal`` declares the note input
``optional`` with ``max_length=_NOTE_MAX_LENGTH`` (approval_actions.py:522-565),
but that bound is a Block Kit CLIENT-side constraint: the submit handler
(``resolve_note_submission``, approval_actions.py:647-717) reads the note, strips
it and posts it, and ``ApprovalResolve.note`` on the API side is an unconstrained
``str | None`` (schemas.py:1559-1566). Adding such validation is a production
``src/`` change and is out of this stage's authority, so case 4 is NOT covered
here and is not claimed as covered anywhere. It is tracked as a gap in the
stage-3 handoff. What case 4 carries instead is a CHARACTERISATION test of the
behavior that actually ships today, named so it cannot be mistaken for a
validation proof.

Placement and the import dance mirror ``test_approval_journey_dev.py``: under
``--import-mode=importlib`` with no ``__init__.py`` a test module cannot import a
sibling's helpers by name, so the stage-2 module is loaded BY PATH and its
helpers are reused rather than forked. Forking ``_card`` / ``_click`` /
``_view_submission`` / ``_refusal_assertions`` would let this module drift away
from the renders and envelopes stage 2 pins.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_api.resumequeue import resume_event_id
from curie_dispatcher.approval_actions import NOTE_MODAL_CALLBACK_ID
from fastapi import Request
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _module_from_path(path: Path, name: str) -> Any:
    """Import one file as a module, by path, reusing an already-imported one.

    Deliberately a local copy of ``test_approval_journey_dev.py``'s helper: that
    module is the thing being loaded, so it cannot also be the source of the
    loader. Everything else below comes from it rather than being redeclared.
    """

    for module in list(sys.modules.values()):
        file = getattr(module, "__file__", None)
        if file and Path(file).resolve() == path:
            return module
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_JOURNEY = _module_from_path(
    Path(__file__).resolve().parent / "test_approval_journey_dev.py",
    "_journey_stage_two",
)

_card = _JOURNEY._card
_buttons = _JOURNEY._buttons
_click = _JOURNEY._click
_view_submission = _JOURNEY._view_submission
_drain = _JOURNEY._drain
_wait_for = _JOURNEY._wait_for
_stream_entries = _JOURNEY._stream_entries
_build_dispatcher = _JOURNEY._build_dispatcher
_dispatcher_config = _JOURNEY._dispatcher_config
FakeSocketClient = _JOURNEY.FakeSocketClient
_APPROVERS_CHANNEL = _JOURNEY._APPROVERS_CHANNEL
_APPROVER = _JOURNEY._APPROVER
_NOTE_TEXT = _JOURNEY._NOTE_TEXT
_DB_SCHEMA = _JOURNEY._DB_SCHEMA

# The agent-facing harness Stream A is building. Imported at module scope on
# purpose: if it is missing, every test in this module errors loudly rather than
# silently falling back to the stage-2 composition it exists to replace.
from curie_test_support.interaction import (  # noqa: E402 - see the comment above
    InteractionHarness,
    UncapturedAction,
)


@pytest.fixture
def api(approval_api_server: Any, approval_api_env: Any) -> Any:
    """The composed API's read/write helper, reused from stage 2 by path.

    Stage 2 declares this fixture in its own module body, and a fixture declared
    in a test module is visible only to that module -- which is why every case
    here that asked for ``api`` errored with "fixture 'api' not found". Declared
    here over the SAME ``_Api`` class rather than forked, for the reason the
    module docstring gives: a copy would drift from the routes stage 2 pins.
    """

    helper = _JOURNEY._Api(approval_api_server.base_url, approval_api_env.api_key)
    try:
        yield helper
    finally:
        helper.close()


# Bounds. Every blocking call in this module carries one: the threaded uvicorn
# server, the Bolt listener executor and the real consumer are all hang
# surfaces, and a hang reports nothing at all in the run report.
_DEADLINE_S = 20.0


def _server_loop(composed: Any, api_key: str) -> asyncio.AbstractEventLoop:
    """The event loop the uvicorn thread is actually serving this app on.

    Every object the lifespan composed -- ``app.state.sessionmaker`` and its
    asyncpg pool, ``app.state.resume_queue``'s async redis client,
    ``app.state.resume_reconciler`` -- belongs to THAT loop; awaiting one from a
    loop of this test's own dies with "got Future attached to a different loop".
    The loop is not reachable off the app object, so it is captured through the
    seam FastAPI provides for exactly this: ``dependency_overrides`` wraps the
    production ``get_session`` for the duration of ONE ordinary request (a GET
    of an approval id that does not exist -- the dependency runs before the
    handler, so a 404 is a fine carrier), the wrapper reads
    ``asyncio.get_running_loop()`` and then delegates to the real dependency,
    and the override is removed again in a ``finally``. No route is added, no
    production module is patched, and the only thing the wrapper does beyond
    delegating is read the loop.

    Cached on ``app.state`` so a second caller does not re-run the probe.
    """

    from curie_api.deps import get_session

    cached = getattr(composed.app.state, "journey_server_loop", None)
    if cached is not None:
        return cached  # type: ignore[no-any-return]

    captured: dict[str, asyncio.AbstractEventLoop] = {}

    async def _capturing_session(request: Request) -> Any:
        captured["loop"] = asyncio.get_running_loop()
        async for session in get_session(request):
            yield session

    composed.app.dependency_overrides[get_session] = _capturing_session
    try:
        response = httpx.get(
            f"{composed.base_url}/approvals/{uuid.uuid4()}",
            headers={"X-API-Key": api_key},
            timeout=_DEADLINE_S,
        )
        assert response.status_code == 404, response.text
    finally:
        composed.app.dependency_overrides.pop(get_session, None)

    loop = captured["loop"]
    assert loop.is_running(), "the captured server loop is not running"
    composed.app.state.journey_server_loop = loop
    return loop


async def _on_server_loop(loop: asyncio.AbstractEventLoop, coro: Any) -> Any:
    """Await ``coro`` on the API server's loop from this test's loop, bounded."""

    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return await asyncio.wait_for(asyncio.wrap_future(future), timeout=_DEADLINE_S)


async def _resolve_note_through_the_dispatcher(
    *,
    h: Any,
    api: Any,
    approval_id: str,
    resolver: Any,
    sync_redis: redis.Redis,
    envelope: str,
    note: str | None = None,
    mutate_metadata: Callable[[str], str] | None = None,
) -> tuple[Any, Any, Any]:
    """Click, capture the modal metadata, then submit -- the stage-2 note path.

    Returns ``(submit_socket, web_client, card)``. ``mutate_metadata`` is the
    only addition over stage 2's inline sequence and exists for case 6: it
    receives the CAPTURED ``private_metadata`` and returns the string the
    submission carries, so the tamper is applied to real captured bytes rather
    than to a hand-built blob that might not resemble what production emits.
    """

    card = _card(approval_id, allow_free_text=True)
    approve = _buttons(card)[0]
    app, web_client = _build_dispatcher(_dispatcher_config(h.config.stream), sync_redis, resolver)
    web_client.conversations_replies = MagicMock(return_value={"messages": [card]})
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _click(
            f"{envelope}-click",
            card=card,
            button=approve,
            user=_APPROVER,
            channel=_APPROVERS_CHANNEL,
        ),
    )
    _wait_for(
        lambda: bool(web_client.views_open.call_args),
        what="the note modal to be opened after the click",
        timeout=_DEADLINE_S,
    )
    metadata = web_client.views_open.call_args.kwargs["view"]["private_metadata"]
    if mutate_metadata is not None:
        metadata = mutate_metadata(metadata)

    submit = FakeSocketClient()
    handler.handle(
        submit,
        _view_submission(
            f"{envelope}-submit",
            private_metadata=metadata,
            user=_APPROVER,
            note=note,
        ),
    )
    _drain(app, timeout=_DEADLINE_S)
    return submit, web_client, card


# --- Case 1: a rejection is a terminal outcome, and the worker resumes with it


def test_case1_a_rejected_approval_resumes_the_session_with_the_rejection() -> None:
    """A real reject card action reaches ``rejected`` and wakes the session.

    Driven entirely through the harness verbs -- ``send`` / ``messages`` /
    ``act`` / ``await_outcome`` -- because this case is the one with a fully
    known answer, which makes it the honest place to prove the harness is
    usable rather than merely present. ``act`` is given the action OBJECT
    captured from ``messages()``; handing it the literal action id raises
    ``UncapturedAction``, and that anti-shortcut is asserted here too so this
    module cannot quietly degrade into driving literals.

    FALSIFIABLE NEGATIVE: flip the kernel's reject branch to take the approve
    path (``kernel.py`` approval-resume routing) and the resumed turn no longer
    carries the rejection -- ``outcome.status`` stops being ``rejected`` and the
    delivered text stops naming the refusal.
    """

    with InteractionHarness() as harness:
        sent = harness.send("please approve scaling payments to 10 replicas")
        assert sent.to_dict(), "every harness result must be JSON-serialisable"

        captured = harness.messages()
        card = captured.cards[-1]
        reject = [a for a in card.actions if "reject" in a.action_id][0]

        # Anti-shortcut: a bare string is not a captured action.
        with pytest.raises(UncapturedAction):
            harness.act(message=card.message, action=reject.action_id, actor=_APPROVER)

        acted = harness.act(
            message=card.message, action=reject, actor=_APPROVER, deadline_s=_DEADLINE_S
        )
        assert acted.accepted is True, acted.to_dict()

        outcome = harness.await_outcome(
            predicate=lambda state: state.approval_status == "rejected",
            deadline_s=_DEADLINE_S,
        )
        assert outcome.approval_status == "rejected"
        assert outcome.resolved_by == _APPROVER
        # Terminal for the APPROVAL and a wake for the SESSION: exactly one
        # resume, and the resumed turn carries the rejection rather than
        # silently resuming as though it had been approved.
        assert len(outcome.resume_turns) == 1, outcome.to_dict()
        assert outcome.resume_turns[0].event_id == resume_event_id(outcome.approval_id)
        assert "reject" in outcome.resume_turns[0].text.casefold(), outcome.to_dict()
        assert json.dumps(outcome.to_dict())


# --- Case 2: expiry, driven by the PRODUCTION sweeper, invoked once ----------


async def _age_the_approval(approval_id: str, *, age: timedelta) -> None:
    """Move the row's ``expires_at`` into the past, in the real database.

    Ageing the ROW rather than passing a future ``now`` to the sweeper is what
    keeps the case honest: the sweeper then selects the record through its own
    real ``list_expired_pending_approvals`` query against a real clock, instead
    of being told what time it is.
    """

    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text(
                    f"UPDATE {_DB_SCHEMA}.approvals SET expires_at = :expires "
                    "WHERE id = CAST(:id AS uuid) AND status = 'pending'"
                ),
                {
                    "expires": datetime.now(UTC).replace(tzinfo=None) - age,
                    "id": approval_id,
                },
            )
            assert result.rowcount == 1, (
                f"approval {approval_id} was not pending when it was aged, so the "
                "sweep below would have nothing to select and would pass vacuously"
            )
    finally:
        await engine.dispose()


def test_case2_the_production_expiry_sweeper_expires_and_wakes_the_session(
    make_harness: Any,
    api: Any,
    approval_api_server: Any,
    approval_api_env: Any,
    journey_recorder: Any,
) -> None:
    """One direct call to the REAL ``sweep_expired_approvals``, after ageing a row.

    The sweeper loop is disabled by ``approval_api_env`` (its 30 s interval can
    flip a pending row out from under any other test in the module), so it is
    invoked exactly ONCE here, by hand -- but against the LIVE
    ``app.state.sessionmaker`` and ``app.state.resume_queue`` the running
    server's lifespan composed, driven on that server's own loop, not against a
    look-alike engine and queue built here. Those objects are what make the pins
    below mean something: an ``is`` check against a name this test imported
    itself cannot fail, so the honest pins are (a) ``lifespan``'s own bytecode
    still naming ``run_expiry_sweeper``, (b) the name ``curie_api.main`` resolves
    that global to being the sweeper module's function, and (c) the loop's
    bytecode still naming ``sweep_expired_approvals``. Together those are the
    chain lifespan actually walks at runtime; a rename or a wrapper anywhere on
    it breaks one of the three.

    FALSIFIABLE NEGATIVE: remove the expiry check (make
    ``crud.expire_approval``'s CAS unconditional, or drop the ``expires_at``
    filter from ``list_expired_pending_approvals``) and either the flip count or
    the "still pending before the sweep" assertion fails.
    """

    import inspect

    from curie_api import main as api_main
    from curie_api import sweeper as api_sweeper

    # The chain the running app walks, pinned link by link. `lifespan` is an
    # @asynccontextmanager, so unwrap to the function whose bytecode schedules
    # the loop.
    lifespan_fn = inspect.unwrap(api_main.lifespan)
    assert "run_expiry_sweeper" in lifespan_fn.__code__.co_names, (
        "the API lifespan no longer schedules run_expiry_sweeper by that global "
        "name, so this test is invoking a sweeper the app does not run"
    )
    scheduled = api_main.__dict__["run_expiry_sweeper"]
    assert scheduled is api_sweeper.run_expiry_sweeper, (
        "curie_api.main resolves run_expiry_sweeper to something other than the "
        "sweeper module's function"
    )
    sweep_expired_approvals = api_sweeper.sweep_expired_approvals
    assert "sweep_expired_approvals" in scheduled.__code__.co_names, (
        "the production sweeper loop no longer calls sweep_expired_approvals by "
        "that global name, so invoking it here proves nothing about the loop"
    )

    state = approval_api_server.app.state
    loop = _server_loop(approval_api_server, approval_api_env.api_key)

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]
        await _age_the_approval(approval_id, age=timedelta(minutes=5))
        assert api.approval(approval_id)["status"] == "pending"

        async with make_harness(actions=journey_recorder) as h:
            # The live queue must be writing to the stream this test reads, or
            # the "exactly one wake" assertion below would be vacuous.
            assert state.resume_queue._stream == h.config.stream

            async def sweep() -> int:
                async with state.sessionmaker() as session:
                    swept = await sweep_expired_approvals(session, state.resume_queue)
                    await session.commit()
                    return swept

            flipped = await _on_server_loop(loop, sweep())

            assert flipped == 1, "the aged approval was not swept"
            row = api.approval(approval_id)
            assert row["status"] == "expired"
            assert row["resolved_by"] is None

            audit = api.audit(approval_id)
            expired = [e for e in audit if e["action"] == "expired"]
            assert len(expired) == 1, audit
            assert expired[0]["authorizer"] == "ExpirySweeper"

            # The wake reached the stream exactly once, and it is the expiry
            # resume rather than a resolve resume.
            turns = await _stream_entries(h)
            assert len(turns) == 1, turns
            assert turns[0].event_id == resume_event_id(approval_id)

    asyncio.run(go())


# --- Case 3: a lost resume enqueue, recovered by the PRODUCTION reconciler ---


def test_case3_the_production_reconciler_enqueues_exactly_one_lost_resume() -> None:
    """The enqueue is dropped by an armed fault; the reconciler re-enqueues once.

    Driven entirely inside the harness -- one composition, one database, one
    stream namespace. Mixing the harness with stage 2's fixtures here would put
    two API servers on one row and make "exactly one resume" ambiguous about
    which server wrote it.

    ``inject_fault("resume_enqueue_lost")`` is a context manager and auto-reverts;
    a leaked fault would poison every later test in the process, which is why the
    recovery pass below is asserted rather than assumed.

    FALSIFIABLE NEGATIVE: never call the reconciler (or disable it) and the
    stream stays empty; drop the ``resumed_at`` mark / the row claim and the
    second pass enqueues again, which double-wakes the session.
    """

    import inspect

    from curie_api import main as api_main
    from curie_api import resumereconciler as api_reconciler

    ResumeReconciler = api_reconciler.ResumeReconciler
    # The chain the running app walks: lifespan's own bytecode still names the
    # class, and main resolves that global to the reconciler module's class. An
    # ``is`` check against a name imported here could not fail and proves
    # nothing; these two can.
    lifespan_fn = inspect.unwrap(api_main.lifespan)
    assert "ResumeReconciler" in lifespan_fn.__code__.co_names, (
        "the API lifespan no longer constructs ResumeReconciler by that global "
        "name, so this test drives a reconciler the app does not run"
    )
    assert api_main.__dict__["ResumeReconciler"] is ResumeReconciler

    async def go() -> None:
        with InteractionHarness() as harness:
            approval_id = harness.create_approval().approval_id

            with harness.inject_fault("resume_enqueue_lost"):
                acted = harness.resolve(
                    approval_id=approval_id,
                    decision="approved",
                    actor=_APPROVER,
                    deadline_s=_DEADLINE_S,
                )
                assert acted.accepted is True, acted.to_dict()

            # The resolve committed; the wake did not reach the stream. That is
            # precisely the stranded shape the reconciler exists for.
            assert harness.approval(approval_id).status == "approved"
            assert harness.resume_turns().turns == ()

            # The LIVE objects, not look-alikes: the reconciler instance the
            # app's lifespan composed, holding app.state.sessionmaker and
            # app.state.resume_queue. Those belong to the uvicorn server's loop
            # (an asyncpg connection and an asyncio redis client are bound to
            # the loop they were created on), so the pass is driven there with
            # ``_on_server_loop`` rather than on a loop of this test's own.
            composed = harness.api
            state = composed.app.state
            loop = _server_loop(composed, harness.env.api_key)

            reconciler = state.resume_reconciler
            assert type(reconciler) is ResumeReconciler, (
                "the lifespan composed something other than the production ResumeReconciler"
            )
            # The method invoked is the production one, not a subclass override.
            assert type(reconciler).reconcile_once is ResumeReconciler.reconcile_once
            # And it is wired to the app's own state, so the row it claims and
            # the stream it enqueues on are the ones this harness is reading.
            assert reconciler._sessionmaker is state.sessionmaker
            assert reconciler._resume_queue is state.resume_queue
            assert state.resume_queue._stream == harness.stream

            # The single divergence from the running configuration, and the only
            # one: a zero grace. The grace exists so a live inline-delivered
            # turn is not double-woken, and the fault above guarantees no turn
            # was ever delivered. It is set on the production instance (and
            # restored below) rather than escaped by building a second
            # reconciler, so everything else about the object under test is what
            # the app runs.
            grace = reconciler._grace_seconds
            reconciler._grace_seconds = 0
            try:
                enqueued = await _on_server_loop(loop, reconciler.reconcile_once())
                assert enqueued == 1, "the reconciler did not recover the lost wake"

                turns = harness.resume_turns().turns
                assert len(turns) == 1, turns
                assert turns[0].event_id == resume_event_id(approval_id)

                # A second pass is a no-op: ``resumed_at`` is now set, so the
                # NULL-gated finder no longer selects the row. Without this,
                # "exactly one" above would only mean "one so far".
                assert await _on_server_loop(loop, reconciler.reconcile_once()) == 0
                assert len(harness.resume_turns().turns) == 1
            finally:
                reconciler._grace_seconds = grace

    asyncio.run(go())


# --- Case 4: what the product actually does with a note today ---------------
#
# NOT a validation proof. See the module docstring's UNMET PROOF note: there is
# no server-side note validation to exercise, so this records the shipped
# behavior instead of asserting behavior nobody implemented.


def test_case4_the_product_does_not_validate_the_note_server_side_today(
    make_harness: Any, api: Any, composed_resolver_factory: Any, sync_redis: redis.Redis
) -> None:
    """CHARACTERISATION: a note of any length and content is accepted and stored.

    This test documents a gap; it does not endorse it. The modal declares
    ``max_length=_NOTE_MAX_LENGTH`` on its input element, which Slack enforces in
    the CLIENT. Nothing downstream re-checks it: ``resolve_note_submission``
    strips the note and posts it, and ``ApprovalResolve.note`` is an
    unconstrained ``str | None``. So a submission carrying a note longer than the
    modal's own declared bound -- which is exactly what an envelope replayed or
    synthesised outside the Slack client looks like -- resolves normally.

    If a note validator is ever added, THIS test is the one that must be changed,
    deliberately, to the refusal shape. That is the point of characterising it:
    the change becomes visible in review instead of silently widening.

    The genuine modal error path is NOT re-implemented here. The one production
    source of a ``response_action: errors`` body is a non-200 resolve outcome
    (``resolve_note_submission``, approval_actions.py:700-706), and stage 2
    already covers it end to end in
    ``test_approval_journey_dev.py::test_ac3_an_unlisted_actor_is_refused_by_the_explicit_user_list``
    (a 403 rendered into the view ack) and
    ``::test_ac5_a_replayed_submission_loses_and_is_told_who_won`` (a 409). Its
    single-fact coupling to this module is asserted below, against the same
    captured ack, rather than duplicated.
    """

    from curie_api.resumequeue import _RESUME_NOTE_MAX
    from curie_dispatcher.approval_actions import _NOTE_MAX_LENGTH, _VERDICT_LINE_MAX

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]
        over_long = "x" * (_NOTE_MAX_LENGTH + 1)

        async with make_harness() as h:
            submit, web_client, card = await _resolve_note_through_the_dispatcher(
                h=h,
                api=api,
                approval_id=approval_id,
                resolver=composed_resolver_factory(),
                sync_redis=sync_redis,
                envelope="env-note-unvalidated",
                note=over_long,
            )

            # An EMPTY ack body is the accepted submission; an errors body is the
            # refusal. Today the over-length note takes the first branch.
            assert submit.acked_envelope_ids == ["env-note-unvalidated-submit"]
            assert submit.ack_payload_for("env-note-unvalidated-submit") is None, (
                "a note longer than the modal's declared max_length was REFUSED, "
                "which means server-side note validation now exists; the gap this "
                "test characterises has been closed, so rewrite it to the refusal "
                "shape rather than relaxing this assertion"
            )

            # And it really did resolve and persist, unvalidated and untruncated.
            row = api.approval(approval_id)
            assert row["status"] == "approved"
            assert row["resolved_by"] == _APPROVER
            resolved = [e for e in api.audit(approval_id) if e["action"] == "resolved"]
            assert len(resolved) == 1, api.audit(approval_id)
            # The DURABLE record is the unvalidated one, and that is the gap
            # being characterised: nothing between the modal and the database
            # checked the note's length, so the row holds all 2001 characters.
            assert row["resolution_note"] == over_long, (
                "the stored note was not the submitted note, so something "
                "between the modal and the database is now validating or "
                "transforming it and this characterisation is out of date"
            )

            turns = await _stream_entries(h)
            assert len(turns) == 1, turns
            # CORRECTED to the product's real behavior. The resume turn does NOT
            # carry the note verbatim: ``_framed_note`` cuts it to
            # ``_RESUME_NOTE_MAX`` and marks the cut with an ellipsis
            # (resumequeue.py:144-167). That is a RENDER bound on what is handed
            # to the model, the same class of bound as the card's verdict line
            # below -- not validation, which is why this remains a
            # characterisation of an unvalidated note rather than a proof of one.
            assert _RESUME_NOTE_MAX < len(over_long), (
                "the over-long note is no longer past the resume turn's own "
                "bound, so the truncation asserted below is vacuous"
            )
            framed = over_long[: _RESUME_NOTE_MAX - 1] + "\u2026"
            assert framed in turns[0].text, turns[0].text[-200:]
            assert over_long not in turns[0].text

            # The card settled carrying the WHOLE note. CORRECTED: the previous
            # expectation (the verdict line truncated for Slack's own limit) was
            # wrong about the product at this length -- the card's render bounds
            # are ``_VERDICT_LINE_MAX`` (2900) on the context block and
            # ``_FALLBACK_TEXT_MAX`` (39000) on the ``text`` fallback, and a note
            # one character past the MODAL's 2000 is under both. That is the
            # characterisation's point rather than a detail: the only bound the
            # note ever meets is the resume turn's, and no surface refuses it.
            web_client.chat_update.assert_called_once()
            update = web_client.chat_update.call_args.kwargs
            assert update["ts"] == card["ts"]
            assert len(over_long) < _VERDICT_LINE_MAX, (
                "the over-long note now exceeds the card's own render bound, so "
                "the whole-note assertion below is asserting truncation instead"
            )
            assert over_long in update["text"]

    asyncio.run(go())


# --- Case 5: the approver cancels the modal ----------------------------------


def _view_closed(envelope_id: str, *, private_metadata: str, user: str) -> SocketModeRequest:
    """A ``view_closed`` envelope for the note modal, from CAPTURED metadata.

    Same envelope shape Slack sends when the approver presses Cancel: the view,
    its callback id and its ``private_metadata``, with no ``state`` values at
    all, because nothing was submitted.
    """

    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "view_closed",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "is_cleared": False,
            "view": {
                "id": f"V-{envelope_id}",
                "type": "modal",
                "callback_id": NOTE_MODAL_CALLBACK_ID,
                "private_metadata": private_metadata,
                "state": {"values": {}},
                "hash": "1",
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
            },
        },
    )


def test_case5_cancelling_the_modal_changes_nothing_at_all(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
) -> None:
    """Cancel is a no-op on every surface: record, card and stream.

    The click already happened, so the approval is mid-journey with a modal open;
    the cancel must leave it exactly where the click left it. All three surfaces
    are asserted because "still pending" alone is also true of a dispatcher that
    stamped the card or wrote a stream entry and then failed to persist.

    FALSIFIABLE NEGATIVE: treat cancel as an approve (register a ``view_closed``
    listener that resolves) and the status, the card and the stream assertions
    all fail together.
    """

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]
        card = _card(approval_id, allow_free_text=True)
        approve = _buttons(card)[0]

        async with make_harness(actions=journey_recorder) as h:
            app, web_client = _build_dispatcher(
                _dispatcher_config(h.config.stream), sync_redis, composed_resolver_factory()
            )
            handler = SocketModeHandler(app, app_token="xapp-test")

            handler.handle(
                FakeSocketClient(),
                _click(
                    "env-cancel-click",
                    card=card,
                    button=approve,
                    user=_APPROVER,
                    channel=_APPROVERS_CHANNEL,
                ),
            )
            _wait_for(
                lambda: bool(web_client.views_open.call_args),
                what="the note modal to be opened after the click",
                timeout=_DEADLINE_S,
            )
            metadata = web_client.views_open.call_args.kwargs["view"]["private_metadata"]
            # Nothing is resolved by the click itself; this is the BEFORE half of
            # the comparison the cancel must not move.
            assert api.approval(approval_id)["status"] == "pending"

            sock = FakeSocketClient()
            handler.handle(
                sock,
                _view_closed("env-cancel-closed", private_metadata=metadata, user=_APPROVER),
            )
            _drain(app, timeout=_DEADLINE_S)

            row = api.approval(approval_id)
            assert row["status"] == "pending"
            assert row["resolved_by"] is None
            # No audit row at all: a cancel is not a decision, and recording one
            # would put a phantom actor in the trail an operator reads.
            assert api.audit(approval_id) == []
            # The card is untouched -- no settle stamp, no ephemeral.
            web_client.chat_update.assert_not_called()
            web_client.chat_postEphemeral.assert_not_called()
            assert await h.async_redis.xlen(h.config.stream) == 0
            assert journey_recorder.recorded == []

    asyncio.run(go())


# --- Case 6: unusable / tampered private_metadata ----------------------------


@pytest.mark.parametrize(
    ("tamper", "why"),
    [
        ("drop_approval_id", "no approval id at all"),
        ("unparseable", "not JSON"),
        ("foreign_decision", "a decision the handler does not accept"),
    ],
)
def test_case6_a_tampered_private_metadata_is_refused_not_declined(
    make_harness: Any,
    api: Any,
    composed_resolver_factory: Any,
    journey_recorder: Any,
    sync_redis: redis.Redis,
    tamper: str,
    why: str,
) -> None:
    """The submission is refused, and the refusal is NOT an ownership decline.

    The distinction is the whole point and it is structural, not cosmetic. An
    ownership DECLINE leaves the Socket Mode envelope UNACKED so Slack retries it
    on another connection of the same app (``decline_unowned_envelope``,
    approval_actions.py:140-156) -- that is how two releases sharing one Slack app
    hand work to each other. An unusable ``private_metadata`` is this release's
    own problem: there is nothing to hand over, so the envelope IS acked and the
    journey simply stops. Asserting only "still pending" would be satisfied by
    both, and by a crash.

    The metadata is mutated from the bytes ``views_open`` really carried, never
    hand-built, so a change to what production puts in there moves this test
    with it.

    FALSIFIABLE NEGATIVE: an ignore-metadata mutation (default the decision to
    ``approved``, or read the approval id off the view id) resolves the approval,
    and the pending/stream/card assertions fail.
    """

    def _mutate(raw: str) -> str:
        if tamper == "unparseable":
            return raw[: len(raw) // 2]
        meta = json.loads(raw)
        assert meta["approval_id"], "the captured metadata carried no approval id to tamper with"
        if tamper == "drop_approval_id":
            meta.pop("approval_id")
        else:
            meta["decision"] = "maybe"
        return json.dumps(meta)

    async def go() -> None:
        created = api.create_approval()
        approval_id = created["id"]

        async with make_harness(actions=journey_recorder) as h:
            submit, web_client, _card_message = await _resolve_note_through_the_dispatcher(
                h=h,
                api=api,
                approval_id=approval_id,
                resolver=composed_resolver_factory(),
                sync_redis=sync_redis,
                envelope=f"env-tamper-{tamper}",
                note=_NOTE_TEXT,
                mutate_metadata=_mutate,
            )

            # REFUSED, not declined: the envelope was acked, so Slack will not
            # retry it against another release.
            assert submit.acked_envelope_ids == [f"env-tamper-{tamper}-submit"], (
                f"an unusable private_metadata ({why}) must be acked and dropped, "
                "not left unacked for another release to retry -- there is "
                "nothing for another release to pick up"
            )
            # Nothing moved, which is what separates this from an ACCEPTED
            # submission (also acked, also empty-bodied).
            assert api.approval(approval_id)["status"] == "pending"
            assert api.audit(approval_id) == []
            web_client.chat_update.assert_not_called()
            assert await h.async_redis.xlen(h.config.stream) == 0
            assert journey_recorder.recorded == []

    asyncio.run(go())


# --- Case 7: the resolve call itself fails -----------------------------------


def test_case7_a_failed_resolve_transport_is_surfaced_and_wakes_nobody() -> None:
    """The resolve POST never lands; the approver is told, and nothing resumes.

    The dangerous shape this pins is the phantom resume: a dispatcher that
    optimistically settled the card or enqueued a wake on a resolve it never got
    an answer to would resume a session for a decision the database does not
    have. ``ResolveOutcome(status_code=0)`` is explicitly an UNKNOWN outcome
    (approval_actions.py:239-244), and the only safe rendering of unknown is to
    tell the approver it failed and change nothing.

    FALSIFIABLE NEGATIVE: swallow the ``httpx.HTTPError`` in
    ``ApprovalResolveClient.resolve`` and return a 200-shaped outcome, and both
    the ``status_code == 0`` assertion and the empty-stream assertion fail.
    """

    with InteractionHarness() as harness:
        approval_id = harness.create_approval().approval_id

        with harness.inject_fault("resolve_transport_error"):
            acted = harness.resolve(
                approval_id=approval_id,
                decision="approved",
                actor=_APPROVER,
                deadline_s=_DEADLINE_S,
            )

        assert acted.accepted is False, acted.to_dict()
        # CORRECTED to the product's real behavior at this seam. The original
        # expectation (``response_action == "errors"`` plus a rendered "try
        # again") describes the MODAL surface, and ``resolve`` deliberately has
        # no card and no view: ``response_action`` is built by
        # ``resolve_note_submission`` out of a ``view_submission`` body
        # (approval_actions.py:700-706) and the text is stamped by
        # ``render_note_submission``. Neither runs on the bare resolve hop, so a
        # harness that reported them here would be inventing a Slack surface the
        # product never rendered. What the product really does is below, and it
        # is the stronger claim: the client turns the transport error into the
        # explicit UNKNOWN outcome and hands it back untouched.
        assert acted.status_code == 0, acted.to_dict()
        assert acted.detail == "injected resolve transport failure", acted.to_dict()
        assert acted.approval_id == approval_id, acted.to_dict()
        # And the words the approver gets for that outcome are the dispatcher's
        # own, from the one shared wording function both surfaces render through.
        # Asserted against the SAME ``ResolveOutcome`` shape the hop produced, so
        # a change to either half moves this with it; the rendering of it INTO a
        # view is covered end to end by stage 2's 403 and 409 cases.
        from curie_dispatcher.approval_actions import ResolveOutcome, _refusal_text

        assert (
            "try again"
            in _refusal_text(
                ResolveOutcome(status_code=acted.status_code, detail=acted.detail)
            ).casefold()
        )

        # Nothing was committed and nothing was woken.
        assert harness.approval(approval_id).status == "pending"
        assert harness.audit(approval_id).entries == ()
        assert harness.resume_turns().turns == ()

        # And the fault auto-reverted: the same act now succeeds, which is what
        # proves the empty stream above was caused by the fault rather than by a
        # harness that never drove anything at all.
        recovered = harness.resolve(
            approval_id=approval_id,
            decision="approved",
            actor=_APPROVER,
            deadline_s=_DEADLINE_S,
        )
        assert recovered.accepted is True, recovered.to_dict()
        assert harness.approval(approval_id).status == "approved"
        assert len(harness.resume_turns().turns) == 1
