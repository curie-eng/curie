"""Contract tests for the agent-facing interaction harness (stage 3, D1).

These tests are the *specification* of ``curie_test_support.interaction``. They
are committed FAILING, before the module exists, and they deliberately assert
the harness's public contract rather than its journey: what a verb returns, that
the result survives ``json.dumps`` unchanged in shape, that every blocking verb
is bounded, that ``act`` cannot be short-circuited with a literal action id, and
that a fault is strictly scoped.

Why the contract and not the journey: the journey is already proven by
``apps/worker/tests/kernel/test_approval_journey_dev.py``. What stage 3 adds is a
REUSABLE surface, and a reusable surface is only reusable if its shape is pinned
somewhere that fails when the shape drifts. A test that drove a full approval
here would duplicate stage 2 and would still pass after a result field was
renamed, which is precisely the regression this module exists to catch.

A skip is a FAILURE for this module. Stage 2 recorded the reason (#1755): an
unreachable store that silently skips half a suite and exits 0 is a falsely
claimed proof. So the preconditions are asserted, not skipped past -- and,
unlike the stage-2 guard this stage is also fixing (D4), WITHOUT a
machine-specific port literal. The property that matters is agreement between
the frozen worker constant and the environment, plus an empty per-run namespace;
36379 was only ever one machine's spelling of that.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest
from curie_test_support.interaction import (
    SUPPORTED_FAULTS,
    HarnessTimeout,
    InteractionHarness,
    UncapturedAction,
)
from curie_test_support.interaction.harness import ComposedApi
from curie_test_support.interaction.results import (
    ActResult,
    CapturedAction,
    MessagesResult,
    OutcomeResult,
    ResetResult,
    SendResult,
)

# The frozen constants, imported as a MODULE attribute read at use time rather
# than re-derived from ``os.environ``. ``curie_test_support.valkey`` binds
# VALKEY_PORT at import (valkey.py:20), so anything that sets TEST_VALKEY_PORT
# after that import is a silent no-op. Reading the frozen value is the only
# honest way to check agreement.
from curie_test_support.valkey import VALKEY_PORT

# Deadlines used by the bounded-wait tests. Short, because the whole point is
# that the verb RETURNS -- a bound long enough to be comfortable is a bound that
# makes a hang look like a slow test.
_NEVER_DEADLINE_S = 1.0

# The upper bound each bounded-wait assertion allows. Twice the deadline, so a
# loaded CI box does not turn a correct implementation red, while a verb that
# ignores its deadline entirely (the mutation these tests defend against) still
# fails: an unbounded wait blows through 2x immediately.
_BOUND_SLACK = 2.0


def _round_trips(result: Any) -> dict[str, Any]:
    """Assert a verb result is JSON-serialisable and give back the parsed dict.

    ``to_dict()`` must yield ONLY str/int/float/bool/None/list/dict -- the whole
    reason the verbs return frozen dataclasses with an explicit serialiser is
    that an agent reads them over a pipe. A nested dataclass, a ``datetime``, a
    ``UUID`` or a tuple-keyed dict all survive an in-process assertion and die at
    the CLI boundary, so the round-trip is asserted here, next to every verb,
    rather than once in the CLI tests.
    """

    payload = result.to_dict()
    assert isinstance(payload, dict), f"to_dict() must return a dict, got {type(payload)!r}"
    encoded = json.dumps(payload)
    reparsed: dict[str, Any] = json.loads(encoded)
    assert reparsed == payload, (
        "to_dict() did not survive a json round-trip unchanged; a non-JSON "
        f"primitive leaked into the result: {payload!r}"
    )
    return reparsed


# --- Module preconditions -----------------------------------------------------


def test_the_harness_tests_run_against_a_real_chosen_valkey() -> None:
    """The stack this module needs is up and is the one the worker constants name.

    Deliberately portable. The property being protected is not "port 36379": it
    is (a) somebody CHOSE a store rather than inheriting a default, (b) the
    frozen worker constant agrees with that choice, and (c) no ``VALKEY_URL``
    silently overrides the parts (config.py:404-406, #2315). All three are true
    on CI's 26379 and on any developer's isolated stack; a literal is true on
    exactly one machine and is the D4 defect being fixed in Stream B.
    """

    assert os.environ.get("CI_REQUIRE_VALKEY_TESTS"), (
        "CI_REQUIRE_VALKEY_TESTS must be set: without it an unreachable Valkey "
        "SKIPS instead of failing, and a skipped harness contract test proves "
        "nothing while still exiting 0 (#1755)."
    )
    chosen = os.environ.get("TEST_VALKEY_PORT")
    assert chosen, (
        "TEST_VALKEY_PORT is unset, so this run inherited the compose default "
        "rather than choosing a store. Bring up the pilot stack and export the "
        "port it publishes."
    )
    assert VALKEY_PORT == int(chosen), (
        "the frozen worker constant disagrees with the environment the harness "
        f"will copy from: VALKEY_PORT={VALKEY_PORT!r} vs "
        f"TEST_VALKEY_PORT={chosen!r}. The constants are frozen at import "
        "(curie_test_support/valkey.py:20), so no fixture can retarget this "
        "afterwards -- the disagreement is fatal, not recoverable."
    )
    assert "VALKEY_URL" not in os.environ, (
        "VALKEY_URL wins outright over the host/port parts, so with it set the "
        "harness's API half would write to a different store than its worker "
        "half reads, and every assertion below would pass or fail for the "
        "wrong reason."
    )


# --- Fixtures -----------------------------------------------------------------


@pytest.fixture
def harness() -> Any:
    """One harness per test, entered and exited as a context manager.

    Typed ``Any`` only because the module does not exist yet; the Implementer
    narrows this to ``Iterator[InteractionHarness]``. The context manager is the
    contract: the composed API server, the dispatcher, the kernel harness, the
    disposable database and the per-run Valkey namespace are all owned by it, so
    a test that forgets to tear down is impossible to write.
    """

    with InteractionHarness() as live:
        yield live


# --- One test per verb: result shape and JSON round-trip ----------------------


def test_send_returns_a_structured_send_result(harness: InteractionHarness) -> None:
    result = harness.send("scale the payments deployment to 10 replicas")

    assert isinstance(result, SendResult)
    payload = _round_trips(result)
    # The identifiers an agent needs to address the next verb. ``message_id``
    # and ``thread`` are what a follow-up ``send(thread=...)`` and every
    # ``messages()`` lookup key on, so their absence is not cosmetic.
    assert set(payload) >= {"message_id", "thread", "run_id"}
    assert payload["message_id"], "send must name the message it created"
    assert payload["run_id"] == harness.run_id, (
        "the send must be attributed to THIS harness's run; a send that reports "
        "another run's id would let a test assert against a neighbour's stream"
    )


def test_send_into_an_existing_thread_stays_in_that_thread(
    harness: InteractionHarness,
) -> None:
    """``thread=`` is threading, not a second conversation.

    Asserted because the obvious wrong implementation -- ignore the kwarg and
    open a new thread -- produces a perfectly well-shaped SendResult, so the
    shape test above cannot catch it.
    """

    first = harness.send("first turn")
    second = harness.send("second turn", thread=first.thread)

    assert second.thread == first.thread
    assert second.message_id != first.message_id


def test_messages_returns_captured_messages_and_their_cards(
    harness: InteractionHarness,
) -> None:
    harness.send("scale the payments deployment to 10 replicas")
    result = harness.await_outcome(
        predicate=lambda snapshot: bool(snapshot.messages),
        deadline_s=30.0,
    )
    assert result.satisfied

    captured = harness.messages()
    assert isinstance(captured, MessagesResult)
    payload = _round_trips(captured)
    assert isinstance(payload["messages"], list)
    assert captured.messages, "the harness captured no channel message at all"

    # Every captured action must carry the identity ``act`` needs. This is the
    # other half of the anti-shortcut contract below: ``act`` may only be handed
    # an object that came from here, so this is where that object's fields are
    # pinned.
    for message in captured.messages:
        for action in message.actions:
            assert action.action_id, "a captured action with no action_id is unusable"
            assert action.message_id == message.message_id, (
                "a captured action must know which message it belongs to, or "
                "act() cannot tell two identical buttons apart"
            )


def test_act_returns_a_structured_act_result(harness: InteractionHarness) -> None:
    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    card = next(m for m in harness.messages().messages if m.actions)
    action = card.actions[0]

    result = harness.act(message=card.message_id, action=action, actor="U0EXAMPLE1")

    assert isinstance(result, ActResult)
    payload = _round_trips(result)
    assert set(payload) >= {"action_id", "message_id", "actor", "accepted", "outcome"}
    assert payload["action_id"] == action.action_id
    assert payload["actor"] == "U0EXAMPLE1"


def test_await_outcome_returns_a_structured_outcome_result(
    harness: InteractionHarness,
) -> None:
    harness.send("scale the payments deployment to 10 replicas")

    result = harness.await_outcome(
        predicate=lambda snapshot: bool(snapshot.messages),
        deadline_s=30.0,
    )

    assert isinstance(result, OutcomeResult)
    payload = _round_trips(result)
    assert set(payload) >= {"satisfied", "elapsed_s"}
    assert payload["satisfied"] is True
    # Elapsed is reported, not merely measured: an agent scripting the harness
    # has no other way to tell "returned immediately" from "waited 29s", and
    # that difference is the difference between a healthy run and a flake.
    assert isinstance(payload["elapsed_s"], float)
    assert payload["elapsed_s"] >= 0.0


def test_reset_returns_a_structured_reset_result(harness: InteractionHarness) -> None:
    harness.send("scale the payments deployment to 10 replicas")

    result = harness.reset()

    assert isinstance(result, ResetResult)
    payload = _round_trips(result)
    assert set(payload) >= {"stream", "entries_remaining"}
    assert payload["stream"] == harness.stream


def test_inject_fault_returns_a_context_manager(harness: InteractionHarness) -> None:
    """``inject_fault`` is a context manager, not a verb that returns a result.

    Stated as its own test because it is the one verb whose signature differs
    from the other five, and an implementation that returned a result dataclass
    here would be usable in a ``with`` statement only by accident.
    """

    with harness.inject_fault("resolve_transport_error") as armed:
        assert armed is not None
        assert harness.armed_faults == ("resolve_transport_error",)

    assert harness.armed_faults == ()


# --- The anti-shortcut test ---------------------------------------------------


def test_act_refuses_a_literal_action_id_that_was_never_captured(
    harness: InteractionHarness,
) -> None:
    """A bare string action id must raise, however plausible the string is.

    This is the test the plan calls the anti-shortcut test, and it is the single
    most load-bearing assertion in this module. ``act`` exists to prove a REAL
    chat click drove the system. An ``act`` that accepts a literal ``action_id``
    lets every future test skip the card entirely -- the card render, the
    ownership probe, the block structure -- and still report a green approval
    journey. The refusal is what keeps "the harness drove it" from degrading
    into "the harness POSTed something".

    Mutate ``act`` to accept a literal and this test fails. That is the whole
    contract.
    """

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    card = next(m for m in harness.messages().messages if m.actions)
    real_action_id = card.actions[0].action_id

    # The string is the CORRECT action id, copied off a genuinely captured
    # action. Using a nonsense string here would let an implementation pass by
    # merely validating the id against a registry, which is not the contract:
    # the contract is provenance, so even the right id in the wrong FORM is
    # refused.
    with pytest.raises(UncapturedAction) as raised:
        harness.act(message=card.message_id, action=real_action_id, actor="U0EXAMPLE1")

    assert "act" in str(raised.value)
    assert real_action_id in str(raised.value), (
        "the refusal must name the action it was handed, or a test author "
        "cannot tell a provenance refusal from a typo"
    )


# --- Bounded waits: every blocking verb has a deadline it honours -------------


def test_await_outcome_raises_harness_timeout_within_its_bound(
    harness: InteractionHarness,
) -> None:
    """A predicate that can never become true must time out, not hang.

    An unbounded wait in a test harness is the worst failure mode available: the
    suite reports nothing at all, the CI job is killed by its outer timeout, and
    the report names no test. So the bound is asserted by wall clock, and the
    exception is asserted to NAME the verb and the predicate, because a bare
    ``TimeoutError`` from 20 frames down tells an agent nothing about which of
    its scripted steps wedged.
    """

    started = time.monotonic()
    with pytest.raises(HarnessTimeout) as raised:
        harness.await_outcome(predicate=lambda snapshot: False, deadline_s=_NEVER_DEADLINE_S)
    elapsed = time.monotonic() - started

    assert elapsed < _NEVER_DEADLINE_S * _BOUND_SLACK, (
        f"await_outcome overran its {_NEVER_DEADLINE_S}s deadline by more than "
        f"{_BOUND_SLACK}x ({elapsed:.2f}s): the deadline is not being honoured"
    )
    assert raised.value.verb == "await_outcome"
    assert raised.value.deadline_s == pytest.approx(_NEVER_DEADLINE_S)
    assert raised.value.elapsed_s == pytest.approx(elapsed, abs=0.5)
    assert raised.value.what, "the timeout must describe what it was waiting for"
    assert "await_outcome" in str(raised.value)


def test_act_raises_harness_timeout_within_its_bound(harness: InteractionHarness) -> None:
    """``act`` is blocking too, and its wait is the one most likely to wedge.

    Driven through an armed ``resolve_transport_error`` rather than a synthetic
    predicate, because that is the real shape of the hang: the click is accepted,
    the post-ack listener body wedges on the resolve transport, and the
    settlement the verb is waiting for never arrives. A ``time.sleep`` stand-in
    would prove the deadline plumbing and miss the wedge.
    """

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    card = next(m for m in harness.messages().messages if m.actions)

    started = time.monotonic()
    with harness.inject_fault("resolve_transport_error", hang=True):
        with pytest.raises(HarnessTimeout) as raised:
            harness.act(
                message=card.message_id,
                action=card.actions[0],
                actor="U0EXAMPLE1",
                deadline_s=_NEVER_DEADLINE_S,
            )
    elapsed = time.monotonic() - started

    assert elapsed < _NEVER_DEADLINE_S * _BOUND_SLACK, (
        f"act overran its {_NEVER_DEADLINE_S}s deadline by more than "
        f"{_BOUND_SLACK}x ({elapsed:.2f}s)"
    )
    assert raised.value.verb == "act"
    assert raised.value.deadline_s == pytest.approx(_NEVER_DEADLINE_S)
    assert "act" in str(raised.value)


# --- Fault injection: scoped, auto-reverting, and closed over a known set -----


def test_inject_fault_reverts_even_when_the_body_raises(
    harness: InteractionHarness,
) -> None:
    """A leaked fault silently poisons every later test in the process.

    That is the failure this test exists for, and it is why the revert is
    asserted on the EXCEPTIONAL path specifically: a ``try/finally``-less
    implementation passes the happy-path test above and fails only here -- and
    in production would fail as an unrelated test, in a later module, with no
    trace back to the fault that caused it.
    """

    from curie_api.resumequeue import ResumeQueue

    # The PRODUCTION objects this fault mutates, captured before it is armed.
    # Asserting ``armed_faults == ()`` is not enough and was the hole this test
    # used to have: a ``disarm_fault`` that popped its bookkeeping dict and
    # never called ``ArmedFault.revert()`` passes that assertion while leaving
    # ``ResumeQueue.enqueue`` patched for the rest of the process.
    original_enqueue = ResumeQueue.__dict__["enqueue"]
    reconciler_was_set = "RESUME_RECONCILER_ENABLED" in os.environ
    original_reconciler = os.environ.get("RESUME_RECONCILER_ENABLED")

    sentinel = RuntimeError("body blew up")
    with pytest.raises(RuntimeError) as raised:
        with harness.inject_fault("resume_enqueue_lost"):
            assert harness.armed_faults == ("resume_enqueue_lost",)
            assert ResumeQueue.__dict__["enqueue"] is not original_enqueue, (
                "the fault armed nothing: the rest of this test would then prove "
                "only that an unarmed fault reverts cleanly"
            )
            assert os.environ["RESUME_RECONCILER_ENABLED"] == "true"
            raise sentinel

    assert raised.value is sentinel
    assert harness.armed_faults == (), (
        "the fault survived an exception in the body: every later test in this "
        "process now runs with a dropped resume enqueue"
    )
    assert ResumeQueue.__dict__["enqueue"] is original_enqueue, (
        "ResumeQueue.enqueue is still the injected stub: every later test in "
        "this process now runs with a dropped resume enqueue, including tests "
        "that assert security properties"
    )
    if reconciler_was_set:
        assert os.environ.get("RESUME_RECONCILER_ENABLED") == original_reconciler
    else:
        assert "RESUME_RECONCILER_ENABLED" not in os.environ, (
            "the fault left RESUME_RECONCILER_ENABLED set in a process that started without it"
        )


def test_inject_fault_restores_a_wrapped_resolve_client(harness: InteractionHarness) -> None:
    """``resolve_transport_error`` must put back the client attributes it broke.

    ``wrap_resolve_client`` overwrites ``client._client.get``/``.post`` IN PLACE.
    Nothing leaks today only because ``act`` and ``resolve`` happen to build a
    fresh client per call -- an incidental property of the callers, not a
    guarantee of the fault. This asserts the guarantee directly, on a client the
    test owns, so a reused client can never inherit a permanently broken
    transport from a fault that has been "reverted".
    """

    import httpx
    from curie_test_support.interaction.faults import arm_fault, wrap_resolve_client

    class _Transport:
        def get(self, *args: Any, **kwargs: Any) -> str:
            return "real-get"

        def post(self, *args: Any, **kwargs: Any) -> str:
            return "real-post"

    class _Client:
        def __init__(self) -> None:
            self._client = _Transport()

    client = _Client()
    transport = client._client
    original_get = transport.get
    assert "get" not in transport.__dict__, "the fixture must start unshadowed"

    armed = arm_fault("resolve_transport_error")
    try:
        wrap_resolve_client(client, {"resolve_transport_error": armed})
        assert transport.get is not original_get, "the wrap bent nothing"
        with pytest.raises(httpx.HTTPError):
            transport.post()
    finally:
        armed.revert()

    assert transport.get() == "real-get"
    assert transport.post() == "real-post"
    assert "get" not in transport.__dict__ and "post" not in transport.__dict__, (
        "revert left a shadowing instance attribute where the class method was"
    )


def test_disarming_an_option_only_fault_mutates_no_production_object(
    harness: InteractionHarness,
) -> None:
    """``private_metadata_unusable`` must remain option-only, arm and revert.

    It is consumed where the harness reads the modal's metadata, so it patches
    nothing -- and this pins that. If it ever grows an in-place mutation it must
    also grow a restore, and the ``_patches`` assertion is what fails first.
    """

    from curie_test_support.interaction.faults import arm_fault

    before = dict(os.environ)
    armed = arm_fault("private_metadata_unusable")
    try:
        assert armed._patches == [], "private_metadata_unusable grew a patch without a revert test"
        assert dict(os.environ) == before
    finally:
        armed.revert()
    assert dict(os.environ) == before
    assert armed._patches == []


def test_inject_fault_rejects_an_unknown_fault_name(harness: InteractionHarness) -> None:
    """An unknown name must fail loudly, naming the supported set.

    The dangerous alternative is a silent no-op: a test that arms
    ``"principle_expired"`` (one typo) would arm nothing, drive the happy path,
    and pass while claiming to cover the expired-principal case. That is a
    falsely claimed proof, so the typo has to be fatal.
    """

    with pytest.raises(ValueError) as raised:
        with harness.inject_fault("principle_expired"):  # deliberate typo
            pytest.fail("an unknown fault name must not enter the body")

    message = str(raised.value)
    assert "principle_expired" in message
    for name in SUPPORTED_FAULTS:
        assert name in message, (
            "the error must list the supported faults, so a caller fixes the "
            f"typo from the message alone; {name!r} was missing"
        )


def test_the_supported_fault_set_is_exactly_the_three_driven_faults() -> None:
    """The fault vocabulary is closed, and pinned here.

    Pinned as an exact set rather than a subset check: ``inject_fault`` is the
    only way a test can bend the system away from the happy path, so a name
    added without a test is a capability nobody is watching, and a name removed
    silently turns its D3 case into a no-op. Every name here is driven by at
    least one test: ``resume_enqueue_lost`` and ``resolve_transport_error`` by
    D3 cases 3 and 7, ``private_metadata_unusable`` by the ``act`` test below.
    The forged-principal faults are deliberately absent -- stage 2 already owns
    that coverage in ``test_approval_journey_dev.py``
    (``test_ac6_the_real_verifier_refuses_a_forged_principal``), and a second
    implementation of the minting here would be unwatched surface.
    """

    assert set(SUPPORTED_FAULTS) == {
        "resume_enqueue_lost",
        "resolve_transport_error",
        "private_metadata_unusable",
    }


def test_inject_fault_makes_act_submit_unusable_private_metadata(
    harness: InteractionHarness,
) -> None:
    """``private_metadata_unusable`` is DRIVEN, not merely declared.

    A fault name in ``SUPPORTED_FAULTS`` that no test arms is advertised surface
    nobody watches, so this is the case that exercises it end to end: the note
    modal really opens, the metadata the dispatcher really stamped is tampered
    with on its way back, and the submission must move nothing.

    The second half is what makes the first half mean anything: after the fault
    reverts, the SAME captured action resolves the approval. Without that, an
    ``act`` that silently drove nothing at all would pass the pending assertion.
    """

    from curie_dispatcher.approval_actions import APPROVE_NOTE_ACTION_ID

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    card = next(m for m in harness.messages().messages if m.actions)
    note_action = next((a for a in card.actions if a.action_id == APPROVE_NOTE_ACTION_ID), None)
    assert note_action is not None, (
        "the approval card carried no approve-with-note button, so the note "
        f"modal this fault bends is unreachable: {[a.action_id for a in card.actions]}"
    )

    with harness.inject_fault("private_metadata_unusable"):
        harness.act(
            message=card.message_id, action=note_action, actor="U0EXAMPLE1", deadline_s=60.0
        )
        assert harness.approval(str(card.approval_id)).status == "pending", (
            "a submission carrying metadata the dispatcher cannot trust must resolve nothing"
        )

    recovered = harness.act(
        message=card.message_id, action=note_action, actor="U0EXAMPLE1", deadline_s=60.0
    )
    assert recovered.accepted is True, recovered.to_dict()
    assert harness.approval(str(card.approval_id)).status == "approved"


# --- reset() ------------------------------------------------------------------


def test_reset_empties_the_runs_stream_namespace(harness: InteractionHarness) -> None:
    """After ``reset`` the run's stream is empty, asserted against real Valkey.

    Against the store, not against a counter the harness keeps: a harness that
    zeroes its own bookkeeping and leaves the entries in Valkey passes every
    in-memory assertion and then makes the NEXT test's "exactly one stream
    entry" read two.
    """

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: snapshot.stream_entries > 0,
        deadline_s=30.0,
    )

    result = harness.reset()

    assert result.entries_remaining == 0
    assert harness.redis.exists(harness.stream) == 0, (
        "reset reported an empty namespace but the stream key still exists in "
        "Valkey; the reported number is bookkeeping, not the store"
    )


def test_reset_is_idempotent(harness: InteractionHarness) -> None:
    """Calling ``reset`` on an already-clean harness must not raise.

    An agent scripting the harness resets defensively between steps and has no
    cheap way to know whether anything happened since the last one. A reset that
    raises on an absent key turns that defensive call into a scripted failure.
    """

    first = harness.reset()
    second = harness.reset()

    assert first.entries_remaining == 0
    assert second.entries_remaining == 0
    assert second.stream == first.stream


def test_reset_keeps_the_harness_usable(harness: InteractionHarness) -> None:
    """``reset`` clears state; it does not tear the harness down.

    The distinction matters because the cheap implementation of ``reset`` --
    dispose of everything and rebuild -- would pass both tests above and then
    strand every identifier an agent captured before the call.
    """

    harness.reset()
    result = harness.send("a turn after the reset")

    assert isinstance(result, SendResult)
    assert result.run_id == harness.run_id


# --- The `python -m` CLI ------------------------------------------------------


def _run_cli(script: str) -> subprocess.CompletedProcess[str]:
    """Drive the CLI exactly as an agent would: a real subprocess over stdin.

    A subprocess, not an in-process ``main([...])`` call, because the contract
    being tested is the process contract -- exit status, stdout framing and
    stderr -- and every one of those is faked by an in-process call.
    """

    return subprocess.run(
        [sys.executable, "-m", "curie_test_support.interaction"],
        input=script,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def test_the_cli_emits_one_json_object_per_verb_and_exits_zero() -> None:
    """NDJSON in, NDJSON out, one line per verb, in order.

    Line-per-verb rather than one document at the end: an agent streaming the
    harness needs the result of step 1 before it writes step 2, and a buffered
    single document is unusable for that. The ordering assertion is what pins
    it.
    """

    script = "\n".join(
        [
            json.dumps({"verb": "send", "text": "scale the payments deployment"}),
            json.dumps({"verb": "messages"}),
            json.dumps({"verb": "reset"}),
        ]
    )

    completed = _run_cli(script + "\n")

    assert completed.returncode == 0, (
        f"the CLI exited {completed.returncode}; stderr was:\n{completed.stderr}"
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 3, f"expected one JSON object per verb, got {lines!r}"
    parsed = [json.loads(line) for line in lines]
    assert [entry["verb"] for entry in parsed] == ["send", "messages", "reset"]
    for entry in parsed:
        assert entry["ok"] is True
        assert isinstance(entry["result"], dict)


def test_the_cli_rejects_an_unknown_verb_without_claiming_success() -> None:
    """A bad verb exits non-zero, says so on stderr, and emits no success line.

    The third clause is the one worth having. A CLI that printed
    ``{"ok": true}`` for the verbs it managed before the bad one, and only THEN
    exited non-zero, would be read by an agent as partial success -- and an
    agent that believes an approval was sent when it was not is exactly the
    class of false proof this whole stage exists to prevent. So no line may
    claim success for the failing verb, and the failing verb must be named.
    """

    script = json.dumps({"verb": "approve_everything"}) + "\n"

    completed = _run_cli(script)

    assert completed.returncode != 0, "an unknown verb must not exit 0"
    assert "approve_everything" in completed.stderr, (
        "stderr must name the verb it rejected, or a script author cannot tell "
        "which line of their NDJSON was wrong"
    )
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        assert entry.get("verb") != "approve_everything" or entry.get("ok") is not True, (
            "the CLI emitted a success object for the verb it rejected"
        )


def test_the_cli_reports_a_harness_timeout_as_structured_failure() -> None:
    """A timeout inside the CLI is a reported failure, not a hang or a traceback.

    Same reasoning as the in-process bounded-wait tests, at the process
    boundary: an agent scripting the harness needs the timeout to arrive as a
    parseable object naming the verb, not as a killed process with a Python
    traceback on stderr that it has to regex.
    """

    script = json.dumps(
        {"verb": "await_outcome", "predicate": "never", "deadline_s": _NEVER_DEADLINE_S}
    )

    completed = _run_cli(script + "\n")

    assert completed.returncode != 0
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert lines, "the CLI must still report the failed verb on stdout"
    entry = json.loads(lines[-1])
    assert entry["ok"] is False
    assert entry["verb"] == "await_outcome"
    assert entry["error"]["type"] == "HarnessTimeout"
    assert entry["error"]["deadline_s"] == pytest.approx(_NEVER_DEADLINE_S)


# --- The bounded wait, with the HTTP path genuinely in it ----------------------


@contextlib.contextmanager
def _black_hole_api() -> Iterator[str]:
    """A loopback server that ACCEPTS and never answers.

    The shape that makes an unclipped timeout visible: a refused connection
    fails instantly and proves nothing, while an accepted connection with no
    response parks the reader for its full read timeout. Nothing is read off the
    socket, so the accepted connections simply sit there until close.
    """

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    accepted: list[socket.socket] = []
    stop = threading.Event()

    def _accept() -> None:
        listener.settimeout(0.1)
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                continue
            accepted.append(conn)

    thread = threading.Thread(target=_accept, name="black-hole-api", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        stop.set()
        thread.join(timeout=5.0)
        for conn in accepted:
            with contextlib.suppress(OSError):
                conn.close()
        listener.close()


def test_await_outcome_stays_bounded_when_the_approval_read_hangs(
    harness: InteractionHarness,
) -> None:
    """The bound must hold when the predicate's own read is the thing that hangs.

    The other bounded-wait test above cannot catch this: it never creates an
    approval, so ``snapshot()`` short-circuits and no HTTP request is ever
    issued. With an approval in play every poll reads the row over the composed
    API, and that read carries its own timeout -- one that is many times the
    deadline this verb was given. So the read is pointed at a server that
    accepts and never answers, which is what a wedged API looks like from here,
    and the verb must still return inside its own bound rather than inside
    httpx's.
    """

    created = harness.create_approval()
    assert harness.approval_id == created.approval_id, (
        "the harness must be tracking the approval, or the HTTP read this test "
        "exists to bound is never taken"
    )

    real = harness.api
    with _black_hole_api() as base_url:
        harness._api = ComposedApi(base_url=base_url, port=real.port, app=real.app)  # noqa: SLF001
        try:
            started = time.monotonic()
            with pytest.raises(HarnessTimeout) as raised:
                harness.await_outcome(
                    predicate=lambda snapshot: False, deadline_s=_NEVER_DEADLINE_S
                )
            elapsed = time.monotonic() - started
        finally:
            harness._api = real  # noqa: SLF001

    assert elapsed < _NEVER_DEADLINE_S * _BOUND_SLACK, (
        f"await_outcome overran its {_NEVER_DEADLINE_S}s deadline by more than "
        f"{_BOUND_SLACK}x ({elapsed:.2f}s): the approval read inside the "
        "predicate is not clipped to the remaining budget"
    )
    assert raised.value.verb == "await_outcome"


# --- Provenance is the OBJECT, not its fields ---------------------------------


def test_act_refuses_a_hand_built_captured_action_with_the_real_triple(
    harness: InteractionHarness,
) -> None:
    """A fabricated ``CapturedAction`` is refused even with every field correct.

    The fields are reconstructible without ever rendering a card: the action id
    is an importable constant, the message id follows the harness's own
    numbering, and the value is the approval id. So a content match is a door
    straight past the card -- the render, the ownership probe, the block
    structure -- which is the one thing ``act`` exists to refuse. Provenance has
    to be the object that came out of ``messages()``.
    """

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    card = next(m for m in harness.messages().messages if m.actions)
    real_action = card.actions[0]

    forged = CapturedAction(
        action_id=real_action.action_id,
        message_id=real_action.message_id,
        value=real_action.value,
        text=real_action.text,
        # The handle too. It is the pipe's provenance token and it is meant to
        # be unguessable, but in process it must buy the forgery NOTHING: the
        # in-process rule is object identity, and copying every field including
        # the handle is the sharpest form of this test.
        handle=real_action.handle,
    )
    assert forged == real_action, (
        "the forgery must be field-identical, or this test would pass against "
        "a content match too and prove nothing"
    )
    assert forged is not real_action

    with pytest.raises(UncapturedAction) as raised:
        harness.act(message=card.message_id, action=forged, actor="U0EXAMPLE1")

    assert real_action.action_id in str(raised.value)


def test_act_accepts_an_action_held_across_two_messages_snapshots(
    harness: InteractionHarness,
) -> None:
    """A REAL captured action stays acceptable after a later ``messages()``.

    The other half of the rule, and the reason the refusal above has to be
    identity against the STORED objects rather than identity against one
    snapshot: an agent captures an action, calls ``messages()`` again while it
    waits, and then acts. That must work, or the provenance rule is a trap.
    """

    harness.send("scale the payments deployment to 10 replicas")
    harness.await_outcome(
        predicate=lambda snapshot: any(m.actions for m in snapshot.messages),
        deadline_s=30.0,
    )
    held = next(m for m in harness.messages().messages if m.actions).actions[0]
    later = harness.messages()
    assert later.messages, "the second snapshot captured nothing"

    result = harness.act(message=held.message_id, action=held, actor="U0EXAMPLE1")

    assert result.action_id == held.action_id


# --- The CLI can arm a fault --------------------------------------------------


def test_the_cli_can_arm_and_disarm_a_fault() -> None:
    """``inject_fault``'s pipe equivalent, or half the harness is unreachable.

    A context manager cannot span two NDJSON lines, so without an arming verb an
    agent on the pipe can drive only the happy path -- every failure case the
    fault set names would be in-process-only while the CLI still claimed the
    whole surface.
    """

    script = "\n".join(
        [
            json.dumps({"verb": "arm_fault", "fault": "resolve_transport_error"}),
            json.dumps({"verb": "disarm_fault", "fault": "resolve_transport_error"}),
        ]
    )

    completed = _run_cli(script + "\n")

    assert completed.returncode == 0, (
        f"the CLI exited {completed.returncode}; stderr was:\n{completed.stderr}"
    )
    lines = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert [entry["verb"] for entry in lines] == ["arm_fault", "disarm_fault"]
    assert lines[0]["result"]["armed"] is True
    assert lines[0]["result"]["armed_faults"] == ["resolve_transport_error"]
    assert lines[1]["result"]["armed"] is False
    assert lines[1]["result"]["armed_faults"] == []


def test_the_cli_rejects_an_unknown_fault_name_without_claiming_success() -> None:
    """A typo in an armed fault must fail the run, not silently arm nothing."""

    script = json.dumps({"verb": "arm_fault", "fault": "principle_expired"}) + "\n"

    completed = _run_cli(script)

    assert completed.returncode != 0
    lines = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert lines, "the CLI must report the failed verb on stdout"
    assert lines[-1]["ok"] is False
    assert "principle_expired" in lines[-1]["error"]["message"]


# --- AC5: the verbs really do run against the real runner ---------------------


def test_send_drives_the_real_runner_behind_the_kernel(
    harness: InteractionHarness,
) -> None:
    """``send`` is a real-runner proof, not only the one bespoke journey test.

    The kernel dials the REAL ``curie_runner`` (``build_runner(...,
    fake_model=True)`` -> ``create_app``), so the only stand-in left in the turn
    loop is the model. The fake MODEL session's recorded queries are what makes
    that assertable: a scripted test double behind the kernel would satisfy
    every other assertion in this module and record nothing here.
    """

    text = "scale the payments deployment to 10 replicas"
    harness.send(text)

    session = harness.runner._session  # noqa: SLF001 - the model seam IS the assertion
    assert session.queries, (
        "the real runner's model session saw no query: the kernel is dialling "
        "something other than the runner this harness booted"
    )
    assert any(text in query for query in session.queries), (
        f"the turn text never reached the runner's model session: {session.queries!r}"
    )
    assert any(m.actions for m in harness.messages().messages), (
        "the real runner's turn produced no approval card, so the approval the "
        "verbs drive did not come out of the production request_approval path"
    )


# --- AC3 over the pipe: the CLI's provenance is a handle, not a guess ---------


_CLI_SETUP_LINES = (
    json.dumps({"verb": "send", "text": "scale the payments deployment"}),
    json.dumps({"verb": "await_outcome", "predicate": "any_action", "deadline_s": 60.0}),
)


def _cli_entries(completed_stdout: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in completed_stdout.splitlines() if line.strip()]


def test_the_cli_refuses_an_act_named_only_by_a_guessable_action_id() -> None:
    """The pipe must be exactly as unforgeable as the in-process identity rule.

    In process ``act`` matches on object identity, so a hand-built
    ``CapturedAction`` is refused however correct its fields. The CLI has no
    objects and must look the stored action up from the request -- and if it
    looks it up by ``action_id`` / ``message`` / ``value``, the identity check
    is handed the REAL stored object and cannot fail, while every one of those
    fields is reconstructible without the card ever rendering. This test
    reconstructs them exactly that way: the action id is imported from the
    production renderer's own constants, and the message id is the harness's
    ``1700.%04d`` scheme. ``messages`` is deliberately never called, so the
    script is a caller who has seen no card at all -- and the click must be
    refused.
    """

    from curie_dispatcher.approval_actions import APPROVE_NOTE_ACTION_ID

    script = (
        "\n".join(
            [
                *_CLI_SETUP_LINES,
                json.dumps(
                    {
                        "verb": "act",
                        "message": "1700.0000",
                        "action_id": APPROVE_NOTE_ACTION_ID,
                        "actor": "U0EXAMPLE1",
                    }
                ),
            ]
        )
        + "\n"
    )

    completed = _run_cli(script)

    assert completed.returncode != 0, (
        "the CLI accepted an act named only by its action id and message id -- "
        "both reconstructible without ever calling messages(), so AC3 is "
        "forgeable over the pipe"
    )
    failed = _cli_entries(completed.stdout)[-1]
    assert failed["verb"] == "act"
    assert failed["ok"] is False
    assert failed["error"]["type"] == "UncapturedAction"
    assert "handle" in failed["error"]["message"]


def test_the_cli_refuses_an_act_with_a_fabricated_handle() -> None:
    """A handle no capture minted names nothing, however well-formed.

    The companion to the test above: refusing a request with no handle buys
    nothing if any string in the handle slot is accepted. A fabricated handle is
    also what a STALE one looks like -- handles are minted per captured action
    inside one harness process, so one carried over from an earlier run has
    nothing to name in this one.
    """

    script = (
        "\n".join(
            [
                *_CLI_SETUP_LINES,
                json.dumps(
                    {
                        "verb": "act",
                        "message": "1700.0000",
                        "handle": "not-a-handle-that-was-ever-minted",
                        "actor": "U0EXAMPLE1",
                    }
                ),
            ]
        )
        + "\n"
    )

    completed = _run_cli(script)

    assert completed.returncode != 0, "the CLI accepted a handle it never minted"
    failed = _cli_entries(completed.stdout)[-1]
    assert failed["verb"] == "act"
    assert failed["ok"] is False
    assert failed["error"]["type"] == "UncapturedAction"
    assert "not-a-handle-that-was-ever-minted" in failed["error"]["message"]


def test_the_cli_accepts_an_act_carrying_a_handle_from_a_real_messages_call() -> None:
    """The positive control: the handle route must actually work, in one process.

    Without this the two refusals above are satisfied by a CLI that refuses
    every ``act``, which is an unusable surface rather than a provenance rule.

    It is driven over a LIVE pipe rather than a pre-written script, and that is
    the point rather than test mechanics: a handle is minted per captured action
    inside one harness process, so the only way to hold one is to read the
    ``messages`` answer from the same process before writing the ``act`` line.
    A caller that could not do that would have no honest route to ``act`` at
    all -- which is the pressure that pushes a provenance rule back into a
    guessable one. So this also pins that the CLI answers line by line, and that
    ``messages`` publishes the handle.
    """

    process = subprocess.Popen(
        [sys.executable, "-m", "curie_test_support.interaction"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert process.stdin is not None and process.stdout is not None
    try:
        for line in (*_CLI_SETUP_LINES, json.dumps({"verb": "messages"})):
            process.stdin.write(line + "\n")
        process.stdin.flush()

        answers = [json.loads(process.stdout.readline()) for _ in range(3)]
        assert [entry["verb"] for entry in answers] == ["send", "await_outcome", "messages"]
        card = next(message for message in answers[-1]["result"]["messages"] if message["actions"])
        handle = card["actions"][0]["handle"]
        assert handle, "messages() published no handle, so the CLI's act is unreachable"

        process.stdin.write(
            json.dumps(
                {
                    "verb": "act",
                    "message": card["message_id"],
                    "handle": handle,
                    "actor": "U0EXAMPLE1",
                }
            )
            + "\n"
        )
        process.stdin.flush()
        acted = json.loads(process.stdout.readline())
        process.stdin.close()
        status = process.wait(timeout=180)
        assert status == 0, f"the CLI exited {status} after a valid act"
    finally:
        process.kill()

    assert acted["verb"] == "act"
    assert acted["ok"] is True, f"the CLI refused an act carrying a real handle: {acted!r}"
    assert acted["result"]["action_id"] == card["actions"][0]["action_id"]


# --- The bounded wait, with VALKEY as the thing that hangs --------------------


def test_await_outcome_stays_bounded_when_valkey_hangs(
    harness: InteractionHarness,
) -> None:
    """The mirror of the black-hole HTTP test, against the other blocking edge.

    ``snapshot()`` -- the thing ``await_outcome`` polls -- reads the stream
    length out of Valkey before it reads the approval status over HTTP, and
    ``resume_turns`` xranges the same stream. The client those go through comes
    from ``connect_or_skip``, which sets ``socket_connect_timeout`` and
    deliberately NO ``socket_timeout``, so a Valkey that accepts and never
    answers parks the command inside a C-level socket read. ``_Deadline.wait``
    polls a predicate and cannot interrupt that, so the verb's headline bound is
    broken by the first thing the predicate touches. The existing HTTP test
    cannot see this: it reaches the status read only after the Valkey read has
    already returned.

    So the harness's client is pointed at a black hole -- the same accept-and-
    never-answer server, which is what a wedged Valkey looks like from here --
    and the verb must still return inside its own bound.
    """

    import redis

    real = harness.redis
    with _black_hole_api() as base_url:
        host, port = base_url.removeprefix("http://").split(":")
        wedged = redis.Redis(
            host=host,
            port=int(port),
            decode_responses=True,
            socket_connect_timeout=2.0,
        )
        harness._redis = wedged  # noqa: SLF001
        try:
            started = time.monotonic()
            with pytest.raises(HarnessTimeout) as raised:
                harness.await_outcome(predicate=lambda snapshot: True, deadline_s=_NEVER_DEADLINE_S)
            elapsed = time.monotonic() - started
        finally:
            harness._redis = real  # noqa: SLF001
            wedged.close()

    assert elapsed < _NEVER_DEADLINE_S * _BOUND_SLACK, (
        f"await_outcome overran its {_NEVER_DEADLINE_S}s deadline by more than "
        f"{_BOUND_SLACK}x ({elapsed:.2f}s): the Valkey read inside snapshot() is "
        "not clipped to the remaining budget"
    )
    assert raised.value.verb == "await_outcome"


def test_reset_stays_bounded_when_valkey_hangs(harness: InteractionHarness) -> None:
    """``reset`` is one of the six verbs and took no ``deadline_s`` at all.

    Two Valkey commands are fast until Valkey is the wedged thing, and an agent
    is told to reset defensively between steps -- so the unbounded one was the
    verb most likely to be called into a hang, with no verb name on the result.
    """

    import redis

    real = harness.redis
    with _black_hole_api() as base_url:
        host, port = base_url.removeprefix("http://").split(":")
        wedged = redis.Redis(
            host=host,
            port=int(port),
            decode_responses=True,
            socket_connect_timeout=2.0,
        )
        harness._redis = wedged  # noqa: SLF001
        try:
            started = time.monotonic()
            with pytest.raises(HarnessTimeout) as raised:
                harness.reset(deadline_s=_NEVER_DEADLINE_S)
            elapsed = time.monotonic() - started
        finally:
            harness._redis = real  # noqa: SLF001
            wedged.close()

    assert elapsed < _NEVER_DEADLINE_S * _BOUND_SLACK, (
        f"reset overran its {_NEVER_DEADLINE_S}s deadline by more than "
        f"{_BOUND_SLACK}x ({elapsed:.2f}s)"
    )
    assert raised.value.verb == "reset"
