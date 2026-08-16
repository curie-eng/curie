"""The seven rule wire floor, as checks a third party can run on its adapter.

``run_floor`` is the one entry point both front doors call. It returns a
``FloorReport`` and never raises on a nonconformant adapter: a refusal is
evidence, and the report is where evidence goes.

**The verdict domain is the AUTOMATABLE clause set, and the field is named for
exactly what it covers.** ``automated_floor`` answers one question, "did every
machine checkable clause hold", and ``manual_review_required`` answers the
other, "what did no machine check". Neither is allowed to swallow the other.
There is no field named ``conformant`` at any depth of the output, because a
boolean by that name is truthfully quotable as whole floor conformance while
clause 3c was never asserted, and the name itself was the hole.

**Missing evidence is never success.** Any automatable clause that is not
``pass``, including ``not_run`` from an unsupplied driver or an adapter that
exposes no side effect probe, makes ``automated_floor`` fail. There is no
``partial`` and no ``skipped`` status: deleting them is what makes the rule
structural rather than a label a later revision can relax.

Two clauses are outside the domain and carry ``automatable=False``:

* **3c, constant time comparison.** Two rejections that take different amounts
  of wall time are indistinguishable from two rejections on a loaded box, and no
  HTTP status carries the answer. An adapter that compares with ``==`` answers
  every request exactly as one that uses ``hmac.compare_digest``.
* **7c, the loud stale credential signal.** The adapter's operator has to LEARN
  that its ingress token died. That signal is a log line, a metric or a page,
  and none of them cross the wire the kit can see.

Both are reported as ``not_run`` with the reason and what a human must read
instead, and neither ever reads as ``pass``.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .driver import IngressDriver, UpstreamIdentity
from .ingress import NO_RESPONSE, TURNS_PATH, FakeIngress, ObservedRequest
from .transport import (
    MAX_ACK_BODY_BYTES,
    WRONG_SECRET,
    AdapterUnderTest,
    AdapterUnreachableError,
    new_conversation_id,
    new_event_id,
    reply_post,
    reply_update,
    turn_completed,
    turn_status,
)

FloorStatus = Literal["pass", "fail", "not_run"]
FloorMode = Literal["strict", "diagnostic"]

_STRICT = ConfigDict(extra="forbid")

# How long the kit waits for an adapter to reach the ingress at all.
_ARRIVAL_TIMEOUT_S = 15.0

# How long the kit waits for the driver to report a delivery RETIRED. This is
# the finality barrier, and it is a bound on the driver's answer rather than a
# window the verdict is taken at the end of: a driver that never answers leaves
# rule 2 nonpassing.
_QUIESCENCE_TIMEOUT_S = 20.0

# The hard bound on ANY vendor supplied driver callback. A callback that does
# not answer is a harness defect and is reported as one; it is never allowed to
# become a hang, because a hang reads as a slow test rather than as a broken
# adapter and therefore tells nobody anything.
_DRIVER_CALLBACK_TIMEOUT_S = 5.0

# The one observation window this kit uses for every negative assertion, and
# the reason there is only one of it: three bespoke constants were three
# separate things to outlast.
#
# It is a DETECTOR window, not an inference window, and the difference is the
# whole fix. The old shape asked "has anything happened recently" and concluded
# from a quiet interval that nothing further ever would, which is not something
# a timer can establish. This one runs AFTER a barrier that claims the work is
# finished, and treats anything it sees as a failure of that claim, named and
# attributed. A pass therefore reports what was actually observed rather than
# an inference from silence, and the clause details say exactly that.
_GRACE_S = 1.8

# How long the kit lets a 401 settle before asking whether the adapter is still
# serving. An adapter that treats a stale credential as fatal takes a moment to
# finish dying, and probing into that window would read a corpse as alive.
_LIVENESS_SETTLE_S = 1.2

# How long the kit waits for a held delivery to resume after an operator
# supplied replacement token.
_RESUME_TIMEOUT_S = 8.0

_POLL_S = 0.02


class ClauseResult(BaseModel):
    """One floor clause, and whether a machine could decide it at all."""

    model_config = _STRICT

    clause: str
    status: FloorStatus
    automatable: bool
    detail: str


class FloorResult(BaseModel):
    """One floor rule, whose status is derived from its automatable clauses."""

    model_config = _STRICT

    rule: int
    title: str
    status: FloorStatus
    clauses: list[ClauseResult]
    detail: str


class ManualReviewItem(BaseModel):
    """A clause no black box check can decide, and what a human must read."""

    model_config = _STRICT

    clause: str
    title: str
    reason: str
    how_to_review: str


class FloorReport(BaseModel):
    """The whole verdict, and everything the verdict does not cover."""

    model_config = _STRICT

    automated_floor: Literal["pass", "fail"]
    manual_review_required: list[ManualReviewItem]
    mode: FloorMode
    counts: dict[str, int]
    results: list[FloorResult]

    def detail(self) -> str:
        """The verdict and the manual review list, rendered together.

        Together, always: a verdict printed on its own reads as more than it
        covers, which is the failure this report's whole shape exists to stop.
        """

        counts = ", ".join(f"{name}: {value}" for name, value in sorted(self.counts.items()))
        lines = [
            f"automated_floor: {self.automated_floor} (mode: {self.mode})",
            f"clause counts: {counts}",
            "manual review required, decided by no machine:",
        ]
        for item in self.manual_review_required:
            lines.append(f"  {item.clause} {item.title}")
            lines.append(f"    why no check decides it: {item.reason}")
            lines.append(f"    how to review it: {item.how_to_review}")
        lines.append("rules:")
        for result in self.results:
            lines.append(f"  rule {result.rule} [{result.status}] {result.title}")
            for clause in result.clauses:
                lines.append(f"    {clause.clause} [{clause.status}] {clause.detail}")
        return "\n".join(lines)


MANUAL_REVIEW: tuple[ManualReviewItem, ...] = (
    ManualReviewItem(
        clause="3c",
        title="the egress secret is compared in constant time",
        reason=(
            "a comparison that returns early and one that does not answer every "
            "request identically, and wall time over a network is dominated by "
            "scheduling noise, so no response the adapter can send carries the answer"
        ),
        how_to_review=(
            "read the adapter's secret check and confirm it uses a constant time "
            "primitive such as hmac.compare_digest, never == on the two strings"
        ),
    ),
    ManualReviewItem(
        clause="7c",
        title="a stale ingress credential is signalled loudly",
        reason=(
            "the signal is for the adapter's own operator and lands in a log, a "
            "metric or a page, so it never crosses the wire this kit observes"
        ),
        how_to_review=(
            "arm a 401 from ingress and confirm the adapter emits a distinguishable "
            "stale credential signal an operator would actually see, not a line "
            "buried among ordinary delivery errors"
        ),
    ),
)


def run_floor(
    adapter: AdapterUnderTest,
    *,
    driver: IngressDriver | None = None,
    side_effect_probe: Callable[[], int] | None = None,
    mode: FloorMode = "strict",
) -> FloorReport:
    """Check one running adapter against the seven rule floor.

    ``driver`` is what makes rules 1, 2, 7 and clause 3b decidable, and
    ``side_effect_probe`` is what makes clause 3a and rule 6 decidable. Omit
    either and the clauses it covers report ``not_run``, which is nonconformant.
    """

    ingress: FakeIngress | None = None
    try:
        if driver is not None:
            ingress = FakeIngress(kind=adapter.kind, address=adapter.address)
            ingress.start()
            _bounded_call(
                "start", lambda: driver.start(ingress_url=ingress.url, token=ingress.token)
            )
        results = [
            _rule_3(adapter, driver, side_effect_probe),
            _rule_4(adapter),
            _rule_5(adapter),
            _rule_6(adapter, side_effect_probe),
        ]
        if driver is None or ingress is None:
            results.extend(_ingress_rules_not_run())
        else:
            # Rule 2 runs before rule 1 deliberately: it is the only rule whose
            # false positive control (a legitimate upstream redelivery under an
            # identity the kit never declared) is provoked by an adapter's FIRST
            # successful delivery, so a rule that consumed that delivery first
            # would leave the control asserting nothing.
            rule_2 = _guarded(
                2, _RULE_2_TITLE, "2", lambda: _rule_2(driver, ingress)
            )
            rule_1 = _guarded(
                1, _RULE_1_TITLE, "1", lambda: _rule_1(driver, ingress)
            )
            rule_7 = _guarded(
                7, _RULE_7_TITLE, "7a", lambda: _rule_7(adapter, driver, ingress)
            )
            results.extend([rule_1, rule_2, rule_7])
    finally:
        if driver is not None:
            # Bounded, and its fault swallowed: the report is already assembled,
            # so there is no clause left to fail, and a driver that will not stop
            # must not be able to hold the run open or to keep the ingress
            # listener alive behind it.
            try:
                _bounded_call("the driver's stop()", driver.stop)
            except _HarnessFault:
                pass
        if ingress is not None:
            ingress.stop()
    results.sort(key=lambda result: result.rule)
    return _report(results, mode=mode)


def _report(results: list[FloorResult], *, mode: FloorMode) -> FloorReport:
    clauses = [clause for result in results for clause in result.clauses]
    counts = {
        status: sum(1 for clause in clauses if clause.status == status)
        for status in ("pass", "fail", "not_run")
    }
    decided = all(
        clause.status == "pass" for clause in clauses if clause.automatable
    )
    return FloorReport(
        automated_floor="pass" if mode == "strict" and decided else "fail",
        manual_review_required=list(MANUAL_REVIEW),
        mode=mode,
        counts=counts,
        results=results,
    )


def _result(rule: int, title: str, clauses: list[ClauseResult]) -> FloorResult:
    """Roll clauses up into a rule status over the AUTOMATABLE clauses only.

    A rule carrying an unobservable clause would otherwise be permanently short
    of pass, which is a verdict no correct adapter can reach and therefore a
    verdict that carries no information.
    """

    automatable = [clause for clause in clauses if clause.automatable]
    if any(clause.status == "fail" for clause in automatable):
        status: FloorStatus = "fail"
    elif any(clause.status == "not_run" for clause in automatable):
        status = "not_run"
    else:
        status = "pass"
    detail = "; ".join(f"{clause.clause} [{clause.status}] {clause.detail}" for clause in clauses)
    return FloorResult(rule=rule, title=title, status=status, clauses=clauses, detail=detail)


def _clause(clause: str, status: FloorStatus, detail: str) -> ClauseResult:
    return ClauseResult(clause=clause, status=status, automatable=True, detail=detail)


def _manual_clause(clause: str, detail: str) -> ClauseResult:
    return ClauseResult(clause=clause, status="not_run", automatable=False, detail=detail)


def _verdict(clause: str, problems: list[str], passed: str) -> ClauseResult:
    if problems:
        return _clause(clause, "fail", "; ".join(problems))
    return _clause(clause, "pass", passed)


def _is_2xx(status: int) -> bool:
    return 200 <= status < 300


# --- the two mechanisms every negative assertion in this kit is built on ------


class _HarnessFault(RuntimeError):
    """A vendor callback that did not answer, or answered by raising.

    Covers both halves of the harness the vendor supplies: the ingress driver
    and the side effect probe. Carried as an exception so no call site can
    forget it, and reported as a clause FAILURE naming the harness. Both are
    written by the same party as the adapter, so this is rarely malice: it is a
    vendor implementing a callback naively, and the outcome that must never
    happen is that they read a pass and ship.

    ``reason`` is the bare cause, so a caller closer to the failure can say
    where it happened without quoting a whole sentence inside its own.
    """

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


def _bounded_call(what: str, call: Callable[[], Any]) -> Any:
    """Run one vendor callback under a hard bound. Never inline.

    The kit calls vendor callbacks inside timed rules, and a synchronous call to
    one that blocks forever takes the whole run with it: the deadline the caller
    thinks it has is never reached, because control never comes back to check
    it. So every callback runs on a daemon thread that is joined with a timeout
    and then ABANDONED. Abandoned deliberately: a blocked callback cannot be
    made to return, and waiting for it during teardown would only move the hang
    somewhere less visible. A daemon thread does not hold interpreter exit, so
    the cost of abandoning it is bounded.

    A callback that misses the bound is never retried. It has already shown it
    does not answer, and polling it again would leak one blocked thread per
    poll.

    Raising is handled here too, and for the same reason: an exception out of
    the middle of a clause aborts the whole report, so it becomes an
    attributable clause failure instead.
    """

    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["value"] = call()
        except BaseException as error:  # noqa: BLE001
            outcome["error"] = f"{type(error).__name__}: {error}"

    worker = threading.Thread(target=run, daemon=True, name="conformance-callback")
    worker.start()
    worker.join(_DRIVER_CALLBACK_TIMEOUT_S)
    if worker.is_alive():
        reason = (
            f"did not answer within {_DRIVER_CALLBACK_TIMEOUT_S} seconds, so it is "
            "holding the kit rather than answering it"
        )
        raise _HarnessFault(
            f"{what} {reason}. This is a defect in the harness rather than in the "
            "adapter: every vendor callback has to return, and one that blocks turns "
            "a verdict into a hang",
            reason=reason,
        )
    if "error" in outcome:
        reason = f"raised {outcome['error']}"
        raise _HarnessFault(
            f"{what} {reason}, so the kit could not reach a verdict. This is a defect "
            "in the harness rather than in the adapter",
            reason=reason,
        )
    return outcome.get("value")


def _probe_reader(probe: Callable[[], int], clause_id: str) -> Callable[[], int]:
    """The side effect probe, wrapped so a failure mid run is a clause failure.

    Discovery only ever established that the probe answered ONCE. A probe that
    starts failing partway through a run used to escape as an httpx error or a
    KeyError out of the middle of a clause and abort the entire report, and the
    observation windows read it many times per clause rather than twice, so
    there are far more chances for it to happen. Same mechanism as the driver
    callbacks, and the same reason: an unexpected condition becomes an
    attributable failure, never an escaped exception and never a silent pass.
    """

    def read() -> int:
        try:
            return int(_bounded_call("the side effect probe", probe))
        except _HarnessFault as fault:
            raise _HarnessFault(
                f"the side effect probe {fault.reason} while clause {clause_id} was "
                "in flight, so the count this clause decides on is unreadable. This "
                "is the probe endpoint rather than the adapter's reply handling: "
                "discovery proved only that it answered once, and a clause reads it "
                "many times",
                reason=fault.reason,
            ) from None

    return read


def _guarded(
    rule: int, title: str, clause_id: str, run: Callable[[], FloorResult]
) -> FloorResult:
    """Run one ingress rule, turning a driver fault into a clause failure."""

    try:
        return run()
    except _HarnessFault as fault:
        return _result(rule, title, [_clause(clause_id, "fail", str(fault))])


def _side_effects_after(
    probe: Callable[[], int], baseline: int, *, allowed: int
) -> tuple[int, bool]:
    """Watch the side effect count for the grace window. Returns (worst, exceeded).

    A DETECTOR, not an inference. The clause has already sent everything it is
    going to send, so the question is not "has the count settled" but "does it
    stay inside its allowance", and any excess is a failure the moment it
    appears rather than a miss. Exits early on an excess, so the break costs
    less than the control does.
    """

    deadline = time.monotonic() + _GRACE_S
    worst = probe() - baseline
    while worst <= allowed and time.monotonic() < deadline:
        time.sleep(_POLL_S)
        worst = max(worst, probe() - baseline)
    return worst, worst > allowed


# --- rule 3: verify the egress secret before any side effect -----------------


def _rule_3(
    adapter: AdapterUnderTest,
    driver: IngressDriver | None,
    probe: Callable[[], int] | None,
) -> FloorResult:
    return _result(
        3,
        "verify the egress secret on every request, before any side effect",
        [
            _clause_3a(adapter, probe),
            _clause_3b(adapter, driver, probe),
            _manual_clause(
                "3c",
                "constant time comparison is not observable over HTTP; see "
                "manual_review_required",
            ),
        ],
    )


def _clause_3a(adapter: AdapterUnderTest, probe: Callable[[], int] | None) -> ClauseResult:
    """A wrong or absent secret is refused, AND causes no side effect.

    The status alone is not enough and this is the exact hole it leaves: an
    adapter that performs the side effect and then returns 401 answers
    identically to a correct one. Only a side effect count separates them, so
    without a probe this clause is ``not_run``.

    Both counts are taken through the settling barrier rather than read off the
    probe directly. The refusal and the effect are separate events, so an
    adapter that answers 401 and acts a moment afterwards is indistinguishable
    from a conformant one at the instant the response arrives, and reading the
    count there is reading the wrong moment.
    """

    if probe is None:
        return _clause(
            "3a",
            "not_run",
            "no side effect probe was supplied, so an adapter that performs the side "
            "effect and then returns 401 could not be told from one that refuses first",
        )
    conversation = new_conversation_id()
    read = _probe_reader(probe, "3a")
    try:
        # The refused requests go FIRST, before this clause has sent the adapter
        # anything it would accept. That is what makes the baseline trustworthy
        # without a drain: nothing the kit has sent can still be in flight, so
        # any movement during the window below was caused by a refusal and by
        # nothing else. Checking the adapter's own secret first would leave its
        # legitimate effect racing the window and cost a false failure.
        baseline = read()
        wrong = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=WRONG_SECRET
        )
        absent = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=None
        )
        leaked, exceeded = _side_effects_after(read, baseline, allowed=0)
        accepted = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=adapter.secret
        )
    except AdapterUnreachableError as error:
        return _clause("3a", "fail", str(error))
    except _HarnessFault as fault:
        return _clause("3a", "fail", str(fault))
    if accepted.status in (401, 403):
        return _clause(
            "3a",
            "fail",
            f"the adapter answered {accepted.status} to its own configured secret, "
            "so it refuses the platform rather than verifying it",
        )
    problems: list[str] = []
    if _is_2xx(wrong.status):
        problems.append(f"a wrong secret was accepted with {wrong.status}")
    if _is_2xx(absent.status):
        problems.append(f"an absent secret was accepted with {absent.status}")
    if exceeded:
        problems.append(
            f"the refused requests moved the side effect count by {leaked} within "
            f"{_GRACE_S} seconds of the refusal, so the adapter answered the "
            "correspondent and rejected the request afterwards"
        )
    return _verdict(
        "3a",
        problems,
        f"a wrong secret answered {wrong.status}, an absent one answered "
        f"{absent.status}, and the side effect count did not move in the "
        f"{_GRACE_S} seconds after either",
    )


def _clause_3b(
    adapter: AdapterUnderTest,
    driver: IngressDriver | None,
    probe: Callable[[], int] | None,
) -> ClauseResult:
    """An adapter whose OWN egress secret is unset refuses everything.

    Serving unauthenticated is worse than not serving: anyone who can reach the
    endpoint could forge a completion. Restoring the real secret afterwards is
    part of the clause, or every rule that runs later is checking a deaf adapter.

    "Refuses" is about the SIDE EFFECT and not only the status, exactly as in
    clause 3a. An adapter that answers the correspondent and then returns 401
    has served an unauthenticated request; its response says otherwise, and the
    count is the only thing that separates the two. So this clause needs the
    probe as much as 3a does, and without one it is ``not_run``.
    """

    if driver is None:
        return _clause(
            "3b",
            "not_run",
            "no ingress driver was supplied, so the adapter could not be restarted "
            "with its own egress secret unset",
        )
    if probe is None:
        return _clause(
            "3b",
            "not_run",
            "no side effect probe was supplied, so an adapter that acts on an "
            "unauthenticated request and then returns a refusal could not be told "
            "from one that refuses it outright",
        )
    read = _probe_reader(probe, "3b")
    try:
        _bounded_call("the driver's restart()", lambda: driver.restart(egress_secret=None))
        try:
            baseline = read()
            with_secret = adapter.post_event(
                turn_status(adapter, conversation_id=new_conversation_id()),
                secret=adapter.secret,
            )
            without_secret = adapter.post_event(
                turn_status(adapter, conversation_id=new_conversation_id()), secret=None
            )
            served, exceeded = _side_effects_after(read, baseline, allowed=0)
        except AdapterUnreachableError:
            return _clause(
                "3b",
                "pass",
                "with its own egress secret unset the adapter stopped answering "
                "entirely, which refuses every request",
            )
    except _HarnessFault as fault:
        return _clause("3b", "fail", str(fault))
    finally:
        # Bounded too, and outside the fault handler above: a driver that cannot
        # put the secret back leaves every later rule checking a deaf adapter,
        # and it must not be able to hang the run while doing it.
        try:
            _bounded_call(
                "the driver's restart()",
                lambda: driver.restart(egress_secret=adapter.secret),
            )
        except _HarnessFault:
            pass
    problems: list[str] = []
    if _is_2xx(with_secret.status):
        problems.append(
            f"with its own secret unset the adapter still accepted a request with "
            f"{with_secret.status}"
        )
    if _is_2xx(without_secret.status):
        problems.append(
            f"with its own secret unset the adapter accepted an unauthenticated "
            f"request with {without_secret.status}"
        )
    if exceeded:
        problems.append(
            f"with its own secret unset the refused requests still moved the side "
            f"effect count by {served} within {_GRACE_S} seconds, so the adapter "
            "served them and answered a refusal afterwards"
        )
    return _verdict(
        "3b",
        problems,
        f"with its own secret unset the adapter refused both requests "
        f"({with_secret.status} and {without_secret.status}) and the side effect "
        f"count did not move in the {_GRACE_S} seconds after either",
    )


# --- rule 4: the acknowledgement shape ---------------------------------------


def _rule_4(adapter: AdapterUnderTest) -> FloorResult:
    return _result(4, "answer 2xx with a JSON body under the ack cap, and never redirect", [
        _clause_4(adapter),
    ])


def _clause_4(adapter: AdapterUnderTest) -> ClauseResult:
    try:
        response = adapter.post_event(
            turn_status(adapter, conversation_id=new_conversation_id()), secret=adapter.secret
        )
    except AdapterUnreachableError as error:
        return _clause("4", "fail", str(error))
    problems: list[str] = []
    if 300 <= response.status < 400:
        problems.append(
            f"answered {response.status}, a redirect, which the platform refuses to "
            "follow rather than replaying the egress credential at the named origin"
        )
    elif not _is_2xx(response.status):
        problems.append(f"answered {response.status}, which is a delivery failure")
    if response.oversize:
        problems.append(
            f"answered with more than {MAX_ACK_BODY_BYTES} bytes, which the worker "
            "refuses to buffer and treats as a failed delivery"
        )
    else:
        try:
            decoded = json.loads(response.body)
        except ValueError:
            problems.append(
                f"the acknowledgement body is not JSON ({len(response.body)} bytes)"
            )
        else:
            if not isinstance(decoded, dict):
                problems.append("the acknowledgement body is not a JSON object")
    return _verdict(
        "4",
        problems,
        f"answered {response.status} with a JSON object of {len(response.body)} bytes",
    )


# --- rule 5: handle all four events, tolerate the unused ones -----------------


def _rule_5(adapter: AdapterUnderTest) -> FloorResult:
    return _result(5, "handle all four reply events, including the ones it does not use", [
        _clause_5(adapter),
    ])


def _clause_5(adapter: AdapterUnderTest) -> ClauseResult:
    """Every member of the four member union, and only the four.

    The reply wire is a STRICT four member union, so a kit that also sent an
    unknown discriminator would be asserting a requirement the platform never
    made, and would teach authors to accept a shape the worker cannot send.

    Rule 5 is about EVENTS: handle all four, and tolerate the ones this adapter
    has no use for. It says nothing about unmodelled keys, and the kit does not
    get to add to it. Forward tolerance, at the event level or the field level,
    is a separate versioned compatibility rule with its own assertion, not
    something smuggled into the floor.
    """

    conversation = new_conversation_id()
    events = (
        turn_status(adapter, conversation_id=conversation),
        reply_update(adapter, conversation_id=conversation),
        reply_post(adapter, conversation_id=conversation),
        turn_completed(
            adapter, conversation_id=conversation, event_id=new_event_id()
        ),
    )
    problems: list[str] = []
    try:
        for event in events:
            response = adapter.post_event(event, secret=adapter.secret)
            if not _is_2xx(response.status):
                problems.append(f"answered {response.status} to {event.event}")
    except AdapterUnreachableError as error:
        return _clause("5", "fail", str(error))
    return _verdict(
        "5",
        problems,
        "all four reply events were accepted, including the ones this adapter "
        "has no use for",
    )


# --- rule 6: dedupe on event_id ----------------------------------------------


def _rule_6(adapter: AdapterUnderTest, probe: Callable[[], int] | None) -> FloorResult:
    return _result(6, "dedupe on turn.completed event_id, and tolerate a finished "
                      "conversation", [_clause_6(adapter, probe)])


def _clause_6(adapter: AdapterUnderTest, probe: Callable[[], int] | None) -> ClauseResult:
    """A duplicate completion is acked but answered ONCE, and a finished
    conversation still accepts a further completion.

    The first half is wire indistinguishable, which is the whole reason this
    needs a probe: an adapter that answers the correspondent twice returns 200
    to both posts, exactly like one that suppressed the duplicate.

    The second half is about the SAME conversation, and it has to be, or it is
    not the rule. The kit drives one conversation to a completed state through
    the ordinary path, then posts another completion for THAT conversation
    under a NEW event_id. Sending a fresh conversation instead would probe
    whether the adapter tolerates a completion it has never seen, which no floor
    rule asks for and which every adapter that fails this one still passes.

    A new event_id on a finished conversation is ordinary traffic rather than a
    redelivery: a conversation with more than one turn produces exactly this,
    and so does a sweeper draining a record after an outage. An adapter that
    treats its own retirement of a conversation as final breaks the platform
    here, and it answers the duplicate perfectly while doing it.
    """

    if probe is None:
        return _clause(
            "6",
            "not_run",
            "no side effect probe was supplied, and both acks are 2xx whether or not "
            "the duplicate was suppressed",
        )
    conversation = new_conversation_id()
    read = _probe_reader(probe, "6")
    completed = turn_completed(
        adapter, conversation_id=conversation, event_id=new_event_id()
    )
    try:
        before = read()
        first = adapter.post_event(completed, secret=adapter.secret)
        second = adapter.post_event(completed, secret=adapter.secret)
        # Watched, never sampled. An adapter that answers the redelivery 200 and
        # hands the second correspondent effect to a queue looks identical to a
        # deduping one at the instant the ack arrives, and the count it moves is
        # the only evidence there is that it answered the correspondent twice.
        moved, exceeded = _side_effects_after(read, before, allowed=1)
        # The same conversation, which the two posts above have now finished,
        # receiving a further completion under an event_id it has never seen.
        # Sent after the window closes, so its own effect is never counted here.
        finished = adapter.post_event(
            turn_completed(
                adapter, conversation_id=conversation, event_id=new_event_id()
            ),
            secret=adapter.secret,
        )
    except AdapterUnreachableError as error:
        return _clause("6", "fail", str(error))
    except _HarnessFault as fault:
        return _clause("6", "fail", str(fault))
    problems: list[str] = []
    if not _is_2xx(first.status):
        problems.append(f"answered {first.status} to a turn.completed")
    if not _is_2xx(second.status):
        problems.append(f"answered {second.status} to a redelivered turn.completed")
    if exceeded:
        problems.append(
            f"the duplicate event_id moved the side effect count by {moved} within "
            f"{_GRACE_S} seconds of the redelivery, so the correspondent was "
            "answered twice"
        )
    if not _is_2xx(finished.status):
        problems.append(
            f"answered {finished.status} to a further turn.completed for a "
            "conversation it had already finished, so a later turn on a retired "
            "conversation, and a sweeper draining a record after an outage, both "
            "break against this adapter"
        )
    return _verdict(
        "6",
        problems,
        f"a duplicate event_id moved the side effect count by {moved} over the "
        f"{_GRACE_S} seconds after it, and a further completion for the conversation "
        "it had just finished was accepted",
    )


# --- rules 1, 2 and 7: the ingress half --------------------------------------


def _ingress_rules_not_run() -> list[FloorResult]:
    """No driver means no evidence, and no evidence is never a pass."""

    reason = (
        "no ingress driver was supplied, so nothing could point the adapter at a "
        "recording ingress or inject an upstream message"
    )
    return [
        _result(1, _RULE_1_TITLE, [_clause("1", "not_run", reason)]),
        _result(2, _RULE_2_TITLE, [_clause("2", "not_run", reason)]),
        _result(
            7,
            _RULE_7_TITLE,
            [
                _clause("7a", "not_run", reason),
                _clause("7b", "not_run", reason),
                _manual_clause("7c", _MANUAL_7C_DETAIL),
            ],
        ),
    ]


_RULE_1_TITLE = "send a delivery_id that is stable across a retried transport failure"
_RULE_2_TITLE = "treat any response from ingress as final, including 202"
_RULE_7_TITLE = "survive a stale ingress credential and resume on a replacement"
_MANUAL_7C_DETAIL = (
    "a loud stale credential signal is not observable over the wire; see "
    "manual_review_required"
)


def _turn_posts(ingress: FakeIngress, since: int) -> list[ObservedRequest]:
    return [record for record in ingress.records()[since:] if record.path == TURNS_PATH]


def _framing_failure(
    ingress: FakeIngress, since: int, clause_id: str
) -> ClauseResult | None:
    """A failure for any request in this window the ingress could not read.

    Every ingress rule decides on which ``ObservedRequest`` values landed in its
    window, and rule 2 and clause 7b decide on which ones did NOT. The adapter
    under test writes its own request headers, so a request the ingress cannot
    frame is a request the kit cannot attribute, and reading an unattributable
    request as an absent one hands the adapter a switch over its own verdict.
    Incomplete evidence is a failure of the rule whose window it landed in, the
    same way missing evidence is never a pass anywhere else in this kit.
    """

    broken = [
        record for record in ingress.records()[since:] if record.framing_error is not None
    ]
    if not broken:
        return None
    reasons = sorted({str(record.framing_error) for record in broken})
    return _clause(
        clause_id,
        "fail",
        f"the adapter sent {len(broken)} request(s) the ingress could not read "
        f"({'; '.join(reasons)}), so those posts could not be attributed and this "
        "rule would be deciding on evidence the adapter chose",
    )


def _posts_after(
    ingress: FakeIngress, since: int, delivery_id: str
) -> list[ObservedRequest]:
    """Any post of this identity that arrives during the grace window.

    The counterpart of ``_side_effects_after`` on the ingress side, and the same
    shape for the same reason: it looks for activity that should not exist
    rather than concluding from quiet that none ever will. Exits early on the
    first offender.
    """

    deadline = time.monotonic() + _GRACE_S
    while True:
        late = [
            post
            for post in _turn_posts(ingress, since)
            if post.delivery_id == delivery_id
        ]
        if late or time.monotonic() >= deadline:
            return late
        time.sleep(_POLL_S)


def _wait_for(predicate: Callable[[], bool], *, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(_POLL_S)
    return False


def _unmatched(declared: str, posts: list[ObservedRequest]) -> str:
    observed = sorted({post.delivery_id or "<absent>" for post in posts})
    return (
        f"no observed delivery matched the declared upstream identity {declared}; "
        f"the wire carried {observed}. Either the adapter derives its delivery_id "
        "differently or the driver's reserve does not return the id the adapter "
        "will send, and both are findings"
    )


def _rule_1(driver: IngressDriver, ingress: FakeIngress) -> FloorResult:
    """After a transport failure, the SAME delivery_id arrives.

    The failure is provoked at the KIT'S OWN ingress, which reads the request,
    records it with the delivery id that was on it, and closes the connection
    with nothing on it. That is a transport failure rather than a response, so
    it is the only kind of failure rule 1 licenses a retry for, and it is the
    only arrangement under which the kit can say the DECLARED delivery is the
    one that met the outage. An outage arranged privately by the driver leaves
    the kit sleeping for a fixed window instead, and an adapter whose first
    attempt lands after that window is certified for a retry that never
    happened, off a single successful post.

    So the kit waits for the failed attempt rather than timing it, then heals
    the transport and requires the SAME id to come back. A delivery_id minted
    per attempt answers the correspondent once per retry, because the platform's
    claim converges on the id and on nothing else.
    """

    identity: UpstreamIdentity = _bounded_call("the driver's reserve()", driver.reserve)
    ingress.arm_blackhole()
    mark = len(ingress.records())
    try:
        _bounded_call("the driver's release()", lambda: driver.release(identity))
        attempted = _wait_for(
            lambda: any(
                post.status == NO_RESPONSE and post.delivery_id == identity.delivery_id
                for post in _turn_posts(ingress, mark)
            ),
            timeout_s=_ARRIVAL_TIMEOUT_S,
        )
    finally:
        ingress.disarm_blackhole()
    if not attempted:
        seen = sorted(
            {post.delivery_id or "<absent>" for post in _turn_posts(ingress, mark)}
        )
        return _result(1, _RULE_1_TITLE, [
            _clause(
                "1",
                "fail",
                f"the declared delivery {identity.delivery_id} never reached the "
                "unavailable transport, so no failure occurred and there is no retry "
                f"to judge; the wire carried {seen}",
            )
        ])
    arrived = _wait_for(
        lambda: any(_is_2xx(post.status) for post in _turn_posts(ingress, mark)),
        timeout_s=_ARRIVAL_TIMEOUT_S,
    )
    posts = [post for post in _turn_posts(ingress, mark) if post.status != NO_RESPONSE]
    framing = _framing_failure(ingress, mark, "1")
    if framing is not None:
        return _result(1, _RULE_1_TITLE, [framing])
    if not arrived:
        return _result(1, _RULE_1_TITLE, [
            _clause(
                "1",
                "fail",
                f"the declared delivery {identity.delivery_id} met the unavailable "
                "transport and nothing arrived once it was restored, so the adapter "
                "did not retry a transport failure",
            )
        ])
    if not any(post.delivery_id == identity.delivery_id for post in posts):
        return _result(
            1, _RULE_1_TITLE, [_clause("1", "fail", _unmatched(identity.delivery_id, posts))]
        )
    return _result(1, _RULE_1_TITLE, [
        _clause(
            "1",
            "pass",
            f"after a transport failure the adapter delivered {identity.delivery_id}, "
            "byte identical to the identity it declared",
        )
    ])


def _rule_2(driver: IngressDriver, ingress: FakeIngress) -> FloorResult:
    """A 202 is a response, so it is final.

    202 reads like an invitation to come back and it is not: another request
    holds the claim, and the answer is already decided. The false positive
    control is what makes the rule usable, because an upstream redelivery under
    an identity the kit never declared is legitimate at least once behavior, so
    the assertion is about THIS delivery id and not about post counts.

    Two properties carry the rule, and neither is a timer.

    * The 202 is armed for the DECLARED identity and no other, which is why
      injection is two phase. A global one shot is consumed by whichever
      delivery reaches the ingress first, and the rule then passes on a declared
      delivery that only ever saw a 200, having tested finality against nothing.
    * The verdict is taken once the driver reports the delivery RETIRED, never
      after a fixed settle. A fixed window is a check an adapter evades by
      retrying more slowly than the window is wide, and no adapter has to be
      hostile to do it. A driver that never retires the identity leaves the rule
      with no finality evidence, and no evidence is never a pass.
    """

    identity: UpstreamIdentity = _bounded_call("the driver's reserve()", driver.reserve)
    ingress.arm_202(identity.delivery_id)
    mark = len(ingress.records())
    _bounded_call("the driver's release()", lambda: driver.release(identity))
    # Waited for on the DECLARED identity, never on "some post arrived": an
    # adapter with another delivery already in flight satisfies the weaker
    # predicate with the wrong message, and the rule would then decide the
    # declared one absent before it had a chance to be sent.
    arrived = _wait_for(
        lambda: any(
            post.delivery_id == identity.delivery_id
            for post in _turn_posts(ingress, mark)
        ),
        timeout_s=_ARRIVAL_TIMEOUT_S,
    )
    if not arrived:
        posts = _turn_posts(ingress, mark)
        detail = (
            f"no delivery arrived for the declared identity {identity.delivery_id}"
            if not posts
            else _unmatched(identity.delivery_id, posts)
        )
        return _result(2, _RULE_2_TITLE, [_clause("2", "fail", detail)])
    retired = _wait_for(
        lambda: bool(_bounded_call("the driver's settled()", lambda: driver.settled(identity))),
        timeout_s=_QUIESCENCE_TIMEOUT_S,
    )
    if not retired:
        return _result(2, _RULE_2_TITLE, [
            _clause(
                "2",
                "fail",
                f"the driver never reported {identity.delivery_id} retired within "
                f"{_QUIESCENCE_TIMEOUT_S} seconds, so the kit cannot say the adapter "
                "has stopped attempting it and there is no finality to judge",
            )
        ])
    # The claim is on the record now, and everything after it is evidence ABOUT
    # the claim rather than an inference from silence. A driver that reports an
    # identity retired while its adapter still has an attempt scheduled is the
    # naive implementation this kit exists to catch, and it used to buy a pass.
    claim_mark = len(ingress.records())
    late = _posts_after(ingress, claim_mark, identity.delivery_id)
    if late:
        return _result(2, _RULE_2_TITLE, [
            _clause(
                "2",
                "fail",
                f"the driver reported {identity.delivery_id} retired and the adapter "
                f"posted it again {len(late)} time(s) within {_GRACE_S} seconds of "
                "that claim. Both halves of the contract broke here: the adapter "
                "treated a response as a retry signal, and the driver's settled() "
                "declared quiescence while an attempt was still scheduled, so the "
                "harness needs fixing as well as the adapter",
            )
        ])
    framing = _framing_failure(ingress, mark, "2")
    if framing is not None:
        # The rule's pass branch is a statement about a post the ingress did NOT
        # see, so an unreadable post inside the window is exactly what would
        # make that statement false while leaving it true on the records.
        return _result(2, _RULE_2_TITLE, [framing])
    matching = [
        post
        for post in _turn_posts(ingress, mark)
        if post.delivery_id == identity.delivery_id
    ]
    still_armed = ingress.armed_202()
    if still_armed is not None:
        return _result(2, _RULE_2_TITLE, [
            _clause(
                "2",
                "fail",
                f"the ingress was armed to answer 202 for {still_armed} and that "
                "delivery never arrived to receive it, so the adapter's handling of "
                "an in flight claim was never exercised",
            )
        ])
    if len(matching) > 1:
        return _result(2, _RULE_2_TITLE, [
            _clause(
                "2",
                "fail",
                f"ingress answered {matching[0].status} and the adapter posted "
                f"{identity.delivery_id} {len(matching)} times, so it treated a "
                "response as a retry signal",
            )
        ])
    return _result(2, _RULE_2_TITLE, [
        _clause(
            "2",
            "pass",
            f"ingress answered {matching[0].status} to {identity.delivery_id}, the "
            f"driver reported it retired, and no further post for it arrived in the "
            f"{_GRACE_S} seconds after that claim",
        )
    ])


def _rule_7(
    adapter: AdapterUnderTest, driver: IngressDriver, ingress: FakeIngress
) -> FloorResult:
    """A 401 from ingress is not fatal, and the adapter never mints.

    The adapter holds no platform key, so re minting is the trust boundary
    breach and not the fix: an adapter that could mint would defeat both the
    token TTL and the binding generation, which are the only two things standing
    in for a revocation list. Conformant behavior is to hold the delivery, stay
    alive, and deliver it once an operator supplies a replacement token.

    That the resumed delivery carries a BYTE IDENTICAL delivery id is rule 1's
    clause, not this one. An adapter that renames a delivery between attempts is
    a rule 1 defect, and deciding it a second time here would report one break
    twice while leaving this rule undecidable for every adapter whose restart
    path attempts once before its ingress url is configured.
    """

    ingress.arm_401()
    identity: UpstreamIdentity = _bounded_call("the driver's reserve()", driver.reserve)
    mark = len(ingress.records())
    _bounded_call("the driver's release()", lambda: driver.release(identity))
    refused = _wait_for(
        lambda: any(
            post.status == 401 and post.delivery_id == identity.delivery_id
            for post in _turn_posts(ingress, mark)
        ),
        timeout_s=_ARRIVAL_TIMEOUT_S,
    )
    clause_7a = _clause_7a(adapter, driver, ingress, identity.delivery_id, mark, refused)
    # Computed after 7a, because the whole 401 window is what 7a spans, and the
    # unreadable request is the more fundamental finding than whatever 7a
    # concluded on the records it could read.
    framing = _framing_failure(ingress, mark, "7a")
    if framing is not None:
        clause_7a = framing
    minted = ingress.mint_attempts()
    mint_problems: list[str] = []
    if minted:
        mint_problems.append(
            f"the adapter tried {minted} time(s) to mint its own replacement token, "
            "which the platform refuses and which would defeat both the token TTL "
            "and the binding generation"
        )
    clause_7b = _verdict(
        "7b", mint_problems, "the adapter never tried to mint its own replacement token"
    )
    return _result(
        7, _RULE_7_TITLE, [clause_7a, clause_7b, _manual_clause("7c", _MANUAL_7C_DETAIL)]
    )


def _clause_7a(
    adapter: AdapterUnderTest,
    driver: IngressDriver,
    ingress: FakeIngress,
    declared: str,
    mark: int,
    refused: bool,
) -> ClauseResult:
    if not refused:
        return _clause(
            "7a",
            "fail",
            f"no delivery reached the ingress to be refused; the declared identity "
            f"was {declared}",
        )
    if not any(post.delivery_id == declared for post in _turn_posts(ingress, mark)):
        return _clause("7a", "fail", _unmatched(declared, _turn_posts(ingress, mark)))
    time.sleep(_LIVENESS_SETTLE_S)
    if not _endpoint_answers(adapter):
        return _clause(
            "7a",
            "fail",
            "the egress endpoint stopped answering after the 401, so the adapter "
            "treated a stale ingress credential as fatal",
        )
    resume_mark = len(ingress.records())
    _bounded_call("the driver's restart()", lambda: driver.restart(token=ingress.token))
    # The resumed post has to carry the DECLARED identity. Any later 2xx would
    # also be satisfied by an adapter that discarded the held delivery and then
    # posted something else entirely, and the clause would then report that the
    # held delivery resumed when it did not.
    resumed = _wait_for(
        lambda: any(
            _is_2xx(post.status) and post.delivery_id == declared
            for post in _turn_posts(ingress, resume_mark)
        ),
        timeout_s=_RESUME_TIMEOUT_S,
    )
    if not resumed:
        after_restart = sorted(
            {
                f"{post.delivery_id or '<absent>'}:{post.status}"
                for post in _turn_posts(ingress, resume_mark)
            }
        )
        return _clause(
            "7a",
            "fail",
            f"the adapter survived the 401 but never delivered {declared} again after "
            "an operator supplied a replacement token, so the in flight delivery was "
            f"discarded; after the restart the wire carried {after_restart}",
        )
    return _clause(
        "7a",
        "pass",
        f"the adapter held {declared} through the 401, stayed alive, and delivered it "
        "once a replacement token was supplied",
    )


def _endpoint_answers(adapter: AdapterUnderTest) -> bool:
    """Whether the egress endpoint is still serving. Any status counts.

    A refusal is still an answer: the question here is whether the process
    survived, not whether it liked the request.
    """

    try:
        adapter.post_event(
            turn_status(adapter, conversation_id=new_conversation_id()),
            secret=adapter.secret,
        )
    except AdapterUnreachableError:
        return False
    return True
