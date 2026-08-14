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
import time
from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel, ConfigDict

from .driver import IngressDriver
from .ingress import TURNS_PATH, FakeIngress, ObservedRequest
from .transport import (
    MAX_ACK_BODY_BYTES,
    WRONG_SECRET,
    AdapterUnderTest,
    AdapterUnreachableError,
    body_with_extra_key,
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

# How long a controlled transport outage lasts before the kit heals it. Long
# enough that the adapter's first attempt has certainly failed, short enough
# that a run does not become a sleep.
_OUTAGE_S = 0.2

# How long the kit waits for an adapter to reach the ingress at all.
_ARRIVAL_TIMEOUT_S = 15.0

# How long rule 2 watches for a SECOND post after ingress answered the first.
# A response is final, so any further attempt for that delivery is the defect.
_FINALITY_SETTLE_S = 0.5

# How long the kit lets a 401 settle before asking whether the adapter is still
# serving. An adapter that treats a stale credential as fatal takes a moment to
# finish dying, and probing into that window would read a corpse as alive.
_LIVENESS_SETTLE_S = 1.2

# How long the kit waits for a held delivery to resume after an operator
# supplied replacement token.
_RESUME_TIMEOUT_S = 8.0

_POLL_S = 0.02

_EXTRA_KEY = "conformance_probe_unmodelled_key"


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
            driver.start(ingress_url=ingress.url, token=ingress.token)
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
            rule_2 = _rule_2(driver, ingress)
            rule_1 = _rule_1(driver, ingress)
            rule_7 = _rule_7(adapter, driver, ingress)
            results.extend([rule_1, rule_2, rule_7])
    finally:
        if driver is not None:
            driver.stop()
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
            _clause_3b(adapter, driver),
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
    """

    if probe is None:
        return _clause(
            "3a",
            "not_run",
            "no side effect probe was supplied, so an adapter that performs the side "
            "effect and then returns 401 could not be told from one that refuses first",
        )
    conversation = new_conversation_id()
    try:
        accepted = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=adapter.secret
        )
        if accepted.status in (401, 403):
            return _clause(
                "3a",
                "fail",
                f"the adapter answered {accepted.status} to its own configured secret, "
                "so it refuses the platform rather than verifying it",
            )
        before = probe()
        wrong = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=WRONG_SECRET
        )
        absent = adapter.post_event(
            turn_status(adapter, conversation_id=conversation), secret=None
        )
        after = probe()
    except AdapterUnreachableError as error:
        return _clause("3a", "fail", str(error))
    problems: list[str] = []
    if _is_2xx(wrong.status):
        problems.append(f"a wrong secret was accepted with {wrong.status}")
    if _is_2xx(absent.status):
        problems.append(f"an absent secret was accepted with {absent.status}")
    if after != before:
        problems.append(
            f"the refused requests moved the side effect count from {before} to {after}, "
            "so the side effect happened before the rejection"
        )
    return _verdict(
        "3a",
        problems,
        f"a wrong secret answered {wrong.status}, an absent one answered "
        f"{absent.status}, and neither moved the side effect count",
    )


def _clause_3b(adapter: AdapterUnderTest, driver: IngressDriver | None) -> ClauseResult:
    """An adapter whose OWN egress secret is unset refuses everything.

    Serving unauthenticated is worse than not serving: anyone who can reach the
    endpoint could forge a completion. Restoring the real secret afterwards is
    part of the clause, or every rule that runs later is checking a deaf adapter.
    """

    if driver is None:
        return _clause(
            "3b",
            "not_run",
            "no ingress driver was supplied, so the adapter could not be restarted "
            "with its own egress secret unset",
        )
    try:
        driver.restart(egress_secret=None)
        try:
            with_secret = adapter.post_event(
                turn_status(adapter, conversation_id=new_conversation_id()),
                secret=adapter.secret,
            )
            without_secret = adapter.post_event(
                turn_status(adapter, conversation_id=new_conversation_id()), secret=None
            )
        except AdapterUnreachableError:
            return _clause(
                "3b",
                "pass",
                "with its own egress secret unset the adapter stopped answering "
                "entirely, which refuses every request",
            )
    finally:
        driver.restart(egress_secret=adapter.secret)
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
    return _verdict(
        "3b",
        problems,
        f"with its own secret unset the adapter refused both requests "
        f"({with_secret.status} and {without_secret.status})",
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
    """Every member of the four member union, plus one unmodelled extra key.

    Only the four. The reply wire is a STRICT four member union, so a kit that
    also sent an unknown discriminator would be asserting a requirement the
    platform never made, and would teach authors to accept a shape the worker
    cannot send. Forward event tolerance is a separate compatibility rule, not
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
        tolerant = adapter.post_body(
            body_with_extra_key(
                reply_update(adapter, conversation_id=conversation),
                key=_EXTRA_KEY,
                value="a key a later wire revision could add",
            ),
            secret=adapter.secret,
        )
    except AdapterUnreachableError as error:
        return _clause("5", "fail", str(error))
    if not _is_2xx(tolerant.status):
        problems.append(
            f"answered {tolerant.status} to a reply.update carrying one unmodelled "
            "key, so a later optional field would break it"
        )
    return _verdict(
        "5",
        problems,
        "all four events and an unmodelled extra key were accepted",
    )


# --- rule 6: dedupe on event_id ----------------------------------------------


def _rule_6(adapter: AdapterUnderTest, probe: Callable[[], int] | None) -> FloorResult:
    return _result(6, "dedupe on turn.completed event_id, and tolerate a finished "
                      "conversation", [_clause_6(adapter, probe)])


def _clause_6(adapter: AdapterUnderTest, probe: Callable[[], int] | None) -> ClauseResult:
    """A duplicate completion is acked but answered ONCE.

    Wire indistinguishable, which is the whole reason this needs a probe: an
    adapter that answers the correspondent twice returns 200 to both posts,
    exactly like one that suppressed the duplicate.
    """

    if probe is None:
        return _clause(
            "6",
            "not_run",
            "no side effect probe was supplied, and both acks are 2xx whether or not "
            "the duplicate was suppressed",
        )
    completed = turn_completed(
        adapter, conversation_id=new_conversation_id(), event_id=new_event_id()
    )
    try:
        before = probe()
        first = adapter.post_event(completed, secret=adapter.secret)
        second = adapter.post_event(completed, secret=adapter.secret)
        after = probe()
        finished = adapter.post_event(
            turn_completed(
                adapter, conversation_id=new_conversation_id(), event_id=new_event_id()
            ),
            secret=adapter.secret,
        )
    except AdapterUnreachableError as error:
        return _clause("6", "fail", str(error))
    problems: list[str] = []
    if not _is_2xx(first.status):
        problems.append(f"answered {first.status} to a turn.completed")
    if not _is_2xx(second.status):
        problems.append(f"answered {second.status} to a redelivered turn.completed")
    if after - before > 1:
        problems.append(
            f"the duplicate event_id moved the side effect count by {after - before}, "
            "so the correspondent was answered twice"
        )
    if not _is_2xx(finished.status):
        problems.append(
            f"answered {finished.status} to a turn.completed for a conversation it "
            "has never seen"
        )
    return _verdict(
        "6",
        problems,
        f"a duplicate event_id moved the side effect count by {after - before}",
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
        "differently or the driver's stimulate does not return the id the adapter "
        "will send, and both are findings"
    )


def _rule_1(driver: IngressDriver, ingress: FakeIngress) -> FloorResult:
    """After a transport failure, the SAME delivery_id arrives.

    The kit takes the transport down itself, so it knows a first attempt failed
    without needing to observe it: the failure happened below the ingress and
    was never answered, which is the only kind of failure rule 1 licenses a
    retry for. What has to be observed is the id the retry carried, and it has
    to be byte identical to the one the driver declared. A delivery_id minted
    per attempt answers the correspondent once per retry, because the platform's
    claim converges on the id and on nothing else.
    """

    driver.set_transport(reachable=False)
    mark = len(ingress.records())
    identity = driver.stimulate()
    time.sleep(_OUTAGE_S)
    driver.set_transport(reachable=True)
    arrived = _wait_for(
        lambda: any(_is_2xx(post.status) for post in _turn_posts(ingress, mark)),
        timeout_s=_ARRIVAL_TIMEOUT_S,
    )
    posts = _turn_posts(ingress, mark)
    if not arrived:
        return _result(1, _RULE_1_TITLE, [
            _clause(
                "1",
                "fail",
                f"no delivery arrived after the transport was restored, so the "
                f"adapter did not retry a transport failure; the declared identity "
                f"was {identity.delivery_id}",
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
    """

    ingress.arm_202()
    mark = len(ingress.records())
    identity = driver.stimulate()
    arrived = _wait_for(
        lambda: bool(_turn_posts(ingress, mark)), timeout_s=_ARRIVAL_TIMEOUT_S
    )
    if not arrived:
        return _result(2, _RULE_2_TITLE, [
            _clause(
                "2",
                "fail",
                f"no delivery arrived for the declared identity {identity.delivery_id}",
            )
        ])
    if not any(
        post.delivery_id == identity.delivery_id for post in _turn_posts(ingress, mark)
    ):
        return _result(2, _RULE_2_TITLE, [
            _clause("2", "fail", _unmatched(identity.delivery_id, _turn_posts(ingress, mark)))
        ])
    time.sleep(_FINALITY_SETTLE_S)
    matching = [
        post
        for post in _turn_posts(ingress, mark)
        if post.delivery_id == identity.delivery_id
    ]
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
            f"ingress answered {matching[0].status} and the adapter did not post "
            f"{identity.delivery_id} again",
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
    mark = len(ingress.records())
    identity = driver.stimulate()
    refused = _wait_for(
        lambda: any(post.status == 401 for post in _turn_posts(ingress, mark)),
        timeout_s=_ARRIVAL_TIMEOUT_S,
    )
    clause_7a = _clause_7a(adapter, driver, ingress, identity.delivery_id, mark, refused)
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
    driver.restart(token=ingress.token)
    resumed = _wait_for(
        lambda: any(_is_2xx(post.status) for post in _turn_posts(ingress, resume_mark)),
        timeout_s=_RESUME_TIMEOUT_S,
    )
    if not resumed:
        return _clause(
            "7a",
            "fail",
            f"the adapter survived the 401 but never delivered {declared} again after "
            "an operator supplied a replacement token, so the in flight delivery was "
            "discarded",
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
