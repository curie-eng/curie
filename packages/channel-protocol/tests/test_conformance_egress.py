"""The conformance kit's egress half, driven over real loopback HTTP (#1516).

Every test here stands up a REAL stub adapter and points the REAL kit at it.
Nothing internal is mocked, because a black box kit that is exercised through an
in process shortcut proves nothing about the black box.

The falsifiability controls are the point of the file: for every automatable
clause there is a ``test_non_conformant_stub_fails_...`` case, so deleting the
kit's assertion for that clause turns its case green and reds the suite.
"""

from __future__ import annotations

import ast
import itertools
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from channel_protocol.conformance import (
    ADAPTER_SECRET_HEADER,
    MAX_ACK_BODY_BYTES,
    MAX_TURN_BODY_BYTES,
    AdapterUnderTest,
    FloorReport,
    run_floor,
    side_effect_probe,
)
from conformance_stubs import (
    SECRET_HEADER,
    StubAdapter,
    StubIngressDriver,
    clause,
    clause_status,
    clauses,
    conformant_stub,
    non_conformant_stub,
    random_secret,
    read_probe,
    rule_status,
    side_effect_probe_for,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_REPLY_SINK = _REPO_ROOT / "apps" / "worker" / "src" / "curie_worker" / "reply_sink.py"
_API_CONFIG = _REPO_ROOT / "apps" / "api" / "src" / "curie_api" / "config.py"

_FOUR_EVENTS = {"turn.status", "reply.update", "reply.post", "turn.completed"}


@contextmanager
def _running(stub: StubAdapter) -> Iterator[StubAdapter]:
    """Start a stub and tear it down, pass or fail.

    A leaked listener survives the test that made it and breaks the next run,
    so every server this file starts is closed here.
    """

    stub.start()
    try:
        yield stub
    finally:
        stub.stop()


def _adapter_for(stub: StubAdapter, secret: str) -> AdapterUnderTest:
    return AdapterUnderTest(
        endpoint=stub.endpoint,
        secret=secret,
        kind="email",
        address="agent@example.test",
        timeout_s=15.0,
    )


@pytest.fixture
def secret() -> str:
    return random_secret()


@pytest.fixture
def conformant(tmp_path: Path, secret: str) -> Iterator[StubAdapter]:
    with _running(
        conformant_stub(secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        yield stub


def _report_for(
    stub: StubAdapter,
    secret: str,
    *,
    with_probe: bool = True,
    with_driver: bool = False,
) -> FloorReport:
    return run_floor(
        _adapter_for(stub, secret),
        driver=StubIngressDriver(stub) if with_driver else None,
        side_effect_probe=side_effect_probe_for(stub.base_url) if with_probe else None,
    )


def _literal(node: ast.expr) -> Any:
    """Evaluate the constant expressions the worker's constants are written as."""

    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult):
        return _literal(node.left) * _literal(node.right)
    raise AssertionError(f"{ast.unparse(node)} is not a constant expression")


def _source_constant(path: Path, name: str) -> Any:
    """One constant, read out of a first party module's source.

    Read rather than imported: ``channel_protocol`` must never depend on
    ``curie_worker`` or ``curie_api``, so the kit re declares these values and
    this is the gate that keeps the copies honest. Both a bare assignment and an
    annotated settings field are accepted, because the two authoritative sites
    write theirs differently.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return _literal(node.value)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.value is not None
        ):
            return _literal(node.value)
    raise AssertionError(f"{name} is not assigned in {path}")


def test_platform_constants_match() -> None:
    """The kit's copies of the platform's wire constants cannot drift.

    Change any of them at its authoritative site and this reds, which is the
    only thing standing between a re declared constant and a kit that sends the
    wrong header, asserts the wrong ack cap, or stands in for the ingress with a
    body bound the real route does not have.
    """

    assert ADAPTER_SECRET_HEADER == _source_constant(_REPLY_SINK, "ADAPTER_SECRET_HEADER")
    assert MAX_ACK_BODY_BYTES == _source_constant(_REPLY_SINK, "MAX_ACK_BODY_BYTES")
    assert MAX_TURN_BODY_BYTES == _source_constant(
        _API_CONFIG, "channel_turn_max_body_bytes"
    )
    # The stub reads the header name from the guide, independently of both.
    assert ADAPTER_SECRET_HEADER == SECRET_HEADER


def test_conformant_stub_passes_and_still_lists_manual_review(
    conformant: StubAdapter, secret: str
) -> None:
    """A correct adapter CAN pass, and the pass is never quotable on its own.

    Both halves are asserted together because the pair IS the contract: a
    verdict printed without the manual review list is the failure mode this
    test exists to prevent.
    """

    report = _report_for(conformant, secret, with_driver=True)

    assert report.automated_floor == "pass", report.detail()
    assert report.mode == "strict"
    for rule_number in (1, 2, 4, 5, 6, 7):
        assert rule_status(report, rule_number) == "pass", report.detail()
    assert clause_status(report, "3a") == "pass", report.detail()
    assert clause_status(report, "3b") == "pass", report.detail()

    reviewed = {item.clause for item in report.manual_review_required}
    assert "3c" in reviewed, report.detail()
    item = next(i for i in report.manual_review_required if i.clause == "3c")
    assert item.reason.strip(), "a manual review item with no reason is a footnote"
    assert item.how_to_review.strip(), "a human needs to be told what to read"


def test_clause_3c_is_never_automatable_and_never_reads_as_pass(
    conformant: StubAdapter, secret: str
) -> None:
    """Constant time comparison is not black box observable, so it is outside
    the verdict domain and an HTTP status is never allowed to imply it."""

    report = _report_for(conformant, secret, with_driver=True)

    constant_time = clause(report, "3c")
    assert constant_time.automatable is False
    assert constant_time.status != "pass"
    assert not any(
        candidate.automatable and candidate.clause == "3c" for candidate in clauses(report)
    )


def test_the_report_carries_no_field_named_conformant(
    conformant: StubAdapter, secret: str
) -> None:
    """A boolean literally named ``conformant`` is quotable as whole floor
    conformance while clause 3c was never asserted. The name was the hole."""

    report = _report_for(conformant, secret, with_driver=True)

    assert "conformant" not in FloorReport.model_fields
    assert _keys_of(report.model_dump(mode="json")).isdisjoint({"conformant"})


def _keys_of(payload: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            found.add(str(key))
            found |= _keys_of(value)
    elif isinstance(payload, list):
        for item in payload:
            found |= _keys_of(item)
    return found


def test_counts_cover_every_clause(conformant: StubAdapter, secret: str) -> None:
    """``counts`` is the verdict's arithmetic, so it has to be over the same
    population the verdict is computed on."""

    report = _report_for(conformant, secret, with_driver=True)

    assert set(report.counts) == {"pass", "fail", "not_run"}
    assert sum(report.counts.values()) == len(list(clauses(report)))
    for status in ("pass", "fail", "not_run"):
        assert report.counts[status] == sum(
            1 for candidate in clauses(report) if candidate.status == status
        )


def test_non_conformant_stub_fails_rule_3_clause_3a_when_it_skips_the_secret_check(
    tmp_path: Path, secret: str
) -> None:
    with _running(
        non_conformant_stub(
            "skips_secret_check", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert clause_status(report, "3a") == "fail", report.detail()
        assert report.automated_floor == "fail"


def test_non_conformant_stub_fails_rule_3_clause_3a_when_it_performs_the_side_effect_first(
    tmp_path: Path, secret: str
) -> None:
    """The status is a conformant 401 and the adapter is still broken.

    This is the exact hole a status only check leaves open, which is why clause
    3a needs a side effect probe rather than a response code.
    """

    with _running(
        non_conformant_stub(
            "side_effect_then_401", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert clause_status(report, "3a") == "fail", report.detail()
        # The probe is what saw it: the rejected requests moved the counter.
        assert read_probe(stub.base_url)["side_effects"] > 0


def test_non_conformant_stub_fails_clause_3a_when_the_side_effect_lands_after_the_401(
    tmp_path: Path, secret: str
) -> None:
    """The refusal and the side effect are two different events.

    This adapter answers a conformant 401 and hands the work to a timer that
    fires 100 ms later, which is what an adapter that queues its work actually
    looks like. A count read the instant the refusal arrives is identical to a
    correct adapter's, so the clause has to read the count once it has stopped
    moving rather than the moment the response lands.
    """

    with _running(
        non_conformant_stub(
            "delayed_side_effect_then_401",
            secret=secret,
            state_path=tmp_path / "state.json",
        )
    ) as stub:
        report = _report_for(stub, secret)

        assert clause_status(report, "3a") == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()
        assert read_probe(stub.base_url)["side_effects"] > 0


def test_non_conformant_stub_fails_clause_3b_when_it_serves_with_its_secret_unset(
    tmp_path: Path, secret: str
) -> None:
    """With its own secret unset, refusing is about the SIDE EFFECT too.

    This adapter answers a clean 503 to every request and answers the
    correspondent anyway. Its statuses are indistinguishable from a conformant
    adapter's, so a clause that inspects only the response certifies an adapter
    that serves unauthenticated requests, which is the exact thing 3b exists to
    forbid.
    """

    with _running(
        non_conformant_stub(
            "side_effect_when_secret_unset",
            secret=secret,
            state_path=tmp_path / "state.json",
        )
    ) as stub:
        report = _report_for(stub, secret, with_driver=True)

        assert clause_status(report, "3b") == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()


def test_clauses_3a_and_3b_catch_a_side_effect_delayed_past_any_quiet_window(
    tmp_path: Path, secret: str
) -> None:
    """A quiet window is not evidence, and 1.2 seconds is not adversarial.

    Both breaks answer a clean refusal and hand the correspondent to a timer
    that fires well after any interval a kit could call quiet. Inferring
    finality from silence passes both of them; observing for a named window and
    failing on ANY movement catches both, and the two clauses share the one
    mechanism rather than each carrying its own constant to outlast.
    """

    with _running(
        non_conformant_stub(
            "slowly_delayed_side_effect_then_401",
            secret=secret,
            state_path=tmp_path / "3a.json",
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert clause_status(report, "3a") == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()

    with _running(
        non_conformant_stub(
            "slowly_serves_when_secret_unset",
            secret=secret,
            state_path=tmp_path / "3b.json",
        )
    ) as stub:
        report = _report_for(stub, secret, with_driver=True)
        assert clause_status(report, "3b") == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()


def test_a_probe_that_stops_answering_fails_the_clause_instead_of_aborting_the_run(
    conformant: StubAdapter, secret: str
) -> None:
    """Discovery proved the probe answered ONCE, and nothing more than that.

    A probe that starts failing partway through used to escape as an httpx error
    out of the middle of a clause and take the whole report with it, and the
    observation windows read it many times per clause rather than twice, so
    there are far more chances for it now. The honest outcome is a clause
    failure that names the probe endpoint, so the vendor knows to look at their
    probe rather than at their reply handling, and every other rule still
    reaches a verdict.
    """

    honest = side_effect_probe_for(conformant.base_url)
    reads = itertools.count()

    def stops_answering() -> int:
        if next(reads) < 1:
            return honest()
        raise httpx.ConnectError("the probe endpoint refused the connection")

    report = run_floor(
        _adapter_for(conformant, secret), driver=None, side_effect_probe=stops_answering
    )

    assert clause_status(report, "3a") == "fail", report.detail()
    assert rule_status(report, 6) == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()
    detail = clause(report, "3a").detail
    assert "side effect probe" in detail, detail
    assert "clause 3a" in detail, detail
    # The run still produced a report, and the rules that never touch the probe
    # still reached their own verdicts rather than being lost with it.
    assert rule_status(report, 4) == "pass", report.detail()
    assert rule_status(report, 5) == "pass", report.detail()


def test_clause_3b_is_not_run_without_a_side_effect_probe(
    conformant: StubAdapter, secret: str
) -> None:
    """3b needs the probe as much as 3a does, and says so rather than passing."""

    report = _report_for(conformant, secret, with_probe=False, with_driver=True)

    assert clause_status(report, "3b") == "not_run", report.detail()
    assert report.automated_floor == "fail", report.detail()


def test_non_conformant_stub_fails_rule_3_clause_3b_when_it_accepts_with_its_secret_unset(
    tmp_path: Path, secret: str
) -> None:
    """An adapter started with no egress secret of its own must refuse
    everything, not serve unauthenticated."""

    with _running(
        non_conformant_stub(
            "accepts_when_secret_unset", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret, with_driver=True)
        assert clause_status(report, "3b") == "fail", report.detail()
        assert report.automated_floor == "fail"


def test_non_conformant_stub_fails_rule_4_when_it_redirects(
    tmp_path: Path, secret: str
) -> None:
    """A 3xx would bounce the platform's egress credential at whatever origin
    the adapter named, so the kit must post with redirects disabled."""

    with _running(
        non_conformant_stub("redirects", secret=secret, state_path=tmp_path / "state.json")
    ) as stub:
        report = _report_for(stub, secret)
        assert rule_status(report, 4) == "fail", report.detail()


def test_non_conformant_stub_fails_rule_4_when_its_ack_is_not_json(
    tmp_path: Path, secret: str
) -> None:
    """The worker tolerates an empty body, the floor does not. The two must not
    disagree silently, so the kit is the stricter one and says so."""

    with _running(
        non_conformant_stub(
            "empty_ack_body", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert rule_status(report, 4) == "fail", report.detail()


def test_non_conformant_stub_fails_rule_5_when_it_rejects_turn_status(
    tmp_path: Path, secret: str
) -> None:
    """``turn.status`` is the event an email adapter has no use for, which is
    exactly the one "tolerate ones it does not use" is about."""

    with _running(
        non_conformant_stub(
            "rejects_turn_status", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert rule_status(report, 5) == "fail", report.detail()


def test_non_conformant_stub_fails_rule_6_when_it_double_sends_a_duplicate(
    tmp_path: Path, secret: str
) -> None:
    """Both acks are 200 and the adapter answered the correspondent twice.

    Wire indistinguishable, which is why rule 6 is ``not_run`` without a probe
    rather than a quiet pass.
    """

    with _running(
        non_conformant_stub(
            "double_sends_duplicate", secret=secret, state_path=tmp_path / "state.json"
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert rule_status(report, 6) == "fail", report.detail()


def test_non_conformant_stub_fails_rule_6_when_the_duplicate_effect_is_delayed(
    tmp_path: Path, secret: str
) -> None:
    """The redelivery is acked 200 immediately and answered 200 ms later.

    Both acks look identical to a deduping adapter's, and the count read the
    instant the second ack lands shows a delta of one, so the whole rule passes
    on an adapter that answered the correspondent twice. The count has to be
    WATCHED across the window rather than sampled at the end of the exchange.
    """

    with _running(
        non_conformant_stub(
            "delayed_duplicate_side_effect",
            secret=secret,
            state_path=tmp_path / "state.json",
        )
    ) as stub:
        report = _report_for(stub, secret)

        assert rule_status(report, 6) == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()
        assert "answered twice" in clause(report, "6").detail, report.detail()


def test_non_conformant_stub_fails_rule_6_when_it_rejects_a_finished_conversation(
    tmp_path: Path, secret: str
) -> None:
    """Rule 6's second half, against an adapter that passes its first half.

    This adapter dedupes perfectly: the exact duplicate event_id is tolerated
    and answered once, so the whole probe the clause is named for reads clean.
    What it refuses is a further completion for a conversation it has already
    retired, which is what a second turn on that conversation looks like, and
    what a sweeper draining a record after an outage looks like. A clause that
    probes a FRESH conversation there reads this adapter as conformant, because
    a conversation it has never seen is not one it has finished.
    """

    with _running(
        non_conformant_stub(
            "rejects_finished_conversation",
            secret=secret,
            state_path=tmp_path / "state.json",
        )
    ) as stub:
        report = _report_for(stub, secret)

        assert rule_status(report, 6) == "fail", report.detail()
        assert report.automated_floor == "fail", report.detail()
        # The dedupe half genuinely held, so the failure is attributable to the
        # tolerance half and not to a break bleeding across the clause.
        assert "already finished" in clause(report, "6").detail, report.detail()
        assert "redelivered" not in clause(report, "6").detail, report.detail()


def test_ack_of_exactly_the_cap_passes_and_one_byte_over_fails(
    tmp_path: Path, secret: str
) -> None:
    """The worker refuses OVER the cap, so the boundary assertion is ``<=``.

    An off by one in either direction is caught: 65536 is a pass and 65537 is a
    failure, in one test so neither half can be relaxed alone.
    """

    with _running(
        conformant_stub(
            secret=secret,
            state_path=tmp_path / "at-cap.json",
            ack_body_bytes=MAX_ACK_BODY_BYTES,
        )
    ) as at_cap:
        at_cap_report = _report_for(at_cap, secret)
        assert rule_status(at_cap_report, 4) == "pass", at_cap_report.detail()

    with _running(
        conformant_stub(
            secret=secret,
            state_path=tmp_path / "over.json",
            ack_body_bytes=MAX_ACK_BODY_BYTES + 1,
        )
    ) as over:
        over_report = _report_for(over, secret)
        assert rule_status(over_report, 4) == "fail", over_report.detail()


def test_chunked_oversize_body_is_caught(tmp_path: Path, secret: str) -> None:
    """A chunked body carries no ``Content-Length`` and answers a short first
    read, so a kit that checks the header, or does one sized read, passes it."""

    with _running(
        conformant_stub(
            secret=secret,
            state_path=tmp_path / "state.json",
            ack_chunked_bytes=MAX_ACK_BODY_BYTES * 3,
        )
    ) as stub:
        report = _report_for(stub, secret)
        assert rule_status(report, 4) == "fail", report.detail()


def test_rule_5_sends_only_the_four_union_members(
    conformant: StubAdapter, secret: str
) -> None:
    """The reply wire is a STRICT four member union.

    Forward event tolerance is not part of floor rule 5, so a kit that sends an
    unknown discriminator is asserting a requirement the platform never made and
    would teach adapters to accept a shape the worker cannot send.
    """

    _report_for(conformant, secret)

    observed = {entry["event"] for entry in read_probe(conformant.base_url)["events"]}
    assert observed == _FOUR_EVENTS, f"the kit sent {sorted(observed)}"


def test_a_hostile_side_effect_probe_response_is_refused_rather_than_buffered(
    tmp_path: Path, secret: str
) -> None:
    """The probe GET lands on the same untrusted origin the ack comes from.

    The payload here is VALID and merely enormous, so an unbounded read returns
    the count and quietly buffers the padding. Only a cap catches it, and the
    honest outcome of a refused probe is ``not_run`` on the two clauses that
    need one, which is nonconformant.
    """

    with _running(
        conformant_stub(
            secret=secret,
            state_path=tmp_path / "state.json",
            probe_chunked_bytes=MAX_ACK_BODY_BYTES * 3,
        )
    ) as stub:
        adapter = _adapter_for(stub, secret)

        assert side_effect_probe(adapter) is None, (
            "an oversize probe response was accepted, so the kit buffered it"
        )

        report = run_floor(adapter, driver=None, side_effect_probe=side_effect_probe(adapter))
        assert clause_status(report, "3a") == "not_run", report.detail()
        assert rule_status(report, 6) == "not_run", report.detail()
        assert report.automated_floor == "fail", report.detail()


def test_clause_3a_and_rule_6_report_not_run_without_a_side_effect_probe(
    conformant: StubAdapter, secret: str
) -> None:
    """Missing evidence is never a pass, and it is never a silent skip either.

    Both clauses need a side effect probe to be decidable, so without one they
    are ``not_run``, and ``not_run`` is nonconformant.
    """

    report = _report_for(conformant, secret, with_probe=False, with_driver=True)

    assert clause_status(report, "3a") == "not_run", report.detail()
    assert rule_status(report, 6) == "not_run", report.detail()
    assert report.automated_floor == "fail", report.detail()
