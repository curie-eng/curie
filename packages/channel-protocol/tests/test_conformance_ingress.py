"""The conformance kit's ingress half: floor rules 1, 2 and 7 (#1516).

**Every rule level test here is parametrized over BOTH drivers.** The in process
``StubIngressDriver`` holds the stub object; the ``SubprocessIngressDriver``
holds a ``subprocess.Popen`` and an HTTP surface and shares no memory with
anything. If the kit ever correlated an observed ingress post to a stimulus
through private in process state instead of through the ``delivery_id`` on the
wire, the in process run would still pass and the out of process run would fail.
That asymmetry is the entire reason the parametrization exists.

The kit is black box: nothing here imports the adapter, and nothing internal is
faked. ``FakeIngress`` is a real HTTP server reproducing the platform's real
claim semantics, and it is part of the kit rather than a mock of it.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from channel_protocol.conformance import (
    MAX_TURN_BODY_BYTES,
    AdapterUnderTest,
    FakeIngress,
    FloorReport,
    run_floor,
)
from conformance_stubs import (
    LyingIngressDriver,
    RacingIngressDriver,
    StubAdapter,
    StubIngressDriver,
    SubprocessIngressDriver,
    clause_status,
    conformant_stub,
    free_port,
    non_conformant_stub,
    post_with_declared_length,
    random_secret,
    rule,
    rule_status,
    side_effect_probe_for,
)

DRIVER_KINDS = ("in-process", "subprocess")

_KIND = "email"
_ADDRESS = "agent@example.test"


@dataclass
class Harness:
    """One running adapter plus the driver that steers it."""

    adapter: AdapterUnderTest
    driver: Any
    base_url: str


@pytest.fixture
def secret() -> str:
    return random_secret()


@contextmanager
def _harness(
    driver_kind: str,
    *,
    tmp_path: Path,
    secret: str,
    behavior_name: str = "conformant",
) -> Iterator[Harness]:
    """Start an adapter and its driver, and tear both down whatever happens.

    A leaked listener or an orphaned subprocess outlives the test that made it
    and breaks the next run, so teardown is unconditional.
    """

    state_path = tmp_path / "stub-state.json"
    if driver_kind == "in-process":
        stub: StubAdapter = (
            conformant_stub(secret=secret, state_path=state_path)
            if behavior_name == "conformant"
            else non_conformant_stub(behavior_name, secret=secret, state_path=state_path)
        )
        stub.start()
        driver: Any = StubIngressDriver(stub)
        harness = Harness(
            adapter=_adapter_for(stub.endpoint, secret),
            driver=driver,
            base_url=stub.base_url,
        )
        try:
            yield harness
        finally:
            stub.stop()
        return

    subprocess_driver = SubprocessIngressDriver(
        port=free_port(),
        secret=secret,
        state_path=state_path,
        behavior_name=behavior_name,
    )
    subprocess_driver.boot()
    try:
        yield Harness(
            adapter=_adapter_for(subprocess_driver.endpoint, secret),
            driver=subprocess_driver,
            base_url=subprocess_driver.base_url,
        )
    finally:
        subprocess_driver.stop()


def _adapter_for(endpoint: str, secret: str) -> AdapterUnderTest:
    return AdapterUnderTest(
        endpoint=endpoint,
        secret=secret,
        kind=_KIND,
        address=_ADDRESS,
        timeout_s=15.0,
    )


def _run(harness: Harness, *, driver: Any = None) -> FloorReport:
    return run_floor(
        harness.adapter,
        driver=harness.driver if driver is None else driver,
        side_effect_probe=side_effect_probe_for(harness.base_url),
    )


def _post(url: str, payload: dict[str, Any], *, api_key: str | None) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if api_key is not None:
        headers["X-API-Key"] = api_key
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return int(response.status), json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return int(error.code), json.loads(error.read() or b"null")


@contextmanager
def _fake_ingress() -> Iterator[FakeIngress]:
    ingress = FakeIngress(kind=_KIND, address=_ADDRESS)
    ingress.start()
    try:
        yield ingress
    finally:
        ingress.stop()


# --- FakeIngress itself, which the ingress rules are decided against ----------


def test_fake_ingress_reproduces_the_duplicate_matrix() -> None:
    """The fake has to answer what the real ingress answers, or every rule
    decided against it is decided against a fiction.

    Includes the claim key: the platform derives ``event_id`` from the binding
    plus ``delivery_id`` and never ``delivery_id`` alone, so two inboxes sharing
    an upstream id space must not swallow each other's turns.
    """

    with _fake_ingress() as ingress:
        turns = f"{ingress.url}/channels/turns"
        body = {
            "kind": _KIND,
            "address": _ADDRESS,
            "delivery_id": "dlv-1",
            "conversation_id": "conv-1",
            "author": "someone@example.test",
            "text": "hello",
        }

        first_status, first = _post(turns, body, api_key=ingress.token)
        assert first_status == 200
        assert first["duplicate"] is False
        assert first["stream_id"]

        retry_status, retry = _post(turns, body, api_key=ingress.token)
        assert retry_status == 200
        assert retry["duplicate"] is True
        assert retry["event_id"] == first["event_id"]
        assert retry["stream_id"] == first["stream_id"]

        other_inbox = dict(body, address="other@example.test")
        other_status, other = _post(turns, other_inbox, api_key=ingress.token)
        assert other_status == 200
        assert other["duplicate"] is False
        assert other["event_id"] != first["event_id"]

        # Armed for ONE identity. A different delivery arriving first must not
        # be able to consume it, or rule 2 passes on a declared delivery that
        # never saw the in flight claim its finality is about.
        ingress.arm_202("dlv-2")
        unrelated_status, _ = _post(
            turns, dict(body, delivery_id="dlv-other"), api_key=ingress.token
        )
        assert unrelated_status == 200
        assert ingress.armed_202() == "dlv-2"

        in_flight_status, in_flight = _post(
            turns, dict(body, delivery_id="dlv-2"), api_key=ingress.token
        )
        assert in_flight_status == 202
        assert in_flight["duplicate"] is True
        assert ingress.armed_202() is None
        # Never the `pending:` sentinel: the real API answers null here, and
        # returning the sentinel would teach an author to parse a value
        # production never sends.
        assert in_flight["stream_id"] is None


def test_a_request_the_ingress_cannot_frame_is_still_recorded() -> None:
    """The adapter under test writes its own request headers.

    Every ingress rule decides on which records landed in its window, and two of
    them read absence as conformance, so a header the ingress cannot parse must
    never be able to delete the observation. It is refused, and it is COUNTED.
    """

    with _fake_ingress() as ingress:
        turn = post_with_declared_length(
            f"{ingress.url}/channels/turns",
            json.dumps({"kind": _KIND, "address": _ADDRESS, "delivery_id": "dlv-1"}).encode(),
            {"Content-Type": "application/json", "X-API-Key": ingress.token},
            declared_length="51x",
        )
        assert turn == 400

        mint = post_with_declared_length(
            f"{ingress.url}/channels/token",
            json.dumps({"kind": _KIND, "address": _ADDRESS}).encode(),
            {"Content-Type": "application/json"},
            declared_length="0abc",
        )
        assert mint == 400

        records = ingress.records()
        assert [record.path for record in records] == [
            "/channels/turns",
            "/channels/token",
        ], records
        assert all(record.framing_error for record in records), records
        # The trust boundary clause counts records on the mint route, so an
        # unreadable mint attempt is still a mint attempt.
        assert ingress.mint_attempts() == 1


def test_an_oversize_declared_turn_body_is_refused_and_recorded() -> None:
    """The bound is enforced on the DECLARED length, before a byte is read.

    Same posture, and the same status, as the real API's ``_read_bounded_body``:
    the ingress the kit points an untrusted adapter at must not be persuadable
    into holding an arbitrary body on the kit's own heap.
    """

    with _fake_ingress() as ingress:
        status = post_with_declared_length(
            f"{ingress.url}/channels/turns",
            b'{"delivery_id": "dlv-1"}',
            {"Content-Type": "application/json", "X-API-Key": ingress.token},
            declared_length=str(MAX_TURN_BODY_BYTES + 1),
        )

        assert status == 413
        records = ingress.records()
        assert len(records) == 1, records
        assert records[0].framing_error, records[0]


def test_fake_ingress_refuses_a_keyless_mint() -> None:
    """The adapter holds no platform key, so a mint it attempts must fail.

    Without this the kit would certify the trust boundary breach outright: a
    self minting adapter defeats both the token TTL and the binding generation.
    """

    with _fake_ingress() as ingress:
        mint = f"{ingress.url}/channels/token"

        keyless_status, _ = _post(mint, {"kind": _KIND, "address": _ADDRESS}, api_key=None)
        assert keyless_status == 401

        wrong_status, _ = _post(
            mint, {"kind": _KIND, "address": _ADDRESS}, api_key="not-the-platform-key"
        )
        assert wrong_status == 401

        operator_status, minted = _post(
            mint, {"kind": _KIND, "address": _ADDRESS}, api_key=ingress.platform_key
        )
        assert operator_status == 200
        assert minted["token"]


# --- the ingress floor rules, under both drivers ------------------------------


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_conformant_stub_passes_every_ingress_rule(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    with _harness(driver_kind, tmp_path=tmp_path, secret=secret) as harness:
        report = _run(harness)

    assert rule_status(report, 1) == "pass", report.detail()
    assert rule_status(report, 2) == "pass", report.detail()
    assert rule_status(report, 7) == "pass", report.detail()
    assert report.automated_floor == "pass", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_1_catches_a_fresh_delivery_id_per_retry(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """A delivery_id minted per attempt answers the correspondent once per
    retry, because the platform's claim converges on the id and nothing else."""

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="fresh_delivery_id_per_retry",
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 1) == "fail", report.detail()
    assert report.automated_floor == "fail"


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_1_fails_an_adapter_that_never_met_the_outage_and_never_retried(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """Rule 1 needs the FAILURE on the wire, not a window it was assumed in.

    This adapter starts slowly and abandons a delivery the moment the transport
    fails. A kit that takes the transport down for a fixed 200 ms and restores
    it on a timer never overlaps with the adapter's first attempt at all: the
    post lands afterwards, gets a 200, and rule 1 certifies a retry that never
    happened off one successful delivery.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="slow_start_and_drops_on_transport_failure",
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 1) == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_1_fails_when_the_driver_declares_an_identity_the_adapter_never_sends(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """A driver that does not implement the correlation contract is a finding.

    Reading a lying driver as conformant is the failure that would let every
    broken vendor driver certify its adapter, so the honest verdict is a rule 1
    failure naming the identity nothing on the wire matched.
    """

    with _harness(driver_kind, tmp_path=tmp_path, secret=secret) as harness:
        report = _run(harness, driver=LyingIngressDriver(harness.driver))

    assert rule_status(report, 1) == "fail", report.detail()
    assert "never-sent-" in rule(report, 1).detail, rule(report, 1).detail


def test_correlation_uses_only_the_declared_identity(tmp_path: Path) -> None:
    """The in process and out of process runs must reach the SAME verdict.

    Correlate by arrival order instead of by the declared ``delivery_id`` and
    the out of process run diverges, because nothing out there can tell the kit
    which stimulus a post belongs to except the value on the wire.
    """

    verdicts: dict[str, dict[str, dict[int, str]]] = {}
    for behavior_name in ("conformant", "fresh_delivery_id_per_retry"):
        verdicts[behavior_name] = {}
        for driver_kind in DRIVER_KINDS:
            secret = random_secret()
            root = tmp_path / f"{behavior_name}-{driver_kind}"
            root.mkdir()
            with _harness(
                driver_kind, tmp_path=root, secret=secret, behavior_name=behavior_name
            ) as harness:
                report = _run(harness)
            verdicts[behavior_name][driver_kind] = {
                number: rule_status(report, number) for number in (1, 2, 7)
            }

    for behavior_name, by_driver in verdicts.items():
        assert by_driver["in-process"] == by_driver["subprocess"], (
            f"{behavior_name} was judged differently out of process: {by_driver}"
        )
    assert verdicts["conformant"]["subprocess"][1] == "pass"
    assert verdicts["fresh_delivery_id_per_retry"]["subprocess"][1] == "fail"


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_2_catches_a_retry_after_a_202(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """A 202 is a RESPONSE, so it is final. Posting again after one is the
    defect, even though 202 reads like an invitation to come back."""

    with _harness(
        driver_kind, tmp_path=tmp_path, secret=secret, behavior_name="retries_after_202"
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 2) == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_2_catches_a_retry_after_a_202_hidden_behind_a_bad_content_length(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The same break as above, with the duplicate post made unreadable.

    This adapter posts honestly, then re posts under a Content-Length no server
    can parse. If a parse failure can cost the ingress an observation, the kit
    sees one post, reports ``2 [pass] the adapter did not post it again``, and
    certifies an adapter that treats a response as a retry signal.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="retries_after_202_evasively",
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 2) == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_2_catches_a_retry_that_arrives_after_any_fixed_settle_window(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The same break, retried late enough to outlast a fixed window.

    650 ms is not adversarial engineering, it is an ordinary backoff. A kit that
    sleeps 500 ms and then decides records the conformant half of this adapter's
    behavior and stops watching before the defect happens, so the verdict is
    about the kit's timer rather than about the adapter.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="retries_late_after_202",
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 2) == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_2_gives_the_202_to_the_declared_delivery_and_no_other(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The in flight claim has to reach the DECLARED delivery, or nothing was
    tested.

    ``RacingIngressDriver`` puts another, entirely legitimate delivery on the
    wire first, which is what an upstream queue holding more than one message
    looks like. Against a globally armed one shot that decoy consumes the 202
    and the declared delivery only ever sees a 200, so the rule reports pass
    having exercised finality against a response it never provoked. The verdict
    stays pass here and the reason is what changed: the 202 went to the
    identity the rule is about.
    """

    with _harness(driver_kind, tmp_path=tmp_path, secret=secret) as harness:
        report = _run(harness, driver=RacingIngressDriver(harness.driver))

    assert rule_status(report, 2) == "pass", report.detail()
    assert "answered 202" in rule(report, 2).detail, rule(report, 2).detail


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_2_tolerates_a_redelivery_under_a_new_stimulus(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The false positive control, and the reason a stimulus label exists.

    An upstream redelivery under an identity the kit never declared is
    legitimate at least once behavior. A kit that simply counted posts after the
    202 would red this, and would then reject correct adapters.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="posts_an_unrelated_delivery",
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 2) == "pass", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_passes_when_a_replacement_token_resumes_the_same_delivery_id(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The corrected rule 7: hold the delivery, do not die, resume on a fresh
    operator supplied token, and do not mint anything."""

    with _harness(driver_kind, tmp_path=tmp_path, secret=secret) as harness:
        report = _run(harness)

    assert rule_status(report, 7) == "pass", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_fails_when_a_401_is_fatal(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    with _harness(
        driver_kind, tmp_path=tmp_path, secret=secret, behavior_name="exits_on_401"
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 7) == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_fails_when_the_delivery_is_silently_dropped(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """Surviving the 401 is not enough: a delivery discarded on the way is a
    turn the correspondent never gets an answer to."""

    with _harness(
        driver_kind, tmp_path=tmp_path, secret=secret, behavior_name="drops_on_401"
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 7) == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_fails_an_adapter_that_tries_to_self_mint(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """An adapter that re mints its own token is the breach RULING 2 exists to
    stop, so it must FAIL rather than pass the rule it used to define."""

    with _harness(
        driver_kind, tmp_path=tmp_path, secret=secret, behavior_name="self_mints_on_401"
    ) as harness:
        report = _run(harness)

    assert rule_status(report, 7) == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_fails_when_the_resumed_post_is_not_the_held_delivery(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """"Something arrived after the restart" is not "the held delivery resumed".

    This adapter discards the held delivery on the 401 and then posts an
    entirely different one once it is back. A resume predicate that accepts any
    later 2xx reports that the held delivery resumed, and the detail it prints
    says so, about a turn the correspondent will never get an answer to.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="drops_on_401_and_posts_unrelated_after_restart",
    ) as harness:
        report = _run(harness)

    assert clause_status(report, "7a") == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()


@pytest.mark.parametrize("driver_kind", DRIVER_KINDS)
def test_rule_7_fails_a_self_mint_hidden_behind_a_bad_content_length(
    driver_kind: str, tmp_path: Path, secret: str
) -> None:
    """The trust boundary clause, against an adapter that hides its mint.

    Clause 7b decides on ``mint_attempts()``, which counts records, so an
    unreadable mint POST that went unrecorded would report ``7b [pass] the
    adapter never tried to mint its own replacement token`` about an adapter
    that just did. This is the one verdict a vendor quotes.
    """

    with _harness(
        driver_kind,
        tmp_path=tmp_path,
        secret=secret,
        behavior_name="self_mints_on_401_evasively",
    ) as harness:
        report = _run(harness)

    assert clause_status(report, "7b") == "fail", report.detail()
    assert rule_status(report, 7) == "fail", report.detail()
    assert report.automated_floor == "fail", report.detail()


def test_rules_1_2_7_report_not_run_without_a_driver_and_the_report_is_nonconformant(
    tmp_path: Path, secret: str
) -> None:
    """The most important assertion in the kit: NO EVIDENCE IS NOT A PASS.

    An unsupplied ingress driver leaves rules 1, 2 and 7 and clause 3b with
    nothing to exercise them. They report ``not_run``, and because ``not_run``
    is an automatable clause short of pass, the whole verdict is ``fail``. The
    day this stops holding, a vendor can claim conformance off a partial run.
    """

    stub = conformant_stub(secret=secret, state_path=tmp_path / "stub-state.json")
    stub.start()
    try:
        report = run_floor(
            _adapter_for(stub.endpoint, secret),
            driver=None,
            side_effect_probe=side_effect_probe_for(stub.base_url),
        )
    finally:
        stub.stop()

    for number in (1, 2, 7):
        assert rule_status(report, number) == "not_run", report.detail()
    assert report.automated_floor == "fail", report.detail()
    # Rules 4, 5 and 6 were genuinely exercised, so this is a partial run
    # reported honestly rather than a run that failed to start.
    for number in (4, 5, 6):
        assert rule_status(report, number) == "pass", report.detail()
