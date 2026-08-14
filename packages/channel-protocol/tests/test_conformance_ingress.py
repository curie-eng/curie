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
from channel_protocol.conformance import AdapterUnderTest, FakeIngress, FloorReport, run_floor
from conformance_stubs import (
    LyingIngressDriver,
    StubAdapter,
    StubIngressDriver,
    SubprocessIngressDriver,
    conformant_stub,
    free_port,
    non_conformant_stub,
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

        ingress.arm_202()
        in_flight_status, in_flight = _post(
            turns, dict(body, delivery_id="dlv-2"), api_key=ingress.token
        )
        assert in_flight_status == 202
        assert in_flight["duplicate"] is True
        # Never the `pending:` sentinel: the real API answers null here, and
        # returning the sentinel would teach an author to parse a value
        # production never sends.
        assert in_flight["stream_id"] is None


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
