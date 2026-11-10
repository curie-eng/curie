"""Standalone commissioning ingress refusal order, @spec PROTECTED-COMMISSIONING-2/4.

Each case drives the real ingress against the owned TLS Valkey with a signed
request and a provisioner-owned directory (``_commissioning_ingress_harness``).
For each refusal step one fault is injected while every earlier step holds; the
refusal names that step, and nothing after it runs: no guard call, broker
connection, reservation, lane entry or admission that a later step would make.
The HTTP application is constructed explicitly and is never mounted on the API.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import _commissioning_ingress_harness as harness
import httpx
import pytest
from _commissioning_ingress_harness import (
    CommissioningTuple,
    GuardProbe,
    canonical,
    connections,
    snapshot,
)

pworker_service = harness.pworker_service

BODY = b'{"alert":"example"}'


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


@pytest.fixture
def campaign(pworker_service: Any) -> Any:
    pworker_service.command("FLUSHDB")
    built = CommissioningTuple(pworker_service)
    built.install()
    try:
        yield built
    finally:
        built.remove()
        pworker_service.command("FLUSHDB")


@dataclass
class Parties:
    """The authenticated issuer session and control capability the ingress holds."""

    issuer: Any
    control: Any


@asynccontextmanager
async def running(campaign: Any, *, start: bool = True) -> AsyncIterator[Parties]:
    """An issued permit, its journal (started unless ``start`` is false), issuer and control."""
    from curie_commissioning_ingress.ingress import CampaignControl

    await campaign.issue()
    await (await campaign.journal(start=start)).close()
    issuer = await campaign.session("issuer")
    try:
        control = await CampaignControl.open(
            await campaign.session("control"), campaign.permit, **campaign.trusted
        )
        try:
            yield Parties(issuer, control)
        finally:
            await control.reservations.journal.close()
    finally:
        await issuer.close()


def ingress(directory: Path, parties: Parties | None = None, **kwargs: Any) -> Any:
    from curie_commissioning_ingress.ingress import CommissioningIngress

    if parties is not None:
        kwargs = {"issuer": parties.issuer, "control": parties.control, **kwargs}
    return CommissioningIngress(str(directory), **kwargs)


async def refusal(target: Any, request: dict[str, Any]) -> int:
    from curie_commissioning_ingress.ingress import CommissioningIngressRefused

    with pytest.raises(CommissioningIngressRefused) as error:
        await target.admit(**request)
    assert str(error.value) == "Commissioning ingress refused"
    step: int = error.value.step
    return step


# --- Positive path --------------------------------------------------------------------------


def test_a_signed_delivery_enqueues_one_lane_entry_and_commits_its_admission(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_admissions import admission_key
    from curie_protected_hooks.commissioning_campaign_records import (
        parse_campaign_admission,
        parse_campaign_envelope,
    )

    directory = campaign.write_directory(tmp_path / "runtime")
    guard = GuardProbe(campaign)
    request = campaign.signed(BODY)

    async def scenario() -> Any:
        async with running(campaign) as parties:
            return await ingress(directory, parties, guard=guard.guard).admit(**request)

    admission = run(scenario())
    assert guard.calls == 1
    a = admission.as_dict()
    assert a["delivery"]["delivery_id"] == request["delivery_id"]
    assert a["delivery"]["source"] == campaign.value["sources"][0]
    assert a["delivery"]["target_ref"] == campaign.value["targets"][0]
    entries = pworker_service.command("XRANGE", campaign.stream, "-", "+")
    assert len(entries) == 1
    entry_id, fields = entries[0]
    entry_id = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
    fields = {
        (k.decode() if isinstance(k, bytes) else k): v
        for k, v in (
            fields.items()
            if isinstance(fields, dict)
            else zip(fields[::2], fields[1::2], strict=True)
        )
    }
    assert set(fields) == {"payload", "commissioning_envelope"}
    assert fields["payload"] == BODY
    envelope = parse_campaign_envelope(fields["commissioning_envelope"])
    assert envelope.digest == a["envelope_digest"] and a["stream_entry_id"] == entry_id
    stored = pworker_service.command(
        "GET", admission_key(campaign.permit, a["delivery"]["reservation_id"])
    )
    assert parse_campaign_admission(stored) == admission
    reservation = json.loads(
        pworker_service.command(
            "GET",
            "protected:commissioning:reservation:"
            + campaign.suffix
            + ":"
            + a["delivery"]["reservation_id"],
        )
    )
    assert reservation["status"] == "dispatched" and reservation["kind"] == "delivery"
    assert not any(key.startswith(b"curie:") for key in pworker_service.command("KEYS", "*"))


# --- Step 1: predeclared entry point and B against G and M ----------------------------------


def _other_guard(campaign: Any) -> bytes:
    value = json.loads(campaign.guard_config)
    value["bootstrap_digest"] = "0" * 64
    return canonical(value)


def _swapped_entry_points(campaign: Any) -> bytes:
    value = campaign.bootstrap.as_dict()
    value["entry_points"] = {
        "production": value["entry_points"]["commissioning"],
        "commissioning": value["entry_points"]["production"],
    }
    return canonical(value)


@pytest.mark.parametrize(
    "files",
    [
        lambda c: {"guard_config.json": _other_guard(c)},
        lambda c: {"commissioning_bootstrap.json": _swapped_entry_points(c)},
        lambda c: {"commissioning_ingress.json": b"{}"},
        lambda c: {"manifest.json": b"not json"},
    ],
    ids=["guard-names-another-bootstrap", "production-entry-point", "ingress-file", "manifest"],
)
def test_step_1_refuses_before_any_broker_connection(
    pworker_service: Any, campaign: Any, tmp_path: Path, files: Any
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime", **files(campaign))
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before, opened = snapshot(pworker_service), connections(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 1
            assert connections(pworker_service) == opened
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


@pytest.mark.parametrize(
    "fault",
    [
        "symlinked-file",
        "symlinked-directory",
        "group-writable-file",
        "world-writable-directory",
        "other-owner",
    ],
)
def test_step_1_refuses_a_directory_not_strictly_owned_by_the_provisioner(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    owner = None
    if fault == "symlinked-file":
        real = tmp_path / "manifest.real"
        (directory / "manifest.json").rename(real)
        (directory / "manifest.json").symlink_to(real)
    elif fault == "symlinked-directory":
        link = tmp_path / "link"
        link.symlink_to(directory)
        directory = link
    elif fault == "group-writable-file":
        (directory / "commissioning_bootstrap.json").chmod(0o620)
    elif fault == "world-writable-directory":
        directory.chmod(0o702)
    else:
        owner = os.geteuid() + 1
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before, opened = snapshot(pworker_service), connections(pworker_service)
            target = ingress(directory, parties, guard=guard.guard, owner=owner)
            assert await refusal(target, campaign.signed(BODY)) == 1
            assert connections(pworker_service) == opened
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


def test_step_1_refuses_a_missing_directory(campaign: Any, tmp_path: Path) -> None:
    async def scenario() -> None:
        target = ingress(tmp_path / "absent", guard=GuardProbe(campaign).guard)
        assert await refusal(target, campaign.signed(BODY)) == 1

    run(scenario())


# --- Step 2: pinned broker session as the candidate enqueue ---------------------------------


@pytest.mark.parametrize("fault", ["password", "run_id", "credential_ref"])
def test_step_2_refuses_an_unauthenticated_or_repinned_session(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    overrides: dict[str, bytes] = {}
    if fault == "password":
        value = campaign.ingress_file()
        value["candidate_enqueue"]["password"] = "wrong-" + value["candidate_enqueue"]["password"]
        overrides["commissioning_ingress.json"] = canonical(value)
    elif fault == "run_id":
        m = campaign.manifest.as_dict()
        m["broker_identity"]["run_id"] = "0" * 40
        overrides["manifest.json"] = canonical(m)
    else:
        value = campaign.ingress_file()
        value["candidate_enqueue"]["credential_ref"]["generation"] = "9"
        overrides["commissioning_ingress.json"] = canonical(value)
    directory = campaign.write_directory(tmp_path / "runtime", **overrides)
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 2
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


# --- Step 3: the control identity authenticated, not merely referenced ----------------------


@pytest.mark.parametrize(
    "fault", ["no-control", "terminal-control", "no-issuer", "issuer-is-control", "terminal-issuer"]
)
def test_step_3_refuses_unless_issuer_and_control_authenticate_now(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    """Issuer and control must each be a live session answering ACL WHOAMI as B names."""
    from curie_commissioning_ingress.ingress import CampaignControl

    directory = campaign.write_directory(tmp_path / "runtime")
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            opened: list[Any] = []
            try:
                if fault == "no-control":
                    override: dict[str, Any] = {"control": None}
                elif fault == "terminal-control":
                    terminal = await CampaignControl.open(
                        await campaign.session("control", terminal=True),
                        campaign.permit,
                        **campaign.trusted,
                    )
                    opened.append(terminal.reservations.journal)
                    override = {"control": terminal}
                elif fault == "no-issuer":
                    override = {"issuer": None}
                elif fault == "issuer-is-control":
                    override = {"issuer": parties.control.session}
                else:
                    session = await campaign.session("issuer", terminal=True)
                    opened.append(session)
                    override = {"issuer": session}
                before = snapshot(pworker_service)
                target = ingress(directory, parties, guard=guard.guard, **override)
                assert await refusal(target, campaign.signed(BODY)) == 3
                assert snapshot(pworker_service) == before
            finally:
                for item in opened:
                    await item.close()

    run(scenario())
    assert guard.calls == 0


def test_step_3_refuses_a_control_whose_identity_is_not_the_bootstrap_one(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    """B names another control; the authenticated control journal is not that principal."""
    value = campaign.bootstrap.as_dict()
    value["control_identity"] = {"id": "commissioning/example-other-control", "generation": "1"}
    other = CommissioningTuple(pworker_service, bootstrap=value)
    directory = other.write_directory(
        tmp_path / "runtime",
        **{"commissioning_ingress.json": canonical(campaign.ingress_file())},
    )
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 3
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


# --- Step 4: the permit parses and its binding holds ----------------------------------------


@pytest.mark.parametrize("fault", ["not-issued", "other-campaign"])
def test_step_4_refuses_an_absent_or_foreign_permit(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    value = campaign.ingress_file()
    if fault == "other-campaign":
        value["campaign"]["campaign_generation"] = "7"
    directory = campaign.write_directory(
        tmp_path / "runtime", **{"commissioning_ingress.json": canonical(value)}
    )
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        if fault == "not-issued":
            # A control journal over a permit the issuer never wrote cannot exist,
            # so the capability is opened on the issued permit and then the key is lost.
            async with running(campaign) as parties:
                pworker_service.command("DEL", "protected:commissioning:permit:" + campaign.suffix)
                before = snapshot(pworker_service)
                target = ingress(directory, parties, guard=guard.guard)
                assert await refusal(target, campaign.signed(BODY)) == 4
                assert snapshot(pworker_service) == before
            return
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 4
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


def test_step_4_refuses_a_permit_bound_to_another_manifest(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    """The issued permit names a manifest other than the directory's; G and B still agree."""
    m = campaign.manifest.as_dict()
    m["qualification_id"] = "45454545-4545-4545-8545-454545454545"
    directory = campaign.write_directory(tmp_path / "runtime", **{"manifest.json": canonical(m)})
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 4
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 0


# --- Step 5: the guard, refusing by default -------------------------------------------------


@pytest.mark.parametrize("guard", ["absent", "refuses", "stand-in"])
def test_step_5_refuses_without_the_campaign_guard_and_when_it_refuses(
    pworker_service: Any, campaign: Any, tmp_path: Path, guard: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign, refuse=True)

    class StandIn:
        """Satisfies the guard's method shape without being the guard."""

        async def require_current(self) -> None:
            return None

    supplied = {"absent": None, "refuses": probe.guard, "stand-in": StandIn()}[guard]

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=supplied)
            assert await refusal(target, campaign.signed(BODY)) == 5
            assert snapshot(pworker_service) == before

    run(scenario())
    assert probe.calls == (1 if guard == "refuses" else 0)


@pytest.mark.parametrize("measured_for", ["another-installation", "another-campaign"])
def test_step_5_refuses_a_guard_measured_for_another_tuple(
    pworker_service: Any, campaign: Any, tmp_path: Path, measured_for: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    if measured_for == "another-installation":
        other = CommissioningTuple(pworker_service, bootstrap={"max_permit_ms": "86400001"})
    else:
        other = CommissioningTuple(pworker_service, campaign_generation="5")
    assert (other.bootstrap.digest == campaign.bootstrap.digest) is (
        measured_for == "another-campaign"
    )
    probe = GuardProbe(other)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=probe.guard)
            assert await refusal(target, campaign.signed(BODY)) == 5
            assert snapshot(pworker_service) == before

    run(scenario())
    assert probe.calls == 0


def test_the_guard_is_called_once_per_request_and_never_cached(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign) as parties:
            target = ingress(directory, parties, guard=guard.guard)
            await target.admit(**campaign.signed(BODY))
            await target.admit(**campaign.signed(BODY))
            guard.refuse()
            before = snapshot(pworker_service)
            assert await refusal(target, campaign.signed(BODY)) == 5
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 3


# --- Step 6: a running journal inside the permit interval -----------------------------------


@pytest.mark.parametrize("state", ["prepared", "closing"])
def test_step_6_refuses_unless_the_journal_is_running(
    pworker_service: Any, campaign: Any, tmp_path: Path, state: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    guard = GuardProbe(campaign)

    async def scenario() -> None:
        async with running(campaign, start=state != "prepared") as parties:
            if state == "closing":
                journal = parties.control.reservations.journal
                current = await journal.status()
                await journal.begin_close(current.generation)
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, campaign.signed(BODY)) == 6
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 1


# --- Step 7: an authenticated source inside the permit's scope ------------------------------


@pytest.mark.parametrize("fault", ["signature", "unknown-source", "stale", "outside-permit"])
def test_step_7_refuses_an_unauthenticated_or_out_of_scope_source(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    guard = GuardProbe(campaign)
    if fault == "signature":
        request = campaign.signed(BODY, secret="0" * 48)
    elif fault == "unknown-source":
        request = dict(campaign.signed(BODY), source_id="12121212-1212-4212-8212-121212121212")
    elif fault == "stale":
        request = campaign.signed(BODY, timestamp="1000")
    else:
        # Scoped by B and signed by its own secret, but not admitted by this permit.
        request = campaign.signed(BODY, source=1)

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=guard.guard)
            assert await refusal(target, request) == 7
            assert snapshot(pworker_service) == before

    run(scenario())
    assert guard.calls == 1


def test_step_7_refuses_a_permitted_source_whose_target_the_permit_excludes(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    value = campaign.ingress_file()
    value["sources"][0]["target_ref"] = campaign.bootstrap.as_dict()["target_scope"][1]
    directory = campaign.write_directory(
        tmp_path / "runtime", **{"commissioning_ingress.json": canonical(value)}
    )

    async def scenario() -> None:
        async with running(campaign) as parties:
            before = snapshot(pworker_service)
            target = ingress(directory, parties, guard=GuardProbe(campaign).guard)
            assert await refusal(target, campaign.signed(BODY)) == 7
            assert snapshot(pworker_service) == before

    run(scenario())


# --- Step 8: the atomic aggregate reservation -----------------------------------------------


def test_step_8_refuses_a_replayed_delivery_without_a_second_entry(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    request = campaign.signed(BODY)

    async def scenario() -> None:
        async with running(campaign) as parties:
            target = ingress(directory, parties, guard=GuardProbe(campaign).guard)
            await target.admit(**request)
            before = snapshot(pworker_service)
            assert await refusal(target, request) == 8
            assert snapshot(pworker_service) == before

    run(scenario())
    assert pworker_service.command("XLEN", campaign.stream) == 1


def test_an_exhausted_delivery_limit_stops_the_next_delivery_at_step_6(
    pworker_service: Any, tmp_path: Path
) -> None:
    """A limit of one: the first delivery reaches it and closes the campaign."""
    pworker_service.command("FLUSHDB")
    limited = CommissioningTuple(pworker_service, limits={"deliveries": "1"})
    limited.install()
    try:
        directory = limited.write_directory(tmp_path / "runtime")

        async def scenario() -> None:
            async with running(limited) as parties:
                target = ingress(directory, parties, guard=GuardProbe(limited).guard)
                await target.admit(**limited.signed(BODY))
                assert await refusal(target, limited.signed(BODY)) == 6

        run(scenario())
        assert pworker_service.command("XLEN", limited.stream) == 1
    finally:
        limited.remove()
        pworker_service.command("FLUSHDB")


# --- Step 9: the envelope and transport constraints -----------------------------------------


def test_step_9_refuses_an_oversized_payload_after_charging_without_dispatch(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    request = campaign.signed(b"x" * 32769)

    async def scenario() -> None:
        async with running(campaign) as parties:
            target = ingress(directory, parties, guard=GuardProbe(campaign).guard)
            assert await refusal(target, request) == 9

    run(scenario())
    after = snapshot(pworker_service)
    reservations = [
        json.loads(value)
        for key, (_, value, _) in after.items()
        if key.startswith("protected:commissioning:reservation:")
    ]
    # Charged before the envelope check, never refunded, and never dispatched.
    assert [r["status"] for r in reservations] == ["reserved"]
    assert not any(key.startswith("protected:commissioning:admission:") for key in after)
    assert campaign.stream not in after


# --- HTTP application -----------------------------------------------------------------------


def test_the_explicit_app_admits_signed_requests_and_refuses_with_one_fixed_answer(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_commissioning_ingress.app import create_commissioning_app
    from curie_protected_hooks.commissioning_campaign_records import parse_campaign_admission

    directory = campaign.write_directory(tmp_path / "runtime")

    def headers(request: dict[str, Any]) -> dict[str, str]:
        return {
            "X-Curie-Timestamp": request["timestamp"],
            "X-Curie-Delivery-Id": request["delivery_id"],
            "X-Curie-Signature-256": request["signature"],
        }

    async def scenario() -> None:
        async with running(campaign) as parties:
            app = create_commissioning_app(
                ingress(directory, parties, guard=GuardProbe(campaign).guard)
            )
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                good = campaign.signed(BODY)
                path = "/v1/commissioning/hooks/" + good["source_id"]
                accepted = await client.post(path, content=BODY, headers=headers(good))
                assert accepted.status_code == 202
                parse_campaign_admission(accepted.content)
                bad = campaign.signed(BODY, secret="0" * 48)
                for response in (
                    await client.post(path, content=BODY, headers=headers(bad)),
                    await client.post(
                        path + "?x=1", content=BODY, headers=headers(campaign.signed(BODY))
                    ),
                ):
                    assert response.status_code == 403
                    assert response.json() == {"detail": "Commissioning ingress refused"}
                assert (await client.get(path)).status_code == 405
                assert (await client.get("/docs")).status_code == 404

    run(scenario())
    assert pworker_service.command("XLEN", campaign.stream) == 1


def test_the_api_application_never_mounts_or_imports_the_commissioning_ingress() -> None:
    """The ordinary API process composition loads no commissioning ingress module or route."""
    probe = (
        "import sys\n"
        "from curie_api.main import create_app\n"
        "app = create_app()\n"
        "def paths(routes):\n"
        "    for route in routes:\n"
        "        yield getattr(route, 'path', '')\n"
        "        yield from paths(getattr(route, 'routes', ()))\n"
        "assert not [p for p in paths(app.routes) if 'commissioning' in p]\n"
        "assert not [m for m in sys.modules if m.startswith('curie_commissioning_ingress')]\n"
        "assert not [\n"
        "    m for m in sys.modules if m.startswith('curie_protected_hooks.commissioning')\n"
        "]\n"
        "print('isolated')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip().endswith("isolated")


def test_the_composition_is_the_predeclared_commissioning_ingress() -> None:
    from curie_commissioning_ingress.ingress import COMPOSITION
    from curie_protected_hooks.commissioning_provisioner import (
        COMMISSIONING_ENTRY_POINT,
        ENTRY_POINT_COMPOSITIONS,
        PRODUCTION_ENTRY_POINT,
    )

    assert COMPOSITION in ENTRY_POINT_COMPOSITIONS[COMMISSIONING_ENTRY_POINT]
    assert COMPOSITION not in ENTRY_POINT_COMPOSITIONS[PRODUCTION_ENTRY_POINT]
    assert copy.deepcopy(COMPOSITION) == "commissioning-ingress-v1"
