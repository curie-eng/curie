"""Commissioning journal and campaign floor on the pinned Valkey, @spec PROTECTED-COMMISSIONING-3.

Every case runs against a real owned TLS Valkey at the pinned image. Each
principal is installed from its product ACL recipe, so a case that passes
also proves the script runs under the identity the principals table names,
and the refusal cases prove the other identities cannot run it. A refusal
is also proved to change no key, value or TTL.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb
from .commissioning_journal_broker import (
    CAMPAIGN,
    COUNTERS,
    OTHER_CAMPAIGN,
    Campaign,
    broker_now_ms,
    journal_module,
    principals,
    run,
    snapshot,
    stored_journal,
)

pworker_service = wb.pworker_service
__all__ = ["principals"]


def refused(reason: str | None = None) -> Any:
    api = journal_module()
    return pytest.raises(api.CommissioningJournalRefused, match=reason)


async def issue(principals: Any, campaign: Campaign | None = None) -> Campaign:
    campaign = campaign or principals.campaign
    session = await principals.session("issuer")
    try:
        await journal_module().issue_permit(session, campaign.permit, **campaign.trusted)
    finally:
        await session.close()
    return campaign


async def journal(principals: Any, campaign: Campaign | None = None, **options: Any) -> Any:
    campaign = campaign or principals.campaign
    session = await principals.session("control", **options)
    return await journal_module().CommissioningJournal.open(
        session, campaign.permit, **campaign.trusted
    )


# --- Key names --------------------------------------------------------------------------


def test_keys_derive_from_the_permit_inside_their_acl_families(pworker_service) -> None:
    from curie_protected_hooks import commissioning_acl as acl

    api = journal_module()
    campaign = Campaign(pworker_service)
    keys = api.campaign_keys(campaign.permit)
    assert keys.permit == campaign.permit_key and keys.permit.startswith(acl.PERMIT)
    assert keys.journal == campaign.journal_key and keys.journal.startswith(acl.JOURNAL)
    assert keys.floor == campaign.floor_key and keys.floor.startswith(acl.JOURNAL)
    # The floor is per runtime: a later campaign generation shares it.
    later = Campaign(pworker_service, campaign_generation="2")
    assert api.campaign_keys(later.permit).floor == keys.floor
    assert api.campaign_keys(later.permit).journal != keys.journal
    for value in (None, b"{}", campaign.value):
        with pytest.raises(ValueError):
            api.campaign_keys(value)


# --- Issuer: SETNX permit write -----------------------------------------------------------


def test_issuer_writes_each_permit_once_without_ttl(pworker_service, principals) -> None:
    campaign = run(issue(principals))
    assert pworker_service.command("GET", campaign.permit_key) == campaign.permit.canonical_bytes
    assert pworker_service.command("PTTL", campaign.permit_key) == -1
    # The same bytes again are a replay, not a second write.
    before = snapshot(pworker_service)
    run(issue(principals))
    assert snapshot(pworker_service) == before
    # Different bytes under the same campaign key refuse; the stored permit is unchanged.
    changed = Campaign(pworker_service, limits={"deliveries": "5"})
    with refused("exists"):
        run(issue(principals, changed))
    assert snapshot(pworker_service) == before


def test_issuer_refuses_a_permit_at_or_below_the_campaign_floor(
    pworker_service, principals
) -> None:
    campaign = principals.campaign
    pworker_service.command("SET", campaign.floor_key, "1")
    before = snapshot(pworker_service)
    with refused("floor"):
        run(issue(principals))
    assert snapshot(pworker_service) == before
    run(issue(principals, Campaign(pworker_service, campaign_generation="2")))


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"manifest_digest": "0" * 64}, "binding"),
        ({"bootstrap_digest": "0" * 64}, "binding"),
        ({"runtime_generation": "9"}, "binding"),
        ({"expires_at_ms": "1"}, "binding"),
        ({"trusted_issuer": {"id": "commissioning/other", "generation": "1"}}, "principal"),
    ],
)
def test_issuer_validates_the_binding_before_writing(
    pworker_service, principals, change, reason
) -> None:
    trusted_change = {k: v for k, v in change.items() if k.startswith("trusted_")}
    permit_change = {k: v for k, v in change.items() if not k.startswith("trusted_")}
    if permit_change.get("expires_at_ms") == "1":
        permit_change = {"issued_at_ms": "0", "expires_at_ms": "1"}
    campaign = Campaign(pworker_service, **permit_change)
    trusted = dict(campaign.trusted, **trusted_change)

    async def attempt() -> None:
        session = await principals.session("issuer")
        try:
            await journal_module().issue_permit(session, campaign.permit, **trusted)
        finally:
            await session.close()

    with refused(reason):
        run(attempt())
    assert snapshot(pworker_service) == {}


@pytest.mark.parametrize(
    "role", ["control", "provisioner", "candidate_enqueue", "candidate_worker"]
)
def test_only_the_issuer_writes_a_permit(pworker_service, principals, role) -> None:
    campaign = principals.campaign
    with refused():
        run(issue_as(principals, role, campaign))
    client = principals.raw(role)
    try:
        with pytest.raises(NoPermissionError):
            client.execute_command("SETNX", campaign.permit_key, campaign.permit.canonical_bytes)
    finally:
        client.close()
    assert snapshot(pworker_service) == {}


async def issue_as(principals: Any, role: str, campaign: Campaign) -> None:
    session = await principals.session(role)
    try:
        await journal_module().issue_permit(session, campaign.permit, **campaign.trusted)
    finally:
        await session.close()


# --- Sessions -----------------------------------------------------------------------------


def test_session_refuses_a_credential_that_is_not_the_named_principal(
    pworker_service, principals
) -> None:
    api = journal_module()

    async def attempt() -> None:
        await api.CommissioningSession.connect(
            pworker_service.manifest(),
            principals.credential("issuer"),
            pworker_service.ca_pem,
            principal=principals.reference("control"),
        )

    with refused():
        run(attempt())


@pytest.mark.parametrize("role", ["issuer", "provisioner", "candidate_enqueue", "candidate_worker"])
def test_only_the_control_principal_opens_the_journal(pworker_service, principals, role) -> None:
    from curie_protected_hooks.commissioning_scripts import JOURNAL_SCRIPT

    campaign = run(issue(principals))
    before = snapshot(pworker_service)

    async def attempt() -> None:
        session = await principals.session(role)
        try:
            await journal_module().CommissioningJournal.open(
                session, campaign.permit, **campaign.trusted
            )
        finally:
            await session.close()

    with refused():
        run(attempt())
    # The script itself refuses every other identity, whatever the Python surface says.
    client = principals.raw(role)
    try:
        with pytest.raises((NoPermissionError, ResponseError)) as error:
            client.execute_command(
                "EVAL",
                JOURNAL_SCRIPT,
                3,
                campaign.permit_key,
                campaign.journal_key,
                campaign.floor_key,
                "prepare",
                campaign.permit.canonical_bytes,
                campaign.permit.digest,
                "0",
            )
        assert "NOPERM" in str(error.value) or "permission" in str(error.value).lower()
    finally:
        client.close()
    assert snapshot(pworker_service) == before


# --- Journal state machine ----------------------------------------------------------------


def test_lifecycle_moves_forward_by_compare_and_set(pworker_service, principals) -> None:
    run(issue(principals))
    campaign = principals.campaign

    async def scenario() -> list[Any]:
        j = await journal(principals)
        try:
            prepared = await j.prepare()
            running = await j.start(prepared.generation)
            active = await j.require_running()
            closing = await j.begin_close(running.generation)
            closed = await j.confirm_closed(closing.generation)
            return [prepared, running, active, closing, closed, await j.status()]
        finally:
            await j.close()

    prepared, running, active, closing, closed, status = run(scenario())
    assert [s.state for s in (prepared, running, closing, closed)] == [
        "prepared",
        "running",
        "closing",
        "closed",
    ]
    assert [s.generation for s in (prepared, running, closing, closed)] == [1, 2, 3, 4]
    assert active.state == "running" and active.generation == 2
    assert status == closed
    assert all(value == 0 for value in closed.counters.values())
    assert tuple(closed.counters) == COUNTERS
    assert closed.exhausted is None
    stored = stored_journal(pworker_service, campaign)
    assert stored["state"] == "closed" and stored["generation"] == "4"
    assert stored["permit_digest"] == campaign.permit.digest
    assert stored["broker_run_id"] == campaign.value["broker_identity"]["run_id"]
    # Retention: no TTL on permit, journal or floor.
    for key in (campaign.permit_key, campaign.journal_key, campaign.floor_key):
        assert pworker_service.command("PTTL", key) == -1


@pytest.mark.parametrize("verb", ["start", "begin_close", "confirm_closed", "fail"])
def test_a_stale_expected_generation_changes_nothing(pworker_service, principals, verb) -> None:
    run(issue(principals))

    async def scenario() -> None:
        j = await journal(principals)
        try:
            prepared = await j.prepare()
            if verb == "confirm_closed":
                prepared = await j.begin_close(prepared.generation)
            before = snapshot(pworker_service)
            for stale in (prepared.generation - 1, prepared.generation + 1):
                with refused("generation"):
                    await getattr(j, verb)(stale)
            assert snapshot(pworker_service) == before
        finally:
            await j.close()

    run(scenario())


def test_no_transition_ever_returns_to_running(pworker_service, principals) -> None:
    run(issue(principals))

    async def scenario() -> None:
        j = await journal(principals)
        try:
            state = await j.start((await j.prepare()).generation)
            state = await j.begin_close(state.generation)
            for current in ("closing", "closed"):
                assert state.state == current
                before = snapshot(pworker_service)
                with refused("state"):
                    await j.start(state.generation)
                with refused("state"):
                    await j.require_running()
                with refused("state"):
                    await j.begin_close(state.generation)
                with refused("exists"):
                    await j.prepare()
                assert snapshot(pworker_service) == before
                if current == "closing":
                    state = await j.confirm_closed(state.generation)
            with refused("state"):
                await j.fail(state.generation)
        finally:
            await j.close()

    run(scenario())


def test_fail_is_terminal_from_every_live_state(pworker_service, principals) -> None:
    for depth, generation in enumerate(("1", "2", "3")):
        campaign = Campaign(pworker_service, campaign_generation=generation)
        run(issue(principals, campaign))

        async def scenario(campaign: Campaign = campaign, depth: int = depth) -> None:
            j = await journal(principals, campaign)
            try:
                state = await j.prepare()
                if depth >= 1:
                    state = await j.start(state.generation)
                if depth >= 2:
                    state = await j.begin_close(state.generation)
                failed = await j.fail(state.generation)
                assert failed.state == "failed"
                assert failed.generation == state.generation + 1
                for verb in ("start", "begin_close", "confirm_closed", "fail"):
                    with refused("state"):
                        await getattr(j, verb)(failed.generation)
            finally:
                await j.close()

        run(scenario())


# --- Campaign floor ------------------------------------------------------------------------


def test_prepare_advances_the_floor_and_a_generation_is_never_reused(
    pworker_service, principals
) -> None:
    first = run(issue(principals))
    assert pworker_service.command("GET", first.floor_key) is None

    async def prepare(campaign: Campaign) -> Any:
        j = await journal(principals, campaign)
        try:
            return await j.prepare()
        finally:
            await j.close()

    run(prepare(first))
    assert pworker_service.command("GET", first.floor_key) == b"1"
    # Another campaign at the same generation, issued before the floor moved, refuses.
    reuse = Campaign(pworker_service, campaign_id=OTHER_CAMPAIGN)
    pworker_service.command("SET", reuse.permit_key, reuse.permit.canonical_bytes)
    before = snapshot(pworker_service)
    with refused("floor"):
        run(prepare(reuse))
    assert snapshot(pworker_service) == before
    # The issuer also refuses it now, from the durable floor.
    with refused("floor"):
        run(
            issue(
                principals,
                Campaign(pworker_service, campaign_id=OTHER_CAMPAIGN, campaign_generation="1"),
            )
        )
    # A higher generation advances the floor; a lower one afterwards refuses.
    third = run(issue(principals, Campaign(pworker_service, campaign_generation="3")))
    run(prepare(third))
    assert pworker_service.command("GET", first.floor_key) == b"3"
    second = Campaign(pworker_service, campaign_generation="2")
    pworker_service.command("SET", second.permit_key, second.permit.canonical_bytes)
    with refused("floor"):
        run(prepare(second))
    assert pworker_service.command("GET", first.floor_key) == b"3"
    assert pworker_service.command("PTTL", first.floor_key) == -1


def test_a_corrupt_floor_refuses(pworker_service, principals) -> None:
    campaign = run(issue(principals))
    for value in ("", "01", "-1", "x", "9223372036854775808"):
        pworker_service.command("SET", campaign.floor_key, value)
        before = snapshot(pworker_service)

        async def prepare() -> None:
            j = await journal(principals)
            try:
                await j.prepare()
            finally:
                await j.close()

        with refused("corrupt"):
            run(prepare())
        assert snapshot(pworker_service) == before


# --- Binding ------------------------------------------------------------------------------


def test_prepare_without_the_issued_permit_refuses(pworker_service, principals) -> None:
    async def prepare() -> None:
        j = await journal(principals)
        try:
            await j.prepare()
        finally:
            await j.close()

    with refused("binding"):
        run(prepare())
    assert snapshot(pworker_service) == {}


def test_the_stored_permit_must_be_the_session_permit(pworker_service, principals) -> None:
    run(issue(principals))
    other = Campaign(pworker_service, limits={"deliveries": "9"})
    assert other.permit_key == principals.campaign.permit_key
    before = snapshot(pworker_service)

    async def prepare() -> None:
        j = await journal(principals, other)
        try:
            await j.prepare()
        finally:
            await j.close()

    with refused("binding"):
        run(prepare())
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("swap", ["permit", "journal", "floor"])
def test_the_script_refuses_keys_not_derived_from_the_permit(
    pworker_service, principals, swap
) -> None:
    from curie_protected_hooks.commissioning_scripts import JOURNAL_SCRIPT

    campaign = run(issue(principals))
    other = Campaign(
        pworker_service,
        campaign_id=OTHER_CAMPAIGN,
        campaign_generation="2",
        runtime_id="44444444-4444-4444-8444-444444444444",
    )
    keys = {
        "permit": campaign.permit_key,
        "journal": campaign.journal_key,
        "floor": campaign.floor_key,
    }
    keys[swap] = getattr(other, swap + "_key")
    before = snapshot(pworker_service)
    client = principals.raw("control")
    try:
        reply = client.execute_command(
            "EVAL",
            JOURNAL_SCRIPT,
            3,
            keys["permit"],
            keys["journal"],
            keys["floor"],
            "prepare",
            campaign.permit.canonical_bytes,
            campaign.permit.digest,
            "0",
        )
    finally:
        client.close()
    assert reply == [b"refused", b"binding"]
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize(
    "corruption",
    [
        "not json",
        "[]",
        '{"state":"running"}',
        "extra",
        "numeric_generation",
        "counter_over_limit",
        "unknown_state",
        "foreign_digest",
        "foreign_run_id",
        "exhausted_while_running",
    ],
)
def test_a_corrupt_journal_refuses_and_is_not_rewritten(
    pworker_service, principals, corruption
) -> None:
    import json

    run(issue(principals))
    campaign = principals.campaign

    async def prepare() -> None:
        j = await journal(principals)
        try:
            state = await j.prepare()
            await j.start(state.generation)
        finally:
            await j.close()

    run(prepare())
    value = stored_journal(pworker_service, campaign)
    if corruption == "extra":
        value["extra"] = "1"
    elif corruption == "numeric_generation":
        value["generation"] = 2
    elif corruption == "counter_over_limit":
        value["counters"]["launches"] = "5"
    elif corruption == "unknown_state":
        value["state"] = "accepted"
    elif corruption == "foreign_digest":
        value["permit_digest"] = "0" * 64
    elif corruption == "foreign_run_id":
        value["broker_run_id"] = "0" * 40
    elif corruption == "exhausted_while_running":
        value["exhausted"] = "launches"
        value["counters"]["launches"] = "4"
    raw = {
        "not json": "not json",
        "[]": "[]",
        '{"state":"running"}': '{"state":"running"}',
    }.get(corruption, json.dumps(value))
    pworker_service.command("SET", campaign.journal_key, raw)
    before = snapshot(pworker_service)

    async def attempt() -> None:
        j = await journal(principals)
        try:
            with refused("corrupt"):
                await j.status()
            with refused("corrupt"):
                await j.begin_close(2)
            with refused("corrupt"):
                await j.require_running()
        finally:
            await j.close()

    run(attempt())
    assert snapshot(pworker_service) == before


# --- Expiry -------------------------------------------------------------------------------


def test_expiry_refuses_positive_work_while_close_and_status_remain(
    pworker_service, principals
) -> None:
    now = broker_now_ms(pworker_service)
    campaign = Campaign(
        pworker_service, issued_at_ms=str(now - 1000), expires_at_ms=str(now + 1500)
    )
    run(issue(principals, campaign))

    async def before_expiry() -> int:
        j = await journal(principals, campaign)
        try:
            return (await j.start((await j.prepare()).generation)).generation
        finally:
            await j.close()

    generation = run(before_expiry())
    while broker_now_ms(pworker_service) < now + 1500:
        time.sleep(0.1)

    async def after_expiry() -> Any:
        # A restarted control process still opens the expired campaign to close it.
        j = await journal(principals, campaign)
        try:
            before = snapshot(pworker_service)
            with refused("time"):
                await j.require_running()
            assert snapshot(pworker_service) == before
            assert (await j.status()).state == "running"
            closing = await j.begin_close(generation)
            return await j.confirm_closed(closing.generation)
        finally:
            await j.close()

    assert run(after_expiry()).state == "closed"


def test_prepare_and_start_refuse_outside_the_permit_interval(pworker_service, principals) -> None:
    now = broker_now_ms(pworker_service)
    early = Campaign(pworker_service, issued_at_ms=str(now + 60000), expires_at_ms=str(now + 90000))
    pworker_service.command("SET", early.permit_key, early.permit.canonical_bytes)

    async def prepare() -> None:
        j = await journal(principals, early)
        try:
            await j.prepare()
        finally:
            await j.close()

    before = snapshot(pworker_service)
    with refused("time"):
        run(prepare())
    assert snapshot(pworker_service) == before


# --- Restarts -----------------------------------------------------------------------------


def test_control_restart_resumes_closing_from_the_durable_journal(
    pworker_service, principals
) -> None:
    run(issue(principals))

    async def first_process() -> int:
        j = await journal(principals)
        try:
            state = await j.start((await j.prepare()).generation)
            return (await j.begin_close(state.generation)).generation
        finally:
            await j.close()

    generation = run(first_process())

    async def second_process() -> Any:
        j = await journal(principals)
        try:
            resumed = await j.status()
            assert resumed.state == "closing" and resumed.generation == generation
            with refused("state"):
                await j.start(resumed.generation)
            return await j.confirm_closed(resumed.generation)
        finally:
            await j.close()

    assert run(second_process()).state == "closed"


def test_broker_restart_refuses_the_campaign_and_leaves_only_close_or_fail(
    pworker_service, principals
) -> None:
    run(issue(principals))
    campaign = principals.campaign

    async def started() -> int:
        j = await journal(principals)
        try:
            return (await j.start((await j.prepare()).generation)).generation
        finally:
            await j.close()

    generation = run(started())
    # A prepared campaign and an issued, unprepared one share the same broker epoch.
    pending = run(issue(principals, Campaign(pworker_service, campaign_generation="2")))
    unprepared = run(issue(principals, Campaign(pworker_service, campaign_generation="3")))

    async def prepare_pending() -> None:
        j = await journal(principals, pending)
        try:
            await j.prepare()
        finally:
            await j.close()

    run(prepare_pending())
    old_manifest = campaign.manifest
    old_run_id = old_manifest.as_dict()["broker_identity"]["run_id"]
    retained = {k: pworker_service.command("GET", k) for k in pworker_service.command("KEYS", "*")}
    pworker_service.restart()
    principals.install()
    for key, value in retained.items():
        pworker_service.command("SET", key, value)
    assert pworker_service.command("INFO", "server")["run_id"] != old_run_id

    # Pinned sessions refuse the new broker epoch outright, for control and issuer.
    for role in ("control", "issuer"):
        with refused():
            run(principals.session(role, manifest=old_manifest))

    async def terminal() -> Any:
        j = await journal(principals, terminal=True, manifest=old_manifest)
        try:
            before = snapshot(pworker_service)
            with refused("terminal"):
                await j.require_running()
            with refused("terminal"):
                await j.start(generation)
            assert (await j.status()).state == "running"
            assert snapshot(pworker_service) == before
            closing = await j.begin_close(generation)
            return await j.fail(closing.generation)
        finally:
            await j.close()

    # The script itself refuses every positive step on the changed run_id.
    from curie_protected_hooks.commissioning_scripts import JOURNAL_SCRIPT

    before = snapshot(pworker_service)
    client = principals.raw("control")
    try:
        for target, verb, expected in (
            (campaign, "require_running", "0"),
            (pending, "start", "1"),
            (unprepared, "prepare", "0"),
        ):
            reply = client.execute_command(
                "EVAL",
                JOURNAL_SCRIPT,
                3,
                target.permit_key,
                target.journal_key,
                target.floor_key,
                verb,
                target.permit.canonical_bytes,
                target.permit.digest,
                expected,
            )
            assert reply == [b"refused", b"run_id"], verb
    finally:
        client.close()
    assert snapshot(pworker_service) == before
    assert run(terminal()).state == "failed"


def test_a_terminal_session_exposes_no_positive_verb(pworker_service, principals) -> None:
    run(issue(principals))

    async def scenario() -> None:
        j = await journal(principals, terminal=True)
        try:
            with refused("terminal"):
                await j.prepare()
            with refused("terminal"):
                await j.start(1)
            with refused("terminal"):
                await j.require_running()
        finally:
            await j.close()
        issuer = await principals.session("issuer", terminal=True)
        try:
            with refused("terminal"):
                await journal_module().issue_permit(
                    issuer, principals.campaign.permit, **principals.campaign.trusted
                )
        finally:
            await issuer.close()

    run(scenario())


def test_refusals_never_echo_inputs(pworker_service, principals) -> None:
    api = journal_module()
    error = api.CommissioningJournalRefused("binding")
    assert str(error) == "Commissioning journal refused: binding"
    assert CAMPAIGN not in repr(error)
    with pytest.raises(ValueError):
        api.CommissioningJournalRefused("anything else")
