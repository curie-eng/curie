"""Actual registered TLS observer; no campaign qualification, @spec COMMISSIONING-SERIES-8."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import os
import secrets
import signal
import time
from typing import Any

import pytest
from curie_protected_hooks.commissioning_acl import commissioning_acl_rules
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_operator_custody import OperatorRunCustody
from curie_protected_hooks.commissioning_operator_records import parse_operator_recipe
from curie_protected_hooks.commissioning_run_records import parse_run_intent, parse_run_plan
from curie_protected_hooks.worker_transport import WorkerCredential

from .test_commissioning_evidence_observer import (
    _package,
    encoded,
    pworker_service,  # noqa: F401  Imported fixture owns its isolated broker.
)
from .test_commissioning_evidence_observer import (
    series as series_fixture,
)

series = series_fixture


def api() -> Any:
    name = "curie_worker.commissioning_registered_observer"
    assert importlib.util.find_spec(name) is not None, "registered actual observer not implemented"
    return importlib.import_module(name)


def test_registered_factory_exists_without_starting_resources() -> None:
    assert callable(api().RegisteredEvidenceObserver.connect)


@pytest.fixture
def registered(series, tmp_path):
    guard = json.loads(series.guard)
    username = guard["broker_acl"]["observer"]["credential"]["id"]
    password = secrets.token_hex(32)
    series.broker.command(
        "ACL",
        "SETUSER",
        username,
        "reset",
        "on",
        ">" + password,
        *commissioning_acl_rules("observer"),
    )
    record_tests = importlib.import_module(_package + ".test_commissioning_operator_records")
    run_tests = importlib.import_module(_package + ".test_commissioning_run_records")
    recipe_value = record_tests.recipe_value()
    for row in recipe_value["roles"]:
        if row["role"] == "evidence-observer":
            row["credential"] = series.bootstrap.as_dict()["evidence_observer_identity"]
        elif row["role"] == "acl-observer":
            row["credential"] = guard["broker_acl"]["observer"]["credential"]
    recipe = parse_operator_recipe(encoded(recipe_value))
    intent = run_tests.intent_value()
    intent["recipe_digest"] = recipe.digest
    ledger = OwnershipLedger.create(
        tmp_path / "installation", owner="curie-commission-aabbccddeeff"
    )
    parsed_intent = parse_run_intent(encoded(intent))
    run = OperatorRunCustody.create(ledger, parsed_intent, recipe)
    plan = run_tests.plan_value()
    plan["intent_digest"] = parsed_intent.digest
    plan["static_binding"].update(
        bootstrap=series.bootstrap.digest,
        guard=hashlib.sha256(series.guard).hexdigest(),
        execution_config=series.manifest.as_dict()["execution_config_digest"],
    )
    try:
        run.seal(parse_run_plan(encoded(plan)))
        yield run, WorkerCredential(username, password)
    finally:
        run.close()
        series.broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
        series.broker.command("ACL", "DELUSER", username)


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_actual_identity_precedes_reads_and_terminal_teardown_is_retained(series, registered):
    run, acl = registered

    async def scenario():
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        try:
            events = [e.as_dict() for e in run.operator_view().events]
            assert [row["kind"] for row in events] == ["intended", "bound", "intended", "bound"]
            bindings = [row for row in events if row["kind"] == "bound"]
            assert all(row["identity"]["host_process"]["pid"] for row in bindings)
            result = await observer.read(series.permit, series.inventory())
            assert result.state.permit.canonical_bytes == series.permit.canonical_bytes
            assert {row["identity"]["client_id"] for row in bindings} <= {
                c.client_id for c in result.clients
            }
        finally:
            await observer.close()
        assert [e.as_dict()["kind"] for e in run.operator_view().events][-2:] == [
            "closed",
            "closed",
        ]
        live = series.broker.command("CLIENT", "LIST").decode()
        assert all("id=" + row["identity"]["client_id"] + " " not in live for row in bindings)
        with pytest.raises(api().RegisteredObservationRefused):
            await observer.read(series.permit, series.inventory())

    asyncio.run(scenario())


@pytest.mark.parametrize("party", ["evidence", "acl"])
@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_fresh_rights_drift_closes_both_transports_without_reconnect(series, registered, party):
    run, acl = registered

    async def scenario():
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        username = series.principal.username if party == "evidence" else acl.username
        series.broker.command("ACL", "SETUSER", username, "+set", "~*")
        with pytest.raises(api().RegisteredObservationRefused):
            await observer.read(series.permit, series.inventory())
        await observer.close()
        assert (
            len([e for e in run.operator_view().events if e.as_dict()["kind"] == "intended"]) == 2
        )
        assert not any(e.as_dict()["kind"] == "uncertain" for e in run.operator_view().events)

    asyncio.run(scenario())


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_external_close_joins_owned_io_before_recording_closed(series, registered):
    run, acl = registered

    async def scenario():
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        before = set(asyncio.all_tasks())
        series.broker.command("CLIENT", "PAUSE", 1000, "ALL")
        pending = asyncio.create_task(observer.clients())
        try:
            await asyncio.sleep(0.02)
            assert not pending.done()
            await observer.close()
            # The API caller's own error handling can finish after close. All
            # additional tasks owned by the transport must already be joined.
            assert set(asyncio.all_tasks()) <= before | {pending}
            assert [e.as_dict()["kind"] for e in run.operator_view().events][-2:] == [
                "closed",
                "closed",
            ]
        finally:
            await asyncio.gather(pending, return_exceptions=True)
            await observer.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_cancelled_observation_is_joined_and_cannot_start_another_factory(series, registered):
    run, acl = registered

    async def scenario():
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        before = set(asyncio.all_tasks())
        series.broker.command("CLIENT", "PAUSE", 1000, "ALL")
        pending = asyncio.create_task(observer.clients())
        await asyncio.sleep(0.02)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await observer.close()
        assert set(asyncio.all_tasks()) <= before
        assert [e.as_dict()["kind"] for e in run.operator_view().events][-2:] == [
            "closed",
            "closed",
        ]
        with pytest.raises(api().RegisteredObservationRefused):
            await api().RegisteredEvidenceObserver.connect(
                run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_unbound_earlier_attempt_blocks_factory_before_another_connection(series, registered):
    run, acl = registered
    run.intend_observer("evidence-observer")
    before = int(series.broker.command("INFO", "stats")["total_connections_received"])

    async def scenario():
        opened = None
        try:
            with pytest.raises(api().RegisteredObservationRefused):
                opened = await api().RegisteredEvidenceObserver.connect(
                    run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
                )
        finally:
            if opened is not None:
                await opened.close()

    asyncio.run(scenario())
    assert len(run.operator_view().events) == 1
    assert int(series.broker.command("INFO", "stats")["total_connections_received"]) == before


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_issuer_credential_cannot_be_labelled_a_run_observer(series, registered):
    run, acl = registered
    credential = WorkerCredential(
        series.bootstrap.as_dict()["issuer_identity"]["id"], "example-not-a-real-password"
    )
    before = int(series.broker.command("INFO", "stats")["total_connections_received"])

    async def scenario():
        with pytest.raises(api().RegisteredObservationRefused):
            await api().RegisteredEvidenceObserver.connect(
                run, series.binding(), credential, acl, series.broker.ca_pem
            )

    asyncio.run(scenario())
    assert not run.operator_view().events
    assert int(series.broker.command("INFO", "stats")["total_connections_received"]) == before


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_close_cancellation_before_teardown_starts_still_joins_owned_transports(series, registered):
    run, acl = registered

    async def scenario():
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        ids = {
            e.as_dict()["identity"]["client_id"]
            for e in run.operator_view().events
            if e.as_dict()["kind"] == "bound"
        }
        owner = asyncio.current_task()
        assert owner is not None
        try:
            # Queue cancellation before close's newly created task gets a first
            # execution step. No transport or event-loop implementation is mocked.
            loop = asyncio.get_running_loop()

            def cancel_twice():
                owner.cancel()
                loop.call_soon(owner.cancel)

            loop.call_soon(cancel_twice)
            with pytest.raises(asyncio.CancelledError):
                await observer.close()
            live = series.broker.command("CLIENT", "LIST").decode()
            assert all("id=" + client + " " not in live for client in ids)
            assert [e.as_dict()["kind"] for e in run.operator_view().events][-2:] == [
                "closed",
                "closed",
            ]
            await observer.close()
        finally:
            # The red candidate has no usable close after the scheduling race.
            # Clean only the two fixture-owned users through the actual broker.
            for user in (series.principal.username, acl.username):
                series.broker.command("CLIENT", "KILL", "USER", user, "SKIPME", "yes")
            await asyncio.sleep(0.02)

    asyncio.run(scenario())


@pytest.mark.parametrize("pworker_service", [True], indirect=True)
def test_fork_inherited_tls_descriptors_refuse_a_false_parent_close_witness(series, registered):
    run, acl = registered

    async def scenario():
        def sockets(pid):
            result = set()
            directory = f"/proc/{pid}/fd"
            for fd in os.listdir(directory):
                try:
                    target = os.readlink(directory + "/" + fd)
                except FileNotFoundError:
                    continue
                if target.startswith("socket:"):
                    result.add(target)
            return result

        before = sockets(os.getpid())
        observer = await api().RegisteredEvidenceObserver.connect(
            run, series.binding(), series.principal.credential(), acl, series.broker.ca_pem
        )
        original_sockets = sockets(os.getpid()) - before
        assert len(original_sockets) == 2
        read_fd, write_fd = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(write_fd)
            os.read(read_fd, 1)
            os._exit(0)
        os.close(read_fd)
        try:
            refused = False
            try:
                await observer.close()
            except api().RegisteredObservationRefused:
                refused = True
            # TLS close_notify can remove the broker's logical clients while
            # the child still owns both original TCP socket descriptors.
            assert original_sockets <= sockets(child)
            assert refused, "parent descriptor closure cannot witness the child's live connection"
            assert not any(e.as_dict()["kind"] == "closed" for e in run.operator_view().events)
        finally:
            os.write(write_fd, b"x")
            os.close(write_fd)
            deadline = time.monotonic() + 5
            while True:
                waited, status = os.waitpid(child, os.WNOHANG)
                if waited == child:
                    assert os.waitstatus_to_exitcode(status) == 0
                    break
                if time.monotonic() >= deadline:
                    os.kill(child, signal.SIGKILL)
                    os.waitpid(child, 0)
                    pytest.fail("owned blocked child did not join")
                await asyncio.sleep(0.01)
        with pytest.raises(api().RegisteredObservationRefused):
            await observer.close()

    asyncio.run(scenario())
