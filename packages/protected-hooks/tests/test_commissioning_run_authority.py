"""Actual host custody, concurrency and crash refusal, @spec COMMISSIONING-SERIES-2."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import select
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_run_records import parse_run_intent, parse_run_plan

from .test_commissioning_bootstrap import encoded
from .test_commissioning_run_records import COUNTERS, intent_value, plan_value

OWNER = "curie-commission-aabbccddeeff"


def module() -> Any:
    name = "curie_protected_hooks.commissioning_run_authority"
    assert importlib.util.find_spec(name) is not None, "private run custody not implemented"
    return importlib.import_module(name)


@pytest.fixture
def ledger(tmp_path: Path) -> OwnershipLedger:
    return OwnershipLedger.create(tmp_path.resolve() / "installation", owner=OWNER)


def start(ledger: OwnershipLedger):
    api = module()
    run = api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value())))
    run.seal(parse_run_plan(encoded(plan_value())))
    run.start()
    return run


def test_first_envelope_is_durable_and_never_refunded(ledger) -> None:
    api = module()
    run = start(ledger)
    try:
        event = run.charge_first_trial()
        assert event.as_dict()["kind"] == "charged"
        assert event.as_dict()["ordinal"] == "1"
        assert dict(run.view().charged_limits) == {key: 1 for key in COUNTERS}
        with pytest.raises(api.RunCustodyRefused):
            run.charge_first_trial()
    finally:
        run.close()
    with api.open_run_for_cleanup(ledger) as recovered:
        assert dict(recovered.view().charged_limits) == {key: 1 for key in COUNTERS}
        assert recovered.view().events[-1].digest == event.digest
    with pytest.raises(api.RunCustodyRefused):
        api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value())))


def test_an_unsettled_trial_cannot_advance_or_create_another_custody(ledger) -> None:
    api = module()
    with start(ledger) as run:
        run.charge_first_trial()
        with pytest.raises(api.RunCustodyRefused):
            api.open_run_for_cleanup(ledger)
        with pytest.raises(api.RunCustodyRefused):
            api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value())))


def test_created_resource_history_cannot_start_a_new_run(ledger) -> None:
    api = module()
    ticket = ledger.intend("docker-network", "example-not-an-actual-network")
    ledger.bind(ticket, id="example-original-identity")
    with pytest.raises(api.RunCustodyRefused):
        api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value())))
    assert not (ledger.root / "commissioning-run").exists()


@pytest.mark.parametrize(
    "attack", ["missing-head", "writable-head", "symlink-head", "hardlink-head", "changed-head"]
)
def test_corrupt_or_unsafe_required_input_refuses_before_another_charge(
    ledger, tmp_path, attack
) -> None:
    api = module()
    run = start(ledger)
    head = run.root / "head"
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"a" * 64)
    try:
        if attack == "missing-head":
            head.unlink()
        elif attack == "writable-head":
            head.chmod(0o644)
        elif attack == "symlink-head":
            head.unlink()
            head.symlink_to(foreign)
        elif attack == "hardlink-head":
            os.link(head, tmp_path / "linked")
        else:
            head.write_bytes(b"0" * 64)
        with pytest.raises(api.RunCustodyRefused):
            run.charge_first_trial()
        assert foreign.read_bytes() == b"a" * 64
    finally:
        run.close()


def test_copied_custody_cannot_be_opened_as_the_original(ledger, tmp_path) -> None:
    api = module()
    with start(ledger) as run:
        run.charge_first_trial()
    copy = tmp_path.resolve() / "copied-installation"
    shutil.copytree(ledger.root, copy)
    copied = OwnershipLedger.open(copy, owner=OWNER)
    with pytest.raises(api.RunCustodyRefused):
        api.open_run_for_cleanup(copied)
    with pytest.raises(api.RunCustodyRefused):
        api.RunCustody.create(copied, parse_run_intent(encoded(intent_value())))


def test_short_deadline_refuses_positive_work_but_allows_status_and_close(ledger) -> None:
    import time

    api = module()
    value = intent_value() | {"max_elapsed_ms": "1"}
    run = api.RunCustody.create(ledger, parse_run_intent(encoded(value)))
    try:
        time.sleep(0.01)
        value = plan_value() | {
            "max_elapsed_ms": "1",
            "intent_digest": parse_run_intent(encoded(value)).digest,
        }
        with pytest.raises(api.RunCustodyRefused):
            run.seal(parse_run_plan(encoded(value)))
        assert run.view().events == ()
    finally:
        run.close()


def test_hard_killed_owner_cannot_resume_or_erase_the_durable_charge(ledger) -> None:
    api = module()
    program = r"""
import json, sys
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_run_records import parse_run_intent, parse_run_plan
from curie_protected_hooks.commissioning_run_authority import RunCustody
value = json.loads(sys.stdin.readline())
ledger = OwnershipLedger.open(__import__('pathlib').Path(sys.argv[1]), owner=sys.argv[2])
run = RunCustody.create(ledger, parse_run_intent(value['intent'].encode()))
run.seal(parse_run_plan(value['plan'].encode()))
run.start()
run.charge_first_trial()
print('durably-charged', flush=True)
__import__('threading').Event().wait()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", program, str(ledger.root), OWNER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdin is not None and child.stdout is not None
        child.stdin.write(
            json.dumps(
                {"intent": encoded(intent_value()).decode(), "plan": encoded(plan_value()).decode()}
            )
            + "\n"
        )
        child.stdin.flush()
        assert select.select([child.stdout], [], [], 15)[0], (
            "owned child did not reach its durable boundary"
        )
        assert child.stdout.readline().strip() == "durably-charged"
        head = (ledger.root / "commissioning-run" / "head").read_bytes()
        with pytest.raises(api.RunCustodyRefused):
            api.open_run_for_cleanup(ledger)
        os.kill(child.pid, signal.SIGKILL)
        assert child.wait(timeout=5) == -signal.SIGKILL
        with api.open_run_for_cleanup(ledger) as recovered:
            assert dict(recovered.view().charged_limits) == {key: 1 for key in COUNTERS}
            assert recovered.view().events[-1].as_dict()["kind"] == "charged"
        assert (ledger.root / "commissioning-run" / "head").read_bytes() == head
        with pytest.raises(api.RunCustodyRefused):
            api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value())))
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None:
                stream.close()


@pytest.mark.parametrize(
    "attack",
    [
        "orphan",
        "unfinished",
        "replaced-root",
        "replaced-lock",
        "replaced-records",
        "corrupt-record",
    ],
)
def test_ambiguous_or_replaced_custody_refuses(ledger, attack) -> None:
    api = module()
    run = start(ledger)
    root = run.root
    try:
        if attack == "orphan":
            (root / "records" / ("a" * 64)).write_bytes(b"unreferenced")
            (root / "records" / ("a" * 64)).chmod(0o600)
        elif attack == "unfinished":
            (root / ".head-interrupted").write_bytes(b"unfinished")
        elif attack == "replaced-root":
            root.rename(root.with_name("old-run"))
            shutil.copytree(root.with_name("old-run"), root)
        elif attack == "replaced-records":
            (root / "records").rename(root / "old-records")
            shutil.copytree(root / "old-records", root / "records")
            shutil.rmtree(root / "old-records")
        elif attack == "replaced-lock":
            (root / "lock").unlink()
            (root / "lock").touch(mode=0o600)
        else:
            digest = (root / "head").read_text()
            (root / "records" / digest).write_bytes(b"invalid")
        with pytest.raises(api.RunCustodyRefused):
            run.charge_first_trial()
    finally:
        run.close()
    with pytest.raises(api.RunCustodyRefused):
        api.open_run_for_cleanup(ledger)


def test_concurrent_callers_burn_only_one_envelope(ledger) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    api = module()
    with start(ledger) as run:
        barrier = Barrier(2)

        def charge():
            barrier.wait(timeout=5)
            try:
                return run.charge_first_trial()
            except api.RunCustodyRefused:
                return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(charge) for _ in range(2)]
            results = [future.result(timeout=10) for future in futures]
        assert sum(value is not None for value in results) == 1
        assert dict(run.view().charged_limits) == {key: 1 for key in COUNTERS}
        assert len(run.view().events) == 3


def test_forked_handle_is_revoked_without_unlocking_live_parent(ledger) -> None:
    # Exec first: the fork owner has no pytest/plugin threads or inherited mutexes.
    program = r"""
import json, os, sys
from pathlib import Path
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_run_records import parse_run_intent, parse_run_plan
from curie_protected_hooks.commissioning_run_authority import (
    RunCustody, RunCustodyRefused, open_run_for_cleanup,
)
value = json.loads(sys.stdin.read())
ledger = OwnershipLedger.open(Path(sys.argv[1]), owner=sys.argv[2])
with RunCustody.create(ledger, parse_run_intent(value['intent'].encode())) as run:
    run.seal(parse_run_plan(value['plan'].encode()))
    run.start()
    child = os.fork()
    if child == 0:
        try:
            try:
                run.charge_first_trial()
            except RunCustodyRefused:
                run.close()
                os._exit(0)
            os._exit(2)
        finally:
            os._exit(3)
    waited, status = os.waitpid(child, 0)
    assert waited == child and os.waitstatus_to_exitcode(status) == 0
    try:
        recovered = open_run_for_cleanup(ledger)
    except RunCustodyRefused:
        pass
    else:
        recovered.close()
        raise AssertionError('forked close unlocked the live parent')
    run.charge_first_trial()
    assert all(value == 1 for _, value in run.view().charged_limits)
print('fork-revoked-parent-retained', flush=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", program, str(ledger.root), OWNER],
        input=json.dumps(
            {
                "intent": encoded(intent_value()).decode(),
                "plan": encoded(plan_value()).decode(),
            }
        ),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "fork-revoked-parent-retained"


@pytest.mark.parametrize("attack", ["missing", "malformed", "writable"])
def test_changed_installation_ledger_refuses_positive_work(ledger, attack) -> None:
    api = module()
    with start(ledger) as run:
        required = ledger.root / "ownership.json"
        if attack == "missing":
            required.unlink()
        elif attack == "malformed":
            required.write_bytes(b"{}")
        else:
            required.chmod(0o644)
        with pytest.raises(api.RunCustodyRefused):
            run.charge_first_trial()


def test_legitimate_installation_creation_does_not_invalidate_live_custody(ledger) -> None:
    ticket = ledger.intend("docker-network", "example-intended-network")
    with start(ledger) as run:
        ledger.bind(ticket, id="example-created-identity")
        run.charge_first_trial()
        assert len(run.view().events) == 3


def test_required_fifo_refuses_without_blocking_close(ledger) -> None:
    program = r"""
import json, os, sys
from pathlib import Path
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_run_records import parse_run_intent, parse_run_plan
from curie_protected_hooks.commissioning_run_authority import RunCustody, RunCustodyRefused
value = json.loads(sys.stdin.read())
ledger = OwnershipLedger.open(Path(sys.argv[1]), owner=sys.argv[2])
with RunCustody.create(ledger, parse_run_intent(value['intent'].encode())) as run:
    run.seal(parse_run_plan(value['plan'].encode()))
    run.start()
    head = run.root / 'head'
    head.unlink()
    os.mkfifo(head, 0o600)
    try:
        run.charge_first_trial()
    except RunCustodyRefused:
        print('fifo-refused-and-closed', flush=True)
    else:
        raise AssertionError('FIFO admitted')
"""
    child = subprocess.Popen(
        [sys.executable, "-c", program, str(ledger.root), OWNER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        try:
            output, error = child.communicate(
                json.dumps(
                    {
                        "intent": encoded(intent_value()).decode(),
                        "plan": encoded(plan_value()).decode(),
                    }
                ),
                timeout=4,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("nonregular required input blocked refusal/close")
        assert child.returncode == 0, error
        assert output.strip() == "fifo-refused-and-closed"
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


@pytest.mark.parametrize("malformation", ["unhashable-kind", "deep-json"])
def test_malformed_nested_ledger_refuses_and_cannot_revive_after_restore(
    ledger, malformation
) -> None:
    api = module()
    with start(ledger) as run:
        required = ledger.root / "ownership.json"
        original = required.read_bytes()
        if malformation == "unhashable-kind":
            required.write_text(
                json.dumps(
                    {
                        "owner": OWNER,
                        "entries": [
                            {
                                "kind": [],
                                "key": "example",
                                "state": "intended",
                                "identity": {},
                            }
                        ],
                    }
                )
            )
        else:
            required.write_bytes(b"[" * 10000 + b"]" * 10000)
        try:
            with pytest.raises(api.RunCustodyRefused):
                run.charge_first_trial()
        finally:
            required.write_bytes(original)
        with pytest.raises(api.RunCustodyRefused):
            run.charge_first_trial()
        assert len(run.view().events) == 2


def test_unfinished_plan_seal_cannot_be_recovered_as_valid_custody(ledger) -> None:
    api = module()
    with api.RunCustody.create(ledger, parse_run_intent(encoded(intent_value()))) as run:
        run.seal(parse_run_plan(encoded(plan_value())))
        root = run.root
    # The on-disk boundary after plan flush but before prepared-event publication.
    head = (root / "head").read_text()
    (root / "head").write_bytes(b"0" * 64)
    (root / "records" / head).unlink()
    with pytest.raises(api.RunCustodyRefused):
        api.open_run_for_cleanup(ledger)
