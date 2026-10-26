"""Actual pinned v2 host storage, @spec COMMISSIONING-SERIES-8."""

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
import time
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_run_authority import (
    RunCustodyRefused,
    open_run_for_cleanup,
)
from curie_protected_hooks.commissioning_run_records import parse_run_intent

from .test_commissioning_bootstrap import encoded
from .test_commissioning_operator_records import api as records
from .test_commissioning_operator_records import recipe_value
from .test_commissioning_records import permit_value
from .test_commissioning_run_records import intent_value


def api() -> Any:
    name = "curie_protected_hooks.commissioning_operator_custody"
    assert importlib.util.find_spec(name) is not None, "operator journal not implemented"
    return importlib.import_module(name)


@pytest.fixture
def ledger(tmp_path: Path) -> OwnershipLedger:
    return OwnershipLedger.create(
        tmp_path.resolve() / "installation", owner="curie-commission-aabbccddeeff"
    )


def create(ledger: OwnershipLedger) -> Any:
    recipe = records().parse_operator_recipe(encoded(recipe_value()))
    intent = intent_value()
    intent["recipe_digest"] = recipe.digest
    return api().OperatorRunCustody.create(ledger, parse_run_intent(encoded(intent)), recipe)


def test_attempt_is_durable_before_acknowledgement_and_is_not_refunded(ledger) -> None:
    module = api()
    with create(ledger) as run:
        first = run.intend_observer("evidence-observer")
        second = run.intend_observer("evidence-observer")
        assert first != second
        events = run.operator_view().events
        assert [e.as_dict()["intent_id"] for e in events] == [first, second]
        with pytest.raises(RunCustodyRefused):
            run.intend_observer("evidence-observer")
    with module.open_operator_run_for_cleanup(ledger) as recovered:
        assert len(recovered.operator_view().events) == 2
        assert not hasattr(recovered, "intend_observer")
    with pytest.raises(RunCustodyRefused):
        create(ledger)


def test_v1_reader_refuses_v2_instead_of_ignoring_history(ledger) -> None:
    with create(ledger):
        pass
    with pytest.raises(RunCustodyRefused):
        open_run_for_cleanup(ledger)


def test_recipe_must_be_committed_in_original_intent(ledger) -> None:
    module = api()
    recipe = records().parse_operator_recipe(encoded(recipe_value()))
    with pytest.raises(RunCustodyRefused):
        module.OperatorRunCustody.create(ledger, parse_run_intent(encoded(intent_value())), recipe)
    assert not (ledger.root / "commissioning-run").exists()


def test_same_exclusive_lock_covers_operator_history(ledger) -> None:
    module = api()
    with create(ledger) as run:
        run.intend_observer("acl-observer")
        with pytest.raises(RunCustodyRefused):
            module.open_operator_run_for_cleanup(ledger)


@pytest.mark.parametrize(
    "attack",
    [
        "missing-marker",
        "changed-marker",
        "changed-head",
        "extra-file",
        "copy",
        "replaced-directory",
    ],
)
def test_required_custody_identity_or_history_change_refuses(ledger, tmp_path, attack) -> None:
    module = api()
    with create(ledger) as run:
        run.intend_observer("evidence-observer")
        root = run.root
        if attack == "missing-marker":
            (root / "format").unlink()
        elif attack == "changed-marker":
            (root / "format").write_bytes(b"1")
        elif attack == "changed-head":
            (root / "operator-head").write_bytes(b"0" * 64)
        elif attack == "extra-file":
            (root / "ignored-passing-report").write_bytes(b"true")
        elif attack == "replaced-directory":
            (root / "operators").rename(root / "old-operators")
            (root / "operators").mkdir(mode=0o700)
        else:
            copied = tmp_path / "copied"
            shutil.copytree(ledger.root, copied)
            copied_ledger = OwnershipLedger.open(copied, owner=ledger.owner)
            with pytest.raises(RunCustodyRefused):
                module.open_operator_run_for_cleanup(copied_ledger)
            return
        with pytest.raises(RunCustodyRefused):
            run.intend_observer("acl-observer")


def test_operator_labels_never_enable_unrealized_publisher_or_admin(ledger) -> None:
    api()
    with create(ledger) as run:
        for role in (
            "administrator",
            "acceptance-publisher",
            "candidate",
            "qualification-publisher",
            "activation-publisher",
        ):
            with pytest.raises(RunCustodyRefused):
                run.intend_observer(role)
        assert not run.operator_view().events


def test_operator_event_content_is_retained_under_its_own_digest(ledger) -> None:
    api()
    with create(ledger) as run:
        run.intend_observer("evidence-observer")
        event = run.operator_view().events[0]
        raw = (run.root / "operators" / event.digest).read_bytes()
        assert raw == event.canonical_bytes
        assert json.loads(raw)["kind"] == "intended"


def identity_value() -> dict[str, Any]:
    return {
        "client_id": "123",
        "broker_identity": permit_value()["broker_identity"],
        "credential": {"id": "example-evidence-observer", "generation": "1"},
        "host_process": {
            "pid": "1",
            "start_ticks": "2",
            "pid_namespace_inode": "3",
            "boot_id": "33333333-3333-4333-8333-333333333333",
        },
    }


def test_bound_and_closed_records_do_not_create_a_cleanup_verdict(ledger) -> None:
    api()
    with create(ledger) as run:
        ticket = run.intend_observer("evidence-observer")
        bound = run._record_transport(ticket, kind="bound", identity=identity_value())
        closed = run._record_transport(ticket, kind="closed")
        assert closed.as_dict()["binding_digest"] == bound.digest
        assert [e.as_dict()["kind"] for e in run.operator_view().events] == [
            "intended",
            "bound",
            "closed",
        ]
        assert not hasattr(run.operator_view(), "cleanup_success")
        with pytest.raises(RunCustodyRefused):
            run._record_transport(ticket, kind="bound", identity=identity_value())


def test_wrong_credential_refuses_before_damaging_retained_history(ledger) -> None:
    api()
    with create(ledger) as run:
        ticket = run.intend_observer("evidence-observer")
        identity = identity_value()
        identity["credential"]["id"] = "example-issuer"
        with pytest.raises(RunCustodyRefused):
            run._record_transport(ticket, kind="bound", identity=identity)
        assert [e.as_dict()["kind"] for e in run.operator_view().events] == ["intended"]


def test_same_broker_connection_cannot_be_bound_to_a_second_attempt(ledger) -> None:
    api()
    with create(ledger) as run:
        first = run.intend_observer("evidence-observer")
        run._record_transport(first, kind="bound", identity=identity_value())
        run._record_transport(first, kind="closed")
        second = run.intend_observer("evidence-observer")
        with pytest.raises(RunCustodyRefused):
            run._record_transport(second, kind="bound", identity=identity_value())
        assert len(run.operator_view().events) == 4


def test_expired_run_cannot_open_another_attempt_but_retains_uncertain_close(ledger) -> None:
    module = api()
    recipe = records().parse_operator_recipe(encoded(recipe_value()))
    intent = intent_value()
    intent.update(recipe_digest=recipe.digest, max_elapsed_ms="1000")
    with module.OperatorRunCustody.create(ledger, parse_run_intent(encoded(intent)), recipe) as run:
        ticket = run.intend_observer("evidence-observer")
        time.sleep(1.01)
        with pytest.raises(RunCustodyRefused):
            run.intend_observer("acl-observer")
        run._record_transport(ticket, kind="uncertain")
        assert run.operator_view().events[-1].as_dict()["kind"] == "uncertain"


def test_cleanup_reader_validates_operator_history_even_for_run_status(ledger) -> None:
    module = api()
    with create(ledger) as run:
        run.intend_observer("evidence-observer")
    with module.open_operator_run_for_cleanup(ledger) as recovered:
        (ledger.root / "commissioning-run" / "operator-head").write_bytes(b"0" * 64)
        with pytest.raises(RunCustodyRefused):
            recovered.view()


_CHILD = """
import json, os, sys, threading
from pathlib import Path
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger
from curie_protected_hooks.commissioning_operator_records import parse_operator_recipe
from curie_protected_hooks.commissioning_operator_custody import (
    OperatorRunCustody, open_operator_run_for_cleanup,
)
from curie_protected_hooks.commissioning_run_records import parse_run_intent
from curie_protected_hooks.commissioning_run_storage import RunCustodyRefused
v = json.loads(sys.stdin.readline())
ledger = OwnershipLedger.open(Path(sys.argv[1]), owner=sys.argv[2])
run = OperatorRunCustody.create(
    ledger, parse_run_intent(v['intent'].encode()), parse_operator_recipe(v['recipe'].encode()),
)
"""


def child_payload() -> str:
    recipe = records().parse_operator_recipe(encoded(recipe_value()))
    intent = intent_value() | {"recipe_digest": recipe.digest}
    return (
        json.dumps({"intent": encoded(intent).decode(), "recipe": recipe.canonical_bytes.decode()})
        + "\n"
    )


def test_sigkill_leaves_the_unbound_attempt_cleanup_only(ledger) -> None:
    module = api()
    program = (
        _CHILD
        + """
run.intend_observer('evidence-observer')
print('durable-attempt', flush=True)
threading.Event().wait()
"""
    )
    child = subprocess.Popen(
        [sys.executable, "-c", program, str(ledger.root), ledger.owner],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdin is not None and child.stdout is not None
        child.stdin.write(child_payload())
        child.stdin.flush()
        assert select.select([child.stdout], [], [], 15)[0]
        assert child.stdout.readline().strip() == "durable-attempt"
        head = (ledger.root / "commissioning-run" / "operator-head").read_bytes()
        with pytest.raises(RunCustodyRefused):
            module.open_operator_run_for_cleanup(ledger)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=15)
        with module.open_operator_run_for_cleanup(ledger) as recovered:
            assert recovered.operator_view().events[-1].as_dict()["kind"] == "intended"
            assert (ledger.root / "commissioning-run" / "operator-head").read_bytes() == head
            assert not hasattr(recovered, "intend_observer")
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=15)


def test_forked_operator_handle_cannot_use_or_unlock_parent_custody(ledger) -> None:
    api()
    program = (
        _CHILD
        + """
child = os.fork()
if child == 0:
    try:
        run.intend_observer('evidence-observer')
    except RunCustodyRefused:
        run.close()
        os._exit(0)
    os._exit(2)
_, status = os.waitpid(child, 0)
assert os.waitstatus_to_exitcode(status) == 0
try:
    recovery = open_operator_run_for_cleanup(ledger)
except RunCustodyRefused:
    pass
else:
    recovery.close()
    raise AssertionError('fork released parent lock')
run.intend_observer('evidence-observer')
assert len(run.operator_view().events) == 1
run.close()
print('fork-refused-parent-retained')
"""
    )
    result = subprocess.run(
        [sys.executable, "-c", program, str(ledger.root), ledger.owner],
        input=child_payload(),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "fork-refused-parent-retained"
