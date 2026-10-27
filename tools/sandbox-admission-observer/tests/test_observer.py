"""Admission proof requires observed bounds and affirmative worker evidence."""

from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def helper() -> Any:
    path = Path(__file__).resolve().parents[1] / "run.py"
    spec = importlib.util.spec_from_file_location("sandbox_admission_observer", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def snapshot() -> dict[str, Any]:
    resources = {
        "pods": "1",
        "requests.cpu": "250m",
        "requests.memory": "256Mi",
        "limits.cpu": "1",
        "limits.memory": "1Gi",
    }
    return {
        "claims": [{"metadata": {"name": name}} for name in ("acme-a", "acme-b")],
        "sandboxes": [{"metadata": {"name": name}} for name in ("acme-a", "acme-b")],
        "ledger": {
            name: {"state": "bound", "resources": dict(resources), "owner_token": name}
            for name in ("acme-a", "acme-b")
        },
        "worker_logs": [{"message": "sandbox admission refused", "evidence_type": "admission"}],
    }


def test_two_bound_entries_and_typed_refusal_complete_proof(helper, snapshot) -> None:
    observer = helper.AdmissionObserver()
    original = deepcopy(snapshot)
    observer.observe(claims=[], sandboxes=[], ledger=None, worker_logs=[])
    observer.observe(**snapshot)
    observer.observe(claims=[], sandboxes=[], ledger={}, worker_logs=[])
    observer.verify()
    assert snapshot == original


@pytest.mark.parametrize("kind", ["claims", "sandboxes"])
@pytest.mark.parametrize("deleting", [False, True])
def test_third_object_fails_without_owner_or_labels(helper, snapshot, kind, deleting) -> None:
    third = {"metadata": {"name": "acme-c"}}
    if deleting:
        third["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    snapshot[kind].append(third)
    with pytest.raises(helper.ObservationError):
        observer = helper.AdmissionObserver()
        observer.observe(**snapshot)
        observer.verify()


def test_retiring_reservation_still_counts(helper, snapshot) -> None:
    snapshot["ledger"]["acme-c"] = {
        **snapshot["ledger"]["acme-a"],
        "state": "retiring",
        "owner_token": "acme-c",
    }
    with pytest.raises(helper.ObservationError):
        observer = helper.AdmissionObserver()
        observer.observe(**snapshot)
        observer.verify()


@pytest.mark.parametrize("kind", ["claims", "sandboxes"])
def test_observed_ready_quota_rejection_fails(helper, snapshot, kind) -> None:
    # v0.5.0 emits Ready=False, ReconcilerError and "Error seen: " + the API error.
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/extensions/controllers/sandboxclaim_controller.go#L487-L546
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/controllers/sandbox_controller.go#L322-L325
    snapshot[kind][0]["status"] = {
        "conditions": [{
            "type": "Ready",
            "status": "False",
            "reason": "ReconcilerError",
            "message": 'Error seen: pods "acme-a" is forbidden: exceeded quota: acme-quota, '
                       "requested: pods=1, used: pods=2, limited: pods=2",
        }]
    }
    with pytest.raises(helper.ObservationError):
        observer = helper.AdmissionObserver()
        observer.observe(**snapshot)
        observer.verify()


@pytest.mark.parametrize("missing", ["ledger", "worker_logs", "claims", "sandboxes"])
def test_final_verification_requires_each_evidence_source(helper, snapshot, missing) -> None:
    snapshot[missing] = None if missing == "ledger" else []
    observer = helper.AdmissionObserver()
    observer.observe(**snapshot)
    with pytest.raises(helper.ObservationError):
        observer.verify()


@pytest.mark.parametrize(
    "record",
    [
        {"message": "request failed", "evidence_type": "admission"},
        {"message": "sandbox admission refused", "evidence_type": "quota"},
    ],
)
def test_untyped_or_unrelated_logs_do_not_prove_admission(helper, snapshot, record) -> None:
    snapshot["worker_logs"] = [record]
    observer = helper.AdmissionObserver()
    observer.observe(**snapshot)
    with pytest.raises(helper.ObservationError):
        observer.verify()


@pytest.mark.parametrize("source", ["claims", "ledger", "worker_logs"])
def test_failure_reports_never_echo_objects_or_worker_log_contents(
    helper, snapshot, capsys, source
) -> None:
    marker = "untrusted-payload-canary"
    if source == "claims":
        snapshot["claims"].append({"metadata": {"name": marker}, "credentials": marker})
    elif source == "ledger":
        snapshot["ledger"][marker] = {**snapshot["ledger"]["acme-a"], "owner_token": marker}
    else:
        snapshot["worker_logs"] = [{"message": marker, "authorization": marker}]
    with pytest.raises(helper.ObservationError) as error:
        observer = helper.AdmissionObserver()
        observer.observe(**snapshot)
        observer.verify()
    captured = capsys.readouterr()
    assert marker not in str(error.value) + captured.out + captured.err


@pytest.fixture
def collector(helper, snapshot, monkeypatch) -> tuple[Any, dict[str, Any]]:
    objects = {
        kind: {"metadata": {"name": "acme-owned"}}
        for kind in ("namespace", "deployment", "statefulset")
    }
    subject = helper._Collector("acme-context", "acme-run", [])
    monkeypatch.setattr(subject, "_get", lambda kind, *_args: objects.get(kind, {"items": []}))
    subject._poll_startup()
    for source in ("claims", "sandboxes"):
        for obj in snapshot[source]:
            subject._event("data", source, {"type": "ADDED", "object": obj})
    subject._event("data", "ledger", {"ledger": snapshot["ledger"]})
    return subject, objects


@pytest.fixture(params=[("data", {"error": "ledger observation failed"}), ("end", 1)])
def ledger_event(request) -> tuple[str, str, Any]:
    return request.param[0], "ledger", request.param[1]


def test_ledger_failure_while_owned_objects_are_healthy_fails(
    helper, collector, snapshot, ledger_event
) -> None:
    subject, _ = collector
    subject._event("data", "logs", snapshot["worker_logs"][0])
    with pytest.raises(helper.ObservationError):
        subject._event(*ledger_event)


@pytest.mark.parametrize("owned", ["namespace", "deployment", "statefulset"])
@pytest.mark.parametrize(
    "teardown", [{}, {"metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}}]
)
def test_expected_ledger_closure_after_live_proof_and_owned_teardown(
    collector, snapshot, ledger_event, owned, teardown
) -> None:
    subject, objects = collector
    subject._event("data", "logs", snapshot["worker_logs"][0])
    objects[owned] = teardown
    subject._event(*ledger_event)
    subject.observer.verify()


@pytest.mark.parametrize("owned", ["namespace", "deployment", "statefulset"])
def test_owned_absence_does_not_replace_affirmative_live_proof(
    helper, collector, ledger_event, owned
) -> None:
    subject, objects = collector
    objects[owned] = {}
    with pytest.raises(helper.ObservationError):
        subject._event(*ledger_event)


def test_main_propagates_real_child_exit_without_observed_proof(helper, monkeypatch) -> None:
    monkeypatch.setenv("CURIE_SCENARIO_CONTEXT", "acme-context")
    monkeypatch.setenv("CURIE_SCENARIO_RUN_ID", "acme-run")
    monkeypatch.setattr(helper._Collector, "_get", lambda _self, *_args: {})
    assert helper.main(["--", sys.executable, "-c", "raise SystemExit(17)"]) == 17
