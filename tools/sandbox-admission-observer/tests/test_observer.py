"""Admission proof requires observed bounds and affirmative worker evidence."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

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


@pytest.fixture
def kubectl_inventory(helper, snapshot, monkeypatch):
    subject = helper._Collector("acme-context", "acme-run", [])
    resources = {
        "claims": ("sandboxclaims.extensions.agents.x-k8s.io", "extensions.agents.x-k8s.io",
                   "SandboxClaimList", "110"),
        "sandboxes": ("sandboxes.agents.x-k8s.io", "agents.x-k8s.io", "SandboxList", "220"),
    }
    inventories = {}
    endpoints = {}
    for source, (resource, group, kind, version) in resources.items():
        items = deepcopy(snapshot[source])
        for index, obj in enumerate(items):
            obj["metadata"]["resourceVersion"] = str(int(version) - 10 + index)
        items[1]["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
        inventories[source] = {
            "apiVersion": f"{group}/v1beta1", "kind": kind,
            "metadata": {"resourceVersion": version}, "items": items,
        }
        plural = resource.split(".", 1)[0]
        endpoints[f"/apis/{group}/v1beta1/namespaces/{subject.namespace}/{plural}"] = source
    calls = []
    watch_events = {source: [] for source in resources}

    def arguments(command):
        assert command[:5] == [
            "kubectl", "--context", "acme-context", "--namespace", subject.namespace,
        ]
        calls.append(command)
        assert command[5] == "get"
        return command[6:]

    def run(command, **_kwargs):
        args = arguments(command)
        if "--raw" in args:
            url = urlsplit(args[args.index("--raw") + 1])
            assert not url.query
            source = endpoints[url.path]
            document = inventories[source]
        else:
            source = next(key for key, value in resources.items() if value[0] == args[0])
            # Observed with kubectl: ordinary JSON get synthesizes List with RV "";
            # raw namespaced LIST preserves the server's typed list and nonempty RV.
            document = {
                "apiVersion": "v1", "kind": "List", "metadata": {"resourceVersion": ""},
                "items": inventories[source]["items"],
            }
        return helper.subprocess.CompletedProcess(command, 0, json.dumps(document), "")

    class Watch:
        def __init__(self, events, returncode=0):
            self.stdout = io.StringIO("".join(json.dumps(event) + "\n" for event in events))
            self.returncode = returncode

        def poll(self):
            return self.returncode

        def wait(self, **_kwargs):
            return self.returncode

    def popen(command, **_kwargs):
        args = arguments(command)
        # Observed kubectl get exits 1 for the unsupported --resource-version flag.
        if "--resource-version" in args:
            return Watch([], returncode=1)
        assert "--raw" in args
        url = urlsplit(args[args.index("--raw") + 1])
        query = parse_qs(url.query)
        assert query["watch"] == ["true"]
        source = endpoints[url.path]
        return Watch(watch_events[source])

    monkeypatch.setattr(helper.subprocess, "run", run)
    monkeypatch.setattr(helper.subprocess, "Popen", popen)
    try:
        yield subject, inventories, watch_events, calls
    finally:
        subject.close()


def test_raw_inventory_preserves_server_list_versions_for_both_watches(
    kubectl_inventory,
) -> None:
    subject, _, _, calls = kubectl_inventory
    subject._start_objects()
    assert subject.observer.max_claims == 2
    assert subject.observer.max_sandboxes == 2
    watches = []
    for command in calls:
        if "--raw" in command:
            url = urlsplit(command[command.index("--raw") + 1])
            query = parse_qs(url.query)
            if query:
                watches.append((
                    url.path.rsplit("/", 1)[1], query["watch"], query["resourceVersion"],
                ))
    assert watches == [
        ("sandboxclaims", ["true"], ["110"]),
        ("sandboxes", ["true"], ["220"]),
    ]


@pytest.mark.parametrize("source", ["claims", "sandboxes"])
@pytest.mark.parametrize("metadata", [{}, {"resourceVersion": ""}, {"resourceVersion": 110}])
def test_raw_inventory_rejects_missing_or_invalid_server_version(
    helper, kubectl_inventory, source, metadata,
) -> None:
    subject, inventories, _, _ = kubectl_inventory
    inventories[source]["metadata"] = metadata
    with pytest.raises(helper.ObservationError, match="invalid initial admission inventory"):
        subject._start_objects()


@pytest.mark.parametrize("event_type", ["ADDED", "DELETED", "ERROR"])
def test_watch_third_claim_and_api_errors_fail(helper, kubectl_inventory, event_type) -> None:
    subject, _, watch_events, _ = kubectl_inventory
    obj = {"metadata": {"name": "acme-c", "resourceVersion": "111"}}
    failure = "more than two claims or Sandboxes"
    if event_type == "ERROR":
        obj = {"apiVersion": "v1", "kind": "Status", "status": "Failure",
               "reason": "Expired", "code": 410}
        failure = "invalid admission inventory watch event"
    watch_events["claims"].append({
        "type": event_type,
        "object": obj,
    })
    subject._start_objects()
    assert subject.observer.max_claims == 2
    for reader in subject._readers:
        reader.join()
    while not subject._events.empty():
        event = subject._events.get_nowait()
        if event[:2] == ("data", "claims"):
            with pytest.raises(helper.ObservationError, match=failure):
                subject._event(*event)
            return
    pytest.fail("kubectl watch did not deliver its event")


@pytest.fixture
def preflight_bootstrap(helper, snapshot, kubectl_inventory, monkeypatch):
    subject, _, _, calls = kubectl_inventory
    objects = {
        "namespace": {"metadata": {"name": subject.namespace}},
        "crd": {"items": [
            {"status": {"conditions": [{"type": "Established", "status": "True"}]}}
            for _ in range(2)
        ]},
        "job": {},
        "deployment": {"status": {"availableReplicas": 1}},
    }
    original_get = subject._get

    def get(kind, *args):
        if kind == "job":
            assert args == (f"{subject.fullname}-preflight-controller",)
        return objects[kind] if kind in objects else original_get(kind, *args)

    worker_calls = []
    original_popen = helper.subprocess.Popen

    class WorkerStream:
        def __init__(self, document):
            self.stdout = io.StringIO(json.dumps(document) + "\n")

        def poll(self):
            return 0

        def wait(self, **_kwargs):
            return 0

    def popen(command, **kwargs):
        assert command[:5] == subject.kube
        operation = command[5]
        if operation == "get":
            return original_popen(command, **kwargs)
        assert operation in {"logs", "exec"}
        worker_calls.append(operation)
        document = (
            snapshot["worker_logs"][0] if operation == "logs" else {"ledger": snapshot["ledger"]}
        )
        return WorkerStream(document)

    monkeypatch.setattr(subject, "_get", get)
    monkeypatch.setattr(helper.subprocess, "Popen", popen)
    return subject, objects, calls, worker_calls


@pytest.mark.parametrize(
    "job",
    [
        {},
        {"status": {"active": 1}},
        {"status": {"conditions": [{"type": "Complete", "status": "False"}]}},
        {"status": {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}},
    ],
    ids=["absent", "incomplete", "complete-false", "failed"],
)
def test_established_crds_do_not_start_inventory_before_preflight_completion(
    preflight_bootstrap, job,
) -> None:
    subject, objects, calls, worker_calls = preflight_bootstrap
    objects["job"] = job
    subject._poll_startup()
    assert not calls
    assert not worker_calls


def test_null_startup_status_does_not_crash_or_start_inventory(preflight_bootstrap) -> None:
    # kube v1.31.4 accepts a CRD status patch that stores conditions as null.
    # kubectl 1.37.1 then returns that null, and dict.get's default does not
    # replace a present null, so the old poll raised TypeError.
    subject, objects, calls, worker_calls = preflight_bootstrap
    objects["namespace"] = {"metadata": None}
    subject._poll_startup()
    assert not subject._namespace_present
    objects["namespace"] = {"metadata": {"name": subject.namespace}}
    objects["crd"] = {
        "items": [
            {"status": {"conditions": None}},
            {"status": None},
            "not-an-object",
        ]
    }
    subject._poll_startup()
    objects["crd"] = {"items": None}
    subject._poll_startup()
    objects["crd"] = {
        "items": [
            {"status": {"conditions": [{"type": "Established", "status": "True"}]}},
            {"status": {"conditions": [{"type": "Established", "status": "True"}]}},
        ]
    }
    objects["job"] = {"status": None}
    subject._poll_startup()
    objects["job"] = {"status": {"conditions": None}}
    subject._poll_startup()
    subject._versions = {"claims": "1", "sandboxes": "1"}
    objects["deployment"] = {"status": None}
    subject._poll_startup()
    objects["deployment"] = {"status": {"availableReplicas": None}}
    subject._poll_startup()
    assert not calls
    assert not worker_calls


def test_unexpected_error_names_its_class_only(helper, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CURIE_SCENARIO_CONTEXT", "acme-context")
    monkeypatch.setenv("CURIE_SCENARIO_RUN_ID", "acme-run")

    def explode(_self) -> int:
        raise RuntimeError("secret-payload")

    monkeypatch.setattr(helper._Collector, "run", explode)
    assert helper.main(["--", sys.executable, "-c", "raise SystemExit(0)"]) == 1
    captured = capsys.readouterr()
    assert "admission observer could not complete (RuntimeError)" in captured.err
    assert "secret-payload" not in captured.err
    assert "secret-payload" not in captured.out


def test_completed_preflight_starts_inventory_and_worker_observation(preflight_bootstrap) -> None:
    subject, objects, calls, worker_calls = preflight_bootstrap
    # The chart hook completes only after the controller's informer cache sync:
    # charts/curie/templates/preflight-controller.yaml:107,404-462.
    objects["job"] = {"status": {"conditions": [{"type": "Complete", "status": "True"}]}}
    subject._poll_startup()
    for reader in subject._readers:
        reader.join()
    while not subject._events.empty():
        event = subject._events.get_nowait()
        if event[0] == "data":
            subject._event(*event)
    subject.observer.verify()
    assert subject.observer.max_claims == 2
    assert subject.observer.max_sandboxes == 2
    assert calls
    assert worker_calls == ["logs", "exec"]
