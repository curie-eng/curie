"""The personal rollout hold (#5377 R9).

During a rolling API upgrade an older replica can serve the 0160 database and
expire the transcript of a fenced personal run when it settles. While the API
replicas of an installation run different pod templates, owner enrollment
writes (enroll, replace, reconnect) answer 503 ``personal_rollout_in_progress``
and write nothing; disconnect is never held, because it only reduces access.
Personal trusted admission is held the same way
(``test_personal_execution_admission.py``).

The owner routes run over HTTP with real principal sessions and real Postgres.
Kubernetes is faked behind ``rollout_hold.ApiPodSource`` (``FakeApiPods``,
which models a Deployment's ``pod-template-hash`` label) and the custody store.
Every refusal sits beside the accepted neighbour.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any

import personal_enrollment_support as support
import personal_execution_support as pe
import pytest
from curie_api import rollout_hold
from curie_api.config import get_settings
from curie_api.rollout_hold import KubernetesApiPodSource, RolloutHold, single_template
from fastapi.testclient import TestClient
from oidc_test_idp import TestIdP
from personal_enrollment_support import FakeApiPods, FakeCustodyStore, Owner, api_pod

HELD = (503, rollout_hold.HOLD_CODE)


def _run(held: Coroutine[Any, Any, bool]) -> bool:
    return asyncio.run(held)


@pytest.fixture
def owned(clean_db: None) -> Iterator[None]:
    pe.cleanup_owned()
    yield
    assert pe.cleanup_owned() == 0, "an owned tenant or principal survived"


@pytest.fixture
def idp(owned: None) -> Iterator[TestIdP]:
    with support.identity_provider() as provider:
        yield provider


@pytest.fixture
def store() -> FakeCustodyStore:
    return FakeCustodyStore()


@pytest.fixture
def pods() -> FakeApiPods:
    """This pod and its peer, both on one template until a test rolls one."""

    return FakeApiPods([api_pod("curie-api-a", "5d8f7c"), api_pod("curie-api-b", "5d8f7c")])


@pytest.fixture
def api(idp: TestIdP, store: FakeCustodyStore, pods: FakeApiPods) -> Iterator[TestClient]:
    with support.enrollment_app(store, hold=support.held_by(pods)) as client:
        yield client


@pytest.fixture
def owner(api: TestClient, idp: TestIdP) -> Owner:
    return support.seed_owner(support.seed_tenant(), issuer=idp.issuer)


def _rolling(pods: FakeApiPods) -> None:
    """The peer now runs the next template, as mid-way through a Deployment rollout."""

    pods.pods = [api_pod("curie-api-a", "5d8f7c"), api_pod("curie-api-c", "7b9e41")]


def test_enrollment_writes_are_held_while_api_replicas_run_mixed_templates(
    api: TestClient, owner: Owner, store: FakeCustodyStore, pods: FakeApiPods
) -> None:
    _rolling(pods)

    assert support.refusal(support.put(api, owner, "enroll")) == HELD
    assert support.header(owner) is None and support.custody(owner) == []
    assert store.calls == []
    assert support.body_of(support.status(api, owner))["state"] == "absent"

    # The rollout finishes: every serving API pod shares this pod's template.
    pods.pods = [api_pod("curie-api-a", "5d8f7c"), api_pod("curie-api-c", "5d8f7c")]
    assert support.body_of(support.put(api, owner, "enroll"))["generation"] == 1


def test_a_failed_api_pod_list_holds_and_writes_nothing(
    api: TestClient, owner: Owner, store: FakeCustodyStore, pods: FakeApiPods
) -> None:
    support.enrolled(api, owner)
    unchanged = (support.header(owner), support.custody(owner), store.names())
    pods.error = RuntimeError("the pod list failed with a body that is never read")

    assert support.refusal(support.put(api, owner, "replace", version=1)) == HELD
    assert (support.header(owner), support.custody(owner), store.names()) == unchanged
    assert store.count("create") == 1

    pods.error = None
    assert support.body_of(support.put(api, owner, "replace", version=1))["generation"] == 2


def test_disconnect_is_never_held_but_reconnect_is(
    api: TestClient, owner: Owner, store: FakeCustodyStore, pods: FakeApiPods
) -> None:
    support.enrolled(api, owner)
    _rolling(pods)
    calls = pods.calls

    disconnected = support.body_of(support.disconnect(api, owner, 1))

    assert (disconnected["state"], disconnected["generation"]) == ("inactive", 2)
    assert pods.calls == calls, "disconnect consulted the hold"
    assert store.names() == []
    assert support.refusal(support.put(api, owner, "reconnect", version=2)) == HELD
    assert support.header(owner)["active"] is False  # type: ignore[index]

    pods.pods = [api_pod("curie-api-a", "5d8f7c")]
    assert support.body_of(support.put(api, owner, "reconnect", version=2))["state"] == "active"


@pytest.mark.parametrize(
    ("listed", "held"),
    [
        ([api_pod("curie-api-a", "1"), api_pod("curie-api-b", "1")], False),
        ([api_pod("curie-api-a", "1"), api_pod("curie-api-b", "2")], True),
        ([api_pod("curie-api-a", "1"), api_pod("curie-api-b", "2", phase="Pending")], True),
        # A pod that stopped serving, or another release's API, is not compared.
        ([api_pod("curie-api-a", "1"), api_pod("curie-api-b", "2", phase="Succeeded")], False),
        ([api_pod("curie-api-a", "1"), api_pod("other-api-b", "2", instance="other")], False),
        # This pod must be listed, and must carry a template hash.
        ([api_pod("curie-api-b", "1")], True),
        ([api_pod("curie-api-a", "")], True),
    ],
    ids=[
        "one-template",
        "mixed-running",
        "mixed-pending",
        "stopped-peer",
        "other-release",
        "self-missing",
        "self-unhashed",
    ],
)
def test_only_serving_api_pods_of_this_release_are_compared(
    listed: list[rollout_hold.ApiPod], held: bool
) -> None:
    assert single_template(listed, "curie-api-a") is not held


def test_without_an_in_cluster_identity_the_hold_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    namespace_file = tmp_path / "namespace"
    monkeypatch.setattr(rollout_hold, "_SERVICE_ACCOUNT_NAMESPACE", namespace_file)
    monkeypatch.setenv("HOSTNAME", "curie-api-a")

    with support.environment({"CURIE_PERSONAL_ROLLOUT_HOLD": "true"}):
        missing = rollout_hold.build_rollout_hold(get_settings())
        assert (missing.enabled, missing.pods) == (True, None)
        assert _run(missing.held()) is True

        # In cluster: this pod's namespace and name select the real pod source.
        namespace_file.write_text("curie\n", encoding="utf-8")
        in_cluster = rollout_hold.build_rollout_hold(get_settings())
        assert isinstance(in_cluster.pods, KubernetesApiPodSource)
        assert (in_cluster.namespace, in_cluster.pod_name) == ("curie", "curie-api-a")

    with support.environment({"CURIE_PERSONAL_ROLLOUT_HOLD": "false"}):
        off = rollout_hold.build_rollout_hold(get_settings())
        assert off.enabled is False and _run(off.held()) is False
        # Personal admission consults the installed hold only when the setting is on.
        installed = RolloutHold(enabled=True)
        rollout_hold.install(installed)
        try:
            assert _run(rollout_hold.personal_rollout_held()) is False
        finally:
            rollout_hold.uninstall(installed)

    with support.environment({"CURIE_PERSONAL_ROLLOUT_HOLD": "true"}):
        # On, but no application installed a hold: held.
        assert _run(rollout_hold.personal_rollout_held()) is True
