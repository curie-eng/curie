"""KubernetesSandboxClient.create_claim payload shape.

The agent-sandbox controller injects per-claim env with no ``containerName`` into
only the first main container. The bundle ref must ALSO be targeted at the init
containers by name, or a Kubernetes runner boots an empty plugin dir. These tests
assert the emitted SandboxClaim env, so the fix is mutation-honest: dropping the
named entries fails ``test_bundle_ref_targets_init_containers_by_name``.
"""

from __future__ import annotations

import copy
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_worker.sandbox import QuotaRejection
from curie_worker.sandbox.k8s import (
    BUNDLE_INIT_CONTAINERS,
    WORKSPACE_INIT_CONTAINERS,
    KubernetesSandboxClient,
    _claim_view,
)

# Captured live with `kubectl get sandboxclaims` in JSON form. The controller's
# direct Pod admission failure emitted no Warning quota Event, so this Ready
# condition is the machine readable cluster evidence for the rejection.
LIVE_QUOTA_REJECTED_CLAIM: dict[str, Any] = {
    "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
    "kind": "SandboxClaim",
    "metadata": {
        "annotations": {
            "agents.x-k8s.io/controller-first-observed-at": "2026-08-19T10:24:42.828003465Z"
        },
        "creationTimestamp": "2026-08-19T10:24:42Z",
        "generation": 1,
        "name": "acme-claim",
        "namespace": "acme-quota",
        "resourceVersion": "12345",
        "uid": "00000000-0000-0000-0000-000000000000",
    },
    "status": {
        "conditions": [
            {
                "lastTransitionTime": "2026-08-19T10:24:42Z",
                "message": (
                    'Error seen: pods "acme-claim" is forbidden: exceeded quota: '
                    "acme-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=0, "
                    "limited: limits.cpu=1m"
                ),
                "observedGeneration": 1,
                "reason": "ReconcilerError",
                "status": "False",
                "type": "Ready",
            }
        ],
        "sandbox": {"name": "acme-claim"},
    },
}

ISSUE_QUOTA_REJECTION_MESSAGE = (
    'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
    "curie-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=8, "
    "limited: limits.cpu=8"
)


class _FakeApi:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: dict[str, Any]
    ) -> None:
        self.created.append(body)


def _client(api: _FakeApi) -> KubernetesSandboxClient:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    client._api = api  # type: ignore[attr-defined]
    client._namespace = "test-ns"  # type: ignore[attr-defined]
    return client


def _env_entries(api: _FakeApi) -> list[dict[str, str]]:
    return api.created[0]["spec"]["env"]


def test_bundle_ref_targets_init_containers_by_name() -> None:
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={"CURIE_BUNDLE_REF": "bundles/x.tar.gz", "CURIE_BUDGET": "{}"},
    )
    entries = _env_entries(api)

    # The main runner still receives the ref (unnamed entry).
    assert {"name": "CURIE_BUNDLE_REF", "value": "bundles/x.tar.gz"} in entries

    # And each bundle init container receives it by explicit containerName.
    named = {
        (e["containerName"], e["name"]): e["value"] for e in entries if "containerName" in e
    }
    for container in BUNDLE_INIT_CONTAINERS:
        assert named[(container, "CURIE_BUNDLE_REF")] == "bundles/x.tar.gz"


def test_bundle_version_reaches_the_runner_not_the_init_containers() -> None:
    """#2174: the agent-readable version is runner env, not an object-store key.

    Init containers still receive only CURIE_BUNDLE_REF (the fetch key). The
    version_label is an unnamed main-container entry so the sandboxed agent
    can read it, matching the docker substrate's forward of the same key.
    """

    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={
            "CURIE_BUNDLE_REF": "bundles/x.tar.gz",
            "CURIE_BUNDLE_VERSION": "abc123def456",
            "CURIE_BUDGET": "{}",
        },
    )
    entries = _env_entries(api)

    assert {"name": "CURIE_BUNDLE_VERSION", "value": "abc123def456"} in entries
    named = {
        (e["containerName"], e["name"]): e["value"] for e in entries if "containerName" in e
    }
    assert all(key[1] != "CURIE_BUNDLE_VERSION" for key in named)


def test_no_named_env_without_bundle_ref() -> None:
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1", pool="pool", env={"CURIE_BUDGET": "{}", "CURIE_SESSION_ID": "s"}
    )
    entries = _env_entries(api)
    assert entries  # the main-container env is still present
    assert all("containerName" not in e for e in entries)


def test_workspace_capability_targets_only_workspace_init_containers() -> None:
    api = _FakeApi()
    workspace_ref = "opaque-presigned-workspace-reference"
    workspace_sha256 = "a" * 64
    _client(api).create_claim(
        "claim-workspace",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_WORKSPACE_REF": workspace_ref,
            "CURIE_WORKSPACE_SHA256": workspace_sha256,
        },
    )
    entries = _env_entries(api)

    unnamed = {entry["name"] for entry in entries if "containerName" not in entry}
    assert "CURIE_WORKSPACE_REF" not in unnamed
    assert "CURIE_WORKSPACE_SHA256" not in unnamed
    named = {
        (entry["containerName"], entry["name"]): entry["value"]
        for entry in entries
        if "containerName" in entry
    }
    for container in WORKSPACE_INIT_CONTAINERS:
        assert named[(container, "CURIE_WORKSPACE_REF")] == workspace_ref
        assert named[(container, "CURIE_WORKSPACE_SHA256")] == workspace_sha256


def test_credential_is_never_written_to_the_claim() -> None:
    # The SandboxClaim env is value-only, so the secret must not be persisted on
    # the claim; the template's secretKeyRef supplies it to the runner instead.
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={"CURIE_BUDGET": "{}", "CURIE_CREDENTIALS": "super-secret-token"},
    )
    entries = _env_entries(api)
    assert all(e.get("name") != "CURIE_CREDENTIALS" for e in entries)
    assert all("super-secret-token" not in e.get("value", "") for e in entries)
    # The rest of the boot env is still written.
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries


def test_host_credentials_are_never_written_to_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    denied_names = {
        "POSTGRES_PASSWORD",
        "DATABASE_URL",
        "VALKEY_PASSWORD",
        "SLACK_BOT_TOKEN",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "CURIE_API_KEY",
        "LANGFUSE_SECRET_KEY",
        "CURIE_ADAPTER_CREDENTIALS",
        "CURIE_SEALING_PRIVATE_KEY",
        "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
    }
    for name in denied_names:
        monkeypatch.setenv(name, "placeholder")
    monkeypatch.setenv("CURIE_BUDGET", "{}")
    monkeypatch.setenv("CURIE_CREDENTIALS", "placeholder")
    monkeypatch.delenv("CURIE_CONNECTOR_SECRET_KEYS", raising=False)

    api = _FakeApi()
    _client(api).create_claim("claim-credentials", pool="pool", env=os.environ)

    claim_env_names = {entry["name"] for entry in _env_entries(api)}
    assert denied_names.isdisjoint(claim_env_names)
    assert "CURIE_BUDGET" in claim_env_names
    assert "CURIE_CREDENTIALS" not in claim_env_names


def test_runner_token_is_a_plaintext_env_entry_credential_excluded() -> None:
    # The per-sandbox runner token intentionally rides the generic env loop as a
    # plaintext {name, value} entry on the claim (there is no secretKeyRef path
    # for it), while the model credential stays excluded.
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_RUNNER_TOKEN": "tok-26",
            "CURIE_CREDENTIALS": "super-secret-token",
        },
    )
    entries = _env_entries(api)
    assert {"name": "CURIE_RUNNER_TOKEN", "value": "tok-26"} in entries
    assert all(e.get("name") != "CURIE_CREDENTIALS" for e in entries)


def test_claim_view_surfaces_the_creation_timestamp() -> None:
    # The reaper's bind-window grace is only as good as the age the real
    # cluster adapter reports. An adapter that never surfaced the timestamp
    # would leave every claim at unknown age, which spares every orphan and
    # disables reaping on the tier that actually runs in production -- and the
    # substrate's own tests, which drive an in-memory fake, would not see it.
    view = _claim_view(
        {"metadata": {"name": "claim-1", "creationTimestamp": "2026-08-16T12:00:00Z"}}
    )
    created = view.created_at
    assert created is not None
    assert created.utcoffset() == timedelta(0)  # tz-aware UTC, never naive
    assert created == datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

    # A zoneless instant is read AS UTC, never as host-local: interpreting it
    # in the host's zone shifts the claim's age by that offset, and on a
    # west-of-UTC host that makes a young claim look old enough to reap.
    naive = _claim_view(
        {"metadata": {"name": "claim-3", "creationTimestamp": "2026-08-16T12:00:00"}}
    )
    assert naive.created_at == datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

    # A claim the cluster gave no creation instant for reads as unknown age.
    absent = _claim_view({"metadata": {"name": "claim-2"}})
    assert absent.created_at is None

    # And an unparseable one, so one malformed object cannot raise inside the
    # maintenance tick and silently end reaping.
    malformed = _claim_view(
        {"metadata": {"name": "claim-4", "creationTimestamp": "not-a-timestamp"}}
    )
    assert malformed.created_at is None


def test_claim_view_classifies_live_resource_quota_condition() -> None:
    view = _claim_view(copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM))

    assert view.name == "acme-claim"
    assert view.ready is False
    assert view.sandbox_name == "acme-claim"
    assert view.created_at == datetime(2026, 8, 19, 10, 24, 42, tzinfo=UTC)
    assert view.quota_rejection == QuotaRejection(
        quota_name="acme-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="0",
        hard="1m",
    )
    assert view.ready_reason == "ReconcilerError"
    assert view.ready_message == LIVE_QUOTA_REJECTED_CLAIM["status"]["conditions"][0][
        "message"
    ]


def test_claim_view_classifies_issue_example_at_eight_of_eight() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = ISSUE_QUOTA_REJECTION_MESSAGE

    view = _claim_view(claim)

    assert view.quota_rejection == QuotaRejection(
        quota_name="curie-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="8",
        hard="8",
    )
    assert view.ready_reason == "ReconcilerError"
    assert view.ready_message == ISSUE_QUOTA_REJECTION_MESSAGE


@pytest.mark.parametrize(
    ("field", "value"),
    [("status", "True"), ("type", "Provisioned")],
)
def test_quota_message_requires_failed_ready_condition(field: str, value: str) -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0][field] = value

    assert _claim_view(claim).quota_rejection is None


def test_quota_message_with_another_reason_is_not_classified() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["reason"] = "ProvisioningFailed"

    assert _claim_view(claim).quota_rejection is None


def test_reconciler_error_without_exceeded_quota_clause_is_not_classified() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = (
        'Error seen: pods "curie-thread-example" is forbidden: User "system:serviceaccount:'
        'curie1572:worker" cannot create resource "pods"'
    )

    assert _claim_view(claim).quota_rejection is None


@pytest.mark.parametrize(
    "message",
    [
        (
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=8"
        ),
        (
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: limits.cpu, used: limits.cpu=8, "
            "limited: limits.cpu=8"
        ),
        (
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: requests.cpu=1, used: limits.cpu=8, "
            "limited: limits.cpu=8"
        ),
    ],
    ids=["missing_map", "malformed_map", "nonoverlapping_maps"],
)
def test_incomplete_quota_maps_are_not_classified(message: str) -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = message

    assert _claim_view(claim).quota_rejection is None


def test_quota_parser_selects_first_common_resource_in_sorted_order() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = (
        'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
        "curie-sandbox-quota, requested: requests.memory=1Gi,limits.cpu=1, "
        "used: limits.cpu=8,requests.memory=2Gi, limited: requests.memory=4Gi,limits.cpu=8"
    )

    assert _claim_view(claim).quota_rejection == QuotaRejection(
        quota_name="curie-sandbox-quota",
        resource="limits.cpu",
        requested="1",
        used="8",
        hard="8",
    )


def test_connector_secrets_are_never_written_to_the_claim() -> None:
    # Per-agent connector secrets (#429) ride the substrate-agnostic boot env by
    # value, but the value-only claim CR would persist them in plaintext in etcd.
    # The binding marks their keys in CURIE_CONNECTOR_SECRET_KEYS; the substrate
    # strips both the marker and every key it names (cluster delivery is #1488).
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_super_secret",
            "API_KEY": "k-secret",
            "CURIE_CONNECTOR_SECRET_KEYS": "API_KEY,GITHUB_PERSONAL_ACCESS_TOKEN",
        },
    )
    entries = _env_entries(api)
    # Neither the secret values nor the marker land on the claim.
    for leaked in ("ghp_super_secret", "k-secret"):
        assert all(leaked not in e.get("value", "") for e in entries)
    names = {e.get("name") for e in entries}
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in names
    assert "API_KEY" not in names
    assert "CURIE_CONNECTOR_SECRET_KEYS" not in names
    # Non-secret boot env is still written.
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries
    body = api.created[0]
    assert "additionalPodMetadata" not in body["spec"]
    assert body["spec"]["warmPoolRef"]["name"] == "pool"


def test_claim_metadata_agent_label_is_not_additional_pod_metadata() -> None:
    # The adopted controller rejects spec.additionalPodMetadata.labels under
    # curietech.ai (Ready=False reason=InvalidMetadata). Claim object labels
    # are ordinary Kubernetes metadata and are the rotation selector.
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="curie-agent-acme-a-runner-pool",
        labels={"curietech.ai/agent": "acme-a"},
        env={"CURIE_BUDGET": "{}"},
    )
    body = api.created[0]
    assert body["metadata"]["labels"]["curietech.ai/agent"] == "acme-a"
    assert "additionalPodMetadata" not in body["spec"]
    assert body["spec"]["warmPoolRef"]["name"] == "curie-agent-acme-a-runner-pool"
