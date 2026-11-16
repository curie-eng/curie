"""Inputs, fault proxy manifests and pure judgements for the #5429 AC3 driver.

``tests/test_factory_publication_recovery_live.py`` drives a candidate
install; everything here is pure except reading this checkout's proxy source,
so the judgements are exercised by ordinary tests with recorded row shapes.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import candidate_install as ci
import tls_code_host as tch

FLAG = "CURIE_FACTORY_PUBLICATION_RECOVERY_LIVE"
NAMES = ci.InputNames(
    flag=FLAG,
    namespace="CURIE_FACTORY_PUBLICATION_RECOVERY_NAMESPACE",
    release="CURIE_FACTORY_PUBLICATION_RECOVERY_RELEASE",
    pull_policy="CURIE_FACTORY_PUBLICATION_RECOVERY_PULL_POLICY",
)

AGENT = "pub-recovery"
# The stub repository's default branch (tools/github-stub).
DEFAULT_BRANCH = "main"
TRIGGER_LABEL = "pub-recovery-work"
# One file the fake model writes into the checkout: the candidate's whole change.
CHANGED_FILE = "recovery-proof.txt"
SEED_FILES = {
    "README.md": "# acme-bot\n\nFixture repository for the publication recovery proof.\n",
    "example.py": 'def example() -> str:\n    return "example"\n',
}
SEED_ISSUE = "Publication recovery proof fixture.\n"
# The publication reconcile ceiling the install runs with, and so the number of
# pull request creates the proxy fails: the original request's publication
# spends exactly this many charged attempts, then settles failed.
RECONCILE_MAX_ATTEMPTS = 2
FAULT_STATUS = 502

PROXY_NAME = "forge-fault-proxy"
PROXY_SOURCE_CONFIG_MAP = "forge-fault-proxy-source"
PROXY_UPSTREAM_CONFIG_MAP = "forge-fault-proxy-upstream"
PROXY_PORT = 8443
PROXY_STATE = "/state"
PROXY_SOURCE = Path(__file__).resolve().parent / "forge_fault_proxy.py"
API_PREFIX = "/api/v3"

PULL_CREATE_PATH = f"/repos/{re.escape(tch.STUB_REPO)}/pulls"
_OWNER_KEY, _OWNER_VALUE = "owner", "factory-tools"


def proxy_host(namespace: str) -> str:
    return f"{PROXY_NAME}.{namespace}.svc.cluster.local"


def proxy_base_url(namespace: str) -> str:
    """The base the candidate API is pointed at instead of the forge's REST API. Pure."""

    return f"https://{proxy_host(namespace)}:{PROXY_PORT}{API_PREFIX}"


def stub_api_base(namespace: str) -> str:
    return f"https://{tch.stub_host(namespace)}:{tch.STUB_PORT_DEFAULT}{API_PREFIX}"


def proxy_manifests(
    namespace: str, image: str, pull_policy: str, *, owner: Mapping[str, str]
) -> list[dict[str, Any]]:
    """The proxy's source ConfigMap, Pod and headless Service. Pure except for
    reading the proxy source from this checkout.

    The Pod runs once on the candidate worker image as a non-root user with no
    service account token. Its CA and server keys stay in an emptyDir only it
    mounts; it trusts the upstream through ``PROXY_UPSTREAM_CONFIG_MAP``, which
    the caller creates first.
    """

    labels = {**owner, "app.kubernetes.io/name": PROXY_NAME}
    source = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": PROXY_SOURCE_CONFIG_MAP,
            "namespace": namespace,
            "labels": dict(owner),
        },
        "data": {"forge_fault_proxy.py": PROXY_SOURCE.read_text()},
    }
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": PROXY_NAME, "namespace": namespace, "labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "fsGroup": 1000,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": PROXY_NAME,
                    "image": image,
                    "imagePullPolicy": pull_policy,
                    "command": ["python3", "/proxy/forge_fault_proxy.py", "serve"],
                    "args": [
                        "--state",
                        PROXY_STATE,
                        "--host",
                        proxy_host(namespace),
                        "--bind",
                        "0.0.0.0",
                        "--port",
                        str(PROXY_PORT),
                        "--prefix",
                        API_PREFIX,
                        "--upstream",
                        stub_api_base(namespace),
                        "--upstream-ca",
                        "/upstream/ca.crt",
                    ],
                    "ports": [{"name": "https", "containerPort": PROXY_PORT, "protocol": "TCP"}],
                    "readinessProbe": {"tcpSocket": {"port": PROXY_PORT}, "periodSeconds": 2},
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {
                        "requests": {"cpu": "20m", "memory": "64Mi"},
                        "limits": {"memory": "256Mi"},
                    },
                    "volumeMounts": [
                        {"name": "proxy", "mountPath": "/proxy", "readOnly": True},
                        {"name": "upstream", "mountPath": "/upstream", "readOnly": True},
                        {"name": "state", "mountPath": PROXY_STATE},
                    ],
                }
            ],
            "volumes": [
                {"name": "proxy", "configMap": {"name": PROXY_SOURCE_CONFIG_MAP}},
                {"name": "upstream", "configMap": {"name": PROXY_UPSTREAM_CONFIG_MAP}},
                {"name": "state", "emptyDir": {}},
            ],
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": PROXY_NAME, "namespace": namespace, "labels": dict(owner)},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "None",
            "selector": {"app.kubernetes.io/name": PROXY_NAME},
            "ports": [
                {"name": "https", "port": PROXY_PORT, "targetPort": PROXY_PORT, "protocol": "TCP"}
            ],
        },
    }
    return [source, pod, service]


def recovery_values(base: Mapping[str, Any], *, namespace: str) -> dict[str, Any]:
    """The proof's install values over the candidate base values. Pure.

    Workspaces and candidate retention are on (both off by default), the
    publication reconcile ceiling is ``RECONCILE_MAX_ATTEMPTS``, the API's
    GitHub REST base is the fault proxy, and the agent's fake model writes one
    file into its checkout.
    """

    values: dict[str, Any] = json.loads(json.dumps(base))
    values["api"]["githubApiUrl"] = proxy_base_url(namespace)
    sandbox = values["agentSandbox"]
    sandbox["runner"]["workspace"] = {"enabled": True}
    sandbox["runnerEnv"] = {
        AGENT: {"FAKE_MODEL_DIRECTIVE": f"[fake:write-file {CHANGED_FILE} recovered-bytes]"}
    }
    worker = values.setdefault("worker", {})
    worker.setdefault("workspace", {})["publicationCandidates"] = {"enabled": True}
    worker.setdefault("publication", {})["reconcileMaxAttempts"] = RECONCILE_MAX_ATTEMPTS
    values["factory"].setdefault("sandboxEgress", {})[AGENT] = True
    return values


# --------------------------------------------------------------------------
# Observations
# --------------------------------------------------------------------------


def proxy_requests(log_text: str) -> list[dict[str, Any]]:
    """The proxy's request lines, in order. Pure."""

    found: list[dict[str, Any]] = []
    for line in log_text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and row.get("event") == "request":
            found.append(row)
    return found


def pull_creates(log_text: str) -> list[tuple[int, bool]]:
    """(status, injected) for every pull request create the proxy saw. Pure."""

    pattern = re.compile(PULL_CREATE_PATH)
    return [
        (int(row["status"]), bool(row.get("fault")))
        for row in proxy_requests(log_text)
        if row.get("method") == "POST" and pattern.fullmatch(str(row.get("path") or ""))
    ]


def branch_heads(for_each_ref: str) -> dict[str, str]:
    """``git for-each-ref --format='%(refname) %(objectname)' refs/heads`` as a map. Pure."""

    heads: dict[str, str] = {}
    for line in for_each_ref.splitlines():
        name, _, sha = line.strip().partition(" ")
        if name.startswith("refs/heads/") and re.fullmatch(r"[0-9a-f]{40,64}", sha):
            heads[name.removeprefix("refs/heads/")] = sha
    return heads


def usage_shape(usage: Mapping[str, Any]) -> dict[str, Any]:
    """The comparable part of ``GET /work-items/{id}/usage``. Pure."""

    models = sorted(
        (
            str(m.get("model")),
            str(m.get("role")),
            int(m.get("input_tokens") or 0),
            int(m.get("cached_input_tokens") or 0),
            int(m.get("cache_write_tokens") or 0),
            int(m.get("output_tokens") or 0),
        )
        for m in usage.get("models") or []
    )
    requests = sorted(
        (str(r.get("request_id")), int(r.get("tokens") or 0)) for r in usage.get("requests") or []
    )
    return {
        "total_tokens": int(usage.get("total_tokens") or 0),
        "models": models,
        "requests": requests,
    }


def complete_candidate(listing: Mapping[str, Any]) -> dict[str, Any]:
    """The one complete, available candidate of a candidate listing. Pure."""

    candidates = [c for c in listing.get("candidates") or [] if isinstance(c, dict)]
    assert len(candidates) == 1, f"{len(candidates)} candidates, expected exactly one"
    candidate = candidates[0]
    assert candidate.get("status") == "complete", f"candidate status {candidate.get('status')!r}"
    assert candidate.get("available") is True, "the retained candidate is not available"
    for kind in ("patch", "base"):
        part = candidate.get(kind) or {}
        assert re.fullmatch(r"[0-9a-f]{64}", str(part.get("sha256") or "")), f"no {kind} digest"
        assert int(part.get("size_bytes") or 0) > 0, f"the {kind} object is empty"
    assert re.fullmatch(r"[0-9a-f]{40}", str((candidate.get("base") or {}).get("base_commit"))), (
        "the candidate names no base commit"
    )
    return dict(candidate)


def candidate_identity(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """The candidate facts recovery must keep byte for byte. Pure."""

    patch, base = candidate.get("patch") or {}, candidate.get("base") or {}
    return {
        "candidate_id": str(candidate.get("candidate_id")),
        "patch_sha256": patch.get("sha256"),
        "patch_bytes": patch.get("size_bytes"),
        "base_sha256": base.get("sha256"),
        "base_bytes": base.get("size_bytes"),
        "base_commit": base.get("base_commit"),
        "changed_path_count": candidate.get("changed_path_count"),
    }


def judge_original(
    original: Mapping[str, Any],
    publications: Sequence[Mapping[str, Any]],
    creates: Sequence[tuple[int, bool]],
) -> dict[str, Any]:
    """The original request settled ``publication_failed`` by the injected failures. Pure.

    Its one publication spent exactly the reconcile ceiling, each attempt
    answered by an injected pull request create failure, and failed with its
    branch recorded.
    """

    assert original.get("execution_kind") == "implementation", original.get("execution_kind")
    assert original.get("status") == "failed", (
        f"the original request ended {original.get('status')!r} "
        f"({original.get('terminal_cause')!r}), expected failed with publication_failed"
    )
    assert original.get("terminal_cause") == "publication_failed", original.get("terminal_cause")
    own = [p for p in publications if p.get("execution_request_id") == original.get("id")]
    assert len(own) == 1, f"the original request has {len(own)} publications, expected one"
    publication = own[0]
    assert publication.get("status") == "failed", publication.get("status")
    assert publication.get("reconcile_attempts") == RECONCILE_MAX_ATTEMPTS, (
        f"the failed publication spent {publication.get('reconcile_attempts')} reconcile "
        f"attempts, expected the ceiling {RECONCILE_MAX_ATTEMPTS}"
    )
    injected = [status for status, fault in creates if fault]
    assert injected == [FAULT_STATUS] * RECONCILE_MAX_ATTEMPTS, (
        f"the proxy injected {injected} pull request create failures, expected "
        f"{RECONCILE_MAX_ATTEMPTS} x {FAULT_STATUS}"
    )
    assert creates[: len(injected)] == [(FAULT_STATUS, True)] * len(injected), (
        f"a pull request create reached the forge before the injected failures: {list(creates)}"
    )
    assert publication.get("branch"), "the failed publication names no branch"
    return {
        "request_id": original.get("id"),
        "terminal_cause": original.get("terminal_cause"),
        "publication_id": publication.get("id"),
        "publication_status": publication.get("status"),
        "reconcile_attempts": publication.get("reconcile_attempts"),
        "injected_pull_creates": len(injected),
    }


def judge_recovery(
    *,
    requests: Sequence[Mapping[str, Any]],
    original: Mapping[str, Any],
    candidate: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    candidate_after: Mapping[str, Any] | None,
    publications: Sequence[Mapping[str, Any]],
    heads_after_failure: Mapping[str, str],
    heads_after_recovery: Mapping[str, str],
    pulls: Sequence[Mapping[str, Any]],
    creates: Sequence[tuple[int, bool]],
    usage_at_capture: Mapping[str, Any],
    usage_after: Mapping[str, Any],
    default_branch: str,
) -> dict[str, Any]:
    """One recovery successor published the identical candidate once, with no model call. Pure.

    ``requests`` are every request of the WorkItem in order; ``candidate`` is
    the listing entry captured by the original; ``candidate_row`` its ledger
    row; ``candidate_after`` the listing entry after recovery, when listed.
    """

    assert len(requests) == 2, (
        f"the WorkItem holds {len(requests)} requests, expected the original and one successor"
    )
    successor = requests[1]
    assert successor.get("retry_of_request_id") == original.get("id"), (
        "the second request is not the original's successor"
    )
    assert successor.get("execution_kind") == "publication_recovery", successor.get(
        "execution_kind"
    )
    assert successor.get("status") == "completed", (
        f"the recovery request ended {successor.get('status')!r} "
        f"({successor.get('terminal_cause')!r}), expected completed"
    )
    # Fresh runtime authority: its own admitted request, started after the
    # original ended, on its own sandbox claim, with an epoch of its own.
    assert int(successor.get("runtime_epoch") or 0) >= 1, "the successor never took an epoch"
    assert successor.get("started_at") and original.get("terminal_at"), "missing start or end"
    started = datetime.fromisoformat(str(successor["started_at"]))
    assert started > datetime.fromisoformat(str(original["terminal_at"])), (
        "the successor started before the original request ended"
    )
    assert successor.get("runtime_claim_name"), "the successor recorded no sandbox claim"
    assert successor.get("runtime_claim_name") != original.get("runtime_claim_name"), (
        "the successor reused the original's sandbox claim"
    )
    assert successor.get("model_session_reserved_at") is None, (
        "the recovery request reserved a model session"
    )

    # The identical candidate: same identity, content digest, objects and intent.
    identity = candidate_identity(candidate)
    assert str(successor.get("recovery_candidate_id")) == identity["candidate_id"], (
        "the successor recovers a different candidate"
    )
    assert successor.get("recovery_candidate_sha256") == candidate_row.get("content_sha256"), (
        "the successor's candidate digest differs from the captured candidate"
    )
    assert str(candidate_row.get("origin_request_id")) == str(original.get("id")), (
        "the candidate was not captured by the original request"
    )
    assert str(candidate_row.get("owner_request_id")) == str(successor.get("id")), (
        "the candidate's ownership was not handed to the successor"
    )
    if candidate_after is not None:
        assert candidate_identity(candidate_after) == identity, (
            "the candidate's objects changed during recovery"
        )

    own = [p for p in publications if p.get("execution_request_id") == successor.get("id")]
    assert len(own) == 1, f"the successor has {len(own)} publications, expected one"
    publication = own[0]
    assert publication.get("status") == "succeeded", (
        f"the recovery publication ended {publication.get('status')!r}: "
        f"{str(publication.get('error') or '')[:300]}"
    )
    succeeded = [p for p in publications if p.get("status") == "succeeded"]
    assert len(succeeded) == 1, f"{len(succeeded)} publications succeeded, expected exactly one"
    assert publication.get("base_sha") == identity["base_commit"], (
        "the recovery published on another base"
    )
    assert publication.get("title_sha256") == candidate_row.get("title_sha256"), (
        "the recovery publication's title is not the candidate's"
    )
    assert publication.get("body_sha256") == candidate_row.get("body_sha256"), (
        "the recovery publication's body is not the candidate's"
    )

    # Exactly one branch, holding the commit the original attempt pushed.
    pushed = {k: v for k, v in heads_after_failure.items() if k != default_branch}
    assert len(pushed) == 1, f"after the failure the forge holds branches {sorted(pushed)}"
    branch, commit = next(iter(pushed.items()))
    assert publication.get("branch") == branch, "the recovery published on another branch"
    after = {k: v for k, v in heads_after_recovery.items() if k != default_branch}
    assert after == {branch: commit}, (
        f"recovery changed the forge branches: {pushed} became {after}; the pushed "
        "commit must be adopted, not rewritten"
    )
    assert len(pulls) == 1, f"the forge holds {len(pulls)} pull requests, expected exactly one"
    pull = pulls[0]
    assert (pull.get("head") or {}).get("ref") == branch, "the pull request names another branch"
    assert (pull.get("head") or {}).get("sha") == commit, "the pull request head is not the commit"
    assert publication.get("pr_number") == pull.get("number"), "the lineage names another PR"
    passed = [status for status, fault in creates if not fault]
    assert passed == [201], f"pull request creates that reached the forge: {passed}"

    before_shape, after_shape = usage_shape(usage_at_capture), usage_shape(usage_after)
    assert before_shape["total_tokens"] > 0, "no model usage was recorded by capture time"
    assert before_shape == after_shape, "model usage changed after the candidate was captured"
    successor_tokens = [t for rid, t in after_shape["requests"] if rid == str(successor.get("id"))]
    assert sum(successor_tokens) == 0, "the recovery request recorded model tokens"

    return {
        "successor_id": successor.get("id"),
        "execution_kind": successor.get("execution_kind"),
        "successor_status": successor.get("status"),
        "successor_runtime_epoch": successor.get("runtime_epoch"),
        "successor_claim": successor.get("runtime_claim_name"),
        **identity,
        "publication_id": publication.get("id"),
        "branch_count": len(after),
        "pushed_commit": commit,
        "pull_requests": len(pulls),
        "pull_number": pull.get("number"),
        "pull_creates_reaching_forge": len(passed),
        "tokens_at_capture": before_shape["total_tokens"],
        "tokens_after": after_shape["total_tokens"],
    }
