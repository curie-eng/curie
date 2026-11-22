"""Inputs, fault proxy manifests and pure judgements for the #5429 AC3 driver.

``tests/test_factory_publication_recovery_live.py`` drives a candidate
install; everything here is pure except reading this checkout's proxy source,
so the judgements are exercised by ordinary tests with recorded row shapes.
"""

from __future__ import annotations

import ipaddress
import json
import math
import re
import socket
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import candidate_install as ci
import factory_e2e as fe
import resource_retry_support as rrs
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
# Write, then exactly one publish tool call. A write-file marker alone ends the
# turn done and never reaches the publish pause where capture happens.
STANDING_DIRECTIVE = f"[fake:write-file {CHANGED_FILE} recovered-bytes] [fake:publish-changes]"
# Opt-in paid leg. Unset keeps the fake directive. Never executed by the unit tests.
REAL_MODEL_FLAG = "CURIE_FACTORY_PUBLICATION_RECOVERY_REAL_MODEL"
MODEL_KEY_FILE = "MODEL_KEY_FILE"
# OpenRouter's usage counter lags a finished request. The paid leg waits this
# long between the end of the run and the second reading.
USAGE_LAG_S = 60.0
# The request poll is 3 s. The usage snapshot uses this shorter poll so the
# equality window does not start a whole request interval after capture.
CAPTURE_POLL_S = 0.5
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

PULL_CREATE_PATH = f"{API_PREFIX}/repos/{re.escape(tch.STUB_REPO)}/pulls"
_OWNER_KEY, _OWNER_VALUE = "owner", "factory-tools"


class RealModel:
    """A caller-supplied model credential. ``repr`` never includes the key."""

    __slots__ = ("key", "model")

    def __init__(self, key: str, model: str) -> None:
        self.key = key
        self.model = model

    def __repr__(self) -> str:
        return f"RealModel(model={self.model!r}, key='redacted')"


def real_model_from_env(env: Mapping[str, str]) -> RealModel | None:
    """The paid leg when its flag is ``1``, else None. The key stays in the object.

    A missing or unreadable key file refuses before any cluster call. The
    refusal names the variable, not the path or the key.
    """

    flag = env.get(REAL_MODEL_FLAG, "")
    if flag == "":
        return None
    if flag != "1":
        raise ci.Refused(f"{REAL_MODEL_FLAG} is set but not 1")
    path = env.get(MODEL_KEY_FILE, "").strip()
    if not path:
        raise ci.Refused(f"{REAL_MODEL_FLAG}=1 needs {MODEL_KEY_FILE}")
    try:
        key = Path(path).read_text().strip()
    except OSError:
        raise ci.Refused(f"{MODEL_KEY_FILE} could not be read") from None
    if not key or any(character.isspace() for character in key):
        raise ci.Refused(f"{MODEL_KEY_FILE} could not be read")
    model = (env.get("CURIE_FACTORY_MODEL") or fe.DEFAULT_MODEL).strip()
    if not model:
        raise ci.Refused("CURIE_FACTORY_MODEL is empty")
    return RealModel(key, model)


def provider_usage(key: str, *, opener: Callable[..., Any] | None = None) -> float:
    """The provider's cumulative USD usage for ``key``. Errors omit the key."""

    request = Request(fe.OPENROUTER_KEY_URL, headers={"Authorization": f"Bearer {key}"})
    read = opener or urlopen
    try:
        with read(request, timeout=20) as response:
            payload = json.load(response)
        usage = float(payload["data"]["usage"])
    except (OSError, ValueError, KeyError, TypeError):
        raise ci.Refused("provider usage could not be read") from None
    if not math.isfinite(usage) or usage < 0:
        raise ci.Refused("provider usage could not be read")
    return usage


def pause_for_usage_lag(pause: Callable[[float], None] | None = None) -> None:
    """Wait out the provider usage lag before the second spend reading."""

    (pause or rrs._pause)(USAGE_LAG_S)


def provider_cidrs(lookup: Callable[[], Sequence[str]] | None = None) -> list[str]:
    """OpenRouter IPv4 addresses as single-host CIDRs, for sandbox egress."""

    try:
        if lookup is None:
            infos = socket.getaddrinfo(fe.OPENROUTER_HOST, 443, socket.AF_INET, socket.SOCK_STREAM)
            addresses = sorted({str(info[4][0]) for info in infos})
        else:
            addresses = sorted({str(item) for item in lookup()})
        if not addresses:
            raise ValueError("no addresses")
        return [host_cidr(address) for address in addresses]
    except (OSError, ValueError):
        raise ci.Refused("provider addresses could not be resolved") from None


def proxy_host(namespace: str) -> str:
    return f"{PROXY_NAME}.{namespace}.svc.cluster.local"


def proxy_origin(namespace: str) -> str:
    """The origin the whole install knows the forge by. Pure."""

    return f"https://{proxy_host(namespace)}:{PROXY_PORT}"


def proxy_base_url(namespace: str) -> str:
    """The GitHub REST base the candidate API is pointed at. Pure."""

    return proxy_origin(namespace) + API_PREFIX


def stub_origin(namespace: str) -> str:
    """The stub's own origin, the one only the proxy talks to. Pure."""

    return f"https://{tch.stub_host(namespace)}:{ci.STUB_PORT}"


def host_cidr(address: str) -> str:
    """One address as a single-host CIDR. Pure."""

    ip = ipaddress.ip_address(address)
    return f"{ip}/{ip.max_prefixlen}"


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
                        "--upstream",
                        stub_origin(namespace),
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


def _swap(node: Any, origin: tuple[str, str], exact: Mapping[str, str]) -> Any:
    """``node`` with one origin replaced inside strings and whole strings replaced. Pure."""

    if isinstance(node, dict):
        return {key: _swap(value, origin, exact) for key, value in node.items()}
    if isinstance(node, list):
        return [_swap(value, origin, exact) for value in node]
    if isinstance(node, str):
        return exact[node] if node in exact else node.replace(*origin)
    return node


def _strings(node: Any) -> list[str]:
    if isinstance(node, dict):
        return [s for value in node.values() for s in _strings(value)]
    if isinstance(node, list):
        return [s for value in node for s in _strings(value)]
    return [node] if isinstance(node, str) else []


def recovery_values(
    base: Mapping[str, Any],
    *,
    namespace: str,
    stub_ip: str,
    proxy_ip: str,
    real_model: RealModel | None = None,
    model_cidrs: Sequence[str] = (),
) -> dict[str, Any]:
    """The proof's install values over the candidate base values. Pure.

    The fault proxy stands in for the forge everywhere the base values name
    the stub: the GitHub API and clone bases, the factory's code host and
    tracker entries (API base and identity host), and the stub address in
    every egress rule (sandbox and publication Job) becomes the proxy's. The
    API derives its GitHub identity host from ``api.githubApiUrl`` and checks
    the forge's answers against it, so every forge URL Curie sees must name
    one host. Workspaces and candidate
    retention are on (candidate retention defaults on when workspaces are on),
    the publication reconcile ceiling
    is ``RECONCILE_MAX_ATTEMPTS``, and the agent's fake model writes one file
    and then publishes. ``real_model`` turns the fake model off, installs that
    credential and model, adds ``model_cidrs`` to sandbox egress, and sets no
    fake directive. The stub forge and the fault proxy stay.
    """

    stub, stub_cidr = stub_origin(namespace), host_cidr(stub_ip)
    stub_authority = stub.removeprefix("https://")
    values: dict[str, Any] = _swap(
        json.loads(json.dumps(base)),
        (stub, proxy_origin(namespace)),
        {
            stub_cidr: host_cidr(proxy_ip),
            stub_authority: proxy_origin(namespace).removeprefix("https://"),
        },
    )
    left = [s for s in _strings(values) if stub_authority in s or s == stub_cidr]
    assert not left, f"the base values still route to the stub directly: {left}"
    assert values["api"]["githubApiUrl"] == proxy_base_url(namespace), (
        "the base values name the stub's API base somewhere other than api.githubApiUrl"
    )
    sandbox = values["agentSandbox"]
    sandbox["runner"]["workspace"] = {"enabled": True}
    if real_model is None:
        sandbox["runnerEnv"] = {AGENT: {"FAKE_MODEL_DIRECTIVE": STANDING_DIRECTIVE}}
    else:
        sandbox["runner"]["fakeModel"] = False
        sandbox["runner"]["model"] = real_model.model
        sandbox["runner"]["credentials"] = real_model.key
        sandbox.pop("runnerEnv", None)
        if model_cidrs:
            allowed = values["security"]["networkPolicy"].setdefault("allowedEgress", [])
            allowed.extend(
                {"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}]} for cidr in model_cidrs
            )
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


def objects_by_kind(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    """Index candidate objects by kind. A row with no kind is not the patch."""

    found: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        kind = row.get("kind")
        if kind not in ("base", "patch"):
            continue
        assert kind not in found, f"more than one {kind} object"
        found[str(kind)] = row
    assert "patch" in found, "the candidate listing has no patch object"
    return found


def complete_candidate(listing: Mapping[str, Any]) -> dict[str, Any]:
    """The one complete, available candidate of a candidate listing. Pure.

    When the listing carries ``objects``, the patch digest must be the object
    whose kind is ``patch``. The first object, or an object with no kind, is
    not the patch.
    """

    candidates = [c for c in listing.get("candidates") or [] if isinstance(c, dict)]
    assert len(candidates) == 1, f"{len(candidates)} candidates, expected exactly one"
    candidate = candidates[0]
    assert candidate.get("status") == "complete", f"candidate status {candidate.get('status')!r}"
    assert candidate.get("available") is True, "the retained candidate is not available"
    for kind in ("patch", "base"):
        part = candidate.get(kind) or {}
        if part.get("kind") not in (None, kind):
            raise AssertionError(f"the listing {kind} kind is {part.get('kind')!r}")
        assert re.fullmatch(r"[0-9a-f]{64}", str(part.get("sha256") or "")), f"no {kind} digest"
        assert int(part.get("size_bytes") or 0) > 0, f"the {kind} object is empty"
    assert re.fullmatch(r"[0-9a-f]{40}", str((candidate.get("base") or {}).get("base_commit"))), (
        "the candidate names no base commit"
    )
    raw_objects = listing.get("objects")
    if isinstance(raw_objects, list):
        rows = [row for row in raw_objects if isinstance(row, dict)]
        by_kind = objects_by_kind(rows)
        assert (candidate.get("patch") or {}).get("sha256") == by_kind["patch"].get("sha256"), (
            "the listing patch is not the patch-kind object"
        )
        base = by_kind.get("base")
        if base is not None:
            assert (candidate.get("base") or {}).get("sha256") == base.get("sha256"), (
                "the listing base is not the base-kind object"
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
    row; ``candidate_after`` the listing entry for the same candidate id after
    recovery (the listing keeps every candidate of the WorkItem, so a missing
    entry fails).
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
    # Admission inserts that request without copying or incrementing the
    # predecessor epoch, so a one-run original and its one-run successor
    # both finish at 1. Each must have taken an epoch; neither has to
    # outrank the other.
    original_epoch = int(original.get("runtime_epoch") or 0)
    successor_epoch = int(successor.get("runtime_epoch") or 0)
    assert original_epoch >= 1, f"the original runtime epoch {original_epoch} was never taken"
    assert successor_epoch >= 1, f"the successor runtime epoch {successor_epoch} was never taken"
    assert successor.get("started_at") and original.get("terminal_at"), "missing start or end"
    started = datetime.fromisoformat(str(successor["started_at"]))
    assert started > datetime.fromisoformat(str(original["terminal_at"])), (
        "the successor started before the original request ended"
    )
    assert successor.get("runtime_claim_name"), "the successor recorded no sandbox claim"
    assert successor.get("runtime_claim_name") != original.get("runtime_claim_name"), (
        "the successor reused the original's sandbox claim"
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
    assert candidate_after is not None, "the captured candidate is no longer listed"
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
    # Factory creation keeps the candidate body and appends the tracker's
    # closing reference, the issue marker, and any applied-instruction line.
    # The stored body is therefore not the candidate body alone.
    published_body = str(publication.get("body") or "")
    candidate_body = str(candidate_row.get("body") or "")
    assert candidate_body and published_body.startswith(candidate_body), (
        "the recovery publication's body does not start with the candidate's"
    )
    assert "Closes #" in published_body[len(candidate_body) :], (
        "the recovery publication's body does not carry the closing reference"
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
    assert str(pull.get("title") or "") == str(candidate_row.get("title") or ""), (
        "the forge pull request title is not the candidate's"
    )
    forge_body = str(pull.get("body") or "")
    assert forge_body == published_body, (
        "the forge pull request body is not the published body "
        f"({forge_body[:180]!r} vs {published_body[:180]!r})"
    )
    assert forge_body.startswith(candidate_body), (
        "the forge pull request body does not start with the candidate's"
    )
    assert publication.get("pr_number") == pull.get("number"), "the lineage names another PR"
    passed = [status for status, fault in creates if not fault]
    assert passed == [201], f"pull request creates that reached the forge: {passed}"

    before_shape, after_shape = usage_shape(usage_at_capture), usage_shape(usage_after)
    # The publish permission deny interrupts the fake turn before its
    # scripted usage messages, and the interrupt result carries no usage.
    # A capture reading of zero tokens is the honest one for that turn.
    # What the recovery must hold is that the reading does not move
    # afterwards and the successor recorded none.
    assert before_shape == after_shape, "model usage changed after the candidate was captured"
    successor_tokens = [t for rid, t in after_shape["requests"] if rid == str(successor.get("id"))]
    assert sum(successor_tokens) == 0, "the recovery request recorded model tokens"

    return {
        "successor_id": successor.get("id"),
        "execution_kind": successor.get("execution_kind"),
        "successor_status": successor.get("status"),
        "original_runtime_epoch": original.get("runtime_epoch"),
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
