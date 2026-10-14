"""Delete end to end namespaces nobody tore down (#3245, ADR 0176 decision 4).

Teardown must not depend on the agent calling env_destroy. The worker runs
this sweep on an interval against every configured test cluster and deletes a
namespace once its TTL has passed or its run has finished.

Scope is checked twice. The list asks the API server for the owner label, and
every item is checked again here for the prefix and the exact owner label
value, so a server that ignored the selector still cannot hand this module a
namespace the installation does not own. ``sweep`` checks the prefix once
more on the names it is given.

Since #3246 every reap first runs image retention (``retention``): it closes
build admission, stops build Jobs, and deletes the run's images. A namespace
whose close is still settling or whose build pod still runs is deferred to
the next pass rather than failed.
"""

from __future__ import annotations

import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from curie_e2e_connector.contract import EXPIRES_ANNOTATION, RUN_LABEL
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.registry import RegistryApi
from curie_e2e_connector.retention import TeardownDeferred, prepare_teardown

logger = logging.getLogger(__name__)

# Deleted before the namespace, in this order. A namespace delete waits on its
# contents, and a SandboxClaim or Job finalizer is what usually holds it in
# Terminating. Re-sweeping a Terminating namespace re-issues these, which is
# what unsticks a stalled delete.
_CHILD_COLLECTIONS = (
    "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxclaims",
    "/apis/batch/v1/namespaces/{ns}/jobs?propagationPolicy=Background",
    "/api/v1/namespaces/{ns}/persistentvolumeclaims",
)
_NAMESPACE_DELETED = (200, 202, 404, 409)

# How far past its TTL a namespace may be before it counts as overdue. A
# healthy reaper always sees a few namespaces just past their TTL between
# sweeps; the page keys on ``overdue`` so that churn never pages.
OVERDUE_GRACE_S = 600


@dataclass(frozen=True)
class Scope:
    """Which namespaces this installation owns on the test cluster."""

    namespace_prefix: str
    owner_label_key: str
    owner_label_value: str

    def covers(self, name: str) -> bool:
        return name.startswith(self.namespace_prefix) and len(name) > len(self.namespace_prefix)


@dataclass(frozen=True)
class ScopedNamespace:
    name: str
    # The run label, or "" when the namespace has none.
    run: str
    # None when the TTL annotation is missing or unreadable.
    expires_at: datetime | None
    terminating: bool


@dataclass(frozen=True)
class SweepTarget:
    """One test cluster and the registry its run images were pushed to."""

    cluster: ClusterApi
    registry: RegistryApi


@dataclass
class SweepResult:
    reaped: list[str] = field(default_factory=list)
    # Scoped namespaces past their TTL when the sweep ran, whatever their phase.
    expired: int = 0
    failed: list[str] = field(default_factory=list)
    # Scoped namespaces at least OVERDUE_GRACE_S past their TTL, or without a
    # readable TTL, whatever their phase or whether this sweep deleted them.
    overdue: int = 0
    # Scoped namespaces whose teardown must wait: admission closed less than
    # CLOSE_SETTLE_S ago, or a build pod still runs. Not failed and not unclean;
    # the next pass retries, and the overdue gauge covers a build that never stops.
    deferred: list[str] = field(default_factory=list)


def is_expired(namespace: ScopedNamespace, now: datetime) -> bool:
    """A namespace without a readable TTL cannot be proven live, so it counts as expired."""

    return namespace.expires_at is None or namespace.expires_at <= now


def is_overdue(namespace: ScopedNamespace, now: datetime) -> bool:
    """Past its TTL by at least the grace, or without a readable TTL at all."""

    return namespace.expires_at is None or namespace.expires_at <= now - timedelta(
        seconds=OVERDUE_GRACE_S
    )


def scoped_namespaces(cluster: ClusterApi, scope: Scope) -> list[ScopedNamespace]:
    """List the installation's namespaces. Raises ``ClusterError`` when the list fails."""

    selector = urllib.parse.quote(f"{scope.owner_label_key}={scope.owner_label_value}", safe="")
    code, payload = cluster.request("GET", f"/api/v1/namespaces?labelSelector={selector}")
    if code != 200:
        raise ClusterError(f"the test cluster refused the namespace list ({code})")
    items = payload.get("items")
    if not isinstance(items, list):
        raise ClusterError("the test cluster returned a namespace list without items")
    found: list[ScopedNamespace] = []
    for item in items:
        scoped = _scoped(item, scope)
        if scoped is not None:
            found.append(scoped)
    return found


def _scoped(item: Any, scope: Scope) -> ScopedNamespace | None:
    if not isinstance(item, dict):
        return None
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    labels = metadata.get("labels")
    if not isinstance(name, str) or not scope.covers(name) or not isinstance(labels, dict):
        return None
    if labels.get(scope.owner_label_key) != scope.owner_label_value:
        return None
    run = labels.get(RUN_LABEL)
    annotations = metadata.get("annotations")
    expires = annotations.get(EXPIRES_ANNOTATION) if isinstance(annotations, dict) else None
    status = item.get("status")
    phase = status.get("phase") if isinstance(status, dict) else None
    return ScopedNamespace(
        name=name,
        run=run if isinstance(run, str) else "",
        expires_at=_parse_expiry(expires),
        terminating=phase == "Terminating",
    )


def _parse_expiry(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    # A naive time has no defined instant to compare against.
    return moment if moment.tzinfo is not None else None


def sweep(
    cluster: ClusterApi,
    scope: Scope,
    namespaces: list[ScopedNamespace],
    *,
    now: datetime,
    terminal_runs: set[str],
    registry: RegistryApi,
) -> SweepResult:
    """Delete every scoped namespace that is past its TTL or whose run finished.

    One namespace's failure is recorded and the sweep moves on; the next pass
    retries it.
    """

    result = SweepResult()
    for namespace in namespaces:
        if not scope.covers(namespace.name):
            continue
        expired = is_expired(namespace, now)
        if expired:
            result.expired += 1
        if is_overdue(namespace, now):
            result.overdue += 1
        if not expired and namespace.run not in terminal_runs:
            continue
        try:
            _reap(cluster, registry, namespace.name, terminating=namespace.terminating, now=now)
        except TeardownDeferred as exc:
            logger.info("e2e reaper deferred namespace=%s: %s", namespace.name, exc)
            result.deferred.append(namespace.name)
            continue
        except ClusterError as exc:
            logger.warning("e2e reaper could not delete namespace=%s: %s", namespace.name, exc)
            result.failed.append(namespace.name)
            continue
        result.reaped.append(namespace.name)
    return result


def _reap(
    cluster: ClusterApi,
    registry: RegistryApi,
    name: str,
    *,
    terminating: bool,
    now: datetime,
) -> None:
    """Delete the images, the children, then the namespace.

    Raises ``TeardownDeferred`` when retention must wait and ``ClusterError``
    on failure; either way, nothing after retention is sent.

    A 403 on a child does not stop the namespace delete. On a Terminating
    namespace it is expected, because teardown removes the RoleBinding first.
    On an Active namespace it means the grant is wrong, and the children the
    reaper could not delete may be what holds the namespace, so the namespace
    is still deleted but the pass reports it failed.
    """

    prepare_teardown(cluster, registry, name, terminating=terminating, now=now)
    refused: list[str] = []
    for template in _CHILD_COLLECTIONS:
        path = template.format(ns=name)
        code, _payload = cluster.request("DELETE", path)
        if 200 <= code < 300 or code == 404:
            continue
        if code == 403:
            logger.warning("e2e reaper was refused DELETE %s (403); deleting the namespace", path)
            refused.append(path)
            continue
        raise ClusterError(f"the test cluster refused DELETE {path} ({code})")
    code, _payload = cluster.request("DELETE", f"/api/v1/namespaces/{name}")
    if code not in _NAMESPACE_DELETED:
        raise ClusterError(f"the test cluster refused the namespace delete ({code})")
    if refused and not terminating:
        raise ClusterError(
            f"the test cluster refused DELETE {refused[0]} (403) on an Active namespace"
        )
