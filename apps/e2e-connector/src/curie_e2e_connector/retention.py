"""Delete a run's images before its namespace goes (#3246, ADR 0176 decision 6).

Every build records its target repository in the namespace's ``e2e-images``
ConfigMap before it creates a Secret or a Job. That ledger is both the
retention intent record and the admission gate. Before any namespace delete
(the reaper and env_destroy) ``prepare_teardown``:

0. Closes admission by setting ``closing_at`` in the ledger, and waits until
   it is ``CLOSE_SETTLE_S`` old, so a build admitted just before the close has
   seen it on its own post Job POST re-read and deleted its Job.
1. Stops build Jobs.
2. Defers while any build pod still runs.
3. Re-reads the ledger, whose repositories are now final.
4. Lists every tag of every ledger repository and deletes each distinct digest.

A failure raises and the namespace is kept, so the next pass retries.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from curie_e2e_connector.contract import (
    BUILD_LABEL,
    CLOSE_SETTLE_S,
    IMAGES_CONFIGMAP,
    IMAGES_CONFIGMAP_KEY,
    REFUSAL_BUILD_IN_PROGRESS,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.registry import RegistryApi, RegistryError, owns_repository

logger = logging.getLogger(__name__)

LEDGER_MALFORMED = "e2e image ledger is malformed"
LEDGER_ATTEMPTS = 5
_BUILD_SELECTOR = urllib.parse.quote(BUILD_LABEL, safe="")
_FINISHED_PHASES = ("Succeeded", "Failed")


class TeardownDeferred(ClusterError):
    """Teardown cannot proceed yet. Not a failure: the reaper retries next pass."""


class CloseSettling(TeardownDeferred):
    """Admission closed less than ``CLOSE_SETTLE_S`` ago."""

    def __init__(self, remaining_s: float) -> None:
        super().__init__(
            f"{REFUSAL_BUILD_IN_PROGRESS}: teardown is settling for {remaining_s:.1f}s"
        )
        self.remaining_s = remaining_s


class BuildInProgress(TeardownDeferred):
    """A build pod is still running after its Job was deleted."""

    def __init__(self) -> None:
        super().__init__(f"{REFUSAL_BUILD_IN_PROGRESS}: a build pod is still running")


@dataclass
class Ledger:
    repositories: list[str]
    closing_at: str | None
    resource_version: str
    labels: dict[str, str] = field(default_factory=dict)


def ledger_path(namespace: str) -> str:
    return f"/api/v1/namespaces/{namespace}/configmaps/{IMAGES_CONFIGMAP}"


def configmaps_path(namespace: str) -> str:
    return f"/api/v1/namespaces/{namespace}/configmaps"


def parse_ledger(payload: dict[str, Any]) -> Ledger:
    """Read a ledger ConfigMap. Raises ``ClusterError`` when it is malformed."""

    try:
        parsed = json.loads(payload["data"][IMAGES_CONFIGMAP_KEY])
    except (KeyError, TypeError, ValueError):
        raise ClusterError(LEDGER_MALFORMED) from None
    if not isinstance(parsed, dict):
        raise ClusterError(LEDGER_MALFORMED)
    repositories = parsed.get("repositories", [])
    if not isinstance(repositories, list) or not all(
        isinstance(entry, str) for entry in repositories
    ):
        raise ClusterError(LEDGER_MALFORMED)
    closing_at = parsed.get("closing_at")
    if closing_at is not None and not isinstance(closing_at, str):
        raise ClusterError(LEDGER_MALFORMED)
    metadata = payload.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    version = metadata.get("resourceVersion")
    labels = metadata.get("labels")
    return Ledger(
        repositories=list(repositories),
        closing_at=closing_at,
        resource_version=version if isinstance(version, str) else "",
        labels=dict(labels) if isinstance(labels, dict) else {},
    )


def ledger_text(repositories: list[str], closing_at: str | None = None) -> str:
    body: dict[str, Any] = {"repositories": repositories}
    if closing_at is not None:
        body["closing_at"] = closing_at
    return json.dumps(body)


def configmap_body(
    text: str, *, labels: dict[str, str], resource_version: str | None = None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"name": IMAGES_CONFIGMAP, "labels": labels}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
        "data": {IMAGES_CONFIGMAP_KEY: text},
    }


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_closing_at(value: str) -> datetime:
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise ClusterError(f"{LEDGER_MALFORMED}: closing_at is not a time") from None
    if moment.tzinfo is None:
        raise ClusterError(f"{LEDGER_MALFORMED}: closing_at has no time zone")
    return moment


def prepare_teardown(
    cluster: ClusterApi,
    registry: RegistryApi,
    namespace: str,
    *,
    terminating: bool,
    now: datetime,
) -> None:
    """Close admission, stop builds, and delete the namespace's images.

    Raises ``CloseSettling`` or ``BuildInProgress`` (both ``TeardownDeferred``)
    when teardown must wait, and ``ClusterError`` or ``RegistryError`` when it
    failed. On any raise the caller must not delete the namespace.
    """

    closing_at = _close_admission(cluster, namespace, terminating=terminating, now=now)
    if closing_at is not None:
        age = max(0.0, (now - closing_at).total_seconds())
        if age < CLOSE_SETTLE_S:
            raise CloseSettling(CLOSE_SETTLE_S - age)
    _stop_builds(cluster, namespace, terminating=terminating)
    _require_no_running_build(cluster, namespace, terminating=terminating)
    repositories = _final_repositories(cluster, registry, namespace, terminating=terminating)
    for repo in repositories:
        _delete_images(registry, repo)


def _close_admission(
    cluster: ClusterApi, namespace: str, *, terminating: bool, now: datetime
) -> datetime | None:
    """Set ``closing_at`` (keeping an existing one) and return it.

    Returns None only on a terminating namespace whose ledger cannot be read or
    written, which admits no new objects anyway.
    """

    path = ledger_path(namespace)
    for _attempt in range(LEDGER_ATTEMPTS):
        code, payload = cluster.request("GET", path)
        if code == 403 and terminating:
            return None
        if code == 404:
            body = configmap_body(ledger_text([], stamp(now)), labels={})
            created, _payload = cluster.request("POST", configmaps_path(namespace), body)
            if created in (200, 201):
                return now
            if created == 409:
                continue
            if created == 403 and terminating:
                return None
            raise ClusterError(f"the test cluster refused to create the image ledger ({created})")
        if code != 200:
            raise ClusterError(f"the test cluster refused GET {path} ({code})")
        ledger = parse_ledger(payload)
        if ledger.closing_at is not None:
            return _parse_closing_at(ledger.closing_at)
        body = configmap_body(
            ledger_text(ledger.repositories, stamp(now)),
            labels=ledger.labels,
            resource_version=ledger.resource_version,
        )
        written, _payload = cluster.request("PUT", path, body)
        if written == 200:
            return now
        if written == 409:
            continue
        if written == 403 and terminating:
            return None
        raise ClusterError(f"the test cluster refused to close the image ledger ({written})")
    raise ClusterError(
        f"the image ledger kept changing after {LEDGER_ATTEMPTS} attempts to close it"
    )


def _stop_builds(cluster: ClusterApi, namespace: str, *, terminating: bool) -> None:
    path = (
        f"/apis/batch/v1/namespaces/{namespace}/jobs"
        f"?labelSelector={_BUILD_SELECTOR}&propagationPolicy=Background"
    )
    code, _payload = cluster.request("DELETE", path)
    if 200 <= code < 300 or code == 404 or (code == 403 and terminating):
        return
    raise ClusterError(f"the test cluster refused to delete build Jobs ({code})")


def _require_no_running_build(cluster: ClusterApi, namespace: str, *, terminating: bool) -> None:
    path = f"/api/v1/namespaces/{namespace}/pods?labelSelector={_BUILD_SELECTOR}"
    code, payload = cluster.request("GET", path)
    if code == 403 and terminating:
        return
    if code != 200:
        raise ClusterError(f"the test cluster refused to list build pods ({code})")
    items = payload.get("items")
    if not isinstance(items, list):
        raise ClusterError("the test cluster returned a pod list without items")
    for item in items:
        status = item.get("status") if isinstance(item, dict) else None
        phase = status.get("phase") if isinstance(status, dict) else None
        if phase not in _FINISHED_PHASES:
            raise BuildInProgress()


def _final_repositories(
    cluster: ClusterApi, registry: RegistryApi, namespace: str, *, terminating: bool
) -> list[str]:
    code, payload = cluster.request("GET", ledger_path(namespace))
    if code == 404:
        return []
    if code == 403 and terminating:
        logger.warning(
            "image ledger unreadable on a terminating namespace; its images are left to "
            "registry retention namespace=%s",
            namespace,
        )
        return []
    if code != 200:
        raise ClusterError(f"the test cluster refused the image ledger read ({code})")
    repositories = parse_ledger(payload).repositories
    if not repositories:
        return []
    if not registry.settings.prefix:
        raise RegistryError("registry is not configured but the ledger names images")
    for entry in repositories:
        if not owns_repository(registry.settings, namespace, entry):
            raise ClusterError(LEDGER_MALFORMED)
    return repositories


def _delete_images(registry: RegistryApi, repo: str) -> None:
    digests: list[str] = []
    for tag in registry.list_tags(repo):
        digest = registry.resolve(repo, tag)
        if digest is not None and digest not in digests:
            digests.append(digest)
    for digest in digests:
        registry.delete_manifest(repo, digest)
