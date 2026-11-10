"""Execute a connector reconcile plan against the cluster (ADR-0090).

The half that mutates. ``connector_reconcile`` decides; this carries the
decision out. Written against a protocol seam so the logic that can delete a
live connector is testable with an in-memory fake, the same discipline the
sandbox substrate uses for the K8s control plane.

Two orderings and one refusal are load-bearing:

**Apply before delete.** A failure part-way through leaves EXTRA objects rather
than missing ones. Extra objects are inert -- an orphaned Service nobody
references -- while a missing Deployment is an agent whose tools stopped
working. Deleting first would make a transient API error during apply into an
outage.

**Delete one at a time, continuing past failures.** A single object that will
not delete (finalizer, RBAC gap) must not strand the rest as orphans. Every
failure is reported; none aborts the sweep.

**Refuse to act without an owner.** Every object this touches carries
``curie.dev/connector-owner``. The plan already filters on it, but the applier
checks again before deleting, because a plan built from a mis-scoped list is
exactly the bug that would delete someone else's connector and the check is
one line.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from .connector_reconcile import OWNER_LABEL, ReconcilePlan, identity, owner_of

logger = logging.getLogger(__name__)


class ConnectorClient(Protocol):
    """The cluster operations a reconcile needs, and no more.

    Deliberately four verbs. A wider seam invites the reconciler to grow
    cluster access it does not need, and the RBAC that backs this is scoped to
    exactly the object kinds a connector consists of.
    """

    def list_owned(self, namespace: str, owner: str) -> list[dict[str, Any]]:
        """Every connector object labelled for this owner.

        Every returned object must carry ``kind`` and ``metadata.name``:
        ``identity()`` keys ownership and every prune decision on that pair, so
        an object missing either is invisible to those checks.
        """

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        """Create or update one object."""

    def delete(self, namespace: str, kind: str, name: str) -> None:
        """Remove one object. Missing is success, not an error."""


@dataclass
class ApplyReport:
    """What actually happened, as opposed to what was planned."""

    applied: list[tuple[str, str]] = field(default_factory=list)
    deleted: list[tuple[str, str]] = field(default_factory=list)
    # (kind, name, error). Collected rather than raised so one bad object does
    # not strand the rest.
    failures: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def execute(
    client: ConnectorClient,
    plan: ReconcilePlan,
    *,
    namespace: str,
    agent: str,
) -> ApplyReport:
    """Carry out a plan. Returns what happened; raises only on programmer error."""

    report = ApplyReport()

    # Drift correction is the one action here that overrides a human's
    # `kubectl edit`. Correct for a declared system, and it must be visible --
    # a reconciler that silently reverts an operator is indistinguishable from
    # a cluster that ignores them.
    for obj_id in plan.drifted:
        logger.info(
            "connector drift corrected agent=%s kind=%s name=%s", agent, obj_id[0], obj_id[1]
        )

    for obj in plan.apply:
        kind, name = identity(obj)
        if owner_of(obj) != agent:
            # A rendered object reaching here without this agent's owner label
            # is a bug upstream, not a cluster condition. Refuse rather than
            # write an object nothing can later prune.
            report.failures.append(
                (kind, name, f"refusing to apply: missing or wrong {OWNER_LABEL}")
            )
            continue
        try:
            client.apply(namespace, obj)
            report.applied.append((kind, name))
        except Exception as exc:  # noqa: BLE001 - cluster errors are data here, not control flow
            report.failures.append((kind, name, str(exc)))

    # Only after every apply has been attempted. A failure above leaves extra
    # objects, which are inert; deleting first would turn the same failure into
    # an agent with no connector.
    if report.failures:
        logger.warning(
            "connector apply had %d failure(s) for agent=%s; skipping prune so a "
            "transient error cannot remove a working connector",
            len(report.failures),
            agent,
        )
        return report

    for kind, name in plan.delete:
        try:
            client.delete(namespace, kind, name)
            report.deleted.append((kind, name))
            logger.info("connector removed agent=%s kind=%s name=%s", agent, kind, name)
        except Exception as exc:  # noqa: BLE001 - existing broad catch retained
            # Keep going: one undeletable object must not strand the others.
            report.failures.append((kind, name, str(exc)))

    return report
