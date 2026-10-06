"""Whether a recorded action is undoable, derived at read time (ACTION-EXECUTOR-11).

@spec ACTION-EXECUTOR-11. ``undoable`` is true exactly when every ingredient a
pinned, sealed restore needs is present:

* the record itself (``AgentAction.holds_restore_record``): succeeded, an agent,
  a valid sealed envelope in ``prior_state``, a ``post_version``, a ``target``,
  a ``connector`` and its ``connector_digest``;
* a ``restore_capable`` capability row for that agent, connector and digest
  (ACTION-EXECUTOR-13);
* sealing key custody, computed from the agent's in-force version on every
  read and never cached (ACTION-EXECUTOR-16);
* no restore execution of the record that is not ``refused``.

Nothing here is stored. A capability row landing, a new version dropping the
``SecretRef`` or a restore being refused changes the answer on the next read,
and every API read of an action goes through ``undoable_action_ids`` so the
single and list reads cannot disagree.

The checks run cheapest first and only for records still in the running, so a
read of rows that are not sealed (every legacy row) never touches the database
again or the object store.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from collections.abc import Iterable, Sequence
from pathlib import Path

from plugin_format.connectors import SecretRef
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import bundles
from .config import get_settings
from .db import SCHEMA
from .models import (
    ActionExecution,
    AgentAction,
    ConnectorCapability,
    ExecutionKind,
    ExecutionState,
)
from .storage import ObjectStore

logger = logging.getLogger(__name__)

# @spec ACTION-EXECUTOR-16: the reserved name the first release recognizes as
# the sealing key. Custody holds only when the in-force version declares it as
# a ``SecretRef`` on the connector that recorded the action; a plain named
# secret, another name, or ``SNAPSHOT_SEALING_KEYS_RETAINED`` alone does not.
SEALING_KEY_NAME = "SNAPSHOT_SEALING_KEY"

# The platform's in-force rule, as hook_fire's ``_IN_FORCE_SQL`` and the
# worker's binding ``_RESOLVE_SQL`` apply it: an active deployment, prod
# outranks dev, then the most recent.
_IN_FORCE_BUNDLES_SQL = f"""
SELECT DISTINCT ON (d.agent_id)
       d.agent_id AS agent_id,
       v.bundle_ref AS bundle_ref
FROM {SCHEMA}.deployments d
JOIN {SCHEMA}.agent_versions v ON v.id = d.version_id AND v.agent_id = d.agent_id
WHERE d.status = 'active' AND d.agent_id = ANY(:agent_ids)
ORDER BY d.agent_id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""


def _sealed_connectors(data: bytes) -> frozenset[str]:
    """Connectors whose declaration gives them custody of the sealing key."""

    settings = get_settings()
    with tempfile.TemporaryDirectory() as tmp:
        bundles.extract_stored_bundle(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        declared = bundles.read_connectors(Path(tmp))
    return frozenset(
        name
        for name, spec in declared.connectors.items()
        if any(
            isinstance(secret, SecretRef) and secret.name == SEALING_KEY_NAME
            for secret in spec.secrets
        )
    )


async def sealing_custody(
    session: AsyncSession, store: ObjectStore, agent_ids: Iterable[uuid.UUID]
) -> dict[uuid.UUID, frozenset[str]]:
    """Per agent, the connectors its in-force version gives sealing key custody.

    @spec ACTION-EXECUTOR-16. Read from the stored bundle of the version in
    force now, on every call. An agent with no in-force version, or whose
    stored bundle cannot be read, has custody of nothing: custody fails closed.
    """

    wanted = sorted(set(agent_ids))
    custody: dict[uuid.UUID, frozenset[str]] = dict.fromkeys(wanted, frozenset())
    if not wanted:
        return custody
    rows = (
        await session.execute(text(_IN_FORCE_BUNDLES_SQL), {"agent_ids": wanted})
    ).mappings()
    for row in rows:
        bundle_ref = row["bundle_ref"]
        if bundle_ref is None:
            continue
        try:
            data = await store.get(str(bundle_ref))
            custody[row["agent_id"]] = await run_in_threadpool(_sealed_connectors, data)
        except Exception:  # noqa: BLE001 - an unreadable bundle is no custody
            logger.warning(
                "in-force bundle unreadable; treating sealing key custody as absent",
                extra={"agent_id": str(row["agent_id"])},
                exc_info=True,
            )
    return custody


async def undoable_action_ids(
    session: AsyncSession, store: ObjectStore, actions: Sequence[AgentAction]
) -> set[uuid.UUID]:
    """The ids among ``actions`` that are undoable now. @spec ACTION-EXECUTOR-11."""

    candidates = [action for action in actions if action.holds_restore_record]
    if not candidates:
        return set()

    # A restore in any state but ``refused`` may have written or is about to,
    # so it holds the record. A forward execution naming the record does not:
    # that is the call that created it (ACTION-EXECUTOR-19).
    held = set(
        (
            await session.execute(
                select(ActionExecution.subject_action_id).where(
                    ActionExecution.kind == ExecutionKind.restore,
                    ActionExecution.state != ExecutionState.refused,
                    ActionExecution.subject_action_id.in_([a.id for a in candidates]),
                )
            )
        ).scalars()
    )
    candidates = [action for action in candidates if action.id not in held]
    if not candidates:
        return set()

    # @spec ACTION-EXECUTOR-13: a capable row for this agent, connector AND
    # digest. A missing row, a probe that found no pair, or a row for another
    # image is "treated as restoring nothing".
    capable = set(
        (
            await session.execute(
                select(
                    ConnectorCapability.agent_id,
                    ConnectorCapability.connector,
                    ConnectorCapability.digest,
                ).where(
                    ConnectorCapability.restore_capable.is_(True),
                    ConnectorCapability.agent_id.in_({a.agent_id for a in candidates}),
                )
            )
        ).tuples()
    )
    candidates = [
        action
        for action in candidates
        if (action.agent_id, action.connector, action.connector_digest) in capable
    ]
    if not candidates:
        return set()

    custody = await sealing_custody(
        session, store, (a.agent_id for a in candidates if a.agent_id is not None)
    )
    return {
        action.id
        for action in candidates
        if action.agent_id is not None
        and action.connector in custody.get(action.agent_id, frozenset())
    }
