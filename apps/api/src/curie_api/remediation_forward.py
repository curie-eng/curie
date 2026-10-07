"""Forward execution of a remediation, under a policy or an approval authority.

@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-14 @spec ACTION-EXECUTOR-19

AUTOMATED-REMEDIATION-13: an admitted nomination, or an approved remediation
approval, creates one ``forward`` execution through the ACTION-EXECUTOR-19
creation function (``action_forward.create_forward_execution``). This module
is the authority source that adapts a nomination row and the policy generation
that declares its action into the seam's ``ForwardAuthority``; it never takes a
tool, connector or arguments from a caller:

* connector and tool come from the generation's action, the image digest from
  the agent's in-force version, and the canonical arguments and their digest
  from the nomination row;
* a ``policy`` authority is an ``admitted`` nomination, with ``authority_ref``
  ``policy:<agent_id>:<hook>:<generation>:<nomination id>`` naming the
  generation it was admitted under;
* an ``approval`` authority is an ``approved`` nomination whose recorded
  approval is the one named, with ``authority_ref`` the approval id; its action
  is read from the current generation (``authority_generation``);
* the idempotency key is per authority, ``remediation:<nomination id>:policy``
  or ``remediation:<nomination id>:approval:<approval id>``, so a replayed
  admission or approval adopts its own execution and creates nothing, while
  the approval that follows a ``not_reversible_now`` policy refusal creates
  its own.

A refusal raises the seam's ``ForwardRefused`` and writes nothing. The ledger
row is written at dispatch (``routers/action_executions.py``), which reads
``nomination_for_execution`` to carry the delivery, nomination, actor and
gating approval onto it (AUTOMATED-REMEDIATION-14).
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Final

from plugin_format import connector_lock
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import bundles
from .action_forward import ForwardAuthority, ForwardCreated, ForwardRefused
from .action_forward import create_forward_execution as _create_forward_execution
from .action_undoable import in_force_bundle_refs, sealing_custody
from .config import get_settings
from .models import (
    ActionExecution,
    ConnectorCapability,
    ExecutionKind,
    RemediationNomination,
    RemediationPolicy,
    RemediationPolicyGeneration,
)
from .schemas.action_executions import DIGEST_PATTERN
from .storage import BundleStore, ObjectStore

# @spec AUTOMATED-REMEDIATION-13: the seam's key namespace for remediations.
IDEMPOTENCY_PREFIX: Final = "remediation:"
POLICY_AUTHORITY: Final = "policy"
APPROVAL_AUTHORITY: Final = "approval"

_UNAVAILABLE: Final = "authority_unavailable"
_DIGEST = re.compile(DIGEST_PATTERN)


def idempotency_key(nomination_id: uuid.UUID, kind: str, ref: str) -> str:
    """The per-authority key (AUTOMATED-REMEDIATION-13).

    ``remediation:<nomination id>:policy`` for a policy authority and
    ``remediation:<nomination id>:approval:<approval id>`` for an approval.
    """

    if kind == POLICY_AUTHORITY:
        return f"{IDEMPOTENCY_PREFIX}{nomination_id}:{POLICY_AUTHORITY}"
    return f"{IDEMPOTENCY_PREFIX}{nomination_id}:{APPROVAL_AUTHORITY}:{ref}"


def _parse_key(key: str) -> tuple[uuid.UUID, str, str | None] | None:
    """``(nomination id, authority kind, approval id or None)`` of a remediation key."""

    if not key.startswith(IDEMPOTENCY_PREFIX):
        return None
    parts = key.removeprefix(IDEMPOTENCY_PREFIX).split(":")
    try:
        nomination_id = uuid.UUID(parts[0])
    except ValueError:
        return None
    if parts[1:] == [POLICY_AUTHORITY]:
        return nomination_id, POLICY_AUTHORITY, None
    if len(parts) == 3 and parts[1] == APPROVAL_AUTHORITY:
        return nomination_id, APPROVAL_AUTHORITY, parts[2]
    return None


def policy_ref(agent_id: uuid.UUID, hook: str, generation: int, nomination_id: uuid.UUID) -> str:
    """``policy:<agent_id>:<hook>:<generation>:<nomination id>``."""

    return f"policy:{agent_id}:{hook}:{generation}:{nomination_id}"


def _image_digest(image: str | None) -> str | None:
    """The ``sha256:<hex>`` an image reference pins, or None for a tag."""

    if not image:
        return None
    digest = image.rsplit("@", 1)[1] if "@" in image else image
    return digest if _DIGEST.fullmatch(digest) else None


def _connector_digest(data: bytes, connector: str) -> str | None:
    """The image digest ``connector`` runs at in a stored bundle, or None."""

    settings = get_settings()
    with TemporaryDirectory() as tmp:
        bundles.extract_stored_bundle(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        declared = connector_lock.apply_lock(
            bundles.read_connectors(Path(tmp)),
            bundles.read_connector_lock(Path(tmp)),
            portable=True,
        )
    spec = declared.connectors.get(connector)
    if spec is None or not spec.is_hosted:
        return None
    return _image_digest(spec.image)


async def in_force_connector_digest(
    session: AsyncSession, store: ObjectStore, agent_id: uuid.UUID, connector: str
) -> str | None:
    """The digest ``connector`` is pinned to by the agent's in-force version.

    None when the agent has no in-force version, its bundle cannot be read, or
    it declares no hosted connector of that name pinned by digest: the
    authority is then unavailable, never guessed.
    """

    bundle_ref = (await in_force_bundle_refs(session, [agent_id])).get(agent_id)
    if bundle_ref is None:
        return None
    try:
        data = await store.get(bundle_ref)
        return await run_in_threadpool(_connector_digest, data, connector)
    except Exception:  # noqa: BLE001 - an unreadable bundle is no authority
        return None


def _declared_action(document: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    actions = document.get("actions")
    if not isinstance(actions, list):
        return None
    for action in actions:
        if isinstance(action, Mapping) and action.get("name") == name:
            return action
    return None


async def _refuse(session: AsyncSession, code: str, reason: str) -> ForwardRefused:
    await session.rollback()
    return ForwardRefused(code, reason)


async def _adopted(
    session: AsyncSession, nomination: RemediationNomination, kind: str, ref: str
) -> ForwardCreated | None:
    """The execution this authority already created for the nomination, or None.

    Found by the authority's own key, so a replay after the nomination moved on
    (a refused policy execution sent back to approval, ``executing`` and later)
    still answers that one execution and creates nothing.
    """

    execution = await session.scalar(
        select(ActionExecution)
        .where(
            ActionExecution.agent_id == nomination.agent_id,
            ActionExecution.idempotency_key == idempotency_key(nomination.id, kind, ref),
        )
        .execution_options(populate_existing=True)
    )
    if execution is None:
        return None
    if (
        execution.kind != ExecutionKind.forward
        or execution.authority_kind != kind
        or execution.authority_ref != ref
    ):
        raise await _refuse(
            session, "arguments_mismatch", "this authority's key already names another execution"
        )
    return ForwardCreated(execution_id=execution.id, state=execution.state, created=False)


async def _record_on_nomination(
    session: AsyncSession,
    nomination: RemediationNomination,
    required_state: str,
    execution_id: uuid.UUID,
) -> None:
    """Name ``execution_id`` on the nomination while it holds this authority.

    The nomination names the execution of the authority it currently holds:
    a late replay of an earlier authority (an admission replayed after the
    nomination went to approval) adopts its own execution without moving it.
    """

    await session.execute(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == nomination.id,
            RemediationNomination.state == required_state,
        )
        .values(execution_id=execution_id)
    )
    await session.commit()


async def create_remediation_forward(
    session: AsyncSession,
    nomination_id: uuid.UUID,
    *,
    approval_id: uuid.UUID | None = None,
    store: ObjectStore | None = None,
) -> ForwardCreated:
    """Create, or adopt on replay, the forward execution of one nomination.

    @spec AUTOMATED-REMEDIATION-13. Without ``approval_id`` the authority is the
    policy and the nomination must be ``admitted``; with it, the nomination
    must be ``approved`` under exactly that approval. Commits on success and
    records the execution on the nomination. Raises ``ForwardRefused`` having
    written nothing.
    """

    nomination = await session.scalar(
        select(RemediationNomination)
        .where(RemediationNomination.id == nomination_id)
        .execution_options(populate_existing=True)
    )
    if nomination is None or nomination.action is None or nomination.arguments is None:
        raise await _refuse(session, _UNAVAILABLE, "no nomination names this call")
    kind = POLICY_AUTHORITY if approval_id is None else APPROVAL_AUTHORITY
    if kind == APPROVAL_AUTHORITY and nomination.approval_id != approval_id:
        raise await _refuse(
            session, _UNAVAILABLE, "the approval named did not approve this nomination"
        )
    generation_number = await authority_generation(session, nomination, kind)
    if generation_number is None:
        raise await _refuse(
            session, _UNAVAILABLE, "no policy generation authorizes this nomination"
        )
    ref = (
        policy_ref(nomination.agent_id, nomination.hook, generation_number, nomination.id)
        if kind == POLICY_AUTHORITY
        else str(approval_id)
    )

    required_state = "admitted" if kind == POLICY_AUTHORITY else "approved"
    adopted = await _adopted(session, nomination, kind, ref)
    if adopted is not None:
        await _record_on_nomination(session, nomination, required_state, adopted.execution_id)
        return adopted
    if nomination.state != required_state:
        raise await _refuse(
            session, _UNAVAILABLE, f"a nomination in state {nomination.state} authorizes nothing"
        )

    generation = await session.get(
        RemediationPolicyGeneration, (nomination.agent_id, nomination.hook, generation_number)
    )
    declared = (
        _declared_action(generation.document, nomination.action) if generation is not None else None
    )
    connector = declared.get("connector") if declared is not None else None
    tool = declared.get("tool") if declared is not None else None
    if not isinstance(connector, str) or not isinstance(tool, str):
        raise await _refuse(
            session, _UNAVAILABLE, "no policy generation declares the nominated action"
        )
    digest = await in_force_connector_digest(
        session, store or BundleStore(get_settings()), nomination.agent_id, connector
    )
    if digest is None:
        raise await _refuse(
            session, _UNAVAILABLE, "the in-force version pins no digest for this connector"
        )
    try:
        arguments = json.loads(nomination.arguments)
    except ValueError:
        raise await _refuse(
            session, "arguments_mismatch", "the nominated arguments are not JSON"
        ) from None
    if not isinstance(arguments, dict) or nomination.arguments_sha256 is None:
        raise await _refuse(session, "arguments_mismatch", "the nominated arguments are malformed")

    created = await _create_forward_execution(
        session,
        ForwardAuthority(
            kind=kind,
            ref=ref,
            agent_id=nomination.agent_id,
            connector=connector,
            connector_digest=digest,
            tool=tool,
            arguments=arguments,
            arguments_sha256=nomination.arguments_sha256,
            idempotency_key=idempotency_key(nomination.id, kind, ref),
        ),
    )
    # The seam committed the execution; the nomination now names it. A crash
    # between the two commits leaves the key to join them: a replay adopts the
    # execution and records it here, and dispatch finds the nomination by key.
    await _record_on_nomination(session, nomination, required_state, created.execution_id)
    return created


async def nomination_for_execution(
    session: AsyncSession, execution: ActionExecution
) -> RemediationNomination | None:
    """The nomination a remediation forward execution was created for, or None.

    @spec AUTOMATED-REMEDIATION-14. Joined by the execution's own server-minted
    key (``remediation:<nomination id>:<authority>``) under the same agent and
    authority, so a forward execution of any other producer has none.
    """

    parsed = _parse_key(execution.idempotency_key)
    if execution.kind != ExecutionKind.forward or parsed is None:
        return None
    nomination_id, kind, approval = parsed
    if execution.authority_kind != kind or (
        approval is not None and execution.authority_ref != approval
    ):
        return None
    nomination = await session.get(RemediationNomination, nomination_id)
    if nomination is None or nomination.agent_id != execution.agent_id:
        return None
    return nomination


async def authority_generation(
    session: AsyncSession, nomination: RemediationNomination, kind: str
) -> int | None:
    """The policy generation whose declaration an authority executes, or None.

    @spec AUTOMATED-REMEDIATION-13. A ``policy`` authority is the generation the
    nomination was admitted under (admission requires it to be current and
    armed). An ``approval`` authority is the current generation, the one the
    approval card and AUTOMATED-REMEDIATION-16's ``policy_changed`` judge: a
    nomination sent to approval may have no admitted generation (a delivery
    admitted before any policy existed, an envelope without the field) or an
    older one. The nomination's recorded current generation, else the hook's
    live one.
    """

    if kind == POLICY_AUTHORITY:
        return nomination.admitted_generation
    if nomination.current_generation is not None:
        return nomination.current_generation
    live: int | None = await session.scalar(
        select(RemediationPolicy.generation).where(
            RemediationPolicy.agent_id == nomination.agent_id,
            RemediationPolicy.hook == nomination.hook,
        )
    )
    return live


async def policy_generation(
    session: AsyncSession, nomination: RemediationNomination, kind: str
) -> RemediationPolicyGeneration | None:
    """The generation ``authority_generation`` names, or None."""

    number = await authority_generation(session, nomination, kind)
    if number is None:
        return None
    return await session.get(
        RemediationPolicyGeneration, (nomination.agent_id, nomination.hook, number)
    )


# @spec AUTOMATED-REMEDIATION-13: the nomination state a ``not_reversible_now``
# refusal returns a policy remediation to.
APPROVAL_REQUESTED: Final = "approval_requested"


async def not_reversible_now(
    session: AsyncSession,
    store: ObjectStore,
    execution: ActionExecution,
    nomination: RemediationNomination,
) -> bool:
    """Whether a policy remediation's reversible action can no longer be undone.

    @spec AUTOMATED-REMEDIATION-13: "A forward execution of a ``reversible``
    action whose capability or custody no longer holds at dispatch is refused
    ``not_reversible_now``". Only a ``policy`` authority is judged: an approver
    accepted the action as it stands. The action's reversibility is read from
    the generation that authorized it; capability is the probe's
    ``restore_capable`` row for the execution's connector and digest
    (ACTION-EXECUTOR-13) and custody the in-force version's sealing key
    declaration (ACTION-EXECUTOR-16), each read now.
    """

    if execution.authority_kind != POLICY_AUTHORITY or nomination.action is None:
        return False
    generation = await policy_generation(session, nomination, POLICY_AUTHORITY)
    declared = (
        _declared_action(generation.document, nomination.action) if generation is not None else None
    )
    if declared is None or declared.get("reversibility") != "reversible":
        return False
    capable = await session.scalar(
        select(ConnectorCapability.restore_capable).where(
            ConnectorCapability.agent_id == execution.agent_id,
            ConnectorCapability.connector == execution.connector,
            ConnectorCapability.digest == execution.connector_digest,
        )
    )
    if not capable:
        return True
    custody = await sealing_custody(session, store, [execution.agent_id])
    return execution.connector not in custody.get(execution.agent_id, frozenset())
