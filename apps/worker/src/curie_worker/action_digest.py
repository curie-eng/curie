"""Attributing a connector digest without the kernel. @spec ACTION-EXECUTOR-12.

The kernel's record call passes no deployment, and the kernel is not ours to
change, so the digest is attributed by a wrapper around ``ActionClient`` that
implements the same ``ActionRecorder`` protocol. It reads the target
connector's Deployment by name twice -- once on the opening frame, once on the
closing frame -- and hands ``connector`` and ``connector_digest`` to the
completion only when both reads show:

* the same ``metadata.generation``;
* a completed rollout: ``status.observedGeneration`` at least the generation,
  and ``status.updatedReplicas``, ``status.availableReplicas`` and
  ``status.replicas`` each equal to ``spec.replicas``. The last one is not
  redundant: during a surge the first three can hold while an old pod still
  serves, leaving ``status.replicas`` above spec (measurement M5);
* a ``server`` container image pinned by ``@sha256:``. The ``caller-proxy``
  sidecar carries its own, different digest and is never read.

Everything else records null, which is the ordinary answer (local tier, plugin
MCP server, straddled rollout, tag-referenced image) and makes the row not
undoable rather than wrongly undoable.

**Bounded and never fatal.** The kernel's ``_record_action`` is deliberately not
best effort, so this wrapper sits on the turn's path. Each read -- name
resolution included -- is bounded at ``READ_TIMEOUT_SECONDS``, so the wrapper
adds at most four seconds per action, and any failure of a read becomes null.
A failure of the LEDGER is a different thing and still propagates.

**One verb.** The only Kubernetes call is the single-object
``read_namespaced_deployment``; never a list. The chart grants the worker that
``get`` only with the executor and the connector reconciler both enabled, which
is also the only configuration ``run.py`` composes this wrapper in.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aci_protocol import SideEffectFlag
from plugin_format.connector_render import object_name
from sqlalchemy import text

from .actions import ActionClient, RecordedAction

if TYPE_CHECKING:
    from kubernetes.client import AppsV1Api
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Per read. Two reads per action, so at most four seconds added to a turn.
READ_TIMEOUT_SECONDS = 2.0

# The container of a rendered connector Deployment that runs the connector's own
# image (``plugin_format.connector_render``); the other one is the caller proxy.
SERVER_CONTAINER = "server"

# A hosted connector's tool, as the runner names it: ``mcp__<connector>__<tool>``.
# The connector half is the connector grammar (an RFC 1123 label), which is also
# what excludes ``mcp__plugin_<...>`` servers: an underscore is not in it.
_HOSTED_TOOL = re.compile(r"^mcp__([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)__.+$")
_CONNECTOR_MAX_LENGTH = 63
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")

# Opened records awaiting their closing frame. A turn that dies between the two
# frames never completes, so the map is capped rather than left to grow.
_MAX_PENDING = 1024

DeploymentNameResolver = Callable[[str | None, str], Awaitable[str | None]]


@dataclass(frozen=True)
class Observation:
    """What one read of a connector Deployment shows. None fields mean absent."""

    generation: int | None
    digest: str | None
    rolled_out: bool


@dataclass(frozen=True)
class _Pending:
    connector: str
    deployment: str
    opening: Observation


def hosted_connector(tool: str | None) -> str | None:
    """The hosted connector a tool name targets, or None for anything else."""

    match = _HOSTED_TOOL.match(tool or "")
    if match is None:
        return None
    connector = match.group(1)
    return connector if len(connector) <= _CONNECTOR_MAX_LENGTH else None


def _int(value: Any) -> int | None:
    # bool is an int subclass and is never a count.
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def pinned_digest(image: Any) -> str | None:
    """The ``sha256:`` digest an image reference is pinned by, else None.

    ``repo@sha256:...`` and ``repo:tag@sha256:...`` are pinned; a tag, a bare
    repository or ``:latest`` is not, because what it names can move.
    """

    if not isinstance(image, str) or "@" not in image:
        return None
    digest = image.rsplit("@", 1)[1]
    return digest if _DIGEST.match(digest) else None


def observe(body: dict[str, Any]) -> Observation:
    """Read generation, rollout completion and server digest from a Deployment.

    ``body`` is the API server's wire form (camelCase), as ``connector_k8s``
    reads every connector object.
    """

    metadata = body.get("metadata") or {}
    spec = body.get("spec") or {}
    status = body.get("status") or {}
    generation = _int(metadata.get("generation"))
    wanted = _int(spec.get("replicas"))
    observed = _int(status.get("observedGeneration"))
    rolled_out = (
        generation is not None
        and wanted is not None
        and observed is not None
        and observed >= generation
        and _int(status.get("updatedReplicas")) == wanted
        and _int(status.get("availableReplicas")) == wanted
        and _int(status.get("replicas")) == wanted
    )
    containers = ((spec.get("template") or {}).get("spec") or {}).get("containers") or []
    images = [
        c.get("image")
        for c in containers
        if isinstance(c, dict) and c.get("name") == SERVER_CONTAINER
    ]
    digest = pinned_digest(images[0]) if len(images) == 1 else None
    return Observation(generation=generation, digest=digest, rolled_out=rolled_out)


def attributable(opening: Observation, closing: Observation) -> str | None:
    """The digest both reads agree served the call, or None."""

    if not (opening.rolled_out and closing.rolled_out):
        return None
    if opening.generation is None or opening.generation != closing.generation:
        return None
    if opening.digest is None or opening.digest != closing.digest:
        return None
    return opening.digest


class DigestAttributingRecorder:
    """An ``ActionRecorder`` that adds the connector digest to the completion."""

    def __init__(
        self,
        inner: ActionClient,
        *,
        deployments: AppsV1Api | None,
        namespace: str,
        deployment_name: DeploymentNameResolver,
    ) -> None:
        self._inner = inner
        # None on the local tier: no reconciled Deployment, so always null.
        self._deployments = deployments
        self._namespace = namespace
        self._deployment_name = deployment_name
        self._pending: OrderedDict[str, _Pending] = OrderedDict()

    async def record(
        self,
        frame: SideEffectFlag,
        *,
        event_id: str,
        conversation_id: str,
        agent_id: str | None,
        gate_approval_id: str | None = None,
    ) -> RecordedAction:
        recorded = await self._inner.record(
            frame,
            event_id=event_id,
            conversation_id=conversation_id,
            agent_id=agent_id,
            gate_approval_id=gate_approval_id,
        )
        connector = hosted_connector(frame.tool)
        if self._deployments is None or connector is None:
            return recorded
        opened = await self._bounded(self._open(agent_id, connector), connector)
        if opened is not None:
            self._remember(recorded.id, opened)
        return recorded

    async def complete(self, action_id: str, frame: SideEffectFlag) -> dict[str, Any]:
        pending = self._pending.pop(action_id, None)
        digest: str | None = None
        if pending is not None:
            closing = await self._bounded(self._read(pending.deployment), pending.connector)
            if closing is not None:
                digest = attributable(pending.opening, closing)
            if digest is None:
                logger.debug(
                    "action %s recorded no connector digest connector=%s",
                    action_id,
                    pending.connector,
                )
        return await self._inner.complete(
            action_id,
            frame,
            connector=pending.connector if pending is not None and digest else None,
            connector_digest=digest,
        )

    # -- reads ---------------------------------------------------------------

    async def _open(self, agent_id: str | None, connector: str) -> _Pending | None:
        name = await self._deployment_name(agent_id, connector)
        if not name:
            return None
        opening = await self._read(name)
        # A read that cannot contribute to a digest needs no closing read.
        if not opening.rolled_out or opening.digest is None:
            return None
        return _Pending(connector=connector, deployment=name, opening=opening)

    async def _read(self, name: str) -> Observation:
        deployments = self._deployments
        assert deployments is not None

        def get() -> dict[str, Any]:
            response = deployments.read_namespaced_deployment(
                name,
                self._namespace,
                _preload_content=False,
                _request_timeout=READ_TIMEOUT_SECONDS,
            )
            body = json.loads(response.data)
            if not isinstance(body, dict):
                raise ValueError("Deployment body is not an object")
            return body

        return observe(await asyncio.to_thread(get))

    async def _bounded[T](self, read: Awaitable[T], connector: str) -> T | None:
        """``read`` within the bound; a timeout or any error is None, never raised."""

        try:
            return await asyncio.wait_for(read, timeout=READ_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning(
                "connector Deployment read exceeded %.1fs; recording no digest connector=%s",
                READ_TIMEOUT_SECONDS,
                connector,
            )
        except Exception as exc:  # noqa: BLE001 -- a failed read records null (AE-12)
            logger.warning(
                "connector Deployment read failed; recording no digest connector=%s error=%s",
                connector,
                type(exc).__name__,
            )
        return None

    def _remember(self, action_id: str, pending: _Pending) -> None:
        self._pending[action_id] = pending
        self._pending.move_to_end(action_id)
        while len(self._pending) > _MAX_PENDING:
            self._pending.popitem(last=False)


def agent_deployment_resolver(
    engine: AsyncEngine, *, db_schema: str, release: str
) -> DeploymentNameResolver:
    """Name a connector's Deployment the way the reconciler rendered it.

    ``object_name(release, agent name, connector)`` is the one derivation the
    render uses, so the name is computed rather than searched for: the read
    stays a single-object ``get``. The agent's name comes from the same table
    the connector reconcile loop reads. An unknown agent names nothing.
    """

    # Table identifiers are not user input; the schema comes from config.
    query = text(f"SELECT name FROM {db_schema}.agents WHERE id = :id")

    async def resolve(agent_id: str | None, connector: str) -> str | None:
        if agent_id is None:
            return None
        async with engine.connect() as conn:
            agent = (await conn.execute(query, {"id": uuid.UUID(agent_id)})).scalar_one_or_none()
        if agent is None:
            return None
        return object_name(release, str(agent), connector)

    return resolve
