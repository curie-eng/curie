"""The reconcile-side capability probe trigger. @spec ACTION-EXECUTOR-13.

When the connector reconcile observes a hosted connector rolled out at a digest
with no capability row, this asks the probe route of ACTION-EXECUTOR-1 for a
probe with exactly ``{agent_id, connector, digest}``. The executor runs the
probe and the API stores the row; this module only asks.

**What it reads.** Only what the reconcile already listed in that pass: the
agent's owned objects as ``ConnectorClient.list_owned`` returned them. It adds
no Kubernetes call of any kind. A Deployment is eligible when its ``server``
container image is pinned by ``@sha256:`` and it shows a completed rollout per
ACTION-EXECUTOR-12 (``action_digest.observe``), its ``caller-proxy`` names a
connector inside the connector grammar, and the reconcile did not plan it for
deletion in the same pass.

**Never fatal, never a decision.** A failed capability read sends nothing
(unknown is not absent); a failed request is logged and not remembered, so the
next pass sends again. Neither reaches the pass summary, and nothing here
changes what the reconcile applies or deletes.

**One request per digest.** A probe outlives a pass, so a triple that was sent
is not sent again while no row has landed. The memo expires after
``resend_after_seconds``: the API records no row for a probe that ended
``refused`` or ``failed`` and starts a new attempt when asked again, so without
expiry such a digest would stay unprobed until the worker restarts.
"""

from __future__ import annotations

import logging
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Protocol

import httpx
from plugin_format.connector_render import CALLER_PROXY_CONTAINER, object_name
from sqlalchemy import text

from .action_digest import observe

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

PROBE_PATH = "/connector-capabilities/probes"

# The caller proxy's env var naming the connector it fronts
# (``plugin_format.connector_render``).
_CONNECTOR_ENV = "CURIE_CALLER_PROXY_CONNECTOR"

# The connector grammar: an RFC 1123 label, capped. It is also what excludes
# plugin MCP servers (``plugin_<...>``): an underscore is not in it.
_CONNECTOR = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")
_CONNECTOR_MAX_LENGTH = 63

# A sent triple is not re-requested for this long while no row lands.
DEFAULT_RESEND_AFTER_SECONDS = 3600.0
# Remembered triples are capped; the oldest is forgotten first, which costs at
# most one adopted (200) request.
_MAX_REMEMBERED = 4096

_REQUEST_TIMEOUT_SECONDS = 10.0

Triple = tuple[str, str, str]


class ProbeRequester(Protocol):
    async def request(self, *, agent_id: str, connector: str, digest: str) -> None:
        """Ask for a probe. Raising means "not sent"."""


class CapabilityRows(Protocol):
    async def exists(self, *, agent_id: str, connector: str, digest: str) -> bool:
        """Whether a capability row exists for exactly this triple."""


class ProbeNotAccepted(Exception):
    """The probe route answered with neither 200 nor 201."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"probe route returned {status_code}")
        self.status_code = status_code


class HttpProbeRequester:
    """Posts the three keys to the probe route under the internal worker token."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        worker_token: str,
        client: httpx.AsyncClient,
        timeout: float = _REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._url = f"{api_base_url.rstrip('/')}{PROBE_PATH}"
        self._headers = {
            **({"X-API-Key": api_key} if api_key else {}),
            "X-Curie-Worker-Token": worker_token,
        }
        self._client = client
        self._timeout = timeout

    async def request(self, *, agent_id: str, connector: str, digest: str) -> None:
        response = await self._client.post(
            self._url,
            json={"agent_id": agent_id, "connector": connector, "digest": digest},
            headers=self._headers,
            timeout=self._timeout,
        )
        # 201 is a new probe, 200 the API adopting a pending or confirmed one.
        # Anything else did not record a request.
        if response.status_code not in (200, 201):
            raise ProbeNotAccepted(response.status_code)


class DbCapabilityRows:
    """Reads ``connector_capabilities`` by its key, as the worker reads the
    agents tables. Construction does not touch the engine."""

    def __init__(self, engine: AsyncEngine, *, db_schema: str) -> None:
        self._engine = engine
        # Table identifiers are not user input; the schema comes from config.
        self._query = text(
            f"SELECT 1 FROM {db_schema}.connector_capabilities "
            "WHERE agent_id = CAST(:agent_id AS uuid) AND connector = :connector "
            "AND digest = :digest"
        )

    async def exists(self, *, agent_id: str, connector: str, digest: str) -> bool:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    self._query,
                    {"agent_id": agent_id, "connector": connector, "digest": digest},
                )
            ).first()
        return row is not None


def _proxy_connector(deployment: dict[str, Any]) -> str | None:
    """The connector the Deployment's caller proxy fronts, if in the grammar."""

    spec = deployment.get("spec") or {}
    containers = ((spec.get("template") or {}).get("spec") or {}).get("containers") or []
    named: list[str] = []
    for container in containers:
        if not isinstance(container, dict) or container.get("name") != CALLER_PROXY_CONTAINER:
            continue
        for env in container.get("env") or []:
            if isinstance(env, dict) and env.get("name") == _CONNECTOR_ENV:
                value = env.get("value")
                if isinstance(value, str):
                    named.append(value)
    if len(named) != 1:
        return None
    connector = named[0]
    if len(connector) > _CONNECTOR_MAX_LENGTH or not _CONNECTOR.fullmatch(connector):
        return None
    return connector


class ProbeTrigger:
    """Requests a probe for each eligible owned Deployment the reconcile saw."""

    def __init__(
        self,
        *,
        requester: ProbeRequester,
        capabilities: CapabilityRows,
        release: str | None = None,
        resend_after_seconds: float = DEFAULT_RESEND_AFTER_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._requester = requester
        self._capabilities = capabilities
        # With a release, a Deployment must also carry the name the render
        # gives this agent's connector, so the env var alone cannot name one.
        self._release = release
        self._resend_after = resend_after_seconds
        self._clock = clock
        self._sent: OrderedDict[Triple, float] = OrderedDict()

    def eligible(
        self,
        *,
        agent_name: str,
        observed: Iterable[dict[str, Any]],
        deleting: Iterable[tuple[str, str]] = (),
    ) -> list[tuple[str, str]]:
        """``(connector, digest)`` for each eligible Deployment, in list order."""

        removed = set(deleting)
        found: list[tuple[str, str]] = []
        for obj in observed:
            if obj.get("kind") != "Deployment":
                continue
            name = (obj.get("metadata") or {}).get("name")
            if ("Deployment", name) in removed:
                continue
            connector = _proxy_connector(obj)
            if connector is None:
                continue
            if self._release is not None and name != object_name(
                self._release, agent_name, connector
            ):
                continue
            seen = observe(obj)
            if not seen.rolled_out or seen.digest is None:
                continue
            if (connector, seen.digest) not in found:
                found.append((connector, seen.digest))
        return found

    async def after_reconcile(
        self,
        *,
        agent_id: str,
        agent_name: str,
        observed: Iterable[dict[str, Any]],
        deleting: Iterable[tuple[str, str]] = (),
    ) -> None:
        """Request what is due for one agent.

        A failed read or request is logged here and never raised; the loop
        still wraps this call, so nothing a probe does can reach the pass.
        """

        pairs = self.eligible(agent_name=agent_name, observed=observed, deleting=deleting)
        for connector, digest in pairs:
            await self._one((agent_id, connector, digest), agent_name=agent_name)

    async def _one(self, triple: Triple, *, agent_name: str) -> None:
        agent_id, connector, digest = triple
        now = self._clock()
        sent_at = self._sent.get(triple)
        if sent_at is not None and now - sent_at < self._resend_after:
            return
        try:
            has_row = await self._capabilities.exists(
                agent_id=agent_id, connector=connector, digest=digest
            )
        except Exception as exc:  # noqa: BLE001 - a probe fact, never a pass failure
            # Unknown is not absent: the request waits for a pass that can read.
            logger.warning(
                "capability row read failed agent=%s connector=%s: %s",
                agent_name,
                connector,
                type(exc).__name__,
            )
            return
        if has_row:
            self._sent.pop(triple, None)
            return
        try:
            await self._requester.request(agent_id=agent_id, connector=connector, digest=digest)
        except Exception as exc:  # noqa: BLE001 - a probe fact, never a pass failure
            # Not remembered: a request that never landed recorded nothing.
            self._sent.pop(triple, None)
            logger.warning(
                "capability probe request failed agent=%s connector=%s; retrying next pass: %s",
                agent_name,
                connector,
                exc if isinstance(exc, ProbeNotAccepted) else type(exc).__name__,
            )
            return
        logger.info(
            "capability probe requested agent=%s connector=%s digest=%s",
            agent_name,
            connector,
            digest,
        )
        self._sent[triple] = now
        self._sent.move_to_end(triple)
        while len(self._sent) > _MAX_REMEMBERED:
            self._sent.popitem(last=False)
