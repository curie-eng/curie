"""What the executor loop reads about an agent's in-force binding (ADR 0121).

@spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-14. Two
reads the loop composes, both from the agent's in-force deployment as an
ordinary turn would resolve it (``BindingResolver.resolve_agent``):

* ``in_force_digest``: the ``server`` image digest the in-force version renders
  the connector's Deployment at, through the same API render the connector
  reconcile loop applies (``HttpManifestSource``). A tag, an unknown agent, a
  connector the version does not run, or any failure is None, which refuses
  ``connector_digest_unavailable``. A connector the render lists as shared
  raises ``SharedConnectorUnsupported`` instead (ADR 0239).
* ``executor_boot``: the binding's boot env (stripped by the loop) and the
  connector secrets the target connector's derived MCP entry headers expand,
  read from the version's stored bundle with ``plugin_format``'s own
  ``mcp_entry``, so the header set is the runner's derivation. The bundle's
  declarations are read strictly (``read_bundle_declarations``): an unreadable
  or invalid file raises ``ConnectorDeclarationRefused`` with its code, and
  only no bundle or no ``connectors.yaml`` expands nothing. Shared entries the
  acting agent's target runs are resolved through the API first, and a shared
  target connector raises ``SharedConnectorUnsupported``.

Neither reads conversation text, model output or history.
"""

from __future__ import annotations

import asyncio
import logging
import re
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from aci_protocol import BootEnv
from plugin_format.connector_render import mcp_entry, object_name

from .action_digest import observe
from .action_executor_loop import ExecutorBoot
from .bundle_store import extract_bundle
from .shared_declarations_client import (
    ACTION_UNSUPPORTED,
    WORKER_TOKEN_MISSING,
    SharedConnectorUnsupported,
    SharedDeclarationRefused,
    SharedResolveClient,
    read_bundle_declarations,
)

if TYPE_CHECKING:
    from .binding import BindingResolver, ResolvedDeployment
    from .bundle_store import BundleReader
    from .connector_loop import HttpManifestSource

logger = logging.getLogger(__name__)

# The runner's placeholder grammar (``curie_runner.mcp_tool_capability._VARIABLE``).
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
# The caller token rides its own header and is not a connector secret.
_CALLER_TOKEN_ENV = BootEnv.env_key("connector_caller_token")


class ExecutorBindings:
    """Reads the in-force digest and the executor boot for one agent's connector."""

    def __init__(
        self,
        *,
        binding: BindingResolver,
        bundles: BundleReader,
        manifests: HttpManifestSource | None,
        shared: SharedResolveClient | None,
        release: str,
        namespace: str,
        max_uncompressed_bytes: int,
        max_compression_ratio: float,
        max_members: int,
    ) -> None:
        self._binding = binding
        self._bundles = bundles
        # None on the local tier, which renders no connector Deployment.
        self._manifests = manifests
        # None without an internal worker token: a bundle whose acting target
        # runs a shared entry then refuses ``shared_worker_token_missing``.
        self._shared = shared
        self._release = release
        self._namespace = namespace
        self._max_uncompressed_bytes = max_uncompressed_bytes
        self._max_compression_ratio = max_compression_ratio
        self._max_members = max_members

    async def _resolved(self, agent_id: str) -> ResolvedDeployment | None:
        return await self._binding.resolve_agent(uuid.UUID(str(agent_id)))

    async def in_force_digest(self, agent_id: str, connector: str) -> str | None:
        """The digest the in-force version renders ``connector`` at, or None."""

        manifests = self._manifests
        if manifests is None:
            return None
        resolved = await self._resolved(agent_id)
        if resolved is None:
            return None
        rendered = await asyncio.to_thread(
            manifests.rendered, agent_id=str(resolved.agent_id), version_id=str(resolved.version_id)
        )
        if connector in {entry.name for entry in rendered.shared}:
            raise SharedConnectorUnsupported(ACTION_UNSUPPORTED)
        name = object_name(self._release, resolved.agent_name, connector)
        for manifest in rendered.manifests:
            if not isinstance(manifest, dict) or manifest.get("kind") != "Deployment":
                continue
            if (manifest.get("metadata") or {}).get("name") == name:
                return observe(manifest).digest
        return None

    async def executor_boot(
        self, agent_id: str, connector: str, *, thread_key: str
    ) -> ExecutorBoot:
        """The binding's boot env and the target connector's header secret set.

        ``thread_key`` is the execution's ``action-exec:<execution id>``, so the
        session identity the binding mints is per execution.
        """

        resolved = await self._resolved(agent_id)
        if resolved is None:
            raise LookupError("the agent has no in-force deployment")
        env = self._binding.boot_env(resolved, thread_key)
        names = await asyncio.to_thread(
            self._header_secret_names,
            resolved.bundle_ref,
            str(resolved.agent_id),
            resolved.agent_name,
            connector,
        )
        return ExecutorBoot(boot_env=env, header_secret_names=names, agent_name=resolved.agent_name)

    def _header_secret_names(
        self, bundle_ref: str | None, agent_id: str, agent_name: str, connector: str
    ) -> frozenset[str]:
        """Names the connector's derived MCP entry headers expand (``${NAME}``)."""

        if not bundle_ref:
            return frozenset()
        data = self._bundles.get(bundle_ref)
        with tempfile.TemporaryDirectory() as tmp:
            root = extract_bundle(
                data,
                Path(tmp),
                max_uncompressed_bytes=self._max_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_members=self._max_members,
            )
            # Narrowed to this agent's target: an excluded connector, shared
            # or dedicated, is absent below and expands nothing.
            declared = read_bundle_declarations(root, agent_name=agent_name)
        if declared.shared:
            # Resolved before the target connector is looked at: a shared
            # entry this agent may not run refuses the boot, whichever
            # connector it is for.
            if self._shared is None:
                raise SharedDeclarationRefused(WORKER_TOKEN_MISSING)
            self._shared.resolve(agent_id, declared.shared)
        if connector in {entry.name for entry in declared.shared}:
            raise SharedConnectorUnsupported(ACTION_UNSUPPORTED)
        spec = declared.dedicated.connectors.get(connector)
        if spec is None:
            return frozenset()
        entry = mcp_entry(self._release, agent_name, self._namespace, connector, spec)
        headers = entry.get("headers") or {}
        found = {
            name
            for value in headers.values()
            if isinstance(value, str)
            for name in _PLACEHOLDER.findall(value)
        }
        return frozenset(found - {_CALLER_TOKEN_ENV})
