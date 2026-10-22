"""Which hosted connectors a turn's bundle declares routes on (ADR 0240 decision 2).

The worker mints one ``ccr`` route token per such connector, so it reads the
declaration from the version's stored bundle, the way ``channel_read`` reads a
manifest grant. A stored bundle is write once, so the answer is cached by
bundle ref and agent. An unreadable bundle mints no route token: the route is
refused ``missing``, which the calling tool sees, and the turn still runs.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from plugin_format.connectors import CONNECTORS_FILE, validate_connectors
from plugin_format.deploy_targets import (
    connectors_for_agent,
    restrict_connectors,
    validate_deploy_targets,
)
from plugin_format.validate import DEPLOY_FILE
from plugin_format.yaml_loader import safe_load_unique

from ..bundle_store import BundleReader, extract_bundle
from .log import logger

if TYPE_CHECKING:
    from .core import Kernel

# The same bound and cache size as the channel read grant lookup beside it.
_LOOKUP_TIMEOUT_S = 5.0
_CACHE_MAX = 4096


def _load_yaml(path: Path) -> object:
    return safe_load_unique(path.read_text(encoding="utf-8"))


def read_route_connectors(
    reader: BundleReader, bundle_ref: str, agent: str, limits: tuple[int, float, int]
) -> frozenset[str]:
    """The hosted connectors mounted for ``agent`` that declare ``routes:``."""

    data = reader.get(bundle_ref)
    max_bytes, max_ratio, max_members = limits
    with tempfile.TemporaryDirectory(prefix="curie-route-connectors-") as tmp:
        root = extract_bundle(
            data,
            Path(tmp),
            max_uncompressed_bytes=max_bytes,
            max_compression_ratio=max_ratio,
            max_members=max_members,
        )
        path = root / CONNECTORS_FILE
        if not path.is_file():
            return frozenset()
        parsed, errors = validate_connectors(_load_yaml(path))
        if errors or parsed is None:
            return frozenset()
        deploy_path = root / DEPLOY_FILE
        if deploy_path.is_file():
            targets, target_errors = validate_deploy_targets(_load_yaml(deploy_path))
            if target_errors or targets is None:
                # The runner mounts nothing for a deploy.yaml that does not
                # validate, so nothing here is reachable either.
                return frozenset()
            parsed = restrict_connectors(parsed, connectors_for_agent(targets, agent))
    return frozenset(
        name for name, spec in parsed.connectors.items() if spec.is_hosted and spec.routes
    )


async def route_connectors(self: Kernel, bundle_ref: str | None, agent: str) -> frozenset[str]:
    """``read_route_connectors`` off the event loop, bounded and cached."""

    # Without a caller key nothing is minted, so the bundle is never read.
    if (
        not bundle_ref
        or not agent
        or self._bundles is None
        or not self._config.connector_caller_signing_key.strip()
    ):
        return frozenset()
    key = (bundle_ref, agent)
    cached = self._bundle_routes.get(key)
    if cached is not None:
        return cached
    limits = (
        self._config.bundle_max_uncompressed_bytes,
        self._config.bundle_max_compression_ratio,
        self._config.bundle_max_members,
    )
    # One lookup per key at a time: a timed-out read keeps running, and the
    # next turn waits on it instead of starting another extraction.
    lookup = self._bundle_route_lookups.get(key)
    if lookup is None:
        lookup = asyncio.ensure_future(
            asyncio.to_thread(read_route_connectors, self._bundles, bundle_ref, agent, limits)
        )
        self._bundle_route_lookups[key] = lookup
    try:
        found = await asyncio.wait_for(asyncio.shield(lookup), timeout=_LOOKUP_TIMEOUT_S)
    except TimeoutError:
        logger.warning("connector routes unread from the bundle: the lookup timed out")
        return frozenset()
    except (Exception, yaml.YAMLError) as exc:  # noqa: BLE001 -- no token, not a failed turn
        self._bundle_route_lookups.pop(key, None)
        logger.warning("connector routes unreadable from the bundle: %s", type(exc).__name__)
        return frozenset()
    self._bundle_route_lookups.pop(key, None)
    if len(self._bundle_routes) >= _CACHE_MAX:
        self._bundle_routes.clear()
    self._bundle_routes[key] = found
    return found
