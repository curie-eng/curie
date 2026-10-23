"""The API publishes the shared proxy's caller verify keys (ADR 0239, migration 0125).

Every API process, at startup and then every ``CALLER_KEYS_PUBLISH_INTERVAL_S``
for its lifetime, upserts the caller verify keys it is configured with (the
current key and the previous one during a rotation, from the same
``Settings.connector_proxy()`` dedicated proxies render from) and a heartbeat
for its observed installation, in one transaction on database time. The shared
proxy accepts the keys seen within its window of the newest heartbeat, so the
accepted set is the union of keys carried by live API processes: rolling
updates, rollbacks and in-place rotations converge by republishing, a retired
key ages out once no live process carries it, and an API outage freezes the
last set rather than expiring it.

A blank verify key configuration still heartbeats, so every key ages out and
the proxies refuse closed. A blank, invalid or unobserved installation never
writes: shared mode is cluster only. A malformed key configuration never gets
here, because ``Settings`` refuses to boot. Nothing that can still be accepted
is ever deleted.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

from plugin_format.shared_connector_render import SharedApplication
from sqlalchemy import ColumnClause, delete, func, literal_column, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import Settings
from ..db import SCHEMA
from ..models import SharedConnectorCallerKey, SharedConnectorCallerKeyHeartbeat

logger = logging.getLogger(__name__)

# The shared proxy's window is five of these (``CALLER_KEY_WINDOW_S``).
CALLER_KEYS_PUBLISH_INTERVAL_S = 60
SCHEMA_UNAVAILABLE = "shared_caller_keys_schema_unavailable"
# Hygiene only: a key row this old is days outside any accepted window.
_RETENTION: ColumnClause[Any] = literal_column("interval '7 days'")

Outcome = Literal["published", "skipped", "schema_unavailable"]


def configured_keys(settings: Settings) -> tuple[str, ...]:
    """The verify keys this process carries, current first; empty when none."""

    proxy = settings.connector_proxy()
    return () if proxy is None else proxy.public_keys


def _installation(settings: Settings) -> str | None:
    installation = settings.installation_id
    if not installation.strip() or not settings.installation_id_observed:
        return None
    try:
        SharedApplication.valid_installation(installation)
    except ValueError:
        return None
    return installation


async def publish(session: AsyncSession, settings: Settings) -> Outcome:
    """One publication: every configured key and the heartbeat, in one transaction."""

    installation = _installation(settings)
    if installation is None:
        return "skipped"
    # The serving window admits databases below 0125 (schema_min stays 0122),
    # so the tables may not exist yet; the next tick tries again.
    present = await session.scalar(
        text("SELECT to_regclass(:keys) IS NOT NULL AND to_regclass(:beats) IS NOT NULL"),
        {
            "keys": f"{SCHEMA}.{SharedConnectorCallerKey.__tablename__}",
            "beats": f"{SCHEMA}.{SharedConnectorCallerKeyHeartbeat.__tablename__}",
        },
    )
    if not present:
        await session.rollback()
        return "schema_unavailable"
    for key in configured_keys(settings):
        await session.execute(
            insert(SharedConnectorCallerKey)
            .values(installation_id=installation, public_key=key)
            .on_conflict_do_update(
                index_elements=["installation_id", "public_key"], set_={"seen_at": func.now()}
            )
        )
    await session.execute(
        insert(SharedConnectorCallerKeyHeartbeat)
        .values(installation_id=installation)
        .on_conflict_do_update(index_elements=["installation_id"], set_={"seen_at": func.now()})
    )
    await session.execute(
        delete(SharedConnectorCallerKey)
        .where(
            SharedConnectorCallerKey.installation_id == installation,
            SharedConnectorCallerKey.seen_at < func.now() - _RETENTION,
        )
        .execution_options(synchronize_session=False)
    )
    await session.commit()
    return "published"


async def run_publisher(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    interval_s: float = CALLER_KEYS_PUBLISH_INTERVAL_S,
) -> None:
    """Publish now and then every ``interval_s`` until cancelled.

    A failed tick is logged by exception type only and retried on the next one,
    so a database blip never stops the API. Missing tables are reported once
    until a tick publishes again.
    """

    reported = False
    while True:
        try:
            async with sessionmaker() as session:
                outcome = await publish(session, settings)
            if outcome == "schema_unavailable":
                if not reported:
                    logger.warning(
                        "%s: the caller verify key tables do not exist yet; "
                        "shared proxies refuse until migration 0125 is applied",
                        SCHEMA_UNAVAILABLE,
                    )
                    reported = True
            else:
                reported = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the next tick retries
            logger.warning(
                "shared caller verify key publication failed",
                extra={"error": type(exc).__name__},
            )
        await asyncio.sleep(interval_s)
