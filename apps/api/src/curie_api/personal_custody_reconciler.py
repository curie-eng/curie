"""The personal grant custody cleanup loop (#5377, ADR 0268 section 4).

Runs ``personal_enrollments.reconcile_custody`` on an interval: reclaim staging
whose lease lapsed, delete the Secrets of abandoned and retiring rows by their
recorded UID, and sweep labelled orphans from the grant namespace. It is its
own loop, not a step of the WorkItem reconciler, so factory enablement never
turns custody cleanup off. It runs when the grant namespace is configured and
``CURIE_PERSONAL_CUSTODY_RECONCILER_ENABLED`` is not false.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .custody_store import CustodySecretStore
from .personal_enrollments import Custody, CustodyPass, reconcile_custody

logger = logging.getLogger(__name__)


class PersonalCustodyReconciler:
    def __init__(self, custody: Custody, *, interval_seconds: float) -> None:
        self._custody = custody
        self._interval = interval_seconds

    async def run_once(self) -> CustodyPass:
        return await reconcile_custody(self._custody)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a failed pass must not end the loop
                # The class only: a database error's text carries statement parameters.
                logger.warning("personal custody pass failed (%s)", type(exc).__name__)
            await asyncio.sleep(self._interval)


def custody_for(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    store: CustodySecretStore | None,
) -> Custody | None:
    """The protocol's dependencies, or None when custody is unconfigured."""

    if store is None or not settings.personal_grant_namespace:
        return None
    return Custody(
        sessionmaker=sessionmaker,
        store=store,
        namespace=settings.personal_grant_namespace,
        stage_lease_seconds=settings.personal_custody_stage_lease_seconds,
    )


def start_personal_custody_reconciler(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    store: CustodySecretStore | None,
) -> asyncio.Task[None] | None:
    """Start the loop when custody is configured and the loop is enabled."""

    custody = custody_for(sessionmaker, settings, store)
    if custody is None or not settings.personal_custody_reconciler_enabled:
        return None
    reconciler = PersonalCustodyReconciler(
        custody, interval_seconds=settings.personal_custody_reconcile_interval_seconds
    )
    return asyncio.create_task(reconciler.run_forever())
