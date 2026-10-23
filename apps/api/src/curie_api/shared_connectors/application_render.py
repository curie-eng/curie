"""The worker's view of one shared application: its objects and Secret plan (ADR 0239).

``objects`` is ``render_application(stored spec)``, unmodified, so the worker
applies exactly what the CLI and every other reader would render. Beside it is
what the worker needs to reconcile the application's Secrets: the
worker-managed key Secret, the DSN Secret (worker-managed and sealed for a
bundled database, the operator's own reference for a managed one), the
operator's grant store and snapshot seal references, and the live recipient
ids. The API applies nothing and holds no Secret RBAC.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from fastapi import status
from plugin_format.shared_connector_render import SharedApplication, render_application
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import SharedConnectorRecipient
from ..schemas.shared_connectors import SharedApplicationRenderOut
from . import database_roles, sealing
from .store import (
    SharedRefusal,
    database_role,
    lock_application,
    require_worker_secret_refs,
    usable_installation,
    with_lock,
)


def _ref(ref: Any) -> dict[str, str] | None:
    return None if ref is None else {"name": ref.name, "key": ref.key}


async def application_render(
    session: AsyncSession, application_id: uuid.UUID, *, namespace: str, release: str
) -> SharedApplicationRenderOut:
    """Render one application of this installation at the worker's own root.

    An application of another installation, namespace or release is refused as
    a root mismatch, and a stored spec naming a non-canonical Secret as
    ``shared_application_secret_ref_invalid``. A disabled application still
    renders, marked disabled: the worker keeps its objects and applies nothing
    new.
    """

    installation = usable_installation()
    application = await lock_application(session, application_id, "share")
    try:
        spec = SharedApplication.model_validate_json(json.dumps(application.spec))
    except ValueError:
        raise SharedRefusal(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_application_invalid"
        ) from None
    if (application.installation_id, spec.namespace, spec.release) != (
        installation,
        namespace,
        release,
    ):
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_application_root_mismatch")
    # Stored state: a spec naming a Secret the worker never creates is refused,
    # so the worker creates no Secret and registers no recipient for it.
    require_worker_secret_refs(spec, status_code=status.HTTP_409_CONFLICT)

    database: dict[str, Any] = {
        "mode": "bundled" if spec.dsn_encoding == "bundled-sealed" else "managed",
        "secret": _ref(spec.database_dsn),
        "sealed_dsn": None,
        "dsn_revision": None,
        "status": None,
    }
    row = await database_role(session, application_id)
    if row is not None:
        database["status"] = row.status
        if row.mode == "bundled":
            sealed = await database_roles.sealed_dsn_for_worker(session, application_id)
            if sealed is not None:
                box, revision, _ = sealed
                database["sealed_dsn"] = sealing.b64encode(box)
                database["dsn_revision"] = revision
        else:
            database["dsn_revision"] = row.dsn_revision

    recipients = (
        await session.scalars(
            with_lock(
                select(SharedConnectorRecipient.recipient_id)
                .where(
                    SharedConnectorRecipient.application_id == application_id,
                    SharedConnectorRecipient.retired_at.is_(None),
                )
                .order_by(SharedConnectorRecipient.ordinal),
                None,
            )
        )
    ).all()
    return SharedApplicationRenderOut.model_validate(
        {
            "application_id": application_id,
            "enabled": application.enabled,
            "installation_label": hashlib.sha256(spec.installation.encode("utf-8")).hexdigest()[
                :40
            ],
            "objects": render_application(spec),
            "secrets": {
                "encryption_key": _ref(spec.encryption_key),
                "database": database,
                "grant_store": _ref(spec.grant_store),
                "snapshot_seal": _ref(spec.snapshot_seal),
            },
            "recipients": list(recipients),
        }
    )
