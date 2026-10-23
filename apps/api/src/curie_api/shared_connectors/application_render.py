"""The worker's view of one shared application: its objects and Secret plan (ADR 0239).

``objects`` is ``render_application(stored spec, caller_public_keys=keys)``,
unmodified, so the worker applies exactly what the CLI and every other reader
would render. ``keys`` are the caller verify keys the dedicated render uses
(``Settings.connector_proxy().public_keys``, current then previous), in their
one canonical spelling and without repeats: trusted configuration, never a
stored or database-published value (ADR 0239 Bundle 2). Rotating them changes
only the proxy container's key env, so the worker re-applies the Deployment.
Beside it is
what the worker needs to reconcile the application's Secrets: the
worker-managed key Secret, the DSN Secret (worker-managed and sealed for a
bundled database, the operator's own reference for a managed one), the
operator's grant store and snapshot seal references, and the live recipient
ids. The API applies nothing and holds no Secret RBAC.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from typing import Any

from fastapi import status
from plugin_format.shared_connector_render import SharedApplication, render_application
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
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

# A configured key that is not 32 bytes: the frozen render's own code for it.
_KEY_MALFORMED = "caller_public_key_malformed"
_CALLER_PUBLIC_KEY_BYTES = 32
# The frozen render's named refusals: ``<code>: <text that never echoes a key>``.
_REFUSAL_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")


def _ref(ref: Any) -> dict[str, str] | None:
    return None if ref is None else {"name": ref.name, "key": ref.key}


def _caller_public_keys() -> tuple[str, ...]:
    """The configured caller verify keys, canonical and unrepeated, current first.

    The settings accept a key with surrounding whitespace or nonzero unused
    trailing bits; each is strictly decoded and re-encoded to its one padded
    standard base64 spelling, so the same key spelled two ways is one key.
    No key configured refuses ``503 shared_caller_keys_unconfigured``; a key
    that is not 32 bytes refuses 500 with the frozen render's code. A refusal
    never carries key text.
    """

    proxy = get_settings().connector_proxy()
    if proxy is None:
        raise SharedRefusal(status.HTTP_503_SERVICE_UNAVAILABLE, "shared_caller_keys_unconfigured")
    keys: list[str] = []
    for text in proxy.public_keys:
        try:
            raw = base64.b64decode(text.strip(), validate=True)
        except ValueError:
            raw = b""
        if len(raw) != _CALLER_PUBLIC_KEY_BYTES:
            raise SharedRefusal(status.HTTP_500_INTERNAL_SERVER_ERROR, _KEY_MALFORMED)
        key = base64.b64encode(raw).decode("ascii")
        if key not in keys:
            keys.append(key)
    return tuple(keys)


def _render_objects(spec: SharedApplication, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    """The frozen render; its named refusal answers 500 with its code, never the key.

    Any other ``ValueError`` (the spec was validated already) answers
    ``shared_application_invalid``, so no message text reaches the body.
    """

    try:
        return render_application(spec, caller_public_keys=keys)
    except ValueError as exc:
        code = str(exc).split(":", 1)[0]
        if not _REFUSAL_CODE.fullmatch(code):
            code = "shared_application_invalid"
        raise SharedRefusal(status.HTTP_500_INTERNAL_SERVER_ERROR, code) from None


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
    objects = _render_objects(spec, _caller_public_keys())

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
            "objects": objects,
            "secrets": {
                "encryption_key": _ref(spec.encryption_key),
                "database": database,
                "grant_store": _ref(spec.grant_store),
                "snapshot_seal": _ref(spec.snapshot_seal),
            },
            "recipients": list(recipients),
        }
    )
