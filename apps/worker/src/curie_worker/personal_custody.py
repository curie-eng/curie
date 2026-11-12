"""Resolve an owner's enrolled generation to the personal Claude grant (#5377, ADR 0268).

The API stages each enrollment generation's grant in an immutable Kubernetes
Secret in the operator's grant namespace and publishes a token-free reference
to it in ``personal_claude_custody`` together with the header generation. This
module is the trusted worker's read side: given ``(tenant, principal,
generation)`` it returns the grant only when the header is active at exactly
that generation, the published custody row names a Secret that matches the
shared contract, and the same row is still published after the Secret read.
Every other outcome is a ``GrantRefusal`` classified ``authentication-required``;
nothing here falls back to a service credential.

The SQL is read only, in the style of ``binding.py``. Kubernetes stays behind
``SecretReader``: production reads through a private ``ApiClient`` with no
retries and a request timeout, never the process-global client configuration.
No log line, error or repr here carries the grant; ``PersonalGrant`` shows its
owner and generation only.

Nothing selects owners or launches with this yet: #5378 wires it into owner
selection. The claim env it returns is #5374's delivery shape
(``sandbox/claim_tokens.py``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .sandbox.claim_tokens import MODEL_AUTH_MODE_ENV, PERSONAL_TOKEN_ENV

#: The custody Secret's sole data key, the same literal migration 0160's
#: ``personal_claude_custody_key_ck`` pins. It names the same variable the
#: per-claim delivery carries, but the custody contract is its own literal.
CUSTODY_SECRET_KEY = "CLAUDE_CODE_OAUTH_TOKEN"
#: The annotation naming the custody row a Secret belongs to.
CUSTODY_ID_ANNOTATION = "curietech.ai/personal-custody-id"
AUTHENTICATION_REQUIRED_CLASSIFICATION = "authentication-required"
#: Bound on one Secret read. The client never retries, so this is the whole wait.
SECRET_READ_TIMEOUT_S = 10.0

GrantRefusalCode = Literal[
    "custody_unconfigured",
    "enrollment_absent",
    "enrollment_inactive",
    "generation_mismatch",
    "custody_missing",
    "custody_invalid",
    "custody_unreadable",
]

# The header and its published custody row at the header's own generation. The
# partial unique index on published rows allows at most one match.
_CUSTODY_SQL = """
SELECT e.id AS enrollment_id,
       e.generation AS generation,
       e.active AS active,
       c.id AS custody_id,
       c.secret_namespace AS secret_namespace,
       c.secret_name AS secret_name,
       c.secret_key AS secret_key,
       c.secret_uid AS secret_uid
FROM {schema}.personal_claude_enrollments e
LEFT JOIN {schema}.personal_claude_custody c
  ON c.enrollment_id = e.id
 AND c.tenant_id = e.tenant_id
 AND c.principal_id = e.principal_id
 AND c.generation = e.generation
 AND c.state = 'published'
WHERE e.tenant_id = :tenant_id AND e.principal_id = :principal_id
"""


def custody_secret_name(enrollment_id: uuid.UUID, generation: int) -> str:
    """``curie-pe-<enrollment as 32 lowercase hex>-g<generation>``, the shared name."""

    return f"curie-pe-{enrollment_id.hex}-g{generation}"


@dataclass(frozen=True, slots=True, eq=False, repr=False)
class PersonalGrant:
    """One owner's grant for one active generation. Its repr never shows the grant."""

    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    generation: int
    _token: str = field(repr=False)

    def __repr__(self) -> str:
        return (
            f"PersonalGrant(tenant_id={self.tenant_id}, principal_id={self.principal_id}, "
            f"generation={self.generation})"
        )

    __str__ = __repr__

    def claim_env(self) -> dict[str, str]:
        """The explicit personal pair a claim carries: the mode and the grant.

        Keyed by #5374's delivery names, so ``claim_tokens.split_claim_tokens``
        moves the grant into the per-claim ``<claim>-tokens`` Secret and only the
        mode stays in the claim's value env.
        """

        return {MODEL_AUTH_MODE_ENV: "personal", PERSONAL_TOKEN_ENV: self._token}


@dataclass(frozen=True, slots=True)
class GrantRefusal:
    """Why no grant was returned. Always authentication-required; never a fallback."""

    code: GrantRefusalCode
    classification: ClassVar[str] = AUTHENTICATION_REQUIRED_CLASSIFICATION


@dataclass(frozen=True, slots=True)
class CustodySecret:
    """What one Secret read returned. ``data`` holds the API's base64 values."""

    uid: str | None
    type: str | None
    immutable: bool | None
    annotations: Mapping[str, str]
    data: Mapping[str, str] = field(repr=False)


class SecretReadError(Exception):
    """A Secret read failed for a reason other than absence. Carries no response body."""


class SecretReader(Protocol):
    def read_secret(self, namespace: str, name: str) -> CustodySecret | None:
        """The Secret, or None when it is absent; ``SecretReadError`` otherwise."""
        ...


@dataclass(frozen=True, slots=True)
class _Published:
    enrollment_id: uuid.UUID
    generation: int
    active: bool
    custody_id: uuid.UUID | None
    secret_namespace: str | None
    secret_name: str | None
    secret_key: str | None
    secret_uid: str | None


class PersonalCustodyReader:
    """Read the published grant for ``(tenant, principal, generation)``."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: str,
        namespace: str,
        secrets: SecretReader,
    ) -> None:
        self._engine = engine
        self._namespace = namespace
        self._secrets = secrets
        # Table identifiers are not user input; the schema comes from config.
        self._sql = text(_CUSTODY_SQL.format(schema=schema))

    async def read_grant(
        self, tenant_id: uuid.UUID, principal_id: uuid.UUID, generation: int
    ) -> PersonalGrant | GrantRefusal:
        if not self._namespace:
            return GrantRefusal("custody_unconfigured")
        published = await self._published(tenant_id, principal_id)
        refusal = self._refuse_reference(published, generation)
        if refusal is not None:
            return refusal
        assert published is not None and published.custody_id is not None
        assert published.secret_name is not None and published.secret_uid is not None
        try:
            secret = await asyncio.to_thread(
                self._secrets.read_secret, self._namespace, published.secret_name
            )
        except Exception:  # noqa: BLE001 - any read failure fails closed, text unread
            return GrantRefusal("custody_unreadable")
        # Absence is a 404 or a Secret recreated under the same name (another UID).
        if secret is None or secret.uid != published.secret_uid:
            return GrantRefusal("custody_missing")
        token = _grant_from(secret, published.custody_id)
        if token is None:
            return GrantRefusal("custody_invalid")
        # The reference may have moved while the Secret was read: a replacement or
        # disconnect publishes another generation. Only the same row still
        # published at the same active generation hands the grant out.
        again = await self._published(tenant_id, principal_id)
        refusal = self._refuse_reference(again, generation)
        if refusal is not None:
            return refusal
        assert again is not None
        if again.custody_id != published.custody_id:
            return GrantRefusal("generation_mismatch")
        return PersonalGrant(tenant_id, principal_id, generation, token)

    async def _published(self, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> _Published | None:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    self._sql, {"tenant_id": tenant_id, "principal_id": principal_id}
                )
            ).first()
        return None if row is None else _Published(**row._mapping)

    def _refuse_reference(
        self, published: _Published | None, generation: int
    ) -> GrantRefusal | None:
        if published is None:
            return GrantRefusal("enrollment_absent")
        if not published.active:
            return GrantRefusal("enrollment_inactive")
        if published.generation != generation:
            return GrantRefusal("generation_mismatch")
        if published.custody_id is None:
            return GrantRefusal("custody_missing")
        if (
            published.secret_namespace != self._namespace
            or published.secret_name
            != custody_secret_name(published.enrollment_id, published.generation)
            or published.secret_key != CUSTODY_SECRET_KEY
            or not published.secret_uid
        ):
            return GrantRefusal("custody_invalid")
        return None


def _grant_from(secret: CustodySecret, custody_id: uuid.UUID) -> str | None:
    """The grant when the Secret matches the custody contract, else None."""

    if (
        secret.immutable is not True
        or secret.type != "Opaque"
        or set(secret.data) != {CUSTODY_SECRET_KEY}
        or secret.annotations.get(CUSTODY_ID_ANNOTATION) != str(custody_id)
    ):
        return None
    try:
        token = base64.b64decode(secret.data[CUSTODY_SECRET_KEY], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, TypeError, ValueError):
        return None
    return token if token.strip() else None


class KubernetesSecretReader:
    """``SecretReader`` over one CoreV1 client; ``get`` on Secrets is all it needs."""

    def __init__(self, core_api: Any, *, timeout_s: float = SECRET_READ_TIMEOUT_S) -> None:
        self._core = core_api
        self._timeout_s = timeout_s

    def read_secret(self, namespace: str, name: str) -> CustodySecret | None:
        from kubernetes.client.exceptions import ApiException

        try:
            secret = self._core.read_namespaced_secret(
                name, namespace, _request_timeout=self._timeout_s
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            # Status only: the response body is never carried or chained.
            raise SecretReadError(f"secret read failed with HTTP {exc.status}") from None
        except Exception as exc:  # noqa: BLE001 - transport failures, text unread
            raise SecretReadError(f"secret read failed: {type(exc).__name__}") from None
        metadata = secret.metadata
        return CustodySecret(
            uid=getattr(metadata, "uid", None),
            type=secret.type,
            immutable=secret.immutable,
            annotations=dict(getattr(metadata, "annotations", None) or {}),
            data=dict(secret.data or {}),
        )


def build_secret_reader(
    *, kubeconfig: str | None = None, timeout_s: float = SECRET_READ_TIMEOUT_S
) -> KubernetesSecretReader:
    """In-cluster credentials (kubeconfig off cluster) on a private client.

    The configuration object is this reader's own, so loading it never changes
    the process-global default other clients copy. ``retries = 0`` makes the
    request timeout the whole wait, as in ``sandbox/k8s.py``.
    """

    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    configuration = k8s_client.Configuration()
    try:
        k8s_config.load_incluster_config(client_configuration=configuration)  # type: ignore[no-untyped-call]
    except k8s_config.ConfigException:
        k8s_config.load_kube_config(config_file=kubeconfig, client_configuration=configuration)  # type: ignore[no-untyped-call]
    configuration.retries = 0
    core = k8s_client.CoreV1Api(k8s_client.ApiClient(configuration=configuration))
    return KubernetesSecretReader(core, timeout_s=timeout_s)
