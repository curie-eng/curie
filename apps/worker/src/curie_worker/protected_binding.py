"""Immutable database and boot binding for verified protected deliveries.

This wrapper deliberately does not forward unknown methods to the ordinary
dynamic resolver. It resolves the historical bundle named by the verified
delivery and renders only the closed execution profile.
"""

from __future__ import annotations

import contextvars
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Protocol
from urllib.parse import quote

from aci_protocol import BootEnv, Budget
from aci_protocol.turn import matching_routes
from channel_protocol import parse_scoped_conversation_id
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.worker_verification import Dispatchable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .behaviorpacks import BehaviorPacks
from .binding import HttpIdentityBinding, ResolvedDeployment, binding_adapter_for_handle
from .kernel.routing import _thread_key_for
from .protected_configuration import ProtectedExecutionConfig
from .protected_history import protected_history_key

_CURRENT_DELIVERY: contextvars.ContextVar[Dispatchable | None] = contextvars.ContextVar(
    "curie_protected_delivery", default=None
)


class ProtectedCredentialProvider(Protocol):
    """Resolve only the exact provisioned generations recorded in the profile."""

    def model_credential(self, reference: Mapping[str, str]) -> str: ...

    def history_token(
        self,
        reference: Mapping[str, str],
        *,
        agent_id: str,
        transcript_binding: str,
        expires_at: int,
    ) -> str: ...


class ProtectedCredentialUnavailable(RuntimeError):
    """The exact provisioned credential generation cannot be resolved."""


def current_protected_delivery() -> Dispatchable | None:
    """Return the delivery installed by the actual protected consumer scope."""
    return _CURRENT_DELIVERY.get()


@asynccontextmanager
async def protected_delivery_scope(delivery: Dispatchable | None) -> AsyncIterator[None]:
    """Carry verified proof, or mask an inherited context for a refused entry."""
    if delivery is not None and type(delivery) is not Dispatchable:
        raise ValueError("protected delivery context is invalid")
    token = _CURRENT_DELIVERY.set(delivery)
    try:
        yield
    finally:
        _CURRENT_DELIVERY.reset(token)


class ProtectedBindingResolver:
    """Bind a protected route to its immutable historical bundle and profile."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        db_schema: str,
        execution: ProtectedExecutionConfig,
        trusted_manifest: Manifest,
        credentials: ProtectedCredentialProvider | None = None,
    ) -> None:
        if (
            not db_schema
            or not (db_schema[0].isalpha() or db_schema[0] == "_")
            or not all(char.isalnum() or char == "_" for char in db_schema)
        ):
            raise ValueError("protected database schema is invalid")
        if (
            type(trusted_manifest) is not Manifest
            or execution.digest != trusted_manifest.as_dict()["execution_config_digest"]
            or execution.as_dict()["launch"]["runner_image_digest"]
            != trusted_manifest.as_dict()["runner_image_digest"]
        ):
            raise ValueError("protected execution profile does not match its manifest")
        self._manifest = trusted_manifest
        self._engine = engine
        self._execution = execution
        self._credentials = credentials
        self._sql = text(
            f"""
SELECT a.id AS agent_id, a.name AS agent_name,
       c.kind AS kind, c.address AS address, c.adapter AS adapter,
       c.endpoint AS endpoint,
       v.id AS version_id, v.version_label AS version_label,
       v.bundle_ref AS bundle_ref, v.bundle_sha256 AS bundle_sha256
FROM {db_schema}.agents a
JOIN {db_schema}.agent_channels c ON c.agent_id = a.id
JOIN {db_schema}.agent_versions v ON v.agent_id = a.id
WHERE a.id = :agent_id AND c.kind = :kind AND c.address = :address
  AND v.id = :version_id AND v.bundle_ref = :bundle_ref
  AND v.bundle_sha256 = :bundle_sha256
"""
        )

    def _profile(self) -> dict[str, Any]:
        return self._execution.as_dict()

    def _delivery(self) -> Dispatchable:
        delivery = _CURRENT_DELIVERY.get()
        if type(delivery) is not Dispatchable:
            raise ValueError("protected binding requires a verified delivery context")
        envelope = delivery.envelope.as_dict()
        manifest = self._manifest.as_dict()
        if (
            delivery.intent.as_dict()["identity"]["agent_id"] != self._profile()["agent_id"]
            or envelope.get("manifest_digest") != self._manifest.digest
            or any(
                envelope.get(name) != manifest[name]
                for name in (
                    "runtime_id",
                    "runtime_generation",
                    "qualification_id",
                    "runner_image_digest",
                    "execution_config_digest",
                )
            )
            or envelope.get("bundle_digest") != manifest["bundle_digest"]["sha256"]
        ):
            raise ValueError("protected delivery does not match its execution profile")
        return delivery

    async def identity_binding(
        self, kind: str, adapter: str | None, address: str,
    ) -> HttpIdentityBinding | None:
        """Explicitly local: protected deliveries never come from HTTP invocation ingress.

        @spec HTTP-INVOCATION-EXECUTION-18.
        """
        return None

    async def resolve(
        self, kind: str, adapter: str | None, address: str
    ) -> ResolvedDeployment | None:
        delivery = self._delivery()
        handle = delivery.turn.reply_handle
        if handle is None:
            return None
        profile = self._profile()
        identity = delivery.intent.as_dict()["identity"]
        if str(identity["agent_id"]) != profile["agent_id"] or (kind, adapter, address) != (
            handle.kind,
            binding_adapter_for_handle(handle),
            handle.channel,
        ):
            return None

        bundle = self._manifest.as_dict()["bundle_digest"]
        params = {
            "agent_id": uuid.UUID(profile["agent_id"]),
            "kind": kind,
            "address": address,
            "version_id": uuid.UUID(profile["version_id"]),
            "bundle_ref": bundle["object_identity"],
            "bundle_sha256": bundle["sha256"],
        }
        async with self._engine.connect() as connection:
            result = await connection.execute(self._sql, params)
            rows = result.mappings().all()
        matches = matching_routes(rows, kind, address, adapter)
        if len(matches) != 1:
            return None
        row = matches[0]
        runner = profile["runner"]
        budget = runner["budget"]
        return ResolvedDeployment(
            agent_id=uuid.UUID(str(row["agent_id"])),
            agent_name=str(row["agent_name"]),
            deployment_id=None,
            workspace_enabled=False,
            version_id=uuid.UUID(str(row["version_id"])),
            version_label=str(row["version_label"]),
            bundle_ref=str(row["bundle_ref"]),
            max_usd_per_day=float(Decimal(budget["max_usd_per_day"])),
            max_output_tokens_per_run=int(budget["max_output_tokens_per_run"]),
            behavior_packs=None,
            model=runner["model"],
            reviewer_model=runner["reviewer_model"],
            thinking=runner["thinking"],
            runner_resources=None,
            deployment_environment=None,
            approval_required_tools=None,
            approval_routes=None,
            secrets=None,
            memory=False,
            memory_writes=False,
            endpoint=row["endpoint"],
            adapter=row["adapter"],
        )

    async def resolve_agent(self, agent_id: uuid.UUID) -> ResolvedDeployment | None:
        """Targetless cron is deliberately outside the first protected profile."""
        _ = agent_id
        self._delivery()
        return None

    def _boot_history_key(
        self, resolved: ResolvedDeployment, thread_key: str, *,
        kind: str | None, address: str | None, isolate_memory: bool,
    ) -> str:
        delivery = self._delivery()
        profile = self._profile()
        handle = delivery.turn.reply_handle
        if (
            handle is None
            or thread_key != _thread_key_for(delivery.turn)
            or resolved.agent_id != uuid.UUID(profile["agent_id"])
            or resolved.version_id != uuid.UUID(profile["version_id"])
            or resolved.bundle_ref != self._manifest.as_dict()["bundle_digest"]["object_identity"]
            or (kind, address) != (handle.kind, handle.channel)
            or isolate_memory
        ):
            raise ValueError("protected boot binding does not match the verified delivery")
        history_key = protected_history_key(delivery)
        parsed = parse_scoped_conversation_id(history_key)
        if parsed is None or parsed.kind != "@protected" or parsed.identity is not None:
            raise ValueError("protected transcript identity is invalid")
        return history_key

    def boot_env(
        self,
        resolved: ResolvedDeployment,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        isolate_memory: bool = False,
        caller_run: str | None = None,
        caller_work_item: str | None = None,
        caller_exp_ceiling: int | None = None,
        token_ttl_s: float | None = None,
    ) -> dict[str, str]:
        """Render a protected boot environment with transcript-only authority."""
        _ = caller_run, caller_work_item, caller_exp_ceiling
        profile = self._profile()
        history_key = self._boot_history_key(
            resolved, thread_key, kind=kind, address=address, isolate_memory=isolate_memory
        )
        parsed = parse_scoped_conversation_id(history_key)
        if parsed is None or parsed.kind != "@protected" or parsed.identity is not None:
            raise ValueError("protected transcript identity is invalid")
        binding = f"{parsed.kind}:{parsed.address}"
        runner = profile["runner"]
        bindings = profile["bindings"]
        ttl = runner["execution_deadline_seconds"]
        if token_ttl_s is not None:
            ttl = min(ttl, max(1, int(token_ttl_s)))
        expires_at = int(time.time()) + ttl
        if self._credentials is None:
            raise ValueError("protected credentials are unavailable")
        try:
            history_token = self._credentials.history_token(
                bindings["history_signing_credential_ref"],
                agent_id=str(resolved.agent_id),
                transcript_binding=binding,
                expires_at=expires_at,
            )
            model_credential = self._credentials.model_credential(runner["model_credential_ref"])
        except ProtectedCredentialUnavailable:
            raise ValueError("protected credentials are unavailable") from None
        if (
            type(history_token) is not str
            or not history_token
            or type(model_credential) is not str
            or not model_credential
            or len(model_credential) > 8192
        ):
            raise ValueError("protected credentials are unavailable")
        api_base = bindings["runner_facing_api_base_url"].rstrip("/")
        ref = (
            f"{api_base}/agents/{resolved.agent_id}/state/transcript/{quote(history_key, safe='')}"
        )
        budget = runner["budget"]
        env = BootEnv.render_worker(
            plugin_dir=runner["plugin_dir"],
            session_id=f"agent-{resolved.agent_id}-thread-{history_key}",
            budget=Budget(
                max_output_tokens_per_run=budget["max_output_tokens_per_run"],
                max_usd_per_day=float(Decimal(budget["max_usd_per_day"])),
            ),
            memory_ref="",
            history_ref=ref,
            bundle_ref=resolved.bundle_ref,
            runner_token=secrets.token_urlsafe(32),
            model=runner["model"],
            reviewer_model=runner["reviewer_model"],
            fake_model=False,
            base_url=runner["model_base_url"],
            api_backend=runner["api_backend"],
            thinking=runner["thinking"],
            model_env_key=runner["model_env_key"],
            history_token=history_token,
        )
        env.pop(BootEnv.env_key("memory_ref"), None)
        env[BootEnv.env_key("fake_model")] = "0"
        env[BootEnv.env_key("max_turns")] = str(runner["max_turns"])
        env[runner["model_env_key"]] = model_credential
        return env

    @staticmethod
    def turn_memory_token(*_args: Any, **_kwargs: Any) -> None:
        return None


class ProtectedMediatedBindingResolver(ProtectedBindingResolver):
    """The issuer assigns runner credentials; the worker supplies no boot env."""

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        """The first protected profile has no ordinary behavior packs."""
        delivery = self._delivery()
        handle = delivery.turn.reply_handle
        if handle is None or resolved.behavior_packs is not None:
            raise ValueError("protected behavior packs are unavailable")
        self._boot_history_key(
            resolved, _thread_key_for(delivery.turn), kind=handle.kind,
            address=handle.channel, isolate_memory=False,
        )
        return BehaviorPacks.from_config(None)

    def boot_env(
        self,
        resolved: ResolvedDeployment,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        isolate_memory: bool = False,
        caller_run: str | None = None,
        caller_work_item: str | None = None,
        caller_exp_ceiling: int | None = None,
        token_ttl_s: float | None = None,
    ) -> dict[str, str]:
        if any(value is not None for value in (caller_run, caller_work_item, caller_exp_ceiling)):
            raise ValueError("protected caller authority is unavailable")
        self._boot_history_key(
            resolved, thread_key, kind=kind, address=address, isolate_memory=isolate_memory
        )
        return {}
