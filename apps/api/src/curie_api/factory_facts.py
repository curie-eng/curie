"""Read advisory facts from API state, without admission or observation producers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as redis
from pydantic import ValidationError
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .factory_facts_schema import (
    BindingFact,
    CredentialFact,
    CreditFact,
    ExecutableFact,
    FactoryFact,
    FactoryFacts,
    FactsRequest,
    FactsSubject,
    LabelRoute,
    RequestIdentity,
    ServiceFact,
    digest,
)
from .factory_facts_store import read_observation
from .forge_credential_health import code_host_health
from .forges.registry import forges_registry
from .forges.surfaces import bound_labels
from .killswitch import KillSwitch
from .models import AgentChannel, AgentVersion, Deployment

FACTS_DEADLINE_SECONDS = 45.0


async def facts_binding(
    session: AsyncSession,
    settings: Settings,
    repository: str,
    label: str,
) -> tuple[BindingFact, AgentChannel | None]:
    grouped = await bound_labels(session, forges_registry(settings), "github", repository)
    state = "unbound" if not grouped else "label_not_served"
    matches = grouped.get(label, [])
    if len(matches) > 1:
        state = "ambiguous"
    if len(matches) == 1:
        return BindingFact(state="bound", reason=None), matches[0]
    return BindingFact.model_validate({"state": state, "reason": state}), None


async def read_facts(
    session: AsyncSession,
    settings: Settings,
    valkey: redis.Redis,
    *,
    repository: str,
    repository_id: str,
    query: FactsRequest,
    issue_number: int | None,
    request: RequestIdentity | None = None,
    route: LabelRoute | None = None,
    now: datetime | None = None,
) -> FactoryFacts:
    now = now or datetime.now(UTC)
    binding_fact, binding = await facts_binding(session, settings, repository, query.trigger_label)
    identity: dict[str, Any] = {
        "binding": None,
        "agent": None,
        "bundle_version": None,
        "observations": [],
    }
    tools: list[ExecutableFact] = []
    services: list[ServiceFact] = []
    credit = CreditFact(
        state="unknown", reason="observation_missing", observed_at=None, expires_at=None
    )
    kill_state: str = "unknown"
    pause_state: str = "unknown"
    if binding is not None:
        version = await session.scalar(
            select(AgentVersion)
            .join(Deployment, Deployment.version_id == AgentVersion.id)
            .where(Deployment.agent_id == binding.agent_id, Deployment.status == "active")
            .order_by(desc(Deployment.deployed_at))
            .limit(1)
        )
        identity.update(
            binding=[str(binding.id), binding.generation],
            agent=str(binding.agent_id),
            bundle_version=None if version is None else [str(version.id), version.bundle_sha256],
        )
        if route is not None and settings.factory_model_auth_mode == "personal":
            # Tracker identity is not the verified personal owner context (ADR 0268).
            from types import SimpleNamespace

            from .personal_execution import new_work_item_owner

            owner = await new_work_item_owner(
                session,
                SimpleNamespace(
                    model_auth_mode="personal",
                    owner=None,
                    agent_id=binding.agent_id,
                ),
            )
            if owner is not None:
                route = route.model_copy(
                    update={"state": "ineligible", "reason": "personal_owner_required"}
                )
        for names, model in (
            (query.executables, ExecutableFact),
            (query.services, ServiceFact),
        ):
            for name in names:
                observation = await read_observation(
                    session,
                    kind="tool",
                    agent_id=binding.agent_id,
                    scope=repository_id,
                    item=name,
                    now=now,
                )
                identity["observations"].append(["tool", name, observation.subject_digest])
                fields: dict[str, Any] = {
                    "name": name,
                    "state": observation.state,
                    "reason": observation.reason,
                    "observed_at": observation.observed_at,
                }
                if model is ExecutableFact:
                    fields["basis"] = (
                        "selected_image_probe"
                        if observation.state in ("present", "absent")
                        else None
                    )
                if model is ExecutableFact:
                    try:
                        tools.append(ExecutableFact.model_validate(fields))
                    except ValidationError:
                        tools.append(
                            ExecutableFact(
                                name=name,
                                state="unknown",
                                basis=None,
                                reason="unreadable",
                                observed_at=None,
                            )
                        )
                else:
                    try:
                        services.append(ServiceFact.model_validate(fields))
                    except ValidationError:
                        services.append(
                            ServiceFact(
                                name=name, state="unknown", reason="unreadable", observed_at=None
                            )
                        )
        observation = await read_observation(
            session, kind="model_credit", agent_id=binding.agent_id, scope="", item="", now=now
        )
        identity["observations"].append(["model_credit", observation.subject_digest])
        try:
            credit = CreditFact.model_validate(
                {
                    "state": observation.state,
                    "reason": observation.reason,
                    "observed_at": observation.observed_at,
                    "expires_at": observation.expires_at,
                }
            )
        except ValidationError:
            credit = CreditFact(
                state="unknown", reason="unreadable", observed_at=None, expires_at=None
            )
        try:
            kill_state = (
                "engaged" if await KillSwitch(valkey).is_killed(binding.agent_id) else "clear"
            )
        except redis.RedisError:
            pass
    else:
        tools = [
            ExecutableFact(
                name=n, state="unknown", basis=None, reason="observation_missing", observed_at=None
            )
            for n in query.executables
        ]
        services = [
            ServiceFact(name=n, state="unknown", reason="observation_missing", observed_at=None)
            for n in query.services
        ]
    try:
        pause_state = "paused" if await valkey.exists(settings.upgrade_quiesce_key()) else "clear"
    except redis.RedisError:
        pass
    registry = forges_registry(settings)
    entries = [e for e in registry.config.code_hosts if e.kind == "github"]
    credential = CredentialFact(state="unknown", reason="not_probed", observed_at=None)
    if len(entries) == 1:
        entry = await code_host_health(registry, entries[0])
        credential = CredentialFact.model_validate(
            {
                "state": {"accepted": "healthy", "rejected": "unhealthy", "unknown": "unknown"}[
                    entry.authentication
                ],
                "reason": None
                if entry.authentication == "accepted"
                else "credential_rejected"
                if entry.authentication == "rejected"
                else "not_probed"
                if entry.reason == "not_probed"
                else "unreadable",
                "observed_at": None if entry.reason == "not_probed" else entry.observed_at,
            }
        )
    return FactoryFacts(
        schema_version=1,
        kind="factory_facts",
        admission="not_requested",
        deployment_readiness="unvalidated",
        request=request,
        subject=FactsSubject(
            repository_id=repository_id,
            issue_number=issue_number,
            trigger_label=query.trigger_label,
        ),
        observed_at=now,
        expires_at=now + timedelta(seconds=120),
        context_digest=digest(identity),
        binding=binding_fact,
        label_route=route,
        executables=tools,
        services=services,
        model_credit=credit,
        factory=FactoryFact.model_validate(
            {
                "enabled": settings.factory_enabled,
                "kill_switch": kill_state,
                "upgrade_pause": pause_state,
                "code_host_credential": credential,
            }
        ),
    )
