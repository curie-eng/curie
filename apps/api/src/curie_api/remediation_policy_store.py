"""The remediation policy store: generations, compare and swap, idempotency.

Every write (bind, arm, disarm, removal) runs in one transaction that

1. takes a transaction-scoped advisory lock keyed by agent and hook, and a
   share lock on the agent row so the agent cannot vanish mid-write;
2. answers a replayed ``operation_id`` with the generation it committed, or
   ``policy_operation_conflict`` when the intent differs;
3. compares ``expected_generation`` with the current generation (``0`` when the
   hook has none), else ``stale_policy_generation``;
4. runs the verb's own checks (a protected source policy for bind and arm, the
   approval route for bind, a bound policy for arm, disarm and removal);
5. writes generation ``max + 1`` as an immutable row naming the operator
   principal, and moves the current row to it.

Generation rows are never deleted while the agent exists (a trigger enforces
it), so ``max + 1`` never reuses a number. A removal writes a generation with
``active`` and ``armed`` false and no actions; the current row keeps it.

@spec AUTOMATED-REMEDIATION-1 @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Agent, HookSourcePolicy, RemediationPolicy, RemediationPolicyGeneration
from .remediation_policy_document import PolicyRefused, intent_sha256

Verb = Literal["bind", "arm", "disarm", "remove"]


@dataclass(frozen=True)
class PolicyGeneration:
    """One committed generation as the routes report it. @spec AUTOMATED-REMEDIATION-2."""

    agent_id: uuid.UUID
    hook: str
    generation: int
    armed: bool
    active: bool
    bound_by: str
    document: dict[str, Any]
    created_at: datetime


def _snapshot(row: RemediationPolicyGeneration) -> PolicyGeneration:
    return PolicyGeneration(
        agent_id=row.agent_id,
        hook=row.hook,
        generation=row.generation,
        armed=row.armed,
        active=row.active,
        bound_by=row.bound_by,
        document=dict(row.document),
        created_at=row.created_at,
    )


async def read_policy(session: AsyncSession, agent_id: uuid.UUID, hook: str) -> PolicyGeneration:
    """The current generation, or a refusal. @spec AUTOMATED-REMEDIATION-3."""
    if await session.get(Agent, agent_id) is None:
        raise PolicyRefused("agent_not_found", status_code=404)
    current = await session.get(RemediationPolicy, (agent_id, hook))
    if current is None:
        raise PolicyRefused("remediation_policy_absent", status_code=404)
    row = await session.get(RemediationPolicyGeneration, (agent_id, hook, current.generation))
    if row is None:  # pragma: no cover - the write keeps both rows in one transaction
        raise PolicyRefused("remediation_policy_absent", status_code=404)
    return _snapshot(row)


def _explicit_route(agent: Agent, route: str) -> None:
    """The route exists and names its approvers explicitly. @spec AUTOMATED-REMEDIATION-2.

    The channel-members fallback (no ``approvers`` block, or one with neither
    ``users`` nor ``group``) is refused: anyone in an alert channel would approve.
    """
    routes = agent.approval_routes or {}
    binding = routes.get(route)
    if not isinstance(binding, dict):
        raise PolicyRefused("route_unknown", "/route", "is not one of the agent's approval routes")
    approvers = binding.get("approvers")
    if not isinstance(approvers, dict) or not (approvers.get("users") or approvers.get("group")):
        raise PolicyRefused(
            "route_approvers_not_explicit",
            "/route",
            "the route must name approvers.users or approvers.group",
        )


async def _protected(session: AsyncSession, agent_id: uuid.UUID, hook: str) -> None:
    """A policy applies only to a hook with a protected source policy.

    @spec AUTOMATED-REMEDIATION-1.
    """
    source = await session.get(HookSourcePolicy, (agent_id, hook))
    if source is None or source.mode != "protected":
        raise PolicyRefused(
            "hook_not_protected",
            message="the hook's source policy is not protected",
            status_code=409,
        )


async def write_policy(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    hook: str,
    verb: Verb,
    expected_generation: int,
    operation_id: uuid.UUID,
    principal: str,
    document: dict[str, Any] | None = None,
) -> PolicyGeneration:
    """Commit one write and return its generation, or raise ``PolicyRefused``.

    ``document`` is the already validated policy for ``bind`` and ``None``
    otherwise. @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """
    intent = intent_sha256(verb, document)
    async with session.begin():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"remediation_policy:{agent_id}:{hook}"},
        )
        agent = (
            await session.execute(
                select(Agent).where(Agent.id == agent_id).with_for_update(read=True)
            )
        ).scalar_one_or_none()
        if agent is None:
            raise PolicyRefused("agent_not_found", status_code=404)

        replayed = (
            await session.execute(
                select(RemediationPolicyGeneration).where(
                    RemediationPolicyGeneration.agent_id == agent_id,
                    RemediationPolicyGeneration.hook == hook,
                    RemediationPolicyGeneration.operation_id == operation_id,
                )
            )
        ).scalar_one_or_none()
        if replayed is not None:
            if replayed.intent_sha256 != intent:
                raise PolicyRefused(
                    "policy_operation_conflict",
                    message="this operation_id committed a different intent",
                    status_code=409,
                )
            return _snapshot(replayed)

        current = (
            await session.execute(
                select(RemediationPolicy)
                .where(RemediationPolicy.agent_id == agent_id, RemediationPolicy.hook == hook)
                .with_for_update()
            )
        ).scalar_one_or_none()
        current_generation = current.generation if current is not None else 0
        if expected_generation != current_generation:
            raise PolicyRefused(
                "stale_policy_generation",
                message=f"the current generation is {current_generation}",
                status_code=409,
            )

        previous: RemediationPolicyGeneration | None = None
        if current is not None:
            previous = await session.get(
                RemediationPolicyGeneration, (agent_id, hook, current.generation)
            )

        if verb == "bind":
            assert document is not None
            await _protected(session, agent_id, hook)
            _explicit_route(agent, str(document["route"]))
            armed = bool(current is not None and current.active and current.armed)
            active = True
            stored = document
        else:
            if current is None or previous is None:
                raise PolicyRefused(
                    "remediation_policy_absent",
                    message="no remediation policy is bound to this hook",
                    status_code=409,
                )
            if verb == "arm":
                if not current.active:
                    raise PolicyRefused(
                        "remediation_policy_absent",
                        message="the remediation policy was removed",
                        status_code=409,
                    )
                await _protected(session, agent_id, hook)
                armed, active, stored = True, True, dict(previous.document)
            elif verb == "disarm":
                armed, active, stored = False, current.active, dict(previous.document)
            else:
                stored = {**previous.document, "actions": []}
                armed, active = False, False

        highest = (
            await session.execute(
                select(func.max(RemediationPolicyGeneration.generation)).where(
                    RemediationPolicyGeneration.agent_id == agent_id,
                    RemediationPolicyGeneration.hook == hook,
                )
            )
        ).scalar_one()
        generation = max(int(highest or 0), current_generation) + 1

        row = RemediationPolicyGeneration(
            agent_id=agent_id,
            hook=hook,
            generation=generation,
            operation_id=operation_id,
            intent_sha256=intent,
            document=stored,
            armed=armed,
            active=active,
            bound_by=principal,
        )
        session.add(row)
        if current is None:
            session.add(
                RemediationPolicy(
                    agent_id=agent_id,
                    hook=hook,
                    generation=generation,
                    operation_id=operation_id,
                    armed=armed,
                    active=active,
                )
            )
        else:
            current.generation = generation
            current.operation_id = operation_id
            current.armed = armed
            current.active = active
            current.updated_at = func.now()
        await session.flush()
        await session.refresh(row)
        return _snapshot(row)
