"""Resolve the agent route a GitHub repository and issue are bound to."""

import logging
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.forges.registry import ForgeRegistry
from curie_api.forges.surfaces import effective_trigger_label
from curie_api.github_factory_events import FactoryNotice, FactoryRefused
from curie_api.models import AgentChannel

# The refusal below has always been logged under the factory intake's logger.
logger = logging.getLogger("curie_api.github_factory")


GITHUB_CHANNEL_KIND = "github"


def github_reply_route(repo_full_name: str, issue_number: int) -> tuple[str, str, str]:
    """The reply kind, address, and conversation id a GitHub issue replies on."""

    return GITHUB_CHANNEL_KIND, repo_full_name, f"issue-{issue_number}"


async def bound_labels(
    session: AsyncSession, registry: ForgeRegistry, repo_full_name: str
) -> dict[str, list[AgentChannel]]:
    """The repository's GitHub surfaces, grouped by effective trigger label
    (#4361 decision 14).

    A label that maps to two surfaces is a run time collision (the tracker
    label was changed to equal an explicit label): it is still one label to
    list, and admission under it refuses.
    """

    rows = await session.scalars(
        select(AgentChannel)
        .where(
            AgentChannel.kind == GITHUB_CHANNEL_KIND,
            AgentChannel.address == repo_full_name,
        )
        .order_by(AgentChannel.agent_id)
    )
    grouped: dict[str, list[AgentChannel]] = {}
    for row in rows:
        label = effective_trigger_label(row, registry)
        if label is not None:
            grouped.setdefault(label, []).append(row)
    return grouped


async def agent_binding(
    session: AsyncSession, agent_id: uuid.UUID, repo_full_name: str
) -> AgentChannel | None:
    """The agent's GitHub surface on the repository, if it still holds one.

    One at most: `crud.channels.refuse_label_overlap` keeps an agent to one
    surface per repository.
    """

    found: AgentChannel | None = await session.scalar(
        select(AgentChannel)
        .where(
            AgentChannel.agent_id == agent_id,
            AgentChannel.kind == GITHUB_CHANNEL_KIND,
            AgentChannel.address == repo_full_name,
        )
        .limit(1)
    )
    return found


async def resolve_binding(
    session: AsyncSession, registry: ForgeRegistry, notice: FactoryNotice
) -> AgentChannel:
    """The surface whose effective trigger label is the notice's label.

    A GitHub surface alone makes its agent serve the repository (ADR 0214
    decision 1); `Agent.repo_full_name` is the git-flow bundle source (ADR 0091)
    and is not consulted here. A notice whose repository changed under it is
    refused by the repository id check `verify_current` performs, and the
    allowlist bounds what can be served.

    Two agents may serve one repository under different labels (#4361 decision
    8), so the label picks the surface. No surface for the label refuses
    ``binding_missing``; two surfaces resolving to one label (the tracker label
    was changed to equal an explicit one) also refuse ``binding_missing`` and
    log both agents, never picking one.
    """

    if notice.label is None:
        raise FactoryRefused("binding_missing")
    grouped = await bound_labels(session, registry, notice.repo_full_name)
    matches = grouped.get(notice.label, [])
    if not matches:
        raise FactoryRefused("binding_missing")
    if len(matches) > 1:
        logger.warning(
            "github factory refused %s label %r: agents %s all serve it",
            notice.repo_full_name,
            notice.label,
            ", ".join(str(row.agent_id) for row in matches),
        )
        raise FactoryRefused("binding_missing")
    return matches[0]
