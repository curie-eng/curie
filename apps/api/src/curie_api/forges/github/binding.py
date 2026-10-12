"""Resolve the agent route a GitHub repository and issue are bound to."""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.forges.registry import ForgeRegistry
from curie_api.forges.surfaces import bound_labels
from curie_api.github_factory_events import FactoryNotice, FactoryRefused
from curie_api.models import AgentChannel

# The refusal below has always been logged under the factory intake's logger.
logger = logging.getLogger("curie_api.github_factory")


GITHUB_CHANNEL_KIND = "github"


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
    grouped = await bound_labels(session, registry, GITHUB_CHANNEL_KIND, notice.repo_full_name)
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
