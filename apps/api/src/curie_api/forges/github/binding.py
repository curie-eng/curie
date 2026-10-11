"""Resolve the agent route a GitHub repository and issue are bound to."""

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.crud import channels as crud_channels
from curie_api.github_factory_events import FactoryNotice, FactoryRefused
from curie_api.models import AgentChannel

# The refusal below has always been logged under the factory intake's logger.
logger = logging.getLogger("curie_api.github_factory")


GITHUB_CHANNEL_KIND = "github"


def github_reply_route(repo_full_name: str, issue_number: int) -> tuple[str, str, str]:
    """The reply kind, address, and conversation id a GitHub issue replies on."""

    return GITHUB_CHANNEL_KIND, repo_full_name, f"issue-{issue_number}"


_CHANNEL_KIND = GITHUB_CHANNEL_KIND


async def resolve_binding(session: AsyncSession, notice: FactoryNotice) -> AgentChannel:
    # A GitHub binding alone makes its agent serve the repository (ADR 0214
    # decision 1). `Agent.repo_full_name` is the git-flow bundle source
    # (ADR 0091) and is not consulted here. A notice whose repository changed
    # under it is refused by the repository id check `verify_current`
    # performs, and the allowlist bounds what can be served (decision 3).
    # `agent_channels_route_key` (migration 0070)
    # lets one repository hold several routes, so the query can return more
    # than one row; more than one route on the repository, on one agent or
    # several, still refuses binding_missing (decision 4). `_CHANNEL_KIND` is
    # `GITHUB_CHANNEL_KIND`, never Slack, and this notice names no adapter, so
    # `crud.channels.matching_bindings` with `adapter=None` keeps every row --
    # shared with every other reader of a route rather than a fourth copy of
    # the same rule.
    rows = list(
        await session.scalars(
            select(AgentChannel).where(
                AgentChannel.kind == _CHANNEL_KIND,
                AgentChannel.address == notice.repo_full_name,
            )
        )
    )
    matches = crud_channels.matching_bindings(rows, _CHANNEL_KIND, notice.repo_full_name, None)
    if not matches:
        raise FactoryRefused("binding_missing")
    if len(matches) > 1:
        # Two routes on one repository under this repo's agents: never pick one.
        logger.warning(
            "github factory refused %s: %d routes are bound to it",
            notice.repo_full_name,
            len(matches),
        )
        raise FactoryRefused("binding_missing")
    return matches[0]
