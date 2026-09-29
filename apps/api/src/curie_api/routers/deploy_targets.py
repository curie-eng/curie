"""Resolve a named deploy target from a bundle's ``deploy.yaml`` (ADR-0089).

Its own router because it belongs to no agent and no version: it is a pure
function of the text posted to it, with no database, no bundle store, and no
cluster. Hanging it off the bundle router would have given it the prefix
``/agents/{agent_id}/versions/{version_id}/bundle/...``, which is exactly
backwards -- the caller uses this BEFORE an agent exists, to find out which
agent to create.
"""

from fastapi import APIRouter, Depends, HTTPException, status

from ..auth import require_api_key
from ..deploy_target_parsing import parse_deploy_targets as _parse
from ..schemas import ListedTargets, NamedTarget, ResolvedTarget, ResolveTargetRequest

router = APIRouter(tags=["deploy-targets"], dependencies=[Depends(require_api_key)])

# `_parse` is imported rather than defined here: the git-flow routing check
# (#1221) needs the same parser, and a second copy of it would be the second
# parser ADR-0089 exists to prevent. See `deploy_target_parsing`.


@router.post("/deploy-targets/resolve", response_model=ResolvedTarget)
async def resolve_deploy_target(body: ResolveTargetRequest) -> ResolvedTarget:
    """Resolve a named target from a bundle's ``deploy.yaml`` (ADR-0089).

    Pure function of the supplied text: no database, no store, no cluster. It
    lives here rather than in the CLI so there is exactly ONE parser for this
    format. A second implementation in Rust could disagree with this one about
    the same file, and the file's entire purpose is to be unambiguous about
    where a deploy lands -- a disagreement would route a deploy somewhere the
    author did not intend and report success.

    Validation errors are returned rather than swallowed, because every one of
    them describes a deploy that would otherwise succeed against the wrong
    agent, environment, or channel.
    """

    parsed = _parse(body.content)

    target = parsed.targets.get(body.target)
    if target is None:
        known = ", ".join(sorted(parsed.targets)) or "(none declared)"
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"no target named {body.target!r} in deploy.yaml. Declared: {known}",
        )
    return ResolvedTarget(
        agent=target.agent,
        env=target.env,
        slack_channel=target.slack_channel,
        identity=target.identity,
        connectors=target.connectors,
    )


@router.post("/deploy-targets/list", response_model=ListedTargets)
async def list_deploy_targets(body: ResolveTargetRequest) -> ListedTargets:
    """Every target a ``deploy.yaml`` declares (ADR-0089).

    The sibling of `resolve` above, and here for the same reason: onboarding a
    repository means deploying ALL its targets, and a caller that had to parse
    the file to enumerate them would be the second parser this endpoint exists
    to prevent.

    ``target`` on the request is ignored -- the shape is shared so the CLI has
    one request type for both calls.

    Ordered dev before prod. A caller deploying in sequence that fails part-way
    then leaves prod on its previous version rather than ahead of a dev that
    never landed; recoverable one way and not the other.
    """

    parsed = _parse(body.content)
    order = {"dev": 0, "prod": 1}
    named = [
        NamedTarget(
            name=name,
            agent=t.agent,
            env=t.env,
            slack_channel=t.slack_channel,
            identity=t.identity,
            connectors=t.connectors,
        )
        for name, t in parsed.targets.items()
    ]
    named.sort(key=lambda t: (order.get(t.env, 2), t.name))
    return ListedTargets(targets=named)
