"""Is a repository's git-flow routing still resolvable? (#1221, ADR-0091)

Migration 0018 dropped the unique index on ``agents.repo_full_name`` so one
repository can build several agents -- a dev bot and a prod bot are the same
bundle on two channels. The cost is that binding a SECOND agent to a repository
silently changes the OUTCOME of every future push for the agent that was
already bound: with no declared targets, ``resolve_target_agent`` has nothing to
say which of the two a branch deploys to, so it rejects. Nothing warned, and the
break showed up as pushes that quietly stopped deploying.

This endpoint is where a client asks the question instead of guessing at it. It
answers by CALLING the real resolver, never by restating its rule: a rule
restated in the CLI is a rule that drifts from the one a push actually enforces,
which is the same client/server drift #1212 exists to correct. It is read-only
-- it decides nothing and writes nothing.
"""

from fastapi import APIRouter, Depends

from curie_api.crud import agents as crud_agents
from curie_api.gitflow import target_agent_name
from curie_api.schemas.deployments import RoutingCheck, RoutingCheckProblem, RoutingCheckRequest

from ..auth import require_api_key
from ..config import get_settings
from ..deploy_target_parsing import parse_deploy_targets
from ..deps import SessionDep
from ..gitflow import TargetUnresolved, environment_for_ref, resolve_target_agent
from ..models import Environment

router = APIRouter(tags=["git-flow"], dependencies=[Depends(require_api_key)])


def _reachable_environments() -> list[Environment]:
    """The environments a real push to this installation can actually reach.

    Derived by ASKING ``environment_for_ref``, never by restating its rule --
    the same reason this endpoint calls the resolver instead of reimplementing
    it. ``environment_for_ref`` owns the ref-to-environment precedence and
    compares against ``dev_branch`` FIRST, so an installation that configures
    ``dev_branch`` and ``prod_branch`` to the SAME branch has a prod resolver no
    push can ever reach: the dev comparison always wins. A hardcoded
    (dev, prod) list evaluates prod anyway, and a prod-only target problem then
    reports the repository unresolvable over a push that CANNOT HAPPEN -- a
    warning an operator cannot act on, and the client/server divergence #1221
    exists to remove, merely relocated into this endpoint.

    Asking per configured deploy branch collapses that case to ``[dev]`` on its
    own, with no same-branch special case to keep in sync. Order is
    deterministic and dev-before-prod for the same reason `/deploy-targets/list`
    orders that way: a caller reading the list reads the earlier one first.
    """

    settings = get_settings()
    environments: list[Environment] = []
    for branch in (settings.dev_branch, settings.prod_branch):
        environment = environment_for_ref(f"refs/heads/{branch}", settings)
        if environment is not None and environment not in environments:
            environments.append(environment)
    return environments


@router.post("/git-flow/routing-check", response_model=RoutingCheck)
async def check_git_flow_routing(body: RoutingCheckRequest, session: SessionDep) -> RoutingCheck:
    """Report whether pushes to this repository still resolve to an agent.

    The arguments handed to the resolver are built exactly the way
    ``gitflow.process_push`` builds them, so the routing rule applied here is
    identical to a real push's and cannot drift. It is not an unconditional
    prophecy of the next push's outcome, though: this endpoint resolves
    against the caller's local ``deploy.yaml``, while a real push resolves
    against the pushed commit's -- a dirty working tree or a HEAD that has
    since moved can make the two disagree.

    A repository with several agents AND declared targets is the intended
    ADR-0091 configuration and reports resolvable; warning on it would be noise
    on exactly the shape the ADR asks operators to adopt.
    """

    repo_agents = await crud_agents.get_agents_by_repo(session, body.repo_full_name)

    # Parsing happens before the empty-repo_agents short-circuit below,
    # deliberately: whether the body is well-formed is request validation, and
    # request validation must not depend on database state. A malformed body
    # is a 400 whether or not anything happens to be bound to this repository.
    targets = parse_deploy_targets(body.content) if body.content is not None else None

    # Mirror process_push's own ignore guard (gitflow.py): with no agent bound
    # to this repository yet, a real push returns WebhookResult(status="ignored")
    # BEFORE the resolver is ever reached -- there is no routing decision to make.
    # Reproducing that short-circuit here, rather than letting an empty
    # repo_agents fall into resolve_target_agent's "0 agents" TargetUnresolved,
    # is what keeps this endpoint from being a second, disagreeing copy of the
    # routing rule (#1221, ADR-0091): an unbound repository is the state before
    # the first deploy, not a routing fault.
    if not repo_agents:
        return RoutingCheck(
            repo_full_name=body.repo_full_name,
            agent_count=0,
            agents=[],
            resolvable=True,
            unresolvable=[],
        )

    unresolvable: list[RoutingCheckProblem] = []
    for environment in _reachable_environments():
        named = target_agent_name(targets, environment)
        named_elsewhere = (
            await crud_agents.get_agent_by_name(session, named)
            if named and not any(a.name == named for a in repo_agents)
            else None
        )
        try:
            # A None return is NOT a problem: it means this branch declares no
            # target, which is deliberately ignored rather than rejected.
            resolve_target_agent(targets, environment, repo_agents, named_elsewhere)
        except TargetUnresolved as exc:
            unresolvable.append(
                RoutingCheckProblem(
                    environment=environment.value,
                    code=exc.code,
                    # Verbatim: the caller prints the resolver's own words rather
                    # than paraphrasing a rule it does not own.
                    message=str(exc),
                )
            )

    return RoutingCheck(
        repo_full_name=body.repo_full_name,
        agent_count=len(repo_agents),
        agents=[a.name for a in repo_agents],
        resolvable=not unresolvable,
        unresolvable=unresolvable,
    )
