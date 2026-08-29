"""The closed vocabulary of fleet actions a control proposal may name (ADR-0133).

Three things live here, deliberately in one file, because they are three views of
the same decision and splitting them is how they drift:

- ``validate`` -- what parameters an action takes, and what values are legal.
- ``render`` -- the human-legible consequence line, computed from database facts.
- ``execute`` -- what actually happens, and only ever behind the platform key.

**Why rendering is here and not in the caller.** The control agent proposes an
action by naming it and naming a target agent id. It contributes no prose. The
API looks up what is actually true right now -- the agent's name, what version is
deployed, what the budget currently is -- and writes the sentence a human reads
before clicking execute. If the model wrote that sentence, a prompt injection
would not need tool access to cause damage: it would only need to make the
summary persuasive while the parameters said something else. Server-side
rendering makes the displayed consequence and the stored parameters the same
fact, derived once, from the store.

**Why the vocabulary is closed.** ``ACTIONS`` is the whole set. An action name
outside it is refused at create time rather than stored for the executor to
interpret later, so there is no path where an unrecognized string reaches
``execute``. Adding an action is a code change with a review, which is the point:
each entry widens what the control agent can ask a human to do.

Destructive-but-recoverable actions (kill, rollback, budget) are in scope.
Irreversible ones -- deleting an agent, rotating a credential, changing approval
policy -- are deliberately absent and belong on the console/CLI path only, where
no model is in the loop at all.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from aci_protocol import EvalJob
from sqlalchemy.ext.asyncio import AsyncSession

from . import crud
from .evalqueue import EvalQueue, now_iso
from .killswitch import KillSwitch
from .models import Agent, Environment
from .threadreset import ThreadResetRequests


@dataclass(frozen=True)
class ExecutionContext:
    """The platform handles an action may reach.

    A context object rather than a widening argument list: every action gets the
    same one, so adding an action that needs the eval queue does not re-sign
    every other action's execute function. The handles are the ones the
    equivalent CLI verb goes through, which is what keeps a button and a
    `curie` invocation the same operation rather than two implementations.
    """

    kill_switch: KillSwitch
    thread_resets: ThreadResetRequests | None = None
    eval_queue: EvalQueue | None = None
    default_eval_suite: str = "default"


class ProposalError(Exception):
    """A proposal that cannot be validated, rendered, or executed as written.

    Carries a caller-safe message: these surface as 4xx detail on the fleet
    router, and the control agent shows them to a human, so they say what is
    wrong rather than what went wrong internally.
    """


@dataclass(frozen=True)
class Prepared:
    """A proposal's parameters after validation, plus the rendered summary."""

    params: dict[str, Any]
    summary: str


def _require_no_params(params: dict[str, Any], action: str) -> None:
    if params:
        raise ProposalError(f"{action} takes no parameters, got {sorted(params)}")


def _environment(params: dict[str, Any]) -> Environment:
    """Parse the ``env`` parameter, defaulting to prod.

    Defaulting to prod rather than dev is intentional: a proposal is a request
    for a human to act on the fleet, and the fleet action worth gating is the
    production one. A dev-targeted proposal must say so.
    """

    raw = params.get("env", Environment.prod.value)
    try:
        return Environment(raw)
    except ValueError:
        raise ProposalError(
            f"env must be one of {[e.value for e in Environment]}, got {raw!r}"
        ) from None


async def _prepare_kill(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    _require_no_params(params, "kill")
    return Prepared(
        params={},
        summary=(
            f"Stop agent '{agent.name}' from starting new turns. "
            "In-flight turns finish; new messages go unanswered until it is resumed."
        ),
    )


async def _prepare_resume(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    _require_no_params(params, "resume")
    return Prepared(
        params={},
        summary=f"Let agent '{agent.name}' start new turns again.",
    )


async def _prepare_rollback(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """Roll an environment back to a version that agent already has.

    The version is resolved and checked to belong to this agent before the
    summary is written, so the rendered line names a real label rather than
    echoing an id the proposer supplied. A version id belonging to a DIFFERENT
    agent is refused here rather than at execute time: that would be a
    cross-agent deploy, and the proposal must not even be storable.
    """

    env = _environment(params)
    raw_version = params.get("version_id")
    if raw_version is None:
        raise ProposalError("rollback requires a version_id")
    try:
        version_id = uuid.UUID(str(raw_version))
    except ValueError:
        raise ProposalError(f"version_id must be a UUID, got {raw_version!r}") from None

    version = await crud.get_version(session, version_id)
    if version is None:
        raise ProposalError(f"no version {version_id}")
    if version.agent_id != agent.id:
        raise ProposalError(
            f"version {version_id} belongs to a different agent than '{agent.name}'"
        )

    current = await crud.get_active_deployment(session, agent.id, env)
    if current is None:
        raise ProposalError(f"agent '{agent.name}' has no active {env.value} deployment")
    if current.version_id == version_id:
        raise ProposalError(
            f"agent '{agent.name}' already runs version {version.version_label} in {env.value}"
        )
    current_version = await crud.get_version(session, current.version_id)
    from_label = current_version.version_label if current_version else str(current.version_id)

    return Prepared(
        params={"version_id": str(version_id), "env": env.value},
        summary=(
            f"Roll agent '{agent.name}' back in {env.value}: "
            f"version {from_label} -> version {version.version_label}."
        ),
    )


async def _prepare_set_budget(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """Change the agent's daily spend cap.

    ``max_usd_per_day: null`` is a real value meaning "fall back to the platform
    default", and the summary says so in words rather than printing "None", since
    the difference between "no cap of its own" and "a cap of zero" is exactly the
    thing a human must not misread.
    """

    unknown = set(params) - {"max_usd_per_day"}
    if unknown:
        raise ProposalError(f"set_budget takes max_usd_per_day, got extra {sorted(unknown)}")
    if "max_usd_per_day" not in params:
        raise ProposalError("set_budget requires max_usd_per_day")

    raw = params["max_usd_per_day"]
    if raw is None:
        new_value: float | None = None
    else:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ProposalError(f"max_usd_per_day must be a number or null, got {raw!r}")
        new_value = float(raw)
        if new_value < 0:
            raise ProposalError("max_usd_per_day cannot be negative")

    def describe(value: float | None) -> str:
        return "the platform default" if value is None else f"${value:.2f}/day"

    if agent.max_usd_per_day == new_value:
        raise ProposalError(
            f"agent '{agent.name}' is already at {describe(new_value)}"
        )

    return Prepared(
        params={"max_usd_per_day": new_value},
        summary=(
            f"Change the daily spend cap for agent '{agent.name}': "
            f"{describe(agent.max_usd_per_day)} -> {describe(new_value)}."
        ),
    )


async def _execute_kill(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    await ctx.kill_switch.kill(agent.id)
    return {"killed": True}


async def _execute_resume(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    await ctx.kill_switch.resume(agent.id)
    return {"killed": False}


async def _execute_rollback(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    """Promote-not-rebuild (ADR-0014): a rollback deploys bytes that already
    exist, so it ends the current deployment and appends a new one pointing at
    the older version. Nothing is rebuilt, which is what makes a rollback
    trustworthy -- the artifact going back out is the one that was tested."""

    env = Environment(params["env"])
    version_id = uuid.UUID(params["version_id"])
    current = await crud.get_active_deployment(session, agent.id, env)
    if current is None:
        raise ProposalError(f"agent '{agent.name}' has no active {env.value} deployment")
    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent.id:
        raise ProposalError(f"version {version_id} is no longer deployable for this agent")

    await crud.end_deployment(session, current)
    deployment = await crud.create_deployment_row(
        session,
        agent_id=agent.id,
        version_id=version_id,
        environment=env,
        commit_sha=version.commit_sha,
    )
    return {"deployment_id": str(deployment.id), "version_label": version.version_label}


async def _execute_set_budget(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    updated = await crud.update_budget(
        session, agent, params["max_usd_per_day"], agent.max_output_tokens_per_run
    )
    return {"max_usd_per_day": updated.max_usd_per_day}




# -- Actions added for the screen surface -------------------------------------
#
# Each mirrors a `curie` verb one-for-one, and goes through the same API
# capability the CLI does, so a button and a command are the same operation
# rather than two implementations that will drift.


_THINKING_LEVELS = ("disabled", "low", "medium", "high", "adaptive")


async def _prepare_set_model(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> overrides --model`. Null restores the platform default.

    The value is NOT validated against a list of known models: the vocabulary
    belongs to whatever provider the install points at (ADR-0048), so a
    hard-coded allowlist here would reject a model the platform can actually
    serve. It is validated as a non-empty string, and the rest is the runner's.
    """

    unknown = set(params) - {"model"}
    if unknown:
        raise ProposalError(f"set_model takes model, got extra {sorted(unknown)}")
    if "model" not in params:
        raise ProposalError("set_model requires model (or null for the platform default)")
    raw = params["model"]
    if raw is None:
        new_value: str | None = None
    else:
        if not isinstance(raw, str) or not raw.strip():
            raise ProposalError(f"model must be a non-empty string or null, got {raw!r}")
        new_value = raw.strip()
    if agent.model == new_value:
        raise ProposalError(
            f"agent '{agent.name}' already runs {new_value or 'the platform default'}"
        )

    def describe(value: str | None) -> str:
        return "the platform default model" if value is None else value

    return Prepared(
        params={"model": new_value},
        summary=(
            f"Change the model for agent '{agent.name}': "
            f"{describe(agent.model)} -> {describe(new_value)}. Takes effect on its next turn."
        ),
    )


async def _prepare_set_thinking(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> overrides --thinking`.

    Unlike the model, this IS a closed vocabulary here, because the value is a
    knob Curie defines rather than a provider identifier.
    """

    unknown = set(params) - {"thinking"}
    if unknown:
        raise ProposalError(f"set_thinking takes thinking, got extra {sorted(unknown)}")
    if "thinking" not in params:
        raise ProposalError("set_thinking requires thinking (or null for the platform default)")
    raw = params["thinking"]
    if raw is None:
        new_value: str | None = None
    elif isinstance(raw, str) and raw.strip() in _THINKING_LEVELS:
        new_value = raw.strip()
    else:
        raise ProposalError(
            f"thinking must be null or one of {list(_THINKING_LEVELS)}, got {raw!r}"
        )
    if agent.thinking == new_value:
        raise ProposalError(
            f"agent '{agent.name}' is already at {new_value or 'the platform default'}"
        )

    def describe(value: str | None) -> str:
        return "the platform default" if value is None else value

    return Prepared(
        params={"thinking": new_value},
        summary=(
            f"Change thinking depth for agent '{agent.name}': "
            f"{describe(agent.thinking)} -> {describe(new_value)}."
        ),
    )


async def _prepare_reset_thread(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> reset-thread`. Releases a wedged thread's sandbox.

    The summary states what is NOT lost, because that is the question a human
    actually has before clicking: the durable transcript survives, so the next
    message rehydrates the conversation into a fresh sandbox.
    """

    unknown = set(params) - {"thread_key"}
    if unknown:
        raise ProposalError(f"reset_thread takes thread_key, got extra {sorted(unknown)}")
    raw = params.get("thread_key")
    if not isinstance(raw, str) or not raw.strip():
        raise ProposalError("reset_thread requires a non-empty thread_key")
    thread_key = raw.strip()

    return Prepared(
        params={"thread_key": thread_key},
        summary=(
            f"Release the sandbox for agent '{agent.name}' thread {thread_key}. "
            "The next message cold-starts a fresh sandbox; the conversation "
            "transcript is kept and rehydrated, so no history is lost."
        ),
    )


async def _prepare_run_eval(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> eval`. Enqueues the same EvalJob the git-push fan-out does.

    Costs model tokens, which is why it is a proposal and not a free read.
    """

    unknown = set(params) - {"suite"}
    if unknown:
        raise ProposalError(f"run_eval takes suite, got extra {sorted(unknown)}")
    suite = params.get("suite")
    if suite is not None and (not isinstance(suite, str) or not suite.strip()):
        raise ProposalError(f"suite must be a non-empty string or null, got {suite!r}")

    deployment = await crud.get_active_deployment(session, agent.id, Environment.dev)
    if deployment is None:
        raise ProposalError(
            f"agent '{agent.name}' has no active dev deployment to evaluate"
        )
    version = await crud.get_version(session, deployment.version_id)
    label = version.version_label if version else str(deployment.version_id)

    return Prepared(
        params={"suite": suite.strip() if isinstance(suite, str) else None},
        summary=(
            f"Run the eval suite for agent '{agent.name}' against its dev version "
            f"{label}. This calls the model for every case and costs tokens."
        ),
    )


async def _prepare_remove_surface(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> surfaces --remove`. Unbind one channel from an agent.

    The last binding is refused here rather than at execute time, matching
    ``routers/agents.py::remove_agent_channel``: an agent with no binding is
    deployed, healthy-looking and unable to receive a turn, so a button that
    could produce that state should not render a proposal for it either.
    """

    unknown = set(params) - {"kind", "address"}
    if unknown:
        raise ProposalError(f"remove_surface takes kind and address, got extra {sorted(unknown)}")
    kind = str(params.get("kind", "")).strip()
    address = str(params.get("address", "")).strip()
    if not kind or not address:
        raise ProposalError("remove_surface requires kind and address")

    bindings = list(agent.channels)
    match = next(
        (b for b in bindings if b.kind == kind and b.address == address), None
    )
    if match is None:
        raise ProposalError(f"agent '{agent.name}' has no {kind}:{address} binding")
    if len(bindings) <= 1:
        raise ProposalError(
            f"{kind}:{address} is the last binding for '{agent.name}'; an agent "
            "with no binding cannot receive a turn. Add another first."
        )

    return Prepared(
        params={"kind": kind, "address": address},
        summary=(
            f"Unbind {kind}:{address} from agent '{agent.name}'. It stops "
            f"answering there and keeps its other {len(bindings) - 1} binding(s)."
        ),
    )


async def _execute_remove_surface(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    # Re-read under the same lock the CLI path uses, so two concurrent removals
    # cannot both see "two left" and leave the agent at zero.
    bindings = await crud.lock_agent_bindings(session, agent.id)
    match = next(
        (b for b in bindings if b.kind == params["kind"] and b.address == params["address"]),
        None,
    )
    if match is None:
        raise ProposalError("that binding is already gone")
    if len(bindings) <= 1:
        raise ProposalError("that is now the agent's last binding; refusing to leave it unroutable")
    await crud.delete_channel_binding(session, match)
    await session.commit()
    return {"unbound": f"{params['kind']}:{params['address']}"}


async def _prepare_delete_agent(
    session: AsyncSession, agent: Agent, params: dict[str, Any]
) -> Prepared:
    """`curie <tier> delete`. Operator-only and irreversible.

    Absent from ``ACTIONS`` on purpose: the control agent can never ask for
    this, whatever a message says. It exists only as a button an authenticated
    operator taps behind a typed confirmation, which is why it lives in
    ``OPERATOR_ONLY_ACTIONS``.
    """

    _require_no_params(params, "delete_agent")
    deployments = await crud.list_deployments(session, agent.id)
    versions = await crud.list_versions(session, agent.id)
    return Prepared(
        params={},
        summary=(
            f"Permanently delete agent '{agent.name}', with its "
            f"{len(versions)} version(s) and {len(deployments)} deployment "
            "record(s). This cannot be undone and the channel binding goes with it."
        ),
    )


async def _execute_set_model(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    updated = await crud.update_agent_model(session, agent, params["model"])
    return {"model": updated.model}


async def _execute_set_thinking(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    updated = await crud.update_agent_thinking(session, agent, params["thinking"])
    return {"thinking": updated.thinking}


async def _execute_reset_thread(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    if ctx.thread_resets is None:
        raise ProposalError("thread resets are not wired on this deployment")
    await ctx.thread_resets.request(params["thread_key"])
    return {"requested": True, "thread_key": params["thread_key"]}


async def _execute_run_eval(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    if ctx.eval_queue is None:
        raise ProposalError("the eval queue is not wired on this deployment")
    deployment = await crud.get_active_deployment(session, agent.id, Environment.dev)
    if deployment is None:
        raise ProposalError(f"agent '{agent.name}' has no active dev deployment to evaluate")
    version = await crud.get_version(session, deployment.version_id)
    if version is None:
        raise ProposalError("the deployed version no longer exists")
    suite = params.get("suite") or ctx.default_eval_suite
    stream_id = await ctx.eval_queue.enqueue(
        EvalJob(
            agent_id=agent.id,
            version_id=version.id,
            sha=version.commit_sha or version.bundle_sha256 or "",
            suite=suite,
            bundle_ref=version.bundle_ref,
            requested_at=now_iso(),
        )
    )
    return {"stream_id": stream_id, "suite": suite, "version_label": version.version_label}


async def _execute_delete_agent(
    session: AsyncSession, agent: Agent, params: dict[str, Any], ctx: ExecutionContext
) -> dict[str, Any]:
    name = agent.name
    await crud.delete_agent(session, agent.id)
    return {"deleted": name}


@dataclass(frozen=True)
class Action:
    name: str
    description: str
    prepare: Any
    execute: Any


def _index(actions: tuple[Action, ...]) -> dict[str, Action]:
    return {action.name: action for action in actions}


# What the CONTROL AGENT may put in front of a human. Every entry is
# recoverable: a killed agent resumes, a rolled-back version rolls forward, a
# budget changes again.
ACTIONS: dict[str, Action] = _index(
    (
        Action(
            name="kill",
            description="Stop an agent from starting new turns.",
            prepare=_prepare_kill,
            execute=_execute_kill,
        ),
        Action(
            name="resume",
            description="Let a killed agent start new turns again.",
            prepare=_prepare_resume,
            execute=_execute_resume,
        ),
        Action(
            name="rollback",
            description=(
                "Redeploy an earlier version the agent already has, in one environment."
            ),
            prepare=_prepare_rollback,
            execute=_execute_rollback,
        ),
        Action(
            name="set_budget",
            description="Change an agent's daily spend cap.",
            prepare=_prepare_set_budget,
            execute=_execute_set_budget,
        ),
        Action(
            name="set_model",
            description="Change which model an agent runs on.",
            prepare=_prepare_set_model,
            execute=_execute_set_model,
        ),
        Action(
            name="set_thinking",
            description="Change how deeply an agent reasons before answering.",
            prepare=_prepare_set_thinking,
            execute=_execute_set_thinking,
        ),
        Action(
            name="reset_thread",
            description="Release a wedged thread's sandbox; history is kept.",
            prepare=_prepare_reset_thread,
            execute=_execute_reset_thread,
        ),
        Action(
            name="run_eval",
            description="Run the agent's eval suite against its dev version.",
            prepare=_prepare_run_eval,
            execute=_execute_run_eval,
        ),
        Action(
            name="remove_surface",
            description="Unbind one channel from an agent (never its last).",
            prepare=_prepare_remove_surface,
            execute=_execute_remove_surface,
        ),
    )
)

# What ONLY a human's authenticated button tap may invoke. The control agent
# cannot propose these at any tier: ``propose`` looks in ``ACTIONS`` alone, so a
# request for one is refused as an unknown action, whatever the message said.
#
# The bar for membership is that the action cannot be walked back. A proposal is
# a thing a human reads and approves; that is a good gate for "stop the agent",
# and not a sufficient one for "destroy it and its history", where the mistake
# has no undo and the pressure to click is highest exactly when someone is
# annoyed at a misbehaving bot.
OPERATOR_ONLY_ACTIONS: dict[str, Action] = _index(
    (
        Action(
            name="delete_agent",
            description="Permanently delete an agent and its version history.",
            prepare=_prepare_delete_agent,
            execute=_execute_delete_agent,
        ),
    )
)

# Every action either surface can name. Used by the executor, which does not
# care how the row got there -- the gate is at creation, not execution.
ALL_ACTIONS: dict[str, Action] = {**ACTIONS, **OPERATOR_ONLY_ACTIONS}


def lookup(action: str, *, operator: bool = False) -> Action:
    """The action, or a ProposalError naming the legal vocabulary.

    ``operator=True`` widens the lookup to ``OPERATOR_ONLY_ACTIONS``. The
    default is the narrow set, so every path that forgets to pass it gets the
    agent-safe vocabulary -- the failure mode of a missed keyword is a refusal,
    not a privilege.

    Listing the alternatives in the error is what lets the control agent recover
    without a second round trip: it is talking to a human who asked for
    something, and "no such action" alone would make it guess."""

    table = ALL_ACTIONS if operator else ACTIONS
    try:
        return table[action]
    except KeyError:
        raise ProposalError(
            f"unknown action {action!r}; known actions are {sorted(table)}"
        ) from None


async def prepare(
    session: AsyncSession,
    agent: Agent,
    action: str,
    params: dict[str, Any],
    *,
    operator: bool = False,
) -> Prepared:
    prepared: Prepared = await lookup(action, operator=operator).prepare(session, agent, params)
    return prepared


async def execute(
    session: AsyncSession,
    agent: Agent,
    action: str,
    params: dict[str, Any],
    ctx: ExecutionContext,
) -> dict[str, Any]:
    # ``operator=True`` here is not a widening of authority: this function only
    # ever runs for an already-created row, and creation is where the two
    # vocabularies are told apart. An operator-only row exists solely because an
    # authenticated operator's button made it.
    result: dict[str, Any] = await lookup(action, operator=True).execute(
        session, agent, params, ctx
    )
    return result
