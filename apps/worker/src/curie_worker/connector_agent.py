"""Reconcile one agent's connectors (ADR-0090, #1184).

Joins the three halves that already exist: the API renders, `connector_reconcile`
decides, `connector_apply` carries it out. What lives here is the part none of
them can know on its own -- which objects are this agent's to manage, and when
reconciling would do more harm than leaving it alone.

**The credential hazard, which is the reason this module is not three lines.**
The API's render emits a Service, a Deployment and two NetworkPolicies per
connector. It never emits the Secret. For a connector using the owned form
(`secrets: [NAME]`) that Secret is minted by `curie cluster deploy`, from values
only the operator's environment holds -- the cluster deliberately does not have
them (ADR-0086), so the reconciler cannot render it and never will.

That Secret carries `curie.dev/connector-owner`, because the CLI stamps the same
label the reconciler prunes on. So the naive loop -- list what we own, delete
what we no longer declare -- **deletes the credential the CLI provisioned**, and
every connector pod fails on its next restart with a missing secretKeyRef. The
object is owned, it is genuinely undeclared, and pruning it is exactly what the
plan is supposed to do. Nothing downstream can catch this; it has to be handled
here.

So the owned Secret THE CURRENT RENDER NAMES is protected: never applied (we
have no values) and never pruned (someone else owns its lifecycle). And if it is
protected but absent, the pass suppresses every apply -- applying a Deployment
whose secretKeyRef points at a Secret that does not exist produces pods that
never start, which is worse than not acting. It still prunes: objects the
in-force version no longer declares are deleted as usual, because removing an
object is never harmed by a missing Secret. Holding those deletes back only left
a withdrawn connector running for as long as the agent stayed unprovisioned
(#1214).

That prune has one exclusion, and only on that branch: no Secret of ANY name is
manageable there. The branch is entered because the named Secret is absent, so
an owner-labelled Secret still live at that moment is by construction one whose
name the render no longer matches -- and the name is release-scoped, so a
re-release, or drift between the worker's `CURIE_RELEASE` and the CLI's
`--release`, is enough to produce exactly that. It is the operator's real
credential under a stale name, not garbage, and it is unrecoverable. On the
normal path a stale-named owned Secret is still pruned as usual.

Connectors using the reference form (`secrets: [{from_secret: ...}]`, #1163)
have no owned keys at all, so they reconcile with no exception. That is the
path ADR-0090 calls the prerequisite, and it is why this restriction shrinks as
bundles adopt it rather than being permanent.

`prune_agent` is the same reasoning reached from the other direction (#1216).
When the in-force version has no stored bundle there is no render at all -- not
an empty one, an absent one -- so nothing this agent owns can be declared, and
the same no-Secret-of-any-name exclusion applies for the same reason: without a
render we do not know which Secret name is ours, and the one live there is the
operator's real, unrecoverable credential.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from .connector_apply import ApplyReport, ConnectorClient, execute
from .connector_reconcile import OWNER_LABEL, ReconcilePlan, identity, plan

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RenderedConnectors:
    """What the API returns for one version's `connectors.yaml`."""

    manifests: list[dict[str, Any]] = field(default_factory=list)
    # The Secret Curie owns for this agent, and the keys whose VALUES a caller
    # must supply. Non-empty means the CLI owns that Secret's lifecycle.
    owned_secret_name: str = ""
    owned_secret_keys: list[str] = field(default_factory=list)

    @property
    def needs_operator_credentials(self) -> bool:
        return bool(self.owned_secret_keys)


class ManifestSource(Protocol):
    """Where rendered connector objects come from.

    A seam, so the reconcile step is testable without an API. Rendering stays
    in the API by ADR-0090 -- duplicating it here would recreate exactly the
    drift `connectors.yaml` exists to prevent.
    """

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors: ...


@dataclass(frozen=True)
class AgentOutcome:
    """What reconciling one agent did. `skipped` names a suppressed apply half,
    not an inert pass -- it still carries the deletes that ran and any failures.
    """

    agent: str
    skipped: str | None = None
    plan: ReconcilePlan | None = None
    report: ApplyReport | None = None

    @property
    def ok(self) -> bool:
        return self.report is None or self.report.ok


def own(obj: dict[str, Any], agent: str) -> dict[str, Any]:
    """Stamp the ownership label the whole prune path keys on.

    The renderer does not set it -- it is a deployment fact, not a bundle fact,
    and the CLI applier stamps it at the same point. An object applied without
    it is invisible to every future prune, so the applier refuses to write one;
    this is where the reconciler earns the right to.
    """

    stamped = dict(obj)
    metadata = dict(stamped.get("metadata") or {})
    metadata["labels"] = {**(metadata.get("labels") or {}), OWNER_LABEL: agent}
    stamped["metadata"] = metadata
    return stamped


def _execute_and_report(
    client: ConnectorClient,
    carried: ReconcilePlan,
    *,
    agent: str,
    namespace: str,
    skipped: str | None = None,
) -> AgentOutcome:
    """Run a decided plan and turn it into an outcome.

    Exists so the failure-log format lives in exactly one place: every caller
    here deletes, so every caller can fail deletes, and a finalizer or an RBAC
    gap recurs every pass. Returning straight off `execute()` left the object
    name and the error in no log stream at all -- `connector_apply` does not log
    delete failures either. It deliberately does NOT own the `is_noop` fast
    path: the two callers disagree on what a no-op returns (see each).
    """

    report = execute(client, carried, namespace=namespace, agent=agent)
    if not report.ok:
        logger.warning(
            "connector reconcile agent=%s had %d failure(s): %s",
            agent,
            len(report.failures),
            "; ".join(f"{kind}/{name}: {err}" for kind, name, err in report.failures),
        )
    return AgentOutcome(agent=agent, skipped=skipped, plan=carried, report=report)


def reconcile_agent(
    source: ManifestSource,
    client: ConnectorClient,
    *,
    agent: str,
    agent_id: str,
    version_id: str,
    namespace: str,
) -> AgentOutcome:
    """Converge one agent's connectors toward its in-force version."""

    rendered = source.rendered(agent_id=agent_id, version_id=version_id)
    desired = [own(obj, agent) for obj in rendered.manifests]
    live = client.list_owned(namespace, agent)

    protected: set[tuple[str, str]] = set()
    unprovisioned = False
    if rendered.needs_operator_credentials and rendered.owned_secret_name:
        protected.add(("Secret", rendered.owned_secret_name))
        unprovisioned = not any(identity(obj) in protected for obj in live)
        if not unprovisioned:
            logger.debug(
                "connector reconcile agent=%s protecting its operator-supplied secret", agent
            )

    # Hidden from the plan rather than filtered out of its result: the plan's job
    # is "own it and no longer declare it -> delete it", and that judgement is
    # correct. What is wrong is calling this object ours to begin with.
    manageable = [obj for obj in live if identity(obj) not in protected]

    if unprovisioned:
        # Widened past `protected` (only THIS render's Secret name) for the
        # reason the module docstring gives. Kind is the safe discriminator
        # here, and only here: on this branch no Secret is ours.
        manageable = [obj for obj in manageable if identity(obj)[0] != "Secret"]

    computed = plan(desired, manageable, agent=agent)

    skipped: str | None = None
    if unprovisioned:
        # Applying now would create Deployments referencing a Secret that does
        # not exist. Pods would stay stuck rather than fail loudly, and the
        # cause would be several layers from the symptom. So the applies are
        # dropped and only the delete half runs -- an object being removed does
        # not care whether a credential exists. `computed.delete` is decided
        # against the FULL render, so a still-declared connector's objects are
        # never in it, and no Secret of any name reaches it (above).
        #
        # A RENAMED connector, or a changed release name, is the case this does
        # not spare: its old objects are undeclared and get pruned while their
        # replacements are apply-suppressed, so the pass ends with no connector
        # where before it kept the stale one. Accepted, deliberately. The old
        # connector is already gone from the agent's MCP config, which is
        # rendered from the same in-force version, so those objects were
        # unreachable rather than working -- and leaving them live is exactly the
        # leak #1214 exists to close. Do NOT gate the prune on `computed.apply`
        # being empty to dodge this: a never-yet-deployed agent has every object
        # pending apply, so that gate suppresses pruning in the common case.
        #
        # The message deliberately names neither the Secret nor its keys.
        # Both are identifiers rather than values -- `kubectl get secret`
        # shows them -- but they are also of no use here: the operator's
        # action is the same either way, and `curie cluster deploy` is what
        # reports a key it cannot resolve. Leaving them out keeps a credential
        # name out of a log stream that may be shipped somewhere less
        # protected than the cluster.
        reason = (
            "connector credentials are operator-supplied and not yet "
            "provisioned in this namespace; run `curie cluster deploy` once "
            "for this agent, or move the connector to a referenced secret"
        )
        # Not logged here: a skip recurs every pass until someone acts, so the
        # loop logs it once per transition instead (#1215).
        skipped = reason
        # The plan carried is the one actually executed, not the one computed --
        # reporting applies that were never attempted would make the outcome
        # describe a pass that did not happen.
        carried = ReconcilePlan(delete=computed.delete)
    elif computed.is_noop:
        return AgentOutcome(agent=agent, plan=computed, report=ApplyReport())
    else:
        carried = computed

    return _execute_and_report(
        client, carried, agent=agent, namespace=namespace, skipped=skipped
    )


def prune_agent(
    client: ConnectorClient,
    *,
    agent: str,
    namespace: str,
) -> AgentOutcome:
    """Converge an agent whose in-force version declares nothing we can read.

    Reached when the version the deployment points at has no stored bundle
    (#1216). There is no render to ask for -- the API answers "no bundle stored
    for this version" -- so the desired set is empty, and every object this
    agent owns is by definition undeclared. Pruning them is the correct
    convergence, not a fallback: the alternative the loop used to take was to
    quietly reconcile a LOWER-precedence bundled version instead, which left the
    cluster serving connectors from a version no sandbox boots.

    **No Secret of any name is manageable here**, exactly as on the
    `unprovisioned` branch above and for the same reason. The owned Secret is
    minted by `curie cluster deploy` from values the cluster deliberately does
    not hold (ADR-0086); only a render names it, and there is no render on this
    path. So an owner-labelled Secret alive here cannot be told apart from the
    operator's live credential, and deleting it is unrecoverable and breaks
    every connector pod on its next restart. Kind is the safe discriminator
    here, and only here.

    The outcome is `skipped` rather than a plain reconcile: the applies were not
    merely unnecessary, they were suppressed by a condition an operator can fix
    (push the bundle, or deploy a version that has one), so the pass summary
    must say so instead of reporting a quiet prune.
    """

    live = client.list_owned(namespace, agent)
    manageable = [obj for obj in live if identity(obj)[0] != "Secret"]
    computed = plan([], manageable, agent=agent)

    # Named without the version id: the operator's action is the same either
    # way, and `curie agent deploy` is what reports the version it stored.
    reason = (
        "the in-force version has no stored bundle, so no connectors can be "
        "rendered for it; objects this agent still owns were pruned"
    )
    if computed.is_noop:
        # Converged: nothing owned, nothing to remove. Returning an empty report
        # rather than calling execute() keeps the steady state free of a
        # cluster round-trip, matching reconcile_agent's no-op early return.
        return AgentOutcome(agent=agent, skipped=reason, plan=computed, report=ApplyReport())

    return _execute_and_report(
        client, computed, agent=agent, namespace=namespace, skipped=reason
    )
