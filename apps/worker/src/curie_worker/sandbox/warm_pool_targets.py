"""Warm-pool version targets: exact active winner, pool refs, and lookup (#1492 D1).

A version pool is built for the deployment the cold path actually boots. That
winner is decided by ONE ordering key -- ``(environment = 'prod') DESC,
deployed_at DESC, id DESC`` -- duplicated verbatim from ``binding._RESOLVE_SQL``
and ``connector_loop._TARGETS_SQL`` (the #1216 rule: rank first, decide second).
A bundleless winner is reported and marked unrealizable; it is never skipped so
that a lower-ranked bundleful version could be warmed in its place, because the
sandbox would still boot the bundleless one and a warm pool for the runner-up
would serve a version no thread is bound to.

``bundle_sha256`` is not on ``ResolvedDeployment`` (the resolver does not need
it); this module reads it in the same SELECT as a worker-owned fact. No API
model, migration or public field changes.

Names for the template / pool / Secrets are derived here so W1, the realizer and
the chart assertions agree on them, but this module never creates, reads or
writes any Kubernetes object or Secret. The authority under which a realizer may
write a SandboxTemplate (and thereby reference Secrets) is a separately
unresolved prerequisite; ``derive_ref`` computing a name grants nothing.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from aci_protocol import BootEnv
from sqlalchemy import text

from ..binding import ResolvedDeployment
from .warm_pool_contract import (
    CapabilityProjection,
    CredentialGeneration,
    CredentialRevision,
    SecretKeyRef,
    warm_boot_projection,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from ..binding import BindingResolver


class TargetError(ValueError):
    """A target or ref cannot be formed; the caller stays cold."""


# Same joins and the SAME total ordering key as binding._RESOLVE_SQL /
# connector_loop._TARGETS_SQL; a test pins the ORDER BY byte-for-byte. Every
# agent column ResolvedDeployment reads is selected so the projection is computed
# from the facts boot_env would use, plus v.bundle_sha256 (worker-owned read).
# The bundle is REPORTED, never filtered on.
ACTIVE_WINNERS_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run,
       a.behavior_packs AS behavior_packs,
       a.model AS model,
       a.thinking AS thinking,
       a.approval_required_tools AS approval_required_tools,
       a.approval_routes AS approval_routes,
       a.secrets AS secrets,
       a.memory AS memory,
       d.id AS deployment_id,
       d.workspace_enabled AS workspace_enabled,
       CAST(d.environment AS text) AS environment,
       v.id AS version_id,
       v.version_label AS version_label,
       v.bundle_ref AS bundle_ref,
       v.bundle_sha256 AS bundle_sha256
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
{where}
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

_JSON_COLUMNS = ("behavior_packs", "approval_required_tools", "approval_routes", "secrets")


@dataclass(frozen=True, slots=True, eq=False)
class ActiveWinner:
    """The in-force deployment for one agent plus the worker-owned bundle digest.

    Equality and hashing use nonsecret identity only: ``resolved`` carries the
    local tier's connector-secret VALUES, so the generated dataclass comparison
    would compare credential bytes. ``repr`` likewise never prints ``resolved``.
    """

    resolved: ResolvedDeployment
    bundle_sha256: str | None
    environment: str

    def identity(self) -> tuple[str, str, str | None, str | None, str]:
        return (
            str(self.resolved.agent_id),
            str(self.resolved.version_id),
            None if self.resolved.deployment_id is None else str(self.resolved.deployment_id),
            self.bundle_sha256,
            self.environment,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ActiveWinner):
            return NotImplemented
        return self.identity() == other.identity()

    def __hash__(self) -> int:
        return hash(self.identity())

    def __repr__(self) -> str:  # never the connector-secret values on `resolved`
        return (
            f"ActiveWinner(agent_id={self.resolved.agent_id}, "
            f"version_id={self.resolved.version_id}, "
            f"deployment_id={self.resolved.deployment_id}, environment={self.environment!r})"
        )


def _winner_from_row(row: Any) -> ActiveWinner:
    data = dict(row)
    for column in _JSON_COLUMNS:
        value = data.get(column)
        if isinstance(value, str):
            data[column] = json.loads(value)
    bundle_sha256 = data.pop("bundle_sha256")
    environment = str(data.pop("environment"))
    return ActiveWinner(
        resolved=ResolvedDeployment.model_validate(data),
        bundle_sha256=bundle_sha256,
        environment=environment,
    )


async def active_winners(engine: AsyncEngine, schema: str) -> list[ActiveWinner]:
    """Every agent's in-force deployment, one row per agent."""

    sql = text(ACTIVE_WINNERS_SQL.format(schema=schema, where=""))
    async with engine.connect() as conn:
        rows = (await conn.execute(sql)).mappings().all()
    return [_winner_from_row(row) for row in rows]


async def active_winner(
    engine: AsyncEngine, schema: str, agent_id: uuid.UUID
) -> ActiveWinner | None:
    """One agent's in-force deployment, or None when it has no active deployment."""

    sql = text(ACTIVE_WINNERS_SQL.format(schema=schema, where="WHERE a.id = :agent_id"))
    async with engine.connect() as conn:
        row = (await conn.execute(sql, {"agent_id": agent_id})).mappings().first()
    return None if row is None else _winner_from_row(row)


@dataclass(frozen=True, slots=True)
class VersionPoolTarget:
    """What a version pool would be built for. Printable; carries no credentials."""

    namespace: str
    agent_id: str
    version_id: str
    deployment_id: str
    version_label: str
    bundle_ref: str | None
    bundle_sha256: str | None
    environment: str
    capability_generation: str
    projection: CapabilityProjection
    #: False means report-only: the winner exists but no pool may be realized
    #: for it (and none for any other version of this agent either).
    realizable: bool
    refusal: str | None


def build_target(
    winner: ActiveWinner,
    resolver: BindingResolver,
    *,
    namespace: str,
    model_credential_ref: SecretKeyRef | None,
    connector_secret_name: str | None,
    credential_generation: CredentialGeneration | None,
    credential_revisions: Mapping[str, CredentialRevision] | None = None,
) -> VersionPoolTarget:
    """Project the winner under the resolver's current defaults into a target."""

    resolved = winner.resolved
    if resolved.deployment_id is None:
        raise TargetError("active winner has no deployment id; refusing to name a pool for it")
    if not namespace:
        raise TargetError("a version pool target needs a namespace")
    projection = warm_boot_projection(
        resolver,
        resolved,
        bundle_sha256=winner.bundle_sha256,
        model_credential_ref=model_credential_ref,
        connector_secret_name=connector_secret_name,
        credential_generation=credential_generation,
        credential_revisions=credential_revisions,
    )
    refusal: str | None = None
    if not resolved.bundle_ref:
        refusal = "bundleless-winner"
    elif not winner.bundle_sha256:
        refusal = "bundle-sha256-missing"
    elif not projection.credential_authority_complete:
        refusal = "credential-authority-unverified"
    return VersionPoolTarget(
        namespace=namespace,
        agent_id=str(resolved.agent_id),
        version_id=str(resolved.version_id),
        deployment_id=str(resolved.deployment_id),
        version_label=resolved.version_label,
        bundle_ref=resolved.bundle_ref,
        bundle_sha256=winner.bundle_sha256,
        environment=winner.environment,
        capability_generation=projection.capability_generation,
        projection=projection,
        realizable=refusal is None,
        refusal=refusal,
    )


# RFC 1123 label, as Kubernetes validates object names used as pod-name prefixes.
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_MAX_LABEL = 63
# The Agent Sandbox controller derives Sandbox / pod / Service names FROM the
# pool name; those derived names must also fit the label limit. The suffix the
# vendored controller appends is not part of any contract we own, so a
# documented conservative budget is reserved here (adversarial cleared-attack 3,
# Fable D-2) rather than trusting the pool name alone to fit.
_CONTROLLER_SUFFIX_BUDGET = 12
_MAX_POOL_LABEL = _MAX_LABEL - _CONTROLLER_SUFFIX_BUDGET
# The chart's generic and per-agent pool shapes ({fullname}-runner-pool and
# {fullname}-agent-<name>-runner-pool) and the infix the CLI's Helm-revision
# recovery derives; version-pool names must never be mistaken for either.
_HELM_POOL_SUFFIX = "-runner-pool"
_HELM_AGENT_INFIX = "-agent-"
_INFIX = "-vp-"
_SUFFIXES = {"template": "-tpl", "pool": "-pool"}


def _validate_label(kind: str, name: str, *, limit: int = _MAX_LABEL) -> None:
    if not name or len(name) > limit or not _DNS_LABEL.match(name):
        raise TargetError(f"{kind} name {name!r} is not a valid label of at most {limit} chars")
    if name.endswith(_HELM_POOL_SUFFIX) or _HELM_AGENT_INFIX in name or name.endswith("-runner"):
        raise TargetError(f"{kind} name {name!r} reads as a chart Helm pool")


@dataclass(frozen=True, slots=True)
class SlotAllocation:
    """One bounded, chart-granted Secret slot (proposal D3, amendment B2, Fable D-1).

    Accepted ADR-0122 decision 4 needs exact-name Secret ``get`` that survives a
    worker restart and a second replica. Kubernetes ``resourceNames`` is a
    static list, so Secret names cannot be derived from a version or generation
    hash: they are ``{secret_prefix}-{index}`` for ``index < max_slots``, where
    both values are the chart-exported ``CURIE_WARM_BOOTSTRAP_SECRET_PREFIX`` /
    ``CURIE_WARM_BOOTSTRAP_MAX_SLOTS`` rendered from the SAME template variables
    as the Role's ``resourceNames``. Which generation currently occupies a slot
    is carried by labels, never by the name. Allocation, quarantine and
    exhaustion policy belong to the realizer; this type only refuses a name the
    grant could not cover. A slot's Secret may be written only while the slot is
    unoccupied (its previous pool and template deleted and quarantined); it is
    never updated in place under a live pool, because with static slot names
    that rewrite would be exactly the value rotation beneath running pods that
    Accepted ADR-0122 and Decision B forbid.
    """

    secret_prefix: str
    max_slots: int
    index: int

    def __post_init__(self) -> None:
        if not self.secret_prefix or not _DNS_LABEL.match(self.secret_prefix):
            raise TargetError(
                f"slot secret prefix {self.secret_prefix!r} is not a lowercase DNS label"
            )
        if self.max_slots <= 0:
            raise TargetError("slot bound must be a positive integer")
        if not 0 <= self.index < self.max_slots:
            raise TargetError(
                f"slot index {self.index} is outside the granted range 0..{self.max_slots - 1}"
            )
        _validate_label("slot secret", self.secret_name)

    @property
    def secret_name(self) -> str:
        return f"{self.secret_prefix}-{self.index}"


def slot_secret_names(secret_prefix: str, max_slots: int) -> tuple[str, ...]:
    """The exact bounded name list the chart must grant as ``resourceNames``.

    The chart assertion (proposal control 14) compares its rendered list to this
    function's output for the same prefix and bound; a realizer refuses to
    create any Secret name outside it even where the API would allow it.
    """

    return tuple(SlotAllocation(secret_prefix, max_slots, i).secret_name for i in range(max_slots))


@dataclass(frozen=True, slots=True)
class VersionPoolRef:
    """The deterministic object names of one (version, generation) pool.

    Template and pool names are generation-derived. The ONE granted slot Secret
    carries both the substrate-only bootstrap key (``bootstrap_secret_key``) and
    the generation's state-credential keys (``credential_secret_keys``): one
    Secret per generation, so the exact-name grant, quarantine and retirement
    have a single object to cover. Names only: existence, ownership and
    authority are decided by a realizer this module does not contain.
    """

    namespace: str
    version_id: str
    capability_generation: str
    slot_index: int
    template_name: str
    pool_name: str
    bootstrap_secret_name: str
    bootstrap_secret_key: str
    credential_secret_keys: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["credential_secret_keys"] = tuple(self.credential_secret_keys)
        return data


def _validate_prefix(prefix: str) -> None:
    if not prefix or not _DNS_LABEL.match(prefix):
        raise TargetError(f"pool name prefix {prefix!r} is not a lowercase DNS label")
    if (
        prefix.endswith(_HELM_POOL_SUFFIX)
        or _HELM_AGENT_INFIX in prefix
        or prefix.endswith("-runner")
    ):
        raise TargetError(f"pool name prefix {prefix!r} collides with the chart's Helm pool shapes")


def derive_ref(target: VersionPoolTarget, *, prefix: str, slot: SlotAllocation) -> VersionPoolRef:
    """Names for ``target``'s pool under ``prefix`` in the granted ``slot``.

    ``prefix`` must be the operator-owned name the chart exports for the
    worker, not a value derived by stripping a pool suffix (an operator
    override makes that derivation wrong). Refuses an unrealizable target, any
    prefix or name outside the label rules, a pool name without controller
    headroom, and a slot the grant could not cover.
    """

    if not target.realizable:
        raise TargetError(
            f"target is not realizable ({target.refusal}); no ref for a {target.refusal}"
        )
    _validate_prefix(prefix)
    version8 = uuid.UUID(target.version_id).hex[:8]
    stem = f"{prefix}{_INFIX}{version8}-{target.capability_generation[:12]}"
    names = {kind: f"{stem}{suffix}" for kind, suffix in _SUFFIXES.items()}
    _validate_label("template", names["template"])
    _validate_label("pool", names["pool"], limit=_MAX_POOL_LABEL)
    # Slot Secret names end in a digit; template/pool names end in -tpl/-pool,
    # so the three are disjoint by construction (pinned by a test, not a guard).
    return VersionPoolRef(
        namespace=target.namespace,
        version_id=target.version_id,
        capability_generation=target.capability_generation,
        slot_index=slot.index,
        template_name=names["template"],
        pool_name=names["pool"],
        bootstrap_secret_name=slot.secret_name,
        bootstrap_secret_key=BootEnv.env_key("runner_bootstrap_token"),
        credential_secret_keys=tuple(target.projection.credential_keys),
    )


class LookupOutcome(StrEnum):
    """W1 requires MATCH; ABSENT and MISMATCH both mean cold, never another pool."""

    MATCH = "match"
    ABSENT = "absent"
    MISMATCH = "mismatch"


@dataclass(frozen=True, slots=True)
class ObservedGeneration:
    """What a realizer/W1 observed on an existing template (labels), if any."""

    template_name: str
    version_id: str | None
    capability_generation: str | None


def lookup(observed: ObservedGeneration | None, target: VersionPoolTarget) -> LookupOutcome:
    """Compare an observed template identity against the CURRENT target.

    Pure. The caller re-runs it immediately before each warm claim with a fresh
    observation and a fresh target; a stale match is exactly the skew ADR-0122
    forbids. An unrealizable target never matches.
    """

    if not target.realizable:
        return LookupOutcome.MISMATCH
    if observed is None:
        return LookupOutcome.ABSENT
    if (
        observed.version_id == target.version_id
        and observed.capability_generation == target.capability_generation
    ):
        return LookupOutcome.MATCH
    return LookupOutcome.MISMATCH
