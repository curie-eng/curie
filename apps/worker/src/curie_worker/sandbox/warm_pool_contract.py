"""Warm-pool capability projection: the immutable identity of a version pool (#1492 D1).

Accepted ADR-0122 makes a runner bootstrap credential per VERSION POOL and says a
credential change is a NEW pool, never a mutation. The same rule has to hold for
every other input a warm pod boots with: model, thinking, budget, approval gates,
connector scope, bundle identity, memory ref. Those live on the mutable ``Agent``
row and the worker's own defaults, and ``BindingResolver.boot_env`` overlays them
live on every cold claim. A warm template cannot be overlaid per claim
(``envVarsInjectionPolicy: Overrides`` forces a cold create for any per-claim
env), so the template must carry a frozen snapshot of that overlay, and the pool
is only usable while the live overlay still equals the snapshot.

``capability_generation`` is the SHA-256 of a canonical JSON projection of the
version-stable subset of that boot env. It is computed FROM the real
``boot_env`` render rather than from a second, hand-maintained list of the same
policy, so the precedence rules (agent pin beats worker default, connector
scope is a set or nothing, no-key installs mint no tokens) cannot drift between
the cold path and the warm template. Every key the render can emit is
CLASSIFIED here; an unclassified key refuses the projection instead of being
guessed into or out of the hash.

Credentials are identity only:

- the model credential and per-agent connector secrets carry their
  ``secretKeyRef`` NAME/KEY plus authoritative Secret UID/resourceVersion;
- the agent-scoped ``state`` / ``state.app`` tokens (ADR-0033) are a nonsecret
  ``CredentialGeneration`` -- agent, scopes, issued/expiry window -- whose values
  are minted later with the EXISTING ``sandbox_token`` codec into a protected
  Secret. A renewal is a new generation, so a new pool; nothing rotates under a
  running pod. The signing-key Secret's revision is also part of the projection;
- the per-claim ``runner_token`` and the pool bootstrap value are never in the
  projection at all: the bootstrap arrives by ``secretKeyRef`` and the
  conversation credential arrives on the existing adoption call (W1).

This module creates, reads and writes nothing: no Secret, no Template, no Pool,
no Postgres, no Valkey. It is the shared helper the realizer and W1's adoption
path both call, and the contract the review of the Template writer's authority
(a separately unresolved prerequisite) can assume.

The metadata producer must independently observe the actual credential sources
and recheck them for each claim. This module does not provide that producer or
verify a caller's provenance. Missing metadata keeps a target unrealizable and
a claim cold; fixed Secret names alone never manufacture a generation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from aci_protocol import BootEnv

from .. import sandbox_token
from ..binding import (
    DECISION_ENV,
    FALSE_COMPLETION_CHECK_ENV,
    GRANT_TOOL_ENV,
    MEMORY_REF_ENV,
    RESUMED_KIND_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
)
from ..workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV

if TYPE_CHECKING:
    from ..binding import BindingResolver, ResolvedDeployment

logger = logging.getLogger(__name__)

#: Bump only with a recorded reason: it changes every generation on every install.
# V2 includes authoritative credential source revisions. A fixed Secret name
# cannot identify what an already-running pod loaded before a Secret update.
PROJECTION_VERSION = 2

#: A warm claim is refused this many seconds BEFORE the generation's credentials
#: expire, so a turn admitted at the edge cannot lose its state tokens mid-turn.
#: Sized to the worker's configurable runner ceiling (``runner_total_timeout_s``
#: is capped at 1800 s); a caller with a longer worst case passes a larger margin
#: to ``classify_claim``. The realizer must replace generations with at least
#: this much headroom or every turn near the edge goes cold.
DEFAULT_CREDENTIAL_ADMISSION_MARGIN_SECONDS = 1800

#: The thread key handed to ``boot_env`` when projecting. It only ever lands in
#: per-conversation keys, which the projection drops; a test pins that it never
#: reaches the canonical JSON.
PROJECTION_THREAD_SENTINEL = "warm-pool-projection"

# Mirrors apps/api routers/state.py (STATE_SCOPE / STATE_APP_SCOPE) and the
# scope strings binding.boot_env mints with; they are the existing codec's only
# accepted state scopes.
STATE_SCOPE = "state"
STATE_APP_SCOPE = "state.app"

_SESSION_ENV = BootEnv.env_key("session_id")
_HISTORY_REF_ENV = BootEnv.env_key("history_ref")
_RUNNER_TOKEN_ENV = BootEnv.env_key("runner_token")
_HISTORY_TOKEN_ENV = BootEnv.env_key("history_token")
_MEMORY_TOKEN_ENV = BootEnv.env_key("memory_token")
_STATE_TOKEN_ENV = BootEnv.env_key("state_token")
_STATE_URL_ENV = BootEnv.env_key("state_url")
_CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")
_CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")

#: The env keys a protected credential-generation Secret carries, in order, and
#: the scope each is minted with. ``history``/``memory`` share the broad state
#: scope and the bundle-facing state token gets the narrow one, exactly as
#: ``boot_env`` mints them today.
CREDENTIAL_KEY_SCOPES: Mapping[str, str] = MappingProxyType(
    {
        _HISTORY_TOKEN_ENV: STATE_SCOPE,
        _MEMORY_TOKEN_ENV: STATE_SCOPE,
        _STATE_TOKEN_ENV: STATE_APP_SCOPE,
    }
)


class ProjectionError(ValueError):
    """The boot env cannot be projected into a warm template; stay cold."""


class EnvClass(StrEnum):
    """How one boot-env key relates to a warm template."""

    #: Same for every conversation of this version; part of the hash and the template.
    VERSION_STABLE = "version-stable"
    #: Differs per conversation/claim; delivered by adoption or forces a cold claim.
    CONVERSATION = "conversation"
    #: Secret material; identity (secretKeyRef or generation) is hashed, never the value.
    CREDENTIAL = "credential"


def worker_env_classes() -> Mapping[str, EnvClass]:
    """Every boot key the worker can emit, classified. Unknown keys refuse."""

    return _ENV_CLASSES


_ENV_CLASSES: Mapping[str, EnvClass] = MappingProxyType(
    {
        BootEnv.env_key("plugin_dir"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("budget"): EnvClass.VERSION_STABLE,
        MEMORY_REF_ENV: EnvClass.VERSION_STABLE,
        BootEnv.env_key("bundle_ref"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("bundle_version"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("model"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("fake_model"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("base_url"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("api_backend"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("thinking"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("model_env_key"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("approval_required_tools"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("connector_release"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("connector_agent"): EnvClass.VERSION_STABLE,
        BootEnv.env_key("connector_namespace"): EnvClass.VERSION_STABLE,
        # The marker names the connector-secret KEYS (sorted); the values are
        # delivered by the per-agent Secret's secretKeyRef, never by the claim.
        _CONNECTOR_SECRET_KEYS_ENV: EnvClass.VERSION_STABLE,
        FALSE_COMPLETION_CHECK_ENV: EnvClass.VERSION_STABLE,
        # Agent-wide when memory=True; binding-scoped (unknowable ahead of the
        # claim) when memory=False, handled explicitly in project_boot_env.
        _STATE_URL_ENV: EnvClass.VERSION_STABLE,
        _SESSION_ENV: EnvClass.CONVERSATION,
        _HISTORY_REF_ENV: EnvClass.CONVERSATION,
        _RUNNER_TOKEN_ENV: EnvClass.CONVERSATION,
        GRANT_TOOL_ENV: EnvClass.CONVERSATION,
        RESUMED_KIND_ENV: EnvClass.CONVERSATION,
        DECISION_ENV: EnvClass.CONVERSATION,
        WORKSPACE_REF_ENV: EnvClass.CONVERSATION,
        WORKSPACE_SHA256_ENV: EnvClass.CONVERSATION,
        _CREDENTIALS_ENV: EnvClass.CREDENTIAL,
        _HISTORY_TOKEN_ENV: EnvClass.CREDENTIAL,
        _MEMORY_TOKEN_ENV: EnvClass.CREDENTIAL,
        _STATE_TOKEN_ENV: EnvClass.CREDENTIAL,
    }
)

#: Per-claim keys whose presence means the turn needs an overlay a warm pod
#: cannot receive. Delivered per-conversation keys (session, history ref, runner
#: token, state tokens) are NOT here: adoption and the generation Secret carry
#: those.
_COLD_OVERLAY_KEYS: Mapping[str, ColdReason] = MappingProxyType({})  # filled below


@dataclass(frozen=True, slots=True)
class SecretKeyRef:
    """The nonsecret identity of a Kubernetes ``secretKeyRef``."""

    name: str
    key: str

    def __post_init__(self) -> None:
        if not self.name or not self.key:
            raise ProjectionError("a secretKeyRef needs both a Secret name and a key")

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "key": self.key}


@dataclass(frozen=True, slots=True)
class CredentialRevision:
    """Nonsecret identity from an authoritative credential-source observation.

    The producer must read the actual Secret UID/resourceVersion and bind its
    name/key to the credential being rendered. For generated state tokens this
    names the signing-key Secret. Constructing this value does not verify its
    provenance; no such production observation/rotation producer exists here.
    Never derive either revision field from a fixed name or credential bytes.
    """

    source: SecretKeyRef
    uid: str
    resource_version: str

    def __post_init__(self) -> None:
        if not all(isinstance(v, str) and v.strip() for v in (self.uid, self.resource_version)):
            raise ProjectionError("credential revision needs an observed UID and resourceVersion")

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.as_dict(),
            "uid": self.uid,
            "resource_version": self.resource_version,
        }


@dataclass(frozen=True, slots=True)
class CredentialGeneration:
    """Nonsecret identity of one minted state-credential generation.

    Authority is agent + scope + expiry, exactly the claims the existing
    ``sandbox_token`` codec signs and the API state router checks. Carries no
    token bytes; the values are minted from this identity by
    ``mint_credential_generation`` and stored only in a protected Secret. A
    different window is a different generation and therefore a different pool:
    expiry is handled by replacement, never by extending or rotating in place.
    """

    agent_id: str
    scopes: tuple[str, ...]
    issued_at: int
    expires_at: int

    def __post_init__(self) -> None:
        if not self.agent_id:
            raise ValueError("credential generation needs an agent id")
        if tuple(self.scopes) != (STATE_SCOPE, STATE_APP_SCOPE):
            raise ValueError("credential generation scopes must be exactly (state, state.app)")
        if self.expires_at <= self.issued_at:
            raise ValueError("credential generation must expire after it is issued")

    @classmethod
    def for_window(
        cls, agent_id: str, *, issued_at: int, ttl_seconds: int = SANDBOX_TOKEN_TTL_SECONDS
    ) -> CredentialGeneration:
        if ttl_seconds <= 0:
            raise ValueError("credential generation ttl must be positive")
        return cls(
            agent_id=agent_id,
            scopes=(STATE_SCOPE, STATE_APP_SCOPE),
            issued_at=int(issued_at),
            expires_at=int(issued_at) + int(ttl_seconds),
        )

    def admits(self, now: int, *, margin_seconds: int = 0) -> bool:
        """True while a pod booted on this generation may still be adopted."""

        return self.expires_at - margin_seconds > now

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "scopes": list(self.scopes),
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


class CredentialMaterial:
    """Minted token values for one generation. Redacted everywhere but ``value``.

    Not a dataclass on purpose: no ``__eq__``/``__repr__``/``asdict`` path may
    surface the values. Only ``value(key)`` returns material, for the one
    producer that writes the protected Secret.
    """

    __slots__ = ("_generation", "_values")

    def __init__(self, generation: CredentialGeneration, values: Mapping[str, str]) -> None:
        self._generation = generation
        self._values = dict(values)

    def keys(self) -> tuple[str, ...]:
        return tuple(self._values)

    def value(self, key: str) -> str:
        return self._values[key]

    def identity(self) -> dict[str, Any]:
        return {**self._generation.as_dict(), "keys": list(self._values)}

    def __repr__(self) -> str:
        return (
            f"CredentialMaterial(agent_id={self._generation.agent_id!r}, "
            f"keys={list(self._values)!r}, values=<redacted>)"
        )

    __str__ = __repr__


def mint_credential_generation(
    api_key: str, generation: CredentialGeneration
) -> CredentialMaterial:
    """Mint the generation's token values with the existing scoped codec.

    Pure and deterministic (the codec signs ``agent``/``scope``/``exp`` only), so
    two workers minting the same generation produce byte-identical values and a
    create-or-adopt Secret race cannot leave a loser holding a different live
    token. Never logs, never stores; the caller owns the protected write.
    """

    if not api_key:
        raise ValueError("cannot mint a credential generation without the platform key")
    values = {
        key: sandbox_token.mint(
            api_key, agent=generation.agent_id, scope=scope, exp=generation.expires_at
        )
        for key, scope in CREDENTIAL_KEY_SCOPES.items()
    }
    return CredentialMaterial(generation, values)


@dataclass(frozen=True, slots=True)
class CapabilityProjection:
    """The version-stable, secret-free content of one warm template.

    ``env`` is what the template's runner container carries as literal values;
    ``secret_refs`` are the env keys delivered by an existing ``secretKeyRef``
    (identity only); ``credential_keys`` are the env keys the generation Secret
    carries, minted from ``credential_generation``. ``capability_generation`` is
    the SHA-256 over the canonical JSON of all of that.
    """

    agent_id: str
    version_id: str
    deployment_id: str | None
    version_label: str
    bundle_ref: str | None
    bundle_sha256: str | None
    memory: bool
    env: Mapping[str, str]
    secret_refs: Mapping[str, SecretKeyRef]
    credential_keys: tuple[str, ...]
    credential_generation: CredentialGeneration | None
    credential_revisions: Mapping[str, CredentialRevision] = field(default_factory=dict)
    projection_version: int = PROJECTION_VERSION
    capability_generation: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "env", MappingProxyType(dict(sorted(self.env.items()))))
        object.__setattr__(
            self, "secret_refs", MappingProxyType(dict(sorted(self.secret_refs.items())))
        )
        object.__setattr__(
            self,
            "credential_revisions",
            MappingProxyType(dict(sorted(self.credential_revisions.items()))),
        )
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        object.__setattr__(self, "capability_generation", digest)

    def as_dict(self) -> dict[str, Any]:
        return {
            "projection_version": self.projection_version,
            "agent_id": self.agent_id,
            "version_id": self.version_id,
            "deployment_id": self.deployment_id,
            "version_label": self.version_label,
            "bundle_ref": self.bundle_ref,
            "bundle_sha256": self.bundle_sha256,
            "memory": self.memory,
            "env": dict(self.env),
            "secret_refs": {k: v.as_dict() for k, v in self.secret_refs.items()},
            "credential_keys": list(self.credential_keys),
            "credential_revisions": {k: v.as_dict() for k, v in self.credential_revisions.items()},
            "credential_generation": (
                None if self.credential_generation is None else self.credential_generation.as_dict()
            ),
        }

    def canonical_json(self) -> str:
        """UTF-8, sorted keys, no whitespace variance, no secret values, no timestamps."""

        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def short_generation(self) -> str:
        """The object-name prefix of the generation (names, never identity)."""

        return self.capability_generation[:12]

    @property
    def credential_authority_complete(self) -> bool:
        """Every loaded credential has a source revision, with exact ref binding."""

        required = set(self.secret_refs) | set(self.credential_keys)
        if required != set(self.credential_revisions):
            return False
        # boot_env signs every state scope with the same platform key. A mixed
        # signing snapshot cannot describe any generation that renderer minted.
        signers = {self.credential_revisions[key] for key in self.credential_keys}
        return len(signers) <= 1 and all(
            self.credential_revisions[key].source == ref for key, ref in self.secret_refs.items()
        )


def _iter_connector_keys(env: Mapping[str, str]) -> Iterator[str]:
    marker = env.get(_CONNECTOR_SECRET_KEYS_ENV, "")
    for key in marker.split(","):
        key = key.strip()
        if key:
            yield key


def project_boot_env(
    env: Mapping[str, str],
    *,
    agent_id: str,
    version_id: str,
    deployment_id: str | None,
    version_label: str,
    bundle_ref: str | None,
    bundle_sha256: str | None,
    memory: bool,
    model_credential_ref: SecretKeyRef | None,
    connector_secret_name: str | None,
    credential_generation: CredentialGeneration | None,
    credential_revisions: Mapping[str, CredentialRevision] | None = None,
) -> CapabilityProjection:
    """Project one rendered boot env into its warm-template identity.

    Fail-closed rules, each pinned by a test:

    - an unclassified key refuses (a new boot knob must be classified, not guessed);
    - a credential VALUE never survives: model credential -> baseline
      ``secretKeyRef``; connector secrets -> per-agent Secret refs; state tokens ->
      the generation; a value that also appears under any kept key refuses;
    - state tokens present without a generation (or a generation without state
      tokens) refuses, because the warm pod would boot with different authority
      than the cold path;
    - ``memory=False`` drops ``state_url``: the cold path scopes it to a binding
      that does not exist before the claim, so it cannot be pre-baked;
    - ``memory_ref`` is required: an env-free warm pod without it boots
      ``NullMemoryStore`` silently.
    """

    connector_keys = frozenset(_iter_connector_keys(env))
    kept: dict[str, str] = {}
    secret_refs: dict[str, SecretKeyRef] = {}
    credential_keys: list[str] = []
    dropped_material: list[str] = []
    unknown: list[str] = []
    for key, value in env.items():
        if key in connector_keys:
            if not connector_secret_name:
                raise ProjectionError(
                    f"connector secret {key} has no per-agent Secret name to reference"
                )
            secret_refs[key] = SecretKeyRef(name=connector_secret_name, key=key)
            dropped_material.append(value)
            continue
        klass = _ENV_CLASSES.get(key)
        if klass is None:
            unknown.append(key)
            continue
        if klass is EnvClass.VERSION_STABLE:
            if key == _STATE_URL_ENV and not memory:
                # Binding-scoped; see the docstring. Explicitly cold, not an omission.
                continue
            kept[key] = value
        elif klass is EnvClass.CONVERSATION:
            dropped_material.append(value)
        elif key == _CREDENTIALS_ENV:
            if model_credential_ref is None:
                raise ProjectionError(
                    f"{_CREDENTIALS_ENV} is set but no baseline secretKeyRef identity was given"
                )
            secret_refs[key] = model_credential_ref
            dropped_material.append(value)
        else:
            credential_keys.append(key)
            dropped_material.append(value)
    if unknown:
        raise ProjectionError(
            "unclassified boot env key(s) refuse the warm projection: " + ", ".join(sorted(unknown))
        )
    if MEMORY_REF_ENV not in kept:
        raise ProjectionError("boot env has no memory_ref; a warm pod would boot NullMemoryStore")
    ordered_credential_keys = tuple(k for k in CREDENTIAL_KEY_SCOPES if k in credential_keys)
    if not memory:
        # The bundle-facing state token authorizes a URL the template cannot
        # carry; do not mint it into the generation Secret either.
        ordered_credential_keys = tuple(k for k in ordered_credential_keys if k != _STATE_TOKEN_ENV)
    if ordered_credential_keys and credential_generation is None:
        raise ProjectionError(
            "boot env mints state tokens but no credential generation was given; "
            "a warm pod would boot without state authority"
        )
    if credential_generation is not None:
        if not ordered_credential_keys:
            raise ProjectionError(
                "a credential generation was given but this install mints no state "
                "tokens (no platform key); refuse rather than invent authority"
            )
        if credential_generation.agent_id != agent_id:
            raise ProjectionError("credential generation names a different agent")
    for material in dropped_material:
        if material and any(material == v for v in kept.values()):
            raise ProjectionError(
                "a credential value reappears under a version-stable key; refusing to hash it"
            )
    return CapabilityProjection(
        agent_id=agent_id,
        version_id=version_id,
        deployment_id=deployment_id,
        version_label=version_label,
        bundle_ref=bundle_ref,
        bundle_sha256=bundle_sha256,
        memory=memory,
        env=kept,
        secret_refs=secret_refs,
        credential_keys=ordered_credential_keys,
        credential_generation=credential_generation,
        credential_revisions={
            key: revision
            for key, revision in (credential_revisions or {}).items()
            if key in secret_refs or key in ordered_credential_keys
        },
    )


def warm_boot_projection(
    resolver: BindingResolver,
    resolved: ResolvedDeployment,
    *,
    bundle_sha256: str | None,
    model_credential_ref: SecretKeyRef | None,
    connector_secret_name: str | None,
    credential_generation: CredentialGeneration | None,
    credential_revisions: Mapping[str, CredentialRevision] | None = None,
) -> CapabilityProjection:
    """The projection of ``resolved`` under this worker's current defaults.

    Renders through the real ``BindingResolver.boot_env`` with a sentinel
    thread and no binding, so precedence and defaults are the cold path's own.
    The per-claim tokens that render mints are discarded with the other
    conversation keys; nothing here is persisted.
    """

    env = resolver.boot_env(resolved, PROJECTION_THREAD_SENTINEL)
    return project_boot_env(
        env,
        agent_id=str(resolved.agent_id),
        version_id=str(resolved.version_id),
        deployment_id=None if resolved.deployment_id is None else str(resolved.deployment_id),
        version_label=resolved.version_label,
        bundle_ref=resolved.bundle_ref,
        bundle_sha256=bundle_sha256,
        memory=resolved.memory,
        model_credential_ref=model_credential_ref,
        connector_secret_name=connector_secret_name,
        credential_generation=credential_generation,
        credential_revisions=credential_revisions,
    )


class ColdReason(StrEnum):
    """Why a turn must cold-create instead of binding a warm version pool."""

    GENERATION_MISMATCH = "generation-mismatch"
    MEMORY_FALSE_BINDING_STATE = "memory-false-binding-state-url"
    RESUME_HISTORY = "resume-history-ref"
    EVAL_LANE = "eval-lane"
    WORKSPACE_STAGE = "workspace-stage"
    WORKSPACE_ENV = "workspace-env"
    APPROVAL_GRANT = "approval-grant"
    APPROVAL_RESUMED_KIND = "approval-resumed-kind"
    APPROVAL_DECISION = "approval-decision"
    EXTRA_CLAIM_ENV = "extra-claim-env"
    CREDENTIAL_GENERATION_EXPIRED = "credential-generation-expired"
    CREDENTIAL_AUTHORITY_UNVERIFIED = "credential-authority-unverified"


_COLD_OVERLAY_KEYS = MappingProxyType(
    {
        GRANT_TOOL_ENV: ColdReason.APPROVAL_GRANT,
        RESUMED_KIND_ENV: ColdReason.APPROVAL_RESUMED_KIND,
        DECISION_ENV: ColdReason.APPROVAL_DECISION,
        WORKSPACE_REF_ENV: ColdReason.WORKSPACE_ENV,
        WORKSPACE_SHA256_ENV: ColdReason.WORKSPACE_ENV,
    }
)

#: Conversation keys a warm pod receives by adoption/Secret rather than by claim env.
_DELIVERED_BY_ADOPTION = frozenset({_SESSION_ENV, _HISTORY_REF_ENV, _RUNNER_TOKEN_ENV}) | frozenset(
    CREDENTIAL_KEY_SCOPES
)


@dataclass(frozen=True, slots=True)
class Eligibility:
    """Warm or cold, with every reason (never a silent table cell)."""

    warm: bool
    reasons: tuple[ColdReason, ...]
    mismatched_keys: tuple[str, ...] = ()


def classify_claim(
    claim_env: Mapping[str, str],
    projection: CapabilityProjection,
    *,
    resume: bool = False,
    eval_lane: bool = False,
    workspace_stage: bool = False,
    now: int | None = None,
    margin_seconds: int = DEFAULT_CREDENTIAL_ADMISSION_MARGIN_SECONDS,
    current_credential_revisions: Mapping[str, CredentialRevision] | None = None,
) -> Eligibility:
    """Whether the cold claim env this turn WOULD send can instead bind ``projection``.

    Warm requires: every version-stable key equal to the projection (same
    generation, computed from the same live overlay), ``memory=True`` (an
    agent-wide state URL), no per-claim overlay (approval resume, workspace,
    resume history, eval lane), no unknown key, and an unexpired credential
    generation. Anything else is cold with its reason. A mismatch is never a
    licence to pick another version's pool or the Helm generic pool.
    """

    reasons: list[ColdReason] = []
    mismatched: list[str] = []
    seen_stable: set[str] = set()
    connector_keys = frozenset(_iter_connector_keys(claim_env))
    for key, value in claim_env.items():
        if key in connector_keys:
            if key not in projection.secret_refs:
                mismatched.append(key)
            continue
        overlay = _COLD_OVERLAY_KEYS.get(key)
        if overlay is not None:
            if overlay not in reasons:
                reasons.append(overlay)
            continue
        if key in _DELIVERED_BY_ADOPTION:
            continue
        if key == _CREDENTIALS_ENV:
            if key not in projection.secret_refs:
                mismatched.append(key)
            continue
        klass = _ENV_CLASSES.get(key)
        if klass is None:
            mismatched.append(key)
            if ColdReason.EXTRA_CLAIM_ENV not in reasons:
                reasons.append(ColdReason.EXTRA_CLAIM_ENV)
            continue
        if key == _STATE_URL_ENV and not projection.memory:
            if ColdReason.MEMORY_FALSE_BINDING_STATE not in reasons:
                reasons.append(ColdReason.MEMORY_FALSE_BINDING_STATE)
            continue
        seen_stable.add(key)
        if projection.env.get(key) != value:
            mismatched.append(key)
    missing = set(projection.env) - seen_stable
    if missing:
        mismatched.extend(sorted(missing))
    # A secretKeyRef the template carries that this claim would NOT render (a
    # baseline credential or connector key that disappeared) is a different
    # capability too, not a warm hit with a stray Secret mounted.
    missing_refs = set(projection.secret_refs) - set(claim_env)
    if missing_refs:
        mismatched.extend(sorted(missing_refs))
    # The state tokens the cold path mints must be exactly the generation Secret's
    # keys; a no-key claim against a keyed generation (or the reverse) is skew.
    expected_tokens = set(projection.credential_keys)
    rendered_tokens = {k for k in CREDENTIAL_KEY_SCOPES if k in claim_env}
    if not projection.memory:
        rendered_tokens.discard(_STATE_TOKEN_ENV)
    if expected_tokens != rendered_tokens:
        mismatched.extend(sorted(expected_tokens ^ rendered_tokens))
    required_revisions = set(projection.secret_refs) | set(projection.credential_keys)
    current_revisions = current_credential_revisions or {}
    # The renderer's credential values are intentionally never compared or
    # hashed. Only a fresh authoritative observation can match the exact sources
    # the warm generation loaded. Missing provenance must stay cold.
    if not projection.credential_authority_complete or not required_revisions <= set(
        current_revisions
    ):
        reasons.append(ColdReason.CREDENTIAL_AUTHORITY_UNVERIFIED)
    else:
        mismatched.extend(
            key
            for key in sorted(required_revisions)
            if current_revisions[key] != projection.credential_revisions[key]
        )
    generation_mismatch = [
        k
        for k in mismatched
        if k in _ENV_CLASSES or k in connector_keys or k in projection.secret_refs
    ]
    if generation_mismatch and ColdReason.GENERATION_MISMATCH not in reasons:
        reasons.insert(0, ColdReason.GENERATION_MISMATCH)
    if not projection.memory and ColdReason.MEMORY_FALSE_BINDING_STATE not in reasons:
        reasons.append(ColdReason.MEMORY_FALSE_BINDING_STATE)
    if resume:
        reasons.append(ColdReason.RESUME_HISTORY)
    if eval_lane:
        reasons.append(ColdReason.EVAL_LANE)
    if workspace_stage:
        reasons.append(ColdReason.WORKSPACE_STAGE)
    if projection.credential_generation is not None:
        current = now if now is not None else int(time.time())
        if not projection.credential_generation.admits(current, margin_seconds=margin_seconds):
            reasons.append(ColdReason.CREDENTIAL_GENERATION_EXPIRED)
    return Eligibility(
        warm=not reasons,
        reasons=tuple(reasons),
        mismatched_keys=tuple(dict.fromkeys(mismatched)),
    )
