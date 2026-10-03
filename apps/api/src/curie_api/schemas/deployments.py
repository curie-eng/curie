import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..models import Environment
from .common import validate_optional_commit_sha


class ResolveTargetRequest(BaseModel):
    """A bundle's ``deploy.yaml`` text plus the target to resolve (ADR-0089).

    The CLI sends the file CONTENT rather than parsing it, so there is exactly
    one parser for this format. Two would be a drift hazard on a file whose
    whole job is to be unambiguous about where a deploy lands -- and the Rust
    YAML ecosystem has no maintained successor to serde_yaml to pick from.
    """

    content: str
    target: str


class ResolvedTarget(BaseModel):
    """What a named target resolves to. Pure function of the file."""

    agent: str | None = None
    env: str = "dev"
    slack_channel: str | None = None
    # @spec ADR-0168 d8: the identity the binding speaks through, and the
    # connectors the bound agent runs (None is every declared one).
    identity: str = "default"
    connectors: list[str] | None = None


class NamedTarget(ResolvedTarget):
    """A resolved target plus the name it is declared under."""

    name: str


class ListedTargets(BaseModel):
    """Every target a ``deploy.yaml`` declares, dev before prod.

    Ordered so a caller onboarding a repository deploys dev first. A run that
    fails part-way then leaves prod BEHIND rather than ahead of a dev that
    never landed -- recoverable in one direction, not the other.
    """

    targets: list[NamedTarget] = []


class RoutingCheckRequest(BaseModel):
    """Ask whether a repository's pushes can still be routed to an agent (#1221).

    Migration 0018 (ADR-0091) dropped the unique index on ``repo_full_name``, so
    binding a SECOND agent to a repository is legal -- and silently flips every
    future push for the agent that was already bound from "deploys" to
    "rejected", because nothing says which of the two a branch belongs to. The
    caller sends the bundle's ``deploy.yaml`` TEXT for the same reason
    ``ResolveTargetRequest`` does: the API owns the resolver rule, so a client
    restating it here would drift from the rule actually enforced on a push.
    """

    repo_full_name: str
    # The bundle's deploy.yaml TEXT, or None when the bundle has no such file.
    # None and an empty `targets:` map say the same thing about routing (#1210),
    # and the resolver already treats them identically.
    content: str | None = None


class RoutingCheckProblem(BaseModel):
    """One environment whose pushes this repository can no longer route.

    ``message`` is the resolver's OWN text, carried verbatim so the CLI can
    print it without paraphrasing the rule.
    """

    environment: str
    code: str
    message: str


class RoutingCheck(BaseModel):
    """Whether pushes to a repository still resolve to an agent (#1221).

    ``resolvable`` is false only when the real resolver raised: a branch with no
    matching target resolves to "ignore", which is intended behaviour, not a
    problem. An unbound repository (``agent_count`` 0) stays resolvable too --
    this reports ROUTING, not whether anything is bound.
    """

    repo_full_name: str
    agent_count: int = 0
    agents: list[str] = Field(default_factory=list)
    resolvable: bool = True
    unresolvable: list[RoutingCheckProblem] = Field(default_factory=list)


class ConnectorManifests(BaseModel):
    """Kubernetes objects derived from a version's ``connectors.yaml``.

    The API renders; the caller applies. Rendering is a pure function of the
    bundle plus the deployment context the caller supplies, so producing this
    needs no cluster access and the API's read-only RBAC is untouched
    (ADR-0086, #1063).
    """

    manifests: list[dict[str, Any]] = Field(default_factory=list)
    # The Secret Curie owns for this agent, and the keys the CALLER must
    # resolve values for. Stated explicitly because the caller cannot infer it
    # from the manifests: since #1163 a connector may also reference a Secret
    # provisioned out of band, and those keys must NOT be resolved -- the whole
    # point is that the deploy path never handles them.
    owned_secret_name: str = ""
    owned_secret_keys: list[str] = Field(default_factory=list)
    # name -> the `.mcp.json` entry the agent should use. Derived from the
    # Service in `manifests`, so an author never hand-writes a URL that
    # resolves in one tier and not another.
    mcp_entries: dict[str, Any] = Field(default_factory=dict)
    # The version whose bundle was read. Required: the route always names the
    # version it was given and does not look up a second deployment.
    version_id: uuid.UUID
    # plugin.json triggers as stored. Missing, null, and non-list values are
    # an empty list. Entries are not validated or rewritten.
    triggers: list[dict[str, Any]] = Field(default_factory=list)


class DeploymentCreate(BaseModel):
    agent_id: uuid.UUID
    version_id: uuid.UUID
    environment: Environment
    commit_sha: str | None = None
    # Retained as a compatibility-only deployment field; it is not a runtime
    # coding gate. The worker-wide workspace coordinator plus an allowed root
    # GitHub URL determine whether claim-time repository acquisition occurs.
    workspace_enabled: bool | None = None
    status: str = "active"

    _check_commit_sha = field_validator("commit_sha")(validate_optional_commit_sha)


class DeploymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version_id: uuid.UUID
    environment: Environment
    commit_sha: str | None
    workspace_enabled: bool
    status: str
    deployed_at: datetime


class WebhookResult(BaseModel):
    """The outcome of processing a GitHub webhook event."""

    status: str
    environment: Environment | None = None
    agent_id: uuid.UUID | None = None
    version_id: uuid.UUID | None = None
    deployment_id: uuid.UUID | None = None
    commit_sha: str | None = None
    errors: list[dict[str, str]] | None = None
