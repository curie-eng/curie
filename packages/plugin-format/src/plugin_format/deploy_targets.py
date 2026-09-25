"""Declared deploy targets: where a bundle gets sent (ADR-0089).

``connectors.yaml`` says what a bundle needs wherever it runs. ``deploy.yaml``
says where THIS repository sends it. Two files because they are different kinds
of fact with different lifetimes: a connector changes when the agent's
capabilities change, a target changes when the deployment topology does.

    # deploy.yaml
    targets:
      dev:
        agent: acme-dev
        env: dev
        slack_channel: C0EXAMPLE2
        connectors: [grafana]
      prod:
        agent: acme-bot
        env: prod
        identity: ops-bot
        slack_channel: C0EXAMPLE1

    curie cluster deploy --target prod

``identity`` names the channel identity (the bot) a target's binding speaks
through, and defaults to ``default``, the installation's own. ``connectors``
limits which of ``connectors.yaml``'s connectors run for the target: absent
means all of them, ``[]`` means none. Two agents built from one artifact can
then hold different credentials (ADR-0168 decision 8).

Before this, routing lived in whatever invoked the command. For acme-bot that
was two GitHub Actions workflows describing a dev/prod split that did not
exist: both resolved to the same agent, overwrote each other's active version,
and contended for the one channel that agent can bind. Nothing reported it,
because ``--env`` defaults to ``dev`` and a prod workflow that omits it deploys
to dev silently.

The bundle is IDENTICAL across targets. Only the binding differs -- which is
what lets prod promote the exact artifact dev validated, and why rewriting
``plugin.json``'s name per environment was rejected.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# The renderer owns the join rule, and it is imported rather than restated so
# the name this validator refuses and the name the renderer refuses cannot
# drift. A plain module-level import is fine here: unlike `connectors`, this
# module is not imported by `connector_render`, so there is no cycle (#1446).
from .connector_render import agent_forges_join

# A connector allowlist entry must be a name `connectors.yaml` could declare, so
# the rule is the one that module applies, imported for the same no-drift reason.
from .connectors import _NAME_MAX as _CONNECTOR_NAME_MAX
from .connectors import ADMITS_SELF
from .connectors import _is_valid_name as _is_valid_connector_name

# Curie's two deployment environments. Not open-ended: the worker's binding
# query ranks prod over dev explicitly, so a third value would silently never
# be selected.
_ENVS = ("dev", "prod")

# A target name is a label a human types after `--target`; an agent name becomes
# a platform identity. They are held to the same shape for the same reason
# connector names are -- predictable, no surprises at the boundary.
_NAME_RE = re.compile(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?")
_NAME_MAX = 40

# Slack conversation ids are uppercase alphanumeric starting with C (channel),
# G (private group) or D (DM). Checked because a mistyped id binds the agent to
# a channel nobody is watching, and the deploy reports success.
_SLACK_RE = re.compile(r"[CGD][A-Z0-9]{6,}")


class DeployTarget(BaseModel):
    """One named destination for this bundle."""

    model_config = ConfigDict(extra="forbid")

    # Which agent this target binds. Validation requires it on every declared
    # target, while the nullable shape preserves the existing schema.
    agent: str | None = None
    env: str = "dev"
    # The channel identity the binding speaks through (ADR-0168 decision 8).
    identity: str = "default"
    slack_channel: str | None = None
    # `None` runs every connector the bundle declares; a list runs only those.
    connectors: list[str] | None = None


class DeployTargetsFile(BaseModel):
    """The parsed ``deploy.yaml``."""

    model_config = ConfigDict(extra="forbid")

    targets: dict[str, DeployTarget] = Field(default_factory=dict)


def _is_valid_name(name: str) -> bool:
    return len(name) <= _NAME_MAX and bool(_NAME_RE.fullmatch(name))


def validate_deploy_targets(data: Any) -> tuple[DeployTargetsFile | None, list[tuple[str, str]]]:
    """Validate parsed ``deploy.yaml`` content.

    Returns ``(parsed, errors)`` where each error is ``(code, message)``. Every
    check here exists because the failure it prevents is SILENT: a bad target
    otherwise deploys successfully to the wrong place and says so cheerfully.
    """

    errors: list[tuple[str, str]] = []
    if data is None:
        return DeployTargetsFile(), errors
    if not isinstance(data, dict):
        return None, [("deploy.not_object", "deploy.yaml must be a mapping")]

    try:
        parsed = DeployTargetsFile.model_validate(data)
    except Exception as exc:  # pydantic ValidationError -- surface it verbatim
        return None, [("deploy.invalid", str(exc)[:400])]

    for name, target in parsed.targets.items():
        where = f"targets.{name}"
        if not _is_valid_name(name):
            errors.append(
                (
                    "deploy.bad_target_name",
                    f"{where}: a target name is typed after `--target`, so it must be "
                    "lowercase alphanumeric or dashes, start and end alphanumeric, and be "
                    f"at most {_NAME_MAX} characters",
                )
            )
        if target.env not in _ENVS:
            errors.append(
                (
                    "deploy.bad_env",
                    f"{where}: env must be one of {', '.join(_ENVS)} (got {target.env!r}). "
                    "The worker ranks prod over dev explicitly, so any other value would "
                    "never be selected.",
                )
            )
        if target.agent is None:
            errors.append(
                (
                    "deploy.missing_agent",
                    f"{where}: agent is required for every declared target",
                )
            )
        else:
            # Two independent `if`s inside the `else`, not an `elif` chain. A
            # name can be both malformed and forging (`Acme-mcp-bot`), and this
            # file accumulates every applicable code. The `else` is what keeps
            # the forging check off a `None` agent: called on `None` the
            # predicate raises TypeError out of a validator whose whole contract
            # is to RETURN errors, turning "agent is required" into a crash.
            if target.agent == ADMITS_SELF:
                errors.append(
                    (
                        "deploy.bad_agent_name",
                        f"{where}: `{ADMITS_SELF}` is not a valid agent name -- it is reserved "
                        "for `admits`, where it means the agent this bundle is deployed as "
                        "(ADR-0168 decision 7). A target genuinely named `self` would be "
                        "indistinguishable from that sentinel.",
                    )
                )
            elif not _is_valid_name(target.agent):
                errors.append(
                    (
                        "deploy.bad_agent_name",
                        f"{where}: `{target.agent}` is not a valid agent name. A typo here does "
                        "not fail -- it MINTS A NEW AGENT and the deploy reports success, so the "
                        "name is checked before anything is created (ADR-0089).",
                    )
                )
            # The same class of silent failure one level deeper: a typo MINTS a
            # new agent, a forged join MERGES two. Every connector Curie renders
            # for this agent is named `<release>-<agent>-mcp-<connector>`, and
            # `-mcp-` is a bare substring inside one DNS label rather than a
            # structural separator (#1446).
            if agent_forges_join(target.agent):
                errors.append(
                    (
                        "deploy.ambiguous_agent_name",
                        f"{where}: `{target.agent}` would forge a second `-mcp-` in every "
                        "connector object name Curie renders for this agent "
                        "(`<release>-<agent>-mcp-<connector>`), so a DIFFERENT "
                        "agent/connector pair would render the same Service, Deployment, "
                        "both NetworkPolicies and the same `app.kubernetes.io/name` -- which "
                        "IS the pod selector, so one agent's sandbox would reach the other's "
                        "connector and the credential bound to it (the connector is "
                        "deliberately unauthenticated, ADR-0086) -- rename the agent so it "
                        "does not end in `-mcp` or contain `-mcp-`",
                    )
                )
        if target.slack_channel is not None and not _SLACK_RE.fullmatch(target.slack_channel):
            errors.append(
                (
                    "deploy.bad_slack_channel",
                    f"{where}: `{target.slack_channel}` is not a Slack conversation id. Use "
                    "the id (starts with C, G, or D), not the #name -- a mistyped id binds "
                    "the agent to a channel nobody is watching and the deploy still succeeds.",
                )
            )

        # Shape only: which identities exist is the installation's to say, and
        # this validator never sees the installation.
        if not _is_valid_name(target.identity):
            errors.append(
                (
                    "deploy.bad_identity",
                    f"{where}: `{target.identity}` is not a valid identity name. The "
                    "identity is the channel identity (the bot) this target's binding "
                    "speaks through, and the installation must declare it; it must be "
                    "lowercase alphanumeric or dashes, start and end alphanumeric, and be "
                    f"at most {_NAME_MAX} characters",
                )
            )
        # An explicit null (`connectors:` with no value) is refused rather than
        # read as "all": it widens the target, the one direction this field
        # exists to prevent. Only an absent key means every connector.
        if "connectors" in target.model_fields_set and target.connectors is None:
            errors.append(
                (
                    "deploy.null_connectors",
                    f"{where}: connectors has no value, which is refused rather than read "
                    "as every connector: omit the key to run every connector "
                    "connectors.yaml declares, or write `[]` to run none",
                )
            )
        if target.connectors is not None:
            seen: set[str] = set()
            repeated: set[str] = set()
            for connector in target.connectors:
                if connector not in seen:
                    seen.add(connector)
                    if not _is_valid_connector_name(connector):
                        errors.append(
                            (
                                "deploy.bad_connector_name",
                                f"{where}: connectors lists `{connector}`, which no "
                                "connectors.yaml could declare: a connector name must be "
                                "lowercase alphanumeric or dashes, start and end "
                                f"alphanumeric, and be at most {_CONNECTOR_NAME_MAX} "
                                "characters",
                            )
                        )
                elif connector not in repeated and _is_valid_connector_name(connector):
                    repeated.add(connector)
                    errors.append(
                        (
                            "deploy.duplicate_connector",
                            f"{where}: connectors lists `{connector}` more than once",
                        )
                    )

    return (parsed if not errors else None), errors
