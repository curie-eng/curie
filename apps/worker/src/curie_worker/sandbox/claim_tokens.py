"""Claim-scoped token delivery for the Kubernetes substrate (#3842).

``SandboxClaim.spec.env`` is value-only, so every value written there is stored
in plain text in etcd and readable by any principal with ``get sandboxclaims``.
Scoped tokens therefore leave the claim: the worker writes them to a per-claim
Secret and a per-claim copy of the pool's SandboxTemplate whose runner container
reads each one by ``secretKeyRef``. This module is the pure half of that: which
boot-env keys are tokens, how they split off the claim env, what the per-claim
objects are named, and the per-claim template spec. It imports no Kubernetes
client so the rules are testable without one.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from aci_protocol import BootEnv

from .resources import (
    RESOURCES_POOL_SUFFIX,
    RESOURCES_TEMPLATE_SUFFIX,
    claim_resources_spec,
    resources_object_name,
)

# The label every per-claim object carries, valued with the claim name. The
# chart's cleanup and worker-secrets admission policies key on it, and the
# reaper sweep selects on it.
CLAIM_LABEL = "curietech.ai/sandbox-claim"

# Every boot-env value that authenticates the sandbox to something. Named from
# the ONE declaration in ``aci_protocol.BootEnv`` like the other claim env keys
# in ``k8s.py``, never retyped: a local literal would drift silently on a rename,
# stop matching, and persist that token in plain text on every claim.
#   runner_token            random bearer the worker presents to the runner's ACI
#   history/memory/state/progress_token  ``sbx.`` scoped state-store tokens
#   issue_read_token        ``wir.`` work-item issue read token
#   connector_caller_token  ``cct.`` hosted-connector caller identity
CLAIM_TOKEN_ENVS: tuple[str, ...] = tuple(
    BootEnv.env_key(field)
    for field in (
        "runner_token",
        "history_token",
        "memory_token",
        "state_token",
        "progress_token",
        "issue_read_token",
        "connector_caller_token",
    )
)

_RUNNER_CONTAINER = "runner"
# The chart's per-agent connector Secret is ``<fullname>-agent-<agent>-connector-secrets``
# (charts/curie/templates/agent-connector-secrets.yaml); the runner reads each of
# its keys by ``secretKeyRef``. Matched by suffix so a release name never matters.
CONNECTOR_SECRET_NAME_SUFFIX = "-connector-secrets"
# Runner env an executor claim's template never carries (ACTION-EXECUTOR-5): the
# model credential and the model env-key declaration. Named from ``BootEnv``.
EXECUTOR_WITHHELD_ENVS: frozenset[str] = frozenset(
    BootEnv.env_key(field) for field in ("credentials_ref", "model_env_key")
)
_SECRET_SUFFIX = "-tokens"
# Kubernetes object names and label values are capped at 63 characters; the
# pool carries the longest suffix.
_MAX_NAME = 63
_MAX_CLAIM = _MAX_NAME - len(RESOURCES_POOL_SUFFIX)


@dataclass(frozen=True)
class ClaimObjectNames:
    """Names of the per-claim template, warm pool, and token Secret."""

    template: str
    pool: str
    secret: str


def claim_object_names(claim: str) -> ClaimObjectNames:
    """Per-claim object names, under the suffixes the admission policy admits.

    A claim name too long for the derived pool name is refused before any write.
    """

    if len(claim) > _MAX_CLAIM:
        raise ValueError(
            f"claim name {claim!r} is longer than {_MAX_CLAIM} characters; "
            "its per-claim object names would exceed 63"
        )
    return ClaimObjectNames(
        template=resources_object_name("template", f"{claim}{RESOURCES_TEMPLATE_SUFFIX}"),
        pool=resources_object_name("warmpool", f"{claim}{RESOURCES_POOL_SUFFIX}"),
        secret=f"{claim}{_SECRET_SUFFIX}",
    )


def split_claim_tokens(env: Mapping[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(tokens, rest)``: the scoped tokens and the env without them.

    An empty token value means the turn has no such token. It is dropped from
    both halves rather than written to the Secret, where the runner would read
    an empty string as a credential. ``env`` is not changed.
    """

    tokens = {key: value for key, value in env.items() if key in CLAIM_TOKEN_ENVS and value}
    rest = {key: value for key, value in env.items() if key not in CLAIM_TOKEN_ENVS}
    return tokens, rest


def claim_template_spec(
    source_spec: Mapping[str, Any],
    *,
    secret_name: str,
    token_names: Iterable[str],
    runner_resources: dict[str, Any] | None,
    executor_secret_names: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Copy ``source_spec`` with each token read from ``secret_name`` on the runner.

    Only the container named ``runner`` gets the references; init containers
    never see a token. A same-name entry already on the runner (a warm-pod
    placeholder) is replaced, not duplicated. ``runner_resources``, when set, is
    validated and applied to every container first. ``source_spec`` is not
    changed.

    ``executor_secret_names`` set marks an executor claim (@spec
    ACTION-EXECUTOR-5): the runner additionally loses ``CURIE_CREDENTIALS``, the
    model env-key declaration, and every connector secret ``secretKeyRef`` whose
    name is not in the set. Kept entries stay verbatim and in order; init
    containers and the rest of the pod spec are untouched.
    """

    spec = (
        claim_resources_spec(dict(source_spec), runner_resources)
        if runner_resources is not None
        else copy.deepcopy(dict(source_spec))
    )
    containers = spec.get("podTemplate", {}).get("spec", {}).get("containers") or []
    runner = next((c for c in containers if c.get("name") == _RUNNER_CONTAINER), None)
    if runner is None:
        raise ValueError("source template has no runner container")
    names = list(token_names)
    env = [entry for entry in runner.get("env") or [] if entry.get("name") not in names]
    if executor_secret_names is not None:
        env = [entry for entry in env if _executor_keeps(entry, executor_secret_names)]
    for key in names:
        env.append(
            {
                "name": key,
                "valueFrom": {
                    "secretKeyRef": {"name": secret_name, "key": key, "optional": False}
                },
            }
        )
    runner["env"] = env
    return spec


def _is_connector_secret_ref(entry: Mapping[str, Any]) -> bool:
    ref = (entry.get("valueFrom") or {}).get("secretKeyRef") or {}
    return str(ref.get("name", "")).endswith(CONNECTOR_SECRET_NAME_SUFFIX)


def _executor_keeps(entry: Mapping[str, Any], secret_names: frozenset[str]) -> bool:
    """Whether an executor claim's runner keeps this source env entry."""

    name = entry.get("name")
    if name in EXECUTOR_WITHHELD_ENVS:
        return False
    if _is_connector_secret_ref(entry):
        return name in secret_names
    return True
