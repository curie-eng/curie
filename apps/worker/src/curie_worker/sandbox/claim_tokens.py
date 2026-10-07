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
import json
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
_MODEL_ENV_KEY_ENV = BootEnv.env_key("model_env_key")
# Every model credential an executor runner refuses to boot with (@spec
# ACTION-EXECUTOR-4, ACTION-EXECUTOR-5), so its claim never carries one. This is
# the worker's copy of the runner's two inventories, which the worker cannot
# import: ``runner/src/curie_runner/__main__.py::_EXECUTOR_REFUSED_CREDENTIALS``
# (read through ``executor_model_credentials``) and
# ``runner/src/curie_runner/subprocess_env.py::CLI_PARENT_MODEL_KEYS``.
# ``apps/worker/tests/sandbox/test_executor_claim.py`` derives its oracle from
# those two; keep this set a superset of both. The names ``CURIE_MODEL_ENV_KEY``
# declares are model credentials too; ``declared_model_env_keys`` resolves them
# per claim.
EXECUTOR_WITHHELD_ENVS: frozenset[str] = frozenset(
    {
        BootEnv.env_key("credentials_ref"),
        _MODEL_ENV_KEY_ENV,
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "ANTHROPIC_FOUNDRY_API_KEY",
        "ANTHROPIC_CUSTOM_HEADERS",
    }
)


def declared_model_env_keys(raw: str) -> frozenset[str]:
    """The env names a ``CURIE_MODEL_ENV_KEY`` value declares.

    Mirrors ``runner/src/curie_runner/sdk_auth.py::parse_env_keys``: a JSON array
    of names or a single bare name. It is used only to withhold names, so it is
    lenient where the runner is strict: any string in an array counts, and a
    value of another shape declares nothing the runner would read.
    """

    raw = raw.strip()
    if not raw:
        return frozenset()
    try:
        decoded: object = json.loads(raw)
    except ValueError:
        decoded = raw
    if isinstance(decoded, str):
        items: list[object] = [decoded]
    elif isinstance(decoded, list):
        items = list(decoded)
    else:
        return frozenset()
    return frozenset(item.strip() for item in items if isinstance(item, str) and item.strip())


def executor_withheld_names(env: Mapping[str, str]) -> frozenset[str]:
    """Model credential names withheld from an executor claim given ``env``."""

    return EXECUTOR_WITHHELD_ENVS | declared_model_env_keys(env.get(_MODEL_ENV_KEY_ENV, ""))


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
    executor_withheld: Iterable[str] = (),
) -> dict[str, Any]:
    """Copy ``source_spec`` with each token read from ``secret_name`` on the runner.

    Only the container named ``runner`` gets the references; init containers
    never see a token. A same-name entry already on the runner (a warm-pod
    placeholder) is replaced, not duplicated. ``runner_resources``, when set, is
    validated and applied to every container first. ``source_spec`` is not
    changed.

    ``executor_secret_names`` set marks an executor claim (@spec
    ACTION-EXECUTOR-5): the runner additionally loses every model credential
    (``EXECUTOR_WITHHELD_ENVS``, the names the template's own
    ``CURIE_MODEL_ENV_KEY`` declares, and ``executor_withheld``), every
    connector secret ``secretKeyRef`` whose name is not in the set, and every
    ``envFrom`` source, whose keys cannot be checked here. A declaration this
    copy cannot read (a ``valueFrom`` one) raises ``ValueError``. Kept entries
    stay verbatim and in order; init containers and the rest of the pod spec
    are untouched.
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
        withheld = _template_withheld_names(env) | frozenset(executor_withheld)
        env = [
            entry for entry in env if _executor_keeps(entry, executor_secret_names, withheld)
        ]
        runner.pop("envFrom", None)
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


def _template_withheld_names(env: list[dict[str, Any]]) -> frozenset[str]:
    """``EXECUTOR_WITHHELD_ENVS`` plus what the runner's declaration names."""

    names = set(EXECUTOR_WITHHELD_ENVS)
    for entry in env:
        if entry.get("name") != _MODEL_ENV_KEY_ENV:
            continue
        if "valueFrom" in entry:
            raise ValueError(
                f"source template declares {_MODEL_ENV_KEY_ENV} by valueFrom; an "
                "executor claim cannot tell which model credentials it names"
            )
        names |= declared_model_env_keys(str(entry.get("value") or ""))
    return frozenset(names)


def _executor_keeps(
    entry: Mapping[str, Any], secret_names: frozenset[str], withheld: frozenset[str]
) -> bool:
    """Whether an executor claim's runner keeps this source env entry."""

    name = entry.get("name")
    if name in withheld:
        return False
    if _is_connector_secret_ref(entry):
        return name in secret_names
    return True
