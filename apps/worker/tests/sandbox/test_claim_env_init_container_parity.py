"""#2612: the claim's per-init-container targeting matches what the chart declares.

A ``SandboxClaim`` env entry with no ``containerName`` is injected into the
runner container only. Every ``CURIE_*`` value the SandboxTemplate's staging
init containers bake as an empty default is therefore a key that MUST be
repeated, per container, with an explicit ``containerName`` -- otherwise the
init container keeps the empty default, takes its no-op path, exits 0, and the
sandbox boots having staged nothing while every container reported success
(issue #2612, observed on chart 0.8.7).

``KubernetesSandboxClient.create_claim`` does that targeting today. What had no
gate is the *correspondence*: the container names and the key set live in the
worker's Python, the init containers live in the chart's Go template, and
nothing failed when they drifted. Renaming an init container, adding a fourth
staging init container, or teaching an existing one a new ``CURIE_*`` key
regressed straight back to the silent no-op.

This test reads the chart template as text rather than a Helm render, so it runs
in the plain Python lane with no ``helm`` binary. The render-level proof (the
no-op log line naming the fix) is
``charts/curie/ci/claim-env-init-container-assertions.sh``.
"""

from __future__ import annotations

import re
from pathlib import Path

from curie_worker.sandbox.k8s import (
    BUNDLE_INIT_CONTAINERS,
    BUNDLE_REF_ENV,
    WORKSPACE_INIT_CONTAINERS,
    WORKSPACE_REF_ENV,
    WORKSPACE_SHA256_ENV,
)

_REPO = Path(__file__).resolve().parents[4]
_TEMPLATE = _REPO / "charts/curie/templates/agent-sandbox.yaml"

# The init containers render at eight-space indentation inside `initContainers:`,
# and their env entries at twelve. Go-template control lines (`{{- if ... }}`)
# never carry a `- name:` head, so an indentation scan reads the same structure a
# render would without needing one.
_INIT_CONTAINER = re.compile(r"^ {8}- name: ([a-z0-9][a-z0-9-]*)\s*$")
_ENV_ENTRY = re.compile(r"^ {12}- name: (CURIE_[A-Z0-9_]+)\s*$")
# The keys the worker targets per init container, from the worker's own
# declaration. The chart is the other half of this contract.
_TARGETED: dict[str, frozenset[str]] = {
    **{c: frozenset({BUNDLE_REF_ENV}) for c in BUNDLE_INIT_CONTAINERS},
    **{
        c: frozenset({WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV})
        for c in WORKSPACE_INIT_CONTAINERS
    },
}

# CURIE_ env an init container reads that is deliberately NOT claim-settable
# (chart-wired from values or a Secret). Empty today: every CURIE_ key the
# staging init containers read is claim-settable, and the endpoint, bucket and
# credential env they also read are S3_*/AWS_*-prefixed and out of a claim's
# reach by construction. It exists so a new key has to be classified on purpose
# -- targeted by the worker, or declared worker-side here with its reason.
_CHART_WIRED_ONLY: frozenset[str] = frozenset()


def _init_container_env() -> dict[str, set[str]]:
    """Map each init container in the SandboxTemplate to the CURIE_ keys it declares."""

    found: dict[str, set[str]] = {}
    current: str | None = None
    in_init_containers = False
    for line in _TEMPLATE.read_text(encoding="utf-8").splitlines():
        if line.strip() == "initContainers:":
            in_init_containers = True
            continue
        if not in_init_containers:
            continue
        if line.strip() == "containers:":
            break
        match = _INIT_CONTAINER.match(line)
        if match:
            current = match.group(1)
            found.setdefault(current, set())
            continue
        if current is None:
            continue
        env_match = _ENV_ENTRY.match(line)
        if env_match:
            found[current].add(env_match.group(1))
    return found


def test_the_chart_declares_exactly_the_init_containers_the_worker_targets() -> None:
    declared = set(_init_container_env())
    assert declared, f"no init containers parsed out of {_TEMPLATE}"
    assert declared == set(_TARGETED), (
        "the SandboxTemplate's staging init containers and the worker's "
        "containerName targets have drifted. A container the worker does not "
        "target receives no claim env at all and silently stages nothing "
        "(#2612).\n"
        f"  chart  ({_TEMPLATE}): {sorted(declared)}\n"
        f"  worker (curie_worker.sandbox.k8s): {sorted(_TARGETED)}"
    )


def test_every_claim_consumed_init_env_key_is_targeted_by_name() -> None:
    for container, keys in sorted(_init_container_env().items()):
        unreachable = keys - _TARGETED.get(container, frozenset()) - _CHART_WIRED_ONLY
        assert not unreachable, (
            f"init container {container!r} reads {sorted(unreachable)}, which "
            "KubernetesSandboxClient.create_claim never copies with an explicit "
            "containerName. A claim setting it reaches the runner only and the "
            "staging silently no-ops (#2612). Either target the key in "
            "curie_worker.sandbox.k8s.create_claim, or add it to "
            "_CHART_WIRED_ONLY with the reason it is not claim-settable."
        )


def test_the_runner_and_the_worker_agree_on_the_bundle_init_container_names() -> None:
    """The runner's operator-facing diagnosis names the same containers."""
    from curie_runner.plugin import BUNDLE_INIT_CONTAINERS as RUNNER_BUNDLE_INIT

    assert RUNNER_BUNDLE_INIT == BUNDLE_INIT_CONTAINERS
