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

import pytest
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
#
# The scan FAILS CLOSED. A `- name:` head this file does not recognise -- a
# quoted key, a different indentation, a templated name -- is an error, not an
# absence. Treating it as an absence is how a newly consumed staging key would
# sail past the very gate that advertises it cannot.
_INIT_CONTAINER = re.compile(r"^ {8}- name: (\S+)\s*$")
_ENV_ENTRY = re.compile(r"^ {12}- name: (\S+)\s*$")
_ANY_NAME_HEAD = re.compile(r"^(\s*)- name:(.*)$")
_PLAIN_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


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


class TemplateScanError(AssertionError):
    """A `- name:` head in the init-container region this scanner cannot classify."""


def _init_container_env() -> dict[str, set[str]]:
    """Map each init container in the SandboxTemplate to the CURIE_ keys it declares.

    Raises ``TemplateScanError`` on any `- name:` head inside the region that is
    neither an init container at eight spaces nor an env entry at twelve. That
    is the fail-closed half: an env declaration written in a shape this scanner
    does not read must break the gate, not slip through it.
    """

    found: dict[str, set[str]] = {}
    current: str | None = None
    in_init_containers = False
    for number, line in enumerate(_TEMPLATE.read_text(encoding="utf-8").splitlines(), 1):
        if line.strip() == "initContainers:":
            in_init_containers = True
            continue
        if not in_init_containers:
            continue
        if line.strip() == "containers:":
            break
        head = _ANY_NAME_HEAD.match(line)
        if head is None:
            continue

        match = _INIT_CONTAINER.match(line)
        if match:
            name = _unquote(match.group(1))
            if not _PLAIN_NAME.match(name):
                raise TemplateScanError(
                    f"{_TEMPLATE}:{number}: init container name {name!r} is not a "
                    "plain identifier this scanner can compare against the "
                    "worker's containerName targets."
                )
            current = name
            found.setdefault(current, set())
            continue

        env_match = _ENV_ENTRY.match(line)
        if env_match is None or current is None:
            raise TemplateScanError(
                f"{_TEMPLATE}:{number}: unreadable '- name:' declaration inside "
                f"the init-container region: {line!r}. This gate only reads init "
                "containers at eight-space and env entries at twelve-space "
                "indentation; anything else is treated as a scan failure rather "
                "than as an absent key, because an absence here would silently "
                "un-gate a newly consumed staging key (#2612). Either write the "
                "declaration in the shape above, or teach this scanner to read "
                "the new one."
            )
        key = _unquote(env_match.group(1))
        if not _PLAIN_NAME.match(key):
            raise TemplateScanError(
                f"{_TEMPLATE}:{number}: env name {key!r} is not a plain "
                "identifier; this gate cannot tell whether the worker targets it."
            )
        if key.startswith("CURIE_"):
            found[current].add(key)
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


# --- the scanner's own fail-closed property ------------------------------------
#
# The gate above is only worth anything if a declaration it cannot read breaks
# it. An earlier revision matched only bare, twelve-space `- name: CURIE_...`
# heads and treated everything else as an absent key, so a quoted key or an
# env list at another indentation slipped through the drift gate untargeted.


def _scan(text: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, set[str]]:
    stand_in = tmp_path / "agent-sandbox.yaml"
    stand_in.write_text(text, encoding="utf-8")
    monkeypatch.setitem(globals(), "_TEMPLATE", stand_in)
    return _init_container_env()


_ONE_CONTAINER = """
      initContainers:
        - name: bundle-fetch
          env:
{env}
      containers:
"""


def test_a_quoted_env_key_is_read_not_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    found = _scan(
        _ONE_CONTAINER.format(
            env='            - name: "CURIE_BUNDLE_REF"\n              value: ""'
        ),
        monkeypatch,
        tmp_path,
    )
    assert found == {"bundle-fetch": {"CURIE_BUNDLE_REF"}}


def test_an_env_entry_at_an_unread_indentation_fails_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(TemplateScanError, match="unreadable"):
        _scan(
            _ONE_CONTAINER.format(
                env='              - name: CURIE_BUNDLE_REF\n                value: ""'
            ),
            monkeypatch,
            tmp_path,
        )


def test_a_templated_env_name_fails_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A name the scanner cannot resolve statically is an error, not an absence."""
    with pytest.raises(TemplateScanError, match="not a plain identifier"):
        _scan(
            _ONE_CONTAINER.format(
                env='            - name: {{$key}}\n              value: ""'
            ),
            monkeypatch,
            tmp_path,
        )

    # A templated name carrying a space does not even reach that check; it is
    # rejected one step earlier, still closed.
    with pytest.raises(TemplateScanError, match="unreadable"):
        _scan(
            _ONE_CONTAINER.format(
                env='            - name: {{ $key }}\n              value: ""'
            ),
            monkeypatch,
            tmp_path,
        )
