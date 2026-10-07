"""@spec ACTION-EXECUTOR-16: the sealing key never reaches a sandbox.

ADR 0124 decision 1 requires the snapshot sealing key to reach only the hosted
connector. The first release recognizes it by two reserved names,
``SNAPSHOT_SEALING_KEY`` and ``SNAPSHOT_SEALING_KEYS_RETAINED``, and
``inject_connector_secrets`` withholds both from every sandbox. A connector
secret stored under either name (an agent's ``secrets`` map is the only thing
that hands values to this function) must be neither injected into the boot env
nor named in the ``CURIE_CONNECTOR_SECRET_KEYS`` marker: the k8s substrate
reads the marker to wire a per-agent ``secretKeyRef``, so a marked name would
still be delivered on the cluster tier even with the value dropped here.

Three call shapes are pinned, because both write sites go through the helper
and a fix made in only one of them would leave the other leaking:

* the helper itself,
* ``BindingResolver.boot_env`` (the bound-run path),
* ``EvalStreamConsumer._boot_env`` (the eval path).

Other names pass through unchanged on each, so the withholding cannot pass by
dropping every connector secret. No mocks and no DB: none of these call
shapes touches Postgres or Valkey.
"""

from __future__ import annotations

import uuid

import pytest
from curie_worker.binding import (
    CONNECTOR_SECRET_KEYS_ENV,
    BindingResolver,
    ResolvedDeployment,
    inject_connector_secrets,
)
from curie_worker.config import WorkerConfig
from curie_worker.eval import EvalJob, EvalStreamConsumer

RESERVED = ("SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED")
SEAL_VALUE = "seal-sentinel-value"
# An ordinary connector credential, and a sealing key under a name that is NOT
# reserved: custody refuses it elsewhere (ACTION-EXECUTOR-11), but the sandbox
# path treats it as any other connector secret.
PASSTHROUGH = {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_ok", "MY_SEAL_KEY": "plain-seal"}

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")


def _secrets(reserved: str) -> dict[str, str]:
    return {reserved: SEAL_VALUE, **PASSTHROUGH}


def _assert_withheld(env: dict[str, str], reserved: str) -> None:
    assert reserved not in env, f"{reserved} reached the sandbox env"
    assert SEAL_VALUE not in env.values(), "the sealing key value reached the sandbox env"
    marked = env.get(CONNECTOR_SECRET_KEYS_ENV, "").split(",")
    assert reserved not in marked, f"{reserved} is named in the connector-secret marker"
    # Everything else still passes through, and is exactly what is marked.
    for name, value in PASSTHROUGH.items():
        assert env[name] == value
    assert env[CONNECTOR_SECRET_KEYS_ENV] == ",".join(sorted(PASSTHROUGH))


@pytest.mark.parametrize("reserved", RESERVED)
def test_inject_connector_secrets_withholds_the_sealing_key(reserved: str) -> None:
    """@spec ACTION-EXECUTOR-16"""
    env: dict[str, str] = {}
    inject_connector_secrets(env, _secrets(reserved), agent_label=_AGENT)
    _assert_withheld(env, reserved)


@pytest.mark.parametrize("reserved", RESERVED)
def test_bound_run_boot_env_withholds_the_sealing_key(reserved: str) -> None:
    """@spec ACTION-EXECUTOR-16: the runs binding's write site."""
    resolved = ResolvedDeployment(
        agent_name="test-agent",
        agent_id=_AGENT,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        secrets=_secrets(reserved),
    )
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()
    _assert_withheld(resolver.boot_env(resolved, "thread-1"), reserved)


@pytest.mark.parametrize("reserved", RESERVED)
def test_eval_boot_env_withholds_the_sealing_key(reserved: str) -> None:
    """@spec ACTION-EXECUTOR-16: the eval consumer's write site."""
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = EvalJob(
        agent_id=_AGENT,
        version_id=uuid.uuid4(),
        sha="deadbeef",
        suite="s",
        bundle_ref="bundles/x.zip",
        target_url=None,
        model=None,
        requested_at="2026-10-06T00:00:00+00:00",
    )
    env = consumer._boot_env(item, _secrets(reserved), None, model=None)
    _assert_withheld(env, reserved)
