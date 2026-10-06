"""Fully undoable action records for the undo route tests (ACTION-EXECUTOR-11).

Under the executor contract an undo is ruled only on a record whose derived
``undoable`` is true: succeeded, attributed to an agent, a sealed envelope in
``prior_state``, a ``post_version``, a ``target``, a ``connector`` and its
``connector_digest``, a ``restore_capable`` capability row, and sealing key
custody in the agent's in-force version. Tests about something else (who may
undo, the conflict rule, claiming once) start from such a record so that the
snapshot rule is not what decides them.

The agent, its version and its deployment go through the real API, so custody
is read from a really stored bundle. The ledger row is opened and completed
through the real worker-facing routes; the three ledger columns those routes
do not accept yet (the worker's recording of them is plan task 10) and the
capability row (written by the probe route, plan task 4) are set with SQL in
the columns the spec names.

Imported by test modules through the ``sys.path`` entry ``conftest.py`` adds
(see ``_migration_support``).
"""

from __future__ import annotations

import base64
import io
import json
import os
import tarfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api.config import get_settings

AGENT_NAME = "restorer-bot"
CONNECTOR = "k8s"
DIGEST = "sha256:" + "ab" * 32
IMAGE = f"ghcr.io/example/k8s-restorer@{DIGEST}"
ENVELOPE = {
    "sealed": "curie.snapshot.v1",
    "kid": "seal-2026-10",
    "ciphertext": base64.b64encode(b"opaque sealed prior state").decode(),
}
POST_VERSION = "rv-1042"
LEFT = {"spec": {"replicas": 10}}
TARGET = {"kind": "Deployment", "namespace": "public", "name": "api"}

SEALED_CONNECTORS = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
    secrets:
      - name: SNAPSHOT_SEALING_KEY
        from_secret: k8s-restorer-seal
"""

_LEDGER_COLUMNS = ("post_version", "connector", "connector_digest")

# @spec ACTION-EXECUTOR-1: the one setting the chart and compose render into
# both the API and the worker. Off by default.
EXECUTOR_SETTING = "CURIE_ACTION_EXECUTOR_ENABLED"


@pytest.fixture
def executor_enabled() -> Iterator[None]:
    """Turn the action executor on for one test's app, then put the setting back.

    Requested before ``client`` (through ``usefixtures``), so the app the test
    talks to is built with the setting on. A module that wants it imports this
    name; ``conftest`` does not make it global, because the default is off.
    """

    before = os.environ.get(EXECUTOR_SETTING)
    os.environ[EXECUTOR_SETTING] = "true"
    get_settings.cache_clear()
    try:
        yield
    finally:
        if before is None:
            os.environ.pop(EXECUTOR_SETTING, None)
        else:
            os.environ[EXECUTOR_SETTING] = before
        get_settings.cache_clear()


def executions_of(action_id: str) -> list[dict[str, Any]]:
    """Every execution row naming ``action_id``, oldest first."""

    return sql_dicts(
        "SELECT * FROM curie.action_executions WHERE subject_action_id = :id "
        "ORDER BY created_at, id",
        {"id": uuid.UUID(action_id)},
    )


def _archive(tmp_path: Path, name: str) -> bytes:
    root = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "skills" / name).mkdir(parents=True)
    (root / "skills" / name / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(SEALED_CONNECTORS, encoding="utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(root, arcname=name)
    return buf.getvalue()


def undoable_agent(
    client: Any, headers: dict[str, str], tmp_path: Path, name: str = AGENT_NAME
) -> str:
    """An agent whose in-force version seals ``k8s`` and whose digest is probed capable."""

    agent = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex[:9].upper()}"},
        },
        headers=headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = str(agent.json()["id"])
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "test"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bundle.tar.gz", _archive(tmp_path, name))},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text
    sql_rows(
        "INSERT INTO curie.connector_capabilities "
        "(agent_id, connector, digest, restore_capable, observed_at) "
        "VALUES (:agent_id, :connector, :digest, true, now())",
        {"agent_id": uuid.UUID(agent_id), "connector": CONNECTOR, "digest": DIGEST},
    )
    return agent_id


def sealed_action(
    client: Any,
    headers: dict[str, str],
    agent_id: str | None,
    *,
    gate_approval_id: str | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Record and complete one call holding every ingredient, minus any overridden.

    ``overrides`` may name completion fields (``failed``, ``result``,
    ``prior_state``, ``post_state``, ``target``, ``detail``) or the ledger
    columns ``post_version``, ``connector`` and ``connector_digest``.
    """

    ledger = {"post_version": POST_VERSION, "connector": CONNECTOR, "connector_digest": DIGEST}
    for column in _LEDGER_COLUMNS:
        if column in overrides:
            ledger[column] = overrides.pop(column)
    opened = client.post(
        "/actions",
        json={
            "agent_id": agent_id,
            "conversation_id": "C1",
            "call_id": "toolu_01",
            "tool": "mcp__k8s__scale",
            "arguments": {"name": "api", "replicas": 10},
            "detail": "non-idempotent tool executed",
            "gate_approval_id": gate_approval_id,
            "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
        },
        headers=headers,
    )
    assert opened.status_code == 201, opened.text
    action_id = str(opened.json()["id"])
    completion: dict[str, Any] = {
        "failed": False,
        "result": {"ok": True, "version": POST_VERSION},
        "prior_state": ENVELOPE,
        "post_state": LEFT,
        "target": TARGET,
        "detail": "non-idempotent tool completed",
    }
    completion.update(overrides)
    completed = client.post(f"/actions/{action_id}/complete", json=completion, headers=headers)
    assert completed.status_code == 200, completed.text
    sql_rows(
        "UPDATE curie.agent_actions SET post_version = :post_version, "
        "connector = :connector, connector_digest = :connector_digest WHERE id = :id",
        {**ledger, "id": uuid.UUID(action_id)},
    )
    fetched = client.get(f"/actions/{action_id}", headers=headers)
    assert fetched.status_code == 200, fetched.text
    return dict(fetched.json())
