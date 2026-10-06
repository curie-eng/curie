"""What makes a recorded action undoable once restores are executed (ACTION-EXECUTOR-11).

The connector action executor contract
(docs/superpowers/specs/2026-10-06-connector-action-executor.md) replaces the
cleartext ``prior_state``/``post_state`` rule with one that needs every
ingredient a pinned, sealed restore needs. ``undoable`` becomes true exactly
when the record is:

* succeeded and attributed to an agent;
* holding a valid sealed envelope in ``prior_state`` (ACTION-EXECUTOR-9);
* carrying a ``post_version``, a ``target`` and a ``connector_digest``;
* backed by a ``restore_capable`` capability row for that agent, connector and
  digest (ACTION-EXECUTOR-13);
* under sealing key custody, computed from the agent's in-force version at read
  time and never cached: that version declares ``SNAPSHOT_SEALING_KEY`` as a
  ``SecretRef`` on that connector (ACTION-EXECUTOR-16);
* not the subject of a restore execution that is not ``refused``.

Each test removes exactly one ingredient from a record that is otherwise
complete, and asserts a complete control record beside it IS undoable, so a
read that answers ``False`` for everything cannot pass.

The ledger rows, capability rows and executions are written with SQL in the
columns the spec names: the worker's recording wire and the probe route that
produce them belong to later tasks. The agent, its versions and deployments go
through the real API, against real Postgres and the object store, because
custody is read from the in-force version's stored bundle. ``undoable`` is
always read back through the real API (``GET /actions/{id}`` and the list).
"""

from __future__ import annotations

import base64
import io
import json
import tarfile
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_rows

pytestmark = pytest.mark.usefixtures("clean_db")

AGENT_NAME = "restorer-bot"
CONNECTOR = "k8s"
DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32
IMAGE = f"ghcr.io/example/k8s-restorer@{DIGEST}"

# The sealed envelope of ACTION-EXECUTOR-9: exactly these three keys. The
# ciphertext is opaque to the platform; any standard base64 will do.
ENVELOPE = {
    "sealed": "curie.snapshot.v1",
    "kid": "seal-2026-10",
    "ciphertext": base64.b64encode(b"opaque sealed prior state \x00\x01\x02").decode(),
}
POST_VERSION = "rv-1042"
TARGET = {"kind": "Deployment", "namespace": "public", "name": "api"}

# connectors.yaml bodies. Only the first gives the connector key custody.
SEALED = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
    secrets:
      - name: SNAPSHOT_SEALING_KEY
        from_secret: k8s-restorer-seal
"""
UNSEALED = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
"""
PLAIN_OTHER_NAME = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
    secrets: [MY_SEAL_KEY]
"""
SECRET_REF_OTHER_NAME = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
    secrets:
      - name: MY_SEAL_KEY
        from_secret: k8s-restorer-seal
"""
RETAINED_ONLY = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
    secrets:
      - name: SNAPSHOT_SEALING_KEYS_RETAINED
        from_secret: k8s-restorer-seal
"""
SEALED_ON_ANOTHER_CONNECTOR = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
  vault:
    image: ghcr.io/example/vault-restorer@{OTHER_DIGEST}
    secrets:
      - name: SNAPSHOT_SEALING_KEY
        from_secret: vault-restorer-seal
"""


# --------------------------------------------------------------------------- #
# Agent, version and deployment through the real API
# --------------------------------------------------------------------------- #


def _archive(tmp_path: Path, connectors_yaml: str) -> bytes:
    root = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": AGENT_NAME, "version": "0.1.0", "description": "t"}),
        encoding="utf-8",
    )
    (root / "skills" / AGENT_NAME).mkdir(parents=True)
    (root / "skills" / AGENT_NAME / "SKILL.md").write_text(
        f"---\nname: {AGENT_NAME}\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors_yaml, encoding="utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(root, arcname=AGENT_NAME)
    return buf.getvalue()


def _agent(client: Any, headers: dict[str, str], name: str = AGENT_NAME) -> str:
    response = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex[:9].upper()}"},
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


def _deploy(
    client: Any,
    headers: dict[str, str],
    tmp_path: Path,
    agent_id: str,
    connectors_yaml: str,
    *,
    environment: str = "dev",
) -> str:
    """Store a version with this connectors.yaml and make it the active deployment."""

    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "test"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bundle.tar.gz", _archive(tmp_path, connectors_yaml))},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": environment},
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text
    return version_id


@pytest.fixture
def sealed_agent(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> str:
    """An agent whose in-force version declares the sealing key as a SecretRef."""

    agent_id = _agent(client, auth_headers)
    _deploy(client, auth_headers, tmp_path, agent_id, SEALED)
    return agent_id


def _capable_control(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> str:
    """A complete record of a second sealed, probed agent.

    For tests whose subject agent deliberately lacks a capability row: the
    control proves the read can answer ``True`` at all, so the subject's
    ``False`` is about the missing row and nothing else.
    """

    control_agent = _agent(client, auth_headers, name="control-bot")
    _deploy(client, auth_headers, tmp_path, control_agent, SEALED)
    _capability(control_agent)
    return _action(control_agent)


# --------------------------------------------------------------------------- #
# Ledger, capability and execution rows in the spec's columns
# --------------------------------------------------------------------------- #


def _action(agent_id: str | None, **overrides: Any) -> str:
    """A succeeded record holding every ingredient, minus whatever is overridden."""

    fields: dict[str, Any] = {
        "status": "succeeded",
        "prior_state": ENVELOPE,
        "post_state": None,
        "post_version": POST_VERSION,
        "target": TARGET,
        "connector": CONNECTOR,
        "connector_digest": DIGEST,
    }
    fields.update(overrides)
    action_id = uuid.uuid4()
    call_id = f"toolu_{uuid.uuid4().hex[:10]}"

    def _json(value: Any) -> str | None:
        return None if value is None else json.dumps(value)

    sql_rows(
        "INSERT INTO curie.agent_actions "
        "(id, agent_id, conversation_id, call_id, tool, arguments, result, prior_state, "
        "post_state, target, status, dedupe_key, completed_at, post_version, connector, "
        "connector_digest) "
        "VALUES (:id, :agent_id, 'C1', :call_id, 'mcp__k8s__scale', "
        "CAST(:arguments AS jsonb), CAST(:result AS jsonb), CAST(:prior AS jsonb), "
        "CAST(:post AS jsonb), CAST(:target AS jsonb), :status, :key, now(), "
        ":post_version, :connector, :digest)",
        {
            "id": action_id,
            "agent_id": None if agent_id is None else uuid.UUID(agent_id),
            "call_id": call_id,
            "arguments": json.dumps({"name": "api", "replicas": 10}),
            "result": json.dumps({"ok": True, "version": fields["post_version"]}),
            "prior": _json(fields["prior_state"]),
            "post": _json(fields["post_state"]),
            "target": _json(fields["target"]),
            "status": fields["status"],
            "key": f"event-{uuid.uuid4()}:{call_id}",
            "post_version": fields["post_version"],
            "connector": fields["connector"],
            "digest": fields["connector_digest"],
        },
    )
    return str(action_id)


def _capability(
    agent_id: str,
    *,
    connector: str = CONNECTOR,
    digest: str = DIGEST,
    restore_capable: bool = True,
) -> None:
    sql_rows(
        "INSERT INTO curie.connector_capabilities "
        "(agent_id, connector, digest, restore_capable, observed_at) "
        "VALUES (:agent_id, :connector, :digest, :capable, now())",
        {
            "agent_id": uuid.UUID(agent_id),
            "connector": connector,
            "digest": digest,
            "capable": restore_capable,
        },
    )


_AUTHORITY = {"restore": "undo_ruling", "forward": "policy"}


def _execution(agent_id: str, action_id: str, *, state: str, kind: str = "restore") -> None:
    execution_id = uuid.uuid4()
    authority_ref = str(uuid.uuid4())
    sql_rows(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, tool, subject_action_id, connector_digest, "
        "authority_kind, authority_ref, requested_by, idempotency_key, state, attempt, "
        "created_at) "
        "VALUES (:id, :kind, :agent_id, :connector, :tool, :subject, :digest, "
        ":authority_kind, :authority_ref, 'U-operator', :key, :state, 0, now())",
        {
            "id": execution_id,
            "kind": kind,
            "agent_id": uuid.UUID(agent_id),
            "connector": CONNECTOR,
            "tool": "restore" if kind == "restore" else "scale",
            "subject": uuid.UUID(action_id),
            "digest": DIGEST,
            "authority_kind": _AUTHORITY[kind],
            "authority_ref": authority_ref,
            "key": (
                f"restore:{action_id}:{authority_ref}"
                if kind == "restore"
                else f"forward:{execution_id}"
            ),
            "state": state,
        },
    )


def _undoable(client: Any, headers: dict[str, str], action_id: str) -> bool:
    response = client.get(f"/actions/{action_id}", headers=headers)
    assert response.status_code == 200, response.text
    value = response.json()["undoable"]
    assert isinstance(value, bool)
    return value


# --------------------------------------------------------------------------- #
# The complete record
# --------------------------------------------------------------------------- #


def test_a_sealed_pinned_capable_record_under_custody_is_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11: every ingredient present, so ``undoable`` is true.

    ``post_state`` is absent on purpose: "``post`` is no longer required or read
    for a sealed record" (ACTION-EXECUTOR-9).
    """

    _capability(sealed_agent)
    action_id = _action(sealed_agent)

    assert _undoable(client, auth_headers, action_id) is True


def test_the_list_read_agrees_with_the_single_read(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11: both API reads derive ``undoable`` the same way."""

    _capability(sealed_agent)
    complete = _action(sealed_agent)
    unsealed = _action(sealed_agent, prior_state=None)

    listed = client.get("/actions", params={"agent_id": sealed_agent}, headers=auth_headers)
    assert listed.status_code == 200, listed.text
    by_id = {row["id"]: row["undoable"] for row in listed.json()}

    assert by_id == {complete: True, unsealed: False}


# --------------------------------------------------------------------------- #
# One record ingredient missing at a time
# --------------------------------------------------------------------------- #

_MISSING_RECORD_INGREDIENT: dict[str, dict[str, Any]] = {
    # The call never succeeded, so there is nothing known to reverse.
    "failed": {"status": "failed"},
    "pending": {"status": "pending"},
    # refused_unsealed: no envelope at all.
    "no prior_state": {"prior_state": None},
    # refused_unversioned: nothing to compare the observed version against.
    "no post_version": {"post_version": None},
    # Nowhere to put the state back.
    "no target": {"target": None},
    # refused_no_digest: the connector image the restore must run under is unknown.
    "no connector_digest": {"connector_digest": None},
    # Without a connector the capability and custody lookups have no key.
    "no connector": {"connector": None},
}


@pytest.mark.parametrize(
    "overrides", list(_MISSING_RECORD_INGREDIENT.values()), ids=list(_MISSING_RECORD_INGREDIENT)
)
def test_each_missing_record_ingredient_alone_makes_the_record_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str, overrides: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-11: "each missing ingredient alone makes ``undoable`` false"."""

    _capability(sealed_agent)
    control = _action(sealed_agent)
    missing = _action(sealed_agent, **overrides)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, missing) is False


def test_a_record_without_an_agent_is_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-3: refused_no_agent.

    The executor runs under the agent's own binding (ACTION-EXECUTOR-5); a
    record with no agent names no binding to run under.
    """

    _capability(sealed_agent)
    control = _action(sealed_agent)
    orphan = _action(None)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, orphan) is False


_INVALID_ENVELOPES: dict[str, dict[str, Any]] = {
    "extra key": {**ENVELOPE, "alg": "aes-gcm"},
    "missing kid": {k: v for k, v in ENVELOPE.items() if k != "kid"},
    "wrong constant": {**ENVELOPE, "sealed": "curie.snapshot.v2"},
    "url-safe alphabet": {**ENVELOPE, "ciphertext": "ab-_" * 8},
    "line break in ciphertext": {**ENVELOPE, "ciphertext": "YWJj\nZGVm"},
    "oversized ciphertext": {
        **ENVELOPE,
        "ciphertext": base64.b64encode(b"\x00" * 65537).decode(),
    },
    "kid too long": {**ENVELOPE, "kid": "k" * 65},
    "kid outside alphabet": {**ENVELOPE, "kid": "seal key/1"},
    "placeholder in ciphertext": {**ENVELOPE, "ciphertext": "[REDACTED:token]"},
}


@pytest.mark.parametrize(
    "prior_state", list(_INVALID_ENVELOPES.values()), ids=list(_INVALID_ENVELOPES)
)
def test_a_prior_state_that_is_not_a_valid_envelope_is_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str, prior_state: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-9: "a valid envelope in ``prior_state``"."""

    _capability(sealed_agent)
    control = _action(sealed_agent)
    invalid = _action(sealed_agent, prior_state=prior_state)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, invalid) is False


def test_a_legacy_cleartext_row_is_never_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11: legacy cleartext rows are not undoable (refused_unsealed).

    Shaped like a row written before this change: a cleartext prior and post
    state that the old rule called undoable. Even with every new ingredient
    supplied beside it -- digest, version, capability, custody -- a cleartext
    ``prior_state`` is history, not a restorable snapshot.
    """

    _capability(sealed_agent)
    control = _action(sealed_agent)
    cleartext = _action(
        sealed_agent,
        prior_state={"spec": {"replicas": 3}},
        post_state={"spec": {"replicas": 10}},
    )
    pre_change = _action(
        sealed_agent,
        prior_state={"spec": {"replicas": 3}},
        post_state={"spec": {"replicas": 10}},
        post_version=None,
        connector=None,
        connector_digest=None,
    )

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, cleartext) is False
    assert _undoable(client, auth_headers, pre_change) is False


# --------------------------------------------------------------------------- #
# Capability (ACTION-EXECUTOR-13)
# --------------------------------------------------------------------------- #


def test_a_record_without_a_capability_row_is_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-13: refused_not_restore_capable.

    No probe has recorded this digest yet, which is "treated as restoring
    nothing".
    """

    control = _capable_control(client, auth_headers, tmp_path)
    action_id = _action(sealed_agent)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, action_id) is False


def test_a_probe_that_found_no_restore_pair_is_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-13: a stored ``restore_capable = false`` is not capable."""

    _capability(sealed_agent, restore_capable=False)
    control = _capable_control(client, auth_headers, tmp_path)
    action_id = _action(sealed_agent)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, action_id) is False


@pytest.mark.parametrize(
    "where",
    [
        {"digest": OTHER_DIGEST},
        {"connector": "vault"},
    ],
    ids=["another digest", "another connector"],
)
def test_a_capability_row_for_another_digest_or_connector_does_not_count(
    client: Any,
    auth_headers: dict[str, str],
    sealed_agent: str,
    tmp_path: Path,
    where: dict[str, str],
) -> None:
    """@spec ACTION-EXECUTOR-13: the row must match the record's agent, connector AND digest.

    A digest's tool list is a property of that image; a capable neighbour says
    nothing about the image this record ran against.
    """

    _capability(sealed_agent, **where)
    control = _capable_control(client, auth_headers, tmp_path)
    action_id = _action(sealed_agent)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, action_id) is False


def test_a_capability_row_for_another_agent_does_not_count(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-13: capability is keyed per agent, not per image alone."""

    other = _agent(client, auth_headers, name="other-restorer")
    _deploy(client, auth_headers, tmp_path, other, SEALED)
    _capability(other)
    control = _action(other)
    action_id = _action(sealed_agent)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, action_id) is False


def test_an_earlier_record_becomes_undoable_once_its_probe_lands(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-13: "actions recorded under a digest before its probe
    completes become undoable once the row lands" -- ``undoable`` is derived at
    read time, not frozen when the record is written.
    """

    action_id = _action(sealed_agent)
    assert _undoable(client, auth_headers, action_id) is False

    _capability(sealed_agent)

    assert _undoable(client, auth_headers, action_id) is True


# --------------------------------------------------------------------------- #
# Key custody from the in-force version (ACTION-EXECUTOR-16)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "connectors_yaml",
    [UNSEALED, PLAIN_OTHER_NAME, SECRET_REF_OTHER_NAME, RETAINED_ONLY, SEALED_ON_ANOTHER_CONNECTOR],
    ids=[
        "no sealing secret",
        "plain MY_SEAL_KEY",
        "SecretRef under another name",
        "retained keys only",
        "SecretRef on another connector",
    ],
)
def test_a_version_without_custody_of_the_sealing_key_is_not_undoable(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    sealed_agent: str,
    connectors_yaml: str,
) -> None:
    """@spec ACTION-EXECUTOR-16 @spec ACTION-EXECUTOR-11: refused_key_custody.

    Custody holds "only when that version declares ``SNAPSHOT_SEALING_KEY`` as a
    ``SecretRef`` on that connector". A connector sealing with any other name, a
    plain named secret, or a SecretRef on a different connector never produces
    an undoable record. The control agent, identical except for its declaration,
    is undoable.
    """

    other = _agent(client, auth_headers, name="uncustodied-bot")
    _deploy(client, auth_headers, tmp_path, other, connectors_yaml)
    _capability(sealed_agent)
    _capability(other)
    control = _action(sealed_agent)
    uncustodied = _action(other)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, uncustodied) is False


def test_a_later_version_that_drops_the_secret_ref_revokes_custody_at_once(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-16: custody is computed at read time, never cached.

    "A later version with the same digest that drops the ``SecretRef``
    therefore makes the connector's actions not undoable at once" -- including
    a record that was undoable a moment before.
    """

    _capability(sealed_agent)
    action_id = _action(sealed_agent)
    assert _undoable(client, auth_headers, action_id) is True

    _deploy(client, auth_headers, tmp_path, sealed_agent, UNSEALED)

    assert _undoable(client, auth_headers, action_id) is False


def test_restoring_the_secret_ref_restores_custody(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-16: the derivation follows whichever version is in force."""

    _capability(sealed_agent)
    action_id = _action(sealed_agent)
    _deploy(client, auth_headers, tmp_path, sealed_agent, UNSEALED)
    assert _undoable(client, auth_headers, action_id) is False

    _deploy(client, auth_headers, tmp_path, sealed_agent, SEALED)

    assert _undoable(client, auth_headers, action_id) is True


def test_custody_is_read_from_the_in_force_version_not_the_newest(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-16: "computed ... from the agent's in-force version".

    In force is the platform's existing rule (prod outranks dev, then the most
    recent deployment), the one the binding and the connector reconcile use. A
    newer dev version without the SecretRef does not displace a sealed prod
    version that is what actually runs.
    """

    agent_id = _agent(client, auth_headers)
    _deploy(client, auth_headers, tmp_path, agent_id, SEALED, environment="prod")
    _capability(agent_id)
    action_id = _action(agent_id)

    _deploy(client, auth_headers, tmp_path, agent_id, UNSEALED, environment="dev")

    assert _undoable(client, auth_headers, action_id) is True


def test_an_agent_with_no_in_force_version_has_no_custody(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-16: no in-force version declares anything, so custody fails closed."""

    undeployed = _agent(client, auth_headers, name="undeployed-bot")
    _capability(sealed_agent)
    _capability(undeployed)
    control = _action(sealed_agent)
    action_id = _action(undeployed)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, action_id) is False


# --------------------------------------------------------------------------- #
# Restore executions (ACTION-EXECUTOR-11, -17)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "state", ["requested", "claimed", "dispatched", "confirmed", "failed", "indeterminate"]
)
def test_a_restore_that_is_not_refused_makes_the_record_not_undoable(
    client: Any, auth_headers: dict[str, str], sealed_agent: str, state: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-17

    A live restore holds the action; a confirmed one put it back; a failed or
    indeterminate one may have written, so it "still blocks a second undo".
    """

    _capability(sealed_agent)
    control = _action(sealed_agent)
    held = _action(sealed_agent)
    _execution(sealed_agent, held, state=state)

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, held) is False


def test_a_refused_restore_releases_the_record(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-15

    ``refused`` is a provable non-write (a version conflict called only a
    read), so it "releases the action": a later ruling may try again.
    """

    _capability(sealed_agent)
    action_id = _action(sealed_agent)
    _execution(sealed_agent, action_id, state="refused")
    _execution(sealed_agent, action_id, state="refused")

    assert _undoable(client, auth_headers, action_id) is True


def test_the_forward_execution_that_created_a_record_does_not_block_its_undo(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-19

    Only a RESTORE execution holds the record. A forward execution names the
    record it created at dispatch, and "a platform-executed forward action is
    undoable on the same terms" as a model turn's call. (Whether the ruling
    then authorizes it is ACTION-EXECUTOR-19's ``refused_authority_unresolved``,
    not this derivation.)
    """

    _capability(sealed_agent)
    action_id = _action(sealed_agent)
    _execution(sealed_agent, action_id, state="confirmed", kind="forward")

    assert _undoable(client, auth_headers, action_id) is True


# --------------------------------------------------------------------------- #
# The undo route follows the derivation (ACTION-EXECUTOR-11)
# --------------------------------------------------------------------------- #

# What the forward call left, as a pre-sealing record stored it. The ruling is
# sent the same value as its observation, so the only rule that can refuse
# these undos is the derivation of ``undoable``: under the cleartext rule each
# of them would be authorized.
LEFT = {"spec": {"replicas": 10}}


def _audit(client: Any, headers: dict[str, str], action_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/actions/{action_id}/audit", headers=headers)
    assert response.status_code == 200, response.text
    return list(response.json())


def _assert_refused_without_a_grant(
    client: Any, headers: dict[str, str], action_id: str, code: str
) -> None:
    """The ruling refused with ``code``, wrote that refusal, and granted nothing."""

    assert _undoable(client, headers, action_id) is False

    response = client.post(
        f"/actions/{action_id}/undo",
        json={"actor": "U-operator", "observed_state": LEFT},
        headers=headers,
    )

    assert response.status_code in {409, 412, 503}, response.text
    entries = _audit(client, headers, action_id)
    assert [entry["action"] for entry in entries] == [code]
    assert entries[0]["authorized"] is False
    assert not any(entry["authorized"] for entry in entries)
    after = client.get(f"/actions/{action_id}", headers=headers).json()
    assert after["undone_at"] is None
    assert after["undone_by"] is None


def test_the_undo_route_refuses_a_legacy_cleartext_row_as_unsealed(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11: "a legacy cleartext row is refused ``refused_unsealed``".

    Shaped like a row written before this change: cleartext prior and post
    state, no version, connector or digest. The read already calls it not
    undoable; the ruling must agree, refuse it with the first missing
    ingredient's code, and write no granted-undo audit row.
    """

    _capability(sealed_agent)
    legacy = _action(
        sealed_agent,
        prior_state={"spec": {"replicas": 3}},
        post_state=LEFT,
        post_version=None,
        connector=None,
        connector_digest=None,
    )

    _assert_refused_without_a_grant(client, auth_headers, legacy, "refused_unsealed")


def test_the_undo_route_refuses_a_cleartext_row_that_carries_every_other_ingredient(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11: a cleartext ``prior_state`` alone is ``refused_unsealed``."""

    _capability(sealed_agent)
    cleartext = _action(sealed_agent, prior_state={"spec": {"replicas": 3}}, post_state=LEFT)

    _assert_refused_without_a_grant(client, auth_headers, cleartext, "refused_unsealed")


_RULING_CODE_FOR_MISSING_RECORD_INGREDIENT: dict[str, tuple[dict[str, Any], str]] = {
    "no post_version": ({"post_version": None}, "refused_unversioned"),
    "no connector_digest": ({"connector_digest": None}, "refused_no_digest"),
}


@pytest.mark.parametrize(
    ("overrides", "code"),
    list(_RULING_CODE_FOR_MISSING_RECORD_INGREDIENT.values()),
    ids=list(_RULING_CODE_FOR_MISSING_RECORD_INGREDIENT),
)
def test_the_undo_route_refuses_each_missing_record_ingredient_with_its_code(
    client: Any,
    auth_headers: dict[str, str],
    sealed_agent: str,
    overrides: dict[str, Any],
    code: str,
) -> None:
    """@spec ACTION-EXECUTOR-11: "the undo route refuses the same action with that code"."""

    _capability(sealed_agent)
    action_id = _action(sealed_agent, post_state=LEFT, **overrides)

    _assert_refused_without_a_grant(client, auth_headers, action_id, code)


def test_the_undo_route_refuses_a_record_without_a_capability_row(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-13: ``refused_not_restore_capable``."""

    action_id = _action(sealed_agent, post_state=LEFT)

    _assert_refused_without_a_grant(client, auth_headers, action_id, "refused_not_restore_capable")


def test_the_undo_route_refuses_a_record_without_key_custody(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-16: ``refused_key_custody``.

    The in-force version declares no ``SNAPSHOT_SEALING_KEY`` SecretRef on the
    connector, so custody fails at ruling time exactly as it does at read time.
    """

    agent_id = _agent(client, auth_headers, name="uncustodied-bot")
    _deploy(client, auth_headers, tmp_path, agent_id, UNSEALED)
    _capability(agent_id)
    action_id = _action(agent_id, post_state=LEFT)

    _assert_refused_without_a_grant(client, auth_headers, action_id, "refused_key_custody")


def test_the_undo_route_refuses_a_record_without_an_agent(
    client: Any, auth_headers: dict[str, str], sealed_agent: str
) -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-3: ``refused_no_agent``."""

    _capability(sealed_agent)
    orphan = _action(None, post_state=LEFT)

    _assert_refused_without_a_grant(client, auth_headers, orphan, "refused_no_agent")
