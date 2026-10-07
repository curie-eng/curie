"""@spec ACTION-EXECUTOR-16: sealing key custody on the paths review round 1 found open.

ADR 0124 decision 1 requires the snapshot sealing key to reach only the hosted
connector. ``test_sealing_key_intake.py`` pins the four connector forms, the
``plugin.json`` list and the agent ``secrets`` map. These tests pin what that
file left open:

* M1: a refusal never echoes the submitted value. FastAPI's default 422 body
  carries pydantic's ``input``, which for the agent ``secrets`` map is the whole
  submitted map, so the key's value would land in every client or proxy log
  that prints error bodies.
* L1: a connector can name the key without declaring it as a secret: through
  ``bearer_secret`` (hosted or remote), through a ``${NAME}`` placeholder in a
  remote connector's ``headers``, and through a ``${NAME}`` placeholder in a
  hosted connector's ``unhosted_url``. Each expands from the sandbox
  environment, so each declares that the sandbox holds and presents the key.
* L2: a ``SecretRef`` on a remote (``url:``) connector reaches no pod, so it is
  not custody: the spec's "reaches only the hosted connector".
* L3: a bundle stored before intake refused the key, declaring it in a
  non-SecretRef form, is refused when it is deployed again, not only when it is
  first uploaded.

Distinctive placeholder values throughout; none is a real credential.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import random
import string
import tarfile
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_rows
from _sealed_actions import (
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
)
from curie_api.config import get_settings
from curie_api.storage import BundleStore

pytestmark = pytest.mark.usefixtures("clean_db")

RESERVED = ("SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED")
DIGEST = "sha256:" + "ab" * 32
IMAGE = f"ghcr.io/example/k8s-restorer@{DIGEST}"
REMOTE_URL = "https://mcp.example.com/mcp"

# Placeholder secret values, distinctive enough that any echo is unambiguous.
SEAL_VALUE = "SEALVALUE-placeholder-7f3c9e1b"
SIBLING_VALUE = "SIBLINGVALUE-placeholder-2d8a"


def _address() -> str:
    return "CSEAL" + "".join(random.choices(string.ascii_uppercase, k=6))


def _archive(tmp_path: Path, connectors: str | None, *, name: str = "sealer") -> bytes:
    root = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "skills" / name).mkdir(parents=True)
    (root / "skills" / name / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    if connectors is not None:
        (root / "connectors.yaml").write_text(connectors, encoding="utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(root, arcname=name)
    return buf.getvalue()


def _agent(client: Any, headers: dict[str, str], **extra: Any) -> Any:
    return client.post(
        "/agents",
        json={
            "name": f"seal-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": _address()},
            **extra,
        },
        headers=headers,
    )


def _new_version(client: Any, headers: dict[str, str]) -> tuple[str, str]:
    agent = _agent(client, headers)
    assert agent.status_code == 201, agent.text
    agent_id = str(agent.json()["id"])
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "test"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    return agent_id, str(version.json()["id"])


def _upload(client: Any, headers: dict[str, str], archive: bytes) -> Any:
    agent_id, version_id = _new_version(client, headers)
    return client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bundle.tar.gz", archive)},
        headers=headers,
    )


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for k, v in value.items() for s in [str(k), *_strings(v)]]
    if isinstance(value, list):
        return [s for v in value for s in _strings(v)]
    return []


def _assert_custody_refusal(response: Any, name: str) -> None:
    """Refused, with one reason that names the key and the SecretRef rule."""

    assert response.status_code == 422, (
        f"{name} was accepted ({response.status_code}): {response.text}"
    )
    reasons = _strings(response.json())
    assert any(name in r and "SecretRef" in r for r in reasons), (
        f"{name} was refused without a reason naming it and that it must be a "
        f"SecretRef: {reasons}"
    )


def _store_directly(agent_id: str, version_id: str, data: bytes) -> None:
    """Attach ``data`` to the version without passing intake.

    Simulates a bundle stored before the custody check existed: written straight
    to the object store under a fresh key (keys are write-once) and pointed at
    by the version row.
    """

    async def _run() -> str:
        store = BundleStore(get_settings())
        key = f"bundles/{agent_id}/{version_id}-legacy-{uuid.uuid4().hex[:6]}.tar.gz"
        await store.put(key, data, "application/gzip")
        return key

    key = asyncio.run(_run())
    sql_rows(
        "UPDATE curie.agent_versions SET bundle_ref = :ref, bundle_sha256 = :sha "
        "WHERE id = :id",
        {"ref": key, "sha": hashlib.sha256(data).hexdigest(), "id": uuid.UUID(version_id)},
    )


def _deployments(client: Any, headers: dict[str, str], agent_id: str) -> list[dict[str, Any]]:
    response = client.get("/deployments", params={"agent_id": agent_id}, headers=headers)
    assert response.status_code == 200, response.text
    return list(response.json())


# --------------------------------------------------------------------------- #
# M1: a refusal does not echo the submitted value
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", RESERVED)
def test_a_refused_agent_create_does_not_echo_the_sealing_key_value(
    client: Any, auth_headers: dict[str, str], name: str
) -> None:
    """@spec ACTION-EXECUTOR-16: the value appears nowhere but the hosted connector.

    The refusal itself must still hold, so the absence of the value is not
    bought by accepting the request.
    """

    response = _agent(
        client,
        auth_headers,
        secrets={name: SEAL_VALUE, "GITHUB_PERSONAL_ACCESS_TOKEN": SIBLING_VALUE},
    )

    _assert_custody_refusal(response, name)
    assert SEAL_VALUE not in response.text, f"the 422 body echoes the {name} value"
    assert SIBLING_VALUE not in response.text, (
        "the 422 body echoes another secret value submitted in the same request"
    )


@pytest.mark.parametrize("name", RESERVED)
def test_a_refused_agent_patch_does_not_echo_the_sealing_key_value(
    client: Any, auth_headers: dict[str, str], name: str
) -> None:
    """@spec ACTION-EXECUTOR-16"""

    agent_id, _ = _new_version(client, auth_headers)
    response = client.patch(
        f"/agents/{agent_id}",
        json={"secrets": {name: SEAL_VALUE, "GITHUB_PERSONAL_ACCESS_TOKEN": SIBLING_VALUE}},
        headers=auth_headers,
    )

    _assert_custody_refusal(response, name)
    assert SEAL_VALUE not in response.text, f"the 422 body echoes the {name} value"
    assert SIBLING_VALUE not in response.text, (
        "the 422 body echoes another secret value submitted in the same request"
    )


@pytest.mark.parametrize("name", RESERVED)
@pytest.mark.parametrize("form", ["env", "sealed_secrets"])
def test_a_refused_bundle_upload_does_not_echo_the_carried_value(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, form: str, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16: the two connector forms that carry a value in the bundle."""

    connectors = (
        f"connectors:\n  k8s:\n    image: {IMAGE}\n    {form}:\n      {name}: {SEAL_VALUE}\n"
    )
    response = _upload(client, auth_headers, _archive(tmp_path, connectors))

    _assert_custody_refusal(response, name)
    assert SEAL_VALUE not in response.text, f"the 422 body echoes the {name} value"


# --------------------------------------------------------------------------- #
# L1: forms that name the key without declaring it as a secret
# --------------------------------------------------------------------------- #


def _naming_form(form: str, name: str) -> str:
    if form == "hosted_bearer_secret":
        # Accepted by validate_connectors because the SecretRef declares the
        # name; the derived Authorization header then expands it in the sandbox.
        return (
            f"connectors:\n  k8s:\n    image: {IMAGE}\n"
            f"    secrets:\n      - name: {name}\n        from_secret: k8s-restorer-seal\n"
            f"    bearer_secret: {name}\n"
        )
    if form == "remote_bearer_secret":
        return f"connectors:\n  k8s:\n    url: {REMOTE_URL}\n    bearer_secret: {name}\n"
    if form == "remote_headers":
        return (
            f"connectors:\n  k8s:\n    url: {REMOTE_URL}\n"
            f'    headers:\n      Authorization: "Bearer ${{{name}}}"\n'
        )
    if form == "remote_headers_other_header":
        return (
            f"connectors:\n  k8s:\n    url: {REMOTE_URL}\n"
            f'    headers:\n      X-Seal: "${{{name}}}"\n'
        )
    if form == "unhosted_url":
        return (
            f"connectors:\n  k8s:\n    image: {IMAGE}\n"
            f'    unhosted_url: "http://localhost:8765/mcp?key=${{{name}}}"\n'
        )
    raise AssertionError(form)


_NAMING_FORMS = [
    "hosted_bearer_secret",
    "remote_bearer_secret",
    "remote_headers",
    "remote_headers_other_header",
    "unhosted_url",
]


@pytest.mark.parametrize("name", RESERVED)
@pytest.mark.parametrize("form", _NAMING_FORMS)
def test_a_connector_that_names_the_key_for_the_sandbox_to_expand_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, form: str, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16: intake refuses either name in any form but a SecretRef.

    ``bearer_secret``, a remote ``headers`` placeholder and an ``unhosted_url``
    placeholder are all expanded from the sandbox environment by the MCP client,
    so each is a declaration that the sandbox holds the key.
    """

    response = _upload(client, auth_headers, _archive(tmp_path, _naming_form(form, name)))
    _assert_custody_refusal(response, name)


@pytest.mark.parametrize("form", _NAMING_FORMS)
def test_the_same_forms_with_an_unreserved_name_are_accepted(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, form: str
) -> None:
    """@spec ACTION-EXECUTOR-16: the paired control; the forms themselves are valid."""

    response = _upload(client, auth_headers, _archive(tmp_path, _naming_form(form, "MY_SEAL_KEY")))
    assert response.status_code == 201, response.text


# --------------------------------------------------------------------------- #
# L2: a SecretRef on a remote connector is not custody
# --------------------------------------------------------------------------- #

ENVELOPE = {
    "sealed": "curie.snapshot.v1",
    "kid": "seal-2026-10",
    "ciphertext": base64.b64encode(b"opaque sealed prior state \x00\x01\x02").decode(),
}
TARGET = {"kind": "Deployment", "namespace": "public", "name": "api"}

HOSTED_SEALED = (
    f"connectors:\n  k8s:\n    image: {IMAGE}\n"
    "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n        from_secret: k8s-restorer-seal\n"
)
REMOTE_SEALED = (
    f"connectors:\n  k8s:\n    url: {REMOTE_URL}\n"
    "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n        from_secret: k8s-restorer-seal\n"
)


def _seed_in_force(client: Any, headers: dict[str, str], tmp_path: Path, connectors: str) -> str:
    """An agent whose active deployment is a stored bundle that bypassed intake.

    The deployment row is written directly too, so the derivation is tested on
    its own: whatever intake or deploy later refuses, a version already in
    force must not be granted custody it does not have.
    """

    agent_id, version_id = _new_version(client, headers)
    _store_directly(agent_id, version_id, _archive(tmp_path, connectors))
    sql_rows(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, workspace_enabled, status, deployed_at) "
        "VALUES (:id, :agent_id, :version_id, 'dev', false, 'active', now())",
        {
            "id": uuid.uuid4(),
            "agent_id": uuid.UUID(agent_id),
            "version_id": uuid.UUID(version_id),
        },
    )
    sql_rows(
        "INSERT INTO curie.connector_capabilities "
        "(agent_id, connector, digest, restore_capable, observed_at) "
        "VALUES (:agent_id, 'k8s', :digest, true, now())",
        {"agent_id": uuid.UUID(agent_id), "digest": DIGEST},
    )
    return agent_id


def _sealed_action(agent_id: str) -> str:
    action_id = uuid.uuid4()
    call_id = f"toolu_{uuid.uuid4().hex[:10]}"
    sql_rows(
        "INSERT INTO curie.agent_actions "
        "(id, agent_id, conversation_id, call_id, tool, arguments, result, prior_state, "
        "post_state, target, status, dedupe_key, completed_at, post_version, connector, "
        "connector_digest) "
        "VALUES (:id, :agent_id, 'C1', :call_id, 'mcp__k8s__scale', "
        "CAST(:arguments AS jsonb), CAST(:result AS jsonb), CAST(:prior AS jsonb), "
        "NULL, CAST(:target AS jsonb), 'succeeded', :key, now(), 'rv-1042', 'k8s', :digest)",
        {
            "id": action_id,
            "agent_id": uuid.UUID(agent_id),
            "call_id": call_id,
            "arguments": json.dumps({"name": "api", "replicas": 10}),
            "result": json.dumps({"ok": True, "version": "rv-1042"}),
            "prior": json.dumps(ENVELOPE),
            "target": json.dumps(TARGET),
            "key": f"event-{uuid.uuid4()}:{call_id}",
            "digest": DIGEST,
        },
    )
    return str(action_id)


def _undoable(client: Any, headers: dict[str, str], action_id: str) -> bool:
    response = client.get(f"/actions/{action_id}", headers=headers)
    assert response.status_code == 200, response.text
    value = response.json()["undoable"]
    assert isinstance(value, bool)
    return value


def test_a_secret_ref_on_a_remote_connector_is_not_custody(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-16: the key must reach "only the hosted connector".

    A remote connector runs no pod Curie owns, so a ``SecretRef`` on it reaches
    nothing and grants nothing. The record carries every other ingredient
    (digest and capability row included), and an identical hosted control is
    undoable, so the remote record's ``False`` is about custody alone.
    """

    control = _sealed_action(_seed_in_force(client, auth_headers, tmp_path, HOSTED_SEALED))
    remote = _sealed_action(_seed_in_force(client, auth_headers, tmp_path, REMOTE_SEALED))

    assert _undoable(client, auth_headers, control) is True
    assert _undoable(client, auth_headers, remote) is False, (
        "a SNAPSHOT_SEALING_KEY SecretRef on a url: connector was counted as custody"
    )


@pytest.mark.usefixtures("executor_enabled")
def test_the_undo_route_refuses_a_remote_secret_ref_with_the_custody_code(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-16 @spec ACTION-EXECUTOR-11: ``refused_key_custody``."""

    action_id = _sealed_action(_seed_in_force(client, auth_headers, tmp_path, REMOTE_SEALED))

    response = client.post(
        f"/actions/{action_id}/undo",
        json={"observed_state": {"spec": {"replicas": 10}}},
        headers=operator_headers(),
    )

    assert response.status_code in {409, 412, 503}, response.text
    audit = client.get(f"/actions/{action_id}/audit", headers=auth_headers)
    assert audit.status_code == 200, audit.text
    entries = audit.json()
    assert [entry["action"] for entry in entries] == ["refused_key_custody"], entries
    assert not any(entry["authorized"] for entry in entries)


# --------------------------------------------------------------------------- #
# L3: a stored bundle is re-checked when it is deployed again
# --------------------------------------------------------------------------- #


def _plain_form(form: str, name: str) -> str:
    head = f"connectors:\n  k8s:\n    image: {IMAGE}\n"
    if form == "plain":
        return head + f"    secrets:\n      - {name}\n"
    if form == "secret_files":
        return head + f"    secret_files:\n      {name}: /secrets/seal\n"
    raise AssertionError(form)


@pytest.mark.parametrize("name", RESERVED)
@pytest.mark.parametrize("form", ["plain", "secret_files"])
def test_deploying_a_stored_bundle_that_declares_the_key_plainly_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, form: str, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16: custody holds on every path, not only first upload.

    A version stored before intake refused the key can still be deployed,
    redeployed or rolled back to; at the cluster tier the deploy path would
    then resolve the plain name into the per-agent connector Secret.
    """

    agent_id, version_id = _new_version(client, auth_headers)
    _store_directly(agent_id, version_id, _archive(tmp_path, _plain_form(form, name)))

    response = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=auth_headers,
    )

    _assert_custody_refusal(response, name)
    assert _deployments(client, auth_headers, agent_id) == []


def test_deploying_a_stored_bundle_with_the_secret_ref_is_accepted(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-16: the paired control; direct seeding itself deploys."""

    agent_id, version_id = _new_version(client, auth_headers)
    _store_directly(agent_id, version_id, _archive(tmp_path, HOSTED_SEALED))

    response = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=auth_headers,
    )

    assert response.status_code == 201, response.text
