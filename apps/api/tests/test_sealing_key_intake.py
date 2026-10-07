"""@spec ACTION-EXECUTOR-16: API intake refuses the sealing key in any form but a SecretRef.

ADR 0124 decision 1 requires the snapshot sealing key to reach only the hosted
connector. The first release recognizes it by two reserved names,
``SNAPSHOT_SEALING_KEY`` and ``SNAPSHOT_SEALING_KEYS_RETAINED`` (retired keys
kept while their records are undoable), and the API refuses either name at
intake in every form other than a ``SecretRef`` (``{name, from_secret}``),
because every other form either hands the value to the deploy path or places it
where a runner sandbox can read it:

* a bundle's ``connectors.yaml``:
  - a plain-string ``secrets`` entry (Curie resolves and owns the value),
  - a literal ``env`` value,
  - a ``secret_files`` entry (same per-agent Secret, mounted as a file),
  - a ``sealed_secrets`` blob (the bundle carries the value);
* a bundle's ``plugin.json`` ``secrets`` list (named sandbox secrets);
* the agent ``secrets`` map on ``POST /agents`` and ``PATCH /agents/{id}``,
  whose values the worker forwards into the sandbox env.

Each refusal states the reason: it names the reserved name and says it must be
declared as a ``SecretRef``. The ``sealed_secrets`` form is already refused for
an unrelated reason (nothing decrypts it yet); that refusal does not name the
key or the SecretRef rule, so the test pins the custody reason specifically.

Negatives: the ``SecretRef`` form of both names is accepted, and an unreserved
name such as ``MY_SEAL_KEY`` in the plain forms is still accepted at intake --
custody refuses it later (``refused_key_custody``, ACTION-EXECUTOR-11), which
``test_action_undoable_ingredients.py`` covers.

Every case goes through the real HTTP route (``PUT
/agents/{id}/versions/{vid}/bundle``, ``POST /agents``, ``PATCH
/agents/{id}``) against the disposable real Postgres the conftest provisions.
"""

from __future__ import annotations

import io
import json
import random
import string
import tarfile
import uuid
from pathlib import Path
from typing import Any

import pytest

RESERVED = ("SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED")
IMAGE = "ghcr.io/example/k8s-restorer@sha256:" + "ab" * 32


def _address() -> str:
    # Letters only after the kind letter, so no placeholder resembles a real
    # Slack id (no digit in second position).
    return "CSEAL" + "".join(random.choices(string.ascii_uppercase, k=6))


def _connectors_yaml(form: str, name: str) -> str:
    """One hosted ``k8s`` connector declaring ``name`` in ``form``."""

    head = f"connectors:\n  k8s:\n    image: {IMAGE}\n"
    if form == "plain":
        return head + f"    secrets:\n      - {name}\n"
    if form == "env":
        return head + f"    env:\n      {name}: plain-seal-value\n"
    if form == "secret_files":
        return head + f"    secret_files:\n      {name}: /secrets/seal\n"
    if form == "sealed_secrets":
        return head + f"    sealed_secrets:\n      {name}: AgBv3n2Kexample\n"
    if form == "secret_ref":
        return (
            head
            + f"    secrets:\n      - name: {name}\n        from_secret: k8s-restorer-seal\n"
        )
    raise AssertionError(form)


def _archive(
    tmp_path: Path,
    name: str,
    *,
    connectors: str | None = None,
    manifest_secrets: list[str] | None = None,
) -> bytes:
    root = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    manifest: dict[str, Any] = {"name": name, "version": "0.1.0", "description": "t"}
    if manifest_secrets is not None:
        manifest["secrets"] = manifest_secrets
    (root / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
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


def _new_version(client: Any, headers: dict[str, str]) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={
            "name": f"seal-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": _address()},
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
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in _strings(v)]
    return []


def _assert_custody_refusal(response: Any, name: str) -> None:
    """Refused, with one reason that names the key and the SecretRef rule."""

    assert response.status_code == 422, (
        f"{name} was accepted at intake ({response.status_code}): {response.text}"
    )
    reasons = _strings(response.json())
    assert any(name in r and "SecretRef" in r for r in reasons), (
        f"{name} was refused without a reason naming it and that it must be a "
        f"SecretRef: {reasons}"
    )


# --- bundle intake: every non-SecretRef form is refused ---------------------


@pytest.mark.parametrize("name", RESERVED)
@pytest.mark.parametrize("form", ["plain", "env", "secret_files", "sealed_secrets"])
def test_bundle_connector_form_other_than_secret_ref_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path, form: str, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16"""
    archive = _archive(tmp_path, "sealer", connectors=_connectors_yaml(form, name))
    _assert_custody_refusal(_upload(client, auth_headers, archive), name)


@pytest.mark.parametrize("name", RESERVED)
def test_bundle_manifest_secret_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16: ``plugin.json`` ``secrets`` names sandbox secrets."""
    archive = _archive(tmp_path, "sealer", manifest_secrets=[name])
    _assert_custody_refusal(_upload(client, auth_headers, archive), name)


# --- agent secrets map: values the worker forwards to the sandbox -----------


@pytest.mark.parametrize("name", RESERVED)
def test_agent_create_secret_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16"""
    response = client.post(
        "/agents",
        json={
            "name": f"seal-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": _address()},
            "secrets": {name: "plain-seal-value"},
        },
        headers=auth_headers,
    )
    _assert_custody_refusal(response, name)


@pytest.mark.parametrize("name", RESERVED)
def test_agent_patch_secret_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16"""
    agent_id, _ = _new_version(client, auth_headers)
    response = client.patch(
        f"/agents/{agent_id}",
        json={"secrets": {name: "plain-seal-value"}},
        headers=auth_headers,
    )
    _assert_custody_refusal(response, name)
    # Nothing was stored under the name.
    read = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert read.status_code == 200, read.text
    assert name not in (read.json().get("secrets") or [])


# --- negatives: SecretRef accepted; an unreserved name is not refused here ---


def test_secret_ref_of_both_names_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-16: the one accepted form."""
    connectors = (
        f"connectors:\n  k8s:\n    image: {IMAGE}\n    secrets:\n"
        "      - name: SNAPSHOT_SEALING_KEY\n        from_secret: k8s-restorer-seal\n"
        "      - name: SNAPSHOT_SEALING_KEYS_RETAINED\n"
        "        from_secret: k8s-restorer-seal-retained\n"
    )
    response = _upload(client, auth_headers, _archive(tmp_path, "sealer", connectors=connectors))
    assert response.status_code == 201, response.text


@pytest.mark.parametrize("name", RESERVED)
def test_secret_ref_of_each_name_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path, name: str
) -> None:
    """@spec ACTION-EXECUTOR-16"""
    archive = _archive(tmp_path, "sealer", connectors=_connectors_yaml("secret_ref", name))
    response = _upload(client, auth_headers, archive)
    assert response.status_code == 201, response.text


@pytest.mark.parametrize("form", ["plain", "env", "secret_files"])
def test_unreserved_seal_key_name_is_accepted_at_intake(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path, form: str
) -> None:
    """@spec ACTION-EXECUTOR-16: ``MY_SEAL_KEY`` is not reserved; it fails custody later."""
    archive = _archive(tmp_path, "sealer", connectors=_connectors_yaml(form, "MY_SEAL_KEY"))
    response = _upload(client, auth_headers, archive)
    assert response.status_code == 201, response.text


def test_unreserved_seal_key_name_is_accepted_in_agent_secrets(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """@spec ACTION-EXECUTOR-16"""
    agent_id, _ = _new_version(client, auth_headers)
    response = client.patch(
        f"/agents/{agent_id}",
        json={"secrets": {"MY_SEAL_KEY": "plain-seal-value"}},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert "MY_SEAL_KEY" in response.json()["secrets"]
