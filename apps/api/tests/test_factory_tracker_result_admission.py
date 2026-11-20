"""A tracker result declaration deploys and keeps its digest and policy (ADR 0277, #5586).

A version whose bundle carries a valid ``workflow/result.json`` deploys
through the bundle upload route and keeps its declaration digest and policy on
the version row.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import hashlib
import io
import json
import sys
import tarfile
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)

MANIFEST = '{"name": "triage-plugin", "version": "0.1.0"}'
SKILL = "---\nname: triage\ndescription: investigates one issue\n---\n\n# triage\n"
FINDINGS_ONLY: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["findings"],
}
WITH_TICKET: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["ticket", "findings"],
    "implementation_label": "curie-implement",
}


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _digest(declaration: dict[str, Any]) -> str:
    """The canonical JSON digest, derived here independently of the parser."""

    canonical = json.dumps(declaration, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _bundle(declaration: dict[str, Any] | None) -> bytes:
    files = {".claude-plugin/plugin.json": MANIFEST, "skills/triage/SKILL.md": SKILL}
    if declaration is not None:
        files["workflow/result.json"] = json.dumps(declaration)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for rel, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"triage-plugin/{rel}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _agent_id() -> str:
    (agent,) = _rows("SELECT id FROM curie.agents")
    return str(agent["id"])


def _set_publication_policy(client: Any, agent_id: str, policy: str) -> None:
    resp = client.patch(
        f"/agents/{agent_id}", json={"publication_policy": policy}, headers=_headers()
    )
    assert resp.status_code == 200, resp.text


def _upload(client: Any, agent_id: str, declaration: dict[str, Any] | None) -> tuple[str, Any]:
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "triage-test"},
        headers=_headers(),
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("triage.tar.gz", _bundle(declaration))},
        headers=_headers(),
    )
    return version_id, resp


def _deployed(client: Any, agent_id: str, declaration: dict[str, Any] | None) -> str:
    version_id, uploaded = _upload(client, agent_id, declaration)
    assert uploaded.status_code == 201, uploaded.text
    deployed = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=_headers(),
    )
    assert deployed.status_code == 201, deployed.text
    return version_id


def _versions(agent_id: str) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, result_policy_schema_version, result_policy_digest, result_policy "
        "FROM curie.agent_versions WHERE agent_id = :agent",
        {"agent": uuid.UUID(agent_id)},
    )


# Deploy --------------------------------------------------------------------------


@pytest.mark.parametrize("declaration", [FINDINGS_ONLY, WITH_TICKET], ids=["findings", "ticket"])
def test_a_declared_version_deploys_and_keeps_its_digest_and_policy(
    admitted: Any, declaration: dict[str, Any]
) -> None:
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")

    version_id = _deployed(client, agent_id, declaration)

    assert _versions(agent_id) == [
        {
            "id": uuid.UUID(version_id),
            "result_policy_schema_version": 1,
            "result_policy_digest": _digest(declaration),
            "result_policy": {
                "results": sorted(declaration["results"]),
                "implementation_label": declaration.get("implementation_label"),
            },
        }
    ]
