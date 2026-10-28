"""Configured publication labels from chart rendering to GitHub and real Valkey."""

from __future__ import annotations

import base64
import json
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
import yaml
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from forge_fakes.github import GitHubRepositoryFake
from sqlalchemy import text

from apps.api.tests.test_workitem_outcomes import (  # noqa: F401 - real stack fixture
    BASE_SHA,
    HEAD_SHA,
    REPO,
    WORKER_HEADERS,
    _agent,
    _completed,
    _publish,
    _resolve,
    stack,
    with_session,
)

CHART = Path(__file__).resolve().parents[3] / "charts" / "curie"
ENV_NAME = "CURIE_FACTORY_PUBLICATION_LABELS"
LABELS = {REPO: ["SMOKE"]}
_PUBLICATION_LABEL_VECTORS = json.loads(
    (CHART.parents[1] / "tests/vectors/factory-publication-labels.json").read_text()
)


def _helm(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", "template", "publication-labels", str(CHART), *arguments],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def _render(*arguments: str) -> list[dict[str, Any]]:
    result = _helm(*arguments)
    assert result.returncode == 0, result.stderr
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _label_environment(documents: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    found: list[tuple[str, str, str]] = []

    def visit(node: Any, component: str) -> None:
        if isinstance(node, dict):
            for container in node.get("containers", []) + node.get("initContainers", []):
                for entry in container.get("env", []):
                    if entry.get("name") == ENV_NAME:
                        found.append((component, container["name"], entry["value"]))
            for child in node.values():
                visit(child, component)
        elif isinstance(node, list):
            for child in node:
                visit(child, component)

    for document in documents:
        component = document.get("metadata", {}).get("labels", {}).get(
            "app.kubernetes.io/component", ""
        )
        visit(document, component)
    return found


def test_publication_labels_chart_environment_is_conditional_and_api_only() -> None:
    configured = _render("--set-json", f"api.factoryPublicationLabels={json.dumps(LABELS)}")

    [(component, container, value)] = _label_environment(configured)
    assert component == "api"
    assert container == "api"
    assert json.loads(value) == LABELS
    assert _label_environment(_render()) == []


@pytest.mark.parametrize("vector", _PUBLICATION_LABEL_VECTORS, ids=lambda vector: vector["name"])
def test_publication_labels_chart_schema_shared_vectors(vector: dict[str, Any]) -> None:
    result = _helm("--set-json", f"api.factoryPublicationLabels={json.dumps(vector['value'])}")

    if vector["valid"]:
        assert result.returncode == 0, result.stderr
        documents = [document for document in yaml.safe_load_all(result.stdout) if document]
        entries = _label_environment(documents)
        if vector["value"]:
            [(_component, _container, value)] = entries
            assert json.loads(value) == vector["value"]
        else:
            assert entries == []
    else:
        assert result.returncode != 0
        assert "schema" in result.stderr.lower()
        if isinstance(vector["value"], dict):
            assert vector["key"] in result.stderr


def test_publication_labels_chart_reserves_its_environment_variable() -> None:
    extra = [{"name": ENV_NAME, "value": "{}"}]

    result = _helm("--set-json", f"api.extraEnv={json.dumps(extra)}")

    assert result.returncode != 0
    assert ENV_NAME in result.stderr
    assert "api.factoryPublicationLabels" in result.stderr


@pytest.fixture
def rendered_label_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    documents = _render("--set-json", f"api.factoryPublicationLabels={json.dumps(LABELS)}")
    [(_component, _container, value)] = _label_environment(documents)
    monkeypatch.setenv(ENV_NAME, value)
    monkeypatch.setenv("GITHUB_APP_ID", "")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "")
    monkeypatch.setenv("CURIE_FORGES", "{}")
    get_settings.cache_clear()
    try:
        assert get_settings().factory_publication_labels == LABELS
        yield
    finally:
        get_settings.cache_clear()


@pytest.fixture
def publication_label_stack(
    rendered_label_settings: None,
    stack: TestClient,  # noqa: F811
) -> TestClient:
    return stack


def test_publication_label_scenario(
    publication_label_stack: TestClient, auth_headers: dict[str, str]
) -> None:
    client = publication_label_stack
    agent = _agent(client, auth_headers)
    _completed(client, agent)
    publication = _publish(client, agent["deployment_id"])
    _resolve(client, auth_headers, publication["approval_id"])
    branch = with_session(
        lambda session: session.scalar(
            text("SELECT branch FROM curie.thread_publication_lineages WHERE id = :id"),
            {"id": uuid.UUID(publication["lineage_id"])},
        )
    )
    assert isinstance(branch, str)
    github = GitHubRepositoryFake()
    github.branches.update({"main": BASE_SHA, branch: HEAD_SHA})
    github.commits.update(
        {
            BASE_SHA: {"sha": BASE_SHA, "message": "Base", "parents": []},
            HEAD_SHA: {
                "sha": HEAD_SHA,
                "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
                "parents": [{"sha": BASE_SHA}],
            },
        }
    )
    valkey = redis.Redis.from_url(get_settings().valkey_dsn(), decode_responses=True)
    valkey.ping()
    marker = f"curie:publication-labels:{publication['id']}"
    external = httpx.AsyncClient(transport=httpx.MockTransport(github.handle))
    original = client.app.state.http_client
    client.app.state.http_client = external
    url = f"/v1/internal/publications/{publication['id']}/pull-request"
    body = {"expected_head_sha": HEAD_SHA, "adopt_only": False}
    try:
        unauthenticated = client.post(url, json=body)
        assert unauthenticated.status_code == 401
        assert github.label_requests == []

        first = client.post(url, json=body, headers=WORKER_HEADERS)

        assert first.status_code == 200, first.text
        [request] = github.label_requests
        number = first.json()["number"]
        assert request.method == "POST"
        assert request.url.path == f"/repos/{REPO}/issues/{number}/labels"
        expected_header = "Basic " + base64.b64encode(
            f"x-access-token:{get_settings().github_token}".encode()
        ).decode()
        assert request.headers["Authorization"] == expected_header
        assert json.loads(request.content) == {"labels": ["SMOKE"]}
        assert github.issues[number]["labels"] == [{"name": "SMOKE"}]
        assert valkey.exists(marker) == 1
        assert 0 < valkey.ttl(marker) <= 30 * 24 * 60 * 60
        github.issues[number]["labels"] = []

        repeated = client.post(url, json=body, headers=WORKER_HEADERS)

        assert repeated.status_code == 200, repeated.text
        assert len(github.label_requests) == 1
        assert github.issues[number]["labels"] == []
    finally:
        client.app.state.http_client = original
        client.portal.call(external.aclose)
        valkey.delete(marker)
        assert valkey.exists(marker) == 0
        valkey.close()
