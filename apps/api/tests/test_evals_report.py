"""POST /evals/report -> GitHub commit status (GitHub mocked via a fake reporter)."""

from typing import Any

import httpx
from curie_api.deps import get_github_reporter
from curie_api.github_checks import GitHubStatusReporter
from curie_api.main import create_app
from fastapi.testclient import TestClient


class FakeReporter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int, int, str | None]] = []

    async def report_eval(
        self,
        repo_full_name: str,
        sha: str,
        passed_count: int,
        total: int,
        target_url: str | None = None,
    ) -> str:
        self.calls.append((repo_full_name, sha, passed_count, total, target_url))
        return "success" if passed_count == total and total else "failure"


def test_report_posts_status_and_returns_state(
    auth_headers: dict[str, str],
) -> None:
    reporter = FakeReporter()
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: reporter
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/demo",
                "sha": "abc123",
                "passed_count": 34,
                "total": 36,
                "target_url": "https://x/run",
            },
            headers=auth_headers,
        )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"state": "failure", "sha": "abc123"}
    assert reporter.calls == [("octo/demo", "abc123", 34, 36, "https://x/run")]


def test_report_success_state(auth_headers: dict[str, str]) -> None:
    reporter = FakeReporter()
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: reporter
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/demo",
                "sha": "abc",
                "passed_count": 36,
                "total": 36,
            },
            headers=auth_headers,
        )
    assert resp.json()["state"] == "success"


def test_report_tolerates_unknown_field_from_a_newer_producer(
    auth_headers: dict[str, str],
) -> None:
    """The route is a CONSUMER of the wire, so it ignores fields it does not model.

    A newer worker adding an optional field must not 422 against an older API --
    that is what makes a new optional field a patch bump. Exercised through the
    real route, because the strictness this guards lives in FastAPI's own body
    validation, not in the model as constructed by hand.
    """

    reporter = FakeReporter()
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: reporter
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/demo",
                "sha": "abc",
                "passed_count": 36,
                "total": 36,
                "future_field": "from a newer worker",
            },
            headers=auth_headers,
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["state"] == "success"
    # The unknown field is ignored, not smuggled through to the handler.
    assert reporter.calls == [("octo/demo", "abc", 36, 36, None)]


def test_report_still_rejects_a_structurally_invalid_body(
    auth_headers: dict[str, str],
) -> None:
    """Tolerance is for UNKNOWN fields only; a modelled field's type still binds."""

    reporter = FakeReporter()
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: reporter
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/demo",
                "sha": "abc",
                "passed_count": "not-an-int",
                "total": 36,
            },
            headers=auth_headers,
        )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"][0]["loc"] == ["body", "passed_count"]
    assert reporter.calls == []


def test_report_requires_api_key(client: Any) -> None:
    resp = client.post(
        "/evals/report",
        json={"repo_full_name": "o/r", "sha": "s", "passed_count": 1, "total": 1},
    )
    assert resp.status_code == 401


def _reporter_rejecting_with(github_status: int) -> GitHubStatusReporter:
    """A real reporter whose GitHub calls return the given error status."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(github_status, json={"message": "Not Found"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return GitHubStatusReporter(
        client, api_url="https://api.github.com", token="t", context="curie/evals"
    )


def test_report_unknown_repo_returns_404_not_500(
    auth_headers: dict[str, str],
) -> None:
    # An eval report for a repo/commit GitHub does not know about must degrade to
    # a clear 4xx, never a 500 (the audit MAJOR: report_eval 500s on stale/unknown
    # repo state). GitHub answers 404 -> the API answers 404 with a clear detail.
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: _reporter_rejecting_with(404)
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/ghost",
                "sha": "deadbeef",
                "passed_count": 1,
                "total": 1,
            },
            headers=auth_headers,
        )
    assert resp.status_code == 404, resp.text
    assert "octo/ghost" in resp.json()["detail"]


def test_report_github_client_rejection_maps_to_422(
    auth_headers: dict[str, str],
) -> None:
    # A non-404 GitHub client rejection (e.g. 422 bad sha, 403 token) is still a
    # caller/input fault, not an API server fault: map to 422, not 500.
    app = create_app()
    app.dependency_overrides[get_github_reporter] = lambda: _reporter_rejecting_with(422)
    with TestClient(app) as client:
        resp = client.post(
            "/evals/report",
            json={
                "repo_full_name": "octo/demo",
                "sha": "bad",
                "passed_count": 1,
                "total": 1,
            },
            headers=auth_headers,
        )
    assert resp.status_code == 422, resp.text
