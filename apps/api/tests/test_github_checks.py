"""PR-check reporter: exact GitHub commit-status payload (GitHub mocked)."""

import asyncio
import json
from typing import Any

import httpx
import pytest
from curie_api.github_checks import (
    GitHubReportError,
    GitHubStatusReporter,
    eval_state,
)


def test_eval_state_maps_rollup_to_status() -> None:
    assert eval_state(36, 36) == ("success", "36/36 passed")
    assert eval_state(34, 36) == ("failure", "34/36 passed")
    assert eval_state(0, 0) == ("failure", "0/0 passed")


def _run_report(passed: int, total: int) -> tuple[httpx.Request, str]:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json={})

    async def go() -> tuple[httpx.Request, str]:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            reporter = GitHubStatusReporter(
                client,
                api_url="https://api.github.com",
                token="tok-123",
                context="curie/evals",
            )
            state = await reporter.report_eval(
                "octo/demo", "abc123def", passed, total, target_url="https://x/run"
            )
        return captured[0], state

    return asyncio.run(go())


def test_report_eval_posts_the_exact_commit_status() -> None:
    request, state = _run_report(34, 36)
    assert state == "failure"
    assert str(request.url) == ("https://api.github.com/repos/octo/demo/statuses/abc123def")
    assert request.method == "POST"
    body: dict[str, Any] = json.loads(request.content)
    assert body == {
        "state": "failure",
        "context": "curie/evals",
        "description": "34/36 passed",
        "target_url": "https://x/run",
    }
    assert request.headers["Authorization"] == "Bearer tok-123"
    assert request.headers["Accept"] == "application/vnd.github+json"


def test_report_eval_success_state() -> None:
    _, state = _run_report(36, 36)
    assert state == "success"


def test_report_eval_skips_post_when_no_token() -> None:
    # With no GitHub token (local/dev, or a deploy without a GitHub App) there is
    # nothing to post a commit status to. The reporter must skip the network call
    # entirely and return the computed state, rather than sending an empty
    # "Authorization: Bearer " header that httpx rejects (LocalProtocolError),
    # which would 500 an otherwise successful eval report.
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("no GitHub request should be made without a token")

    async def go() -> str:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            reporter = GitHubStatusReporter(
                client, api_url="https://api.github.com", token="", context="c"
            )
            return await reporter.report_eval("octo/demo", "abc123", 7, 7)

    assert asyncio.run(go()) == "success"


def test_report_eval_raises_typed_error_on_github_rejection() -> None:
    # An unknown repo/commit (or a bad token) makes GitHub reject the post. The
    # reporter must surface that as a typed GitHubReportError carrying the
    # upstream status, not let the raw httpx.HTTPStatusError bubble as a 500.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "Not Found"})

    async def go() -> GitHubReportError:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            reporter = GitHubStatusReporter(
                client,
                api_url="https://api.github.com",
                token="tok-123",
                context="curie/evals",
            )
            with pytest.raises(GitHubReportError) as excinfo:
                await reporter.report_eval("octo/ghost", "nope", 1, 1)
            return excinfo.value

    err = asyncio.run(go())
    assert err.status_code == 404
    assert "Not Found" in err.detail
