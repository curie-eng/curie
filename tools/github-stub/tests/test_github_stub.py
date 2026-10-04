"""Exercise the GitHub stand-in over trusted TLS and real Git smart HTTP.

REST shapes follow https://docs.github.com/en/rest and the captured check
lifecycle in recordings/curie-pr-3400.json. Git runs the installed client.
"""

from __future__ import annotations

import importlib.util
import json
import os
import ssl
import subprocess
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import Settings
from curie_api.github_app import GitHubCredentials

STUB_ROOT = Path(__file__).resolve().parents[1]
REPO = "acme-corp/acme-bot"
RECORDING = STUB_ROOT / "recordings" / "curie-pr-3400.json"
PYTHON_CHECK = "Python (ruff + mypy + pytest)"


def _stub_type() -> Any:
    spec = importlib.util.spec_from_file_location("curie_github_stub", STUB_ROOT / "github_stub.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.GithubStub


def _app_key() -> str:
    return (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )


@pytest.fixture
def recording() -> dict[str, Any]:
    return json.loads(RECORDING.read_text())  # type: ignore[no-any-return]


@pytest.fixture
def stub(tmp_path: Path, recording: dict[str, Any]) -> Iterator[Any]:
    server = _stub_type()(tmp_path, recording)
    try:
        server.start()
        yield server
    finally:
        server.close()


@pytest.fixture
def github(stub: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[httpx.Client]:
    monkeypatch.setenv("SSL_CERT_FILE", str(stub.ca_file))
    credentials = GitHubCredentials(
        Settings(
            GITHUB_API_URL=stub.base_url,
            GITHUB_APP_ID="51",
            GITHUB_APP_PRIVATE_KEY=_app_key(),
            GITHUB_TOKEN="",
        )
    )
    token = credentials.token_for_verified_installation(REPO, 5501)
    with httpx.Client(
        base_url=stub.base_url,
        verify=ssl.create_default_context(cafile=str(stub.ca_file)),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        trust_env=False,
    ) as client:
        yield client


def _ok(response: httpx.Response, status: int = 200) -> Any:
    assert response.status_code == status, response.text
    return response.json()


def _seconds(recording: dict[str, Any], instant: str) -> float:
    return (
        datetime.fromisoformat(instant.replace("Z", "+00:00"))
        - datetime.fromisoformat(recording["epoch"].replace("Z", "+00:00"))
    ).total_seconds()


def test_tls_requires_the_generated_ca(stub: Any) -> None:
    assert stub.base_url.startswith("https://")
    with httpx.Client(trust_env=False) as untrusted, pytest.raises(httpx.ConnectError):
        untrusted.get(f"{stub.base_url}/repos/{REPO}")
    with httpx.Client(
        verify=ssl.create_default_context(cafile=str(stub.ca_file)), trust_env=False
    ) as trusted:
        response = trusted.get(f"{stub.base_url}/repos/{REPO}")
    assert response.status_code == 200
    assert stub.unknown_requests == []


def test_advertised_service_dns_uses_a_trusted_certificate_and_separate_bind_address(
    tmp_path: Path, recording: dict[str, Any]
) -> None:
    hostname = "github-stub.test-3815.svc.cluster.local"
    server = _stub_type()(tmp_path, recording, host=hostname, bind="127.0.0.1")
    try:
        server.start()
        response = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--fail",
                "--noproxy",
                "*",
                "--resolve",
                f"{hostname}:{server.port}:127.0.0.1",
                "--cacert",
                str(server.ca_file),
                f"{server.base_url}/repos/{REPO}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert response.returncode == 0, response.stderr
        assert json.loads(response.stdout)["full_name"] == REPO
        assert server.unknown_requests == []
    finally:
        server.close()


@pytest.mark.parametrize("host", ["user@github.example.com", "github.example.com/path"])
def test_advertised_host_refuses_userinfo_and_paths(
    tmp_path: Path, recording: dict[str, Any], host: str
) -> None:
    with pytest.raises(ValueError):
        _stub_type()(tmp_path, recording, host=host, bind="127.0.0.1")


def test_app_discovery_and_token_scope_use_real_credentials(
    github: httpx.Client, stub: Any
) -> None:
    repository = _ok(github.get(f"/repos/{REPO}"))
    assert (repository["id"], repository["full_name"]) == (4401, REPO)
    installation = _ok(github.get(f"/repos/{REPO}/installation"))
    assert installation["id"] == 5501
    assert installation["permissions"]["contents"] == "write"
    assert installation["permissions"]["issues"] == "write"
    assert installation["permissions"]["pull_requests"] == "write"
    assert installation["permissions"]["checks"] == "read"
    assert installation["permissions"]["statuses"] == "read"
    permission = _ok(github.get(f"/repos/{REPO}/collaborators/octocat/permission"))
    assert permission["permission"] == "write"
    assert permission["user"] == {"id": 6601, "login": "octocat", "type": "User"}
    assert stub.unknown_requests == []


def test_issues_labels_comments_and_polling_round_trip(github: httpx.Client, stub: Any) -> None:
    issue = _ok(
        github.post(
            f"/repos/{REPO}/issues",
            json={
                "title": "Exercise the factory",
                "body": "An example issue.",
                "labels": ["factory", "keep-me"],
            },
        ),
        201,
    )
    number = issue["number"]
    assert issue["user"]["type"] == "Bot"
    assert issue["performed_via_github_app"]["id"] == 51
    issue_path = f"/repos/{REPO}/issues/{number}"
    assert _ok(github.get(issue_path))["body"] == "An example issue."
    listed = _ok(github.get(f"/repos/{REPO}/issues", params={"state": "open", "labels": "factory"}))
    assert number in [item["number"] for item in listed]
    events = _ok(github.get(f"{issue_path}/events"))
    assert any(item["event"] == "labeled" and item["label"]["name"] == "factory" for item in events)

    _ok(github.post(f"{issue_path}/labels", json={"labels": ["curie-factory:running"]}))
    removed = github.delete(f"{issue_path}/labels/curie-factory:running")
    assert removed.status_code == 200, removed.text
    labels = _ok(github.get(f"{issue_path}/labels"))
    assert {item["name"] for item in labels} == {"factory", "keep-me"}

    comment = _ok(github.post(f"{issue_path}/comments", json={"body": "First status."}), 201)
    assert comment["user"]["type"] == "Bot"
    assert comment["performed_via_github_app"]["id"] == 51
    comment_path = f"/repos/{REPO}/issues/comments/{comment['id']}"
    assert _ok(github.get(comment_path))["body"] == "First status."
    updated = _ok(github.patch(comment_path, json={"body": "Final status."}))
    assert updated["id"] == comment["id"]
    assert _ok(github.get(f"{issue_path}/comments"))[0]["body"] == "Final status."
    assert any(
        item["id"] == comment["id"] for item in _ok(github.get(f"/repos/{REPO}/issues/comments"))
    )
    assert stub.unknown_requests == []


def test_captured_lifecycle_replays_pending_shards_before_the_required_aggregate(
    github: httpx.Client, stub: Any, recording: dict[str, Any]
) -> None:
    source = recording["source"]
    assert source["repository"] == "curie-eng/curie"
    assert source["pull_request"] == 3400
    assert source["method"] == "completed-check-lifecycle"
    path = f"/repos/{REPO}/commits/{source['head_sha']}"
    stub.advance(180)
    pending = _ok(github.get(f"{path}/check-runs", params={"per_page": 100, "filter": "latest"}))
    assert pending["total_count"] == len(pending["check_runs"])
    assert not any(run["name"] == PYTHON_CHECK for run in pending["check_runs"])
    shards = [
        run for run in pending["check_runs"] if run["name"].startswith("Python pytest (shard ")
    ]
    assert len(shards) == 3
    assert all(run["status"] == "in_progress" and run["conclusion"] is None for run in shards)
    assert _ok(github.get(f"{path}/status"))["statuses"] == []
    assert _ok(github.get(f"/repos/{REPO}/check-runs/{shards[0]['id']}/annotations")) == []

    final_seconds = max(_seconds(recording, run["completed_at"]) for run in recording["check_runs"])
    stub.advance(final_seconds - 180 + 1)
    final = _ok(github.get(f"{path}/check-runs", params={"per_page": 100, "filter": "latest"}))
    assert final["total_count"] == len(recording["check_runs"])
    assert {run["id"]: run for run in final["check_runs"]} == {
        run["id"]: run for run in recording["check_runs"]
    }
    assert stub.unknown_requests == []


def test_unknown_calls_fail_loudly_and_remain_observable(github: httpx.Client, stub: Any) -> None:
    response = github.get(f"/repos/{REPO}/not-an-implemented-endpoint")
    assert response.status_code == 501
    assert "unsupported_request" in response.text
    assert "not-an-implemented-endpoint" in response.text
    assert len(stub.unknown_requests) == 1
    with pytest.raises(RuntimeError, match="unsupported|unknown"):
        stub.close()


def test_git_smart_http_clones_pushes_and_exposes_the_pushed_ref(
    github: httpx.Client, stub: Any, tmp_path: Path
) -> None:
    env = dict(os.environ)
    env.update({"GIT_TERMINAL_PROMPT": "0", "GIT_SSL_CAINFO": str(stub.ca_file)})
    authorization = github.headers["Authorization"]

    def git(*args: str, cwd: Path = tmp_path) -> str:
        result = subprocess.run(
            ["git", "-c", f"http.extraHeader=Authorization: {authorization}", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    checkout = tmp_path / "checkout"
    git("clone", stub.clone_url, str(checkout))
    git("checkout", "-b", "factory/example", cwd=checkout)
    (checkout / "example.txt").write_text("A real Git object pushed over HTTPS.\n")
    git("add", "example.txt", cwd=checkout)
    git(
        "-c",
        "user.name=Example Author",
        "-c",
        "user.email=author@example.com",
        "commit",
        "-m",
        "Add example",
        cwd=checkout,
    )
    pushed_sha = git("rev-parse", "HEAD", cwd=checkout)
    git("push", "origin", "HEAD:refs/heads/factory/example", cwd=checkout)
    assert git("ls-remote", stub.clone_url, "refs/heads/factory/example").split()[0] == pushed_sha
    branch = _ok(github.get(f"/repos/{REPO}/branches/factory%2Fexample"))
    assert branch["commit"]["sha"] == pushed_sha
    pull = _ok(
        github.post(
            f"/repos/{REPO}/pulls",
            json={
                "title": "Example publication",
                "body": "An example change.",
                "head": "factory/example",
                "base": "main",
            },
        ),
        201,
    )
    assert pull["head"]["ref"] == "factory/example"
    assert pull["head"]["sha"] == pushed_sha
    assert pull["base"]["ref"] == "main"
    assert _ok(github.get(f"/repos/{REPO}/pulls/{pull['number']}"))["html_url"] == pull["html_url"]
    listed = _ok(
        github.get(
            f"/repos/{REPO}/pulls",
            params={"state": "open", "head": "acme-corp:factory/example", "base": "main"},
        )
    )
    assert [item["number"] for item in listed] == [pull["number"]]
    verifier = tmp_path / "verifier"
    git("clone", "--branch", "factory/example", stub.clone_url, str(verifier))
    assert (verifier / "example.txt").read_text() == "A real Git object pushed over HTTPS.\n"
    assert stub.unknown_requests == []
