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
from curie_api.github_app import GitHubAuth, GitHubCredentials

STUB_ROOT = Path(__file__).resolve().parents[1]
REPO = "acme-corp/acme-bot"
RECORDING = STUB_ROOT / "recordings" / "curie-pr-3400.json"
PYTHON_CHECK = "Python (ruff + mypy + pytest)"
SEED_TREE = STUB_ROOT.parent / "factory-e2e" / "fixtures" / "unitconv"
ISSUE_BODY = "Add Kelvin (`k`) temperature conversions and tests in unitconv.\n"


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
def issue_file(tmp_path: Path) -> Path:
    issue = tmp_path / "issue.md"
    issue.write_text(ISSUE_BODY)
    return issue


@pytest.fixture
def stub(tmp_path: Path, recording: dict[str, Any], issue_file: Path) -> Iterator[Any]:
    server = _stub_type()(tmp_path, recording, seed_tree=SEED_TREE, issue_file=issue_file)
    try:
        server.start()
        yield server
    finally:
        server.close()


@pytest.fixture
def github(stub: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[httpx.Client]:
    monkeypatch.setenv("SSL_CERT_FILE", str(stub.ca_file))
    credentials = GitHubCredentials(
        auth=GitHubAuth.from_settings(
            Settings(
                GITHUB_API_URL=stub.base_url,
                GITHUB_APP_ID="51",
                GITHUB_APP_PRIVATE_KEY=_app_key(),
                GITHUB_TOKEN="",
            )
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
    tmp_path: Path, recording: dict[str, Any], issue_file: Path
) -> None:
    hostname = "github-stub.test-3815.svc.cluster.local"
    server = _stub_type()(
        tmp_path,
        recording,
        seed_tree=SEED_TREE,
        issue_file=issue_file,
        host=hostname,
        bind="127.0.0.1",
    )
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
    tmp_path: Path, recording: dict[str, Any], issue_file: Path, host: str
) -> None:
    with pytest.raises(ValueError):
        _stub_type()(
            tmp_path,
            recording,
            seed_tree=SEED_TREE,
            issue_file=issue_file,
            host=host,
            bind="127.0.0.1",
        )


def test_seed_tree_and_issue_are_required_constructor_inputs(
    tmp_path: Path, recording: dict[str, Any], issue_file: Path
) -> None:
    with pytest.raises(TypeError, match="seed_tree|issue_file"):
        _stub_type()(tmp_path, recording)
    with pytest.raises(TypeError, match="seed_tree"):
        _stub_type()(tmp_path, recording, issue_file=issue_file)
    with pytest.raises(TypeError, match="issue_file"):
        _stub_type()(tmp_path, recording, seed_tree=SEED_TREE)


def test_seeded_issue_uses_the_required_input_and_a_human_label_event(
    github: httpx.Client, stub: Any
) -> None:
    issues = _ok(github.get(f"/repos/{REPO}/issues", params={"labels": "factory"}))
    assert len(issues) == 1
    issue = issues[0]
    assert issue["body"] == ISSUE_BODY
    assert issue["user"]["type"] == "User"
    events = _ok(github.get(f"/repos/{REPO}/issues/{issue['number']}/events"))
    assert len(events) == 1
    assert events[0]["event"] == "labeled"
    assert events[0]["label"]["name"] == "factory"
    assert events[0]["actor"]["type"] == "User"
    assert stub.unknown_requests == []


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
        run["id"]: run | {"head_sha": source["head_sha"]} for run in recording["check_runs"]
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
    assert not (checkout / "example.py").exists()
    for relative in ("unitconv/convert.py", "unitconv/tests/test_convert.py", "README.md"):
        assert (checkout / relative).read_bytes() == (SEED_TREE / relative).read_bytes()
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
    # The worker's publication reconcile reads sha, message and parents from
    # GitHub's Git database commit
    # (https://docs.github.com/en/rest/git/commits#get-a-commit-object).
    commit = _ok(github.get(f"/repos/{REPO}/git/commits/{pushed_sha}"))
    assert commit["sha"] == pushed_sha
    assert commit["message"].splitlines()[0] == "Add example"
    assert [parent["sha"] for parent in commit["parents"]] == [
        git("rev-parse", "HEAD~1", cwd=checkout)
    ]
    missing = github.get(f"/repos/{REPO}/git/commits/{'0' * 40}")
    assert missing.status_code == 404, missing.text
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
    fetched = _ok(github.get(f"/repos/{REPO}/pulls/{pull['number']}"))
    assert fetched["html_url"] == pull["html_url"]
    # GitHub returns a global node ID on every pull request
    # (https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request), and the
    # API's publication identity check refuses a pull request without one.
    assert isinstance(pull["node_id"], str) and pull["node_id"]
    assert fetched["node_id"] == pull["node_id"]
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


def test_repository_id_paths_serve_the_same_repository(github: httpx.Client, stub: Any) -> None:
    """GitHub serves /repositories/{id}/... like /repos/{owner}/{repo}/...; a
    factory recording run read its issue through the id form."""

    issue = _ok(github.get(f"/repos/{REPO}/issues", params={"labels": "factory"}))[0]
    by_id = _ok(github.get(f"/repositories/4401/issues/{issue['number']}"))
    assert by_id == _ok(github.get(f"/repos/{REPO}/issues/{issue['number']}"))
    assert _ok(github.get("/repositories/4401"))["full_name"] == REPO
    assert stub.unknown_requests == []


def test_missing_branch_lookups_answer_404_like_github(github: httpx.Client, stub: Any) -> None:
    """The factory publication probes its new head branch before pushing it.

    GitHub answers 404 for both lookups when the branch does not exist:
    "Get a branch" (https://docs.github.com/en/rest/branches/branches#get-a-branch)
    returns {"message": "Branch not found"}, and "Get a reference"
    (https://docs.github.com/en/rest/git/refs#get-a-reference) returns
    {"message": "Not Found"}.
    """

    branch = github.get(f"/repos/{REPO}/branches/factory%2Fnot-pushed-yet")
    assert branch.status_code == 404, branch.text
    assert branch.json()["message"] == "Branch not found"
    ref = github.get(f"/repos/{REPO}/git/ref/heads/factory%2Fnot-pushed-yet")
    assert ref.status_code == 404, ref.text
    assert ref.json()["message"] == "Not Found"
    assert stub.unknown_requests == []


def test_existing_branch_lookups_still_resolve_main_and_pushed_branches(
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

    main_sha = git("ls-remote", stub.clone_url, "refs/heads/main").split()[0]
    assert _ok(github.get(f"/repos/{REPO}/branches/main"))["commit"]["sha"] == main_sha
    main_ref = _ok(github.get(f"/repos/{REPO}/git/ref/heads/main"))
    assert main_ref["ref"] == "refs/heads/main"
    assert main_ref["object"]["sha"] == main_sha

    checkout = tmp_path / "checkout"
    git("clone", stub.clone_url, str(checkout))
    (checkout / "probe.txt").write_text("Pushed after a 404 lookup.\n")
    git("add", "probe.txt", cwd=checkout)
    git(
        "-c",
        "user.name=Example Author",
        "-c",
        "user.email=author@example.com",
        "commit",
        "-m",
        "Add probe",
        cwd=checkout,
    )
    pushed_sha = git("rev-parse", "HEAD", cwd=checkout)
    assert github.get(f"/repos/{REPO}/branches/factory%2Fprobe").status_code == 404
    git("push", "origin", "HEAD:refs/heads/factory/probe", cwd=checkout)
    assert _ok(github.get(f"/repos/{REPO}/branches/factory%2Fprobe"))["commit"]["sha"] == pushed_sha
    pushed_ref = _ok(github.get(f"/repos/{REPO}/git/ref/heads/factory%2Fprobe"))
    assert pushed_ref["object"]["sha"] == pushed_sha
    assert stub.unknown_requests == []
