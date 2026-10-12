"""The Data Center loop driver against a running Bitbucket DC fixture (#4401 AC 12.3).

Skipped unless ``CURIE_BITBUCKET_DC_FIXTURE`` names the ``state.json`` that
``curie dev forge-fixture bitbucket-dc up`` wrote. With the fixture up, this
opens a pull request on it as the factory token, then drives the driver the
way the forge loop does: the red loop gate on the first head, a pushed
revision whose head the driver turns green, a review comment as ``reviewer``,
and the merge as the admin, read back as merged.

Pass or fail, the test leaves the fixture as it found it: it declines the pull
request if still open, deletes its branch, and, once the merge moved the
default branch, force-updates it back to the head recorded at the start.

    CURIE_BITBUCKET_DC_FIXTURE=<dir>/state.json \\
        python3 -m pytest tools/factory-e2e/tests -k bitbucket_dc_live -rs
"""

from __future__ import annotations

import importlib.util
import json
import os
import secrets
import ssl
import subprocess
import sys
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
HELPERS = REPO_ROOT / "tools" / "forge-fixtures" / "bitbucket-dc" / "helpers.py"
STATE_VAR = "CURIE_BITBUCKET_DC_FIXTURE"

pytestmark = pytest.mark.skipif(
    not os.environ.get(STATE_VAR), reason=f"{STATE_VAR} is unset: no Bitbucket DC fixture is up"
)


def _helpers() -> Any:
    spec = importlib.util.spec_from_file_location("bitbucket_dc_fixture_helpers", HELPERS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _preflight(tmp_path: Path) -> fe.Preflight:
    app = tmp_path / "app"
    app.mkdir()
    meta = {"id": 42, "slug": "example-factory", "installation_id": 7, "repo": "acme/fixture"}
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    env = {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(app),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }

    def no_gh(user: str) -> str:
        raise AssertionError(user)

    return fe.Preflight(
        fe.load_config(env, context=None, gh_token=no_gh),
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
    )


class _Fixture:
    """REST and git on the fixture repository as the factory token."""

    def __init__(self, state: dict[str, Any], helpers: Any) -> None:
        self.state = state
        self.helpers = helpers
        self.token = Path(state["token_file"]).read_text().strip()
        self.context = ssl.create_default_context(cafile=state["ca_file"])
        self.rest = (
            f"{state['base_url']}/rest/api/latest/projects/"
            f"{state['project_key']}/repos/{state['repo_slug']}"
        )

    def call(self, method: str, path: str, body: Any = None) -> Any:
        request = urllib.request.Request(
            self.rest + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, context=self.context, timeout=30) as response:
            data = response.read()
        return json.loads(data) if data else None

    def git(self, *args: str, cwd: Path) -> str:
        env = {**os.environ, **self.helpers.git_env(self.state, self.token)}
        done = subprocess.run(
            ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
        )
        return done.stdout.strip()

    def clone_url(self) -> str:
        links = self.call("GET", "")["links"]["clone"]
        return str(next(link["href"] for link in links if link["name"] in ("http", "https")))


@dataclass
class _Mutations:
    """What the test changed on the fixture, so teardown can undo exactly that."""

    work: Path
    default: str
    default_head: str
    branch: str = ""
    pull_request: int | None = None


def _remote_head(fixture: _Fixture, ref: str, cwd: Path) -> str:
    listed = fixture.git("ls-remote", "origin", ref, cwd=cwd)
    return listed.split()[0] if listed else ""


def _undo(fixture: _Fixture, done: _Mutations) -> list[str]:
    """Decline, delete the branch, restore the default branch; each step's failure."""

    failures: list[str] = []
    merged = False
    if done.pull_request is not None:
        try:
            pull = fixture.helpers.pull_request(fixture.state, done.pull_request)
            merged = pull["state"] == "MERGED"
            if pull["state"] == "OPEN":
                fixture.helpers.decline(fixture.state, done.pull_request)
        except Exception as error:  # noqa: BLE001 -- every step runs; all are reported
            failures.append(f"decline pull request {done.pull_request}: {error}")
    if done.branch and _remote_head(fixture, f"refs/heads/{done.branch}", done.work):
        try:
            fixture.git("push", "-q", "origin", "--delete", done.branch, cwd=done.work)
        except subprocess.CalledProcessError as error:
            failures.append(f"delete branch {done.branch}: {error.stderr}")
    if merged and _remote_head(fixture, done.default, done.work) != done.default_head:
        try:
            fixture.git(
                "push",
                "-q",
                "--force",
                "origin",
                f"{done.default_head}:{done.default}",
                cwd=done.work,
            )
        except subprocess.CalledProcessError as error:
            failures.append(f"restore {done.default}: {error.stderr}")
    return failures


@pytest.fixture
def fixture() -> _Fixture:
    helpers = _helpers()
    return _Fixture(helpers.load_state(os.environ[STATE_VAR]), helpers)


@pytest.fixture
def mutations(fixture: _Fixture, tmp_path: Path) -> Iterator[_Mutations]:
    """A clone of the fixture repository; on teardown, every change the test made is undone."""

    work = tmp_path / "work"
    fixture.git("clone", "--quiet", fixture.clone_url(), str(work), cwd=tmp_path)
    default = f"refs/heads/{fixture.state['default_branch']}"
    done = _Mutations(work, default, fixture.git("rev-parse", "HEAD", cwd=work))
    yield done
    failures = _undo(fixture, done)
    assert failures == [], "the fixture was left changed: " + "; ".join(failures)
    if done.default_head:
        assert _remote_head(fixture, done.default, work) == done.default_head


def test_bitbucket_dc_live_driver_posts_status_reviews_and_merges(
    tmp_path: Path, fixture: _Fixture, mutations: _Mutations
) -> None:
    state = fixture.state
    fe.load_forge_entries()
    driver = sys.modules["factory_e2e_forges.bitbucket_dc"].BitbucketDcDriver(_preflight(tmp_path))

    # A branch with one commit, and a pull request from it, as the factory.
    work = mutations.work
    branch = f"curie/live-{secrets.token_hex(4)}"
    for key, value in (("user.name", "Factory Live Test"), ("user.email", "bot@example.invalid")):
        fixture.git("config", key, value, cwd=work)
    fixture.git("checkout", "-q", "-b", branch, cwd=work)
    (work / "LIVE.md").write_text("first\n")
    fixture.git("add", "LIVE.md", cwd=work)
    fixture.git("commit", "-q", "-m", "Live driver test", cwd=work)
    mutations.branch = branch
    fixture.git("push", "-q", "origin", branch, cwd=work)
    default = mutations.default
    opened = fixture.call(
        "POST",
        "/pull-requests",
        {
            "title": "Live driver test",
            "description": "Opened by test_bitbucket_dc_live.",
            "fromRef": {"id": f"refs/heads/{branch}"},
            "toRef": {"id": default},
        },
    )
    mutations.pull_request = int(opened["id"])
    change = fe.ChangeRef(int(opened["id"]), str(opened["links"]["self"][0]["href"]))

    first, count = driver.change_head(change)
    assert first == fixture.git("rev-parse", "HEAD", cwd=work)
    assert count == 1

    # Red on the first head.
    driver.set_ci_red(first)
    assert driver.ci_state(first)["state"] == "failing"

    # A revision: the driver turns its head green, the first head stays red.
    (work / "LIVE.md").write_text("first\nsecond\n")
    fixture.git("commit", "-q", "-am", "Live driver revision", cwd=work)
    fixture.git("push", "-q", "origin", branch, cwd=work)
    second, count = driver.change_head(change)
    assert second == fixture.git("rev-parse", "HEAD", cwd=work) != first
    assert count == 2
    assert driver.ci_state(second)["state"] == "passing"
    assert driver.ci_state(first)["state"] == "failing"

    # The review comment is authored by `reviewer`.
    review = driver.post_review(change, "Please add a round trip test.")
    assert review["status_code"] == 201
    comment = fixture.call("GET", f"/pull-requests/{change.number}/comments/{review['comment_id']}")
    assert comment["author"]["slug"] == state["users"]["reviewer"]["slug"]
    assert "Please add a round trip test." in comment["text"]

    merged = driver.merge(change)
    assert merged["merged"] is True
    assert driver.change_merged(change) is True
