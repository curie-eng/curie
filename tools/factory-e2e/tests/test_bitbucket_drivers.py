"""Unit contract for the Bitbucket loop drivers and their acceptance entries (#4401).

`forges/bitbucket_cloud.py` and `forges/bitbucket_dc.py` implement the
ForgeDriver code host operations (forges/base.py) and register installed. The
ticket operations delegate to the Jira Cloud driver. These pin, without a
network:

1. Both entries register installed and implement every protocol method.
2. The Cloud driver reads its repository, both token files and the Jira inputs
   from the environment and fails naming every unset one (decision 18.6).
3. The request each code host operation sends: the red loop gate status on
   the first head, the green one posted once by the watcher on the first
   later head while the loop waits (decision 18.1), the review comment as the
   reviewer, and the merge as the reviewer on Cloud and as the admin on the
   Data Center fixture (decision 18.2). `ci_state` only reads.
4. Every redirect is refused, so no credential reaches another URL.
5. What the drivers report passes `judge_github_loop`, the loop's judge, on a
   whole loop driven against a stubbed provider, and fails it when the
   factory never pushes the CI fix.
6. `jira_cloud+bitbucket_cloud` and `jira_cloud+bitbucket_dc` dry-run to exit 0
   and list every step without constructing a driver (decision 18.7).

The HTTP seam is the HTTPS handler under each driver's opener: the stub
replaces `urllib.request.HTTPSHandler`, so every request still passes through
the driver's real opener, redirect handler and error processing.
"""

from __future__ import annotations

import base64
import dataclasses
import email.message
import io
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

import acceptance
import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SHA = "c" * 40
GREEN = "d" * 40
THIRD = "e" * 40
DRIVER_METHODS = (
    "open_and_label_ticket",
    "await_change",
    "change_head",
    "set_ci_red",
    "ci_state",
    "post_review",
    "merge",
    "change_merged",
    "changes_opened",
    "default_branch_head",
    "reset_fixture",
    "pin_fixture_base",
)
DRIVERS = {"bitbucket_cloud": "BitbucketCloudDriver", "bitbucket_dc": "BitbucketDcDriver"}
LOOP_GATE_KEY = "factory-e2e-loop-gate"

CLOUD_VARS = (
    "CURIE_BITBUCKET_CLOUD_REPO",
    "CURIE_BITBUCKET_CLOUD_TOKEN_FILE",
    "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE",
    "CURIE_JIRA_SITE",
    "CURIE_JIRA_TOKEN_FILE",
)
CLOUD_API = "https://api.bitbucket.org/2.0/repositories/example-workspace/example-repo"
FACTORY_TOKEN = "factory-token-placeholder"
REVIEWER_TOKEN = "reviewer-token-placeholder"

DC_BASE = "https://localhost:27443"
DC_REST = f"{DC_BASE}/rest/api/latest/projects/FAC/repos/demo"
REVIEWER_PASSWORD = "reviewer-password-placeholder"
ADMIN_PASSWORD = "admin-password-placeholder"

PAIRINGS = ("jira_cloud+bitbucket_cloud", "jira_cloud+bitbucket_dc")


# --- a Preflight, as the loop hands every driver one -----------------------------------


def _app_dir(tmp_path: Path) -> Path:
    app = tmp_path / "app"
    app.mkdir(parents=True)
    meta = {"id": 42, "slug": "example-factory", "installation_id": 7, "repo": "acme/fixture"}
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    return app


def _preflight(tmp_path: Path) -> fe.Preflight:
    env = {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(_app_dir(tmp_path)),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }

    def no_gh(user: str) -> str:
        raise AssertionError(user)

    return fe.Preflight(
        fe.load_config(env, context=None, gh_token=no_gh),
        repo_root=REPO_ROOT,
        candidate=SHA,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
    )


def _entry_module(name: str) -> Any:
    fe.load_forge_entries()
    return sys.modules[f"factory_e2e_forges.{name}"]


# --- the HTTP stub -----------------------------------------------------------------------


@dataclasses.dataclass
class _Call:
    method: str
    url: str
    headers: dict[str, str]
    body: Any


class _Response:
    def __init__(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.code = status
        self.reason = self.msg = "stub"
        self.headers = email.message.Message()
        self.headers["Content-Type"] = "application/json"
        for name, value in (headers or {}).items():
            self.headers[name] = value
        self._data = b"" if payload is None else json.dumps(payload).encode()

    def read(self, *_a: Any) -> bytes:
        return self._data

    def getcode(self) -> int:
        return self.status

    def info(self) -> email.message.Message:
        return self.headers

    def close(self) -> None:
        return None

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


Answer = Callable[[str, str], tuple[Any, ...]]


class _Http:
    """Records every request; ``answer(method, url)`` gives (status, JSON payload[, headers])."""

    def __init__(self, answer: Answer) -> None:
        self.answer = answer
        self.calls: list[_Call] = []
        # The request being answered on this thread (the green watcher runs on its own).
        self.current = threading.local()

    def __call__(self, request: urllib.request.Request | str, *_a: Any, **_k: Any) -> _Response:
        if isinstance(request, str):
            request = urllib.request.Request(request)
        data = request.data
        body: Any = None
        if isinstance(data, bytes) and data:
            try:
                body = json.loads(data)
            except ValueError:
                body = data.decode()
        headers = {key.lower(): value for key, value in request.header_items()}
        call = _Call(request.get_method(), request.full_url, headers, body)
        self.calls.append(call)
        self.current.call = call
        status, payload, *rest = self.answer(call.method, call.url)
        if status >= 400:
            raise urllib.error.HTTPError(
                call.url,
                status,
                "stub",
                email.message.Message(),
                io.BytesIO(json.dumps(payload or {}).encode()),
            )
        return _Response(status, payload, rest[0] if rest else None)

    def writes(self) -> list[_Call]:
        return [call for call in self.calls if call.method != "GET"]


def _install(monkeypatch: pytest.MonkeyPatch, answer: Answer) -> _Http:
    """Answer at the HTTPS handler, below the driver's own opener."""

    http = _Http(answer)

    class _StubHttps(urllib.request.HTTPSHandler):
        def https_open(self, req: urllib.request.Request) -> Any:
            return http(req)

    monkeypatch.setattr(urllib.request, "HTTPSHandler", _StubHttps)
    return http


def _ok(method: str, url: str) -> tuple[int, Any]:
    if method == "GET":
        return 200, {"values": [], "isLastPage": True}
    return 201, {"id": 501}


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


# --- registration ------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(DRIVERS))
def test_each_bitbucket_entry_registers_its_driver_installed(name: str) -> None:
    module = _entry_module(name)
    entry = fe.FORGES[name]
    assert entry.name == name
    assert entry.installed is True
    assert entry.driver is getattr(module, DRIVERS[name])


@pytest.mark.parametrize("name", sorted(DRIVERS))
def test_each_bitbucket_driver_implements_every_protocol_method(name: str) -> None:
    driver = getattr(_entry_module(name), DRIVERS[name])
    assert driver.forge == name
    for method in DRIVER_METHODS:
        assert callable(getattr(driver, method, None)), method


# --- Bitbucket Cloud: configuration ------------------------------------------------------


def _cloud_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    factory = tmp_path / "factory-token"
    factory.write_text(FACTORY_TOKEN + "\n")
    reviewer = tmp_path / "reviewer-token"
    reviewer.write_text(REVIEWER_TOKEN + "\n")
    jira = tmp_path / "jira-token"
    jira.write_text("jira-token-placeholder\n")
    values = {
        "CURIE_BITBUCKET_CLOUD_REPO": "example-workspace/example-repo",
        "CURIE_BITBUCKET_CLOUD_TOKEN_FILE": str(factory),
        "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE": str(reviewer),
        "CURIE_JIRA_SITE": "https://example.invalid",
        "CURIE_JIRA_TOKEN_FILE": str(jira),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _cloud_driver(tmp_path: Path) -> Any:
    return _entry_module("bitbucket_cloud").BitbucketCloudDriver(_preflight(tmp_path))


def test_cloud_driver_with_every_variable_unset_names_each_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in CLOUD_VARS:
        monkeypatch.delenv(name, raising=False)
    http = _install(monkeypatch, _ok)

    with pytest.raises(fe.ConfigError) as raised:
        _cloud_driver(tmp_path)

    for name in CLOUD_VARS:
        assert name in str(raised.value), name
    assert http.calls == []


@pytest.mark.parametrize("missing", CLOUD_VARS)
@pytest.mark.parametrize("how", ["unset", "empty"])
def test_cloud_driver_names_only_the_variable_that_is_unset(
    missing: str, how: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    if how == "unset":
        monkeypatch.delenv(missing)
    else:
        monkeypatch.setenv(missing, "")
    http = _install(monkeypatch, _ok)

    with pytest.raises(fe.ConfigError) as raised:
        _cloud_driver(tmp_path)

    message = str(raised.value)
    assert missing in message
    for other in CLOUD_VARS:
        if other != missing:
            assert other not in message, other
    for token in (FACTORY_TOKEN, REVIEWER_TOKEN):
        assert token not in message
    assert http.calls == []


def test_cloud_driver_delegates_ticket_operations_to_the_jira_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    _install(monkeypatch, _ok)
    fe.load_forge_entries()
    opened: list[tuple[str, str, float]] = []

    class _Jira:
        forge = "jira_cloud"

        def __init__(self, p: fe.Preflight) -> None:
            self.p = p

        def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
            opened.append((title, body, since))
            return "wi-1"

    monkeypatch.setitem(
        fe.FORGES, "jira_cloud", fe.ForgeEntry(name="jira_cloud", driver=_Jira, installed=True)
    )

    driver = _cloud_driver(tmp_path)

    assert driver.open_and_label_ticket("Title", "Body", 12.5) == "wi-1"
    assert opened == [("Title", "Body", 12.5)]


# --- Bitbucket Cloud: request shapes -----------------------------------------------------


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        ([], "none"),
        (["SUCCESSFUL"], "passing"),
        (["SUCCESSFUL", "INPROGRESS"], "pending"),
        (["SUCCESSFUL", "FAILED"], "failing"),
        (["INPROGRESS", "FAILED"], "failing"),
        (["STOPPED"], "failing"),
    ],
)
def test_cloud_ci_state_maps_the_head_statuses(
    states: list[str], expected: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    values = [{"key": f"k{i}", "state": state} for i, state in enumerate(states)]

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "GET" and url.startswith(f"{CLOUD_API}/commit/{SHA}/statuses"):
            return 200, {"values": values, "pagelen": 50, "page": 1}
        return _ok(method, url)

    http = _install(monkeypatch, answer)
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    assert driver.ci_state(SHA)["state"] == expected
    # A read only: no status is ever posted from here.
    assert http.writes() == []


def test_cloud_ci_state_is_unreadable_when_the_status_read_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "GET" and "/statuses" in url:
            return 500, {"error": {"message": "stub"}}
        return _ok(method, url)

    _install(monkeypatch, answer)
    assert _cloud_driver(tmp_path).ci_state(SHA)["state"] == "unreadable"


def test_cloud_merge_is_posted_by_the_reviewer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "POST" and url.startswith(f"{CLOUD_API}/pullrequests/7/merge"):
            return 200, {"id": 7, "state": "MERGED"}
        if method == "GET" and url.startswith(f"{CLOUD_API}/pullrequests/7"):
            return 200, {"id": 7, "state": "OPEN"}
        return _ok(method, url)

    http = _install(monkeypatch, answer)
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    result = driver.merge(fe.ChangeRef(7, "https://example.invalid/pr/7"))

    (post,) = http.writes()
    assert post.url.startswith(f"{CLOUD_API}/pullrequests/7/merge")
    assert post.headers["authorization"] == f"Bearer {REVIEWER_TOKEN}"
    assert (result["status_code"], result["merged"]) == (200, True)


@pytest.mark.parametrize(
    ("status", "state", "expected"),
    [(200, "MERGED", True), (200, "OPEN", False), (200, "DECLINED", False), (500, None, None)],
)
def test_cloud_change_merged_reads_the_pull_request(
    status: int,
    state: str | None,
    expected: bool | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cloud_env(tmp_path, monkeypatch)

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "GET" and url.split("?")[0] == f"{CLOUD_API}/pullrequests/7":
            return status, {"id": 7, "state": state}
        return _ok(method, url)

    _install(monkeypatch, answer)
    driver = _cloud_driver(tmp_path)
    assert driver.change_merged(fe.ChangeRef(7, "https://example.invalid/pr/7")) is expected


def _paged_commits(next_url: str) -> Answer:
    """The pull request, then a first commits page whose ``next`` is ``next_url``."""

    commits = f"{CLOUD_API}/pullrequests/7/commits"

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "GET" and url.split("?")[0] == f"{CLOUD_API}/pullrequests/7":
            return 200, {"id": 7, "source": {"commit": {"hash": SHA}}}
        if method == "GET" and url.startswith(commits) and "page=2" not in url:
            return 200, {"values": [{"hash": SHA}], "next": next_url}
        if method == "GET" and url.startswith(commits):
            return 200, {"values": [{"hash": GREEN}]}
        return 404, {"error": {"message": "stub"}}

    return answer


def test_cloud_paging_refuses_a_next_link_on_another_origin_before_sending_the_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    foreign = "https://attacker.example.invalid/2.0/page?page=2"
    http = _install(monkeypatch, _paged_commits(foreign))
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    with pytest.raises(fe.PreflightFailed):
        driver.change_head(fe.ChangeRef(7, "https://example.invalid/pr/7"))

    # No request, so no Authorization header, ever reached the foreign host.
    assert {urllib.parse.urlsplit(call.url).netloc for call in http.calls} == {"api.bitbucket.org"}
    assert any(call.url.startswith(f"{CLOUD_API}/pullrequests/7/commits") for call in http.calls)


@pytest.mark.parametrize(
    "foreign",
    [
        "http://api.bitbucket.org/2.0/repositories/example-workspace/example-repo/x?page=2",
        "https://api.bitbucket.org:8443/2.0/repositories/example-workspace/example-repo/x?page=2",
        "https://api.bitbucket.org.example.invalid/2.0/x?page=2",
    ],
)
def test_cloud_paging_refuses_a_next_link_with_another_scheme_port_or_host(
    foreign: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    http = _install(monkeypatch, _paged_commits(foreign))
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    with pytest.raises(fe.PreflightFailed):
        driver.change_head(fe.ChangeRef(7, "https://example.invalid/pr/7"))

    assert foreign not in [call.url for call in http.calls]


def test_cloud_paging_follows_a_same_origin_next_link_with_the_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    same = f"{CLOUD_API}/pullrequests/7/commits?pagelen=50&page=2"
    http = _install(monkeypatch, _paged_commits(same))
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    head, count = driver.change_head(fe.ChangeRef(7, "https://example.invalid/pr/7"))

    assert (head, count) == (SHA, 2)
    (second,) = [call for call in http.calls if call.url == same]
    assert second.headers["authorization"] == f"Bearer {FACTORY_TOKEN}"


# --- Bitbucket Data Center: configuration and request shapes -----------------------------


def _dc_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state_dir = tmp_path / "fixture"
    state_dir.mkdir()
    ca = state_dir / "ca.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=example.invalid",
            "-days",
            "1",
            "-keyout",
            str(state_dir / "ca.key"),
            "-out",
            str(ca),
        ],
        check=True,
        capture_output=True,
    )
    secrets = {
        "factory-token": FACTORY_TOKEN,
        "admin-password": ADMIN_PASSWORD,
        "reviewer-password": REVIEWER_PASSWORD,
        "outsider-password": "outsider-password-placeholder",
    }
    for name, value in secrets.items():
        (state_dir / name).write_text(value + "\n")
    state = {
        "schema": "curie.forge-fixture.bitbucket-dc/v1",
        "project": "curie-forge-bbdc-0a1b2c3d",
        "base_url": DC_BASE,
        "advertised_url": "https://example.invalid:27443",
        "host": "localhost",
        "port": 27443,
        "ca_file": str(ca),
        "version": "9.4.24",
        "project_key": "FAC",
        "repo_slug": "demo",
        "repo_path": "FAC/demo",
        "repo_id": "1",
        "default_branch": "main",
        "token_file": str(state_dir / "factory-token"),
        "token_expires_at": "2026-10-13T00:00:00Z",
        "admin": {
            "username": "admin",
            "user_id": "1",
            "password_file": str(state_dir / "admin-password"),
        },
        "users": {
            "reviewer": {
                "user_id": "2",
                "slug": "reviewer",
                "password_file": str(state_dir / "reviewer-password"),
            },
            "outsider": {
                "user_id": "3",
                "slug": "outsider",
                "password_file": str(state_dir / "outsider-password"),
            },
        },
        "compose_file": str(state_dir / "compose.yaml"),
    }
    path = state_dir / "state.json"
    path.write_text(json.dumps(state))
    monkeypatch.setenv("CURIE_BITBUCKET_DC_FIXTURE", str(path))
    return path


def _dc_driver(tmp_path: Path) -> Any:
    return _entry_module("bitbucket_dc").BitbucketDcDriver(_preflight(tmp_path))


def test_dc_driver_without_the_fixture_variable_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CURIE_BITBUCKET_DC_FIXTURE", raising=False)
    http = _install(monkeypatch, _ok)

    with pytest.raises(fe.ConfigError, match="CURIE_BITBUCKET_DC_FIXTURE"):
        _dc_driver(tmp_path)
    assert http.calls == []


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        ([], "none"),
        (["SUCCESSFUL"], "passing"),
        (["SUCCESSFUL", "INPROGRESS"], "pending"),
        (["SUCCESSFUL", "UNKNOWN"], "pending"),
        (["SUCCESSFUL", "FAILED"], "failing"),
        (["CANCELLED"], "failing"),
    ],
)
def test_dc_ci_state_maps_the_head_builds(
    states: list[str], expected: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dc_state(tmp_path, monkeypatch)
    values = [{"key": f"k{i}", "state": state} for i, state in enumerate(states)]

    # Bitbucket DC 9.4.24, observed on the fixture: the repository-scoped builds
    # GET answers 400 without ?key= and one build with it, so keys come from the
    # build-status listing and each state from the repository-scoped read.
    def answer(method: str, url: str) -> tuple[int, Any]:
        path, _, query = url.partition("?")
        if method == "GET" and path == f"{DC_BASE}/rest/build-status/latest/commits/{SHA}":
            return 200, {"values": values, "isLastPage": True, "start": 0, "size": len(values)}
        if method == "GET" and path == f"{DC_REST}/commits/{SHA}/builds":
            key = urllib.parse.parse_qs(query)["key"][0]
            return 200, next(v for v in values if v["key"] == key)
        return _ok(method, url)

    http = _install(monkeypatch, answer)
    driver = _dc_driver(tmp_path)
    http.calls.clear()

    assert driver.ci_state(SHA)["state"] == expected
    assert http.writes() == []


def test_dc_merge_is_posted_by_the_admin_at_the_current_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dc_state(tmp_path, monkeypatch)

    def answer(method: str, url: str) -> tuple[int, Any]:
        path = url.split("?")[0]
        if method == "POST" and path == f"{DC_REST}/pull-requests/7/merge":
            return 200, {"id": 7, "version": 4, "state": "MERGED"}
        if method == "GET" and path == f"{DC_REST}/pull-requests/7":
            return 200, {"id": 7, "version": 3, "state": "OPEN"}
        return _ok(method, url)

    http = _install(monkeypatch, answer)
    driver = _dc_driver(tmp_path)
    http.calls.clear()

    result = driver.merge(fe.ChangeRef(7, f"{DC_BASE}/pr/7"))

    (post,) = http.writes()
    assert post.url.split("?")[0] == f"{DC_REST}/pull-requests/7/merge"
    assert "version=3" in post.url
    assert post.headers["authorization"] == _basic("admin", ADMIN_PASSWORD)
    assert (result["status_code"], result["merged"]) == (200, True)


@pytest.mark.parametrize(
    ("status", "state", "expected"),
    [(200, "MERGED", True), (200, "OPEN", False), (200, "DECLINED", False), (500, None, None)],
)
def test_dc_change_merged_reads_the_pull_request(
    status: int,
    state: str | None,
    expected: bool | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _dc_state(tmp_path, monkeypatch)

    def answer(method: str, url: str) -> tuple[int, Any]:
        if method == "GET" and url.split("?")[0] == f"{DC_REST}/pull-requests/7":
            return status, {"id": 7, "version": 3, "state": state}
        return _ok(method, url)

    _install(monkeypatch, answer)
    assert _dc_driver(tmp_path).change_merged(fe.ChangeRef(7, f"{DC_BASE}/pr/7")) is expected


# --- both drivers against a stubbed provider: CI gate, review, merge, redirects ----------

FORGES = ("bitbucket_cloud", "bitbucket_dc")
CHANGE = fe.ChangeRef(7, "https://example.invalid/pr/7")


class _Provider:
    """One pull request (#7) on a stubbed Bitbucket, in each forge's real shapes.

    Cloud: REST 2.0 pullrequests, commit statuses and merge. Data Center 9.4:
    pull-requests with ``fromRef.latestCommit``, build keys from the
    build-status listing and each build read with ``?key=``, a 204 on a build
    POST, and the merge at ``?version=``.
    """

    def __init__(self, forge: str) -> None:
        self.forge = forge
        self.commits = [SHA]
        self.state = "OPEN"
        self.statuses: dict[str, list[dict[str, Any]]] = {}
        self.comments: list[str] = []
        self.merge_status = 200

    def push(self, sha: str) -> None:
        self.commits.append(sha)

    def _pr(self) -> dict[str, Any]:
        if self.forge == "bitbucket_cloud":
            return {
                "id": 7,
                "state": self.state,
                "created_on": "2026-10-12T00:00:00+00:00",
                "source": {"commit": {"hash": self.commits[-1][:12]}},
            }
        return {
            "id": 7,
            "version": 3,
            "state": self.state,
            "createdDate": 1_760_000_000_000,
            "fromRef": {"latestCommit": self.commits[-1]},
        }

    def _set_status(self, sha: str, body: dict[str, Any]) -> None:
        kept = [s for s in self.statuses.get(sha, []) if s["key"] != body["key"]]
        self.statuses[sha] = [*kept, {"key": body["key"], "state": body["state"]}]

    def answer(self, http: _Http) -> Answer:
        def respond(method: str, url: str) -> tuple[Any, ...]:
            body = http.current.call.body
            if self.forge == "bitbucket_cloud":
                return self._cloud(method, url, body)
            return self._dc(method, url, body)

        return respond

    def _cloud(self, method: str, url: str, body: Any) -> tuple[Any, ...]:
        path, _, query = url.partition("?")
        rest = path.removeprefix(CLOUD_API)
        if method == "GET" and rest == "/pullrequests/7":
            return 200, self._pr()
        if method == "GET" and rest == "/pullrequests/7/commits":
            return 200, {"values": [{"hash": sha} for sha in self.commits]}
        if method == "GET" and rest.startswith("/commit/") and rest.count("/") == 2:
            sha = rest.split("/")[2]
            full = next(c for c in self.commits if c.startswith(sha))
            return 200, {"hash": full}
        if method == "POST" and rest.endswith("/statuses/build"):
            self._set_status(rest.split("/")[2], body)
            return 201, {"key": body["key"], "state": body["state"]}
        if method == "GET" and rest.endswith("/statuses"):
            return 200, {"values": self.statuses.get(rest.split("/")[2], [])}
        if method == "POST" and rest == "/pullrequests/7/comments":
            self.comments.append(body["content"]["raw"])
            return 201, {"id": 501, "links": {"html": {"href": "https://example.invalid/c/501"}}}
        if method == "POST" and rest == "/pullrequests/7/merge":
            if self.merge_status != 200:
                return self.merge_status, {"error": {"message": "stub"}}
            self.state = "MERGED"
            return 200, {**self._pr(), "merge_commit": {"hash": THIRD}}
        if method == "GET" and rest == "/pullrequests":
            listed = "state=MERGED" in query or self.state == "OPEN"
            return 200, {"values": [self._pr()] if listed else []}
        return 200, {"values": []}

    def _dc(self, method: str, url: str, body: Any) -> tuple[Any, ...]:
        path, _, query = url.partition("?")
        listing = f"{DC_BASE}/rest/build-status/latest/commits/"
        if method == "GET" and path.startswith(listing):
            values = self.statuses.get(path.removeprefix(listing), [])
            return 200, {"values": values, "isLastPage": True}
        rest = path.removeprefix(DC_REST)
        if method == "GET" and rest == "/pull-requests/7":
            return 200, self._pr()
        if method == "GET" and rest == "/pull-requests/7/commits":
            return 200, {"values": [{"id": sha} for sha in self.commits], "isLastPage": True}
        if method == "POST" and rest.endswith("/builds"):
            self._set_status(rest.split("/")[2], body)
            return 204, None
        if method == "GET" and rest.endswith("/builds"):
            key = urllib.parse.parse_qs(query)["key"][0]
            builds = self.statuses.get(rest.split("/")[2], [])
            return 200, next(b for b in builds if b["key"] == key)
        if method == "POST" and rest == "/pull-requests/7/comments":
            self.comments.append(body["text"])
            return 201, {"id": 501}
        if method == "POST" and rest == "/pull-requests/7/merge":
            if self.merge_status != 200:
                return self.merge_status, {"errors": [{"message": "stub"}]}
            self.state = "MERGED"
            return 200, {**self._pr(), "version": 4}
        if method == "GET" and rest == "/pull-requests":
            return 200, {"values": [self._pr()], "isLastPage": True}
        return 200, {"values": [], "isLastPage": True}


def _driver_on(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Any, _Provider, _Http]:
    """A driver on a stubbed provider; the WorkItem gains a request per review comment."""

    if forge == "bitbucket_cloud":
        _cloud_env(tmp_path, monkeypatch)
    else:
        _dc_state(tmp_path, monkeypatch)
    provider = _Provider(forge)
    http = _install(monkeypatch, lambda _m, _u: (200, None))
    http.answer = provider.answer(http)
    monkeypatch.setattr(_entry_module(forge), "WATCH_INTERVAL_SECONDS", 0.01)
    driver = _cloud_driver(tmp_path) if forge == "bitbucket_cloud" else _dc_driver(tmp_path)
    driver.p.evidence["work_item_id"] = "wi-1"

    def work_item_detail(work_item_id: str) -> dict[str, Any]:
        assert work_item_id == "wi-1"
        count = 1 + len(provider.comments)
        return {"requests": [{"id": f"req-{n}", "sequence": n} for n in range(1, count + 1)]}

    monkeypatch.setattr(driver.p, "work_item_detail", work_item_detail)
    return driver, provider, http


def _until(condition: Callable[[], bool], seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def _gate_posts(http: _Http, state: str) -> list[str]:
    return [
        call.url
        for call in http.writes()
        if isinstance(call.body, dict) and call.body.get("state") == state
    ]


def _gate_url(forge: str, sha: str) -> str:
    if forge == "bitbucket_cloud":
        return f"{CLOUD_API}/commit/{sha}/statuses/build"
    return f"{DC_REST}/commits/{sha}/builds"


@pytest.mark.parametrize("forge", FORGES)
def test_red_gate_is_one_failed_status_on_the_first_head_as_the_test_actor(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, _provider, http = _driver_on(forge, tmp_path, monkeypatch)
    head, _ = driver.change_head(CHANGE)
    http.calls.clear()

    red = driver.set_ci_red(head)
    driver.p.teardown.run()

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", _gate_url(forge, SHA))
    assert (post.body["key"], post.body["state"]) == (LOOP_GATE_KEY, "FAILED")
    test_actor = (
        f"Bearer {REVIEWER_TOKEN}"
        if forge == "bitbucket_cloud"
        else _basic("admin", ADMIN_PASSWORD)
    )
    assert post.headers["authorization"] == test_actor
    assert red == {"sha": SHA, "context": LOOP_GATE_KEY, "state": "failure"}


@pytest.mark.parametrize("forge", FORGES)
def test_set_ci_red_before_the_change_is_read_refuses_and_writes_nothing(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, _provider, http = _driver_on(forge, tmp_path, monkeypatch)

    with pytest.raises(fe.PreflightFailed, match="read its head first"):
        driver.set_ci_red(SHA)
    assert http.writes() == []


@pytest.mark.parametrize("forge", FORGES)
def test_watcher_posts_green_once_on_the_first_new_head_and_never_on_the_red_one(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on(forge, tmp_path, monkeypatch)
    head, _ = driver.change_head(CHANGE)
    driver.set_ci_red(head)

    # Many watcher reads of the unchanged head turn nothing green.
    assert _until(lambda: sum(c.method == "GET" for c in http.calls) > 20)
    assert _gate_posts(http, "SUCCESSFUL") == []

    provider.push(GREEN)
    assert _until(lambda: driver.green_heads == [GREEN])
    provider.push(THIRD)
    time.sleep(0.2)
    driver.p.teardown.run()

    assert _gate_posts(http, "SUCCESSFUL") == [_gate_url(forge, GREEN)]
    assert _gate_posts(http, "FAILED") == [_gate_url(forge, SHA)]
    assert driver.ci_state(GREEN)["state"] == "passing"
    assert driver.ci_state(SHA)["state"] == "failing"
    assert driver.ci_state(THIRD)["state"] == "none"


@pytest.mark.parametrize("forge", FORGES)
def test_ci_state_never_writes_even_on_a_later_head_with_no_status(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on(forge, tmp_path, monkeypatch)
    monkeypatch.setattr(_entry_module(forge), "WATCH_LIFETIME_SECONDS", 0.0)
    head, _ = driver.change_head(CHANGE)
    driver.set_ci_red(head)
    provider.push(GREEN)
    http.calls.clear()

    assert driver.ci_state(GREEN)["state"] == "none"
    assert driver.ci_state(SHA)["state"] == "failing"
    assert http.writes() == []
    # The watcher's lifetime had already run out, so it posted nothing either.
    assert driver.green_heads == []


@pytest.mark.parametrize("forge", FORGES)
@pytest.mark.parametrize("stop", ["teardown", "reset_fixture"])
def test_a_stopped_watcher_posts_nothing(
    forge: str, stop: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on(forge, tmp_path, monkeypatch)
    head, _ = driver.change_head(CHANGE)
    driver.set_ci_red(head)

    if stop == "teardown":
        driver.p.teardown.run()
    else:
        provider.state = "DECLINED"
        driver.reset_fixture()
    provider.push(GREEN)
    time.sleep(0.2)

    assert _gate_posts(http, "SUCCESSFUL") == []
    assert driver.green_heads == []


@pytest.mark.parametrize("forge", FORGES)
def test_review_is_a_top_level_comment_by_the_reviewer_proved_admitted(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on(forge, tmp_path, monkeypatch)

    review = driver.post_review(CHANGE, "Add a test.")

    (post,) = http.writes()
    if forge == "bitbucket_cloud":
        assert post.url == f"{CLOUD_API}/pullrequests/7/comments"
        assert "Add a test." in post.body["content"]["raw"]
        assert "inline" not in post.body and "parent" not in post.body
        assert post.headers["authorization"] == f"Bearer {REVIEWER_TOKEN}"
    else:
        assert post.url == f"{DC_REST}/pull-requests/7/comments"
        assert "Add a test." in post.body["text"]
        assert "anchor" not in post.body and "parent" not in post.body
        assert post.headers["authorization"] == _basic("reviewer", REVIEWER_PASSWORD)
    assert provider.comments and "Add a test." in provider.comments[0]
    assert (review["status_code"], review["comment_id"]) == (201, 501)
    assert review["delivery_id"] == "req-2"
    assert review["delivery_status_code"] == 200
    assert review["delivery_api_status"] == "factory_admitted"


@pytest.mark.parametrize("forge", FORGES)
def test_review_the_factory_never_admits_fails_instead_of_reporting_delivery(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, _provider, _http = _driver_on(forge, tmp_path, monkeypatch)
    monkeypatch.setattr(
        driver.p, "work_item_detail", lambda _id: {"requests": [{"id": "req-1", "sequence": 1}]}
    )
    driver.p.admission_timeout = 0

    with pytest.raises(fe.PreflightFailed, match="admit review comment 501"):
        driver.post_review(CHANGE, "Add a test.")


def test_cloud_accepted_merge_is_read_back_until_merged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on("bitbucket_cloud", tmp_path, monkeypatch)
    respond = http.answer

    def answer(method: str, url: str) -> tuple[Any, ...]:
        if method == "POST" and url.startswith(f"{CLOUD_API}/pullrequests/7/merge"):
            provider.state = "MERGED"
            return 202, None
        return respond(method, url)

    http.answer = answer

    result = driver.merge(CHANGE)

    assert (result["status_code"], result["merged"], result["accepted"]) == (200, True, 202)


@pytest.mark.parametrize("forge", FORGES)
@pytest.mark.parametrize(
    "location",
    ["https://attacker.example.invalid/steal", "same-origin"],
)
def test_every_redirect_is_refused_without_a_request_to_its_target(
    forge: str, location: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, http = _driver_on(forge, tmp_path, monkeypatch)
    pr = (
        f"{CLOUD_API}/pullrequests/7"
        if forge == "bitbucket_cloud"
        else f"{DC_REST}/pull-requests/7"
    )
    target = f"{pr}/moved" if location == "same-origin" else location
    respond = http.answer
    redirect = {"on": True}

    def answer(method: str, url: str) -> tuple[Any, ...]:
        if redirect["on"] and url == pr:
            return 302, None, {"Location": target}
        if url.startswith(target):
            return 200, {"id": 7, "state": "MERGED"}
        return respond(method, url)

    http.answer = answer

    assert driver.change_merged(CHANGE) is None
    assert [call.url for call in http.calls] == [pr]
    assert target not in [call.url for call in http.calls]

    # The neighbor: the same read answered 200 goes through.
    redirect["on"] = False
    provider.state = "MERGED"
    assert driver.change_merged(CHANGE) is True


def _loop_obs(driver: Any, provider: _Provider, *, fix: bool) -> dict[str, Any]:
    """Observations of the forge loop, in run_loop's order, from the driver alone.

    The factory side is played by the provider: it pushes the CI fix (when
    ``fix``) while the loop waits, and the review revision after the comment.
    The WorkItem fields come from the platform, not the driver, so they hold.
    """

    head, commits = driver.change_head(CHANGE)
    obs: dict[str, Any] = {
        "change_number": CHANGE.number,
        "head_initial": head,
        "commits_initial": commits,
        "red_ci": driver.set_ci_red(head),
    }
    if fix:
        provider.push(GREEN)
        assert _until(lambda: driver.green_heads == [GREEN])
    obs["first_run_statuses"] = ["completed"]
    obs["head_after_ci"], obs["commits_after_ci"] = driver.change_head(CHANGE)
    obs["ci_after_fix"] = driver.ci_state(str(obs["head_after_ci"]))
    review = driver.post_review(CHANGE, "Add a test.")
    obs.update({f"review_{key}": value for key, value in review.items()})
    provider.push(THIRD)
    obs["request_statuses"] = ["completed", "completed"]
    obs["head_after_review"], obs["commits_after_review"] = driver.change_head(CHANGE)
    obs["work_item_change_number"] = CHANGE.number
    obs["default_branch_moved_before_merge"] = False
    obs["merge"] = driver.merge(CHANGE)
    obs["forge_merged"] = driver.change_merged(CHANGE)
    obs["work_item_pr_status"] = obs["cli_pr_status"] = "merged"
    obs["change_numbers"] = driver.changes_opened()
    driver.p.teardown.run()
    return obs


@pytest.mark.parametrize("forge", FORGES)
def test_a_whole_loop_of_driver_observations_passes_the_loop_judge(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, _http = _driver_on(forge, tmp_path, monkeypatch)

    obs = _loop_obs(driver, provider, fix=True)

    assert fe.judge_github_loop(obs) == []
    assert obs["ci_after_fix"]["sha"] == GREEN


@pytest.mark.parametrize("forge", FORGES)
def test_the_loop_judge_fails_driver_observations_without_a_ci_fix_or_a_merge(
    forge: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, provider, _http = _driver_on(forge, tmp_path, monkeypatch)
    provider.merge_status = 409

    failures = fe.judge_github_loop(_loop_obs(driver, provider, fix=False))

    assert "no CI fix commit: the head did not move after the red CI" in failures
    assert "checks on the CI fix head read 'failing', expected 'passing'" in failures
    assert "the test actor's merge failed (HTTP 409)" in failures
    assert "the forge does not read the pull request as merged" in failures


# --- acceptance entries ------------------------------------------------------------------


def _forbid_drivers(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a dry run constructs no driver and launches nothing")

    fe.load_forge_entries()
    for name in ("jira_cloud", *DRIVERS):
        entry = fe.FORGES[name]
        monkeypatch.setitem(fe.FORGES, name, dataclasses.replace(entry, driver=boom))
    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    monkeypatch.setattr(fe, "Preflight", boom)
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", boom)


@pytest.mark.parametrize("key", PAIRINGS)
def test_bitbucket_pairing_dry_run_lists_every_step_and_passes(
    key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_drivers(monkeypatch)
    pairing = acceptance.PAIRINGS[key]
    assert pairing.installed is True
    assert acceptance.unset_fields(pairing) == []

    out = io.StringIO()
    assert acceptance.dry_run(key, out) == 0

    text = out.getvalue()
    assert "  installed\n" in text
    assert "not installed" not in text
    assert "unset:" not in text
    tracker, code_host = key.split("+")
    assert f"tracker {tracker}, code host {code_host}" in text
    for step in pairing.steps:
        assert step.name in text
        assert " ".join(step.command) in text
    assert ["run", "--scenario", "forge-loop", "--forge", code_host] in [
        step.command[:5] for step in pairing.steps
    ]


@pytest.mark.parametrize("key", PAIRINGS)
def test_main_dry_run_of_a_bitbucket_pairing_exits_zero(
    key: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_drivers(monkeypatch)
    assert fe.main(["acceptance", "--pairing", key, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "not installed" not in out
    for step in acceptance.PAIRINGS[key].steps:
        assert step.name in out


def test_the_bitbucket_self_pairings_are_gone() -> None:
    assert "bitbucket_cloud+bitbucket_cloud" not in acceptance.PAIRINGS
    assert "bitbucket_dc+bitbucket_dc" not in acceptance.PAIRINGS
