"""Unit contract for the Bitbucket loop drivers and their acceptance entries (#4401).

`forges/bitbucket_cloud.py` and `forges/bitbucket_dc.py` implement the
ForgeDriver code host operations (forges/base.py) and register installed. The
ticket operations delegate to the Jira Cloud driver. These pin, without a
network:

1. Both entries register installed and implement every protocol method.
2. The Cloud driver reads its repository, both token files and the Jira inputs
   from the environment and fails naming every unset one (decision 18.6).
3. The request each code host operation sends: the red loop gate status on
   the first head, the green one on a later head that has none (decision
   18.1), the review comment as the reviewer, and the merge as the reviewer
   on Cloud and as the admin on the Data Center fixture (decision 18.2).
4. `jira_cloud+bitbucket_cloud` and `jira_cloud+bitbucket_dc` dry-run to exit 0
   and list every step without constructing a driver (decision 18.7).

The HTTP seam is `urllib.request.urlopen`: every driver request, including
those the fixture's helpers.py makes, goes through it, so the stub below sees
each one.
"""

from __future__ import annotations

import base64
import dataclasses
import email.message
import io
import json
import subprocess
import sys
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
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self.code = status
        self.reason = "stub"
        self.headers = email.message.Message()
        self.headers["Content-Type"] = "application/json"
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


Answer = Callable[[str, str], tuple[int, Any]]


class _Http:
    """Records every request; ``answer(method, url)`` gives (status, JSON payload)."""

    def __init__(self, answer: Answer) -> None:
        self.answer = answer
        self.calls: list[_Call] = []

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
        status, payload = self.answer(call.method, call.url)
        if status >= 400:
            raise urllib.error.HTTPError(
                call.url,
                status,
                "stub",
                email.message.Message(),
                io.BytesIO(json.dumps(payload or {}).encode()),
            )
        return _Response(status, payload)

    def writes(self) -> list[_Call]:
        return [call for call in self.calls if call.method != "GET"]


def _install(monkeypatch: pytest.MonkeyPatch, answer: Answer) -> _Http:
    http = _Http(answer)
    monkeypatch.setattr(urllib.request, "urlopen", http)
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


def test_cloud_red_gate_is_a_failed_build_status_on_the_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    result = driver.set_ci_red(SHA)

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", f"{CLOUD_API}/commit/{SHA}/statuses/build")
    assert post.body["key"] == LOOP_GATE_KEY
    assert post.body["state"] == "FAILED"
    assert post.headers["authorization"] in {f"Bearer {FACTORY_TOKEN}", f"Bearer {REVIEWER_TOKEN}"}
    assert isinstance(result, dict)


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
    driver.set_ci_red(SHA)
    http.calls.clear()

    assert driver.ci_state(SHA)["state"] == expected
    # The first head is never turned green by the driver.
    assert [call for call in http.writes() if (call.body or {}).get("state") == "SUCCESSFUL"] == []


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


def test_cloud_ci_state_posts_the_green_gate_on_a_later_head_with_no_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _cloud_driver(tmp_path)
    driver.set_ci_red(SHA)
    http.calls.clear()

    driver.ci_state(GREEN)

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", f"{CLOUD_API}/commit/{GREEN}/statuses/build")
    assert (post.body["key"], post.body["state"]) == (LOOP_GATE_KEY, "SUCCESSFUL")


def test_cloud_review_is_a_top_level_comment_by_the_reviewer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cloud_env(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _cloud_driver(tmp_path)
    http.calls.clear()

    result = driver.post_review(fe.ChangeRef(7, "https://example.invalid/pr/7"), "Add a test.")

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", f"{CLOUD_API}/pullrequests/7/comments")
    assert "Add a test." in post.body["content"]["raw"]
    assert "inline" not in post.body and "parent" not in post.body
    assert post.headers["authorization"] == f"Bearer {REVIEWER_TOKEN}"
    assert (result["status_code"], result["comment_id"]) == (201, 501)


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


def test_dc_red_gate_is_a_failed_build_on_the_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dc_state(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _dc_driver(tmp_path)
    http.calls.clear()

    driver.set_ci_red(SHA)

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", f"{DC_REST}/commits/{SHA}/builds")
    assert (post.body["key"], post.body["state"]) == (LOOP_GATE_KEY, "FAILED")


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
    driver.set_ci_red(SHA)
    http.calls.clear()

    assert driver.ci_state(SHA)["state"] == expected
    assert [call for call in http.writes() if (call.body or {}).get("state") == "SUCCESSFUL"] == []


def test_dc_ci_state_posts_the_green_gate_on_a_later_head_with_no_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dc_state(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _dc_driver(tmp_path)
    driver.set_ci_red(SHA)
    http.calls.clear()

    driver.ci_state(GREEN)

    (post,) = http.writes()
    assert (post.method, post.url) == ("POST", f"{DC_REST}/commits/{GREEN}/builds")
    assert (post.body["key"], post.body["state"]) == (LOOP_GATE_KEY, "SUCCESSFUL")


def test_dc_review_is_a_comment_by_the_reviewer_user(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dc_state(tmp_path, monkeypatch)
    http = _install(monkeypatch, _ok)
    driver = _dc_driver(tmp_path)
    http.calls.clear()

    result = driver.post_review(fe.ChangeRef(7, f"{DC_BASE}/pr/7"), "Add a test.")

    (post,) = http.writes()
    assert (post.method, post.url.split("?")[0]) == (
        "POST",
        f"{DC_REST}/pull-requests/7/comments",
    )
    assert "Add a test." in post.body["text"]
    assert "anchor" not in post.body and "parent" not in post.body
    assert post.headers["authorization"] == _basic("reviewer", REVIEWER_PASSWORD)
    assert (result["status_code"], result["comment_id"]) == (201, 501)


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
