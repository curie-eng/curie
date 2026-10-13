"""Unit contract for the GitLab loop driver (tools/factory-e2e/forges/gitlab.py, #4399).

The live `gitlab+gitlab@fixture` run needs a GitLab CE fixture, a cluster and a
real model, and `gitlab+gitlab@saas` needs gitlab.com test identities, so both
are proved by running them. These pin, without a network, the request each
driver step sends on both targets (AC 12, decision 26).

The production contract the fakes below rely on:

1. ``forges/gitlab.py`` registers ``ForgeEntry(name="gitlab", installed=True,
   driver=<factory>, setup=install_gitlab)``. The factory reads the target from
   ``p.evidence["forge_target"]``, which main records from the new
   ``run --forge-target <fixture|saas>`` flag (``args.forge_target``), and
   returns ``GitLabDriver(p, target=<target>)``; no target, or another one, is
   a ``fe.ConfigError`` naming ``--forge-target``. ``TARGETS == ("fixture",
   "saas")``.
2. ``GitLabDriver``: ``forge == "gitlab"``, ``pairing ==
   "gitlab+gitlab@<target>"`` (the acceptance key), ``target``, and every
   ForgeDriver method (forges/base.py).
3. Target ``fixture``: the fixture's ``state.json`` path comes from
   ``CURIE_GITLAB_FIXTURE`` (the schema in tools/forge-fixtures/gitlab/README.md).
   Construction records ``p.evidence["gitlab_fixture"]["state_dir"]`` (the
   directory holding state.json) so acceptance can check the fixture is gone.
   Every request goes to the fixture's ``api_url`` or ``local_api_url``, with
   the root token from ``root_token_file`` in ``PRIVATE-TOKEN`` and never an
   ``Authorization`` header; it acts as ``developer`` (opens and labels the
   ticket, posts the diff comment) and as ``reporter`` (the reporter label
   probe) with ``Sudo: <username>``, and merges as root with no ``Sudo``
   header. TLS verifies with an ``ssl.SSLContext`` that loads the fixture CA
   (``ca_path``) and requires a certificate.
4. Target ``saas``: ``SAAS_VARS == ("CURIE_FACTORY_GITLAB_PROJECT",
   "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE",
   "CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE")``. Construction fails with
   ``fe.ConfigError`` naming ``missing_saas_env(os.environ)`` before any
   request. Requests go to ``https://gitlab.com/api/v4``; the human token
   opens, labels, reviews, sets the red status and merges; no request carries
   ``Sudo``. The project path is resolved once by ``GET
   /projects/<url-encoded path>``; every other request names the numeric id.
   There is no reporter probe.
5. Requests go through ``urllib.request`` (``urlopen`` or an opener built with
   ``HTTPSHandler``), so the stub below answers at the HTTPS handler.
6. Pure helpers: ``missing_saas_env(environ) -> str | None`` (the first unset or
   blank name in ``SAAS_VARS`` order); ``gitlab_ci_state(statuses) -> str``
   over ``GET /projects/:id/repository/commits/:sha/statuses`` items
   (``none``, ``failing``, ``pending`` or ``passing``; ``failed`` with
   ``allow_failure`` does not fail, ``skipped`` and ``manual`` do not hold it
   pending); and request builders returning ``(method, path under /api/v4,
   body)``: ``label_request(project_id, iid, label)``,
   ``status_request(project_id, sha, state)``, ``diff_comment_request(project_id,
   mr_iid, body, *, diff_refs, path, line)`` and ``merge_request(project_id,
   mr_iid)``.
7. ``set_ci_red(sha)`` posts ``status_request(pid, sha, "failed")`` and returns
   ``state == "failure"``, ``context == LOOP_GATE_NAME``. ``ci_state(sha)``
   reads the commit statuses and returns ``{"sha", "state"}``, ``unreadable``
   when the read fails. ``post_review`` posts a diff comment positioned on the
   merge request's ``diff_refs`` and a path from its diff, then waits for the
   WorkItem to show a new request and reports ``delivery_status_code == 200``,
   ``delivery_api_status == "factory_admitted"``.
8. ``tracker_evidence(change, work_item_id)`` returns ``closes_issue`` (the
   merge request description carries ``Closes #<iid>``) and ``thread_replies``
   (factory notes in the review discussion), and on the fixture
   ``reporter_label_admitted`` and ``reporter_state_labels`` from a second
   issue labelled by ``reporter``. ``judge_tracker`` is empty for True, 1, and
   on the fixture False and [], and names each shortfall otherwise.
9. ``install_gitlab(p)`` runs ``curie factory quickstart --code-host gitlab
   --tracker gitlab`` with ``--repo``, ``--token-file`` (the factory token
   file), and on the fixture ``--forge-url <base_url>`` and ``--ca-file
   <ca_path>``; no token value in argv or the environment.
10. ``install_values(..., forge="gitlab")`` writes no GitHub App entries.
"""

from __future__ import annotations

import dataclasses
import email.message
import io
import json
import logging
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SHA = "c" * 40
HEAD_RED = "a" * 40
HEAD_FIX = "b" * 40
BASE_SHA = "9" * 40
MERGE_SHA = "e" * 40

FIXTURE_PID = 4101
FIXTURE_PATH = "factory-fixture/nested/widgets"
FIXTURE_HOST = "gitlab-fixture.test"
FIXTURE_PORT = 28443
FIXTURE_BRIDGE = "172.18.0.0/24"
_HOST_EGRESS = {"cidr": "192.0.2.10/32", "ports": [{"protocol": "TCP", "port": FIXTURE_PORT}]}
# The factory block `curie factory quickstart --code-host gitlab` leaves on the release.
QUICKSTART_VALUES: dict[str, Any] = {
    "factory": {
        "enabled": True,
        "codeHosts": [{"kind": "gitlab", "name": "gitlab", "egress": [_HOST_EGRESS]}],
        "trackers": [{"kind": "gitlab", "name": "gitlab", "egress": [_HOST_EGRESS]}],
    }
}
SAAS_PID = 58123
SAAS_PROJECT = "curie-test-group/factory/widgets"
MR_IID = 7
CA_SUBJECT = "factory-e2e gitlab test ca"

# Distinctive so any leak into evidence, logs, reprs or errors is a substring hit.
ROOT_TOKEN = "gitlab-root-fixture-token-4399-r8t1"
SERVICE_TOKEN = "gitlab-sa-fixture-token-4399-s2v5"
REPORTER_TOKEN = "gitlab-reporter-fixture-token-4399-p0q3"
HUMAN_TOKEN = "gitlab-human-saas-token-4399-h3m9"
FACTORY_TOKEN = "gitlab-factory-saas-token-4399-f6k2"
SECRETS = (ROOT_TOKEN, SERVICE_TOKEN, REPORTER_TOKEN, HUMAN_TOKEN, FACTORY_TOKEN)

SAAS_VARS = (
    "CURIE_FACTORY_GITLAB_PROJECT",
    "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE",
    "CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE",
)
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
    "tracker_evidence",
    "judge_tracker",
    "teardowns",
)
USERS = {
    "root": {"id": 1, "username": "root"},
    "developer": {"id": 11, "username": "developer"},
    "reporter": {"id": 12, "username": "reporter"},
    "factory-bot": {"id": 13, "username": "factory-bot", "bot": True},
    "human": {"id": 21, "username": "human-owner"},
    "factory": {"id": 22, "username": "factory-dev"},
}
DIFF_PATH = "unitconv/units.py"
DIFF = "@@ -1,2 +1,3 @@\n LENGTH = {}\n+NAUTICAL_MILE = 1852\n UNITS = []\n"


def _no_secret(text: str, where: str) -> None:
    for secret in SECRETS:
        assert secret not in text, f"a GitLab token leaked into {where}"


def _gitlab_module() -> Any:
    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.gitlab"]


# --- harness scaffolding (copied: tests import by path, not from each other) -------


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


class _Clock:
    """time.time/monotonic that advance on sleep, so every wait ends at once."""

    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def time(self) -> float:
        self.now += 0.01
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(float(seconds), 0.01)


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """No real network, no proxy rewriting and no real waiting in any test here."""

    def no_network(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a test reached the real network")

    for name in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(urllib.request.HTTPSHandler, "https_open", no_network)
    monkeypatch.setattr(urllib.request.HTTPHandler, "http_open", no_network)
    monkeypatch.setattr(urllib.request, "_opener", None)
    clock = _Clock()
    monkeypatch.setattr(time, "time", clock.time)
    monkeypatch.setattr(time, "monotonic", clock.time)
    monkeypatch.setattr(time, "sleep", clock.sleep)


@pytest.fixture(scope="module")
def fixture_ca(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A real throwaway CA, so a context that loads it lists its subject."""

    work = tmp_path_factory.mktemp("ca")
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(work / "ca.key"),
            "-out",
            str(work / "ca.crt"),
            "-days",
            "2",
            "-subj",
            f"/CN={CA_SUBJECT}",
        ],
        check=True,
        capture_output=True,
    )
    return work / "ca.crt"


# --- the HTTPS stub ----------------------------------------------------------------------


@dataclasses.dataclass
class _Call:
    method: str
    url: str
    headers: dict[str, str]
    body: Any
    context: Any

    @property
    def path(self) -> str:
        return urllib.parse.urlsplit(self.url).path

    @property
    def host(self) -> str:
        return urllib.parse.urlsplit(self.url).netloc


class _Response:
    def __init__(self, status: int, payload: Any, extra: Mapping[str, str] | None = None) -> None:
        self.status = status
        self.code = status
        self.reason = self.msg = "stub"
        self.headers = email.message.Message()
        self.headers["Content-Type"] = "application/json"
        for key, value in (extra or {}).items():
            self.headers[key] = value
        self._data = b"" if payload is None else json.dumps(payload).encode()

    def read(self, *_a: Any) -> bytes:
        return self._data

    def getcode(self) -> int:
        return self.status

    def info(self) -> email.message.Message:
        return self.headers

    def geturl(self) -> str:
        return ""

    def close(self) -> None:
        return None

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def _decode(request: urllib.request.Request) -> Any:
    data = request.data
    if not isinstance(data, bytes) or not data:
        return None
    try:
        return json.loads(data)
    except ValueError:
        pairs = urllib.parse.parse_qs(data.decode(), keep_blank_values=True)
        return {key: values[-1] for key, values in pairs.items()}


def _install(monkeypatch: pytest.MonkeyPatch, answer: Callable[[_Call], tuple[Any, ...]]) -> list:
    """Answer at the HTTPS handler under urlopen or the driver's own opener.

    ``answer`` returns ``(status, payload)`` or ``(status, payload, headers)``.
    """

    calls: list[_Call] = []

    class _StubHttps(urllib.request.HTTPSHandler):
        def https_open(self, req: urllib.request.Request) -> Any:
            call = _Call(
                req.get_method(),
                req.full_url,
                {key.lower(): value for key, value in req.header_items()},
                _decode(req),
                getattr(self, "_context", None),
            )
            calls.append(call)
            status, payload, *extra = answer(call)
            if status >= 400:
                raise urllib.error.HTTPError(
                    call.url,
                    status,
                    "stub",
                    email.message.Message(),
                    io.BytesIO(json.dumps(payload or {}).encode()),
                )
            return _Response(status, payload, extra[0] if extra else None)

    monkeypatch.setattr(urllib.request, "HTTPSHandler", _StubHttps)
    monkeypatch.setattr(urllib.request, "_opener", None)
    return calls


# --- the in-process GitLab and platform ------------------------------------------------


def _labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [part for part in str(value).split(",") if part]


class FakeGitLab:
    """GitLab REST v4 for one project, plus the factory's reaction to a label.

    A trigger label added by ``developer`` (fixture) or ``human`` (saas) admits
    one WorkItem. A label from ``reporter`` admits nothing unless
    ``admit_reporter`` is set. A diff comment by the labeler opens a review
    discussion that the factory answers ``reply_count`` times.
    """

    def __init__(self, *, target: str, label: str) -> None:
        self.target = target
        self.label = label
        self.pid = FIXTURE_PID if target == "fixture" else SAAS_PID
        self.path = FIXTURE_PATH if target == "fixture" else SAAS_PROJECT
        self.factory_user = "factory-bot" if target == "fixture" else "factory"
        self.issues: dict[int, dict[str, Any]] = {}
        self.next_iid = 3
        self.label_events: list[tuple[int, str, str]] = []
        self.work_items: list[tuple[str, int]] = []
        self.statuses: dict[str, list[dict[str, Any]]] = {}
        self.discussions: list[dict[str, Any]] = []
        self.mr: dict[str, Any] = {"sha": HEAD_RED, "state": "opened", "commits": 1}
        self.merged_by: str | None = None
        self.bad_auth: list[str] = []
        # Knobs for the negative cases.
        self.admit_reporter = False
        self.reporter_state_label = False
        self.reply_count = 1
        self.closes = True
        self.fail_statuses_read = False
        self.refuse_all = False

    # -- identity --

    def actor(self, call: _Call) -> str | None:
        if "authorization" in call.headers:
            self.bad_auth.append(call.url)
        token = call.headers.get("private-token")
        if self.target == "fixture":
            if token != ROOT_TOKEN:
                return None
            return call.headers.get("sudo") or "root"
        if "sudo" in call.headers:
            return None
        return {HUMAN_TOKEN: "human", FACTORY_TOKEN: "factory"}.get(str(token))

    def labeler(self) -> str:
        return "developer" if self.target == "fixture" else "human"

    # -- views --

    def project(self) -> dict[str, Any]:
        return {
            "id": self.pid,
            "path_with_namespace": self.path,
            "default_branch": "main",
            "web_url": f"https://example.invalid/{self.path}",
        }

    def first_iid(self) -> int | None:
        return min(self.issues) if self.issues else None

    def mr_view(self) -> dict[str, Any]:
        first = self.first_iid()
        description = "Adds the nautical mile."
        if self.closes and first is not None:
            description += f"\n\nCloses #{first}"
        return {
            "id": 9001,
            "iid": MR_IID,
            "project_id": self.pid,
            "sha": self.mr["sha"],
            "state": self.mr["state"],
            "description": description,
            "source_branch": "curie/nautical-mile",
            "target_branch": "main",
            "web_url": f"https://example.invalid/{self.path}/-/merge_requests/{MR_IID}",
            "diff_refs": {"base_sha": BASE_SHA, "start_sha": BASE_SHA, "head_sha": self.mr["sha"]},
            "merge_commit_sha": MERGE_SHA if self.mr["state"] == "merged" else None,
            "created_at": "2027-01-15T08:00:00Z",
        }

    def note(self, author: str, body: str, note_id: int) -> dict[str, Any]:
        return {"id": note_id, "body": body, "author": USERS[author], "system": False}

    # -- the router --

    def __call__(self, call: _Call) -> tuple[int, Any]:
        actor = self.actor(call)
        if actor is None or self.refuse_all:
            return 401, {"message": "401 Unauthorized"}
        path = call.path
        if not path.startswith("/api/v4/"):
            return 404, {"message": "404 Not Found"}
        segments = path[len("/api/v4/") :].split("/")
        method = call.method
        if segments == ["version"]:
            return 200, {"version": "19.4.1"}
        if segments == ["user"]:
            return 200, USERS[actor]
        if segments[0] != "projects" or len(segments) < 2:
            return (200, []) if method == "GET" else (201, {})
        ident = urllib.parse.unquote(segments[1])
        if ident != str(self.pid):
            if method == "GET" and len(segments) == 2 and ident == self.path:
                return 200, self.project()
            return 404, {"message": "404 Project Not Found"}
        return self.project_route(method, segments[2:], call, actor)

    def project_route(
        self, method: str, rest: list[str], call: _Call, actor: str
    ) -> tuple[int, Any]:
        body = call.body if isinstance(call.body, dict) else {}
        if not rest:
            return 200, self.project()
        if rest == ["issues"] and method == "POST":
            iid = self.next_iid
            self.next_iid += 1
            self.issues[iid] = {
                "iid": iid,
                "project_id": self.pid,
                "title": body.get("title"),
                "description": body.get("description"),
                "labels": _labels(body.get("labels")),
                "author": actor,
                "state": "opened",
            }
            return 201, self.issue_view(iid)
        if rest[0] == "issues" and len(rest) >= 2:
            iid = int(rest[1])
            if iid not in self.issues:
                return 404, {"message": "404 Issue Not Found"}
            if len(rest) == 2 and method == "PUT":
                return self.edit_issue(iid, body, actor)
            if len(rest) == 2 and method == "GET":
                return 200, self.issue_view(iid)
            return (200, []) if method == "GET" else (201, {})
        if rest == ["merge_requests"] and method == "GET":
            return 200, [self.mr_view()]
        if rest[0] == "merge_requests" and len(rest) >= 2:
            if rest[1] != str(MR_IID):
                return 404, {"message": "404 Not Found"}
            return self.mr_route(method, rest[2:], body, actor)
        if rest[0] == "statuses" and len(rest) == 2 and method == "POST":
            entry = {
                "id": 700 + sum(len(v) for v in self.statuses.values()),
                "sha": rest[1],
                "status": body.get("state"),
                "name": body.get("name"),
                "allow_failure": False,
                "author": USERS[actor],
                "actor": actor,
                "body": body,
            }
            self.statuses.setdefault(rest[1], []).append(entry)
            return 201, {k: v for k, v in entry.items() if k not in ("actor", "body")}
        if rest[:2] == ["repository", "commits"] and len(rest) == 4 and rest[3] == "statuses":
            if self.fail_statuses_read:
                return 500, {"message": "500 Internal Server Error"}
            listed = [
                {k: v for k, v in entry.items() if k not in ("actor", "body")}
                for entry in self.statuses.get(rest[2], [])
            ]
            return 200, listed
        if rest[:2] == ["repository", "branches"] and method == "GET":
            return 200, {"name": urllib.parse.unquote(rest[2]), "commit": {"id": BASE_SHA}}
        return (200, []) if method == "GET" else (201, {})

    def issue_view(self, iid: int) -> dict[str, Any]:
        issue = self.issues[iid]
        return {
            "iid": iid,
            "project_id": self.pid,
            "title": issue["title"],
            "description": issue["description"],
            "labels": list(issue["labels"]),
            "state": issue["state"],
            "author": USERS[issue["author"]],
            "web_url": f"https://example.invalid/{self.path}/-/issues/{iid}",
        }

    def edit_issue(self, iid: int, body: Mapping[str, Any], actor: str) -> tuple[int, Any]:
        issue = self.issues[iid]
        for label in _labels(body.get("add_labels")):
            if label not in issue["labels"]:
                issue["labels"].append(label)
            self.label_events.append((iid, label, actor))
            if label != self.label:
                continue
            if actor == self.labeler() or (actor == "reporter" and self.admit_reporter):
                self.work_items.append((f"wi-{iid}", iid))
            if actor == "reporter" and self.reporter_state_label:
                issue["labels"].append("curie-factory:in-progress")
        for label in _labels(body.get("remove_labels")):
            if label in issue["labels"]:
                issue["labels"].remove(label)
        if body.get("labels") is not None and "add_labels" not in body:
            issue["labels"] = _labels(body.get("labels"))
        if body.get("state_event") == "close":
            issue["state"] = "closed"
        return 200, self.issue_view(iid)

    def mr_route(
        self, method: str, rest: list[str], body: Mapping[str, Any], actor: str
    ) -> tuple[int, Any]:
        if not rest and method == "GET":
            return 200, self.mr_view()
        if rest == ["commits"] and method == "GET":
            return 200, [{"id": f"{n:040x}"} for n in range(self.mr["commits"])]
        if rest == ["diffs"] and method == "GET":
            return 200, [{"old_path": DIFF_PATH, "new_path": DIFF_PATH, "diff": DIFF}]
        if rest == ["changes"] and method == "GET":
            view = self.mr_view()
            view["changes"] = [{"old_path": DIFF_PATH, "new_path": DIFF_PATH, "diff": DIFF}]
            return 200, view
        if rest == ["discussions"] and method == "POST":
            n = len(self.discussions) + 1
            notes = [self.note(actor, str(body.get("body")), 100 * n)]
            notes += [
                self.note(self.factory_user, "Done: pushed the follow-up.", 100 * n + i + 1)
                for i in range(self.reply_count)
            ]
            discussion = {
                "id": f"d{n:039x}",
                "individual_note": False,
                "notes": notes,
                "actor": actor,
                "position": body.get("position"),
            }
            self.discussions.append(discussion)
            return 201, {k: v for k, v in discussion.items() if k not in ("actor",)}
        if rest == ["discussions"] and method == "GET":
            return 200, [
                {k: v for k, v in d.items() if k not in ("actor", "position")}
                for d in self.discussions
            ]
        if len(rest) == 2 and rest[0] == "discussions" and method == "GET":
            for d in self.discussions:
                if d["id"] == rest[1]:
                    return 200, {k: v for k, v in d.items() if k not in ("actor", "position")}
            return 404, {"message": "404 Not Found"}
        if rest == ["notes"] and method == "GET":
            return 200, [note for d in self.discussions for note in d["notes"]]
        if rest == ["merge"] and method == "PUT":
            self.mr["state"] = "merged"
            self.merged_by = actor
            return 200, self.mr_view()
        return (200, []) if method == "GET" else (201, {})


class FakePlatform:
    """The platform's work-items API, as Preflight.api reaches it."""

    def __init__(self, gitlab: FakeGitLab) -> None:
        self.gitlab = gitlab
        self.calls: list[tuple[str, str]] = []

    def _item(self, work_item_id: str, iid: int) -> dict[str, Any]:
        reviews = len(self.gitlab.discussions)
        return {
            "id": work_item_id,
            "tracker": {
                "kind": "gitlab",
                "host": "example.invalid",
                "scope_id": str(self.gitlab.pid),
                "issue_id": str(iid),
                "display_key": f"{self.gitlab.path}#{iid}",
            },
            "repository": {"kind": "gitlab", "project_id": str(self.gitlab.pid)},
            "state": "running",
            "pr": {"number": MR_IID, "url": "https://example.invalid/mr/7", "status": "open"},
            "requests": [
                {"id": f"req-{n}", "status": "completed", "sequence": n}
                for n in range(1, 2 + reviews)
            ],
        }

    def __call__(
        self, method: str, path: str, *, headers: Mapping[str, str], body: Any = None
    ) -> tuple[int, Any]:
        self.calls.append((method, path))
        route = path.split("?", 1)[0].rstrip("/")
        items = [self._item(wid, iid) for wid, iid in self.gitlab.work_items]
        if method == "GET" and route == "/work-items":
            return 200, {"items": items, "limit": 200, "truncated": False}
        if method == "GET" and route.startswith("/work-items/"):
            wanted = route.split("/")[2]
            for item in items:
                if item["id"] == wanted:
                    return 200, item
        return 404, {"code": "not_found"}


# --- rigs for both targets --------------------------------------------------------------


def _state_file(tmp_path: Path, ca: Path) -> Path:
    state_dir = tmp_path / "gitlab-state"
    state_dir.mkdir(parents=True)
    tokens = {}
    for name, value in (
        ("root", ROOT_TOKEN),
        ("service-account", SERVICE_TOKEN),
        ("reporter", REPORTER_TOKEN),
    ):
        (state_dir / f"token-{name}").write_text(value + "\n")
        tokens[name] = str(state_dir / f"token-{name}")
    base = f"https://{FIXTURE_HOST}:{FIXTURE_PORT}"
    local = f"https://localhost:{FIXTURE_PORT}"
    state = {
        "project": "curie-forge-gitlab-0a1b2c3d",
        "status": "up",
        "base_url": base,
        "local_base_url": local,
        "api_url": f"{base}/api/v4",
        "local_api_url": f"{local}/api/v4",
        "ca_path": str(ca),
        "advertise_host": FIXTURE_HOST,
        "port": FIXTURE_PORT,
        "docker_subnets": [FIXTURE_BRIDGE],
        "version": "19.4.1",
        "server_version": "19.4.1",
        "root_token_file": tokens["root"],
        "group": {"id": 4001, "path": "factory-fixture"},
        "subgroup": {"id": 4002, "path": "factory-fixture/nested"},
        "projects": {
            "widgets": {
                "id": FIXTURE_PID,
                "path": FIXTURE_PATH,
                "default_branch": "main",
                "http_url_to_repo": f"{base}/{FIXTURE_PATH}.git",
            },
            "renamable": {
                "id": 4102,
                "path": "factory-fixture/nested/renamable",
                "default_branch": "main",
            },
        },
        "users": {
            "root": USERS["root"],
            "developer": {**USERS["developer"], "access_level": 30},
            "reporter": {**USERS["reporter"], "access_level": 20},
            "service_account": {
                **USERS["factory-bot"],
                "access_level": 30,
                "kind": "group_service_account",
            },
        },
        "tokens": {
            "service_account": {
                "file": tokens["service-account"],
                "expires_at": "2027-02-14",
                "scopes": ["api", "write_repository"],
                "token_id": 31,
            },
            "reporter": {
                "file": tokens["reporter"],
                "expires_at": "2027-02-14",
                "scopes": ["api", "write_repository"],
                "token_id": 32,
            },
        },
        "runner": {"id": 5},
    }
    path = state_dir / "state.json"
    path.write_text(json.dumps(state))
    return path


def _saas_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    factory = tmp_path / "factory-token"
    factory.write_text(FACTORY_TOKEN + "\n")
    human = tmp_path / "human-token"
    human.write_text(HUMAN_TOKEN + "\n")
    values = {
        "CURIE_FACTORY_GITLAB_PROJECT": SAAS_PROJECT,
        "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE": str(factory),
        "CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE": str(human),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return values


@dataclasses.dataclass
class _Rig:
    p: fe.Preflight
    target: str
    gitlab: FakeGitLab
    platform: FakePlatform
    calls: list[_Call]
    state_path: Path | None = None

    def driver(self) -> Any:
        return _gitlab_module().GitLabDriver(self.p, target=self.target)

    def project_calls(self) -> list[_Call]:
        return [c for c in self.calls if "/projects/" in c.path]

    def writes(self) -> list[_Call]:
        return [c for c in self.calls if c.method != "GET"]


def _rig(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str, ca: Path | None = None
) -> _Rig:
    p = _preflight(tmp_path)
    p.api_url = "http://127.0.0.1:9"
    p.api_key = "platform-api-key"
    state_path = None
    for name in SAAS_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("CURIE_GITLAB_FIXTURE", raising=False)
    if target == "fixture":
        assert ca is not None
        state_path = _state_file(tmp_path, ca)
        monkeypatch.setenv("CURIE_GITLAB_FIXTURE", str(state_path))
    else:
        _saas_env(tmp_path / "saas", monkeypatch)
    gitlab = FakeGitLab(target=target, label=p.config.label)
    platform = FakePlatform(gitlab)
    p.api = platform  # type: ignore[method-assign]
    calls = _install(monkeypatch, gitlab)
    return _Rig(p, target, gitlab, platform, calls, state_path)


CHANGE = fe.ChangeRef(MR_IID, "https://example.invalid/mr/7")


def _whole_loop(rig: _Rig) -> tuple[Any, dict[str, Any]]:
    """Every driver step the loop takes, in its order; the tracker evidence."""

    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Add nmi", "Please add nmi.", time.time())
    rig.p.evidence["work_item_id"] = work_item_id
    driver.pin_fixture_base()
    head, _commits = driver.change_head(CHANGE)
    driver.set_ci_red(str(head))
    rig.gitlab.mr.update(sha=HEAD_FIX, commits=2)
    driver.ci_state(HEAD_RED)
    driver.post_review(CHANGE, "Please add a round trip unit test.")
    driver.merge(CHANGE)
    driver.change_merged(CHANGE)
    evidence = driver.tracker_evidence(CHANGE, work_item_id)
    return driver, evidence


def _target_rig(target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ca: Path) -> _Rig:
    return _rig(tmp_path, monkeypatch, target, ca if target == "fixture" else None)


# --- the entry and target selection ---------------------------------------------------


def test_the_gitlab_entry_is_installed_with_its_setup() -> None:
    gitlab = _gitlab_module()
    entry = fe.FORGES["gitlab"]
    assert entry.name == "gitlab"
    assert entry.installed is True
    assert entry.setup is gitlab.install_gitlab
    assert tuple(gitlab.TARGETS) == ("fixture", "saas")
    assert tuple(gitlab.SAAS_VARS) == SAAS_VARS


def test_the_gitlab_driver_implements_every_protocol_method() -> None:
    driver = _gitlab_module().GitLabDriver
    assert driver.forge == "gitlab"
    for method in DRIVER_METHODS:
        assert callable(getattr(driver, method, None)), method


@pytest.mark.parametrize("target", ["fixture", "saas"])
def test_the_entry_builds_the_driver_for_the_recorded_forge_target(
    target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _target_rig(target, tmp_path, monkeypatch, fixture_ca)
    rig.p.evidence["forge_target"] = target
    driver = fe.FORGES["gitlab"].driver(rig.p)
    assert isinstance(driver, _gitlab_module().GitLabDriver)
    assert driver.target == target
    assert driver.forge == "gitlab"
    assert driver.pairing == f"gitlab+gitlab@{target}"


@pytest.mark.parametrize("recorded", [None, "", "github", "gitlab.com"])
def test_the_entry_refuses_a_missing_or_unknown_forge_target(
    recorded: str | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    if recorded is not None:
        rig.p.evidence["forge_target"] = recorded
    with pytest.raises(fe.ConfigError, match="--forge-target"):
        fe.FORGES["gitlab"].driver(rig.p)
    assert rig.calls == []


@pytest.mark.parametrize("target", ["fixture", "saas"])
def test_run_takes_a_forge_target_for_the_gitlab_forge(target: str) -> None:
    fe.load_forge_entries()
    args = fe.parse_args(
        ["run", "--scenario", "forge-loop", "--forge", "gitlab", "--forge-target", target]
    )
    assert (args.scenario, args.forge, args.forge_target) == ("forge-loop", "gitlab", target)
    plain = fe.parse_args(["run", "--scenario", "forge-loop", "--forge", "github"])
    assert plain.forge_target is None


# --- saas configuration: fail fast, before any request ---------------------------------


def test_missing_saas_env_is_none_when_every_variable_is_set(tmp_path: Path) -> None:
    environ = dict.fromkeys(SAAS_VARS, "set")
    assert _gitlab_module().missing_saas_env(environ) is None


@pytest.mark.parametrize("index", range(len(SAAS_VARS)))
@pytest.mark.parametrize("how", ["unset", "empty", "blank"])
def test_missing_saas_env_names_the_one_unset_variable(index: int, how: str) -> None:
    environ = dict.fromkeys(SAAS_VARS, "set")
    name = SAAS_VARS[index]
    if how == "unset":
        del environ[name]
    else:
        environ[name] = "" if how == "empty" else "   "
    assert _gitlab_module().missing_saas_env(environ) == name


def test_missing_saas_env_names_the_first_unset_in_declared_order() -> None:
    missing = _gitlab_module().missing_saas_env
    assert missing({}) == "CURIE_FACTORY_GITLAB_PROJECT"
    assert missing({SAAS_VARS[0]: "g/p"}) == "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE"
    assert missing({SAAS_VARS[2]: "/f"}) == "CURIE_FACTORY_GITLAB_PROJECT"
    assert missing({SAAS_VARS[0]: "g/p", SAAS_VARS[2]: "/f"}) == SAAS_VARS[1]


@pytest.mark.parametrize("missing", SAAS_VARS)
def test_a_saas_driver_without_its_variable_fails_naming_it_before_any_request(
    missing: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    monkeypatch.delenv(missing)
    with pytest.raises(fe.ConfigError) as raised:
        rig.driver()
    message = str(raised.value)
    assert missing in message
    _no_secret(message, "the configuration error")
    assert rig.calls == []


def test_a_saas_driver_with_nothing_set_names_the_project_variable_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    for name in SAAS_VARS:
        monkeypatch.delenv(name)
    with pytest.raises(fe.ConfigError, match="CURIE_FACTORY_GITLAB_PROJECT"):
        rig.driver()
    assert rig.calls == []


# --- pure helpers ------------------------------------------------------------------------


def _status(state: str, *, allow_failure: bool = False, name: str = "test") -> dict[str, Any]:
    return {"name": name, "status": state, "allow_failure": allow_failure}


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([], "none"),
        ([_status("success")], "passing"),
        ([_status("success"), _status("success", name="factory-e2e/loop-gate")], "passing"),
        ([_status("failed")], "failing"),
        ([_status("success"), _status("failed", name="factory-e2e/loop-gate")], "failing"),
        ([_status("canceled")], "failing"),
        ([_status("failed"), _status("running", name="lint")], "failing"),
        ([_status("running")], "pending"),
        ([_status("pending")], "pending"),
        ([_status("created")], "pending"),
        ([_status("success"), _status("running", name="lint")], "pending"),
        ([_status("failed", allow_failure=True), _status("success", name="lint")], "passing"),
        ([_status("success"), _status("skipped", name="deploy")], "passing"),
        ([_status("success"), _status("manual", name="deploy")], "passing"),
    ],
)
def test_gitlab_ci_state_maps_commit_statuses(
    statuses: list[dict[str, Any]], expected: str
) -> None:
    assert _gitlab_module().gitlab_ci_state(statuses) == expected


def test_label_request_adds_the_label_on_the_numeric_project() -> None:
    method, path, body = _gitlab_module().label_request(FIXTURE_PID, 3, "curie-factory")
    assert (method, path) == ("PUT", f"/projects/{FIXTURE_PID}/issues/3")
    assert _labels(body["add_labels"]) == ["curie-factory"]
    assert "labels" not in body and "remove_labels" not in body


@pytest.mark.parametrize("state", ["failed", "success"])
def test_status_request_posts_the_loop_gate_on_the_sha(state: str) -> None:
    gitlab = _gitlab_module()
    method, path, body = gitlab.status_request(SAAS_PID, HEAD_RED, state)
    assert (method, path) == ("POST", f"/projects/{SAAS_PID}/statuses/{HEAD_RED}")
    assert body["state"] == state
    assert body["name"] == gitlab.LOOP_GATE_NAME


def test_diff_comment_request_positions_a_new_line_on_the_diff_refs() -> None:
    refs = {"base_sha": BASE_SHA, "start_sha": BASE_SHA, "head_sha": HEAD_FIX}
    method, path, body = _gitlab_module().diff_comment_request(
        FIXTURE_PID, MR_IID, "Please add a test.", diff_refs=refs, path=DIFF_PATH, line=2
    )
    assert (method, path) == (
        "POST",
        f"/projects/{FIXTURE_PID}/merge_requests/{MR_IID}/discussions",
    )
    assert body["body"] == "Please add a test."
    position = body["position"]
    assert position["position_type"] == "text"
    assert (position["base_sha"], position["start_sha"], position["head_sha"]) == (
        BASE_SHA,
        BASE_SHA,
        HEAD_FIX,
    )
    assert position["new_path"] == DIFF_PATH
    assert position["new_line"] == 2


def test_merge_request_puts_the_merge_on_the_numeric_project() -> None:
    method, path, _body = _gitlab_module().merge_request(SAAS_PID, MR_IID)
    assert (method, path) == ("PUT", f"/projects/{SAAS_PID}/merge_requests/{MR_IID}/merge")


# --- the fixture target -----------------------------------------------------------------


def test_the_fixture_driver_records_the_fixture_state_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    rig.driver()
    assert rig.state_path is not None
    recorded = rig.p.evidence["gitlab_fixture"]["state_dir"]
    assert Path(recorded) == rig.state_path.parent
    _no_secret(json.dumps(rig.p.evidence, default=str), "the run evidence")


def test_fixture_requests_go_to_the_fixture_with_the_root_token_and_its_ca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    _whole_loop(rig)

    assert rig.calls, "the driver made no GitLab request"
    hosts = {f"{FIXTURE_HOST}:{FIXTURE_PORT}", f"localhost:{FIXTURE_PORT}"}
    for call in rig.calls:
        assert call.host in hosts, call.url
        assert call.path.startswith("/api/v4/"), call.url
        assert call.headers.get("private-token") == ROOT_TOKEN, call.url
        assert "authorization" not in call.headers, call.url
        context = call.context
        assert isinstance(context, ssl.SSLContext), call.url
        assert context.verify_mode == ssl.CERT_REQUIRED
        subjects = [
            value
            for cert in context.get_ca_certs()
            for rdn in cert.get("subject", ())
            for _key, value in rdn
        ]
        assert CA_SUBJECT in subjects, "the fixture CA is not trusted"
    assert rig.gitlab.bad_auth == []


@pytest.mark.parametrize("target", ["fixture", "saas"])
@pytest.mark.parametrize("location", ["https://attacker.example.invalid/steal", "same-origin"])
def test_every_redirect_is_refused_without_a_request_to_its_target(
    target: str, location: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    """A 3xx is never followed: following it would copy PRIVATE-TOKEN (and a
    fixture actor's Sudo) to the Location, another origin or plain http."""

    rig = _target_rig(target, tmp_path, monkeypatch, fixture_ca)
    pid = rig.gitlab.pid
    redirected = (f"/projects/{pid}/merge_requests/{MR_IID}", f"/projects/{pid}/issues")
    moved: list[str] = []
    redirect = {"on": True}

    def answer(call: _Call) -> tuple[Any, ...]:
        if redirect["on"] and call.path.endswith(redirected):
            where = f"{call.url}/moved" if location == "same-origin" else location
            moved.append(where)
            return 302, None, {"Location": where}
        return rig.gitlab(call)

    calls = _install(monkeypatch, answer)
    driver = rig.driver()

    assert driver.change_merged(CHANGE) is None
    with pytest.raises(fe.PreflightFailed, match="HTTP 302"):
        driver.open_and_label_ticket("Add nmi", "Please add nmi.", time.time())

    originals = [c for c in calls if c.path.endswith(redirected)]
    assert len(moved) == 2 and len(originals) == 2, [c.url for c in calls]
    assert not {c.url for c in calls} & set(moved), "a redirect target was requested"
    assert all(c.headers.get("private-token") for c in originals), "the token rode the original"
    if target == "fixture":
        assert originals[1].headers.get("sudo") == "developer"

    # The neighbor: the same read answered 200 goes through.
    redirect["on"] = False
    assert driver.change_merged(CHANGE) in (True, False)


def test_fixture_requests_name_the_project_by_numeric_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    _whole_loop(rig)
    assert rig.project_calls()
    for call in rig.project_calls():
        segment = call.path.split("/projects/", 1)[1].split("/", 1)[0]
        assert segment == str(FIXTURE_PID), call.url


def test_fixture_opens_and_labels_the_ticket_as_developer_through_sudo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    work_item_id = rig.driver().open_and_label_ticket("Add nmi", "Please add nmi.", time.time())

    (iid,) = rig.gitlab.issues
    issue = rig.gitlab.issues[iid]
    assert issue["author"] == "developer"
    assert issue["title"] == "Add nmi"
    assert rig.gitlab.label_events == [(iid, rig.p.config.label, "developer")]
    for call in rig.writes():
        assert call.headers.get("sudo") == "developer", call.url
    label_puts = [c for c in rig.writes() if c.path.endswith(f"/issues/{iid}")]
    assert [c.method for c in label_puts] == ["PUT"]
    assert work_item_id == f"wi-{iid}"
    assert rig.p.evidence["work_item_id"] == work_item_id


def test_fixture_red_ci_is_a_failed_commit_status_on_the_first_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    gitlab = _gitlab_module()
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    red = rig.driver().set_ci_red(HEAD_RED)

    (posted,) = rig.gitlab.statuses[HEAD_RED]
    assert posted["status"] == "failed"
    assert posted["name"] == gitlab.LOOP_GATE_NAME
    (call,) = rig.writes()
    assert call.path == f"/api/v4/projects/{FIXTURE_PID}/statuses/{HEAD_RED}"
    assert red["sha"] == HEAD_RED
    assert red["state"] == "failure"
    assert red["context"] == gitlab.LOOP_GATE_NAME


def test_fixture_diff_comment_is_posted_as_developer_on_the_head_diff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    driver = rig.driver()
    driver.open_and_label_ticket("Add nmi", "Please add nmi.", time.time())
    rig.gitlab.mr.update(sha=HEAD_FIX, commits=2)
    review = driver.post_review(CHANGE, "Please add a round trip unit test.")

    (discussion,) = rig.gitlab.discussions
    assert discussion["actor"] == "developer"
    assert "Please add a round trip unit test." in discussion["notes"][0]["body"]
    position = discussion["position"]
    assert isinstance(position, dict), "the review is not a diff comment"
    assert position["position_type"] == "text"
    assert position["head_sha"] == HEAD_FIX
    assert position["base_sha"] == BASE_SHA
    assert position["start_sha"] == BASE_SHA
    assert position["new_path"] == DIFF_PATH
    assert int(position["new_line"]) >= 1
    assert review["delivery_status_code"] == 200
    assert review["delivery_api_status"] == "factory_admitted"


def test_fixture_merges_as_root_without_sudo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    driver = rig.driver()
    merged = driver.merge(CHANGE)

    assert rig.gitlab.merged_by == "root"
    (call,) = [c for c in rig.writes() if c.path.endswith("/merge")]
    assert call.method == "PUT"
    assert call.path == f"/api/v4/projects/{FIXTURE_PID}/merge_requests/{MR_IID}/merge"
    assert "sudo" not in call.headers
    assert merged["merged"] is True
    assert merged["status_code"] == 200
    assert driver.change_merged(CHANGE) is True


def test_fixture_reporter_probe_labels_as_reporter_and_admits_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    driver, evidence = _whole_loop(rig)

    actors = [
        actor for _iid, label, actor in rig.gitlab.label_events if label == rig.p.config.label
    ]
    assert actors == ["developer", "reporter"]
    reporter_puts = [
        c for c in rig.writes() if c.headers.get("sudo") == "reporter" and c.method == "PUT"
    ]
    assert reporter_puts, "no label was applied through Sudo: reporter"
    first = rig.gitlab.first_iid()
    assert evidence["closes_issue"] is True
    assert evidence["thread_replies"] == 1
    assert evidence["reporter_label_admitted"] is False
    assert evidence["reporter_state_labels"] == []
    assert driver.judge_tracker(evidence) == []
    assert first is not None and len(rig.gitlab.work_items) == 1


@pytest.mark.parametrize(
    ("knob", "value", "field"),
    [
        ("admit_reporter", True, "reporter_label_admitted"),
        ("reporter_state_label", True, "reporter_state_labels"),
        ("reply_count", 0, "thread_replies"),
        ("reply_count", 2, "thread_replies"),
        ("closes", False, "closes_issue"),
    ],
)
def test_fixture_tracker_evidence_and_judge_name_each_shortfall(
    knob: str,
    value: Any,
    field: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fixture_ca: Path,
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    setattr(rig.gitlab, knob, value)
    driver, evidence = _whole_loop(rig)
    failures = driver.judge_tracker(evidence)
    assert failures, f"{knob}={value!r} passed the tracker judge"
    assert len(failures) == 1, failures


def test_fixture_judge_fails_an_empty_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    driver = _rig(tmp_path, monkeypatch, "fixture", fixture_ca).driver()
    assert len(driver.judge_tracker({})) == 4


def test_fixture_ci_state_reads_the_commit_statuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    driver = rig.driver()
    assert driver.ci_state(HEAD_FIX)["state"] == "none"
    rig.gitlab.statuses[HEAD_FIX] = [_status("success")]
    result = driver.ci_state(HEAD_FIX)
    assert (result["sha"], result["state"]) == (HEAD_FIX, "passing")
    read = [c for c in rig.calls if c.method == "GET"][-1]
    assert read.path == f"/api/v4/projects/{FIXTURE_PID}/repository/commits/{HEAD_FIX}/statuses"
    assert rig.writes() == [], "ci_state wrote to GitLab"
    rig.gitlab.fail_statuses_read = True
    assert driver.ci_state(HEAD_FIX)["state"] == "unreadable"


def test_fixture_change_head_reads_the_merge_request_sha_and_commits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _rig(tmp_path, monkeypatch, "fixture", fixture_ca)
    driver = rig.driver()
    assert driver.change_head(CHANGE) == (HEAD_RED, 1)
    rig.gitlab.mr.update(sha=HEAD_FIX, commits=2)
    assert driver.change_head(CHANGE) == (HEAD_FIX, 2)


# --- the saas target ---------------------------------------------------------------------


def test_saas_requests_go_to_gitlab_com_and_never_use_sudo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    _whole_loop(rig)

    assert rig.calls
    for call in rig.calls:
        assert call.host == "gitlab.com", call.url
        assert call.path.startswith("/api/v4/"), call.url
        assert "sudo" not in call.headers, call.url
        assert "authorization" not in call.headers, call.url
        assert call.headers.get("private-token") in (HUMAN_TOKEN, FACTORY_TOKEN), call.url


def test_saas_resolves_the_project_path_once_then_uses_the_numeric_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    _whole_loop(rig)

    by_path = []
    for call in rig.project_calls():
        segment = urllib.parse.unquote(call.path.split("/projects/", 1)[1].split("/", 1)[0])
        if segment == str(SAAS_PID):
            continue
        assert segment == SAAS_PROJECT, call.url
        by_path.append(call)
    assert len(by_path) == 1, [c.url for c in by_path]
    (resolve,) = by_path
    assert resolve.method == "GET"
    assert resolve.path.rstrip("/").endswith(urllib.parse.quote(SAAS_PROJECT, safe=""))
    assert rig.calls.index(resolve) == min(rig.calls.index(c) for c in rig.project_calls())


def test_saas_human_token_labels_reviews_sets_red_ci_and_merges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    _whole_loop(rig)

    (iid,) = rig.gitlab.issues
    assert rig.gitlab.issues[iid]["author"] == "human"
    assert rig.gitlab.label_events == [(iid, rig.p.config.label, "human")]
    assert [d["actor"] for d in rig.gitlab.discussions] == ["human"]
    assert isinstance(rig.gitlab.discussions[0]["position"], dict)
    assert [s["actor"] for s in rig.gitlab.statuses[HEAD_RED]] == ["human"]
    assert rig.gitlab.statuses[HEAD_RED][0]["status"] == "failed"
    assert rig.gitlab.merged_by == "human"
    for call in rig.writes():
        assert call.headers.get("private-token") == HUMAN_TOKEN, call.url


def test_saas_has_no_reporter_probe_and_judges_without_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch, "saas")
    driver, evidence = _whole_loop(rig)

    assert len(rig.gitlab.issues) == 1, "the saas run opened a second (probe) issue"
    assert {actor for _iid, _label, actor in rig.gitlab.label_events} == {"human"}
    assert "reporter_label_admitted" not in evidence
    assert "reporter_state_labels" not in evidence
    assert evidence["closes_issue"] is True
    assert evidence["thread_replies"] == 1
    assert driver.judge_tracker(evidence) == []
    assert len(driver.judge_tracker({})) == 2


# --- credentials never leak ------------------------------------------------------------


@pytest.mark.parametrize("target", ["fixture", "saas"])
def test_tokens_ride_only_in_the_private_token_header_and_never_leak(
    target: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    fixture_ca: Path,
) -> None:
    caplog.set_level(logging.DEBUG)
    rig = _target_rig(target, tmp_path, monkeypatch, fixture_ca)
    driver, evidence = _whole_loop(rig)

    for call in rig.calls:
        _no_secret(call.url, "a request URL")
        _no_secret(json.dumps(call.body, default=str), "a request body")
    out = capsys.readouterr()
    for where, text in (
        ("the tracker evidence", json.dumps(evidence, default=str)),
        ("the run evidence", json.dumps(rig.p.evidence, default=str)),
        ("the driver repr", repr(driver)),
        ("stdout", out.out),
        ("stderr", out.err),
        ("the log", caplog.text),
    ):
        _no_secret(text, where)


@pytest.mark.parametrize("target", ["fixture", "saas"])
def test_a_gitlab_error_names_no_token(
    target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig = _target_rig(target, tmp_path, monkeypatch, fixture_ca)
    driver = rig.driver()
    rig.gitlab.refuse_all = True
    with pytest.raises(Exception) as raised:  # noqa: PT011 - any refusal, never a leak
        driver.open_and_label_ticket("Title", "Body", time.time())
    chain: list[BaseException] = []
    exc: BaseException | None = raised.value
    while exc is not None and exc not in chain:
        chain.append(exc)
        exc = exc.__cause__ or exc.__context__
    for item in chain:
        _no_secret(str(item), "the raised error")
        _no_secret(repr(item), "the raised error repr")


# --- install_gitlab: the quickstart, and no GitHub App values ------------------------------


class _Processes:
    """subprocess.run for helm, kubectl and curie; records every argv and env."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], Mapping[str, str] | None]] = []
        self.upgrades: list[dict[str, Any]] = []

    def __call__(self, argv: Any, *_a: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        argv = [str(a) for a in argv]
        self.calls.append((argv, kwargs.get("env")))
        name = Path(argv[0]).name
        if name == "helm" and "get" in argv and "values" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps(QUICKSTART_VALUES), "")
        if name == "helm" and "upgrade" in argv:
            self.upgrades.append(json.loads(Path(_flag(argv, "-f")[0]).read_text()))
        if name == "kubectl" and "config" in argv:
            return subprocess.CompletedProcess(argv, 0, "apiVersion: v1\nkind: Config\n", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    def quickstarts(self) -> list[tuple[list[str], Mapping[str, str] | None]]:
        return [(a, e) for a, e in self.calls if "factory" in a and "quickstart" in a]


def _flag(argv: list[str], flag: str) -> list[str]:
    return [argv[i + 1] for i, item in enumerate(argv[:-1]) if item == flag]


def _install_rig(
    target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ca: Path
) -> tuple[_Rig, _Processes]:
    rig = _target_rig(target, tmp_path, monkeypatch, ca)
    rig.p.evidence["forge"] = "gitlab"
    rig.p.evidence["forge_target"] = target
    rig.p.chart_dir = tmp_path / "charts" / "curie"
    rig.p.chart_dir.mkdir(parents=True)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    monkeypatch.setattr(rig.p, "build_runner_layer", lambda *_a, **_k: bundle)
    processes = _Processes()
    monkeypatch.setattr(subprocess, "run", processes)

    def no_popen(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("install_gitlab started a background process")

    monkeypatch.setattr(subprocess, "Popen", no_popen)
    monkeypatch.setattr(rig.p, "ensure_api", lambda: None)
    return rig, processes


@pytest.mark.parametrize("target", ["fixture", "saas"])
def test_install_gitlab_runs_the_gitlab_quickstart_with_token_files_only(
    target: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig, processes = _install_rig(target, tmp_path, monkeypatch, fixture_ca)
    _gitlab_module().install_gitlab(rig.p)

    (quickstart,) = processes.quickstarts()
    argv, env = quickstart
    assert argv[0] == rig.p.config.curie_bin
    assert argv.index("factory") < argv.index("quickstart")
    assert _flag(argv, "--context") == [rig.p.config.kube_context]
    assert _flag(argv, "--namespace") == [rig.p.namespace]
    assert _flag(argv, "--code-host") == ["gitlab"]
    assert _flag(argv, "--tracker") == ["gitlab"]
    if target == "fixture":
        state = json.loads(Path(str(rig.state_path)).read_text())
        assert _flag(argv, "--repo") == [FIXTURE_PATH]
        assert _flag(argv, "--token-file") == [state["tokens"]["service_account"]["file"]]
        assert _flag(argv, "--forge-url") == [state["base_url"]]
        assert _flag(argv, "--ca-file") == [state["ca_path"]]
    else:
        assert _flag(argv, "--repo") == [SAAS_PROJECT]
        assert _flag(argv, "--token-file") == [
            str(tmp_path / "saas" / "factory-token"),
        ]
        assert _flag(argv, "--forge-url") in ([], ["https://gitlab.com"])
        assert _flag(argv, "--ca-file") == []
    assert "--app-id" not in argv and "--private-key-file" not in argv
    for item in argv:
        _no_secret(item, "the quickstart argv")
    for value in (env or {}).values():
        _no_secret(str(value), "the quickstart environment")


def test_install_gitlab_on_the_fixture_also_allows_its_docker_bridge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    # A pod on the fixture's own node is DNATed to the container's bridge
    # address, so the publication policy must allow that subnet as well
    # (refused live: the push failed to connect with only the host /32).
    rig, processes = _install_rig("fixture", tmp_path, monkeypatch, fixture_ca)
    _gitlab_module().install_gitlab(rig.p)

    (upgrade,) = processes.upgrades
    bridge = {"cidr": FIXTURE_BRIDGE, "ports": [{"protocol": "TCP", "port": FIXTURE_PORT}]}
    host = QUICKSTART_VALUES["factory"]["codeHosts"][0]["egress"][0]
    for key in ("codeHosts", "trackers"):
        (entry,) = upgrade["factory"][key]
        assert entry["kind"] == "gitlab"
        assert entry["egress"] == [host, bridge]
    (upgrade_argv,) = [a for a, _ in processes.calls if a[0] == "helm" and "upgrade" in a]
    assert "--reuse-values" in upgrade_argv
    assert _flag(upgrade_argv, "--kube-context") == [rig.p.config.kube_context]


def test_install_gitlab_on_saas_does_not_touch_the_egress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    rig, processes = _install_rig("saas", tmp_path, monkeypatch, fixture_ca)
    _gitlab_module().install_gitlab(rig.p)
    assert processes.upgrades == []


def test_fixture_bridge_egress_keeps_other_kinds_and_refuses_a_missing_gitlab_host() -> None:
    gl = _gitlab_module()
    github = {"kind": "github", "egress": [{"cidr": "192.0.2.1/32", "ports": []}]}
    values = {"factory": {"codeHosts": [github, *QUICKSTART_VALUES["factory"]["codeHosts"]]}}
    out = gl.fixture_bridge_egress(values, [FIXTURE_BRIDGE], FIXTURE_PORT)
    assert out["factory"]["codeHosts"][0] == github
    assert len(out["factory"]["codeHosts"][1]["egress"]) == 2
    assert out["factory"]["trackers"] == []
    again = gl.fixture_bridge_egress(out, [FIXTURE_BRIDGE], FIXTURE_PORT)
    assert again["factory"]["codeHosts"] == out["factory"]["codeHosts"]
    with pytest.raises(fe.PreflightFailed, match="no gitlab code host"):
        gl.fixture_bridge_egress({"factory": {"codeHosts": [github]}}, ["10.0.0.0/24"], 1)
    with pytest.raises(fe.PreflightFailed, match="no factory values"):
        gl.fixture_bridge_egress({}, ["10.0.0.0/24"], 1)


def test_install_values_for_the_gitlab_forge_write_no_github_app(tmp_path: Path) -> None:
    config = _preflight(tmp_path).config
    values = fe.install_values(
        config,
        candidate=SHA,
        app_key_secret="factory-app",
        consumer_controller=True,
        forge="gitlab",
    )
    api = values.get("api") or {}
    for key in (
        "githubAppId",
        "githubAppExistingSecret",
        "githubWebhookSecret",
        "githubRepoAllowlist",
    ):
        assert key not in api, key
    factory = values.get("factory") or {}
    for section in ("codeHosts", "trackers"):
        for entry in factory.get(section) or []:
            assert entry.get("kind") != "github", (section, entry)
            assert (entry.get("credential") or {}).get("type") != "github_app", (section, entry)
    assert "factory-app" not in json.dumps(values)
    # The images are still the candidate's.
    assert values["api"]["image"]["tag"] == f"sha-{SHA}"


def test_install_values_default_still_write_the_github_app(tmp_path: Path) -> None:
    config = _preflight(tmp_path).config
    values = fe.install_values(
        config, candidate=SHA, app_key_secret="factory-app", consumer_controller=True
    )
    assert values["api"]["githubAppExistingSecret"] == "factory-app"
    kinds = {entry["kind"] for entry in values["factory"]["codeHosts"]}
    assert kinds == {"github"}


# --- restore_fixture_base: GitLab keeps the default branch's protection -----------------


class _ProtectingGitLab:
    """The branch and protection calls restore_fixture_base makes, answered as the
    GitLab 19.4.1 fixture answered them in a replay: a DELETE of the default
    branch's protection answers 204 but the rule stays, the recreated branch is
    protected again, and the parking branch is protected once it is the
    default. A POST for an existing rule answers 409 "already exists"."""

    RULE = {
        "name": "main",
        "push_access_levels": [{"access_level": 40}],
        "merge_access_levels": [{"access_level": 40}],
        "allow_force_push": False,
    }

    def __init__(self, kept: dict[str, Any] | None = None) -> None:
        self.branches = {"main": HEAD_FIX}
        self.default = "main"
        self.protected: dict[str, dict[str, Any]] = {"main": dict(kept or self.RULE)}
        self.writes: list[tuple[str, str]] = []

    def __call__(self, _actor: str, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        tail = path.split(f"/projects/{FIXTURE_PID}", 1)[1]
        if method != "GET":
            self.writes.append((method, tail))
        name = urllib.parse.unquote(tail.rsplit("/", 1)[-1])
        if tail.startswith("/repository/branches"):
            if method == "GET":
                sha = self.branches.get(name)
                return (200, {"name": name, "commit": {"id": sha}}) if sha else (404, None)
            if method == "POST":
                self.branches[body["branch"]] = body["ref"]
                if body["branch"] == "main":
                    self.protected.setdefault("main", dict(self.RULE))
                return 201, {"name": body["branch"]}
            if name == self.default:
                return 400, {"message": "Cannot remove HEAD branch"}
            self.branches.pop(name, None)
            return 204, None
        if tail.startswith("/protected_branches"):
            if method == "GET":
                rule = self.protected.get(name)
                return (200, rule) if rule else (404, None)
            if method == "DELETE":
                return 204, None  # the rule stays
            if body["name"] in self.protected:
                return 409, {"message": f"Protected branch '{body['name']}' already exists"}
            self.protected[body["name"]] = dict(self.RULE)
            return 201, self.protected[body["name"]]
        if method == "PUT" and tail == "":
            self.default = body["default_branch"]
            self.protected.setdefault(self.default, dict(self.RULE))
            return 200, {"default_branch": self.default}
        raise AssertionError(f"unexpected {method} {tail}")


def _restoring_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ca: Path, gitlab: _ProtectingGitLab
) -> Any:
    rig = _target_rig("fixture", tmp_path, monkeypatch, ca)
    driver = rig.driver()
    monkeypatch.setattr(driver, "_json", gitlab)
    driver._base_sha = BASE_SHA
    return driver


def test_restore_accepts_the_protection_gitlab_kept_and_moves_the_default_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    # Live run 10: re-protecting answered 409, the default stayed on the
    # parking branch, and the reset then failed deleting it with 400.
    gitlab = _ProtectingGitLab()
    driver = _restoring_driver(tmp_path, monkeypatch, fixture_ca, gitlab)

    assert driver.restore_fixture_base() == {"restored": True, "moved": True}

    assert gitlab.default == "main"
    assert gitlab.branches == {"main": BASE_SHA}
    assert ("PUT", "") in gitlab.writes and gitlab.writes[-1][0] == "DELETE"


def test_restore_refuses_a_kept_protection_that_differs_but_still_unparks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_ca: Path
) -> None:
    weaker = {**_ProtectingGitLab.RULE, "push_access_levels": [{"access_level": 30}]}
    gitlab = _ProtectingGitLab()
    driver = _restoring_driver(tmp_path, monkeypatch, fixture_ca, gitlab)
    # The original rule is Maintainer-only; the one GitLab keeps is weaker.
    real = gitlab.__call__

    def weaken(actor: str, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        result = real(actor, method, path, body)
        if method == "POST" and path.endswith("/repository/branches") and body["branch"] == "main":
            gitlab.protected["main"] = dict(weaker)
        return result

    monkeypatch.setattr(driver, "_json", weaken)

    with pytest.raises(fe.PreflightFailed, match="differs from the original"):
        driver.restore_fixture_base()

    assert gitlab.default == "main"
    assert "factory-e2e-park-" not in " ".join(gitlab.branches)
