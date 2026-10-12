"""Unit contract for the Jira Cloud forge entry (tools/factory-e2e/forges/jira_cloud.py).

The live `jira_cloud+github` acceptance run needs a Jira Cloud site, two Jira
accounts and the dedicated test GitHub App, so it is proved by running it.
These pin what must hold before then, against an in-process fake of the Jira
REST v3 API and the platform work-items API. No real credential, site or
network is touched.

The production contract the fakes below rely on:

1. Site facts come only from the env file named by CURIE_FACTORY_JIRA_ENV_FILE,
   one ``key=value`` per line with the keys ``site`` (a bare host),
   ``cloud_id``, ``project_key``, ``labeler_email``, ``labeler_token_file``,
   ``bot_email``, ``bot_account_id`` and ``bot_token_file``.
   ``load_site(environ)`` reads it and raises ``fe.ConfigError`` naming a
   missing key, never a value.
2. Every Jira request goes through ``fe.http_json(method, url, headers=...,
   body=...)`` to the API gateway,
   ``https://api.atlassian.com/ex/jira/<cloud_id>/rest/api/3/...`` (scoped
   tokens authenticate only there; the site host is only for browse links), with
   ``Authorization: Basic base64(<email>:<token>)``; the token is read from the
   token file. The platform is read through ``Preflight.api`` and the GitHub
   pull request through ``Preflight.as_actor``.
3. ``JiraCloudDriver(p, github=<GitHub driver>)``: ``forge == "jira_cloud"``,
   ``pairing == "jira_cloud+github"``; ``open_and_label_ticket`` creates the
   issue in ``project_key`` and adds ``p.config.label`` as the labeler, then
   proves exactly one WorkItem for that issue through ``GET /work-items`` and
   records it as ``p.evidence["work_item_id"]``; the pull request, CI, review,
   merge and fixture steps delegate to ``github``;
   ``tracker_evidence(change, work_item_id)`` returns ``key``,
   ``status_comments``, ``remote_links``, ``pr_body_has_key`` and
   ``bot_label_admitted``; ``judge_tracker`` returns one line per value other
   than 1, 1, True, False; ``teardowns()`` returns exactly
   ``(("close Jira test issues", close_test_issues),)``.
4. ``install_jira(p)`` is the entry's ``setup``: it refuses a release whose
   ``helm get values`` lacks the recorded GitHub code host ``existingSecret``,
   else runs ``curie factory quickstart --tracker jira-cloud --code-host github``
   in AddAgent mode with the bot token passed as ``--token-file``.
5. Pure helpers over the recorded responses in fixtures/jira/:
   ``bot_comment_count``, ``remote_link_count``, ``done_transition`` and
   ``jira_label_update(add=..., remove=...)``.
"""

from __future__ import annotations

import base64
import copy
import importlib.util
import json
import logging
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "jira"
SHA = "c" * 40

SITE = "example.atlassian.net"
CLOUD_ID = "11111111-1111-1111-1111-111111111111"
OTHER_SITE = "acme.atlassian.net"
OTHER_CLOUD_ID = "22222222-2222-2222-2222-222222222222"
PROJECT = "FAC"
LABELER_EMAIL = "labeler@example.com"
LABELER_ACCOUNT = "557058:00000000-0000-4000-8000-0000000000a1"
BOT_EMAIL = "factory-bot@example.com"
BOT_ACCOUNT = "712020:00000000-0000-4000-8000-00000000b07a"
# Distinctive so any leak into evidence, logs, reprs or errors is a substring hit.
LABELER_TOKEN = "jira-labeler-fixture-token-4358-q7w2"
BOT_TOKEN = "jira-bot-fixture-token-4358-k9p4"
REPO = "acme/fixture"
PR_NUMBER = 42
PR_URL = f"https://github.com/{REPO}/pull/{PR_NUMBER}"
CLOSE_STEP = "close Jira test issues"


def _basic(email: str, token: str) -> str:
    return "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()


LABELER_AUTH = _basic(LABELER_EMAIL, LABELER_TOKEN)
BOT_AUTH = _basic(BOT_EMAIL, BOT_TOKEN)
SECRETS = (
    LABELER_TOKEN,
    BOT_TOKEN,
    LABELER_AUTH.removeprefix("Basic "),
    BOT_AUTH.removeprefix("Basic "),
)


def _fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def _no_secret(text: str, where: str) -> None:
    for secret in SECRETS:
        assert secret not in text, f"a Jira credential leaked into {where}"


# --- harness scaffolding (copied: tests import by path, not from each other) -------


def _app_dir(tmp_path: Path) -> Path:
    app = tmp_path / "app"
    app.mkdir(parents=True)
    meta = {"id": 42, "slug": "example-factory", "installation_id": 7, "repo": REPO}
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    return app


def _preflight(tmp_path: Path, **kwargs: Any) -> fe.Preflight:
    env = {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(_app_dir(tmp_path)),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }

    def no_gh(user: str) -> str:
        raise AssertionError(user)

    config = fe.load_config(env, context=None, gh_token=no_gh)
    return fe.Preflight(
        config,
        repo_root=REPO_ROOT,
        candidate=SHA,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
        **kwargs,
    )


def _env_file(
    tmp_path: Path,
    *,
    site: str = SITE,
    cloud_id: str = CLOUD_ID,
    drop: str | None = None,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    labeler_file = tmp_path / "labeler-token"
    labeler_file.write_text(LABELER_TOKEN + "\n")
    bot_file = tmp_path / "bot-token"
    bot_file.write_text(BOT_TOKEN + "\n")
    facts = {
        "site": site,
        "cloud_id": cloud_id,
        "project_key": PROJECT,
        "labeler_email": LABELER_EMAIL,
        "labeler_token_file": str(labeler_file),
        "bot_email": BOT_EMAIL,
        "bot_account_id": BOT_ACCOUNT,
        "bot_token_file": str(bot_file),
    }
    if drop is not None:
        del facts[drop]
    path = tmp_path / "jira.env"
    path.write_text("".join(f"{key}={value}\n" for key, value in facts.items()))
    return path


def _jira_module() -> Any:
    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.jira_cloud"]


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
def _offline(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """No real network and no real waiting in any test here."""

    def no_network(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a test reached the real network")

    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    clock = _Clock()
    monkeypatch.setattr(time, "time", clock.time)
    monkeypatch.setattr(time, "monotonic", clock.time)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock


# --- the in-process Jira, platform and GitHub fakes ---------------------------------


class FakeJira:
    """Jira Cloud REST v3 for one site, plus the factory's reaction to a label.

    A label add by the labeler admits one WorkItem (the platform side), after
    which the bot owns one status comment carrying the ``curie.factory``
    property and, once a pull request exists, one remote link to it. A label
    add by the bot admits nothing unless ``admit_bot_labels`` is set.
    """

    def __init__(self, *, site: str = SITE, cloud_id: str = CLOUD_ID, label: str) -> None:
        self.site = site
        self.cloud_id = cloud_id
        self.label = label
        self.requests: list[dict[str, Any]] = []
        self.hosts: set[str] = set()
        self.issues: dict[str, dict[str, Any]] = {}
        self.label_events: list[tuple[str, str, str, str]] = []
        self.work_items: list[tuple[str, dict[str, Any]]] = []
        # Knobs for the negative cases.
        self.admit_bot_labels = False
        self.duplicate_admission = False
        self.extra_status_comment = False
        # Jira's per-page cap on comment reads; small values force paging.
        self.comment_page_size = 5000
        self.link_pull_request = True
        self.fail_label: set[str] = set()
        self.fail_close: set[str] = set()
        self.status_401 = False

    # -- HTTP --

    def __call__(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        timeout: float = 30,
    ) -> tuple[int, Any]:
        parsed = urllib.parse.urlsplit(url)
        assert parsed.scheme == "https", url
        path = parsed.path
        host = parsed.hostname or ""
        # Scoped API tokens answer only on the gateway: a site URL is a defect.
        assert host == "api.atlassian.com", f"Jira request off the API gateway: {url}"
        match = re.match(r"/ex/jira/([^/]+)(/.*)$", path)
        assert match, url
        assert match.group(1) == self.cloud_id, f"gateway URL for another cloud: {url}"
        path = match.group(2)
        self.hosts.add(host)
        lowered = {k.lower(): v for k, v in (headers or {}).items()}
        auth = lowered.get("authorization")
        actor = {LABELER_AUTH: "labeler", BOT_AUTH: "bot"}.get(auth or "")
        query = urllib.parse.parse_qs(parsed.query)
        self.requests.append(
            {"method": method, "path": path, "query": query, "actor": actor, "body": body}
        )
        for secret in (LABELER_TOKEN, BOT_TOKEN):
            assert secret not in url, "a token rode in a Jira URL"
            assert secret not in json.dumps(body), "a token rode in a Jira request body"
        if actor is None or self.status_401:
            return 401, {"errorMessages": ["Client must be authenticated to access this resource."]}
        assert path.startswith("/rest/api/3/"), url
        return self._route(method, path.removeprefix("/rest/api/3"), query, actor, body)

    def _route(
        self, method: str, path: str, query: dict[str, list[str]], actor: str, body: Any
    ) -> tuple[int, Any]:
        parts = [p for p in path.split("/") if p]
        if method == "GET" and parts == ["myself"]:
            me = _fixture("myself.json")
            if actor == "labeler":
                me.update(accountId=LABELER_ACCOUNT, emailAddress=LABELER_EMAIL)
            else:
                me.update(accountId=BOT_ACCOUNT, emailAddress=BOT_EMAIL, accountType="app")
            me["self"] = f"https://{self.site}/rest/api/3/user?accountId={me['accountId']}"
            return 200, me
        if method == "POST" and parts == ["issue"]:
            return self._create(actor, body)
        if method == "POST" and parts == ["comment", "list"]:
            return self._comment_list(query, body)
        if parts[:1] == ["comment"] and len(parts) >= 3 and parts[2] == "properties":
            return self._comment_property(parts)
        if parts[:1] != ["issue"] or len(parts) < 2:
            return 404, {"errorMessages": [f"no route {method} {path}"]}
        issue = self._issue(parts[1])
        if issue is None:
            return 404, {"errorMessages": ["Issue does not exist or you do not have permission."]}
        rest = parts[2:]
        if method == "GET" and not rest:
            return 200, self._issue_view(issue)
        if method == "PUT" and not rest:
            if issue["key"] in self.fail_label and _touches_labels(body):
                return 500, {"errorMessages": ["Internal server error"]}
            self._apply_labels(issue, body, actor)
            return 204, None
        if method == "GET" and rest == ["comment"]:
            # Jira Cloud's issue listing pages and never expands entity
            # properties, whatever ``expand`` asks for.
            comments = issue["comments"]
            start = int((query.get("startAt") or ["0"])[0])
            size = min(int((query.get("maxResults") or ["5000"])[0]), self.comment_page_size)
            page = [
                {k: v for k, v in c.items() if k != "properties"}
                for c in copy.deepcopy(comments[start : start + size])
            ]
            return 200, {
                "startAt": start,
                "maxResults": size,
                "total": len(comments),
                "comments": page,
            }
        if method == "GET" and rest == ["remotelink"]:
            links = issue["links"]
            wanted = (query.get("globalId") or [None])[0]
            if wanted is not None:
                links = [link for link in links if link["globalId"] == wanted]
            return 200, copy.deepcopy(links)
        if method == "GET" and rest == ["transitions"]:
            return 200, _fixture("transitions.json")
        if method == "POST" and rest == ["transitions"]:
            if issue["key"] in self.fail_close:
                return 500, {"errorMessages": ["Internal server error"]}
            wanted = str(((body or {}).get("transition") or {}).get("id"))
            for transition in _fixture("transitions.json")["transitions"]:
                if transition["id"] == wanted:
                    issue["status"] = transition["to"]["statusCategory"]["key"]
                    break
            else:
                return 400, {"errorMessages": [f"transition {wanted} is not valid"]}
            self._apply_labels(issue, body, actor)
            return 204, None
        return 404, {"errorMessages": [f"no route {method} {path}"]}

    def _create(self, actor: str, body: Any) -> tuple[int, Any]:
        fields = (body or {}).get("fields") or {}
        assert (fields.get("project") or {}).get("key") == PROJECT, body
        number = len(self.issues) + 42
        created = _fixture("issue_created.json")
        issue_id = str(10000 + number)
        key = f"{PROJECT}-{number}"
        created.update(
            id=issue_id, key=key, self=f"https://{self.site}/rest/api/3/issue/{issue_id}"
        )
        self.issues[key] = {
            "id": issue_id,
            "key": key,
            "creator": actor,
            "summary": fields.get("summary"),
            "labels": [],
            "status": "new",
            "comments": [],
            "links": [],
        }
        if fields.get("labels"):
            self._apply_labels(self.issues[key], {"fields": {"labels": fields["labels"]}}, actor)
        return 201, created

    def _issue(self, id_or_key: str) -> dict[str, Any] | None:
        for issue in self.issues.values():
            if id_or_key in (issue["id"], issue["key"]):
                return issue
        return None

    def _issue_view(self, issue: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "id": issue["id"],
            "key": issue["key"],
            "self": f"https://{self.site}/rest/api/3/issue/{issue['id']}",
            "fields": {
                "summary": issue["summary"],
                "labels": list(issue["labels"]),
                "status": {"name": issue["status"], "statusCategory": {"key": issue["status"]}},
            },
        }

    def _apply_labels(self, issue: dict[str, Any], body: Any, actor: str) -> None:
        body = body or {}
        for op in ((body.get("update") or {}).get("labels")) or []:
            for verb, label in op.items():
                if verb == "add" and label not in issue["labels"]:
                    issue["labels"].append(label)
                    self._labelled(issue, actor, label)
                elif verb == "remove" and label in issue["labels"]:
                    issue["labels"].remove(label)
                    self.label_events.append((issue["key"], actor, "remove", label))
                elif verb == "set":
                    self._set_labels(issue, list(label), actor)
        labels = (body.get("fields") or {}).get("labels")
        if labels is not None:
            self._set_labels(issue, list(labels), actor)

    def _set_labels(self, issue: dict[str, Any], labels: list[str], actor: str) -> None:
        for label in labels:
            if label not in issue["labels"]:
                issue["labels"].append(label)
                self._labelled(issue, actor, label)
        for label in [label for label in issue["labels"] if label not in labels]:
            issue["labels"].remove(label)
            self.label_events.append((issue["key"], actor, "remove", label))

    def _labelled(self, issue: dict[str, Any], actor: str, label: str) -> None:
        self.label_events.append((issue["key"], actor, "add", label))
        if label != self.label:
            return
        if actor == "labeler" or (actor == "bot" and self.admit_bot_labels):
            self._admit(issue)
            if self.duplicate_admission:
                self._admit(issue)

    # -- the factory's side --

    def _admit(self, issue: dict[str, Any]) -> None:
        work_item_id = f"wi-{len(self.work_items) + 1}"
        self.work_items.append((work_item_id, issue))
        recorded = _fixture("comments.json")["comments"]
        status, human_copy, plain = (copy.deepcopy(c) for c in recorded)
        status["id"] = f"{issue['id']}1"
        human_copy["id"] = f"{issue['id']}2"
        plain["id"] = f"{issue['id']}3"
        issue["comments"].extend([status, human_copy, plain])
        if self.extra_status_comment:
            again = copy.deepcopy(status)
            again["id"] = f"{issue['id']}4"
            issue["comments"].append(again)
        if self.link_pull_request:
            recorded_links = copy.deepcopy(_fixture("remotelinks.json"))
            recorded_links[0]["globalId"] = recorded_links[0]["object"]["url"] = PR_URL
            issue["links"].extend(recorded_links)

    def _comment_list(self, query: dict[str, list[str]], body: Any) -> tuple[int, Any]:
        """POST /comment/list: the only read that expands comment properties."""

        wanted = [str(i) for i in (body or {}).get("ids") or []]
        expand = "properties" in (query.get("expand") or [""])[0].split(",")
        found = [
            copy.deepcopy(comment)
            for issue in self.issues.values()
            for comment in issue["comments"]
            if comment["id"] in wanted
        ]
        if not expand:
            found = [{k: v for k, v in c.items() if k != "properties"} for c in found]
        start = int((query.get("startAt") or ["0"])[0])
        size = min(int((query.get("maxResults") or ["50"])[0]), self.comment_page_size)
        values = found[start : start + size]
        return 200, {
            "startAt": start,
            "maxResults": size,
            "total": len(found),
            "isLast": start + len(values) >= len(found),
            "values": values,
        }

    def _comment_property(self, parts: list[str]) -> tuple[int, Any]:
        for issue in self.issues.values():
            for comment in issue["comments"]:
                if comment["id"] != parts[1]:
                    continue
                props = comment.get("properties") or []
                if len(parts) == 3:
                    return 200, {"keys": [{"key": p["key"]} for p in props]}
                for prop in props:
                    if prop["key"] == parts[3]:
                        return 200, copy.deepcopy(prop)
                return 404, {"errorMessages": ["The property was not found."]}
        return 404, {"errorMessages": ["The comment was not found."]}

    # -- views for assertions --

    def actor_requests(self, method: str, suffix: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["method"] == method and r["path"].endswith(suffix)]

    def labelled_by(self, actor: str) -> list[str]:
        return [
            key
            for key, who, verb, label in self.label_events
            if who == actor and verb == "add" and label == self.label
        ]


def _touches_labels(body: Any) -> bool:
    body = body or {}
    return bool((body.get("update") or {}).get("labels")) or (
        "labels" in (body.get("fields") or {})
    )


class FakePlatform:
    """The platform's work-items API, as Preflight.api reaches it."""

    def __init__(self, jira: FakeJira, *, down: bool = False) -> None:
        self.jira = jira
        self.down = down
        self.calls: list[tuple[str, str]] = []

    def _item(self, work_item_id: str, issue: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "id": work_item_id,
            "agent_id": "00000000-0000-4000-8000-0000000000ag",
            "tracker": {
                "kind": "jira_cloud",
                "host": self.jira.site,
                "scope_id": self.jira.cloud_id,
                "issue_id": issue["id"],
                "display_key": issue["key"],
                "url": f"https://{self.jira.site}/browse/{issue['key']}",
            },
            "repository": {"kind": "github", "host": "github.com", "full_name": REPO},
            "state": "running",
            "pr": None,
            "requests": [{"status": "running", "sequence": 1}],
        }

    def __call__(
        self, method: str, path: str, *, headers: Mapping[str, str], body: Any = None
    ) -> tuple[int, Any]:
        self.calls.append((method, path))
        if self.down:
            return 503, {"code": "unavailable"}
        route = path.split("?", 1)[0].rstrip("/")
        items = [self._item(wid, issue) for wid, issue in self.jira.work_items]
        if method == "GET" and route == "/work-items":
            return 200, {"items": items, "limit": 200, "truncated": False}
        if method == "GET" and route.startswith("/work-items/"):
            wanted = route.split("/")[2]
            for item in items:
                if item["id"] == wanted:
                    return 200, item
        return 404, {"code": "not_found"}


class FakeGitHub:
    """The GitHub delegate: records each call and answers it."""

    forge = "github"
    pairing = "github+github"

    def __init__(self, p: fe.Preflight) -> None:
        self.p = p
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def _record(self, name: str, *args: Any) -> Any:
        self.calls.append((name, args))
        return {"delegated": name}

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        raise AssertionError("the Jira driver opened a GitHub issue")

    def await_change(self, work_item_id: str) -> Any:
        self.calls.append(("await_change", (work_item_id,)))
        return fe.ChangeRef(PR_NUMBER, PR_URL)

    def change_head(self, change: Any) -> tuple[str | None, int | None]:
        self.calls.append(("change_head", (change,)))
        return ("a" * 40, 1)

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        return self._record("set_ci_red", sha)

    def ci_state(self, sha: str) -> dict[str, Any]:
        return self._record("ci_state", sha)

    def post_review(self, change: Any, text: str) -> dict[str, Any]:
        return self._record("post_review", change, text)

    def merge(self, change: Any) -> dict[str, Any]:
        return self._record("merge", change)

    def change_merged(self, change: Any) -> bool | None:
        self.calls.append(("change_merged", (change,)))
        return True

    def changes_opened(self) -> list[int]:
        self.calls.append(("changes_opened", ()))
        return [PR_NUMBER]

    def default_branch_head(self) -> str:
        self.calls.append(("default_branch_head", ()))
        return "f" * 40

    def reset_fixture(self) -> dict[str, Any]:
        return self._record("reset_fixture")

    def pin_fixture_base(self) -> None:
        self.calls.append(("pin_fixture_base", ()))

    def tracker_evidence(self, change: Any, work_item_id: str) -> dict[str, Any]:
        return {}

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        return []

    def teardowns(self) -> tuple[Any, ...]:
        return ()


class _Rig:
    def __init__(self, p: fe.Preflight, jira: FakeJira, platform: FakePlatform, gh: FakeGitHub):
        self.p = p
        self.jira = jira
        self.platform = platform
        self.github = gh
        self.pr_body: str | None = None

    def driver(self) -> Any:
        return _jira_module().JiraCloudDriver(self.p, github=self.github)


def _rig(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    site: str = SITE,
    cloud_id: str = CLOUD_ID,
    **preflight: Any,
) -> _Rig:
    p = _preflight(tmp_path, **preflight)
    p.api_url = "http://127.0.0.1:9"
    p.api_key = "platform-api-key"
    monkeypatch.setenv(
        "CURIE_FACTORY_JIRA_ENV_FILE",
        str(_env_file(tmp_path / "jira", site=site, cloud_id=cloud_id)),
    )
    jira = FakeJira(site=site, cloud_id=cloud_id, label=p.config.label)
    platform = FakePlatform(jira)
    gh = FakeGitHub(p)
    rig = _Rig(p, jira, platform, gh)
    monkeypatch.setattr(fe, "http_json", jira)
    p.api = platform  # type: ignore[method-assign]

    def as_actor(method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if method == "GET" and path.split("?")[0] == f"/repos/{REPO}/pulls/{PR_NUMBER}":
            first = next(iter(jira.issues.values()), None)
            default = (
                f"Adds the nautical mile.\n\nJira: {first['key']} "
                f"https://{site}/browse/{first['key']}\n"
                if first
                else "No ticket."
            )
            text = rig.pr_body if rig.pr_body is not None else default
            return 200, {"number": PR_NUMBER, "body": text, "html_url": PR_URL}
        raise AssertionError(f"unexpected GitHub call {method} {path}")

    p.as_actor = as_actor  # type: ignore[method-assign]
    return rig


def _first_issue(jira: FakeJira) -> dict[str, Any]:
    return next(iter(jira.issues.values()))


# --- site facts from the env file -----------------------------------------------------


def test_site_facts_come_only_from_the_named_env_file(tmp_path: Path) -> None:
    jira = _jira_module()
    path = _env_file(tmp_path)
    site = jira.load_site({"CURIE_FACTORY_JIRA_ENV_FILE": str(path)})
    assert site.site == SITE
    assert site.cloud_id == CLOUD_ID
    assert site.project_key == PROJECT
    assert site.labeler_email == LABELER_EMAIL
    assert str(site.labeler_token_file) == str(tmp_path / "labeler-token")
    assert site.bot_email == BOT_EMAIL
    assert site.bot_account_id == BOT_ACCOUNT
    assert str(site.bot_token_file) == str(tmp_path / "bot-token")
    _no_secret(repr(site), "the site facts repr")


def test_site_token_files_named_relatively_are_read_beside_the_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The operator's env file names its token files relative to itself; the
    # harness runs from the repository root, not from that directory.
    path = _env_file(tmp_path / "site")
    text = path.read_text()
    text = text.replace(str(tmp_path / "site" / "labeler-token"), "labeler-token")
    text = text.replace(str(tmp_path / "site" / "bot-token"), "bot-token")
    path.write_text(text)
    monkeypatch.chdir(tmp_path)

    site = _jira_module().load_site({"CURIE_FACTORY_JIRA_ENV_FILE": str(path)})

    assert site.labeler_token_file == tmp_path / "site" / "labeler-token"
    assert site.bot_token_file == tmp_path / "site" / "bot-token"


def test_site_facts_without_the_env_var_are_a_config_error() -> None:
    with pytest.raises(fe.ConfigError, match="CURIE_FACTORY_JIRA_ENV_FILE"):
        _jira_module().load_site({})


@pytest.mark.parametrize(
    "key",
    [
        "site",
        "cloud_id",
        "project_key",
        "labeler_email",
        "labeler_token_file",
        "bot_email",
        "bot_account_id",
        "bot_token_file",
    ],
)
def test_a_missing_site_fact_names_the_key_and_no_value(key: str, tmp_path: Path) -> None:
    path = _env_file(tmp_path, drop=key)
    with pytest.raises(fe.ConfigError) as raised:
        _jira_module().load_site({"CURIE_FACTORY_JIRA_ENV_FILE": str(path)})
    message = str(raised.value)
    assert key in message
    for value in (SITE, CLOUD_ID, LABELER_EMAIL, BOT_EMAIL, BOT_ACCOUNT, *SECRETS):
        assert value not in message, value


def test_requests_go_to_the_env_file_site_not_a_hardcoded_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for site, cloud_id, sub in ((SITE, CLOUD_ID, "a"), (OTHER_SITE, OTHER_CLOUD_ID, "b")):
        rig = _rig(tmp_path / sub, monkeypatch, site=site, cloud_id=cloud_id)
        rig.driver().open_and_label_ticket("Title", "Body", time.time())
        assert rig.jira.requests, "the driver made no Jira request"
        assert rig.jira.hosts == {"api.atlassian.com"}, rig.jira.hosts


# --- credentials: Basic auth from the token files, never anywhere else ---------------


def test_tokens_ride_only_in_basic_auth_headers_and_never_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    evidence = driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)
    closed = driver.close_test_issues()

    assert {r["actor"] for r in rig.jira.requests} <= {"labeler", "bot"}
    assert all(r["actor"] is not None for r in rig.jira.requests)
    out = capsys.readouterr()
    for where, text in (
        ("the tracker evidence", json.dumps(evidence, default=str)),
        ("the run evidence", json.dumps(rig.p.evidence, default=str)),
        ("the teardown result", json.dumps(closed, default=str)),
        ("the driver repr", repr(driver)),
        ("stdout", out.out),
        ("stderr", out.err),
        ("the log", caplog.text),
    ):
        _no_secret(text, where)


@pytest.mark.parametrize("status", [401, 500])
def test_a_jira_error_names_no_credential(
    status: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    real = rig.jira

    def failing(method: str, url: str, **kwargs: Any) -> tuple[int, Any]:
        real(method, url, **kwargs)
        return status, {"errorMessages": ["Client must be authenticated."]}

    monkeypatch.setattr(fe, "http_json", failing)
    with pytest.raises(Exception) as raised:  # noqa: PT011 - any refusal, never a leak
        rig.driver().open_and_label_ticket("Title", "Body", time.time())
    chain: list[BaseException] = []
    exc: BaseException | None = raised.value
    while exc is not None and exc not in chain:
        chain.append(exc)
        exc = exc.__cause__ or exc.__context__
    for item in chain:
        _no_secret(str(item), "the raised error")
        _no_secret(repr(item), "the raised error repr")


# --- opening and labelling as the labeler ----------------------------------------------


def test_open_and_label_creates_the_issue_and_labels_it_as_the_labeler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    work_item_id = rig.driver().open_and_label_ticket("Add nmi", "Please add nmi.", time.time())

    assert len(rig.jira.issues) == 1
    issue = _first_issue(rig.jira)
    assert issue["summary"] == "Add nmi"
    assert issue["key"].startswith(f"{PROJECT}-")
    creates = rig.jira.actor_requests("POST", "/issue")
    assert [r["actor"] for r in creates] == ["labeler"]
    assert rig.jira.labelled_by("labeler") == [issue["key"]]
    assert rig.jira.labelled_by("bot") == []
    assert work_item_id == rig.jira.work_items[0][0]
    assert rig.p.evidence["work_item_id"] == work_item_id
    assert any(path.split("?")[0] == "/work-items" for _m, path in rig.platform.calls)


def test_admission_proves_exactly_one_work_item_for_the_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    rig.jira.duplicate_admission = True
    with pytest.raises(fe.PreflightFailed):
        rig.driver().open_and_label_ticket("Title", "Body", time.time())


def test_admission_that_never_comes_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    rig.platform.down = True
    with pytest.raises(fe.PreflightFailed):
        rig.driver().open_and_label_ticket("Title", "Body", time.time())


# --- the evidence the run collects ----------------------------------------------------


def test_tracker_evidence_on_a_complete_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    evidence = driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)

    first = _first_issue(rig.jira)
    assert evidence["key"] == first["key"]
    assert evidence["status_comments"] == 1
    assert evidence["remote_links"] == 1
    assert evidence["pr_body_has_key"] is True
    assert evidence["bot_label_admitted"] is False
    assert driver.judge_tracker(evidence) == []
    # The bot-label probe: a second issue, labelled with the bot's token.
    assert len(rig.jira.issues) == 2
    bot_labelled = rig.jira.labelled_by("bot")
    assert len(bot_labelled) == 1 and bot_labelled[0] != first["key"]
    # Exactly one WorkItem: the labeler's label admitted one, the bot's none.
    assert [issue["key"] for _wid, issue in rig.jira.work_items] == [first["key"]]


def test_status_comments_read_properties_through_the_bulk_endpoint_across_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    rig.jira.extra_status_comment = True
    rig.jira.comment_page_size = 1
    driver = rig.driver()
    driver.open_and_label_ticket("Title", "Body", time.time())
    first = _first_issue(rig.jira)

    assert driver._status_comments(first["key"]) == 2
    listings = rig.jira.actor_requests("GET", f"/issue/{first['key']}/comment")
    assert len(listings) == len(first["comments"])
    bulk = rig.jira.actor_requests("POST", "/comment/list")
    assert bulk and all(r["query"].get("expand") == ["properties"] for r in bulk)
    bot_ids = {int(c["id"]) for c in first["comments"] if c["author"]["accountId"] == BOT_ACCOUNT}
    assert {i for r in bulk for i in r["body"]["ids"]} == bot_ids


@pytest.mark.parametrize(
    ("knob", "field", "value"),
    [
        ("extra_status_comment", "status_comments", 2),
        ("no_remote_link", "remote_links", 0),
        ("pr_body_without_key", "pr_body_has_key", False),
        ("admit_bot_labels", "bot_label_admitted", True),
    ],
)
def test_tracker_evidence_and_judge_name_each_missing_observation(
    knob: str, field: str, value: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    if knob == "extra_status_comment":
        rig.jira.extra_status_comment = True
    elif knob == "no_remote_link":
        rig.jira.link_pull_request = False
    elif knob == "pr_body_without_key":
        rig.pr_body = "Adds the nautical mile. No ticket reference here."
    else:
        rig.jira.admit_bot_labels = True
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    evidence = driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)
    assert evidence[field] == value
    failures = driver.judge_tracker(evidence)
    assert len(failures) == 1, failures


def test_pr_body_needs_both_the_key_and_the_browse_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    key = _first_issue(rig.jira)["key"]
    rig.pr_body = f"Jira: {key}"
    evidence = driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)
    assert evidence["pr_body_has_key"] is False


def test_tracker_evidence_without_a_pull_request_still_probes_the_bot_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    evidence = driver.tracker_evidence(None, work_item_id)
    assert evidence["status_comments"] == 1
    assert evidence["bot_label_admitted"] is False
    assert evidence["pr_body_has_key"] is False
    assert len(rig.jira.labelled_by("bot")) == 1
    assert driver.judge_tracker(evidence)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status_comments", 0),
        ("status_comments", 2),
        ("remote_links", 0),
        ("remote_links", 2),
        ("pr_body_has_key", False),
        ("bot_label_admitted", True),
    ],
)
def test_judge_tracker_names_each_shortfall(
    field: str, value: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    good = {
        "key": f"{PROJECT}-42",
        "status_comments": 1,
        "remote_links": 1,
        "pr_body_has_key": True,
        "bot_label_admitted": False,
    }
    assert driver.judge_tracker(good) == []
    failures = driver.judge_tracker({**good, field: value})
    assert len(failures) == 1, failures


def test_judge_tracker_fails_an_empty_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    assert len(rig.driver().judge_tracker({})) == 4


# --- the pull request, CI, review and merge go to GitHub ------------------------------


def test_identity_and_code_host_steps_delegate_to_the_github_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    assert driver.forge == "jira_cloud"
    assert driver.pairing == "jira_cloud+github"
    change = fe.ChangeRef(PR_NUMBER, PR_URL)
    assert driver.await_change("wi-1") == change
    assert driver.change_head(change) == ("a" * 40, 1)
    assert driver.set_ci_red("a" * 40) == {"delegated": "set_ci_red"}
    assert driver.ci_state("b" * 40) == {"delegated": "ci_state"}
    assert driver.post_review(change, "review text") == {"delegated": "post_review"}
    assert driver.merge(change) == {"delegated": "merge"}
    assert driver.change_merged(change) is True
    assert driver.changes_opened() == [PR_NUMBER]
    assert driver.default_branch_head() == "f" * 40
    assert driver.reset_fixture() == {"delegated": "reset_fixture"}
    driver.pin_fixture_base()
    called = [name for name, _args in rig.github.calls]
    for name in (
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
    ):
        assert name in called, name
    assert ("post_review", (change, "review text")) in rig.github.calls
    # Resetting the GitHub fixture touches no Jira issue.
    assert rig.jira.requests == []


# --- the entry -------------------------------------------------------------------------


def test_the_jira_entry_is_installed_with_the_quickstart_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jira = _jira_module()
    entry = fe.FORGES["jira_cloud"]
    assert entry.installed is True
    assert entry.setup is jira.install_jira
    assert fe.FORGES["github"].setup is None
    monkeypatch.setenv("CURIE_FACTORY_JIRA_ENV_FILE", str(_env_file(tmp_path / "jira")))
    driver = entry.driver(_preflight(tmp_path))
    assert isinstance(driver, jira.JiraCloudDriver)
    assert type(driver.github).__name__ == "GitHubDriver"
    assert driver.forge == "jira_cloud"


# --- the "close Jira test issues" teardown --------------------------------------------


def test_teardowns_is_exactly_the_close_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _rig(tmp_path, monkeypatch).driver()
    steps = driver.teardowns()
    assert [name for name, _step in steps] == [CLOSE_STEP]
    assert callable(steps[0][1])


def _closed(issue: Mapping[str, Any], label: str) -> bool:
    return issue["status"] == "done" and label not in issue["labels"]


def test_close_test_issues_closes_and_unlabels_every_created_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)
    assert len(rig.jira.issues) == 2

    result = driver.close_test_issues()

    assert len(result["closed"]) == 2 and result["failed"] == []
    for issue in rig.jira.issues.values():
        assert _closed(issue, rig.jira.label), issue["key"]
    # The transition used is the one into the done category.
    transitions = rig.jira.actor_requests("POST", "/transitions")
    assert transitions and all(str(r["body"]["transition"]["id"]) == "31" for r in transitions)


def test_an_issue_whose_label_failed_is_still_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    rig.jira.fail_label = {f"{PROJECT}-42"}
    driver = rig.driver()
    with pytest.raises(Exception):  # noqa: B017, PT011 - the label call failed
        driver.open_and_label_ticket("Title", "Body", time.time())
    assert list(rig.jira.issues) == [f"{PROJECT}-42"]
    result = driver.close_test_issues()
    assert len(result["closed"]) == 1
    assert rig.jira.issues[f"{PROJECT}-42"]["status"] == "done"


def test_close_test_issues_continues_past_a_failure_then_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(tmp_path, monkeypatch)
    driver = rig.driver()
    work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
    driver.tracker_evidence(fe.ChangeRef(PR_NUMBER, PR_URL), work_item_id)
    first, second = list(rig.jira.issues)
    rig.jira.fail_close = {first}
    with pytest.raises(Exception) as raised:  # noqa: PT011 - the step must record ok False
        driver.close_test_issues()
    _no_secret(str(raised.value), "the close failure")
    assert _closed(rig.jira.issues[second], rig.jira.label)
    assert rig.jira.issues[first]["status"] != "done"


def _stub_install(p: fe.Preflight, monkeypatch: pytest.MonkeyPatch, order: list[str]) -> None:
    for step in (
        "check_tools",
        "check_images",
        "check_app",
        "sweep_stale_namespaces",
        "create_namespace",
        "install",
        "port_forward",
        "bind_agent",
        "ensure_label",
        "tunnel",
        "point_card_at_tunnel",
        "repoint_webhook",
    ):

        def stub(*_a: Any, _step: str = step, **_k: Any) -> None:
            order.append(_step)

        monkeypatch.setattr(p, step, stub)
    monkeypatch.setattr(p, "_lock", lambda *_a, **_k: None)


def _jira_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[_Rig, list[str], list[dict[str, Any]]]:
    """A Preflight.run on the Jira entry that fails mid-run, then main's teardown."""

    rig = _rig(tmp_path, monkeypatch, scenario_name="forge-loop")
    rig.p.evidence["forge"] = "jira_cloud"
    order: list[str] = []
    _stub_install(rig.p, monkeypatch, order)
    monkeypatch.setitem(
        fe.FORGES,
        "jira_cloud",
        fe.ForgeEntry(
            name="jira_cloud",
            driver=lambda p: _jira_module().JiraCloudDriver(p, github=rig.github),
            installed=True,
            setup=lambda p: order.append("setup"),
        ),
    )

    def scenario(p: fe.Preflight) -> dict[str, Any]:
        driver = p.loop_driver()
        work_item_id = driver.open_and_label_ticket("Title", "Body", time.time())
        driver.tracker_evidence(None, work_item_id)
        raise RuntimeError("the loop failed mid-run")

    with pytest.raises(RuntimeError, match="mid-run"):
        rig.p.run(scenario)
    return rig, order, rig.p.teardown.run()


def test_close_jira_test_issues_is_a_separate_teardown_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig, order, results = _jira_run(tmp_path, monkeypatch)

    assert "setup" in order and "bind_agent" not in order
    assert order.index("setup") > order.index("port_forward")
    names = [r["step"] for r in results]
    assert names.count(CLOSE_STEP) == 1
    assert "reset fixture repository" in names
    # Pushed right after the fixture reset, so it runs just before it (LIFO).
    assert names.index(CLOSE_STEP) == names.index("reset fixture repository") - 1
    (close,) = [r for r in results if r["step"] == CLOSE_STEP]
    assert close["ok"] is True, close
    assert len(rig.jira.issues) == 2
    for issue in rig.jira.issues.values():
        assert _closed(issue, rig.jira.label), issue["key"]


def test_a_failed_close_marks_the_step_not_ok_and_closes_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = FakeJira.__init__

    def failing_first(self: FakeJira, **kwargs: Any) -> None:
        original(self, **kwargs)
        self.fail_close = {f"{PROJECT}-42"}

    monkeypatch.setattr(FakeJira, "__init__", failing_first)
    rig, _order, results = _jira_run(tmp_path, monkeypatch)

    (close,) = [r for r in results if r["step"] == CLOSE_STEP]
    assert close["ok"] is False
    _no_secret(json.dumps(close, default=str), "the teardown record")
    first, second = list(rig.jira.issues)
    assert first == f"{PROJECT}-42"
    assert _closed(rig.jira.issues[second], rig.jira.label)


# --- run_loop records and judges the tracker evidence (r1 finding 8) -------------------


class _LoopDriver(FakeGitHub):
    """A driver serving a complete loop whose tracker evidence is set per test."""

    forge = "fakejira"
    pairing = "fakejira+github"

    def __init__(self, p: fe.Preflight, evidence: dict[str, Any], change: bool = True) -> None:
        super().__init__(p)
        self.evidence = evidence
        self.change = change
        self.heads = iter([("a" * 40, 1), ("b" * 40, 2), ("d" * 40, 3)])
        self.tracker_calls: list[tuple[Any, str]] = []

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        self.p.evidence["work_item_id"] = "wi-1"
        return "wi-1"

    def await_change(self, work_item_id: str) -> Any:
        return fe.ChangeRef(7, "https://forge.example/change/7") if self.change else None

    def change_head(self, change: Any) -> tuple[str | None, int | None]:
        return next(self.heads)

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        return {"sha": sha, "state": "failure"}

    def ci_state(self, sha: str) -> dict[str, Any]:
        return {"sha": sha, "state": "failing" if sha == "a" * 40 else "passing"}

    def post_review(self, change: Any, text: str) -> dict[str, Any]:
        return {"id": 5, "delivery_status_code": 200, "delivery_api_status": "factory_admitted"}

    def merge(self, change: Any) -> dict[str, Any]:
        return {"status_code": 200, "merged": True}

    def changes_opened(self) -> list[int]:
        return [7]

    def tracker_evidence(self, change: Any, work_item_id: str) -> dict[str, Any]:
        self.tracker_calls.append((change, work_item_id))
        return dict(self.evidence)

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        count = evidence.get("status_comments")
        return [] if count == 1 else [f"the ticket has {count} status comments, expected 1"]


def _loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, driver_of: Any
) -> tuple[fe.Preflight, Any]:
    p = _preflight(tmp_path, scenario_name="forge-loop")
    driver = driver_of(p)
    p.head_before = "f" * 40
    one = {"status": "completed", "started_at": "2026-10-11T00:00:00Z"}
    details = iter(
        [
            {"state": "published", "requests": [one], "pr": {"number": 7}},
            {"state": "published", "requests": [one, one], "pr": {"number": 7}},
        ]
    )
    merged = {"state": "published", "requests": [one, one], "pr": {"number": 7, "status": "merged"}}
    p.work_item_detail = lambda _id: next(details, merged)  # type: ignore[method-assign]
    p.cli_check = lambda *_a, **_k: None  # type: ignore[method-assign]
    p.cli_not_found_check = lambda *_a, **_k: None  # type: ignore[method-assign]
    cli_body = json.dumps({"item": {"state": "published", "pr": {"number": 7, "status": "merged"}}})
    p.cli_work_items_raw = lambda _id: (0, cli_body)  # type: ignore[method-assign]
    return p, driver


def test_run_loop_records_and_judges_tracker_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = {"key": "FAC-42", "status_comments": 1, "remote_links": 1}
    p, driver = _loop(tmp_path / "good", monkeypatch, lambda p: _LoopDriver(p, good))
    obs = fe.run_loop(p, driver)
    assert obs["verdict"] == "passed", obs["failures"]
    assert obs["tracker"] == good
    assert obs["pairing"] == "fakejira+github"
    assert obs["forge"] == "fakejira"
    (call,) = driver.tracker_calls
    assert call == (fe.ChangeRef(7, "https://forge.example/change/7"), "wi-1")

    bad = {**good, "status_comments": 2}
    p, driver = _loop(tmp_path / "bad", monkeypatch, lambda p: _LoopDriver(p, bad))
    with pytest.raises(fe.PreflightFailed, match="2 status comments"):
        fe.run_loop(p, driver)
    scenario = p.evidence["scenario"]
    assert scenario["verdict"] == "failed"
    assert scenario["tracker"] == bad
    assert any("2 status comments" in f for f in scenario["failures"])


def test_run_loop_without_a_pull_request_still_records_tracker_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = {"key": "FAC-42", "status_comments": 0}
    p, driver = _loop(tmp_path, monkeypatch, lambda p: _LoopDriver(p, evidence, change=False))
    with pytest.raises(fe.PreflightFailed):
        fe.run_loop(p, driver)
    scenario = p.evidence["scenario"]
    assert driver.tracker_calls == [(None, "wi-1")]
    assert scenario["tracker"] == evidence
    assert scenario["pairing"] == "fakejira+github"
    assert "the run opened no pull request" in scenario["failures"]
    assert any("0 status comments" in f for f in scenario["failures"])


# --- install_jira: the quickstart in AddAgent mode -------------------------------------


def _recorded_values(*, with_secret: bool = True) -> dict[str, Any]:
    credential: dict[str, Any] = {"type": "github_app", "appId": "42"}
    if with_secret:
        credential.update({"existingSecret": fe.APP_KEY_REF, "existingSecretKey": "privateKey"})
    github = {"name": "github", "kind": "github", "host": "github.com", "apiUrl": fe.GITHUB_API}
    return {
        "api": {"githubRepoAllowlist": [REPO]},
        "factory": {
            "enabled": True,
            "intake": "poll",
            "codeHosts": [{**github, "credential": credential}],
            "trackers": [{**github, "credential": credential, "label": "curie-factory"}],
        },
    }


class _Processes:
    """subprocess.run for helm, kubectl and curie; records every argv and env."""

    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values
        self.calls: list[tuple[list[str], Mapping[str, str] | None]] = []

    def __call__(self, argv: Any, *_a: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        argv = [str(a) for a in argv]
        self.calls.append((argv, kwargs.get("env")))
        name = Path(argv[0]).name
        if name == "helm" and "get" in argv and "values" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.values), "")
        if name == "kubectl" and "config" in argv:
            return subprocess.CompletedProcess(argv, 0, "apiVersion: v1\nkind: Config\n", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    def quickstarts(self) -> list[tuple[list[str], Mapping[str, str] | None]]:
        return [(a, e) for a, e in self.calls if "factory" in a and "quickstart" in a]


def _install_rig(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, values: dict[str, Any]
) -> tuple[_Rig, _Processes]:
    rig = _rig(tmp_path, monkeypatch)
    rig.p.chart_dir = tmp_path / "charts" / "curie"
    rig.p.chart_dir.mkdir(parents=True)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    monkeypatch.setattr(rig.p, "build_runner_layer", lambda *_a, **_k: bundle)
    processes = _Processes(values)
    monkeypatch.setattr(subprocess, "run", processes)

    def no_popen(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("install_jira started a background process")

    monkeypatch.setattr(subprocess, "Popen", no_popen)
    return rig, processes


def _flag(argv: list[str], flag: str) -> list[str]:
    return [argv[i + 1] for i, item in enumerate(argv[:-1]) if item == flag]


def test_install_jira_runs_the_quickstart_with_the_token_file_never_the_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig, processes = _install_rig(tmp_path, monkeypatch, _recorded_values())
    _jira_module().install_jira(rig.p)

    (quickstart,) = processes.quickstarts()
    argv, env = quickstart
    assert argv[0] == rig.p.config.curie_bin
    assert argv.index("factory") < argv.index("quickstart")
    assert _flag(argv, "--context") == [rig.p.config.kube_context]
    assert _flag(argv, "--namespace") == [rig.p.namespace]
    assert _flag(argv, "--release") == [fe.RELEASE]
    assert _flag(argv, "--agent") == [fe.FACTORY_AGENT]
    assert _flag(argv, "--repo") == [REPO]
    assert _flag(argv, "--chart") == [str(rig.p.chart_dir)]
    assert len(_flag(argv, "--plugin-dir")) == 1
    assert _flag(argv, "--tracker") == ["jira-cloud"]
    assert _flag(argv, "--code-host") == ["github"]
    assert _flag(argv, "--forge-url") == [f"https://api.atlassian.com/ex/jira/{CLOUD_ID}"]
    assert _flag(argv, "--token-file") == [str(tmp_path / "jira" / "bot-token")]
    assert _flag(argv, "--forge-username") == [BOT_EMAIL]
    # The labeler's accountId, read from /myself with the labeler's token.
    assert _flag(argv, "--start-account") == [LABELER_ACCOUNT]
    assert [r["actor"] for r in rig.jira.actor_requests("GET", "/myself")] == ["labeler"]
    # AddAgent mode: no App flags, so the recorded intake is reused.
    assert "--app-id" not in argv and "--private-key-file" not in argv
    for item in argv:
        _no_secret(item, "the quickstart argv")
    for value in (env or {}).values():
        _no_secret(str(value), "the quickstart environment")


def test_install_jira_refuses_a_release_without_recorded_intake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig, processes = _install_rig(tmp_path, monkeypatch, _recorded_values(with_secret=False))
    with pytest.raises((fe.PreflightFailed, fe.ConfigError), match="existingSecret"):
        _jira_module().install_jira(rig.p)
    assert processes.quickstarts() == []


# --- pure helpers over the recorded responses ------------------------------------------


def test_bot_comment_count_counts_only_bot_comments_carrying_the_property() -> None:
    jira = _jira_module()
    recorded = _fixture("comments.json")
    assert jira.bot_comment_count(recorded, BOT_ACCOUNT) == 1
    assert jira.bot_comment_count(recorded, "557058:00000000-0000-4000-8000-0000000000ff") == 0
    assert jira.bot_comment_count({"comments": []}, BOT_ACCOUNT) == 0


def test_remote_link_count_matches_the_pull_request_url_as_global_id() -> None:
    jira = _jira_module()
    recorded = _fixture("remotelinks.json")
    assert jira.remote_link_count(recorded, "https://github.com/acme/fixture/pull/42") == 1
    assert jira.remote_link_count(recorded, "https://github.com/acme/fixture/pull/99") == 0
    assert jira.remote_link_count([], "https://github.com/acme/fixture/pull/42") == 0


def test_done_transition_picks_the_transition_into_the_done_category() -> None:
    jira = _jira_module()
    recorded = _fixture("transitions.json")
    assert jira.done_transition(recorded) == "31"
    kept = [t for t in recorded["transitions"] if t["to"]["statusCategory"]["key"] != "done"]
    not_done = {"transitions": kept}
    assert jira.done_transition(not_done) is None


def test_jira_label_update_adds_and_removes_in_one_update() -> None:
    jira = _jira_module()
    assert jira.jira_label_update(add=["curie-factory"]) == {
        "update": {"labels": [{"add": "curie-factory"}]}
    }
    assert jira.jira_label_update(remove=["curie-factory"]) == {
        "update": {"labels": [{"remove": "curie-factory"}]}
    }


def _hygiene() -> Any:
    path = Path(__file__).resolve().with_name("test_recordings_hygiene.py")
    spec = importlib.util.spec_from_file_location("jira_fixture_hygiene_probe", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_recorded_jira_responses_are_scrubbed() -> None:
    hygiene = _hygiene()
    recordings = sorted(FIXTURES.glob("*.json"))
    assert {p.name for p in recordings} >= {
        "comments.json",
        "issue_created.json",
        "myself.json",
        "remotelinks.json",
        "transitions.json",
    }
    for path in recordings:
        text = path.read_text()
        assert hygiene.findings(text) == [], path.name
        hosts = set(re.findall(r"([A-Za-z0-9-]+)\.atlassian\.net", text))
        assert hosts <= {"example"}, (path.name, hosts)
        emails = set(re.findall(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+)", text))
        assert emails <= {"example.com"}, (path.name, emails)
        for account in re.findall(r"\d{6}:([0-9a-f-]{36})", text):
            assert account.startswith("00000000-0000-4000-8000-"), (path.name, account)
