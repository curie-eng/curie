"""Tests for the self-upgrade connector.

Two are load-bearing.

`test_the_posted_spec_is_the_cronjob_template_verbatim` is the security
property in a single assertion: this connector's argument for holding
namespace-wide `create` on `jobs` is that no caller input reaches the body. If
the body is ever assembled rather than copied, that argument is gone and the
grant is just a broad grant.

`test_the_label_it_stamps_is_the_one_it_selects_on` guards a failure that is
invisible when it happens: if the label written at creation and the selector used
to find running Jobs drift apart, the concurrency check silently matches nothing
and every call starts another overlapping upgrade.
"""

import importlib.util
import inspect
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import anyio
import httpx
import pytest
import yaml
from mcp import types as mcp_types
from mcp.server.mcpserver.exceptions import ToolError

_MODULE_NAME = "sre_bot_self_upgrade_server"
_SERVER_PY = Path(__file__).parent / "server.py"

GOOD_KUBECONFIG = {
    "clusters": [{"cluster": {"server": "https://k8s.example:6443"}}],
    "users": [{"user": {"token": "upgrade-token"}}],
}

# What a real CronJob's `spec.jobTemplate` looks like, trimmed to the parts this
# file touches. The container is deliberately distinctive: an assertion that the
# posted spec still contains it is an assertion that nothing reassembled it.
JOB_TEMPLATE = {
    "metadata": {"labels": {"app": "sre-bot-self-upgrade"}},
    "spec": {
        "backoffLimit": 2,
        "template": {
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "check",
                        "image": "ghcr.io/curie-eng/curie-api:0.7.2",
                        "command": ["python3", "/opt/self-upgrade/redeploy.py"],
                    }
                ],
            }
        },
    },
}


def _load(
    tmp_path,
    kubeconfig=GOOD_KUBECONFIG,
    cronjob="sre-bot-self-upgrade",
    platform_cronjob="platform-upgrade",
    release_repo="curie-eng/curie",
):
    cfg = tmp_path / "kubeconfig"
    cfg.write_text(yaml.safe_dump(kubeconfig), encoding="utf-8")
    os.environ["KUBECONFIG_PATH"] = str(cfg)
    os.environ["SELF_UPGRADE_CRONJOB"] = cronjob
    os.environ["SELF_UPGRADE_NAMESPACE"] = "curie"
    os.environ["PLATFORM_UPGRADE_CRONJOB"] = platform_cronjob
    os.environ["SELF_UPGRADE_RELEASE_REPO"] = release_repo
    os.environ["SELF_UPGRADE_RELEASE_API"] = "https://api.github.test"
    sys.modules.pop(_MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SERVER_PY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


class _Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


class _FakeClient:
    """Records what the tool asked the API server to do."""

    def __init__(
        self,
        seen,
        cronjob=(200, {"spec": {"jobTemplate": JOB_TEMPLATE}}),
        jobs=(200, {"items": []}),
        create=(201, {"metadata": {"name": "sre-bot-self-upgrade-abc12"}}),
    ):
        self.seen = seen
        self._cronjob = cronjob
        self._jobs = jobs
        self._create = create

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    # These signatures MIRROR httpx.Client, keyword-only marker included. The
    # sibling scale connector shipped a verb that could never run because its
    # fake took the body positionally while the real client does not (#1947);
    # a fake looser than the thing it stands in for tests the fake.
    def get(self, path, *, params=None):
        if "/cronjobs/" in path:
            self.seen["cronjob_path"] = path
            return _Response(*self._cronjob)
        self.seen["jobs_path"] = path
        self.seen["jobs_params"] = params
        status, body = self._jobs
        # FILTER, like the API server does. Echoing every item back regardless of
        # `labelSelector` makes the fake looser than the thing it stands in for:
        # a tool that sent the WRONG selector would still see the job and still
        # refuse, so the concurrency scoping could regress with every test green.
        selector = (params or {}).get("labelSelector")
        if selector and isinstance(body, dict) and "items" in body:
            key, _, value = selector.partition("=")
            body = {
                "items": [
                    item
                    for item in body["items"]
                    if ((item.get("metadata") or {}).get("labels") or {}).get(key) == value
                ]
            }
        return _Response(status, body)

    def post(self, path, *, json=None, content=None, headers=None):
        self.seen["post_path"] = path
        self.seen["post_body"] = json
        return _Response(*self._create)


def test_the_posted_spec_is_the_cronjob_template_verbatim(tmp_path, monkeypatch):
    """No caller input reaches the body -- the whole basis for the jobs grant."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    result = json.loads(srv.upgrade_self())
    assert result["ok"] is True
    assert seen["post_body"]["spec"] == JOB_TEMPLATE["spec"]


def test_the_tool_exposes_no_parameters_at_all(tmp_path):
    """A tool with no arguments has nothing to validate and nothing to escape."""
    srv = _load(tmp_path)
    assert inspect.signature(srv.upgrade_self).parameters == {}


def test_the_label_it_stamps_is_the_one_it_selects_on(tmp_path, monkeypatch):
    """Drift between the two makes the concurrency guard silently match nothing."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    srv.upgrade_self()
    stamped = seen["post_body"]["metadata"]["labels"]
    selector = seen["jobs_params"]["labelSelector"]
    key, _, value = selector.partition("=")
    assert stamped[key] == value


def test_it_refuses_while_an_upgrade_is_still_running(tmp_path, monkeypatch):
    """Two overlapping runs race on creating the version; the loser leaves a row."""
    srv = _load(tmp_path)
    seen = {}
    running = {
        "items": [
            {
                # Labelled as the connector labels its own Jobs, because the fake
                # now filters by selector exactly as the API server does.
                "metadata": {
                    "name": "in-flight",
                    "labels": {"curie.dev/self-upgrade-of": "sre-bot-self-upgrade"},
                },
                "status": {"active": 1},
            }
        ]
    }
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, jobs=(200, running)))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "in-flight" in str(excinfo.value)
    assert "post_body" not in seen


def test_a_finished_job_does_not_block_a_new_one(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    seen = {}
    done = {"items": [{"metadata": {"name": "old"}, "status": {"succeeded": 1}}]}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, jobs=(200, done)))
    assert json.loads(srv.upgrade_self())["ok"] is True


def test_an_unset_cronjob_refuses_before_reaching_the_api(tmp_path, monkeypatch):
    """A missing config fails closed, like the other connectors' allowlists."""
    srv = _load(tmp_path, cronjob="")
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "SELF_UPGRADE_CRONJOB is not set" in str(excinfo.value)
    assert seen == {}


def test_a_missing_cronjob_says_it_is_not_installed(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}, cronjob=(404, {})))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "not installed" in str(excinfo.value)


@pytest.mark.parametrize("status", [401, 403])
def test_permission_errors_name_the_missing_verb(tmp_path, monkeypatch, status):
    """So the reader fixes RBAC instead of retrying a call that cannot succeed."""
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}, create=(status, {})))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "create on jobs" in str(excinfo.value)


def test_prior_is_null_even_when_it_succeeds(tmp_path, monkeypatch):
    """This action has no snapshot; the platform must never record one."""
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}))
    result = json.loads(srv.upgrade_self())
    assert result["ok"] is True
    assert result["prior"] is None
    assert "no undo" in result["summary"]


def test_success_does_not_claim_the_upgrade_finished(tmp_path, monkeypatch):
    """Starting a Job is not finishing it, and the bot reports from this text."""
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}))
    summary = json.loads(srv.upgrade_self())["summary"]
    assert "does not" in summary and "succeeded" in summary


def test_the_created_job_name_comes_back(tmp_path, monkeypatch):
    """The bot needs it to watch the Job with the read-only tools."""
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}))
    result = json.loads(srv.upgrade_self())
    assert result["target"] == {
        "kind": "Job",
        "namespace": "curie",
        "name": "sre-bot-self-upgrade-abc12",
    }


def test_the_name_is_generated_server_side(tmp_path, monkeypatch):
    """Two approvals landing together must not collide on a fixed name."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    srv.upgrade_self()
    metadata = seen["post_body"]["metadata"]
    assert metadata["generateName"].startswith("sre-bot-self-upgrade")
    assert "name" not in metadata


def test_tool_is_annotated_as_a_destructive_non_idempotent_write(tmp_path):
    srv = _load(tmp_path)
    assert srv.UPGRADE.read_only_hint is False
    assert srv.UPGRADE.destructive_hint is True
    assert srv.UPGRADE.idempotent_hint is False


def test_insecure_skip_tls_verify_is_refused(tmp_path):
    """A write path that skips verification can be pointed at an impostor."""
    srv = _load(
        tmp_path,
        kubeconfig={
            "clusters": [{"cluster": {"server": "https://k8s", "insecure-skip-tls-verify": True}}],
            "users": [{"user": {"token": "upgrade-token"}}],
        },
    )
    with pytest.raises(ToolError) as excinfo:
        srv._client()
    assert "insecure-skip-tls-verify" in str(excinfo.value)


def test_missing_kubeconfig_is_a_sentence_not_a_traceback(tmp_path):
    srv = _load(tmp_path)
    srv.KUBECONFIG = str(tmp_path / "absent")
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "not mounted" in str(excinfo.value)


def test_the_token_never_appears_in_an_error_message(tmp_path, monkeypatch):
    """A refusal is posted back into Slack; a credential must not ride along."""
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}, cronjob=(500, {})))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_self()
    assert "upgrade-token" not in str(excinfo.value)


# --- The MCP wire distinguishes a refusal from a started upgrade -------------


def _call_tool(srv, name, args):
    """Call one tool through the real MCP path and return its CallToolResult."""

    async def go():
        entry = srv.mcp._lowlevel_server.get_request_handler("tools/call")
        assert entry is not None
        return await entry.handler(None, mcp_types.CallToolRequestParams(name=name, arguments=args))

    return anyio.run(go)


def test_an_active_job_refusal_and_a_started_upgrade_have_different_error_flags(
    tmp_path, monkeypatch
):
    srv = _load(tmp_path)
    refused_seen = {}
    running = {
        "items": [
            {
                # Labelled as the connector labels its own Jobs, because the fake
                # now filters by selector exactly as the API server does.
                "metadata": {
                    "name": "in-flight",
                    "labels": {"curie.dev/self-upgrade-of": "sre-bot-self-upgrade"},
                },
                "status": {"active": 1},
            }
        ]
    }
    monkeypatch.setattr(
        srv,
        "_client",
        lambda: _FakeClient(refused_seen, jobs=(200, running)),
    )

    refused = _call_tool(srv, "upgrade_self", {})
    assert refused.is_error is True
    assert "post_body" not in refused_seen
    refusal_text = refused.content[0].text

    # OBSERVED against the pinned mcp==1.28.1. FastMCP builds this prefix at
    # mcp/server/fastmcp/tools/base.py:117
    # (`raise ToolError(f"Error executing tool {self.name}: {e}") from e`) and
    # puts str(e) in the result as its only text content. Pin the whole string so
    # an SDK change cannot silently reshape what operators and agents read.
    assert refusal_text == (
        "Error executing tool upgrade_self: refusing: upgrade job in-flight is "
        "still running. Wait for it to finish rather than starting a second one "
        "-- two overlapping runs race on creating the version."
    )

    started_seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(started_seen))
    started = _call_tool(srv, "upgrade_self", {})
    assert started.is_error is False
    assert not started.content[0].text.startswith("Error executing tool")
    payload = json.loads(started.content[0].text)
    assert payload["ok"] is True
    assert payload["prior"] is None
    assert payload["post"] == {"job": "sre-bot-self-upgrade-abc12"}
    assert payload["target"] == {
        "kind": "Job",
        "namespace": "curie",
        "name": "sre-bot-self-upgrade-abc12",
    }
    assert refused.is_error != started.is_error


def test_the_fake_client_cannot_accept_a_call_the_real_one_rejects():
    """Keep the fake from accepting a call the real httpx client rejects."""

    import httpx

    for method in ("get", "post"):
        real = inspect.signature(getattr(httpx.Client, method)).parameters
        fake = inspect.signature(getattr(_FakeClient, method)).parameters
        # [0] is self, [1] is the URL; both are positional in httpx too.
        for name, param in list(fake.items())[2:]:
            assert name in real, f"_FakeClient.{method} accepts {name!r}, httpx does not"
            assert param.kind is inspect.Parameter.KEYWORD_ONLY
            assert real[name].kind is inspect.Parameter.KEYWORD_ONLY


# --- latest_release -------------------------------------------------------
#
# These keep the REAL httpx.Client and swap only its transport, for the reason
# the sibling scale connector had to learn the hard way (#1947): a hand-written
# double accepted a call the real client rejects, and every test passed while the
# tool could never run.

RELEASE_BODY = {
    "tag_name": "v0.8.1",
    "name": "v0.8.1",
    "html_url": "https://github.test/curie-eng/curie/releases/tag/v0.8.1",
    "published_at": "2026-08-30T20:40:35Z",
}


def _with_release_response(srv, monkeypatch, status=200, body=RELEASE_BODY, seen=None):
    """Point the module's httpx at a mock transport, keeping the real client."""

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen["request"] = request
        return httpx.Response(status, json=body)

    real_client = httpx.Client

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(srv.httpx, "Client", factory)


def test_it_reports_the_newest_published_tag(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    _with_release_response(srv, monkeypatch)
    result = json.loads(srv.latest_release())
    assert result["tag"] == "v0.8.1"
    assert result["published_at"] == "2026-08-30T20:40:35Z"


def test_the_summary_says_this_is_not_what_is_installed(tmp_path, monkeypatch):
    """The exact confusion this tool exists next to, so it is pinned.

    A bot that reports the newest tag as its own version is worse than one that
    cannot answer: the installed version is a property of the cluster and this
    number is a property of a repository, and they are routinely different.
    """
    srv = _load(tmp_path)
    _with_release_response(srv, monkeypatch)
    assert "NOT what this install is running" in json.loads(srv.latest_release())["summary"]


def test_it_sends_no_credential(tmp_path, monkeypatch):
    """A connector whose whole job is one public read must not hold a token."""
    srv = _load(tmp_path)
    seen = {}
    _with_release_response(srv, monkeypatch, seen=seen)
    srv.latest_release()
    assert "authorization" not in {k.lower() for k in seen["request"].headers}


def test_it_asks_the_configured_repository(tmp_path, monkeypatch):
    srv = _load(tmp_path, release_repo="acme/widget")
    seen = {}
    _with_release_response(srv, monkeypatch, seen=seen)
    srv.latest_release()
    assert seen["request"].url.path == "/repos/acme/widget/releases/latest"


def test_the_read_tool_exposes_no_parameters(tmp_path):
    srv = _load(tmp_path)
    assert inspect.signature(srv.latest_release).parameters == {}


def test_it_is_annotated_as_a_read(tmp_path):
    srv = _load(tmp_path)
    assert srv.READ.read_only_hint is True
    assert srv.READ.destructive_hint is False


def test_an_unset_repository_refuses_without_a_network_call(tmp_path, monkeypatch):
    srv = _load(tmp_path, release_repo="")
    seen = {}
    _with_release_response(srv, monkeypatch, seen=seen)
    with pytest.raises(ToolError) as excinfo:
        srv.latest_release()
    assert "SELF_UPGRADE_RELEASE_REPO" in str(excinfo.value)
    assert seen == {}


@pytest.mark.parametrize(
    "status,expected",
    [(404, "no published releases"), (403, "rate limits"), (500, "HTTP 500")],
)
def test_release_failures_explain_themselves(tmp_path, monkeypatch, status, expected):
    srv = _load(tmp_path)
    _with_release_response(srv, monkeypatch, status=status, body={})
    with pytest.raises(ToolError) as excinfo:
        srv.latest_release()
    assert expected in str(excinfo.value)


def test_a_body_with_no_tag_is_reported_not_guessed(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    _with_release_response(srv, monkeypatch, body={"name": "no tag here"})
    with pytest.raises(ToolError) as excinfo:
        srv.latest_release()
    assert "no tag_name" in str(excinfo.value)


# --- upgrade_platform -----------------------------------------------------


def test_it_starts_the_platform_template_not_the_bundle_one(tmp_path, monkeypatch):
    """The two verbs must not be able to run each other's template."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    result = json.loads(srv.upgrade_platform())
    assert result["ok"] is True
    assert seen["cronjob_path"].endswith("/cronjobs/platform-upgrade")
    assert seen["post_body"]["metadata"]["generateName"].startswith("platform-upgrade")


def test_an_unset_platform_cronjob_refuses(tmp_path, monkeypatch):
    """An install that has not opted in does not get a platform upgrade."""
    srv = _load(tmp_path, platform_cronjob="")
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    with pytest.raises(ToolError) as excinfo:
        srv.upgrade_platform()
    assert "PLATFORM_UPGRADE_CRONJOB" in str(excinfo.value)
    assert seen == {}


def test_the_two_verbs_do_not_block_each_other(tmp_path, monkeypatch):
    """A running bundle redeploy must not refuse a platform upgrade.

    They run different templates against different credentials and are
    independent. The concurrency guard is scoped to the CronJob being started,
    so a Job labelled for one does not look active to the other -- if that
    scoping regresses, one in-flight upgrade silently blocks the unrelated verb.
    """
    srv = _load(tmp_path)
    running_self = {
        "items": [
            {
                "metadata": {
                    "name": "in-flight",
                    "labels": {"curie.dev/self-upgrade-of": "sre-bot-self-upgrade"},
                },
                "status": {"active": 1},
            }
        ]
    }
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, jobs=(200, running_self)))
    srv.upgrade_platform()
    assert seen["jobs_params"]["labelSelector"] == ("curie.dev/self-upgrade-of=platform-upgrade")


def test_the_platform_tool_exposes_no_parameters(tmp_path):
    srv = _load(tmp_path)
    assert inspect.signature(srv.upgrade_platform).parameters == {}


def test_its_reply_refuses_to_imply_a_rollback(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient({}))
    result = json.loads(srv.upgrade_platform())
    assert result["prior"] is None
    assert "no undo" in result["summary"]


# --- the concurrency read, against a local stand-in API --------------------
#
# The tests above drive the operation through a fake client, which is the
# right tool for the shapes a fake can represent. The concurrency read is
# different: the defect is what the connector does when the read FAILS, and a
# fake that always answers cannot fail. So these drive the REAL `_client` --
# a real httpx client built from a real kubeconfig -- against a local stand-in
# for the Kubernetes API. The stand-in records every request it receives and
# creates no cluster object; it is the whole cluster for one test.


# A second template, deliberately different in every field the connector
# copies, so a test can tell WHICH CronJob's template a verb posted.
PLATFORM_JOB_TEMPLATE = {
    "metadata": {"labels": {"app": "sre-bot-platform-upgrade"}},
    "spec": {
        "backoffLimit": 0,
        "template": {
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": "platform-upgrader",
                "containers": [
                    {
                        "name": "upgrade",
                        "image": "ghcr.io/curie-eng/curie-helm:0.7.2",
                        "command": ["helm", "upgrade", "--reset-values"],
                    }
                ],
            }
        },
    },
}


class _ExternalAPI:
    """A local stand-in Kubernetes API server, owned by one test at a time.

    `jobs` is called with the request handler when the connector reads the
    Jobs list, and shapes that one read into whatever the test is about: a
    healthy list, or the failure under test. Everything else behaves as a
    healthy cluster -- the CronJob read returns that CronJob's template, and a
    Job-creation POST succeeds -- so a recorded POST means the connector chose
    to start a Job, and an absent POST means it chose not to.
    """

    TEMPLATES = {
        "sre-bot-self-upgrade": JOB_TEMPLATE,
        "platform-upgrade": PLATFORM_JOB_TEMPLATE,
    }

    def __init__(self, jobs):
        self.jobs = jobs
        self.requests = []

    def __enter__(self):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # keep pytest output clean

            def respond(self, status, payload):
                self._send(status, json.dumps(payload).encode("utf-8"))

            def raw(self, status, body):
                self._send(status, body.encode("utf-8"))

            def _send(self, status, body):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def selected(self, items):
                """`items`, filtered by this request's labelSelector.

                FILTER, like the API server: the Jobs a verb receives are the
                selection its own selector made, not every Job on the cluster.
                Without this a test cannot tell a verb that respects its scope
                from one that does not.
                """
                query = parse_qs(urlsplit(self.path).query)
                selector = (query.get("labelSelector") or [""])[0]
                key, _, value = selector.partition("=")
                return [
                    item
                    for item in items
                    if ((item.get("metadata") or {}).get("labels") or {}).get(key)
                    == value
                ]

            def record(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                fixture.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "authorization": self.headers.get("Authorization"),
                        "body": json.loads(body) if body else None,
                    }
                )

            def do_GET(self):
                self.record()
                if "/cronjobs/" in self.path:
                    cronjob = self.path.rstrip("/").rpartition("/")[2]
                    self.respond(
                        200, {"spec": {"jobTemplate": fixture.TEMPLATES[cronjob]}}
                    )
                    return
                try:
                    fixture.jobs(self)
                except OSError:
                    # A read that outlives the client's timeout answers a
                    # socket the client has already given up on. That is the
                    # timeout case finishing, not a failure the test means.
                    pass

            def do_POST(self):
                self.record()
                self.respond(201, {"metadata": {"name": "sre-bot-self-upgrade-abc12"}})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        return False

    @property
    def url(self):
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def posts(self):
        return [r for r in self.requests if r["method"] == "POST"]


def _stand_in_kubeconfig(url):
    """A kubeconfig that points the REAL `_client` at the stand-in API."""
    return {
        "clusters": [{"cluster": {"server": url}}],
        "users": [{"user": {"token": "upgrade-token"}}],
    }


def _hang_up(handler):
    """A closed connection: the socket dies before any response byte."""
    handler.connection.close()


def _stall(handler):
    """A read that outlives the client's timeout: the reply is never seen."""
    time.sleep(4.0)
    handler.respond(200, {"items": []})


def _misframe_with_the_token(handler):
    """Malformed chunk framing that names the credential.

    Transport exceptions can quote peer-supplied bytes verbatim -- this frame
    names the token, so a refusal that interpolated the exception message
    would echo it back into the channel.
    """
    handler.send_response(200)
    handler.send_header("Transfer-Encoding", "chunked")
    handler.end_headers()
    handler.wfile.write(b"upgrade-token: definitely not a chunk length\r\n\r\n")


# Every way the running-Jobs read can fail, each of which must refuse the
# call: a connector that cannot know whether an upgrade is running must not
# start one on top of that not-knowing.
CONCURRENCY_READ_FAILURES = [
    ("http_401", lambda handler: handler.respond(401, {})),
    ("http_403", lambda handler: handler.respond(403, {})),
    ("http_503", lambda handler: handler.respond(503, {})),
    ("closed_connection", _hang_up),
    ("read_timeout", _stall),
    ("reflected_chunk_framing", _misframe_with_the_token),
    ("malformed_json", lambda handler: handler.raw(200, "not a jobs list {")),
    ("non_object_payload", lambda handler: handler.respond(200, ["not", "an", "object"])),
    ("missing_items", lambda handler: handler.respond(200, {})),
    ("non_list_items", lambda handler: handler.respond(200, {"items": "not a list"})),
]

# Both verbs: each one carries its own `_start_job_from` call site, so each
# failure is exercised through each verb rather than assumed shared.
VERBS = [
    ("upgrade_self", "sre-bot-self-upgrade", JOB_TEMPLATE),
    ("upgrade_platform", "platform-upgrade", PLATFORM_JOB_TEMPLATE),
]


@pytest.mark.parametrize("verb", ["upgrade_self", "upgrade_platform"])
@pytest.mark.parametrize("case,respond_with", CONCURRENCY_READ_FAILURES)
def test_a_failed_concurrency_read_refuses_rather_than_starting_a_job(
    tmp_path, monkeypatch, verb, case, respond_with
):
    """The race this guard exists for is one Job started on an unknown list.

    A concurrency read that fails is not an empty list. An HTTP refusal, a
    transport failure and a malformed list all mean the connector cannot know
    whether an upgrade is already running -- and starting one anyway is
    exactly the overlapping run the guard exists to prevent. These drive the
    real `_client` and the real `_start_job_from`, so the path that refuses is
    the path production takes, and the stand-in records every request, so a
    refusal is provable by the absence of a Job-creation POST.
    """
    # Short enough that `read_timeout` is a timeout, not a wait; long enough
    # that every other case answers well inside it under load.
    monkeypatch.setenv("K8S_TIMEOUT_SECONDS", "2.0")
    with _ExternalAPI(respond_with) as api:
        srv = _load(tmp_path, kubeconfig=_stand_in_kubeconfig(api.url))
        with pytest.raises(ToolError) as excinfo:
            getattr(srv, verb)()
        # A refusal is reported back into the channel. It carries the generic
        # status or transport cause -- never the credential, never the raw body
        # the API sent.
        refusal = str(excinfo.value)
        assert "upgrade-token" not in refusal
        if case == "malformed_json":
            assert "not a jobs list" not in refusal
        if case == "reflected_chunk_framing":
            assert "definitely not a chunk length" not in refusal
        assert api.posts() == [], f"{verb} must not start a Job when {case}"


@pytest.mark.parametrize("verb,cronjob,template", VERBS)
def test_a_well_formed_concurrency_read_permits_exactly_one_job_post(
    tmp_path, monkeypatch, verb, cronjob, template
):
    """Only a well-formed empty list means "no upgrade is running."

    The permit has to be earned by the read: the verb reads its OWN CronJob,
    then one Job-creation POST, carrying that template verbatim -- the
    property the namespace-wide jobs grant rests on -- with the bearer token
    from the mounted kubeconfig, through the real client.
    """

    def nothing_running(handler):
        handler.respond(200, {"items": handler.selected([])})

    with _ExternalAPI(nothing_running) as api:
        srv = _load(tmp_path, kubeconfig=_stand_in_kubeconfig(api.url))
        result = json.loads(getattr(srv, verb)())
        assert result["ok"] is True
        reads = [
            r["path"]
            for r in api.requests
            if r["method"] == "GET" and "/cronjobs/" in r["path"]
        ]
        assert reads == [f"/apis/batch/v1/namespaces/curie/cronjobs/{cronjob}"]
        posts = api.posts()
        assert len(posts) == 1
        assert posts[0]["path"] == "/apis/batch/v1/namespaces/curie/jobs"
        assert posts[0]["authorization"] == "Bearer upgrade-token"
        assert posts[0]["body"]["spec"] == template["spec"]
        assert posts[0]["body"]["metadata"]["generateName"] == f"{cronjob}-"


@pytest.mark.parametrize("verb,cronjob", [(v, c) for v, c, _ in VERBS])
def test_an_active_matching_job_refuses_through_the_real_client(
    tmp_path, monkeypatch, verb, cronjob
):
    """Two overlapping runs race on creating the version; the read stops one."""

    def running(handler):
        job = {
            "metadata": {
                "name": "in-flight",
                "labels": {"curie.dev/self-upgrade-of": cronjob},
            },
            "status": {"active": 1},
        }
        handler.respond(200, {"items": handler.selected([job])})

    with _ExternalAPI(running) as api:
        srv = _load(tmp_path, kubeconfig=_stand_in_kubeconfig(api.url))
        with pytest.raises(ToolError) as excinfo:
            getattr(srv, verb)()
        assert "in-flight" in str(excinfo.value)
        assert api.posts() == []


def test_a_completed_job_in_the_concurrency_read_does_not_block(tmp_path, monkeypatch):
    """A finished upgrade is not a running one: the list may name it and still permit."""

    def finished(handler):
        job = {
            "metadata": {
                "name": "old",
                "labels": {"curie.dev/self-upgrade-of": "sre-bot-self-upgrade"},
            },
            "status": {"succeeded": 1},
        }
        handler.respond(200, {"items": handler.selected([job])})

    with _ExternalAPI(finished) as api:
        srv = _load(tmp_path, kubeconfig=_stand_in_kubeconfig(api.url))
        assert json.loads(srv.upgrade_self())["ok"] is True
        assert len(api.posts()) == 1


def test_the_other_verbs_concurrency_read_stays_independent(tmp_path, monkeypatch):
    """A running bundle redeploy must not refuse a platform upgrade.

    The two verbs run different templates against different CronJobs and are
    independent; the guard is scoped to the CronJob being started. The
    stand-in filters by the request's labelSelector exactly as the API server
    does, so what reaches the platform verb is the empty selection its own
    selector made -- not the other verb's in-flight Job.
    """

    def running_for_the_bundle(handler):
        job = {
            "metadata": {
                "name": "in-flight",
                "labels": {"curie.dev/self-upgrade-of": "sre-bot-self-upgrade"},
            },
            "status": {"active": 1},
        }
        handler.respond(200, {"items": handler.selected([job])})

    with _ExternalAPI(running_for_the_bundle) as api:
        srv = _load(tmp_path, kubeconfig=_stand_in_kubeconfig(api.url))
        result = json.loads(srv.upgrade_platform())
        assert result["ok"] is True
        posts = api.posts()
        assert len(posts) == 1
        assert posts[0]["body"]["metadata"]["generateName"] == "platform-upgrade-"
        assert posts[0]["body"]["spec"] == PLATFORM_JOB_TEMPLATE["spec"]


def test_both_verbs_still_declare_gated_destructive_writes(tmp_path):
    """The approval gate reads these declarations off the MCP surface.

    A refusal must not quietly become an ungated path: both verbs stay
    destructive, non-idempotent writes, and the read stays a read.
    """
    srv = _load(tmp_path)
    tools = {tool.name: tool for tool in anyio.run(srv.mcp.list_tools)}
    for verb in ("upgrade_self", "upgrade_platform"):
        assert tools[verb].annotations.destructive_hint is True
        assert tools[verb].annotations.read_only_hint is False
        assert tools[verb].annotations.idempotent_hint is False
    assert tools["latest_release"].annotations.read_only_hint is True
