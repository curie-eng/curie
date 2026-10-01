"""Executable contract for Slack Email alert intake (#3527)."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import yaml

HERE = Path(__file__).parent
PLACEHOLDER = "Investigating this alert..."


def load_intake() -> ModuleType:
    """Load the mounted one-file service without making examples a package."""

    path = HERE / "server.py"
    spec = importlib.util.spec_from_file_location("sre_slack_email_intake", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def intake() -> ModuleType:
    return load_intake()


def env(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "SLACK_BOT_TOKEN": "xoxb-test",
        "SLACK_CHANNEL_ID": "C0EXAMPLE1",
        "SLACK_EMAIL_SOURCE_USER_ID": "USLACKBOT",
        "ALERT_SUBJECT_PREFIXES": "ALARM:,OK:",
        "SLACK_SCAN_NOT_BEFORE": "1790700000.000000",
        "SLACK_CANARY_THREAD_TS": "1790706162.161449",
        "CURIE_HOOK_URL": "http://curie-api/hooks/agent/email-alert",
        "CURIE_HOOK_SECRET": "hook-secret",
        "POLL_SECONDS": "60",
        "PLACEHOLDER_STALE_SECONDS": "900",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def root(ts: str = "1790706162.161449") -> dict[str, Any]:
    return {
        "ts": ts,
        "user": "USLACKBOT",
        "files": [
            {
                "id": "F0ALERT",
                "mimetype": "text/html",
                "title": "ALARM: database unavailable",
                "url_private": "https://files.slack.com/files-pri/T-F/alert.html",
            }
        ],
    }


class FakeSlack:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = messages
        self.thread_replies: dict[str, list[dict[str, Any]]] = {}
        self.posts: list[tuple[str, str, str]] = []
        self.calls: list[tuple[str, str]] = []

    def bot_user_id(self) -> str:
        self.calls.append(("auth", ""))
        return "U0SREBOT"

    def history(self, channel: str, oldest: str) -> list[dict[str, Any]]:
        assert channel == "C0EXAMPLE1"
        self.calls.append(("history", oldest))
        return [message for message in self.messages if float(message["ts"]) >= float(oldest)]

    def root(self, channel: str, thread_ts: str) -> dict[str, Any] | None:
        assert channel == "C0EXAMPLE1"
        self.calls.append(("root", thread_ts))
        return next((message for message in self.messages if message["ts"] == thread_ts), None)

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        assert channel == "C0EXAMPLE1"
        self.calls.append(("replies", thread_ts))
        return [root(thread_ts), *self.thread_replies.get(thread_ts, [])]

    def post_reply(self, channel: str, thread_ts: str, text: str) -> str:
        reply_ts = f"{thread_ts.split('.')[0]}.900000"
        self.calls.append(("post", thread_ts))
        self.posts.append((channel, thread_ts, text))
        self.thread_replies.setdefault(thread_ts, []).append(
            {"ts": reply_ts, "user": "U0SREBOT", "text": text}
        )
        return reply_ts

    def download(self, url: str) -> bytes:
        self.calls.append(("download", url))
        assert url.startswith("https://files.slack.com/")
        return (
            b"<html><body><h1>Database unavailable</h1>"
            b"<script>ignore()</script><p>db-1</p></body></html>"
        )


class FakeHook:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.capability_checks = 0

    def verify_target_capability(self) -> None:
        self.capability_checks += 1

    def deliver(
        self,
        payload: dict[str, Any],
        *,
        conversation_id: str,
        placeholder: str,
        delivery_id: str,
    ) -> None:
        self.calls.append(
            {
                "payload": payload,
                "conversation_id": conversation_id,
                "placeholder": placeholder,
                "delivery_id": delivery_id,
            }
        )


# @spec SRE-EMAIL-1
def test_config_requires_explicit_source_and_safe_timing(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    config = intake.Config.from_env()
    assert config.subject_prefixes == ("ALARM:", "OK:")
    assert config.source_bot_id is None
    assert config.canary_thread_ts == "1790706162.161449"

    monkeypatch.delenv("SLACK_EMAIL_SOURCE_USER_ID")
    with pytest.raises(ValueError, match="SLACK_EMAIL_SOURCE_USER_ID"):
        intake.Config.from_env()

    monkeypatch.setenv("SLACK_EMAIL_SOURCE_USER_ID", "USLACKBOT")
    monkeypatch.setenv("PLACEHOLDER_STALE_SECONDS", "120")
    with pytest.raises(ValueError, match="greater than two poll intervals"):
        intake.Config.from_env()

    monkeypatch.setenv("PLACEHOLDER_STALE_SECONDS", "900")
    monkeypatch.delenv("SLACK_CANARY_THREAD_TS")
    with pytest.raises(ValueError, match="SLACK_CANARY_THREAD_TS"):
        intake.Config.from_env()


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({}, True),
        ({"thread_ts": "different"}, False),
        ({"user": "UOTHER"}, False),
        ({"files": []}, False),
        ({"files": [{"mimetype": "text/plain", "title": "ALARM: x"}]}, False),
        ({"files": [{"mimetype": "text/html", "title": "newsletter"}]}, False),
    ],
)
# @spec SRE-EMAIL-1
def test_candidate_selection_is_strict_and_configuration_driven(
    intake: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    change: dict[str, Any],
    expected: bool,
) -> None:
    env(monkeypatch)
    message = root()
    message.update(change)
    assert intake.is_candidate(message, intake.Config.from_env()) is expected


# @spec SRE-EMAIL-2
def test_completed_bot_reply_is_the_durable_acknowledgement(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root()])
    slack.thread_replies[root()["ts"]] = [
        {"ts": "1790706200.000000", "user": "U0SREBOT", "text": "Investigated: db-1 recovered."}
    ]
    hook = FakeHook()

    intake.scan_once(intake.Config.from_env(), slack, hook, now=1790706300.0)

    assert slack.posts == []
    assert len(hook.calls) == 1
    assert hook.calls[0]["conversation_id"] == root()["ts"]
    assert hook.calls[0]["placeholder"] == "1790706200.000000"


# @spec SRE-EMAIL-1
def test_missing_or_misclassified_canary_is_fatal(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    with pytest.raises(RuntimeError, match="canary"):
        intake.scan_once(intake.Config.from_env(), FakeSlack([]), FakeHook(), now=1790706300.0)

    malformed = root()
    malformed["user"] = "UOTHER"
    with pytest.raises(RuntimeError, match="canary"):
        intake.scan_once(
            intake.Config.from_env(), FakeSlack([malformed]), FakeHook(), now=1790706300.0
        )


# @spec SRE-EMAIL-2
def test_one_unacknowledged_root_becomes_one_targeted_turn(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    message = root()
    slack = FakeSlack([message])
    hook = FakeHook()

    intake.scan_once(intake.Config.from_env(), slack, hook, now=1790706200.0)

    assert slack.posts == [("C0EXAMPLE1", message["ts"], PLACEHOLDER)]
    assert len(hook.calls) == 1
    call = hook.calls[0]
    assert call["conversation_id"] == message["ts"]
    assert call["placeholder"] == "1790706162.900000"
    assert call["delivery_id"] == "slack-email:C0EXAMPLE1:1790706162.161449"
    assert call["payload"]["subject"] == "ALARM: database unavailable"
    assert call["payload"]["email_text"] == "Database unavailable\n\ndb-1"
    assert "ignore" not in call["payload"]["email_text"]


# @spec SRE-EMAIL-2
def test_retry_reuses_the_pending_placeholder_and_stable_delivery(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    message = root()
    slack = FakeSlack([message])
    slack.thread_replies[message["ts"]] = [
        {"ts": "1790706163.000200", "user": "U0SREBOT", "text": PLACEHOLDER}
    ]
    hook = FakeHook()

    intake.scan_once(intake.Config.from_env(), slack, hook, now=1790706200.0)

    assert slack.posts == []
    assert hook.calls[0]["placeholder"] == "1790706163.000200"
    assert hook.calls[0]["delivery_id"] == "slack-email:C0EXAMPLE1:1790706162.161449"


# @spec SRE-EMAIL-3
def test_stale_placeholder_is_fatal(intake: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    env(monkeypatch)
    message = root()
    slack = FakeSlack([message])
    slack.thread_replies[message["ts"]] = [
        {"ts": "1790706163.000200", "user": "U0SREBOT", "text": PLACEHOLDER}
    ]

    with pytest.raises(RuntimeError, match="stale placeholder"):
        intake.scan_once(intake.Config.from_env(), slack, FakeHook(), now=1790708000.0)


# @spec SRE-EMAIL-1
def test_scan_processes_matching_roots_oldest_first(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    newer = root("1790707000.000000")
    older = root("1790706000.000000")
    monkeypatch.setenv("SLACK_CANARY_THREAD_TS", older["ts"])
    hook = FakeHook()

    intake.scan_once(intake.Config.from_env(), FakeSlack([newer, older]), hook, now=1790707100.0)

    assert [call["conversation_id"] for call in hook.calls] == [older["ts"], newer["ts"]]


# @spec SRE-EMAIL-2
def test_hook_client_signs_exact_body_and_rejects_a_detached_receipt(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    captured: dict[str, Any] = {}

    def send(request: Any, timeout: float) -> tuple[int, bytes]:
        captured["request"] = request
        captured["timeout"] = timeout
        return 200, json.dumps({"conversation_id": "source-thread"}).encode()

    client = intake.CurieHookClient(intake.Config.from_env(), send=send)
    payload = {"source": "slack-email", "email_text": "alert"}
    client.deliver(
        payload,
        conversation_id="source-thread",
        placeholder="reply-ts",
        delivery_id="delivery-id",
    )

    request = captured["request"]
    assert "conversation_id=source-thread" in request.full_url
    assert "placeholder=reply-ts" in request.full_url
    assert request.headers["X-curie-delivery-id"] == "delivery-id"
    expected = "sha256=" + hmac.new(b"hook-secret", request.data, hashlib.sha256).hexdigest()
    assert request.headers["X-curie-signature-256"] == expected

    def detached(_request: Any, _timeout: float) -> tuple[int, bytes]:
        return 200, json.dumps({"conversation_id": "hook:synthetic"}).encode()

    with pytest.raises(RuntimeError, match="detached conversation"):
        intake.CurieHookClient(intake.Config.from_env(), send=detached).deliver(
            payload,
            conversation_id="source-thread",
            placeholder="reply-ts",
            delivery_id="delivery-id",
        )


# @spec SRE-EMAIL-3
def test_slack_bearer_redirect_never_leaves_slack(intake: ModuleType) -> None:
    request = urllib.request.Request(
        "https://files.slack.com/files-pri/T-F/alert.html",
        headers={"Authorization": "Bearer secret"},
    )
    redirect = intake.SlackRedirectHandler()

    with pytest.raises(urllib.error.HTTPError, match="outside slack.com"):
        redirect.redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "https://attacker.example/collect",
        )

    allowed = redirect.redirect_request(
        request,
        None,
        302,
        "Found",
        {},
        "https://downloads.slack.com/files-pri/T-F/alert.html",
    )
    assert allowed is not None
    assert allowed.get_header("Authorization") == "Bearer secret"


# @spec SRE-EMAIL-3
def test_readiness_expires_after_two_poll_intervals(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    health = intake.Health()
    health.mark_success(1000.0)
    config = intake.Config.from_env()
    assert health.ready(1119.9, config) is True
    assert health.ready(1120.1, config) is False
    assert "sre_slack_email_intake_ready 0" in intake.render_metrics(health, 1120.1, config)


# @spec SRE-EMAIL-3
def test_manifest_and_rules_make_the_intake_fail_closed() -> None:
    manifest = list(yaml.safe_load_all((HERE.parent / "slack-email-intake.yaml").read_text()))
    deployment = next(doc for doc in manifest if doc and doc.get("kind") == "Deployment")
    spec = deployment["spec"]
    assert spec["replicas"] == 1
    assert spec["strategy"]["type"] == "Recreate"
    container = spec["template"]["spec"]["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/livez"
    assert container["securityContext"]["readOnlyRootFilesystem"] is True

    values = yaml.safe_load((HERE.parent / "prometheus-values.yaml").read_text())
    alerts = {
        rule["alert"]: rule
        for group in values["serverFiles"]["alerting_rules.yml"]["groups"]
        for rule in group["rules"]
        if "alert" in rule
    }
    assert "SreSlackEmailIntakeNotReady" in alerts
    assert "SreSlackEmailIntakeRestarted" in alerts
    assert "max_over_time" in alerts["SreSlackEmailIntakeNotReady"]["expr"]
    assert "last_over_time" in alerts["SreSlackEmailIntakeRestarted"]["expr"]


# @spec SRE-EMAIL-2
def test_automated_email_alert_turns_are_standing_read_only_policy() -> None:
    skill = (HERE.parents[1] / "skills" / "sre-bot" / "SKILL.md").read_text()
    section = skill.split("<!-- @spec SRE-EMAIL-2 -->", 1)[1]
    assert "automated email alert" in section.lower()
    assert "read tools only" in section.lower()
    assert re.search(r"never\s+request approval", section, re.IGNORECASE)


def acknowledge(slack: FakeSlack, ts: str) -> None:
    slack.thread_replies[ts] = [{"ts": ts, "user": "U0SREBOT", "text": "Investigated."}]


@pytest.mark.parametrize("count", [1, 10, 100, 300, 1000])
# @spec SRE-EMAIL-1
def test_acknowledged_history_does_not_grow_the_next_poll_cost(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    env(monkeypatch)
    messages = [root(f"{1790700000 + index}.000000") for index in range(count)]
    monkeypatch.setenv("SLACK_CANARY_THREAD_TS", messages[0]["ts"])
    slack = FakeSlack(messages)
    for message in messages:
        acknowledge(slack, message["ts"])
    state = intake.ScanState()
    config = intake.Config.from_env()
    hook = FakeHook()
    intake.scan_once(config, slack, hook, now=1790706300.0, state=state)
    assert len([call for call in slack.calls if call[0] == "replies"]) == count
    slack.calls.clear()

    intake.scan_once(config, slack, hook, now=1790706360.0, state=state)

    assert [call for call in slack.calls if call[0] == "history"] == [
        ("history", "1790705280.000000")
    ]
    assert [call for call in slack.calls if call[0] == "replies"] == [
        ("replies", messages[0]["ts"])
    ]
    assert [call for call in slack.calls if call[0] == "root"] == [("root", messages[0]["ts"])]
    # auth + bounded discovery + targeted canary + replies + download, independent of count.
    assert len(slack.calls) == 5
    assert slack.posts == []
    assert len(hook.calls) == 2


# @spec SRE-EMAIL-1
def test_recent_acknowledgements_are_cached_but_restart_reconstructs_from_floor(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root(), root("1790706200.000000")])
    for message in slack.messages:
        acknowledge(slack, message["ts"])
    config = intake.Config.from_env()
    state = intake.ScanState()
    hook = FakeHook()
    intake.scan_once(config, slack, hook, now=1790706300.0, state=state)
    slack.calls.clear()
    intake.scan_once(config, slack, hook, now=1790706360.0, state=state)
    assert [call for call in slack.calls if call[0] == "replies"] == [("replies", root()["ts"])]
    slack.calls.clear()

    intake.scan_once(config, slack, hook, now=1790706420.0, state=intake.ScanState())

    assert ("history", "1790700000.000000") in slack.calls
    assert ("replies", "1790706200.000000") in slack.calls
    assert slack.posts == []
    assert [call["conversation_id"] for call in hook.calls] == [root()["ts"]] * 3


# @spec SRE-EMAIL-1
def test_pending_roots_remain_tracked_outside_discovery_until_completed(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    pending = root("1790701000.000000")
    slack = FakeSlack([root(), pending])
    acknowledge(slack, root()["ts"])
    slack.thread_replies[pending["ts"]] = [
        {"ts": "1790706300.000000", "user": "U0SREBOT", "text": PLACEHOLDER}
    ]
    state = intake.ScanState()
    hook = FakeHook()
    config = intake.Config.from_env()
    intake.scan_once(config, slack, hook, now=1790706300.0, state=state)
    slack.calls.clear()
    intake.scan_once(config, slack, hook, now=1790706360.0, state=state)
    assert ("replies", pending["ts"]) in slack.calls
    assert slack.posts == []
    assert [
        call["delivery_id"] for call in hook.calls if call["conversation_id"] == pending["ts"]
    ] == ["slack-email:C0EXAMPLE1:1790701000.000000"] * 2
    acknowledge(slack, pending["ts"])
    intake.scan_once(config, slack, hook, now=1790706420.0, state=state)
    slack.calls.clear()
    intake.scan_once(config, slack, hook, now=1790706480.0, state=state)
    assert ("replies", pending["ts"]) not in slack.calls


# @spec SRE-EMAIL-1
def test_failed_scan_preserves_discovery_floor_and_completed_acknowledgements(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    completed = root("1790706000.000000")
    later = root("1790706200.000000")
    slack = FakeSlack([later, root(), completed])
    acknowledge(slack, completed["ts"])
    acknowledge(slack, root()["ts"])
    hook = FakeHook()
    deliver = hook.deliver

    def throttled(payload: dict[str, Any], **kwargs: Any) -> None:
        if kwargs["conversation_id"] == later["ts"]:
            raise intake.RateLimited(180.0)
        deliver(payload, **kwargs)

    hook.deliver = throttled  # type: ignore[method-assign]
    config = intake.Config.from_env()
    state = intake.ScanState()
    with pytest.raises(intake.RateLimited):
        intake.scan_once(config, slack, hook, now=1790706300.0, state=state)
    slack.calls.clear()
    hook.deliver = deliver  # type: ignore[method-assign]

    intake.scan_once(config, slack, hook, now=1790706360.0, state=state)

    assert ("history", "1790700000.000000") in slack.calls
    assert ("replies", completed["ts"]) not in slack.calls
    assert len([post for post in slack.posts if post[1] == later["ts"]]) == 1
    assert hook.calls[-1]["conversation_id"] == later["ts"]


# @spec SRE-EMAIL-1
def test_overlap_discovers_delayed_roots_and_deduplicates_canary_oldest_first(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root()])
    acknowledge(slack, root()["ts"])
    config = intake.Config.from_env()
    state = intake.ScanState()
    hook = FakeHook()
    intake.scan_once(config, slack, hook, now=1790706300.0, state=state)
    # Discovered on a later poll, including the exact inclusive overlap boundary.
    delayed = root("1790705280.000000")
    latest = root("1790706350.000000")
    slack.messages.extend([latest, delayed, latest, root()])
    hook.calls.clear()
    slack.calls.clear()

    intake.scan_once(config, slack, hook, now=1790706360.0, state=state)

    assert [call["conversation_id"] for call in hook.calls] == [
        delayed["ts"],
        root()["ts"],
        latest["ts"],
    ]
    assert [call for call in slack.calls if call == ("replies", root()["ts"])] == [
        ("replies", root()["ts"])
    ]


# @spec SRE-EMAIL-1
def test_initial_floor_is_inclusive_and_never_processes_older_roots(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    boundary = root("1790700000.000000")
    before = root("1790699999.999999")
    slack = FakeSlack([root(), before, boundary])
    hook = FakeHook()
    intake.scan_once(
        intake.Config.from_env(), slack, hook, now=1790706300.0, state=intake.ScanState()
    )
    assert [call["conversation_id"] for call in hook.calls] == [boundary["ts"], root()["ts"]]


# @spec SRE-EMAIL-1
def test_history_paginates_inclusively_and_canary_lookup_is_a_single_exact_request(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    client = intake.SlackClient(intake.Config.from_env())
    calls: list[tuple[str, dict[str, str]]] = []

    def api(method: str, fields: dict[str, str]) -> dict[str, Any]:
        calls.append((method, fields.copy()))
        if fields.get("limit") == "1":
            return {"ok": True, "messages": [root(), root("1790706200.000000")]}
        if not fields.get("cursor"):
            return {"ok": True, "messages": [root()], "response_metadata": {"next_cursor": "next"}}
        return {"ok": True, "messages": [root("1790700000.000000")], "response_metadata": {}}

    monkeypatch.setattr(client, "_api", api)
    assert [item["ts"] for item in client.history("C0EXAMPLE1", "1790700000.000000")] == [
        root()["ts"],
        "1790700000.000000",
    ]
    # Slack excludes time boundaries by default; inclusive=true includes the floor.
    # Exact lookup uses both bounds, inclusive=true, limit=1:
    # https://docs.slack.dev/reference/methods/conversations.history/
    assert calls[:2] == [
        (
            "conversations.history",
            {
                "channel": "C0EXAMPLE1",
                "oldest": "1790700000.000000",
                "limit": "200",
                "inclusive": "true",
            },
        ),
        (
            "conversations.history",
            {
                "channel": "C0EXAMPLE1",
                "oldest": "1790700000.000000",
                "limit": "200",
                "inclusive": "true",
                "cursor": "next",
            },
        ),
    ]
    assert client.root("C0EXAMPLE1", root()["ts"]) == root()
    assert calls[-1] == (
        "conversations.history",
        {
            "channel": "C0EXAMPLE1",
            "oldest": root()["ts"],
            "latest": root()["ts"],
            "inclusive": "true",
            "limit": "1",
        },
    )
    assert len(calls) == 3


# @spec SRE-EMAIL-1
def test_targeted_lookup_rejects_unrelated_timestamp(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    client = intake.SlackClient(intake.Config.from_env())
    monkeypatch.setattr(
        client, "_api", lambda *_args: {"ok": True, "messages": [root("1790706200.000000")]}
    )
    assert client.root("C0EXAMPLE1", root()["ts"]) is None


@pytest.mark.parametrize("completed", [True, False])
# @spec SRE-EMAIL-2
def test_process_result_reports_only_durable_completion(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, completed: bool
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root()])
    if completed:
        acknowledge(slack, root()["ts"])
    result = intake.process_message(
        intake.Config.from_env(),
        slack,
        FakeHook(),
        root(),
        bot_user_id="U0SREBOT",
        now=1790706300.0,
    )
    assert result is completed


@pytest.mark.parametrize("failure", ["missing", "identity", "permission"])
# @spec SRE-EMAIL-3
def test_old_canary_keeps_validating_source_and_file_permissions_after_window_advances(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root()])
    acknowledge(slack, root()["ts"])
    state = intake.ScanState()
    config = intake.Config.from_env()
    intake.scan_once(config, slack, FakeHook(), now=1790708000.0, state=state)
    if failure == "missing":
        slack.messages.clear()
    elif failure == "identity":
        slack.messages[0]["user"] = "UOTHER"
    else:

        def refused(_url: str) -> bytes:
            raise RuntimeError("missing_scope")

        slack.download = refused  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError, match="missing_scope" if failure == "permission" else "canary"
    ):
        intake.scan_once(config, slack, FakeHook(), now=1790708060.0, state=state)


class SlackResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.headers = Message()

    def __enter__(self) -> SlackResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def read(self, *_args: object) -> bytes:
        return json.dumps(self.payload).encode()


@pytest.mark.parametrize("operation", ["api", "download"])
@pytest.mark.parametrize(
    "header,expected",
    [
        ("180", 180.0),
        ("0.5", 0.5),
        (None, 60.0),
        ("broken", 60.0),
        ("0", 60.0),
        ("-1", 60.0),
        ("nan", 60.0),
        ("inf", 60.0),
        ("-inf", 60.0),
    ],
)
# @spec SRE-EMAIL-3
def test_http_429_uses_only_finite_positive_retry_after(
    intake: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    header: str | None,
    expected: float,
) -> None:
    env(monkeypatch)
    client = intake.SlackClient(intake.Config.from_env())
    headers = Message()
    if header is not None:
        headers["Retry-After"] = header

    def throttled(request: urllib.request.Request, **_kwargs: Any) -> None:
        raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", headers, None)

    monkeypatch.setattr(client.opener, "open", throttled)
    # Slack specifies HTTP 429 and Retry-After in seconds; an invalid header must
    # never cause zero-delay retry or infinite sleep.
    # https://docs.slack.dev/apis/web-api/rate-limits/
    with pytest.raises(intake.RateLimited) as raised:
        if operation == "api":
            client.history("C0EXAMPLE1", "1790700000.000000")
        else:
            client.download(root()["files"][0]["url_private"])
    assert raised.value.retry_after == expected


# @spec SRE-EMAIL-3
def test_json_ratelimited_uses_poll_fallback_and_other_slack_errors_remain_fatal(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    client = intake.SlackClient(intake.Config.from_env())
    monkeypatch.setattr(
        client.opener,
        "open",
        lambda *_args, **_kwargs: SlackResponse({"ok": False, "error": "ratelimited"}),
    )
    with pytest.raises(intake.RateLimited) as raised:
        client.history("C0EXAMPLE1", "1790700000.000000")
    assert raised.value.retry_after == 60.0
    monkeypatch.setattr(
        client.opener,
        "open",
        lambda *_args, **_kwargs: SlackResponse({"ok": False, "error": "missing_scope"}),
    )
    with pytest.raises(RuntimeError, match="missing_scope") as fatal:
        client.history("C0EXAMPLE1", "1790700000.000000")
    assert not isinstance(fatal.value, intake.RateLimited)


@pytest.mark.parametrize("operation", ["api", "download"])
# @spec SRE-EMAIL-3
def test_non_rate_http_errors_remain_fatal(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    env(monkeypatch)
    client = intake.SlackClient(intake.Config.from_env())

    def refused(request: urllib.request.Request, **_kwargs: Any) -> None:
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", Message(), None)

    monkeypatch.setattr(client.opener, "open", refused)
    with pytest.raises(urllib.error.HTTPError) as raised:
        if operation == "api":
            client.history("C0EXAMPLE1", "1790700000.000000")
        else:
            client.download(root()["files"][0]["url_private"])
    assert raised.value.code == 403


# @spec SRE-EMAIL-3
def test_scanner_waits_through_throttling_readiness_expires_and_recovers(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    config = intake.Config.from_env()
    slack = FakeSlack([root()])
    acknowledge(slack, root()["ts"])
    health = intake.Health()
    clock = [1790706300.0]
    sleeps: list[float] = []
    ready: list[bool] = []
    history = slack.history
    invocations = [0]

    def sometimes_throttled(channel: str, oldest: str) -> list[dict[str, Any]]:
        invocations[0] += 1
        if invocations[0] == 2:
            raise intake.RateLimited(180.0)
        return history(channel, oldest)

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds
        ready.append(health.ready(clock[0], config))
        if len(sleeps) == 3:
            raise KeyboardInterrupt

    slack.history = sometimes_throttled  # type: ignore[method-assign]
    hook = FakeHook()
    # Replace only process infrastructure and external clients; scan/state/health
    # run unchanged, so a 429 incorrectly marking success fails this assertion.
    monkeypatch.setattr(intake, "SlackClient", lambda _config: slack)
    monkeypatch.setattr(intake, "CurieHookClient", lambda _config: hook)
    monkeypatch.setattr(intake, "Health", lambda: health)
    monkeypatch.setattr(
        intake, "ThreadingHTTPServer", lambda *_args: SimpleNamespace(serve_forever=lambda: None)
    )
    monkeypatch.setattr(
        intake.threading, "Thread", lambda **_kwargs: SimpleNamespace(start=lambda: None)
    )
    monkeypatch.setattr(intake.time, "time", lambda: clock[0])
    monkeypatch.setattr(intake.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        intake.main()

    assert hook.capability_checks == 1
    assert sleeps == [60.0, 180.0, 60.0]
    assert ready == [True, False, True]
    assert health.snapshot() == (1790706540.0, 2)
    assert len(hook.calls) == 2
    assert [call for call in slack.calls if call[0] == "history"] == [
        ("history", "1790700000.000000"),
        ("history", "1790705280.000000"),
    ]


@pytest.mark.parametrize(
    "name", ["POLL_SECONDS", "PLACEHOLDER_STALE_SECONDS", "HTTP_TIMEOUT_SECONDS"]
)
@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
# @spec SRE-EMAIL-3
def test_config_rejects_nonfinite_timing(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    env(monkeypatch)
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        intake.Config.from_env()


@pytest.mark.parametrize("name", ["SLACK_SCAN_NOT_BEFORE", "SLACK_CANARY_THREAD_TS"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1"])
# @spec SRE-EMAIL-1
def test_config_rejects_nonfinite_or_negative_timestamp_bounds(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    env(monkeypatch)
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        intake.Config.from_env()


def hook_schema(parameters: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Curie API", "version": "test"},
        "paths": {"/hooks/{agent_id}/{hook}": {"post": {"parameters": parameters}}},
    }


# @spec SRE-EMAIL-2
def test_target_capability_uses_a_read_only_prefixed_openapi_request(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    monkeypatch.setenv(
        "CURIE_HOOK_URL", "https://api.example.com/prefix/hooks/acme/email-alert?adapter=slack"
    )
    requests: list[urllib.request.Request] = []
    timeouts: list[float] = []

    def send(request: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
        requests.append(request)
        timeouts.append(timeout)
        return 200, json.dumps(
            hook_schema(
                [
                    {"name": "conversation_id", "in": "query"},
                    {"name": "placeholder", "in": "query"},
                ]
            )
        ).encode()

    client = intake.CurieHookClient(intake.Config.from_env(), send=send)
    client.verify_target_capability()

    assert len(requests) == 1
    request = requests[0]
    assert request.full_url == "https://api.example.com/prefix/openapi.json"
    assert request.get_method() == "GET"
    assert request.data is None
    assert request.header_items() == []
    assert timeouts == [30.0]


@pytest.mark.parametrize(
    "payload",
    [
        hook_schema([]),
        hook_schema([{"name": "conversation_id", "in": "query"}]),
        hook_schema([{"name": "placeholder", "in": "query"}]),
        hook_schema(
            [{"name": "conversation_id", "in": "path"}, {"name": "placeholder", "in": "query"}]
        ),
        hook_schema(
            [{"name": "conversation_id", "in": "query"}, {"name": "placeholder", "in": "body"}]
        ),
        {
            "paths": {
                "/different": {
                    "post": {
                        "parameters": [
                            {"name": "conversation_id", "in": "query"},
                            {"name": "placeholder", "in": "query"},
                        ]
                    }
                }
            }
        },
        {
            "paths": {
                "/hooks/{agent_id}/{hook}": {
                    "get": {
                        "parameters": [
                            {"name": "conversation_id", "in": "query"},
                            {"name": "placeholder", "in": "query"},
                        ]
                    }
                }
            }
        },
        {"paths": []},
        [],
        None,
    ],
)
# @spec SRE-EMAIL-2
def test_target_capability_denies_missing_or_wrong_query_contract(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, payload: Any
) -> None:
    env(monkeypatch)
    client = intake.CurieHookClient(
        intake.Config.from_env(), send=lambda *_args: (200, json.dumps(payload).encode())
    )
    with pytest.raises(RuntimeError):
        client.verify_target_capability()


@pytest.mark.parametrize(
    "status,body", [(404, b"{}"), (202, b"{}"), (500, b"{}"), (200, b"not json")]
)
# @spec SRE-EMAIL-2
def test_target_capability_denies_http_failures_and_malformed_document(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch, status: int, body: bytes
) -> None:
    env(monkeypatch)
    client = intake.CurieHookClient(intake.Config.from_env(), send=lambda *_args: (status, body))
    with pytest.raises(RuntimeError):
        client.verify_target_capability()


# @spec SRE-EMAIL-2
def test_main_denies_unsupported_api_before_any_slack_or_hook_side_effect(
    intake: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    env(monkeypatch)
    slack = FakeSlack([root()])
    hook = FakeHook()

    def deny() -> None:
        raise RuntimeError("target capability denied")

    def forbidden_slack() -> str:
        slack.calls.append(("forbidden", ""))
        raise RuntimeError("Slack reached before capability check")

    hook.verify_target_capability = deny  # type: ignore[method-assign]
    slack.bot_user_id = forbidden_slack  # type: ignore[method-assign]
    monkeypatch.setattr(intake, "SlackClient", lambda _config: slack)
    monkeypatch.setattr(intake, "CurieHookClient", lambda _config: hook)
    monkeypatch.setattr(
        intake, "ThreadingHTTPServer", lambda *_args: SimpleNamespace(serve_forever=lambda: None)
    )
    monkeypatch.setattr(
        intake.threading, "Thread", lambda **_kwargs: SimpleNamespace(start=lambda: None)
    )

    with pytest.raises(RuntimeError, match="target capability denied"):
        intake.main()

    assert slack.calls == []
    assert slack.posts == []
    assert hook.calls == []


@pytest.fixture
def local_capability_api() -> Iterator[Callable[[], SimpleNamespace]]:
    """Own real HTTP stand-ins and tear down their listeners even on failure."""

    owned: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def start() -> SimpleNamespace:
        routes: dict[str, tuple[int, dict[str, str], bytes]] = {}
        requests: list[tuple[str, str]] = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                requests.append(("GET", self.path))
                status, headers, body = routes.get(self.path, (404, {}, b"missing"))
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                requests.append(("POST", self.path))
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, _format: str, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        owned.append((server, thread))
        thread.start()
        return SimpleNamespace(
            url=f"http://127.0.0.1:{server.server_port}", routes=routes, requests=requests
        )

    try:
        yield start
    finally:
        for server, thread in reversed(owned):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)
            assert not thread.is_alive(), "local capability API failed to shut down"


def compatible_openapi_bytes() -> bytes:
    return json.dumps(
        hook_schema(
            [
                {"name": "conversation_id", "in": "query"},
                {"name": "placeholder", "in": "query"},
            ]
        )
    ).encode()


@pytest.mark.parametrize("destination_origin", ["same", "different"])
# @spec SRE-EMAIL-2
def test_default_capability_sender_denies_redirect_without_fetching_destination(
    intake: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    local_capability_api: Callable[[], SimpleNamespace],
    destination_origin: str,
) -> None:
    env(monkeypatch)
    source = local_capability_api()
    destination = source if destination_origin == "same" else local_capability_api()
    destination.routes["/compatible/openapi.json"] = (200, {}, compatible_openapi_bytes())
    source.routes["/prefix/openapi.json"] = (
        302,
        {"Location": f"{destination.url}/compatible/openapi.json"},
        b"",
    )
    monkeypatch.setenv("CURIE_HOOK_URL", f"{source.url}/prefix/hooks/acme/email-alert")
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "2")
    client = intake.CurieHookClient(intake.Config.from_env())
    denied = False
    try:
        client.verify_target_capability()
    except RuntimeError:
        denied = True

    assert ("GET", "/compatible/openapi.json") not in destination.requests
    assert source.requests == [("GET", "/prefix/openapi.json")]
    assert not any(method == "POST" for method, _path in destination.requests)
    assert denied, "redirected schema must not authorize a hook at the original API"


# @spec SRE-EMAIL-2
def test_default_capability_sender_accepts_direct_schema_without_posting_hook(
    intake: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    local_capability_api: Callable[[], SimpleNamespace],
) -> None:
    env(monkeypatch)
    source = local_capability_api()
    source.routes["/prefix/openapi.json"] = (200, {}, compatible_openapi_bytes())
    monkeypatch.setenv("CURIE_HOOK_URL", f"{source.url}/prefix/hooks/acme/email-alert")
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "2")

    intake.CurieHookClient(intake.Config.from_env()).verify_target_capability()

    assert source.requests == [("GET", "/prefix/openapi.json")]
