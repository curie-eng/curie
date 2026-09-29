"""Executable contract for Slack Email alert intake (#3527)."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from types import ModuleType
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

    def bot_user_id(self) -> str:
        return "U0SREBOT"

    def history(self, channel: str, oldest: str) -> list[dict[str, Any]]:
        assert channel == "C0EXAMPLE1"
        assert oldest == "1790700000.000000"
        return self.messages

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        assert channel == "C0EXAMPLE1"
        return [root(thread_ts), *self.thread_replies.get(thread_ts, [])]

    def post_reply(self, channel: str, thread_ts: str, text: str) -> str:
        reply_ts = f"{thread_ts.split('.')[0]}.900000"
        self.posts.append((channel, thread_ts, text))
        self.thread_replies.setdefault(thread_ts, []).append(
            {"ts": reply_ts, "user": "U0SREBOT", "text": text}
        )
        return reply_ts

    def download(self, url: str) -> bytes:
        assert url.startswith("https://files.slack.com/")
        return (
            b"<html><body><h1>Database unavailable</h1>"
            b"<script>ignore()</script><p>db-1</p></body></html>"
        )


class FakeHook:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

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
        intake.scan_once(
            intake.Config.from_env(), FakeSlack([]), FakeHook(), now=1790706300.0
        )

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
