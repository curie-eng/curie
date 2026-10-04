"""Redaction: every secret class is scrubbed at every runner output boundary.

Frozen vectors exercise stdout structured logs and gen_ai span attributes through
a real logging handler and RunTracer exporter. The telemetry tripwires bind every
shared rule to both boundaries. HTTP regressions drive the real boot, translation
and NDJSON channel to protect replies and structured tool results with the runner's
held credential inventory.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import random
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import PROTOCOL_VERSION, Event, parse_ndjson
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner import __main__ as boot
from curie_runner import redact as redact_module
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.redact import (
    REDACTION_BOUNDARIES,
    REDACTION_RULES,
    OutboundRedactor,
    install_stdout_redaction,
    redact_span_attribute,
    redact_text,
)
from curie_runner.session import SessionRunner
from curie_telemetry.redact import (
    REDACTION_RULES as SHARED_REDACTION_RULES,
)
from curie_telemetry.redact import redact_text as shared_redact_text
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# Every literal below is a hoisted, obviously-fake constant. The repo's
# check-secrets pre-commit hook false-positives on inline token literals, so the
# vectors are assembled from named constants and split prefixes rather than
# written inline at the call site.
FAKE_API_KEY = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0000"
FAKE_AWS_ACCESS_KEY_ID = "AKIA" + "EXAMPLEFAKEKEY0000"
FAKE_GITHUB_PAT = "ghp_" + "0000FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE"
FAKE_GITLAB_TOKEN = "glpat-" + "0000FAKEFAKEFAKEFAKE"
FAKE_SLACK_TOKEN = "xoxb-" + "0000000000-0000000000-FAKEFAKEFAKEFAKEFAKEFAKE"
FAKE_GOOGLE_API_KEY = "AIza" + "SyFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0"
FAKE_PEM_PRIVATE_KEY = (
    "-----BEGIN " + "RSA PRIVATE KEY-----\n"
    "MIIFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0000\n"
    "-----END " + "RSA PRIVATE KEY-----"
)
FAKE_BEARER_HEADER = "Bearer " + "abc0000FAKEFAKEFAKEFAKEFAKEFAKE"
FAKE_BASIC_HEADER = "Basic " + "QUNNRUZBS0VGQUtFUEFTUw=="
FAKE_DSN_USERINFO = "postgresql://acme-user:fake-password@db.example.invalid/acme"
FAKE_JWT = "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiJmYWtlIn0.FAKEFAKEFAKEFAKEFAKEFAKE00"
FAKE_URL_WITH_TOKEN = "https://example.invalid/hook?token=" + "0000FAKEFAKEFAKEFAKE"
FAKE_SECRET_ASSIGNMENT = "secret=" + "0000FAKEFAKEFAKEVALUE"
FAKE_JSON_FIELD_SECRET = "0000FAKEJSONFIELDSECRET"
FAKE_DICT_FIELD_SECRET = "0000FAKEDICTFIELDSECRET"
FAKE_COLON_FIELD_SECRET = "0000FAKECOLONFIELDSECRET"
FAKE_HOME_PATH = "/home/theconnman/.config/curie/settings.json"
FAKE_CHANNEL_TOKEN = "chn." + "ZXhhbXBsZWNoYW5uZWxwYXlsb2Fk." + "FAKEFAKEFAKESIG0000"
# The local token minters use base64url payload and signature segments.
# See curie_internal.sandbox_token.mint and curie_worker.caller_token.mint.
FAKE_SANDBOX_TOKEN = "sbx." + "ZXhhbXBsZXNhbmRib3hwYXlsb2Fk." + "FAKEFAKEFAKESIG0000"
FAKE_CONNECTOR_CALLER_TOKEN = "cct." + "ZXhhbXBsZWNhbGxlcnBheWxvYWQ." + "FAKEFAKEFAKESIG0000"
FAKE_X_API_KEY_HEADER = "X-API-Key: " + "FAKEFAKEFAKEHEADERVALUE0000"
_FAKE_DISCORD_BOT_TOKEN = (
    "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00." + "FAKEFAKEFAKEFAKEFAKEFAKE000"
)
FAKE_DISCORD_BOT_AUTHORIZATION = "Authorization: Bot " + _FAKE_DISCORD_BOT_TOKEN
FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT = "DISCORD_BOT_TOKEN=" + _FAKE_DISCORD_BOT_TOKEN
# Shape-valid bot token (id segment starts with M) carried with no context.
FAKE_SHAPED_DISCORD_BOT_TOKEN = (
    "M" + "FAKEFAKEFAKEFAKEFAKE000." + "FAKE00." + "FAKEFAKEFAKEFAKEFAKEFAKE000"
)
# The webhook URL keeps its scheme, host, path and id; only the token is secret.
FAKE_DISCORD_WEBHOOK_PREFIX = "https://discord.com/api/webhooks/" + "100000000000000000/"
FAKE_DISCORD_WEBHOOK_TOKEN = (
    "FAKE" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE-" + "FAKEFAKEFAKEFAKEFAKEFAKE_FAKE000"
)

# The sensitive substring that must be absent from every boundary's output,
# keyed by rule name. VECTORS is derived from this so the two cannot drift.
SECRET_LITERALS: dict[str, str] = {
    "api_key": FAKE_API_KEY,
    "aws_access_key_id": FAKE_AWS_ACCESS_KEY_ID,
    "github_pat": FAKE_GITHUB_PAT,
    "gitlab_token": FAKE_GITLAB_TOKEN,
    "slack_token": FAKE_SLACK_TOKEN,
    "google_api_key": FAKE_GOOGLE_API_KEY,
    "pem_private_key": FAKE_PEM_PRIVATE_KEY,
    "bearer_token": FAKE_BEARER_HEADER,
    "basic_auth": FAKE_BASIC_HEADER,
    "dsn_userinfo": FAKE_DSN_USERINFO,
    "jwt": FAKE_JWT,
    "url_secret_param": FAKE_URL_WITH_TOKEN,
    "secret_assignment": FAKE_SECRET_ASSIGNMENT,
    "secret_json_field": FAKE_JSON_FIELD_SECRET,
    "secret_dict_field": FAKE_DICT_FIELD_SECRET,
    "secret_colon_field": FAKE_COLON_FIELD_SECRET,
    "home_path": FAKE_HOME_PATH,
    "channel_token": FAKE_CHANNEL_TOKEN,
    "sandbox_token": FAKE_SANDBOX_TOKEN,
    "connector_caller_token": FAKE_CONNECTOR_CALLER_TOKEN,
    "x_api_key": FAKE_X_API_KEY_HEADER,
    "discord_bot_authorization": FAKE_DISCORD_BOT_AUTHORIZATION,
    "discord_bot_token_assignment": FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT,
    "discord_bot_token": FAKE_SHAPED_DISCORD_BOT_TOKEN,
    "discord_webhook_url": FAKE_DISCORD_WEBHOOK_TOKEN,
}

# Non-secret text that must precede a literal for its rule to apply, for rules
# whose redacted secret is only part of the matched value.
_LITERAL_CARRIERS: dict[str, str] = {
    "discord_webhook_url": FAKE_DISCORD_WEBHOOK_PREFIX,
    "secret_json_field": '{"AWS_SECRET_ACCESS_KEY": "',
    "secret_dict_field": "{'MY_PRIVATE_KEY': '",
    "secret_colon_field": "private_key: ",
}

_LITERAL_SUFFIXES: dict[str, str] = {
    "secret_json_field": '", "status": "ok"}',
    "secret_dict_field": "', 'status': 'ok'}",
    "secret_colon_field": " status=ok",
}

_SHARED_PLACEHOLDERS: dict[str, str] = {
    "secret_json_field": "secret_assignment",
    "secret_dict_field": "secret_assignment",
    "secret_colon_field": "secret_assignment",
}

_PRESERVED_CONTEXT: dict[str, str] = {
    "secret_json_field": '"status": "ok"',
    "secret_dict_field": "'status': 'ok'",
    "secret_colon_field": "status=ok",
}

# One frozen vector per rule: a realistic runner output line carrying that class
# of secret. The tripwire below binds this table to REDACTION_RULES.
VECTORS: tuple[tuple[str, str], ...] = tuple(
    (
        name,
        f"runner output carrying {_LITERAL_CARRIERS.get(name, '')}{literal}"
        f"{_LITERAL_SUFFIXES.get(name, '')} in context",
    )
    for name, literal in SECRET_LITERALS.items()
)

BOUNDARIES: tuple[str, ...] = ("stdout", "gen_ai_span")

CASES: tuple[tuple[str, str, str], ...] = tuple(
    (name, vector, boundary) for name, vector in VECTORS for boundary in BOUNDARIES
)


def _placeholder(name: str) -> str:
    return f"[REDACTED:{_SHARED_PLACEHOLDERS.get(name, name)}]"


def _log_through_stdout(*args: object) -> str:
    """Log through a real root handler with the stdout redaction pass installed."""

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    root.addHandler(handler)
    # install_stdout_redaction() filters EVERY root handler, including pytest's
    # own capture handlers. Restore their filters, or any later test in this
    # process that asserts on a raw log message sees it redacted (surfaced
    # under xdist, where file order differs from the serial run).
    saved_filters = {item: list(item.filters) for item in root.handlers}
    try:
        install_stdout_redaction()
        logger = logging.getLogger("curie_runner.test_redact")
        logger.setLevel(logging.INFO)
        logger.info(*args)
    finally:
        root.removeHandler(handler)
        for item, filters in saved_filters.items():
            item.filters[:] = filters
    return stream.getvalue()


def _span_attributes(vector: str) -> dict[str, dict[str, object]]:
    """Drive a real turn whose trace name, model, and tool name carry the vector."""

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    usage = {"input_tokens": 20, "output_tokens": 8}

    def script() -> list[object]:
        return [
            AssistantMessage(content=[TextBlock(text="working")], model=vector),
            AssistantMessage(
                content=[ToolUseBlock(id="t1", name=vector, input={"command": "echo hi"})],
                model=vector,
            ),
            UserMessage(
                content=[ToolResultBlock(tool_use_id="t1", content="command completed")]
            ),
            AssistantMessage(content=[TextBlock(text="done")], model=vector, usage=usage),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="fake-session",
                result="done",
                usage=usage,
            ),
        ]

    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: FakeModelSession(script_factory=script),
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name=vector,
        model=vector,
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)
    return {span.name: dict(span.attributes or {}) for span in exporter.get_finished_spans()}


@pytest.mark.parametrize(("name", "vector", "boundary"), CASES)
def test_every_rule_is_redacted_at_every_boundary(name: str, vector: str, boundary: str) -> None:
    secret = SECRET_LITERALS[name]
    placeholder = _placeholder(name)

    if boundary == "stdout":
        out = _log_through_stdout(vector)
        assert secret not in out
        assert placeholder in out
        if name in _PRESERVED_CONTEXT:
            assert _PRESERVED_CONTEXT[name] in out
        return

    spans = _span_attributes(vector)
    root = spans["agent.run"]
    generation = spans["llm.generation"]
    tool = spans["execute_tool"]

    for value in (
        root["langfuse.trace.name"],
        generation["gen_ai.request.model"],
        generation["model"],
        tool["gen_ai.tool.name"],
    ):
        assert isinstance(value, str)
        assert secret not in value
        assert placeholder in value
        if name in _PRESERVED_CONTEXT:
            assert _PRESERVED_CONTEXT[name] in value


@pytest.mark.parametrize(("name", "vector"), VECTORS)
def test_every_rule_is_redacted_through_the_logging_args_path(name: str, vector: str) -> None:
    # The dangerous shape is `logger.info("token=%s", secret)`: the secret arrives
    # via record.args and never appears in record.msg, so a filter that only scans
    # msg leaks it. Redaction must run over the fully formatted message.
    out = _log_through_stdout("runner emitted t=%s", vector)
    assert SECRET_LITERALS[name] not in out
    assert _placeholder(name) in out
    if name in _PRESERVED_CONTEXT:
        assert _PRESERVED_CONTEXT[name] in out


def test_non_string_span_attributes_survive_redaction() -> None:
    spans = _span_attributes(VECTORS[0][1])
    generation = spans["llm.generation"]

    assert generation["gen_ai.usage.input_tokens"] == 20
    assert generation["gen_ai.usage.output_tokens"] == 8
    assert isinstance(generation["gen_ai.usage.input_tokens"], int)
    assert isinstance(generation["gen_ai.usage.output_tokens"], int)


def test_redact_span_attribute_preserves_non_string_types() -> None:
    for value in (0, 42, True, False, 1.5):
        result = redact_span_attribute(value)
        assert result == value
        assert type(result) is type(value)


def test_every_rule_has_a_frozen_vector() -> None:
    # TRIPWIRE. Adding a redaction regex requires adding a frozen vector here and
    # confirming every boundary pass in REDACTION_BOUNDARIES actually redacts it.
    # Without this gate a new rule can ship applied at one boundary and absent at
    # the other, which is a leak that no other test would catch.
    assert {rule.name for rule in REDACTION_RULES} == {name for name, _ in VECTORS}


def test_runner_reuses_the_shared_redaction_policy_without_drift() -> None:
    runner_rules = [
        (rule.name, rule.pattern.pattern, rule.pattern.flags, rule.placeholder)
        for rule in REDACTION_RULES
    ]
    shared_rules = [
        (rule.name, rule.pattern.pattern, rule.pattern.flags, rule.placeholder)
        for rule in SHARED_REDACTION_RULES
    ]
    assert runner_rules == shared_rules
    for _, vector in VECTORS:
        assert redact_text(vector) == shared_redact_text(vector)


def test_every_boundary_is_exercised() -> None:
    # TRIPWIRE. Adding a new runner output boundary requires extending this test
    # module's parametrization to drive the real code at that boundary.
    assert {boundary for _, _, boundary in CASES} == set(REDACTION_BOUNDARIES)


def test_ordinary_log_lines_are_untouched() -> None:
    line = "runner configured session=s-1 model=claude-opus-4-8 port=8080"
    assert redact_text(line) == line


def test_normal_prose_is_untouched() -> None:
    line = "The turn finished after two tool calls and the budget ceiling was not reached."
    assert redact_text(line) == line


def test_redact_span_attribute_scrubs_inside_sequences() -> None:
    # #935: sequences passed through the scrub untouched. OTel permits them, and a
    # future sequence-valued attribute must not depend on someone remembering to
    # extend this function -- so scrub elementwise, preserving the container type
    # (OTel requires a homogeneous sequence, and str elements stay str).
    scrubbed = redact_span_attribute(["sk-abcdefghijklmnopqrstuvwx", "clean"])
    assert isinstance(scrubbed, list)
    assert scrubbed[0] != "sk-abcdefghijklmnopqrstuvwx"
    assert scrubbed[1] == "clean"

    as_tuple = redact_span_attribute(("sk-abcdefghijklmnopqrstuvwx",))
    assert isinstance(as_tuple, tuple)
    assert as_tuple[0] != "sk-abcdefghijklmnopqrstuvwx"


def test_redact_span_attribute_leaves_non_string_scalars_and_types_alone() -> None:
    # Negative control: the token counts must stay ints, and a numeric sequence
    # must not be stringified by the new recursion.
    assert redact_span_attribute(12) == 12
    assert redact_span_attribute(True) is True
    assert redact_span_attribute([1, 2]) == [1, 2]


_MESSAGE_FRAME = {"kind": "event", "type": "message", "text": "go", "user": "U", "ts": "1"}
_SPLIT_SECRET = "q7X!p9"
_OVERLAP_FIRST = "FAKEPREFIXOVERLAP"
_OVERLAP_SECOND = "OVERLAPSECRETSUFFIX0000"
_OVERLAP_COMBINED = "FAKEPREFIXOVERLAPSECRETSUFFIX0000"


def _result(text: str, *, failed: bool = False) -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution" if failed else "success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=failed,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage={"input_tokens": 20, "output_tokens": 8},
    )


def _reply_script(text: str, result: dict[str, object] | None = None) -> list[object]:
    messages: list[object] = [AssistantMessage(content=[TextBlock(text=text)], model="fake-model")]
    if result is not None:
        messages.extend(
            [
                AssistantMessage(
                    content=[ToolUseBlock(id="call", name="Bash", input={"command": "echo hi"})],
                    model="fake-model",
                ),
                UserMessage(
                    content=[
                        ToolResultBlock(
                            tool_use_id="call",
                            content=[{"type": "text", "text": json.dumps(result)}],
                            is_error=False,
                        )
                    ]
                ),
            ]
        )
    messages.append(_result(text))
    return messages


def _boot_reply_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    script: list[object],
    *,
    config_env: dict[str, str] | None = None,
    sdk_env: dict[str, str] | None = None,
    connectors: str | None = None,
    mcp: dict[str, object] | None = None,
    manifest: dict[str, object] | None = None,
    provider: FakeModelSession | None = None,
) -> SessionRunner:
    """Keep boot, translation, serialization and HTTP real; replace only the provider."""

    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True, exist_ok=True)
    (plugin / "plugin.json").write_text(json.dumps(manifest or {"name": "acme-bot"}))
    if connectors is not None:
        (tmp_path / "connectors.yaml").write_text(connectors)
    if mcp is not None:
        (tmp_path / ".mcp.json").write_text(json.dumps(mcp))
    for name in (
        "CURIE_STATE_URL",
        "CURIE_PROGRESS_URL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "session-acme-redaction",
            "CURIE_SANDBOX_ID": "sandbox-acme-redaction",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
            **(config_env or {}),
        }
    )

    def session(options: Any) -> FakeModelSession:
        return provider or FakeModelSession(
            script_factory=lambda: script,
            can_use_tool=options.can_use_tool,
        )

    monkeypatch.setattr(boot, "ClaudeAgentSession", session)
    return boot.build_runner(
        config,
        sdk_env=sdk_env,
        mcp_capability=McpToolCapabilityProbe(
            complete=True,
            has_potential_write_tool=True,
            tool_count=1,
        ),
    )


def _http_reply(runner: SessionRunner) -> tuple[str, list[dict[str, Any]]]:
    async def go() -> tuple[str, list[dict[str, Any]]]:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post("/v1/event", json=_MESSAGE_FRAME)
            assert response.status == 200
            raw = await response.text()
            assert parse_ndjson(raw), "the reply must remain valid ACI NDJSON"
            return raw, [json.loads(line) for line in raw.splitlines()]

    return anyio.run(go)


def _assistant_text(frames: list[dict[str, Any]]) -> str:
    return "".join(frame["text"] for frame in frames if frame["type"] == "text_delta")


def test_boot_http_scrubs_held_secrets_from_replies_and_tool_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fix pin crosses credential collection and the common reply boundary."""

    config_env = {
        "CURIE_CREDENTIALS": "FAKECONFIGCREDENTIALVALUE0000",
        "CURIE_RUNNER_TOKEN": "FAKECONFIGRUNNERVALUE0000",
        "CURIE_CONNECTOR_CALLER_TOKEN": "FAKECONFIGCALLERVALUE0000",
    }
    process_env = {
        "CURIE_MODEL_ENV_KEY": '["CUSTOM_MODEL_INPUT", "SECOND_MODEL_INPUT"]',
        "CUSTOM_MODEL_INPUT": "FAKEPROCESSMODELVALUE0000",
        "SECOND_MODEL_INPUT": "FAKESECONDMODELVALUE0000",
        "ANTHROPIC_API_KEY": "FAKEPROCESSPROVIDERVALUE0000",
        "CURIE_CONNECTOR_SECRET_KEYS": "CUSTOM_CONNECTOR_INPUT",
        "CUSTOM_CONNECTOR_INPUT": "FAKEPROCESSCONNECTORVALUE0000",
        "CURIE_STATE_TOKEN": "FAKESTATETOKENVALUE0000",
        "CURIE_PROGRESS_TOKEN": "FAKEPROGRESSTOKENVALUE0000",
        "DECLARED_INPUT": "FAKEDECLAREDCONNECTORVALUE0000",
        "HEADER_INPUT": "FAKEPROCESSHEADERVALUE0000",
    }
    sdk_env = {
        "CURIE_MODEL_ENV_KEY": '["CUSTOM_MODEL_INPUT", "SDK_MODEL_INPUT"]',
        "CUSTOM_MODEL_INPUT": "FAKESDKMODELVALUE0000",
        "SDK_MODEL_INPUT": "FAKESDKSECONDMODELVALUE0000",
        "CLAUDE_CODE_OAUTH_TOKEN": "FAKESDKOAUTHVALUE0000",
        "ANTHROPIC_AUTH_TOKEN": "FAKESDKAUTHVALUE0000",
        "CURIE_CONNECTOR_SECRET_KEYS": "SDK_CONNECTOR_INPUT",
        "SDK_CONNECTOR_INPUT": "FAKESDKCONNECTORVALUE0000",
        "HEADER_INPUT": "FAKESDKHEADERVALUE0000",
    }
    literal_header = "FAKELITERALHEADERVALUE0000"
    literal_mcp_env = "FAKELITERALMCPENVVALUE0000"
    for name, value in process_env.items():
        monkeypatch.setenv(name, value)
    secrets = [
        *config_env.values(),
        *(value for name, value in process_env.items() if name not in (
            "CURIE_MODEL_ENV_KEY", "CURIE_CONNECTOR_SECRET_KEYS"
        )),
        *(value for name, value in sdk_env.items() if name not in (
            "CURIE_MODEL_ENV_KEY", "CURIE_CONNECTOR_SECRET_KEYS"
        )),
        literal_header,
        literal_mcp_env,
    ]
    text = "Visible before " + " / ".join(secrets) + " visible after."
    runner = _boot_reply_runner(
        tmp_path,
        monkeypatch,
        _reply_script(text, {"nested": [{"observed": text}], "count": 3, "ok": True}),
        config_env=config_env,
        sdk_env=sdk_env,
        connectors=(
            "connectors:\n  remote:\n    url: https://mcp.example.com/mcp\n"
            "    secrets: [DECLARED_INPUT]\n"
            "    headers:\n      X-API-Key: ${HEADER_INPUT}\n"
        ),
        mcp={
            "mcpServers": {
                "external": {
                    "type": "http",
                    "url": "https://mcp.example.com/mcp",
                    "headers": {"Authorization": f"Bearer {literal_header}"},
                },
                "stdio": {
                    "command": "python3",
                    "args": ["-V"],
                    "env": {"ACCESS_TOKEN": literal_mcp_env},
                },
            }
        },
    )
    raw, frames = _http_reply(runner)
    for secret in secrets:
        assert secret not in raw
        assert secret not in _assistant_text(frames)
    assert "Visible before " in _assistant_text(frames)
    assert " visible after." in _assistant_text(frames)
    assert "[REDACTED:" in _assistant_text(frames)
    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert closing["result"]["count"] == 3
    assert closing["result"]["ok"] is True
    assert closing["call_id"] == "call"
    assert closing["failed"] is False
    assert frames[-1]["type"] == "final"
    assert frames[-1]["status"] == "done"
    assert frames[-1]["input_tokens"] == 20
    assert frames[-1]["output_tokens"] == 8


@pytest.mark.parametrize("source", ["process", "sdk"])
def test_boot_http_keeps_hosted_secret_protected_after_env_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    secret = "FAKEHOSTEDINPUTVALUE0000"
    marker = "CURIE_CONNECTOR_SECRET_KEYS"
    sdk_env = {"GITHUB_PERSONAL_ACCESS_TOKEN": secret, marker: "GITHUB_PERSONAL_ACCESS_TOKEN"}
    monkeypatch.setenv("GITHUB_PERSONAL_ACCESS_TOKEN", secret)
    monkeypatch.setenv(marker, "GITHUB_PERSONAL_ACCESS_TOKEN")
    runner = _boot_reply_runner(
        tmp_path,
        monkeypatch,
        _reply_script(f"Connector returned {secret}."),
        sdk_env=sdk_env if source == "sdk" else None,
        config_env={
            "CURIE_CONNECTOR_RELEASE": "curie",
            "CURIE_CONNECTOR_AGENT": "acme-dev",
            "CURIE_CONNECTOR_NAMESPACE": "curie",
        },
        connectors=(
            "connectors:\n  github:\n    image: ghcr.io/github/github-mcp-server:v0.32.0\n"
            "    secrets: [GITHUB_PERSONAL_ACCESS_TOKEN]\n"
        ),
    )
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in os.environ
    if source == "sdk":
        assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in sdk_env
    raw, frames = _http_reply(runner)
    assert secret not in raw
    assert "[REDACTED:" in _assistant_text(frames)
    assert frames[-1]["status"] == "done"


@pytest.mark.parametrize("offset", range(1, len(_SPLIT_SECRET)))
@pytest.mark.parametrize("with_tool_note", [False, True])
def test_http_redacts_held_secret_split_at_every_offset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, offset: int, with_tool_note: bool
) -> None:
    messages: list[object] = [
        AssistantMessage(content=[TextBlock(text="Before " + _SPLIT_SECRET[:offset])], model="fake")
    ]
    if with_tool_note:
        messages.append(
            AssistantMessage(content=[ToolUseBlock(id="read", name="Read", input={})], model="fake")
        )
    messages.extend(
        [
            AssistantMessage(
                content=[TextBlock(text=_SPLIT_SECRET[offset:] + " after.")], model="fake"
            ),
            _result(""),
        ]
    )
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path, monkeypatch, messages, config_env={"CURIE_RUNNER_TOKEN": _SPLIT_SECRET}
        )
    )
    assert _SPLIT_SECRET not in raw
    assert _SPLIT_SECRET not in _assistant_text(frames)
    assert _assistant_text(frames).startswith("Before [REDACTED:")
    assert _assistant_text(frames).endswith(" after.")
    assert frames[-1]["text"] == _assistant_text(frames)
    if with_tool_note:
        assert any(frame["type"] == "tool_note" for frame in frames)


def test_http_reply_keeps_a_paragraph_break_between_text_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#3694: two progress sentences around a tool call must not be glued.

    Covers the stream and the empty-result final, which falls back to the
    streamed text (#107).
    """

    messages: list[object] = [
        AssistantMessage(content=[TextBlock(text="Staging the files.")], model="fake"),
        AssistantMessage(content=[ToolUseBlock(id="read", name="Read", input={})], model="fake"),
        AssistantMessage(content=[TextBlock(text="All three files staged.")], model="fake"),
        _result(""),
    ]
    _, frames = _http_reply(_boot_reply_runner(tmp_path, monkeypatch, messages))

    assert _assistant_text(frames) == "Staging the files.\n\nAll three files staged."
    assert frames[-1]["text"] == _assistant_text(frames)
    assert frames[-1]["status"] == "done"


def test_http_reply_does_not_pad_text_blocks_the_model_already_separated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    messages: list[object] = [
        AssistantMessage(content=[TextBlock(text="One.\n")], model="fake"),
        AssistantMessage(content=[TextBlock(text="Two.")], model="fake"),
        AssistantMessage(content=[TextBlock(text=" Three.")], model="fake"),
        _result(""),
    ]
    _, frames = _http_reply(_boot_reply_runner(tmp_path, monkeypatch, messages))

    assert _assistant_text(frames) == "One.\nTwo. Three."
    assert frames[-1]["text"] == _assistant_text(frames)


def test_http_reply_keeps_the_authoritative_result_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    messages: list[object] = [
        AssistantMessage(content=[TextBlock(text="Looking.")], model="fake"),
        AssistantMessage(content=[TextBlock(text="Done.")], model="fake"),
        _result("Done."),
    ]
    _, frames = _http_reply(_boot_reply_runner(tmp_path, monkeypatch, messages))

    assert _assistant_text(frames) == "Looking.\n\nDone."
    assert frames[-1]["text"] == "Done."


def _redacted_turn(
    held: frozenset[str], blocks: list[str], final_text: str
) -> tuple[str, str]:
    """The streamed text and the final text one turn's redactor emits."""

    redactor = OutboundRedactor(held)
    lines: list[str] = []
    for block in blocks:
        lines.extend(redactor.push(json.dumps({"type": "text_delta", "text": block})))
    lines.extend(
        redactor.push(json.dumps({"type": "final", "status": "done", "text": final_text}))
    )
    frames = [json.loads(line) for line in lines]
    return _assistant_text(frames), frames[-1]["text"]


def test_block_break_final_scrubs_a_pattern_cut_by_a_held_prefix() -> None:
    """#3694 review: the final is scrubbed whole, not as its streamed chunks.

    The reply ends in characters that open a held value, so the stream holds
    that tail back as its own chunk and the key pattern spans the cut.
    """

    block = "Your key: sk-proj-1234567890abcd"
    _, final = _redacted_turn(frozenset({"abcdHELDVALUE0000"}), [block], block)

    assert "sk-proj" not in final
    assert "[REDACTED:" in final


def test_block_break_never_splits_a_pattern_secret() -> None:
    """#3694 review: a break inside a pattern match would let both halves out."""

    blocks = ["The key is sk-", "proj0123456789abcdefXYZ"]
    stream, final = _redacted_turn(
        frozenset({"sk-ant-oat01-HELDVALUE0000"}), blocks, "".join(blocks)
    )

    assert "proj0123456789abcdefXYZ" not in stream
    assert "proj0123456789abcdefXYZ" not in final


def test_block_break_reaches_a_final_led_by_a_connector_notice() -> None:
    """The DONE final may carry the connector notice ahead of the streamed text."""

    stream, final = _redacted_turn(
        frozenset(), ["First.", "Second."], "Connector notice.\n\nFirst.Second."
    )

    assert stream == "First.\n\nSecond."
    assert final == "Connector notice.\n\nFirst.\n\nSecond."


@pytest.mark.parametrize(
    ("held", "blocks"),
    [
        pytest.param(
            frozenset({"conn_HELD"}),
            ["conn_HELDpostgres://app:hunter2", "pass@db/prod"],
            id="match_appears_after_the_held_value_is_replaced",
        ),
        pytest.param(
            frozenset(),
            ['eyJhbGc.eyJzdWI.sigvalue"password', '": "hunter2pass"'],
            id="match_appears_after_an_earlier_rule_runs",
        ),
    ],
)
def test_block_break_never_splits_a_match_made_while_scrubbing(
    held: frozenset[str], blocks: list[str]
) -> None:
    """#3694 review: a rule can match only after an earlier replacement, so the
    raw text alone cannot say where a break is safe."""

    _, final = _redacted_turn(held, blocks, "".join(blocks))

    assert "hunter2" not in final


def test_block_breaks_add_nothing_but_breaks_to_the_final() -> None:
    """With its breaks removed, the final is exactly what one unbroken block scrubs to."""

    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789"
    rng = random.Random(3694)

    def body(size: int) -> str:
        return "".join(rng.choice(alphabet) for _ in range(size))

    fillers = ["Done.", "Here", " is ", "the", "value", ":", " ", "x", "ok", "-", ".", "_"]
    for _ in range(400):
        secrets = [
            "sk-ant-api03-" + body(24),
            "ghp_" + body(30),
            "eyJ" + body(10) + "." + body(10) + "." + body(12),
            "Bearer " + body(20),
            "token=" + body(14),
            "Authorization: Basic " + body(16),
            "postgres://u:" + body(10) + "@h/db",
            '"password": "' + body(10) + '"',
        ]
        held = frozenset(
            {rng.choice(secrets) for _ in range(rng.randint(0, 2))}
            | {rng.choice(secrets)[: rng.randint(1, 6)] + body(8) for _ in range(rng.randint(0, 2))}
        )
        text = "".join(
            rng.choice(fillers) + rng.choice(secrets) + rng.choice(fillers)
            for _ in range(rng.randint(1, 3))
        )
        cuts = sorted(rng.sample(range(1, len(text)), k=rng.randint(1, 4)))
        blocks = [text[a:b] for a, b in zip([0, *cuts], [*cuts, len(text)], strict=True)]
        lead = rng.choice(["", "Connector notice.\n\n"])

        _, broken = _redacted_turn(held, blocks, lead + text)
        _, unbroken = _redacted_turn(held, [lead + text], lead + text)

        assert broken.replace("\n\n", "") == unbroken.replace("\n\n", ""), (held, blocks)


def test_block_breaks_scrub_a_long_turn_in_linear_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Placing breaks must not rescan the rest of the turn once per block.

    Counted in characters handed to the rule pass, not seconds, so the bound
    holds on any machine.
    """

    scanned: list[int] = []
    real = redact_module.redact_text

    def counting(text: str) -> str:
        scanned.append(len(text))
        return real(text)

    monkeypatch.setattr(redact_module, "redact_text", counting)
    blocks = [f"Progress sentence number {index} with some words." for index in range(300)]
    total = sum(len(block) for block in blocks)

    held = frozenset({"sk-ant-oat01-HELDVALUE0000"})
    stream, final = _redacted_turn(held, blocks, "".join(blocks))

    assert final == stream == "\n\n".join(blocks)
    assert sum(scanned) <= 6 * total, sum(scanned)


def test_block_breaks_stay_linear_inside_a_long_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """#3694 review: a match that appears only after a held value is replaced
    can cover many joins. Rejected breaks must not rescan the run each time,
    and no break may trail the placeholder that swallowed the rest."""

    scanned: list[int] = []
    real = redact_module.redact_text

    def counting(text: str) -> str:
        scanned.append(len(text))
        return real(text)

    monkeypatch.setattr(redact_module, "redact_text", counting)
    blocks = ["see ?token=Ax y", *(f"blk{index}" for index in range(500))]
    total = sum(len(block) for block in blocks)

    _, final = _redacted_turn(frozenset({"x y"}), blocks, "".join(blocks))

    assert final == "see [REDACTED:url_secret_param]"
    assert sum(scanned) <= 12 * total, sum(scanned)


@pytest.mark.parametrize(("name", "vector"), VECTORS)
def test_http_reply_applies_every_shared_redaction_rule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, vector: str
) -> None:
    raw, frames = _http_reply(
        _boot_reply_runner(tmp_path, monkeypatch, _reply_script(vector, {"observed": vector}))
    )
    assert SECRET_LITERALS[name] not in raw
    assert _placeholder(name) in _assistant_text(frames)
    assert _placeholder(name) in frames[-1]["text"]
    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert _placeholder(name) in closing["result"]["observed"]
    assert closing["redacted"] is True


def test_http_marks_a_side_effect_redacted_when_its_prior_state_held_a_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scrubbed snapshot is not a restore, so the frame has to say it was scrubbed.

    The worker builds the ledger's prior state from this result. Without the
    marker, a record whose prior state reads ``[REDACTED:held_secret]`` looks
    exactly as undoable as a clean one (#1873).
    """

    secret = "FAKESNAPSHOTTOKENVALUE0000"
    monkeypatch.setenv("ACME_API_TOKEN", secret)
    snapshot = {
        "prior": {"env": [{"name": "API_TOKEN", "value": secret}]},
        "post": {"env": [{"name": "API_TOKEN", "value": "rotated"}]},
        "target": {"kind": "Deployment", "name": "acme-api"},
    }
    raw, frames = _http_reply(
        _boot_reply_runner(tmp_path, monkeypatch, _reply_script("Rotated.", snapshot))
    )

    assert secret not in raw
    flags = [frame for frame in frames if frame["type"] == "side_effect_flag"]
    closing = next(frame for frame in flags if frame.get("result") is not None)
    assert closing["result"]["prior"]["env"][0]["value"] == "[REDACTED:held_secret]"
    assert closing["redacted"] is True
    # The opening frame carries no result, so there is nothing to have scrubbed.
    assert all(frame.get("redacted") is None for frame in flags if frame.get("result") is None)


def test_http_leaves_a_clean_side_effect_result_unmarked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a replacement marks the frame; a clean snapshot stays restorable."""

    snapshot = {
        "prior": {"spec": {"replicas": 3}},
        "post": {"spec": {"replicas": 10}},
        "target": {"kind": "Deployment", "name": "acme-api"},
    }
    _, frames = _http_reply(
        _boot_reply_runner(tmp_path, monkeypatch, _reply_script("Scaled.", snapshot))
    )

    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert closing["result"] == snapshot
    assert closing.get("redacted") is None


def test_http_redacts_repeated_overlapping_and_escaped_values_without_losing_result_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    escaped = 'FAKE"quoted\\path\n雪VALUE0000'
    short = "FAKEOVERLAP"
    long = short + "REMAINDER0000"
    first_key = "FAKEFIRSTKEYVALUE0000"
    second_key = "FAKESECONDKEYVALUE0000"
    values = [escaped, short, long, first_key, second_key]
    names = {f"CUSTOM_INPUT_{index}": value for index, value in enumerate(values)}
    text = f"Before {long} and {short} and {escaped} and {long} after."
    result = {
        first_key: {"entries": [escaped, {"observed": long}, None, False, 3, 1.5]},
        second_key: "second value",
        "plain": "unknownOpaqueValue0000",
    }
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(text, result),
            sdk_env={"CURIE_MODEL_ENV_KEY": json.dumps(list(names)), **names},
        )
    )
    decoded_reply = _assistant_text(frames)
    closing = next(frame for frame in frames if frame.get("result") is not None)["result"]
    for value in values:
        assert value not in decoded_reply
        assert json.dumps(value)[1:-1] not in raw
        assert value not in json.dumps(closing, ensure_ascii=False)
    assert "REMAINDER0000" not in decoded_reply
    assert decoded_reply.startswith("Before [REDACTED:")
    assert decoded_reply.endswith(" after.")
    assert len(closing) == 3, "colliding redacted keys must retain both original values"
    assert "second value" in closing.values()
    entries = next(value["entries"] for value in closing.values() if isinstance(value, dict))
    assert entries[2:] == [None, False, 3, 1.5]
    assert closing["plain"] == "unknownOpaqueValue0000"


def test_http_hides_the_union_of_partially_overlapping_held_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    text = f"Before {_OVERLAP_COMBINED} after."
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(text, {"nested": [{"observed": text}]}),
            config_env={"CURIE_RUNNER_TOKEN": _OVERLAP_FIRST},
            sdk_env={"CURIE_MODEL_ENV_KEY": "CUSTOM_INPUT", "CUSTOM_INPUT": _OVERLAP_SECOND},
        )
    )
    closing = next(frame for frame in frames if frame.get("result") is not None)
    for protected in (
        _assistant_text(frames),
        frames[-1]["text"],
        closing["result"]["nested"][0]["observed"],
    ):
        assert "[REDACTED:held_secret]" in protected
        assert protected.replace("[REDACTED:held_secret]", "") == "Before  after."
    assert "FAKEPREFIX" not in raw
    assert "SECRETSUFFIX0000" not in raw
    assert frames[-1]["status"] == "done"


@pytest.mark.parametrize(
    "chunks",
    [
        pytest.param(
            ("FAKEPREFIXOVER", "LAPSECRETSUFFIX0000"),
            id="split_inside_shared_span",
        ),
        pytest.param(
            (_OVERLAP_FIRST, "SECRETSUFFIX0000"),
            id="complete_first_match_overlaps_possible_second_prefix",
        ),
        pytest.param(
            (_OVERLAP_FIRST + "SECRET", "SUFFIX0000"),
            id="complete_first_match_overlaps_longer_pending_prefix",
        ),
        pytest.param(
            (_OVERLAP_FIRST, "SECRET", "SUFFIX0000"),
            id="overlapping_prefix_remains_pending_across_multiple_deltas",
        ),
    ],
)
def test_http_hides_overlapping_secret_spans_across_buffered_text_deltas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, chunks: tuple[str, ...]
) -> None:
    messages: list[object] = [
        AssistantMessage(content=[TextBlock(text="Before " + chunks[0])], model="fake"),
        AssistantMessage(content=[ToolUseBlock(id="read", name="Read", input={})], model="fake"),
        *(
            AssistantMessage(content=[TextBlock(text=chunk)], model="fake")
            for chunk in chunks[1:]
        ),
        AssistantMessage(content=[TextBlock(text=" after.")], model="fake"),
        _result(""),
    ]
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            messages,
            config_env={"CURIE_RUNNER_TOKEN": _OVERLAP_FIRST},
            sdk_env={"CURIE_MODEL_ENV_KEY": "CUSTOM_INPUT", "CUSTOM_INPUT": _OVERLAP_SECOND},
        )
    )
    text = _assistant_text(frames)
    assert "[REDACTED:held_secret]" in text
    assert text.replace("[REDACTED:held_secret]", "") == "Before  after."
    assert "FAKEPREFIX" not in raw
    assert "SECRETSUFFIX0000" not in raw
    assert any(frame["type"] == "tool_note" for frame in frames)
    assert frames[-1]["text"] == text
    assert frames[-1]["status"] == "done"


def test_http_preserves_protocol_metadata_when_short_secrets_match_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = {
        "SHORT_ONE": "done",
        "SHORT_TWO": "call",
        "SHORT_THREE": PROTOCOL_VERSION,
        "SHORT_FOUR": "text_delta",
        "SHORT_FIVE": "type",
    }
    _, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(
                f"done call {PROTOCOL_VERSION} text_delta type",
                {"done": "done", "number": 4, "ok": True},
            ),
            sdk_env={"CURIE_MODEL_ENV_KEY": json.dumps(list(values)), **values},
        )
    )
    assert {frame["type"] for frame in frames} == {
        "text_delta", "tool_note", "side_effect_flag", "final"
    }
    assert all(frame["version"] == PROTOCOL_VERSION for frame in frames)
    assert frames[-1]["type"] == "final"
    assert frames[-1]["status"] == "done"
    assert frames[-1]["input_tokens"] == 20
    for secret in values.values():
        assert secret not in _assistant_text(frames)
        assert secret not in frames[-1]["text"]
    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert closing["call_id"] == "call"
    assert closing["result"]["number"] == 4
    assert closing["result"]["ok"] is True


def test_http_preserves_clean_text_structure_and_unmatched_secret_prefix_on_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    text = 'Ordinary unknownOpaqueValue0000 "quoted" \\path\n雪 q7X'
    clean_result = {"nested": ["ordinary", {"count": 4}], "nil": None, "ok": False}
    _, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(text, clean_result),
            config_env={"CURIE_RUNNER_TOKEN": _SPLIT_SECRET, "CURIE_CREDENTIALS": ""},
            sdk_env={"CUSTOM_EMPTY_INPUT": "", "CURIE_MODEL_ENV_KEY": "CUSTOM_EMPTY_INPUT"},
        )
    )
    assert _assistant_text(frames) == text
    assert frames[-1]["text"] == text
    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert closing["result"] == clean_result


@pytest.mark.parametrize("failure_surface", ["assistant", "result", "exception"])
def test_http_scrubs_held_secret_from_provider_and_runner_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_surface: str
) -> None:
    secret = "FAKEFAILUREINPUTVALUE0000"
    text = f"Provider failed with {secret}."

    class FailingProvider(FakeModelSession):
        async def receive_turn(self):
            if False:
                yield None
            raise RuntimeError(text)

    script: list[object] = (
        [
            AssistantMessage(content=[TextBlock(text=text)], model="fake", error="server-error"),
            _result(text, failed=True),
        ]
        if failure_surface == "assistant"
        else [_result(text, failed=True)]
    )
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            script,
            config_env={"CURIE_RUNNER_TOKEN": secret},
            provider=FailingProvider() if failure_surface == "exception" else None,
        )
    )
    assert secret not in raw
    assert any(frame["type"] == "error" for frame in frames)
    assert any("[REDACTED:" in frame["message"] for frame in frames if frame["type"] == "error")
    assert frames[-1]["status"] == "classified-failure"


def test_http_redacts_permission_display_and_keeps_approved_arguments_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "FAKEAPPROVALINPUTVALUE0000"
    arguments = {"command": f"echo {secret}"}
    script: list[object] = [
        AssistantMessage(
            content=[ToolUseBlock(id="approval-call", name="Bash", input=arguments)], model="fake"
        ),
        _result("done"),
    ]
    _, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            script,
            config_env={"CURIE_RUNNER_TOKEN": secret},
            manifest={
                "name": "acme-bot",
                "approvalPolicy": {
                    "gates": [{"gate": "Bash", "route": "managers", "summary": "Run {command}."}]
                },
            },
        )
    )
    final = frames[-1]
    assert final["status"] == "awaiting-approval"
    assert secret not in final["approval_summary"]
    assert secret not in final["approval_display"]
    assert "[REDACTED:" in final["approval_display"]
    assert final["approval_granted_arguments"] == arguments
    assert final["approval_granted_tool"] == "Bash"
    opening = next(frame for frame in frames if frame["type"] == "side_effect_flag")
    assert opening["arguments"] == arguments


def test_http_collects_otel_auth_headers_and_preserves_benign_header_and_marker_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_secret = "FAKECONFIGOTELVALUE0000"
    process_secret = "FAKEPROCESSOTELVALUE0000"
    sdk_secret = "FAKESDKOTELVALUE0000"
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        f"Authorization=Bearer%20{process_secret},X-Request-Label=ordinary-label",
    )
    monkeypatch.setenv("CURIE_CONNECTOR_SECRET_KEYS", "UNSET_CONNECTOR_INPUT")
    text = (
        f"Observed {config_secret} {process_secret} {sdk_secret} ordinary-label "
        "UNSET_CONNECTOR_INPUT."
    )
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(text),
            config_env={
                "OTEL_EXPORTER_OTLP_HEADERS":
                    f"Authorization=Bearer%20{config_secret},X-Request-Label=ordinary-label"
            },
            sdk_env={
                "OTEL_EXPORTER_OTLP_HEADERS":
                    f"X-API-Key={sdk_secret},X-Request-Label=ordinary-label"
            },
        )
    )
    for value in (config_secret, process_secret, sdk_secret):
        assert value not in raw
        assert value not in _assistant_text(frames)
    assert "ordinary-label" in _assistant_text(frames)
    assert "UNSET_CONNECTOR_INPUT" in _assistant_text(frames)


@pytest.mark.parametrize(
    ("header_name", "header_value", "credential"),
    [
        ("Authorization", FAKE_BASIC_HEADER, FAKE_BASIC_HEADER.removeprefix("Basic ")),
        ("X-API-Key", "FAKELITERALAPIHEADERVALUE0000", "FAKELITERALAPIHEADERVALUE0000"),
        ("X-Curie-Caller", "FAKELITERALCALLERHEADERVALUE0000", "FAKELITERALCALLERHEADERVALUE0000"),
    ],
)
def test_http_collects_literal_auth_header_credential_portions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    header_name: str,
    header_value: str,
    credential: str,
) -> None:
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(f"Observed {credential} and {header_value} ordinary-header."),
            mcp={
                "mcpServers": {
                    "external": {
                        "type": "http",
                        "url": "https://mcp.example.com/mcp",
                        "headers": {
                            header_name: header_value,
                            "X-Request-Label": "ordinary-header",
                        },
                    }
                }
            },
        )
    )
    assert credential not in raw
    assert header_value not in raw
    assert "[REDACTED:" in _assistant_text(frames)
    assert "ordinary-header" in _assistant_text(frames)


@pytest.mark.parametrize("variable_present", [True, False], ids=["present", "absent"])
def test_http_collects_auth_header_variable_default_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variable_present: bool
) -> None:
    # Claude MCP documents ${VAR:-default} for env and headers: use VAR when
    # set, otherwise default. Verified 2026-09-30 by the driver.
    # https://code.claude.com/docs/en/mcp.md
    fallback = "FAKEFALLBACKHEADERVALUE0000"
    credential = "FAKEPRESENTHEADERVALUE0000" if variable_present else fallback
    monkeypatch.delenv("PAT", raising=False)
    sdk_env = {"PAT": credential} if variable_present else {}
    text = f"Observed {credential} ordinary-header."
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(text, {"observed": text}),
            sdk_env=sdk_env,
            mcp={
                "mcpServers": {
                    "external": {
                        "type": "http",
                        "url": "https://mcp.example.com/mcp",
                        "headers": {"Authorization": "Bearer ${PAT:-" + fallback + "}"},
                    }
                }
            },
        )
    )
    assert credential not in raw
    assert "[REDACTED:" in _assistant_text(frames)
    assert "ordinary-header" in _assistant_text(frames)
    assert frames[-1]["status"] == "done"
    closing = next(frame for frame in frames if frame.get("result") is not None)
    assert credential not in closing["result"]["observed"]


@pytest.mark.parametrize("secret", ["!", "xy"])
def test_http_redacts_held_values_without_a_minimum_length(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, secret: str
) -> None:
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            _reply_script(f"Before {secret} after.", {"observed": secret}),
            config_env={"CURIE_RUNNER_TOKEN": secret},
        )
    )
    assert secret not in raw
    assert _assistant_text(frames).startswith("Before [REDACTED:")
    assert _assistant_text(frames).endswith(" after.")


class _PausedReplyProvider(FakeModelSession):
    """A provider whose completion is controlled outside the runner."""

    def __init__(self, release: anyio.Event) -> None:
        super().__init__()
        self.release = release

    async def receive_turn(self):
        if len(self.queries) > 1:
            text = _SPLIT_SECRET[3:] + " clean followup"
            yield AssistantMessage(content=[TextBlock(text=text)], model="fake")
            yield _result(text)
            return
        yield AssistantMessage(
            content=[TextBlock(text="Visible prefix. " + _SPLIT_SECRET[:3])], model="fake"
        )
        await self.release.wait()
        yield AssistantMessage(content=[TextBlock(text=_SPLIT_SECRET[3:])], model="fake")
        yield _result("Visible prefix. " + _SPLIT_SECRET)

    async def interrupt(self) -> None:
        await super().interrupt()
        self.release.set()

    async def close(self) -> None:
        self.release.set()
        await super().close()


def test_http_emits_clean_prefix_before_provider_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        release = anyio.Event()
        provider = _PausedReplyProvider(release)
        runner = _boot_reply_runner(
            tmp_path,
            monkeypatch,
            [],
            config_env={"CURIE_RUNNER_TOKEN": _SPLIT_SECRET},
            provider=provider,
        )
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post("/v1/event", json=_MESSAGE_FRAME)
            try:
                with anyio.fail_after(2):
                    first_line = await response.content.readline()
                first = json.loads(first_line)
                assert first["type"] == "text_delta"
                assert first["text"] == "Visible prefix. "
                assert not release.is_set(), "clean text must arrive while the provider is waiting"
            finally:
                release.set()
            raw = first_line.decode() + await response.text()
            frames = [json.loads(line) for line in raw.splitlines()]
            assert _SPLIT_SECRET not in _assistant_text(frames)
            assert "[REDACTED:" in _assistant_text(frames)
            assert frames[-1]["status"] == "done"

    anyio.run(go)


def test_http_interrupt_does_not_carry_secret_prefix_into_followup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        release = anyio.Event()
        provider = _PausedReplyProvider(release)
        runner = _boot_reply_runner(
            tmp_path,
            monkeypatch,
            [],
            config_env={"CURIE_RUNNER_TOKEN": _SPLIT_SECRET},
            provider=provider,
        )
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post("/v1/event", json=_MESSAGE_FRAME)
            try:
                with anyio.fail_after(2):
                    first = json.loads(await response.content.readline())
                assert first["text"] == "Visible prefix. "
                stopped = await client.post(
                    "/v1/interrupt", json={"kind": "interrupt", "reason": "stop"}
                )
                assert stopped.status == 200
                assert (await stopped.json())["ok"] is True
            finally:
                release.set()
            interrupted = [json.loads(line) for line in (await response.text()).splitlines()]
            assert interrupted[-1]["type"] == "final"
            followup = await client.post("/v1/event", json=_MESSAGE_FRAME)
            assert followup.status == 200
            frames = [json.loads(line) for line in (await followup.text()).splitlines()]
            assert _assistant_text(frames) == _SPLIT_SECRET[3:] + " clean followup"
            assert frames[-1]["text"] == _assistant_text(frames)
            assert frames[-1]["status"] == "done"

    anyio.run(go)


def test_turn_close_drops_pending_secret_text_and_followup_is_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        release = anyio.Event()
        runner = _boot_reply_runner(
            tmp_path,
            monkeypatch,
            [],
            config_env={"CURIE_RUNNER_TOKEN": _SPLIT_SECRET},
            provider=_PausedReplyProvider(release),
        )
        await runner.start()
        try:
            stream = runner.run_turn(Event(type="message", text="first", user="U", ts="1"))
            first = json.loads(await anext(stream))
            assert first["text"] == "Visible prefix. "
            await stream.aclose()
            release.set()
            raw = "".join([
                line async for line in runner.run_turn(
                    Event(type="message", text="followup", user="U", ts="2")
                )
            ])
            frames = [json.loads(line) for line in raw.splitlines()]
            assert _assistant_text(frames) == _SPLIT_SECRET[3:] + " clean followup"
            assert frames[-1]["status"] == "done"
        finally:
            release.set()
            await runner.close()

    anyio.run(go)


def test_outbound_redactor_scrubs_base64_of_a_short_held_token() -> None:
    """Encoded forms of a 10 character held token are replaced."""

    secret = "acme-token"
    encoded = base64.standard_b64encode(secret.encode()).decode()
    redactor = OutboundRedactor(frozenset({secret}))
    raw = json.dumps({"type": "text_delta", "text": f"before {encoded} after"})
    emitted = redactor.push(raw + "\n")
    assert encoded not in "".join(emitted)
    assert "[REDACTED:held_secret]" in "".join(emitted)
    finished = redactor.finish()
    assert finished is None or encoded not in finished


def test_http_reply_scrubs_base64_of_a_held_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reply scrubs both base64 forms of a held secret and keeps an unrelated blob."""

    standard = "Pj4+PmhlbGQtc2VjcmV0LXZhbHVl"
    urlsafe = "Pj4-PmhlbGQtc2VjcmV0LXZhbHVl"
    unrelated = "QU5PTkVYRElGRkVSRU5U"
    text = f"before {standard} middle {urlsafe} after b64:{unrelated}"
    raw, frames = _http_reply(
        _boot_reply_runner(
            tmp_path,
            monkeypatch,
            [
                AssistantMessage(content=[TextBlock(text=text)], model="fake-model"),
                _result(text),
            ],
            config_env={"CURIE_RUNNER_TOKEN": ">>>>held-secret-value"},
        )
    )
    assistant = _assistant_text(frames)
    assert standard not in raw
    assert urlsafe not in raw
    assert standard not in assistant
    assert urlsafe not in assistant
    assert "[REDACTED:held_secret]" in raw
    assert "[REDACTED:held_secret]" in assistant
    assert unrelated in raw
    assert unrelated in assistant
    assert frames[-1]["status"] == "done"
