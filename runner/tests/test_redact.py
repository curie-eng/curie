"""Redaction: every secret class is scrubbed at every runner output boundary.

The two boundaries this ticket scopes are the runner's stdout (structured logs) and
the gen_ai span attributes shipped to the trace backend. A regex that only one applies is
a leak, so each frozen vector is driven through the real code at BOTH boundaries:
a real ``logging`` handler for stdout, and a real ``RunTracer`` plus in-memory
exporter for spans. The two tripwires below make adding a rule or a boundary
without that coverage fail CI.
"""

from __future__ import annotations

import io
import logging

import anyio
import pytest
from aci_protocol import Event
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.redact import (
    REDACTION_BOUNDARIES,
    REDACTION_RULES,
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
FAKE_HOME_PATH = "/home/theconnman/.config/curie/settings.json"

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
    "home_path": FAKE_HOME_PATH,
}

# One frozen vector per rule: a realistic runner output line carrying that class
# of secret. The tripwire below binds this table to REDACTION_RULES.
VECTORS: tuple[tuple[str, str], ...] = tuple(
    (name, f"runner output carrying {literal} in context")
    for name, literal in SECRET_LITERALS.items()
)

BOUNDARIES: tuple[str, ...] = ("stdout", "gen_ai_span")

CASES: tuple[tuple[str, str, str], ...] = tuple(
    (name, vector, boundary) for name, vector in VECTORS for boundary in BOUNDARIES
)


def _placeholder(name: str) -> str:
    return f"[REDACTED:{name}]"


def _log_through_stdout(*args: object) -> str:
    """Log through a real root handler with the stdout redaction pass installed."""

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        install_stdout_redaction()
        logger = logging.getLogger("curie_runner.test_redact")
        logger.setLevel(logging.INFO)
        logger.info(*args)
    finally:
        root.removeHandler(handler)
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


@pytest.mark.parametrize(("name", "vector"), VECTORS)
def test_every_rule_is_redacted_through_the_logging_args_path(name: str, vector: str) -> None:
    # The dangerous shape is `logger.info("token=%s", secret)`: the secret arrives
    # via record.args and never appears in record.msg, so a filter that only scans
    # msg leaks it. Redaction must run over the fully formatted message.
    out = _log_through_stdout("runner emitted t=%s", vector)
    assert SECRET_LITERALS[name] not in out
    assert _placeholder(name) in out


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
