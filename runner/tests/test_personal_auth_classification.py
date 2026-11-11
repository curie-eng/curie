"""Personal mode ends a refused or rejected owner grant as authentication-required (#5377).

ADR 0268 section 1 and #5377 decision 9: missing material and a real provider
rejection of the owner's grant become ``authentication-required``, a terminal
class the worker does not retry, with a message that names the enrollment the
owner must reconnect or replace. Service mode keeps ``model-credential-rejected``
naming ``CURIE_CREDENTIALS``. No case infers expiry from the token's shape.

The SDK boundary is the only thing faked: the missing-grant case runs the real
``build_options`` and ``ClaudeAgentSession`` with ``ClaudeSDKClient.connect``
replaced by a probe that fails the test if anything reaches it, and the
rejection cases replay SDK messages through ``FakeModelSession``. Every
credential here is a placeholder.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import anyio
import claude_agent_sdk
import pytest
from aci_protocol import ErrorEvent, Event, Final, SessionStatus, parse_ndjson
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.fake import FakeModelSession
from curie_runner.personal_auth import (
    AuthenticationRequiredError,
    PersonalLaunchRefusedError,
    ReviewerModelRequiredError,
)
from curie_runner.session import SessionRunner

AUTH_MODE_ENV = "CURIE_MODEL_AUTH_MODE"
GRANT_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
SERVICE_API_KEY = "sk-ant-api03-PLACEHOLDER-service-key"
SERVICE_CREDENTIALS = "sk-ant-oat01-PLACEHOLDER-service-credentials"
SENTINEL = "SHOULD-NOT-APPEAR-AFTER-AUTH-FAIL"
REFUSAL_TEXT = "PLACEHOLDER-REFUSAL-TEXT-names-a-settings-file"

# The provider rejection shape. ``claude_agent_sdk.types.AssistantMessageError``
# (SDK 0.2.168, the version pinned in this repo) lists ``"authentication_failed"``
# as the value the SDK sets on ``AssistantMessage.error`` when the provider
# answers 401/403; session.py reads it as ``_AUTH_REJECTION_SDK_CODE``, and the
# observed staging fixtures in runner/tests/test_translate.py (#4104, #5163)
# carry the same field. Only that field is scripted here.
_REJECTION = AssistantMessage(content=[], model="m", error="authentication_failed")


def _rejection_script() -> list[object]:
    return [
        _REJECTION,
        AssistantMessage(content=[TextBlock(text=SENTINEL)], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result=SENTINEL,
        ),
    ]


def _session_runner(factory: Callable[[], object]) -> SessionRunner:
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=factory,  # type: ignore[arg-type]
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )


def _turn(runner: SessionRunner) -> list[object]:
    """One turn with no prior ``start``: the turn itself awaits initialization."""

    lines: list[str] = []
    frame = Event(type="message", text="go", user="U", ts="1")

    async def go() -> None:
        try:
            async for line in runner.run_inbound(frame):
                lines.append(line)
        finally:
            await runner.close()

    anyio.run(go)
    return parse_ndjson("".join(lines))


def _clear_model_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (AUTH_MODE_ENV, GRANT_ENV, "ANTHROPIC_API_KEY", "CURIE_CREDENTIALS"):
        monkeypatch.delenv(name, raising=False)


def _logged(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def _assert_authentication_required(events: list[object], *, reason: str) -> ErrorEvent:
    assert [type(event) for event in events] == [ErrorEvent, Final]
    error, final = events
    assert isinstance(error, ErrorEvent) and isinstance(final, Final)
    assert error.classification == "authentication-required"
    assert error.message.startswith("authentication required: ")
    assert reason in error.message
    assert "reconnect or replace the personal Claude enrollment" in error.message
    assert "CURIE_CREDENTIALS" not in error.message
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    return error


def test_missing_grant_in_personal_mode_ends_authentication_required_before_any_sdk_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _clear_model_env(monkeypatch)
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    # Service credentials are present and usable; personal mode must not use them.
    monkeypatch.setenv("ANTHROPIC_API_KEY", SERVICE_API_KEY)
    monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_CREDENTIALS)
    connects: list[object] = []

    async def no_sdk_session(self: object) -> None:
        connects.append(self)
        raise AssertionError("an SDK session was started without an owner grant")

    monkeypatch.setattr(claude_agent_sdk.ClaudeSDKClient, "connect", no_sdk_session)
    options = build_options(
        plugins=[],
        model="claude-sonnet-5-5",
        reviewer_model="claude-opus-5-5",
        system_prompt=None,
        max_turns=1,
        max_budget_usd=None,
        resume=None,
        cwd=str(tmp_path),
        env={},
    )
    # The same refusal the session hits, read here only for its text.
    with pytest.raises(AuthenticationRequiredError) as refusal:
        anyio.run(ClaudeAgentSession(options).connect)
    refusal_text = str(refusal.value)
    caplog.clear()

    runner = _session_runner(lambda: ClaudeAgentSession(options))
    with caplog.at_level(logging.INFO, logger="curie_runner"):
        events = _turn(runner)

    error = _assert_authentication_required(
        events, reason="no personal Claude grant reached this runner"
    )
    assert connects == [], "the SDK client connected"
    assert runner.status is SessionStatus.CLASSIFIED_FAILURE
    logged = _logged(caplog)
    # Classified before any handler logged or copied the refusal's own text.
    assert refusal_text not in logged and refusal_text not in error.message
    assert "AuthenticationRequiredError" in logged
    for placeholder in (SERVICE_API_KEY, SERVICE_CREDENTIALS):
        assert placeholder not in logged
        assert all(placeholder not in str(event) for event in events)


@pytest.mark.parametrize(
    "refusal",
    [
        AuthenticationRequiredError(REFUSAL_TEXT),
        PersonalLaunchRefusedError(REFUSAL_TEXT),
        ReviewerModelRequiredError(REFUSAL_TEXT),
    ],
    ids=["missing-grant", "launch-refused", "reviewer-model"],
)
def test_personal_auth_refusal_text_is_never_logged_or_copied(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    refusal: Exception,
) -> None:
    _clear_model_env(monkeypatch)
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")

    class RefusingSession(FakeModelSession):
        async def connect(self) -> None:
            raise refusal

    runner = _session_runner(lambda: RefusingSession(_rejection_script))
    with caplog.at_level(logging.INFO, logger="curie_runner"):
        events = _turn(runner)

    reason = (
        "no personal Claude grant reached this runner"
        if isinstance(refusal, AuthenticationRequiredError)
        else f"personal model authentication refused the launch ({type(refusal).__name__})"
    )
    error = _assert_authentication_required(events, reason=reason)
    assert REFUSAL_TEXT not in error.message
    assert REFUSAL_TEXT not in _logged(caplog)
    assert all(REFUSAL_TEXT not in str(event) for event in events)


def test_provider_rejection_in_personal_mode_is_authentication_required(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _clear_model_env(monkeypatch)
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    fake = FakeModelSession(_rejection_script)
    runner = _session_runner(lambda: fake)

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _turn(runner)

    _assert_authentication_required(
        events, reason="the provider rejected the personal Claude grant"
    )
    # Fast-fail as in service mode: the live session is interrupted and nothing
    # after the rejection is consumed.
    assert fake.interrupts >= 1
    assert all(SENTINEL not in str(event) for event in events)
    assert runner.status is SessionStatus.CLASSIFIED_FAILURE
    assert "authentication required" in _logged(caplog)


@pytest.mark.parametrize("mode", [None, "service"], ids=["unset", "service"])
def test_service_mode_rejection_stays_model_credential_rejected(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    _clear_model_env(monkeypatch)
    if mode is not None:
        monkeypatch.setenv(AUTH_MODE_ENV, mode)
    fake = FakeModelSession(_rejection_script)

    events = _turn(_session_runner(lambda: fake))

    assert [type(event) for event in events] == [ErrorEvent, Final]
    error = events[0]
    assert isinstance(error, ErrorEvent)
    assert error.classification == "model-credential-rejected"
    assert "CURIE_CREDENTIALS" in error.message
    assert "enrollment" not in error.message
    assert fake.interrupts >= 1
