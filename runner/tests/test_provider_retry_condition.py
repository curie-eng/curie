"""Provider retry conditions on runner ErrorEvents, across the NDJSON wire (#5426).

Every case drives the real translator or the real session, then encodes each
ErrorEvent with the ACI NDJSON encoder and decodes it back, so the asserted
``retry_condition`` is what a worker would read off the stream.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import cast

import anyio
import pytest
from aci_protocol import (
    ErrorEvent,
    Event,
    RetryCondition,
    RetryReason,
    SessionStatus,
    parse_ndjson,
    parse_ndjson_line,
    to_ndjson_line,
)
from claude_agent_sdk import (
    AssistantMessage,
    RateLimitEvent,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import AssistantMessageError, RateLimitInfo
from curie_internal.provider_scope import ProviderScope, provider_scope
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_runner.translate import TurnState, translate_message

# Low-entropy placeholder credentials, never real keys.
_OPENROUTER_PLACEHOLDER_KEY = "sk-or-v1-" + "0" * 64
_SCOPE = provider_scope({"CURIE_CREDENTIALS": _OPENROUTER_PLACEHOLDER_KEY})
assert _SCOPE is not None
_SCOPE_KEY = _SCOPE.scope_key

# Observed 2026-10-29 (#5163, the same fixture as test_translate.py and
# test_session.py): an OpenRouter workspace daily budget refusal is HTTP 403,
# and the SDK reports it as error="authentication_failed".
_OPENROUTER_WORKSPACE_BUDGET_403 = (
    'API Error: 403 {"error":{"message":"Workspace daily budget of $20.00 exceeded. '
    'Contact your org admin. $20.08 of $20.00 used; resets 2026-10-30T00:00:00.000Z.",'
    '"code":403}}'
)


def _budget_403_with_reset(reset: str) -> str:
    """The observed workspace budget refusal with its reset clause respelled."""

    return _OPENROUTER_WORKSPACE_BUDGET_403.replace("resets 2026-10-30T00:00:00.000Z.", reset)


# Observed 2026-09-24 (#3073, test_translate.py): the bundled SDK reports
# OpenRouter's HTTP 402 body as error="unknown" with this assistant text.
_OPENROUTER_402 = (
    "API Error: 402 This request requires more credits, or fewer max_tokens. You "
    "requested up to 32000 tokens, but can only afford 1200. To increase, visit "
    "https://openrouter.ai/settings/credits and upgrade to a paid account"
)
# Observed 2026-10-05 on a staging install (#4104, test_translate.py): an
# OpenRouter key at its own spend limit, reported as error="authentication_failed".
_OPENROUTER_KEY_LIMIT_403 = (
    'API Error: 403 {"error":{"message":"Key limit exceeded (total limit). Manage it '
    'using https://openrouter.ai/workspaces/example/keys/example","code":403}}'
)
# The subscription refusal the bundled CLI formatter emits (translate.py,
# _USAGE_LIMITED_TEXT, SDK 0.2.159 / CLI 2.1.281), with its natural-language reset.
_SUBSCRIPTION_LIMIT = "You've hit your session limit · resets 3pm (America/New_York)"
# OpenRouter documents HTTP 401 for invalid credentials, with an
# {"error":{"code","message"}} body:
# https://openrouter.ai/docs/api/reference/errors-and-debugging
# The 'API Error: <status>' prefix is the CLI format observed for the 402 and
# 403 fixtures above.
_OPENROUTER_401 = 'API Error: 401 {"error":{"message":"User not found.","code":401}}'
# Anthropic's documented authentication error body (HTTP 401):
# https://docs.anthropic.com/en/api/errors
_ANTHROPIC_401_BODY = (
    '{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}'
)
# A 403 with no credit, budget or documented 401 text (the shape test_translate.py
# pins as not credit-exhausted).
_GENERIC_403 = 'API Error: 403 {"error":{"message":"invalid api key","code":403}}'

_RESET = datetime(2026, 10, 30, tzinfo=UTC)


def _wire(event: object) -> ErrorEvent:
    """Encode one ErrorEvent as NDJSON and decode it back, as the worker reads it."""

    assert isinstance(event, ErrorEvent)
    decoded = parse_ndjson_line(to_ndjson_line(event))
    assert isinstance(decoded, ErrorEvent)
    return decoded


def _translate(message: object, state: TurnState) -> list[ErrorEvent]:
    events = translate_message(message, state, SideEffectClassifier(), None)
    return [_wire(event) for event in events if isinstance(event, ErrorEvent)]


def _state(scope: ProviderScope | None = _SCOPE) -> TurnState:
    return TurnState(provider_scope=scope)


def _errored(error: str, text: str) -> AssistantMessage:
    return AssistantMessage(
        content=[TextBlock(text=text)] if text else [],
        model="<synthetic>",
        error=cast("AssistantMessageError", error),
    )


def _result(*, is_error: bool, result: str = "done", subtype: str = "success") -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="s",
        result=result,
    )


def _runner(script: list[object], scope: ProviderScope | None) -> SessionRunner:
    fake = FakeModelSession(lambda: script)
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="provider-retry",
        provider_scope=scope,
    )


def _drain(runner: SessionRunner) -> Sequence[object]:
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_inbound(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    return parse_ndjson("".join(lines))


def _errors(events: Sequence[object]) -> list[ErrorEvent]:
    return [event for event in events if isinstance(event, ErrorEvent)]


# --- Documented daily reset ---------------------------------------------------


def test_workspace_daily_budget_carries_its_utc_reset_and_scope() -> None:
    errors = _translate(
        _errored("authentication_failed", _OPENROUTER_WORKSPACE_BUDGET_403), _state()
    )

    assert [e.classification for e in errors] == ["model-credit-exhausted"]
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=_RESET, scope_key=_SCOPE_KEY
    )
    assert '"retry_at":"2026-10-30T00:00:00Z"' in to_ndjson_line(errors[0])


@pytest.mark.parametrize(
    "reset",
    [
        pytest.param("resets 2026-10-30T00:00:00.000+00:00.", id="offset"),
        pytest.param("resets 2026-10-30T00:00:00.000.", id="naive"),
        pytest.param("", id="missing"),
        pytest.param("resets soon.", id="malformed"),
        pytest.param("resets 2026-02-30T00:00:00.000Z.", id="impossible-date"),
        pytest.param("resets 2026-10-30 00:00:00Z.", id="space-separator"),
    ],
)
def test_workspace_daily_budget_without_a_utc_reset_has_no_time(reset: str) -> None:
    text = _budget_403_with_reset(reset)
    errors = _translate(_errored("authentication_failed", text), _state())

    assert [e.classification for e in errors] == ["model-credit-exhausted"]
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=None, scope_key=_SCOPE_KEY
    )


def test_workspace_daily_budget_through_the_session_ends_on_the_reset_condition() -> None:
    runner = _runner(
        [
            _errored("authentication_failed", _OPENROUTER_WORKSPACE_BUDGET_403),
            _result(is_error=True, result=_OPENROUTER_WORKSPACE_BUDGET_403),
        ],
        _SCOPE,
    )
    events = _drain(runner)

    errors = _errors(events)
    assert errors[-1].classification == "model-credit-exhausted"
    assert errors[-1].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=_RESET, scope_key=_SCOPE_KEY
    )
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_a_subagent_workspace_daily_budget_result_carries_the_reset() -> None:
    # The Agent tool's error result wraps the provider text (#3935 measured shape).
    state = _state()
    call = AssistantMessage(
        content=[ToolUseBlock(id="toolu_rev1", name="Agent", input={"description": "review"})],
        model="m",
    )
    result = UserMessage(
        content=[
            ToolResultBlock(
                tool_use_id="toolu_rev1",
                content="Agent terminated early due to an API error: "
                + _OPENROUTER_WORKSPACE_BUDGET_403,
                is_error=True,
            )
        ]
    )
    errors = _translate(call, state) + _translate(result, state)
    errors += _translate(_result(is_error=False, result="recovered"), state)

    assert [e.classification for e in errors] == ["model-credit-exhausted"]
    assert errors[-1].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=_RESET, scope_key=_SCOPE_KEY
    )


# --- Depleted credit -----------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "text"),
    [
        pytest.param("unknown", _OPENROUTER_402, id="openrouter-402"),
        pytest.param("authentication_failed", _OPENROUTER_KEY_LIMIT_403, id="key-limit-403"),
        pytest.param("billing_error", "", id="sdk-billing-error"),
    ],
)
def test_other_credit_refusals_are_credit_exhausted_without_a_time(error: str, text: str) -> None:
    errors = _translate(_errored(error, text), _state())

    assert [e.classification for e in errors] == ["model-credit-exhausted"]
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.CREDIT_EXHAUSTED, retry_at=None, scope_key=_SCOPE_KEY
    )


def test_key_limit_403_through_the_session_is_credit_exhausted() -> None:
    runner = _runner(
        [
            _errored("authentication_failed", _OPENROUTER_KEY_LIMIT_403),
            _result(is_error=True, result=_OPENROUTER_KEY_LIMIT_403),
        ],
        _SCOPE,
    )
    errors = _errors(_drain(runner))

    assert "model-credential-rejected" not in [e.classification for e in errors]
    assert errors[-1].retry_condition == RetryCondition(
        reason=RetryReason.CREDIT_EXHAUSTED, scope_key=_SCOPE_KEY
    )


# --- Subscription usage --------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "text"),
    [
        pytest.param("unknown", _SUBSCRIPTION_LIMIT, id="subscription-text"),
        pytest.param("rate_limit", "", id="sdk-rate-limit-code"),
    ],
)
def test_usage_limits_are_usage_reset_without_a_time(error: str, text: str) -> None:
    errors = _translate(_errored(error, text), _state())

    assert [e.classification for e in errors] == ["model-usage-limited"]
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=None, scope_key=_SCOPE_KEY
    )


def test_a_usage_limited_result_message_is_usage_reset_without_a_time() -> None:
    errors = _translate(_result(is_error=True, result=_SUBSCRIPTION_LIMIT), _state())

    assert [e.classification for e in errors] == ["model-usage-limited"]
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=None, scope_key=_SCOPE_KEY
    )


# --- Rate limit events -----------------------------------------------------------


def _rate_limit(info: RateLimitInfo) -> RateLimitEvent:
    return RateLimitEvent(rate_limit_info=info, uuid="u", session_id="s")


def test_a_rejected_rate_limit_carries_its_typed_reset_instant() -> None:
    # Provenance: the typed SDK field claude_agent_sdk.types.RateLimitInfo.resets_at
    # ("Unix timestamp when the rate limit window resets").
    resets_at = int(_RESET.timestamp())
    errors = _translate(
        _rate_limit(RateLimitInfo(status="rejected", resets_at=resets_at)), _state()
    )

    assert [e.classification for e in errors] == ["rate-limit"]
    assert errors[0].message == "model rate limit reached"
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.RATE_LIMITED, retry_at=_RESET, scope_key=_SCOPE_KEY
    )


@pytest.mark.parametrize(
    "resets_at",
    [pytest.param(None, id="absent"), pytest.param(True, id="bool")],
)
def test_a_rejected_rate_limit_without_an_int_reset_has_no_time(resets_at: object) -> None:
    info = RateLimitInfo(status="rejected", resets_at=resets_at)  # type: ignore[arg-type]
    errors = _translate(_rate_limit(info), _state())

    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.RATE_LIMITED, retry_at=None, scope_key=_SCOPE_KEY
    )


def test_an_allowed_warning_rate_limit_emits_no_event() -> None:
    info = RateLimitInfo(status="allowed_warning", resets_at=int(_RESET.timestamp()))
    assert _translate(_rate_limit(info), _state()) == []


# --- Credential rejection ---------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(_OPENROUTER_401, id="cli-401-openrouter-body"),
        pytest.param(_ANTHROPIC_401_BODY, id="anthropic-authentication-error-body"),
        pytest.param("API Error: 401 " + _ANTHROPIC_401_BODY, id="cli-401-anthropic-body"),
    ],
)
def test_a_documented_401_halt_is_credential_rejected(text: str) -> None:
    runner = _runner([_errored("authentication_failed", text), _result(is_error=False)], _SCOPE)
    events = _drain(runner)

    assert [type(e).__name__ for e in events] == ["ErrorEvent", "Final"]
    error = _errors(events)[0]
    assert error.classification == "model-credential-rejected"
    assert error.message == "model credential rejected by provider (check CURIE_CREDENTIALS)"
    assert error.retry_condition == RetryCondition(
        reason=RetryReason.CREDENTIAL_REJECTED, retry_at=None, scope_key=_SCOPE_KEY
    )


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(_GENERIC_403, id="generic-403"),
        pytest.param("", id="no-provider-text"),
        pytest.param("credential refused with status 401", id="401-without-cli-prefix"),
    ],
)
def test_an_undocumented_credential_halt_carries_no_condition(text: str) -> None:
    runner = _runner([_errored("authentication_failed", text), _result(is_error=False)], _SCOPE)
    errors = _errors(_drain(runner))

    assert [e.classification for e in errors] == ["model-credential-rejected"]
    assert errors[0].message == "model credential rejected by provider (check CURIE_CREDENTIALS)"
    assert errors[0].retry_condition is None


def test_a_personal_mode_halt_carries_no_condition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
    runner = _runner(
        [_errored("authentication_failed", _OPENROUTER_401), _result(is_error=False)], _SCOPE
    )
    errors = _errors(_drain(runner))

    assert [e.classification for e in errors] == ["authentication-required"]
    assert errors[0].retry_condition is None


# --- Scope, latching and unchanged classifications ---------------------------------


def test_no_scope_gives_a_condition_with_no_scope_key() -> None:
    errors = _translate(
        _errored("authentication_failed", _OPENROUTER_WORKSPACE_BUDGET_403), _state(None)
    )
    assert errors[0].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, retry_at=_RESET, scope_key=None
    )

    runner = _runner([_errored("authentication_failed", _OPENROUTER_401)], None)
    halted = _errors(_drain(runner))
    assert halted[0].retry_condition == RetryCondition(
        reason=RetryReason.CREDENTIAL_REJECTED, scope_key=None
    )


def test_a_later_recoverable_error_does_not_erase_the_latched_condition() -> None:
    state = _state()
    errors = _translate(_errored("unknown", _OPENROUTER_402), state)
    errors += _translate(_errored("server_error", "API Error: 500 upstream exploded"), state)
    errors += _translate(_rate_limit(RateLimitInfo(status="rejected")), state)
    errors += _translate(_result(is_error=True, result="run failed"), state)

    assert [e.classification for e in errors] == [
        "model-credit-exhausted",
        "server-error",
        "rate-limit",
        "model-credit-exhausted",
    ]
    assert errors[1].retry_condition is None
    assert errors[-1].message == "model credit exhausted earlier in the turn"
    assert errors[-1].retry_condition == RetryCondition(
        reason=RetryReason.CREDIT_EXHAUSTED, scope_key=_SCOPE_KEY
    )


def test_a_latched_usage_condition_outranks_a_later_credit_condition() -> None:
    state = _state()
    errors = _translate(_errored("unknown", _SUBSCRIPTION_LIMIT), state)
    errors += _translate(_errored("unknown", _OPENROUTER_402), state)
    errors += _translate(_result(is_error=False, result="recovered"), state)

    assert [e.classification for e in errors] == [
        "model-usage-limited",
        "model-credit-exhausted",
        "model-usage-limited",
    ]
    # The credit frame carries its own signal; the last frame re-emits the latch.
    assert errors[1].retry_condition == RetryCondition(
        reason=RetryReason.CREDIT_EXHAUSTED, scope_key=_SCOPE_KEY
    )
    assert errors[-1].message == "model usage limit reached earlier in the turn"
    assert errors[-1].retry_condition == RetryCondition(
        reason=RetryReason.USAGE_RESET, scope_key=_SCOPE_KEY
    )


@pytest.mark.parametrize(
    ("error", "text", "classification", "message"),
    [
        pytest.param(
            "authentication_failed",
            _OPENROUTER_WORKSPACE_BUDGET_403,
            "model-credit-exhausted",
            "model error: authentication_failed: " + _OPENROUTER_WORKSPACE_BUDGET_403,
            id="workspace-budget-403",
        ),
        pytest.param(
            "authentication_failed",
            _OPENROUTER_KEY_LIMIT_403,
            "model-credit-exhausted",
            "model error: authentication_failed: " + _OPENROUTER_KEY_LIMIT_403,
            id="key-limit-403",
        ),
        pytest.param(
            "unknown",
            _OPENROUTER_402,
            "model-credit-exhausted",
            "model error: unknown: " + _OPENROUTER_402,
            id="openrouter-402",
        ),
        pytest.param(
            "unknown",
            _SUBSCRIPTION_LIMIT,
            "model-usage-limited",
            "model error: unknown: " + _SUBSCRIPTION_LIMIT,
            id="subscription-limit",
        ),
    ],
)
def test_existing_fixture_classifications_and_messages_are_unchanged(
    error: str, text: str, classification: str, message: str
) -> None:
    with_scope = _translate(_errored(error, text), _state())
    without_scope = _translate(_errored(error, text), _state(None))

    for errors in (with_scope, without_scope):
        assert [(e.classification, e.message) for e in errors] == [(classification, message)]
