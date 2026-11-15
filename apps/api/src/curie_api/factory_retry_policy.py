"""Factory policy for a failed implementation request (ADR 0269, #5424).

One pure function maps the typed facts of a terminal request (its cause, the
worker's safety fact and the provider retry condition it carried) to a
disposition. The SQL lifecycle owns the allowance, the fences and the timing;
this module only names what the factory table allows. It imports nothing but
the standard library so the table can be read and tested on its own.

Unknown causes and unproved conditions never default to a retry. A cause is
matched exactly: an underscored terminal cause, or one of the hyphenated
kernel classification tokens named in ``_TOKEN_ALIASES``. Nothing is inferred
by rewriting a string.

Owner loss is terminal here on purpose. Its successor is admitted only by the
observed teardown path (ADR 0206), so a worker that reports ``owner_lost`` by
name through a finish earns nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Final, Literal

RetrySafety = Literal["no_effects", "flagged", "uncertain"]

# The scheduler's own margin after a provider reset time; the producer reports
# the provider's time without it.
RESET_GRACE: Final = timedelta(seconds=60)


class DispositionKind(StrEnum):
    RETRY_BACKOFF = "retry_backoff"
    CONDITION_WAIT = "condition_wait"
    SAME_REQUEST_WAIT = "same_request_wait"
    PUBLICATION_RECOVERY = "publication_recovery"
    CI_OBSERVATION = "ci_observation"
    OBSERVE_ONLY = "observe_only"
    TERMINAL_SUCCESS = "terminal_success"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class RetryFacts:
    # An underscored terminal cause or a hyphenated classification token.
    cause: str
    # None means the fact is absent, which never admits.
    safety: RetrySafety | None
    # A RetryReason value, already validated at the wire boundary.
    condition_reason: str | None
    condition_retry_at: datetime | None


@dataclass(frozen=True)
class Disposition:
    kind: DispositionKind
    # The typed condition name; it names the block when one applies.
    condition: str | None = None
    # The provider time, with the reset grace already added where the row says so.
    not_before: datetime | None = None
    # True only when a fresh request may be admitted.
    successor: bool = False


# Kernel classification tokens and the cause each one reports. Every target is
# written out so a new token cannot match by accident.
_TOKEN_ALIASES: Final[dict[str, str]] = {
    "rate-limit": "model_rate_limited",
    "server-error": "model_error",
    "model-unreachable": "model_unreachable",
    "model-usage-limited": "model_usage_limited",
    "model-credit-exhausted": "model_credit_exhausted",
    "model-credential-rejected": "model_credential_rejected",
    "workspace-error": "workspace_error",
    "runner-error": "runner_error",
    "runner-timeout": "runner_timeout",
    "sandbox-terminated": "sandbox_terminated",
    "history-persistence-error": "history_capacity",
    "max-turns": "max_turns",
    "budget-exceeded": "budget_exceeded",
    "pull-request-not-adopted": "pull_request_not_adopted",
    "publication-unrecorded": "publication_unrecorded",
}
_OBSERVE_ONLY_TOKENS: Final = frozenset({"approval-not-acted"})
_SAME_REQUEST_TOKENS: Final = frozenset({"sandbox-capacity"})

# Row a: a provider throttle, transport failure or server error.
_PROVIDER_TRANSIENT: Final = frozenset({"model_rate_limited", "model_unreachable", "model_error"})
# Row b: a generic class retries only a diagnosed transient; otherwise it
# waits on the named diagnosis.
_DIAGNOSIS_CONDITIONS: Final[dict[str, str]] = {
    "workspace_error": "workspace_preparation",
    "runner_error": "runner_fault_diagnosis",
    "runner_failed": "runner_fault_diagnosis",
    "sandbox_terminated": "sandbox_disruption_diagnosis",
    "runner_timeout": "runner_stop_confirmation",
}
# Row d: a limit that needs a raised allowance before a fresh run.
_LIMIT_CONDITIONS: Final[dict[str, str]] = {
    "history_capacity": "history_capacity",
    "max_turns": "turn_limit",
    "execution_deadline": "execution_time_limit",
}
_PUBLICATION_RECOVERY: Final = frozenset(
    {"approval_create_failed", "publication_failed", "publication_unrecorded"}
)
_CI_OBSERVATION: Final = frozenset({"ci_timeout", "ci_unverified"})
_LINEAGE_WAIT: Final = frozenset({"merge_conflict", "pull_request_not_adopted"})

_TERMINAL: Final = Disposition(DispositionKind.TERMINAL)

_CONDITION_TEXT: Final[dict[str, str]] = {
    "provider_rate_limit": "the model provider's rate limit time",
    "provider_service": "the model provider's service recovery time",
    "provider_usage_reset": "the model provider's usage allowance reset",
    "provider_credit": "a verified model provider credit or limit increase",
    "provider_credential": "a successful sign in after the model credential is replaced",
    "workspace_preparation": "a repair of the workspace preparation fault",
    "runner_fault_diagnosis": "a diagnosis of the runner fault",
    "sandbox_disruption_diagnosis": "a diagnosis of the sandbox disruption",
    "runner_stop_confirmation": "confirmation of why the runner stopped",
    "history_capacity": "a raised conversation history capacity",
    "turn_limit": "a raised turn limit",
    "execution_time_limit": "a raised execution time limit",
    "start_preparation": "a repair of the sandbox start fault",
    "pull_request_lineage": "a verified change to the pull request base, head or ownership",
}


def decide(facts: RetryFacts) -> Disposition:
    """The factory disposition for one terminal request's typed facts."""

    cause = facts.cause
    if cause in _OBSERVE_ONLY_TOKENS:
        return Disposition(DispositionKind.OBSERVE_ONLY)
    if cause in _SAME_REQUEST_TOKENS:
        return Disposition(DispositionKind.SAME_REQUEST_WAIT)
    cause = _TOKEN_ALIASES.get(cause, cause)
    clean = facts.safety == "no_effects"
    reason = facts.condition_reason

    if cause in _PROVIDER_TRANSIENT:
        if not clean:
            return _TERMINAL
        if reason == "rate_limited":
            return _rate_limited(facts)
        if reason in (None, "transient_service"):
            at = facts.condition_retry_at
            return Disposition(
                DispositionKind.RETRY_BACKOFF,
                condition="provider_rate_limit" if at is not None else None,
                not_before=at,
                successor=True,
            )
        if reason == "usage_reset":
            return _usage_reset(facts)
        if reason == "credit_exhausted":
            return _credit(facts)
        if reason == "credential_rejected":
            return _credential()
        return _TERMINAL
    if cause in _DIAGNOSIS_CONDITIONS:
        if not clean:
            return _TERMINAL
        if reason == "transient_service":
            at = facts.condition_retry_at
            return Disposition(
                DispositionKind.RETRY_BACKOFF,
                condition="provider_service" if at is not None else None,
                not_before=at,
                successor=True,
            )
        return Disposition(
            DispositionKind.CONDITION_WAIT,
            condition=_DIAGNOSIS_CONDITIONS[cause],
            successor=True,
        )
    if cause == "model_usage_limited":
        return _usage_reset(facts) if clean else _TERMINAL
    if cause == "model_credit_exhausted":
        return _credit(facts) if clean else _TERMINAL
    if cause == "model_credential_rejected":
        return _credential() if clean else _TERMINAL
    if cause in _LIMIT_CONDITIONS:
        if not clean:
            return _TERMINAL
        return Disposition(
            DispositionKind.CONDITION_WAIT, condition=_LIMIT_CONDITIONS[cause], successor=True
        )
    if cause == "start_failed":
        return Disposition(DispositionKind.TERMINAL, condition="start_preparation")
    if cause in _PUBLICATION_RECOVERY:
        return Disposition(DispositionKind.PUBLICATION_RECOVERY)
    if cause in _CI_OBSERVATION:
        return Disposition(DispositionKind.CI_OBSERVATION)
    if cause in _LINEAGE_WAIT:
        return Disposition(DispositionKind.CONDITION_WAIT, condition="pull_request_lineage")
    if cause == "workspace_timeout":
        return Disposition(DispositionKind.SAME_REQUEST_WAIT)
    if cause == "completed":
        return Disposition(DispositionKind.TERMINAL_SUCCESS)
    # owner_lost, every refusal, every limit Curie owns and every unknown cause.
    return _TERMINAL


def _rate_limited(facts: RetryFacts) -> Disposition:
    # A typed throttle reset is scheduled with the reset grace. Without one the
    # retry waits blocked: no probe verifies a rate window, so a guessed timer
    # would only spend the retry early.
    at = facts.condition_retry_at
    if at is None:
        return Disposition(
            DispositionKind.CONDITION_WAIT, condition="provider_rate_limit", successor=True
        )
    return Disposition(
        DispositionKind.RETRY_BACKOFF,
        condition="provider_rate_limit",
        not_before=at + RESET_GRACE,
        successor=True,
    )


def _usage_reset(facts: RetryFacts) -> Disposition:
    at = facts.condition_retry_at
    trusted = facts.condition_reason == "usage_reset" and at is not None
    return Disposition(
        DispositionKind.CONDITION_WAIT,
        condition="provider_usage_reset",
        not_before=at + RESET_GRACE if trusted and at is not None else None,
        successor=True,
    )


def _credit(facts: RetryFacts) -> Disposition:
    # A daily reset refusal carries a usage reset time; a credit balance does not.
    if facts.condition_reason == "usage_reset" and facts.condition_retry_at is not None:
        return _usage_reset(facts)
    return Disposition(DispositionKind.CONDITION_WAIT, condition="provider_credit", successor=True)


def _credential() -> Disposition:
    # A rejected credential never resets on a clock, so a carrier time is ignored.
    return Disposition(
        DispositionKind.CONDITION_WAIT, condition="provider_credential", successor=True
    )


def condition_text(name: str) -> str:
    """The operator sentence fragment for a condition name."""

    return _CONDITION_TEXT.get(name, "a condition Curie cannot name")


__all__ = [
    "RESET_GRACE",
    "Disposition",
    "DispositionKind",
    "RetryFacts",
    "RetrySafety",
    "condition_text",
    "decide",
]
