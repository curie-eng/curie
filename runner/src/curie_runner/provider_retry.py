"""Retry conditions the runner reads off a provider's own refusal (#5426).

Pure functions. Each one turns the provider signal that decided an error
classification into the ``RetryCondition`` that rides the same ``ErrorEvent``.
A condition is never built from ``ErrorEvent.classification`` alone: every
caller hands over the provider text or the typed SDK field it classified, at
the point it classified it. ``scope_key`` comes from the turn's
``ProviderScope`` and is None when the runner could not name the scope, when
the provider has no probe (only ``PROBED_PROVIDERS`` do), or when the refused
scope is wider than the turn's credential (an account or a workspace), so no
probe of the key could verify it.

Nothing here logs, and nothing here keeps the provider text.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from aci_protocol import RetryCondition, RetryReason
from curie_internal.provider_scope import PROBED_PROVIDERS, ProviderScope

# Observed 2026-10-29 (#5163, fixture ``_OPENROUTER_WORKSPACE_BUDGET_403`` in
# runner/tests/test_translate.py): an OpenRouter workspace daily budget refusal
# reads 'Workspace daily budget of $20.00 exceeded. Contact your org admin.
# $20.08 of $20.00 used; resets 2026-10-30T00:00:00.000Z.' inside the CLI's
# 'API Error: 403 {...}' text. The budget names a daily allowance that resets,
# so it is a usage reset, not depleted credit.
_WORKSPACE_DAILY_BUDGET = re.compile(r"Workspace daily budget of \$\S+ exceeded\.")
# The reset clause of that same refusal. The token ends at the sentence period
# that is followed by the closing JSON quote, whitespace or the end of the text,
# so the fraction's own period stays inside the token.
_RESETS_CLAUSE = re.compile(r"\bresets\s+(?P<when>[^\s\"]+?)\.(?=[\"\s]|$)")
# The only reset spelling read as a time: RFC 3339 UTC with an uppercase T, the
# Z designator and at most six fractional digits (the observed fixture has
# three). An offset, a naive time or any other spelling is not read.
_RFC3339_UTC = re.compile(
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,6}))?Z"
)

# The bundled CLI prefixes a provider HTTP refusal with 'API Error: <status>',
# observed for the 402 and 403 fixtures in runner/tests/test_translate.py.
# OpenRouter documents 401 for invalid credentials:
# https://openrouter.ai/docs/api/reference/errors-and-debugging
_CLI_HTTP_401 = re.compile(r"API Error: 401(?!\d)")
# Observed 2026-10-05 on a staging install (#4104, fixture
# ``_OPENROUTER_KEY_LIMIT_403`` in runner/tests/test_translate.py): an OpenRouter
# key at its own spend limit answers 'API Error: 403 {"error":{"message":"Key
# limit exceeded (total limit). ..."}}'. The refused scope is the key the turn
# authenticated with.
_KEY_LIMIT_EXCEEDED = re.compile(r"\bkey limit exceeded\b", re.IGNORECASE)
# Anthropic's documented error body for an invalid credential (HTTP 401):
# https://docs.anthropic.com/en/api/errors
_ANTHROPIC_AUTHENTICATION_ERROR = re.compile(r'"type"\s*:\s*"authentication_error"')


def _scope_key(scope: ProviderScope | None) -> str | None:
    # A provider with no non-generating allowance read cannot be probed, so a
    # key on its condition would name a wait nothing can release (#5424).
    if scope is None or scope.provider not in PROBED_PROVIDERS:
        return None
    return scope.scope_key


def _parse_utc_reset(value: str) -> datetime | None:
    """The UTC instant an RFC 3339 Z timestamp names, or None for any other spelling."""

    match = _RFC3339_UTC.fullmatch(value)
    if match is None:
        return None
    fraction = (match["fraction"] or "").ljust(6, "0")
    try:
        return datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
            int(fraction),
            tzinfo=UTC,
        )
    except ValueError:
        # An impossible calendar value, such as a day past the end of its month.
        return None


def daily_budget_reset_at(provider_text: str) -> tuple[bool, datetime | None]:
    """Whether ``provider_text`` is the workspace daily budget refusal, and its reset.

    The reset is None unless the refusal names it as an RFC 3339 UTC time.
    """

    budget = _WORKSPACE_DAILY_BUDGET.search(provider_text)
    if budget is None:
        return False, None
    clause = _RESETS_CLAUSE.search(provider_text, budget.end())
    if clause is None:
        return True, None
    return True, _parse_utc_reset(clause["when"])


def credit_exhausted_condition(provider_text: str, scope: ProviderScope | None) -> RetryCondition:
    """The condition for a refusal classified ``model-credit-exhausted``.

    The workspace daily budget refusal is a usage reset, timed when it names a
    UTC reset. Every other credit refusal (an HTTP 402, the SDK's
    ``billing_error``, an OpenRouter key at its limit) is depleted credit. Only
    the key limit refusal names the turn's key scope.
    """

    is_daily_budget, reset_at = daily_budget_reset_at(provider_text)
    if is_daily_budget:
        # The budget is workspace scoped, not the key's, so a key probe cannot
        # verify it. Only its typed reset time can release the wait.
        return RetryCondition(reason=RetryReason.USAGE_RESET, retry_at=reset_at, scope_key=None)
    if _KEY_LIMIT_EXCEEDED.search(provider_text):
        return RetryCondition(reason=RetryReason.CREDIT_EXHAUSTED, scope_key=_scope_key(scope))
    # An account-level refusal (an OpenRouter 402 or the SDK's billing_error,
    # or any other credit text): the refused scope is the account, so no key
    # probe can verify it, and the wait expires visibly.
    return RetryCondition(reason=RetryReason.CREDIT_EXHAUSTED, scope_key=None)


def usage_limited_condition(scope: ProviderScope | None) -> RetryCondition:
    """The condition for a refusal classified ``model-usage-limited``.

    The subscription text's reset ('resets 3pm (America/New_York)') is natural
    language and never supplies a time.
    """

    return RetryCondition(reason=RetryReason.USAGE_RESET, scope_key=_scope_key(scope))


def rate_limited_condition(resets_at: object, scope: ProviderScope | None) -> RetryCondition:
    """The condition for a rejected ``RateLimitEvent``, given its ``resets_at``.

    Provenance: the typed SDK field ``claude_agent_sdk.types.RateLimitInfo.resets_at``
    ('Unix timestamp when the rate limit window resets', typed ``int | None``).
    The caller hands over the plain value so this module stays free of the SDK.
    Read only when it is a real int, as Unix seconds in UTC.
    """

    retry_at: datetime | None = None
    if isinstance(resets_at, int) and not isinstance(resets_at, bool):
        try:
            retry_at = datetime.fromtimestamp(resets_at, tz=UTC)
        except (OverflowError, OSError, ValueError):
            retry_at = None
    return RetryCondition(
        reason=RetryReason.RATE_LIMITED, retry_at=retry_at, scope_key=_scope_key(scope)
    )


def credential_rejected_condition(
    provider_text: str, scope: ProviderScope | None
) -> RetryCondition | None:
    """The condition for a credential rejection, only on a documented 401.

    ``provider_text`` is the redacted provider message off the rejecting
    assistant message. A generic 403 or any other text gives no condition.
    """

    if _CLI_HTTP_401.match(provider_text.lstrip()) or _ANTHROPIC_AUTHENTICATION_ERROR.search(
        provider_text
    ):
        return RetryCondition(reason=RetryReason.CREDENTIAL_REJECTED, scope_key=_scope_key(scope))
    return None
