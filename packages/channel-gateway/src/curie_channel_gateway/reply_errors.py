"""Closed error vocabulary for reply delivery across the gateway boundary."""

import json
from collections.abc import Mapping
from types import MappingProxyType

import aiohttp


class MissingAdapterCredentialError(RuntimeError):
    """Platform egress had no credential (or no target) and sent nothing."""


class InvalidReplyTargetError(ValueError):
    """The reply target cannot be addressed, so no retry can ever deliver it."""


class RejectedAdapterResponseError(RuntimeError):
    """The adapter answered with an error status.

    Raised INSTEAD of ``ClientResponse.raise_for_status``: aiohttp's
    ``ClientResponseError`` carries ``request_info.real_url`` and prints the full
    URL in ``str(exc)``, so every upstream logger that formats the exception
    re-leaks the endpoint's path and query -- exactly what redaction here exists
    to prevent. The aiohttp exception is never constructed, never chained, and
    never reaches a log.
    """


class DeletedReplyTargetError(RejectedAdapterResponseError):
    """The adapter explicitly reports permanent provider thread deletion."""

    reason = "thread deleted at provider"


class ProviderEgressRefusedError(RejectedAdapterResponseError):
    """The adapter reports a refused outbound provider connection."""

    reason = "provider egress refused"


class OversizedAdapterResponseError(RuntimeError):
    """The adapter's acknowledgement body exceeded ``MAX_ACK_BODY_BYTES``.

    A delivery FAILURE, deliberately: the turn is retried or dead-lettered like
    any other rejected response rather than being recorded as delivered off a
    body the worker refused to finish reading.
    """


class RedirectedAdapterEndpointError(RuntimeError):
    """The adapter endpoint answered with a redirect, which is NOT followed.

    Following it would re-send the request -- and the per-adapter egress secret
    in ``X-Curie-Adapter-Secret`` -- to whatever origin the redirect named, so a
    compromised or misconfigured adapter could bounce the platform's credential
    anywhere. A redirect is therefore a delivery FAILURE, loud like any other
    rejected response, never a hop.
    """


class UntrustedSlackEndpointError(RuntimeError):
    """A turn named a Slack endpoint outside the configured Slack origin.

    Refused loudly and BEFORE any request: the route's bot token (ADR-0168
    decision 5) authenticates every Slack call, so honoring an arbitrary
    per-turn base URL hands that token to whoever named it (D4.4, finding 8).
    """


class UnconfiguredSlackIdentityError(RuntimeError):
    """A Slack route names an identity this worker holds no bot token for.

    Refused before any request (ADR-0168 decision 5): another identity's token
    would post as the wrong bot.
    """


class ReplyPayloadTooLargeError(InvalidReplyTargetError):
    """The configured receiver cannot accept this event's serialized size."""


class ReplayedReplyRequestError(InvalidReplyTargetError):
    """A signed request nonce has already been consumed."""


class ConflictingReplyRequestError(InvalidReplyTargetError):
    """An idempotency key was reused with different delivery input."""


class IndeterminateReplyDeliveryError(RuntimeError):
    """A claimed delivery has no confirmed outcome and cannot safely run again."""


class ProviderReplyUnreachableError(aiohttp.ClientConnectionError):
    """The gateway's configured provider could not be reached."""


ERROR_RESPONSES: Mapping[type[Exception], tuple[int, str]] = MappingProxyType(
    {
        InvalidReplyTargetError: (400, "invalid_reply_target"),
        MissingAdapterCredentialError: (403, "missing_adapter_credential"),
        UnconfiguredSlackIdentityError: (404, "unconfigured_slack_identity"),
        ValueError: (406, "invalid_reply_event"),
        IndeterminateReplyDeliveryError: (409, "reply_delivery_indeterminate"),
        DeletedReplyTargetError: (410, DeletedReplyTargetError.reason),
        ConflictingReplyRequestError: (412, "reply_request_conflict"),
        ReplyPayloadTooLargeError: (413, "reply_payload_too_large"),
        UntrustedSlackEndpointError: (421, "untrusted_slack_endpoint"),
        RejectedAdapterResponseError: (422, "rejected_adapter_response"),
        ProviderEgressRefusedError: (424, ProviderEgressRefusedError.reason),
        ReplayedReplyRequestError: (425, "reply_request_replayed"),
        OversizedAdapterResponseError: (502, "oversized_adapter_response"),
        ProviderReplyUnreachableError: (503, "reply_provider_unreachable"),
        RedirectedAdapterEndpointError: (508, "redirected_adapter_endpoint"),
    }
)


def reply_error_response(error: Exception) -> tuple[int, str]:
    """Expose fixed diagnostics, never provider messages or route credentials."""
    return ERROR_RESPONSES.get(type(error), (500, "reply_delivery_failed"))


def reply_error_from(
    status: int, payload: bytes, *, provider_outcomes: bool = True
) -> Exception | None:
    try:
        body = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(body, dict):
        return None
    for error_type, (code, detail) in ERROR_RESPONSES.items():
        if not provider_outcomes and error_type in {
            DeletedReplyTargetError,
            ProviderEgressRefusedError,
        }:
            continue
        if status == code and body.get("detail") == detail:
            return error_type(detail)
    return None
