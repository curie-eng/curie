"""Boot-time platform API and Slack channel capability gates (#442, #2205).

The dispatcher resolves Slack approval clicks by calling the platform API. When
``CURIE_API_URL`` points somewhere unreachable, the only symptom today is
a warning at click time and a dead-ended button: ``ApprovalResolveClient.resolve``
catches the ``httpx.HTTPError`` and returns ``ResolveOutcome(status_code=0)``.
This gate turns that silent misconfiguration into a loud boot failure naming the
URL it could not reach.

The gate is bounded retry, not a single probe: one probe at t=0 races the API's
own startup, and in Kubernetes pod start order is not ordered at all, so
fail-immediately would crash-loop a healthy stack. Restart backoff is the outer
retry loop there, and a CrashLoopBackOff with the named URL in the log is the
operator signal.

These are wiring gates, not liveness monitors: one shot at boot only. The API
health probe proves reachability; authenticated agent discovery loads the Slack
destinations before Slack starts. The Slack phase probes destination visibility,
but not message deliverability. Only the definitive ``missing_scope`` and
``invalid_types`` responses from the public-channel ``conversations.list`` probe
receive the exact ``channels:read`` recovery, and only a definitive
``missing_scope`` from the bounded ``files.list`` probe receives the exact
``files:read`` recovery (#2567) -- that second scope is what lets an inbound
attachment's bytes be fetched at all, and a workspace that has not reinstalled
since the scope was added would otherwise lose every upload silently, so it is
checked at boot rather than discovered in a worker log. Discovery failures and
aggregate deadline exhaustion also refuse startup; ambiguous Slack provider
outcomes remain ``unverified`` so one destination cannot crash-loop every agent.

API health retains its full configured startup budget. Once health succeeds,
authenticated discovery and all Slack checks share one fresh aggregate budget.
If that second deadline leaves any configured destination unattempted, startup
refuses rather than silently treating an absent scope check as success.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx
from slack_sdk.errors import SlackApiError
from slack_sdk.web import WebClient

from .app import _SLACK_API_TIMEOUT_SECONDS
from .config import DispatcherConfig
from .supervisor import BackoffPolicy

# Cap on any single probe, so one hung connect cannot eat the whole deadline and
# collapse bounded retry into the single probe the design rejected. A black-holed
# address (a DROP'd rule, an unroutable IP) hangs until this fires rather than
# refusing fast like a closed port does.
_MAX_PROBE_TIMEOUT_S = 5.0

_MISSING_CHANNELS_READ_MESSAGE = (
    "Slack channel capability preflight failed: bot token is missing required "
    "scope channels:read. Add channels:read under OAuth & Permissions > Bot "
    "Token Scopes, then reinstall the app to the workspace."
)
# The attachment scope's recovery (#2567). Unlike `channels:read` above this is
# NOT boot-fatal, and the difference is the point: `channels:read` is what
# routing itself needs, so a stack without it cannot answer anyone, while
# `files:read` is needed only to fetch an upload. Refusing to boot for it would
# stop every message in a workspace over a capability nobody had yesterday --
# and every existing installation would go down on upgrade until an admin
# reinstalled the app. So the bot keeps answering and attachments degrade,
# loudly: this text is logged at WARNING with the one scope to add, and the
# summary line names the probe so the reason is not buried.
_MISSING_FILES_READ_MESSAGE = (
    "Slack attachment capability unavailable: bot token is missing scope "
    "files:read, so inbound uploads cannot be fetched. Add files:read under "
    "OAuth & Permissions > Bot Token Scopes, then reinstall the app to the "
    "workspace. Messages are answered normally in the meantime."
)
_AGENT_DISCOVERY_FAILURE_MESSAGE = (
    "Slack channel capability preflight failed: could not load configured "
    "channel destinations from the platform API."
)
_AGENT_DISCOVERY_CONNECTIVITY_MESSAGE = (
    "Slack channel capability preflight failed: platform API agent discovery "
    "could not connect after bounded retries."
)
_AGENT_DISCOVERY_RESPONSE_SHAPE_MESSAGE = (
    "Slack channel capability preflight failed: platform API agent discovery "
    "returned an invalid response shape."
)
_SLACK_CAPABILITY_DEADLINE_MESSAGE = (
    "Slack channel capability preflight failed: could not attempt every configured "
    "destination within the bounded startup budget. Check Slack API availability "
    "and retry."
)
# slack_sdk logs request and response context at DEBUG/ERROR. Production
# preflight calls cross a redaction boundary, so give their clients a dedicated
# sink rather than allowing SDK records to inherit application/root handlers.
_SLACK_SDK_SILENT_LOGGER = logging.getLogger(f"{__name__}.slack_sdk_silent")
if not _SLACK_SDK_SILENT_LOGGER.handlers:
    _SLACK_SDK_SILENT_LOGGER.addHandler(logging.NullHandler())
_SLACK_SDK_SILENT_LOGGER.propagate = False
_SLACK_SDK_SILENT_LOGGER.setLevel(logging.CRITICAL + 1)

# Keep polling throughout longer readiness windows even when the shared
# reconnect backoff allows much longer sleeps. The deadline still bounds the
# whole preflight and each request remains capped separately above.
_MAX_POLL_INTERVAL_S = 10.0

class ApiUnreachableError(RuntimeError):
    """The platform API did not answer ``GET /health`` before the deadline."""


class SlackChannelPreflightError(RuntimeError):
    """Configured Slack destinations could not be safely capability-checked."""


class SlackChannelClient(Protocol):
    """The narrow Slack Web API surface used by the channel preflight."""

    def conversations_list(
        self,
        *,
        types: str,
        exclude_archived: bool,
        limit: int,
    ) -> Any: ...

    def conversations_info(self, *, channel: str) -> Any: ...

    # `files.list` is the `files:read` probe (#2567). It is deliberately the
    # whole surface for that scope: the method requires `files:read`, takes no
    # required argument, and answers `ok` with an empty array on a workspace
    # that holds no files, so boot never comes to depend on a particular file
    # existing. The alternative -- `files.info` on a known id -- would make the
    # gate unrunnable on a fresh workspace and turn a deleted file into a
    # crash-loop. https://docs.slack.dev/reference/methods/files.list
    #
    # The page-size argument is `count`, not `limit`: `files.list` predates
    # Slack's cursor pagination and pages with `count`/`page`. `slack_sdk`'s
    # `WebClient.files_list` types `count` as keyword-only and would swallow a
    # `limit` into `**kwargs`, sending an argument the method does not document
    # and quietly fetching the 100-file default page instead of one.
    def files_list(self, *, count: int) -> Any: ...


def _safe_for_log(url: str) -> str:
    """Drop any userinfo from a URL so credentials cannot reach the logs.

    AC2 requires naming the resolved URL, so the URL itself must survive; only
    the ``user:pass@`` part is removed. ``httpx`` accepts userinfo, so a BYO
    ``dispatcher.apiBaseUrl`` could otherwise write it into pod logs and any log
    shipper downstream.
    """
    parts = urlsplit(url)
    if not (parts.username or parts.password):
        return url
    host = parts.hostname or ""
    netloc = f"{host}:{parts.port}" if parts.port else host
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def check_api_reachable(
    config: DispatcherConfig,
    *,
    logger: logging.Logger,
    client: httpx.Client | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll ``GET {api_base_url}/health`` until it answers 200 or the deadline passes.

    Raises ``ApiUnreachableError`` naming the resolved base URL when the deadline
    expires.
    """
    # The resolve path rstrips its base too; without this a perfectly reasonable
    # "http://curie-api:8000/" would probe "//health" and fail a wired stack.
    base = config.api_base_url.rstrip("/")
    logged_base = _safe_for_log(base)
    timeout_s = config.api_preflight_timeout_s

    backoff = BackoffPolicy(
        initial_seconds=config.backoff_initial_seconds,
        max_seconds=config.backoff_max_seconds,
        multiplier=config.backoff_multiplier,
    )
    http = client or httpx.Client(timeout=min(_MAX_PROBE_TIMEOUT_S, timeout_s))
    owned = client is None
    start = monotonic()
    deadline = start + timeout_s
    attempt = 0
    last_error = ""
    try:
        while True:
            # Bound each probe to what is left of the budget, so the loop cannot
            # start a request it has no time for and then block past the deadline
            # on that request's own timeout. Without this a 30s deadline takes
            # ~35s and a short one nearly doubles.
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            try:
                response = http.get(
                    f"{base}/health", timeout=min(_MAX_PROBE_TIMEOUT_S, remaining)
                )
                if response.status_code == 200:
                    logger.info("platform API reachable at %s", logged_base)
                    return
                last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)

            delay = min(backoff.delay(attempt), _MAX_POLL_INTERVAL_S)
            attempt += 1
            # Clamp to the remaining budget rather than breaking when the next
            # delay would overshoot it. Combined with the poll ceiling above,
            # this keeps probes near the end of a long readiness window while
            # preserving the configured terminal deadline.
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(delay, remaining))
    finally:
        if owned:
            http.close()

    elapsed = monotonic() - start
    raise ApiUnreachableError(
        f"cannot reach the platform API at {logged_base} after {elapsed:.1f}s "
        f"({attempt} attempts, last error: {last_error}); check CURIE_API_URL; "
        "the API may still be starting, so check that the API pod is Ready"
    )


def _collect_slack_addresses(payload: object) -> set[str]:
    """Extract Slack destinations from the display-safe ``AgentOut`` projection."""
    if not isinstance(payload, list):
        raise ValueError("agent list is not an array")

    addresses: set[str] = set()

    def collect_target(target: object) -> None:
        if not isinstance(target, Mapping):
            raise ValueError("channel target is not an object")
        kind = target.get("kind")
        if not isinstance(kind, str):
            raise ValueError("channel target kind is malformed")
        if kind != "slack":
            return
        address = target.get("address")
        if not isinstance(address, str) or not address:
            raise ValueError("Slack channel target is malformed")
        addresses.add(address)

    for agent in payload:
        if not isinstance(agent, Mapping):
            raise ValueError("agent is not an object")

        channels = agent.get("channels")
        if not isinstance(channels, list):
            raise ValueError("agent channels are not an array")
        for target in channels:
            collect_target(target)

        approval_routes = agent.get("approval_routes")
        if approval_routes is None:
            continue
        if not isinstance(approval_routes, Mapping):
            raise ValueError("approval routes are not an object")
        for route in approval_routes.values():
            if not isinstance(route, Mapping) or "resolution" not in route:
                raise ValueError("approval route is malformed")
            collect_target(route["resolution"])
            notification = route.get("notification")
            if notification is not None:
                collect_target(notification)

    return addresses


def _slack_response_field(response: object, field: str) -> object:
    """Read a Slack response field without assuming a printable response shape."""
    getter = getattr(response, "get", None)
    if not callable(getter):
        return None
    try:
        return getter(field)
    except Exception:
        return None


def _slack_error_code(exc: SlackApiError) -> str | None:
    """Read only Slack's documented string error code from an SDK exception."""
    error = _slack_response_field(exc.response, "error")
    return error if isinstance(error, str) else None


def _is_conversations_list_success(response: object) -> bool:
    """Accept only the documented successful public-channel listing shape."""
    return _slack_response_field(response, "ok") is True and isinstance(
        _slack_response_field(response, "channels"), list
    )


def _is_files_list_success(response: object) -> bool:
    """Accept only the documented successful file-listing shape.

    An empty ``files`` array is a full pass: the probe proves the token may
    *ask*, which is the scope question, and says nothing about whether the
    workspace happens to hold a file today.
    """
    return _slack_response_field(response, "ok") is True and isinstance(
        _slack_response_field(response, "files"), list
    )


def _is_conversations_info_success(response: object) -> bool:
    """Fail closed if a client returns a malformed success instead of raising."""
    return _slack_response_field(response, "ok") is True and isinstance(
        _slack_response_field(response, "channel"), Mapping
    )


def _slack_probe_client(
    config: DispatcherConfig,
    *,
    injected: SlackChannelClient | None,
    remaining: float,
) -> SlackChannelClient:
    """Return the injected seam or a no-retry client bounded by remaining time."""
    if injected is not None:
        return injected

    # Ceil retains usable sub-second remainder while the one-second minimum
    # satisfies slack_sdk's integer timeout contract. With retries disabled,
    # aggregate overshoot is bounded to this one in-flight provider call.
    probe_timeout = max(
        1,
        min(_SLACK_API_TIMEOUT_SECONDS, math.ceil(remaining)),
    )
    return WebClient(
        token=config.slack_bot_token,
        timeout=probe_timeout,
        retry_handlers=[],
        logger=_SLACK_SDK_SILENT_LOGGER,
    )


def check_slack_channel_capabilities(
    config: DispatcherConfig,
    *,
    logger: logging.Logger,
    web_client: SlackChannelClient | None = None,
    api_client: httpx.Client | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Probe visibility of each unique Slack destination within a fixed budget.

    Agent discovery and Slack calls share one fresh configured deadline after the
    API health gate. A 401 fails immediately with a generic redaction-safe
    refusal, while deterministic response-shape failures fail promptly. A bounded
    public-channel-only ``conversations.list`` call proves ``channels:read``
    directly; its documented ``missing_scope`` and ``invalid_types`` errors are
    provider-terminal because the request fixes ``types=public_channel``. A
    second bounded call, ``files.list`` with ``count=1``, proves ``files:read``;
    it names no file, so boot never depends on one existing, and its
    ``missing_scope`` is unambiguous because the request carries no channel and
    no type filter. That answer is definitive but NOT fatal: `channels:read` is
    what routing needs, while `files:read` only fetches an upload, so a stack
    missing it keeps answering and loses attachments rather than refusing to
    start and losing everything. Ambiguous capability and destination outcomes are
    counted as unverified; a nondefinitive ``files:read`` probe raises the
    summary line's level AND names itself in its text, so an operator chasing a
    dropped attachment finds the attachment probe in the line rather than a
    warning that reads clean (#2567).
    Production builds a no-retry client for each provider call, with its integer
    timeout capped by both the remaining aggregate budget and the dispatcher's
    two-second Slack policy. If time expires before the capability probe or every
    destination is attempted, startup refuses with a fixed recovery message.
    Output never exposes credentials, destinations, scopes, bodies, or deployment
    identifiers.
    """
    timeout_s = config.api_preflight_timeout_s
    http = api_client or httpx.Client(timeout=min(_MAX_PROBE_TIMEOUT_S, timeout_s))
    owned = api_client is None
    backoff = BackoffPolicy(
        initial_seconds=config.backoff_initial_seconds,
        max_seconds=config.backoff_max_seconds,
        multiplier=config.backoff_multiplier,
    )
    headers = {"X-API-Key": config.api_key}
    deadline = monotonic() + timeout_s
    attempt = 0
    addresses: set[str] | None = None
    discovery_failure_message = _AGENT_DISCOVERY_FAILURE_MESSAGE
    try:
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break

            try:
                response = http.get(
                    f"{config.api_base_url.rstrip('/')}/agents",
                    headers=headers,
                    timeout=min(_MAX_PROBE_TIMEOUT_S, remaining),
                )
                if response.status_code == 401:
                    raise SlackChannelPreflightError(
                        _AGENT_DISCOVERY_FAILURE_MESSAGE
                    ) from None
                if response.status_code == 200:
                    try:
                        addresses = _collect_slack_addresses(response.json())
                    except Exception:
                        raise SlackChannelPreflightError(
                            _AGENT_DISCOVERY_RESPONSE_SHAPE_MESSAGE
                        ) from None
                    break
                status_class = response.status_code // 100
                discovery_failure_message = (
                    "Slack channel capability preflight failed: platform API "
                    f"agent discovery returned HTTP {status_class}xx after "
                    "bounded retries."
                )
            except SlackChannelPreflightError:
                raise
            except Exception:
                # Provider bodies, exception messages, and request metadata are
                # deliberately discarded at this redaction boundary.
                discovery_failure_message = _AGENT_DISCOVERY_CONNECTIVITY_MESSAGE

            delay = backoff.delay(attempt)
            attempt += 1
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(delay, remaining))
    finally:
        if owned:
            http.close()

    if addresses is None:
        raise SlackChannelPreflightError(discovery_failure_message) from None

    ordered_addresses = sorted(addresses)
    checked = 0
    unverified = 0
    capability_status = "verified"

    remaining = deadline - monotonic()
    if remaining <= 0:
        raise SlackChannelPreflightError(
            _SLACK_CAPABILITY_DEADLINE_MESSAGE
        ) from None

    capability_client = _slack_probe_client(
        config,
        injected=web_client,
        remaining=remaining,
    )
    try:
        capability_response = capability_client.conversations_list(
            types="public_channel",
            exclude_archived=True,
            limit=1,
        )
    except SlackApiError as exc:
        error = _slack_error_code(exc)
        if error in {"missing_scope", "invalid_types"}:
            raise SlackChannelPreflightError(
                _MISSING_CHANNELS_READ_MESSAGE
            ) from None
        capability_status = "unverified"
    except Exception:
        capability_status = "unverified"
    else:
        if not _is_conversations_list_success(capability_response):
            capability_status = "unverified"

    # The second workspace-level scope probe, for the scope that lets an inbound
    # attachment's bytes actually be fetched (#2567). It sits here, beside the
    # `channels:read` probe and before the per-destination loop, because it asks
    # the same kind of question: one bounded call about the token, not about a
    # destination. It is gated on the same deadline for the reason the docstring
    # gives -- an unattempted scope check must refuse rather than read as a pass.
    #
    # It reuses `capability_client` rather than building its own. Per-*destination*
    # clients exist so one hung destination cannot borrow another's share of the
    # budget; these two are the same single workspace-level question asked twice,
    # already bounded by the same `remaining`, so a second construction would buy
    # nothing and would make the number of provider clients a stack opens depend
    # on how many scopes we happen to probe.
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise SlackChannelPreflightError(_SLACK_CAPABILITY_DEADLINE_MESSAGE) from None

    try:
        files_response = capability_client.files_list(count=1)
    except SlackApiError as exc:
        # `missing_scope` is the one provider-terminal answer, and it is
        # unambiguous here in a way it is not on `conversations.info`: this
        # request carries no channel and no type filter, so the only scope it
        # can be missing is `files:read`. That is the difference between a loud
        # boot failure naming the fix and a workspace that silently drops every
        # upload until someone reads the worker's logs.
        if _slack_error_code(exc) == "missing_scope":
            # Definitive, and definitively NOT fatal: this request carries no
            # channel and no type filter, so `files:read` is the only scope it
            # can be missing. Name it and carry on -- see the constant above for
            # why this one does not refuse the boot the way `channels:read` does.
            logger.warning("%s", _MISSING_FILES_READ_MESSAGE)
            files_status = "missing-scope"
        else:
            files_status = "unverified"
    except Exception:
        # Every other outcome stays nondefinitive, exactly as the destination
        # loop's are: a rate limit, a transport fault, an org-token refusal, or
        # an injected seam that predates this probe must not crash-loop a stack
        # whose other capabilities check out. Provider bodies are discarded at
        # this redaction boundary.
        files_status = "unverified"
    else:
        files_status = "verified" if _is_files_list_success(files_response) else "unverified"

    for address in ordered_addresses:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise SlackChannelPreflightError(
                _SLACK_CAPABILITY_DEADLINE_MESSAGE
            ) from None

        probe_client = _slack_probe_client(
            config,
            injected=web_client,
            remaining=remaining,
        )

        try:
            slack_response = probe_client.conversations_info(channel=address)
        except SlackApiError:
            unverified += 1
            continue
        except Exception:
            unverified += 1
            continue

        if not _is_conversations_info_success(slack_response):
            unverified += 1
            continue

        checked += 1

    # A nondefinitive `files:read` probe raises the level without changing the
    # line: the definitive answer already refused above. What is left to say is
    # WHICH check did not come back clean, and each probe therefore names its own
    # status. Reporting only the level, or only the channels probe's status, made a
    # degraded `files:read` probe surface as a warning whose text read clean --
    # an operator debugging a dropped attachment would have found nothing about
    # attachments in it (#2567). The counters stay about destinations.
    log = (
        logger.warning
        if capability_status == "unverified" or files_status != "verified" or unverified
        else logger.info
    )
    log(
        "Slack channel capability preflight public-channel capability %s; "
        "attachment download capability %s; checked "
        "%d configured destinations; unverified %d",
        capability_status,
        files_status,
        checked,
        unverified,
    )
