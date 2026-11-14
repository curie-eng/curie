"""The completion contract and the ``submit_tracker_result`` tool (ADR 0277, #5586).

A factory execution learns how it must end from the API, through the same
issue read route and capability the worker already injects (ADR 0187). Two
sibling routes of ``/work-items/issue-read`` carry it:

1. ``/work-items/completion-contract``: the runner posts an empty body once,
   at startup, and learns whether this request ends in an implementation (a
   pull request, everything as before) or a tracker result (a published
   ticket or a findings comment). For a tracker result that first call also
   reserves the request's one model session; any later call is refused
   ``session_already_started``.
2. ``/work-items/tracker-result``: the ``submit_tracker_result`` tool posts
   the run's one result proposal there.

The contract is a security boundary, so discovering it fails closed: a route
that is not the expected sibling, a transport failure after the bounded
attempts, any non-200 answer, an unparseable or oversized body, an unknown
contract, an invalid result kind list or allowance, the internal deadline, and
any unexpected exception all raise `ContractUnavailable`, which carries a fixed
reason code and nothing else (never the capability, the URL or a body). The
runner then answers offline and starts no model.

With no capability injected (local, eval and fake-model use) nothing here is
called and the session is an implementation session, unchanged.
"""

from __future__ import annotations

import json
import logging
import math
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import aiohttp
import anyio

from .issue_read import CAPABILITY_HEADER
from .requester_context import ExecutionBinding

logger = logging.getLogger(__name__)


ContractKind = Literal["implementation", "tracker_result"]
CONTRACT_IMPLEMENTATION: ContractKind = "implementation"
CONTRACT_TRACKER_RESULT: ContractKind = "tracker_result"
RESULT_KINDS = frozenset({"ticket", "findings"})

# The built-in tools a tracker result session may not use: a subagent would
# be a second model loop outside the one reserved session and its allowance.
TRACKER_RESULT_DISALLOWED_TOOLS: tuple[str, ...] = ("Task", "Agent")

ISSUE_READ_SEGMENT = "issue-read"
_ISSUE_READ_SUFFIX = "/work-items/" + ISSUE_READ_SEGMENT
_CONTRACT_SEGMENT = "completion-contract"
_RESULT_SEGMENT = "tracker-result"

# ``ContractUnavailable.reason`` codes. Fixed strings only.
REASON_ROUTE_INVALID = "route_invalid"
REASON_TRANSPORT_FAILED = "transport_failed"
REASON_STATUS_REFUSED = "status_refused"
REASON_STATUS_TRANSIENT = "status_transient"
REASON_SESSION_ALREADY_STARTED = "session_already_started"
REASON_BODY_INVALID = "body_invalid"
REASON_CONTRACT_UNKNOWN = "contract_unknown"
REASON_RESULT_KINDS_INVALID = "result_kinds_invalid"
REASON_ALLOWANCE_INVALID = "allowance_invalid"
REASON_DEADLINE_EXCEEDED = "deadline_exceeded"
REASON_INTERNAL_ERROR = "internal_error"
CONTRACT_REASONS = frozenset(
    {
        REASON_ROUTE_INVALID,
        REASON_TRANSPORT_FAILED,
        REASON_STATUS_REFUSED,
        REASON_STATUS_TRANSIENT,
        REASON_SESSION_ALREADY_STARTED,
        REASON_BODY_INVALID,
        REASON_CONTRACT_UNKNOWN,
        REASON_RESULT_KINDS_INVALID,
        REASON_ALLOWANCE_INVALID,
        REASON_DEADLINE_EXCEEDED,
        REASON_INTERNAL_ERROR,
    }
)

# Bounds on the contract call: three attempts at most, each under 15 seconds,
# with 1 and 2 seconds between transient failures (transport errors, timeouts,
# HTTP 5xx and 429), all inside a 50 second
# deadline (under the 60 second ceiling every gate here keeps).
CONTRACT_ATTEMPTS = 3
CONTRACT_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0)
CONTRACT_DEADLINE_SECONDS = 50.0
_ATTEMPT_TIMEOUT_SECONDS = 15
_CONTRACT_MAX_BYTES = 65_536
_RESULT_TIMEOUT_SECONDS = 45
_RESULT_MAX_BYTES = 65_536

# The answer the offline session gives when the contract cannot be confirmed,
# and when a tracker result process is asked for a second model session.
CONTRACT_BLOCKED_EXPLANATION = (
    "Could not complete: the platform could not confirm how this factory run "
    "must end, so no model round ran."
)
SECOND_SESSION_EXPLANATION = (
    "Could not complete: this triage run already used its one model session, "
    "so no further model round ran."
)

Delay = Callable[[float], Awaitable[None]]


class ContractUnavailable(Exception):
    """The completion contract could not be confirmed; carries a reason code only."""

    def __init__(self, reason: str) -> None:
        if reason not in CONTRACT_REASONS:
            reason = REASON_INTERNAL_ERROR
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CompletionContract:
    """How this execution must end, as the API answered it."""

    kind: ContractKind
    result_kinds: tuple[str, ...] = ()
    session_usd_allowance: float | None = None
    # ADR 0278 (#5592): the execution this process is bound to. Present, it
    # makes the start wait for the worker's requester context envelope.
    execution: ExecutionBinding | None = None

    @property
    def tracker_result(self) -> bool:
        return self.kind == CONTRACT_TRACKER_RESULT


IMPLEMENTATION = CompletionContract(CONTRACT_IMPLEMENTATION)


@dataclass(frozen=True)
class SiblingUrls:
    completion_contract: str
    tracker_result: str


@dataclass(frozen=True)
class SessionLimits:
    """The SDK controls a contract sets on the model session."""

    disallowed_tools: tuple[str, ...]
    max_budget_usd: float | None


ResultHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class BoundContract:
    """The discovered contract and, for a tracker result, its submission tool."""

    contract: CompletionContract
    # The plain async handler of ``submit_tracker_result``; the Claude harness
    # wraps it as an SDK tool (harness.claude.tracker_result_tool), so this
    # module stays free of the SDK (ADR 0140).
    result_handler: ResultHandler | None = None


def sibling_urls(issue_read_url: str) -> SiblingUrls:
    """The contract and result routes beside ``issue_read_url``.

    The URL must be an http(s) URL whose path ends with
    ``/work-items/issue-read`` and that carries no query, fragment or
    credentials; anything else is a contract failure.
    """

    try:
        parts = urlsplit(issue_read_url.strip())
        valid = (
            parts.scheme in {"http", "https"}
            and bool(parts.hostname)
            and parts.username is None
            and parts.password is None
            and not parts.query
            and not parts.fragment
            and parts.path.endswith(_ISSUE_READ_SUFFIX)
        )
    except ValueError:
        valid = False
    if not valid:
        raise ContractUnavailable(REASON_ROUTE_INVALID)
    base = parts.path[: -len(ISSUE_READ_SEGMENT)]

    def at(segment: str) -> str:
        return urlunsplit((parts.scheme, parts.netloc, base + segment, "", ""))

    return SiblingUrls(at(_CONTRACT_SEGMENT), at(_RESULT_SEGMENT))


def session_limits(
    contract: CompletionContract,
    *,
    disallowed_tools: tuple[str, ...],
    max_budget_usd: float | None,
) -> SessionLimits:
    """The disallowed tools and dollar ceiling for a session under ``contract``.

    An implementation session keeps the configured values. A tracker result
    session also denies ``Task`` and ``Agent`` and spends at most the API's
    allowance.
    """

    if not contract.tracker_result:
        return SessionLimits(tuple(disallowed_tools), max_budget_usd)
    extra = tuple(name for name in TRACKER_RESULT_DISALLOWED_TOOLS if name not in disallowed_tools)
    return SessionLimits((*disallowed_tools, *extra), contract.session_usd_allowance)


class SingleModelSession:
    """Admits one model session per process for a tracker result contract.

    The API reservation refuses a second process; this refuses a second
    session inside one process (``reset`` and the session replacement both
    call the session factory again).
    """

    def __init__(self) -> None:
        self._admitted = False

    def admit(self) -> bool:
        if self._admitted:
            return False
        self._admitted = True
        return True


def _code(payload: Any) -> str | None:
    """The refusal ``code`` of an API answer, bare or under FastAPI's ``detail``."""

    if not isinstance(payload, dict):
        return None
    detail = payload.get("detail")
    source = detail if isinstance(detail, dict) else payload
    code = source.get("code")
    return code if isinstance(code, str) else None


class _TooLarge(Exception):
    """A response body past its bound."""


async def _post(
    url: str, token: str, body: dict[str, Any], *, timeout: float, limit: int
) -> tuple[int, bytes]:
    async with (
        aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout), trust_env=False
        ) as session,
        session.post(
            url,
            json=body,
            headers={CAPABILITY_HEADER: token},
            allow_redirects=False,
        ) as response,
    ):
        raw = bytearray()
        async for chunk in response.content.iter_any():
            raw.extend(chunk)
            if len(raw) > limit:
                raise _TooLarge
        return response.status, bytes(raw)


def _decode(raw: bytes) -> Any:
    return json.loads(raw)


def parse_contract(payload: Any) -> CompletionContract:
    """The typed contract from a 200 answer; any other shape is a contract failure."""

    if not isinstance(payload, dict):
        raise ContractUnavailable(REASON_BODY_INVALID)
    kind = payload.get("completion_contract")
    execution = _parse_execution(payload.get("execution"))
    if kind == CONTRACT_IMPLEMENTATION:
        return (
            IMPLEMENTATION
            if execution is None
            else CompletionContract(CONTRACT_IMPLEMENTATION, execution=execution)
        )
    if kind != CONTRACT_TRACKER_RESULT:
        raise ContractUnavailable(REASON_CONTRACT_UNKNOWN)
    kinds = payload.get("result_kinds")
    if (
        not isinstance(kinds, list)
        or not kinds
        or not all(isinstance(item, str) and item in RESULT_KINDS for item in kinds)
        or len(set(kinds)) != len(kinds)
    ):
        raise ContractUnavailable(REASON_RESULT_KINDS_INVALID)
    allowance = payload.get("session_usd_allowance")
    if (
        isinstance(allowance, bool)
        or not isinstance(allowance, int | float)
        or not math.isfinite(allowance)
        or allowance <= 0
    ):
        raise ContractUnavailable(REASON_ALLOWANCE_INVALID)
    return CompletionContract(
        CONTRACT_TRACKER_RESULT,
        result_kinds=tuple(sorted(kinds)),
        session_usd_allowance=float(allowance),
        execution=execution,
    )


def _parse_execution(value: Any) -> ExecutionBinding | None:
    """The contract's execution binding; a malformed one is a contract failure."""

    if value is None:
        return None
    if not isinstance(value, dict):
        raise ContractUnavailable(REASON_BODY_INVALID)
    ids = [value.get(key) for key in ("tenant_id", "work_item_id", "execution_request_id")]
    fence = value.get("execution_fence")
    if (
        not all(isinstance(item, str) and _is_uuid(item) for item in ids)
        or type(fence) is not int
        or fence < 1
    ):
        raise ContractUnavailable(REASON_BODY_INVALID)
    tenant, work_item, request = (str(item) for item in ids)
    return ExecutionBinding(
        tenant_id=tenant,
        work_item_id=work_item,
        execution_request_id=request,
        execution_fence=fence,
    )


def _is_uuid(value: str) -> bool:
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


async def _fetch(url: str, token: str, delay: Delay) -> CompletionContract:
    for attempt in range(CONTRACT_ATTEMPTS):
        try:
            status, raw = await _post(
                url, token, {}, timeout=_ATTEMPT_TIMEOUT_SECONDS, limit=_CONTRACT_MAX_BYTES
            )
        except _TooLarge:
            raise ContractUnavailable(REASON_BODY_INVALID) from None
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            # The class only: a transport diagnostic can name the endpoint.
            logger.info(
                "completion contract attempt %d failed error_class=%s",
                attempt + 1,
                type(exc).__name__,
            )
            if attempt + 1 >= CONTRACT_ATTEMPTS:
                raise ContractUnavailable(REASON_TRANSPORT_FAILED) from None
            await delay(CONTRACT_RETRY_DELAYS[min(attempt, len(CONTRACT_RETRY_DELAYS) - 1)])
            continue
        # A transient HTTP answer (5xx or 429) is retried like a transport
        # failure; every other answer is final.
        if status == 429 or status >= 500:
            logger.info("completion contract attempt %d answered transient status", attempt + 1)
            if attempt + 1 >= CONTRACT_ATTEMPTS:
                raise ContractUnavailable(REASON_STATUS_TRANSIENT)
            await delay(CONTRACT_RETRY_DELAYS[min(attempt, len(CONTRACT_RETRY_DELAYS) - 1)])
            continue
        try:
            payload = _decode(raw)
        except ValueError:
            payload = None
        if status == 200:
            if payload is None:
                raise ContractUnavailable(REASON_BODY_INVALID)
            return parse_contract(payload)
        if status == 409 and _code(payload) == REASON_SESSION_ALREADY_STARTED:
            raise ContractUnavailable(REASON_SESSION_ALREADY_STARTED)
        raise ContractUnavailable(REASON_STATUS_REFUSED)
    raise ContractUnavailable(REASON_TRANSPORT_FAILED)


async def fetch_completion_contract(
    url: str,
    token: str,
    *,
    delay: Delay = anyio.sleep,
    deadline_s: float = CONTRACT_DEADLINE_SECONDS,
) -> CompletionContract:
    """Post ``{}`` to the completion contract route and return the contract.

    ``url`` is the ``completion-contract`` route (see `sibling_urls`). Raises
    `ContractUnavailable` on every failure, including its own unexpected
    exceptions and the internal deadline.
    """

    try:
        with anyio.fail_after(deadline_s):
            return await _fetch(url, token, delay)
    except ContractUnavailable:
        raise
    except TimeoutError:
        raise ContractUnavailable(REASON_DEADLINE_EXCEEDED) from None
    except Exception:  # noqa: BLE001 - the gate denies on its own failure
        raise ContractUnavailable(REASON_INTERNAL_ERROR) from None


async def bind_completion_contract(
    issue_read: tuple[str, str] | None,
    *,
    delay: Delay = anyio.sleep,
    deadline_s: float = CONTRACT_DEADLINE_SECONDS,
) -> BoundContract:
    """Discover this execution's contract from the injected issue read capability.

    No capability means an implementation session and no call at all. With
    one, the contract route is called once; a tracker result contract comes
    back with its ``submit_tracker_result`` tool.
    """

    if issue_read is None:
        return BoundContract(IMPLEMENTATION)
    url, token = issue_read
    urls = sibling_urls(url)
    contract = await fetch_completion_contract(
        urls.completion_contract, token, delay=delay, deadline_s=deadline_s
    )
    if not contract.tracker_result:
        return BoundContract(contract)
    return BoundContract(contract, tracker_result_handler(urls.tracker_result, token))


# --- The submission tool -------------------------------------------------------

_TEXT = {"type": "string"}
_REVISION = {
    "type": "string",
    "description": "The investigated commit: 40 lowercase hex characters.",
}

TRACKER_RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "kind": {
            "type": "string",
            "enum": ["ticket", "findings"],
            "description": "ticket publishes a new implementation ticket; findings comments "
            "on the source ticket.",
        },
        "title": {**_TEXT, "description": "ticket only: 1 to 256 characters."},
        "body": {**_TEXT, "description": "ticket only: 1 to 60,000 characters."},
        "reproduction": {
            "type": "object",
            "description": "ticket only: how the problem was reproduced.",
            "properties": {
                "command": {**_TEXT, "description": "1 to 2,000 characters."},
                "observed": {**_TEXT, "description": "1 to 4,000 characters."},
                "revision": _REVISION,
            },
            "required": ["command", "observed", "revision"],
            "additionalProperties": False,
        },
        "reason": {
            "type": "string",
            "enum": [
                "not_reproduced",
                "already_fixed",
                "product_decision_needed",
                "capability_unavailable",
            ],
            "description": "findings only.",
        },
        "tried": {**_TEXT, "description": "findings only: 1 to 4,000 characters."},
        "missing": {**_TEXT, "description": "findings only: 1 to 4,000 characters."},
        "revision": {
            **_REVISION,
            "description": "findings only, optional: the investigated commit, "
            "40 lowercase hex characters.",
        },
    },
    "required": ["kind"],
    "additionalProperties": False,
}

TRACKER_RESULT_DESCRIPTION = (
    "Submit this run's one platform result. This run ends in a tracker result, "
    "not a pull request. Submit either a ticket (kind ticket, with title, body "
    "and reproduction: command, observed and revision) when you reproduced a "
    "problem that needs implementation, or findings (kind findings, with "
    "reason, tried, missing and an optional revision) when you did not. The "
    "first accepted submission is final: a later one is refused, so submit "
    "once, when the result is complete, then end your turn."
)

_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_FIELD_RE = re.compile(r"^[a-z0-9_.*]{0,64}$")
_MAX_ISSUES = 20


def _error(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def _ok(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _url(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 2_048:
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return None
    if parts.username is not None or parts.password is not None:
        return None
    return value


def _links(payload: dict[str, Any]) -> str:
    links = []
    ticket = _url(payload.get("ticket_url"))
    comment = _url(payload.get("result_comment_url"))
    if ticket is not None:
        links.append(f"Ticket: {ticket}")
    if comment is not None:
        links.append(f"Result comment: {comment}")
    return (" " + " ".join(links)) if links else ""


def _render_answer(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return _error("The result submission answer could not be read. Retry shortly.")
    kind = payload.get("result_kind")
    kind_text = kind if kind in RESULT_KINDS else "result"
    state = payload.get("state")
    if state == "delivered":
        return _ok(
            f"The {kind_text} result was accepted and delivered.{_links(payload)} "
            "This is the final result for this run; do not submit again."
        )
    if state == "owed":
        return _ok(
            f"The {kind_text} result was accepted; the platform will finish "
            "delivering it. This is the final result for this run; do not submit again."
        )
    if state == "failed":
        return _error(
            f"The {kind_text} result was accepted but its delivery failed; the "
            "platform records the failure. Do not submit again."
        )
    return _error("The result submission answer could not be read. Retry shortly.")


def _render_issues(payload: Any) -> str:
    detail = payload.get("detail") if isinstance(payload, dict) else None
    source = detail if isinstance(detail, dict) else payload
    issues = source.get("issues") if isinstance(source, dict) else None
    pairs: list[str] = []
    for issue in issues if isinstance(issues, list) else ():
        if not isinstance(issue, dict):
            continue
        field = issue.get("field")
        code = issue.get("code")
        if not isinstance(field, str) or not isinstance(code, str):
            continue
        if not _FIELD_RE.match(field) or not _CODE_RE.match(code):
            continue
        pairs.append(f"{field or '(top level)'}: {code}")
        if len(pairs) >= _MAX_ISSUES:
            break
    return "; ".join(pairs)


def tracker_result_handler(url: str, token: str) -> ResultHandler:
    """The handler of ``submit_tracker_result``, closed over the route and capability."""

    async def submit_tracker_result(args: dict[str, Any]) -> dict[str, Any]:
        try:
            status, raw = await _post(
                url,
                token,
                {"proposal": args},
                timeout=_RESULT_TIMEOUT_SECONDS,
                limit=_RESULT_MAX_BYTES,
            )
        except (aiohttp.ClientError, TimeoutError, OSError, _TooLarge) as exc:
            # Never render a transport diagnostic: it can name the endpoint.
            logger.warning("tracker result transport failure: %s", type(exc).__name__)
            return _error("The result could not be submitted right now. Retry shortly.")
        try:
            payload = _decode(raw)
        except ValueError:
            payload = None
        if status == 200:
            return _render_answer(payload)
        if status == 422:
            issues = _render_issues(payload)
            return _error(
                "The result was refused as invalid"
                + (f": {issues}." if issues else ".")
                + " Correct it and submit again."
            )
        if status == 409:
            code = _code(payload)
            if code is not None and _CODE_RE.match(code):
                return _error(f"The result was refused: {code}.")
            return _error("The result was refused.")
        if status == 503:
            return _error("The tracker could not take the result right now. Retry shortly.")
        return _error(f"The result was refused (status {status}).")

    return submit_tracker_result
