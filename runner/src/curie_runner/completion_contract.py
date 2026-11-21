"""The completion contract: how a factory execution must end (ADR 0277, #5586).

A factory execution learns how it must end from the API, through the same
issue read route and capability the worker already injects (ADR 0187). The
sibling route ``/work-items/completion-contract`` carries it: the runner posts
an empty body once, at startup, and learns whether this request ends in an
implementation (a pull request) or a publication recovery (#5429: the process
only validates a retained publication candidate and runs no model).

The contract answer also says whether this run may propose a learned rule
(ADR 0287, #5796): ``rule_proposal`` is a permanent field of this parser, read
as true only when the answer carries exactly JSON ``true``. Absent or any
other value reads false, so a runner newer than its API mounts nothing and a
malformed value never blocks the run. True mounts ``propose_rule``
(`curie_runner.propose_rule`), which posts to the sibling
``/work-items/rule-proposal``.

The contract is a security boundary, so discovering it fails closed: a route
that is not the expected sibling, a transport failure after the bounded
attempts, any non-200 answer, an unparseable or oversized body, an unknown
contract (a removed contract value included), the internal deadline, and any
unexpected exception all raise `ContractUnavailable`, which carries a fixed
reason code and nothing else (never the capability, the URL or a body). The
runner then answers offline and starts no model.

With no capability injected (local, eval and fake-model use) nothing here is
called and the session is an implementation session, unchanged.
"""

from __future__ import annotations

import json
import logging
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


ContractKind = Literal["implementation", "publication_recovery"]
CONTRACT_IMPLEMENTATION: ContractKind = "implementation"
CONTRACT_PUBLICATION_RECOVERY: ContractKind = "publication_recovery"

ISSUE_READ_SEGMENT = "issue-read"
_ISSUE_READ_SUFFIX = "/work-items/" + ISSUE_READ_SEGMENT
_CONTRACT_SEGMENT = "completion-contract"
_QUESTIONS_SEGMENT = "questions"
_RULE_PROPOSAL_SEGMENT = "rule-proposal"

# ``ContractUnavailable.reason`` codes. Fixed strings only.
REASON_ROUTE_INVALID = "route_invalid"
REASON_TRANSPORT_FAILED = "transport_failed"
REASON_STATUS_REFUSED = "status_refused"
REASON_STATUS_TRANSIENT = "status_transient"
REASON_BODY_INVALID = "body_invalid"
REASON_CONTRACT_UNKNOWN = "contract_unknown"
REASON_DEADLINE_EXCEEDED = "deadline_exceeded"
REASON_INTERNAL_ERROR = "internal_error"
CONTRACT_REASONS = frozenset(
    {
        REASON_ROUTE_INVALID,
        REASON_TRANSPORT_FAILED,
        REASON_STATUS_REFUSED,
        REASON_STATUS_TRANSIENT,
        REASON_BODY_INVALID,
        REASON_CONTRACT_UNKNOWN,
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

# The answer the offline session gives when the contract cannot be confirmed.
CONTRACT_BLOCKED_EXPLANATION = (
    "Could not complete: the platform could not confirm how this factory run "
    "must end, so no model round ran."
)
# The answer every turn gets in a publication recovery process (#5429): the
# process exists to validate a retained candidate, never to run a model.
PUBLICATION_RECOVERY_EXPLANATION = (
    "Could not complete: this process only validates a retained publication "
    "candidate and runs no model, so no model round ran."
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
    # ADR 0278 (#5592): the execution this process is bound to. Present, it
    # makes the start wait for the worker's requester context envelope.
    execution: ExecutionBinding | None = None
    # ADR 0287 (#5796): the API offers this run the ``propose_rule`` tool.
    # A permanent field: later narrowing of the contract keeps it.
    rule_proposal: bool = False

    @property
    def publication_recovery(self) -> bool:
        return self.kind == CONTRACT_PUBLICATION_RECOVERY


IMPLEMENTATION = CompletionContract(CONTRACT_IMPLEMENTATION)
PUBLICATION_RECOVERY = CompletionContract(CONTRACT_PUBLICATION_RECOVERY)


@dataclass(frozen=True)
class SiblingUrls:
    completion_contract: str
    # The ``ask_questions`` route (#5785), beside the issue read route.
    questions: str
    # The ``propose_rule`` route (ADR 0287, #5796), beside the issue read route.
    rule_proposal: str


ResultHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class BoundContract:
    """The discovered contract and the platform tool handlers it offers.

    The handlers are plain async functions; the Claude harness wraps them as
    SDK tools, so this module stays free of the SDK (ADR 0140).
    """

    contract: CompletionContract
    # The plain async handler of ``ask_questions`` (#5785), set only for an
    # implementation contract the API answered. Never for a publication
    # recovery.
    question_handler: ResultHandler | None = None
    # The plain async handler of ``propose_rule`` (ADR 0287, #5796), set only
    # when the API offered it on an implementation contract and the session
    # is not the fake model.
    rule_handler: ResultHandler | None = None


def sibling_urls(issue_read_url: str) -> SiblingUrls:
    """The contract and tool routes beside ``issue_read_url``.

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

    return SiblingUrls(
        at(_CONTRACT_SEGMENT),
        at(_QUESTIONS_SEGMENT),
        at(_RULE_PROPOSAL_SEGMENT),
    )


_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _error(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def _ok(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


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
    if kind == CONTRACT_PUBLICATION_RECOVERY:
        return _parse_publication_recovery(payload)
    if kind != CONTRACT_IMPLEMENTATION:
        # Checked before the execution binding so any other contract value,
        # a removed one included, is always an unknown contract and never a
        # body error. Nothing maps a removed value to an implementation.
        raise ContractUnavailable(REASON_CONTRACT_UNKNOWN)
    execution = _parse_execution(payload.get("execution"))
    # Exactly JSON true offers the tool; anything else, absent included, is
    # false and never a contract failure.
    rule_proposal = payload.get("rule_proposal") is True
    return (
        IMPLEMENTATION
        if execution is None and not rule_proposal
        else CompletionContract(
            CONTRACT_IMPLEMENTATION, execution=execution, rule_proposal=rule_proposal
        )
    )


def _parse_publication_recovery(payload: dict[str, Any]) -> CompletionContract:
    """A publication recovery answer carries no execution binding and no rule
    proposal offer. Either present is a contract failure, since the API never
    sends one for this kind. ``rule_proposal: false`` is the API's answer for
    this kind (#5796) and parses, as an absent or null field does; any other
    value of it is refused."""

    if payload.get("execution") is not None:
        raise ContractUnavailable(REASON_BODY_INVALID)
    offer = payload.get("rule_proposal")
    if offer is not None and offer is not False:
        raise ContractUnavailable(REASON_BODY_INVALID)
    return PUBLICATION_RECOVERY


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
    fake_model: bool = False,
    delay: Delay = anyio.sleep,
    deadline_s: float = CONTRACT_DEADLINE_SECONDS,
) -> BoundContract:
    """Discover this execution's contract from the injected issue read capability.

    No capability means an implementation session and no call at all. With
    one, the contract route is called once; an implementation contract comes
    back with the ``ask_questions`` tool (#5785), and a publication recovery
    contract with nothing (the process then constructs no model session).
    An implementation contract that offers a rule proposal also comes back
    with the ``propose_rule`` handler (ADR 0287, #5796), except for the fake
    model (``fake_model``), which never gets one.
    """

    if issue_read is None:
        return BoundContract(IMPLEMENTATION)
    url, token = issue_read
    urls = sibling_urls(url)
    contract = await fetch_completion_contract(
        urls.completion_contract, token, delay=delay, deadline_s=deadline_s
    )
    rule_handler: ResultHandler | None = None
    if contract.rule_proposal and not fake_model:
        # Imported here because ``propose_rule`` builds on this module's
        # shared post and refusal helpers.
        from .propose_rule import propose_rule_handler

        rule_handler = propose_rule_handler(urls.rule_proposal, token)
    if contract.kind != CONTRACT_IMPLEMENTATION:
        return BoundContract(contract)
    # Imported here because ``question_tool`` builds on this module's shared
    # post and refusal helpers.
    from .question_tool import questions_handler

    return BoundContract(
        contract,
        question_handler=questions_handler(urls.questions, token),
        rule_handler=rule_handler,
    )
