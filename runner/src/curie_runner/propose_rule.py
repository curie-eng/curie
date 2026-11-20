"""The ``propose_rule`` tool (ADR 0287, #5796): a revision run proposes one learned rule.

A factory run revising a pull request from reviewer feedback may propose one
standing rule for the repository, when that feedback states a general rule
rather than a one-off change. The platform stores it as a hidden candidate; a
person sees it only after the pull request merges and decides whether it
applies.

The model supplies only the rule sentence and why it is general. The API takes
the agent, scope, source request, pull request, comment and person from the
request bound capability, never from the model, so this handler posts exactly
``rule`` and ``reason`` and drops anything else the model passes.

This module owns the schema, the description and the handler and stays free of
the SDK (ADR 0140); `curie_runner.harness.claude.propose_rule_tool` wraps the
handler as the SDK tool. The post, the body decoding and the refusal code
reading are the helpers `curie_runner.tracker_result` already owns.

Nothing here logs or renders the rule, the reason or the capability: a log
line names the outcome only.
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from .tracker_result import (
    _CODE_RE,
    ResultHandler,
    _code,
    _decode,
    _error,
    _ok,
    _post,
    _TooLarge,
)

logger = logging.getLogger(__name__)

_PROPOSAL_TIMEOUT_SECONDS = 30
_PROPOSAL_MAX_BYTES = 16_384

PROPOSE_RULE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "rule": {
            "type": "string",
            "description": "The rule in one sentence, on one line, at most 240 bytes.",
        },
        "reason": {
            "type": "string",
            "description": "Why the feedback states a general rule for this codebase.",
        },
    },
    "required": ["rule", "reason"],
    "additionalProperties": False,
}

PROPOSE_RULE_DESCRIPTION = (
    "Propose one standing rule for this repository, learned from the reviewer "
    "feedback this run is revising. Call it at most once, only when the "
    "feedback states a general rule for this codebase, not a one-off change. "
    "A person who can merge here must accept it before it applies."
)

# What each refusal code tells the model. None of them repeats the rule.
_REFUSALS: dict[str, str] = {
    "rule_proposal_not_eligible": (
        "This run cannot propose a rule. Do not call propose_rule again; continue the revision."
    ),
    "rule_already_proposed": (
        "This run already proposed its one rule. Do not call propose_rule again; "
        "continue the revision."
    ),
    "invalid_context": (
        "This run can no longer propose a rule. Do not call propose_rule again; "
        "continue the revision."
    ),
    "invalid_text": (
        "The rule or reason is not valid text: use one line with no control "
        "characters. Correct it and call again, at most once more."
    ),
    "too_large": (
        "The rule or reason is too long: the rule is at most 240 bytes on one line. "
        "Shorten it and call again, at most once more."
    ),
    "reserved_text": (
        "The rule or reason uses a reserved prefix or a prompt section marker. "
        "Rephrase it in plain words and call again, at most once more."
    ),
    "invalid_body": (
        "The proposal was refused as malformed: pass exactly rule and reason, "
        "both strings. Correct it and call again, at most once more."
    ),
    "invalid_capability": (
        "This run's platform credential was refused, so no rule can be proposed. "
        "Do not call propose_rule again; continue the revision."
    ),
}


def _refusal(status: int, payload: Any) -> dict[str, Any]:
    code = _code(payload)
    if code is not None and not _CODE_RE.match(code):
        code = None
    logger.info("propose_rule refused status=%d code=%s", status, code or "none")
    if code is not None and code in _REFUSALS:
        return _error(f"The rule proposal was refused ({code}). {_REFUSALS[code]}")
    if status in (429, 503) or status >= 500:
        return _error(
            f"The platform could not take the rule proposal right now (status {status}). "
            "Retry once shortly, or continue the revision without it."
        )
    named = f" ({code})" if code is not None else ""
    return _error(
        f"The rule proposal was refused (status {status}){named}. "
        "Do not call propose_rule again; continue the revision."
    )


def propose_rule_handler(url: str, token: str) -> ResultHandler:
    """The handler of ``propose_rule``, closed over the route and capability."""

    async def propose_rule(args: dict[str, Any]) -> dict[str, Any]:
        rule = args.get("rule") if isinstance(args, dict) else None
        reason = args.get("reason") if isinstance(args, dict) else None
        if not isinstance(rule, str) or not isinstance(reason, str):
            logger.info("propose_rule refused locally code=invalid_arguments")
            return _error(
                "The rule was not proposed: pass rule and reason, both strings. "
                "Correct it and call again."
            )
        try:
            # Only the two fields: the API takes everything else from the
            # capability, so nothing else the model passed is sent.
            status, raw = await _post(
                url,
                token,
                {"rule": rule, "reason": reason},
                timeout=_PROPOSAL_TIMEOUT_SECONDS,
                limit=_PROPOSAL_MAX_BYTES,
            )
        except (aiohttp.ClientError, TimeoutError, OSError, _TooLarge) as exc:
            # Never render a transport diagnostic: it can name the endpoint.
            logger.warning("propose_rule transport failure: %s", type(exc).__name__)
            return _error(
                "The rule proposal could not be sent right now. Retry once shortly, "
                "or continue the revision without it."
            )
        try:
            payload = _decode(raw)
        except ValueError:
            payload = None
        if status == 200:
            if isinstance(payload, dict) and payload.get("status") == "candidate":
                logger.info("propose_rule accepted code=candidate")
                return _ok(
                    "The rule was recorded as a candidate. A person who can merge here "
                    "decides after the pull request merges. Do not propose another rule "
                    "in this run; continue the revision."
                )
            logger.info("propose_rule answer unreadable status=200")
            return _error(
                "The rule proposal answer could not be read. Do not call propose_rule "
                "again; continue the revision."
            )
        return _refusal(status, payload)

    return propose_rule
