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
reading are the helpers `curie_runner.completion_contract` already owns.

Nothing here logs or renders the rule, the reason or the capability: a log
line names the outcome only.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import aiohttp

from .completion_contract import (
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


# --- A channel or agent rule for a turn with no work item (#5801) -----------
#
# The same tool name, offered when the factory's is not: the platform ``curie``
# server mounts it beside ``remember`` only when the memory tools mount, the
# session start read reported ``proposals_enabled`` and the run holds no
# ``wir`` capability. It writes with the turn's own memory credential
# (``MemoryTurn.write_token``, ADR 0188), never the boot token, and the API
# derives the agent, the binding, the turn and the person from that credential.

CHANNEL_SCOPES = ("channel", "agent")

CHANNEL_PROPOSE_RULE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "rule": {
            "type": "string",
            "description": "The rule in one sentence, on one line, at most 240 bytes.",
        },
        "reason": {
            "type": "string",
            "description": "Why this is how you should always work from now on.",
        },
        "scope": {
            "type": "string",
            "enum": list(CHANNEL_SCOPES),
            "description": (
                "channel (the default) for this channel only, or agent for every "
                "channel you work in."
            ),
        },
    },
    "required": ["rule", "reason"],
    "additionalProperties": False,
}

CHANNEL_PROPOSE_RULE_DESCRIPTION = (
    "Propose one standing rule for this channel (or, with scope agent, for every "
    "channel you work in) when someone states how you should always work from now "
    "on. A person must accept it before it applies; until then, do not act as if it "
    "applies."
)

_CHANNEL_CONTINUE = "Do not call propose_rule again in this turn; continue the conversation."

# What each refusal code tells the model. None of them repeats the rule.
_CHANNEL_REFUSALS: dict[str, str] = {
    "rule_proposal_not_eligible": (
        f"Rule proposals are turned off for this agent. {_CHANNEL_CONTINUE}"
    ),
    "rule_already_proposed": (f"This turn already proposed its one rule. {_CHANNEL_CONTINUE}"),
    "invalid_text": _REFUSALS["invalid_text"],
    "too_large": _REFUSALS["too_large"],
    "reserved_text": _REFUSALS["reserved_text"],
    "invalid_body": (
        "The proposal was refused as malformed: pass rule and reason as strings and, "
        "if you pass it, scope as channel or agent. Correct it and call again, at most "
        "once more."
    ),
}

_NO_TURN_CREDENTIAL = (
    "The rule was not proposed: this turn has no credential to propose with. "
    "Tell the person it was not recorded. " + _CHANNEL_CONTINUE
)


def _channel_refusal(status: int, payload: Any) -> dict[str, Any]:
    code = _code(payload)
    if code is not None and not _CODE_RE.match(code):
        code = None
    logger.info("propose_rule refused status=%d code=%s", status, code or "none")
    if code is not None and code in _CHANNEL_REFUSALS:
        return _error(f"The rule proposal was refused ({code}). {_CHANNEL_REFUSALS[code]}")
    if status in (429, 503) or status >= 500:
        return _error(
            f"The platform could not take the rule proposal right now (status {status}). "
            "Retry once shortly, or tell the person it was not recorded."
        )
    named = f" ({code})" if code is not None else ""
    # A plain refusal (a read only or ended turn credential) is never rendered:
    # the platform's detail is not the model's to repeat.
    return _error(
        f"The rule proposal was refused (status {status}){named}. "
        "Tell the person it was not recorded. " + _CHANNEL_CONTINUE
    )


async def _post_with_key(
    url: str, token: str, body: dict[str, Any], *, timeout: float, limit: int
) -> tuple[int, bytes]:
    async with (
        aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout), trust_env=False
        ) as session,
        session.post(
            url, json=body, headers={"X-API-Key": token}, allow_redirects=False
        ) as response,
    ):
        raw = bytearray()
        async for chunk in response.content.iter_any():
            raw.extend(chunk)
            if len(raw) > limit:
                raise _TooLarge
        return response.status, bytes(raw)


def channel_propose_rule_handler(url: str, turn_token: Callable[[], str | None]) -> ResultHandler:
    """The handler of the channel ``propose_rule``, posting to ``state/_rules``.

    ``turn_token`` returns the current turn's memory write credential; with none
    the call is refused here and nothing is sent.
    """

    async def propose_rule(args: dict[str, Any]) -> dict[str, Any]:
        rule = args.get("rule") if isinstance(args, dict) else None
        reason = args.get("reason") if isinstance(args, dict) else None
        scope = args.get("scope", "channel") if isinstance(args, dict) else None
        if not isinstance(rule, str) or not isinstance(reason, str):
            logger.info("propose_rule refused locally code=invalid_arguments")
            return _error(
                "The rule was not proposed: pass rule and reason, both strings. "
                "Correct it and call again."
            )
        if scope not in CHANNEL_SCOPES:
            logger.info("propose_rule refused locally code=invalid_scope")
            return _error(
                "The rule was not proposed: scope is channel or agent. Correct it and call again."
            )
        token = turn_token()
        if not token:
            logger.info("propose_rule refused locally code=no_turn_credential")
            return _error(_NO_TURN_CREDENTIAL)
        try:
            # Exactly the three fields: the API derives everything else from
            # the turn credential, so nothing else the model passed is sent.
            status, raw = await _post_with_key(
                url,
                token,
                {"rule": rule, "reason": reason, "scope": scope},
                timeout=_PROPOSAL_TIMEOUT_SECONDS,
                limit=_PROPOSAL_MAX_BYTES,
            )
        except (aiohttp.ClientError, TimeoutError, OSError, _TooLarge) as exc:
            # Never render a transport diagnostic: it can name the endpoint.
            logger.warning("propose_rule transport failure: %s", type(exc).__name__)
            return _error(
                "The rule proposal could not be sent right now. Retry once shortly, "
                "or tell the person it was not recorded."
            )
        try:
            payload = _decode(raw)
        except ValueError:
            payload = None
        if status == 201:
            if isinstance(payload, dict) and payload.get("state") == "proposed":
                logger.info("propose_rule accepted code=proposed scope=%s", scope)
                where = "every channel you work in" if scope == "agent" else "this channel"
                return _ok(
                    f"The rule was proposed for {where}. A person must accept it before "
                    "it applies, and an accepted rule applies from the next session; "
                    "until then, do not act as if it applies. Tell the person it awaits "
                    "acceptance. " + _CHANNEL_CONTINUE
                )
            logger.info("propose_rule answer unreadable status=201")
            return _error("The rule proposal answer could not be read. " + _CHANNEL_CONTINUE)
        return _channel_refusal(status, payload)

    return propose_rule
