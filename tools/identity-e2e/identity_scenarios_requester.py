"""ADR 0270 slice 2 requester scenarios for `curie dev identity-e2e -- prove`.

1. ``requester-effect`` (acceptance item 5, decisions 4 and 5): the reference
   human-restricted connector applies an effect only for the verified human
   the caller proxy attests, and only while the provider's current authority
   allows it. Needs ``up --reference-connector``.
2. ``requester-expiry`` (items 3 and 4): a copied requester token stays usable
   until its signed expiry, even after handoff, and is refused after it; a warm
   route whose caller token is too short is replaced; a thread idle past the
   hour gets a fresh token. Waits about an hour of real time.
3. ``requester-approval`` (item 4): an approval a human requested and another
   human answered by a signed email answer resumes on a fresh claim whose
   caller token still names the requester.

Registered by ``identity_scenarios`` on import. The same rules hold as there:
every step drives the install through its own boundaries, a caller token stays
in memory and only its fingerprint and ``exp`` reach the evidence, and stored
state no product surface can write is changed only to probe it and restored.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from identity_assets import role_filter
from identity_e2e import (
    ADMIN_AUTHORITY,
    ADMIN_MEMBERSHIP,
    AUTHORITY_GROUP,
    REFERENCE_CONNECTOR,
    Harness,
    HarnessError,
    caller_fingerprint,
    decode_caller_token,
    epoch,
    principal_fingerprint,
    principal_header_value,
    proxied_connector,
)
from identity_pod_scripts import ANSWER_SIGN_SCRIPT
from identity_scenarios import (
    CLOCK_SKEW_S,
    CONNECTOR_CALL,
    LOG_MESSAGE_LIMIT,
    Evidence,
    accepted_event_id,
    await_turns,
    check,
    conversation_turn,
    default_agents,
    judge_conversation_turn,
    observed_token,
    principal_for,
    require_connector_admitted,
    require_delivered,
    require_install_ready,
    require_passed,
    require_principal,
    require_replaced,
    require_reply_fingerprint,
    scenario,
    send_summary,
    set_requester_flag,
    two_agents,
)
from identity_target import ADAPTER, ROLE_FILTER_SCRIPT, http, log, summarize_event

EFFECT_TOOL = "record_effect"
OK = "ok"
NOT_AUTHORIZED = "not_authorized"
AUTHORITY_UNAVAILABLE = "authority_unavailable"
MISSING_REQUESTER = "missing_requester"
EXPIRED = "expired"
# ADR 0270 decision 3: a requester cct lives at most an hour from its mint.
REQUESTER_TOKEN_TTL_S = 3600
# X6 replays the captured token this long after its signed expiry.
EXPIRY_MARGIN_S = 5
# How often a long real-time wait reports on stderr.
WAIT_PROGRESS_S = 240.0
# X5: the doctored caller_token_exp sits this far past now: unexpired, yet far
# short of what any turn needs (its bound plus the boot grace).
SHORT_CALLER_WINDOW_S = 120
# How long a connector's outcome line may trail the reply that reported it.
EFFECT_LOG_WAIT_S = 15.0
# requester-approval's route: the card in the asking email thread, answerable
# only by the listed approver address (ADR-0177 amendment), whom the agent's
# approver role must also admit (ADR 0267).
APPROVAL_ROUTE = "identity-e2e-approve"
APPROVAL_MARKER = "[fake:request-approval:"
ADAPTER_PRINCIPAL_HEADER = "X-Curie-Adapter-Principal"
APPROVAL_ACTOR_HEADER = "X-Curie-Approval-Actor"
APPROVAL_PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"
HTTP_ANSWER_AUTHORIZER = "HttpApprovalAnswer"

_TOOL_LINE = re.compile(
    r"\bconnector-tool(?: (?P<connector>[^\s/:]+)(?:/(?P<tool>[^\s:]+))?)?: "
    r"(?P<outcome>ok|refused|failed)\b(?P<rest>[^\n]*)"
)
_EFFECT = re.compile(r"(?:^|\s)effect=(?P<effect>\d+)(?:\s|$)")
_REQUESTER = re.compile(r"(?:^|\s)requester=(?P<requester>\S+)")
_CALLER = re.compile(r"(?:^|\s)caller=(?P<caller>\S+)")
_BOUNDED = re.compile(
    r"bounded to (?P<bound>\d+)s by the requester token window "
    r"\(configured bound (?P<configured>\d+)s\)"
)


# --- pure checks --------------------------------------------------------------------


def effect_directive(arguments: Mapping[str, Any], *, forge: str | None = None) -> str:
    """The fake model's directive to call ``record_effect`` once with ``arguments``.

    ``forge`` adds ``[fake:forge-principal <forge>]``, a literal
    ``X-Curie-Principal`` the runner sends on that dial only. It comes first so
    the tool directive's JSON stays the last bracketed object on the line.
    """

    payload = json.dumps(dict(arguments), separators=(",", ":"), sort_keys=True)
    directive = f"[fake:connector-tool {REFERENCE_CONNECTOR} {EFFECT_TOOL} {payload}]"
    return directive if forge is None else f"[fake:forge-principal {forge}] {directive}"


def connector_tool_calls(texts: Iterable[Any]) -> list[dict[str, Any]]:
    """The ``connector-tool`` answer lines in reply texts, parsed.

    ``connector-tool <c>/<t>: ok <text> caller=<fp>``, ``... refused <code>
    caller=<fp>`` or ``... failed <reason> caller=<fp>``. Each is
    ``{"connector", "tool", "outcome", "code", "effect", "requester",
    "caller"}``; ``code`` is the refusal code or failure reason, ``effect`` and
    ``requester`` come from an ok line's ``effect=<n> requester=<fp>``.
    """

    calls = []
    for text in texts:
        if not isinstance(text, str):
            continue
        for match in _TOOL_LINE.finditer(text):
            rest = match["rest"]
            words = rest.split()
            outcome = match["outcome"]
            code = None
            if outcome != OK and words and not words[0].startswith("caller="):
                code = words[0]
            effect = _EFFECT.search(rest)
            requester = _REQUESTER.search(rest)
            caller = _CALLER.search(rest)
            calls.append(
                {
                    "connector": match["connector"],
                    "tool": match["tool"],
                    "outcome": outcome,
                    "code": code,
                    "effect": None if effect is None else int(effect["effect"]),
                    "requester": None if requester is None else requester["requester"],
                    "caller": None if caller is None else caller["caller"],
                }
            )
    return calls


def require_effect(
    calls: Sequence[Mapping[str, Any]],
    effects: Sequence[Mapping[str, Any]],
    *,
    expected: str,
    requester: str | None,
    caller: str | None,
) -> dict[str, Any]:
    """One ``record_effect`` call ended as ``expected``, by the reply and the connector's log.

    ``calls`` are the reply's :func:`connector_tool_calls`; ``effects`` the
    reference connector's outcome lines since the send. ``expected`` is ``ok``
    or a refusal code; ``requester`` the fingerprint of the principal header
    the connector should have read (None: it read none, logged as ``-``);
    ``caller`` the claim's token fingerprint the dial must have sent. Requires
    exactly one reply line for the tool with that outcome (an ok line naming
    ``requester``), exactly one connector line with that outcome and
    requester, and so no applied effect for a refusal.
    """

    shown = [
        f"{c.get('connector')}/{c.get('tool')}: {c.get('outcome')} {c.get('code') or ''}".strip()
        for c in calls
    ]
    mine = [
        c
        for c in calls
        if c.get("connector") == REFERENCE_CONNECTOR and c.get("tool") == EFFECT_TOOL
    ]
    check(
        len(mine) == 1,
        f"the reply should carry one `connector-tool {REFERENCE_CONNECTOR}/{EFFECT_TOOL}` line; "
        f"its lines: {shown}",
    )
    call = mine[0]
    if expected == OK:
        check(
            call["outcome"] == OK,
            f"record_effect answered {call['outcome']} {call.get('code')}, expected ok",
        )
        check(
            call.get("requester") == requester,
            f"the effect names requester {call.get('requester')}, expected {requester}",
        )
    else:
        check(
            call["outcome"] == "refused" and call.get("code") == expected,
            f"record_effect answered {call['outcome']} {call.get('code')}, expected refused "
            f"{expected}",
        )
    if caller is not None:
        check(
            call.get("caller") == caller,
            f"the dial sent caller {call.get('caller')}, not the claim's token ({caller})",
        )
    logged = [(str(e.get("outcome")), str(e.get("requester"))) for e in effects]
    check(
        len(effects) == 1,
        f"the connector logged {len(effects)} record_effect outcome lines for this one call: "
        f"{logged}",
    )
    wanted = (expected, requester or "-")
    check(logged[0] == wanted, f"the connector logged {logged[0]}, expected {wanted}")
    applied = [entry for entry in logged if entry[0] == OK]
    check(expected == OK or not applied, f"a refused call applied an effect: {applied}")
    return {
        "reply": dict(call),
        "logged": {"outcome": logged[0][0], "requester": logged[0][1]},
        "effect": call.get("effect"),
    }


def require_proxy_principal(
    decisions: Sequence[Mapping[str, Any]], *, agent: str, connector: str, principal: bool
) -> dict[str, Any]:
    """``connector``'s caller proxy admitted ``agent`` with the principal present or absent.

    At least one admitted decision for ``agent`` on ``connector``, none refused,
    and every admitted one logging ``principal=present`` (``principal``) or
    ``principal=absent``.
    """

    expected = "present" if principal else "absent"
    mine = [d for d in decisions if d.get("caller") == agent and d.get("connector") == connector]
    refused = [d for d in mine if d.get("outcome") != "admitted"]
    check(not refused, f"the {connector} caller proxy refused {agent}: {refused[:3]}")
    admitted = [d for d in mine if d.get("outcome") == "admitted"]
    check(bool(admitted), f"the {connector} caller proxy logged no admitted request from {agent}")
    logged = sorted({str(d.get("principal")) for d in admitted})
    check(
        all(d.get("principal") == expected for d in admitted),
        f"every admitted request from {agent} should log principal={expected}; logged {logged}",
    )
    return {"connector": connector, "admitted": len(admitted), "principal": expected}


def decisions_between(
    decisions: Sequence[Mapping[str, Any]], start: float, end: float
) -> list[dict[str, Any]]:
    """The stamped decisions logged within ``[start, end]``, allowing the clock skew."""

    return [
        dict(d)
        for d in decisions
        if isinstance(d.get("at_epoch"), (int, float))
        and start - CLOCK_SKEW_S <= float(d["at_epoch"]) <= end + CLOCK_SKEW_S
    ]


def require_replay(
    replay: Mapping[str, Any], *, admitted: bool, reason: str | None = None
) -> dict[str, Any]:
    """A :meth:`Harness.proxy_replay` answer was admitted, or refused for ``reason``.

    A refusal must also carry the proxy's frozen refusal body for that reason.
    """

    summary = {
        key: replay.get(key)
        for key in ("mode", "clock", "status", "admitted", "reason", "frozen_refusal_body", "pod")
    }
    if admitted:
        check(
            replay.get("admitted") is True,
            f"the proxy refused the replayed token: {replay.get('status')} {replay.get('reason')}",
        )
    else:
        check(
            replay.get("admitted") is False and replay.get("reason") == reason,
            f"the replay answered {replay.get('status')} {replay.get('reason')}, expected the "
            f"proxy's {reason} refusal",
        )
        check(
            replay.get("frozen_refusal_body") is True,
            f"the {reason} refusal is not the frozen refusal body",
        )
    return summary


def require_replay_logged(
    decisions: Sequence[Mapping[str, Any]],
    replay: Mapping[str, Any],
    *,
    agent: str,
    outcome: str,
    principal: str | None,
) -> dict[str, Any]:
    """The live proxy logged the replay: ``outcome`` for ``agent`` within its window.

    ``decisions`` are ``Harness.proxy_decisions`` since the replay began. Only
    the replay connector's lines inside the replay's host window count, and
    nothing else ran on that connector then. Requires a line with
    ``outcome`` (and ``principal=<principal>`` when given) and no other
    outcome for ``agent``.
    """

    window = [
        d
        for d in decisions_between(decisions, float(replay["started"]), float(replay["finished"]))
        if d.get("connector") == replay.get("connector") and d.get("caller") == agent
    ]
    matching = [
        d
        for d in window
        if d.get("outcome") == outcome and (principal is None or d.get("principal") == principal)
    ]
    shown = [(d.get("outcome"), d.get("principal")) for d in window]
    check(
        bool(matching),
        f"the {replay.get('connector')} proxy logged no outcome={outcome}"
        + (f" principal={principal}" if principal else "")
        + f" for {agent} during the replay; it logged {shown}",
    )
    other = [d for d in window if d.get("outcome") != outcome]
    check(not other, f"the proxy also logged {[(d.get('outcome')) for d in other]} in the window")
    return {"logged": shown}


def requester_bounds(messages: Iterable[Any]) -> list[dict[str, int]]:
    """Every ``bounded to <n>s ... (configured bound <m>s)`` in worker messages."""

    found = []
    for message in messages:
        for match in _BOUNDED.finditer(str(message)):
            found.append({"bound_s": int(match["bound"]), "configured_s": int(match["configured"])})
    return found


def require_bounded(messages: Sequence[Any]) -> dict[str, Any]:
    """The worker bounded opted-in turns inside the requester token's hour.

    At least one ``bounded to <n>s by the requester token window`` line, and
    every bound shorter than both the token's lifetime and the configured bound
    it replaced.
    """

    bounds = requester_bounds(messages)
    check(
        bool(bounds),
        "the worker logged no `bounded to <n>s by the requester token window` line for an "
        "opted-in turn; is the configured turn bound above the requester window?",
    )
    wrong = [
        entry
        for entry in bounds
        if not entry["bound_s"] < REQUESTER_TOKEN_TTL_S
        or not entry["bound_s"] < entry["configured_s"]
    ]
    check(not wrong, f"a bound reaches past the requester token or its configured bound: {wrong}")
    return {"bounds": bounds[:5], "count": len(bounds)}


def token_facts(observed: Mapping[str, Any]) -> dict[str, Any]:
    """An observation's claim, token fingerprint and ``exp``, for evidence."""

    claims = observed.get("claims") if isinstance(observed.get("claims"), Mapping) else {}
    return {
        "claim": observed.get("claim"),
        "claim_created_epoch": observed.get("claim_created_epoch"),
        "fingerprint": observed.get("caller_fingerprint"),
        "exp": claims.get("exp"),
    }


def require_fresher(
    previous: Mapping[str, Any], current: Mapping[str, Any], *, now: float
) -> dict[str, Any]:
    """``current``'s token was minted fresh: its ``exp`` is past ``previous``'s and ``now``."""

    before, after = token_facts(previous)["exp"], token_facts(current)["exp"]
    check(isinstance(after, int), f"the new token carries no integer exp: {after!r}")
    check(after > now, f"the new token's exp {after} is not in the future (now {now:.0f})")
    check(
        isinstance(before, int) and after > before,
        f"the new token's exp {after} is not later than the previous token's {before}",
    )
    return {"previous_exp": before, "exp": after, "exp_in_s": round(after - now)}


def approval_for(
    approvals: Sequence[Mapping[str, Any]], *, conversation: str, since: float, route: str
) -> dict[str, Any] | None:
    """The newest approval on ``conversation`` named ``route``, created at or after ``since``."""

    found = [
        dict(row)
        for row in approvals
        if row.get("conversation_id") == conversation
        and row.get("route") == route
        and epoch(str(row.get("created_at", ""))) >= since - CLOCK_SKEW_S
    ]
    return max(found, key=lambda row: epoch(str(row.get("created_at", "")))) if found else None


def approval_card(replies: Sequence[Mapping[str, Any]], approval_id: str) -> dict[str, Any] | None:
    """The approval card the worker posted for ``approval_id`` among reply events, or None.

    The card is a ``reply.post`` whose message carries a ``confirm``
    interaction with the approval's id.
    """

    for reply in replies:
        event = reply.get("event") if isinstance(reply.get("event"), Mapping) else {}
        message = event.get("message") if isinstance(event.get("message"), Mapping) else {}
        interaction = message.get("interaction")
        if (
            isinstance(interaction, Mapping)
            and interaction.get("kind") == "confirm"
            and str(interaction.get("id")) == approval_id
        ):
            target = event.get("target") if isinstance(event.get("target"), Mapping) else {}
            return {
                "at": reply.get("at"),
                "event": event.get("event"),
                "target": dict(target),
                "fields": message.get("fields"),
            }
    return None


def approver_route(approver: str) -> dict[str, Any]:
    """The approval route binding: the card in the asking thread, ``approver`` may answer."""

    return {"resolution": {"mode": "requesting_surface"}, "approvers": {"emails": [approver]}}


def wait_until(
    deadline: float,
    label: str,
    *,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Sleep until unix time ``deadline``, reporting what is left on stderr every few minutes."""

    while True:
        left = deadline - now()
        if left <= 0:
            return
        log(f"{label}: {left / 60:.1f} min left")
        sleep(min(left, WAIT_PROGRESS_S))


# --- shared steps -------------------------------------------------------------------


def flag_agent_of(harness: Harness, args: Any) -> str:
    """``--flag-agent``, defaulting to the first agent `up` registered."""

    flag, _ = default_agents(harness.data, args.flag_agent, None)
    if flag is None:
        raise HarnessError("this scenario needs an agent; run `up --agent A` or pass --flag-agent")
    harness.agent_entry(flag)
    return str(flag)


def roster_groups(harness: Harness, sender: str) -> list[str]:
    entry = harness.data.get("roster", {}).get("senders", {}).get(sender.lower()) or {}
    return [str(group) for group in entry.get("groups") or []]


def require_reference_connector(harness: Harness, agents: Sequence[str]) -> dict[str, Any]:
    """Every agent has the reference connector deployed behind its caller proxy."""

    seen = {}
    for name in agents:
        deployment = harness.connector_deployment(name, REFERENCE_CONNECTOR)
        proxied = proxied_connector(deployment)
        check(
            proxied == REFERENCE_CONNECTOR,
            f"{name}'s {REFERENCE_CONNECTOR} connector has no caller proxy naming it ({proxied})",
        )
        seen[name] = str(deployment["metadata"]["name"])
    return seen


def await_effects(harness: Harness, agent: str, since: float) -> list[dict[str, Any]]:
    """The connector's outcome lines since ``since``, waiting briefly for a trailing line."""

    deadline = time.monotonic() + EFFECT_LOG_WAIT_S
    while True:
        effects = harness.connector_effects(agent, since)
        if effects or time.monotonic() > deadline:
            return effects
        time.sleep(2)


def effect_call(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    sender: str,
    arguments: Mapping[str, Any],
    expected: str,
    principal: Mapping[str, Any] | None,
    wait: float,
    forge: str | None = None,
) -> dict[str, Any]:
    """One email turn calling ``record_effect``, judged from the claim to the connector.

    The claim's caller token names ``principal`` (none when None); the turn
    is delivered; the reply, the connector's log and the caller proxy agree on
    ``expected`` for the requester the token named. Returns the effect summary.
    """

    text = f"requester-effect {effect_directive(arguments, forge=forge)}"
    sent = harness.send(agent, sender, text)
    seen["send"] = send_summary(sent)
    check(sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}")
    since = float(sent["sent_at"]) - CLOCK_SKEW_S
    observed = harness.observe(agent, wait=wait)
    seen["observe"] = observed_token(observed)
    check(bool(observed.get("caller_token_present")), "no connector caller token reached the claim")
    claims = observed.get("claims") or {}
    check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
    if principal is None:
        check("principal" not in claims, f"the token carries {claims.get('principal')}")
    else:
        seen["principal"] = require_principal(observed, principal)
    turn = observed.get("turn") or {}
    require_delivered(turn)
    calls = connector_tool_calls(turn.get("texts") or [])
    effects = await_effects(harness, agent, since)
    decisions = harness.proxy_decisions(agent, since)
    seen.update(calls=calls, effects=effects, decisions=decisions)
    requester = None if principal is None else principal_fingerprint(principal)
    seen["effect"] = require_effect(
        calls,
        effects,
        expected=expected,
        requester=requester,
        caller=observed.get("caller_fingerprint"),
    )
    seen["proxy"] = require_proxy_principal(
        decisions, agent=agent, connector=REFERENCE_CONNECTOR, principal=principal is not None
    )
    return dict(seen["effect"])


def _quietly(call: Callable[[], Any]) -> Any:
    """``call()``, or None when it fails; for restores in a ``finally``."""

    try:
        return call()
    except HarnessError as exc:
        log(f"restore failed: {exc}")
        return None


# --- requester-effect ---------------------------------------------------------------


@scenario("requester-effect")
def requester_effect(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0270 decisions 4 and 5: a human-restricted effect follows the attested human only.

    E0 preconditions; E1 a member's call applies the effect under their
    fingerprint; E2 a verified human outside the group is refused; E3 so are
    model-written arguments naming the member; E4 so is a forged
    ``X-Curie-Principal`` (the proxy drops it and attests the real caller); E5
    a revoked membership refuses until restored; E6 an unavailable authority
    refuses; E7 the flag-off control's token names no one, refused
    ``missing_requester``. Every refusal applies no effect.
    """

    flag_agent, control_agent = two_agents(harness, args)
    sender, outsider = args.sender.lower(), args.outsider.lower()
    if sender == outsider:
        raise HarnessError("requester-effect needs two humans; --outsider equals --sender")
    principal_a, principal_d = principal_for(harness, sender), principal_for(harness, outsider)
    evidence.data.update(
        flag_agent=flag_agent,
        control_agent=control_agent,
        sender=sender,
        outsider=outsider,
        connector=REFERENCE_CONNECTOR,
        authority_group=AUTHORITY_GROUP,
        expected_principals={sender: principal_a, outsider: principal_d},
        requester_fingerprints={
            sender: principal_fingerprint(principal_a),
            outsider: principal_fingerprint(principal_d),
        },
    )
    wait = float(args.wait)
    membership = {"sender": sender, "group": AUTHORITY_GROUP}

    with (
        evidence.step(
            "E0",
            "preconditions: the flag agent opted in, the control not, both with the reference "
            "connector behind a caller proxy, and the provider's authority reset",
        ) as seen,
        harness.api() as api,
    ):
        require_install_ready(harness, seen, (flag_agent, control_agent))
        # The admin change to a membership changes the roster lookups read too,
        # and an issuer-group invocation filter would refuse the outsider before
        # any claim, so this scenario needs `up` without --role-filter.
        check(
            not harness.data.get("role_filter"),
            "`up --role-filter` is set; requester-effect needs every verified human admitted",
        )
        seen["reference_connector"] = require_reference_connector(
            harness, (flag_agent, control_agent)
        )
        check(
            AUTHORITY_GROUP in roster_groups(harness, sender),
            f"{sender} is not in {AUTHORITY_GROUP} in the roster `up` installed",
        )
        check(
            AUTHORITY_GROUP not in roster_groups(harness, outsider),
            f"{outsider} is in {AUTHORITY_GROUP}; pass an --outsider who is not",
        )
        for name, wanted in ((flag_agent, True), (control_agent, False)):
            flag, version = set_requester_flag(
                api,
                harness.agent_entry(name)["agent_id"],
                wanted,
                what=f"setting {name}'s flag {'on' if wanted else 'off'}",
            )
            seen[name] = {"flag": flag, "version": version}
            check(flag is wanted, f"{name}'s flag is {flag!r}, not {wanted}")
        reset = {
            "authority": harness.provider_admin(ADMIN_AUTHORITY, {"available": True}),
            "membership": harness.provider_admin(ADMIN_MEMBERSHIP, {**membership, "member": True}),
        }
        seen["provider_reset"] = reset
        check(
            all(status == 200 for status in reset.values()),
            f"the provider admin answered {reset}; does it serve the authority "
            "(`up --reference-connector`)?",
        )
    require_passed(evidence)

    def nonce(step: str) -> str:
        return f"{step} {uuid.uuid4().hex[:8]}"

    with evidence.step(
        "E1", f"{sender}, a member, applies the effect under their own fingerprint"
    ) as seen:
        effect_call(
            harness,
            seen,
            agent=flag_agent,
            sender=sender,
            arguments={"note": nonce("E1")},
            expected=OK,
            principal=principal_a,
            wait=wait,
        )

    with evidence.step("E2", f"{outsider}, a verified human outside the group, is refused") as seen:
        effect_call(
            harness,
            seen,
            agent=flag_agent,
            sender=outsider,
            arguments={"note": nonce("E2")},
            expected=NOT_AUTHORIZED,
            principal=principal_d,
            wait=wait,
        )

    with evidence.step("E3", f"{outsider}'s model arguments naming {sender} grant nothing") as seen:
        effect_call(
            harness,
            seen,
            agent=flag_agent,
            sender=outsider,
            arguments={
                "note": nonce("E3"),
                "subject": principal_a["subject"],
                "as_subject": principal_a["subject"],
                "issuer": principal_a["issuer"],
                "tenant_id": principal_a["tenant_id"],
            },
            expected=NOT_AUTHORIZED,
            principal=principal_d,
            wait=wait,
        )

    with evidence.step(
        "E4",
        f"{outsider}'s forged X-Curie-Principal naming {sender} is dropped; the proxy attests "
        f"{outsider}",
    ) as seen:
        forged = principal_header_value(principal_a)
        seen["forged_fingerprint"] = principal_fingerprint(principal_a)
        result = effect_call(
            harness,
            seen,
            agent=flag_agent,
            sender=outsider,
            arguments={"note": nonce("E4")},
            expected=NOT_AUTHORIZED,
            principal=principal_d,
            wait=wait,
            forge=forged,
        )
        check(
            result["logged"]["requester"] != seen["forged_fingerprint"],
            "the connector read the forged principal",
        )

    with evidence.step(
        "E5", f"{sender}'s revoked membership refuses; restored, the effect applies again"
    ) as seen:
        restored = None
        try:
            revoke = harness.provider_admin(ADMIN_MEMBERSHIP, {**membership, "member": False})
            seen["revoke_status"] = revoke
            check(revoke == 200, f"revoking {sender}'s membership answered {revoke}")
            revoked: dict[str, Any] = {}
            seen["revoked"] = revoked
            effect_call(
                harness,
                revoked,
                agent=flag_agent,
                sender=sender,
                arguments={"note": nonce("E5 revoked")},
                expected=NOT_AUTHORIZED,
                principal=principal_a,
                wait=wait,
            )
        finally:
            restored = _quietly(
                lambda: harness.provider_admin(ADMIN_MEMBERSHIP, {**membership, "member": True})
            )
            seen["restore_status"] = restored
        check(restored == 200, f"restoring {sender}'s membership answered {restored}")
        again: dict[str, Any] = {}
        seen["restored"] = again
        effect_call(
            harness,
            again,
            agent=flag_agent,
            sender=sender,
            arguments={"note": nonce("E5 restored")},
            expected=OK,
            principal=principal_a,
            wait=wait,
        )

    with evidence.step(
        "E6", "an unavailable authority refuses the effect; the authority is restored"
    ) as seen:
        restored = None
        try:
            down = harness.provider_admin(ADMIN_AUTHORITY, {"available": False})
            seen["unavailable_status"] = down
            check(down == 200, f"taking the authority down answered {down}")
            effect_call(
                harness,
                seen,
                agent=flag_agent,
                sender=sender,
                arguments={"note": nonce("E6")},
                expected=AUTHORITY_UNAVAILABLE,
                principal=principal_a,
                wait=wait,
            )
        finally:
            restored = _quietly(
                lambda: harness.provider_admin(ADMIN_AUTHORITY, {"available": True})
            )
            seen["restore_status"] = restored
        check(restored == 200, f"restoring the authority answered {restored}")
        status, decision = harness.authority_decision(principal_a)
        seen["restored_decision"] = {"status": status, "decision": decision}
        check(
            status == 200 and decision == {"decision": "allow"},
            f"the restored authority answers {status} {decision} for {sender}",
        )

    with evidence.step(
        "E7",
        f"the flag-off control agent names no one: {sender} is refused {MISSING_REQUESTER}",
    ) as seen:
        effect_call(
            harness,
            seen,
            agent=control_agent,
            sender=sender,
            arguments={"note": nonce("E7")},
            expected=MISSING_REQUESTER,
            principal=None,
            wait=wait,
        )


# --- requester-expiry ---------------------------------------------------------------


def replay_connector(harness: Harness, agent: str) -> str:
    """The connector a replay goes to: the reference one when deployed, else the first."""

    connectors = harness.connectors(agent)
    check(bool(connectors), f"{agent} has no hosted connector to replay a token to")
    return REFERENCE_CONNECTOR if REFERENCE_CONNECTOR in connectors else connectors[0]


@scenario("requester-expiry")
def requester_expiry(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0270 decision 3 and acceptance items 3 and 4, on the opted-in flag agent.

    X1 the first human (A) writes on C3 and then C1, and C1's caller token is
    captured in memory; X0 the worker bounded those turns to the token's
    window; X2 the second human (B) on C1 replaces the sandbox; X3 A's old
    token is still admitted by the live proxy (the accepted residual window);
    X4 under a controlled clock the deployed proxy admits it at ``exp - 1``
    and refuses it ``expired`` at ``exp``; X5 a warm route whose caller token
    is doctored too short is replaced with a fresh token for A; X6 after real
    time passes the token's expiry the live proxy refuses it, and A's next
    turn on the idle C3 gets a fresh token. X6 waits up to an hour.
    """

    agent = flag_agent_of(harness, args)
    first, second = args.sender.lower(), args.second_sender.lower()
    if first == second:
        raise HarnessError("requester-expiry needs two humans; --second-sender equals --sender")
    principal_a, principal_b = principal_for(harness, first), principal_for(harness, second)
    evidence.data.update(
        flag_agent=agent,
        sender=first,
        second_sender=second,
        expected_principals={first: principal_a, second: principal_b},
    )
    wait = float(args.wait)
    # The captured C1 token lives only in this frame.
    captured: dict[str, Any] = {}
    c1: dict[str, Any] = {}
    c3: dict[str, Any] = {}
    started = time.time()

    with evidence.step(
        "X1",
        f"{first} writes on C3 and then on C1: each claim names {first}; C1's caller token is "
        "captured",
    ) as seen:
        require_install_ready(harness, seen, (agent,))
        with harness.api() as api:
            flag, version = set_requester_flag(
                api, harness.agent_entry(agent)["agent_id"], True, what=f"setting {agent}'s flag on"
            )
        seen["flag"] = {"flag": flag, "version": version}
        check(flag is True, f"{agent}'s flag is {flag!r}, not on")
        for label, into in (("C3", c3), ("C1", c1)):
            turn_seen: dict[str, Any] = {}
            seen[label] = turn_seen
            sent, observed, turn = conversation_turn(
                harness,
                turn_seen,
                agent=agent,
                sender=first,
                text=f"requester-expiry: X1 {label} {CONNECTOR_CALL}",
                conversation=None,
                wait=wait,
            )
            judge_conversation_turn(
                harness,
                turn_seen,
                agent=agent,
                sent=sent,
                observed=observed,
                turn=turn,
                principal=principal_a,
            )
            into.update(conversation=str(sent["conversation_id"]), observed=observed)
            turn_seen["token"] = token_facts(observed)
        token = harness.capture_caller_token(str(c1["observed"]["claim"]))
        check(token is not None, "C1's claim has no caller token to capture")
        assert token is not None
        check(
            caller_fingerprint(token) == c1["observed"].get("caller_fingerprint"),
            "the captured token is not the one C1's claim observed",
        )
        exp = decode_caller_token(token)["payload"].get("exp")
        check(isinstance(exp, int), f"C1's token carries no integer exp: {exp!r}")
        captured.update(token=token, exp=int(exp), fingerprint=caller_fingerprint(token))
        seen["captured"] = {"fingerprint": captured["fingerprint"], "exp": captured["exp"]}
        check(
            captured["exp"] - time.time() <= REQUESTER_TOKEN_TTL_S,
            f"C1's requester token lives past an hour (exp {captured['exp']})",
        )
    require_passed(evidence)

    with evidence.step(
        "X0", "the worker bounded opted-in turns to the requester token window"
    ) as seen:
        # Whole messages: the thread key in the line can push its bounds past
        # the evidence truncation.
        messages = [
            entry["message"]
            for entry in harness.worker_log(started - CLOCK_SKEW_S)
            if "requester token window" in entry["message"]
        ]
        seen["messages"] = [str(m)[:LOG_MESSAGE_LIMIT] for m in messages[:3]]
        seen["bounded"] = require_bounded(messages)

    with evidence.step(
        "X2", f"{second} writes on C1: the sandbox is replaced and names {second}"
    ) as seen:
        sent, observed, turn = conversation_turn(
            harness,
            seen,
            agent=agent,
            sender=second,
            text=f"requester-expiry: X2 {CONNECTOR_CALL}",
            conversation=c1["conversation"],
            wait=wait,
        )
        seen["replaced"] = require_replaced(
            c1["observed"], observed, sent_at=float(sent["sent_at"])
        )
        judge_conversation_turn(
            harness,
            seen,
            agent=agent,
            sent=sent,
            observed=observed,
            turn=turn,
            principal=principal_b,
        )
    require_passed(evidence)

    connector = replay_connector(harness, agent)
    evidence.data["replay_connector"] = connector

    with evidence.step(
        "X3",
        f"after the handoff, {first}'s copied C1 token is still admitted by the live proxy "
        "(the accepted residual window)",
    ) as seen:
        check(time.time() < captured["exp"], "C1's token already expired before the replay")
        replay = harness.proxy_replay(agent, connector, captured["token"])
        seen["replay"] = require_replay(replay, admitted=True)
        seen["exp_in_s"] = round(captured["exp"] - time.time())
        decisions = harness.proxy_decisions(agent, float(replay["started"]) - CLOCK_SKEW_S)
        seen["logged"] = require_replay_logged(
            decisions, replay, agent=agent, outcome="admitted", principal="present"
        )

    with evidence.step(
        "X4",
        "under a controlled clock the deployed proxy admits the token at exp - 1 and refuses it "
        "expired at exp",
    ) as seen:
        before = harness.proxy_replay(
            agent, connector, captured["token"], clock=captured["exp"] - 1
        )
        seen["at_exp_minus_1"] = require_replay(before, admitted=True)
        at = harness.proxy_replay(agent, connector, captured["token"], clock=captured["exp"])
        seen["at_exp"] = require_replay(at, admitted=False, reason=EXPIRED)

    with evidence.step(
        "X5",
        f"a warm route whose caller token is too short is replaced: a fresh token for {first}",
    ) as seen:
        warm: dict[str, Any] = {}
        seen["warm"] = warm
        sent, observed, turn = conversation_turn(
            harness,
            warm,
            agent=agent,
            sender=first,
            text=f"requester-expiry: X5 warm {CONNECTOR_CALL}",
            conversation=None,
            wait=wait,
        )
        judge_conversation_turn(
            harness,
            warm,
            agent=agent,
            sent=sent,
            observed=observed,
            turn=turn,
            principal=principal_a,
        )
        warm["token"] = token_facts(observed)
        conversation, claim = str(sent["conversation_id"]), str(observed["claim"])
        doctored = int(time.time()) + SHORT_CALLER_WINDOW_S
        route = harness.route_doctor(claim, doctored)
        seen["doctored"] = route
        check(
            route.get("found") == 1 and route.get("changed") is True,
            f"the live route record of {claim} could not be doctored: {route}",
        )
        original = route["before"]["caller_token_exp"]
        check(
            route["after"]["state_token_exp"] == route["before"]["state_token_exp"],
            "doctoring changed the route's state token expiry",
        )
        seen["caller_window_before_s"] = (
            None if not isinstance(original, int) else round(original - time.time())
        )
        try:
            again: dict[str, Any] = {}
            seen["again"] = again
            sent, replaced, turn = conversation_turn(
                harness,
                again,
                agent=agent,
                sender=first,
                text=f"requester-expiry: X5 again {CONNECTOR_CALL}",
                conversation=conversation,
                wait=wait,
            )
            seen["replaced"] = require_replaced(observed, replaced, sent_at=float(sent["sent_at"]))
            judge_conversation_turn(
                harness,
                again,
                agent=agent,
                sent=sent,
                observed=replaced,
                turn=turn,
                principal=principal_a,
            )
            seen["fresh"] = require_fresher(observed, replaced, now=float(sent["sent_at"]))
        finally:
            # A replaced route no longer names the claim, so nothing is restored;
            # a route still holding the doctored value gets its own back.
            seen["restored"] = (
                None
                if not isinstance(original, int)
                else _quietly(lambda: harness.route_doctor(claim, original, only_if=doctored))
            )

    with evidence.step(
        "X6",
        f"past C1's token expiry the live proxy refuses it expired, and {first}'s next turn on "
        "the idle C3 gets a fresh token",
    ) as seen:
        deadline = captured["exp"] + EXPIRY_MARGIN_S
        seen["waited_until"] = deadline
        wait_until(
            deadline, f"requester-expiry X6: waiting for C1's token to expire at {captured['exp']}"
        )
        replay = harness.proxy_replay(agent, connector, captured["token"])
        seen["replay"] = require_replay(replay, admitted=False, reason=EXPIRED)
        decisions = harness.proxy_decisions(agent, float(replay["started"]) - CLOCK_SKEW_S)
        seen["logged"] = require_replay_logged(
            decisions, replay, agent=agent, outcome=EXPIRED, principal=None
        )
        idle: dict[str, Any] = {}
        seen["C3"] = idle
        sent, observed, turn = conversation_turn(
            harness,
            idle,
            agent=agent,
            sender=first,
            text=f"requester-expiry: X6 {CONNECTOR_CALL}",
            conversation=c3["conversation"],
            wait=wait,
        )
        seen["replaced"] = require_replaced(
            c3["observed"], observed, sent_at=float(sent["sent_at"])
        )
        judge_conversation_turn(
            harness,
            idle,
            agent=agent,
            sent=sent,
            observed=observed,
            turn=turn,
            principal=principal_a,
        )
        seen["fresh"] = require_fresher(c3["observed"], observed, now=float(sent["sent_at"]))
    captured.clear()


# --- requester-approval -------------------------------------------------------------


def await_approval(
    harness: Harness, agent_id: str, conversation: str, since: float, wait: float
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """The approval raised on ``conversation`` since ``since``, and what the API listed."""

    deadline = time.monotonic() + wait
    while True:
        with harness.api() as api:
            rows = list(api.ok("GET", f"/approvals?agent_id={agent_id}&limit=50"))
        found = approval_for(rows, conversation=conversation, since=since, route=APPROVAL_ROUTE)
        if found is not None or time.monotonic() > deadline:
            return found, rows
        time.sleep(3)


def await_card(
    harness: Harness, agent: str, conversation: str, since: float, approval_id: str, wait: float
) -> dict[str, Any] | None:
    deadline = time.monotonic() + wait
    while True:
        card = approval_card(harness.replies(agent, since, conversation), approval_id)
        if card is not None or time.monotonic() > deadline:
            return card
        time.sleep(3)


def approval_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    keep = (
        "id",
        "status",
        "route",
        "author",
        "conversation_id",
        "reply_channel",
        "card_channel",
        "resolved_by",
        "created_at",
        "resolved_at",
    )
    return {key: row.get(key) for key in keep}


@scenario("requester-approval")
def requester_approval(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0270 decision 3: an approval resume names the original requester, freshly minted.

    P0 binds an approval route whose card stays in the asking email thread and
    lists the approver, and sets the agent's approver role to the member
    group; P1 the requester (A) asks for approval on the flag agent and the
    card reaches the email thread; P2 the approver (B) answers by a signed
    email approval answer (ADR 0267), accepted through the HTTP approver role;
    P3 the resume runs on a claim created after the answer whose caller token
    names A, never B, and differs from the paused turn's; P4 restores what P0
    changed.
    """

    agent = flag_agent_of(harness, args)
    requester, approver = args.sender.lower(), args.second_sender.lower()
    if requester == approver:
        raise HarnessError("requester-approval needs two humans; --second-sender equals --sender")
    principal_a, principal_b = principal_for(harness, requester), principal_for(harness, approver)
    entry = harness.agent_entry(agent)
    agent_id, binding_id = str(entry["agent_id"]), str(entry["binding_id"])
    evidence.data.update(
        flag_agent=agent,
        requester=requester,
        approver=approver,
        route=APPROVAL_ROUTE,
        expected_principals={requester: principal_a, approver: principal_b},
    )
    wait = float(args.wait)
    restore: dict[str, Any] = {}
    state: dict[str, Any] = {}
    try:
        with (
            evidence.step(
                "P0",
                f"preconditions; bind route {APPROVAL_ROUTE} (card in the email thread, approver "
                f"{approver}) and the approver role to {AUTHORITY_GROUP}",
            ) as seen,
            harness.api() as api,
        ):
            require_install_ready(harness, seen, (agent,))
            flag, version = set_requester_flag(
                api, agent_id, True, what=f"setting {agent}'s flag on"
            )
            seen["flag"] = {"flag": flag, "version": version}
            check(flag is True, f"{agent}'s flag is {flag!r}, not on")
            check(
                AUTHORITY_GROUP in roster_groups(harness, approver),
                f"{approver} is not in {AUTHORITY_GROUP}, so the approver role cannot admit them",
            )
            # Read raw so a restore writes back exactly what was stored; the
            # API's read shape redacts a notification target's transport.
            rows = harness.target.sql(
                (
                    "SELECT approval_routes FROM curie.agents WHERE id::text = :a",
                    {"a": agent_id},
                )
            )[0]
            stored = rows[0]["approval_routes"] if rows else None
            restore["routes"] = dict(stored) if isinstance(stored, dict) else {}
            routes = {**restore["routes"], APPROVAL_ROUTE: approver_route(approver)}
            restore["routes_written"] = True
            status, body = api.call("PATCH", f"/agents/{agent_id}", {"approval_routes": routes})
            seen["route"] = {"status": status, "binding": routes[APPROVAL_ROUTE]}
            check(status == 200, f"binding the approval route answered {status}: {body}")
            role = harness.target.pod_python(
                ROLE_FILTER_SCRIPT,
                {
                    "tenant_id": harness.data["tenant_id"],
                    "agent_id": agent_id,
                    "role": "approver",
                    "filter": role_filter(),
                },
            )
            restore["approver_role"] = {
                "previous": role.get("previous"),
                "previous_present": role.get("previous_present"),
            }
            seen["approver_role"] = {
                "action": role.get("action"),
                "legacy_state": role.get("legacy_state"),
                "version": role.get("version"),
                "filter": role_filter(),
                "previous_present": role.get("previous_present"),
            }
        require_passed(evidence)

        with evidence.step(
            "P1",
            f"{requester} asks for approval on the flag agent: the approval is raised and its "
            "card is delivered on the email thread",
        ) as seen:
            text = f"{APPROVAL_MARKER}{APPROVAL_ROUTE}] requester-approval P1 {CONNECTOR_CALL}"
            sent = harness.send(agent, requester, text)
            seen["send"] = send_summary(sent)
            check(
                sent.get("http_status") == 200,
                f"the email turn answered {sent.get('http_status')}",
            )
            conversation = str(sent["conversation_id"])
            since = float(sent["sent_at"]) - CLOCK_SKEW_S
            paused = harness.observe(agent, since=since, wait=wait)
            seen["pre_pause"] = observed_token(paused)
            check(bool(paused.get("caller_token_present")), "no caller token reached the claim")
            seen["principal"] = require_principal(paused, principal_a)
            approval, listed = await_approval(harness, agent_id, conversation, since, wait)
            check(
                approval is not None,
                f"no approval on route {APPROVAL_ROUTE} was raised for the conversation; the API "
                f"lists {[approval_summary(row) for row in listed[:5]]}",
            )
            assert approval is not None
            seen["approval"] = approval_summary(approval)
            check(approval.get("status") == "pending", f"the approval is {approval.get('status')}")
            check(
                approval.get("card_channel") == approval.get("reply_channel"),
                "the card was not shown in the asking email thread, so it cannot be answered "
                "by email",
            )
            approval_id = str(approval["id"])
            card = await_card(harness, agent, conversation, since, approval_id, wait)
            check(card is not None, f"no card for approval {approval_id} reached the email thread")
            seen["card"] = card
            state.update(
                approval_id=approval_id,
                conversation=conversation,
                paused=paused,
                event_id=accepted_event_id(sent),
            )
        require_passed(evidence)

        with (
            evidence.step(
                "P2",
                f"{approver} answers by a signed email approval answer: accepted as approved "
                "through the HTTP approver role",
            ) as seen,
            harness.api() as api,
        ):
            approval_id = state["approval_id"]
            minted = api.ok(
                "POST",
                "/approvals/principals/adapter",
                {"subject": ADAPTER, "binding_ids": [binding_id], "ttl_s": 3600},
            )
            seen["adapter_principal"] = {
                key: minted.get(key) for key in ("subject", "binding_ids", "scopes", "expires_at")
            }
            body = json.dumps({"decision": "approved"}, separators=(",", ":")).encode()
            registration = harness.data["registration"]
            signed = harness.target.pod_python(
                ANSWER_SIGN_SCRIPT,
                {
                    "key": harness.data["keys"]["receiver"],
                    "proof": {
                        "schema_version": 1,
                        "purpose": "approval_answer",
                        "operation": "resolve",
                        "receiver_id": registration["receiver_id"],
                        "receiver_generation": registration["generation"],
                        "binding_id": binding_id,
                        "approval_id": approval_id,
                        "sender": approver,
                        "body_sha256": hashlib.sha256(body).hexdigest(),
                    },
                },
            )
            answered_at = time.time()
            status, payload = http(
                "POST",
                f"{api.base}/approvals/{approval_id}/resolve",
                raw=body,
                headers={
                    ADAPTER_PRINCIPAL_HEADER: str(minted["token"]),
                    APPROVAL_ACTOR_HEADER: approver,
                    APPROVAL_PROOF_HEADER: str(signed["proof"]),
                },
            )
            state["answered_at"] = answered_at
            seen["resolve"] = {"status": status, "answer": summarize_event(payload)}
            check(status == 200, f"the email approval answer answered {status}: {payload}")
            check(
                isinstance(payload, dict)
                and payload.get("status") == "approved"
                and payload.get("resolved_by") == approver,
                f"the answer did not resolve the approval approved by {approver}",
            )
            audit = list(api.ok("GET", f"/approvals/{approval_id}/audit"))
            resolved = [row for row in audit if row.get("action") == "resolved"]
            seen["audit"] = [
                {
                    key: row.get(key)
                    for key in ("action", "actor", "principal_kind", "authorizer", "authorized")
                }
                for row in resolved
            ]
            check(
                any(
                    row.get("actor") == approver
                    and row.get("principal_kind") == "adapter"
                    and row.get("authorizer") == HTTP_ANSWER_AUTHORIZER
                    and row.get("authorized") is True
                    for row in resolved
                ),
                f"no audit row records {approver}'s authorized answer by {HTTP_ANSWER_AUTHORIZER}",
            )
        require_passed(evidence)

        with evidence.step(
            "P3",
            f"the resume runs on a claim created after the answer whose caller token names "
            f"{requester}, never {approver}, freshly minted",
        ) as seen:
            answered_at = float(state["answered_at"])
            since = answered_at - CLOCK_SKEW_S
            resumed = harness.observe(agent, since=since, wait=wait)
            seen["resume"] = observed_token(resumed)
            check(bool(resumed.get("caller_token_present")), "no caller token reached the claim")
            seen["replaced"] = require_replaced(state["paused"], resumed, sent_at=answered_at)
            claims = resumed.get("claims") or {}
            check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
            check(
                claims.get("principal") != principal_b,
                f"the resumed token names the approver {approver}",
            )
            seen["principal"] = require_principal(resumed, principal_a)
            resume_event = f"approval-{state['approval_id']}-resolved"
            turns, _ = await_turns(
                harness, agent, state["conversation"], since, [resume_event], wait
            )
            turn = turns[resume_event]
            seen["turn"] = turn
            require_delivered(turn)
            fingerprint = str(resumed.get("caller_fingerprint"))
            seen["reply_fingerprint"] = require_reply_fingerprint(
                turn.get("texts") or [], fingerprint
            )
            decisions = harness.proxy_decisions(agent, since)
            seen["connector_call"] = require_connector_admitted(
                turn.get("texts") or [],
                decisions,
                agent=agent,
                connectors=harness.connectors(agent),
                principal=True,
            )
    finally:
        restore_approval_setup(harness, evidence, agent_id=agent_id, restore=restore)


def restore_approval_setup(
    harness: Harness, evidence: Evidence, *, agent_id: str, restore: Mapping[str, Any]
) -> None:
    """P4: put back the approval routes and the approver role P0 changed."""

    if not restore:
        return
    with evidence.step(
        "P4", "restore the approval routes and the approver role P0 changed"
    ) as seen:
        failures = []
        if restore.get("routes_written"):
            with harness.api() as api:
                status, body = api.call(
                    "PATCH", f"/agents/{agent_id}", {"approval_routes": restore["routes"]}
                )
            seen["routes"] = {"status": status, "restored": sorted(restore["routes"])}
            if status != 200:
                failures.append(f"routes: {status} {body}")
        previous = restore.get("approver_role")
        if previous is not None:
            role = _quietly(
                lambda: harness.target.pod_python(
                    ROLE_FILTER_SCRIPT,
                    {
                        "tenant_id": harness.data["tenant_id"],
                        "agent_id": agent_id,
                        "role": "approver",
                        "filter": previous["previous"],
                    },
                )
            )
            seen["approver_role"] = (
                None
                if role is None
                else {key: role.get(key) for key in ("action", "legacy_state", "version")}
            )
            if role is None:
                failures.append("approver role: the restore failed")
            elif not previous.get("previous_present"):
                # agent_role_policy has no removal: the role stays configured
                # with no filter, which the answer path reads as unrestricted.
                seen["residual"] = (
                    "the approver role had no entry before P0; it now has one with no filter"
                )
        check(not failures, f"restoring P0's changes failed; repair by hand: {failures}")
