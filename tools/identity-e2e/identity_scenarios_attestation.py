"""ADR 0271 grant attestation scenario for `curie dev identity-e2e -- prove`.

``grant-attestation`` (acceptance items 5 and the carried part of 2, decision
7): through the real worker, caller proxy and reference connector, with the two
protected execution tools absent from the proxy's gated list, a grant-less
call has no effect; the grant the worker mints for an approved resume rides
unspent on unrelated calls, is spent once by the approved call and emits its
id; and replay, forged attestation headers, changed arguments, batches and
tampered tickets refuse, with an uncertain execution never resent. Needs
``up --reference-connector``. The caller proxy exists only on Kubernetes, so
there is no Docker variant.

Registered by ``identity_scenarios`` on import. The same rules hold as there:
every step drives the install through its own boundaries, a caller token, a
grant, a ticket and every key stay in memory or in the private state, and the
evidence carries only fingerprints, counts and codes, never a token, grant,
ticket, key or principal value.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from fnmatch import fnmatchcase
from typing import Any

from identity_assets import ed25519_pair
from identity_e2e import (
    AUTHORITY_GROUP,
    CALLER_PROXY_CONTAINER,
    REFERENCE_CONNECTOR,
    WORKER_CONTAINER,
    Harness,
    HarnessError,
    caller_fingerprint,
    connector_server_container,
    decode_caller_token,
    epoch,
    principal_fingerprint,
)
from identity_scenarios import (
    CLOCK_SKEW_S,
    Evidence,
    accepted_event_id,
    await_turns,
    check,
    principal_for,
    requester_flag,
    require_delivered,
    require_install_ready,
    require_passed,
    require_principal,
    require_replaced,
    scenario,
    send_summary,
    set_requester_flag,
)
from identity_scenarios_requester import (
    _quietly,
    answer_by_email,
    approval_summary,
    await_card,
    await_effects,
    flag_agent_of,
    require_proxy_principal,
    require_reference_connector,
    restore_approver_role,
    roster_groups,
    set_approver_role,
)
from identity_target import b64url_decode, log

EFFECT_TOOL = "record_effect"
ATTESTED_TOOL = "execute_attested"
APPROVED_TOOL = "execute_approved"
PROTECTED_TOOLS = (ATTESTED_TOOL, APPROVED_TOOL)
OK = "ok"
ATTESTED = "present"
# The caller proxy's refusal and grant sub-reasons (curie_connector_proxy).
GRANT_REQUIRED = "grant_required"
GRANT_NOT_SPENT = "not_spent"
GRANT_BATCHED_CALL = "batched_call"
# The reference connector's refusal codes (human_restricted_connector).
ATTESTATION_MISSING = "attestation_missing"
TICKET_REQUESTER_MISMATCH = "ticket_requester_mismatch"
TICKET_SCOPE_MISMATCH = "ticket_scope_mismatch"
TICKET_INVALID = "ticket_invalid"
TICKET_ARGUMENTS_MISMATCH = "ticket_arguments_mismatch"
# The worker's connector reconciler switch (charts/curie worker.connectorReconciler).
RECONCILE_ENV = "CURIE_CONNECTOR_RECONCILE"
_RECONCILE_OFF = frozenset({"0", "false", "f", "no", "n", "off"})
# The caller proxy's deploy-time gated tool patterns (curie_connector_proxy.server).
GATED_TOOLS_ENV = "CURIE_CALLER_PROXY_GATED_TOOLS"
# The reference connector's ticket settings, and the values this run gives them.
TICKET_ENV = ("TICKET_PUBLIC_KEY", "TICKET_KEY_ID", "TICKET_ROUTE", "TICKET_AGENT_ID")
SELECTOR_KEY_ID = "identity-e2e-selector"
TICKET_ROUTE = "identity-e2e-ticket"
OTHER_TICKET_ROUTE = "identity-e2e-elsewhere"
# The run's selector key pair is generated here, under the state directory.
SELECTOR_KEY_DIR = "selector-key"
# A permission gate's approval row (curie_api ApprovalOut.gate_kind).
PERMISSION_GATE = "permission"
# G9: the pause between two probes, and how far a log stamp may sit outside the
# probe's own pod-clock window. The pause is wider than twice the slack, so no
# line can fall into two windows.
PROBE_GAP_S = 1.0
PROBE_SLACK_S = 0.3
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DIAL_LINE = re.compile(
    r"\bconnector-tool (?P<connector>[^\s/:]+)/(?P<tool>[^\s:]+): "
    r"(?P<outcome>ok|refused|failed)\b(?P<rest>[^\n]*)"
)
_DIAL_CALLER = re.compile(r"(?:^|\s)caller=(?P<caller>\S+)\s*$")
_DIAL_FIELD = re.compile(r"(?:^|\s)(?P<key>effect|requester|grant)=(?P<value>\S+)")


def live_name(tool: str) -> str:
    """The SDK live name of a reference connector tool, as ``approval_required_tools`` holds it."""

    return f"mcp__{REFERENCE_CONNECTOR}__{tool}"


PROTECTED_LIVE_NAMES = tuple(live_name(tool) for tool in PROTECTED_TOOLS)


# --- pure checks --------------------------------------------------------------------


def gated_tools_from_env(value: str | None) -> list[str]:
    """The caller proxy's gated tool patterns from its ``CURIE_CALLER_PROXY_GATED_TOOLS``.

    Absent or empty means none, as the proxy reads it. Anything but a JSON
    list of strings is a :class:`HarnessError`: the proxy would not start.
    """

    if not value:
        return []
    try:
        parsed = json.loads(value)
    except ValueError:
        raise HarnessError(f"{GATED_TOOLS_ENV} is not JSON") from None
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise HarnessError(f"{GATED_TOOLS_ENV} is not a JSON list of strings")
    return list(parsed)


def require_ungated(patterns: Sequence[str], connector: str, tools: Sequence[str]) -> None:
    """No gated pattern names any of ``tools``, by the proxy's own match.

    The proxy gates a call when ``<connector>/<tool>`` or
    ``mcp__<connector>__<tool>`` matches a pattern, case-sensitively
    (``server._is_gated``). The proof needs the protected tools held for
    approval only by the runner, so the proxy attests them as ungated calls.
    """

    for tool in tools:
        names = (f"{connector}/{tool}", f"mcp__{connector}__{tool}")
        for pattern in patterns:
            check(
                not any(fnmatchcase(name, pattern) for name in names),
                f"the caller proxy gates {connector}/{tool} by pattern {pattern!r}; the proof "
                "needs the protected tools absent from the proxy's gated list",
            )


def _within(entry: Mapping[str, Any], since: float, until: float | None) -> bool:
    """An entry stamped in ``[since, until]``; an unstamped entry always counts."""

    at = entry.get("at_epoch")
    if not isinstance(at, (int, float)):
        return True
    return float(at) >= since and (until is None or float(at) <= until)


def _shown_effects(effects: Iterable[Mapping[str, Any]]) -> list[str]:
    return [
        f"{e.get('tool')} outcome={e.get('outcome')} requester={e.get('requester')} "
        f"grant={e.get('grant')}"
        for e in effects
    ]


def _shown_decisions(decisions: Iterable[Mapping[str, Any]]) -> list[str]:
    return [
        f"{d.get('outcome')} {d.get('method')} grant={d.get('grant')} attested={d.get('attested')}"
        for d in decisions
    ]


def _one_effect(effects: Sequence[Mapping[str, Any]], tool: str) -> Mapping[str, Any]:
    check(
        len(effects) == 1,
        f"the connector logged {len(effects)} outcome lines for this one {tool} call: "
        f"{_shown_effects(effects)}",
    )
    effect = effects[0]
    check(
        effect.get("tool") == tool,
        f"the connector's one outcome line is for {effect.get('tool')}, not {tool}",
    )
    return effect


def require_unattested(
    decisions: Sequence[Mapping[str, Any]],
    effects: Sequence[Mapping[str, Any]],
    *,
    tool: str,
    outcome: str,
    since: float,
    until: float | None = None,
) -> dict[str, Any]:
    """One ``tool`` call forwarded unattested and the connector answered ``outcome``.

    ``decisions`` are the reference connector's caller proxy decisions and
    ``effects`` its outcome lines; only those stamped in ``[since, until]``
    count. Requires an admitted request, no attested request and no grant
    refusal (so nothing was spent or refused for a grant), and exactly one
    connector line, for ``tool``, with ``outcome`` and ``grant=-``: the
    connector saw no attestation. A line without a ``grant`` field comes from a
    connector that predates the field, which cannot prove that.
    """

    window = [d for d in decisions if _within(d, since, until)]
    lines = [e for e in effects if _within(e, since, until)]
    attested = [d for d in window if d.get("attested") is not None]
    check(
        not attested,
        f"the caller proxy attested {len(attested)} request(s) in the window; an unattested "
        f"call is never attested: {_shown_decisions(attested)}",
    )
    refused = [
        d for d in window if d.get("grant") is not None or d.get("outcome") == GRANT_REQUIRED
    ]
    check(
        not refused,
        f"the caller proxy refused a grant check in the window ({_shown_decisions(refused)}); "
        "an unrelated or mismatched call neither spends nor refuses a grant",
    )
    admitted = [d for d in window if d.get("outcome") == "admitted"]
    check(bool(admitted), "the caller proxy logged no admitted request in the window")
    effect = _one_effect(lines, tool)
    check(
        effect.get("outcome") == outcome,
        f"the connector logged {tool} outcome={effect.get('outcome')}, expected {outcome}",
    )
    check(
        effect.get("grant") == "-",
        f"the connector logged grant={effect.get('grant')} for an unattested call, expected "
        "grant=- (None means a connector that predates the field)",
    )
    return {
        "admitted": len(admitted),
        "logged": {
            "tool": effect.get("tool"),
            "outcome": effect.get("outcome"),
            "requester": effect.get("requester"),
            "grant": effect.get("grant"),
        },
    }


def require_attested_effect(
    decisions: Sequence[Mapping[str, Any]],
    effects: Sequence[Mapping[str, Any]],
    *,
    tool: str,
    requester: str,
    spent: Sequence[str],
    outcome: str = OK,
) -> dict[str, Any]:
    """One ``tool`` call the proxy attested, judged by the proxy, the connector and Valkey.

    ``decisions`` and ``effects`` are already limited to the call's window and
    ``spent`` is the grant fingerprints newly spent in it. Requires exactly one
    ``attested=present`` decision and no grant refusal, exactly one new spend,
    and exactly one connector line for ``tool`` ending ``outcome`` (``ok``: the
    effect applied) under ``requester``'s fingerprint whose ``grant=`` is that
    spent grant. ``outcome`` other than ``ok`` is a connector refusal after the
    attestation, such as a ticket refusal.
    """

    attested = [d for d in decisions if d.get("attested") == ATTESTED]
    check(
        len(attested) == 1,
        f"the caller proxy logged {len(attested)} attested=present requests, expected exactly "
        f"one: {_shown_decisions(decisions)}",
    )
    refused = [d for d in decisions if d.get("outcome") == GRANT_REQUIRED]
    check(not refused, f"the caller proxy also refused a grant check: {_shown_decisions(refused)}")
    effect = _one_effect(effects, tool)
    check(
        effect.get("outcome") == outcome,
        f"the connector logged {tool} outcome={effect.get('outcome')}, expected {outcome}",
    )
    check(
        effect.get("requester") == requester,
        f"the connector logged requester={effect.get('requester')}, expected the attested "
        f"requester {requester}",
    )
    check(
        len(spent) == 1,
        f"expected exactly one new connector-grant spend key for this call, Valkey shows "
        f"{len(spent)}: {list(spent)}",
    )
    check(
        effect.get("grant") not in (None, "-") and effect.get("grant") == spent[0],
        f"the connector logged grant={effect.get('grant')}, not the grant the proxy spent "
        f"({spent[0]})",
    )
    return {
        "attested": len(attested),
        "spent": spent[0],
        "logged": {
            "tool": effect.get("tool"),
            "outcome": effect.get("outcome"),
            "requester": effect.get("requester"),
            "grant": effect.get("grant"),
        },
    }


def require_proxy_refused(
    decisions: Sequence[Mapping[str, Any]], effects: Sequence[Mapping[str, Any]], *, reason: str
) -> dict[str, Any]:
    """The proxy refused one call ``grant_required`` for ``reason``, before the connector.

    ``decisions`` and ``effects`` are already limited to the call's window;
    admitted decisions beside the refusal (the dial's ``initialize``) are
    ignored. Requires exactly one ``grant_required`` decision whose ``grant=``
    sub-reason is ``reason``, and no connector outcome line at all.
    """

    refusals = [d for d in decisions if d.get("outcome") == GRANT_REQUIRED]
    check(
        len(refusals) == 1,
        f"expected exactly one {GRANT_REQUIRED} refusal, the caller proxy logged "
        f"{len(refusals)}: {_shown_decisions(decisions)}",
    )
    got = refusals[0].get("grant")
    check(got == reason, f"the {GRANT_REQUIRED} refusal names grant={got}, expected {reason}")
    check(
        not effects,
        f"the connector logged {len(effects)} outcome lines for a refused call, which must "
        f"reach no effect: {_shown_effects(effects)}",
    )
    return {"refusal": GRANT_REQUIRED, "grant": got}


def spent_since(before: Iterable[str], after: Iterable[str]) -> list[str]:
    """The spend fingerprints in ``after`` that ``before`` lacks, sorted and distinct."""

    return sorted(set(after) - set(before))


def require_no_spend(before: Iterable[str], after: Iterable[str]) -> None:
    """No connector grant was spent between the two Valkey snapshots."""

    new = spent_since(before, after)
    check(not new, f"a connector grant was spent: new connector-grant keys {new}")


def fake_dial(texts: Iterable[Any], connector: str, tool: str) -> dict[str, Any]:
    """The one ``connector-tool <connector>/<tool>`` answer line in reply texts, parsed.

    ``... ok <text> caller=<fp>``, ``... refused <code> caller=<fp>`` or
    ``... failed <reason> caller=<fp>`` (runner/src/curie_runner/fake.py).
    Returns ``outcome``, ``code`` (the refusal code or failure reason, None
    for ok), ``text`` (what precedes ``caller=``), ``caller`` and, from an ok
    text, ``effect``, ``requester`` and ``grant``. Exactly one line for the
    pair, or :class:`StepFailed`.
    """

    found = []
    shown = []
    for text in texts:
        if not isinstance(text, str):
            continue
        for match in _DIAL_LINE.finditer(text):
            shown.append(f"{match['connector']}/{match['tool']}: {match['outcome']}")
            if match["connector"] == connector and match["tool"] == tool:
                found.append(match)
    check(
        len(found) == 1,
        f"the reply should carry one `connector-tool {connector}/{tool}` line, it carries "
        f"{len(found)}; its lines: {shown}",
    )
    match = found[0]
    rest = match["rest"].strip()
    caller = _DIAL_CALLER.search(rest)
    body = rest[: caller.start()].strip() if caller is not None else rest
    outcome = match["outcome"]
    words = body.split()
    fields = {item["key"]: item["value"] for item in _DIAL_FIELD.finditer(body)}
    return {
        "connector": connector,
        "tool": tool,
        "outcome": outcome,
        "code": words[0] if outcome != OK and words else None,
        "text": body,
        "caller": None if caller is None else caller["caller"],
        "effect": fields.get("effect"),
        "requester": fields.get("requester"),
        "grant": fields.get("grant"),
    }


def reconciler_setting(deployment: Mapping[str, Any]) -> dict[str, Any]:
    """The worker container's ``CURIE_CONNECTOR_RECONCILE`` in the worker Deployment.

    ``{"set": False}`` when absent, ``{"set": True, "value": <text>}`` for a
    value, ``{"set": True, "reference": True}`` for a ``valueFrom``.
    """

    containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers")
    for container in containers or []:
        if container.get("name") != WORKER_CONTAINER:
            continue
        for entry in container.get("env") or []:
            if entry.get("name") != RECONCILE_ENV:
                continue
            if "value" in entry:
                return {"set": True, "value": str(entry["value"])}
            return {"set": True, "reference": True}
    return {"set": False}


def reconciler_on(setting: Mapping[str, Any]) -> bool:
    """Whether the setting may turn the reconciler on: anything but absent or a false value."""

    if not setting.get("set"):
        return False
    value = setting.get("value")
    return not isinstance(value, str) or value.strip().lower() not in _RECONCILE_OFF


def tool_directive(tool: str, arguments: Mapping[str, Any], *, gated: bool = False) -> str:
    """The fake model's directive for one reference connector tool call with ``arguments``.

    ``[fake:connector-tool ...]`` dials the connector directly through the
    runner's hosted entry; ``[fake:gated-connector-tool ...]`` only attempts
    the call through the permission gate and dials nothing. The JSON comes last.
    """

    payload = json.dumps(dict(arguments), separators=(",", ":"), sort_keys=True)
    marker = "fake:gated-connector-tool" if gated else "fake:connector-tool"
    return f"[{marker} {REFERENCE_CONNECTOR} {tool} {payload}]"


def grant_payload(grant: str) -> dict[str, Any]:
    """The claims of a ``ccg.<payload>.<signature>`` grant. Held in memory only."""

    parts = grant.split(".")
    if len(parts) != 3 or parts[0] != "ccg":
        raise HarnessError("the connector tool grant is not a ccg grant")
    try:
        payload = json.loads(b64url_decode(parts[1]))
    except (ValueError, UnicodeDecodeError):
        raise HarnessError("the connector tool grant payload is not JSON") from None
    if not isinstance(payload, dict):
        raise HarnessError("the connector tool grant payload is not an object")
    return payload


def grant_facts(
    grant: str, *, agent: str, tool: str, arguments: Mapping[str, Any]
) -> dict[str, Any]:
    """What a grant names, as evidence: its ``jti`` fingerprint and whether it fits the call.

    The fingerprint is the first 16 hex of the ``jti``'s sha256, which the
    reference connector logs as ``grant=`` and the spend scan reports. The
    arguments compare against the canonical text the worker signs
    (``kernel/claim.py::_connector_tool_grant``). Never the ``jti`` itself.
    """

    payload = grant_payload(grant)
    jti = payload.get("jti")
    if not isinstance(jti, str) or not jti:
        raise HarnessError("the connector tool grant carries no jti")
    canonical = json.dumps(
        dict(arguments), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return {
        "jti_fingerprint": caller_fingerprint(jti),
        "jti_canonical_uuid": _UUID.fullmatch(jti) is not None,
        "agent_matches": payload.get("agent") == agent,
        "connector_matches": payload.get("connector") == REFERENCE_CONNECTOR,
        "tool_matches": payload.get("tool") == tool,
        "arguments_match": payload.get("args") == canonical,
        "exp": payload.get("exp"),
    }


def claim_facts(observed: Mapping[str, Any]) -> dict[str, Any]:
    """An observation's claim and caller token, as evidence: fingerprints, never a principal."""

    raw = observed.get("claims")
    claims: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
    principal = claims.get("principal")
    principal_fp = None
    if isinstance(principal, Mapping):
        with contextlib.suppress(KeyError, TypeError):
            principal_fp = principal_fingerprint(principal)
    return {
        "claim": observed.get("claim"),
        "claim_created": observed.get("claim_created"),
        "claim_created_epoch": observed.get("claim_created_epoch"),
        "caller_token_present": observed.get("caller_token_present"),
        "caller_fingerprint": observed.get("caller_fingerprint"),
        "agent": claims.get("agent"),
        "exp": claims.get("exp"),
        "principal_fingerprint": principal_fp,
        "turn": observed.get("turn"),
    }


def gated_approval_for(
    rows: Sequence[Mapping[str, Any]], *, conversation: str, since: float, tool: str
) -> dict[str, Any] | None:
    """The newest approval on ``conversation`` granting ``tool``, created at or after ``since``."""

    found = [
        dict(row)
        for row in rows
        if row.get("conversation_id") == conversation
        and row.get("granted_tool") == tool
        and epoch(str(row.get("created_at", ""))) >= since - CLOCK_SKEW_S
    ]
    return max(found, key=lambda row: epoch(str(row.get("created_at", "")))) if found else None


def entries_between(
    entries: Sequence[Mapping[str, Any]], start: float, end: float
) -> list[dict[str, Any]]:
    """The entries stamped in ``[start, end]``; an unstamped entry always counts."""

    return [dict(entry) for entry in entries if _within(entry, start, end)]


# --- G9 probes ----------------------------------------------------------------------


def probe_cases(nonce: str) -> list[dict[str, Any]]:
    """G9's probes, in order. Each carries the harness's expectation under ``expect``.

    A probe names its ``tool`` and ``arguments``, a fresh ``grant`` (minted
    over ``grant_arguments`` when they differ from the call's), an approval
    ``ticket`` and its tampering, a ``batch`` with a ping, and ``forged``
    attestation header names; or ``replay_of`` an earlier probe, which resends
    that probe's exact call and grant. ``expect.kind`` is ``unattested`` (the
    proxy forwarded without attesting and the connector refused),
    ``attested`` (the proxy spent the grant and attested; the connector
    answered ``outcome``), ``dropped`` (attested and applied, and the
    connection dropped) or ``refused`` (the proxy refused ``reason`` before
    forwarding).
    """

    def note(label: str) -> dict[str, Any]:
        return {"note": f"grant-attestation G9 {label} {nonce}"}

    forged = ("X-Curie-Grant-Verified", "x-curie-grant-verified", "X-CURIE-GRANT-VERIFIED")
    unattested = {"kind": "unattested", "outcome": ATTESTATION_MISSING}
    cases: list[dict[str, Any]] = [
        {
            "id": f"forged-{label}",
            "tool": ATTESTED_TOOL,
            "arguments": note(f"forged {label}"),
            "forged": [name],
            "expect": unattested,
        }
        for label, name in zip(("canonical", "lower", "upper"), forged, strict=True)
    ]
    cases += [
        {
            "id": "forged-repeated",
            "tool": ATTESTED_TOOL,
            "arguments": note("forged repeated"),
            "forged": list(forged),
            "expect": unattested,
        },
        {
            "id": "ticket-valid",
            "tool": APPROVED_TOOL,
            "arguments": note("ticket valid"),
            "ticket": {"tamper": None},
            "grant": True,
            "expect": {"kind": "attested", "outcome": OK},
        },
        {
            "id": "ticket-valid-replay",
            "tool": APPROVED_TOOL,
            "replay_of": "ticket-valid",
            "expect": {"kind": "refused", "reason": GRANT_NOT_SPENT},
        },
        {
            "id": "batch",
            "tool": ATTESTED_TOOL,
            "arguments": note("batch"),
            "grant": True,
            "batch": True,
            "expect": {"kind": "refused", "reason": GRANT_BATCHED_CALL},
        },
        {
            "id": "batch-grant-alone",
            "tool": ATTESTED_TOOL,
            "replay_of": "batch",
            "expect": {"kind": "attested", "outcome": OK},
        },
    ]
    for tamper, outcome in (
        ("requester", TICKET_REQUESTER_MISMATCH),
        ("route", TICKET_SCOPE_MISMATCH),
        ("key", TICKET_INVALID),
        ("digest", TICKET_ARGUMENTS_MISMATCH),
    ):
        cases.append(
            {
                "id": f"ticket-other-{tamper}",
                "tool": APPROVED_TOOL,
                "arguments": note(f"ticket other {tamper}"),
                "ticket": {"tamper": tamper},
                "grant": True,
                "expect": {"kind": "attested", "outcome": outcome},
            }
        )
    cases += [
        {
            "id": "drop-reply",
            "tool": ATTESTED_TOOL,
            "arguments": {**note("drop reply"), "drop_reply": True},
            "grant": True,
            "expect": {"kind": "dropped"},
        },
        {
            "id": "drop-reply-resend",
            "tool": ATTESTED_TOOL,
            "replay_of": "drop-reply",
            "expect": {"kind": "refused", "reason": GRANT_NOT_SPENT},
        },
        {
            "id": "mismatched-arguments",
            "tool": ATTESTED_TOOL,
            "arguments": note("mismatched call"),
            "grant": True,
            "grant_arguments": note("mismatched grant"),
            "expect": unattested,
        },
        {
            "id": "applied-grant-replay",
            "tool": ATTESTED_TOOL,
            "replay_of": "batch-grant-alone",
            "expect": {"kind": "refused", "reason": GRANT_NOT_SPENT},
        },
    ]
    return cases


def judge_probe(
    case: Mapping[str, Any],
    answer: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    effects: Sequence[Mapping[str, Any]],
    *,
    new_spent: Sequence[str],
    requester: str,
) -> dict[str, Any]:
    """One G9 probe against its expectation, from its answer, its log window and Valkey.

    ``decisions`` and ``effects`` are the reference connector's proxy
    decisions and outcome lines inside this probe's window; ``new_spent`` every
    grant fingerprint spent during G9; ``requester`` the fingerprint of the
    principal the captured token names.
    """

    expect = case["expect"]
    kind, tool = expect["kind"], str(case["tool"])
    check("error" not in answer, f"the probe raised {answer.get('error')} in the pod")
    shown = {key: answer.get(key) for key in ("status", "is_error", "text", "refusal", "dropped")}
    held = answer.get("grant_fingerprint")
    if kind == "unattested":
        outcome = expect["outcome"]
        check(
            (answer.get("status"), answer.get("is_error"), answer.get("text"))
            == (200, True, f"refused {outcome}"),
            f"the probe answered {shown}, expected 200 with the tool result refused {outcome}",
        )
        judged = require_unattested(
            decisions, effects, tool=tool, outcome=outcome, since=float("-inf")
        )
        check(
            held is None or held not in new_spent,
            f"the grant {held} was spent although the call forwarded unattested",
        )
    elif kind in ("attested", "dropped"):
        outcome = expect.get("outcome", OK)
        spent = [held] if isinstance(held, str) and held in new_spent else []
        judged = require_attested_effect(
            decisions, effects, tool=tool, requester=requester, spent=spent, outcome=outcome
        )
        if kind == "dropped":
            check(
                answer.get("status") == 502 and answer.get("dropped") is True,
                f"the probe answered {shown}, expected the proxy's 502 for a dropped connection",
            )
        elif outcome == OK:
            text = str(answer.get("text") or "")
            check(
                answer.get("status") == 200
                and answer.get("is_error") is False
                and text.startswith("effect=")
                and text.endswith(f" requester={requester} grant={held}"),
                f"the probe answered {shown}, expected 200 with effect=<n> requester="
                f"{requester} grant={held}",
            )
        else:
            check(
                (answer.get("status"), answer.get("is_error"), answer.get("text"))
                == (200, True, f"refused {outcome}"),
                f"the probe answered {shown}, expected 200 with the tool result refused {outcome}",
            )
    elif kind == "refused":
        check(
            answer.get("status") == 403
            and answer.get("refusal") == GRANT_REQUIRED
            and answer.get("frozen_refusal_body") is True,
            f"the probe answered {shown}, expected the proxy's frozen 403 {GRANT_REQUIRED}",
        )
        judged = require_proxy_refused(decisions, effects, reason=expect["reason"])
    else:
        raise HarnessError(f"probe {case['id']} has an unknown expectation {kind}")
    return {"answer": shown, "grant_fingerprint": held, "judged": judged}


def expected_spends(
    cases: Sequence[Mapping[str, Any]], answers: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    """The grant fingerprints G9 must have spent: one per attested or dropped probe."""

    held = [
        answers.get(str(case["id"]), {}).get("grant_fingerprint")
        for case in cases
        if case["expect"]["kind"] in ("attested", "dropped")
    ]
    return sorted({str(fp) for fp in held if isinstance(fp, str)})


# --- harness steps ------------------------------------------------------------------


def connector_decisions(harness: Harness, agent: str, since: float) -> list[dict[str, Any]]:
    """The reference connector's caller proxy decisions for ``agent`` since ``since``."""

    return [
        decision
        for decision in harness.proxy_decisions(agent, since)
        if decision.get("connector") == REFERENCE_CONNECTOR
    ]


def stored_approval_tools(harness: Harness, agent_id: str) -> list[str] | None:
    """The agent's stored ``approval_required_tools``, read raw so a restore is exact."""

    rows = harness.target.sql(
        (
            "SELECT approval_required_tools FROM curie.agents WHERE id::text = :a",
            {"a": agent_id},
        )
    )[0]
    if len(rows) != 1:
        raise HarnessError(f"expected one agent row for {agent_id}, found {len(rows)}")
    value = rows[0]["approval_required_tools"]
    return [str(item) for item in value] if isinstance(value, list) else None


def serving_gated_tools(harness: Harness, agent: str) -> dict[str, Any]:
    """The gated tool patterns the pod serving the reference connector runs with."""

    serving = harness.connector_pod_env(
        agent, REFERENCE_CONNECTOR, CALLER_PROXY_CONTAINER, (GATED_TOOLS_ENV,)
    )
    return {
        "pod": serving["pod"],
        "patterns": gated_tools_from_env(serving["env"][GATED_TOOLS_ENV]),
    }


def await_gated_approval(
    harness: Harness, agent_id: str, conversation: str, since: float, tool: str, wait: float
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """The approval raised on ``conversation`` for ``tool`` since ``since``, and what was listed."""

    deadline = time.monotonic() + wait
    while True:
        with harness.api() as api:
            rows = list(api.ok("GET", f"/approvals?agent_id={agent_id}&limit=50"))
        found = gated_approval_for(rows, conversation=conversation, since=since, tool=tool)
        if found is not None or time.monotonic() > deadline:
            return found, rows
        time.sleep(3)


def warm_call(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    sender: str,
    conversation: str,
    resume: Mapping[str, Any],
    text: str,
    tool: str,
    wait: float,
) -> dict[str, Any]:
    """One email turn on the resumed conversation, run on the warm resume claim.

    The turn must create no claim and its dial must send the resume claim's
    caller token (by fingerprint), so it ran in the resumed sandbox, whose
    boot env carries the worker-minted grant as a static header; only then is
    it judged. Returns the dial, the reference connector's decisions and
    outcome lines since the send (less the clock skew), and that ``since``.
    """

    sent = harness.send(agent, sender, text, conversation=conversation)
    seen["send"] = send_summary(sent)
    check(sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}")
    event_id = accepted_event_id(sent)
    since = float(sent["sent_at"]) - CLOCK_SKEW_S
    turns, _ = await_turns(harness, agent, conversation, since, [event_id], wait)
    turn = turns[event_id]
    seen["turn"] = turn
    fresh = harness.claims(agent, since)
    seen["claims_since_send"] = [claim["name"] for claim in fresh]
    check(
        not fresh,
        f"the turn ran on new claim(s) {seen['claims_since_send']}, not the warm resume claim "
        f"{resume['claim']}",
    )
    require_delivered(turn)
    dial = fake_dial(turn.get("texts") or [], REFERENCE_CONNECTOR, tool)
    seen["dial"] = dial
    check(
        dial.get("caller") == resume["caller_fingerprint"],
        f"the dial sent caller {dial.get('caller')}, not the resume claim's token "
        f"({resume['caller_fingerprint']}), so it did not run in the resumed sandbox",
    )
    effects = await_effects(harness, agent, since)
    decisions = connector_decisions(harness, agent, since)
    seen.update(effects=effects, decisions=decisions)
    return {"dial": dial, "decisions": decisions, "effects": effects, "since": since}


# --- grant-attestation --------------------------------------------------------------


@scenario("grant-attestation")
def grant_attestation(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0271 decision 7, acceptance items 5 and the carried part of 2.

    G0 setup (reconciler off, flag and approver role on, the protected tools
    held by the runner but absent from the proxy's gated list, the connector's
    ticket settings); G1 a grant-less direct call is refused; G2 the requester
    trips the gate; G3 the approver answers by email and the resume boots
    with the worker-minted grant; G4 an unrelated call leaves it unspent; G5
    the approved call spends it once and is attested; G6 an unrelated call
    after the spend; G7 the replay is refused; G8 changed arguments forward
    unattested; G9 loopback probes of forged headers, batches, tickets,
    uncertain execution and replays; G10 restores everything G0 changed.
    """

    if not isinstance(harness, Harness):
        raise HarnessError(
            "grant-attestation needs the Kubernetes harness: the caller proxy exists only there"
        )
    agent = flag_agent_of(harness, args)
    requester, approver = args.sender.lower(), args.second_sender.lower()
    if requester == approver:
        raise HarnessError("grant-attestation needs two humans; --second-sender equals --sender")
    principal_a, principal_b = principal_for(harness, requester), principal_for(harness, approver)
    requester_fp = principal_fingerprint(principal_a)
    entry = harness.agent_entry(agent)
    agent_id, binding_id = str(entry["agent_id"]), str(entry["binding_id"])
    nonce = uuid.uuid4().hex[:8]
    approved = {"note": f"grant-attestation approved {nonce}"}
    evidence.data.update(
        flag_agent=agent,
        requester=requester,
        approver=approver,
        connector=REFERENCE_CONNECTOR,
        protected_tools=list(PROTECTED_LIVE_NAMES),
        requester_fingerprint=requester_fp,
        approver_fingerprint=principal_fingerprint(principal_b),
        selector_key_id=SELECTOR_KEY_ID,
        ticket_route=TICKET_ROUTE,
        run_nonce=nonce,
    )
    wait = float(args.wait)
    restore: dict[str, Any] = {}
    state: dict[str, Any] = {}
    # The resume claim's caller token lives only in this frame.
    captured: dict[str, Any] = {}

    def text_for(step: str, directive: str) -> str:
        return f"grant-attestation {step} {nonce} {directive}"

    try:
        with evidence.step(
            "G0",
            "setup on default chart values: no connector reconciler, the flag and approver role "
            "on, the protected tools gated by the runner and absent from the proxy's gated list, "
            "and the connector's ticket settings",
        ) as seen:
            setup(
                harness,
                seen,
                agent=agent,
                agent_id=agent_id,
                approver=approver,
                restore=restore,
                state=state,
            )
        require_passed(evidence)

        with evidence.step(
            "G1", f"a grant-less direct {ATTESTED_TOOL} call is forwarded unattested and refused"
        ) as seen:
            before = harness.grant_spends()
            arguments = {"note": f"grant-attestation G1 {nonce}"}
            sent = harness.send(
                agent, requester, text_for("G1", tool_directive(ATTESTED_TOOL, arguments))
            )
            seen["send"] = send_summary(sent)
            check(
                sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}"
            )
            since = float(sent["sent_at"]) - CLOCK_SKEW_S
            observed = harness.observe(agent, wait=wait)
            seen["claim"] = claim_facts(observed)
            check(bool(observed.get("caller_token_present")), "no caller token reached the claim")
            require_principal(observed, principal_a)
            turn = observed.get("turn") or {}
            require_delivered(turn)
            grant, where = harness.connector_tool_grant(str(observed["claim"]))
            seen["grant"] = {"present": grant is not None, "where": where}
            check(grant is None, f"the claim of a turn with no approval carries a grant ({where})")
            dial = fake_dial(turn.get("texts") or [], REFERENCE_CONNECTOR, ATTESTED_TOOL)
            seen["dial"] = dial
            check(
                (dial["outcome"], dial["code"]) == ("refused", ATTESTATION_MISSING),
                f"the call answered {dial['outcome']} {dial['code']}, expected refused "
                f"{ATTESTATION_MISSING}",
            )
            check(
                dial.get("caller") == observed.get("caller_fingerprint"),
                "the dial did not send the claim's caller token",
            )
            effects = await_effects(harness, agent, since)
            decisions = connector_decisions(harness, agent, since)
            seen.update(effects=effects, decisions=decisions)
            seen["unattested"] = require_unattested(
                decisions, effects, tool=ATTESTED_TOOL, outcome=ATTESTATION_MISSING, since=since
            )
            seen["proxy"] = require_proxy_principal(
                decisions, agent=agent, connector=REFERENCE_CONNECTOR, principal=True
            )
            require_no_spend(before, harness.grant_spends())

        with evidence.step(
            "G2",
            f"{requester} trips the runner's gate on {ATTESTED_TOOL}: the approval is raised, its "
            "card reaches the email thread, and nothing is dialled",
        ) as seen:
            before = harness.grant_spends()
            sent = harness.send(
                agent,
                requester,
                text_for("G2", tool_directive(ATTESTED_TOOL, approved, gated=True)),
            )
            seen["send"] = send_summary(sent)
            check(
                sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}"
            )
            conversation = str(sent["conversation_id"])
            since = float(sent["sent_at"]) - CLOCK_SKEW_S
            paused = harness.observe(agent, since=since, wait=wait)
            seen["paused"] = claim_facts(paused)
            check(bool(paused.get("caller_token_present")), "no caller token reached the claim")
            require_principal(paused, principal_a)
            approval, listed = await_gated_approval(
                harness, agent_id, conversation, since, live_name(ATTESTED_TOOL), wait
            )
            check(
                approval is not None,
                f"no approval granting {live_name(ATTESTED_TOOL)} was raised for the "
                f"conversation; the API lists {[approval_summary(row) for row in listed[:5]]}",
            )
            assert approval is not None
            seen["approval"] = {
                **approval_summary(approval),
                "gate_kind": approval.get("gate_kind"),
                "granted_tool": approval.get("granted_tool"),
            }
            check(approval.get("status") == "pending", f"the approval is {approval.get('status')}")
            check(
                approval.get("gate_kind") == PERMISSION_GATE,
                f"the approval's gate is {approval.get('gate_kind')}, not the permission gate",
            )
            approval_id = str(approval["id"])
            card = await_card(harness, agent, conversation, since, approval_id, wait)
            check(card is not None, f"no card for approval {approval_id} reached the email thread")
            seen["card"] = card
            effects = harness.connector_effects(agent, since)
            seen["effects"] = effects
            check(
                not effects, f"the gated attempt reached the connector: {_shown_effects(effects)}"
            )
            require_no_spend(before, harness.grant_spends())
            state.update(conversation=conversation, approval_id=approval_id, paused=paused)
        require_passed(evidence)

        with evidence.step(
            "G3",
            f"{approver} approves by a signed email answer; the resume boots a fresh sandbox "
            f"whose boot env carries the worker-minted grant for exactly the approved call",
        ) as seen:
            before = harness.grant_spends()
            answer: dict[str, Any] = {}
            seen["answer"] = answer
            with harness.api() as api:
                answered_at = answer_by_email(
                    harness,
                    api,
                    answer,
                    binding_id=binding_id,
                    approval_id=state["approval_id"],
                    approver=approver,
                )
            since = answered_at - CLOCK_SKEW_S
            resumed = harness.observe(agent, since=since, wait=wait)
            seen["resume"] = claim_facts(resumed)
            check(bool(resumed.get("caller_token_present")), "no caller token reached the claim")
            seen["replaced"] = require_replaced(state["paused"], resumed, sent_at=answered_at)
            claims = resumed.get("claims") or {}
            check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
            check(claims.get("principal") != principal_b, f"the resumed token names {approver}")
            require_principal(resumed, principal_a)
            resume_event = f"approval-{state['approval_id']}-resolved"
            turns, _ = await_turns(
                harness, agent, state["conversation"], since, [resume_event], wait
            )
            seen["resume_turn"] = turns[resume_event]
            require_delivered(turns[resume_event])
            resume_claim = str(resumed["claim"])
            grant, where = harness.connector_tool_grant(resume_claim)
            seen["grant"] = {"present": grant is not None, "where": where}
            check(grant is not None, f"the resume claim {resume_claim} carries no connector grant")
            assert grant is not None
            facts = grant_facts(grant, agent=agent, tool=ATTESTED_TOOL, arguments=approved)
            del grant
            seen["grant"].update(
                jti_fingerprint=facts["jti_fingerprint"],
                names_the_approved_call=all(
                    facts[key]
                    for key in (
                        "jti_canonical_uuid",
                        "agent_matches",
                        "connector_matches",
                        "tool_matches",
                        "arguments_match",
                    )
                ),
            )
            check(
                seen["grant"]["names_the_approved_call"],
                "the worker-minted grant does not name exactly the approved call: "
                f"{ {key: value for key, value in facts.items() if key != 'jti_fingerprint'} }",
            )
            token = harness.capture_caller_token(resume_claim)
            check(token is not None, f"the resume claim {resume_claim} has no caller token")
            assert token is not None
            check(
                caller_fingerprint(token) == resumed.get("caller_fingerprint"),
                "the captured token is not the one the resume claim observed",
            )
            captured.update(token=token, exp=decode_caller_token(token)["payload"].get("exp"))
            after = harness.grant_spends()
            require_no_spend(before, after)
            check(
                facts["jti_fingerprint"] not in after,
                "the worker-minted grant is already spent before any call presented it",
            )
            state.update(
                grant_fp=facts["jti_fingerprint"],
                resume={
                    "claim": resume_claim,
                    "caller_fingerprint": resumed.get("caller_fingerprint"),
                },
            )
        require_passed(evidence)

        warm = {
            "agent": agent,
            "sender": requester,
            "conversation": state["conversation"],
            "resume": state["resume"],
            "wait": wait,
        }
        grant_fp = str(state["grant_fp"])

        with evidence.step(
            "G4",
            f"warm reuse with the grant unspent: an unrelated {EFFECT_TOOL} call succeeds "
            "unattested and spends nothing",
        ) as seen:
            before = harness.grant_spends()
            arguments = {"note": f"grant-attestation G4 {nonce}"}
            call = warm_call(
                harness,
                seen,
                text=text_for("G4", tool_directive(EFFECT_TOOL, arguments)),
                tool=EFFECT_TOOL,
                **warm,
            )
            unrelated_call_after(
                harness,
                seen,
                call,
                before=before,
                grant_fp=grant_fp,
                agent=agent,
                requester=requester_fp,
            )

        with evidence.step(
            "G5",
            f"the approved {ATTESTED_TOOL} call spends the worker-minted grant once, is attested, "
            "and applies the effect for the requester",
        ) as seen:
            before = harness.grant_spends()
            call = warm_call(
                harness,
                seen,
                text=text_for("G5", tool_directive(ATTESTED_TOOL, approved)),
                tool=ATTESTED_TOOL,
                **warm,
            )
            dial = call["dial"]
            check(
                dial["outcome"] == OK,
                f"the approved call answered {dial['outcome']} {dial['code']}",
            )
            spent = spent_since(before, harness.grant_spends())
            seen["spent"] = spent
            seen["attested"] = require_attested_effect(
                call["decisions"],
                call["effects"],
                tool=ATTESTED_TOOL,
                requester=requester_fp,
                spent=spent,
            )
            check(
                spent == [grant_fp],
                f"the spent grant {spent} is not the worker-minted grant ({grant_fp})",
            )
            check(
                (dial.get("requester"), dial.get("grant")) == (requester_fp, grant_fp),
                f"the tool result names requester={dial.get('requester')} grant="
                f"{dial.get('grant')}, expected {requester_fp} and {grant_fp}",
            )
            seen["proxy"] = require_proxy_principal(
                call["decisions"], agent=agent, connector=REFERENCE_CONNECTOR, principal=True
            )

        with evidence.step(
            "G6",
            f"after the spend, an unrelated {EFFECT_TOOL} call still succeeds unattested and "
            "spends nothing",
        ) as seen:
            before = harness.grant_spends()
            arguments = {"note": f"grant-attestation G6 {nonce}"}
            call = warm_call(
                harness,
                seen,
                text=text_for("G6", tool_directive(EFFECT_TOOL, arguments)),
                tool=EFFECT_TOOL,
                **warm,
            )
            unrelated_call_after(
                harness,
                seen,
                call,
                before=before,
                grant_fp=None,
                agent=agent,
                requester=requester_fp,
            )

        with evidence.step(
            "G7",
            f"replaying the approved call is refused {GRANT_REQUIRED} ({GRANT_NOT_SPENT}) with no "
            "effect",
        ) as seen:
            before = harness.grant_spends()
            call = warm_call(
                harness,
                seen,
                text=text_for("G7", tool_directive(ATTESTED_TOOL, approved)),
                tool=ATTESTED_TOOL,
                **warm,
            )
            dial = call["dial"]
            check(
                (dial["outcome"], dial["code"]) == ("failed", GRANT_REQUIRED),
                f"the replay answered {dial['outcome']} {dial['code']}, expected the proxy's "
                f"{GRANT_REQUIRED}",
            )
            seen["refused"] = require_proxy_refused(
                call["decisions"], call["effects"], reason=GRANT_NOT_SPENT
            )
            require_no_spend(before, harness.grant_spends())

        with evidence.step(
            "G8",
            f"{ATTESTED_TOOL} with changed arguments forwards unattested, spends nothing and is "
            f"refused {ATTESTATION_MISSING}",
        ) as seen:
            before = harness.grant_spends()
            arguments = {"note": f"grant-attestation G8 other {nonce}"}
            call = warm_call(
                harness,
                seen,
                text=text_for("G8", tool_directive(ATTESTED_TOOL, arguments)),
                tool=ATTESTED_TOOL,
                **warm,
            )
            dial = call["dial"]
            check(
                (dial["outcome"], dial["code"]) == ("refused", ATTESTATION_MISSING),
                f"the call answered {dial['outcome']} {dial['code']}, expected refused "
                f"{ATTESTATION_MISSING}",
            )
            seen["unattested"] = require_unattested(
                call["decisions"],
                call["effects"],
                tool=ATTESTED_TOOL,
                outcome=ATTESTATION_MISSING,
                since=call["since"],
            )
            seen["proxy"] = require_proxy_principal(
                call["decisions"], agent=agent, connector=REFERENCE_CONNECTOR, principal=True
            )
            require_no_spend(before, harness.grant_spends())

        with evidence.step(
            "G9",
            "loopback probes with the captured token: forged attestation headers are stripped, "
            "a batch is refused before spend, tickets are checked after attestation, an "
            "uncertain execution is never resent, and a mismatched call does not unlock a replay",
        ) as seen:
            probe_step(
                harness,
                seen,
                agent=agent,
                agent_id=agent_id,
                principal=principal_a,
                other=principal_b,
                requester=requester_fp,
                captured=captured,
                gated=state["gated"],
                nonce=nonce,
            )
    finally:
        captured.clear()
        restore_grant_setup(
            harness, evidence, agent=agent, agent_id=agent_id, restore=restore, state=state
        )


def setup(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    agent_id: str,
    approver: str,
    restore: dict[str, Any],
    state: dict[str, Any],
) -> None:
    """G0. Every change is recorded in ``restore`` before it is made, for G10."""

    require_install_ready(harness, seen, (agent,))
    seen["reference_connector"] = require_reference_connector(harness, (agent,))
    worker = harness.target.kubectl_json("get", "deployment", harness.target.component("worker"))
    setting = reconciler_setting(worker)
    seen["connector_reconciler"] = setting
    check(
        not reconciler_on(setting),
        f"the worker runs with {RECONCILE_ENV} {setting}; the proof needs the proxy to keep its "
        "deploy-time gated list, so run it on default chart values",
    )
    keys = harness.data.setdefault("keys", {})
    check(
        "connectorCaller.signingKey" in harness.data.get("helm_previous", {})
        and bool(keys.get("caller_signing")),
        "the release's caller key pair was not set by `up`, so the harness cannot mint grants "
        "the proxy verifies (G9)",
    )
    check(
        AUTHORITY_GROUP in roster_groups(harness, approver),
        f"{approver} is not in {AUTHORITY_GROUP}, so the approver role cannot admit them",
    )
    with harness.api() as api:
        flag, version = requester_flag(api, agent_id)
        restore["flag"] = bool(flag)
        flag, version = set_requester_flag(api, agent_id, True, what=f"setting {agent}'s flag on")
    seen["flag"] = {"flag": flag, "version": version, "previous": restore["flag"]}
    check(flag is True, f"{agent}'s flag is {flag!r}, not on")
    restore["approver_role"], seen["approver_role"] = set_approver_role(harness, agent_id)

    stored = stored_approval_tools(harness, agent_id)
    restore["approval_tools"] = stored
    wanted = list(stored or [])
    wanted += [name for name in PROTECTED_LIVE_NAMES if name not in wanted]
    restore["approval_tools_written"] = True
    with harness.api() as api:
        status, body = api.call("PATCH", f"/agents/{agent_id}", {"approval_required_tools": wanted})
    readback = stored_approval_tools(harness, agent_id)
    seen["approval_required_tools"] = {"status": status, "before": stored, "after": readback}
    check(
        status == 200,
        f"adding the protected tools to approval_required_tools answered {status}: {body}",
    )
    check(
        all(name in (readback or []) for name in PROTECTED_LIVE_NAMES),
        f"the stored approval_required_tools {readback} lack the protected tools",
    )

    # The run's selector key pair: the seed stays in the private state only.
    directory = harness.state.directory / SELECTOR_KEY_DIR
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    seed, verify = ed25519_pair(directory)
    keys["selector_signing"], keys["selector_verify"] = seed, verify
    harness.state.save()
    previous = harness.connector_server_env(agent, REFERENCE_CONNECTOR, TICKET_ENV)
    restore["ticket_env"] = previous
    wanted_env = {
        "TICKET_PUBLIC_KEY": verify,
        "TICKET_KEY_ID": SELECTOR_KEY_ID,
        "TICKET_ROUTE": TICKET_ROUTE,
        "TICKET_AGENT_ID": agent_id,
    }
    restore["ticket_env_written"] = True
    harness.set_connector_server_env(agent, REFERENCE_CONNECTOR, wanted_env)
    server = connector_server_container(harness.connector_deployment(agent, REFERENCE_CONNECTOR))
    serving = harness.connector_pod_env(agent, REFERENCE_CONNECTOR, server, TICKET_ENV)
    seen["ticket_env"] = {
        "previously_set": sorted(key for key, value in previous.items() if value is not None),
        "pod": serving["pod"],
        "key_id": serving["env"].get("TICKET_KEY_ID"),
        "route": serving["env"].get("TICKET_ROUTE"),
        "agent_id_matches": serving["env"].get("TICKET_AGENT_ID") == agent_id,
        "public_key_matches": serving["env"].get("TICKET_PUBLIC_KEY") == verify,
    }
    check(
        serving["env"] == wanted_env,
        f"the serving connector pod {serving['pod']} does not run with the ticket settings",
    )

    gated = serving_gated_tools(harness, agent)
    seen["gated_tools"] = gated
    state["gated"] = gated["patterns"]
    require_ungated(gated["patterns"], REFERENCE_CONNECTOR, PROTECTED_TOOLS)
    seen["spend_keys"] = len(harness.grant_spends())


def unrelated_call_after(
    harness: Harness,
    seen: dict[str, Any],
    call: Mapping[str, Any],
    *,
    before: Sequence[str],
    grant_fp: str | None,
    agent: str,
    requester: str,
) -> None:
    """G4 and G6: an unrelated ``record_effect`` with the static grant header rides along.

    It succeeds for the requester, the proxy neither attests nor refuses a
    grant, the connector saw no attestation, and nothing is spent; with
    ``grant_fp`` (G4, before the approved call), that grant is still unspent.
    """

    dial = call["dial"]
    check(dial["outcome"] == OK, f"the unrelated call answered {dial['outcome']} {dial['code']}")
    seen["unattested"] = require_unattested(
        call["decisions"], call["effects"], tool=EFFECT_TOOL, outcome=OK, since=call["since"]
    )
    logged = seen["unattested"]["logged"]["requester"]
    check(logged == requester, f"the effect names requester {logged}, expected {requester}")
    seen["proxy"] = require_proxy_principal(
        call["decisions"], agent=agent, connector=REFERENCE_CONNECTOR, principal=True
    )
    after = harness.grant_spends()
    require_no_spend(before, after)
    if grant_fp is not None:
        check(grant_fp not in after, "the worker-minted grant was spent by an unrelated call")


def probe_step(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    agent_id: str,
    principal: Mapping[str, Any],
    other: Mapping[str, Any],
    requester: str,
    captured: Mapping[str, Any],
    gated: Sequence[str],
    nonce: str,
) -> None:
    """G9: the loopback probes, each judged in its own pod-clock window."""

    now_gated = serving_gated_tools(harness, agent)
    seen["gated_tools"] = now_gated
    require_ungated(now_gated["patterns"], REFERENCE_CONNECTOR, PROTECTED_TOOLS)
    check(
        now_gated["patterns"] == list(gated),
        f"the proxy's gated list changed since G0: {now_gated['patterns']}",
    )
    exp = captured.get("exp")
    check(
        isinstance(exp, int) and exp > time.time() + 120,
        f"the captured caller token expires at {exp}, too soon for the probes",
    )
    keys = harness.data["keys"]
    cases = probe_cases(nonce)
    params = {
        "token": captured["token"],
        "caller_seed": keys["caller_signing"],
        "selector_seed": keys["selector_signing"],
        "agent": agent,
        "agent_id": agent_id,
        "tenant_id": harness.data["tenant_id"],
        "connector": REFERENCE_CONNECTOR,
        "ticket_tool": APPROVED_TOOL,
        "key_id": SELECTOR_KEY_ID,
        "route": TICKET_ROUTE,
        "other_route": OTHER_TICKET_ROUTE,
        "requester": dict(principal),
        "other_requester": dict(other),
        "approver": dict(other),
        "gap_s": PROBE_GAP_S,
        "cases": [{key: value for key, value in case.items() if key != "expect"} for case in cases],
    }
    before = harness.grant_spends()
    probed = harness.grant_probes(agent, params)
    del params
    after = harness.grant_spends()
    answers = {str(answer["id"]): answer for answer in probed.get("answers") or []}
    seen["pod"] = probed.get("pod")
    check(
        sorted(answers) == sorted(str(case["id"]) for case in cases),
        f"the pod answered probes {sorted(answers)}, not every case",
    )
    starts = [float(a["started"]) for a in answers.values() if isinstance(a.get("started"), float)]
    since = min([float(probed["started"]), *starts]) - CLOCK_SKEW_S
    decisions = connector_decisions(harness, agent, since)
    effects = harness.connector_effects(agent, since)
    new_spent = spent_since(before, after)
    results: dict[str, Any] = {}
    failures: list[str] = []
    for case in cases:
        answer = answers[str(case["id"])]
        result: dict[str, Any] = {"expect": case["expect"]}
        results[str(case["id"])] = result
        try:
            start = float(answer["started"]) - PROBE_SLACK_S
            end = float(answer["finished"]) + PROBE_SLACK_S
        except (KeyError, TypeError, ValueError):
            result.update(passed=False, failure=f"the probe raised {answer.get('error')}")
            failures.append(f"{case['id']}: the probe raised {answer.get('error')}")
            continue
        window_decisions = entries_between(decisions, start, end)
        window_effects = entries_between(effects, start, end)
        result.update(decisions=window_decisions, effects=window_effects)
        try:
            result.update(
                judge_probe(
                    case,
                    answer,
                    window_decisions,
                    window_effects,
                    new_spent=new_spent,
                    requester=requester,
                ),
                passed=True,
            )
        except AssertionError as exc:
            result.update(passed=False, failure=str(exc), answer=answer_summary(answer))
            failures.append(f"{case['id']}: {exc}")
    seen["probes"] = results
    expected = expected_spends(cases, answers)
    seen["spends"] = {"new": new_spent, "expected": expected}
    check(
        new_spent == expected,
        f"G9 spent grants {new_spent}, expected exactly one per attested probe {expected}",
    )
    check(not failures, f"{len(failures)} probe(s) failed: {failures}")


def answer_summary(answer: Mapping[str, Any]) -> dict[str, Any]:
    keep = ("status", "is_error", "text", "refusal", "frozen_refusal_body", "dropped", "error")
    return {key: answer.get(key) for key in keep}


def restore_grant_setup(
    harness: Harness,
    evidence: Evidence,
    *,
    agent: str,
    agent_id: str,
    restore: Mapping[str, Any],
    state: Mapping[str, Any],
) -> None:
    """G10: put back the agent's tools, the connector env, the flag and the role G0 changed.

    Always runs. Each restore is attempted whatever the others did, and the
    proxy's gated list must be what G0 recorded.
    """

    with evidence.step(
        "G10",
        "restore the agent's approval_required_tools, the connector's ticket settings, the "
        "flag and the approver role; the proxy's gated list is unchanged from G0",
    ) as seen:
        failures: list[str] = []
        if restore.get("approval_tools_written"):
            previous_tools = restore.get("approval_tools")

            def put_back_tools() -> dict[str, Any]:
                with harness.api() as api:
                    status, _ = api.call(
                        "PATCH",
                        f"/agents/{agent_id}",
                        {"approval_required_tools": list(previous_tools or [])},
                    )
                return {"status": status, "readback": stored_approval_tools(harness, agent_id)}

            done = _quietly(put_back_tools)
            seen["approval_required_tools"] = {"restored": previous_tools, **(done or {})}
            if done is None or done["status"] != 200:
                failures.append("approval_required_tools: the PATCH failed")
            elif (done["readback"] or None) != (previous_tools or None):
                failures.append(f"approval_required_tools: stored {done['readback']}")
        if restore.get("ticket_env_written"):
            previous_env: Mapping[str, str | None] = restore["ticket_env"]

            def put_back_env() -> dict[str, Any]:
                harness.set_connector_server_env(agent, REFERENCE_CONNECTOR, previous_env)
                server = connector_server_container(
                    harness.connector_deployment(agent, REFERENCE_CONNECTOR)
                )
                return harness.connector_pod_env(agent, REFERENCE_CONNECTOR, server, TICKET_ENV)

            serving = _quietly(put_back_env)
            seen["ticket_env"] = (
                None
                if serving is None
                else {
                    "pod": serving["pod"],
                    "set": sorted(
                        key for key, value in serving["env"].items() if value is not None
                    ),
                }
            )
            if serving is None:
                failures.append("connector ticket env: the restore failed")
            elif serving["env"] != dict(previous_env):
                failures.append("connector ticket env: the serving pod still differs")
        if "flag" in restore:
            wanted = bool(restore["flag"])

            def put_back_flag() -> dict[str, Any]:
                with harness.api() as api:
                    flag, version = requester_flag(api, agent_id)
                    if bool(flag) != wanted:
                        api.call(
                            "PATCH",
                            f"/agents/{agent_id}",
                            {
                                "connector_requester_identity": wanted,
                                "connector_requester_identity_expected_version": version,
                            },
                        )
                        flag, version = requester_flag(api, agent_id)
                return {"flag": flag, "version": version}

            flagged = _quietly(put_back_flag)
            seen["flag"] = flagged
            if flagged is None or bool(flagged["flag"]) != wanted:
                failures.append(f"flag: not restored to {wanted}")
        previous_role = restore.get("approver_role")
        if previous_role is not None:
            seen["approver_role"], failure, residual = restore_approver_role(
                harness, agent_id, previous_role, step="G0"
            )
            if failure is not None:
                failures.append(failure)
            elif residual is not None:
                seen["residual"] = residual
        if "gated" in state:
            now_gated = _quietly(lambda: serving_gated_tools(harness, agent))
            seen["gated_tools"] = now_gated
            if now_gated is None or now_gated["patterns"] != list(state["gated"]):
                failures.append(
                    f"gated list: {None if now_gated is None else now_gated['patterns']}"
                )
        keys = harness.data.get("keys", {})
        if keys.pop("selector_signing", None) is not None:
            keys.pop("selector_verify", None)
            try:
                harness.state.save()
                seen["selector_seed"] = "removed from the state"
            except OSError as exc:
                failures.append(
                    f"selector seed: the state could not be saved ({type(exc).__name__})"
                )
        with contextlib.suppress(OSError):
            (harness.state.directory / SELECTOR_KEY_DIR).rmdir()
        if failures:
            log(f"grant-attestation restore incomplete: {failures}")
        check(not failures, f"restoring G0's changes failed; repair by hand: {failures}")
