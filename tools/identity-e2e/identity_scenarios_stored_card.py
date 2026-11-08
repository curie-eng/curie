"""ADR 0271 stored-arguments-card scenario for `curie dev identity-e2e -- prove`.

``stored-arguments-card`` (decision 8, required evidence item 6): the card of a
dynamic approval, the requester's pending notice and the API's
``display_summary`` describe the arguments the approval stores, never the
bundle's display template and never the opaque ``approval_ticket``. It reuses
the ``approver-ticket-creation`` setup (``identity_scenarios.TicketScenario``):
the selector registered on the ``ticket-review`` route for
``netpol-probe``/``search_dashboards``, served by a version of examples/weather
whose gate carries a ``summary`` template holding a per-run template marker and
no placeholders.

1. S0: that setup, and real email turns from the approver and the requester,
   which give both humans their canonical principal rows.
2. S1: a ticket for the requester's call, signed in the API pod with the run's
   selector seed, rides the requester's email turn in
   ``[fake:gated-connector-tool netpol-probe search_dashboards {...}]``; the
   runner's gate holds the call and the worker creates one dynamic pending
   approval on the conversation, read from the API's Postgres (the public read
   model carries neither ``selection_mode`` nor ``granted_arguments``).
3. S2: the card and the pending notice as the worker delivered them to the
   email thread, and ``GET /approvals/<id>``'s ``display_summary``.
4. S3, always: the ``TicketScenario`` restore and its verification, the
   selector seed out of the state, and then
   ``identity_scenarios_attestation.require_private_run`` over the whole evidence.

Registered by ``identity_scenarios`` on import. The ticket stays in memory: it
crosses the API pod's exec pipe and the requester's turn, and the evidence
carries only lengths, fingerprints, counts and booleans, never a ticket, key,
token or principal value.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import approver_ticket_driver as tickets
import identity_scenarios_attestation as attestation
import identity_scenarios_requester as req_helpers
from identity_e2e import Harness, caller_fingerprint, principal_header_value, turn_summary
from identity_scenarios import (
    CLOCK_SKEW_S,
    Evidence,
    TicketScenario,
    check,
    principal_rows_summary,
    require_passed,
    scenario,
    send_summary,
)

SCENARIO = "stored-arguments-card"
LIVE_TOOL = tickets.live_tool(tickets.CONNECTOR, tickets.TOOL)
# The describer's preview bound and truncation notice
# (apps/worker/src/curie_worker/approval_wording.py::describe_approval).
PREVIEW_LIMIT = 2400
TRUNCATION_NOTICE = "… Details exceed this preview; review the original request before approving."
TICKET_PREFIX = "cat1."
# The shortest run of a ticket's characters that counts as showing it.
TICKET_SLICE = 24
_TICKET_LABELS = ("approval_ticket", "approval ticket")
# The pending notice is one fixed line when the card follows in the same thread
# (kernel/constants.py ``_IN_THREAD_APPROVAL_NOTICE``), else it carries the
# summary after this marker (kernel/approval.py ``_pause_for_approval``).
IN_THREAD_NOTICE = "Approval requested. See the card below."
AWAITING_NOTICE = "Awaiting approval ({approval_id})"
# (label, who): the turns whose capsules and principal rows the steps need.
CARD_TURNS = (("approver", "approver"), ("requester", "requester"))
SELECTOR_FILE = "approver-selector.json"
APPROVAL_ROWS = (
    "SELECT id::text AS id, conversation_id, status, selection_mode, route, gate_kind, "
    "granted_tool, granted_arguments, selection_key_id, expires_at FROM curie.approvals "
    "WHERE agent_id::text = :a AND conversation_id = :c ORDER BY created_at"
)
POLL_S = 3.0


# --- pure checks --------------------------------------------------------------------


def _shown(value: Any) -> str:
    """A field value fit for a failure message: short, and never ticket-shaped."""

    text = repr(value)
    return "<withheld>" if TICKET_PREFIX in text else text[:60]


def require_clean(text: str, *, template_marker: str, ticket: str) -> None:
    """``text`` shows neither the template marker, nor the ticket, nor a label for it."""

    check(template_marker not in text, "the text carries the bundle's template marker")
    check(TICKET_PREFIX not in text, f"the text carries the {TICKET_PREFIX} ticket prefix")
    width = min(TICKET_SLICE, len(ticket))
    shown = width > 0 and any(
        ticket[start : start + width] in text for start in range(len(ticket) - width + 1)
    )
    check(not shown, f"the text carries {width} consecutive characters of the ticket")
    folded = text.lower()
    check(
        not any(label in folded for label in _TICKET_LABELS),
        "the text carries a label for the ticket field",
    )


def require_stored_text(
    text: str, *, stored_marker: str, template_marker: str, ticket: str
) -> dict[str, Any]:
    """``text`` shows the stored marker and nothing :func:`require_clean` refuses."""

    check(isinstance(text, str), "the text is not a string")
    check(stored_marker in text, "the text does not carry the stored argument marker")
    require_clean(text, template_marker=template_marker, ticket=ticket)
    return {"length": len(text), "stored_marker": True}


def require_dynamic_row(row: Mapping[str, Any], *, tool: str, stored_marker: str) -> dict[str, Any]:
    """A pending dynamic approval granting ``tool`` over the stored marker and a ticket.

    Returns the row id, its selection mode and the ticket's fingerprint only.
    """

    mode = row.get("selection_mode")
    check(mode == "dynamic", f"the approval's selection_mode is {_shown(mode)}, not dynamic")
    status = row.get("status")
    check(status == "pending", f"the approval's status is {_shown(status)}, not pending")
    granted = row.get("granted_tool")
    check(granted == tool, f"the approval's granted_tool is {_shown(granted)}, not {tool}")
    arguments = row.get("granted_arguments")
    check(
        isinstance(arguments, Mapping),
        f"the approval's granted_arguments is a {type(arguments).__name__}, not an object",
    )
    assert isinstance(arguments, Mapping)
    check(
        arguments.get("query") == stored_marker,
        "the approval's granted_arguments query is not the stored marker",
    )
    ticket = arguments.get("approval_ticket")
    check(isinstance(ticket, str), "the approval's granted_arguments carry no approval_ticket text")
    assert isinstance(ticket, str)
    check(
        ticket.startswith(TICKET_PREFIX),
        f"the approval's granted_arguments approval_ticket is not a {TICKET_PREFIX} ticket",
    )
    return {
        "id": row.get("id"),
        "selection_mode": mode,
        "ticket_fingerprint": caller_fingerprint(ticket),
    }


def require_bounded_preview(text: str, limit: int) -> int:
    """``text`` is at most ``limit`` plus the truncation notice; returns its length."""

    check(isinstance(text, str), "the preview is not a string")
    bound = limit + len(TRUNCATION_NOTICE)
    check(len(text) <= bound, f"the preview is {len(text)} characters, over its bound of {bound}")
    return len(text)


def single_dynamic(
    rows: Iterable[Mapping[str, Any]], *, conversation_id: str, tool: str
) -> dict[str, Any]:
    """The one approval on ``conversation_id`` granting ``tool``; zero or several fail."""

    found = [
        dict(row)
        for row in rows
        if row.get("conversation_id") == conversation_id and row.get("granted_tool") == tool
    ]
    check(
        len(found) == 1,
        f"{len(found)} approvals on the conversation grant {tool}, expected exactly one "
        f"(ids {[row.get('id') for row in found]})",
    )
    return found[0]


def delivered_card(replies: Sequence[Mapping[str, Any]], approval_id: str) -> dict[str, Any] | None:
    """The card for ``approval_id`` among reply events: its text, prompt and event."""

    for reply in replies:
        event = reply.get("event") if isinstance(reply.get("event"), Mapping) else {}
        message = event.get("message") if isinstance(event.get("message"), Mapping) else {}
        interaction = message.get("interaction")
        if (
            isinstance(interaction, Mapping)
            and interaction.get("kind") == "confirm"
            and str(interaction.get("id")) == approval_id
        ):
            return {
                "text": message.get("text"),
                "prompt": interaction.get("prompt"),
                "event": event,
            }
    return None


def pending_notice(replies: Sequence[Mapping[str, Any]], approval_id: str) -> dict[str, Any] | None:
    """The last delivered pending notice for ``approval_id``, with its form, or None.

    ``awaiting`` carries the summary; ``in_thread`` is the fixed line above a
    card that follows in the same thread, so the card carries the summary.
    """

    awaiting = AWAITING_NOTICE.format(approval_id=approval_id)
    found = None
    for reply in replies:
        event = reply.get("event") if isinstance(reply.get("event"), Mapping) else {}
        text = event.get("text")
        if not isinstance(text, str):
            continue
        if awaiting in text:
            found = {"form": "awaiting", "text": text}
        elif IN_THREAD_NOTICE in text:
            found = {"form": "in_thread", "text": text}
    return found


def require_notice(
    notice: Mapping[str, Any], *, stored_marker: str, template_marker: str, ticket: str
) -> dict[str, Any]:
    """A summary-bearing notice must show the stored marker; any notice must stay clean."""

    text = str(notice["text"])
    marks = {"template_marker": template_marker, "ticket": ticket}
    if notice["form"] == "awaiting":
        return {
            "form": "awaiting",
            **require_stored_text(text, stored_marker=stored_marker, **marks),
        }
    require_clean(text, **marks)
    return {"form": "in_thread", "length": len(text), "stored_marker": stored_marker in text}


def gated_directive(arguments: Mapping[str, Any]) -> str:
    """The fake model's directive that trips the runner's gate on the registered pair."""

    payload = json.dumps(dict(arguments))
    return f"[fake:gated-connector-tool {tickets.CONNECTOR} {tickets.TOOL} {payload}]"


# --- install reads ------------------------------------------------------------------


def approval_rows(harness: Harness, agent_id: str, conversation: str) -> list[dict[str, Any]]:
    """The conversation's stored approvals, read in the API pod. Held in memory only."""

    rows = []
    for row in harness.target.sql((APPROVAL_ROWS, {"a": agent_id, "c": conversation}))[0]:
        item = dict(row)
        if isinstance(item.get("granted_arguments"), str):
            try:
                item["granted_arguments"] = json.loads(item["granted_arguments"])
            except ValueError:
                pass
        rows.append(item)
    return rows


def stored_tickets(rows: Iterable[Mapping[str, Any]]) -> list[Any]:
    """The ``approval_ticket`` each row's ``granted_arguments`` stores, to hold as private."""

    return [
        arguments.get("approval_ticket")
        for arguments in (row.get("granted_arguments") for row in rows)
        if isinstance(arguments, Mapping)
    ]


def await_rows(
    harness: Harness, agent_id: str, conversation: str, wait: float
) -> list[dict[str, Any]]:
    """The conversation's approvals once one grants the pair, or when ``wait`` runs out."""

    deadline = time.monotonic() + wait
    while True:
        rows = approval_rows(harness, agent_id, conversation)
        if any(row.get("granted_tool") == LIVE_TOOL for row in rows) or (
            time.monotonic() > deadline
        ):
            return rows
        time.sleep(POLL_S)


def await_delivery(
    harness: Harness, agent: str, conversation: str, since: float, approval_id: str, wait: float
) -> list[dict[str, Any]]:
    """The thread's reply events once the card and a pending notice arrived, or at ``wait``."""

    deadline = time.monotonic() + wait
    while True:
        replies = harness.replies(agent, since, conversation)
        if (
            delivered_card(replies, approval_id) is not None
            and pending_notice(replies, approval_id) is not None
        ) or time.monotonic() > deadline:
            return replies
        time.sleep(POLL_S)


def _fingerprinted(result: Mapping[str, Any], text: str) -> dict[str, Any]:
    return {**result, "fingerprint": caller_fingerprint(text)}


# --- stored-arguments-card ----------------------------------------------------------


@scenario(SCENARIO)
def stored_arguments_card(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0271 decision 8, required evidence item 6, on the approver-ticket-creation setup.

    The agent, ``--sender`` (requester), ``--second-sender`` (selected
    approver) and the bundle prerequisites are ``approver-ticket-creation``'s.
    """

    run = TicketScenario(harness, args, evidence)
    humans = [{**human, "tenant_id": run.tenant_id} for human in (run.requester, run.approver)]
    # Every private value is held as soon as the run has it, so finish() scrubs
    # it out of the evidence even when the run stops before S3.
    evidence.hold_private(
        attestation.private_values(
            [human["subject"] for human in humans],
            [principal_header_value(human) for human in humans],
        )
    )
    tag = uuid.uuid4().hex[:12]
    stored_marker = f"stored-card-{tag}"
    template_marker = f"template-card-{uuid.uuid4().hex[:12]}"
    template = f"identity-e2e bundle display {template_marker}"
    evidence.data.update(
        agent=run.agent,
        requester=run.senders["requester"],
        approver=run.senders["approver"],
        connector=tickets.CONNECTOR,
        tool=LIVE_TOOL,
        route=tickets.TICKET_ROUTE,
        stored_marker=stored_marker,
        template_marker=template_marker,
    )
    wait = float(args.wait)
    state: dict[str, Any] = {}
    # The ticket lives only in this frame.
    secret: dict[str, str] = {}
    try:
        with evidence.step(
            "S0",
            f"the approver-ticket-creation setup, with the served gate's summary template "
            f"holding the template marker; the selector registered on {tickets.TICKET_ROUTE}; "
            "the approver's and requester's canonical principal rows",
        ) as seen:
            run.prepare()
            evidence.hold_private([run.key.get("seed_hex")])
            run.install(summary=template)
            run.send_turns(CARD_TURNS)
            setup = evidence.data.get("setup") or {}
            seen.update(
                key_id=run.key.get("key_id"),
                gated_version=setup.get("gated_version"),
                serving_versions=setup.get("serving_versions"),
            )
            with harness.api() as api:
                _, _, stored = run.configuration(api)
            registered = (stored.get(tickets.TICKET_ROUTE) or {}).get("approver_ticket") or {}
            seen["registration"] = {
                key: registered.get(key) for key in ("connector", "tool", "key_id")
            }
            check(
                registered.get("key_id") == run.key["key_id"],
                f"the run's selector is not registered on {tickets.TICKET_ROUTE}",
            )
            rows = run.drive(
                "principal",
                tenant_id=run.tenant_id,
                issuer=run.requester["issuer"],
                subject=run.requester["subject"],
            )["rows"]
            seen["requester_principal"] = principal_rows_summary(rows)
            check(
                any(row.get("type") == "human" and row.get("status") == "active" for row in rows),
                f"{run.senders['requester']} has no active human principals row after their turn",
            )
        require_passed(evidence)

        with evidence.step(
            "S1",
            f"a ticket signed in the API pod rides the requester's call; the runner's gate "
            f"holds {LIVE_TOOL} and the worker creates one dynamic pending approval over the "
            "stored marker and the ticket",
        ) as seen:
            facts = run.turns["requester"]["capsule_facts"]
            scope = facts.get("scope") or {}
            arguments = {"query": stored_marker}
            signed = run.drive(
                "sign",
                key={"seed_hex": run.key["seed_hex"], "key_id": run.key["key_id"]},
                tenant_id=scope.get("tenant_id"),
                agent_id=scope.get("agent_id"),
                requester=facts.get("canonical"),
                approvers=[run.approver],
                registered={
                    "connector": tickets.CONNECTOR,
                    "tool": tickets.TOOL,
                    "route": tickets.TICKET_ROUTE,
                },
                arguments=arguments,
            )
            secret["ticket"] = str(signed["ticket"])
            evidence.hold_private([secret["ticket"]])
            seen["ticket"] = {
                "fingerprint": caller_fingerprint(secret["ticket"]),
                "length": len(secret["ticket"]),
                "issued_at": signed.get("issued_at"),
                "expires_at": signed.get("expires_at"),
            }
            call = {**arguments, "approval_ticket": secret["ticket"]}
            sent = harness.send(
                run.agent,
                run.senders["requester"],
                f"{SCENARIO} S1 {tag} {gated_directive(call)}",
            )
            seen["send"] = send_summary(sent)
            check(
                sent.get("http_status") == 200,
                f"the email turn answered {sent.get('http_status')}",
            )
            conversation = str(sent["conversation_id"])
            since = float(sent["sent_at"]) - CLOCK_SKEW_S
            rows = await_rows(harness, run.agent_id, conversation, wait)
            evidence.hold_private(stored_tickets(rows))
            seen["rows_on_conversation"] = len(rows)
            if not any(row.get("granted_tool") == LIVE_TOOL for row in rows):
                turn = turn_summary(harness.replies(run.agent, since, conversation))
                seen["turn"] = {"completed": turn["completed"], "outcome": turn["outcome"]}
            row = single_dynamic(rows, conversation_id=conversation, tool=LIVE_TOOL)
            judged = require_dynamic_row(row, tool=LIVE_TOOL, stored_marker=stored_marker)
            seen["approval"] = {
                **judged,
                "route": row.get("route"),
                "gate_kind": row.get("gate_kind"),
                "selection_key_matches": row.get("selection_key_id") == run.key["key_id"],
                "expires_at": row.get("expires_at"),
            }
            check(
                judged["ticket_fingerprint"] == seen["ticket"]["fingerprint"],
                "the stored ticket is not the one the requester's call carried",
            )
            state.update(
                approval_id=str(judged["id"]),
                conversation=conversation,
                since=since,
                expires_at=row.get("expires_at"),
            )
        require_passed(evidence)

        with evidence.step(
            "S2",
            "the card and the requester's pending notice as delivered to the email thread, and "
            "GET /approvals/<id>'s display_summary, show the stored marker, never the template "
            "marker or the ticket, within the describer's preview bound",
        ) as seen:
            approval_id = state["approval_id"]
            marks = {
                "stored_marker": stored_marker,
                "template_marker": template_marker,
                "ticket": secret["ticket"],
            }
            replies = await_delivery(
                harness, run.agent, state["conversation"], state["since"], approval_id, wait
            )
            delivery = req_helpers.approval_card(replies, approval_id) or {}
            seen["card_delivery"] = {key: delivery.get(key) for key in ("at", "event")}
            card = delivered_card(replies, approval_id)
            check(card is not None, f"no card for approval {approval_id} reached the email thread")
            assert card is not None
            text, prompt = card["text"], card["prompt"]
            check(
                isinstance(text, str) and isinstance(prompt, str),
                "the card carries no text or no prompt",
            )
            seen["card"] = {
                "text": _fingerprinted(require_stored_text(text, **marks), text),
                "prompt": _fingerprinted(require_stored_text(prompt, **marks), prompt),
            }
            require_clean(
                json.dumps(card["event"], ensure_ascii=False),
                template_marker=template_marker,
                ticket=secret["ticket"],
            )
            seen["card"]["whole_event_clean"] = True
            notice = pending_notice(replies, approval_id)
            check(notice is not None, f"no pending notice for {approval_id} reached the thread")
            assert notice is not None
            seen["notice"] = _fingerprinted(require_notice(notice, **marks), str(notice["text"]))
            with harness.api() as api:
                status, public = api.call("GET", f"/approvals/{approval_id}")
            check(
                status == 200 and isinstance(public, dict),
                f"GET /approvals/{approval_id} answered {status}",
            )
            assert isinstance(public, dict)
            display = public.get("display_summary")
            check(isinstance(display, str), "the approval has no display_summary")
            assert isinstance(display, str)
            seen["display_summary"] = {
                **_fingerprinted(require_stored_text(display, **marks), display),
                "bounded_length": require_bounded_preview(display, PREVIEW_LIMIT),
                "limit": PREVIEW_LIMIT,
            }
            seen["card_text_equals_display_summary"] = text == display
    finally:
        private = attestation.private_values(
            [human["subject"] for human in humans],
            [principal_header_value(human) for human in humans],
            secret.get("ticket"),
            run.key.get("seed_hex"),
        )
        evidence.hold_private(private)
        secret.clear()
        try:
            restore_step(harness, evidence, run, state, private)
        finally:
            private.clear()


def restore_step(
    harness: Harness,
    evidence: Evidence,
    run: TicketScenario,
    state: Mapping[str, Any],
    secrets: Sequence[str],
) -> None:
    """S3: the ``TicketScenario`` restore, the selector seed removed, then the privacy guard.

    ``secrets`` are every private value the run held: the humans' subjects and
    principal header values, the ticket and the selector seed. The whole
    evidence, this step's included, must carry none of them and no
    principal-shaped object or token envelope.
    """

    with evidence.step(
        "S3",
        "restore: the registration, flag, deployments, routes and approver role put back "
        "and verified as approver-ticket-creation does, and the selector seed removed; the "
        "evidence carries no ticket, key, token or principal value",
    ) as seen:
        run.restore()
        report = evidence.data.get("restore") or {}
        done = list(report.get("done") or [])
        seen["done"] = [item.get("action") for item in done]
        seen["verified"] = next(
            (item.get("result") for item in done if item.get("action") == "verify"), None
        )
        seen["failed"] = list(report.get("failed") or [])
        seed = harness.state.directory / SELECTOR_FILE
        seed.unlink(missing_ok=True)
        seen["selector_seed_removed"] = not seed.exists()
        if state.get("approval_id"):
            seen["left"] = (
                f"approval {state['approval_id']} stays pending until it expires at "
                f"{state.get('expires_at')}; its expiry resumes the paused conversation on "
                "the restored deployment"
            )
        failed = seen["failed"]
        attestation.require_private_run(
            evidence,
            seen,
            secrets=secrets,
            failure="restore incomplete: "
            + "; ".join(f"{item.get('action')}: {item.get('error')}" for item in failed)
            if failed
            else None,
        )
