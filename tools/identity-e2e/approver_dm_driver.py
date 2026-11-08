"""ADR 0272 approver DM delivery through the real creation, registration and answer paths.

One file in three parts, as ``answer_authority_driver.py`` is, so what the
host judges and what the pods send cannot drift apart:

1. On the host, standard library only: the DM route map, the recipient
   accounts a ticket names, the creation case table, the card registration
   body, the Slack interaction evidence and answer proof fields, and the
   judges the ``approver-dm`` scenario in ``approver_dm_scenario.py`` uses.
2. Inside the release's API pod, ``Target.pod_python`` runs
   :data:`POD_BOOTSTRAP`, which loads ``approver_ticket_driver.py``,
   ``answer_authority_driver.py`` and this file (all sent on stdin) and prints
   :func:`dispatch`'s one JSON line. It creates approvals with the creation
   driver, registers a DM card at ``POST /v1/internal/identity/approvals/{id}/card``
   with the worker token, playing the worker's post (a scripted receiver
   proof stands in for Slack), and answers as the dispatcher forwards a Slack
   click: a chat approval principal minted with the API's attester secret and
   an ``ApprovalAnswerProofV1`` signed with the Slack receiver key ``up``
   registered.
3. Inside the release's worker pod, ``Target.worker_python`` runs
   :data:`WORKER_BOOTSTRAP`, which calls :func:`worker_dispatch`: it remembers
   a registered DM card with the worker's own ``ApprovalCardStore`` and
   settings, as the worker's DM post does, and reads the reference's Valkey
   TTL.

Every product import is inside a function. Nothing any part returns carries a
key, token, proof, capsule, nonce or ticket: results are statuses, refusal
codes, ids, synthetic Slack ids, scrubbed audit rows and TTLs.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import answer_authority_driver as answers
import approver_ticket_driver as tickets

SCENARIO = "approver-dm"
# ADR 0272 required evidence items 1, 3 and 4, in run order.
STEP_IDS = (
    "dm-route-write-refusals",
    "dm-creation-refusals",
    "dm-unregistered-unanswerable",
    "dm-card-registration",
    "dm-answer-authority",
    "dm-pending-retention",
)
# What this scenario cannot observe on an unattended install, with the reason.
CARRIED = {
    "worker-dm-post-failure-log": (
        "the fake model has no directive that calls a gated connector tool with an "
        "approval_ticket argument, so no live turn makes the worker create, post or "
        "register a DM approval; creation, registration and the card reference are "
        "played by the harness (runner/src/curie_runner/fake.py)"
    ),
    "genuine-slack-dm-interactions": (
        "ADR 0272 item 2: a real bot installation, Socket Mode button and note modal "
        "clicks and the im:history card read need an authenticated Slack session"
    ),
}

DM_MODE = "approver_dm"
CARD_PATH = tickets.CREATION_PATH + "/{approval_id}/card"
# The synthetic DM channels the harness registers, as Slack's chat.postMessage
# would return them (``^D[A-Z0-9]{8,}$``).
DM_UNREGISTERED = "D0EXAMPLEDM3"
DM_REGISTERED = "D0EXAMPLEDM4"
DM_CONFLICT = "D0EXAMPLEDM9"
DM_ANSWER = "D0EXAMPLEDM5"
DM_RETENTION = "D0EXAMPLEDM6"
DM_ELSEWHERE = "D0EXAMPLEDMX"
COPY_CHANNEL = "C0EXAMPLECOPY"
NOTIFY = {"kind": "slack", "address": "C0EXAMPLENOTE"}
UNREGISTERED_IDENTITY = "identity-e2e-dm-unregistered"
CARD_TS = "1700000000.000100"
CHAT_PRINCIPAL_TTL_S = 120
# The worker's terminal reference retention (approval_cards.DEFAULT_CARD_TTL_S),
# and the clock difference a settled TTL may carry beyond it.
CARD_TTL_S = 14 * 24 * 60 * 60
CARD_TTL_SLACK_S = 300
RETENTION_WAIT_S = 90.0
RETENTION_POLL_S = 1.0

# Refusal codes, as the ADR 0272 plan pins them.
STATIC_ROUTE = "approver_dm_static_route"
WITH_NOTIFICATION = "approver_dm_notification"
RECEIVER_MISSING = "approver_dm_receiver_missing"
RECIPIENT_COUNT = "approver_dm_recipient_count"
ACCOUNT_MISSING = "approver_dm_account_missing"
WRONG_INSTALLATION = "approver_dm_wrong_installation"
ACCOUNT_UNRESOLVED = "approver_dm_account_unresolved"
CARD_CONFLICT = "approval_card_conflict"
CARD_REGISTERED = "card_registered"
CARD_EVIDENCE_KIND = "approver_dm_card"
# An answer that is not proof for this card: no principal (401). An actor the
# answer owner resolves and then refuses: 403.
UNAUTHENTICATED = (401,)
REFUSED = (401, 403)
EMAIL_REFUSED = (401, 403, 404)


# --- routes ---------------------------------------------------------------------------------


def dm_resolution(identity: str) -> dict[str, Any]:
    """The pinned route resolution, exactly."""

    return {"mode": DM_MODE, "identity": identity}


def dm_routes(
    base: Mapping[str, Any],
    registration: Mapping[str, Any] | None,
    *,
    identity: str,
    route: str = tickets.TICKET_ROUTE,
    resolution: Mapping[str, Any] | None = None,
    notification: Mapping[str, Any] | None = None,
    approvers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The owned routes with ``registration`` on the ticket route and a DM resolution on ``route``.

    ``resolution`` replaces the DM object (a requesting-surface repoint);
    ``notification`` and ``approvers`` ride the DM route when given.
    """

    routes = answers.owned_routes(base, registration=registration)
    binding = routes[route]
    binding["resolution"] = copy.deepcopy(
        dict(resolution) if resolution is not None else dm_resolution(identity)
    )
    if notification is not None:
        binding["notification"] = copy.deepcopy(dict(notification))
    if approvers is not None:
        binding["approvers"] = copy.deepcopy(dict(approvers))
    return routes


def resolution_of(routes: Any, route: str = tickets.TICKET_ROUTE) -> Any:
    binding = routes.get(route) if isinstance(routes, Mapping) else None
    return binding.get("resolution") if isinstance(binding, Mapping) else None


# --- tickets and creation cases ----------------------------------------------------------------


def recipient_account(
    *, installation_id: str, namespace: Mapping[str, Any], account_id: str
) -> dict[str, Any]:
    """A ticket's Slack recipient account (``RecipientAccount``)."""

    return {
        "provider": "slack",
        "installation_id": str(installation_id),
        "namespace": {key: namespace[key] for key in ("id", "authority", "kind", "key")},
        "account_id": account_id,
    }


def selected(
    issuer: str, subject: str, accounts: Sequence[Mapping[str, Any]] = ()
) -> dict[str, Any]:
    """One selected approver as the creation driver signs it; accounts only when given."""

    person: dict[str, Any] = {"issuer": issuer, "subject": subject}
    if accounts:
        person["recipient_accounts"] = [copy.deepcopy(dict(item)) for item in accounts]
    return person


def creation_case(
    name: str,
    turn: str,
    *,
    expected: Mapping[str, Any],
    dm: bool = True,
) -> dict[str, Any]:
    """One DM creation on ``turn`` with a fresh ticket nonce named ``name``."""

    return tickets.case(name, turn=turn, ticket=tickets.ticket_spec(name), expected=expected, dm=dm)


def batch(approvers: Sequence[Mapping[str, Any]], *cases: Mapping[str, Any]) -> dict[str, Any]:
    """Cases that share one selected approver list (nonces are filled in by the scenario)."""

    return {
        "approvers": [copy.deepcopy(dict(item)) for item in approvers],
        "nonces": {},
        "cases": [dict(item) for item in cases],
    }


def expected_delivery(*, identity: str, installation_id: str, recipient: str) -> dict[str, Any]:
    """The pinned ``delivery`` block of a DM creation response."""

    return {
        "mode": DM_MODE,
        "identity": identity,
        "provider_installation_id": str(installation_id),
        "recipient": recipient,
    }


# --- card registration ---------------------------------------------------------------------------


def card_registration_body(
    *,
    approval_id: str,
    dedupe_key: str,
    identity: Mapping[str, Any],
    card_identity: str,
    recipient: str,
    channel: str,
    request_id: str | None = None,
) -> dict[str, Any]:
    """An ``ApprovalCardRegistrationRequestV1``: the creation's identity block and the DM."""

    return {
        "schema_version": 1,
        "request_id": request_id or str(uuid.uuid4()),
        "approval_id": str(approval_id),
        "dedupe_key": dedupe_key,
        "identity": copy.deepcopy(dict(identity)),
        "card_identity": card_identity,
        "recipient": recipient,
        "channel": channel,
    }


# --- Slack answers, judged by the product's wire models in the unit tests ------------------------


def answer_body(decision: str) -> bytes:
    """The exact bytes the dispatcher forwards for a button click, which the proof covers."""

    return json.dumps({"decision": decision}, separators=(",", ":")).encode()


def slack_evidence(native: str, team: str) -> dict[str, Any]:
    """A button interaction's Slack evidence: the clicking user and their workspace."""

    return {
        "delivery": "block_actions",
        "user_id": native,
        "event_user_team": None,
        "event_team": None,
        "interaction_user_team_id": team,
        "is_ext_shared_channel": None,
        "enterprise_ids": [],
    }


def answer_proof_fields(
    *,
    receiver: Mapping[str, Any],
    approval_id: str,
    body: bytes,
    attestation: str,
    channel: str,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """An ``ApprovalAnswerProofV1`` a Slack receiver signs for one interaction."""

    return {
        "schema_version": 1,
        "purpose": "approval_answer",
        "operation": "resolve",
        "receiver_id": str(receiver["receiver_id"]),
        "receiver_generation": int(receiver.get("generation", 1)),
        "channel_identity_id": str(receiver["identity_id"]),
        "provider_installation_id": str(receiver["installation_id"]),
        "approval_id": str(approval_id),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "attestation_sha256": hashlib.sha256(attestation.encode("ascii")).hexdigest(),
        "native_channel_id": channel,
        "slack": {**dict(evidence), "enterprise_ids": tuple(evidence.get("enterprise_ids") or ())},
    }


def answer_spec(
    label: str,
    *,
    native: str,
    channel: str,
    receiver: Mapping[str, Any],
    decision: str = "approved",
) -> dict[str, Any]:
    """One scripted Slack click the pod signs and sends; ``receiver`` travels on stdin only."""

    return {
        "label": label,
        "native": native,
        "channel": channel,
        "receiver": dict(receiver),
        "decision": decision,
    }


# --- judges ----------------------------------------------------------------------------------


def route_write(
    label: str,
    code: str | None,
    *,
    status: int,
    body: Any,
    before: tuple[Any, Any],
    after: tuple[Any, Any],
) -> dict[str, Any]:
    """One route write as evidence: status, refusal code, whether it named ``code`` and
    whether the stored map and configuration version are unchanged. Never the body, since
    a validation refusal echoes its input."""

    return {
        "label": label,
        "code": code,
        "http": status,
        "detail": tickets.detail_code(body),
        "names_code": code is not None and code in json.dumps(body, default=str),
        "unchanged": before == after,
        "configuration_version": {"before": before[1], "after": after[1]},
    }


def refused_write(write: Mapping[str, Any]) -> list[str]:
    """A route write refused 422 naming its code, which stored nothing."""

    problems = []
    if write.get("http") != 422 or not write.get("names_code"):
        problems.append(
            f"{write.get('label')}: answered {write.get('http')} {write.get('detail')!r}, "
            f"not 422 naming {write.get('code')}"
        )
    if not write.get("unchanged"):
        problems.append(f"{write.get('label')}: the refused write changed the stored routes")
    return problems


def creation_problems(
    cases: Iterable[Mapping[str, Any]],
    rows: Mapping[str, Any],
    spent: Mapping[str, Any],
) -> list[str]:
    """Every case answered as expected; no row for the refusal turns, no nonce spent."""

    problems = tickets.case_mismatches(cases)
    problems += [f"{count} approval rows stored for {key}" for key, count in rows.items() if count]
    problems += [f"nonce {label} spent {count} times" for label, count in spent.items() if count]
    return problems


def delivery_problems(created: Mapping[str, Any], wanted: Mapping[str, Any]) -> list[str]:
    """A fresh DM creation: 201, pending, not duplicate, exactly the pinned delivery."""

    problems = []
    if created.get("http") != 201 or created.get("duplicate") is not False:
        problems.append(
            f"the creation answered {created.get('http')} {created.get('detail')!r} "
            f"(duplicate {created.get('duplicate')!r}), not a fresh 201"
        )
    if created.get("approval_status") != "pending":
        problems.append(f"the created approval is {created.get('approval_status')!r}")
    if created.get("delivery") != dict(wanted):
        problems.append(f"the delivery block is {created.get('delivery')!r}, not {dict(wanted)}")
    return problems


def registration_problems(
    answer: Mapping[str, Any],
    *,
    http: int,
    status: str = "pending",
    channel: str | None = None,
    duplicate: bool | None = None,
    detail: str | None = None,
) -> list[str]:
    """One registration answer against what it must be."""

    label = answer.get("label")
    problems = []
    if answer.get("http") != http:
        problems.append(
            f"{label}: answered {answer.get('http')} {answer.get('detail')!r}, not {http}"
        )
        return problems
    if detail is not None and answer.get("detail") != detail:
        problems.append(f"{label}: refused {answer.get('detail')!r}, not {detail}")
    if http == 200:
        found = (answer.get("status"), answer.get("card_channel"), answer.get("duplicate"))
        if found != (status, channel, duplicate):
            problems.append(
                f"{label}: answered status, card_channel, duplicate {found}, not "
                f"{(status, channel, duplicate)}"
            )
    return problems


def card_audit_problems(
    audits: Sequence[Mapping[str, Any]], *, channel: str, recipient: str, identity: str
) -> list[str]:
    """Exactly one ``card_registered`` audit row, by the system, naming the DM."""

    if len(audits) != 1:
        return [f"{len(audits)} {CARD_REGISTERED} audit rows, not one"]
    row = audits[0]
    evidence = row.get("evidence") if isinstance(row.get("evidence"), Mapping) else {}
    wanted = {
        "kind": CARD_EVIDENCE_KIND,
        "identity": identity,
        "recipient": recipient,
        "channel": channel,
    }
    found = {key: evidence.get(key) for key in wanted}
    problems = []
    if found != wanted:
        problems.append(f"the {CARD_REGISTERED} evidence reads {found}, not {wanted}")
    if row.get("actor") != "system" or row.get("authorized") is not True:
        problems.append(
            f"the {CARD_REGISTERED} row reads actor {row.get('actor')!r}, "
            f"authorized {row.get('authorized')!r}"
        )
    return problems


def unanswered_problems(
    result: Mapping[str, Any],
    *,
    statuses: Sequence[int],
    card_channel: str | None,
) -> list[str]:
    """An answer with no effect: a refusal status, the row pending on its card, nothing resolved."""

    label = result.get("label")
    problems = []
    if result.get("http") not in statuses:
        problems.append(
            f"{label}: answered {result.get('http')} {result.get('detail')!r}, "
            f"not one of {list(statuses)}"
        )
    if result.get("status_after") != "pending":
        problems.append(f"{label}: the row is {result.get('status_after')!r} after it")
    if any(row.get("action") == answers.RESOLVED for row in result.get("audit") or []):
        problems.append(f"{label}: a resolved audit row was written")
    if result.get("card_channel") != card_channel:
        problems.append(
            f"{label}: card_channel is {result.get('card_channel')!r}, not {card_channel!r}"
        )
    return problems


def race_problems(result: Mapping[str, Any], *, actor: str) -> list[str]:
    """Concurrent valid answers: exactly one decision, by ``actor``, and one resolved row."""

    answered = list(result.get("results") or [])
    won = [item for item in answered if item.get("http") == 200]
    lost = [item for item in answered if item.get("http") != 200]
    problems = []
    if len(won) != 1:
        problems.append(
            f"{len(won)} of {len(answered)} concurrent answers succeeded: "
            f"{[(item.get('http'), item.get('detail')) for item in answered]}"
        )
    if any(item.get("http") not in (409, 503) for item in lost):
        problems.append(
            f"a losing answer was not 409 or 503: "
            f"{[(item.get('http'), item.get('detail')) for item in lost]}"
        )
    if result.get("status_after") != "approved":
        problems.append(f"the row is {result.get('status_after')!r}, not approved")
    resolved = [row for row in result.get("audit") or [] if row.get("action") == answers.RESOLVED]
    if len(resolved) != 1 or resolved[0].get("actor") != actor:
        problems.append(
            f"{len(resolved)} new resolved audit rows "
            f"(actors {[row.get('actor') for row in resolved]}), not one by {actor}"
        )
    return problems


def retention_problems(
    pending: Mapping[str, Any], settled: Mapping[str, Any], *, channel: str
) -> list[str]:
    """The DM card reference: no TTL while pending, a bounded one (or exact deletion) after.

    ``pending`` and ``settled`` are :func:`_worker_card` reads. Settled, the
    card key either carries a TTL of at most the worker's terminal window, or
    is gone while the retention marker carries that bounded TTL (the worker
    deleted the exact reference).
    """

    problems = []
    if pending.get("pending_retention_enabled") is not True:
        problems.append(
            "the worker runs with pending retention off; rerun `up` so it sets "
            "CURIE_APPROVAL_PENDING_RETENTION_ENABLED=true"
        )
    card = pending.get("card") or {}
    if not card.get("exists") or card.get("channel") != channel or card.get("kind") != "slack":
        problems.append(f"the pending DM card reference reads {card}, not the {channel} card")
    if card.get("pttl") != -1:
        problems.append(f"the pending card reference has PTTL {card.get('pttl')!r}, not -1")
    if pending.get("indexed") is not True:
        problems.append("the pending reference is not in the worker's retention index")
    after = settled.get("card") or {}
    bound = (CARD_TTL_S + CARD_TTL_SLACK_S) * 1000
    marker = settled.get("retention") or {}
    if after.get("exists"):
        if not 0 < int(after.get("pttl") or 0) <= bound:
            problems.append(
                f"the settled card reference has PTTL {after.get('pttl')!r}, not bounded by "
                f"{CARD_TTL_S} s"
            )
    elif not (marker.get("exists") and 0 < int(marker.get("pttl") or 0) <= bound):
        problems.append(
            f"the settled card reference is gone and the retention marker reads {marker}, "
            "so no bounded retention was observed"
        )
    if settled.get("indexed") is not False:
        problems.append("the settled reference is still in the worker's retention index")
    return problems


# --- inside the API pod ---------------------------------------------------------------------------

DriverRefusal = tickets.DriverRefusal
_settings = tickets._settings


def _dm_rows(ids: Sequence[str]) -> dict[str, Any]:
    """Each approval's DM facts and its ``card_registered`` audit rows."""

    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> dict[str, Any]:
        found: dict[str, Any] = {}
        for approval_id in ids:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT id::text AS id, status, route, card_channel, reply_kind, "
                            "reply_channel, selection_mode, resolved_by, dedupe_key, "
                            "(resumed_at IS NOT NULL) AS resumed "
                            f'FROM "{SCHEMA}".approvals WHERE id::text = :i'
                        ),
                        {"i": str(approval_id)},
                    )
                )
                .mappings()
                .first()
            )
            audits = await session.execute(
                text(
                    "SELECT action, actor, authorizer, authorized, evidence "
                    f'FROM "{SCHEMA}".approval_audit_entries WHERE approval_id::text = :i '
                    "AND action = :a ORDER BY created_at, id"
                ),
                {"i": str(approval_id), "a": CARD_REGISTERED},
            )
            found[str(approval_id)] = (
                None
                if row is None
                else {
                    **dict(row),
                    "card_audits": [
                        {**dict(item._mapping), "evidence": answers._json_value(item.evidence)}
                        for item in audits
                    ],
                }
            )
        return found

    return dict(tickets._sql(body))


def _rows(params: Mapping[str, Any]) -> dict[str, Any]:
    return {"rows": _dm_rows([str(item) for item in params["ids"]])}


def _create(params: Mapping[str, Any]) -> dict[str, Any]:
    """Each batch through the creation driver, then row counts, spent nonces and DM facts."""

    tickets.APPROVAL_TTL_S = int(params.get("ttl_s") or answers.APPROVAL_TTL_S)
    reports: list[dict[str, Any]] = []
    nonces: dict[str, str] = {}
    for item in params["batches"]:
        nonces.update(item["nonces"])
        labels = tickets.turn_labels(item["cases"])
        result = tickets._create(
            {
                "agent_id": params["agent_id"],
                "key": params["key"],
                "registered": params["registered"],
                "approvers": item["approvers"],
                "nonces": item["nonces"],
                "turns": {label: params["turns"][label] for label in labels},
                "cases": item["cases"],
            }
        )
        reports.extend(result["cases"])
    counts = tickets._counts(
        {
            "agent_id": params["agent_id"],
            "count_dedupe": list(params.get("count_dedupe") or []),
            "count_nonces": {
                label: nonces[label]
                for label in params.get("count_nonces") or []
                if label in nonces
            },
        }
    )
    created = [
        str(answer["id"]) for item in reports for answer in item["results"] if answer.get("id")
    ]
    return {
        "cases": reports,
        "rows": counts["rows"],
        "nonces": counts["nonces"],
        "created": _dm_rows(created),
    }


def _register(params: Mapping[str, Any]) -> dict[str, Any]:
    """Register DM cards for one approval in order, as the worker does after its post."""

    settings = _settings()
    approval_id = str(uuid.UUID(str(params["approval_id"])))
    turn = tickets._load_turns({"turn": params["turn"]})["turn"]
    identity = tickets.creation_request(
        {}, capsule=turn.capsule, payload=turn.payload, scope=turn.context.scope
    )["identity"]
    headers = {answers.WORKER_TOKEN_HEADER: settings.internal_worker_token}
    results = []
    for item in params["requests"]:
        body = card_registration_body(
            approval_id=approval_id,
            dedupe_key=str(params["dedupe_key"]),
            identity=identity,
            card_identity=str(item["card_identity"]),
            recipient=str(item["recipient"]),
            channel=str(item["channel"]),
        )
        status, payload = answers._json_call(
            "POST", CARD_PATH.format(approval_id=approval_id), body, headers
        )
        payload = payload if isinstance(payload, dict) else {}
        results.append(
            {
                "label": item["label"],
                "channel": item["channel"],
                "http": status,
                "detail": None if status == 200 else tickets.detail_code(payload),
                "status": payload.get("status"),
                "card_channel": payload.get("card_channel"),
                "duplicate": payload.get("duplicate"),
                "echo": payload.get("approval_id") == approval_id
                and payload.get("request_id") == body["request_id"],
            }
        )
    return {"results": results, "row": _dm_rows([approval_id]).get(approval_id)}


def _slack_request(spec: Mapping[str, Any], approval_id: str) -> tuple[bytes, dict[str, str]]:
    """The dispatcher's forward of one click: exact body, chat principal, receiver proof."""

    from curie_api import approval_principal
    from curie_api.identity.slack import SlackEvidence
    from curie_internal import identity_approval as wire
    from curie_internal import identity_context as codec
    from curie_internal.sandbox_token import b64url_decode

    settings = _settings()
    receiver = spec["receiver"]
    body = answer_body(str(spec["decision"]))
    evidence = slack_evidence(str(spec["native"]), str(receiver["account"]))
    principal = approval_principal.mint(
        settings.approval_chat_attester_secret,
        subject=str(spec["native"]),
        kind="chat",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + CHAT_PRINCIPAL_TTL_S,
        actor_channel=str(spec["channel"]),
        approval_id=approval_id,
        slack=SlackEvidence.model_validate(evidence),
    )
    proof = wire.ApprovalAnswerProofV1.model_validate(
        answer_proof_fields(
            receiver=receiver,
            approval_id=approval_id,
            body=body,
            attestation=principal,
            channel=str(spec["channel"]),
            evidence=evidence,
        )
    )
    ring = codec.build_identity_keyring(
        "receiver",
        ((str(receiver["receiver_id"]), 1, b64url_decode(str(receiver["key"]))),),
    )
    return body, {
        answers.APPROVAL_PRINCIPAL_HEADER: principal,
        answers.ANSWER_PROOF_HEADER: wire.sign_approval_answer_proof(proof, ring),
        "Content-Type": "application/json",
    }


def _slack_answers(params: Mapping[str, Any]) -> dict[str, Any]:
    """Scripted Slack clicks on one approval, in order or at once behind a barrier."""

    approval_id = str(uuid.UUID(str(params["approval_id"])))
    before = answers._public(approval_id)
    path = answers.RESOLVE_PATH.format(approval_id=approval_id)
    built = []
    for spec in params["answers"]:
        try:
            built.append((spec, *_slack_request(spec, approval_id)))
        except ValueError:
            raise DriverRefusal(f"answer {spec.get('label')}: the click does not encode") from None
    outcomes: list[tuple[int, Any] | None] = [None] * len(built)

    def send(index: int) -> None:
        _, body, headers = built[index]
        outcomes[index] = answers._raw_call("POST", path, body, headers)

    if params.get("concurrent") and len(built) > 1:
        barrier = threading.Barrier(len(built))

        def run(index: int) -> None:
            try:
                barrier.wait(timeout=tickets.BARRIER_TIMEOUT_S)
            except threading.BrokenBarrierError:
                outcomes[index] = (0, {"detail": "barrier_broken"})
                return
            send(index)

        threads = [threading.Thread(target=run, args=(i,)) for i in range(len(built))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=tickets.HTTP_TIMEOUT_S * 2)
    else:
        for index in range(len(built)):
            send(index)
    after = answers._public(approval_id)
    row = _dm_rows([approval_id]).get(approval_id) or {}
    results = []
    for (spec, _, _), answered in zip(built, outcomes, strict=True):
        status, payload = answered or (0, {"detail": "no_response"})
        results.append(
            {
                "label": spec["label"],
                "native": spec["native"],
                "channel": spec["channel"],
                "http": status,
                "detail": None if 200 <= status < 300 else tickets.detail_code(payload),
                "approval_status": payload.get("status") if isinstance(payload, dict) else None,
            }
        )
    return {
        "results": results,
        "status_before": before["status"],
        "status_after": after["status"],
        "audit": answers._new_audit(before, after),
        "card_channel": row.get("card_channel"),
        "resolved_by": row.get("resolved_by"),
        "resumed": bool(row.get("resumed")),
    }


_ACTIONS: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
    "dm_create": _create,
    "dm_rows": _rows,
    "register_card": _register,
    "slack_answers": _slack_answers,
}


def dispatch(params: Any) -> dict[str, Any]:
    """Run one action; the answer and creation drivers' actions pass through."""

    action = params.get("action") if isinstance(params, Mapping) else None
    handler = _ACTIONS.get(action) if isinstance(action, str) else None
    if handler is None:
        return answers.dispatch(params)
    try:
        return handler(params)
    except DriverRefusal as exc:
        return {"error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - product exception text may quote inputs.
        database = tickets._database_message(exc)
        suffix = f": {database}" if database else ""
        return {"error": f"{action} failed with {type(exc).__name__}{suffix}"}


# --- inside the worker pod ------------------------------------------------------------------------


async def _key_state(redis: Any, key: str) -> dict[str, Any]:
    ttl = int(await redis.pttl(key))
    return {"exists": ttl != -2, "pttl": None if ttl == -2 else ttl}


async def _card_state(redis: Any, config: Any, store: Any, approval_id: str) -> dict[str, Any]:
    """The card reference, its retention marker and its index membership, as Valkey holds them."""

    card = await _key_state(redis, config.approval_card_key(approval_id))
    found = await store.read(approval_id) if card["exists"] else None
    ref = found[0] if found else None
    if ref is not None:
        card.update(channel=ref.channel, kind=ref.kind, adapter=ref.adapter)
    return {
        "card": card,
        "retention": await _key_state(redis, config.approval_retention_key(approval_id)),
        "indexed": bool(
            await redis.sismember(config.approval_references_pending_key(), approval_id)
        ),
    }


def settled_reference(state: Mapping[str, Any]) -> bool:
    """Whether a card reference no longer persists without a TTL (bounded or deleted)."""

    card = state.get("card") or {}
    return not card.get("exists") or card.get("pttl") != -1


async def _worker_card_async(params: Mapping[str, Any]) -> dict[str, Any]:
    import asyncio

    from curie_worker.approval_cards import ApprovalCardStore
    from curie_worker.config import WorkerConfig
    from redis.asyncio import Redis

    config = WorkerConfig()
    redis = Redis(**config.valkey_client_kwargs())
    approval_id = str(uuid.UUID(str(params["approval_id"])))
    try:
        store = ApprovalCardStore(redis, config)
        ref = params.get("remember")
        if ref is not None:
            # What the worker's DM post records after a registration answered
            # pending: the returned DM channel on the selected bot identity.
            await store.remember(
                approval_id,
                channel=str(ref["channel"]),
                ts=CARD_TS,
                summary=str(ref["summary"]),
                endpoint=None,
                kind="slack",
                adapter=ref.get("adapter"),
            )
        deadline = time.monotonic() + min(max(float(params.get("wait_s") or 0), 0.0), 100.0)
        while True:
            state = await _card_state(redis, config, store, approval_id)
            if not params.get("until_settled") or settled_reference(state):
                break
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(RETENTION_POLL_S)
        state["pending_retention_enabled"] = bool(config.approval_pending_retention_enabled)
        return state
    finally:
        await redis.aclose()


def _worker_card(params: Mapping[str, Any]) -> dict[str, Any]:
    import asyncio

    return asyncio.run(_worker_card_async(params))


def worker_dispatch(params: Any) -> dict[str, Any]:
    """The worker pod's one action; any failure becomes ``{"error": ...}``."""

    if not isinstance(params, Mapping) or params.get("action") != "card":
        return {"error": "unknown worker action"}
    try:
        return _worker_card(params)
    except Exception as exc:  # noqa: BLE001 - no input is echoed.
        return {"error": f"card failed with {type(exc).__name__}"}


# This file's and the two drivers' text, sent to a pod on stdin. Each bootstrap
# loads the drivers as modules first, so this file's imports find them.
SOURCE = Path(__file__).read_text(encoding="utf-8") if "__file__" in globals() else ""
SOURCE_PARAM = "dm_driver_source"


def _bootstrap(entry: str) -> str:
    return (
        "import json, sys, types\n"
        "P = json.load(sys.stdin)\n"
        "def load(name, source):\n"
        "    module = types.ModuleType(name)\n"
        "    sys.modules[name] = module\n"
        "    exec(compile(source, name + '.py', 'exec'), module.__dict__)\n"
        "    return module\n"
        f"load('approver_ticket_driver', P.pop({answers.TICKETS_PARAM!r}))\n"
        f"load('answer_authority_driver', P.pop({answers.SOURCE_PARAM!r}))\n"
        f"D = load('approver_dm_driver', P.pop({SOURCE_PARAM!r}))\n"
        f"print(json.dumps(D.{entry}(P), sort_keys=True, default=str))\n"
    )


POD_BOOTSTRAP = _bootstrap("dispatch")
WORKER_BOOTSTRAP = _bootstrap("worker_dispatch")


def pod_params(action: str, **params: Any) -> dict[str, Any]:
    """What a bootstrap sends on stdin for one action."""

    return {
        SOURCE_PARAM: SOURCE,
        answers.SOURCE_PARAM: answers.SOURCE,
        answers.TICKETS_PARAM: tickets.SOURCE,
        "action": action,
        **params,
    }
