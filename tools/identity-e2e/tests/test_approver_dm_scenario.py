"""The approver-dm scenario's pure parts, judged by the product code that consumes them.

The Slack interaction evidence, chat principal and answer proof are verified
with the API's own principal verifier, the wire verifier and the registered
sender derivation over a registration ``identity_assets`` builds; tickets with
recipient accounts by the API's ticket verifier; a Slack roster entry by the
provider's lookup answer and the API's lookup validator; a DM creation's
approval by the queue wire model; the card store reads by the worker's own
config and store. The ADR 0272 wire and schema models (route mode,
registration request, delivery block) judge their builders once the product
defines them; until then those tests skip, naming the missing contract. The
judges and the evidence checker are checked by tables. No cluster is involved.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import hashlib
import json
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import answer_authority_driver as answers
import approver_dm_driver as dm
import approver_dm_scenario
import approver_ticket_driver as tickets
import check_approver_dm_evidence as checker
import fake_idp
import identity_assets as assets
import identity_scenarios
import pytest
from aci_protocol import ApprovalRequest
from aci_protocol.turn import QueuedTurn, ReplyHandle
from curie_api import approval_principal
from curie_api.approver_tickets import verify_ticket
from curie_api.identity.http_invocation import ReceiverRegistrationV1, registered_sender
from curie_api.identity.slack import SlackEvidence
from curie_api.schemas.http_identity import (
    FreshnessLimits,
    LookupRequestV1,
    ValidatedFacts,
    validate_lookup,
)
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from curie_internal.approver_ticket import execution_arguments_sha256, sign_ticket
from fastapi import HTTPException
from identity_assets import DEFAULT_ROSTER, ISSUER, VECTORS
from identity_e2e import HarnessError
from nacl.signing import SigningKey

TENANT = "6f9619ff-8b86-4d01-b42d-00c04fc964ff"
AGENT = "0f8fad5b-d9cb-469f-a165-70867728950e"
INSTALLATION = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
NAMESPACE_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
IDENTITY_ID = "16fd2706-8baf-433b-82eb-8c7fada847da"
RECEIVER_ID = "886313e1-3b8a-5372-9b90-0c9aee199e5d"
APPROVAL_ID = "a8098c1a-f86e-11da-bd1a-00112444be1e"
RECEIVER_KEY = bytes(range(32, 64))
ATTESTER = "synthetic-attester-secret"
KEY = SigningKey(bytes(range(32)))
KEY_ID = tickets.key_id_for("20261108000000")
NOW = 1_800_000_000
LOADED_VECTORS = json.loads(VECTORS.read_text())
LIMITS = FreshnessLimits(
    max_identity_age_ms=60_000, max_membership_age_ms=60_000, max_clock_skew_ms=5_000
)
NAMESPACE = assets.slack_namespace(NAMESPACE_ID, assets.DM_ACCOUNT)
RECEIVER = {
    "receiver_id": RECEIVER_ID,
    "key": "unused-on-the-host",
    "identity_id": IDENTITY_ID,
    "installation_id": INSTALLATION,
    "account": assets.DM_ACCOUNT,
}


def queued_turn() -> QueuedTurn:
    """An email turn as the runs stream carries it (``test_approver_ticket_scenario``'s)."""

    return QueuedTurn(
        event_id="identity-e2e-event-1",
        conversation_id="<conversation@identity-e2e.example.test>",
        author="alice@example.test",
        text="approver-dm: r02 turn",
        reply_handle=ReplyHandle(
            kind="email",
            channel="agent-a@inbox.example.test",
            placeholder=None,
            endpoint="http://identity-e2e-idp.test.svc:8080/reply/agent-a",
            adapter="identity-e2e",
        ),
        received_at="2026-11-08T00:00:00Z",
    )


def registration() -> ReceiverRegistrationV1:
    # As the API's initializer reads it: the JSON wire form.
    return ReceiverRegistrationV1.model_validate_json(
        json.dumps(
            assets.slack_registration(
                tenant_id=TENANT,
                receiver_id=RECEIVER_ID,
                identity_id=IDENTITY_ID,
                identity_name=assets.DM_IDENTITY,
                installation_id=INSTALLATION,
                namespace_id=NAMESPACE_ID,
                account=assets.DM_ACCOUNT,
                binding_id=str(uuid.uuid4()),
            )
        )
    )


def contract(module: Any, name: str) -> Any:
    """An ADR 0272 product model, or a skip naming it until the product defines it."""

    found = getattr(module, name, None)
    if found is None:
        pytest.skip(f"{module.__name__}.{name} (ADR 0272) is not defined in this checkout yet")
    return found


# --- the scenario, its steps and its checker --------------------------------------------------


def test_the_scenario_is_registered_with_six_steps() -> None:
    assert dm.SCENARIO == "approver-dm"
    assert len(dm.STEP_IDS) == len(set(dm.STEP_IDS)) == 6
    assert dm.SCENARIO in identity_scenarios.SCENARIOS


def passing_evidence() -> dict[str, Any]:
    return {
        "scenario": dm.SCENARIO,
        "passed": True,
        "candidate_commit": "0123456789abcdef0123456789abcdef01234567",
        "steps": [{"id": step, "passed": True} for step in dm.STEP_IDS],
    }


def test_the_checker_passes_complete_evidence() -> None:
    assert checker.judge(passing_evidence()) == []


@pytest.mark.parametrize(
    ("change", "needle"),
    [
        (lambda e: e.update(scenario="approver-ticket-answer"), "not approver-dm"),
        (lambda e: e.update(passed=False, aborted="restore incomplete"), "restore incomplete"),
        (lambda e: e["steps"].pop(), "dm-pending-retention"),
        (lambda e: e["steps"][2].update(passed=False, failure="refused 200"), "refused 200"),
        (lambda e: e.update(candidate_commit=None), "no candidate commit"),
    ],
)
def test_the_checker_names_what_it_observed(change: Any, needle: str) -> None:
    evidence = passing_evidence()
    change(evidence)
    problems = checker.judge(evidence)
    assert problems and any(needle in problem for problem in problems), problems


def test_the_checker_exits_by_the_checkout_comparison(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "dm.json"
    path.write_text(json.dumps(passing_evidence()))

    def clean(args: Any) -> tuple[int, str]:
        return (0, str(tmp_path)) if "rev-parse" in args else (0, "docs/testing/identity-e2e.md\n")

    def touched(args: Any) -> tuple[int, str]:
        return (0, str(tmp_path)) if "rev-parse" in args else (0, "apps/api/src/x.py\n")

    assert checker.main([str(path), "--repo", str(tmp_path)], git=clean) == 0
    assert "ADR 0272 items 1, 3 and 4" in capsys.readouterr().out
    assert checker.main([str(path), "--repo", str(tmp_path)], git=touched) == 1


# --- route maps -------------------------------------------------------------------------------


def test_the_dm_route_map_carries_the_pinned_resolution_beside_the_registration() -> None:
    registration_write = answers.ticket_registration(
        key_id=KEY_ID, public_key="synthetic", role_version=3, issuer=ISSUER
    )
    routes = dm.dm_routes(
        {"other": {"resolution": {"mode": "requesting_surface"}}},
        registration_write,
        identity=assets.DM_IDENTITY,
        approvers={"users": [answers.OPERATOR_USER, assets.SLACK_APPROVER]},
    )
    binding = routes[tickets.TICKET_ROUTE]
    assert binding["resolution"] == {"mode": "approver_dm", "identity": assets.DM_IDENTITY}
    assert binding["approver_ticket"]["key_id"] == KEY_ID
    assert binding["approvers"]["users"] == [answers.OPERATOR_USER, assets.SLACK_APPROVER]
    assert routes["other"] == {"resolution": {"mode": "requesting_surface"}}
    assert routes[tickets.LOOSER_ROUTE] == tickets.REQUESTING_SURFACE
    static = dm.dm_routes({}, None, identity=assets.DM_IDENTITY, route=tickets.LOOSER_ROUTE)
    assert "approver_ticket" not in static[tickets.TICKET_ROUTE]
    assert dm.resolution_of(static, tickets.LOOSER_ROUTE)["mode"] == dm.DM_MODE
    noted = dm.dm_routes({}, registration_write, identity="x", notification=dm.NOTIFY)
    assert noted[tickets.TICKET_ROUTE]["notification"] == dm.NOTIFY


def test_the_dm_route_validates_with_the_api_route_schema() -> None:
    from curie_api.schemas import approvals

    contract(approvals, "ApprovalApproverDmTarget")
    registration_write = answers.ticket_registration(
        key_id=KEY_ID,
        public_key=tickets.b64url(bytes(KEY.verify_key)),
        role_version=3,
        issuer=ISSUER,
    )
    routes = dm.dm_routes({}, registration_write, identity=assets.DM_IDENTITY)
    binding = approvals.ApprovalRouteBinding.model_validate(routes[tickets.TICKET_ROUTE])
    assert binding.model_dump(mode="json")["resolution"] == dm.dm_resolution(assets.DM_IDENTITY)


def test_a_route_write_records_no_body_and_judges_the_code() -> None:
    body = {"detail": [{"msg": f"Value error, {dm.STATIC_ROUTE}", "input": {"public_key": "k"}}]}
    stored = ({"r": 1}, 4)
    write = dm.route_write(
        "static", dm.STATIC_ROUTE, status=422, body=body, before=stored, after=stored
    )
    assert "public_key" not in json.dumps(write)
    assert dm.refused_write(write) == []
    moved = dm.route_write(
        "static", dm.STATIC_ROUTE, status=200, body={}, before=stored, after=({"r": 2}, 5)
    )
    assert len(dm.refused_write(moved)) == 2


# --- tickets and creation requests ------------------------------------------------------------


def account(native: str = assets.SLACK_APPROVER) -> dict[str, Any]:
    return dm.recipient_account(
        installation_id=INSTALLATION, namespace=NAMESPACE, account_id=native
    )


def test_a_ticket_with_recipient_accounts_verifies_in_the_api() -> None:
    item = dm.creation_case("d3", "r02", expected=tickets.expect(201))
    approver = dm.selected(ISSUER, "identity-e2e-bob", [account()])
    token, claims = tickets.mint_ticket(
        item,
        nonces={"d3": tickets.new_nonce()},
        now=NOW,
        sign=sign_ticket,
        digest=execution_arguments_sha256,
        registered_key=KEY,
        other_key=SigningKey.generate,
        tenant_id=TENANT,
        agent_id=AGENT,
        requester={"issuer": ISSUER, "subject": "identity-e2e-alice"},
        approvers=[approver],
        registered={
            "connector": tickets.CONNECTOR,
            "tool": tickets.TOOL,
            "route": tickets.TICKET_ROUTE,
        },
        key_id=KEY_ID,
    )
    verified = verify_ticket(
        public_key=KEY.verify_key,
        key_id=KEY_ID,
        arguments={**item["arguments"], "approval_ticket": token},
        clock=lambda: NOW,
    )
    (selected,) = verified.approvers
    assert selected.subject == "identity-e2e-bob"
    (recipient,) = selected.recipient_accounts or []
    assert (recipient.provider, recipient.installation_id, recipient.account_id) == (
        "slack",
        INSTALLATION,
        assets.SLACK_APPROVER,
    )
    assert recipient.namespace.id == NAMESPACE_ID
    # A person with no accounts is signed without the member, as the schema requires.
    assert "recipient_accounts" not in dm.selected(ISSUER, "identity-e2e-bob")
    assert claims["approvers"][0]["recipient_accounts"][0]["account_id"] == assets.SLACK_APPROVER


def test_a_dm_case_sends_a_null_card_channel_the_queue_model_accepts() -> None:
    item = dm.creation_case("d4", "r03", expected=tickets.expect(201))
    fields = tickets.approval_fields(item, turn=queued_turn(), agent_id=AGENT, granted_arguments={})
    approval = ApprovalRequest.model_validate(fields)
    assert approval.card_channel is None and approval.reply_channel
    ordinary = dm.creation_case("x", "r01", expected=tickets.expect(409), dm=False)
    assert (
        tickets.approval_fields(ordinary, turn=queued_turn(), agent_id=AGENT, granted_arguments={})[
            "card_channel"
        ]
        == queued_turn().reply_handle.channel
    )


def test_the_delivery_judge_and_the_wire_model_agree() -> None:
    wanted = dm.expected_delivery(
        identity=assets.DM_IDENTITY, installation_id=INSTALLATION, recipient=assets.SLACK_APPROVER
    )
    fresh = {"http": 201, "duplicate": False, "approval_status": "pending", "delivery": wanted}
    assert dm.delivery_problems(fresh, wanted) == []
    assert dm.delivery_problems({**fresh, "delivery": None}, wanted)
    assert dm.delivery_problems({**fresh, "http": 200, "duplicate": True}, wanted)
    model = contract(wire, "ApprovalDmDeliveryV1")
    assert model.model_validate(wanted).model_dump(mode="json") == wanted


def test_the_card_registration_body_validates_with_the_internal_wire_model() -> None:
    identity = {
        "schema_version": 1,
        "context": "curie-context-v1.synthetic.synthetic",
        "payload": queued_turn().model_dump_json(),
        "scope": {
            "tenant_id": TENANT,
            "agent_id": AGENT,
            "binding_id": str(uuid.uuid4()),
            "binding_generation": 1,
        },
    }
    body = dm.card_registration_body(
        approval_id=APPROVAL_ID,
        dedupe_key="identity-e2e-event-1",
        identity=identity,
        card_identity=assets.DM_IDENTITY,
        recipient=assets.SLACK_APPROVER,
        channel=dm.DM_REGISTERED,
    )
    assert body["identity"] == identity and body["identity"] is not identity
    model = contract(wire, "ApprovalCardRegistrationRequestV1")
    parsed = model.model_validate_json(json.dumps(body))
    assert parsed.channel == dm.DM_REGISTERED and parsed.approval_id == APPROVAL_ID
    for channel in (dm.DM_UNREGISTERED, dm.DM_ANSWER, dm.DM_RETENTION, dm.DM_CONFLICT):
        model.model_validate_json(json.dumps({**body, "channel": channel}))


# --- Slack answers ----------------------------------------------------------------------------


def signed_click(native: str, channel: str, *, team: str = assets.DM_ACCOUNT) -> tuple[Any, ...]:
    """What the pod builds for one click: the body, the chat principal and the proof."""

    body = dm.answer_body("approved")
    evidence = dm.slack_evidence(native, team)
    principal = approval_principal.mint(
        ATTESTER,
        subject=native,
        kind="chat",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + dm.CHAT_PRINCIPAL_TTL_S,
        actor_channel=channel,
        approval_id=APPROVAL_ID,
        slack=SlackEvidence.model_validate(evidence),
    )
    proof = wire.ApprovalAnswerProofV1.model_validate(
        dm.answer_proof_fields(
            receiver=RECEIVER,
            approval_id=APPROVAL_ID,
            body=body,
            attestation=principal,
            channel=channel,
            evidence=evidence,
        )
    )
    ring = codec.build_identity_keyring("receiver", ((RECEIVER_ID, 1, RECEIVER_KEY),))
    return body, principal, wire.sign_approval_answer_proof(proof, ring), ring


def test_a_scripted_click_verifies_as_the_answer_owner_reads_it() -> None:
    body, principal, token, ring = signed_click(assets.SLACK_APPROVER, dm.DM_ANSWER)
    proof = wire.verify_approval_answer_proof(token, ring)
    assert isinstance(proof, wire.ApprovalAnswerProofV1)
    claims = approval_principal.verify_claims(
        principal, ATTESTER, scope=approval_principal.APPROVE_SCOPE, approval_id=APPROVAL_ID
    )
    assert claims is not None and claims.kind == "chat"
    assert claims.actor_channel == proof.native_channel_id == dm.DM_ANSWER
    signed = SlackEvidence.model_validate(proof.slack.model_dump(mode="json"))
    assert signed == claims.slack and signed.user_id == claims.subject
    assert proof.body_sha256 == hashlib.sha256(body).hexdigest()
    assert (proof.channel_identity_id, proof.provider_installation_id) == (
        IDENTITY_ID,
        INSTALLATION,
    )
    native, namespace = registered_sender(registration(), signed)
    assert native == assets.SLACK_APPROVER and namespace.id == NAMESPACE_ID
    other = codec.build_identity_keyring("receiver", ((RECEIVER_ID, 1, bytes(32)),))
    assert not isinstance(
        wire.verify_approval_answer_proof(token, other), wire.ApprovalAnswerProofV1
    )


def test_a_click_from_another_workspace_derives_no_registered_sender() -> None:
    _, principal, token, ring = signed_click(
        assets.SLACK_APPROVER, dm.DM_ANSWER, team=assets.DM_FOREIGN_ACCOUNT
    )
    proof = wire.verify_approval_answer_proof(token, ring)
    assert isinstance(proof, wire.ApprovalAnswerProofV1)
    signed = SlackEvidence.model_validate(proof.slack.model_dump(mode="json"))
    with pytest.raises(HTTPException):
        registered_sender(registration(), signed)


# --- the scripted directory -------------------------------------------------------------------


def slack_facts(roster: dict[str, Any], native: str) -> Any:
    request = copy.deepcopy(LOADED_VECTORS["request"])
    request["request_id"] = str(uuid.uuid4())
    request["scope"]["expected_issuer"] = ISSUER
    request["scope"]["sender"].update(provider="slack", native_id=native)
    request["scope"]["sender"]["namespace"].update(kind="slack_workspace", key=assets.DM_ACCOUNT)
    parsed = LookupRequestV1.model_validate(request)
    response = fake_idp.answer(json.loads(parsed.model_dump_json()), roster, LOADED_VECTORS)
    return validate_lookup(json.dumps(response).encode(), parsed, LIMITS, now=datetime.now(UTC))


def test_a_slack_user_roster_entry_resolves_to_its_person_in_the_api() -> None:
    entry = answers.entry_of(DEFAULT_ROSTER, "bob@example.test")
    key = assets.SLACK_APPROVER.lower()
    fake_idp.check_entry(key, entry, LOADED_VECTORS)
    roster = copy.deepcopy(DEFAULT_ROSTER)
    roster["senders"][key] = entry
    found = slack_facts(roster, assets.SLACK_APPROVER)
    assert isinstance(found, ValidatedFacts), found
    assert found.identity is not None and found.identity.subject == entry["subject"]
    unknown = slack_facts(roster, assets.SLACK_UNKNOWN)
    assert not isinstance(unknown, ValidatedFacts) or unknown.outcome == "unresolved"


@pytest.mark.parametrize("address", ["U0EXAMPLEDMAPPR", "u0", "u0example dm", "t0exampledm"])
def test_a_roster_key_is_an_email_or_a_lowercased_slack_user(address: str) -> None:
    entry = answers.entry_of(DEFAULT_ROSTER, "bob@example.test")
    with pytest.raises(fake_idp.RosterError):
        fake_idp.check_entry(address, entry, LOADED_VECTORS)


# --- judges -----------------------------------------------------------------------------------


def test_creation_problems_name_rows_spent_nonces_and_mismatches() -> None:
    case = {
        "name": "two-selected",
        "expected": tickets.expect(422, dm.RECIPIENT_COUNT),
        "results": [{"http": 422, "detail": dm.RECIPIENT_COUNT}],
    }
    assert dm.creation_problems([case], {"event": 0}, {"two-selected": 0}) == []
    wrong = {**case, "results": [{"http": 201, "detail": None}]}
    problems = dm.creation_problems([wrong], {"event": 1}, {"two-selected": 1})
    assert len(problems) == 3


def test_registration_and_card_audit_judges() -> None:
    first = {
        "label": "first",
        "http": 200,
        "status": "pending",
        "card_channel": dm.DM_REGISTERED,
        "duplicate": False,
    }
    assert (
        dm.registration_problems(first, http=200, channel=dm.DM_REGISTERED, duplicate=False) == []
    )
    assert dm.registration_problems(first, http=200, channel=dm.DM_REGISTERED, duplicate=True)
    conflict = {"label": "c", "http": 409, "detail": dm.CARD_CONFLICT}
    assert dm.registration_problems(conflict, http=409, detail=dm.CARD_CONFLICT) == []
    audit = {
        "actor": "system",
        "authorized": True,
        "evidence": {
            "kind": dm.CARD_EVIDENCE_KIND,
            "identity": assets.DM_IDENTITY,
            "provider_installation_id": INSTALLATION,
            "namespace_id": NAMESPACE_ID,
            "recipient": assets.SLACK_APPROVER,
            "channel": dm.DM_REGISTERED,
        },
    }
    wanted = {
        "channel": dm.DM_REGISTERED,
        "recipient": assets.SLACK_APPROVER,
        "identity": assets.DM_IDENTITY,
    }
    assert dm.card_audit_problems([audit], **wanted) == []
    assert dm.card_audit_problems([audit, audit], **wanted)
    assert dm.card_audit_problems([{**audit, "authorized": False}], **wanted)


def test_unanswered_and_race_judges() -> None:
    refused = {
        "label": "other-dm",
        "http": 401,
        "status_after": "pending",
        "audit": [{"action": "denied"}],
        "card_channel": dm.DM_ANSWER,
    }
    assert (
        dm.unanswered_problems(refused, statuses=dm.UNAUTHENTICATED, card_channel=dm.DM_ANSWER)
        == []
    )
    leaked = {**refused, "http": 200, "status_after": "approved", "audit": [{"action": "resolved"}]}
    assert len(dm.unanswered_problems(leaked, statuses=dm.REFUSED, card_channel=None)) == 4
    race = {
        "results": [{"http": 200}, {"http": 409, "detail": "approval already resolved"}],
        "status_after": "approved",
        "audit": [{"action": "resolved", "actor": assets.SLACK_APPROVER}],
    }
    assert dm.race_problems(race, actor=assets.SLACK_APPROVER) == []
    both = {**race, "results": [{"http": 200}, {"http": 200}]}
    assert dm.race_problems(both, actor=assets.SLACK_APPROVER)


def card_state(
    card: dict[str, Any],
    *,
    retention: dict[str, Any] | None = None,
    indexed: bool,
    enabled: bool = True,
) -> dict[str, Any]:
    return {
        "card": card,
        "retention": retention or {"exists": False, "pttl": None},
        "indexed": indexed,
        "pending_retention_enabled": enabled,
    }


def test_the_retention_judge() -> None:
    live = {"exists": True, "pttl": -1, "channel": dm.DM_RETENTION, "kind": "slack"}
    pending = card_state(live, indexed=True)
    bounded = card_state({**live, "pttl": (dm.CARD_TTL_S - 5) * 1000}, indexed=False)
    assert dm.settled_reference(bounded) and not dm.settled_reference(pending)
    assert dm.retention_problems(pending, bounded, channel=dm.DM_RETENTION) == []
    deleted = card_state(
        {"exists": False, "pttl": None},
        retention={"exists": True, "pttl": 1000},
        indexed=False,
    )
    assert dm.retention_problems(pending, deleted, channel=dm.DM_RETENTION) == []
    off = card_state({**live, "pttl": dm.CARD_TTL_S * 1000}, indexed=True, enabled=False)
    assert len(dm.retention_problems(off, bounded, channel=dm.DM_RETENTION)) == 2
    endless = card_state({**live, "pttl": (dm.CARD_TTL_S + 3600) * 1000}, indexed=True)
    assert len(dm.retention_problems(pending, endless, channel=dm.DM_RETENTION)) == 2


class _Valkey:
    """The three reads the worker card state makes, over a dict of key -> (value, pttl)."""

    def __init__(self, keys: dict[str, tuple[str, int]], members: set[str]) -> None:
        self.keys, self.members = keys, members

    async def pttl(self, key: str) -> int:
        return self.keys[key][1] if key in self.keys else -2

    async def get(self, key: str) -> str | None:
        return self.keys[key][0] if key in self.keys else None

    async def sismember(self, _name: str, member: str) -> bool:
        return member in self.members


def test_the_worker_card_state_reads_the_workers_own_keys() -> None:
    from curie_worker.approval_cards import ApprovalCardRef, ApprovalCardStore
    from curie_worker.config import WorkerConfig

    config = WorkerConfig()
    ref = ApprovalCardRef(
        channel=dm.DM_RETENTION,
        ts=dm.CARD_TS,
        summary="synthetic",
        kind="slack",
        adapter=assets.DM_IDENTITY,
    )
    redis = _Valkey(
        {config.approval_card_key(APPROVAL_ID): (json.dumps(dataclasses.asdict(ref)), -1)},
        {APPROVAL_ID},
    )
    store = ApprovalCardStore(redis, config)  # type: ignore[arg-type]
    state = asyncio.run(dm._card_state(redis, config, store, APPROVAL_ID))
    assert state["card"] == {
        "exists": True,
        "pttl": -1,
        "channel": dm.DM_RETENTION,
        "kind": "slack",
        "adapter": assets.DM_IDENTITY,
    }
    assert state["retention"] == {"exists": False, "pttl": None} and state["indexed"] is True
    empty = asyncio.run(dm._card_state(_Valkey({}, set()), config, store, APPROVAL_ID))
    assert empty["card"] == {"exists": False, "pttl": None} and empty["indexed"] is False


# --- the pod bootstraps -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bootstrap", "action", "needle"),
    [
        (dm.POD_BOOTSTRAP, "no-such-action", "unknown action"),
        (dm.WORKER_BOOTSTRAP, "no-such-action", "unknown worker action"),
    ],
)
def test_the_bootstraps_load_the_drivers_from_stdin(
    bootstrap: str, action: str, needle: str
) -> None:
    result = subprocess.run(
        [sys.executable, "-c", bootstrap],
        input=json.dumps(dm.pod_params(action)),
        capture_output=True,
        text=True,
        check=True,
        cwd="/",
    )
    assert needle in json.loads(result.stdout.splitlines()[-1])["error"]


# --- the scenario's out-of-band route change ----------------------------------------------


@pytest.mark.parametrize("fails", [False, True])
def test_the_out_of_band_route_is_put_back_however_the_creation_ends(fails: bool) -> None:
    """Step 2 stores an unregistered DM identity directly and must restore it exactly."""

    stored = {
        tickets.TICKET_ROUTE: {
            "resolution": dm.dm_resolution(assets.DM_IDENTITY),
            "approver_ticket": {"key_id": KEY_ID},
        }
    }
    current = {"routes": copy.deepcopy(stored)}
    written: list[Any] = []

    class Target:
        def sql(self, *statements: Any) -> list[Any]:
            ((_, params),) = statements
            current["routes"] = json.loads(params["r"])
            written.append(dm.resolution_of(current["routes"]))
            return [1]

    calls: list[dict[str, Any]] = []

    def ddrive(action: str, **params: Any) -> dict[str, Any]:
        assert action == "dm_create"
        calls.append(params)
        if fails and len(calls) == 2:
            raise HarnessError("the API pod went away")
        cases = [
            {
                "name": case["name"],
                "expected": case["expected"],
                "results": [
                    {"http": case["expected"]["status"][0], "detail": case["expected"]["detail"]}
                ],
            }
            for item in params["batches"]
            for case in item["cases"]
        ]
        return {"cases": cases, "rows": {"refuse": 0}, "nonces": {}}

    scenario: Any = object.__new__(approver_dm_scenario.DmScenario)
    scenario.evidence = identity_scenarios.Evidence(dm.SCENARIO)
    scenario.harness = SimpleNamespace(target=Target())
    scenario.stored_routes = lambda: copy.deepcopy(current["routes"])
    scenario.ensure_active = lambda *parts: {"active": list(parts)}
    scenario.ddrive = ddrive
    scenario.turns = {"r01": {"event_id": "identity-e2e-event-1", "stream_id": "1-0"}}
    scenario.nonces = {}
    scenario.keys = {"k1": {"seed_hex": "00" * 32, "key_id": KEY_ID}}
    scenario.agent_id, scenario.issuer = AGENT, ISSUER
    scenario.subjects = {answers.APPROVER: "identity-e2e-bob", answers.SECOND: "identity-e2e-dana"}
    scenario.dm_installation, scenario.foreign_installation = INSTALLATION, str(uuid.uuid4())
    scenario.dm_namespace = NAMESPACE
    scenario.foreign_namespace = assets.slack_namespace(str(uuid.uuid4()), "T0EXAMPLEFRGN")

    scenario.creation_refusals()

    (step,) = scenario.evidence.data["steps"]
    assert step["passed"] is not fails, step
    assert current["routes"] == stored
    assert written == [
        dm.dm_resolution(dm.UNREGISTERED_IDENTITY),
        stored[tickets.TICKET_ROUTE]["resolution"],
    ]
    first = [case["name"] for item in calls[0]["batches"] for case in item["cases"]]
    assert first == [
        "two-selected",
        "no-account",
        "other-installation",
        "account-of-another-person",
        "account-unresolved",
        "card-channel-named",
    ]
    two = calls[0]["batches"][0]["approvers"]
    assert [len(person["recipient_accounts"]) for person in two] == [1, 1]
    assert "recipient_accounts" not in calls[0]["batches"][1]["approvers"][0]
    assert all(item["nonces"] for item in calls[0]["batches"])
