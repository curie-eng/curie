"""The approver-ticket-answer scenario's pure parts, judged by the product code.

Roster changes are answered by the provider's own lookup code and judged by
the API's ``validate_lookup``; role filters, route bindings, the email answer
proof and the resume check bodies by the models the API and worker parse; the
judges and the evidence checker by tables. No cluster is involved.

The exempt-group separation shape and a separation on a route without a
selector are the 0271b route schema; those cases fail until the API accepts
them, which is the point.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import subprocess
import sys
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import answer_authority_driver as answers
import answer_authority_scenario
import approver_ticket_driver as tickets
import check_answer_evidence as checker
import fake_idp
import identity_scenarios
import pytest
from curie_api.schemas.agent_access import RoleFilterV1
from curie_api.schemas.approvals import ApprovalRouteBinding
from curie_api.schemas.http_identity import (
    FactsRefusal,
    FreshnessLimits,
    LookupRequestV1,
    ValidatedFacts,
    validate_lookup,
)
from curie_internal import identity_context as codec
from curie_internal.identity_approval import (
    ApprovalGrantAuthorityRequestV1,
    EmailApprovalAnswerProofV1,
    sign_email_approval_answer_proof,
    verify_email_approval_answer_proof,
)
from curie_internal.identity_authority import TurnAuthorityRequestV1
from identity_assets import DEFAULT_ROSTER, DOMAIN, ISSUER, MEMBER_GROUP, VECTORS

# The step ids the 0271b task names, in its order.
TASK_STEPS = (
    "answer-selected-approver-allowed",
    "answer-unselected-refused",
    "selection-fixed-after-source-change",
    "ceiling-member-revoked",
    "key-revoked",
    "registration-removed",
    "canonical-inactive",
    "migrated-role-no-legacy-block",
    "raw-account-not-selected",
    "ceiling-widened",
    "separation-default-on-omission",
    "separation-requester-excluded-alias",
    "separation-requester-rejection",
    "separation-exemption-member",
    "separation-exemption-removed",
    "separation-exemption-unavailable",
    "separation-exempt-unselected",
    "separation-operator-opt-out",
    "separation-agent-owned-opt-out-refused",
    "static-exclusion-route",
    "operator-principal-refused",
)
LOADED_VECTORS = json.loads(VECTORS.read_text())
LIMITS = FreshnessLimits(
    max_identity_age_ms=60_000, max_membership_age_ms=60_000, max_clock_skew_ms=5_000
)
NAMESPACE = {
    "id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
    "authority": "",
    "kind": "email_domain",
    "key": DOMAIN,
}
REGISTRATION = {"domains": [NAMESPACE]}
PEOPLE = {
    answers.REQUESTER: "alice@example.test",
    answers.APPROVER: "bob@example.test",
    answers.SECOND: "dana@example.test",
    answers.OUTSIDER: "erin@example.test",
    answers.EXEMPT: "frank@example.test",
}
APPROVAL_ID = "9b2f1c4e-0d6a-4b8e-9f1a-2c3d4e5f6a7b"
BINDING_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
RECEIVER_ID = "16fd2706-8baf-433b-82eb-8c7fada847da"
SCOPE = {
    "tenant_id": "6f9619ff-8b86-4d01-b42d-00c04fc964ff",
    "agent_id": "0f8fad5b-d9cb-469f-a165-70867728950e",
    "binding_id": BINDING_ID,
    "binding_generation": 1,
}


# --- the scenario's step ids --------------------------------------------------------------


def test_the_step_ids_are_the_tasks_and_the_scenario_is_registered() -> None:
    assert answers.STEP_IDS == TASK_STEPS
    assert answers.SCENARIO == "approver-ticket-answer"
    assert len(set(TASK_STEPS)) == len(TASK_STEPS) == 21
    assert identity_scenarios.SCENARIOS[answers.SCENARIO] is (
        identity_scenarios.approver_ticket_answer
    )
    assert checker.STEP_IDS == TASK_STEPS


def step_ids_by_method() -> dict[str, str]:
    """The step id each ``AnswerScenario`` method passes to ``evidence.step``, from its source."""

    tree = ast.parse(Path(answer_authority_scenario.__file__).read_text())
    owner = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "AnswerScenario"
    )
    found: dict[str, str] = {}
    for method in owner.body:
        if not isinstance(method, ast.FunctionDef):
            continue
        constants = {
            target.id: node.value.value
            for node in ast.walk(method)
            if isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        for node in ast.walk(method):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "step"
                and node.args
            ):
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    found[method.name] = first.value
                elif isinstance(first, ast.Name) and first.id in constants:
                    found[method.name] = constants[first.id]
    return found


def test_the_scenario_records_each_task_step_once_in_order() -> None:
    by_method = step_ids_by_method()
    assert sorted(by_method.values()) == sorted(TASK_STEPS)
    tree = ast.parse(Path(answer_authority_scenario.__file__).read_text())
    run = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run"
    )
    called = [
        node.func.attr
        for node in ast.walk(run)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "scenario"
    ]
    assert [by_method[name] for name in called if name in by_method] == list(TASK_STEPS)
    assert called[-1] == "restore"


def test_every_step_has_its_approvals_and_one_turn_each() -> None:
    assert tuple(answers.APPROVALS) == TASK_STEPS
    names = [name for made in answers.APPROVALS.values() for name, _, _ in made]
    assert len(names) == len(set(names)) == answers.TURN_COUNT == 20
    for made in answers.APPROVALS.values():
        for _, kind, parts in made:
            assert set(parts) <= set(answers.ROLES)
            assert bool(parts) is (kind == answers.DYNAMIC)
    assert answers.OWNED_ROUTES[-1] == answers.STATIC_ROUTE
    assert set(tickets.OWNED_ROUTES) < set(answers.OWNED_ROUTES)


# --- people and the roster, answered by the provider and judged by the API -------------------


def test_the_default_roster_has_the_five_people() -> None:
    assert answers.choose_people(DEFAULT_ROSTER, "Alice@example.test", "bob@example.test") == (
        PEOPLE
    )


@pytest.mark.parametrize(
    ("requester", "approver", "needle"),
    [
        ("alice@example.test", "alice@example.test", "must differ"),
        ("carol@example.test", "bob@example.test", "carol@example.test"),
        ("robot@example.test", "bob@example.test", "robot@example.test"),
    ],
)
def test_people_refuse_a_wrong_pair(requester: str, approver: str, needle: str) -> None:
    with pytest.raises(ValueError, match=needle):
        answers.choose_people(DEFAULT_ROSTER, requester, approver)


def test_people_name_the_five_a_short_roster_lacks() -> None:
    roster = copy.deepcopy(DEFAULT_ROSTER)
    del roster["senders"]["frank@example.test"]
    with pytest.raises(ValueError, match="five active verified humans"):
        answers.choose_people(roster, "alice@example.test", "bob@example.test")


def facts(roster: dict[str, Any], native_id: str) -> Any:
    """The provider's answer for one sender, through the API's lookup validator."""

    request = copy.deepcopy(LOADED_VECTORS["request"])
    request["request_id"] = str(uuid.uuid4())
    request["scope"]["expected_issuer"] = ISSUER
    request["scope"]["sender"].update(provider="email", native_id=native_id)
    request["scope"]["sender"]["namespace"].update(kind="email_domain", key=DOMAIN)
    parsed = LookupRequestV1.model_validate(request)
    response = fake_idp.answer(json.loads(parsed.model_dump_json()), roster, LOADED_VECTORS)
    return validate_lookup(
        json.dumps(response).encode(), parsed, LIMITS, now=datetime.now(UTC)
    )


def overridden(address: str, entry: dict[str, Any]) -> dict[str, Any]:
    fake_idp.check_entry(address, entry, LOADED_VECTORS)
    roster = copy.deepcopy(DEFAULT_ROSTER)
    roster["senders"][address] = entry
    return roster


def test_an_exempt_group_reaches_the_api_as_a_canonical_group() -> None:
    entry = answers.with_groups(
        answers.entry_of(DEFAULT_ROSTER, "alice@example.test"), add=[answers.EXEMPT_GROUP]
    )
    result = facts(overridden("alice@example.test", entry), "alice@example.test")
    assert isinstance(result, ValidatedFacts), result
    assert result.groups is not None
    assert {(group.issuer, group.id) for group in result.groups} == {
        (ISSUER, MEMBER_GROUP),
        (ISSUER, answers.EXEMPT_GROUP),
    }
    removed = answers.with_groups(entry, remove=[answers.EXEMPT_GROUP])
    assert removed == answers.entry_of(DEFAULT_ROSTER, "alice@example.test")


def test_an_alias_is_the_same_canonical_person() -> None:
    alias = answers.alias_address(DEFAULT_ROSTER, "alice@example.test")
    assert alias == "alice-alias@example.test"
    entry = answers.entry_of(DEFAULT_ROSTER, "alice@example.test")
    result = facts(overridden(alias, entry), alias)
    assert isinstance(result, ValidatedFacts), result
    assert result.identity is not None
    assert (result.identity.issuer, result.identity.subject) == (ISSUER, "identity-e2e-alice")
    taken = overridden(alias, entry)
    assert answers.alias_address(taken, "alice@example.test") == "alice-alias2@example.test"


def test_a_deactivated_person_answers_inactive() -> None:
    entry = answers.deactivated(answers.entry_of(DEFAULT_ROSTER, "bob@example.test"))
    result = facts(overridden("bob@example.test", entry), "bob@example.test")
    assert isinstance(result, ValidatedFacts), result
    assert result.outcome == "inactive"


def test_an_unavailable_person_is_refused_by_the_api_validator() -> None:
    """The API refuses an unavailable lookup (and answers 503), never validating it."""

    entry = answers.unavailable(answers.entry_of(DEFAULT_ROSTER, "bob@example.test"))
    result = facts(overridden("bob@example.test", entry), "bob@example.test")
    assert isinstance(result, FactsRefusal), result
    assert result.code == "unavailable"


def test_the_outsider_namespace_is_the_registered_email_domain() -> None:
    assert answers.email_namespace(REGISTRATION, "Erin@example.test") == NAMESPACE
    with pytest.raises(ValueError, match="no domain"):
        answers.email_namespace(REGISTRATION, "erin@elsewhere.test")


# --- role filters and route maps, judged by the API's models -----------------------------------


def test_role_filters_validate_with_the_api_model() -> None:
    account = answers.surface_account(NAMESPACE, "Erin@example.test")
    role = RoleFilterV1.model_validate(
        answers.role_filter(
            ISSUER,
            ["identity-e2e-bob", "identity-e2e-alice", "identity-e2e-bob"],
            accounts=[account],
        )
    )
    assert [getattr(entry, "subject", None) for entry in role.entries] == [
        "identity-e2e-alice",
        "identity-e2e-bob",
        None,
    ]
    assert account["native_id"] == "erin@example.test"
    assert answers.role_filter(ISSUER, ["identity-e2e-alice"]) == tickets.approver_filter(
        ISSUER, ["identity-e2e-alice"]
    )


def registration(users: Sequence[str] | None = None) -> dict[str, Any]:
    return answers.ticket_registration(
        key_id="identity-e2e-selector-20261108000000",
        public_key="A" * 43,
        role_version=3,
        issuer=ISSUER,
        users=users,
    )


BASE = {"keep": {"resolution": {"mode": "requesting_surface"}}}


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("plain", {}),
        ("registered", {"registration": registration()}),
        ("users", {"registration": registration(["identity-e2e-bob"])}),
        ("opt-out", {"registration": registration(), "separation": answers.separation(False)}),
        (
            "exempt",
            {
                "registration": registration(),
                "separation": answers.separation(
                    True, exempt_group=answers.EXEMPT_GROUP, issuer=ISSUER
                ),
            },
        ),
        (
            "static",
            {
                "static": answers.static_binding(
                    emails=["Alice@example.test", "bob@example.test"],
                    operators=[answers.OPERATOR_USER],
                )
            },
        ),
    ],
)
def test_every_owned_route_map_validates_with_the_api_binding(
    label: str, kwargs: dict[str, Any]
) -> None:
    routes = answers.owned_routes(BASE, **kwargs)
    assert routes["keep"] == BASE["keep"]
    for name, binding in routes.items():
        ApprovalRouteBinding.model_validate(binding)
        assert name in (*answers.OWNED_ROUTES, "keep"), label


def test_separation_rides_only_the_registered_route() -> None:
    bare = answers.owned_routes(BASE, separation=answers.separation(False))
    assert not answers.has_separation(bare)
    assert not answers.needs_version(BASE, bare)
    registered = answers.owned_routes(BASE, registration=registration())
    assert answers.needs_version(BASE, registered) and answers.needs_version(registered, BASE)
    static = answers.owned_routes(BASE, static=answers.static_binding(emails=[], operators=[]))
    assert answers.has_separation(static) and answers.needs_version(BASE, static)
    assert answers.base_routes(static) == BASE
    ceiling = registered[tickets.TICKET_ROUTE]["approver_ticket"]["ceiling"]
    assert ceiling == {"approver_role_version": 3}
    users = answers.owned_routes(BASE, registration=registration(["identity-e2e-bob"]))
    assert users[tickets.TICKET_ROUTE]["approver_ticket"]["ceiling"]["users"] == [
        {"issuer": ISSUER, "subject": "identity-e2e-bob"}
    ]


def test_an_omitted_separation_means_the_stored_default() -> None:
    omitted = answers.owned_routes(BASE, registration=registration())
    explicit = answers.owned_routes(
        BASE, registration=registration(), separation=answers.separation(True)
    )
    stored = copy.deepcopy(explicit)
    stored[tickets.TICKET_ROUTE]["approver_ticket"]["ceiling"]["approver_role"] = "{}"
    assert answers.owned_summary(omitted) == answers.owned_summary(explicit)
    assert answers.owned_summary(stored) == answers.owned_summary(explicit)
    for changed in (
        answers.owned_routes(
            BASE, registration=registration(), separation=answers.separation(False)
        ),
        answers.owned_routes(BASE, registration=registration(["identity-e2e-bob"])),
        answers.owned_routes(BASE),
    ):
        assert answers.owned_summary(changed) != answers.owned_summary(omitted)


def test_the_readback_names_the_pinned_role_and_the_ceiling() -> None:
    pinned = {
        "schema_version": 1,
        "legacy_state": "migrated",
        "filter": answers.role_filter(
            ISSUER,
            ["identity-e2e-alice"],
            accounts=[answers.surface_account(NAMESPACE, "erin@example.test")],
        ),
    }
    routes = {
        tickets.TICKET_ROUTE: {
            "resolution": {"mode": "requesting_surface"},
            "approver_ticket": {
                "key_id": "k",
                "ceiling": {
                    "approver_role_version": 4,
                    "approver_role": json.dumps(pinned),
                    "users": [{"issuer": ISSUER, "subject": "identity-e2e-bob"}],
                },
            },
            "separation": {"exclude_requester": True},
        }
    }
    assert answers.ticket_readback(routes) == {
        "bound": True,
        "registered": True,
        "has_approvers_block": False,
        "key_id": "k",
        "users": ["identity-e2e-bob"],
        "approver_role_version": 4,
        "pinned_subjects": ["identity-e2e-alice"],
        "pinned_accounts": 1,
        "pinned_legacy_state": "migrated",
        "separation": {"exclude_requester": True},
    }
    assert answers.ticket_readback(routes, answers.STATIC_ROUTE)["bound"] is False


# --- the email answer and the resume checks, judged by the wire models -------------------------


def test_the_answer_proof_covers_the_exact_body_and_verifies() -> None:
    body = answers.answer_body("approved")
    assert body == json.dumps(
        {"decision": "approved", "note": answers.NOTE}, separators=(",", ":")
    ).encode()
    fields = answers.answer_proof_fields(
        receiver_id=RECEIVER_ID,
        receiver_generation=1,
        binding_id=BINDING_ID,
        approval_id=APPROVAL_ID,
        sender="Alice@example.test",
        body=body,
    )
    assert fields["body_sha256"] == hashlib.sha256(body).hexdigest()
    assert fields["sender"] == "alice@example.test"
    proof = EmailApprovalAnswerProofV1.model_validate_json(json.dumps(fields))
    ring = codec.build_identity_keyring("receiver", ((RECEIVER_ID, 1, bytes(range(32))),))
    token = sign_email_approval_answer_proof(proof, ring)
    assert verify_email_approval_answer_proof(token, ring) == proof
    rejected = answers.answer_body("rejected")
    assert hashlib.sha256(rejected).hexdigest() != fields["body_sha256"]


def check_body(stage: str) -> dict[str, Any]:
    return answers.turn_check_body(
        request_id=str(uuid.uuid4()),
        stage=stage,
        context="v1.synthetic-context",
        payload='{"event_id":"approval-x-resolved"}',
        event_id=answers.resume_event_id(APPROVAL_ID),
        scope=answers.check_scope(SCOPE),
        approval_id=APPROVAL_ID,
    )


def test_the_resume_and_grant_checks_validate_with_the_wire_models() -> None:
    turn = TurnAuthorityRequestV1.model_validate_json(json.dumps(check_body(answers.RESUME_STAGE)))
    assert turn.event_id == f"approval-{APPROVAL_ID}-resolved"
    assert turn.stage == "claim_or_resume"
    row = {
        "id": APPROVAL_ID,
        "agent_id": SCOPE["agent_id"],
        "conversation_id": "<c1@identity-e2e.example.test>",
        "gate_kind": "permission",
        "granted_tool": tickets.live_tool(tickets.CONNECTOR, tickets.TOOL),
        "granted_arguments": json.dumps({"query": "x", "approval_ticket": "t", "a": "é"}),
    }
    grant = answers.grant_expectation(row)
    decoded = {**row, "granted_arguments": json.loads(row["granted_arguments"])}
    assert grant == answers.grant_expectation(decoded)
    body = answers.grant_check_body(check_body(answers.GRANT_STAGE), grant)
    parsed = ApprovalGrantAuthorityRequestV1.model_validate_json(json.dumps(body))
    assert parsed.grant.approval_id == APPROVAL_ID
    assert answers.grant_expectation({**row, "granted_arguments": None})[
        "granted_arguments_json"
    ] is None


def test_a_check_scope_reads_an_object_or_a_mapping() -> None:
    class Sealed:
        tenant_id = SCOPE["tenant_id"]
        agent_id = SCOPE["agent_id"]
        binding_id = SCOPE["binding_id"]
        binding_generation = "1"

    assert answers.check_scope(Sealed()) == answers.check_scope(SCOPE) == SCOPE


# --- evidence, scrubbed ---------------------------------------------------------------------------


def test_scrub_drops_every_secret_member_and_cuts_long_text() -> None:
    value = {
        "nonce": "n",
        "ticket": "t",
        "proof": "p",
        "context": "c",
        "nested": [{"selection_nonce": "s", "keep": "x" * 400}],
        "code": answers.NOT_SELECTED,
    }
    scrubbed = answers.scrub(value)
    assert scrubbed == {
        "nested": [{"keep": "x" * 300 + "..."}],
        "code": answers.NOT_SELECTED,
    }
    row = {"id": 1, "action": "denied", "evidence": value, "token": "secret", "extra": 1}
    assert answers.audit_summary(row)["evidence"] == scrubbed
    assert "token" not in answers.audit_summary(row)


def test_a_selection_summary_carries_digests_never_the_nonce() -> None:
    row = {
        "id": APPROVAL_ID,
        "status": "pending",
        "selected_approvers": json.dumps([{"issuer": ISSUER, "subject": "identity-e2e-bob"}]),
        "selection_requester": {"issuer": ISSUER, "subject": "identity-e2e-alice"},
        "selection_key_id": "k",
        "selection_nonce": "never-shown",
        "resumed": True,
    }
    summary = answers.selection_summary(row)
    assert summary["selected"] == ["identity-e2e-bob"]
    assert summary["requester"] == "identity-e2e-alice"
    assert summary["resumed"] is True
    assert "never-shown" not in json.dumps(summary)
    moved = answers.selection_summary({**row, "selection_key_id": "other"})
    assert moved["selected_sha256"] == summary["selected_sha256"]
    assert moved["snapshot_sha256"] != summary["snapshot_sha256"]


# --- judges ---------------------------------------------------------------------------------


def denied_row(**evidence: Any) -> dict[str, Any]:
    return {"id": 2, "action": answers.DENIED, "authorized": False, "evidence": evidence}


RESOLVED_ROW = {
    "id": 3,
    "action": answers.RESOLVED,
    "authorized": True,
    "decision": "approved",
    "evidence": {"selection": {"kind": answers.SELECTION_KIND, "equality": False}},
}


def answer_result(http: int, after: str, *rows: dict[str, Any]) -> dict[str, Any]:
    return {
        "http": http,
        "detail": None,
        "status_before": "pending",
        "status_after": after,
        "audit": list(rows),
    }


def test_judge_answer_passes_an_audited_refusal_and_an_approval() -> None:
    refused = answer_result(403, "pending", denied_row(code=answers.NOT_SELECTED))
    assert answers.judge_answer(
        refused, http=403, row_after="pending", action=answers.DENIED, code=answers.NOT_SELECTED
    ) == []
    allowed = answer_result(200, "approved", RESOLVED_ROW)
    assert (
        answers.judge_answer(allowed, http=200, row_after="approved", action=answers.RESOLVED)
        == []
    )
    by_reason = answer_result(403, "pending", {**denied_row(), "reason": answers.KEY_REVOKED})
    assert answers.judge_answer(
        by_reason, http=403, row_after="pending", action=answers.DENIED, code=answers.KEY_REVOKED
    ) == []


@pytest.mark.parametrize(
    ("result", "want", "needle"),
    [
        (answer_result(200, "approved", RESOLVED_ROW), {"http": 403}, "not 403"),
        (answer_result(403, "approved", denied_row()), {"row_after": "pending"}, "not pending"),
        (answer_result(403, "pending"), {}, "no new denied audit row"),
        (
            answer_result(403, "pending", {**denied_row(), "authorized": True}),
            {},
            "authorized=True",
        ),
        (
            answer_result(403, "pending", denied_row(code=answers.NOT_SELECTED)),
            {"code": answers.REQUESTER_EXCLUDED},
            "does not name separation_requester_excluded",
        ),
        (
            answer_result(503, "pending", denied_row(status=403)),
            {"http": 503, "status": 503},
            "records status 403",
        ),
        (
            answer_result(403, "pending", {**denied_row(), "principal_kind": "human"}),
            {"principal_kind": "operator"},
            "principal kind 'human'",
        ),
    ],
)
def test_judge_answer_names_each_difference(
    result: dict[str, Any], want: dict[str, Any], needle: str
) -> None:
    wanted = {"http": 403, "row_after": "pending", "action": answers.DENIED, **want}
    problems = answers.judge_answer(result, **wanted)
    assert any(needle in problem for problem in problems), problems


def resume_result(decision: str, reason: str, *rows: dict[str, Any]) -> dict[str, Any]:
    check = {"http": 200, "decision": decision, "reason": reason}
    return {"wake": True, "turn": check, "grant": dict(check), "audit": list(rows)}


def test_judge_resume_needs_both_checks_and_an_audited_refusal() -> None:
    allowed = resume_result("continue", "current_authority")
    assert answers.judge_resume(allowed, decision="continue") == []
    refused = resume_result(
        "refuse", answers.CALLER_NOT_ALLOWED, {"action": answers.RESUME_REFUSED}
    )
    assert answers.judge_resume(refused, decision="refuse") == []
    unaudited = resume_result("refuse", answers.CALLER_NOT_ALLOWED)
    assert any("resume_refused" in p for p in answers.judge_resume(unaudited, decision="refuse"))
    invalid = resume_result("refuse", "invalid_context", {"action": answers.RESUME_REFUSED})
    assert any("invalid_context" in p for p in answers.judge_resume(invalid, decision="refuse"))
    split = resume_result("continue", "current_authority")
    split["grant"] = {"http": 200, "decision": "refuse", "reason": answers.CALLER_NOT_ALLOWED}
    assert any("grant check" in p for p in answers.judge_resume(split, decision="continue"))
    assert answers.judge_resume({"wake": False}, decision="continue") == [
        "the API published no resume wake for the approval"
    ]


def test_selection_facts_read_top_level_or_nested_evidence() -> None:
    top = {"evidence": {"kind": answers.SELECTION_KIND, "requester_equal": True}}
    assert answers.evidence_fact(top, "equality") is True
    assert answers.evidence_fact(RESOLVED_ROW, "equality") is False
    nested = {"evidence": {"selection": {"exemption_used": True}}}
    assert answers.evidence_fact(nested, "exemption_used") is True
    ordinary = {"evidence": {"separation": {"requester_equality": True}}}
    assert answers.selection_facts(ordinary) == {}
    assert answers.evidence_fact(ordinary, "equality") is True
    assert answers.selection_facts(None) == {}
    assert answers.refusal_code(denied_row(refusal_code="x")) == "x"
    assert answers.refusal_status(denied_row(status=True)) is None
    assert answers.newest([RESOLVED_ROW, {**RESOLVED_ROW, "id": 9}], answers.RESOLVED)["id"] == 9


def test_separation_change_reads_flat_or_nested_values() -> None:
    flat = {
        "route": tickets.TICKET_ROUTE,
        "old_exclude_requester": True,
        "new_exclude_requester": False,
        "old_configuration_version": 4,
        "new_configuration_version": 5,
    }
    nested = {
        "route": tickets.TICKET_ROUTE,
        "old": {"exclude_requester": True},
        "new": {"exclude_requester": False, "exempt_group": {"id": answers.EXEMPT_GROUP}},
        "old_configuration_version": 4,
        "new_configuration_version": 5,
    }
    for details in (flat, nested):
        change = answers.separation_change(details)
        assert (change["old_exclude_requester"], change["new_exclude_requester"]) == (True, False)
        assert (change["old_configuration_version"], change["new_configuration_version"]) == (4, 5)
    assert answers.separation_change(nested)["new_exempt_group"] == {"id": answers.EXEMPT_GROUP}


def test_only_an_unchanged_503_or_an_unavailable_check_is_retried() -> None:
    assert answers.transient({"http": 503, "status_before": "pending", "status_after": "pending"})
    for http, after in ((503, "approved"), (403, "pending")):
        assert not answers.transient(
            {"http": http, "status_before": "pending", "status_after": after}
        )
    assert answers.resume_transient({"turn": {"decision": "unavailable"}, "grant": {}})
    assert not answers.resume_transient(resume_result("refuse", answers.CALLER_NOT_ALLOWED))


# --- the scenario's creation batches ------------------------------------------------------------


def test_creation_batches_group_by_selected_set_and_keep_static_ticketless() -> None:
    scenario = object.__new__(answer_authority_scenario.AnswerScenario)
    scenario.issuer = ISSUER
    scenario.subjects = {part: f"subject-{part}" for part in answers.ROLES}
    scenario.nonces = {"y1": "n1", "x1": "n2", "x2": "n3"}
    specs = {
        "x1": {"kind": answers.DYNAMIC, "parts": (answers.APPROVER,), "turn": "r01"},
        "x2": {"kind": answers.DYNAMIC, "parts": (answers.APPROVER,), "turn": "r02"},
        "y1": {
            "kind": answers.DYNAMIC,
            "parts": (answers.REQUESTER, answers.APPROVER),
            "turn": "r03",
        },
        "z1": {"kind": answers.STATIC, "parts": (), "turn": "r04"},
    }
    batches = scenario.batches(specs)
    assert [sorted(item["name"] for item in batch["cases"]) for batch in batches] == [
        ["x1", "x2"],
        ["y1"],
        ["z1"],
    ]
    assert batches[1]["approvers"] == [
        {"issuer": ISSUER, "subject": "subject-requester"},
        {"issuer": ISSUER, "subject": "subject-approver"},
    ]
    for batch in batches:
        assert tickets.nonce_labels(batch["cases"]) == sorted(batch["nonces"])
    static = batches[2]["cases"][0]
    assert static["ticket"] is None and static["route"] == answers.STATIC_ROUTE
    assert static["tool"] == tickets.live_tool(tickets.CONNECTOR, tickets.OTHER_TOOL)
    assert batches[0]["cases"][0]["route"] == tickets.TICKET_ROUTE


# --- the in-pod half ------------------------------------------------------------------------------


def test_the_driver_imports_only_the_standard_library_at_module_level() -> None:
    for source in (answers.SOURCE, Path(checker.__file__).read_text()):
        tree = ast.parse(source)
        names = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                names.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                names.add((node.module or "").split(".")[0])
        local = {"approver_ticket_driver", "answer_authority_driver"}
        assert names <= set(sys.stdlib_module_names) | {"__future__"} | local


def test_the_bootstrap_loads_both_drivers_and_refuses_without_echoing() -> None:
    action = "unknown-" + "x" * 200
    params = answers.pod_params(action, receiver={"key": "ab" * 32})
    result = subprocess.run(  # noqa: S603 - this interpreter, a fixed bootstrap
        [sys.executable, "-I", "-c", answers.POD_BOOTSTRAP],
        input=json.dumps(params),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    answer = json.loads(result.stdout.strip().splitlines()[-1])
    assert set(answer) == {"error"}
    assert "x" * 50 not in answer["error"]
    assert "ab" * 32 not in result.stdout + result.stderr
    assert len(answers.POD_BOOTSTRAP) < 1000


def test_the_creation_drivers_actions_are_delegated(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []

    def routes(params: Any) -> dict[str, Any]:
        seen.append(params)
        return {"routes": {}}

    monkeypatch.setitem(tickets._ACTIONS, "agent_routes", routes)
    assert answers.dispatch({"action": "agent_routes", "agent_id": "a"}) == {"routes": {}}
    assert seen == [{"action": "agent_routes", "agent_id": "a"}]


# --- the evidence checker -------------------------------------------------------------------------


def passing_evidence(**changes: Any) -> dict[str, Any]:
    evidence = {
        "scenario": answers.SCENARIO,
        "passed": True,
        "candidate_commit": "ab" * 20,
        "steps": [{"id": step, "passed": True} for step in TASK_STEPS],
    }
    evidence.update(changes)
    return evidence


def test_the_checker_passes_complete_evidence() -> None:
    assert checker.judge(passing_evidence()) == []


@pytest.mark.parametrize(
    ("evidence", "needle"),
    [
        (passing_evidence(scenario="approver-ticket-creation"), "approver-ticket-creation"),
        (passing_evidence(passed=False, aborted="restore incomplete: x"), "aborted: restore"),
        (
            passing_evidence(steps=[{"id": s, "passed": True} for s in TASK_STEPS[1:]]),
            "1 required steps are missing: answer-selected-approver-allowed",
        ),
        (
            passing_evidence(
                steps=[
                    *({"id": s, "passed": True} for s in TASK_STEPS),
                    {"id": TASK_STEPS[3], "passed": False, "failure": "the answer was 200"},
                ]
            ),
            "step ceiling-member-revoked has passed=False: the answer was 200",
        ),
        (passing_evidence(candidate_commit=None, api_image="curie-api:dev"), "curie-api:dev"),
        (passing_evidence(candidate_commit="not-a-sha"), "'not-a-sha'"),
        ([], "JSON list"),
    ],
)
def test_the_checker_names_what_it_observed(evidence: Any, needle: str) -> None:
    problems = checker.judge(evidence)
    assert any(needle in problem for problem in problems), problems


def fake_git(top: str | None, diff: tuple[int, str]) -> Any:
    calls: list[list[str]] = []

    def git(args: Sequence[str]) -> tuple[int, str]:
        calls.append(list(args))
        if "rev-parse" in args:
            return (0, f"{top}\n") if top else (128, "not a git repository")
        return diff

    git.calls = calls  # type: ignore[attr-defined]
    return git


def run_checker(
    tmp_path: Path, evidence: Any, git: Any, capsys: pytest.CaptureFixture[str]
) -> tuple[int, str]:
    path = tmp_path / "answer.json"
    path.write_text(json.dumps(evidence))
    code = checker.main([str(path), "--repo", str(tmp_path)], git=git)
    return code, capsys.readouterr().out


def test_the_checker_compares_the_checkout_with_the_candidate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clean = fake_git("/checkout", (0, "tools/identity-e2e/check_answer_evidence.py\ndocs/x.md\n"))
    code, out = run_checker(tmp_path, passing_evidence(), clean, capsys)
    assert code == 0, out
    assert "HEAD matches the candidate" in out and "21 of 21" in out
    assert clean.calls[-1] == ["-C", "/checkout", "diff", "--name-only", "ab" * 20, "HEAD"]

    dirty = fake_git("/checkout", (0, "apps/api/src/x.py\ncli/src/main.rs\ncli/README.md\n"))
    code, out = run_checker(tmp_path, passing_evidence(), dirty, capsys)
    assert code == 1
    assert "apps/api/src/x.py, cli/src/main.rs" in out and "cli/README.md" not in out

    unknown = fake_git("/checkout", (128, "bad revision"))
    code, out = run_checker(tmp_path, passing_evidence(), unknown, capsys)
    assert code == 1 and "bad revision" in out

    outside = fake_git(None, (0, ""))
    code, out = run_checker(tmp_path, passing_evidence(), outside, capsys)
    assert code == 0 and "comparison skipped" in out


def test_the_checker_refuses_failed_or_unreadable_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clean = fake_git(None, (0, ""))
    code, out = run_checker(tmp_path, passing_evidence(passed=False), clean, capsys)
    assert code == 1 and "MISMATCH" in out and "overall passed is False" in out
    bad = tmp_path / "bad.json"
    bad.write_text("{")
    assert checker.main([str(bad)], git=clean) == 2
    assert "unreadable evidence" in capsys.readouterr().out
