"""The approver-ticket-creation scenario's pure parts, judged by the product code.

Tickets the case table describes are minted with the frozen encoder and judged
by the API's own preparation verifier, the derived bundles by the API's bundle
intake and route reader, the multipart upload by FastAPI, and the approval and
creation bodies by the wire models the API parses. No cluster is involved.
"""

from __future__ import annotations

import ast
import base64
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import approver_ticket_driver as tickets
import identity_scenarios
import pytest
from aci_protocol import ApprovalRequest
from aci_protocol.turn import QueuedTurn, ReplyHandle
from curie_api import bundles
from curie_api.approver_tickets import verify_ticket
from curie_api.schemas.agent_access import RoleFilterV1
from curie_api.schemas.approvals import ApprovalRouteBinding
from curie_internal.approver_ticket import (
    TicketError,
    execution_arguments_sha256,
    parse_ticket,
    sign_ticket,
    unverified_key_id,
)
from curie_internal.identity_approval import ApprovalCreationRequestV1
from fastapi import FastAPI, UploadFile
from fastapi.testclient import TestClient
from identity_assets import DEFAULT_ROSTER, ISSUER
from identity_target import REPO
from nacl.signing import SigningKey

# The step ids the 0271a task names, in its order.
TASK_STEPS = (
    "registration-refused-flag-off",
    "flag-disable-refused-while-registered",
    "concurrent-flag-and-registration",
    "registration-bundle-gate-refused",
    "registration-accepted",
    "deploy-breaking-gate-refused",
    "creation-valid",
    "creation-bad-signature",
    "creation-scope-change",
    "creation-requester-change",
    "creation-argument-change",
    "creation-expired",
    "creation-nonce-replay",
    "creation-conflicting-dedupe",
    "creation-identical-race",
    "legacy-new-policy-refused",
    "legacy-ordinary-unchanged",
    "route-bypass-refused",
    "cluster-message-negative-control",
)
KEY = SigningKey(bytes(range(32)))
KEY_ID = tickets.key_id_for("20261108000000")
TENANT = "6f9619ff-8b86-4d01-b42d-00c04fc964ff"
AGENT = "0f8fad5b-d9cb-469f-a165-70867728950e"
REQUESTER = {"issuer": ISSUER, "subject": "identity-e2e-alice"}
APPROVER = {"issuer": ISSUER, "subject": "identity-e2e-bob"}
REGISTERED = {"connector": tickets.CONNECTOR, "tool": tickets.TOOL, "route": tickets.TICKET_ROUTE}
NOW = 1_800_000_000
WEATHER = REPO / "examples" / "weather"


def mint(
    item: dict[str, Any], *, nonces: dict[str, str] | None = None, now: int = NOW
) -> tuple[str, dict[str, Any]]:
    """The pod's own minting call, with the product signer and digest."""

    known = nonces or {}
    labels = {
        label: known.get(label) or tickets.new_nonce() for label in tickets.nonce_labels([item])
    }
    return tickets.mint_ticket(
        item,
        nonces=labels,
        now=now,
        sign=sign_ticket,
        digest=execution_arguments_sha256,
        registered_key=KEY,
        other_key=SigningKey.generate,
        tenant_id=TENANT,
        agent_id=AGENT,
        requester=REQUESTER,
        approvers=[APPROVER],
        registered=REGISTERED,
        key_id=KEY_ID,
    )


def verify(item: dict[str, Any], token: str, *, now: int = NOW) -> Any:
    """The API's preparation verifier, over the request's complete arguments."""

    return verify_ticket(
        public_key=KEY.verify_key,
        key_id=KEY_ID,
        arguments={**item["arguments"], "approval_ticket": token},
        clock=lambda: now,
    )


# --- the scenario's step ids --------------------------------------------------------------


def test_the_step_ids_are_the_tasks_and_the_scenario_is_registered() -> None:
    assert tickets.STEP_IDS == TASK_STEPS
    assert len(set(TASK_STEPS)) == len(TASK_STEPS)
    assert identity_scenarios.SCENARIOS[tickets.SCENARIO] is (
        identity_scenarios.approver_ticket_creation
    )


def recorded_step_ids() -> list[str]:
    """Step ids the scenario passes to ``evidence.step`` or ``refusal_step``, from its source."""

    tree = ast.parse(Path(identity_scenarios.__file__).read_text())
    owners = [
        node
        for node in tree.body
        if (isinstance(node, ast.ClassDef) and node.name == "TicketScenario")
        or (isinstance(node, ast.FunctionDef) and node.name == "approver_ticket_creation")
    ]
    found = []
    for owner in owners:
        for node in ast.walk(owner):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"step", "refusal_step"}
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                found.append(node.args[0].value)
    return found


def test_the_scenario_records_each_task_step_once() -> None:
    recorded = recorded_step_ids()
    assert sorted(recorded) == sorted(TASK_STEPS)


# --- tickets, judged by the API verifier ------------------------------------------------


def test_the_valid_case_mints_a_ticket_the_api_verifier_accepts() -> None:
    item = tickets.valid_cases()[0]
    nonce = tickets.new_nonce()
    token, claims = mint(item, nonces={"valid": nonce})
    verified = verify(item, token)
    assert verified.nonce == nonce
    assert verified.key_id == KEY_ID
    assert (verified.connector, verified.tool, verified.route) == (
        tickets.CONNECTOR,
        tickets.TOOL,
        tickets.TICKET_ROUTE,
    )
    assert (verified.tenant_id, verified.agent_id) == (TENANT, AGENT)
    assert verified.requester.subject == REQUESTER["subject"]
    assert [person.subject for person in verified.approvers] == [APPROVER["subject"]]
    assert verified.arguments_sha256 == execution_arguments_sha256(dict(tickets.DEFAULT_ARGUMENTS))
    assert claims["issued_at"] == NOW - tickets.ISSUED_SKEW_S
    assert item["expected"] == tickets.expect(201, duplicate=False)


VERIFIER_REFUSALS = [
    *tickets.bad_signature_cases(),
    *tickets.argument_change_cases(),
    *tickets.expired_cases(),
]


@pytest.mark.parametrize("item", VERIFIER_REFUSALS, ids=lambda item: item["name"])
def test_each_ticket_refusal_is_the_api_verifiers_code(item: dict[str, Any]) -> None:
    token, _ = mint(item)
    with pytest.raises(TicketError) as refused:
        verify(item, token)
    assert tickets.ticket_refusal(refused.value.code) == item["expected"]["detail"]
    assert item["expected"]["status"] == [422]


@pytest.mark.parametrize(
    "item",
    [*tickets.scope_change_cases(), *tickets.requester_change_cases({"bob": APPROVER})],
    ids=lambda item: item["name"],
)
def test_scope_and_requester_changes_pass_the_verifier_and_change_only_their_claim(
    item: dict[str, Any],
) -> None:
    """These are refused by creation's scope and requester checks, not by signature or time."""

    valid_token, valid = mint(tickets.valid_cases()[0])
    token, claims = mint(item)
    verify(item, token)
    changed = {
        key for key in valid if key not in {"nonce", "approvers"} and claims[key] != valid[key]
    }
    overrides = item["ticket"]["overrides"]
    if "tenant_id" in overrides:
        assert changed == {"tenant_id", "requester"}
        assert {person["tenant_id"] for person in claims["approvers"]} == {claims["tenant_id"]}
    else:
        assert changed == set(overrides)
    assert valid_token != token


def test_an_identical_retry_remints_the_same_ticket_later() -> None:
    nonce = tickets.new_nonce()
    first, claims = mint(tickets.valid_cases()[0], nonces={"valid": nonce})
    retry = tickets.conflicting_dedupe_cases(claims["issued_at"])[0]
    again, _ = mint(retry, nonces={"valid": nonce}, now=NOW + 200)
    assert again == first
    assert retry["expected"] == tickets.expect(200, duplicate=True)


def test_a_tampered_signature_keeps_a_canonical_envelope_and_fails_verification() -> None:
    token, _ = mint(tickets.valid_cases()[0])
    tampered = tickets.tamper_signature(token)
    assert tampered != token
    assert unverified_key_id(tampered) == KEY_ID
    with pytest.raises(TicketError) as refused:
        parse_ticket(tampered, public_key=KEY.verify_key)
    assert refused.value.code == "invalid_ticket"


def test_nonces_and_their_stored_forms() -> None:
    nonce = tickets.new_nonce()
    assert len(tickets.b64url_decode(nonce)) == 32
    forms = tickets.nonce_forms(nonce)
    assert forms[0] == nonce
    assert forms[1] == hashlib.sha256(nonce.encode()).hexdigest()
    assert forms[2] == hashlib.sha256(tickets.b64url_decode(nonce)).hexdigest()


# --- the case tables -----------------------------------------------------------------------


def all_tables() -> dict[str, list[dict[str, Any]]]:
    dedupe = {name: f"legacy-{name}" for name in tickets.LEGACY_POLICY_NAMES}
    slack = tickets.slack_ordinary_approval(
        agent_id=AGENT, channel="C0EXAMPLE1", adapter=None, dedupe_key="ordinary"
    )
    return {
        "valid": tickets.valid_cases(),
        "bad-signature": tickets.bad_signature_cases(),
        "scope": tickets.scope_change_cases(),
        "requester": tickets.requester_change_cases({"bob": APPROVER}),
        "arguments": tickets.argument_change_cases(),
        "expired": tickets.expired_cases(),
        "replay": tickets.nonce_replay_cases(),
        "conflict": tickets.conflicting_dedupe_cases(NOW),
        "race": tickets.race_cases(),
        "legacy-policy": tickets.legacy_policy_cases("bypass", dedupe),
        "legacy-ordinary": tickets.legacy_ordinary_cases(
            turn="bypass", selected_dedupe="selected", slack=slack
        ),
        "bypass": tickets.route_bypass_cases(),
        "liveness": tickets.route_bypass_liveness(),
    }


def test_every_case_names_its_expectation_and_a_turn_or_body() -> None:
    for table in all_tables().values():
        for item in table:
            assert item["expected"]["status"], item["name"]
            assert item["turn"] or item["approval"] or item["reuse"], item["name"]
            assert item["endpoint"] in {"http", "legacy"}


def test_only_the_valid_ticket_its_replay_and_its_retries_share_its_nonce() -> None:
    sharing = sorted(
        item["name"]
        for table in all_tables().values()
        for item in table
        if item["ticket"] and item["ticket"]["nonce"] == "valid"
    )
    assert sharing == [
        "identical-retry",
        "spent-nonce-on-another-turn",
        "summary-changed",
        "valid",
    ]


def test_route_bypass_covers_every_bypass_with_every_gate_kind() -> None:
    cases = {item["name"]: item for item in tickets.route_bypass_cases()}
    kinds = {
        "looser-route": tickets.ROUTE_LOCKED,
        "routeless": tickets.ROUTE_LOCKED,
        "empty-route": tickets.ROUTE_LOCKED,
        "ticket-on-unregistered-route": tickets.TICKET_UNREGISTERED,
        "missing-ticket-on-owning-route": tickets.TICKET_REQUIRED,
        "other-tool-on-owning-route": tickets.ROUTE_MISMATCH,
    }
    for kind, code in kinds.items():
        for gate in tickets.GATE_KINDS:
            item = cases[f"{kind}/{gate or 'absent'}"]
            assert item["gate_kind"] == gate
            assert item["expected"] == tickets.expect(409, code)
    assert cases["routeless/absent"]["route"] is None
    assert cases["empty-route/absent"]["route"] == ""
    assert cases["looser-route/absent"]["ticket"] is None
    assert cases["missing-ticket-on-owning-route/permission"]["ticket"] is None
    assert cases["plugin-prefixed-name-on-owning-route"]["expected"]["detail"] == (
        tickets.ROUTE_MISMATCH
    )


def test_the_race_sends_identical_bodies_at_once_then_retries() -> None:
    race, retry = tickets.race_cases()
    assert race["repeat"] == tickets.RACE_POSTS and race["concurrent"] is True
    assert retry["reuse"] == race["name"]


def test_case_mismatches_names_every_wrong_answer() -> None:
    items = [
        {
            "name": "a",
            "expected": tickets.expect(409, tickets.REPLAYED),
            "results": [{"http": 409, "detail": tickets.REPLAYED}],
        },
        {
            "name": "b",
            "expected": tickets.expect(201, duplicate=False),
            "results": [{"http": 200, "detail": None, "duplicate": True}],
        },
        {
            "name": "c",
            "expected": tickets.expect(422, "approval_ticket_invalid_ticket"),
            "results": [{"http": 422, "detail": "approval_ticket_key_mismatch"}],
        },
    ]
    problems = tickets.case_mismatches(items)
    assert len(problems) == 2
    assert problems[0].startswith("b[0] answered 200")
    assert problems[1].startswith("c[0] answered 422 'approval_ticket_key_mismatch'")


# --- request bodies, judged by the wire models ------------------------------------------------


def queued_turn() -> QueuedTurn:
    return QueuedTurn(
        event_id="identity-e2e-event-1",
        conversation_id="<conversation@identity-e2e.example.test>",
        author="alice@example.test",
        text="approver-ticket-creation: valid turn",
        reply_handle=ReplyHandle(
            kind="email",
            channel="agent-a@inbox.example.test",
            placeholder=None,
            endpoint="http://identity-e2e-idp.test.svc:8080/reply/agent-a",
            adapter="identity-e2e",
        ),
        received_at="2026-11-08T00:00:00Z",
    )


def test_the_approval_and_creation_request_validate_with_the_api_wire_models() -> None:
    item = tickets.valid_cases()[0]
    token, _ = mint(item)
    turn = queued_turn()
    fields = tickets.approval_fields(
        item,
        turn=turn,
        agent_id=AGENT,
        granted_arguments={**item["arguments"], "approval_ticket": token},
    )
    approval = ApprovalRequest.model_validate(fields)
    assert approval.dedupe_key == turn.event_id
    assert approval.card_channel == approval.reply_channel == turn.reply_handle.channel
    assert (approval.route, approval.gate_kind) == (tickets.TICKET_ROUTE, "permission")
    assert approval.granted_tool == "mcp__netpol-probe__search_dashboards"
    scope = SimpleNamespace(
        tenant_id=TENANT,
        agent_id=AGENT,
        binding_id="9a1c2b3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d",
        binding_generation=1,
    )
    body = tickets.creation_request(
        approval.model_dump(mode="json"),
        capsule="curie-context-v1.synthetic.synthetic",
        payload=turn.model_dump_json(),
        scope=scope,
    )
    request = ApprovalCreationRequestV1.model_validate_json(json.dumps(body))
    assert request.approval.granted_arguments == {**item["arguments"], "approval_ticket": token}


def test_a_case_dedupe_key_replaces_the_turns_event_id() -> None:
    dedupe = {name: f"legacy-{name}" for name in tickets.LEGACY_POLICY_NAMES}
    item = tickets.legacy_policy_cases("bypass", dedupe)[1]
    fields = tickets.approval_fields(item, turn=queued_turn(), agent_id=AGENT, granted_arguments={})
    assert ApprovalRequest.model_validate(fields).dedupe_key == "legacy-routeless"
    assert fields["route"] is None


def test_the_slack_ordinary_approval_validates() -> None:
    body = tickets.slack_ordinary_approval(
        agent_id=AGENT, channel="C0EXAMPLE1", adapter=None, dedupe_key="identity-e2e-ordinary"
    )
    approval = ApprovalRequest.model_validate(body)
    assert approval.route is None and approval.card_channel == "C0EXAMPLE1"


def test_outcome_keeps_codes_and_never_echoes_granted_arguments() -> None:
    created = tickets.outcome(
        201,
        {
            "id": "x",
            "status": "pending",
            "duplicate": False,
            "granted_arguments": {"approval_ticket": "cat1.secret.secret"},
        },
    )
    assert created == {
        "http": 201,
        "detail": None,
        "id": "x",
        "duplicate": False,
        "approval_status": "pending",
    }
    assert "cat1" not in json.dumps(created)
    assert tickets.outcome(409, {"detail": tickets.REPLAYED})["detail"] == tickets.REPLAYED
    coded = {"detail": {"code": tickets.FLAG_VERSION_CONFLICT, "message": "m"}}
    assert tickets.outcome(409, coded)["detail"] == tickets.FLAG_VERSION_CONFLICT
    invalid = {"detail": [{"msg": "Value error, nope", "input": "cat1.secret.secret"}]}
    assert tickets.outcome(422, invalid)["detail"] == "validation_error: Value error, nope"


# --- registration, routes and the approver role ---------------------------------------------


def test_routes_keep_other_bindings_and_carry_the_registration_on_one_route() -> None:
    other = {"resolution": {"kind": "slack", "address": "C0EXAMPLE2"}}
    stored = {"ops": other, tickets.TICKET_ROUTE: {"resolution": {"mode": "requesting_surface"}}}
    written = tickets.registration(key_id=KEY_ID, public_key="pk", role_version=4)
    routes = tickets.routes_with(stored, written)
    assert routes["ops"] == other
    assert routes[tickets.LOOSER_ROUTE] == tickets.REQUESTING_SURFACE
    assert routes[tickets.TICKET_ROUTE]["approver_ticket"] == written
    assert tickets.has_registration(routes)
    assert not tickets.has_registration(tickets.routes_with(stored))
    assert tickets.base_routes(routes) == {"ops": other}
    assert "pk" not in json.dumps(tickets.redacted(routes))
    for binding in tickets.routes_with(stored).values():
        ApprovalRouteBinding.model_validate(binding)
    elsewhere = tickets.routes_with(stored, written, route=tickets.LOOSER_ROUTE)
    assert "approver_ticket" not in elsewhere[tickets.TICKET_ROUTE]


def test_the_registration_has_the_plan_d4_write_shape() -> None:
    written = tickets.registration(key_id=KEY_ID, public_key="pk", role_version=4)
    assert set(written) == {"connector", "tool", "key_id", "public_key", "ceiling"}
    assert written["ceiling"] == {"approver_role_version": 4}
    assert "__" not in written["connector"] and not written["connector"].startswith("plugin_")


def test_the_selector_key_converts_openssl_encodings() -> None:
    seed = base64.b64encode(bytes(range(32))).decode()
    public = base64.b64encode(bytes(KEY.verify_key)).decode()
    key = tickets.selector_key(seed, public)
    assert SigningKey(bytes.fromhex(key["seed_hex"])).verify_key == KEY.verify_key
    assert tickets.b64url_decode(key["public_key"]) == bytes(KEY.verify_key)
    assert "=" not in key["public_key"]
    with pytest.raises(ValueError):
        tickets.selector_key(seed[:-4], public)


def test_the_approver_ceiling_is_a_restricted_role_naming_the_active_humans() -> None:
    subjects = tickets.ceiling_subjects(DEFAULT_ROSTER)
    assert subjects == ["identity-e2e-alice", "identity-e2e-bob", "identity-e2e-dana"]
    role = RoleFilterV1.model_validate(tickets.approver_filter(ISSUER, subjects))
    assert [entry.subject for entry in role.entries] == subjects


@pytest.mark.parametrize(
    ("image", "revision"),
    [
        ("ghcr.io/curie-eng/curie-api:sha-" + "cd" * 6, "cd" * 6),
        ("ghcr.io/curie-eng/curie-api:" + "ab" * 20, "ab" * 20),
        ("ghcr.io/curie-eng/curie-api:dev", None),
        ("ghcr.io/curie-eng/curie-api:0.15.3", None),
        ("ghcr.io/curie-eng/curie-api:20261108", None),
        ("ghcr.io/curie-eng/curie-api@sha256:" + "a" * 64, None),
        ("localhost:5000/curie-api", None),
    ],
)
def test_the_api_revision_comes_from_an_image_tag(image: str, revision: str | None) -> None:
    assert tickets.revision_from_image(image) == revision


# --- the serving bundles ------------------------------------------------------------------------


def gated_archive(route: str) -> bytes:
    manifest = json.loads((WEATHER / tickets.MANIFEST_PATH).read_text())
    gate = tickets.live_tool(tickets.CONNECTOR, tickets.TOOL)
    return tickets.bundle_archive(WEATHER, tickets.gated_manifest(manifest, [(gate, route)]))


@pytest.mark.parametrize("route", [tickets.TICKET_ROUTE, tickets.LOOSER_ROUTE])
def test_the_gated_bundle_passes_intake_and_declares_only_its_route(
    tmp_path: Path, route: str
) -> None:
    data = gated_archive(route)
    assert data == gated_archive(route)
    dest = tmp_path / "bundle"
    dest.mkdir()
    _, _, result = bundles.extract_and_validate(data, dest)
    assert result.valid, result.errors
    assert bundles.declared_approval_routes(dest) == {route}
    assert not any(
        part in tickets.EXCLUDED_NAMES for path in dest.rglob("*") for part in path.parts
    )


def test_the_multipart_upload_reaches_fastapi_as_the_archive() -> None:
    app = FastAPI()

    @app.put("/upload")
    async def upload(file: UploadFile) -> dict[str, Any]:
        data = await file.read()
        return {"name": file.filename, "sha256": hashlib.sha256(data).hexdigest()}

    data = gated_archive(tickets.TICKET_ROUTE)
    body, content_type = tickets.multipart("file", "bundle.tar.gz", data, "application/gzip")
    with TestClient(app) as client:
        response = client.put("/upload", content=body, headers={"Content-Type": content_type})
    assert response.status_code == 200, response.text
    assert response.json() == {
        "name": "bundle.tar.gz",
        "sha256": hashlib.sha256(data).hexdigest(),
    }


# --- the in-pod half -----------------------------------------------------------------------


def test_the_driver_imports_only_the_standard_library_at_module_level() -> None:
    tree = ast.parse(tickets.SOURCE)
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add((node.module or "").split(".")[0])
    assert names <= set(sys.stdlib_module_names) | {"__future__"}


def test_the_bootstrap_runs_the_source_and_refuses_without_echoing_its_input() -> None:
    action = "unknown-" + "x" * 200
    params = tickets.pod_params(action, key={"seed_hex": "ab" * 32})
    result = subprocess.run(  # noqa: S603 - this interpreter, a fixed bootstrap
        [sys.executable, "-I", "-c", tickets.POD_BOOTSTRAP],
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
    assert len(tickets.POD_BOOTSTRAP) < 1000


@pytest.mark.parametrize(
    ("code", "shown"),
    [("stale_role_policy_version", True), ("seed ab" + "ab" * 32, False), (None, False)],
)
def test_a_failure_names_a_product_refusal_code_but_never_free_text(
    monkeypatch: pytest.MonkeyPatch, code: str | None, shown: bool
) -> None:
    class Refusal(ValueError):
        def __init__(self) -> None:
            super().__init__("free text " + "ab" * 32)
            self.code = code

    def refuse(_params: object) -> dict[str, object]:
        raise Refusal

    monkeypatch.setitem(tickets._ACTIONS, "approver_role", refuse)
    answer = tickets.dispatch({"action": "approver_role"})
    assert answer["error"].startswith("approver_role failed with Refusal")
    assert ("stale_role_policy_version" in answer["error"]) is shown
    assert "ab" * 32 not in answer["error"]
