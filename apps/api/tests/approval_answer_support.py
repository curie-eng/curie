"""Shared ADR 0271 answer-authority fixtures (decisions 5 and 6); this module holds no tests.

@spec REQUEST-APPROVAL-5 REQUEST-APPROVAL-6.

Builds on ``approver_ticket_support``: a real adopted HTTP tenant with one
Slack binding an operator receiver registration lists, a served bundle gating
``mcp__access__execute`` on the owning route, the requester flag on and the
restricted, migrated approver role. Two things differ:

1. The synthetic TLS directory keeps a roster. Each Slack account a lookup
   names resolves to its own canonical person (subject, account status,
   groups), so the requester, the selected approvers, an outsider and a second
   account of the requester are distinct observations. An account outside the
   roster keeps the vertical suite's behaviour: the ingress requester.
   ``outage`` makes every lookup answer 503.
2. The operator receiver registration declares every roster account a human.

Answers go through the real ``POST /approvals/{id}/resolve`` with a
receiver-signed answer proof, exactly as ``test_http_approval_answers.answer``
builds it, for any account. Outcomes are read at the HTTP boundary, in the
approval row, its audit log and the identity audit log, and through the real
approved-resume publication, ``turn-authority/check`` and
``approval-grant/check``. Synthetic, reserved example values only.

Audit evidence vocabulary these suites pin (plan "Approach" 6 and "Callers" 1
and 3): a refusal on a canonical route writes one ``denied`` row whose evidence
is ``{"kind": "approval_selection", "status": <int>, "detail": <str>, ...}``
plus, when ``SelectionRefused`` is the cause, its ``code`` and selection
evidence (``equality``, ``exclusion``, ``exemption_used``,
``configuration_version``); an admitted answer's ``resolved`` row carries the
same selection evidence under ``evidence["selection"]``; a refused resume check
writes one ``resume_refused`` row with the same evidence.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from _migration_support import sql_dicts, sql_rows
from aci_protocol import QueuedTurn
from approver_ticket_support import (
    APPROVER_A,
    APPROVER_B,
    CONFIG_VERSION,
    KEY_ID,
    OTHER_ROUTE,
    OUTSIDER,
    REQUESTER,
    ROUTE,
    carried,
    create,
    dynamic_request,
    ingress,
    prepare,
    registration,
    route_map,
    selector_key,
    worker_headers,
    write_routes,
)
from curie_api import approval_principal, identity_fence
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_transport import HttpIdentityClient, ProviderBearer
from curie_api.identity.slack import SlackEvidence
from curie_api.main import create_app
from curie_api.resumequeue import resume_event_id
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER, VECTORS

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_answers import PROOF_HEADER, slack_evidence
from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    opened,
    request_for,
    rows,
)
from apps.api.tests.test_http_approval_resume import CONTEXT_FIELD, resume_module, with_stores
from apps.api.tests.test_http_turn_authority import post_check
from apps.api.tests.test_http_turn_authority import request_for as check_for

SENDER = membership.SENDER  # the requester's own Slack account (the ingress author)
NATIVE_A = "U0EXAMPLEA"
NATIVE_B = "U0EXAMPLEB"
NATIVE_OUTSIDER = "U0EXAMPLEOUT"
NATIVE_ALIAS = "U0EXAMPLE2"  # a second Slack account of the requester's canonical person
NATIVE_UNRESOLVED = "U0EXAMPLEUNK"
OPERATOR = "U0OPERATOR1"  # the operator principal ``operator_headers`` mints
HUMANS = (SENDER, NATIVE_A, NATIVE_B, NATIVE_OUTSIDER, NATIVE_ALIAS, NATIVE_UNRESOLVED)
EXEMPT_GROUP = "SyntheticExemptGroup"
EXEMPT_REF = {"issuer": ISSUER, "id": EXEMPT_GROUP}
ARGS: dict[str, Any] = {"account": "acct-example-1", "change": {"role": "reader"}, "count": 2}
SELECTION_FIELDS = (
    "selection_mode", "selection_requester", "selection_connector", "selection_tool",
    "selection_key_id", "selection_nonce", "selection_arguments_sha256", "selected_approvers",
)
GRANT_PATH = "/v1/internal/identity/approval-grant/check"
NEEDS_HTTP = "approval_separation_requires_http_identity"
SEPARATION_CHANGED = "approval_separation.changed"

# Audit vocabulary (see the module docstring).
SELECTION = "approval_selection"
OPERATOR_REFUSED = "canonical_route_operator_refused"
REQUIRES_HTTP_ANSWER = "canonical_route_requires_http_answer"
RESUME_REFUSED = "resume_refused"
EQUALITY = "equality"
EXCLUSION = "exclusion"
EXEMPTION_USED = "exemption_used"
CONFIGURATION_VERSION = "configuration_version"
FIXED = {403: "caller_not_allowed", 503: "authority_unavailable"}
ANY_CODE = object()


# --- the roster directory ----------------------------------------------------------


def person(
    subject: str, *, groups: tuple[str, ...] | None = (), status: str = "active",
) -> dict[str, Any]:
    """One canonical person as the synthetic directory reports them.

    ``status`` other than active reports the account inactive. ``groups=None``
    omits the membership a resolved answer must carry, which the API reads as
    an invalid (unavailable) observation. @spec REQUEST-APPROVAL-5.
    """
    outcome = "resolved" if status == "active" else "inactive"
    return {
        "subject": subject, "status": status, "outcome": outcome,
        "groups": None if groups is None else list(groups),
    }


UNRESOLVED: dict[str, Any] = {"outcome": "unresolved"}


def requester(*extra_groups: str) -> dict[str, Any]:
    """The requester's person: in the invocation group, plus ``extra_groups``.

    @spec REQUEST-APPROVAL-6.
    """
    return person(REQUESTER, groups=(membership.GROUP, *extra_groups))


def default_roster() -> dict[str, dict[str, Any]]:
    """@spec REQUEST-APPROVAL-5/6: every account but the ingress author's own."""
    return {
        NATIVE_A: person(APPROVER_A),
        NATIVE_B: person(APPROVER_B),
        NATIVE_OUTSIDER: person(OUTSIDER),
        NATIVE_ALIAS: requester(),
        NATIVE_UNRESOLVED: dict(UNRESOLVED),
    }


def _observation(provider: SimpleNamespace, request: dict[str, Any]) -> dict[str, Any]:
    """The directory's answer for the account the lookup names. @spec REQUEST-APPROVAL-5."""
    response = copy.deepcopy(VECTORS["cases"][0]["response"])
    response.update(copy.deepcopy(request))
    issuer = request["scope"]["expected_issuer"]
    stamp = (datetime.now(UTC) - timedelta(milliseconds=20)).isoformat()
    observed = {"as_of": stamp, "max_source_lag_ms": 5}
    entry = provider.roster.get(request["scope"]["sender"]["native_id"])
    if entry is None:
        group = membership.GROUP if provider.member else "SyntheticOtherGroup"
        entry = person(membership.SUBJECT, groups=(group,))
    if entry["outcome"] == "unresolved":
        response.update(
            outcome="unresolved", sender_kind="human", identity=None,
            identity_observation=observed, membership=None,
        )
        return response
    groups = entry["groups"]
    response.update(
        outcome=entry["outcome"], sender_kind="human",
        identity={
            "issuer": issuer, "subject": entry["subject"], "kind": "human",
            "account_status": entry["status"],
        },
        identity_observation=observed,
        membership=None if entry["outcome"] != "resolved" or groups is None else {
            "complete": True,
            "groups": [{"issuer": issuer, "id": group} for group in groups],
            "observation": observed,
        },
    )
    return response


@pytest.fixture
def synthetic_provider(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """@spec REQUEST-APPROVAL-5/6: the vertical suite's owned TLS directory, with a roster.

    A drop-in for ``test_http_invocation_membership.synthetic_provider``: same
    attributes and the same answer for an account outside ``roster``. The
    serving loop waits on an event the teardown sets, never on a sleep.
    """
    harness = membership._transport_harness()
    ready = threading.Event()
    control: dict[str, Callable[[], Any]] = {}
    provider = SimpleNamespace(
        member=True, requests=[], responses=[], errors=[], during_lookup=None,
        roster={}, outage=False,
    )

    async def serve() -> None:
        """@spec REQUEST-APPROVAL-5."""
        loop = asyncio.get_running_loop()
        stopped = asyncio.Event()
        control["stop"] = lambda: loop.call_soon_threadsafe(stopped.set)
        rig = harness.Rig(tmp_path)
        try:
            server_tls, provider.tls = rig.certificates()

            async def echo(_reader: Any, writer: Any) -> None:
                """@spec REQUEST-APPROVAL-5: echo the request scope, answer for its account."""
                first, headers, raw = listener.requests[-1]
                request = json.loads(raw)
                if provider.during_lookup is not None:
                    membership.joined(provider.during_lookup)
                assert first == b"POST /lookup HTTP/1.1"
                assert headers["authorization"] == "Bearer " + membership.BEARER
                assert "cookie" not in headers
                provider.requests.append(request)
                if provider.outage:
                    provider.responses.append(None)
                    writer.write(harness.wire(
                        b'{"error":"synthetic outage"}', status=b"503 Service Unavailable",
                    ))
                else:
                    response = _observation(provider, request)
                    provider.responses.append(copy.deepcopy(response))
                    writer.write(harness.wire(json.dumps(response).encode()))
                await writer.drain()

            listener = await rig.listen(handler=echo, tls=server_tls)
            provider.url = listener.url
            ready.set()
            await stopped.wait()
        finally:
            await rig.cleanup()

    def start() -> None:
        """@spec REQUEST-APPROVAL-5: relay owned thread failures to pytest."""
        try:
            asyncio.run(serve())
        except BaseException as exc:  # noqa: BLE001 - relayed to the fixture below.
            provider.errors.append(exc)
            ready.set()

    thread = threading.Thread(target=start, name="synthetic-roster-provider")
    try:
        thread.start()
        assert ready.wait(5), "owned TLS provider did not start"
        assert not provider.errors
        yield provider
    finally:
        stop = control.get("stop")
        if stop is not None:
            try:
                stop()
            except RuntimeError:  # the loop already ended with an error.
                pass
        thread.join(timeout=5)
        assert not thread.is_alive(), "owned TLS provider thread survived cleanup"
        assert not provider.errors, "owned TLS provider failed"


# --- the receiving API and the case -------------------------------------------------


def runtime_for(scope: dict[str, Any], *, credential: bool = True) -> Any:
    """The operator registration of the vertical suite, every roster account a human.

    ``credential=False`` registers no provider credential for the tenant's
    stored HTTP selection, as when the provider secret is not mounted, so
    every capture's credential resolution is unavailable. @spec REQUEST-APPROVAL-5/6.
    """
    module = membership._runtime_api()
    providers = build_provider_credential_registry(
        ((scope["configuration"], ProviderBearer(membership.BEARER)),) if credential else ()
    )
    registration_v1 = module.ReceiverRegistrationV1(
        schema_version=1, receiver_id=scope["receiver_id"], generation=1,
        tenant_id=scope["tenant"], channel_identity_id=scope["identity"],
        channel_identity_name=scope["adapter"],
        provider_installation_id=scope["installation"], namespace=scope["namespace"],
        installation_account=scope["account"], bot_user_id="U0EXAMPLEBOT",
        binding_ids=(scope["binding"],), human_native_ids=HUMANS,
    )
    return module.build_http_invocation_runtime(
        providers,
        ((registration_v1, scope["receiver_key"]),),
        (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
    )


@contextmanager
def queued(
    provider: SimpleNamespace, valkey: redis.Redis, stream: str,
) -> Iterator[SimpleNamespace]:
    """One real accepted ingress turn, as ``queued_invocation`` takes it.

    @spec REQUEST-APPROVAL-5/6.
    """
    scope = membership._scope(provider.url)
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=runtime_for(scope), http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            submitted = membership._post(client, credential, *membership._submission(scope))
            assert submitted.status_code == 200, submitted.text
            entries = valkey.xrange(stream)
            assert len(entries) == 1
            fields = entries[0][1]
            capsule, payload = fields["curie_identity_context_v1"], fields["payload"]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(capsule, ring)
            assert isinstance(context, codec.InvocationContextV1)
            assert len(provider.requests) == 1
            yield SimpleNamespace(
                client=client, scope=scope, capsule=capsule, context=context,
                payload=payload, entries=entries,
            )
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


@pytest.fixture
def selection_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec REQUEST-APPROVAL-5/6: an eligible agent with one ingress turn, not yet registered.

    Each test performs its own route configuration (``configure``), so the
    behaviour under test is written in the test body.
    """
    provider = synthetic_provider
    provider.roster.update(default_roster())
    with queued(provider, valkey, runs_stream) as case:
        case.provider, case.valkey, case.stream = provider, valkey, runs_stream
        case.key = selector_key()
        case.turn_used = False
        prepare(case.client, case.scope)
        yield case


# --- operator configuration ---------------------------------------------------------


def configure(
    case: SimpleNamespace, *, ticket: bool = True, users: list[str] | None = None,
    key: Any = None, key_id: str = KEY_ID, separation: dict[str, Any] | None = None,
    approvers: dict[str, Any] | None = None, static: Any = None,
    static_approvers: dict[str, Any] | None = None, client: TestClient | None = None,
) -> Any:
    """One versioned operator route-map write; tracks the configuration version.

    ``ticket`` registers the selector on the owning route (``users`` narrows its
    ceiling); ``separation`` and ``approvers`` go on the owning route,
    ``static`` and ``static_approvers`` on the selector-free route.
    @spec REQUEST-APPROVAL-5/6.
    """
    scope = case.scope
    ticketed = registration(
        key or case.key, scope["role_version"], users=users, key_id=key_id,
    ) if ticket else None
    routes = route_map(scope["address"], ticket=ticketed, separation=separation)
    if approvers is not None:
        routes[ROUTE]["approvers"] = approvers
    if static is not None:
        routes[OTHER_ROUTE]["separation"] = static
    if static_approvers is not None:
        routes[OTHER_ROUTE]["approvers"] = static_approvers
    response = write_routes(
        client or case.client, scope["agent"], routes, expected=scope["config_version"],
    )
    if response.status_code == 200:
        scope["config_version"] = response.json()[CONFIG_VERSION]
    return response


def configured(case: SimpleNamespace, **options: Any) -> dict[str, Any]:
    """@spec REQUEST-APPROVAL-5/6: a write that must be accepted."""
    response = configure(case, **options)
    assert response.status_code == 200, response.text
    return dict(response.json())


# --- approvals ----------------------------------------------------------------------


def _turn(case: SimpleNamespace) -> SimpleNamespace:
    """The fixture's own turn first, then a fresh accepted turn per approval.

    @spec REQUEST-APPROVAL-5.
    """
    if not case.turn_used:
        case.turn_used = True
        return case
    return ingress(case)


def dynamic(
    case: SimpleNamespace, approvers: tuple[str, ...] = (APPROVER_A, APPROVER_B),
) -> dict[str, Any]:
    """A dynamic approval selecting ``approvers``, through the real HTTP creation owner.

    @spec REQUEST-APPROVAL-5.
    """
    request = dynamic_request(_turn(case), carried(case, ARGS, approvers=approvers))
    response = create(case.client, request)
    assert response.status_code == 201, response.text
    found = rows(request["approval"]["dedupe_key"])
    assert len(found) == 1 and found[0]["selection_mode"] == "dynamic", found
    # A dynamic creation answers the V2 receipt carrying the stored row's id.
    assert response.json()["schema_version"] == 2, response.text
    assert response.json()["id"] == str(found[0]["id"])
    return found[0]


def ordinary(case: SimpleNamespace, route: str | None = None) -> dict[str, Any]:
    """An ordinary HTTP approval on ``route``, through the real creation owner.

    @spec REQUEST-APPROVAL-6.
    """
    request = request_for(_turn(case))
    request["approval"]["route"] = route
    accepted(create(case.client, request), request)
    found = rows(request["approval"]["dedupe_key"])
    assert len(found) == 1 and found[0]["selection_mode"] == "ordinary", found
    return found[0]


def approval_row(approval_id: Any) -> dict[str, Any]:
    """@spec REQUEST-APPROVAL-5: the committed row, read independently."""
    found = sql_dicts(
        "SELECT * FROM curie.approvals WHERE id = :id", {"id": uuid.UUID(str(approval_id))},
    )
    assert len(found) == 1, found
    return found[0]


def audit_rows(approval_id: Any) -> list[dict[str, Any]]:
    """@spec REQUEST-APPROVAL-5/6: the committed approval audit log, in order."""
    return sql_dicts(
        "SELECT action, actor, principal_kind, authorized, reason, decision, evidence "
        "FROM curie.approval_audit_entries WHERE approval_id = :id ORDER BY created_at, id",
        {"id": uuid.UUID(str(approval_id))},
    )


def actions(approval_id: Any, action: str) -> list[dict[str, Any]]:
    """@spec REQUEST-APPROVAL-5/6."""
    return [entry for entry in audit_rows(approval_id) if entry["action"] == action]


def still_pending(approval_id: Any) -> None:
    """@spec REQUEST-APPROVAL-5: a refusal leaves the row pending and unanswered."""
    row = approval_row(approval_id)
    assert (row["status"], row["resolved_by"], row["resolved_at"]) == ("pending", None, None)
    assert actions(approval_id, "resolved") == []


def fence_stable(valkey: redis.Redis) -> None:
    """@spec REQUEST-APPROVAL-5: no identity writer or fence is left open after a refusal."""
    stable = valkey.get(identity_fence.FENCE_KEY)
    if isinstance(stable, bytes):
        stable = stable.decode("ascii")
    assert isinstance(stable, str) and stable.startswith("stable:"), stable


# --- answers ------------------------------------------------------------------------


def answer_headers(
    case: SimpleNamespace, row: dict[str, Any], native: str, body: bytes, *,
    operation: str = "resolve",
) -> dict[str, str]:
    """``native``'s chat attestation plus a receiver-signed answer proof over ``body``.

    The exact construction of ``test_http_approval_answers.answer`` for any
    registered account and either operation (``resolve`` or ``recover``).
    @spec REQUEST-APPROVAL-5/6.
    """
    scope = case.scope
    evidence = slack_evidence(user_id=native)
    apr = approval_principal.mint(
        get_settings().approval_chat_attester_secret, subject=native, kind="chat",
        actor_channel=row["card_channel"], approval_id=str(row["id"]),
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
        slack=SlackEvidence.model_validate(evidence),
    )
    proof = {
        "schema_version": 1, "purpose": "approval_answer", "operation": operation,
        "receiver_id": scope["receiver_id"], "receiver_generation": 1,
        "channel_identity_id": scope["identity"],
        "provider_installation_id": scope["installation"],
        "approval_id": str(row["id"]),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "attestation_sha256": hashlib.sha256(apr.encode("ascii")).hexdigest(),
        "native_channel_id": row["card_channel"],
        "slack": {**evidence, "enterprise_ids": ()},
    }
    ring = codec.build_identity_keyring(
        "receiver", ((scope["receiver_id"], 1, scope["receiver_key"]),),
    )
    token = wire.sign_approval_answer_proof(wire.ApprovalAnswerProofV1.model_validate(proof), ring)
    return {
        "X-Curie-Approval-Principal": apr, PROOF_HEADER: token,
        "Content-Type": "application/json",
    }


def answer_as(
    case: SimpleNamespace, row: dict[str, Any], native: str, *, decision: str = "approved",
) -> Any:
    """``native`` clicks the card: chat attestation plus a receiver-signed answer proof.

    The exact construction of ``test_http_approval_answers.answer`` for any
    registered account. @spec REQUEST-APPROVAL-5/6.
    """
    body = json.dumps({"decision": decision}, separators=(",", ":")).encode()
    return case.client.post(
        f"/approvals/{row['id']}/resolve", content=body,
        headers=answer_headers(case, row, native, body),
    )


def refused(
    case: SimpleNamespace, row: dict[str, Any], native: str, status: int = 403, *,
    code: Any = ANY_CODE, decision: str = "approved",
) -> dict[str, Any]:
    """The answer is refused with its fixed detail and audited exactly once.

    The row stays pending, one ``denied`` row is added whose evidence names the
    refusal status and, when given, its stable code, and the identity fence is
    stable again. Returns that evidence. @spec REQUEST-APPROVAL-5/6.
    """
    before = len(actions(row["id"], "denied"))
    response = answer_as(case, row, native, decision=decision)
    assert response.status_code == status, response.text
    assert response.json() == {"detail": FIXED[status]}
    still_pending(row["id"])
    denied = actions(row["id"], "denied")
    assert len(denied) == before + 1, denied
    assert denied[-1]["actor"] == native, denied[-1]
    evidence = denied[-1]["evidence"]
    assert isinstance(evidence, dict) and evidence["kind"] == SELECTION, evidence
    assert evidence["status"] == status, evidence
    if code is not ANY_CODE:
        assert evidence.get("code") == code, evidence
    fence_stable(case.valkey)
    return dict(evidence)


def answered(
    case: SimpleNamespace, row: dict[str, Any], native: str, *, decision: str = "approved",
) -> dict[str, Any]:
    """The answer commits: 200, the row settled by ``native``, one ``resolved`` row.

    Returns the resolved row's selection evidence. @spec REQUEST-APPROVAL-5/6.
    """
    response = answer_as(case, row, native, decision=decision)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == decision
    assert response.json()["resolved_by"] == native
    stored = approval_row(row["id"])
    assert (stored["status"], stored["resolved_by"]) == (decision, native)
    resolved = actions(row["id"], "resolved")
    assert len(resolved) == 1, resolved
    assert resolved[0]["actor"] == native and resolved[0]["decision"] == decision
    selection = resolved[0]["evidence"]["selection"]
    assert isinstance(selection, dict) and selection["kind"] == SELECTION, selection
    return dict(selection)


def store_snapshot(case: SimpleNamespace, approval_id: Any, value: Any) -> None:
    """Hand-written stored selection, resealed so the sealing owner still names the row.

    Only the snapshot changes; the seal is recomputed over the stored row as
    the creation owner does, so the answer reaches the selection check.
    @spec REQUEST-APPROVAL-5.
    """
    identifier = uuid.UUID(str(approval_id))
    sql_rows(
        "UPDATE curie.approvals SET selected_approvers = CAST(:value AS jsonb) WHERE id = :id",
        {"value": json.dumps(value), "id": identifier},
    )
    row = approval_row(identifier)
    module, ring, capsule = opened(case, row)
    data = capsule.model_dump(mode="json")
    data["approval_record_sha256"] = digest(canonical({
        key: row[key] for key in RECORD_FIELDS + SELECTION_FIELDS
    }))
    token = module.seal_approval_context(module.ApprovalContextV1.model_validate(data), ring)
    sql_rows(
        "UPDATE curie.approvals SET identity_context = :token WHERE id = :id",
        {"token": token, "id": identifier},
    )


# --- resume -------------------------------------------------------------------------


def _wakes(case: SimpleNamespace, approval_id: Any) -> list[dict[str, str]]:
    """@spec REQUEST-APPROVAL-5: resume wakes on the runs stream for this approval."""
    event = resume_event_id(uuid.UUID(str(approval_id)))
    return [
        dict(fields) for _, fields in case.valkey.xrange(case.stream)
        if "payload" in fields
        and QueuedTurn.model_validate_json(fields["payload"]).event_id == event
    ]


def no_wake(case: SimpleNamespace, approval_id: Any) -> None:
    """@spec REQUEST-APPROVAL-5: a refused answer publishes nothing."""
    assert _wakes(case, approval_id) == []


def published(case: SimpleNamespace, approval_id: Any) -> dict[str, str]:
    """The one resume wake the committed answer appended for this approval.

    @spec REQUEST-APPROVAL-5.
    """
    found = _wakes(case, approval_id)
    assert len(found) == 1, found
    assert CONTEXT_FIELD in found[0]
    return found[0]


def _resume_check(
    case: SimpleNamespace, entry: dict[str, str], approval_id: Any, *, stage: str,
) -> dict[str, Any]:
    """The worker's current-check request for the published resume. @spec REQUEST-APPROVAL-5."""
    turn = QueuedTurn.model_validate_json(entry["payload"])
    request = check_for(case, stage=stage)
    request.update(
        context=entry[CONTEXT_FIELD], event_id=turn.event_id, approval_id=str(approval_id),
        payload_sha256=hashlib.sha256(entry["payload"].encode("utf-8")).hexdigest(),
    )
    return request


def turn_check(
    case: SimpleNamespace, entry: dict[str, str], approval_id: Any,
) -> tuple[str, str]:
    """``POST /v1/internal/identity/turn-authority/check`` on the approved resume.

    @spec REQUEST-APPROVAL-5.
    """
    response = post_check(
        case.client, _resume_check(case, entry, approval_id, stage="claim_or_resume"),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    return body["decision"], body["reason"]


def grant_check(
    case: SimpleNamespace, entry: dict[str, str], approval_id: Any,
) -> tuple[str, str]:
    """``POST /v1/internal/identity/approval-grant/check`` for the stored grant.

    @spec REQUEST-APPROVAL-5.
    """
    row = approval_row(approval_id)
    arguments = row["granted_arguments"]
    body = {
        "schema_version": 1,
        "check": _resume_check(case, entry, approval_id, stage="prepare"),
        "grant": {
            "approval_id": str(approval_id), "agent_id": str(row["agent_id"]),
            "conversation_id": row["conversation_id"], "gate_kind": row["gate_kind"],
            "granted_tool": row["granted_tool"],
            "granted_arguments_json": None if arguments is None else canonical(arguments),
        },
    }
    response = case.client.post(
        GRANT_PATH, content=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **worker_headers()},
    )
    assert response.status_code == 200, response.text
    check = response.json()["check"]
    return check["decision"], check["reason"]


def republish(case: SimpleNamespace, approval_id: Any) -> str | None:
    """Reopen the owed wake and run the production approved-resume publication once.

    Returns None when it appended exactly one wake, otherwise the refusal code
    it raised (and nothing was appended). @spec REQUEST-APPROVAL-5.
    """
    identifier = uuid.UUID(str(approval_id))
    sql_rows("UPDATE curie.approvals SET resumed_at = NULL WHERE id = :id", {"id": identifier})
    outcome: list[str | None] = []

    async def body(stores: SimpleNamespace) -> None:
        """@spec REQUEST-APPROVAL-5."""
        from curie_api.identity.http_approval_answer import approved_resume_authority

        stores.state.approver_sets = case.client.app.state.approver_sets
        owner = resume_module().http_resume_owner(
            stores.state, approved=approved_resume_authority(stores.state),
        )
        queue = type(stores.queue)(stores.valkey, stream=stores.stream, approvals=owner)
        try:
            await queue.enqueue_approval(identifier)
            outcome.append(None)
        except resume_module().ApprovalResumeRefused as refusal:
            outcome.append(refusal.code)

    entries = with_stores(case, body)
    assert len(outcome) == 1
    assert len(entries) == (1 if outcome[0] is None else 0), entries
    return outcome[0]


def resume_refusals(approval_id: Any) -> list[dict[str, Any]]:
    """@spec REQUEST-APPROVAL-5/6: the ``resume_refused`` audit rows, in order."""
    return actions(approval_id, RESUME_REFUSED)


def one_more_resume_refusal(
    approval_id: Any, before: int, actor: str, *, code: Any = ANY_CODE,
) -> dict[str, Any]:
    """Exactly one refused check added one row for the preserved answering actor.

    @spec REQUEST-APPROVAL-5/6.
    """
    found = resume_refusals(approval_id)
    assert len(found) == before + 1, found
    entry = found[-1]
    assert entry["actor"] == actor and entry["principal_kind"] is None, entry
    evidence = entry["evidence"]
    assert isinstance(evidence, dict) and evidence["kind"] == SELECTION, evidence
    if code is not ANY_CODE:
        assert evidence.get("code") == code, evidence
    return dict(evidence)


# --- identity audit -----------------------------------------------------------------


def separation_events(tenant: str) -> list[dict[str, Any]]:
    """@spec REQUEST-APPROVAL-6: ``approval_separation.changed`` identity audit rows."""
    return sql_dicts(
        "SELECT event_type, actor, details FROM curie.identity_audit_events "
        "WHERE tenant_id = :tenant AND event_type = :event ORDER BY created_at, id",
        {"tenant": uuid.UUID(tenant), "event": SEPARATION_CHANGED},
    )


# --- tenant selection ---------------------------------------------------------------


@contextmanager
def without_http_selection(tenant: str) -> Iterator[None]:
    """The tenant on its local authority for the block, then its selection restored.

    @spec REQUEST-APPROVAL-6.
    """
    identifier = uuid.UUID(tenant)
    saved = sql_dicts(
        "SELECT http_selection::text AS selection, governed_domains::text AS domains, "
        "managed_by FROM curie.tenant_identity_providers WHERE tenant_id = :tenant",
        {"tenant": identifier},
    )
    assert len(saved) == 1 and saved[0]["selection"] is not None, saved
    sql_rows(
        "UPDATE curie.tenant_identity_providers SET http_selection = NULL, "
        "governed_domains = CAST(:domains AS jsonb), managed_by = 'synthetic-directory' "
        "WHERE tenant_id = :tenant",
        {"domains": json.dumps(["example.test"]), "tenant": identifier},
    )
    try:
        yield
    finally:
        sql_rows(
            "UPDATE curie.tenant_identity_providers SET "
            "http_selection = CAST(:selection AS jsonb), "
            "governed_domains = CAST(:domains AS jsonb), managed_by = :managed "
            "WHERE tenant_id = :tenant",
            {
                "selection": saved[0]["selection"], "domains": saved[0]["domains"],
                "managed": saved[0]["managed_by"], "tenant": identifier,
            },
        )
