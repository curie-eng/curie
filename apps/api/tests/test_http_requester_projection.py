"""The current check projects the verified original requester for an opted-in agent.

ADR 0270 decision 1, through the real worker route, real ingress, the owned
synthetic TLS directory, PostgreSQL and Valkey. The operator flag
(``agents.connector_requester_identity``) is set with SQL here: its writer is a
separate surface with its own tests.

The directory double answers per sender and replays recorded shapes from
``tests/vectors/http-identity-v1.json`` (resolved-human-active,
inactive-human-disabled, unresolved-human, unavailable-unknown), echoing the
actual request. Synthetic inputs only.

@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-8/16 HTTP-APPROVAL-CONTINUITY-12.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import json
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import agent_role_policy, approval_principal
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.identity.slack import SlackEvidence
from curie_api.main import create_app
from curie_api.resumequeue import resume_event_id
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER, VECTORS
from role_policy_support import change, run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_approval_answers as answers
from apps.api.tests import test_http_approval_creation as creation
from apps.api.tests import test_http_approval_grants as grants
from apps.api.tests import test_http_email_receivers as mail
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests import test_http_turn_authority as authority
from apps.api.tests.test_http_approval_grants import resolved_case as resolved_case
from apps.api.tests.test_http_approval_legacy_writers import created

CHANGED_SUBJECT = "SyntheticChangedSubject"
APPROVER = answers.OTHER_APPROVER
APPROVER_SUBJECT = "SyntheticApproverSubject"
EMAIL_SUBJECT = "SyntheticEmailSubject"
CONTEXT_FIELD = "curie_identity_context_v1"
# Recorded directory shapes, by outcome. @spec HTTP-INVOCATION-EXECUTION-4.
_VECTOR = {"resolved": 0, "inactive": 3, "unresolved": 7, "unavailable": 11}


@pytest.fixture
def synthetic_provider(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """This module's owned TLS directory: the membership double, answering per sender.

    Overrides the shared ``synthetic_provider`` for this module only, so every
    reused fixture (``resolved_case``) talks to it. ``subjects`` and
    ``outcomes`` map a native sender to its canonical subject and recorded
    outcome; unlisted senders resolve to ``membership.SUBJECT``.
    @spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-12.
    """
    harness = membership._transport_harness()
    ready = threading.Event()
    # The serving loop and its stop event, set from teardown with
    # call_soon_threadsafe: no polling sleep and no executor thread.
    stopping: dict[str, Any] = {}
    provider = SimpleNamespace(
        member=True, requests=[], responses=[], errors=[], subjects={}, outcomes={},
    )

    async def serve() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-12: all external observations are synthetic."""
        rig = harness.Rig(tmp_path)
        stopped = asyncio.Event()
        stopping["loop"], stopping["event"] = asyncio.get_running_loop(), stopped
        try:
            server_tls, provider.tls = rig.certificates()

            async def echo(_reader: Any, writer: Any) -> None:
                """@spec HTTP-INVOCATION-EXECUTION-4: a recorded shape for the actual request."""
                first, headers, raw = listener.requests[-1]
                request = json.loads(raw)
                assert first == b"POST /lookup HTTP/1.1"
                assert headers["authorization"] == "Bearer " + membership.BEARER
                assert "cookie" not in headers
                native = request["scope"]["sender"]["native_id"]
                outcome = provider.outcomes.get(native, "resolved")
                response = copy.deepcopy(VECTORS["cases"][_VECTOR[outcome]]["response"])
                response.update(copy.deepcopy(request))
                issuer = request["scope"]["expected_issuer"]
                if outcome != "unavailable":
                    stamp = (datetime.now(UTC) - timedelta(milliseconds=20)).isoformat()
                    observation = {"as_of": stamp, "max_source_lag_ms": 5}
                    response["identity_observation"] = observation
                    if response["identity"] is not None:
                        response["identity"] = {
                            **response["identity"], "issuer": issuer,
                            "subject": provider.subjects.get(native, membership.SUBJECT),
                        }
                    if outcome == "resolved":
                        response["membership"] = {
                            "complete": True,
                            "groups": [{
                                "issuer": issuer,
                                "id": membership.GROUP if provider.member
                                else "SyntheticOtherGroup",
                            }],
                            "observation": observation,
                        }
                provider.requests.append(request)
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
        """@spec HTTP-INVOCATION-EXECUTION-12: propagate thread failures into fixture errors."""
        try:
            asyncio.run(serve())
        except BaseException as exc:  # noqa: BLE001 - relay owned thread failures to pytest.
            provider.errors.append(exc)
            ready.set()

    thread = threading.Thread(target=start, name="synthetic-requester-provider")
    try:
        thread.start()
        assert ready.wait(5), "owned TLS provider did not start"
        assert not provider.errors
        yield provider
    finally:
        if "loop" in stopping:
            with contextlib.suppress(RuntimeError):  # the loop already closed
                stopping["loop"].call_soon_threadsafe(stopping["event"].set)
        thread.join(timeout=5)
        assert not thread.is_alive(), "owned TLS provider thread survived cleanup"
        assert not provider.errors, "owned TLS provider failed"


@pytest.fixture
def stores(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> SimpleNamespace:
    """@spec CONNECTOR-REQUESTER-1: one clean database, stream and directory per test."""
    return SimpleNamespace(valkey=valkey, stream=runs_stream, provider=synthetic_provider)


def admit_raw_sender(scope: dict[str, Any]) -> None:
    """Add the migrated raw-account selector for the registered human sender.

    This is the path ``source_permission`` tolerates without a canonical match,
    so the projection must check the association itself.
    @spec CONNECTOR-REQUESTER-1 AGENT-ROLE-LAYER-V1-4.
    """

    async def apply(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-LAYER-V1-4."""
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change(
                uuid.UUID(scope["tenant"]), uuid.UUID(scope["agent"]),
                version=scope["role_version"], target="invocation",
                filter={"entries": [
                    {"kind": "issuer_group", "group": {"issuer": ISSUER, "id": membership.GROUP}},
                    {
                        "kind": "surface_account", "provider": "slack",
                        "namespace": scope["namespace"].model_dump(),
                        "native_id": membership.SENDER,
                    },
                ]},
            ))
            scope["role_version"] = result.version

    run(apply)


def requester_identity(agent: str, enabled: bool) -> None:
    """The operator flag, written directly. @spec CONNECTOR-REQUESTER-1."""

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        async with factory() as session, session.begin():
            result = await session.execute(
                text(
                    f'UPDATE "{SCHEMA}".agents SET connector_requester_identity = :enabled '
                    "WHERE id = :id"
                ),
                {"enabled": enabled, "id": uuid.UUID(agent)},
            )
            assert result.rowcount == 1

    run(write)


@contextmanager
def invocation(stores: SimpleNamespace, *, raw: bool = False) -> Iterator[SimpleNamespace]:
    """One real accepted Slack ingress and its sealed context.

    The same composition as ``test_http_turn_authority.queued_invocation``, with
    the raw-account selector optionally applied before ingress.
    @spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-8/16.
    """
    provider, valkey = stores.provider, stores.valkey
    scope = membership._scope(provider.url)
    if raw:
        admit_raw_sender(scope)
    module = membership._runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(module, scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            accepted = membership._post(client, credential, *membership._submission(scope))
            assert accepted.status_code == 200, accepted.text
            entries = valkey.xrange(stores.stream)
            assert len(entries) == 1
            fields = entries[0][1]
            capsule, payload = fields[CONTEXT_FIELD], fields["payload"]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(capsule, ring)
            assert isinstance(context, codec.InvocationContextV1)
            yield SimpleNamespace(
                client=client, scope=scope, capsule=capsule, context=context,
                payload=payload, entries=entries, provider=provider,
            )
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def original_requester(case: SimpleNamespace, subject: str = membership.SUBJECT) -> dict[str, str]:
    """The sealed verified invoker, read from the real context. @spec CONNECTOR-REQUESTER-1."""
    context = case.context
    assert context.outcome == "resolved"
    assert context.canonical is not None and context.principal is not None
    assert (context.canonical.issuer, context.canonical.subject) == (ISSUER, subject)
    return {"tenant_id": context.scope.tenant_id, "issuer": ISSUER, "subject": subject}


def assert_v2(response: Any, request: dict[str, Any], *, requester: dict[str, str] | None) -> None:
    """A complete V2 continue echoing this exact request. @spec CONNECTOR-REQUESTER-1."""
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "schema_version": 2,
        "request_id": request["request_id"],
        "stage": request["stage"],
        "event_id": request["event_id"],
        "context_sha256": hashlib.sha256(request["context"].encode("ascii")).hexdigest(),
        "scope": request["scope"],
        "decision": "continue",
        "reason": "current_authority",
        "requester": requester,
    }


def assert_v1(response: Any, request: dict[str, Any], *, decision: str, reason: str) -> None:
    """Today's exact V1 answer: no version 2, no requester key. @spec CONNECTOR-REQUESTER-1."""
    authority.assert_response(response, request, decision=decision, reason=reason)
    assert "requester" not in response.text


def lookups(case: SimpleNamespace, since: int) -> list[tuple[str, str]]:
    """Purpose and sender of each lookup after ``since``. @spec CONNECTOR-REQUESTER-1."""
    return [
        (item["purpose"], item["scope"]["sender"]["native_id"])
        for item in case.provider.requests[since:]
    ]


# -- Invocation contexts -----------------------------------------------------------------------


@pytest.mark.parametrize("flag", ["default", "explicit-off"])
def test_an_agent_that_has_not_opted_in_keeps_todays_v1_answers(
    stores: SimpleNamespace, flag: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: no requester transport without the operator opt-in."""
    with invocation(stores) as case:
        original_requester(case)
        if flag == "explicit-off":
            requester_identity(case.scope["agent"], False)
        for stage in ("prepare", "claim_or_resume", "start", "steer"):
            request = authority.request_for(case, stage=stage)
            assert_v1(
                authority.post_check(case.client, request), request,
                decision="continue", reason="current_authority",
            )
        case.provider.member = False
        refused = authority.request_for(case, stage="start")
        assert_v1(
            authority.post_check(case.client, refused), refused,
            decision="refuse", reason="caller_not_allowed",
        )


def test_an_opted_in_agent_gets_v2_with_the_verified_original_requester_at_every_stage(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: each projection follows a fresh current check."""
    with invocation(stores) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        for stage in ("prepare", "claim_or_resume", "start", "steer"):
            before = len(case.provider.requests)
            request = authority.request_for(case, stage=stage)
            assert_v2(authority.post_check(case.client, request), request, requester=expected)
            assert lookups(case, before) == [("invocation", membership.SENDER)]


def test_opted_in_refusals_and_unavailability_stay_v1_without_a_requester(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: refused or unavailable checks carry no projection."""
    with invocation(stores) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = authority.request_for(case)
        assert_v2(authority.post_check(case.client, live), live, requester=expected)

        forged = authority.request_for(case)
        forged["context"] = "synthetic-invalid-context"
        assert_v1(
            authority.post_check(case.client, forged), forged,
            decision="refuse", reason="invalid_context",
        )
        resume = authority.request_for(case, stage="claim_or_resume")
        resume["approval_id"] = str(uuid.uuid4())
        assert_v1(
            authority.post_check(case.client, resume), resume,
            decision="refuse", reason="unsupported_resume",
        )
        case.provider.member = False
        denied = authority.request_for(case, stage="start")
        assert_v1(
            authority.post_check(case.client, denied), denied,
            decision="refuse", reason="caller_not_allowed",
        )
        case.provider.member = True
        case.provider.outcomes[membership.SENDER] = "unavailable"
        unavailable = authority.request_for(case, stage="steer")
        assert_v1(
            authority.post_check(case.client, unavailable), unavailable,
            decision="unavailable", reason="authority_unavailable",
        )


def test_an_opted_in_sender_without_a_verified_human_gets_v2_with_a_null_requester(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a raw-account turn never acquires a human."""
    stores.provider.outcomes[membership.SENDER] = "unresolved"
    with invocation(stores, raw=True) as case:
        assert case.context.outcome == "unresolved"
        assert case.context.canonical is None and case.context.principal is None
        off = authority.request_for(case)
        assert_v1(
            authority.post_check(case.client, off), off,
            decision="continue", reason="current_authority",
        )
        requester_identity(case.scope["agent"], True)
        for stage in ("prepare", "start"):
            request = authority.request_for(case, stage=stage)
            assert_v2(authority.post_check(case.client, request), request, requester=None)


@pytest.mark.parametrize("outcome", ["unresolved", "inactive"])
def test_a_fresh_lookup_that_is_not_resolved_projects_a_null_requester(
    stores: SimpleNamespace, outcome: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: only a resolved, current human is projected.

    The sealed context still names a verified human; the raw-account exception
    admits the turn, but the current answer is not a resolved identity.
    """
    with invocation(stores, raw=True) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = authority.request_for(case)
        assert_v2(authority.post_check(case.client, live), live, requester=expected)
        case.provider.outcomes[membership.SENDER] = outcome
        request = authority.request_for(case, stage="start")
        assert_v2(authority.post_check(case.client, request), request, requester=None)


@pytest.mark.parametrize("raw", [False, True], ids=["group-only", "raw-account-exception"])
def test_a_changed_canonical_association_refuses_an_opted_in_agent(
    stores: SimpleNamespace, raw: bool,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a changed canonical association refuses.

    The raw-account exception admits this turn for an agent that has not opted
    in; the projection's own equality check refuses it here.
    """
    with invocation(stores, raw=raw) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = authority.request_for(case)
        assert_v2(authority.post_check(case.client, live), live, requester=expected)
        case.provider.subjects[membership.SENDER] = CHANGED_SUBJECT
        before = len(case.provider.requests)
        request = authority.request_for(case, stage="start")
        assert_v1(
            authority.post_check(case.client, request), request,
            decision="refuse", reason="caller_not_allowed",
        )
        assert lookups(case, before) == [("invocation", membership.SENDER)]


def test_an_agent_not_opted_in_keeps_the_raw_account_tolerance_unchanged(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: the projection rule never changes a V1 agent's answer."""
    with invocation(stores, raw=True) as case:
        original_requester(case)
        case.provider.subjects[membership.SENDER] = CHANGED_SUBJECT
        request = authority.request_for(case, stage="start")
        assert_v1(
            authority.post_check(case.client, request), request,
            decision="continue", reason="current_authority",
        )


def test_an_opted_in_email_turn_projects_its_verified_sender(stores: SimpleNamespace) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-29: the email receiver path."""
    provider, valkey = stores.provider, stores.valkey
    provider.subjects[mail.SENDER] = EMAIL_SUBJECT
    scope = mail._email_scope(provider.url)
    try:
        with mail._client(provider, scope) as client:
            token = mail._email_credential(client, scope)
            body = mail._body()
            accepted = mail._post(client, token, body, mail._proof(scope, body))
            assert accepted.status_code == 200, accepted.text
            fields = valkey.xrange(stores.stream)[0][1]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(fields[CONTEXT_FIELD], ring)
            assert isinstance(context, codec.InvocationContextV1)
            assert context.canonical is not None and context.principal is not None
            assert context.canonical.subject == EMAIL_SUBJECT
            case = SimpleNamespace(
                capsule=fields[CONTEXT_FIELD], payload=fields["payload"], context=context,
            )
            off = authority.request_for(case)
            assert_v1(
                authority.post_check(client, off), off,
                decision="continue", reason="current_authority",
            )
            requester_identity(scope["agent"], True)
            request = authority.request_for(case, stage="start")
            assert_v2(
                authority.post_check(client, request), request,
                requester={
                    "tenant_id": scope["tenant"], "issuer": ISSUER, "subject": EMAIL_SUBJECT,
                },
            )
    finally:
        mail._cleanup(valkey, scope)


# -- Resume contexts ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", ["expired", "rejected"])
def test_a_non_approved_resume_projects_the_original_invoker(
    resolved_case: SimpleNamespace, status: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-12: never the resolving actor."""
    case = resolved_case
    case.provider.subjects[grants.ACTOR] = APPROVER_SUBJECT
    expected = original_requester(case)
    token, payload = grants.resume_token(case, grants.resolve(case, status))
    off = grants.resume_check(case, token, payload, stage="prepare")
    assert_v1(
        authority.post_check(case.client, off), off,
        decision="continue", reason="current_authority",
    )
    requester_identity(case.scope["agent"], True)
    before = len(case.provider.requests)
    request = grants.resume_check(case, token, payload, stage="start")
    assert_v2(authority.post_check(case.client, request), request, requester=expected)
    assert lookups(case, before) == [("invocation", membership.SENDER)]
    case.provider.member = False
    denied = grants.resume_check(case, token, payload, stage="steer")
    assert_v1(
        authority.post_check(case.client, denied), denied,
        decision="refuse", reason="caller_not_allowed",
    )


def answer_as(case: SimpleNamespace, actor: str) -> tuple[bytes, dict[str, str]]:
    """The listener's exact approve bytes, attestation and proof for ``actor``.

    ``test_http_approval_answers.answer`` always answers as the invoker; this
    one names another registered Slack user. @spec HTTP-APPROVAL-CONTINUITY-7.
    """
    row = case.row
    body = json.dumps({"decision": "approved"}, separators=(",", ":")).encode()
    evidence = answers.slack_evidence(user_id=actor)
    apr = approval_principal.mint(
        get_settings().approval_chat_attester_secret, subject=actor, kind="chat",
        actor_channel=row["card_channel"], approval_id=str(row["id"]),
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
        slack=SlackEvidence.model_validate(evidence),
    )
    proof = {
        "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
        "receiver_id": case.scope["receiver_id"], "receiver_generation": 1,
        "channel_identity_id": case.scope["identity"],
        "provider_installation_id": case.scope["installation"],
        "approval_id": str(row["id"]),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "attestation_sha256": hashlib.sha256(apr.encode("ascii")).hexdigest(),
        "native_channel_id": row["card_channel"],
        "slack": {**evidence, "enterprise_ids": ()},
    }
    ring = codec.build_identity_keyring(
        "receiver", ((case.scope["receiver_id"], 1, case.scope["receiver_key"]),),
    )
    token = wire.sign_approval_answer_proof(wire.ApprovalAnswerProofV1.model_validate(proof), ring)
    return body, {
        "X-Curie-Approval-Principal": apr, answers.PROOF_HEADER: token,
        "Content-Type": "application/json",
    }


@contextmanager
def approved_resume(stores: SimpleNamespace, *, raw: bool = False) -> Iterator[SimpleNamespace]:
    """A real approval answered by ANOTHER approver, and the resume the core published.

    Creation, the answer route, both fresh answer checks and the core-owned
    publication are all the platform's own; nothing here reseals a capsule.
    @spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-7/8/11.
    """
    stores.provider.subjects[APPROVER] = APPROVER_SUBJECT
    with invocation(stores, raw=raw) as case:
        case.request = creation.request_for(case)
        answers.restrict_route(case, [APPROVER])
        case.request["approval"]["route"] = answers.ROUTE
        case.row = created(case)
        case.approval_id = str(case.row["id"])
        answers.approver_role(case)
        before = stores.valkey.xrange(stores.stream)
        answered = answers.resolve(case, *answer_as(case, APPROVER))
        assert answered.status_code == 200, answered.text
        assert answered.json()["status"] == "approved"
        assert answered.json()["resolved_by"] == APPROVER
        added = stores.valkey.xrange(stores.stream)[len(before):]
        assert len(added) == 1, added
        try:
            fields = added[0][1]
            case.resume, case.resume_payload = fields[CONTEXT_FIELD], fields["payload"]
            turn = QueuedTurn.model_validate_json(case.resume_payload)
            assert turn.event_id == resume_event_id(case.row["id"])
            yield case
        finally:
            stores.valkey.xdel(stores.stream, added[0][0])


def approved_check(case: SimpleNamespace, *, stage: str = "prepare") -> dict[str, Any]:
    """The worker's check for the published approved resume. @spec HTTP-APPROVAL-CONTINUITY-12."""
    return grants.resume_check(case, case.resume, case.resume_payload, stage=stage)


def test_an_approved_resume_answered_by_another_approver_projects_the_original_requester(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-8/12: never the approver."""
    with approved_resume(stores) as case:
        expected = original_requester(case)
        off = approved_check(case)
        assert_v1(
            authority.post_check(case.client, off), off,
            decision="continue", reason="current_authority",
        )
        requester_identity(case.scope["agent"], True)
        before = len(case.provider.requests)
        request = approved_check(case, stage="start")
        assert_v2(authority.post_check(case.client, request), request, requester=expected)
        assert expected["subject"] != APPROVER_SUBJECT
        # Both subjects were verified fresh for this one answer.
        assert sorted(lookups(case, before)) == [
            ("approval", APPROVER), ("invocation", membership.SENDER),
        ]


@pytest.mark.parametrize("raw", [False, True], ids=["group-only", "raw-account-exception"])
def test_a_changed_original_invoker_association_refuses_an_approved_resume(
    stores: SimpleNamespace, raw: bool,
) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-8: the approver cannot carry it."""
    with approved_resume(stores, raw=raw) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = approved_check(case)
        assert_v2(authority.post_check(case.client, live), live, requester=expected)
        case.provider.subjects[membership.SENDER] = CHANGED_SUBJECT
        request = approved_check(case, stage="start")
        assert_v1(
            authority.post_check(case.client, request), request,
            decision="refuse", reason="caller_not_allowed",
        )


def test_an_unresolved_original_invoker_projects_a_null_requester_on_an_approved_resume(
    stores: SimpleNamespace,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: the approver's verified identity never stands in."""
    with approved_resume(stores, raw=True) as case:
        expected = original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = approved_check(case)
        assert_v2(authority.post_check(case.client, live), live, requester=expected)
        case.provider.outcomes[membership.SENDER] = "unresolved"
        request = approved_check(case, stage="start")
        assert_v2(authority.post_check(case.client, request), request, requester=None)


def test_the_grant_check_stays_v1_for_an_opted_in_agent(stores: SimpleNamespace) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-12: grants never carry a requester."""
    with approved_resume(stores) as case:
        original_requester(case)
        requester_identity(case.scope["agent"], True)
        live = approved_check(case)
        assert authority.post_check(case.client, live).json()["schema_version"] == 2
        body = grants.grant_request(case, case.resume, case.resume_payload)
        response = grants.post_grant(case.client, body)
        grants.assert_grant(response, body, decision="continue", reason="current_authority")
        assert "requester" not in response.text
