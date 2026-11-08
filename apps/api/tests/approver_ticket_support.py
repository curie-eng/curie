"""Shared ADR 0271 creation-slice fixtures; this module holds no tests.

@spec REQUEST-APPROVAL-1 REQUEST-APPROVAL-2 REQUEST-APPROVAL-4.

Every agent here is a real adopted HTTP tenant built by the HTTP invocation
suites (``test_http_invocation_membership._scope``): a migrated, filtered
invocation role, one Slack binding an operator receiver registration lists, and
the synthetic TLS directory where a test needs a real ingress capsule. All
operator configuration goes through the real routes (``PATCH /agents``,
version and bundle upload, ``POST /deployments``); only identity records the
API has no operator write for (principals, IdP team projections) and
deliberately invalid stored state are written with SQL. Synthetic, reserved
example values only.

Shared rather than repeated per module so each test file spends its setup on
one agent and asserts many outcomes against it.
"""

from __future__ import annotations

import base64
import copy
import io
import json
import secrets
import tarfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from _migration_support import sql_dicts, sql_rows
from curie_api import agent_role_policy
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_internal import identity_context as codec
from curie_internal.approver_ticket import execution_arguments_sha256, sign_ticket
from fastapi.testclient import TestClient
from nacl.signing import SigningKey
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_creation import request_for
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import queued_invocation

CREATION_PATH = "/v1/internal/identity/approvals"
LOOKUP_URL = "https://lookup.example.test/v1/lookup"
CONNECTOR = "access"
OTHER_CONNECTOR = "other"
TOOL = "execute"
LIVE = f"mcp__{CONNECTOR}__{TOOL}"
ROUTE = "access_review"
OTHER_ROUTE = "ops_review"
SEED_ROUTE = "sensitive_tool"
KEY_ID = "selector-example-1"
REQUESTER = membership.SUBJECT
APPROVER_A = "SyntheticApproverA"
APPROVER_B = "SyntheticApproverB"
GROUP_MEMBER = "SyntheticApproverC"
OUTSIDER = "SyntheticOutsider"
APPROVER_GROUP = "SyntheticApproverGroup"
MANAGED_BY = "synthetic-directory"
FLAG = "connector_requester_identity"
FLAG_EXPECTED = "connector_requester_identity_expected_version"
CONFIG_VERSION = "connector_requester_identity_version"
ROUTES_EXPECTED = "approval_routes_expected_version"
IMAGE_DIGEST = "sha256:" + "cd" * 32
BUNDLE_NAME = "ticket-plugin"


# --- credentials and wire helpers ---------------------------------------------


def api_headers() -> dict[str, str]:
    """The operator platform credential every configuration route takes."""
    return {"X-API-Key": get_settings().api_key}


def worker_headers() -> dict[str, str]:
    """The existing internal worker credential of the HTTP creation owner."""
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def selector_key(seed: int = 1) -> SigningKey:
    """A deterministic synthetic Ed25519 selector key; never a real secret."""
    return SigningKey(bytes([seed]) * 32)


def public_key_text(key: SigningKey) -> str:
    """Unpadded base64url of the 32 public key bytes (plan D4)."""
    return base64.urlsafe_b64encode(bytes(key.verify_key)).rstrip(b"=").decode("ascii")


def now() -> int:
    return int(time.time())


def detail_text(response: Any) -> str:
    """The refusal detail as text, whatever envelope a route uses."""
    try:
        body = response.json()
    except ValueError:
        return response.text
    return json.dumps(body.get("detail") if isinstance(body, dict) else body, sort_keys=True)


def refused_with(response: Any, status: int, code: str) -> None:
    """A configuration refusal carries its plan code in its detail."""
    assert response.status_code == status, response.text
    assert code in detail_text(response), response.text


def exactly(response: Any, status: int, code: str) -> None:
    """A creation-owner refusal: bounded detail, never cached (HTTP-APPROVAL-CONTINUITY-3)."""
    assert response.status_code == status, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"detail": code}


# --- role ceiling ---------------------------------------------------------------


def subject_entry(subject: str, issuer: str = ISSUER) -> dict[str, Any]:
    return {"kind": "issuer_subject", "issuer": issuer, "subject": subject}


def group_entry(group: str, issuer: str = ISSUER) -> dict[str, Any]:
    return {"kind": "issuer_group", "group": {"issuer": issuer, "id": group}}


def surface_entry(scope: dict[str, Any], native_id: str = "U0EXAMPLEAPPROVER") -> dict[str, Any]:
    namespace = scope["namespace"]
    return {
        "kind": "surface_account",
        "provider": "slack",
        "namespace": {
            "id": namespace.id, "authority": namespace.authority,
            "kind": namespace.kind, "key": namespace.key,
        },
        "native_id": native_id,
    }


# The restricted approver role every registration pins by default. It names the
# requester too, so an ordinary approval stays answerable by the invoking human.
CEILING_FILTER: dict[str, Any] = {
    "entries": [
        subject_entry(REQUESTER),
        subject_entry(APPROVER_A),
        subject_entry(APPROVER_B),
        group_entry(membership.GROUP),
        group_entry(APPROVER_GROUP),
    ],
}


def set_approver_role(scope: dict[str, Any], role_filter: Any) -> int:
    """Write the approver role through the agent-owned role writer; returns its version.

    The first write migrates the role (no legacy route restriction is inferred);
    later writes replace the filter of the migrated role.
    """

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(
                session,
                change=change(
                    uuid.UUID(scope["tenant"]), uuid.UUID(scope["agent"]),
                    version=scope["role_version"], target="approver",
                    operation="set_filter" if scope.get("approver_migrated") else "migrate_role",
                    filter=role_filter,
                ),
            )
            scope["role_version"] = result.version
            scope["approver_migrated"] = True

    run(write)
    return int(scope["role_version"])


def stored_approver_role(agent_id: str) -> tuple[int, Any]:
    """The policy version and the approver role JSON as stored, read independently."""
    found = sql_dicts(
        "SELECT version, role_configuration->'approver' AS approver "
        "FROM curie.agent_access_policies WHERE agent_id = :agent",
        {"agent": uuid.UUID(agent_id)},
    )
    assert len(found) == 1, found
    return int(found[0]["version"]), found[0]["approver"]


def pinned_role(value: Any) -> Any:
    """The pinned role copy compared as JSON, whichever canonical render carries it."""
    return json.loads(value) if isinstance(value, str) else value


# --- identity records -----------------------------------------------------------


def add_principal(
    tenant: str, subject: str, *, status: str = "active", kind: str = "human",
) -> uuid.UUID:
    """A canonical identity record as the identity owner keeps it."""
    principal = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.principals (id, tenant_id, idp_issuer, idp_subject, type, status) "
        "VALUES (:id, :tenant, :issuer, :subject, :kind, :status)",
        {
            "id": principal, "tenant": uuid.UUID(tenant), "issuer": ISSUER,
            "subject": subject, "kind": kind, "status": status,
        },
    )
    return principal


def add_team(tenant: str, group: str, *, synced: bool, managed_by: str = MANAGED_BY) -> uuid.UUID:
    """An idp_group team projection, synced (epoch 1) or never synced (epoch 0)."""
    team = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.teams (id, tenant_id, source, external_id, name, epoch, "
        "member_count, digest, last_synced_at, managed_by) VALUES (:id, :tenant, "
        "'idp_group', :group, :name, :epoch, :count, :digest, "
        "CASE WHEN :synced THEN now() ELSE NULL END, :managed)",
        {
            "id": team, "tenant": uuid.UUID(tenant), "group": group,
            "name": "synthetic-team-" + uuid.uuid4().hex, "epoch": 1 if synced else 0,
            "count": 1 if synced else 0, "digest": ("ab" * 32) if synced else None,
            "synced": synced, "managed": managed_by,
        },
    )
    return team


def join_team(tenant: str, principal: uuid.UUID, team: uuid.UUID) -> None:
    sql_rows(
        "INSERT INTO curie.principal_teams (principal_id, team_id, tenant_id, source) "
        "VALUES (:principal, :team, :tenant, 'idp_group')",
        {"principal": principal, "team": team, "tenant": uuid.UUID(tenant)},
    )


def set_idp_managed_by(tenant: str, managed_by: str | None) -> None:
    sql_rows(
        "UPDATE curie.tenant_identity_providers SET managed_by = :managed "
        "WHERE tenant_id = :tenant",
        {"managed": managed_by, "tenant": uuid.UUID(tenant)},
    )


# --- bundles and deployments ----------------------------------------------------


def bundle(gates: list[tuple[str, str]] | tuple[tuple[str, str], ...]) -> bytes:
    """A plugin bundle declaring two hosted connectors and these ``(tool, route)`` gates."""
    connectors = "connectors:\n" + "".join(
        f"  {name}:\n    image: ghcr.io/example/{name}-connector@{IMAGE_DIGEST}\n"
        for name in (CONNECTOR, OTHER_CONNECTOR)
    )
    files = {
        ".claude-plugin/plugin.json": json.dumps({
            "name": BUNDLE_NAME, "version": "0.1.0",
            "approvalPolicy": {"gates": [{"gate": tool, "route": route} for tool, route in gates]},
        }),
        "skills/alpha/SKILL.md": (
            "---\nname: alpha\ndescription: does alpha things\n---\n\n# alpha\n"
        ),
        "connectors.yaml": connectors,
    }
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(f"{BUNDLE_NAME}/{path}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def stored_version(
    client: TestClient, agent_id: str, gates: Any, label: str | None = None,
) -> str:
    """A version whose stored bundle declares ``gates``; not deployed."""
    created = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": label or "v-" + uuid.uuid4().hex[:8], "created_by": "synthetic"},
        headers=api_headers(),
    )
    assert created.status_code == 201, created.text
    version = str(created.json()["id"])
    upload = client.put(
        f"/agents/{agent_id}/versions/{version}/bundle",
        files={"file": ("bundle.tar.gz", bundle(gates))},
        headers=api_headers(),
    )
    assert upload.status_code == 201, upload.text
    return version


def deploy(client: TestClient, agent_id: str, version: str) -> Any:
    return client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version, "environment": "dev"},
        headers=api_headers(),
    )


def serve(client: TestClient, agent_id: str, gates: Any) -> tuple[str, str]:
    """Store and deploy a version; returns (version id, deployment id)."""
    version = stored_version(client, agent_id, gates)
    deployed = deploy(client, agent_id, version)
    assert deployed.status_code == 201, deployed.text
    return version, str(deployed.json()["id"])


def active_deployments(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT id, version_id FROM curie.deployments WHERE agent_id = :agent "
        "AND status = 'active' ORDER BY deployed_at, id",
        {"agent": uuid.UUID(agent_id)},
    )


# --- route maps and operator writes ---------------------------------------------


def registration(
    key: SigningKey, role_version: int, *, key_id: str = KEY_ID, connector: str = CONNECTOR,
    tool: str = TOOL, users: list[str] | None = None,
) -> dict[str, Any]:
    """The operator's registration write (plan D4); server-owned pins are absent."""
    ceiling: dict[str, Any] = {"approver_role_version": role_version}
    if users is not None:
        ceiling["users"] = [{"issuer": ISSUER, "subject": subject} for subject in users]
    return {
        "connector": connector, "tool": tool, "key_id": key_id,
        "public_key": public_key_text(key), "ceiling": ceiling,
    }


def route_map(
    address: str, *, ticket: dict[str, Any] | None = None, on: str = ROUTE,
    separation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The full replacement map: the seeded route, the owning route and a looser one."""
    routes: dict[str, Any] = {
        SEED_ROUTE: {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE"},
            "approvers": {"users": ["U0EXAMPLE1"]},
        },
        ROUTE: {"resolution": {"kind": "slack", "address": address}},
        OTHER_ROUTE: {"resolution": {"kind": "slack", "address": address}},
    }
    if ticket is not None:
        routes[on]["approver_ticket"] = copy.deepcopy(ticket)
    if separation is not None:
        routes[on]["separation"] = copy.deepcopy(separation)
    return routes


def patch_agent(client: TestClient, agent_id: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{agent_id}", json=body, headers=api_headers())


def write_routes(
    client: TestClient, agent_id: str, routes: dict[str, Any], expected: Any = None,
) -> Any:
    body: dict[str, Any] = {"approval_routes": routes}
    if expected is not None:
        body[ROUTES_EXPECTED] = expected
    return patch_agent(client, agent_id, body)


def set_flag(client: TestClient, agent_id: str, enabled: bool, expected: int) -> Any:
    return patch_agent(client, agent_id, {FLAG: enabled, FLAG_EXPECTED: expected})


def agent_state(agent_id: str) -> dict[str, Any]:
    """The stored agent row, read independently of the router."""
    found = sql_dicts(
        "SELECT approval_routes, connector_requester_identity AS flag, "
        "connector_requester_identity_version AS version FROM curie.agents WHERE id = :id",
        {"id": uuid.UUID(agent_id)},
    )
    assert len(found) == 1, found
    return found[0]


def registered(state: dict[str, Any]) -> bool:
    """Whether any stored route carries an ``approver_ticket`` key."""
    routes = state["approval_routes"] or {}
    return any(isinstance(b, dict) and "approver_ticket" in b for b in routes.values())


def store_routes(agent_id: str, routes: Any, *, flag: bool | None = None) -> None:
    """Hand-written stored state no operator route could produce (fail-closed fixtures)."""
    sql_rows(
        "UPDATE curie.agents SET approval_routes = CAST(:routes AS jsonb)"
        + (", connector_requester_identity = :flag" if flag is not None else "")
        + " WHERE id = :id",
        {"routes": json.dumps(routes), "id": uuid.UUID(agent_id),
         **({"flag": flag} if flag is not None else {})},
    )


def readback(client: TestClient, agent_id: str) -> dict[str, Any]:
    response = client.get(f"/agents/{agent_id}", headers=api_headers())
    assert response.status_code == 200, response.text
    return dict(response.json())


def identity_events(tenant: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT event_type, actor, details FROM curie.identity_audit_events "
        "WHERE tenant_id = :tenant ORDER BY created_at, id",
        {"tenant": uuid.UUID(tenant)},
    )


def ticket_events(tenant: str) -> list[dict[str, Any]]:
    """Only the registration and separation audit rows (plan D4)."""
    return [
        row for row in identity_events(tenant)
        if row["event_type"].startswith(("approver_ticket.", "approval_separation."))
    ]


@contextmanager
def operator_app(runtime: Any) -> Iterator[TestClient]:
    """An API instance whose live runtime lists the agent's binding."""
    with TestClient(create_app(http_identity_runtime=runtime)) as client:
        yield client


def prepare(
    client: TestClient, scope: dict[str, Any], *, flag: bool = True,
    role_filter: Any = CEILING_FILTER, gates: Any = ((LIVE, ROUTE), ("Bash", OTHER_ROUTE)),
    serve_bundle: bool = True, principals: tuple[str, ...] = (APPROVER_A, APPROVER_B),
) -> None:
    """Everything a registration needs except the registration itself.

    Binds the routes (an unversioned selector-free write), serves a bundle that
    gates ``mcp__access__execute`` on the owning route, turns the requester flag
    on, sets the restricted approver role and records the selected approvers.
    """
    agent = scope["agent"]
    bound = write_routes(client, agent, route_map(scope["address"]))
    assert bound.status_code == 200, bound.text
    if serve_bundle:
        scope["version_id"], scope["deployment_id"] = serve(client, agent, gates)
    scope["config_version"] = agent_state(agent)["version"]
    if flag:
        enabled = set_flag(client, agent, True, scope["config_version"])
        assert enabled.status_code == 200, enabled.text
        scope["config_version"] = enabled.json()[CONFIG_VERSION]
    if role_filter is not None:
        set_approver_role(scope, role_filter)
    for subject in principals:
        scope.setdefault("principals", {})[subject] = add_principal(scope["tenant"], subject)


def register(
    client: TestClient, scope: dict[str, Any], key: SigningKey, *, on: str = ROUTE,
    users: list[str] | None = None, separation: dict[str, Any] | None = None,
    key_id: str = KEY_ID,
) -> Any:
    """The versioned operator registration; tracks the new configuration version."""
    response = write_routes(
        client, scope["agent"],
        route_map(
            scope["address"], on=on, separation=separation,
            ticket=registration(key, scope["role_version"], users=users, key_id=key_id),
        ),
        expected=scope["config_version"],
    )
    if response.status_code == 200:
        scope["config_version"] = response.json()[CONFIG_VERSION]
    return response


@pytest.fixture
def operator_case(clean_db: None) -> Iterator[SimpleNamespace]:
    """An adopted HTTP agent behind an operator app; no provider is ever called."""
    scope = membership._scope(LOOKUP_URL)
    module = membership._runtime_api()
    runtime = membership._runtime(module, scope)
    with operator_app(runtime) as client:
        yield SimpleNamespace(
            client=client, scope=scope, key=selector_key(),
            runtime_for=lambda: membership._runtime(module, scope),
        )


# --- creation through the real ingress capsule ----------------------------------


@pytest.fixture
def ticket_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """An eligible agent with one real accepted ingress turn, not yet registered.

    The turn is queued first (the HTTP invocation suites' own ingress), then the
    operator configuration that exists today lands. The registration itself is
    the behavior under test, so each test performs it with ``enroll`` and a
    missing registration owner fails in the test body, not in setup.
    """
    with queued_invocation(synthetic_provider, valkey, runs_stream) as invocation:
        case = invocation
        case.provider, case.valkey, case.stream = synthetic_provider, valkey, runs_stream
        case.key = selector_key()
        prepare(case.client, case.scope)
        yield case


def enroll(case: SimpleNamespace, **options: Any) -> dict[str, Any]:
    """Register the selector on the owning route; returns the agent readback."""
    response = register(case.client, case.scope, case.key, **options)
    assert response.status_code == 200, response.text
    return dict(response.json())


def ingress(case: SimpleNamespace) -> SimpleNamespace:
    """Another real accepted turn on the same binding: a fresh event id and capsule."""
    token = membership._credential(case.client, case.scope)
    before = {entry for entry, _ in case.valkey.xrange(case.stream)}
    body, proof = membership._submission(case.scope)
    response = membership._post(case.client, token, body, proof)
    assert response.status_code == 200, response.text
    added = [fields for entry, fields in case.valkey.xrange(case.stream) if entry not in before]
    assert len(added) == 1, added
    capsule, payload = added[0]["curie_identity_context_v1"], added[0]["payload"]
    ring = codec.build_identity_keyring(
        "context", (("synthetic-context", 1, case.scope["context_key"]),),
    )
    context = codec.open_invocation_context(capsule, ring)
    assert isinstance(context, codec.InvocationContextV1)
    return SimpleNamespace(
        client=case.client, scope=case.scope, capsule=capsule, payload=payload, context=context,
    )


def fresh_nonce() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")


def principal_claims(tenant: str, subject: str) -> dict[str, Any]:
    return {"tenant_id": tenant, "issuer": ISSUER, "subject": subject}


def ticket_payload(
    case: SimpleNamespace, arguments: dict[str, Any], *,
    approvers: tuple[str, ...] = (APPROVER_A, APPROVER_B), **changes: Any,
) -> dict[str, Any]:
    """Closed ticket claims for this agent, route and the ingress requester."""
    tenant = case.scope["tenant"]
    issued = now() - 5
    payload: dict[str, Any] = {
        "version": 1, "purpose": "approval_request", "tenant_id": tenant,
        "agent_id": case.scope["agent"], "connector": CONNECTOR, "tool": TOOL,
        "route": ROUTE, "key_id": KEY_ID,
        "requester": {
            "tenant_id": tenant, "issuer": case.context.canonical.issuer,
            "subject": case.context.canonical.subject,
        },
        "nonce": fresh_nonce(), "issued_at": issued, "expires_at": issued + 300,
        "arguments_sha256": execution_arguments_sha256(arguments),
        "approvers": [principal_claims(tenant, subject) for subject in approvers],
    }
    payload.update(changes)
    return payload


def carried(
    case: SimpleNamespace, arguments: dict[str, Any], *, key: SigningKey | None = None,
    payload: dict[str, Any] | None = None, **changes: Any,
) -> dict[str, Any]:
    """Execution arguments carrying a ticket signed over exactly those arguments."""
    claims = payload if payload is not None else ticket_payload(case, arguments, **changes)
    token = sign_ticket(claims, signing_key=key or case.key)
    return {**arguments, "approval_ticket": token}


def dynamic_request(
    invocation: SimpleNamespace, arguments: dict[str, Any] | None, *, route: str | None = ROUTE,
    tool: str | None = LIVE, gate_kind: str | None = "policy",
) -> dict[str, Any]:
    """The HTTP creation body for a gated connector call on ``route``."""
    request = request_for(invocation)
    request["approval"].update(
        summary="Approve the synthetic access change", route=route, gate_kind=gate_kind,
        granted_tool=tool, granted_arguments=arguments,
    )
    return request


def create(client: TestClient, request: dict[str, Any]) -> Any:
    return client.post(CREATION_PATH, json=request, headers=worker_headers())


def legacy(client: TestClient, approval: dict[str, Any]) -> Any:
    """The public legacy creation owner with the platform key."""
    return client.post("/approvals", json=approval, headers=api_headers())


def approval_rows(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT * FROM curie.approvals WHERE agent_id = :agent ORDER BY created_at, id",
        {"agent": uuid.UUID(agent_id)},
    )


def owner_verifies(case: SimpleNamespace, approval_id: str) -> bool:
    """The sealing owner's own check that the stored seal names this row as stored."""
    from curie_api.identity.http_approval import verified_capsule
    from curie_api.models import Approval

    runtime = case.client.app.state.http_identity_runtime
    verdict: list[bool] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session:
            row = await session.get(Approval, uuid.UUID(approval_id))
            assert row is not None
            verdict.append(verified_capsule(runtime, row) is not None)

    run(read)
    return verdict[0]


def approval_audits(approval_id: Any) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT action, actor, evidence FROM curie.approval_audit_entries "
        "WHERE approval_id = :id ORDER BY created_at, id",
        {"id": uuid.UUID(str(approval_id))},
    )
