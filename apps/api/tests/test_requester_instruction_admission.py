"""Requester selection through real API admission and Postgres (#5591)."""

# ruff: noqa: F401, F811

from __future__ import annotations

import hashlib
import uuid

import pytest
from fastapi.testclient import TestClient
from test_factory_label_reconcile import factory as reconcile_factory
from test_factory_poll_intake import poll_factory
from test_personal_execution_admission import (
    ADDRESS,
    World,
    _admit,
    _body,
    _rows,
    dispatch_client,
    github_factory,
    jira_intake,
    make_intake,
    owned_rows,
    world,
)

TEXT = "EXAMPLE_PRIVATE_INSTRUCTIONS_5591\nPrefer explicit names.\n"


def _profile(tenant: uuid.UUID, principal: uuid.UUID) -> None:
    _rows(
        "INSERT INTO curie.requester_coding_profiles "
        "(tenant_id, principal_id, text, revision, sha256, format_version) "
        "VALUES (:t, :p, :text, 1, :sha, 1)",
        {
            "t": tenant,
            "p": principal,
            "text": TEXT,
            "sha": hashlib.sha256(TEXT.encode()).hexdigest(),
        },
    )


def test_trusted_admission_pins_selected_metadata_and_pending_reference(
    dispatch_client: TestClient, world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", "true")
    from curie_api.config import get_settings

    get_settings.cache_clear()
    _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5591001, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)
    response = _admit(dispatch_client, body)
    assert response.status_code == 200, response.text
    item, request = response.json()["work_item"], response.json()["request"]
    assert item.get("requester_principal_id") == str(world.owner)
    assert item["coding_instructions"] == {
        "state": "selected",
        "absence_reason": None,
        "revision": 1,
        "sha256": hashlib.sha256(TEXT.encode()).hexdigest(),
        "format_version": 1,
    }
    assert request["coding_instructions_selection_id"] == item["id"]
    assert request["coding_instructions_evidence"] == "pending"
    assert "EXAMPLE_PRIVATE_INSTRUCTIONS_5591" not in response.text


@pytest.fixture
def links():
    namespaces: list[uuid.UUID] = []

    def link(tenant, principal, account, *, host="github.com", revoked=False, disabled=False):
        namespace = uuid.uuid4()
        namespaces.append(namespace)
        _rows(
            "INSERT INTO curie.identity_namespaces "
            "(id, tenant_id, provider, kind, authority, key, status) "
            "VALUES (:id, :t, 'github', 'github_host', :host, :host, :status)",
            {
                "id": namespace,
                "t": tenant,
                "host": host,
                "status": "disabled" if disabled else "active",
            },
        )
        _rows(
            "INSERT INTO curie.identity_links "
            "(id, tenant_id, identity_namespace_id, provider_native_id, principal_id, "
            "verification_source, created_by_principal_id, revoked_at) "
            "VALUES (:id, :t, :n, :account, :p, 'admin_mapped', :p, "
            "CASE WHEN :revoked THEN now() ELSE NULL END)",
            {
                "id": uuid.uuid4(),
                "t": tenant,
                "n": namespace,
                "account": str(account),
                "p": principal,
                "revoked": revoked,
            },
        )

    yield link
    for namespace in namespaces:
        _rows(
            "DELETE FROM curie.identity_links WHERE identity_namespace_id = :id", {"id": namespace}
        )
        _rows("DELETE FROM curie.identity_namespaces WHERE id = :id", {"id": namespace})


def _stored(number: int):
    return _rows(
        "SELECT w.*, r.coding_instructions_selection_id, r.coding_instructions_evidence, "
        "s.text AS snapshot_text FROM curie.work_items w "
        "JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "LEFT JOIN curie.work_item_coding_snapshots s ON s.work_item_id = w.id "
        "WHERE w.tracker_issue_id = :issue ORDER BY r.sequence",
        {"issue": str(number)},
    )


def _application(monkeypatch, enabled):
    from curie_api.config import get_settings

    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", str(enabled).lower())
    get_settings.cache_clear()


@pytest.mark.parametrize(
    "state",
    ["active", "revoked", "inactive", "foreign", "unlinked", "namespace_disabled", "wrong_host"],
)
def test_github_actor_resolution_uses_verified_account_and_active_tenant_link(
    github_factory, owned_rows, links, monkeypatch, state
):
    from forge_fakes.github import _post
    from test_personal_execution_admission import (
        DEFAULT_TENANT,
        _owner_text_event,
        _seed_default_tenant_owner,
    )

    _application(monkeypatch, True)
    principal = _seed_default_tenant_owner()
    client, github = github_factory(None)
    _profile(DEFAULT_TENANT, principal)
    github.label_actor["login"] = str(principal)
    if state == "foreign":
        from test_personal_execution_admission import _principal, _run, _tenant

        async def seed(connection):
            tenant = await _tenant(connection)
            return tenant, await _principal(connection, tenant)

        tenant, foreign = _run(seed)
        links(tenant, foreign, github.label_actor["id"])
    elif state != "unlinked":
        links(
            DEFAULT_TENANT,
            principal,
            github.label_actor["id"],
            revoked=state == "revoked",
            disabled=state == "namespace_disabled",
            host="other.example.com" if state == "wrong_host" else "github.com",
        )
    if state == "inactive":
        _rows("UPDATE curie.principals SET status = 'disabled' WHERE id = :id", {"id": principal})
    number = 5591100 + [
        "active",
        "revoked",
        "inactive",
        "foreign",
        "unlinked",
        "namespace_disabled",
        "wrong_host",
    ].index(state)
    response = _post(client, "issues", _owner_text_event(number, principal))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted", response.text
    [row] = _stored(number)
    assert row["owner_principal_id"] is None
    assert row["enrollment_generation"] is None
    if state == "active":
        assert row["requester_principal_id"] == principal
        assert row["snapshot_text"] == TEXT
        assert row["coding_instructions_evidence"] == "pending"
    else:
        assert row["requester_principal_id"] is None
        assert row["coding_instructions_absence_reason"] == "no_verified_requester"
        assert row["snapshot_text"] is None
        assert row["coding_instructions_evidence"] == "none"


@pytest.mark.parametrize("enrolled", [True, False])
def test_linked_github_actor_supplies_the_personal_owner(
    github_factory, owned_rows, links, monkeypatch, enrolled
):
    from forge_fakes.github import _post
    from test_personal_execution_admission import (
        DEFAULT_TENANT,
        _owner_text_event,
        _seed_default_tenant_owner,
    )

    _application(monkeypatch, True)
    principal = _seed_default_tenant_owner()
    client, github = github_factory("personal")
    links(DEFAULT_TENANT, principal, github.label_actor["id"])
    if not enrolled:
        _rows(
            "UPDATE curie.personal_claude_enrollments SET active = false WHERE principal_id = :p",
            {"p": principal},
        )
    number = 5591200 + enrolled
    response = _post(client, "issues", _owner_text_event(number, principal))
    assert response.status_code == 200, response.text
    if enrolled:
        assert response.json()["status"] == "factory_admitted", response.text
        [row] = _stored(number)
        assert row["requester_principal_id"] == row["owner_principal_id"] == principal
    else:
        assert response.json()["status"] == "factory_ignored", response.text
        assert _stored(number) == []


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("door", ["webhook", "poll", "reconcile", "internal", "generic"])
def test_each_admission_door_persists_selection_or_explicit_absence(
    request, monkeypatch, owned_rows, links, enabled, door
):
    from datetime import timedelta

    from forge_fakes.github import LABEL, _issue_event, _post
    from test_personal_execution_admission import DEFAULT_TENANT, _seed_default_tenant_owner

    _application(monkeypatch, enabled)
    principal = _seed_default_tenant_owner()
    _profile(DEFAULT_TENANT, principal)
    if door == "webhook":
        client, github = request.getfixturevalue("github_factory")(None)
        links(DEFAULT_TENANT, principal, github.label_actor["id"])
        number = 5591300 + enabled
        response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
        assert response.json()["status"] == "factory_admitted", response.text
    elif door == "poll":
        from test_factory_poll_intake import _run_once

        client, github = request.getfixturevalue("poll_factory")
        links(DEFAULT_TENANT, principal, github.label_actor["id"])
        number = github.plant_issue()
        _run_once(github)
    elif door == "reconcile":
        from test_factory_label_reconcile import _reconcile

        client, github = request.getfixturevalue("reconcile_factory")
        links(DEFAULT_TENANT, principal, github.label_actor["id"])
        number = 5591400 + enabled
        github.label(number, age=timedelta(minutes=10))
        assert _reconcile(github) == 1
    elif door == "internal":
        client = request.getfixturevalue("dispatch_client")
        w = request.getfixturevalue("world")
        principal = w.owner
        _profile(w.tenant, principal)
        number = 5591500 + enabled
        body = _body(w.agent, ADDRESS, number, uuid.uuid4())
        body["requester_principal_id"] = str(principal)
        response = _admit(client, body)
        assert response.status_code == 200, response.text
    else:
        from test_factory_tracker_intake import STARTER_ID

        intake = request.getfixturevalue("jira_intake")(None)
        from test_personal_execution_admission import _owner_text_label

        intake.agent()
        issue, _ = _owner_text_label(intake, principal)
        intake.poll()
        rows = _rows("SELECT tracker_issue_id FROM curie.work_items")
        assert len(rows) == 1
        number = int(rows[0]["tracker_issue_id"])
    [row] = _stored(number)
    assert row["coding_instructions_selection_id"] == row["id"]
    if enabled and door != "generic":
        assert row["coding_instructions_state"] == "selected"
        assert row["coding_instructions_revision"] == 1
        assert row["coding_instructions_sha256"] == hashlib.sha256(TEXT.encode()).hexdigest()
        assert row["snapshot_text"] == TEXT
        assert row["coding_instructions_evidence"] == "pending"
    else:
        reason = "no_verified_requester" if enabled else "application_unavailable"
        assert row["coding_instructions_state"] == "absent"
        assert row["coding_instructions_absence_reason"] == reason
        assert row["snapshot_text"] is None
        assert row["coding_instructions_evidence"] == "none"


@pytest.mark.parametrize("which", ["foreign", "inactive", "missing", "owner_mismatch"])
def test_internal_requester_reference_is_validated_without_using_requester_text(
    dispatch_client, github_factory, jira_intake, make_intake, world, monkeypatch, which
):
    _application(monkeypatch, True)
    principal = {
        "foreign": world.foreign,
        "inactive": world.disabled,
        "missing": uuid.uuid4(),
        "owner_mismatch": world.peer,
    }[which]
    body = _body(world.agent, ADDRESS, 5591600, uuid.uuid4(), requester=str(world.owner))
    body["requester_principal_id"] = str(principal)
    if which == "owner_mismatch":
        body.update(model_auth_mode="personal", owner=world.owner_context())
    response = _admit(dispatch_client, body)
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": {"code": "requester_principal_invalid"}}
    assert _stored(5591600) == []


def test_disabled_policy_never_copies_the_document(dispatch_client, world, monkeypatch):
    _application(monkeypatch, True)
    _profile(world.tenant, world.owner)
    _rows(
        "INSERT INTO curie.requester_coding_policies (tenant_id, enabled) VALUES (:t, false)",
        {"t": world.tenant},
    )
    body = _body(world.agent, ADDRESS, 5591700, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)
    response = _admit(dispatch_client, body)
    assert response.status_code == 200, response.text
    [row] = _stored(5591700)
    assert row["coding_instructions_absence_reason"] == "tenant_disabled"
    assert row["snapshot_text"] is None
    assert "EXAMPLE_PRIVATE_INSTRUCTIONS_5591" not in response.text


@pytest.mark.parametrize("failure_table", ["execution_requests", "work_item_coding_snapshots"])
def test_a_database_failure_after_selection_leaves_no_partial_admission(
    dispatch_client, world, monkeypatch, failure_table
):
    from curie_api.requester_instruction_admission import CodingInstructionsUnavailable

    _application(monkeypatch, True)
    _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5592000, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)
    _rows(
        f"ALTER TABLE curie.{failure_table} "
        "ADD CONSTRAINT test_5591_failure CHECK (false) NOT VALID"
    )
    try:
        if failure_table == "execution_requests":
            response = _admit(dispatch_client, body)
            assert response.status_code == 409, response.text
            assert "EXAMPLE_PRIVATE_INSTRUCTIONS_5591" not in response.text
        else:
            with pytest.raises(CodingInstructionsUnavailable) as caught:
                _admit(dispatch_client, body)
            assert str(caught.value) == "coding_instructions_unavailable"
            assert caught.value.__suppress_context__ is True
        assert _rows("SELECT id FROM curie.work_items") == []
        assert _rows("SELECT id FROM curie.execution_requests") == []
        assert _rows("SELECT work_item_id FROM curie.work_item_coding_snapshots") == []
    finally:
        _rows(f"ALTER TABLE curie.{failure_table} DROP CONSTRAINT test_5591_failure")
    assert _admit(dispatch_client, body).status_code == 200
    assert _stored(5592000)[0]["snapshot_text"] == TEXT


@pytest.mark.parametrize("first_write", [False, True])
def test_admission_serializes_with_a_replacement_or_first_profile_write(
    dispatch_client, world, monkeypatch, first_write
):
    import asyncio

    from curie_api.config import get_settings
    from curie_api.requester_coding_profiles import write_profile
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from test_requester_coding_selection import _wait_for_lock

    _application(monkeypatch, True)
    if not first_write:
        _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5592100, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)

    async def run():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as writer, AsyncSession(engine) as observer:
                await write_profile(
                    writer,
                    tenant_id=world.tenant,
                    principal_id=world.owner,
                    value="replacement document",
                    expected_revision=0 if first_write else 1,
                )
                admission = asyncio.create_task(asyncio.to_thread(_admit, dispatch_client, body))
                deadline = asyncio.get_running_loop().time() + 10
                try:
                    while asyncio.get_running_loop().time() < deadline:
                        blocked = await observer.scalar(
                            text(
                                "SELECT pid FROM pg_stat_activity "
                                "WHERE datname = current_database() "
                                "AND wait_event_type = 'Lock' AND query LIKE '%principals%' LIMIT 1"
                            )
                        )
                        await observer.rollback()
                        if blocked is not None:
                            await _wait_for_lock(observer, blocked, admission)
                            break
                    else:
                        pytest.fail("the admission never waited for the profile writer")
                finally:
                    await writer.commit()
                response = await admission
                assert response.status_code == 200, response.text
        finally:
            await engine.dispose()

    asyncio.run(run())
    [row] = _stored(5592100)
    assert row["snapshot_text"] == "replacement document"
    assert row["coding_instructions_revision"] == (1 if first_write else 2)
    assert row["coding_instructions_sha256"] == hashlib.sha256(b"replacement document").hexdigest()


@pytest.mark.parametrize("selected", [False, True])
def test_owner_peer_and_platform_reads_expose_only_selection_metadata(
    dispatch_client, world, monkeypatch, selected
):
    import personal_enrollment_support as enrollment
    from curie_api.config import get_settings
    from test_personal_execution_admission import WORKER_HEADERS

    _application(monkeypatch, selected)
    issuer = "https://issuer.example.com"
    monkeypatch.setenv("CURIE_OIDC_ISSUER", issuer)
    monkeypatch.setenv("CURIE_OIDC_AUDIENCE", "acme-console")
    monkeypatch.setenv("CURIE_OIDC_REDIRECT_URI", "https://console.example.com/oidc/callback")
    monkeypatch.setenv("CURIE_OIDC_JWKS_URL", issuer + "/jwks")
    monkeypatch.setenv("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "true")
    get_settings.cache_clear()
    _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5592200, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)
    admitted = _admit(dispatch_client, body)
    assert admitted.status_code == 200, admitted.text
    item = admitted.json()["work_item"]
    owners = [
        enrollment.sign_in(world.tenant, principal, issuer=issuer)
        for principal in (world.owner, world.peer)
    ]
    headers = [owner.headers(origin=None) for owner in owners]
    headers.append({"X-API-Key": get_settings().api_key})
    for auth in headers:
        for path in (f"/work-items/{item['id']}", f"/work-items?agent_id={world.agent}"):
            response = dispatch_client.get(path, headers=auth)
            assert response.status_code == 200, response.text
            assert "EXAMPLE_PRIVATE_INSTRUCTIONS_5591" not in response.text
            projected = response.json().get("items", [response.json()])[0]
            assert projected["coding_instructions"] == item["coding_instructions"]
            assert projected["requester_principal_id"] == str(world.owner)
    request_read = dispatch_client.get(
        f"/v1/internal/work-items/requests/{body['request_id']}",
        headers=WORKER_HEADERS,
    )
    assert request_read.status_code == 200, request_read.text
    assert "EXAMPLE_PRIVATE_INSTRUCTIONS_5591" not in request_read.text
    if not selected:
        assert _rows("SELECT * FROM curie.work_item_coding_snapshots") == []


@pytest.mark.parametrize("first_write", [False, True])
def test_request_insert_keeps_the_selection_lock_until_admission_commits(
    dispatch_client, world, monkeypatch, first_write
):
    import asyncio

    from curie_api.config import get_settings
    from curie_api.requester_coding_profiles import write_profile
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from test_requester_coding_selection import _wait_for_lock

    _application(monkeypatch, True)
    if not first_write:
        _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5592300, uuid.uuid4())
    body["requester_principal_id"] = str(world.owner)
    _rows(
        "CREATE FUNCTION curie.test_5591_gate() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN PERFORM pg_advisory_xact_lock(5591, 5591); RETURN NEW; END $$"
    )
    _rows(
        "CREATE TRIGGER test_5591_gate BEFORE INSERT ON curie.execution_requests "
        "FOR EACH ROW EXECUTE FUNCTION curie.test_5591_gate()"
    )

    async def run():
        engine = create_async_engine(get_settings().database_url)
        admission = None
        writing = None
        try:
            async with (
                AsyncSession(engine) as gate,
                AsyncSession(engine) as writer,
                AsyncSession(engine) as observer,
            ):
                await gate.execute(text("SELECT pg_advisory_xact_lock(5591, 5591)"))
                writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
                admission = asyncio.create_task(asyncio.to_thread(_admit, dispatch_client, body))
                try:
                    deadline = asyncio.get_running_loop().time() + 10
                    while asyncio.get_running_loop().time() < deadline:
                        blocked = await observer.scalar(
                            text(
                                "SELECT pid FROM pg_stat_activity "
                                "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                                "AND query LIKE 'INSERT INTO curie.execution_requests%' LIMIT 1"
                            )
                        )
                        await observer.rollback()
                        if blocked is not None:
                            break
                        assert not admission.done(), "admission bypassed the request insert"
                    else:
                        pytest.fail("admission never reached the gated request insert")
                    writing = asyncio.create_task(
                        write_profile(
                            writer,
                            tenant_id=world.tenant,
                            principal_id=world.owner,
                            value="replacement document",
                            expected_revision=0 if first_write else 1,
                        )
                    )
                    await _wait_for_lock(observer, writer_pid, writing)
                finally:
                    await gate.rollback()
                response = await admission
                assert response.status_code == 200, response.text
                await writing
                await writer.commit()
        finally:
            if admission is not None:
                await asyncio.gather(admission, return_exceptions=True)
            if writing is not None:
                await asyncio.gather(writing, return_exceptions=True)
            await engine.dispose()

    try:
        asyncio.run(run())
    finally:
        _rows("DROP TRIGGER test_5591_gate ON curie.execution_requests")
        _rows("DROP FUNCTION curie.test_5591_gate()")
    [row] = _stored(5592300)
    if first_write:
        assert row["coding_instructions_absence_reason"] == "no_profile"
        assert row["snapshot_text"] is None
    else:
        assert row["coding_instructions_revision"] == 1
        assert row["snapshot_text"] == TEXT
    assert _rows(
        "SELECT text FROM curie.requester_coding_profiles WHERE principal_id = :p",
        {"p": world.owner},
    ) == [{"text": "replacement document"}]


@pytest.mark.parametrize("tracker_kind", ["gitlab", "jira_cloud"])
def test_other_tracker_identities_are_explicitly_absent_even_when_text_names_an_owner(
    dispatch_client, world, monkeypatch, tracker_kind
):
    _application(monkeypatch, True)
    _profile(world.tenant, world.owner)
    body = _body(world.agent, ADDRESS, 5592400, uuid.uuid4(), requester=str(world.owner))
    body["tracker"].update(kind=tracker_kind, host="tracker.example.com")
    response = _admit(dispatch_client, body)
    assert response.status_code == 200, response.text
    [row] = _stored(5592400)
    assert row["requester_principal_id"] is None
    assert row["coding_instructions_absence_reason"] == "no_verified_requester"
    assert row["snapshot_text"] is None
    assert row["coding_instructions_evidence"] == "none"
