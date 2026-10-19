"""Generic compatibility gates use current tracker bytes and immutable identity.

Provider reads and database lookups are replaced with fixture transports; the
production parser, admission gate and start gate execute unchanged.
"""

import asyncio
import hashlib
import json
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from curie_api import input_contracts
from curie_api.input_contracts import InputCompatibilityError
from plugin_format.input_contracts import INPUT_CONTRACT_ENFORCEMENT

CONTRACT = "example.specification/v1"
AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT = uuid.UUID("22222222-2222-4222-8222-222222222222")
VERSION = uuid.UUID("33333333-3333-4333-8333-333333333333")
BUNDLE = "a" * 64
BODY = "## Problem\nImplement the agreed change.\n"


def envelope(**overrides: Any) -> str:
    header = {
        "contract": CONTRACT,
        "deployment": str(DEPLOYMENT),
        "bundle_sha256": BUNDLE,
        "body_sha256": hashlib.sha256(BODY.encode()).hexdigest(),
        **overrides,
    }
    return "<!-- curie-input@1 " + json.dumps(header, separators=(",", ":")) + " -->\n" + BODY


def selected(*, declaration: Any = ...) -> tuple[SimpleNamespace, SimpleNamespace]:
    if declaration is ...:
        declaration = {
            "interface": INPUT_CONTRACT_ENFORCEMENT,
            "accepts": [CONTRACT],
            "required": True,
        }
    deployment = SimpleNamespace(id=DEPLOYMENT, agent_id=AGENT, version_id=VERSION, status="active")
    version = SimpleNamespace(
        id=VERSION, agent_id=AGENT, bundle_sha256=BUNDLE, input_contracts=declaration
    )
    return deployment, version


def install_transports(
    monkeypatch: pytest.MonkeyPatch, *, body: str | None = None, declaration: Any = ...
) -> tuple[AsyncMock, AsyncMock]:
    lookup = AsyncMock(return_value=selected(declaration=declaration))
    read = AsyncMock(return_value=envelope() if body is None else body)
    monkeypatch.setattr(input_contracts, "_selected_deployment", lookup)
    monkeypatch.setattr(input_contracts, "_read_input_body", read)
    return lookup, read


def admit() -> dict[str, Any] | None:
    return asyncio.run(
        input_contracts.check_admission_input(SimpleNamespace(), SimpleNamespace(agent_id=AGENT))
    )


def test_input_compatibility_errors_use_safe_codes() -> None:
    error = InputCompatibilityError("input_contract_mismatch")
    assert error.code == "input_contract_mismatch"
    assert BODY not in str(error)


def test_accepted_input_receipt_records_identity_and_hash_without_ticket_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, read = install_transports(monkeypatch)
    receipt = admit()
    assert receipt == {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "contract": CONTRACT,
        "deployment_id": str(DEPLOYMENT),
        "version_id": str(VERSION),
        "bundle_sha256": BUNDLE,
        "body_sha256": hashlib.sha256(BODY.encode()).hexdigest(),
    }
    assert BODY not in json.dumps(receipt)
    read.assert_awaited_once()


def test_legacy_undeclared_bundle_retains_admission_without_new_tracker_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, read = install_transports(monkeypatch, declaration=None)
    assert admit() is None
    read.assert_not_awaited()


@pytest.mark.parametrize(
    "body",
    [
        "ordinary legacy issue body",
        envelope(contract="example.specification/v2"),
        envelope(deployment="44444444-4444-4444-8444-444444444444"),
        envelope(bundle_sha256="b" * 64),
        envelope() + "new edit\n",
    ],
)
def test_incompatible_or_edited_input_is_refused(
    monkeypatch: pytest.MonkeyPatch, body: str
) -> None:
    install_transports(monkeypatch, body=body)
    with pytest.raises(InputCompatibilityError) as caught:
        admit()
    assert caught.value.code == "input_contract_mismatch"
    assert body not in str(caught.value)


@pytest.mark.parametrize(
    "declaration",
    [None, {"interface": INPUT_CONTRACT_ENFORCEMENT, "accepts": [], "required": True}],
)
def test_bad_bundle_identity_or_declaration_is_unknown(
    monkeypatch: pytest.MonkeyPatch, declaration: Any
) -> None:
    lookup, _ = install_transports(monkeypatch, declaration=declaration)
    deployment, version = selected(declaration=declaration)
    version.bundle_sha256 = None
    lookup.return_value = (deployment, version)
    if declaration is None:
        # Undeclared bundles are explicitly legacy; an absent digest must not
        # accidentally opt them into compatibility enforcement.
        assert admit() is None
    else:
        with pytest.raises(InputCompatibilityError) as caught:
            admit()
        assert caught.value.code == "input_contract_unknown"


def test_tracker_read_failure_is_unknown_and_does_not_expose_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, read = install_transports(monkeypatch)
    read.side_effect = InputCompatibilityError("input_contract_unknown")
    with pytest.raises(InputCompatibilityError) as caught:
        admit()
    assert caught.value.code == "input_contract_unknown"
    assert "private upstream details" not in str(caught.value)


def start(receipt: dict[str, Any] | None, **identity: Any) -> None:
    asyncio.run(
        input_contracts.check_start_input(
            SimpleNamespace(),
            SimpleNamespace(
                agent_id=AGENT, tracker_issue=None, repository=None, code_host_installation_id=None
            ),
            SimpleNamespace(input_contract_receipt=receipt),
            runtime_deployment_id=identity.get("deployment", DEPLOYMENT),
            runtime_version_id=identity.get("version", VERSION),
            runtime_bundle_sha256=identity.get("bundle", BUNDLE),
        )
    )


def test_start_rechecks_receipt_and_current_tracker_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _, read = install_transports(monkeypatch)
    receipt = admit()
    start(receipt)
    assert read.await_count == 2


@pytest.mark.parametrize(
    "identity",
    [
        {"deployment": uuid.UUID("44444444-4444-4444-8444-444444444444")},
        {"version": uuid.UUID("55555555-5555-4555-8555-555555555555")},
        {"bundle": "b" * 64},
        {"deployment": None},
        {"version": None},
        {"bundle": None},
    ],
)
def test_start_refuses_a_runtime_that_does_not_match_accepted_input(
    monkeypatch: pytest.MonkeyPatch, identity: dict[str, Any]
) -> None:
    install_transports(monkeypatch)
    receipt = admit()
    with pytest.raises(InputCompatibilityError) as caught:
        start(receipt, **identity)
    assert caught.value.code == "input_contract_mismatch"


def test_start_refuses_ticket_edit_after_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    _, read = install_transports(monkeypatch)
    receipt = admit()
    read.return_value = envelope() + "edited after approval\n"
    with pytest.raises(InputCompatibilityError) as caught:
        start(receipt)
    assert caught.value.code == "input_contract_mismatch"


def test_start_refuses_deployment_change_after_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    lookup, _ = install_transports(monkeypatch)
    receipt = admit()
    deployment, version = selected()
    deployment.id = uuid.UUID("44444444-4444-4444-8444-444444444444")
    lookup.return_value = (deployment, version)
    with pytest.raises(InputCompatibilityError):
        start(receipt)


@pytest.mark.parametrize("entrypoint", ["admit", "readmit", "admit_revision"])
def test_central_admission_refuses_incompatible_input_before_allocating_work(
    monkeypatch: pytest.MonkeyPatch, entrypoint: str
) -> None:
    from curie_api import workitem_dispatch
    from curie_api.forges.types import RepositoryRef, TrackerIssueRef

    install_transports(monkeypatch, body=envelope(contract="example.specification/v99"))
    monkeypatch.setattr(
        workitem_dispatch,
        "_admission_refusal",
        AsyncMock(return_value=SimpleNamespace(adapter=None)),
    )
    allocate = AsyncMock(side_effect=AssertionError("must refuse before allocating work"))
    monkeypatch.setattr(workitem_dispatch, "_admit_new", allocate)
    create_revision = AsyncMock(side_effect=AssertionError("must not allocate a revision"))
    monkeypatch.setattr(workitem_dispatch.lifecycle, "create_revision_request", create_revision)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None), rollback=AsyncMock(), commit=AsyncMock()
    )
    facts = SimpleNamespace(
        agent_id=AGENT,
        request_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
        issue=TrackerIssueRef("github", "github.com", "fixture", "1"),
        repository=RepositoryRef("github", "github.com", "fixture", "example/repository"),
        code_host_installation_id=None,
    )
    result = asyncio.run(getattr(workitem_dispatch, entrypoint)(session, facts))
    assert result.code == "input_contract_mismatch"
    allocate.assert_not_awaited()
    create_revision.assert_not_awaited()


def test_admission_carries_verified_receipt_from_normalized_namespace_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api import workitem_dispatch
    from curie_api.forges.types import RepositoryRef, TrackerIssueRef

    install_transports(monkeypatch)
    monkeypatch.setattr(
        workitem_dispatch,
        "_admission_refusal",
        AsyncMock(return_value=SimpleNamespace(adapter=None)),
    )
    allocate = AsyncMock(return_value=SimpleNamespace(accepted=True))
    monkeypatch.setattr(workitem_dispatch, "_admit_new", allocate)
    session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    facts = SimpleNamespace(
        agent_id=AGENT,
        request_id=uuid.uuid4(),
        kind="github",
        address="example/repository",
        reply_conversation_id="issue-1",
        issue=TrackerIssueRef("github", "github.com", "fixture", "1"),
        repository=RepositoryRef("github", "github.com", "fixture", "example/repository"),
        code_host_installation_id=None,
        objective="Implement the reviewed behavior",
        requester="fixture-user",
        reply_url="https://github.com/example/repository/issues/1",
    )

    result = asyncio.run(workitem_dispatch.admit(session, facts))

    assert result.accepted
    allocated = allocate.await_args.args[1]
    assert allocated.input_contract_receipt == admit()
    assert allocated.issue == facts.issue
    assert allocated.repository == facts.repository
    assert allocated.reply_url == facts.reply_url
    assert not hasattr(facts, "input_contract_receipt")


def test_central_start_refuses_changed_runtime_before_mutating_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime, timedelta

    from curie_api import workitem_dispatch

    install_transports(monkeypatch)
    receipt = admit()
    now = datetime.now(UTC)
    context = workitem_dispatch.FactoryContext(
        agent_id=AGENT,
        tenant_id=uuid.uuid4(),
        binding_id=uuid.uuid4(),
        kind="github",
        address="example/repository",
        adapter=None,
    )
    monkeypatch.setattr(workitem_dispatch, "_factory_context", AsyncMock(return_value=context))
    item = SimpleNamespace(id=uuid.uuid4(), agent_id=AGENT, version=1, cancelled_at=None)
    request = SimpleNamespace(
        id=uuid.uuid4(),
        version=1,
        input_contract_receipt=receipt,
        status="waiting",
        wait_deadline=now + timedelta(minutes=1),
        acquire_owner="fixture-worker",
        acquired_generation=1,
        acquire_expires_at=now + timedelta(minutes=1),
    )
    monkeypatch.setattr(workitem_dispatch, "_lock_pair", AsyncMock(return_value=(item, request)))
    monkeypatch.setattr(workitem_dispatch, "database_now", AsyncMock(return_value=now))
    mutate = AsyncMock(side_effect=AssertionError("must refuse before execution mutation"))
    monkeypatch.setattr(workitem_dispatch, "start_execution_where", mutate)
    session = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    result = asyncio.run(
        workitem_dispatch.start(
            session,
            request.id,
            owner="fixture-worker",
            generation=1,
            claim_name="fixture-claim",
            sandbox_name="fixture-sandbox",
            context=context,
            runtime_deployment_id=DEPLOYMENT,
            runtime_version_id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
            runtime_bundle_sha256=BUNDLE,
        )
    )
    assert result.code == "input_contract_mismatch"
    mutate.assert_not_awaited()


def test_deployment_contract_projection_reports_only_current_enforced_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_transports(monkeypatch)
    deployment, _ = selected()
    session = SimpleNamespace(get=AsyncMock(return_value=deployment))
    result = asyncio.run(input_contracts.deployment_contracts(session, DEPLOYMENT))
    assert result == {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "deployment_id": str(DEPLOYMENT),
        "agent_id": str(AGENT),
        "version_id": str(VERSION),
        "bundle_sha256": BUNDLE,
        "accepted_contracts": [CONTRACT],
        "status": "active",
        "enforced": True,
    }


@pytest.mark.parametrize("case", ["missing", "inactive", "superseded", "legacy"])
def test_unavailable_deployment_does_not_claim_compatibility(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    lookup, _ = install_transports(monkeypatch, declaration=None if case == "legacy" else ...)
    deployment, version = selected()
    if case == "inactive":
        deployment.status = "inactive"
    if case == "superseded":
        newer, _ = selected()
        newer.id = uuid.UUID("44444444-4444-4444-8444-444444444444")
        lookup.return_value = (newer, version)
    session = SimpleNamespace(get=AsyncMock(return_value=None if case == "missing" else deployment))
    with pytest.raises(InputCompatibilityError) as caught:
        asyncio.run(input_contracts.deployment_contracts(session, DEPLOYMENT))
    assert caught.value.code == "input_contract_unknown"


def test_deployment_contract_http_requires_auth_and_returns_typed_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api import auth, deps
    from curie_api.routers.deployments import router
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    response = {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "deployment_id": str(DEPLOYMENT),
        "agent_id": str(AGENT),
        "version_id": str(VERSION),
        "bundle_sha256": BUNDLE,
        "accepted_contracts": [CONTRACT],
        "status": "active",
        "enforced": True,
    }
    read = AsyncMock(return_value=response)
    monkeypatch.setattr(input_contracts, "deployment_contracts", read)
    monkeypatch.setattr(auth, "get_settings", lambda: SimpleNamespace(api_key="fixture-api-key"))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[deps.get_session] = lambda: SimpleNamespace()
    with TestClient(app) as client:
        path = f"/deployments/{DEPLOYMENT}/input-contracts"
        assert client.get(path).status_code == 401
        assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401
        read.assert_not_awaited()
        accepted = client.get(path, headers={"X-API-Key": "fixture-api-key"})
        assert accepted.status_code == 200
        assert accepted.json() == response
        read.side_effect = InputCompatibilityError("input_contract_unknown")
        refused = client.get(path, headers={"X-API-Key": "fixture-api-key"})
        assert refused.status_code == 409
        assert refused.json() == {"detail": "input_contract_unknown"}


def stored_archive(*, declared: bool = True) -> bytes:
    import io
    import tarfile

    manifest: dict[str, Any] = {"name": "example-coworker"}
    if declared:
        manifest["inputContracts"] = {
            "interface": INPUT_CONTRACT_ENFORCEMENT,
            "accepts": [CONTRACT],
            "required": True,
        }
    payload = json.dumps(manifest).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        member = tarfile.TarInfo(".claude-plugin/plugin.json")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


def stored_transports(
    monkeypatch: pytest.MonkeyPatch,
    *,
    declared: bool = True,
    data: bytes | None = None,
) -> tuple[SimpleNamespace, AsyncMock]:
    raw = stored_archive(declared=declared) if data is None else data
    digest = hashlib.sha256(raw).hexdigest()
    lookup, _ = install_transports(
        monkeypatch, declaration=None, body=envelope(bundle_sha256=digest)
    )
    deployment, version = selected(declaration=None)
    version.bundle_ref = "bundles/fixture/immutable.tar.gz"
    version.bundle_sha256 = digest
    lookup.return_value = (deployment, version)
    get = AsyncMock(return_value=raw)
    # The fixture port deliberately has no put/update method. Reads must not
    # mutate a stored version merely to answer compatibility or admit input.
    monkeypatch.setattr(input_contracts, "BundleStore", lambda _settings: SimpleNamespace(get=get))
    return version, get


def test_uncached_stored_declaration_is_read_from_verified_immutable_archive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version, get = stored_transports(monkeypatch)
    receipt = admit()
    assert receipt is not None
    assert receipt["contract"] == CONTRACT
    assert receipt["bundle_sha256"] == version.bundle_sha256
    assert version.input_contracts is None, "compatibility reads must not write a cache through GET"
    get.assert_awaited_once_with(version.bundle_ref)


def test_verified_undeclared_archive_preserves_legacy_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version, get = stored_transports(monkeypatch, declared=False)
    assert admit() is None
    assert version.input_contracts is None
    get.assert_awaited_once()


@pytest.mark.parametrize("case", ["digest", "archive", "provider"])
def test_unverifiable_stored_archive_is_unknown_instead_of_legacy(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    from botocore.exceptions import ClientError

    version, get = stored_transports(
        monkeypatch, data=b"not an archive" if case == "archive" else None
    )
    if case == "digest":
        version.bundle_sha256 = "f" * 64
    if case == "provider":
        get.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "private storage details"}}, "GetObject"
        )
    with pytest.raises(InputCompatibilityError) as caught:
        admit()
    assert caught.value.code == "input_contract_unknown"
    assert "private storage details" not in str(caught.value)


def test_new_unreceipted_request_cannot_start_against_a_now_declared_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_transports(monkeypatch)
    with pytest.raises(InputCompatibilityError) as caught:
        start(None)
    assert caught.value.code == "input_contract_unknown"


def test_unreceipted_undeclared_legacy_request_keeps_start_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_transports(monkeypatch, declaration=None)
    start(None)


def test_running_legacy_request_is_not_reinterpreted_after_deployment_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime

    _, read = install_transports(monkeypatch)
    asyncio.run(
        input_contracts.check_start_input(
            SimpleNamespace(),
            SimpleNamespace(agent_id=AGENT),
            SimpleNamespace(input_contract_receipt=None, started_at=datetime.now(UTC)),
            runtime_deployment_id=DEPLOYMENT,
            runtime_version_id=VERSION,
            runtime_bundle_sha256=BUNDLE,
        )
    )
    read.assert_not_awaited()
