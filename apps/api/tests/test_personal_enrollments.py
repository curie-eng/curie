"""Owner enrollment through a principal session (#5377 AC1, ADR 0268 section 3).

``GET``, ``PUT`` and ``DELETE /personal-claude/enrollment`` act on the caller's
own enrollment header, with tenant and principal taken from a live principal
console session minted for a principal of the configured OIDC issuer (the
repo's in-process ``TestIdP``). Each test runs the real application with its
lifespan over HTTP against real Postgres; only the Kubernetes Secret API is
faked, behind ``custody_store.CustodySecretStore``. Every refusal sits beside
the accepted neighbour that proves the refusal is not a broken route.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import personal_enrollment_support as support
import personal_execution_support as pe
import pytest
from curie_api import personal_enrollments
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_telemetry.tracing import configure_tracer_provider
from fastapi.testclient import TestClient
from oidc_test_idp import TestIdP
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from personal_enrollment_support import FakeCustodyStore, Owner
from sqlalchemy.ext.asyncio import AsyncSession

VECTOR = Path(__file__).resolve().parents[3] / "tests/vectors/personal-enrollment-token.json"
SENTINEL = "SENTINELmalformed0123456789"


@pytest.fixture
def owned(clean_db: None) -> Iterator[None]:
    pe.cleanup_owned()
    yield
    assert pe.cleanup_owned() == 0, "an owned tenant or principal survived"


@pytest.fixture
def idp(owned: None) -> Iterator[TestIdP]:
    with support.identity_provider() as provider:
        yield provider


@pytest.fixture
def store() -> FakeCustodyStore:
    return FakeCustodyStore()


@pytest.fixture
def api(idp: TestIdP, store: FakeCustodyStore) -> Iterator[TestClient]:
    with support.enrollment_app(store) as client:
        yield client


@pytest.fixture
def owner(api: TestClient, idp: TestIdP) -> Owner:
    return support.seed_owner(support.seed_tenant(), issuer=idp.issuer)


def _snapshot(owner: Owner, store: FakeCustodyStore) -> tuple[Any, ...]:
    """Everything a refused write must leave unchanged."""

    secrets = tuple((name, secret.uid) for (_, name), secret in sorted(store.secrets.items()))
    return support.header(owner), support.custody(owner), secrets


def _no_reference(text: str, *tokens: str) -> None:
    for fragment in (*support.REFERENCE_FRAGMENTS, *tokens):
        assert fragment not in text, fragment


# --- the lifecycle -------------------------------------------------------------------------


def test_enroll_creates_generation_one_through_a_principal_session(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    before = support.status(api, owner)
    assert support.body_of(before) == {
        "state": "absent",
        "generation": None,
        "version": None,
        "teardown": {"pending_runtimes": 0, "pending_custody_cleanup": 0},
    }

    enrolled = support.put(api, owner, "enroll")

    assert support.body_of(enrolled) == {
        "state": "active",
        "generation": 1,
        "version": 1,
        "teardown": {"pending_runtimes": 0, "pending_custody_cleanup": 0},
    }
    _no_reference(enrolled.text, support.TOKEN)
    header = support.header(owner)
    assert header is not None and (header["generation"], header["version"], header["active"]) == (
        1,
        1,
        True,
    )
    [row] = support.custody(owner)
    assert (row["enrollment_id"], row["generation"], row["state"]) == (header["id"], 1, "published")
    assert store.names() == [row["secret_name"]]
    assert store.grant(row["secret_name"]) == support.TOKEN
    assert support.body_of(support.status(api, owner))["generation"] == 1


def test_replace_disconnect_reconnect_advance_generation_and_version_monotonically(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    seen = [(1, 1, "active")]
    for step in ("replace", "disconnect", "reconnect", "replace"):
        version = seen[-1][1]
        if step == "disconnect":
            response = support.disconnect(api, owner, version)
        else:
            response = support.put(api, owner, step, version=version, token=support.REPLACEMENT)
        body = support.body_of(response)
        seen.append((body["generation"], body["version"], body["state"]))

    assert seen == [
        (1, 1, "active"),
        (2, 2, "active"),
        (3, 3, "inactive"),
        (4, 4, "active"),
        (5, 5, "active"),
    ]
    assert (
        len(
            pe.rows(
                "SELECT 1 FROM curie.personal_claude_enrollments WHERE tenant_id = :t "
                "AND principal_id = :p",
                {"t": owner.tenant_id, "p": owner.principal_id},
            )
        )
        == 1
    )
    # Disconnect publishes no material; every superseded Secret is retired and gone.
    assert support.custody_states(owner) == [
        (1, "retired", True),
        (2, "retired", True),
        (4, "retired", True),
        (5, "published", False),
    ]
    [published] = [row for row in support.custody(owner) if row["state"] == "published"]
    assert store.names() == [published["secret_name"]]


def test_stale_writes_conflict_and_change_nothing(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    support.body_of(support.disconnect(api, owner, 1))  # inactive at version 2
    support.body_of(support.put(api, owner, "reconnect", version=2))  # active at version 3
    unchanged = _snapshot(owner, store)
    creates = store.count("create")

    for response in (
        support.put(api, owner, "replace", version=2),
        support.put(api, owner, "reconnect", version=1),
        support.disconnect(api, owner, 2),
    ):
        assert support.refusal(response) == (409, "stale_version")
        assert _snapshot(owner, store) == unchanged
        assert store.count("create") == creates

    assert support.body_of(support.put(api, owner, "replace", version=3))["version"] == 4


def test_intent_state_mismatches_conflict_with_their_codes(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        409,
        "enrollment_absent",
    )
    assert support.refusal(support.put(api, owner, "reconnect", version=1)) == (
        409,
        "enrollment_absent",
    )
    assert support.refusal(support.disconnect(api, owner, 1)) == (404, "enrollment_absent")
    assert support.header(owner) is None and store.count("create") == 0

    support.enrolled(api, owner)
    assert support.refusal(support.put(api, owner, "enroll")) == (409, "enrollment_exists")
    assert support.refusal(support.put(api, owner, "reconnect", version=1)) == (
        409,
        "enrollment_active",
    )
    support.body_of(support.disconnect(api, owner, 1))
    assert support.refusal(support.put(api, owner, "replace", version=2)) == (
        409,
        "enrollment_inactive",
    )
    assert support.refusal(support.put(api, owner, "enroll")) == (409, "enrollment_exists")
    assert store.count("create") == 1

    assert support.body_of(support.put(api, owner, "reconnect", version=2))["state"] == "active"


def test_contended_owner_rows_answer_enrollment_busy_and_write_nothing(
    api: TestClient, owner: Owner, store: FakeCustodyStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging, publishing and disconnect wait at most the lock timeout, then 503."""

    support.enrolled(api, owner)
    first = support.custody(owner)[0]
    unchanged = _snapshot(owner, store)
    monkeypatch.setattr(personal_enrollments, "LOCK_TIMEOUT", "200ms")
    owned = {"t": owner.tenant_id, "p": owner.principal_id}

    # The header is held: Tx1 staging and the disconnect both time out first.
    with support.row_locked(
        "SELECT id FROM curie.personal_claude_enrollments "
        "WHERE tenant_id = :t AND principal_id = :p FOR UPDATE",
        owned,
    ):
        for response in (
            support.put(api, owner, "replace", version=1),
            support.disconnect(api, owner, 1),
        ):
            assert support.refusal(response) == (503, "enrollment_busy")
            assert _snapshot(owner, store) == unchanged
    assert store.count("create") == 1

    # The published custody row is held: Tx2 times out after the create, so the
    # writer abandons its row and removes only the Secret it created.
    with support.row_locked(
        "SELECT id FROM curie.personal_claude_custody WHERE id = :id FOR UPDATE",
        {"id": first["id"]},
    ):
        busy = support.put(api, owner, "replace", version=1, token=support.REPLACEMENT)
    assert support.refusal(busy) == (503, "enrollment_busy")
    assert support.header(owner) == unchanged[0]
    assert support.custody_states(owner) == [(1, "published", False), (2, "abandoned", True)]
    assert store.names() == [first["secret_name"]] and store.count("create") == 2

    assert support.body_of(support.put(api, owner, "replace", version=1))["generation"] == 2


def test_repeated_disconnect_at_the_inactive_version_is_idempotent(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    first = support.body_of(support.disconnect(api, owner, 1))
    assert (first["state"], first["generation"], first["version"]) == ("inactive", 2, 2)
    settled = _snapshot(owner, store)

    again = support.body_of(support.disconnect(api, owner, 2))
    assert again == first
    assert _snapshot(owner, store) == settled
    assert support.refusal(support.disconnect(api, owner, 1)) == (409, "stale_version")


def test_concurrent_same_version_writers_advance_exactly_once(
    idp: TestIdP, store: FakeCustodyStore
) -> None:
    """Two replicas, two Postgres sessions, both past the intent check before staging."""

    barrier = threading.Barrier(2, timeout=30)
    results: dict[str, Any] = {}
    with support.enrollment_app(store) as first, support.enrollment_app(store) as second:
        owner = support.seed_owner(support.seed_tenant(), issuer=idp.issuer)
        for client in (first, second):
            support.on_statement(client, "INSERT INTO curie.personal_claude_custody", barrier.wait)

        def writer(name: str, client: TestClient, token: str) -> None:
            results[name] = support.put(client, owner, "enroll", token=token)

        threads = [
            threading.Thread(target=writer, args=("first", first, support.TOKEN)),
            threading.Thread(target=writer, args=("second", second, support.REPLACEMENT)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            assert not thread.is_alive()

    answers = sorted(results.values(), key=lambda response: response.status_code)
    assert [response.status_code for response in answers] == [200, 409]
    assert support.refusal(answers[1]) == (409, "staging_conflict")
    assert support.body_of(answers[0])["generation"] == 1
    [published] = support.custody(owner)
    assert published["state"] == "published"
    # The loser never created material; the one Secret is the winner's.
    assert store.names() == [published["secret_name"]]
    assert store.count("create") == 1
    secret = store.get(published["secret_name"])
    assert secret is not None and secret.uid == published["secret_uid"]


def test_another_principal_cannot_read_or_mutate_the_owner_enrollment(
    api: TestClient, owner: Owner, store: FakeCustodyStore, idp: TestIdP
) -> None:
    support.enrolled(api, owner)
    colleague = support.seed_owner(owner.tenant_id, issuer=idp.issuer)
    unchanged = _snapshot(owner, store)

    assert support.body_of(support.status(api, colleague))["state"] == "absent"
    assert support.refusal(support.put(api, colleague, "replace", version=1)) == (
        409,
        "enrollment_absent",
    )
    assert support.refusal(support.disconnect(api, colleague, 1)) == (404, "enrollment_absent")
    assert _snapshot(owner, store) == unchanged

    # The colleague's own enroll makes the colleague's header only.
    assert support.body_of(support.put(api, colleague, "enroll"))["generation"] == 1
    assert _snapshot(owner, store)[:2] == unchanged[:2]
    assert support.header(colleague) is not None
    assert support.header(colleague)["id"] != unchanged[0]["id"]  # type: ignore[index]


def test_wrong_tenant_session_sees_only_its_own_enrollment(
    api: TestClient, owner: Owner, store: FakeCustodyStore, idp: TestIdP
) -> None:
    support.enrolled(api, owner)
    stranger = support.seed_owner(support.seed_tenant(), issuer=idp.issuer)
    unchanged = _snapshot(owner, store)

    assert support.body_of(support.status(api, stranger))["state"] == "absent"
    assert support.refusal(support.disconnect(api, stranger, 1)) == (404, "enrollment_absent")
    assert support.body_of(support.put(api, stranger, "enroll"))["generation"] == 1

    assert _snapshot(owner, store)[:2] == unchanged[:2]
    stranger_header = support.header(stranger)
    assert stranger_header is not None
    assert pe.rows(
        "SELECT tenant_id FROM curie.personal_claude_enrollments WHERE id = :id",
        {"id": stranger_header["id"]},
    ) == [{"tenant_id": stranger.tenant_id}]


@pytest.mark.parametrize(
    "extra",
    [
        "X-API-Key",
        "X-Curie-Approval-Principal",
        "X-Curie-Adapter-Principal",
        "X-Curie-Worker-Token",
    ],
)
def test_platform_key_and_extra_credentials_are_refused(
    api: TestClient, owner: Owner, store: FakeCustodyStore, extra: str
) -> None:
    support.enrolled(api, owner)
    unchanged = _snapshot(owner, store)
    platform_key = {"X-API-Key": get_settings().api_key, "Origin": support.ORIGIN}
    value = get_settings().api_key if extra == "X-API-Key" else "example-credential"
    alongside = {extra: value}

    refused = [
        # The platform key alone names no person.
        api.get(support.ENROLLMENT, headers=platform_key),
        api.put(
            support.ENROLLMENT,
            json={"intent": "replace", "expected_version": 1, "token": support.TOKEN},
            headers=platform_key,
        ),
        api.delete(support.ENROLLMENT, params={"expected_version": 1}, headers=platform_key),
        # The session plus any other credential fails closed, never by precedence.
        api.get(support.ENROLLMENT, headers={**owner.headers(origin=None), **alongside}),
        support.put(api, owner, "replace", version=1, headers=alongside),
        support.disconnect(api, owner, 1, headers=alongside),
    ]
    for response in refused:
        assert support.refusal(response) == (
            401,
            "missing, invalid, or expired principal session",
        )
    assert _snapshot(owner, store) == unchanged

    assert support.body_of(support.status(api, owner))["generation"] == 1


def test_disabled_principal_suspended_tenant_and_revoked_session_fail_authentication(
    api: TestClient, owner: Owner, store: FakeCustodyStore, idp: TestIdP
) -> None:
    support.enrolled(api, owner)
    unchanged = _snapshot(owner, store)

    def refused() -> None:
        assert support.refusal(support.status(api, owner))[0] == 401
        assert support.refusal(support.disconnect(api, owner, 1))[0] == 401
        assert support.refusal(support.put(api, owner, "replace", version=1))[0] == 401
        assert _snapshot(owner, store) == unchanged

    for table, row_id, off in (
        ("principals", owner.principal_id, "disabled"),
        ("tenants", owner.tenant_id, "suspended"),
    ):
        pe.rows(f"UPDATE curie.{table} SET status = :s WHERE id = :id", {"s": off, "id": row_id})
        refused()
        pe.rows(f"UPDATE curie.{table} SET status = 'active' WHERE id = :id", {"id": row_id})
        assert support.body_of(support.status(api, owner))["generation"] == 1

    second = support.sign_in(owner.tenant_id, owner.principal_id, issuer=idp.issuer)
    pe.rows(
        "UPDATE curie.console_sessions SET revoked_at = now() WHERE principal_id = :p "
        "AND session_token_hash = :h",
        {"p": owner.principal_id, "h": crud_console.hash_console_credential(owner.session)},
    )
    refused()
    assert support.body_of(support.status(api, second))["generation"] == 1

    async def login_code_session(session: AsyncSession) -> str:
        code, _ = await crud_console.create_console_login_code(session, subject="operator")
        exchanged = await crud_console.exchange_console_login_code(session, code)
        assert exchanged is not None
        return exchanged[0]

    bare = Owner(owner.tenant_id, owner.principal_id, pe.with_session(login_code_session))
    assert support.refusal(support.status(api, bare))[0] == 401
    assert support.refusal(support.put(api, bare, "enroll"))[0] == 401


def test_cookie_mutations_require_a_matching_origin(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    for origin in ("https://console.example.com", None):
        for response in (
            support.put(api, owner, "enroll", origin=origin),
            support.disconnect(api, owner, 1, origin=origin),
        ):
            assert support.refusal(response) == (403, "console session origin rejected")
    assert support.header(owner) is None and store.count("create") == 0

    # Reads need no origin; a same-host Referer stands in for a missing Origin.
    assert support.body_of(support.status(api, owner))["state"] == "absent"
    via_referer = support.put(
        api, owner, "enroll", origin=None, headers={"Referer": f"{support.ORIGIN}/console"}
    )
    assert support.body_of(via_referer)["generation"] == 1


def test_service_ownership_stays_null(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    agent_id = pe.insert_agent(owner.tenant_id)
    support.enrolled(api, owner)
    service = pe.admit_service(
        api, support.WORKER_HEADERS, agent_id, issue_number=4101, conversation="4101.000100"
    )
    assert pe.work_item_tuple(service.work_item_id) == (owner.tenant_id, "service", None, None)
    pe.start_request(service.request_id)
    version = pe.request_column(service.request_id, "version")

    support.body_of(support.put(api, owner, "replace", version=1, token=support.REPLACEMENT))
    support.body_of(support.disconnect(api, owner, 2))

    assert pe.request_tuples(service.work_item_id) == {
        service.request_id: (owner.tenant_id, "service", None, None)
    }
    assert pe.request_column(service.request_id, "status") == "running"
    assert pe.request_column(service.request_id, "version") == version
    assert pe.request_column(service.request_id, "teardown_unconfirmed_at") is None


def test_token_cases_follow_the_shared_vector(
    api: TestClient, store: FakeCustodyStore, idp: TestIdP
) -> None:
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert vector["max_length"] == 4096 and len(vector["cases"]) >= 9
    for case in vector["cases"]:
        token = case.get("token")
        if token is None:
            token = case["token_repeat"]["char"] * case["token_repeat"]["count"]
        owner = support.seed_owner(support.seed_tenant(), issuer=idp.issuer)
        response = support.put(api, owner, "enroll", token=token)
        if case["valid"]:
            assert support.body_of(response)["generation"] == 1, case["name"]
            [row] = support.custody(owner)
            assert store.grant(row["secret_name"]) == token, case["name"]
        else:
            assert support.refusal(response) == (422, "invalid_enrollment_request"), case["name"]
            assert support.header(owner) is None and support.custody(owner) == [], case["name"]


def test_unconfigured_grant_namespace_answers_503_and_writes_nothing(
    idp: TestIdP, store: FakeCustodyStore
) -> None:
    with support.enrollment_app(store, namespace=None) as api:
        owner = support.seed_owner(support.seed_tenant(), issuer=idp.issuer)
        assert api.app.state.custody_store is None  # type: ignore[attr-defined]
        assert api.app.state.personal_custody_reconciler_task is None  # type: ignore[attr-defined]
        for response in (
            support.put(api, owner, "enroll"),
            support.disconnect(api, owner, 1),
        ):
            assert support.refusal(response) == (503, "custody_unconfigured")
        assert support.body_of(support.status(api, owner))["state"] == "absent"
    assert support.header(owner) is None and support.custody(owner) == []
    assert store.calls == []


def _malformed(api: TestClient, owner: Owner) -> list[Any]:
    headers = {**owner.headers(), "Content-Type": "application/json"}
    return [
        api.put(
            support.ENROLLMENT,
            json={"intent": SENTINEL, "token": "EXAMPLEtoken"},
            headers=headers,
        ),
        api.put(
            support.ENROLLMENT,
            json={"intent": "enroll", "token": "EXAMPLEtoken", "extra": SENTINEL},
            headers=headers,
        ),
        api.put(
            support.ENROLLMENT,
            content=f'{{"intent": "enroll", "token": "{SENTINEL}',
            headers=headers,
        ),
        api.put(
            support.ENROLLMENT,
            json={"intent": "enroll", "token": f"{SENTINEL} with space"},
            headers=headers,
        ),
        api.put(
            support.ENROLLMENT,
            json={"intent": "replace", "token": SENTINEL},
            headers=headers,
        ),
        api.delete(
            support.ENROLLMENT, params={"expected_version": SENTINEL}, headers=owner.headers()
        ),
    ]


def test_malformed_requests_never_echo_the_token(
    api: TestClient,
    owner: Owner,
    store: FakeCustodyStore,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.DEBUG)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    try:
        responses = _malformed(api, owner)
    finally:
        configure_tracer_provider(None)
        provider.shutdown()

    for response in responses:
        assert support.refusal(response) == (422, "invalid_enrollment_request"), response.text
        assert SENTINEL not in response.text
    # The test client logs its own request URL; everything the server wrote is checked.
    server_records = [
        record.getMessage() + repr(record.__dict__)
        for record in caplog.records
        if not record.name.startswith("httpx")
    ]
    assert server_records, "nothing was logged, so the check below proves nothing"
    assert all(SENTINEL not in text for text in server_records)
    captured = capfd.readouterr()
    server_output = [
        line for line in (captured.out + captured.err).splitlines() if "HTTP Request:" not in line
    ]
    assert all(SENTINEL not in line for line in server_output)
    spans = exporter.get_finished_spans()
    assert spans, "the HTTP telemetry recorded no span"
    assert all(SENTINEL not in repr(span.to_json()) for span in spans)
    assert support.header(owner) is None and store.calls == []

    assert support.body_of(support.put(api, owner, "enroll"))["generation"] == 1
