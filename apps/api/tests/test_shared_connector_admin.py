"""Shared connector administration through the API (ADR 0239, AC2, AC3, AC4, AC6).

ADR 0239 "Bundle, identity and request contract" 2 refuses shared registration
unless the installation identity is configured, valid and observed, and stores
the exact opaque value. Decision 7 keeps membership, alias narrowing and policy
generation in Postgres. "Custody and least privilege" 4 provisions one
restricted role per application and seals its DSN to the application's
recipient before storing or returning it; 5 registers only public key material
through a worker-authenticated route; 6 keeps previous recipients until nothing
references them. "Static sources" 1 and 4 cover imports, custody and the
refresh grant's pinned client configuration.

Every case drives the real routers through a TestClient on the disposable
database and checks state through the owner connection or through a real
connection made as the generated role.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import secrets
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, unquote

import asyncpg
import pytest
from _migration_support import render_url, sql_dicts
from _shared_connector_support import (
    DSN_SECRET,
    OPERATOR_PREFIX,
    OTHER_SERVER_IMAGE,
    SHARED_TABLES,
    SLOT,
    TOOLS,
    WORKER_PREFIX,
    Recipient,
    SessionPool,
    SharedCleanup,
    SharedStore,
    application_row,
    application_spec,
    assert_no_store,
    assert_redacted_422,
    assert_refused,
    b64,
    binding_row,
    capture_debug_logs,
    captured_log_text,
    client_fingerprint,
    connect_as,
    database_role_rows,
    dsn_parts,
    dsn_password,
    intent_row,
    operator_headers,
    quote_ident,
    random_installation,
    role_exists,
    role_name_for,
    secret_sentinel,
    shared_app,
    shared_policy,
    shared_table_text,
    source_metadata,
    worker_headers,
)
from curie_api.config import get_settings
from sqlalchemy.engine import make_url

APPLICATION_KEYS = {
    "application_id",
    "installation_id",
    "server_image",
    "credential_slots",
    "provider_origins",
    "snapshot_seal",
    "policy",
    "custody",
    "policy_generation",
    "enabled",
    "recipients",
    "database",
    "created_at",
}
BINDING_KEYS = {
    "binding_id",
    "application_id",
    "agent_id",
    "provider_account",
    "custody",
    "enabled",
    "credential_revision",
    "recipient_id",
    "source",
    "refresh",
    "created_at",
    "updated_at",
}
PROVISIONED_KEYS = {
    "application_id",
    "mode",
    "role_name",
    "status",
    "recipient_id",
    "dsn_revision",
    "sealed_dsn",
}
FUNCTION_SIGNATURES = (
    "curie.shared_connector_refresh_begin(text, text, integer)",
    "curie.shared_connector_refresh_commit(text, uuid, bigint, text, bytea)",
    "curie.shared_connector_refresh_mark_recovery(text, uuid, bigint)",
    "curie.shared_connector_reseal(uuid, bigint, text, text, bytea)",
)
# The Curve25519 small-order points libsodium refuses (RFC 7748 section 6.1 and the
# non-canonical encodings of 0 and 1) other than the all-zero key. A sealed box to
# any of them has a shared secret an attacker can compute without a private key.
LOW_ORDER_X25519_KEYS = (
    "0100000000000000000000000000000000000000000000000000000000000000",
    "e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800",
    "5f9c95bca3508c24b1d0b1559c83ef5b04445cc4581c8e86d8224eddd09f1157",
    "ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
    "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
    "eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
)


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def sessions(cleanup: SharedCleanup) -> Iterator[SessionPool]:
    pool = SessionPool()
    yield pool
    pool.close_all()


@pytest.fixture
def store(cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


def _credential(label: str = "PLAINTEXT") -> bytes:
    return json.dumps({SLOT: secret_sentinel(label)}).encode()


def _source() -> dict[str, str]:
    return source_metadata(uid=f"uid-{uuid.uuid4().hex[:12]}")


def _applications_with_id(application_id: str) -> int:
    (row,) = sql_dicts(
        "SELECT count(*) AS n FROM curie.shared_connector_applications WHERE id = :id",
        {"id": uuid.UUID(application_id)},
    )
    return int(row["n"])


def _memberships(application_id: str, agent_id: str) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT alias, tools FROM curie.shared_connector_memberships "
        "WHERE application_id = :application AND agent_id = :agent",
        {"application": uuid.UUID(application_id), "agent": uuid.UUID(agent_id)},
    )
    return {row["alias"]: row["tools"] for row in rows}


def _aliases(body: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(body["aliases"], key=lambda item: item["alias"])


def _recipients(store: SharedStore, application_id: str) -> dict[str, dict[str, Any]]:
    return {item["recipient_id"]: item for item in store.application(application_id)["recipients"]}


def _register(store: SharedStore, body: dict[str, Any]) -> Any:
    return store.operator("POST", "/applications", body)


# --- registration and installation identity ----------------------------------


VALID_INSTALLATION = random_installation()


@pytest.mark.parametrize(
    ("configured", "observed", "code"),
    [
        (None, "true", "shared_installation_unavailable"),
        ("", "true", "shared_installation_unavailable"),
        (VALID_INSTALLATION, "false", "shared_installation_unobserved"),
        (VALID_INSTALLATION, None, "shared_installation_unobserved"),
        ("acme installation", "true", "shared_installation_invalid"),
        ("acme\tinstallation", "true", "shared_installation_invalid"),
        ("acme\x07installation", "true", "shared_installation_invalid"),
        ("x" * 129, "true", "shared_installation_invalid"),
        ("é" * 65, "true", "shared_installation_invalid"),
    ],
    ids=[
        "unset",
        "blank",
        "observed-false",
        "observed-unset",
        "space",
        "tab",
        "control",
        "129-bytes",
        "130-utf8-bytes",
    ],
)
def test_registration_refuses_an_unusable_configured_installation(
    cleanup: SharedCleanup,
    monkeypatch: pytest.MonkeyPatch,
    configured: str | None,
    observed: str | None,
    code: str,
) -> None:
    cleanup.installation(VALID_INSTALLATION)
    application_id = str(uuid.uuid4())
    with shared_app(monkeypatch, configured, observed=observed) as client:
        store = SharedStore(client, configured or "", cleanup)
        body = {
            "application": application_spec(VALID_INSTALLATION, application_id),
            "policy": shared_policy(),
        }
        assert_refused(_register(store, body), 409, code)
    assert _applications_with_id(application_id) == 0


def test_registration_checks_the_installation_then_the_spec_then_the_match(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    application_id = str(uuid.uuid4())
    spec = application_spec(installation, application_id)
    unlocked = {**spec, "server_image": "example.com/acme/server:latest"}

    with shared_app(monkeypatch, "") as client:
        blank = SharedStore(client, "", cleanup)
        refused = _register(blank, {"application": unlocked, "policy": shared_policy()})
        assert_refused(refused, 409, "shared_installation_unavailable")

    with shared_app(monkeypatch, installation) as client:
        store = SharedStore(client, installation, cleanup)
        for application in (
            unlocked,
            {**spec, "installation": "acme installation"},
            {**spec, "installation": "x" * 129},
            {**spec, "unexpected": "field"},
            {**spec, "credential_slots": "access_token"},
        ):
            assert_refused(
                _register(store, {"application": application, "policy": shared_policy()}),
                422,
                "shared_application_invalid",
            )
        mismatched = application_spec(random_installation(), application_id)
        assert_refused(
            _register(store, {"application": mismatched, "policy": shared_policy()}),
            409,
            "shared_installation_mismatch",
        )
        assert _applications_with_id(application_id) == 0
        created = _register(store, {"application": spec, "policy": shared_policy()})
        assert created.status_code == 201, created.text


@pytest.mark.parametrize(
    "installation",
    [random_installation(), str(uuid.uuid4()).upper(), random_installation(128)],
    ids=["fresh-chart-32", "uppercase-uuid-shape", "128-bytes"],
)
def test_the_configured_installation_is_stored_exactly_as_given(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch, installation: str
) -> None:
    cleanup.installation(installation)
    with shared_app(monkeypatch, installation) as client:
        record = SharedStore(client, installation, cleanup).register_application()
    assert record["installation_id"] == installation
    row = application_row(record["application_id"])
    assert row["installation_id"] == installation
    assert row["spec"]["installation"] == installation
    assert row["custody"] == "platform"


def test_identical_registration_replays_and_different_pins_conflict(store: SharedStore) -> None:
    application_id = str(uuid.uuid4())
    body = store.registration_body(application_id)
    created = _register(store, body)
    assert created.status_code == 201, created.text
    record = created.json()
    assert set(record) == APPLICATION_KEYS
    assert record["application_id"] == application_id
    assert record["installation_id"] == store.installation
    assert record["server_image"] == body["application"]["server_image"]
    assert record["credential_slots"] == [SLOT]
    assert record["provider_origins"] == body["application"]["provider_origins"]
    assert record["snapshot_seal"] is None
    assert record["policy"] == shared_policy()
    assert record["custody"] == "platform"
    assert record["enabled"] is True
    assert record["recipients"] == []
    assert record["database"] is None
    assert record["policy_generation"] >= 1

    replay = _register(store, body)
    assert replay.status_code == 200, replay.text
    assert replay.json() == record
    for changed in (
        store.registration_body(application_id, server_image=OTHER_SERVER_IMAGE),
        store.registration_body(application_id, tools=TOOLS[:2]),
        store.registration_body(application_id, slots=(SLOT, "refresh_token")),
    ):
        assert_refused(_register(store, changed), 409, "shared_application_immutable")
    assert store.application(application_id) == record


@pytest.mark.parametrize(
    ("custody", "slots", "accepted"),
    [
        ("platform", (), False),
        ("connector", (SLOT,), False),
        ("platform", (SLOT,), True),
        ("connector", (), True),
    ],
    ids=["platform-without-slots", "connector-with-slots", "platform", "connector"],
)
def test_custody_must_agree_with_the_declared_slots(
    store: SharedStore, custody: str, slots: tuple[str, ...], accepted: bool
) -> None:
    application_id = str(uuid.uuid4())
    response = _register(
        store, store.registration_body(application_id, slots=slots, custody=custody)
    )
    if not accepted:
        assert_refused(response, 422, "shared_application_invalid")
        assert _applications_with_id(application_id) == 0
        return
    assert response.status_code == 201, response.text
    assert response.json()["custody"] == custody
    assert response.json()["credential_slots"] == list(slots)


def test_unknown_applications_and_bindings_are_not_found(store: SharedStore) -> None:
    missing = str(uuid.uuid4())
    agent_id = store.create_agent()
    stranger = Recipient.generate()
    for response in (
        store.operator("GET", f"/applications/{missing}"),
        store.disable_application(missing),
        store.put_members(missing, agent_id, [{"alias": "primary", "tools": None}]),
        store.provision(missing, {"mode": "bundled"}),
        store.post_binding(
            missing, store.binding_body(agent_id, stranger, _credential(), source=_source())
        ),
        store.worker("POST", f"/applications/{missing}/recipients", stranger.registration(missing)),
    ):
        assert_refused(response, 404, "shared_application_not_found")
    for response in (
        store.operator("GET", f"/bindings/{missing}"),
        store.disable_binding(missing),
        store.reimport(missing, 1, stranger, _credential(), _source()),
    ):
        assert_refused(response, 404, "shared_binding_not_found")
    known = store.register_application()["application_id"]
    assert store.application(known)["application_id"] == known


# --- membership and policy generation ----------------------------------------


def test_member_aliases_narrow_the_policy_and_raise_the_generation_only_on_change(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    start = store.application(application_id)["policy_generation"]

    narrowed = [{"alias": "reader", "tools": ["issues.read"]}]
    first = store.put_members(application_id, agent_id, narrowed)
    assert first.status_code == 200, first.text
    assert first.json()["application_id"] == application_id
    assert first.json()["agent_id"] == agent_id
    assert _aliases(first.json()) == narrowed
    generation = first.json()["policy_generation"]
    assert generation > start
    assert store.application(application_id)["policy_generation"] == generation

    same = store.put_members(application_id, agent_id, narrowed)
    assert same.status_code == 200, same.text
    assert same.json()["policy_generation"] == generation

    widened = store.put_members(
        application_id,
        agent_id,
        [
            {"alias": "reader", "tools": ["issues.read"]},
            {"alias": "admin", "tools": ["issues.delete"]},
        ],
    )
    assert_refused(widened, 422, "shared_policy_widening")
    assert store.application(application_id)["policy_generation"] == generation
    assert _memberships(application_id, agent_id) == {"reader": ["issues.read"]}

    both = [
        {"alias": "reader", "tools": ["issues.read"]},
        {"alias": "writer", "tools": None},
    ]
    replaced = store.put_members(application_id, agent_id, both)
    assert replaced.status_code == 200, replaced.text
    assert _aliases(replaced.json()) == both
    assert replaced.json()["policy_generation"] > generation
    generation = replaced.json()["policy_generation"]
    assert _memberships(application_id, agent_id) == {"reader": ["issues.read"], "writer": None}

    removed = store.operator("DELETE", f"/applications/{application_id}/members/{agent_id}")
    assert removed.status_code == 200, removed.text
    assert removed.json()["policy_generation"] > generation
    assert removed.json()["aliases"] == []
    assert _memberships(application_id, agent_id) == {}
    assert_refused(
        store.operator("DELETE", f"/applications/{application_id}/members/{agent_id}"),
        404,
        "shared_member_not_found",
    )


def test_member_changes_refuse_unknown_agents_bad_aliases_and_disabled_applications(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    aliases = [{"alias": "primary", "tools": ["issues.read"]}]
    assert_refused(
        store.put_members(application_id, str(uuid.uuid4()), aliases), 404, "shared_agent_not_found"
    )
    for malformed in (
        [],
        [{"alias": "primary", "tools": None}, {"alias": "primary", "tools": None}],
        [{"alias": "Not_A_Label", "tools": None}],
        [{"alias": f"alias-{index}", "tools": None} for index in range(17)],
    ):
        response = store.put_members(application_id, agent_id, malformed)
        assert response.status_code == 422, response.text
    assert _memberships(application_id, agent_id) == {}

    accepted = store.put_members(application_id, agent_id, aliases)
    assert accepted.status_code == 200, accepted.text
    disabled = store.disable_application(application_id)
    assert disabled.status_code == 200, disabled.text
    assert_refused(
        store.put_members(application_id, agent_id, [{"alias": "other", "tools": None}]),
        409,
        "shared_application_disabled",
    )
    assert _memberships(application_id, agent_id) == {"primary": ["issues.read"]}


def test_deleting_an_agent_removes_its_membership_and_binding_and_raises_the_generation(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    leaving, staying = store.create_agent(), store.create_agent()
    for agent_id in (leaving, staying):
        member = store.put_members(application_id, agent_id, [{"alias": "primary", "tools": None}])
        assert member.status_code == 200, member.text
    binding = store.import_binding(application_id, leaving, recipient, _credential())
    before = store.application(application_id)["policy_generation"]

    deleted = store.call("DELETE", f"/agents/{leaving}", headers=operator_headers())
    assert deleted.status_code == 204, deleted.text

    assert _memberships(application_id, leaving) == {}
    assert _memberships(application_id, staying) == {"primary": None}
    assert (
        sql_dicts(
            "SELECT id FROM curie.shared_connector_bindings WHERE agent_id = :agent",
            {"agent": uuid.UUID(leaving)},
        )
        == []
    )
    assert store.application(application_id)["policy_generation"] > before
    assert_refused(
        store.operator("GET", f"/bindings/{binding['binding_id']}"), 404, "shared_binding_not_found"
    )


# --- bindings ----------------------------------------------------------------


def test_an_agent_has_at_most_one_enabled_binding(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    source = _source()
    first = store.import_binding(
        application_id, agent_id, recipient, _credential(), source=source, provider_account="one"
    )
    assert set(first) == BINDING_KEYS
    assert first["application_id"] == application_id
    assert first["agent_id"] == agent_id
    assert first["provider_account"] == "one"
    assert first["custody"] == "platform"
    assert first["enabled"] is True
    assert first["credential_revision"] == 1
    assert first["recipient_id"] == recipient.recipient_id
    assert first["source"] == source
    assert first["refresh"] is None

    replacement = store.binding_body(
        agent_id, recipient, _credential(), source=_source(), provider_account="two"
    )
    assert_refused(store.post_binding(application_id, replacement), 409, "shared_binding_exists")
    disabled = store.disable_binding(first["binding_id"])
    assert disabled.status_code == 200, disabled.text
    assert disabled.json() == {
        "binding_id": first["binding_id"],
        "enabled": False,
        "acknowledged": True,
    }
    created = store.post_binding(application_id, replacement)
    assert created.status_code == 201, created.text
    assert created.json()["binding_id"] != first["binding_id"]
    assert store.binding(first["binding_id"])["enabled"] is False
    assert store.binding(created.json()["binding_id"])["enabled"] is True
    assert_refused(
        store.reimport(first["binding_id"], 1, recipient, _credential(), source),
        409,
        "shared_binding_disabled",
    )
    again = store.disable_binding(first["binding_id"])
    assert again.status_code == 200, again.text
    assert again.json() == disabled.json()
    assert sql_dicts(
        "SELECT count(*) AS n FROM curie.shared_connector_bindings "
        "WHERE application_id = :application AND agent_id = :agent AND enabled",
        {"application": uuid.UUID(application_id), "agent": uuid.UUID(agent_id)},
    ) == [{"n": 1}]


def test_binding_import_refuses_unknown_agents_recipients_custody_gaps_and_disabled_apps(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    plaintext, source = _credential(), _source()
    assert_refused(
        store.post_binding(
            application_id,
            store.binding_body(str(uuid.uuid4()), recipient, plaintext, source=source),
        ),
        404,
        "shared_agent_not_found",
    )
    assert_refused(
        store.post_binding(
            application_id,
            store.binding_body(agent_id, Recipient.generate(), plaintext, source=source),
        ),
        409,
        "shared_recipient_unknown",
    )
    for body in (
        store.binding_body(agent_id, None, None, source=source),
        store.binding_body(agent_id, recipient, plaintext, source=None),
    ):
        assert_refused(
            store.post_binding(application_id, body), 422, "shared_binding_custody_mismatch"
        )
    assert (
        sql_dicts(
            "SELECT id FROM curie.shared_connector_bindings WHERE agent_id = :agent",
            {"agent": uuid.UUID(agent_id)},
        )
        == []
    )
    created = store.post_binding(
        application_id, store.binding_body(agent_id, recipient, plaintext, source=source)
    )
    assert created.status_code == 201, created.text

    closed = store.register_application()["application_id"]
    closed_recipient = store.register_recipient(closed)
    assert store.disable_application(closed).status_code == 200
    assert_refused(
        store.post_binding(
            closed, store.binding_body(agent_id, closed_recipient, plaintext, source=source)
        ),
        409,
        "shared_application_disabled",
    )


def test_a_connector_custody_binding_holds_no_credential_and_refuses_reimport_and_check(
    store: SharedStore,
) -> None:
    application_id = store.register_application(slots=(), custody="connector")["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    grant = f"grant:{uuid.uuid4().hex}"
    for body in (
        store.binding_body(agent_id, recipient, _credential(), source=_source()),
        store.binding_body(agent_id, recipient, _credential()),
        store.binding_body(agent_id, None, None, source=_source()),
        store.binding_body(
            agent_id,
            None,
            None,
            refresh_grant={
                "grant_id": grant,
                "client_config_fingerprint": client_fingerprint(grant),
            },
        ),
    ):
        assert_refused(
            store.post_binding(application_id, body), 422, "shared_binding_custody_mismatch"
        )

    created = store.post_binding(
        application_id, store.binding_body(agent_id, None, None, provider_account="acct-custom")
    )
    assert created.status_code == 201, created.text
    record = created.json()
    assert record["custody"] == "connector"
    assert record["recipient_id"] is None
    assert record["source"] is None
    assert record["refresh"] is None
    assert record["credential_revision"] == 1
    row = binding_row(record["binding_id"])
    for column in (
        "recipient_id",
        "ciphertext",
        "source_kind",
        "source_namespace",
        "source_name",
        "source_uid",
        "source_resource_version",
        "source_fingerprint",
    ):
        assert row[column] is None, column

    assert_refused(
        store.reimport(record["binding_id"], 1, recipient, _credential(), _source()),
        409,
        "shared_binding_connector_custody",
    )
    assert_refused(
        store.check(record["binding_id"], "uid-any", "1"), 409, "shared_binding_connector_custody"
    )
    assert binding_row(record["binding_id"]) == row


def test_a_refresh_grant_pins_its_client_configuration(
    store: SharedStore, sessions: SessionPool
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id, other_agent = store.create_agent(), store.create_agent()
    grant = f"grant:{uuid.uuid4().hex}"
    pinned = client_fingerprint(grant)
    binding = store.import_binding(
        application_id,
        agent_id,
        recipient,
        _credential(),
        refresh_grant={"grant_id": grant, "client_config_fingerprint": pinned},
    )
    assert binding["refresh"] == {
        "grant_id": grant,
        "client_config_fingerprint": pinned,
        "state": "ready",
        "epoch": 0,
    }
    intent = intent_row(binding["binding_id"])
    assert intent["installation_id"] == store.installation
    assert str(intent["application_id"]) == application_id
    assert intent["client_config_fingerprint"] == pinned

    duplicate = store.binding_body(
        other_agent,
        recipient,
        _credential(),
        source=_source(),
        refresh_grant={"grant_id": grant, "client_config_fingerprint": client_fingerprint("x")},
    )
    assert_refused(
        store.post_binding(application_id, duplicate), 409, "shared_refresh_grant_exists"
    )
    assert (
        sql_dicts(
            "SELECT id FROM curie.shared_connector_bindings WHERE agent_id = :agent",
            {"agent": uuid.UUID(other_agent)},
        )
        == []
    )
    malformed = store.binding_body(
        other_agent,
        recipient,
        _credential(),
        source=_source(),
        refresh_grant={"grant_id": "grant:other", "client_config_fingerprint": "sha1:abc"},
    )
    assert store.post_binding(application_id, malformed).status_code == 422
    other_grant = f"grant:{uuid.uuid4().hex}"
    accepted = store.binding_body(
        other_agent,
        recipient,
        _credential(),
        source=_source(),
        refresh_grant={
            "grant_id": other_grant,
            "client_config_fingerprint": client_fingerprint(other_grant),
        },
    )
    assert store.post_binding(application_id, accepted).status_code == 201

    reimported = store.reimport(binding["binding_id"], 1, recipient, _credential(), _source())
    assert reimported.status_code == 200, reimported.text
    assert reimported.json()["refresh"]["grant_id"] == grant
    assert reimported.json()["refresh"]["client_config_fingerprint"] == pinned

    owner = sessions.owner()
    with pytest.raises(asyncpg.PostgresError):
        owner.execute(
            "UPDATE curie.shared_connector_refresh_intents "
            "SET client_config_fingerprint = $1 WHERE binding_id = $2",
            client_fingerprint("replaced"),
            uuid.UUID(binding["binding_id"]),
        )
    assert intent_row(binding["binding_id"])["client_config_fingerprint"] == pinned


def test_malformed_requests_never_echo_secret_material(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    store.provision_bundled(application_id, recipient)
    sentinel = secret_sentinel("ECHO")
    registration = Recipient.generate().registration(application_id)
    worker_path = f"/applications/{application_id}/recipients"
    for body in (
        {**registration, "public_key": {**registration["public_key"], "private_key": sentinel}},
        {**registration, "private_key": sentinel},
        {**registration, "readback": {**registration["readback"], "private_key": sentinel}},
        {**registration, "public_key": {"algorithm": "x25519-sealed-box", "key": sentinel}},
    ):
        response = store.worker("POST", worker_path, body)
        assert_redacted_422(response, sentinel)
        assert_no_store(response)
    status = store.worker(
        "PUT",
        f"/applications/{application_id}/database/status",
        {"status": "ready", "reason": sentinel, "dsn_revision": 1},
    )
    assert_redacted_422(status, sentinel)

    source = _source()
    good = store.binding_body(agent_id, recipient, _credential(), source=source)
    for body in (
        {
            **good,
            "credential": {"recipient_id": recipient.recipient_id, "ciphertext": "!" + sentinel},
        },
        {**good, "credential": {**good["credential"], "plaintext": sentinel}},
        {**good, "source": {**source, "uid": sentinel + " spaced"}},
        {
            **good,
            "refresh_grant": {"grant_id": sentinel + "/bad", "client_config_fingerprint": "x"},
        },
    ):
        response = store.post_binding(application_id, body)
        assert_redacted_422(response, sentinel)
        assert_no_store(response)
    created = store.post_binding(application_id, good)
    assert created.status_code == 201, created.text
    reimport = store.operator(
        "POST",
        f"/bindings/{created.json()['binding_id']}/reimport",
        {
            "expected_revision": 1,
            "credential": {"recipient_id": recipient.recipient_id, "ciphertext": "!" + sentinel},
            "source": source,
        },
    )
    assert_redacted_422(reimport, sentinel)

    leaked_spec = {
        **application_spec(store.installation, str(uuid.uuid4())),
        "secret_value": sentinel,
    }
    registration_refused = _register(store, {"application": leaked_spec, "policy": shared_policy()})
    assert_refused(registration_refused, 422, "shared_application_invalid")
    assert sentinel not in registration_refused.text
    for response in store.responses:
        assert sentinel not in response.text


def _with_unknown_key(body: dict[str, Any], key: str, *path: str | int) -> dict[str, Any]:
    """A copy of ``body`` with ``{key: "x"}`` merged into the object found at ``path``."""

    mutated = copy.deepcopy(body)
    target: Any = mutated
    for step in path:
        target = target[step]
    target[key] = "x"
    return mutated


@pytest.mark.parametrize(
    ("route", "path"),
    [
        ("binding-import", ()),
        ("binding-import", ("credential",)),
        ("binding-import", ("source",)),
        ("membership-put", ()),
        ("membership-put", ("aliases", 0)),
        ("database-status", ()),
    ],
    ids=[
        "binding-import",
        "binding-import-credential",
        "binding-import-source",
        "membership-put",
        "membership-put-alias",
        "database-status",
    ],
)
def test_an_unknown_key_named_like_a_secret_is_never_echoed_in_a_422(
    store: SharedStore, route: str, path: tuple[str | int, ...]
) -> None:
    """A rejected key NAME is request input too: pydantic reports it in the error ``loc``.

    Liveness: the same body without the extra key is accepted right after the refusal.
    """

    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    valid: dict[str, Any]
    send = store.operator
    if route == "binding-import":
        method, accepted_status = "POST", 201
        route_path = f"/applications/{application_id}/bindings"
        valid = store.binding_body(agent_id, recipient, _credential(), source=_source())
    elif route == "membership-put":
        method, accepted_status = "PUT", 200
        route_path = f"/applications/{application_id}/members/{agent_id}"
        valid = {"aliases": [{"alias": "primary", "tools": None}]}
    else:
        store.provision_bundled(application_id, recipient)
        send = store.worker
        method, accepted_status = "PUT", 200
        route_path = f"/applications/{application_id}/database/status"
        valid = {"status": "ready", "reason": None, "dsn_revision": 1}
    sentinel = f"sk-live-LOC-SENTINEL-{secrets.token_hex(8)}"

    refused = send(method, route_path, _with_unknown_key(valid, sentinel, *path))
    assert_redacted_422(refused, sentinel)
    assert_no_store(refused)
    assert sentinel not in json.dumps(dict(refused.headers))
    assert sentinel not in shared_table_text()
    assert _memberships(application_id, agent_id) == {}

    accepted = send(method, route_path, valid)
    assert accepted.status_code == accepted_status, accepted.text
    assert_no_store(accepted)


# --- recipient keys ----------------------------------------------------------


def test_recipient_registration_is_worker_only_and_operator_routes_refuse_the_worker_token(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = Recipient.generate()
    path = f"{WORKER_PREFIX}/applications/{application_id}/recipients"
    body = recipient.registration(application_id)
    for headers in (None, operator_headers(), {"X-Curie-Worker-Token": "not-the-token"}):
        assert store.call("POST", path, json=body, headers=headers).status_code == 401
    assert (
        store.call(
            "PUT",
            f"{WORKER_PREFIX}/applications/{application_id}/database/status",
            json={"status": "ready", "reason": None, "dsn_revision": 1},
            headers=operator_headers(),
        ).status_code
        == 401
    )
    for method, operator_path, payload in (
        ("GET", f"/applications/{application_id}", None),
        ("POST", "/applications", store.registration_body(str(uuid.uuid4()))),
        ("POST", f"/applications/{application_id}/disable", None),
    ):
        response = store.call(
            method, OPERATOR_PREFIX + operator_path, json=payload, headers=worker_headers()
        )
        assert response.status_code == 401, (operator_path, response.text)
    assert store.application(application_id)["enabled"] is True
    assert store.application(application_id)["recipients"] == []

    store.register_recipient(application_id, recipient)
    (row,) = sql_dicts(
        "SELECT algorithm, public_key FROM curie.shared_connector_recipients "
        "WHERE application_id = :application AND recipient_id = :recipient",
        {"application": uuid.UUID(application_id), "recipient": recipient.recipient_id},
    )
    assert row == {"algorithm": "x25519-sealed-box", "public_key": recipient.public_bytes}


def test_recipient_registration_refuses_mismatched_identity_and_failed_readback(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    other_application = store.register_application()["application_id"]
    recipient = Recipient.generate()
    body = recipient.registration(application_id)
    path = f"/applications/{application_id}/recipients"
    missing = str(uuid.uuid4())
    assert_refused(
        store.worker(
            "POST", f"/applications/{missing}/recipients", recipient.registration(missing)
        ),
        404,
        "shared_application_not_found",
    )
    assert_refused(
        store.worker("POST", path, {**body, "application_id": other_application}),
        409,
        "shared_recipient_application_mismatch",
    )
    assert_refused(
        store.worker("POST", path, {**body, "recipient_id": Recipient.generate().recipient_id}),
        409,
        "shared_recipient_id_mismatch",
    )
    assert_refused(
        store.worker("POST", path, {**body, "readback": {"public_key_sha256": "0" * 64}}),
        409,
        "shared_recipient_readback_failed",
    )
    assert store.application(application_id)["recipients"] == []

    store.register_recipient(application_id, recipient)
    replay = store.worker("POST", path, body)
    assert replay.status_code == 200, replay.text
    assert replay.json() == {
        "application_id": application_id,
        "recipient_id": recipient.recipient_id,
        "current": True,
    }


def test_malformed_recipient_keys_are_refused(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    path = f"/applications/{application_id}/recipients"
    recipient = Recipient.generate()
    body = recipient.registration(application_id)

    def keyed(raw: bytes) -> dict[str, Any]:
        digest = hashlib.sha256(raw).hexdigest()
        return {
            **body,
            "recipient_id": digest,
            "public_key": {"algorithm": "x25519-sealed-box", "key": b64(raw)},
            "readback": {"public_key_sha256": digest},
        }

    cases = {
        "bad-algorithm": {**body, "public_key": {**body["public_key"], "algorithm": "x25519"}},
        "not-base64": {**body, "public_key": {**body["public_key"], "key": "not base64!"}},
        "short-key": keyed(secrets.token_bytes(31)),
        "long-key": keyed(secrets.token_bytes(33)),
        "all-zero-key": keyed(bytes(32)),
        "private-key-field": {
            **body,
            "public_key": {**body["public_key"], "private_key": b64(bytes(recipient.private_key))},
        },
        "unknown-field": {**body, "comment": "extra"},
    }
    for name, payload in cases.items():
        response = store.worker("POST", path, payload)
        assert response.status_code == 422, (name, response.text)
        assert response.json() == {"detail": "shared_recipient_key_invalid"}, (name, response.text)
        assert b64(bytes(recipient.private_key)) not in response.text
    assert store.application(application_id)["recipients"] == []
    store.register_recipient(application_id, recipient)


@pytest.mark.parametrize("public_key_hex", LOW_ORDER_X25519_KEYS)
def test_a_small_order_public_key_is_refused_even_with_a_correct_identity(
    store: SharedStore, public_key_hex: str
) -> None:
    """Only the key is wrong: id and readback are the correct digest of these very bytes.

    Liveness: a generated key registers right after (the same path
    test_recipient_registration_is_worker_only_and_operator_routes_refuse_the_worker_token and
    test_malformed_recipient_keys_are_refused end on).
    """

    application_id = store.register_application()["application_id"]
    raw = bytes.fromhex(public_key_hex)
    assert len(raw) == 32
    digest = hashlib.sha256(raw).hexdigest()
    body = {
        "application_id": application_id,
        "recipient_id": digest,
        "public_key": {"algorithm": "x25519-sealed-box", "key": b64(raw)},
        "readback": {"public_key_sha256": digest},
    }

    response = store.worker("POST", f"/applications/{application_id}/recipients", body)
    assert response.status_code == 422, response.text
    assert response.json() == {"detail": "shared_recipient_key_invalid"}, response.text
    assert_no_store(response)
    assert store.application(application_id)["recipients"] == []
    assert (
        sql_dicts(
            "SELECT recipient_id FROM curie.shared_connector_recipients "
            "WHERE application_id = :application",
            {"application": uuid.UUID(application_id)},
        )
        == []
    )

    generated = store.register_recipient(application_id)
    assert set(_recipients(store, application_id)) == {generated.recipient_id}


def test_registering_a_new_recipient_keeps_the_previous_one_live(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    first = store.register_recipient(application_id)
    second = store.register_recipient(application_id)
    recipients = _recipients(store, application_id)
    assert set(recipients) == {first.recipient_id, second.recipient_id}
    assert recipients[first.recipient_id]["current"] is False
    assert recipients[first.recipient_id]["retired_at"] is None
    assert recipients[first.recipient_id]["algorithm"] == "x25519-sealed-box"
    assert recipients[second.recipient_id]["current"] is True

    path = f"/applications/{application_id}/recipients"
    for recipient, current in ((second, True), (first, False)):
        replay = store.worker("POST", path, recipient.registration(application_id))
        assert replay.status_code == 200, replay.text
        assert replay.json() == {
            "application_id": application_id,
            "recipient_id": recipient.recipient_id,
            "current": current,
        }
    assert _recipients(store, application_id)[second.recipient_id]["current"] is True
    assert len(_recipients(store, application_id)) == 2

    agent_id = store.create_agent()
    older = store.import_binding(application_id, agent_id, first, _credential())
    assert older["recipient_id"] == first.recipient_id


def test_retirement_refuses_current_and_referenced_recipients_until_reimported(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    first = store.register_recipient(application_id)
    agent_id = store.create_agent()
    source = _source()
    binding = store.import_binding(application_id, agent_id, first, _credential(), source=source)
    assert_refused(
        store.retire(application_id, first.recipient_id), 409, "shared_recipient_current"
    )
    second = store.register_recipient(application_id)
    assert_refused(
        store.retire(application_id, first.recipient_id), 409, "shared_recipient_referenced"
    )
    assert_refused(
        store.retire(application_id, second.recipient_id), 409, "shared_recipient_current"
    )
    assert_refused(
        store.retire(application_id, Recipient.generate().recipient_id),
        404,
        "shared_recipient_not_found",
    )
    assert _recipients(store, application_id)[first.recipient_id]["retired_at"] is None

    moved = store.reimport(binding["binding_id"], 1, second, _credential(), source)
    assert moved.status_code == 200, moved.text
    assert moved.json()["recipient_id"] == second.recipient_id
    retired = store.retire(application_id, first.recipient_id)
    assert retired.status_code == 200, retired.text
    assert retired.json()["application_id"] == application_id
    assert retired.json()["recipient_id"] == first.recipient_id
    assert retired.json()["retired_at"] is not None
    again = store.retire(application_id, first.recipient_id)
    assert again.status_code == 200, again.text
    assert again.json() == retired.json()
    assert_refused(
        store.worker(
            "POST",
            f"/applications/{application_id}/recipients",
            first.registration(application_id),
        ),
        409,
        "shared_recipient_retired",
    )


# --- database roles ----------------------------------------------------------


def test_bundled_provisioning_creates_a_restricted_role_and_seals_its_dsn(
    store: SharedStore, sessions: SessionPool
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    expected = role_name_for(store.installation, application_id)
    record, dsn = store.provision_bundled(application_id, recipient)
    assert set(record) == PROVISIONED_KEYS
    assert {key: value for key, value in record.items() if key != "sealed_dsn"} == {
        "application_id": application_id,
        "mode": "bundled",
        "role_name": expected,
        "status": "provisioned",
        "recipient_id": recipient.recipient_id,
        "dsn_revision": 1,
    }
    assert len(expected.encode()) == 50

    (flags,) = sql_dicts(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, "
        "rolbypassrls, rolcanlogin, rolconnlimit FROM pg_roles WHERE rolname = :name",
        {"name": expected},
    )
    assert flags == {
        "rolsuper": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolinherit": False,
        "rolreplication": False,
        "rolbypassrls": False,
        "rolcanlogin": True,
        "rolconnlimit": 32,
    }
    assert (
        sql_dicts(
            "SELECT m.roleid FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
            "WHERE r.rolname = :name",
            {"name": expected},
        )
        == []
    )
    for table in SHARED_TABLES:
        (privileges,) = sql_dicts(
            "SELECT "
            + ", ".join(
                f"has_table_privilege(CAST(:role AS name), CAST(:table AS text), '{privilege}') "
                f"AS can_{privilege.lower()}"
                for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")
            ),
            {"role": expected, "table": f"curie.{table}"},
        )
        assert privileges == {
            "can_select": True,
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_truncate": False,
        }, table
    for table, column, allowed in (
        ("shared_connector_applications", "lock_guard", True),
        ("shared_connector_bindings", "lock_guard", True),
        ("shared_connector_applications", "enabled", False),
        ("shared_connector_applications", "policy", False),
        ("shared_connector_applications", "policy_generation", False),
        ("shared_connector_bindings", "enabled", False),
        ("shared_connector_bindings", "application_id", False),
        ("shared_connector_bindings", "agent_id", False),
        ("shared_connector_bindings", "provider_account", False),
        ("shared_connector_bindings", "credential_revision", False),
        ("shared_connector_bindings", "ciphertext", False),
        ("shared_connector_refresh_intents", "state", False),
    ):
        (row,) = sql_dicts(
            "SELECT has_column_privilege(CAST(:role AS name), CAST(:table AS text), "
            "CAST(:column AS text), 'UPDATE') AS can_update",
            {"role": expected, "table": f"curie.{table}", "column": column},
        )
        assert row["can_update"] is allowed, (table, column)
    for signature in FUNCTION_SIGNATURES:
        (row,) = sql_dicts(
            "SELECT has_function_privilege(CAST(:role AS name), CAST(:function AS text), "
            "'EXECUTE') AS can_execute",
            {"role": expected, "function": signature},
        )
        assert row["can_execute"] is True, signature
    (agents,) = sql_dicts(
        "SELECT has_table_privilege(CAST(:role AS name), 'curie.agents', 'SELECT') AS can_select",
        {"role": expected},
    )
    assert agents["can_select"] is False

    api = make_url(get_settings().database_url)
    parts = dsn_parts(dsn)
    assert parts.scheme == "postgresql"
    assert unquote(parts.username or "") == expected
    assert (parts.hostname, parts.port, parts.path) == (api.host, api.port, f"/{api.database}")
    assert dsn_password(dsn) != api.password
    role = sessions.open(dsn)
    assert role.fetchval("SELECT current_user") == expected
    assert [
        str(row["id"]) for row in role.fetch("SELECT id FROM curie.shared_connector_applications")
    ] == [application_id]

    (stored,) = database_role_rows(application_id)
    assert recipient.open(stored["sealed_dsn"]).decode() == dsn
    assert stored["dsn_recipient_id"] == recipient.recipient_id
    application = store.application(application_id)
    assert application["database"] == {
        "mode": "bundled",
        "role_name": expected,
        "status": "provisioned",
        "status_reason": None,
        "dsn_recipient_id": recipient.recipient_id,
        "dsn_revision": 1,
        "dsn_secret": None,
    }
    assert record["sealed_dsn"] not in json.dumps(application)


def test_each_application_gets_its_own_role_and_provisions_once(store: SharedStore) -> None:
    roles = []
    for _ in range(2):
        application_id = store.register_application()["application_id"]
        recipient = store.register_recipient(application_id)
        record, dsn = store.provision_bundled(application_id, recipient)
        assert record["role_name"] == role_name_for(store.installation, application_id)
        assert connect_as(dsn) == record["role_name"]
        roles.append((application_id, record["role_name"]))
    assert roles[0][1] != roles[1][1]
    first_application = roles[0][0]
    before = database_role_rows(first_application)
    assert_refused(
        store.provision(first_application, {"mode": "bundled"}),
        409,
        "shared_database_already_provisioned",
    )
    assert database_role_rows(first_application) == before


def test_bundled_provisioning_needs_a_current_recipient_and_an_enabled_application(
    store: SharedStore,
) -> None:
    application_id = store.register_application()["application_id"]
    expected = store.cleanup.role(role_name_for(store.installation, application_id))
    assert_refused(
        store.provision(application_id, {"mode": "bundled"}), 409, "shared_recipient_required"
    )
    assert not role_exists(expected)
    assert database_role_rows(application_id) == []

    recipient = store.register_recipient(application_id)
    record, dsn = store.provision_bundled(application_id, recipient)
    assert connect_as(dsn) == expected == record["role_name"]

    closed = store.register_application()["application_id"]
    closed_role = store.cleanup.role(role_name_for(store.installation, closed))
    store.register_recipient(closed)
    assert store.disable_application(closed).status_code == 200
    assert_refused(store.provision(closed, {"mode": "bundled"}), 409, "shared_application_disabled")
    assert not role_exists(closed_role)


def test_provisioning_through_a_connection_without_role_admin_refuses_and_creates_nothing(
    store: SharedStore,
    sessions: SessionPool,
    cleanup: SharedCleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    expected = cleanup.role(role_name_for(store.installation, application_id))
    limited = cleanup.role(f"sca_limited_{secrets.token_hex(6)}")
    password = secrets.token_urlsafe(18)
    owner = sessions.owner()
    owner.execute(
        f"CREATE ROLE {quote_ident(limited)} LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB "
        f"BYPASSRLS PASSWORD '{password}'"
    )
    for grant in (
        "GRANT USAGE ON SCHEMA curie TO {role}",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA curie TO {role}",
        "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curie TO {role}",
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA curie TO {role}",
    ):
        owner.execute(grant.format(role=quote_ident(limited)))

    original = os.environ["DATABASE_URL"]
    limited_url = make_url(original).set(username=limited, password=password)
    with shared_app(
        monkeypatch, store.installation, DATABASE_URL=render_url(limited_url)
    ) as client:
        refused = SharedStore(client, store.installation, cleanup).provision(
            application_id, {"mode": "bundled"}
        )
    monkeypatch.setenv("DATABASE_URL", original)
    get_settings.cache_clear()
    assert_refused(refused, 409, "shared_role_admin_required")
    assert not role_exists(expected)
    assert database_role_rows(application_id) == []

    record, dsn = store.provision_bundled(application_id, recipient)
    assert connect_as(dsn) == expected == record["role_name"]


def test_the_sealed_dsn_carries_no_api_credentials_or_unlisted_parameters(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    original = os.environ["DATABASE_URL"]
    api = make_url(original)
    noisy = api.update_query_dict(
        {"user": api.username or "", "password": api.password or "", "ssl": "disable"}
    )
    with shared_app(monkeypatch, store.installation, DATABASE_URL=render_url(noisy)) as client:
        record, dsn = SharedStore(client, store.installation, store.cleanup).provision_bundled(
            application_id, recipient
        )
    monkeypatch.setenv("DATABASE_URL", original)
    get_settings.cache_clear()

    parts = dsn_parts(dsn)
    assert parts.scheme == "postgresql"
    assert unquote(parts.username or "") == record["role_name"]
    assert dsn_password(dsn) != api.password
    assert (parts.hostname, parts.port, parts.path) == (api.host, api.port, f"/{api.database}")
    assert parse_qs(parts.query) == {"sslmode": ["disable"]}
    assert connect_as(dsn) == record["role_name"]


def test_the_provisioning_mode_must_agree_with_the_spec(store: SharedStore) -> None:
    bundled = store.register_application()["application_id"]
    managed = store.register_application(dsn_encoding="managed-raw")["application_id"]
    for application_id in (bundled, managed):
        store.register_recipient(application_id)
        store.cleanup.role(role_name_for(store.installation, application_id))
    operator_role = store.cleanup.role(f"ops_shared_{secrets.token_hex(6)}")
    assert_refused(
        store.provision(bundled, {"mode": "managed", "role_name": operator_role}),
        409,
        "shared_database_mode_mismatch",
    )
    assert_refused(
        store.provision(managed, {"mode": "bundled"}), 409, "shared_database_mode_mismatch"
    )
    for application_id in (bundled, managed):
        assert database_role_rows(application_id) == []
        assert not role_exists(role_name_for(store.installation, application_id))

    assert store.provision(bundled, {"mode": "bundled"}).status_code == 201
    assert (
        store.provision(managed, {"mode": "managed", "role_name": operator_role}).status_code == 201
    )


def test_managed_provisioning_records_the_spec_reference_and_creates_no_role(
    store: SharedStore,
) -> None:
    application_id = store.register_application(dsn_encoding="managed-raw")["application_id"]
    operator_role = store.cleanup.role(f"ops_shared_{secrets.token_hex(6)}")
    created = store.provision(application_id, {"mode": "managed", "role_name": operator_role})
    assert created.status_code == 201, created.text
    assert created.json() == {
        "application_id": application_id,
        "mode": "managed",
        "role_name": operator_role,
        "status": "pending",
        "dsn_secret": DSN_SECRET,
    }
    assert not role_exists(operator_role)
    (row,) = database_role_rows(application_id)
    assert row["sealed_dsn"] is None
    assert row["dsn_recipient_id"] is None
    assert (row["dsn_secret_name"], row["dsn_secret_key"]) == (
        DSN_SECRET["name"],
        DSN_SECRET["key"],
    )
    assert store.application(application_id)["database"] == {
        "mode": "managed",
        "role_name": operator_role,
        "status": "pending",
        "status_reason": None,
        "dsn_recipient_id": None,
        "dsn_revision": 1,
        "dsn_secret": DSN_SECRET,
    }
    assert_refused(store.rotate(application_id, 1), 409, "shared_database_mode_mismatch")
    assert_refused(
        store.provision(application_id, {"mode": "managed", "role_name": operator_role}),
        409,
        "shared_database_already_provisioned",
    )
    ready = store.report_status(application_id, "ready", None, 1)
    assert ready.status_code == 200, ready.text
    assert ready.json() == {
        "application_id": application_id,
        "mode": "managed",
        "status": "ready",
        "status_reason": None,
        "dsn_revision": 1,
    }

    other = store.register_application(dsn_encoding="managed-raw")["application_id"]
    response = store.provision(other, {"mode": "managed", "role_name": "Not-A-Role"})
    assert response.status_code == 422, response.text
    assert database_role_rows(other) == []


def test_dsn_rotation_is_a_cas_that_replaces_the_role_password(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    record, dsn = store.provision_bundled(application_id, recipient)
    assert connect_as(dsn) == record["role_name"]
    assert_refused(store.rotate(application_id, 2), 409, "shared_database_revision_conflict")
    assert connect_as(dsn) == record["role_name"]

    rotated = store.rotate(application_id, 1)
    assert rotated.status_code == 200, rotated.text
    body = rotated.json()
    assert set(body) == PROVISIONED_KEYS
    assert {key: value for key, value in body.items() if key != "sealed_dsn"} == {
        "application_id": application_id,
        "mode": "bundled",
        "role_name": record["role_name"],
        "status": "provisioned",
        "recipient_id": recipient.recipient_id,
        "dsn_revision": 2,
    }
    replacement = recipient.open(body["sealed_dsn"]).decode()
    assert dsn_password(replacement) != dsn_password(dsn)
    assert connect_as(replacement) == record["role_name"]
    with pytest.raises(asyncpg.InvalidPasswordError):
        connect_as(dsn)
    assert_refused(store.rotate(application_id, 1), 409, "shared_database_revision_conflict")
    assert store.application(application_id)["database"]["dsn_revision"] == 2

    unprovisioned = store.register_application()["application_id"]
    store.register_recipient(unprovisioned)
    assert_refused(store.rotate(unprovisioned, 1), 404, "shared_database_not_provisioned")


def test_worker_status_reports_are_fenced_by_the_current_dsn_revision(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    store.provision_bundled(application_id, recipient)
    ready = store.report_status(application_id, "ready", None, 1)
    assert ready.status_code == 200, ready.text
    assert ready.json() == {
        "application_id": application_id,
        "mode": "bundled",
        "status": "ready",
        "status_reason": None,
        "dsn_revision": 1,
    }
    assert store.rotate(application_id, 1).status_code == 200
    assert store.application(application_id)["database"]["status"] == "provisioned"

    delayed = store.report_status(application_id, "ready", None, 1)
    assert_refused(delayed, 409, "shared_database_revision_conflict")
    assert store.application(application_id)["database"]["status"] == "provisioned"
    failed = store.report_status(application_id, "failed", "role_privileges_unexpected", 2)
    assert failed.status_code == 200, failed.text
    assert failed.json()["status"] == "failed"
    assert failed.json()["status_reason"] == "role_privileges_unexpected"
    database = store.application(application_id)["database"]
    assert (database["status"], database["status_reason"]) == (
        "failed",
        "role_privileges_unexpected",
    )
    assert store.report_status(application_id, "ready", "Not A Code", 2).status_code == 422

    unprovisioned = store.register_application()["application_id"]
    assert_refused(
        store.report_status(unprovisioned, "ready", None, 1),
        404,
        "shared_database_not_provisioned",
    )


# --- secret custody ----------------------------------------------------------


def test_plaintext_and_dsn_passwords_never_reach_responses_logs_or_rows(
    store: SharedStore, sessions: SessionPool, caplog: pytest.LogCaptureFixture
) -> None:
    sentinels = [secret_sentinel(label) for label in ("IMPORTED", "REIMPORTED", "REFRESHED")]
    with capture_debug_logs(caplog):
        application_id = store.register_application()["application_id"]
        recipient = store.register_recipient(application_id)
        agent_id = store.create_agent()
        member = store.put_members(application_id, agent_id, [{"alias": "primary", "tools": None}])
        assert member.status_code == 200, member.text
        grant = f"grant:{uuid.uuid4().hex}"
        source = _source()
        binding = store.import_binding(
            application_id,
            agent_id,
            recipient,
            json.dumps({SLOT: sentinels[0]}).encode(),
            source=source,
            refresh_grant={
                "grant_id": grant,
                "client_config_fingerprint": client_fingerprint(grant),
            },
        )
        reimported = store.reimport(
            binding["binding_id"],
            1,
            recipient,
            json.dumps({SLOT: sentinels[1]}).encode(),
            source_metadata(uid=source["uid"], resource_version="101"),
        )
        assert reimported.status_code == 200, reimported.text
        assert store.check(binding["binding_id"], source["uid"], "101").status_code == 200
        _, dsn = store.provision_bundled(application_id, recipient)
        role = sessions.open(dsn)
        began = role.fetchrow(
            "SELECT * FROM curie.shared_connector_refresh_begin($1, $2, $3)", grant, "resolver", 60
        )
        assert began is not None and began["outcome"] == "began"
        committed = role.fetchrow(
            "SELECT * FROM curie.shared_connector_refresh_commit($1, $2, $3, $4, $5)",
            grant,
            began["intent_id"],
            began["epoch"],
            recipient.recipient_id,
            recipient.seal(json.dumps({SLOT: sentinels[2]}).encode()),
        )
        assert committed is not None and committed["outcome"] == "committed"
        rotated = store.rotate(application_id, 1)
        assert rotated.status_code == 200, rotated.text
        replacement = recipient.open(rotated.json()["sealed_dsn"]).decode()
        assert store.report_status(application_id, "ready", None, 2).status_code == 200
        store.application(application_id)
        store.binding(binding["binding_id"])

    secret_values = [*sentinels, dsn, replacement, dsn_password(dsn), dsn_password(replacement)]
    responses = "\n".join(
        response.text + json.dumps(dict(response.headers)) for response in store.responses
    )
    logs = captured_log_text(caplog.records) + "\n" + caplog.text
    rows = shared_table_text()
    assert any(record.name.startswith("sqlalchemy") for record in caplog.records)
    assert application_id in rows
    for value in secret_values:
        assert value not in responses
        assert value not in logs
        assert value not in rows
