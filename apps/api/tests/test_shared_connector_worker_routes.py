"""The worker's two shared connector routes and the registration pins (ADR 0239, cp5, AC3).

``GET /v1/internal/shared-connectors/applications/{id}/render`` answers the
frozen ``render_application`` output for the stored spec, unmodified, plus the
Secret plan the worker reconciler needs: the worker-managed key Secret, the
DSN Secret (worker-managed and sealed for bundled mode, the operator's own
reference for managed mode), the grant store and snapshot seal references, and
the live recipient ids. The sealed DSN is the one read of that column the API
makes (``database_roles.sealed_dsn_for_worker``), and only this
worker-authenticated route returns it.

``POST /v1/internal/shared-connectors/resolve`` is the declaration resolution
the worker's own readers call: one read transaction answering each entry or
the first refusal, in the dict form ``{"detail": {"code", "application",
"connector", "declared"}}``.

Registration pins the worker-managed Secret names, so a spec can never point
the worker's create-once Secrets at an operator Secret or the reverse.

Every case drives the real routers through a TestClient on the disposable
database. The API's RBAC (no ``secrets`` rule) is asserted by the chart's
``shared-connector-admission-assertions.sh``, not here: the API test job has no
``helm`` binary.
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _migration_support import sql_dicts
from _shared_connector_support import (
    DSN_SECRET,
    DSN_SECRET_KEY,
    DSN_SECRET_PREFIX,
    KEY_SECRET_KEY,
    KEY_SECRET_PREFIX,
    WORKER_PREFIX,
    Recipient,
    SharedCleanup,
    SharedStore,
    application_row,
    assert_declaration_refused,
    assert_no_store,
    assert_refused,
    b64,
    canonical,
    detail_code,
    dsn_secret_ref,
    key_secret_ref,
    operator_headers,
    random_installation,
    recipient_rows,
    rewrite_stored_spec,
    shared_app,
    worker_headers,
)
from curie_api.config import get_settings
from plugin_format.shared_connector_render import SharedApplication, render_application

GRANT_STORE = {"name": "acme-grants", "key": "url"}
SNAPSHOT_SEAL = {"name": "acme-snapshot", "key": "seal"}


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def store(cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


def _stored_spec(application_id: str) -> SharedApplication:
    """The spec as stored, revalidated with the frozen model, independent of any route."""

    return SharedApplication.model_validate_json(
        json.dumps(application_row(application_id)["spec"])
    )


def _rendered(store: SharedStore, application_id: str) -> dict[str, Any]:
    response = store.application_render(application_id)
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


def _recipient_rows(application_id: str) -> int:
    (row,) = sql_dicts(
        "SELECT count(*) AS n FROM curie.shared_connector_recipients WHERE application_id = :id",
        {"id": uuid.UUID(application_id)},
    )
    return int(row["n"])


def _applications_with_id(application_id: str) -> int:
    (row,) = sql_dicts(
        "SELECT count(*) AS n FROM curie.shared_connector_applications WHERE id = :id",
        {"id": uuid.UUID(application_id)},
    )
    return int(row["n"])


# --- 1. authentication --------------------------------------------------------


def test_application_render_requires_worker_token(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    path = f"{WORKER_PREFIX}/applications/{application_id}/render"
    params = {"namespace": "acme", "release": "acme"}
    entries = [{"name": "tracker", "application": application_id}]
    resolve = {"agent_id": agent_id, "entries": entries}
    for headers in ({}, operator_headers(), {"X-Curie-Worker-Token": "not-the-token"}):
        refused = store.client.get(path, params=params, headers=headers)
        assert refused.status_code == 401, refused.text
        assert "objects" not in refused.text
        assert_no_store(refused)
        resolved = store.client.post(f"{WORKER_PREFIX}/resolve", json=resolve, headers=headers)
        assert resolved.status_code == 401, resolved.text
    # Liveness: the worker token answers.
    allowed = store.client.get(path, params=params, headers=worker_headers())
    assert allowed.status_code == 200, allowed.text
    assert_no_store(allowed)


# --- 2. and 3. the Secret plan ------------------------------------------------


def test_secret_plan_bundled_includes_sealed_dsn_and_revision(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)

    before = _rendered(store, application_id)
    assert before["secrets"]["database"]["sealed_dsn"] is None
    assert before["secrets"]["database"]["dsn_revision"] is None

    store.provision_bundled(application_id, recipient)
    body = _rendered(store, application_id)
    assert body["application_id"] == application_id
    assert body["enabled"] is True
    assert body["recipients"] == [recipient.recipient_id]
    secrets = body["secrets"]
    assert secrets["encryption_key"] == key_secret_ref(application_id)
    assert secrets["encryption_key"] == {
        "name": KEY_SECRET_PREFIX + application_id,
        "key": KEY_SECRET_KEY,
    }
    assert secrets["grant_store"] == GRANT_STORE
    assert secrets["snapshot_seal"] is None
    database = secrets["database"]
    assert database["mode"] == "bundled"
    assert database["secret"] == dsn_secret_ref(application_id)
    assert database["secret"] == {"name": DSN_SECRET_PREFIX + application_id, "key": DSN_SECRET_KEY}
    assert database["status"] == "provisioned"
    assert database["dsn_revision"] == 1
    first = recipient.open(database["sealed_dsn"]).decode()
    assert first.startswith("postgresql://")

    rotated = store.rotate(application_id, 1)
    assert rotated.status_code == 200, rotated.text
    after = _rendered(store, application_id)["secrets"]["database"]
    assert after["dsn_revision"] == 2
    second = recipient.open(after["sealed_dsn"]).decode()
    assert second.startswith("postgresql://")
    assert second != first

    # The sealed DSN is this worker route's alone: the operator record never carries it.
    record = store.application(application_id)
    assert "sealed_dsn" not in json.dumps(record)
    assert after["sealed_dsn"] not in json.dumps(record)


def test_secret_plan_managed_has_reference_only(store: SharedStore) -> None:
    application_id = store.register_application(dsn_encoding="managed-raw")["application_id"]
    operator_role = store.cleanup.role(f"ops_shared_{uuid.uuid4().hex[:12]}")
    created = store.provision(application_id, {"mode": "managed", "role_name": operator_role})
    assert created.status_code == 201, created.text

    database = _rendered(store, application_id)["secrets"]["database"]
    assert database["mode"] == "managed"
    assert database["secret"] == DSN_SECRET
    assert database["sealed_dsn"] is None
    assert database["status"] == "pending"
    # Never the worker-managed DSN name in managed mode.
    assert not database["secret"]["name"].startswith(DSN_SECRET_PREFIX)


# --- the application render is the frozen output ------------------------------


def test_application_render_objects_are_the_frozen_function_output(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    body = _rendered(store, application_id)
    spec = _stored_spec(application_id)
    assert canonical(body["objects"]) == canonical(render_application(spec))
    assert (
        body["installation_label"] == hashlib.sha256(store.installation.encode()).hexdigest()[:40]
    )
    # Negative: another application's render is not this one's.
    other = store.register_application()["application_id"]
    assert canonical(_rendered(store, other)["objects"]) != canonical(body["objects"])


def test_application_render_refuses_a_foreign_root(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, cleanup: SharedCleanup
) -> None:
    application_id = store.register_application()["application_id"]
    for root in ({"namespace": "other"}, {"release": "other"}):
        refused = store.application_render(application_id, **root)
        assert refused.status_code == 409, refused.text
        assert detail_code(refused) == "shared_application_root_mismatch"
        assert "objects" not in refused.json()
        assert_no_store(refused)

    # The same application read by an API of another installation is foreign too.
    foreign = cleanup.installation(random_installation())
    with shared_app(monkeypatch, foreign) as client:
        other = SharedStore(client, foreign, cleanup)
        refused = other.application_render(application_id)
        assert refused.status_code == 409, refused.text
        assert detail_code(refused) == "shared_application_root_mismatch"


def test_disabled_application_still_renders_marked_disabled(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    enabled = _rendered(store, application_id)
    disabled = store.disable_application(application_id)
    assert disabled.status_code == 200, disabled.text
    body = _rendered(store, application_id)
    assert body["enabled"] is False
    # The worker keeps a disabled application's objects; the render is unchanged.
    assert canonical(body["objects"]) == canonical(enabled["objects"])


# --- 4. registration pins the worker-managed names ---------------------------


def _pinned_variants(application_id: str) -> dict[str, dict[str, Any]]:
    other = str(uuid.uuid4())
    return {
        "bundled-key-wrong-name": {"encryption_key": {"name": "acme-encryption", "key": "private"}},
        "bundled-key-wrong-data-key": {
            "encryption_key": {"name": KEY_SECRET_PREFIX + application_id, "key": "private"}
        },
        "bundled-key-other-application": {
            "encryption_key": {"name": KEY_SECRET_PREFIX + other, "key": KEY_SECRET_KEY}
        },
        "bundled-dsn-wrong-name": {"database_dsn": dict(DSN_SECRET)},
        "bundled-dsn-wrong-data-key": {
            "database_dsn": {"name": DSN_SECRET_PREFIX + application_id, "key": "dsn"}
        },
        "managed-dsn-worker-prefix": {
            "dsn_encoding": "managed-raw",
            "database_dsn": {"name": DSN_SECRET_PREFIX + application_id, "key": "dsn"},
        },
        "managed-dsn-key-prefix": {
            "dsn_encoding": "managed-raw",
            "database_dsn": {"name": KEY_SECRET_PREFIX + other, "key": "dsn"},
        },
        "grant-store-key-prefix": {
            "grant_store": {"name": KEY_SECRET_PREFIX + other, "key": "url"}
        },
        "grant-store-dsn-prefix": {
            "grant_store": {"name": DSN_SECRET_PREFIX + other, "key": "url"}
        },
        "snapshot-seal-key-prefix": {
            "snapshot_seal": {"name": KEY_SECRET_PREFIX + other, "key": "seal"}
        },
    }


def test_registration_pins_worker_secret_names(store: SharedStore) -> None:
    for name in _pinned_variants(str(uuid.uuid4())):
        application_id = str(uuid.uuid4())
        body = store.registration_body(application_id)
        # Rebuilt per application: the pins name the application's own UUID.
        body["application"].update(copy.deepcopy(_pinned_variants(application_id)[name]))
        refused = store.operator("POST", "/applications", body)
        assert_refused(refused, 422, "shared_application_secret_ref_invalid")
        assert _applications_with_id(application_id) == 0, name

    # Liveness: the canonical names register, for both DSN modes and a snapshot seal.
    bundled = store.registration_body(str(uuid.uuid4()))
    bundled["application"]["snapshot_seal"] = dict(SNAPSHOT_SEAL)
    assert store.operator("POST", "/applications", bundled).status_code == 201
    managed = store.registration_body(str(uuid.uuid4()), dsn_encoding="managed-raw")
    assert managed["application"]["database_dsn"] == DSN_SECRET
    assert store.operator("POST", "/applications", managed).status_code == 201


# --- 5. recipient registration -----------------------------------------------


def test_recipient_registration_rejections(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = Recipient.generate()
    good = recipient.registration(application_id)
    other = Recipient.generate()
    cases: dict[str, tuple[dict[str, Any], int, str]] = {
        "application-mismatch": (
            {**good, "application_id": str(uuid.uuid4())},
            409,
            "shared_recipient_application_mismatch",
        ),
        "recipient-mismatch": (
            {**good, "recipient_id": other.recipient_id},
            409,
            "shared_recipient_id_mismatch",
        ),
        "malformed-key": (
            {**good, "public_key": {"algorithm": "x25519-sealed-box", "key": b64(b"short")}},
            422,
            "shared_recipient_key_invalid",
        ),
        "private-key-field": (
            {
                **good,
                "public_key": {
                    **good["public_key"],
                    "private_key": b64(bytes(Recipient.generate().private_key)),
                },
            },
            422,
            "shared_recipient_key_invalid",
        ),
        "readback-mismatch": (
            {**good, "readback": {"public_key_sha256": other.recipient_id}},
            409,
            "shared_recipient_readback_failed",
        ),
    }
    for name, (body, status, code) in cases.items():
        response = store.worker("POST", f"/applications/{application_id}/recipients", body)
        assert response.status_code == status, (name, response.text)
        assert response.json() == {"detail": code}, (name, response.text)
        assert _recipient_rows(application_id) == 0, name
    # Liveness: the unaltered body registers.
    store.register_recipient(application_id, recipient)
    assert _recipient_rows(application_id) == 1


# --- 6. status reports are fenced by the DSN revision -------------------------


def test_status_report_fences_revision(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    store.provision_bundled(application_id, recipient)

    stale = store.report_status(application_id, "ready", None, 2)
    assert_refused(stale, 409, "shared_database_revision_conflict")
    assert store.application(application_id)["database"]["status"] == "provisioned"

    ready = store.report_status(application_id, "ready", None, 1)
    assert ready.status_code == 200, ready.text
    assert store.application(application_id)["database"]["status"] == "ready"
    assert _rendered(store, application_id)["secrets"]["database"]["status"] == "ready"

    failed = store.report_status(application_id, "failed", "deployment_unavailable", 1)
    assert failed.status_code == 200, failed.text
    database = store.application(application_id)["database"]
    assert (database["status"], database["status_reason"]) == ("failed", "deployment_unavailable")

    # A rotation moves the revision; the worker's report for the old one is stale.
    assert store.rotate(application_id, 1).status_code == 200
    assert_refused(
        store.report_status(application_id, "ready", None, 1),
        409,
        "shared_database_revision_conflict",
    )
    assert store.report_status(application_id, "ready", None, 2).status_code == 200


# --- 7. the resolve route ------------------------------------------------------


def test_resolve_route_codes(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, cleanup: SharedCleanup
) -> None:
    first = store.register_application()["application_id"]
    second = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(first, agent_id, "tracker")
    store.member(second, agent_id, "wiki")

    resolved = store.resolve(
        agent_id,
        [{"name": "wiki", "application": second}, {"name": "tracker", "application": first}],
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json() == {
        "agent_id": agent_id,
        "entries": [
            {"name": "tracker", "application": first, "enabled": True},
            {"name": "wiki", "application": second, "enabled": True},
        ],
    }

    unknown = str(uuid.uuid4())
    assert_declaration_refused(
        store.resolve(agent_id, [{"name": "tracker", "application": unknown}]),
        409,
        "shared_application_unknown",
        application=unknown,
        connector="tracker",
    )
    # A member under another alias is not a member under this one.
    assert_declaration_refused(
        store.resolve(agent_id, [{"name": "renamed", "application": first}]),
        409,
        "shared_application_not_member",
        application=first,
        connector="renamed",
    )
    stranger = store.create_agent()
    assert_declaration_refused(
        store.resolve(stranger, [{"name": "tracker", "application": first}]),
        409,
        "shared_application_not_member",
        application=first,
        connector="tracker",
    )
    # The first refusal stops resolution and names its own entry.
    assert_declaration_refused(
        store.resolve(
            agent_id,
            [{"name": "tracker", "application": first}, {"name": "wiki", "application": unknown}],
        ),
        409,
        "shared_application_unknown",
        application=unknown,
        connector="wiki",
    )
    assert store.disable_application(second).status_code == 200
    assert_declaration_refused(
        store.resolve(agent_id, [{"name": "wiki", "application": second}]),
        409,
        "shared_application_disabled",
        application=second,
        connector="wiki",
    )
    # Liveness: the enabled one still resolves after its sibling was disabled.
    assert store.resolve(agent_id, [{"name": "tracker", "application": first}]).status_code == 200

    # Malformed bodies refuse at the schema, before any read.
    for body in (
        {"agent_id": agent_id, "entries": []},
        {"agent_id": agent_id, "entries": [{"name": "Bad Name", "application": first}]},
        {"agent_id": agent_id, "entries": [{"name": "tracker", "application": "nope"}]},
        {"agent_id": agent_id, "entries": [{"name": "tracker", "application": first}], "x": 1},
        {
            "agent_id": agent_id,
            "entries": [{"name": f"c{i}", "application": first} for i in range(65)],
        },
    ):
        refused = store.worker("POST", "/resolve", body)
        assert refused.status_code == 422, refused.text
        assert isinstance(refused.json()["detail"], list), refused.text

    # Installation refusals: a foreign installation sees an unknown application,
    # and an unobserved or blank installation refuses before any read.
    foreign = cleanup.installation(random_installation())
    entries = [{"name": "tracker", "application": first}]
    with shared_app(monkeypatch, foreign) as client:
        assert_declaration_refused(
            SharedStore(client, foreign, cleanup).resolve(agent_id, entries),
            409,
            "shared_application_unknown",
            application=first,
            connector="tracker",
        )
    with shared_app(monkeypatch, store.installation, observed="false") as client:
        assert_declaration_refused(
            SharedStore(client, store.installation, cleanup).resolve(agent_id, entries),
            409,
            "shared_installation_unobserved",
        )
    with shared_app(monkeypatch, None) as client:
        assert_declaration_refused(
            SharedStore(client, "", cleanup).resolve(agent_id, entries),
            409,
            "shared_installation_unavailable",
        )


# --- 8. stored specs are checked on every worker read (review r1, CR1-3) -------


def _stored_ref_drifts(application_id: str) -> dict[str, dict[str, Any]]:
    """Stored spec changes registration refuses: references the worker never creates."""

    other = str(uuid.uuid4())
    return {
        "key-ref-non-canonical": {"encryption_key": {"name": "acme-encryption", "key": "private"}},
        "bundled-dsn-non-canonical": {"database_dsn": dict(DSN_SECRET)},
        "operator-ref-worker-prefix": {
            "grant_store": {"name": KEY_SECRET_PREFIX + other, "key": "url"}
        },
        "managed-dsn-worker-prefix": {
            "dsn_encoding": "managed-raw",
            "database_dsn": {"name": DSN_SECRET_PREFIX + application_id, "key": "dsn"},
        },
    }


@pytest.mark.parametrize("drift", list(_stored_ref_drifts(str(uuid.uuid4()))))
def test_stored_spec_secret_ref_invalid_refuses_worker_reads(
    store: SharedStore, drift: str
) -> None:
    application_id = store.register_application()["application_id"]
    canonical_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, "tracker")
    store.member(canonical_id, agent_id, "wiki")
    store.register_recipient(application_id)
    rewrite_stored_spec(application_id, _stored_ref_drifts(application_id)[drift])
    before = recipient_rows(application_id)
    assert len(before) == 1

    assert_refused(
        store.application_render(application_id), 409, "shared_application_secret_ref_invalid"
    )
    assert_declaration_refused(
        store.resolve(agent_id, [{"name": "tracker", "application": application_id}]),
        409,
        "shared_application_secret_ref_invalid",
        application=application_id,
        connector="tracker",
    )
    assert recipient_rows(application_id) == before, drift

    # Liveness: the canonical stored spec still renders and resolves.
    assert canonical(_rendered(store, canonical_id)["objects"]) == canonical(
        render_application(_stored_spec(canonical_id))
    )
    resolved = store.resolve(agent_id, [{"name": "wiki", "application": canonical_id}])
    assert resolved.status_code == 200, resolved.text
