"""Run and gated tool authority in the registered policy (ADR 0239, cp4 AC1, AC3, AC5).

The shared caller proxy decides which tools need a run pair (``run_tools``) and
which need an approval grant (``gated_tools``) from the application's policy,
read per call through the application's own database role. So the policy is
where that authority is stored, and it must be:

1. written only through the API's validated ``SharedPolicy``: each list holds
   unique tools drawn from ``tools``;
2. stored in the ``policy`` JSONB only when nonempty, so a policy without them
   keeps the JSON and the pin digest it always had, and an identical
   registration replays instead of conflicting;
3. readable by the application's own role and by no other application's role.

Every case drives the real routers through a TestClient on the disposable
database, then reads the row back through the owner connection or through a real
connection made as the generated role.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from _migration_support import sql_dicts
from _shared_connector_support import (
    TOOLS,
    SessionPool,
    SharedCleanup,
    SharedStore,
    application_row,
    assert_redacted_422,
    assert_refused,
    random_installation,
    shared_app,
    shared_policy,
)
from curie_api.config import get_settings

RUN_TOOLS = ["issues.close", "issues.write"]
GATED_TOOLS = ["issues.close"]


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


def _register(store: SharedStore, application_id: str, **policy_extra: Any) -> httpx.Response:
    body = store.registration_body(application_id)
    body["policy"] = {**body["policy"], **policy_extra}
    return store.operator("POST", "/applications", body)


def _applications_with_id(application_id: str) -> int:
    (row,) = sql_dicts(
        "SELECT count(*) AS n FROM curie.shared_connector_applications WHERE id = :id",
        {"id": uuid.UUID(application_id)},
    )
    return int(row["n"])


def _pin_digest(spec: dict[str, Any], policy: dict[str, Any]) -> str:
    """The digest ``store._pin_digest`` takes, restated so the test does not import it."""

    canonical = json.dumps(
        {"spec": spec, "policy": policy},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _stored_policy(application_id: str) -> dict[str, Any]:
    policy = application_row(application_id)["policy"]
    assert isinstance(policy, dict), policy
    return policy


def _as_json(value: Any) -> Any:
    """asyncpg hands a jsonb column back as text unless a codec is set."""

    return json.loads(value) if isinstance(value, str) else value


# --- registration stores and returns the lists --------------------------------


def test_run_and_gated_tools_are_stored_in_the_policy_row_and_returned(
    store: SharedStore,
) -> None:
    application_id = str(uuid.uuid4())
    created = _register(store, application_id, run_tools=RUN_TOOLS, gated_tools=GATED_TOOLS)
    assert created.status_code == 201, created.text

    expected = {
        "tools": list(TOOLS),
        "custody": "platform",
        "run_tools": RUN_TOOLS,
        "gated_tools": GATED_TOOLS,
    }
    assert created.json()["policy"] == expected
    assert store.application(application_id)["policy"] == expected

    row = application_row(application_id)
    assert row["policy"] == expected
    assert row["pin_digest"] == _pin_digest(row["spec"], row["policy"])
    # The lists are pinned: the same application without them is another policy.
    assert row["pin_digest"] != _pin_digest(row["spec"], shared_policy())


def test_a_valid_policy_with_both_lists_nonempty_and_subsets_is_accepted(
    store: SharedStore,
) -> None:
    for run_tools, gated_tools in (
        (["issues.read"], ["issues.write"]),
        (list(TOOLS), list(TOOLS[:1])),
        (list(TOOLS[:1]), list(TOOLS)),
    ):
        response = _register(
            store, str(uuid.uuid4()), run_tools=run_tools, gated_tools=gated_tools
        )
        assert response.status_code == 201, response.text
        policy = response.json()["policy"]
        assert policy["run_tools"] == run_tools
        assert policy["gated_tools"] == gated_tools


def test_a_tool_may_be_both_a_run_tool_and_a_gated_tool(store: SharedStore) -> None:
    application_id = str(uuid.uuid4())
    response = _register(
        store, application_id, run_tools=["issues.close"], gated_tools=["issues.close"]
    )
    assert response.status_code == 201, response.text
    stored = _stored_policy(application_id)
    assert stored["run_tools"] == ["issues.close"]
    assert stored["gated_tools"] == ["issues.close"]


def test_either_list_alone_is_stored_without_the_other(store: SharedStore) -> None:
    run_only = str(uuid.uuid4())
    gated_only = str(uuid.uuid4())
    assert _register(store, run_only, run_tools=RUN_TOOLS).status_code == 201
    assert _register(store, gated_only, gated_tools=GATED_TOOLS).status_code == 201

    run_policy = _stored_policy(run_only)
    assert run_policy["run_tools"] == RUN_TOOLS
    assert "gated_tools" not in run_policy
    gated_policy = _stored_policy(gated_only)
    assert gated_policy["gated_tools"] == GATED_TOOLS
    assert "run_tools" not in gated_policy

    assert store.application(run_only)["policy"]["gated_tools"] == []
    assert store.application(gated_only)["policy"]["run_tools"] == []


# --- refusals store nothing ---------------------------------------------------


@pytest.mark.parametrize(
    ("extra", "offender"),
    [
        pytest.param({"run_tools": ["issues.delete"]}, "issues.delete", id="run-outside-tools"),
        pytest.param({"gated_tools": ["issues.delete"]}, "issues.delete", id="gated-outside-tools"),
        pytest.param(
            {"run_tools": ["issues.read", "issues.delete"]},
            "issues.delete",
            id="run-one-inside-one-outside",
        ),
        pytest.param(
            {"gated_tools": ["issues.write", "issues.delete"]},
            "issues.delete",
            id="gated-one-inside-one-outside",
        ),
        pytest.param(
            {"run_tools": ["ISSUES.READ"]}, "ISSUES.READ", id="run-differs-only-in-case"
        ),
        pytest.param(
            {"run_tools": ["issues.read"], "gated_tools": ["not.a.policy.tool"]},
            "not.a.policy.tool",
            id="valid-run-invalid-gated",
        ),
        pytest.param({"run_tools": ["bad tool"]}, "bad tool", id="run-malformed-name"),
        pytest.param({"gated_tools": ["bad tool"]}, "bad tool", id="gated-malformed-name"),
    ],
)
def test_a_run_or_gated_tool_outside_the_policy_tools_is_refused_and_not_stored(
    store: SharedStore, extra: dict[str, Any], offender: str
) -> None:
    application_id = str(uuid.uuid4())
    response = _register(store, application_id, **extra)
    assert_redacted_422(response, offender)
    assert _applications_with_id(application_id) == 0


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"run_tools": ["issues.read", "issues.read"]}, id="run-duplicate"),
        pytest.param({"gated_tools": ["issues.write", "issues.write"]}, id="gated-duplicate"),
        pytest.param(
            {"run_tools": ["issues.read", "issues.write", "issues.read"]},
            id="run-duplicate-not-adjacent",
        ),
        pytest.param(
            {"run_tools": ["issues.read", "issues.read"], "gated_tools": ["issues.write"]},
            id="run-duplicate-gated-valid",
        ),
    ],
)
def test_duplicate_entries_are_refused_and_not_stored(
    store: SharedStore, extra: dict[str, Any]
) -> None:
    application_id = str(uuid.uuid4())
    response = _register(store, application_id, **extra)
    assert_redacted_422(response)
    assert _applications_with_id(application_id) == 0


def test_a_refused_registration_does_not_block_the_valid_one_after_it(
    store: SharedStore,
) -> None:
    application_id = str(uuid.uuid4())
    assert_redacted_422(_register(store, application_id, run_tools=["issues.delete"]))
    assert _applications_with_id(application_id) == 0
    accepted = _register(store, application_id, run_tools=["issues.read"])
    assert accepted.status_code == 201, accepted.text
    assert _stored_policy(application_id)["run_tools"] == ["issues.read"]


# --- a policy without the lists is what it was before -------------------------


def test_a_policy_without_the_lists_stores_no_keys_and_keeps_its_pin_digest(
    store: SharedStore,
) -> None:
    application_id = str(uuid.uuid4())
    body = store.registration_body(application_id)
    assert set(body["policy"]) == {"tools", "custody"}
    created = store.operator("POST", "/applications", body)
    assert created.status_code == 201, created.text
    record = created.json()

    assert record["policy"] == {
        "tools": list(TOOLS),
        "custody": "platform",
        "run_tools": [],
        "gated_tools": [],
    }
    row = application_row(application_id)
    assert row["policy"] == shared_policy()
    assert set(row["policy"]) == {"tools", "custody"}
    # The digest the policy had before the lists existed: spec plus the two-key policy.
    assert row["pin_digest"] == _pin_digest(row["spec"], shared_policy())
    assert row["pin_digest"] == _pin_digest(row["spec"], row["policy"])

    replay = store.operator("POST", "/applications", body)
    assert replay.status_code == 200, replay.text
    assert replay.json() == record
    assert application_row(application_id)["pin_digest"] == row["pin_digest"]


def test_explicit_empty_lists_are_the_same_registration_as_omitting_them(
    store: SharedStore,
) -> None:
    application_id = str(uuid.uuid4())
    omitted = store.registration_body(application_id)
    created = store.operator("POST", "/applications", omitted)
    assert created.status_code == 201, created.text
    digest = application_row(application_id)["pin_digest"]

    replay = _register(store, application_id, run_tools=[], gated_tools=[])
    assert replay.status_code == 200, replay.text
    assert replay.json() == created.json()
    row = application_row(application_id)
    assert row["pin_digest"] == digest
    assert set(row["policy"]) == {"tools", "custody"}


def test_explicit_empty_lists_on_first_registration_store_no_keys(store: SharedStore) -> None:
    application_id = str(uuid.uuid4())
    created = _register(store, application_id, run_tools=[], gated_tools=[])
    assert created.status_code == 201, created.text
    assert created.json()["policy"]["run_tools"] == []
    assert created.json()["policy"]["gated_tools"] == []

    row = application_row(application_id)
    assert row["policy"] == shared_policy()
    assert row["pin_digest"] == _pin_digest(row["spec"], shared_policy())

    replay = store.operator("POST", "/applications", store.registration_body(application_id))
    assert replay.status_code == 200, replay.text


def test_lists_are_pinned_so_a_changed_registration_is_immutable(store: SharedStore) -> None:
    application_id = str(uuid.uuid4())
    created = _register(store, application_id, run_tools=RUN_TOOLS, gated_tools=GATED_TOOLS)
    assert created.status_code == 201, created.text
    before = application_row(application_id)

    replay = _register(store, application_id, run_tools=RUN_TOOLS, gated_tools=GATED_TOOLS)
    assert replay.status_code == 200, replay.text
    assert replay.json() == created.json()

    for changed in (
        {"run_tools": RUN_TOOLS[:1], "gated_tools": GATED_TOOLS},
        {"run_tools": RUN_TOOLS, "gated_tools": ["issues.write"]},
        {"run_tools": RUN_TOOLS},
        {"gated_tools": GATED_TOOLS},
        {},
    ):
        assert_refused(
            _register(store, application_id, **changed), 409, "shared_application_immutable"
        )
    after = application_row(application_id)
    assert after["policy"] == before["policy"]
    assert after["pin_digest"] == before["pin_digest"]


# --- the application role reads the policy ------------------------------------


def test_the_application_role_reads_its_own_policy_lists_and_no_other_applications(
    store: SharedStore, sessions: SessionPool
) -> None:
    first = str(uuid.uuid4())
    second = str(uuid.uuid4())
    assert (
        _register(store, first, run_tools=RUN_TOOLS, gated_tools=GATED_TOOLS).status_code == 201
    )
    assert _register(store, second).status_code == 201

    _, first_dsn = store.provision_bundled(first, store.register_recipient(first))
    _, second_dsn = store.provision_bundled(second, store.register_recipient(second))
    first_role = sessions.open(first_dsn)
    second_role = sessions.open(second_dsn)

    query = "SELECT policy FROM curie.shared_connector_applications WHERE id = $1"
    own = first_role.fetchrow(query, uuid.UUID(first))
    assert own is not None
    policy = _as_json(own["policy"])
    assert policy["tools"] == list(TOOLS)
    assert policy["run_tools"] == RUN_TOOLS
    assert policy["gated_tools"] == GATED_TOOLS
    assert policy == application_row(first)["policy"]

    listed = _as_json(
        first_role.fetchval(
            "SELECT policy->'run_tools' FROM curie.shared_connector_applications WHERE id = $1",
            uuid.UUID(first),
        )
    )
    assert listed == RUN_TOOLS

    # The other application's role reads its own row, without the lists, and not this one.
    assert second_role.fetch(query, uuid.UUID(first)) == []
    assert [
        str(row["id"])
        for row in second_role.fetch("SELECT id FROM curie.shared_connector_applications")
    ] == [second]
    assert [
        str(row["id"])
        for row in first_role.fetch("SELECT id FROM curie.shared_connector_applications")
    ] == [first]
    other = second_role.fetchrow(query, uuid.UUID(second))
    assert other is not None
    assert _as_json(other["policy"]) == shared_policy()
    assert first_role.fetch(query, uuid.UUID(second)) == []
