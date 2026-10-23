"""Shared connector renders are the frozen functions' output (ADR 0239, cp5, AC2, AC5).

``GET /agents/{id}/versions/{vid}/connectors`` renders the dedicated subset
exactly as before and adds ``shared``: per resolved shared entry, its alias,
application and ``reach = render_member_reach(stored spec, member)``, where the
member is the agent UUID with the runner sandbox selector built from the
request's release and app name. Any shared refusal is an explicit 4xx with the
code, never a 200 with the entry dropped.

The worker's application render (``GET /v1/internal/shared-connectors/
applications/{id}/render``) answers ``render_application(stored spec)``. Both
are compared here, as canonical JSON bytes, against the frozen functions called
independently on the stored spec. Membership never reaches the application's
workload, so adding or removing a member leaves the Deployment template, and so
every running proxy, untouched.

Every case drives the real routes through a TestClient on the disposable
database with real RustFS; nothing is faked.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from collections.abc import Iterator
from typing import Any

import nacl.signing
import pytest
from _shared_connector_support import (
    HOSTED_DEDICATED,
    REMOTE_DEDICATED,
    RENDER_ROOT,
    SharedCleanup,
    SharedStore,
    application_row,
    assert_declaration_refused,
    bundle_archive,
    canonical,
    connectors_yaml,
    random_installation,
    recipient_rows,
    restore_installation,
    rewrite_stored_spec,
    shared_app,
    shared_entry,
)
from curie_api.config import get_settings
from plugin_format.shared_connector_render import (
    SharedApplication,
    SharedMember,
    render_application,
    render_member_reach,
)

AGENT_LABEL = "curietech.ai/agent-id"
APPLICATION_LABEL = "curietech.ai/shared-application"
OWNER_LABEL = "curie.dev/connector-owner"
ALIAS = "tracker"
PROXY_IMAGE = "ghcr.io/curie-eng/curie-worker:0.0.0"
SNAPSHOT_SEAL = {"name": "acme-snapshot", "key": "seal"}


def _caller_env() -> dict[str, str]:
    """A caller verify key, so the hosted dedicated sibling renders with its proxy."""

    key = base64.b64encode(bytes(nacl.signing.SigningKey.generate().verify_key)).decode()
    return {"CURIE_CONNECTOR_CALLER_PUBLIC_KEY": key, "CURIE_CONNECTOR_PROXY_IMAGE": PROXY_IMAGE}


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def caller_env() -> dict[str, str]:
    return _caller_env()


@pytest.fixture
def store(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch, caller_env: dict[str, str]
) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation, **caller_env) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


def _stored_spec(application_id: str) -> SharedApplication:
    return SharedApplication.model_validate_json(
        json.dumps(application_row(application_id)["spec"])
    )


def _member(agent_id: str) -> SharedMember:
    return SharedMember.model_validate(
        {
            "agent": agent_id,
            "runner_labels": {
                AGENT_LABEL: agent_id,
                "app.kubernetes.io/component": "runner-sandbox",
                "app.kubernetes.io/name": RENDER_ROOT["app_name"],
                "app.kubernetes.io/instance": RENDER_ROOT["release"],
            },
        }
    )


def _mixed(application_id: str, alias: str = ALIAS) -> bytes:
    return bundle_archive(connectors_yaml(shared_entry(alias, application_id), HOSTED_DEDICATED))


def _member_version(
    store: SharedStore, application_id: str, agent_id: str, alias: str = ALIAS
) -> str:
    """``agent_id`` joins under ``alias``, then uploads a mixed bundle: membership first."""

    store.member(application_id, agent_id, alias)
    return store.uploaded(agent_id, _mixed(application_id, alias))


def _ok(response: Any) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


def _app_objects(store: SharedStore, application_id: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = _ok(store.application_render(application_id))["objects"]
    return objects


def _deployment(objects: list[dict[str, Any]]) -> dict[str, Any]:
    (deployment,) = [item for item in objects if item["kind"] == "Deployment"]
    return deployment


def _template_hash(objects: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical(_deployment(objects)["spec"]["template"])).hexdigest()


# --- member reach --------------------------------------------------------------


def test_render_lists_member_reach_from_frozen_function(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    version_id = _member_version(store, application_id, agent_id)

    body = _ok(store.render(agent_id, version_id))
    expected = render_member_reach(_stored_spec(application_id), _member(agent_id))
    assert [(entry["name"], entry["application"]) for entry in body["shared"]] == [
        (ALIAS, application_id)
    ]
    assert canonical(body["shared"][0]["reach"]) == canonical(expected)

    # Negative: nothing in the reach selects the runner sandbox without pinning
    # this member's UUID, and every object is labelled with it.
    for item in body["shared"][0]["reach"]:
        assert item["metadata"]["labels"][AGENT_LABEL] == agent_id
        assert item["metadata"]["labels"][APPLICATION_LABEL] == application_id
        selectors = [item["spec"]["podSelector"]["matchLabels"]]
        for rule in item["spec"].get("ingress", []) + item["spec"].get("egress", []):
            for peer in rule.get("from", []) + rule.get("to", []):
                selectors.append(peer["podSelector"]["matchLabels"])
        for selector in selectors:
            if selector.get("app.kubernetes.io/component") == "runner-sandbox":
                assert selector.get(AGENT_LABEL) == agent_id, item
    # The reach never carries the dedicated owner label the dedicated applier prunes by.
    assert OWNER_LABEL not in json.dumps(body["shared"])


def test_render_dedicated_manifests_unchanged_with_shared_present(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    mixed = _member_version(store, application_id, agent_id)
    dedicated = store.uploaded(agent_id, bundle_archive(connectors_yaml(HOSTED_DEDICATED)))

    with_shared = _ok(store.render(agent_id, mixed))
    without = _ok(store.render(agent_id, dedicated))
    assert without["shared"] == []
    assert with_shared["shared"] != []
    for field in ("manifests", "mcp_entries", "owned_secret_name", "owned_secret_keys", "triggers"):
        assert canonical(with_shared[field]) == canonical(without[field]), field
    # Secondary: the shared alias never becomes a dedicated object or MCP entry.
    assert ALIAS not in with_shared["mcp_entries"]
    assert all(ALIAS not in item["metadata"]["name"] for item in with_shared["manifests"])
    assert with_shared["manifests"], "the hosted dedicated sibling must still render"


def test_render_refuses_explicitly(
    store: SharedStore,
    cleanup: SharedCleanup,
    monkeypatch: pytest.MonkeyPatch,
    caller_env: dict[str, str],
) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    version_id = _member_version(store, application_id, agent_id)
    named = {"application": application_id, "connector": ALIAS, "declared": [application_id]}

    # Liveness: the member renders.
    assert _ok(store.render(agent_id, version_id))["shared"][0]["application"] == application_id

    for root in ({"release": "other"}, {"app_name": "other"}, {"namespace": "other"}):
        assert_declaration_refused(
            store.render(agent_id, version_id, **root),
            409,
            "shared_application_root_mismatch",
            **named,
        )

    removed = store.remove_member(application_id, agent_id)
    assert removed.status_code == 200, removed.text
    assert_declaration_refused(
        store.render(agent_id, version_id), 409, "shared_application_not_member", **named
    )
    store.member(application_id, agent_id, ALIAS)
    assert _ok(store.render(agent_id, version_id))["shared"]

    # The same stored application, read by an API of another installation, is unknown.
    foreign = cleanup.installation(random_installation())
    with shared_app(monkeypatch, foreign, **caller_env) as client:
        assert_declaration_refused(
            SharedStore(client, foreign, cleanup).render(agent_id, version_id),
            409,
            "shared_application_unknown",
            **named,
        )
    with shared_app(monkeypatch, store.installation, observed="false", **caller_env) as client:
        assert_declaration_refused(
            SharedStore(client, store.installation, cleanup).render(agent_id, version_id),
            409,
            "shared_installation_unobserved",
        )
    restore_installation(monkeypatch, store.installation)
    assert _ok(store.render(agent_id, version_id))["shared"]

    assert store.disable_application(application_id).status_code == 200
    assert_declaration_refused(
        store.render(agent_id, version_id), 409, "shared_application_disabled", **named
    )


def test_render_restricted_by_the_agents_target(store: SharedStore) -> None:
    """Objection 5: parse, restrict to the agent's targets, then resolve what remains."""

    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    name = store.agent_name(agent_id)
    store.member(application_id, agent_id, ALIAS)
    deploy = f"targets:\n  dev:\n    agent: {name}\n    connectors: [grafana]\n"
    version_id = store.uploaded(
        agent_id,
        bundle_archive(
            connectors_yaml(shared_entry(ALIAS, application_id), HOSTED_DEDICATED), deploy=deploy
        ),
    )
    assert store.remove_member(application_id, agent_id).status_code == 200
    # The target excludes the alias, so no membership is needed and none is rendered.
    body = _ok(store.render(agent_id, version_id))
    assert body["shared"] == []
    assert sorted(body["mcp_entries"]) == ["grafana"]


# --- the application render ----------------------------------------------------


def test_application_render_is_frozen_output(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    objects = _app_objects(store, application_id)
    assert canonical(objects) == canonical(render_application(_stored_spec(application_id)))
    for item in objects:
        labels = item["metadata"]["labels"]
        assert labels[APPLICATION_LABEL] == application_id
        assert labels["app.kubernetes.io/component"] == "shared-connector-application"
        assert OWNER_LABEL not in labels
        assert AGENT_LABEL not in labels
    assert OWNER_LABEL not in json.dumps(objects)


def test_membership_change_leaves_template_hash_identical(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    a, b = store.create_agent(), store.create_agent()
    _member_version(store, application_id, a)
    before = _template_hash(_app_objects(store, application_id))

    b_version = _member_version(store, application_id, b)
    joined = _template_hash(_app_objects(store, application_id))
    b_reach = _ok(store.render(b, b_version))["shared"]
    assert [entry["application"] for entry in b_reach] == [application_id]

    assert store.remove_member(application_id, b).status_code == 200
    left = _template_hash(_app_objects(store, application_id))
    assert before == joined == left
    # Secondary: B's own render followed the membership, not the template.
    assert_declaration_refused(
        store.render(b, b_version),
        409,
        "shared_application_not_member",
        application=application_id,
        connector=ALIAS,
    )


def test_membership_never_in_pod_env(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    a, b = store.create_agent(), store.create_agent()
    _member_version(store, application_id, a, "tracker")
    _member_version(store, application_id, b, "issues")

    objects = _app_objects(store, application_id)
    deployment = _deployment(objects)
    values = [
        str(entry.get("value", ""))
        for container in deployment["spec"]["template"]["spec"]["containers"]
        for entry in container.get("env", [])
    ]
    assert values, "the proxy and server carry environment"
    for marker in (a, b, "tracker", "issues"):
        assert all(marker not in value for value in values), marker
    # Secondary: no member identity anywhere in the application's objects.
    text = json.dumps(objects)
    assert a not in text and b not in text


def _containers(objects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    pod = _deployment(objects)["spec"]["template"]["spec"]
    return {container["name"]: container for container in pod["containers"]}


def _mounts(container: dict[str, Any]) -> set[str]:
    return {mount["name"] for mount in container.get("volumeMounts", [])}


def test_pod_least_privilege(store: SharedStore) -> None:
    sealed_id = str(uuid.uuid4())
    sealed = store.registration_body(sealed_id)
    sealed["application"]["snapshot_seal"] = dict(SNAPSHOT_SEAL)
    created = store.operator("POST", "/applications", sealed)
    assert created.status_code == 201, created.text
    plain_id = store.register_application()["application_id"]

    for application_id, snapshot in ((sealed_id, True), (plain_id, False)):
        objects = _app_objects(store, application_id)
        pod = _deployment(objects)["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False
        assert pod["shareProcessNamespace"] is False
        for field in ("hostNetwork", "hostPID", "hostIPC"):
            assert not pod.get(field), field
        volumes = {volume["name"]: volume for volume in pod["volumes"]}
        assert all("hostPath" not in volume for volume in volumes.values())
        assert volumes["resolver"] == {"name": "resolver", "emptyDir": {}}

        containers = _containers(objects)
        assert set(containers) == {"server", "proxy"}
        for name, container in containers.items():
            context = container["securityContext"]
            assert context["runAsNonRoot"] is True, name
            assert context["runAsUser"] == 10001, name
            assert context["capabilities"]["drop"] == ["ALL"], name
            assert context["readOnlyRootFilesystem"] is True, name
            assert context["allowPrivilegeEscalation"] is False, name
            assert not context.get("privileged"), name

        proxy, server = _mounts(containers["proxy"]), _mounts(containers["server"])
        for secret in ("encryption-key", "database-dsn", "grant-store"):
            assert secret in proxy, secret
            assert secret not in server, secret
        if snapshot:
            assert "snapshot-seal" in server
            assert "snapshot-seal" not in proxy
            assert volumes["snapshot-seal"]["secret"]["secretName"] == SNAPSHOT_SEAL["name"]
        else:
            assert "snapshot-seal" not in volumes
            assert "snapshot-seal" not in server | proxy


def test_remote_dedicated_sibling_renders_beside_shared(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    version_id = store.uploaded(
        agent_id,
        bundle_archive(connectors_yaml(shared_entry(ALIAS, application_id), REMOTE_DEDICATED)),
    )
    body = _ok(store.render(agent_id, version_id))
    assert body["manifests"] == []
    assert sorted(body["mcp_entries"]) == ["internal"]
    assert [entry["name"] for entry in body["shared"]] == [ALIAS]


@pytest.mark.parametrize(
    "drift",
    [
        pytest.param(
            {"encryption_key": {"name": "acme-encryption", "key": "private"}}, id="key-ref"
        ),
        pytest.param({"database_dsn": {"name": "acme-database", "key": "dsn"}}, id="bundled-dsn"),
        pytest.param(
            {"snapshot_seal": {"name": "shared-dsn-" + "0" * 8, "key": "seal"}},
            id="operator-ref-worker-prefix",
        ),
    ],
)
def test_render_refuses_stored_spec_with_invalid_secret_ref(
    store: SharedStore, drift: dict[str, Any]
) -> None:
    """Review r1 CR1-3: the agent render resolves the stored spec and refuses it, never omits."""

    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    version_id = _member_version(store, application_id, agent_id)
    store.register_recipient(application_id)
    # Liveness before the drift: the member renders.
    assert _ok(store.render(agent_id, version_id))["shared"]
    rewrite_stored_spec(application_id, drift)
    before = recipient_rows(application_id)

    assert_declaration_refused(
        store.render(agent_id, version_id),
        409,
        "shared_application_secret_ref_invalid",
        application=application_id,
        connector=ALIAS,
        declared=[application_id],
    )
    assert recipient_rows(application_id) == before

    # Liveness: another member of a canonical stored spec still renders.
    other_application = store.register_application()["application_id"]
    other_agent = store.create_agent()
    other_version = _member_version(store, other_application, other_agent)
    body = _ok(store.render(other_agent, other_version))
    assert canonical(body["shared"][0]["reach"]) == canonical(
        render_member_reach(_stored_spec(other_application), _member(other_agent))
    )
