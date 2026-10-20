"""Pure shared application manifests and independently owned member reach."""

import json
import re

import pytest
from plugin_format.shared_connector_render import (
    DatabaseTarget,
    SecretKeyRef,
    SharedApplication,
    SharedMember,
    render_application,
    render_member_reach,
)
from pydantic import ValidationError

APPLICATION = "12345678-1234-4234-8234-123456789abc"
AGENT = "22345678-1234-4234-8234-123456789abc"
OTHER_AGENT = "32345678-1234-4234-8234-123456789abc"
SERVER_IMAGE = "example.com/acme/server@sha256:" + "a" * 64
PROXY_IMAGE = "example.com/acme/proxy@sha256:" + "b" * 64
DB_LABELS = {
    "curietech.ai/shared-application": APPLICATION,
    "app.kubernetes.io/component": "database",
}


def _application(**changes) -> SharedApplication:
    return SharedApplication(
        **(
            {
                "installation": "opaque:installation/1",
                "application": APPLICATION,
                "release": "acme",
                "namespace": "acme",
                "app_name": "curie",
                "server_image": SERVER_IMAGE,
                "proxy_image": PROXY_IMAGE,
                "server_args": ("--transport", "streamable-http"),
                "server_env": {"MODE": "read"},
                "encryption_key": {"name": "acme-encryption", "key": "private"},
                "database_dsn": {"name": "acme-database", "key": "dsn"},
                "dsn_encoding": "bundled-sealed",
                "grant_store": {"name": "acme-grants", "key": "url"},
                "provider_origins": ("https://api.example.com",),
                "credential_slots": ("access_token",),
                "database_target": {
                    "namespace": "acme-data",
                    "pod_labels": DB_LABELS,
                    "port": 5432,
                },
            }
            | changes
        )
    )


def _member(application: SharedApplication, agent: str = AGENT) -> SharedMember:
    return SharedMember(
        agent=agent,
        runner_labels={
            "app.kubernetes.io/name": application.app_name,
            "app.kubernetes.io/instance": application.release,
            "app.kubernetes.io/component": "runner-sandbox",
            "curietech.ai/agent-id": agent,
        },
    )


def _object(objects: list[dict], kind: str) -> dict:
    return next(item for item in objects if item["kind"] == kind)


def _mounted_secrets(pod: dict, container: dict) -> set[tuple[str, str]]:
    volumes = {volume["name"]: volume for volume in pod["volumes"]}
    found = set()
    for mount in container["volumeMounts"]:
        volume = volumes[mount["name"]]
        if "secret" in volume:
            assert mount["readOnly"] is True
            secret = volume["secret"]
            assert secret.get("optional", False) is False
            found.update((secret["secretName"], item["key"]) for item in secret["items"])
    return found


@pytest.mark.parametrize("encoding", ["bundled-sealed", "managed-raw"])
@pytest.mark.parametrize("snapshot", [False, True])
def test_shared_application_isolates_proxy_secrets_and_snapshot(
    encoding: str, snapshot: bool
) -> None:
    application = _application(
        dsn_encoding=encoding,
        snapshot_seal=({"name": "acme-snapshot", "key": "seal"} if snapshot else None),
    )
    before = application.model_dump_json()
    objects = render_application(application)
    assert application.model_dump_json() == before
    pod = _object(objects, "Deployment")["spec"]["template"]["spec"]
    assert len(pod["containers"]) == 2
    server = next(
        container for container in pod["containers"] if container["image"] == SERVER_IMAGE
    )
    proxy = next(container for container in pod["containers"] if container["image"] == PROXY_IMAGE)
    assert _mounted_secrets(pod, server) == ({("acme-snapshot", "seal")} if snapshot else set())
    assert _mounted_secrets(pod, proxy) == {
        ("acme-encryption", "private"),
        ("acme-database", "dsn"),
        ("acme-grants", "url"),
    }
    assert encoding in json.dumps(proxy)
    assert {entry["name"]: entry.get("value") for entry in server["env"]}["MODE"] == "read"
    assert server["args"] == list(application.server_args)
    socket_mounts = [
        next(
            mount
            for mount in container["volumeMounts"]
            if application.resolver_socket.startswith(mount["mountPath"].rstrip("/") + "/")
        )
        for container in (server, proxy)
    ]
    assert socket_mounts[0]["name"] == socket_mounts[1]["name"]
    assert (
        next(volume for volume in pod["volumes"] if volume["name"] == socket_mounts[0]["name"])[
            "emptyDir"
        ]
        == {}
    )
    assert pod["automountServiceAccountToken"] is False
    assert pod["shareProcessNamespace"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["runAsUser"] > 0
    assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    for container in (server, proxy):
        assert not container.get("envFrom")
        assert all("valueFrom" not in entry for entry in container.get("env", []))
        security = container["securityContext"]
        assert security["allowPrivilegeEscalation"] is False
        assert security["readOnlyRootFilesystem"] is True
        assert security["capabilities"] == {"drop": ["ALL"]}
        assert not security.get("privileged", False)
        assert security.get("runAsUser", pod["securityContext"]["runAsUser"]) > 0
        assert container["resources"].keys() == {"requests", "limits"}
        assert all(set(side) == {"cpu", "memory"} for side in container["resources"].values())
    assert proxy["resources"] == {
        "requests": {"cpu": "25m", "memory": "128Mi"},
        "limits": {"cpu": "1000m", "memory": "512Mi"},
    }
    assert proxy["readinessProbe"]


@pytest.mark.parametrize("managed", [False, True])
def test_shared_application_default_deny_service_and_database_target(managed: bool) -> None:
    target = (
        {"cidrs": ("192.0.2.0/24", "2001:db8::/64"), "port": 6432}
        if managed
        else {
            "namespace": "acme-data",
            "pod_labels": DB_LABELS,
            "port": 5432,
        }
    )
    application = _application(
        database_target=target, dsn_encoding=("managed-raw" if managed else "bundled-sealed")
    )
    objects = render_application(application)
    assert sorted(item["kind"] for item in objects) == [
        "Deployment",
        "NetworkPolicy",
        "NetworkPolicy",
        "Service",
    ]
    deployment = _object(objects, "Deployment")
    selector = deployment["spec"]["selector"]["matchLabels"]
    labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert selector and selector.items() <= labels.items()
    assert labels["app.kubernetes.io/component"] == "shared-connector-application"
    assert "curietech.ai/agent" not in labels and "curietech.ai/agent-id" not in labels
    assert APPLICATION in labels.values()
    service = _object(objects, "Service")
    assert service["spec"]["selector"] == selector
    ports = service["spec"]["ports"]
    assert len(ports) == 1 and ports[0]["port"] == application.proxy_port
    pod = deployment["spec"]["template"]["spec"]
    proxy = next(container for container in pod["containers"] if container["image"] == PROXY_IMAGE)
    target_port = ports[0]["targetPort"]
    if isinstance(target_port, str):
        target_port = next(
            port["containerPort"] for port in proxy["ports"] if port["name"] == target_port
        )
    assert target_port == application.proxy_port
    policies = [item["spec"] for item in objects if item["kind"] == "NetworkPolicy"]
    ingress = next(policy for policy in policies if policy["policyTypes"] == ["Ingress"])
    assert ingress["podSelector"] == {"matchLabels": selector}
    assert ingress["ingress"] == []
    egress = next(policy for policy in policies if policy["policyTypes"] == ["Egress"])
    assert egress["podSelector"] == {"matchLabels": selector}
    peers = (
        [{"ipBlock": {"cidr": cidr}} for cidr in target["cidrs"]]
        if managed
        else [
            {
                "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "acme-data"}},
                "podSelector": {"matchLabels": DB_LABELS},
            }
        ]
    )
    assert egress["egress"] == [
        {"to": peers, "ports": [{"protocol": "TCP", "port": target["port"]}]}
    ]


def test_member_reach_is_exact_and_leaves_application_template_unchanged() -> None:
    application = _application()
    original = render_application(application)
    template = json.dumps(_object(original, "Deployment")["spec"]["template"], sort_keys=True)
    selector = _object(original, "Deployment")["spec"]["selector"]
    names = set()
    for agent in (AGENT, OTHER_AGENT):
        member = _member(application, agent)
        before = member.model_dump_json()
        policies = render_member_reach(application, member)
        assert member.model_dump_json() == before
        assert len(policies) == 2
        ingress = next(item for item in policies if item["spec"]["policyTypes"] == ["Ingress"])
        egress = next(item for item in policies if item["spec"]["policyTypes"] == ["Egress"])
        runner = {"matchLabels": member.runner_labels}
        ports = [{"protocol": "TCP", "port": application.proxy_port}]
        assert ingress["spec"] == {
            "podSelector": selector,
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [{"podSelector": runner}], "ports": ports}],
        }
        assert egress["spec"] == {
            "podSelector": runner,
            "policyTypes": ["Egress"],
            "egress": [{"to": [{"podSelector": selector}], "ports": ports}],
        }
        new_names = {item["metadata"]["name"] for item in policies}
        assert len(new_names) == 2 and names.isdisjoint(new_names)
        names.update(new_names)
        assert template == json.dumps(
            _object(render_application(application), "Deployment")["spec"]["template"],
            sort_keys=True,
        )
    assert render_application(application) == original


@pytest.mark.parametrize("release", ["r" * 63, "1" + "r" * 62])
def test_shared_names_are_bounded_deterministic_and_identity_scoped(release: str) -> None:
    name_sets = []
    for changes in (
        {},
        {"application": OTHER_AGENT},
        {"installation": "opaque:installation/2"},
        {"installation": "é" * 64},
    ):
        application = _application(release=release, app_name="a" * 63, **changes)
        objects = render_application(application) + render_member_reach(
            application, _member(application)
        )
        names = {item["metadata"]["name"] for item in objects}
        assert len(names) == len(objects)
        assert all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", name) for name in names)
        assert objects == render_application(application) + render_member_reach(
            application, _member(application)
        )
        assert all(names.isdisjoint(previous) for previous in name_sets)
        name_sets.append(names)


def test_shared_resources_preserve_explicit_bounds() -> None:
    resources = {
        "requests": {"cpu": "50m", "memory": "96Mi"},
        "limits": {"cpu": "200m", "memory": "192Mi"},
    }
    objects = render_application(
        _application(server_resources=resources, proxy_resources=resources)
    )
    containers = _object(objects, "Deployment")["spec"]["template"]["spec"]["containers"]
    assert all(container["resources"] == resources for container in containers)


def test_shared_outputs_omit_dedicated_prune_ownership() -> None:
    application = _application()

    def check(value) -> None:
        if isinstance(value, dict):
            assert "curie.dev/connector-owner" not in value
            for child in value.values():
                check(child)
        elif isinstance(value, list):
            for child in value:
                check(child)

    check(render_application(application) + render_member_reach(application, _member(application)))


@pytest.mark.parametrize("invalid", ["runner-selector", "snapshot-overlap", "secret-ref"])
def test_render_revalidates_mutable_and_unvalidated_inputs(invalid: str) -> None:
    neighbor = _application()
    expected = render_application(neighbor), render_member_reach(neighbor, _member(neighbor))
    application = _application()
    member = _member(application)
    if invalid == "runner-selector":
        member.runner_labels.clear()
    else:
        field = "snapshot_seal" if invalid == "snapshot-overlap" else "encryption_key"
        reference = (
            application.encryption_key
            if invalid == "snapshot-overlap"
            else (SecretKeyRef.model_construct(name="../invalid", key="private"))
        )
        application = application.model_copy(update={field: reference})
        with pytest.raises(ValueError):
            render_application(application)
    with pytest.raises(ValueError):
        render_member_reach(application, member)
    assert (
        render_application(neighbor),
        render_member_reach(neighbor, _member(neighbor)),
    ) == expected


@pytest.mark.parametrize(
    "changes",
    [{"installation": value} for value in ("", "a b", "a\tb", "a\x00b", "x" * 129, "é" * 65, 1)]
    + [
        {"application": value}
        for value in (
            APPLICATION.upper(),
            APPLICATION.replace("-", ""),
            "00000000-0000-0000-0000-000000000000",
            "bad",
        )
    ]
    + [
        {field: value}
        for field in ("server_image", "proxy_image")
        for value in (
            "example.com/acme/mcp:latest",
            "example.com/acme/mcp@sha256:" + "A" * 64,
            "example.com/acme/mcp@sha256:abc",
            "@sha256:" + "a" * 64,
        )
    ]
    + [{"server_port": value} for value in (0, 65536, True, 8000.0, "8000", 8480)]
    + [
        {"namespace": ""},
        {"release": "../acme"},
        {"app_name": "Bad Name"},
        {"resolver_socket": "relative.sock"},
        {"dsn_encoding": "unknown"},
        {"server_env": {"BAD-NAME": "value"}},
        {"server_env": {"MODE": 1}},
        {"provider_origins": ("https://api.example.com/path",)},
        {"provider_origins": ("https://user:pass@example.com",)},
        {"provider_origins": ("https://*.example.com",)},
        {"provider_origins": ("https://api.example.com?query=1",)},
        {"provider_origins": ("https://api.example.com#fragment",)},
        {"credential_slots": ("",)},
        {"credential_slots": ("../token",)},
        {"credential_slots": ("access_token", "access_token")},
        {"server_resources": {"limits": {"memory": "unbounded"}}},
        {"proxy_resources": {"requests": {"cpu": "2000m"}, "limits": {"cpu": "1000m"}}},
        {"members": {}},
        {"alias": "docs"},
        {"policy": {}},
        {"enabled": True},
    ],
)
def test_shared_application_rejects_invalid_inputs_on_construction(changes: dict) -> None:
    with pytest.raises(ValidationError):
        _application(**changes)


@pytest.mark.parametrize(
    "target",
    [
        {"port": 5432},
        {"cidrs": (), "port": 5432},
        {"pod_labels": {}, "port": 5432},
        {"pod_labels": DB_LABELS, "port": 5432},
        {"cidrs": ("0.0.0.0/0",), "port": 5432},
        {"cidrs": ("::/0",), "port": 5432},
        {"cidrs": ("192.0.2.1/24",), "port": 5432},
        {
            "namespace": "acme-data",
            "pod_labels": DB_LABELS,
            "cidrs": ("192.0.2.0/24",),
            "port": 5432,
        },
        {"cidrs": ("192.0.2.0/24",)},
        {"cidrs": ("192.0.2.0/24",), "port": True},
        {"cidrs": ("192.0.2.0/24",), "port": "5432"},
        {"cidrs": ("192.0.2.0/24",), "port": 0},
        {"namespace": "acme-data", "pod_labels": {"bad key": "value"}, "port": 5432},
    ],
)
def test_database_target_rejects_broad_or_ambiguous_destinations(target: dict) -> None:
    with pytest.raises(ValidationError):
        DatabaseTarget(**target)


@pytest.mark.parametrize("name", ["acme-encryption", "acme-database", "acme-grants"])
def test_snapshot_secret_name_cannot_be_shared_with_proxy(name: str) -> None:
    with pytest.raises(ValidationError):
        _application(snapshot_seal={"name": name, "key": "different-key"})


@pytest.mark.parametrize(
    "reference",
    [
        {"name": "Bad Name", "key": "key"},
        {"name": "acme", "key": ""},
        {"name": "acme", "key": "../key"},
        {"name": "acme", "key": 1},
    ],
)
def test_secret_key_ref_validates_on_construction(reference: dict) -> None:
    with pytest.raises(ValidationError):
        SecretKeyRef(**reference)


@pytest.mark.parametrize(
    "change", [{"agent": AGENT.upper()}, {"agent": "bad"}, {"alias": "docs"}, {"policy": {}}]
)
def test_member_identity_and_fields_validate_on_construction(change: dict) -> None:
    values = {"agent": AGENT, "runner_labels": _member(_application()).runner_labels}
    with pytest.raises(ValidationError):
        SharedMember(**(values | change))


@pytest.mark.parametrize(
    "missing",
    [
        "curietech.ai/agent-id",
        "app.kubernetes.io/instance",
        "app.kubernetes.io/name",
        "app.kubernetes.io/component",
    ],
)
def test_member_rejects_broad_runner_selector(missing: str) -> None:
    labels = dict(_member(_application()).runner_labels)
    del labels[missing]
    with pytest.raises(ValidationError):
        SharedMember(agent=AGENT, runner_labels=labels)


def test_member_rejects_wrong_agent_or_release_selector() -> None:
    application = _application()
    labels = dict(_member(application).runner_labels)
    with pytest.raises(ValidationError):
        SharedMember(agent=OTHER_AGENT, runner_labels=labels)
    labels["app.kubernetes.io/instance"] = "other-release"
    with pytest.raises(ValueError):
        render_member_reach(application, SharedMember(agent=AGENT, runner_labels=labels))


@pytest.mark.parametrize(
    "factory,field",
    [
        (_application, "application"),
        (lambda: _member(_application()), "agent"),
        (lambda: SecretKeyRef(name="acme", key="key"), "key"),
        (lambda: DatabaseTarget(cidrs=("192.0.2.0/24",), port=5432), "port"),
    ],
)
def test_shared_root_inputs_are_immutable(factory, field: str) -> None:
    value = factory()
    before = value.model_dump_json()
    with pytest.raises(ValidationError):
        setattr(value, field, getattr(value, field))
    assert value.model_dump_json() == before


@pytest.mark.parametrize("installation", ["x" * 128, "é" * 64, "opaque:/identifier"])
def test_installation_accepts_opaque_utf8_within_byte_limit(installation: str) -> None:
    assert _application(installation=installation).installation == installation
