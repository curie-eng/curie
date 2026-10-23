"""The shared render's caller public verify keys (ADR 0239 identity contract 2).

``render_application`` takes the installation's caller public keys as a
required keyword-only render-time input, never a field of the stored
``SharedApplication``, and writes them comma-joined into the proxy container's
``CURIE_CALLER_PROXY_PUBLIC_KEYS`` and nowhere else. The rule is frozen in
``tests/vectors/shared-render-caller-keys.json`` (ADR 0194 parity with the
Rust mirror); the render without that one entry is byte for byte the render
recorded before it existed (``fixtures/shared-render-baseline.json``).
"""

import base64
import hashlib
import inspect
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from plugin_format import connector_render
from plugin_format.connectors import ConnectorSpec
from plugin_format.shared_connector_render import (
    SharedApplication,
    SharedMember,
    render_application,
    render_member_reach,
)

_TESTS = Path(__file__).resolve().parent
_VECTOR_FILE = _TESTS.parents[2] / "tests" / "vectors" / "shared-render-caller-keys.json"
_BASELINE_FILE = _TESTS / "fixtures" / "shared-render-baseline.json"
_DOCUMENT = json.loads(_VECTOR_FILE.read_text(encoding="utf-8"))
_VECTORS: list[dict[str, Any]] = _DOCUMENT["vectors"]
_CASES: list[dict[str, Any]] = json.loads(_BASELINE_FILE.read_text(encoding="utf-8"))["cases"]

ENV = "CURIE_CALLER_PROXY_PUBLIC_KEYS"
CODES = [
    "caller_public_keys_empty",
    "caller_public_keys_too_many",
    "caller_public_key_malformed",
    "caller_public_keys_duplicate",
]
# The current key, then the one it replaced: the two frozen public keys.
CURRENT = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
PREVIOUS = "Kay64UG8yvCyLhqU000LxzYeUm0L/hLIl5S8kyKWbdc="
CALLER_KEYS = (CURRENT, PREVIOUS)
AGENT = "22345678-1234-4234-8234-123456789abc"
DEDICATED_PROXY_IMAGE = "example.com/acme/proxy@sha256:" + "b" * 64


def _application(case: dict[str, Any] | None = None) -> SharedApplication:
    chosen = _CASES[0] if case is None else case
    return SharedApplication.model_validate_json(json.dumps(chosen["application"]))


def _member(application: SharedApplication) -> SharedMember:
    return SharedMember(
        agent=AGENT,
        runner_labels={
            "app.kubernetes.io/name": application.app_name,
            "app.kubernetes.io/instance": application.release,
            "app.kubernetes.io/component": "runner-sandbox",
            "curietech.ai/agent-id": AGENT,
        },
    )


def _deployment(objects: list[dict[str, Any]]) -> dict[str, Any]:
    (deployment,) = [item for item in objects if item["kind"] == "Deployment"]
    return deployment


def _containers(objects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    containers = _deployment(objects)["spec"]["template"]["spec"]["containers"]
    named = {container["name"]: container for container in containers}
    assert len(named) == len(containers)
    return named


def _env(container: dict[str, Any]) -> dict[str, str]:
    names = [entry["name"] for entry in container.get("env", [])]
    assert len(set(names)) == len(names), names
    return {entry["name"]: entry["value"] for entry in container.get("env", [])}


def _without_keys(objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A copy with exactly the one proxy keys entry removed."""

    copy = deepcopy(objects)
    removed = []
    for item in copy:
        if item["kind"] != "Deployment":
            continue
        for container in item["spec"]["template"]["spec"]["containers"]:
            kept = [entry for entry in container.get("env", []) if entry["name"] != ENV]
            removed += [container["name"]] * (len(container.get("env", [])) - len(kept))
            if "env" in container:
                container["env"] = kept
    assert removed == ["proxy"], removed
    return copy


def _digest(objects: list[dict[str, Any]]) -> str:
    canonical = json.dumps(objects, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _fragments(keys: list[Any]) -> set[str]:
    """Key text a refusal must not echo; too-short pieces are skipped."""

    found = set()
    for key in keys:
        if not isinstance(key, str):
            continue
        for piece in (key, key.strip(), *key.split(",")):
            if len(piece) >= 8:
                found.add(piece)
    return found


@pytest.mark.parametrize("vector", _VECTORS, ids=[vector["name"] for vector in _VECTORS])
def test_every_vector_renders_or_refuses_as_frozen(vector: dict[str, Any]) -> None:
    keys = tuple(vector["public_keys"])
    if "env" in vector:
        objects = render_application(_application(), caller_public_keys=keys)
        assert _env(_containers(objects)["proxy"])[ENV] == vector["env"]
        return
    with pytest.raises(ValueError) as refused:
        render_application(_application(), caller_public_keys=keys)
    message = str(refused.value)
    assert message.startswith(f'{vector["refusal"]}:'), message
    for fragment in _fragments(vector["public_keys"]):
        assert fragment not in message, vector["name"]


@pytest.mark.parametrize(
    "vector",
    [vector for vector in _VECTORS if "env" in vector],
    ids=[vector["name"] for vector in _VECTORS if "env" in vector],
)
def test_every_accepted_vector_encodes_as_the_dedicated_render_does(
    vector: dict[str, Any],
) -> None:
    keys = tuple(vector["public_keys"])
    proxy = connector_render.ConnectorProxy(image=DEDICATED_PROXY_IMAGE, public_keys=keys)
    dedicated = connector_render.render(
        release="acme",
        agent="acme-bot",
        namespace="acme",
        app_name="curie",
        connector="grafana",
        spec=ConnectorSpec(
            image="grafana/mcp-grafana:0.17.2",
            args=["-t", "streamable-http", "-disable-write"],
            env={"GRAFANA_URL": "https://g.example.com"},
            secrets=["GRAFANA_TOKEN"],
        ),
        secret_name="conn-secrets",
        proxy=proxy,
    )
    dedicated_values = [
        entry["value"]
        for item in dedicated
        if item["kind"] == "Deployment"
        for container in item["spec"]["template"]["spec"]["containers"]
        for entry in container.get("env", [])
        if entry["name"] == ENV
    ]
    shared = render_application(_application(), caller_public_keys=keys)
    assert dedicated_values == [_env(_containers(shared)["proxy"])[ENV]] == [vector["env"]]


def test_the_vector_file_freezes_the_bound_and_the_precedence() -> None:
    assert _DOCUMENT["env"] == ENV
    assert _DOCUMENT["max_keys"] == 2
    assert _DOCUMENT["precedence"] == CODES
    assert {vector.get("refusal") for vector in _VECTORS} - {None} == set(CODES)


@pytest.mark.parametrize("code", CODES)
def test_every_refusal_code_has_a_vector(code: str) -> None:
    assert any(vector.get("refusal") == code for vector in _VECTORS)


# Typed as Any so the calls below reach the runtime check, not a type checker.
_RENDER: Any = render_application


@pytest.mark.parametrize("value", [list(CALLER_KEYS), CURRENT, None])
def test_keys_that_are_not_a_tuple_are_a_type_error(value: object) -> None:
    with pytest.raises(TypeError):
        _RENDER(_application(), caller_public_keys=value)


def test_the_keys_are_required() -> None:
    with pytest.raises(TypeError):
        _RENDER(_application())


def test_the_keys_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        _RENDER(_application(), CALLER_KEYS)


def test_the_keys_reach_the_proxy_container_only() -> None:
    objects = render_application(_application(), caller_public_keys=CALLER_KEYS)
    containers = _containers(objects)
    assert set(containers) == {"server", "proxy"}
    found = [
        (name, entry)
        for name, container in containers.items()
        for entry in container.get("env", [])
        if entry["name"] == ENV
    ]
    assert found == [("proxy", {"name": ENV, "value": ",".join(CALLER_KEYS)})]
    for container in containers.values():
        names = [entry["name"] for entry in container.get("env", [])]
        assert names == sorted(names), container["name"]
    server = _env(containers["server"])
    assert not [name for name in server if name.startswith("CURIE_CALLER_")]
    for value in server.values():
        assert not any(key in value for key in CALLER_KEYS), value
    # Each key appears once in the whole render: that one env entry.
    text = json.dumps(objects)
    assert all(text.count(key) == 1 for key in CALLER_KEYS)
    # No Secret, volume or mount carries the keys: another key list changes none.
    other = render_application(_application(), caller_public_keys=(PREVIOUS,))
    pod = _deployment(objects)["spec"]["template"]["spec"]
    other_pod = _deployment(other)["spec"]["template"]["spec"]
    assert pod["volumes"] == other_pod["volumes"]
    other_containers = _containers(other)
    for name, container in containers.items():
        assert container["volumeMounts"] == other_containers[name]["volumeMounts"], name


@pytest.mark.parametrize("case", _CASES, ids=[case["name"] for case in _CASES])
def test_the_render_without_the_keys_entry_is_the_recorded_render(case: dict[str, Any]) -> None:
    assert len(_CASES) == 4
    objects = render_application(_application(case), caller_public_keys=CALLER_KEYS)
    assert _digest(_without_keys(objects)) == case["sha256"]


def test_the_stored_application_contract_is_unchanged() -> None:
    assert sorted(SharedApplication.model_fields) == [
        "app_name",
        "application",
        "credential_slots",
        "database_dsn",
        "database_target",
        "dsn_encoding",
        "encryption_key",
        "grant_store",
        "installation",
        "namespace",
        "provider_origins",
        "proxy_image",
        "proxy_port",
        "proxy_resources",
        "release",
        "resolver_socket",
        "server_args",
        "server_env",
        "server_image",
        "server_port",
        "server_resources",
        "snapshot_seal",
    ]
    schema = json.dumps(
        SharedApplication.model_json_schema(), sort_keys=True, separators=(",", ":")
    )
    assert (
        hashlib.sha256(schema.encode()).hexdigest()
        == "6273043a5ca3e27bd9d0d584d839514e7e77031bad2acc91d3a2a16bb25ce351"
    )


def test_a_rotation_changes_only_the_keys_entry_and_never_member_reach() -> None:
    application = _application()
    before = render_application(application, caller_public_keys=(CURRENT,))
    after = render_application(application, caller_public_keys=(PREVIOUS, CURRENT))
    assert _env(_containers(before)["proxy"])[ENV] == CURRENT
    assert _env(_containers(after)["proxy"])[ENV] == f"{PREVIOUS},{CURRENT}"
    assert before != after
    assert _without_keys(before) == _without_keys(after)
    member = _member(application)
    reach = render_member_reach(application, member)
    assert reach == render_member_reach(application, member)
    # The rolled Pod keeps its labels, so the member's reach still selects it.
    ingress = next(item for item in reach if item["spec"]["policyTypes"] == ["Ingress"])
    for objects in (before, after):
        assert ingress["spec"]["podSelector"] == _deployment(objects)["spec"]["selector"]
    assert list(inspect.signature(render_member_reach).parameters) == ["application", "member"]


def test_the_current_and_previous_pair_renders_in_order() -> None:
    objects = render_application(_application(), caller_public_keys=CALLER_KEYS)
    value = _env(_containers(objects)["proxy"])[ENV]
    assert value.split(",") == [CURRENT, PREVIOUS]
    raw = [base64.b64decode(key, validate=True) for key in value.split(",")]
    assert [len(key) for key in raw] == [32, 32]
