"""Independent scoped snapshots, without provisioning or runtime permission proof.

@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-2
@spec HTTP-CREDENTIAL-REGISTRY-V1-3 HTTP-CREDENTIAL-REGISTRY-V1-4
@spec HTTP-CREDENTIAL-REGISTRY-V1-5 HTTP-CREDENTIAL-REGISTRY-V1-6
"""

from __future__ import annotations

import copy
import gc
import importlib
import json
import pickle
import subprocess
import sys
import weakref
from collections.abc import Callable
from functools import partial
from types import ModuleType
from typing import Any

import pytest

TENANT = "11111111-1111-1111-1111-111111111111"
OTHER_TENANT = "22222222-2222-2222-2222-222222222222"
FIRST = "SyntheticRegistryBearerAlpha+/=="
SECOND = "SyntheticRegistryBearerBeta._~=="
INVALID = "invalid_provider_credential_registry"
UNAVAILABLE = "provider_credential_unavailable"


def api() -> ModuleType:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: absence fails inside test execution."""
    name = "curie_api.identity.http_credential_registry"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "scoped provider credential registry is not implemented"
        raise
    for symbol in (
        "build_provider_credential_registry",
        "ProviderCredentialRegistry",
        "ResolvedProviderCredential",
        "CredentialRegistryRefusal",
    ):
        assert hasattr(module, symbol), f"registry entry point missing: {symbol}"
    return module


def authority_api() -> ModuleType:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: existing owner, no alternate grammar."""
    return importlib.import_module("curie_api.schemas.http_authority")


def transport_api() -> ModuleType:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-3: reuse actual transport value types."""
    return importlib.import_module("curie_api.identity.http_transport")


def payload() -> dict[str, Any]:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: independently authored synthetic configuration."""
    return {
        "schema_version": 1,
        "tenant_id": TENANT,
        "version": 7,
        "selection": {
            "mode": "http",
            "issuer": "https://Identity.example.test/Exact%2fIssuer",
            "lookup_url": "https://Lookup.example.test:443/Exact%2fOperation",
            "credential_ref": {"id": "ExampleCredential", "generation": 3},
            "max_identity_age_ms": 101,
            "max_membership_age_ms": 202,
            "max_clock_skew_ms": 0,
        },
    }


def changed(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-2: change one synthetic input component."""
    result = copy.deepcopy(value)
    node = result
    keys = path.split(".")
    for key in keys[:-1]:
        node = node[key]
    node[keys[-1]] = replacement
    return result


def configuration(**changes: Any) -> Any:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: every ordinary configuration uses its owner."""
    value = payload()
    for path, replacement in changes.items():
        value = changed(value, path, replacement)
    result = authority_api().validate_authority_config_v1(value)
    assert isinstance(result, authority_api().AuthorityConfigV1), result
    return result


def bearer(value: str = FIRST) -> Any:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: existing syntactic wrapper, no entropy claim."""
    return transport_api().ProviderBearer(value)


def registry(registrations: Any) -> Any:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1."""
    result = api().build_provider_credential_registry(registrations)
    assert isinstance(result, api().ProviderCredentialRegistry), result
    return result


def resolved(value: Any, expected_bearer: str = FIRST, *, version: int = 7) -> Any:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-3: assert real transport inputs, never allow."""
    assert isinstance(value, api().ResolvedProviderCredential), value
    assert isinstance(value.target, transport_api().LookupTarget)
    facts = importlib.import_module("curie_api.schemas.http_identity")
    assert isinstance(value.limits, facts.FreshnessLimits)
    assert isinstance(value.bearer, transport_api().ProviderBearer)
    assert value.target.tenant_id == TENANT
    assert value.target.expected_issuer == "https://Identity.example.test/Exact%2fIssuer"
    assert value.target.endpoint == "https://Lookup.example.test:443/Exact%2fOperation"
    assert value.target.authority_version == version
    assert value.bearer.value == expected_bearer
    assert not hasattr(value, "allowed") and not hasattr(value, "permission")
    with pytest.raises(TypeError, match="^provider_credential_is_not_permission$"):
        bool(value)
    return value


def refusal(value: Any, code: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4: code-only failure with no input retention."""
    assert isinstance(value, api().CredentialRegistryRefusal), value
    assert value.model_dump(mode="json") == {"code": code}
    assert getattr(value, "__cause__", None) is None
    assert getattr(value, "__context__", None) is None
    for rendered in (str(value), repr(value), value.model_dump_json()):
        for sensitive in (FIRST, SECOND, TENANT, "ExampleCredential", "example.test"):
            assert sensitive not in rendered


class TupleSubclass(tuple):
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: distinguish exact tuple from isinstance."""


@pytest.mark.parametrize("value", [FIRST, SECOND, "a", "x/y+z._~=="])
def test_existing_typed_fixture_controls(value: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: owner fixtures pass without new behavior."""
    config = configuration()
    assert config.model_dump() == payload()
    token = bearer(value)
    assert token.value == value and repr(token) == "ProviderBearer()"
    assert not hasattr(token, "__dict__")


def test_empty_single_and_multiple_explicit_registrations() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    config = configuration()
    empty = registry(())
    refusal(empty.resolve(config), UNAVAILABLE)
    single = registry(((config, bearer()),))
    value = resolved(single.resolve(config))
    assert value.limits.model_dump() == {
        "max_identity_age_ms": 101,
        "max_membership_age_ms": 202,
        "max_clock_skew_ms": 0,
    }
    other = configuration(**{"selection.credential_ref.id": "OtherCredential"})
    multiple = registry(((config, bearer()), (other, bearer(SECOND))))
    resolved(multiple.resolve(config))
    resolved(multiple.resolve(other), SECOND)


@pytest.mark.parametrize("size", [0, 256, 257])
def test_registration_count_boundary(size: int) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-6."""
    registrations = tuple(
        (configuration(**{"selection.credential_ref.id": f"ExampleCredential{i}"}), bearer())
        for i in range(size)
    )
    result = api().build_provider_credential_registry(registrations)
    if size == 257:
        refusal(result, INVALID)
    else:
        assert isinstance(result, api().ProviderCredentialRegistry)
        if size:
            resolved(result.resolve(registrations[-1][0]))
        else:
            refusal(result.resolve(configuration()), UNAVAILABLE)


@pytest.mark.parametrize("shape", ["list", "generator", "mapping", "subclass", "none", "text"])
def test_outer_container_is_an_exact_tuple(shape: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: no implicit loading or iteration contract."""
    entry = (configuration(), bearer())
    candidates = {
        "list": [entry],
        "generator": (item for item in (entry,)),
        "mapping": {"ExampleCredential": entry},
        "subclass": TupleSubclass((entry,)),
        "none": None,
        "text": FIRST,
    }
    refusal(api().build_provider_credential_registry(candidates[shape]), INVALID)


@pytest.mark.parametrize("shape", ["list", "subclass", "short", "long", "empty", "mapping"])
def test_each_registration_is_an_exact_two_tuple(shape: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: valid siblings never make partial construction."""
    config, token = configuration(), bearer()
    candidates = {
        "list": [config, token],
        "subclass": TupleSubclass((config, token)),
        "short": (config,),
        "long": (config, token, token),
        "empty": (),
        "mapping": {"configuration": config, "bearer": token},
    }
    refusal(api().build_provider_credential_registry(((config, token), candidates[shape])), INVALID)


@pytest.mark.parametrize("side", ["configuration", "bearer"])
@pytest.mark.parametrize("value", [None, FIRST, b"SyntheticBytes", {}, True])
def test_registration_requires_existing_typed_values(side: str, value: Any) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1: plaintext and decoded mappings are not accepted."""
    pair = (value, bearer()) if side == "configuration" else (configuration(), value)
    refusal(api().build_provider_credential_registry((pair,)), INVALID)


@pytest.mark.parametrize("mode", ["disabled", "local_projection"])
def test_non_http_selection_is_refused_for_build_and_resolution(mode: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    selection = {"mode": mode}
    if mode == "local_projection":
        selection.update(
            issuer="https://Identity.example.test/Exact%2fIssuer",
            governed_domains=["example.test"],
            managed_by="ExampleDirectory",
        )
    non_http = configuration(selection=selection)
    refusal(api().build_provider_credential_registry(((non_http, bearer()),)), INVALID)
    current = configuration()
    refusal(registry(((current, bearer()),)).resolve(non_http), UNAVAILABLE)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("tenant_id", OTHER_TENANT),
        ("selection.issuer", "https://Identity.example.test/Exact%2fIssuer/"),
        ("selection.issuer", "https://identity.example.test/Exact%2fIssuer"),
        ("selection.lookup_url", "https://Lookup.example.test/Exact%2fOperation"),
        ("selection.lookup_url", "https://lookup.example.test:443/Exact%2fOperation"),
        ("selection.lookup_url", "https://Lookup.example.test:443/Exact%2FOperation"),
        ("selection.lookup_url", "https://Lookup.example.test:443/Exact%2fOperation/"),
        ("selection.credential_ref.id", "exampleCredential"),
        ("selection.credential_ref.id", "ExampleCredential_more"),
        ("selection.credential_ref.generation", 4),
    ],
)
def test_each_scope_component_requires_exact_registered_binding(path: str, value: Any) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-2 HTTP-CREDENTIAL-REGISTRY-V1-3: never normalize scope."""
    config = configuration()
    result = registry(((config, bearer()),))
    resolved(result.resolve(config))
    refusal(result.resolve(configuration(**{path: value})), UNAVAILABLE)
    resolved(result.resolve(config))


@pytest.mark.parametrize(
    "conflict", ["identical", "material", "tenant", "issuer", "url", "version"]
)
def test_every_repeated_reference_generation_refuses_even_with_conflicting_scope(
    conflict: str,
) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-2: duplicates cannot shadow or choose first/last."""
    config = configuration()
    changes = {
        "identical": {},
        "material": {},
        "tenant": {"tenant_id": OTHER_TENANT},
        "issuer": {"selection.issuer": "https://Identity.example.test/other"},
        "url": {"selection.lookup_url": "https://Lookup.example.test/other"},
        "version": {"version": 8},
    }
    duplicate = configuration(**changes[conflict])
    second = bearer(SECOND if conflict == "material" else FIRST)
    for entries in (
        ((config, bearer()), (duplicate, second)),
        ((duplicate, second), (config, bearer())),
    ):
        refusal(api().build_provider_credential_registry(entries), INVALID)


def test_distinct_generations_coexist_without_latest_or_previous_fallback() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-2 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    old = configuration()
    new = configuration(**{"selection.credential_ref.generation": 4})
    result = registry(((old, bearer()), (new, bearer(SECOND))))
    resolved(result.resolve(old))
    resolved(result.resolve(new), SECOND)
    refusal(
        result.resolve(configuration(**{"selection.credential_ref.generation": 5})), UNAVAILABLE
    )
    replacement = registry(((new, bearer(SECOND)),))
    refusal(replacement.resolve(old), UNAVAILABLE)
    resolved(result.resolve(old))  # A new registry does not retroactively revoke the old one.


def test_current_version_and_each_freshness_value_derive_new_detached_inputs() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-2 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    config = configuration()
    result = registry(((config, bearer()),))
    first = resolved(result.resolve(config))
    updated = configuration(
        **{
            "version": 8,
            "selection.max_identity_age_ms": 303,
            "selection.max_membership_age_ms": 404,
            "selection.max_clock_skew_ms": 5,
        }
    )
    second = resolved(result.resolve(updated), version=8)
    assert second is not first and second.target is not first.target
    assert second.limits is not first.limits
    assert second.limits.model_dump() == {
        "max_identity_age_ms": 303,
        "max_membership_age_ms": 404,
        "max_clock_skew_ms": 5,
    }
    assert first.target.authority_version == 7
    assert first.limits.model_dump() == {
        "max_identity_age_ms": 101,
        "max_membership_age_ms": 202,
        "max_clock_skew_ms": 0,
    }
    object.__setattr__(updated.selection, "max_identity_age_ms", 999)
    assert second.limits.max_identity_age_ms == 303
    object.__setattr__(first.target, "endpoint", "https://other.example.test/altered")
    object.__setattr__(first.limits, "max_identity_age_ms", 999)
    fresh = resolved(result.resolve(config))
    assert fresh.limits.max_identity_age_ms == 101


def test_initializer_configuration_and_bearer_are_snapshots_not_aliases() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    config, token = configuration(), bearer()
    result = registry(((config, token),))
    initial = resolved(result.resolve(configuration()))
    assert initial.bearer is not token
    object.__setattr__(token, "_ProviderBearer__value", SECOND)
    object.__setattr__(config.selection.credential_ref, "generation", 4)
    object.__setattr__(config.selection, "lookup_url", "https://other.example.test/changed")
    resolved(result.resolve(configuration()))
    refusal(result.resolve(config), UNAVAILABLE)
    assert initial.bearer.value == FIRST
    assert initial.target.endpoint == "https://Lookup.example.test:443/Exact%2fOperation"


def test_registry_does_not_retain_initializer_registration_or_configuration() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3."""
    config = configuration()
    original = weakref.ref(config)
    registrations = ((config, bearer()),)
    result = registry(registrations)
    del registrations, config
    gc.collect()
    assert original() is None
    current = configuration()
    current_ref = weakref.ref(current)
    handle = result.resolve(current)
    del current
    gc.collect()
    assert current_ref() is None
    resolved(handle)


@pytest.mark.parametrize("value", [None, {}, b"{}", FIRST, True])
def test_resolution_accepts_no_untyped_configuration(value: Any) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-3."""
    config = configuration()
    result = registry(((config, bearer()),))
    refusal(result.resolve(value), UNAVAILABLE)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("schema_version", True),
        ("version", 0),
        ("tenant_id", "invalid"),
        ("selection", None),
        ("selection.credential_ref", None),
        ("selection.credential_ref.id", "bad/ref"),
        ("selection.credential_ref.generation", None),
        ("selection.credential_ref.generation", 0),
        ("selection.credential_ref.generation", True),
        ("selection.lookup_url", "https://user@lookup.example.test/private"),
        ("selection.max_identity_age_ms", 0),
        ("selection.max_membership_age_ms", None),
        ("selection.max_clock_skew_ms", -1),
    ],
)
def test_nested_mutations_are_revalidated_at_build_and_resolution(path: str, value: Any) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3: no stale model trust."""
    original = configuration()
    result = registry(((original, bearer()),))
    malformed = configuration()
    node = malformed
    keys = path.split(".")
    for key in keys[:-1]:
        node = getattr(node, key)
    object.__setattr__(node, keys[-1], value)
    refusal(api().build_provider_credential_registry(((malformed, bearer()),)), INVALID)
    refusal(result.resolve(malformed), UNAVAILABLE)
    resolved(result.resolve(original))


def test_constructed_missing_or_invalid_values_never_escape_as_raw_errors() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 HTTP-CREDENTIAL-REGISTRY-V1-3.

    @spec HTTP-CREDENTIAL-REGISTRY-V1-4.
    """
    config = configuration()
    result = registry(((config, bearer()),))
    missing = authority_api().AuthorityConfigV1.model_construct()
    refusal(api().build_provider_credential_registry(((missing, bearer()),)), INVALID)
    refusal(result.resolve(missing), UNAVAILABLE)
    for value in (None, "bad token with spaces", FIRST + "\n"):
        malformed = object.__new__(transport_api().ProviderBearer)
        if value is not None:
            object.__setattr__(malformed, "_ProviderBearer__value", value)
        refusal(api().build_provider_credential_registry(((config, malformed),)), INVALID)
    untyped = authority_api().AuthorityConfigV1.model_construct(**dict(payload(), version=True))
    refusal(api().build_provider_credential_registry(((untyped, bearer()),)), INVALID)
    refusal(result.resolve(untyped), UNAVAILABLE)
    refusal(api().build_provider_credential_registry(((payload(), bearer()),)), INVALID)


def failing(operation: Callable[[], Any]) -> tuple[type[BaseException], str]:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4: capture only fixed errors for comparison."""
    with pytest.raises((TypeError, AttributeError, ValueError)) as caught:
        operation()
    error = caught.value
    assert error.__cause__ is None and error.__context__ is None
    text = str(error)
    assert text and len(text) < 200
    for sensitive in (FIRST, SECOND, TENANT, "ExampleCredential", "example.test", "SyntheticField"):
        assert sensitive not in text
    return type(error), text


def handles(token: str) -> tuple[Any, Any]:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
    result = registry(((configuration(), bearer(token)),))
    return result, result.resolve(configuration())


def test_handles_have_fixed_opaque_formatting_and_identity_equality_hash() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4: equal bindings must not reveal value equality."""
    first, first_resolved = handles(FIRST)
    second, second_resolved = handles(FIRST)
    for left, right, text in (
        (first, second, "ProviderCredentialRegistry()"),
        (first_resolved, second_resolved, "ResolvedProviderCredential()"),
    ):
        assert str(left) == repr(left) == str(right) == repr(right) == text
        assert left == left and left != right and left != FIRST
        assert hash(left) == object.__hash__(left)
        assert len({left, right}) == 2
        assert not hasattr(left, "__dict__")
        failing(partial(vars, left))
        failing(partial(iter, left))
        failing(partial(dict, left))
        failing(partial(json.dumps, left))
        assert not hasattr(left, "model_dump") and not hasattr(left, "model_dump_json")
    with pytest.raises(TypeError, match="^provider_credential_is_not_permission$"):
        bool(first_resolved)


def test_handles_expose_only_explicit_read_only_transport_properties() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-3 HTTP-CREDENTIAL-REGISTRY-V1-4."""
    original_registry, original = handles(FIRST)
    other_registry, other = handles(SECOND)
    resolved(original)
    resolved(other, SECOND)
    for first, second in ((original_registry, other_registry), (original, other)):
        for name in ("SyntheticField", "target", "limits", "bearer"):
            assert failing(partial(setattr, first, name, FIRST)) == failing(
                partial(setattr, second, name, SECOND)
            )
            assert failing(partial(delattr, first, name)) == failing(partial(delattr, second, name))
    resolved(original)
    assert not any(
        hasattr(original_registry, name) for name in ("revoke", "reload", "activate", "allowed")
    )


@pytest.mark.parametrize("kind", ["ProviderCredentialRegistry", "ResolvedProviderCredential"])
def test_handles_reject_public_constructors_and_state_serialization(kind: str) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4: no accidental copied secret graph."""
    cls = getattr(api(), kind)
    failing(lambda: cls())
    assert failing(lambda: cls(FIRST)) == failing(lambda: cls(SECOND))
    first = handles(FIRST)[kind == "ResolvedProviderCredential"]
    second = handles(SECOND)[kind == "ResolvedProviderCredential"]
    for method, args in (
        ("__getstate__", ()),
        ("__reduce__", ()),
        ("__reduce_ex__", (0,)),
        ("__reduce_ex__", (pickle.HIGHEST_PROTOCOL,)),
    ):
        error = failing(partial(getattr(first, method), *args))
        assert error[0] is TypeError
        assert error == failing(partial(getattr(second, method), *args))
    for protocol in (0, pickle.HIGHEST_PROTOCOL):
        error = failing(partial(pickle.dumps, first, protocol=protocol))
        assert error[0] is TypeError
        assert error == failing(partial(pickle.dumps, second, protocol=protocol))


def test_refusals_are_frozen_code_only_and_invalid_inputs_emit_no_logs(caplog: Any) -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4 HTTP-CREDENTIAL-REGISTRY-V1-6."""
    module = api()
    for code in (INVALID, UNAVAILABLE):
        value = module.CredentialRegistryRefusal(code=code)
        refusal(value, code)
        with pytest.raises(ValueError):
            value.code = INVALID
        with pytest.raises(ValueError):
            module.CredentialRegistryRefusal(code=code, detail=FIRST)
    with pytest.raises(ValueError):
        module.CredentialRegistryRefusal(code=FIRST)
    config = configuration()
    result = registry(((config, bearer()),))
    caplog.clear()
    refusal(module.build_provider_credential_registry(((config, FIRST),)), INVALID)
    refusal(result.resolve(payload()), UNAVAILABLE)
    assert caplog.records == []


def test_fresh_process_import_build_and_resolve_need_no_runtime_or_external_io() -> None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-5 HTTP-CREDENTIAL-REGISTRY-V1-6: executable isolation."""
    api()
    script = r"""
import builtins
import datetime as clock_module
import importlib.abc
import io
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
forbidden = ("curie_api.main", "curie_api.app", "curie_api.config", "curie_api.db",
             "curie_api.models", "curie_api.routers", "curie_api.identity.service",
             "curie_api.identity.connect", "curie_api.identity.attach", "sqlalchemy",
             "asyncpg", "redis", "boto3", "kubernetes", "curie_worker")
class RuntimeForbidden(importlib.abc.MetaPathFinder):
    # @spec HTTP-CREDENTIAL-REGISTRY-V1-5
    def find_spec(self, fullname, path=None, target=None):
        # @spec HTTP-CREDENTIAL-REGISTRY-V1-5
        if any(fullname == p or fullname.startswith(p + ".") for p in forbidden):
            raise AssertionError("registry imported runtime: " + fullname)
sys.meta_path.insert(0, RuntimeForbidden())
# These existing owners may load ordinary Python dependencies and a passive lock.
from curie_api.schemas.http_authority import AuthorityConfigV1
from curie_api.identity.http_transport import ProviderBearer
configuration = AuthorityConfigV1.model_validate(json.loads(sys.argv[1]))
token = ProviderBearer(sys.argv[2])
def no_io(*args, **kwargs):
    # @spec HTTP-CREDENTIAL-REGISTRY-V1-5
    raise AssertionError("registry attempted discovery, external IO or worker activity")
class NoEnvironment(Mapping):
    # @spec HTTP-CREDENTIAL-REGISTRY-V1-5
    __getitem__ = __iter__ = __len__ = no_io
class NoClock(clock_module.datetime):
    # @spec HTTP-CREDENTIAL-REGISTRY-V1-5
    now = utcnow = today = staticmethod(no_io)
clock_module.datetime = NoClock
os.getenv = no_io
os.environ = NoEnvironment()
threading.Thread.start = no_io
threading.Lock = threading.RLock = no_io
socket.socket = socket.create_connection = socket.getaddrinfo = no_io
subprocess.Popen = os.system = no_io
time.time = time.monotonic = no_io
def audit(event, args):
    # @spec HTTP-CREDENTIAL-REGISTRY-V1-5: permit ordinary module loading only.
    if event == "open":
        path = args[0]
        if not isinstance(path, str) or not path.endswith((".py", ".pyc")):
            no_io()
sys.addaudithook(audit)
from curie_api.identity.http_credential_registry import build_provider_credential_registry
builtins.open = io.open = os.open = no_io
result = build_provider_credential_registry(((configuration, token),))
for _ in range(2):
    handle = result.resolve(configuration)
    assert handle.bearer.value == sys.argv[2]
    assert handle.target.authority_version == 7
    assert handle.limits.max_identity_age_ms == 101
    assert repr(result) == "ProviderCredentialRegistry()"
    assert repr(handle) == "ResolvedProviderCredential()"
assert not any(n == p or n.startswith(p + ".") for n in sys.modules for p in forbidden)
print("pure registry import and repeated resolution passed")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(payload()), FIRST],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "pure registry import and repeated resolution passed"
