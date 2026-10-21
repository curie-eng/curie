"""Closed authorizer seam for the shared protected launch mechanics."""

from __future__ import annotations

import asyncio
import inspect

import pytest
from curie_worker.protected_launch_kubernetes import (
    KubernetesLaunchAPI,
    KubernetesTLS,
    LaunchControlUnavailable,
)
from curie_worker.protected_launch_mechanics import LaunchAuthorizer, ProtectedLaunchMechanics
from curie_worker.protected_launch_server import ProductionLaunchAuthorizer, ProtectedLaunchServer

from .protected_launch_endpoint_helpers import certificates


def _kubernetes(tmp_path):
    root = certificates(tmp_path / "tls")
    return KubernetesLaunchAPI(
        KubernetesTLS(
            "https://127.0.0.1:6443",
            str(root / "ca.pem"),
            str(root / "worker.pem"),
            str(root / "worker.key"),
            "synthetic-control",
        ),
        authority_domain_id="55555555-5555-4555-8555-555555555555",
    )


def _mechanics(authorizer, kubernetes, tmp_path):
    root = tmp_path / "registry"
    root.mkdir(mode=0o700, exist_ok=True)
    return ProtectedLaunchMechanics(
        authorizer,
        kubernetes=kubernetes,
        registry_root=str(root),
        runner_scheme="http",
        runner_dns_suffix="svc.cluster.local",
        runner_ca_file=None,
    )


def _forged_production_name():
    # A class body can claim any module and qualified name.
    class Forged(LaunchAuthorizer):
        __module__ = "curie_worker.protected_launch_server"
        __qualname__ = "ProductionLaunchAuthorizer"

    return Forged()


def test_production_server_cannot_be_given_an_authorizer():
    parameters = inspect.signature(ProtectedLaunchServer.__init__).parameters
    assert list(parameters) == [
        "self",
        "config",
        "runtimes",
        "workers",
        "credentials",
        "kubernetes",
    ]
    source = inspect.getsource(ProtectedLaunchServer.__init__)
    assert source.count("Authorizer(") == 1
    assert "self._authorizer = ProductionLaunchAuthorizer()" in source


def test_authorizer_set_is_closed():
    with pytest.raises(TypeError):

        class _Other(LaunchAuthorizer):
            pass

    with pytest.raises(TypeError):

        class _Derived(ProductionLaunchAuthorizer):  # type: ignore[misc]
            pass

    # @final is static only; a forged name must not admit a runtime subclass.
    with pytest.raises(TypeError):

        class _ForgedDerived(ProductionLaunchAuthorizer):  # type: ignore[misc]
            __module__ = "curie_worker.protected_launch_server"
            __qualname__ = "ProductionLaunchAuthorizer"


def test_mechanics_accept_the_production_authorizer(tmp_path):
    kubernetes = _kubernetes(tmp_path)
    mechanics = _mechanics(ProductionLaunchAuthorizer(), kubernetes, tmp_path)
    assert type(mechanics.authorizer) is ProductionLaunchAuthorizer
    asyncio.run(mechanics.close(None))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "authorizer",
    [LaunchAuthorizer, object, _forged_production_name],
    ids=["base", "foreign", "forged-name"],
)
def test_mechanics_refuse_an_authorizer_outside_the_closed_set(authorizer, tmp_path):
    kubernetes = _kubernetes(tmp_path)
    try:
        with pytest.raises(LaunchControlUnavailable):
            _mechanics(authorizer(), kubernetes, tmp_path)
    finally:
        asyncio.run(kubernetes.close())


def test_mechanics_refuse_a_substitute_kubernetes_api(tmp_path):
    with pytest.raises(LaunchControlUnavailable):
        _mechanics(ProductionLaunchAuthorizer(), object(), tmp_path)
