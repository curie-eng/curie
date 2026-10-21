"""Closed authorizer seam for the shared protected launch mechanics."""

from __future__ import annotations

import inspect

import pytest
from curie_worker.protected_launch_kubernetes import LaunchControlUnavailable
from curie_worker.protected_launch_mechanics import LaunchAuthorizer, ProtectedLaunchMechanics
from curie_worker.protected_launch_server import ProductionLaunchAuthorizer, ProtectedLaunchServer


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


@pytest.mark.parametrize("authorizer", [LaunchAuthorizer(), object()])
def test_mechanics_refuse_an_authorizer_outside_the_closed_set(authorizer, tmp_path):
    with pytest.raises(LaunchControlUnavailable):
        ProtectedLaunchMechanics(
            authorizer,  # type: ignore[arg-type]
            kubernetes=object(),  # type: ignore[arg-type]
            registry_root=str(tmp_path),
            runner_scheme="http",
            runner_dns_suffix="svc.cluster.local",
            runner_ca_file=None,
        )


def test_mechanics_refuse_a_substitute_kubernetes_api_for_the_production_authorizer(tmp_path):
    with pytest.raises(LaunchControlUnavailable):
        ProtectedLaunchMechanics(
            ProductionLaunchAuthorizer(),
            kubernetes=object(),  # type: ignore[arg-type]
            registry_root=str(tmp_path),
            runner_scheme="http",
            runner_dns_suffix="svc.cluster.local",
            runner_ca_file=None,
        )
