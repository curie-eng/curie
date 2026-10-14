"""Primary protected dispatch never exposes ordinary capability methods."""

import asyncio

import pytest
from curie_worker.protected_dispatch import GuardedProtectedKernel, ProtectedDispatchUnavailable


@pytest.mark.parametrize(
    "method",
    [
        "release_thread",
        "reap_orphans",
        "resolve_capacity_grant",
        "notify_capacity_expired",
        "notify_capacity_queued",
    ],
)
def test_unsupported_kernel_method_refuses_before_any_component_access(method):
    facade = object.__new__(GuardedProtectedKernel)
    with pytest.raises(ProtectedDispatchUnavailable):
        asyncio.run(getattr(facade, method)("thread-example"))


def test_missing_original_scoped_delivery_never_calls_kernel():
    facade = object.__new__(GuardedProtectedKernel)
    with pytest.raises(ProtectedDispatchUnavailable):
        asyncio.run(facade.process_event(object(), lease=None))
