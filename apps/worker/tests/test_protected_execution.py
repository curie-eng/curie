"""Protected ownership refuses missing or unfenced context before broker access."""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
from typing import Any

import pytest


def api() -> Any:
    name = "curie_worker.protected_execution"
    assert importlib.util.find_spec(name) is not None, "protected execution fence missing"
    return importlib.import_module(name)


def test_missing_execution_scope_refuses_without_any_broker() -> None:
    async def go() -> None:
        module = api()
        with pytest.raises(module.ExecutionOwnershipUnavailable) as caught:
            await module.require_current_execution(None, None)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None

    asyncio.run(go())


def test_invalid_scope_never_exposes_a_context() -> None:
    async def go() -> None:
        module = api()
        with pytest.raises(module.ExecutionOwnershipUnavailable):
            async with module.protected_execution_scope(
                None, trusted_manifest=None, config=None, lease=None
            ):
                pytest.fail("invalid ownership scope entered")
        with pytest.raises(module.ExecutionOwnershipUnavailable):
            await module.require_current_execution(None, None)

    asyncio.run(go())


def test_missing_scope_error_detaches_ambient_sensitive_exception() -> None:
    async def go() -> None:
        module = api()
        try:
            raise ValueError("SYNTHETIC-LEASE-OWNER-MARKER")
        except ValueError:
            with pytest.raises(module.ExecutionOwnershipUnavailable) as caught:
                await module.require_current_execution(None, None)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None
        assert "SYNTHETIC-LEASE-OWNER-MARKER" not in str(caught.value)

    asyncio.run(go())
