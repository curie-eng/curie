"""Test cache authorities. @spec IDENTITY-WRITER-LIFETIME-V1-10."""

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from curie_api import identity_fence
from curie_dispatcher import admission
from fastapi import FastAPI


def fence_key_for_worker(run_uid: str, worker_id: str) -> str:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    return f"curie:test:identity-fence:{run_uid}:{worker_id}"


def _install_key(key: str) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    identity_fence.FENCE_KEY = key
    admission.IDENTITY_FENCE_KEY = key


@contextmanager
def installed_fence_key(key: str) -> Iterator[None]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    previous = identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY
    try:
        _install_key(key)
        yield
    finally:
        identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY = previous


def fence_child_env(base_env: Mapping[str, str], key: str) -> dict[str, str]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    env = dict(base_env)
    env["CURIE_TEST_IDENTITY_FENCE_KEY"] = key
    path = str(Path(__file__).resolve().parent)
    if env.get("PYTHONPATH"):
        path += os.pathsep + env["PYTHONPATH"]
    env["PYTHONPATH"] = path
    return env


def create_namespaced_app() -> FastAPI:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    key = os.environ.get("CURIE_TEST_IDENTITY_FENCE_KEY")
    if not key:
        raise ValueError("test API child requires an identity fence key")
    from curie_api.main import create_app

    _install_key(key)
    return create_app()
