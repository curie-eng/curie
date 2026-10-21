"""Separate cache authority for API test processes with independent databases."""

from collections.abc import Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
import redis
from curie_api import identity_fence
from curie_api.config import get_settings


@pytest.fixture(autouse=True)
def identity_fence_backend(monkeypatch: pytest.MonkeyPatch, worker_id: str) -> Iterator[None]:
    # Each xdist process owns a different Postgres database. Its fixed fence
    # must likewise belong to a different real Valkey logical database, rather
    # than racing unrelated API mutations on another authority database.
    # The disposable Compose Valkey uses its default sixteen databases.
    database = 15 if worker_id == "master" else int(worker_id.removeprefix("gw")) + 1
    if not 1 <= database <= 15:
        raise RuntimeError("identity fence verification needs one Valkey database per worker")
    parts = urlsplit(get_settings().valkey_dsn())
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query) if key != "db"])
    endpoint = urlunsplit(parts._replace(path=f"/{database}", query=query))
    try:
        with monkeypatch.context() as scoped:
            scoped.setenv("VALKEY_URL", endpoint)
            get_settings.cache_clear()
            client = redis.Redis.from_url(endpoint)
            try:
                client.ping()
                yield
            finally:
                client.delete(identity_fence.FENCE_KEY)
                client.close()
    finally:
        get_settings.cache_clear()
