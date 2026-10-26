"""Owned real-process fence probes. @spec IDENTITY-WRITER-LIFETIME-V1-10."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import asyncpg
import redis
from curie_api.config import get_settings
from sqlalchemy.engine import make_url


def pytest_load_initial_conftests(early_config: Any, parser: Any, args: Any) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Forbid backend constructors in a pure child."""
    if os.environ.get("FENCE_ISOLATION_PURE_PROBE") != "1":
        return
    import pytest
    import redis.asyncio

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        raise AssertionError("pure fence fixture attempted backend construction or I/O")

    patch = pytest.MonkeyPatch()
    patch.setattr(redis.Redis, "__init__", forbidden)
    patch.setattr(redis.asyncio.Redis, "__init__", forbidden)
    patch.setattr(socket.socket, "connect", forbidden)
    patch.setattr(socket.socket, "connect_ex", forbidden)


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Observe teardown before another suite runs."""
    if os.environ.get("FENCE_ISOLATION_PURE_PROBE") == "1":
        from curie_api import identity_fence
        from curie_dispatcher import admission

        if (identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY) != (
            "curie:identity:admission-fence",
            "curie:identity:admission-fence",
        ):
            print("paired fence constants were not restored after the ordinary API fixture")
            session.exitstatus = 1


class Child:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Own the exact process and its pipe reader."""

    def __init__(self, database: str, key: str, value: str, directory: Path) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        env = dict(os.environ)
        env["DATABASE_URL"] = (
            make_url(get_settings().database_url)
            .set(database=database)
            .render_as_string(hide_password=False)
        )
        env["CURIE_TEST_IDENTITY_FENCE_KEY"] = key
        env["FENCE_PROBE_VALUE"] = value
        self.stderr = (directory / f"{value}.stderr").open("w")
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(Path(__file__).resolve())],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
        )
        self.messages: queue.Queue[Any] = queue.Queue()
        self.history: list[dict[str, Any]] = []
        self.reader = threading.Thread(target=self.read, daemon=False)
        self.reader.start()

    def read(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Preserve actual event ordering."""
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.messages.put(json.loads(line))
        self.messages.put(None)

    def wait(self, event: str) -> dict[str, Any]:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. A bounded IPC barrier, not timing luck."""
        deadline = time.monotonic() + 12
        while True:
            message = self.messages.get(timeout=max(0.01, deadline - time.monotonic()))
            assert message is not None, f"owned child exited before {event}"
            self.history.append(message)
            if message["event"] == event:
                return message

    def send(self, command: str) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        assert self.process.stdin is not None
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def join(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. EOF unwinds only this owned client process."""
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=12)
        self.reader.join(timeout=2)
        assert not self.reader.is_alive()
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.stderr.close()


class Probe:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Register exact resources before creation."""

    def __init__(self, directory: Path, keys: list[str], databases: int = 2) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        self.directory, self.keys = directory, list(dict.fromkeys(keys))
        self.databases = [f"fence_probe_{uuid.uuid4().hex}" for _ in range(databases)]
        self.created: list[str] = []
        self.children: list[Child] = []
        self.finalizers: list[Any] = []
        self.record: dict[str, Any] = {
            "databases": self.databases,
            "keys": self.keys,
            "processes": [],
            "cleaned": False,
        }
        self.save()

    def save(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. The manifest contains no connection secrets."""
        (self.directory / "ownership.json").write_text(json.dumps(self.record, indent=2) + "\n")

    async def sql(self, statement: str, *, database: str = "postgres") -> Any:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Connect through the authoritative test setting."""
        url = make_url(get_settings().database_url)
        connection = await asyncpg.connect(
            user=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=database,
        )
        try:
            return await connection.fetch(statement)
        finally:
            await connection.close()

    def start(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. No migrations or aged-database sweep."""
        for database in self.databases:
            asyncio.run(self.sql(f'CREATE DATABASE "{database}"'))
            self.created.append(database)
            asyncio.run(
                self.sql("CREATE TABLE fence_probe(value text PRIMARY KEY)", database=database)
            )

    def child(self, database: str, key: str, value: str) -> Child:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        child = Child(database, key, value, self.directory)
        self.children.append(child)
        self.record["processes"].append(child.process.pid)
        self.save()
        return child

    def rows(self, database: str) -> list[str]:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Observe durable writes independently."""
        return [
            row["value"]
            for row in asyncio.run(
                self.sql("SELECT value FROM fence_probe ORDER BY value", database=database)
            )
        ]

    def close(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Join every consumer before exact cleanup."""
        for child in self.children:
            child.join()
        for finalizer in reversed(self.finalizers):
            finalizer()
        self.record["events"] = [child.history for child in self.children]
        assert all(child.process.poll() is not None for child in self.children)
        client = redis.Redis.from_url(get_settings().valkey_dsn())
        try:
            for key in self.keys:
                client.delete(key)
                assert client.get(key) is None
        finally:
            client.close()
        for database in self.created:
            # No FORCE: all clients must have closed normally before database disposal.
            asyncio.run(self.sql(f'DROP DATABASE "{database}"'))
        self.record["cleaned"] = True
        self.save()


@contextmanager
def owned_probe(directory: Path, keys: list[str], databases: int = 2) -> Any:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Cleanup is installed before resources start."""
    probe = Probe(directory, keys, databases)
    try:
        probe.start()
        yield probe
    finally:
        probe.close()


async def child_main() -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Exercise actual mutation/ACK/SQL in an OS child."""
    import identity_fence_support
    import redis.asyncio as redis_async
    from curie_api import identity_fence
    from sqlalchemy import event, text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    key = os.environ["CURIE_TEST_IDENTITY_FENCE_KEY"]
    value = os.environ["FENCE_PROBE_VALUE"]

    def emit(event_name: str, **fields: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        print(json.dumps({"event": event_name, **fields}), flush=True)

    async def permit(expected: str) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Parent releases each exact stage."""
        assert (await asyncio.to_thread(sys.stdin.readline)).strip() == expected

    @contextmanager
    def installation() -> Any:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Old shared topology has an explicit owned key."""
        helper = getattr(identity_fence_support, "installed_fence_key", None)
        if callable(helper):
            with helper(key):
                yield
        else:
            previous = identity_fence.FENCE_KEY
            identity_fence.FENCE_KEY = key
            try:
                yield
            finally:
                identity_fence.FENCE_KEY = previous

    engine = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
    client = redis_async.Redis.from_url(get_settings().valkey_dsn())

    def locking(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Observe the actual lock-wait backend."""
        if "pg_advisory_lock(" in statement:
            emit("locking", backend_pid=connection.connection.driver_connection.get_server_pid())

    class BarrierClient:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Delegate all real datastore operations."""

        async def set(self, *args: Any, **kwargs: Any) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-10. Barrier follows the actual SET ACK."""
            acknowledgement = await client.set(*args, **kwargs)
            assert acknowledgement is True
            emit("pending_ack")
            await permit("enter")
            return acknowledgement

        def __getattr__(self, name: str) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
            return getattr(client, name)

    event.listen(engine.sync_engine, "before_cursor_execute", locking)
    try:
        with installation():
            try:
                async with identity_fence.mutation(
                    async_sessionmaker(engine), BarrierClient()
                ) as session:
                    await session.execute(
                        text("INSERT INTO fence_probe(value) VALUES (:value)"), {"value": value}
                    )
                    await session.commit()
                    emit("committed")
                    await permit("publish")
                outcome = "published"
            except identity_fence.IdentityWriterUnavailable:
                outcome = "refused"
            emit("finished", outcome=outcome)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", locking)
        await client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(child_main())
