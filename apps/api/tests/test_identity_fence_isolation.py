"""Independent fence-authority isolation. @spec IDENTITY-WRITER-LIFETIME-V1-10."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_dispatcher import admission

DEFAULT = "curie:identity:admission-fence"


def helper(name: str) -> Any:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Missing helpers fail only inside bodies."""
    result = getattr(importlib.import_module("identity_fence_support"), name, None)
    assert callable(result), f"test authority helper missing: {name}"
    return result


def import_env() -> dict[str, str]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Give fresh children only this test import path."""
    env = dict(os.environ)
    path = str(Path(__file__).resolve().parent)
    if env.get("PYTHONPATH"):
        path += os.pathsep + env["PYTHONPATH"]
    env["PYTHONPATH"] = path
    return env


def test_ordinary_module_receives_paired_worker_authority(
    testrun_uid: str,
    worker_id: str,
    tmp_path: Path,
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. No opt-in fixture import or helper lookup."""
    expected = f"curie:test:identity-fence:{testrun_uid}:{worker_id}"
    (tmp_path / "authority.json").write_text(
        json.dumps(
            {
                "testrun_uid": testrun_uid,
                "worker_id": worker_id,
                "key": expected,
            }
        )
        + "\n"
    )
    assert (identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY) == (expected, expected)


def test_ordinary_fixture_discovery_needs_no_backend(testrun_uid: str, tmp_path: Path) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Actual ordinary-module discovery in a fresh pytest."""
    env = import_env()
    env["FENCE_ISOLATION_PURE_PROBE"] = "1"
    selector = (
        str(Path(__file__).resolve()) + "::test_ordinary_module_receives_paired_worker_authority"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-n0",
            "--testrunuid",
            testrun_uid,
            "-p",
            "identity_fence_isolation_process",
            "--basetemp",
            str(tmp_path / "ordinary-child"),
            selector,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_namespace_installation_is_pure_nested_and_restores_exact_prior_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Restore paired reader/writer state on every exit."""
    namespace, install = helper("fence_key_for_worker"), helper("installed_fence_key")
    assert namespace("synthetic-run", "gw0") == "curie:test:identity-fence:synthetic-run:gw0"
    assert namespace("synthetic-run", "gw0") == namespace("synthetic-run", "gw0")
    assert (
        len(
            {
                namespace("synthetic-run", "gw0"),
                namespace("synthetic-run", "gw1"),
                namespace("other-run", "gw0"),
            }
        )
        == 3
    )
    previous = identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY
    environment = dict(os.environ)
    settings = get_settings()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        pytest.fail("namespace installation attempted backend I/O")

    with monkeypatch.context() as patch:
        patch.setattr(redis.Redis, "__init__", forbidden)
        patch.setattr(socket.socket, "connect", forbidden)
        patch.setattr(get_settings, "cache_clear", forbidden)
        with install("curie:test:outer"):
            assert (identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY) == (
                "curie:test:outer",
                "curie:test:outer",
            )
            with pytest.raises(LookupError):
                with install("curie:test:inner"):
                    assert (
                        identity_fence.FENCE_KEY
                        == admission.IDENTITY_FENCE_KEY
                        == "curie:test:inner"
                    )
                    raise LookupError("synthetic")
            assert identity_fence.FENCE_KEY == admission.IDENTITY_FENCE_KEY == "curie:test:outer"
    assert (identity_fence.FENCE_KEY, admission.IDENTITY_FENCE_KEY) == previous
    assert dict(os.environ) == environment and get_settings() is settings


def test_copied_child_environment_and_actual_factory_agree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Actual create_app and child imports share the key."""
    overlay = helper("fence_child_env")
    monkeypatch.setenv("FENCE_PROBE_UNRELATED_AMBIENT", "must-not-copy")
    assert set(overlay({}, "curie:test:synthetic-child")) == {
        "CURIE_TEST_IDENTITY_FENCE_KEY",
        "PYTHONPATH",
    }
    original = {"PYTHONPATH": "synthetic-existing-path", "UNRELATED": "retained"}
    snapshot = dict(original)
    copied = overlay(original, "curie:test:synthetic-child")
    assert original == snapshot and copied is not original
    assert copied["UNRELATED"] == "retained"
    assert copied["CURIE_TEST_IDENTITY_FENCE_KEY"] == "curie:test:synthetic-child"
    assert copied["PYTHONPATH"].split(os.pathsep) == [
        str(Path(__file__).resolve().parent),
        "synthetic-existing-path",
    ]
    env = overlay(import_env(), identity_fence.FENCE_KEY)
    code = (
        "import json,identity_fence_support as h;from curie_api import identity_fence as w;"
        "from curie_dispatcher import admission as r;from fastapi import FastAPI;"
        "from curie_api.main import create_app;baseline=create_app();"
        "app=h.create_namespaced_app();"
        "print(json.dumps([w.FENCE_KEY,r.IDENTITY_FENCE_KEY,"
        "isinstance(app,FastAPI) and app.openapi()==baseline.openapi()]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert json.loads(result.stdout) == [
        identity_fence.FENCE_KEY,
        admission.IDENTITY_FENCE_KEY,
        True,
    ]


def test_fresh_unmodified_imports_and_missing_child_key_preserve_wire_defaults() -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Mere helper import must not install authority."""
    env = import_env()
    env.pop("CURIE_TEST_IDENTITY_FENCE_KEY", None)
    code = (
        "import json,identity_fence_support;from curie_api import identity_fence as w;"
        "from curie_dispatcher import admission as r;"
        "print(json.dumps([w.FENCE_KEY,r.IDENTITY_FENCE_KEY]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert json.loads(result.stdout) == [DEFAULT, DEFAULT]


@pytest.mark.parametrize("key", [None, ""])
def test_child_factory_requires_key_before_actual_app_creation(
    monkeypatch: pytest.MonkeyPatch,
    key: str | None,
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Missing key is a local programming refusal."""
    factory = helper("create_namespaced_app")
    import curie_api.main as main

    def forbidden() -> Any:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        pytest.fail("missing child key reached app creation")

    with monkeypatch.context() as patch:
        patch.delenv("CURIE_TEST_IDENTITY_FENCE_KEY", raising=False)
        if key is not None:
            patch.setenv("CURIE_TEST_IDENTITY_FENCE_KEY", key)
        patch.setattr(main, "create_app", forbidden)
        with pytest.raises(ValueError):
            factory()


def test_default_entrypoint_still_uses_actual_main(tmp_path: Path) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Keep the original executable/logging path."""
    from types import SimpleNamespace

    from identity_fence_isolation_process import owned_probe
    from sqlalchemy.engine import make_url

    from .test_access_log_redaction import _start_api

    with owned_probe(tmp_path, [], databases=1) as probe:
        env = import_env()
        env["DATABASE_URL"] = (
            make_url(get_settings().database_url)
            .set(database=probe.databases[0])
            .render_as_string(hide_password=False)
        )
        migrate = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from _migration_support import alembic_config;"
                "from alembic import command;command.upgrade(alembic_config(),'head')",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        probe.record["processes"].append(migrate.pid)
        probe.save()
        try:
            output, _ = migrate.communicate(timeout=30)
            assert migrate.returncode == 0, output
        finally:
            if migrate.poll() is None:
                migrate.terminate()
                migrate.communicate(timeout=10)
        owner = SimpleNamespace(addfinalizer=probe.finalizers.append)
        api = _start_api(owner, tmp_path, {"DATABASE_URL": env["DATABASE_URL"]})
        probe.record["processes"].append(api.process.pid)
        probe.save()
        try:
            assert api.process.args[1:3] == ["-m", "curie_api"]
            assert not api.missing_entrypoint
            with httpx.Client(base_url=api.url, trust_env=False, timeout=5) as client:
                assert client.get("/health").json() == {"status": "ok"}
        finally:
            api.stop()
            assert not api.forced_kill


@pytest.mark.parametrize("topology", ["shared_control", "worker_authorities"])
def test_two_sql_authorities_observe_real_publication_order(tmp_path: Path, topology: str) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Both SQL commits are real; exact CAS stays strict."""
    from identity_fence_isolation_process import owned_probe

    run = "probe-" + uuid.uuid4().hex
    namespace = getattr(
        importlib.import_module("identity_fence_support"), "fence_key_for_worker", None
    )
    shared = f"curie:test:identity-fence:{run}:shared"
    # The old wiring deliberately uses one owned key across independent databases.
    # A missing helper must still execute/prove the collision, not fail discovery first.
    keys = (
        [namespace(run, "gw0"), namespace(run, "gw1")]
        if topology == "worker_authorities" and callable(namespace)
        else [shared, shared]
    )
    with owned_probe(tmp_path, keys) as probe:
        first = probe.child(probe.databases[0], keys[0], "first")
        first.wait("pending_ack")
        second = probe.child(probe.databases[1], keys[1], "second")
        second.wait("pending_ack")
        for child in (first, second):
            child.send("enter")
            child.wait("committed")
        second.send("publish")
        later = second.wait("finished")
        first.send("publish")
        earlier = first.wait("finished")
        rows = [probe.rows(database) for database in probe.databases]
        probe.record["durable_rows"] = rows
        probe.save()
        assert rows == [["first"], ["second"]]
        observed = [earlier["outcome"], later["outcome"]]
        assert observed == (
            ["refused", "published"] if topology == "shared_control" else ["published", "published"]
        ), observed


def test_same_sql_authority_remains_serialized(tmp_path: Path) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Observe a real blocking edge before release."""
    from identity_fence_isolation_process import owned_probe

    key = f"curie:test:identity-fence:probe-{uuid.uuid4().hex}:same"
    with owned_probe(tmp_path, [key], databases=1) as probe:
        first = probe.child(probe.databases[0], key, "first")
        first.wait("pending_ack")
        holder = first.history[0]["backend_pid"]
        second = probe.child(probe.databases[0], key, "second")
        waiter = second.wait("locking")["backend_pid"]
        deadline = time.monotonic() + 5
        while True:
            rows = asyncio.run(probe.sql(f"SELECT pg_blocking_pids({waiter}) AS blockers"))
            if holder in rows[0]["blockers"]:
                probe.record["blocking_edge"] = {
                    "holder": holder,
                    "waiter": waiter,
                    "blockers": rows[0]["blockers"],
                }
                probe.save()
                break
            assert time.monotonic() < deadline, "same-authority writer never blocked"
            time.sleep(0.01)
        assert not any(row["event"] == "pending_ack" for row in second.history)
        first.send("enter")
        first.wait("committed")
        first.send("publish")
        assert first.wait("finished")["outcome"] == "published"
        second.wait("pending_ack")
        second.send("enter")
        second.wait("committed")
        second.send("publish")
        assert second.wait("finished")["outcome"] == "published"
        assert probe.rows(probe.databases[0]) == ["first", "second"]
