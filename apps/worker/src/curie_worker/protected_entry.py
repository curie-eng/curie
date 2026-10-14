"""Explicit provisioner process inputs, never installation qualification.

No ordinary worker bootstrap, model/history credential or ambient settings
are used. The provisioner separately establishes mount ownership and runtime
qualification. @spec PROTECTED-HOOK-LANE-6/7.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory, _open_root
from curie_protected_hooks.authority_records import parse_qualification, parse_readiness
from curie_protected_hooks.worker_artifact_selection import select_artifact
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from .protected_kubernetes_projection import ProjectionDirectory
from .protected_launch_client import LaunchTLSConfig, ProtectedLaunchClient
from .protected_nomination import HttpNominationSubmitter
from .protected_run import ProtectedRuntime, configuration_for_bootstrap
from .protected_worker_bootstrap import (
    WorkerBootstrapInvalid as WorkerBootstrapInvalid,
)
from .protected_worker_bootstrap import (
    _pairs,
    _read,
    _refuse,
    _reject,
    _require,
    load_worker_bootstrap,
)
from .reply_sink import HttpReplyAdapter, ReplySinkRouter
from .slack_sink import SlackReplyAdapter


@dataclass(frozen=True, slots=True, repr=False)
class ProcessSettings:
    database_url: str = field(repr=False)
    db_schema: str = field(repr=False)
    api_base_url: str = field(repr=False)
    internal_worker_token: str = field(repr=False)
    adapter_credentials: dict[str, str] = field(repr=False)
    slack_token: str = field(repr=False)
    slack_base_url: str = field(repr=False)
    slack_trusted_origins: tuple[str, ...] = field(repr=False)
    slack_identity_tokens: dict[str, str] = field(repr=False)
    launch_tls: LaunchTLSConfig = field(repr=False)

    def __repr__(self) -> str:
        return "ProtectedProcessSettings()"


def _http_origin(raw: str, *, allow_path: bool = False) -> None:
    parsed = urlsplit(raw)
    _require(
        parsed.scheme in {"http", "https"} and bool(parsed.hostname)
        and not parsed.username and not parsed.password
        and (allow_path or not parsed.path) and not parsed.query and not parsed.fragment
        and (parsed.port is None or 1 <= parsed.port <= 65535)
    )


def load_process_settings(directory: str) -> ProcessSettings:
    """Read one bounded regular process.json; no environment fallback."""
    try:
        root = _open_root(directory)
        try:
            value = json.loads(
                _read(root, "process.json").decode("utf-8", errors="strict"),
                object_pairs_hook=_pairs, parse_float=_reject, parse_constant=_reject,
            )
        finally:
            os.close(root)
        _require(type(value) is dict and value.keys() == {
            "schema_version", "database_url", "db_schema", "api_base_url",
            "internal_worker_token", "adapter_credentials", "slack", "launch_tls",
        })
        _require(type(value["schema_version"]) is int and value["schema_version"] == 1)
        for name in ("database_url", "db_schema", "api_base_url", "internal_worker_token"):
            _require(type(value[name]) is str and bool(value[name]))
        _require(make_url(value["database_url"]).drivername == "postgresql+asyncpg")
        _require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value["db_schema"]) is not None)
        _http_origin(value["api_base_url"])
        adapters = value["adapter_credentials"]
        _require(type(adapters) is dict and all(
            type(k) is str and bool(k) and type(v) is str and bool(v) for k, v in adapters.items()
        ))
        slack = value["slack"]
        _require(type(slack) is dict and slack.keys() == {
            "token", "base_url", "trusted_origins", "identity_tokens",
        })
        _require(type(slack["token"]) is str and type(slack["base_url"]) is str)
        if slack["base_url"]:
            _http_origin(slack["base_url"], allow_path=True)
        origins = slack["trusted_origins"]
        _require(type(origins) is list and all(type(v) is str for v in origins))
        for origin in origins:
            _http_origin(origin)
        tokens = slack["identity_tokens"]
        _require(type(tokens) is dict and all(
            type(k) is str and bool(k) and type(v) is str and bool(v) for k, v in tokens.items()
        ))
        tls = value["launch_tls"]
        _require(type(tls) is dict and tls.keys() == {
            "origin", "ca_file", "server_name", "server_spki_sha256",
            "client_certificate", "client_key", "worker_principal_id",
        } and all(type(v) is str for v in tls.values()))
        return ProcessSettings(
            value["database_url"], value["db_schema"], value["api_base_url"],
            value["internal_worker_token"], dict(adapters), slack["token"],
            slack["base_url"], tuple(origins), dict(tokens), LaunchTLSConfig(**tls),
        )
    except Exception:  # noqa: BLE001  Never retain private configuration diagnostics.
        pass
    _refuse()


@asynccontextmanager
async def open_runtime(directory: str) -> AsyncIterator[ProtectedRuntime]:
    """Own one file-selected runtime; caller joins run before leaving this scope.

    Partial construction and cancellation close only resources opened here.
    Qualified artifact selection is authenticated on the real private broker;
    structural files never mint qualification or install control records.
    """
    async with AsyncExitStack() as owner:
        bootstrap = await asyncio.to_thread(load_worker_bootstrap, directory)
        settings = await asyncio.to_thread(load_process_settings, directory)
        root = _open_root(directory)
        try:
            ca_pem = _read(root, "ca.pem").decode("ascii")
        finally:
            os.close(root)
        broker = await AuthenticatedWorkerClient.connect(
            bootstrap.manifest, bootstrap.credential, ca_pem
        )
        owner.push_async_callback(broker.close)
        manifest = bootstrap.manifest
        m = manifest.as_dict()
        raw = await broker.read_control(
            "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
        )
        _require(type(raw) is bytes)
        assert isinstance(raw, bytes)
        readiness = parse_readiness(raw).as_dict()
        qualification_raw = await broker.read_control(
            "protected:control:qualification:" + m["qualification_id"] + ":"
            + readiness["qualification_generation"]
        )
        _require(type(qualification_raw) is bytes)
        assert isinstance(qualification_raw, bytes)
        qualification = parse_qualification(qualification_raw)
        with ArtifactMeasurementDirectory(os.path.join(directory, "measurements")) as measurements:
            measurement = measurements.load(manifest=manifest, qualification=qualification)
        selected = await select_artifact(broker, trusted_manifest=manifest, measurement=measurement)
        _require(selected.qualification.canonical_bytes == qualification.canonical_bytes)
        with ProjectionDirectory(os.path.join(directory, "projections")) as projections:
            projection = projections.load(
                manifest=manifest, measurement=measurement, execution=bootstrap.execution
            )
        config = configuration_for_bootstrap(
            bootstrap, database_url=settings.database_url, db_schema=settings.db_schema,
            api_base_url=settings.api_base_url,
            internal_worker_token=settings.internal_worker_token,
            adapter_credentials=settings.adapter_credentials,
        )
        engine = create_async_engine(config.database_url, pool_pre_ping=True)
        owner.push_async_callback(engine.dispose)
        client = ProtectedLaunchClient(
            settings.launch_tls, broker=broker, manifest=manifest,
            qualification=qualification, execution=bootstrap.execution,
            measurement=measurement, projection=projection,
        )
        owner.push_async_callback(client.close)
        slack = SlackReplyAdapter(
            settings.slack_token, identity_tokens=settings.slack_identity_tokens,
            base_url=settings.slack_base_url or None,
            trusted_origins=settings.slack_trusted_origins,
        )
        reply = ReplySinkRouter(
            adapters={"slack": slack}, default=HttpReplyAdapter(settings.adapter_credentials)
        )
        owner.push_async_callback(reply.aclose)
        runtime = ProtectedRuntime(
            broker=broker, manifest=manifest, engine=engine, db_schema=config.db_schema,
            execution=bootstrap.execution, launch_client=client, config=config, reply_sink=reply,
            nomination_submitter=HttpNominationSubmitter(
                config.api_base_url, config.internal_worker_token
            ),
        )
        owner.push_async_callback(runtime.close)
        yield runtime


async def _serve(directory: str) -> None:
    async with open_runtime(directory) as runtime:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            loop.add_signal_handler(sig, runtime.request_stop)
        run = asyncio.create_task(runtime.run(), name="protected-worker:run")
        try:
            try:
                await asyncio.shield(run)
            except asyncio.CancelledError:
                runtime.request_stop()
                # Repeated parent cancellation must not skip the owned generation's
                # normal join and close clients beneath its background work.
                while not run.done():
                    try:
                        await asyncio.shield(run)
                    except asyncio.CancelledError:
                        continue
                    except Exception:  # noqa: BLE001  Preserve caller cancellation after joining.
                        break
                if not run.cancelled():
                    run.exception()
                raise
        finally:
            for sig in signals:
                loop.remove_signal_handler(sig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one provisioned protected worker")
    parser.add_argument("--runtime-dir", required=True)
    args = parser.parse_args()
    try:
        asyncio.run(_serve(args.runtime_dir))
        return
    except Exception:  # noqa: BLE001  Process diagnostics must not disclose supplied credentials.
        pass
    error = SystemExit("protected worker stopped without confirmed completion")
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


if __name__ == "__main__":
    main()
