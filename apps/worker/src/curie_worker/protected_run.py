"""Explicit source composition; no ordinary worker activation or background jobs.

Provisioner-selected inputs are required. This module issues no qualification.
@spec PROTECTED-HOOK-LANE-6/7.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_transport import (
    PROTECTED_WORKER_MAX_READ_BLOCK_MS,
    AuthenticatedWorkerClient,
)
from pydantic import AliasChoices
from sqlalchemy.ext.asyncio import AsyncEngine

from .binding import BindingResolver
from .config import WorkerConfig
from .kernel.core import Kernel
from .protected_binding import ProtectedMediatedBindingResolver
from .protected_completion import ProtectedReplySink
from .protected_configuration import ProtectedExecutionConfig
from .protected_dispatch import ExecutingProtectedConsumer, GuardedProtectedKernel, _unavailable
from .protected_launch_client import ProtectedLaunchClient
from .protected_markers import ProtectedMarkers
from .protected_nomination import GuardedRunner, HttpNominationSubmitter, NominationCapturingRunner
from .protected_runner import ProtectedRunnerClient
from .protected_substrate import ProtectedSandboxSubstrate
from .protected_worker_bootstrap import ProtectedWorkerBootstrap
from .reply_sink import HttpReplyAdapter, ReplySinkRouter
from .runner_client import RunnerClient
from .sandbox import SandboxSubstrate
from .threadlock import ThreadLock


def configuration_for_bootstrap(
    bootstrap: ProtectedWorkerBootstrap,
    *,
    database_url: str,
    db_schema: str,
    api_base_url: str,
    internal_worker_token: str,
    adapter_credentials: Mapping[str, str],
) -> WorkerConfig:
    """Use explicit provisioner inputs and defaults, never ordinary environment.

    Broker handles come exclusively from the authenticated private client;
    ordinary Valkey settings and runner credentials have no role here.
    """
    try:
        if type(bootstrap) is not ProtectedWorkerBootstrap or not internal_worker_token:
            _unavailable()
        namespace = derive_lane_namespace(bootstrap.manifest)
        values = {
            name: member.get_default(call_default_factory=True)
            for name, member in WorkerConfig.model_fields.items()
        }
        values.update(
            database_url=database_url,
            db_schema=db_schema,
            api_base_url=api_base_url,
            internal_worker_token=internal_worker_token,
            adapter_credentials=dict(adapter_credentials),
            key_prefix=namespace.prefix,
            consumer_group=namespace.group,
            stream=namespace.stream,
            dead_letter_stream=namespace.dead,
            max_attempts=1,
            read_block_ms=PROTECTED_WORKER_MAX_READ_BLOCK_MS,
        )
        # Populate every validation alias explicitly. Leaving even defaulted
        # fields absent lets BaseSettings fill them from the ordinary process.
        selected: dict[str, Any] = {}
        for name, member in WorkerConfig.model_fields.items():
            alias = member.validation_alias
            if isinstance(alias, AliasChoices):
                alias = alias.choices[0]
            if alias is not None and not isinstance(alias, str):
                _unavailable()
            selected[alias if isinstance(alias, str) else name] = values[name]
        return WorkerConfig(**selected)
    except Exception:  # noqa: BLE001  Configuration may contain credentials.
        pass
    _unavailable()


class ProtectedRuntime:
    """Concrete primary runtime; caller owns supplied broker/database lifetimes."""

    def __init__(
        self,
        *,
        broker: AuthenticatedWorkerClient,
        manifest: Manifest,
        engine: AsyncEngine,
        db_schema: str,
        execution: ProtectedExecutionConfig,
        launch_client: ProtectedLaunchClient,
        config: WorkerConfig,
        reply_sink: HttpReplyAdapter | ReplySinkRouter,
        nomination_submitter: HttpNominationSubmitter | None = None,
    ) -> None:
        if (
            type(broker) is not AuthenticatedWorkerClient
            or type(manifest) is not Manifest
            or type(launch_client) is not ProtectedLaunchClient
            or type(engine) is not AsyncEngine
            or type(execution) is not ProtectedExecutionConfig
            or type(config) is not WorkerConfig
            or type(reply_sink) not in {HttpReplyAdapter, ReplySinkRouter}
            or (
                nomination_submitter is not None
                and type(nomination_submitter) is not HttpNominationSubmitter
            )
            or launch_client._broker is not broker
            or launch_client._manifest.canonical_bytes != manifest.canonical_bytes
            or launch_client._execution.canonical_bytes != execution.canonical_bytes
            or execution.digest != manifest.as_dict()["execution_config_digest"]
        ):
            _unavailable()
        ns = derive_lane_namespace(manifest)
        if (
            config.key_prefix,
            config.stream,
            config.consumer_group,
            config.dead_letter_stream_name(),
        ) != (ns.prefix, ns.stream, ns.group, ns.dead):
            _unavailable()
        runs = broker.lane().runs
        self.affinity = broker.lane().affinity
        self.binding = ProtectedMediatedBindingResolver(
            engine, db_schema=db_schema, execution=execution, trusted_manifest=manifest
        )
        self.substrate = ProtectedSandboxSubstrate(launch_client)
        guarded_runner = ProtectedRunnerClient(launch_client, self.substrate)
        self.runner = (
            NominationCapturingRunner(cast(GuardedRunner, guarded_runner), nomination_submitter)
            if nomination_submitter is not None
            else guarded_runner
        )
        self.markers = ProtectedMarkers(
            runs,
            config,
            broker=broker,
            manifest=manifest,
            substrate=self.substrate,
            launch_client=launch_client,
        )
        self.sink = ProtectedReplySink(self.markers, reply_sink)
        self._reply = reply_sink
        self.lock = ThreadLock(
            runs,
            ttl_ms=config.lock_ttl_ms,
            acquire_timeout_s=config.lock_acquire_timeout_s,
            poll_interval_s=config.lock_poll_interval_s,
        )
        self.pressure_lock = ThreadLock(
            runs,
            ttl_ms=config.lock_ttl_ms,
            acquire_timeout_s=config.lock_acquire_timeout_s,
            poll_interval_s=config.lock_poll_interval_s,
        )
        kernel = Kernel(
            substrate=cast(SandboxSubstrate, self.substrate),
            runner=cast(RunnerClient, self.runner),
            sink=self.sink,
            lock=self.lock,
            pressure_lock=self.pressure_lock,
            markers=self.markers,
            config=config,
            binding=cast(BindingResolver, self.binding),
        )
        self.kernel = GuardedProtectedKernel(
            kernel,
            broker=broker,
            manifest=manifest,
            markers=self.markers,
            sink=self.sink,
            substrate=self.substrate,
        )
        self.consumer = ExecutingProtectedConsumer(
            client=broker, trusted_manifest=manifest, kernel=self.kernel, config=config
        )

    async def run(self) -> None:
        await self.consumer.run()

    def request_stop(self) -> None:
        self.consumer.request_stop()

    async def close(self) -> None:
        self.request_stop()
        await self.substrate.close()
        await self._reply.aclose()
