"""Internal commissioning control composition, ADR 0236.

@spec PROTECTED-COMMISSIONING-5/8. This foundation binds B, G, M and the
control profile, owns per-boot primitive registries, constructs the per-call
signed-channel guard and joins a campaign under its supervisor. The admission
guard is deployed separately by ControlInstallation; it is never embedded here.

There is intentionally no executable entry point, SDK/retained-peer campaign
driver, model credential, acceptance or publisher here. The later operator
must supply the real producers, campaign and authenticated transports. A
successful cleanup report is not commissioning evidence or qualification.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import stat
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn
from uuid import UUID, uuid4

from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.commissioning_bootstrap import CommissioningBootstrap
from curie_protected_hooks.commissioning_operator_guard import ChannelCampaignGuard
from curie_protected_hooks.commissioning_producers import (
    AclPrincipalStateProducer,
    FactProducer,
    PhaseSource,
    ReobservationSet,
)
from curie_protected_hooks.commissioning_provisioner import parse_guard_config, verify_tuple
from curie_protected_hooks.commissioning_records import CommissioningPermit

from .commissioning_control_profile import ControlProfile, validate_control_profile_binding
from .commissioning_observation_channel import ObservationRequester
from .commissioning_probe_registry import ProbeRegistry
from .commissioning_probe_runtime import (
    PrimitiveCompletions,
    PrimitiveProbeRuntime,
    ProbeKubernetes,
)
from .commissioning_supervisor import (
    CampaignLaunches,
    CampaignState,
    CampaignSupervisor,
    CampaignView,
    CloseReport,
)
from .managed_cni_probe_kit import PRODUCER_CONSTANTS
from .protected_kubernetes_projection import Projection
from .protected_launch_registry import _open_root


def _deadline() -> float:
    return float(PRODUCER_CONSTANTS["per_call_deadline_ms"]) / 1000


class ControlProcessRefused(RuntimeError):
    """Fixed diagnostic: never contains a record, credential or producer exception."""

    def __init__(self) -> None:
        super().__init__("Commissioning control process refused")


def _refuse() -> NoReturn:
    raise ControlProcessRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


@dataclass(frozen=True, slots=True, repr=False)
class ControlBinding:
    """Structurally bound installation inputs; authenticating their source is external."""

    bootstrap: CommissioningBootstrap
    manifest: Manifest
    guard_config: bytes
    profile: ControlProfile
    projection: Projection

    def __post_init__(self) -> None:
        try:
            verify_tuple(self.bootstrap, self.guard_config, self.manifest)
            g = parse_guard_config(self.guard_config)
            validate_control_profile_binding(
                self.profile,
                admission_policy_sha256=g["admission_policy_sha256"],
                projection=self.projection,
            )
            m, p = self.manifest.as_dict(), self.projection.as_dict()
            _require(
                m["substrate"]
                == {
                    "kind": "kubernetes",
                    "authority_domain_id": p["authority_domain_id"],
                    "launch_identity": p["launch_identity"],
                    "launch_config_sha256": self.projection.digest,
                }
            )
            _require(m["runner_image_digest"] == p["image"]["digest"])
            _require(p["embedded_bundle"] == {**m["bundle_digest"], "path": "/bundles/current"})
        except Exception:  # noqa: BLE001 - no input diagnostics
            _refuse()


class BoundedProducer:
    """Cancel each fresh observation at its deadline; never accept a late result.

    Joining cancellation can take longer when a transport must drain a reply.
    The transport must itself bound that drain and honor eventual cancellation.
    """

    def __init__(self, producer: FactProducer) -> None:
        self._producer = producer

    @property
    def fact(self) -> str:
        return self._producer.fact

    async def observe(self) -> str:
        try:
            async with asyncio.timeout(_deadline()) as limit:
                result = await self._producer.observe()
            _require(not limit.expired())
            return result
        except Exception:  # noqa: BLE001 - cancellation still propagates and fences the guard
            _refuse()


class ProbeBoot:
    """One boot's probes, after every earlier boot's ownership has been settled.

    The stable private root holds only a process lock and boot UUID directories.
    Every earlier registry is reopened and retired, never discarded or adopted.
    An uncertain create or unproved termination blocks a fresh boot. Even fully
    retired old rows need fresh absence observations before a new registry is
    created. Prior completion rows never enter the new fact producer.
    """

    def __init__(
        self,
        *,
        root_fd: int,
        lock_fd: int,
        registry_root: Path,
        runtime: PrimitiveProbeRuntime,
        probe_ids: tuple[str, ...],
    ) -> None:
        self._root_fd, self._lock_fd = root_fd, lock_fd
        self.registry_root, self.runtime, self.probe_ids = registry_root, runtime, probe_ids
        self.boot_id = runtime.registry.boot_id
        self.completions = PrimitiveCompletions(runtime, probe_ids)
        self._released = False

    @classmethod
    async def open(
        cls,
        root: Path,
        *,
        binding: ControlBinding,
        kubernetes: ProbeKubernetes,
        count: int,
    ) -> ProbeBoot:
        """Retire old ownership before creating anything; cancellation closes all descriptors."""
        root_fd = lock_fd = -1
        current: ProbeRegistry | None = None
        try:
            _require(type(binding) is ControlBinding and type(count) is int and 1 <= count <= 64)
            root_fd = _open_root(str(root))
            lock_fd = os.open(
                "control.lock",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=root_fd,
            )
            info = os.fstat(lock_fd)
            _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid())
            _require(info.st_mode & 0o077 == 0 and info.st_nlink == 1)
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            boot_id = str(uuid4())
            old_ids: set[str] = set()
            for name in sorted(os.listdir(root_fd)):
                if name == "control.lock":
                    continue
                _require(str(UUID(name)) == name)
                path = root / name
                _require(not path.is_symlink())
                # An interrupted mkdir/open is not silently treated as an empty
                # old registry: a missing database requires independent recovery.
                _require((path / "probe.sqlite").is_file())
                current = ProbeRegistry.open(
                    str(path), boot_id=boot_id, scope_digest=binding.profile.digest
                )
                runtime = PrimitiveProbeRuntime(
                    current, kubernetes, profile=binding.profile, projection=binding.projection
                )
                old_ids.update(row.probe_id for row in current.probes())
                async with asyncio.timeout(_deadline()) as limit:
                    _require(await runtime.retire_all())
                    for row in current.probes():
                        _require(await runtime.absent(row))
                _require(not limit.expired())
                current.close()
                current = None
            registry_root = root / boot_id
            os.mkdir(boot_id, mode=0o700, dir_fd=root_fd)
            os.fsync(root_fd)
            current = ProbeRegistry.open(
                str(registry_root), boot_id=boot_id, scope_digest=binding.profile.digest
            )
            runtime = PrimitiveProbeRuntime(
                current, kubernetes, profile=binding.profile, projection=binding.projection
            )
            ids: set[str] = set()
            while len(ids) < count:
                candidate = str(uuid4())
                if candidate not in old_ids:
                    ids.add(candidate)
            return cls(
                root_fd=root_fd,
                lock_fd=lock_fd,
                registry_root=registry_root,
                runtime=runtime,
                probe_ids=tuple(sorted(ids)),
            )
        except BaseException as error:
            if current is not None:
                current.close()
            for fd in (lock_fd, root_fd):
                if fd >= 0:
                    os.close(fd)
            if not isinstance(error, Exception):
                raise
            _refuse()

    def release(self) -> None:
        """Close local handles only. Durable ownership remains for the next boot."""
        if not self._released:
            self._released = True
            try:
                self.runtime.registry.close()
            finally:
                os.close(self._lock_fd)
                os.close(self._root_fd)

    async def close(self) -> bool:
        """Bounded retirement and fresh absence; false means incomplete, never success."""
        if self._released:
            return False
        try:
            async with asyncio.timeout(_deadline()) as limit:
                if not await self.runtime.retire_all():
                    return False
                for row in self.runtime.registry.probes():
                    if not await self.runtime.absent(row):
                        return False
            return not limit.expired()
        except Exception:  # noqa: BLE001 - durable ownership survives an incomplete close
            return False
        finally:
            self.release()


def campaign_guard(
    binding: ControlBinding,
    *,
    permit: CommissioningPermit,
    probes: ProbeBoot,
    premeasured: Mapping[str, str],
    producers: tuple[FactProducer, ...],
    acl: AclPrincipalStateProducer,
    phase: PhaseSource,
    requester: ObservationRequester,
) -> ChannelCampaignGuard:
    """Compose a guard with this boot's real completion producer and signed requester.

    Other producers must be supplied by the later operator composition. This
    internal seam performs no independent acceptance and cannot certify that a
    caller's producer implementation measured a real installation.
    """
    try:
        _require(type(binding) is ControlBinding and type(permit) is CommissioningPermit)
        _require(type(probes) is ProbeBoot and not probes._released)
        _require(type(requester) is ObservationRequester and type(producers) is tuple)
        _require(probes.runtime.registry.scope_digest == binding.profile.digest)
        p = permit.as_dict()
        _require(p["manifest_digest"] == binding.manifest.digest)
        _require(p["bootstrap_digest"] == binding.bootstrap.digest)
        for name, expected in (
            ("manifest", binding.manifest.digest),
            ("bootstrap", binding.bootstrap.digest),
            ("permit", permit.digest),
            ("projection", binding.projection.digest),
        ):
            _require(premeasured[name] == expected)
        _require(all(producer.fact != "primitive_completions" for producer in producers))
        return ChannelCampaignGuard(
            premeasured=premeasured,
            control=ReobservationSet(
                tuple(BoundedProducer(p) for p in (*producers, probes.completions))
            ),
            acl=acl,
            phase=phase,
            transport=requester,
            # Preserve the channel's 30s aggregate roundtrip budget. Each fact
            # and the complete ACL stage separately has the producer deadline.
            acl_deadline_s=_deadline(),
        )
    except Exception:  # noqa: BLE001
        _refuse()


@dataclass(frozen=True, slots=True)
class ControlClose:
    """Local lifecycle report only. No campaign case or installation is accepted."""

    campaign: CloseReport
    probes_retired: bool

    @property
    def complete(self) -> bool:
        return self.campaign.complete and self.probes_retired


class _BoundedCampaignView:
    def __init__(self, campaign: CampaignView) -> None:
        self._campaign = campaign
        self.uncertain = False

    @property
    def expires_at_ms(self) -> int:
        return self._campaign.expires_at_ms

    async def status(self) -> CampaignState:
        try:
            async with asyncio.timeout(_deadline()) as limit:
                result = await self._campaign.status()
            _require(not limit.expired())
            return result
        except BaseException:
            self.uncertain = True
            raise

    async def begin_close(self) -> None:
        try:
            async with asyncio.timeout(_deadline()) as limit:
                await self._campaign.begin_close()
            _require(not limit.expired())
        except BaseException:
            self.uncertain = True
            raise


class _BoundedCampaignLaunches:
    def __init__(self, launches: CampaignLaunches) -> None:
        self._launches = launches
        self.uncertain = False

    def fence(self) -> None:
        self._launches.fence()

    async def close_campaign(self) -> tuple[str, ...]:
        try:
            async with asyncio.timeout(_deadline()) as limit:
                result = await self._launches.close_campaign()
            _require(not limit.expired())
            return result
        except BaseException:
            self.uncertain = True
            raise


class ControlProcess:
    """Single-use lifecycle for a supervisor and probe boot.

    The supervisor must own every SDK/runner task. Normal driver return, driver
    failure and external cancellation all revoke it, join its cleanup, and then
    settle probes. Servers/transport credentials remain owned by the caller's
    outer resource scope. External adapters must honor cancellation: asyncio
    deadlines cannot forcibly join arbitrary cancellation-resistant coroutines.
    A timed-out observation or a supervisor report with joined=False refuses;
    the latter can leave an owned session pending and requires outer recovery.
    """

    def __init__(
        self,
        *,
        campaign: CampaignView,
        launches: CampaignLaunches,
        probes: ProbeBoot,
        interval_s: float = 1.0,
        join_s: float = 10.0,
        close_s: float = 120.0,
    ) -> None:
        _require(type(probes) is ProbeBoot and not probes._released)
        self._campaign = _BoundedCampaignView(campaign)
        self._launches = _BoundedCampaignLaunches(launches)
        self.supervisor = CampaignSupervisor(
            self._campaign,
            self._launches,
            interval_s=interval_s,
            join_s=join_s,
            close_s=close_s,
        )
        self.probes = probes
        self._ran = False
        self.close_report: ControlClose | None = None

    async def run(self, campaign: Callable[[], Coroutine[Any, Any, None]]) -> ControlClose:
        """Run once; any incomplete cleanup or failed driver refuses."""
        _require(not self._ran and callable(campaign))
        self._ran = True
        watch = asyncio.create_task(self.supervisor.run(), name="commissioning-supervisor")

        # The driver first completes primitive probes, then constructs and
        # calls campaign_guard before opening the authenticated worker/ingress.
        # Nothing in this lifecycle grants permission to bypass those gates.
        failed = False
        try:
            work = self.supervisor.own(campaign())
            await asyncio.wait((work, watch), return_when=asyncio.FIRST_COMPLETED)
            if work.done() and not work.cancelled():
                work.result()
            else:
                failed = True
        except Exception:  # noqa: BLE001 - caller gets only a fixed refusal after cleanup
            failed = True
        finally:
            self.supervisor.revoke()

            async def finish() -> ControlClose:
                try:
                    report = await watch
                    if self._campaign.uncertain or self._launches.uncertain:
                        report = CloseReport(report.reason, report.joined, None)
                finally:
                    retired = await self.probes.close()
                return ControlClose(report, retired)

            # External cancellation cannot interrupt ownership cleanup. The
            # supervisor's own join/close budgets govern the cleanup, not the
            # cancelled campaign task.
            cleanup = asyncio.create_task(finish(), name="commissioning-control-cleanup")
            interrupted = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Repeated signals must not release the registry lock while
                    # the supervisor is still settling its owned resources.
                    interrupted = True
            try:
                self.close_report = cleanup.result()
            except Exception:  # noqa: BLE001 - cleanup uncertainty is always a refusal
                _refuse()
            if interrupted:
                raise asyncio.CancelledError
        _require(not failed and self.close_report.complete)
        return self.close_report
