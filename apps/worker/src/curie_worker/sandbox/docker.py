"""Docker access for the sandbox substrate: local middle mode, no Kubernetes.

``DockerSandboxClient`` implements the same ``SandboxClient`` seam
(``apps/worker/src/curie_worker/sandbox/k8s.py``) that ``SandboxSubstrate`` is
written against, so selecting it (``CURIE_SANDBOX_SUBSTRATE=docker``) makes the
whole worker -- both the runs kernel and the eval consumer, which share one
substrate object -- boot runner containers with ``docker run`` instead of
claiming agent-sandbox CRs. The boot recipe mirrors the CLI's
``cli/src/docker.rs`` (image ``curie-runner``, port publish, plugin mount,
network join, the ACI boot env).

Local middle mode defaults to a REAL model: the runner authenticates through the
claude-agent-sdk, which reads ``CLAUDE_CODE_OAUTH_TOKEN`` / ``ANTHROPIC_API_KEY``
from its own environment (the runner never dereferences the ACI
``CURIE_CREDENTIALS`` reference). So this client forwards those SDK credential
vars BY NAME into every runner container when the worker has them set -- Docker
reads the value from the worker's environment, this code never does. Fake model
is an explicit offline/test opt-in (``CURIE_FAKE_MODEL=1``), gated in
``run.py``: middle mode never silently degrades to a fake when a credential is
missing.

Model mapping. Docker has no warm pool, no separate Sandbox object, and no init
containers, so one container is simultaneously the "claim" and the "sandbox"
(claim name == container name == sandbox name), and there is no bundle-fetch init
pair -- the worker fetches the plugin bundle from RustFS and bind-mounts it. The
host-process worker reaches each runner over a loopback-bound, Docker-assigned
host port (``docker port <name> 8080``) -- but only when it can actually reach
that host loopback. A worker running as a host-net *container* on Docker Desktop
(macOS/Windows) shares the VM's netns and cannot, so when a network is
configured the worker instead dials the runner on its shared-network container
IP at the fixed container port (``_dial_endpoint``), which is reachable in every
topology; the container joins that network anyway to reach the OTel collector by
name. Readiness (the substrate's ``claim.ready`` gate) is the runner's own
``/healthz`` answering, so ``claim()`` never returns a handle to a not-yet-listening
runner. Suspend maps to ``docker pause`` (the process freezes but keeps its port);
the substrate's resume path retires the paused container and claims a fresh one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from aci_protocol import BootEnv
from plugin_format import (
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBERS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
)

from ..attachments import (
    ATTACHMENTS_MOUNT_PATH,
    ATTACHMENTS_REF_ENV,
    AttachmentLimits,
    AttachmentResolutionError,
    AttachmentTooLargeError,
    decode_attachment_refs,
)
from ..binding import (
    BASE_URL_ENV,
    BUNDLE_REF_ENV,
    CREDENTIALS_ENV,
    FAKE_MODEL_ENV,
    PLUGIN_DIR_ENV,
)
from ..bundle_store import BundleReader, extract_bundle
from ..workspace import (
    WORKSPACE_MOUNT_PATH,
    WORKSPACE_REF_ENV,
    WORKSPACE_SHA256_ENV,
    WorkspaceLimits,
    WorkspacePreparationError,
    WorkspaceRef,
    validate_workspace_archive,
)
from .types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimView,
    OperatingMode,
    SandboxError,
    SandboxView,
    filter_agent_child_env,
)

#: Per-object read deadline when redeeming an attachment capability. A local
#: constant rather than an AttachmentLimits field: the reference already
#: expires, the size cap already bounds the transfer, and widening the operator
#: envelope for a value nobody asked to tune would add a knob and a chart value
#: that mean nothing to them.
_ATTACHMENT_FETCH_TIMEOUT_S = 30.0


class _NoAttachments(Exception):
    """The claim carried a reference that decoded to zero files.

    Not an error: a turn whose attachment list is empty must produce exactly
    today's container spec, with no mount and no staged directory, so the empty
    case is indistinguishable from a turn that never carried the key at all.
    """

logger = logging.getLogger(__name__)

# The runner listens on this fixed port inside every container; the host port it
# is published to is Docker-assigned and read back per-container.
RUNNER_CONTAINER_PORT = 8080

# Declared boot keys this substrate produces. Docker is the OTel producer here
# (the chart's env block is its Kubernetes counterpart), and the runner parses
# both names out of the one declaration, so they are read from it (#488).
_OTEL_ENDPOINT_ENV = BootEnv.env_key("otel_endpoint")
_OTEL_PROTOCOL_ENV = BootEnv.env_key("otel_protocol")

# The ambient SDK credential vars, forwarded into the container BY NAME (docker
# reads the value from the worker env; this code never does, and no secret ever
# lands in the docker argv). These authenticate the runner directly on the legacy
# real-Anthropic path, and are forwarded only when no explicit CURIE_CREDENTIALS
# is chosen. CURIE_CREDENTIALS (the ACI reference the runner maps onto an SDK
# var) is selected positively and alone. Mirrors the CLI (cli/src/docker.rs,
# cli/src/commands.rs).
_SDK_PASSTHROUGH_ENV = (
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
)
# A Claude Code OAuth token shares the sk-ant- prefix with an API key; this more
# specific prefix marks it. Under a base-URL override the runner blanks such a
# token (runner sdk_auth.resolve_sdk_env), so forwarding it authenticates nothing
# and only lands a real token in the container's /proc/1/environ. Kept a literal
# mirror of runner/src/curie_runner/sdk_auth.py::OAUTH_TOKEN_PREFIX -- the
# authority for the prefix semantics -- and pinned across both lanes by the
# `byo_oauth_shaped` vector input (issue #603).
_OAUTH_TOKEN_PREFIX = "sk-ant-oat"
# Env keys the worker sets explicitly or forwards specially, so the generic
# value loop must not also emit them: the plugin dir and sandbox id are set
# explicitly, the bundle ref named a RustFS object the worker already fetched
# (the runner never fetches), and the credential is forwarded by name (never as
# a value in the argv).
_WORKER_OWNED_ENV = frozenset(
    {
        BUNDLE_REF_ENV,
        PLUGIN_DIR_ENV,
        "CURIE_SANDBOX_ID",
        CREDENTIALS_ENV,
        # The attachment capability is redeemed HERE, by this driver, exactly as
        # the bundle ref is. Forwarding it would hand the sandbox a live signed
        # url it has no reason to hold; Kubernetes scopes the same key to its
        # init container and keeps it off the runner, and the two substrates
        # must not disagree about what the agent can reach.
        ATTACHMENTS_REF_ENV,
    }
)


def _parse_created(raw: str) -> datetime | None:
    """A container's ``.Created`` as tz-aware UTC, or None when unreadable.

    Docker emits RFC3339Nano, for example ``2026-08-16T12:00:00.123456789Z``.
    On this project's Python (>=3.13) ``datetime.fromisoformat`` parses that
    directly, ``Z`` and nine fractional digits included, truncating the
    fraction to microseconds itself.

    Normalizing to aware UTC is load-bearing: the reaper compares this against
    ``datetime.now(UTC)``, and a naive value raises TypeError inside a
    maintenance tick whose caller swallows exceptions, silently ending reaping.
    An unreadable value returns None so one odd container cannot kill the tick.
    """

    raw = raw.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class RunnerHardening:
    """Container-level isolation applied to every runner container the Docker
    substrate boots.

    Local mode accepts TRUSTED bundles only and is NOT the Kubernetes security
    boundary (that is the Helm path: gVisor + a default-deny egress
    NetworkPolicy). These controls are practical defense-in-depth that mirror the
    K8s runner ``securityContext`` + ``resources`` (charts/curie:
    ``readOnlyRootFilesystem``, ``capabilities.drop: [ALL]``,
    ``allowPrivilegeEscalation: false``, ``seccompProfile: RuntimeDefault``, and
    bounded cpu/memory), so a trusted-but-buggy bundle cannot escalate on the
    host, reach the Docker daemon, or roam the data tier.

    Every field is overridable from the worker env (``from_env``) so an operator
    can loosen a limit a heavy bundle needs without editing code; ``enabled=0``
    turns the whole set off for debugging. Docker's default seccomp profile (the
    RuntimeDefault equivalent) stays active whenever hardening is on -- this code
    never passes ``seccomp=unconfined``.
    """

    enabled: bool = True
    read_only_root: bool = True
    # tmpfs mounts (the emptyDir equivalent) so a read-only-rootfs runner can
    # still write HOME and /tmp. Mirrors the chart's runner writablePaths. The
    # runner image runs as uid 1000; sticky world-writable (mode 1777) mounts let
    # it create entries it owns under a root-owned mount, matching the chart's
    # fsGroup-owned emptyDir semantics.
    writable_paths: tuple[str, ...] = ("/tmp", "/home/runner")
    drop_all_capabilities: bool = True
    no_new_privileges: bool = True
    # Bounded process/memory/cpu (chart runner limits: memory 768Mi, cpu "1").
    # pids has no chart analogue; a fork-bomb ceiling is cheap defense here.
    pids_limit: int = 512
    memory_limit: str = "768m"
    cpu_limit: str = "1"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> RunnerHardening:
        """Build from ``CURIE_RUNNER_HARDENING*`` env, falling back to the
        K8s-mirroring defaults above. Any unset knob keeps its default."""

        def _flag(name: str, default: bool) -> bool:
            raw = env.get(name)
            if raw is None or raw == "":
                return default
            return raw.strip().lower() not in ("0", "false", "no", "off")

        enabled = _flag("CURIE_RUNNER_HARDENING", True)
        paths_raw = env.get("CURIE_RUNNER_WRITABLE_PATHS")
        writable_paths = (
            tuple(p for p in (s.strip() for s in paths_raw.split(",")) if p)
            if paths_raw
            else cls.writable_paths
        )
        pids_raw = env.get("CURIE_RUNNER_PIDS_LIMIT")
        return cls(
            enabled=enabled,
            read_only_root=_flag("CURIE_RUNNER_READ_ONLY", True),
            writable_paths=writable_paths,
            drop_all_capabilities=_flag("CURIE_RUNNER_CAP_DROP_ALL", True),
            no_new_privileges=_flag("CURIE_RUNNER_NO_NEW_PRIVILEGES", True),
            pids_limit=int(pids_raw) if pids_raw else cls.pids_limit,
            memory_limit=env.get("CURIE_RUNNER_MEMORY_LIMIT") or cls.memory_limit,
            cpu_limit=env.get("CURIE_RUNNER_CPU_LIMIT") or cls.cpu_limit,
        )

    def run_args(self) -> list[str]:
        """The ``docker run`` flags that impose this hardening. Empty when
        disabled, so a debug run degrades to the pre-hardening argv exactly."""
        if not self.enabled:
            return []
        args: list[str] = []
        if self.read_only_root:
            args.append("--read-only")
            for path in self.writable_paths:
                # rw + sticky world-writable so the non-root runner can write.
                args += ["--tmpfs", f"{path}:rw,mode=1777"]
        if self.drop_all_capabilities:
            args += ["--cap-drop", "ALL"]
        if self.no_new_privileges:
            # Blocks setuid/gain-privilege on exec (== allowPrivilegeEscalation:
            # false). Docker's default seccomp profile is left active alongside.
            args += ["--security-opt", "no-new-privileges"]
        if self.pids_limit > 0:
            args += ["--pids-limit", str(self.pids_limit)]
        if self.memory_limit:
            args += ["--memory", self.memory_limit]
        if self.cpu_limit:
            args += ["--cpus", self.cpu_limit]
        return args


class DockerError(SandboxError):
    """A ``docker`` CLI invocation failed.

    Subclasses ``SandboxError`` so a provisioning failure (missing runner image,
    Docker daemon down) is handled by the kernel's runner-error retry path and
    the eval consumer's provisioning-failure path, rather than escaping unhandled
    and looping on reclaim.
    """


class DockerSandboxClient:
    """SandboxClient backed by local Docker containers (no Kubernetes)."""

    def __init__(
        self,
        *,
        image: str,
        bundle_store: BundleReader,
        network: str | None = None,
        otel_endpoint: str | None = None,
        host: str = "127.0.0.1",
        default_plugin_dir: str = "/bundles/current",
        healthz_timeout_s: float = 0.5,
        hardening: RunnerHardening | None = None,
        environ: Mapping[str, str] | None = None,
        bundle_max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
        bundle_max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
        bundle_max_members: int = DEFAULT_MAX_MEMBERS,
        workspace_limits: WorkspaceLimits | None = None,
        attachment_limits: AttachmentLimits | None = None,
    ) -> None:
        self._image = image
        self._bundles = bundle_store
        self._network = network
        self._otel_endpoint = otel_endpoint
        self._host = host
        self._default_plugin_dir = default_plugin_dir
        self._healthz_timeout_s = healthz_timeout_s
        # Extraction bounds (ADR-0059 decision 3), forwarded to
        # ``extract_bundle`` at claim time below. Defaults to plugin_format's
        # generous fallbacks; ``run.py`` passes the operator-configured
        # ``WorkerConfig`` values instead.
        self._bundle_max_uncompressed_bytes = bundle_max_uncompressed_bytes
        self._bundle_max_compression_ratio = bundle_max_compression_ratio
        self._bundle_max_members = bundle_max_members
        # Container isolation (read-only rootfs, dropped caps, no-new-privileges,
        # bounded resources). Defaults to the K8s-mirroring RunnerHardening; a
        # None means "use the on-by-default set", never "no hardening".
        self._hardening = hardening if hardening is not None else RunnerHardening()
        # Presence-only view of the worker environment, used to decide which SDK
        # credential vars to forward by name. Never read for their values.
        self._environ = environ if environ is not None else os.environ
        # Per-container host dir holding the extracted bundle, cleaned on delete.
        self._bundle_dirs: dict[str, str] = {}
        self._workspace_dirs: dict[str, str] = {}
        self._workspace_limits = workspace_limits or WorkspaceLimits()
        # Per-container host dir holding the redeemed attachments, cleaned on
        # delete exactly as the bundle and workspace dirs are.
        self._attachment_dirs: dict[str, str] = {}
        self._attachment_limits = attachment_limits or AttachmentLimits()

    # -- claim lifecycle ------------------------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,  # noqa: ARG002 -- no warm pool in Docker; kept for the seam.
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        env = filter_agent_child_env(env)
        plugin_dir = env.get(PLUGIN_DIR_ENV, self._default_plugin_dir)
        args = [
            "run",
            "-d",
            "--name",
            name,
            "--label",
            f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
            "-p",
            f"{self._host}::{RUNNER_CONTAINER_PORT}",
        ]
        # Container isolation flags (read-only rootfs + tmpfs, cap-drop ALL,
        # no-new-privileges, bounded pids/memory/cpu). The runner never receives
        # the Docker socket -- only the worker does -- so the daemon stays out of
        # reach; the data tier is denied by the runner's dedicated network (see
        # CURIE_DOCKER_NETWORK). This is local trusted-bundle defense-in-depth,
        # not the K8s security boundary.
        args += self._hardening.run_args()
        for key, value in (labels or {}).items():
            args += ["--label", f"{key}={value}"]
        if self._network:
            args += ["--network", self._network]

        # Bundle: no init containers here, so fetch + extract + bind-mount the
        # plugin dir ourselves (fail-open only when there is no ref, e.g. a fake
        # model run that never reads the plugin dir).
        if env.get(BUNDLE_REF_ENV):
            root = self._prepare_bundle(name, env[BUNDLE_REF_ENV])
            args += ["-v", f"{root}:{plugin_dir}:ro"]
        if env.get(WORKSPACE_REF_ENV):
            try:
                root = self._prepare_workspace(
                    name,
                    env[WORKSPACE_REF_ENV],
                    env.get(WORKSPACE_SHA256_ENV, ""),
                )
            except Exception:
                self._cleanup_bundle(name)
                raise
            args += ["-v", f"{root}:{WORKSPACE_MOUNT_PATH}:rw"]
        # Attachments: same reason as the bundle above -- no init containers
        # here, so this driver redeems the signed reference itself. Kubernetes
        # does this in `attachments-init`; without the equivalent, `curie local
        # up` and `curie skill` would park the bytes and hand the agent nothing,
        # which is a fabricated empty result at those tiers (ADR-0041) and the
        # very silent loss #2567 exists to close. Read-only: an attachment is an
        # input a person sent, not a scratch space.
        if env.get(ATTACHMENTS_REF_ENV):
            try:
                root = self._prepare_attachments(name, env[ATTACHMENTS_REF_ENV])
            except _NoAttachments:
                pass
            except Exception:
                self._cleanup_workspace(name)
                self._cleanup_bundle(name)
                raise
            else:
                args += ["-v", f"{root}:{ATTACHMENTS_MOUNT_PATH}:ro"]

        args += [
            "-e",
            f"{PLUGIN_DIR_ENV}={plugin_dir}",
            "-e",
            f"CURIE_SANDBOX_ID={name}",
            "-e",
            f"CURIE_RUNNER_PORT={RUNNER_CONTAINER_PORT}",
        ]
        if self._otel_endpoint:
            args += [
                "-e",
                f"{_OTEL_ENDPOINT_ENV}={self._otel_endpoint}",
                "-e",
                f"{_OTEL_PROTOCOL_ENV}=http/protobuf",
            ]
        for key, value in sorted(env.items()):
            if key not in _WORKER_OWNED_ENV:
                args += ["-e", f"{key}={value}"]
        # Forward exactly one model credential into the runner, always BY NAME (docker
        # reads the value from the worker env; the secret never lands in the argv). An
        # explicit CURIE_CREDENTIALS is the operator's chosen BYO credential and is
        # forwarded alone, so an ambient SDK token (leaked into the worker via compose's
        # .env auto-load or the operator shell) can neither shadow it nor ride into the
        # sandbox. With no BYO credential set, fall back to the ambient SDK credential(s)
        # for the legacy real-Anthropic path. Selection keys on the worker environ (the
        # by-name forward reads the value from there); an empty CURIE_CREDENTIALS is
        # treated as unset.
        # Suppress credentials the runner does not need, so a real token never sits
        # in a container that resolves none (recoverable via /proc/1/environ by a
        # same-uid agent):
        #   - fake-model run (CURIE_FAKE_MODEL): the runner authenticates nothing,
        #     so forward no credential at all.
        #   - base-URL-override / local run (ANTHROPIC_BASE_URL): drop the ambient
        #     SDK fallback -- a local endpoint needs no real Anthropic token -- and
        #     also drop an EXPLICIT CURIE_CREDENTIALS that is OAuth-shaped
        #     (sk-ant-oat): the runner blanks such a token under the override
        #     (runner sdk_auth.resolve_sdk_env), so forwarding it authenticates
        #     nothing and only lands a real token in /proc/1/environ (issue #603).
        #     A non-OAuth provider key (e.g. sk-or- OpenRouter) is still forwarded,
        #     because the runner routes it into ANTHROPIC_API_KEY even with a preset
        #     base URL; suppressing it would break BYO OpenRouter.
        fake_model = FAKE_MODEL_ENV in env
        base_url_override = BASE_URL_ENV in env
        if not fake_model:
            byo = self._environ.get(CREDENTIALS_ENV)
            drop_oauth_byo = (
                base_url_override and byo is not None and byo.startswith(_OAUTH_TOKEN_PREFIX)
            )
            if byo and not drop_oauth_byo:
                args += ["-e", CREDENTIALS_ENV]
            elif not byo and not base_url_override:
                for var in _SDK_PASSTHROUGH_ENV:
                    if var in self._environ:
                        args += ["-e", var]
        args.append(self._image)

        try:
            self._docker(args)
        except DockerError:
            # A failed boot must not leak staged claim data.
            self._cleanup_bundle(name)
            self._cleanup_workspace(name)
            self._cleanup_attachments(name)
            raise

    def get_claim(self, name: str) -> ClaimView | None:
        inspected = self._inspect(name)
        if inspected is None:
            return None
        status, _labels, created = inspected
        ready = status == "running" and self._healthz_ok(name)
        return ClaimView(
            name=name,
            ready=ready,
            # The container is its own sandbox; expose the name once it exists so
            # the substrate can advance to awaiting the dial target.
            sandbox_name=name,
            # The container's own creation instant is this tier's claim age: the
            # substrate runs one reaper over both tiers, so returning None here
            # would silently stop reaping local orphans.
            created_at=created,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
        )

    def delete_claim(self, name: str) -> None:
        # -f removes a running/paused container too; ignore "no such container".
        self._docker(["rm", "-f", name], check=False)
        self._cleanup_bundle(name)
        self._cleanup_workspace(name)
        self._cleanup_attachments(name)

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        out = self._docker(
            [
                "ps",
                "-a",
                "--filter",
                f"label={label_selector}",
                "--format",
                "{{.Names}}",
            ]
        )
        names = [line.strip() for line in out.splitlines() if line.strip()]
        views: list[ClaimView] = []
        for cname in names:
            view = self.get_claim(cname)
            if view is not None:
                views.append(view)
        return views

    # -- sandbox lifecycle ----------------------------------------------------

    def get_sandbox(self, name: str) -> SandboxView | None:
        inspected = self._inspect(name)
        if inspected is None:
            return None
        # A Sandbox has no age question; this unpacks three only because
        # get_claim shares the same single inspect call.
        status, _labels, _created = inspected
        # Only a running or paused container is a live/suspended sandbox. An
        # exited/dead/created/restarting container is NOT live: report it gone
        # (None) so the substrate evicts the stale route and re-claims rather than
        # handing back a route that keeps dialing a dead runner until TTL expiry.
        if status == "paused":
            operating_mode = "Suspended"
        elif status == "running":
            operating_mode = "Running"
        else:
            return None
        endpoint = self._dial_endpoint(name)
        return SandboxView(
            name=name,
            ready=status == "running",
            # Dial target is ready only once the runner is reachable: its
            # shared-network container IP (preferred) or the published host port.
            service_fqdn=endpoint[0] if endpoint is not None else None,
            operating_mode=operating_mode,
            port=endpoint[1] if endpoint is not None else None,
        )

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None:
        # Docker has no cold suspend; pause freezes the process while keeping the
        # published port, which is all the substrate's liveness check reads back.
        verb = "pause" if mode == "Suspended" else "unpause"
        self._docker([verb, name], check=False)

    # -- helpers --------------------------------------------------------------

    def _prepare_bundle(self, name: str, ref: str) -> str:
        data = self._bundles.get(ref)
        tmp = tempfile.mkdtemp(prefix="curie-bundle-")
        try:
            root = extract_bundle(
                data,
                Path(tmp),
                max_uncompressed_bytes=self._bundle_max_uncompressed_bytes,
                max_compression_ratio=self._bundle_max_compression_ratio,
                max_members=self._bundle_max_members,
            )
            # The mount is read-only and the runner is non-root (uid 1000), so the
            # staged bundle must be group/other readable+traversable; mkdtemp
            # defaults to 0700, which the non-root runner cannot enter.
            os.chmod(tmp, 0o755)
            for dirpath, dirnames, filenames in os.walk(tmp):
                for d in dirnames:
                    os.chmod(os.path.join(dirpath, d), 0o755)
                for f in filenames:
                    p = os.path.join(dirpath, f)
                    os.chmod(p, os.stat(p).st_mode | 0o044)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._bundle_dirs[name] = tmp
        return str(root)

    def _cleanup_bundle(self, name: str) -> None:
        tmp = self._bundle_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    def _prepare_workspace(self, name: str, encoded_ref: str, expected_sha256: str) -> str:
        """Fetch one signed object, verify it, and safely materialize /workspace."""

        reference = WorkspaceRef.decode(encoded_ref)
        if reference.expires_in_seconds <= 0:
            raise WorkspacePreparationError("workspace-fetch", "workspace reference expired")
        if expected_sha256 != reference.sha256:
            raise WorkspacePreparationError(
                "workspace-fetch", "claim digest disagrees with signed workspace reference"
            )
        tmp = tempfile.mkdtemp(prefix="curie-workspace-")
        archive_path = Path(tmp) / ".workspace-archive"
        root = Path(tmp) / "checkout"
        root.mkdir(mode=0o700)
        digest = hashlib.sha256()
        total = 0
        request = urllib.request.Request(reference.url, method="GET")
        try:
            with (
                urllib.request.urlopen(
                    request, timeout=self._workspace_limits.clone_timeout_seconds
                ) as response,
                archive_path.open("wb") as output,
            ):
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > self._workspace_limits.max_archive_bytes:
                        raise WorkspacePreparationError(
                            "workspace-fetch", "workspace archive exceeds compressed-size limit"
                        )
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != reference.sha256:
                raise WorkspacePreparationError(
                    "workspace-fetch", "workspace archive digest mismatch"
                )
            with archive_path.open("rb") as source:
                validate_workspace_archive(
                    iter(lambda: source.read(1024 * 1024), b""),
                    limits=self._workspace_limits,
                )
            with tarfile.open(archive_path, mode="r:*") as archive:
                archive.extractall(root, filter="data")
            archive_path.unlink(missing_ok=True)

            # Local Docker may run under a host uid different from the runner's
            # fixed uid 1000. The workspace is deliberately writable, so make
            # the bind mount writable independent of that host uid.
            os.chmod(root, 0o777)
            for dirpath, dirnames, filenames in os.walk(root):
                for dirname in dirnames:
                    os.chmod(os.path.join(dirpath, dirname), 0o777)
                for filename in filenames:
                    path = os.path.join(dirpath, filename)
                    os.chmod(path, os.stat(path).st_mode | 0o666)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._workspace_dirs[name] = tmp
        return str(root)

    def _cleanup_workspace(self, name: str) -> None:
        tmp = self._workspace_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    def _prepare_attachments(self, name: str, encoded_refs: str) -> str:
        """Redeem the signed references and materialize a read-only mount root.

        The Kubernetes twin of this is the ``attachments-init`` container, and
        the refusals are deliberately the same set: ``decode_attachment_refs``
        has already rejected a non-HTTP(S) url and a digest that is not real
        hex, and what is left to refuse here is an expired capability, a file
        that outgrows the cap mid-stream, a digest that does not match the bytes
        that arrived, and a name that would land outside the mount root.

        All or nothing, matching ``AttachmentCoordinator.resolve``: a partial set
        is indistinguishable to the agent from a complete one, so one refusal
        discards the whole directory rather than mounting the survivors.
        """

        refs = decode_attachment_refs(encoded_refs)
        if not refs:
            raise _NoAttachments
        tmp = tempfile.mkdtemp(prefix="curie-attachments-")
        root = Path(tmp) / "attachments"
        root.mkdir(mode=0o755)
        try:
            for ref in refs:
                if ref.expires_in_seconds <= 0:
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"reference for {ref.name!r} expired",
                    )
                # Refused before the join, not after: `Path("   ").name` is
                # `"   "`, and `Path("..").name` is `""`, so a containment check
                # alone would let a blank file through or silently target the
                # root itself. The init container makes the same refusal.
                leaf = Path(ref.name.strip()).name.strip()
                if not leaf or leaf in {".", ".."}:
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"name {ref.name!r} is unusable as a filename",
                    )
                destination = root / leaf
                if destination.resolve().parent != root.resolve():
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"name {ref.name!r} escapes the mount root",
                    )
                digest = hashlib.sha256()
                total = 0
                request = urllib.request.Request(ref.url, method="GET")
                with (
                    urllib.request.urlopen(
                        request, timeout=_ATTACHMENT_FETCH_TIMEOUT_S
                    ) as response,
                    destination.open("wb") as output,
                ):
                    while chunk := response.read(self._attachment_limits.read_chunk_bytes):
                        total += len(chunk)
                        if total > self._attachment_limits.max_file_bytes:
                            raise AttachmentTooLargeError(
                                ref.name, self._attachment_limits.max_file_bytes
                            )
                        digest.update(chunk)
                        output.write(chunk)
                if digest.hexdigest() != ref.sha256:
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"{ref.name!r} digest mismatch",
                    )
                # The runner runs as uid 1000 and the mount is read-only, so the
                # bytes only need to be readable; unlike the workspace they are
                # never written back.
                os.chmod(destination, 0o644)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._attachment_dirs[name] = tmp
        return str(root)

    def _cleanup_attachments(self, name: str) -> None:
        tmp = self._attachment_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    def _inspect(self, name: str) -> tuple[str, dict[str, str], datetime | None] | None:
        """(status, labels, created) for the container, or None when it does not
        exist.

        One docker call carries all three: the creation instant rides along with
        the status read rather than costing a second inspect per poll.
        """
        out = self._docker(
            [
                "inspect",
                "--format",
                "{{.State.Status}}\t{{json .Config.Labels}}\t{{.Created}}",
                name,
            ],
            check=False,
        )
        if not out.strip():
            return None
        status, _, rest = out.strip().partition("\t")
        labels_json, _, created_raw = rest.partition("\t")
        labels_raw = json.loads(labels_json) if labels_json and labels_json != "null" else {}
        labels = {str(k): str(v) for k, v in labels_raw.items()}
        return status, labels, _parse_created(created_raw)

    def _published_port(self, name: str) -> int | None:
        out = self._docker(["port", name, f"{RUNNER_CONTAINER_PORT}/tcp"], check=False)
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            # e.g. "127.0.0.1:49153" or "0.0.0.0:49153"; take the trailing port.
            _, _, port = line.rpartition(":")
            if port.isdigit():
                return int(port)
        return None

    def _container_ip(self, name: str) -> str | None:
        """The runner's IP on the configured docker network, or None.

        A host-networked worker *container* on Docker Desktop (macOS/Windows)
        shares the Docker VM's network namespace, where the runner's port
        published on the *host* loopback is unreachable but its bridge-network
        IP is. Dialing the container directly on the shared network is reachable
        across all three worker topologies (a real host-process worker, a
        native-Linux host-net worker, and a Docker Desktop host-net worker), so
        it is preferred whenever a network is configured. Returns None when no
        network is set or the address is not yet assigned.
        """
        if not self._network:
            return None
        out = self._docker(
            ["inspect", "--format", "{{json .NetworkSettings.Networks}}", name],
            check=False,
        )
        if not out.strip():
            return None
        try:
            nets = json.loads(out)
        except json.JSONDecodeError:
            return None
        if not isinstance(nets, dict) or not nets:
            return None
        entry = nets.get(self._network)
        if not isinstance(entry, dict):
            # Fall back to whatever single network the container joined.
            entry = next(iter(nets.values()), None)
        ip = entry.get("IPAddress") if isinstance(entry, dict) else None
        return ip or None

    def _dial_endpoint(self, name: str) -> tuple[str, int] | None:
        """(host, port) the worker reaches the runner on, or None if not yet
        dialable. Prefer the container's shared-network address (reachable from
        a Docker Desktop VM netns); fall back to the Docker-assigned loopback
        host port for a host-process worker with no shared network."""
        ip = self._container_ip(name)
        if ip is not None:
            return ip, RUNNER_CONTAINER_PORT
        port = self._published_port(name)
        if port is None:
            return None
        return self._host, port

    def _healthz_ok(self, name: str) -> bool:
        endpoint = self._dial_endpoint(name)
        if endpoint is None:
            return False
        host, port = endpoint
        url = f"http://{host}:{port}/healthz"
        try:
            with urllib.request.urlopen(url, timeout=self._healthz_timeout_s) as resp:
                return bool(200 <= resp.status < 300)
        except (urllib.error.URLError, OSError):
            return False

    def _docker(self, args: list[str], *, check: bool = True) -> str:
        proc = subprocess.run(  # noqa: S603 -- fixed argv, no shell.
            ["docker", *args],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            if check:
                stderr = proc.stderr.strip()
                # Never echo args: they may carry CURIE_CREDENTIALS.
                raise DockerError(
                    f"docker {args[0]} failed ({proc.returncode}): {stderr}"
                    f"{self._network_remediation_hint(stderr)}"
                )
            return ""
        return proc.stdout

    def _network_remediation_hint(self, stderr: str) -> str:
        """An actionable one-liner appended when ``stderr`` names our own
        configured docker network as missing (#715).

        A stack whose compose topology has drifted (e.g. a service was
        recreated individually, bypassing `docker compose up`'s network
        reconciliation) surfaces this as a bare `docker: Error response from
        daemon: failed to set up container networking: network <name> not
        found` with nothing to act on beyond raw container logs -- exactly
        what escalated as an opaque runner-error while getting a real agent
        working locally. This does not auto-heal (the substrate has no compose
        project to reconcile from); it only turns the dead end into a next
        step for whoever is reading the worker's logs."""
        if (
            self._network
            and "network" in stderr
            and self._network in stderr
            and "not found" in stderr
        ):
            return (
                " -- the docker network the sandbox substrate expects"
                f" ({self._network!r}) does not exist; run `curie local up`"
                " (or the cluster/compose equivalent) to reconcile the stack's"
                " topology, then retry"
            )
        return ""

    def ensure_image(self) -> None:
        """Pre-pull the runner image if absent (IfNotPresent semantics).

        Mirrors the K8s prewarm: run once at worker startup so the first claim
        window never contains a cold image download. Best-effort -- a pull
        failure (offline, registry down) warns and continues rather than
        crashing the worker, and a truly-missing image still fails clearly
        later at claim time.
        """
        try:
            present = self._docker(["image", "inspect", self._image], check=False)
            if present.strip():
                return
            self._docker(["pull", self._image])
        except (DockerError, OSError):
            # Log the image name only -- never the argv or stderr (house rule
            # above: args may carry credentials).
            logger.warning(
                "runner image pre-pull failed for %s; first claim will pull implicitly",
                self._image,
            )
