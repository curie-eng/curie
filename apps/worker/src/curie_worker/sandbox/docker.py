"""Docker access for the sandbox substrate: local middle mode, no Kubernetes.

``DockerSandboxClient`` implements the same ``SandboxClient`` seam
(``apps/worker/src/curie_worker/sandbox/k8s.py``) that ``SandboxSubstrate`` is
written against, so selecting it (``CURIE_SANDBOX_SUBSTRATE=docker``) makes the
whole worker -- both the runs kernel and the eval consumer, which share one
substrate object -- boot runner containers with ``docker create`` and ``docker start`` instead of
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
pair -- the worker fetches the plugin bundle from RustFS and copies it into a
claim-owned Docker volume. The
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

import json
import logging
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
)
from ..binding import (
    BASE_URL_ENV,
    BUNDLE_REF_ENV,
    CREDENTIALS_ENV,
    FAKE_MODEL_ENV,
    PLUGIN_DIR_ENV,
)
from ..bundle_store import BundleReader
from ..workspace import (
    WORKSPACE_MOUNT_PATH,
    WORKSPACE_REF_ENV,
    WORKSPACE_SHA256_ENV,
    WorkspaceLimits,
)
from .claim_tokens import executor_claim_env
from .staging import WORKER_OWNED_ENV, ClaimInputStaging, NoAttachments
from .types import (
    BUNDLE_REF_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    CapacityInventory,
    ClaimView,
    DaemonObjectKind,
    DaemonObjectView,
    ListableDaemonKind,
    OperatingMode,
    QuotaRejection,
    QuotaStatus,
    SandboxError,
    SandboxTermination,
    SandboxView,
    ToolDaemonMount,
    filter_agent_child_env,
)

_NO_TOOL_DAEMON = "the Docker substrate runs no per-claim tool daemon (ADR 0217)"


logger = logging.getLogger(__name__)

# The runner listens on this fixed port inside every container; the host port it
# is published to is Docker-assigned and read back per-container.
RUNNER_CONTAINER_PORT = 8080
_DOCKER_CONTROL_TIMEOUT_S = 30.0
_DOCKER_CREATE_TIMEOUT_S = 90.0
_DOCKER_IMAGE_PULL_TIMEOUT_S = 300.0
_LIST_INSPECTION_TIMEOUT_S = 30.0
_CLAIM_LABEL = "curietech.ai/claim"
_COMPONENT_LABEL = "curietech.ai/component"
_ATTEMPT_LABEL = "curietech.ai/claim-attempt"
_STACK_LABEL = "curietech.ai/stack"
_INPUT_COMPONENTS = ("claim-bundle", "claim-workspace", "claim-attachments")
_INPUT_SEED_LIFETIME_S = 1800
_SEED_WORKSPACE = """import os
import stat
def fail(error):
    raise error
def prepare(path):
    os.chown(path, 0, 0, follow_symlinks=False)
    entry = os.lstat(path)
    if not stat.S_ISLNK(entry.st_mode):
        mode = 0o700 if stat.S_ISDIR(entry.st_mode) else 0o600
        os.chmod(path, entry.st_mode | mode, follow_symlinks=False)
paths = ['/curie-input']
prepare('/curie-input')
for root, directories, files in os.walk('/curie-input', topdown=True, onerror=fail):
    for name in directories + files:
        path = os.path.join(root, name)
        prepare(path)
        paths.append(path)
for path in reversed(paths):
    os.chown(path, 1000, 1000, follow_symlinks=False)
"""

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
# cli/src/commands/credential_env.rs).
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


def _without_flags(args: list[str], flags: tuple[str, ...]) -> list[str]:
    """Drop ``flags`` and the value that follows each one. Other flags stay."""

    kept: list[str] = []
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg in flags:
            skip = True
            continue
        kept.append(arg)
    return kept


class DockerError(SandboxError):
    """A ``docker`` CLI invocation failed.

    Subclasses ``SandboxError`` so a provisioning failure (missing runner image,
    Docker daemon down) is handled by the kernel's runner-error retry path and
    the eval consumer's provisioning-failure path, rather than escaping unhandled
    and looping on reclaim.
    """


class DockerSandboxClient(ClaimInputStaging):
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
        attachment_cache_size_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        self._image = image
        self._network = network
        self._stack = network or "default"
        self._otel_endpoint = otel_endpoint
        self._host = host
        self._default_plugin_dir = default_plugin_dir
        self._healthz_timeout_s = healthz_timeout_s
        self._init_staging(
            bundle_store=bundle_store,
            bundle_max_uncompressed_bytes=bundle_max_uncompressed_bytes,
            bundle_max_compression_ratio=bundle_max_compression_ratio,
            bundle_max_members=bundle_max_members,
            workspace_limits=workspace_limits,
            attachment_limits=attachment_limits,
        )
        # Container isolation (read-only rootfs, dropped caps, no-new-privileges,
        # bounded resources). Defaults to the K8s-mirroring RunnerHardening; a
        # None means "use the on-by-default set", never "no hardening".
        self._hardening = hardening if hardening is not None else RunnerHardening()
        # Presence-only view of the worker environment, used to decide which SDK
        # credential vars to forward by name. Never read for their values.
        self._environ = environ if environ is not None else os.environ
        if attachment_cache_size_bytes <= 0:
            raise ValueError("attachment cache capacity must be positive")
        self._attachment_cache_size_bytes = attachment_cache_size_bytes

    # -- claim lifecycle ------------------------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,  # noqa: ARG002 -- no warm pool in Docker; kept for the seam.
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, Any] | None = None,
        deadline: float,  # noqa: ARG002 -- each docker call carries its own timeout.
        agent_name: str | None = None,  # noqa: ARG002 -- naming is a cluster concern.
        executor_secret_names: frozenset[str] | None = None,
        tool_daemon: ToolDaemonMount | None = None,
    ) -> None:
        if tool_daemon is not None:
            raise SandboxError(_NO_TOOL_DAEMON)
        env = filter_agent_child_env(env)
        executor = executor_secret_names is not None
        if executor_secret_names is not None:
            env = executor_claim_env(env, executor_secret_names)
        attempt = uuid.uuid4().hex
        plugin_dir = env.get(PLUGIN_DIR_ENV, self._default_plugin_dir)
        args = [
            "create",
            "--name",
            name,
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
        if runner_resources is not None:
            from .resources import docker_limit_args

            args = _without_flags(args, ("--memory", "--cpus"))
            args += docker_limit_args(
                runner_resources,
                self._hardening.memory_limit,
                self._hardening.cpu_limit,
            )
        for key, value in (labels or {}).items():
            args += ["--label", f"{key}={value}"]
        args += [
            "--label",
            f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
            "--label",
            f"{_CLAIM_LABEL}={name}",
            "--label",
            f"{_COMPONENT_LABEL}=claim-runner",
            "--label",
            f"{_ATTEMPT_LABEL}={attempt}",
            "--label",
            f"{_STACK_LABEL}={self._stack}",
        ]
        bundle_ref = env.get(BUNDLE_REF_ENV)
        if bundle_ref:
            args += ["--label", f"{BUNDLE_REF_LABEL}={bundle_ref}"]
        if self._network:
            args += ["--network", self._network]

        inputs: list[tuple[str, str, str, bool]] = []
        try:
            if env.get(BUNDLE_REF_ENV):
                root = self._prepare_bundle(attempt, env[BUNDLE_REF_ENV])
                inputs.append(("claim-bundle", root, plugin_dir, False))
            if env.get(WORKSPACE_REF_ENV):
                root = self._prepare_workspace(
                    attempt,
                    env[WORKSPACE_REF_ENV],
                    env.get(WORKSPACE_SHA256_ENV, ""),
                )
                inputs.append(("claim-workspace", root, WORKSPACE_MOUNT_PATH, True))
            if env.get(BootEnv.env_key("attachment_access_enabled")) == "1":
                # An empty writable claim-owned cache. No provider/object store
                # read occurs while booting lazy mode. Existing seed/ownership
                # fencing and cleanup apply to this named volume unchanged.
                tmp = tempfile.mkdtemp(prefix="curie-attachments-")
                cache_root = Path(tmp) / "attachments"
                cache_root.mkdir(mode=0o700)
                self._attachment_dirs[attempt] = tmp
                inputs.append(("claim-attachments", str(cache_root), ATTACHMENTS_MOUNT_PATH, True))
            elif env.get(ATTACHMENTS_REF_ENV):
                try:
                    root = self._prepare_attachments(attempt, env[ATTACHMENTS_REF_ENV])
                except NoAttachments:
                    pass
                else:
                    inputs.append(("claim-attachments", root, ATTACHMENTS_MOUNT_PATH, False))
        except BaseException:
            self._cleanup_staged_inputs(attempt)
            raise

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
            if key not in WORKER_OWNED_ENV:
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
        # An executor runner calls one connector tool and never the model
        # (@spec ACTION-EXECUTOR-5), so it is forwarded no model credential.
        if not fake_model and not executor:
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
        volumes: list[tuple[str, str, str]] = []
        try:
            for component, _root, destination, writable in inputs:
                volume, helper = self._allocate_input_volume(
                    name,
                    component,
                    attempt=attempt,
                    cache_capacity=(
                        self._attachment_cache_size_bytes
                        if component == "claim-attachments"
                        and env.get(BootEnv.env_key("attachment_access_enabled")) == "1"
                        else None
                    ),
                )
                volumes.append((component, volume, helper))
                mode = "rw" if writable else "ro"
                args += ["-v", f"{volume}:{destination}:{mode}"]
            args.append(self._image)
            runner = self._docker(
                args,
                request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
            ).strip()
            if not runner:
                raise DockerError("docker create returned no runner identity")
            for (component, root, _destination, writable), (_, volume, helper) in zip(
                inputs, volumes, strict=True
            ):
                self._validate_input_volume(name, component, volume, attempt=attempt)
                self._seed_input_volume(helper, root, writable=writable)
            self._cleanup_staged_inputs(attempt)
            for component, volume, helper in volumes:
                self._validate_input_volume(name, component, volume, attempt=attempt)
                self._validate_seed_helper(name, component, helper, attempt=attempt)
            inspected = self._inspect(runner, deadline=time.monotonic() + _DOCKER_CONTROL_TIMEOUT_S)
            if inspected is None or not self._owns_resource(
                inspected[1], name=name, component="claim-runner", attempt=attempt
            ):
                raise DockerError("runner ownership changed before start")
            self._docker(["start", runner], request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S)
            for _component, _volume, helper in volumes:
                self._docker(
                    ["rm", "-f", helper], request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S
                )
        except BaseException:
            self._cleanup_claim_inputs(
                name,
                request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
                attempt=attempt,
                include_runner=True,
            )
            raise

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None:
        deadline = time.monotonic() + request_timeout_seconds
        inspected = self._inspect(name, deadline=deadline)
        if inspected is None:
            return None
        status, labels, created, identifier = inspected
        if not self._owns_resource(labels, name=name, component="claim-runner"):
            return None
        attempt = labels.get(_ATTEMPT_LABEL)
        if (
            status == "created"
            and attempt
            and self._attempt_is_seeding(name, attempt, deadline=deadline)
        ):
            return None
        ready = status == "running" and self._healthz_ok(identifier, deadline=deadline)
        return ClaimView(
            name=name,
            ready=ready,
            # The container is its own sandbox; expose the name once it exists so
            # the substrate can advance to awaiting the dial target.
            sandbox_name=name,
            # The bind window starts after seeding, when the runner starts.
            created_at=created,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
            bundle_ref=labels.get(BUNDLE_REF_LABEL) or None,
        )

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        self._cleanup_claim_inputs(
            name, request_timeout_seconds=request_timeout_seconds, include_runner=True
        )

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        out = self._docker(
            [
                "ps",
                "-a",
                "--filter",
                f"label={MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                "--filter",
                f"label={_STACK_LABEL}={self._stack}",
                "--filter",
                f"label={_COMPONENT_LABEL}=claim-runner",
                "--filter",
                f"label={label_selector}",
                "--format",
                "{{.Names}}",
            ],
            request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
        )
        names = [line.strip() for line in out.splitlines() if line.strip()]
        views: list[ClaimView] = []
        for cname in names:
            view = self.get_claim(cname, request_timeout_seconds=_LIST_INSPECTION_TIMEOUT_S)
            if view is not None:
                views.append(view)
        return views

    # The Docker substrate has no repository runner pools, so the substrate
    # never provisions a per-claim tool daemon here (ADR 0217).

    def create_daemon_object(
        self, namespace: str, manifest: Mapping[str, Any], *, request_timeout_seconds: float
    ) -> str:
        raise SandboxError(_NO_TOOL_DAEMON)

    def list_daemon_objects(
        self,
        namespace: str,
        kind: ListableDaemonKind,
        *,
        label_selector: str,
        request_timeout_seconds: float,
    ) -> list[DaemonObjectView]:
        raise SandboxError(_NO_TOOL_DAEMON)

    def delete_daemon_object(
        self,
        namespace: str,
        kind: DaemonObjectKind,
        name: str,
        *,
        uid: str,
        request_timeout_seconds: float,
    ) -> None:
        raise SandboxError(_NO_TOOL_DAEMON)

    def get_daemon_pod(
        self, namespace: str, name: str, *, request_timeout_seconds: float
    ) -> DaemonObjectView | None:
        raise SandboxError(_NO_TOOL_DAEMON)

    def open_daemon_stream(
        self, namespace: str, pod: str, port: int, *, timeout_seconds: float
    ) -> AbstractContextManager[socket.socket]:
        raise SandboxError(_NO_TOOL_DAEMON)

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        removed: list[str] = []
        removed_attempts: set[str] = set()
        for component in _INPUT_COMPONENTS:
            volumes = self._docker(
                [
                    "volume",
                    "ls",
                    "--filter",
                    f"label={MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                    "--filter",
                    f"label={_STACK_LABEL}={self._stack}",
                    "--filter",
                    f"label={_COMPONENT_LABEL}={component}",
                    "--format",
                    "{{.Name}}",
                ],
                request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
            )
            for volume in volumes.splitlines():
                if not volume.strip():
                    continue
                inspected = self._docker(
                    [
                        "volume",
                        "inspect",
                        "--format",
                        "{{json .Labels}}\t{{.CreatedAt}}",
                        volume.strip(),
                    ],
                    request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
                    check=False,
                )
                labels_json, _, created_raw = inspected.strip().partition("\t")
                if not labels_json:
                    continue
                labels = json.loads(labels_json)
                if not isinstance(labels, dict):
                    continue
                claim = labels.get(_CLAIM_LABEL)
                attempt = labels.get(_ATTEMPT_LABEL)
                created = _parse_created(created_raw)
                if (
                    labels.get(MANAGED_BY_LABEL) != MANAGED_BY_VALUE
                    or labels.get(_STACK_LABEL) != self._stack
                    or labels.get(_COMPONENT_LABEL) != component
                    or not isinstance(claim, str)
                    or not claim
                    or not isinstance(attempt, str)
                    or not attempt
                    or claim in keep
                    or attempt in removed_attempts
                    or created is None
                    or created >= created_before
                ):
                    continue
                if self._attempt_is_seeding(
                    claim, attempt, deadline=time.monotonic() + _DOCKER_CONTROL_TIMEOUT_S
                ):
                    continue
                runner = self._inspect(claim, deadline=time.monotonic() + _DOCKER_CONTROL_TIMEOUT_S)
                if (
                    runner is not None
                    and self._owns_resource(
                        runner[1], name=claim, component="claim-runner", attempt=attempt
                    )
                    and (runner[2] is None or runner[2] >= created_before)
                ):
                    continue
                removed_attempts.add(attempt)
                try:
                    self._cleanup_claim_inputs(
                        claim,
                        request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
                        attempt=attempt,
                        include_runner=True,
                    )
                except DockerError:
                    logger.warning("orphaned claim input cleanup failed; retrying next sweep")
                    continue
                if claim not in removed:
                    removed.append(claim)
        return removed

    # -- sandbox lifecycle ----------------------------------------------------

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        deadline = time.monotonic() + request_timeout_seconds
        inspected = self._inspect(name, deadline=deadline)
        if inspected is None:
            return None
        # A Sandbox has no age question; this unpacks four only because
        # get_claim shares the same single inspect call.
        status, labels, _created, identifier = inspected
        if not self._owns_resource(labels, name=name, component="claim-runner"):
            return None
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
        endpoint = self._dial_endpoint(identifier, deadline=deadline)
        return SandboxView(
            name=name,
            ready=status == "running",
            # Dial target is ready only once the runner is reachable: its
            # shared-network container IP (preferred) or the published host port.
            service_fqdn=endpoint[0] if endpoint is not None else None,
            operating_mode=operating_mode,
            port=endpoint[1] if endpoint is not None else None,
            # Loopback or the shared local network: plain HTTP at the root.
            transport="http",
        )

    def quota_has_headroom(
        self,
        rejection: QuotaRejection,
        *,
        request_timeout_seconds: float,
    ) -> bool:
        del rejection, request_timeout_seconds
        return False

    def claim_resources(
        self,
        *,
        pool: str,
        runner_resources: dict[str, object] | None,
        agent_name: str | None,
        executor_secret_names: frozenset[str] | None,
    ) -> dict[str, str]:
        raise ValueError("Docker substrate does not support Kubernetes quota admission")

    def capacity_inventory(
        self,
        *,
        priority_class: str,
        known_claim_resources: Mapping[str, Mapping[str, str]],
        request_timeout_seconds: float,
    ) -> CapacityInventory:
        raise ValueError("Docker substrate does not support Kubernetes quota admission")

    def read_quota(
        self, quota_name: str, *, request_timeout_seconds: float
    ) -> QuotaStatus:
        raise ValueError("Docker substrate does not support Kubernetes quota admission")

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        # A Docker runner has no scheduler; it starts or fails.
        del name, request_timeout_seconds
        return None

    def pod_termination(
        self, name: str, *, since: datetime, request_timeout_seconds: float
    ) -> SandboxTermination | None:
        # Docker has no Kubernetes pod status or events.
        del name, since, request_timeout_seconds
        return None

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None:
        # Docker has no cold suspend; pause freezes the process while keeping the
        # published port, which is all the substrate's liveness check reads back.
        verb = "pause" if mode == "Suspended" else "unpause"
        inspected = self._inspect(name, deadline=time.monotonic() + _DOCKER_CONTROL_TIMEOUT_S)
        if inspected is None or not self._owns_resource(
            inspected[1], name=name, component="claim-runner"
        ):
            return
        self._docker(
            [verb, inspected[3]],
            request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
            check=False,
        )

    # -- helpers --------------------------------------------------------------

    def _resource_labels(self, name: str, component: str, attempt: str) -> dict[str, str]:
        return {
            MANAGED_BY_LABEL: MANAGED_BY_VALUE,
            _STACK_LABEL: self._stack,
            _CLAIM_LABEL: name,
            _COMPONENT_LABEL: component,
            _ATTEMPT_LABEL: attempt,
        }

    def _owns_resource(
        self,
        labels: Mapping[str, str],
        *,
        name: str,
        component: str,
        attempt: str | None = None,
    ) -> bool:
        expected = self._resource_labels(name, component, attempt or "")
        if attempt is None:
            del expected[_ATTEMPT_LABEL]
        return all(labels.get(key) == value for key, value in expected.items())

    def _allocate_input_volume(
        self, name: str, component: str, *, attempt: str, cache_capacity: int | None = None
    ) -> tuple[str, str]:
        volume = f"curie-input-{uuid.uuid4().hex}"
        helper = f"{volume}-seed"
        labels = [
            argument
            for key, value in self._resource_labels(name, component, attempt).items()
            for argument in ("--label", f"{key}={value}")
        ]
        capacity_options = (
            []
            if cache_capacity is None
            else [
                "--opt",
                "type=tmpfs",
                "--opt",
                "device=tmpfs",
                "--opt",
                f"o=size={cache_capacity},uid=1000,gid=1000,mode=0700",
            ]
        )
        self._docker(
            ["volume", "create", *labels, *capacity_options, volume],
            request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
        )
        helper_id = self._docker(
            [
                "create",
                "--name",
                helper,
                *labels,
                "--network",
                "none",
                "--user",
                "0:0",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--cap-add",
                "CHOWN",
                "--security-opt",
                "no-new-privileges",
                "--mount",
                f"type=volume,source={volume},target=/curie-input,volume-nocopy",
                "--entrypoint",
                "python",
                self._image,
                "-c",
                f"import time; time.sleep({_INPUT_SEED_LIFETIME_S})",
            ],
            request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
        ).strip()
        if not helper_id:
            raise DockerError("docker create returned no input helper identity")
        self._docker(["start", helper_id], request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S)
        self._validate_input_volume(name, component, volume, attempt=attempt)
        self._validate_seed_helper(name, component, helper_id, attempt=attempt)
        return volume, helper_id

    def _validate_input_volume(
        self, name: str, component: str, volume: str, *, attempt: str
    ) -> None:
        inspected = self._docker(
            ["volume", "inspect", "--format", "{{json .Labels}}", volume],
            request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
        )
        try:
            labels = json.loads(inspected)
        except (TypeError, ValueError) as exc:
            raise DockerError("claim input volume has invalid ownership labels") from exc
        if labels != self._resource_labels(name, component, attempt):
            raise DockerError("claim input volume ownership changed before runner start")

    def _validate_seed_helper(
        self, name: str, component: str, helper: str, *, attempt: str
    ) -> None:
        inspected = self._inspect(helper, deadline=time.monotonic() + _DOCKER_CONTROL_TIMEOUT_S)
        if (
            inspected is None
            or inspected[0] != "running"
            or not self._owns_resource(
                inspected[1], name=name, component=component, attempt=attempt
            )
        ):
            raise DockerError("claim input helper is no longer active")

    def _seed_input_volume(self, helper: str, root: str, *, writable: bool = False) -> None:
        self._docker(
            ["cp", f"{root}/.", f"{helper}:/curie-input"],
            request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
        )
        if writable:
            self._docker(
                ["exec", helper, "python", "-c", _SEED_WORKSPACE],
                request_timeout_seconds=_DOCKER_CREATE_TIMEOUT_S,
            )

    def _attempt_is_seeding(self, name: str, attempt: str, *, deadline: float) -> bool:
        runner = self._inspect(name, deadline=deadline)
        if (
            runner is not None
            and self._owns_resource(runner[1], name=name, component="claim-runner", attempt=attempt)
            and runner[0] != "created"
        ):
            return False
        helpers = self._docker(
            [
                "ps",
                "--filter",
                f"label={MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                "--filter",
                f"label={_STACK_LABEL}={self._stack}",
                "--filter",
                f"label={_CLAIM_LABEL}={name}",
                "--filter",
                f"label={_ATTEMPT_LABEL}={attempt}",
                "--filter",
                "status=running",
                "--format",
                "{{.ID}}",
            ],
            request_timeout_seconds=self._remaining_seconds(deadline),
        )
        for helper in helpers.splitlines():
            inspected = self._inspect(helper.strip(), deadline=deadline)
            if inspected is None or inspected[0] != "running":
                continue
            component = inspected[1].get(_COMPONENT_LABEL)
            if (
                component is not None
                and component in _INPUT_COMPONENTS
                and self._owns_resource(
                    inspected[1], name=name, component=component, attempt=attempt
                )
            ):
                return True
        return False

    def _cleanup_claim_inputs(
        self,
        name: str,
        *,
        request_timeout_seconds: float,
        attempt: str | None = None,
        include_runner: bool = False,
    ) -> None:
        deadline = time.monotonic() + request_timeout_seconds
        failures: list[str] = []
        try:
            components = (("claim-runner",) if include_runner else ()) + _INPUT_COMPONENTS
            for component in components:
                filters = [
                    "--filter",
                    f"label={MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                    "--filter",
                    f"label={_STACK_LABEL}={self._stack}",
                    "--filter",
                    f"label={_CLAIM_LABEL}={name}",
                    "--filter",
                    f"label={_COMPONENT_LABEL}={component}",
                ]
                if attempt is not None:
                    filters += ["--filter", f"label={_ATTEMPT_LABEL}={attempt}"]
                operations = [(["ps", "-a", *filters, "--format", "{{.ID}}"], ["rm", "-f"])]
                if component != "claim-runner":
                    operations.append(
                        (
                            ["volume", "ls", *filters, "--format", "{{.Name}}"],
                            ["volume", "rm"],
                        )
                    )
                for listing, removal in operations:
                    try:
                        resources = self._docker(
                            listing, request_timeout_seconds=self._remaining_seconds(deadline)
                        )
                    except DockerError as exc:
                        failures.append(str(exc))
                        continue
                    for resource in resources.splitlines():
                        if not resource.strip():
                            continue
                        try:
                            resource = resource.strip()
                            if removal[0] == "rm":
                                inspected = self._inspect(resource, deadline=deadline)
                                if inspected is None or not self._owns_resource(
                                    inspected[1],
                                    name=name,
                                    component=component,
                                    attempt=attempt,
                                ):
                                    continue
                                resource = inspected[3]
                            else:
                                inspected_labels = self._docker(
                                    [
                                        "volume",
                                        "inspect",
                                        "--format",
                                        "{{json .Labels}}",
                                        resource,
                                    ],
                                    request_timeout_seconds=self._remaining_seconds(deadline),
                                    check=False,
                                )
                                if not inspected_labels.strip():
                                    continue
                                volume_labels = json.loads(inspected_labels)
                                if not isinstance(volume_labels, dict) or not self._owns_resource(
                                    volume_labels,
                                    name=name,
                                    component=component,
                                    attempt=attempt,
                                ):
                                    continue
                            self._docker(
                                [*removal, resource],
                                request_timeout_seconds=self._remaining_seconds(deadline),
                            )
                        except DockerError as exc:
                            failures.append(str(exc))
        finally:
            if attempt is not None:
                self._cleanup_staged_inputs(attempt)
        if failures:
            raise DockerError("claim input cleanup failed: " + "; ".join(failures))

    def _inspect(
        self, name: str, *, deadline: float
    ) -> tuple[str, dict[str, str], datetime | None, str] | None:
        """(status, labels, age origin, id), or None when the container does not
        exist.

        Starting the runner begins its bind window after input seeding. The id
        keeps subsequent actions on the inspected container across name reuse.
        """
        out = self._docker(
            [
                "inspect",
                "--format",
                "{{.State.Status}}\t{{json .Config.Labels}}\t{{.Created}}"
                "\t{{.Id}}\t{{.State.StartedAt}}",
                name,
            ],
            request_timeout_seconds=self._remaining_seconds(deadline),
            check=False,
        )
        if not out.strip():
            return None
        status, _, rest = out.strip().partition("\t")
        labels_json, _, rest = rest.partition("\t")
        created_raw, _, rest = rest.partition("\t")
        identifier, _, started_raw = rest.partition("\t")
        labels_raw = json.loads(labels_json) if labels_json and labels_json != "null" else {}
        labels = {str(k): str(v) for k, v in labels_raw.items()}
        if not identifier:
            raise DockerError("docker inspect returned no container identity")
        created = _parse_created(created_raw)
        started = _parse_created(started_raw)
        if started is not None and (created is None or started > created):
            created = started
        return status, labels, created, identifier

    def _published_port(self, name: str, *, deadline: float) -> int | None:
        out = self._docker(
            ["port", name, f"{RUNNER_CONTAINER_PORT}/tcp"],
            request_timeout_seconds=self._remaining_seconds(deadline),
            check=False,
        )
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            # e.g. "127.0.0.1:49153" or "0.0.0.0:49153"; take the trailing port.
            _, _, port = line.rpartition(":")
            if port.isdigit():
                return int(port)
        return None

    def _container_ip(self, name: str, *, deadline: float) -> str | None:
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
            request_timeout_seconds=self._remaining_seconds(deadline),
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

    def _dial_endpoint(self, name: str, *, deadline: float) -> tuple[str, int] | None:
        """(host, port) the worker reaches the runner on, or None if not yet
        dialable. Prefer the container's shared-network address (reachable from
        a Docker Desktop VM netns); fall back to the Docker-assigned loopback
        host port for a host-process worker with no shared network."""
        ip = self._container_ip(name, deadline=deadline)
        if ip is not None:
            return ip, RUNNER_CONTAINER_PORT
        port = self._published_port(name, deadline=deadline)
        if port is None:
            return None
        return self._host, port

    def _healthz_ok(self, name: str, *, deadline: float) -> bool:
        endpoint = self._dial_endpoint(name, deadline=deadline)
        if endpoint is None:
            return False
        host, port = endpoint
        url = f"http://{host}:{port}/healthz"
        try:
            remaining = self._remaining_seconds(deadline)
            timeout = min(self._healthz_timeout_s, remaining)
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return bool(200 <= resp.status < 300)
        except (urllib.error.URLError, OSError):
            return False

    @staticmethod
    def _remaining_seconds(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DockerError("docker request deadline expired")
        return remaining

    def _docker(
        self,
        args: list[str],
        *,
        request_timeout_seconds: float,
        check: bool = True,
    ) -> str:
        try:
            proc = subprocess.run(  # noqa: S603 -- fixed argv, no shell.
                ["docker", *args],
                capture_output=True,
                text=True,
                check=False,
                timeout=request_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            # Never echo args: they may carry CURIE_CREDENTIALS.
            raise DockerError(f"docker {args[0]} exceeded its request timeout") from exc
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
            present = self._docker(
                ["image", "inspect", self._image],
                request_timeout_seconds=_DOCKER_CONTROL_TIMEOUT_S,
                check=False,
            )
            if present.strip():
                return
            self._docker(
                ["pull", self._image],
                request_timeout_seconds=_DOCKER_IMAGE_PULL_TIMEOUT_S,
            )
        except (DockerError, OSError):
            # Log the image name only -- never the argv or stderr (house rule
            # above: args may carry credentials).
            logger.warning(
                "runner image pre-pull failed for %s; first claim will pull implicitly",
                self._image,
            )
