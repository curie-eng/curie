#!/usr/bin/env python3
"""Drive real HTTP identity turns through the Docker sandbox substrate.

The Kubernetes harness (``identity_e2e.py``) proves who a turn ran as on a
cluster install. This backend proves the same through the product's local
tier: an isolated ``curie local up`` stack whose worker runs every turn in a
runner container on the Docker daemon (``CURIE_SANDBOX_SUBSTRATE=docker``).

Actions (``python3 tools/identity-e2e/identity_compose.py <action> --project P
--state-dir DIR``):

1. ``build`` builds candidate api, worker and runner images from a source
   tarball or directory (``--build-context``) under ``--image-repo-prefix``.
2. ``up`` writes a compose override into the state directory, brings the
   isolated stack up with ``curie local up --minimal`` (project ``P``, the base
   ``compose.dev.yaml`` plus the override), starts the scripted TLS identity
   provider as a compose service, deploys the agents, adopts the tenant's HTTP
   selection, binds one email address per agent, writes the API's
   ``api-runtime.json`` and ``provider-ca.pem`` into a named volume, brings the
   API up again reading them, and mints a channel token per binding.
3. ``send`` signs an email receiver proof inside the API container and posts
   ``/channels/turns``.
4. ``observe`` reads the newest runner container of an agent since its last
   send, decodes the ``CURIE_CONNECTOR_CALLER_TOKEN`` in that container's
   environment with the signature withheld, and reports the reply events.
5. ``prove --scenario requester-handoff-docker`` runs the scenario in
   ``identity_scenarios_docker.py`` and writes JSON evidence.
6. ``down`` removes the runner containers the stack's worker created, the
   compose project with its volumes, and the images ``build`` or ``up``
   tagged, then verifies that nothing remains.

Files reach containers only through named volumes filled from a tar stream;
no local path is bind-mounted, so the stack works on a remote Docker host.
State (generated keys, ids, channel tokens) lives in ``--state-dir`` with mode
0700 and 0600 files. No action prints a key, bearer, proof or token. See
docs/testing/identity-e2e-docker.md.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from identity_assets import (
    CREDENTIAL_ID,
    DEFAULT_ROSTER,
    DOMAIN,
    INBOX_DOMAIN,
    ISSUER,
    LOOKUP_PORT,
    OWNER_LABEL,
    PROVIDER_SCRIPT,
    SINK_PORT,
    VECTORS,
    ed25519_pair,
    make_certificates,
    runtime_document,
)
from identity_e2e import (
    CALLER_TOKEN_ENV,
    REPLY_TEXT_LIMIT,
    STATE_FILE,
    STATE_VERSION,
    State,
    caller_fingerprint,
    decode_caller_token,
    epoch,
    slack_channel_for,
    slug,
    split_log_stamp,
    turn_completed,
    turn_summary,
    worker_log_message,
)
from identity_target import (
    ADAPTER,
    ADOPT_SCRIPT,
    HERE,
    REPO,
    SIGN_SCRIPT,
    SQL_SCRIPT,
    Api,
    HarnessError,
    b64url,
    http,
    log,
    summarize_event,
    write_private,
)

SUBSTRATE = "docker"
BASE_COMPOSE = REPO / "compose.dev.yaml"
WORKER_OVERLAY_DOCKERFILE = REPO / "compose" / "worker-local.Dockerfile"
DEFAULT_BUNDLE = REPO / "examples" / "weather"
DEFAULT_IMAGE_PREFIX = "ghcr.io/curie-eng"
PUBLISHED_PREFIX = "ghcr.io/curie-eng/"
DEFAULT_PORT_BASE = 27100
PORT_SPAN = 100
# The harness's own compose profile, so `curie local up` never starts its services.
PROFILE = "identity-e2e"
ALL_PROFILES = ("core", "full", "local-model", "slack", PROFILE)
BASE_PROFILES = ("core", "full", "local-model", "slack")
IDP_SERVICE = "identity-e2e-idp"
SEED_SERVICE = "identity-e2e-seed"
API_SERVICE = "curie-api"
WORKER_SERVICE = "curie-worker"
HTTP_IDENTITY_VOLUME = "identity_e2e_http_identity"
IDP_VOLUME = "identity_e2e_idp_files"
HTTP_IDENTITY_MOUNT = "/var/run/curie/http-identity"
IDP_MOUNT = "/idp/files"
SEED_ROOT = "/seed"
# The api image's `app` user and group (apps/api/Dockerfile). Seeded files are
# root-owned and group-readable by it, as the chart's 0440 Secret mount with
# fsGroup 1000 is; the API's operator-file check accepts root or its own uid.
APP_UID = 1000
APP_GID = 1000
SEED_MODE = 0o440
OVERRIDE_FILE = "compose.override.yaml"
IDENTITY_FILE = "compose.identity.yaml"
CONTEXT_TAR = "build-context.tar"
# The Docker substrate's runner container labels (apps/worker/.../sandbox/types.py
# and sandbox/docker.py). The stack label is the worker's runner network name.
MANAGED_BY_LABEL = "curietech.ai/managed-by"
MANAGED_BY_VALUE = "curie-sandbox-substrate"
COMPONENT_LABEL = "curietech.ai/component"
RUNNER_COMPONENT = "claim-runner"
STACK_LABEL = "curietech.ai/stack"
AGENT_LABEL = "curietech.ai/agent"
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
COMPOSE_SERVICE_LABEL = "com.docker.compose.service"
# Candidate images built from a source context: (component, image name, Dockerfile).
CANDIDATES = (
    ("api", "curie-api", "apps/api/Dockerfile"),
    ("worker", "curie-worker", "apps/worker/Dockerfile"),
    ("runner", "curie-runner", "runner/Dockerfile"),
)
# Shell variables the child CLI and compose must not inherit: model
# credentials (the stack runs the fake model) and anything that would steer
# the stack away from this project's isolation contract.
_CREDENTIAL_ENV = frozenset({"ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "CURIE_CREDENTIALS"})
_STEERING_PREFIXES = ("CURIE_", "COMPOSE_", "OTEL_", "VALKEY_", "S3_", "SLACK_", "LANGFUSE_")
_STEERING_NAMES = frozenset({"DATABASE_URL", "ANTHROPIC_BASE_URL"})
_PROJECT = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_IMAGE_REF = re.compile(r"[a-z0-9][a-z0-9._/:-]*(?:@sha256:[0-9a-f]{64})?")
_WORKER_FROM = re.compile(r"^FROM\s+ghcr\.io/curie-eng/curie-worker:\$\{BASE_TAG\}[ \t]*$", re.M)
# Path components never sent in a build context: VCS data, build outputs,
# environments and anything that can hold a credential.
_CONTEXT_SKIP_PARTS = frozenset(
    {
        ".git",
        "node_modules",
        ".venv",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".worktrees",
        ".identity-e2e",
        ".ssh",
        ".aws",
    }
)
_CONTEXT_SKIP_PREFIXES = ("cli/target/", "apps/ui/dist/", "apps/ui/test-results/")
_ENV_EXAMPLES = frozenset({".env.example", ".env.sample", ".env.template"})


# --- pure helpers (unit tested) ----------------------------------------------------


@dataclass(frozen=True)
class Ports:
    """Every host port the isolated stack publishes, inside one 100-port block."""

    base: int

    def __post_init__(self) -> None:
        if not 1024 <= self.base or self.base + PORT_SPAN > 32768:
            raise HarnessError(
                f"--port-base {self.base} must leave {PORT_SPAN} ports between 1024 and 32767"
            )

    @property
    def api(self) -> int:
        return self.base

    @property
    def postgres(self) -> int:
        return self.base + 1

    @property
    def valkey(self) -> int:
        return self.base + 2

    @property
    def s3(self) -> int:
        return self.base + 3

    @property
    def s3_console(self) -> int:
        return self.base + 4

    @property
    def stub(self) -> int:
        return self.base + 5

    @property
    def sink(self) -> int:
        return self.base + 6

    @property
    def broker(self) -> int:
        return self.base + 7

    @property
    def extra_start(self) -> int:
        return self.base + 20

    @property
    def last(self) -> int:
        return self.base + PORT_SPAN - 1

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.api}"

    @property
    def sink_url(self) -> str:
        return f"http://127.0.0.1:{self.sink}"


@dataclass(frozen=True)
class Images:
    """The image references the stack runs, and the worker overlay `up` builds."""

    api: str
    worker: str
    runner: str
    ui: str
    worker_local: str

    def as_dict(self) -> dict[str, str]:
        return {
            "api": self.api,
            "worker": self.worker,
            "runner": self.runner,
            "ui": self.ui,
            "worker_local": self.worker_local,
        }


class Tagged:
    """A YAML node with an explicit tag, such as compose's ``!override``."""

    def __init__(self, tag: str, value: Any) -> None:
        if not re.fullmatch(r"![a-z_]+", tag):
            raise ValueError(f"unsupported YAML tag {tag!r}")
        self.tag = tag
        self.value = value


def check_project(project: str) -> str:
    if not _PROJECT.fullmatch(project or ""):
        raise HarnessError("--project must be a lowercase Compose project name")
    return project


def check_image(ref: str) -> str:
    if not _IMAGE_REF.fullmatch(ref or "") or ".." in ref or "//" in ref:
        raise HarnessError(f"{ref!r} is not an image reference this harness accepts")
    return ref


def runner_network(project: str) -> str:
    """The isolated runner network: the CLI requires ``{project}_runner``."""

    return f"{project}_runner"


def provider_host(project: str) -> str:
    """The provider's network alias, which its certificate names."""

    return f"{project}-idp"


def resolve_images(
    *,
    project: str,
    tag: str,
    prefix: str = DEFAULT_IMAGE_PREFIX,
    api_repo: str | None = None,
    worker_repo: str | None = None,
    runner_repo: str | None = None,
    ui_repo: str | None = None,
) -> Images:
    """The refs for one tag; each repo defaults to ``<prefix>/curie-<component>``."""

    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag or ""):
        raise HarnessError(f"--image-tag {tag!r} is not a Docker tag")
    prefix = prefix.rstrip("/")

    def ref(repo: str | None, name: str) -> str:
        return check_image(f"{(repo or f'{prefix}/{name}').rstrip('/')}:{tag}")

    return Images(
        api=ref(api_repo, "curie-api"),
        worker=ref(worker_repo, "curie-worker"),
        runner=ref(runner_repo, "curie-runner"),
        ui=ref(ui_repo, "curie-ui"),
        worker_local=check_image(f"{check_project(project)}/curie-worker-local:{tag}"),
    )


def compose_escape(text: str) -> str:
    """``text`` with every ``$`` doubled, so compose interpolates nothing in it."""

    return text.replace("$", "$$")


def worker_dockerfile(text: str, base_image: str) -> str:
    """compose/worker-local.Dockerfile built FROM ``base_image`` instead of the published worker."""

    check_image(base_image)
    replaced, count = _WORKER_FROM.subn(lambda _match: f"FROM {base_image}", text)
    if count != 1:
        raise HarnessError(
            "compose/worker-local.Dockerfile no longer builds FROM the published worker; "
            "update identity_compose.worker_dockerfile"
        )
    return replaced


def isolation_env(
    *, project: str, files: Sequence[Path | str], ports: Ports, image_tag: str
) -> dict[str, str]:
    """The all-or-nothing isolation contract `curie local` reads (cli/src/local.rs)."""

    paths = [str(path) for path in files]
    if len(paths) < 2 or any(":" in path or not Path(path).is_absolute() for path in paths):
        raise HarnessError("isolation needs two or more absolute compose files without ':'")
    return {
        "COMPOSE_PROJECT_NAME": check_project(project),
        "COMPOSE_FILE": ":".join(paths),
        "CURIE_API_URL": ports.api_url,
        "VALKEY_HOST": "127.0.0.1",
        "VALKEY_PORT": str(ports.valkey),
        "S3_ENDPOINT_URL": f"http://127.0.0.1:{ports.s3}",
        "CURIE_DOCKER_NETWORK": runner_network(project),
        "CURIE_LOCAL_STUB_PORT": str(ports.stub),
        "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
        "CURIE_LOCAL_POSTGRES_PORT": str(ports.postgres),
        "CURIE_LOCAL_IMAGE_TAG": image_tag,
    }


def port_overrides(config: Mapping[str, Any], ports: Ports) -> dict[str, list[str]]:
    """Every port the base compose publishes, remapped into this stack's block.

    ``config`` is ``docker compose config --format json`` of the base file
    with every profile on. The services the harness reaches keep fixed
    offsets; any other published port takes the next free one. Each mapping
    stays bound to 127.0.0.1, as the base file binds them.
    """

    fixed = {
        (API_SERVICE, 8000): ports.api,
        ("postgres", 5432): ports.postgres,
        ("valkey", 6379): ports.valkey,
        ("rustfs", 9000): ports.s3,
        ("rustfs", 9001): ports.s3_console,
    }
    taken = {*fixed.values(), ports.stub, ports.sink, ports.broker}
    seen: set[tuple[str, int]] = set()
    following = ports.extra_start
    out: dict[str, list[str]] = {}
    services = config.get("services") or {}
    for service in sorted(services):
        mapped: list[str] = []
        for entry in services[service].get("ports") or ():
            target = int(entry["target"])
            protocol = str(entry.get("protocol") or "tcp")
            host = fixed.get((service, target))
            if host is None:
                while following in taken:
                    following += 1
                host = following
                taken.add(host)
            if host > ports.last:
                raise HarnessError(f"the base compose publishes more ports than {ports.base}+99")
            seen.add((service, target))
            mapped.append(
                f"127.0.0.1:{host}:{target}" + ("" if protocol == "tcp" else f"/{protocol}")
            )
        if mapped:
            out[service] = mapped
    missing = sorted(f"{s}:{t}" for s, t in fixed if (s, t) not in seen)
    if missing:
        raise HarnessError(f"the base compose no longer publishes {missing}")
    return out


def worker_environment(ports: Ports, project: str, runner_image: str) -> dict[str, str]:
    """The worker env the override rebinds; secrets arrive only by interpolation."""

    return {
        # The CLI checks that each of the first seven names the isolated endpoint.
        "DATABASE_URL": (
            "postgresql+asyncpg://postgres:${CURIE_LOCAL_POSTGRES_PASSWORD:-postgres}"
            f"@127.0.0.1:{ports.postgres}/postgres"
        ),
        "VALKEY_HOST": "127.0.0.1",
        "VALKEY_PORT": str(ports.valkey),
        "S3_ENDPOINT_URL": f"http://127.0.0.1:{ports.s3}",
        "CURIE_API_URL": ports.api_url,
        "SLACK_API_BASE_URL": f"http://127.0.0.1:{ports.stub}/api/",
        "CURIE_DOCKER_NETWORK": runner_network(project),
        "CURIE_RUNNER_IMAGE": compose_escape(runner_image),
        "CURIE_FAKE_MODEL": "1",
        "CURIE_ATTACHMENT_BROKER_PORT": str(ports.broker),
        # Not in the base list, so the override must add it. The value comes
        # from the harness's process environment, never from this file.
        "CURIE_CONNECTOR_CALLER_SIGNING_KEY": "${CURIE_CONNECTOR_CALLER_SIGNING_KEY:-}",
    }


def provider_command() -> list[str]:
    files = IDP_MOUNT
    return [
        "python",
        f"{files}/fake_idp.py",
        "--roster",
        f"{files}/roster.json",
        "--vectors",
        f"{files}/vectors.json",
        "--bearer-file",
        f"{files}/bearer",
        "--adapter-secret-file",
        f"{files}/adapter-secret",
        "--cert",
        f"{files}/tls.crt",
        "--key",
        f"{files}/tls.key",
        "--lookup-port",
        str(LOOKUP_PORT),
        "--sink-port",
        str(SINK_PORT),
    ]


def override_document(
    *,
    project: str,
    ports: Ports,
    published: Mapping[str, Sequence[str]],
    images: Images,
    overlay_dockerfile: str,
    overlay_context: Path,
) -> dict[str, Any]:
    """The compose override: ports, images, worker rebinding and the harness services."""

    services: dict[str, dict[str, Any]] = {
        name: {"ports": Tagged("!override", list(mapped))} for name, mapped in published.items()
    }

    def service(name: str) -> dict[str, Any]:
        return services.setdefault(name, {})

    service("curie-migrate")["image"] = compose_escape(images.api)
    service(API_SERVICE)["image"] = compose_escape(images.api)
    service("curie-ui")["image"] = compose_escape(images.ui)
    worker = service(WORKER_SERVICE)
    worker["build"] = Tagged(
        "!override",
        {"context": str(overlay_context), "dockerfile_inline": compose_escape(overlay_dockerfile)},
    )
    worker["image"] = compose_escape(images.worker_local)
    # Rebuild the thin overlay on every up, so it always sits on the base
    # image the tag names now (a rebuilt candidate keeps its tag).
    worker["pull_policy"] = "build"
    worker["environment"] = worker_environment(ports, project, images.runner)
    hardening: dict[str, Any] = {
        "read_only": True,
        "cap_drop": ["ALL"],
        "security_opt": ["no-new-privileges:true"],
    }
    services[SEED_SERVICE] = {
        "image": compose_escape(images.api),
        "profiles": [PROFILE],
        "user": "0",
        "network_mode": "none",
        "entrypoint": ["tar", "--numeric-owner", "-x", "-f", "-", "-C", SEED_ROOT],
        "volumes": [
            f"{IDP_VOLUME}:{SEED_ROOT}/idp",
            f"{HTTP_IDENTITY_VOLUME}:{SEED_ROOT}/http-identity",
        ],
        "restart": "no",
    }
    services[IDP_SERVICE] = {
        "image": compose_escape(images.api),
        "profiles": [PROFILE],
        "user": f"{APP_UID}:{APP_GID}",
        "command": provider_command(),
        "networks": {"default": {"aliases": [provider_host(project)]}},
        "ports": [f"127.0.0.1:{ports.sink}:{SINK_PORT}"],
        "volumes": [f"{IDP_VOLUME}:{IDP_MOUNT}:ro"],
        "healthcheck": {
            "test": [
                "CMD",
                "python",
                "-c",
                "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen("
                f"'http://127.0.0.1:{SINK_PORT}/healthz').status==200 else 1)",
            ],
            "interval": "2s",
            "timeout": "5s",
            "retries": 30,
        },
        "restart": "unless-stopped",
        **hardening,
    }
    return {
        "services": services,
        "networks": {"curie_runner": {"name": runner_network(project)}},
        "volumes": {HTTP_IDENTITY_VOLUME: {}, IDP_VOLUME: {}},
    }


def identity_overlay_document() -> dict[str, Any]:
    """The API's HTTP identity directory: a read-only named volume and its env."""

    return {
        "services": {
            API_SERVICE: {
                "environment": {"CURIE_HTTP_IDENTITY_DIR": HTTP_IDENTITY_MOUNT},
                "volumes": [f"{HTTP_IDENTITY_VOLUME}:{HTTP_IDENTITY_MOUNT}:ro"],
            }
        }
    }


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    # A JSON string is a valid YAML double-quoted scalar.
    return json.dumps(str(value), ensure_ascii=False)


def _yaml_block(value: Any, indent: int) -> list[str]:
    pad = "  " * indent
    if isinstance(value, Mapping):
        lines: list[str] = []
        for key, item in value.items():
            head = f"{pad}{_yaml_scalar(key)}:"
            tag = ""
            if isinstance(item, Tagged):
                tag, item = f" {item.tag}", item.value
            if isinstance(item, (Mapping, list)) and item:
                lines.append(head + tag)
                lines.extend(_yaml_block(item, indent + 1))
            elif isinstance(item, Mapping):
                lines.append(f"{head}{tag} {{}}")
            elif isinstance(item, list):
                lines.append(f"{head}{tag} []")
            else:
                lines.append(f"{head}{tag} {_yaml_scalar(item)}")
        return lines
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, Tagged):
                raise ValueError("tagged sequence items are not supported")
            if isinstance(item, (Mapping, list)) and item:
                nested = _yaml_block(item, indent + 1)
                inner = "  " * (indent + 1)
                lines.append(f"{pad}- {nested[0][len(inner) :]}")
                lines.extend(nested[1:])
            elif isinstance(item, Mapping):
                lines.append(f"{pad}- {{}}")
            elif isinstance(item, list):
                lines.append(f"{pad}- []")
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
        return lines
    return [f"{pad}{_yaml_scalar(value)}"]


def yaml_dump(document: Mapping[str, Any]) -> str:
    """Block-style YAML for a compose file, with ``Tagged`` nodes."""

    return "\n".join(_yaml_block(document, 0)) + "\n"


def seed_archive(entries: Mapping[str, bytes], *, uid: int = 0, gid: int = APP_GID) -> bytes:
    """A tar of ``entries`` (relative paths), each root-owned with mode 0440 for ``gid``."""

    buffer = io.BytesIO()
    now = int(time.time())
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.GNU_FORMAT) as archive:
        for name in sorted(entries):
            if name.startswith("/") or ".." in Path(name).parts:
                raise HarnessError(f"seed entry {name!r} is not a relative path")
            data = entries[name]
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = SEED_MODE
            info.uid, info.gid = uid, gid
            info.uname = info.gname = ""
            info.mtime = now
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def context_excluded(relative: str) -> bool:
    """Whether a path (relative to the context root) stays out of a build context."""

    path = relative[2:] if relative.startswith("./") else relative
    if not path or path == ".":
        return False
    parts = Path(path).parts
    if any(part in _CONTEXT_SKIP_PARTS for part in parts):
        return True
    if any((path + "/").startswith(prefix) for prefix in _CONTEXT_SKIP_PREFIXES):
        return True
    name = parts[-1]
    if name == ".env" or (name.startswith(".env.") and name not in _ENV_EXAMPLES):
        return True
    return name.endswith(".key")


def context_member(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
    """A ``tarfile.add`` filter: drop excluded paths and the local owner's identity."""

    if context_excluded(info.name):
        return None
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def context_filter(
    context: Path, state: Path
) -> Callable[[tarfile.TarInfo], tarfile.TarInfo | None]:
    """The archive filter for a directory build context that may hold the state directory.

    The state directory holds the generated keys, the stack's credentials and
    the archive being written, so nothing under it enters the context. A
    context inside the state directory, or equal to it, is refused.
    """

    root = context.resolve()
    private = state.resolve()
    if root == private or private in root.parents:
        raise HarnessError(f"--build-context {context} is inside the state directory {state}")
    try:
        skipped: str | None = private.relative_to(root).as_posix()
    except ValueError:
        skipped = None

    def member(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        name = info.name[2:] if info.name.startswith("./") else info.name
        if skipped is not None and (name == skipped or name.startswith(skipped + "/")):
            return None
        return context_member(info)

    return member


def parse_created(raw: str) -> float | None:
    """Unix seconds of a Docker RFC 3339 timestamp (nanoseconds allowed), or None."""

    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})", (raw or "").strip()
    )
    if match is None:
        return None
    fraction = (match[2] or "")[:7]
    zone = "+00:00" if match[3] == "Z" else match[3]
    value = epoch(match[1] + fraction + zone)
    return value or None


def container_env(inspected: Mapping[str, Any]) -> dict[str, str]:
    """``.Config.Env`` of one ``docker inspect`` object as a mapping."""

    env: dict[str, str] = {}
    for entry in (inspected.get("Config") or {}).get("Env") or ():
        key, sep, value = str(entry).partition("=")
        if sep:
            env[key] = value
    return env


def runner_view(inspected: Mapping[str, Any]) -> dict[str, Any]:
    """One runner container: name, agent, creation and its caller token (never printed)."""

    labels = (inspected.get("Config") or {}).get("Labels") or {}
    created = str(inspected.get("Created") or "")
    return {
        "name": str(inspected.get("Name") or "").lstrip("/"),
        "agent": labels.get(AGENT_LABEL),
        "status": (inspected.get("State") or {}).get("Status"),
        "created": created,
        "created_epoch": parse_created(created),
        "token": container_env(inspected).get(CALLER_TOKEN_ENV),
    }


def log_entries(text: str, since: float, source: str) -> list[dict[str, Any]]:
    """``docker logs --timestamps`` lines at or after ``since``, as worker log entries."""

    entries = []
    for line in text.splitlines():
        if not line.strip():
            continue
        at, rest = split_log_stamp(line)
        if at is not None and at < since:
            continue
        entries.append(
            {"at": since if at is None else at, "pod": source, "message": worker_log_message(rest)}
        )
    return entries


_SECRET_ASSIGNMENT = re.compile(
    r"(?P<name>\b[A-Z][A-Z0-9_]*(?:KEY|PASSWORD|SECRET|TOKEN|CREDENTIALS)[A-Z0-9_]*)=(?P<value>'[^']*'|\"[^\"]*\"|\S*)"
)


def redact(text: str) -> str:
    """``text`` with the value of every ``NAME=value`` whose name looks secret removed.

    The CLI prints its compose command with the stack's key and password
    partly masked; a failure tail must not carry even that prefix.
    """

    return _SECRET_ASSIGNMENT.sub(lambda match: f"{match['name']}=<redacted>", text)


def failure_tail(raw: bytes, lines: int = 8) -> str:
    text = redact(raw.decode("utf-8", "replace"))
    return " | ".join(text.strip().splitlines()[-lines:])


def child_env(base: Mapping[str, str], extra: Mapping[str, str]) -> dict[str, str]:
    """``base`` without model credentials or stack-steering names, plus ``extra``."""

    kept = {
        key: value
        for key, value in base.items()
        if key not in _CREDENTIAL_ENV
        and key not in _STEERING_NAMES
        and not key.startswith(_STEERING_PREFIXES)
    }
    kept.update(extra)
    return kept


# --- process helpers -----------------------------------------------------------------


def run_process(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    stdin: bytes | None = None,
    check: bool = True,
    timeout: float = 600,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run ``argv`` with exactly ``env``. A failure names the command, not its input."""

    try:
        result = subprocess.run(  # noqa: S603 - argv lists only
            list(argv),
            input=stdin,
            stdin=None if stdin is not None else subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=None if env is None else dict(env),
            cwd=cwd,
        )
    except FileNotFoundError:
        raise HarnessError(f"{argv[0]} is not installed or not on PATH") from None
    except subprocess.TimeoutExpired:
        raise HarnessError(f"{' '.join(argv[:4])} timed out after {timeout:.0f}s") from None
    if check and result.returncode != 0:
        tail = failure_tail(result.stderr or result.stdout)
        raise HarnessError(f"{' '.join(argv[:5])} exited {result.returncode}: {tail}")
    return result


def _text(result: subprocess.CompletedProcess[bytes]) -> str:
    return result.stdout.decode("utf-8", "replace")


# --- the isolated stack ----------------------------------------------------------------


class ComposeTarget:
    """One isolated `curie local` compose project on the Docker daemon DOCKER_HOST names."""

    def __init__(self, *, project: str, state_dir: Path, curie_bin: str, ports: Ports) -> None:
        self.project = check_project(project)
        self.state_dir = state_dir
        self.curie_bin = curie_bin
        self.ports = ports
        self.network = runner_network(project)
        self.private_config = state_dir / "curie-config"
        self.override_path = state_dir / OVERRIDE_FILE
        self.identity_path = state_dir / IDENTITY_FILE
        self.image_tag = "latest"
        # Values the compose files interpolate; set from the state, never printed.
        self.secret_env: dict[str, str] = {}
        self._api_key: str | None = None

    # docker / compose -----------------------------------------------------------

    def docker(
        self, *args: str, stdin: bytes | None = None, check: bool = True, timeout: float = 600
    ) -> subprocess.CompletedProcess[bytes]:
        return run_process(["docker", *args], stdin=stdin, check=check, timeout=timeout)

    def files(self, *, identity: bool) -> list[Path]:
        files = [BASE_COMPOSE, self.override_path]
        if identity:
            files.append(self.identity_path)
        return files

    def env(self, *, identity: bool) -> dict[str, str]:
        """The child environment for `curie` and compose: isolation plus interpolated secrets."""

        extra = isolation_env(
            project=self.project,
            files=self.files(identity=identity),
            ports=self.ports,
            image_tag=self.image_tag,
        )
        extra.update(self.secret_env)
        extra["CURIE_CONFIG_DIR"] = str(self.private_config)
        extra["CURIE_FAKE_MODEL"] = "1"
        return child_env(os.environ, extra)

    def compose(
        self,
        *args: str,
        identity: bool,
        stdin: bytes | None = None,
        check: bool = True,
        timeout: float = 900,
    ) -> subprocess.CompletedProcess[bytes]:
        argv = ["docker", "compose"]
        for profile in ALL_PROFILES:
            argv += ["--profile", profile]
        argv += ["-p", self.project]
        for path in self.files(identity=identity):
            argv += ["-f", str(path)]
        return run_process(
            [*argv, *args],
            env=self.env(identity=identity),
            stdin=stdin,
            check=check,
            timeout=timeout,
        )

    def base_config(self) -> dict[str, Any]:
        """The base compose file with every profile on, as compose itself parses it."""

        argv = ["docker", "compose", "-p", self.project, "-f", str(BASE_COMPOSE)]
        for profile in BASE_PROFILES:
            argv += ["--profile", profile]
        result = run_process(
            [*argv, "config", "--format", "json"],
            env=child_env(os.environ, {}),
            timeout=120,
        )
        return dict(json.loads(_text(result)))

    def curie(
        self,
        *args: str,
        identity: bool,
        extra_env: Mapping[str, str] | None = None,
        timeout: float = 1800,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run the curie CLI with this stack's isolation contract and a private config dir.

        The private ``CURIE_CONFIG_DIR`` keeps the stack's generated API key and
        Postgres password apart from any other local stack, and stops the CLI
        swapping in a key stored for another one.
        """

        self.private_config.mkdir(mode=0o700, parents=True, exist_ok=True)
        env = self.env(identity=identity)
        env.update(extra_env or {})
        # A source-built CLI resolves compose.dev.yaml from its working directory.
        return run_process([self.curie_bin, *args], env=env, check=False, timeout=timeout, cwd=REPO)

    # discovery ------------------------------------------------------------------

    def container(self, service: str, *, running: bool = True) -> str:
        args = [
            "ps",
            "-q",
            "--filter",
            f"label={COMPOSE_PROJECT_LABEL}={self.project}",
            "--filter",
            f"label={COMPOSE_SERVICE_LABEL}={service}",
        ]
        if not running:
            args.insert(1, "-a")
        ids = _text(self.docker(*args)).split()
        if not ids:
            raise HarnessError(
                f"no {'running ' if running else ''}{service} container in {self.project}"
            )
        return ids[0]

    def inspect(self, *names: str) -> list[dict[str, Any]]:
        if not names:
            return []
        result = self.docker("inspect", *names, check=False)
        text = _text(result).strip()
        if not text:
            return []
        try:
            return list(json.loads(text))
        except ValueError:
            raise HarnessError("docker inspect printed no JSON") from None

    def service_env(self, service: str) -> dict[str, str]:
        """A service container's environment. Callers keep secret values in memory only."""

        found = self.inspect(self.container(service))
        if not found:
            raise HarnessError(f"cannot inspect the {service} container")
        return container_env(found[0])

    def platform_key(self) -> str:
        if self._api_key is None:
            result = self.docker("exec", self.container(API_SERVICE), "printenv", "API_KEY")
            key = _text(result).strip()
            if not key:
                raise HarnessError("the API container has no API_KEY")
            self._api_key = key
        return self._api_key

    def forget_api_key(self) -> None:
        self._api_key = None

    def runner_names(self, agent: str | None = None) -> list[str]:
        args = [
            "ps",
            "-a",
            "--filter",
            f"label={MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
            "--filter",
            f"label={COMPONENT_LABEL}={RUNNER_COMPONENT}",
            "--filter",
            f"label={STACK_LABEL}={self.network}",
            "--format",
            "{{.Names}}",
        ]
        if agent is not None:
            args[-2:-2] = ["--filter", f"label={AGENT_LABEL}={agent}"]
        return [name for name in _text(self.docker(*args)).split() if name]

    # in-container product calls -------------------------------------------------

    def pod_python(self, script: str, params: Mapping[str, Any]) -> Any:
        """Run ``script`` in the API container against the deployed product code."""

        result = run_process(
            ["docker", "exec", "-i", self.container(API_SERVICE), "python", "-c", script],
            stdin=json.dumps(params).encode(),
            timeout=120,
        )
        lines = [line for line in _text(result).splitlines() if line.strip()]
        if not lines:
            raise HarnessError("the in-container product call printed nothing")
        return json.loads(lines[-1])

    def sql(self, *statements: tuple[str, Mapping[str, Any]]) -> list[Any]:
        """Run statements in one transaction inside the API container."""

        return list(
            self.pod_python(
                SQL_SCRIPT,
                {
                    "statements": [
                        {"sql": sql, "params": dict(params)} for sql, params in statements
                    ]
                },
            )
        )

    # waiting --------------------------------------------------------------------

    def wait_http(self, url: str, *, timeout: float = 300, what: str) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                status, _ = http("GET", url, timeout=5)
                if status == 200:
                    return
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise HarnessError(f"{what} never answered at {url}")
            time.sleep(1)


# --- the harness -------------------------------------------------------------------------


class ComposeHarness:
    """Actions over one isolated compose stack and one state directory.

    Method names and result shapes follow ``identity_e2e.Harness`` (``send``,
    ``observe``, ``claims``, ``replies``, ``worker_log``, ``api``), so the
    shared scenario helpers in ``identity_scenarios`` run unchanged. A claim
    here is a runner container.
    """

    substrate = SUBSTRATE

    def __init__(self, target: ComposeTarget, state: State) -> None:
        self.target = target
        self.state = state

    @property
    def data(self) -> dict[str, Any]:
        return self.state.data

    def load(self) -> None:
        self.state.load()
        self._check_target()
        self._apply_state()

    def _check_target(self) -> None:
        current = {"substrate": SUBSTRATE, "project": self.target.project}
        recorded = self.data.get("target")
        if recorded is not None and {k: recorded.get(k) for k in current} != current:
            raise HarnessError(f"the state directory belongs to {recorded}, not {current}")
        port_base = (recorded or {}).get("port_base")
        if port_base is not None and port_base != self.target.ports.base:
            # Later actions take the block `up` used.
            self.target.ports = Ports(int(port_base))
        self.data["target"] = {**current, "port_base": self.target.ports.base}

    def _apply_state(self) -> None:
        keys = self.data.get("keys", {})
        secret_env: dict[str, str] = {}
        if keys.get("adapter_secret"):
            secret_env["CURIE_ADAPTER_CREDENTIALS"] = json.dumps(
                {ADAPTER: keys["adapter_secret"]}, separators=(",", ":")
            )
        if keys.get("caller_signing"):
            secret_env["CURIE_CONNECTOR_CALLER_SIGNING_KEY"] = keys["caller_signing"]
        self.target.secret_env = secret_env
        images = self.data.get("images") or {}
        self.target.image_tag = str(images.get("tag") or "latest")

    @contextlib.contextmanager
    def api(self) -> Iterator[Api]:
        yield Api(self.target.ports.api_url, self.target.platform_key())

    def records(self) -> list[dict[str, Any]]:
        status, payload = http("GET", self.target.ports.sink_url + "/records")
        if status != 200:
            raise HarnessError(f"the provider records answered {status}")
        return list(payload["records"])

    def agent_entry(self, name: str) -> dict[str, Any]:
        agent = self.data.get("agents", {}).get(name)
        if agent is None:
            raise HarnessError(f"agent {name} is not in the harness state; run `up --agent {name}`")
        return agent  # type: ignore[no-any-return]

    # build ------------------------------------------------------------------------

    def build(self, context: Path, images: Images) -> list[str]:
        """Build candidate api, worker and runner images from ``context``.

        ``context`` is a tarball (``git archive``) or a source directory, which
        is tarred without VCS data, build outputs or credential files.
        """

        refs = {"api": images.api, "worker": images.worker, "runner": images.runner}
        for ref in refs.values():
            if ref.startswith(PUBLISHED_PREFIX):
                raise HarnessError(
                    f"refusing to build {ref}: candidate images must not shadow a published "
                    "repository; pass --image-repo-prefix"
                )
        self.state.ensure_directory()
        built: list[str] = []
        temporary: Path | None = None
        if context.is_dir():
            temporary = self.state.directory / CONTEXT_TAR
            log(f"archiving the build context {context}")
            # GNU format: docker build reads a PAX stream on stdin as a Dockerfile.
            with tarfile.open(temporary, "w", format=tarfile.GNU_FORMAT) as archive:
                archive.add(
                    context,
                    arcname=".",
                    filter=context_filter(context, self.state.directory),
                )
            tarball = temporary
        elif context.is_file():
            tarball = context
        else:
            raise HarnessError(f"--build-context {context} does not exist")
        try:
            for component, _name, dockerfile in CANDIDATES:
                ref = refs[component]
                log(f"building {ref} from {dockerfile}")
                started = time.monotonic()
                run_process(
                    [
                        "docker",
                        "build",
                        "--progress=plain",
                        "--label",
                        f"{OWNER_LABEL}={self.target.project}",
                        "-f",
                        dockerfile,
                        "-t",
                        ref,
                        "-",
                    ],
                    stdin=tarball.read_bytes(),
                    timeout=3600,
                )
                built.append(ref)
                owned = self.data.setdefault("owned_images", [])
                if ref not in owned:
                    owned.append(ref)
                self.state.save()
                log(f"built {ref} in {time.monotonic() - started:.0f}s")
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return built

    # up -----------------------------------------------------------------------------

    def _init_state(self) -> None:
        self.state.ensure_directory()
        if self.state.exists():
            self.state.load()
        else:
            self.data.update(
                version=STATE_VERSION, substrate=SUBSTRATE, agents={}, sends={}, owned_images=[]
            )
        self._check_target()

    def up(
        self,
        agents: Sequence[str],
        bundle: Path,
        roster: Mapping[str, Any],
        images: Images,
        *,
        keep_connectors: bool = False,
    ) -> dict[str, Any]:
        if not agents:
            raise HarnessError("up needs at least one --agent")
        target = self.target
        self._init_state()
        keys = self.data.setdefault("keys", {})
        keys.setdefault("receiver", b64url(secrets.token_bytes(32)))
        keys.setdefault("context", b64url(secrets.token_bytes(32)))
        keys.setdefault("bearer", secrets.token_urlsafe(32))
        keys.setdefault("adapter_secret", secrets.token_urlsafe(32))
        if "caller_signing" not in keys:
            keys["caller_signing"], keys["caller_verify"] = ed25519_pair(self.state.directory)
        self.data["images"] = {**images.as_dict(), "tag": target.image_tag}
        owned = self.data.setdefault("owned_images", [])
        if images.worker_local not in owned:
            owned.append(images.worker_local)
        self.data["roster"] = copy.deepcopy(dict(roster))
        host = provider_host(target.project)
        self.data.update(
            issuer=ISSUER,
            domain=DOMAIN,
            lookup_url=f"https://{host}:{LOOKUP_PORT}/lookup",
            reply_base=f"{target.ports.sink_url}/reply",
        )
        self.state.save()
        self._apply_state()
        if not (self.state.directory / "server.pem").is_file():
            log("creating the private CA and provider certificate")
            make_certificates(self.state.directory, [host])

        log("writing the compose override")
        overlay = worker_dockerfile(WORKER_OVERLAY_DOCKERFILE.read_text(), images.worker)
        published = port_overrides(target.base_config(), target.ports)
        document = override_document(
            project=target.project,
            ports=target.ports,
            published=published,
            images=images,
            overlay_dockerfile=overlay,
            overlay_context=WORKER_OVERLAY_DOCKERFILE.parent,
        )
        write_private(target.override_path, yaml_dump(document).encode())
        write_private(target.identity_path, yaml_dump(identity_overlay_document()).encode())

        identity = bool(self.data.get("identity_applied"))
        self._local_up(identity=identity)

        log("seeding and starting the test identity provider")
        self._seed(
            {
                "idp/fake_idp.py": PROVIDER_SCRIPT.read_bytes(),
                "idp/vectors.json": VECTORS.read_bytes(),
                "idp/roster.json": json.dumps(roster, indent=2, sort_keys=True).encode(),
                "idp/tls.crt": (self.state.directory / "server.pem").read_bytes(),
                "idp/tls.key": (self.state.directory / "server.key").read_bytes(),
                "idp/bearer": keys["bearer"].encode(),
                "idp/adapter-secret": keys["adapter_secret"].encode(),
            }
        )
        target.compose(
            "up", "-d", "--no-deps", "--force-recreate", "--wait", IDP_SERVICE, identity=identity
        )
        target.wait_http(target.ports.sink_url + "/healthz", timeout=120, what="the provider sink")

        with self.api() as api:
            self._agents(api, agents, bundle, keep_connectors=keep_connectors)
            tenant_id = self._tenant()
            self._bindings(api, tenant_id)
        selection = {
            "mode": "http",
            "issuer": ISSUER,
            "lookup_url": self.data["lookup_url"],
            "credential_ref": {"id": CREDENTIAL_ID, "generation": 1},
            "max_identity_age_ms": 60_000,
            "max_membership_age_ms": 60_000,
            "max_clock_skew_ms": 5_000,
        }
        log("adopting the tenant HTTP selection")
        adoption = target.pod_python(ADOPT_SCRIPT, {"tenant_id": tenant_id, "selection": selection})
        recorded = self.data.setdefault("adoption", adoption)
        if adoption.get("created_row"):
            recorded["created_row"] = True
        self.state.save()
        with self.api() as api:
            for name, agent in self.data["agents"].items():
                self._access_policy(api, name, agent)
        self.state.save()

        registration = self.data.setdefault(
            "registration",
            {
                "schema_version": 1,
                "receiver_id": str(uuid.uuid4()),
                "generation": 1,
                "tenant_id": tenant_id,
                "channel_identity_id": str(uuid.uuid4()),
                "provider_installation_id": str(uuid.uuid4()),
                "binding_ids": [],
                "domains": [
                    {
                        "id": str(uuid.uuid4()),
                        "authority": "",
                        "kind": "email_domain",
                        "key": DOMAIN,
                    }
                ],
            },
        )
        registration["binding_ids"] = sorted(
            agent["binding_id"] for agent in self.data["agents"].values()
        )
        runtime = runtime_document(
            tenant_id=tenant_id,
            selection=selection,
            bearer=keys["bearer"],
            registration=registration,
            receiver_key=keys["receiver"],
            context_key=keys["context"],
        )
        log("writing the API runtime document into its volume")
        self._seed(
            {
                "http-identity/api-runtime.json": json.dumps(runtime, sort_keys=True).encode(),
                "http-identity/provider-ca.pem": (self.state.directory / "ca.pem").read_bytes(),
            }
        )
        self.state.save()
        if identity:
            log("restarting the api to read the runtime document")
            target.docker("restart", target.container(API_SERVICE))
        else:
            log("bringing the api up again with its HTTP identity directory")
            self._local_up(identity=True)
            self.data["identity_applied"] = True
            self.state.save()
        target.forget_api_key()
        target.wait_http(target.ports.api_url + "/health", what="the API")
        api_env = target.service_env(API_SERVICE)
        if api_env.get("CURIE_HTTP_IDENTITY_DIR") != HTTP_IDENTITY_MOUNT:
            raise HarnessError("the API container does not read the HTTP identity directory")

        with self.api() as api:
            for name, agent in self.data["agents"].items():
                agent.update(api.mint_channel_token(agent["address"]))
                log(f"minted the channel token for {name}")
        self.state.save()
        return {
            "action": "up",
            "substrate": SUBSTRATE,
            "project": target.project,
            "api_url": target.ports.api_url,
            "tenant_id": tenant_id,
            "issuer": ISSUER,
            "lookup_url": self.data["lookup_url"],
            "images": self.data["images"],
            "adoption": {k: v for k, v in adoption.items() if k != "previous"},
            "agents": {
                name: {k: agent[k] for k in ("agent_id", "binding_id", "address", "created")}
                for name, agent in self.data["agents"].items()
            },
            "roster": sorted(roster.get("senders", {})),
            "state_dir": str(self.state.directory),
        }

    def _local_up(self, *, identity: bool) -> None:
        log(f"curie local up --minimal ({'with' if identity else 'without'} the identity overlay)")
        result = self.target.curie("local", "up", "--minimal", identity=identity)
        if result.returncode != 0:
            tail = failure_tail(result.stderr + result.stdout)
            raise HarnessError(f"curie local up exited {result.returncode}: {tail}")
        self.target.forget_api_key()
        self.target.wait_http(self.target.ports.api_url + "/health", what="the API")

    def _seed(self, entries: Mapping[str, bytes]) -> None:
        """Write ``entries`` into the harness volumes through a one-shot container."""

        self.target.compose(
            "run",
            "--rm",
            "-T",
            "--no-deps",
            SEED_SERVICE,
            identity=bool(self.data.get("identity_applied")),
            stdin=seed_archive(entries),
            timeout=300,
        )

    def _agents(
        self, api: Api, names: Sequence[str], bundle: Path, *, keep_connectors: bool
    ) -> None:
        existing = api.agents()
        agents = self.data["agents"]
        plugin_dir: Path | None = None
        for name in names:
            if name not in existing:
                if plugin_dir is None:
                    plugin_dir = self._stage_bundle(bundle, keep_connectors=keep_connectors)
                log(f"deploying {name} from {bundle}")
                result = self.target.curie(
                    "local",
                    "deploy",
                    "--plugin-dir",
                    str(plugin_dir),
                    "--agent",
                    name,
                    "--slack-channel",
                    slack_channel_for(name),
                    "--api-url",
                    self.target.ports.api_url,
                    "--json",
                    identity=bool(self.data.get("identity_applied")),
                    extra_env={"CURIE_API_KEY": self.target.platform_key()},
                    timeout=900,
                )
                if result.returncode != 0:
                    tail = failure_tail(result.stderr + result.stdout, 5)
                    raise HarnessError(f"curie local deploy {name} failed: {tail}")
                agents.setdefault(name, {})["created"] = True
                existing = api.agents()
                if name not in existing:
                    raise HarnessError(f"agent {name} is still missing after deploy")
            entry = agents.setdefault(name, {})
            entry.setdefault("created", False)
            entry["agent_id"] = str(existing[name]["id"])
            # The state file sorts its keys, so the registration order is kept apart.
            order = self.data.setdefault("agent_order", [])
            if name not in order:
                order.append(name)
        self.state.save()

    def _stage_bundle(self, bundle: Path, *, keep_connectors: bool) -> Path:
        """A copy of ``bundle`` in the state dir, without hosted connectors unless kept.

        The local tier would start a bundle's hosted connectors as containers,
        and no caller proxy fronts them there, so the scenario never dials one.
        """

        if not bundle.is_dir():
            raise HarnessError(f"--bundle {bundle} is not a directory")
        staged = self.state.directory / "bundle"
        shutil.rmtree(staged, ignore_errors=True)
        shutil.copytree(bundle, staged, ignore=shutil.ignore_patterns(".curie", "__pycache__"))
        if not keep_connectors:
            for name in ("connectors.yaml", "connectors.yml", "connectors.lock.yaml"):
                (staged / name).unlink(missing_ok=True)
        return staged

    def _tenant(self) -> str:
        ids = [agent["agent_id"] for agent in self.data["agents"].values()]
        rows = self.target.sql(
            (
                "SELECT DISTINCT tenant_id::text AS tenant_id FROM curie.agents "
                "WHERE id::text = ANY(:ids)",
                {"ids": ids},
            )
        )[0]
        if len(rows) != 1:
            raise HarnessError("the agents do not share one tenant")
        tenant = str(rows[0]["tenant_id"])
        self.data["tenant_id"] = tenant
        return tenant

    def _bindings(self, api: Api, tenant_id: str) -> None:
        del tenant_id  # one tenant per stack; kept for the Kubernetes signature
        for name, agent in self.data["agents"].items():
            address = f"{slug(name)}@{INBOX_DOMAIN}"
            # The worker runs on the host network, so it reaches the sink on
            # the port the provider publishes on the Docker host's loopback.
            endpoint = f"{self.data['reply_base']}/{slug(name)}"
            status, payload = api.call(
                "POST",
                f"/agents/{agent['agent_id']}/channels",
                {"kind": "email", "address": address, "adapter": ADAPTER, "endpoint": endpoint},
            )
            if not 200 <= status < 300 and status != 409:
                raise HarnessError(
                    f"binding {address} answered {status}: {summarize_event(payload)}"
                )
            rows = self.target.sql(
                (
                    "SELECT id::text AS id FROM curie.agent_channels WHERE agent_id::text = :a "
                    "AND kind = 'email' AND address = :address AND adapter = :adapter",
                    {"a": agent["agent_id"], "address": address, "adapter": ADAPTER},
                )
            )[0]
            if len(rows) != 1:
                raise HarnessError(f"expected one email binding for {name}, found {len(rows)}")
            agent.update(address=address, binding_id=rows[0]["id"], label=slug(name))
            log(f"email binding for {name}: {address}")
        self.state.save()

    def _access_policy(self, api: Api, name: str, agent: dict[str, Any]) -> None:
        """The HTTP lane answers 503 until the agent has a stored access policy."""

        policy = api.ok("GET", f"/agents/{agent['agent_id']}/access-policy")
        if int(policy["version"]) == 0:
            api.ok(
                "PUT",
                f"/agents/{agent['agent_id']}/access-policy",
                {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0},
            )
            agent["created_policy"] = True
            log(f"wrote a teamless access policy for {name}")

    # send ---------------------------------------------------------------------------

    def ensure_token(self, api: Api, name: str) -> str:
        agent = self.agent_entry(name)
        if not agent.get("token") or agent.get("expires_at", 0) < time.time() + 300:
            agent.update(api.mint_channel_token(agent["address"]))
            self.state.save()
        return str(agent["token"])

    def send(
        self,
        name: str,
        sender: str,
        text: str,
        conversation: str | None = None,
        delivery: str | None = None,
    ) -> dict[str, Any]:
        return self.post(self.prepare(name, sender, text, conversation, delivery))

    def prepare(
        self,
        name: str,
        sender: str,
        text: str,
        conversation: str | None = None,
        delivery: str | None = None,
    ) -> dict[str, Any]:
        """One signed turn, ready to post. Signing runs in the API container and
        takes a second or two, so a scenario that needs two sends one second
        apart prepares both first. The result holds the proof: keep it in memory.
        """

        agent = self.agent_entry(name)
        delivery_id = delivery or f"<{uuid.uuid4().hex}@identity-e2e.example.test>"
        conversation_id = conversation or f"<{uuid.uuid4().hex}@identity-e2e.example.test>"
        body = json.dumps(
            {
                "kind": "email",
                "address": agent["address"],
                "delivery_id": delivery_id,
                "conversation_id": conversation_id,
                "author": sender,
                "text": text,
                "reply_ref": "",
                "attachments": [],
            },
            separators=(",", ":"),
        ).encode()
        registration = self.data["registration"]
        signed = self.target.pod_python(
            SIGN_SCRIPT,
            {
                "key": self.data["keys"]["receiver"],
                "proof": {
                    "schema_version": 1,
                    "receiver_id": registration["receiver_id"],
                    "receiver_generation": registration["generation"],
                    "binding_id": agent["binding_id"],
                    "sender": sender.lower(),
                    "delivery_id": delivery_id,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                },
            },
        )
        with self.api() as api:
            self.ensure_token(api, name)
        return {
            "agent": name,
            "sender": sender.lower(),
            "delivery_id": delivery_id,
            "conversation_id": conversation_id,
            "body": body,
            "proof": signed["proof"],
        }

    def post(self, prepared: Mapping[str, Any]) -> dict[str, Any]:
        """Post a prepared turn to ``/channels/turns``; the send record, without the proof."""

        name = str(prepared["agent"])
        token = str(self.agent_entry(name)["token"])
        sent_at = time.time()
        status, payload = http(
            "POST",
            self.target.ports.api_url + "/channels/turns",
            raw=prepared["body"],
            headers={"X-API-Key": token, "X-Curie-Receiver-Proof": prepared["proof"]},
        )
        record = {
            "agent": name,
            "sender": prepared["sender"],
            "delivery_id": prepared["delivery_id"],
            "conversation_id": prepared["conversation_id"],
            "sent_at": sent_at,
            "http_status": status,
            "response": payload,
        }
        self.data.setdefault("sends", {}).setdefault(name, []).append(
            {k: record[k] for k in ("delivery_id", "conversation_id", "sent_at", "http_status")}
        )
        del self.data["sends"][name][:-20]
        self.state.save()
        return record

    # observe ------------------------------------------------------------------------

    def runners(self, name: str | None, since: float) -> list[dict[str, Any]]:
        """Runner containers of ``name`` (every agent when None) created at or after ``since``."""

        views = [runner_view(item) for item in self.target.inspect(*self.target.runner_names(name))]
        found = [
            view
            for view in views
            if view["created_epoch"] is not None and view["created_epoch"] >= since
        ]
        return sorted(found, key=lambda view: float(view["created_epoch"]))

    def claims(self, name: str, since: float) -> list[dict[str, Any]]:
        """The ``Harness.claims`` shape: one entry per runner container, oldest first."""

        return [
            {
                "name": view["name"],
                "created": view["created"],
                "created_epoch": view["created_epoch"],
            }
            for view in self.runners(name, since)
        ]

    def observe(
        self,
        name: str,
        *,
        since: float | None = None,
        wait: float = 180,
        conversation: str | None = None,
        reply_wait: float | None = None,
    ) -> dict[str, Any]:
        """The newest runner container's caller token for ``name``, and the turn's outcome.

        Same contract as ``identity_e2e.Harness.observe``: the container and
        its token are awaited for ``wait`` seconds; with no ``since`` the
        window starts at the agent's last send and the conversation's reply
        events are awaited too. The token is read from the container's
        environment and kept in memory only.
        """

        self.agent_entry(name)
        if since is None:
            sends = self.data.get("sends", {}).get(name, [])
            matching = [s for s in sends if conversation in (None, s["conversation_id"])]
            if matching:
                since = matching[-1]["sent_at"] - 2
                conversation = matching[-1]["conversation_id"]
            else:
                since = 0.0
        deadline = time.monotonic() + wait
        view: dict[str, Any] | None = None
        while True:
            views = self.runners(name, since)
            if views:
                view = views[-1]
                if view["token"] is not None:
                    break
            if time.monotonic() > deadline:
                break
            time.sleep(1)
        token = None if view is None else view.get("token")
        result: dict[str, Any] = {
            "agent": name,
            "since": since,
            "conversation_id": conversation,
            "claim": None if view is None else view["name"],
            "claim_created": None if view is None else view["created"],
            "claim_created_epoch": None if view is None else view["created_epoch"],
            "caller_token_present": token is not None,
        }
        reply_deadline = deadline
        if token is not None:
            decoded = decode_caller_token(token)
            result.update(
                prefix=decoded["prefix"],
                claims=decoded["payload"],
                signature="redacted",
                caller_fingerprint=caller_fingerprint(token),
            )
            reply_deadline = time.monotonic() + (wait if reply_wait is None else reply_wait)
        replies = self.replies(name, since, conversation)
        while conversation is not None and not turn_completed(replies):
            if time.monotonic() > reply_deadline:
                break
            time.sleep(2)
            replies = self.replies(name, since, conversation)
        result["replies"] = replies
        result["turn"] = turn_summary(replies)
        return result

    def replies(
        self, name: str, since: float, conversation: str | None = None
    ) -> list[dict[str, Any]]:
        """Reply events the sink recorded for ``name`` since ``since``."""

        label = self.agent_entry(name)["label"]
        out = []
        for record in self.records():
            if record.get("type") != "reply" or record.get("label") != label:
                continue
            event = record.get("event") or {}
            target = event.get("target") if isinstance(event, dict) else None
            if conversation is not None and (
                not isinstance(target, dict) or target.get("conversation_id") != conversation
            ):
                continue
            at = str(record.get("at", ""))
            if epoch(at) >= since:
                out.append(
                    {
                        "at": at,
                        "status": record.get("status"),
                        "event": summarize_event(event, REPLY_TEXT_LIMIT),
                    }
                )
        return out

    # worker -------------------------------------------------------------------------

    def worker_log(self, since: float) -> list[dict[str, Any]]:
        """The worker container's log at or after ``since`` (unix seconds), oldest first."""

        container = self.target.container(WORKER_SERVICE, running=False)
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(0.0, since - 1)))
        result = self.target.docker(
            "logs", "--timestamps", "--since", stamp, container, check=False, timeout=120
        )
        if result.returncode != 0:
            raise HarnessError(f"docker logs of the worker exited {result.returncode}")
        entries: list[dict[str, Any]] = []
        for stream in (result.stdout, result.stderr):
            entries.extend(log_entries(stream.decode("utf-8", "replace"), since, WORKER_SERVICE))
        return sorted(entries, key=lambda entry: float(entry["at"]))

    def worker_facts(self) -> dict[str, Any]:
        """What the running worker and API were configured with; never a secret value."""

        worker = self.target.service_env(WORKER_SERVICE)
        api = self.target.service_env(API_SERVICE)
        try:
            adapters = sorted(json.loads(worker.get("CURIE_ADAPTER_CREDENTIALS") or "{}"))
        except ValueError:
            adapters = ["<unparseable>"]
        return {
            "substrate": worker.get("CURIE_SANDBOX_SUBSTRATE"),
            "caller_signing_key_set": bool(
                (worker.get("CURIE_CONNECTOR_CALLER_SIGNING_KEY") or "").strip()
            ),
            "docker_network": worker.get("CURIE_DOCKER_NETWORK"),
            "runner_image": worker.get("CURIE_RUNNER_IMAGE"),
            "fake_model": worker.get("CURIE_FAKE_MODEL"),
            "connector_scope": {
                "release": worker.get("CURIE_RELEASE"),
                "namespace": worker.get("CURIE_NAMESPACE"),
            },
            "adapter_credentials_for": adapters,
            "api_http_identity_dir": api.get("CURIE_HTTP_IDENTITY_DIR"),
        }

    # down ---------------------------------------------------------------------------

    def down(self, *, keep_images: bool = False) -> dict[str, Any]:
        """Remove the stack, its runner containers and volumes, and the images it tagged."""

        target = self.target
        if self.state.exists():
            self.load()
        report: dict[str, Any] = {"action": "down", "removed": [], "residual": []}
        runners = target.runner_names()
        if runners:
            target.docker("rm", "-f", "-v", *runners, check=False, timeout=300)
            report["removed"].append(f"{len(runners)} runner container(s)")
        volumes = _text(
            target.docker("volume", "ls", "-q", "--filter", f"label={STACK_LABEL}={target.network}")
        ).split()
        if volumes:
            target.docker("volume", "rm", "-f", *volumes, check=False)
            report["removed"].append(f"{len(volumes)} runner input volume(s)")
        if target.override_path.is_file():
            target.compose(
                "down",
                "-v",
                "--remove-orphans",
                "--timeout",
                "20",
                identity=target.identity_path.is_file(),
                check=False,
                timeout=600,
            )
        # Containers of the project that no compose file describes any more.
        run_process(
            ["docker", "compose", "-p", target.project, "down", "-v", "--remove-orphans"],
            env=child_env(os.environ, {}),
            check=False,
            timeout=600,
        )
        report["removed"].append(f"compose project {target.project} and its volumes")
        target.docker("network", "rm", target.network, check=False)
        if not keep_images:
            for ref in self.data.get("owned_images", []):
                result = target.docker("image", "rm", ref, check=False)
                if result.returncode == 0:
                    report["removed"].append(f"image {ref}")
        report["verified"] = self._verify_removed(keep_images=keep_images)
        for name in (
            STATE_FILE,
            "ca.pem",
            "ca.key",
            "server.pem",
            "server.key",
            OVERRIDE_FILE,
            IDENTITY_FILE,
            CONTEXT_TAR,
        ):
            (self.state.directory / name).unlink(missing_ok=True)
        for name in ("curie-config", "bundle"):
            shutil.rmtree(self.state.directory / name, ignore_errors=True)
        with contextlib.suppress(OSError):
            self.state.directory.rmdir()
        return report

    def _verify_removed(self, *, keep_images: bool) -> dict[str, Any]:
        target = self.target
        left: dict[str, list[str]] = {}
        checks = {
            "containers": [
                "ps",
                "-a",
                "--format",
                "{{.Names}}",
                "--filter",
                f"name={target.project}",
            ],
            "project_containers": [
                "ps",
                "-a",
                "--format",
                "{{.Names}}",
                "--filter",
                f"label={COMPOSE_PROJECT_LABEL}={target.project}",
            ],
            "runner_containers": [
                "ps",
                "-a",
                "--format",
                "{{.Names}}",
                "--filter",
                f"label={STACK_LABEL}={target.network}",
            ],
            "volumes": ["volume", "ls", "-q", "--filter", f"name={target.project}"],
            "runner_volumes": [
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label={STACK_LABEL}={target.network}",
            ],
            "networks": ["network", "ls", "-q", "--filter", f"name={target.project}"],
        }
        for what, args in checks.items():
            names = _text(target.docker(*args)).split()
            if names:
                left[what] = names
        if not keep_images:
            remaining = [
                ref
                for ref in self.data.get("owned_images", [])
                if target.docker("image", "inspect", ref, check=False).returncode == 0
            ]
            if remaining:
                left["images"] = remaining
        if left:
            raise HarnessError(f"objects remain after down: {left}")
        return {"remaining": 0, "checked": sorted(checks) + ([] if keep_images else ["images"])}


# --- command line ------------------------------------------------------------------


def default_agents(
    data: Mapping[str, Any], flag: str | None, control: str | None
) -> tuple[str | None, str | None]:
    """``--flag-agent`` and ``--control-agent``, defaulting to the first two `up` registered."""

    order = [name for name in data.get("agent_order") or () if name in data.get("agents", {})]
    if flag is None and order:
        flag = order[0]
    if control is None:
        control = next((name for name in order if name != flag), None)
    return flag, control


def _default_curie_bin() -> str:
    return os.environ.get("CURIE_BIN") or shutil.which("curie") or "curie"


def _images(args: argparse.Namespace) -> Images:
    return resolve_images(
        project=args.project,
        tag=args.image_tag,
        prefix=args.image_repo_prefix,
        api_repo=args.api_repo,
        worker_repo=args.worker_repo,
        runner_repo=args.runner_repo,
        ui_repo=args.ui_repo,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="identity_compose.py",
        description="Drive real HTTP identity turns through the Docker sandbox substrate.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", required=True, help="the isolated compose project name")
    common.add_argument("--state-dir", required=True, type=Path)
    common.add_argument("--curie-bin", default=_default_curie_bin())
    common.add_argument(
        "--port-base",
        type=int,
        default=DEFAULT_PORT_BASE,
        help=f"first of {PORT_SPAN} host ports the stack publishes (up; later actions reuse it)",
    )
    images = argparse.ArgumentParser(add_help=False)
    images.add_argument("--image-tag", default="latest")
    images.add_argument(
        "--image-repo-prefix",
        default=DEFAULT_IMAGE_PREFIX,
        help="registry/namespace for curie-api, curie-worker, curie-runner and curie-ui",
    )
    for component in ("api", "worker", "runner", "ui"):
        images.add_argument(f"--{component}-repo", help=f"override the {component} repository")
    actions = parser.add_subparsers(dest="action", required=True)

    build = actions.add_parser(
        "build", parents=[common, images], help="build candidate api, worker and runner images"
    )
    build.add_argument("--build-context", required=True, type=Path)

    up = actions.add_parser("up", parents=[common, images], help="bring the isolated stack up")
    up.add_argument("--agent", action="append", required=True, dest="agents")
    up.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE)
    up.add_argument(
        "--keep-connectors",
        action="store_true",
        help="deploy the bundle's hosted connectors too (the local tier then starts them)",
    )
    up.add_argument("--roster", type=Path, help="a roster JSON replacing the default one")
    up.add_argument(
        "--build-context",
        type=Path,
        help="build the api, worker and runner images from this tarball or directory first",
    )

    send = actions.add_parser("send", parents=[common], help="post one signed email turn")
    send.add_argument("--agent", required=True)
    send.add_argument("--sender", required=True)
    send.add_argument("--text", required=True)
    send.add_argument("--conversation")
    send.add_argument("--delivery-id")

    observe = actions.add_parser("observe", parents=[common], help="decode the caller token")
    observe.add_argument("--agent", required=True)
    observe.add_argument("--conversation")
    observe.add_argument("--since", type=float, help="unix seconds; default the last send")
    observe.add_argument("--wait", type=float, default=180)
    observe.add_argument("--reply-wait", type=float)

    prove = actions.add_parser("prove", parents=[common], help="run a scripted scenario")
    prove.add_argument("--scenario", default="requester-handoff-docker")
    prove.add_argument("--evidence", type=Path)
    prove.add_argument("--flag-agent", help="default: the first agent `up` registered")
    prove.add_argument("--control-agent", help="default: the second agent `up` registered")
    prove.add_argument("--sender", default="alice@example.test")
    prove.add_argument("--second-sender", default="bob@example.test")
    prove.add_argument("--wait", type=float, default=240)
    prove.add_argument(
        "--expect-durable-wait",
        action="store_true",
        help="require the worker's `capacity wait parked ... cause=requester_handoff` line in D4",
    )

    down = actions.add_parser("down", parents=[common], help="remove the stack and its images")
    down.add_argument("--keep-images", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        target = ComposeTarget(
            project=args.project,
            state_dir=args.state_dir.resolve(),
            curie_bin=args.curie_bin,
            ports=Ports(args.port_base),
        )
        harness = ComposeHarness(target, State(args.state_dir.resolve()))
        if args.action in ("build", "up"):
            images = _images(args)
            target.image_tag = args.image_tag
            harness._init_state()
            if args.build_context is not None:
                built = harness.build(args.build_context.resolve(), images)
                if args.action == "build":
                    result: Any = {"action": "build", "built": built}
                    print(json.dumps(result, indent=2, sort_keys=True))
                    return 0
            if args.action == "up":
                roster = (
                    DEFAULT_ROSTER if args.roster is None else json.loads(args.roster.read_text())
                )
                result = harness.up(
                    args.agents,
                    args.bundle.resolve(),
                    roster,
                    images,
                    keep_connectors=args.keep_connectors,
                )
            else:
                result = {"action": "build", "built": []}
        elif args.action == "send":
            harness.load()
            result = harness.send(
                args.agent, args.sender, args.text, args.conversation, args.delivery_id
            )
        elif args.action == "observe":
            harness.load()
            result = harness.observe(
                args.agent,
                since=args.since,
                wait=args.wait,
                conversation=args.conversation,
                reply_wait=args.reply_wait,
            )
        elif args.action == "prove":
            import identity_scenarios
            import identity_scenarios_docker  # noqa: F401 - registers the docker scenario

            harness.load()
            args.flag_agent, args.control_agent = default_agents(
                harness.data, args.flag_agent, args.control_agent
            )
            evidence = identity_scenarios.run(args.scenario, harness, args)  # type: ignore[arg-type]
            if args.evidence is not None:
                args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps(evidence, indent=2, sort_keys=True))
            return 0 if evidence.get("passed") else 1
        else:
            result = harness.down(keep_images=args.keep_images)
    except HarnessError as exc:
        print(json.dumps({"action": args.action, "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(HERE))
    # The scenario module imports these by name; share one copy of each.
    sys.modules.setdefault("identity_compose", sys.modules[__name__])
    sys.exit(main())
