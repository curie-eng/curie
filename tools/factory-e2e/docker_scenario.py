"""Docker scenario evidence and offline judge (ADR 0216, ADR 0217).

Two factory runs that opt into Docker must each reach only their own per-run daemon,
that daemon must refuse every host escape the authorization plugin covers, and Docker
must not open a path to the cluster API, the Curie API or cloud metadata. Around that,
the runner image of a catalog repository is rebuilt by an upgrade while a repository
with its own Dockerfile is preserved, and the change is green on its first head.

Collectors run real commands and each writes one JSON section; ``assemble`` merges
them into a ``docker-scenario/v1`` envelope and ``judge`` returns every failure reason
offline (empty means passed):

1. runs: per run ``a`` and ``b``, the claim, its daemon pod, the ``probe`` section
   (written by ``probe`` inside the runner turn, where DOCKER_HOST names the run's
   daemon: docker info, a named and labelled holder container, eight escape attempts,
   egress probes to IP:port targets and an image pull, then a verified cleanup)
   and the ``daemon`` section (written by ``collect`` from the daemon pod through
   kubectl: pod uid, docker info, nonce-labelled events). The
   two runs must use distinct claims and daemons with overlapping windows (float
   seconds, never rounded).
2. cleanup: success, cancel, worker_kill and orphan_sweep each removed the daemon's
   Pod, Service, NetworkPolicy and Secret within 300 s.
3. control: a run without the opt-in had no Docker environment, daemon or files.
4. upgrade: the upgrade plan lines, the runner image of each repository before and
   after, from the chart values, and the reply of a turn on the custom repository after
   the upgrade, which must be repo_runner's stale_base refusal (written by ``upgrade``).
5. ci: the base branch's required checks and the head's check runs and statuses
   (written by ``ci``), judged by ``factory_e2e.judge_first_head_ci``.
6. compose: a two-service Compose project (a server publishing an ephemeral port and a
   client) built and started on the run's daemon; the client reads the server by
   service name, the published port answers on the published host with the nonce
   marker, and ``down`` leaves no labelled container, network, volume or image
   (written by ``compose``).
7. skill_rung: the e2e ladder's skill rung (CURIE_E2E_TIERS=skill, CURIE_E2E_LIVE=0)
   against a curie-runner image built from the source checkout on the run's daemon,
   with the inputs' digests, the image's id and nonce label, and a verified removal
   of that image and of every Curie-managed container and volume (written by
   ``skill-rung``).

``compose`` and ``skill-rung`` run where DOCKER_HOST names the run's daemon, like
``probe``. The compose files reach the daemon only as build context and no collector
bind-mounts a host path (ADR 0217 decisions 4.1 and 8.1). The skill rung takes the
candidate CLI and the source checkout as inputs because the runner tool catalog has
no curie CLI, and it refuses to run while a curie-runner image it did not build is
present, so it never replaces or removes one.

cleanup and control are not collected here: ``from_lifecycle`` maps them from a
``daemon-lifecycle/v1`` envelope written by daemon_lifecycle.py from a real cluster
run, together with a ``lifecycle`` section holding that envelope's own judge failures.
Live use of this module is the docker scenario driver, the w2 waypoint and acceptance.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import math
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from functools import partial
from http.client import HTTPException
from pathlib import Path
from typing import Any, TypeGuard
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import factory_e2e as fe

Obj = dict[str, Any]


def _lifecycle() -> Any:
    """The daemon_lifecycle module, loaded on first use."""

    return fe._load_by_path("daemon_lifecycle", Path(__file__).with_name("daemon_lifecycle.py"))


def _real(value: object) -> TypeGuard[int | float]:
    """A real number, not a bool; floats must be finite (ints always are)."""

    return bool(_lifecycle()._number(value)) and (
        not isinstance(value, float) or math.isfinite(value)
    )


SCHEMA = "docker-scenario/v1"
LIFECYCLE_SCHEMA = "daemon-lifecycle/v1"
NONCE_LABEL = "curie.fixture.nonce"
LIMIT_SECONDS = 300
ESCAPES: tuple[tuple[str, str], ...] = (
    ("userns-host", "host-namespace"),
    ("privileged", "privileged"),
    ("cap-add", "cap-add"),
    ("host-bind", "host-bind"),
    ("mount-bind", "host-bind"),
    ("volume-driver-bind", "volume-driver-options"),
    ("seccomp-unconfined", "security-opt"),
    ("exec-privileged", "exec-privileged"),
)
EGRESS_TARGETS = ("cluster-api", "curie-api", "metadata")
METADATA = "169.254.169.254:80"
CLEANUP_PATHS = ("success", "cancel", "worker_kill", "orphan_sweep")
DAEMON_KINDS = ("Pod", "Service", "NetworkPolicy", "Secret")
KIND_BY_ROLE = {
    "daemon": "Pod",
    "service": "Service",
    "ingress": "NetworkPolicy",
    "server-secret": "Secret",
}
LIFECYCLE_PATHS = {
    "success": ("turns", "success"),
    "cancel": ("turns", "cancel"),
    "worker_kill": ("turns", "worker_loss"),
    "orphan_sweep": ("orphan",),
}
INFO_FORMAT = "{{.ID}} {{.Name}}"
DIGEST = re.compile(r"\S+@sha256:[0-9a-f]{64}")
REFUSAL = re.compile(r"refused by rule ([a-z][a-z-]*)")
REBUILD_PREFIX = "repository runner rebuild: "
GREEN_STATES = {"success": "success", "failure": "failure", "error": "failure"}
COMMAND_TIMEOUT = 120
PULL_TIMEOUT = 300
COMPOSE_PORT = 8080
COMPOSE_SERVICES = ("server", "client")
COMPOSE_FILES = ("compose.yaml", "server/Dockerfile", "server/index.html", "client/Dockerfile")
COMPOSE_LABEL = "com.docker.compose.project"
COMPOSE_KINDS = ("containers", "networks", "volumes", "images")
LADDER_FILES = ("cli/scripts/e2e-ladder.sh", "cli/scripts/e2e.sh", "cli/scripts/gnu-process.py")
LADDER_BUNDLE = "examples/weather"
RUNNER_TAG = "curie-runner"
RUNNER_DOCKERFILE = "runner/Dockerfile"
RUNNER_INSPECT_FORMAT = '{{.Id}} {{index .Config.Labels "' + NONCE_LABEL + '"}}'
LADDER_PASS = "LADDER PASS (tiers: skill)"
MANAGED_LABEL = "curietech.ai/managed-by=curie-cli"
MODEL_ENV = (
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CURIE_CREDENTIALS",
    "CURIE_MODEL_CREDENTIALS",
    "CURIE_MODEL_BASE_URL",
    "ANTHROPIC_BASE_URL",
    "CURIE_MODEL",
)
BUILD_TIMEOUT = 3600
LADDER_TIMEOUT = 3600
HTTP_ATTEMPTS = 30  # one per second
HEX64 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")

EPILOG = """\
envelope (docker-scenario/v1) sections:
  runs     a and b: claim, daemon_pod, probe (from `probe`), daemon (from `collect`)
  cleanup  success, cancel, worker_kill, orphan_sweep, from the daemon-lifecycle envelope
  control  the non-opted run, from the daemon-lifecycle envelope
  lifecycle  the daemon-lifecycle envelope's own judge failures (must be empty)
  upgrade  plan lines, runner images before and after, and the custom repository's
           refusal reply (from `upgrade`, which needs --custom-reply)
  ci       required checks on the first published head (from `ci`)
  compose  a two-service Compose project built, reached by service name and on its
           published port, then taken down without leftovers (from `compose`)
  skill_rung  the e2e ladder's skill rung against a curie-runner image built from
           the source checkout, then removed (from `skill-rung`)
`probe`, `compose` and `skill-rung` run where DOCKER_HOST names the run's daemon and
exit 1 after writing their section unless its cleanup is verified. `probe` takes IP:port
egress targets (no DNS names); `collect` needs an explicit --context. `compose` sends its
files only as build context and binds no host path (ADR 0217 decisions 4.1 and 8.1).
`skill-rung` takes the candidate CLI (--curie-bin) and the source checkout (--source)
because the runner tool catalog has no curie CLI, and refuses to run while a
curie-runner image it did not build is present. Live use is the docker scenario driver,
the w2 waypoint and acceptance; `judge` is offline.
"""


class CommandError(RuntimeError):
    """A collector's command failed; nothing was written."""


# pure helpers


def escape_commands(image: str, nonce: str, holder: str) -> list[tuple[str, list[str], str]]:
    """The escape battery as (name, docker argv, expected refusal rule), in ESCAPES order."""

    run = ["docker", "run", "--rm"]
    opts = ["--opt", "type=none", "--opt", "o=bind", "--opt", "device=/"]
    argv = {
        "userns-host": [*run, "--userns=host", image, "true"],
        "privileged": [*run, "--privileged", image, "true"],
        "cap-add": [*run, "--cap-add", "SYS_ADMIN", image, "true"],
        "host-bind": [*run, "-v", "/:/host", image, "true"],
        "mount-bind": [*run, "--mount", "type=bind,source=/,target=/host", image, "true"],
        "volume-driver-bind": ["docker", "volume", "create", *opts, escape_volume(nonce)],
        "seccomp-unconfined": [*run, "--security-opt", "seccomp=unconfined", image, "true"],
        "exec-privileged": ["docker", "exec", "--privileged", holder, "true"],
    }
    return [(name, argv[name], rule) for name, rule in ESCAPES]


def escape_volume(nonce: str) -> str:
    return f"curie-escape-{nonce}"


def parse_info(text: str) -> dict[str, str]:
    """``docker info --format '{{.ID}} {{.Name}}'`` output; blanks for any other shape."""

    fields = text.split()
    if len(fields) != 2:
        return {"id": "", "name": ""}
    return {"id": fields[0], "name": fields[1]}


def parse_refusal(text: str) -> str | None:
    match = REFUSAL.search(text)
    return match.group(1) if match else None


def parse_events(text: str) -> list[Obj]:
    """One JSON object per nonblank ``docker events --format '{{json .}}'`` line."""

    events: list[Obj] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if not isinstance(event, dict):
            raise ValueError(f"docker event is not an object: {line[:80]!r}")
        events.append(event)
    return events


def escape_result(name: str, exit_code: int, output: str) -> Obj:
    return {
        "name": name,
        "exit_code": exit_code,
        "rule": parse_refusal(output),
        "output_tail": output[-400:],
    }


def egress_result(name: str, target: str, exit_code: int, output: str = "") -> Obj:
    return {
        "name": name,
        "target": target,
        "exit_code": exit_code,
        "reachable": exit_code == 0,
        "output_tail": output[-400:],
    }


def split_target(target: object) -> tuple[str, str] | None:
    """(host, port) of ``<IPv4 or [IPv6]>:<port 1-65535>``; None for anything else."""

    if not isinstance(target, str):
        return None
    host, _, port = target.rpartition(":")
    try:
        if host.startswith("[") and host.endswith("]"):
            ipaddress.IPv6Address(host[1:-1])
            host = host[1:-1]
        else:
            ipaddress.IPv4Address(host)
    except ValueError:
        return None
    if not (port.isascii() and port.isdigit() and 1 <= int(port) <= 65535):
        return None
    return host, port


def ip_target(text: str) -> str:
    """argparse ``type=`` for an egress target."""

    if split_target(text) is None:
        raise argparse.ArgumentTypeError(f"expected IPv4:PORT or [IPv6]:PORT, got {text!r}")
    return text


def compose_marker(nonce: str) -> str:
    return f"curie-compose {nonce}"


def compose_files(nonce: str, image: str) -> dict[str, str]:
    """The compose project as relative path -> text, keyed exactly by COMPOSE_FILES.

    Both services build from ``image`` (which must ship python3); the marker reaches
    the server only through its build context, and nothing declares a volume or mount.
    """

    serve = f'["python3", "-m", "http.server", "{COMPOSE_PORT}", "-d", "/srv"]'
    return {
        "compose.yaml": (
            "services:\n"
            "  server:\n"
            "    build: ./server\n"
            "    ports:\n"
            f'      - "{COMPOSE_PORT}"\n'
            "  client:\n"
            "    build: ./client\n"
        ),
        "server/Dockerfile": f"FROM {image}\nCOPY index.html /srv/index.html\nCMD {serve}\n",
        "server/index.html": compose_marker(nonce) + "\n",
        "client/Dockerfile": f'FROM {image}\nCMD ["sleep", "infinity"]\n',
    }


def parse_port(text: str) -> int | None:
    """The port of the first ``docker compose port`` line (``HOST:PORT``), if 1-65535."""

    lines = text.strip().splitlines()
    port = lines[0].strip().rpartition(":")[2] if lines else ""
    if port.isascii() and port.isdigit() and 1 <= int(port) <= 65535:
        return int(port)
    return None


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_digest(root: Path) -> str:
    """sha256 over ``path\\0<file sha256>\\n`` for each regular file, sorted by POSIX path.

    A symlink anywhere under root (or root itself) raises ValueError.
    """

    files = []
    for path in [root, *root.rglob("*")]:
        if path.is_symlink():
            raise ValueError(f"tree_digest: {path} is a symlink")
        if path.is_file():
            files.append(path.relative_to(root).as_posix())
    digest = hashlib.sha256()
    for rel in sorted(files):
        digest.update(f"{rel}\0{sha256_file(root / rel)}\n".encode())
    return digest.hexdigest()


def _url_host(host: str) -> str:
    return f"[{host}]" if ":" in host else host


def published_url(host: str, port: int) -> str:
    return f"http://{_url_host(host)}:{port}/index.html"


def runner_image(values: Obj, repo: str) -> str:
    """``runner.image`` of the ``factory.bindings[*].repos[*]`` entry whose path is repo."""

    factory = values.get("factory") if isinstance(values, dict) else None
    bindings = factory.get("bindings") if isinstance(factory, dict) else None
    for binding in bindings if isinstance(bindings, list) else []:
        repos = binding.get("repos") if isinstance(binding, dict) else None
        for entry in repos if isinstance(repos, list) else []:
            if isinstance(entry, dict) and entry.get("path") == repo:
                runner = entry.get("runner")
                image = runner.get("image") if isinstance(runner, dict) else None
                return image if isinstance(image, str) else ""
    return ""


def upgrade_section(
    plan_text: str,
    before: Obj,
    after: Obj,
    catalog_repo: str,
    custom_repo: str,
    custom_reply: str = "",
) -> Obj:
    def images(repo: str) -> Obj:
        before_image, after_image = runner_image(before, repo), runner_image(after, repo)
        return {"repo": repo, "before": before_image, "after": after_image}

    return {
        "plan_lines": [line.strip() for line in plan_text.splitlines() if line.strip()],
        "catalog": images(catalog_repo),
        "custom": {**images(custom_repo), "reply": custom_reply.strip()},
    }


def ci_section(
    first_head: str,
    rules: list[Any] | None,
    check_runs: Obj,
    statuses: list[Any],
    required: list[str] | None = None,
) -> Obj:
    """Required check names (explicit ``required`` wins) and every check on the head."""

    if required is None:
        required = []
        for rule in rules or []:
            if rule.get("type") != "required_status_checks":
                continue
            for check in rule["parameters"]["required_status_checks"]:
                if check["context"] not in required:
                    required.append(check["context"])
    checks = [
        {key: run[key] for key in ("name", "head_sha", "status", "conclusion")}
        for run in check_runs.get("check_runs", [])
    ]
    newest: dict[str, Obj] = {}
    for status in statuses:  # newest first; keep the first row per context
        newest.setdefault(status["context"], status)
    for status in newest.values():
        state = GREEN_STATES.get(status["state"])
        checks.append(
            {
                "name": status["context"],
                "head_sha": first_head,
                "status": "completed" if state else "queued",
                "conclusion": state,
            }
        )
    return {"first_head": first_head, "required": required, "checks": checks}


def _cleanup_path(section: Obj) -> Obj:
    objects = [
        {"kind": KIND_BY_ROLE[obj["role"]], "name": obj["name"], "uid": obj["uid"]}
        for obj in section["objects"]
        if obj.get("role") in KIND_BY_ROLE
    ]
    return {
        "claim": section["claim"],
        "objects": objects,
        "absent": section["objects_absent"],
        "remaining": section["leaked"],
        "settle_seconds": section["settle_seconds"],
    }


def from_lifecycle(evidence: Obj) -> Obj:
    """cleanup and control sections from a daemon-lifecycle/v1 evidence envelope."""

    if not isinstance(evidence, dict) or evidence.get("schema") != LIFECYCLE_SCHEMA:
        raise ValueError(f"expected a {LIFECYCLE_SCHEMA} envelope")
    cleanup: Obj = {}
    for path, keys in LIFECYCLE_PATHS.items():
        section: Any = evidence
        for key in keys:
            section = section[key]
        cleanup[path] = _cleanup_path(section)
    control = evidence["control"]
    return {
        "cleanup": cleanup,
        "lifecycle": {
            "schema": evidence["schema"],
            "failures": _lifecycle().judge(evidence),
        },
        "control": {
            "docker_env": control["runner"]["docker_env"],
            "daemon_objects": control["daemon_objects"],
            "client_docker_files": control["client_docker_files"],
        },
    }


def assemble(
    nonce: str, runs: Obj, lifecycle: Obj, upgrade: Obj, ci: Obj, compose: Obj, skill_rung: Obj
) -> Obj:
    keys = ("claim", "daemon_pod", "probe", "daemon")
    return {
        "schema": SCHEMA,
        "nonce": nonce,
        "runs": {name: {key: run[key] for key in keys} for name, run in runs.items()},
        **from_lifecycle(lifecycle),
        "upgrade": upgrade,
        "ci": ci,
        "compose": compose,
        "skill_rung": skill_rung,
    }


# judge


def _section(name: str, check: Callable[[], list[str]]) -> list[str]:
    try:
        return check()
    except (KeyError, TypeError, AttributeError, IndexError, ValueError):
        return [f"missing_observation: {name}"]


def _window(probe: Obj) -> tuple[float, float] | None:
    """The probe window when it is two finite real numbers with t0 <= t1, else None."""

    window = probe["window"]
    if not isinstance(window, list) or len(window) != 2:
        return None
    t0, t1 = window
    if not (_real(t0) and _real(t1)):
        return None
    return (t0, t1) if t0 <= t1 else None


def _judge_identity(where: str, run: Obj) -> list[str]:
    probe_info, daemon_info = run["probe"]["info"], run["daemon"]["info"]
    reasons = []
    if not probe_info["id"] or probe_info["id"] != daemon_info["id"]:
        reasons.append(f"identity_mismatch: {where} probe daemon id is not the daemon pod's")
    names = {probe_info["name"], run["daemon"]["pod"], run["daemon_pod"]}
    if len(names) != 1 or not run["daemon_pod"]:
        reasons.append(f"identity_mismatch: {where} probe daemon name is not its daemon pod")
    return reasons


def _judge_events(where: str, run: Obj, nonce: object) -> list[str]:
    events = run["daemon"]["events"]
    if not isinstance(events, list) or not events:
        return [f"events_missing: {where} recorded no daemon events"]
    valid = isinstance(nonce, str) and bool(nonce)
    foreign = sum(
        1
        for e in events
        if not valid
        or NONCE_LABEL not in e["Actor"]["Attributes"]
        or e["Actor"]["Attributes"][NONCE_LABEL] != nonce
    )
    reasons = [f"events_foreign: {where} {foreign} event(s) lack nonce {nonce}"] if foreign else []
    holder = run["probe"]["holder"]["id"]
    actions = {e["Action"] for e in events if holder and e["Actor"]["ID"] == holder}
    for action in ("create", "start"):
        if action not in actions:
            reasons.append(f"events_missing: {where} holder container has no {action} event")
    return reasons


def _judge_escapes(where: str, probe: Obj) -> list[str]:
    reasons = []
    if probe["holder"]["exit_code"] != 0:
        reasons.append(f"missing_observation: {where} holder container did not run")
    escapes = probe["escapes"]
    for name, rule in ESCAPES:
        seen = [e for e in escapes if e["name"] == name]
        if len(seen) != 1:
            reasons.append(f"missing_observation: {where} escape {name} seen {len(seen)} times")
            continue
        code = seen[0]["exit_code"]
        if not isinstance(code, int):
            reasons.append(f"missing_observation: {where} escape {name} has no exit code")
        elif code == 0:
            reasons.append(f"escape_allowed: {where} escape {name} exited 0")
        elif seen[0]["rule"] != rule:
            got = seen[0]["rule"]
            reasons.append(f"escape_wrong_rule: {where} escape {name} refused by {got}, not {rule}")
    return reasons


def _judge_egress(where: str, probe: Obj) -> list[str]:
    egress = probe["egress"]
    reasons = []
    pulled = egress["pull"]["exit_code"]
    if pulled != 0:
        reasons.append(f"egress_pull_failed: {where} image pull exited {pulled}")
    targets = egress["targets"]
    for name in EGRESS_TARGETS:
        seen = [t for t in targets if t["name"] == name]
        if not seen:
            reasons.append(f"missing_observation: {where} egress target {name}")
        for target in seen:
            code = target["exit_code"]
            if split_target(target["target"]) is None:
                reasons.append(
                    f"egress_probe_failed: {where} {name} target {target['target']!r} "
                    "is not an IP:port"
                )
            elif code == 0:
                reasons.append(f"egress_reachable: {where} {name} {target['target']} answered")
            elif code != 1:
                reasons.append(f"egress_probe_failed: {where} {name} probe exited {code}")
    return reasons


def _judge_probe_cleanup(where: str, probe: Obj) -> list[str]:
    cleanup = probe["cleanup"]
    return [
        f"incomplete_cleanup: {where} probe {what} was not verified"
        for key, what in (("holder_removed", "holder removal"), ("volume_absent", "volume removal"))
        if cleanup[key] is not True
    ]


def _judge_run_probe(check: Callable[[str, Obj], list[str]], where: str, run: Obj) -> list[str]:
    return check(where, run["probe"])


def _judge_concurrency(a: Obj, b: Obj) -> list[str]:
    reasons = []
    pairs = {
        "claim": (a["claim"], b["claim"]),
        "daemon pod": (a["daemon_pod"], b["daemon_pod"]),
        "daemon id": (a["daemon"]["info"]["id"], b["daemon"]["info"]["id"]),
    }
    for what, (left, right) in pairs.items():
        if left == right:
            reasons.append(f"concurrency_shared_daemon: runs a and b share {what} {left}")
    wa, wb = _window(a["probe"]), _window(b["probe"])
    for name, window in (("a", wa), ("b", wb)):
        if window is None:
            reasons.append(f"missing_observation: run {name} window")
    if wa is not None and wb is not None:
        (a0, a1), (b0, b1) = wa, wb
        if not (a0 < b1 and b0 < a1):
            reasons.append(f"not_concurrent: windows {[a0, a1]} and {[b0, b1]} do not overlap")
    return reasons


def _judge_cleanup_path(path: str, entry: Obj) -> list[str]:
    kinds = {o["kind"] for o in entry["objects"] if o.get("name") and o.get("uid")}
    reasons = [
        f"incomplete_cleanup: {path} has no {kind} with name and uid"
        for kind in DAEMON_KINDS
        if kind not in kinds
    ]
    if entry["absent"] is not True:
        reasons.append(f"incomplete_cleanup: {path} objects were not confirmed absent")
    if entry["remaining"] != []:
        reasons.append(f"incomplete_cleanup: {path} left {entry['remaining']}")
    settle = entry["settle_seconds"]
    if not _real(settle) or not 0 <= settle <= LIMIT_SECONDS:
        reasons.append(f"incomplete_cleanup: {path} settled in {settle!r}s, limit {LIMIT_SECONDS}")
    return reasons


def _judge_cleanup(cleanup: Obj) -> list[str]:
    reasons: list[str] = []
    for path in CLEANUP_PATHS:
        entry = cleanup.get(path)
        if not isinstance(entry, dict):
            reasons.append(f"missing_observation: cleanup {path}")
            continue
        reasons += _section(f"cleanup {path}", partial(_judge_cleanup_path, path, entry))
    return reasons


def _judge_control(control: Obj) -> list[str]:
    daemon_objects = control["daemon_objects"]
    if (
        control["docker_env"] is False
        and type(daemon_objects) is int
        and daemon_objects == 0
        and control["client_docker_files"] is False
    ):
        return []
    return [f"control_docker: the non-opted run saw Docker: {control}"]


def _judge_lifecycle(lifecycle: Obj) -> list[str]:
    failures = lifecycle["failures"]
    if not isinstance(failures, list):
        raise TypeError("lifecycle failures is not a list")
    reasons = []
    if lifecycle["schema"] != LIFECYCLE_SCHEMA:
        reasons.append(f"incomplete_cleanup: lifecycle schema is not {LIFECYCLE_SCHEMA}")
    if failures:
        reasons.append(f"incomplete_cleanup: lifecycle evidence failed its own judge: {failures}")
    return reasons


def _judge_upgrade(upgrade: Obj) -> list[str]:
    lines, catalog, custom = upgrade["plan_lines"], upgrade["catalog"], upgrade["custom"]
    if not isinstance(lines, list) or not all(isinstance(line, str) for line in lines):
        return ["missing_observation: upgrade plan_lines"]
    images = (catalog["before"], catalog["after"], custom["before"], custom["after"])
    if not all(isinstance(v, str) for v in images) or not catalog["repo"] or not custom["repo"]:
        return ["missing_observation: upgrade repositories and images"]
    reasons = []
    rebuild = f"{REBUILD_PREFIX}build catalog repository {catalog['repo']} in "
    pinned = all(DIGEST.fullmatch(catalog[k]) for k in ("before", "after"))
    if not pinned or catalog["before"] == catalog["after"]:
        reasons.append(
            f"upgrade_catalog_not_rebuilt: {catalog['repo']} image {catalog['before']} -> "
            f"{catalog['after']} is not a new digest-pinned image"
        )
    if not any(line.startswith(rebuild) for line in lines):
        reasons.append(f"upgrade_catalog_not_rebuilt: no rebuild plan line for {catalog['repo']}")
    if not custom["before"] or custom["before"] != custom["after"]:
        reasons.append(
            f"upgrade_custom_rebuilt: {custom['repo']} image {custom['before']} -> "
            f"{custom['after']}"
        )
    preserved = f"custom repository runner preserved: {custom['repo']} declares Dockerfile "
    if not any(line.startswith(preserved) for line in lines):
        reasons.append(f"upgrade_custom_not_refused: no preserved plan line for {custom['repo']}")
    if any(line.startswith(REBUILD_PREFIX) and f" {custom['repo']} " in line for line in lines):
        reasons.append(f"upgrade_custom_not_refused: the plan rebuilds {custom['repo']}")
    reply = custom.get("reply")
    pool_refusal = fe.load_repo_runner()._POOL_REFUSAL_REPLY
    match = pool_refusal.fullmatch(reply) if isinstance(reply, str) else None
    if (
        match is None
        or match.group("repository") != custom["repo"]
        or match.group("stale") != "stale_base"
    ):
        reasons.append(
            f"upgrade_custom_not_refused: no stale_base refusal reply for {custom['repo']}"
        )
    return reasons


def _ok(code: object) -> bool:
    """An exit code of exactly 0 (not False, not 0.0)."""

    return type(code) is int and code == 0


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _hex64(value: object) -> bool:
    return isinstance(value, str) and HEX64.fullmatch(value) is not None


def _marked(body: object, expected: object) -> bool:
    return _text(expected) and isinstance(body, str) and str(expected) in body


def _missing(where: str, fields: list[str]) -> list[str]:
    return [f"missing_observation: {where} {field}" for field in fields]


def _common_fields(section: Obj) -> list[str]:
    """Blank or absent docker_host, published_host, info id/name and window."""

    missing = [f for f in ("docker_host", "published_host") if not _text(section.get(f))]
    info = section.get("info")
    if not isinstance(info, dict):
        missing.append("info")
    else:
        missing += [f"info {key}" for key in ("id", "name") if not _text(info.get(key))]
    if "window" not in section or _window(section) is None:
        missing.append("window")
    return missing


def _compose_fields(section: Obj) -> list[str]:
    missing = _common_fields(section)
    missing += [f for f in ("project", "expected") if not _text(section.get(f))]
    files = section.get("files")
    missing += [
        f"files {rel}"
        for rel in COMPOSE_FILES
        if not isinstance(files, dict) or not _hex64(files.get(rel))
    ]
    return _missing("compose", missing)


def _compose_step(step: str, section: Obj) -> list[str]:
    code = section[step]["exit_code"]
    return [] if _ok(code) else [f"compose_{step}_failed: compose {step} exited {code!r}"]


def _compose_services(section: Obj) -> list[str]:
    services = section["services"]
    if not isinstance(services, list):
        raise TypeError("compose services is not a list")
    reasons = []
    for name in COMPOSE_SERVICES:
        seen = [s for s in services if s["service"] == name]
        if len(seen) != 1:
            reasons.append(f"compose_service_not_running: {name} seen {len(seen)} times")
        elif seen[0]["state"] != "running":
            reasons.append(f"compose_service_not_running: {name} is {seen[0]['state']!r}")
    return reasons


def _compose_reach(section: Obj) -> list[str]:
    reach, to = section["reach"], f"server:{COMPOSE_PORT}"
    code = reach["exit_code"]
    if (
        reach["from"] == "client"
        and reach["to"] == to
        and _ok(code)
        and _marked(reach["body"], section["expected"])
    ):
        return []
    return [f"compose_service_unreachable: client did not read {to} (exit {code!r})"]


def _compose_published(section: Obj) -> list[str]:
    published, host, expected = section["published"], section["published_host"], section["expected"]
    port = published["port"]
    prefix = "compose_published_port_unreachable:"
    if type(port) is not int or not 1 <= port <= 65535:
        return [f"{prefix} server:{COMPOSE_PORT} has no published port (got {port!r})"]
    url, status = published["url"], published["status"]
    reasons = []
    if not _text(host) or url != published_url(host, port):
        reasons.append(f"{prefix} {url!r} is not port {port} on published host {host!r}")
    if type(status) is not int or status != 200:
        reasons.append(f"{prefix} {url} answered {status!r}")
    if not _marked(published["body"], expected):
        reasons.append(f"{prefix} {url} body lacks {expected!r}")
    return reasons


def _leftovers(where: str, cleanup: Obj, kinds: tuple[str, ...]) -> list[str]:
    reasons = [
        f"incomplete_cleanup: {where} left {kind} {cleanup[kind]}"
        for kind in kinds
        if cleanup[kind] != []
    ]
    if cleanup["verified"] is not True:
        reasons.append(f"incomplete_cleanup: {where} cleanup was not verified")
    return reasons


def judge_compose(section: object) -> list[str]:
    """Every reason the compose section fails; empty means it passed. Never raises."""

    if not isinstance(section, dict):
        return ["missing_observation: compose"]
    checks: dict[str, Callable[[], list[str]]] = {
        "fields": partial(_compose_fields, section),
        "build": partial(_compose_step, "build", section),
        "up": partial(_compose_step, "up", section),
        "services": partial(_compose_services, section),
        "reach": partial(_compose_reach, section),
        "published": partial(_compose_published, section),
        "down": partial(_compose_step, "down", section),
        "cleanup": lambda: _leftovers("compose", section["cleanup"], COMPOSE_KINDS),
    }
    return [r for name, check in checks.items() for r in _section(f"compose {name}", check)]


def _skill_rung_fields(section: Obj) -> list[str]:
    missing = _common_fields(section)
    if not _text(section.get("nonce")):
        missing.append("nonce")
    inputs = section.get("inputs")
    if not isinstance(inputs, dict):
        return _missing("skill_rung", [*missing, "inputs"])
    curie_bin = inputs.get("curie_bin")
    if not isinstance(curie_bin, dict):
        missing.append("inputs curie_bin")
    else:
        if not _hex64(curie_bin.get("sha256")):
            missing.append("inputs curie_bin sha256")
        if not _text(curie_bin.get("version")):
            missing.append("inputs curie_bin version")
    files = inputs.get("files")
    missing += [
        f"inputs files {rel}"
        for rel in LADDER_FILES
        if not isinstance(files, dict) or not _hex64(files.get(rel))
    ]
    bundle = inputs.get("bundle")
    if (
        not isinstance(bundle, dict)
        or bundle.get("path") != LADDER_BUNDLE
        or not _hex64(bundle.get("digest"))
    ):
        missing.append(f"inputs bundle {LADDER_BUNDLE}")
    revision = inputs.get("source_revision")
    if not isinstance(revision, str) or REVISION.fullmatch(revision) is None:
        missing.append("inputs source_revision")
    return _missing("skill_rung", missing)


def _skill_rung_image(section: Obj) -> list[str]:
    image = section["image"]
    info = section.get("info")
    daemon = info.get("id") if isinstance(info, dict) else None
    nonce = section.get("nonce")
    problems = []
    if not _ok(image["exit_code"]):
        problems.append(f"build exited {image['exit_code']!r}")
    if image["tag"] != RUNNER_TAG:
        problems.append(f"tag {image['tag']!r} is not {RUNNER_TAG}")
    if not isinstance(image["id"], str) or IMAGE_ID.fullmatch(image["id"]) is None:
        problems.append(f"id {image['id']!r} is not sha256:<64 hex>")
    if image["dockerfile"] != RUNNER_DOCKERFILE or not _hex64(image["dockerfile_sha256"]):
        problems.append(f"dockerfile {image['dockerfile']!r} is not a digested {RUNNER_DOCKERFILE}")
    if not _text(nonce) or image["label_nonce"] != nonce:
        problems.append(f"label nonce {image['label_nonce']!r} is not {nonce!r}")
    if not _text(daemon) or image["daemon_id"] != daemon:
        problems.append(f"built on daemon {image['daemon_id']!r}, not {daemon!r}")
    return [f"skill_rung_image_not_built: {problem}" for problem in problems]


def _skill_rung_ladder(section: Obj) -> list[str]:
    ladder = section["ladder"]
    reasons = []
    if ladder["tiers"] != "skill" or ladder["live"] != "0":
        reasons.append(
            f"skill_rung_wrong_mode: ladder ran tiers {ladder['tiers']!r} "
            f"live {ladder['live']!r}, not skill with live 0"
        )
    code, tail = ladder["exit_code"], ladder["output_tail"]
    if not _ok(code) or ladder["passed"] is not True or LADDER_PASS not in tail.splitlines():
        reasons.append(f"skill_rung_ladder_failed: ladder exited {code!r} without {LADDER_PASS}")
    return reasons


def _skill_rung_cleanup(section: Obj) -> list[str]:
    cleanup = section["cleanup"]
    for kind in ("containers", "networks", "volumes"):
        if not isinstance(cleanup[kind], list):
            raise TypeError(f"skill rung cleanup {kind} is not a list")
    reasons = _leftovers("skill rung", cleanup, ("containers", "networks", "volumes"))
    if cleanup["image_removed"] is not True:
        reasons.append(f"incomplete_cleanup: skill rung did not remove its {RUNNER_TAG} image")
    return reasons


def judge_skill_rung(section: object) -> list[str]:
    """Every reason the skill_rung section fails; empty means it passed. Never raises."""

    if not isinstance(section, dict):
        return ["missing_observation: skill_rung"]
    checks: dict[str, Callable[[], list[str]]] = {
        "fields": partial(_skill_rung_fields, section),
        "image": partial(_skill_rung_image, section),
        "ladder": partial(_skill_rung_ladder, section),
        "cleanup": partial(_skill_rung_cleanup, section),
    }
    return [r for name, check in checks.items() for r in _section(f"skill_rung {name}", check)]


def _judge_section_daemons(envelope: Obj, runs: Obj, present: list[str]) -> list[str]:
    """compose and skill_rung must have run on run a's or run b's daemon."""

    run_ids = set()
    for name in present:
        daemon = runs[name].get("daemon")
        info = daemon.get("info") if isinstance(daemon, dict) else None
        daemon_id = info.get("id") if isinstance(info, dict) else None
        if _text(daemon_id):
            run_ids.add(daemon_id)
    reasons = []
    for section in ("compose", "skill_rung"):
        value = envelope.get(section)
        info = value.get("info") if isinstance(value, dict) else None
        daemon_id = info.get("id") if isinstance(info, dict) else None
        if _text(daemon_id) and daemon_id not in run_ids:
            reasons.append(
                f"identity_mismatch: {section} daemon {daemon_id} is not run a's or b's daemon"
            )
    return reasons


def judge(envelope: Obj) -> list[str]:
    """Every reason the envelope fails the docker scenario; empty means it passed."""

    if not isinstance(envelope, dict):
        return [f"schema: expected {SCHEMA}", "missing_observation: envelope"]
    reasons: list[str] = []
    if envelope.get("schema") != SCHEMA:
        reasons.append(f"schema: expected {SCHEMA}")
    nonce = envelope.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        reasons.append("missing_observation: nonce")
    runs = envelope.get("runs")
    if not isinstance(runs, dict):
        reasons.append("missing_observation: runs")
        runs, present = {}, []
    else:
        present = [name for name in ("a", "b") if isinstance(runs.get(name), dict)]
        if len(present) < 2:
            missing = [name for name in ("a", "b") if name not in present]
            reasons.append(f"missing_observation: runs {', '.join(missing)}")
    for name in present:
        run = runs[name]
        where = f"run {name}"
        reasons += _section(f"runs {name} identity", partial(_judge_identity, where, run))
        reasons += _section(f"runs {name} events", partial(_judge_events, where, run, nonce))
        reasons += _section(
            f"runs {name} escapes", partial(_judge_run_probe, _judge_escapes, where, run)
        )
        reasons += _section(
            f"runs {name} egress", partial(_judge_run_probe, _judge_egress, where, run)
        )
        reasons += _section(
            f"runs {name} cleanup", partial(_judge_run_probe, _judge_probe_cleanup, where, run)
        )
    if len(present) == 2:
        reasons += _section("runs concurrency", lambda: _judge_concurrency(runs["a"], runs["b"]))
    judges: dict[str, Callable[[Obj], list[str]]] = {
        "cleanup": _judge_cleanup,
        "control": _judge_control,
        "lifecycle": _judge_lifecycle,
        "upgrade": _judge_upgrade,
        "ci": lambda ci: [f"ci_not_green_first_head: {r}" for r in fe.judge_first_head_ci(ci)],
        "compose": judge_compose,
        "skill_rung": judge_skill_rung,
    }
    for section, check in judges.items():
        value = envelope.get(section)
        if not isinstance(value, dict):
            reasons.append(f"missing_observation: {section}")
            continue
        reasons += _section(section, partial(check, value))
    if present:
        reasons += _judge_section_daemons(envelope, runs, present)
    return reasons


# collectors


def _command(
    argv: list[str], timeout: int = COMMAND_TIMEOUT, env: dict[str, str] | None = None
) -> tuple[int, str, str]:
    """(exit code, stdout, stderr); a timeout or missing binary is a nonzero code.

    ``env`` replaces the inherited environment when given. The child runs in its own
    session, and a timeout kills the child's whole process group.
    """

    try:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,
        )
    except FileNotFoundError:
        return 127, "", f"{argv[0]}: not found on PATH"
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        return 124, "", f"{argv[0]}: timed out after {timeout}s"
    return proc.returncode, stdout, stderr


def _docker_env() -> dict[str, str]:
    """A copy of this environment without the settings that redirect docker.

    DOCKER_CONTEXT and BUILDX_BUILDER can each send docker away from the daemon
    DOCKER_HOST names.
    """

    return {k: v for k, v in os.environ.items() if k not in ("DOCKER_CONTEXT", "BUILDX_BUILDER")}


def _cleanup(name: str, nonce: str) -> Obj:
    """Remove the holder and escape volume by name, then verify both are gone."""

    volume = escape_volume(nonce)
    _command(["docker", "rm", "-f", name])
    _command(["docker", "volume", "rm", "-f", volume])
    ps_code, ps_out, _ = _command(["docker", "ps", "-aq", "--filter", f"name=^{name}$"])
    ls_code, ls_out, _ = _command(["docker", "volume", "ls", "-q", "--filter", f"name=^{volume}$"])
    return {
        "holder_removed": ps_code == 0 and not ps_out.strip(),
        "volume_absent": ls_code == 0 and not ls_out.strip(),
    }


def probe(nonce: str, image: str, targets: list[tuple[str, str]]) -> Obj:
    """The probe section, run against the daemon DOCKER_HOST names; always cleans up."""

    t0 = time.time()
    name = f"curie-probe-{nonce}-{secrets.token_hex(3)}"
    label = f"{NONCE_LABEL}={nonce}"
    info, holder, holder_code, pull_code = "", "", -1, -1
    escapes: list[Obj] = []
    egress: list[Obj] = []
    try:
        _, info, _ = _command(["docker", "info", "--format", INFO_FORMAT])
        pull_code, _, _ = _command(["docker", "pull", image], timeout=PULL_TIMEOUT)
        holder_code, holder_out, _ = _command(
            ["docker", "run", "-d", "--name", name, "--label", label, image, "sleep", "300"]
        )
        lines = holder_out.strip().splitlines()
        holder = lines[-1] if holder_code == 0 and lines else ""
        for esc, argv, _ in escape_commands(image, nonce, holder):
            code, out, err = _command(argv)
            escapes.append(escape_result(esc, code, out + err))
        for target_name, target in targets:
            host, port = split_target(target) or ("", "")
            argv = ["docker", "run", "--rm", "--label", label, image, "nc", "-z", "-w", "5"]
            code, out, err = _command([*argv, host, port])
            egress.append(egress_result(target_name, target, code, out + err))
    finally:
        cleanup = _cleanup(name, nonce)
    return {
        "window": [t0, time.time()],
        "info": parse_info(info),
        "holder": {"id": holder, "exit_code": holder_code},
        "escapes": escapes,
        "egress": {"pull": {"image": image, "exit_code": pull_code}, "targets": egress},
        "cleanup": cleanup,
    }


def collect(context: str, namespace: str, pod: str, nonce: str, since: str, until: str) -> Obj:
    """The daemon section, read from the daemon pod's dockerd container."""

    kubectl = ["kubectl", "--context", context, "-n", namespace]

    def checked(argv: list[str]) -> str:
        code, out, err = _command([*kubectl, *argv])
        if code != 0:
            raise CommandError(f"kubectl {' '.join(argv)} exited {code}: {err.strip()[-400:]}")
        return out

    uid = json.loads(checked(["get", "pod", pod, "-o", "json"]))["metadata"]["uid"]
    docker = ["exec", pod, "-c", "dockerd", "--", "docker"]
    info = checked([*docker, "info", "--format", INFO_FORMAT])
    events = checked(
        [
            *docker, "events", "--since", since, "--until", until,
            "--filter", f"label={NONCE_LABEL}={nonce}", "--format", "{{json .}}",
        ]
    )  # fmt: skip
    return {"pod": pod, "pod_uid": uid, "info": parse_info(info), "events": parse_events(events)}


def _outcome(code: int, out: str, err: str) -> Obj:
    return {"exit_code": code, "output_tail": (out + err)[-400:]}


def _listing(argv: list[str]) -> tuple[bool, list[str]]:
    """(the listing exited 0 and printed nothing, the ids it printed)."""

    code, out, _ = _command(argv, env=_docker_env())
    ids = out.split()
    return code == 0 and not ids, ids


def _http_get(url: str, timeout: int = 5) -> tuple[int | None, str]:
    """(HTTP status, body text); (None, error text) when nothing answered."""

    try:
        with urlopen(url, timeout=timeout) as response:
            return response.status, response.read().decode(errors="replace")
    except HTTPError as exc:
        return exc.code, str(exc)
    except (URLError, OSError, HTTPException) as exc:
        return None, str(exc)


def _docker_host(published_host: str) -> str:
    """DOCKER_HOST, refusing an unset or empty one and an empty published host."""

    docker_host = os.environ.get("DOCKER_HOST", "")
    if not docker_host:
        raise CommandError("DOCKER_HOST must name the run's daemon")
    if not published_host:
        raise CommandError(
            "a published host is required (--published-host or DOCKER_PUBLISHED_HOST)"
        )
    return docker_host


def _parse_services(text: str) -> list[Obj]:
    """``docker compose ps --format json``: one object per line, or one JSON array."""

    stripped = text.strip()
    try:
        if stripped.startswith("["):
            rows = json.loads(stripped)
        else:
            rows = [json.loads(line) for line in stripped.splitlines() if line.strip()]
    except ValueError:
        return []
    return [
        {"service": row.get("Service", ""), "state": row.get("State", "")}
        for row in rows
        if isinstance(row, dict)
    ]


REACH_SCRIPT = """\
import sys, time, urllib.request
error = None
for attempt in range({attempts}):
    if attempt:
        time.sleep(1)
    try:
        with urllib.request.urlopen({url!r}, timeout=2) as response:
            sys.stdout.write(response.read().decode())
            sys.exit(0)
    except Exception as exc:
        error = exc
sys.exit(f"unreachable: {{error}}")
"""


def _published(host: str, port: int | None) -> Obj:
    """GET the published index up to HTTP_ATTEMPTS times, 1 s apart, until 200."""

    if port is None:
        return {"port": None, "url": "", "status": None, "body": ""}
    url = published_url(host, port)
    status, body = None, ""
    for attempt in range(HTTP_ATTEMPTS):
        if attempt:
            time.sleep(1)
        status, body = _http_get(url)
        if status == 200:
            break
    return {"port": port, "url": url, "status": status, "body": body}


def compose(nonce: str, image: str, published_host: str) -> Obj:
    """The compose section, run against the daemon DOCKER_HOST names; always cleans up.

    The files reach the daemon only as build context; nothing binds a host path.
    """

    docker_host = _docker_host(published_host)
    t0 = time.time()
    project = f"curie-compose-{nonce}-{secrets.token_hex(3)}"
    files = compose_files(nonce, image)
    expected = compose_marker(nonce)
    to = f"server:{COMPOSE_PORT}"
    info = ""
    build = up = down = {"exit_code": -1, "output_tail": ""}
    services: list[Obj] = []
    reach: Obj = {"from": "client", "to": to, "exit_code": -1, "body": ""}
    published = _published(published_host, None)
    with tempfile.TemporaryDirectory(prefix="curie-compose-") as tmp:
        root = Path(tmp)
        for rel, text in files.items():
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
        base = ["docker", "compose", "-p", project, "-f", str(root / "compose.yaml")]
        try:
            _, info, _ = _command(["docker", "info", "--format", INFO_FORMAT], env=_docker_env())
            build = _outcome(*_command([*base, "build"], timeout=BUILD_TIMEOUT, env=_docker_env()))
            if build["exit_code"] == 0:
                up = _outcome(*_command([*base, "up", "-d"], env=_docker_env()))
            if up["exit_code"] == 0:
                _, ps_out, _ = _command(
                    [*base, "ps", "--all", "--format", "json"], env=_docker_env()
                )
                services = _parse_services(ps_out)
                script = REACH_SCRIPT.format(attempts=HTTP_ATTEMPTS, url=f"http://{to}/index.html")
                code, out, _ = _command(
                    [*base, "exec", "-T", "client", "python3", "-c", script], env=_docker_env()
                )
                reach = {**reach, "exit_code": code, "body": out}
                code, out, _ = _command(
                    [*base, "port", "server", str(COMPOSE_PORT)], env=_docker_env()
                )
                published = _published(published_host, parse_port(out) if code == 0 else None)
        finally:
            down = _outcome(
                *_command(
                    [*base, "down", "--volumes", "--rmi", "local", "--remove-orphans"],
                    env=_docker_env(),
                )
            )
            label = f"label={COMPOSE_LABEL}={project}"
            listings = {
                "containers": ["docker", "ps", "-aq", "--filter", label],
                "networks": ["docker", "network", "ls", "-q", "--filter", label],
                "volumes": ["docker", "volume", "ls", "-q", "--filter", label],
                "images": ["docker", "image", "ls", "-q", "--filter", label],
            }
            results = {kind: _listing(argv) for kind, argv in listings.items()}
    return {
        "project": project,
        "window": [t0, time.time()],
        "docker_host": docker_host,
        "published_host": published_host,
        "info": parse_info(info),
        "image": image,
        "files": {rel: _sha256_text(text) for rel, text in files.items()},
        "expected": expected,
        "build": build,
        "up": up,
        "services": services,
        "reach": reach,
        "published": published,
        "down": down,
        "cleanup": {
            **{kind: ids for kind, (_, ids) in results.items()},
            "verified": all(ok for ok, _ in results.values()),
        },
    }


SKILL_RUNG_LISTINGS = {
    "containers": ["docker", "ps", "-aq", "--no-trunc"],
    "networks": ["docker", "network", "ls", "-q", "--no-trunc"],
    "volumes": ["docker", "volume", "ls", "-q"],
}
SKILL_RUNG_REMOVALS = {
    "containers": ["docker", "rm", "-f", "-v"],
    "networks": ["docker", "network", "rm"],
    "volumes": ["docker", "volume", "rm", "-f"],
}


def _ids(argv: list[str]) -> tuple[bool, list[str]]:
    """(the listing exited 0, the ids it printed)."""

    code, out, _ = _command(argv, env=_docker_env())
    return code == 0, out.split()


def _skill_rung_inputs(source: Path, curie_bin: Path) -> None:
    """Refuse, before any command runs, a missing CLI or ladder input."""

    if not curie_bin.is_file() or not os.access(curie_bin, os.X_OK):
        raise CommandError(f"--curie-bin {curie_bin} is not an executable file")
    for rel in (*LADDER_FILES, RUNNER_DOCKERFILE):
        if not (source / rel).is_file():
            raise CommandError(f"--source {source} has no {rel}")
    if not (source / LADDER_BUNDLE).is_dir():
        raise CommandError(f"--source {source} has no {LADDER_BUNDLE} bundle")


def _ladder_env(curie: Path, published_host: str) -> dict[str, str]:
    """This environment for the skill rung, without any model credential or route.

    Every inherited CURIE_E2E_ key is dropped, then the rung's own are set, so an
    inherited CURIE_E2E_IMAGE cannot run it on an image other than the recorded one.
    """

    env = {
        key: value
        for key, value in _docker_env().items()
        if key not in MODEL_ENV and not key.startswith("CURIE_E2E_")
    }
    env.update(
        CURIE_E2E_TIERS="skill",
        CURIE_E2E_LIVE="0",
        CURIE_E2E_IMAGE=RUNNER_TAG,
        CURIE_BIN=str(curie),
        DOCKER_PUBLISHED_HOST=published_host,
    )
    return env


def skill_rung(
    nonce: str, source: Path, curie_bin: Path, published_host: str, source_revision: str
) -> Obj:
    """The skill_rung section, run against the daemon DOCKER_HOST names.

    The candidate CLI and the source checkout are inputs because the runner tool
    catalog has no curie CLI. The runner image is built with the checkout as build
    context; an existing curie-runner image is refused, never replaced or removed.
    The daemon's containers, networks and volumes are snapshotted before the build and
    every one new since is removed afterwards, so the collector assumes nothing else
    creates objects on the daemon while it runs.
    """

    docker_host = _docker_host(published_host)
    _skill_rung_inputs(source, curie_bin)
    t0 = time.time()
    inputs: Obj = {
        "curie_bin": {"sha256": sha256_file(curie_bin), "version": ""},
        "files": {rel: sha256_file(source / rel) for rel in LADDER_FILES},
        "bundle": {"path": LADDER_BUNDLE, "digest": tree_digest(source / LADDER_BUNDLE)},
        "source_revision": source_revision,
    }
    dockerfile = source / RUNNER_DOCKERFILE
    with tempfile.TemporaryDirectory(prefix="curie-skill-rung-") as tmp:
        work = Path(tmp)
        repo = work / "repo"
        for rel in LADDER_FILES:
            (repo / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / rel, repo / rel)
        shutil.copytree(source / LADDER_BUNDLE, repo / LADDER_BUNDLE)
        curie = work / "bin" / "curie"
        curie.parent.mkdir()
        shutil.copyfile(curie_bin, curie)
        curie.chmod(0o755)
        code, out, _ = _command([str(curie), "--version"], env=_docker_env())
        inputs["curie_bin"]["version"] = out.strip() if code == 0 else ""

        _, info_out, _ = _command(["docker", "info", "--format", INFO_FORMAT], env=_docker_env())
        info = parse_info(info_out)
        code, out, err = _command(["docker", "image", "ls", "-q", RUNNER_TAG], env=_docker_env())
        if code != 0:
            raise CommandError(f"docker image ls {RUNNER_TAG} exited {code}: {err.strip()[-400:]}")
        if out.strip():
            raise CommandError(
                f"a {RUNNER_TAG} image ({' '.join(out.split())}) already exists on the daemon; "
                "refusing to replace an image this collector did not build"
            )
        before = {}
        for kind, argv in SKILL_RUNG_LISTINGS.items():
            code, out, err = _command(argv, env=_docker_env())
            if code != 0:
                raise CommandError(f"{' '.join(argv)} exited {code}: {err.strip()[-400:]}")
            before[kind] = set(out.split())
        image: Obj = {
            "tag": RUNNER_TAG,
            "id": "",
            "label_nonce": "",
            "dockerfile": RUNNER_DOCKERFILE,
            "dockerfile_sha256": sha256_file(dockerfile),
            "daemon_id": info["id"],
            "exit_code": -1,
            "output_tail": "",
        }
        ladder: Obj = {
            "tiers": "skill",
            "live": "0",
            "exit_code": -1,
            "passed": False,
            "output_tail": "",
        }
        try:
            build = [
                "docker", "build", "-f", str(dockerfile), "-t", RUNNER_TAG,
                "--label", f"{NONCE_LABEL}={nonce}", str(source),
            ]  # fmt: skip
            image.update(_outcome(*_command(build, timeout=BUILD_TIMEOUT, env=_docker_env())))
            if image["exit_code"] == 0:
                inspect = ["docker", "image", "inspect", "--format", RUNNER_INSPECT_FORMAT]
                code, out, _ = _command([*inspect, RUNNER_TAG], env=_docker_env())
                fields = out.split() if code == 0 else []
                image["id"] = fields[0] if fields else ""
                image["label_nonce"] = fields[1] if len(fields) > 1 else ""
                script = str(repo / "cli" / "scripts" / "e2e-ladder.sh")
                code, out, err = _command(
                    ["bash", script],
                    timeout=LADDER_TIMEOUT,
                    env=_ladder_env(curie, published_host),
                )
                ladder.update(
                    exit_code=code,
                    passed=code == 0 and LADDER_PASS in out.splitlines() + err.splitlines(),
                    output_tail=(out + err)[-4000:],
                )
        finally:
            # Everything new since the snapshot is ours; the image was proven absent above.
            listed_ok = True
            for kind, remove in SKILL_RUNG_REMOVALS.items():
                ok, ids = _ids(SKILL_RUNG_LISTINGS[kind])
                listed_ok = listed_ok and ok
                new = [i for i in ids if i not in before[kind]]
                if ok and new:
                    _command([*remove, *new], env=_docker_env())
            rm_code, _, _ = _command(["docker", "image", "rm", "-f", RUNNER_TAG], env=_docker_env())
            image_absent, _ = _listing(["docker", "image", "ls", "-q", RUNNER_TAG])
            left: dict[str, list[str]] = {}
            for kind in SKILL_RUNG_LISTINGS:
                ok, ids = _ids(SKILL_RUNG_LISTINGS[kind])
                listed_ok = listed_ok and ok
                left[kind] = [i for i in ids if i not in before[kind]]
    return {
        "window": [t0, time.time()],
        "docker_host": docker_host,
        "published_host": published_host,
        "nonce": nonce,
        "info": info,
        "inputs": inputs,
        "image": image,
        "ladder": ladder,
        "cleanup": {
            "image_removed": rm_code == 0 and image_absent,
            **left,
            "verified": listed_ok and image_absent and not any(left.values()),
        },
    }


NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


def _github(api_url: str, path: str, token: str | None) -> tuple[Any, str | None]:
    """(JSON body, the ``rel="next"`` URL from the Link header)."""

    url = path if path.startswith("http") else f"{api_url.rstrip('/')}{path}"
    request = Request(url)
    request.add_header("Accept", "application/vnd.github+json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urlopen(request, timeout=COMMAND_TIMEOUT) as response:
            match = NEXT_LINK.search(response.headers.get("Link") or "")
            return json.load(response), match.group(1) if match else None
    except URLError as exc:
        raise CommandError(f"GET {path} failed: {exc}") from None


def _github_pages(api_url: str, path: str, token: str | None) -> list[Any]:
    """Every page's body, following Link rel="next"."""

    pages = []
    next_url: str | None = path
    while next_url:
        body, next_url = _github(api_url, next_url, token)
        pages.append(body)
    return pages


def collect_ci(api_url: str, repo: str, base: str, sha: str, required: list[str] | None) -> Obj:
    token = os.environ.get("GITHUB_TOKEN") or None
    required = required or None
    rules = (
        _github(api_url, f"/repos/{repo}/rules/branches/{base}", token)[0] if not required else []
    )
    commit = f"/repos/{repo}/commits/{sha}"
    run_pages = _github_pages(api_url, f"{commit}/check-runs?per_page=100&filter=latest", token)
    runs = {"check_runs": [run for page in run_pages for run in page["check_runs"]]}
    status_pages = _github_pages(api_url, f"{commit}/statuses?per_page=100", token)
    statuses = [status for page in status_pages for status in page]
    return ci_section(sha, rules, runs, statuses, required)


# CLI


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text())


def _emit(section: Obj, out: Path | None) -> None:
    text = json.dumps(section, indent=2) + "\n"
    if out is None:
        sys.stdout.write(text)
    else:
        out.write_text(text)


def _pairs(values: list[str], flag: str) -> dict[str, str]:
    pairs = {}
    for value in values:
        name, sep, rest = value.partition("=")
        if not sep or not name or not rest:
            raise CommandError(f"{flag} expects NAME=VALUE, got {value!r}")
        pairs[name] = rest
    return pairs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        "docker_scenario.py",
        description="Docker scenario evidence and offline judge (ADR 0216, ADR 0217).",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    probe_cmd = commands.add_parser("probe", help="probe the daemon DOCKER_HOST names")
    probe_cmd.add_argument("--nonce", required=True)
    probe_cmd.add_argument("--image", required=True)
    probe_cmd.add_argument("--cluster-api", required=True, type=ip_target, metavar="IP:PORT")
    probe_cmd.add_argument(
        "--api", required=True, type=ip_target, metavar="IP:PORT", help="the Curie API"
    )
    probe_cmd.add_argument("--metadata", default=METADATA, type=ip_target, metavar="IP:PORT")

    collect_cmd = commands.add_parser("collect", help="read the daemon pod through kubectl")
    for flag in ("context", "namespace", "pod", "nonce", "since", "until"):
        collect_cmd.add_argument(f"--{flag}", required=True)

    upgrade_cmd = commands.add_parser("upgrade", help="upgrade plan lines and runner images")
    upgrade_cmd.add_argument("--plan", required=True, type=Path)
    upgrade_cmd.add_argument("--before", required=True, type=Path, metavar="VALUES.json")
    upgrade_cmd.add_argument("--after", required=True, type=Path, metavar="VALUES.json")
    upgrade_cmd.add_argument("--catalog-repo", required=True)
    upgrade_cmd.add_argument("--custom-repo", required=True)
    upgrade_cmd.add_argument(
        "--custom-reply",
        required=True,
        type=Path,
        metavar="FILE",
        help="reply text of a turn on the custom repository after the upgrade",
    )

    ci_cmd = commands.add_parser("ci", help="required checks on the first head (GITHUB_TOKEN)")
    for flag in ("api-url", "repo", "base", "sha"):
        ci_cmd.add_argument(f"--{flag}", required=True)
    ci_cmd.add_argument("--required", action="append", help="skip the branch rules request")

    published_help = "host where published ports answer (default: $DOCKER_PUBLISHED_HOST)"
    compose_cmd = commands.add_parser(
        "compose", help="build and reach a compose project on the daemon DOCKER_HOST names"
    )
    compose_cmd.add_argument("--nonce", required=True)
    compose_cmd.add_argument("--image", required=True, help="base image; must ship python3")
    compose_cmd.add_argument("--published-host", help=published_help)

    skill_cmd = commands.add_parser(
        "skill-rung", help="run the ladder's skill rung on the daemon DOCKER_HOST names"
    )
    skill_cmd.add_argument("--nonce", required=True)
    skill_cmd.add_argument("--source", required=True, type=Path, help="the source checkout")
    skill_cmd.add_argument(
        "--curie-bin", type=Path, help="the candidate curie CLI (default: $CURIE_BIN)"
    )
    skill_cmd.add_argument("--source-revision", required=True, help="commit of --source")
    skill_cmd.add_argument("--published-host", help=published_help)

    assemble_cmd = commands.add_parser("assemble", help="merge sections into an envelope")
    assemble_cmd.add_argument("--nonce", required=True)
    assemble_cmd.add_argument("--run", action="append", required=True, metavar="a=PROBE,COLLECT")
    assemble_cmd.add_argument("--claim", action="append", required=True, metavar="a=CLAIM")
    assemble_cmd.add_argument("--lifecycle", required=True, type=Path, metavar="EVIDENCE")
    assemble_cmd.add_argument("--upgrade", required=True, type=Path)
    assemble_cmd.add_argument("--ci", required=True, type=Path)
    assemble_cmd.add_argument("--compose", required=True, type=Path)
    assemble_cmd.add_argument("--skill-rung", required=True, type=Path)

    collectors = (probe_cmd, collect_cmd, upgrade_cmd, ci_cmd, compose_cmd, skill_cmd)
    for command in (*collectors, assemble_cmd):
        command.add_argument("--out", type=Path, required=command is assemble_cmd)

    judge_cmd = commands.add_parser("judge", help="judge an envelope offline")
    judge_cmd.add_argument("envelope", type=Path)
    return parser


def _assemble(args: argparse.Namespace) -> Obj:
    claims = _pairs(args.claim, "--claim")
    runs = {}
    for name, files in _pairs(args.run, "--run").items():
        probe_file, sep, collect_file = files.partition(",")
        if not sep or name not in claims:
            raise CommandError(f"--run {name} needs PROBE,COLLECT and a --claim {name}=CLAIM")
        daemon = _read_json(collect_file)
        runs[name] = {
            "claim": claims[name],
            "daemon_pod": daemon.get("pod", ""),
            "probe": _read_json(probe_file),
            "daemon": daemon,
        }
    upgrade, ci = _read_json(args.upgrade), _read_json(args.ci)
    compose_section, skill = _read_json(args.compose), _read_json(args.skill_rung)
    lifecycle = _read_json(args.lifecycle)
    return assemble(args.nonce, runs, lifecycle, upgrade, ci, compose_section, skill)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(sys.argv[1:] if argv is None else argv)
    if args.command == "judge":
        try:
            envelope = _read_json(args.envelope)
        except (OSError, ValueError) as exc:
            print(f"judge: cannot read {args.envelope}: {exc}", file=sys.stderr)
            return 2
        failures = judge(envelope)
        print(json.dumps({"result": "failed" if failures else "passed", "failures": failures}))
        return 1 if failures else 0
    try:
        if args.command == "probe":
            targets = [
                ("cluster-api", args.cluster_api),
                ("curie-api", args.api),
                ("metadata", args.metadata),
            ]
            section = probe(args.nonce, args.image, targets)
            if not all(section["cleanup"].values()):
                _emit(section, args.out)
                print(f"probe: cleanup not verified: {section['cleanup']}", file=sys.stderr)
                return 1
        elif args.command == "collect":
            section = collect(
                args.context, args.namespace, args.pod, args.nonce, args.since, args.until
            )
        elif args.command == "upgrade":
            before, after = _read_json(args.before), _read_json(args.after)
            plan = args.plan.read_text()
            reply = args.custom_reply.read_text()
            section = upgrade_section(
                plan, before, after, args.catalog_repo, args.custom_repo, reply
            )
        elif args.command == "ci":
            section = collect_ci(args.api_url, args.repo, args.base, args.sha, args.required)
        elif args.command in ("compose", "skill-rung"):
            host = args.published_host or os.environ.get("DOCKER_PUBLISHED_HOST", "")
            if args.command == "compose":
                section = compose(args.nonce, args.image, host)
            else:
                curie_bin = args.curie_bin or Path(os.environ.get("CURIE_BIN", ""))
                if not str(curie_bin) or str(curie_bin) == ".":
                    raise CommandError("--curie-bin or CURIE_BIN must name the candidate CLI")
                section = skill_rung(args.nonce, args.source, curie_bin, host, args.source_revision)
            if section["cleanup"]["verified"] is not True:
                _emit(section, args.out)
                print(
                    f"{args.command}: cleanup not verified: {section['cleanup']}", file=sys.stderr
                )
                return 1
        else:
            section = _assemble(args)
    except (CommandError, OSError, ValueError) as exc:
        print(f"{args.command}: {exc}", file=sys.stderr)
        return 1
    _emit(section, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
