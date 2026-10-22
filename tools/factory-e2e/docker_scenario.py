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
   daemon: docker info, a labelled holder container, eight escape attempts, egress
   probes and an image pull) and the ``daemon`` section (written by ``collect`` from
   the daemon pod through kubectl: pod uid, docker info, nonce-labelled events). The
   two runs must use distinct claims and daemons with overlapping windows.
2. cleanup: success, cancel, worker_kill and orphan_sweep each removed the daemon's
   Pod, Service, NetworkPolicy and Secret within 300 s.
3. control: a run without the opt-in had no Docker environment, daemon or files.
4. upgrade: the upgrade plan lines and the runner image of each repository before and
   after, from the chart values (written by ``upgrade``).
5. ci: the base branch's required checks and the head's check runs and statuses
   (written by ``ci``), judged by ``factory_e2e.judge_first_head_ci``.

cleanup and control are not collected here: ``from_lifecycle`` maps them from a
``daemon-lifecycle/v1`` envelope written by daemon_lifecycle.py from a real cluster
run. Live use of this module is the w2 waypoint and acceptance.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import factory_e2e as fe

Obj = dict[str, Any]

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

EPILOG = """\
envelope (docker-scenario/v1) sections:
  runs     a and b: claim, daemon_pod, probe (from `probe`), daemon (from `collect`)
  cleanup  success, cancel, worker_kill, orphan_sweep, from the daemon-lifecycle envelope
  control  the non-opted run, from the daemon-lifecycle envelope
  upgrade  plan lines and runner images before and after (from `upgrade`)
  ci       required checks on the first published head (from `ci`)
`probe` runs where DOCKER_HOST names the run's daemon; `collect` needs an explicit
--context. Live use is the w2 waypoint and acceptance; `judge` is offline.
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


def egress_result(name: str, target: str, exit_code: int) -> Obj:
    return {"name": name, "target": target, "exit_code": exit_code, "reachable": exit_code == 0}


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
    plan_text: str, before: Obj, after: Obj, catalog_repo: str, custom_repo: str
) -> Obj:
    def images(repo: str) -> Obj:
        before_image, after_image = runner_image(before, repo), runner_image(after, repo)
        return {"repo": repo, "before": before_image, "after": after_image}

    return {
        "plan_lines": [line.strip() for line in plan_text.splitlines() if line.strip()],
        "catalog": images(catalog_repo),
        "custom": images(custom_repo),
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
    for status in statuses:
        state = GREEN_STATES.get(status["state"])
        checks.append(
            {
                "name": status["context"],
                "head_sha": first_head,
                "status": "completed" if state else "queued",
                "conclusion": state,
            }
        )
    return {"first_head": first_head, "required": list(required), "checks": checks}


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
        "control": {
            "docker_env": control["runner"]["docker_env"],
            "daemon_objects": control["daemon_objects"],
            "client_docker_files": control["client_docker_files"],
        },
    }


def assemble(nonce: str, runs: Obj, lifecycle: Obj, upgrade: Obj, ci: Obj) -> Obj:
    keys = ("claim", "daemon_pod", "probe", "daemon")
    return {
        "schema": SCHEMA,
        "nonce": nonce,
        "runs": {name: {key: run[key] for key in keys} for name, run in runs.items()},
        **from_lifecycle(lifecycle),
        "upgrade": upgrade,
        "ci": ci,
    }


# judge


def _section(name: str, check: Callable[[], list[str]]) -> list[str]:
    try:
        return check()
    except (KeyError, TypeError, AttributeError, IndexError, ValueError):
        return [f"missing_observation: {name}"]


def _window(probe: Obj) -> tuple[float, float]:
    t0, t1 = probe["window"]
    if not all(isinstance(t, int | float) and not isinstance(t, bool) for t in (t0, t1)):
        raise TypeError("window is not two numbers")
    return t0, t1


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
    foreign = sum(1 for e in events if e["Actor"]["Attributes"].get(NONCE_LABEL) != nonce)
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
            if code == 0:
                reasons.append(f"egress_reachable: {where} {name} {target['target']} answered")
            elif code != 1:
                reasons.append(f"egress_probe_failed: {where} {name} probe exited {code}")
    return reasons


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
    (a0, a1), (b0, b1) = _window(a["probe"]), _window(b["probe"])
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
    number = isinstance(settle, int | float) and not isinstance(settle, bool)
    if not number or not 0 <= settle <= LIMIT_SECONDS:
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
    return reasons


def judge(envelope: Obj) -> list[str]:
    """Every reason the envelope fails the docker scenario; empty means it passed."""

    if not isinstance(envelope, dict):
        return [f"schema: expected {SCHEMA}", "missing_observation: envelope"]
    reasons: list[str] = []
    if envelope.get("schema") != SCHEMA:
        reasons.append(f"schema: expected {SCHEMA}")
    nonce = envelope.get("nonce")
    runs = envelope.get("runs")
    if not isinstance(runs, dict):
        reasons.append("missing_observation: runs")
        runs = {}
    elif not all(isinstance(runs.get(name), dict) for name in ("a", "b")):
        missing = [name for name in ("a", "b") if not isinstance(runs.get(name), dict)]
        reasons.append(f"missing_observation: runs {', '.join(missing)}")
    for name in ("a", "b"):
        run = runs.get(name)
        if not isinstance(run, dict):
            continue
        where = f"run {name}"
        reasons += _section(f"runs {name} identity", partial(_judge_identity, where, run))
        reasons += _section(f"runs {name} events", partial(_judge_events, where, run, nonce))
        reasons += _section(
            f"runs {name} escapes", partial(_judge_run_probe, _judge_escapes, where, run)
        )
        reasons += _section(
            f"runs {name} egress", partial(_judge_run_probe, _judge_egress, where, run)
        )
    if all(isinstance(runs.get(name), dict) for name in ("a", "b")):
        reasons += _section("runs concurrency", lambda: _judge_concurrency(runs["a"], runs["b"]))
    judges: dict[str, Callable[[Obj], list[str]]] = {
        "cleanup": _judge_cleanup,
        "control": _judge_control,
        "upgrade": _judge_upgrade,
        "ci": lambda ci: [f"ci_not_green_first_head: {r}" for r in fe.judge_first_head_ci(ci)],
    }
    for section, check in judges.items():
        value = envelope.get(section)
        if not isinstance(value, dict):
            reasons.append(f"missing_observation: {section}")
            continue
        reasons += _section(section, partial(check, value))
    return reasons


# collectors


def _command(argv: list[str], timeout: int = COMMAND_TIMEOUT) -> tuple[int, str, str]:
    """(exit code, stdout, stderr); a timeout or missing binary is a nonzero code."""

    try:
        done = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return 124, "", f"{argv[0]}: timed out after {timeout}s"
    except FileNotFoundError:
        return 127, "", f"{argv[0]}: not found on PATH"
    return done.returncode, done.stdout, done.stderr


def probe(nonce: str, image: str, targets: list[tuple[str, str]]) -> Obj:
    """The probe section, run against the daemon DOCKER_HOST names; always cleans up."""

    t0 = int(time.time())
    _, info, _ = _command(["docker", "info", "--format", INFO_FORMAT])
    pull_code, _, _ = _command(["docker", "pull", image], timeout=PULL_TIMEOUT)
    label = f"{NONCE_LABEL}={nonce}"
    holder_code, holder_out, _ = _command(
        ["docker", "run", "-d", "--label", label, image, "sleep", "300"]
    )
    holder = holder_out.strip().splitlines()[-1] if holder_code == 0 and holder_out.strip() else ""
    try:
        escapes = []
        for name, argv, _ in escape_commands(image, nonce, holder):
            code, out, err = _command(argv)
            escapes.append(escape_result(name, code, out + err))
        egress = []
        for name, target in targets:
            host, _, port = target.rpartition(":")
            argv = ["docker", "run", "--rm", "--label", label, image, "nc", "-z", "-w", "5"]
            egress.append(egress_result(name, target, _command([*argv, host, port])[0]))
    finally:
        if holder:
            _command(["docker", "rm", "-f", holder])
        _command(["docker", "volume", "rm", "-f", escape_volume(nonce)])
    return {
        "window": [t0, math.ceil(time.time())],
        "info": parse_info(info),
        "holder": {"id": holder, "exit_code": holder_code},
        "escapes": escapes,
        "egress": {"pull": {"image": image, "exit_code": pull_code}, "targets": egress},
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


def _github(api_url: str, path: str, token: str | None) -> Any:
    request = Request(f"{api_url.rstrip('/')}{path}")
    request.add_header("Accept", "application/vnd.github+json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urlopen(request, timeout=COMMAND_TIMEOUT) as response:
            return json.load(response)
    except URLError as exc:
        raise CommandError(f"GET {path} failed: {exc}") from None


def collect_ci(api_url: str, repo: str, base: str, sha: str, required: list[str] | None) -> Obj:
    token = os.environ.get("GITHUB_TOKEN") or None
    rules = None if required else _github(api_url, f"/repos/{repo}/rules/branches/{base}", token)
    commit = f"/repos/{repo}/commits/{sha}"
    runs = _github(api_url, f"{commit}/check-runs?per_page=100&filter=latest", token)
    statuses = _github(api_url, f"{commit}/statuses?per_page=100", token)
    return ci_section(sha, rules, runs, statuses, required or None)


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
    probe_cmd.add_argument("--cluster-api", required=True, metavar="HOST:PORT")
    probe_cmd.add_argument("--api", required=True, metavar="HOST:PORT", help="the Curie API")
    probe_cmd.add_argument("--metadata", default=METADATA, metavar="HOST:PORT")

    collect_cmd = commands.add_parser("collect", help="read the daemon pod through kubectl")
    for flag in ("context", "namespace", "pod", "nonce", "since", "until"):
        collect_cmd.add_argument(f"--{flag}", required=True)

    upgrade_cmd = commands.add_parser("upgrade", help="upgrade plan lines and runner images")
    upgrade_cmd.add_argument("--plan", required=True, type=Path)
    upgrade_cmd.add_argument("--before", required=True, type=Path, metavar="VALUES.json")
    upgrade_cmd.add_argument("--after", required=True, type=Path, metavar="VALUES.json")
    upgrade_cmd.add_argument("--catalog-repo", required=True)
    upgrade_cmd.add_argument("--custom-repo", required=True)

    ci_cmd = commands.add_parser("ci", help="required checks on the first head (GITHUB_TOKEN)")
    for flag in ("api-url", "repo", "base", "sha"):
        ci_cmd.add_argument(f"--{flag}", required=True)
    ci_cmd.add_argument("--required", action="append", help="skip the branch rules request")

    assemble_cmd = commands.add_parser("assemble", help="merge sections into an envelope")
    assemble_cmd.add_argument("--nonce", required=True)
    assemble_cmd.add_argument("--run", action="append", required=True, metavar="a=PROBE,COLLECT")
    assemble_cmd.add_argument("--claim", action="append", required=True, metavar="a=CLAIM")
    assemble_cmd.add_argument("--lifecycle", required=True, type=Path, metavar="EVIDENCE")
    assemble_cmd.add_argument("--upgrade", required=True, type=Path)
    assemble_cmd.add_argument("--ci", required=True, type=Path)

    for command in (probe_cmd, collect_cmd, upgrade_cmd, ci_cmd, assemble_cmd):
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
    return assemble(args.nonce, runs, _read_json(args.lifecycle), upgrade, ci)


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
        elif args.command == "collect":
            section = collect(
                args.context, args.namespace, args.pod, args.nonce, args.since, args.until
            )
        elif args.command == "upgrade":
            before, after = _read_json(args.before), _read_json(args.after)
            plan = args.plan.read_text()
            section = upgrade_section(plan, before, after, args.catalog_repo, args.custom_repo)
        elif args.command == "ci":
            section = collect_ci(args.api_url, args.repo, args.base, args.sha, args.required)
        else:
            section = _assemble(args)
    except (CommandError, OSError, ValueError) as exc:
        print(f"{args.command}: {exc}", file=sys.stderr)
        return 1
    _emit(section, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
