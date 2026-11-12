"""Two-owner personal factory lifecycle scenario (#5379): the live driver.

``personal_profile.py --scenario two-owner`` imports this module and hands it
the parsed command. It reuses the single-profile harness instead of copying it:
``TwoOwnerRun`` subclasses ``personal_profile.LiveRun`` (namespace, image
cleanup, port-forwards, forge client, drill, finalizer, inventory) and adds what
two independently enrolled owners need:

1. an owned custody namespace ``<namespace>-grants`` created before helm;
2. a private Dex (mirrored into the run's loopback registry) and the API's OIDC
   settings, so each owner gets a real principal console session;
3. per-owner enrollment through ``curie cluster subscription`` with that
   session, never a seeded header or an installation-wide grant;
4. per-owner state (``Owner``), a fence that scales the worker to zero, deletes
   the run agent's claims and runner pods and disconnects every active owner,
   and a per-owner fence;
5. the unpaid rehearsal's admission, disconnect and restart controls.

Every input comes from the private ``--inputs-file`` descriptor
(``personal_two_owner_support.load_inputs_file``). No held value (grant,
rehearsal marker, session cookie, client secret, API key, worker token, Valkey
password) is printed, logged, recorded or placed in a child's argv; the CLI
reads session and token files the driver writes 0600 into the private work
directory, and the finalizer removes them.

The paid phases (``PAID_PHASES``) follow the live timeline: a grant probe per
owner, A1 and B1 parked on their cards with A1's pod deleted at the park, the
wrong-owner controls, A1's rejection and its fresh successor A2, both
approvals, a worker stop that lets B's real failing check queue the
production repair, A's follow-up A3, the worker restart, B-r1 held at its
prompt while A3 runs one tool, A's disconnect, the process interruption that
makes the kernel retry B's repair as B-r2, B's revision-2 approval, and the
post-disconnect refusals. Every native start is reserved first
(``TwoOwnerLedger.reserve_group``); every runner pod gets one gate stream whose
holds are released only after the harvest has read the reported transcript
and ``pto.gate_decision`` answers go. A rehearsal never reaches them.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import datetime as dt
import functools
import hashlib
import http.client
import json
import os
import queue
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Literal, NoReturn, Protocol

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import personal_owner_enforcement_support as poes  # noqa: E402
import personal_profile as pp  # noqa: E402
import personal_profile_support as pps  # noqa: E402
import personal_two_owner_support as pto  # noqa: E402

SCENARIO = "two-owner"
StepFailed = pp.StepFailed

# --------------------------------------------------------------------------
# Phases
# --------------------------------------------------------------------------

#: Unpaid phases every run takes, in order.
UNPAID_PHASES = ("refusals", "namespace", "images", "install", "setup", "forge")
#: The rehearsal-only unpaid phases after setup and forge.
REHEARSAL_ONLY_PHASES = ("drill", "rehearsal-controls")
#: The last phase a rehearsal runs; the fence and the finalizer follow it.
REHEARSAL_CUT = "rehearsal-controls"
#: The paid phases, in the live timeline's order.
PAID_PHASES = (
    "probe-a",
    "attempt-a1",
    "probe-b",
    "attempt-b1",
    "wrong-owner",
    "reject-a1",
    "attempt-a2",
    "approve-a2",
    "approve-b1",
    "worker-stop",
    "queue-a3",
    "worker-start",
    "hold-both",
    "disconnect-a",
    "process-interruption",
    "approve-b2",
    "post-disconnect",
)
#: The two methods another stream supplies; this build raises with this text.
S2B2 = "provided by stream S2b-2"

# --------------------------------------------------------------------------
# Names and labels
# --------------------------------------------------------------------------

DEX_NAME = "pto-dex"
DEX_CONFIG_SECRET = "pto-dex-config"
DEX_LABELS = {"app.kubernetes.io/name": DEX_NAME, pps.MANAGED_BY_LABEL: pps.MANAGED_BY_VALUE}
DEX_SELECTOR = ",".join(f"{key}={value}" for key, value in sorted(DEX_LABELS.items()))
#: The mirrored Dex repository in the run's loopback registry (per-run tag).
DEX_REPOSITORY = "curie-pto-dex"
#: ``ghcr.io/dexidp/dex@sha256:...``: the pin without its tag, for pull, tag and rm.
DEX_SOURCE = pto.DEX_IMAGE.split(":", 1)[0] + "@" + pto.DEX_IMAGE.split("@", 1)[1]
OIDC_SECRET = "pto-oidc-client"
OIDC_SECRET_KEY = "clientSecret"
OWNER_PRINCIPAL_LABEL = "curietech.ai/owner-principal"
CUSTODY_ENROLLMENT_LABEL = "curietech.ai/personal-enrollment"
CUSTODY_GENERATION_LABEL = "curietech.ai/personal-generation"
CLAIM_CRD = "sandboxclaims.extensions.agents.x-k8s.io"
TEMPLATE_HASH_LABEL = "pod-template-hash"
# kubectl jsonpath: one ``name|enrollment|generation`` line per custody Secret.
# Metadata only: the Secret's data never leaves the cluster.
CUSTODY_JSONPATH = (
    'jsonpath={range .items[*]}{.metadata.name}{"|"}'
    '{.metadata.labels.curietech\\.ai/personal-enrollment}{"|"}'
    '{.metadata.labels.curietech\\.ai/personal-generation}{"\\n"}{end}'
)
#: valkey-cli reads the password from stdin into REDISCLI_AUTH; argv never holds it.
VALKEY_EXEC = (
    "IFS= read -r REDISCLI_AUTH && export REDISCLI_AUTH && "
    'exec valkey-cli --no-auth-warning --json "$@"'
)

ROLLOUT_HOLD_SECONDS = 300.0
WORKER_GONE_SECONDS = 300
DISCONNECT_WAIT_SECONDS = 300.0
REQUEST_SETTLE_SECONDS = 120.0
#: After the worker restart, how long no claim of the disconnected owner may appear.
OBSERVATION_SECONDS = 60.0
ENROLL_ATTEMPTS = 3
#: ``curie`` exit code for a transient failure (cli/src/exit.rs).
CLI_TRANSIENT = 3
#: Hold policies: release every hold, keep the prompt, or keep the first hold
#: after one main-session tool has a result.
RELEASE = "release"
KEEP_PROMPT = "keep-prompt"
KEEP_AFTER_TOOL = "keep-after-tool"
#: How long a hold waits for the harvest to read its transcript to the reported size.
COVERAGE_SECONDS = 30.0
#: A kept hold counts as expired this long before the hook's own limit.
KEPT_MARGIN_SECONDS = 15.0
#: How long the gate stream may take to acknowledge a pod's subagent limits.
LIMITS_ACK_SECONDS = 60.0
#: How long a parked pod may take to report a terminal harvest, end its stream or vanish.
SETTLE_SECONDS = 120.0
QUIESCENT_SECONDS = 300.0
RESTART_SECONDS = 300.0
RUNS_STREAM = "curie:runs"
STREAM_SCAN_COUNT = 5000
THREAD_HASH_LABEL = "curietech.ai/thread-hash"
ROLE_OF_KIND = {"plan": pto.PLAN_REVIEWER, "diff": pto.DIFF_REVIEWER}
#: apps/api/src/curie_api/factory_ci.py RERUN_REQUESTED_NOTE, and its
#: rerun_refused_note for the GitHub 403 reason (forges/github/ci.py
#: RERUN_REFUSED[403]): the App token has no Actions permission.
RERUN_REQUESTED_NOTE = "Reran failed Actions jobs once at this head."
RERUN_FORBIDDEN_NOTE = "CI rerun refused: github_forbidden."
#: packages/channel-protocol work_item_events.py CI_FIRST_FIX_ROUND.
CI_REPAIR_ROUND = 2
#: Credential-shaped variable names a probe environment may hold only once.
_CREDENTIAL_NAME = re.compile(r"TOKEN|KEY|SECRET|AUTH|PASSWORD|COOKIE")
#: Every unbound transcript thread of one agent with its record ids, oldest first.
THREAD_QUERY = (
    "SELECT thread_key, (SELECT coalesce(json_agg(e ->> 'record_id'), '[]'::json) "
    "FROM jsonb_array_elements(CASE WHEN jsonb_typeof(value) = 'array' THEN value "
    "ELSE '[]'::jsonb END) AS e) AS record_ids FROM curie.thread_transcripts "
    "WHERE agent_id = :'agent'::uuid AND binding_scope IS NULL"
)
#: The gate decision reasons that are allowance breaches (every other stop is a
#: gate violation: an unreadable hold, unmapped session or incomplete capture).
ALLOWANCE_REASONS = frozenset(
    {
        "session_share",
        "session_output_tokens",
        "session_turns",
        "session_seconds",
        "owner_allocation",
        "aggregate_guard",
    }
)
_REQUEST_COLUMNS = (
    "id::text AS id, work_item_id::text AS work_item_id, sequence, status, terminal_cause, "
    "(termination_observation IS NOT NULL AND btrim(termination_observation) <> '') "
    "AS observed, teardown_unconfirmed_at IS NOT NULL AS teardown_pending, "
    "runtime_claim_name, runtime_epoch, runtime_owner, started_at IS NOT NULL AS started, "
    "owner_lost_retry, tenant_id::text AS tenant_id, model_auth_mode, "
    "owner_principal_id::text AS owner_principal_id, enrollment_generation, "
    "(runtime_heartbeat_expires_at IS NOT NULL "
    "AND runtime_heartbeat_expires_at = execution_deadline) AS heartbeat_parked"
)
_SQL_NAME = re.compile(r"[a-z][a-z0-9_]{0,30}")
_STREAM_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_.\-]{0,200}")
_STREAM_ID = re.compile(r"[-+]|\(?[0-9]{1,20}(-[0-9]{1,20})?")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


# --------------------------------------------------------------------------
# Per-owner state
# --------------------------------------------------------------------------


@dataclasses.dataclass(repr=False)
class Owner:
    """One owner's identity, enrollment and work, as the driver observed them.

    ``token`` is the held grant (live) or synthetic marker (rehearsal) for the
    leak scan; ``token_file`` is the 0600 file the CLI reads it from. The repr
    names the owner only.
    """

    name: str
    email: str
    principal_id: str = ""
    tenant_id: str = ""
    principal_status: str = ""
    session: pps.HeldSecret | None = None
    session_file: Path | None = None
    token: pps.HeldSecret | None = None
    token_file: Path | None = None
    token_kind: Literal["grant", "marker", ""] = ""
    state: str = ""
    generation: int = 0
    version: int = 0
    custody_secret: str = ""
    issue_number: int | None = None
    nonce: str = ""
    work_item_ids: list[str] = dataclasses.field(default_factory=list)
    request_ids: list[str] = dataclasses.field(default_factory=list)
    conversation_ids: list[str] = dataclasses.field(default_factory=list)
    claims: list[str] = dataclasses.field(default_factory=list)

    def __repr__(self) -> str:
        return f"<owner {self.name}>"

    def tuple(self) -> dict[str, Any]:
        """The owner tuple ``pps.owner_matches`` compares (the authorized generation)."""

        return {
            "tenant_id": self.tenant_id,
            "mode": pps.PERSONAL_MODE,
            "principal_id": self.principal_id,
            "generation": pps.OWNER_GENERATION,
        }

    def may_be_active(self) -> bool:
        """Enrolled, or an enroll whose answer was not read: the fence must disconnect it."""

        return self.state in {"active", "unknown"}


class HttpClient(Protocol):
    """``pp.JsonHttp.request``: the seam tests replace with recorded API answers."""

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        sensitive: bool = False,
    ) -> tuple[int, Any]: ...


# --------------------------------------------------------------------------
# The in-pod gate stream and one reservation group's runner state
# --------------------------------------------------------------------------


class GateChannel(Protocol):
    """One runner container's gate stream (``pto.GATE_STREAM_PROGRAM``).

    ``events`` drains what the program printed (``ready``, ``hold``, ``gone``,
    ``heartbeat``, ``applied``, ``limits_applied``, ``error``); ``send``
    writes one release or limits command; ``ended`` is true once its output
    has closed and every event was drained.
    """

    def events(self) -> list[dict[str, Any]]: ...

    def send(self, command: Mapping[str, Any]) -> None: ...

    def ended(self) -> bool: ...

    def close(self) -> None: ...


def spawn_piped(shell: pp.Shell, argv: Sequence[str]) -> subprocess.Popen[bytes]:
    """``pp.Shell.spawn`` with a stdin pipe: the same guard, environment and command log.

    The gate stream reads release commands from stdin, which ``Shell.spawn``
    opens on /dev/null. Commands carry hold keys and verdicts only.
    """

    shell.guard(argv)
    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=shell.cwd,
            env=dict(shell.env),
        )
    except OSError:
        raise StepFailed(f"{Path(argv[0]).name} could not start") from None
    if shell.log is not None:
        entry = {"argv": [shell.scrubber.scrub(arg) for arg in argv[:6]], "spawned": True}
        descriptor = os.open(
            shell.log, os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "a") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
    return process


class GateStream:
    """``GateChannel`` over a ``kubectl exec -i`` child; every byte is scanned."""

    def __init__(self, process: subprocess.Popen[bytes], scan: pps.TokenScan) -> None:
        self.process = process
        self.scan = scan
        self.items: queue.Queue[dict[str, Any]] = queue.Queue()
        self.eof = threading.Event()
        self.lock = threading.Lock()
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._drain, daemon=True).start()

    def __repr__(self) -> str:
        return "<gate stream>"

    def _read(self) -> None:
        stdout = self.process.stdout
        for line in stdout if stdout is not None else []:
            self.scan.scan("children", line)
            try:
                item = json.loads(line)
            except ValueError:
                item = None
            self.items.put(item if isinstance(item, dict) else {"error": "unreadable gate line"})
        self.eof.set()

    def _drain(self) -> None:
        stderr = self.process.stderr
        for line in stderr if stderr is not None else []:
            self.scan.scan("children", line)

    def events(self) -> list[dict[str, Any]]:
        drained: list[dict[str, Any]] = []
        while True:
            try:
                drained.append(self.items.get_nowait())
            except queue.Empty:
                return drained

    def send(self, command: Mapping[str, Any]) -> None:
        stdin = self.process.stdin
        if stdin is None:
            raise StepFailed("the gate stream has no input")
        data = (json.dumps(dict(command), sort_keys=True) + "\n").encode()
        with self.lock:
            try:
                stdin.write(data)
                stdin.flush()
            except (OSError, ValueError):
                raise StepFailed("the gate stream closed") from None

    def ended(self) -> bool:
        return self.eof.is_set() and self.items.empty()

    def close(self) -> None:
        stdin = self.process.stdin
        if stdin is not None:
            with contextlib.suppress(OSError, ValueError):
                stdin.close()
        pp.stop_process(self.process)


@dataclasses.dataclass(repr=False)
class PodRun(pp.Attempt):
    """One reservation group on one runner container, as the driver gates it.

    ``number`` is the group's manifest position and ``card`` its publication
    card (0 for none). ``sessions`` maps each native session (transcript stem)
    to the reservation seq it was mapped to at its first hold. ``baseline``
    holds the transcripts an earlier group in the same container left; a file
    that still starts with them is read past them.
    """

    owner: str = ""
    group: str = ""
    card: int = 0
    policy: str = RELEASE
    work_item_id: str = ""
    admission_key: str = ""
    conversation_id: str = ""
    admitted_owner: bool = False
    consumer: str = ""
    delivery: dict[str, bool] = dataclasses.field(default_factory=dict)
    reserved_at: float = 0.0
    started_at: float = 0.0
    claim_labels: dict[str, str] = dataclasses.field(default_factory=dict)
    gate: GateChannel | None = None
    harvest_lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    limits: dict[str, int] = dataclasses.field(default_factory=dict)
    limits_sent_at: float = 0.0
    limits_acked: bool = False
    prompt_seen: bool = False
    deferred: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    released: set[str] = dataclasses.field(default_factory=set)
    ignored: set[str] = dataclasses.field(default_factory=set)
    kept: dict[str, Any] | None = None
    kept_keys: set[str] = dataclasses.field(default_factory=set)
    kept_at: float = 0.0
    kept_files: dict[str, bytes] = dataclasses.field(default_factory=dict)
    kept_facts: pps.TranscriptFacts | None = None
    sessions: dict[str, int] = dataclasses.field(default_factory=dict)
    agent_roles: dict[str, str] = dataclasses.field(default_factory=dict)
    pending_kinds: list[str] = dataclasses.field(default_factory=list)
    baseline: dict[str, bytes] = dataclasses.field(default_factory=dict)
    exclude_ids: frozenset[str] = frozenset()
    clis: list[tuple[int, int]] = dataclasses.field(default_factory=list)
    stale_clis: set[tuple[int, int]] = dataclasses.field(default_factory=set)
    last_facts: pps.TranscriptFacts | None = None
    reviewed: dict[str, Any] | None = None
    epoch: int | None = None
    restart_count: int = 0
    branch_known: bool = False
    resolved_at: float = 0.0
    ending: bool = False
    settled: bool = False
    closed: bool = False

    def __repr__(self) -> str:
        return f"<pod run {self.group}>"


# --------------------------------------------------------------------------
# The OIDC opener: urllib, redirects off, proxies off, loopback only
# --------------------------------------------------------------------------


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


class LoopbackOpener:
    """``pto.Opener`` over urllib: no redirects, no proxy, only ``http://127.0.0.1``.

    Response bodies are scanned on the ``http`` channel; headers are not, since
    the callback's Set-Cookie carries the session before it is held.
    """

    def __init__(self, scan: pps.TokenScan, *, timeout: float = 30.0) -> None:
        self.scan = scan
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def __repr__(self) -> str:
        return "<loopback opener>"

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> tuple[int, Sequence[tuple[str, str]], bytes]:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "http" or parts.hostname != "127.0.0.1":
            raise pto.LoginFailed("opener_origin")
        request = urllib.request.Request(
            url,
            data=body,
            headers={"User-Agent": pp.USER_AGENT, **dict(headers)},
            method=method,
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                status = int(response.status)
                pairs = list(response.headers.items())
                data = response.read(1 << 20)
        except urllib.error.HTTPError as error:
            status = int(error.code)
            pairs = list(error.headers.items()) if error.headers is not None else []
            data = error.read(1 << 20)
        except (OSError, http.client.HTTPException):
            raise pto.LoginFailed("transport") from None
        self.scan.scan("http", data)
        return status, pairs, data


# --------------------------------------------------------------------------
# Inputs
# --------------------------------------------------------------------------


def inputs_for(base: pp.Inputs, scenario: pto.ScenarioInputs) -> pp.Inputs:
    """The single-profile ``Inputs`` the reused harness reads, from the descriptor."""

    target = scenario.target
    expires = min((item.usable_until for item in scenario.owners), default=0.0)
    return dataclasses.replace(
        base,
        context=target.context,
        namespace=target.namespace,
        release=target.release,
        registry=target.registry,
        registry_port=target.registry_port,
        docker_context=target.docker_context,
        kubeconfig=str(target.kubeconfig),
        app_dir=scenario.forge.app_dir,
        app=dict(scenario.forge.app),
        slack_bot_token_file=scenario.gateway.slack_bot_token_file,
        resolution_channel=scenario.gateway.resolution_channel,
        grant_probe_cli=scenario.probe_cli,
        expires_at=expires,
    )


def load_scenario(
    args: argparse.Namespace, runtime: pp.Runtime, mode: Literal["setup", "live"]
) -> tuple[pp.Inputs, pto.ScenarioInputs]:
    base = pp.base_inputs(args)
    path = getattr(args, "inputs_file", None)
    if not path:
        raise pps.Refusal("arguments_invalid", "--inputs-file")
    scenario = pto.load_inputs_file(Path(path), mode=mode, now=runtime.clock.now())
    return inputs_for(base, scenario), scenario


def schedule_summary(schedule: Mapping[str, Any]) -> dict[str, Any]:
    """The schedule check's shares, without anything private."""

    return {
        "groups": [
            {key: row[key] for key in ("name", "owner", "sessions", "share", "turn_share")}
            for row in schedule.get("groups") or []
        ],
        "owner_totals": dict(schedule.get("owner_totals") or {}),
        "aggregate": schedule.get("aggregate"),
        "aggregate_limit": schedule.get("aggregate_limit"),
        "sessions": schedule.get("sessions"),
    }


# --------------------------------------------------------------------------
# images: candidate images, the gated bundle and the mirrored Dex
# --------------------------------------------------------------------------


def bundle_gated(bundle: Path) -> bool:
    """The private bundle carries the two-owner gate hook and its hooks.json entries."""

    hooks_path = bundle / "hooks" / "hooks.json"
    gate = bundle / "hooks" / pto.GATE_FILE
    try:
        hooks = json.loads(hooks_path.read_text())
        program = gate.read_text()
    except (OSError, ValueError):
        return False
    commands = list(pto._commands(hooks))
    return program == pto.GATE_HOOK_PROGRAM and all(
        any(pto.gate_command(mode) == command for command in commands) for mode in pto.GATE_MODES
    )


def patch_bundle(bundle: Path) -> None:
    """Merge the gate hooks and write the gate program; a second call changes nothing.

    The runner layer's build context is the bundle root (examples/dark-factory
    connectors.yaml), so the lock's source digest covers hooks/ and the
    platform refuses a lock built before a later hooks edit. The gate therefore
    goes in before ``curie build``. It sits beside the bundle's own
    review_gate.py, which the runner image can import (curie_runner is present
    there), exactly as the reservation gate already relies on.
    """

    hooks_path = bundle / "hooks" / "hooks.json"
    hooks = json.loads(hooks_path.read_text())
    if not any(pto.GATE_FILE in command for command in pto._commands(hooks)):
        hooks = pto.two_owner_bundle_hooks(hooks)
        hooks_path.write_text(json.dumps(hooks, indent=2) + "\n")
    (bundle / "hooks" / pto.GATE_FILE).write_text(pto.GATE_HOOK_PROGRAM)


class TwoOwnerImageBuilder(pp.ImageBuilder):
    """``pp.ImageBuilder`` that merges the two-owner gate into the bundle before a layer build."""

    def build_layer(
        self,
        bundle: Path,
        platform: str,
        tag: str,
        pushed: list[pp.Pushed],
        local_tags: list[str],
    ) -> pps.RegistryImage:
        patch_bundle(bundle)
        return super().build_layer(bundle, platform, tag, pushed, local_tags)


def mirror_dex(shell: pp.Shell, ledger: pps.Ledger, inputs: pp.Inputs) -> dict[str, Any]:
    """Pull the pinned Dex, push it under a per-run tag and record its registry digest.

    Whether the pinned source image was present before is recorded first, so
    cleanup removes it only when this run pulled it.
    """

    images = ledger.observations("images")
    platform = images.get("platform")
    if not isinstance(platform, str) or not platform.startswith("linux/"):
        raise StepFailed("the image platform is not recorded")
    tag = pps.image_tag(inputs.candidate, inputs.run_id)
    target = f"{inputs.registry}/{DEX_REPOSITORY}:{tag}"
    present = (
        shell.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", DEX_SOURCE],
            check=False,
            timeout=60,
        ).returncode
        == 0
    )
    record: dict[str, Any] = {
        "source": DEX_SOURCE,
        "source_present_before": present,
        "repository": DEX_REPOSITORY,
        "tag": tag,
        "digest": "",
    }
    ledger.record("images", "dex", record)
    local_tags = [str(item) for item in images.get("local_tags") or []]
    if target not in local_tags:
        local_tags.append(target)
        ledger.record("images", "local_tags", local_tags)
    shell.run(["docker", "pull", "--platform", platform, DEX_SOURCE], timeout=900)
    shell.run(["docker", "tag", DEX_SOURCE, target], timeout=60)
    shell.run(["docker", "push", target], timeout=900)
    get = pps.loopback_get("127.0.0.1", inputs.registry_port)
    status, headers, body = get(
        f"/v2/{DEX_REPOSITORY}/manifests/{tag}", {"Accept": pps.MANIFEST_ACCEPT}
    )
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    reported = headers.get("docker-content-digest")
    if status != 200 or (reported and reported != digest):
        raise StepFailed("the mirrored Dex manifest could not be read back")
    pushed = [item for item in pp.recorded_pushed(ledger) if item.name != "dex"]
    pushed.append(pp.Pushed("dex", DEX_REPOSITORY, tag, digest))
    ledger.record("images", "pushed", [dataclasses.asdict(item) for item in pushed])
    record["digest"] = digest
    ledger.record("images", "dex", record)
    return record


def verify_dex(inputs: pp.Inputs, ledger: pps.Ledger) -> str:
    """The mirrored Dex as ``localhost:<port>/<repo>@sha256:...``, served by that digest."""

    dex = ledger.observations("images").get("dex")
    repository = dex.get("repository") if isinstance(dex, dict) else None
    digest = dex.get("digest") if isinstance(dex, dict) else None
    if repository != DEX_REPOSITORY or not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise pps.Refusal("candidate_image_mismatch", "dex")
    get = pps.loopback_get("127.0.0.1", inputs.registry_port)
    try:
        status, _headers, body = get(
            f"/v2/{DEX_REPOSITORY}/manifests/{digest}", {"Accept": pps.MANIFEST_ACCEPT}
        )
    except (OSError, http.client.HTTPException):
        raise pps.Refusal("candidate_image_mismatch", "dex") from None
    if status != 200 or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise pps.Refusal("candidate_image_mismatch", "dex")
    return f"{inputs.registry}/{DEX_REPOSITORY}@{digest}"


def dex_source_owned(ledger: pps.Ledger) -> bool:
    dex = ledger.observations("images").get("dex")
    return isinstance(dex, dict) and dex.get("source_present_before") is False


def remove_dex_source(shell: pp.Shell, ledger: pps.Ledger) -> None:
    """Remove the pulled Dex source image, only when this run pulled it."""

    if not dex_source_owned(ledger):
        return
    inspect = ["docker", "image", "inspect", "--format", "{{.Id}}", DEX_SOURCE]
    if shell.run(inspect, check=False, timeout=60).returncode == 0:
        shell.run(["docker", "image", "rm", DEX_SOURCE], timeout=120)


def register_image_cleanup(
    finalizer: pp.Finalizer, shell: pp.Shell, ledger: pps.Ledger, inputs: pp.Inputs
) -> None:
    """Registry manifests, the Dex source, local tags: run LIFO, local tags first."""

    port = inputs.registry_port
    finalizer.register(
        "remove-registry-images",
        lambda: pp.remove_registry_images(
            port, pp.recorded_pushed(ledger), pp.owned_tags(ledger, port, None)
        ),
    )
    finalizer.register("remove-dex-source", lambda: remove_dex_source(shell, ledger))
    finalizer.register(
        "remove-local-images",
        lambda: pp.remove_local_images(shell, pp.owned_tags(ledger, port, shell)),
    )


def cmd_images(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    inputs, _scenario = load_scenario(args, runtime, "setup")
    ledger = pto.TwoOwnerLedger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="images")
        shell = pp.make_shell(inputs, runtime)
        finalizer = pp.Finalizer(None)
        builder = TwoOwnerImageBuilder(
            inputs=inputs,
            shell=shell,
            ledger=ledger,
            finalizer=finalizer,
            out=runtime.out,
            curie_bin=pp.curie_binary(runtime.environ),
        )
        try:
            counts = builder.build()
            dex = mirror_dex(shell, ledger, inputs)
        except BaseException:
            register_image_cleanup(finalizer, shell, ledger, inputs)
            finalizer.run()
            raise
        finalizer.run()
    finally:
        ledger.close()
    runtime.out.emit(
        {
            "command": "images",
            "scenario": SCENARIO,
            "verdict": "pass",
            "counts": {**counts, "dex_mirrored": int(bool(dex.get("digest")))},
        }
    )
    return pps.EXIT_PASS


# --------------------------------------------------------------------------
# preflight, run, rehearse, judge
# --------------------------------------------------------------------------


def cmd_preflight(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    """Unpaid: approval, inputs, schedule and candidate images. No cluster mutation."""

    pp.base_inputs(args)
    pto.require_live_approval(getattr(args, "approve_live", None))
    inputs, scenario = load_scenario(args, runtime, "live")
    schedule = pto.schedule_check(scenario.prior_item_output_tokens)
    ledger = pto.TwoOwnerLedger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="preflight")
        verified = pp.verify_images(inputs, ledger)
        dex = verify_dex(inputs, ledger)
        ledger.record(
            "images",
            "preflight_digests",
            {name: image.digest for name, image in verified.items()},
        )
        ledger.record("images", "preflight_dex", dex)
    finally:
        ledger.close()
    runtime.out.emit(
        {
            "command": "preflight",
            "scenario": SCENARIO,
            "verdict": "pass",
            "checks": {
                "service_credentials_absent": True,
                "fake_mode_absent": True,
                "live_execution_approved": True,
                "inputs_file_private": True,
                "owners_independent": scenario.independent_accounts,
                "owner_grants_distinct": True,
                "owner_windows_sufficient": True,
                "session_plan_possible": schedule["verdict"] == "pass",
                "candidate_images_match": True,
                "dex_image_mirrored": True,
            },
            "counts": {"images_verified": len(verified), "owners": len(scenario.owners)},
            "schedule": schedule_summary(schedule),
        }
    )
    return pps.EXIT_PASS


def cmd_run(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    pp.base_inputs(args)
    pto.require_live_approval(getattr(args, "approve_live", None))
    inputs, scenario = load_scenario(args, runtime, "live")
    schedule = pto.schedule_check(scenario.prior_item_output_tokens)
    ledger = pto.TwoOwnerLedger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        if ledger.is_rehearsal():
            raise pps.Refusal("rehearsal_run_id", "--run-id")
        ledger.ensure_run(inputs.candidate, phase="run")
        ledger.refuse_paid_retry()
        ledger.set_prior_spend(scenario.prior_item_output_tokens)
        for owner in scenario.owners:
            ledger.set_owner_window(owner.owner, owner.usable_until)
        ledger.record("budget", "schedule_check_passed", schedule["verdict"] == "pass")
        ledger.record("budget", "schedule", schedule_summary(schedule))
        live = TwoOwnerRun(inputs=inputs, scenario=scenario, runtime=runtime, ledger=ledger)
    except (pps.Refusal, ValueError):
        ledger.close()
        raise
    return live.execute()


def cmd_rehearse(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    """Every unpaid phase and the rehearsal controls, then the fence and finalizer. No grant."""

    if getattr(args, "approve_live", None) is not None:
        raise pps.Refusal("arguments_invalid", "rehearse takes no --approve-live")
    pp.base_inputs(args)
    inputs, scenario = load_scenario(args, runtime, "setup")
    schedule = pto.schedule_check(scenario.prior_item_output_tokens)
    ledger = pto.TwoOwnerLedger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="rehearse")
        live = TwoOwnerRun(
            inputs=inputs, scenario=scenario, runtime=runtime, ledger=ledger, rehearsal=True
        )
        ledger.record("rehearsal", "schedule_check_passed", schedule["verdict"] == "pass")
        ledger.record("rehearsal", "schedule", schedule_summary(schedule))
    except (pps.Refusal, ValueError):
        ledger.close()
        raise
    return live.execute()


def cmd_judge(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    """Receipts only: no inputs file, no credential, no cluster, no inference."""

    if getattr(args, "inputs_file", None) is not None:
        raise pps.Refusal("arguments_invalid", "judge reads receipts only; drop --inputs-file")
    inputs = pp.base_inputs(args)
    ledger = pto.TwoOwnerLedger.open_existing(inputs.state_dir, inputs.run_id)
    try:
        if ledger.candidate() != inputs.candidate:
            raise pps.Refusal("candidate_mismatch", "--candidate")
        result = pto.judge(ledger, args.criterion)
    finally:
        ledger.close()
    runtime.out.emit({**result, "scenario": SCENARIO})
    return pps.EXIT_PASS if result["verdict"] == "pass" else pps.EXIT_FAIL


def cmd_grant_probe(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    raise pps.Refusal(
        "arguments_invalid", "grant-probe: the two-owner run probes each owner inside run"
    )


# --------------------------------------------------------------------------
# forge-probe: the single-profile write probe, plus read-only CI reads
# --------------------------------------------------------------------------


class CapturingOutput(pp.Output):
    """Scans like ``pp.Output`` and keeps the payload instead of printing it."""

    def __init__(self, scrubber: pps.Scrubber, scan: pps.TokenScan) -> None:
        super().__init__(scrubber, scan)
        self.payloads: list[dict[str, Any]] = []

    def emit(self, payload: Mapping[str, Any], *, channel: str = "stdout") -> None:
        self.scan.scan(channel, json.dumps(payload, sort_keys=True).encode())
        self.payloads.append(dict(payload))


def forge_reads(forge: pp.Forge) -> dict[str, Any]:
    """Read-only: the fixture's workflow files and the check runs at its default head.

    Shapes: ``GET /repos/{o}/{r}/contents/{path}?ref=`` answers a list of
    ``{"name", "type"}`` for a directory, and ``GET /repos/{o}/{r}/commits/{sha}
    /check-runs`` answers ``{"total_count", "check_runs"}``
    (https://docs.github.com/en/rest/repos/contents, .../checks/runs).
    """

    repo = forge.call("GET", forge.repo_path(""))
    default = str(repo.get("default_branch") or "") if isinstance(repo, dict) else ""
    if not default:
        raise StepFailed("the fixture repository names no default branch")
    branch = urllib.parse.quote(default, safe="")
    head = forge.call("GET", forge.repo_path(f"/git/ref/heads/{branch}"))
    sha = str(((head or {}).get("object") or {}).get("sha") or "")
    if not pps.is_commit_sha(sha):
        raise StepFailed("the fixture default branch has no readable head")
    status, listing = forge.request(
        "GET", forge.repo_path(f"/contents/.github/workflows?ref={branch}")
    )
    files = (
        sorted(
            str(item.get("name"))
            for item in listing
            if isinstance(item, dict) and item.get("type") == "file" and item.get("name")
        )
        if status == 200 and isinstance(listing, list)
        else []
    )
    runs_status, runs = forge.request(
        "GET", forge.repo_path(f"/commits/{sha}/check-runs?filter=latest&per_page=100")
    )
    checks = pps.judge_head_checks(runs, {"statuses": []}) if runs_status == 200 else None
    return {
        "workflows_listed": status == 200,
        "workflow_files": files,
        "checks_read": runs_status == 200,
        "default_head_checks": checks,
    }


def cmd_forge_probe(args: argparse.Namespace, runtime: pp.Runtime) -> int:
    inputs, scenario = load_scenario(args, runtime, "setup")
    capture = CapturingOutput(runtime.scrubber, runtime.scan)
    probe_args = argparse.Namespace(
        candidate=args.candidate,
        state_dir=args.state_dir,
        run_id=args.run_id,
        app_dir=str(scenario.forge.app_dir),
    )
    code = pp.cmd_forge_probe(probe_args, dataclasses.replace(runtime, out=capture))
    written = capture.payloads[-1] if capture.payloads else {}
    probe_checks = written.get("checks") if isinstance(written.get("checks"), dict) else {}
    shell = pp.make_shell(inputs, runtime)
    forge = pp.Forge(
        shell,
        pp.forge_http(runtime.scan),
        scenario.forge.app_dir,
        inputs.app,
        runtime.scrubber,
        runtime.clock,
    )
    reads: dict[str, Any] = {}
    try:
        reads = forge_reads(forge)
    except (StepFailed, pps.Refusal):
        reads = {}
    finally:
        try:
            forge.revoke()
        except (StepFailed, OSError):
            pass
    ledger = pto.TwoOwnerLedger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="forge-probe")
        for key in ("workflows_listed", "workflow_files", "checks_read", "default_head_checks"):
            ledger.record("forge", key, reads.get(key))
    finally:
        ledger.close()
    checks = {
        **{str(key): bool(value) for key, value in (probe_checks or {}).items()},
        "forge_workflows_listed": reads.get("workflows_listed") is True,
        "forge_workflows_present": bool(reads.get("workflow_files")),
        "forge_checks_read": reads.get("checks_read") is True,
    }
    passed = code == pps.EXIT_PASS and written.get("verdict") == "pass" and all(checks.values())
    runtime.out.emit(
        {
            "command": "forge-probe",
            "scenario": SCENARIO,
            "verdict": "pass" if passed else "fail",
            "checks": checks,
            "counts": {"workflow_files": len(reads.get("workflow_files") or [])},
        }
    )
    return pps.EXIT_PASS if passed else pps.EXIT_FAIL


# --------------------------------------------------------------------------
# The live run
# --------------------------------------------------------------------------


class TwoOwnerRun(pp.LiveRun):
    """The two-owner scenario on ``pp.LiveRun``'s harness.

    ``rehearsal`` runs the unpaid phases, the drill and the rehearsal controls,
    then the fence; it reads no grant and cannot reach a paid phase. A live
    run takes the paid phases after setup and forge.
    """

    def __init__(
        self,
        *,
        inputs: pp.Inputs,
        scenario: pto.ScenarioInputs,
        runtime: pp.Runtime,
        ledger: pto.TwoOwnerLedger,
        shell: pp.Shell | None = None,
        resolver: Callable[[str], list[str]] | None = None,
        http_factory: Callable[[int], HttpClient] | None = None,
        forge_http: pp.JsonHttp | None = None,
        opener: pto.Opener | None = None,
        rehearsal: bool = False,
        stop_after: str | None = None,
        gate_factory: Callable[[PodRun], GateChannel] | None = None,
    ) -> None:
        # phases() runs inside LiveRun.__init__ and must know the mode already.
        self.scenario_rehearsal = rehearsal
        super().__init__(
            inputs=inputs,
            runtime=runtime,
            ledger=ledger,
            shell=shell,
            resolver=resolver,
            stop_after=None,
        )
        if rehearsal != (scenario.mode == "setup"):
            raise ValueError("a rehearsal takes a setup descriptor and a run a live one")
        names = [name for name, _phase in self.phases()]
        if rehearsal:
            cut = REHEARSAL_CUT if stop_after is None else stop_after
            if cut not in names or any(
                name in PAID_PHASES for name in names[: names.index(cut) + 1]
            ):
                raise ValueError("a rehearsal cut must stop before every paid phase")
            self.stop_after = cut
            self.rehearsal = True
            self.command = "rehearse"
            ledger.mark_rehearsal()
        elif stop_after is not None:
            raise ValueError("a live run takes no phase cut")
        self.scenario = scenario
        self.owner_ledger = ledger
        self.http_factory: Callable[[int], HttpClient] = http_factory or (
            lambda port: pp.JsonHttp("127.0.0.1", port, tls=False, scan=self.scan)
        )
        self.forge_http = forge_http
        self.opener: pto.Opener = opener or LoopbackOpener(self.scan)
        self.owners: dict[str, Owner] = {
            name: Owner(name=name, email=pto.OWNER_EMAILS[name]) for name in pto.OWNERS
        }
        self.grant_namespace = scenario.target.grant_namespace
        self.issuer = pto.dex_issuer(DEX_NAME, inputs.namespace)
        self.redirect_uri = pto.api_redirect_uri(inputs.release, inputs.namespace)
        self.dex_image = ""
        self.api_port = 0
        self.dex_port = 0
        self.forwards: list[Any] = []
        self.valkey_password: pps.HeldSecret | None = None
        self.agent_tenant = ""
        self.custody_seen: set[str] = set()
        self.claims_seen: set[str] = set()
        self.worker_observations: list[dict[str, Any]] = []
        self.cards: list[str] = []
        self.owner_dir = inputs.work_dir / "owners"
        # -- paid-phase state ------------------------------------------------
        self.gate_factory = gate_factory
        self.pods: list[PodRun] = []
        self.pumping = False
        self.reserved_at: dict[str, float] = {}
        self.breaches: list[str] = []
        self.violations: list[str] = []
        self.unmapped: list[str] = []
        self.releases = 0
        self.summaries: dict[str, list[dict[str, Any]]] = {}
        self.zero_classes: list[dict[str, Any]] = []
        self.deliveries: dict[str, list[dict[str, Any]]] = {name: [] for name in pto.OWNERS}
        self.resolutions: dict[str, list[float]] = {name: [] for name in pto.OWNERS}
        self.checks: dict[str, dict[str, list[bool]]] = {name: {} for name in pto.OWNERS}
        self.approver_checks: list[bool] = []
        self.no_push: dict[int, bool] = {}
        self.threads: dict[str, str] = {}
        self.markers: dict[str, str] = {}
        self.pod_deletions: list[tuple[str, str, float]] = []
        self.worker_before: list[str] = []
        self.worker_started_at = 0.0
        self.a_stop_at: float | None = None
        self.b1_head = ""
        self.b1_pull: int | None = None
        self.b1_check_names: list[str] = []
        self.b2_done_at: float | None = None
        self.controls_done_at: float | None = None
        self.worker_name = ""
        self.default_branch = ""
        self.claims_after_b2: set[str] = set()

    def __repr__(self) -> str:
        return "<two-owner run>"

    # -- phases -------------------------------------------------------------

    def phases(self) -> list[tuple[str, Callable[[], None]]]:
        unpaid: list[tuple[str, Callable[[], None]]] = [
            ("refusals", self.phase_refusals),
            ("namespace", self.phase_namespace),
            ("images", self.phase_images),
            ("install", self.phase_install),
            ("setup", self.phase_setup),
            ("forge", self.phase_forge),
        ]
        if self.scenario_rehearsal:
            return [
                *unpaid,
                ("drill", self.phase_drill),
                ("rehearsal-controls", self.phase_rehearsal_controls),
            ]
        return [*unpaid, *self.paid_phases()]

    def paid_phases(self) -> list[tuple[str, Callable[[], None]]]:
        """The live timeline after forge, in ``PAID_PHASES`` order."""

        methods: dict[str, Callable[[], None]] = {
            "probe-a": self.phase_probe_a,
            "attempt-a1": self.phase_attempt_a1,
            "probe-b": self.phase_probe_b,
            "attempt-b1": self.phase_attempt_b1,
            "wrong-owner": self.phase_wrong_owner,
            "reject-a1": self.phase_reject_a1,
            "attempt-a2": self.phase_attempt_a2,
            "approve-a2": self.phase_approve_a2,
            "approve-b1": self.phase_approve_b1,
            "worker-stop": self.phase_worker_stop,
            "queue-a3": self.phase_queue_a3,
            "worker-start": self.phase_worker_start,
            "hold-both": self.phase_hold_both,
            "disconnect-a": self.phase_disconnect_a,
            "process-interruption": self.phase_process_interruption,
            "approve-b2": self.phase_approve_b2,
            "post-disconnect": self.phase_post_disconnect,
        }
        return [(name, methods[name]) for name in PAID_PHASES]

    def require_live(self, name: str) -> None:
        """A rehearsal never takes a paid phase."""

        if self.rehearsal or self.scenario.mode != "live":
            raise pps.Refusal("rehearsal_run_id", f"{name} is a paid phase")

    # -- the live window and every wait pumps the gates --------------------------

    def deadline(self) -> float:
        """The live window's end on the two-owner ceiling (pto.LIVE_SECONDS)."""

        started = self.ledger.run_row()["live_started_at"]
        return float(started) + pto.LIVE_SECONDS if started is not None else self.clock.now()

    def wait_until(
        self,
        condition: Callable[[], bool],
        *,
        until: float,
        what: str,
        every: float = pp.POLL_SECONDS,
    ) -> None:
        """``pp.LiveRun.wait_until`` that serves every open gate before each check."""

        while True:
            self.pump()
            if condition():
                return
            if self.clock.now() >= until:
                raise StepFailed(f"{what} did not happen in time")
            self.clock.wait(every)

    def pause(self, seconds: float) -> None:
        """Let ``seconds`` pass while the gates are served."""

        until = self.clock.now() + seconds
        while self.clock.now() < until:
            self.pump()
            self.clock.wait(min(pp.POLL_SECONDS, max(0.0, until - self.clock.now())))

    def active_pods(self) -> list[PodRun]:
        return [run for run in self.pods if not run.closed]

    def pod_run(self, group: str) -> PodRun:
        for run in self.pods:
            if run.group == group:
                return run
        raise StepFailed(f"{group} has no runner")

    def pump(self) -> None:
        """Serve every open gate: apply its events, then check its clocks."""

        if self.pumping:
            return
        self.pumping = True
        try:
            for run in self.active_pods():
                self.pump_pod(run)
        finally:
            self.pumping = False

    def pump_pod(self, run: PodRun) -> None:
        if run.gate is not None:
            for event in run.gate.events():
                self.gate_event(run, event)
        self.check_clock(run)

    def gate_event(self, run: PodRun, event: Mapping[str, Any]) -> None:
        if "hold" in event:
            record, key = event.get("hold"), event.get("key")
            if not isinstance(record, dict) or record.get("key", key) != key:
                self.stop_session(run, "gate_hold_unreadable")
            self.handle_hold(run, {**record, "key": key})
        elif "limits_applied" in event:
            if event.get("limits_applied") != run.limits:
                self.stop_session(run, "gate_limits_mismatch")
            run.limits_acked = True
            deferred, run.deferred = run.deferred, []
            for record in deferred:
                self.handle_hold(run, record)
        elif "gone" in event:
            key = str(event.get("gone") or "")
            if key in run.released or key in run.ignored or run.ending:
                return
            # A hold the driver neither released nor kept on purpose timed out in
            # the hook, which failed closed: the session was blocked, unplanned.
            self.stop_session(run, "gate_hold_expired")
        elif "error" in event:
            self.stop_session(run, "gate_command_refused")

    # -- one hold ---------------------------------------------------------------

    @staticmethod
    def cli_identity(record: Mapping[str, Any]) -> tuple[int, int] | None:
        pid, start = record.get("cli_pid"), record.get("cli_start")
        if type(pid) is int and type(start) is int and pid > 0 and start >= 0:
            return pid, start
        return None

    def handle_hold(self, run: PodRun, record: Mapping[str, Any]) -> None:
        """Release, keep or stop one hold (plan "Gating every response").

        The harvest must have read every transcript the hold names to the
        reported size; the session's usage, the owner's spend and the
        aggregate (settled plus every open session's live usage) then go to
        ``pto.gate_decision``. A stop interrupts the runner and fences.
        """

        key = str(record.get("key") or "")
        mode = record.get("mode")
        if not pto.GATE_KEY.fullmatch(key) or mode not in pto.GATE_MODES:
            self.stop_session(run, "gate_hold_unreadable")
        if key in run.released or key in run.ignored or key in run.kept_keys:
            return
        cli = self.cli_identity(record)
        if cli is not None and cli in run.stale_clis:
            # A hold file the interrupted process left behind: never released.
            run.ignored.add(key)
            return
        if cli is None:
            self.stop_session(run, "native_process_unknown", record=record)
        if cli not in run.clis:
            run.clis.append(cli)
        if mode == "prompt":
            run.prompt_seen = True
            if not run.limits_acked:
                run.deferred.append(dict(record))
                return
        if not self.await_coverage(run, record):
            self.stop_session(run, "capture_incomplete", record=record)
        files = self.pod_files(run)
        facts = self.pod_facts(run, files)
        stem, role, session = self.session_of(run, record, facts)
        usage = pto.hold_usage(session)
        reservation = self.map_session(run, stem, role, usage)
        now = self.clock.now()
        live = self.live_usage()
        if usage is not None:
            live[reservation.seq] = usage.output_tokens
        reading = pto.elapsed_reading(session, facts.sessions, now)
        verdict, reason = pto.gate_decision(
            session_usage=usage,
            reservation=reservation,
            owner_spent=(
                self.owner_ledger.owner_spent(run.owner, live=live) if usage is not None else None
            ),
            aggregate_spent=(
                self.owner_ledger.aggregate_spent(live=live) if usage is not None else None
            ),
            session_started_at=now - reading,
            now=now,
        )
        if verdict != "go":
            self.stop_session(run, reason, record=record, role=role)
        if self.keeps(run, record, files):
            run.kept_keys.add(key)
            if run.kept is None:
                run.kept, run.kept_at = dict(record), now
                run.kept_files, run.kept_facts = files, facts
            return
        if mode == "response" and record.get("tool_name") in pps.AGENT_TOOLS:
            kind = record.get("subagent_kind")
            if kind in ROLE_OF_KIND:
                run.pending_kinds.append(str(kind))
        self.release(run, key, "go")
        self.releases += 1

    def keeps(self, run: PodRun, record: Mapping[str, Any], files: Mapping[str, bytes]) -> bool:
        """Whether this hold is one the scenario keeps on purpose (B-r1's prompt, A3's next)."""

        if run.policy == KEEP_PROMPT:
            return record.get("mode") == "prompt" or run.kept is not None
        if run.policy == KEEP_AFTER_TOOL:
            if run.kept is not None:
                return True
            if record.get("mode") == "prompt":
                return False
            calls = pto.native_tool_calls(
                files, exclude_ids=run.exclude_ids, not_before=self.not_before(run)
            )
            return any(not call["subagent"] and call["result"] for call in calls)
        return False

    def release(self, run: PodRun, key: str, verdict: str) -> None:
        if run.gate is None:
            raise StepFailed(f"{run.group} has no gate stream")
        run.gate.send({"release": key, "verdict": verdict})
        if verdict == "go":
            run.released.add(key)

    def await_coverage(self, run: PodRun, record: Mapping[str, Any]) -> bool:
        """Wait until the harvest synced after the hold and holds every reported byte."""

        wanted: list[tuple[str, int]] = []
        for path_key, size_key in (
            ("transcript_path", "transcript_size"),
            ("agent_transcript_path", "agent_transcript_size"),
        ):
            path, size = record.get(path_key), record.get(size_key)
            if path is None:
                continue
            rel = pto.hold_relpath(path)
            if rel is None:
                return False
            if size is None:
                continue
            if type(size) is not int or size < 0:
                return False
            wanted.append((rel, size))
        written = record.get("written_at")
        if not isinstance(written, (int, float)) or isinstance(written, bool):
            return False
        until = self.clock.now() + COVERAGE_SECONDS
        while True:
            with run.harvest_lock:
                harvest = run.harvest
                covered = (
                    harvest is not None
                    and not harvest.gaps
                    and not harvest.bad_lines
                    and harvest.synced_at is not None
                    and harvest.synced_at >= float(written)
                    and all(len(harvest.files.get(rel, b"")) >= size for rel, size in wanted)
                )
            if covered:
                return True
            if self.clock.now() >= until:
                return False
            self.clock.wait(0.5)

    def not_before(self, run: PodRun) -> float:
        return run.reserved_at - pp.REPLAY_MARGIN_SECONDS

    def pod_files(self, run: PodRun) -> dict[str, bytes]:
        """The run's harvested transcripts, past any earlier group's baseline in its container."""

        if run.harvest is None:
            return {}
        with run.harvest_lock:
            files = run.harvest.transcript_files()
        for path, earlier in run.baseline.items():
            if files.get(path, b"").startswith(earlier):
                files[path] = files[path][len(earlier) :]
        return files

    def pod_facts(
        self, run: PodRun, files: Mapping[str, bytes] | None = None
    ) -> pps.TranscriptFacts:
        facts = pps.parse_native_transcripts(
            self.pod_files(run) if files is None else files,
            exclude_ids=run.exclude_ids,
            not_before=self.not_before(run),
        )
        for session in facts.sessions:
            if session.role == pps.UNATTRIBUTED:
                agent = PurePosixPath(session.path).name.removeprefix("agent-")
                session.role = run.agent_roles.get(agent, pps.UNATTRIBUTED)
        run.last_facts = facts
        return facts

    def session_of(
        self, run: PodRun, record: Mapping[str, Any], facts: pps.TranscriptFacts
    ) -> tuple[str, str, pps.SessionFacts | None]:
        """The native session a hold belongs to: transcript stem, role and parsed facts."""

        main = pto.hold_relpath(record.get("transcript_path"))
        agent = record.get("agent_id")
        if isinstance(agent, str) and agent:
            own = pto.hold_relpath(record.get("agent_transcript_path"))
            if own is not None:
                stem = own.removesuffix(".jsonl")
            elif main is not None and PurePosixPath(main).stem == f"agent-{agent}":
                stem = main.removesuffix(".jsonl")
            elif main is not None:
                stem = f"{main.removesuffix('.jsonl')}/subagents/agent-{agent}"
            else:
                self.stop_session(run, "session_unknown", record=record)
            if PurePosixPath(stem).name != f"agent-{agent}":
                self.stop_session(run, "session_unknown", record=record)
            session = next((item for item in facts.sessions if item.path == stem), None)
            role = run.agent_roles.get(agent)
            if session is not None and session.role in pps.REVIEWER_ROLES:
                role = session.role
            if role is None and run.pending_kinds:
                role = ROLE_OF_KIND[run.pending_kinds.pop(0)]
            role = role or pps.UNATTRIBUTED
            run.agent_roles[agent] = role
            if session is not None:
                session.role = role
            return stem, role, session
        if main is None:
            self.stop_session(run, "session_unknown", record=record)
        stem = main.removesuffix(".jsonl")
        session = next(
            (item for item in facts.sessions if item.path == stem and item.role == pps.IMPLEMENTER),
            None,
        )
        return stem, pps.IMPLEMENTER, session

    def map_session(
        self, run: PodRun, stem: str, role: str, usage: pps.SessionUsage | None
    ) -> pps.Reservation:
        """The reservation a session takes at its first hold; none left fences the run."""

        by_seq = {item.seq: item for item in run.reservations}
        seq = run.sessions.get(stem)
        if seq is not None:
            return by_seq[seq]
        taken = set(run.sessions.values())
        free = next(
            (item for item in run.reservations if item.role == role and item.seq not in taken),
            None,
        )
        if free is None:
            self.unmapped.append(f"{run.group}:{role}")
            self.owner_ledger.record_unreserved_session(run.owner, run.group, role, usage)
            self.ledger.record("roles", "sessions_mapped", False)
            self.stop_session(run, "unmapped_session", role=role)
        run.sessions[stem] = free.seq
        return free

    def live_usage(self) -> dict[int, int]:
        """Output tokens each mapped, unsettled session has written, by reservation seq."""

        live: dict[int, int] = {}
        for run in self.active_pods():
            if run.settled or run.last_facts is None:
                continue
            by_path = {item.path: item for item in run.last_facts.sessions}
            for stem, seq in run.sessions.items():
                session = by_path.get(stem)
                if session is not None:
                    live[seq] = session.output_tokens
        return live

    def check_clock(self, run: PodRun) -> None:
        """The session deadline and kept-hold limits, on a timer from harvest data."""

        if run.closed or run.harvest is None:
            return
        now = self.clock.now()
        expired = now - run.kept_at >= pto.HOLD_SECONDS - KEPT_MARGIN_SECONDS
        if run.kept is not None and not run.eof.is_set() and expired:
            # The hook would fail the held call closed and the model would answer it.
            self.stop_session(run, "kept_hold_expired")
        if run.settled or run.ending:
            return
        if run.deferred and run.limits_sent_at and now - run.limits_sent_at > LIMITS_ACK_SECONDS:
            self.stop_session(run, "gate_limits_unacknowledged")
        facts = self.pod_facts(run)
        by_seq = {item.seq: item for item in run.reservations}
        for session in facts.sessions:
            seq = run.sessions.get(session.path)
            limit = float(pto.SESSION_SECONDS)
            if seq is not None:
                limit = min(limit, float(by_seq[seq].seconds))
            if pto.elapsed_reading(session, facts.sessions, now) >= limit:
                self.stop_session(run, "session_seconds", role=session.role)

    # -- stopping, fencing and charging -------------------------------------------

    def stop_session(
        self,
        run: PodRun,
        reason: str,
        *,
        record: Mapping[str, Any] | None = None,
        role: str = "",
    ) -> NoReturn:
        """Stop the hold, interrupt the runner, charge the group, fence, and fail."""

        entry = f"{run.group}:{role or '-'}:{reason}"
        if reason in ALLOWANCE_REASONS:
            self.breaches.append(entry)
            self.ledger.record("budget", "allowance_breach", sorted(set(self.breaches)))
            self.ledger.record("budget", "no_allowance_breach", False)
        else:
            self.violations.append(entry)
            self.ledger.record("budget", "gate_violations", sorted(set(self.violations)))
            self.ledger.record("budget", "gate_every_response", False)
        key = str((record or {}).get("key") or "")
        if run.gate is not None and pto.GATE_KEY.fullmatch(key):
            with contextlib.suppress(StepFailed):
                self.release(run, key, "stop")
        self.safe(lambda: self.interrupt_runner(run), None)
        self.charge_open()
        self.safe(self.fence_paid_starts, None)
        raise StepFailed(f"{run.group}: the gate stopped a session ({reason}); paid work fenced")

    def interrupt_runner(self, run: PodRun) -> None:
        """Delete the runner pod at once; the hook already failed the held call closed."""

        run.ending = True
        if run.pod:
            self.pod_deletions.append((run.pod, run.pod_uid, self.clock.now()))
            self.kube.ctl(
                "delete",
                "pod",
                run.pod,
                "--ignore-not-found",
                "--grace-period=0",
                "--wait=false",
                timeout=60,
                check=False,
            )

    def charge_open(self) -> None:
        """Every reserved, unsettled session becomes unknown: charged at its full share."""

        for row in self.owner_ledger.session_rows():
            if row["source"] == "reserved" and row["output_tokens"] is None:
                self.ledger.settle(int(row["seq"]), None, source="unknown")

    def fail_paid(self, message: str) -> NoReturn:
        """A paid phase failed: charge what is open, fence, and stop."""

        self.charge_open()
        self.safe(self.fence_paid_starts, None)
        raise StepFailed(message + "; paid work fenced")

    def require(self, checks: Mapping[str, bool], what: str) -> None:
        failed = sorted(key for key, value in checks.items() if value is not True)
        if failed:
            self.fail_paid(f"{what} failed: " + ", ".join(failed))

    # -- settlement ---------------------------------------------------------------

    def settle_run(self, run: PodRun, *, facts: pps.TranscriptFacts | None, complete: bool) -> None:
        """Settle one group from transcript evidence; unknown or unmapped usage fences.

        The interrupted group (B-r1, held at its prompt) settles to zero only
        when the capture was complete and holds no assistant record at all.
        Every other group goes through ``pps.settle_attempt``: an incomplete
        capture leaves every slot unknown (charged at its share), a slot with
        no session settles not_started, and a session beyond the group's
        reservations is recorded unreserved.
        """

        group = pto.GROUPS[run.group]
        if group.kind == "interrupted":
            zero = (
                complete
                and facts is not None
                and not facts.malformed_lines
                and not facts.truncated_files
                and all(
                    item.assistant_turns == 0 and item.synthetic == 0 for item in facts.sessions
                )
            )
            elapsed = max((item.elapsed for item in facts.sessions), default=0.0) if facts else 0.0
            for reservation in run.reservations:
                self.ledger.settle(
                    reservation.seq,
                    pps.SessionUsage(0, 0, round(elapsed, 3), "") if zero else None,
                    source="native",
                )
            counts = {
                "settled": len(run.reservations) if zero else 0,
                "unknown": 0 if zero else len(run.reservations),
                "unreserved": 0,
            }
        else:
            counts = pps.settle_attempt(self.ledger, run.reservations, facts, complete=complete)
        run.facts, run.settled = facts, True
        summaries = [pps.session_summary(item) for item in facts.sessions] if facts else []
        self.summaries[run.group] = summaries
        self.ledger.record("roles", f"sessions_{run.group}", summaries)
        self.classify_zero(run, summaries, interrupted=group.kind == "interrupted")
        if counts["unreserved"]:
            self.unmapped.append(f"{run.group}:settle")
            self.ledger.record("roles", "sessions_mapped", False)
        if counts["unknown"] or counts["unreserved"]:
            self.violations.append(f"{run.group}:-:usage_unknown_or_unmapped")
            self.ledger.record("budget", "gate_violations", sorted(set(self.violations)))
            self.fail_paid(f"{run.group}: usage unknown or unmapped, charged at its share")

    def classify_zero(
        self, run: PodRun, summaries: Sequence[Mapping[str, Any]], *, interrupted: bool
    ) -> None:
        """Every zero-output or synthetic session keeps a sanitized class (budget key)."""

        for row in self.owner_ledger.session_rows():
            if row["grp"] == run.group and row["source"] == "not_started":
                self.zero_classes.append(
                    {"group": run.group, "role": row["role"], "class": "not_started"}
                )
        for summary in summaries:
            if summary.get("output_tokens") == 0 or summary.get("synthetic"):
                planned = "prompt_hold" if interrupted else None
                self.zero_classes.append(
                    {
                        "group": run.group,
                        "role": summary.get("role"),
                        "class": pto.zero_usage_class(summary, planned=planned),
                    }
                )
        if interrupted and not summaries and run.settled:
            self.zero_classes.append(
                {"group": run.group, "role": pps.IMPLEMENTER, "class": "prompt_hold"}
            )
        self.ledger.record("budget", "zero_usage_sessions", self.zero_classes)

    def settled_known(self, group: str) -> bool:
        rows = [row for row in self.owner_ledger.session_rows() if row["grp"] == group]
        return bool(rows) and all(
            row["output_tokens"] is not None and row["source"] in {"native", "not_started"}
            for row in rows
        )

    def owner_message_ids(self, owner: str) -> frozenset[str]:
        return frozenset(
            item
            for run in self.pods
            if run.owner == owner and run.facts is not None
            for item in run.facts.message_ids()
        )

    # -- runner pods: reservation, discovery, streams, park and removal ------------

    def new_run(self, group: str, *, card: int = 0, policy: str = RELEASE) -> PodRun:
        spec = pto.GROUPS[group]
        return PodRun(
            number=pto.group_index(group), owner=spec.owner, group=group, card=card, policy=policy
        )

    def reserve(self, run: PodRun) -> None:
        """Persist the group's reservations (``reserve_group``) before the start they cover."""

        now = self.clock.now()
        run.reservations = self.owner_ledger.reserve_group(run.group, now=now)
        run.reserved_at = now
        self.reserved_at[run.group] = now
        self.attempts[run.number] = run

    def admit_group(self, run: PodRun, owner: Owner) -> None:
        """Reserve the group, then admit it on the owner's own tracker issue."""

        self.reserve(run)
        run.admission_key = str(uuid.uuid4())
        run.admitted_at = self.clock.now()
        status, body = self.admit_owner(owner, issue_owner=owner, request_id=run.admission_key)
        work_item = body.get("work_item") if isinstance(body, dict) else None
        request = body.get("request") if isinstance(body, dict) else None
        if status != 200 or not isinstance(work_item, dict) or not isinstance(request, dict):
            # A 4xx refusal admitted nothing; any other answer leaves usage unknown.
            refused = 400 <= status < 500
            for item in run.reservations:
                self.ledger.settle(
                    item.seq,
                    pps.SessionUsage(0, 0, 0.0, "") if refused else None,
                    source="not_started" if refused else "unknown",
                )
            run.settled = True
            self.fail_paid(f"{run.group} admission returned {status}")
        try:
            run.work_item_id = str(uuid.UUID(str(work_item.get("id"))))
            run.request_id = str(uuid.UUID(str(request.get("id"))))
        except ValueError:
            self.fail_paid(f"{run.group} admission named no readable ids")
        run.sequence = request.get("sequence") if type(request.get("sequence")) is int else None
        run.conversation_id = str(work_item.get("conversation_id") or "")
        run.admitted_owner = pps.owner_matches(
            pps.owner_of(work_item), owner.tuple()
        ) and pps.owner_matches(pps.owner_of(request), owner.tuple())
        if run.work_item_id not in owner.work_item_ids:
            owner.work_item_ids.append(run.work_item_id)
        owner.request_ids.append(run.request_id)
        if run.conversation_id not in owner.conversation_ids:
            owner.conversation_ids.append(run.conversation_id)

    def seen_pod_uids(self) -> set[str]:
        return {run.pod_uid for run in self.pods if run.pod_uid}

    @staticmethod
    def runner_restarts(pod: Mapping[str, Any]) -> int:
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            if isinstance(status, dict) and status.get("name") == pps.RUNNER_CONTAINER:
                count = status.get("restartCount")
                return count if type(count) is int else 0
        return 0

    @staticmethod
    def runner_running(pod: Mapping[str, Any]) -> bool:
        return any(
            isinstance(status, dict)
            and status.get("name") == pps.RUNNER_CONTAINER
            and bool((status.get("state") or {}).get("running"))
            for status in (pod.get("status") or {}).get("containerStatuses") or []
        )

    def find_runner_pod(self, run: PodRun) -> bool:
        """A Running runner pod on one of the owner's claims, never a pod seen before."""

        owner = self.owners[run.owner]
        principal = str(uuid.UUID(owner.principal_id))
        claims = self.kube.json(
            "get",
            pp.CLAIM_RESOURCE,
            "-l",
            f"{pp.CLAIM_AGENT_LABEL}={self.agent},{OWNER_PRINCIPAL_LABEL}={principal}",
        )
        old = self.seen_pod_uids()
        for claim in claims.get("items") or [] if isinstance(claims, dict) else []:
            meta = claim.get("metadata") or {} if isinstance(claim, dict) else {}
            name = str(meta.get("name") or "")
            if not name or not ((claim.get("status") or {}).get("sandbox") or {}).get("name"):
                continue
            pods = self.kube.json("get", "pods", "-l", f"{pp.POD_CLAIM_LABEL}={name}")
            for pod in pods.get("items") or [] if isinstance(pods, dict) else []:
                pod_meta = pod.get("metadata") or {}
                uid = str(pod_meta.get("uid") or "")
                if uid and uid not in old and (pod.get("status") or {}).get("phase") == "Running":
                    run.claim, run.pod, run.pod_uid = name, str(pod_meta.get("name")), uid
                    run.grant_ref = pps.pod_grant_ref(pod)
                    run.pod_record = pod
                    run.claim_labels = {
                        str(key): str(value) for key, value in (meta.get("labels") or {}).items()
                    }
                    run.restart_count = self.runner_restarts(pod)
                    self.claims_seen.add(name)
                    if name not in owner.claims:
                        owner.claims.append(name)
                    return True
        return False

    def await_runner(self, run: PodRun) -> None:
        self.wait_until(
            lambda: self.find_runner_pod(run),
            until=self.deadline(),
            what=f"{run.group} runner pod",
            every=2.0,
        )
        run.started_at = self.clock.now()
        self.begin_pod_run(run)

    def note(self, criterion: str, key: str, owner: str, value: Any) -> None:
        """Fold one more real reading into a per-owner key: true only while every reading is."""

        values = self.checks[owner].setdefault(f"{criterion}/{key}", [])
        values.append(value is True)
        self.ledger.record(criterion, f"{key}_{owner}", all(values))

    def begin_pod_run(self, run: PodRun) -> None:
        """Check the runner pod's image, owner labels and grant, then open its streams.

        The subagent limits go to the gate before any prompt hold is released.
        """

        owner = self.owners[run.owner]
        others = [item for item in self.active_pods() if item.owner == run.owner]
        run.exclude_ids = self.owner_message_ids(run.owner)
        self.note("owners", "one_native_session", run.owner, not others)
        self.pods.append(run)
        self.attempts[run.number] = run
        self.safe(self.record_volumes, None)
        try:
            self.check_runner_image(run)
            candidate = True
        except StepFailed:
            candidate = False
        self.note("owners", "runner_images_candidate", run.owner, candidate)
        labels = pto.claim_labels_match(
            run.claim_labels,
            tenant_id=owner.tenant_id,
            principal_id=str(uuid.UUID(owner.principal_id)),
            generation=pps.OWNER_GENERATION,
        )
        self.note("owners", "claim_labels", run.owner, labels)
        own_ref = run.grant_ref == (f"{run.claim}{pto.CLAIM_TOKENS_SUFFIX}", pps.PERSONAL_GRANT_ENV)
        self.note("owners", "runner_grant_ref_per_claim", run.owner, own_ref)
        run.delivery = self.delivery_checks(owner, run.claim)
        self.note("owners", "delivery_own_grant", run.owner, run.delivery.get("own_grant"))
        self.note("owners", "delivery_not_other", run.owner, run.delivery.get("not_other"))
        if self.resolutions[run.owner]:
            # A claim started after one of the owner's cards was resolved.
            self.note(
                "approval",
                "grant_unchanged_after_resolution",
                run.owner,
                run.delivery.get("own_grant") is True and run.delivery.get("not_other") is True,
            )
        if not (candidate and labels and own_ref and not others):
            self.fail_paid(f"{run.group} runner pod failed its owner or image checks")
        if not (run.delivery.get("own_grant") is True and run.delivery.get("not_other") is True):
            self.fail_paid(f"{run.group} claim does not deliver the owner's own grant")
        self.open_harvest(run)
        run.gate = self.open_gate(run)
        run.limits = pto.subagent_limits(run.group)
        run.gate.send({"limits": dict(run.limits)})
        run.limits_sent_at = self.clock.now()

    def open_harvest(self, run: PodRun) -> None:
        """``pps.HARVEST_PROGRAM`` in the runner container, fed under the run's lock."""

        state = pps.HarvestState(self.scan)
        run.harvest = state
        run.eof = threading.Event()
        process = self.shell.spawn(
            [
                "kubectl",
                "--context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "exec",
                "-i",
                run.pod,
                "-c",
                pps.RUNNER_CONTAINER,
                "--",
                "python3",
                "-c",
                pps.HARVEST_PROGRAM,
                pps.harvest_config(nonce=self.nonce, max_seconds=pto.LIVE_SECONDS),
            ]
        )
        run.process = process
        self.processes.append(process)
        self.finalizer.register(
            f"stop-harvest-{run.group}-{len(self.processes)}", lambda: pp.stop_process(process)
        )
        eof = run.eof

        def read() -> None:
            for line in process.stdout if process.stdout is not None else []:
                with run.harvest_lock:
                    state.feed(line)
            eof.set()

        def drain() -> None:
            for line in process.stderr if process.stderr is not None else []:
                self.scan.scan("children", line)

        threading.Thread(target=read, daemon=True).start()
        threading.Thread(target=drain, daemon=True).start()

    def open_gate(self, run: PodRun) -> GateChannel:
        """One ``pto.GATE_STREAM_PROGRAM`` stream per runner container."""

        if self.gate_factory is not None:
            return self.gate_factory(run)
        process = spawn_piped(
            self.shell,
            [
                "kubectl",
                "--context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "exec",
                "-i",
                run.pod,
                "-c",
                pps.RUNNER_CONTAINER,
                "--",
                "python3",
                "-c",
                pto.GATE_STREAM_PROGRAM,
            ],
        )
        self.processes.append(process)
        stream = GateStream(process, self.scan)
        self.finalizer.register(f"stop-gate-{run.group}-{len(self.processes)}", stream.close)
        return stream

    def close_pod(self, run: PodRun) -> None:
        """Apply the stream's last events, then close it."""

        if run.closed:
            return
        if run.gate is not None:
            for event in run.gate.events():
                self.gate_event(run, event)
            run.gate.close()
        run.closed = True

    def pod_uid_gone(self, run: PodRun) -> bool:
        return not self.pod_uid_present(run.pod, run.pod_uid)

    def remove_pod(self, run: PodRun) -> None:
        """Delete the run's pod if it is still there; wait for it and the capture to end."""

        run.ending = True
        if self.pod_uid_present(run.pod, run.pod_uid):
            self.pod_deletions.append((run.pod, run.pod_uid, self.clock.now()))
            self.kube.ctl(
                "delete", "pod", run.pod, "--ignore-not-found", "--wait=false", timeout=60
            )
        until = self.clock.now() + SETTLE_SECONDS
        self.wait_until(
            lambda: self.pod_uid_gone(run), until=until, what=f"{run.group} pod removal", every=2.0
        )
        run.gone_at = self.clock.now()
        self.wait_until(run.eof.is_set, until=until, what=f"{run.group} capture end", every=1.0)
        self.close_pod(run)

    def pending_publication_approvals(self) -> list[dict[str, Any]]:
        return [
            row
            for row in self.pending_approvals()
            if row.get("route") == pps.PUBLICATION_ROUTE
            or row.get("granted_tool") == pps.PUBLICATION_GATE
        ]

    def publications(self) -> list[dict[str, Any]]:
        listed = self.require_api().get("/publications?limit=200")
        return [row for row in listed or [] if isinstance(row, dict)]

    def await_park(self, run: PodRun) -> None:
        """The run's own publication approval: fresh, on the owner's conversation."""

        known = {item.approval_id for item in self.pods if item.approval_id}

        def parked() -> bool:
            item = self.work_item_of(run.work_item_id)
            mine = [
                row
                for row in item.get("requests") or []
                if isinstance(row, dict) and str(row.get("id")) == run.request_id
            ]
            if any(row.get("terminal_at") for row in mine):
                self.fail_paid(f"{run.group} ended before parking for approval")
            for row in self.pending_publication_approvals():
                approval_id = str(row.get("id") or "")
                if not approval_id or approval_id in known:
                    continue
                if self.approval(approval_id).get("conversation_id") == run.conversation_id:
                    run.approval_id = approval_id
                    return item.get("state") == "awaiting_approval"
            return False

        self.wait_until(parked, until=self.deadline(), what=f"{run.group} publication park")
        run.parked_at = pps.iso_epoch(self.approval(run.approval_id).get("created_at"))
        run.publication_id = next(
            (
                str(row.get("id"))
                for row in self.publications()
                if str(row.get("approval_id")) == run.approval_id
            ),
            "",
        )
        self.cards.append(pps.ref_digest(run.approval_id))
        if not run.publication_id:
            self.fail_paid(f"card {run.card} has no publication")
        self.record_publication_branch(run)
        run.branch_known = True
        self.no_push[run.card] = self.unpushed(run)

    def branch_head(self, branch: str) -> str | None:
        """The branch's head commit on the forge, or None when it does not exist."""

        forge = self.require_forge()
        status, body = forge.request(
            "GET", forge.repo_path(f"/git/ref/heads/{urllib.parse.quote(branch, safe='/')}")
        )
        if status == 404:
            return None
        sha = str(((body or {}).get("object") or {}).get("sha") or "") if status == 200 else ""
        if not isinstance(body, dict) or not pps.is_commit_sha(sha):
            raise StepFailed("a branch read returned no commit")
        return sha

    def branch_pulls(self, branch: str) -> list[dict[str, Any]]:
        return [
            pull
            for pull in self.run_pulls()
            if str((pull.get("head") or {}).get("ref") or "") == branch
        ]

    def unpushed(self, run: PodRun) -> bool:
        """Nothing was pushed for this card before its approval (card 4: B1's head stays)."""

        head = self.branch_head(run.branch)
        pulls = self.branch_pulls(run.branch)
        if run.card == 4:
            return (
                bool(self.b1_head)
                and head == self.b1_head
                and all(str((pull.get("head") or {}).get("sha")) == self.b1_head for pull in pulls)
            )
        return head is None and not pulls

    def finish_park(self, run: PodRun) -> None:
        """Terminal capture after the park, the reviewed workspace, pod removal, settlement."""

        harvest = run.harvest
        if harvest is None:
            self.fail_paid(f"{run.group} has no capture")

        def terminal() -> bool:
            with run.harvest_lock:
                return harvest.terminal_after(run.parked_at)

        with contextlib.suppress(StepFailed):
            self.wait_until(
                terminal, until=self.clock.now() + SETTLE_SECONDS, what=f"{run.group} capture"
            )
        with run.harvest_lock:
            run.reviewed = pps.workspace_record(harvest.workspace)
        self.remove_pod(run)
        with run.harvest_lock:
            complete = run.eof.is_set() and harvest.terminal_after(run.parked_at)
            process_max = harvest.process_max
            credential = harvest.credential_evidence()
        self.note("owners", "one_native_session", run.owner, process_max <= 1)
        self.ledger.record("diagnostics", f"runner_credential_{run.group}", credential)
        self.settle_run(run, facts=self.pod_facts(run), complete=complete)

    def deliver_card(self, run: PodRun) -> None:
        key = f"card{run.card}"
        try:
            self.await_card(run.publication_id, key)
        finally:
            state = self.ledger.observations("approval").get(f"{key}_card")
            delivered = isinstance(state, dict) and state.get("delivered") is True
            self.ledger.record("approval", f"{key}_delivered", delivered)

    def park_and_settle(self, run: PodRun) -> None:
        self.await_park(run)
        self.finish_park(run)
        self.deliver_card(run)

    # -- request, thread and marker reads ---------------------------------------------

    def request_rows(self, work_item_id: str) -> list[dict[str, Any]]:
        return self.sql_rows(
            f"SELECT {_REQUEST_COLUMNS} FROM curie.execution_requests "
            "WHERE work_item_id = :'work_item'::uuid ORDER BY sequence",
            {"work_item": work_item_id},
        )

    def request_row(self, request_id: str) -> dict[str, Any]:
        rows = self.sql_rows(
            f"SELECT {_REQUEST_COLUMNS} FROM curie.execution_requests WHERE id = :'request'::uuid",
            {"request": request_id},
        )
        if len(rows) != 1:
            raise StepFailed("the execution request could not be read")
        return rows[0]

    def phase_notes(self, request_id: str) -> list[str]:
        rows = self.sql_rows(
            "SELECT note FROM curie.execution_request_phase_reports "
            "WHERE execution_request_id = :'request'::uuid",
            {"request": request_id},
        )
        return [str(row.get("note") or "") for row in rows]

    def thread_records(self) -> dict[str, list[str]]:
        """Every unbound transcript thread of the run agent: key -> record ids."""

        rows = self.sql_rows(THREAD_QUERY, {"agent": self.agent_id})
        found: dict[str, list[str]] = {}
        for row in rows:
            ids = row.get("record_ids")
            found[str(row.get("thread_key") or "")] = (
                [str(item) for item in ids if isinstance(item, str)]
                if isinstance(ids, list)
                else []
            )
        return found

    def owner_thread(self, owner: Owner) -> str:
        """The owner's thread: the row keyed by its WorkItem's conversation."""

        threads: dict[str, list[str]] = {}

        def present() -> bool:
            nonlocal threads
            threads = self.thread_records()
            return any(key in threads for key in owner.conversation_ids if key)

        self.wait_until(
            present, until=self.clock.now() + SETTLE_SECONDS, what=f"owner {owner.name} thread"
        )
        return next(key for key in owner.conversation_ids if key in threads)

    def append_marker(self, owner: Owner) -> bool:
        """Append a synthetic marker turn to the owner's thread and read it back."""

        thread = self.owner_thread(owner)
        record_id = uuid.uuid4().hex
        marker = f"pto-{owner.name}-{secrets.token_hex(6)}"
        stamp = (
            dt.datetime.fromtimestamp(self.clock.now(), dt.UTC).isoformat().replace("+00:00", "Z")
        )
        key = urllib.parse.quote(thread, safe="")
        status, _body = self.require_http().request(
            "POST",
            f"/agents/{self.agent_id}/state/transcript/{key}/append",
            headers={"X-API-Key": self.require_api().api_key.reveal()},
            body={"item": pto.marker_turn(marker, record_id, stamp)},
        )
        if status not in (200, 201):
            self.fail_paid(f"owner {owner.name} marker append returned {status}")
        self.threads[owner.name] = thread
        self.markers[owner.name] = record_id
        return self.marker_present(owner)

    def marker_present(self, owner: Owner) -> bool:
        record_id = self.markers.get(owner.name, "")
        thread = self.threads.get(owner.name, "")
        return bool(record_id and thread) and record_id in self.thread_records().get(thread, [])

    # -- cards and resolutions --------------------------------------------------------

    def resolve(self, approval_id: str, token: pps.HeldSecret | None, *extra: str) -> pp.Completed:
        if token is None:
            raise StepFailed("an operator principal was not minted")
        return self.curie.run(
            "approvals",
            self.agent,
            "--resolve",
            approval_id,
            *extra,
            check=False,
            extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": token.reveal()},
            secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
        )

    def unlisted_control(self, run: PodRun) -> None:
        """The unlisted operator is refused 403 by the explicit list; the card stays pending."""

        key = f"card{run.card}"
        result = self.resolve(run.approval_id, self.unlisted_token)
        status = pps.resolve_status(result.returncode, result.stdout + result.stderr)
        audit = self.audit(run.approval_id)
        self.record_resolution(f"{key}_unlisted", result, audit, self.unlisted_subject)
        pending = self.approval(run.approval_id).get("status") == "pending"
        denied = pps.audit_row(
            audit,
            action="denied",
            subject=self.unlisted_subject,
            authorized=False,
            authorizer=pps.EXPLICIT_USERS_AUTHORIZER,
        )
        self.ledger.record("approval", f"{key}_unlisted_status", status)
        self.ledger.record("approval", f"{key}_pending_after_unlisted", pending)
        self.ledger.record("approval", f"{key}_unlisted_audit_denied", denied)
        if status != 403 or not pending or not denied:
            self.fail_paid(f"the unlisted operator was not refused on {key}")

    def resolve_card(self, run: PodRun, decision: Literal["approved", "rejected"]) -> None:
        """The allowed operator answers the card; the owner checks follow every resolution."""

        key = f"card{run.card}"
        extra: tuple[str, ...] = ()
        if decision == "rejected":
            extra = ("--reject", "--note", "two-owner rejection control")
        result = self.resolve(run.approval_id, self.allowed_token, *extra)
        audit = self.audit(run.approval_id)
        self.record_resolution(f"{key}_{decision}", result, audit, self.allowed_subject)
        resolved = (
            result.returncode == 0
            and self.approval(run.approval_id).get("status") == decision
            and pps.audit_row(
                audit,
                action="resolved",
                subject=self.allowed_subject,
                authorized=True,
                decision=decision,
            )
        )
        self.ledger.record("approval", f"{key}_{decision}_by_operator", resolved)
        run.resolved_at = self.clock.now()
        self.resolutions[run.owner].append(run.resolved_at)
        self.after_resolution(self.owners[run.owner], run.work_item_id)
        if not resolved:
            self.fail_paid(f"the allowed operator could not resolve {key}")

    def after_resolution(self, owner: Owner, work_item_id: str) -> None:
        item = self.work_item_of(work_item_id)
        tuples = [pps.owner_of(item), *pps.request_owners(item)]
        unchanged = len(tuples) > 1 and all(
            pps.owner_matches(value, owner.tuple()) for value in tuples
        )
        self.note("approval", "owner_unchanged_after_resolution", owner.name, unchanged)
        principals = {str((value or {}).get("principal_id")) for value in tuples}
        self.approver_checks.append(
            unchanged
            and self.allowed_subject not in principals
            and self.unlisted_subject not in principals
        )
        self.ledger.record("approval", "approver_never_owner", all(self.approver_checks))
        for claim in self.owner_claims(owner):
            delivery = self.delivery_checks(owner, claim)
            self.note(
                "approval",
                "grant_unchanged_after_resolution",
                owner.name,
                delivery.get("own_grant") is True and delivery.get("not_other") is True,
            )

    def await_publication(self, run: PodRun) -> dict[str, Any]:
        publication: dict[str, Any] = {}

        def published() -> bool:
            nonlocal publication
            publication = self.publication(run.publication_id)
            if publication.get("status") in {"failed", "denied", "expired"}:
                self.fail_paid(f"card {run.card}'s publication did not succeed")
            return publication.get("status") == "succeeded" and bool(publication.get("pr_url"))

        self.wait_until(
            published,
            until=self.clock.now() + pp.PUBLICATION_WAIT_SECONDS,
            what=f"card {run.card} publication",
        )
        return publication

    def head_blobs(self, reviewed: Mapping[str, Any] | None, head_sha: str) -> dict[str, Any]:
        """The reviewed paths' blobs at ``head_sha``, hashed; the nonce paths among them."""

        hashes: dict[str, str | None] = {}
        nonce_paths: list[str] = []
        for path in sorted(((reviewed or {}).get("hashes") or {}).keys()):
            content = self.forge_file(path, head_sha)
            if content is None:
                hashes[path] = None
                continue
            hashes[path] = hashlib.sha256(content).hexdigest()
            if self.nonce.encode() in content:
                nonce_paths.append(path)
        return {"hashes": hashes, "nonce_paths": nonce_paths}

    def forge_file(self, path: str, ref: str) -> bytes | None:
        forge = self.require_forge()
        status, body = forge.request(
            "GET", forge.repo_path(f"/contents/{urllib.parse.quote(path)}?ref={ref}")
        )
        if status == 404:
            return None
        if status != 200 or not isinstance(body, dict):
            raise StepFailed("a published file could not be read")
        content = base64.b64decode(str(body.get("content") or ""))
        self.scan.scan("forge", content)
        return content

    def published_facts(self, run: PodRun, *, full: bool) -> dict[str, Any]:
        """Publication succeeded; its branch from GET /publications/{id}; PR head vs review."""

        publication = self.await_publication(run)
        forge = self.require_forge()
        branch = str(publication.get("branch") or "")
        number = publication.get("pr_number")
        if type(number) is not int:
            self.fail_paid(f"card {run.card}'s publication names no pull request")
        pull = forge.call("GET", forge.repo_path(f"/pulls/{number}"))
        head = pull.get("head") or {} if isinstance(pull, dict) else {}
        head_sha = str(head.get("sha") or "")
        on_branch = (
            pps.publication_branch_valid(branch)
            and str(head.get("ref") or "") == branch
            and pps.is_commit_sha(head_sha)
        )
        if on_branch:
            # Owned only now that its creation by this run is confirmed.
            self.published_heads[branch] = head_sha
        blobs = self.head_blobs(run.reviewed, head_sha)
        changed, listed_all = forge.pull_files(number)
        matches = pps.published_matches(
            run.reviewed,
            {**blobs, "changed_paths": changed, "changed_paths_complete": listed_all},
        )
        if not full:
            # A repair's reviewed record covers only the repair's own change.
            matches = {
                key: matches[key] for key in ("hashes_equal", "deleted_absent", "head_recorded")
            }
        self.ledger.record("publication", f"card{run.card}_matches", matches)
        return {
            "succeeded": publication.get("status") == "succeeded",
            "branch": branch,
            "branch_from_api": bool(run.branch) and branch == run.branch and on_branch,
            "pr": number,
            "pr_ready": isinstance(pull, dict) and pull.get("draft") is False and on_branch,
            "head": head_sha,
            "tree": bool(matches) and all(matches.values()),
        }

    def head_checks(self, head_sha: str) -> tuple[dict[str, Any], Any, Any]:
        """Latest check runs and statuses at one head, once none is pending (or the wait ends)."""

        latest: dict[str, Any] = {}
        runs: Any = {}
        statuses: Any = {}

        def completed() -> bool:
            nonlocal latest, runs, statuses
            runs = self.forge_get(f"/commits/{head_sha}/check-runs?filter=latest&per_page=100")
            statuses = self.forge_get(f"/commits/{head_sha}/status")
            latest = pps.judge_head_checks(runs, statuses)
            return int(latest["total"]) > 0 and int(latest["pending"]) == 0

        with contextlib.suppress(StepFailed):
            self.wait_until(
                completed,
                until=self.clock.now() + pp.CI_WAIT_SECONDS,
                what="head checks",
                every=15.0,
            )
        return latest, runs, statuses

    def forge_get(self, suffix: str) -> Any:
        forge = self.require_forge()
        return forge.call("GET", forge.repo_path(suffix))

    def request_of(self, item: Mapping[str, Any], request_id: str) -> dict[str, Any]:
        return next(
            (
                row
                for row in item.get("requests") or []
                if isinstance(row, dict) and str(row.get("id")) == request_id
            ),
            {},
        )

    # -- the paid phases ----------------------------------------------------------------

    def phase_probe_a(self) -> None:
        self.require_live("probe-a")
        self.probe_owner(self.owners["a"])

    def phase_probe_b(self) -> None:
        self.require_live("probe-b")
        self.probe_owner(self.owners["b"])

    def resolve_probe_cli(self) -> str:
        resolved = shutil.which(self.scenario.probe_cli, path=self.shell.env.get("PATH"))
        if resolved is None:
            raise StepFailed("the grant probe CLI was not found")
        return resolved

    def probe_owner(self, owner: Owner) -> None:
        """One probe-model turn in a fresh empty HOME, config and cwd with only the owner's grant.

        Reserved first; settled from the CLI JSON with source ``probe``. A
        refusal (a weekly limit included) records its sanitized error class
        and stops before any further reservation.
        """

        if self.ledger.run_row()["live_started_at"] is None:
            self.ledger.begin_live(now=self.clock.now(), grant_expires_at=self.inputs.expires_at)
            self.ledger.record("budget", "allowance_breach", [])
        token = owner.token
        if token is None or owner.token_kind != "grant" or not token.reveal():
            raise StepFailed(f"owner {owner.name} holds no grant")
        resolved = self.resolve_probe_cli()
        group = f"{owner.name}-probe"
        with tempfile.TemporaryDirectory(prefix="pto-probe-") as home:
            empty = not any(Path(home).iterdir())
            env = pps.grant_probe_env(
                home=home, cli_dir=str(Path(resolved).parent), grant=token.reveal()
            )
            isolated = (
                empty
                and env["HOME"] == home
                and all(
                    env[name].startswith(home + "/")
                    for name in ("CLAUDE_CONFIG_DIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME")
                )
                and [name for name in env if _CREDENTIAL_NAME.search(name)]
                == [pps.PERSONAL_GRANT_ENV]
            )
            self.ledger.record("budget", f"probe_isolated_{owner.name}", isolated)
            if not isolated:
                raise StepFailed(f"owner {owner.name}'s probe environment is not isolated")
            now = self.clock.now()
            reservations = self.owner_ledger.reserve_group(group, now=now)
            self.reserved_at[group] = now
            started = self.clock.now()
            try:
                result = self.shell.run(
                    pto.probe_argv(resolved),
                    stdin=pps.GRANT_PROBE_PROMPT,
                    timeout=pto.PROBE_SECONDS,
                    check=False,
                    extra_env=env,
                    secret_env=(pps.PERSONAL_GRANT_ENV,),
                    clean_env=True,
                    cwd=Path(home),
                    full_output=True,
                )
            except StepFailed:
                for item in reservations:
                    self.ledger.settle(item.seq, None, source="unknown")
                self.fail_paid(f"owner {owner.name}'s grant probe did not finish")
            elapsed = self.clock.now() - started
        outcome = pto.probe_outcome(result.returncode, result.stdout, result.stderr)
        tokens, turns = outcome.get("output_tokens"), outcome.get("turns")
        usage = (
            pps.SessionUsage(tokens, turns, round(elapsed, 3), ",".join(outcome["models"]))
            if type(tokens) is int and type(turns) is int
            else None
        )
        for item in reservations:
            self.ledger.settle(item.seq, usage, source="probe" if usage else "unknown")
        error = pps.error_summary(outcome.get("error") or {}) if not outcome["ok"] else ""
        self.ledger.record(
            "grant_probe",
            f"result_{owner.name}",
            {
                key: outcome.get(key)
                for key in (
                    "ok",
                    "exit",
                    "model",
                    "models",
                    "output_tokens",
                    "turns",
                    "on_probe_model",
                    "within_limits",
                )
            }
            | {"elapsed": round(elapsed, 3), "error_class": error},
        )
        passed = (
            outcome["ok"] is True
            and outcome["on_probe_model"] is True
            and outcome["within_limits"] is True
            and turns == pto.PROBE_TURNS
        )
        self.ledger.record("budget", f"probe_{owner.name}_ok", passed)
        if not passed:
            self.ledger.record("budget", f"probe_{owner.name}_error", error or "exit")
            self.fail_paid(f"owner {owner.name}'s grant probe failed: {error or 'no usage'}")

    def phase_attempt_a1(self) -> None:
        """A1 parks on card 1; its lineage and marker are recorded, then its pod is deleted."""

        self.require_live("attempt-a1")
        a = self.owners["a"]
        run = self.new_run("a1", card=1)
        self.admit_group(run, a)
        self.await_runner(run)
        self.await_park(run)
        lineage = self.record_lineage(run)
        marker = self.append_marker(a)
        self.ledger.record("restarts", "pod_lineage_before", lineage)
        self.ledger.record("restarts", "pod_marker_before", marker)
        self.finish_park(run)
        self.deliver_card(run)
        absent = self.pod_uid_gone(run)
        self.ledger.record("restarts", "pod_old_runtime_absent", absent)
        self.require({"lineage": lineage, "marker": marker, "old_pod_absent": absent}, "attempt A1")

    def record_lineage(self, run: PodRun) -> bool:
        """Request, epoch, consumer, claim, pod and native sessions, read at the park."""

        row = self.request_row(run.request_id)
        epoch = row.get("runtime_epoch")
        run.epoch = epoch if type(epoch) is int else None
        run.consumer = str(row.get("runtime_owner") or "")
        lineage = {
            "request": row.get("id") == run.request_id and row.get("started") is True,
            "epoch": run.epoch is not None,
            "claim": bool(run.claim),
            "pod": bool(run.pod_uid),
            "sessions": bool(run.sessions),
        }
        self.ledger.record("diagnostics", f"lineage_{run.group}", lineage)
        return all(lineage.values())

    def phase_attempt_b1(self) -> None:
        self.require_live("attempt-b1")
        b = self.owners["b"]
        run = self.new_run("b1", card=3)
        self.admit_group(run, b)
        self.await_runner(run)
        self.await_park(run)
        lineage = self.record_lineage(run)
        marker = self.append_marker(b)
        self.ledger.record("restarts", "worker_lineage_before", lineage)
        self.ledger.record("restarts", "worker_marker_before", marker)
        self.finish_park(run)
        self.deliver_card(run)
        self.require({"lineage": lineage, "marker": marker}, "attempt B1")

    def phase_wrong_owner(self) -> None:
        """B names itself on A's issue, replays A1's request and reads A's context."""

        self.require_live("wrong-owner")
        a, b = self.owners["a"], self.owners["b"]
        first = self.pod_run("a1")
        thread = self.threads.get("a", "")
        before = self.thread_records().get(thread)
        controls = self.wrong_owner_controls(a, b, first.work_item_id, first.admission_key)
        after = self.thread_records().get(thread)
        a_hash = first.claim_labels.get(THREAD_HASH_LABEL, "")
        principal_b = str(uuid.UUID(b.principal_id))
        on_thread = (
            self.kube.json(
                "get",
                pp.CLAIM_RESOURCE,
                "-l",
                f"{pp.CLAIM_AGENT_LABEL}={self.agent},{THREAD_HASH_LABEL}={a_hash}",
            )
            if a_hash
            else None
        )
        b_on_thread = [
            item
            for item in (on_thread or {}).get("items") or []
            if isinstance(item, dict)
            and ((item.get("metadata") or {}).get("labels") or {}).get(OWNER_PRINCIPAL_LABEL)
            == principal_b
        ]
        rows = self.request_rows(first.work_item_id)
        self.controls_done_at = self.clock.now()
        checks = {
            "admission_b_on_a_issue_refused": controls["b_on_a_issue_refused"],
            "admission_request_count_unchanged": controls["request_count_unchanged"],
            "replay_owner_b_refused": controls["replay_owner_b_refused"],
            "personal_context_b_hidden": controls["personal_context_b_hidden"],
            "a_marker_unchanged": before is not None
            and before == after
            and self.markers.get("a", "") in (after or []),
            "no_b_claim_on_a_thread": on_thread is not None and not b_on_thread,
            "no_foreign_runnable_request": bool(rows)
            and all(row.get("owner_principal_id") == a.principal_id for row in rows),
        }
        for key, value in checks.items():
            self.ledger.record("wrong-owner", key, value)
        self.require(checks, "wrong-owner controls")

    def phase_reject_a1(self) -> None:
        self.require_live("reject-a1")
        first = self.pod_run("a1")
        self.unlisted_control(first)
        self.resolve_card(first, "rejected")

        def denied() -> bool:
            row = self.request_row(first.request_id)
            return row.get("terminal_cause") == "publication_denied"

        self.wait_until(denied, until=self.clock.now() + 180, what="publication_denied")
        self.pause(pp.REJECTION_SETTLE_SECONDS)
        checks = {
            "rejected_no_branch": self.branch_head(first.branch) is None,
            "rejected_no_pr": not self.branch_pulls(first.branch),
            "rejection_stays_rejected": self.approval(first.approval_id).get("status")
            == "rejected",
        }
        for key, value in checks.items():
            self.ledger.record("publication", key, value)
        self.require(checks, "card 1 rejection")

    def phase_attempt_a2(self) -> None:
        """A's fresh successor: same WorkItem and conversation, a new pod, A's own grant."""

        self.require_live("attempt-a2")
        a = self.owners["a"]
        first = self.pod_run("a1")
        run = self.new_run("a2", card=2)
        self.admit_group(run, a)
        self.ledger.record(
            "wrong-owner",
            "controls_before_continuation",
            self.controls_done_at is not None and self.controls_done_at <= run.admitted_at,
        )
        self.await_runner(run)
        self.park_and_settle(run)
        old_row = self.request_row(first.request_id)
        new_row = self.request_row(run.request_id)
        code, status = self.subscription(a, "status")
        tool = self.first_tool(run, run.started_at - pp.REPLAY_MARGIN_SECONDS)
        checks = {
            "pod_termination_epoch": first.epoch is not None
            and old_row.get("terminal_cause") is not None
            and old_row.get("runtime_epoch") == first.epoch,
            "pod_fresh_runtime": bool(run.pod_uid) and run.pod_uid != first.pod_uid,
            "pod_same_work_item": run.work_item_id == first.work_item_id,
            "pod_same_conversation": bool(run.conversation_id)
            and run.conversation_id == first.conversation_id,
            "pod_owner_unchanged": run.admitted_owner
            and new_row.get("owner_principal_id") == a.principal_id
            and new_row.get("tenant_id") == a.tenant_id,
            "pod_generation_unchanged": new_row.get("enrollment_generation") == pps.OWNER_GENERATION
            and code == 0
            and status.get("generation") == pps.OWNER_GENERATION,
            "pod_grant_unchanged": run.delivery.get("own_grant") is True
            and run.delivery.get("not_other") is True,
            "pod_marker_after": self.marker_present(a),
            "pod_continued_native": tool is not None,
            "pod_usage_settled": self.settled_known("a1"),
        }
        absent = self.pod_uid_gone(first)
        observed = self.ledger.observations("restarts").get("pod_old_runtime_absent") is True
        checks["pod_old_runtime_absent"] = observed and absent
        for key, value in checks.items():
            self.ledger.record("restarts", key, value)
        self.ledger.record(
            "wrong-owner",
            "a_continuation_original_tuple",
            checks["pod_owner_unchanged"]
            and checks["pod_generation_unchanged"]
            and checks["pod_continued_native"],
        )
        self.require(checks, "A's pod interruption")

    def first_tool(self, run: PodRun, moment: float) -> float | None:
        calls = pto.native_tool_calls(
            self.pod_files(run), exclude_ids=run.exclude_ids, not_before=self.not_before(run)
        )
        return pto.first_tool_after(calls, moment)

    def phase_approve_a2(self) -> None:
        self.require_live("approve-a2")
        first, run = self.pod_run("a1"), self.pod_run("a2")
        self.resolve_card(run, "approved")
        facts = self.published_facts(run, full=True)
        latest, _runs, _statuses = self.head_checks(facts["head"])
        self.ledger.record("publication", "a2_head_checks", latest)
        checks = {
            "a2_publication_succeeded": facts["succeeded"],
            "a2_branch_from_api": facts["branch_from_api"],
            "a2_pr_ready": facts["pr_ready"],
            "a2_tree_matches_reviewed": facts["tree"],
            "a2_checks_passed": latest.get("all_completed_successfully") is True,
            "rejection_stays_rejected": self.approval(first.approval_id).get("status")
            == "rejected",
        }
        for key, value in checks.items():
            self.ledger.record("publication", key, value)
        self.require(checks, "A2's publication")

    def phase_approve_b1(self) -> None:
        """Only once A has no claim and no running runner, B1's card is approved."""

        self.require_live("approve-b1")
        a = self.owners["a"]
        run = self.pod_run("b1")

        def quiescent() -> bool:
            rows = self.request_rows(self.pod_run("a2").work_item_id)
            return not self.owner_claims(a) and all(row.get("status") != "running" for row in rows)

        self.wait_until(quiescent, until=self.clock.now() + QUIESCENT_SECONDS, what="A quiescent")
        self.resolve_card(run, "approved")
        facts = self.published_facts(run, full=True)
        self.b1_head, self.b1_pull = facts["head"], facts["pr"]
        checks = {
            "b1_publication_succeeded": facts["succeeded"],
            "b1_branch_from_api": facts["branch_from_api"],
            "b1_pr_ready": facts["pr_ready"],
            "b1_tree_matches_reviewed": facts["tree"],
        }
        for key, value in checks.items():
            self.ledger.record("publication", key, value)
        self.require(checks, "B1's publication")

    def phase_worker_stop(self) -> None:
        """Worker to 0; B's real failing check queues the repair; A2 completes meanwhile."""

        self.require_live("worker-stop")
        self.worker_before = self.stop_worker()
        self.ledger.record(
            "restarts",
            "worker_old_runtime_absent",
            bool(self.worker_before) and not self.worker_pods(),
        )
        self.observe_ci_failure()
        second = self.pod_run("a2")

        def completed() -> bool:
            row = self.request_of(self.work_item_of(second.work_item_id), second.request_id)
            return row.get("status") == "completed"

        with contextlib.suppress(StepFailed):
            self.wait_until(
                completed, until=self.clock.now() + pp.CI_WAIT_SECONDS, what="A2 completed"
            )
        item = self.work_item_of(second.work_item_id)
        checks = {
            "a2_request_completed": self.request_of(item, second.request_id).get("status")
            == "completed",
            "a2_work_item_published": item.get("state") == "published",
        }
        for key, value in checks.items():
            self.ledger.record("publication", key, value)
        self.require(checks, "A2's completion")

    def observe_ci_failure(self) -> None:
        """B1's head check fails on CI_ACK.md, the rerun settles, round 2 is queued and parked."""

        first = self.pod_run("b1")
        head = self.b1_head
        failing: list[dict[str, Any]] = []

        def failed() -> bool:
            nonlocal failing
            runs = self.forge_get(f"/commits/{head}/check-runs?filter=all&per_page=100")
            failing = pto.failing_actions_runs(runs)
            return bool(failing)

        with contextlib.suppress(StepFailed):
            self.wait_until(
                failed,
                until=self.clock.now() + pp.CI_WAIT_SECONDS,
                what="B's failing head check",
                every=15.0,
            )
        self.ledger.record("ci-repair", "b1_head_check_failed", bool(failing))
        if not failing:
            self.fail_paid("B's first published head has no failing Actions check")
        names_ack = any(
            pto.annotations_name(
                self.forge_get(f"/check-runs/{item['id']}/annotations"), pto.CI_ACK_FILE
            )
            for item in failing
        )
        self.ledger.record("ci-repair", "b1_failure_names_ci_ack", names_ack)
        latest = self.forge_get(f"/commits/{head}/check-runs?filter=latest&per_page=100")
        self.b1_check_names = sorted(
            {
                str(item.get("name"))
                for item in (latest.get("check_runs") or [] if isinstance(latest, dict) else [])
                if isinstance(item, dict) and item.get("name")
            }
        )
        name = str(failing[0]["name"])

        def rerun_settled() -> bool:
            notes = self.phase_notes(first.request_id)
            if RERUN_FORBIDDEN_NOTE in notes:
                return True
            count, done = pto.rerun_count(
                self.forge_get(f"/commits/{head}/check-runs?filter=all&per_page=100"), name
            )
            return RERUN_REQUESTED_NOTE in notes and count >= 2 and done

        settled = self.observed(rerun_settled, pp.CI_WAIT_SECONDS, "CI rerun settled")
        self.ledger.record("ci-repair", "rerun_settled", settled)
        event = f"work-item-{first.request_id}-ci-{CI_REPAIR_ROUND}"
        enqueued = self.observed(
            lambda: pto.stream_has_event(
                self.valkey_xrange(RUNS_STREAM, count=STREAM_SCAN_COUNT), event
            ),
            pp.CI_WAIT_SECONDS,
            "round-2 event",
        )
        self.ledger.record("ci-repair", "round2_event_enqueued", enqueued)
        parked = self.observed(
            lambda: self.request_row(first.request_id).get("heartbeat_parked") is True,
            SETTLE_SECONDS,
            "parked heartbeat",
        )
        self.ledger.record("ci-repair", "round2_heartbeat_parked", parked)
        self.require(
            {
                "b1_failure_names_ci_ack": names_ack,
                "rerun_settled": settled,
                "round2_event_enqueued": enqueued,
                "round2_heartbeat_parked": parked,
            },
            "B's CI failure",
        )

    def observed(self, condition: Callable[[], bool], seconds: float, what: str) -> bool:
        """Whether ``condition`` became true within ``seconds`` (gates served while waiting)."""

        try:
            self.wait_until(condition, until=self.clock.now() + seconds, what=what, every=10.0)
        except StepFailed:
            return False
        return True

    def phase_queue_a3(self) -> None:
        """A3 is reserved, then admitted while the worker is stopped."""

        self.require_live("queue-a3")
        run = self.new_run("a3", policy=KEEP_AFTER_TOOL)
        self.admit_group(run, self.owners["a"])

    def phase_worker_start(self) -> None:
        """B-r1 is reserved, then the worker returns as a new pod on the candidate image."""

        self.require_live("worker-start")
        first = self.pod_run("b1")
        run = self.new_run("b-r1", policy=KEEP_PROMPT)
        run.request_id, run.work_item_id = first.request_id, first.work_item_id
        run.conversation_id = first.conversation_id
        self.reserve(run)
        self.worker_started_at = self.clock.now()
        self.kube.ctl("scale", f"deployment/{self.fullname}-worker", "--replicas=1", timeout=120)

        def started() -> bool:
            running = [
                pod
                for pod in self.worker_pods()
                if (pod.get("status") or {}).get("phase") == "Running"
                and str((pod.get("metadata") or {}).get("uid") or "") not in self.worker_before
            ]
            return len(running) == 1

        self.wait_until(
            started, until=self.clock.now() + RESTART_SECONDS, what="new worker pod", every=2.0
        )
        uid, candidate = self.observe_worker()
        fresh = bool(uid) and uid not in self.worker_before
        self.worker_name = next(
            (
                str((pod.get("metadata") or {}).get("name") or "")
                for pod in self.worker_pods()
                if str((pod.get("metadata") or {}).get("uid") or "") == uid
            ),
            "",
        )
        self.ledger.record("restarts", "worker_fresh_runtime", fresh)
        self.ledger.record("restarts", "worker_candidate_image", candidate)
        self.require({"fresh": fresh, "candidate": candidate}, "the worker restart")

    def phase_hold_both(self) -> None:
        """B-r1 held at its prompt with zero usage; A3 runs one tool and is held at its next."""

        self.require_live("hold-both")
        b = self.owners["b"]
        first, held, follow = self.pod_run("b1"), self.pod_run("b-r1"), self.pod_run("a3")

        def found() -> bool:
            for run in (held, follow):
                if not run.pod and self.find_runner_pod(run):
                    run.started_at = self.clock.now()
                    self.begin_pod_run(run)
            return bool(held.pod and follow.pod)

        self.wait_until(found, until=self.deadline(), what="B-r1 and A3 runner pods", every=2.0)
        self.wait_until(
            lambda: held.kept is not None and follow.kept is not None,
            until=self.clock.now() + pto.HOLD_SECONDS,
            what="B-r1's prompt hold and A3's next hold",
        )
        facts = held.kept_facts
        zero = facts is not None and all(
            item.assistant_turns == 0 and item.synthetic == 0 for item in facts.sessions
        )
        self.ledger.record(
            "diagnostics",
            "b_r1_prompt_hold",
            {"zero_usage": zero, "sessions": len(facts.sessions) if facts else None},
        )
        row = self.request_row(first.request_id)
        epoch = row.get("runtime_epoch")
        consumer = str(row.get("runtime_owner") or "")
        stale = {str((pod.get("metadata") or {}).get("uid") or "") for pod in self.worker_pods()}
        checks = {
            "worker_termination_epoch": not (stale & set(self.worker_before))
            and first.epoch is not None
            and type(epoch) is int
            and epoch >= first.epoch,
            "worker_same_work_item": row.get("work_item_id") == first.work_item_id
            and row.get("id") == first.request_id,
            "worker_same_conversation": held.conversation_id == first.conversation_id
            and bool(self.threads.get("b"))
            and self.threads.get("b") in b.conversation_ids,
            "worker_owner_unchanged": row.get("owner_principal_id") == b.principal_id
            and row.get("tenant_id") == b.tenant_id
            and row.get("model_auth_mode") == pps.PERSONAL_MODE,
            "worker_generation_unchanged": row.get("enrollment_generation") == pps.OWNER_GENERATION,
            "worker_grant_unchanged": held.delivery.get("own_grant") is True
            and held.delivery.get("not_other") is True,
            "worker_marker_after": self.marker_present(b),
            "worker_usage_settled": all(self.settled_known(name) for name in ("a1", "b1", "a2")),
            "worker_owner_lost_safe": row.get("owner_lost_retry") is False
            and row.get("status") == "running"
            and self.publication(first.publication_id).get("status") == "succeeded",
        }
        consumer_moved = bool(self.worker_name) and consumer.startswith(self.worker_name + "-")
        checks["worker_termination_epoch"] = checks["worker_termination_epoch"] and consumer_moved
        for key, value in checks.items():
            self.ledger.record("restarts", key, value)
        tool = any(
            not call["subagent"] and call["result"]
            for call in pto.native_tool_calls(
                follow.kept_files,
                exclude_ids=follow.exclude_ids,
                not_before=self.not_before(follow),
            )
        )
        self.ledger.record("disconnect", "a3_tool_observed", tool)
        self.require({**checks, "b_r1_zero": zero, "a3_tool_observed": tool}, "hold-both")

    def phase_disconnect_a(self) -> None:
        """A disconnects while A3 is held: A3 is fenced and torn down, A leaves nothing."""

        self.require_live("disconnect-a")
        a = self.owners["a"]
        follow, held = self.pod_run("a3"), self.pod_run("b-r1")
        follow.ending = True
        self.settle_run(follow, facts=follow.kept_facts, complete=follow.kept is not None)
        self.ledger.record("disconnect", "a3_settled_from_hold", self.settled_known("a3"))
        disconnect = self.disconnect_owner(a)
        self.a_stop_at = self.clock.now()
        row = self.request_row(follow.request_id)
        tokens = self.kube.ctl(
            "get",
            "secret",
            f"{follow.claim}{pto.CLAIM_TOKENS_SUFFIX}",
            "--ignore-not-found",
            "-o",
            "name",
        )
        checks: dict[str, Any] = {
            "disconnect_inactive": disconnect["inactive"],
            "disconnect_generation_advanced": disconnect["generation_advanced"],
            "teardown_pending_runtimes": disconnect["pending_runtimes"],
            "teardown_pending_custody_cleanup": disconnect["pending_custody_cleanup"],
            "a3_failed_enrollment_fenced": row.get("status") == "failed"
            and row.get("terminal_cause") == "enrollment_fenced",
            "a3_termination_observation": row.get("observed") is True,
            "a3_pod_absent": bool(follow.pod_uid) and self.pod_uid_gone(follow),
            "a3_tokens_secret_absent": bool(follow.claim) and not tokens.stdout.strip(),
            "a_claims_absent": not self.owner_claims(a),
            "a_custody_secret_absent": disconnect["custody_secret_absent"],
        }
        for key, value in checks.items():
            self.ledger.record("disconnect", key, value)
        self.wait_until(
            follow.eof.is_set,
            until=self.clock.now() + SETTLE_SECONDS,
            what="A3 capture end",
            every=1.0,
        )
        grown = self.grew_after_settle(follow)
        self.close_pod(follow)
        delivery = self.delivery_checks(self.owners["b"], held.claim)
        b_grant = delivery.get("own_grant") is True and delivery.get("not_other") is True
        self.ledger.record("disconnect", "b_grant_unchanged", b_grant)
        self.require(
            {
                **{
                    key: (type(value) is int and value == 0)
                    if key.startswith("teardown_")
                    else value is True
                    for key, value in checks.items()
                },
                "a3_no_growth": not grown,
                "a3_settled": self.settled_known("a3"),
                "b_grant_unchanged": b_grant,
            },
            "A's disconnect",
        )

    def grew_after_settle(self, run: PodRun) -> bool:
        """Whether the final capture holds more usage than the snapshot the run settled from."""

        settled = run.facts
        final = self.pod_facts(run)
        before = {
            item.path: (item.output_tokens, item.assistant_turns)
            for item in (settled.sessions if settled is not None else [])
        }
        grown = settled is None or any(
            item.output_tokens > before.get(item.path, (0, 0))[0]
            or item.assistant_turns > before.get(item.path, (0, 0))[1]
            for item in final.sessions
        )
        if grown:
            self.violations.append(f"{run.group}:-:usage_after_settle")
            self.ledger.record("budget", "gate_violations", sorted(set(self.violations)))
        return grown

    def phase_process_interruption(self) -> None:
        """B-r1's held CLI is killed in place; the kernel's retry runs B-r2 on the same claim."""

        self.require_live("process-interruption")
        b = self.owners["b"]
        held = self.pod_run("b-r1")
        if held.kept is None:
            self.fail_paid("B-r1 is not held at its prompt")
        held.ending = True
        self.settle_run(held, facts=held.kept_facts, complete=True)
        before = self.request_row(held.request_id)
        epoch = before.get("runtime_epoch")
        cli = self.cli_identity(held.kept)
        lineage = {
            "request": before.get("id") == held.request_id,
            "epoch": type(epoch) is int,
            "claim": bool(held.claim) and before.get("runtime_claim_name") == held.claim,
            "pod": bool(held.pod_uid),
            "cli": cli is not None,
        }
        self.ledger.record("restarts", "process_lineage_before", all(lineage.values()))
        self.ledger.record("restarts", "process_marker_before", self.marker_present(b))
        repair = self.new_run("b-r2", card=4)
        repair.request_id, repair.work_item_id = held.request_id, held.work_item_id
        repair.conversation_id = held.conversation_id
        self.reserve(repair)
        deletes = len(self.pod_deletions)
        witness = self.interrupt_native_process(held.pod, held.kept)
        pod = self.await_container_restart(held)
        self.wait_until(
            held.eof.is_set,
            until=self.clock.now() + SETTLE_SECONDS,
            what="B-r1 capture end",
            every=1.0,
        )
        grown = self.grew_after_settle(held)
        with held.harvest_lock:
            baseline = held.harvest.transcript_files() if held.harvest is not None else {}
        self.close_pod(held)
        after = self.request_row(held.request_id)
        claims = self.owner_claims(b)
        facts = dict(witness)
        facts.setdefault("confirmed_epoch", after.get("runtime_epoch"))
        facts.update(
            {
                "old_epoch": epoch,
                "old_claim_still_bound_to_request": after.get("runtime_claim_name") == held.claim,
                "successor_claim_observed": claims != [held.claim],
                "driver_pod_delete_calls": sum(
                    1 for name, uid, _at in self.pod_deletions[deletes:] if uid == held.pod_uid
                ),
            }
        )
        verdict = poes.process_interruption_checks(facts)
        self.ledger.record("diagnostics", "process_interruption", verdict)
        repair.claim, repair.pod, repair.pod_uid = held.claim, held.pod, held.pod_uid
        repair.claim_labels = dict(held.claim_labels)
        repair.pod_record = pod
        repair.grant_ref = pps.pod_grant_ref(pod)
        repair.restart_count = self.runner_restarts(pod)
        repair.baseline = {path: data for path, data in baseline.items() if path.endswith(".jsonl")}
        repair.stale_clis = set(held.clis)
        repair.started_at = self.clock.now()
        self.begin_pod_run(repair)
        tool_at: float | None = None

        def tool_after_stop() -> bool:
            nonlocal tool_at
            tool_at = self.first_tool(repair, self.a_stop_at or float("inf"))
            return tool_at is not None

        self.wait_until(
            tool_after_stop, until=self.deadline(), what="B-r2's first tool call after A's stop"
        )
        row = self.request_row(repair.request_id)
        fresh = bool(repair.clis) and not (set(repair.clis) & set(held.clis))
        checks = {
            "process_old_runtime_absent": witness.get("native_gone") is True and not grown,
            "process_termination_epoch": verdict["old_epoch_confirmed_before_bind"],
            "process_fresh_runtime": fresh,
            "process_same_work_item": row.get("work_item_id") == held.work_item_id,
            "process_same_conversation": repair.conversation_id == held.conversation_id,
            "process_owner_unchanged": row.get("owner_principal_id") == b.principal_id
            and row.get("tenant_id") == b.tenant_id,
            "process_generation_unchanged": row.get("enrollment_generation")
            == pps.OWNER_GENERATION,
            "process_grant_unchanged": repair.delivery.get("own_grant") is True
            and repair.delivery.get("not_other") is True,
            "process_marker_after": self.marker_present(b),
            "process_continued_native": tool_at is not None,
            "worker_continued_native": tool_at is not None and tool_at > self.worker_started_at,
            "process_usage_settled": self.settled_known("b-r1"),
            "process_identity_matched": witness.get("native_identity_matched") is True,
            "process_no_pod_delete": verdict["interruption_process_only"],
            "process_claim_still_bound": row.get("runtime_claim_name") == held.claim,
            "process_no_successor_claim": self.owner_claims(b) == [held.claim],
            "process_same_request_epoch": row.get("id") == held.request_id
            and type(epoch) is int
            and row.get("runtime_epoch") == epoch,
        }
        for key, value in checks.items():
            self.ledger.record("restarts", key, value)
        self.ledger.record(
            "disconnect",
            "b_tool_after_a_stop",
            tool_at is not None and self.a_stop_at is not None and tool_at > self.a_stop_at,
        )
        self.require(checks, "B's process interruption")

    def await_container_restart(self, run: PodRun) -> dict[str, Any]:
        """The same pod uid with the runner container restarted and running again."""

        found: dict[str, Any] = {}

        def restarted() -> bool:
            nonlocal found
            pods = self.kube.json("get", "pods", "-l", f"{pp.POD_CLAIM_LABEL}={run.claim}")
            for pod in pods.get("items") or [] if isinstance(pods, dict) else []:
                uid = str((pod.get("metadata") or {}).get("uid") or "")
                if uid != run.pod_uid:
                    self.fail_paid("B-r1's pod was replaced, not restarted in place")
                if self.runner_restarts(pod) > run.restart_count and self.runner_running(pod):
                    found = pod
                    return True
            return False

        self.wait_until(
            restarted,
            until=self.clock.now() + RESTART_SECONDS,
            what="runner container restart",
            every=2.0,
        )
        return found

    def phase_approve_b2(self) -> None:
        """B-r2 parks on card 4; the operator approves; the same lineage branch moves."""

        self.require_live("approve-b2")
        b = self.owners["b"]
        first, repair = self.pod_run("b1"), self.pod_run("b-r2")
        self.park_and_settle(repair)
        summaries = self.summaries.get("b-r2", [])
        roles = pto.role_checks(summaries)
        calls = pto.native_tool_calls(
            self.pod_files(repair),
            exclude_ids=repair.exclude_ids,
            not_before=self.not_before(repair),
        )
        native = any(not call["subagent"] and call["result"] for call in calls)
        self.unlisted_control(repair)
        self.resolve_card(repair, "approved")
        facts = self.published_facts(repair, full=False)
        repaired = self.repaired_checks(facts["head"], facts["branch"])
        item = self.work_item_of(first.work_item_id)
        tuples = [pps.owner_of(item), *pps.request_owners(item)]
        owned = len(tuples) > 1 and all(pps.owner_matches(value, b.tuple()) for value in tuples)
        requests = [row for row in item.get("requests") or [] if isinstance(row, dict)]
        checks: dict[str, Any] = {
            "repair_implementer_b": roles["implementer_session"] and roles["implementer_model"],
            "repair_diff_reviewer_b": roles["diff_reviewer_session"],
            "repair_same_request": repair.request_id == first.request_id
            and [str(row.get("id")) for row in requests] == [first.request_id],
            "repair_owner_b": self.owner_claim_checks("b", repair),
            "repair_native_execution": native,
            "b2_publication_succeeded": facts["succeeded"],
            "b2_same_branch": facts["branch"] == first.branch and facts["branch_from_api"],
            "b2_same_pr": facts["pr"] == self.b1_pull,
            "b2_tree_matches_reviewed": facts["tree"],
            "repaired_head_on_branch": facts["head"] != self.b1_head
            and self.branch_head(first.branch) == facts["head"],
            **repaired,
            "ci_author_not_owner": owned and all(self.approver_checks),
            "owner_unchanged_through_repair": owned,
        }
        self.ledger.record("roles", "repair_implementer_b", checks.pop("repair_implementer_b"))
        self.ledger.record("roles", "repair_diff_reviewer_b", checks.pop("repair_diff_reviewer_b"))
        publication_keys = (
            "b2_publication_succeeded",
            "b2_same_branch",
            "b2_same_pr",
            "b2_tree_matches_reviewed",
        )
        for key, value in checks.items():
            criterion = "publication" if key in publication_keys else "ci-repair"
            self.ledger.record(criterion, key, value)

        def completed() -> bool:
            row = self.request_of(self.work_item_of(first.work_item_id), first.request_id)
            return row.get("status") == "completed"

        done = self.observed(completed, pp.CI_WAIT_SECONDS, "B's request completed")
        published = self.work_item_of(first.work_item_id).get("state") == "published"
        self.ledger.record("publication", "b_request_completed", done)
        self.ledger.record("publication", "b_work_item_published", published)
        self.ledger.record(
            "disconnect",
            "b_passing_publication_after",
            done
            and published
            and repaired["repaired_checks_passed"] is True
            and self.a_stop_at is not None
            and repair.resolved_at > self.a_stop_at,
        )
        self.b2_done_at = self.clock.now()
        self.claims_after_b2 = set(self.agent_claims())
        self.require(
            {**checks, "b_request_completed": done, "b_work_item_published": published},
            "B's repair publication",
        )

    def owner_claim_checks(self, owner: str, run: PodRun) -> bool:
        principal = str(uuid.UUID(self.owners[owner].principal_id))
        return (
            pto.claim_labels_match(
                run.claim_labels,
                tenant_id=self.owners[owner].tenant_id,
                principal_id=principal,
                generation=pps.OWNER_GENERATION,
            )
            and run.delivery.get("own_grant") is True
            and run.delivery.get("not_other") is True
        )

    def repaired_checks(self, head: str, branch: str) -> dict[str, bool]:
        """The repaired head adds the CI_ACK line; its latest checks and every required one pass.

        A pending or missing check fails: the wait ends at CI_WAIT_SECONDS and
        whatever is still pending is a failure, and a required name with no
        successful run or status at the head is missing.
        """

        content = self.forge_file(pto.CI_ACK_FILE, head) if pps.is_commit_sha(head) else None
        latest, runs, statuses = self.head_checks(head)
        self.ledger.record("ci-repair", "repaired_head_checks", latest)
        required: list[str] = []
        if self.default_branch:
            base = urllib.parse.quote(self.default_branch, safe="")
            required = pto.required_check_names(
                self.forge_get(f"/rules/branches/{base}"), self.forge_get(f"/branches/{base}")
            )
        names = sorted(set(self.b1_check_names) | set(required))
        self.ledger.record("ci-repair", "required_check_names", names)
        return {
            "repaired_head_adds_ci_ack": content is not None
            and pto.ci_ack_present(content, branch),
            "repaired_checks_passed": latest.get("all_completed_successfully") is True,
            "required_checks_present": pto.checks_present(names, runs, statuses),
        }

    def phase_post_disconnect(self) -> None:
        """After another worker restart A cannot launch, reuse or resume; B stays active."""

        self.require_live("post-disconnect")
        a, b = self.owners["a"], self.owners["b"]
        follow = self.pod_run("a3")
        before = self.stop_worker()
        restart = self.restart_worker(before)
        after = self.refusals_after_disconnect(a, follow.work_item_id)
        rows = self.request_rows(follow.work_item_id)
        a3_sequence = next(
            (row.get("sequence") for row in rows if row.get("id") == follow.request_id), None
        )
        successors = [
            row
            for row in rows
            if row.get("owner_lost_retry") is True
            or (
                type(a3_sequence) is int
                and type(row.get("sequence")) is int
                and row["sequence"] > a3_sequence
            )
        ]
        revalidation = self.revalidate(a, follow)
        b_code, b_status = self.subscription(b, "status")
        b_active = (
            b_code == 0
            and b_status.get("state") == "active"
            and b_status.get("generation") == pps.OWNER_GENERATION
        )
        round3 = pto.stream_has_event(
            self.valkey_xrange(RUNS_STREAM, count=STREAM_SCAN_COUNT),
            f"work-item-{self.pod_run('b1').request_id}-ci-{CI_REPAIR_ROUND + 1}",
        )
        late = [name for name in self.agent_claims() if name not in self.claims_after_b2]
        checks = {
            "post_restart_admission_refused": after["admission_refused"],
            "post_restart_no_a_claim": after["no_claim_in_window"],
            "post_restart_no_a_running": after["no_request_running"],
            "post_restart_no_a3_successor": bool(rows) and not successors,
            "post_restart_revalidation_refused": revalidation,
            "b_active_gen1_after": b_active,
        }
        for key, value in checks.items():
            self.ledger.record("disconnect", key, value)
        self.ledger.record("ci-repair", "no_round3", not round3)
        self.ledger.record("restarts", "post_disconnect_worker", restart)
        unplanned = bool(successors) or round3 or bool(late) or bool(self.unmapped)
        self.fence_paid_starts()
        self.ledger.record("budget", "unplanned_starts_fenced", not unplanned)
        self.ledger.end_live(now=self.clock.now())
        self.record_summary_keys()
        self.require(
            {**checks, "no_round3": not round3, "no_unplanned_start": not unplanned},
            "post-disconnect",
        )

    def revalidate(self, owner: Owner, run: PodRun) -> bool:
        """Revalidating the WorkItem at the versions read now answers enrollment_inactive."""

        query = urllib.parse.urlencode(
            {
                "tenant_id": owner.tenant_id,
                "agent_id": self.agent_id,
                "owner_principal_id": owner.principal_id,
            }
        )
        status, context = self.internal(
            "GET", f"/v1/internal/work-items/{run.work_item_id}/personal-context?{query}"
        )
        request = context.get("request") if isinstance(context, dict) else None
        enrollment = context.get("enrollment") if isinstance(context, dict) else None
        if status != 200 or not isinstance(request, dict) or not isinstance(enrollment, dict):
            return False
        code, body = self.internal(
            "POST",
            f"/v1/internal/work-items/{run.work_item_id}/personal-context/revalidate",
            {
                "tenant_id": owner.tenant_id,
                "agent_id": self.agent_id,
                "owner_principal_id": owner.principal_id,
                "request_id": str(request.get("id")),
                "expected_work_item_version": context.get("work_item_version"),
                "expected_request_version": request.get("version"),
                "expected_enrollment_version": enrollment.get("version"),
            },
        )
        return code == 409 and self.code_of(body) == "enrollment_inactive"

    # -- the two methods stream S2b-2 supplies -------------------------------------------

    def interrupt_native_process(self, pod: str, hold: Mapping[str, Any]) -> dict[str, Any]:
        """Kill B-r1's held native CLI and SIGTERM runner PID 1 in ``pod`` (stream S2b-2).

        ``hold`` is B-r1's kept prompt-hold record; its ``cli_pid`` and raw
        ``cli_start`` ticks name the native process, which must match before
        any signal. The pod is never deleted. Returns the runner-side witness
        facts ``poes.process_interruption_checks`` reads: ``action``
        (``"signal_processes"``), ``native_identity_matched`` (the signalled
        process had that pid and starttime) and ``native_gone`` (it is absent
        afterwards), plus ``confirmed_epoch`` when the termination evidence
        names one. The driver adds the epoch, claim and pod-delete facts from
        SQL, the claim listing and its own delete log.
        """

        raise StepFailed(f"interrupt_native_process: {S2B2}")

    def delivery_checks(self, owner: Owner, claim: str) -> dict[str, bool]:
        """Whether ``<claim>-tokens`` delivers ``owner``'s own grant (stream S2b-2).

        Returns ``own_grant`` (the Secret's grant digest equals the digest of
        ``owner``'s grant file) and ``not_other`` (it differs from the other
        owner's). Digests only: the Secret's bytes and the grant are never
        printed, logged, recorded or placed in argv.
        """

        raise StepFailed(f"delivery_checks: {S2B2}")

    # -- keys read across the run ---------------------------------------------------------

    def record_summary_keys(self) -> None:
        """Owner, role, approval, publication and budget keys from the evidence gathered."""

        for name, owner in self.owners.items():
            runs = [run for run in self.pods if run.owner == name]
            summaries = [item for run in runs for item in self.summaries.get(run.group, [])]
            if summaries:
                for key, value in pto.role_checks(summaries).items():
                    self.ledger.record("roles", f"{key}_{name}", value)
            facts = [run.facts for run in runs if run.facts is not None]
            if facts:
                self.ledger.record(
                    "roles", f"aci_tool_observed_{name}", sum(f.aci_tool_calls for f in facts) > 0
                )
            harvests = [run.harvest for run in runs if run.harvest is not None]
            if harvests:
                self.ledger.record(
                    "roles",
                    f"bundle_hook_observed_{name}",
                    any(item.review_verdicts()["hook_observed"] for item in harvests),
                )
            if owner.work_item_ids:
                self.safe(functools.partial(self.record_owner_items, owner), None)
        carded = [run for run in self.pods if run.card]
        self.ledger.record(
            "publication",
            "absence_checks_use_api_branches",
            bool(carded)
            and all(run.branch_known and run.branch in self.publication_branches for run in carded),
        )
        self.ledger.record(
            "approval",
            "no_push_before_approval",
            set(self.no_push) == {1, 2, 3, 4} and all(self.no_push.values()),
        )
        self.ledger.record(
            "roles",
            "sessions_mapped",
            not self.unmapped and any(run.sessions for run in self.pods),
        )
        prompts = [run for run in self.pods if run.prompt_seen]
        self.ledger.record(
            "budget",
            "limits_written_before_prompt",
            bool(prompts) and all(run.limits_acked for run in prompts),
        )
        self.ledger.record(
            "budget", "gate_every_response", not self.violations and self.releases > 0
        )
        self.ledger.record(
            "budget",
            "zero_usage_classified",
            bool(self.zero_classes) and all(item["class"] for item in self.zero_classes),
        )
        rows = self.owner_ledger.session_rows()
        within = [
            row["output_tokens"] < row["reserved_output"]
            and row["assistant_turns"] < row["reserved_turns"]
            and row["elapsed"] < row["reserved_seconds"]
            for row in rows
            if row["source"] == "native" and row["output_tokens"] is not None
        ]
        self.ledger.record(
            "budget", "no_allowance_breach", not self.breaches and bool(within) and all(within)
        )
        owners = self.ledger.observations("owners")
        refs = [
            values
            for name in pto.OWNERS
            for key, values in self.checks[name].items()
            if key == "owners/runner_grant_ref_per_claim"
        ]
        self.ledger.record(
            "budget",
            "no_credential_fallback",
            owners.get("installation_grant_absent") is True
            and owners.get("service_credentials_absent") is True
            and len(refs) == len(pto.OWNERS)
            and all(all(values) for values in refs),
        )

    def record_owner_items(self, owner: Owner) -> None:
        items = [self.work_item_of(item) for item in dict.fromkeys(owner.work_item_ids)]
        self.ledger.record(
            "owners",
            f"work_item_owner_{owner.name}",
            all(pps.owner_matches(pps.owner_of(item), owner.tuple()) for item in items),
        )
        requests = [value for item in items for value in pps.request_owners(item)]
        self.ledger.record(
            "owners",
            f"request_owners_{owner.name}",
            bool(requests) and all(pps.owner_matches(value, owner.tuple()) for value in requests),
        )
        agrees = []
        for item in items:
            cli = self.curie.json("work-items", str(item.get("id")))
            cli_item = cli.get("item") if isinstance(cli, dict) else None
            agrees.append(
                pps.owner_matches(pps.owner_of(cli_item), owner.tuple())
                and pps.owner_of(cli_item) == pps.owner_of(item)
            )
        self.ledger.record("owners", f"cli_owner_{owner.name}", bool(agrees) and all(agrees))

    # -- small helpers --------------------------------------------------------

    @property
    def fullname(self) -> str:
        return self.inputs.fullname

    @property
    def api_origin(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    def kube_in(self, namespace: str) -> pp.Kube:
        return pp.Kube(self.shell, self.inputs.context, namespace)

    def worker_selector(self) -> str:
        return (
            f"app.kubernetes.io/component=worker,app.kubernetes.io/instance={self.inputs.release}"
        )

    def api_selector(self) -> str:
        return f"app.kubernetes.io/component=api,app.kubernetes.io/instance={self.inputs.release}"

    def record_both(self, key: str, value: Any, *criteria: str) -> None:
        for criterion in criteria:
            self.ledger.record(criterion, key, value)

    # -- phase 1: refusals ------------------------------------------------------

    def phase_refusals(self) -> None:
        now = self.clock.now()
        for item in self.scenario.owners:
            if item.usable_until - now < pto.GRANT_MIN_LIFETIME:
                raise pps.Refusal("lifetime_insufficient", f"owners.{item.owner}.usable_until")
        for namespace in (self.inputs.namespace, self.grant_namespace):
            existing = self.kube.ctl(
                "get",
                "namespace",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
                namespaced=False,
            )
            if existing.stdout.strip():
                raise pps.Refusal("namespace_exists", "target.namespace")
        self.verify_all_images()
        self.foreign_before = self.foreign_snapshot()

    def verify_all_images(self) -> None:
        verified = pp.verify_images(self.inputs, self.ledger)
        self.digests = {name: image.digest for name, image in verified.items()}
        self.dex_image = verify_dex(self.inputs, self.ledger)
        if not bundle_gated(self.inputs.work_dir / "bundle"):
            raise pps.Refusal("candidate_image_mismatch", "bundle gate")

    # -- phase 2: namespaces ------------------------------------------------------

    def phase_namespace(self) -> None:
        super().phase_namespace()
        self.finalizer.register("delete-grant-namespace", self.delete_grant_namespace)
        self.kube.create(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": self.grant_namespace,
                    "labels": {pps.MANAGED_BY_LABEL: pps.MANAGED_BY_VALUE},
                    "annotations": {pps.RUN_ANNOTATION: self.inputs.run_id},
                },
            }
        )

    def delete_grant_namespace(self) -> None:
        key = pps.RUN_ANNOTATION.replace(".", "\\.")
        current = self.kube.ctl(
            "get",
            "namespace",
            self.grant_namespace,
            "--ignore-not-found",
            "-o",
            "jsonpath={.metadata.annotations." + key + "}",
            namespaced=False,
        )
        owner = current.stdout.decode().strip()
        if not owner:
            return
        if owner != self.inputs.run_id:
            raise StepFailed("grant namespace ownership changed; not deleting")
        self.safe(self.record_custody, None)
        self.kube.ctl(
            "delete",
            "namespace",
            self.grant_namespace,
            "--wait=true",
            "--timeout=240s",
            namespaced=False,
            timeout=260,
        )

    # -- phase 3: images (verify, not build) ----------------------------------------

    def phase_images(self) -> None:
        register_image_cleanup(self.finalizer, self.shell, self.ledger, self.inputs)
        self.finalizer.register("remove-work-dir", self.remove_work_dir)
        images = self.ledger.observations("images")
        self.pushed = pp.recorded_pushed(self.ledger)
        self.local_tags = [str(tag) for tag in images.get("local_tags") or []]
        self.verify_all_images()
        self.ledger.record("images", "images_reused", True)
        self.ledger.record("install", "dex_image", self.dex_image)

    # -- phase 4: install ---------------------------------------------------------------

    def record_rollout_diagnostics(self) -> None:
        """Pod states and Warning events, plus the log tail of each restarting container."""

        super().record_rollout_diagnostics()
        logs = self.safe(self.restarting_container_logs, {"unavailable": "pod list unreadable"})
        self.ledger.record("install", "rollout_container_logs", logs)
        self.out.progress("rollout container logs: " + json.dumps(logs, sort_keys=True))

    def restarting_container_logs(self) -> dict[str, str]:
        """Last 20 log lines (previous, else current) of each restarting container."""

        found: dict[str, str] = {}
        for pod in self.kube.json("get", "pods").get("items") or []:
            name = str((pod.get("metadata") or {}).get("name"))
            status = pod.get("status") or {}
            for item in [
                *(status.get("initContainerStatuses") or []),
                *(status.get("containerStatuses") or []),
            ]:
                waiting = (item.get("state") or {}).get("waiting") or {}
                if not (item.get("restartCount", 0) or waiting.get("reason") == "CrashLoopBackOff"):
                    continue
                container = str(item.get("name"))
                found[f"{name}/{container}"] = self.container_log_tail(name, container)
        return found

    def container_log_tail(self, pod: str, container: str) -> str:
        for flags in (("--previous",), ()):
            try:
                result = self.kube.ctl(
                    "logs", pod, "-c", container, *flags, "--tail=20", check=False
                )
            except Exception:  # noqa: BLE001 - a failed read is recorded, never raised
                continue
            text = result.stdout.decode(errors="replace")
            if result.returncode == 0 and text.strip():
                return self.scrubber.scrub(text)[:2000]
        return "unavailable"

    def phase_install(self) -> None:
        inputs = self.inputs
        work = pp.private_dir(inputs.work_dir)
        chart_root = work / "chart"
        archive = work / "chart.tar"
        self.shell.run(
            [
                "git",
                "-C",
                str(pp.REPO_ROOT),
                "archive",
                "--format=tar",
                "-o",
                str(archive),
                inputs.candidate,
                "charts/curie",
            ],
            timeout=300,
        )
        chart_root.mkdir(mode=0o700, exist_ok=True)
        pp.extract_archive(archive, chart_root)
        self.chart = chart_root / "charts" / "curie"
        self.create_app_secret()
        self.create_slack_secret()
        self.create_dex()
        controller = self.kube.ctl(
            "get",
            "deployment",
            pp.CONTROLLER_DEPLOYMENT,
            "--namespace",
            pp.CONTROLLER_NAMESPACE,
            "--ignore-not-found",
            "-o",
            "name",
            namespaced=False,
        ).stdout.strip()
        base = pps.install_values(
            release=inputs.release,
            registry=inputs.registry,
            digests=self.digests,
            agent=self.agent,
            repo=str(inputs.app["repo"]),
            app_id=str(inputs.app["id"]),
            app_secret=self.app_secret,
            trigger_label=self.trigger_label,
            priority_classes=self.reused_priority_classes(),
            controller_deploy=not controller,
            model_addresses=self.resolver("api.anthropic.com"),
            slack_bot_secret=self.slack_secret,
        )
        values = pto.two_owner_install_values(
            base,
            release=inputs.release,
            namespace=inputs.namespace,
            grant_namespace=self.grant_namespace,
            issuer=self.issuer,
            redirect_uri=self.redirect_uri,
            client_id=pto.OIDC_CLIENT_ID,
            client_secret_ref=(OIDC_SECRET, OIDC_SECRET_KEY),
        )
        values_file = work / "values.json"
        self.finalizer.register("delete-values-file", lambda: values_file.unlink(missing_ok=True))
        pp.write_private(values_file, json.dumps(values, indent=2).encode())
        self.finalizer.register("helm-uninstall", self.helm_uninstall)
        self.shell.run(
            [
                "helm",
                "--kube-context",
                inputs.context,
                "--namespace",
                inputs.namespace,
                "install",
                inputs.release,
                str(self.chart),
                "-f",
                str(values_file),
                "--timeout",
                "900s",
            ],
            timeout=960,
        )
        try:
            for workload in ("api", "worker", "channel-gateway"):
                self.kube.ctl(
                    "rollout",
                    "status",
                    f"deployment/{self.fullname}-{workload}",
                    "--timeout=600s",
                    timeout=620,
                )
            self.kube.ctl(
                "rollout",
                "status",
                f"statefulset/{self.fullname}-postgres",
                "--timeout=600s",
                timeout=620,
            )
            self.kube.ctl(
                "rollout", "status", f"deployment/{DEX_NAME}", "--timeout=300s", timeout=320
            )
        except Exception:
            self.record_rollout_diagnostics()
            raise
        self.wait_rollout_hold()
        self.check_install_images()
        self.safe(self.record_volumes, None)
        self.check_installation_grant(values)

    def create_app_secret(self) -> None:
        if self.inputs.app_dir is None:
            raise StepFailed("the forge App kit is not configured")
        pem = (self.inputs.app_dir / "app.pem").read_bytes()
        key = self.scrubber.hold(pps.HeldSecret(pem.decode("ascii", errors="replace")))
        self.finalizer.register("delete-app-secret", lambda: self.delete_secret(self.app_secret))
        try:
            self.kube.create(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": self.app_secret},
                    "type": "Opaque",
                    "data": {"privateKey": base64.b64encode(pem).decode()},
                },
                secret=True,
            )
        finally:
            key.clear()

    def create_dex(self) -> None:
        """The OIDC client Secret, then Dex's config Secret, Deployment and Service.

        All in the release namespace (never the grant namespace, which holds
        only custody Secrets). The client secret is a fresh random value held
        in the scrubber; it reaches Kubernetes only through ``create -f -``.
        """

        client_secret = self.scrubber.hold(pps.HeldSecret(secrets.token_urlsafe(32)))
        self.finalizer.register(
            "delete-oidc-client-secret", lambda: self.delete_secret(OIDC_SECRET)
        )
        self.kube.create(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": OIDC_SECRET, "labels": dict(DEX_LABELS)},
                "type": "Opaque",
                "data": {
                    OIDC_SECRET_KEY: base64.b64encode(client_secret.reveal().encode()).decode()
                },
            },
            secret=True,
        )
        config = pto.dex_config(
            issuer=self.issuer,
            client_id=pto.OIDC_CLIENT_ID,
            client_secret=client_secret.reveal(),
            redirect_uri=self.redirect_uri,
            users=[pto.OWNER_EMAILS[name] for name in pto.OWNERS],
        )
        manifests = pto.dex_manifests(
            namespace=self.inputs.namespace,
            name=DEX_NAME,
            image_ref=self.dex_image,
            config_secret=DEX_CONFIG_SECRET,
            labels=DEX_LABELS,
            config=config,
        )
        self.finalizer.register("delete-dex", self.delete_dex)
        for manifest in manifests:
            self.kube.create(manifest, secret=manifest.get("kind") == "Secret")
        self.ledger.record("install", "dex_applied", [str(item.get("kind")) for item in manifests])

    def delete_dex(self) -> None:
        self.kube.ctl(
            "delete",
            "deployment,service,secret",
            "-l",
            DEX_SELECTOR,
            "--ignore-not-found",
            "--wait=true",
            timeout=180,
        )

    def wait_rollout_hold(self) -> None:
        """Every serving API pod on one pod-template-hash (apps/api rollout_hold.single_template).

        Until then enroll and personal admission answer
        ``personal_rollout_in_progress``.
        """

        hashes: set[str] = set()

        def single() -> bool:
            nonlocal hashes
            pods = self.kube.json("get", "pods", "-l", self.api_selector())
            serving = [
                pod
                for pod in pods.get("items") or []
                if isinstance(pod, dict)
                and (pod.get("status") or {}).get("phase") in {"Running", "Pending"}
            ]
            hashes = {
                str(
                    ((pod.get("metadata") or {}).get("labels") or {}).get(TEMPLATE_HASH_LABEL) or ""
                )
                for pod in serving
            }
            return len(hashes) == 1 and "" not in hashes

        self.wait_until(
            single, until=self.clock.now() + ROLLOUT_HOLD_SECONDS, what="API rollout hold"
        )
        self.ledger.record("install", "api_single_template", True)

    def check_install_images(self) -> None:
        pods = self.kube.json("get", "pods")
        failures = pps.running_image_failures(
            pods,
            {
                name: self.digests[name]
                for name in ("curie-api", "curie-worker", "curie-dispatcher")
            },
            registry=self.inputs.registry,
            required=("curie-api", "curie-worker", "curie-dispatcher"),
        )
        dex = self.dex_image.rsplit("@", 1)[-1]
        dex_failures = pps.running_image_failures(
            pods, {DEX_REPOSITORY: dex}, registry=self.inputs.registry, required=(DEX_REPOSITORY,)
        )
        self.ledger.record("owners", "platform_images_candidate", not failures)
        self.ledger.record("install", "dex_image_running", not dex_failures)
        self.observe_worker(pods)
        if failures or dex_failures:
            raise StepFailed("candidate images are not the running images")

    def check_installation_grant(self, values: Mapping[str, Any]) -> None:
        """No installation-wide grant or service credential in the values or live objects."""

        helm = self.shell.run(
            [
                "helm",
                "--kube-context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "get",
                "values",
                self.inputs.release,
                "--all",
                "-o",
                "json",
            ],
        )
        try:
            rendered = json.loads(helm.stdout)
        except ValueError:
            raise StepFailed("helm returned unreadable values") from None
        listed = self.kube.json("get", "secrets", sensitive_output=True)
        # Metadata and key names only; every value is dropped before any check.
        secret_docs = [
            {
                "kind": "Secret",
                "metadata": {
                    "name": (item.get("metadata") or {}).get("name"),
                    "labels": (item.get("metadata") or {}).get("labels") or {},
                },
                "data": {key: "" for key in (item.get("data") or {})},
            }
            for item in listed.get("items") or []
            if isinstance(item, dict)
        ]
        documents = [
            dict(values),
            rendered,
            self.kube.json("get", "sandboxtemplates.extensions.agents.x-k8s.io"),
            self.kube.json("get", "deployments"),
            {"items": secret_docs},
        ]
        # installation_grant_findings already folds in the service-credential findings.
        findings = pto.installation_grant_findings(documents)
        service = sorted(
            set(pps.deployed_credential_findings(documents, grant_secret=""))
            - {pps.PERSONAL_GRANT_ENV}
        )
        self.ledger.record("install", "grant_findings", findings)
        self.ledger.record("owners", "installation_grant_absent", not findings)
        self.ledger.record("owners", "service_credentials_absent", not service)
        self.ledger.record("rehearsal", "install_no_runner_grant", not findings)
        if findings:
            raise pps.Refusal("deployed_service_credential", ", ".join(findings))

    def worker_pods(self, pods: Any | None = None) -> list[dict[str, Any]]:
        listed = (
            pods
            if pods is not None
            else self.kube.json("get", "pods", "-l", self.worker_selector())
        )
        return [
            pod
            for pod in (listed.get("items") or [] if isinstance(listed, dict) else [])
            if isinstance(pod, dict)
            and ((pod.get("metadata") or {}).get("labels") or {}).get("app.kubernetes.io/component")
            == "worker"
        ]

    def observe_worker(self, pods: Any | None = None) -> tuple[str, bool]:
        """The running worker pod's uid and whether it runs the candidate worker digest."""

        workers = self.worker_pods(pods)
        running = [pod for pod in workers if (pod.get("status") or {}).get("phase") == "Running"]
        failures = pps.running_image_failures(
            {"items": running},
            {"curie-worker": self.digests.get("curie-worker", "")},
            registry=self.inputs.registry,
            required=("curie-worker",),
        )
        uid = (
            str(((running[0].get("metadata") or {}).get("uid")) or "") if len(running) == 1 else ""
        )
        ok = bool(uid) and not failures
        self.worker_observations.append({"uid": uid, "candidate": ok})
        self.ledger.record("install", "worker_observations", self.worker_observations)
        self.ledger.record(
            "owners",
            "worker_images_candidate",
            all(item["candidate"] for item in self.worker_observations),
        )
        return uid, ok

    # -- phase 5: setup ------------------------------------------------------------------

    def phase_setup(self) -> None:
        inputs = self.inputs
        if inputs.app_dir is None:
            raise StepFailed("the forge App kit is not configured")
        self.forge = pp.Forge(
            self.shell,
            self.forge_http or pp.forge_http(self.scan),
            inputs.app_dir,
            inputs.app,
            self.scrubber,
            self.clock,
        )
        self.forge.mint()
        self.api_port = self.forward(f"svc/{self.fullname}-api", 8000)
        self.dex_port = self.forward(f"svc/{DEX_NAME}", pto.DEX_PORT)
        self.connect_api()
        self.finalizer.register("remove-owner-files", self.remove_owner_files)
        self.finalizer.register("clear-owner-tokens", self.clear_owner_tokens)
        pp.private_dir(pp.private_dir(inputs.work_dir) / "owners")
        for owner in self.owners.values():
            self.login(owner)
            self.prepare_token(owner)
        a, b = self.owners["a"], self.owners["b"]
        distinct = bool(a.principal_id) and a.principal_id != b.principal_id
        self.record_both("principals_distinct", distinct, "rehearsal")
        self.ledger.record("owners", "principal_ids_distinct", distinct)
        self.ledger.record("owners", "independence_asserted", self.scenario.independent_accounts)
        if self.scenario.mode == "live":
            self.ledger.record("owners", "grant_files_distinct", self.grant_files_distinct())
        if not distinct:
            raise StepFailed("the two Dex logins resolved one principal")
        for owner in self.owners.values():
            self.enroll(owner)
        self.check_custody()
        self.create_agent()
        for owner in self.owners.values():
            self.ledger.record(
                "owners",
                f"tenant_is_agent_tenant_{owner.name}",
                bool(owner.tenant_id) and owner.tenant_id == self.agent_tenant,
            )
        self.configure_agent()
        self.ledger.record(
            "approval",
            "principals_distinct",
            len(
                {
                    self.allowed_subject,
                    self.unlisted_subject,
                    a.principal_id,
                    b.principal_id,
                }
            )
            == 4,
        )

    def forward(self, target: str, remote: int) -> int:
        port = self.port_forward(target, remote)
        self.forwards.append(self.processes[-1])
        return port

    def connect_api(self) -> None:
        secret = self.kube.json("get", "secret", f"{self.fullname}-secrets", sensitive_output=True)
        data = secret.get("data") or {}
        try:
            api_key = base64.b64decode(data["apiKey"]).decode()
            worker_token = base64.b64decode(data["internalWorkerToken"]).decode()
        except (KeyError, ValueError):
            raise StepFailed("release secrets lack the api key or worker token") from None
        self.api = pp.CurieApi(
            self.http_factory(self.api_port),  # type: ignore[arg-type]
            self.scrubber.hold(pps.HeldSecret(api_key)),
            self.scrubber.hold(pps.HeldSecret(worker_token)),
        )

    def require_http(self) -> HttpClient:
        return self.require_api().http

    def internal(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        """One worker-token call to ``/v1/internal/...``."""

        api = self.require_api()
        return self.require_http().request(
            method, path, headers={"X-Curie-Worker-Token": api.worker_token.reveal()}, body=body
        )

    def console_get(self, owner: Owner, path: str) -> tuple[int, Any]:
        """A GET with the owner's principal console session cookie."""

        if owner.session is None or not owner.session.reveal():
            raise StepFailed(f"owner {owner.name} has no console session")
        return self.require_http().request(
            "GET", path, headers={"Cookie": f"{pto.SESSION_COOKIE}={owner.session.reveal()}"}
        )

    def login(self, owner: Owner) -> None:
        """A Dex password login, then ``GET /console/principal`` with the session."""

        try:
            session = pto.oidc_login(
                self.opener,
                api_origin=self.api_origin,
                issuer_origin=self.issuer,
                dex_local_origin=f"http://127.0.0.1:{self.dex_port}",
                redirect_origin=self.redirect_uri,
                email=owner.email,
                password=pto.DEX_PASSWORD,
            )
        except pto.LoginFailed as failure:
            raise StepFailed(f"owner {owner.name} Dex login failed at {failure.step}") from None
        owner.session = self.scrubber.hold(session)
        status, body = self.console_get(owner, "/console/principal")
        if status != 200 or not isinstance(body, dict):
            raise StepFailed(f"owner {owner.name} principal read returned {status}")
        try:
            owner.principal_id = str(uuid.UUID(str(body.get("id"))))
            owner.tenant_id = str(uuid.UUID(str(body.get("tenant_id"))))
        except ValueError:
            raise StepFailed(f"owner {owner.name} principal has no readable id") from None
        owner.principal_status = str(body.get("status") or "")
        active = owner.principal_status == "active"
        self.ledger.record("owners", f"principal_active_{owner.name}", active)
        self.ledger.record("rehearsal", f"dex_login_{owner.name}", active)
        owner.session_file = self.owner_dir / f"session-{owner.name}"
        pp.write_private(owner.session_file, (session.reveal() + "\n").encode())

    def prepare_token(self, owner: Owner) -> None:
        """Live: hold the caller's grant for the leak scan. Rehearsal: a synthetic marker file."""

        if self.scenario.mode == "live":
            source = self.scenario.owner(owner.name).grant_file
            owner.token = self.scrubber.hold(pps.load_grant(source))
            owner.token_file = source
            owner.token_kind = "grant"
            return
        marker = self.scrubber.hold(
            pps.HeldSecret(f"pto-rehearsal-marker-{owner.name}-{secrets.token_hex(16)}")
        )
        owner.token = marker
        owner.token_file = self.owner_dir / f"marker-{owner.name}"
        owner.token_kind = "marker"
        pp.write_private(owner.token_file, (marker.reveal() + "\n").encode())

    def grant_files_distinct(self) -> bool:
        """Resolved path, inode and in-memory digest all differ (``pto.load_inputs_file`` rule)."""

        identities = []
        for owner in self.owners.values():
            if owner.token_file is None or owner.token is None or not owner.token.reveal():
                return False
            info = os.stat(owner.token_file)
            identities.append(
                (
                    owner.token_file.resolve(),
                    (info.st_dev, info.st_ino),
                    hashlib.sha256(owner.token.reveal().encode()).digest(),
                )
            )
        first, second = identities
        return all(left != right for left, right in zip(first, second, strict=True))

    def remove_owner_files(self) -> None:
        if self.owner_dir.exists():
            shutil.rmtree(self.owner_dir)

    def clear_owner_tokens(self) -> None:
        """Drop live values; the scrubber keeps fingerprints for the final leak scan."""

        for owner in self.owners.values():
            if owner.token is not None:
                owner.token.clear()

    def subscription(self, owner: Owner, verb: str, *extra: str) -> tuple[int, dict[str, Any]]:
        """``curie --json cluster subscription <verb>`` with the owner's session file.

        One JSON object on stdout, success or failure (cli/src/main.rs; the
        ``--json`` error is ``{"error", "fix"}``). The token, when sent, is a
        file path; its bytes never reach argv.
        """

        if owner.session_file is None:
            raise StepFailed(f"owner {owner.name} has no session file")
        result = self.curie.run(
            "subscription",
            verb,
            "--session-file",
            str(owner.session_file),
            "--api-url",
            self.api_origin,
            *extra,
            check=False,
            full_output=True,
            timeout=120,
        )
        lines = [
            line for line in result.stdout.decode(errors="replace").splitlines() if line.strip()
        ]
        body: Any = None
        if len(lines) == 1:
            try:
                body = json.loads(lines[0])
            except ValueError:
                body = None
        return result.returncode, body if isinstance(body, dict) else {}

    def enroll(self, owner: Owner) -> None:
        if owner.token_file is None:
            raise StepFailed(f"owner {owner.name} has no token file")
        owner.state = "unknown"
        code, body = 1, dict[str, Any]()
        for _ in range(ENROLL_ATTEMPTS):
            code, body = self.subscription(owner, "enroll", "--token-file", str(owner.token_file))
            if code != CLI_TRANSIENT:
                break
            self.wait_rollout_hold()
        self.ledger.record(
            "setup",
            f"enroll_{owner.name}",
            {key: body.get(key) for key in ("action", "state", "generation", "version")}
            | {"exit": code},
        )
        if code != 0:
            raise StepFailed(f"owner {owner.name} enroll exited {code}")
        self.read_enrollment(owner, body)
        code, status = self.subscription(owner, "status")
        active = (
            code == 0
            and status.get("state") == "active"
            and status.get("generation") == pps.OWNER_GENERATION
        )
        self.ledger.record("owners", f"enrollment_active_gen1_{owner.name}", active)
        if not active:
            raise StepFailed(f"owner {owner.name} is not active at generation 1 after enroll")

    def read_enrollment(self, owner: Owner, body: Mapping[str, Any]) -> None:
        state, generation, version = body.get("state"), body.get("generation"), body.get("version")
        if not isinstance(state, str) or type(generation) is not int or type(version) is not int:
            raise StepFailed(f"owner {owner.name} subscription answer is unreadable")
        owner.state, owner.generation, owner.version = state, generation, version

    def custody_secrets(self) -> list[dict[str, str]]:
        """Custody Secrets in the grant namespace by label: name, enrollment and generation only."""

        result = self.kube_in(self.grant_namespace).ctl(
            "get", "secrets", "-l", f"{pto.CUSTODY_LABEL}=true", "-o", CUSTODY_JSONPATH
        )
        rows = []
        for line in result.stdout.decode(errors="replace").splitlines():
            parts = line.strip().split("|")
            if len(parts) == 3 and parts[0]:
                rows.append({"name": parts[0], "enrollment": parts[1], "generation": parts[2]})
        return rows

    def record_custody(self) -> None:
        self.custody_seen.update(row["name"] for row in self.custody_secrets())

    def check_custody(self) -> None:
        rows = self.custody_secrets()
        self.custody_seen.update(row["name"] for row in rows)
        for owner in self.owners.values():
            published = self.sql_rows(
                "SELECT secret_name FROM curie.personal_claude_custody "
                "WHERE tenant_id = :'tenant'::uuid AND principal_id = :'principal'::uuid "
                "AND state = 'published'",
                {"tenant": owner.tenant_id, "principal": owner.principal_id},
            )
            owner.custody_secret = (
                str(published[0].get("secret_name") or "") if len(published) == 1 else ""
            )
        names = {row["name"] for row in rows}
        distinct = (
            len(rows) == 2
            and len({row["enrollment"] for row in rows}) == 2
            and all(row["generation"] == str(pps.OWNER_GENERATION) for row in rows)
            and all(owner.custody_secret in names for owner in self.owners.values())
            and len({owner.custody_secret for owner in self.owners.values()}) == 2
        )
        self.ledger.record("owners", "custody_secret_count", len(rows))
        self.ledger.record("owners", "custody_enrollments_distinct", distinct)
        if not distinct:
            raise StepFailed("the grant namespace does not hold one custody Secret per owner")

    def create_agent(self) -> None:
        repo = str(self.inputs.app["repo"])
        status, body = self.require_api().post(
            "/agents",
            {
                "name": self.agent,
                "repo_full_name": repo,
                "channel": {"kind": "github", "address": repo},
            },
        )
        if status != 201 or not isinstance(body, dict) or not body.get("id"):
            raise StepFailed(f"agent create returned {status}")
        self.agent_id = str(uuid.UUID(str(body["id"])))
        rows = self.sql_rows(
            "SELECT tenant_id::text AS tenant_id FROM curie.agents WHERE id = :'agent'::uuid",
            {"agent": self.agent_id},
        )
        self.agent_tenant = str(rows[0].get("tenant_id") or "") if len(rows) == 1 else ""

    def configure_agent(self) -> None:
        """Route, bundle, overrides, approve policy and operator principals (``pp`` setup)."""

        self.curie.run(
            "approvals",
            self.agent,
            "--route-resolution",
            f"{pps.PUBLICATION_ROUTE}={self.inputs.resolution_channel}",
            "--route-approvers",
            f"{pps.PUBLICATION_ROUTE}=users:{self.allowed_subject}",
        )
        self.curie.run(
            "deploy",
            "--plugin-dir",
            str(self.inputs.work_dir / "bundle"),
            "--agent",
            self.agent,
            "--env",
            "prod",
            "--chart",
            str(self.chart),
            timeout=900,
        )
        self.curie.run(
            "overrides",
            self.agent,
            "--reviewer-model",
            pps.REVIEWER_MODEL,
            "--model",
            pps.IMPLEMENTER_MODEL,
            "--execution-deadline",
            str(pto.EXECUTION_DEADLINE),
        )
        self.finalizer.register("close-run-pulls", self.close_run_pulls)
        self.finalizer.register("delete-run-branches", self.delete_run_branches)
        self.policy_set = True
        self.curie.run(
            "publication-policy",
            self.agent,
            "--policy",
            "approve",
            "--no-draft",
            "--branch-prefix",
            self.prefix,
        )
        self.allowed_token = self.mint_operator(self.allowed_subject)
        self.unlisted_token = self.mint_operator(self.unlisted_subject)
        routes = self.curie.json("approvals", self.agent, "--list-routes")
        self.ledger.record(
            "approval",
            "route_explicit_users",
            pps.route_bound_to(routes, pps.PUBLICATION_ROUTE, self.allowed_subject),
        )
        self.check_read_paths()

    # -- phase 6: forge ---------------------------------------------------------------

    def phase_forge(self) -> None:
        forge = self.require_forge()
        repo = forge.call("GET", forge.repo_path(""))
        self.repo_id = str(repo.get("id"))
        self.default_branch = str(repo.get("default_branch") or "")
        self.app_slug, self.bot_id = forge.app_identity()
        self.finalizer.register("close-owner-issues", self.close_owner_issues)
        for owner in self.owners.values():
            owner.nonce = f"{self.nonce}-{owner.name}"
            title, body = pto.render_issue(owner.name, owner.nonce)
            issue = forge.call(
                "POST", forge.repo_path("/issues"), {"title": title, "body": body}, ok=(201,)
            )
            number = issue.get("number") if isinstance(issue, dict) else None
            if not isinstance(number, int):
                raise StepFailed(f"owner {owner.name} tracker issue was not created")
            owner.issue_number = number
        self.ledger.record("forge", "owner_issues", len(self.owners))

    def close_owner_issues(self) -> None:
        """Close only the issues this run created (their numbers came from a 201)."""

        forge = self.require_forge()
        for owner in self.owners.values():
            if owner.issue_number is None:
                continue
            forge.call(
                "PATCH",
                forge.repo_path(f"/issues/{owner.issue_number}"),
                {"state": "closed", "state_reason": "completed"},
            )

    def issue_closed(self) -> bool:
        numbers = [owner.issue_number for owner in self.owners.values() if owner.issue_number]
        if not numbers:
            return True
        forge = self.require_forge()
        for number in numbers:
            issue = forge.call("GET", forge.repo_path(f"/issues/{number}"))
            if not isinstance(issue, dict) or issue.get("state") != "closed":
                return False
        return True

    # -- phase 6b: the drill (rehearsal only) ---------------------------------------------

    def phase_drill(self) -> None:
        try:
            super().phase_drill()
        finally:
            if self.rehearsal:
                self.record_drill_keys()

    def drill_publication(self, label: str) -> str:
        approval_id = super().drill_publication(label)
        self.cards.append(pps.ref_digest(approval_id))
        return approval_id

    def record_drill_keys(self) -> None:
        """The drill's evidence under ``pto.REHEARSAL_EXPECTED`` names."""

        approval = self.ledger.observations("approval")
        rehearsal = self.ledger.observations("rehearsal")
        delivered = all(
            isinstance(approval.get(f"drill_{label}_card"), dict)
            and approval[f"drill_{label}_card"].get("delivered") is True
            for label in ("reject", "approve")
        )
        unlisted = approval.get("cli_drill_unlisted")
        status = unlisted.get("http_status") if isinstance(unlisted, dict) else None
        self.ledger.record("rehearsal", "card_delivered", delivered)
        self.ledger.record(
            "rehearsal", "card_unlisted_status", status if type(status) is int else None
        )
        self.ledger.record(
            "rehearsal", "card_rejected_by_operator", rehearsal.get("drill_reject_resolved") is True
        )
        self.ledger.record(
            "rehearsal",
            "card_approved_by_operator",
            rehearsal.get("drill_approve_resolved") is True,
        )

    # -- the rehearsal controls --------------------------------------------------------------

    def phase_rehearsal_controls(self) -> None:
        """Unpaid: wrong-owner admission controls with the worker stopped, then disconnect.

        A, then a worker restart: A's new admission is refused and no A claim
        appears, while B stays active. Nothing here starts a native session:
        the worker is stopped while A's request waits, and A's request is
        cancelled before the worker returns.
        """

        if not self.rehearsal:
            raise StepFailed("the rehearsal controls run only in a rehearsal")
        a, b = self.owners["a"], self.owners["b"]
        before = self.stop_worker()
        work_item_id, request_id = self.admit_waiting(a)
        controls = self.wrong_owner_controls(a, b, work_item_id, request_id)
        self.cancel_work_item(work_item_id)
        disconnect = self.disconnect_owner(a)
        restart = self.restart_worker(before)
        after = self.refusals_after_disconnect(a, work_item_id)
        b_code, b_status = self.subscription(b, "status")
        b_active = (
            b_code == 0
            and b_status.get("state") == "active"
            and b_status.get("generation") == pps.OWNER_GENERATION
        )
        evidence = {**controls, **disconnect, **restart, **after, "b_active_gen1": b_active}
        for key, value in evidence.items():
            self.ledger.record("rehearsal", f"control_{key}", value)
        admission = all(
            controls[key]
            for key in (
                "b_on_a_issue_refused",
                "request_count_unchanged",
                "replay_owner_b_refused",
                "personal_context_b_hidden",
                "personal_context_a_readable",
                "no_request_started",
            )
        )
        refused = (
            disconnect["inactive"] is True
            and disconnect["generation_advanced"] is True
            and disconnect["pending_runtimes"] == 0
            and disconnect["pending_custody_cleanup"] == 0
            and disconnect["custody_secret_absent"] is True
            and restart["new_worker_uid"] is True
            and restart["candidate_image"] is True
            and after["admission_refused"] is True
            and after["no_claim_in_window"] is True
            and b_active
        )
        self.ledger.record("rehearsal", "admission_controls_refused", admission)
        self.ledger.record("rehearsal", "disconnect_refused_after_restart", refused)
        self.ledger.record("rehearsal", "no_grant_loaded", self.no_grant_loaded())
        failed = sorted(
            key
            for key, value in {
                "admission_controls_refused": admission,
                "disconnect_refused_after_restart": refused,
            }.items()
            if not value
        )
        if failed:
            raise StepFailed("rehearsal controls failed: " + ", ".join(failed))

    def no_grant_loaded(self) -> bool:
        """A setup descriptor names no grant, and every token the CLI read was a driver marker."""

        return (
            self.scenario.mode == "setup"
            and not self.scenario.owners
            and all(
                owner.token_kind == "marker"
                and owner.token_file is not None
                and owner.token_file.parent == self.owner_dir
                for owner in self.owners.values()
            )
        )

    # -- reusable controls (the live wrong-owner and disconnect phases reuse these) ---------

    def admission_body(
        self, owner: Owner, *, issue_owner: Owner, request_id: str, generation: int | None = None
    ) -> dict[str, Any]:
        """``AdmissionFacts`` (apps/api routers/work_items.py) on ``issue_owner``'s issue."""

        if issue_owner.issue_number is None:
            raise StepFailed(f"owner {issue_owner.name} has no tracker issue")
        repo = str(self.inputs.app["repo"])
        issue = issue_owner.issue_number
        return {
            "agent_id": self.agent_id,
            "kind": "github",
            "address": repo,
            "reply_conversation_id": f"issue-{issue}",
            "tracker": {
                "kind": "github",
                "host": "github.com",
                "scope_id": self.repo_id,
                "issue_id": str(issue),
            },
            "repository": {
                "code_host_kind": "github",
                "host": "github.com",
                "project_id": self.repo_id,
                "path": repo,
            },
            "code_host_installation_id": int(self.inputs.app["installation_id"]),
            "objective": f"https://github.com/{repo}/issues/{issue}",
            "requester": f"github:{self.bot_id}:{self.app_slug}[bot]",
            "request_id": request_id,
            "model_auth_mode": pps.PERSONAL_MODE,
            "owner": {
                "tenant_id": owner.tenant_id,
                "principal_id": owner.principal_id,
                "enrollment_generation": generation or pps.OWNER_GENERATION,
            },
        }

    def admit_owner(
        self, owner: Owner, *, issue_owner: Owner, request_id: str | None = None
    ) -> tuple[int, Any]:
        body = self.admission_body(
            owner, issue_owner=issue_owner, request_id=request_id or str(uuid.uuid4())
        )
        return self.internal("POST", "/v1/internal/work-items/admissions", body)

    @staticmethod
    def code_of(body: Any) -> Any:
        """``{"detail": {"code": ...}}`` (routers/work_items.py ``_raise_conflict``)."""

        detail = body.get("detail") if isinstance(body, dict) else None
        return detail.get("code") if isinstance(detail, dict) else None

    def work_item_of(self, work_item_id: str) -> dict[str, Any]:
        item = self.require_api().get(f"/work-items/{work_item_id}")
        if not isinstance(item, dict):
            raise StepFailed("work item read returned no object")
        return item

    def admit_waiting(self, owner: Owner) -> tuple[str, str]:
        """Admit one WorkItem for ``owner`` on its own issue; the worker must be stopped."""

        request_id = str(uuid.uuid4())
        status, body = self.admit_owner(owner, issue_owner=owner, request_id=request_id)
        work_item = body.get("work_item") if isinstance(body, dict) else None
        request = body.get("request") if isinstance(body, dict) else None
        if status != 200 or not isinstance(work_item, dict) or not isinstance(request, dict):
            raise StepFailed(f"owner {owner.name} admission returned {status}")
        work_item_id = str(uuid.UUID(str(work_item.get("id"))))
        owner.work_item_ids.append(work_item_id)
        owner.request_ids.append(request_id)
        owner.conversation_ids.append(str(work_item.get("conversation_id") or ""))
        return work_item_id, request_id

    def wrong_owner_controls(
        self, owner: Owner, other: Owner, work_item_id: str, request_id: str
    ) -> dict[str, bool]:
        """``other`` names itself on ``owner``'s issue, replays its request and reads its context.

        Expected answers (apps/api/src/curie_api/personal_execution.py
        ``existing_work_item_refusal`` then ``compare_owner``; routers/
        work_items.py): 409 ``{"detail": {"code": "owner_mismatch"}}`` for a new
        admission and for a replay, and 404 ``{"detail": {"code": "not_found"}}``
        for the owner-scoped personal-context read.
        """

        before = len(self.work_item_of(work_item_id).get("requests") or [])
        status, body = self.admit_owner(other, issue_owner=owner)
        foreign = status == 409 and self.code_of(body) == "owner_mismatch"
        after_item = self.work_item_of(work_item_id)
        after = len(after_item.get("requests") or [])
        replay_status, replay = self.admit_owner(other, issue_owner=owner, request_id=request_id)
        replayed = replay_status == 409 and self.code_of(replay) == "owner_mismatch"

        def context(reader: Owner) -> tuple[int, Any]:
            query = urllib.parse.urlencode(
                {
                    "tenant_id": reader.tenant_id,
                    "agent_id": self.agent_id,
                    "owner_principal_id": reader.principal_id,
                }
            )
            return self.internal(
                "GET", f"/v1/internal/work-items/{work_item_id}/personal-context?{query}"
            )

        hidden_status, hidden = context(other)
        own_status, own = context(owner)
        requests = [row for row in after_item.get("requests") or [] if isinstance(row, dict)]
        return {
            "b_on_a_issue_refused": foreign,
            "request_count_unchanged": before == after,
            "replay_owner_b_refused": replayed,
            "personal_context_b_hidden": hidden_status == 404
            and self.code_of(hidden) == "not_found",
            "personal_context_a_readable": own_status == 200 and isinstance(own, dict),
            "no_request_started": bool(requests)
            and all(not row.get("started_at") for row in requests),
            "owner_unchanged": pps.owner_matches(pps.owner_of(after_item), owner.tuple()),
        }

    def cancel_work_item(self, work_item_id: str) -> None:
        """``POST /v1/internal/work-items/{id}/cancel`` at the current version (#5378's route)."""

        rows = self.sql_rows(
            "SELECT version FROM curie.work_items WHERE id = :'work_item'::uuid",
            {"work_item": work_item_id},
        )
        version = rows[0].get("version") if len(rows) == 1 else None
        if type(version) is not int:
            raise StepFailed("the work item version could not be read")
        status, _body = self.internal(
            "POST", f"/v1/internal/work-items/{work_item_id}/cancel", {"expected_version": version}
        )
        if status != 200:
            raise StepFailed(f"work item cancel returned {status}")

        def settled() -> bool:
            requests = self.work_item_of(work_item_id).get("requests") or []
            return all(isinstance(row, dict) and row.get("terminal_at") for row in requests)

        self.wait_until(
            settled, until=self.clock.now() + REQUEST_SETTLE_SECONDS, what="cancelled request"
        )

    def disconnect_owner(self, owner: Owner) -> dict[str, Any]:
        """``curie cluster subscription disconnect`` with the owner's session, then teardown.

        Shape (cli/schema/subscription.schema.json): ``{"action", "changed",
        "previous_version", "state", "generation", "version", "teardown":
        {"pending_runtimes", "pending_custody_cleanup"}}``.
        """

        code, body = self.subscription(
            owner, "disconnect", "--expected-version", str(owner.version)
        )
        if code != 0:
            raise StepFailed(f"owner {owner.name} disconnect exited {code}")
        self.read_enrollment(owner, body)
        teardown: dict[str, Any] = dict(body.get("teardown") or {})

        def drained() -> bool:
            nonlocal teardown
            status_code, status = self.subscription(owner, "status")
            teardown = dict(status.get("teardown") or {}) if status_code == 0 else {}
            return (
                teardown.get("pending_runtimes") == 0
                and teardown.get("pending_custody_cleanup") == 0
            )

        self.wait_until(
            drained, until=self.clock.now() + DISCONNECT_WAIT_SECONDS, what="disconnect teardown"
        )
        names = {row["name"] for row in self.custody_secrets()}
        return {
            "inactive": owner.state == "inactive",
            "generation_advanced": owner.generation > pps.OWNER_GENERATION,
            "pending_runtimes": teardown.get("pending_runtimes"),
            "pending_custody_cleanup": teardown.get("pending_custody_cleanup"),
            "custody_secret_absent": bool(owner.custody_secret)
            and owner.custody_secret not in names,
        }

    def disconnect_if_active(self, owner: Owner) -> bool:
        """Disconnect ``owner`` when its status reads active; True when it is not active after."""

        code, status = self.subscription(owner, "status")
        if code != 0:
            raise StepFailed(f"owner {owner.name} status exited {code}")
        if status.get("state") != "active":
            owner.state = str(status.get("state") or "")
            return True
        version = status.get("version")
        if type(version) is not int:
            raise StepFailed(f"owner {owner.name} status names no version")
        code, body = self.subscription(owner, "disconnect", "--expected-version", str(version))
        if code != 0:
            raise StepFailed(f"owner {owner.name} disconnect exited {code}")
        self.read_enrollment(owner, body)
        return owner.state == "inactive"

    def stop_worker(self) -> list[str]:
        """Scale the worker to zero and wait until its pod is gone; the uids it had."""

        uids = [str((pod.get("metadata") or {}).get("uid") or "") for pod in self.worker_pods()]
        self.kube.ctl("scale", f"deployment/{self.fullname}-worker", "--replicas=0", timeout=120)
        self.kube.ctl(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            self.worker_selector(),
            f"--timeout={WORKER_GONE_SECONDS}s",
            timeout=WORKER_GONE_SECONDS + 20,
            check=False,
        )
        if self.worker_pods():
            raise StepFailed("the worker pod is still present after scaling to zero")
        return [uid for uid in uids if uid]

    def restart_worker(self, before: Sequence[str]) -> dict[str, bool]:
        """Scale the worker back to one: a new pod uid running the candidate worker image."""

        self.kube.ctl("scale", f"deployment/{self.fullname}-worker", "--replicas=1", timeout=120)
        self.kube.ctl(
            "rollout",
            "status",
            f"deployment/{self.fullname}-worker",
            "--timeout=600s",
            timeout=620,
        )
        uid, candidate = self.observe_worker()
        return {"new_worker_uid": bool(uid) and uid not in before, "candidate_image": candidate}

    def owner_claims(self, owner: Owner) -> list[str]:
        """The run agent's SandboxClaims labelled with ``owner``'s principal."""

        principal = str(uuid.UUID(owner.principal_id))
        listed = self.kube.json(
            "get",
            pp.CLAIM_RESOURCE,
            "-l",
            f"{pp.CLAIM_AGENT_LABEL}={self.agent},{OWNER_PRINCIPAL_LABEL}={principal}",
        )
        return self.claim_names(listed)

    def agent_claims(self) -> list[str]:
        listed = self.kube.json(
            "get", pp.CLAIM_RESOURCE, "-l", f"{pp.CLAIM_AGENT_LABEL}={self.agent}"
        )
        return self.claim_names(listed)

    def claim_names(self, listed: Any) -> list[str]:
        names = sorted(
            str((item.get("metadata") or {}).get("name") or "")
            for item in (listed.get("items") or [] if isinstance(listed, dict) else [])
            if isinstance(item, dict)
        )
        names = [name for name in names if name]
        self.claims_seen.update(names)
        return names

    def refusals_after_disconnect(self, owner: Owner, work_item_id: str) -> dict[str, bool]:
        """A new admission is 409 ``enrollment_inactive`` and no claim of the owner appears.

        ``_verify`` (apps/api personal_execution.py) answers
        ``enrollment_inactive`` for an inactive header before it compares the
        generation, and the existing WorkItem reaches it first.
        """

        status, body = self.admit_owner(owner, issue_owner=owner)
        refused = status == 409 and self.code_of(body) == "enrollment_inactive"
        until = self.clock.now() + OBSERVATION_SECONDS
        clean = True
        while True:
            if self.owner_claims(owner):
                clean = False
                break
            if self.clock.now() >= until:
                break
            self.clock.wait(pp.POLL_SECONDS)
        requests = self.work_item_of(work_item_id).get("requests") or []
        return {
            "admission_refused": refused,
            "no_claim_in_window": clean,
            "no_request_running": all(
                isinstance(row, dict) and row.get("status") != "running" for row in requests
            ),
        }

    # -- SQL and Valkey reads --------------------------------------------------------------

    def sql_rows(
        self, query: str, params: Mapping[str, uuid.UUID | int | str] | None = None
    ) -> list[dict[str, Any]]:
        """Rows of one SELECT as JSON objects, through ``psql`` in the release's Postgres pod.

        Parameters are psql variables (``-v name=value``) referenced as
        ``:'name'``, which psql quotes as a literal. Only validated UUIDs and
        integers are accepted; nothing is formatted into the query text.
        """

        if ";" in query or "\\" in query:
            raise StepFailed("an SQL read must be one statement without meta-commands")
        variables: list[str] = []
        for name, value in (params or {}).items():
            if not _SQL_NAME.fullmatch(name):
                raise StepFailed("an SQL parameter name is invalid")
            if isinstance(value, bool):
                raise StepFailed("an SQL parameter is neither a UUID nor an integer")
            if isinstance(value, int):
                text = str(value)
            elif isinstance(value, uuid.UUID):
                text = str(value)
            elif isinstance(value, str):
                try:
                    text = str(uuid.UUID(value))
                except ValueError:
                    raise StepFailed("an SQL parameter is neither a UUID nor an integer") from None
            else:
                raise StepFailed("an SQL parameter is neither a UUID nor an integer")
            variables += ["-v", f"{name}={text}"]
        script = f"SELECT coalesce(json_agg(row_to_json(q)), '[]'::json) FROM ({query}) AS q;\n"
        result = self.kube.ctl(
            "exec",
            "-i",
            f"{self.fullname}-postgres-0",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-X",
            "-q",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            *variables,
            "-f",
            "-",
            stdin=script.encode(),
            timeout=60,
        )
        try:
            rows = json.loads(result.stdout)
        except ValueError:
            raise StepFailed("psql returned unreadable JSON") from None
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise StepFailed("psql returned no JSON rows")
        return rows

    def require_valkey_password(self) -> pps.HeldSecret:
        if self.valkey_password is None or not self.valkey_password.reveal():
            secret = self.kube.json(
                "get", "secret", f"{self.fullname}-secrets", sensitive_output=True
            )
            try:
                value = base64.b64decode((secret.get("data") or {})["valkeyPassword"]).decode()
            except (KeyError, ValueError):
                raise StepFailed("release secrets lack the store password") from None
            self.valkey_password = self.scrubber.hold(pps.HeldSecret(value))
        return self.valkey_password

    def valkey_xrange(
        self, stream: str, start: str = "-", end: str = "+", *, count: int | None = None
    ) -> list[tuple[str, dict[str, str]]]:
        """``XRANGE`` on the release's Valkey; the password goes over stdin, never argv.

        ``valkey-cli --json`` answers ``[[id, [field, value, ...]], ...]``.
        """

        if not _STREAM_KEY.fullmatch(stream) or not all(
            _STREAM_ID.fullmatch(item) for item in (start, end)
        ):
            raise StepFailed("a stream key or id is invalid")
        extra: list[str] = []
        if count is not None:
            if type(count) is not int or count < 1:
                raise StepFailed("an XRANGE count is invalid")
            extra = ["COUNT", str(count)]
        password = self.require_valkey_password()
        result = self.kube.ctl(
            "exec",
            "-i",
            f"{self.fullname}-valkey-0",
            "-c",
            "valkey",
            "--",
            "sh",
            "-c",
            VALKEY_EXEC,
            "valkey-xrange",
            "XRANGE",
            stream,
            start,
            end,
            *extra,
            stdin=(password.reveal() + "\n").encode(),
            secret_stdin=True,
            timeout=60,
        )
        try:
            entries = json.loads(result.stdout or b"[]")
        except ValueError:
            raise StepFailed("valkey-cli returned unreadable JSON") from None
        parsed: list[tuple[str, dict[str, str]]] = []
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[0], str):
                raise StepFailed("valkey-cli returned an unreadable stream entry")
            fields = entry[1]
            if isinstance(fields, dict):
                parsed.append((entry[0], {str(k): str(v) for k, v in fields.items()}))
            elif isinstance(fields, list) and len(fields) % 2 == 0:
                pairs = zip(fields[0::2], fields[1::2], strict=True)
                parsed.append((entry[0], {str(k): str(v) for k, v in pairs}))
            else:
                raise StepFailed("valkey-cli returned an unreadable stream entry")
        return parsed

    # -- fences -----------------------------------------------------------------------------

    def fence_paid_starts(self) -> None:
        """No worker, no claim or runner pod of the run's agent, no active owner.

        There is no release-wide grant Secret in this scenario: each owner's
        grant reaches only its own ``<claim>-tokens``, so disconnecting the
        owner (which retires its custody Secret) and deleting the claims is the
        fence. Every protective step is issued before any wait.
        """

        failures: list[str] = []

        def attempt(name: str, call: Callable[[], Any]) -> Any:
            try:
                return call()
            except Exception:  # noqa: BLE001 - every protective step is attempted
                failures.append(name)
                return None

        attempt(
            "scale-worker",
            lambda: self.kube.ctl(
                "scale", f"deployment/{self.fullname}-worker", "--replicas=0", timeout=120
            ),
        )
        claims = attempt("list-claims", self.agent_claims) or []
        attempt("delete-claims", lambda: self.delete_claims(claims))
        attempt("delete-runner-pods", lambda: self.delete_runner_pods(claims))
        for owner in self.owners.values():
            if owner.may_be_active():
                step = functools.partial(self.disconnect_if_active, owner)
                if not attempt(f"disconnect-{owner.name}", step):
                    failures.append(f"owner-{owner.name}-active")
        for selector in (pp.POD_CLAIM_LABEL, self.worker_selector()):
            self.kube.ctl(
                "wait",
                "--for=delete",
                "pod",
                "-l",
                selector,
                "--timeout=120s",
                timeout=140,
                check=False,
            )
        runners = attempt(
            "list-runner-pods", lambda: self.kube.json("get", "pods", "-l", pp.POD_CLAIM_LABEL)
        )
        running = [
            pod
            for pod in ((runners or {}).get("items") or [])
            if any(
                (status.get("state") or {}).get("running")
                for status in (pod.get("status") or {}).get("containerStatuses") or []
            )
        ]
        fenced = not failures and runners is not None and not running
        self.ledger.record("budget", "paid_starts_fenced", fenced)
        self.ledger.record("budget", "fence_failures", sorted(set(failures)))
        if not fenced:
            raise StepFailed(
                "paid starts could not be fenced: "
                + ", ".join(sorted(set(failures)) or ["runner running"])
            )

    def fence_owner(self, owner: Owner) -> dict[str, Any]:
        """Disconnect one owner and delete only that owner's claims and runner pods."""

        inactive = self.disconnect_if_active(owner)
        claims = self.owner_claims(owner)
        self.delete_claims(claims)
        self.delete_runner_pods(claims)
        if claims:
            self.kube.ctl(
                "wait",
                "--for=delete",
                "pod",
                "-l",
                self.claim_pod_selector(claims),
                "--timeout=120s",
                timeout=140,
                check=False,
            )
        return {"inactive": inactive, "claims_deleted": len(claims)}

    @staticmethod
    def claim_pod_selector(claims: Sequence[str]) -> str:
        return f"{pp.POD_CLAIM_LABEL} in ({','.join(claims)})"

    def delete_claims(self, claims: Sequence[str]) -> None:
        if claims:
            self.kube.ctl(
                "delete",
                pp.CLAIM_RESOURCE,
                *claims,
                "--ignore-not-found",
                "--wait=false",
                timeout=120,
            )

    def delete_runner_pods(self, claims: Sequence[str]) -> None:
        if claims:
            self.kube.ctl(
                "delete",
                "pod",
                "-l",
                self.claim_pod_selector(claims),
                "--ignore-not-found",
                "--wait=false",
                timeout=120,
            )

    # -- finalize and inventory ----------------------------------------------------------

    def finalize(self) -> bool:
        if self.ledger.run_row()["live_started_at"] is not None:
            # Whatever is still open is charged at its share; the keys gathered
            # so far are recorded before the fence disconnects anyone.
            self.safe(self.charge_open, None)
            self.safe(self.record_summary_keys, None)
        for run in self.pods:
            if run.gate is not None and not run.closed:
                self.safe(run.gate.close, None)
                run.closed = True
        if self.mutated and (
            self.ledger.run_row()["live_started_at"] is not None
            or any(owner.may_be_active() for owner in self.owners.values())
        ):
            # Fence first: no owner stays enrolled and no runner keeps going while
            # the release is torn down.
            self.safe(self.fence_paid_starts, None)
        ok = super().finalize()
        if self.mutated:
            self.record_cleanup_keys()
        return ok

    def record_cleanup_keys(self) -> None:
        # Both owners' tokens (grants live, markers in a rehearsal) and both
        # session cookies were held by the scrubber the leak scan reads, so their
        # fingerprints were searched on every channel.
        scrubbed = self.scrubber.held()
        held = all(
            item is not None and item.held and any(item is entry for entry in scrubbed)
            for owner in self.owners.values()
            for item in (owner.token, owner.session)
        )
        kinds = {owner.token_kind for owner in self.owners.values()}
        self.ledger.record("cleanup", "token_scan_both_grants", held)
        self.ledger.record("cleanup", "token_scan_subject", sorted(kinds))
        self.ledger.record(
            "cleanup",
            "card_residue",
            {
                "cards": len(self.cards),
                "channel": pps.ref_digest(self.inputs.resolution_channel),
                "removed": False,
            },
        )
        self.ledger.record("cleanup", "card_residue_recorded", True)
        statuses = {row["kind"]: row["status"] for row in self.ledger.cleanup_rows()}
        guarded = [
            statuses.get(f"step:{name}", "ok")
            for name in ("close-owner-issues", "close-run-pulls", "delete-run-branches")
        ]
        self.ledger.record(
            "cleanup", "forge_removal_guarded", all(item == "ok" for item in guarded)
        )

    def query_inventory(self) -> dict[str, str]:
        results = super().query_inventory()
        checks: dict[str, Callable[[], bool]] = {
            "grant_namespace": self.grant_namespace_absent,
            "custody_secrets": lambda: not self.custody_secrets(),
            "claims": self.claims_absent,
            "claim_tokens": self.claim_tokens_absent,
            "dex": self.dex_absent,
            "dex_source_image": self.dex_source_absent,
            "owner_files": lambda: not self.owner_dir.exists(),
            "port_forwards": lambda: all(process.poll() is not None for process in self.forwards),
        }
        for kind, check in checks.items():
            try:
                results[kind] = "absent" if check() else "present"
            except Exception:  # noqa: BLE001 - an unanswered query is a failure
                results[kind] = "uncertain"
        return results

    def grant_namespace_absent(self) -> bool:
        result = self.kube.ctl(
            "get",
            "namespace",
            self.grant_namespace,
            "--ignore-not-found",
            "-o",
            "name",
            namespaced=False,
        )
        return not result.stdout.strip()

    def claims_absent(self) -> bool:
        crd = self.kube.ctl(
            "get", "crd", CLAIM_CRD, "--ignore-not-found", "-o", "name", namespaced=False
        )
        if not crd.stdout.strip():
            return True
        listed = self.kube.ctl(
            "get", pp.CLAIM_RESOURCE, "-l", f"{pp.CLAIM_AGENT_LABEL}={self.agent}", "-o", "name"
        )
        return not listed.stdout.strip()

    def claim_tokens_absent(self) -> bool:
        for claim in sorted(self.claims_seen):
            found = self.kube.ctl(
                "get",
                "secret",
                f"{claim}{pto.CLAIM_TOKENS_SUFFIX}",
                "--ignore-not-found",
                "-o",
                "name",
            )
            if found.stdout.strip():
                return False
        return True

    def dex_absent(self) -> bool:
        found = self.kube.ctl(
            "get",
            "deployment,service,secret",
            "-l",
            DEX_SELECTOR,
            "--ignore-not-found",
            "-o",
            "name",
        )
        return not found.stdout.strip()

    def dex_source_absent(self) -> bool:
        if not dex_source_owned(self.ledger):
            return True
        inspect = ["docker", "image", "inspect", "--format", "{{.Id}}", DEX_SOURCE]
        return self.shell.run(inspect, check=False, timeout=60).returncode != 0


# --------------------------------------------------------------------------
# Entry
# --------------------------------------------------------------------------

COMMANDS: dict[str, Callable[[argparse.Namespace, pp.Runtime], int]] = {
    "images": cmd_images,
    "preflight": cmd_preflight,
    "forge-probe": cmd_forge_probe,
    "grant-probe": cmd_grant_probe,
    "rehearse": cmd_rehearse,
    "run": cmd_run,
    "judge": cmd_judge,
}


def run_command(name: str, args: argparse.Namespace, runtime: pp.Runtime) -> int:
    """One ``--scenario two-owner`` subcommand. Single-profile flags are refused."""

    supplied = sorted(getattr(args, "supplied", frozenset()) or ())
    if supplied:
        flag = "--" + supplied[0].replace("_", "-")
        raise pps.Refusal(
            "arguments_invalid", f"{flag} is a single-profile flag; two-owner reads --inputs-file"
        )
    handler = COMMANDS.get(name)
    if handler is None:
        raise pps.Refusal("arguments_invalid", f"{name} is not a two-owner command")
    return handler(args, runtime)
