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

The paid phases (probes, attempts, response gating, CI repair, interruptions,
the disconnect of an active runtime) are named in ``PAID_PHASES``. In this
build each one refuses with ``StepFailed("not implemented in this build")``;
a rehearsal never reaches them.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import functools
import hashlib
import http.client
import json
import os
import re
import secrets
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal, Protocol

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

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
#: The paid phases, in the live timeline's order. Filled by a later build.
PAID_PHASES = (
    "probe-a",
    "attempt-a1",
    "probe-b",
    "attempt-b1",
    "wrong-owner",
    "reject-a1",
    "attempt-a2",
    "approve-b1",
    "worker-restart",
    "repair-b-r1",
    "attempt-a3",
    "disconnect-a",
    "process-interruption",
    "approve-b2",
    "post-disconnect",
)
NOT_IMPLEMENTED = "not implemented in this build"

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
        """The live timeline after forge, in ``PAID_PHASES`` order (extension points)."""

        methods: dict[str, Callable[[], None]] = {
            "probe-a": self.phase_probe_a,
            "attempt-a1": self.phase_attempt_a1,
            "probe-b": self.phase_probe_b,
            "attempt-b1": self.phase_attempt_b1,
            "wrong-owner": self.phase_wrong_owner,
            "reject-a1": self.phase_reject_a1,
            "attempt-a2": self.phase_attempt_a2,
            "approve-b1": self.phase_approve_b1,
            "worker-restart": self.phase_worker_restart,
            "repair-b-r1": self.phase_repair_b_r1,
            "attempt-a3": self.phase_attempt_a3,
            "disconnect-a": self.phase_disconnect_a,
            "process-interruption": self.phase_process_interruption,
            "approve-b2": self.phase_approve_b2,
            "post-disconnect": self.phase_post_disconnect,
        }
        return [(name, methods[name]) for name in PAID_PHASES]

    def paid(self, name: str) -> None:
        if self.rehearsal:
            raise pps.Refusal("rehearsal_run_id", f"{name} is a paid phase")
        raise StepFailed(f"{name}: {NOT_IMPLEMENTED}")

    def phase_probe_a(self) -> None:
        self.paid("probe-a")

    def phase_attempt_a1(self) -> None:
        self.paid("attempt-a1")

    def phase_probe_b(self) -> None:
        self.paid("probe-b")

    def phase_attempt_b1(self) -> None:
        self.paid("attempt-b1")

    def phase_wrong_owner(self) -> None:
        self.paid("wrong-owner")

    def phase_reject_a1(self) -> None:
        self.paid("reject-a1")

    def phase_attempt_a2(self) -> None:
        self.paid("attempt-a2")

    def phase_approve_b1(self) -> None:
        self.paid("approve-b1")

    def phase_worker_restart(self) -> None:
        self.paid("worker-restart")

    def phase_repair_b_r1(self) -> None:
        self.paid("repair-b-r1")

    def phase_attempt_a3(self) -> None:
        self.paid("attempt-a3")

    def phase_disconnect_a(self) -> None:
        self.paid("disconnect-a")

    def phase_process_interruption(self) -> None:
        self.paid("process-interruption")

    def phase_approve_b2(self) -> None:
        self.paid("approve-b2")

    def phase_post_disconnect(self) -> None:
        self.paid("post-disconnect")

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
