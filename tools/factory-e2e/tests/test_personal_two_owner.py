"""Hermetic contract for the two-owner live driver (#5379).

The live scenario needs a throwaway cluster, a private Dex, two enrolled
owners and an owned forge, so it is proved by ``personal_profile.py --scenario
two-owner rehearse`` and ``run``. These tests pin the driver around it: every
command-path refusal through the real ``main`` with an authorized control, the
judge reading receipts only, the rehearsal's phase order and its cut before
any paid phase, the install's ordering and values, the enrollment argv, the
fences' selectors, the rehearsal controls against replayed API answers, and
the SQL and stream reads.

The fakes replay real shapes, cited where they are built: kubectl and helm
JSON, ``curie --json cluster subscription`` objects, the platform API's
admission, cancel and personal-context answers, and Dex redirects. Nothing
here touches a cluster, GitHub, Docker or a network beyond a loopback
registry fixture, and no value is a real credential or identifier.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import hashlib
import io
import json
import tarfile
import urllib.parse
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import personal_owner_enforcement_support as poes
import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
import personal_two_owner_support as pto
import pytest
from two_owner_shared import (
    AGENT_ID,
    API_PORT,
    CANDIDATE,
    CONTEXT,
    DEX_DIGEST,
    DEX_PORT,
    DIGESTS,
    FULLNAME,
    MARKER_A,
    MARKER_B,
    NAMESPACE,
    PATH_ONLY,
    PRINCIPAL_A,
    PRINCIPAL_B,
    REGISTRY,
    RELEASE,
    REPO_ROOT,
    RUN_ID,
    SESSION_MARKER,
    STATE_MARKER,
    T0,
    TAG,
    TENANT,
    WINDOW,
    WORK_ITEM,
    _FakeClock,
    _image,
    _inputs_file,
    _kit,
    _private,
    _registry,
    _setup_document,
)


def _runtime(clock: pp.Clock | None = None) -> pp.Runtime:
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, pps.SCAN_CHANNELS)
    return pp.Runtime(
        dict(PATH_ONLY), pp.Output(scrubber, scan), scrubber, scan, clock or _FakeClock()
    )


# --------------------------------------------------------------------------
# Fakes: a recording Shell, a replaying API, a scripted Dex
# --------------------------------------------------------------------------


@dataclasses.dataclass
class _Call:
    argv: list[str]
    stdin: bytes | None
    secret_stdin: bool
    sensitive_output: bool


Answer = Callable[
    [list[str], bytes | None], "pp.Completed | bytes | str | dict[str, Any] | list[Any]"
]


def _completed(value: Any) -> pp.Completed:
    if isinstance(value, pp.Completed):
        return value
    if isinstance(value, bytes):
        return pp.Completed(0, value, b"")
    if isinstance(value, str):
        return pp.Completed(0, value.encode(), b"")
    return pp.Completed(0, json.dumps(value).encode(), b"")


class _FakeShell(pp.Shell):
    """``pp.Shell`` with the real held-secret guard and scripted children.

    Every argv is recorded after the guard accepts it. ``on(match, answer)``
    answers the first argv whose tokens include every ``match`` token in
    order; anything else exits 0, with ``{"items": []}`` for ``-o json``.
    With ``log`` set, each call goes to the command log as ``pp.Shell.run``
    writes it.
    """

    def __init__(self, runtime: pp.Runtime, cwd: Path, events: list[str] | None = None) -> None:
        super().__init__(env=dict(PATH_ONLY), scrubber=runtime.scrubber, scan=runtime.scan, cwd=cwd)
        self.calls: list[_Call] = []
        self.handlers: list[tuple[tuple[str, ...], Answer]] = []
        self.events = events if events is not None else []
        self.ports = iter(range(43001, 43100))

    def on(self, *match: str) -> Callable[[Answer], Answer]:
        def register(answer: Answer) -> Answer:
            self.handlers.append((match, answer))
            return answer

        return register

    @staticmethod
    def matches(argv: Sequence[str], match: Sequence[str]) -> bool:
        position = 0
        for token in argv:
            if position < len(match) and token == match[position]:
                position += 1
        return position == len(match)

    def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        timeout: float = 120.0,
        check: bool = True,
        extra_env: Mapping[str, str] | None = None,
        secret_env: Sequence[str] = (),
        secret_stdin: bool = False,
        sensitive_output: bool = False,
        full_output: bool = False,
        record_output: bool = True,
        clean_env: bool = False,
        cwd: Path | None = None,
    ) -> pp.Completed:
        self.guard(
            argv,
            stdin=stdin,
            extra_env=extra_env,
            secret_env=secret_env,
            secret_stdin=secret_stdin,
            clean_env=clean_env,
        )
        call = _Call(list(argv), stdin, secret_stdin, sensitive_output)
        self.calls.append(call)
        result = pp.Completed(0, b"", b"")
        for match, answer in self.handlers:
            if self.matches(call.argv, match):
                result = _completed(answer(call.argv, stdin))
                break
        else:
            if "-o" in call.argv and call.argv[call.argv.index("-o") + 1 :][:1] == ["json"]:
                result = pp.Completed(0, b'{"items": []}', b"")
        if not sensitive_output:
            self.scan.scan("children", result.stdout)
        if record_output:
            self.record(call.argv, result, sensitive=sensitive_output, full=full_output)
        if check and result.returncode != 0:
            raise pp.StepFailed(f"{Path(call.argv[0]).name} exited {result.returncode}")
        return result

    def spawn(self, argv: Sequence[str]) -> Any:
        """``kubectl port-forward`` printing its real first line."""

        self.guard(argv)
        self.calls.append(_Call(list(argv), None, False, False))
        return _FakeProcess(next(self.ports))

    def kubectl(self) -> list[list[str]]:
        return [call.argv for call in self.calls if call.argv[0] == "kubectl"]

    def curie(self, *verbs: str) -> list[list[str]]:
        return [
            call.argv
            for call in self.calls
            if "cluster" in call.argv and self.matches(call.argv, verbs)
        ]


class _FakeProcess:
    def __init__(self, port: int) -> None:
        # kubectl port-forward's first stdout line (kubectl 1.30).
        self.stdout = io.BytesIO(f"Forwarding from 127.0.0.1:{port} -> 8000\n".encode())
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int | None:
        return self.returncode


Route = Callable[[str, Mapping[str, str], Any], tuple[int, Any]]


class _FakeApi:
    """``pp.JsonHttp.request`` replaying the platform API's recorded answer shapes."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.routes: list[tuple[str, str, Route]] = []
        self.requests: list[tuple[str, str, dict[str, str], Any]] = []
        self.events = events if events is not None else []

    def route(self, method: str, prefix: str) -> Callable[[Route], Route]:
        def register(answer: Route) -> Route:
            self.routes.append((method, prefix, answer))
            return answer

        return register

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        sensitive: bool = False,
    ) -> tuple[int, Any]:
        self.requests.append((method, path, dict(headers or {}), body))
        for expected, prefix, answer in self.routes:
            if method == expected and path.startswith(prefix):
                return answer(path, headers or {}, body)
        return 404, {"detail": {"code": "not_found"}}


Response = tuple[int, list[tuple[str, str]], bytes]


class _ScriptedOpener:
    """Replays Dex and API redirects (the shapes test_personal_two_owner_support pins)."""

    def __init__(self, script: list[tuple[str, str, Callable[..., Response]]]) -> None:
        self.script = list(script)
        self.requests: list[tuple[str, str, dict[str, str]]] = []

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> Response:
        self.requests.append((method, url, dict(headers)))
        expected_method, expected_url, answer = self.script.pop(0)
        assert (method, url) == (expected_method, expected_url)
        return answer(headers, body)


@dataclasses.dataclass
class _Harness:
    run: pt.TwoOwnerRun
    ledger: pto.TwoOwnerLedger
    runtime: pp.Runtime
    shell: _FakeShell
    api: _FakeApi
    clock: _FakeClock
    events: list[str]


def _harness(
    tmp_path: Path,
    *,
    rehearsal: bool = True,
    opener: pto.Opener | None = None,
    stop_after: str | None = None,
    cls: type[pt.TwoOwnerRun] = pt.TwoOwnerRun,
) -> _Harness:
    clock = _FakeClock()
    runtime = _runtime(clock)
    events: list[str] = []
    document = _setup_document(tmp_path) if rehearsal else _kit(tmp_path)
    scenario = pto.load_inputs_file(
        _inputs_file(tmp_path, document), mode="setup" if rehearsal else "live", now=T0
    )
    state = _private(tmp_path / "state")
    inputs = pt.inputs_for(pp.Inputs(candidate=CANDIDATE, run_id=RUN_ID, state_dir=state), scenario)
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID, scrubber=runtime.scrubber)
    ledger.ensure_run(CANDIDATE, phase="rehearse" if rehearsal else "run")
    shell = _FakeShell(runtime, tmp_path, events)
    api = _FakeApi(events)
    run = cls(
        inputs=inputs,
        scenario=scenario,
        runtime=runtime,
        ledger=ledger,
        shell=shell,
        resolver=lambda _host: ["192.0.2.10"],
        http_factory=lambda _port: api,
        opener=opener,
        rehearsal=rehearsal,
        stop_after=stop_after,
    )
    return _Harness(run, ledger, runtime, shell, api, clock, events)


def _connect(h: _Harness) -> None:
    """The state phase_setup leaves: API client, ports, owner files and identities."""

    run = h.run
    run.api_port, run.dex_port = API_PORT, DEX_PORT
    run.api = pp.CurieApi(
        h.api,  # type: ignore[arg-type]
        h.runtime.scrubber.hold(pps.HeldSecret("api-key-marker-0001")),
        h.runtime.scrubber.hold(pps.HeldSecret("worker-token-marker-0001")),
    )
    pp.private_dir(pp.private_dir(run.inputs.work_dir) / "owners")
    run.agent_id, run.repo_id, run.app_slug, run.bot_id = AGENT_ID, "123456", "acme-factory", 77
    run.digests = dict(DIGESTS)
    for owner, principal, issue in (("a", PRINCIPAL_A, 11), ("b", PRINCIPAL_B, 12)):
        current = run.owners[owner]
        current.principal_id, current.tenant_id = principal, TENANT
        current.issue_number, current.state, current.version, current.generation = (
            issue,
            "active",
            1,
            1,
        )
        current.session = h.runtime.scrubber.hold(pps.HeldSecret(f"session-marker-{owner}"))
        current.session_file = run.owner_dir / f"session-{owner}"
        pp.write_private(current.session_file, f"session-marker-{owner}\n".encode())
        current.custody_secret = f"curie-pe-{owner * 8}-g1"
        run.prepare_token(current)


# --------------------------------------------------------------------------
# Phases: order, cut and paid phases
# --------------------------------------------------------------------------


def test_rehearsal_runs_every_unpaid_phase_then_the_controls_and_stops(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    names = [name for name, _phase in h.run.phases()]
    assert names == [*pt.UNPAID_PHASES, *pt.REHEARSAL_ONLY_PHASES]
    assert h.run.stop_after == pt.REHEARSAL_CUT == names[-1]
    assert h.run.rehearsal and h.run.command == "rehearse" and h.ledger.is_rehearsal()
    assert not set(names) & set(pt.PAID_PHASES)
    with pytest.raises(pps.Refusal) as caught:
        h.run.phase_probe_a()
    assert caught.value.code == "rehearsal_run_id"


def test_rehearsal_execute_follows_the_phase_order_then_fences(tmp_path: Path) -> None:
    order: list[str] = []

    class _Recorded(pt.TwoOwnerRun):
        def phases(self) -> list[tuple[str, Callable[[], None]]]:
            return [(name, _bind(order.append, name)) for name, _ in super().phases()]

        def fence_paid_starts(self) -> None:
            order.append("fence")

        def finalize(self) -> bool:
            order.append("finalize")
            return True

    h = _harness(tmp_path, cls=_Recorded)
    assert h.run.execute() == 0
    assert order == [*pt.UNPAID_PHASES, *pt.REHEARSAL_ONLY_PHASES, "fence", "finalize"]
    # execute closes the ledger; the receipt is read back as judge would.
    ledger = pto.TwoOwnerLedger.open_existing(h.run.inputs.state_dir, RUN_ID)
    try:
        assert ledger.observations("rehearsal")["fence_exercised"] is True
        assert ledger.run_row()["live_started_at"] is None
    finally:
        ledger.close()


def _bind(call: Callable[[str], None], name: str) -> Callable[[], None]:
    return lambda: call(name)


def test_live_run_takes_the_paid_phases_after_forge(tmp_path: Path) -> None:
    h = _harness(tmp_path, rehearsal=False)
    names = [name for name, _phase in h.run.phases()]
    assert names == [*pt.UNPAID_PHASES, *pt.PAID_PHASES]
    assert h.run.stop_after is None and not h.run.rehearsal and h.run.command == "run"
    assert not h.ledger.is_rehearsal()
    paid = dict(h.run.phases()[len(pt.UNPAID_PHASES) :])
    for name in pt.PAID_PHASES:
        method = getattr(pt.TwoOwnerRun, "phase_" + name.replace("-", "_"))
        assert getattr(paid[name], "__func__", None) is method, name
    # A paid phase does nothing before its first reservation without the live window.
    assert h.ledger.session_rows() == []


def test_a_rehearsal_cannot_be_cut_at_or_after_a_paid_phase(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="before every paid phase"):
        _harness(tmp_path / "paid-cut", stop_after="probe-a")

    class _PaidFirst(pt.TwoOwnerRun):
        def phases(self) -> list[tuple[str, Callable[[], None]]]:
            phases = super().phases()
            return [phases[0], ("probe-a", self.phase_probe_a), *phases[1:]]

    with pytest.raises(ValueError, match="before every paid phase"):
        _harness(tmp_path / "reordered", cls=_PaidFirst)
    with pytest.raises(ValueError, match="no phase cut"):
        _harness(tmp_path / "live-cut", rehearsal=False, stop_after="forge")
    # Control: an earlier unpaid cut is accepted.
    assert _harness(tmp_path / "early", stop_after="install").run.stop_after == "install"


def test_descriptor_mode_must_match_the_run(tmp_path: Path) -> None:
    runtime = _runtime()
    state = _private(tmp_path / "state")
    setup = pto.load_inputs_file(
        _inputs_file(tmp_path / "s", _setup_document(tmp_path / "s")), mode="setup", now=T0
    )
    live = pto.load_inputs_file(
        _inputs_file(tmp_path / "l", _kit(tmp_path / "l")), mode="live", now=T0
    )
    for scenario, rehearsal in ((setup, False), (live, True)):
        ledger = pto.TwoOwnerLedger.open(state, RUN_ID, scrubber=runtime.scrubber)
        ledger.ensure_run(CANDIDATE, phase="run")
        inputs = pt.inputs_for(pp.Inputs(CANDIDATE, RUN_ID, state), scenario)
        with pytest.raises(ValueError, match="setup descriptor"):
            pt.TwoOwnerRun(
                inputs=inputs,
                scenario=scenario,
                runtime=runtime,
                ledger=ledger,
                shell=_FakeShell(runtime, tmp_path),
                rehearsal=rehearsal,
            )
        ledger.close()


# --------------------------------------------------------------------------
# images: the gated bundle and the mirrored Dex
# --------------------------------------------------------------------------


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    (bundle / "hooks").mkdir(parents=True)
    source = REPO_ROOT / pp.BUNDLE_SOURCE / "hooks" / "hooks.json"
    hooks = pps.private_bundle_hooks(json.loads(source.read_text()))
    (bundle / "hooks" / "hooks.json").write_text(json.dumps(hooks))
    return bundle


def test_patch_bundle_merges_the_gate_once(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    assert not pt.bundle_gated(bundle)
    pt.patch_bundle(bundle)
    first = (bundle / "hooks" / "hooks.json").read_text()
    pt.patch_bundle(bundle)
    assert (bundle / "hooks" / "hooks.json").read_text() == first
    assert pt.bundle_gated(bundle)
    assert (bundle / "hooks" / pto.GATE_FILE).read_text() == pto.GATE_HOOK_PROGRAM
    (bundle / "hooks" / pto.GATE_FILE).write_text(pto.GATE_HOOK_PROGRAM + "# edited\n")
    assert not pt.bundle_gated(bundle)


@pytest.mark.parametrize("present_before", [False, True])
def test_mirror_dex_records_the_registry_digest_and_owns_only_its_pull(
    tmp_path: Path, present_before: bool
) -> None:
    runtime = _runtime()
    dex = _image("dex-upstream")
    digest = "sha256:" + hashlib.sha256(dex[0]).hexdigest()
    with _registry({(pt.DEX_REPOSITORY, TAG): dex, (pt.DEX_REPOSITORY, digest): dex}) as port:
        state = _private(tmp_path / "state")
        inputs = pp.Inputs(
            CANDIDATE, RUN_ID, state, registry=f"localhost:{port}", registry_port=port
        )
        ledger = pto.TwoOwnerLedger.open(state, RUN_ID, scrubber=runtime.scrubber)
        ledger.ensure_run(CANDIDATE, phase="images")
        ledger.record("images", "platform", "linux/amd64")
        shell = _FakeShell(runtime, tmp_path)
        local = {"present": present_before}

        @shell.on("docker", "pull")
        def pull(_argv: list[str], _stdin: bytes | None) -> bytes:
            local["present"] = True
            return b""

        shell.on("docker", "image", "inspect")(
            lambda _a, _s: pp.Completed(0 if local["present"] else 1, b"", b"")
        )
        record = pt.mirror_dex(shell, ledger, inputs)
        assert record["digest"] == digest
        assert pt.verify_dex(inputs, ledger) == f"localhost:{port}/{pt.DEX_REPOSITORY}@{digest}"
        images = ledger.observations("images")
        assert {
            "name": "dex",
            "repository": pt.DEX_REPOSITORY,
            "reference": TAG,
            "digest": digest,
        } in images["pushed"]
        assert f"localhost:{port}/{pt.DEX_REPOSITORY}:{TAG}" in images["local_tags"]
        pt.remove_dex_source(shell, ledger)
        removed = [call.argv for call in shell.calls if call.argv[:3] == ["docker", "image", "rm"]]
        assert removed == ([] if present_before else [["docker", "image", "rm", pt.DEX_SOURCE]])
        pulls = [call.argv for call in shell.calls if call.argv[:2] == ["docker", "pull"]]
        assert pulls == [["docker", "pull", "--platform", "linux/amd64", pt.DEX_SOURCE]]
        ledger.close()


# --------------------------------------------------------------------------
# install: Dex before helm, no runner grant in the values
# --------------------------------------------------------------------------


def _pod(
    name: str, component: str, repository: str, digest: str, *, uid: str = "", hash_: str = "h1"
) -> dict[str, Any]:
    """``kubectl get pods -o json`` item: spec image plus the runtime-reported imageID."""

    image = f"{REGISTRY}/{repository}@{digest}"
    return {
        "metadata": {
            "name": name,
            "uid": uid or f"uid-{name}",
            "labels": {
                "app.kubernetes.io/component": component,
                "app.kubernetes.io/instance": RELEASE,
                pt.TEMPLATE_HASH_LABEL: hash_,
            },
        },
        "spec": {"containers": [{"name": component, "image": image}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": component, "imageID": f"{REGISTRY}/{repository}@{digest}"}
            ],
        },
    }


def _install_shell(
    h: _Harness, *, rendered_extra: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """kubectl and helm answers for phase_install (shapes: kubectl 1.30, helm 3.15 -o json)."""

    seen: dict[str, Any] = {}
    shell = h.shell

    @shell.on("git", "archive")
    def archive(argv: list[str], _stdin: bytes | None) -> bytes:
        out = Path(argv[argv.index("-o") + 1])
        with tarfile.open(out, "w") as tar:
            chart = b"apiVersion: v2\nname: curie\nversion: 0.0.0\n"
            info = tarfile.TarInfo("charts/curie/Chart.yaml")
            info.size = len(chart)
            tar.addfile(info, io.BytesIO(chart))
        return b""

    @shell.on("helm", "install")
    def install(argv: list[str], _stdin: bytes | None) -> bytes:
        seen["values"] = json.loads(Path(argv[argv.index("-f") + 1]).read_text())
        seen["helm_index"] = len(shell.calls) - 1
        return b""

    @shell.on("helm", "get", "values")
    def values(_argv: list[str], _stdin: bytes | None) -> dict[str, Any]:
        rendered: dict[str, Any] = json.loads(json.dumps(seen["values"]))
        if rendered_extra:
            rendered["worker"].setdefault("extraEnv", []).append(dict(rendered_extra))
        return rendered

    api_pod = _pod("api-0", "api", "curie-api", DIGESTS["curie-api"])
    pods = [
        api_pod,
        _pod("worker-0", "worker", "curie-worker", DIGESTS["curie-worker"]),
        _pod("gateway-0", "channel-gateway", "curie-dispatcher", DIGESTS["curie-dispatcher"]),
        _pod("dex-0", "dex", pt.DEX_REPOSITORY, DEX_DIGEST),
    ]
    shell.on("get", "pods", "-l", h.run.api_selector(), "-o", "json")(
        lambda _a, _s: {"items": [api_pod]}
    )
    shell.on("get", "pods", "-o", "json")(lambda _a, _s: {"items": pods})
    shell.on("get", "secrets", "-o", "json")(
        lambda _a, _s: {
            "items": [
                {
                    "metadata": {"name": f"{FULLNAME}-secrets", "labels": {}},
                    "data": {"apiKey": "bWFya2Vy", "internalWorkerToken": "bWFya2Vy"},
                }
            ]
        }
    )
    return seen


def test_install_applies_dex_before_helm_with_no_runner_grant(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    h.run.digests = dict(DIGESTS)
    h.run.dex_image = f"{REGISTRY}/{pt.DEX_REPOSITORY}@{DEX_DIGEST}"
    seen = _install_shell(h)
    h.run.phase_install()
    created = [
        (index, json.loads(call.stdin or b"{}"), call)
        for index, call in enumerate(h.shell.calls)
        if call.argv[-3:] == ["create", "-f", "-"]
    ]
    kinds = {(doc["kind"], doc["metadata"]["name"]): index for index, doc, _call in created}
    assert kinds[("Deployment", pt.DEX_NAME)] < seen["helm_index"]
    assert kinds[("Secret", pt.OIDC_SECRET)] < kinds[("Secret", pt.DEX_CONFIG_SECRET)]
    # Secrets travel only on stdin, flagged so, and their output is never scanned or logged.
    for _index, doc, call in created:
        if doc["kind"] == "Secret":
            assert call.secret_stdin and call.sensitive_output
    client = next(doc for _i, doc, _c in created if doc["metadata"]["name"] == pt.OIDC_SECRET)
    client_value = client["data"][pt.OIDC_SECRET_KEY]
    rendered = json.dumps(seen["values"])
    assert pps.PERSONAL_GRANT_ENV not in rendered
    assert pto.installation_grant_findings([seen["values"]]) == []
    assert client_value not in rendered
    assert all(client_value not in " ".join(call.argv) for call in h.shell.calls)
    assert all(argv[1:3] == ["--context", CONTEXT] for argv in h.shell.kubectl())
    helm = [call.argv for call in h.shell.calls if call.argv[0] == "helm"]
    assert helm and all(argv[1:3] == ["--kube-context", CONTEXT] for argv in helm)
    owners = h.ledger.observations("owners")
    assert owners["installation_grant_absent"] is True
    assert owners["service_credentials_absent"] is True
    assert owners["platform_images_candidate"] is True
    assert owners["worker_images_candidate"] is True
    assert h.ledger.observations("rehearsal")["install_no_runner_grant"] is True
    assert h.ledger.observations("install")["api_single_template"] is True
    assert h.ledger.observations("install")["dex_image_running"] is True


def test_stalled_rollout_records_the_log_tail_of_each_restarting_container(
    tmp_path: Path,
) -> None:
    h = _harness(tmp_path)
    secret = "s3cr3t-marker-value"
    h.run.scrubber.hold(pps.HeldSecret(secret))
    crashed = {
        "metadata": {"name": "dex-0"},
        "status": {
            "containerStatuses": [
                {
                    "name": "dex",
                    "restartCount": 4,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                },
                {"name": "side", "restartCount": 0, "state": {"running": {}}},
            ]
        },
    }
    fresh = {
        "metadata": {"name": "api-0"},
        "status": {
            "containerStatuses": [
                {"name": "api", "restartCount": 1, "state": {"running": {}}},
                {"name": "gone", "restartCount": 2, "state": {"running": {}}},
            ]
        },
    }
    h.shell.on("get", "pods", "-o", "json")(lambda _a, _s: {"items": [crashed, fresh]})
    h.shell.on("logs", "dex-0", "--previous")(
        lambda _a, _s: ("boom\n" * 5 + secret + "\n" + "x" * 5000).encode()
    )
    # api: no previous log yet, the current one is read.
    h.shell.on("logs", "api-0", "-c", "api", "--previous")(lambda _a, _s: pp.Completed(1, b"", b""))
    h.shell.on("logs", "api-0", "-c", "api")(lambda _a, _s: b"listening\n")
    # gone: neither read works, so it is recorded as unavailable and nothing raises.
    h.shell.on("logs", "api-0", "-c", "gone")(lambda _a, _s: pp.Completed(1, b"", b""))

    h.run.record_rollout_diagnostics()

    logs = h.ledger.observations("install")["rollout_container_logs"]
    assert set(logs) == {"dex-0/dex", "api-0/api", "api-0/gone"}
    assert logs["dex-0/dex"].startswith("boom") and len(logs["dex-0/dex"]) <= 2000
    assert secret not in json.dumps(logs)
    assert logs["api-0/api"] == "listening\n"
    assert logs["api-0/gone"] == "unavailable"
    log_calls = [argv for argv in h.shell.kubectl() if "logs" in argv]
    assert log_calls and all(argv[1:3] == ["--context", CONTEXT] for argv in log_calls)
    assert all("--tail=20" in argv for argv in log_calls)


def test_install_refuses_a_rendered_runner_grant(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    h.run.digests = dict(DIGESTS)
    h.run.dex_image = f"{REGISTRY}/{pt.DEX_REPOSITORY}@{DEX_DIGEST}"
    grant_ref = {"secretKeyRef": {"name": f"{RELEASE}-personal-grant", "key": "token"}}
    _install_shell(h, rendered_extra={"name": pps.PERSONAL_GRANT_ENV, "valueFrom": grant_ref})
    with pytest.raises(pps.Refusal) as caught:
        h.run.phase_install()
    assert caught.value.code == "deployed_service_credential"
    assert h.ledger.observations("rehearsal")["install_no_runner_grant"] is False
    assert h.ledger.observations("owners")["installation_grant_absent"] is False


# --------------------------------------------------------------------------
# setup: the Dex login, the principal read and the enrollment argv
# --------------------------------------------------------------------------


def _dex_script(run: pt.TwoOwnerRun, email: str) -> list[tuple[str, str, Callable[..., Response]]]:
    """``/console/oidc/login`` 302, Dex's password form, its 303, the callback's 303.

    Shapes: apps/api/src/curie_api/routers/console.py and Dex v2.41
    (tools/oidc-e2e/oidc_e2e.py ``begin_login``).
    """

    issuer_origin = pto._origin(run.issuer)
    dex_local = f"http://127.0.0.1:{DEX_PORT}"
    api_local = f"http://127.0.0.1:{API_PORT}"
    authorize = "/dex/auth?client_id=curie-console&state=s1"
    callback = run.redirect_uri + "?code=code-marker&state=s1"
    form = (
        '<form method="post" action="/dex/auth/local/login?back=&amp;state=r1">'
        '<input name="login"><input type="password" name="password"></form>'
    )

    def password(headers: Mapping[str, str], body: bytes | None) -> Response:
        assert urllib.parse.parse_qs((body or b"").decode())["login"] == [email]
        return 303, [("Location", callback)], b""

    def finish(headers: Mapping[str, str], _body: bytes | None) -> Response:
        assert headers == {"Cookie": f"{pto.STATE_COOKIE}={STATE_MARKER}"}
        cookie = f"{pto.SESSION_COOKIE}={SESSION_MARKER}; HttpOnly; Path=/; Secure"
        return 303, [("Location", "/"), ("Set-Cookie", cookie)], b""

    return [
        (
            "GET",
            api_local + pto.LOGIN_PATH,
            lambda _h, _b: (
                302,
                [
                    ("Location", issuer_origin + authorize),
                    ("Set-Cookie", f"{pto.STATE_COOKIE}={STATE_MARKER}; Path=/; Secure"),
                ],
                b"",
            ),
        ),
        ("GET", dex_local + authorize, lambda _h, _b: (200, [], form.encode())),
        ("POST", dex_local + "/dex/auth/local/login?back=&state=r1", password),
        ("GET", api_local + pto.CALLBACK_PATH + "?code=code-marker&state=s1", finish),
    ]


def test_login_reads_the_principal_and_writes_a_private_session_file(tmp_path: Path) -> None:
    opener = _ScriptedOpener([])
    h = _harness(tmp_path, opener=opener)
    _connect(h)
    owner = h.run.owners["a"]
    opener.script = _dex_script(h.run, owner.email)

    @h.api.route("GET", "/console/principal")
    def principal(_path: str, headers: Mapping[str, str], _body: Any) -> tuple[int, Any]:
        # apps/api/src/curie_api/routers/console.py ConsolePrincipalOut.
        assert headers == {"Cookie": f"{pto.SESSION_COOKIE}={SESSION_MARKER}"}
        return 200, {
            "id": PRINCIPAL_A,
            "tenant_id": TENANT,
            "idp_subject": "dex|owner-a",
            "display_name": "Owner A",
            "email": owner.email,
            "status": "active",
        }

    h.run.login(owner)
    assert not opener.script
    assert owner.principal_id == PRINCIPAL_A and owner.tenant_id == TENANT
    assert owner.session_file is not None
    assert owner.session_file.read_text() == SESSION_MARKER + "\n"
    assert owner.session_file.stat().st_mode & 0o777 == 0o600
    assert any(held is owner.session for held in h.runtime.scrubber.held())
    assert h.ledger.observations("rehearsal")["dex_login_a"] is True
    assert h.ledger.observations("owners")["principal_active_a"] is True


def _subscription_answers(
    shell: _FakeShell, answers: Callable[[str, str], tuple[int, dict[str, Any]]]
) -> None:
    """``curie --json cluster subscription <verb>``: one object on stdout.

    Shape: cli/src/commands/subscription.rs ``SubscriptionOutput`` (state,
    generation, version, teardown, action, changed, previous_version); an
    error is ``{"error", "fix"}`` on stdout with exit 1 or 3.
    """

    @shell.on("cluster", "subscription")
    def answer(argv: list[str], _stdin: bytes | None) -> pp.Completed:
        verb = argv[argv.index("subscription") + 1]
        session = Path(argv[argv.index("--session-file") + 1]).name.removeprefix("session-")
        code, body = answers(verb, session)
        return pp.Completed(code, json.dumps(body).encode(), b"")


def _enrollment(state: str, generation: int, version: int, **teardown: int) -> dict[str, Any]:
    return {
        "action": "status",
        "changed": False,
        "previous_version": None,
        "state": state,
        "generation": generation,
        "version": version,
        "teardown": {"pending_runtimes": 0, "pending_custody_cleanup": 0, **teardown},
    }


def test_enroll_argv_names_the_token_file_and_never_its_bytes(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    owner = h.run.owners["a"]
    owner.state = ""
    _subscription_answers(h.shell, lambda _verb, _owner: (0, _enrollment("active", 1, 1)))
    h.run.enroll(owner)
    marker = owner.token.reveal() if owner.token else ""
    assert marker.startswith("pto-rehearsal-marker-a-")
    assert owner.token_file is not None and owner.token_file.parent == h.run.owner_dir
    assert owner.token_file.stat().st_mode & 0o777 == 0o600
    assert owner.token_file.read_text() == marker + "\n"
    enroll = h.shell.curie("subscription", "enroll")
    assert len(enroll) == 1
    argv = enroll[0]
    assert argv[1:5] == ["cluster", "--context", CONTEXT, "subscription"]
    assert argv[argv.index("--token-file") + 1] == str(owner.token_file)
    assert argv[argv.index("--session-file") + 1] == str(owner.session_file)
    assert argv[argv.index("--api-url") + 1] == f"http://127.0.0.1:{API_PORT}"
    assert argv[-5:] == ["--namespace", NAMESPACE, "--release", RELEASE, "--json"]
    every = " ".join(" ".join(call.argv) for call in h.shell.calls)
    assert marker not in every and "session-marker-a" not in every
    assert owner.state == "active" and owner.generation == 1
    assert h.ledger.observations("owners")["enrollment_active_gen1_a"] is True
    # The guard is live in the fake: the marker cannot reach an argument.
    with pytest.raises(pp.StepFailed, match="held secret"):
        h.shell.run(["curie", marker])


def test_live_setup_enrolls_from_the_callers_grant_file(tmp_path: Path) -> None:
    h = _harness(tmp_path, rehearsal=False)
    _connect(h)
    owner = h.run.owners["b"]
    grant_file = h.run.scenario.owner("b").grant_file
    assert owner.token_kind == "grant" and owner.token_file == grant_file
    _subscription_answers(h.shell, lambda _verb, _owner: (0, _enrollment("active", 1, 1)))
    h.run.enroll(owner)
    argv = h.shell.curie("subscription", "enroll")[0]
    assert argv[argv.index("--token-file") + 1] == str(grant_file)
    assert MARKER_B not in " ".join(" ".join(call.argv) for call in h.shell.calls)
    assert h.run.grant_files_distinct()
    assert not h.run.no_grant_loaded()


def test_enroll_fails_unless_status_reads_active_at_generation_one(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    owner = h.run.owners["a"]
    answers = {
        "enroll": (0, _enrollment("active", 1, 1)),
        "status": (0, _enrollment("active", 2, 3)),
    }
    _subscription_answers(h.shell, lambda verb, _owner: answers[verb])
    with pytest.raises(pp.StepFailed, match="generation 1"):
        h.run.enroll(owner)
    assert h.ledger.observations("owners")["enrollment_active_gen1_a"] is False


# --------------------------------------------------------------------------
# Fences: only the run's own claims, pods and owners
# --------------------------------------------------------------------------


def _selector(argv: list[str]) -> dict[str, str]:
    text = argv[argv.index("-l") + 1]
    return dict(item.split("=", 1) for item in text.split(","))


def _claims_cluster(h: _Harness, *, fail_disconnect: str = "") -> dict[str, Any]:
    """SandboxClaims with the labels the worker writes (apps/worker personal claims)."""

    agent = h.run.agent
    claims = [
        ("claim-a1", agent, PRINCIPAL_A),
        ("claim-b1", agent, PRINCIPAL_B),
        ("claim-x1", "acme-other-agent", PRINCIPAL_A),
    ]
    seen: dict[str, Any] = {"deleted": [], "pod_selectors": [], "versions": {"a": 1, "b": 3}}

    @h.shell.on("get", pp.CLAIM_RESOURCE, "-l")
    def listed(argv: list[str], _stdin: bytes | None) -> dict[str, Any]:
        wanted = _selector(argv)
        items: list[dict[str, Any]] = [
            {
                "metadata": {
                    "name": name,
                    "labels": {pp.CLAIM_AGENT_LABEL: owner_agent, pt.OWNER_PRINCIPAL_LABEL: who},
                }
            }
            for name, owner_agent, who in claims
            if name not in seen["deleted"]
        ]
        return {
            "items": [
                item
                for item in items
                if all(item["metadata"]["labels"].get(k) == v for k, v in wanted.items())
            ]
        }

    @h.shell.on("delete", pp.CLAIM_RESOURCE)
    def deleted(argv: list[str], _stdin: bytes | None) -> bytes:
        names = [
            arg for arg in argv[argv.index(pp.CLAIM_RESOURCE) + 1 :] if not arg.startswith("-")
        ]
        seen["deleted"].extend(names)
        return b""

    @h.shell.on("delete", "pod", "-l")
    def pods(argv: list[str], _stdin: bytes | None) -> bytes:
        seen["pod_selectors"].append(argv[argv.index("-l") + 1])
        return b""

    def answers(verb: str, owner: str) -> tuple[int, dict[str, Any]]:
        version = seen["versions"][owner]
        if verb == "status":
            state = "inactive" if version > 3 or (owner == "a" and version > 1) else "active"
            return 0, _enrollment(state, 1 if state == "active" else 2, version)
        if owner == fail_disconnect:
            return 1, {"error": "stale_version", "fix": "read status again"}
        seen["versions"][owner] = version + 1
        return 0, _enrollment("inactive", 2, version + 1)

    _subscription_answers(h.shell, answers)
    return seen


def test_fence_deletes_only_the_run_agents_claims_and_disconnects_active_owners(
    tmp_path: Path,
) -> None:
    h = _harness(tmp_path)
    _connect(h)
    h.run.owners["a"].state = "inactive"
    seen = _claims_cluster(h)
    h.run.fence_paid_starts()
    assert sorted(seen["deleted"]) == ["claim-a1", "claim-b1"]
    assert seen["pod_selectors"] == [f"{pp.POD_CLAIM_LABEL} in (claim-a1,claim-b1)"]
    scale = [argv for argv in h.shell.kubectl() if "scale" in argv]
    assert scale and scale[0][-2:] == [f"deployment/{FULLNAME}-worker", "--replicas=0"]
    first_disconnect = next(
        index for index, call in enumerate(h.shell.calls) if "disconnect" in call.argv
    )
    first_scale = next(index for index, call in enumerate(h.shell.calls) if "scale" in call.argv)
    assert first_scale < first_disconnect
    disconnects = h.shell.curie("subscription", "disconnect")
    assert len(disconnects) == 1
    assert disconnects[0][disconnects[0].index("--session-file") + 1].endswith("session-b")
    assert disconnects[0][disconnects[0].index("--expected-version") + 1] == "3"
    assert all(argv[1:3] == ["--context", CONTEXT] for argv in h.shell.kubectl())
    assert h.ledger.observations("budget")["paid_starts_fenced"] is True
    assert h.run.owners["b"].state == "inactive"


def test_fence_attempts_every_step_and_fails_when_an_owner_stays_active(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    seen = _claims_cluster(h, fail_disconnect="b")
    with pytest.raises(pp.StepFailed, match="owner-b-active"):
        h.run.fence_paid_starts()
    assert sorted(seen["deleted"]) == ["claim-a1", "claim-b1"]
    assert h.ledger.observations("budget")["paid_starts_fenced"] is False
    assert "disconnect-b" in h.ledger.observations("budget")["fence_failures"]


def test_fence_owner_selects_that_owners_claims_on_the_run_agent_only(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    seen = _claims_cluster(h)
    result = h.run.fence_owner(h.run.owners["a"])
    assert result == {"inactive": True, "claims_deleted": 1}
    # claim-x1 carries A's principal on another agent; claim-b1 is B's.
    assert seen["deleted"] == ["claim-a1"]
    assert seen["pod_selectors"] == [f"{pp.POD_CLAIM_LABEL} in (claim-a1)"]
    listed = [argv for argv in h.shell.kubectl() if "get" in argv and pp.CLAIM_RESOURCE in argv]
    assert _selector(listed[0]) == {
        pp.CLAIM_AGENT_LABEL: h.run.agent,
        pt.OWNER_PRINCIPAL_LABEL: PRINCIPAL_A,
    }
    assert not [argv for argv in h.shell.curie("disconnect") if "session-b" in " ".join(argv)]


# --------------------------------------------------------------------------
# The rehearsal controls against replayed API answers
# --------------------------------------------------------------------------


def _controls_cluster(h: _Harness, *, replay_refused: bool = True) -> dict[str, Any]:
    """The worker, Postgres, custody listing and API answers the controls read.

    API shapes: apps/api/src/curie_api/routers/work_items.py (admission
    ``{"work_item", "request", "replayed"}``, 409 ``{"detail": {"code"}}``,
    cancel, personal-context 404) and schemas/workitems.py
    ``WorkItemOutcomeOut.requests``.
    """

    state: dict[str, Any] = {"replicas": 1, "uid": "uid-worker-1", "disconnected": False}
    request: dict[str, Any] = {
        "sequence": 1,
        "status": "queued",
        "started_at": None,
        "terminal_at": None,
        "tenant_id": TENANT,
        "model_auth_mode": "personal",
        "owner_principal_id": PRINCIPAL_A,
        "enrollment_generation": 1,
    }
    shell, api, events = h.shell, h.api, h.events

    @shell.on("scale")
    def scale(argv: list[str], _stdin: bytes | None) -> bytes:
        replicas = int(argv[-1].removeprefix("--replicas="))
        events.append(f"scale-{replicas}")
        state["replicas"] = replicas
        if replicas:
            state["uid"] = "uid-worker-2"
        return b""

    shell.on("get", "pods", "-l", h.run.worker_selector())(
        lambda _a, _s: {
            "items": [
                _pod("worker", "worker", "curie-worker", DIGESTS["curie-worker"], uid=state["uid"])
            ]
            if state["replicas"]
            else []
        }
    )
    shell.on("psql")(
        lambda _a, stdin: [{"version": 1}] if b"curie.work_items" in (stdin or b"") else []
    )

    @shell.on("get", "secrets", "-l")
    def custody(_argv: list[str], _stdin: bytes | None) -> bytes:
        rows = ["curie-pe-bbbbbbbb-g1|enr-b|1"]
        if not state["disconnected"]:
            rows.insert(0, "curie-pe-aaaaaaaa-g1|enr-a|1")
        return ("\n".join(rows) + "\n").encode()

    def answers(verb: str, owner: str) -> tuple[int, dict[str, Any]]:
        if verb == "disconnect":
            events.append(f"disconnect-{owner}")
            state["disconnected"] = True
            return 0, _enrollment("inactive", 2, 2, pending_custody_cleanup=1)
        if owner == "a" and state["disconnected"]:
            return 0, _enrollment("inactive", 2, 2)
        return 0, _enrollment("active", 1, 1)

    _subscription_answers(shell, answers)

    @api.route("POST", "/v1/internal/work-items/admissions")
    def admit(_path: str, headers: Mapping[str, str], body: Any) -> tuple[int, Any]:
        assert headers == {"X-Curie-Worker-Token": "worker-token-marker-0001"}
        owner = body["owner"]["principal_id"]
        issue = body["tracker"]["issue_id"]
        events.append(f"admit-{owner[-4:]}-{issue}")
        if owner == PRINCIPAL_A and state["disconnected"]:
            return 409, {"detail": {"code": "enrollment_inactive"}}
        if owner == PRINCIPAL_B:
            if not replay_refused and body["request_id"] == state.get("request_id"):
                return 200, {"work_item": {"id": WORK_ITEM}, "request": request, "replayed": True}
            return 409, {"detail": {"code": "owner_mismatch"}}
        assert state["replicas"] == 0
        state["request_id"] = body["request_id"]
        work_item = {
            "id": WORK_ITEM,
            "conversation_id": "github:acme-corp/acme-fixture:issue-11",
            "version": 1,
            "tenant_id": TENANT,
            "model_auth_mode": "personal",
            "owner_principal_id": PRINCIPAL_A,
            "enrollment_generation": 1,
        }
        return 200, {"work_item": work_item, "request": request, "replayed": False}

    @api.route("GET", f"/work-items/{WORK_ITEM}")
    def item(_path: str, _headers: Mapping[str, str], _body: Any) -> tuple[int, Any]:
        return 200, {
            "id": WORK_ITEM,
            "tenant_id": TENANT,
            "model_auth_mode": "personal",
            "owner_principal_id": PRINCIPAL_A,
            "enrollment_generation": 1,
            "requests": [dict(request)],
        }

    @api.route("GET", f"/v1/internal/work-items/{WORK_ITEM}/personal-context")
    def context(path: str, _headers: Mapping[str, str], _body: Any) -> tuple[int, Any]:
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
        if query["owner_principal_id"] == [PRINCIPAL_A]:
            return 200, {"work_item_id": WORK_ITEM, "enrollment_generation": 1}
        return 404, {"detail": {"code": "not_found"}}

    @api.route("POST", f"/v1/internal/work-items/{WORK_ITEM}/cancel")
    def cancel(_path: str, _headers: Mapping[str, str], body: Any) -> tuple[int, Any]:
        assert body == {"expected_version": 1}
        events.append("cancel")
        request.update(status="cancelled", terminal_at="2027-01-15T08:00:00Z")
        return 200, {"work_item": {"id": WORK_ITEM}, "request": request}

    return state


def test_rehearsal_controls_record_the_expected_keys(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    _controls_cluster(h)
    h.run.phase_rehearsal_controls()
    observed = h.ledger.observations("rehearsal")
    for key in (
        "admission_controls_refused",
        "disconnect_refused_after_restart",
        "no_grant_loaded",
    ):
        assert observed[key] is True, key
    for key in (
        "b_on_a_issue_refused",
        "request_count_unchanged",
        "replay_owner_b_refused",
        "personal_context_b_hidden",
        "personal_context_a_readable",
        "no_request_started",
        "owner_unchanged",
        "inactive",
        "generation_advanced",
        "custody_secret_absent",
        "new_worker_uid",
        "candidate_image",
        "admission_refused",
        "no_claim_in_window",
        "b_active_gen1",
    ):
        assert observed[f"control_{key}"] is True, key
    assert observed["control_pending_runtimes"] == 0
    assert observed["control_pending_custody_cleanup"] == 0
    a_tail = PRINCIPAL_A[-4:]
    assert h.events.index("scale-0") < h.events.index(f"admit-{a_tail}-11")
    assert h.events.index("cancel") < h.events.index("disconnect-a")
    assert h.events.index("disconnect-a") < h.events.index("scale-1")
    assert h.events[-1] == f"admit-{a_tail}-11" and h.events.count(f"admit-{a_tail}-11") == 2
    # No B disconnect and no paid start: the fence and finalizer do the rest.
    assert "disconnect-b" not in h.events
    assert h.run.owners["a"].state == "inactive" and h.run.owners["a"].generation == 2


def test_rehearsal_controls_fail_when_a_wrong_owner_replay_is_accepted(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    _connect(h)
    _controls_cluster(h, replay_refused=False)
    with pytest.raises(pp.StepFailed, match="admission_controls_refused"):
        h.run.phase_rehearsal_controls()
    observed = h.ledger.observations("rehearsal")
    assert observed["admission_controls_refused"] is False
    assert observed["control_replay_owner_b_refused"] is False


# --------------------------------------------------------------------------
# SQL and stream reads
# --------------------------------------------------------------------------


def test_sql_rows_pass_only_validated_values_as_psql_variables(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    h.shell.on("psql")(lambda _a, _s: b'[{"version": 4},\n {"version": 5}]\n')
    rows = h.run.sql_rows(
        "SELECT version FROM curie.work_items WHERE id = :'item'::uuid AND version > :'floor'",
        {"item": WORK_ITEM, "floor": 3},
    )
    assert rows == [{"version": 4}, {"version": 5}]
    call = h.shell.calls[-1]
    assert call.argv[:3] == ["kubectl", "--context", CONTEXT]
    assert call.argv[call.argv.index("exec") + 2] == f"{FULLNAME}-postgres-0"
    assert ["-v", f"item={WORK_ITEM}"] == call.argv[call.argv.index(f"item={WORK_ITEM}") - 1 :][:2]
    assert ["-f", "-"] == call.argv[-2:]
    assert call.stdin is not None and WORK_ITEM.encode() not in call.stdin
    assert b":'item'" in call.stdin and b"json_agg(row_to_json(q))" in call.stdin
    for bad_query, params in (
        ("SELECT 1; DROP TABLE x", {}),
        ("SELECT \\! true", {}),
        ("SELECT :'x'", {"x": "1' OR '1'='1"}),
        ("SELECT :'x'", {"x": True}),
        ("SELECT :'Bad'", {"Bad": 1}),
    ):
        with pytest.raises(pp.StepFailed):
            h.run.sql_rows(bad_query, params)


def test_valkey_xrange_sends_the_password_on_stdin_only(tmp_path: Path) -> None:
    h = _harness(tmp_path)
    password = h.runtime.scrubber.hold(pps.HeldSecret("store-password-marker-0001"))
    h.run.valkey_password = password
    # valkey-cli 8.1 --json: [[id, [field, value, ...]], ...] (RESP2) or a map (RESP3).
    h.shell.on("valkey-cli")(lambda _a, _s: b'[["1-0", ["k", "v"]], ["2-0", {"a": "b"}]]')
    h.shell.on("sh", "-c")(lambda _a, _s: b'[["1-0", ["k", "v"]], ["2-0", {"a": "b"}]]')
    entries = h.run.valkey_xrange("curie:stream:acme", "-", "+", count=10)
    assert entries == [("1-0", {"k": "v"}), ("2-0", {"a": "b"})]
    call = h.shell.calls[-1]
    assert call.argv[:3] == ["kubectl", "--context", CONTEXT]
    assert password.reveal() not in " ".join(call.argv)
    assert call.stdin == (password.reveal() + "\n").encode() and call.secret_stdin
    assert call.argv[-6:] == ["XRANGE", "curie:stream:acme", "-", "+", "COUNT", "10"]
    for stream, start in (("bad stream", "-"), ("curie:s", "0; FLUSHALL")):
        with pytest.raises(pp.StepFailed):
            h.run.valkey_xrange(stream, start)


def test_loopback_opener_refuses_any_non_loopback_origin() -> None:
    opener = pt.LoopbackOpener(pps.TokenScan(pps.Scrubber(), pps.SCAN_CHANNELS))
    for url in ("http://example.test/dex", "https://127.0.0.1:1/x", "http://10.0.0.1:5556/dex"):
        with pytest.raises(pto.LoginFailed) as caught:
            opener("GET", url, {}, None)
        assert caught.value.step == "opener_origin"


def test_inventory_queries_every_two_owner_kind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The single-profile kinds are LiveRun's own, pinned by test_personal_profile.
    monkeypatch.setattr(
        pp.LiveRun, "query_inventory", lambda _self: dict.fromkeys(pps.INVENTORY_KINDS, "absent")
    )
    h = _harness(tmp_path)
    leftover = {"dex": False}
    h.shell.on("get", "deployment,service,secret", "-l", pt.DEX_SELECTOR)(
        lambda _a, _s: b"deployment.apps/pto-dex\n" if leftover["dex"] else b""
    )
    results = h.run.query_inventory()
    assert set(pto.INVENTORY_KINDS) <= set(results)
    assert set(results.values()) == {"absent"}, results
    leftover["dex"] = True
    h.run.claims_seen.add("claim-a1")
    h.shell.on("get", "secret", "claim-a1" + pto.CLAIM_TOKENS_SUFFIX)(
        lambda _a, _s: pp.Completed(1, b"", b"forbidden")
    )
    results = h.run.query_inventory()
    assert results["dex"] == "present" and results["claim_tokens"] == "uncertain"
    assert repr(h.run.owners["a"]) == "<owner a>"


# --------------------------------------------------------------------------
# Paid phases: reservations, the gate, settlement and the phase checks
#
# The cluster-facing seams (runner image check, harvest and gate streams,
# delivery_checks, the fence) are replaced; the ledger, the gate decision, the
# transcript parsing and the phase logic are the real ones. The interruption
# and delivery tests below run the real methods against the fake shell.
# Transcripts take
# the native CLI shape (test_personal_profile.py _transcripts), harvest lines
# the pps.HARVEST_PROGRAM shape, and holds the pto.GATE_HOOK_PROGRAM record.
# --------------------------------------------------------------------------

PROJECT = "curie-claude-config-t1/projects/-workspace"
MAIN = f"{PROJECT}/session-1.jsonl"
REQUEST_A1 = "5d0c7f3e-0000-4000-8000-0000000d0a01"
REQUEST_B1 = "5d0c7f3e-0000-4000-8000-0000000d0b01"
WORK_ITEM_B = "5d0c7f3e-0000-4000-8000-0000000c0002"
HEAD_B1 = "1" * 40
DELIVERY_KEYS = ("own_grant", "not_other", "runner_grant_ref_per_claim", "custody_secret_matches")
HEAD_B2 = "2" * 40
BRANCH_B = "curie/publication-" + "b" * 32


class _FakeGate:
    """``GateChannel`` answering a limits command the way GATE_STREAM_PROGRAM does."""

    def __init__(self, *, ack: bool = True) -> None:
        self.queue: list[dict[str, Any]] = []
        self.sent: list[dict[str, Any]] = []
        self.ack = ack
        self.closed = False

    def events(self) -> list[dict[str, Any]]:
        drained, self.queue = self.queue, []
        return drained

    def send(self, command: Mapping[str, Any]) -> None:
        self.sent.append(dict(command))
        if self.ack and "limits" in command:
            self.queue.append({"limits_applied": dict(command["limits"])})

    def ended(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True


class _Live(pt.TwoOwnerRun):
    """The live driver with its cluster-facing seams replaced."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.trace: list[str] = []
        self.gates: list[_FakeGate] = []

    def fence_paid_starts(self) -> None:
        self.trace.append("fence")

    def check_runner_image(self, attempt: pp.Attempt) -> None:
        self.trace.append(f"image-{attempt.number}")

    def delivery_checks(self, owner: pt.Owner, claim: str) -> dict[str, bool]:
        self.trace.append(f"delivery-{owner.name}")
        return dict.fromkeys(DELIVERY_KEYS, True)

    def open_harvest(self, run: pt.PodRun) -> None:
        run.harvest = pps.HarvestState(self.scan)
        self.trace.append(f"harvest-{run.group}")

    def open_gate(self, run: pt.PodRun) -> pt.GateChannel:
        gate = _FakeGate()
        self.gates.append(gate)
        return gate

    def resolve_probe_cli(self) -> str:
        return "/opt/fake/bin/claude"

    def record_summary_keys(self) -> None:
        self.trace.append("summary")


def _live(tmp_path: Path, *, begin: bool = True) -> _Harness:
    h = _harness(tmp_path, rehearsal=False, cls=_Live)
    _connect(h)
    for owner in pto.OWNERS:
        h.ledger.set_owner_window(owner, WINDOW)
    if begin:
        h.ledger.begin_live(now=T0, grant_expires_at=WINDOW)
    return h


def _trace(h: _Harness) -> list[str]:
    run = h.run
    assert isinstance(run, _Live)
    return run.trace


def _settle_group(h: _Harness, group: str) -> None:
    probe = pto.GROUPS[group].kind == "probe"
    for item in h.ledger.reserve_group(group, now=h.clock.now()):
        h.ledger.settle(
            item.seq,
            pps.SessionUsage(5 if probe else 0, 1 if probe else 0, 0.0, ""),
            source="probe" if probe else "not_started",
        )


def _through(h: _Harness, group: str) -> None:
    """Reserve and settle every earlier group of the same owner, in manifest order."""

    owner = pto.GROUPS[group].owner
    for spec in pto.MANIFEST[: pto.group_index(group) - 1]:
        if spec.owner == owner:
            _settle_group(h, spec.name)


def _pod_run(
    h: _Harness, group: str, *, policy: str = pt.RELEASE, card: int = 0, reserve: bool = True
) -> pt.PodRun:
    run = h.run.new_run(group, card=card, policy=policy)
    if reserve:
        h.run.reserve(run)
    run.claim, run.pod, run.pod_uid = f"claim-{group}", f"pod-{group}", f"uid-{group}"
    run.harvest = pps.HarvestState(h.runtime.scan)
    run.gate = _FakeGate()
    run.limits = pto.subagent_limits(group)
    run.limits_acked = True
    run.started_at = h.clock.now()
    h.run.pods.append(run)
    return run


def _stamp(offset: float) -> str:
    return dt.datetime.fromtimestamp(T0 + offset, dt.UTC).isoformat().replace("+00:00", "Z")


def _jsonl(records: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _reply(message_id: str, at: float, tokens: int, content: list[Any]) -> dict[str, Any]:
    return {
        "type": "assistant",
        "timestamp": _stamp(at),
        "message": {
            "id": message_id,
            "model": pps.IMPLEMENTER_MODEL,
            "role": "assistant",
            "content": content,
            "usage": {"input_tokens": 10, "output_tokens": tokens},
        },
    }


def _tool(tool_id: str, name: str = "Bash") -> dict[str, Any]:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": {}}


def _result(tool_id: str, at: float) -> dict[str, Any]:
    return {
        "type": "user",
        "timestamp": _stamp(at),
        "message": {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": tool_id, "content": "ok"}],
        },
    }


def _prompt(at: float = 0) -> dict[str, Any]:
    return {"type": "user", "timestamp": _stamp(at), "message": {"role": "user", "content": "go"}}


def _feed(run: pt.PodRun, files: Mapping[str, bytes], *, at: float = T0) -> None:
    """Harvest ``file`` and ``sync`` lines (pps.HARVEST_PROGRAM) up to ``files``."""

    state = run.harvest
    assert state is not None
    for path, data in sorted(files.items()):
        offset = len(state.files.get(path, b""))
        if len(data) > offset:
            chunk = base64.b64encode(data[offset:]).decode()
            line = {"kind": "file", "path": path, "offset": offset, "data": chunk}
            state.feed(json.dumps(line).encode())
    sizes = {path: len(data) for path, data in files.items()}
    state.feed(json.dumps({"kind": "sync", "sizes": sizes, "at": at}).encode())


def _hold(
    mode: str,
    number: int,
    *,
    size: int | None = None,
    written: float = T0,
    tool: str | None = None,
    kind: str | None = None,
    agent: str | None = None,
    cli: tuple[int, int] = (4242, 77),
) -> dict[str, Any]:
    """One gate stream ``hold`` event carrying a GATE_HOOK_PROGRAM record."""

    key = f"{mode}-{number}-{number}"
    record = {
        "key": key,
        "mode": mode,
        "tool_name": tool,
        "subagent_kind": kind,
        "session_id": "session-1",
        "transcript_path": f"/tmp/{MAIN}",
        "transcript_size": size,
        "agent_id": agent,
        "agent_transcript_path": None,
        "agent_transcript_size": None,
        "cli_pid": cli[0],
        "cli_start": cli[1],
        "hook_pid": 5000 + number,
        "hook_start": 9,
        "written_at": written,
    }
    return {"hold": record, "key": key}


def _gate(run: pt.PodRun) -> _FakeGate:
    assert isinstance(run.gate, _FakeGate)
    return run.gate


def _rows(h: _Harness, group: str) -> list[dict[str, Any]]:
    return [row for row in h.ledger.session_rows() if row["grp"] == group]


# -- reservations come first ----------------------------------------------------


def _admission_answer(owner: str, request_id: str) -> dict[str, Any]:
    principal = PRINCIPAL_A if owner == "a" else PRINCIPAL_B
    tuple_ = {
        "tenant_id": TENANT,
        "model_auth_mode": "personal",
        "owner_principal_id": principal,
        "enrollment_generation": 1,
    }
    return {
        "work_item": {"id": WORK_ITEM, "conversation_id": "issue-11", **tuple_},
        "request": {"id": request_id, "sequence": 1, "status": "queued", **tuple_},
        "replayed": False,
    }


def test_reservation_precedes_the_admission(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    seen: list[int] = []

    @h.api.route("POST", "/v1/internal/work-items/admissions")
    def admit(_path: str, _headers: Mapping[str, str], _body: Any) -> tuple[int, Any]:
        seen.append(len(h.ledger.reservations_of("a1")))
        return 200, _admission_answer("a", REQUEST_A1)

    run = h.run.new_run("a1", card=1)
    h.run.admit_group(run, h.run.owners["a"])
    assert seen == [3]
    assert run.request_id == REQUEST_A1 and run.work_item_id == WORK_ITEM
    assert run.admitted_owner is True and h.run.owners["a"].work_item_ids == [WORK_ITEM]
    # Control: a group that cannot be reserved is never admitted.
    with pytest.raises(pps.Refusal) as refused:
        h.run.admit_group(h.run.new_run("b1", card=3), h.run.owners["b"])
    assert refused.value.code == "group_out_of_order"
    assert seen == [3]


@pytest.mark.parametrize(("status", "source"), [(409, "not_started"), (503, "unknown")])
def test_a_failed_admission_settles_its_reservations_and_fences(
    tmp_path: Path, status: int, source: str
) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    h.api.route("POST", "/v1/internal/work-items/admissions")(
        lambda _p, _h, _b: (status, {"detail": {"code": "owner_mismatch"}})
    )
    with pytest.raises(pp.StepFailed, match=f"admission returned {status}"):
        h.run.admit_group(h.run.new_run("a1", card=1), h.run.owners["a"])
    assert {row["source"] for row in _rows(h, "a1")} == {source}
    assert _trace(h).count("fence") == 1


def test_reservation_precedes_the_worker_scale_up(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "b-r1")
    first = _pod_run(h, "b1", card=3, reserve=False)
    first.request_id, first.work_item_id = REQUEST_B1, WORK_ITEM_B
    h.run.worker_before = ["uid-worker-1"]
    seen: list[int] = []

    @h.shell.on("scale")
    def scale(_argv: list[str], _stdin: bytes | None) -> bytes:
        seen.append(len(h.ledger.reservations_of("b-r1")))
        return b""

    worker = _pod("worker-2", "worker", "curie-worker", DIGESTS["curie-worker"], uid="uid-w2")
    h.run.worker_pods = lambda pods=None: [worker]  # type: ignore[method-assign]
    h.run.observe_worker = lambda pods=None: ("uid-w2", True)  # type: ignore[method-assign]
    h.run.phase_worker_start()
    assert seen == [1]
    restarts = h.ledger.observations("restarts")
    assert restarts["worker_fresh_runtime"] is True
    assert restarts["worker_candidate_image"] is True
    assert h.run.worker_name == "worker-2"


def test_the_worker_is_not_scaled_up_when_b_r1_cannot_be_reserved(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "b-r1")
    first = _pod_run(h, "b1", card=3, reserve=False)
    first.request_id, first.work_item_id = REQUEST_B1, WORK_ITEM_B
    # An unknown session anywhere blocks every further reservation.
    seq = h.ledger.reserve_group("a-probe", now=T0)[0].seq
    h.ledger.settle(seq, None, source="unknown")
    with pytest.raises(pps.Refusal) as refused:
        h.run.phase_worker_start()
    assert refused.value.code == "usage_unknown"
    assert not [argv for argv in h.shell.kubectl() if "scale" in argv]


def _interruption_setup(h: _Harness, *, prompt_only: bool = True) -> pt.PodRun:
    _through(h, "b-r1")
    held = _pod_run(h, "b-r1", policy=pt.KEEP_PROMPT)
    held.request_id, held.work_item_id = REQUEST_B1, WORK_ITEM_B
    held.conversation_id = "issue-12"
    records = [_prompt()] if prompt_only else [_prompt(), _reply("m1", 1, 40, [])]
    files = {MAIN: _jsonl(records)}
    _feed(held, files)
    held.kept = _hold("prompt", 1, size=len(files[MAIN]))["hold"]
    held.kept_files, held.kept_facts = files, h.run.pod_facts(held, files)
    held.clis = [(4242, 77)]
    held.eof.set()
    h.run.a_stop_at = T0 + 5
    row = {
        "id": REQUEST_B1,
        "work_item_id": WORK_ITEM_B,
        "runtime_epoch": 3,
        "runtime_claim_name": held.claim,
        "owner_principal_id": PRINCIPAL_B,
        "tenant_id": TENANT,
        "enrollment_generation": 1,
    }
    h.run.request_row = lambda _request: dict(row)  # type: ignore[method-assign]
    h.run.marker_present = lambda _owner: True  # type: ignore[method-assign]
    h.run.owner_claims = lambda _owner: [held.claim]  # type: ignore[method-assign]
    h.run.await_container_restart = lambda _run: {"metadata": {"uid": held.pod_uid}}  # type: ignore[method-assign]

    def begin(run: pt.PodRun) -> None:
        run.clis = [(5151, 88)]
        run.delivery = {"own_grant": True, "not_other": True}
        h.run.pods.append(run)

    h.run.begin_pod_run = begin  # type: ignore[method-assign]
    h.run.first_tool = lambda _run, _moment: T0 + 50  # type: ignore[method-assign]
    return held


def test_reservation_precedes_the_interruption_call(tmp_path: Path) -> None:
    h = _live(tmp_path)
    held = _interruption_setup(h)
    seen: list[tuple[int, bool]] = []

    def interrupt(pod: str, hold: Mapping[str, Any]) -> dict[str, Any]:
        seen.append((len(h.ledger.reservations_of("b-r2")), h.run.settled_known("b-r1")))
        assert pod == held.pod and hold["cli_pid"] == 4242
        return {"action": "signal_processes", "native_identity_matched": True, "native_gone": True}

    h.run.interrupt_native_process = interrupt  # type: ignore[method-assign]
    h.run.phase_process_interruption()
    assert seen == [(3, True)]
    restarts = h.ledger.observations("restarts")
    for key in (
        "process_lineage_before",
        "process_termination_epoch",
        "process_fresh_runtime",
        "process_usage_settled",
        "process_identity_matched",
        "process_no_pod_delete",
        "process_claim_still_bound",
        "process_no_successor_claim",
        "process_same_request_epoch",
    ):
        assert restarts[key] is True, key
    assert h.ledger.observations("disconnect")["b_tool_after_a_stop"] is True
    assert [row["output_tokens"] for row in _rows(h, "b-r1")] == [0]


def test_no_interruption_when_the_held_prompt_did_not_settle_to_zero(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _interruption_setup(h, prompt_only=False)
    calls: list[str] = []
    h.run.interrupt_native_process = lambda pod, _hold: calls.append(pod) or {}  # type: ignore[method-assign]
    with pytest.raises(pp.StepFailed, match="usage unknown or unmapped"):
        h.run.phase_process_interruption()
    assert calls == [] and h.ledger.reservations_of("b-r2") == []
    assert {row["source"] for row in _rows(h, "b-r1")} == {"unknown"}
    assert "fence" in _trace(h)


def _witness(*, matched: bool = True, gone: bool = True) -> bytes:
    """pto.INTERRUPT_PROGRAM's one stdout line."""

    line = {"action": "signal_processes", "native_identity_matched": matched, "native_gone": gone}
    return json.dumps(line).encode() + b"\n"


def _answer(stdout: bytes) -> Answer:
    return lambda _argv, _stdin: stdout


def _interrupts(h: _Harness) -> list[list[str]]:
    return [argv for argv in h.shell.kubectl() if "exec" in argv and "python3" in argv]


def test_interruption_signals_the_held_identity_and_returns_the_judged_facts(
    tmp_path: Path,
) -> None:
    h = _live(tmp_path)
    held = _interruption_setup(h)
    h.shell.on("exec", held.pod, "python3")(lambda _a, _s: _witness())
    facts = h.run.interrupt_native_process(held.pod, held.kept or {})
    assert facts == {
        "action": "signal_processes",
        "native_identity_matched": True,
        "native_gone": True,
        "old_epoch": 3,
        "confirmed_epoch": 3,
        "old_claim_still_bound_to_request": True,
        "successor_claim_observed": False,
        "driver_pod_delete_calls": 0,
    }
    assert poes.process_interruption_checks(facts) == {
        "interruption_process_only": True,
        "old_epoch_confirmed_before_bind": True,
    }
    (argv,) = _interrupts(h)
    assert argv[:3] == ["kubectl", "--context", CONTEXT]
    assert argv[argv.index("exec") :] == [
        "exec",
        held.pod,
        "-c",
        "runner",
        "--",
        "python3",
        "-c",
        pto.INTERRUPT_PROGRAM,
        "4242",
        "77",
    ]
    assert not [argv for argv in h.shell.kubectl() if "delete" in argv]
    assert h.ledger.observations("diagnostics")["process_interruption_witness"] == facts
    assert "fence" not in _trace(h)


def test_interruption_fails_fenced_on_a_mismatch_or_an_unreadable_witness(
    tmp_path: Path,
) -> None:
    for name, answer, match, recorded in (
        ("mismatch", _witness(matched=False, gone=False), "identity did not match", True),
        ("unreadable", b"Error from server: container not found\n", "no readable witness", False),
        ("two-lines", _witness() + _witness(), "no readable witness", False),
    ):
        h = _live(tmp_path / name)
        held = _interruption_setup(h)
        h.shell.on("exec", held.pod, "python3")(_answer(answer))
        with pytest.raises(pp.StepFailed, match=match):
            h.run.interrupt_native_process(held.pod, held.kept or {})
        assert len(_interrupts(h)) == 1 and "fence" in _trace(h), name
        # fail_paid charged the open B-r1 reservation at its share.
        assert {row["source"] for row in _rows(h, "b-r1")} == {"unknown"}, name
        witness = h.ledger.observations("diagnostics").get("process_interruption_witness")
        assert (witness is not None) is recorded, name
        assert witness is None or witness["native_identity_matched"] is False
    # An unknown hold or pod signals nothing: the program never runs.
    h = _live(tmp_path / "unknown")
    held = _interruption_setup(h)
    for pod, hold in (
        (held.pod, {**(held.kept or {}), "cli_pid": None}),
        (held.pod, {**(held.kept or {}), "cli_pid": 1}),
        ("pod-elsewhere", held.kept or {}),
    ):
        with pytest.raises(pp.StepFailed, match="nothing was signalled"):
            h.run.interrupt_native_process(pod, hold)
    assert _interrupts(h) == []


def test_interruption_fails_fenced_when_a_successor_claim_appears(tmp_path: Path) -> None:
    h = _live(tmp_path)
    held = _interruption_setup(h)
    listings = iter([[held.claim], [held.claim, "claim-b-r2"]])

    def owner_claims(owner: pt.Owner) -> list[str]:
        return next(listings)

    h.run.owner_claims = owner_claims  # type: ignore[method-assign]
    h.shell.on("exec", held.pod, "python3")(lambda _a, _s: _witness())
    with pytest.raises(pp.StepFailed, match="successor claim"):
        h.run.interrupt_native_process(held.pod, held.kept or {})
    facts = h.ledger.observations("diagnostics")["process_interruption_witness"]
    assert facts["successor_claim_observed"] is True and "fence" in _trace(h)
    assert poes.process_interruption_checks(facts)["old_epoch_confirmed_before_bind"] is False


def test_process_phase_passes_through_the_real_interruption(tmp_path: Path) -> None:
    h = _live(tmp_path)
    held = _interruption_setup(h)
    h.shell.on("exec", held.pod, "python3")(lambda _a, _s: _witness())
    h.run.phase_process_interruption()
    assert len(_interrupts(h)) == 1
    restarts = h.ledger.observations("restarts")
    for key in (
        "process_termination_epoch",
        "process_identity_matched",
        "process_no_pod_delete",
        "process_old_runtime_absent",
    ):
        assert restarts[key] is True, key
    assert h.ledger.observations("diagnostics")["process_interruption"] == {
        "interruption_process_only": True,
        "old_epoch_confirmed_before_bind": True,
    }


ENROLLMENT_A = "5d0c7f3e-0000-4000-8000-0000000e0a01"


def _delivery_cluster(h: _Harness) -> dict[str, Any]:
    """``claim-a-tokens``, its runner pod, A's custody row and Secret metadata.

    Shapes: ``kubectl get secret -o json`` (data base64), a pod's
    ``secretKeyRef`` env entry, the joined custody row as ``sql_rows`` reads
    it, and the pto.CUSTODY_METADATA_JSONPATH line.
    """

    custody = f"curie-pe-{uuid.UUID(ENROLLMENT_A).hex}-g1"
    state: dict[str, Any] = {"value": MARKER_A, "key": pps.PERSONAL_GRANT_ENV, "absent": False}

    @h.shell.on("get", "secret", "claim-a-tokens")
    def tokens(_argv: list[str], _stdin: bytes | None) -> Any:
        if state["absent"]:
            return pp.Completed(1, b"", b'Error from server (NotFound): secrets "x" not found\n')
        encoded = base64.b64encode(state["value"].encode()).decode()
        return {"metadata": {"name": "claim-a-tokens"}, "data": {state["key"]: encoded}}

    ref = {"secretKeyRef": {"name": "claim-a-tokens", "key": pps.PERSONAL_GRANT_ENV}}
    env = [{"name": pps.PERSONAL_GRANT_ENV, "valueFrom": ref}]
    pod = {"metadata": {"name": "pod-a1"}, "spec": {"containers": [{"name": "runner", "env": env}]}}
    h.shell.on("get", "pods")(lambda _a, _s: {"items": [pod]})
    row = {
        "enrollment_id": ENROLLMENT_A,
        "generation": 1,
        "state": "published",
        "secret_namespace": h.run.grant_namespace,
        "secret_name": custody,
        "secret_key": pps.PERSONAL_GRANT_ENV,
        "secret_uid": "uid-custody-a",
        "header_generation": 1,
        "active": True,
    }
    h.shell.on("psql")(lambda _a, _s: [row])
    line = f"{custody}|uid-custody-a|curie|true|{uuid.UUID(ENROLLMENT_A).hex}|1"
    h.shell.on("get", "secret", custody)(lambda _a, _s: line)
    return state


def test_delivery_checks_compare_digests_and_never_surface_the_grant(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    h = _harness(tmp_path, rehearsal=False)
    _connect(h)
    h.shell.log = h.run.inputs.command_log
    state = _delivery_cluster(h)
    owner = h.run.owners["a"]
    assert h.run.delivery_checks(owner, "claim-a") == dict.fromkeys(DELIVERY_KEYS, True)
    # B's grant in A's claim: neither A's own nor "not the other owner's".
    state["value"] = MARKER_B
    assert h.run.delivery_checks(owner, "claim-a") == {
        **dict.fromkeys(DELIVERY_KEYS, True),
        "own_grant": False,
        "not_other": False,
    }
    # A missing key, a missing Secret, a failing read and no claim answer False.
    state["value"], state["key"] = MARKER_A, "token"
    missing = h.run.delivery_checks(owner, "claim-a")
    state["key"], state["absent"] = pps.PERSONAL_GRANT_ENV, True
    absent = h.run.delivery_checks(owner, "claim-a")
    for result in (missing, absent):
        assert result["own_grant"] is False and result["not_other"] is False
    assert h.run.delivery_checks(owner, "") == dict.fromkeys(DELIVERY_KEYS, False)
    reads = [call for call in h.shell.calls if "claim-a-tokens" in call.argv]
    assert len(reads) == 4 and all(call.sensitive_output for call in reads)
    selectors = [argv[argv.index("-l") + 1] for argv in h.shell.kubectl() if "pods" in argv]
    assert set(selectors) == {f"{pp.POD_CLAIM_LABEL}=claim-a"}
    # No grant form reaches argv, stdin, the command log, the ledger, a scan or the output.
    forms = {
        form
        for marker in (MARKER_A, MARKER_B)
        for raw in (marker.encode(),)
        for form in (raw, base64.b64encode(raw), base64.b64encode(raw).rstrip(b"="))
    }
    for call in h.shell.calls:
        assert not any(form.decode() in arg for form in forms for arg in call.argv)
        assert not any(form in (call.stdin or b"") for form in forms)
    entries = [json.loads(line) for line in h.run.inputs.command_log.read_bytes().splitlines()]
    assert len(entries) == len(h.shell.calls)
    for call, entry in zip(h.shell.calls, entries, strict=True):
        if call in reads:
            # pp.Shell.record keeps a sensitive call's exit only: no stdout, no stderr.
            assert entry == {"argv": call.argv[:6], "exit": entry["exit"], "stderr_tail": ""}
    for path in h.run.inputs.state_dir.rglob("*"):
        if path.is_file():
            assert not any(form in path.read_bytes() for form in forms), path.name
    assert not any(entry.token_bytes_found for entry in h.runtime.scan.channels.values())
    captured = capsys.readouterr()
    assert not any(form.decode() in captured.out + captured.err for form in forms)


def test_delivery_checks_never_raise_past_the_caller(tmp_path: Path) -> None:
    h = _harness(tmp_path, rehearsal=False)
    _connect(h)

    def broken(_argv: list[str], _stdin: bytes | None) -> Any:
        raise pp.StepFailed("kubectl could not complete")

    for match in (("get", "secret"), ("get", "pods"), ("psql",)):
        h.shell.on(*match)(broken)
    assert h.run.delivery_checks(h.run.owners["b"], "claim-b") == dict.fromkeys(
        DELIVERY_KEYS, False
    )


# -- the gate -------------------------------------------------------------------


def test_a_prompt_hold_waits_for_the_limits_ack(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    run.limits_acked = False
    files = {MAIN: _jsonl([_prompt()])}
    _feed(run, files)
    _gate(run).queue.append(_hold("prompt", 1, size=len(files[MAIN])))
    h.run.pump()
    assert _gate(run).sent == [] and run.deferred
    _gate(run).queue.append({"limits_applied": dict(run.limits)})
    h.run.pump()
    assert _gate(run).sent == [{"release": "prompt-1-1", "verdict": "go"}]
    assert run.limits_acked and run.prompt_seen and not run.deferred


def test_an_unacknowledged_limits_command_stops_the_pod(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    run.limits_acked, run.limits_sent_at = False, h.clock.now()
    _feed(run, {MAIN: _jsonl([_prompt()])})
    _gate(run).queue.append(_hold("prompt", 1))
    h.run.pump()
    h.clock.wait(pt.LIMITS_ACK_SECONDS + 1)
    with pytest.raises(pp.StepFailed, match="gate_limits_unacknowledged"):
        h.run.pump()
    assert _gate(run).sent == [] and "fence" in _trace(h)


def test_a_hold_the_capture_does_not_cover_stops(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    files = {MAIN: _jsonl([_prompt(), _reply("m1", 1, 10, [_tool("t1")])])}
    _feed(run, files)
    # Control: a hold the harvest has read to its reported size is released.
    _gate(run).queue.append(_hold("response", 1, size=len(files[MAIN]), tool="Bash"))
    h.run.pump()
    assert _gate(run).sent == [{"release": "response-1-1", "verdict": "go"}]
    _gate(run).queue.append(_hold("response", 2, size=len(files[MAIN]) + 50, tool="Bash"))
    with pytest.raises(pp.StepFailed, match="capture_incomplete"):
        h.run.pump()
    assert _gate(run).sent[-1] == {"release": "response-2-2", "verdict": "stop"}
    assert "a1:-:capture_incomplete" in h.ledger.observations("budget")["gate_violations"]
    assert "fence" in _trace(h)


def test_a_stop_verdict_at_a_hold_interrupts_and_fences(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    share = run.reservations[0].output_tokens
    under = {MAIN: _jsonl([_prompt(), _reply("m1", 1, 100, [_tool("t1")])])}
    _feed(run, under)
    # Control: under the share the response is released.
    _gate(run).queue.append(_hold("response", 1, size=len(under[MAIN]), tool="Bash"))
    h.run.pump()
    assert _gate(run).sent[-1] == {"release": "response-1-1", "verdict": "go"}
    over = {MAIN: under[MAIN] + _jsonl([_result("t1", 2), _reply("m2", 3, share, [_tool("t2")])])}
    _feed(run, over)
    _gate(run).queue.append(_hold("response", 2, size=len(over[MAIN]), tool="Bash"))
    with pytest.raises(pp.StepFailed, match="session_share"):
        h.run.pump()
    assert _gate(run).sent[-1] == {"release": "response-2-2", "verdict": "stop"}
    deletes = [argv for argv in h.shell.kubectl() if "delete" in argv and "pod-a1" in argv]
    assert deletes and "--grace-period=0" in deletes[0]
    budget = h.ledger.observations("budget")
    assert budget["allowance_breach"] == ["a1:implementer:session_share"]
    assert budget["no_allowance_breach"] is False
    assert {row["source"] for row in _rows(h, "a1")} == {"unknown"}
    assert "fence" in _trace(h)


def test_the_session_deadline_fires_on_the_fake_clock(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    _feed(run, {MAIN: _jsonl([_prompt(0), _reply("m1", 10, 10, [])])})
    h.clock.current = T0 + 10
    h.run.pump()
    h.clock.current = T0 + pto.SESSION_SECONDS - 1
    h.run.pump()  # control: one second before the deadline nothing stops
    assert "fence" not in _trace(h)
    h.clock.current = T0 + pto.SESSION_SECONDS
    with pytest.raises(pp.StepFailed, match="session_seconds"):
        h.run.pump()
    assert h.ledger.observations("budget")["allowance_breach"] == ["a1:implementer:session_seconds"]
    assert any("delete" in argv and "pod-a1" in argv for argv in h.shell.kubectl())


def test_a_kept_hold_is_stopped_before_the_hook_gives_up(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "b-r1")
    run = _pod_run(h, "b-r1", policy=pt.KEEP_PROMPT)
    files = {MAIN: _jsonl([_prompt()])}
    _feed(run, files)
    _gate(run).queue.append(_hold("prompt", 1, size=len(files[MAIN])))
    h.run.pump()
    assert run.kept is not None and _gate(run).sent == []
    h.clock.wait(pto.HOLD_SECONDS - pt.KEPT_MARGIN_SECONDS - 1)
    h.run.pump()  # control: inside the window the kept hold stays kept
    h.clock.wait(1)
    with pytest.raises(pp.StepFailed, match="kept_hold_expired"):
        h.run.pump()


def test_an_unmapped_session_fences(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a3")
    run = _pod_run(h, "a3", policy=pt.RELEASE)
    files = {MAIN: _jsonl([_prompt(), _reply("m1", 1, 20, [_tool("t1", "Agent")])])}
    _feed(run, files)
    # Control: the main session maps to A3's implementer and its Agent call is released.
    _gate(run).queue.append(_hold("response", 1, size=len(files[MAIN]), tool="Agent", kind="plan"))
    h.run.pump()
    assert _gate(run).sent == [{"release": "response-1-1", "verdict": "go"}]
    assert run.sessions == {MAIN.removesuffix(".jsonl"): run.reservations[0].seq}
    # A3 reserved no plan reviewer: the subagent's first hold has nowhere to map.
    _gate(run).queue.append(_hold("response", 2, size=len(files[MAIN]), agent="x1"))
    with pytest.raises(pp.StepFailed, match="unmapped_session"):
        h.run.pump()
    unreserved = [row for row in h.ledger.session_rows() if row["source"] == "unreserved"]
    assert [(row["grp"], row["role"]) for row in unreserved] == [("a3", pto.PLAN_REVIEWER)]
    assert h.ledger.run_row()["budget_state"] != "ok"
    assert h.ledger.observations("roles")["sessions_mapped"] is False
    assert "fence" in _trace(h)


# -- settlement -----------------------------------------------------------------


def test_unknown_usage_is_charged_and_fences(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    _feed(run, {MAIN: _jsonl([_prompt(), _reply("m1", 1, 30, [])])})
    with pytest.raises(pp.StepFailed, match="usage unknown or unmapped"):
        h.run.settle_run(run, facts=h.run.pod_facts(run), complete=False)
    rows = _rows(h, "a1")
    assert {row["source"] for row in rows} == {"unknown"}
    # Each unknown session is charged its full share, not the 30 tokens it showed.
    probe = sum(row["output_tokens"] for row in _rows(h, "a-probe"))
    assert h.ledger.owner_spent("a") == probe + sum(row["reserved_output"] for row in rows)
    assert "fence" in _trace(h)
    with pytest.raises(pps.Refusal) as refused:
        h.ledger.reserve_group("b-probe", now=h.clock.now())
    assert refused.value.code == "usage_unknown"


def test_complete_capture_settles_known_usage(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    _feed(run, {MAIN: _jsonl([_prompt(), _reply("m1", 1, 30, [])])})
    h.run.settle_run(run, facts=h.run.pod_facts(run), complete=True)
    sources = sorted(row["source"] for row in _rows(h, "a1"))
    assert sources == ["native", "not_started", "not_started"]
    assert h.run.settled_known("a1") and "fence" not in _trace(h)
    classes = h.ledger.observations("budget")["zero_usage_sessions"]
    assert {item["class"] for item in classes} == {"not_started"}


# -- the grant probe ------------------------------------------------------------


def _probe_answer(h: _Harness, body: Mapping[str, Any], returncode: int = 0) -> None:
    base: dict[str, Any] = {
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "num_turns": 1,
        "result": "ok",
        "total_cost_usd": 0.0001,
        "usage": {"output_tokens": 5},
        "modelUsage": {pto.PROBE_MODEL: {}},
    }
    answer = pp.Completed(returncode, json.dumps({**base, **body}).encode(), b"")
    h.shell.on("--model", pto.PROBE_MODEL)(lambda _argv, _stdin: answer)


def test_a_probe_usage_limit_refusal_stops_before_any_reservation(tmp_path: Path) -> None:
    h = _live(tmp_path, begin=False)
    _probe_answer(
        h,
        {
            "is_error": True,
            "terminal_reason": "api_error",
            "api_error_status": 429,
            "result": "API Error: 429 weekly limit reached "
            '{"type":"error","error":{"type":"rate_limit_error"}}',
            "usage": {"output_tokens": 0},
            "modelUsage": {},
        },
        returncode=1,
    )
    with pytest.raises(pp.StepFailed, match="grant probe failed: api_error 429 rate_limit_error"):
        h.run.phase_probe_a()
    assert {row["grp"] for row in h.ledger.session_rows()} == {"a-probe"}
    assert [row["source"] for row in _rows(h, "a-probe")] == ["probe"]
    budget = h.ledger.observations("budget")
    assert budget["probe_a_ok"] is False
    assert budget["probe_a_error"] == "api_error 429 rate_limit_error"
    assert budget["probe_isolated_a"] is True
    result = h.ledger.observations("grant_probe")["result_a"]
    assert "first_line" not in json.dumps(result) and "weekly" not in json.dumps(result)
    assert "fence" in _trace(h)
    # The next phase's reservation is never reached: the run stops on the failure.
    probe = [call for call in h.shell.calls if "--model" in call.argv]
    assert len(probe) == 1 and MARKER_A not in " ".join(probe[0].argv)


def test_a_passing_probe_settles_with_source_probe(tmp_path: Path) -> None:
    h = _live(tmp_path, begin=False)
    _probe_answer(h, {})
    h.run.phase_probe_a()
    budget = h.ledger.observations("budget")
    assert budget["probe_a_ok"] is True and budget["probe_isolated_a"] is True
    assert budget["allowance_breach"] == []
    assert [(row["source"], row["output_tokens"]) for row in _rows(h, "a-probe")] == [("probe", 5)]
    assert h.ledger.run_row()["live_started_at"] == T0
    assert "fence" not in _trace(h)


# -- the disconnect -------------------------------------------------------------

_DISCONNECT_BREAKS: dict[str, dict[str, Any]] = {
    "disconnect_inactive": {"disconnect": {"inactive": False}},
    "disconnect_generation_advanced": {"disconnect": {"generation_advanced": False}},
    "teardown_pending_runtimes": {"disconnect": {"pending_runtimes": 1}},
    "teardown_pending_custody_cleanup": {"disconnect": {"pending_custody_cleanup": None}},
    "a_custody_secret_absent": {"disconnect": {"custody_secret_absent": False}},
    "a3_failed_enrollment_fenced": {"row": {"terminal_cause": "cancelled"}},
    "a3_termination_observation": {"row": {"observed": False}},
    "a3_pod_absent": {"pod_present": True},
    "a3_tokens_secret_absent": {"secret": True},
    "a_claims_absent": {"claims": ["claim-a9"]},
}


def _disconnect_setup(h: _Harness, breaks: Mapping[str, Any]) -> None:
    _through(h, "a3")
    _through(h, "b-r1")
    held = _pod_run(h, "b-r1", policy=pt.KEEP_PROMPT)
    held.request_id = REQUEST_B1
    follow = _pod_run(h, "a3", policy=pt.KEEP_AFTER_TOOL)
    follow.request_id = REQUEST_A1
    records = [_prompt(), _reply("m1", 1, 40, [_tool("t1")]), _result("t1", 2)]
    records.append(_reply("m2", 3, 20, [_tool("t2")]))
    files = {MAIN: _jsonl(records)}
    _feed(follow, files)
    follow.kept = _hold("response", 3, size=len(files[MAIN]), tool="Bash")["hold"]
    follow.kept_at = h.clock.now()
    follow.kept_files, follow.kept_facts = files, h.run.pod_facts(follow, files)
    follow.eof.set()
    disconnect = {
        "inactive": True,
        "generation_advanced": True,
        "pending_runtimes": 0,
        "pending_custody_cleanup": 0,
        "custody_secret_absent": True,
        **breaks.get("disconnect", {}),
    }
    row = {"status": "failed", "terminal_cause": "enrollment_fenced", "observed": True}
    row.update(breaks.get("row", {}))
    h.run.disconnect_owner = lambda _owner: dict(disconnect)  # type: ignore[method-assign]
    h.run.request_row = lambda _request: dict(row)  # type: ignore[method-assign]
    h.run.pod_uid_present = lambda _n, _u: bool(breaks.get("pod_present"))  # type: ignore[method-assign]
    h.run.owner_claims = lambda _owner: list(breaks.get("claims", []))  # type: ignore[method-assign]
    if breaks.get("secret"):
        h.shell.on("get", "secret", "claim-a3-tokens")(lambda _a, _s: b"secret/claim-a3-tokens\n")


def test_the_disconnect_confirms_every_key(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _disconnect_setup(h, {})
    h.run.phase_disconnect_a()
    observed = h.ledger.observations("disconnect")
    for key in _DISCONNECT_BREAKS:
        expected = 0 if key.startswith("teardown_") else True
        assert observed[key] == expected and type(observed[key]) is type(expected), key
    assert observed["a3_settled_from_hold"] is True
    assert observed["b_grant_unchanged"] is True
    assert h.run.a_stop_at == h.clock.now()
    assert [row["source"] for row in _rows(h, "a3")] == ["native"]
    assert "fence" not in _trace(h)


@pytest.mark.parametrize("key", sorted(_DISCONNECT_BREAKS))
def test_the_disconnect_fails_when_a_confirmation_is_missing(tmp_path: Path, key: str) -> None:
    h = _live(tmp_path)
    _disconnect_setup(h, _DISCONNECT_BREAKS[key])
    with pytest.raises(pp.StepFailed, match=key):
        h.run.phase_disconnect_a()
    observed = h.ledger.observations("disconnect")[key]
    assert observed is not True and not (type(observed) is int and observed == 0)
    assert "fence" in _trace(h)


# -- CI repair --------------------------------------------------------------------


def _ci_runs(conclusion: str, status: str = "completed") -> dict[str, Any]:
    return {
        "total_count": 1,
        "check_runs": [
            {
                "id": 31,
                "name": "unit",
                "status": status,
                "conclusion": conclusion if status == "completed" else None,
                "app": {"slug": pto.ACTIONS_APP},
            }
        ],
    }


def _ci_setup(h: _Harness, *, failing: bool = True, event: bool = True) -> None:
    _through(h, "b-r1")
    first = _pod_run(h, "b1", card=3, reserve=False)
    first.request_id = REQUEST_B1
    h.run.b1_head = HEAD_B1
    runs = _ci_runs("failure" if failing else "success")
    answers = {
        f"/commits/{HEAD_B1}/check-runs?filter=all&per_page=100": runs,
        f"/commits/{HEAD_B1}/check-runs?filter=latest&per_page=100": runs,
        "/check-runs/31/annotations": [{"path": "CI_ACK.md", "message": "missing line"}],
    }
    h.run.forge_get = lambda suffix: answers[suffix]  # type: ignore[method-assign]
    h.run.phase_notes = lambda _r: [pt.RERUN_FORBIDDEN_NOTE]  # type: ignore[method-assign]
    payload = json.dumps({"event_id": f"work-item-{REQUEST_B1}-ci-2"})
    entries = [("1-0", {pto.STREAM_PAYLOAD_FIELD: payload})] if event else []
    h.run.valkey_xrange = lambda *_a, **_k: list(entries)  # type: ignore[method-assign]
    h.run.request_row = lambda _r: {"heartbeat_parked": True}  # type: ignore[method-assign]


def test_the_ci_failure_is_observed_from_real_reads(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _ci_setup(h)
    h.run.observe_ci_failure()
    observed = h.ledger.observations("ci-repair")
    for key in (
        "b1_head_check_failed",
        "b1_failure_names_ci_ack",
        "rerun_settled",
        "round2_event_enqueued",
        "round2_heartbeat_parked",
    ):
        assert observed[key] is True, key
    assert h.run.b1_check_names == ["unit"]


def test_the_ci_phase_fails_when_the_head_check_is_not_failing(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _ci_setup(h, failing=False)
    with pytest.raises(pp.StepFailed, match="no failing Actions check"):
        h.run.observe_ci_failure()
    assert h.ledger.observations("ci-repair")["b1_head_check_failed"] is False
    assert "fence" in _trace(h)


def test_the_ci_phase_fails_without_the_round_two_event(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _ci_setup(h, event=False)
    with pytest.raises(pp.StepFailed, match="round2_event_enqueued"):
        h.run.observe_ci_failure()
    assert h.ledger.observations("ci-repair")["round2_event_enqueued"] is False


def _repaired_setup(h: _Harness, runs: Any, *, required: str = "unit") -> None:
    h.run.default_branch = "main"
    h.run.b1_check_names = ["unit"]
    rules = [
        {
            "type": "required_status_checks",
            "parameters": {"required_status_checks": [{"context": required}]},
        }
    ]
    answers = {
        f"/commits/{HEAD_B2}/check-runs?filter=latest&per_page=100": runs,
        f"/commits/{HEAD_B2}/status": {"state": "success", "statuses": []},
        "/rules/branches/main": rules,
        "/branches/main": {"protection": {}},
    }
    h.run.forge_get = lambda suffix: answers[suffix]  # type: ignore[method-assign]
    ack = f"ci ack: {BRANCH_B}\n".encode()
    h.run.forge_file = lambda path, ref: ack if path == pto.CI_ACK_FILE else None  # type: ignore[method-assign]


def test_the_repaired_head_passes_with_every_required_check(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _repaired_setup(h, _ci_runs("success"))
    assert h.run.repaired_checks(HEAD_B2, BRANCH_B) == {
        "repaired_head_adds_ci_ack": True,
        "repaired_checks_passed": True,
        "required_checks_present": True,
    }


def test_the_repaired_head_fails_with_a_pending_check(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _repaired_setup(h, _ci_runs("success", status="in_progress"))
    checks = h.run.repaired_checks(HEAD_B2, BRANCH_B)
    assert checks["repaired_checks_passed"] is False
    assert checks["required_checks_present"] is False
    assert h.clock.now() >= T0 + pp.CI_WAIT_SECONDS


def test_the_repaired_head_fails_with_a_missing_required_check(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _repaired_setup(h, _ci_runs("success"), required="integration")
    checks = h.run.repaired_checks(HEAD_B2, BRANCH_B)
    assert checks["repaired_checks_passed"] is True
    assert checks["required_checks_present"] is False


# -- after the disconnect ---------------------------------------------------------


def _post_setup(h: _Harness, *, a_claims: list[str]) -> None:
    follow = _pod_run(h, "a3", reserve=False)
    follow.request_id, follow.work_item_id = REQUEST_A1, WORK_ITEM
    first = _pod_run(h, "b1", reserve=False)
    first.request_id = REQUEST_B1
    h.run.stop_worker = lambda: ["uid-w2"]  # type: ignore[method-assign]
    h.run.restart_worker = lambda before: {"new_worker_uid": True, "candidate_image": True}  # type: ignore[method-assign]
    h.api.route("POST", "/v1/internal/work-items/admissions")(
        lambda _p, _h, _b: (409, {"detail": {"code": "enrollment_inactive"}})
    )
    h.api.route("GET", f"/work-items/{WORK_ITEM}")(
        lambda _p, _h, _b: (200, {"requests": [{"id": REQUEST_A1, "status": "failed"}]})
    )
    h.run.owner_claims = (  # type: ignore[method-assign]
        lambda owner: list(a_claims) if owner.name == "a" else ["claim-b1"]
    )
    rows = [{"id": REQUEST_A1, "sequence": 3, "owner_lost_retry": False}]
    h.run.request_rows = lambda _w: list(rows)  # type: ignore[method-assign]
    h.run.revalidate = lambda _owner, _run: True  # type: ignore[method-assign]
    active = _enrollment("active", 1, 1)
    h.run.subscription = lambda owner, verb, *extra: (0, dict(active))  # type: ignore[method-assign]
    h.run.valkey_xrange = lambda *_a, **_k: []  # type: ignore[method-assign]
    h.run.agent_claims = lambda: []  # type: ignore[method-assign]


def test_post_disconnect_refusals_pass_and_close_the_live_window(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _post_setup(h, a_claims=[])
    h.run.phase_post_disconnect()
    observed = h.ledger.observations("disconnect")
    for key in (
        "post_restart_admission_refused",
        "post_restart_no_a_claim",
        "post_restart_no_a_running",
        "post_restart_no_a3_successor",
        "post_restart_revalidation_refused",
        "b_active_gen1_after",
    ):
        assert observed[key] is True, key
    assert h.ledger.observations("budget")["unplanned_starts_fenced"] is True
    assert h.ledger.observations("ci-repair")["no_round3"] is True
    assert _trace(h)[-2:] == ["fence", "summary"]
    assert h.ledger.run_row()["live_ended_at"] is not None


def test_post_disconnect_fails_if_an_a_claim_appears(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _post_setup(h, a_claims=["claim-a9"])
    with pytest.raises(pp.StepFailed, match="post_restart_no_a_claim"):
        h.run.phase_post_disconnect()
    assert h.ledger.observations("disconnect")["post_restart_no_a_claim"] is False
    assert "fence" in _trace(h)


def test_summary_keys_read_the_gathered_evidence(tmp_path: Path) -> None:
    h = _live(tmp_path)
    _through(h, "a1")
    run = _pod_run(h, "a1", card=1)
    files = {MAIN: _jsonl([_prompt(), _reply("m1", 1, 30, [_tool("t1", "mcp__curie__x")])])}
    _feed(run, files)
    _gate(run).queue.append(_hold("prompt", 1, size=len(files[MAIN])))
    h.run.pump()
    h.run.settle_run(run, facts=h.run.pod_facts(run), complete=True)
    h.run.owners["a"].work_item_ids = [WORK_ITEM]
    item = _admission_answer("a", REQUEST_A1)
    work_item = {**item["work_item"], "requests": [item["request"]]}
    h.api.route("GET", f"/work-items/{WORK_ITEM}")(lambda _p, _h, _b: (200, work_item))
    h.shell.on("work-items", WORK_ITEM)(lambda _a, _s: {"item": work_item})
    h.ledger.record("owners", "installation_grant_absent", True)
    h.ledger.record("owners", "service_credentials_absent", True)
    pt.TwoOwnerRun.record_summary_keys(h.run)
    owners = h.ledger.observations("owners")
    assert owners["work_item_owner_a"] is True and owners["request_owners_a"] is True
    assert owners["cli_owner_a"] is True
    roles = h.ledger.observations("roles")
    assert roles["implementer_session_a"] is True and roles["aci_tool_observed_a"] is True
    # No plan or diff reviewer ran in this one-session fixture.
    assert roles["plan_reviewer_session_a"] is False
    assert roles["sessions_mapped"] is True
    budget = h.ledger.observations("budget")
    assert budget["limits_written_before_prompt"] is True
    assert budget["gate_every_response"] is True and budget["no_allowance_breach"] is True
    assert budget["zero_usage_classified"] is True
    # B never ran, so the per-claim grant reference was read for one owner only.
    assert budget["no_credential_fallback"] is False
    # Cards 2 to 4 never parked here.
    assert h.ledger.observations("approval")["no_push_before_approval"] is False
