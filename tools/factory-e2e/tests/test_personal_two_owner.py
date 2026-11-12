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

import contextlib
import dataclasses
import hashlib
import http.server
import io
import json
import os
import tarfile
import threading
import urllib.parse
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
import personal_two_owner_support as pto
import pytest

REPO_ROOT = pp.REPO_ROOT
T0 = 1_800_000_000.0
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
RUN_ID = "acme-two-owner-1"
TAG = pps.image_tag(CANDIDATE, RUN_ID)
MARKER_A = "synthetic-owner-a-marker-not-a-token"
MARKER_B = "synthetic-owner-b-marker-not-a-token"
SLACK_MARKER = "xoxb-synthetic-gateway-marker-not-a-token"
SESSION_MARKER = "session-marker-0001"
STATE_MARKER = "state-marker-0001"
CHANNEL = "C0EXAMPLE1"
REPO = "acme-corp/acme-fixture"
CONTEXT = "kind-acme-test"
NAMESPACE = "pto-acme-1"
RELEASE = "pto-acme"
FULLNAME = pps.chart_fullname(RELEASE)
REGISTRY = "localhost:5999"
WINDOW = T0 + pto.GRANT_MIN_LIFETIME + 3600
TENANT = pps.DEFAULT_TENANT
PRINCIPAL_A = "5d0c7f3e-0000-4000-8000-00000000a001"
PRINCIPAL_B = "5d0c7f3e-0000-4000-8000-00000000b002"
AGENT_ID = "5d0c7f3e-0000-4000-8000-0000000a9e01"
WORK_ITEM = "5d0c7f3e-0000-4000-8000-0000000c0001"
DIGESTS = {
    name: "sha256:" + hashlib.sha256(name.encode()).hexdigest() for name in pps.REQUIRED_IMAGES
}
DEX_DIGEST = "sha256:" + "d" * 64
API_PORT = 41001
DEX_PORT = 41002
PATH_ONLY = {"PATH": "/nonexistent"}


# --------------------------------------------------------------------------
# Builders (no provisioning fixtures)
# --------------------------------------------------------------------------


def _private(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _file(path: Path, content: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(content)
    os.chmod(path, mode)
    return path


def _kit(tmp_path: Path, *, registry: str = REGISTRY, prior: int = 0) -> dict[str, Any]:
    """A valid live inputs document (personal_two_owner_support.load_inputs_file)."""

    kit = _private(tmp_path / "kit")
    app = _private(kit / "app")
    (app / "app.json").write_text(json.dumps({"id": 42, "installation_id": 7, "repo": REPO}))
    (app / "app.pem").write_text("placeholder, never read here\n")
    kubeconfig = _file(kit / "kubeconfig", b"apiVersion: v1\nkind: Config\n")
    slack = _file(kit / "slack-bot", SLACK_MARKER.encode() + b"\n")
    grant_a = _file(kit / "grant-a", MARKER_A.encode() + b"\n")
    grant_b = _file(kit / "grant-b", MARKER_B.encode() + b"\n")
    return {
        "target": {
            "context": CONTEXT,
            "namespace": NAMESPACE,
            "release": RELEASE,
            "registry": registry,
            "docker_context": "acme-docker",
            "kubeconfig": str(kubeconfig),
        },
        "forge": {"app_dir": str(app)},
        "gateway": {"slack_bot_token_file": str(slack), "resolution_channel": CHANNEL},
        "owners": {
            "a": {"grant_file": str(grant_a), "usable_until": int(WINDOW)},
            "b": {"grant_file": str(grant_b), "usable_until": int(WINDOW)},
        },
        "independent_accounts": True,
        "prior_item_output_tokens": prior,
        "probe_cli": "claude",
    }


def _setup_document(tmp_path: Path, **kwargs: Any) -> dict[str, Any]:
    document = _kit(tmp_path, **kwargs)
    for key in ("owners", "independent_accounts"):
        document.pop(key)
    return document


def _inputs_file(tmp_path: Path, document: Mapping[str, Any]) -> Path:
    folder = _private(tmp_path / "inputs")
    return _file(folder / "two-owner.json", json.dumps(document).encode())


class _FakeClock(pp.Clock):
    """Waiting advances time; nothing sleeps."""

    def __init__(self, now: float = T0) -> None:
        self.current = now
        self.ticks = 0.0

    def now(self) -> float:
        return self.current

    def monotonic(self) -> float:
        return self.ticks

    def wait(self, seconds: float) -> None:
        self.current += max(0.0, seconds)
        self.ticks += max(0.0, seconds)


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


def _refusal(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(lines[-1])
    assert isinstance(payload, dict)
    return payload


# --------------------------------------------------------------------------
# A loopback registry (the pp tests' recorded registry:2.8.3 shapes)
# --------------------------------------------------------------------------


def _image(revision: str) -> tuple[bytes, bytes]:
    """Docker manifest v2 schema 2 plus its config blob (distribution spec manifest-v2-2)."""

    config = json.dumps(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {pps.REVISION_LABEL: revision}},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "1" * 64]},
        }
    ).encode()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "size": len(config),
                "digest": "sha256:" + hashlib.sha256(config).hexdigest(),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 32,
                    "digest": "sha256:" + "2" * 64,
                }
            ],
        }
    ).encode()
    return manifest, config


@contextlib.contextmanager
def _registry(images: Mapping[tuple[str, str], tuple[bytes, bytes]]) -> Iterator[int]:
    """Registry HTTP API v2 GETs for manifests (by tag or digest) and config blobs."""

    manifests = {key: value[0] for key, value in images.items()}
    blobs = {
        "sha256:" + hashlib.sha256(config).hexdigest(): config for _m, config in images.values()
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802 - http.server naming
            parts = self.path.split("/")
            body: bytes | None = None
            headers: dict[str, str] = {}
            if len(parts) == 5 and parts[1] == "v2" and parts[3] == "manifests":
                found = manifests.get((parts[2], parts[4]))
                accepted = [item.strip() for item in self.headers.get("Accept", "").split(",")]
                media_type = json.loads(found).get("mediaType") if found else None
                if found and media_type in accepted:
                    body = found
                    headers = {
                        "Content-Type": str(media_type),
                        "Docker-Content-Digest": "sha256:" + hashlib.sha256(body).hexdigest(),
                    }
            elif len(parts) == 5 and parts[1] == "v2" and parts[3] == "blobs":
                body = blobs.get(parts[4])
            if body is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def _candidate_registry(revision: str = CANDIDATE) -> dict[tuple[str, str], tuple[bytes, bytes]]:
    images = {(name, TAG): _image(revision) for name in pps.REQUIRED_IMAGES}
    dex = _image("dex-upstream")
    images[(pt.DEX_REPOSITORY, "sha256:" + hashlib.sha256(dex[0]).hexdigest())] = dex
    return images


def _record_dex(state: Path, *, digest: str) -> None:
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID)
    ledger.ensure_run(CANDIDATE, phase="images")
    ledger.record(
        "images",
        "dex",
        {"source": pt.DEX_SOURCE, "repository": pt.DEX_REPOSITORY, "digest": digest, "tag": TAG},
    )
    ledger.close()


# --------------------------------------------------------------------------
# Command path: refusals through the real main, each with its control
# --------------------------------------------------------------------------


def _argv(command: str, inputs: Path | None, state: Path, *extra: str) -> list[str]:
    argv = [command, "--scenario", "two-owner"]
    if inputs is not None:
        argv += ["--inputs-file", str(inputs)]
    return [*argv, "--candidate", CANDIDATE, "--state-dir", str(state), "--run-id", RUN_ID, *extra]


def _main(
    argv: list[str], capsys: pytest.CaptureFixture[str], environ: Mapping[str, str] = PATH_ONLY
) -> tuple[int, dict[str, Any]]:
    code = pp.main(argv, environ=dict(environ), clock=_FakeClock())
    return code, _refusal(capsys)


APPROVE = ("--approve-live", pto.approved_ceiling())


def test_preflight_passes_with_candidate_images_and_the_mirrored_dex(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    images = _candidate_registry()
    dex_digest = next(ref for (repo, ref) in images if repo == pt.DEX_REPOSITORY)
    with _registry(images) as port:
        state = _private(tmp_path / "state")
        _record_dex(state, digest=dex_digest)
        inputs = _inputs_file(tmp_path, _kit(tmp_path, registry=f"localhost:{port}"))
        code, result = _main(_argv("preflight", inputs, state, *APPROVE), capsys)
    assert code == 0, result
    assert result["verdict"] == "pass" and result["scenario"] == "two-owner"
    assert all(result["checks"].values())
    assert {row["owner"] for row in result["schedule"]["groups"]} == {"a", "b"}
    ledger = pto.TwoOwnerLedger.open_existing(state, RUN_ID)
    try:
        dex_ref = f"localhost:{port}/{pt.DEX_REPOSITORY}@{dex_digest}"
        assert ledger.observations("images")["preflight_dex"] == dex_ref
    finally:
        ledger.close()


@pytest.mark.parametrize(
    ("served", "dex", "detail"),
    [("other", "served", "curie-"), ("candidate", "missing", "dex")],
)
def test_preflight_refuses_a_candidate_or_dex_image_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], served: str, dex: str, detail: str
) -> None:
    images = _candidate_registry(OTHER_SHA if served == "other" else CANDIDATE)
    dex_digest = next(ref for (repo, ref) in images if repo == pt.DEX_REPOSITORY)
    with _registry(images) as port:
        state = _private(tmp_path / "state")
        _record_dex(state, digest=dex_digest if dex == "served" else "sha256:" + "e" * 64)
        inputs = _inputs_file(tmp_path, _kit(tmp_path, registry=f"localhost:{port}"))
        code, result = _main(_argv("preflight", inputs, state, *APPROVE), capsys)
    assert code == 2 and result["error"] == "candidate_image_mismatch", result
    assert detail in result["fix"]


@pytest.mark.parametrize("command", ["preflight", "run"])
def test_live_commands_refuse_a_setup_descriptor(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = _private(tmp_path / "state")
    setup = _inputs_file(tmp_path / "setup", _setup_document(tmp_path / "setup"))
    code, result = _main(_argv(command, setup, state, *APPROVE), capsys)
    assert code == 2 and result["error"] == "independence_unasserted"
    # Control: the live descriptor gets past every input refusal.
    with _registry({}) as port:
        live = _inputs_file(
            tmp_path / "live", _kit(tmp_path / "live", registry=f"localhost:{port}")
        )
        code, result = _main(_argv(command, live, state, *APPROVE), capsys)
    assert result.get("error") != "independence_unasserted"
    if command == "run":
        assert code == 1 and result["phase"] == "refusals" and result["error"] == "step_failed"
        ledger = pto.TwoOwnerLedger.open_existing(state, RUN_ID)
        try:
            assert ledger.observations("budget")["schedule_check_passed"] is True
            assert not ledger.is_rehearsal()
        finally:
            ledger.close()


@pytest.mark.parametrize("command", ["images", "forge-probe", "rehearse"])
def test_setup_commands_refuse_a_live_descriptor(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = _private(tmp_path / "state")
    live = _inputs_file(tmp_path / "live", _kit(tmp_path / "live"))
    code, result = _main(_argv(command, live, state), capsys)
    assert code == 2 and result["error"] == "setup_inputs_carry_grants"
    # Control: a setup descriptor is accepted and the command reaches its first
    # child, which this PATH lacks (git, openssl or kubectl): a step failure.
    setup = _inputs_file(tmp_path / "setup", _setup_document(tmp_path / "setup"))
    code, result = _main(_argv(command, setup, state), capsys)
    assert code == 1 and result.get("error", "step_failed") == "step_failed", result
    if command == "rehearse":
        ledger = pto.TwoOwnerLedger.open_existing(state, RUN_ID)
        try:
            assert ledger.is_rehearsal()
            assert ledger.observations("rehearsal")["schedule_check_passed"] is True
        finally:
            ledger.close()


@pytest.mark.parametrize(
    ("command", "flag", "value"),
    [
        ("images", "--context", CONTEXT),
        ("forge-probe", "--app-dir", "app"),
        ("rehearse", "--resolution-channel", CHANNEL),
        ("rehearse", "--grant-file", "grant"),
        ("preflight", "--output-token-ceiling", "32768"),
        ("run", "--item-output-tokens-spent", "0"),
        # Equal to its default, still a single-profile flag.
        ("run", "--grant-probe-cli", pp.DEFAULT_PROBE_CLI),
    ],
)
def test_single_profile_flags_are_refused_with_the_scenario(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, flag: str, value: str
) -> None:
    state = _private(tmp_path / "state")
    live = command in {"preflight", "run"}
    approve = APPROVE if live else ()
    with _registry({}) as port:
        build = _kit if live else _setup_document
        inputs = _inputs_file(tmp_path, build(tmp_path, registry=f"localhost:{port}"))
        code, result = _main(_argv(command, inputs, state, *approve, flag, value), capsys)
        assert code == 2 and result["error"] == "arguments_invalid" and flag in result["fix"]
        code, result = _main(_argv(command, inputs, state, *approve), capsys)
    assert result.get("error") != "arguments_invalid", result


def test_inputs_file_is_a_two_owner_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    inputs = _inputs_file(tmp_path, _setup_document(tmp_path))
    argv = ["judge", "--inputs-file", str(inputs), "--candidate", CANDIDATE]
    argv += ["--state-dir", str(state), "--run-id", RUN_ID, "--criterion", "roles"]
    code, result = _main(argv, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"
    # Control: without it the single-profile judge runs as before (no ledger yet).
    code, result = _main([argv[0], *argv[3:]], capsys)
    assert result["error"] != "arguments_invalid"


@pytest.mark.parametrize("command", ["preflight", "run"])
@pytest.mark.parametrize("approval", [None, "approve everything"])
def test_live_commands_need_the_exact_live_approval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, approval: str | None
) -> None:
    state = _private(tmp_path / "state")
    extra = () if approval is None else ("--approve-live", approval)
    with _registry({}) as port:
        inputs = _inputs_file(tmp_path, _kit(tmp_path, registry=f"localhost:{port}"))
        code, result = _main(_argv(command, inputs, state, *extra), capsys)
        assert code == 2 and result["error"] == "live_not_approved"
        code, result = _main(_argv(command, inputs, state, *APPROVE), capsys)
    assert result.get("error") != "live_not_approved"


def test_rehearse_refuses_a_live_approval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    inputs = _inputs_file(tmp_path, _setup_document(tmp_path))
    code, result = _main(_argv("rehearse", inputs, state, *APPROVE), capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


@pytest.mark.parametrize("command", ["preflight", "run", "rehearse"])
def test_an_impossible_schedule_is_refused_before_any_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = _private(tmp_path / "state")
    build = _setup_document if command == "rehearse" else _kit
    approve = () if command == "rehearse" else APPROVE
    inputs = _inputs_file(tmp_path / "high", build(tmp_path / "high", prior=12_000))
    code, result = _main(_argv(command, inputs, state, *approve), capsys)
    assert code == 2 and result["error"] == "session_plan_impossible"
    assert not (state / f"{RUN_ID}.work").exists()
    with _registry({}) as port:
        low = build(tmp_path / "low", prior=4_000, registry=f"localhost:{port}")
        code, result = _main(
            _argv(command, _inputs_file(tmp_path / "low", low), state, *approve), capsys
        )
    assert result.get("error") != "session_plan_impossible"


@pytest.mark.parametrize("command", ["images", "preflight", "rehearse", "run", "judge"])
@pytest.mark.parametrize(
    ("variable", "code"),
    [("ANTHROPIC_API_KEY", "service_credentials"), (pps.FAKE_MODEL_NAME, "fake_mode")],
)
def test_ambient_credentials_and_fake_mode_are_refused_first(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
    variable: str,
    code: str,
) -> None:
    state = _private(tmp_path / "state")
    live = command in {"preflight", "run"}
    with _registry({}) as port:
        build = _kit if live else _setup_document
        inputs = _inputs_file(tmp_path, build(tmp_path, registry=f"localhost:{port}"))
        argv = _argv(command, None if command == "judge" else inputs, state)
        argv += [*APPROVE] if live else []
        if command == "judge":
            argv += ["--criterion", "rehearsal"]
        exit_code, result = _main(argv, capsys, {**PATH_ONLY, variable: "marker-value-1"})
        assert exit_code == 2 and result["error"] == code
        assert "marker-value-1" not in json.dumps(result)
        _exit, control = _main(argv, capsys)
    assert control.get("error") not in {"service_credentials", "fake_mode"}


def test_grant_probe_is_refused_for_the_scenario(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    code, result = _main(_argv("grant-probe", None, state), capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


# --------------------------------------------------------------------------
# judge: receipts only
# --------------------------------------------------------------------------


def _rehearsal_ledger(state: Path, *, drop: str | None = None) -> None:
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID)
    ledger.ensure_run(CANDIDATE, phase="rehearse")
    ledger.mark_rehearsal()
    for key in pto.REHEARSAL_EXPECTED:
        if key != drop:
            ledger.record("rehearsal", key, 403 if key == "card_unlisted_status" else True)
    ledger.close()


def _judge_argv(state: Path, criterion: str, candidate: str = CANDIDATE) -> list[str]:
    return [
        "judge",
        "--scenario",
        "two-owner",
        "--candidate",
        candidate,
        "--state-dir",
        str(state),
        "--run-id",
        RUN_ID,
        "--criterion",
        criterion,
    ]


def test_judge_reads_a_complete_rehearsal_ledger_and_nothing_else(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    _rehearsal_ledger(state)
    # No inputs file, no tools on PATH, no credential: receipts only.
    code, result = _main(_judge_argv(state, "rehearsal"), capsys)
    assert code == 0 and result["verdict"] == "pass" and result["scenario"] == "two-owner"
    assert result["checks"]["no_native_sessions"] is True
    code, result = _main(_judge_argv(state, "owners"), capsys)
    assert code == 1 and result["checks"] == {"live_ledger": False}
    code, result = _main(_judge_argv(state, "rehearsal", OTHER_SHA), capsys)
    assert code == 2 and result["error"] == "candidate_mismatch"


def test_judge_fails_a_rehearsal_ledger_missing_one_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    _rehearsal_ledger(state, drop="admission_controls_refused")
    code, result = _main(_judge_argv(state, "rehearsal"), capsys)
    assert code == 1 and result["checks"]["admission_controls_refused"] is False


def test_judge_refuses_a_single_profile_ledger_and_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    ledger = pps.Ledger.open(state, RUN_ID)
    ledger.ensure_run(CANDIDATE, phase="run")
    ledger.close()
    code, result = _main(_judge_argv(state, "rehearsal"), capsys)
    assert code == 2 and result["error"] == "ledger_missing"
    inputs = _inputs_file(tmp_path, _setup_document(tmp_path))
    argv = [*_judge_argv(state, "rehearsal"), "--inputs-file", str(inputs)]
    code, result = _main(argv, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"
    # A two-owner criterion is not a single-profile one.
    single = [arg for arg in _judge_argv(state, "owners") if arg not in {"--scenario", "two-owner"}]
    code, result = _main(single, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


def test_parser_criteria_cover_every_two_owner_criterion() -> None:
    choices = {*pps.CRITERIA, *pp.TWO_OWNER_CRITERIA}
    assert {*pto.CRITERIA, "rehearsal"} <= choices


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
    for _name, phase in h.run.phases()[len(pt.UNPAID_PHASES) :]:
        with pytest.raises(pp.StepFailed, match=pt.NOT_IMPLEMENTED):
            phase()


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
