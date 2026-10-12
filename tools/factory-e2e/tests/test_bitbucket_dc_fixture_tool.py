"""Pure unit tests of the Bitbucket Data Center forge fixture (#4401 decision 14).

``tools/forge-fixtures/bitbucket-dc/fixture.py`` is loaded by path. Nothing
here starts a container or reaches the network: every external command and
every HTTP request goes through two seams the fixture takes as keyword
arguments, and the tests hand it fakes. The seams, which the implementation
follows:

``main(argv: list[str], *, runner: Runner, urlopen: Opener) -> int``
    ``argv`` is everything after the script name, for example
    ``["up", "--state-dir", D, "--port", "27443"]``. It returns the exit code
    and does not raise for a fixture failure: the reason goes to stderr. The
    script's ``__main__`` block calls it with the real runner and
    ``urllib.request.urlopen``. ``CURIE_FORGE_FIXTURE_FAIL_AT`` is read from
    ``os.environ``.

``runner(argv, *, env=None, input=None, timeout=None, cwd=None)``
    Runs every external command (``docker``, ``docker compose``, ``openssl``,
    ``git``) and returns ``subprocess.CompletedProcess[str]`` with text
    ``stdout`` and ``stderr``. It never raises on a non-zero exit; the fixture
    checks ``returncode``. A secret travels in ``env`` or ``input``, never in
    ``argv``.

``urlopen(request, *, context=None, timeout=None)``
    The ``urllib.request.urlopen`` signature. Every HTTP request goes through
    it: Atlassian's timebomb license page, ``/status`` and the REST seeding.
    A non-2xx answer raises ``urllib.error.HTTPError``.

Other fixed points: the required tools are found with ``shutil.which``; the
local port probe binds ``("0.0.0.0", port)``; the daemon probe is
``docker ps ... --filter publish=<port>``; the license reaches the Bitbucket
container only through the ``env`` of the ``docker compose ... create`` call
(the rendered compose file names the variable, never the value).
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO_ROOT / "tools" / "forge-fixtures" / "bitbucket-dc"
FIXTURE_PATH = FIXTURE_DIR / "fixture.py"

LICENSE_PAGE = (
    "https://developer.atlassian.com/platform/marketplace/"
    "timebomb-licenses-for-testing-server-apps/"
)
FAIL_AT_ENV = "CURIE_FORGE_FIXTURE_FAIL_AT"
DEFAULT_PORT = 27443
PHASES = ("license", "certs", "create", "start", "ready", "seed", "state")
PROJECT_RE = re.compile(r"^curie-forge-bbdc-[0-9a-f]{8}$")
LABEL_PREFIX = "label=com.docker.compose.project="

# A synthetic license, shaped like Atlassian's (base64 lines, an X02 tail).
# It is not a real license and only has to be recognisable in every place it
# must not appear.
LICENSE_LINES = [
    "AAABfakeBitbucketDcTimebombLicenseForUnitTestsOnly0000000000000000000a",
    "QXRsYXNzaWFuVGltZWJvbWJGYWtlTGljZW5zZUxpbmVUd29Gb3JUaGVVbml0VGVzdHM1",
    "MC0CFQCfakeSignatureForTheBitbucketDcFixtureToolTests==X02q7",
]
LICENSE = "\n".join(LICENSE_LINES)
LICENSE_MARK = "BitbucketDcTimebombLicenseForUnitTestsOnly"
OTHER_LICENSE = "AAABotherProductLicenseThatMustNotBePicked==X02zz"

FAKE_TOKEN = "BBDC-faketokenvalue-for-unit-tests-0123456789abcdef"
FAKE_SHA = "0123456789abcdef0123456789abcdef01234567"
USER_IDS = {"admin": 1, "reviewer": 2, "outsider": 3}


def _code_block(heading: str, body: str) -> str:
    # The real page's shape (fetched 2026-10-12): a bold heading paragraph, then
    # a <pre> of line numbers, then the license in <pre><code>.
    numbers = "\n".join(str(i) for i in range(1, body.count("\n") + 2))
    return (
        f"<p><strong>{heading}</strong></p>\n"
        '<pre><div class="sc-a"><div class="sc-b">'
        f'<pre>{numbers}</pre><pre><code class="bash">{body}\n</code></pre>'
        "</div></div></pre>\n"
    )


def license_page_html() -> str:
    # The page also carries the markdown source JSON-escaped in a script tag,
    # before the rendered HTML, exactly as the real one does.
    markdown = (
        "**10 user Bitbucket Data Center license, expires in 3 hours**\n\n```bash\n"
        + LICENSE
        + "\n```\n"
    )
    next_data = json.dumps({"props": {"pageProps": {"markdown": markdown}}})
    return (
        "<html><head>"
        f'<script id="__NEXT_DATA__" type="application/json">{next_data}</script>'
        "</head><body><h1>Timebomb licenses for testing</h1>\n"
        + _code_block("10 user Confluence Data Center license, expires in 3 hours", OTHER_LICENSE)
        + _code_block("10 user Bitbucket Data Center license, expires in 3 hours", LICENSE)
        + _code_block("10 user Bamboo Data Center license, expires in 24 hours", OTHER_LICENSE)
        + "</body></html>"
    )


def _squash(text: str) -> str:
    return re.sub(r"\s+", "", text)


# --------------------------------------------------------------------------
# Fakes


class FakeResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

    def read(self, *_: Any) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class FakeHttp:
    """Atlassian's license page plus a stateful-enough Bitbucket DC REST API."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.license_failure: str | None = None  # "unreachable", "http", "no-block"

    def __call__(
        self,
        request: urllib.request.Request | str,
        data: bytes | None = None,
        timeout: float | None = None,
        *,
        context: Any = None,
    ) -> FakeResponse:
        if isinstance(request, str):
            request = urllib.request.Request(request, data=data)
        raw = request.data if isinstance(request.data, bytes) else None
        try:
            body: Any = json.loads(raw) if raw else None
        except ValueError:
            body = raw.decode(errors="replace") if raw else None
        url = request.full_url
        method = request.get_method()
        self.requests.append(
            {"method": method, "url": url, "body": body, "headers": dict(request.header_items())}
        )
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname == "developer.atlassian.com":
            return self._license(url)
        return self._bitbucket(method, parsed.path, parsed.query, body)

    def _license(self, url: str) -> FakeResponse:
        if self.license_failure == "unreachable":
            raise urllib.error.URLError("name resolution failed")
        if self.license_failure == "http":
            raise urllib.error.HTTPError(url, 503, "Service Unavailable", None, io.BytesIO(b""))  # type: ignore[arg-type]
        if self.license_failure == "no-block":
            return FakeResponse(200, b"<html><body><p>Page moved.</p></body></html>")
        return FakeResponse(200, license_page_html().encode())

    def _bitbucket(self, method: str, path: str, query: str, body: Any) -> FakeResponse:
        def ok(payload: dict[str, Any], status: int = 200) -> FakeResponse:
            return FakeResponse(status, json.dumps(payload).encode())

        if path.endswith("/status"):
            return ok({"state": "RUNNING"})
        if "application-properties" in path:
            return ok({"version": "9.4.24", "buildNumber": "9004024", "displayName": "Bitbucket"})
        if path.startswith("/rest/access-tokens/"):
            return ok(
                {
                    "id": "112233445566",
                    "name": "curie-factory",
                    "permissions": ["REPO_WRITE"],
                    "expiryDays": 1,
                    "createdDate": 1_760_000_000_000,
                    "token": FAKE_TOKEN,
                },
                201,
            )
        if "/permissions/" in path:
            return FakeResponse(204, b"")
        if "/admin/users" in path and method in ("POST", "PUT"):
            return FakeResponse(204, b"")
        if "/users" in path:
            who = next((name for name in USER_IDS if name in path + "?" + query), "admin")
            user = {"id": USER_IDS[who], "name": who, "slug": who, "displayName": who}
            # Answers both a single-user read and a filtered listing.
            return ok({**user, "values": [user], "isLastPage": True, "size": 1, "start": 0})
        if "/repos" in path:
            repo = {
                "id": 1,
                "slug": "demo",
                "name": "demo",
                "state": "AVAILABLE",
                "project": {"key": "FAC", "id": 1},
            }
            return ok(
                {**repo, "values": [repo], "isLastPage": True, "size": 1, "start": 0},
                201 if method == "POST" else 200,
            )
        if path.rstrip("/").endswith("/projects") or "/projects/FAC" in path:
            project = {"key": "FAC", "id": 1, "name": "Factory"}
            return ok(
                {**project, "values": [project], "isLastPage": True},
                201 if method == "POST" else 200,
            )
        return ok(
            {
                "id": FAKE_SHA,
                "displayId": "main",
                "latestCommit": FAKE_SHA,
                "values": [],
                "isLastPage": True,
                "size": 0,
                "start": 0,
            }
        )

    def any(self, fragment: str, method: str | None = None) -> list[dict[str, Any]]:
        return [
            r
            for r in self.requests
            if fragment in r["url"] and (method is None or r["method"] == method)
        ]


class FakeDocker:
    """Every external command: docker, docker compose, openssl and git."""

    KINDS = ("containers", "volumes", "networks")

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.resources: dict[str, dict[str, set[str]]] = {}
        self.busy_publish: set[int] = set()
        self.failing_listing: set[str] = set()  # "containers", "volumes", "networks"
        self.leave_on_down: set[str] = set()
        # Observations taken at the moment of each creating call.
        self.first_create_snapshot: dict[str, Any] | None = None
        self.state_at_first_openssl: dict[str, Any] | None = None
        self.compose_text_at_create: str | None = None
        self.create_env: dict[str, str] | None = None

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _cp(
        argv: list[str], rc: int = 0, out: str = "", err: str = ""
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, rc, out, err)

    def compose_subcommands(self) -> list[str]:
        return [c["compose"] for c in self.calls if c.get("compose")]

    def all_ids(self, project: str) -> set[str]:
        held = self.resources.get(project, {})
        return set().union(*(held.get(k, set()) for k in self.KINDS))

    def everything(self) -> set[str]:
        return set().union(*(self.all_ids(p) for p in self.resources))

    @staticmethod
    def _compose_parts(argv: list[str]) -> tuple[str | None, Path | None, str | None]:
        project = compose_file = None
        sub: str | None = None
        i = argv.index("compose") + 1
        while i < len(argv):
            tok = argv[i]
            if tok in ("-p", "--project-name"):
                project = argv[i + 1]
                i += 2
            elif tok in ("-f", "--file"):
                compose_file = Path(argv[i + 1])
                i += 2
            elif tok in ("--project-directory", "--env-file", "--profile", "--ansi", "--progress"):
                i += 2
            elif tok.startswith("--project-name="):
                project = tok.split("=", 1)[1]
                i += 1
            elif tok.startswith("--file="):
                compose_file = Path(tok.split("=", 1)[1])
                i += 1
            elif tok.startswith("-"):
                i += 1
            else:
                sub = tok
                break
        return project, compose_file, sub

    @staticmethod
    def _filters(argv: list[str]) -> list[str]:
        out = []
        for i, tok in enumerate(argv):
            if tok == "--filter" and i + 1 < len(argv):
                out.append(argv[i + 1])
            elif tok.startswith("--filter="):
                out.append(tok.split("=", 1)[1])
        return out

    @staticmethod
    def _state_of(state_dir: Path) -> dict[str, Any]:
        path = state_dir / "state.json"
        if not path.is_file():
            return {"exists": False}
        try:
            return {"exists": True, "data": json.loads(path.read_text())}
        except ValueError:
            return {"exists": True, "data": None}

    # -- dispatch -----------------------------------------------------------

    def __call__(
        self,
        argv: list[str],
        *,
        env: dict[str, str] | None = None,
        input: str | None = None,
        timeout: float | None = None,
        cwd: str | os.PathLike[str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        argv = [str(a) for a in argv]
        call: dict[str, Any] = {"argv": argv, "env": dict(env or {}), "input": input, "cwd": cwd}
        self.calls.append(call)
        tool = Path(argv[0]).name
        if tool == "docker":
            if "compose" in argv[1:3]:
                return self._compose(argv, call, env)
            return self._docker(argv)
        if tool == "openssl":
            return self._openssl(argv)
        if tool == "git":
            return self._git(argv, cwd)
        return self._cp(argv)

    def _compose(
        self, argv: list[str], call: dict[str, Any], env: dict[str, str] | None
    ) -> subprocess.CompletedProcess[str]:
        project, compose_file, sub = self._compose_parts(argv)
        call["compose"] = sub
        call["project"] = project
        if sub in ("create", "up") and project:
            if self.first_create_snapshot is None:
                state_dir = compose_file.parent if compose_file else None
                self.first_create_snapshot = {
                    "project": project,
                    "state": self._state_of(state_dir) if state_dir else {"exists": False},
                }
                self.compose_text_at_create = (
                    compose_file.read_text() if compose_file and compose_file.is_file() else None
                )
                self.create_env = dict(env or {})
            held = self.resources.setdefault(project, {k: set() for k in self.KINDS})
            for svc in ("postgres", "bitbucket", "nginx"):
                held["containers"].add(f"ctr-{project}-{svc}")
            held["volumes"].update({f"vol-{project}-pgdata", f"vol-{project}-bbdata"})
            held["networks"].add(f"net-{project}-default")
        elif sub == "down" and project:
            held = self.resources.get(project, {})
            for kind in self.KINDS:
                if kind not in self.leave_on_down:
                    held.get(kind, set()).clear()
        elif sub == "logs":
            return self._cp(argv, out="bitbucket | startup log line\n" * 3)
        return self._cp(argv)

    def _listing(self, argv: list[str], kind: str) -> subprocess.CompletedProcess[str]:
        if kind in self.failing_listing:
            return self._cp(argv, 1, err="Cannot connect to the Docker daemon")
        filters = self._filters(argv)
        if kind == "containers":
            for f in filters:
                if f.startswith("publish="):
                    port = int(f.split("=", 1)[1].split("/")[0])
                    if port in self.busy_publish:
                        return self._cp(argv, out="someone-elses-proxy\n")
                    return self._cp(argv)
        for f in filters:
            if f.startswith(LABEL_PREFIX):
                project = f[len(LABEL_PREFIX) :]
                ids = sorted(self.resources.get(project, {}).get(kind, set()))
                return self._cp(argv, out="".join(f"{i}\n" for i in ids))
        return self._cp(argv)

    def _remove(self, argv: list[str], kind: str) -> subprocess.CompletedProcess[str]:
        targets = {a for a in argv[2:] if not a.startswith("-") and a not in ("rm", "remove")}
        for held in self.resources.values():
            held.get(kind, set()).difference_update(targets)
        return self._cp(argv)

    def _docker(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        verb = argv[1:3]
        if verb[:1] == ["ps"] or verb in (["container", "ls"], ["container", "list"]):
            return self._listing(argv, "containers")
        if verb in (["volume", "ls"], ["volume", "list"]):
            return self._listing(argv, "volumes")
        if verb in (["network", "ls"], ["network", "list"]):
            return self._listing(argv, "networks")
        if verb[:1] == ["rm"] or verb in (["container", "rm"], ["container", "remove"]):
            return self._remove(argv, "containers")
        if verb in (["volume", "rm"], ["volume", "remove"]):
            return self._remove(argv, "volumes")
        if verb in (["network", "rm"], ["network", "remove"]):
            return self._remove(argv, "networks")
        return self._cp(argv)

    def _openssl(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        for i, tok in enumerate(argv[:-1]):
            if tok in ("-out", "-keyout"):
                target = Path(argv[i + 1])
                if self.state_at_first_openssl is None:
                    self.state_at_first_openssl = self._state_of(target.parent)
                target.write_text("-----BEGIN FAKE PEM-----\nAAAA\n-----END FAKE PEM-----\n")
        return self._cp(argv)

    def _git(
        self, argv: list[str], cwd: str | os.PathLike[str] | None
    ) -> subprocess.CompletedProcess[str]:
        where = Path(cwd) if cwd else Path.cwd()
        if "-C" in argv:
            where = Path(argv[argv.index("-C") + 1])
        if "rev-parse" in argv and "--show-toplevel" in argv:
            try:
                where.resolve().relative_to(REPO_ROOT)
            except ValueError:
                return self._cp(argv, 128, err="fatal: not a git repository")
            return self._cp(argv, out=f"{REPO_ROOT}\n")
        if "rev-parse" in argv:
            return self._cp(argv, out=f"{FAKE_SHA}\n")
        return self._cp(argv)


# --------------------------------------------------------------------------
# Fixtures


@pytest.fixture(scope="module")
def fx() -> ModuleType:
    assert FIXTURE_PATH.is_file(), f"{FIXTURE_PATH.relative_to(REPO_ROOT)} is missing"
    spec = importlib.util.spec_from_file_location("curie_bitbucket_dc_fixture", FIXTURE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _tools_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda name, *a, **k: f"/usr/bin/{name}")
    monkeypatch.delenv(FAIL_AT_ENV, raising=False)


@pytest.fixture
def docker() -> FakeDocker:
    return FakeDocker()


@pytest.fixture
def http() -> FakeHttp:
    return FakeHttp()


@pytest.fixture
def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", 0))
        port: int = s.getsockname()[1]
    return port


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "bbdc-state"


@pytest.fixture
def held_port(free_port: int) -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", free_port))
        s.listen(1)
        yield free_port


class Run:
    def __init__(
        self, fx: ModuleType, docker: FakeDocker, http: FakeHttp, capsys: pytest.CaptureFixture[str]
    ) -> None:
        self.fx, self.docker, self.http, self.capsys = fx, docker, http, capsys
        self.out = self.err = ""

    def __call__(self, *argv: str) -> int:
        rc = self.fx.main(list(argv), runner=self.docker, urlopen=self.http)
        captured = self.capsys.readouterr()
        self.out, self.err = captured.out, captured.err
        assert isinstance(rc, int)
        return rc


@pytest.fixture
def run(
    fx: ModuleType, docker: FakeDocker, http: FakeHttp, capsys: pytest.CaptureFixture[str]
) -> Run:
    return Run(fx, docker, http, capsys)


def up(run: Run, state_dir: Path, port: int | None, *extra: str) -> int:
    argv = ["up", "--state-dir", str(state_dir), *extra]
    if port is not None:
        argv += ["--port", str(port)]
    return run(*argv)


def load_state(state_dir: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((state_dir / "state.json").read_text())
    return data


def mode(path: Path | str) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def files_under(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file()]


def secret_files(state: dict[str, Any]) -> list[Path]:
    paths = [Path(state["token_file"]), Path(state["admin"]["password_file"])]
    paths += [Path(u["password_file"]) for u in state["users"].values()]
    return paths


# --------------------------------------------------------------------------
# up: the success path every refusal below is measured against


def test_up_succeeds_and_writes_state(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    state = load_state(state_dir)
    assert state["schema"] == "curie.forge-fixture.bitbucket-dc/v1"
    assert PROJECT_RE.match(state["project"]), state["project"]
    assert state["port"] == free_port
    assert state["base_url"] == f"https://localhost:{free_port}"
    assert state["version"] == "9.4.24"
    assert (state["project_key"], state["repo_slug"], state["repo_path"]) == (
        "FAC",
        "demo",
        "FAC/demo",
    )
    assert state["default_branch"] == "main"
    assert set(state["users"]) == {"reviewer", "outsider"}
    for key in (
        "advertised_url",
        "host",
        "ca_file",
        "repo_id",
        "token_file",
        "token_expires_at",
        "admin",
        "compose_file",
    ):
        assert key in state, key
    assert Path(state["compose_file"]).is_file()
    # The same project name drives Compose.
    assert {c["project"] for c in docker.calls if c.get("compose")} == {state["project"]}


def test_up_writes_skeleton_state_before_creating_anything(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    project = load_state(state_dir)["project"]
    # The first file the fixture makes (the CA key) already finds state.json.
    assert docker.state_at_first_openssl is not None
    assert docker.state_at_first_openssl["exists"], "openssl ran before state.json was written"
    assert docker.state_at_first_openssl["data"]["project"] == project
    # And so does the first container create.
    assert docker.first_create_snapshot is not None
    assert docker.first_create_snapshot["state"]["exists"]
    assert docker.first_create_snapshot["state"]["data"]["project"] == project


def test_up_uses_default_port_27443(run: Run, state_dir: Path) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", DEFAULT_PORT))
        except OSError:
            pytest.skip(f"port {DEFAULT_PORT} is taken on this machine")
    assert up(run, state_dir, None) == 0, run.err
    state = load_state(state_dir)
    assert state["port"] == DEFAULT_PORT
    assert state["base_url"] == f"https://localhost:{DEFAULT_PORT}"


def test_up_refuses_default_port_published_on_daemon(
    run: Run, docker: FakeDocker, state_dir: Path
) -> None:
    docker.busy_publish.add(DEFAULT_PORT)
    assert up(run, state_dir, None) != 0
    assert str(DEFAULT_PORT) in run.err
    assert not state_dir.exists()
    assert "create" not in docker.compose_subcommands()


# --------------------------------------------------------------------------
# up: refusals, each beside the success it differs from by one input


def test_up_refuses_state_dir_inside_repository(
    run: Run, docker: FakeDocker, http: FakeHttp, free_port: int
) -> None:
    inside = FIXTURE_DIR / f".pytest-state-{os.getpid()}"
    try:
        assert up(run, inside, free_port) != 0
        assert "repositor" in run.err.lower()
        assert not inside.exists()
        assert docker.compose_subcommands() == []
        assert http.requests == []
    finally:
        shutil.rmtree(inside, ignore_errors=True)


def test_up_refuses_relative_state_dir_resolving_into_repository(
    run: Run, docker: FakeDocker, free_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `curie dev forge-fixture` runs from the repository root, so a relative
    # --state-dir lands inside the checkout.
    monkeypatch.chdir(REPO_ROOT)
    name = f".pytest-bbdc-state-{os.getpid()}"
    try:
        assert up(run, Path(name), free_port) != 0
        assert "repositor" in run.err.lower()
        assert not (REPO_ROOT / name).exists()
        assert docker.compose_subcommands() == []
    finally:
        shutil.rmtree(REPO_ROOT / name, ignore_errors=True)


def test_up_accepts_state_dir_outside_repository(run: Run, state_dir: Path, free_port: int) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    assert state_dir.is_dir()


def test_up_refuses_non_empty_state_dir_and_keeps_it(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    state_dir.mkdir()
    (state_dir / "keep.txt").write_text("not the fixture's\n")
    assert up(run, state_dir, free_port) != 0
    assert (state_dir / "keep.txt").read_text() == "not the fixture's\n"
    assert docker.compose_subcommands() == []


def test_up_refuses_port_bound_locally(
    run: Run, docker: FakeDocker, state_dir: Path, held_port: int
) -> None:
    assert up(run, state_dir, held_port) != 0
    assert str(held_port) in run.err
    assert not state_dir.exists()
    assert docker.compose_subcommands() == []


def test_up_accepts_same_port_once_released(run: Run, state_dir: Path, free_port: int) -> None:
    # Liveness neighbor of the locally bound refusal: the port fixture closed
    # its socket, so the same port is free.
    assert up(run, state_dir, free_port) == 0, run.err
    assert load_state(state_dir)["port"] == free_port


def test_up_refuses_port_published_on_daemon(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    docker.busy_publish.add(free_port)
    assert up(run, state_dir, free_port) != 0
    assert str(free_port) in run.err
    assert not state_dir.exists()
    assert "create" not in docker.compose_subcommands()
    probes = [c["argv"] for c in docker.calls if f"publish={free_port}" in " ".join(c["argv"])]
    assert probes, "the daemon was never asked who publishes the port"


def test_up_accepts_port_daemon_publishes_elsewhere(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    docker.busy_publish.add(free_port + 1 if free_port < 65535 else free_port - 1)
    assert up(run, state_dir, free_port) == 0, run.err


@pytest.mark.parametrize("missing", ["docker", "openssl", "git"])
def test_up_refuses_missing_tool(
    run: Run,
    docker: FakeDocker,
    state_dir: Path,
    free_port: int,
    missing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        shutil, "which", lambda name, *a, **k: None if name == missing else f"/usr/bin/{name}"
    )
    assert up(run, state_dir, free_port) != 0
    assert missing in run.err
    assert not state_dir.exists()
    assert docker.compose_subcommands() == []


# --------------------------------------------------------------------------
# up: CURIE_FORGE_FIXTURE_FAIL_AT and teardown on failure (decision 14.8)


def _reached(phase: str, docker: FakeDocker, http: FakeHttp) -> tuple[bool, bool]:
    """(the phase before ran, this phase did not start) for each phase."""
    subs = docker.compose_subcommands()
    license_fetched = bool(http.any("developer.atlassian.com"))
    openssl_ran = any(Path(c["argv"][0]).name == "openssl" for c in docker.calls)
    status_polled = bool(
        [r for r in http.requests if urllib.parse.urlsplit(r["url"]).path.endswith("/status")]
    )
    rest_called = bool(http.any("/rest/"))
    token_made = bool(http.any("/rest/access-tokens/"))
    return {
        "license": (True, not license_fetched),
        "certs": (license_fetched, not openssl_ran),
        "create": (openssl_ran, "create" not in subs),
        "start": ("create" in subs, "start" not in subs),
        "ready": ("start" in subs, not status_polled),
        "seed": (status_polled, not rest_called),
        "state": (token_made, True),
    }[phase]


@pytest.mark.parametrize("phase", PHASES)
def test_fail_at_phase_fails_there_and_tears_down(
    run: Run,
    docker: FakeDocker,
    http: FakeHttp,
    state_dir: Path,
    free_port: int,
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(FAIL_AT_ENV, phase)
    assert up(run, state_dir, free_port) != 0
    assert phase in run.err
    before_ran, this_not_started = _reached(phase, docker, http)
    assert before_ran, f"up never reached {phase}"
    assert this_not_started, f"{phase} ran before failing"
    # Everything up created is gone: containers, volumes, network, state dir.
    assert docker.everything() == set()
    assert not state_dir.exists()
    if phase not in ("license", "certs", "create"):
        assert "down" in docker.compose_subcommands()
    # And a later down agrees there is nothing left.
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err


def test_fail_at_unset_runs_every_phase(
    run: Run, docker: FakeDocker, http: FakeHttp, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    assert "down" not in docker.compose_subcommands()
    assert docker.all_ids(load_state(state_dir)["project"])
    assert http.any("/rest/access-tokens/")


# --------------------------------------------------------------------------
# down (decision 14.9)


def test_down_removes_containers_volumes_network_and_state_dir(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    project = load_state(state_dir)["project"]
    assert docker.all_ids(project)
    docker.calls.clear()
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err
    assert docker.all_ids(project) == set()
    assert not state_dir.exists()
    downs = [c["argv"] for c in docker.calls if c.get("compose") == "down"]
    assert downs and "-v" in downs[0] and "--remove-orphans" in downs[0]
    label = LABEL_PREFIX + project
    listed = " ".join(
        " ".join(c["argv"])
        for c in docker.calls
        if label in c["argv"] or f"--filter={label}" in c["argv"]
    )
    for listing in ("ps -a", "volume ls", "network ls"):
        assert listing in listed, f"down never checked `docker {listing}` by label"


def test_down_removes_leftovers_by_id(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    project = load_state(state_dir)["project"]
    docker.leave_on_down = {"containers", "volumes", "networks"}
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err
    assert docker.all_ids(project) == set()
    assert not state_dir.exists()


def test_down_fails_closed_when_a_listing_errors(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    state_before = (state_dir / "state.json").read_text()
    docker.failing_listing.add("volumes")
    assert run("down", "--state-dir", str(state_dir)) != 0
    assert state_dir.is_dir()
    assert (state_dir / "state.json").read_text() == state_before
    # Liveness neighbor: once the listing answers, the same down completes.
    docker.failing_listing.clear()
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err
    assert not state_dir.exists()


def test_second_down_exits_zero(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err
    assert not state_dir.exists()


def test_down_on_never_created_state_dir_exits_zero(run: Run, state_dir: Path) -> None:
    assert run("down", "--state-dir", str(state_dir)) == 0, run.err


# --------------------------------------------------------------------------
# license (decision 14.3)


def test_license_is_read_from_atlassian_page_and_reaches_only_create_env(
    run: Run, docker: FakeDocker, http: FakeHttp, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    assert http.any(
        "developer.atlassian.com/platform/marketplace/timebomb-licenses-for-testing-server-apps"
    )
    assert docker.create_env is not None
    passed = [v for v in docker.create_env.values() if LICENSE_MARK in v]
    assert passed, "the license never reached docker compose create"
    assert _squash(passed[0]) == _squash(LICENSE)
    # Never on a command line, so never in a process listing.
    for call in docker.calls:
        assert not any(LICENSE_MARK in a for a in call["argv"]), call["argv"]


def test_license_never_lands_in_a_file(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    assert docker.compose_text_at_create is not None
    assert LICENSE_MARK not in docker.compose_text_at_create
    for path in files_under(state_dir):
        assert LICENSE_MARK not in path.read_text(errors="replace"), path.name
    # The fixture's own committed directory gains no copy either.
    for path in files_under(FIXTURE_DIR):
        assert LICENSE_MARK not in path.read_text(errors="replace"), path
    assert LICENSE_MARK not in run.out + run.err


@pytest.mark.parametrize("failure", ["unreachable", "http", "no-block"])
def test_unreadable_license_page_fails_naming_it(
    run: Run, docker: FakeDocker, http: FakeHttp, state_dir: Path, free_port: int, failure: str
) -> None:
    http.license_failure = failure
    assert up(run, state_dir, free_port) != 0
    assert LICENSE_PAGE.rstrip("/") in run.err
    assert "create" not in docker.compose_subcommands()
    assert not state_dir.exists()


# --------------------------------------------------------------------------
# secrets and modes (decision 14.5)


def test_state_dir_and_secret_files_are_private(run: Run, state_dir: Path, free_port: int) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    state = load_state(state_dir)
    assert mode(state_dir) == 0o700
    assert mode(state_dir / "state.json") == 0o600
    assert mode(state["ca_file"]) == 0o600
    for path in secret_files(state):
        assert path.resolve().parent == state_dir.resolve()
        assert mode(path) == 0o600, path.name


def test_token_lives_only_in_its_file(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    state = load_state(state_dir)
    assert Path(state["token_file"]).read_text().strip() == FAKE_TOKEN
    text = (state_dir / "state.json").read_text()
    assert FAKE_TOKEN not in text
    for path in secret_files(state)[1:]:
        password = path.read_text().strip()
        assert password and password not in text, path.name
    secrets_ = [FAKE_TOKEN] + [p.read_text().strip() for p in secret_files(state)[1:]]
    for secret in secrets_:
        assert secret not in run.out + run.err
        for call in docker.calls:
            assert not any(secret in a for a in call["argv"]), call["argv"][:3]


# --------------------------------------------------------------------------
# what up builds (decisions 14.2, 14.4, 14.6)


def test_compose_is_created_then_copied_then_started_with_no_bind_mounts(
    run: Run, docker: FakeDocker, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    subs = docker.compose_subcommands()
    assert "up" not in subs
    assert subs.index("create") < subs.index("cp") < subs.index("start")
    text = docker.compose_text_at_create or ""
    assert "atlassian/bitbucket:9.4.24" in text
    assert "postgres:15." in text
    assert "nginx:" in text
    assert "curie.forge-fixture" in text and "bitbucket-dc" in text
    assert f"{free_port}:443" in text
    assert "type: bind" not in text
    assert str(state_dir) not in text
    assert not re.search(r"^\s*-\s*['\"]?(\.{1,2}/|/|~)", text, re.MULTILINE), (
        "bind mount in compose"
    )


def test_up_seeds_project_repo_users_and_token(
    run: Run, docker: FakeDocker, http: FakeHttp, state_dir: Path, free_port: int
) -> None:
    assert up(run, state_dir, free_port) == 0, run.err
    tokens = http.any("/rest/access-tokens/latest/projects/FAC/repos/demo", "PUT")
    assert tokens, "no repository access token was requested"
    body = tokens[0]["body"]
    assert body["permissions"] == ["REPO_WRITE"] and body["expiryDays"] == 1
    for user in ("reviewer", "outsider"):
        created = [
            r
            for r in http.any("/admin/users")
            if r["method"] in ("POST", "PUT") and user in r["url"] + json.dumps(r["body"])
        ]
        assert created, f"{user} was never created"
        granted = [
            r
            for r in http.any("/repos/demo/permissions/users", "PUT")
            if user in r["url"] and "REPO_WRITE" in r["url"]
        ]
        assert granted, f"{user} was never granted REPO_WRITE on demo"
    pushes = [c for c in docker.calls if Path(c["argv"][0]).name == "git" and "push" in c["argv"]]
    assert pushes, "no seed commit was pushed"
    assert any(k.startswith("GIT_CONFIG_") for k in pushes[0]["env"])
