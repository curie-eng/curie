#!/usr/bin/env python3
"""Bitbucket Data Center 9.4.24 forge fixture (#4401 decision 14).

    fixture.py <up|self-test|down> --state-dir D [--advertise-host H] [--port P]

``up`` starts a Compose project (Postgres, Bitbucket, an nginx TLS front end
signed by a test CA), seeds it over REST and writes ``state.json`` into the
state directory. ``self-test`` proves TLS, git with the repository access
token and a build status round trip. ``down`` removes everything the project
created and the state directory.

Stdlib only; runs on the system ``python3``. Every external command goes
through ``runner``, every HTTP request through ``urlopen`` and every reading
of the time against ``up``'s deadline through ``clock``, all handed to ``main``
so the unit tests can fake them. The real ``urlopen`` is ``refusing_urlopen``,
which follows no redirect, so a password or token is only ever sent to the URL
the fixture named.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import html
import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from email.message import Message
from pathlib import Path
from typing import IO, Any, Protocol

SCHEMA = "curie.forge-fixture.bitbucket-dc/v1"
VERSION = "9.4.24"
DEFAULT_PORT = 27443
READY_TIMEOUT = 900.0
# The longest one seeding request may take; less when the deadline is nearer.
SEED_REQUEST_TIMEOUT = 30.0
# The longest each kind of command may take. Under ``up`` the budget's
# remainder caps it further; ``down`` and ``self-test`` use these as they are.
QUICK_COMMAND_TIMEOUT = 30.0
OPENSSL_TIMEOUT = 60.0
COMPOSE_CP_TIMEOUT = 60.0
COMPOSE_START_TIMEOUT = 300.0
COMPOSE_DOWN_TIMEOUT = 300.0
DOCKER_REMOVE_TIMEOUT = 120.0
GIT_NETWORK_TIMEOUT = 120.0
LOGS_TIMEOUT = 60.0
LICENSE_PAGE = (
    "https://developer.atlassian.com/platform/marketplace/"
    "timebomb-licenses-for-testing-server-apps/"
)
LICENSE_HEADING = "Bitbucket Data Center license, expires in 3 hours"
FAIL_AT_ENV = "CURIE_FORGE_FIXTURE_FAIL_AT"
PHASES = ("license", "certs", "create", "start", "ready", "seed", "state")
PROJECT_RE = re.compile(r"^curie-forge-bbdc-[0-9a-f]{8}$")
HOST_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
TOOLS = ("docker", "openssl", "git")

PROJECT_KEY = "FAC"
REPO_SLUG = "demo"
DEFAULT_BRANCH = "main"
ADMIN = "admin"
USERS = ("reviewer", "outsider")
TOKEN_NAME = "curie-factory"

HERE = Path(__file__).resolve().parent
TLS_IN_NGINX = {
    "tls.pem": "/etc/nginx/fixture-tls.pem",
    "tls-key.pem": "/etc/nginx/fixture-tls-key.pem",
    "nginx.conf": "/etc/nginx/conf.d/default.conf",
}

OPENSSL_CONFIG = """\
[req]
distinguished_name = dn
prompt = no
[dn]
CN = placeholder
[ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[leaf]
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid, issuer
subjectAltName = {san}
"""


class Runner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        input: str | None = None,
        timeout: float | None = None,
        cwd: str | os.PathLike[str] | None = None,
    ) -> subprocess.CompletedProcess[str]: ...


class Opener(Protocol):
    def __call__(
        self,
        url: str | urllib.request.Request,
        data: None = None,
        timeout: float | None = None,
        *,
        context: ssl.SSLContext | None = None,
    ) -> Any: ...


class _RefuseRedirect(urllib.request.HTTPRedirectHandler):
    """Follow no redirect: the 3xx surfaces as an HTTPError, nothing is re-sent."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        return None


def refusing_urlopen(
    url: str | urllib.request.Request,
    data: None = None,
    timeout: float | None = None,
    *,
    context: ssl.SSLContext | None = None,
) -> Any:
    """``urllib.request.urlopen`` that refuses every redirect; the real ``Opener``.

    Every request goes through it, the unauthenticated license page read too:
    one opener leaves no path where a credential follows a redirect, and the
    license page is a fixed HTTPS URL that answers 200, so a redirect there
    means Atlassian moved it and ``up`` fails naming the page.
    """
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=context), _RefuseRedirect()
    )
    if timeout is None:
        return opener.open(url, data)
    return opener.open(url, data, timeout)


class FixtureError(Exception):
    """A fixture failure; the message goes to stderr and never holds a secret."""


def run_command(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    input: str | None = None,
    timeout: float | None = None,
    cwd: str | os.PathLike[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """The real runner: never raises on a non-zero exit or a timeout."""
    try:
        return subprocess.run(
            list(argv),
            env=None if env is None else dict(env),
            input=input,
            timeout=timeout,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(list(argv), 124, "", f"timed out after {timeout}s\n")


def _invoke(
    runner: Runner,
    argv: Sequence[str],
    what: str,
    *,
    timeout: float,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """One bounded command; an injected runner's timeout is a fixture failure."""
    try:
        return runner(argv, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise FixtureError(f"{what} timed out after {timeout:.0f} s") from None


class Budget:
    """``up``'s deadline. Every command under it is capped by what is left."""

    def __init__(self, runner: Runner, deadline: float, clock: Callable[[], float]) -> None:
        self.runner = runner
        self.deadline = deadline
        self.clock = clock

    def remaining(self) -> float:
        left = self.deadline - self.clock()
        if left <= 0:
            raise FixtureError(f"up ran past its {int(READY_TIMEOUT)} s budget")
        return left

    def timeout(self, cap: float) -> float:
        """``cap``, or less when the deadline is nearer. Refuses once it has passed."""

        return min(cap, self.remaining())

    def run(
        self,
        argv: Sequence[str],
        what: str,
        *,
        cap: float,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return _invoke(self.runner, argv, what, timeout=self.timeout(cap), env=env)


def log(message: str) -> None:
    print(f"bitbucket-dc: {message}", file=sys.stderr, flush=True)


def _fail_at(phase: str) -> None:
    if os.environ.get(FAIL_AT_ENV) == phase:
        raise FixtureError(f"{FAIL_AT_ENV}={phase}: failing at the start of the {phase} phase")


def _write_private(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(text)
    os.chmod(path, 0o600)


def _write_state(state_dir: Path, state: Mapping[str, Any]) -> None:
    staged = state_dir / "state.json.tmp"
    _write_private(staged, json.dumps(state, indent=2, sort_keys=True) + "\n")
    os.replace(staged, state_dir / "state.json")


def _check(done: subprocess.CompletedProcess[str], what: str) -> str:
    if done.returncode != 0:
        detail = (done.stderr or done.stdout).strip().splitlines()[-5:]
        raise FixtureError(f"{what} failed (exit {done.returncode}): {' | '.join(detail)}")
    return done.stdout


def _compose(project: str, compose_file: Path, *args: str) -> list[str]:
    return ["docker", "compose", "-p", project, "-f", str(compose_file), *args]


def _git_env(ca_file: str, authorization: str) -> dict[str, str]:
    """Git's config through the environment: the CA and the auth header, no files."""
    return {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "http.sslCAInfo",
        "GIT_CONFIG_VALUE_0": ca_file,
        "GIT_CONFIG_KEY_1": "http.extraHeader",
        "GIT_CONFIG_VALUE_1": f"Authorization: {authorization}",
    }


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


GIT_IDENTITY = [
    "-c",
    "user.name=Curie Forge Fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "-c",
    "commit.gpgsign=false",
]


# --------------------------------------------------------------------------
# HTTP


class Http:
    def __init__(self, urlopen: Opener, base_url: str, ca_file: Path) -> None:
        self.urlopen = urlopen
        self.base_url = base_url
        self.ca_file = ca_file
        # Trusts the fixture CA and nothing else. A CA file that does not load
        # leaves the context trusting nothing, so every request fails closed
        # and its error names why.
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.ca_error: ssl.SSLError | None = None
        try:
            self.context.load_verify_locations(cafile=str(ca_file))
        except ssl.SSLError as error:
            self.ca_error = error

    def call(
        self,
        method: str,
        path: str,
        *,
        auth: str | None = None,
        body: Any = None,
        query: Mapping[str, str] | None = None,
        timeout: float = 30.0,
    ) -> Any:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        headers = {"Accept": "application/json", "X-Atlassian-Token": "no-check"}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if auth is not None:
            headers["Authorization"] = auth
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self.urlopen(request, context=self.context, timeout=timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            text = error.read().decode(errors="replace")[:300] if error.fp else ""
            # The path only: a query string may carry a throwaway password.
            raise FixtureError(f"{method} {path} answered HTTP {error.code}: {text}") from None
        except (urllib.error.URLError, OSError) as error:
            why = f" (the CA {self.ca_file} did not load: {self.ca_error})" if self.ca_error else ""
            raise FixtureError(f"{method} {path} failed: {error}{why}") from None
        return json.loads(raw) if raw else None


# --------------------------------------------------------------------------
# up


def _repository_root(budget: Budget) -> Path | None:
    done = budget.run(
        ["git", "-C", str(HERE), "rev-parse", "--show-toplevel"],
        "git rev-parse --show-toplevel",
        cap=QUICK_COMMAND_TIMEOUT,
    )
    if done.returncode != 0 or not done.stdout.strip():
        return None
    return Path(done.stdout.strip()).resolve()


def _refuse_taken_port(port: int, budget: Budget) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        # A listener still refuses the bind; TIME_WAIT left by the last
        # fixture's clients on this port does not.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError as error:
            raise FixtureError(f"port {port} is already in use on this machine: {error}") from None
    what = f"asking the Docker daemon who publishes port {port}"
    done = budget.run(
        ["docker", "ps", "-a", "--filter", f"publish={port}", "--format", "{{.Names}}"],
        what,
        cap=QUICK_COMMAND_TIMEOUT,
    )
    _check(done, what)
    if done.stdout.strip():
        holders = ", ".join(done.stdout.split())
        raise FixtureError(f"port {port} is already published on the Docker daemon by {holders}")


def _read_license(urlopen: Opener) -> str:
    request = urllib.request.Request(LICENSE_PAGE, headers={"User-Agent": "curie-forge-fixture"})
    try:
        with urlopen(request, timeout=60) as response:
            page = response.read().decode(errors="replace")
    except urllib.error.HTTPError as error:
        raise FixtureError(
            f"cannot read the timebomb license page {LICENSE_PAGE}: HTTP {error.code}"
        ) from None
    except (urllib.error.URLError, OSError) as error:
        raise FixtureError(
            f"cannot read the timebomb license page {LICENSE_PAGE}: {error}"
        ) from None
    # The page carries a JSON copy of its markdown first; the rendered heading
    # is the bold paragraph, and the license is the first <code> after it.
    heading = page.find(f"{LICENSE_HEADING}</strong>")
    block = re.search(r"<code[^>]*>(.*?)</code>", page[heading:], re.S) if heading >= 0 else None
    text = re.sub(r"\s+", "", html.unescape(block.group(1))) if block else ""
    if not re.fullmatch(r"[A-Za-z0-9+/=]{100,}", text):
        raise FixtureError(
            f"the timebomb license page {LICENSE_PAGE} has no "
            f"'{LICENSE_HEADING}' block; Atlassian may have moved it"
        )
    return text


def _make_certs(state_dir: Path, advertise_host: str, budget: Budget) -> None:
    sans = ["DNS:localhost", "IP:127.0.0.1"]
    try:
        address = ipaddress.ip_address(advertise_host)
    except ValueError:
        extra = f"DNS:{advertise_host}"
    else:
        extra = f"IP:{address}"
    if extra not in sans:
        sans.append(extra)
    config = state_dir / "openssl.cnf"
    _write_private(config, OPENSSL_CONFIG.format(san=", ".join(sans)))
    common = ["openssl", "req", "-x509", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256"]
    common += ["-days", "7", "-config", str(config)]
    ca, ca_key = state_dir / "ca.pem", state_dir / "ca-key.pem"
    tls, tls_key = state_dir / "tls.pem", state_dir / "tls-key.pem"
    steps = [
        (
            [*common, "-extensions", "ca", "-subj", "/CN=Curie forge fixture test CA"]
            + ["-keyout", str(ca_key), "-out", str(ca)],
            "openssl (test CA)",
        ),
        (
            [*common, "-extensions", "leaf", "-subj", "/CN=localhost"]
            + ["-CA", str(ca), "-CAkey", str(ca_key), "-keyout", str(tls_key), "-out", str(tls)],
            "openssl (server certificate)",
        ),
    ]
    for argv, what in steps:
        _check(budget.run(argv, what, cap=OPENSSL_TIMEOUT), what)
    for path in (ca, ca_key, tls, tls_key):
        os.chmod(path, 0o600)
    ca_key.unlink()


def _render(name: str, values: Mapping[str, str]) -> str:
    text = (HERE / name).read_text()
    for key, value in values.items():
        text = text.replace(f"@@{key}@@", value)
    leftover = re.search(r"@@[A-Z_]+@@", text)
    if leftover:
        raise FixtureError(f"{name} has an unrendered placeholder {leftover.group(0)}")
    return text


class Up:
    def __init__(
        self,
        budget: Budget,
        urlopen: Opener,
        state_dir: Path,
        project: str,
        port: int,
        advertise_host: str,
    ) -> None:
        self.budget = budget
        self.urlopen = urlopen
        self.state_dir = state_dir
        self.project = project
        self.port = port
        self.advertise_host = advertise_host
        self.compose_file = state_dir / "compose.yaml"
        self.ca_file = state_dir / "ca.pem"
        self.base_url = f"https://localhost:{port}"
        self.advertised_url = f"https://{advertise_host}:{port}"
        self.passwords = {name: secrets.token_urlsafe(24) for name in (ADMIN, *USERS)}
        self.db_password = secrets.token_urlsafe(24)

    def compose(self, *args: str, cap: float, env: Mapping[str, str] | None = None) -> str:
        what = f"docker compose {args[0]}"
        done = self.budget.run(
            _compose(self.project, self.compose_file, *args),
            what,
            cap=cap,
            env=None if env is None else {**os.environ, **env},
        )
        return _check(done, what)

    def run(self) -> None:
        _fail_at("license")
        log(f"reading the timebomb license from {LICENSE_PAGE}")
        license_text = _read_license(self.urlopen)

        _fail_at("certs")
        _make_certs(self.state_dir, self.advertise_host, self.budget)

        _fail_at("create")
        values = {"PORT": str(self.port), "HOST": self.advertise_host}
        values["BASE_URL"] = self.advertised_url
        _write_private(self.compose_file, _render("compose.yaml.tmpl", values))
        _write_private(self.state_dir / "nginx.conf", _render("nginx.conf.tmpl", values))
        log(f"creating Compose project {self.project} (pulls images on first use)")
        # Pulls the images on first use, so only the budget bounds it.
        self.compose(
            "create",
            cap=READY_TIMEOUT,
            env={
                "CURIE_BBDC_LICENSE": license_text,
                "CURIE_BBDC_ADMIN_PASSWORD": self.passwords[ADMIN],
                "CURIE_BBDC_DB_PASSWORD": self.db_password,
            },
        )
        for name, target in TLS_IN_NGINX.items():
            self.compose(
                "cp", str(self.state_dir / name), f"nginx:{target}", cap=COMPOSE_CP_TIMEOUT
            )

        _fail_at("start")
        self.compose("start", cap=COMPOSE_START_TIMEOUT)

        _fail_at("ready")
        self.wait_ready()

        _fail_at("seed")
        seeded = self.seed()

        _fail_at("state")
        # Seeding may end past the deadline; such a fixture is a failure, not up.
        self.budget.remaining()
        self.write_state(seeded)

    def wait_ready(self) -> None:
        log(f"waiting for {self.base_url}/status to report RUNNING")
        http = Http(self.urlopen, self.base_url, self.ca_file)
        last = "no answer yet"
        budget = self.budget
        next_note = budget.clock() + 60
        while budget.clock() < budget.deadline:
            try:
                state = http.call("GET", "/status", timeout=budget.timeout(10)).get("state")
            except FixtureError as error:
                last = str(error)
            else:
                if state == "RUNNING":
                    return
                last = f"state {state}"
                if state == "ERROR":
                    self.fail_with_logs(f"Bitbucket reported state ERROR at {self.base_url}/status")
            if budget.clock() >= next_note:
                log(f"still waiting: {last}")
                next_note += 60
            time.sleep(5)
        self.fail_with_logs(
            f"Bitbucket was not RUNNING within {int(READY_TIMEOUT)} s (last: {last})"
        )

    def fail_with_logs(self, reason: str) -> None:
        # Diagnostics for a failure, often one past the deadline: a fixed bound.
        done = _invoke(
            self.budget.runner,
            _compose(self.project, self.compose_file, "logs", "--no-color", "--tail", "100")
            + ["bitbucket"],
            "docker compose logs",
            timeout=LOGS_TIMEOUT,
        )
        raise FixtureError(f"{reason}\nlast 100 Bitbucket log lines:\n{done.stdout}{done.stderr}")

    def seed(self) -> dict[str, Any]:
        http = Http(self.urlopen, self.base_url, self.ca_file)

        def call(method: str, path: str, **kwargs: Any) -> Any:
            return http.call(
                method, path, timeout=self.budget.timeout(SEED_REQUEST_TIMEOUT), **kwargs
            )

        admin = _basic(ADMIN, self.passwords[ADMIN])
        api = "/rest/api/latest"
        repo_api = f"{api}/projects/{PROJECT_KEY}/repos/{REPO_SLUG}"

        version = call("GET", f"{api}/application-properties", auth=admin)["version"]
        if version != VERSION:
            raise FixtureError(f"Bitbucket reports version {version}, expected {VERSION}")
        log(f"seeding project {PROJECT_KEY}, repository {REPO_SLUG}, users and a token")
        call("POST", f"{api}/projects", auth=admin, body={"key": PROJECT_KEY, "name": "Factory"})
        repo = call(
            "POST",
            f"{api}/projects/{PROJECT_KEY}/repos",
            auth=admin,
            body={"name": REPO_SLUG, "scmId": "git", "defaultBranch": DEFAULT_BRANCH},
        )
        head = self.push_seed_commit(admin)

        user_ids = {ADMIN: str(call("GET", f"{api}/users/{ADMIN}", auth=admin)["id"])}
        for user in USERS:
            # Created with a throwaway password (the API takes it in the query
            # string), then given the real one in a JSON body.
            call(
                "POST",
                f"{api}/admin/users",
                auth=admin,
                query={
                    "name": user,
                    "password": secrets.token_urlsafe(24),
                    "displayName": user.capitalize(),
                    "emailAddress": f"{user}@example.invalid",
                    "addToDefaultGroup": "true",
                    "notify": "false",
                },
            )
            password = self.passwords[user]
            call(
                "PUT",
                f"{api}/admin/users/credentials",
                auth=admin,
                body={"name": user, "password": password, "passwordConfirm": password},
            )
            call(
                "PUT",
                f"{api}/admin/permissions/users",
                auth=admin,
                query={"name": user, "permission": "LICENSED_USER"},
            )
            call(
                "PUT",
                f"{repo_api}/permissions/users",
                auth=admin,
                query={"name": user, "permission": "REPO_WRITE"},
            )
            user_ids[user] = str(call("GET", f"{api}/users/{user}", auth=admin)["id"])

        token = call(
            "PUT",
            f"/rest/access-tokens/latest/projects/{PROJECT_KEY}/repos/{REPO_SLUG}",
            auth=admin,
            body={"name": TOKEN_NAME, "permissions": ["REPO_WRITE"], "expiryDays": 1},
        )
        if "expiryDate" in token:
            expires_ms = int(token["expiryDate"])
        else:
            expires_ms = int(token["createdDate"]) + 86_400_000
        expires = datetime.datetime.fromtimestamp(expires_ms / 1000, tz=datetime.UTC)
        return {
            "version": version,
            "repo_id": str(repo["id"]),
            "head": head,
            "user_ids": user_ids,
            "token": str(token["token"]),
            "token_expires_at": expires.isoformat().replace("+00:00", "Z"),
        }

    def push_seed_commit(self, admin_auth: str) -> str:
        work = Path(tempfile.mkdtemp(prefix="seed-", dir=self.state_dir))
        try:
            (work / "README.md").write_text("# demo\n\nSeeded by the Curie forge fixture.\n")
            git = ["git", "-C", str(work)]
            env = {**os.environ, **_git_env(str(self.ca_file), admin_auth)}
            clone_url = f"{self.base_url}/scm/{PROJECT_KEY.lower()}/{REPO_SLUG}.git"
            quick = QUICK_COMMAND_TIMEOUT
            steps: list[tuple[list[str], str, float]] = [
                (["git", "init", "-q", "-b", DEFAULT_BRANCH, str(work)], "git init", quick),
                ([*git, "add", "README.md"], "git add", quick),
                (
                    [*git, *GIT_IDENTITY, "commit", "-q", "-m", "Seed the fixture"],
                    "git commit",
                    quick,
                ),
                (
                    [*git, "push", "-q", clone_url, f"HEAD:refs/heads/{DEFAULT_BRANCH}"],
                    "git push",
                    GIT_NETWORK_TIMEOUT,
                ),
            ]
            for argv, what, cap in steps:
                _check(self.budget.run(argv, what, cap=cap, env=env), what)
            done = self.budget.run([*git, "rev-parse", "HEAD"], "git rev-parse", cap=quick, env=env)
            return _check(done, "git rev-parse").strip()
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def write_state(self, seeded: Mapping[str, Any]) -> None:
        sd = self.state_dir
        token_file = sd / "factory-token"
        _write_private(token_file, seeded["token"] + "\n")
        password_files = {name: sd / f"{name}-password" for name in (ADMIN, *USERS)}
        for name, path in password_files.items():
            _write_private(path, self.passwords[name] + "\n")
        user_ids = seeded["user_ids"]
        state = {
            "schema": SCHEMA,
            "project": self.project,
            "base_url": self.base_url,
            "advertised_url": self.advertised_url,
            "host": "localhost",
            "port": self.port,
            "ca_file": str(self.ca_file),
            "version": seeded["version"],
            "project_key": PROJECT_KEY,
            "repo_slug": REPO_SLUG,
            "repo_path": f"{PROJECT_KEY}/{REPO_SLUG}",
            "repo_id": seeded["repo_id"],
            "default_branch": DEFAULT_BRANCH,
            "seed_commit": seeded["head"],
            "token_file": str(token_file),
            "token_expires_at": seeded["token_expires_at"],
            "admin": {
                "username": ADMIN,
                "user_id": user_ids[ADMIN],
                "password_file": str(password_files[ADMIN]),
            },
            "users": {
                user: {
                    "user_id": user_ids[user],
                    "slug": user,
                    "password_file": str(password_files[user]),
                }
                for user in USERS
            },
            "compose_file": str(self.compose_file),
        }
        _write_state(sd, state)


def up(
    args: argparse.Namespace, runner: Runner, urlopen: Opener, clock: Callable[[], float]
) -> int:
    started = clock()
    fail_at = os.environ.get(FAIL_AT_ENV)
    if fail_at and fail_at not in PHASES:
        raise FixtureError(f"{FAIL_AT_ENV}={fail_at} is not one of {', '.join(PHASES)}")
    missing = [tool for tool in TOOLS if shutil.which(tool) is None]
    if missing:
        raise FixtureError(f"required tool not on PATH: {', '.join(missing)}")
    state_dir = Path(args.state_dir).resolve()
    budget = Budget(runner, started + READY_TIMEOUT, clock)
    root = _repository_root(budget)
    if root is not None and state_dir.is_relative_to(root):
        raise FixtureError(
            f"the state directory {state_dir} is inside the repository {root}; "
            "pick one outside it (it will hold credentials)"
        )
    if state_dir.exists() and (not state_dir.is_dir() or any(state_dir.iterdir())):
        raise FixtureError(f"the state directory {state_dir} exists and is not empty")
    advertise_host = args.advertise_host or "localhost"
    if not HOST_RE.match(advertise_host):
        raise FixtureError(f"--advertise-host {advertise_host!r} is not a host name or IPv4")
    port = args.port if args.port is not None else DEFAULT_PORT
    _refuse_taken_port(port, budget)

    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state_dir, 0o700)
    project = f"curie-forge-bbdc-{secrets.token_hex(4)}"
    # Written first, so `down` can always find what to remove.
    _write_state(state_dir, {"schema": SCHEMA, "project": project})
    fixture = Up(budget, urlopen, state_dir, project, port, advertise_host)
    try:
        fixture.run()
    except BaseException:
        log(f"up failed; tearing down {project}")
        try:
            teardown(state_dir, project, runner)
        except FixtureError as error:
            log(f"teardown incomplete, rerun down: {error}")
        raise
    took = int(clock() - started)
    print(f"bitbucket-dc fixture up in {took} s: {fixture.base_url} (project {project})")
    print(f"state: {state_dir / 'state.json'}")
    return 0


# --------------------------------------------------------------------------
# down


LISTINGS = (
    ("containers", ["docker", "ps", "-a"], ["docker", "rm", "-f", "-v"]),
    ("volumes", ["docker", "volume", "ls"], ["docker", "volume", "rm", "-f"]),
    ("networks", ["docker", "network", "ls"], ["docker", "network", "rm"]),
)


def _leftovers(listing: list[str], project: str, kind: str, runner: Runner) -> list[str]:
    what = f"listing the project's {kind}"
    argv = [*listing, "--filter", f"label=com.docker.compose.project={project}", "-q"]
    out = _check(_invoke(runner, argv, what, timeout=QUICK_COMMAND_TIMEOUT), what)
    return out.split()


def teardown(state_dir: Path, project: str, runner: Runner) -> None:
    compose_file = state_dir / "compose.yaml"
    if compose_file.is_file():
        done = _invoke(
            runner,
            _compose(project, compose_file, "down", "-v", "--remove-orphans"),
            "docker compose down",
            timeout=COMPOSE_DOWN_TIMEOUT,
        )
        if done.returncode != 0:
            log(f"docker compose down exited {done.returncode}; removing by label instead")
    for kind, listing, remove in LISTINGS:
        ids = _leftovers(listing, project, kind, runner)
        if ids:
            log(f"removing leftover {kind}: {' '.join(ids)}")
            for ident in ids:
                _invoke(
                    runner, [*remove, ident], f"removing {ident}", timeout=DOCKER_REMOVE_TIMEOUT
                )
            still = _leftovers(listing, project, kind, runner)
            if still:
                raise FixtureError(f"could not remove {kind} {' '.join(still)}")
    shutil.rmtree(state_dir)


def down(args: argparse.Namespace, runner: Runner) -> int:
    state_dir = Path(args.state_dir).resolve()
    if not state_dir.exists():
        print(f"bitbucket-dc fixture: {state_dir} is gone; nothing to remove")
        return 0
    state_file = state_dir / "state.json"
    if not state_file.is_file():
        raise FixtureError(
            f"{state_dir} has no state.json; refusing to delete a directory "
            "the fixture did not make"
        )
    project = str(json.loads(state_file.read_text()).get("project", ""))
    if not PROJECT_RE.match(project):
        raise FixtureError(f"{state_file} names no fixture project")
    teardown(state_dir, project, runner)
    print(f"bitbucket-dc fixture down: {project} removed, {state_dir} deleted")
    return 0


# --------------------------------------------------------------------------
# self-test


def self_test(args: argparse.Namespace, runner: Runner, urlopen: Opener) -> int:
    state_dir = Path(args.state_dir).resolve()
    state_file = state_dir / "state.json"
    if not state_file.is_file():
        raise FixtureError(f"{state_file} is missing; run up first")
    state = json.loads(state_file.read_text())
    if "token_file" not in state:
        raise FixtureError(f"{state_file} is a skeleton; up did not finish")
    base, ca_file = state["base_url"], state["ca_file"]
    http = Http(urlopen, base, Path(ca_file))
    token = Path(state["token_file"]).read_text().strip()
    bearer = f"Bearer {token}"
    repo_api = f"/rest/api/latest/projects/{state['project_key']}/repos/{state['repo_slug']}"

    # 1. TLS verifies with the CA and is refused without it.
    if http.call("GET", "/status").get("state") != "RUNNING":
        raise FixtureError(f"{base}/status is not RUNNING")
    try:
        urlopen(
            urllib.request.Request(base + "/status"),
            context=ssl.create_default_context(),
            timeout=SEED_REQUEST_TIMEOUT,
        )
    except urllib.error.URLError as error:
        if not isinstance(error.reason, ssl.SSLCertVerificationError):
            raise FixtureError(f"TLS without the CA failed for another reason: {error}") from None
    else:
        raise FixtureError("TLS without the fixture CA was accepted")
    log("ok: TLS verifies with the CA and is refused without it")

    version = http.call("GET", "/rest/api/latest/application-properties", auth=bearer)["version"]
    if version != state["version"]:
        raise FixtureError(f"version {version} != {state['version']}")
    log(f"ok: REST answers with the access token (version {version})")

    # 2. Clone and push a new commit with the token's Bearer header.
    branch = f"fixture-self-test-{secrets.token_hex(4)}"
    work = Path(tempfile.mkdtemp(prefix="self-test-", dir=state_dir))
    try:
        env = {**os.environ, **_git_env(ca_file, bearer)}
        clone_url = f"{base}/scm/{state['project_key'].lower()}/{state['repo_slug']}.git"
        checkout = work / "repo"
        git = ["git", "-C", str(checkout)]

        def git_step(argv: list[str], what: str, timeout: float) -> str:
            return _check(_invoke(runner, argv, what, timeout=timeout, env=env), what)

        quick, network = QUICK_COMMAND_TIMEOUT, GIT_NETWORK_TIMEOUT
        git_step(["git", "clone", "-q", clone_url, str(checkout)], "git clone", network)
        if token in (checkout / ".git" / "config").read_text():
            raise FixtureError("the token landed in .git/config")
        (checkout / "SELF_TEST.md").write_text(f"{branch}\n")
        git_step([*git, "checkout", "-q", "-b", branch], "git checkout", quick)
        git_step([*git, "add", "SELF_TEST.md"], "git add", quick)
        git_step([*git, *GIT_IDENTITY, "commit", "-q", "-m", branch], "git commit", quick)
        git_step([*git, "push", "-q", "origin", branch], "git push", network)
        sha = git_step([*git, "rev-parse", "HEAD"], "git rev-parse", quick).strip()
        log(f"ok: clone and push of {sha[:12]} with the Bearer token")

        # 3. A build status on the pushed commit reads back.
        key = "fixture-self-test"
        http.call(
            "POST",
            f"{repo_api}/commits/{sha}/builds",
            auth=bearer,
            body={"key": key, "state": "SUCCESSFUL", "url": "https://example.invalid/self-test"},
        )
        # On 9.4.24 this GET reads one status by key; it does not list them.
        build = http.call(
            "GET", f"{repo_api}/commits/{sha}/builds", auth=bearer, query={"key": key}
        )
        if (build.get("key"), build.get("state")) != (key, "SUCCESSFUL"):
            raise FixtureError(f"the posted build status did not read back: {build}")
        log("ok: build status posted and read back")
        git_step([*git, "push", "-q", "origin", "--delete", branch], "git push --delete", network)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    # 4. The seeded users sign in and can reach the repository.
    for user, info in state["users"].items():
        password = Path(info["password_file"]).read_text().strip()
        auth = _basic(user, password)
        me = http.call("GET", f"/rest/api/latest/users/{info['slug']}", auth=auth)
        if str(me["id"]) != info["user_id"]:
            raise FixtureError(f"{user} has id {me['id']}, state.json says {info['user_id']}")
        repo = http.call("GET", repo_api, auth=auth)
        if str(repo["id"]) != state["repo_id"]:
            raise FixtureError(f"{user} sees repository id {repo['id']}")
    log(f"ok: users {', '.join(state['users'])} sign in and read the repository")
    print("bitbucket-dc fixture self-test passed")
    return 0


# --------------------------------------------------------------------------


def main(
    argv: list[str],
    *,
    runner: Runner,
    urlopen: Opener,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    parser = argparse.ArgumentParser(prog="fixture.py", description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=("up", "self-test", "down"))
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--advertise-host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args(argv)
    try:
        if args.action == "up":
            return up(args, runner, urlopen, clock)
        if args.action == "self-test":
            return self_test(args, runner, urlopen)
        return down(args, runner)
    except FixtureError as error:
        print(f"forge-fixture bitbucket-dc {args.action}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], runner=run_command, urlopen=refusing_urlopen))
