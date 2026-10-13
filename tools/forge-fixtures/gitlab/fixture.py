#!/usr/bin/env python3
# Embedded Ruby and Omnibus settings keep their own line lengths.
# ruff: noqa: E501, UP031
"""GitLab CE forge fixture for the factory end to end harness (#4399).

Usage:
    fixture.py up|self-test|down --state-dir <dir> [--advertise-host H] [--port P]

Standard library only: it runs under a bare ``python3`` and shells out to
``docker compose``, ``docker``, ``openssl`` and ``git``. Nothing is
bind-mounted, so it works against a remote Docker daemon: the compose file is
generated locally, containers are created, files are copied in with
``docker compose cp``, then the containers are started.

No token value is ever printed, logged, put in an exception message, or passed
on a command line. See README.md for the seeded data and the state.json schema.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import ipaddress
import json
import os
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
from pathlib import Path
from typing import Any

GITLAB_IMAGE = "gitlab/gitlab-ce:19.4.1-ce.0"
RUNNER_IMAGE = "gitlab/gitlab-runner:v19.4.0"
GITLAB_VERSION = "19.4.1"
PROJECT_PREFIX = "curie-forge-gitlab-"
FIXTURE_LABEL = "io.curie.forge-fixture"
DEFAULT_PORT = 28443
DEFAULT_HOST = "localhost"
UP_DEADLINE_S = 15 * 60
TOKEN_DAYS = 30
ROOT_TOKEN_DAYS = 4
RUNNER_JOB_WAIT_S = 300
RUNNER_ONLINE_WAIT_S = 180
REPO_ROOT = Path(__file__).resolve().parents[3]
# Set to 1 to leave a failed `up` running for debugging; `down` still cleans it.
KEEP_ENV = "CURIE_FORGE_FIXTURE_KEEP_ON_FAILURE"

GROUP_PATH = "factory-fixture"
SUBGROUP_PATH = "nested"
WIDGETS = f"{GROUP_PATH}/{SUBGROUP_PATH}/widgets"
RENAMABLE = f"{GROUP_PATH}/{SUBGROUP_PATH}/renamable"

WIDGET_SH = """\
# widget.sh: arithmetic helpers for the factory fixture.
add() { echo $(( $1 - $2 )); }
"""
TEST_SH = """\
#!/bin/sh
# test.sh: fails until add() in widget.sh adds.
. ./widget.sh
got="$(add 2 3)"
if [ "$got" != "5" ]; then
  echo "FAIL: add 2 3 = $got, want 5"
  exit 1
fi
echo "ok"
"""
GITLAB_CI = """\
test:
  script:
    - sh test.sh
"""


class FixtureError(Exception):
    """A fixture failure whose message is safe to print (never a secret)."""


def log(msg: str) -> None:
    print(f"[gitlab-fixture] {msg}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# Paths and state


def check_state_dir_location(state_dir: Path) -> None:
    resolved = state_dir.resolve()
    try:
        resolved.relative_to(REPO_ROOT)
    except ValueError:
        pass
    else:
        raise FixtureError(f"state dir {resolved} is inside the repository {REPO_ROOT}")
    probe = resolved
    while not probe.exists():
        probe = probe.parent
    inside = subprocess.run(
        ["git", "-C", str(probe), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
    )
    if inside.returncode == 0:
        raise FixtureError(f"state dir {resolved} is inside a git repository")


def write_private(path: Path, content: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(content)
    os.chmod(path, 0o600)


def save_state(state_dir: Path, state: dict[str, Any]) -> None:
    write_private(state_dir / "state.json", json.dumps(state, indent=2, sort_keys=True) + "\n")


def load_state(state_dir: Path) -> dict[str, Any]:
    path = state_dir / "state.json"
    if not path.is_file():
        raise FixtureError(f"no state.json in {state_dir}: is the fixture up?")
    return json.loads(path.read_text())


# --------------------------------------------------------------------------
# Subprocess helpers


def run(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = True,
    timeout: float | None = None,
    input_text: str | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    full_env = None
    if env is not None:
        full_env = dict(os.environ)
        full_env.update(env)
    proc = subprocess.run(
        cmd,
        env=full_env,
        capture_output=capture,
        text=True,
        timeout=timeout,
        input=input_text,
        cwd=cwd,
    )
    if check and proc.returncode != 0:
        # The command line never carries a secret, so it is safe to name.
        tail = (proc.stderr or "").strip().splitlines()[-5:] if capture else []
        raise FixtureError(
            f"command failed ({proc.returncode}): {' '.join(cmd[:4])}...: {' | '.join(tail)}"
        )
    return proc


def compose(
    state: dict[str, Any], state_dir: Path, *args: str, **kw: Any
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-p", state["project"]]
    compose_file = state_dir / "compose.json"
    if compose_file.is_file():
        cmd += ["-f", str(compose_file)]
    return run(cmd + list(args), **kw)


# --------------------------------------------------------------------------
# HTTP helpers


class Api:
    def __init__(self, api_url: str, ca_path: Path, token: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.ctx = ssl.create_default_context(cafile=str(ca_path))
        self._token = token

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        sudo: str | int | None = None,
        ok: tuple[int, ...] = (200, 201, 202, 204),
        timeout: float = 120,
    ) -> tuple[int, Any]:
        url = self.api_url + path
        data = None
        headers = {"PRIVATE-TOKEN": self._token, "Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if sudo is not None:
            headers["Sudo"] = str(sudo)
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        # Only idempotent methods retry; a retried POST could seed twice.
        attempts = 4 if method in ("GET", "PUT", "DELETE") else 1
        for attempt in range(1, attempts + 1):
            try:
                with urllib.request.urlopen(req, context=self.ctx, timeout=timeout) as resp:
                    status, raw = resp.status, resp.read()
            except urllib.error.HTTPError as exc:
                status, raw = exc.code, exc.read()
            except (urllib.error.URLError, OSError) as exc:
                if attempt < attempts:
                    time.sleep(5)
                    continue
                raise FixtureError(
                    f"{method} {path}: transport error {type(exc).__name__}"
                ) from None
            if status in (502, 503, 504) and attempt < attempts:
                time.sleep(5)
                continue
            break
        payload: Any = None
        if raw:
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = raw.decode(errors="replace")
        if status not in ok:
            detail = (
                payload.get("message") or payload.get("error")
                if isinstance(payload, dict)
                else None
            )
            raise FixtureError(
                f"{method} {path}: HTTP {status} {json.dumps(detail)[:300] if detail else ''}"
            )
        return status, payload

    def get(self, path: str, **kw: Any) -> Any:
        return self.request("GET", path, **kw)[1]

    def post(self, path: str, body: dict[str, Any] | None = None, **kw: Any) -> Any:
        return self.request("POST", path, body or {}, **kw)[1]

    def put(self, path: str, body: dict[str, Any], **kw: Any) -> Any:
        return self.request("PUT", path, body, **kw)[1]


def enc(path: str) -> str:
    return urllib.parse.quote(path, safe="")


# --------------------------------------------------------------------------
# Port, CA, compose file


def port_in_use(port: int) -> str | None:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return f"127.0.0.1:{port} accepts connections"
    except OSError:
        pass
    proc = run(["docker", "ps", "-a", "--filter", f"publish={port}", "--format", "{{.Names}}"])
    names = proc.stdout.split()
    if names:
        return f"docker containers publish {port}: {', '.join(names)}"
    return None


def make_certs(state_dir: Path, advertise_host: str) -> Path:
    tls = state_dir / "tls"
    tls.mkdir(mode=0o700)
    sans = ["DNS:localhost", "IP:127.0.0.1"]
    try:
        ipaddress.ip_address(advertise_host)
        extra = f"IP:{advertise_host}"
    except ValueError:
        extra = f"DNS:{advertise_host}"
    if extra not in sans:
        sans.append(extra)
    ca_key, ca_crt = tls / "ca.key", tls / "ca.crt"
    srv_key, srv_csr, srv_crt = tls / "server.key", tls / "server.csr", tls / "server.crt"
    ext = tls / "server.ext"
    ext.write_text(
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        f"subjectAltName={','.join(sans)}\n"
    )
    run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "30",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_crt),
            "-subj",
            "/CN=curie-forge-fixture-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(srv_key),
            "-out",
            str(srv_csr),
            "-subj",
            f"/CN={advertise_host}",
        ]
    )
    run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(srv_csr),
            "-CA",
            str(ca_crt),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-days",
            "30",
            "-out",
            str(srv_crt),
            "-extfile",
            str(ext),
        ]
    )
    for p in tls.iterdir():
        os.chmod(p, 0o600)
    # Staging dirs copied into the containers as whole directories.
    gl_ssl = tls / "gitlab-ssl"
    gl_ssl.mkdir()
    shutil.copy(srv_crt, gl_ssl / "fixture.crt")
    shutil.copy(srv_key, gl_ssl / "fixture.key")
    gl_trust = tls / "gitlab-trusted-certs"
    gl_trust.mkdir()
    shutil.copy(ca_crt, gl_trust / "curie-fixture-ca.crt")
    runner_certs = tls / "runner-certs"
    runner_certs.mkdir()
    shutil.copy(ca_crt, runner_certs / "ca.crt")
    ca_path = state_dir / "ca.crt"
    shutil.copy(ca_crt, ca_path)
    os.chmod(ca_path, 0o600)
    return ca_path


def omnibus_config(advertise_host: str, port: int) -> str:
    host = f"[{advertise_host}]" if ":" in advertise_host else advertise_host
    return (
        "\n".join(
            [
                f"external_url 'https://{host}:{port}'",
                f"nginx['listen_port'] = {port}",
                "nginx['listen_https'] = true",
                "nginx['redirect_http_to_https'] = false",
                "nginx['ssl_certificate'] = '/etc/gitlab/ssl/fixture.crt'",
                "nginx['ssl_certificate_key'] = '/etc/gitlab/ssl/fixture.key'",
                "letsencrypt['enable'] = false",
                "puma['worker_processes'] = 0",
                "sidekiq['concurrency'] = 10",
                "prometheus_monitoring['enable'] = false",
                "gitlab_rails['monitoring_whitelist'] = ['0.0.0.0/0', '::/0']",
                "gitlab_rails['usage_ping_enabled'] = false",
                "gitlab_rails['rack_attack_git_basic_auth'] = { 'enabled' => false }",
                "gitlab_kas['enable'] = false",
                "registry['enable'] = false",
                "gitlab_rails['env'] = { 'MALLOC_CONF' => 'dirty_decay_ms:1000,muzzy_decay_ms:1000' }",
                "gitaly['env'] = { 'MALLOC_CONF' => 'dirty_decay_ms:1000,muzzy_decay_ms:1000', 'GITALY_COMMAND_SPAWN_MAX_PARALLEL' => '2' }",
            ]
        )
        + "\n"
    )


def compose_document(project: str, advertise_host: str, port: int) -> dict[str, Any]:
    labels = {FIXTURE_LABEL: "gitlab"}
    volumes = ["gitlab-config", "gitlab-logs", "gitlab-data", "runner-config", "runner-home"]
    return {
        "name": project,
        "services": {
            "gitlab": {
                "image": GITLAB_IMAGE,
                "hostname": "gitlab",
                "mem_limit": "4g",
                "shm_size": "256m",
                "ports": [f"{port}:{port}"],
                "environment": {"GITLAB_OMNIBUS_CONFIG": omnibus_config(advertise_host, port)},
                "labels": labels,
                "volumes": [
                    "gitlab-config:/etc/gitlab",
                    "gitlab-logs:/var/log/gitlab",
                    "gitlab-data:/var/opt/gitlab",
                ],
            },
            "runner": {
                "image": RUNNER_IMAGE,
                "mem_limit": "512m",
                "network_mode": "service:gitlab",
                "depends_on": ["gitlab"],
                "labels": labels,
                "volumes": [
                    "runner-config:/etc/gitlab-runner",
                    "runner-home:/home/gitlab-runner",
                ],
            },
        },
        "volumes": {v: {"labels": labels} for v in volumes},
        "networks": {"default": {"labels": labels}},
    }


# --------------------------------------------------------------------------
# up


class Deadline:
    def __init__(self, seconds: float) -> None:
        self.end = time.monotonic() + seconds

    def remaining(self) -> float:
        return self.end - time.monotonic()

    def check(self, what: str) -> None:
        if self.remaining() <= 0:
            raise FixtureError(
                f"up exceeded its {UP_DEADLINE_S // 60} minute deadline while {what}"
            )


def wait_http(local_base: str, ca_path: Path, deadline: Deadline) -> None:
    ctx = ssl.create_default_context(cafile=str(ca_path))
    start = time.monotonic()
    last = ""
    while True:
        deadline.check("waiting for GitLab HTTP")
        try:
            with urllib.request.urlopen(
                local_base + "/users/sign_in", context=ctx, timeout=10
            ) as resp:
                if resp.status == 200:
                    log(f"GitLab answers HTTP after {int(time.monotonic() - start)} s")
                    return
                last = f"HTTP {resp.status}"
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
        except (urllib.error.URLError, OSError) as exc:
            last = type(exc).__name__
        elapsed = int(time.monotonic() - start)
        if elapsed and elapsed % 60 < 5:
            log(f"still waiting for GitLab HTTP ({elapsed} s, last: {last})")
        time.sleep(5)


ROOT_TOKEN_RUBY = (
    """\
u = User.find_by_username!('root')
attrs = { name: 'curie-forge-fixture-root', scopes: [:api, :sudo, :admin_mode, :read_api, :write_repository],
          expires_at: %d.days.from_now.to_date }
if PersonalAccessToken.column_names.include?('organization_id')
  org = (u.respond_to?(:organizations) && u.organizations.first) || Organizations::Organization.first
  attrs[:organization_id] = org.id
end
t = u.personal_access_tokens.build(attrs)
t.save!
STDOUT.puts 'CURIE_FIXTURE_ROOT_TOKEN=' + t.token
"""
    % ROOT_TOKEN_DAYS
)


def create_root_token(state: dict[str, Any], state_dir: Path, deadline: Deadline) -> str:
    start = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        deadline.check("creating the root token")
        proc = compose(
            state,
            state_dir,
            "exec",
            "-T",
            "gitlab",
            "gitlab-rails",
            "runner",
            ROOT_TOKEN_RUBY,
            check=False,
            timeout=max(30.0, min(600.0, deadline.remaining())),
        )
        for line in proc.stdout.splitlines():
            if line.startswith("CURIE_FIXTURE_ROOT_TOKEN="):
                log(f"root token created after {int(time.monotonic() - start)} s")
                return line.split("=", 1)[1].strip()
        # stderr can carry a Ruby backtrace but never the token (it is only on stdout).
        tail = " | ".join((proc.stderr or "").strip().splitlines()[-3:])
        log(f"root token attempt {attempt} failed (exit {proc.returncode}): {tail[:400]}")
        if attempt >= 4:
            raise FixtureError("could not create the root token with gitlab-rails runner")
        time.sleep(15)


def future_date(days: int) -> str:
    return (dt.date.today() + dt.timedelta(days=days)).isoformat()


def random_password() -> str:
    return "Fx1!" + secrets.token_urlsafe(24)


def seed(api: Api, state: dict[str, Any], state_dir: Path) -> None:
    root = api.get("/user")
    state["users"] = {"root": {"id": root["id"], "username": root["username"]}}

    log("seeding groups and projects")
    group = api.post("/groups", {"name": GROUP_PATH, "path": GROUP_PATH, "visibility": "private"})
    sub = api.post(
        "/groups",
        {
            "name": SUBGROUP_PATH,
            "path": SUBGROUP_PATH,
            "parent_id": group["id"],
            "visibility": "private",
        },
    )
    state["group"] = {"id": group["id"], "path": group["full_path"]}
    state["subgroup"] = {"id": sub["id"], "path": sub["full_path"]}

    widgets = api.post(
        "/projects",
        {
            "name": "widgets",
            "path": "widgets",
            "namespace_id": sub["id"],
            "visibility": "private",
            "issues_enabled": True,
            "issues_access_level": "enabled",
            "merge_requests_access_level": "enabled",
            "builds_access_level": "enabled",
            "initialize_with_readme": False,
            "default_branch": "main",
        },
    )
    api.post(
        f"/projects/{widgets['id']}/repository/commits",
        {
            "branch": "main",
            "commit_message": "Add widget arithmetic with a failing test",
            "actions": [
                {"action": "create", "file_path": "widget.sh", "content": WIDGET_SH},
                {"action": "create", "file_path": "test.sh", "content": TEST_SH},
                {"action": "create", "file_path": ".gitlab-ci.yml", "content": GITLAB_CI},
                {
                    "action": "create",
                    "file_path": "README.md",
                    "content": "# widgets\n\nFixture project. `sh test.sh` fails until `add` in widget.sh adds.\n",
                },
            ],
        },
    )
    api.put(f"/projects/{widgets['id']}", {"default_branch": "main"})
    # GitLab protects the default branch asynchronously after the first
    # commit, so a protect call can race it and get 422 "already been taken".
    # Either way, confirm main ends up protected for Maintainers only.
    protected = f"/projects/{widgets['id']}/protected_branches"
    status, _ = api.request("GET", f"{protected}/main", ok=(200, 404))
    if status == 404:
        api.request(
            "POST",
            protected,
            {"name": "main", "push_access_level": 40, "merge_access_level": 40},
            ok=(200, 201, 409, 422),
        )
    for _ in range(20):
        status, rule = api.request("GET", f"{protected}/main", ok=(200, 404))
        if status == 200:
            break
        time.sleep(1)
    else:
        raise FixtureError("main on widgets did not become protected")
    if any(a.get("access_level") != 40 for a in rule.get("push_access_levels", [])):
        raise FixtureError("main on widgets is protected but not Maintainers-only for push")
    widgets = api.get(f"/projects/{widgets['id']}")
    if widgets["path_with_namespace"] != WIDGETS or not widgets.get("issues_enabled", True):
        raise FixtureError("widgets project was not created as expected")

    renamable = api.post(
        "/projects",
        {
            "name": "renamable",
            "path": "renamable",
            "namespace_id": sub["id"],
            "visibility": "private",
            "initialize_with_readme": True,
            "default_branch": "main",
        },
    )
    state["projects"] = {
        "widgets": {
            "id": widgets["id"],
            "path": widgets["path_with_namespace"],
            "default_branch": widgets["default_branch"],
            "http_url_to_repo": widgets["http_url_to_repo"],
        },
        "renamable": {
            "id": renamable["id"],
            "path": renamable["path_with_namespace"],
            "default_branch": renamable.get("default_branch") or "main",
        },
    }

    log("seeding users and memberships")
    for username, level in (("developer", 30), ("reporter", 20)):
        user = api.post(
            "/users",
            {
                "username": username,
                "name": username.capitalize(),
                "email": f"{username}@fixture.invalid",
                "password": random_password(),
                "skip_confirmation": True,
            },
        )
        api.post(f"/groups/{group['id']}/members", {"user_id": user["id"], "access_level": level})
        state["users"][username] = {
            "id": user["id"],
            "username": user["username"],
            "access_level": level,
        }

    sa_status, sa = api.request(
        "POST",
        f"/groups/{group['id']}/service_accounts",
        {"name": "Factory Bot", "username": "factory-bot"},
        ok=(200, 201, 403, 404),
    )
    if sa_status in (200, 201):
        sa_kind = "group_service_account"
        sa_token_path = f"/groups/{group['id']}/service_accounts/{sa['id']}/personal_access_tokens"
    else:
        log(
            f"group service accounts unavailable (HTTP {sa_status}); using a regular user as the bot"
        )
        sa = api.post(
            "/users",
            {
                "username": "factory-bot",
                "name": "Factory Bot",
                "email": "factory-bot@fixture.invalid",
                "password": random_password(),
                "skip_confirmation": True,
            },
        )
        sa_kind = "user"
        sa_token_path = f"/users/{sa['id']}/personal_access_tokens"
    api.post(f"/groups/{group['id']}/members", {"user_id": sa["id"], "access_level": 30})
    sa_user = api.get(f"/users/{sa['id']}")
    state["users"]["service_account"] = {
        "id": sa["id"],
        "username": sa["username"],
        "access_level": 30,
        "kind": sa_kind,
        "bot": bool(sa_user.get("bot")),
    }

    log("minting tokens")
    expires = future_date(TOKEN_DAYS)
    rw_scopes = ["api", "write_repository"]
    tokens: dict[str, Any] = {}
    plan = [
        ("service_account", sa_token_path, rw_scopes, "curie-factory", sa),
        (
            "reporter",
            f"/users/{state['users']['reporter']['id']}/personal_access_tokens",
            rw_scopes,
            "curie-fixture-reporter",
            state["users"]["reporter"],
        ),
        ("read_api_bot", sa_token_path, ["read_api"], "curie-fixture-read-api", sa),
    ]
    for key, path, scopes, name, owner in plan:
        tok = api.post(path, {"name": name, "scopes": scopes, "expires_at": expires})
        token_file = state_dir / f"token-{key.replace('_', '-')}"
        write_private(token_file, tok["token"])
        tokens[key] = {
            "file": str(token_file),
            "expires_at": tok["expires_at"],
            "scopes": tok["scopes"],
            "token_id": tok["id"],
        }
        if key == "read_api_bot":
            tokens[key].update({"user_id": owner["id"], "username": owner["username"]})
    state["tokens"] = tokens


def register_runner(api: Api, state: dict[str, Any], state_dir: Path, deadline: Deadline) -> None:
    log("registering the runner")
    runner = api.post(
        "/user/runners",
        {
            "runner_type": "instance_type",
            "run_untagged": True,
            "description": "curie-forge-fixture",
        },
    )
    port = state["port"]
    config = "\n".join(
        [
            "concurrent = 2",
            "check_interval = 3",
            "shutdown_timeout = 0",
            "",
            "[[runners]]",
            '  name = "curie-forge-fixture"',
            f'  url = "https://localhost:{port}"',
            f'  clone_url = "https://localhost:{port}"',
            f'  token = "{runner["token"]}"',
            '  tls-ca-file = "/etc/gitlab-runner/certs/ca.crt"',
            '  executor = "shell"',
            '  shell = "bash"',
            '  builds_dir = "/home/gitlab-runner/builds"',
            '  cache_dir = "/home/gitlab-runner/cache"',
            "",
        ]
    )
    staging = Path(tempfile.mkdtemp(prefix="runner-", dir=state_dir))
    try:
        cfg = staging / "config.toml"
        write_private(cfg, config)
        compose(state, state_dir, "cp", str(cfg), "runner:/etc/gitlab-runner/config.toml")
        compose(
            state,
            state_dir,
            "cp",
            # The image already ships /etc/gitlab-runner/certs, so copy the
            # directory's contents ("/."), not the directory, or it nests.
            str(state_dir / "tls" / "runner-certs") + "/.",
            "runner:/etc/gitlab-runner/certs",
        )
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    compose(state, state_dir, "start", "runner")
    state["runner"] = {"id": runner["id"]}
    end = time.monotonic() + RUNNER_ONLINE_WAIT_S
    while time.monotonic() < end:
        deadline.check("waiting for the runner to come online")
        online = api.get("/runners/all?status=online&type=instance_type")
        if any(r["id"] == runner["id"] for r in online):
            log("runner is online")
            return
        time.sleep(3)
    proc = compose(state, state_dir, "logs", "--no-color", "--tail", "20", "runner", check=False)
    print(proc.stdout, file=sys.stderr, end="")
    raise FixtureError(f"runner {runner['id']} did not come online within {RUNNER_ONLINE_WAIT_S} s")


def cmd_up(args: argparse.Namespace) -> int:
    state_dir = Path(args.state_dir).expanduser().absolute()
    check_state_dir_location(state_dir)
    if state_dir.exists() and any(state_dir.iterdir()):
        raise FixtureError(f"state dir {state_dir} already exists and is not empty; run down first")
    advertise_host, port = args.advertise_host, args.port
    taken = port_in_use(port)
    if taken:
        raise FixtureError(f"port {port} is taken ({taken}); pass --port")

    started = time.monotonic()
    deadline = Deadline(UP_DEADLINE_S)
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state_dir, 0o700)
    project = PROJECT_PREFIX + secrets.token_hex(4)
    url_host = f"[{advertise_host}]" if ":" in advertise_host else advertise_host
    base_url = f"https://{url_host}:{port}"
    local_base = f"https://localhost:{port}"
    state: dict[str, Any] = {
        "project": project,
        "base_url": base_url,
        "local_base_url": local_base,
        "api_url": base_url + "/api/v4",
        "local_api_url": local_base + "/api/v4",
        "advertise_host": advertise_host,
        "port": port,
        "version": GITLAB_VERSION,
        "status": "starting",
    }
    save_state(state_dir, state)
    log(f"compose project {project}, state dir {state_dir}")
    try:
        ca_path = make_certs(state_dir, advertise_host)
        state["ca_path"] = str(ca_path)
        (state_dir / "compose.json").write_text(
            json.dumps(compose_document(project, advertise_host, port), indent=2) + "\n"
        )
        os.chmod(state_dir / "compose.json", 0o600)
        save_state(state_dir, state)

        log("creating containers")
        compose(state, state_dir, "create")
        compose(
            state,
            state_dir,
            "cp",
            str(state_dir / "tls" / "gitlab-ssl") + "/.",
            "gitlab:/etc/gitlab/ssl",
        )
        compose(
            state,
            state_dir,
            "cp",
            str(state_dir / "tls" / "gitlab-trusted-certs") + "/.",
            "gitlab:/etc/gitlab/trusted-certs",
        )
        log("starting GitLab (first HTTP takes about 2 to 4 minutes)")
        compose(state, state_dir, "start", "gitlab")
        wait_http(local_base, ca_path, deadline)

        log("creating the root token with gitlab-rails runner (under a minute to 2 minutes)")
        root_token = create_root_token(state, state_dir, deadline)
        root_file = state_dir / "token-root"
        write_private(root_file, root_token)
        state["root_token_file"] = str(root_file)
        save_state(state_dir, state)

        api = Api(state["local_api_url"], ca_path, root_token)
        version = api.get("/version")
        state["server_version"] = version.get("version")
        seed(api, state, state_dir)
        save_state(state_dir, state)
        register_runner(api, state, state_dir, deadline)
        state["status"] = "up"
        save_state(state_dir, state)
    except BaseException as exc:
        reason = str(exc) if isinstance(exc, FixtureError) else type(exc).__name__
        log(f"up failed: {reason}")
        dump_logs(state, state_dir)
        if os.environ.get(KEEP_ENV) == "1":
            log(f"{KEEP_ENV}=1: leaving the fixture running; run down to remove it")
        else:
            log("tearing down")
            try:
                teardown(state, state_dir)
            except FixtureError as td:
                log(f"teardown also failed: {td}")
        if isinstance(exc, KeyboardInterrupt):
            return 130
        if not isinstance(exc, FixtureError):
            raise
        return 1
    log(f"up finished in {int(time.monotonic() - started)} s")
    print(str(state_dir / "state.json"))
    return 0


def dump_logs(state: dict[str, Any], state_dir: Path) -> None:
    try:
        proc = compose(
            state,
            state_dir,
            "logs",
            "--no-color",
            "--tail",
            "100",
            "gitlab",
            check=False,
            timeout=60,
        )
    except (subprocess.TimeoutExpired, OSError):
        return
    print("----- last 100 GitLab log lines -----", file=sys.stderr)
    print(proc.stdout, file=sys.stderr, end="")
    print("----- end of GitLab logs -----", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# down


def leftovers(project: str) -> list[str]:
    flt = f"label=com.docker.compose.project={project}"
    found: list[str] = []
    for kind, cmd in (
        ("container", ["docker", "ps", "-a", "-q", "--filter", flt]),
        ("volume", ["docker", "volume", "ls", "-q", "--filter", flt]),
        ("network", ["docker", "network", "ls", "-q", "--filter", flt]),
    ):
        proc = run(cmd, check=False)
        if proc.returncode != 0:
            raise FixtureError(
                f"listing {kind}s for {project} failed (exit {proc.returncode}); cannot confirm teardown"
            )
        found += [f"{kind} {x}" for x in proc.stdout.split()]
    return found


def teardown(state: dict[str, Any], state_dir: Path) -> None:
    project = state["project"]
    if not project.startswith(PROJECT_PREFIX):
        raise FixtureError(f"refusing to tear down project {project!r}: not a fixture project")
    proc = compose(
        state,
        state_dir,
        "down",
        "-v",
        "--remove-orphans",
        "--timeout",
        "10",
        check=False,
        timeout=300,
    )
    if proc.returncode != 0:
        log(
            f"compose down exited {proc.returncode}: {' | '.join(proc.stderr.strip().splitlines()[-3:])}"
        )
    remaining = leftovers(project)
    if remaining:
        raise FixtureError(f"resources remain after down: {', '.join(remaining)}")
    shutil.rmtree(state_dir)
    log(f"removed {project} and {state_dir}")


def cmd_down(args: argparse.Namespace) -> int:
    state_dir = Path(args.state_dir).expanduser().absolute()
    if not state_dir.is_dir():
        raise FixtureError(f"state dir {state_dir} does not exist; nothing to tear down")
    teardown(load_state(state_dir), state_dir)
    return 0


# --------------------------------------------------------------------------
# self-test


def git_env(header: str, ca_path: str) -> dict[str, str]:
    return {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "",
        "SSH_ASKPASS": "",
        "GIT_SSL_CAINFO": ca_path,
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "http.extraHeader",
        "GIT_CONFIG_VALUE_0": header,
        "GIT_CONFIG_KEY_1": "credential.helper",
        "GIT_CONFIG_VALUE_1": "",
    }


def cmd_self_test(args: argparse.Namespace) -> int:
    state_dir = Path(args.state_dir).expanduser().absolute()
    state = load_state(state_dir)
    if state.get("status") != "up":
        raise FixtureError("the fixture did not finish up; run down and up again")
    ca = state["ca_path"]
    root_token = Path(state["root_token_file"]).read_text().strip()
    api = Api(state["local_api_url"], Path(ca), root_token)
    widgets = state["projects"]["widgets"]
    results: list[tuple[str, bool, str]] = []

    def check(name: str, fn: Any) -> None:
        try:
            ok, detail = fn()
        except FixtureError as exc:
            ok, detail = False, str(exc)
        except Exception as exc:  # noqa: BLE001 - report any failure as a failed check
            ok, detail = False, type(exc).__name__
        results.append((name, ok, detail))
        print(f"{'PASS' if ok else 'FAIL'} {name}: {detail}", flush=True)

    def tls_check() -> tuple[bool, str]:
        api.get("/version")
        try:
            with urllib.request.urlopen(
                state["local_base_url"] + "/users/sign_in",
                context=ssl.create_default_context(),
                timeout=15,
            ):
                return False, "default trust accepted the fixture certificate"
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, ssl.SSLCertVerificationError):
                return True, "verifies with the fixture CA, refused by default trust"
            return False, f"default trust failed for another reason: {type(exc.reason).__name__}"

    def path_check() -> tuple[bool, str]:
        proj = api.get(f"/projects/{widgets['id']}")
        segs = proj["path_with_namespace"].split("/")
        return len(segs) == 3, f"{proj['path_with_namespace']} has {len(segs)} segments"

    def access_check() -> tuple[bool, str]:
        got = {}
        for name in ("developer", "reporter"):
            uid = state["users"][name]["id"]
            got[name] = api.get(f"/projects/{widgets['id']}/members/all/{uid}")["access_level"]
        return got == {
            "developer": 30,
            "reporter": 20,
        }, f"developer={got['developer']} reporter={got['reporter']}"

    branch = f"selftest-{secrets.token_hex(4)}"
    pushed = {"done": False}

    def git_check() -> tuple[bool, str]:
        token = Path(state["tokens"]["service_account"]["file"]).read_text().strip()
        basic = "Authorization: Basic " + base64.b64encode(f"oauth2:{token}".encode()).decode()
        bearer = f"Authorization: Bearer {token}"
        url = f"{state['local_base_url']}/{widgets['path']}.git"
        with tempfile.TemporaryDirectory(prefix="gitlab-selftest-") as tmp:
            work = Path(tmp) / "widgets"
            env = git_env(basic, ca)
            run(["git", "clone", "--quiet", url, str(work)], env=env, timeout=120)
            run(["git", "-C", str(work), "checkout", "-q", "-b", branch], env=env)
            (work / "selftest.txt").write_text(f"{branch}\n")
            run(["git", "-C", str(work), "add", "selftest.txt"], env=env)
            run(
                [
                    "git",
                    "-C",
                    str(work),
                    "-c",
                    "user.name=Fixture Self Test",
                    "-c",
                    "user.email=selftest@fixture.invalid",
                    "commit",
                    "-q",
                    "-m",
                    "Self test",
                ],
                env=env,
            )
            run(
                ["git", "-C", str(work), "push", "-q", "origin", f"HEAD:refs/heads/{branch}"],
                env=env,
                timeout=120,
            )
            pushed["done"] = True
            refused = run(
                [
                    "git",
                    "-C",
                    str(work),
                    "push",
                    "-q",
                    "origin",
                    f"HEAD:refs/heads/{branch}-bearer",
                ],
                env=git_env(bearer, ca),
                check=False,
                timeout=120,
            )
        if refused.returncode == 0:
            return False, "Bearer push was accepted"
        return (
            True,
            f"Basic oauth2 clone and push of {branch} ok; Bearer push refused (exit {refused.returncode})",
        )

    def runner_check() -> tuple[bool, str]:
        online = api.get("/runners/all?status=online&type=instance_type")
        if not online:
            return False, "no instance runner is online"
        pid = widgets["id"]
        main_branch = widgets["default_branch"]
        pipes = api.get(f"/projects/{pid}/pipelines?ref={enc(main_branch)}")
        if not pipes:
            pipes = [api.post(f"/projects/{pid}/pipeline?ref={enc(main_branch)}")]
        pipe_id = pipes[0]["id"]
        end = time.monotonic() + RUNNER_JOB_WAIT_S
        status = "none"
        while time.monotonic() < end:
            jobs = api.get(f"/projects/{pid}/pipelines/{pipe_id}/jobs")
            status = ",".join(f"{j['name']}={j['status']}" for j in jobs) or "no jobs"
            ran = [j for j in jobs if j["status"] in ("success", "failed") and j.get("runner")]
            if jobs and len(ran) == len(jobs):
                return True, f"pipeline {pipe_id} on {main_branch} ran on the runner: {status}"
            time.sleep(5)
        return (
            False,
            f"pipeline {pipe_id} had no finished job within {RUNNER_JOB_WAIT_S} s ({status})",
        )

    try:
        check("tls", tls_check)
        check("widgets-path", path_check)
        check("access-levels", access_check)
        check("git-basic-oauth2", git_check)
        check("runner-job", runner_check)
    finally:
        if pushed["done"]:
            api.request(
                "DELETE",
                f"/projects/{widgets['id']}/repository/branches/{enc(branch)}",
                ok=(204, 404),
            )
    failed = [n for n, ok, _ in results if not ok]
    print(f"self-test: {len(results) - len(failed)}/{len(results)} passed", flush=True)
    return 1 if failed else 0


# --------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="fixture.py", description="GitLab CE forge fixture")
    parser.add_argument("action", choices=["up", "self-test", "down"])
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--advertise-host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args(argv)
    handlers = {"up": cmd_up, "self-test": cmd_self_test, "down": cmd_down}
    try:
        return handlers[args.action](args)
    except FixtureError as exc:
        log(f"error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
