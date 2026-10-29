"""Recovery administration through real routes, PostgreSQL and private S3 objects."""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import logging
import os
import stat
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from botocore.exceptions import ClientError
from curie_api.config import get_settings
from curie_api.forges.registry import forges_registry
from curie_api.main import create_app
from curie_internal.sandbox_token import mint
from curie_worker.workspace_recovery import WorkspaceRecoveryClient
from curie_worker.workspace_recovery_storage import (
    RecoveryCollector,
    RecoveryObjectStore,
    build_recovery_s3_client,
)
from fastapi.testclient import TestClient
from forge_fakes.github import forges_env_json
from test_console_session_api_auth import _cookie, _exchange, _expire_sessions
from test_workspace_recovery import (
    ARCHIVE_LIMIT,
    BASE,
    _authority,
    _ok,
    _post,
    _seed,
    _sql,
    recovery,  # noqa: F401
)

REPOSITORY = {
    "provider": "gitlab",
    "host": "forge.example.com",
    "project_id": "101",
    "path": "acme-corp/acme-bot",
}
CANARY = b"private-source-canary-5144"
LIST_FIELDS = {
    "work_item_id", "repository", "capture_outcome", "capture_outcome_at", "checkpoints"
}
CHECKPOINT_FIELDS = {
    "checkpoint_id", "work_item_id", "source_request_id", "repository", "base_commit",
    "target_branch", "source_sequence", "source_status", "completed_at", "expires_at",
    "age_seconds", "status", "available", "total_bytes", "objects",
}
OBJECT_FIELDS = {"kind", "size_bytes", "sha256"}
pytestmark = pytest.mark.usefixtures("recovery")


@dataclass
class StoreProxy:
    upstream: str
    endpoint: str = ""
    mode: str = ""
    seen: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    requests: list[tuple[str, float]] = field(default_factory=list)

    def arm(self, mode: str) -> None:
        self.mode = mode
        self.seen.clear()
        self.release.clear()


@contextmanager
def _store_proxy(upstream: str) -> Iterator[StoreProxy]:
    proxy = StoreProxy(upstream)
    target = urlsplit(upstream)
    assert target.scheme == "http", "the disposable store must expose HTTP"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def forward(self) -> None:
            signed = self.command == "GET" and "X-Amz-Signature=" in self.path
            mode = proxy.mode if signed else ""
            if signed:
                proxy.requests.append((self.path, time.monotonic()))
                proxy.seen.set()
            if mode == "headers":
                if not proxy.release.wait(15):
                    return
            if mode == "failure":
                self.send_response(503)
                self.send_header("Content-Length", str(len(CANARY)))
                self.end_headers()
                self.wfile.write(CANARY)
                return
            connection = http.client.HTTPConnection(target.hostname, target.port, timeout=15)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length else None
                connection.request(
                    self.command, self.path, body=body,
                    headers={key: value for key, value in self.headers.items()},
                )
                response = connection.getresponse()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in {"connection", "transfer-encoding"}:
                        self.send_header(key, value)
                self.end_headers()
                if mode == "body" and not proxy.release.wait(15):
                    return
                while chunk := response.read(65536):
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                connection.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    proxy.endpoint = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy
    finally:
        proxy.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@dataclass
class Access:
    client: TestClient
    s3: Any
    bucket: str
    proxy: StoreProxy
    document: dict[str, Any]
    monkeypatch: pytest.MonkeyPatch
    key_dir: Path
    forge_state: dict[str, Any]

    @property
    def headers(self) -> dict[str, str]:
        return {"X-API-Key": get_settings().api_key}

    def policy(self, *, bases: list[str] | None = None, bound: bool = True) -> None:
        binding = self.document["bindings"][0]
        binding["repos"][0]["bases"] = ["main"] if bases is None else bases
        document = json.loads(json.dumps(self.document))
        if not bound:
            document["bindings"] = []
        self.monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, self.key_dir))
        get_settings.cache_clear()
        self.client.app.state.forges = forges_registry(get_settings())

    def source(self, **kwargs: Any) -> dict[str, Any]:
        agent = kwargs.pop("agent_id", None) or str(uuid.uuid4())
        item = kwargs.pop("item_id", None) or str(uuid.uuid4())
        if not _sql("SELECT id FROM curie.agents WHERE id=:id", id=uuid.UUID(agent)):
            _sql("INSERT INTO curie.agents (id,name) VALUES (:id,:name)",
                 id=uuid.UUID(agent), name=f"acme-access-{uuid.uuid4().hex[:12]}")
        if not _sql("SELECT id FROM curie.work_items WHERE id=:id", id=uuid.UUID(item)):
            _sql(
                "INSERT INTO curie.work_items "
                "(id,tracker_kind,tracker_host,tracker_scope_id,tracker_issue_id,"
                "code_host_kind,code_host_host,repository_project_id,repository_path,"
                "agent_id,conversation_id,version,next_sequence,"
                "base_branch,base_source,base_commit) "
                "VALUES (:id,'gitlab',:host,'101',:issue,'gitlab',:host,'101',:path,"
                ":agent,:conversation,7,2,'main','default',:base)",
                id=uuid.UUID(item), agent=uuid.UUID(agent), host=REPOSITORY["host"],
                path=REPOSITORY["path"], issue=str(uuid.uuid4().int), base=BASE,
                conversation=f"gitlab:acme-corp/acme-bot:{uuid.uuid4().int}",
            )
        return _seed(agent_id=agent, item_id=item, **kwargs)

    def capture(
        self, source: dict[str, Any], *, reuse: bool = False, complete: bool = True
    ) -> tuple[dict[str, Any], dict[str, bytes]]:
        payloads = {
            "base": b"base archive " + CANARY,
            "patch": b"diff --git a/example b/example\n+" + CANARY,
            "manifest": b'{"capture":"' + CANARY + b'"}',
        }
        declarations = [
            {"kind": kind, "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for kind, data in payloads.items() if not (reuse and kind == "base")
        ]
        reservation = _ok(_post(self.client, "reserve", {
            **_authority(source), "idempotency_key": uuid.uuid4().hex,
            "repository": REPOSITORY, "base_commit": BASE, "target_branch": "main",
            "base_archive_limit_bytes": ARCHIVE_LIMIT, "objects": declarations,
        }))
        for obj in reservation["objects"]:
            self.s3.put_object(
                Bucket=self.bucket, Key=f"private/{obj['key']}", Body=payloads[obj["kind"]]
            )
        if complete:
            _ok(_post(self.client, "complete", {
                **_authority(source), "checkpoint_id": reservation["checkpoint_id"],
                "receipts": reservation["objects"],
            }))
        return reservation, payloads

    def get(self, source: dict[str, Any], checkpoint: dict[str, Any] | None = None,
            kind: str | None = None, **kwargs: Any) -> httpx.Response:
        return self.client.get(_path(source, checkpoint, kind), headers=self.headers, **kwargs)


@pytest.fixture
def access(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[Access]:
    settings = get_settings()
    s3 = build_recovery_s3_client(
        endpoint_url=settings.s3_endpoint_url, access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key, region=settings.s3_region,
    )
    bucket = f"curie-recovery-access-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=bucket)
    forge_state: dict[str, Any] = {"default_branch": "main", "id": 101, "unavailable": False}

    class Forge(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            # GitLab REST v4: https://docs.gitlab.com/api/projects/#get-a-single-project
            assert self.path == "/api/v4/projects/101", self.path
            data = json.dumps({
                "id": forge_state["id"], "path_with_namespace": REPOSITORY["path"],
                "default_branch": forge_state["default_branch"],
            }).encode()
            self.send_response(503 if forge_state["unavailable"] else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    forge = ThreadingHTTPServer(("127.0.0.1", 0), Forge)
    forge_thread = threading.Thread(target=forge.serve_forever, daemon=True)
    try:
        forge_thread.start()
        with _store_proxy(settings.s3_endpoint_url) as proxy:
            entry = {
                "name": "forge", "kind": "gitlab", "host": REPOSITORY["host"],
                "api_url": f"http://127.0.0.1:{forge.server_port}/api/v4",
                "credential": {"type": "static_token", "token": "example-forge-token"},
            }
            document = {
                "code_hosts": [entry],
                "trackers": [{**entry, "label": "factory", "mention": "curie"}],
                "bindings": [{
                    "name": "repository", "tracker": "forge", "tracker_scope_id": "101",
                    "repos": [{
                        "alias": "repo", "code_host": "forge", "project_id": "101",
                        "path": REPOSITORY["path"], "bases": ["main"],
                    }],
                }],
            }
            monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path))
            monkeypatch.setenv("S3_ENDPOINT_URL", proxy.endpoint)
            monkeypatch.setenv("CURIE_WORKSPACE_BUCKET", bucket)
            monkeypatch.setenv("CURIE_WORKSPACE_REFERENCE_TTL_SECONDS", "5")
            monkeypatch.setenv("CURIE_WORKSPACE_OBJECT_PREFIX", "claims-only")
            get_settings.cache_clear()
            with TestClient(create_app()) as client:
                yield Access(
                    client, s3, bucket, proxy, document, monkeypatch, tmp_path, forge_state
                )
    finally:
        forge.shutdown()
        forge.server_close()
        if forge_thread.ident is not None:
            forge_thread.join(timeout=5)
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in objects]}
            )
        s3.delete_bucket(Bucket=bucket)
        assert bucket not in {row["Name"] for row in s3.list_buckets()["Buckets"]}
        get_settings.cache_clear()


def _path(source: dict[str, Any], checkpoint: dict[str, Any] | None = None,
          kind: str | None = None) -> str:
    result = f"/work-items/{source['work_item_id']}/recovery"
    if checkpoint is not None:
        result += f"/{checkpoint['checkpoint_id']}"
    return result + (f"/objects/{kind}" if kind is not None else "")


def _refused(response: httpx.Response, status: int = 409) -> None:
    assert response.status_code == status, response.text
    if status not in {401, 404, 422}:
        assert response.json()["detail"]["code"].startswith("workspace_recovery.")
    _private(response.text + str(dict(response.headers)))


def _private(value: str) -> None:
    for forbidden in (CANARY.decode(), "X-Amz-", "x-amz-signature", "private/workspace-recovery/",
                      "rustfssecret", "example-forge-token"):
        assert forbidden.casefold() not in value.casefold()


def _readers(source: dict[str, Any]) -> list[dict[str, Any]]:
    return _sql(
        "SELECT *, clock_timestamp() AS observed_at FROM curie.workspace_recovery_readers "
        "WHERE work_item_id=:id", id=uuid.UUID(source["work_item_id"]),
    )


def _authority_counts() -> list[dict[str, Any]]:
    return _sql(
        "SELECT (SELECT count(*) FROM curie.approvals) AS approvals, "
        "(SELECT count(*) FROM curie.publications) AS publications, "
        "(SELECT count(*) FROM curie.execution_requests) AS requests"
    )


def _metadata(value: dict[str, Any], source: dict[str, Any], reservation: dict[str, Any],
              payloads: dict[str, bytes]) -> None:
    assert set(value) == CHECKPOINT_FIELDS
    assert value["checkpoint_id"] == reservation["checkpoint_id"]
    assert value["work_item_id"] == source["work_item_id"]
    assert value["source_request_id"] == source["source_request_id"]
    assert value["repository"] == REPOSITORY
    assert (value["base_commit"], value["target_branch"]) == (BASE, "main")
    assert value["source_sequence"] >= 1 and value["source_status"] == "failed"
    assert value["status"] == "complete" and value["available"] is True
    assert 0 <= value["age_seconds"] < 120
    completed = datetime.fromisoformat(value["completed_at"].replace("Z", "+00:00"))
    expires = datetime.fromisoformat(value["expires_at"].replace("Z", "+00:00"))
    assert expires - completed == timedelta(hours=24)
    assert value["total_bytes"] == sum(map(len, payloads.values()))
    assert len(value["objects"]) == 3
    for obj in value["objects"]:
        assert set(obj) == OBJECT_FIELDS
        expected = payloads[obj["kind"]]
        assert (obj["size_bytes"], obj["sha256"]) == (
            len(expected), hashlib.sha256(expected).hexdigest()
        )
    _private(json.dumps(value))


def test_recovery_admin_access(access: Access, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    source = access.source()
    reservation, payloads = access.capture(source)
    counts = _authority_counts()
    outcome = _sql(
        "SELECT workspace_recovery_outcome, workspace_recovery_outcome_at FROM curie.work_items "
        "WHERE id=:id", id=uuid.UUID(source["work_item_id"]),
    )
    listed = _ok(access.get(source))
    assert set(listed) == LIST_FIELDS
    assert listed["repository"] == REPOSITORY
    assert listed["work_item_id"] == source["work_item_id"]
    assert listed["capture_outcome"] == "complete" and listed["capture_outcome_at"]
    assert len(listed["checkpoints"]) == 1
    _metadata(listed["checkpoints"][0], source, reservation, payloads)
    _metadata(_ok(access.get(source, reservation)), source, reservation, payloads)
    for kind, data in payloads.items():
        response = access.get(source, reservation, kind, follow_redirects=False)
        assert response.status_code == 200, response.text
        assert response.content == data
        assert hashlib.sha256(response.content).hexdigest() == hashlib.sha256(data).hexdigest()
        _private(str(dict(response.headers)))
        assert not _readers(source)

    console = _cookie(_exchange(access.client, access.headers))
    for path in (_path(source), _path(source, reservation), _path(source, reservation, "patch")):
        response = access.client.get(path, headers=console)
        assert response.status_code == 200, response.text
        for headers in ({}, {"X-API-Key": "wrong"},
                        {"X-Curie-Worker-Token": get_settings().internal_worker_token}):
            _refused(access.client.get(path, headers=headers), 401)
        for scope in ("state", "sandbox", "publication.precheck", "work_item.issue_read"):
            token = mint(get_settings().api_key, agent=source["agent_id"], scope=scope,
                         exp=4102444800)
            for headers in ({"X-API-Key": token}, {"X-Curie-Publication-Precheck": token},
                            {"X-Curie-Issue-Read": token}):
                _refused(access.client.get(path, headers=headers), 401)
    _expire_sessions()
    _refused(access.client.get(_path(source), headers=console), 401)

    other = access.source()
    _refused(access.get(other, reservation, "patch"), 404)
    _refused(access.get({"work_item_id": str(uuid.uuid4())}), 404)
    _refused(access.get(source, {"checkpoint_id": str(uuid.uuid4())}), 404)
    for kind in ("key", "../base", "%2e%2e%2fbase", "patch%2F..%2Fbase", "https:%2f%2fevil"):
        response = access.get(source, reservation, kind)
        assert response.status_code in {404, 422}, response.text

    _sql("UPDATE curie.work_items SET base_branch='next', base_commit=:base WHERE id=:id",
         base="b" * 40, id=uuid.UUID(source["work_item_id"]))
    _metadata(_ok(access.get(source, reservation)), source, reservation, payloads)
    assert access.get(source, reservation, "base").content == payloads["base"]
    for bases, bound in ((["next"], True), (["main"], False)):
        access.policy(bases=bases, bound=bound)
        _refused(access.get(source), 403)
        _refused(access.get(source, reservation, "patch"), 403)
    access.policy(bases=[])
    assert access.get(source, reservation, "patch").content == payloads["patch"]
    access.forge_state["default_branch"] = "next"
    _refused(access.get(source), 403)
    access.forge_state["default_branch"] = "main"
    access.forge_state["id"] = 999
    _refused(access.get(source), 403)
    access.forge_state["id"] = 101
    access.forge_state["unavailable"] = True
    _refused(access.get(source), 503)
    access.forge_state["unavailable"] = False
    access.policy()

    for column, wrong, original in (
        ("repository_project_id", "999", "101"),
        ("repository_host", "wrong.example.com", REPOSITORY["host"]),
    ):
        _sql(f"UPDATE curie.workspace_recovery_groups SET {column}=:value WHERE work_item_id=:id",
             value=wrong, id=uuid.UUID(source["work_item_id"]))
        _refused(access.get(source), 403)
        _sql(f"UPDATE curie.workspace_recovery_groups SET {column}=:value WHERE work_item_id=:id",
             value=original, id=uuid.UUID(source["work_item_id"]))
    _sql("UPDATE curie.workspace_recovery_checkpoints SET source_request_id=:request WHERE id=:id",
         request=uuid.UUID(other["source_request_id"]), id=uuid.UUID(reservation["checkpoint_id"]))
    _refused(access.get(source, reservation, "patch"), 403)
    _sql("UPDATE curie.workspace_recovery_checkpoints SET source_request_id=:request WHERE id=:id",
         request=uuid.UUID(source["source_request_id"]), id=uuid.UUID(reservation["checkpoint_id"]))

    patch = next(obj for obj in reservation["objects"] if obj["kind"] == "patch")
    for key in (
        "workspace-recovery/../escape", f"workspace-recovery/checkpoints/{uuid.uuid4()}/patch"
    ):
        _sql("UPDATE curie.workspace_recovery_objects SET key=:key WHERE key=:original",
             key=key, original=patch["key"])
        _refused(access.get(source, reservation, "patch"))
        _sql("UPDATE curie.workspace_recovery_objects SET key=:original WHERE key=:key",
             key=key, original=patch["key"])
    for status in ("staging", "tombstoned"):
        _sql("UPDATE curie.workspace_recovery_objects SET status=:status WHERE key=:key",
             status=status, key=patch["key"])
        _refused(access.get(source, reservation, "patch"))
    _sql("UPDATE curie.workspace_recovery_objects SET status='complete' WHERE key=:key",
         key=patch["key"])
    access.s3.delete_object(Bucket=access.bucket, Key=f"private/{patch['key']}")
    _refused(access.get(source, reservation, "patch"), 502)
    for damaged in (CANARY, b"x" * len(payloads["patch"]), payloads["patch"] + b"excess"):
        access.s3.put_object(Bucket=access.bucket, Key=f"private/{patch['key']}", Body=damaged)
        corrupt = access.get(source, reservation, "patch")
        _refused(corrupt, 502)
        assert payloads["patch"] not in corrupt.content and not _readers(source)
    access.s3.put_object(
        Bucket=access.bucket, Key=f"private/{patch['key']}", Body=payloads["patch"]
    )
    _sql("UPDATE curie.workspace_recovery_checkpoints SET expires_at=clock_timestamp() "
         "- interval '1 second' WHERE id=:id", id=uuid.UUID(reservation["checkpoint_id"]))
    assert _ok(access.get(source, reservation))["available"] is False
    _refused(access.get(source, reservation, "patch"))
    _sql("UPDATE curie.workspace_recovery_checkpoints SET status='tombstoned' WHERE id=:id",
         id=uuid.UUID(reservation["checkpoint_id"]))
    _refused(access.get(source, reservation, "base"))
    staging, _ = access.capture(other, complete=False)
    _refused(access.get(other, staging, "patch"))
    empty = access.source()
    _sql("UPDATE curie.work_items SET workspace_recovery_outcome='capture_failed', "
         "workspace_recovery_outcome_at=clock_timestamp() WHERE id=:id",
         id=uuid.UUID(empty["work_item_id"]))
    empty_result = _ok(access.get(empty))
    assert set(empty_result) == LIST_FIELDS
    assert empty_result["checkpoints"] == []
    assert empty_result["capture_outcome"] == "capture_failed"
    assert empty_result["capture_outcome_at"]
    assert _authority_counts()[0]["approvals"] == counts[0]["approvals"]
    assert _authority_counts()[0]["publications"] == counts[0]["publications"]
    assert _sql(
        "SELECT workspace_recovery_outcome, workspace_recovery_outcome_at FROM curie.work_items "
        "WHERE id=:id", id=uuid.UUID(source["work_item_id"]),
    ) == outcome
    assert not _readers(source)
    _private(caplog.text)


@dataclass
class HeldResponse:
    client: TestClient
    path: str
    fail_send: bool = False
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    disconnect: threading.Event = field(default_factory=threading.Event)
    messages: list[dict[str, Any]] = field(default_factory=list)

    def run(self) -> None:
        async def request() -> None:
            received = False

            async def receive() -> dict[str, Any]:
                nonlocal received
                if not received:
                    received = True
                    return {"type": "http.request", "body": b"", "more_body": False}
                await asyncio.to_thread(self.disconnect.wait)
                return {"type": "http.disconnect"}

            async def send(message: dict[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    self.entered.set()
                    if self.fail_send:
                        raise OSError("synthetic client send failure")
                    await asyncio.to_thread(self.release.wait)
                self.messages.append(message)

            scope = {
                "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1", "method": "GET", "scheme": "https",
                "path": self.path, "raw_path": self.path.encode(), "query_string": b"",
                "root_path": "", "client": ("127.0.0.1", 5000),
                "server": ("testserver", 443),
                "headers": [(b"x-api-key", get_settings().api_key.encode())],
            }
            try:
                await self.client.app(scope, receive, send)
            finally:
                self.disconnect.set()

        assert self.client.portal is not None
        self.client.portal.call(request)

    @property
    def body(self) -> bytes:
        return b"".join(message.get("body", b"") for message in self.messages)


def _regular_fds() -> dict[tuple[int, int, int], int]:
    result = {}
    for name in os.listdir("/proc/self/fd"):
        try:
            fd = int(name)
            info = os.fstat(fd)
        except OSError:
            continue
        if stat.S_ISREG(info.st_mode):
            result[(fd, info.st_dev, info.st_ino)] = stat.S_IMODE(info.st_mode)
    return result


@contextmanager
def _short_readers(seconds: int = 3) -> Iterator[None]:
    assert 1 <= seconds <= 10
    _sql(
        "CREATE FUNCTION curie.access_test_short_reader() RETURNS trigger LANGUAGE plpgsql AS $$ "
        f"BEGIN NEW.expires_at := clock_timestamp() + interval '{seconds} seconds'; "
        "RETURN NEW; END $$"
    )
    try:
        _sql(
            "CREATE TRIGGER access_test_short_reader BEFORE INSERT ON "
            "curie.workspace_recovery_readers FOR EACH ROW "
            "EXECUTE FUNCTION curie.access_test_short_reader()"
        )
        yield
    finally:
        _sql("DROP TRIGGER IF EXISTS access_test_short_reader ON curie.workspace_recovery_readers")
        _sql("DROP FUNCTION curie.access_test_short_reader()")


def _raw_signed_get(proxy: StoreProxy, path: str) -> tuple[int, bytes]:
    endpoint = urlsplit(proxy.endpoint)
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=10)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def test_recovery_reader_lifetime(access: Access, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    source = access.source()
    reservation, payloads = access.capture(source)
    assert access.get(source).status_code == 200
    counts = _authority_counts()
    path = _path(source, reservation, "patch")
    initial_files = _regular_fds()
    held = [HeldResponse(access.client, path) for _ in range(8)]
    with ThreadPoolExecutor(max_workers=9) as pool:
        running = []
        try:
            for response in held:
                running.append(pool.submit(response.run))
                assert response.entered.wait(10), "authenticated response did not reach send"
            readers = _readers(source)
            assert len(readers) == 8
            assert len({row["holder_id"] for row in readers}) == 8
            assert all(285 < (r["expires_at"] - r["observed_at"]).total_seconds() <= 300
                       for r in readers)
            spools = _regular_fds().keys() - initial_files.keys()
            assert len(spools) >= 8, "each pending response must own its verified spool"
            assert all(_regular_fds()[fd] == 0o600 for fd in spools)
            _refused(access.get(source, reservation, "patch"), 429)
            retained = readers[0]
            repeated = _ok(_post(access.client, "acquire-reader", {
                "work_item_id": source["work_item_id"],
                "checkpoint_id": reservation["checkpoint_id"],
                "holder_id": retained["holder_id"],
            }))
            assert repeated["lease_id"] == str(retained["id"])
            assert datetime.fromisoformat(repeated["expires_at"].replace("Z", "+00:00")) == (
                retained["expires_at"]
            )
            for suffix in ("readers", "acquire-reader", "renew-reader"):
                response = access.client.post(f"{_path(source, reservation)}/{suffix}",
                                              json={}, headers=access.headers)
                assert response.status_code in {404, 405}
            signed_path, _ = access.proxy.requests[-1]
            signed = parse_qs(urlsplit(signed_path).query)
            assert 1 <= int(signed["X-Amz-Expires"][0]) <= 5
            status, body = _raw_signed_get(access.proxy, signed_path)
            assert status == 200 and body == payloads["patch"]
            held[0].release.set()
            running[0].result(timeout=10)
            assert held[0].body == payloads["patch"]
            assert len(_readers(source)) == 7
            assert access.get(source, reservation, "manifest").content == payloads["manifest"]
        finally:
            for response in held:
                response.release.set()
                response.disconnect.set()
            for future in running:
                future.result(timeout=10)
    assert not _readers(source)
    assert not (spools & _regular_fds().keys())

    for fail_send in (True, False):
        response = HeldResponse(access.client, path, fail_send=fail_send)
        before = _regular_fds()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(response.run)
            try:
                assert response.entered.wait(10)
                if fail_send:
                    with pytest.raises((OSError, ExceptionGroup)):
                        future.result(timeout=10)
                else:
                    assert len(_readers(source)) == 1
                    response.disconnect.set()
                    future.result(timeout=10)
                    assert response.body == b""
            finally:
                response.release.set()
                response.disconnect.set()
        assert not _readers(source)
        assert not (_regular_fds().keys() - before.keys())

    for mode in ("failure", "headers", "body"):
        before = _regular_fds()
        access.proxy.arm(mode)
        try:
            with _short_readers():
                response = access.get(source, reservation, "patch")
            assert access.proxy.seen.is_set()
            _refused(response, 502 if mode == "failure" else 409)
            assert not _readers(source)
            assert not (_regular_fds().keys() - before.keys())
        finally:
            access.proxy.release.set()
            access.proxy.mode = ""

    with _short_readers():
        response = HeldResponse(access.client, path)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(response.run)
            try:
                assert response.entered.wait(10)
                reader = _readers(source)[0]
                signed_path, _ = access.proxy.requests[-1]
                query = parse_qs(urlsplit(signed_path).query)
                signed_ttl = int(query["X-Amz-Expires"][0])
                assert 1 <= signed_ttl <= 2
                remaining = (reader["expires_at"] - reader["observed_at"]).total_seconds()
                assert signed_ttl <= remaining + 1
                status, body = _raw_signed_get(access.proxy, signed_path)
                assert status == 200 and body == payloads["patch"]
                with pytest.raises((TimeoutError, ExceptionGroup)):
                    future.result(timeout=8)
                assert not _readers(source)
                assert response.body == b""
            finally:
                response.release.set()
                response.disconnect.set()
        signed_at = datetime.strptime(query["X-Amz-Date"][0], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        expires = signed_at + timedelta(seconds=signed_ttl + 1)
        until_expired = (expires - datetime.now(UTC)).total_seconds()
        if until_expired > 0:
            threading.Event().wait(until_expired)
        status, _ = _raw_signed_get(access.proxy, signed_path)
        assert status == 403, "the actual store accepted an expired signed capability"
    assert access.get(source, reservation, "patch").content == payloads["patch"]
    assert not _readers(source)
    assert _authority_counts() == counts
    _private(caplog.text)


def _collect(access: Access) -> Any:
    async def collect() -> Any:
        transport = httpx.ASGITransport(app=access.client.app)
        async with httpx.AsyncClient(transport=transport) as client:
            ledger = WorkspaceRecoveryClient(
                api_base_url="http://testserver", worker_token=get_settings().internal_worker_token,
                client=client,
            )
            collector = RecoveryCollector(
                ledger=ledger, store=RecoveryObjectStore(client=access.s3, bucket=access.bucket),
                collector_id="access-test-collector",
            )
            return await collector.collect_once()

    assert access.client.portal is not None
    return access.client.portal.call(collect)


def _head(access: Access, key: str, *, present: bool) -> None:
    if present:
        metadata = access.s3.head_object(Bucket=access.bucket, Key=f"private/{key}")
        assert metadata["ContentLength"] >= 1
    else:
        with pytest.raises(ClientError) as caught:
            access.s3.head_object(Bucket=access.bucket, Key=f"private/{key}")
        assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_recovery_access_gc_race(access: Access) -> None:
    source = access.source()
    pinned, payloads = access.capture(source)
    second_source = access.source(agent_id=source["agent_id"], item_id=source["work_item_id"])
    second, _ = access.capture(second_source, reuse=True)
    assert access.get(source).status_code == 200
    counts = _authority_counts()
    base = next(obj for obj in pinned["objects"] if obj["kind"] == "base")
    access.proxy.arm("headers")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(access.get, source, pinned, "patch")
        try:
            assert access.proxy.seen.wait(10), "the route never reached the signed store GET"
            readers = _readers(source)
            assert len(readers) == 1
            assert str(readers[0]["checkpoint_id"]) == pinned["checkpoint_id"]
            _sql(
                "UPDATE curie.workspace_recovery_checkpoints SET expires_at=clock_timestamp() "
                "- interval '1 second' WHERE work_item_id=:id",
                id=uuid.UUID(source["work_item_id"]),
            )
            collected = _collect(access)
            assert collected.claimed == collected.deleted == collected.finalized == 2
            assert collected.failed == collected.conflicts == 0
            pinned_row = _sql(
                "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id=:id",
                id=uuid.UUID(pinned["checkpoint_id"]),
            )
            assert pinned_row == [{"status": "tombstoned"}], "collector did not visit the group"
            assert not _sql("SELECT id FROM curie.workspace_recovery_checkpoints WHERE id=:id",
                            id=uuid.UUID(second["checkpoint_id"]))
            assert not _sql(
                "SELECT id FROM curie.workspace_recovery_objects WHERE work_item_id=:item "
                "AND (checkpoint_id=:checkpoint OR kind='base') AND status='claimed'",
                item=uuid.UUID(source["work_item_id"]),
                checkpoint=uuid.UUID(pinned["checkpoint_id"]),
            )
            for obj in pinned["objects"]:
                _head(access, obj["key"], present=True)
            for obj in second["objects"]:
                _head(access, obj["key"], present=False)
            assert not future.done(), "bytes escaped the external store barrier"
            access.proxy.release.set()
            response = future.result(timeout=10)
            assert response.status_code == 200, response.text
            assert response.content == payloads["patch"]
        finally:
            access.proxy.release.set()
            access.proxy.mode = ""
    assert not _readers(source)
    after = _collect(access)
    assert after.claimed == after.deleted == after.finalized == 2
    for obj in pinned["objects"]:
        _head(access, obj["key"], present=obj["kind"] == "base")
    final = _collect(access)
    assert final.claimed == final.deleted == final.finalized == 1
    _head(access, base["key"], present=False)

    other = access.source()
    tombstoned, _ = access.capture(other)
    _sql("UPDATE curie.workspace_recovery_checkpoints SET expires_at=clock_timestamp() "
         "- interval '1 second' WHERE id=:id", id=uuid.UUID(tombstoned["checkpoint_id"]))
    claims = _ok(_post(access.client, "claim-collection", {
        "collector_id": "access-test-collector", "limit": 100,
    }))["claims"]
    assert len(claims) == 2
    assert _sql("SELECT status FROM curie.workspace_recovery_checkpoints WHERE id=:id",
                id=uuid.UUID(tombstoned["checkpoint_id"])) == [{"status": "tombstoned"}]
    before_gets = len(access.proxy.requests)
    _refused(access.get(other, tombstoned, "patch"))
    assert len(access.proxy.requests) == before_gets and not _readers(other)
    removed = _collect(access)
    assert removed.deleted == removed.finalized == 2
    assert _collect(access).finalized == 1
    for obj in tombstoned["objects"]:
        _head(access, obj["key"], present=False)
    assert _authority_counts()[0]["approvals"] == counts[0]["approvals"]
    assert _authority_counts()[0]["publications"] == counts[0]["publications"]
