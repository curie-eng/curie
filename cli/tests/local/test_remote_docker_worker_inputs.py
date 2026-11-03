"""Exercise worker input permissions through the production Docker substrate."""

from __future__ import annotations

import errno
import hashlib
import io
import json
import subprocess
import tarfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from curie_worker.attachments import AttachmentRef, encode_attachment_refs
from curie_worker.sandbox.docker import DockerSandboxClient
from curie_worker.workspace import WorkspaceRef

OWNER_LABEL = "curietech.ai/e2e-owner"
BASE_IMAGE = (
    "python:3.13.15-slim-bookworm@sha256:"
    "ed86c82274b3c69b52fb5820f358f0bd7df0b603332063cb5c6e32bd220c3e6e"
)
MANIFEST = b'{"name":"acme-input-probe","version":"1.0.0"}\n'
README = b"private workspace seed\n"
NOTE = b"private directory seed\n"
ATTACHMENT = b"validated attachment seed\n"
BUNDLE_KEY = "bundles/acme-input-probe.tar.gz"


def _docker(*args: str, data: bytes | None = None, timeout: float = 45) -> str:
    result = subprocess.run(
        ["docker", *args],
        input=data,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"Docker {' '.join(args)} failed:\n"
            f"{result.stdout.decode(errors='replace')}\n"
            f"{result.stderr.decode(errors='replace')}"
        )
    return result.stdout.decode()


def _add_file(archive: tarfile.TarFile, name: str, content: bytes, mode: int) -> None:
    member = tarfile.TarInfo(name)
    member.mode = mode
    member.size = len(content)
    archive.addfile(member, io.BytesIO(content))


def _bundle_archive() -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        _add_file(archive, "acme-input-probe/.claude-plugin/plugin.json", MANIFEST, 0o644)
        _add_file(
            archive,
            "acme-input-probe/skills/probe/SKILL.md",
            b"---\nname: probe\ndescription: Read the supplied inputs.\n---\nRead the inputs.\n",
            0o644,
        )
    return output.getvalue()


def _workspace_archive() -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        _add_file(archive, "README.md", README, 0o600)
        directory = tarfile.TarInfo("private")
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o700
        archive.addfile(directory)
        _add_file(archive, "private/note.txt", NOTE, 0o600)
        link = tarfile.TarInfo("README-hardlink.md")
        link.type = tarfile.LNKTYPE
        link.linkname = "README.md"
        link.mode = 0o600
        archive.addfile(link)
    return output.getvalue()


@dataclass
class MemoryBundleReader:
    data: bytes
    requested: list[str] = field(default_factory=list)

    def get(self, key: str) -> bytes:
        if key != BUNDLE_KEY:
            raise KeyError(key)
        self.requested.append(key)
        return self.data


class InputObjects(ThreadingHTTPServer):
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.requested: list[str] = []
        super().__init__(("127.0.0.1", 0), InputObjectHandler)


class InputObjectHandler(BaseHTTPRequestHandler):
    server: InputObjects

    def do_GET(self) -> None:
        content = self.server.objects.get(self.path)
        if content is None:
            self.send_error(404)
            return
        self.server.requested.append(self.path)
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class WorkerInputs:
    def __init__(self) -> None:
        self.owner = f"curie-check-worker-inputs-{uuid.uuid4().hex}"
        self.claim = f"{self.owner}-runner"
        self.network = f"{self.owner}-network"
        self.image = f"{self.owner}:test"
        self.docker_ready = False
        self.server: InputObjects | None = None
        self.server_thread: threading.Thread | None = None
        self.bundles = MemoryBundleReader(_bundle_archive())
        self.env: dict[str, str] = {}
        self.client = DockerSandboxClient(
            image=self.image,
            bundle_store=self.bundles,
            network=self.network,
            environ={},
        )

    def start(self) -> None:
        _docker("info", "--format", "{{.ServerVersion}}")
        self.docker_ready = True
        dockerfile = (
            f"FROM {BASE_IMAGE}\n"
            "USER 1000:1000\n"
            'ENTRYPOINT ["python", "-c", "import time; time.sleep(300)"]\n'
            "HEALTHCHECK NONE\n"
        ).encode()
        context = io.BytesIO()
        with tarfile.open(fileobj=context, mode="w") as archive:
            _add_file(archive, "Dockerfile", dockerfile, 0o644)
        _docker(
            "build",
            "--tag",
            self.image,
            "--label",
            f"{OWNER_LABEL}={self.owner}",
            "-",
            data=context.getvalue(),
            timeout=300,
        )
        _docker("network", "create", "--label", f"{OWNER_LABEL}={self.owner}", self.network)
        workspace = _workspace_archive()
        self.server = InputObjects({"/workspace.tar.gz": workspace, "/report.txt": ATTACHMENT})
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        origin = f"http://127.0.0.1:{self.server.server_port}"
        expires = int(time.time()) + 600
        digest = hashlib.sha256(workspace).hexdigest()
        self.env = {
            "CURIE_FAKE_MODEL": "1",
            "CURIE_BUNDLE_REF": BUNDLE_KEY,
            "CURIE_WORKSPACE_REF": WorkspaceRef(
                url=f"{origin}/workspace.tar.gz",
                sha256=digest,
                expires_at_epoch=expires,
            ).encode(),
            "CURIE_WORKSPACE_SHA256": digest,
            "CURIE_ATTACHMENTS_REF": encode_attachment_refs(
                [
                    AttachmentRef(
                        name="report.txt",
                        url=f"{origin}/report.txt",
                        sha256=hashlib.sha256(ATTACHMENT).hexdigest(),
                        size_bytes=len(ATTACHMENT),
                        expires_at_epoch=expires,
                    )
                ]
            ),
        }

    def owned(self, kind: str) -> list[str]:
        if kind == "container":
            listing = ("ps", "-aq", "--no-trunc")
        else:
            listing = (kind, "ls", "-q")
        filters = (
            (
                "curietech.ai/managed-by=curie-sandbox-substrate",
                f"curietech.ai/stack={self.network}",
                f"curietech.ai/claim={self.claim}",
            )
            if kind in ("container", "volume")
            else (f"{OWNER_LABEL}={self.owner}",)
        )
        args = [*listing]
        for label in filters:
            args.extend(("--filter", f"label={label}"))
        return _docker(*args).split()

    def close(self) -> None:
        errors: list[str] = []
        if self.server is not None:
            try:
                if self.server_thread is not None:
                    self.server.shutdown()
                    self.server_thread.join(timeout=5)
                    if self.server_thread.is_alive():
                        errors.append("Input HTTP server thread survived shutdown")
            except (OSError, RuntimeError) as exc:
                errors.append(f"Stopping input HTTP server failed: {exc}")
            finally:
                try:
                    self.server.server_close()
                except (OSError, RuntimeError) as exc:
                    errors.append(f"Closing input HTTP server failed: {exc}")
        if not self.docker_ready:
            return
        for kind in ("container", "volume", "network", "image"):
            try:
                resources = self.owned(kind)
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                errors.append(f"Listing owned {kind} resources failed: {exc}")
                continue
            for resource in resources:
                try:
                    args = ("rm", "-f") if kind == "container" else (kind, "rm")
                    _docker(*args, resource)
                except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                    errors.append(f"Removing owned {kind} {resource} failed: {exc}")
        for kind in ("container", "volume", "network", "image"):
            try:
                remaining = self.owned(kind)
                if remaining:
                    errors.append(f"Owned {kind} resources remain after cleanup: {remaining}")
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                errors.append(f"Verifying {kind} cleanup failed: {exc}")
        if errors:
            raise RuntimeError("\n".join(errors))


@pytest.fixture
def worker_inputs(request: pytest.FixtureRequest) -> WorkerInputs:
    inputs = WorkerInputs()
    request.addfinalizer(inputs.close)
    inputs.start()
    return inputs


def test_create_claim_seeds_private_worker_inputs_without_host_binds(
    worker_inputs: WorkerInputs,
) -> None:
    inputs = worker_inputs
    inputs.client.create_claim(
        inputs.claim,
        pool="acme-input-probe",
        env=inputs.env,
        labels={OWNER_LABEL: inputs.owner},
        deadline=time.monotonic() + 300,
    )
    inspected = json.loads(_docker("inspect", inputs.claim))[0]
    assert inspected["State"]["Running"] is True
    assert inspected["Config"]["User"] == "1000:1000"
    assert all(mount["Type"] in ("volume", "tmpfs") for mount in inspected["Mounts"])
    mounts = {
        mount["Destination"]: mount for mount in inspected["Mounts"] if mount["Type"] == "volume"
    }
    assert set(mounts) == {"/bundles/current", "/workspace", "/attachments"}
    assert {path: mount["RW"] for path, mount in mounts.items()} == {
        "/bundles/current": False,
        "/workspace": True,
        "/attachments": False,
    }
    assert len({mount["Name"] for mount in mounts.values()}) == 3
    assert set(inspected["NetworkSettings"]["Networks"]) == {inputs.network}
    assert inputs.bundles.requested == [BUNDLE_KEY]
    assert inputs.server is not None
    assert inputs.server.requested == ["/workspace.tar.gz", "/report.txt"]
    assert inputs.owned("container") == [inspected["Id"]]
    assert set(inputs.owned("volume")) == {mount["Name"] for mount in mounts.values()}

    probe = _docker(
        "exec",
        "--user",
        "1000:1000",
        inputs.claim,
        "python",
        "-c",
        """import json, os, stat
from pathlib import Path
workspace = Path('/workspace')
paths = ['.', 'README.md', 'README-hardlink.md', 'private', 'private/note.txt']
metadata = {}
for name in paths:
    info = (workspace / name).stat()
    metadata[name] = [info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)]
files = {
    'readme': workspace / 'README.md',
    'hardlink': workspace / 'README-hardlink.md',
    'note': workspace / 'private/note.txt',
    'manifest': Path('/bundles/current/.claude-plugin/plugin.json'),
    'skill': Path('/bundles/current/skills/probe/SKILL.md'),
    'attachment': Path('/attachments/report.txt'),
}
contents = {name: path.read_text() for name, path in files.items()}
same_inode = files['readme'].stat().st_ino == files['hardlink'].stat().st_ino
writes = {}
for name in ('readme', 'hardlink', 'note'):
    try:
        with files[name].open('a') as stream:
            stream.write('runner edit\\n')
        writes[name] = None
    except OSError as error:
        writes[name] = error.errno
for name, directory in (('root', workspace), ('private', workspace / 'private')):
    try:
        (directory / 'runner-created.txt').write_text('runner created\\n')
        writes[name] = None
    except OSError as error:
        writes[name] = error.errno
readonly = {}
for name in ('manifest', 'attachment'):
    try:
        fd = os.open(files[name], os.O_WRONLY | os.O_APPEND)
    except OSError as error:
        readonly[name] = error.errno
    else:
        os.close(fd)
        readonly[name] = None
print(json.dumps({
    'uid': os.getuid(), 'gid': os.getgid(), 'metadata': metadata,
    'contents': contents, 'same_inode': same_inode, 'writes': writes,
    'readonly': readonly, 'edited_readme': files['readme'].read_text(),
    'edited_hardlink': files['hardlink'].read_text(),
    'created': [(workspace / name / 'runner-created.txt').read_text()
                for name in ('.', 'private')],
}))
""",
    )
    observed = json.loads(probe)
    assert (observed["uid"], observed["gid"]) == (1000, 1000)
    metadata = observed["metadata"]
    assert set(metadata) == {
        ".",
        "README.md",
        "README-hardlink.md",
        "private",
        "private/note.txt",
    }
    assert all(entry[:2] == [1000, 1000] for entry in metadata.values())
    for name in ("README.md", "README-hardlink.md", "private/note.txt"):
        assert metadata[name][2] == 0o600
    # The data filter clears directory modes before extraction:
    # https://docs.python.org/3.13/library/tarfile.html#tarfile.data_filter
    for name in (".", "private"):
        assert metadata[name][2] & 0o700 == 0o700
    assert observed["contents"] == {
        "readme": README.decode(),
        "hardlink": README.decode(),
        "note": NOTE.decode(),
        "manifest": MANIFEST.decode(),
        "skill": (
            "---\nname: probe\ndescription: Read the supplied inputs.\n---\nRead the inputs.\n"
        ),
        "attachment": ATTACHMENT.decode(),
    }
    assert observed["same_inode"] is True
    assert observed["writes"] == dict.fromkeys(("readme", "hardlink", "note", "root", "private"))
    assert observed["readonly"] == {"manifest": errno.EROFS, "attachment": errno.EROFS}
    edited = README.decode() + "runner edit\nrunner edit\n"
    assert observed["edited_readme"] == observed["edited_hardlink"] == edited
    assert observed["created"] == ["runner created\n", "runner created\n"]
