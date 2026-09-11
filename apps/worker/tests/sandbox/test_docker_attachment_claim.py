"""Inbound attachments reach the DOCKER sandbox too (#2567, follow-up).

#2567 landed the lane on Kubernetes only. ``sandbox/k8s.py`` names
``CURIE_ATTACHMENTS_REF`` at an ``attachments-init`` container; ``sandbox/docker.py``
does not contain the word "attachment" at all. But ``curie local up`` and
``curie skill`` select the DOCKER driver, so on the substrate developers and
skill authors actually run, a person uploads a file, the worker downloads it,
parks the bytes and mints a signed one-object reference -- and then nothing
redeems it. The agent is handed no file and told nothing, which is the
fabricated empty result ADR-0041 forbids ("a verb either does the work, or
explicitly reports that the concept does not exist at this tier"), and it
recreates on the local tier the exact silent-failure bug #2567 was filed to fix.

**What these tests specify.** The docker driver has to do for attachments what
it already does for the workspace in ``_prepare_workspace``: decode the signed
reference, refuse an expired one, refuse a digest that disagrees with the claim,
stream the download under a cap, verify sha256, materialize into a temp dir, and
bind-mount it -- read-only, like ``_prepare_bundle`` does -- at the path the
runner probes. The refusal set is the one the chart's ``attachments-init``
already enforces (agent-sandbox.yaml): expired, non-HTTP(S), bad digest,
unusable name, over the cap. A refusal must leave nothing half-materialized.

**What is mocked, and why only that.** The presigned object store's HTTP GET,
and nothing else. It is an external service; the decode, the digest check, the
cap, the materialization, the chmod and the argv are all exercised for real.
The download is intercepted at ``urllib.request.urlopen``, which is the call
``_prepare_workspace`` makes -- a driver that fetches some other way will fail
here saying it never fetched at all, which is the correct thing for it to say.

**The mount path is a cross-substrate seam.** ``curie_runner.__main__.ATTACHMENTS_DIR``
is compiled into the runner (the reference env is deliberately scoped away from
the runner container, so nothing at runtime can tell it where the volume
landed), and ``charts/curie/values.yaml`` templates the same path for the pod.
``runner/tests/test_attachments_mount.py`` pins those two to each other; this
file adds the third side, so the docker driver cannot mount somewhere the runner
never looks.

**The refusal type** is ``AttachmentResolutionError`` -- the lane's own error,
which ``decode_attachment_refs`` already raises for a malformed or non-HTTP(S)
reference. A subclass is fine; a bare ``RuntimeError`` is not, because the
kernel and the eval consumer distinguish lanes by exception type.
"""

from __future__ import annotations

import ast
import hashlib
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from curie_worker.attachments import (
    ATTACHMENTS_REF_ENV,
    AttachmentLimits,
    AttachmentRef,
    AttachmentResolutionError,
    encode_attachment_refs,
)
from curie_worker.sandbox.docker import DockerError

from .conftest import _FakeBundleStore, _flag_values, _RecordingDocker

# Fake model everywhere: it suppresses the ambient-credential forwarding, so the
# argv a test compares does not depend on whether the developer running it has
# ANTHROPIC_API_KEY exported.
_BASE_ENV = {
    "CURIE_BUDGET": "{}",
    "CURIE_SESSION_ID": "sess-2567",
    "CURIE_FAKE_MODEL": "1",
}


# --- the two other sides of the mount seam ----------------------------------


def _repo_root() -> Path:
    """Resolved from this file, not the cwd, so it holds from anywhere."""

    return Path(__file__).resolve().parents[4]


def _runner_attachments_dir() -> str:
    """``curie_runner.__main__.ATTACHMENTS_DIR``, read from source not imported.

    Read as text on purpose. Importing ``curie_runner.__main__`` pulls in the
    agent SDK and an aiohttp app at import time, and the worker package has no
    business importing the runner. The constant is a compiled-in literal, so the
    source is the authority either way.
    """

    source = (_repo_root() / "runner" / "src" / "curie_runner" / "__main__.py").read_text()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "ATTACHMENTS_DIR" for t in node.targets):
            continue
        value = node.value
        if isinstance(value, ast.Call) and value.args and isinstance(value.args[0], ast.Constant):
            return str(value.args[0].value)
    pytest.fail("curie_runner.__main__ declares no ATTACHMENTS_DIR = Path(...) literal")


def _chart_attachments_mount_path() -> str:
    values = yaml.safe_load((_repo_root() / "charts" / "curie" / "values.yaml").read_text())
    attachments = values["agentSandbox"]["runner"].get("attachments")
    assert attachments is not None, (
        "charts/curie/values.yaml declares no agentSandbox.runner.attachments"
    )
    return str(attachments["mountPath"])


# --- the one mocked collaborator: the presigned object store -----------------


class _FakeBody:
    """An HTTP body that behaves like a socket: ``read(n)`` may return less.

    That is what makes streaming observable. A driver that loops on ``read(n)``
    pulls at most ``chunk_size`` per call, so a cap can be enforced against what
    was actually pulled; a driver that calls ``read()`` with no argument gets the
    whole body in one go -- exactly as a real ``HTTPResponse`` would -- and the
    boundedness test then reports the whole body as delivered.
    """

    def __init__(self, store: _FakeObjectStore, url: str, payload: bytes, chunk_size: int) -> None:
        self._store = store
        self._url = url
        self._payload = payload
        self._chunk_size = chunk_size
        self._at = 0
        self.status = 200
        self.headers: dict[str, str] = {"Content-Type": "application/octet-stream"}

    def read(self, amount: int | None = None) -> bytes:
        remaining = len(self._payload) - self._at
        if remaining <= 0:
            return b""
        take = remaining if amount is None else min(amount, self._chunk_size, remaining)
        data = self._payload[self._at : self._at + take]
        self._at += take
        self._store.delivered[self._url] = self._store.delivered.get(self._url, 0) + len(data)
        return data

    def close(self) -> None:
        return None

    def __enter__(self) -> _FakeBody:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class _FakeObjectStore:
    """The presigned GET the sandbox tier redeems, and nothing else.

    Records every URL requested and every byte handed over, so "the driver
    refused before fetching" and "the driver stopped pulling at the cap" are
    both assertable facts rather than inferences.
    """

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, int]] = {}
        self.requested: list[str] = []
        self.delivered: dict[str, int] = {}

    def add(self, url: str, payload: bytes, *, chunk_size: int = 16) -> str:
        self.objects[url] = (payload, chunk_size)
        return url

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(urllib.request, "urlopen", self._urlopen)

    def _urlopen(self, request: Any, *_args: Any, **_kwargs: Any) -> _FakeBody:
        url = str(getattr(request, "full_url", request))
        self.requested.append(url)
        if url not in self.objects:
            raise urllib.error.HTTPError(url, 404, "no such object", {}, None)  # type: ignore[arg-type]
        payload, chunk_size = self.objects[url]
        return _FakeBody(self, url, payload, chunk_size)


@pytest.fixture
def objects(monkeypatch: pytest.MonkeyPatch) -> _FakeObjectStore:
    store = _FakeObjectStore()
    store.install(monkeypatch)
    return store


@pytest.fixture
def staged(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[Path]]:
    """Every temp dir the driver really created, so "left nothing" is provable.

    A refusal that returns the right exception while leaving a half-written
    directory on the host is still a partial materialization: the next thing to
    read that path sees an attachment set that was never verified.
    """

    created: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def _record(*args: Any, **kwargs: Any) -> str:
        path = real_mkdtemp(*args, **kwargs)
        created.append(Path(path))
        return path

    monkeypatch.setattr(tempfile, "mkdtemp", _record)
    yield created
    for path in created:
        shutil.rmtree(path, ignore_errors=True)


# --- building claims ---------------------------------------------------------


def _limits(**overrides: Any) -> AttachmentLimits:
    """Small, test-legible bounds, every field named rather than defaulted."""

    values: dict[str, Any] = {
        "max_file_bytes": 64,
        "read_chunk_bytes": 16,
        "reference_ttl_seconds": 300,
        "retention_ttl_seconds": 3600,
        "max_files": 10,
    }
    values.update(overrides)
    return AttachmentLimits(**values)


def _client(
    *,
    limits: AttachmentLimits | None = None,
    docker_class: type[_RecordingDocker] = _RecordingDocker,
) -> _RecordingDocker:
    """A recording docker client, optionally with an operator-configured cap.

    ``attachment_limits=`` mirrors the ``workspace_limits=`` the constructor
    already takes, and ``run.py`` already computes the value
    (``_attachment_limits(config)``) for the coordinator -- so the same envelope
    reaches the claim path on this tier instead of a hardcoded number.
    """

    extra: dict[str, Any] = {} if limits is None else {"attachment_limits": limits}
    try:
        return docker_class(image="curie-runner", bundle_store=_FakeBundleStore(), **extra)
    except TypeError as exc:  # pragma: no cover - the red state
        pytest.fail(
            f"DockerSandboxClient does not accept attachment_limits= yet: {exc}. "
            "The operator-configured envelope run.py already builds for the "
            "coordinator has to reach the claim path, exactly as workspace_limits= does."
        )


def _ref(
    *,
    url: str,
    name: str = "report.csv",
    payload: bytes = b"",
    sha256: str | None = None,
    expires_in: int = 300,
) -> AttachmentRef:
    return AttachmentRef(
        name=name,
        url=url,
        sha256=sha256 if sha256 is not None else hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        expires_at_epoch=int(time.time()) + expires_in,
        mime_type="text/csv",
    )


def _env(*refs: AttachmentRef) -> dict[str, str]:
    return {**_BASE_ENV, ATTACHMENTS_REF_ENV: encode_attachment_refs(refs)}


def _attachment_mount(argv: list[str]) -> tuple[Path, str]:
    """(host dir, mode) of the bind mount at the path the runner probes."""

    expected = _runner_attachments_dir()
    mounts = _flag_values(argv, "-v")
    for mount in mounts:
        parts = mount.split(":")
        if len(parts) >= 2 and parts[1] == expected:
            return Path(parts[0]), parts[2] if len(parts) > 2 else ""
    pytest.fail(
        f"the docker driver bind-mounted nothing at {expected!r} (mounts: {mounts}). "
        "The worker downloaded the file, parked the bytes and minted a signed "
        "reference, and no container will ever see it."
    )


def _refused(client: _RecordingDocker, env: dict[str, str], *, because: str) -> Exception:
    """Drive a claim that must be refused, and hand back the refusal."""

    try:
        client.create_claim("claim-attachments", pool="pool", env=env)
    except AttachmentResolutionError as exc:
        return exc
    pytest.fail(
        f"the docker driver accepted {because}. The chart's attachments-init "
        "refuses it, and this tier must fail closed the same way rather than "
        "boot a sandbox around unverified bytes."
    )


def _left_nothing(staged_dirs: list[Path]) -> None:
    survivors = [path for path in staged_dirs if path.exists()]
    assert survivors == [], (
        f"a refused resolve left {survivors} on the host; a half-materialized "
        "attachment dir is indistinguishable from a verified one to whatever reads it next"
    )


# --- the common case: a turn with no attachments -----------------------------


@pytest.mark.parametrize(
    "extra",
    [{}, {ATTACHMENTS_REF_ENV: ""}],
    ids=["no-key-at-all", "present-but-empty"],
)
def test_a_turn_with_no_attachments_produces_todays_container_spec_exactly(
    extra: dict[str, str], objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The overwhelming majority of turns. Nothing about them may change.

    No mount, no temp dir, no HTTP, and an argv byte-identical to the one the
    same claim produces with the concept absent -- including for an empty-valued
    key, which must be dropped rather than forwarded as ``-e KEY=``: the
    reference is a capability, and the runner container is the one process that
    must never hold it (the k8s side scopes it to the init container for the
    same reason).
    """

    baseline = _client()
    baseline.create_claim("claim-plain", pool="pool", env=dict(_BASE_ENV))

    client = _client()
    client.create_claim("claim-plain", pool="pool", env={**_BASE_ENV, **extra})

    assert client.calls[0] == baseline.calls[0]
    assert _flag_values(client.calls[0], "-v") == []
    assert staged == []
    assert objects.requested == []


# --- the working path --------------------------------------------------------


def test_attachments_are_materialized_and_bind_mounted_read_only(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The bug this file exists for: the files must actually arrive.

    Read-only for the same reason ``_prepare_bundle``'s mount is: these are
    third-party bytes handed to prompt-injectable model code, and the workspace
    -- not the attachment mount -- is the writable surface an agent is given.
    """

    report = b"a,b\n1,2\n"
    notes = b"hello"
    report_url = objects.add("https://objects.example.test/attachments/a/000.bin", report)
    notes_url = objects.add("https://objects.example.test/attachments/a/001.bin", notes)

    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(
            _ref(url=report_url, name="report.csv", payload=report),
            _ref(url=notes_url, name="notes.txt", payload=notes),
        ),
    )

    root, mode = _attachment_mount(client.calls[0])
    assert mode == "ro", f"the attachment mount is {mode!r}, not read-only"
    # Dot-entries are the init container's own bookkeeping and the runner
    # already ignores them (runner/tests/test_attachments_mount.py), so only the
    # visible entries are the person's files.
    visible = sorted(entry.name for entry in root.iterdir() if not entry.name.startswith("."))
    assert visible == ["notes.txt", "report.csv"]
    assert (root / "report.csv").read_bytes() == report
    assert (root / "notes.txt").read_bytes() == notes
    assert objects.requested == [report_url, notes_url]


def test_two_attachments_sharing_a_filename_both_survive(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """Two ``report.pdf`` in one message. Both must be readable.

    Before this, both were written to ``<mount>/report.pdf`` and the second
    destroyed the first: the agent saw one file where two arrived, with nothing
    saying one had been lost. That is exactly the silent loss #2567 exists to
    close, reintroduced one layer down, and it defeats the lane's own
    all-or-nothing argument -- a partial set reads to an agent like a complete
    one.

    The disambiguation is asserted by NAME, not merely by count: the scheme is a
    cross-substrate contract (the rendered ``attachments-init`` program applies
    the same one, and ci/attachment-init-behavior-assertions.sh executes it on
    this case), so a driver that invented its own suffix would still mount two
    files and still be wrong.
    """

    first, second, third = b"first report", b"second report", b"third report"
    urls = [
        objects.add("https://objects.example.test/attachments/a/000.bin", first),
        objects.add("https://objects.example.test/attachments/a/001.bin", second),
        objects.add("https://objects.example.test/attachments/a/002.bin", third),
    ]

    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(
            _ref(url=urls[0], name="report.pdf", payload=first),
            _ref(url=urls[1], name="report.pdf", payload=second),
            _ref(url=urls[2], name="report.pdf", payload=third),
        ),
    )

    root, _mode = _attachment_mount(client.calls[0])
    visible = sorted(entry.name for entry in root.iterdir() if not entry.name.startswith("."))
    assert visible == ["report-2.pdf", "report-3.pdf", "report.pdf"], (
        f"three same-named uploads materialized as {visible}; one of them was "
        "silently destroyed or renamed by some other scheme than the chart's"
    )
    # The bytes, not just the names: a scheme that produced three paths but
    # pointed two of them at the same object would pass a name-only check.
    assert (root / "report.pdf").read_bytes() == first
    assert (root / "report-2.pdf").read_bytes() == second
    assert (root / "report-3.pdf").read_bytes() == third
    assert objects.requested == urls


def test_a_collision_with_an_already_disambiguated_name_does_not_overwrite(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """A person really did upload a file called ``report-2.pdf``.

    The counter has to skip a name that is already taken, or the disambiguation
    itself becomes the overwrite it was added to prevent.
    """

    first, literal, third = b"one", b"literally report-2", b"three"
    urls = [
        objects.add("https://objects.example.test/attachments/a/000.bin", first),
        objects.add("https://objects.example.test/attachments/a/001.bin", literal),
        objects.add("https://objects.example.test/attachments/a/002.bin", third),
    ]

    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(
            _ref(url=urls[0], name="report.pdf", payload=first),
            _ref(url=urls[1], name="report-2.pdf", payload=literal),
            _ref(url=urls[2], name="report.pdf", payload=third),
        ),
    )

    root, _mode = _attachment_mount(client.calls[0])
    assert (root / "report.pdf").read_bytes() == first
    assert (root / "report-2.pdf").read_bytes() == literal
    assert (root / "report-3.pdf").read_bytes() == third


def test_the_mount_path_agrees_with_the_runner_and_the_chart(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """One path in three languages; drift is silent and total.

    The runner compiles the path in (nothing at runtime tells it where the
    volume landed), the chart templates it for the pod, and this driver picks it
    for the bind mount. Move one alone and the sandbox mounts a directory
    nothing reads -- the only symptom is an agent that cannot see a file sitting
    right there in the container.
    """

    runner_dir = _runner_attachments_dir()
    chart_dir = _chart_attachments_mount_path()
    assert chart_dir == runner_dir, (
        f"charts/curie/values.yaml mounts {chart_dir!r} but curie_runner reads {runner_dir!r}"
    )

    payload = b"x"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(_ref(url=url, name="report.csv", payload=payload)),
    )

    mounts = _flag_values(client.calls[0], "-v")
    assert len(mounts) == 1, f"expected exactly the attachment mount, got {mounts}"
    container_path = mounts[0].split(":")[1]
    assert container_path == runner_dir == chart_dir, (
        f"the docker driver mounts attachments at {container_path!r} while the "
        f"runner probes {runner_dir!r} and the chart mounts {chart_dir!r}"
    )


def test_the_signed_reference_never_reaches_the_runner_container(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The capability stays with the worker, as it does on Kubernetes.

    ``CURIE_ATTACHMENTS_REF`` carries presigned URLs. On k8s it is named at the
    init container so the runner never sees it; here the worker itself redeems
    it, so it must simply not be forwarded. A plain env var in a container
    running model code is one the agent can read out of /proc/1/environ and
    echo into a channel.
    """

    payload = b"x"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    env = _env(_ref(url=url, name="report.csv", payload=payload))
    client = _client()
    client.create_claim("claim-attachments", pool="pool", env=env)

    argv = client.calls[0]
    forwarded = [e for e in _flag_values(argv, "-e") if e.startswith(f"{ATTACHMENTS_REF_ENV}=")]
    assert forwarded == [], (
        f"{ATTACHMENTS_REF_ENV} was forwarded into the runner container: {forwarded}"
    )
    assert all(env[ATTACHMENTS_REF_ENV] not in arg for arg in argv), (
        "the encoded capability set is in the container argv under another name"
    )
    assert all(url not in arg for arg in argv), "the presigned URL landed in the container argv"


def test_the_materialized_tree_is_readable_by_the_nonroot_runner(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """mkdtemp is 0700 and the runner is uid 1000; the mount must be widened.

    Exactly the property ``test_prepare_bundle_is_readable_by_nonroot_runner``
    pins for the bundle. A tree the runner cannot traverse is an empty
    attachment dir as far as the agent is concerned -- the same silent nothing,
    one layer down.
    """

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(_ref(url=url, name="report.csv", payload=payload)),
    )

    root, _mode = _attachment_mount(client.calls[0])
    assert root.stat().st_mode & 0o005 == 0o005, "the mount root is not o+rx"
    assert (root / "report.csv").stat().st_mode & 0o004 == 0o004, "report.csv is not o+r"


# --- the four refusals, each leaving nothing behind --------------------------


def test_an_expired_reference_is_refused_and_materializes_nothing(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """Refused before the fetch, as ``_prepare_workspace`` refuses one.

    The TTL is the whole point of a short-lived capability: redeeming a lapsed
    one either fetches bytes the retention sweep may already have reclaimed, or
    proves the deadline was decorative.
    """

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()

    _refused(
        client,
        _env(_ref(url=url, name="report.csv", payload=payload, expires_in=-5)),
        because="an expired attachment reference",
    )

    assert objects.requested == [], "an expired reference must be refused before any fetch"
    assert client.calls == [], "no container may be started for a refused claim"
    _left_nothing(staged)


def test_a_digest_mismatch_is_refused_and_materializes_nothing(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The digest is verified against the bytes that actually arrived.

    The claim's sha256 is of what the worker parked. If what comes back differs,
    the object was swapped or truncated, and handing it to the agent is handing
    it content nobody vouched for.
    """

    payload = b"a,b\n1,2\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    wrong = hashlib.sha256(b"something else entirely").hexdigest()
    client = _client()

    _refused(
        client,
        _env(_ref(url=url, name="report.csv", payload=payload, sha256=wrong)),
        because="an attachment whose bytes do not match the signed digest",
    )

    assert objects.requested == [url], "the digest can only be checked after the download"
    assert client.calls == []
    _left_nothing(staged)


def test_a_non_http_reference_is_refused_and_materializes_nothing(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """``file://`` would turn the reference into a local-read primitive.

    ``decode_attachment_refs`` already refuses this, so the driver gets it for
    free by decoding -- which is the point: the refusal exists only if the
    driver actually decodes rather than pattern-matching the env value.
    """

    client = _client()

    _refused(
        client,
        _env(_ref(url="file:///etc/passwd", name="report.csv", payload=b"x")),
        because="an attachment reference with a non-HTTP(S) URL",
    )

    assert objects.requested == []
    assert client.calls == []
    _left_nothing(staged)


@pytest.mark.parametrize("name", ["", "   ", ".", "..", "/"], ids=lambda n: repr(n))
def test_a_filename_that_escapes_the_mount_root_is_refused(
    name: str, objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The names the chart's attachments-init calls "unusable", refused here too.

    The filename is text supplied by whoever uploaded the file. None of these
    can name a file inside the mount, so materializing one either writes outside
    the root or clobbers the root itself.
    """

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()

    _refused(
        client,
        _env(_ref(url=url, name=name, payload=payload)),
        because=f"an attachment named {name!r}",
    )

    assert objects.requested == [], "an unusable name must be refused before any fetch"
    assert client.calls == []
    _left_nothing(staged)


def test_a_name_carrying_a_path_never_writes_outside_the_mount_root(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """Refuse it or basename it -- but nothing may land above the root.

    ``attachments-init`` takes the basename and then re-checks the resolved
    parent, so a traversal is neutralized rather than refused there. Either
    behavior is fine here; a file appearing outside the mount is not.
    """

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()

    try:
        client.create_claim(
            "claim-attachments",
            pool="pool",
            env=_env(_ref(url=url, name="../escape.csv", payload=payload)),
        )
    except AttachmentResolutionError:
        _left_nothing(staged)
        return

    root, _mode = _attachment_mount(client.calls[0])
    strays = [
        path
        for staged_dir in staged
        for path in staged_dir.rglob("escape.csv")
        if path.parent != root
    ]
    assert strays == [], f"an attachment name traversed out of the mount root: {strays}"


# --- the size cap, both directions -------------------------------------------


def test_a_file_just_under_the_cap_is_materialized_whole(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    payload = b"c" * 64
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client(limits=_limits(max_file_bytes=64, read_chunk_bytes=16))
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(_ref(url=url, name="report.csv", payload=payload)),
    )

    root, _mode = _attachment_mount(client.calls[0])
    assert (root / "report.csv").read_bytes() == payload


def test_a_file_over_the_cap_is_refused_and_materializes_nothing(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    payload = b"c" * 96
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client(limits=_limits(max_file_bytes=64, read_chunk_bytes=16))

    error = _refused(
        client,
        _env(_ref(url=url, name="report.csv", payload=payload)),
        because="an attachment over the configured per-file cap",
    )

    assert "64" in str(error), f"the refusal must name the cap it enforced: {str(error)!r}"
    assert client.calls == []
    _left_nothing(staged)


def test_the_oversize_download_stops_pulling_at_the_cap(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The cap is enforced WHILE STREAMING, never on a buffered body.

    ADR-0059 decision 3, and the shape ``_prepare_workspace`` and
    ``_read_bounded_upload`` both use: the loop refuses the moment the running
    total crosses the cap, so memory never holds more than one chunk past it.
    Counting what the body was asked for is the only way to see the difference;
    a driver that reads the whole 4 KiB and then measures it passes the refusal
    test above and fails here.
    """

    payload = b"c" * 4096
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload, chunk_size=16)
    client = _client(limits=_limits(max_file_bytes=64, read_chunk_bytes=16))

    _refused(
        client,
        _env(_ref(url=url, name="report.csv", payload=payload)),
        because="an attachment over the configured per-file cap",
    )

    delivered = objects.delivered.get(url, 0)
    assert delivered <= 64 + 16, (
        f"{delivered} of {len(payload)} bytes were pulled for a 64-byte cap; the "
        "download must be refused at the crossing chunk, not buffered and then measured"
    )
    _left_nothing(staged)


# --- cleanup -----------------------------------------------------------------


def test_deleting_the_claim_removes_the_materialized_directory(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """Same lifecycle as the bundle and workspace dirs: gone with the claim.

    A per-claim host dir that outlives its container is a copy of a person's
    upload sitting on the developer's laptop indefinitely, past every retention
    window the lane bothered to define.
    """

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client()
    client.create_claim(
        "claim-attachments",
        pool="pool",
        env=_env(_ref(url=url, name="report.csv", payload=payload)),
    )
    root, _mode = _attachment_mount(client.calls[0])
    assert root.exists()

    client.delete_claim("claim-attachments")

    assert not root.exists(), "the attachment dir outlived the claim it belonged to"
    _left_nothing(staged)


def test_a_failed_container_boot_removes_the_materialized_directory(
    objects: _FakeObjectStore, staged: list[Path]
) -> None:
    """The bundle and workspace dirs are cleaned when ``docker run`` fails.

    Without the same handling, every failed boot on a laptop -- a busy port, a
    stopped daemon -- leaves another copy of the upload behind, and the claim
    that would have deleted it never existed.
    """

    class _FailingDocker(_RecordingDocker):
        def _docker(self, args: list[str], *, check: bool = True) -> str:
            self.calls.append(args)
            if args[0] == "run":
                raise DockerError("docker run failed")
            return ""

    payload = b"a,b\n"
    url = objects.add("https://objects.example.test/attachments/a/000.bin", payload)
    client = _client(docker_class=_FailingDocker)

    with pytest.raises(DockerError):
        client.create_claim(
            "claim-attachments",
            pool="pool",
            env=_env(_ref(url=url, name="report.csv", payload=payload)),
        )

    assert staged, (
        "no attachment dir was staged at all, so this test would pass vacuously: "
        "the driver never materialized the files it was given a reference to"
    )
    _left_nothing(staged)
