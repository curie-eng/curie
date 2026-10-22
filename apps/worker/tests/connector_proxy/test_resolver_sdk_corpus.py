"""The reference SDK runs the shared wire corpus on Python 3.12 (ADR 0239, AC5).

"Bundle, identity and request contract" 5: the reference SDK is one
dependency-free, Python 3.12-compatible source file that adopters vendor and
exercise against the same executable wire corpus. Each provider API call
needs a new resolve; the SDK restricts declared origins, refuses credential
forwarding on redirects, and partitions auth and cache state by
binding/account/revision.

This module runs the canonical ``run_wire_corpus.py`` under a real Python 3.12
(``uv python find --no-project 3.12``; not finding one is an error) with
``-I -B`` against a live resolver, a live API and Postgres, and the fake HTTPS
provider. It then checks what the runner cannot vouch for itself: what the
provider actually received and what the resolver actually counted.

Fixture JSON the harness writes and passes as ``--fixture`` (the runner is
built to this schema)::

    {
      "version": 1,
      "socket": "<primary resolver socket path>",
      "slot": "access_token",
      "handles": {"<role>": "<handle>", ...},
      "sockets": {"<role>": "<socket path that role's handle belongs to>", ...},
      "slots": {"<role>": "<slot to request for that role>", ...},
      "expected": {
        "member":          {"status": 200, "admission": "credential", "binding": "<uuid>",
                            "account": "<str>", "revision": 1, "expires_at": <int>,
                            "field": "token", "text": "<bearer token text>"},
        "second_member":   {same shape as member, a different binding/account/text},
        "identity_member": {"status": 200, "admission": "identity", "binding": "<uuid>",
                            "account": "<str>", "revision": 1, "expires_at": <int>},
        "foreign":         {"status": 401, "refusal": "handle_unknown"},
        "expired":         {"status": 401, "refusal": "handle_expired"},
        "removed":         {"status": 401, "refusal": "handle_unknown"},
        "stale_generation":{"status": 403, "refusal": "policy_generation_mismatch"}
      },
      "barriers": {
        "reimport": {"role": "member",
                     "expected": {member's shape with "revision": 2 and the new "text"}},
        "disable":  {"role": "member",
                     "expected": {"status": 403, "refusal": "binding_disabled"}}
      },
      "provider": {
        "origin": "https://localhost:<port>",
        "upper_origin": "https://LOCALHOST:<port>",
        "allowed_path": "/v1/items",
        "undeclared_origin": "https://127.0.0.1:<port>",
        "redirect_path": "/v1/moved",
        "redirect_target_path": "/v1/moved-target",
        "ca_file": "<PEM CA file that issued the provider's localhost certificate>"
      },
      "authorization_sha256": {
        "member":        {"before_reimport": "<hex>", "after_reimport": "<hex>"},
        "second_member": {"before_reimport": "<hex>", "after_reimport": "<hex>"}
      }
    }

``authorization_sha256`` names, per credential role and per phase, the hex
SHA-256 (of the UTF-8 bytes) of the exact Authorization value that role's
provider call must send: ``"Bearer " + <that role's token text>``.
``before_reimport`` holds until the ``reimport`` barrier case has run,
``after_reimport`` from then on (the member's token changes at the barrier;
the second member's does not, so its two digests are equal).

Roles: ``member`` and ``second_member`` hold platform-custody static
credentials on the primary application, each a single text field ``token``
that ``bearer`` sends; ``identity_member`` is on a separate connector-custody
application with its own socket and requests the empty identity slot;
``foreign`` is a live handle issued by that other application's resolver;
``expired`` lapses before the runner starts and answers ``handle_expired`` on
first presentation only (an expired handle is then forgotten);
``removed`` was registered then removed; ``stale_generation`` carries the
previous policy generation. Every ``expires_at`` other than ``expired`` is at
least ten minutes out.

Runner protocol this test drives (plan R1-6 plus the line contract it needs):

1. A barrier case prints ``{"barrier": "<name>"}`` and blocks on one stdin
   line; the test performs the named action through the API (``reimport``:
   the member's binding is reimported with the barrier's new text;
   ``disable``: the member's binding is disabled) and writes ``go``.
2. Every corpus case, barrier cases included, prints exactly one line
   ``{"id": <case id>, "kind": <kind>, "passed": true|false, ...}``.
3. One summary line carries ``python`` (``sys.version_info[:3]``) and, if
   present, ``failed`` must be 0.
4. Exactly one provider call happens between the ``reimport`` and ``disable``
   barriers (the R1-6 sequence: call, barrier reimport, call, barrier
   disable, refused call). The test asserts that window's resolver and
   provider counts exactly.
5. Credential attribution (code review F3). The fake provider answers every
   ordinary request with a JSON body carrying ``authorization_sha256``: the
   hex SHA-256 of the Authorization value it received, or null. A provider
   case whose expected outcome is ``response`` passes only when that echoed
   digest equals ``fixture["authorization_sha256"][<case role>][<phase>]``,
   so a client that sends a credential cached from an earlier call, or
   another member's, fails its case. The digest is compared, never printed.
   The test independently compares the provider's own ordered observations
   with the same per-call expectations, and runs a mutated SDK copy that
   reuses the first call's credential, which must make the runner exit
   nonzero with the attribution cases failed.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from _fake_provider import (
    ALLOWED_PATH,
    REDIRECT_PATH,
    REDIRECT_TARGET_PATH,
    FakeProvider,
    authorization_digest,
)
from _resolver_harness import IDENTITY_SLOT, STATIC_FIELD, Fleet, Member, ResolverHarness
from _shared_connector_support import SLOT, secret_sentinel

SDK_DIR = Path(__file__).resolve().parents[2] / "src" / "curie_connector_proxy" / "resolver" / "sdk"
SDK_FILE = SDK_DIR / "curie_resolver_sdk.py"
CORPUS_FILE = SDK_DIR / "wire_corpus.json"
RUNNER_FILE = SDK_DIR / "run_wire_corpus.py"
RUN_DEADLINE_S = 180.0
SDK_NAMES = (
    "SOCKET_ENV",
    "RESOLVE_PATH",
    "ResolverRefused",
    "OriginRefused",
    "RedirectRefused",
    "Field",
    "Resolution",
    "ResolverClient",
    "origin_key",
    "PartitionedCache",
    "ProviderResponse",
    "bearer",
    "ProviderClient",
)


def python_312() -> str:
    """The Python 3.12 interpreter; not finding one is an error, never a skip."""

    uv = shutil.which("uv")
    if uv is None:
        pytest.fail("uv is not on PATH, so no Python 3.12 can be located")
    found = subprocess.run(
        [uv, "python", "find", "--no-project", "3.12"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    lines = found.stdout.strip().splitlines()
    if found.returncode != 0 or not lines:
        pytest.fail(f"uv python find 3.12 failed (exit {found.returncode}): {found.stderr[-400:]}")
    interpreter = lines[-1].strip()
    version = subprocess.run(
        [interpreter, "-I", "-B", "-c", "import sys; print(list(sys.version_info[:2]))"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if version.returncode != 0 or json.loads(version.stdout) != [3, 12]:
        pytest.fail(f"{interpreter} is not Python 3.12: {version.stdout!r} {version.stderr[-400:]}")
    return interpreter


def _imported_modules(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"relative import at line {node.lineno}"
            assert node.module is not None
            names.add(node.module.split(".")[0])
    return names


def test_sdk_file_is_stdlib_only_and_parses_for_312() -> None:
    interpreter = python_312()
    for path in (SDK_FILE, RUNNER_FILE):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path), feature_version=(3, 12))
        modules = _imported_modules(tree)
        assert modules, path
        outside = sorted(name for name in modules if name not in sys.stdlib_module_names)
        assert outside == [], (path.name, outside)
        compiled = subprocess.run(
            [
                interpreter,
                "-I",
                "-B",
                "-c",
                "import sys; compile(open(sys.argv[1], encoding='utf-8').read(), sys.argv[1], "
                "'exec')",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert compiled.returncode == 0, compiled.stderr[-800:]
    sdk_tree = ast.parse(SDK_FILE.read_text(encoding="utf-8"), feature_version=(3, 12))
    future = [
        node
        for node in sdk_tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "__future__"
    ]
    assert any(alias.name == "annotations" for node in future for alias in node.names)

    # Loaded by path under 3.12 with no install: the public names exist.
    probe = (
        "import importlib.util, json, sys\n"
        "spec = importlib.util.spec_from_file_location('curie_resolver_sdk', sys.argv[1])\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "names = json.loads(sys.argv[2])\n"
        "print(json.dumps({'missing': [n for n in names if not hasattr(module, n)],"
        " 'socket_env': module.SOCKET_ENV, 'path': module.RESOLVE_PATH}))\n"
    )
    loaded = subprocess.run(
        [interpreter, "-I", "-B", "-c", probe, str(SDK_FILE), json.dumps(SDK_NAMES)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert loaded.returncode == 0, loaded.stderr[-800:]
    assert json.loads(loaded.stdout) == {
        "missing": [],
        "socket_env": "CURIE_SHARED_RESOLVER_SOCKET",
        "path": "/v1/resolve",
    }


def _credential_expectation(
    member: Member, expires_at: int, *, text: str | None = None, revision: int = 1
) -> dict[str, Any]:
    return {
        "status": 200,
        "admission": "credential",
        "binding": member.binding_id,
        "account": member.account,
        "revision": revision,
        "expires_at": expires_at,
        "field": STATIC_FIELD,
        "text": member.text if text is None else text,
    }


class _Run:
    """The live world one corpus run talks to, and the observations it takes."""

    def __init__(self, fleet: Fleet, provider: FakeProvider) -> None:
        self.provider = provider
        self.app = fleet.application(origins=(provider.origin,))
        self.member = self.app.add_member(
            fields={STATIC_FIELD: f"tok-member-{os.urandom(8).hex()}"}
        )
        self.second = self.app.add_member(
            fields={STATIC_FIELD: f"tok-second-{os.urandom(8).hex()}"}
        )
        self.harness: ResolverHarness = fleet.launch(self.app)
        custom = fleet.application(slots=(), custody="connector")
        self.identity = custom.add_member()
        self.identity_harness: ResolverHarness = fleet.launch(custom)
        self.new_text = f"tok-member-r2-{os.urandom(8).hex()}"
        self.old_text = self.member.text
        self.snapshots: dict[str, tuple[int, int, int]] = {}
        self.barriers: list[str] = []

    def fixture(self) -> dict[str, Any]:
        harness, identity_harness = self.harness, self.identity_harness
        member_invocation = harness.register(self.member, expires_in=900)
        second_invocation = harness.register(self.second, expires_in=900)
        identity_invocation = identity_harness.register(self.identity, expires_in=900)
        foreign = identity_harness.handle(self.identity, expires_in=900)
        removed = harness.register(self.member, expires_in=900)
        assert harness.handles.remove(removed.handle) is True
        stale = harness.handle(self.member, generation=self.app.generation() - 1, expires_in=900)
        lapse_at = int(time.time()) + 2
        expired = harness.handle(self.member, expires_at=lapse_at)
        primary, other = harness.socket_path, identity_harness.socket_path
        roles = {
            "member": (member_invocation.handle, primary, SLOT),
            "second_member": (second_invocation.handle, primary, SLOT),
            "identity_member": (identity_invocation.handle, other, IDENTITY_SLOT),
            "foreign": (foreign, primary, SLOT),
            "expired": (expired, primary, SLOT),
            "removed": (removed.handle, primary, SLOT),
            "stale_generation": (stale, primary, SLOT),
        }
        member_expires = member_invocation.context.expires_at
        document = {
            "version": 1,
            "socket": primary,
            "slot": SLOT,
            "handles": {role: value[0] for role, value in roles.items()},
            "sockets": {role: value[1] for role, value in roles.items()},
            "slots": {role: value[2] for role, value in roles.items()},
            "expected": {
                "member": _credential_expectation(self.member, member_expires),
                "second_member": _credential_expectation(
                    self.second, second_invocation.context.expires_at
                ),
                "identity_member": {
                    "status": 200,
                    "admission": "identity",
                    "binding": self.identity.binding_id,
                    "account": self.identity.account,
                    "revision": 1,
                    "expires_at": identity_invocation.context.expires_at,
                },
                "foreign": {"status": 401, "refusal": "handle_unknown"},
                "expired": {"status": 401, "refusal": "handle_expired"},
                "removed": {"status": 401, "refusal": "handle_unknown"},
                "stale_generation": {"status": 403, "refusal": "policy_generation_mismatch"},
            },
            "barriers": {
                "reimport": {
                    "role": "member",
                    "expected": _credential_expectation(
                        self.member, member_expires, text=self.new_text, revision=2
                    ),
                },
                "disable": {
                    "role": "member",
                    "expected": {"status": 403, "refusal": "binding_disabled"},
                },
            },
            "provider": {
                "origin": self.provider.origin,
                "upper_origin": self.provider.upper_origin,
                "allowed_path": ALLOWED_PATH,
                "undeclared_origin": self.provider.undeclared_origin,
                "redirect_path": REDIRECT_PATH,
                "redirect_target_path": REDIRECT_TARGET_PATH,
                "ca_file": str(self.provider.ca_file),
            },
            "authorization_sha256": self.digests(),
        }
        # The expired role must have lapsed before the runner presents it.
        while time.time() < lapse_at + 0.1:
            time.sleep(0.05)
        return document

    def digests(self) -> dict[str, dict[str, str]]:
        """The expected Authorization digest per credential role and phase."""

        def bearer(text: str) -> str:
            return authorization_digest(f"Bearer {text}")

        return {
            "member": {
                "before_reimport": bearer(self.old_text),
                "after_reimport": bearer(self.new_text),
            },
            "second_member": {
                "before_reimport": bearer(self.second.text),
                "after_reimport": bearer(self.second.text),
            },
        }

    def observe(self) -> tuple[int, int, int]:
        return self.harness.admissions, self.harness.releases, len(self.provider.requests)

    def barrier(self, name: str) -> None:
        self.barriers.append(name)
        self.snapshots[name] = self.observe()
        if name == "reimport":
            response = self.member.reimport_text(self.new_text, resource_version="101")
            assert response.status_code == 200, response.text
        elif name == "disable":
            response = self.app.store.disable_binding(self.member.binding_id)
            assert response.status_code == 200, response.text
        else:
            pytest.fail(f"the corpus named an unknown barrier {name!r}")


def _drive(process: subprocess.Popen[str], run: _Run) -> list[dict[str, Any]]:
    """Read the runner's lines, answering each barrier, under one deadline."""

    lines: queue.Queue[str | None] = queue.Queue()

    def pump() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put(line)
        lines.put(None)

    threading.Thread(target=pump, daemon=True).start()
    records: list[dict[str, Any]] = []
    deadline = time.monotonic() + RUN_DEADLINE_S
    while True:
        try:
            line = lines.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty:
            process.kill()
            pytest.fail("the corpus run exceeded its deadline")
        if line is None:
            break
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            process.kill()
            pytest.fail(f"the runner printed a non-JSON line: {line[:200]!r}")
        records.append(record)
        if "barrier" in record:
            run.barrier(str(record["barrier"]))
            assert process.stdin is not None
            process.stdin.write("go\n")
            process.stdin.flush()
    process.wait(timeout=30)
    return records


def _execute(
    run: _Run, interpreter: str, sdk_file: Path
) -> tuple[int, list[dict[str, Any]], str, tuple[int, int, int], tuple[int, int, int]]:
    """Run the canonical runner on ``sdk_file`` against ``run``'s live world.

    Answers the exit code, the printed records, the stderr tail, and the
    (admissions, releases, provider requests) observations before and after.
    """

    work = Path(tempfile.mkdtemp(prefix="crs-corpus-", dir="/tmp"))
    try:
        fixture_file = work / "fixture.json"
        fixture_file.write_text(json.dumps(run.fixture()))
        fixture_file.chmod(0o600)
        stderr_file = work / "stderr.txt"
        start = run.observe()
        with stderr_file.open("w") as stderr:
            process = subprocess.Popen(
                [
                    interpreter,
                    "-I",
                    "-B",
                    str(RUNNER_FILE),
                    "--sdk",
                    str(sdk_file),
                    "--corpus",
                    str(CORPUS_FILE),
                    "--fixture",
                    str(fixture_file),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr,
                text=True,
                env={
                    "PATH": os.environ.get("PATH", ""),
                    "LANG": "C.UTF-8",
                    "CURIE_SHARED_RESOLVER_SOCKET": run.harness.socket_path,
                },
            )
            try:
                records = _drive(process, run)
            finally:
                if process.poll() is None:
                    process.kill()
        error_tail = stderr_file.read_text()[-2000:]
    finally:
        shutil.rmtree(work, ignore_errors=True)
    assert process.returncode is not None
    return process.returncode, records, error_tail, start, run.observe()


def _corpus() -> dict[str, Any]:
    corpus: dict[str, Any] = json.loads(CORPUS_FILE.read_text(encoding="utf-8"))
    assert corpus["version"] == 1
    case_ids = [case["id"] for case in corpus["cases"]]
    assert case_ids and len(set(case_ids)) == len(case_ids)
    return corpus


def _expected_authorizations(
    corpus: dict[str, Any], digests: dict[str, dict[str, str]]
) -> list[str]:
    """The Authorization digest of every provider call the corpus makes, in order.

    A provider case reaches the provider when it expects a response or a
    refused redirect (the credential is sent once, to the declared origin);
    any other outcome refuses before a byte is sent.
    """

    phase = "before_reimport"
    expected: list[str] = []
    for case in corpus["cases"]:
        if case["kind"] == "barrier" and case.get("barrier") == "reimport":
            phase = "after_reimport"
        if case["kind"] != "provider":
            continue
        if case["expect"]["outcome"] in ("response", "redirect_refused"):
            expected.append(digests[case["role"]][phase])
    return expected


def test_sdk_corpus_passes_under_python_312_against_the_live_resolver(
    fleet: Fleet, provider: FakeProvider
) -> None:
    interpreter = python_312()
    run = _Run(fleet, provider)
    corpus = _corpus()
    case_ids = [case["id"] for case in corpus["cases"]]
    returncode, records, error_tail, start, end = _execute(run, interpreter, SDK_FILE)

    # The runner's own verdict.
    assert returncode == 0, error_tail
    cases = [record for record in records if "id" in record]
    assert sorted(record["id"] for record in cases) == sorted(case_ids), error_tail
    failed = [record for record in cases if record.get("passed") is not True]
    assert failed == [], failed
    (summary,) = [record for record in records if "python" in record]
    assert list(summary["python"])[:2] == [3, 12], summary
    assert summary.get("failed", 0) == 0, summary
    assert run.barriers == ["reimport", "disable"], run.barriers

    # What the provider actually received.
    requests = provider.requests
    assert all(not request.host.startswith("127.0.0.1") for request in requests), [
        request.host for request in requests
    ]
    assert provider.calls(REDIRECT_TARGET_PATH) == []
    tokens = {run.old_text, run.new_text, run.second.text}
    allowed = provider.calls(ALLOWED_PATH)
    redirected = provider.calls(REDIRECT_PATH)
    assert redirected, "the corpus never exercised a redirect"
    for request in [*allowed, *redirected]:
        assert len(request.authorization) == 1, request.authorization
        (header,) = request.authorization
        assert header.startswith("Bearer ") and header[len("Bearer ") :] in tokens
    used = {request.authorization[0][len("Bearer ") :] for request in allowed}
    assert {run.old_text, run.new_text} <= used, used
    assert len({request.peer_port for request in requests}) == len(requests), "pooled connection"

    # One resolve per provider call: the window between the barriers holds
    # exactly one call, with one admission, one release and the new token.
    admissions_r, releases_r, calls_r = run.snapshots["reimport"]
    admissions_d, releases_d, calls_d = run.snapshots["disable"]
    assert (admissions_d - admissions_r, releases_d - releases_r, calls_d - calls_r) == (1, 1, 1)
    (window,) = requests[calls_r:calls_d]
    assert window.path == ALLOWED_PATH
    assert window.authorization == (f"Bearer {run.new_text}",)
    assert all(run.old_text not in "".join(request.authorization) for request in requests[calls_r:])
    for request in requests[calls_d:]:
        assert run.new_text not in "".join(request.authorization), "a call after disable"
    # Every provider call was preceded by its own release.
    provider_calls = len(allowed) + len(redirected)
    assert end[1] - start[1] >= provider_calls, (start, end, provider_calls)
    assert end[0] - start[0] >= end[1] - start[1]

    # Credential attribution: every call, in order, carried exactly the
    # credential of its own role at its own phase (code review F3).
    observed = [request.authorization_sha256 for request in requests]
    assert observed == _expected_authorizations(corpus, run.digests()), [
        request.path for request in requests
    ]


# The minimal attribution mutation: authorize every call with the resolution
# of the FIRST call, while the partition still comes from the current resolve.
# Every origin, redirect and partition check still holds, so only a corpus
# that checks the exact credential each call sent can notice.
_REUSE_FIRST_CREDENTIAL = (
    re.compile(r"self\._authorize\(\s*resolution\b"),
    "self._authorize(self.__dict__.setdefault('_crs_first_resolution', resolution)",
)
ATTRIBUTION_CASES = frozenset(
    {"provider-second-member-partition", "provider-after-reimport-new-partition"}
)


def test_sdk_corpus_fails_an_sdk_copy_that_reuses_an_earlier_credential(
    fleet: Fleet, provider: FakeProvider, tmp_path: Path
) -> None:
    """Negative control: the corpus detects wrong credential attribution (code review F3)."""

    interpreter = python_312()
    original = SDK_FILE.read_text(encoding="utf-8")
    pattern, replacement = _REUSE_FIRST_CREDENTIAL
    mutated, count = pattern.subn(replacement, original)
    assert count == 1, "the attribution mutation no longer applies to the SDK"
    assert mutated != original
    copy = tmp_path / "vendored" / "curie_resolver_sdk.py"
    copy.parent.mkdir()
    copy.write_text(mutated, encoding="utf-8")

    run = _Run(fleet, provider)
    corpus = _corpus()
    kinds = {case["id"]: case["kind"] for case in corpus["cases"]}
    returncode, records, error_tail, _, _ = _execute(run, interpreter, copy)

    assert returncode != 0, "the corpus passed an SDK that sends another call's credential"
    cases = {record["id"]: record for record in records if "id" in record}
    assert set(cases) == set(kinds), error_tail
    failed = {case_id for case_id, record in cases.items() if record.get("passed") is not True}
    assert failed >= ATTRIBUTION_CASES, (sorted(failed), error_tail)
    assert {kinds[case_id] for case_id in failed} == {"provider"}, sorted(failed)
    (summary,) = [record for record in records if "python" in record]
    assert list(summary["python"])[:2] == [3, 12], summary
    assert summary["failed"] == len(failed), summary
    # No failing line quotes a credential.
    printed = json.dumps(records)
    for text in (run.old_text, run.new_text, run.second.text):
        assert text not in printed

    # The mutation really sent the wrong credential: the provider saw the
    # first call's token on the second member's call and after the reimport.
    observed = [request.authorization_sha256 for request in provider.requests]
    expected = _expected_authorizations(corpus, run.digests())
    assert len(observed) == len(expected)
    assert observed != expected
    first = run.digests()["member"]["before_reimport"]
    assert set(observed) == {first}


# --- the SDK in process ----------------------------------------------------------


def _load_sdk() -> ModuleType:
    """The SDK file loaded by path, unregistered, as a vendored copy would be."""

    name = f"curie_resolver_sdk_probe_{os.urandom(4).hex()}"
    spec = importlib.util.spec_from_file_location(name, SDK_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stub_client(sdk: ModuleType, provider: FakeProvider, token: str) -> Any:
    """A ``ProviderClient`` whose resolver answers one fixed credential admission."""

    class StubResolver(sdk.ResolverClient):  # type: ignore[name-defined,misc]
        def __init__(self) -> None:
            super().__init__("/nonexistent/stub.sock")
            self.calls = 0

        def resolve(self, handle: str, slot: str, purpose: str) -> Any:
            self.calls += 1
            return sdk.Resolution(
                admission="credential",
                binding="00000000-0000-4000-8000-000000000001",
                account="acct-stub",
                revision=1,
                origins=(provider.origin,),
                expires_at=int(time.time()) + 600,
                slot=slot,
                fields={STATIC_FIELD: sdk.Field(text=token)},
            )

    return sdk.ProviderClient(StubResolver(), SLOT, ssl_context=provider.client_ssl())


def _raised_in_sdk(error: BaseException) -> None:
    """The exception was raised by the SDK's own code, not deep in ``http.client``."""

    frames = traceback.extract_tb(error.__traceback__)
    assert frames, "the exception carries no traceback"
    assert Path(frames[-1].filename).resolve() == SDK_FILE.resolve(), frames[-1]


def _shows_nothing(error: BaseException, *values: str) -> None:
    shown = f"{error} {error!r} " + "".join(traceback.format_exception(error))
    for value in values:
        assert value not in shown, f"{type(error).__name__} or its chain carries a secret"


def test_sdk_refuses_a_caller_host_header_before_sending(provider: FakeProvider) -> None:
    """A caller cannot override the checked origin's authority (security review S1)."""

    sdk = _load_sdk()
    token = f"tok-host-{os.urandom(8).hex()}"
    client = _stub_client(sdk, provider, token)
    url = provider.origin + ALLOWED_PATH
    for name in ("Host", "host", "HOST"):
        with pytest.raises(ValueError) as refused:
            client.request(
                "crh1.stub", "GET", url, purpose="host probe", headers={name: "evil.example"}
            )
        assert not isinstance(refused.value, UnicodeError)
        _raised_in_sdk(refused.value)
        _shows_nothing(refused.value, token)
    assert provider.requests == [], "a request with a caller Host header reached the provider"

    # Liveness: ordinary headers pass, and the authority is the checked origin's.
    response = client.request(
        "crh1.stub",
        "GET",
        url,
        purpose="host probe",
        headers={"Accept": "application/json", "X-Trace": "abc"},
    )
    assert response.status == 200
    (seen,) = provider.requests
    assert seen.host.lower() == f"localhost:{provider.port}", seen.host
    assert seen.authorization == (f"Bearer {token}",)
    echoed = json.loads(response.body)["authorization_sha256"]
    assert echoed == seen.authorization_sha256 == authorization_digest(f"Bearer {token}")


def test_sdk_header_errors_never_carry_the_credential(provider: FakeProvider) -> None:
    """A header the SDK cannot send raises a fixed error, chain included (security review S4).

    ``http.client`` encodes header values as Latin-1, and its
    ``UnicodeEncodeError`` quotes the whole value, Authorization included.
    The SDK must refuse such values itself, with a message that names no
    value, and never let that exception ride along as the context.
    """

    sdk = _load_sdk()
    secret = secret_sentinel("NONLATIN")
    url = provider.origin + ALLOWED_PATH
    ordinary = f"tok-ordinary-{os.urandom(8).hex()}"
    cases = (
        ("non-latin-1 credential", secret + "\u2603", {}),
        ("non-latin-1 caller header", ordinary, {"X-Note": secret + "\u2603"}),
        ("CR/LF caller header", ordinary, {"X-Note": "ok\r\nX-Leak: " + secret}),
        ("LF caller header", ordinary, {"X-Note": "ok\nX-Leak: " + secret}),
    )
    for label, token, headers in cases:
        client = _stub_client(sdk, provider, token)
        with pytest.raises(ValueError) as refused:
            client.request("crh1.stub", "GET", url, purpose="header probe", headers=headers)
        error = refused.value
        assert not isinstance(error, UnicodeError), label
        _raised_in_sdk(error)
        _shows_nothing(error, secret, ordinary)
    assert provider.requests == [], "a request with an unsendable header reached the provider"

    # Liveness: an ordinary credential and header are sent as they are.
    response = _stub_client(sdk, provider, ordinary).request(
        "crh1.stub", "GET", url, purpose="header probe", headers={"X-Note": "plain"}
    )
    assert response.status == 200
    (seen,) = provider.requests
    assert seen.authorization == (f"Bearer {ordinary}",)
