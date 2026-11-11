"""Pure policy, ledger and evidence logic for the single-profile acceptance driver (#5375).

Everything here runs without a cluster, a forge or a network: the approved
numbers, the refusals that run before any model session, the private SQLite
ledger and its budget rules, native transcript parsing, the in-pod harvester
program and its stream consumer, secret scrubbing, and the judge. The live
orchestration in ``personal_profile.py`` only gathers observations and records
them under the keys this module's judge reads, so the judge never infers.

Standard library only. Nothing in this module prints, logs or persists a held
secret; every error names a variable or a refusal code, never a value.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import dataclasses
import datetime as dt
import hashlib
import http.client
import json
import os
import re
import sqlite3
import stat
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

# --------------------------------------------------------------------------
# Approved numbers (maintainer decision, 2026-11-10)
# --------------------------------------------------------------------------

MAX_SESSIONS = 8
MAX_TURNS = 96
SESSION_TURNS = 32
MAX_OUTPUT_TOKENS = 32768
SESSION_OUTPUT_TOKENS = 8192
SESSION_SECONDS = 300
# Per response. The live watchdog stops paid work once a session reaches its
# allowance; the response in flight at that moment is the bounded overshoot.
RESPONSE_OUTPUT_TOKENS = 4096
LIVE_SECONDS = 2400
CLEANUP_SECONDS = 300
PAID_RETRIES = 0
GRANT_MIN_LIFETIME = LIVE_SECONDS + CLEANUP_SECONDS
IMPLEMENTER_MODEL = "claude-sonnet-5-5"
REVIEWER_MODEL = "claude-opus-5-5"
IMPLEMENTER = "implementer"
PLAN_REVIEWER = "plan-reviewer"
DIFF_REVIEWER = "diff-reviewer"
REVIEWER_ROLES = (PLAN_REVIEWER, DIFF_REVIEWER)
UNATTRIBUTED = "unattributed"
# One attempt reserves its implementer, both reviewers and one diff re-review
# round (the bundle's diff loop may ask for changes once). Two attempts fill
# the eight approved sessions exactly; an unused slot settles to zero usage.
ATTEMPT_ROLES = (IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER, DIFF_REVIEWER)


def approved_ceiling() -> str:
    """The canonical ``--approve-live`` string, derived from the constants above."""

    return ",".join(
        [
            f"sessions={MAX_SESSIONS}",
            f"turns={MAX_TURNS}",
            f"session-turns={SESSION_TURNS}",
            f"output-tokens={MAX_OUTPUT_TOKENS}",
            f"session-output-tokens={SESSION_OUTPUT_TOKENS}",
            f"session-seconds={SESSION_SECONDS}",
            f"live-seconds={LIVE_SECONDS}",
            f"cleanup-seconds={CLEANUP_SECONDS}",
            f"paid-retries={PAID_RETRIES}",
        ]
    )


DEFAULT_TENANT = "00000000-0000-0000-0000-000000000001"
PERSONAL_MODE = "personal"
OWNER_GENERATION = 1
REVISION_LABEL = "org.opencontainers.image.revision"
PLATFORM_IMAGES = ("curie-api", "curie-worker", "curie-runner")
LAYER_IMAGE = "curie-factory-runner"
REQUIRED_IMAGES = (*PLATFORM_IMAGES, LAYER_IMAGE)
IMAGE_MANIFEST_TYPES = (
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
)
INDEX_MANIFEST_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
# registry:2 answers an index-tagged manifest with 404 MANIFEST_UNKNOWN unless the
# Accept header names the index type, and buildkit pushes an index whenever it
# attaches a provenance attestation.
MANIFEST_ACCEPT = ", ".join((*IMAGE_MANIFEST_TYPES, *INDEX_MANIFEST_TYPES))
ATTESTATION_REFERENCE = "attestation-manifest"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "curie-personal-profile"
RUN_ANNOTATION = "curie.dev/personal-profile-run"
REDACTED = "<redacted>"

# --------------------------------------------------------------------------
# Refusals
# --------------------------------------------------------------------------

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_REFUSED = 2

FIXES: dict[str, str] = {
    "service_credentials": "unset the named service credential variables; the grant comes "
    "only from --grant-file",
    "fake_mode": "unset CURIE_FAKE_MODEL; this proof needs the real model",
    "grant_missing": "pass --grant-file naming a nonempty private file",
    "grant_unsafe": "the grant file must be a regular file you own, mode 0600, "
    "one ASCII line of 1 to 4096 bytes, not a symlink",
    "expiry_unknown": "pass --grant-expires-at as ISO 8601 with an offset or epoch seconds",
    "lifetime_insufficient": f"use a grant with at least {GRANT_MIN_LIFETIME} s remaining",
    "live_unapproved": "pass --approve-live with the exact approved ceiling string",
    "candidate_invalid": "pass --candidate as a full 40 hex commit SHA",
    "candidate_image_mismatch": "run the images subcommand for this candidate, then retry",
    "layer_repository_not_owned": "use a fresh registry; the runner layer repository holds tags",
    "candidate_mismatch": "judge the run with the candidate its ledger recorded",
    "registry_invalid": "pass --registry as a loopback HOST:PORT (127.0.0.1 or localhost)",
    "state_dir_unsafe": "--state-dir must be an existing directory you own, mode 0700",
    "ledger_missing": "no private ledger exists for this run ID",
    "ledger_unsafe": "the ledger file must be a regular file you own, mode 0600",
    "paid_retry_refused": "a run ID gets one live start; choose a new run ID",
    "usage_unknown": "an earlier session's usage is unknown, so no further start is allowed",
    "budget_exceeded": "an approved bound was exceeded; no further start is allowed",
    "allowance_exhausted": "the remaining approved allowance cannot cover another start",
    "arguments_invalid": "fix the named argument",
    "namespace_exists": "choose a namespace that does not exist; the driver deletes it on exit",
    "release_reserved": "choose a release name other than curie",
    "deployed_service_credential": "the deployed settings carry a service credential or "
    "fallback; fix the values",
    "app_kit_invalid": "--app-dir needs app.json (id, installation_id, repo) and app.pem",
    "rehearsal_run_id": "this run ID was a rehearsal; a live run needs a fresh run ID",
    "rehearsal_ledger": "a rehearsal ledger is judged only with --criterion cleanup",
}


class Refusal(Exception):
    """A refusal before any model session or cluster mutation. Names only, never values."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail

    def payload(self) -> dict[str, str]:
        fix = FIXES.get(self.code, "see the README")
        if self.detail:
            fix = f"{fix} ({self.detail})"
        return {"error": self.code, "fix": fix}


# --------------------------------------------------------------------------
# Held secrets and scrubbing
# --------------------------------------------------------------------------


def _variants(raw: bytes) -> set[bytes]:
    found = {raw, base64.b64encode(raw), base64.urlsafe_b64encode(raw)}
    found.update(item.rstrip(b"=") for item in tuple(found))
    return {item for item in found if item}


# A cleared secret is matched by a key-derivation digest under a salt that lives
# only in this process, so the fingerprint is never a cheap hash of the secret.
_FINGERPRINT_SALT = os.urandom(16)


def _fingerprint_digest(raw: bytes) -> str:
    return hashlib.scrypt(raw, salt=_FINGERPRINT_SALT, n=2**12, r=8, p=1, dklen=32).hex()


@dataclasses.dataclass(frozen=True)
class Fingerprint:
    """A cleared secret's shape for in-memory scanning: length, a 4 byte prefilter, a digest."""

    length: int
    prefix: bytes
    digest: str

    @classmethod
    def of(cls, raw: bytes) -> Fingerprint:
        return cls(len(raw), raw[:4], _fingerprint_digest(raw))

    def __repr__(self) -> str:
        return "<fingerprint>"

    def spans(self, data: bytes) -> list[tuple[int, int]]:
        found: list[tuple[int, int]] = []
        start = data.find(self.prefix)
        while start != -1:
            end = start + self.length
            if end <= len(data) and _fingerprint_digest(data[start:end]) == self.digest:
                found.append((start, end))
            start = data.find(self.prefix, start + 1)
        return found


class HeldSecret:
    """A secret value whose repr and str are redacted; clear() keeps only fingerprints."""

    def __init__(self, value: str) -> None:
        self._value = value
        raw = value.encode()
        self._fingerprints = tuple(Fingerprint.of(item) for item in sorted(_variants(raw)))
        self.held = bool(raw)

    def __repr__(self) -> str:
        return "<held secret>"

    __str__ = __repr__

    def __reduce__(self) -> Any:
        raise TypeError("a held secret cannot be serialized")

    def reveal(self) -> str:
        return self._value

    def clear(self) -> None:
        self._value = ""

    @property
    def fingerprints(self) -> tuple[Fingerprint, ...]:
        return self._fingerprints if self.held else ()


class Scrubber:
    """Replaces every held secret (live literal or cleared fingerprint) with ``<redacted>``."""

    def __init__(self, secrets: Iterable[HeldSecret] = ()) -> None:
        self._secrets: list[HeldSecret] = list(secrets)

    def __repr__(self) -> str:
        return "<scrubber>"

    def hold(self, secret: HeldSecret) -> HeldSecret:
        self._secrets.append(secret)
        return secret

    def held(self) -> tuple[HeldSecret, ...]:
        return tuple(self._secrets)

    def _spans(self, data: bytes) -> list[tuple[int, int]]:
        spans: list[tuple[int, int]] = []
        for secret in self._secrets:
            live = secret.reveal().encode()
            if live:
                for variant in _variants(live):
                    start = data.find(variant)
                    while start != -1:
                        spans.append((start, start + len(variant)))
                        start = data.find(variant, start + 1)
            for fingerprint in secret.fingerprints:
                spans.extend(fingerprint.spans(data))
        return sorted(spans)

    def found(self, data: bytes) -> bool:
        return bool(self._spans(data))

    def longest(self) -> int:
        """The longest byte form any held secret can take, live or fingerprinted."""

        lengths = [0]
        for secret in self._secrets:
            live = secret.reveal().encode()
            lengths.extend(len(variant) for variant in _variants(live) if live)
            lengths.extend(fingerprint.length for fingerprint in secret.fingerprints)
        return max(lengths)

    def scrub_bytes(self, data: bytes) -> bytes:
        spans = self._spans(data)
        if not spans:
            return data
        out = bytearray()
        cursor = 0
        for start, end in spans:
            if end <= cursor:
                continue
            out += data[cursor : max(cursor, start)] + REDACTED.encode()
            cursor = end
        out += data[cursor:]
        return bytes(out)

    def scrub(self, text: str) -> str:
        return self.scrub_bytes(text.encode()).decode(errors="replace")


@dataclasses.dataclass
class ChannelScan:
    """Sanitized result of scanning one output channel in memory."""

    scanned_bytes: int = 0
    token_bytes_found: bool = False

    def as_json(self) -> dict[str, Any]:
        return {"scanned_bytes": self.scanned_bytes, "token_bytes_found": self.token_bytes_found}


class TokenScan:
    """Counts per channel; records booleans and counts only."""

    def __init__(self, scrubber: Scrubber, channels: Sequence[str] = ()) -> None:
        self.scrubber = scrubber
        self.channels: dict[str, ChannelScan] = {name: ChannelScan() for name in channels}
        self._tails: dict[tuple[str, str], bytes] = {}

    def __repr__(self) -> str:
        return "<token scan>"

    def scan(self, channel: str, data: bytes) -> bool:
        entry = self.channels.setdefault(channel, ChannelScan())
        entry.scanned_bytes += len(data)
        hit = self.scrubber.found(data)
        entry.token_bytes_found |= hit
        return hit

    def scan_stream(self, channel: str, key: str, data: bytes) -> bool:
        """Scan one chunk of a reassembled stream, so a secret split across chunks is found.

        The previous chunk's tail (one byte shorter than the longest held form)
        is scanned again with this chunk; only new bytes are counted.
        """

        entry = self.channels.setdefault(channel, ChannelScan())
        entry.scanned_bytes += len(data)
        joined = self._tails.get((channel, key), b"") + data
        hit = self.scrubber.found(joined)
        entry.token_bytes_found |= hit
        keep = max(0, self.scrubber.longest() - 1)
        self._tails[(channel, key)] = joined[-keep:] if keep else b""
        return hit

    def as_json(self) -> dict[str, Any]:
        return {name: entry.as_json() for name, entry in sorted(self.channels.items())}


# --------------------------------------------------------------------------
# Caller environment
# --------------------------------------------------------------------------

SERVICE_CREDENTIAL_NAMES = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_FOUNDRY_API_KEY",
    "ANTHROPIC_CUSTOM_HEADERS",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CURIE_CREDENTIALS",
    "CURIE_MODEL_BASE_URL",
    "CURIE_MODEL_ENV_KEY",
    "CURIE_FACTORY_MODEL_API_KEY",
    "OPENROUTER_API_KEY",
    "OPENROUTER_TOKEN",
    "AWS_BEARER_TOKEN_BEDROCK",
)
FAKE_MODEL_NAME = "CURIE_FAKE_MODEL"
_CREDENTIAL_PATTERN = re.compile(r"^(ANTHROPIC|CLAUDE_CODE)_.*(KEY|TOKEN|SECRET)$")
CHILD_ENV_NAMES = ("PATH", "HOME", "LANG", "SSH_AUTH_SOCK")


def refuse_ambient_credentials(environ: Mapping[str, str]) -> None:
    """The first check: a nonempty service credential or fake mode in the caller env."""

    named = sorted(
        name
        for name, value in environ.items()
        if value and (name in SERVICE_CREDENTIAL_NAMES or _CREDENTIAL_PATTERN.fullmatch(name))
    )
    if named:
        raise Refusal("service_credentials", "present: " + ", ".join(named))
    if environ.get(FAKE_MODEL_NAME):
        raise Refusal("fake_mode", f"present: {FAKE_MODEL_NAME}")


def child_environment(
    environ: Mapping[str, str],
    *,
    kubeconfig: str | None,
    docker_context: str | None,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The only environment any child process receives."""

    env = {name: environ[name] for name in CHILD_ENV_NAMES if environ.get(name)}
    if kubeconfig:
        env["KUBECONFIG"] = kubeconfig
    if docker_context:
        key = "DOCKER_HOST" if docker_context.startswith(("ssh://", "tcp://")) else "DOCKER_CONTEXT"
        env[key] = docker_context
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra or {})
    return env


# --------------------------------------------------------------------------
# Inputs
# --------------------------------------------------------------------------

_SHA = re.compile(r"[0-9a-f]{40}")
_DNS_LABEL = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
_GRANT_LINE = re.compile(rb"[\x21-\x7e]{1,4096}")


def require_candidate(candidate: str) -> str:
    if not _SHA.fullmatch(candidate or ""):
        raise Refusal("candidate_invalid", "--candidate")
    return candidate


def require_run_id(run_id: str) -> str:
    if not _RUN_ID.fullmatch(run_id or ""):
        raise Refusal("arguments_invalid", "--run-id")
    return run_id


def require_dns_name(value: str, flag: str, *, limit: int) -> str:
    if not value or len(value) > limit or not _DNS_LABEL.fullmatch(value):
        raise Refusal("arguments_invalid", flag)
    return value


def require_release(release: str) -> str:
    require_dns_name(release, "--release", limit=40)
    if release == "curie":
        raise Refusal("release_reserved", "--release")
    return release


def require_live_approval(text: str | None) -> None:
    if text != approved_ceiling():
        raise Refusal("live_unapproved", "--approve-live")


def require_registry(registry: str) -> tuple[str, int]:
    """A loopback ``HOST:PORT``; returns the address to dial and the port."""

    host, _, port = (registry or "").rpartition(":")
    if host not in {"127.0.0.1", "localhost"} or not port.isdigit() or not 0 < int(port) < 65536:
        raise Refusal("registry_invalid", "--registry")
    return "127.0.0.1", int(port)


def require_private_dir(path: Path, *, code: str = "state_dir_unsafe") -> Path:
    try:
        info = os.lstat(path)
    except OSError:
        raise Refusal(code, "--state-dir") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise Refusal(code, "--state-dir")
    return path


def require_app_kit(app_dir: Path) -> dict[str, Any]:
    try:
        meta = json.loads((app_dir / "app.json").read_text())
        key_ok = (app_dir / "app.pem").is_file()
    except (OSError, ValueError):
        raise Refusal("app_kit_invalid", "--app-dir") from None
    if (
        not isinstance(meta, dict)
        or not key_ok
        or not str(meta.get("id") or "").isdigit()
        or not str(meta.get("installation_id") or "").isdigit()
        or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", str(meta.get("repo") or ""))
    ):
        raise Refusal("app_kit_invalid", "--app-dir")
    return meta


def _open_grant(path: Path | None) -> bytes:
    if path is None or not str(path):
        raise Refusal("grant_missing", "--grant-file")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        raise Refusal("grant_missing", "--grant-file") from None
    except OSError:
        raise Refusal("grant_unsafe", "--grant-file") from None
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise Refusal("grant_unsafe", "--grant-file")
        if info.st_size == 0:
            raise Refusal("grant_missing", "--grant-file")
        if info.st_size > 4097:
            raise Refusal("grant_unsafe", "--grant-file")
        data = os.read(descriptor, 4098)
    finally:
        os.close(descriptor)
    line = data[:-1] if data.endswith(b"\n") else data
    if not _GRANT_LINE.fullmatch(line):
        raise Refusal("grant_unsafe", "--grant-file")
    return line


def check_grant_file(path: Path | None) -> None:
    """Preflight: confirm a private, nonempty, single ASCII line; retain nothing."""

    buffer = bytearray(_open_grant(path))
    buffer[:] = b"\0" * len(buffer)


def load_grant(path: Path | None) -> HeldSecret:
    """Inside ``run`` only, after install and setup: the grant as a held secret."""

    return HeldSecret(_open_grant(path).decode("ascii"))


def iso_epoch(value: Any) -> float:
    """An API timestamp (ISO 8601, UTC when no offset) as epoch seconds; 0.0 when unreadable."""

    if not isinstance(value, str) or not value:
        return 0.0
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.timestamp()


def parse_expiry(text: str | None) -> float:
    """``--grant-expires-at``: ISO 8601 with an explicit offset, or integer epoch seconds."""

    value = (text or "").strip()
    if re.fullmatch(r"[0-9]{1,12}", value):
        return float(int(value))
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})", value
    ):
        raise Refusal("expiry_unknown", "--grant-expires-at")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise Refusal("expiry_unknown", "--grant-expires-at") from None
    if parsed.tzinfo is None:
        raise Refusal("expiry_unknown", "--grant-expires-at")
    return parsed.timestamp()


def require_lifetime(expires_at: float, now: float, needed: float = GRANT_MIN_LIFETIME) -> float:
    remaining = expires_at - now
    if remaining < needed:
        raise Refusal("lifetime_insufficient", "--grant-expires-at")
    return remaining


def same_model(observed: str, expected: str) -> bool:
    """Mirror of runner/tests/test_personal_claude_live.py::_same_model."""

    base = re.sub(r"\[[^\[\]]*\]$", "", observed)
    return base == expected or re.fullmatch(re.escape(expected) + r"-\d{8}", base) is not None


# --------------------------------------------------------------------------
# Candidate images in the loopback registry
# --------------------------------------------------------------------------

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


@dataclasses.dataclass(frozen=True)
class RegistryImage:
    repository: str
    tag: str
    digest: str
    revision: str


RegistryGet = Callable[[str, Mapping[str, str]], tuple[int, Mapping[str, str], bytes]]


def loopback_get(address: str, port: int, *, timeout: float = 30) -> RegistryGet:
    """Plain HTTP GET against the loopback registry, without any proxy."""

    def get(path: str, headers: Mapping[str, str]) -> tuple[int, Mapping[str, str], bytes]:
        connection = http.client.HTTPConnection(address, port, timeout=timeout)
        try:
            connection.request("GET", path, headers=dict(headers))
            response = connection.getresponse()
            body = response.read(16 << 20)
            return response.status, {k.lower(): v for k, v in response.getheaders()}, body
        finally:
            connection.close()

    return get


def _platform_manifest(
    get: RegistryGet, repository: str, index: dict[str, Any]
) -> dict[str, Any] | None:
    """The one runnable image manifest an index names, read by digest; else None.

    The digest the pod reports and the chart pins stays the index digest. Only
    the revision label is read through the index, from the single entry whose
    platform is a real os and architecture and which is not an attestation.
    """

    entries = [
        entry
        for entry in index.get("manifests") or []
        if isinstance(entry, dict)
        and entry.get("mediaType") in IMAGE_MANIFEST_TYPES
        and (entry.get("annotations") or {}).get("vnd.docker.reference.type")
        != ATTESTATION_REFERENCE
        and (entry.get("platform") or {}).get("os") not in {None, "", "unknown"}
        and (entry.get("platform") or {}).get("architecture") not in {None, "", "unknown"}
    ]
    if len(entries) != 1:
        return None
    child = entries[0].get("digest")
    if not isinstance(child, str) or not _DIGEST.fullmatch(child):
        return None
    status, _headers, body = get(
        f"/v2/{repository}/manifests/{child}", {"Accept": ", ".join(IMAGE_MANIFEST_TYPES)}
    )
    if status != 200 or "sha256:" + hashlib.sha256(body).hexdigest() != child:
        return None
    manifest = json.loads(body)
    return manifest if isinstance(manifest, dict) else None


def registry_image(get: RegistryGet, repository: str, tag: str) -> RegistryImage | None:
    """Manifest then config blob, per the Registry HTTP API v2.

    https://distribution.github.io/distribution/spec/api/ : ``GET
    /v2/<name>/manifests/<reference>`` with an Accept of the image manifest media
    types answers the manifest, whose ``config.digest`` names a blob served by
    ``GET /v2/<name>/blobs/<digest>``; the image config's ``config.Labels``
    carries the OCI revision label. Digests are verified against the bytes.
    """

    try:
        status, headers, body = get(
            f"/v2/{repository}/manifests/{tag}", {"Accept": MANIFEST_ACCEPT}
        )
        if status != 200:
            return None
        manifest = json.loads(body)
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        reported = headers.get("docker-content-digest")
        if reported and reported != digest:
            return None
        if isinstance(manifest, dict) and manifest.get("mediaType") in INDEX_MANIFEST_TYPES:
            manifest = _platform_manifest(get, repository, manifest)
        config = manifest.get("config") if isinstance(manifest, dict) else None
        config_digest = config.get("digest") if isinstance(config, dict) else None
        if not isinstance(config_digest, str) or not _DIGEST.fullmatch(config_digest):
            return None
        status, _headers, blob = get(f"/v2/{repository}/blobs/{config_digest}", {})
        if status != 200 or "sha256:" + hashlib.sha256(blob).hexdigest() != config_digest:
            return None
        image_config = json.loads(blob)
        labels = (image_config.get("config") or {}).get("Labels") or {}
        revision = labels.get(REVISION_LABEL) if isinstance(labels, dict) else None
    except (OSError, ValueError, AttributeError, http.client.HTTPException):
        return None
    return RegistryImage(repository, tag, digest, revision if isinstance(revision, str) else "")


def verify_candidate_images(
    get: RegistryGet, candidate: str, refs: Mapping[str, tuple[str, str]]
) -> dict[str, RegistryImage]:
    """Every required image served with the candidate's exact revision label, else refuse."""

    verified: dict[str, RegistryImage] = {}
    for name in REQUIRED_IMAGES:
        ref = refs.get(name)
        image = registry_image(get, *ref) if ref else None
        if image is None or image.revision != candidate:
            raise Refusal("candidate_image_mismatch", name)
        verified[name] = image
    return verified


def docker_listed_tags(stdout: bytes) -> list[str]:
    """``docker image ls --format '{{.Repository}}:{{.Tag}}'`` lines, untagged rows dropped."""

    return [
        line
        for line in stdout.decode(errors="replace").split()
        if ":" in line and not line.endswith(":<none>")
    ]


def split_image_ref(ref: str) -> tuple[str, str]:
    """``[host:port/]name(:tag|@digest)`` as (registry path, tag or digest)."""

    first, slash, rest = ref.partition("/")
    if slash and (":" in first or "." in first or first == "localhost"):
        ref = rest
    if "@" in ref:
        repository, _, reference = ref.partition("@")
    else:
        repository, _, reference = ref.rpartition(":")
    if not repository or not reference:
        raise Refusal("candidate_image_mismatch", LAYER_IMAGE)
    return repository, reference


def image_tag(candidate: str, run_id: str) -> str:
    """A tag owned by one run: the candidate prefix plus a digest of the run ID."""

    return f"{candidate[:12]}-{hashlib.sha256(run_id.encode()).hexdigest()[:8]}"


def layer_repository(registry: str, bundle_name: str) -> str:
    """Where ``curie build --registry`` pushes a runner layer (cli/src/connector_build.rs)."""

    return f"{registry.rstrip('/')}/{bundle_name}-runner"


def candidate_refs(
    candidate: str, run_id: str, layer_ref: str | None = None
) -> dict[str, tuple[str, str]]:
    """The registry reference of every required image; the layer from the ledger if recorded."""

    tag = image_tag(candidate, run_id)
    refs = {name: (name, tag) for name in PLATFORM_IMAGES}
    refs[LAYER_IMAGE] = split_image_ref(layer_ref) if layer_ref else (LAYER_IMAGE, tag)
    return refs


# --------------------------------------------------------------------------
# Private ledger (SQLite) and budget
# --------------------------------------------------------------------------

LEDGER_FILE = "ledger.sqlite3"
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, candidate TEXT NOT NULL, "
    "started_at REAL NOT NULL, phase TEXT NOT NULL, status TEXT NOT NULL, "
    "budget_state TEXT NOT NULL DEFAULT 'ok', live_started_at REAL, live_ended_at REAL, "
    "live_starts INTEGER NOT NULL DEFAULT 0, grant_expires_at REAL)",
    "CREATE TABLE IF NOT EXISTS sessions (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "attempt INTEGER NOT NULL, role TEXT NOT NULL, reserved_output INTEGER NOT NULL, "
    "reserved_turns INTEGER NOT NULL, reserved_seconds REAL NOT NULL, "
    "started_at REAL NOT NULL, output_tokens INTEGER, assistant_turns INTEGER, "
    "elapsed REAL, model TEXT, source TEXT NOT NULL DEFAULT 'reserved', "
    "PRIMARY KEY (run_id, seq))",
    "CREATE TABLE IF NOT EXISTS observations (run_id TEXT NOT NULL, criterion TEXT NOT NULL, "
    "key TEXT NOT NULL, value_json TEXT NOT NULL, recorded_at REAL NOT NULL, "
    "PRIMARY KEY (run_id, criterion, key))",
    "CREATE TABLE IF NOT EXISTS cleanup (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "resource_kind TEXT NOT NULL, resource_ref_digest TEXT NOT NULL, status TEXT NOT NULL, "
    "detail TEXT NOT NULL, PRIMARY KEY (run_id, seq))",
)


@dataclasses.dataclass(frozen=True)
class SessionUsage:
    output_tokens: int
    assistant_turns: int
    elapsed: float
    model: str


@dataclasses.dataclass(frozen=True)
class Reservation:
    seq: int
    attempt: int
    role: str
    output_tokens: int
    turns: int
    seconds: float


def ref_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


class Ledger:
    """The run's private record. Every write is one ``BEGIN IMMEDIATE`` transaction."""

    def __init__(
        self, db: sqlite3.Connection, run_id: str, scrubber: Scrubber | None = None
    ) -> None:
        self.db = db
        self.run_id = run_id
        self.scrubber = scrubber or Scrubber()

    def __repr__(self) -> str:
        return "<private ledger>"

    @classmethod
    def open(cls, state_dir: Path, run_id: str, *, scrubber: Scrubber | None = None) -> Ledger:
        require_private_dir(state_dir)
        path = state_dir / LEDGER_FILE
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise Refusal("ledger_unsafe", LEDGER_FILE) from None
        try:
            info = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise Refusal("ledger_unsafe", LEDGER_FILE)
        db = sqlite3.connect(path, timeout=10, isolation_level=None)
        db.execute("PRAGMA journal_mode=DELETE")
        ledger = cls(db, run_id, scrubber)
        with ledger.transaction():
            for statement in _SCHEMA:
                db.execute(statement)
        return ledger

    @classmethod
    def open_existing(cls, state_dir: Path, run_id: str) -> Ledger:
        require_private_dir(state_dir)
        path = state_dir / LEDGER_FILE
        try:
            info = os.lstat(path)
        except OSError:
            raise Refusal("ledger_missing", LEDGER_FILE) from None
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise Refusal("ledger_unsafe", LEDGER_FILE)
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10, isolation_level=None)
        ledger = cls(db, run_id)
        try:
            row = db.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone()
        except sqlite3.Error:
            row = None
        if row is None:
            db.close()
            raise Refusal("ledger_missing", "--run-id")
        return ledger

    def close(self) -> None:
        self.db.close()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    # -- run row --------------------------------------------------------

    def ensure_run(self, candidate: str, *, phase: str, now: float | None = None) -> None:
        with self.transaction() as db:
            row = db.execute("SELECT candidate FROM runs WHERE run_id=?", (self.run_id,)).fetchone()
            if row is None:
                db.execute(
                    "INSERT INTO runs(run_id,candidate,started_at,phase,status) VALUES(?,?,?,?,?)",
                    (self.run_id, candidate, time.time() if now is None else now, phase, "open"),
                )
            elif row[0] != candidate:
                raise Refusal("candidate_mismatch", "--candidate")
            else:
                db.execute("UPDATE runs SET phase=? WHERE run_id=?", (phase, self.run_id))

    def run_row(self) -> dict[str, Any]:
        cursor = self.db.execute("SELECT * FROM runs WHERE run_id=?", (self.run_id,))
        row = cursor.fetchone()
        if row is None:
            raise Refusal("ledger_missing", "--run-id")
        return dict(zip([column[0] for column in cursor.description], row, strict=True))

    def candidate(self) -> str:
        return str(self.run_row()["candidate"])

    def set_phase(self, phase: str, status: str | None = None) -> None:
        with self.transaction() as db:
            if status is None:
                db.execute("UPDATE runs SET phase=? WHERE run_id=?", (phase, self.run_id))
            else:
                db.execute(
                    "UPDATE runs SET phase=?, status=? WHERE run_id=?",
                    (phase, status, self.run_id),
                )

    def has_live_start(self) -> bool:
        row = self.run_row()
        sessions = self.db.execute(
            "SELECT COUNT(*) FROM sessions WHERE run_id=?", (self.run_id,)
        ).fetchone()[0]
        return bool(row["live_starts"]) or row["live_started_at"] is not None or sessions > 0

    def refuse_paid_retry(self) -> None:
        """A run ID gets exactly one live start; there is no retry path."""

        if self.has_live_start():
            self.record(
                "budget", "retry_refusals", self.observation_int("budget", "retry_refusals") + 1
            )
            raise Refusal("paid_retry_refused", "--run-id")

    def _rehearsal(self, db: sqlite3.Connection) -> bool:
        row = db.execute(
            "SELECT value_json FROM observations WHERE run_id=? AND criterion='rehearsal' "
            "AND key='marker'",
            (self.run_id,),
        ).fetchone()
        return row is not None and json.loads(row[0]) is True

    def is_rehearsal(self) -> bool:
        return self._rehearsal(self.db)

    def mark_rehearsal(self) -> None:
        """Mark this run ID as an unpaid rehearsal before anything else happens to it."""

        if self.has_live_start():
            raise Refusal("arguments_invalid", "--run-id already has a live start")
        self.record("rehearsal", "marker", True)

    def begin_live(self, *, now: float, grant_expires_at: float) -> None:
        with self.transaction() as db:
            if self._rehearsal(db):
                raise Refusal("rehearsal_run_id", "--run-id")
            row = db.execute(
                "SELECT live_starts, live_started_at FROM runs WHERE run_id=?", (self.run_id,)
            ).fetchone()
            if row is None:
                raise Refusal("ledger_missing", "--run-id")
            if row[0] or row[1] is not None:
                raise Refusal("paid_retry_refused", "--run-id")
            db.execute(
                "UPDATE runs SET live_started_at=?, live_starts=live_starts+1, "
                "grant_expires_at=?, status='live' WHERE run_id=?",
                (now, grant_expires_at, self.run_id),
            )

    def end_live(self, *, now: float) -> None:
        with self.transaction() as db:
            db.execute(
                "UPDATE runs SET live_ended_at=? WHERE run_id=? AND live_ended_at IS NULL",
                (now, self.run_id),
            )

    # -- budget ---------------------------------------------------------

    def _sessions(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT * FROM sessions WHERE run_id=? ORDER BY seq", (self.run_id,)
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def sessions(self) -> list[dict[str, Any]]:
        return self._sessions()

    def _mark_exceeded(self, db: sqlite3.Connection) -> None:
        db.execute("UPDATE runs SET budget_state='exceeded' WHERE run_id=?", (self.run_id,))

    def reserve(self, roles: Sequence[str], *, attempt: int, now: float) -> list[Reservation]:
        """Persist reservations for sessions about to start; refuse past any allowance.

        Each session reserves an equal share of what remains, capped at its
        per-session bound. Unknown usage, an exceeded bound, a missing live
        start, the live window or the grant lifetime refuse the whole batch.
        """

        if not roles:
            raise Refusal("arguments_invalid", "roles")
        with self.transaction() as db:
            row = db.execute(
                "SELECT budget_state, live_started_at, grant_expires_at FROM runs WHERE run_id=?",
                (self.run_id,),
            ).fetchone()
            if row is None:
                raise Refusal("ledger_missing", "--run-id")
            budget_state, live_started_at, grant_expires_at = row
            if self._rehearsal(db):
                raise Refusal("rehearsal_run_id", "--run-id")
            if budget_state != "ok":
                raise Refusal("budget_exceeded")
            if live_started_at is None or grant_expires_at is None:
                raise Refusal("allowance_exhausted", "live start not recorded")
            sessions = self._sessions()
            if any(
                item["output_tokens"] is None
                or item["assistant_turns"] is None
                or item["elapsed"] is None
                for item in sessions
            ):
                raise Refusal("usage_unknown")
            count = len(roles)
            if len(sessions) + count > MAX_SESSIONS:
                raise Refusal("allowance_exhausted", "sessions")
            used_turns = sum(int(item["assistant_turns"]) for item in sessions)
            used_tokens = sum(int(item["output_tokens"]) for item in sessions)
            turns = min(SESSION_TURNS, (MAX_TURNS - used_turns) // count)
            tokens = min(SESSION_OUTPUT_TOKENS, (MAX_OUTPUT_TOKENS - used_tokens) // count)
            if turns < 1:
                raise Refusal("allowance_exhausted", "turns")
            if tokens < 1:
                raise Refusal("allowance_exhausted", "output tokens")
            live_remaining = LIVE_SECONDS - (now - float(live_started_at))
            if live_remaining < SESSION_SECONDS:
                raise Refusal("allowance_exhausted", "live seconds")
            if float(grant_expires_at) - now < live_remaining:
                raise Refusal("lifetime_insufficient", "remaining live window")
            seconds = float(SESSION_SECONDS)
            base = max((int(item["seq"]) for item in sessions), default=0)
            reservations = []
            for offset, role in enumerate(roles, start=1):
                reservation = Reservation(base + offset, attempt, role, tokens, turns, seconds)
                db.execute(
                    "INSERT INTO sessions(run_id,seq,attempt,role,reserved_output,"
                    "reserved_turns,reserved_seconds,started_at) VALUES(?,?,?,?,?,?,?,?)",
                    (self.run_id, reservation.seq, attempt, role, tokens, turns, seconds, now),
                )
                reservations.append(reservation)
            return reservations

    def settle(self, seq: int, usage: SessionUsage | None, *, source: str) -> None:
        """Record observed usage. ``None`` leaves the session unknown, which blocks starts."""

        with self.transaction() as db:
            if usage is None:
                db.execute(
                    "UPDATE sessions SET source=? WHERE run_id=? AND seq=?",
                    ("unknown", self.run_id, seq),
                )
                return
            db.execute(
                "UPDATE sessions SET output_tokens=?, assistant_turns=?, elapsed=?, model=?, "
                "source=? WHERE run_id=? AND seq=?",
                (
                    usage.output_tokens,
                    usage.assistant_turns,
                    usage.elapsed,
                    usage.model,
                    source,
                    self.run_id,
                    seq,
                ),
            )
            self._check_bounds(db)

    def record_unreserved(self, attempt: int, role: str, usage: SessionUsage | None) -> int:
        """An observed session with no reservation: counted, and the budget is exceeded."""

        with self.transaction() as db:
            seq = 1 + max((int(item["seq"]) for item in self._sessions()), default=0)
            db.execute(
                "INSERT INTO sessions(run_id,seq,attempt,role,reserved_output,reserved_turns,"
                "reserved_seconds,started_at,output_tokens,assistant_turns,elapsed,model,source) "
                "VALUES(?,?,?,?,0,0,0,?,?,?,?,?,'unreserved')",
                (
                    self.run_id,
                    seq,
                    attempt,
                    role,
                    time.time(),
                    None if usage is None else usage.output_tokens,
                    None if usage is None else usage.assistant_turns,
                    None if usage is None else usage.elapsed,
                    None if usage is None else usage.model,
                ),
            )
            self._mark_exceeded(db)
            return seq

    def _check_bounds(self, db: sqlite3.Connection) -> None:
        sessions = self._sessions()
        settled = [item for item in sessions if item["output_tokens"] is not None]
        if (
            any(
                int(item["output_tokens"]) > SESSION_OUTPUT_TOKENS
                or int(item["assistant_turns"]) > SESSION_TURNS
                or float(item["elapsed"]) > SESSION_SECONDS
                for item in settled
            )
            or sum(int(item["output_tokens"]) for item in settled) > MAX_OUTPUT_TOKENS
            or sum(int(item["assistant_turns"]) for item in settled) > MAX_TURNS
            or len(sessions) > MAX_SESSIONS
        ):
            self._mark_exceeded(db)

    def budget_state(self) -> str:
        return str(self.run_row()["budget_state"])

    # -- observations and cleanup ----------------------------------------

    def record(self, criterion: str, key: str, value: Any) -> None:
        text = self.scrubber.scrub(json.dumps(value, sort_keys=True))
        with self.transaction() as db:
            db.execute(
                "INSERT INTO observations(run_id,criterion,key,value_json,recorded_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(run_id,criterion,key) DO UPDATE SET "
                "value_json=excluded.value_json, recorded_at=excluded.recorded_at",
                (self.run_id, criterion, key, text, time.time()),
            )

    def observations(self, criterion: str) -> dict[str, Any]:
        rows = self.db.execute(
            "SELECT key, value_json FROM observations WHERE run_id=? AND criterion=?",
            (self.run_id, criterion),
        ).fetchall()
        return {key: json.loads(value) for key, value in rows}

    def observation_int(self, criterion: str, key: str) -> int:
        value = self.observations(criterion).get(key)
        return value if type(value) is int else 0

    def record_cleanup(self, kind: str, ref: str, status: str, detail: str = "") -> None:
        with self.transaction() as db:
            seq = 1 + int(
                db.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM cleanup WHERE run_id=?", (self.run_id,)
                ).fetchone()[0]
            )
            db.execute(
                "INSERT INTO cleanup(run_id,seq,resource_kind,resource_ref_digest,status,detail) "
                "VALUES(?,?,?,?,?,?)",
                (self.run_id, seq, kind, ref_digest(ref), status, self.scrubber.scrub(detail)),
            )

    def cleanup_rows(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT resource_kind, status, detail FROM cleanup WHERE run_id=? ORDER BY seq",
            (self.run_id,),
        )
        return [
            {"kind": kind, "status": status, "detail": detail}
            for kind, status, detail in cursor.fetchall()
        ]


# --------------------------------------------------------------------------
# Native transcript parsing
# --------------------------------------------------------------------------
#
# Shapes follow the native Claude Code CLI transcript records observed in the
# #5531/#5422 live proofs (runner/tests/personal_token_cluster_support.py::
# native_transcript_records) and the SDK's subagent listing
# (claude_agent_sdk/_internal/sessions.py, list_subagents and
# _read_agent_metadata_sidecar): main sessions are
# ``<config>/projects/<slug>/<session>.jsonl``; subagents are
# ``<config>/projects/<slug>/<session>/subagents/agent-<id>.jsonl`` with an
# ``agent-<id>.meta.json`` sidecar carrying ``toolUseId`` (and possibly
# ``agentType``).

SYNTHETIC_MODEL = "<synthetic>"
ACI_TOOL_PREFIX = "mcp__curie__"
AGENT_TOOLS = frozenset({"Agent", "Task"})


@dataclasses.dataclass
class SessionFacts:
    path: str
    role: str
    parent: str
    models: tuple[str, ...]
    assistant_turns: int
    output_tokens: int
    synthetic: int
    first: float | None
    last: float | None
    elapsed: float
    aci_tool_calls: int
    usage_known: bool
    message_ids: frozenset[str] = frozenset()

    def usage(self) -> SessionUsage | None:
        if not self.usage_known:
            return None
        return SessionUsage(
            self.output_tokens, self.assistant_turns, round(self.elapsed, 3), ",".join(self.models)
        )


@dataclasses.dataclass
class TranscriptFacts:
    sessions: list[SessionFacts]
    malformed_lines: int
    synthetic_records: int
    aci_tool_calls: int
    replayed_records: int = 0

    def message_ids(self) -> frozenset[str]:
        return frozenset(item for session in self.sessions for item in session.message_ids)

    def by_role(self, role: str) -> list[SessionFacts]:
        return [session for session in self.sessions if session.role == role]


def _timestamp(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.timestamp()


def _records(data: bytes) -> tuple[list[dict[str, Any]], int]:
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    elif lines:
        # A trailing line without a newline may still be being written.
        lines.pop()
    records: list[dict[str, Any]] = []
    malformed = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except ValueError:
            malformed += 1
            continue
        if isinstance(value, dict):
            records.append(value)
        else:
            malformed += 1
    return records, malformed


def _role_of(subagent_type: Any) -> str | None:
    if not isinstance(subagent_type, str):
        return None
    tail = subagent_type.rsplit(":", 1)[-1].strip()
    return tail if tail in REVIEWER_ROLES else None


def _content(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    message = record.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    return [item for item in content if isinstance(item, dict)] if isinstance(content, list) else []


def _union_within(intervals: Iterable[tuple[float, float]], low: float, high: float) -> float:
    clipped = sorted(
        (max(low, start), min(high, end)) for start, end in intervals if end > low and start < high
    )
    total = 0.0
    current: tuple[float, float] | None = None
    for start, end in clipped:
        if current is None or start > current[1]:
            if current is not None:
                total += current[1] - current[0]
            current = (start, end)
        else:
            current = (current[0], max(current[1], end))
    if current is not None:
        total += current[1] - current[0]
    return total


def _session_facts(
    path: str, role: str, parent: str, records: Sequence[Mapping[str, Any]]
) -> SessionFacts:
    by_id: dict[str, int | None] = {}
    models: set[str] = set()
    synthetic = 0
    aci = 0
    stamps: list[float] = []
    for record in records:
        stamp = _timestamp(record.get("timestamp"))
        if stamp is not None:
            stamps.append(stamp)
        if record.get("type") != "assistant":
            continue
        message = record.get("message") if isinstance(record.get("message"), dict) else {}
        assert isinstance(message, dict)
        model = message.get("model")
        if model == SYNTHETIC_MODEL:
            synthetic += 1
            continue
        if isinstance(model, str):
            models.add(model)
        message_id = message.get("id")
        if not isinstance(message_id, str):
            message_id = f"<no id {len(by_id)}>"
            by_id[message_id] = None
        usage = message.get("usage")
        tokens = usage.get("output_tokens") if isinstance(usage, dict) else None
        previous = by_id.get(message_id)
        if type(tokens) is int and tokens >= 0:
            by_id[message_id] = tokens if previous is None else max(previous, tokens)
        else:
            by_id.setdefault(message_id, None)
        aci += sum(
            1
            for item in _content(record)
            if item.get("type") == "tool_use"
            and str(item.get("name") or "").startswith(ACI_TOOL_PREFIX)
        )
    first = min(stamps) if stamps else None
    last = max(stamps) if stamps else None
    return SessionFacts(
        path=path,
        role=role,
        parent=parent,
        models=tuple(sorted(models)),
        assistant_turns=len(by_id),
        output_tokens=sum(value or 0 for value in by_id.values()),
        synthetic=synthetic,
        first=first,
        last=last,
        elapsed=(last - first) if first is not None and last is not None else 0.0,
        aci_tool_calls=aci,
        # A session with no assistant usage at all (only user records) is not known.
        usage_known=bool(by_id) and all(value is not None for value in by_id.values()),
        message_ids=frozenset(key for key in by_id if not key.startswith("<no id")),
    )


def _replayed(
    record: Mapping[str, Any], exclude_ids: frozenset[str], not_before: float | None
) -> bool:
    """A record carried over from an earlier attempt's history, not this attempt's work."""

    stamp = _timestamp(record.get("timestamp"))
    if not_before is not None and stamp is not None and stamp < not_before:
        return True
    message = record.get("message")
    message_id = message.get("id") if isinstance(message, dict) else None
    return record.get("type") == "assistant" and message_id in exclude_ids


def parse_native_transcripts(
    files: Mapping[str, bytes],
    *,
    exclude_ids: frozenset[str] = frozenset(),
    not_before: float | None = None,
) -> TranscriptFacts:
    """Sessions, roles, models, usage and synthetic records from harvested files.

    A fresh attempt replays the conversation history into a new native
    session. Records older than ``not_before`` or repeating an assistant
    message id in ``exclude_ids`` are that replay, so they are not counted
    again; links between tool uses and subagents still read every record.
    """

    mains: dict[str, list[dict[str, Any]]] = {}
    subagents: dict[str, list[dict[str, Any]]] = {}
    sidecars: dict[str, dict[str, Any]] = {}
    malformed = 0
    for name, data in files.items():
        parts = PurePosixPath(name).parts
        if "projects" not in parts:
            continue
        if name.endswith(".meta.json"):
            try:
                value = json.loads(data)
            except ValueError:
                malformed += 1
                continue
            if isinstance(value, dict):
                sidecars[name.removesuffix(".meta.json")] = value
            continue
        if not name.endswith(".jsonl"):
            continue
        records, bad = _records(data)
        malformed += bad
        if "subagents" in parts:
            subagents[name.removesuffix(".jsonl")] = records
        else:
            mains[name.removesuffix(".jsonl")] = records

    tool_roles: dict[str, str] = {}
    agent_roles: dict[str, str] = {}
    for records in mains.values():
        for record in records:
            for item in _content(record):
                if item.get("type") == "tool_use" and item.get("name") in AGENT_TOOLS:
                    tool_input = item.get("input")
                    role = _role_of(
                        tool_input.get("subagent_type") if isinstance(tool_input, dict) else None
                    )
                    if role and isinstance(item.get("id"), str):
                        tool_roles[item["id"]] = role
        for record in records:
            result = record.get("toolUseResult")
            agent_id = result.get("agentId") if isinstance(result, dict) else None
            if not isinstance(agent_id, str):
                continue
            for item in _content(record):
                if item.get("type") == "tool_result" and item.get("tool_use_id") in tool_roles:
                    agent_roles[agent_id] = tool_roles[item["tool_use_id"]]

    replayed = 0

    def current(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        nonlocal replayed
        kept = [record for record in records if not _replayed(record, exclude_ids, not_before)]
        replayed += len(records) - len(kept)
        return kept

    sessions: list[SessionFacts] = []
    for stem, records in sorted(mains.items()):
        session_id = PurePosixPath(stem).name
        main_records = current([record for record in records if not record.get("isSidechain")])
        if not main_records:
            continue
        sessions.append(_session_facts(stem, IMPLEMENTER, session_id, main_records))
    for stem, records in sorted(subagents.items()):
        parts = PurePosixPath(stem).parts
        parent = parts[parts.index("subagents") - 1] if parts.index("subagents") > 0 else ""
        sidecar = sidecars.get(stem, {})
        agent_id = PurePosixPath(stem).name.removeprefix("agent-")
        role = (
            _role_of(sidecar.get("agentType"))
            or tool_roles.get(str(sidecar.get("toolUseId") or ""))
            or agent_roles.get(agent_id)
            or next(
                (
                    agent_roles[str(record.get("agentId"))]
                    for record in records
                    if str(record.get("agentId")) in agent_roles
                ),
                None,
            )
            or UNATTRIBUTED
        )
        kept = current(records)
        if kept:
            sessions.append(_session_facts(stem, role, parent, kept))

    for session in sessions:
        if session.role != IMPLEMENTER or session.first is None or session.last is None:
            continue
        nested = [
            (other.first, other.last)
            for other in sessions
            if other.role != IMPLEMENTER
            and other.parent == session.parent
            and other.first is not None
            and other.last is not None
        ]
        session.elapsed = (session.last - session.first) - _union_within(
            nested, session.first, session.last
        )
    return TranscriptFacts(
        sessions=sessions,
        malformed_lines=malformed,
        synthetic_records=sum(session.synthetic for session in sessions),
        aci_tool_calls=sum(session.aci_tool_calls for session in sessions),
        replayed_records=replayed,
    )


def session_summary(session: SessionFacts) -> dict[str, Any]:
    """The sanitized per-session fact the ledger keeps (no raw transcript, no path)."""

    return {
        "role": session.role,
        "models": list(session.models),
        "assistant_turns": session.assistant_turns,
        "output_tokens": session.output_tokens,
        "synthetic": session.synthetic,
        "elapsed": round(session.elapsed, 3),
        "usage_known": session.usage_known,
    }


def allowance_breaches(
    reservations: Sequence[Reservation], facts: TranscriptFacts | None, *, now: float
) -> list[str]:
    """Bounds an observed session has reached, matched to reservations like settlement. Pure.

    The most recently active session is charged the time since its last record,
    since it may still be running. A session beyond the reservations is a breach.
    """

    if facts is None:
        return []
    breaches: list[str] = []
    pending = list(reservations)
    latest = max((s.last for s in facts.sessions if s.last is not None), default=None)
    for session in facts.sessions:
        slot = next((item for item in pending if item.role == session.role), None)
        if slot is None:
            breaches.append(f"unreserved:{session.role}")
            continue
        pending.remove(slot)
        elapsed = session.elapsed
        if latest is not None and session.last == latest:
            elapsed += max(0.0, now - latest)
        if session.output_tokens >= slot.output_tokens:
            breaches.append(f"output_tokens:{session.role}")
        if session.assistant_turns >= slot.turns:
            breaches.append(f"turns:{session.role}")
        if elapsed >= slot.seconds:
            breaches.append(f"seconds:{session.role}")
    return breaches


def settle_attempt(
    ledger: Ledger,
    reservations: Sequence[Reservation],
    facts: TranscriptFacts | None,
    *,
    complete: bool,
) -> dict[str, int]:
    """Match observed sessions to this attempt's reservations and settle every one.

    An incomplete harvest (a gap, an unreadable line, or a stream that never
    ended) leaves every reserved slot unknown, because partial transcripts can
    undercount. Otherwise a slot with no observed session settles to zero and
    an observed session beyond the reservations is recorded as unreserved.
    """

    admitted_without_implementer = (
        facts is not None
        and any(item.role == IMPLEMENTER for item in reservations)
        and not any(session.role == IMPLEMENTER for session in facts.sessions)
    )
    if not complete or facts is None or facts.malformed_lines or admitted_without_implementer:
        # Admitted work always runs an implementer; no implementer record means
        # the capture missed it, so nothing here may settle to zero.
        for reserved in reservations:
            ledger.settle(reserved.seq, None, source="unknown")
        return {"settled": 0, "unknown": len(reservations), "unreserved": 0}
    pending = list(reservations)
    settled = unknown = unreserved = 0
    observed = list(facts.sessions)
    for session in observed:
        slot = next((item for item in pending if item.role == session.role), None)
        if slot is None:
            ledger.record_unreserved(
                reservations[0].attempt if reservations else 0, session.role, session.usage()
            )
            unreserved += 1
            continue
        pending.remove(slot)
        usage = session.usage()
        ledger.settle(slot.seq, usage, source="native")
        settled += usage is not None
        unknown += usage is None
    for unused in pending:
        ledger.settle(unused.seq, SessionUsage(0, 0, 0.0, ""), source="not_started")
        settled += 1
    return {"settled": settled, "unknown": unknown, "unreserved": unreserved}


# --------------------------------------------------------------------------
# In-pod harvester and its stream consumer
# --------------------------------------------------------------------------

# Run as ``python3 -c HARVEST_PROGRAM <config json>`` inside the runner
# container. It reads only transcript files, the review gate state files, the
# workspace through git, and /proc/<pid>/cmdline and /proc/<pid>/stat. It never
# reads environment variables, /proc/*/environ or any credential file, and the
# git children get a fresh minimal environment.
HARVEST_PROGRAM = r"""
import base64, glob, hashlib, json, os, subprocess, sys, time
cfg = json.loads(sys.argv[1])
offsets = {}
metas = {}
reviews = {}
state = {"workspace": None, "process": None}
CHUNK = 262144
def emit(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")
    sys.stdout.flush()
def walk():
    root = cfg["tmp_root"]
    for config in sorted(glob.glob(os.path.join(root, "curie-claude-config-*"))):
        projects = os.path.join(config, "projects")
        for dirpath, dirnames, names in os.walk(projects, followlinks=False):
            dirnames.sort()
            for name in sorted(names):
                if name.endswith(".jsonl") or name.endswith(".meta.json"):
                    full = os.path.join(dirpath, name)
                    if not os.path.islink(full) and os.path.isfile(full):
                        yield os.path.relpath(full, root), full
def scan_files():
    for rel, full in walk():
        try:
            size = os.path.getsize(full)
            if rel.endswith(".meta.json"):
                with open(full, "rb") as handle:
                    data = handle.read(65536)
                digest = hashlib.sha256(data).hexdigest()
                if metas.get(rel) != digest:
                    metas[rel] = digest
                    emit({"kind": "meta", "path": rel, "data": base64.b64encode(data).decode()})
                continue
            start = offsets.get(rel, 0)
            if size < start:
                emit({"kind": "truncated", "path": rel})
                start = 0
            with open(full, "rb") as handle:
                handle.seek(start)
                while True:
                    data = handle.read(CHUNK)
                    if not data:
                        break
                    emit({"kind": "file", "path": rel, "offset": start,
                          "data": base64.b64encode(data).decode()})
                    start += len(data)
            offsets[rel] = start
        except OSError:
            continue
def scan_reviews():
    folder = cfg["review_dir"]
    if not os.path.isdir(folder):
        return
    for name in sorted(os.listdir(folder)):
        full = os.path.join(folder, name)
        if not name.endswith(".json") or os.path.islink(full) or not os.path.isfile(full):
            continue
        try:
            with open(full, "rb") as handle:
                data = handle.read(262144)
        except OSError:
            continue
        digest = hashlib.sha256(data).hexdigest()
        if reviews.get(name) != digest:
            reviews[name] = digest
            emit({"kind": "review", "name": name, "data": base64.b64encode(data).decode()})
def git(*args):
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": cfg["tmp_root"],
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C",
           "GIT_OPTIONAL_LOCKS": "0"}
    ws = cfg["workspace"]
    result = subprocess.run([cfg["git"], "-c", "safe.directory=" + ws, "-C", ws] + list(args),
                            capture_output=True, timeout=30, env=env)
    return result.returncode, result.stdout
def workspace():
    ws = cfg["workspace"]
    if not os.path.isdir(ws):
        return {"kind": "workspace", "ok": False}
    try:
        code, head = git("rev-parse", "HEAD")
        code2, status = git("status", "--porcelain=v1", "-z", "--untracked-files=all")
    except (OSError, subprocess.SubprocessError):
        return {"kind": "workspace", "ok": False}
    if code or code2:
        return {"kind": "workspace", "ok": False}
    fields = status.split(b"\0")
    entries = []
    index = 0
    while index < len(fields):
        item = fields[index]
        index += 1
        if len(item) < 4:
            continue
        code_xy, path = item[:2].decode(), item[3:].decode("utf-8", "replace")
        if code_xy[0] in "RC":
            index += 1
        entries.append([code_xy, path])
    hashes = {}
    nonce = cfg.get("nonce", "").encode()
    nonce_paths = []
    for code_xy, path in entries:
        full = os.path.join(ws, path)
        if os.path.isfile(full) and not os.path.islink(full):
            with open(full, "rb") as handle:
                data = handle.read()
            hashes[path] = hashlib.sha256(data).hexdigest()
            if nonce and nonce in data:
                nonce_paths.append(path)
        else:
            hashes[path] = None
    return {"kind": "workspace", "ok": True, "head": head.decode().strip(),
            "status": sorted(entries), "hashes": hashes, "nonce_paths": sorted(nonce_paths)}
def is_claude(argv):
    for arg in argv[:2]:
        text = arg.decode("utf-8", "replace")
        base = os.path.basename(text)
        if base == "claude" or ("claude-code" in text and text.endswith(".js")):
            return True
    return False
def processes():
    proc = cfg["proc_root"]
    btime = 0.0
    try:
        with open(os.path.join(proc, "stat"), "rb") as handle:
            for line in handle:
                if line.startswith(b"btime "):
                    btime = float(line.split()[1])
    except OSError:
        pass
    ticks = os.sysconf("SC_CLK_TCK")
    found = []
    for pid in sorted(os.listdir(proc)):
        if not pid.isdigit():
            continue
        try:
            with open(os.path.join(proc, pid, "cmdline"), "rb") as handle:
                argv = handle.read().split(b"\0")
            if not is_claude(argv):
                continue
            with open(os.path.join(proc, pid, "stat"), "rb") as handle:
                fields = handle.read().rsplit(b")", 1)[1].split()
            found.append({"pid": int(pid), "start": round(btime + int(fields[19]) / ticks, 2)})
        except (OSError, IndexError, ValueError):
            continue
    return {"kind": "process", "count": len(found), "processes": found}
deadline = time.monotonic() + float(cfg["max_seconds"])
while True:
    scan_files()
    emit({"kind": "sync", "sizes": dict(offsets), "at": round(time.time(), 3)})
    scan_reviews()
    record = workspace()
    if record != state["workspace"]:
        state["workspace"] = record
        emit(record)
    record = processes()
    if record != state["process"]:
        state["process"] = record
        emit(record)
    if cfg.get("once") or time.monotonic() >= deadline:
        break
    time.sleep(float(cfg["interval"]))
emit({"kind": "end"})
"""


def harvest_config(
    *,
    nonce: str,
    tmp_root: str = "/tmp",
    review_dir: str = "/tmp/dark-factory-review",
    workspace: str = "/workspace",
    proc_root: str = "/proc",
    git: str = "git",
    interval: float = 0.5,
    max_seconds: float = LIVE_SECONDS,
    once: bool = False,
) -> str:
    return json.dumps(
        {
            "tmp_root": tmp_root,
            "review_dir": review_dir,
            "workspace": workspace,
            "proc_root": proc_root,
            "git": git,
            "interval": interval,
            "max_seconds": max_seconds,
            "once": once,
            "nonce": nonce,
        },
        sort_keys=True,
    )


class HarvestState:
    """Reassembles the harvester stream in memory and scans every byte for held secrets."""

    def __init__(self, scan: TokenScan, *, channel: str = "harvest") -> None:
        self.scan = scan
        self.channel = channel
        self.files: dict[str, bytearray] = {}
        self.metas: dict[str, bytes] = {}
        self.reviews: dict[str, bytes] = {}
        self.workspace: dict[str, Any] | None = None
        self.processes: list[dict[str, Any]] = []
        self.process_max = 0
        self.process_records = 0
        self.gaps = 0
        self.bad_lines = 0
        self.ended = False
        self.synced: dict[str, int] | None = None
        self.synced_at: float | None = None

    def __repr__(self) -> str:
        return "<harvest state>"

    def _decoded(self, value: Any, *, stream: str | None = None) -> bytes | None:
        if not isinstance(value, str):
            return None
        try:
            data = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error):
            return None
        if stream is None:
            self.scan.scan(self.channel, data)
        else:
            self.scan.scan_stream(self.channel, stream, data)
        return data

    def feed(self, line: bytes) -> None:
        self.scan.scan(self.channel, line)
        if not line.strip():
            return
        try:
            record = json.loads(line)
        except ValueError:
            self.bad_lines += 1
            return
        if not isinstance(record, dict):
            self.bad_lines += 1
            return
        kind = record.get("kind")
        if kind == "file":
            path = record.get("path")
            data = self._decoded(record.get("data"), stream=path if isinstance(path, str) else None)
            offset = record.get("offset")
            if data is None or not isinstance(path, str) or type(offset) is not int:
                self.bad_lines += 1
                return
            buffer = self.files.get(path)
            if offset != (0 if buffer is None else len(buffer)):
                self.gaps += 1
                return
            self.files.setdefault(path, bytearray()).extend(data)
        elif kind == "meta":
            data = self._decoded(record.get("data"))
            if data is None or not isinstance(record.get("path"), str):
                self.bad_lines += 1
                return
            self.metas[record["path"]] = data
        elif kind == "truncated":
            self.gaps += 1
        elif kind == "review":
            data = self._decoded(record.get("data"))
            if data is None or not isinstance(record.get("name"), str):
                self.bad_lines += 1
                return
            self.reviews[record["name"]] = data
        elif kind == "workspace":
            if record.get("ok"):
                self.workspace = record
        elif kind == "process":
            count = record.get("count")
            processes = record.get("processes")
            if type(count) is int and isinstance(processes, list):
                self.process_records += 1
                self.process_max = max(self.process_max, count)
                if processes:
                    self.processes = [item for item in processes if isinstance(item, dict)]
        elif kind == "sync":
            sizes = record.get("sizes")
            at = record.get("at")
            if (
                not isinstance(sizes, dict)
                or not all(isinstance(k, str) and type(v) is int for k, v in sizes.items())
                or not isinstance(at, (int, float))
                or isinstance(at, bool)
            ):
                self.bad_lines += 1
                return
            self.synced = dict(sizes)
            self.synced_at = float(at)
        elif kind == "end":
            self.ended = True
        else:
            self.bad_lines += 1

    @property
    def complete(self) -> bool:
        """Affirmative capture: the harvester's last size report matches every reassembled file.

        A stream that never reported (a failed exec, an empty EOF) or that
        delivered bytes after its last report is not complete.
        """

        if self.gaps or self.bad_lines or self.synced is None:
            return False
        held = {path: len(data) for path, data in self.files.items()}
        reported = {path: size for path, size in self.synced.items() if size}
        return held == reported

    def terminal_after(self, moment: float) -> bool:
        """Complete, and the matching size report was taken at or after ``moment``.

        ``moment`` is when native execution had ended (the approval's creation,
        which follows the turn). Every transcript byte is written before it, so
        a matching report taken later proves nothing was missed.
        """

        return self.complete and self.synced_at is not None and self.synced_at >= moment

    def transcript_files(self) -> dict[str, bytes]:
        merged = {path: bytes(data) for path, data in self.files.items()}
        merged.update(self.metas)
        return merged

    def review_verdicts(self) -> dict[str, Any]:
        """Bundle hook evidence from examples/dark-factory/hooks/review_gate.py state files."""

        plan: list[str] = []
        diff: list[str] = []
        rounds = 0
        files = 0
        for data in self.reviews.values():
            try:
                state = json.loads(data)
            except ValueError:
                continue
            if not isinstance(state, dict):
                continue
            files += 1
            for name, bucket in (("plan", plan), ("diff", diff)):
                entry = state.get(name)
                if isinstance(entry, dict):
                    verdict = entry.get("verdict")
                    if verdict in {"APPROVE", "CHANGES"}:
                        bucket.append(verdict)
                    if type(entry.get("round")) is int:
                        rounds += entry["round"]
        return {
            "state_files": files,
            "plan_verdicts": sorted(plan),
            "diff_verdicts": sorted(diff),
            "rounds": rounds,
            "hook_observed": files > 0 and (rounds > 0 or bool(plan or diff)),
            "diff_approved": "APPROVE" in diff,
        }

    def process_start(self) -> float | None:
        starts = [
            float(item["start"])
            for item in self.processes
            if isinstance(item.get("start"), (int, float))
        ]
        return min(starts) if starts else None


def workspace_record(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The comparable workspace record: HEAD, status, hashes and the nonce paths."""

    if not record or not record.get("ok"):
        return None
    return {
        "head": record.get("head"),
        "status": record.get("status"),
        "hashes": record.get("hashes"),
        "nonce_paths": record.get("nonce_paths"),
    }


def published_matches(
    reviewed: Mapping[str, Any] | None, published: Mapping[str, Any] | None
) -> dict[str, bool]:
    """Compare the reviewed workspace record with the published head's blobs."""

    if not reviewed or not published:
        return {
            "hashes_equal": False,
            "nonce_present": False,
            "deleted_absent": False,
            "head_recorded": False,
            "paths_equal": False,
        }
    hashes = reviewed.get("hashes") or {}
    blobs = published.get("hashes") or {}
    changed = {path: digest for path, digest in hashes.items() if digest is not None}
    deleted = [path for path, digest in hashes.items() if digest is None]
    return {
        "hashes_equal": bool(changed) and all(blobs.get(p) == d for p, d in changed.items()),
        "nonce_present": bool(reviewed.get("nonce_paths"))
        and bool(published.get("nonce_paths"))
        and set(reviewed.get("nonce_paths") or []) <= set(published.get("nonce_paths") or []),
        "deleted_absent": all(blobs.get(path) is None for path in deleted),
        "head_recorded": bool(re.fullmatch(r"[0-9a-f]{40}", str(reviewed.get("head") or "")))
        and isinstance(reviewed.get("status"), list),
        # The pull request's complete file list, read independently of the
        # reviewed record, names exactly the reviewed paths.
        "paths_equal": published.get("changed_paths_complete") is True
        and bool(hashes)
        and sorted(published.get("changed_paths") or []) == sorted(hashes),
    }


# --------------------------------------------------------------------------
# Deployed settings scan and forge check judging
# --------------------------------------------------------------------------

_FORBIDDEN_ENV = frozenset(
    {
        "CURIE_FAKE_MODEL",
        "CURIE_CREDENTIALS",
        "CURIE_MODEL_BASE_URL",
        "CURIE_MODEL_ENV_KEY",
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_CUSTOM_HEADERS",
        "ANTHROPIC_FOUNDRY_API_KEY",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
        "AWS_BEARER_TOKEN_BEDROCK",
        "OPENROUTER_API_KEY",
        "CURIE_FACTORY_MODEL_API_KEY",
    }
)
_FALSE = frozenset({"", "0", "false", "False", "no", "off"})


def deployed_credential_findings(documents: Iterable[Any], *, grant_secret: str) -> list[str]:
    """Names of any service credential, helper, fallback or fake-model setting. Pure.

    The personal grant may appear only as ``CLAUDE_CODE_OAUTH_TOKEN`` from the
    ``grant_secret`` Secret's ``token`` key.
    """

    findings: set[str] = set()

    def env_entry(entry: Mapping[str, Any]) -> None:
        name = entry.get("name")
        if not isinstance(name, str):
            return
        value = entry.get("value")
        source = entry.get("valueFrom")
        if name == "CLAUDE_CODE_OAUTH_TOKEN":
            ref = (source or {}).get("secretKeyRef") if isinstance(source, dict) else None
            if not (
                isinstance(ref, dict)
                and ref.get("name") == grant_secret
                and ref.get("key") == "token"
                and value is None
            ):
                findings.add(name)
            return
        if name in _FORBIDDEN_ENV and (source is not None or str(value or "") not in _FALSE):
            findings.add(name)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "name" in node and ("value" in node or "valueFrom" in node):
                env_entry(node)
            for key, value in node.items():
                if key == "apiKeyHelper" and value:
                    findings.add("apiKeyHelper")
                elif key == "fakeModel" and value not in (False, None, "", "false"):
                    findings.add("fakeModel")
                elif (
                    key in {"credentials", "credentialsExistingSecret"}
                    and isinstance(value, str)
                    and value
                ):
                    findings.add(key)
                elif (
                    isinstance(key, str)
                    and key in _FORBIDDEN_ENV
                    and isinstance(value, (str, int, bool))
                    and str(value) not in _FALSE
                ):
                    findings.add(key)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for document in documents:
        walk(document)
    return sorted(findings)


def judge_head_checks(check_runs: Any, statuses: Any) -> dict[str, Any]:
    """Latest check runs and combined statuses for one head commit. Pure.

    Shapes: GitHub REST ``GET /repos/{o}/{r}/commits/{ref}/check-runs?filter=latest``
    answers ``{"total_count", "check_runs": [{"status", "conclusion"}]}`` and
    ``GET /repos/{o}/{r}/commits/{ref}/status`` answers ``{"state", "statuses":
    [{"context", "state"}]}`` (https://docs.github.com/en/rest/checks/runs and
    https://docs.github.com/en/rest/commits/statuses).
    """

    runs = check_runs.get("check_runs") if isinstance(check_runs, dict) else None
    contexts = statuses.get("statuses") if isinstance(statuses, dict) else None
    runs = [item for item in runs or [] if isinstance(item, dict)]
    contexts = [item for item in contexts or [] if isinstance(item, dict)]
    pending = sum(item.get("status") != "completed" for item in runs) + sum(
        item.get("state") == "pending" for item in contexts
    )
    failed = sum(
        item.get("status") == "completed"
        and item.get("conclusion") not in {"success", "neutral", "skipped"}
        for item in runs
    ) + sum(item.get("state") in {"failure", "error"} for item in contexts)
    succeeded = sum(item.get("conclusion") == "success" for item in runs) + sum(
        item.get("state") == "success" for item in contexts
    )
    return {
        "total": len(runs) + len(contexts),
        "pending": pending,
        "failed": failed,
        "succeeded": succeeded,
        "all_completed_successfully": bool(runs or contexts)
        and pending == 0
        and failed == 0
        and succeeded > 0,
    }


def foreign_preserved(before: Mapping[str, str], after: Mapping[str, str]) -> bool:
    """Every foreign resource seen before the run still exists with the same uid."""

    return bool(before) and all(after.get(name) == uid for name, uid in before.items())


# --------------------------------------------------------------------------
# Judge
# --------------------------------------------------------------------------

CRITERIA = ("roles", "approval", "pod-restart", "publication", "budget", "cleanup")
INVENTORY_KINDS = (
    "namespace",
    "cluster_scoped",
    "registry_tags",
    "local_images",
    "processes",
    "values_file",
    "forge_issue",
    "forge_prs",
    "forge_branches",
)
SCAN_CHANNELS = ("stdout", "stderr", "children", "http", "harvest", "ledger", "evidence")
OWNER_KEYS = ("tenant_id", "mode", "principal_id", "generation")


def owner_matches(observed: Any, seeded: Any) -> bool:
    if not isinstance(observed, dict) or not isinstance(seeded, dict):
        return False
    return (
        all(observed.get(key) == seeded.get(key) for key in OWNER_KEYS)
        and seeded.get("mode") == PERSONAL_MODE
        and seeded.get("generation") == OWNER_GENERATION
        and bool(seeded.get("tenant_id"))
        and bool(seeded.get("principal_id"))
    )


def _true(observed: Mapping[str, Any], key: str) -> bool:
    return observed.get(key) is True


def _zero(observed: Mapping[str, Any], key: str) -> bool:
    return type(observed.get(key)) is int and observed[key] == 0


def _sessions_of(observed: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = observed.get(key)
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _role_ok(sessions: Sequence[Mapping[str, Any]], role: str, model: str) -> tuple[bool, bool]:
    chosen = [item for item in sessions if item.get("role") == role]
    present = any(
        type(item.get("assistant_turns")) is int
        and item["assistant_turns"] > 0
        and item.get("synthetic") == 0
        for item in chosen
    )
    models = [model_name for item in chosen for model_name in item.get("models") or []]
    return present, bool(models) and all(
        isinstance(name, str) and same_model(name, model) for name in models
    )


def _judge_roles(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("roles")
    sessions = _sessions_of(observed, "sessions_initial")
    every = sessions + _sessions_of(observed, "sessions_resumed")
    implementer, implementer_model = _role_ok(sessions, IMPLEMENTER, IMPLEMENTER_MODEL)
    plan, plan_model = _role_ok(sessions, PLAN_REVIEWER, REVIEWER_MODEL)
    diff, diff_model = _role_ok(sessions, DIFF_REVIEWER, REVIEWER_MODEL)
    seeded = observed.get("owner_seeded")
    requests = observed.get("request_owners")
    checks = {
        "implementer_session": implementer,
        "plan_reviewer_session": plan,
        "diff_reviewer_session": diff,
        "implementer_model": implementer_model,
        "reviewer_model": plan_model and diff_model,
        "no_synthetic_records": _zero(observed, "synthetic_records")
        and all(item.get("synthetic") == 0 for item in every),
        "owner_api_matches": owner_matches(observed.get("owner_api"), seeded),
        "owner_cli_matches": owner_matches(observed.get("owner_cli"), seeded),
        "request_owners_match": isinstance(requests, list)
        and bool(requests)
        and all(owner_matches(item, seeded) for item in requests),
        "bundle_hook_observed": _true(observed, "bundle_hook_observed"),
        "aci_tool_call_observed": type(observed.get("aci_tool_calls")) is int
        and observed["aci_tool_calls"] >= 1,
        "service_credentials_absent": _true(observed, "service_credentials_absent"),
        "candidate_images_running": _true(observed, "candidate_images_running"),
        "one_active_profile_process": observed.get("active_profile_processes_max") == 1,
    }
    counts = {
        "sessions": len(sessions),
        "implementer_sessions": sum(item.get("role") == IMPLEMENTER for item in sessions),
        "reviewer_sessions": sum(item.get("role") in REVIEWER_ROLES for item in sessions),
        "unattributed_sessions": sum(item.get("role") == UNATTRIBUTED for item in every),
    }
    return checks, counts


def _distinct(values: Any, count: int) -> bool:
    return (
        isinstance(values, list)
        and len(values) == count
        and all(isinstance(value, str) and value for value in values)
        and len(set(values)) == count
    )


def _judge_approval(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("approval")
    checks = {
        "approval_parked": _true(observed, "approval_parked"),
        "unlisted_refused_403": observed.get("unlisted_status") == 403,
        "pending_after_unlisted": _true(observed, "pending_after_unlisted"),
        "unlisted_audit_denied": _true(observed, "unlisted_audit_denied"),
        "no_enqueue_after_unlisted": _true(observed, "work_item_unchanged_after_unlisted")
        and _true(observed, "execution_requests_unchanged_after_unlisted")
        and _zero(observed, "publication_jobs_after_unlisted")
        and _zero(observed, "remote_branches_after_unlisted"),
        "owner_unchanged_after_unlisted": _true(observed, "owner_unchanged_after_unlisted"),
        "subjects_distinct": _distinct(observed.get("subject_digests"), 3),
        "reject_resolved": _true(observed, "reject_resolved"),
        "reject_audit_operator": _true(observed, "reject_audit_operator"),
        "approve_resolved": _true(observed, "approve_resolved"),
        "approve_audit_operator": _true(observed, "approve_audit_operator"),
        "fresh_attempt": _distinct(observed.get("approval_digests"), 2),
        "owner_unchanged_after_resume": _true(observed, "owner_unchanged_after_resume"),
        "work_item_unchanged_after_resume": _true(observed, "work_item_unchanged_after_resume"),
        "same_grant": _true(observed, "same_grant"),
    }
    return checks, {"approvals": len(observed.get("approval_digests") or [])}


def _judge_pod_restart(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("pod-restart")
    match = published_matches(
        observed.get("reviewed_workspace"), observed.get("published_workspace")
    )
    checks = {
        "old_pod_absent": _true(observed, "old_pod_absent"),
        "old_process_stream_ended": _true(observed, "old_process_stream_ended"),
        "new_pod_uid_differs": _true(observed, "new_pod_uid_differs"),
        "new_process_after_deletion": _true(observed, "new_process_after_deletion"),
        "owner_generation_unchanged": _true(observed, "owner_generation_unchanged"),
        "approval_pending_after_deletion": _true(observed, "approval_pending_after_deletion"),
        "no_publication_during_restart": _true(observed, "no_publication_during_restart"),
        "reviewed_pod_absent_at_park": _true(observed, "reviewed_pod_absent_at_park"),
        "workspace_head_recorded": match["head_recorded"],
        "workspace_hashes_restored": match["hashes_equal"],
        "workspace_nonce_restored": match["nonce_present"],
        "workspace_deletions_restored": match["deleted_absent"],
        "workspace_paths_equal": match["paths_equal"],
    }
    return checks, {
        "changed_paths": len((observed.get("reviewed_workspace") or {}).get("hashes") or {})
    }


def _judge_publication(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("publication")
    restart = ledger.observations("pod-restart")
    match = published_matches(restart.get("reviewed_workspace"), restart.get("published_workspace"))
    head_checks = (
        observed.get("head_checks") if isinstance(observed.get("head_checks"), dict) else {}
    )
    assert isinstance(head_checks, dict)
    checks = {
        "rejected_no_branch": _zero(observed, "rejected_remote_branches"),
        "rejected_no_pr": _zero(observed, "rejected_prs"),
        "rejection_stays_rejected": _true(observed, "rejection_stays_rejected"),
        "exactly_one_pr": observed.get("prs_for_prefix") == 1,
        "pr_ready": observed.get("pr_draft") is False,
        "pr_head_prefix": _true(observed, "pr_head_prefix"),
        "publication_succeeded": observed.get("publication_status") == "succeeded",
        "work_item_published": observed.get("work_item_state") == "published",
        "diff_review_approved": _true(observed, "diff_review_approved"),
        "published_tree_matches_reviewed": match["hashes_equal"]
        and match["nonce_present"]
        and match["deleted_absent"]
        and match["paths_equal"],
        "head_recorded": bool(
            re.fullmatch(r"[0-9a-f]{40}", str(observed.get("head_sha") or ""))
            and re.fullmatch(r"[0-9a-f]{40}", str(observed.get("tree_sha") or ""))
        ),
        "checks_completed_successfully": head_checks.get("all_completed_successfully") is True,
        "owner_unchanged": _true(observed, "owner_unchanged"),
    }
    counts = {
        key: int(head_checks[key])
        for key in ("total", "pending", "failed", "succeeded")
        if type(head_checks.get(key)) is int
    }
    return checks, counts


def _judge_budget(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("budget")
    run = ledger.run_row()
    sessions = ledger.sessions()
    settled = [item for item in sessions if item["output_tokens"] is not None]
    known = len(settled) == len(sessions) and all(
        item["assistant_turns"] is not None and item["elapsed"] is not None for item in sessions
    )
    live_start, live_end = run["live_started_at"], run["live_ended_at"]
    expires = run["grant_expires_at"]
    cleanup = observed.get("cleanup_elapsed")
    checks = {
        "sessions_recorded": bool(sessions),
        "session_count_within": len(sessions) <= MAX_SESSIONS,
        "no_unknown_usage": bool(sessions) and known,
        "settled_from_native": bool(sessions)
        and all(item["source"] in {"native", "not_started"} for item in sessions),
        "no_unreserved_sessions": not any(item["source"] == "unreserved" for item in sessions),
        "session_bounds": known
        and all(
            int(item["output_tokens"]) <= SESSION_OUTPUT_TOKENS
            and int(item["assistant_turns"]) <= SESSION_TURNS
            and float(item["elapsed"]) <= SESSION_SECONDS
            for item in sessions
        ),
        "total_turns_within": known
        and sum(int(item["assistant_turns"]) for item in sessions) <= MAX_TURNS,
        "total_output_tokens_within": known
        and sum(int(item["output_tokens"]) for item in sessions) <= MAX_OUTPUT_TOKENS,
        "budget_not_exceeded": run["budget_state"] == "ok",
        "no_paid_retry": run["live_starts"] == 1 and "paid_retry" not in observed,
        # Starts were fenced (worker stopped, grant removed, no runner pod) before
        # the live window closed, so no continuation could spend unreserved.
        "paid_starts_fenced": _true(observed, "paid_starts_fenced"),
        "no_allowance_breach": observed.get("allowance_breach") == [],
        "live_elapsed_within": live_start is not None
        and live_end is not None
        and 0 <= float(live_end) - float(live_start) <= LIVE_SECONDS,
        "cleanup_elapsed_within": isinstance(cleanup, (int, float))
        and 0 <= float(cleanup) <= CLEANUP_SECONDS,
        "grant_covered_live_window": live_end is not None
        and expires is not None
        and float(expires) >= float(live_end),
    }
    counts = {
        "sessions": len(sessions),
        "assistant_turns": sum(int(item["assistant_turns"] or 0) for item in settled),
        "output_tokens": sum(int(item["output_tokens"] or 0) for item in settled),
        "retry_refusals": ledger.observation_int("budget", "retry_refusals"),
    }
    return checks, counts


def _judge_cleanup(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("cleanup")
    rows = ledger.cleanup_rows()
    steps = [row for row in rows if row["kind"].startswith("step:")]
    inventory = [row for row in rows if row["kind"].startswith("inventory:")]
    kinds = {row["kind"].removeprefix("inventory:") for row in inventory}
    scans = observed.get("token_scan") if isinstance(observed.get("token_scan"), dict) else {}
    assert isinstance(scans, dict)
    checks = {
        "steps_recorded": bool(steps),
        "steps_ok": bool(steps) and all(row["status"] == "ok" for row in steps),
        "inventory_complete": set(INVENTORY_KINDS) <= kinds,
        "inventory_absent": bool(inventory) and all(row["status"] == "absent" for row in inventory),
        "token_scan_complete": set(SCAN_CHANNELS) <= set(scans),
        "token_bytes_absent": bool(scans)
        and all(
            isinstance(entry, dict) and entry.get("token_bytes_found") is False
            for entry in scans.values()
        ),
        "foreign_preserved": _true(observed, "foreign_preserved"),
    }
    counts = {
        "steps": len(steps),
        "failed_steps": sum(row["status"] != "ok" for row in steps),
        "inventory_queries": len(inventory),
        "residual_or_uncertain": sum(row["status"] != "absent" for row in inventory),
        "scanned_channels": len(scans),
    }
    return checks, counts


_JUDGES: dict[str, Callable[[Ledger], tuple[dict[str, bool], dict[str, int]]]] = {
    "roles": _judge_roles,
    "approval": _judge_approval,
    "pod-restart": _judge_pod_restart,
    "publication": _judge_publication,
    "budget": _judge_budget,
    "cleanup": _judge_cleanup,
}


def judge(ledger: Ledger, criterion: str) -> dict[str, Any]:
    """One criterion's verdict from recorded observations only. No identifiers."""

    if criterion == "all":
        checks: dict[str, bool] = {}
        counts: dict[str, int] = {}
        for name in CRITERIA:
            result = judge(ledger, name)
            checks.update({f"{name}.{key}": value for key, value in result["checks"].items()})
            counts.update({f"{name}.{key}": value for key, value in result["counts"].items()})
        verdict = "pass" if all(checks.values()) else "fail"
        return {"criterion": "all", "verdict": verdict, "checks": checks, "counts": counts}
    checks, counts = _JUDGES[criterion](ledger)
    return {
        "criterion": criterion,
        "verdict": "pass" if checks and all(checks.values()) else "fail",
        "checks": checks,
        "counts": counts,
    }


# --------------------------------------------------------------------------
# Live observation helpers (pure; the driver feeds them recorded API shapes)
# --------------------------------------------------------------------------

GITHUB_API_HOST = "api.github.com"
FORGE_PERMISSIONS = {
    "contents": "write",
    "pull_requests": "write",
    "issues": "write",
    "checks": "read",
    "statuses": "read",
}
PUBLICATION_ROUTE = "publication"
PUBLICATION_GATE = "mcp__curie__publish_changes"
RESOLUTION_CHANNEL = "C0EXAMPLE1"
_BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


def chart_fullname(release: str, chart: str = "curie") -> str:
    """The chart's ``curie.fullname`` helper (charts/curie/templates/_helpers.tpl)."""

    name = release if chart in release else f"{release}-{chart}"
    return name[:63].rstrip("-")


def slack_shaped_subject(random_bytes: bytes) -> str:
    """``U0`` plus eight uppercase base32 characters, as approver user ids must be."""

    if len(random_bytes) < 8:
        raise ValueError("eight random bytes are required")
    return "U0" + "".join(_BASE32[byte % 32] for byte in random_bytes[:8])


def owner_of(item: Any) -> dict[str, Any] | None:
    """The owner tuple from a work item or request row (``GET /work-items/{id}`` shape).

    apps/api/src/curie_api/schemas/workitems.py names the four keys
    ``tenant_id``, ``model_auth_mode``, ``owner_principal_id`` and
    ``enrollment_generation`` on the item and on every ``requests[]`` row.
    """

    if not isinstance(item, dict):
        return None
    return {
        "tenant_id": item.get("tenant_id"),
        "mode": item.get("model_auth_mode"),
        "principal_id": item.get("owner_principal_id"),
        "generation": item.get("enrollment_generation"),
    }


def request_owners(item: Any) -> list[dict[str, Any]]:
    requests = item.get("requests") if isinstance(item, dict) else None
    return [owner for owner in map(owner_of, requests or []) if owner is not None]


def lock_runner_image(text: str) -> str | None:
    """``runner.image`` from a ``connectors.lock.yaml`` (cli/src/connector_build.rs shape)."""

    in_runner = False
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t")):
            in_runner = line.rstrip() == "runner:"
            continue
        if in_runner:
            match = re.fullmatch(r"\s+image:\s*['\"]?([^'\"\s]+)['\"]?\s*", line)
            if match:
                return match[1]
    return None


def pod_grant_ref(pod: Any) -> tuple[str, str] | None:
    """The Secret name and key the runner container reads its grant from, or None."""

    containers = (
        ((pod or {}).get("spec") or {}).get("containers") if isinstance(pod, dict) else None
    )
    for container in containers or []:
        if not isinstance(container, dict) or container.get("name") != "runner":
            continue
        for entry in container.get("env") or []:
            if isinstance(entry, dict) and entry.get("name") == "CLAUDE_CODE_OAUTH_TOKEN":
                ref = (entry.get("valueFrom") or {}).get("secretKeyRef") or {}
                if isinstance(ref, dict) and ref.get("name") and ref.get("key"):
                    return str(ref["name"]), str(ref["key"])
    return None


def audit_row(
    rows: Any, *, action: str, subject: str, authorized: bool, decision: str | None = None
) -> bool:
    """A ``GET /approvals/{id}/audit`` row (schemas/approvals.py ApprovalAuditOut shape)."""

    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if (
            row.get("action") == action
            and row.get("authorized") is authorized
            and subject in (row.get("principal_subject"), row.get("actor"))
            and (decision is None or row.get("decision") == decision)
            and (action == "denied" or row.get("principal_kind") == "operator")
        ):
            return True
    return False


def resolve_status(returncode: int, output: bytes) -> int:
    """The HTTP status a failed ``curie cluster approvals --resolve`` reports, else 0 or 200.

    cli/src/main.rs prints ``resolving approval failed with 403 Forbidden`` and
    exits 1 when the api refuses the operator principal.
    """

    if returncode == 0:
        return 200
    match = re.search(rb"failed with (\d{3})\b", output)
    return int(match[1]) if match else 0


def priority_class_values(classes: tuple[str, str] | None, release: str) -> dict[str, Any]:
    """Reuse configured classes, else per-release ones that helm uninstall removes."""

    if classes is None:
        return {
            "platform": {"create": True, "name": f"{release}-platform"},
            "sandbox": {"create": True, "name": f"{release}-sandbox"},
        }
    return {
        "platform": {"create": False, "name": classes[0]},
        "sandbox": {"create": False, "name": classes[1]},
    }


def install_values(
    *,
    release: str,
    registry: str,
    digests: Mapping[str, str],
    agent: str,
    repo: str,
    app_id: str,
    app_secret: str,
    trigger_label: str,
    priority_classes: tuple[str, str] | None,
    controller_deploy: bool,
    model_addresses: Sequence[str],
) -> dict[str, Any]:
    """Helm values for the personal-profile install. No fake model, no service credential."""

    def image(name: str) -> dict[str, Any]:
        return {
            "repository": f"{registry}/{name}",
            "digest": digests[name],
            "pullPolicy": "IfNotPresent",
        }

    credential = {
        "type": "github_app",
        "appId": app_id,
        "existingSecret": app_secret,
        "existingSecretKey": "privateKey",
    }
    github = {
        "name": "github",
        "kind": "github",
        "host": "github.com",
        "apiUrl": f"https://{GITHUB_API_HOST}",
    }
    return {
        "api": {
            "image": image("curie-api"),
            "extraEnv": [{"name": "FACTORY_MODEL_AUTH_MODE", "value": PERSONAL_MODE}],
            "githubAppId": app_id,
            "githubAppExistingSecret": app_secret,
            "githubRepoAllowlist": [repo],
        },
        "worker": {
            "image": image("curie-worker"),
            "maxConcurrency": 1,
            "workItemMaxTurns": SESSION_TURNS,
        },
        "dispatcher": {"deploy": False},
        "ui": {"deploy": False},
        "inference": {"deploy": False},
        "agentSandbox": {
            "controller": {"deploy": controller_deploy},
            "warmPool": {"replicas": 0},
            "runner": {
                "image": f"{registry}/curie-runner",
                "digest": digests["curie-runner"],
                "imagePullPolicy": "IfNotPresent",
                "fakeModel": False,
                "credentials": "",
                "model": IMPLEMENTER_MODEL,
                "extraEnv": [
                    {"name": "CURIE_MODEL_AUTH_MODE", "value": PERSONAL_MODE},
                    {
                        "name": "CLAUDE_CODE_OAUTH_TOKEN",
                        "valueFrom": {
                            "secretKeyRef": {"name": f"{release}-personal-grant", "key": "token"}
                        },
                    },
                    {"name": "CURIE_MAX_TURNS", "value": str(SESSION_TURNS)},
                    {"name": "CLAUDE_CODE_MAX_OUTPUT_TOKENS", "value": str(RESPONSE_OUTPUT_TOKENS)},
                ],
            },
        },
        "factory": {
            "enabled": True,
            "intake": "poll",
            "codeHosts": [{**github, "credential": dict(credential)}],
            # Intake stays dormant: the label and the mention are this run's unused
            # value, and the one admission is the driver's trusted machine admission.
            "trackers": [
                {
                    **github,
                    "credential": dict(credential),
                    "label": trigger_label,
                    "mention": trigger_label,
                }
            ],
            "sandboxEgress": {agent: True},
        },
        "priorityClasses": priority_class_values(priority_classes, release),
        "security": {
            "gvisor": {"mode": "off"},
            "networkPolicy": {
                "allowedEgress": [
                    {"cidr": f"{address}/32", "ports": [{"protocol": "TCP", "port": 443}]}
                    for address in sorted(set(model_addresses))
                ]
            },
        },
    }


RESERVATION_GATE_FILE = "reservation_gate.py"
RESERVATION_LIMITS = {
    "plan": ATTEMPT_ROLES.count(PLAN_REVIEWER),
    "diff": ATTEMPT_ROLES.count(DIFF_REVIEWER),
}

# A PreToolUse hook added to the private bundle copy. It counts reviewer starts
# at invocation, before any model call, and refuses a start past this attempt's
# reservations or of any other subagent type. Exit 2 blocks the call; every
# unexpected state also exits 2. One attempt is one pod, so /tmp is per attempt.
RESERVATION_GATE_PROGRAM = r"""
import json, os, sys
LIMITS = json.loads(sys.argv[1])
def refuse(reason):
    print("reservation gate: " + reason, file=sys.stderr)
    sys.exit(2)
try:
    event = json.load(sys.stdin)
    if not isinstance(event, dict):
        refuse("unreadable hook input")
    if event.get("tool_name") not in ("Agent", "Task"):
        sys.exit(0)
    # The bundle's own reviewer resolution, including a type inferred from the
    # description, so this gate admits exactly what review_gate.py routes.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import review_gate
    resolved = review_gate.reviewer_kind(dict(event.get("tool_input") or {}))
    kind = {review_gate.PLAN: "plan", review_gate.DIFF: "diff"}.get(resolved)
    if kind is None:
        refuse("only the reserved plan and diff reviewers may start in this run")
    folder = os.environ.get("PPA_RESERVATION_DIR") or "/tmp/ppa-reservations"
    os.makedirs(folder, mode=0o700, exist_ok=True)
    path = os.path.join(folder, "starts.json")
    counts = {}
    if os.path.lexists(path):
        with open(path) as handle:
            counts = json.load(handle)
        if not isinstance(counts, dict):
            refuse("unreadable reservation state")
    if int(counts.get(kind, 0)) >= int(LIMITS[kind]):
        refuse("the reserved " + kind + " review allowance is spent; no further " + kind
               + " reviewer may start")
    counts[kind] = int(counts.get(kind, 0)) + 1
    with open(path + ".tmp", "w") as handle:
        json.dump(counts, handle)
    os.replace(path + ".tmp", path)
except SystemExit:
    raise
except Exception:
    refuse("reservation state could not be updated")
"""


def private_bundle_hooks(hooks: Mapping[str, Any]) -> dict[str, Any]:
    """The private copy's hooks.json with the reservation gate added for Agent and Task."""

    patched: dict[str, Any] = json.loads(json.dumps(hooks))
    limits = json.dumps(RESERVATION_LIMITS, sort_keys=True, separators=(",", ":"))
    command = f"python3 ${{CLAUDE_PLUGIN_ROOT}}/hooks/{RESERVATION_GATE_FILE} '{limits}'"
    entry = {
        "matcher": "Agent|Task",
        "hooks": [{"type": "command", "command": command, "timeout": 10}],
    }
    patched.setdefault("hooks", {}).setdefault("PreToolUse", []).append(entry)
    return patched


def private_bundle_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """The private bundle copy's manifest: the publication gate routed to ``publication``.

    Recipe: examples/coder/README.md "Approving publication without Slack".
    """

    patched = dict(manifest)
    patched["approvalPolicy"] = {"gates": [{"gate": PUBLICATION_GATE, "route": PUBLICATION_ROUTE}]}
    return patched


def pod_diagnostics(pods: Mapping[str, Any], events: Mapping[str, Any]) -> dict[str, Any]:
    """Why a rollout stalled, from pod status and Warning events only.

    Reads ``status.containerStatuses[].state`` and Warning event reasons, never
    logs or env, and cuts each message to 300 characters.
    """

    def state(status: Mapping[str, Any]) -> dict[str, Any]:
        current = status.get("state") or {}
        last = (status.get("lastState") or {}).get("terminated") or {}
        found: dict[str, Any] = {
            "container": status.get("name"),
            "ready": bool(status.get("ready")),
            "restarts": status.get("restartCount", 0),
        }
        for key in ("waiting", "terminated"):
            detail = current.get(key)
            if isinstance(detail, dict):
                found[key] = {
                    "reason": detail.get("reason"),
                    "exit_code": detail.get("exitCode"),
                    "message": str(detail.get("message") or "")[:300],
                }
        if last:
            found["last_terminated"] = {
                "reason": last.get("reason"),
                "exit_code": last.get("exitCode"),
            }
        return found

    pod_states = []
    for pod in pods.get("items") or []:
        if not isinstance(pod, dict):
            continue
        status = pod.get("status") or {}
        containers = [
            *(status.get("initContainerStatuses") or []),
            *(status.get("containerStatuses") or []),
        ]
        pod_states.append(
            {
                "pod": (pod.get("metadata") or {}).get("name"),
                "phase": status.get("phase"),
                "containers": [state(item) for item in containers if isinstance(item, dict)],
            }
        )
    warnings = [
        {
            "object": f"{(item.get('involvedObject') or {}).get('kind')}/"
            f"{(item.get('involvedObject') or {}).get('name')}",
            "reason": item.get("reason"),
            "message": str(item.get("message") or "")[:300],
            "count": item.get("count"),
        }
        for item in events.get("items") or []
        if isinstance(item, dict) and item.get("type") == "Warning"
    ]
    return {"pods": pod_states, "warnings": warnings[-20:]}


def running_image_failures(
    pods: Any, expected: Mapping[str, str], *, registry: str, required: Sequence[str]
) -> list[str]:
    """Every running container of a candidate repository must report its pushed digest.

    ``pods`` is a ``kubectl get pods -o json`` body; ``expected`` maps image
    name to the pushed ``sha256:`` digest. Returns sanitized failure names.
    """

    failures: list[str] = []
    seen: set[str] = set()
    for pod in (pods or {}).get("items") or [] if isinstance(pods, dict) else []:
        if not isinstance(pod, dict):
            continue
        spec = pod.get("spec") or {}
        images = {
            str(item.get("name")): str(item.get("image") or "")
            for item in [*(spec.get("initContainers") or []), *(spec.get("containers") or [])]
            if isinstance(item, dict)
        }
        status = pod.get("status") or {}
        for item in [
            *(status.get("initContainerStatuses") or []),
            *(status.get("containerStatuses") or []),
        ]:
            if not isinstance(item, dict):
                continue
            spec_image = images.get(str(item.get("name")), "")
            if not spec_image.startswith(registry + "/"):
                continue
            name = re.split(r"[@:]", spec_image[len(registry) + 1 :], maxsplit=1)[0]
            if name not in expected:
                continue
            found = _DIGEST.findall(str(item.get("imageID") or ""))
            if not found or found[-1] != expected[name]:
                failures.append(f"{name}_digest_mismatch")
            else:
                seen.add(name)
    failures.extend(f"{name}_not_running" for name in required if name not in seen)
    return sorted(set(failures))


def route_bound_to(listing: Any, route: str, subject: str) -> bool:
    """``approvals <agent> --list-routes --json`` binds ``route`` to exactly ``subject``.

    Shape: cli/src/commands/approvals.rs ``ApprovalsOutput::Routes`` emits
    ``{"agent", "routes": {<name>: {"resolution", "approvers": {"users": [...]}}}, "count"}``.
    """

    routes = listing.get("routes") if isinstance(listing, dict) else None
    binding = routes.get(route) if isinstance(routes, dict) else None
    approvers = binding.get("approvers") if isinstance(binding, dict) else None
    users = approvers.get("users") if isinstance(approvers, dict) else None
    return users == [subject]


def read_path_checks(
    *,
    pending: Any,
    routes: Any,
    work_items: Any,
    api_pending: Any,
    allowed_subject: str,
) -> dict[str, bool]:
    """The read paths the live run depends on, answered in the shapes it parses. Pure.

    ``pending`` is ``approvals --list --json`` (``{"pending": [...], "count"}``),
    ``work_items`` is ``work-items --agent <agent> --json`` (``{"items", "limit",
    "truncated"}``, cli/src/commands/work.rs) and ``api_pending`` is ``GET
    /approvals?status_filter=pending&agent_id=<id>`` (a list).
    """

    rows = pending.get("pending") if isinstance(pending, dict) else None
    items = work_items.get("items") if isinstance(work_items, dict) else None
    return {
        "approvals_list_empty": isinstance(rows, list) and not rows,
        "publication_route_bound": route_bound_to(routes, PUBLICATION_ROUTE, allowed_subject),
        "work_items_list_empty": isinstance(items, list) and not items,
        "api_pending_empty": isinstance(api_pending, list) and not api_pending,
    }
