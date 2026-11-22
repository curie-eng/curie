"""Support for the live publication candidate proof (#5428, acceptance criterion 4).

The inputs, probes, reads, worker-side reader program and owned fixture cleanup
that ``test_publication_candidates_live.py`` drives. Nothing here names a host,
user, repository, App, registry or kube context; every target comes from the
caller's environment through the factory end to end driver.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
import json
import re
import shutil
import sys
import time
import urllib.parse
from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from curie_worker.sandbox.k8s import ADMISSION_CLAIM_LABEL

FLAG = "CURIE_FACTORY_CANDIDATE_RETENTION_LIVE"
REPO_ROOT = Path(__file__).resolve().parents[3]
DRIVER_DIR = REPO_ROOT / "tools" / "factory-e2e"
LEDGER_LIMIT_USD = Decimal("20.00")
CLAIMS = "sandboxclaims.extensions.agents.x-k8s.io"
ADMISSION_TIMEOUT_S = 900.0
RUN_TIMEOUT_S = 3600.0
RUNTIME_GONE_TIMEOUT_S = 1200.0
QUIET_WINDOW_S = 60.0
POLL_S = 15.0
SHA40 = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"\b[0-9a-f]{7,40}\b")
PUBLISHED_STATES = frozenset({"awaiting_approval", "publishing", "published"})


# Runs inside the worker pod with the worker's own configuration. Prints one
# JSON line of digests, sizes, counts and booleans; a failure prints only its type.
WORKER_PROGRAM = r"""
import asyncio, hashlib, json, os, shutil, subprocess, sys, tarfile, tempfile, uuid
from pathlib import Path

PARAMS = json.loads(__PARAMS__)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def file_digest(root, relative):
    target = root / relative
    return digest(target) if target.is_file() else None


def git(root, *args):
    return subprocess.run(
        ["git", "-c", "safe.directory=*", *args],
        cwd=root, capture_output=True, text=True, check=True, timeout=120,
    ).stdout


async def main():
    import httpx
    from aci_protocol.s3 import build_s3_client
    from curie_worker.config import WorkerConfig
    from curie_worker.publication_candidate_storage import CandidateObjectStore, CandidateReader
    from curie_worker.publication_candidates import PublicationCandidateClient
    from curie_worker.workspace import WorkspaceObjectStore
    from curie_worker.workspace_recovery_storage import build_recovery_s3_client

    config = WorkerConfig()
    s3 = dict(
        endpoint_url=config.s3_endpoint_url, access_key=config.s3_access_key,
        secret_key=config.s3_secret_key, region=config.s3_region,
    )
    out = {}
    workspaces = WorkspaceObjectStore(
        client=build_s3_client(**s3), bucket=config.workspace_bucket,
        prefix=config.workspace_object_prefix,
    )
    keys = list(workspaces.list_keys(""))
    out["ownership_records"] = sum(1 for key in keys if key.startswith("_ownership/"))
    out["claim_archives"] = sum(
        1 for key in keys if not key.startswith("_ownership/") and key.endswith(".tar.gz")
    )
    store = CandidateObjectStore(
        client=build_recovery_s3_client(**s3), bucket=config.workspace_bucket
    )
    root = config.workspace_scratch_root if os.path.isdir(config.workspace_scratch_root) else None
    scratch = Path(tempfile.mkdtemp(prefix="candidate-verify-", dir=root))
    transport = httpx.AsyncHTTPTransport()
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            ledger = PublicationCandidateClient(
                api_base_url=config.api_base_url, worker_token=config.internal_worker_token,
                client=http,
            )
            reader = CandidateReader(
                ledger=ledger, store=store, transport=transport,
                reference_ttl_seconds=config.workspace_reference_ttl_seconds,
            )
            lease = await reader.acquire(
                work_item_id=uuid.UUID(PARAMS["work_item_id"]),
                candidate_id=uuid.UUID(PARAMS["candidate_id"]),
                holder_id="live-verify-" + uuid.uuid4().hex[:12],
            )
            paths = {}
            try:
                for kind in ("base", "patch"):
                    destination = scratch / kind
                    await reader.read(lease, kind, destination)
                    ref = lease.objects[kind]
                    paths[kind] = destination
                    out[kind] = {
                        "sha256": digest(destination),
                        "size_bytes": destination.stat().st_size,
                        "ledger_sha256": ref.sha256,
                        "ledger_size_bytes": ref.size_bytes,
                    }
            finally:
                await reader.release(lease)
        tree = scratch / "tree"
        tree.mkdir()
        with tarfile.open(paths["base"], mode="r:gz") as archive:
            archive.extractall(tree, filter="tar")
        out["base_head"] = git(tree, "rev-parse", "HEAD").strip()
        before = file_digest(tree, PARAMS["protected_path"])
        numstat = git(tree, "apply", "--numstat", str(paths["patch"]))
        out["changed_files"] = sum(1 for line in numstat.splitlines() if line.strip())
        git(tree, "apply", "--binary", str(paths["patch"]))
        after = file_digest(tree, PARAMS["protected_path"])
        out["protected_in_base"] = before is not None
        out["protected_changed"] = before != after
    finally:
        await transport.aclose()
        shutil.rmtree(scratch, ignore_errors=True)
    return out


try:
    result = asyncio.run(main())
except Exception as exc:
    code = getattr(exc, "code", None)
    code = code if isinstance(code, str) else None
    print(json.dumps({"error": type(exc).__name__, "code": code}))
    sys.exit(3)
print(json.dumps(result, sort_keys=True))
"""


class SetupRefused(Exception):
    """A required input is missing or failed its probe."""


@dataclasses.dataclass
class Watch:
    """What the run loop saw of the runtime and the candidate."""

    claims: set[str] = dataclasses.field(default_factory=set)
    pods: dict[str, str] = dataclasses.field(default_factory=dict)
    capture_usage: dict[str, Any] | None = None
    capture_usage_after_terminal: bool = False


@dataclasses.dataclass
class Live:
    fe: Any
    p: Any
    candidate: str
    model_key: str
    ledger_usd: Decimal
    protected_path: str
    label: str
    branch_prefix: str
    nonce: str
    curie_version: str
    spend_before: Decimal | None = None
    issue_title: str = ""
    issue_body: str = ""
    issue_numbers: set[int] = dataclasses.field(default_factory=set)
    pr_numbers: set[int] = dataclasses.field(default_factory=set)
    branches: set[str] = dataclasses.field(default_factory=set)

    def scrub(self, text: str) -> str:
        p = self.p
        secrets = [
            self.model_key,
            p.api_key,
            p.worker_token,
            p.issue_token,
            p.config.actor_token,
            p.config.webhook_secret,
        ]
        redacted, _ = self.fe.redact_agent_text(text, secrets)
        return str(redacted or "")


def evidence(step: str, **facts: Any) -> None:
    line = json.dumps({"step": step, **facts}, sort_keys=True, default=str)
    print(f"candidate-retention-live {line}", flush=True)


def driver() -> Any:
    if str(DRIVER_DIR) not in sys.path:
        sys.path.insert(0, str(DRIVER_DIR))
    return importlib.import_module("factory_e2e")


def decimal_value(raw: str) -> Decimal | None:
    try:
        value = Decimal(raw.strip())
    except (InvalidOperation, AttributeError):
        return None
    return value if value.is_finite() else None


def missing_inputs(fe: Any, env: Mapping[str, str]) -> list[str]:
    """Every missing or malformed input, named, with no network or cluster call."""

    missing: list[str] = []
    if not SHA40.fullmatch(env.get("CURIE_FACTORY_CANDIDATE_SHA", "")):
        missing.append("CURIE_FACTORY_CANDIDATE_SHA (the full 40 hex candidate commit)")
    if not env.get("CURIE_BIN"):
        missing.append("CURIE_BIN (the candidate curie CLI)")
    key_file = env.get("MODEL_KEY_FILE", "")
    if not key_file or not Path(key_file).is_file():
        missing.append("MODEL_KEY_FILE (the supported model key file)")
    if decimal_value(env.get("CURIE_FACTORY_SPEND_LEDGER_USD", "")) is None:
        missing.append("CURIE_FACTORY_SPEND_LEDGER_USD (the shared ledger before this run)")
    if not env.get("BUILDX_BUILDER"):
        missing.append("BUILDX_BUILDER (the buildx builder candidate images build on)")
    try:
        # A placeholder actor token: naming inputs never runs `gh`.
        fe.load_config(env, context=None, gh_token=lambda _user: "deferred")
    except fe.ConfigError as exc:
        missing.append(str(exc))
    return missing


def spend(fe: Any, key: str) -> Decimal:
    """The provider key's cumulative usage: a non-generating key read."""

    try:
        status, body = fe.http_json(
            "GET", fe.OPENROUTER_KEY_URL, headers={"Authorization": f"Bearer {key}"}
        )
    except OSError as exc:
        raise SetupRefused(f"the spend reading failed ({type(exc).__name__})") from None
    data = body.get("data") if status == 200 and isinstance(body, dict) else None
    usage = data.get("usage") if isinstance(data, dict) else None
    if isinstance(usage, bool) or not isinstance(usage, (int, float)):
        raise SetupRefused(f"the spend reading returned no number (HTTP {status})")
    return Decimal(str(usage))


def candidate_revision_check(fe: Any, candidate: str, binary: str) -> tuple[str, str]:
    resolved, version = fe.resolve_curie_binary(binary)
    revisions = _REVISION.findall(version)
    if revisions and not any(candidate.startswith(found) for found in revisions):
        raise SetupRefused("CURIE_BIN reports a revision other than the candidate SHA")
    return str(resolved), str(version)


def choose_protected_file(p: Any) -> str:
    """Mint an App installation token, read the default branch tree, revoke it."""

    name = p.config.repo.split("/", 1)[1]
    status, body = p.as_app(
        "POST",
        f"/app/installations/{p.config.installation_id}/access_tokens",
        {"repositories": [name], "permissions": {"contents": "read"}},
    )
    token = body.get("token") if status == 201 and isinstance(body, dict) else None
    if not isinstance(token, str) or not token:
        raise SetupRefused(f"the App could not mint an installation token (HTTP {status})")
    try:
        branch = urllib.parse.quote(p.default_branch, safe="")
        status, tree = p.github("GET", f"/repos/{p.config.repo}/git/trees/{branch}", token=token)
    finally:
        p.github("DELETE", "/installation/token", token=token)
    entries = tree.get("tree") if status == 200 and isinstance(tree, dict) else None
    if not isinstance(entries, list):
        raise SetupRefused(f"the App token could not read the default branch tree ({status})")
    blobs = [
        str(entry["path"])
        for entry in entries
        if isinstance(entry, dict) and entry.get("type") == "blob" and entry.get("path")
    ]
    readme = [path for path in blobs if path.casefold() in ("readme.md", "readme")]
    text = [p for p in blobs if not p.startswith(".") and p.endswith((".md", ".txt"))]
    chosen = (readme or text or [""])[0]
    if not chosen:
        raise SetupRefused("the fixture default branch has no top-level text file to protect")
    return chosen


def probe_label(fe: Any, p: Any, nonce: str) -> None:
    """Authenticated write access: create and delete a unique probe label."""

    repo = f"/repos/{p.config.repo}"
    name = f"curie-cand-probe-{nonce}"
    quoted = urllib.parse.quote(name, safe="")
    status, _ = p.as_actor("POST", f"{repo}/labels", {"name": name, "color": "ededed"})
    try:
        if status != 201:
            raise SetupRefused(f"the actor could not create a probe label (HTTP {status})")
    finally:
        p.as_actor("DELETE", f"{repo}/labels/{quoted}")

    def gone() -> bool:
        return bool(p.as_actor("GET", f"{repo}/labels/{quoted}")[0] == 404)

    fe._wait("the probe label to read back as deleted", 60, gone, 3)


def which(tool: str) -> bool:
    return shutil.which(tool) is not None


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------


def raw_get(live: Live, path: str) -> tuple[int, str]:
    p = live.p
    headers = {"X-API-Key": p.api_key}
    try:
        status, text = live.fe.http_text(p.api_url + path, headers=headers)
    except OSError:
        p.ensure_api()
        status, text = live.fe.http_text(p.api_url + path, headers=headers)
    return int(status), str(text)


def read_candidates(live: Live, work_item_id: str) -> tuple[str, dict[str, Any]]:
    status, text = raw_get(live, f"/work-items/{work_item_id}/publication-candidates")
    assert status == 200, f"candidate metadata answered HTTP {status}"
    body = json.loads(text)
    assert isinstance(body, dict), "candidate metadata is not an object"
    return text, body


def read_usage(live: Live, work_item_id: str) -> dict[str, Any]:
    usage = live.p.work_item_usage(work_item_id)
    assert isinstance(usage, dict), "the WorkItem usage record could not be read"
    return usage


def usage_shape(usage: Mapping[str, Any]) -> dict[str, Any]:
    models = sorted(
        (
            str(m.get("model")),
            str(m.get("role")),
            int(m.get("input_tokens") or 0),
            int(m.get("cached_input_tokens") or 0),
            int(m.get("cache_write_tokens") or 0),
            int(m.get("output_tokens") or 0),
        )
        for m in usage.get("models") or []
    )
    requests = sorted(
        (str(r.get("request_id")), int(r.get("tokens") or 0)) for r in usage.get("requests") or []
    )
    return {
        "total_tokens": int(usage.get("total_tokens") or 0),
        "requests_without_usage": int(usage.get("requests_without_usage") or 0),
        "models": models,
        "requests": requests,
    }


def observe_runtime(live: Live, watch: Watch) -> tuple[set[str], dict[str, str]]:
    """Claims and claim-bound runner pods in the owned namespace now."""

    p = live.p
    claims_raw = p.kubectl("-n", p.namespace, "get", CLAIMS, "-o", "json", check=False)
    pods_raw = p.kubectl(
        "-n", p.namespace, "get", "pods", "-l", ADMISSION_CLAIM_LABEL, "-o", "json", check=False
    )
    claims: set[str] = set()
    pods: dict[str, str] = {}
    for item in items_of(claims_raw):
        claims.add(str((item.get("metadata") or {}).get("name") or ""))
    for item in items_of(pods_raw):
        meta = item.get("metadata") or {}
        owner = str((meta.get("labels") or {}).get(ADMISSION_CLAIM_LABEL) or "")
        pods[f"{owner}/{meta.get('name')}"] = str(meta.get("creationTimestamp") or "")
    claims.discard("")
    watch.claims |= claims
    watch.pods.update(pods)
    return claims, pods


def items_of(raw: str) -> list[dict[str, Any]]:
    try:
        body = json.loads(raw or "{}")
    except ValueError:
        return []
    items = body.get("items") if isinstance(body, dict) else None
    return [item for item in items or [] if isinstance(item, dict)]


def admitted(live: Live, agent_id: str, number: int) -> str | None:
    p = live.p
    status, body = p.api(
        "GET", f"/work-items?agent_id={agent_id}&limit=50", headers={"X-API-Key": p.api_key}
    )
    if status != 200 or not isinstance(body, dict):
        return None
    for item in body.get("items") or []:
        tracker = item.get("tracker") or {}
        if str(tracker.get("issue_id")) == str(number):
            return str(item["id"])
    return None


def ended(detail: Mapping[str, Any] | None, fe: Any) -> bool:
    requests = (detail or {}).get("requests") or []
    return bool(
        requests
        and all(r.get("terminal_at") for r in requests)
        and requests[-1].get("status") not in fe.ACTIVE_REQUEST_STATUSES
    )


def quiet(fe: Any, seconds: float, what: str) -> None:
    until = time.monotonic() + seconds
    fe._wait(what, seconds + 60, lambda: time.monotonic() >= until, 5)


# --------------------------------------------------------------------------
# Forge cleanup: only what this run created
# --------------------------------------------------------------------------


def forge_cleanup(live: Live) -> dict[str, Any]:
    p = live.p
    repo = f"/repos/{p.config.repo}"
    label = urllib.parse.quote(live.label, safe="")
    errors: list[str] = []

    def attempt(what: str, step: Callable[[], Any]) -> None:
        try:
            step()
        except Exception as exc:  # noqa: BLE001 - keep cleaning, report at the end
            errors.append(f"{what}: {type(exc).__name__}")

    def discover() -> None:
        for issue in p._paged(f"{repo}/issues?state=all&labels={label}"):
            live.issue_numbers.add(int(issue["number"]))
        for pull in p._paged(f"{repo}/pulls?state=all"):
            head = str((pull.get("head") or {}).get("ref") or "")
            number = int(pull["number"])
            if head.startswith(live.branch_prefix) or number in live.pr_numbers:
                live.pr_numbers.add(number)
                live.branches.add(head)
        status, refs = p.as_actor(
            "GET", f"{repo}/git/matching-refs/heads/{live.branch_prefix.rstrip('/')}"
        )
        for ref in refs if status == 200 and isinstance(refs, list) else []:
            name = str(ref.get("ref") or "").removeprefix("refs/heads/")
            if name.startswith(live.branch_prefix):
                live.branches.add(name)

    attempt("inventory", discover)
    for number in sorted(live.pr_numbers):
        attempt(
            f"close pull #{number}", functools.partial(patch_closed, p, f"{repo}/pulls/{number}")
        )
    for name in sorted(live.branches):
        ref = urllib.parse.quote(name, safe="/")
        attempt(
            f"delete branch {name}",
            functools.partial(delete_ref, p, f"{repo}/git/refs/heads/{ref}"),
        )
    for number in sorted(live.issue_numbers):
        attempt(
            f"close issue #{number}", functools.partial(patch_closed, p, f"{repo}/issues/{number}")
        )
    attempt("delete label", functools.partial(delete_ref, p, f"{repo}/labels/{label}"))

    def state(path: str) -> str | None:
        status, body = p.as_actor("GET", path)
        return str(body.get("state")) if status == 200 and isinstance(body, dict) else None

    def verified() -> bool:
        return (
            all(state(f"{repo}/issues/{n}") == "closed" for n in live.issue_numbers)
            and all(state(f"{repo}/pulls/{n}") == "closed" for n in live.pr_numbers)
            and all(
                p.as_actor("GET", f"{repo}/git/ref/heads/{urllib.parse.quote(b, safe='/')}")[0]
                == 404
                for b in live.branches
            )
            and p.as_actor("GET", f"{repo}/labels/{label}")[0] == 404
        )

    try:
        live.fe._wait("owned fixture objects to read back as removed", 120, verified, 5)
    except Exception as exc:  # noqa: BLE001 - reported below with the other errors
        errors.append(f"verification: {type(exc).__name__}")
    if errors:
        raise AssertionError("fixture cleanup incomplete: " + "; ".join(errors))
    return {
        "issues_closed": len(live.issue_numbers),
        "pulls_closed": len(live.pr_numbers),
        "branches_deleted": len(live.branches),
        "label_absent": True,
    }


def delete_ref(p: Any, path: str) -> None:
    status, _ = p.as_actor("DELETE", path)
    if status not in (204, 404, 422):
        raise AssertionError(f"deleting answered HTTP {status}")


def patch_closed(p: Any, path: str) -> None:
    status, _ = p.as_actor("PATCH", path, {"state": "closed"})
    if status != 200:
        raise AssertionError(f"closing answered HTTP {status}")
