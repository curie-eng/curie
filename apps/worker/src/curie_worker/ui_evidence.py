"""Optional UI evidence: capture from the runner, relay to the API.

A factory run may leave before and after screenshots in the gitignored
``.curie/ui-evidence/`` directory of its checkout. The kernel calls into this
module twice and nowhere else:

``capture_ui_evidence`` runs once in the pre-approval window, right after the
publication snapshot validates (the sandbox is released at approval). It reads
the runner's validated index, re-derives every binding with the worker's own
rule, and streams each bound image into an anonymous temporary file;
``RunnerClient.ui_evidence_image`` checks its size and sha256 while it streams.

``relay_ui_evidence`` runs once right after the publication is created and
uploads the index, the images and a seal to the API's worker-token evidence
routes keyed by the publication id.

Evidence is bonus material. Both entry points are bounded, never raise (other
than ``asyncio.CancelledError``), and never touch the snapshot, the publication
request, creation, the suspend or the reply. This module imports no forge code;
the API owns every code host call.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import re
import tempfile
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import IO, TYPE_CHECKING, Any
from urllib.parse import quote

import httpx
from curie_telemetry import inject_trace_context

from .api_retry import DEFAULT_BUDGET_S as _API_WRITE_BUDGET_S

if TYPE_CHECKING:
    from .runner_client import RunnerClient

logger = logging.getLogger(__name__)

# Budgets in seconds. Read at call time so tests may shorten them.
CAPTURE_BUDGET_S = 120.0
UPLOAD_BUDGET_S = 60.0
# Evidence work never spends what the publication flow still needs. What is
# left is the earlier of the delivery deadline and the factory execution
# deadline (the publication create and the approval hold are both refused
# after it). Each evidence step leaves one whole API write budget plus the
# settle floor of that untouched, for the create after the capture and for the
# hold and settle after the upload. The capture is skipped when what it may
# spend is _MIN_CAPTURE_S or less, the upload when nothing is left.
_ATTEMPT_RESERVE_S = 5.0
_MIN_CAPTURE_S = 5.0


def _evidence_budget(
    cap_s: float, remaining_s: float | None, execution_deadline: datetime | None
) -> float:
    """``cap_s``, cut so the flow after this step keeps its own budget."""

    left = [] if remaining_s is None else [remaining_s]
    if execution_deadline is not None:
        left.append((execution_deadline - datetime.now(UTC)).total_seconds())
    if not left:
        return cap_s
    return min(cap_s, min(left) - _ATTEMPT_RESERVE_S - _API_WRITE_BUDGET_S)


# A runner request's own HTTP bound sits just past the capture deadline so the
# capture deadline, not the transport, decides a timeout.
_HTTP_GRACE_S = 1.0

MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 100 * 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024
_RETRY_STATUSES = frozenset({500, 502, 503, 504})
_UPLOAD_BACKOFF_S = (0.5, 1.0, 2.0)

_OID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_STATES = frozenset({"before", "after"})
_MANIFEST_STATUSES = frozenset({"complete", "partial", "skipped", "failed"})
_MANIFEST_REFUSALS = frozenset({"too_large", "not_json", "schema", "symlink", "not_regular"})
# Refusals decided before the PNG validated: such an entry carries no size or
# digest and is never bound.
_UNVALIDATED_REFUSALS = frozenset(
    {"traversal", "invalid_entry", "symlink", "missing"}
    | {"not_regular", "not_png", "corrupt_png", "too_large"}
)
# A runner whose index budget ran out mid-index reports the rest ``timeout``,
# with a size and digest only when the image was read before the deadline.
_RUNNER_REFUSALS = _UNVALIDATED_REFUSALS | {"unbound", "total_limit", "timeout"}
_ORACLE_KEYS = (
    "manifest_base_sha",
    "manifest_head_sha",
    "manifest_candidate_tree",
    "candidate_tree",
    "head_tree",
)


class UiEvidenceIndexError(ValueError):
    """The runner's evidence index broke its wire shape."""


class UiEvidenceUploadError(RuntimeError):
    """The API refused or could not take the upload; ``code`` never carries a body."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class UiEvidenceCapture:
    """One run's evidence, between capture and relay.

    ``index`` is the normalized worker-to-API index, kept current as images are
    fetched or refused. ``files`` holds one anonymous temporary file per fetched
    ordinal, positioned at 0; ``close`` releases them all.
    """

    index: dict[str, Any]
    files: dict[int, IO[bytes]] = field(default_factory=dict)

    def close(self) -> None:
        for handle in self.files.values():
            try:
                handle.close()
            except OSError:
                pass


# --- Index ---------------------------------------------------------------------


def _bindable(state: str, *, base_sha: str, manifest: dict[str, Any]) -> bool:
    """Decision 2: whether an image of ``state`` may be labelled for this publication.

    ``before`` binds only to the publication base. ``after`` binds only to the
    candidate tree, named directly or, for a checkout declared exactly clean,
    through the tree of its head commit.
    """

    if state == "before":
        return bool(manifest["manifest_base_sha"] == base_sha)
    candidate = manifest["candidate_tree"]
    return candidate is not None and (
        manifest["manifest_candidate_tree"] == candidate
        or (manifest["candidate_dirty"] is False and manifest["head_tree"] == candidate)
    )


def _image(
    raw: object, position: int, *, base_sha: str, manifest: dict[str, Any]
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise UiEvidenceIndexError("image entry is not an object")
    ordinal, refusal = raw.get("ordinal"), raw.get("refusal")
    if type(ordinal) is not int or ordinal != position:
        raise UiEvidenceIndexError("image ordinal is out of order")
    if refusal is not None and refusal not in _RUNNER_REFUSALS:
        raise UiEvidenceIndexError("image refusal is not a known code")
    path, label, state = raw.get("path"), raw.get("label"), raw.get("state")
    # The runner reports a missing or ill-typed field as null; only an entry it
    # refused before reading the image may carry one.
    nullable = refusal in _UNVALIDATED_REFUSALS or refusal == "timeout"
    if (
        not (isinstance(path, str) or (nullable and path is None))
        or not (isinstance(label, str) or (nullable and label is None))
        or not (state in _STATES or (nullable and state is None))
    ):
        raise UiEvidenceIndexError("image entry lacks a path, label or state")
    entry: dict[str, Any] = {"ordinal": ordinal, "path": path, "label": label, "state": state}
    entry.update(size=None, sha256=None, refusal=refusal, bound=False)
    size, sha256 = raw.get("size"), raw.get("sha256")
    if refusal in _UNVALIDATED_REFUSALS or (
        refusal == "timeout" and size is None and sha256 is None
    ):
        return entry
    if type(size) is not int or not 0 < size <= MAX_IMAGE_BYTES:
        raise UiEvidenceIndexError("validated image has no usable size")
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise UiEvidenceIndexError("validated image has no usable digest")
    entry.update(size=size, sha256=sha256)
    # Never trust a runner ``bound`` the worker's own rule cannot confirm.
    if (
        refusal != "unbound"
        and raw.get("bound") is True
        and isinstance(state, str)
        and _bindable(state, base_sha=base_sha, manifest=manifest)
    ):
        entry["bound"] = True
    else:
        entry["refusal"] = "unbound"
    return entry


def parse_index(raw: dict[str, object], *, base_sha: str) -> dict[str, Any] | None:
    """The worker-to-API index for the runner's index body, None when there is no manifest.

    Every binding is re-derived by the worker's rule. Raises
    ``UiEvidenceIndexError`` when the body breaks its wire shape.
    """

    body: dict[str, Any] = dict(raw)
    if body.get("version") != 1 or type(body.get("version")) is not int:
        raise UiEvidenceIndexError("unsupported index version")
    if body.get("base_sha") != base_sha:
        raise UiEvidenceIndexError("index answers for another base")
    state = body.get("manifest")
    if state == "absent":
        return None
    index: dict[str, Any] = dict.fromkeys(("manifest_refusal", "status", "reason"))
    index.update(dict.fromkeys(("candidate_dirty", *_ORACLE_KEYS)))
    index.update(version=1, manifest_state=state, base_sha=base_sha, warnings=[], images=[])
    if state == "invalid":
        refusal = body.get("manifest_refusal")
        if refusal not in _MANIFEST_REFUSALS:
            raise UiEvidenceIndexError("invalid manifest has no known refusal")
        index["manifest_refusal"] = refusal
        return index
    if state != "valid":
        raise UiEvidenceIndexError("unknown manifest state")
    status, reason, dirty = body.get("status"), body.get("reason"), body.get("candidate_dirty")
    if status not in _MANIFEST_STATUSES:
        raise UiEvidenceIndexError("manifest status is not a known value")
    if reason is not None and not isinstance(reason, str):
        raise UiEvidenceIndexError("manifest reason is not a string")
    if dirty is not None and not isinstance(dirty, bool):
        raise UiEvidenceIndexError("candidate_dirty is not a boolean")
    index.update(status=status, reason=reason, candidate_dirty=dirty)
    for key in _ORACLE_KEYS:
        value = body.get(key)
        if value is not None and (not isinstance(value, str) or _OID.fullmatch(value) is None):
            raise UiEvidenceIndexError(f"{key} is not an object id")
        index[key] = value
    # The head tree speaks for the candidate only for a checkout declared
    # exactly clean at a named head commit.
    if dirty is not False or index["manifest_head_sha"] is None:
        index["head_tree"] = None
    images = body.get("images")
    if not isinstance(images, list):
        raise UiEvidenceIndexError("images is not a list")
    index["images"] = [
        _image(image, position, base_sha=base_sha, manifest=index)
        for position, image in enumerate(images)
    ]
    return index


# --- Capture -------------------------------------------------------------------


async def _fetch_image(
    runner: RunnerClient, base_url: str, token: str | None, image: dict[str, Any], deadline: float
) -> IO[bytes]:
    handle = tempfile.TemporaryFile()  # noqa: SIM115 - ownership passes to the capture
    try:
        await runner.ui_evidence_image(
            base_url,
            token,
            index=image["ordinal"],
            sha256=image["sha256"],
            size=image["size"],
            sink=handle,
            remaining_s=max(0.0, deadline - asyncio.get_running_loop().time()) + _HTTP_GRACE_S,
        )
        handle.flush()
        handle.seek(0)
    except BaseException:
        handle.close()
        raise
    return handle


async def capture_ui_evidence(
    runner: RunnerClient,
    base_url: str,
    token: str | None,
    *,
    base_sha: str,
    remaining_s: float | None,
    execution_deadline: datetime | None,
) -> UiEvidenceCapture | None:
    """Copy this run's bound evidence images out of the sandbox, best effort.

    Bounded to ``CAPTURE_BUDGET_S`` cut by ``_evidence_budget`` (the earlier of
    ``remaining_s``, the delivery time left, and ``execution_deadline``, less
    one API write budget and the settle floor) and skipped when that is 5 s or
    less. Returns
    None when there is nothing to relay (no manifest, an unreachable or older
    runner, or a malformed index). Never
    raises except ``asyncio.CancelledError``, after closing its files.
    """

    budget = _evidence_budget(float(CAPTURE_BUDGET_S), remaining_s, execution_deadline)
    if budget <= _MIN_CAPTURE_S:
        logger.info("ui evidence capture skipped: the attempt budget is spent")
        return None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget
    capture: UiEvidenceCapture | None = None
    pending: list[dict[str, Any]] = []
    try:
        async with asyncio.timeout_at(deadline):
            raw = await runner.ui_evidence_index(
                base_url,
                token,
                base_sha=base_sha,
                budget_s=budget,
                remaining_s=budget + _HTTP_GRACE_S,
            )
            index = parse_index(raw, base_sha=base_sha)
            if index is None:
                logger.info("ui evidence capture: the run left no evidence manifest")
                return None
            capture = UiEvidenceCapture(index=index)
            total = 0
            for image in index["images"]:
                if image["refusal"] is not None or not image["bound"]:
                    continue
                if total + image["size"] > MAX_TOTAL_BYTES:
                    image["refusal"] = "total_limit"
                    continue
                total += image["size"]
                pending.append(image)
            for image in pending:
                try:
                    handle = await _fetch_image(runner, base_url, token, image, deadline)
                except Exception as exc:  # noqa: BLE001 - one image, best effort
                    logger.warning(
                        "ui evidence image %s could not be copied: %s",
                        image["ordinal"],
                        type(exc).__name__,
                    )
                    image["refusal"] = "transfer_failed"
                    continue
                capture.files[image["ordinal"]] = handle
    except asyncio.CancelledError:
        if capture is not None:
            capture.close()
        raise
    except Exception as exc:  # noqa: BLE001 - evidence never fails the publication
        timed_out = isinstance(exc, TimeoutError)
        if capture is None:
            if timed_out:
                logger.warning("ui evidence capture timed out before the index was read")
            else:
                logger.warning("ui evidence capture unavailable: %s", type(exc).__name__)
            return None
        capture.index["warnings"].append(
            "capture_timeout" if timed_out else f"capture_failed:{type(exc).__name__}"
        )
        for image in pending:
            if image["refusal"] is None and image["ordinal"] not in capture.files:
                image["refusal"] = "timeout" if timed_out else "transfer_failed"
    return capture


# --- Relay ---------------------------------------------------------------------


async def relay_ui_evidence(
    creator: object,
    publication_id: str,
    capture: UiEvidenceCapture | None,
    *,
    remaining_s: float | None,
    execution_deadline: datetime | None,
) -> None:
    """Upload ``capture`` for ``publication_id``, best effort, then close it.

    Bounded to ``UPLOAD_BUDGET_S`` cut by ``_evidence_budget`` (``remaining_s``
    is the delivery lease's remaining time) and skipped when nothing is left.
    A None capture or a creator without ``upload_ui_evidence`` sends nothing. Never raises except
    ``asyncio.CancelledError``; the capture's files are closed on every path.
    """

    if capture is None:
        return
    try:
        upload = getattr(creator, "upload_ui_evidence", None)
        if upload is None:
            return
        budget = _evidence_budget(float(UPLOAD_BUDGET_S), remaining_s, execution_deadline)
        if budget <= 0:
            logger.info("ui evidence upload skipped: the delivery budget is spent")
            return
        async with asyncio.timeout(budget):
            await upload(publication_id, capture, budget_s=budget)
    except Exception as exc:  # noqa: BLE001 - evidence never fails the publication
        code = exc.code if isinstance(exc, UiEvidenceUploadError) else type(exc).__name__
        logger.warning("ui evidence upload for publication %s stopped: %s", publication_id, code)
    finally:
        capture.close()


async def upload_capture(
    client: httpx.AsyncClient,
    publication_url: str,
    worker_headers: dict[str, str],
    publication_id: str,
    capture: UiEvidenceCapture,
    *,
    budget_s: float,
) -> None:
    """Upload ``capture`` through the API's three worker-token evidence routes.

    Every call is idempotent on the API, so transport faults and 5xx are
    retried with a short backoff. A 4xx stops the upload.
    """

    base = f"{publication_url}/{quote(publication_id, safe='')}/ui-evidence"
    headers = dict(worker_headers)
    inject_trace_context(headers)
    json_headers = {**headers, "Content-Type": "application/json"}

    async def send(
        method: str, url: str, extra: dict[str, str], content: Callable[[], Any]
    ) -> httpx.Response:
        attempt = 0
        while True:
            try:
                response = await client.request(
                    method, url, content=content(), headers=extra, follow_redirects=False
                )
            except httpx.TransportError:
                if attempt >= len(_UPLOAD_BACKOFF_S):
                    raise
            else:
                if response.status_code not in _RETRY_STATUSES or attempt >= len(_UPLOAD_BACKOFF_S):
                    return response
            await asyncio.sleep(_UPLOAD_BACKOFF_S[attempt])
            attempt += 1

    async with asyncio.timeout(budget_s):
        body = json.dumps(capture.index, separators=(",", ":")).encode()
        response = await send("PUT", base, json_headers, lambda: body)
        _require_success(response, "index")
        answer = _json_object(response)
        if answer.get("status") not in (None, "receiving"):
            return
        expected = answer.get("expected")
        for ordinal, handle in sorted(capture.files.items()):
            if isinstance(expected, list) and ordinal not in expected:
                continue
            image = capture.index["images"][ordinal]
            image_headers = {**headers, "Content-Type": "image/png"}
            image_headers["Content-Length"] = str(image["size"])
            image_headers["X-Curie-Evidence-Sha256"] = image["sha256"]
            url = f"{base}/images/{ordinal}"
            response = await send("PUT", url, image_headers, functools.partial(_chunks, handle))
            if response.status_code in _RETRY_STATUSES:
                # The seal records it missing; the other images still go.
                logger.warning(
                    "ui evidence image %s was not stored: HTTP %s", ordinal, response.status_code
                )
                continue
            _require_success(response, "image")
        response = await send("POST", f"{base}/seal", json_headers, lambda: b"{}")
        _require_success(response, "seal")


async def _chunks(handle: IO[bytes]) -> AsyncIterator[bytes]:
    handle.seek(0)
    while chunk := handle.read(_UPLOAD_CHUNK_BYTES):
        yield chunk


def _json_object(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _require_success(response: httpx.Response, step: str) -> None:
    status = response.status_code
    if status in (200, 201):
        return
    if 400 <= status < 500:
        detail = _json_object(response).get("detail")
        code = detail.get("code") if isinstance(detail, dict) else None
        suffix = code if isinstance(code, str) and len(code) <= 80 else str(status)
        raise UiEvidenceUploadError(f"upload_rejected:{suffix}")
    raise UiEvidenceUploadError(f"upload_failed:{step}:{status}")
