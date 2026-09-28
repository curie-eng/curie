"""One bounded publication decision shared by all observations of a tool call."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import anyio
from aci_protocol import PublicationContext

from .workspace_snapshot import capture_workspace_snapshot

PRECHECK_TIMEOUT_SECONDS = 10
_MAX_PRECHECKS = 5
_MAX_RESPONSE_BYTES = 4096
_UNAVAILABLE = (
    "precheck_unavailable: Publication could not be verified. No approval was created. "
    "Make a working tree file change or retry with fresh execution context."
)


class _MalformedPublication(ValueError):
    """A malformed proposal retains the existing halting gate behavior."""


def _origin(url: str | None) -> tuple[str, str, int] | None:
    if not url:
        return None
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            return None
        return parsed.scheme, parsed.hostname, parsed.port or (
            443 if parsed.scheme == "https" else 80
        )
    except ValueError:
        return None


@dataclass
class _Call:
    tool_input: dict[str, Any]
    ready: anyio.Event = field(default_factory=anyio.Event)
    refusal: str | None = _UNAVAILABLE
    malformed: ValueError | None = None
    conflicted: bool = False


class PublicationPrecheck:
    """Own active Event authority and coalesce exact SDK call identities.

    The boot configuration is immutable for this object's lifetime. Binding a
    new Event replaces all decisions and read accounting, including on a warm
    runner. No capability is rendered into a model response or exception.
    """

    def __init__(
        self, workspace: Path | None, trusted_url: str | None, *, network_enabled: bool
    ) -> None:
        self._workspace = workspace
        self._trusted_origin = _origin(trusted_url)
        self._network_enabled = network_enabled
        self._context: PublicationContext | None = None
        self._active = False
        self._calls: dict[str, _Call] = {}
        self._attempts = 0
        self.refused_ids: set[str | None] = set()

    def bind(self, context: PublicationContext | None) -> None:
        self._context = context.model_copy(deep=True) if context is not None else None
        self._active = True
        self._calls.clear()
        self._attempts = 0
        self.refused_ids.clear()

    def clear(self) -> None:
        self._context = None
        self._active = False
        self._calls.clear()
        self._attempts = 0
        self.refused_ids.clear()

    async def decide(self, tool_use_id: str | None, tool_input: dict[str, Any]) -> str | None:
        if not self._active:
            return _UNAVAILABLE
        context = self._context
        if context is None:
            return None
        if not isinstance(tool_use_id, str) or not tool_use_id.strip():
            self.refused_ids.add(tool_use_id)
            return _UNAVAILABLE
        call = self._calls.get(tool_use_id)
        if call is not None:
            if call.tool_input != tool_input:
                call.conflicted = True
                self.refused_ids.add(tool_use_id)
                return _UNAVAILABLE
            await call.ready.wait()
        else:
            call = _Call(copy.deepcopy(tool_input))
            self._calls[tool_use_id] = call
            try:
                with anyio.fail_after(PRECHECK_TIMEOUT_SECONDS):
                    call.refusal = await self._compare(context, tool_input)
            except _MalformedPublication as exc:
                call.malformed = exc
            except Exception:
                # Never include a transport diagnostic containing the endpoint
                # or its credential in model text, transcripts or logs.
                call.refusal = _UNAVAILABLE
            finally:
                call.ready.set()
        if self._context is not context or not self._active:
            return _UNAVAILABLE
        refusal: str | None
        if call.conflicted:
            refusal = _UNAVAILABLE
        else:
            if call.malformed is not None:
                raise call.malformed
            refusal = call.refusal
        if refusal is not None:
            self.refused_ids.add(tool_use_id)
        return refusal

    async def _compare(
        self, context: PublicationContext, tool_input: dict[str, Any]
    ) -> str | None:
        body = tool_input.get("body")
        if body is None or (isinstance(body, str) and not body.strip()):
            return "body_required: Provide a useful nonblank publication body and retry."
        title = tool_input.get("title")
        if not isinstance(title, str) or not title.strip() or len(title) > 240:
            raise _MalformedPublication("publication title must be 1 to 240 characters")
        if not isinstance(body, str) or len(body) > 65_536:
            raise _MalformedPublication("publication body must be at most 65536 characters")
        if (
            self._trusted_origin is None
            or _origin(context.precheck_url) != self._trusted_origin
        ):
            return _UNAVAILABLE
        if self._workspace is None:
            return "precheck_unavailable: The managed workspace is unavailable."
        try:
            snapshot = await anyio.to_thread.run_sync(
                capture_workspace_snapshot, self._workspace, abandon_on_cancel=True
            )
        except Exception:
            return (
                "precheck_unavailable: The workspace snapshot could not be captured. "
                "Correct the working tree and retry."
            )
        if snapshot.base_sha != context.expected_head:
            return (
                "head_changed: Local HEAD differs from the prepared head. Restore or "
                "refresh the checkout through the trusted workspace path before retrying."
            )
        if snapshot.patch:
            return None
        if not self._network_enabled:
            return _UNAVAILABLE
        if self._attempts >= _MAX_PRECHECKS:
            return (
                "rate_limited: This turn has used its five publication prechecks. "
                "Make a working tree file change before requesting publication again."
            )
        self._attempts += 1
        payload = {
            "observed_title": context.observed_title,
            "observed_body_sha256": context.observed_body_sha256,
            "observed_at": context.observed_at.isoformat(),
            "proposed_title": title.strip(),
            "proposed_body": body,
        }
        async with (
            aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=PRECHECK_TIMEOUT_SECONDS),
                trust_env=False,
            ) as client,
            client.post(
                context.precheck_url,
                json=payload,
                headers={"X-Curie-Publication-Precheck": context.capability},
                allow_redirects=False,
            ) as response,
        ):
            raw = await response.content.read(_MAX_RESPONSE_BYTES + 1)
            if len(raw) > _MAX_RESPONSE_BYTES:
                return _UNAVAILABLE
            try:
                result = json.loads(raw)
            except (ValueError, UnicodeError):
                return _UNAVAILABLE
            if not isinstance(result, dict):
                return _UNAVAILABLE
            detail = result.get("detail")
            if (
                response.status == 409
                and isinstance(detail, dict)
                and detail.get("code") == "stale_context"
            ):
                return (
                    "stale_context: Pull request metadata or execution authority changed. "
                    "Preserve external edits and obtain a fresh trusted Event."
                )
            if response.status == 429:
                return (
                    "rate_limited: Publication prechecks are temporarily exhausted. "
                    "Make a working tree file change or retry later."
                )
            if response.status != 200:
                return _UNAVAILABLE
            if result.get("result") == "unchanged":
                return (
                    "no_change: Neither files nor pull request metadata changed. "
                    "Make a working tree file change before requesting publication."
                )
            if result.get("result") == "metadata_changed":
                return None
            return _UNAVAILABLE
