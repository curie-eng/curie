"""A shared fake of one state-API key (GET, POST .../append, PUT with CAS)."""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

TRANSCRIPT_CAP = 65_536
TRANSCRIPT_KEY = "/agents/A/state/transcript/t1"


def json_size(value: Any) -> int:
    """The compact-JSON byte size the state API caps a whole value at."""
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


class CappedCasState:
    """One state key on a fake state API with the real API's write rules.

    - whole-array compact-JSON cap on POST /append and on PUT (413);
    - optional ``reserve_bytes`` on append: 413 when the new array would leave
      fewer than that many bytes free under the cap;
    - a version bumped on every write, and CAS PUT with ``expected_version``
      (409 on mismatch);
    - every request recorded as ``(method, body, status)``;
    - the transcript cap advertised on GET unless ``advertise_cap`` is false.
    """

    def __init__(
        self,
        seed: list[dict[str, Any]] | None = None,
        *,
        key: str = TRANSCRIPT_KEY,
        max_bytes: int = TRANSCRIPT_CAP,
        advertise_cap: bool = True,
    ) -> None:
        self.key = key
        self.namespace, self.name = key.rsplit("/", 2)[-2:]
        self.value: list[dict[str, Any]] | None = list(seed) if seed else None
        self.max_bytes = max_bytes
        self.cap_header: str | None = str(max_bytes) if advertise_cap else None
        self.version = 1 if seed else 0
        self.requests: list[tuple[str, Any, int]] = []
        # Test hooks for interleavings and forced statuses.
        self.inject_after_first_get: dict[str, Any] | None = None
        self.concurrent_item_before_first_put: dict[str, Any] | None = None
        self.force_append_status: int | None = None
        self.force_put_status: int | None = None
        self._gets = 0
        self._puts = 0

    def methods(self) -> list[tuple[str, int]]:
        return [(method, status) for method, _body, status in self.requests]

    def rejections(self) -> list[tuple[str, int]]:
        """Refused writes; a 404 GET of an unwritten key is not a refusal."""
        return [(m, status) for m, status in self.methods() if m != "GET" and status >= 400]

    def _write(self, value: list[dict[str, Any]]) -> None:
        self.value = value
        self.version += 1

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "key": self.name,
            "value": list(self.value or []),
            "version": self.version,
        }

    def app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            self._gets += 1
            headers = (
                {"X-Curie-Transcript-Max-Bytes": self.cap_header}
                if self.cap_header is not None
                else {}
            )
            if self.value is None:
                self.requests.append(("GET", None, 404))
                return web.json_response({"detail": "not found"}, status=404, headers=headers)
            response = self._entry()
            self.requests.append(("GET", None, 200))
            if self._gets == 1 and self.inject_after_first_get is not None:
                # A concurrent writer lands right after this reader's load.
                self._write([*(self.value or []), self.inject_after_first_get])
                self.inject_after_first_get = None
            return web.json_response(response, headers=headers)

        async def append_key(request: web.Request) -> web.Response:
            body = await request.json()
            if self.force_append_status is not None:
                self.requests.append(("POST", body, self.force_append_status))
                return web.json_response({"detail": "forced"}, status=self.force_append_status)
            candidate = [*(self.value or []), body["item"]]
            size = json_size(candidate)
            reserve = body.get("reserve_bytes")
            if size > self.max_bytes:
                self.requests.append(("POST", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, over the {self.max_bytes}-byte cap"},
                    status=413,
                )
            if reserve is not None and self.max_bytes - size < int(reserve):
                self.requests.append(("POST", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, leaves under the {reserve}-byte reserve"},
                    status=413,
                )
            self._write(candidate)
            self.requests.append(("POST", body, 200))
            return web.json_response(self._entry())

        async def put_key(request: web.Request) -> web.Response:
            body = await request.json()
            self._puts += 1
            if self._puts == 1 and self.concurrent_item_before_first_put is not None:
                # Another writer appends between the compactor's GET and its PUT.
                self._write([*(self.value or []), self.concurrent_item_before_first_put])
                self.concurrent_item_before_first_put = None
            if self.force_put_status is not None:
                self.requests.append(("PUT", body, self.force_put_status))
                return web.json_response({"detail": "forced"}, status=self.force_put_status)
            expected = body.get("expected_version")
            if expected is not None and (self.value is None or expected != self.version):
                self.requests.append(("PUT", body, 409))
                return web.json_response(
                    {"detail": f"version mismatch: expected {expected}, stored {self.version}"},
                    status=409,
                )
            value = body["value"]
            size = json_size(value)
            if size > self.max_bytes:
                self.requests.append(("PUT", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, over the {self.max_bytes}-byte cap"},
                    status=413,
                )
            self._write(list(value))
            self.requests.append(("PUT", body, 200))
            return web.json_response(self._entry())

        app.router.add_get(self.key, get_key)
        app.router.add_post(f"{self.key}/append", append_key)
        app.router.add_put(self.key, put_key)
        return app
