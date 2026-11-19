"""A binding address may contain a slash: a GitHub channel's ``owner/name``.

The state routes name a binding as ``/state/bindings/{kind}/{address}/...``.
A real ASGI server decodes the request path once before it routes, so an
address sent as one percent-encoded segment (``owner%2Fname``) arrives split in
two: a list request landed on the single-key route and looked up the binding
``github:owner`` (404), and a write matched no route at all (404). The runner
reads a 404 list as an empty namespace, so a GitHub channel's memory booted
empty with no warning.

Callers therefore escape ``%`` and ``/`` in the address before encoding it as a
path segment, and the API decodes the segment once more
(``routers/state.py`` ``_binding_address``). These checks run against the real
state router and the disposable Postgres the conftest provisions; nothing is
mocked. Every request goes through ``_send``, which hands the app the scope a
real server builds from the bytes on the wire, because Starlette's TestClient
(pinned 1.6.0) decodes the path twice where a real server decodes once (see
``test_transcript_identity._url``).
"""

from __future__ import annotations

import json
import secrets
from typing import Any
from urllib.parse import quote, unquote

import anyio
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint

_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time

REPO = "acme-corp/widgets"
BINDING = f"github:{REPO}"
SENDER = "github:1000001:example-user"
FACT = "fact-" + "0123456789abcdef" * 2
MAIL = "agent@example.test"


def _segment(address: str) -> str:
    # What the worker (binding_state_path) and the CLI (binding_address_segment)
    # put on the wire: only ``%`` and ``/`` escaped, then one path-segment quote.
    return quote(address.replace("%", "%25").replace("/", "%2F"), safe="")


def _old_segment(address: str) -> str:
    # What a worker or CLI built before this change put on the wire.
    return quote(address, safe="")


def _send(
    client: Any,
    method: str,
    wire_path: str,
    headers: dict[str, str],
    body: Any = None,
) -> tuple[int, Any]:
    """Drive the app with the scope a real server builds for ``wire_path``.

    uvicorn keeps the request target's bytes as ``raw_path`` and decodes them
    once into ``path``, which is what Starlette routes on. The app runs on the
    TestClient's portal, so its lifespan state and engine are the fixture's.
    """

    payload = b"" if body is None else json.dumps(body).encode()
    raw_headers = [(b"host", b"testserver")]
    raw_headers += [(key.lower().encode(), value.encode()) for key, value in headers.items()]
    if body is not None:
        raw_headers += [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
        ]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": unquote(wire_path),
        "raw_path": wire_path.encode("ascii"),
        "root_path": "",
        "query_string": b"",
        "headers": raw_headers,
        "client": ("2001:db8::" + secrets.token_hex(2), 5000),
        "server": ("testserver", 80),
        "state": client.app_state.copy(),
    }
    messages: list[dict[str, Any]] = []

    async def run() -> None:
        done = anyio.Event()
        delivered = False

        async def receive() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": payload, "more_body": False}
            await done.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            messages.append(message)
            if message["type"] == "http.response.body" and not message.get("more_body"):
                done.set()

        await client.app(scope, receive, send)

    client.portal.call(run)
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return status, (json.loads(raw) if raw else None)


def _binding_base(aid: str, address: str = REPO, kind: str = "github") -> str:
    return f"/agents/{aid}/state/bindings/{kind}/{_segment(address)}"


def _github_agent(client: Any, auth_headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={"name": "slash-address", "channel": {"kind": "github", "address": REPO}},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    return aid


def _token(aid: str, claims: dict[str, str | None]) -> dict[str, str]:
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def _fact_value(statement: str = "pull requests here target the release branch") -> Any:
    return {
        "statement": statement,
        "author": SENDER,
        "stated_at": "2026-10-01T00:00:00+00:00",
        "session_id": "sess-1",
    }


def test_channel_memory_round_trips_for_an_address_with_a_slash(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # What a factory claim on a GitHub repo does: the runner lists the
    # channel's memory at boot with the read credential, and the remember tool
    # writes a fact with the per-turn write credential for this binding.
    aid = _github_agent(client, auth_headers)
    memory = f"{_binding_base(aid)}/memory"
    assert memory.endswith("/github/acme-corp%252Fwidgets/memory")
    fact = f"{memory}/{FACT}"
    reader = _token(aid, {"binding": BINDING, "memory": "read"})
    writer = _token(aid, {"binding": BINDING, "memory": "write", "sender": SENDER, "turn": "evt-1"})

    status, body = _send(client, "GET", memory, reader)
    assert (status, body) == (200, [])

    status, body = _send(client, "PUT", fact, writer, {"value": _fact_value()})
    assert status == 200, body

    status, body = _send(client, "GET", memory, reader)
    assert status == 200, body
    assert [entry["key"] for entry in body] == [FACT]
    status, body = _send(client, "GET", fact, reader)
    assert status == 200, body
    assert body["value"]["statement"] == "pull requests here target the release branch"

    # The row is stored under the whole address, not the part before the slash.
    status, body = _send(client, "GET", _binding_base(aid), auth_headers)
    assert status == 200, body
    assert "memory" in {row["namespace"] for row in body}

    status, body = _send(client, "DELETE", fact, writer)
    assert status == 204, body
    assert _send(client, "GET", memory, reader) == (200, [])


def test_the_single_encoded_slash_address_still_splits_at_routing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The symptom itself, on the wire a pre-change worker sends: the server's
    # one decode turns ``%2F`` into a path separator before any route sees it.
    aid = _github_agent(client, auth_headers)
    wire = f"/agents/{aid}/state/bindings/github/{_old_segment(REPO)}/memory"
    assert wire.endswith("/github/acme-corp%2Fwidgets/memory")
    status, body = _send(client, "GET", wire, auth_headers)
    assert status == 404, body


def test_general_state_routes_accept_an_address_with_a_slash(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A memory=False agent's bundle composes /<namespace>/<key> onto the
    # binding base the worker hands it, so the put, append, read, list and
    # delete routes must all resolve the same binding.
    aid = _github_agent(client, auth_headers)
    base = _binding_base(aid)

    status, body = _send(client, "PUT", f"{base}/workflow/step", auth_headers, {"value": {"n": 1}})
    assert status == 200, body
    status, body = _send(client, "POST", f"{base}/log/runs/append", auth_headers, {"item": 1})
    assert status == 200, body

    status, body = _send(client, "GET", f"{base}/workflow/step", auth_headers)
    assert status == 200, body
    assert body["value"] == {"n": 1}
    status, body = _send(client, "GET", f"{base}/log", auth_headers)
    assert [e["key"] for e in body] == ["runs"]
    status, body = _send(client, "GET", base, auth_headers)
    assert {row["namespace"] for row in body} == {"workflow", "log"}
    assert _send(client, "DELETE", f"{base}/workflow/step", auth_headers)[0] == 204


def test_a_mail_address_reads_the_same_on_the_old_and_new_wire(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # An address with no ``%`` or ``/`` is sent byte for byte as before, so a
    # worker or CLI from before this change still reaches its binding here,
    # and a new one still reaches an API that decodes the segment only once.
    resp = client.post(
        "/agents",
        json={
            "name": "mail-address",
            "channel": {
                "kind": "email",
                "address": MAIL,
                "endpoint": "http://curie-mail-adapter:8080/",
                "adapter": "agentmail-sandbox",
            },
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid = resp.json()["id"]
    assert _segment(MAIL) == _old_segment(MAIL) == "agent%40example.test"
    base = _binding_base(aid, MAIL, kind="email")
    status, body = _send(client, "PUT", f"{base}/workflow/step", auth_headers, {"value": {"n": 2}})
    assert status == 200, body
    status, body = _send(client, "GET", f"{base}/workflow/step", auth_headers)
    assert (status, body["value"]) == (200, {"n": 2})


def test_a_slash_address_credential_is_still_held_to_its_own_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Decoding the address must not loosen ADR-0188. The 403 is decided
    # before the binding lookup, so it held before this change too; the reads
    # after it are what pin the decode.
    aid = _github_agent(client, auth_headers)
    other = _token(aid, {"binding": "github:acme-corp/other", "memory": "read"})
    status, body = _send(client, "GET", f"{_binding_base(aid)}/memory", other)
    assert status == 403, body
    assert "credential is scoped to another channel" in json.dumps(body)

    # The credential for this repo is let through to the same route.
    own = _token(aid, {"binding": BINDING, "memory": "read"})
    assert _send(client, "GET", f"{_binding_base(aid)}/memory", own) == (200, [])

    # An address the agent does not hold is a 404 naming the whole address.
    missing = f"{_binding_base(aid, 'acme-corp/other')}/memory"
    status, body = _send(client, "GET", missing, auth_headers)
    assert status == 404, body
    assert "github:acme-corp/other" in json.dumps(body)


def test_a_literal_percent_in_an_address_round_trips(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The one address shape whose wire form differs from before: a literal
    # ``%`` is escaped so the route's extra decode restores it unchanged.
    address = "acme-corp/100%25-done"
    resp = client.post(
        "/agents",
        json={"name": "percent-address", "channel": {"kind": "github", "address": address}},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid = resp.json()["id"]
    base = _binding_base(aid, address)
    assert base.endswith("/github/acme-corp%252F100%252525-done")
    status, body = _send(client, "PUT", f"{base}/workflow/step", auth_headers, {"value": {"n": 3}})
    assert status == 200, body
    status, body = _send(client, "GET", f"{base}/workflow/step", auth_headers)
    assert (status, body["value"]) == (200, {"n": 3})
