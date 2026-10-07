"""The worker's client for the action ledger (ADR-0117).

The worker has no database of its own, so recording what a turn did to the world
is an HTTP call to the platform API, exactly as creating an approval is. This
covers the two calls one side-effecting tool call produces and the failure that
must not be swallowed.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from aci_protocol import SideEffectFlag
from curie_worker.actions import ActionBackendError, ActionClient

pytestmark = pytest.mark.anyio


def _client(handler: Any) -> tuple[httpx.AsyncClient, list[dict[str, Any]]]:
    seen: list[dict[str, Any]] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "method": request.method,
                "path": request.url.path,
                "body": json.loads(request.content) if request.content else None,
                "api_key": request.headers.get("X-API-Key"),
            }
        )
        return handler(request)

    return httpx.AsyncClient(transport=httpx.MockTransport(wrapped)), seen


async def test_recording_a_call_sends_its_arguments_and_a_dedupe_key() -> None:
    client, seen = _client(lambda _r: httpx.Response(201, json={"id": "a1", "status": "pending"}))

    async with client:
        recorded = await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                arguments={"replicas": 10},
                detail="non-idempotent tool executed",
            ),
            event_id="event-1",
            conversation_id="C1",
            agent_id=None,
        )

    assert recorded.id == "a1"
    assert seen[0]["method"] == "POST"
    assert seen[0]["path"] == "/actions"
    assert seen[0]["api_key"] == "k"
    # The event id AND the call id: one turn can call the same tool twice, and a
    # redelivery of that turn must adopt both rows rather than collapse them.
    assert seen[0]["body"]["dedupe_key"] == "event-1:toolu_01"
    assert seen[0]["body"]["arguments"] == {"replicas": 10}


async def test_a_redelivered_record_is_not_an_error() -> None:
    """200 is the API's idempotent replay; only a non-2xx is a failure."""

    client, _ = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "pending"}))

    async with client:
        recorded = await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
            SideEffectFlag(tool="t", call_id="c", arguments={}),
            event_id="e",
            conversation_id="C1",
            agent_id=None,
        )

    assert recorded.id == "a1"


_ENVELOPE = {
    "sealed": "curie.snapshot.v1",
    "kid": "example-key-2026-10",
    "ciphertext": "ZXhhbXBsZSBzZWFsZWQgc25hcHNob3QgYnl0ZXMgMDE=",
}


async def test_completing_a_call_forwards_the_sealed_prior_state_a_restore_replays() -> None:
    """`prior`, `version` and `target` come out of the CONNECTOR's reply, not the arguments.

    No function of `replicas=10` can produce the replica count from before the
    call. That is why the reply is the declaration (ADR-0117 decision 1).
    @spec ACTION-EXECUTOR-9: the restorable prior is a sealed envelope, and the
    version the call left is recorded as ``post_version``.
    """

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                failed=False,
                result={
                    "ok": True,
                    "prior": _ENVELOPE,
                    "version": "rv-1041",
                    "post": {"spec": {"replicas": 10}},
                    "target": {"kind": "Deployment", "name": "api"},
                },
                detail="non-idempotent tool completed",
            ),
        )

    assert seen[0]["path"] == "/actions/a1/complete"
    assert seen[0]["body"]["prior_state"] == _ENVELOPE
    assert seen[0]["body"]["post_version"] == "rv-1041"
    # What the call LEFT, readable beside the opaque version. It cannot be
    # derived from `replicas=10`: a PATCH's result is not its request body.
    assert seen[0]["body"]["post_state"] == {"spec": {"replicas": 10}}
    assert seen[0]["body"]["target"] == {"kind": "Deployment", "name": "api"}
    assert seen[0]["body"]["failed"] is False


async def test_a_cleartext_prior_state_is_history_not_a_restorable_snapshot() -> None:
    """@spec ACTION-EXECUTOR-9: a cleartext ``prior`` stays in ``result`` only.

    The reply is still forwarded whole, so the record keeps the history of the
    call, and ``target`` and ``post`` still come from the reply, not the
    arguments; but nothing restorable is recorded.
    """

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))
    result = {
        "ok": True,
        "prior": {"spec": {"replicas": 3}},
        "version": "rv-1041",
        "post": {"spec": {"replicas": 10}},
        "target": {"kind": "Deployment", "name": "api"},
    }

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                failed=False,
                result=result,
                detail="non-idempotent tool completed",
            ),
        )

    body = seen[0]["body"]
    assert body["result"] == result
    assert body["prior_state"] is None
    assert body["post_version"] is None
    assert body["post_state"] == {"spec": {"replicas": 10}}
    assert body["target"] == {"kind": "Deployment", "name": "api"}
    assert body["failed"] is False


async def test_a_prose_reply_completes_with_nothing_to_restore() -> None:
    """The connector answered in a sentence, so the result is absent entirely."""

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="restart", call_id="c", failed=False, detail="restarted"),
        )

    assert seen[0]["body"]["result"] is None
    assert seen[0]["body"]["prior_state"] is None
    assert seen[0]["body"]["post_state"] is None
    assert seen[0]["body"]["target"] is None


async def test_a_structured_reply_without_a_prior_is_still_not_undoable() -> None:
    """A connector may return JSON and still not report what it overwrote."""

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="t", call_id="c", failed=False, result={"ok": True}),
        )

    assert seen[0]["body"]["result"] == {"ok": True}
    assert seen[0]["body"]["prior_state"] is None


async def test_a_redacted_snapshot_never_produces_an_undoable_action() -> None:
    """A scrubbed prior state is not a restore; it is a placeholder a restore would write.

    The runner redacts a held secret or a token-shaped value inside the result
    before the frame leaves the sandbox, and says so. Forwarding that snapshot
    would give the ledger a row that looks undoable and whose undo sets the
    resource's value to ``[REDACTED:...]`` (#1873). With neither state recorded,
    the row is not undoable and the receipt says so.
    """

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))
    result = {
        "summary": "rotated acme-api's token",
        "prior": {"env": [{"name": "API_TOKEN", "value": "[REDACTED:held_secret]"}]},
        "post": {"env": [{"name": "API_TOKEN", "value": "[REDACTED:held_secret]"}]},
        "target": {"kind": "Deployment", "name": "acme-api"},
    }

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="set_env", call_id="c", failed=False, result=result, redacted=True),
        )

    body = seen[0]["body"]
    assert body["prior_state"] is None
    assert body["post_state"] is None
    # What the call acted on and what it said stay on the record: they are what a
    # person reads, and neither is replayed.
    assert body["target"] == {"kind": "Deployment", "name": "acme-api"}
    assert body["result"] == result
    assert body["failed"] is False


async def test_a_refused_write_is_raised_not_swallowed() -> None:
    """A record the platform failed to write is a hole in its account of a change.

    The same branch already fails the turn when the no-retry marker cannot be
    persisted; losing the record of WHAT changed is not the lesser failure.
    """

    client, _ = _client(lambda _r: httpx.Response(500, text="nope"))

    async with client:
        with pytest.raises(ActionBackendError):
            await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
                SideEffectFlag(tool="t", call_id="c"),
                event_id="e",
                conversation_id="C1",
                agent_id=None,
            )


# -- ACTION-EXECUTOR-12: the connector digest rides the completion -------------
#
# The digest-attributing recorder wrapper hands its verdict to this client as
# keyword arguments, so the kernel's two-argument ``complete(action_id, frame)``
# keeps working unchanged.

_DIGEST = "sha256:" + "ab" * 32


async def _complete_with(**kwargs: Any) -> dict[str, Any]:
    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1"}))
    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="mcp__grafana__scale", call_id="c", result={"ok": True}),
            **kwargs,
        )
    assert [s["path"] for s in seen] == ["/actions/a1/complete"]
    body: dict[str, Any] = seen[0]["body"]
    return body


async def test_completion_forwards_the_connector_and_its_digest() -> None:
    """@spec ACTION-EXECUTOR-12."""

    body = await _complete_with(connector="grafana", connector_digest=_DIGEST)

    assert body["connector"] == "grafana"
    assert body["connector_digest"] == _DIGEST
    assert body["result"] == {"ok": True}


async def test_completion_without_attribution_sends_none() -> None:
    """@spec ACTION-EXECUTOR-12: omitted or null, never a placeholder."""

    for body in (
        await _complete_with(connector=None, connector_digest=None),
        await _complete_with(),
    ):
        assert body.get("connector") is None
        assert body.get("connector_digest") is None


async def test_an_attributed_completion_carries_the_worker_token() -> None:
    """@spec ACTION-EXECUTOR-12: the API takes attribution only under the worker token.

    The platform key still rides along; the token is what lets the completion
    carry ``connector``/``connector_digest`` at all.
    """

    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "path": request.url.path,
                "api_key": request.headers.get("X-API-Key"),
                "worker_token": request.headers.get("X-Curie-Worker-Token"),
                "body": json.loads(request.content),
            }
        )
        return httpx.Response(200, json={"id": "a1"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await ActionClient(
            api_base_url="http://api", api_key="k", client=http, worker_token="wt"
        ).complete(
            "a1",
            SideEffectFlag(tool="mcp__grafana__scale", call_id="c", result={"ok": True}),
            connector="grafana",
            connector_digest=_DIGEST,
        )

    assert seen[0]["path"] == "/actions/a1/complete"
    assert seen[0]["body"]["connector_digest"] == _DIGEST
    assert seen[0]["worker_token"] == "wt"
    assert seen[0]["api_key"] == "k"


# -- ACTION-EXECUTOR-12: a refused attribution costs only the digest -----------
#
# Attribution is optional metadata. If the API refuses it (403: worker token
# mismatch, unset or mid-rotation; 422: tool-prefix mismatch or a tightened
# validator), the refusal stored nothing, so the client re-posts the same
# completion once without the pair. The action completes with a null digest and
# the turn does not fail. A refusal of a plain completion is the ledger failing
# and still raises, exactly as before.


def _refusing_attribution(status: int, *, plain_status: int = 200) -> tuple[Any, list[Any]]:
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(
            {
                "path": request.url.path,
                "body": body,
                "worker_token": request.headers.get("X-Curie-Worker-Token"),
            }
        )
        if "connector" in body or "connector_digest" in body:
            return httpx.Response(status, json={"detail": "refused"})
        if plain_status != 200:
            return httpx.Response(plain_status, json={"detail": "refused"})
        return httpx.Response(200, json={"id": "a1", "status": "succeeded", **body})

    return handler, seen


async def _attributed_complete(handler: Any) -> dict[str, Any]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await ActionClient(
            api_base_url="http://api", api_key="k", client=http, worker_token="wt"
        ).complete(
            "a1",
            SideEffectFlag(tool="mcp__grafana__scale", call_id="c", result={"ok": True}),
            connector="grafana",
            connector_digest=_DIGEST,
        )


@pytest.mark.parametrize("status", [403, 422])
async def test_a_refused_attribution_reposts_once_without_the_pair(status: int) -> None:
    handler, seen = _refusing_attribution(status)

    row = await _attributed_complete(handler)

    assert [s["path"] for s in seen] == ["/actions/a1/complete"] * 2
    first, second = seen[0]["body"], seen[1]["body"]
    assert (first["connector"], first["connector_digest"]) == ("grafana", _DIGEST)
    assert second.get("connector") is None
    assert second.get("connector_digest") is None
    # Everything else is the same completion.
    rest = {k: v for k, v in first.items() if k not in ("connector", "connector_digest")}
    assert {k: v for k, v in second.items() if v is not None} == {
        k: v for k, v in rest.items() if v is not None
    }
    # The ledger's row comes back: completed, with no attribution.
    assert row["id"] == "a1"
    assert row["status"] == "succeeded"
    assert row.get("connector") is None and row.get("connector_digest") is None


@pytest.mark.parametrize("status", [403, 422])
async def test_a_repost_that_is_refused_too_surfaces_and_is_not_retried_again(
    status: int,
) -> None:
    """The fallback is the plain completion; its refusal is the ledger failing."""

    handler, seen = _refusing_attribution(status, plain_status=status)

    with pytest.raises(ActionBackendError):
        await _attributed_complete(handler)

    assert len(seen) == 2


@pytest.mark.parametrize("status", [403, 422, 503])
async def test_a_refused_plain_completion_still_raises_without_a_retry(status: int) -> None:
    handler, seen = _refusing_attribution(200, plain_status=status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(ActionBackendError):
            await ActionClient(
                api_base_url="http://api", api_key="k", client=http, worker_token="wt"
            ).complete("a1", SideEffectFlag(tool="t", call_id="c", result={"ok": True}))

    assert len(seen) == 1


async def test_a_server_error_on_an_attributed_completion_is_not_a_refusal() -> None:
    """Only 403 and 422 mean 'refused, nothing stored'; a 5xx still surfaces."""

    handler, seen = _refusing_attribution(503)

    with pytest.raises(ActionBackendError):
        await _attributed_complete(handler)

    assert len(seen) == 1
