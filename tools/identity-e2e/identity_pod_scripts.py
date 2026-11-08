"""In-pod scripts the requester scenarios run inside the release's own containers.

Each reads its parameters as JSON on stdin and prints one JSON line, so no
token, key or proof ever travels on argv or reaches a log. They import only
the deployed product code of the container they run in: the API for the
answer proof, the worker for its sandbox route store, and a hosted
connector's caller proxy for replays.
"""

from __future__ import annotations

# ADR 0267: the signed email approval answer a mail adapter forwards with an
# email reply, under the same receiver key as the invocation proof.
ANSWER_SIGN_SCRIPT = """
import json, sys
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from curie_internal.sandbox_token import b64url_decode
P = json.load(sys.stdin)
proof = wire.EmailApprovalAnswerProofV1.model_validate_json(json.dumps(P["proof"]))
ring = codec.build_identity_keyring(
    "receiver", ((P["proof"]["receiver_id"], P["proof"]["receiver_generation"],
                  b64url_decode(P["key"])),),
)
print(json.dumps({"proof": wire.sign_email_approval_answer_proof(proof, ring)}))
"""

# Runs in the worker container. Finds the one sandbox route record naming
# P["claim"] under the substrate's route keys (sandbox.affinity: the default
# sandbox key prefix, then ":route:<thread key>") and, unless P["only_if"] is
# given and differs from the stored caller_token_exp, rewrites only that field
# with the record's own serialization and TTL. The record carries the runner's
# bearer; nothing printed names it.
ROUTE_DOCTOR_SCRIPT = """
import json, sys
import redis
from curie_internal.keyspace import SANDBOX_KEY_PREFIX_DEFAULT
from curie_worker.config import WorkerConfig

P = json.load(sys.stdin)


def summary(key, record):
    requester = record.get("caller_requester")
    return {
        "thread_key": key.split(":route:", 1)[-1],
        "claim_name": record.get("claim_name"),
        "state": record.get("state"),
        "caller_token_exp": record.get("caller_token_exp"),
        "state_token_exp": record.get("state_token_exp"),
        "requester_kind": None if requester is None else str(requester).split(":", 1)[0],
    }


def main():
    client = redis.Redis(**WorkerConfig().valkey_client_kwargs(), socket_timeout=10)
    prefix = SANDBOX_KEY_PREFIX_DEFAULT + ":route:"
    keys = []
    for key in client.scan_iter(match=prefix + "*", count=1000):
        raw = client.get(key)
        try:
            record = json.loads(raw) if raw is not None else None
        except ValueError:
            continue
        if isinstance(record, dict) and record.get("claim_name") == P["claim"]:
            keys.append(key)
    if len(keys) != 1:
        return {"found": len(keys), "changed": False}
    key = keys[0]
    with client.pipeline() as pipe:
        try:
            pipe.watch(key)
            raw = pipe.get(key)
            record = json.loads(raw) if raw is not None else None
            if not isinstance(record, dict) or record.get("claim_name") != P["claim"]:
                return {"found": 0, "changed": False}
            before = summary(key, record)
            only_if = P.get("only_if")
            if only_if is not None and record.get("caller_token_exp") != only_if:
                return {"found": 1, "changed": False, "before": before, "after": before}
            record["caller_token_exp"] = P["caller_token_exp"]
            pipe.multi()
            pipe.set(key, json.dumps(record, sort_keys=True), keepttl=True)
            pipe.execute()
        except redis.WatchError:
            return {"found": 1, "changed": False, "conflict": True}
    return {"found": 1, "changed": True, "before": before, "after": summary(key, record),
            "ttl_s": client.ttl(key)}


try:
    result = main()
except Exception as exc:
    result = {"error": type(exc).__name__}
print(json.dumps(result))
"""

# Runs in a hosted connector pod's caller-proxy container, with the caller
# token on stdin. Without P["clock"] it POSTs an MCP initialize to the live
# proxy on its loopback listen port. With P["clock"] (unix seconds) it builds the
# deployed proxy from this container's own env, on a free loopback port, with
# that frozen clock, and sends the same request to it. Either way it answers the
# HTTP status and, for a proxy refusal, its reason and whether the body is the
# frozen refusal body. The token is never printed.
PROXY_REPLAY_SCRIPT = """
import asyncio, json, os, socket, sys

P = json.load(sys.stdin)
INITIALIZE = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "identity-e2e-replay", "version": "0"}},
}).encode()


async def post(port, token):
    from aiohttp import ClientSession, ClientTimeout
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "X-Curie-Caller": token,
    }
    async with ClientSession(timeout=ClientTimeout(total=20)) as session:
        async with session.post(
            f"http://127.0.0.1:{port}/mcp", data=INITIALIZE, headers=headers
        ) as answer:
            return answer.status, await answer.read()


async def controlled(token, clock):
    from aiohttp import web
    from curie_connector_proxy.server import LISTEN_PORT_ENV, ProxyConfig, make_app
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = dict(os.environ)
    env[LISTEN_PORT_ENV] = str(port)
    runner = web.AppRunner(make_app(ProxyConfig.from_env(env), clock=lambda: clock))
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()
        return await post(port, token)
    finally:
        await runner.cleanup()


def judged(status, raw):
    from curie_connector_proxy import caller
    from curie_connector_proxy.server import refusal_body
    out = {"status": status, "reason": None, "frozen_refusal_body": None}
    if status == 403:
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        error = body.get("error") if isinstance(body, dict) else None
        data = error.get("data") if isinstance(error, dict) else None
        reason = data.get("curie_caller") if isinstance(data, dict) else None
        if isinstance(reason, str) and reason in caller.REFUSALS:
            out["reason"] = reason
            out["frozen_refusal_body"] = body == refusal_body(reason)
    out["admitted"] = out["reason"] is None
    return out


def main():
    token, clock = P["token"], P.get("clock")
    if clock is None:
        port = int(os.environ["CURIE_CALLER_PROXY_PORT"])
        return {"mode": "live", **judged(*asyncio.run(post(port, token)))}
    return {"mode": "controlled_clock", "clock": clock,
            **judged(*asyncio.run(controlled(token, int(clock))))}


try:
    result = main()
except Exception as exc:
    result = {"error": type(exc).__name__}
print(json.dumps(result))
"""
