"""In-pod scripts the identity scenarios run inside the release's own containers.

Each reads its parameters as JSON on stdin and prints one JSON line, so no
token, grant, ticket, key or proof ever travels on argv or reaches a log. They
import only the deployed product code of the container they run in: the API
for the answer proof, the worker for its sandbox route store and the grant
spend keys, and a hosted connector's caller proxy for replays and grant probes.
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

# Runs in a hosted connector pod's server container (never the caller proxy),
# with a principal header on stdin. It dials the server on the pod's own
# address, as a caller on the connector's -direct Service reaches it, so the
# request does not arrive on the caller proxy's loopback forward. It sends one
# MCP tools/call record_effect carrying the header and answers the HTTP status,
# the tool result's error flag and text, and whether its own source address
# was loopback. The header is never printed.
DIRECT_EFFECT_SCRIPT = """
import http.client, ipaddress, json, os, socket, sys

P = json.load(sys.stdin)


def main():
    address = socket.gethostbyname(socket.gethostname())
    if ipaddress.ip_address(address).is_loopback:
        return {"error": "no pod address"}
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "record_effect", "arguments": {"note": P["note"]}},
    }).encode()
    port = int(os.environ.get("PORT", "8000"))
    connection = http.client.HTTPConnection(address, port, timeout=20)
    try:
        connection.connect()
        source = connection.sock.getsockname()[0]
        connection.request("POST", "/mcp", body, {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "X-Curie-Principal": P["header"],
        })
        response = connection.getresponse()
        raw = response.read()
    finally:
        connection.close()
    result = json.loads(raw).get("result") or {}
    content = result.get("content") or [{}]
    return {
        "status": response.status,
        "is_error": bool(result.get("isError")),
        "text": content[0].get("text"),
        "source_loopback": ipaddress.ip_address(source).is_loopback,
    }


try:
    result = main()
except Exception as exc:
    result = {"error": type(exc).__name__}
print(json.dumps(result))
"""

# Runs in the worker container. Scans the connector grant spend keys the caller
# proxies write in Valkey (``connector-grant:<jti>``, SET NX EX by
# curie_connector_proxy.server._ValkeyGrantStore) with the worker's own Valkey
# client, and answers only the first 16 hex of each jti's sha256: the same
# fingerprint the reference connector logs for an attested grant. A jti is
# never printed.
SPEND_KEYS_SCRIPT = """
import hashlib, json, sys
import redis
from curie_worker.config import WorkerConfig

P = json.load(sys.stdin)
PREFIX = "connector-grant:"


def main():
    client = redis.Redis(**WorkerConfig().valkey_client_kwargs(), socket_timeout=10)
    found = set()
    for key in client.scan_iter(match=PREFIX + "*", count=1000):
        jti = str(key)[len(PREFIX):]
        found.add(hashlib.sha256(jti.encode("utf-8")).hexdigest()[:16])
    return {"fingerprints": sorted(found), "count": len(found)}


try:
    result = main()
except Exception as exc:
    result = {"error": type(exc).__name__}
print(json.dumps(result))
"""

# Runs in a hosted connector pod's caller-proxy container (the worker image),
# with the caller token, the install's caller signing seed and the run's
# selector seed on stdin. For each case in order it builds one JSON-RPC
# tools/call (or a two-element batch with a ping), adds the approval ticket the
# case asks for (signed with curie_internal.approver_ticket.sign_ticket, fresh
# 32-byte nonce, issued now, five minutes to expire, digest by
# execution_arguments_sha256, with the case's tampering), mints the case's
# grant with curie_worker.connector_grant.mint over the canonical arguments
# (fresh uuid4 jti, ten minutes to expire), or replays an earlier case's exact
# call and grant, and POSTs it to the live proxy on loopback with the case's
# forged headers. It answers per case only the HTTP status, the tool result's
# error flag and first text line, the proxy's refusal code and whether its body
# is the frozen one, whether the connection was dropped (the proxy's 502), the
# grant's jti fingerprint and the pod-clock window of the request. No token,
# grant, ticket, key or jti is printed.
GRANT_PROBE_SCRIPT = """
import base64, hashlib, http.client, json, os, sys, time, uuid

P = json.load(sys.stdin)
GRANT_HEADER = "X-Curie-Connector-Grant"
TEXT_LIMIT = 200


def b64url(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def fingerprint(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def ticket(arguments, tamper):
    from curie_internal.approver_ticket import execution_arguments_sha256, sign_ticket
    from nacl.signing import SigningKey
    now = int(time.time())
    digested = dict(arguments)
    if tamper == "digest":
        digested["note"] = str(digested.get("note", "")) + " (not the signed arguments)"
    payload = {
        "version": 1,
        "purpose": "approval_request",
        "tenant_id": P["tenant_id"],
        "agent_id": P["agent_id"],
        "connector": P["connector"],
        "tool": P["ticket_tool"],
        "route": P["other_route"] if tamper == "route" else P["route"],
        "key_id": P["key_id"],
        "requester": P["other_requester"] if tamper == "requester" else P["requester"],
        "nonce": b64url(os.urandom(32)),
        "issued_at": now,
        "expires_at": now + 300,
        "arguments_sha256": execution_arguments_sha256(digested),
        "approvers": [P["approver"]],
    }
    if tamper == "key":
        key = SigningKey.generate()
    else:
        key = SigningKey(base64.b64decode(P["selector_seed"], validate=True))
    return sign_ticket(payload, signing_key=key)


def grant(tool, arguments):
    from curie_connector_proxy.canonical import canonical_arguments
    from curie_worker.connector_grant import mint
    jti = str(uuid.uuid4())
    token = mint(
        P["caller_seed"], agent=P["agent"], connector=P["connector"], tool=tool,
        args=canonical_arguments(arguments), exp=int(time.time()) + 600, jti=jti,
    )
    return token, fingerprint(jti)


def post(port, body, headers):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        connection.putrequest("POST", "/mcp", skip_accept_encoding=True)
        for name, value in headers:
            connection.putheader(name, value)
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def judged(status, raw):
    from curie_connector_proxy import caller
    from curie_connector_proxy.server import refusal_body
    out = {"status": status, "is_error": None, "text": None, "refusal": None,
           "frozen_refusal_body": None, "dropped": status == 502}
    try:
        body = json.loads(raw)
    except ValueError:
        return out
    if not isinstance(body, dict):
        return out
    result = body.get("result")
    if isinstance(result, dict):
        out["is_error"] = bool(result.get("isError"))
        content = result.get("content") or []
        first = content[0].get("text") if content and isinstance(content[0], dict) else None
        if isinstance(first, str):
            out["text"] = first.split("\\n", 1)[0][:TEXT_LIMIT]
    error = body.get("error")
    data = error.get("data") if isinstance(error, dict) else None
    reason = data.get("curie_caller") if isinstance(data, dict) else None
    if isinstance(reason, str) and reason in caller.REFUSALS:
        out["refusal"] = reason
        out["frozen_refusal_body"] = body == refusal_body(reason)
    return out


def call(port, case, sent):
    if case.get("replay_of"):
        tool, arguments, token, held = sent[case["replay_of"]]
    else:
        tool = case["tool"]
        arguments = dict(case["arguments"])
        if case.get("ticket") is not None:
            arguments["approval_ticket"] = ticket(arguments, case["ticket"].get("tamper"))
        token, held = None, None
        if case.get("grant"):
            granted = case.get("grant_arguments")
            token, held = grant(tool, arguments if granted is None else dict(granted))
    sent[case["id"]] = (tool, arguments, token, held)
    message = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": tool, "arguments": arguments}}
    if case.get("batch"):
        message = [message, {"jsonrpc": "2.0", "id": 2, "method": "ping"}]
    headers = [
        ("Content-Type", "application/json"),
        ("Accept", "application/json, text/event-stream"),
        ("X-Curie-Caller", P["token"]),
    ]
    if token is not None:
        headers.append((GRANT_HEADER, token))
    for name in case.get("forged") or ():
        headers.append((name, str(uuid.uuid4())))
    started = time.time()
    status, raw = post(port, json.dumps(message).encode("utf-8"), headers)
    finished = time.time()
    return {**judged(status, raw), "grant_fingerprint": held,
            "started": started, "finished": finished}


def main():
    port = int(os.environ["CURIE_CALLER_PROXY_PORT"])
    sent = {}
    answers = []
    for case in P["cases"]:
        try:
            answer = call(port, case, sent)
        except Exception as exc:
            answer = {"error": type(exc).__name__}
        answers.append({"id": case["id"], **answer})
        time.sleep(float(P["gap_s"]))
    return {"answers": answers}


try:
    result = main()
except Exception as exc:
    result = {"error": type(exc).__name__}
print(json.dumps(result))
"""
