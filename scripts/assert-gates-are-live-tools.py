#!/usr/bin/env python3
"""Assert every declared approval gate names a tool that actually exists.

Why this has to exist
---------------------
An approval gate is armed by EXACT STRING MATCH against the tool name the SDK
reports (`curie_runner.approval.build_can_use_tool` compares
`tool_name in gate.required`). A gate naming a tool that does not exist arms
nothing -- and nothing anywhere reports it. The connector still works, the tool
still runs, no error is raised, and the approval card simply never appears.

Curie's own source calls this out as "a fail-open on a security-relevant control,
with no signal anywhere that the gate is a no-op", and cannot fully guard it:
the deploy validator skips the check when it cannot determine the bundle's MCP
server names, which is the case for a connectors.yaml bundle (curie#1495).

So the guard belongs here, where the live tool list is knowable: start the
connectors, ask each one for its tools, and compare.

The naming rule this encodes
----------------------------
Curie connectors are PLATFORM-SUPPLIED servers -- they ride
`ClaudeAgentOptions.mcp_servers` alongside curie's own `mcp__curie__*` servers
(`runner/src/curie_runner/connectors.py`). They are therefore named:

    mcp__<connector>__<tool>

and NOT `mcp__plugin_<bundle>_<connector>__<tool>`, which is the form used for
servers a bundle declares in its own MCP config. This matters because curie's
deploy-time error message currently advises the plugin-prefixed form for
connector tools; following that advice produces a gate that validates but never
fires. Confirmed against a live gated write loop in sre-bot#90.

Usage
-----
    scripts/assert-gates-are-live-tools.py \
        --connector k8s-write=http://localhost:8003/mcp \
        --connector tempo=http://localhost:8002/mcp

Two directions, and the second matters more
-------------------------------------------
1. Every declared gate names a tool that exists. Catches a typo, and catches the
   plugin-prefixed form curie's own error message recommends.
2. Every tool that is NOT `readOnlyHint` has a gate. Catches the failure the
   one-tool-per-write-connector rule exists to avoid: someone adds a second
   write tool and forgets to declare its gate. Nothing else reports that -- the
   connector works, the tool runs, and no card is ever posted.

Direction 2 was missing until sre-bot#90 review. `readOnlyHint` is an
agent-facing annotation rather than a security boundary (the credential is that),
but a tool declaring itself write-shaped with no gate in front of it is
unambiguously a mistake.

Exits non-zero naming the offending tool or gate.
"""

# Annotations stay strings so this runs on the older interpreters a cluster host
# may have -- the point of a runtime check is that it runs where the connectors are.
from __future__ import annotations

import argparse
import difflib
import fnmatch
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parent.parent


def with_url_port(host: str, url: str) -> str:
    """Give ``host`` the URL's port when it has none.

    `-allowed-hosts` entries are `host:port`, and a Host header without the port
    does not match one that has it. Measured against a live connector: the same
    name answered 200 as `<svc>:8000` and 403 as `<svc>`. Nobody passing
    `--host name=<svc>` means "and drop the port", so supply it rather than
    letting the request 403 for a reason the caller cannot see.

    An IPv6 literal is bracketed (`[::1]`), so the colon test looks after the
    closing bracket rather than anywhere in the string.
    """

    tail = host[host.rindex("]") + 1 :] if host.endswith("]") or "]" in host else host
    if ":" in tail:
        return host
    port = urllib.parse.urlsplit(url).port
    return f"{host}:{port}" if port else host


class _KeepMethodRedirect(urllib.request.HTTPRedirectHandler):
    """Follow 307/308 for a POST, which urllib will not do on its own.

    A streamable-HTTP server mounted at `/mcp/` answers `/mcp` with a 307. urllib
    raises rather than re-issuing the POST, so the connector reads as unreachable
    -- and this script treats unreachable as fatal, so one missing slash fails the
    whole check with a message naming the wrong cause. Measured on a live pair:
    two connectors from the same bundle differed, because their FastMCP versions
    did.

    Only 307 and 308, which are defined to preserve the method and body. A 301 or
    302 would turn this POST into a GET, and silently checking a different request
    than the one asked for is worse than failing.
    """

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if code not in (307, 308):
            redirected: urllib.request.Request | None = super().redirect_request(
                req, fp, code, msg, headers, newurl
            )
            return redirected
        return urllib.request.Request(
            newurl,
            data=req.data,
            headers=dict(req.header_items()),
            method=req.get_method(),
        )


_OPENER = urllib.request.build_opener(_KeepMethodRedirect)


def list_tools(url: str, timeout: float, host: str | None = None) -> list[tuple[str, bool]]:
    """Return one connector's tool names via the MCP handshake.

    The session dance is not optional. A streamable-HTTP server issues an
    `Mcp-Session-Id` on the initialize response and rejects every later request
    that does not carry it, and it expects `notifications/initialized` before it
    will answer `tools/list`. An earlier version of this function skipped both
    and was only ever tested against a stub that ignored them -- it returned
    HTTP 400 the first time it met a real FastMCP connector, which would have
    made this guard fail open on every run (it exits non-zero, so CI would go
    red rather than pass silently, but a check nobody can keep green gets
    deleted).
    """

    session: dict[str, str] = {}

    def post(payload: dict[str, object], notify: bool = False) -> dict[str, object]:
        headers = {
            "Content-Type": "application/json",
            # Streamable-HTTP servers negotiate; without both types some
            # return 406 and the failure looks like an empty tool list.
            "Accept": "application/json, text/event-stream",
        }
        # A curie connector runs behind `-allowed-hosts`, so it answers 403 to any
        # request whose Host it does not recognize. Reaching one through a
        # port-forward changes the Host to `127.0.0.1:<port>` and every call fails
        # -- which reads as an unreachable connector rather than as a rejected
        # Host, and this script treats unreachable as fatal. So the caller can say
        # what Host the connector expects.
        if host:
            headers["Host"] = host
        headers.update(session)
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
        with _OPENER.open(req, timeout=timeout) as r:
            sid = r.headers.get("Mcp-Session-Id")
            if sid:
                session["Mcp-Session-Id"] = sid
            body = r.read().decode()
        if notify or not body.strip():
            return {}
        # An SSE-framed reply carries the JSON on `data:` lines.
        if body.lstrip().startswith(("event:", "data:")):
            for line in body.splitlines():
                if line.startswith("data:"):
                    framed: dict[str, object] = json.loads(line[5:].strip())
                    return framed
        plain: dict[str, object] = json.loads(body)
        return plain

    post(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "gate-assert", "version": "1"},
            },
        }
    )
    post({"jsonrpc": "2.0", "method": "notifications/initialized"}, notify=True)
    got = post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    # Keep the annotation, not just the name. `readOnlyHint` is what says which
    # tools NEED a gate, and discarding it is why this script only ever checked
    # one of the two directions that matter.
    result = got.get("result")
    tools = result.get("tools", []) if isinstance(result, dict) else []
    return [(str(t["name"]), bool((t.get("annotations") or {}).get("readOnlyHint"))) for t in tools]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--connector",
        action="append",
        default=[],
        metavar="NAME=URL",
        help="connector name and its MCP URL (repeatable)",
    )
    ap.add_argument(
        "--bundle",
        type=pathlib.Path,
        required=True,
        help="bundle directory whose .claude-plugin/plugin.json declares the gates",
    )
    ap.add_argument(
        "--host",
        action="append",
        default=[],
        metavar="NAME=HOST",
        help="Host header a connector expects, when its URL is not that Host "
        "(repeatable; needed behind a port-forward). The URL's port is "
        "appended when absent, because -allowed-hosts entries carry one",
    )
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    manifest_path = args.bundle / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    legacy_gates = [
        g["gate"].strip()
        for g in (manifest.get("approvalPolicy") or {}).get("gates", [])
        if g.get("gate")
    ]
    approval_patterns = [
        str(pattern).strip()
        for pattern in (manifest.get("toolPolicy") or {}).get("approvalRequired", [])
        if str(pattern).strip()
    ]
    if not legacy_gates and not approval_patterns:
        # NOT a free pass, which is what it used to be. A bundle with no gates
        # and no write tools is fine; a bundle with no gates and a write tool is
        # exactly the hole this script exists for, so the ungated-write check
        # below still has to run.
        print("no approvalPolicy or toolPolicy approval gates declared")

    hosts: dict[str, str] = {}
    for spec in args.host:
        if "=" not in spec:
            print(f"--host wants NAME=HOST, got {spec!r}", file=sys.stderr)
            return 2
        name, value = spec.split("=", 1)
        hosts[name] = value

    live: list[str] = []
    live_canonical: list[str] = []
    write_tools: list[str] = []
    for spec in args.connector:
        if "=" not in spec:
            print(f"--connector wants NAME=URL, got {spec!r}", file=sys.stderr)
            return 2
        name, url = spec.split("=", 1)
        try:
            declared_host = hosts.get(name)
            sent_host = with_url_port(declared_host, url) if declared_host else None
            tools = list_tools(url, args.timeout, sent_host)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            # Fail rather than skip. A connector that will not answer means the
            # assertion is unproven, and an unproven gate check is the exact
            # thing this script exists to prevent being assumed.
            detail = f"could not list tools for {name!r} at {url}: {exc}"
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 403:
                # The one failure whose cause is invisible in the exception: a
                # connector behind `-allowed-hosts` rejects the Host, not the
                # caller. Name what was sent so the fix is `--host`, not a hunt
                # for a dead connector.
                detail += (
                    f"\n  the connector rejected the Host it was sent"
                    f" ({sent_host or urllib.parse.urlsplit(url).netloc});"
                    f" pass --host {name}=<host:port> naming a host it allows"
                )
            print(detail, file=sys.stderr)
            return 2
        for tool, read_only in tools:
            qualified = f"mcp__{name}__{tool}"
            live.append(qualified)
            live_canonical.append(f"{name}/{tool}")
            if not read_only:
                write_tools.append(qualified)
        writes = sum(1 for _, ro in tools if not ro)
        print(f"  {name:<18} {len(tools)} tools ({writes} write)")

    policy_gates = {
        f"mcp__{canonical.split('/', 1)[0]}__{canonical.split('/', 1)[1]}"
        for canonical in live_canonical
        if any(fnmatch.fnmatchcase(canonical, pattern) for pattern in approval_patterns)
    }
    unmatched_patterns = [
        pattern
        for pattern in approval_patterns
        if not any(fnmatch.fnmatchcase(canonical, pattern) for canonical in live_canonical)
    ]
    gates = sorted(set(legacy_gates) | policy_gates)
    bad = [g for g in legacy_gates if g not in live]
    for g in bad:
        near = difflib.get_close_matches(g, live, n=1, cutoff=0.5)
        hint = f" -- closest live tool: {near[0]}" if near else ""
        print(f"GATE ARMS NOTHING: {g}{hint}", file=sys.stderr)
    if bad:
        print(
            f"\n{len(bad)} of {len(gates)} gate(s) name no live tool. Armed as "
            "written they would validate, deploy, and silently never fire.",
            file=sys.stderr,
        )
        return 1
    for pattern in unmatched_patterns:
        print(
            f"GATE ARMS NOTHING: toolPolicy approvalRequired pattern {pattern!r} "
            "matches no live tool",
            file=sys.stderr,
        )
    if unmatched_patterns:
        return 1

    # THE OTHER DIRECTION, and the one that matters more. The check above stops a
    # gate from naming a tool that does not exist. This stops a tool that can
    # WRITE from existing with no gate in front of it -- which fails silently:
    # the connector works, the tool runs, no error is raised, and no card is ever
    # posted. Adding a second tool to a write connector and forgetting to declare
    # its gate used to pass this script trivially.
    #
    # `readOnlyHint` is an agent-facing annotation, not a security boundary --
    # the credential is. But a tool that declares itself write-shaped and carries
    # no gate is unambiguously a mistake, and that is what this catches.
    ungated = [t for t in write_tools if t not in gates]
    for t in ungated:
        print(
            f"UNGATED WRITE TOOL: {t} is not readOnlyHint and no approvalPolicy "
            "gate or toolPolicy approvalRequired pattern names it -- it would "
            "execute without asking anyone",
            file=sys.stderr,
        )
    if ungated:
        print(
            f"\n{len(ungated)} write tool(s) have no gate. Declare each in "
            ".claude-plugin/plugin.json, or make the tool read-only.",
            file=sys.stderr,
        )
        return 1

    print(
        f"all {len(gates)} gate(s) match a live tool; "
        f"all {len(write_tools)} write tool(s) are gated"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
