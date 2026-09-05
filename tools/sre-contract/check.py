#!/usr/bin/env python3
"""Validate the supported SRE surface; --endpoints additionally consumes MCP catalogs.

Run from a checkout with PyYAML, jsonschema and plugin-format importable.
No catalog fixture or static pass establishes runtime tier parity.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import jsonschema
import yaml
from plugin_format import (
    ApprovalPolicy,
    PluginManifest,
    ToolPolicy,
    check_policy_patterns,
    classify_tool,
)

ROOT = Path(__file__).resolve().parents[2]
LEGACY = ROOT / "scripts/assert-gates-are-live-tools.py"


def read_json(path):
    return json.loads(path.read_text())


def effective(policy, gates, canonical):
    decision = str(classify_tool(policy, canonical))
    server, tool = canonical.split("/")
    if decision == "allow" and f"mcp__{server}__{tool}" in gates:
        return "approval-required"
    return decision


def validate(bundle):
    manifest = read_json(bundle / ".claude-plugin/plugin.json")
    connectors = yaml.safe_load((bundle / "connectors.yaml").read_text())
    surface = read_json(bundle / "supported-surface.json")
    schema = read_json(ROOT / "examples/sre-bot/supported-surface.schema.json")
    data = {"plugin": manifest, "connectors": connectors, "surface": surface}
    errors = [
        f"schema: invalid {'/'.join(map(str, e.absolute_path))}"
        for e in jsonschema.Draft202012Validator(schema).iter_errors(data)
    ]
    if errors:
        return errors, None
    plugin = PluginManifest.model_validate(manifest)
    policy = ToolPolicy.model_validate(plugin.toolPolicy)
    errors += ["invalid toolPolicy pattern" for _ in check_policy_patterns(policy)]
    declared = connectors["connectors"]
    expected = surface["connectors"]
    if set(declared) != set(expected):
        errors.append("connector set differs from supported surface")
    gates = {g.gate for g in ApprovalPolicy.model_validate(plugin.approvalPolicy).gates}
    rows = re.findall(
        r"^\| `([^`]+/[^`]+)` \| `(allow|approval-required|deny)` \|$",
        (bundle / "docs/PERMISSION-MAP.md").read_text(),
        re.M,
    )
    documented = dict(rows)
    if len(rows) != len(documented):
        errors.append("permission map contains duplicate tool rows")
    intended = {
        f"{name}/{tool}": decision
        for name, spec in expected.items()
        for tool, decision in spec["tools"].items()
    }
    if documented != intended:
        errors.append("permission map differs from supported surface")
    for canonical, decision in intended.items():
        actual = effective(policy, gates, canonical)
        if actual != decision:
            errors.append(f"{canonical}: expected {decision}, got {actual}")
    for collection in (policy.allow, policy.approvalRequired, policy.deny):
        for pattern in collection:
            if pattern.split("/")[0] not in declared:
                errors.append("toolPolicy references an undeclared connector")
    expected_legacy = {f"mcp__{name.split('/')[0]}__{name.split('/')[1]}" for name in intended}
    if gates - expected_legacy:
        errors.append("approval gate names no supported tool")
    return errors, (surface, policy, gates)


def live_catalogs(endpoints):
    # The MCP SDK owns HTTP negotiation, sessions and pagination. Protocol:
    # https://modelcontextprotocol.io/specification/2025-06-18/server/tools
    # https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
    import asyncio
    import logging

    # Third-party transport diagnostics may contain endpoint URLs or response bodies.
    logging.disable(logging.CRITICAL)

    from mcp import ClientSession, types
    from mcp.client.streamable_http import streamable_http_client

    async def collect():
        catalogs = {}
        for name, url in endpoints.items():
            async with streamable_http_client(url) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = []
                    cursor = None
                    seen = set()
                    for _ in range(100):
                        page = await session.list_tools(
                            params=types.PaginatedRequestParams(cursor=cursor)
                        )
                        tools.extend(page.tools)
                        cursor = page.next_cursor
                        if not cursor:
                            break
                        if cursor in seen:
                            raise ValueError("repeated MCP cursor")
                        seen.add(cursor)
                    else:
                        raise ValueError("MCP pagination exceeded bound")
                    catalogs[name] = [
                        (t.name, t.annotations is not None and t.annotations.read_only_hint is True)
                        for t in tools
                    ]
        return catalogs

    async def bounded():
        return await asyncio.wait_for(collect(), timeout=120)

    return asyncio.run(bounded())


def validate_catalogs(catalogs, surface, policy, gates):
    errors = []
    for name, spec in surface["connectors"].items():
        tools = catalogs[name]
        names = [tool for tool, _ in tools]
        if not names or len(names) != len(set(names)):
            errors.append(f"{name}: empty or duplicate MCP catalog")
            continue
        expected = set(spec["tools"])
        if not expected <= set(names) or (spec["catalog"] == "exact" and set(names) != expected):
            errors.append(f"{name}: live catalog differs from supported surface")
        for tool, read_only in tools:
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", tool):
                errors.append(f"{name}: malformed tool name")
                continue
            canonical = f"{name}/{tool}"
            if canonical in surface["forbiddenTools"]:
                errors.append(f"{canonical}: forbidden tool advertised")
            decision = effective(policy, gates, canonical)
            # Explicit denial is valid, accidental default denial is not coverage.
            from fnmatch import fnmatchcase

            covered = any(
                fnmatchcase(canonical, pattern)
                for pattern in policy.allow + policy.approvalRequired + policy.deny
            )
            if not covered:
                errors.append(f"{canonical}: uncovered live tool")
            if not read_only and decision == "allow":
                errors.append(f"{canonical}: ungated live write")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, default=ROOT / "examples/sre-bot")
    parser.add_argument(
        "--endpoints", type=Path, help="protected JSON object of connector names to real MCP URLs"
    )
    args = parser.parse_args()
    try:
        errors, parsed = validate(args.bundle)
        if args.endpoints and parsed:
            surface, policy, gates = parsed
            endpoints = read_json(args.endpoints)
            if not isinstance(endpoints, dict) or set(endpoints) != set(surface["connectors"]):
                errors.append("endpoints must name every declared connector exactly once")
            elif not all(
                isinstance(v, str) and v.startswith(("http://", "https://"))
                for v in endpoints.values()
            ):
                errors.append("invalid endpoint URL")
            else:
                catalogs = live_catalogs(endpoints)
                errors.extend(validate_catalogs(catalogs, surface, policy, gates))
                # Keep the existing two-direction gate checker an executed consumer,
                # with complete endpoint coverage supplied by this wrapper.
                command = [sys.executable, str(LEGACY), "--bundle", str(args.bundle)]
                for name, url in endpoints.items():
                    command += ["--connector", f"{name}={url}"]
                result = subprocess.run(command, capture_output=True, timeout=150)
                if result.returncode:
                    errors.append("assert-gates-are-live-tools: failed")
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "status": "pass",
                    "mode": "live-catalog" if args.endpoints else "static",
                    "runtime_tiers_proven": [],
                }
            )
        )
        return 0
    except Exception as exc:
        # Endpoint URLs, response bodies and credentials never enter diagnostics.
        print(f"SRE contract failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
