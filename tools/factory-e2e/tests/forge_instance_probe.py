"""Read through production settings, mounts, registry and adapters in an API pod.

This file is passed to ``python3 -`` over kubectl stdin. It is never installed
in the product image. Neither tokens nor certificate files enter the request.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from types import SimpleNamespace
from typing import Any


async def observe(request: dict[str, Any]) -> dict[str, Any]:
    operation = request["operation"]
    if operation == "bridge":
        # This infrastructure check must still work when a refusal vector has
        # deliberately removed a required mounted credential or trust object.
        import socket
        from urllib.parse import urlsplit

        for url in request["urls"]:
            parts = urlsplit(url)
            with socket.create_connection((parts.hostname, parts.port), timeout=10):
                pass
        return {"reachable": True}

    import httpx
    from curie_api.code_host_trust import code_host_verify
    from curie_api.config import get_settings
    from curie_api.forges.registry import forges_registry
    from curie_api.forges.surfaces import effective_trigger_label
    from curie_api.forges.types import TrackerIssueRef

    settings = get_settings()
    registry = forges_registry(settings)
    if operation == "labels":
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://127.0.0.1:8000/agents", headers={"X-API-Key": settings.api_key}
            )
            response.raise_for_status()
        return {
            "surfaces": [
                {
                    "agent": agent["name"],
                    "kind": surface["kind"],
                    "address": surface["address"],
                    "stored": surface["trigger_label"],
                    "effective": effective_trigger_label(SimpleNamespace(**surface), registry),
                }
                for agent in response.json()
                for surface in agent["channels"]
            ]
        }
    if operation == "untrusted":
        # The consumer is the same production trust builder used by adapters.
        # No test CA file, verify=False or replacement SSLContext is supplied.
        try:
            async with httpx.AsyncClient(
                verify=code_host_verify(settings), trust_env=False
            ) as client:
                await client.get(request["url"], timeout=15)
        except httpx.ConnectError as error:
            tls = "CERTIFICATE_VERIFY_FAILED" in str(error)
            return {"tls_refused": tls, "error_type": type(error).__name__}
        return {"tls_refused": False}
    binding = registry.binding(request["binding"])
    tracker = registry.config.tracker(binding.tracker)
    connection = registry.binding_tracker(tracker.name)
    async with httpx.AsyncClient(verify=code_host_verify(settings), trust_env=False) as client:
        adapter = connection.for_binding(client, binding)
        issue = TrackerIssueRef(
            kind=tracker.kind,
            host=tracker.host,
            scope_id=binding.tracker_scope_id,
            issue_id="1" if tracker.kind == "gitlab" else "10001",
            display_key=binding.repos[0].path + "#1" if tracker.kind == "gitlab" else "ACME-1",
        )
        content = await adapter.read_ticket(issue)
        repositories = []
        for repo in binding.repos:
            host = registry.code_host(repo.code_host)
            if host.config.kind == "github":
                # Jira is observed through its tracker port. GitHub preflight
                # is driven by the real CLI; this probe's repository reads
                # exercise the GitLab and Data Center immutable-id consumers.
                continue
            # Public adapter wiring and the persisted immutable repository id.
            resolved = await host.adapter(client).resolve_repository(repo.project_id)
            if resolved.project_id != repo.project_id:
                return {"error_type": "RepositoryIdentityMismatch"}
            repositories.append(
                {
                    "kind": resolved.kind,
                    "host": resolved.host,
                    "id": resolved.project_id,
                    "path": resolved.path,
                }
            )
    return {
        "binding": binding.name,
        "tracker": tracker.name,
        "host": tracker.host,
        "scope": binding.tracker_scope_id,
        "content": content,
        "repositories": repositories,
    }


if __name__ == "__main__":
    try:
        result = asyncio.run(observe(json.loads(sys.argv[1])))
    except Exception as error:  # noqa: BLE001 - redact every adapter exception before pod stdout.
        # An adapter failure is product evidence. Exception text can contain a
        # provider response or credential, so expose only its type.
        result = {"error_type": type(error).__name__}
    print("FORGE_OBSERVATION=" + json.dumps(result, sort_keys=True))
    sys.stdout.flush()
    os._exit(0)
