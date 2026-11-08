"""ADR 0271 decision 2, lock half: a registered pair belongs to its route both ways.

@spec REQUEST-APPROVAL-2.

The lock key comes only from ``granted_tool``, parsed like the worker's grant
minting (``mcp__<connector>__<tool>``, split once, never ``mcp__plugin_``);
``gate_kind`` never enables or bypasses it. Both creation owners are driven
through their real routes: the HTTP owner with a real ingress capsule and the
legacy owner with the platform key. Every refusal reads the stored approvals
afterwards, and the ordinary neighbors (unrelated tools on other routes,
plugin and malformed names off the owning route, the owning route with a valid
ticket) must still create.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from approver_ticket_support import (
    CONNECTOR,
    LIVE,
    OTHER_ROUTE,
    approval_rows,
    carried,
    create,
    dynamic_request,
    enroll,
    exactly,
    ingress,
    legacy,
    refused_with,
)
from approver_ticket_support import synthetic_provider as synthetic_provider
from approver_ticket_support import ticket_case as ticket_case

from apps.api.tests.test_http_approval_creation import accepted, rows

GATE_KINDS = (None, "policy", "permission")
PLAIN: dict[str, Any] = {"target": "acct-example-1"}
LOCKED = "approval_route_locked"
REQUIRED = "approval_ticket_required"
MISMATCH = "approval_ticket_route_mismatch"
UNREGISTERED = "approval_ticket_unregistered"
# Names that are not the registered pair's live name, judged on the owning route.
NOT_THE_PAIR = (
    f"mcp__{CONNECTOR}__read",
    "mcp__other__execute",
    f"{LIVE}__extra",
    "Bash",
    f"mcp__plugin_ticket-plugin_{CONNECTOR}__execute",
    f"mcp__{CONNECTOR}",
    "mcp____execute",
    f"mcp__{CONNECTOR}__",
)


def lock_cases(case: SimpleNamespace) -> list[tuple[str, dict[str, Any], str]]:
    """Each ``(name, approval changes, code)`` the lock refuses, for one gate kind."""
    ticket = carried(case, PLAIN)
    looser_ticket = carried(case, PLAIN, route=OTHER_ROUTE)
    token = ticket["approval_ticket"]
    cases: list[tuple[str, dict[str, Any], str]] = [
        ("looser_route", {"route": OTHER_ROUTE, "granted_arguments": PLAIN}, LOCKED),
        ("looser_route_signed_for_it",
         {"route": OTHER_ROUTE, "granted_arguments": looser_ticket}, LOCKED),
        ("routeless", {"route": None, "granted_arguments": PLAIN}, LOCKED),
        ("routeless_with_ticket", {"route": None, "granted_arguments": ticket}, LOCKED),
        ("empty_route", {"route": "", "granted_arguments": PLAIN}, LOCKED),
        ("missing_ticket", {"granted_arguments": PLAIN}, REQUIRED),
        ("non_string_ticket", {"granted_arguments": {**PLAIN, "approval_ticket": 7}}, REQUIRED),
        ("nested_ticket_only",
         {"granted_arguments": {**PLAIN, "nested": {"approval_ticket": token}}}, REQUIRED),
        ("ticket_for_unregistered_pair_on_looser_route",
         {"route": OTHER_ROUTE, "granted_tool": "mcp__other__execute",
          "granted_arguments": ticket}, UNREGISTERED),
        ("ticket_for_unrelated_routeless_tool",
         {"route": None, "granted_tool": "synthetic_read", "granted_arguments": ticket},
         UNREGISTERED),
    ]
    cases.extend(
        (f"owning_route_{name}", {"granted_tool": name, "granted_arguments": ticket}, MISMATCH)
        for name in NOT_THE_PAIR
    )
    return cases


def approval_for(case: SimpleNamespace, gate_kind: str | None, changes: dict[str, Any]) -> Any:
    request = dynamic_request(case, None, gate_kind=gate_kind)
    request["approval"].update(changes)
    return request


def test_http_creation_locks_the_registered_pair_to_its_route(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: each refusal under every gate kind; ordinary use continues."""
    case = ticket_case
    enroll(case)
    agent = case.scope["agent"]
    for gate_kind in GATE_KINDS:
        for name, changes, code in lock_cases(case):
            request = approval_for(case, gate_kind, changes)
            response = create(case.client, request)
            assert response.status_code == 409, (gate_kind, name, response.text)
            exactly(response, 409, code)
    assert approval_rows(agent) == []

    # Neighbors: the bundle still routes ordinary unrelated tools, and names that
    # are not the registered live name are ordinary off the owning route.
    # Each fresh ingress mints a channel token, which bumps the binding
    # generation and retires every earlier capsule, so a neighbor's capsule is
    # minted only just before that neighbor is posted.
    for invocation_for, tool in (
        (lambda: case, "synthetic_read"),
        (lambda: ingress(case), f"mcp__plugin_ticket-plugin_{CONNECTOR}__execute"),
        (lambda: ingress(case), f"mcp__{CONNECTOR}"),
    ):
        request = dynamic_request(
            invocation_for(), {}, route=OTHER_ROUTE, tool=tool, gate_kind="permission",
        )
        accepted(create(case.client, request), request)
        assert rows(request["approval"]["dedupe_key"])[0]["route"] == OTHER_ROUTE
    owning = ingress(case)
    request = dynamic_request(owning, carried(case, PLAIN))
    accepted(create(case.client, request), request)
    assert len(approval_rows(agent)) == 4


def test_legacy_creation_applies_the_same_lock(ticket_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: legacy creation cannot take a looser route either.

    Lock refusals keep their codes; a registered pair with a valid ticket is
    refused because only the HTTP owner verifies selections. This agent is an
    HTTP-selected one, so its ordinary legacy creation keeps today's refusal.
    """
    case = ticket_case
    enroll(case)
    agent = case.scope["agent"]
    for gate_kind in GATE_KINDS:
        for name, changes, code in lock_cases(case):
            approval = approval_for(case, gate_kind, changes)["approval"]
            response = legacy(case.client, approval)
            assert response.status_code == 409, (gate_kind, name, response.text)
            refused_with(response, 409, code)
        selected = approval_for(case, gate_kind, {"granted_arguments": carried(case, PLAIN)})
        refused_with(
            legacy(case.client, selected["approval"]), 409,
            "approval_ticket_requires_http_creation",
        )
    assert approval_rows(agent) == []
    ordinary = dynamic_request(case, {}, route=OTHER_ROUTE, tool="synthetic_read",
                               gate_kind="permission")["approval"]
    refused_with(legacy(case.client, ordinary), 409, "unsupported_approval")
    assert approval_rows(agent) == []
