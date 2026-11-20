"""Listing learned rules and deciding them through the memory API (#5797).

The real ``create_app()`` with its lifespan, real Postgres, the real OIDC
principal sessions, the real identity admin route for code host links, and the
real GitHub code host adapter the forge registry builds from ``CURIE_FORGES``.
Only GitHub REST is a stand-in (``forge_fakes.github.GitHubRepositoryFake``),
answering in GitHub's documented shapes:

1. ``GET /repositories/{id}``:
   https://docs.github.com/en/rest/repos/repos#get-a-repository (answers ``id``,
   ``full_name`` and ``default_branch``; an unknown id is 404).
2. ``GET /user/{account_id}``:
   https://docs.github.com/en/rest/users/users#get-a-user-using-their-id
3. ``GET /repos/{owner}/{repo}/collaborators/{login}/permission``:
   https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
   (answers ``permission`` and the ``user`` it is for).

App installation tokens are answered by ``fake_installation_tokens``. Rules are
seeded as rows, since no proposal route exists in this change. Placeholder
identifiers only.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import os
import re
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import personal_enrollment_support as enrollment
import personal_execution_support as pe
import pytest
from curie_api.config import get_settings
from curie_api.forges.github.code_host import PATH_ID_PREFIX
from curie_api.github_app import _RESOLVERS
from curie_internal.memory_rules import MAX_RULE_BYTES, MAX_SNAPSHOT_BYTES
from fastapi.testclient import TestClient
from forge_fakes.github import (
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubRepositoryFake,
    fake_installation_tokens,
    forges_document,
    forges_env_json,
)
from personal_enrollment_support import FakeCustodyStore, Owner

HOST = "github.com"
SCOPE = f"github:{HOST}:{REPO_ID}"
# A repository id the scope recorded before its repository moved or was replaced.
OLD_REPO_ID = 7701
SECOND_ID = 6602
SECOND_LOGIN = "hubot"
STAMP = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
ITEM_FIELDS = {
    "id",
    "number",
    "label",
    "scope_kind",
    "scope_key",
    "scope_label",
    "state",
    "text",
    "reason",
    "source_url",
    "proposed_by",
    "proposed_at",
    "accepted_by",
    "accepted_at",
    "rejected_by",
    "rejected_at",
    "removed_by",
    "removed_at",
    "version",
}
UNAUTHENTICATED = "missing, invalid, or expired principal session"
ORIGIN_REJECTED = "console session origin rejected"

Override = Callable[[httpx.Request], httpx.Response | None]


# --- the stack -----------------------------------------------------------------------


@dataclass
class Stack:
    client: TestClient
    github: GitHubRepositoryFake
    issuer: str
    tenant_id: uuid.UUID
    owner: Owner
    agent_id: uuid.UUID
    platform: dict[str, str]
    # Every request the code host adapter sent, in order.
    seen: list[httpx.Request] = field(default_factory=list)
    # Answers consulted before the repository fake, for one case's own shape.
    overrides: list[Override] = field(default_factory=list)
    numbers: Iterator[int] = field(default_factory=lambda: itertools.count(1))

    def outbound(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        for override in self.overrides:
            answer = override(request)
            if answer is not None:
                return answer
        return self.github.handle(request)


def _cleanup() -> int:
    owned = "SELECT id FROM curie.tenants WHERE deployment_id LIKE :mark"
    mark = {"mark": f"{pe.MARK}%"}
    for table in ("identity_links", "identity_audit_events", "identity_namespaces"):
        pe.rows(f"DELETE FROM curie.{table} WHERE tenant_id IN ({owned})", mark)
    return pe.cleanup_owned()


@pytest.fixture
def owned(clean_db: None) -> Iterator[None]:
    _cleanup()
    yield
    assert _cleanup() == 0


@pytest.fixture
def stack(owned: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Stack]:
    github = GitHubRepositoryFake()
    github.accounts[str(SENDER_ID)] = {"id": SENDER_ID, "login": SENDER, "type": "User"}
    github.permissions[SENDER] = (SENDER_ID, "write")
    fake_installation_tokens(monkeypatch)
    forges = forges_env_json(forges_document(), tmp_path)
    _RESOLVERS.clear()
    with (
        enrollment.identity_provider() as idp,
        enrollment.environment({"CURIE_FORGES": forges}),
        enrollment.enrollment_app(FakeCustodyStore()) as client,
    ):
        tenant_id = enrollment.seed_tenant()
        owner = enrollment.seed_owner(tenant_id, issuer=idp.issuer)
        found = Stack(
            client=client,
            github=github,
            issuer=idp.issuer,
            tenant_id=tenant_id,
            owner=owner,
            agent_id=pe.insert_agent(tenant_id),
            platform={"X-API-Key": get_settings().api_key},
        )
        original = client.app.state.http_client  # type: ignore[attr-defined]
        outbound = httpx.AsyncClient(transport=httpx.MockTransport(found.outbound))
        client.app.state.http_client = outbound  # type: ignore[attr-defined]
        try:
            yield found
        finally:
            client.app.state.http_client = original  # type: ignore[attr-defined]
            asyncio.run(outbound.aclose())
    _RESOLVERS.clear()


# --- seeding -------------------------------------------------------------------------


def _link(
    stack: Stack, owner: Owner, *, account_id: int = SENDER_ID, host: str = HOST
) -> uuid.UUID:
    """Link ``owner`` to a GitHub account through the operator route."""

    response = stack.client.post(
        f"/identity/admin/{owner.tenant_id}/code-host-links",
        headers=stack.platform,
        json={
            "principal_id": str(owner.principal_id),
            "code_host_kind": "github",
            "host": host,
            "account_id": str(account_id),
            "created_by_principal_id": str(owner.principal_id),
        },
    )
    assert response.status_code == 201, response.text
    return uuid.UUID(response.json()["link_id"])


def _text(index: int, length: int = 40) -> str:
    head = f"Keep rule {index:02d} "
    return head + "x" * (length - len(head))


def _rule(
    stack: Stack,
    *,
    state: str = "proposed",
    number: int | None = None,
    text: str | None = None,
    scope_kind: str = "repository",
    scope_key: str = SCOPE,
    scope_label: str = REPO,
    minute: int = 0,
    agent_id: uuid.UUID | None = None,
    tenant_id: uuid.UUID | None = None,
    decider: uuid.UUID | None = None,
) -> uuid.UUID:
    numberless = state in ("candidate", "superseded", "discarded")
    if number is None and not numberless:
        number = next(stack.numbers)
    body = text if text is not None else _text(number or 0)
    person = decider or stack.owner.principal_id
    at = STAMP + timedelta(minutes=minute)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id or stack.tenant_id,
        "agent_id": agent_id or stack.agent_id,
        "scope_kind": scope_kind,
        "scope_key": "" if scope_kind == "agent" else scope_key,
        "scope_label": scope_label,
        "number": None if numberless else number,
        "state": state,
        "text": body,
        "sha256": hashlib.sha256(body.encode()).hexdigest(),
        "format_version": 1,
        "reason": "Reviewers asked for it twice.",
        "proposed_by": f"github:{SENDER_ID}:{SENDER}",
        "proposed_at": STAMP,
        "source_url": f"https://github.com/{REPO}/pull/12",
    }
    if state in ("accepted", "removed"):
        values |= {"accepted_by": person, "accepted_at": at}
    if state == "rejected":
        values |= {"rejected_by": person, "rejected_at": at}
    if state == "removed":
        values |= {"removed_by": person, "removed_at": at + timedelta(seconds=1)}
    pe.rows(
        f"INSERT INTO curie.memory_rules ({', '.join(values)}) "
        f"VALUES ({', '.join(':' + name for name in values)})",
        values,
    )
    return values["id"]  # type: ignore[no-any-return]


def _second_owner(stack: Stack, *, account_id: int = SECOND_ID, login: str = SECOND_LOGIN) -> Owner:
    """Another person of the same tenant, linked to a writer account."""

    other = enrollment.seed_owner(stack.tenant_id, issuer=stack.issuer)
    stack.github.accounts[str(account_id)] = {"id": account_id, "login": login, "type": "User"}
    stack.github.permissions[login] = (account_id, "write")
    _link(stack, other, account_id=account_id)
    return other


# --- reading back --------------------------------------------------------------------


def _row(rule_id: uuid.UUID) -> dict[str, Any]:
    (found,) = pe.rows(
        "SELECT state, version, accepted_by, accepted_at, rejected_by, rejected_at, "
        "removed_by, removed_at, updated_at FROM curie.memory_rules WHERE id = :id",
        {"id": rule_id},
    )
    return found


def _events(rule_id: uuid.UUID) -> list[dict[str, Any]]:
    return pe.rows(
        "SELECT id, event, from_state, to_state, actor_kind, actor, principal_id "
        "FROM curie.memory_rule_events WHERE rule_id = :id ORDER BY id",
        {"id": rule_id},
    )


def _snapshot(rule_id: uuid.UUID) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    return _row(rule_id), _events(rule_id)


def _accepted_count(stack: Stack, scope_key: str = SCOPE) -> int:
    (found,) = pe.rows(
        "SELECT count(*) AS n FROM curie.memory_rules WHERE agent_id = :agent "
        "AND scope_kind = 'repository' AND scope_key = :key AND state = 'accepted'",
        {"agent": stack.agent_id, "key": scope_key},
    )
    return int(found["n"])


def _clock() -> datetime:
    (found,) = pe.rows("SELECT clock_timestamp() AS now")
    return found["now"]  # type: ignore[no-any-return]


# --- requests ------------------------------------------------------------------------


def _list(stack: Stack, headers: dict[str, str] | None = None, **params: str) -> httpx.Response:
    return stack.client.get(
        f"/agents/{stack.agent_id}/memory/rules",
        params=params,
        headers=stack.platform if headers is None else headers,
    )


def _decide(
    stack: Stack,
    rule_id: uuid.UUID,
    action: str = "accept",
    *,
    version: int = 1,
    owner: Owner | None = None,
    headers: dict[str, str] | None = None,
    body: Any = None,
    agent_id: uuid.UUID | None = None,
) -> httpx.Response:
    return stack.client.post(
        f"/agents/{agent_id or stack.agent_id}/memory/rules/{rule_id}/{action}",
        json={"expected_version": version} if body is None else body,
        headers=(owner or stack.owner).headers() if headers is None else headers,
    )


def _refused(
    response: httpx.Response,
    status: int,
    code: str,
    rule_id: uuid.UUID,
    before: tuple[dict[str, Any], list[dict[str, Any]]],
) -> dict[str, Any] | str:
    assert response.status_code == status, response.text
    assert response.headers["Cache-Control"] == "no-store"
    detail: dict[str, Any] | str = response.json()["detail"]
    if code.startswith("memory_rules."):
        assert isinstance(detail, dict), detail
        assert detail["code"] == code
        assert detail["message"] and detail["fix"]
    else:
        assert detail == code
    assert _snapshot(rule_id) == before
    return detail


def _accepts(stack: Stack, rule_id: uuid.UUID, owner: Owner | None = None) -> dict[str, Any]:
    """Liveness: a valid writer's accept through the same route succeeds."""

    response = _decide(stack, rule_id, owner=owner)
    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "no-store"
    found: dict[str, Any] = response.json()
    assert found["applies_from"] == "next_ticket"
    assert found["rule"]["state"] == "accepted"
    assert _row(rule_id)["state"] == "accepted"
    return found


# --- the CLI's recorded responses ---------------------------------------------------

# The real list and accept responses the CLI parses (`cli/tests/memory_rules.rs`),
# with volatile ids and clocks replaced by stable placeholders.
CLI_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "memory-rules-responses.json"
)
CLI_VECTOR_NOTE = (
    "Real responses of GET /agents/{agent_id}/memory/rules and POST "
    ".../memory/rules/{rule_id}/accept, recorded from the routes by "
    "apps/api/tests/test_memory_rule_routes.py::"
    "test_list_and_accept_responses_match_the_cli_vector, which fails when they drift. "
    "Ids and clocks are placeholders. Refresh: CURIE_WRITE_MEMORY_RULES_VECTOR=1 "
    "uv run pytest apps/api/tests/test_memory_rule_routes.py -k cli_vector"
)
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_CLOCK = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?")


def _stable(document: Any) -> Any:
    """``document`` with each distinct id and clock replaced by a numbered placeholder."""

    ids: dict[str, str] = {}
    clocks: dict[str, str] = {}

    def clock(match: re.Match[str]) -> str:
        stamp = clocks.setdefault(
            match.group(0),
            (STAMP + timedelta(minutes=len(clocks))).strftime("%Y-%m-%dT%H:%M:%S"),
        )
        return stamp + (".000000" if match.group(1) else "")

    def walk(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [walk(item) for item in value]
        if isinstance(value, str):
            if _UUID.fullmatch(value):
                return ids.setdefault(value, f"00000000-0000-4000-8000-{len(ids) + 1:012d}")
            return _CLOCK.sub(clock, value)
        return value

    return walk(document)


def test_list_and_accept_responses_match_the_cli_vector(stack: Stack) -> None:
    _link(stack, stack.owner)
    other = enrollment.seed_owner(stack.tenant_id, issuer=stack.issuer)
    proposed = _rule(stack, minute=1)
    _rule(stack, state="accepted", decider=other.principal_id, minute=2)
    _rule(stack, state="rejected", decider=other.principal_id, minute=3)
    _rule(stack, state="removed", decider=other.principal_id, minute=4)
    listed = _list(stack)
    assert listed.status_code == 200, listed.text
    accepted = _decide(stack, proposed)
    assert accepted.status_code == 200, accepted.text

    recorded = _stable({"list": listed.json(), "accept": accepted.json()})
    assert recorded["list"]["rules"], "the vector must carry rules"
    states = {rule["state"] for rule in recorded["list"]["rules"]}
    assert states == {"proposed", "accepted", "rejected", "removed"}
    document = {"_comment": CLI_VECTOR_NOTE} | recorded
    if os.environ.get("CURIE_WRITE_MEMORY_RULES_VECTOR") == "1":
        CLI_VECTOR.write_text(json.dumps(document, indent=2) + "\n")
    assert json.loads(CLI_VECTOR.read_text()) == document, (
        f"{CLI_VECTOR.name} no longer matches the routes; refresh it as its _comment says"
    )


# --- AC1: the list route -------------------------------------------------------------


def test_list_returns_visible_rules_with_every_field_for_key_and_session(stack: Stack) -> None:
    proposed = _rule(stack, number=3)
    accepted = _rule(stack, state="accepted", number=1)
    rejected = _rule(stack, state="rejected", number=2)
    removed = _rule(stack, state="removed", number=4)
    agent_rule = _rule(stack, scope_kind="agent", scope_label="agent", number=1)
    for hidden in ("superseded", "discarded"):
        _rule(stack, state=hidden)

    by_key = _list(stack)
    by_session = _list(stack, headers=stack.owner.headers(origin=None))

    assert by_key.status_code == 200, by_key.text
    assert by_key.headers["Cache-Control"] == "no-store"
    assert by_session.status_code == 200, by_session.text
    assert by_session.json() == by_key.json()
    found = by_key.json()
    assert set(found) == {"rules", "scopes", "in_force_limit"}
    assert found["in_force_limit"] == 16
    rules = found["rules"]
    assert [rule["id"] for rule in rules] == [
        str(agent_rule),
        str(accepted),
        str(rejected),
        str(proposed),
        str(removed),
    ]
    assert all(set(rule) == ITEM_FIELDS for rule in rules)
    owner = {"principal_id": str(stack.owner.principal_id), "display_name": "Acme Owner"}
    first = rules[1]
    assert first == {
        "id": str(accepted),
        "number": 1,
        "label": "R1",
        "scope_kind": "repository",
        "scope_key": SCOPE,
        "scope_label": REPO,
        "state": "accepted",
        "text": _text(1),
        "reason": "Reviewers asked for it twice.",
        "source_url": f"https://github.com/{REPO}/pull/12",
        "proposed_by": f"github:{SENDER_ID}:{SENDER}",
        "proposed_at": "2026-11-19T12:00:00Z",
        "accepted_by": owner,
        "accepted_at": "2026-11-19T12:00:00Z",
        "rejected_by": None,
        "rejected_at": None,
        "removed_by": None,
        "removed_at": None,
        "version": 1,
    }
    assert rules[2]["rejected_by"] == owner and rules[2]["accepted_by"] is None
    assert rules[4]["removed_by"] == owner and rules[4]["accepted_by"] == owner
    assert {rule["state"] for rule in rules} == {"proposed", "accepted", "rejected", "removed"}
    assert found["scopes"] == [
        {"scope_kind": "agent", "scope_key": "", "scope_label": "agent", "in_force_count": 0},
        {"scope_kind": "repository", "scope_key": SCOPE, "scope_label": REPO, "in_force_count": 1},
    ]


@pytest.mark.parametrize("state", ["candidate", "superseded", "discarded", "junk"])
def test_list_refuses_states_that_are_never_listed(stack: Stack, state: str) -> None:
    _rule(stack)
    response = _list(stack, state=state)
    assert response.status_code == 422, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json()["detail"]["code"] == "memory_rules.invalid_request"
    # Liveness: a listed state through the same route answers.
    assert _list(stack, state="proposed").status_code == 200


def test_list_filters_and_counts_in_force_regardless_of_the_filter(stack: Stack) -> None:
    other_scope = f"github:{HOST}:{REPO_ID + 1}"
    proposed = _rule(stack)
    _rule(stack, state="accepted")
    _rule(stack, state="accepted")
    _rule(stack, scope_key=other_scope, scope_label="acme-corp/acme-elsewhere")

    found = _list(stack, state="proposed", scope_key=SCOPE).json()

    assert [rule["id"] for rule in found["rules"]] == [str(proposed)]
    assert found["scopes"] == [
        {"scope_kind": "repository", "scope_key": SCOPE, "scope_label": REPO, "in_force_count": 2}
    ]
    assert _list(stack, scope_kind="agent").json() == {
        "rules": [],
        "scopes": [],
        "in_force_limit": 16,
    }


def test_list_unknown_agent_is_404_and_no_credential_is_401(stack: Stack) -> None:
    missing = stack.client.get(f"/agents/{uuid.uuid4()}/memory/rules", headers=stack.platform)
    assert missing.status_code == 404, missing.text
    assert missing.json()["detail"]["code"] == "memory_rules.not_found"
    anonymous = _list(stack, headers={})
    assert anonymous.status_code == 401, anonymous.text
    assert anonymous.headers["Cache-Control"] == "no-store"


# --- AC2: transitions ----------------------------------------------------------------


def _assert_transition(
    stack: Stack,
    rule_id: uuid.UUID,
    action: str,
    from_state: str,
    to_state: str,
    columns: tuple[str, str],
) -> dict[str, Any]:
    events_before = _events(rule_id)
    started = _clock()
    response = _decide(stack, rule_id, action)
    finished = _clock()

    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "no-store"
    found = response.json()
    assert found["applies_from"] == "next_ticket"
    row = _row(rule_id)
    assert row["state"] == to_state
    assert row["version"] == 2
    by, at = columns
    assert row[by] == stack.owner.principal_id
    assert started <= row[at] <= finished
    events = _events(rule_id)
    assert events[: len(events_before)] == events_before
    (event,) = events[len(events_before) :]
    assert {name: value for name, value in event.items() if name != "id"} == {
        "event": action,
        "from_state": from_state,
        "to_state": to_state,
        "actor_kind": "principal",
        "actor": str(stack.owner.principal_id),
        "principal_id": stack.owner.principal_id,
    }
    rule = found["rule"]
    assert rule["state"] == to_state and rule["version"] == 2
    assert rule[by] == {"principal_id": str(stack.owner.principal_id), "display_name": "Acme Owner"}
    # The answer is what the list route says of the same rule.
    (listed,) = [item for item in _list(stack).json()["rules"] if item["id"] == str(rule_id)]
    assert listed == rule
    return row


def test_transition_accept_records_the_writer(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    row = _assert_transition(
        stack, rule_id, "accept", "proposed", "accepted", ("accepted_by", "accepted_at")
    )
    assert row["rejected_by"] is None and row["removed_by"] is None


def test_transition_reject_records_the_writer(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    row = _assert_transition(
        stack, rule_id, "reject", "proposed", "rejected", ("rejected_by", "rejected_at")
    )
    assert row["accepted_by"] is None and row["removed_by"] is None


def test_transition_remove_records_the_writer_and_keeps_the_acceptance(stack: Stack) -> None:
    _link(stack, stack.owner)
    other = enrollment.seed_owner(stack.tenant_id, issuer=stack.issuer)
    rule_id = _rule(stack, state="accepted", decider=other.principal_id)
    accepted_at = _row(rule_id)["accepted_at"]
    row = _assert_transition(
        stack, rule_id, "remove", "accepted", "removed", ("removed_by", "removed_at")
    )
    assert row["accepted_by"] == other.principal_id and row["accepted_at"] == accepted_at


# --- AC3: refusals -------------------------------------------------------------------


def test_refusal_platform_key_is_401(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id, headers=stack.platform), 401, UNAUTHENTICATED, rule_id, before)
    beside = {**stack.owner.headers(), **stack.platform}
    _refused(_decide(stack, rule_id, headers=beside), 401, UNAUTHENTICATED, rule_id, before)
    _accepts(stack, rule_id)


def test_refusal_session_with_authorization_header_is_401(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    headers = {**stack.owner.headers(), "Authorization": "Bearer example-token"}
    _refused(_decide(stack, rule_id, headers=headers), 401, UNAUTHENTICATED, rule_id, before)
    _accepts(stack, rule_id)


def test_refusal_cross_origin_is_403(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    for origin in ("https://elsewhere.example", None):
        headers = stack.owner.headers(origin=origin)
        _refused(_decide(stack, rule_id, headers=headers), 403, ORIGIN_REJECTED, rule_id, before)
    assert stack.seen == []
    _accepts(stack, rule_id)


def test_refusal_principal_of_another_tenant_is_404(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    stranger_tenant = enrollment.seed_tenant()
    stranger = enrollment.seed_owner(stranger_tenant, issuer=stack.issuer)
    _link(stack, stranger)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id, owner=stranger)
    _refused(response, 404, "memory_rules.not_found", rule_id, before)
    unknown = _decide(stack, uuid.uuid4())
    _refused(unknown, 404, "memory_rules.not_found", rule_id, before)
    assert stack.seen == []
    _accepts(stack, rule_id)


def test_refusal_principal_without_a_link_needs_write_access(stack: Stack) -> None:
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    detail = _refused(
        _decide(stack, rule_id), 403, "memory_rules.write_access_required", rule_id, before
    )
    assert isinstance(detail, dict)
    assert "/identity/admin/{tenant_id}/code-host-links" in detail["fix"]
    assert stack.seen == []
    _link(stack, stack.owner)
    _accepts(stack, rule_id)


def _revoke_on_permission_read(link_id: uuid.UUID, revoked: list[uuid.UUID]) -> Override:
    """Revoke ``link_id`` once, through another connection, while the code host
    answers the permission read, then let the fake answer it as before."""

    def override(request: httpx.Request) -> httpx.Response | None:
        if revoked or "/collaborators/" not in request.url.path:
            return None
        statement = (
            "UPDATE curie.identity_links SET revoked_at = now() "
            "WHERE id = :id AND revoked_at IS NULL RETURNING id"
        )
        # This handler runs on the app's event loop; the update needs its own.
        with ThreadPoolExecutor(max_workers=1) as pool:
            (row,) = pool.submit(pe.rows, statement, {"id": link_id}).result(timeout=60)
        revoked.append(row["id"])
        return None

    return override


def test_refusal_link_revoked_during_the_code_host_read(stack: Stack) -> None:
    """The permission read answers ``write``; only the re-read of the exact link
    under the identity fence, after the provider reads, can refuse."""

    link_id = _link(stack, stack.owner)
    revoked: list[uuid.UUID] = []
    stack.overrides.append(_revoke_on_permission_read(link_id, revoked))
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.write_access_required", rule_id, before)
    assert revoked == [link_id]
    assert [r for r in stack.seen if "/collaborators/" in r.url.path]
    # Liveness: the same flow, with the override spent, accepts on a fresh link.
    _link(stack, stack.owner)
    _accepts(stack, rule_id)
    assert revoked == [link_id]


_DEACTIVATE = {
    "principal": (
        "UPDATE curie.principals SET status = :status WHERE id = :id RETURNING id",
        "disabled",
    ),
    "tenant": (
        "UPDATE curie.tenants SET status = :status WHERE id = :id RETURNING id",
        "suspended",
    ),
}


def _set_status(subject: str, row_id: uuid.UUID, active: bool) -> None:
    statement, inactive = _DEACTIVATE[subject]
    status = "active" if active else inactive
    (_found,) = pe.rows(statement, {"id": row_id, "status": status})


def _deactivate_on_permission_read(
    subject: str, row_id: uuid.UUID, done: list[uuid.UUID]
) -> Override:
    """Disable the principal or suspend its tenant once, through another
    connection, while the code host answers the permission read, then let the
    fake answer ``write`` as before."""

    def override(request: httpx.Request) -> httpx.Response | None:
        if done or "/collaborators/" not in request.url.path:
            return None
        # This handler runs on the app's event loop; the update needs its own.
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(_set_status, subject, row_id, False).result(timeout=60)
        done.append(row_id)
        return None

    return override


@pytest.mark.parametrize("subject", ["principal", "tenant"])
def test_refusal_principal_deactivated_during_the_code_host_read(
    stack: Stack, subject: str
) -> None:
    """The session authenticated an active principal and the permission read
    answers ``write``; only the re-read of the principal and its tenant under
    the identity fence, after the provider reads, can refuse."""

    _link(stack, stack.owner)
    row_id = stack.owner.principal_id if subject == "principal" else stack.tenant_id
    done: list[uuid.UUID] = []
    stack.overrides.append(_deactivate_on_permission_read(subject, row_id, done))
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id), 401, UNAUTHENTICATED, rule_id, before)
    assert done == [row_id]
    assert [r for r in stack.seen if "/collaborators/" in r.url.path]
    # Liveness: active again, the same flow (override spent) accepts.
    _set_status(subject, row_id, True)
    _accepts(stack, rule_id)
    assert done == [row_id]


def test_refusal_revoked_link_needs_write_access(stack: Stack) -> None:
    link_id = _link(stack, stack.owner)
    revoked = stack.client.post(
        f"/identity/admin/{stack.tenant_id}/links/{link_id}/revoke", headers=stack.platform
    )
    assert revoked.status_code == 200, revoked.text
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id), 403, "memory_rules.write_access_required", rule_id, before)
    _link(stack, stack.owner)
    _accepts(stack, rule_id)


@pytest.mark.parametrize("permission", ["read", "triage", "none"])
def test_refusal_account_without_write_permission(stack: Stack, permission: str) -> None:
    _link(stack, stack.owner)
    stack.github.permissions[SENDER] = (SENDER_ID, permission)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id), 403, "memory_rules.write_access_required", rule_id, before)
    stack.github.permissions[SENDER] = (SENDER_ID, "write")
    _accepts(stack, rule_id)


def test_refusal_permission_payload_for_another_account_id(stack: Stack) -> None:
    _link(stack, stack.owner)
    # The login now answers for a different immutable account.
    stack.github.permissions[SENDER] = (SENDER_ID + 1000, "admin")
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id), 403, "memory_rules.write_access_required", rule_id, before)
    stack.github.permissions[SENDER] = (SENDER_ID, "admin")
    _accepts(stack, rule_id)


def test_refusal_unknown_account_id_needs_write_access(stack: Stack) -> None:
    _link(stack, stack.owner)
    del stack.github.accounts[str(SENDER_ID)]
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    _refused(_decide(stack, rule_id), 403, "memory_rules.write_access_required", rule_id, before)
    assert not [r for r in stack.seen if "/collaborators/" in r.url.path]
    stack.github.accounts[str(SENDER_ID)] = {"id": SENDER_ID, "login": SENDER, "type": "User"}
    _accepts(stack, rule_id)


def _moved(answer: str) -> Override:
    def override(request: httpx.Request) -> httpx.Response | None:
        if request.method != "GET" or request.url.path != f"/repositories/{OLD_REPO_ID}":
            return None
        if answer == "missing":
            return httpx.Response(404, json={"message": "Not Found"})
        # The old id answering for the repository now at the label's path.
        return httpx.Response(
            200, json={"id": REPO_ID, "full_name": REPO, "default_branch": "main"}
        )

    return override


@pytest.mark.parametrize("answer", ["other_id", "missing"])
def test_refusal_scope_id_no_longer_names_the_labelled_repository(
    stack: Stack, answer: str
) -> None:
    """The label's path now holds a repository where this account can write;
    the decision follows the immutable id and never reads that permission."""

    _link(stack, stack.owner)
    stack.overrides.append(_moved(answer))
    rule_id = _rule(stack, scope_key=f"github:{HOST}:{OLD_REPO_ID}")
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.write_access_unverifiable", rule_id, before)
    assert [r.url.path for r in stack.seen] == [f"/repositories/{OLD_REPO_ID}"]
    assert not [r for r in stack.seen if "/collaborators/" in r.url.path]
    _accepts(stack, _rule(stack))


@pytest.mark.parametrize(
    "scope_key",
    [
        f"github:{HOST}:{PATH_ID_PREFIX}{REPO}",
        f"github:{HOST}:repo-{REPO_ID}",
        "gitlab:gitlab.example.com:77",
        "github",
    ],
    ids=["path_form", "non_numeric", "kind_without_identity", "malformed"],
)
def test_refusal_scope_key_that_cannot_be_verified(stack: Stack, scope_key: str) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack, scope_key=scope_key)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.write_access_unverifiable", rule_id, before)
    assert stack.seen == []
    _accepts(stack, _rule(stack))


def test_refusal_code_host_not_configured_is_unverifiable(stack: Stack) -> None:
    unconfigured = "github.example.com"
    _link(stack, stack.owner, host=unconfigured)
    _link(stack, stack.owner, account_id=SENDER_ID)
    rule_id = _rule(stack, scope_key=f"github:{unconfigured}:{REPO_ID}")
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.write_access_unverifiable", rule_id, before)
    assert stack.seen == []
    _accepts(stack, _rule(stack))


def test_refusal_two_linked_accounts_on_the_host_are_unverifiable(stack: Stack) -> None:
    _link(stack, stack.owner)
    second = _link(stack, stack.owner, account_id=SECOND_ID)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.write_access_unverifiable", rule_id, before)
    assert stack.seen == []
    revoked = stack.client.post(
        f"/identity/admin/{stack.tenant_id}/links/{second}/revoke", headers=stack.platform
    )
    assert revoked.status_code == 200, revoked.text
    _accepts(stack, rule_id)


@pytest.mark.parametrize("failing", ["/repositories/", "/user/", "/collaborators/"])
def test_refusal_code_host_transport_failure_is_503(stack: Stack, failing: str) -> None:
    _link(stack, stack.owner)

    def unreachable(request: httpx.Request) -> httpx.Response | None:
        if failing in request.url.path:
            raise httpx.ConnectError("connection refused", request=request)
        return None

    stack.overrides.append(unreachable)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 503, "memory_rules.code_host_unavailable", rule_id, before)
    stack.overrides.clear()
    _accepts(stack, rule_id)


def test_refusal_code_host_server_error_is_503(stack: Stack) -> None:
    _link(stack, stack.owner)
    stack.github.fail_reads(r"/collaborators/", 502)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 503, "memory_rules.code_host_unavailable", rule_id, before)
    _accepts(stack, rule_id)


def test_refusal_stale_version_carries_the_current_one(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    detail = _refused(
        _decide(stack, rule_id, version=2), 409, "memory_rules.stale_version", rule_id, before
    )
    assert isinstance(detail, dict) and detail["current_version"] == 1
    _accepts(stack, rule_id)
    removed = _decide(stack, rule_id, "remove", version=1)
    after = _snapshot(rule_id)
    detail = _refused(removed, 409, "memory_rules.stale_version", rule_id, after)
    assert isinstance(detail, dict) and detail["current_version"] == 2
    assert _decide(stack, rule_id, "remove", version=2).status_code == 200


@pytest.mark.parametrize(
    ("state", "action"),
    [
        ("accepted", "accept"),
        ("accepted", "reject"),
        ("proposed", "remove"),
        ("rejected", "accept"),
        ("removed", "remove"),
    ],
)
def test_refusal_invalid_transition(stack: Stack, state: str, action: str) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack, state=state)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id, action)
    _refused(response, 409, "memory_rules.invalid_transition", rule_id, before)
    _accepts(stack, _rule(stack))


@pytest.mark.parametrize("action", ["accept", "reject", "remove"])
@pytest.mark.parametrize("state", ["superseded", "discarded"])
def test_refusal_invalid_transition_from_an_unlisted_state(
    stack: Stack, state: str, action: str
) -> None:
    """A rule the list never shows still answers the transition refusal, not 404.
    A candidate needs an execution request row this module does not seed."""

    _link(stack, stack.owner)
    rule_id = _rule(stack, state=state)
    before = _snapshot(rule_id)
    assert before[0]["version"] == 1
    response = _decide(stack, rule_id, action)
    _refused(response, 409, "memory_rules.invalid_transition", rule_id, before)
    _accepts(stack, _rule(stack))


@pytest.mark.parametrize("scope_kind", ["agent", "binding"])
def test_refusal_scope_not_supported(stack: Stack, scope_kind: str) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack, scope_kind=scope_kind, scope_key="slack:C0EXAMPLE1")
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id)
    _refused(response, 403, "memory_rules.scope_not_supported", rule_id, before)
    assert stack.seen == []
    _accepts(stack, _rule(stack))


@pytest.mark.parametrize(
    "body",
    [
        {"expected_version": 1, "note": "extra"},
        {"expected_version": True},
        {"expected_version": "1"},
        {"expected_version": 0},
        {},
    ],
    ids=["extra_key", "bool", "string", "zero", "missing"],
)
def test_refusal_malformed_body_is_422(stack: Stack, body: dict[str, Any]) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    response = _decide(stack, rule_id, body=body)
    _refused(response, 422, "memory_rules.invalid_request", rule_id, before)
    assert stack.seen == []
    _accepts(stack, rule_id)


# --- AC4: the per-scope cap ----------------------------------------------------------


def test_cap_refuses_the_seventeenth_accepted_rule(stack: Stack) -> None:
    _link(stack, stack.owner)
    accepted = [_rule(stack, state="accepted", minute=index) for index in range(16)]
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    detail = _refused(_decide(stack, rule_id), 409, "memory_rules.scope_full", rule_id, before)
    assert isinstance(detail, dict) and detail["limit"] == 16
    assert _accepted_count(stack) == 16
    # Reject never meets the cap; nor does remove, which makes room.
    rejected = _rule(stack)
    assert _decide(stack, rejected, "reject").status_code == 200
    assert _decide(stack, accepted[0], "remove").status_code == 200
    _accepts(stack, rule_id)
    assert _accepted_count(stack) == 16


def _await_lock_waiters(minimum: int, requests: list[Future[httpx.Response]]) -> None:
    """Wait until ``minimum`` requests wait on the agent row lock in Postgres."""

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        (found,) = pe.rows(
            "SELECT count(*) AS n FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
            "AND query LIKE '%FROM curie.agents%' AND query LIKE '%FOR UPDATE%'"
        )
        if int(found["n"]) >= minimum:
            return
        done = [request.result().text for request in requests if request.done()]
        assert not done, f"a request finished before it waited on the agent lock: {done}"
    raise AssertionError(f"fewer than {minimum} requests waited on the agent lock")


def test_cap_two_overlapping_accepts_leave_exactly_sixteen(stack: Stack) -> None:
    _link(stack, stack.owner)
    second = _second_owner(stack)
    for index in range(15):
        _rule(stack, state="accepted", minute=index)
    sixteenth, seventeenth = _rule(stack), _rule(stack)
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        with enrollment.row_locked(
            "SELECT id FROM curie.agents WHERE id = :id FOR UPDATE", {"id": stack.agent_id}
        ):
            requests = [
                pool.submit(_decide, stack, sixteenth, owner=stack.owner),
                pool.submit(_decide, stack, seventeenth, owner=second),
            ]
            _await_lock_waiters(2, requests)
        responses = [request.result(timeout=60) for request in requests]
    finally:
        pool.shutdown(wait=True)

    statuses = sorted(response.status_code for response in responses)
    assert statuses == [200, 409], [response.text for response in responses]
    (refused,) = [response for response in responses if response.status_code == 409]
    assert refused.json()["detail"]["code"] == "memory_rules.scope_full"
    assert _accepted_count(stack) == 16
    states = {_row(sixteenth)["state"], _row(seventeenth)["state"]}
    assert states == {"accepted", "proposed"}
    (still,) = [rule for rule in (sixteenth, seventeenth) if _row(rule)["state"] == "proposed"]
    assert _events(still) == []


# --- brief 1: the snapshot byte bound ------------------------------------------------


def test_snapshot_sixteen_long_rules_fit_with_small_numbers(stack: Stack) -> None:
    _link(stack, stack.owner)
    for index in range(1, 16):
        _rule(
            stack, state="accepted", number=index, text=_text(index, MAX_RULE_BYTES), minute=index
        )
    last = _rule(stack, number=16, text=_text(16, MAX_RULE_BYTES))
    _accepts(stack, last)
    assert _accepted_count(stack) == 16


def test_snapshot_refuses_long_rules_with_large_numbers(stack: Stack) -> None:
    _link(stack, stack.owner)
    base = 10**18
    for index in range(1, 16):
        _rule(
            stack,
            state="accepted",
            number=base + index,
            text=_text(index, MAX_RULE_BYTES),
            minute=index,
        )
    rule_id = _rule(stack, number=base + 16, text=_text(16, MAX_RULE_BYTES))
    before = _snapshot(rule_id)
    detail = _refused(_decide(stack, rule_id), 409, "memory_rules.snapshot_full", rule_id, before)
    assert isinstance(detail, dict) and detail["limit_bytes"] == MAX_SNAPSHOT_BYTES
    assert _accepted_count(stack) == 15
    # A short rule still fits beside the same fifteen.
    _accepts(stack, _rule(stack, number=base + 17, text="Keep it short."))
    assert _accepted_count(stack) == 16


# --- the console's recorded requests and responses ----------------------------------

# The console's client (`apps/ui/src/api/client.ts`) is the caller of these routes.
# Its requests are recorded by `apps/ui/src/api/memoryRules.client.test.ts` and
# replayed here against the registered routes; the routes' answers are recorded here
# and read back through that client by the console's own tests. Each side fails when
# its vector drifts, so a renamed path, a renamed body field or a dropped refusal code
# cannot slip between the two.
CONSOLE_REQUESTS = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "memory-rules-console-requests.json"
)
CONSOLE_RESPONSES = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "vectors"
    / "memory-rules-console-responses.json"
)
CONSOLE_RESPONSES_NOTE = (
    "Real answers of the learned rule decision routes, recorded by "
    "apps/api/tests/test_memory_rule_routes.py::"
    "test_console_responses_match_the_console_vector, which fails when they drift. "
    "Ids and clocks are placeholders. Refresh: CURIE_WRITE_MEMORY_RULES_VECTOR=1 "
    "uv run pytest apps/api/tests/test_memory_rule_routes.py -k console_vector"
)


def _recorded(response: httpx.Response) -> dict[str, Any]:
    return {"status": response.status_code, "body": response.json()}


def test_console_responses_match_the_console_vector(stack: Stack) -> None:
    _link(stack, stack.owner)
    other = enrollment.seed_owner(stack.tenant_id, issuer=stack.issuer)
    accepted = _decide(stack, _rule(stack, minute=1))
    rejected = _decide(stack, _rule(stack, minute=2), "reject")
    removed = _decide(
        stack,
        _rule(stack, state="accepted", decider=other.principal_id, minute=3),
        "remove",
    )
    stale = _decide(stack, _rule(stack, minute=4), version=2)
    unlinked = _decide(stack, _rule(stack, minute=5), owner=other)

    def unreachable(request: httpx.Request) -> httpx.Response | None:
        if "/collaborators/" in request.url.path:
            raise httpx.ConnectError("connection refused", request=request)
        return None

    stack.overrides.append(unreachable)
    unavailable = _decide(stack, _rule(stack, minute=6))

    expected = {
        "accept": (accepted, 200, "accepted"),
        "reject": (rejected, 200, "rejected"),
        "remove": (removed, 200, "removed"),
    }
    for name, (response, status, state) in expected.items():
        assert response.status_code == status, (name, response.text)
        assert response.json()["rule"]["state"] == state, name
    refusals = {
        "refusal_stale_version": (stale, 409, "memory_rules.stale_version"),
        "refusal_write_access_required": (unlinked, 403, "memory_rules.write_access_required"),
        "refusal_code_host_unavailable": (unavailable, 503, "memory_rules.code_host_unavailable"),
    }
    for name, (response, status, code) in refusals.items():
        assert response.status_code == status, (name, response.text)
        assert response.json()["detail"]["code"] == code, name

    recorded = _stable(
        {
            "accept": _recorded(accepted),
            "reject": _recorded(rejected),
            "remove": _recorded(removed),
            "refusal_stale_version": _recorded(stale),
            "refusal_write_access_required": _recorded(unlinked),
            "refusal_code_host_unavailable": _recorded(unavailable),
        }
    )
    document = {"_comment": CONSOLE_RESPONSES_NOTE} | recorded
    if os.environ.get("CURIE_WRITE_MEMORY_RULES_VECTOR") == "1":
        CONSOLE_RESPONSES.write_text(json.dumps(document, indent=2) + "\n")
    assert json.loads(CONSOLE_RESPONSES.read_text()) == document, (
        f"{CONSOLE_RESPONSES.name} no longer matches the routes; refresh it as its _comment says"
    )


def _console_requests(stack: Stack, rule_ids: dict[str, uuid.UUID]) -> dict[str, dict[str, Any]]:
    """The requests the console client recorded, with the placeholder ids replaced."""

    recorded = json.loads(CONSOLE_REQUESTS.read_text())
    found: dict[str, dict[str, Any]] = {}
    for request in recorded["requests"]:
        operation = request["operation"]
        path = request["path"].replace(recorded["agent_id"], str(stack.agent_id))
        if operation != "list":
            path = path.replace(recorded["rule_id"], str(rule_ids[operation]))
        found[operation] = {"method": request["method"], "path": path, "body": request["body"]}
    assert set(found) == {"list", "accept", "reject", "remove"}, set(found)
    return found


def _send(
    stack: Stack, request: dict[str, Any], *, path: str | None = None, body: Any = None
) -> httpx.Response:
    return stack.client.request(
        request["method"],
        path or request["path"],
        json=body if body is not None else request["body"],
        headers=stack.owner.headers(),
    )


def test_console_client_requests_are_served_by_the_registered_routes(stack: Stack) -> None:
    _link(stack, stack.owner)
    other = enrollment.seed_owner(stack.tenant_id, issuer=stack.issuer)
    to_accept = _rule(stack, minute=1)
    to_reject = _rule(stack, minute=2)
    to_remove = _rule(stack, state="accepted", decider=other.principal_id, minute=3)
    requests = _console_requests(
        stack, {"accept": to_accept, "reject": to_reject, "remove": to_remove}
    )

    listed = _send(stack, requests["list"])
    assert listed.status_code == 200, listed.text
    assert {item["id"] for item in listed.json()["rules"]} == {
        str(to_accept),
        str(to_reject),
        str(to_remove),
    }
    for operation, rule_id, state in (
        ("accept", to_accept, "accepted"),
        ("reject", to_reject, "rejected"),
        ("remove", to_remove, "removed"),
    ):
        answered = _send(stack, requests[operation])
        assert answered.status_code == 200, (operation, answered.text)
        assert answered.json()["rule"]["state"] == state, operation
        assert _row(rule_id)["state"] == state, operation


def test_console_client_request_negative_controls_are_refused(stack: Stack) -> None:
    _link(stack, stack.owner)
    rule_id = _rule(stack)
    before = _snapshot(rule_id)
    requests = _console_requests(stack, {"accept": rule_id, "reject": rule_id, "remove": rule_id})

    # A wrong path: the console once called Dismiss by that name, and no route has it.
    wrong_path = requests["reject"]["path"].replace("/reject", "/dismiss")
    missing = _send(stack, requests["reject"], path=wrong_path)
    assert missing.status_code == 404, missing.text
    assert _snapshot(rule_id) == before

    # A wrong body: the rule's version under another name is refused, never read.
    for body in ({"version": 1}, {"expected_version": 1, "version": 1}, {}):
        malformed = _send(stack, requests["accept"], body=body)
        assert malformed.status_code == 422, (body, malformed.text)
        assert malformed.json()["detail"]["code"] == "memory_rules.invalid_request"
        assert _snapshot(rule_id) == before

    # Liveness: the recorded request itself still goes through.
    assert _send(stack, requests["accept"]).status_code == 200
