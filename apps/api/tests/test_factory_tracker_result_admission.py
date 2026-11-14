"""Tracker result bindings deploy and admit tracker result requests (ADR 0277, #5586 AC1).

A version whose bundle carries a valid ``workflow/result.json`` deploys
through the bundle upload route and keeps its declaration digest and policy.
A trigger label a write-access human applies (a signed GitHub issues webhook
against ``create_app()``, the fixtures test_factory_tracker_results.py uses)
then admits one WorkItem whose request carries the ``tracker_result``
contract and that digest. Each deploy-time refusal answers its own code and
leaves no version row; a binding without a declaration admits exactly as
before. The admission recheck refuses, as a label no binding serves, when the
bound tracker cannot create a linked issue: the GitHub tracker's declaration
is replaced by an in-memory tracker's that marks the operation unsupported.
The worker's admission route, a second entry point into the same dispatch
admission, selects the same contract. A mention never revises a tracker
result WorkItem, and the worker reads the contract from the acquire, start
and running answers and the request view.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import hashlib
import io
import json
import sys
import tarfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.memory import InMemoryTracker, all_supported
from forge_fakes import factory_wake
from forge_fakes.github import (
    INSTALLATION_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    GitHubAPI,
    _comment_event,
    _issue_event,
    _post,
)
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)

MANIFEST = '{"name": "triage-plugin", "version": "0.1.0"}'
SKILL = "---\nname: triage\ndescription: investigates one issue\n---\n\n# triage\n"
FINDINGS_ONLY: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["findings"],
}
WITH_TICKET: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["ticket", "findings"],
    "implementation_label": "curie-implement",
}
WORKER_TOKEN = "factory-terminus-worker"


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _digest(declaration: dict[str, Any]) -> str:
    """The canonical JSON digest, derived here independently of the parser."""

    canonical = json.dumps(declaration, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _bundle(declaration: dict[str, Any] | None) -> bytes:
    files = {".claude-plugin/plugin.json": MANIFEST, "skills/triage/SKILL.md": SKILL}
    if declaration is not None:
        files["workflow/result.json"] = json.dumps(declaration)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for rel, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"triage-plugin/{rel}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _agent_id() -> str:
    (agent,) = _rows("SELECT id FROM curie.agents")
    return str(agent["id"])


def _set_publication_policy(client: Any, agent_id: str, policy: str) -> None:
    resp = client.patch(
        f"/agents/{agent_id}", json={"publication_policy": policy}, headers=_headers()
    )
    assert resp.status_code == 200, resp.text


def _upload(client: Any, agent_id: str, declaration: dict[str, Any] | None) -> tuple[str, Any]:
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "triage-test"},
        headers=_headers(),
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("triage.tar.gz", _bundle(declaration))},
        headers=_headers(),
    )
    return version_id, resp


def _deployed(client: Any, agent_id: str, declaration: dict[str, Any] | None) -> str:
    version_id, uploaded = _upload(client, agent_id, declaration)
    assert uploaded.status_code == 201, uploaded.text
    deployed = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=_headers(),
    )
    assert deployed.status_code == 201, deployed.text
    return version_id


def _versions(agent_id: str) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, result_policy_schema_version, result_policy_digest, result_policy "
        "FROM curie.agent_versions WHERE agent_id = :agent",
        {"agent": uuid.UUID(agent_id)},
    )


def _label(client: Any, github: GitHubAPI, number: int) -> Any:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    return response.json()


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.completion_contract, r.result_policy_digest, "
        "r.model_session_reserved_at FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )


def _work_items(number: int) -> int:
    rows = _rows(
        "SELECT count(*) AS n FROM curie.work_items "
        "WHERE tracker_scope_id = :scope AND tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    return int(rows[0]["n"])


def _without_linked_issues(monkeypatch: pytest.MonkeyPatch) -> None:
    """The GitHub tracker declares what an in-memory tracker without
    `Operation.CREATE_LINKED_ISSUE` declares; every other operation is unchanged."""

    from curie_api.forges.github.tracker import GitHubTracker

    declared = InMemoryTracker(
        capabilities={
            **all_supported(TRACKER_OPERATIONS),
            Operation.CREATE_LINKED_ISSUE: Support.UNSUPPORTED,
        }
    ).capabilities
    monkeypatch.setattr(GitHubTracker, "capabilities", declared)


# Deploy --------------------------------------------------------------------------


@pytest.mark.parametrize("declaration", [FINDINGS_ONLY, WITH_TICKET], ids=["findings", "ticket"])
def test_a_declared_version_deploys_and_keeps_its_digest_and_policy(
    admitted: Any, declaration: dict[str, Any]
) -> None:
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")

    version_id = _deployed(client, agent_id, declaration)

    assert _versions(agent_id) == [
        {
            "id": uuid.UUID(version_id),
            "result_policy_schema_version": 1,
            "result_policy_digest": _digest(declaration),
            "result_policy": {
                "results": sorted(declaration["results"]),
                "implementation_label": declaration.get("implementation_label"),
            },
        }
    ]


@pytest.mark.parametrize(
    ("setup", "declaration", "code"),
    [
        pytest.param(
            "approve",
            FINDINGS_ONLY,
            "tracker_result_needs_unattended_publication",
            id="needs-unattended-publication",
        ),
        pytest.param(
            "self_label",
            {**WITH_TICKET, "implementation_label": LABEL},
            "tracker_result_target_is_self",
            id="target-is-self",
        ),
        pytest.param(
            "no_linked_issue", WITH_TICKET, "tracker_result_unsupported", id="unsupported"
        ),
    ],
)
def test_each_deploy_refusal_answers_its_code_and_leaves_no_version(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    setup: str,
    declaration: dict[str, Any],
    code: str,
) -> None:
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "approve" if setup == "approve" else "auto")
    if setup == "no_linked_issue":
        _without_linked_issues(monkeypatch)

    _version_id, resp = _upload(client, agent_id, declaration)

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["code"] == code
    assert _versions(agent_id) == []
    assert (
        _rows(
            "SELECT id FROM curie.deployments WHERE agent_id = :agent",
            {"agent": uuid.UUID(agent_id)},
        )
        == []
    )


@pytest.mark.parametrize(
    ("setup", "declaration"),
    [
        pytest.param("auto", FINDINGS_ONLY, id="auto-publication"),
        pytest.param(
            "self_label", {**WITH_TICKET, "implementation_label": "curie-other"}, id="other-label"
        ),
        pytest.param("no_linked_issue", FINDINGS_ONLY, id="findings-without-linked-issues"),
    ],
)
def test_the_same_agent_deploys_once_the_refused_fact_is_fixed(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    setup: str,
    declaration: dict[str, Any],
) -> None:
    # Liveness for each refusal above: the one fact changed, the deploy passes.
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    if setup == "no_linked_issue":
        _without_linked_issues(monkeypatch)

    version_id = _deployed(client, agent_id, declaration)

    (version,) = _versions(agent_id)
    assert version["id"] == uuid.UUID(version_id)
    assert version["result_policy_digest"] == _digest(declaration)


# Admission -----------------------------------------------------------------------


def test_a_human_label_on_a_declared_binding_admits_one_tracker_result_request(
    admitted: Any,
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, WITH_TICKET)

    assert _label(client, github, 9101)["status"] == "factory_admitted"

    assert _work_items(9101) == 1
    (request,) = _requests(9101)
    assert request["completion_contract"] == "tracker_result"
    assert request["result_policy_digest"] == _digest(WITH_TICKET)
    # Admission reserves no model session; the runner's first contract call does.
    assert request["model_session_reserved_at"] is None


@pytest.mark.parametrize("deployed", [True, False], ids=["undeclared-version", "no-deployment"])
def test_a_binding_without_a_declaration_admits_exactly_as_before(
    admitted: Any, deployed: bool
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    if deployed:
        _deployed(client, agent_id, None)
        assert _versions(agent_id)[0]["result_policy"] is None

    assert _label(client, github, 9102)["status"] == "factory_admitted"

    assert _work_items(9102) == 1
    (request,) = _requests(9102)
    assert request["completion_contract"] == "implementation"
    assert request["result_policy_digest"] is None


def test_the_admission_recheck_refuses_a_tracker_that_cannot_create_linked_issues(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, WITH_TICKET)
    # The tracker changed after the deploy: it can no longer create the ticket.
    _without_linked_issues(monkeypatch)

    outcome = _label(client, github, 9103)

    # Exactly what a label no binding serves answers, and nothing is written.
    assert outcome["status"] == "factory_ignored", outcome
    assert [error["code"] for error in outcome["errors"]] == ["binding_missing"]
    assert _work_items(9103) == 0
    assert _requests(9103) == []


def test_the_admission_recheck_still_admits_findings_only_without_linked_issues(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, FINDINGS_ONLY)
    _without_linked_issues(monkeypatch)

    assert _label(client, github, 9104)["status"] == "factory_admitted"

    (request,) = _requests(9104)
    assert request["completion_contract"] == "tracker_result"
    assert request["result_policy_digest"] == _digest(FINDINGS_ONLY)


def _worker_admission(client: Any, agent_id: str, number: int) -> Any:
    tracker = get_settings().forges.trackers[0]
    return client.post(
        "/v1/internal/work-items/admissions",
        headers={"X-Curie-Worker-Token": WORKER_TOKEN},
        json={
            "agent_id": agent_id,
            "kind": "github",
            "address": REPO,
            "reply_conversation_id": f"issue-{number}",
            "tracker": {
                "kind": "github",
                "host": tracker.host,
                "scope_id": str(REPO_ID),
                "issue_id": str(number),
            },
            "repository": {
                "code_host_kind": "github",
                "host": tracker.host,
                "project_id": str(REPO_ID),
                "path": REPO,
            },
            "code_host_installation_id": INSTALLATION_ID,
            "objective": f"Investigate {REPO}#{number}",
            "requester": "github:1:acme-dev",
            "request_id": str(uuid.uuid4()),
        },
    )


@pytest.mark.parametrize(
    ("declaration", "contract"),
    [(WITH_TICKET, "tracker_result"), (None, "implementation")],
    ids=["declared", "undeclared"],
)
def test_another_admission_entry_point_selects_the_same_contract(
    admitted: Any, declaration: dict[str, Any] | None, contract: str
) -> None:
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, declaration)

    resp = _worker_admission(client, agent_id, 9105)

    assert resp.status_code == 200, resp.text
    (request,) = _requests(9105)
    assert request["completion_contract"] == contract
    assert request["result_policy_digest"] == (
        None if declaration is None else _digest(declaration)
    )


def test_another_admission_entry_point_refuses_the_recheck_like_a_missing_binding(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, WITH_TICKET)
    _without_linked_issues(monkeypatch)

    resp = _worker_admission(client, agent_id, 9106)

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == {"code": "binding_missing"}
    assert _work_items(9106) == 0


# Revisions -----------------------------------------------------------------------


@contextmanager
def _comments_on_the_tracker_api(client: Any, github: GitHubAPI) -> Iterator[None]:
    """Answer a comment read with the ``issue_url`` GitHub gives on the API that served it.

    ``GitHubAPI`` names ``https://api.github.com`` in a comment's
    ``issue_url``, while the ``admitted`` fixture's tracker reads from the
    loopback stand-in, so the mention verifier would refuse every comment as
    ``comment_target_mismatch``. Every other answer is the stand-in's own.
    """

    def handle(request: httpx.Request) -> httpx.Response:
        response = github.handle(request)
        if request.method == "GET" and "/issues/comments/" in request.url.path:
            api = str(request.url).split("/repos/", 1)[0]
            body = {
                **response.json(),
                "issue_url": f"{api}/repos/{REPO}/issues/{github.issue_number}",
            }
            return httpx.Response(response.status_code, json=body)
        return response

    original = client.app.state.http_client
    wrapped = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client.app.state.http_client = wrapped
    try:
        yield
    finally:
        client.app.state.http_client = original
        client.portal.call(wrapped.aclose)


@pytest.mark.parametrize(
    ("declaration", "status", "code", "requests"),
    [
        pytest.param(
            WITH_TICKET, "factory_ignored", "tracker_result_no_revision", 1, id="tracker-result"
        ),
        pytest.param(None, "factory_queued", None, 2, id="implementation"),
    ],
)
def test_a_mention_revises_an_implementation_work_item_and_never_a_tracker_result_one(
    admitted: Any,
    declaration: dict[str, Any] | None,
    status: str,
    code: str | None,
    requests: int,
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, declaration)
    assert _label(client, github, 9107)["status"] == "factory_admitted"

    github.comment_body = f"Please revise @{MENTION}"
    payload = _comment_event(9107, github.comment_body, 7107)
    payload["comment"]["author_association"] = "NONE"
    with _comments_on_the_tracker_api(client, github):
        mention = _post(client, "issue_comment", payload)

    assert mention.status_code == 200, mention.text
    outcome = mention.json()
    assert outcome["status"] == status, outcome
    if code is not None:
        assert [error["code"] for error in outcome["errors"]] == [code]
    assert len(_requests(9107)) == requests


# The worker learns the contract ------------------------------------------------------


def _internal(client: Any, method: str, path: str, **kwargs: Any) -> Any:
    return client.request(
        method,
        f"/v1/internal/work-items{path}",
        headers={"X-Curie-Worker-Token": WORKER_TOKEN},
        **kwargs,
    )


@pytest.mark.parametrize(
    ("declaration", "contract"),
    [(FINDINGS_ONLY, "tracker_result"), (None, "implementation")],
    ids=["declared", "undeclared"],
)
def test_acquire_start_running_and_the_request_view_carry_the_contract(
    admitted: Any, declaration: dict[str, Any] | None, contract: str
) -> None:
    client, github, _sink = admitted
    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    _deployed(client, agent_id, declaration)
    assert _label(client, github, 9108)["status"] == "factory_admitted"
    (request,) = _requests(9108)
    request_id = request["id"]

    async def wake() -> dict[str, Any]:
        async with client.app.state.sessionmaker() as session:
            found = await factory_wake(session, request_id)
            return found.model_dump(mode="json")

    woken = client.portal.call(wake)
    (generation,) = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    acquired = _internal(
        client,
        "POST",
        f"/requests/{request_id}/acquire",
        json={
            "owner": "triage-owner",
            "generation": generation["dispatch_generation"],
            "wake": woken,
            "context": None,
        },
    )
    assert acquired.status_code == 200, acquired.text
    assert acquired.json()["completion_contract"] == contract

    started = _internal(
        client,
        "POST",
        f"/requests/{request_id}/start",
        json={
            "owner": "triage-owner",
            "generation": generation["dispatch_generation"],
            "claim_name": f"claim-{request_id.hex[:12]}",
            "sandbox_name": f"sbx-{request_id.hex[:12]}",
            "context": acquired.json()["context"],
        },
    )
    assert started.status_code == 200, started.text
    assert started.json()["completion_contract"] == contract

    running = _internal(
        client,
        "GET",
        "/running",
        params={
            "conversation_id": acquired.json()["conversation_id"],
            "wake_kind": woken["kind"],
            "wake_address": woken["address"],
            "wake_conversation_id": woken["conversation_id"],
            "wake_adapter": woken["adapter"] or "",
        },
    )
    assert running.status_code == 200, running.text
    assert running.json()["completion_contract"] == contract

    view = _internal(client, "GET", f"/requests/{request_id}")
    assert view.status_code == 200, view.text
    assert view.json()["completion_contract"] == contract
