"""Each handoff of a learned rule proposal feeds the real producer's output to the
real consumer (#5796, ADR 0287), and rejects a named counterexample.

1. The runner's ``propose_rule`` handler to the proposal route: the handler
   posts to an in-process server that forwards the exact request bytes and
   the capability header to ``POST /work-items/rule-proposal`` and returns
   the API's status and body unchanged.
2. The completion contract to the runner's contract parser.
3. The route's stored candidate to the terminal writer's settlement.
4. A terminal lineage write to promotion, against a non-terminal write of the
   same lineage.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_api.crud import lineages as crud_lineages
from curie_api.forges.types import PullRequestState
from curie_api.models import ThreadPublicationLineage
from curie_runner.completion_contract import ContractUnavailable, parse_contract
from curie_runner.issue_read import CAPABILITY_HEADER
from curie_runner.propose_rule import propose_rule_handler
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_memory_rule_lifecycle import (  # noqa: E402
    NEXT_HEAD,
    RULE_A,
    RULE_B,
    _advance,
    _by_text,
    _events_of,
    _propose,
    _revision_candidates,
)
from test_memory_rule_proposal import (  # noqa: E402
    CAPABILITY,
    PROPOSAL_URL,
    REASON,
    RULE,
    ReviewGitHubAPI,
    contract,
    eligible_revision,
    reconcile,
    rules,  # noqa: F401  (fixture)
    stored_events,
    stored_rules,
    switch,
)
from test_workitem_outcomes import stack  # noqa: E402, F401  (fixture)

EXTRAS = {
    "scope_key": "github:999:other/repository",
    "agent_id": str(uuid.uuid4()),
    "source_url": "https://example.com/elsewhere",
}


def _text_of(result: dict[str, Any]) -> str:
    return "".join(part["text"] for part in result["content"])


# 1. Runner tool output to the proposal route ------------------------------------


def test_the_runner_handler_body_is_what_the_route_stores(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    run = eligible_revision(client, api)

    async def forward(request: web.Request) -> web.Response:
        raw = await request.read()
        headers = {
            CAPABILITY: request.headers.get(CAPABILITY_HEADER, ""),
            "Content-Type": request.headers.get("Content-Type", "application/json"),
        }
        answered = await asyncio.to_thread(client.post, PROPOSAL_URL, content=raw, headers=headers)
        return web.Response(
            status=answered.status_code,
            body=answered.content,
            content_type="application/json",
        )

    async def scenario() -> tuple[tuple[int, Any], list[Any], dict[str, Any], dict[str, Any]]:
        app = web.Application()
        app.router.add_post("/rule-proposal", forward)
        server = TestServer(app)
        await server.start_server()
        try:
            url = str(server.make_url("/rule-proposal"))
            # The counterexample: a body carrying a field the platform owns is
            # refused at the same forwarded route.
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    url,
                    json={"rule": RULE, "reason": REASON, "scope_key": EXTRAS["scope_key"]},
                    headers={CAPABILITY_HEADER: run.capability},
                ) as response,
            ):
                refused = (response.status, await response.json())
            after_refusal = await asyncio.to_thread(stored_rules)
            handler = propose_rule_handler(url, run.capability)
            first = await handler({"rule": RULE, "reason": REASON, **EXTRAS})
            second = await handler({"rule": RULE, "reason": REASON})
            return refused, after_refusal, first, second
        finally:
            await server.close()

    refused, after_refusal, first, second = asyncio.run(scenario())

    assert refused[0] == 422
    assert refused[1]["detail"]["code"] == "invalid_body"
    assert after_refusal == []

    assert first.get("is_error") is not True, first
    assert "recorded as a candidate" in _text_of(first)
    (stored,) = stored_rules()
    assert stored["state"] == "candidate"
    assert (stored["text"], stored["reason"]) == (RULE, REASON)
    assert stored["agent_id"] == run.agent_id
    assert stored["source_request_id"] == run.request_id
    assert stored["source_lineage_id"] == run.lineage_id
    assert stored["source_url"] == run.reply_target_url
    assert stored["scope_kind"] == "repository"
    assert stored["scope_key"] != EXTRAS["scope_key"]

    # The real 409 reaches the handler as a readable refusal without the rule.
    assert second["is_error"] is True
    assert "rule_already_proposed" in _text_of(second)
    assert RULE not in _text_of(second)
    assert len(stored_rules()) == 1


# 2. Completion contract to the runner's parser ---------------------------------


def test_the_completion_contract_offer_parses_as_the_runner_reads_it(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    run = eligible_revision(client, api)

    offered = contract(client, run.capability)
    assert offered.status_code == 200, offered.text
    assert parse_contract(offered.json()).rule_proposal is True

    # The counterexample: the switch off, the same request is not offered.
    switch(client, run.agent_id, False)
    withheld = contract(client, run.capability)
    assert withheld.status_code == 200, withheld.text
    assert parse_contract(withheld.json()).rule_proposal is False


def test_the_real_contract_answer_is_the_three_keys_the_runner_reads(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    run = eligible_revision(client, api)

    answer = contract(client, run.capability)
    assert answer.status_code == 200, answer.text
    body = answer.json()
    # The producer's answer is exactly what the runner's parser reads.
    assert set(body) == {"completion_contract", "execution", "rule_proposal"}
    parsed = parse_contract(body)
    assert parsed.kind == "implementation"
    assert parsed.execution is not None
    assert parsed.execution.execution_fence == body["execution"]["execution_fence"]
    assert parsed.rule_proposal is body["rule_proposal"] is True

    # The counterexample: the same answer with a fence the runner refuses
    # fails closed rather than parsing to an unbound implementation.
    broken = {**body, "execution": {**body["execution"], "execution_fence": 0}}
    with pytest.raises(ContractUnavailable) as caught:
        parse_contract(broken)
    assert caught.value.reason == "body_invalid"


# 3. Route's stored candidate to settlement --------------------------------------


def test_the_merge_settles_the_candidate_the_route_stored_for_its_lineage(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    merged = eligible_revision(client, api)
    _propose(client, merged, RULE_A)
    # The counterexample: a candidate of another pull request's lineage.
    other = eligible_revision(client, api)
    _propose(client, other, RULE_B)
    assert merged.lineage_id != other.lineage_id

    assert reconcile({merged.pr: PullRequestState.MERGED}).merged == 1

    rows = _by_text()
    assert (rows[RULE_A]["state"], rows[RULE_A]["number"]) == ("proposed", 1)
    assert rows[RULE_A]["source_lineage_id"] == merged.lineage_id
    assert (rows[RULE_B]["state"], rows[RULE_B]["number"]) == ("candidate", None)
    assert [event[0] for event in _events_of(rows[RULE_B]["id"])] == ["propose"]


# 4. Terminal writer to promotion ------------------------------------------------


def test_only_the_terminal_write_of_the_lineage_promotes(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    _agent, publication, revision = _revision_candidates(stack, auth_headers, (RULE_A,))

    # The counterexample: the revision pushes onto the open pull request, a
    # non-terminal write of the same lineage.
    _advance(stack, revision["id"], "open")
    (candidate,) = stored_rules()
    assert (candidate["state"], candidate["number"]) == ("candidate", None)
    assert len(stored_events()) == 1

    # The pull request is then found merged at that head.
    async def merged() -> None:
        async with stack.app.state.sessionmaker() as session:
            lineage = await session.get(
                ThreadPublicationLineage, uuid.UUID(publication["lineage_id"])
            )
            assert lineage is not None and lineage.head_sha == NEXT_HEAD
            recorded = await crud_lineages.mark_publication_lineage_terminal(
                session,
                lineage,
                expected_version=lineage.version,
                expected_head_sha=NEXT_HEAD,
                state="merged",
            )
            assert recorded.status == "merged"

    stack.portal.call(merged)

    rows = _by_text()
    assert (rows[RULE_A]["state"], rows[RULE_A]["number"]) == ("proposed", 1)
    assert [event[0] for event in _events_of(rows[RULE_A]["id"])] == ["propose", "promote"]
