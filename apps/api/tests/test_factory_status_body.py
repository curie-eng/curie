"""status_body renders the card or the checklist, never both (#3125)."""

from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
from curie_api.factory_notices import (
    FINAL_MARKER,
    _deliver,
    _GitHub,
    _patch,
    marker_for,
    result_section,
    status_body,
    upsert_issue_notice,
)
from curie_api.factory_progress import PhaseSlot, PhaseView, StageSlot
from curie_api.factory_reply_target import ReplyTarget
from curie_api.models import FactoryStatusComment

REQUEST = uuid.UUID("00000000-0000-0000-0000-000000003125")
CARD = "https://curie.example.com/v1/factory/cards/abc.svg"
WAITING = "_Waiting for the agent to report progress._"


def _view() -> PhaseView:
    return PhaseView(
        phases=(
            PhaseSlot(id="read_issue", label="Read issue", state="done", round_label=None),
            PhaseSlot(id="plan", label="Plan", state="current", round_label="round 1 of 3"),
            PhaseSlot(id="ci", label="Wait for CI", state="pending", round_label=None),
        ),
        loops=(),
        current="plan",
        stages=(
            StageSlot(
                id="read_issue",
                label="Read issue",
                phase_ids=("read_issue",),
                state="done",
                round_label=None,
            ),
            StageSlot(
                id="plan",
                label="Plan",
                phase_ids=("plan",),
                state="current",
                round_label="round 1 of 3",
            ),
            StageSlot(
                id="ci",
                label="Wait for CI",
                phase_ids=("ci",),
                state="pending",
                round_label=None,
            ),
        ),
        reviewer_model=None,
        staged=False,
    )


def test_a_card_url_emits_no_checklist_while_running() -> None:
    body = status_body(
        request_id=REQUEST, card_url=CARD, pill_label="RUNNING", phase_view=_view(), result=None
    )
    assert body == f"![Curie status]({CARD})\n\nStatus: RUNNING\n\n{marker_for(REQUEST)}\n"


def test_a_card_url_emits_no_waiting_placeholder() -> None:
    body = status_body(
        request_id=REQUEST, card_url=CARD, pill_label="QUEUED", phase_view=None, result=None
    )
    assert WAITING not in body
    assert body == f"![Curie status]({CARD})\n\nStatus: QUEUED\n\n{marker_for(REQUEST)}\n"


def test_a_card_url_final_body_keeps_the_result_and_card_only() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="PR OPEN",
        phase_view=_view(),
        result="Opened https://github.com/acme/fixture/pull/7\n",
    )
    assert body == (
        "Opened https://github.com/acme/fixture/pull/7\n\n"
        f"![Curie status]({CARD})\n\nStatus: PR OPEN\n\n{FINAL_MARKER}\n\n{marker_for(REQUEST)}\n"
    )


def test_without_a_card_url_the_checklist_is_the_fallback() -> None:
    body = status_body(
        request_id=REQUEST, card_url=None, pill_label="RUNNING", phase_view=_view(), result=None
    )
    assert body == (
        "- [x] Read issue\n- [ ] **Plan** (in progress, round 1 of 3)\n- [ ] Wait for CI\n\n"
        f"Status: RUNNING\n\n{marker_for(REQUEST)}\n"
    )


def test_without_a_card_url_the_waiting_placeholder_stays() -> None:
    body = status_body(
        request_id=REQUEST, card_url=None, pill_label="QUEUED", phase_view=None, result=None
    )
    assert body == f"{WAITING}\n\nStatus: QUEUED\n\n{marker_for(REQUEST)}\n"


_WORKSPACE = "acme-provider-workspace"
_KEY_ID = "acme_fake_provider_key_id_3936"
_KEY = "sk-or-v1-" + "0123456789abcdef" * 4
_KEY_URL = f"https://openrouter.ai/settings/keys/{_KEY_ID}?workspace={_WORKSPACE}"
_PR_URL = "https://github.com/acme-corp/acme-bot/pull/7"
# The 402 sentence is grounded in the SDK observation recorded in
# runner/tests/test_translate.py. Workspace and key metadata are synthetic
# additions, not claims about OpenRouter's response fields.
_PROVIDER_DETAIL = (
    "API Error: 402 This request requires more credits, or fewer max_tokens. "
    "You requested up to 32000 tokens, but can only afford 1200. "
    f'Manage your key at {_KEY_URL}. Key ID: {_KEY_ID}; workspace "{_WORKSPACE}". '
    f"key={_KEY}"
)


def _assert_redacted(body: str) -> None:
    assert "API Error: 402 This request requires more credits" in body
    assert _WORKSPACE not in body
    assert _KEY_ID not in body
    assert _KEY not in body
    assert "https://openrouter.ai/settings/keys" not in body
    assert "[REDACTED" in body
    assert marker_for(REQUEST) in body
    assert _PR_URL in body


@pytest.mark.parametrize("cause", ["early_stop", "no_pull_request", "model_credit_exhausted"])
def test_final_status_redacts_provider_metadata_in_quotes_and_cause_lines(cause: str) -> None:
    result = result_section(cause, pr_url=None, detail=_PROVIDER_DETAIL)
    result += f"Cause: key_id={_KEY_ID}; workspace: {_WORKSPACE}\n{_PR_URL}\n"
    body = status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    _assert_redacted(body)
    assert f"Cause: {cause}" in body
    assert body.count(FINAL_MARKER) == 1
    if cause in {"early_stop", "no_pull_request"}:
        assert "Agent's last message:\n```text\n" in body


def test_running_status_redacts_metadata_outside_the_terminal_result() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="RUNNING",
        phase_view=None,
        result=None,
        waiting_line=_PROVIDER_DETAIL,
        base_line=f"{_PR_URL}\nworkspace_name={_WORKSPACE}",
    )
    _assert_redacted(body)
    assert FINAL_MARKER not in body


@pytest.mark.parametrize(
    "metadata",
    [
        f"workspace '{_WORKSPACE}'; key identifier '{_KEY_ID}'",
        f'{{"workspace": "{_WORKSPACE}", "key_id": "{_KEY_ID}"}}',
        f"workspace: {_WORKSPACE}; key hash={_KEY_ID}",
        f"workspace '[REDACTED:foo]{_WORKSPACE}'; key id '[REDACTED:foo]{_KEY_ID}'",
        f"workspace: [REDACTED:provider_workspace]{_WORKSPACE}; "
        f"key id=[REDACTED:provider_key_id]{_KEY_ID}",
    ],
)
def test_status_redacts_contextual_names_without_removing_workspace_failure_text(
    metadata: str,
) -> None:
    result = result_section(
        "workspace_error",
        pr_url=None,
        detail=f"The repository workspace could not be prepared. {metadata}",
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert _WORKSPACE not in body
    assert _KEY_ID not in body
    assert "The repository workspace could not be prepared." in body
    assert "Cause: workspace_error" in body
    assert "Failure class: workspace-error" in body


def test_backtick_labels_and_escaped_json_values_are_redacted() -> None:
    escaped = 'acme \\"Research\\" Team'
    result = result_section(
        "model_error",
        pr_url=None,
        detail=(
            f'`key_id`: "{_KEY_ID}"; `workspace`: "{_WORKSPACE}"\n'
            f'{{"workspace": "{escaped}", "key_id": "{_KEY_ID}"}}'
        ),
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert _KEY_ID not in body
    assert _WORKSPACE not in body
    assert "Research" not in body
    assert "Cause: model_error" in body


def test_an_empty_key_label_does_not_consume_the_cause_line() -> None:
    result = result_section("model_error", pr_url=None, detail="Key ID:")
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert "Cause: model_error" in body
    assert "Failure class: server-error" in body


def test_bold_labels_and_capitalized_workspace_names_are_redacted() -> None:
    team = "Acme Research Team"
    escaped_key = 'acme \\"Research\\" key'
    result = result_section(
        "model_error",
        pr_url=None,
        detail=(
            f'**key_id**: "{_KEY_ID}"; **workspace name**: {team}\n{{"key_id": "{escaped_key}"}}'
        ),
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert _KEY_ID not in body
    assert "Acme" not in body
    assert "Research" not in body
    assert "Cause: model_error" in body


def test_underscore_emphasis_labels_are_redacted() -> None:
    team = "Acme Research Team"
    result = result_section(
        "model_error",
        pr_url=None,
        detail=(f'_key_id_: "{_KEY_ID}"; _workspace_: "{team}"\n__key_id__: "{_KEY_ID}"'),
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert _KEY_ID not in body
    assert "Acme" not in body
    assert "Research" not in body
    assert "Cause: model_error" in body


def test_equals_assignment_redacts_a_lowercase_workspace_name() -> None:
    name = "zephyrworkspace"
    result = result_section(
        "model_error",
        pr_url=None,
        detail=f"workspace={name}; workspace_name={name}",
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert name not in body
    assert "Cause: model_error" in body


def test_workspace_diagnostic_prose_stays_while_a_slug_is_redacted() -> None:
    prose = "workspace: failed to prepare the repository"
    result = result_section(
        "workspace_error",
        pr_url=None,
        detail=f"{prose}. workspace: {_WORKSPACE}",
    )
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result,
    )
    assert prose in body
    assert _WORKSPACE not in body
    assert "Cause: workspace_error" in body


def test_provider_key_url_with_pull_request_path_is_still_redacted() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="RUNNING",
        phase_view=None,
        result=None,
        waiting_line=f"{_PROVIDER_DETAIL}\n{_KEY_URL.split('?')[0]}/pull/7\n{_PR_URL}",
    )
    _assert_redacted(body)
    assert "openrouter.ai" not in body


def test_github_pull_request_in_a_keys_repository_stays_usable() -> None:
    pr_url = "https://github.com/acme-corp/keys/pull/7#issuecomment-3936"
    result = result_section("completed", pr_url=pr_url)
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="PR OPEN",
        phase_view=None,
        result=result,
    )
    assert f"Completed: {pr_url}" in body
    assert marker_for(REQUEST) in body
    assert FINAL_MARKER in body


@pytest.mark.parametrize("target_kind", ["issue", "pr", "thread", "thread_fallback"])
def test_every_factory_comment_creation_redacts_at_the_http_boundary(target_kind: str) -> None:
    async def exercise() -> None:
        posted: list[tuple[str, str]] = []

        def github(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json=[])
            assert request.method == "POST"
            posted.append((request.url.path, json.loads(request.content)["body"]))
            if target_kind == "thread_fallback" and request.url.path.endswith("/replies"):
                return httpx.Response(422, json={"message": "Validation Failed"})
            return httpx.Response(201, json={"id": 3936})

        if target_kind in {"thread", "thread_fallback"}:
            target = ReplyTarget("thread", pr_number=7, comment_id=33, url=_PR_URL)
            expected = ["/repos/acme-corp/acme-bot/pulls/7/comments/33/replies"]
            if target_kind == "thread_fallback":
                expected.append("/repos/acme-corp/acme-bot/issues/7/comments")
        elif target_kind == "pr":
            target = ReplyTarget("pr", pr_number=7, url=_PR_URL)
            expected = ["/repos/acme-corp/acme-bot/issues/7/comments"]
        else:
            target = ReplyTarget("issue")
            expected = ["/repos/acme-corp/acme-bot/issues/3936/comments"]

        async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as client:
            outcome = await _deliver(
                _GitHub(client, "https://api.github.com", "/repos/acme-corp/acme-bot", {}),
                3936,
                FactoryStatusComment(execution_request_id=REQUEST, scan_page=1),
                target,
                f"{_PROVIDER_DETAIL}\n{_PR_URL}\n{marker_for(REQUEST)}\n",
            )
        assert outcome is not None and outcome[0] == "posted"
        assert [path for path, _ in posted] == expected
        for _, body in posted:
            _assert_redacted(body)
            assert FINAL_MARKER not in body

    asyncio.run(exercise())


@pytest.mark.parametrize("comment_list", ["issue", "review"])
def test_every_factory_comment_update_redacts_at_the_http_boundary(comment_list: str) -> None:
    async def exercise() -> None:
        patched: list[tuple[str, str]] = []

        def github(request: httpx.Request) -> httpx.Response:
            assert request.method == "PATCH"
            patched.append((request.url.path, json.loads(request.content)["body"]))
            return httpx.Response(200, json={"id": 3936})

        async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as client:
            outcome = await _patch(
                _GitHub(client, "https://api.github.com", "/repos/acme-corp/acme-bot", {}),
                FactoryStatusComment(comment_id=3936, comment_list=comment_list),
                f"{_PROVIDER_DETAIL}\n{_PR_URL}\n{marker_for(REQUEST)}\n{FINAL_MARKER}\n",
            )
        assert outcome == "edited"
        kind = "pulls" if comment_list == "review" else "issues"
        assert len(patched) == 1
        assert patched[0][0] == f"/repos/acme-corp/acme-bot/{kind}/comments/3936"
        _assert_redacted(patched[0][1])
        assert patched[0][1].count(FINAL_MARKER) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("existing", [False, True])
def test_marked_notice_create_update_and_unchanged_use_the_redacted_body(existing: bool) -> None:
    async def exercise() -> None:
        stored = f"Old notice\n{marker_for(REQUEST)}\n" if existing else None
        writes: list[tuple[str, str]] = []

        def github(request: httpx.Request) -> httpx.Response:
            nonlocal stored
            if request.method == "GET":
                comments = (
                    [{"id": 3936, "body": stored, "performed_via_github_app": {"id": 42}}]
                    if stored is not None
                    else []
                )
                return httpx.Response(200, json=comments)
            assert request.method in {"POST", "PATCH"}
            stored = json.loads(request.content)["body"]
            assert isinstance(stored, str)
            writes.append((request.method, stored))
            return httpx.Response(200 if request.method == "PATCH" else 201, json={"id": 3936})

        async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as client:
            for expected_outcome in ["written", "unchanged"]:
                outcome = await upsert_issue_notice(
                    client,
                    api="https://api.github.com",
                    repo_path="/repos/acme-corp/acme-bot",
                    headers={},
                    issue_number=3936,
                    marker=marker_for(REQUEST),
                    body=f"{_PROVIDER_DETAIL}\n{_PR_URL}\n{marker_for(REQUEST)}\n",
                    app_id="42",
                )
                assert outcome == expected_outcome
        assert len(writes) == 1
        assert writes[0][0] == ("PATCH" if existing else "POST")
        _assert_redacted(writes[0][1])

    asyncio.run(exercise())
