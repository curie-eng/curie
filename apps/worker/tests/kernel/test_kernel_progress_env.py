"""A factory execution's sandbox can report progress for its request (#3077).

The worker mints a request-bound ``work_item.progress`` token and the progress
URL into the claim env of a work-item execution, and into no other turn. The
API verifies the token with the same shared ``sandbox_token`` module.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_internal import sandbox_token  # noqa: E402
from test_work_item_workspace import (  # noqa: E402
    ISSUE_URL,
    _Binding,
    _turn,
    _WorkItems,
    _Workspace,
)

PROGRESS_URL_ENV = "CURIE_PROGRESS_URL"
PROGRESS_TOKEN_ENV = "CURIE_PROGRESS_TOKEN"


def _claim_envs(h: object) -> list[dict[str, str]]:
    return [env or {} for env in h.fake_k8s.claim_envs]  # type: ignore[attr-defined]


def test_a_work_item_execution_claim_carries_a_request_bound_progress_token(
    make_harness,
) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            h.kernel._work_items = _WorkItems()
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = [env for env in _claim_envs(h) if PROGRESS_URL_ENV in env]
            assert len(envs) == 1, _claim_envs(h)
            env = envs[0]
            base = h.config.runner_facing_api_base_url.rstrip("/")
            assert env[PROGRESS_URL_ENV] == f"{base}/v1/work-item-progress/{request_id}"
            token = env[PROGRESS_TOKEN_ENV]
            assert sandbox_token.verify(
                token, h.config.api_key, agent=str(request_id), scope="work_item.progress"
            )
            # Bound to this request and this scope only.
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=str(uuid.uuid4()), scope="work_item.progress"
            )
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=str(request_id), scope="state"
            )
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=str(request_id), scope="state.app"
            )

    asyncio.run(exercise())


def test_an_ordinary_turn_carries_no_progress_env(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            h.kernel._work_items = _WorkItems()

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4().hex}", f"What is {ISSUE_URL} about?")
            )

            assert _claim_envs(h), "the turn must have claimed a sandbox"
            for env in _claim_envs(h):
                assert PROGRESS_URL_ENV not in env
                assert PROGRESS_TOKEN_ENV not in env

    asyncio.run(exercise())


ISSUE_READ_URL_ENV = "CURIE_ISSUE_READ_URL"
ISSUE_READ_TOKEN_ENV = "CURIE_ISSUE_READ_TOKEN"


def test_a_work_item_execution_claim_carries_its_issue_read_capability(make_harness) -> None:
    """ADR 0187: the API minted capability rides the claim env of this execution."""

    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = [env for env in _claim_envs(h) if ISSUE_READ_URL_ENV in env]
            assert len(envs) == 1, _claim_envs(h)
            base = h.config.runner_facing_api_base_url.rstrip("/")
            assert envs[0][ISSUE_READ_URL_ENV] == f"{base}/work-items/issue-read"
            assert envs[0][ISSUE_READ_TOKEN_ENV] == f"wir.capability-for-{request_id}"
            assert "issue_read_context" in work_items.calls

    asyncio.run(exercise())


def test_a_refused_issue_read_mint_leaves_the_tool_off_and_the_turn_running(
    make_harness,
) -> None:
    from curie_worker.workitem_dispatch import WorkItemConflict

    class _Refusing(_WorkItems):
        async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
            self.calls.append("issue_read_context")
            raise WorkItemConflict("invalid_context")

    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            h.kernel._work_items = _Refusing()
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = _claim_envs(h)
            assert any(PROGRESS_URL_ENV in env for env in envs), envs
            for env in envs:
                assert ISSUE_READ_URL_ENV not in env
                assert ISSUE_READ_TOKEN_ENV not in env

    asyncio.run(exercise())


def test_an_ordinary_turn_carries_no_issue_read_env(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4().hex}", f"What is {ISSUE_URL} about?")
            )

            assert _claim_envs(h), "the turn must have claimed a sandbox"
            for env in _claim_envs(h):
                assert ISSUE_READ_URL_ENV not in env
            assert "issue_read_context" not in work_items.calls

    asyncio.run(exercise())
