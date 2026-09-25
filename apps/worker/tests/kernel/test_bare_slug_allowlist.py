"""A bare owner/repo guess outside the allowlist names no repository (#2947).

`profit/loss` matches the bare-slug pattern. The installation's allowlist is
the ground truth for a bare slug, so a 403 on one means the message named no
repository and the turn proceeds with no workspace. A github.com URL outside
the allowlist is still a terminal refusal.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

from curie_worker.workspace import WorkspaceRepositoryNotAllowed

from .test_approval_lifecycle import GrantBinding, _qevent

_ALLOWLIST_REFUSAL = (
    "That repository is not in api.githubRepoAllowlist for this installation; "
    "allow `owner/repo` or `owner/*` in the chart values."
)
_DEPLOYMENT_ID = uuid.UUID("33333333-3333-4333-8333-333333332947")


class _Binding(GrantBinding):
    async def resolve(self, kind: str, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="ledger-bot",
            deployment_id=_DEPLOYMENT_ID,
            workspace_enabled=True,
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )


class _AllowlistWorkspace:
    """Refuses any named repository with the API's 403, like an empty allowlist."""

    def __init__(self) -> None:
        self.substrate = None
        self.selections: list[object] = []

    def select_repository(self, **kwargs: object) -> str | None:
        repo = kwargs["repo_full_name"]
        self.selections.append(repo)
        if repo is not None:
            raise WorkspaceRepositoryNotAllowed(_ALLOWLIST_REFUSAL)
        return None

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        assert self.substrate is not None
        return SimpleNamespace(
            handle=self.substrate.claim(
                str(kwargs["thread_key"]),
                env=kwargs["env"],
                agent_name=kwargs["agent_name"],
                workspace_repo=kwargs["repo_full_name"],
            ),
            prepared=None,
        )

    def release(self, _thread_identity: str) -> None:
        return None

    def touch(self, _thread_identity: str, *, ttl_seconds: int) -> bool:
        return ttl_seconds > 0


def _run(make_harness, text: str, thread: str) -> tuple[_AllowlistWorkspace, object]:
    workspace = _AllowlistWorkspace()

    async def go() -> object:
        binding = _Binding(grant_event_id="unused", grant_tool="unused")
        async with make_harness(binding=binding) as h:
            workspace.substrate = h.substrate
            h.kernel._workspace = workspace  # type: ignore[assignment]
            await h.kernel.process_event(_qevent(text, thread=thread))
            return h

    return workspace, asyncio.run(go())


def test_bare_english_pair_outside_allowlist_proceeds_without_workspace(
    make_harness,
) -> None:
    workspace, h = _run(
        make_harness, "What was profit/loss for the quarter?", "tBarePair2947"
    )

    assert workspace.selections == ["profit/loss", None]
    assert h.sink.last_text is not None
    assert "githubRepoAllowlist" not in h.sink.last_text
    assert h.runner.opened, "the turn must reach the runner"


def test_url_outside_allowlist_is_still_refused(make_harness) -> None:
    workspace, h = _run(
        make_harness,
        "Fix the bug in https://github.com/other-org/other-repo please",
        "tUrlRepo2947",
    )

    assert workspace.selections == ["other-org/other-repo"]
    assert h.sink.last_text is not None
    assert "githubRepoAllowlist" in h.sink.last_text
    assert not h.runner.opened
