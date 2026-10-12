"""Review feedback authority on Bitbucket Data Center follows the allowlist (#4401 AC 9).

The adapter declares ``user_can_write`` unsupported (decision 11), so
`binding_may_act_on_feedback` acts only on DC user ids the binding lists under
the code host's name. The outsider holds ``REPO_WRITE`` on the fake, so a pass
here cannot come from a permission read. The registry, binding and adapter are
built through the production config and registry.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from curie_api.forges import registry
from curie_api.forges.authority import (
    binding_may_act_on_feedback,
    feedback_actionable,
    feedback_allowlist,
)
from curie_api.forges.capabilities import Operation, Support
from curie_api.forges.config import ForgesConfig, TrackerConfig
from curie_api.forges.errors import Unsupported
from curie_api.forges.types import Actor
from forge_fakes.bitbucket_dc import (
    BINDING_NAME,
    CODE_HOST_NAME,
    OUTSIDER,
    REVIEWER,
    BitbucketDcFake,
    forges_document,
)


class _UnusedJira:
    """Stands in for the ``jira_cloud`` kind (decision 15.2); never asked here."""

    def __init__(self, config: TrackerConfig, *, ca_bundle: str = "") -> None:
        self.config = config

    def for_repository(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the tracker is not part of feedback authority")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def jira_installed(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setitem(
        registry._KINDS,
        "jira_cloud",
        registry._Kind(tracker=_UnusedJira, code_host=None, installed=True),
    )
    yield


def _built(fake: BitbucketDcFake, reviewers: tuple[str, ...]) -> tuple[Any, Any, Any]:
    config = ForgesConfig.model_validate(forges_document(reviewers=reviewers))
    built = registry.build_registry(config)
    binding = built.binding(BINDING_NAME)
    adapter = built.code_host(CODE_HOST_NAME).adapter(fake.client())
    return binding, binding.repos[0], adapter


@pytest.mark.anyio
@pytest.mark.usefixtures("jira_installed")
async def test_bitbucket_feedback_authority_dc_allowlist_decides_not_write_access() -> None:
    fake = BitbucketDcFake()
    assert fake.permissions[int(OUTSIDER.id)] == "REPO_WRITE"
    binding, repo, adapter = _built(fake, (REVIEWER.id,))
    repository = fake.repository()

    assert adapter.capabilities[Operation.USER_CAN_WRITE] is Support.UNSUPPORTED
    assert feedback_allowlist(binding, repo) == frozenset({REVIEWER.id})
    assert await binding_may_act_on_feedback(binding, repo, adapter, repository, REVIEWER) is True
    assert await binding_may_act_on_feedback(binding, repo, adapter, repository, OUTSIDER) is False
    # A login that matches is not an id that matches.
    impostor = Actor(id="999", login=REVIEWER.login)
    assert await binding_may_act_on_feedback(binding, repo, adapter, repository, impostor) is False
    with pytest.raises(Unsupported):
        await adapter.user_can_write(repository, OUTSIDER)
    assert not [r for r in fake.requests if "/permissions" in r.path]


@pytest.mark.anyio
@pytest.mark.usefixtures("jira_installed")
async def test_bitbucket_feedback_authority_dc_listing_the_outsider_makes_it_actionable() -> None:
    fake = BitbucketDcFake()
    binding, repo, adapter = _built(fake, (REVIEWER.id, OUTSIDER.id))

    assert (
        await binding_may_act_on_feedback(binding, repo, adapter, fake.repository(), OUTSIDER)
        is True
    )


@pytest.mark.anyio
@pytest.mark.usefixtures("jira_installed")
async def test_bitbucket_feedback_authority_dc_listed_feedback_is_actionable_per_author() -> None:
    fake = BitbucketDcFake()
    binding, repo, adapter = _built(fake, (REVIEWER.id,))
    fake.push("factory/authority")
    pull = await adapter.open_pull_request(
        fake.repository(), head_ref="factory/authority", base_ref="main", title="t", body="b"
    )
    number = int(pull.ref.number)
    fake.comment(number, REVIEWER, "Please rename it.", anchor=fake.inline_anchor(number))
    fake.comment(number, OUTSIDER, "Delete it all.")

    items = {i.author: i for i in (await adapter.list_review_feedback(pull.ref, None)).items}
    allowlist = feedback_allowlist(binding, repo)
    assert await feedback_actionable(adapter, items[REVIEWER], allowlist) is True
    assert await feedback_actionable(adapter, items[OUTSIDER], allowlist) is False
