"""A rotated Jira token for another account is refused before it acts (#4630).

The Jira connection caches whose site and whose token it verified, keyed by a
fingerprint of the credential. When the operator replaces the token file with a
token that authenticates as a different account, the next tracker must read
``/myself`` again and refuse with `Unauthorized` before any poll search or
write reaches the site, rather than trust the identity it cached for the
previous token. Restoring the configured bot's token recovers.

Driven through production wiring: ``load_forges_json`` reads a ``CURIE_FORGES``
document naming a projected-volume key path, ``build_registry`` builds the
connection whose identity cache every tracker shares, and each operation is a
newly built tracker on the stateful Jira fake. What reached the site is read
from the fake's request log.

Placeholder hosts, accounts and tokens only.
"""

from __future__ import annotations

import json
import secrets
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.config import BindingConfig
from curie_api.forges.errors import Unauthorized
from curie_api.forges.loader import load_forges_json
from curie_api.forges.registry import ForgeRegistry, build_registry
from forge_fakes import jira as jira_fake
from forge_fakes.projected_volume import ProjectedVolume

OTHER_ACCOUNT = "712020:00000000-0000-4000-8000-0000000000bf"
HUMAN = "712020:00000000-0000-4000-8000-000000000101"
# Only the identity reads may reach the site while the token is refused.
IDENTITY_READS = {"tenant_info", "myself"}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _binding() -> BindingConfig:
    return BindingConfig.model_validate(
        {
            "name": jira_fake.BINDING_NAME,
            "tracker": jira_fake.TRACKER_NAME,
            "tracker_scope_id": jira_fake.CLOUD_ID,
            "repos": [
                {
                    "alias": "bot",
                    "code_host": jira_fake.CODE_HOST_NAME,
                    "project_id": jira_fake.GITHUB_REPO_ID,
                    "path": jira_fake.GITHUB_REPO,
                }
            ],
            "default_repo": "bot",
            "start_authority": {"type": "allowlist", "accounts": [HUMAN]},
        }
    )


def _registry(volume: ProjectedVolume) -> ForgeRegistry:
    entry = {
        "name": jira_fake.TRACKER_NAME,
        "kind": "jira_cloud",
        "host": jira_fake.SITE_HOST,
        "api_url": jira_fake.SITE,
        "credential": {
            "type": "static_token",
            "token_file": str(volume.path),
            "username": jira_fake.BOT_EMAIL,
        },
        "label": jira_fake.LABEL,
        "mention": jira_fake.BOT_ACCOUNT_ID,
    }
    return build_registry(load_forges_json(json.dumps({"code_hosts": [], "trackers": [entry]})))


def _tracker(registry: ForgeRegistry, client: httpx.AsyncClient) -> Any:
    return registry.binding_tracker(jira_fake.TRACKER_NAME).for_binding(client, _binding())


@pytest.mark.anyio
async def test_a_token_for_another_account_is_refused_after_a_warm_cache_and_recovers(
    tmp_path: Path,
) -> None:
    sentinel = secrets.token_hex(8)
    bot_token, other_token = f"bot-{sentinel}", f"other-{sentinel}"
    fake = jira_fake.JiraFake(token=bot_token)
    other = fake.add_user(OTHER_ACCOUNT, "Someone Else", account_type="app")
    fake.accept_credential(jira_fake.BOT_EMAIL, other_token, other)
    human = fake.add_user(HUMAN, "Human 1")
    issue = fake.seed_issue("Widget parser drops the last field")
    fake.add_label(issue.id, jira_fake.LABEL, human)
    ref = fake.ref(issue)
    volume = ProjectedVolume(tmp_path / "jira", f"{bot_token}\n")
    registry = _registry(volume)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=fake.app)) as client:
        # Warm the connection's identity cache with the configured bot's token.
        warm = await _tracker(registry, client).poll_marked(None, running=())
        assert len(warm.notices) == 1
        assert fake.requests_of("myself")

        # The operator rotates the file to a token for a different account.
        volume.publish(f"{other_token}\n")
        mark = len(fake.requests)
        writes = fake.writes

        with pytest.raises(Unauthorized) as polled:
            await _tracker(registry, client).poll_marked(None, running=())
        assert "bot account mismatch" in str(polled.value)
        with pytest.raises(Unauthorized):
            await _tracker(registry, client).set_state_label(
                ref, add="curie-factory:working", remove=()
            )

        refused = fake.requests[mark:]
        # The changed credential was verified again, not trusted from the warm
        # cache; the write then refuses on what that read recorded.
        myself = [r for r in refused if r.kind == "myself"]
        assert [r.authorization for r in myself] == [
            jira_fake.basic(jira_fake.BOT_EMAIL, other_token)
        ]
        assert {r.kind for r in refused} <= IDENTITY_READS
        assert [r for r in refused if r.method != "GET"] == []
        assert fake.writes == writes

        # Restoring the configured bot's token recovers both the poll and the write.
        volume.publish(f"{bot_token}\n")
        mark = len(fake.requests)
        page = await _tracker(registry, client).poll_marked(None, running=())
        assert len(page.notices) == 1
        await _tracker(registry, client).set_state_label(
            ref, add="curie-factory:working", remove=()
        )

        recovered = fake.requests[mark:]
        assert any(r.kind == "search" for r in recovered)
        assert any(r.method == "PUT" for r in recovered)
        assert {r.authorization for r in recovered if r.kind != "tenant_info"} == {
            jira_fake.basic(jira_fake.BOT_EMAIL, bot_token)
        }
