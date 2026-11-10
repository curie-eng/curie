"""The Bitbucket Cloud recordings carry nothing real (#4401, AC 5; decision 16.4).

`scan_recording` is the check the recorder runs before it writes, and the one
this module runs on every committed recording. Each planted case shows it
fails on one kind of leak, beside the clean recording it passes.

A realistic recording is made at runtime: real-shaped workspace, repository,
account and UUID values (assembled from fragments, so this file holds none)
are served by the fake through the recording transport, then scrubbed. So
the scrubber is proven on the payload shapes it meets, not on a hand-written
sample.
"""

from __future__ import annotations

import asyncio
import copy
import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.types import Actor
from forge_fakes.bitbucket_cloud import FACTORY_TOKEN, INLINE, REVIEWER_TOKEN, BitbucketCloudFake
from forge_fakes.bitbucket_cloud_record import (
    PLACEHOLDER_SLUG,
    PLACEHOLDER_WORKSPACE,
    scan_recording,
    scrub,
)
from forge_fakes.bitbucket_cloud_replay import (
    Cassette,
    RecordingTransport,
    committed_recordings,
)


def _real_like() -> dict[str, Any]:
    """Values shaped like a live workspace's, fresh each run."""

    return {
        "workspace": "north" + "wind-" + uuid.uuid4().hex[:6],
        "slug": "pay" + "ments-" + uuid.uuid4().hex[:6],
        "repo_uuid": "{" + str(uuid.uuid4()) + "}",
        "factory": Actor("557058:" + str(uuid.uuid4()), "jd" + "oe-bot-" + uuid.uuid4().hex[:4]),
        "reviewer": Actor("712020:" + str(uuid.uuid4()), "rev" + "iewer-" + uuid.uuid4().hex[:4]),
    }


async def _record(real: dict[str, Any]) -> dict[str, Any]:
    fake = BitbucketCloudFake(
        workspace=real["workspace"],
        slug=real["slug"],
        repo_uuid=real["repo_uuid"],
        factory=real["factory"],
    )
    fake.add_user(real["reviewer"], REVIEWER_TOKEN)
    recording = RecordingTransport(httpx.ASGITransport(app=fake.app))
    repo = f"/repositories/{real['workspace']}/{real['slug']}"
    headers = {"Authorization": f"Bearer {FACTORY_TOKEN}"}
    async with httpx.AsyncClient(
        transport=recording, base_url=fake.api_url, headers=headers
    ) as api:
        await api.get(repo)
        await api.get(
            "/repositories/%7B%7D/" + real["repo_uuid"].replace("{", "%7B").replace("}", "%7D")
        )
        fake.push("curie-record-branch", "Record")
        opened = (
            await api.post(
                f"{repo}/pullrequests",
                json={
                    "title": "Recording",
                    "source": {"branch": {"name": "curie-record-branch"}},
                    "destination": {"branch": {"name": "main"}},
                },
            )
        ).json()
        number = opened["id"]
        await api.post(
            f"{repo}/commit/{fake.branches['curie-record-branch']}/statuses/build",
            json={"key": "unit", "state": "FAILED", "name": "unit"},
        )
        fake.comment(number, real["reviewer"], "@curie rename it", inline=dict(INLINE))
        await api.post(f"{repo}/pullrequests/{number}/comments", json={"content": {"raw": "Done."}})
        await api.get(f"{repo}/pullrequests/{number}/comments", params={"pagelen": "1"})
        await api.get(f"{repo}/commit/{opened['source']['commit']['hash']}")
    meta = {
        "branch": "curie-record-branch",
        "repo_uuid": real["repo_uuid"],
        "reviewer_account": real["reviewer"].id,
    }
    return Cassette(meta, recording.exchanges).to_json()


def _real_values(real: dict[str, Any]) -> list[str]:
    return [
        real["workspace"],
        real["slug"],
        real["repo_uuid"].strip("{}"),
        real["factory"].id,
        real["factory"].login,
        real["reviewer"].id,
        real["reviewer"].login,
        FACTORY_TOKEN,
    ]


@pytest.fixture(scope="module")
def recorded() -> tuple[dict[str, Any], dict[str, Any]]:
    real = _real_like()
    raw = asyncio.run(_record(real))
    return real, raw


@pytest.fixture
def clean(recorded: tuple[dict[str, Any], dict[str, Any]]) -> dict[str, Any]:
    real, raw = recorded
    scrubbed, _originals = scrub(raw, workspace=real["workspace"], slug=real["slug"])
    return copy.deepcopy(scrubbed)


@pytest.mark.parametrize(
    "path",
    [pytest.param(p, id=p.name) for p in committed_recordings()],
)
def test_bitbucket_recordings_committed_files_scan_clean(path: Path) -> None:
    document = json.loads(path.read_text())
    Cassette.from_json(document)
    assert scan_recording(document) == []


def test_bitbucket_recordings_the_raw_recording_is_not_clean(
    recorded: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    real, raw = recorded
    findings = scan_recording(raw, _real_values(real))
    assert any("account_id" in f for f in findings)
    assert any("uuid" in f for f in findings)
    assert any("repository segment" in f for f in findings)
    assert any("real value" in f for f in findings)


def test_bitbucket_recordings_scrub_leaves_nothing_real(
    recorded: tuple[dict[str, Any], dict[str, Any]], clean: dict[str, Any]
) -> None:
    real, _raw = recorded
    assert scan_recording(clean, _real_values(real)) == []
    text = json.dumps(clean)
    assert f"{PLACEHOLDER_WORKSPACE}/{PLACEHOLDER_SLUG}" in text
    assert f"bitbucket.org/workspaces/{PLACEHOLDER_WORKSPACE}/avatar/" in text
    assert f"bitbucket.org/account/{PLACEHOLDER_WORKSPACE}/avatar/" in text
    assert f'"username": "{PLACEHOLDER_WORKSPACE}"' in text
    assert f"bitbucket.org/{PLACEHOLDER_WORKSPACE}/workspace/projects/" in text
    assert "placeholder-account-1" in text
    assert clean["meta"]["repo_uuid"].startswith("{00000000-0000-4000-8000-")
    # Still a recording the replay reads.
    assert len(Cassette.from_json(clean).exchanges) > 0


def _first_user(document: Any) -> dict[str, Any]:
    if isinstance(document, dict):
        if "account_id" in document:
            return document
        values: list[Any] = list(document.values())
    elif isinstance(document, list):
        values = document
    else:
        raise LookupError
    for value in values:
        try:
            return _first_user(value)
        except LookupError:
            continue
    raise LookupError


def test_bitbucket_recordings_planted_authorization_header_fails(clean: dict[str, Any]) -> None:
    clean["exchanges"][0]["response"]["headers"]["authorization"] = "Bearer " + "x" * 8
    findings = scan_recording(clean)
    assert findings and all("credential header" in f for f in findings)


def test_bitbucket_recordings_planted_uuid_fails(clean: dict[str, Any]) -> None:
    clean["meta"]["repo_uuid"] = "{" + str(uuid.uuid4()) + "}"
    findings = scan_recording(clean)
    assert len(findings) == 1 and "uuid" in findings[0] and "$.meta.repo_uuid" in findings[0]


def test_bitbucket_recordings_planted_account_id_fails(clean: dict[str, Any]) -> None:
    _first_user(clean["exchanges"])["account_id"] = "5570" + "58:" + "ab12cd34ef56ab78cd90ef12"
    findings = scan_recording(clean)
    assert len(findings) == 1 and "account_id is not a placeholder" in findings[0]


def test_bitbucket_recordings_planted_repository_segment_fails(clean: dict[str, Any]) -> None:
    href = "https://" + "bitbucket.org/" + "real" + "-ws/real-repo"
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = href
    findings = scan_recording(clean)
    assert len(findings) == 1 and "repository segment" in findings[0]
    # The same link on the placeholder names passes.
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = (
        "https://" + "bitbucket.org/" + f"{PLACEHOLDER_WORKSPACE}/{PLACEHOLDER_SLUG}"
    )
    assert scan_recording(clean) == []


def test_bitbucket_recordings_planted_workspace_avatar_segment_fails(
    clean: dict[str, Any],
) -> None:
    href = "https://" + "bitbucket.org/" + "workspaces/" + "real" + "-ws/avatar/"
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = href
    findings = scan_recording(clean)
    assert len(findings) == 1 and "repository segment" in findings[0]
    # The same link on the placeholder workspace passes.
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = (
        "https://" + "bitbucket.org/" + f"workspaces/{PLACEHOLDER_WORKSPACE}/avatar/"
    )
    assert scan_recording(clean) == []


def test_bitbucket_recordings_planted_project_segment_fails(clean: dict[str, Any]) -> None:
    href = "https://" + "bitbucket.org/" + "real" + "-ws/workspace/projects/PROJ"
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = href
    findings = scan_recording(clean)
    assert len(findings) == 1 and "repository segment" in findings[0]
    # The same link on the placeholder workspace passes.
    clean["exchanges"][0]["response"]["body"]["links"]["html"]["href"] = (
        "https://" + "bitbucket.org/" + f"{PLACEHOLDER_WORKSPACE}/workspace/projects/PROJ"
    )
    assert scan_recording(clean) == []


def test_bitbucket_recordings_planted_real_value_fails(
    recorded: tuple[dict[str, Any], dict[str, Any]], clean: dict[str, Any]
) -> None:
    real, _raw = recorded
    clean["exchanges"][0]["response"]["body"]["description"] = f"owned by {real['reviewer'].login}"
    findings = scan_recording(clean, _real_values(real))
    assert findings == ["real value #6 is still present"]
