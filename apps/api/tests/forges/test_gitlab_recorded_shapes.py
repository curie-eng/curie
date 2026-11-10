"""The GitLab fake answers with every key real GitLab CE 19.4.1 sends (#4399 decision 25).

Each file under ``forge_fakes/gitlab_payloads/`` is one response recorded from
the live fixture (hosts replaced by ``gitlab.example``, no token values). This
test asks the fake (`forge_fakes.gitlab.GitLabAPI`) for the same endpoint over
the same kind of state and requires its answer to carry every key the
recording carries, nested objects and the first item of each list included.
Extra keys in the fake are fine; a missing one means an adapter could read a
field GitLab sends that the fake never exercises.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from curie_api.forges.types import Actor
from forge_fakes.gitlab import API, TOKEN, GitLabAPI

PAYLOADS = Path(__file__).resolve().parents[1] / "forge_fakes" / "gitlab_payloads"
DEVELOPER = Actor("2", "developer")
RECORDED = (
    "version",
    "personal_access_tokens_self",
    "issue",
    "resource_label_events",
    "notes",
    "merge_request",
    "discussions",
    "pipelines",
    "jobs",
    "statuses",
)


def _missing(recorded: Any, faked: Any, where: str) -> list[str]:
    """The key paths ``recorded`` carries that ``faked`` lacks."""

    if isinstance(recorded, list):
        if not recorded or not isinstance(recorded[0], dict | list):
            return []
        if not isinstance(faked, list) or not faked:
            return [f"{where}[] (the fake listed nothing)"]
        return _missing(recorded[0], faked[0], f"{where}[]")
    if not isinstance(recorded, dict):
        return []
    if not isinstance(faked, dict):
        return [f"{where} (the fake sent {type(faked).__name__})"]
    gaps = []
    for key, value in recorded.items():
        path = f"{where}.{key}" if where else key
        if key not in faked:
            gaps.append(path)
        elif value is not None and faked[key] is not None:
            gaps.extend(_missing(value, faked[key], path))
    return gaps


def _fake_answers() -> dict[str, Any]:
    fake = GitLabAPI()
    fake.add_user(DEVELOPER)
    fake.set_member(DEVELOPER, 30)
    iid = fake.add_issue("Recorded issue", "Recorded for shapes.", author=DEVELOPER)
    fake.label(iid, "factory", DEVELOPER)
    fake.issue_note(iid, DEVELOPER, "@curie-bot please look")
    branch = "curie-test/thread"
    head = fake.push(branch, "Work on thread")
    mr = fake.add_merge_request(branch, title="Fixture thread", description="Fixture change.")
    fake.mr_discussion(mr, DEVELOPER, "Why this way?", diff=False)
    fake.report_job(head, "test", "failed")
    fake.add_status(head, "external-ci", "failed", author=DEVELOPER)
    pipeline = next(p.id for p in fake.pipelines.values() if p.sha == head)
    project = f"/projects/{fake.project_id}"
    paths = {
        "version": "/version",
        "personal_access_tokens_self": "/personal_access_tokens/self",
        "issue": f"{project}/issues/{iid}",
        "resource_label_events": f"{project}/issues/{iid}/resource_label_events",
        "notes": f"{project}/issues/{iid}/notes?sort=asc&order_by=created_at",
        "merge_request": f"{project}/merge_requests/{mr}",
        "discussions": f"{project}/merge_requests/{mr}/discussions",
        "pipelines": f"{project}/pipelines?sha={head}",
        "jobs": f"{project}/pipelines/{pipeline}/jobs?include_retried=false",
        "statuses": f"{project}/repository/commits/{head}/statuses?all=true",
    }
    answers = {}
    for name, path in paths.items():
        response = fake.handle(
            httpx.Request("GET", f"{API}{path}", headers={"PRIVATE-TOKEN": TOKEN})
        )
        assert response.status_code == 200, (name, response.status_code)
        answers[name] = response.json()
    return answers


def test_gitlab_fake_carries_every_recorded_key() -> None:
    answers = _fake_answers()
    gaps = {}
    for name in RECORDED:
        recorded = json.loads((PAYLOADS / f"{name}.json").read_text())
        missing = _missing(recorded, answers[name], "")
        if missing:
            gaps[name] = missing
    assert gaps == {}
