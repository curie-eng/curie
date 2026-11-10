"""An authentication deferral of tracker intake is a WARNING with a fixed classification (#4630).

When a tracker refuses the credential (`Unauthorized`, or a subclass such as
Jira's `Forbidden`) or the credential file cannot be read
(`CredentialUnreadable`), the intake pass defers and logs at WARNING the
tracker, binding and label with a fixed classification, ``authentication
rejected`` or ``credential unreadable``, never the exception text. Any other
deferral stays at INFO.

The end-to-end cases drive the real port-driven intake pass
(`factory_tracker_intake.poll_trackers`) the way ``test_factory_tracker_intake.py``
does: the real Jira Cloud adapter, built by the production registry from the
install's ``CURIE_FORGES``, over the Jira fake through ASGI, the real GitHub
adapter over `GitHubRepositoryFake`, and real Postgres. The Jira token is a
Kubernetes-style projected volume (`forge_fakes.projected_volume`), so a
replaced file is read by the next pass with no restart. Placeholder
identifiers only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from curie_api import factory_tracker_intake
from curie_api.config import get_settings
from curie_api.forges.errors import CredentialUnreadable, Unauthorized, Unavailable
from curie_api.forges.jira_cloud.transport import Forbidden
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes import jira as jira_fake
from forge_fakes.github import (
    REPO,
    REPO_ID,
    GitHubRepositoryFake,
    fake_installation_tokens,
    forges_document,
    forges_env_json,
)
from forge_fakes.github_comments import _rows
from forge_fakes.projected_volume import ProjectedVolume
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

LOGGER = factory_tracker_intake.__name__
LABEL = jira_fake.LABEL
BINDING = jira_fake.BINDING_NAME
TRACKER = jira_fake.TRACKER_NAME
STARTER_ID = "5b10a0000000000000000a11"
REJECTED = "authentication rejected"
UNREADABLE = "credential unreadable"


def _intake_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == LOGGER]


# _deferred on its own ------------------------------------------------------------------


def _scope() -> Any:
    return factory_tracker_intake._Scope(
        registry=SimpleNamespace(),
        settings=SimpleNamespace(),
        client=SimpleNamespace(),
        tracker=SimpleNamespace(name=TRACKER, kind="jira_cloud"),
        binding=SimpleNamespace(name=BINDING, tracker_scope_id=jira_fake.CLOUD_ID),
        surface_kind="jira",
        location=BINDING,
    )


@pytest.mark.parametrize(
    ("build", "classification"),
    [
        (lambda text: Unauthorized(text), REJECTED),
        (lambda text: Forbidden(text), REJECTED),
        (lambda text: CredentialUnreadable(text), UNREADABLE),
    ],
    ids=["unauthorized", "forbidden-subclass", "credential-unreadable"],
)
@pytest.mark.parametrize("what", ["poll", "sweep"])
def test_an_authentication_deferral_warns_with_a_fixed_classification(
    caplog: pytest.LogCaptureFixture, build: Any, classification: str, what: str
) -> None:
    sentinel = f"exception-text-{secrets.token_hex(6)}"
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    factory_tracker_intake._deferred(_scope(), LABEL, what, build(sentinel))

    [record] = _intake_records(caplog)
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    for expected in (TRACKER, BINDING, LABEL, classification):
        assert expected in message, message
    assert record.exc_info is None
    assert sentinel not in caplog.text


@pytest.mark.parametrize("what", ["poll", "sweep"])
def test_a_plain_unavailable_deferral_stays_at_info(
    caplog: pytest.LogCaptureFixture, what: str
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    factory_tracker_intake._deferred(_scope(), LABEL, what, Unavailable("rate_limited"))

    [record] = _intake_records(caplog)
    assert record.levelno == logging.INFO
    assert REJECTED not in record.getMessage()
    assert UNREADABLE not in record.getMessage()


# The intake pass end to end -------------------------------------------------------------


class _GitHub(GitHubRepositoryFake):
    """The GitHub repository fake, also readable by the repository's immutable id."""

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repositories/{REPO_ID}":
            request = httpx.Request(
                request.method,
                request.url.copy_with(path=f"/repos/{REPO}"),
                headers=request.headers,
            )
        return super().handle(request)


class _ByHost(httpx.AsyncBaseTransport):
    """The Jira site to the Jira fake, every other host to the GitHub fake."""

    def __init__(self, jira: jira_fake.JiraFake, github: _GitHub) -> None:
        self._jira = httpx.ASGITransport(app=jira.app)
        self._github = httpx.MockTransport(github.handle)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == jira_fake.SITE_HOST:
            return await self._jira.handle_async_request(request)
        return await self._github.handle_async_request(request)


def _document() -> dict[str, Any]:
    document = forges_document()
    document["trackers"].append(jira_fake.jira_tracker_entry())
    document["bindings"] = [
        {
            "name": BINDING,
            "tracker": TRACKER,
            "tracker_scope_id": jira_fake.CLOUD_ID,
            "repos": [
                {
                    "alias": "bot",
                    "code_host": "github",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                }
            ],
            "default_repo": "bot",
            "start_authority": {"type": "allowlist", "accounts": [STARTER_ID]},
        }
    ]
    return document


@dataclass
class Intake:
    client: TestClient
    jira: jira_fake.JiraFake
    github: _GitHub
    volume: ProjectedVolume
    token: str
    spare: str
    due: dict[str, float] = field(default_factory=dict)
    clock: float = 10_000.0

    def agent(self) -> uuid.UUID:
        created = self.client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-jira-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "jira", "address": BINDING},
            },
        )
        assert created.status_code == 201, created.text
        return uuid.UUID(created.json()["id"])

    def poll(self) -> None:
        """One pass after the poll interval, not the sweep's."""

        self.clock += 60.0

        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            try:
                maker = async_sessionmaker(engine, expire_on_commit=False)
                async with httpx.AsyncClient(transport=_ByHost(self.jira, self.github)) as client:
                    await factory_tracker_intake.poll_trackers(
                        maker, get_settings(), client, due=self.due, clock=self.clock
                    )
            finally:
                await engine.dispose()

        asyncio.run(go())


@pytest.fixture
def intake(clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Intake]:
    sentinel = secrets.token_hex(8)
    token, spare = f"jira-cur-{sentinel}", f"jira-next-{sentinel}"
    jira = jira_fake.JiraFake(token=token)
    jira.accept_credential(jira_fake.BOT_EMAIL, spare, jira.bot)
    volume = ProjectedVolume(tmp_path / "jira-token", f"{token}\n")
    rendered = json.loads(forges_env_json(_document(), tmp_path / "forges"))
    for tracker in rendered["trackers"]:
        if tracker["name"] == TRACKER:
            tracker["credential"]["token_file"] = str(volume.path)
    env = {
        "GITHUB_APP_ID": "51",
        "GITHUB_APP_PRIVATE_KEY": "example-private-key",
        "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_TOKEN": "",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "FACTORY_ENABLED": "true",
        "FACTORY_INTAKE": "webhook",
        "FACTORY_RECONCILE_INTERVAL_S": "300",
        "CURIE_FORGES": json.dumps(rendered),
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "RUNS_STREAM": f"test:curie:tracker-intake-auth:{uuid.uuid4().hex}",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    get_settings.cache_clear()
    fake_installation_tokens(monkeypatch)
    try:
        with TestClient(create_app()) as client:
            yield Intake(client, jira, _GitHub(), volume, token, spare)
    finally:
        get_settings.cache_clear()


def _work_items(issue: jira_fake.JiraIssue) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id FROM curie.work_items WHERE tracker_kind = 'jira_cloud' "
        "AND tracker_issue_id = :id",
        {"id": str(issue.id)},
    )


def _cursor() -> Any:
    rows = _rows(
        "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'jira_cloud' "
        "AND tracker_host = :host AND tracker_scope_id = :scope",
        {"host": jira_fake.SITE_HOST, "scope": jira_fake.CLOUD_ID},
    )
    return None if not rows else (rows[0]["etags"] or {}).get(f"marked:{LABEL}")


def _labelled(intake: Intake) -> jira_fake.JiraIssue:
    starter = intake.jira.add_user(STARTER_ID, "Ada Starter")
    issue = intake.jira.seed_issue(
        "Widget parser drops the last field", "Parsing a three-field line yields two fields."
    )
    intake.jira.add_label(issue.id, LABEL, starter)
    return issue


def _needles(*tokens: str) -> list[str]:
    found: list[str] = []
    for token in tokens:
        found += [token, jira_fake.basic(jira_fake.BOT_EMAIL, token)]
    return found


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in _intake_records(caplog)
        if record.levelno >= logging.WARNING
    ]


def test_a_rejected_token_defers_intake_with_a_warning_and_no_token(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    intake.agent()
    issue = _labelled(intake)
    # Jira Cloud answers a refused credential with 401
    # (https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#status-codes).
    intake.jira.fail_next("search", status=401)

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    assert _work_items(issue) == []
    assert _cursor() is None
    [warning] = [message for message in _warnings(caplog) if REJECTED in message]
    for expected in (TRACKER, BINDING, LABEL):
        assert expected in warning, warning
    assert not [needle for needle in _needles(intake.token) if needle in caplog.text]


def test_an_unreadable_token_file_defers_intake_with_a_warning_and_a_valid_one_admits(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    intake.agent()
    issue = _labelled(intake)
    intake.volume.break_with("empty")
    mark = len(intake.jira.requests)

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    assert _work_items(issue) == []
    assert _cursor() is None
    [warning] = [message for message in _warnings(caplog) if UNREADABLE in message]
    for expected in (TRACKER, BINDING, LABEL):
        assert expected in warning, warning
    sent = [request.authorization for request in intake.jira.requests[mark:]]
    assert jira_fake.basic(jira_fake.BOT_EMAIL, intake.token) not in sent
    assert not [needle for needle in _needles(intake.token) if needle in caplog.text]

    # The operator replaces the file; the next pass reads it, with no restart.
    intake.volume.publish(f"{intake.spare}\n")
    mark = len(intake.jira.requests)
    intake.poll()

    assert len(_work_items(issue)) == 1
    recovered = {request.authorization for request in intake.jira.requests[mark:]}
    assert jira_fake.basic(jira_fake.BOT_EMAIL, intake.spare) in recovered
    assert jira_fake.basic(jira_fake.BOT_EMAIL, intake.token) not in recovered


def test_a_plain_unavailable_deferral_of_a_pass_stays_at_info(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    intake.agent()
    issue = _labelled(intake)
    intake.jira.fail_next("search", status=500)

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    assert _work_items(issue) == []
    deferred = [r for r in _intake_records(caplog) if "deferred" in r.getMessage()]
    assert deferred, caplog.text
    assert {record.levelno for record in deferred} == {logging.INFO}
    assert _warnings(caplog) == []
