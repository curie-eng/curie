"""The Jira stub serves the Jira fake's behaviour over TLS (#5791 AC 1 to AC 3).

Every test starts ``tools/jira-stub/jira_stub.py serve`` as a subprocess behind
a certificate signed by a CA made here, and drives it with the production Jira
tracker (`build_registry` -> `binding_tracker` -> `for_binding`) over a real
TLS client, the way an install does. The parity test replays one seeding on
the ASGI fake the adapter's own tests use and on the stub, and requires the
same answers from both.
"""

from __future__ import annotations

import base64
import re
import ssl
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.config import ForgesConfig
from curie_api.forges.registry import build_registry
from curie_api.forges.types import JIRA_CLOUD, Disposition, ReplyTarget, TrackerIssueRef

TESTS = Path(__file__).resolve()
REPO_ROOT = TESTS.parents[3]
sys.path.insert(0, str(REPO_ROOT / "apps/api/tests"))
sys.path.insert(0, str(REPO_ROOT / "tools/factory-e2e"))

import jira_one_ticket_run as run  # noqa: E402
import one_ticket_jira as ojt  # noqa: E402
from forge_fakes import jira as fake_jira  # noqa: E402

LABEL = fake_jira.LABEL
QUESTIONS_LABEL = "curie-factory:questions"
MARKER = "curie-factory-questions:00000000-0000-4000-8000-000000000001"
QUESTIONS = "### Questions\n\n1. Which spelling?\n2. Which sign?\n"
LABELLER = ojt.LABELLER
OUTSIDER = ojt.OUTSIDER
ALLOWLIST = {"type": "allowlist", "accounts": [LABELLER["account_id"]]}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _ca(workdir: Path) -> tuple[Path, Path]:
    cert, key = workdir / "ca.pem", workdir / "ca.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=jira-stub-test-ca",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return cert, key


@contextmanager
def served(workdir: Path) -> Iterator[run.JiraStubProcess]:
    """The stub on 127.0.0.1, stopped and checked closed on the way out."""

    cert, key = _ca(workdir)
    stub = run.JiraStubProcess(workdir / "stub", "127.0.0.1", cert, key, python=(sys.executable,))
    try:
        stub.start({"PATH": "/usr/bin:/bin"}, timeout=60)
        yield stub
    finally:
        teardown = stub.close()
    assert teardown["exited"] and not teardown["listening"], teardown


def _stub_tracker(stub: run.JiraStubProcess, client: httpx.AsyncClient) -> Any:
    """The production tracker for a site at the stub, reached by its gateway URL."""

    document = fake_jira.jira_document(api_url=stub.api_url, start_authority=ALLOWLIST)
    document["trackers"][0]["host"] = stub.netloc
    registry = build_registry(ForgesConfig.model_validate(document))
    connection = registry.binding_tracker(fake_jira.TRACKER_NAME)
    return connection.for_binding(client, registry.binding(fake_jira.BINDING_NAME))


def _tls_client(stub: run.JiraStubProcess) -> httpx.AsyncClient:
    return httpx.AsyncClient(verify=ssl.create_default_context(cafile=str(stub.ca_cert)))


def _stub_ref(stub: run.JiraStubProcess, created: dict[str, str]) -> TrackerIssueRef:
    return TrackerIssueRef(JIRA_CLOUD, stub.netloc, ojt.CLOUD_ID, created["id"], created["key"])


class Site:
    """One seeding vocabulary over the fake (in process) or the stub (over HTTP)."""

    def __init__(
        self,
        tracker: Any,
        add_user: Callable[[dict[str, str]], None],
        create_issue: Callable[[str, str, str], TrackerIssueRef],
        add_label: Callable[[TrackerIssueRef, str, str], None],
        add_comment: Callable[[TrackerIssueRef, str, str], str],
    ) -> None:
        self.tracker = tracker
        self.add_user = add_user
        self.create_issue = create_issue
        self.add_label = add_label
        self.add_comment = add_comment


def _fake_site(fake: fake_jira.JiraFake) -> Site:
    def create(summary: str, description: str, reporter: str) -> TrackerIssueRef:
        person = fake.users[reporter]
        return fake.ref(fake.seed_issue(summary, description, creator=person, reporter=person))

    def add_user(person: dict[str, str]) -> None:
        fake.add_user(person["account_id"], person["display_name"])

    def add_label(ref: TrackerIssueRef, label: str, by: str) -> None:
        fake.add_label(ref, label, fake.users[by])

    def add_comment(ref: TrackerIssueRef, text: str, by: str) -> str:
        return fake.add_comment(ref, fake.users[by], text)

    tracker = fake_jira.jira_tracker(fake, api_url=fake_jira.SCOPED_API, start_authority=ALLOWLIST)
    return Site(tracker, add_user, create, add_label, add_comment)


def _served_site(stub: run.JiraStubProcess, client: httpx.AsyncClient) -> Site:
    control = stub.client()

    def create(summary: str, description: str, reporter: str) -> TrackerIssueRef:
        return _stub_ref(stub, control.create_issue(summary, description, reporter))

    def add_label(ref: TrackerIssueRef, label: str, by: str) -> None:
        control.add_label(ref.issue_id, label, by)

    def add_comment(ref: TrackerIssueRef, text: str, by: str) -> str:
        return str(control.add_comment(ref.issue_id, text, by)["id"])

    return Site(_stub_tracker(stub, client), control.add_user, create, add_label, add_comment)


def _notices(page: Any) -> list[tuple[str, str, str, str, str]]:
    """What a poll decided, without the cursor and the site's clock."""

    return [
        (n.marker, n.actor.id, n.event_id, n.disposition.value, n.issue.issue_id)
        for n in page.notices
    ]


async def _walk(site: Site) -> dict[str, Any]:
    """Label, ask, label the questions, answer: the scenario's tracker calls in order."""

    for person in (LABELLER, OUTSIDER):
        site.add_user(person)
    ref = site.create_issue(
        "Add Kelvin conversions", "Add `k` to unitconv.", LABELLER["account_id"]
    )
    site.add_label(ref, LABEL, LABELLER["account_id"])
    tracker = site.tracker
    admitted = await tracker.poll_marked(None, running=(), answering=())
    target = ReplyTarget.on_issue(ref)
    first = await tracker.marked_comments.upsert_marked(target, MARKER, QUESTIONS)
    again = await tracker.marked_comments.upsert_marked(target, MARKER, QUESTIONS)
    await tracker.set_state_label(ref, add=QUESTIONS_LABEL, remove=())
    labels = await tracker.ticket_labels(ref)
    answer = site.add_comment(ref, "Use K; reject negatives.", LABELLER["account_id"])
    answered = await tracker.poll_marked(admitted.cursor, running=(), answering=(ref,))
    ticket = await tracker.read_ticket(ref)
    return {
        "key": ref.display_key,
        "admitted": _notices(admitted),
        "upsert": [(first.comment.id, first.written), (again.comment.id, again.written)],
        "labels": labels,
        "answer": answer,
        "answered": _notices(answered),
        # Comment times are rendered from each site's own clock.
        "ticket": re.sub(r"\d{4}-\d\d-\d\d[T ][\d:.]+(Z|[+-]\d{2}:?\d{2})?", "<t>", ticket),
    }


@pytest.mark.anyio
async def test_the_stub_answers_the_tracker_as_the_asgi_fake_does(tmp_path: Path) -> None:
    expected = await _walk(_fake_site(fake_jira.JiraFake()))
    with served(tmp_path) as stub:
        async with _tls_client(stub) as client:
            observed = await _walk(_served_site(stub, client))
    assert observed == expected
    # The walk is not vacuous: one admission, one written comment, one answer.
    assert [notice[3] for notice in observed["admitted"]] == [Disposition.ADMIT.value]
    assert observed["upsert"][0][1] is True and observed["upsert"][1][1] is False
    assert QUESTIONS_LABEL in observed["labels"].labels
    assert [notice[1:4] for notice in observed["answered"]] == [
        (LABELLER["account_id"], observed["answer"], Disposition.ANSWER.value)
    ]


@pytest.mark.anyio
async def test_the_stub_refuses_a_wrong_api_token_with_401(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        async with _tls_client(stub) as client:
            for token, status in ((fake_jira.TOKEN, 200), ("not-the-token", 401)):
                basic = base64.b64encode(f"{fake_jira.BOT_EMAIL}:{token}".encode()).decode()
                response = await client.get(
                    f"{stub.api_url}/rest/api/3/myself",
                    headers={"Authorization": f"Basic {basic}"},
                )
                assert response.status_code == status


@pytest.mark.anyio
async def test_control_refuses_a_request_without_the_run_token(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        control = stub.client()
        for person in (LABELLER, OUTSIDER):
            control.add_user(person)
        created = control.create_issue("A ticket", "Body.", LABELLER["account_id"])
        control.add_label(created["id"], LABEL, LABELLER["account_id"])
        ref = _stub_ref(stub, created)
        async with _tls_client(stub) as client:
            tracker = _stub_tracker(stub, client)
            admitted = await tracker.poll_marked(None, running=(), answering=())
            comment = {"text": "Seeded without the token.", "author": LABELLER["account_id"]}
            path = f"/_stub/issues/{created['id']}/comments"
            for headers in ({}, {ojt.TOKEN_HEADER: "not-the-run-token"}):
                refused = await client.post(f"{stub.base_url}{path}", json=comment, headers=headers)
                assert refused.status_code == 403
            assert (await client.get(f"{stub.base_url}/_stub/state")).status_code == 403
            # The counterexample: the refused comment seeded nothing to answer with.
            page = await tracker.poll_marked(admitted.cursor, running=(), answering=(ref,))
        assert _notices(page) == []
        assert ojt.issue_of(control.state(), created["id"])["comments"] == []


@pytest.mark.anyio
async def test_control_seeds_a_plain_comment_the_next_poll_reads_as_the_answer(
    tmp_path: Path,
) -> None:
    with served(tmp_path) as stub:
        control = stub.client()
        for person in (LABELLER, OUTSIDER):
            control.add_user(person)
        created = control.create_issue("A ticket", "Body.", LABELLER["account_id"])
        control.add_label(created["id"], LABEL, LABELLER["account_id"])
        ref = _stub_ref(stub, created)
        async with _tls_client(stub) as client:
            tracker = _stub_tracker(stub, client)
            admitted = await tracker.poll_marked(None, running=(), answering=())
            seeded = control.add_comment(created["id"], "Use K.", LABELLER["account_id"])
            page = await tracker.poll_marked(admitted.cursor, running=(), answering=(ref,))
    assert [(n.actor.id, n.event_id, n.disposition) for n in page.notices] == [
        (LABELLER["account_id"], str(seeded["id"]), Disposition.ANSWER)
    ]
    assert page.notices[0].issue == ref


def test_the_adapter_constants_are_the_fakes() -> None:
    assert (ojt.CLOUD_ID, ojt.BOT_ACCOUNT_ID, ojt.BOT_EMAIL, ojt.BOT_TOKEN) == (
        fake_jira.CLOUD_ID,
        fake_jira.BOT_ACCOUNT_ID,
        fake_jira.BOT_EMAIL,
        fake_jira.TOKEN,
    )


def test_serve_refuses_to_start_without_a_control_token(tmp_path: Path) -> None:
    cert, key = _ca(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            str(run.STUB),
            "serve",
            "--host",
            "127.0.0.1",
            "--ca-cert",
            str(cert),
            "--ca-key",
            str(key),
            "--workdir",
            str(tmp_path / "tls"),
        ],
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 2
    assert run.TOKEN_ENV in result.stderr
    assert result.stdout == ""
