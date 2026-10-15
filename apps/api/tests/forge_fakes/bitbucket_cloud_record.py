"""The Bitbucket Cloud recording session, its scrubber and its scan (#4401, decision 16).

`drive` is the one session, run live by `record_session` and replayed by
``tests/forges/test_bitbucket_cloud_recorded.py``. It makes the same requests
in the same order in both modes; only the git steps differ (`LiveGit` pushes,
`ReplayGit` answers from the recording's ``meta``). Port calls go through the
real Cloud adapter; the test side (statuses, the reviewer's comments, the
teardown) through plain clients on the same transport, so every exchange
lands in one ordered recording:

1. If ``main`` has no commit, seed it with a README commit (git).
2. Resolve the repository by UUID, read the push credential, push the branch
   ``curie-record-<UTC timestamp>`` with the credential in ``GIT_CONFIG_*``
   environment variables, never in argv.
3. Open a pull request; post a failed status, then successful ones, on its
   head (and one on the base commit, which must not count).
4. Post a marked comment, then edit it.
5. As the reviewer, post an inline comment and a top-level comment; reply to
   the inline one in its thread as the factory.
6. Read the feedback back, twice (the second from the cursor), and verify it.
7. Decline the pull request, delete the branch, and list what is left open.

When a live run fails, including when it runs out of its request budget,
`cleanup` declines the pull request and deletes the branch that step 7 has not
already, through the session's ``cleanup`` client on its own reserved
allowance. It attempts the deletion even when the decline fails.

`scrub` rewrites everything identifying before a recording is written, and
`scan_recording` is the check both the recorder and the scrub test apply.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.types import (
    CiRollup,
    Commit,
    Credential,
    CredentialExpiry,
    CredentialHeader,
    CredentialScope,
    FeedbackPage,
    PullRequest,
    ReplyTarget,
    RepositoryRef,
    UpsertResult,
)

from forge_fakes.bitbucket_cloud_replay import (
    CLEANUP_REQUESTS,
    MAX_REQUESTS,
    AllowanceTransport,
    Cassette,
    RecordingTransport,
    kept_header,
)

logger = logging.getLogger(__name__)

API_URL = "https://api.bitbucket.org/2.0"
HOST = "bitbucket.org"
API_HOST = "api.bitbucket.org"
MAIN = "main"
PLACEHOLDER_WORKSPACE = "example-workspace"
PLACEHOLDER_SLUG = "example-repo"
RECORD_FILE = "curie-record.txt"
PAGE_SIZE = 2

ENV_RECORD = "CURIE_BITBUCKET_CLOUD_RECORD"
ENV_REPO = "CURIE_BITBUCKET_CLOUD_REPO"
ENV_TOKEN_FILE = "CURIE_BITBUCKET_CLOUD_TOKEN_FILE"
ENV_REVIEWER_TOKEN_FILE = "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE"
RECORD_VARIABLES = (ENV_REPO, ENV_TOKEN_FILE, ENV_REVIEWER_TOKEN_FILE)

MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000101"
THREAD_MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000102"
MENTION = "curie"
REVIEW_INLINE = f"@{MENTION} please rename this line"
REVIEW_TOP = f"@{MENTION} also update the changelog"
STATUS_KEYS = ("unit", "lint")
OTHER_COMMIT_KEY = "other-commit"


def adapter_for(client: httpx.AsyncClient, token: str, *, api_url: str = API_URL) -> Any:
    """The Cloud adapter on ``client``, paging at two so listings span pages.

    Imported here, not at module load, so the suites collect while the adapter
    is missing.
    """

    from curie_api.forges.bitbucket_cloud.code_host import BitbucketCloudCodeHost

    return BitbucketCloudCodeHost(
        client,
        entry="bitbucket-cloud",
        api_url=api_url,
        token=token,
        credentials=StaticTokenProvider(
            token,
            origin=f"https://{HOST}",
            header=CredentialHeader.AUTHORIZATION_BASIC,
            username="x-token-auth",
        ),
        page_size=PAGE_SIZE,
    )


def side_client(
    transport: httpx.AsyncBaseTransport, token: str, *, api_url: str = API_URL
) -> httpx.AsyncClient:
    """A plain client for the test side's own calls, as the token's account."""

    return httpx.AsyncClient(
        transport=transport,
        base_url=api_url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=15.0,
    )


# --- the session -----------------------------------------------------------------


class Git(Protocol):
    async def seed_main(self) -> None: ...

    async def push_branch(self, credential: Credential, branch: str) -> str:
        """Push one commit to ``branch`` from ``main``; returns its sha."""
        ...


@dataclass
class Session:
    workspace: str
    slug: str
    factory: Any  # the Cloud adapter, as the factory token
    api: httpx.AsyncClient  # the test side, as the factory token
    reviewer_api: httpx.AsyncClient  # the test side, as the reviewer token
    git: Git
    branch: str
    # Live runs only: the factory token on the reserved cleanup allowance. A
    # replay has nothing to clean up and passes None.
    cleanup: httpx.AsyncClient | None

    @property
    def repo_path(self) -> str:
        return f"/repositories/{self.workspace}/{self.slug}"


@dataclass
class Observed:
    """What the session saw, for the recorded suite to assert on."""

    seeded: bool = False
    repo_uuid: str | None = None
    resolved: RepositoryRef | None = None
    base: str | None = None
    credential: Credential | None = None
    head: str | None = None
    commit: Commit | None = None
    pull: PullRequest | None = None
    red: CiRollup | None = None
    green: CiRollup | None = None
    marked: list[UpsertResult] = field(default_factory=list)
    marked_payload: dict[str, Any] | None = None
    review_root: str | None = None
    review_top: str | None = None
    thread_reply: UpsertResult | None = None
    feedback: FeedbackPage | None = None
    after: FeedbackPage | None = None
    verified: list[bool] = field(default_factory=list)
    declined: PullRequest | None = None
    # What the teardown or the cleanup has already undone.
    pull_declined: bool = False
    branch_deleted: bool = False
    open_after: list[str] = field(default_factory=list)
    branches_after: list[str] = field(default_factory=list)


async def _ok(response: httpx.Response) -> dict[str, Any]:
    if response.is_error:
        raise AssertionError(
            f"test side {response.request.method} {response.request.url.path}"
            f" answered {response.status_code}"
        )
    body: dict[str, Any] = response.json() if response.content else {}
    return body


async def _post_status(session: Session, sha: str, key: str, state: str) -> None:
    await _ok(
        await session.api.post(
            f"{session.repo_path}/commit/{sha}/statuses/build",
            json={
                "key": key,
                "state": state,
                "name": key,
                "url": f"https://example.invalid/builds/{key}",
                "description": f"curie recording {key}",
            },
        )
    )


async def _flow(session: Session, seen: Observed) -> None:
    factory, repo_path = session.factory, session.repo_path
    main = await session.api.get(f"{repo_path}/refs/branches/{MAIN}")
    if main.status_code == 404:
        await session.git.seed_main()
        seen.seeded = True
    else:
        await _ok(main)
    seen.repo_uuid = (await _ok(await session.api.get(repo_path)))["uuid"]
    seen.resolved = repository = await factory.resolve_repository(seen.repo_uuid)
    seen.base = await factory.branch_head(repository, MAIN)
    assert seen.base is not None
    seen.credential = await factory.credential(repository, CredentialScope.PUSH)
    seen.head = await session.git.push_branch(seen.credential, session.branch)
    assert await factory.branch_head(repository, session.branch) == seen.head
    seen.commit = await factory.read_commit(repository, seen.head)
    assert await factory.find_pull_request(repository, head_ref=session.branch) is None
    seen.pull = pull = await factory.open_pull_request(
        repository,
        head_ref=session.branch,
        base_ref=MAIN,
        title="Curie recording",
        body="Recorded by the Bitbucket Cloud adapter suite (#4401).",
    )

    await _post_status(session, seen.head, STATUS_KEYS[0], "FAILED")
    seen.red = await factory.observe_ci(repository, seen.head)
    for key in STATUS_KEYS:
        await _post_status(session, seen.head, key, "SUCCESSFUL")
    await _post_status(session, seen.base, OTHER_COMMIT_KEY, "FAILED")
    seen.green = await factory.observe_ci(repository, seen.head)

    comments = factory.marked_comments
    on_pull = ReplyTarget.on_pull_request(pull.ref)
    for body in ("Working on it.", "Done.", "Done."):
        seen.marked.append(await comments.upsert_marked(on_pull, MARKER, body))
    comments_path = f"{repo_path}/pullrequests/{pull.ref.number}/comments"
    seen.marked_payload = await _ok(
        await session.api.get(f"{comments_path}/{seen.marked[0].comment.id}")
    )

    root = await _ok(
        await session.reviewer_api.post(
            comments_path,
            json={"content": {"raw": REVIEW_INLINE}, "inline": {"path": RECORD_FILE, "to": 1}},
        )
    )
    seen.review_root = str(root["id"])
    top = await _ok(
        await session.reviewer_api.post(comments_path, json={"content": {"raw": REVIEW_TOP}})
    )
    seen.review_top = str(top["id"])
    seen.thread_reply = await comments.upsert_marked(
        ReplyTarget.on_thread(pull.ref, seen.review_root), THREAD_MARKER, "Renamed it."
    )

    seen.feedback = await factory.list_review_feedback(pull.ref, None)
    seen.after = await factory.list_review_feedback(pull.ref, seen.feedback.cursor)
    seen.verified = [await factory.verify_feedback(item) for item in seen.feedback.items]


async def _decline(client: httpx.AsyncClient, session: Session, seen: Observed) -> None:
    assert seen.pull is not None
    await _ok(await client.post(f"{session.repo_path}/pullrequests/{seen.pull.ref.number}/decline"))
    seen.pull_declined = True


async def _decline_unseen(client: httpx.AsyncClient, session: Session) -> None:
    """Decline what the branch opened when the run failed before it read the id back.

    Opening the pull request is one request and reading it back another, so a
    run can fail between them with a pull request open that it never saw.
    """

    listed = await _ok(
        await client.get(
            f"{session.repo_path}/pullrequests",
            params={"q": f'source.branch.name = "{session.branch}"', "state": "OPEN"},
        )
    )
    numbers = [p["id"] for p in listed["values"]]
    if len(numbers) > 1:
        raise AssertionError(f"{len(numbers)} open pull requests on the recording branch")
    for number in numbers:
        await _ok(await client.post(f"{session.repo_path}/pullrequests/{number}/decline"))


async def _delete_branch(client: httpx.AsyncClient, session: Session, seen: Observed) -> None:
    branch = quote(session.branch, safe="")
    deleted = await client.delete(f"{session.repo_path}/refs/branches/{branch}")
    if deleted.status_code != 204:
        raise AssertionError(f"deleting the recording branch answered {deleted.status_code}")
    seen.branch_deleted = True


async def _teardown(session: Session, seen: Observed) -> None:
    """Step 7, recorded: decline, delete, and read back that nothing is left."""

    repo_path = session.repo_path
    assert seen.pull is not None and seen.head is not None
    await _decline(session.api, session, seen)
    seen.declined = await session.factory.read_pull_request(seen.pull.ref)
    await _delete_branch(session.api, session, seen)
    listed = await _ok(await session.api.get(f"{repo_path}/pullrequests", params={"state": "OPEN"}))
    seen.open_after = [p["source"]["branch"]["name"] for p in listed["values"]]
    branches = await _ok(
        await session.api.get(f"{repo_path}/refs/branches", params={"q": 'name ~ "curie-record-"'})
    )
    seen.branches_after = [b["name"] for b in branches["values"]]


async def cleanup(client: httpx.AsyncClient, session: Session, seen: Observed) -> None:
    """Undo what a failed live run left: decline the pull request, delete the branch.

    Each step is attempted whatever the other did; the first failure is raised
    once both have run.
    """

    failures: list[Exception] = []
    if seen.pull is not None and not seen.pull_declined:
        try:
            await _decline(client, session, seen)
        except Exception as failed:  # noqa: BLE001  the branch is still deleted
            failures.append(failed)
    elif seen.pull is None and seen.head is not None:
        try:
            await _decline_unseen(client, session)
        except Exception as failed:  # noqa: BLE001  the branch is still deleted
            failures.append(failed)
    if seen.head is not None and not seen.branch_deleted:
        try:
            await _delete_branch(client, session, seen)
        except Exception as failed:  # noqa: BLE001  raised below, after the other step
            failures.append(failed)
    if failures:
        raise failures[0]


async def drive(session: Session) -> Observed:
    """Run the session; a live run cleans up when any step fails, its teardown too."""

    seen = Observed()
    try:
        await _flow(session, seen)
        await _teardown(session, seen)
    except BaseException as failure:
        if session.cleanup is not None:
            try:
                await cleanup(session.cleanup, session, seen)
            except Exception as failed:  # noqa: BLE001  the first failure is the one to report
                logger.error("cleanup after a failed recording also failed: %s", type(failed))
                failure.add_note(f"the cleanup after it also failed: {type(failed).__name__}")
        raise
    return seen


# --- git -------------------------------------------------------------------------


class ReplayGit:
    """The git steps of a replay: what the recording says they produced."""

    def __init__(self, meta: Mapping[str, Any]) -> None:
        self._meta = meta

    async def seed_main(self) -> None:
        assert self._meta["seeded"] is True

    async def push_branch(self, credential: Credential, branch: str) -> str:
        assert branch == self._meta["branch"]
        head: str = self._meta["head"]
        return head


class LiveGit:
    """Git over HTTPS to the test repository, the credential in the environment."""

    def __init__(self, workspace: str, slug: str, token: str) -> None:
        self._remote = f"https://{HOST}/{workspace}/{slug}.git"
        # Only for seeding an empty main, before the adapter can resolve it.
        self._seed = Credential(
            origin=f"https://{HOST}",
            header=CredentialHeader.AUTHORIZATION_BASIC,
            secret=token,
            scope=CredentialScope.PUSH,
            expiry=CredentialExpiry.UNKNOWN,
            username="x-token-auth",
        )

    def _git(self, credential: Credential, cwd: str, *args: str) -> str:
        env = {
            **os.environ,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": f"http.https://{HOST}/.extraHeader",
            "GIT_CONFIG_VALUE_0": credential.git_header(),
            "GIT_AUTHOR_NAME": "Curie Recorder",
            "GIT_AUTHOR_EMAIL": "recorder@example.invalid",
            "GIT_COMMITTER_NAME": "Curie Recorder",
            "GIT_COMMITTER_EMAIL": "recorder@example.invalid",
        }
        done = subprocess.run(
            ["git", "-c", "http.followRedirects=false", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if done.returncode != 0:
            raise RuntimeError(f"git {args[0]} exited {done.returncode}")
        return done.stdout.strip()

    def _seed_main(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            self._git(self._seed, work, "init", "-b", MAIN)
            Path(work, "README.md").write_text("Curie recording repository.\n")
            self._git(self._seed, work, "add", "README.md")
            self._git(self._seed, work, "commit", "-m", "Seed main")
            self._git(self._seed, work, "push", self._remote, f"HEAD:refs/heads/{MAIN}")

    def _push_branch(self, credential: Credential, branch: str) -> str:
        with tempfile.TemporaryDirectory() as work:
            self._git(
                credential, work, "clone", "--depth", "1", "--branch", MAIN, self._remote, "."
            )
            Path(work, RECORD_FILE).write_text(f"Recorded on {branch}.\n")
            self._git(credential, work, "add", RECORD_FILE)
            self._git(credential, work, "commit", "-m", f"Record {branch}")
            self._git(credential, work, "push", self._remote, f"HEAD:refs/heads/{branch}")
            return self._git(credential, work, "rev-parse", "HEAD")

    async def seed_main(self) -> None:
        await asyncio.to_thread(self._seed_main)

    async def push_branch(self, credential: Credential, branch: str) -> str:
        return await asyncio.to_thread(self._push_branch, credential, branch)


# --- live recording --------------------------------------------------------------


def missing_variables(env: Mapping[str, str]) -> list[str]:
    return [name for name in RECORD_VARIABLES if not env.get(name)]


def _secret(path: str) -> str:
    return Path(path).read_text().strip()


async def record_session(env: Mapping[str, str]) -> tuple[Cassette, Observed]:
    """Drive the session live and return the scrubbed recording with what it saw.

    Raises before returning when the scan finds anything real left in it.
    """

    missing = missing_variables(env)
    if missing:
        raise ValueError(f"recording needs {', '.join(missing)}")
    workspace, slash, slug = env[ENV_REPO].partition("/")
    if not slash or not workspace or not slug or "/" in slug:
        raise ValueError(f"{ENV_REPO} must be <workspace>/<repo slug>")
    token, reviewer_token = _secret(env[ENV_TOKEN_FILE]), _secret(env[ENV_REVIEWER_TOKEN_FILE])
    branch = f"curie-record-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    recording = RecordingTransport(httpx.AsyncHTTPTransport(), limit=MAX_REQUESTS)
    allowance = AllowanceTransport(httpx.AsyncHTTPTransport(), limit=CLEANUP_REQUESTS)
    async with (
        httpx.AsyncClient(transport=recording, timeout=15.0) as adapter_client,
        side_client(recording, token) as api,
        side_client(recording, reviewer_token) as reviewer_api,
        side_client(allowance, token) as cleanup_api,
    ):
        session = Session(
            workspace,
            slug,
            adapter_for(adapter_client, token),
            api,
            reviewer_api,
            LiveGit(workspace, slug, token),
            branch,
            cleanup=cleanup_api,
        )
        seen = await drive(session)
    assert seen.open_after == [] and seen.branches_after == [], "the recording left state behind"
    assert seen.pull is not None and seen.feedback is not None
    reviewer_ids = {item.author.id for item in seen.feedback.items}
    meta = {
        "branch": branch,
        "head": seen.head,
        "seeded": seen.seeded,
        "repo_uuid": seen.repo_uuid,
        "reviewer_account": sorted(reviewer_ids)[0] if reviewer_ids else None,
        "requests": len(recording.exchanges),
        "recorded_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    raw = Cassette(meta, recording.exchanges).to_json()
    scrubbed, originals = scrub(raw, workspace=workspace, slug=slug)
    findings = scan_recording(scrubbed, (*originals, workspace, slug, token, reviewer_token))
    if findings:
        raise AssertionError(f"the scrubbed recording still carries {findings}")
    return Cassette.from_json(scrubbed), seen


# --- scrub and scan --------------------------------------------------------------

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_PLACEHOLDER_UUID = re.compile(r"00000000-0000-4000-8000-[0-9]{12}")
_PLACEHOLDER_ACCOUNT = re.compile(r"placeholder-account-[0-9]+")
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_URL = re.compile(r"https?://[^\s\"'<>]+")
_TOKEN = re.compile(r"(?<![A-Za-z0-9_])(?:ATBB|ATCTT)[A-Za-z0-9_=-]{16,}")
_REPO_SEGMENTS = re.compile(
    r"(/repositories/|/workspaces/|(?<![A-Za-z0-9_.-])bitbucket\.org/)"
    r"([^/?#\s\"'<>]+)(?:/([^/?#\s\"'<>]+))?"
)
CREDENTIAL_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie", "set-cookie"})
# First segments under bitbucket.org that are product routes, not a workspace.
BITBUCKET_ROUTES = frozenset({"account", "site", "product", "dashboard", "repo"})
# The workspace-scoped route that project pages live under: bitbucket.org/<workspace>/workspace/...
WORKSPACE_ROUTE = "workspace"
PLACEHOLDER_EMAIL = "placeholder@example.invalid"
PLACEHOLDER_AUTHOR = f"Placeholder User <{PLACEHOLDER_EMAIL}>"
PLACEHOLDER_AVATAR = "https://example.invalid/avatar.png"


def _walk(node: Any, visit: Callable[[str | None, Any], Any], key: str | None = None) -> Any:
    """Rebuild ``node``, passing every value with its key through ``visit``."""

    if isinstance(node, dict):
        return {k: _walk(v, visit, k) for k, v in node.items()}
    if isinstance(node, list):
        return [_walk(v, visit, key) for v in node]
    return visit(key, node)


def _strings(node: Any) -> Iterable[tuple[str | None, str]]:
    found: list[tuple[str | None, str]] = []

    def visit(key: str | None, value: Any) -> Any:
        if isinstance(value, str):
            found.append((key, value))
        return value

    _walk(node, visit)
    return found


def _authors(node: Any) -> Any:
    """Every commit author's ``raw`` ("Name <email>") replaced whole."""

    if isinstance(node, dict):
        rebuilt = {k: _authors(v) for k, v in node.items()}
        if rebuilt.get("type") == "author" and "raw" in rebuilt:
            rebuilt["raw"] = PLACEHOLDER_AUTHOR
        return rebuilt
    if isinstance(node, list):
        return [_authors(v) for v in node]
    return node


def _bounded(value: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9_-]){re.escape(value)}(?![A-Za-z0-9_-])")


def scrub(
    document: Mapping[str, Any], *, workspace: str, slug: str
) -> tuple[dict[str, Any], set[str]]:
    """``document`` with every identifying value replaced by a placeholder.

    Returns the scrubbed document and every original value it replaced, for
    `scan_recording` to prove none is left.
    """

    accounts: dict[str, str] = {}
    names: dict[str, str] = {}
    uuids: dict[str, str] = {}
    # Every response keeps only allowlisted headers, whatever was captured.
    exchanges = [
        {
            **exchange,
            "response": {
                **exchange["response"],
                "headers": {
                    k.lower(): v
                    for k, v in exchange["response"]["headers"].items()
                    if kept_header(k)
                },
            },
        }
        for exchange in document["exchanges"]
    ]
    ordered = _authors({"exchanges": exchanges, "meta": dict(document["meta"])})
    for key, value in _strings(ordered):
        if key == "account_id" and value not in accounts:
            accounts[value] = f"placeholder-account-{len(accounts) + 1}"
        elif key in {"display_name", "nickname"} and value not in names:
            names[value] = f"placeholder-user-{len(names) + 1}"
    for _key, value in _strings(ordered):
        for found in _UUID.findall(value):
            lowered = found.lower()
            if not _PLACEHOLDER_UUID.fullmatch(lowered) and lowered not in uuids:
                uuids[lowered] = f"00000000-0000-4000-8000-{len(uuids) + 1:012d}"
    replacements: list[tuple[re.Pattern[str], str]] = []
    # Workspace and slug first: a team workspace's owner display name is its slug, so
    # the name patterns must not rewrite URL segments before these run.
    for form in dict.fromkeys(
        (f"{workspace}/{slug}", f"{quote(workspace, safe='')}/{quote(slug, safe='')}")
    ):
        replacements.append((_bounded(form), f"{PLACEHOLDER_WORKSPACE}/{PLACEHOLDER_SLUG}"))
    replacements.append((_bounded(workspace), PLACEHOLDER_WORKSPACE))
    replacements.append((_bounded(slug), PLACEHOLDER_SLUG))
    for original, placeholder in [*accounts.items(), *names.items()]:
        for form in dict.fromkeys((original, quote(original, safe=""))):
            replacements.append((_bounded(form), placeholder))

    def host(url: re.Match[str]) -> str:
        parts = urlsplit(url[0])
        if parts.hostname in {HOST, API_HOST}:
            return url[0]
        if "avatar" in url[0]:
            return PLACEHOLDER_AVATAR
        return urlunsplit((parts.scheme, "example.invalid", parts.path, parts.query, ""))

    def visit(key: str | None, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        if key == "account_id":
            return accounts.get(value, value)
        if key in {"display_name", "nickname"}:
            return names.get(value, value)
        for pattern, placeholder in replacements:
            value = pattern.sub(placeholder, value)
        value = _UUID.sub(lambda m: uuids.get(m[0].lower(), m[0]), value)
        value = _EMAIL.sub(PLACEHOLDER_EMAIL, value)
        return _URL.sub(host, value)

    scrubbed = _walk(ordered, visit)
    originals = {
        *accounts,
        *names,
        *uuids,
        *(u.upper() for u in uuids),
        *(e for _, v in _strings(ordered) for e in _EMAIL.findall(v)),
    }
    originals -= {PLACEHOLDER_EMAIL, "recorder@example.invalid"}
    return {"schema": document["schema"], **scrubbed}, originals


def _placeholder_id(segment: str) -> bool:
    bare = segment.replace("%7B", "").replace("%7D", "").strip("{}")
    return bool(_PLACEHOLDER_UUID.fullmatch(bare.lower()))


def _segment_finding(where: str, prefix: str, first: str, second: str | None) -> str | None:
    second = second.removesuffix(".git") if second is not None else None
    if prefix == "/workspaces/":
        allowed = first == PLACEHOLDER_WORKSPACE or _placeholder_id(first)
    elif prefix == "/repositories/":
        if first in {"{}", "%7B%7D"}:
            allowed = second is None or _placeholder_id(second)
        else:
            allowed = first == PLACEHOLDER_WORKSPACE and second in {None, PLACEHOLDER_SLUG}
    else:
        allowed = (
            first in BITBUCKET_ROUTES
            or _placeholder_id(first)
            or (
                first == PLACEHOLDER_WORKSPACE
                and second in {None, PLACEHOLDER_SLUG, WORKSPACE_ROUTE}
            )
            or (
                first == "workspaces"
                and second is not None
                and (second == PLACEHOLDER_WORKSPACE or _placeholder_id(second))
            )
        )
    return None if allowed else f"{where}: repository segment {first[:3]}..."


def scan_recording(document: Any, real_values: Iterable[str] = ()) -> list[str]:
    """Everything in a recording that is not a placeholder; empty when clean.

    A finding names where it is and shows at most a short prefix of a value.
    """

    findings: list[str] = []

    def walk(node: Any, where: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                here = f"{where}.{key}"
                if str(key).lower() in CREDENTIAL_HEADERS:
                    findings.append(f"{here}: credential header")
                if key == "account_id" and not (
                    isinstance(value, str) and _PLACEHOLDER_ACCOUNT.fullmatch(value)
                ):
                    findings.append(f"{here}: account_id is not a placeholder")
                walk(value, here)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{where}[{index}]")
        elif isinstance(node, str):
            for found in _UUID.findall(node):
                if not _PLACEHOLDER_UUID.fullmatch(found):
                    findings.append(f"{where}: uuid {found[:4]}... is not a placeholder")
            for prefix, first, second in _REPO_SEGMENTS.findall(node):
                finding = _segment_finding(where, prefix, first, second or None)
                if finding is not None:
                    findings.append(finding)
            if _TOKEN.search(node):
                findings.append(f"{where}: Bitbucket token")

    walk(document, "$")
    text = json.dumps(document)
    for index, value in enumerate(v for v in real_values if v):
        forms = dict.fromkeys((value, json.dumps(value)[1:-1], quote(value, safe="")))
        if any(_bounded(form).search(text) for form in forms):
            findings.append(f"real value #{index} is still present")
    return findings
