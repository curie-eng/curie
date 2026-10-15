"""The Jira Cloud entry of the forge-neutral factory loop: JiraCloudDriver.

`run --scenario forge-loop --forge jira_cloud` drives the `jira_cloud+github`
pairing. The release is installed exactly as for `github+github`; then this
entry's setup, ``install_jira``, runs `curie factory quickstart --tracker
jira-cloud --code-host github` in add agent mode against it, in place of the
GitHub agent binding. The ticket is a Jira issue the human labeler opens and
labels; the pull request, CI, review and merge steps are the GitHub driver's.

Site facts come only from the env file named by CURIE_FACTORY_JIRA_ENV_FILE,
one ``key=value`` per line:

1. ``site``: the bare Jira Cloud host (``<name>.atlassian.net``).
2. ``cloud_id``: the site's cloud id.
3. ``project_key``: the project test issues are opened in.
4. ``labeler_email`` and ``labeler_token_file``: the human account allowed to
   start work, and a file holding its API token.
5. ``bot_email``, ``bot_account_id`` and ``bot_token_file``: the factory
   account the quickstart installs, and a file holding its API token.

Every Jira REST call goes through the API gateway,
``https://api.atlassian.com/ex/jira/<cloud_id>/rest/api/3``, so the test
accounts' tokens are scoped API tokens; the site host is used only for browse
links. A token is read from its file for each request and goes only into that
request's Basic Authorization header: never into argv, evidence, logs or an
error message.

Tracker evidence, read once the merge is observed: the factory account's
status comments on the ticket (those carrying the ``curie.factory`` comment
property), the remote links to the pull request, whether the pull request
body names the key and its browse URL, and whether a second issue labelled by
the factory account itself admitted a WorkItem (it must not). Every issue the
driver opened is closed and unlabelled by its own teardown step, "close Jira
test issues".
"""

from __future__ import annotations

import base64
import dataclasses
import json
import os
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import factory_e2e as fe

ENV_FILE_VAR = "CURIE_FACTORY_JIRA_ENV_FILE"
SITE_KEYS = (
    "site",
    "cloud_id",
    "project_key",
    "labeler_email",
    "labeler_token_file",
    "bot_email",
    "bot_account_id",
    "bot_token_file",
)
CLOSE_STEP = "close Jira test issues"
# The comment property the factory's status comment carries.
STATUS_PROPERTY = "curie.factory"
# Comment paging: the issue listing's page size and the bulk read's id cap.
COMMENT_PAGE_SIZE = 100
COMMENT_LIST_IDS_LIMIT = 1000
MAX_COMMENT_PAGES = 1000
# The tracker label `curie factory quickstart --tracker jira-cloud` writes.
QUICKSTART_LABEL = "curie-factory"
# The tracker poll interval the quickstart leaves at its default; the
# bot-label probe waits three of them for an admission that must not come.
TRACKER_POLL_SECONDS = 45
BOT_LABEL_PROBE_SECONDS = 3 * TRACKER_POLL_SECONDS
ADMISSION_POLL_SECONDS = 5
BOT_PROBE_TITLE = "factory-e2e: a label from the factory account starts nothing"
BOT_PROBE_BODY = "The factory account labels this issue. No WorkItem may be admitted for it."


@dataclasses.dataclass(frozen=True)
class JiraSite:
    """The test site's facts. Token files only: no token is held here."""

    site: str
    cloud_id: str
    project_key: str
    labeler_email: str
    labeler_token_file: Path
    bot_email: str
    bot_account_id: str
    bot_token_file: Path


def load_site(environ: Mapping[str, str]) -> JiraSite:
    """The site facts from the env file CURIE_FACTORY_JIRA_ENV_FILE names.

    A token file path is read relative to the env file's own directory (an
    absolute path stays as written). A missing variable, file or key is a
    ConfigError naming the variable or the key, never a value.
    """

    named = environ.get(ENV_FILE_VAR)
    if not named:
        raise fe.ConfigError(f"{ENV_FILE_VAR} must name the Jira test site env file")
    env_file = Path(named)
    try:
        text = env_file.read_text()
    except OSError as exc:
        raise fe.ConfigError(f"{ENV_FILE_VAR} names an unreadable file: {exc.strerror}") from None
    facts: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        facts[key.strip()] = value.strip()
    missing = [key for key in SITE_KEYS if not facts.get(key)]
    if missing:
        raise fe.ConfigError(f"the {ENV_FILE_VAR} file sets no {', '.join(missing)}")
    if "/" in facts["site"] or ":" in facts["site"]:
        raise fe.ConfigError(f"the {ENV_FILE_VAR} file's site must be a bare host name")
    return JiraSite(
        site=facts["site"],
        cloud_id=facts["cloud_id"],
        project_key=facts["project_key"],
        labeler_email=facts["labeler_email"],
        labeler_token_file=env_file.parent / facts["labeler_token_file"],
        bot_email=facts["bot_email"],
        bot_account_id=facts["bot_account_id"],
        bot_token_file=env_file.parent / facts["bot_token_file"],
    )


# --- pure helpers over Jira REST v3 responses -----------------------------------------


def bot_comment_count(comments: Mapping[str, Any], bot_account_id: str) -> int:
    """Comments authored by the bot that carry the factory status property. Pure."""

    count = 0
    for comment in comments.get("comments") or []:
        if not isinstance(comment, dict):
            continue
        author = comment.get("author") or {}
        props = comment.get("properties") or []
        if author.get("accountId") == bot_account_id and any(
            isinstance(prop, dict) and prop.get("key") == STATUS_PROPERTY for prop in props
        ):
            count += 1
    return count


def remote_link_count(links: Iterable[Mapping[str, Any]], url: str) -> int:
    """Remote links whose globalId is the pull request URL. Pure."""

    return sum(1 for link in links if isinstance(link, Mapping) and link.get("globalId") == url)


def done_transition(transitions: Mapping[str, Any]) -> str | None:
    """The id of the transition into the done status category, or None. Pure."""

    for transition in transitions.get("transitions") or []:
        category = ((transition.get("to") or {}).get("statusCategory") or {}).get("key")
        if category == "done":
            return str(transition.get("id"))
    return None


def jira_label_update(*, add: Iterable[str] = (), remove: Iterable[str] = ()) -> dict[str, Any]:
    """An issue edit body adding and removing labels in one update. Pure."""

    ops = [{"add": label} for label in add] + [{"remove": label} for label in remove]
    return {"update": {"labels": ops}}


def _token(site: JiraSite, actor: str) -> str:
    key = "labeler_token_file" if actor == "labeler" else "bot_token_file"
    path = site.labeler_token_file if actor == "labeler" else site.bot_token_file
    try:
        token = path.read_text().strip()
    except OSError as exc:
        raise fe.ConfigError(f"the Jira {key} is unreadable: {exc.strerror}") from None
    if not token:
        raise fe.ConfigError(f"the Jira {key} is empty")
    return token


def gateway_url(site: JiraSite) -> str:
    """The Atlassian API gateway for the site. A scoped API token authenticates
    only here; the site URL answers it 401."""

    return f"https://api.atlassian.com/ex/jira/{site.cloud_id}"


def jira_request(
    site: JiraSite, actor: str, method: str, path: str, body: Any = None
) -> tuple[int, Any]:
    """One Jira REST v3 call as ``labeler`` or ``bot``, Basic auth from its token file."""

    email = site.labeler_email if actor == "labeler" else site.bot_email
    basic = base64.b64encode(f"{email}:{_token(site, actor)}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}", "Accept": "application/json"}
    url = f"{gateway_url(site)}/rest/api/3{path}"
    return fe.http_json(method, url, headers=headers, body=body)


def _recorded_code_host_secret(values: Mapping[str, Any]) -> str | None:
    factory = values.get("factory") if isinstance(values, Mapping) else None
    for entry in (factory or {}).get("codeHosts") or []:
        if isinstance(entry, dict) and entry.get("kind") == "github":
            secret = (entry.get("credential") or {}).get("existingSecret")
            return secret if isinstance(secret, str) and secret else None
    return None


# --- the driver -------------------------------------------------------------------------


class JiraCloudDriver:
    """ForgeDriver (forges/base.py) for a Jira Cloud tracker with a GitHub code host."""

    forge = "jira_cloud"
    pairing = "jira_cloud+github"

    def __init__(self, p: fe.Preflight, *, github: Any) -> None:
        self.p = p
        self.github = github
        self.site = load_site(os.environ)
        # (id, key) of every issue this driver created, appended before labelling.
        self.created: list[tuple[str, str]] = []
        self.key: str | None = None

    def __repr__(self) -> str:
        return f"JiraCloudDriver(project={self.site.project_key}, created={len(self.created)})"

    # -- Jira plumbing --

    def _jira(self, actor: str, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return jira_request(self.site, actor, method, path, body)

    def _create_issue(self, title: str, body: str) -> tuple[str, str]:
        status, created = self._jira(
            "labeler",
            "POST",
            "/issue",
            {
                "fields": {
                    "project": {"key": self.site.project_key},
                    "issuetype": {"name": "Task"},
                    "summary": title,
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {"type": "paragraph", "content": [{"type": "text", "text": body}]}
                        ],
                    },
                }
            },
        )
        if status != 201 or not isinstance(created, dict) or not created.get("id"):
            raise fe.PreflightFailed(f"creating the Jira test issue failed (HTTP {status})")
        issue = (str(created["id"]), str(created.get("key")))
        self.created.append(issue)
        return issue

    def _label(self, actor: str, issue_id: str) -> None:
        status, _ = self._jira(
            actor, "PUT", f"/issue/{issue_id}", jira_label_update(add=[self.p.config.label])
        )
        if status != 204:
            raise fe.PreflightFailed(
                f"labelling Jira issue {issue_id} as the {actor} failed (HTTP {status})"
            )

    def _work_items_for(self, issue_id: str) -> list[dict[str, Any]] | None:
        """WorkItems whose tracker issue is ``issue_id``; None when unreadable."""

        status, body = self.p.api("GET", "/work-items", headers={"X-API-Key": self.p.api_key})
        if status != 200 or not isinstance(body, dict):
            return None
        return [
            item
            for item in body.get("items") or []
            if isinstance(item, dict)
            and (item.get("tracker") or {}).get("kind") == "jira_cloud"
            and str((item.get("tracker") or {}).get("issue_id")) == issue_id
        ]

    # -- the ticket --

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        issue_id, key = self._create_issue(title, body)
        self.key = key
        self.p.evidence["jira_issue_key"] = key
        self._label("labeler", issue_id)
        self.p.step("Jira issue opened and labelled by the labeler", key=key)

        def admitted() -> list[dict[str, Any]] | None:
            return self._work_items_for(issue_id) or None

        items = fe._wait(
            f"a WorkItem for Jira issue {key}",
            self.p.admission_timeout,
            admitted,
            ADMISSION_POLL_SECONDS,
        )
        if len(items) != 1:
            raise fe.PreflightFailed(
                f"Jira issue {key} admitted {len(items)} WorkItems, expected 1"
            )
        work_item_id = str(items[0]["id"])
        self.p.evidence["work_item_id"] = work_item_id
        self.p.step("WorkItem admitted", work_item_id=work_item_id, key=key)
        return work_item_id

    # -- the change, CI, review and merge are GitHub's --

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        return self.github.await_change(work_item_id)  # type: ignore[no-any-return]

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        return self.github.change_head(change)  # type: ignore[no-any-return]

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        return self.github.set_ci_red(sha)  # type: ignore[no-any-return]

    def ci_state(self, sha: str) -> dict[str, Any]:
        return self.github.ci_state(sha)  # type: ignore[no-any-return]

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        return self.github.post_review(change, text)  # type: ignore[no-any-return]

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        return self.github.merge(change)  # type: ignore[no-any-return]

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        return self.github.change_merged(change)  # type: ignore[no-any-return]

    def changes_opened(self) -> list[int]:
        return self.github.changes_opened()  # type: ignore[no-any-return]

    def default_branch_head(self) -> str:
        return self.github.default_branch_head()  # type: ignore[no-any-return]

    def reset_fixture(self) -> dict[str, Any]:
        return self.github.reset_fixture()  # type: ignore[no-any-return]

    def pin_fixture_base(self) -> None:
        self.github.pin_fixture_base()

    # -- tracker evidence --

    def _status_comments(self, key: str) -> int | None:
        """The bot's status comments on ``key``, read the way Jira Cloud REST v3 allows.

        The issue-comments listing does not expand entity properties, so this
        lists the comment ids page by page, then reads the bot's comments with
        their properties through the bulk endpoint, as the adapter does
        (apps/api/src/curie_api/forges/jira_cloud/comments.py). See
        https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/
        """

        ids = self._bot_comment_ids(key)
        if ids is None:
            return None
        listed: list[dict[str, Any]] = []
        for start in range(0, len(ids), COMMENT_LIST_IDS_LIMIT):
            page = self._comments_with_properties(ids[start : start + COMMENT_LIST_IDS_LIMIT])
            if page is None:
                return None
            listed.extend(page)
        return bot_comment_count({"comments": listed}, self.site.bot_account_id)

    def _bot_comment_ids(self, key: str) -> list[int] | None:
        """Ids of the bot's comments on ``key`` from every page of the listing."""

        ids: list[int] = []
        start = 0
        for _ in range(MAX_COMMENT_PAGES):
            status, page = self._jira(
                "labeler",
                "GET",
                f"/issue/{key}/comment?startAt={start}&maxResults={COMMENT_PAGE_SIZE}",
            )
            comments = page.get("comments") if isinstance(page, dict) else None
            if status != 200 or not isinstance(comments, list):
                return None
            for comment in comments:
                if not isinstance(comment, dict):
                    continue
                author = comment.get("author") or {}
                comment_id = str(comment.get("id"))
                if author.get("accountId") == self.site.bot_account_id and comment_id.isdigit():
                    ids.append(int(comment_id))
            start += len(comments)
            total = page.get("total")
            if not comments or not isinstance(total, int) or start >= total:
                return ids
        return None

    def _comments_with_properties(self, ids: list[int]) -> list[dict[str, Any]] | None:
        """The comments ``ids`` with their entity properties, every page, or None."""

        listed: list[dict[str, Any]] = []
        start = 0
        for _ in range(MAX_COMMENT_PAGES):
            status, page = self._jira(
                "labeler",
                "POST",
                f"/comment/list?expand=properties&startAt={start}&maxResults={len(ids)}",
                {"ids": ids},
            )
            values = page.get("values") if isinstance(page, dict) else None
            if status != 200 or not isinstance(values, list):
                return None
            listed.extend(value for value in values if isinstance(value, dict))
            if page.get("isLast") is True:
                return listed
            if not values:
                return None
            start += len(values)
        return None

    def _remote_links(self, key: str, change: fe.ChangeRef | None) -> int | None:
        if change is None:
            return 0
        status, body = self._jira("labeler", "GET", f"/issue/{key}/remotelink")
        if status != 200 or not isinstance(body, list):
            return None
        return remote_link_count(body, change.url)

    def _pr_body_has_key(self, key: str, change: fe.ChangeRef | None) -> bool:
        if change is None:
            return False
        status, pr = self.p.as_actor("GET", f"/repos/{self.p.config.repo}/pulls/{change.number}")
        text = pr.get("body") if status == 200 and isinstance(pr, dict) else None
        if not isinstance(text, str):
            return False
        return key in text and f"https://{self.site.site}/browse/{key}" in text

    def _bot_label_admitted(self) -> bool | None:
        """Label a second issue as the bot; True if it admits a WorkItem in time."""

        issue_id, key = self._create_issue(BOT_PROBE_TITLE, BOT_PROBE_BODY)
        self._label("bot", issue_id)
        self.p.step("Jira issue labelled by the factory account", key=key)
        deadline = time.monotonic() + BOT_LABEL_PROBE_SECONDS
        read = False
        while True:
            items = self._work_items_for(issue_id)
            if items:
                return True
            read = read or items is not None
            if time.monotonic() > deadline:
                return False if read else None
            time.sleep(fe.POLL_SECONDS)

    def tracker_evidence(self, change: fe.ChangeRef | None, work_item_id: str) -> dict[str, Any]:
        key = self.key or ""
        return {
            "key": key,
            "status_comments": self._status_comments(key),
            "remote_links": self._remote_links(key, change),
            "pr_body_has_key": self._pr_body_has_key(key, change),
            "bot_label_admitted": self._bot_label_admitted(),
        }

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        failures: list[str] = []
        comments = evidence.get("status_comments")
        if comments != 1:
            failures.append(f"the ticket has {comments} factory status comments, expected 1")
        links = evidence.get("remote_links")
        if links != 1:
            failures.append(f"the ticket has {links} remote links to the pull request, expected 1")
        if evidence.get("pr_body_has_key") is not True:
            failures.append("the pull request body does not name the Jira key and its browse URL")
        admitted = evidence.get("bot_label_admitted")
        if admitted is not False:
            failures.append(
                f"a label from the factory account admitted a WorkItem: {admitted!r}, "
                "expected False"
            )
        return failures

    # -- teardown --

    def teardowns(self) -> tuple[tuple[str, Callable[[], Any]], ...]:
        return ((CLOSE_STEP, self.close_test_issues),)

    def _close(self, issue_id: str) -> None:
        status, issue = self._jira("labeler", "GET", f"/issue/{issue_id}?fields=labels,status")
        if status != 200 or not isinstance(issue, dict):
            raise fe.PreflightFailed(f"reading issue {issue_id} failed (HTTP {status})")
        fields = issue.get("fields") or {}
        # The trigger label and every factory state label (curie-factory:*) the
        # run applied: the issue was created by this driver, so it keeps none.
        labels = [label for label in fields.get("labels") or [] if isinstance(label, str)]
        if labels:
            status, _ = self._jira(
                "labeler", "PUT", f"/issue/{issue_id}", jira_label_update(remove=labels)
            )
            if status != 204:
                raise fe.PreflightFailed(f"unlabelling issue {issue_id} failed (HTTP {status})")
        if ((fields.get("status") or {}).get("statusCategory") or {}).get("key") == "done":
            return
        status, transitions = self._jira("labeler", "GET", f"/issue/{issue_id}/transitions")
        wanted = done_transition(transitions) if isinstance(transitions, dict) else None
        if status != 200 or wanted is None:
            raise fe.PreflightFailed(f"issue {issue_id} offers no done transition (HTTP {status})")
        status, _ = self._jira(
            "labeler", "POST", f"/issue/{issue_id}/transitions", {"transition": {"id": wanted}}
        )
        if status != 204:
            raise fe.PreflightFailed(f"closing issue {issue_id} failed (HTTP {status})")

    def close_test_issues(self) -> dict[str, Any]:
        """Close and unlabel every issue this driver created; raise if any failed."""

        closed: list[str] = []
        failed: list[str] = []
        for issue_id, key in self.created:
            try:
                self._close(issue_id)
            except (fe.PreflightFailed, fe.ConfigError, OSError) as exc:
                fe.log(f"closing Jira issue {key} failed: {exc}")
                failed.append(key)
            else:
                closed.append(key)
        if failed:
            raise fe.PreflightFailed(
                f"could not close Jira test issues {', '.join(failed)}; closed {closed}"
            )
        return {"closed": closed, "failed": failed}


# --- the entry's setup: the quickstart in add agent mode ------------------------------


def _labeler_account_id(site: JiraSite) -> str:
    status, me = jira_request(site, "labeler", "GET", "/myself")
    account = me.get("accountId") if status == 200 and isinstance(me, dict) else None
    if not isinstance(account, str) or not account:
        raise fe.PreflightFailed(f"reading the labeler's Jira account failed (HTTP {status})")
    return account


def install_jira(p: fe.Preflight) -> None:
    """Bind the factory agent to the Jira tracker with `curie factory quickstart`.

    The release must already record the GitHub factory intake (the code host
    credential's existingSecret), so the quickstart takes its add agent path;
    otherwise this refuses before running anything.
    """

    raw = fe.run(
        [
            "helm",
            "--kube-context",
            p.config.kube_context,
            "get",
            "values",
            p.release,
            "-n",
            p.namespace,
            "-o",
            "json",
        ]
    )
    try:
        values = json.loads(raw) if raw.strip() else {}
    except ValueError as exc:
        raise fe.PreflightFailed("helm get values did not print JSON") from exc
    if _recorded_code_host_secret(values) is None:
        raise fe.PreflightFailed(
            f"release {p.release} records no factory intake: the github factory.codeHosts "
            "credential has no existingSecret, so the quickstart would install, not add an agent"
        )
    if p.config.label != QUICKSTART_LABEL:
        raise fe.ConfigError(
            f"the Jira tracker label is {QUICKSTART_LABEL}; unset CURIE_FACTORY_LABEL for "
            "a jira_cloud run"
        )
    site = load_site(os.environ)
    start_account = _labeler_account_id(site)
    assert p.chart_dir is not None, "install() extracts the chart first"
    bundle = p.build_runner_layer(p.config.bundle_dir)
    argv = [
        p.config.curie_bin,
        "factory",
        "quickstart",
        "--context",
        p.config.kube_context,
        "--namespace",
        p.namespace,
        "--release",
        p.release,
        "--agent",
        fe.FACTORY_AGENT,
        "--plugin-dir",
        str(bundle),
        "--chart",
        str(p.chart_dir),
        "--repo",
        p.config.repo,
        "--execution-deadline",
        str(fe.EXECUTION_BOUND_SECONDS),
        "--tracker",
        "jira-cloud",
        "--code-host",
        "github",
        "--forge-url",
        gateway_url(site),
        "--token-file",
        str(site.bot_token_file),
        "--forge-username",
        site.bot_email,
        "--start-account",
        start_account,
    ]
    env = {
        **os.environ,
        "KUBECONFIG": str(p._write_kubeconfig()),
        "CURIE_API_KEY": p.api_key,
        "CURIE_CONFIG_DIR": str(p._curie_config_dir()),
    }
    fe.log("curie factory quickstart --tracker jira-cloud (add the factory agent)")
    result = subprocess.run(argv, capture_output=True, text=True, env=env, check=False)
    if result.returncode != 0:
        secrets = [
            p.api_key,
            p.config.model_api_key,
            p.worker_token,
            _token(site, "labeler"),
            _token(site, "bot"),
        ]
        tail = fe._redact((result.stderr or result.stdout).strip(), secrets)[-1500:]
        raise fe.PreflightFailed(f"curie factory quickstart --tracker jira-cloud failed: {tail}")
    p.evidence["jira"] = {"project_key": site.project_key, "binding": "jira"}
    p.step("factory agent bound to the Jira tracker", agent=fe.FACTORY_AGENT)


def _driver(p: fe.Preflight) -> JiraCloudDriver:
    fe.load_forge_entries()
    return JiraCloudDriver(p, github=fe.FORGES["github"].driver(p))


fe.register_forge(
    fe.ForgeEntry(name="jira_cloud", driver=_driver, installed=True, setup=install_jira)
)
