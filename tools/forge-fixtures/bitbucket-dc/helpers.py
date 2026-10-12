"""Test-side helpers for a running Bitbucket DC fixture (#4401 decision 14.10).

Load by path (``importlib.util.spec_from_file_location``) and pass the dict
``load_state`` returns. Stdlib only. Users act with basic auth from their
password files; ``admin`` merges and declines.
"""

from __future__ import annotations

import base64
import json
import os
import ssl
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SCHEMA = "curie.forge-fixture.bitbucket-dc/v1"


def load_state(path: str | os.PathLike[str]) -> dict[str, Any]:
    """The fixture's ``state.json``; refuses a skeleton left by a failed ``up``."""
    state: dict[str, Any] = json.loads(Path(path).read_text())
    if state.get("schema") != SCHEMA or "token_file" not in state:
        raise ValueError(f"{path} is not a finished {SCHEMA} state file")
    return state


def _password_file(state: dict[str, Any], user: str) -> str:
    if user == state["admin"]["username"]:
        return str(state["admin"]["password_file"])
    return str(state["users"][user]["password_file"])


def _auth(state: dict[str, Any], user: str) -> str:
    password = Path(_password_file(state, user)).read_text().strip()
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def _repo_api(state: dict[str, Any]) -> str:
    return (
        f"{state['base_url']}/rest/api/latest/projects/"
        f"{state['project_key']}/repos/{state['repo_slug']}"
    )


def _call(
    state: dict[str, Any],
    user: str,
    method: str,
    url: str,
    body: Any = None,
    query: dict[str, str] | None = None,
) -> Any:
    if query:
        url += "?" + urllib.parse.urlencode(query)
    headers = {
        "Authorization": _auth(state, user),
        "Accept": "application/json",
        "X-Atlassian-Token": "no-check",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    context = ssl.create_default_context(cafile=state["ca_file"])
    with urllib.request.urlopen(request, context=context, timeout=30) as response:
        raw = response.read()
    return json.loads(raw) if raw else None


def post_build_status(
    state: dict[str, Any], sha: str, key: str, state_word: str, *, as_user: str = "admin"
) -> None:
    """Post a build status (``SUCCESSFUL``, ``FAILED``, ``INPROGRESS``...) on ``sha``."""
    body = {
        "key": key,
        "name": key,
        "state": state_word,
        "url": f"https://example.invalid/builds/{key}",
    }
    _call(state, as_user, "POST", f"{_repo_api(state)}/commits/{sha}/builds", body)


def comment(
    state: dict[str, Any],
    user: str,
    pr_id: int,
    text: str,
    *,
    anchor: dict[str, Any] | None = None,
    parent: int | None = None,
) -> int:
    """Comment on a pull request as ``reviewer``, ``outsider`` or ``admin``; returns its id.

    ``anchor`` makes it inline, for example ``{"path": "README.md", "line": 1,
    "lineType": "ADDED", "fileType": "TO", "diffType": "EFFECTIVE"}``;
    ``parent`` makes it a reply in that comment's thread.
    """
    body: dict[str, Any] = {"text": text}
    if anchor is not None:
        body["anchor"] = anchor
    if parent is not None:
        body["parent"] = {"id": parent}
    url = f"{_repo_api(state)}/pull-requests/{pr_id}/comments"
    return int(_call(state, user, "POST", url, body)["id"])


def pull_request(state: dict[str, Any], pr_id: int) -> dict[str, Any]:
    result: dict[str, Any] = _call(
        state, "admin", "GET", f"{_repo_api(state)}/pull-requests/{pr_id}"
    )
    return result


def _transition(state: dict[str, Any], pr_id: int, action: str) -> dict[str, Any]:
    version = str(pull_request(state, pr_id)["version"])
    url = f"{_repo_api(state)}/pull-requests/{pr_id}/{action}"
    result: dict[str, Any] = _call(state, "admin", "POST", url, {}, {"version": version})
    return result


def merge(state: dict[str, Any], pr_id: int) -> dict[str, Any]:
    """Merge as the admin at the pull request's current version."""
    return _transition(state, pr_id, "merge")


def decline(state: dict[str, Any], pr_id: int) -> dict[str, Any]:
    """Decline as the admin at the pull request's current version."""
    return _transition(state, pr_id, "decline")


def git_env(state: dict[str, Any], token: str) -> dict[str, str]:
    """``GIT_CONFIG_*`` for git over HTTPS: the fixture CA and the token's Bearer header.

    Nothing is written to a git config file. Merge into ``os.environ``.
    """
    return {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "http.sslCAInfo",
        "GIT_CONFIG_VALUE_0": str(state["ca_file"]),
        "GIT_CONFIG_KEY_1": "http.extraHeader",
        "GIT_CONFIG_VALUE_1": f"Authorization: Bearer {token}",
    }
