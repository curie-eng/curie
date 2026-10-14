"""Binding writes carry a trigger label (#4361 AC 3, decisions 3 to 7, as amended
by #4397 decisions 2 and 10).

A tracker surface (``github``, ``gitlab``, ``jira``) may carry an optional
``trigger_label``. A surface with no label serves the configured tracker
``label``; that is its effective label. On one tracker location the effective
labels are pairwise distinct and an agent holds at most one surface, so two
agents may share a repository only under different labels. Every refusal is a
409 naming the other agent (or a 422 for a malformed write), and every refusal
here has a liveness neighbour that the same write, made valid, succeeds.

Everything drives the real ``/agents`` routes against the real Postgres. The
concurrency tests race two app instances on one database, each request on its
own thread under ``asyncio.gather``, the shape
``test_agent_channels_subresource.py`` established: one TestClient's portal
would serialize the very overlap under test.

The install's tracker label is ``curie-factory`` (``factory_env``), so a NULL
label row's effective label is ``curie-factory``.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Callable, Iterator
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes import config_samples as samples  # noqa: E402
from forge_fakes.github import factory_env, forges_document, forges_env_json  # noqa: E402

INSTALL = "curie-factory"
FAST = "curie-factory-fast"
REPO = "acme-corp/acme-bot"
OTHER_REPO = "acme-corp/widget-svc"
GITLAB_PATH = "acme/platform/infra"
GITLAB_DEEP_PATH = "acme/platform/team/infra"
JIRA_BINDING = "acme-jira"


# --- fixtures -----------------------------------------------------------------


def _apply_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


@pytest.fixture
def github_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None
) -> Iterator[tuple[TestClient, TestClient]]:
    """Two app instances on one database, both on a GitHub-only forge config
    whose tracker label is ``curie-factory``."""

    _apply_env(monkeypatch, factory_env(tmp_path, label=INSTALL))
    try:
        with TestClient(create_app()) as client, TestClient(create_app()) as rival:
            yield client, rival
    finally:
        get_settings.cache_clear()


class _FakeConnection:
    """A connection the registry can build for a kind a test installs. As a
    tracker it is binding scoped, as every non-GitHub kind must be."""

    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        self.config = config

    def for_binding(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("surface resolution makes no tracker")

    def issue_url(self, *args: Any, **kwargs: Any) -> str:
        raise AssertionError("surface resolution makes no tracker")


def _kind_table(registry: ModuleType) -> dict[str, Any]:
    """The registry's kind table, found by what it holds rather than its name:
    the one module-level dict that registers both ``github`` and ``gitlab``."""

    tables = [
        value
        for value in vars(registry).values()
        if isinstance(value, dict) and "github" in value and "gitlab" in value
    ]
    assert len(tables) == 1, "forges.registry must keep one kind table"
    return tables[0]


def _gitlab_and_jira_document() -> dict[str, Any]:
    """Today's GitHub entries plus a GitLab code host and tracker with two
    native bindings (one nested four deep) and a Jira site bound to a GitLab
    project. Shapes follow ``forge_fakes.config_samples``."""

    document = forges_document(label=INSTALL)
    document["code_hosts"].append(samples.gitlab_code_host("gitlab"))
    document["trackers"].append(
        {
            "name": "gitlab-issues",
            "kind": "gitlab",
            "host": "gitlab.example",
            "api_url": "https://gitlab.example/api/v4",
            "credential": samples.static_token(),
            "label": INSTALL,
            "mention": "curie-bot",
        }
    )
    document["trackers"].append({**samples.jira_tracker("jira"), "label": INSTALL})
    for name, project_id, path in (
        ("infra", "4242", GITLAB_PATH),
        ("team-infra", "4244", GITLAB_DEEP_PATH),
    ):
        document["bindings"].append(
            {
                "name": name,
                "tracker": "gitlab-issues",
                "tracker_scope_id": project_id,
                "repos": [
                    {
                        "alias": "infra",
                        "code_host": "gitlab",
                        "project_id": project_id,
                        "path": path,
                    }
                ],
            }
        )
    document["bindings"].append(
        {
            "name": JIRA_BINDING,
            "tracker": "jira",
            "tracker_scope_id": samples.JIRA_SITE_ID,
            "repos": [
                {
                    "alias": "platform",
                    "code_host": "gitlab",
                    "project_id": "4243",
                    "path": "acme/platform/web",
                }
            ],
            "default_repo": "platform",
            "start_authority": {"type": "allowlist", "accounts": [samples.JIRA_STARTER]},
        }
    )
    return document


@pytest.fixture
def tracker_kinds_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None
) -> Iterator[TestClient]:
    """An install with a configured GitLab project and Jira binding, where
    ``gitlab`` and ``jira_cloud`` are registered with connections that make no
    request, for this test only.

    Surface resolution reads only config, so the shipped adapters (#4358, #4399)
    are replaced by stand-ins that fail if a binding write ever builds a
    tracker (plan Open question 8)."""

    from curie_api.forges import registry

    table = _kind_table(registry)
    for kind in ("gitlab", "jira_cloud"):
        monkeypatch.delitem(table, kind)
        registry.register(kind, tracker=_FakeConnection, code_host=_FakeConnection)
    env = factory_env(tmp_path, label=INSTALL)
    env["CURIE_FORGES"] = forges_env_json(_gitlab_and_jira_document(), tmp_path / "keys")
    _apply_env(monkeypatch, env)
    try:
        with TestClient(create_app()) as client:
            yield client
    finally:
        get_settings.cache_clear()


# --- helpers ------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _github(address: str = REPO, label: str | None = None) -> dict[str, Any]:
    channel: dict[str, Any] = {"kind": "github", "address": address}
    if label is not None:
        channel["trigger_label"] = label
    return channel


def _create(client: TestClient, channel: dict[str, Any]) -> Any:
    name = f"acme-factory-{uuid.uuid4().hex[:8]}"
    return client.post(
        "/agents",
        headers=_headers(),
        json={"name": name, "repo_full_name": "acme-corp/factory-home", "channel": channel},
    )


def _agent(client: TestClient, channel: dict[str, Any]) -> tuple[str, str]:
    """A created agent's (id, name)."""

    created = _create(client, channel)
    assert created.status_code == 201, created.text
    body = created.json()
    return str(body["id"]), str(body["name"])


def _slack_agent(client: TestClient) -> tuple[str, str]:
    return _agent(client, {"kind": "slack", "address": f"C0{uuid.uuid4().hex[:8].upper()}"})


def _add(client: TestClient, agent_id: str, channel: dict[str, Any]) -> Any:
    return client.post(f"/agents/{agent_id}/channels", headers=_headers(), json=channel)


def _move(client: TestClient, agent_id: str, kind: str, address: str, body: dict[str, Any]) -> Any:
    return client.patch(
        f"/agents/{agent_id}/channels",
        headers=_headers(),
        params={"kind": kind, "address": address},
        json=body,
    )


def _channels(client: TestClient, agent_id: str) -> list[dict[str, Any]]:
    read = client.get(f"/agents/{agent_id}", headers=_headers())
    assert read.status_code == 200, read.text
    channels: list[dict[str, Any]] = read.json()["channels"]
    return channels


def _github_channels(client: TestClient, agent_id: str) -> list[tuple[str, str | None]]:
    return sorted(
        (channel["address"], channel["trigger_label"])
        for channel in _channels(client, agent_id)
        if channel["kind"] == "github"
    )


def _detail(response: Any) -> str:
    return str(response.json().get("detail"))


def _names_agent(response: Any, agent: tuple[str, str]) -> bool:
    detail = _detail(response)
    return agent[0] in detail or agent[1] in detail


def _both(first: Callable[[], Any], second: Callable[[], Any]) -> tuple[Any, Any]:
    """Run two request thunks genuinely in parallel and return both responses."""

    async def run() -> list[Any]:
        return list(await asyncio.gather(asyncio.to_thread(first), asyncio.to_thread(second)))

    left, right = asyncio.run(run())
    return left, right


# --- two agents on one repository ---------------------------------------------


def test_trigger_label_binding_null_and_fast_on_two_agents_both_succeed(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    full = _agent(client, _github())

    fast = _add(client, _slack_agent(client)[0], _github(label=FAST))

    assert fast.status_code == 201, fast.text
    assert _github_channels(client, full[0]) == [(REPO, None)]
    assert [
        (channel["address"], channel["trigger_label"])
        for channel in fast.json()["channels"]
        if channel["kind"] == "github"
    ] == [(REPO, FAST)]


def test_trigger_label_binding_create_persists_label(
    github_install: tuple[TestClient, TestClient],
) -> None:
    """R1 finding 7: the initial binding of ``POST /agents`` keeps its label."""

    client, _rival = github_install
    _agent(client, _github())

    created = _create(client, _github(label=FAST))

    assert created.status_code == 201, created.text
    assert [(c["kind"], c["address"], c["trigger_label"]) for c in created.json()["channels"]] == [
        ("github", REPO, FAST)
    ]
    assert _github_channels(client, str(created.json()["id"])) == [(REPO, FAST)]


def test_trigger_label_binding_read_shape_shows_trigger_label(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    labelled = _agent(client, _github(label=FAST))
    slack = _slack_agent(client)

    listed = client.get("/agents", headers=_headers())

    assert listed.status_code == 200, listed.text
    by_id = {str(agent["id"]): agent["channels"] for agent in listed.json()}
    (github_channel,) = by_id[labelled[0]]
    assert (github_channel["kind"], github_channel["address"]) == ("github", REPO)
    assert github_channel["trigger_label"] == FAST
    (slack_channel,) = by_id[slack[0]]
    assert slack_channel["trigger_label"] is None


@pytest.mark.parametrize("path", ["add", "create"])
def test_trigger_label_binding_same_explicit_label_on_a_second_agent_is_409_naming_it(
    github_install: tuple[TestClient, TestClient], path: str
) -> None:
    client, _rival = github_install
    owner = _agent(client, _github(label=FAST))

    if path == "add":
        taker = _slack_agent(client)
        refused = _add(client, taker[0], _github(label=FAST))
    else:
        refused = _create(client, _github(label=FAST))

    assert refused.status_code == 409, refused.text
    assert _names_agent(refused, owner), refused.text
    assert _github_channels(client, owner[0]) == [(REPO, FAST)]
    # Liveness: a different label on the same repository is accepted.
    if path == "add":
        assert _add(client, taker[0], _github(label="curie-factory-slow")).status_code == 201
    else:
        assert _create(client, _github(label="curie-factory-slow")).status_code == 201


@pytest.mark.parametrize("path", ["add", "create"])
def test_trigger_label_binding_two_null_labels_on_one_repository_are_409_naming_it(
    github_install: tuple[TestClient, TestClient], path: str
) -> None:
    client, _rival = github_install
    owner = _agent(client, _github())

    if path == "add":
        refused = _add(client, _slack_agent(client)[0], _github())
    else:
        refused = _create(client, _github())

    assert refused.status_code == 409, refused.text
    assert _names_agent(refused, owner), refused.text


@pytest.mark.parametrize(
    ("first", "second"),
    [(None, INSTALL), (INSTALL, None)],
    ids=["explicit_beside_null", "null_beside_explicit"],
)
@pytest.mark.parametrize("path", ["add", "create"])
def test_trigger_label_binding_explicit_install_label_beside_a_null_row_is_409(
    github_install: tuple[TestClient, TestClient],
    first: str | None,
    second: str | None,
    path: str,
) -> None:
    """Edge case 3: the index does not see these two collide (one NULL, one
    ``curie-factory``), so the API check under the pair lock is the only guard."""

    client, _rival = github_install
    owner = _agent(client, _github(label=first))

    if path == "add":
        refused = _add(client, _slack_agent(client)[0], _github(label=second))
    else:
        refused = _create(client, _github(label=second))

    assert refused.status_code == 409, refused.text
    assert _names_agent(refused, owner), refused.text
    rows = [
        channel
        for agent in client.get("/agents", headers=_headers()).json()
        for channel in agent["channels"]
        if channel["kind"] == "github" and channel["address"] == REPO
    ]
    assert len(rows) == 1, rows


def test_trigger_label_binding_second_github_binding_for_one_agent_on_one_repository_is_409(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    agent = _agent(client, _github())

    refused = _add(client, agent[0], _github(label=FAST))

    assert refused.status_code == 409, refused.text
    assert _github_channels(client, agent[0]) == [(REPO, None)]
    # Liveness: the same agent may serve another repository.
    assert _add(client, agent[0], _github(OTHER_REPO, FAST)).status_code == 201
    assert _github_channels(client, agent[0]) == [(REPO, None), (OTHER_REPO, FAST)]


def test_trigger_label_binding_repost_same_label_is_idempotent(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    agent = _slack_agent(client)
    assert _add(client, agent[0], _github(label=FAST)).status_code == 201

    again = _add(client, agent[0], _github(label=FAST))

    assert again.status_code == 201, again.text
    assert _github_channels(client, agent[0]) == [(REPO, FAST)]


def test_trigger_label_binding_repost_other_label_is_refused(
    github_install: tuple[TestClient, TestClient],
) -> None:
    """R1 finding 8: the same route under a different label is decision 5.1's
    409, never a silent success that keeps the old label."""

    client, _rival = github_install
    agent = _slack_agent(client)
    assert _add(client, agent[0], _github(label=FAST)).status_code == 201

    other = _add(client, agent[0], _github(label="curie-factory-slow"))
    unlabelled = _add(client, agent[0], _github())

    assert other.status_code == 409, other.text
    assert unlabelled.status_code == 409, unlabelled.text
    assert _github_channels(client, agent[0]) == [(REPO, FAST)]


# --- refused writes -------------------------------------------------------------


@pytest.mark.parametrize("path", ["add", "create"])
def test_trigger_label_binding_label_on_a_slack_binding_is_422(
    github_install: tuple[TestClient, TestClient], path: str
) -> None:
    client, _rival = github_install
    slack = {"kind": "slack", "address": "C0EXAMPLE9", "trigger_label": FAST}

    if path == "add":
        agent = _slack_agent(client)
        refused = _add(client, agent[0], slack)
    else:
        refused = _create(client, slack)

    assert refused.status_code == 422, refused.text
    assert "trigger_label" in refused.text
    # Refused by the label rule, not because the field is unknown.
    assert "extra_forbidden" not in refused.text, refused.text
    # Liveness: the same Slack binding without a label is accepted.
    plain = {"kind": "slack", "address": "C0EXAMPLE9"}
    accepted = _add(client, agent[0], plain) if path == "add" else _create(client, plain)
    assert accepted.status_code == 201, accepted.text


@pytest.mark.parametrize(
    "label",
    ["", "x" * 51, "curie factory", "curie\tfactory", "curie-factory:x", "base:x", "repo:x"],
    ids=["empty", "51_chars", "space", "tab", "state_prefix", "base_prefix", "repo_prefix"],
)
def test_trigger_label_binding_invalid_label_is_422(
    github_install: tuple[TestClient, TestClient], label: str
) -> None:
    client, _rival = github_install
    agent = _slack_agent(client)

    refused = _add(client, agent[0], _github(label=label))
    created = _create(client, _github(label=label))

    assert refused.status_code == 422, refused.text
    assert created.status_code == 422, created.text
    # Refused by the label rule, not because the field is unknown.
    assert "extra_forbidden" not in refused.text, refused.text
    assert "extra_forbidden" not in created.text, created.text
    assert _github_channels(client, agent[0]) == []


@pytest.mark.parametrize("label", ["x" * 50, "curie-factory-fast", "Base-next", "my-base:x"])
def test_trigger_label_binding_valid_label_edges_are_accepted(
    github_install: tuple[TestClient, TestClient], label: str
) -> None:
    """Liveness for the invalid-label refusals: 50 characters, and a reserved
    word anywhere but at the start, are accepted."""

    client, _rival = github_install
    agent = _slack_agent(client)

    added = _add(client, agent[0], _github(label=label))

    assert added.status_code == 201, added.text
    assert _github_channels(client, agent[0]) == [(REPO, label)]


# --- move -----------------------------------------------------------------------


def test_trigger_label_binding_move_keeps_the_label_and_refuses_a_label_field(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    agent = _agent(client, _github(label=FAST))

    refused = _move(
        client, agent[0], "github", REPO, {**_github(OTHER_REPO), "trigger_label": "other"}
    )

    assert refused.status_code == 422, refused.text
    assert _github_channels(client, agent[0]) == [(REPO, FAST)]

    moved = _move(client, agent[0], "github", REPO, _github(OTHER_REPO))

    assert moved.status_code == 200, moved.text
    assert _github_channels(client, agent[0]) == [(OTHER_REPO, FAST)]


def test_trigger_label_binding_move_does_not_conflict_with_itself(
    github_install: tuple[TestClient, TestClient],
) -> None:
    """R1 finding 8: re-asserting a labelled row in place does not count the
    row against itself."""

    client, _rival = github_install
    agent = _agent(client, _github(label=FAST))
    _agent(client, _github())

    moved = _move(client, agent[0], "github", REPO, _github(REPO))

    assert moved.status_code == 200, moved.text
    assert _github_channels(client, agent[0]) == [(REPO, FAST)]


def test_trigger_label_binding_move_onto_another_agents_effective_label_is_409(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, _rival = github_install
    owner = _agent(client, _github())
    mover = _agent(client, _github(OTHER_REPO))

    refused = _move(client, mover[0], "github", OTHER_REPO, _github(REPO))

    assert refused.status_code == 409, refused.text
    assert _names_agent(refused, owner), refused.text
    assert _github_channels(client, mover[0]) == [(OTHER_REPO, None)]

    # Liveness: a row whose stored label differs moves onto the repository.
    labelled = _agent(client, _github("acme-corp/third-svc", FAST))
    moved = _move(client, labelled[0], "github", "acme-corp/third-svc", _github(REPO))
    assert moved.status_code == 200, moved.text
    assert _github_channels(client, labelled[0]) == [(REPO, FAST)]


# --- gitlab and jira surfaces ---------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "address"),
    [("gitlab", GITLAB_PATH), ("jira", JIRA_BINDING)],
)
def test_trigger_label_binding_tracker_surface_with_no_configured_location_is_422_unconfigured(
    github_install: tuple[TestClient, TestClient], kind: str, address: str
) -> None:
    """A GitHub-only install configures no GitLab project or Jira binding, so
    every such surface is unconfigured. (Both kinds are installed since #4358
    and #4399; the refusal is about configuration, not installation.)"""

    client, _rival = github_install
    agent = _slack_agent(client)

    for label in (None, FAST):
        channel: dict[str, Any] = {"kind": kind, "address": address}
        if label is not None:
            channel["trigger_label"] = label
        refused = _add(client, agent[0], channel)
        assert refused.status_code == 422, refused.text
        assert "surface_unconfigured" in refused.text, refused.text
        created = _create(client, channel)
        assert created.status_code == 422, created.text
        assert "surface_unconfigured" in created.text, created.text


@pytest.mark.parametrize(
    ("kind", "address"),
    [("gitlab", GITLAB_PATH), ("gitlab", GITLAB_DEEP_PATH), ("jira", JIRA_BINDING)],
    ids=["gitlab", "gitlab_four_deep", "jira"],
)
def test_trigger_label_binding_tracker_surface_of_an_installed_kind_is_accepted(
    tracker_kinds_install: TestClient, kind: str, address: str
) -> None:
    """Liveness for the refusal above: with the kind installed and the location
    configured, the same write binds, with and without a label, on two agents."""

    client = tracker_kinds_install
    plain = _slack_agent(client)
    labelled = _slack_agent(client)

    unlabelled = _add(client, plain[0], {"kind": kind, "address": address})
    fast = _add(client, labelled[0], {"kind": kind, "address": address, "trigger_label": FAST})

    assert unlabelled.status_code == 201, unlabelled.text
    assert fast.status_code == 201, fast.text
    assert [
        (c["address"], c["trigger_label"])
        for c in _channels(client, labelled[0])
        if c["kind"] == kind
    ] == [(address, FAST)]
    # Overlap is the same location with the same effective label, on any kind.
    again = _add(client, _slack_agent(client)[0], {"kind": kind, "address": address})
    assert again.status_code == 409, again.text
    assert _names_agent(again, plain), again.text


@pytest.mark.parametrize(
    ("kind", "address"),
    [("gitlab", "acme/platform/unlisted"), ("jira", "unknown-site")],
)
def test_trigger_label_binding_unconfigured_location_of_an_installed_kind_is_422(
    tracker_kinds_install: TestClient, kind: str, address: str
) -> None:
    client = tracker_kinds_install

    refused = _add(client, _slack_agent(client)[0], {"kind": kind, "address": address})

    assert refused.status_code == 422, refused.text
    assert "surface_unconfigured" in refused.text, refused.text


def test_trigger_label_binding_gitlab_address_shape(
    github_install: tuple[TestClient, TestClient],
) -> None:
    """A GitLab project path may nest to any depth; a single segment is not a
    path. The shape is checked before the configuration, so a nested path in a
    shipped build reaches the unconfigured refusal, not a shape refusal."""

    client, _rival = github_install
    agent = _slack_agent(client)

    deep = _add(client, agent[0], {"kind": "gitlab", "address": "a/b/c/d"})
    single = _add(client, agent[0], {"kind": "gitlab", "address": "a"})

    assert deep.status_code == 422, deep.text
    assert "surface_unconfigured" in deep.text, deep.text
    assert single.status_code == 422, single.text
    assert "surface_unconfigured" not in single.text, single.text


# --- concurrency against Postgres -----------------------------------------------

_RACES = 4


def _github_rows_on(client: TestClient, repo: str) -> list[tuple[str, str | None]]:
    return [
        (str(agent["id"]), channel["trigger_label"])
        for agent in client.get("/agents", headers=_headers()).json()
        for channel in agent["channels"]
        if channel["kind"] == "github" and channel["address"] == repo
    ]


def _one_won(first: Any, second: Any) -> None:
    assert sorted([first.status_code, second.status_code]) == [201, 409], (
        first.status_code,
        first.text,
        second.status_code,
        second.text,
    )


@pytest.mark.parametrize(
    ("left_label", "right_label"),
    [(FAST, FAST), (None, INSTALL)],
    ids=["same_explicit_label", "null_and_install_label"],
)
def test_trigger_label_binding_concurrent_adds_same_label_two_agents_one_wins(
    github_install: tuple[TestClient, TestClient],
    left_label: str | None,
    right_label: str | None,
) -> None:
    """Two agents add the same effective label on one repository at once:
    exactly one 201 and one 409. ``null_and_install_label`` is the case only the
    pair advisory lock can decide: the two rows differ at the index."""

    client, rival = github_install
    for race in range(_RACES):
        repo = f"acme-corp/race-add-{race}"
        left, right = _slack_agent(client), _slack_agent(client)

        first, second = _both(
            partial(_add, client, left[0], _github(repo, left_label)),
            partial(_add, rival, right[0], _github(repo, right_label)),
        )

        _one_won(first, second)
        assert len(_github_rows_on(client, repo)) == 1, _github_rows_on(client, repo)


def test_trigger_label_binding_concurrent_add_and_move_onto_one_label_one_wins(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, rival = github_install
    for race in range(_RACES):
        repo = f"acme-corp/race-move-{race}"
        source = f"acme-corp/race-source-{race}"
        mover = _agent(client, _github(source))
        adder = _slack_agent(client)

        moved, added = _both(
            partial(_move, client, mover[0], "github", source, _github(repo)),
            partial(_add, rival, adder[0], _github(repo, INSTALL)),
        )

        assert sorted([moved.status_code, added.status_code]) in ([200, 409], [201, 409]), (
            moved.status_code,
            moved.text,
            added.status_code,
            added.text,
        )
        assert moved.status_code != added.status_code
        assert len(_github_rows_on(client, repo)) == 1, _github_rows_on(client, repo)


def test_trigger_label_binding_concurrent_create_and_add_one_wins(
    github_install: tuple[TestClient, TestClient],
) -> None:
    client, rival = github_install
    for race in range(_RACES):
        repo = f"acme-corp/race-create-{race}"
        adder = _slack_agent(client)

        created, added = _both(
            partial(_create, client, _github(repo)),
            partial(_add, rival, adder[0], _github(repo, INSTALL)),
        )

        _one_won(created, added)
        assert len(_github_rows_on(client, repo)) == 1, _github_rows_on(client, repo)
