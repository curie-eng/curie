"""POST /agents conflict handling: a duplicate is a 409, not a 500.

Real Postgres round-trip (the disposable-DB conftest provisions and migrates a
throwaway database per run); name and repo_full_name are unique columns, so a
collision must surface as a caller conflict, not an opaque server error.
"""

from typing import Any

import pytest


def _create(client: Any, headers: dict[str, str], **fields: Any) -> Any:
    return client.post("/agents", json=fields, headers=headers)


def test_duplicate_name_is_409(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    first = _create(client, auth_headers, name="dup-name", slack_channel="C0AAAAAA1")
    assert first.status_code == 201, first.text

    dup = _create(client, auth_headers, name="dup-name", slack_channel="C0BBBBBB2")
    assert dup.status_code == 409, dup.text
    assert dup.json()["detail"] == "an agent with that name already exists"


def test_duplicate_repo_is_409(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    first = _create(
        client,
        auth_headers,
        name="repo-agent-a",
        slack_channel="C0CCCCCC3",
        repo_full_name="octo/shared-repo",
    )
    assert first.status_code == 201, first.text

    dup = _create(
        client,
        auth_headers,
        name="repo-agent-b",
        slack_channel="C0DDDDDD4",
        repo_full_name="octo/shared-repo",
    )
    assert dup.status_code == 409, dup.text
    assert dup.json()["detail"] == "an agent for that repository already exists"


def test_duplicate_slack_channel_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#38: one agent per channel. A second agent on a taken channel is refused
    at create time, rather than being accepted and then silently shadowed by the
    worker's resolver (which routes a channel to exactly one agent)."""

    first = _create(client, auth_headers, name="chan-agent-a", slack_channel="C0EEEEEE5")
    assert first.status_code == 201, first.text

    dup = _create(client, auth_headers, name="chan-agent-b", slack_channel="C0EEEEEE5")
    assert dup.status_code == 409, dup.text
    assert "already bound to that Slack channel" in dup.json()["detail"]


def test_patch_onto_taken_slack_channel_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The update seam is fenced identically to create (#143's posture): the
    constraint cannot be sidestepped by creating on a free channel and then
    PATCHing onto a taken one."""

    first = _create(client, auth_headers, name="patch-chan-a", slack_channel="C0FFFFFF6")
    assert first.status_code == 201, first.text
    second = _create(client, auth_headers, name="patch-chan-b", slack_channel="C0FFFFFF7")
    assert second.status_code == 201, second.text

    moved = client.patch(
        f"/agents/{second.json()['id']}",
        json={"slack_channel": "C0FFFFFF6"},
        headers=auth_headers,
    )
    assert moved.status_code == 409, moved.text
    assert "already bound to that Slack channel" in moved.json()["detail"]


def test_agent_approval_required_tools_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Create with permission gates (#245); they come back on reads.
    created = client.post(
        "/agents",
        json={
            "name": "gated-agent",
            "slack_channel": "C000000G01",
            "approval_required_tools": ["Bash", "mcp__github__create_issue"],
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["approval_required_tools"] == ["Bash", "mcp__github__create_issue"]

    # PATCH replaces the set; an explicit empty list clears it (NULL posture).
    patched = client.patch(
        f"/agents/{body['id']}",
        json={"approval_required_tools": ["WebFetch"]},
        headers=auth_headers,
    )
    assert patched.status_code == 200
    assert patched.json()["approval_required_tools"] == ["WebFetch"]

    cleared = client.patch(
        f"/agents/{body['id']}",
        json={"approval_required_tools": []},
        headers=auth_headers,
    )
    assert cleared.json()["approval_required_tools"] is None

    # Omitting the field leaves the gates unchanged.
    repatched = client.patch(
        f"/agents/{body['id']}",
        json={"approval_required_tools": ["Bash"]},
        headers=auth_headers,
    )
    assert repatched.json()["approval_required_tools"] == ["Bash"]
    untouched = client.patch(
        f"/agents/{body['id']}", json={"model": "claude-sonnet-5"}, headers=auth_headers
    )
    assert untouched.json()["approval_required_tools"] == ["Bash"]


def test_agent_approval_required_tools_rejects_bad_names(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A comma inside a name would split into two wrong gates on the env wire.
    for bad in (["Bash,Read"], [""], ["  "]):
        resp = client.post(
            "/agents",
            json={
                "name": f"bad-{bad[0].strip() or 'blank'}",
                "slack_channel": "C000000G02",
                "approval_required_tools": bad,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422, resp.text


def test_agent_approval_routes_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = client.post(
        "/agents",
        json={
            "name": "routed-agent",
            "slack_channel": "C000000R01",
            "approval_routes": {"managers": {"channel": "C000000R02"}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["approval_routes"] == {"managers": {"channel": "C000000R02"}}

    # PATCH replaces the map; an explicit empty dict clears it.
    patched = client.patch(
        f"/agents/{body['id']}",
        json={"approval_routes": {"legal": {"channel": "C000000R03"}}},
        headers=auth_headers,
    )
    assert patched.json()["approval_routes"] == {"legal": {"channel": "C000000R03"}}
    cleared = client.patch(
        f"/agents/{body['id']}", json={"approval_routes": {}}, headers=auth_headers
    )
    assert cleared.json()["approval_routes"] is None


def test_agent_approval_routes_rejects_bad_bindings(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A binding must carry a Slack channel ID, not a #name; route names must be
    # non-empty.
    for routes in (
        {"managers": {"channel": "#managers"}},
        {" ": {"channel": "C000000R04"}},
    ):
        resp = client.post(
            "/agents",
            json={
                "name": f"bad-routes-{list(routes)[0].strip() or 'blank'}",
                "slack_channel": "C000000R05",
                "approval_routes": routes,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422, resp.text


# --- #420: the approvers block on a route binding ------------------------------
#
# `approvers` is the WHO, sitting alongside the binding's `channel` (the WHERE).
# It is workspace deployment config, so it is validated on write with the same
# allowlist-ID discipline #143 established for channels: real IDs, never
# @handles or bare names, which never resolve and fail silently.


def test_agent_approval_routes_with_approvers_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The extended binding shape survives create and PATCH verbatim.

    The stored JSONB stays minimal: a group-only binding must NOT read back with
    a `users: null` sibling, or every pre-#420 binding gets rewritten with null
    padding on the next write.
    """

    created = client.post(
        "/agents",
        json={
            "name": "approvers-agent",
            "slack_channel": "C000000A01",
            "approval_routes": {
                "managers": {"channel": "C000000A02", "approvers": {"group": "S000000G1"}}
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["approval_routes"] == {
        "managers": {"channel": "C000000A02", "approvers": {"group": "S000000G1"}}
    }

    patched = client.patch(
        f"/agents/{body['id']}",
        json={
            "approval_routes": {
                "managers": {
                    "channel": "C000000A02",
                    "approvers": {"users": ["U000000U1", "W000000E1"]},
                }
            }
        },
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["approval_routes"] == {
        "managers": {
            "channel": "C000000A02",
            "approvers": {"users": ["U000000U1", "W000000E1"]},
        }
    }


def test_agent_approval_routes_accepts_both_users_and_group(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Both set is VALID, not an error: issue #420 settles the precedence
    (`users` wins, `group` is ignored at read time) rather than refusing the
    combination at write time."""

    created = client.post(
        "/agents",
        json={
            "name": "both-approvers-agent",
            "slack_channel": "C000000B01",
            "approval_routes": {
                "managers": {
                    "channel": "C000000B02",
                    "approvers": {"group": "S000000G2", "users": ["U000000U2"]},
                }
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    assert created.json()["approval_routes"]["managers"]["approvers"] == {
        "group": "S000000G2",
        "users": ["U000000U2"],
    }


def test_agent_approval_routes_rejects_bad_approvers(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Every way an approvers block can be meaningless is a clear 422 on write,
    never a silently-unenforceable binding:

    - `{}`: declares an approvers block that restricts nothing.
    - `users: []`: neither "unset" (omit the key) nor "nobody may approve" --
      the latter as silent config is a footgun, since the approval could then
      only ever expire.
    - a `@handle` or bare name where an ID belongs: never resolves (#143).
    - a channel ID where a usergroup ID belongs: the S-prefix is the whole
      distinction, and a C-prefixed value would look plausible in a config file.
    """

    bad_approvers = [
        {},
        {"users": []},
        {"group": "@managers"},
        {"group": "managers"},
        {"group": "C000000C9"},
        {"group": ""},
        {"users": ["not-a-user"]},
        {"users": ["@brian"]},
        {"users": ["U000000U3", "nope"]},
        {"users": [""]},
    ]
    for index, approvers in enumerate(bad_approvers):
        resp = client.post(
            "/agents",
            json={
                "name": f"bad-approvers-{index}",
                "slack_channel": "C000000C01",
                "approval_routes": {"managers": {"channel": "C000000C02", "approvers": approvers}},
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422, f"{approvers!r} was accepted: {resp.text}"


def test_agent_approval_routes_rejects_unknown_keys(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A typo in an optional key is a 422, not a silently narrower-looking
    binding.

    Ignoring the extra key is the one config error the fail-closed doctrine
    would otherwise miss: nothing was "declared", so the route falls back to
    channel membership and every member of the (deliberately broad) card channel
    becomes an approver, while the operator believes they narrowed authority to
    the users they listed.
    """

    bad_bindings = [
        # `approver`, missing the `s`: the whole approvers block disappears.
        {"channel": "C000000E02", "approver": {"users": ["U000000U1"]}},
        # A typo inside the approvers block: `user` instead of `users` leaves a
        # group-only spec, or nothing at all.
        {"channel": "C000000E02", "approvers": {"user": ["U000000U1"]}},
        {"channel": "C000000E02", "approvers": {"groups": "S000000G1"}},
        {
            "channel": "C000000E02",
            "approvers": {"users": ["U000000U1"], "unknown": "x"},
        },
    ]
    for index, binding in enumerate(bad_bindings):
        resp = client.post(
            "/agents",
            json={
                "name": f"unknown-key-{index}",
                "slack_channel": "C000000E01",
                "approval_routes": {"managers": binding},
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422, f"{binding!r} was accepted: {resp.text}"


def test_agent_approval_routes_patch_rejects_unknown_keys(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#143's posture: create and PATCH validate identically, so a typo'd key
    cannot be smuggled in through the update path either."""

    created = client.post(
        "/agents",
        json={"name": "patch-unknown-key-agent", "slack_channel": "C000000F01"},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    for binding in (
        {"channel": "C000000F02", "approver": {"users": ["U000000U1"]}},
        {"channel": "C000000F02", "approvers": {"users": ["U000000U1"], "extra": 1}},
    ):
        patched = client.patch(
            f"/agents/{agent_id}",
            json={"approval_routes": {"managers": binding}},
            headers=auth_headers,
        )
        assert patched.status_code == 422, f"{binding!r} was accepted: {patched.text}"


def test_agent_approval_routes_patch_rejects_bad_approvers(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#143's posture: create and PATCH validate identically, so a bad binding
    cannot be smuggled in through the update path."""

    created = client.post(
        "/agents",
        json={"name": "patch-approvers-agent", "slack_channel": "C000000D01"},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text

    patched = client.patch(
        f"/agents/{created.json()['id']}",
        json={
            "approval_routes": {"managers": {"channel": "C000000D02", "approvers": {"users": []}}}
        },
        headers=auth_headers,
    )
    assert patched.status_code == 422, patched.text


def test_agent_secrets_round_trip_exposes_names_only(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Create with connector secrets (#429): values go in, only NAMES come back.
    created = client.post(
        "/agents",
        json={
            "name": "secret-agent",
            "slack_channel": "C000000S01",
            "secrets": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_supersecret"},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["secrets"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
    assert "ghp_supersecret" not in created.text

    # PATCH adds a second secret and reflects both names, still no values.
    patched = client.patch(
        f"/agents/{body['id']}",
        json={"secrets": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_x", "API_KEY": "k"}},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["secrets"] == ["API_KEY", "GITHUB_PERSONAL_ACCESS_TOKEN"]
    assert "ghp_x" not in patched.text


def test_agent_non_env_var_secret_name_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    bad = client.post(
        "/agents",
        json={
            "name": "bad-secret-agent",
            "slack_channel": "C000000S02",
            "secrets": {"github-token": "x"},
        },
        headers=auth_headers,
    )
    assert bad.status_code == 422, bad.text


def test_agent_reserved_secret_name_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # CURIE_* names are reserved platform boot-env keys; rejected on write.
    bad = client.post(
        "/agents",
        json={
            "name": "reserved-secret-agent",
            "slack_channel": "C000000S03",
            "secrets": {"CURIE_BUDGET": "x"},
        },
        headers=auth_headers,
    )
    assert bad.status_code == 422, bad.text


# The four runner-owned credential keys are NOT CURIE_-prefixed, so #445's
# prefix fence never caught them; a connector secret named `ANTHROPIC_BASE_URL`
# would silently redirect the model. #457 rejects them at the write seam too.
_RESERVED_CREDENTIAL_KEYS = [
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_AUTH_TOKEN",
]


@pytest.mark.parametrize("name", _RESERVED_CREDENTIAL_KEYS)
def test_agent_reserved_credential_secret_name_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None, name: str
) -> None:
    bad = client.post(
        "/agents",
        json={
            "name": "cred-secret-agent",
            "slack_channel": "C000000S04",
            "secrets": {name: "x"},
        },
        headers=auth_headers,
    )
    assert bad.status_code == 422, bad.text


# #487: redirect/capture-capable env (proxy, extra CA, custom headers) is reserved
# on the API write seam too, at the same 422 as the credential keys.
_REDIRECT_CAPTURE_KEYS = [
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NODE_EXTRA_CA_CERTS",
    "ANTHROPIC_CUSTOM_HEADERS",
]


@pytest.mark.parametrize("name", _REDIRECT_CAPTURE_KEYS)
def test_agent_reserved_redirect_capture_secret_name_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None, name: str
) -> None:
    bad = client.post(
        "/agents",
        json={
            "name": "redirect-secret-agent",
            "slack_channel": "C000000S05",
            "secrets": {name: "x"},
        },
        headers=auth_headers,
    )
    assert bad.status_code == 422, bad.text


def test_agent_legitimate_secret_name_still_creates(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Negative control: a real connector token name is unaffected by the fence.
    ok = client.post(
        "/agents",
        json={
            "name": "ok-secret-agent",
            "slack_channel": "C000000S05",
            "secrets": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_x"},
        },
        headers=auth_headers,
    )
    assert ok.status_code == 201, ok.text
    assert ok.json()["secrets"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
