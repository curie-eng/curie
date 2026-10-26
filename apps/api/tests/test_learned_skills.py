"""Learned skills (ADR 0247, epic #4780 slice 1): the store, its revisions and who may touch it.

Everything here goes through the real HTTP app against the disposable Postgres
and the compose Valkey the conftest provisions; nothing is mocked. Sandbox
credentials are minted with the shared ``sandbox_token`` module, exactly as the
worker mints them:

* the per-turn write credential: ``{binding, memory: "read", sender, turn,
  conversation, skills: "write"}`` (learned skills on, memory writes off);
* the broad boot read credential: ``{binding, memory: "read"}``;
* the bundle-facing app token: scope ``state.app``, no claims.

The routes live under ``/agents/{agent_id}/learned-skills``. ``{ref}`` is the
skill id or its slug. Every refusal below sits next to the valid neighbouring
input that must still succeed, so a route that refuses everything cannot pass.
"""

from __future__ import annotations

import hashlib
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint

_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time
SENDER = "U0ALICE001"
CONVERSATION = "slack:C0EXAMPLE1:1700000000.000100"
CLOSE_URL = "/v1/internal/memory/closed-turns"
OFF = "learned skills are off for this agent"
SCHEDULED = "scheduled runs can read learned skills but cannot change them"
CAP = 32
BODY_LIMIT = 16384
DESC_LIMIT = 1024

BODY = "1. Run the release checklist.\n2. Tag the build.\n3. Post the tag in the channel."
DESCRIPTION = "Cut a release the way this team does it"

_OMIT = object()


# --- helpers -------------------------------------------------------------------


def _detail(resp: Any) -> str:
    try:
        return str(resp.json().get("detail", ""))
    except ValueError:
        return resp.text


def _channel() -> str:
    return "C" + uuid.uuid4().hex[:9].upper()


def _agent(client: Any, auth_headers: dict[str, str], *, enabled: bool = True) -> tuple[str, str]:
    channel = _channel()
    resp = client.post(
        "/agents",
        json={
            "name": f"learned-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    if enabled:
        _toggle(client, auth_headers, aid, True)
    return aid, channel


def _toggle(client: Any, auth_headers: dict[str, str], aid: str, on: bool) -> None:
    resp = client.patch(f"/agents/{aid}", json={"learned_skills": on}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["learned_skills"] is on


def _turn() -> str:
    return f"evt-{uuid.uuid4().hex}#1"


def _token(aid: str, claims: dict[str, str | None] | None, scope: str = "state") -> dict[str, str]:
    token = mint(get_settings().api_key, agent=aid, scope=scope, exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def _writer(
    aid: str,
    channel: str = "C0EXAMPLE1",
    *,
    sender: Any = SENDER,
    turn: Any = None,
    conversation: Any = CONVERSATION,
    skills: Any = "write",
    memory: Any = "read",
) -> dict[str, str]:
    """The per-turn credential the worker mints with learned skills on."""
    claims: dict[str, str | None] = {"binding": f"slack:{channel}"}
    for name, value in (
        ("memory", memory),
        ("sender", sender),
        ("turn", _turn() if turn is None else turn),
        ("conversation", conversation),
        ("skills", skills),
    ):
        if value is not _OMIT:
            claims[name] = value
    return _token(aid, claims)


def _reader(aid: str, channel: str = "C0EXAMPLE1") -> dict[str, str]:
    """The broad boot read credential: no skills claim."""
    return _token(aid, {"binding": f"slack:{channel}", "memory": "read"})


def _app(aid: str) -> dict[str, str]:
    return _token(aid, None, scope="state.app")


def _base(aid: str) -> str:
    return f"/agents/{aid}/learned-skills"


def _create(
    client: Any,
    headers: dict[str, str],
    aid: str,
    slug: str = "cut-release",
    description: str = DESCRIPTION,
    body: str = BODY,
) -> Any:
    return client.post(
        _base(aid),
        json={"slug": slug, "description": description, "body": body},
        headers=headers,
    )


def _created(
    client: Any, headers: dict[str, str], aid: str, slug: str = "cut-release", **kw: str
) -> dict[str, Any]:
    resp = _create(client, headers, aid, slug, **kw)
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _patch(client: Any, headers: dict[str, str], aid: str, ref: str, **body: Any) -> Any:
    return client.post(f"{_base(aid)}/{ref}/patch", json=body, headers=headers)


def _revert(
    client: Any,
    headers: dict[str, str],
    aid: str,
    ref: str,
    expected_revision: int,
    to_revision: int | None = None,
) -> Any:
    body: dict[str, Any] = {"expected_revision": expected_revision}
    if to_revision is not None:
        body["to_revision"] = to_revision
    return client.post(f"{_base(aid)}/{ref}/revert", json=body, headers=headers)


def _delete(
    client: Any, headers: dict[str, str], aid: str, ref: str, expected_revision: int
) -> Any:
    return client.delete(
        f"{_base(aid)}/{ref}", params={"expected_revision": expected_revision}, headers=headers
    )


def _show(client: Any, headers: dict[str, str], aid: str, ref: str) -> Any:
    return client.get(f"{_base(aid)}/{ref}", headers=headers)


def _revisions(client: Any, headers: dict[str, str], aid: str, ref: str) -> list[dict[str, Any]]:
    resp = client.get(f"{_base(aid)}/{ref}/revisions", headers=headers)
    assert resp.status_code == 200, resp.text
    revisions: list[dict[str, Any]] = resp.json()["revisions"]
    return revisions


def _list(client: Any, headers: dict[str, str], aid: str) -> dict[str, Any]:
    resp = client.get(_base(aid), headers=headers)
    assert resp.status_code == 200, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _digest(slug: str, description: str, body: str) -> str:
    return hashlib.sha256(f"{slug}\0{description}\0{body}".encode()).hexdigest()


def _ok(resp: Any, code: int = 200) -> dict[str, Any]:
    assert resp.status_code == code, resp.text
    return resp.json()  # type: ignore[no-any-return]


# --- the per-agent toggle ----------------------------------------------------------


def test_learned_skills_defaults_off_and_agent_out_carries_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers, enabled=False)
    got = client.get(f"/agents/{aid}", headers=auth_headers)
    assert got.status_code == 200, got.text
    assert got.json()["learned_skills"] is False
    listed = client.get("/agents", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert next(a for a in listed.json() if a["id"] == aid)["learned_skills"] is False


def test_patch_turns_learned_skills_on_and_off_independent_of_memory_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers, enabled=False)
    _toggle(client, auth_headers, aid, True)
    got = client.get(f"/agents/{aid}", headers=auth_headers).json()
    assert got["learned_skills"] is True
    assert got["memory_writes"] is False
    # Omitting it leaves it unchanged.
    resp = client.patch(f"/agents/{aid}", json={"memory_writes": True}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["learned_skills"] is True
    _toggle(client, auth_headers, aid, False)
    got = client.get(f"/agents/{aid}", headers=auth_headers).json()
    assert got["learned_skills"] is False
    assert got["memory_writes"] is True
    bad = client.patch(f"/agents/{aid}", json={"learned_skills": "yes"}, headers=auth_headers)
    assert bad.status_code == 422, bad.text


# --- the store: create, list, show, history ----------------------------------------


def test_operator_create_list_show_and_history(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    zeta = _created(client, auth_headers, aid, "zeta-steps")
    alpha = _created(client, auth_headers, aid, "alpha-steps")
    assert alpha["revision"] == 1
    assert alpha["deleted"] is False
    assert alpha["author"] == "operator"
    assert alpha["conversation"] is None
    assert alpha["body"] == BODY
    assert alpha["description"] == DESCRIPTION
    uuid.UUID(alpha["id"])
    for field in ("created_at", "updated_at"):
        assert alpha[field]

    listed = _list(client, auth_headers, aid)
    assert listed["enabled"] is True
    assert [s["slug"] for s in listed["skills"]] == ["alpha-steps", "zeta-steps"]

    by_id = _ok(_show(client, auth_headers, aid, zeta["id"]))
    by_slug = _ok(_show(client, auth_headers, aid, "zeta-steps"))
    assert by_id["id"] == by_slug["id"] == zeta["id"]

    [rev] = _revisions(client, auth_headers, aid, alpha["id"])
    assert rev["revision"] == 1
    assert rev["action"] == "create"
    assert rev["source"] == "operator"
    assert rev["author"] == "operator"
    assert rev["conversation"] is None
    assert rev["turn"] is None
    assert rev["reverted_to"] is None
    assert rev["slug"] == "alpha-steps"
    assert rev["body"] == BODY
    assert rev["digest"] == _digest("alpha-steps", DESCRIPTION, BODY)
    assert rev["created_at"]


def test_unknown_ref_is_404(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid, _ = _agent(client, auth_headers)
    _created(client, auth_headers, aid)
    assert _show(client, auth_headers, aid, str(uuid.uuid4())).status_code == 404
    assert _show(client, auth_headers, aid, "no-such-skill").status_code == 404
    assert _show(client, auth_headers, aid, "cut-release").status_code == 200


def test_agent_write_stamps_provenance_server_side(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    turn = _turn()
    writer = _writer(aid, channel, turn=turn)
    # A body that tries to name its own author never wins over the claims.
    resp = client.post(
        _base(aid),
        json={
            "slug": "cut-release",
            "description": DESCRIPTION,
            "body": BODY,
            "author": "U0FORGED01",
            "conversation": "forged",
            "turn": "forged",
            "source": "operator",
        },
        headers=writer,
    )
    if resp.status_code == 422:
        # A schema that forbids extra fields is also fine; create it cleanly.
        resp = _create(client, writer, aid)
    created = _ok(resp, 201)
    assert created["author"] == SENDER

    full = _ok(_show(client, auth_headers, aid, created["id"]))
    assert full["author"] == SENDER
    assert full["conversation"] == CONVERSATION
    [rev] = _revisions(client, auth_headers, aid, created["id"])
    assert rev["author"] == SENDER
    assert rev["conversation"] == CONVERSATION
    assert rev["turn"] == turn
    assert rev["source"] == "agent"
    assert rev["action"] == "create"
    assert rev["digest"] == _digest("cut-release", DESCRIPTION, BODY)

    # The operator's own write is stamped operator, not the last agent sender.
    patched = _ok(
        _patch(
            client,
            auth_headers,
            aid,
            created["id"],
            expected_revision=1,
            description="Cut a release, operator wording",
        )
    )
    assert patched["author"] == "operator"
    assert patched["conversation"] is None
    revs = _revisions(client, auth_headers, aid, created["id"])
    assert [(r["revision"], r["source"], r["author"]) for r in revs] == [
        (1, "agent", SENDER),
        (2, "operator", "operator"),
    ]


def test_state_responses_omit_conversation_and_turn(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    reader = _reader(aid, channel)

    def _no_provenance(skill: dict[str, Any]) -> None:
        assert "conversation" not in skill, skill
        assert "turn" not in skill, skill
        assert skill["author"]

    created = _ok(_create(client, writer, aid), 201)
    _no_provenance(created)
    for headers in (writer, reader):
        listed = _list(client, headers, aid)
        assert listed["enabled"] is True
        for skill in listed["skills"]:
            _no_provenance(skill)
        _no_provenance(_ok(_show(client, headers, aid, created["id"])))
        for rev in _revisions(client, headers, aid, created["id"]):
            _no_provenance(rev)
            assert rev["source"] == "agent"
    patched = _ok(
        _patch(
            client,
            writer,
            aid,
            "cut-release",
            expected_revision=1,
            old_string="Tag the build",
            new_string="Tag the build with the date",
        )
    )
    _no_provenance(patched)
    reverted = _ok(_revert(client, writer, aid, "cut-release", 2))
    _no_provenance(reverted)
    deleted = _ok(_delete(client, writer, aid, "cut-release", 3))
    _no_provenance(deleted)
    # The operator still sees full provenance on the same rows.
    revs = _revisions(client, auth_headers, aid, created["id"])
    assert all(r["conversation"] == CONVERSATION for r in revs)
    assert all(r["turn"] for r in revs)


def test_agent_credential_can_delete_and_revisions_are_immutable(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    created = _ok(_create(client, writer, aid), 201)
    before = _revisions(client, auth_headers, aid, created["id"])
    patched = _ok(
        _patch(
            client,
            writer,
            aid,
            created["id"],
            expected_revision=1,
            old_string="Tag the build",
            new_string="Tag the build twice",
        )
    )
    assert patched["revision"] == 2
    deleted = _ok(_delete(client, writer, aid, created["id"], 2))
    assert deleted["deleted"] is True
    assert deleted["revision"] == 3
    after = _revisions(client, auth_headers, aid, created["id"])
    # Revision 1 is byte-for-byte what it was: revisions are insert-only.
    assert after[0] == before[0]
    assert [r["action"] for r in after] == ["create", "patch", "delete"]
    assert [r["revision"] for r in after] == [1, 2, 3]
    assert _list(client, auth_headers, aid)["skills"] == []


def test_create_conflicts_on_an_active_slug_and_reuses_a_deleted_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    first = _created(client, auth_headers, aid)
    dup = _create(client, auth_headers, aid, body="something else")
    assert dup.status_code == 409, dup.text
    _ok(_delete(client, auth_headers, aid, first["id"], 1))
    second = _created(client, auth_headers, aid, body="Fresh steps")
    assert second["id"] != first["id"]
    assert second["revision"] == 1


# --- CAS ---------------------------------------------------------------------------


def test_stale_expected_revision_is_409_on_every_mutation(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    skill = _created(client, auth_headers, aid)
    _ok(
        _patch(
            client,
            writer,
            aid,
            skill["id"],
            expected_revision=1,
            old_string="Tag the build",
            new_string="Tag it",
        )
    )
    # Two writers both read revision 1; the second is now stale.
    for headers in (writer, auth_headers):
        stale = _patch(
            client,
            headers,
            aid,
            skill["id"],
            expected_revision=1,
            old_string="Tag it",
            new_string="Tag it now",
        )
        assert stale.status_code == 409, stale.text
        assert _revert(client, headers, aid, skill["id"], 1).status_code == 409
        assert _delete(client, headers, aid, skill["id"], 1).status_code == 409
        assert _delete(client, headers, aid, skill["id"], 7).status_code == 409
    head = _ok(_show(client, auth_headers, aid, skill["id"]))
    assert head["revision"] == 2
    assert "Tag it" in head["body"]
    # Liveness: the current revision goes through.
    fresh = _ok(
        _patch(
            client,
            writer,
            aid,
            skill["id"],
            expected_revision=2,
            old_string="Tag it",
            new_string="Tag it now",
        )
    )
    assert fresh["revision"] == 3


# --- limits ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "slug",
    ["../x", "Deploy", "UPPER", "-lead", "", "a" * 65, "a/b", "a b", "a_b", "x\n", "..", "a.b"],
)
def test_bad_slug_is_422_naming_slug(
    client: Any, auth_headers: dict[str, str], clean_db: None, slug: str
) -> None:
    aid, channel = _agent(client, auth_headers)
    for headers in (auth_headers, _writer(aid, channel)):
        resp = _create(client, headers, aid, slug)
        assert resp.status_code == 422, (slug, resp.text)
        assert "slug" in _detail(resp)
    assert _list(client, auth_headers, aid)["skills"] == []


@pytest.mark.parametrize("slug", ["a", "a" * 64, "deploy-v2", "0day", "a-"])
def test_valid_slug_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None, slug: str
) -> None:
    aid, _ = _agent(client, auth_headers)
    assert _created(client, auth_headers, aid, slug)["slug"] == slug


@pytest.mark.parametrize(
    "description", ["", "x" * (DESC_LIMIT + 1), "first line\nsecond", "first\rsecond", "a\r\n"]
)
def test_bad_description_is_422_naming_description(
    client: Any, auth_headers: dict[str, str], clean_db: None, description: str
) -> None:
    aid, _ = _agent(client, auth_headers)
    resp = _create(client, auth_headers, aid, description=description)
    assert resp.status_code == 422, resp.text
    assert "description" in _detail(resp)


def test_description_at_the_limit_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    # Characters, not bytes: 1024 multi-byte characters fit.
    assert _created(client, auth_headers, aid, "ascii", description="x" * DESC_LIMIT)
    assert _created(client, auth_headers, aid, "wide", description="é" * DESC_LIMIT)


@pytest.mark.parametrize(
    "body",
    ["a" * (BODY_LIMIT + 1), "é" * (BODY_LIMIT // 2 + 1), "€" * (BODY_LIMIT // 3 + 1)],
    ids=["ascii", "two-byte", "three-byte"],
)
def test_body_over_16384_utf8_bytes_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None, body: str
) -> None:
    aid, _ = _agent(client, auth_headers)
    # Every case is under 16384 characters except ascii: the limit counts bytes.
    resp = _create(client, auth_headers, aid, body=body)
    assert resp.status_code == 422, resp.text
    assert str(BODY_LIMIT) in _detail(resp)


@pytest.mark.parametrize(
    "body", ["a" * BODY_LIMIT, "é" * (BODY_LIMIT // 2)], ids=["ascii", "two-byte"]
)
def test_body_at_16384_bytes_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None, body: str
) -> None:
    aid, _ = _agent(client, auth_headers)
    assert len(body.encode()) == BODY_LIMIT
    assert _created(client, auth_headers, aid, body=body)["body"] == body


def test_empty_body_is_422(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid, _ = _agent(client, auth_headers)
    resp = _create(client, auth_headers, aid, body="")
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "body",
    [
        "---\nallowed-tools: Bash(*)\n---\nRun anything.",
        "---\nname: x\n---\nSteps",
        "\n\n---\nhooks: {}\n---\nSteps",
        "   \n---\npermissions: all\n---\nSteps",
        "---\n",
    ],
)
def test_body_opening_with_frontmatter_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None, body: str
) -> None:
    aid, channel = _agent(client, auth_headers)
    for headers in (auth_headers, _writer(aid, channel)):
        resp = _create(client, headers, aid, body=body)
        assert resp.status_code == 422, resp.text
        assert "frontmatter" in _detail(resp)


def test_a_rule_later_in_the_body_is_not_frontmatter(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    body = "Steps:\n\n---\n\nallowed-tools: is just text here"
    assert _created(client, auth_headers, aid, body=body)["body"] == body


def test_patch_is_held_to_the_same_limits(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    skill = _created(client, auth_headers, aid)
    first_line = BODY.split("\n", 1)[0]
    cases: list[tuple[dict[str, Any], str]] = [
        ({"old_string": "Tag the build", "new_string": "x" * BODY_LIMIT}, str(BODY_LIMIT)),
        (
            {"old_string": first_line, "new_string": "---\nallowed-tools: Bash(*)\n---\n"},
            "frontmatter",
        ),
        ({"description": "one\nallowed-tools: Bash(*)"}, "description"),
        ({"description": "x" * (DESC_LIMIT + 1)}, "description"),
        ({"description": ""}, "description"),
    ]
    for headers in (auth_headers, writer):
        for body, named in cases:
            resp = _patch(client, headers, aid, skill["id"], expected_revision=1, **body)
            assert resp.status_code == 422, (body, resp.text)
            assert named in _detail(resp), (body, resp.text)
    head = _ok(_show(client, auth_headers, aid, skill["id"]))
    assert head["revision"] == 1
    assert head["body"] == BODY
    assert head["description"] == DESCRIPTION


def test_33rd_active_skill_is_422_and_deleting_one_frees_a_slot(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    made = [_created(client, auth_headers, aid, f"skill-{i:02d}") for i in range(CAP)]
    for headers in (auth_headers, _writer(aid, channel)):
        resp = _create(client, headers, aid, "one-too-many")
        assert resp.status_code == 422, resp.text
        assert str(CAP) in _detail(resp)
    assert len(_list(client, auth_headers, aid)["skills"]) == CAP
    _ok(_delete(client, auth_headers, aid, made[0]["id"], 1))
    assert _created(client, auth_headers, aid, "one-too-many")["revision"] == 1


def test_concurrent_creates_at_31_never_exceed_the_cap(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The TestClient's portal serves calls from several threads on one event
    # loop, so these requests really overlap inside the app.
    aid, _ = _agent(client, auth_headers)
    for i in range(CAP - 1):
        _created(client, auth_headers, aid, f"skill-{i:02d}")
    racers = 4
    barrier = threading.Barrier(racers)

    def race(n: int) -> int:
        barrier.wait()
        resp = _create(client, auth_headers, aid, f"racer-{n}")
        assert resp.status_code in (201, 422), resp.text
        return int(resp.status_code)

    with ThreadPoolExecutor(max_workers=racers) as pool:
        codes = list(pool.map(race, range(racers)))
    assert codes.count(201) == 1, codes
    assert len(_list(client, auth_headers, aid)["skills"]) == CAP


# --- patch semantics ---------------------------------------------------------------


def test_patch_replaces_the_one_occurrence_and_keeps_the_id(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    patched = _ok(
        _patch(
            client,
            _writer(aid, channel),
            aid,
            "cut-release",
            expected_revision=1,
            old_string="Tag the build.",
            new_string="Tag the build as vYYYY.MM.DD.",
        )
    )
    assert patched["id"] == skill["id"]
    assert patched["revision"] == 2
    assert patched["body"] == BODY.replace("Tag the build.", "Tag the build as vYYYY.MM.DD.")
    assert patched["description"] == DESCRIPTION
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert rev["action"] == "patch"
    assert rev["digest"] == _digest("cut-release", DESCRIPTION, patched["body"])


def test_patch_old_string_not_found_or_ambiguous_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    body = "Step: build.\nStep: test.\nThen ship."
    skill = _created(client, auth_headers, aid, body=body)
    missing = _patch(
        client,
        auth_headers,
        aid,
        skill["id"],
        expected_revision=1,
        old_string="Step: deploy.",
        new_string="x",
    )
    assert missing.status_code == 422, missing.text
    assert "old_string" in _detail(missing)
    ambiguous = _patch(
        client,
        auth_headers,
        aid,
        skill["id"],
        expected_revision=1,
        old_string="Step:",
        new_string="Stage:",
    )
    assert ambiguous.status_code == 422, ambiguous.text
    assert "old_string" in _detail(ambiguous)
    # Case matters: an exact match only.
    wrong_case = _patch(
        client,
        auth_headers,
        aid,
        skill["id"],
        expected_revision=1,
        old_string="then ship.",
        new_string="x",
    )
    assert wrong_case.status_code == 422, wrong_case.text
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: a unique match goes through.
    ok = _ok(
        _patch(
            client,
            auth_headers,
            aid,
            skill["id"],
            expected_revision=1,
            old_string="Step: test.",
            new_string="Step: test twice.",
        )
    )
    assert ok["body"] == "Step: build.\nStep: test twice.\nThen ship."


def test_description_only_patch(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid, _ = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    patched = _ok(
        _patch(
            client,
            auth_headers,
            aid,
            skill["id"],
            expected_revision=1,
            description="Cut and announce a release",
        )
    )
    assert patched["description"] == "Cut and announce a release"
    assert patched["body"] == BODY
    assert patched["revision"] == 2


def test_empty_patch_is_422(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid, _ = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    resp = _patch(client, auth_headers, aid, skill["id"], expected_revision=1)
    assert resp.status_code == 422, resp.text
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1


def test_there_is_no_wholesale_body_field(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    for headers in (auth_headers, _writer(aid, channel)):
        resp = _patch(
            client, headers, aid, skill["id"], expected_revision=1, body="Replaced wholesale."
        )
        assert resp.status_code == 422, resp.text
    head = _ok(_show(client, auth_headers, aid, skill["id"]))
    assert head["body"] == BODY
    assert head["revision"] == 1


# --- undo and revert (R8) ----------------------------------------------------------


def test_undo_of_create_deletes_and_undo_again_brings_it_back(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    skill = _ok(_create(client, writer, aid), 201)
    undone = _ok(_revert(client, writer, aid, "cut-release", 1))
    assert undone["id"] == skill["id"]
    assert undone["deleted"] is True
    assert undone["revision"] == 2
    assert _list(client, auth_headers, aid)["skills"] == []
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert rev["action"] == "delete"

    # Undo of the undo: revert resolves the deleted skill by slug for any caller.
    redone = _ok(_revert(client, writer, aid, "cut-release", 2))
    assert redone["deleted"] is False
    assert redone["revision"] == 3
    assert redone["body"] == BODY
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert rev["action"] == "revert"
    assert rev["reverted_to"] == 1


def test_undo_after_patch_restores_prior_content_and_keeps_the_id(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    skill = _ok(_create(client, writer, aid), 201)
    patched = _ok(
        _patch(
            client,
            writer,
            aid,
            skill["id"],
            expected_revision=1,
            old_string="Tag the build",
            new_string="Skip tagging",
            description="Changed description",
        )
    )
    undone = _ok(_revert(client, writer, aid, skill["id"], 2))
    assert undone["id"] == skill["id"]
    assert undone["revision"] == 3
    assert undone["body"] == BODY
    assert undone["description"] == DESCRIPTION
    assert undone["deleted"] is False
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert rev["action"] == "revert"
    assert rev["reverted_to"] == 1
    assert rev["digest"] == _digest("cut-release", DESCRIPTION, BODY)

    # Undo of the undo redoes the patch; undoing again toggles back.
    redone = _ok(_revert(client, writer, aid, skill["id"], 3))
    assert redone["revision"] == 4
    assert redone["body"] == patched["body"]
    assert redone["description"] == "Changed description"
    assert _revisions(client, auth_headers, aid, skill["id"])[-1]["reverted_to"] == 2
    again = _ok(_revert(client, writer, aid, skill["id"], 4))
    assert again["body"] == BODY
    assert _revisions(client, auth_headers, aid, skill["id"])[-1]["reverted_to"] == 3


def test_undo_of_a_delete_undeletes_with_the_last_content(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    skill = _ok(_create(client, writer, aid), 201)
    patched = _ok(
        _patch(
            client,
            writer,
            aid,
            skill["id"],
            expected_revision=1,
            old_string="Tag the build",
            new_string="Tag the build twice",
        )
    )
    _ok(_delete(client, writer, aid, "cut-release", 2))
    undone = _ok(_revert(client, writer, aid, "cut-release", 3))
    assert undone["id"] == skill["id"]
    assert undone["deleted"] is False
    assert undone["revision"] == 4
    assert undone["body"] == patched["body"]
    assert [s["id"] for s in _list(client, writer, aid)["skills"]] == [skill["id"]]
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert (rev["action"], rev["reverted_to"]) == ("revert", 2)


def test_revert_to_an_explicit_revision_and_to_a_tombstone(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    _ok(_delete(client, auth_headers, aid, skill["id"], 1))  # r2: tombstone
    _ok(_revert(client, auth_headers, aid, skill["id"], 2))  # r3: back, content of r1
    _ok(
        _patch(
            client,
            auth_headers,
            aid,
            skill["id"],
            expected_revision=3,
            old_string="Tag the build",
            new_string="Tag A",
        )
    )  # r4
    _ok(
        _patch(
            client,
            auth_headers,
            aid,
            skill["id"],
            expected_revision=4,
            old_string="Tag A",
            new_string="Tag B",
        )
    )  # r5

    to_one = _ok(_revert(client, auth_headers, aid, skill["id"], 5, to_revision=1))
    assert to_one["revision"] == 6
    assert to_one["body"] == BODY
    assert to_one["deleted"] is False

    tomb = _ok(_revert(client, auth_headers, aid, skill["id"], 6, to_revision=2))
    assert tomb["revision"] == 7
    assert tomb["deleted"] is True
    assert _list(client, auth_headers, aid)["skills"] == []
    rev = _revisions(client, auth_headers, aid, skill["id"])[-1]
    assert (rev["action"], rev["reverted_to"]) == ("revert", 2)

    bad = _revert(client, auth_headers, aid, skill["id"], 7, to_revision=99)
    assert bad.status_code in (404, 422), bad.text
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 7


def test_undelete_is_held_to_the_cap(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    gone = _created(client, auth_headers, aid, "gone")
    _ok(_delete(client, auth_headers, aid, gone["id"], 1))
    made = [_created(client, auth_headers, aid, f"skill-{i:02d}") for i in range(CAP)]
    for headers in (auth_headers, writer):
        resp = _revert(client, headers, aid, gone["id"], 2)
        assert resp.status_code == 422, resp.text
        assert str(CAP) in _detail(resp)
        tomb = _revert(client, headers, aid, gone["id"], 2, to_revision=1)
        assert tomb.status_code == 422, tomb.text
    head = _ok(_show(client, auth_headers, aid, gone["id"]))
    assert head["deleted"] is True
    assert head["revision"] == 2
    _ok(_delete(client, auth_headers, aid, made[0]["id"], 1))
    back = _ok(_revert(client, writer, aid, gone["id"], 2))
    assert back["deleted"] is False


def test_undelete_into_an_active_slug_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    old = _created(client, auth_headers, aid, "deploy")
    _ok(_delete(client, auth_headers, aid, old["id"], 1))
    new = _created(client, auth_headers, aid, "deploy", body="New steps")
    for headers in (auth_headers, writer):
        resp = _revert(client, headers, aid, old["id"], 2)
        assert resp.status_code == 409, resp.text
    assert _ok(_show(client, auth_headers, aid, old["id"]))["deleted"] is True
    assert _ok(_show(client, auth_headers, aid, "deploy"))["id"] == new["id"]
    _ok(_delete(client, auth_headers, aid, new["id"], 1))
    back = _ok(_revert(client, writer, aid, old["id"], 2))
    assert back["deleted"] is False
    assert back["slug"] == "deploy"


# --- ref resolution (R13) ----------------------------------------------------------


def test_platform_reads_reach_the_most_recently_deleted_slug(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    first = _created(client, auth_headers, aid, "deploy", body="First steps")
    _ok(_delete(client, auth_headers, aid, first["id"], 1))
    second = _created(client, auth_headers, aid, "deploy", body="Second steps")
    _ok(_delete(client, auth_headers, aid, second["id"], 1))

    by_slug = _ok(_show(client, auth_headers, aid, "deploy"))
    assert by_slug["id"] == second["id"]
    assert by_slug["deleted"] is True
    by_id = _ok(_show(client, auth_headers, aid, first["id"]))
    assert by_id["deleted"] is True
    assert [r["action"] for r in _revisions(client, auth_headers, aid, "deploy")] == [
        "create",
        "delete",
    ]
    assert _revisions(client, auth_headers, aid, "deploy")[0]["body"] == "Second steps"

    # STATE reads never see a deleted skill, by slug or by id.
    for headers in (_reader(aid, channel), _writer(aid, channel)):
        for ref in ("deploy", first["id"], second["id"]):
            assert _show(client, headers, aid, ref).status_code == 404
            got = client.get(f"{_base(aid)}/{ref}/revisions", headers=headers)
            assert got.status_code == 404, got.text

    # Revert by slug (any caller) finds the most recently deleted one.
    back = _ok(_revert(client, _writer(aid, channel), aid, "deploy", 2))
    assert back["id"] == second["id"]
    assert back["body"] == "Second steps"


def test_an_active_slug_wins_over_a_deleted_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    old = _created(client, auth_headers, aid, "deploy")
    _ok(_delete(client, auth_headers, aid, old["id"], 1))
    new = _created(client, auth_headers, aid, "deploy", body="New steps")
    for headers in (auth_headers, _reader(aid, channel)):
        assert _ok(_show(client, headers, aid, "deploy"))["id"] == new["id"]
    # Undo by slug acts on the active skill (its create): it deletes it.
    undone = _ok(_revert(client, auth_headers, aid, "deploy", 1))
    assert undone["id"] == new["id"]
    assert undone["deleted"] is True


def test_operator_delete_then_operator_revert_by_slug(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, _ = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    _ok(_delete(client, auth_headers, aid, "cut-release", 1))
    back = _ok(_revert(client, auth_headers, aid, "cut-release", 2))
    assert back["id"] == skill["id"]
    assert back["deleted"] is False
    assert [s["slug"] for s in _list(client, auth_headers, aid)["skills"]] == ["cut-release"]


# --- authorization -----------------------------------------------------------------


def _every_route(client: Any, headers: dict[str, str], aid: str, ref: str) -> list[Any]:
    return [
        client.get(_base(aid), headers=headers),
        _show(client, headers, aid, ref),
        client.get(f"{_base(aid)}/{ref}/revisions", headers=headers),
        _create(client, headers, aid, "app-made"),
        _patch(client, headers, aid, ref, expected_revision=1, description="app wording"),
        _revert(client, headers, aid, ref, 1),
        _delete(client, headers, aid, ref, 1),
    ]


def test_app_token_is_refused_on_every_learned_skills_route(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    for ref in (skill["id"], "cut-release"):
        for resp in _every_route(client, _app(aid), aid, ref):
            assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
    head = _ok(_show(client, auth_headers, aid, skill["id"]))
    assert head["revision"] == 1
    assert [s["slug"] for s in _list(client, auth_headers, aid)["skills"]] == ["cut-release"]
    # Liveness: the reader and the operator still list.
    assert _list(client, _reader(aid, channel), aid)["skills"]


def test_a_token_for_another_agent_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    other, _ = _agent(client, auth_headers)
    _created(client, auth_headers, aid)
    resp = _create(client, _writer(other, channel), aid, "cross-agent")
    assert resp.status_code in (401, 403), resp.text
    assert client.get(_base(aid), headers=_reader(other)).status_code in (401, 403)


def _raw_calls(client: Any, headers: dict[str, str], aid: str, channel: str) -> list[Any]:
    calls = []
    for prefix in (f"/agents/{aid}/state", f"/agents/{aid}/state/bindings/slack/{channel}"):
        ns = f"{prefix}/learned_skills"
        calls += [
            client.get(ns, headers=headers),
            client.get(f"{ns}/cut-release", headers=headers),
            client.put(f"{ns}/cut-release", json={"value": {"body": "planted"}}, headers=headers),
            client.post(f"{ns}/log/append", json={"item": {"body": "planted"}}, headers=headers),
            client.delete(f"{ns}/cut-release", headers=headers),
        ]
    return calls


def test_raw_learned_skills_state_namespace_is_refused_to_sandbox_tokens(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    for headers in (_app(aid), _reader(aid, channel), _writer(aid, channel)):
        for resp in _raw_calls(client, headers, aid, channel):
            assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
            assert "learned_skills" in _detail(resp)
    # Nothing was stored.
    listed = client.get(f"/agents/{aid}/state/learned_skills", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert listed.json() == []
    # Liveness: the same tokens still use an ordinary namespace.
    for headers in (_app(aid), _writer(aid, channel)):
        put = client.put(f"/agents/{aid}/state/notes/k", json={"value": {"n": 1}}, headers=headers)
        assert put.status_code == 200, put.text
        assert client.get(f"/agents/{aid}/state/notes/k", headers=headers).status_code == 200


def test_broad_read_credential_reads_but_never_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    reader = _reader(aid, channel)
    assert [s["slug"] for s in _list(client, reader, aid)["skills"]] == ["cut-release"]
    assert _show(client, reader, aid, skill["id"]).status_code == 200
    assert _revisions(client, reader, aid, "cut-release")
    for resp in _every_route(client, reader, aid, skill["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: the per-turn credential writes.
    assert _create(client, _writer(aid, channel), aid, "app-made").status_code == 201


@pytest.mark.parametrize("sender", ["cron:nightly", "<no person>", "cron:"])
def test_a_scheduled_run_reads_but_cannot_change_skills(
    client: Any, auth_headers: dict[str, str], clean_db: None, sender: str
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    job = _writer(aid, channel, sender=sender)
    assert [s["slug"] for s in _list(client, job, aid)["skills"]] == ["cut-release"]
    assert _show(client, job, aid, "cut-release").status_code == 200
    assert _revisions(client, job, aid, "cut-release")
    for resp in _every_route(client, job, aid, skill["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
        assert SCHEDULED in _detail(resp)
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: a person in the same channel writes.
    assert _create(client, _writer(aid, channel), aid, "app-made").status_code == 201


def _continuation(aid: str, channel: str, *, scheduled: bool) -> dict[str, str]:
    """An approval continuation's credential: the approver is the sender, and
    ``scheduled`` (ADR 0248) says whether its chain started at a cron event."""
    claims = {
        "binding": f"slack:{channel}",
        "memory": "read",
        "sender": SENDER,
        "turn": _turn(),
        "conversation": CONVERSATION,
        "skills": "write",
        "scheduled": scheduled,
    }
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def test_a_scheduled_continuation_reads_but_cannot_change_skills(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    job = _continuation(aid, channel, scheduled=True)
    assert [s["slug"] for s in _list(client, job, aid)["skills"]] == ["cut-release"]
    for resp in _every_route(client, job, aid, skill["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
        assert SCHEDULED in _detail(resp)
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: the same person's continuation of their own turn writes.
    person = _continuation(aid, channel, scheduled=False)
    assert _create(client, person, aid, "app-made").status_code == 201


@pytest.mark.parametrize(
    "claims",
    [
        {"turn": _OMIT},
        {"turn": ""},
        {"conversation": _OMIT},
        {"conversation": ""},
        {"sender": _OMIT},
        {"sender": ""},
        {"skills": _OMIT},
        {"skills": "read"},
        {"skills": "WRITE"},
    ],
    ids=[
        "no-turn",
        "empty-turn",
        "no-conversation",
        "empty-conversation",
        "no-sender",
        "empty-sender",
        "no-skills",
        "skills-read",
        "skills-uppercase",
    ],
)
def test_a_write_credential_missing_a_claim_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, claims: dict[str, Any]
) -> None:
    aid, channel = _agent(client, auth_headers)
    skill = _created(client, auth_headers, aid)
    partial = _writer(aid, channel, **claims)
    for resp in _every_route(client, partial, aid, skill["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: the complete credential writes.
    assert _create(client, _writer(aid, channel), aid, "app-made").status_code == 201


def test_a_skills_write_credential_with_memory_read_cannot_write_memory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    writer = _writer(aid, channel)
    fact = "fact-" + "0123456789abcdef" * 2
    put = client.put(
        f"/agents/{aid}/state/bindings/slack/{channel}/memory/{fact}",
        json={
            "value": {
                "statement": "x",
                "author": SENDER,
                "stated_at": "2026-10-01T00:00:00+00:00",
                "session_id": "s",
            }
        },
        headers=writer,
    )
    assert put.status_code == 403, put.text
    assert "read-only" in _detail(put)
    assert _create(client, writer, aid).status_code == 201


def test_a_closed_turn_cannot_write_skills(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    turn = _turn()
    writer = _writer(aid, channel, turn=turn)
    skill = _ok(_create(client, writer, aid), 201)
    closed = client.post(
        CLOSE_URL,
        json={"agent_id": aid, "turn": turn},
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    assert closed.status_code == 204, closed.text
    for resp in _every_route(client, writer, aid, skill["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
        assert "turn" in _detail(resp)
    # Reads still work with the closed turn's credential.
    assert _list(client, writer, aid)["skills"]
    assert _ok(_show(client, auth_headers, aid, skill["id"]))["revision"] == 1
    # Liveness: an open turn in the same conversation writes.
    assert _create(client, _writer(aid, channel), aid, "app-made").status_code == 201


def test_toggle_off_hides_skills_from_state_and_limits_the_operator(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid, channel = _agent(client, auth_headers)
    keep = _created(client, auth_headers, aid, "keep")
    _created(client, auth_headers, aid, "spare")
    _toggle(client, auth_headers, aid, False)
    writer = _writer(aid, channel)
    reader = _reader(aid, channel)

    for headers in (writer, reader):
        assert _list(client, headers, aid) == {"enabled": False, "skills": []}
        shown = _show(client, headers, aid, keep["id"])
        assert shown.status_code in (403, 404), shown.text
        assert "keep" not in shown.text
    for resp in _every_route(client, writer, aid, keep["id"])[3:]:
        assert resp.status_code == 403, (resp.request.method, resp.request.url, resp.text)
        assert OFF in _detail(resp)

    # The operator still reads, but may not create or patch while off.
    listed = _list(client, auth_headers, aid)
    assert listed["enabled"] is False
    assert {s["slug"] for s in listed["skills"]} == {"keep", "spare"}
    assert _ok(_show(client, auth_headers, aid, "keep"))["revision"] == 1
    assert _revisions(client, auth_headers, aid, "keep")
    created = _create(client, auth_headers, aid, "new-one")
    assert created.status_code == 403, created.text
    assert OFF in _detail(created)
    patched = _patch(client, auth_headers, aid, "keep", expected_revision=1, description="x y")
    assert patched.status_code == 403, patched.text
    assert OFF in _detail(patched)

    # R16: operator delete and revert repair the store while it is off.
    deleted = _ok(_delete(client, auth_headers, aid, "spare", 1))
    assert deleted["deleted"] is True
    restored = _ok(_revert(client, auth_headers, aid, "spare", 2))
    assert restored["deleted"] is False
    assert _ok(_revert(client, auth_headers, aid, "keep", 1))["deleted"] is True
    assert {s["slug"] for s in _list(client, auth_headers, aid)["skills"]} == {"spare"}

    # Liveness: back on, the agent sees and writes again.
    _toggle(client, auth_headers, aid, True)
    on = _list(client, reader, aid)
    assert on["enabled"] is True
    assert [s["slug"] for s in on["skills"]] == ["spare"]
    assert _create(client, _writer(aid, channel), aid, "new-one").status_code == 201
