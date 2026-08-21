"""POST /agents conflict handling: a duplicate is a 409, not a 500.

Real Postgres round-trip (the disposable-DB conftest provisions and migrates a
throwaway database per run); name and repo_full_name are unique columns, so a
collision must surface as a caller conflict, not an opaque server error.

An agent's channel binding is the neutral `{kind, address}` object of ADR-0096
(#1459). Cardinality stopped being 1:1 in ADR-0116 (#1525), which amends
ADR-0089's "one agent still binds one channel" clause: a create still supplies
exactly ONE binding through the singular `channel` key, reads carry a
`channels` LIST ordered by `(kind, address)`, and every binding after the first
is written through the `/agents/{id}/channels` subresource -- covered by
`test_agent_channels_subresource.py`, not here. `AgentUpdate` carries no
binding key at all, which is why the retired shapes are refused below rather
than ignored.
"""

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from sqlalchemy import event
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine

# The committed, exported contract -- the artifact every generated client and
# every drift gate reads, not the in-process Pydantic model.
OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"


def _slack(address: str) -> dict[str, str]:
    """The Slack-kind binding literal, so a shape change lands in one place."""

    return {"kind": "slack", "address": address}


def _create(client: Any, headers: dict[str, str], **fields: Any) -> Any:
    return client.post("/agents", json=fields, headers=headers)


def _binding_row(agent_id: str) -> Any:
    """The agent's `agent_channels` row, read straight from Postgres.

    `generation` (ADR-0096 phase 2, D5/EB-A16) is deliberately NOT on the API's
    response shape -- it is a token-validation fact, not an operator-facing one
    -- so the only honest way to assert it is against the durable row. Follows
    `test_crud.py`'s fresh-engine-per-query pattern, which keeps the query off
    the TestClient's portal loop.
    """

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT kind, address, generation, endpoint, adapter "
                        "FROM curie.agent_channels WHERE agent_id = :aid"
                    ),
                    {"aid": agent_id},
                )
                return result.mappings().one()
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_duplicate_name_is_409(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    first = _create(client, auth_headers, name="dup-name", channel=_slack("C0AAAAAA1"))
    assert first.status_code == 201, first.text

    dup = _create(client, auth_headers, name="dup-name", channel=_slack("C0BBBBBB2"))
    assert dup.status_code == 409, dup.text
    assert dup.json()["detail"] == "an agent with that name already exists"


def test_two_agents_may_share_one_repository(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """ADR-0091: one repository builds many agents.

    This asserted a 409 until migration 0018. That constraint is what forced a
    repo wanting a dev bot AND a prod bot -- the same bundle on two channels,
    which is what a dev/prod split IS -- to create the second one out of band
    and carry deploy workflows to do it. Which agent a push deploys to is now
    answered by the bundle's `deploy.yaml`, not by the schema.
    """

    first = _create(
        client,
        auth_headers,
        name="repo-agent-a",
        channel={"kind": "slack", "address": "C0CCCCCC3"},
        repo_full_name="octo/shared-repo",
    )
    assert first.status_code == 201, first.text

    second = _create(
        client,
        auth_headers,
        name="repo-agent-b",
        channel={"kind": "slack", "address": "C0DDDDDD4"},
        repo_full_name="octo/shared-repo",
    )
    assert second.status_code == 201, second.text
    assert second.json()["repo_full_name"] == "octo/shared-repo"


def test_two_agents_may_not_share_one_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The deliberate asymmetry (#38). Sharing a repo is intended; sharing a
    channel is the silent-shadowing bug -- the worker resolves a channel to one
    agent, so the loser is deployed, healthy-looking, and never answering."""

    assert (
        _create(
            client,
            auth_headers,
            name="chan-agent-a",
            channel={"kind": "slack", "address": "C0EEEEEE5"},
            repo_full_name="octo/one-repo",
        ).status_code
        == 201
    )
    clash = _create(
        client,
        auth_headers,
        name="chan-agent-b",
        channel={"kind": "slack", "address": "C0EEEEEE5"},
        repo_full_name="octo/other-repo",
    )
    assert clash.status_code == 409, clash.text


def test_duplicate_address_is_409_on_create(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T3, create half. #38: one agent per address. A second agent on a taken
    address is refused at create time, rather than being accepted and then
    silently shadowed by the worker's resolver (which routes an address to
    exactly one agent).

    409 specifically, not "not 201": the failure mode the constraint rename
    invites is an unmapped constraint name, which turns this into an opaque 500
    and hands the operator nothing. The guidance is asserted for the same
    reason -- naming the fix IS the deliverable of #38's error map -- while
    "Slack" deliberately is NOT asserted, because the invariant now holds for
    every channel kind and a Slack-only message would misdescribe it.
    """

    first = _create(client, auth_headers, name="chan-agent-a", channel=_slack("C0EEEEEE5"))
    assert first.status_code == 201, first.text

    dup = _create(client, auth_headers, name="chan-agent-b", channel=_slack("C0EEEEEE5"))
    assert dup.status_code == 409, dup.text
    detail = dup.json()["detail"]
    assert "already bound" in detail, detail
    assert "move or delete" in detail, detail


def test_patch_onto_taken_address_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T3, move half. The write seam is fenced identically to create (#143's
    posture): the constraint cannot be sidestepped by creating on a free address
    and then moving onto a taken one. The seam is now the channels subresource
    (ADR-0116), so the move goes there; the 409 it must answer is unchanged."""

    first = _create(client, auth_headers, name="patch-chan-a", channel=_slack("C0FFFFFF6"))
    assert first.status_code == 201, first.text
    second = _create(client, auth_headers, name="patch-chan-b", channel=_slack("C0FFFFFF7"))
    assert second.status_code == 201, second.text

    moved = client.patch(
        f"/agents/{second.json()['id']}/channels",
        params={"kind": "slack", "address": "C0FFFFFF7"},
        json=_slack("C0FFFFFF6"),
        headers=auth_headers,
    )
    assert moved.status_code == 409, moved.text
    detail = moved.json()["detail"]
    assert "already bound" in detail, detail
    assert "move or delete" in detail, detail


# --- ADR-0096 / #1459: the channel-neutral binding ----------------------------


def test_a_non_slack_kind_binds_and_reads_back_through_the_api(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T1 / AC1, the acceptance criterion stated as a command.

    An agent binds a channel kind the platform has never heard of, with no
    schema change and no migration between the write and the read. `webhook` has
    no registered address shape, so it validates on the generic rule; that is
    exactly what makes "without schema changes" true rather than aspirational.

    The read-back asserts the binding surface is a LIST. ADR-0116 amends
    ADR-0089's singular clause, so an object here would satisfy "the binding
    round-trips" while making the second binding of #1525 unrepresentable in
    the response an operator and the console both read. The shape is asserted,
    not just the values, in both directions: a create still SENDS one object,
    and the read returns a one-element list.
    """

    created = _create(
        client,
        auth_headers,
        name="webhook-agent",
        channel={"kind": "webhook", "address": "acme-room-7"},
    )
    assert created.status_code == 201, created.text
    assert created.json()["channels"] == [{"kind": "webhook", "address": "acme-room-7"}]

    fetched = client.get(f"/agents/{created.json()['id']}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    bindings = fetched.json()["channels"]
    assert isinstance(bindings, list), bindings
    assert bindings == [{"kind": "webhook", "address": "acme-room-7"}]


def test_a_plural_channels_payload_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """CREATE binds exactly one channel, even under ADR-0116.

    Plural bindings arrive through `POST /agents/{id}/channels`, never through
    the create body: a create that names `channels` is not a partially-honored
    request, it is a caller describing a shape this endpoint has never had, and
    accepting it silently would create an agent with no binding at all -- which
    looks deployed and answers nothing, #38's exact failure mode.

    The guidance is asserted, not just the 422, and it is what changed with
    ADR-0116: the message must now point at the subresource, because "send the
    singular key instead" is only half the answer for an operator who genuinely
    wants two bindings and would otherwise read the 422 as a flat refusal.

    Two payloads, because they fail for different reasons: the first omits the
    required `channel` entirely, the second sends a list where an object belongs.
    """

    plural = _create(
        client,
        auth_headers,
        name="plural-agent",
        channels=[{"kind": "slack", "address": "C0EXAMPLE1"}],
    )
    assert plural.status_code == 422, plural.text
    # The new guidance: not "the plural surface does not exist" (it does now),
    # but "not on create -- bind one here and add the rest over there".
    assert "/channels" in plural.text, plural.text
    assert "channels is not an agent field" in plural.text, plural.text

    listed = _create(
        client,
        auth_headers,
        name="listed-agent",
        channel=[{"kind": "slack", "address": "C0EXAMPLE2"}],
    )
    assert listed.status_code == 422, listed.text


def test_the_slack_address_shape_check_survives_the_rename(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T2 / AC2. #143's regression: `--slack-channel '#name'` stored the literal
    name, reported success, and never routed, because the worker matches on the
    channel ID. The validator's actionable text IS the fix, so the assertion is
    on the guidance, not merely on the 422. A kind-dispatched validator that
    forgot to keep Slack's arm re-opens #143 while the status code stays green.
    """

    ok = _create(client, auth_headers, name="slack-ok", channel=_slack("C0123ABCD"))
    assert ok.status_code == 201, ok.text
    assert ok.json()["channels"] == [{"kind": "slack", "address": "C0123ABCD"}]

    bad = _create(client, auth_headers, name="slack-bad", channel=_slack("#general"))
    assert bad.status_code == 422, bad.text
    # The About tab and the /archives/ URL form are the guidance #143 shipped.
    assert "About tab" in bad.text, bad.text
    assert "archives/C0123ABCD" in bad.text, bad.text


def test_the_pair_is_identity_and_the_address_alone_is_not(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-A6 / AC5. The INVERSION PR 1 promised, now that the wire carries kind.

    This is the rewrite of `test_the_address_is_identity_and_the_kind_is_not`,
    which asserted that a second kind on a taken address was a 409. That was
    correct while the resolver saw only the address and could not have told the
    two rows apart. Phase 2's resolver routes on the pair (T-A4), so the
    constraint widens to `(kind, address)` in migration 0023 and the second kind
    is now legal. Rewritten, never deleted: this inversion is the observable
    proof that kind actually reached the wire rather than only the schema.

    Both halves are asserted in one test on purpose. Widening the constraint
    while forgetting to remap the API's 409 message (the constraint name changes
    from `agent_channels_address_key` to `agent_channels_kind_address_key`, and
    `routers/agents.py` keys its message map on that literal) turns #38's
    user-facing conflict into an opaque 500 -- and a test that only checked the
    new success case would never notice.
    """

    # A Slack-shaped address, because the slack binding has to pass the slack arm
    # of the write-time validator before any constraint can be reached.
    first = _create(client, auth_headers, name="kind-a", channel=_slack("C0EXAMPLE1"))
    assert first.status_code == 201, first.text

    # The widening: a DIFFERENT kind at the same address is a distinct route.
    other_kind = _create(
        client,
        auth_headers,
        name="kind-b",
        channel={"kind": "email", "address": "C0EXAMPLE1"},
    )
    assert other_kind.status_code == 201, other_kind.text
    assert other_kind.json()["channels"] == [{"kind": "email", "address": "C0EXAMPLE1"}]

    # And the pair itself is still identity: the SAME pair still conflicts, with
    # the guidance that names the fix (#38's error map), not a bare 500.
    dup_pair = _create(client, auth_headers, name="kind-c", channel=_slack("C0EXAMPLE1"))
    assert dup_pair.status_code == 409, dup_pair.text
    detail = dup_pair.json()["detail"]
    assert "already bound" in detail, detail
    assert "move or delete" in detail, detail

    # The PATCH seam is fenced identically -- the constraint cannot be sidestepped
    # by binding a free pair and then moving onto a taken one.
    free = _create(client, auth_headers, name="kind-d", channel=_slack("C0EXAMPLE2"))
    assert free.status_code == 201, free.text
    moved = client.patch(
        f"/agents/{free.json()['id']}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE2"},
        json=_slack("C0EXAMPLE1"),
        headers=auth_headers,
    )
    assert moved.status_code == 409, moved.text
    assert "already bound" in moved.json()["detail"]


def test_patching_a_binding_moves_it_rather_than_adding_a_second(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T4, rewritten for ADR-0116: the two verbs stay distinct.

    Adding a binding is `POST /agents/{id}/channels`; PATCH still MOVES the row
    it names and never appends. Collapsing the two would make the move silently
    additive, leaving the agent listening on an address the operator believed
    they had left -- and holding that pair against every other agent.

    The freed-pair assertion is the observable half: if the PATCH had appended,
    the create below would collide with the row it left behind.
    """

    created = _create(client, auth_headers, name="single-binding", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1"},
        json={"kind": "webhook", "address": "moved-here"},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["channels"] == [{"kind": "webhook", "address": "moved-here"}]

    # The move REPLACED the binding; the old address is now free for another
    # agent. If the PATCH had appended, this create would collide.
    reuse = _create(client, auth_headers, name="reuses-old", channel=_slack("C0EXAMPLE1"))
    assert reuse.status_code == 201, reuse.text

    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.json()["channels"] == [{"kind": "webhook", "address": "moved-here"}]


def test_every_rebind_bumps_the_generation_including_a_no_op_patch(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-A13 / AC13 (plan D5, EB-A16, finding 10).

    A `chn` ingress token claims `{channel_id, generation}`, not `(kind,
    address)`, because `update_channel_binding` mutates the binding row IN PLACE:
    without a generation, a token minted before a rebind stays valid against the
    row's NEW owner. The generation is what makes a rebind observable to a token
    that never saw it.

    The no-op PATCH is the load-bearing half, and the one an implementation gets
    wrong by writing `if channel.address != agent.channel.address`. A rebind to
    the SAME value is still a rebind for token purposes -- an operator
    re-asserting a binding is exactly the "I think something is wrong with this
    route" gesture that should invalidate outstanding credentials. Guarding the
    bump on a value change makes T-C8 pass on the moved case and silently fail on
    this one.
    """

    created = _create(client, auth_headers, name="gen-agent", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    fresh = _binding_row(agent_id)
    assert fresh["generation"] == 0, "a newly created binding starts at generation 0"

    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1"},
        json={"kind": "email", "address": "ops@example.test"},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    after_move = _binding_row(agent_id)
    assert after_move["generation"] == 1
    assert after_move["kind"] == "email"

    # A move that changes nothing at all still counts.
    same = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": "ops@example.test"},
        json={"kind": "email", "address": "ops@example.test"},
        headers=auth_headers,
    )
    assert same.status_code == 200, same.text
    assert _binding_row(agent_id)["generation"] == 2

    # An agent PATCH that does not touch the binding at all does NOT bump it:
    # the generation tracks rebinds, and bumping on every unrelated write would
    # invalidate live adapter tokens on a model change.
    unrelated = client.patch(
        f"/agents/{agent_id}", json={"model": "claude-sonnet-5"}, headers=auth_headers
    )
    assert unrelated.status_code == 200, unrelated.text
    assert _binding_row(agent_id)["generation"] == 2


def test_an_explicit_null_channel_is_rejected_on_patch(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Edge case E1, still 422 but for a stronger reason under ADR-0116.

    It used to be refused by a dedicated validator: `model` and `thinking` treat
    explicit null as "clear back to the platform default", and the binding
    deliberately did not follow that neighbouring convention, because there is
    no default binding to fall back to and a null would strand the agent --
    deployed, healthy-looking, unable to receive a turn.

    `AgentUpdate` now carries no binding key at all, so the null is refused as a
    retired key rather than as a null. Kept, not deleted: the OUTCOME an
    operator sees is the thing that must not regress, and `extra="ignore"` would
    turn this exact payload back into a 200 that changed nothing.
    """

    created = _create(client, auth_headers, name="null-channel", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    cleared = client.patch(f"/agents/{agent_id}", json={"channel": None}, headers=auth_headers)
    assert cleared.status_code == 422, cleared.text

    untouched = client.patch(
        f"/agents/{agent_id}", json={"model": "claude-sonnet-5"}, headers=auth_headers
    )
    assert untouched.status_code == 200, untouched.text
    assert untouched.json()["channels"] == [_slack("C0EXAMPLE1")]


def test_a_legacy_slack_channel_patch_is_rejected_not_silently_ignored(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The rename must break loudly for a caller that never got the memo.

    Pydantic's default `extra="ignore"` makes `{"slack_channel": "..."}` parse
    into an AgentUpdate with NOTHING set, so the PATCH returns 200 having done
    nothing at all. Every layer then agrees the move succeeded: the API says
    200, the operator's script exits 0, and the agent stays on its old address,
    answering in a channel nobody is watching. That is #38's silent-shadow
    failure re-entered through the write path, and a 200 is strictly worse than
    a 500 here because nothing anywhere reports it.

    A released CLI, a shell script, or a curl in a runbook is exactly this
    caller. The contract is one shape, and a request in the old shape is a
    contract violation, not a partial request.
    """

    created = _create(client, auth_headers, name="legacy-patch", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    legacy = client.patch(
        f"/agents/{agent_id}",
        json={"slack_channel": "C0EXAMPLE2"},
        headers=auth_headers,
    )
    assert legacy.status_code == 422, legacy.text

    # And the refusal was total: nothing moved, so a caller cannot read the
    # response as "partially applied" either.
    after = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert after.json()["channels"] == [_slack("C0EXAMPLE1")]


def test_a_singular_channel_patch_is_rejected_not_silently_ignored(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """D1's no-silently-accepted-retired-shape test, replacing the plural one.

    `channel` on `PATCH /agents/{id}` is the shape that was withdrawn by
    ADR-0116, and PATCH is where saying so matters. Pydantic's default
    `extra="ignore"` makes the retired key parse into an AgentUpdate with
    NOTHING set, so the request returns 200 having done nothing at all: every
    layer agrees the rebind succeeded (the API says 200, the operator's script
    exits 0) while the agent keeps answering on its old address. That is the #38
    silent-misroute failure, reached by a caller who read last release's docs.

    Deleting the field is therefore not enough on its own, and a grep for
    `AgentUpdate.channel` cannot prove this: the 422 has to be asserted, and it
    has to NAME the subresource, or the operator reads it as "my JSON was
    malformed" and retries the same shape.
    """

    created = _create(client, auth_headers, name="singular-patch", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]

    retired = client.patch(
        f"/agents/{agent_id}",
        json={"channel": {"kind": "slack", "address": "C0EXAMPLE2"}},
        headers=auth_headers,
    )
    assert retired.status_code == 422, retired.text
    body = retired.text
    assert "channel is no longer an agent field" in body, body
    assert "/channels" in body, body

    after = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert after.json()["channels"] == [_slack("C0EXAMPLE1")]


def test_a_legacy_slack_channel_create_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Create is fenced identically to PATCH (#143's posture).

    The second case is the dangerous one and the reason this is not just a
    missing-required-field test: `channel` present AND `slack_channel` present
    would otherwise be a clean 201 with the legacy key silently dropped. An
    operator migrating a manifest half-way gets an agent bound to whichever key
    the API happened to prefer, with no signal about which one lost.
    """

    instead = _create(client, auth_headers, name="legacy-create", slack_channel="C0EXAMPLE1")
    assert instead.status_code == 422, instead.text

    alongside = _create(
        client,
        auth_headers,
        name="legacy-both",
        channel=_slack("C0EXAMPLE2"),
        slack_channel="C0EXAMPLE3",
    )
    assert alongside.status_code == 422, alongside.text


def test_the_published_agent_update_has_no_channel_property_at_all() -> None:
    """A contract pin on the exported artifact, because the artifact IS the
    contract. Replaces the nullable-channel pin, which died with the field.

    The old test existed because the published `AgentUpdate.channel` advertised
    `null` as a value the server refused. ADR-0116 removes the field outright,
    and the same generated-client argument now runs the other way: generated
    clients and the CLI's field-parity gate read this file, not the Python, so a
    `channel` property left in the document hands their users a call that always
    422s -- and no runtime test catches it, because no runtime test sends the
    shape a generated client would.

    `AgentOut.channels` is asserted beside it, since a document that retired the
    write key while still publishing a singular read key would describe an API
    that never existed.

    A structural assertion on the JSON is the right shape ONLY here, where the
    published document is itself the deliverable. Everything else in this file
    asserts through HTTP.
    """

    schemas = json.loads(OPENAPI.read_text(encoding="utf-8"))["components"]["schemas"]
    update = schemas["AgentUpdate"]["properties"]

    assert "channel" not in update, (
        "openapi.json still publishes AgentUpdate.channel, but the API refuses "
        "that key with a 422 naming the /agents/{id}/channels subresource "
        f"(ADR-0116). Published properties: {sorted(update)}"
    )
    assert "channels" not in update, (
        "the binding is not an AgentUpdate field in either number; the plural "
        f"write surface is the subresource. Published properties: {sorted(update)}"
    )

    read = schemas["AgentOut"]["properties"]["channels"]
    assert read.get("type") == "array", read
    # Still the binding object, one element type: this cannot be satisfied by
    # loosening the read surface to a bare list of anything.
    assert json.dumps(read).count("#/components/schemas/ChannelBinding") == 1, read


@pytest.fixture
def statement_log() -> Iterator[list[str]]:
    """Every SQL statement the app issues, captured at the driver.

    Listens on the Engine CLASS rather than on an instance because the app builds
    its own engine inside its lifespan, which the TestClient owns. Used only to
    bound a query COUNT; nothing asserts on statement text.
    """

    captured: list[str] = []

    def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
        captured.append(statement)

    event.listen(Engine, "before_cursor_execute", _record)
    try:
        yield captured
    finally:
        event.remove(Engine, "before_cursor_execute", _record)


def test_the_binding_serializes_on_every_read_endpoint(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T14 / AC10, and it must hit HTTP, not `crud`.

    `AgentOut.model_validate` on a relationship that was never loaded raises
    under asyncio instead of lazy-loading. A crud-level test holds its own live
    session open and passes against exactly that bug; only the endpoint, whose
    session is already closed by the time the response model is built, provokes
    it. So all three read paths are exercised through the app: the create
    response, the list, and the by-id fetch.
    """

    created = _create(client, auth_headers, name="reader-a", channel=_slack("C0EXAMPLE1"))
    assert created.status_code == 201, created.text
    assert created.json()["channels"] == [_slack("C0EXAMPLE1")]
    agent_id = created.json()["id"]

    # A second binding, because the plural relationship is a DIFFERENT loading
    # problem from the singular one: an eager strategy that survives one row can
    # still raise, or truncate, on a collection.
    added = client.post(
        f"/agents/{agent_id}/channels", json=_slack("C0EXAMPLE2"), headers=auth_headers
    )
    assert added.status_code == 201, added.text

    listed = client.get("/agents", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert [a["channels"] for a in listed.json()] == [
        [_slack("C0EXAMPLE1"), _slack("C0EXAMPLE2")]
    ]

    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["channels"] == [_slack("C0EXAMPLE1"), _slack("C0EXAMPLE2")]


def test_listing_agents_does_not_issue_a_query_per_agent(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    statement_log: list[str],
) -> None:
    """T14's N+1 half / AC10.

    A relationship loaded per row is correct and unboundedly slow: the list
    endpoint would issue one extra query per agent, so a hundred-agent install
    pays a hundred round trips to render one page. Measured at two sizes and
    asserted EQUAL rather than against a magic number, so the test states the
    property (the cost does not grow with the number of agents) instead of
    pinning an implementation's exact query count, which a legitimate refactor
    could change.
    """

    def _cost(count: int, prefix: str) -> int:
        for index in range(count):
            resp = _create(
                client,
                auth_headers,
                name=f"{prefix}-{index}",
                channel=_slack(f"C0{prefix.upper()}{index:04d}"),
            )
            assert resp.status_code == 201, resp.text
        statement_log.clear()
        listed = client.get("/agents", headers=auth_headers)
        assert listed.status_code == 200, listed.text
        return sum(1 for s in statement_log if "agent_channels" in s)

    small = _cost(2, "few")
    large = _cost(6, "many")
    assert small == large, (
        f"listing 2 agents cost {small} agent_channels queries and listing 8 cost "
        f"{large}: the binding is being loaded per row (N+1), not eagerly"
    )


def test_agent_approval_required_tools_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Create with permission gates (#245); they come back on reads.
    created = client.post(
        "/agents",
        json={
            "name": "gated-agent",
            "channel": {"kind": "slack", "address": "C000000G01"},
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
                "channel": {"kind": "slack", "address": "C000000G02"},
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
            "channel": {"kind": "slack", "address": "C000000R01"},
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
                "channel": {"kind": "slack", "address": "C000000R05"},
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
            "channel": {"kind": "slack", "address": "C000000A01"},
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
            "channel": {"kind": "slack", "address": "C000000B01"},
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
                "channel": {"kind": "slack", "address": "C000000C01"},
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
                "channel": {"kind": "slack", "address": "C000000E01"},
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
        json={"name": "patch-unknown-key-agent", "channel": _slack("C000000F01")},
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
        json={"name": "patch-approvers-agent", "channel": _slack("C000000D01")},
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
            "channel": {"kind": "slack", "address": "C000000S01"},
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
            "channel": {"kind": "slack", "address": "C000000S02"},
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
            "channel": {"kind": "slack", "address": "C000000S03"},
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
            "channel": {"kind": "slack", "address": "C000000S04"},
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
            "channel": {"kind": "slack", "address": "C000000S05"},
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
            "channel": {"kind": "slack", "address": "C000000S05"},
            "secrets": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_x"},
        },
        headers=auth_headers,
    )
    assert ok.status_code == 201, ok.text
    assert ok.json()["secrets"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
