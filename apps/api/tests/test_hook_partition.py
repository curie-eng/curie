"""One thread per PARTITION of a hook, and a refusal when a delivery cannot name one.

Written before ``curie_api.hook_partition`` exists, so every import below is a
claim about the surface that module must expose. The sections map to the
acceptance criteria one to one:

* **A** -- a hook that does not opt in mints the id it minted before, byte- and
  SHA-256-identically, and its body is never parsed (AC1);
* **B** -- a partitioned hook mints one conversation per partition VALUE (AC2);
* **C** -- a misconfigured partitioned delivery refuses; it never falls back to
  the unpartitioned id (AC3);
* **D** -- the receipt (and the ingress log) names the thread the delivery landed
  on, which is the only way a caller learns the id it needs for a thread reset or
  the ``GET /approvals?conversation_id=`` filter (AC4);
* **E** -- the opt-in is operator-owned, round-trips, and clears (AC5).

**Posture, inherited from ``test_hooks.py``.** This route lets an outside system
make an agent act, so the refusals in section C deliberately outnumber the happy
paths. Each of them asserts THREE things rather than one -- the 422, that nothing
was enqueued, and that no delivery claim key was written -- because a refusal
that still took the claim would deduplicate away the upstream's retry of a
corrected payload, and a test asserting only the status code cannot tell a
refusal-before-the-claim from a refusal-after-it.

**Fall-back is the failure mode this file exists to forbid.** If a pointer does
not resolve, collapsing N intended threads into one would leave the operator
looking at a working hook that had quietly stopped fanning out. That is why the
negative assertions here (`no enqueue`, `no claim key`, `not the three-segment
id`) carry more of the contract than the positive ones.

The pure-function tests in the first section import nothing but
``curie_api.hook_partition`` and take no fixtures, so they run with the dev stack
down. Everything from the second section on needs the disposable database and
Valkey, exactly as ``test_hooks.py`` does.

The ``runs_stream`` / ``valkey`` / ``hooks_client`` fixtures are shared with
``test_hooks.py`` through ``conftest.py``. The plain helpers below are copied in
SHAPE from ``test_hooks.py`` rather than imported from it: the suite runs under
``--import-mode=importlib`` with no ``__init__.py`` in this directory, so one
test module cannot reliably import another by name.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import uuid
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from channel_protocol import hook_conversation_id as conversation_id
from curie_api.config import get_settings
from curie_api.delivery import sha16
from curie_api.hook_partition import (
    HOOK_NAME,
    PARTITION_VALUE,
    PartitionError,
    derive_partition,
    resolve_pointer,
    validate_pointer_syntax,
)
from curie_api.hook_signing import derive
from fastapi.testclient import TestClient

EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"

# A fixed example UUID for the pure-function tests. This repo is public: example
# agent ids are example UUIDs and never a real one.
EXAMPLE_AGENT = uuid.UUID("00000000-0000-4000-8000-000000000001")

# The hook-name corpus the byte-identity claim is made over: the 1-character
# boundary, the 63-character boundary, and one name per allowed character class.
HOOK_CORPUS = ("a", "a" * 63, "a.b-c_d", "0deploy", "x9._-", "sweep")


# =============================================================================
# Pure functions -- no database, no Valkey (sections A, B and edge case E5)
# =============================================================================


def test_the_hook_name_corpus_is_itself_inside_the_allowed_shape() -> None:
    """Guards the corpus, not the code.

    Every byte-identity assertion below is worthless if it is made over names the
    ingress would have refused at step 1, so the corpus is checked against the
    same pattern the route uses before it is used as evidence of anything.
    """

    for hook in HOOK_CORPUS:
        assert HOOK_NAME.fullmatch(hook), hook


def test_the_hook_name_shape_refuses_a_trailing_newline() -> None:
    """A regression guard for the ingress bug the end-to-end test in Section A
    also exercises.

    Python's `$` matches immediately BEFORE a trailing newline (not only at the
    true end of string), so a hook-name check written as `HOOK_NAME.match(hook)`
    against this `$`-anchored pattern would accept `"prs\\n"` -- and the name goes
    straight into the claim key, the event id, and the ingress log line Section D
    depends on. `fullmatch` closes it, because it requires the match to consume
    the whole string, trailing newline included; this pins the pattern side of
    that fix the way the corpus test above pins the accept side.
    """

    assert HOOK_NAME.fullmatch("prs\n") is None


def test_an_unpartitioned_hook_mints_the_id_it_minted_before() -> None:
    """AC1, and the highest-value assertion in this file.

    Compared against a literal f-string built HERE, never against a second
    production copy of the mint: a test that calls the same helper twice would
    stay green through a change that altered the string for every existing agent.
    The SHA-256 equality is the same claim stated at the artifact level -- every
    downstream key, label and hash derived from this id is untouched.
    """

    for hook in HOOK_CORPUS:
        expected = f"hook:{EXAMPLE_AGENT}:{hook}"

        assert conversation_id(EXAMPLE_AGENT, hook) == expected
        # The default and an explicit None must not diverge; the router passes a
        # derived `partition` that is None on the unpartitioned path.
        assert conversation_id(EXAMPLE_AGENT, hook, None) == expected
        assert (
            hashlib.sha256(conversation_id(EXAMPLE_AGENT, hook).encode()).hexdigest()
            == hashlib.sha256(expected.encode()).hexdigest()
        )
        assert expected.count(":") == 2, "an unpartitioned id has exactly three segments"


def test_a_partition_is_appended_as_a_fourth_segment_and_nothing_else() -> None:
    """AC2 at the unit level: the partition never rewrites the first three parts."""

    partitioned = conversation_id(EXAMPLE_AGENT, "prs", "42")

    assert partitioned == f"hook:{EXAMPLE_AGENT}:prs:42"
    assert partitioned.count(":") == 3
    # The prefix is preserved verbatim, which is what keeps a partitioned id
    # disjoint from the agent's Slack thread ids exactly as before.
    assert partitioned.startswith(f"{conversation_id(EXAMPLE_AGENT, 'prs')}:")


def test_an_unconfigured_hook_never_parses_the_body() -> None:
    """AC1's second half: the byte-identity path does no work on the payload.

    A body that is not JSON at all is the counterfactual. If `derive_partition`
    parsed first and consulted the map second, this raises instead of returning
    None, and every existing unconfigured hook would start refusing bodies it has
    always accepted.
    """

    not_json = b"not json at all"

    assert derive_partition(None, "issues", not_json) is None
    assert derive_partition({}, "issues", not_json) is None
    # A POPULATED map with no entry for THIS hook is the same path (edge case E8).
    assert derive_partition({"deploys": {"pointer": "/id"}}, "issues", not_json) is None


@pytest.mark.parametrize(
    ("pointer", "document", "expected"),
    [
        pytest.param("", {"a": 1}, {"a": 1}, id="empty-pointer-is-the-whole-document"),
        pytest.param("/a", {"a": 1}, 1, id="top-level-key"),
        pytest.param("/a/b", {"a": {"b": 2}}, 2, id="nested-key"),
        pytest.param("/items/0", {"items": ["x", "y"]}, "x", id="array-index-zero"),
        pytest.param("/items/10", {"items": list(range(11))}, 10, id="multi-digit-index"),
        pytest.param("/a~1b", {"a/b": "v"}, "v", id="tilde-one-decodes-to-slash"),
        pytest.param("/m~0n", {"m~n": "v"}, "v", id="tilde-zero-decodes-to-tilde"),
        # Edge case E11, the classic RFC 6901 bug: `~1` must be applied BEFORE
        # `~0`. Applied the other way round, `~01` becomes `~1` becomes `/`, and
        # this pointer silently reads the WRONG key -- which is a partition
        # derived from the wrong field, not an error anyone would see.
        pytest.param("/a~01b", {"a~1b": "right", "a/1b": "wrong"}, "right", id="escape-order"),
    ],
)
def test_resolve_pointer_follows_rfc_6901(pointer: str, document: Any, expected: Any) -> None:
    assert resolve_pointer(document, pointer) == expected


@pytest.mark.parametrize(
    ("pointer", "document"),
    [
        pytest.param("/missing", {"a": 1}, id="absent-key"),
        pytest.param("/items/5", {"items": [1, 2]}, id="index-past-the-end"),
        pytest.param("/a/b", {"a": 1}, id="descend-into-a-scalar"),
        pytest.param("/x", 42, id="scalar-document-root"),
        # A list index is digits with no leading zero (except "0" itself), so a
        # pointer cannot address one member two ways.
        pytest.param("/items/01", {"items": [1, 2]}, id="leading-zero-index"),
        pytest.param("/items/x", {"items": [1, 2]}, id="non-numeric-index"),
        pytest.param("/items/-1", {"items": [1, 2]}, id="negative-index"),
    ],
)
def test_resolve_pointer_refuses_rather_than_guessing(pointer: str, document: Any) -> None:
    """No `.get(..., default)` anywhere in here. A pointer that does not resolve
    is a configuration error, and returning a default would be the fall-back this
    whole feature refuses to do."""

    with pytest.raises((PartitionError, ValueError, KeyError, IndexError, TypeError)):
        resolve_pointer(document, pointer)


@pytest.mark.parametrize("pointer", ["", "/", "/n", "/a~1b", "/pull_request/number"])
def test_validate_pointer_syntax_accepts_a_well_formed_pointer(pointer: str) -> None:
    assert validate_pointer_syntax(pointer) == pointer


@pytest.mark.parametrize("pointer", ["number", "n/umber", "~0", " /n", "a"])
def test_validate_pointer_syntax_refuses_a_pointer_with_no_leading_slash(pointer: str) -> None:
    """The write surface rejects a malformed pointer at CONFIGURATION time, so an
    operator learns about it on the PATCH rather than on the first delivery that
    was supposed to run."""

    with pytest.raises(ValueError):
        validate_pointer_syntax(pointer)


@pytest.mark.parametrize("pointer", ["/a~0b", "/a~1b", "/a~01b", "", "/"])
def test_validate_pointer_syntax_accepts_every_well_formed_escape(pointer: str) -> None:
    """RFC 6901 defines exactly two escapes, `~0` and `~1`, and any digit may
    follow a literal `~1` or `~0` pair once escaped (`~01` is `~1` followed by a
    literal `1`, not a third escape) -- so a well-formed pointer built from them
    must still round-trip through syntax validation."""

    assert validate_pointer_syntax(pointer) == pointer


@pytest.mark.parametrize(
    "pointer",
    [
        pytest.param("/a~2b", id="tilde-followed-by-neither-0-nor-1"),
        pytest.param("/a~", id="trailing-bare-tilde"),
        pytest.param("/~", id="bare-tilde-segment"),
        pytest.param("/x/~", id="bare-tilde-in-a-later-segment"),
    ],
)
def test_validate_pointer_syntax_refuses_an_invalid_escape(pointer: str) -> None:
    """RFC 6901 permits only `~0` and `~1`. A bare or dangling `~` is a malformed
    pointer that can never resolve as the operator intended -- `resolve_pointer`'s
    `.replace("~1", ...).replace("~0", ...)` would leave the stray `~` (and
    whatever follows it) untouched in the lookup key, so this must be refused at
    CONFIGURATION time rather than surfacing as a confusing absent-key refusal on
    the first delivery."""

    with pytest.raises(ValueError):
        validate_pointer_syntax(pointer)


@pytest.mark.parametrize(
    "value",
    [
        "a",
        "9",
        "A",
        "a" * 63,
        "PR-42",
        "C0EXAMPLE1",
        "OPS.1_2",
        # A timestamp passes the bound. Deliberate, and NOT an endorsement: edge
        # case E3 records that the charset cannot detect an unstable identity, and
        # that the stability requirement is a documentation-and-review control.
        # A runtime heuristic here would refuse legitimate numeric ids.
        "1756425600",
    ],
)
def test_the_partition_bound_admits_a_stable_identity(value: str) -> None:
    assert PARTITION_VALUE.fullmatch(value), value


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("", id="empty"),
        pytest.param("a" * 64, id="one-past-the-63-char-bound"),
        pytest.param(".lead", id="leading-dot-could-build-a-dot-segment"),
        pytest.param("-lead", id="leading-dash"),
        pytest.param("_lead", id="leading-underscore"),
        pytest.param("a:b", id="colon-would-forge-a-segment"),
        pytest.param("a/b", id="slash-would-escape-a-url-path-segment"),
        pytest.param("a b", id="space"),
        pytest.param("a\nb", id="embedded-newline-would-forge-a-log-line"),
        pytest.param("ab\n", id="trailing-newline"),
        pytest.param("..", id="dot-dot"),
        pytest.param("a%2Fb", id="percent"),
        pytest.param("café", id="non-ascii-refuses-rather-than-transliterating"),
    ],
)
def test_the_partition_bound_refuses_everything_that_could_forge_a_key(value: str) -> None:
    """Checked with `fullmatch`, on purpose. `re.match` with a `$`-anchored
    pattern still admits a TRAILING newline, which would let a partition value
    forge a second line in the ingress log (edge case E1's log-forging row). The
    behavioural version of that claim is asserted through `derive_partition`
    below, so the module is free to spell it `fullmatch` or `\\Z`."""

    assert PARTITION_VALUE.fullmatch(value) is None, value


@pytest.mark.parametrize(
    ("body", "why"),
    [
        pytest.param(b'{"n": "ab\\n"}', "a trailing newline", id="trailing-newline"),
        pytest.param(b'{"n": true}', "a bool is an int in Python", id="bool-true"),
        pytest.param(b'{"n": false}', "a bool is an int in Python", id="bool-false"),
        pytest.param(b'{"n": -5}', "a negative int leads with a dash", id="negative-int"),
        pytest.param(b'{"n": 1.5}', "a float is not an identity", id="float"),
        pytest.param(b'{"n": null}', "null names nothing", id="null"),
    ],
)
def test_derive_partition_refuses_a_value_the_bound_excludes(body: bytes, why: str) -> None:
    """The behavioural form of the bound, kept separate from the pattern test.

    `True` is the one worth stating out loud: Python's bool IS an int, so a
    `str(value)` coercion written against the int case turns `true` into the
    partition `"True"` -- a plausible-looking string that silently becomes a
    thread. It must refuse.
    """

    config = {"prs": {"pointer": "/n"}}

    with pytest.raises(PartitionError) as raised:
        derive_partition(config, "prs", body)

    detail = str(raised.value)
    assert "prs" in detail and "/n" in detail, f"{why}: {detail}"


def test_derive_partition_coerces_an_int_but_returns_a_string() -> None:
    """AC2's coercion rule. A JSON number is the commonest stable identity there
    is (a PR number, an issue number), so it is accepted -- as `str(value)`, since
    the conversation id is a string and `42` and `"42"` must not become two
    threads for one pull request."""

    config = {"prs": {"pointer": "/number"}}

    assert derive_partition(config, "prs", b'{"number": 42}') == "42"
    assert derive_partition(config, "prs", b'{"number": "42"}') == "42"


def test_the_longest_representable_session_id_is_arithmetic_someone_re_checked() -> None:
    """Edge case E5: the length composition, pinned so the bound cannot drift.

    `binding.py` builds `CURIE_SESSION_ID` as `agent-<uuid>-thread-<thread key>`,
    and the conversation id is the last component of that thread key. Its own
    arithmetic at the bound:

        "hook:" 5 + uuid 36 + ":" 1 + hook 63 + ":" 1 + partition 63 = 169

    and the session id built directly from it:

        "agent-" 6 + uuid 36 + "-thread-" 8 + 169 = 219

    Stated as an equality rather than an inequality, because a `<=` against a
    round number invites someone to widen the round number instead of re-doing
    the arithmetic. NOTE for the reviewer: the plan's E5 estimated ~161 for this
    string; the arithmetic above says 219, and the worker's real thread key adds a
    percent-encoded `kind:address` prefix on top of that. Nothing here overflows a
    bound (the K8s claim name and `THREAD_HASH_LABEL` are fixed-length SHA
    prefixes), but the estimate is the number to correct, not this test.
    """

    longest = conversation_id(EXAMPLE_AGENT, "a" * 63, "b" * 63)
    session_id = f"agent-{EXAMPLE_AGENT}-thread-{longest}"

    assert len(longest) == 5 + 36 + 1 + 63 + 1 + 63
    assert len(longest) == 169
    assert len(session_id) == 6 + 36 + 8 + 169
    assert len(session_id) == 219


# =============================================================================
# Helpers (shape copied from test_hooks.py -- see the module docstring for why
# this file does not import them; the `runs_stream` / `valkey` / `hooks_client`
# fixtures are shared through conftest.py instead)
# =============================================================================


def _bind(
    client: TestClient,
    headers: dict[str, str],
    *,
    name: str,
    hook_partitions: dict[str, Any] | None = None,
) -> str:
    """Create an agent bound to an email channel and return its id.

    The `hook_partitions` keyword is the only difference from `test_hooks._bind`:
    the create surface is one of the two write paths AC5 covers, so a partitioned
    agent is built through it rather than through a direct column write.
    """

    payload: dict[str, Any] = {
        "name": name,
        "channel": {
            "kind": "email",
            "address": f"{name}@example.test",
            "endpoint": EMAIL_ENDPOINT,
            "adapter": EMAIL_ADAPTER,
        },
    }
    if hook_partitions is not None:
        payload["hook_partitions"] = hook_partitions
    created = client.post("/agents", json=payload, headers=headers)
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _secret_for(agent_id: str, generation: int = 0) -> str:
    return derive(get_settings().api_key, agent_id=agent_id, generation=generation)


def _post(
    client: TestClient,
    agent_id: str,
    hook: str,
    body: bytes,
    *,
    secret: str | None = None,
    signature: str | None = None,
    delivery_id: str | None = "dlv-1",
) -> Any:
    """POST one delivery, signing with `secret` unless a signature is forced."""

    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Curie-Signature-256"] = signature
    elif secret is not None:
        headers["X-Curie-Signature-256"] = _sign(secret, body)
    if delivery_id is not None:
        headers["X-Curie-Delivery-Id"] = delivery_id
    return client.post(f"/hooks/{agent_id}/{hook}", content=body, headers=headers)


def _queued(valkey: redis.Redis, stream: str) -> list[QueuedTurn]:
    entries = valkey.xrange(stream)
    return [QueuedTurn.model_validate_json(fields["payload"]) for _, fields in entries]


def _claim_key(agent_id: str, hook: str, delivery_id: str) -> str:
    """The delivery claim key `hooks.py` writes, rebuilt independently here.

    Edge case E9 pins that this key carries the agent, the hook and the delivery
    digest and deliberately NOT the partition: one upstream delivery id must run
    at most once, whatever partition it names. Rebuilt from `sha16` rather than
    read back from the route so a change to the key's shape shows up as a failing
    refusal test rather than as an assertion that quietly checks nothing.
    """

    return f"curie:hook:delivery:{agent_id}:{hook}:{sha16(delivery_id)}"


# =============================================================================
# Section A -- byte identity through the ingress (AC1)
# =============================================================================


def test_an_unconfigured_hook_enqueues_the_three_segment_id(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The unit claim, restated end to end. An agent that never opted in must
    reach the stream carrying the exact string it carried before this feature."""

    agent_id = _bind(hooks_client, auth_headers, name="unconfiguredagent")

    answer = _post(
        hooks_client,
        agent_id,
        "issues",
        b'{"number": 41}',
        secret=_secret_for(agent_id),
        delivery_id="a-1",
    )

    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:issues"
    assert turn.conversation_id.split(":") == ["hook", agent_id, "issues"]
    # The body carried number 41. A substring search on conversation_id is not
    # the proof: agent UUIDs are hex and can contain "41", which is what failed
    # on 4fc0d1c6. The segment list is the proof the payload was not read.


def test_a_hook_with_no_entry_in_a_populated_map_is_unpartitioned(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Configuring ONE hook must not change any other hook on the same agent.

    The body is deliberately not JSON at all: if the route parsed the body before
    consulting the map, this delivery would refuse, and every unconfigured hook on
    a partitioned agent would start rejecting payloads it used to accept.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="populatedmapagent",
        hook_partitions={"deploys": {"pointer": "/number"}},
    )

    answer = _post(
        hooks_client,
        agent_id,
        "issues",
        b"not json at all",
        secret=_secret_for(agent_id),
        delivery_id="a-2",
    )

    assert 200 <= answer.status_code < 300, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:issues"


def test_a_hook_name_with_a_trailing_newline_is_refused_at_ingress(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The end-to-end form of the `HOOK_NAME` regression pinned at the unit level
    above. The hook name is checked FIRST, before the agent row, the signature or
    the delivery id (the module docstring's step 1), because it is about to be
    used to build key names -- a name with a trailing newline could forge a
    second line in the claim key, the event id, or the ingress log line Section D
    depends on. The newline arrives percent-encoded in the path (`%0A`), the way
    an upstream URL-building library would send it.
    """

    agent_id = _bind(hooks_client, auth_headers, name="newlinehookagent")

    refused = _post(
        hooks_client,
        agent_id,
        "prs%0A",
        b'{"number": 41}',
        secret=_secret_for(agent_id),
        delivery_id="f-newline",
    )

    assert refused.status_code == 400, refused.text
    assert "hook name" in refused.json()["detail"], refused.json()["detail"]
    assert _queued(valkey, runs_stream) == []


# =============================================================================
# Section B -- partitioned id shape (AC2)
# =============================================================================


def test_a_partitioned_hook_mints_a_distinct_thread_per_value(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The whole point of the feature: one sweep finding three pull requests
    produces three threads, not three deliveries serialized into one sandbox."""

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="fanoutagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    for number in (41, 42, 43):
        answer = _post(
            hooks_client,
            agent_id,
            "prs",
            b'{"number": %d}' % number,
            secret=secret,
            delivery_id=f"b-{number}",
        )
        assert answer.status_code == 200, answer.text

    ids = [turn.conversation_id for turn in _queued(valkey, runs_stream)]
    assert ids == [f"hook:{agent_id}:prs:{n}" for n in (41, 42, 43)]
    assert len(set(ids)) == 3
    assert all(one.count(":") == 3 for one in ids)


def test_two_deliveries_naming_one_partition_share_a_thread(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The other half of AC2, and the half a fan-out-only test would miss.

    Two DIFFERENT deliveries about the SAME thing must land on one thread, or the
    partition is just a per-delivery id wearing a pointer, and intra-partition
    serialization -- the property ADR-0079 bought -- is gone.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="sameparttagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    first = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="b-same-1"
    )
    second = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="b-same-2"
    )

    assert first.status_code == 200 and second.status_code == 200
    one, two = _queued(valkey, runs_stream)
    # Two distinct events -- this is NOT the duplicate path.
    assert one.event_id != two.event_id
    assert one.conversation_id == two.conversation_id == f"hook:{agent_id}:prs:42"


@pytest.mark.parametrize(
    ("pointer", "body", "expected"),
    [
        pytest.param("/number", b'{"number": 42}', "42", id="int-coerced-to-str"),
        pytest.param("/number", b'{"number": "42"}', "42", id="string-value"),
        pytest.param(
            "/pull_request/number",
            b'{"pull_request": {"number": 42}, "number": 99}',
            "42",
            id="nested-and-not-the-decoy-at-the-root",
        ),
        pytest.param(
            "/items/0/key",
            b'{"items": [{"key": "OPS-7"}, {"key": "OPS-8"}]}',
            "OPS-7",
            id="array-index",
        ),
        pytest.param("/a~1b", b'{"a/b": "slashkey"}', "slashkey", id="tilde-one-is-a-slash"),
        pytest.param("/m~0n", b'{"m~n": "tildekey"}', "tildekey", id="tilde-zero-is-a-tilde"),
        pytest.param(
            "/a~01b",
            b'{"a~1b": "rightkey", "a/1b": "wrongkey"}',
            "rightkey",
            id="escape-order-e11",
        ),
    ],
)
def test_the_pointer_forms_an_operator_can_configure(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    pointer: str,
    body: bytes,
    expected: str,
) -> None:
    """Each case carries a decoy where one exists, so a resolver that reached the
    right value by the wrong route (reading the root, taking the first array
    member unconditionally, decoding `~0` before `~1`) fails here."""

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="pointeragent",
        hook_partitions={"prs": {"pointer": pointer}},
    )

    answer = _post(
        hooks_client, agent_id, "prs", body, secret=_secret_for(agent_id), delivery_id="b-ptr"
    )

    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:prs:{expected}"


def test_two_hooks_on_one_agent_partition_independently(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Edge case E8. The map is keyed by hook name, and a firing reads only its
    own entry -- so two hooks with different pointers cannot read each other's
    field, and a body shaped for one cannot refuse for the other."""

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="twopointeragent",
        hook_partitions={
            "prs": {"pointer": "/number"},
            "issues": {"pointer": "/issue/key"},
        },
    )
    secret = _secret_for(agent_id)

    _post(hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="b-h1")
    _post(
        hooks_client,
        agent_id,
        "issues",
        b'{"issue": {"key": "OPS-3"}}',
        secret=secret,
        delivery_id="b-h2",
    )

    ids = [turn.conversation_id for turn in _queued(valkey, runs_stream)]
    assert ids == [f"hook:{agent_id}:prs:42", f"hook:{agent_id}:issues:OPS-3"]


# =============================================================================
# Section C -- refusals (AC3): 422, no enqueue, no claim
# =============================================================================


@pytest.mark.parametrize(
    ("pointer", "body"),
    [
        pytest.param("/number", b"not json at all", id="body-is-not-json"),
        pytest.param("/number", b"", id="body-is-empty"),
        pytest.param("/x", b"42", id="json-scalar-at-the-document-root"),
        pytest.param("/x", b'["a", "b"]', id="json-array-at-the-document-root"),
        pytest.param("/number", b'{"other": 1}', id="pointer-names-an-absent-key"),
        pytest.param("/items/5", b'{"items": [1, 2]}', id="index-past-the-end"),
        pytest.param("/number", b'{"number": null}', id="value-is-null"),
        pytest.param("/number", b'{"number": {"a": 1}}', id="value-is-a-nested-object"),
        pytest.param("/number", b'{"number": [1]}', id="value-is-a-list"),
        pytest.param("/number", b'{"number": 1.5}', id="value-is-a-float"),
        # Python's `True` IS an int. A `str(value)` coercion written for the int
        # case turns this into the partition "True" -- a real-looking thread that
        # no operator configured. It must refuse, not coerce.
        pytest.param("/number", b'{"number": true}', id="value-is-a-bool"),
        pytest.param("/number", b'{"number": -5}', id="value-is-a-negative-int"),
        pytest.param("/number", b'{"number": "a:b"}', id="value-contains-a-colon"),
        pytest.param("/number", b'{"number": "a/b"}', id="value-contains-a-slash"),
        pytest.param("/number", b'{"number": "ab\\n"}', id="value-ends-in-a-newline"),
        pytest.param("/number", b'{"number": ""}', id="value-is-the-empty-string"),
        pytest.param(
            "/number",
            b'{"number": "' + b"a" * 64 + b'"}',
            id="value-is-64-chars-one-past-the-bound",
        ),
        pytest.param("/number", b'{"number": ".lead"}', id="value-starts-with-a-dot"),
        pytest.param("/number", b'{"number": "-lead"}', id="value-starts-with-a-dash"),
        pytest.param("/number", b'{"number": "_lead"}', id="value-starts-with-an-underscore"),
    ],
)
def test_a_delivery_that_cannot_name_its_partition_is_refused(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    pointer: str,
    body: bytes,
) -> None:
    """AC3, one case per failure mode, and three assertions per case.

    The status code alone proves almost nothing here. What must hold is that the
    refusal happened BEFORE the claim: a refused delivery that took its claim key
    would make the upstream's retry of a CORRECTED payload look like a duplicate
    and drop it silently, which is the same silent-degradation failure the 422
    exists to avoid in the first place.

    The detail names both the hook and the pointer because the operator's only
    other signal is a 422 on a route they do not control the caller of.
    """

    delivery_id = "c-refused"
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="refusalagent",
        hook_partitions={"prs": {"pointer": pointer}},
    )

    refused = _post(
        hooks_client,
        agent_id,
        "prs",
        body,
        secret=_secret_for(agent_id),
        delivery_id=delivery_id,
    )

    assert refused.status_code == 422, refused.text
    detail = refused.json()["detail"]
    assert isinstance(detail, str), detail
    assert "prs" in detail, detail
    assert pointer in detail, detail
    assert _queued(valkey, runs_stream) == []
    assert not valkey.exists(_claim_key(agent_id, "prs", delivery_id)), (
        "the refusal took the delivery claim, so a corrected retry of this same "
        "delivery id would be deduplicated away and never run"
    )


def test_a_corrected_retry_of_a_refused_delivery_still_runs(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The counterfactual for the no-claim assertion above, stated as behaviour.

    Same delivery id, same hook, same agent: only the body is fixed. If the
    refusal had claimed, this second request answers `duplicate: true` and
    enqueues nothing, and the operator's fix never takes effect.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="correctedagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    refused = _post(
        hooks_client, agent_id, "prs", b'{"wrong": 1}', secret=secret, delivery_id="c-retry"
    )
    assert refused.status_code == 422, refused.text
    assert _queued(valkey, runs_stream) == []

    corrected = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="c-retry"
    )

    assert corrected.status_code == 200, corrected.text
    assert corrected.json()["duplicate"] is False
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:prs:42"


def test_an_unsigned_delivery_to_a_partitioned_hook_answers_401_not_422(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The partition check runs AFTER signature verification, so an unsigned
    caller learns nothing about the hook's configuration.

    A 422 naming the pointer here would tell an unauthenticated caller which
    field of its payload the operator reads -- and would confirm that this hook is
    partitioned at all. The detail must be the shared auth string, byte for byte.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="unsignedpartagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )

    refused = _post(hooks_client, agent_id, "prs", b"not json at all", signature=None)

    assert refused.status_code == 401, refused.text
    assert refused.json()["detail"] == "missing or invalid signature"
    assert _queued(valkey, runs_stream) == []


def test_a_delivery_with_no_id_to_a_partitioned_hook_answers_400_not_422(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The other half of the ordering claim: the partition check also runs after
    the delivery-id check, so a caller with no `X-Curie-Delivery-Id` is told about
    the header it is missing rather than about a pointer it cannot fix."""

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="noidpartagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )

    refused = _post(
        hooks_client,
        agent_id,
        "prs",
        b"not json at all",
        secret=_secret_for(agent_id),
        delivery_id=None,
    )

    assert refused.status_code == 400, refused.text
    assert "X-Curie-Delivery-Id" in refused.json()["detail"]
    assert _queued(valkey, runs_stream) == []


def test_a_refused_partition_never_falls_back_to_the_unpartitioned_thread(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The negative-space assertion the whole 422 decision rests on.

    A fall-back would look like success: a 200, a turn on the stream, the agent
    running. The only visible symptom would be that N intended threads had
    quietly collapsed into one. This asserts the ABSENCE of the three-segment id,
    which no status-code assertion anywhere else in this file covers.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="nofallbackagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )

    refused = _post(
        hooks_client,
        agent_id,
        "prs",
        b'{"wrong_field": 1}',
        secret=_secret_for(agent_id),
        delivery_id="c-fallback",
    )

    assert refused.status_code == 422, refused.text
    threads = [turn.conversation_id for turn in _queued(valkey, runs_stream)]
    assert threads == []
    assert f"hook:{agent_id}:prs" not in threads


def test_deeply_nested_json_is_refused_as_422_not_500(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """A pathological body must refuse cleanly, not crash the ingress.

    `json.loads` raises `RecursionError` on JSON nested past Python's recursion
    limit, and `RecursionError` is a `RuntimeError`, not a `ValueError` -- so the
    `except ValueError` in `derive_partition`'s parse step does not catch it. Left
    uncaught, this reaches the ASGI server as an unhandled 500, which is strictly
    worse than every other malformed body in this section: it also skips the
    no-enqueue, no-claim guarantee this file otherwise proves for every refusal.
    The body is 200 KB, comfortably under `hook_max_body_bytes` (1 MiB), so the
    size bound cannot be the thing doing the refusing here.
    """

    delivery_id = "c-deepnest"
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="deepnestagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )

    body = b"[" * 100_000 + b"]" * 100_000

    refused = _post(
        hooks_client,
        agent_id,
        "prs",
        body,
        secret=_secret_for(agent_id),
        delivery_id=delivery_id,
    )

    assert refused.status_code == 422, refused.text
    detail = refused.json()["detail"]
    assert isinstance(detail, str), detail
    assert "prs" in detail, detail
    assert "/number" in detail, detail
    assert _queued(valkey, runs_stream) == []
    assert not valkey.exists(_claim_key(agent_id, "prs", delivery_id)), (
        "the refusal took the delivery claim, so a corrected retry of this same "
        "delivery id would be deduplicated away and never run"
    )


# =============================================================================
# Section D -- the receipt and the ingress log (AC4)
# =============================================================================


def test_the_receipt_names_the_thread_the_delivery_landed_on(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The caller of a partitioned hook has no other way to learn its four-segment
    id, and it needs one for `POST /control/threads/{key}/reset` and for the
    `GET /approvals?conversation_id=` filter. Asserted against the ENQUEUED turn,
    not against a second f-string, so a receipt built from a different mint fails.
    """

    partitioned = _bind(
        hooks_client,
        auth_headers,
        name="receiptpartagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    plain = _bind(hooks_client, auth_headers, name="receiptplainagent")

    one = _post(
        hooks_client,
        partitioned,
        "prs",
        b'{"number": 42}',
        secret=_secret_for(partitioned),
        delivery_id="d-1",
    )
    two = _post(
        hooks_client, plain, "issues", b"{}", secret=_secret_for(plain), delivery_id="d-2"
    )

    assert one.status_code == 200 and two.status_code == 200, (one.text, two.text)
    first, second = _queued(valkey, runs_stream)
    assert one.json()["conversation_id"] == first.conversation_id
    assert one.json()["conversation_id"] == f"hook:{partitioned}:prs:42"
    assert two.json()["conversation_id"] == second.conversation_id
    assert two.json()["conversation_id"] == f"hook:{plain}:issues"


def test_a_retried_delivery_reports_the_same_conversation_id(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The duplicate path carries the id too.

    An upstream that only ever sees the retry -- because its first attempt timed
    out on the wire -- would otherwise never learn the thread its delivery is on.
    Only one turn is enqueued, so the second receipt cannot be reading it off a
    turn it just minted; it has to derive the id the same way the first did.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="dupreceiptagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)
    body = b'{"number": 42}'

    first = _post(hooks_client, agent_id, "prs", body, secret=secret, delivery_id="d-dup")
    second = _post(hooks_client, agent_id, "prs", body, secret=secret, delivery_id="d-dup")

    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True
    assert second.json()["conversation_id"] == first.json()["conversation_id"]
    assert second.json()["conversation_id"] == f"hook:{agent_id}:prs:42"
    assert len(_queued(valkey, runs_stream)) == 1


def test_a_duplicate_receipt_names_the_thread_the_delivery_landed_on(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The duplicate path must report where the delivery ACTUALLY landed, not a
    thread recomputed from whatever body the retry happens to carry.

    An upstream retry of the SAME delivery id can legitimately carry a DIFFERENT
    body -- a webhook re-signed after the PR moved on, a payload the sender
    regenerated -- but the delivery already ran once and landed on a thread.
    Recomputing the partition from the retry's body would answer with a thread
    this delivery never touched, and the operator would reset or filter the wrong
    one.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="dupthreadagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    first = _post(
        hooks_client, agent_id, "prs", b'{"number": 41}', secret=secret, delivery_id="d-retry"
    )
    assert first.status_code == 200, first.text
    assert first.json()["duplicate"] is False
    assert first.json()["conversation_id"] == f"hook:{agent_id}:prs:41"

    second = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="d-retry"
    )

    assert second.status_code == 200, second.text
    assert second.json()["duplicate"] is True
    assert second.json()["conversation_id"] == f"hook:{agent_id}:prs:41", (
        "the retry's body named partition 42, but this delivery already landed "
        "on 41 -- the receipt must name the thread it landed on, not the one its "
        "retry's body would derive"
    )
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:prs:41"


def test_a_duplicate_receipt_ignores_a_partition_config_change_after_it_landed(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Same claim, a sharper angle: the operator can repoint the hook entirely
    between the two deliveries. The retry must still answer with the thread the
    FIRST delivery landed on, derived under the OLD config -- not one recomputed
    under the new pointer, which would read a different field of the SAME retry
    body and could name a different partition entirely.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="dupconfigagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    first = _post(
        hooks_client, agent_id, "prs", b'{"number": 41}', secret=secret, delivery_id="d-cfg"
    )
    assert first.status_code == 200, first.text
    assert first.json()["conversation_id"] == f"hook:{agent_id}:prs:41"

    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {"prs": {"pointer": "/other"}}},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text

    second = _post(
        hooks_client,
        agent_id,
        "prs",
        b'{"number": 41, "other": "zz"}',
        secret=secret,
        delivery_id="d-cfg",
    )

    assert second.status_code == 200, second.text
    assert second.json()["duplicate"] is True
    assert second.json()["conversation_id"] == f"hook:{agent_id}:prs:41"
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:prs:41"


def test_the_ingress_log_names_the_conversation_id(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The operator's only server-side record of which thread a delivery landed on.

    There is no fan-out reset verb, so an operator resets a partition by its full
    four-segment id -- and this log line plus the receipt are the two places that
    id is ever shown. A log line that named only the hook would leave an operator
    with N live threads and no way to enumerate them.

    `caplog.handler` is attached to the service logger BY HAND. `configure_logging`
    sets `propagate = False` on `curie_api` (deliberately, so records are not
    double-emitted through whatever uvicorn and the OTel wiring put on root), and
    pytest installs its capture handler on the ROOT logger only -- so the obvious
    `with caplog.at_level(...)` form captures nothing here and the assertion below
    would fail for a reason that has nothing to do with the log line.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="logagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    service_logger = logging.getLogger("curie_api")
    service_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.INFO, logger="curie_api"):
            answer = _post(
                hooks_client,
                agent_id,
                "prs",
                b'{"number": 42}',
                secret=_secret_for(agent_id),
                delivery_id="d-log",
            )
    finally:
        service_logger.removeHandler(caplog.handler)

    assert answer.status_code == 200, answer.text
    expected = f"hook:{agent_id}:prs:42"
    lines = [
        record.getMessage()
        for record in caplog.records
        if record.name == "curie_api.routers.hooks"
    ]
    assert any(f"conversation_id={expected}" in line for line in lines), lines


# =============================================================================
# Section E -- the write surface round-trip (AC5)
# =============================================================================


def test_the_partition_map_round_trips_through_create_and_get(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Create is one of the two write paths, and GET is the only read path."""

    configured = {"prs": {"pointer": "/number"}, "issues": {"pointer": "/issue/key"}}
    agent_id = _bind(
        hooks_client, auth_headers, name="roundtripagent", hook_partitions=configured
    )

    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)

    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["hook_partitions"] == configured


def test_an_agent_created_without_the_map_reads_back_null(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The default is NULL, not an empty object: NULL is the unpartitioned
    posture, and every pre-existing agent row has it."""

    agent_id = _bind(hooks_client, auth_headers, name="defaultnullagent")

    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)

    assert fetched.json()["hook_partitions"] is None


def test_a_post_with_an_empty_map_persists_null(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """CREATE's version of edge case E10. `test_clearing_the_map_returns_the_hook_to_one_thread`
    proves the rule on PATCH; this proves the SAME rule on the other write path,
    so an operator who passes an explicit `{}` on `POST /agents` -- rather than
    omitting the key entirely -- gets the identical unpartitioned posture, both in
    the stored column and in what the very next firing mints.
    """

    agent_id = _bind(hooks_client, auth_headers, name="createemptymapagent", hook_partitions={})

    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["hook_partitions"] is None, "an empty map on create must persist as NULL"

    answer = _post(
        hooks_client,
        agent_id,
        "issues",
        b'{"number": 41}',
        secret=_secret_for(agent_id),
        delivery_id="e-createempty",
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["conversation_id"] == f"hook:{agent_id}:issues"
    (turn,) = _queued(valkey, runs_stream)
    assert turn.conversation_id == f"hook:{agent_id}:issues"


def test_a_patch_that_omits_the_map_leaves_it_unchanged(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Omitted is not cleared. A PATCH that renames an agent must not silently
    return every one of its hooks to a single thread."""

    configured = {"prs": {"pointer": "/number"}}
    agent_id = _bind(
        hooks_client, auth_headers, name="omitpatchagent", hook_partitions=configured
    )

    patched = hooks_client.patch(
        f"/agents/{agent_id}", json={"model": "example-model"}, headers=auth_headers
    )

    assert patched.status_code == 200, patched.text
    assert patched.json()["hook_partitions"] == configured
    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.json()["hook_partitions"] == configured


def test_a_patch_replaces_the_map_wholesale(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Replacement, not a merge. A PATCH naming only `issues` must drop `prs`, or
    an operator could never remove a partition without clearing everything."""

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="replacepatchagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )

    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {"issues": {"pointer": "/issue/key"}}},
        headers=auth_headers,
    )

    assert patched.status_code == 200, patched.text
    assert patched.json()["hook_partitions"] == {"issues": {"pointer": "/issue/key"}}
    assert "prs" not in (patched.json()["hook_partitions"] or {})


def test_clearing_the_map_returns_the_hook_to_one_thread(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Edge case E10, and the BEHAVIOUR is the assertion, not the column.

    An explicit `{}` clears; it is stored as NULL, so the GET answers `null`
    rather than `{}`. Asserting only the column would leave the clear path
    untested where it matters -- what the operator is buying is that the very next
    firing goes back to the three-segment id.
    """

    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="clearagent",
        hook_partitions={"prs": {"pointer": "/number"}},
    )
    secret = _secret_for(agent_id)

    before = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="e-1"
    )
    assert before.json()["conversation_id"] == f"hook:{agent_id}:prs:42"

    cleared = hooks_client.patch(
        f"/agents/{agent_id}", json={"hook_partitions": {}}, headers=auth_headers
    )

    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["hook_partitions"] is None, "an empty map must persist as NULL"
    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.json()["hook_partitions"] is None

    after = _post(
        hooks_client, agent_id, "prs", b'{"number": 42}', secret=secret, delivery_id="e-2"
    )

    assert after.status_code == 200, after.text
    assert after.json()["conversation_id"] == f"hook:{agent_id}:prs"
    threads = [turn.conversation_id for turn in _queued(valkey, runs_stream)]
    assert threads == [f"hook:{agent_id}:prs:42", f"hook:{agent_id}:prs"]


@pytest.mark.parametrize(
    ("key", "token"),
    [
        pytest.param("Issues", "Issues", id="uppercase"),
        pytest.param("has:colon", "has:colon", id="colon-would-forge-a-segment"),
        pytest.param("has/slash", "has/slash", id="slash"),
        pytest.param("has space", "has space", id="space"),
        pytest.param(".leading", ".leading", id="leading-dot"),
        pytest.param("", "hook_partitions", id="empty-key"),
        pytest.param("x" * 64, "x" * 64, id="64-chars-one-past-the-bound"),
    ],
)
def test_a_key_outside_the_hook_name_shape_is_refused_on_both_write_paths(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    key: str,
    token: str,
) -> None:
    """A configured hook name and a fired hook name cannot be allowed to disagree.

    A key the ingress would refuse at step 1 can never match a firing, so it
    configures nothing while looking configured -- the operator sees a partition
    map and gets unpartitioned threads. Both write paths are checked: a rule
    enforced only on create leaks in through PATCH.
    """

    created = hooks_client.post(
        "/agents",
        json={
            "name": "badkeyagent",
            "channel": {
                "kind": "email",
                "address": "badkeyagent@example.test",
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
            "hook_partitions": {key: {"pointer": "/number"}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text
    assert token in created.text, created.text

    agent_id = _bind(hooks_client, auth_headers, name="badkeypatchagent")
    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {key: {"pointer": "/number"}}},
        headers=auth_headers,
    )
    assert patched.status_code == 422, patched.text
    assert token in patched.text, patched.text


@pytest.mark.parametrize(
    "pointer",
    [
        pytest.param("number", id="no-leading-slash"),
        pytest.param("pull_request/number", id="path-with-no-leading-slash"),
        pytest.param("~0", id="escape-with-no-leading-slash"),
    ],
)
def test_a_malformed_pointer_is_refused_at_configuration_time(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None, pointer: str
) -> None:
    """Refused on the write, not on the first delivery.

    A pointer that can never resolve is an operator error, and the operator is at
    the keyboard during the PATCH. Deferring it to ingress means the failure
    surfaces as a 422 to a third-party upstream nobody at this end is watching.
    """

    agent_id = _bind(hooks_client, auth_headers, name="badpointeragent")

    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {"prs": {"pointer": pointer}}},
        headers=auth_headers,
    )

    assert patched.status_code == 422, patched.text
    assert pointer in patched.text, patched.text


def test_an_invalid_escape_pointer_is_refused_on_both_write_paths(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """RFC 6901 permits only the two escapes `~0` and `~1`. A bare or dangling `~`
    can never resolve as the operator intended, so it must be refused at
    CONFIGURATION time on both write paths, the same as any other malformed
    pointer shape in this section.
    """

    created = hooks_client.post(
        "/agents",
        json={
            "name": "badescapeagent",
            "channel": {
                "kind": "email",
                "address": "badescapeagent@example.test",
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
            "hook_partitions": {"prs": {"pointer": "/a~2b"}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text

    agent_id = _bind(hooks_client, auth_headers, name="badescapepatchagent")
    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {"prs": {"pointer": "/a~"}}},
        headers=auth_headers,
    )

    assert patched.status_code == 422, patched.text
    assert "/a~" in patched.text, patched.text


def test_an_unknown_key_inside_the_config_object_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Pins `extra="forbid"` on the config model.

    A typo'd key that is silently dropped leaves the operator believing a
    configuration is active that is not -- the same reason `ApprovalApprovers`
    forbids extras. Here the dropped key would be the whole partition, and the
    hook would run unpartitioned while its config looked right in a GET.
    """

    agent_id = _bind(hooks_client, auth_headers, name="extrakeyagent")

    patched = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"hook_partitions": {"prs": {"pointer": "/n", "poitner": "/m"}}},
        headers=auth_headers,
    )

    assert patched.status_code == 422, patched.text
    assert "poitner" in patched.text, patched.text
