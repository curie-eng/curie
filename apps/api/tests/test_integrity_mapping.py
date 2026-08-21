"""Unit coverage for the IntegrityError -> 409 classifier.

`create_agent`'s current except-clause forces every IntegrityError to a 409
and picks the message by substring-matching `str(exc.orig)` -- that also
mislabels a NOT NULL / FK violation as a uniqueness conflict. The intended
fix is a small pure helper, `classify_integrity_error`, that inspects the
driver exception's structured fields instead of the stringified message and
only ever classifies a real unique violation as a 409.

Expected contract (not yet implemented -- this file is RED until it lands):

    def classify_integrity_error(exc: IntegrityError) -> tuple[int, str] | None:
        ...

Returns `(409, message)` when `exc.orig` is a unique violation on one of the
two real unique constraints (`agents_name_key`, `ix_agents_repo_full_name`)
or, for a unique violation on some other constraint, a generic message.
Returns `None` when `exc.orig` is not a unique violation at all (e.g. NOT
NULL or FK), signalling the caller should re-raise / let it surface as a 500.

This app talks to Postgres over asyncpg (`apps/api/src/curie_api/config.py`,
`postgresql+asyncpg://`), not psycopg. asyncpg's `PostgresError` subclasses
expose the wire-protocol error fields as plain attributes on the exception
instance -- `sqlstate` and `constraint_name` -- there is no psycopg-style
`.diag` namespace. See asyncpg's `PostgresMessageMeta._field_map`. The fakes
below mimic that real shape so the unit test pins the actual attribute path
the implementation must read.
"""

from typing import Any

from curie_api.routers.agents import _UNIQUE_CONSTRAINT_MESSAGES, classify_integrity_error
from sqlalchemy.exc import IntegrityError

# Real Postgres SQLSTATE codes (see apps/api/tests conftest for the live
# equivalents these mirror): unique_violation, not_null_violation,
# foreign_key_violation.
_UNIQUE_VIOLATION = "23505"
_NOT_NULL_VIOLATION = "23502"
_FOREIGN_KEY_VIOLATION = "23503"


class _FakePgError:
    """Mimics an asyncpg PostgresError: sqlstate/constraint_name are plain
    attributes on the exception instance, not nested under `.diag`."""

    def __init__(self, sqlstate: str, constraint_name: str | None = None) -> None:
        self.sqlstate = sqlstate
        self.constraint_name = constraint_name


def _integrity_error(sqlstate: str, constraint_name: str | None) -> Any:
    return IntegrityError("INSERT ...", {}, _FakePgError(sqlstate, constraint_name))


class _FakeAsyncpgError:
    """The real asyncpg error one `__cause__` hop below `exc.orig`. Only this
    object carries `constraint_name` -- `exc.orig` itself does not."""

    def __init__(self, constraint_name: str | None) -> None:
        self.constraint_name = constraint_name


class _FakeDbapiWrapper:
    """Mimics SQLAlchemy's DBAPI wrapper (`exc.orig`) as asyncpg actually
    layers it: `sqlstate` lives here, but `constraint_name` is absent -- it
    only shows up on `__cause__`, the underlying asyncpg error. Reading
    `constraint_name` off this object directly (no cause-walk) returns
    nothing, which is exactly the shape that catches a regression that drops
    the walk."""

    def __init__(self, sqlstate: str, cause: Any) -> None:
        self.sqlstate = sqlstate
        self.__cause__ = cause


def _layered_integrity_error(sqlstate: str, constraint_name: str | None) -> Any:
    asyncpg_error = _FakeAsyncpgError(constraint_name)
    orig = _FakeDbapiWrapper(sqlstate, cause=asyncpg_error)
    return IntegrityError("INSERT ...", {}, orig)


def test_classifies_name_unique_violation() -> None:
    exc = _integrity_error(_UNIQUE_VIOLATION, "agents_name_key")
    assert classify_integrity_error(exc) == (
        409,
        "an agent with that name already exists",
    )


def test_classifies_repo_full_name_unique_violation() -> None:
    # Kept after 0018 dropped the constraint (ADR-0091): a pre-0018 database
    # still carries it, and that operator deserves the fix, not the fallback.
    exc = _integrity_error(_UNIQUE_VIOLATION, "ix_agents_repo_full_name")
    code, message = classify_integrity_error(exc)
    assert code == 409
    assert "several agents" in message and "0018" in message


def test_classifies_unrecognized_unique_violation_with_generic_message() -> None:
    exc = _integrity_error(_UNIQUE_VIOLATION, "some_other_constraint")
    assert classify_integrity_error(exc) == (
        409,
        "agent violates a uniqueness constraint",
    )


def test_not_null_violation_is_not_a_uniqueness_conflict() -> None:
    # A NOT NULL failure has no constraint_name at all; must not be reported
    # as a 409 "uniqueness constraint" -- the caller should re-raise it.
    exc = _integrity_error(_NOT_NULL_VIOLATION, None)
    assert classify_integrity_error(exc) is None


def test_foreign_key_violation_is_not_a_uniqueness_conflict() -> None:
    # An FK violation does carry a constraint_name, but it isn't a unique
    # violation (different sqlstate) so it must not be classified as one.
    exc = _integrity_error(_FOREIGN_KEY_VIOLATION, "agent_versions_agent_id_fkey")
    assert classify_integrity_error(exc) is None


def test_classifies_name_unique_violation_via_cause_chain() -> None:
    # Real asyncpg shape: `exc.orig` has `sqlstate` but no `constraint_name`
    # of its own -- that field is only reachable via `exc.orig.__cause__`.
    # A classifier that reads `constraint_name` straight off `exc.orig`
    # (skipping the cause-walk) would see it as absent and fall back to the
    # generic message here, so this pins the walk itself, not just the
    # message table.
    exc = _layered_integrity_error(_UNIQUE_VIOLATION, "agents_name_key")
    assert classify_integrity_error(exc) == (
        409,
        "an agent with that name already exists",
    )


def test_classifies_repo_full_name_unique_violation_via_cause_chain() -> None:
    exc = _layered_integrity_error(_UNIQUE_VIOLATION, "ix_agents_repo_full_name")
    code, message = classify_integrity_error(exc)
    assert code == 409
    assert "several agents" in message


def test_the_map_no_longer_carries_the_retired_one_binding_per_agent_entry() -> None:
    """[FAIL-FIRST] Migration 0025 drops `agent_channels_agent_id_key`, so the
    message keyed on it can no longer fire.

    A dead entry is worse than no entry: it READS as a protection. Its text
    ("one agent binds one channel (ADR-0089), so PATCH the agent's channel to
    move it rather than adding a second binding") is the exact instruction
    ADR-0116 reverses, so a reader auditing what this API refuses would conclude
    the second binding is still forbidden, and the next person to touch the
    subresource would build to that.

    The pair constraint's entry is asserted present in the same breath: it is
    the only binding conflict left, and deleting both is the plausible
    over-correction. `ix_agents_repo_full_name` is the precedent for keeping a
    dropped constraint's message -- but only because a pre-0018 database still
    HAS that constraint, and no database will ever carry `agent_channels_agent_id_key`
    while also serving a platform that offers the subresource.
    """

    assert "agent_channels_agent_id_key" not in _UNIQUE_CONSTRAINT_MESSAGES, (
        "the one-binding-per-agent constraint is dropped by migration 0028 "
        "(ADR-0116); its 409 message can never fire again and tells an operator "
        "the opposite of what the API now does"
    )
    assert "agent_channels_kind_address_key" in _UNIQUE_CONSTRAINT_MESSAGES, (
        "one agent per route (#38) is unchanged and is now the ONLY binding "
        "conflict; dropping its message turns that 409 into a bare fallback"
    )
