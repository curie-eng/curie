"""The identity-bearing migrations' shared fence and provenance-declaration arm.

Imported BY Alembic revisions (`apps/api/alembic.ini` sets `prepend_sys_path =
src`), which is why this module lives here and not in the versions directory:
three revisions share it, and a copy per revision is how they silently diverge.

**Contract, because a historical revision imports it.** This module may import
only the standard library, `sqlalchemy` and `alembic`. It must NEVER import
`curie_api.models`, `curie_api.db`, `curie_api.crud` or any other ORM-bearing
module: a revision is pinned to the schema as of that revision, and the ORM
describes the schema as of HEAD. Every column this module touches is named
explicitly and gated on the caller's `audit_columns`, for the same reason --
`approval_audit_entries.evidence` exists from 0013, while `principal_kind` and
`authenticated` only arrive at 0038, three revisions after 0024.

Two things are exposed:

1. `fence_identity_tables` -- `SET LOCAL lock_timeout` then `LOCK TABLE
   curie.agent_channels, curie.approvals IN ACCESS EXCLUSIVE MODE` inside the
   revision's own transaction (`alembic/env.py` sets
   `transaction_per_migration=True`), so the preflight, the backfill and the
   constraint tightening commit as one unit and a binding cannot move between
   the preflight's answer and the backfill that records it.

   **ACCESS EXCLUSIVE, and it must be the STRONGEST lock the revision will
   need, taken UP FRONT.** The weaker SHARE ROW EXCLUSIVE this fence first used
   deadlocks rather than merely making a writer wait: an ordinary resolver
   holding ACCESS SHARE from a read does not conflict with SHARE ROW EXCLUSIVE,
   so the fence is granted; the revision's own `ALTER TABLE ... ADD COLUMN`
   then needs ACCESS EXCLUSIVE and waits on that reader; when the reader
   proceeds to its UPDATE it waits on the fence -- a cycle, and PostgreSQL
   aborts one participant. Acquiring the strongest mode first removes the later
   upgrade step, so there is no cycle to detect. Every revision that fences runs
   DDL on one of these two tables, so the strongest mode is ACCESS EXCLUSIVE for
   all of them; taking it on both tables in one statement, in a fixed order,
   keeps two fencing revisions deadlock-free against each other too.

   **The order is `agent_channels` THEN `approvals`, and it is chosen for the
   writers.** One `LOCK TABLE` over two tables is not simultaneous: PostgreSQL
   takes them in list order. Every writer that touches both tables takes them
   in that same order -- `crud.delete_agent` deletes the agent's
   `agent_channels` rows and then the agent, whose cascade reaches `approvals`,
   and publication create writes the binding before the approval. Fencing
   `approvals` first let the fence hold `approvals` while such a writer held
   `agent_channels`, a cycle. In this order the fence waits on the writer
   before it holds anything, so the writer finishes and the fence follows.

   **Readers cannot be ordered away, and that is accepted, not overlooked.** The
   approval-recovery router READS `approvals` and then `agent_channels`, the
   opposite order, so no single fence order is consistent with every
   transaction. A reader that holds ACCESS SHARE on `approvals` while the fence
   holds `agent_channels` and waits on `approvals` closes a cycle when the
   reader then reads `agent_channels`. PostgreSQL's deadlock detector breaks it
   after `deadlock_timeout` (1 s by default) by aborting one side, and either
   outcome is safe: an aborted fence raises BEFORE any mutation (see
   `fence_identity_tables`) and the migrate Job retries it (`backoffLimit: 3`,
   `charts/curie/templates/schema-migrate.yaml`); an aborted read gets SQLSTATE
   `40P01`, which the recovery routes answer with a retryable 409. The
   `lock_timeout` stays the outer bound on every other wait.

   The operational property is unchanged and is the point of the design: a
   concurrent writer BLOCKS and then SUCCEEDS, and is never refused. Under
   ACCESS EXCLUSIVE a concurrent plain SELECT also blocks, for the fence's
   duration only -- the accepted cost of removing an abort. The bounded
   `lock_timeout` is the other half -- if the fence cannot be taken the
   migration refuses BEFORE mutating anything and names what held the lock,
   rather than wedging a Job that has an `activeDeadlineSeconds`.

2. The provenance declaration arm -- a human-authored, per-row document stating
   the reply identity an approval was RAISED on, honored for EXACTLY the rows a
   preflight could not reconstruct and refused for everything else. Every
   honored declaration appends one `approval_audit_entries` row, so the bypass
   is attributed rather than silent, and nothing is ever deleted.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

SCHEMA = "curie"
APPROVALS = "approvals"
CHANNELS = "agent_channels"
AUDIT = "approval_audit_entries"

#: Locked in this order by every revision that fences, so two of them can never
#: deadlock against each other -- and `agent_channels` FIRST, because every
#: writer that touches both tables (`crud.delete_agent`, publication create)
#: takes them in that order. See the module docstring for the reader cycle this
#: order cannot exclude and why deadlock detection resolving it is safe.
FENCED_TABLES = (f"{SCHEMA}.{CHANNELS}", f"{SCHEMA}.{APPROVALS}")

#: The STRONGEST mode any fencing revision needs. See the module docstring: a
#: weaker fence followed by the revision's own ACCESS EXCLUSIVE DDL is a lock
#: upgrade, and a lock upgrade against a reader-that-becomes-a-writer is a
#: deadlock, not a wait.
LOCK_MODE = "ACCESS EXCLUSIVE"

#: The one reply kind whose egress identity is legitimately implicit: Slack
#: replies go back through the worker's configured Slack origin, so NULL is the
#: correct -- and the only correct -- adapter for a Slack row. Named here
#: because `load_declarations` enforces the pair rule the document states.
SLACK_KIND = "slack"

#: The identity NULL names for a Slack row (ADR-0168 decision 3). The stored
#: form keeps the installation's one Slack app as NULL, never as this string,
#: until the contract migration for that decision (#3100) flips it; but
#: a declaration is an OPERATOR document, not a stored row, and honoring one
#: has to accept the name a human would actually write. This module may import
#: only the standard library, `sqlalchemy` and `alembic` (module docstring),
#: so this is a second copy of `aci_protocol.turn.DEFAULT_IDENTITY` rather
#: than an import of it -- the same reason `SLACK_KIND` above is a second copy
#: of `aci_protocol.turn.SLACK_KIND`.
DEFAULT_IDENTITY = "default"

#: The worker is already quiesced at hook weight -10, so the only remaining
#: contenders are ordinary API requests whose budgets are sub-second. 15 s
#: comfortably outlasts one, while three `backoffLimit` retries of a wedged
#: fence cost 45 s against the migrate Job's `activeDeadlineSeconds: 600`.
DEFAULT_LOCK_TIMEOUT_MS = 15000
LOCK_TIMEOUT_ENV = "CURIE_MIGRATION_FENCE_LOCK_TIMEOUT_MS"

DECLARATIONS_ENV = "CURIE_APPROVAL_PROVENANCE_DECLARATIONS"

#: The audit action and authorizer a honored declaration is recorded under.
HONORED_ACTION = "provenance_declaration_honored"
FENCE_AUTHORIZER = "migration_provenance_fence"

#: `approval_audit_entries` as of 0013, which is the column set available to
#: 0022 and 0024. Callers pass this explicitly rather than having it assumed.
AUDIT_COLUMNS_AT_0013 = frozenset(
    {
        "id",
        "approval_id",
        "action",
        "actor",
        "actor_channel",
        "decision",
        "authorizer",
        "authorized",
        "reason",
        "evidence",
        "created_at",
    }
)

#: Every field is required. The point of the document is that a human vouched
#: for the row, so an anonymous or unexplained declaration is not a weaker
#: declaration, it is not one at all. `reply_adapter` is required as a KEY and
#: may be null as a VALUE, because NULL is the correct adapter for a Slack row.
REQUIRED_FIELDS = ("approval_id", "reply_kind", "actor", "reason")
OPTIONAL_NULLABLE_FIELDS = ("reply_adapter",)


@dataclass(frozen=True)
class Declaration:
    """One operator statement of the identity an approval was raised on."""

    approval_id: str
    reply_kind: str
    reply_adapter: str | None
    actor: str
    reason: str


def _lock_timeout_ms(override: int | None) -> int:
    if override is not None:
        return override
    raw = os.environ.get(LOCK_TIMEOUT_ENV, "").strip()
    if not raw:
        return DEFAULT_LOCK_TIMEOUT_MS
    try:
        parsed = int(raw)
    except ValueError as error:
        raise RuntimeError(
            f"{LOCK_TIMEOUT_ENV}={raw!r} is not an integer number of milliseconds; "
            "the migration fence refuses rather than falling back to a default it "
            "was explicitly told not to use."
        ) from error
    if parsed <= 0:
        raise RuntimeError(
            f"{LOCK_TIMEOUT_ENV}={raw!r} must be a positive number of milliseconds: "
            "0 means 'wait forever' to PostgreSQL, which is exactly the unbounded "
            "wedge this bound exists to prevent."
        )
    return parsed


def _blockers(conn: Connection, own_pid: int | None) -> str:
    """What is already holding the identity tables, read BEFORE the lock attempt.

    Read up front rather than after the failure on purpose: the lock timeout
    aborts the fence's own transaction, so nothing can be queried on it
    afterwards. A holder that is already parked on the table when the fence
    starts is the one that will time it out, and naming it is far better than a
    bare "lock timeout" -- the operator needs to know WHICH writer to look at.
    """

    # Literal identifiers rather than bind params: this runs on asyncpg, whose
    # protocol cannot infer a type for a parameter in a position like this and
    # errors out -- which would abort the fence's transaction before the LOCK.
    # The values are module constants, never operator input.
    query = sa.text(
        f"""
        SELECT c.relname AS table_name,
               l.pid AS pid,
               l.mode AS mode,
               a.state AS state,
               a.query AS query
        FROM pg_locks l
        JOIN pg_class c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE n.nspname = '{SCHEMA}'
          AND c.relname IN ('{APPROVALS}', '{CHANNELS}')
          AND l.granted
        ORDER BY c.relname, l.pid
        """
    )
    try:
        rows = [row for row in conn.execute(query).all() if row.pid != own_pid]
    except Exception:  # pragma: no cover - diagnostics must never mask the refusal
        return "the holder could not be read from pg_locks"
    if not rows:
        return "no other session held either table when the fence was attempted"
    return "; ".join(
        f"pid {row.pid} holds {row.mode} on {SCHEMA}.{row.table_name} "
        f"(state {row.state or 'unknown'}, query {(row.query or '').strip()!r})"
        for row in rows
    )


def fence_identity_tables(conn: Connection, *, lock_timeout_ms: int | None = None) -> None:
    """Take the identity fence for the rest of this migration's transaction.

    Issues `SET LOCAL lock_timeout` then one `LOCK TABLE` over both identity
    tables in a fixed order. PostgreSQL holds the lock until Alembic commits or
    rolls back the revision, so everything the revision does afterwards is one
    unit behind it.

    Raises `RuntimeError` -- before the caller has mutated anything -- when the
    fence cannot be taken inside the bound.
    """

    timeout_ms = _lock_timeout_ms(lock_timeout_ms)
    own_pid: int | None = None
    try:
        own_pid = conn.execute(sa.text("SELECT pg_backend_pid()")).scalar_one()
    except Exception:  # pragma: no cover - only affects the diagnostic filter
        own_pid = None

    conn.execute(sa.text(f"SET LOCAL lock_timeout = '{timeout_ms}ms'"))
    blockers = _blockers(conn, own_pid)
    statement = f"LOCK TABLE {', '.join(FENCED_TABLES)} IN {LOCK_MODE} MODE"
    try:
        conn.execute(sa.text(statement))
    except DBAPIError as error:
        raise RuntimeError(
            f"the migration fence could not be taken within {timeout_ms} ms: "
            f"`{statement}` was refused -- {blockers}. "
            "Refusing BEFORE any schema or row was mutated, so this database is "
            "exactly as it was and re-running the upgrade is safe. Settle or stop "
            f"the writer above -- it holds {SCHEMA}.{APPROVALS} or "
            f"{SCHEMA}.{CHANNELS} -- or raise the bound with "
            f"{LOCK_TIMEOUT_ENV}, then re-run this migration."
        ) from error


def refusal_guidance(revision: str) -> str:
    """The shared disposition sentence every identity refusal ends with.

    It never says to delete an approval or to settle one: destroying history, or
    forcing a decision a human has not made, is not a recovery. The supported
    round trip is report, declare, re-run.
    """

    return (
        "Re-point or restore the bindings these addresses belong to, or declare "
        "each approval's identity by hand: the full report and a ready-to-fill "
        "declaration document are printed below, and are also in this Job's log. "
        "(`curie cluster approvals --report-identity` emits the same document on "
        "an installation that is NOT blocked; a blocked one is on a pre-head "
        "schema, which the API serving that report refuses to start against, "
        "which is why the report is here.) Complete the document, mount it from a "
        f"Secret at {DECLARATIONS_ENV}, and re-run revision {revision}. Nothing is "
        "deleted and no approval is settled to get the migration through."
    )


def _declaration_refusal(path: str, detail: str) -> RuntimeError:
    return RuntimeError(
        f"the approval provenance declaration document {path} was refused: {detail}. "
        "No declaration in it was applied and no row was touched -- a document is "
        "honored whole or not at all, because a half-applied one leaves a bypass "
        "that names nobody. Fix the document and re-run this migration."
    )


def load_declarations() -> dict[str, Declaration]:
    """Read and validate the declaration document, or return `{}` when unset.

    Refuses a malformed, incomplete or unreadable document by naming the FILE
    and the offending row and field -- the operator already knows which rows are
    unreconstructable; what they need is what is wrong with their document.
    """

    raw_path = os.environ.get(DECLARATIONS_ENV, "").strip()
    if not raw_path:
        return {}

    try:
        with open(raw_path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as error:
        raise _declaration_refusal(raw_path, f"it could not be read ({error})") from error

    try:
        document: Any = json.loads(text)
    except json.JSONDecodeError as error:
        raise _declaration_refusal(raw_path, f"it is not valid JSON ({error})") from error

    if not isinstance(document, Mapping) or "declarations" not in document:
        raise _declaration_refusal(
            raw_path,
            "its top level must be a JSON object with a 'declarations' array",
        )
    entries = document["declarations"]
    if not isinstance(entries, list):
        raise _declaration_refusal(raw_path, "'declarations' must be an array")

    declarations: dict[str, Declaration] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise _declaration_refusal(
                raw_path, f"declaration #{index} is not a JSON object"
            )
        named = entry.get("approval_id")
        label = f"declaration #{index}" + (
            f" (approval_id {named})" if isinstance(named, str) and named else ""
        )
        for field in REQUIRED_FIELDS:
            if field not in entry:
                raise _declaration_refusal(
                    raw_path, f"{label} is missing the required field {field!r}"
                )
            value = entry[field]
            if not isinstance(value, str) or not value.strip():
                raise _declaration_refusal(
                    raw_path,
                    f"{label} has a {field!r} that is not a non-empty string",
                )
        for field in OPTIONAL_NULLABLE_FIELDS:
            if field not in entry:
                raise _declaration_refusal(
                    raw_path, f"{label} is missing the required field {field!r}"
                )
            value = entry[field]
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise _declaration_refusal(
                    raw_path,
                    f"{label} has a {field!r} that is neither null nor a non-empty string",
                )

        approval_id = str(entry["approval_id"]).strip()
        try:
            uuid.UUID(approval_id)
        except ValueError:
            raise _declaration_refusal(
                raw_path, f"{label} has an 'approval_id' that is not a UUID"
            ) from None
        if approval_id in declarations:
            raise _declaration_refusal(
                raw_path,
                f"approval {approval_id} is declared more than once, so there is no "
                "single statement to honor",
            )
        adapter = entry["reply_adapter"]

        # The kind/adapter pair rule, enforced HERE so a document that cannot
        # satisfy a revision's routing obligation is refused before anything is
        # honored. `reply_adapter: null` with a non-Slack kind used to pass:
        # 0024 then dropped the row from its refusal set although the missing
        # egress identity that raised the guard was still missing, the migration
        # succeeded, and the eventual reply failed for want of a credential --
        # the exact latent failure the guard exists to stop, now laundered
        # through a declaration. Slack is the mirror image: an adapter on a
        # Slack row names a credential the Slack egress never consults, and
        # would breach 0024's `agent_channels_route_pair_ck` reasoning about
        # what a complete route is.
        #
        # ADR-0168 decision 3 widens the Slack half of the rule: a
        # declaration may now name the default identity EXPLICITLY, as
        # `'default'`, not only as null -- an operator vouching for a row's
        # provenance is describing what they observed, and the identity has a
        # name now. It is still STORED as NULL (below): the column's form is
        # unchanged until the contract migration for that decision (#3100),
        # only what a declaration may say moves. Any other name is still
        # refused: this module cannot import `identities.declared_identities`
        # (the ORM-import contract above) to check a name against the chart's
        # list, and a declaration vouches for a row written before a second
        # Slack identity could exist. The contract migration for ADR-0168
        # decision 3 (#3146) is what widens this.
        kind = str(entry["reply_kind"]).strip()
        if kind == SLACK_KIND and adapter not in (None, DEFAULT_IDENTITY):
            raise _declaration_refusal(
                raw_path,
                f"{label} declares reply_kind {SLACK_KIND!r} with a 'reply_adapter' of "
                f"{adapter!r}; a Slack reply goes back through the worker's configured "
                "Slack origin and names only its default identity, so "
                f"'reply_adapter' must be null or {DEFAULT_IDENTITY!r}",
            )
        if kind != SLACK_KIND and adapter is None:
            raise _declaration_refusal(
                raw_path,
                f"{label} declares reply_kind {kind!r} with a null 'reply_adapter'; "
                "only a Slack reply has an implicit egress. Honoring this would clear "
                "the very obligation the migration is guarding -- the reply would be "
                "POSTed with no credential selector and fail closed inside the worker "
                "-- so the adapter this approval's reply must be authenticated with "
                "has to be named",
            )

        if adapter is None or (kind == SLACK_KIND and adapter == DEFAULT_IDENTITY):
            # The stored form is unchanged: the default Slack identity is
            # still NULL (`route_identity`, migrations 0023/0024),
            # so a declaration naming it as `'default'` is normalized back to
            # NULL here -- the same row shape `crud.py`'s raw
            # `approval.reply_adapter != data.reply_adapter` replay-conflict
            # check still compares, which a literal `'default'` would fail.
            stored_adapter = None
        else:
            stored_adapter = str(adapter).strip()

        declarations[approval_id] = Declaration(
            approval_id=approval_id,
            reply_kind=kind,
            reply_adapter=stored_adapter,
            actor=str(entry["actor"]).strip(),
            reason=str(entry["reason"]).strip(),
        )
    return declarations


def declarations_document_path() -> str:
    """The document's path, for a message. Empty when none is mounted."""

    return os.environ.get(DECLARATIONS_ENV, "").strip()


def _already_honored(conn: Connection, *, declaration: Declaration) -> bool:
    """Has THIS declaration already been honored, by an earlier revision?

    The operator mounts ONE document and runs `alembic upgrade head`, which is
    the command the documentation tells them to run. A declaration honored at
    0022 is therefore still mounted when 0024 loads the same file, and 0024's
    own preflight -- a different question -- does not name that row. Refusing it
    as "reconstructable" made the workflow reject its own successfully recovered
    rows and stop the upgrade at 0023.

    The honored audit row is the durable record that the statement was already
    applied, so a declaration matching one is a no-op here rather than a refusal
    OR a re-application: it is never re-written onto the row, which is what
    keeps a later revision from replacing an identity an earlier one
    established.
    """

    row = conn.execute(
        sa.text(
            f"""
            SELECT 1
            FROM {SCHEMA}.{AUDIT}
            WHERE approval_id = CAST(:id AS uuid)
              AND action = CAST(:action AS text)
              AND evidence ->> 'declared_reply_kind' = CAST(:kind AS text)
              AND (evidence ->> 'declared_reply_adapter')
                  IS NOT DISTINCT FROM CAST(:adapter AS text)
            LIMIT 1
            """
        ),
        {
            "id": declaration.approval_id,
            "action": HONORED_ACTION,
            "kind": declaration.reply_kind,
            "adapter": declaration.reply_adapter,
        },
    ).first()
    return row is not None


def honor_declarations(
    conn: Connection,
    *,
    unreconstructable: Mapping[str, str],
    declarations: Mapping[str, Declaration],
    established_kinds: Mapping[str, str | None],
    revision: str,
    audit_columns: frozenset[str] | set[str],
) -> set[str]:
    """Apply the declarations for the preflight's unreconstructable rows only.

    `unreconstructable` maps an approval id to the reason the preflight could
    not reconstruct it; that reason is recorded on the audit row, so the
    operator's own record says what they were vouching over.

    `established_kinds` is what this revision ALREADY knows about each of those
    rows, and is what makes the arm revision-aware. At 0022 nothing is
    established (the column is created by that very revision) and a declaration
    supplies the whole identity. At 0024 every row already carries a
    `reply_kind` 0022 persisted, and only the adapter half is missing: a
    declared kind that CONFLICTS with the established one is refused, and a
    declaration that agrees recovers the adapter ALONE. Letting a later revision
    write the kind would change recoverable original identity instead of
    recovering the missing component, which AC1 and AC4 forbid.

    Refuses -- naming the row -- a declaration for a row the preflight CAN
    reconstruct (its answer wins; a human overriding recoverable provenance is a
    rewrite, not a recovery) and a declaration for an id the preflight does not
    name at all (a stale document left mounted). A declaration an EARLIER
    revision already honored is neither: it is recognized and skipped, so one
    mounted document survives `alembic upgrade head`. All problems are collected
    first, so a document is never partially applied.

    Returns the ids honored BY THIS CALL. Writes ONLY columns in
    `audit_columns`, because `principal_kind` and `authenticated` do not exist
    until 0038.
    """

    if not declarations:
        return set()

    path = declarations_document_path()
    known = set(unreconstructable)
    reconstructable: list[str] = []
    unknown: list[str] = []
    for approval_id, declaration in declarations.items():
        if approval_id in known:
            continue
        if _already_honored(conn, declaration=declaration):
            continue
        exists = conn.execute(
            sa.text(f"SELECT 1 FROM {SCHEMA}.{APPROVALS} WHERE id = CAST(:id AS uuid)"),
            {"id": approval_id},
        ).first()
        (reconstructable if exists else unknown).append(approval_id)

    conflicting: list[str] = []
    for approval_id in sorted(known & set(declarations)):
        established = established_kinds.get(approval_id)
        declared = declarations[approval_id].reply_kind
        if established is not None and declared != established:
            conflicting.append(
                f"{approval_id} (established {established!r}, declared {declared!r})"
            )

    problems: list[str] = []
    if reconstructable:
        problems.append(
            "these approvals are declared but this revision's preflight can still "
            "establish their identity from a binding, and its answer wins -- "
            + ", ".join(sorted(reconstructable))
        )
    if unknown:
        problems.append(
            "these approvals are declared but this revision's preflight does not "
            "name them, so the document is stale -- " + ", ".join(sorted(unknown))
        )
    if conflicting:
        problems.append(
            "these approvals already carry a reply_kind an earlier revision "
            "established from their own provenance, and this document declares a "
            "different one -- " + ", ".join(conflicting) + ". This revision recovers "
            "only the component that is missing; changing an established kind is a "
            "rewrite of recoverable identity, not a recovery of a lost one"
        )
    if problems:
        raise _declaration_refusal(path, "; ".join(problems))

    honored: set[str] = set()
    for approval_id in sorted(known & set(declarations)):
        declaration = declarations[approval_id]
        established = established_kinds.get(approval_id)
        if established is None:
            # Nothing is known yet (0022): the declaration supplies the pair.
            statement = "SET reply_kind = :kind, reply_adapter = :adapter"
            params = {"kind": declaration.reply_kind, "adapter": declaration.reply_adapter}
        else:
            # The kind is already established and agrees; recover the adapter
            # alone, so the persisted origin is never rewritten.
            statement = "SET reply_adapter = :adapter"
            params = {"adapter": declaration.reply_adapter}
        conn.execute(
            sa.text(
                f"""
                UPDATE {SCHEMA}.{APPROVALS}
                {statement}
                WHERE id = CAST(:id AS uuid)
                """
            ),
            {**params, "id": approval_id},
        )
        # The declared values have to leave the row actually able to route, not
        # merely out of the refusal set. `load_declarations` enforces the pair
        # rule on the document; this re-reads what landed, so a future caller
        # that applies only half a declaration fails here rather than shipping a
        # row that resumes with no credential.
        resulting_kind, resulting_adapter = conn.execute(
            sa.text(
                f"SELECT reply_kind, reply_adapter FROM {SCHEMA}.{APPROVALS} "
                "WHERE id = CAST(:id AS uuid)"
            ),
            {"id": approval_id},
        ).one()
        if (resulting_kind == SLACK_KIND) != (resulting_adapter is None):
            raise _declaration_refusal(
                path,
                f"honoring the declaration for {approval_id} would leave reply_kind "
                f"{resulting_kind!r} with reply_adapter {resulting_adapter!r}, which "
                "does not satisfy this revision's routing obligation: exactly a Slack "
                "reply has no adapter",
            )
        _append_audit_entry(
            conn,
            declaration=declaration,
            revision=revision,
            preflight_reason=unreconstructable[approval_id],
            established_kind=established,
            audit_columns=audit_columns,
        )
        honored.add(approval_id)
    return honored


def _append_audit_entry(
    conn: Connection,
    *,
    declaration: Declaration,
    revision: str,
    preflight_reason: str,
    established_kind: str | None,
    audit_columns: frozenset[str] | set[str],
) -> None:
    """One append-only row recording the bypass. Nothing is ever rewritten."""

    evidence = json.dumps(
        {
            "declared_reply_kind": declaration.reply_kind,
            "declared_reply_adapter": declaration.reply_adapter,
            "revision": revision,
            "preflight_reason": preflight_reason,
            # What this revision already knew, so the record says which half of
            # the identity the declaration actually recovered.
            "established_reply_kind": established_kind,
            "document": declarations_document_path(),
        }
    )
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "approval_id": uuid.UUID(declaration.approval_id),
        "action": HONORED_ACTION,
        "actor": declaration.actor,
        "decision": "none",
        "authorizer": FENCE_AUTHORIZER,
        "authorized": True,
        "reason": declaration.reason,
        "evidence": evidence,
    }
    # `created_at` is left to its server default, and every other column is
    # dropped unless the caller says it exists at this revision.
    columns = [column for column in values if column in audit_columns]
    placeholders = [
        "CAST(:evidence AS jsonb)" if column == "evidence" else f":{column}"
        for column in columns
    ]
    conn.execute(
        sa.text(
            f"INSERT INTO {SCHEMA}.{AUDIT} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        ),
        {column: values[column] for column in columns},
    )


@dataclass(frozen=True)
class UnreconstructableRow:
    """One approval a revision's preflight could not establish an identity for."""

    approval_id: str
    reply_channel: str | None
    status: str | None
    reason: str


def declaration_skeleton(rows: Sequence[UnreconstructableRow]) -> str:
    """A ready-to-fill declaration document, one entry per offending row.

    The value fields are left EMPTY on purpose. An empty string is not a valid
    declaration (`load_declarations` refuses it by name), so a skeleton pasted
    back unedited fails loudly instead of being honored as a blank statement
    attributed to nobody.
    """

    return json.dumps(
        {
            "declarations": [
                {
                    "approval_id": row.approval_id,
                    "reply_kind": "",
                    "reply_adapter": "",
                    "actor": "",
                    "reason": "",
                }
                for row in rows
            ]
        },
        indent=2,
    )


def identity_report(rows: Sequence[UnreconstructableRow], *, revision: str) -> str:
    """The whole disposition, printed INTO the refusal and into the Job log.

    A blocked installation is on a pre-head schema, so the API that serves the
    reporting endpoint refuses to start against it -- which means
    `curie cluster approvals --report-identity` is unavailable in exactly the
    situation this refusal is about. The failed migrate Job's log is where the
    operator already is, so it has to be sufficient on its own: every offending
    row's facts, and the document to fill in, both right here.
    """

    listing = "\n".join(
        f"  - {row.approval_id}: reply_channel {row.reply_channel!r}, "
        f"status {row.status}, {row.reason}"
        for row in rows
    )
    return (
        f"\n\nIdentity report for revision {revision} -- "
        f"{len(rows)} approval(s) whose reply identity could not be reconstructed:\n"
        f"{listing}\n\n"
        "Fill in one declaration per row below, stating the identity each approval "
        "was RAISED on (every field is required; reply_adapter may be null, and "
        "only null, for a Slack row), put it in a Secret, and supply it to the "
        f"migration through {DECLARATIONS_ENV}:\n\n"
        f"{declaration_skeleton(rows)}\n"
    )


def report_and_guidance(rows: Sequence[UnreconstructableRow], *, revision: str) -> str:
    """Log the identity report, then return it appended to the guidance sentence.

    Logged as well as raised because an exception string is the thing most
    likely to be truncated on its way to a human, and the report is the part
    they need whole.
    """

    report = identity_report(rows, revision=revision)
    logging.getLogger("alembic.runtime.migration").error(
        "revision %s refused on approval reply identity.%s", revision, report
    )
    return refusal_guidance(revision) + report
