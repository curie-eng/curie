"""Persist shared connector source staleness (ADR 0239).

A platform binding records the version of the Kubernetes Secret it was imported
from. The resolver cannot read that Secret and holds no Kubernetes credential,
so it cannot compare versions itself ("Static sources" 1). The API check, which
is handed the Secret's current uid and resource version by the operator, is the
only place a mismatch is seen. Until now it reported the mismatch and stored
nothing, so a resolve after the check still served the old credential.

``source_stale boolean NOT NULL DEFAULT false`` on
``curie.shared_connector_bindings`` closes that gap. The API check sets it on a
mismatch and reports ``source_stale`` for as long as it is set; only an explicit
reimport clears it. The resolver reads the flag with the SELECT grant it already
has and refuses a stale binding. The application role gains no new grant:
``lock_guard`` stays the only column it may update, so it can neither set nor
clear the flag.

``shared_connector_bindings_source_stale_ck`` (``NOT source_stale OR custody =
'platform'``) keeps connector custody, which holds no source, from ever
carrying the flag.

This revision also serializes every transition into ``recovery_required`` with
in-flight resolves ("Static sources" 5 and 6). A resolve holds its binding
``FOR SHARE`` for its whole admission transaction and reads the refresh intent
unlocked. In 0123, ``shared_connector_refresh_mark_recovery`` locked only the
intent and ``shared_connector_refresh_begin`` (which turns a lapsed lease into
recovery) held the binding only ``FOR SHARE``, so neither waited for a resolve
already admitted on a ``refreshing`` intent, and that resolve could release a
credential after recovery had committed. Both functions are replaced so they
take the binding ``FOR NO KEY UPDATE``, which conflicts with a resolve's
``FOR SHARE``: a transition into recovery commits only after every resolve
already holding the binding has committed, and a resolve that reaches the
binding later waits for the transition and then, under READ COMMITTED, reads
the intent in a new statement that sees it. The lock order stays 0123's
(application, binding, intent, recipient last). It ships here rather than in
its own revision because 0124 is unmerged and is already the head: no new
revision, and no grant to the application role. The role gains nothing; the
definer functions change only their own lock modes. ``CREATE OR REPLACE``
keeps each function's signature, owner and EXECUTE grants. The new bodies are
derived from 0123's own definitions (loaded from that immutable module, never
copied), changing only the lock statements.

Expand only: a constant-default, NOT NULL column add is metadata-only on
PostgreSQL 16, and existing rows read false. The downgrade restores 0123's two
function definitions exactly, then drops the constraint, then the column.

Revision ID: 0124
Revises: 0123
Create Date: 2026-10-22
"""

import importlib.util
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import sqlalchemy as sa
from alembic import op

revision: str = "0124"
down_revision: str | None = "0123"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
BINDINGS = "shared_connector_bindings"
SOURCE_STALE_CK = "shared_connector_bindings_source_stale_ck"


def _load_0123() -> ModuleType:
    """The merged 0123 migration, whose definitions are immutable and so safe to derive from."""

    path = Path(__file__).with_name("0123_shared_connector_store.py")
    spec = importlib.util.spec_from_file_location("_curie_migration_0123", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load migration 0123 from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CP1 = _load_0123()
REFRESH_INTENTS: str = _CP1.REFRESH_INTENTS
# 0123's definitions as it created them (``CREATE FUNCTION``).
CP1_REFRESH_BEGIN: str = _CP1.REFRESH_BEGIN
CP1_REFRESH_MARK_RECOVERY: str = _CP1.REFRESH_MARK_RECOVERY


def _replace_once(text: str, old: str, new: str) -> str:
    """Replace ``old``, which must occur exactly once, so drift in 0123 fails loudly."""

    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected exactly one occurrence in 0123, found {count}: {old!r}")
    return text.replace(old, new)


def _or_replace(definition: str, name: str) -> str:
    return _replace_once(
        definition,
        f"CREATE FUNCTION {SCHEMA}.{name}(",
        f"CREATE OR REPLACE FUNCTION {SCHEMA}.{name}(",
    )


# Why the binding lock is taken, for both functions. No colons in the SQL
# comments: SQLAlchemy's text() would read a colon before a word as a bind.
_WHY_BINDING_LOCK = """\
    -- Binding FOR NO KEY UPDATE (0124). It conflicts with the FOR SHARE every
    -- in-flight resolve holds on its binding for its whole admission
    -- transaction, so a move into recovery_required commits only after the
    -- resolves already holding the binding have committed. A resolve that
    -- locks the binding later waits for this transaction, and its intent read,
    -- a new statement under READ COMMITTED once the lock is granted, sees the
    -- recovery. Lock order stays application, binding, intent, recipient last.
"""

_BEGIN_BINDING_SHARE = f"""\
    -- Binding FOR SHARE.
    SELECT b.enabled, b.custody, b.credential_revision, b.recipient_id, b.ciphertext
    INTO v_binding_enabled, v_custody, v_revision, v_recipient, v_ciphertext
    FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = v_binding AND b.application_id = v_application
    FOR SHARE;
"""

_BEGIN_BINDING_NO_KEY_UPDATE = f"""\
{_WHY_BINDING_LOCK}\
    -- Every begin takes it, because a begin can turn a lapsed lease into
    -- recovery and the mode cannot be raised once the intent is locked
    -- without breaking that order.
    SELECT b.enabled, b.custody, b.credential_revision, b.recipient_id, b.ciphertext
    INTO v_binding_enabled, v_custody, v_revision, v_recipient, v_ciphertext
    FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = v_binding AND b.application_id = v_application
    FOR NO KEY UPDATE;
"""

_MARK_DECLARE = """\
    v_installation text;
    v_state text;
"""

_MARK_DECLARE_BINDING = """\
    v_installation text;
    v_binding uuid;
    v_state text;
"""

_MARK_INTENT_ONLY = """\
    -- Plan decision 15. Only the intent is locked.
"""

_MARK_BINDING_THEN_INTENT = f"""\
    -- Unlocked: a grant's binding never changes, so it names the row to lock.
    SELECT i.binding_id INTO v_binding
    FROM {SCHEMA}.{REFRESH_INTENTS} i
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
      AND i.application_id = v_application;
    IF NOT FOUND THEN
        RETURN 'not_found';
    END IF;
{_WHY_BINDING_LOCK}\
    PERFORM 1 FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = v_binding AND b.application_id = v_application
    FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RETURN 'not_found';
    END IF;
    -- Intent FOR UPDATE.
"""

REFRESH_BEGIN = _replace_once(
    _or_replace(CP1_REFRESH_BEGIN, "shared_connector_refresh_begin"),
    _BEGIN_BINDING_SHARE,
    _BEGIN_BINDING_NO_KEY_UPDATE,
)

REFRESH_MARK_RECOVERY = _replace_once(
    _replace_once(
        _or_replace(CP1_REFRESH_MARK_RECOVERY, "shared_connector_refresh_mark_recovery"),
        _MARK_DECLARE,
        _MARK_DECLARE_BINDING,
    ),
    _MARK_INTENT_ONLY,
    _MARK_BINDING_THEN_INTENT,
)


def upgrade() -> None:
    op.add_column(
        BINDINGS,
        sa.Column("source_stale", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        SOURCE_STALE_CK,
        BINDINGS,
        "NOT source_stale OR custody = 'platform'",
        schema=SCHEMA,
    )
    # Same signatures and return types, so OR REPLACE keeps owner and grants.
    op.execute(REFRESH_BEGIN)
    op.execute(REFRESH_MARK_RECOVERY)


def downgrade() -> None:
    # 0123's exact bodies, so the restored definitions match a fresh 0123 upgrade.
    op.execute(_or_replace(CP1_REFRESH_BEGIN, "shared_connector_refresh_begin"))
    op.execute(_or_replace(CP1_REFRESH_MARK_RECOVERY, "shared_connector_refresh_mark_recovery"))
    op.drop_constraint(SOURCE_STALE_CK, BINDINGS, schema=SCHEMA, type_="check")
    op.drop_column(BINDINGS, "source_stale", schema=SCHEMA)
