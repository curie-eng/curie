"""Key WorkItems by their tracker issue and bind them to a code host (#3831, ADR 0197).

Contract. ADR 0197 "Identity": a WorkItem's unique key is (tracker kind,
tracker host, scope id, issue id), and its repository is bound by (code host
kind, code host host, immutable project id) with the path for display. Every
identifier is text. The GitHub id columns are dropped, not aliased, so an
application before this revision cannot serve against it.

``work_items`` gains ``tracker_kind``, ``tracker_host``, ``tracker_scope_id``,
``tracker_issue_id``, ``tracker_display_key`` (nullable), ``code_host_kind``,
``code_host_host`` and ``repository_project_id``; ``repo_full_name`` becomes
``repository_path`` and ``github_installation_id`` becomes the nullable
``code_host_installation_id``. The unique key ``work_items_github_issue_key``
on (github_repository_id, github_issue_number) is replaced by
``work_items_tracker_issue_key`` and both GitHub columns are dropped.

``thread_publication_lineages`` gains ``code_host_kind``, ``code_host_host``
and ``repository_project_id`` in place of ``github_repository_id``;
``github_installation_id`` becomes ``code_host_installation_id`` and
``github_pr_node_id`` becomes ``code_host_pr_id``. Its identity CHECK and the
two GitHub unique indexes are replaced by code host ones.

``factory_poll_cursors`` is keyed by (tracker_kind, tracker_host,
tracker_scope_id); ``repo_full_name`` becomes ``scope_path`` and
``repository_id`` is dropped. A cursor that never recorded a repository id is
deleted, and of two cursors for one repository (the repository was renamed)
only the newest is kept: a missing cursor polls from the lookback window and
admission dedupes what it sees again.

The installation id stays because it is an authority fence, not a lookup:
review feedback, issue reads and publication truth refuse an installation that
differs from the one recorded at admission. Only GitHub has one, so it is
nullable and required where the code host is GitHub.

Primary keys, execution request ids and every derived id are unchanged: the
GitHub derivations read the same repository id and issue number, now as text.

The backfill host: every existing row is GitHub. A migration cannot read the
API settings, so the host is ``CURIE_MIGRATION_GITHUB_HOST`` when set, else the
host of ``GITHUB_API_URL`` derived as the API derives its GitHub HTML base
(``api.github.com`` is ``github.com``; the chart passes the API's value to the
migrate Job), else ``github.com``. A GitHub Enterprise install that runs the
migration without either variable must set one, or its rows will not match the
host its adapter reports.

Downgrade restores the GitHub columns from the text ones and refuses while any
row names another tracker or code host kind.

Revision ID: 0090
Revises: 0089
Create Date: 2026-10-05
"""

import os
from collections.abc import Sequence
from urllib.parse import urlsplit

import sqlalchemy as sa
from alembic import op

revision: str = "0090"
down_revision: str | None = "0089"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
_WORK_ITEMS = "work_items"
_LINEAGES = "thread_publication_lineages"
_CURSORS = "factory_poll_cursors"

# A GitHub id is a positive decimal integer without a leading zero; the
# identity derivations refuse anything else.
_DECIMAL = "'^[1-9][0-9]*$'"

# Frozen copies of the model constraints at this revision.
_WORK_ITEM_TEXT_CK = (
    "length(btrim(tracker_kind)) > 0 AND length(btrim(tracker_host)) > 0 "
    "AND length(btrim(tracker_scope_id)) > 0 AND length(btrim(tracker_issue_id)) > 0 "
    "AND (tracker_display_key IS NULL OR length(btrim(tracker_display_key)) > 0) "
    "AND length(btrim(code_host_kind)) > 0 AND length(btrim(code_host_host)) > 0 "
    "AND length(btrim(repository_project_id)) > 0 AND length(btrim(repository_path)) > 0"
)
_WORK_ITEM_GITHUB_CK = (
    f"(tracker_kind <> 'github' OR (tracker_scope_id ~ {_DECIMAL} "
    f"AND tracker_issue_id ~ {_DECIMAL})) "
    f"AND (code_host_kind <> 'github' OR (repository_project_id ~ {_DECIMAL} "
    "AND code_host_installation_id IS NOT NULL))"
)
_INSTALLATION_CK = "code_host_installation_id IS NULL OR code_host_installation_id > 0"
# Wrapped in IS TRUE: a CHECK whose expression is NULL passes, and a half-set
# identity would otherwise make the second branch NULL.
_LINEAGE_IDENTITY_CK = (
    "((code_host_kind IS NULL AND code_host_host IS NULL AND repository_project_id IS NULL "
    "AND code_host_installation_id IS NULL AND code_host_pr_id IS NULL AND base_ref IS NULL) "
    "OR (length(btrim(code_host_kind)) > 0 AND length(btrim(code_host_host)) > 0 "
    "AND length(btrim(repository_project_id)) > 0 "
    "AND (code_host_installation_id IS NULL OR code_host_installation_id > 0) "
    f"AND (code_host_kind <> 'github' OR (repository_project_id ~ {_DECIMAL} "
    "AND code_host_installation_id IS NOT NULL)) "
    "AND length(code_host_pr_id) > 0 AND pr_number IS NOT NULL "
    "AND length(base_ref) > 0)) IS TRUE"
)


# The WorkItem identity the 0046 trigger keeps immutable, before and after.
# ``tracker_display_key`` is left out: a Jira key changes when the issue moves.
_OLD_IDENTITY = (
    "github_repository_id",
    "github_issue_number",
    "github_installation_id",
    "agent_id",
    "repo_full_name",
)
_NEW_IDENTITY = (
    "tracker_kind",
    "tracker_host",
    "tracker_scope_id",
    "tracker_issue_id",
    "code_host_kind",
    "code_host_host",
    "repository_project_id",
    "repository_path",
    "code_host_installation_id",
    "agent_id",
)
_TRIGGER = "work_items_update_invariants"


def _work_items_trigger(identity: Sequence[str]) -> str:
    """The 0054 trigger function with ``identity`` as the immutable columns."""

    immutable = "".join(
        f"\n               OR NEW.{column} IS DISTINCT FROM OLD.{column}" for column in identity
    )
    return f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.enforce_work_items_update_invariants()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id{immutable}
               OR NEW.conversation_id IS DISTINCT FROM OLD.conversation_id
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item identity is immutable';
            END IF;

            IF OLD.cancelled_at IS NOT NULL AND NEW.cancelled_at IS NOT NULL
               AND NEW.cancelled_at IS DISTINCT FROM OLD.cancelled_at THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item cancellation is sticky';
            END IF;

            IF OLD.publication_lineage_id IS NOT NULL
               AND NEW.publication_lineage_id IS DISTINCT FROM OLD.publication_lineage_id THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item publication lineage is write once';
            END IF;

            RETURN NEW;
        END;
        $$
        """


def _drop_trigger() -> None:
    op.execute(f"DROP TRIGGER {_TRIGGER} ON {SCHEMA}.{_WORK_ITEMS}")


def _create_trigger(identity: Sequence[str]) -> None:
    op.execute(_work_items_trigger(identity))
    op.execute(
        f"CREATE TRIGGER {_TRIGGER} BEFORE UPDATE ON {SCHEMA}.{_WORK_ITEMS} "
        "FOR EACH ROW EXECUTE FUNCTION curie.enforce_work_items_update_invariants()"
    )


def _github_host() -> str:
    explicit = os.environ.get("CURIE_MIGRATION_GITHUB_HOST", "").strip().lower()
    if explicit:
        return explicit
    api = os.environ.get("GITHUB_API_URL", "").strip()
    if api:
        netloc = urlsplit(api.rstrip("/")).netloc.lower()
        if netloc:
            return "github.com" if netloc == "api.github.com" else netloc
    return "github.com"


def _text(name: str, *, nullable: bool = True) -> sa.Column[str]:
    return sa.Column(name, sa.Text(), nullable=nullable)


def _upgrade_work_items(host: str) -> None:
    # The immutability trigger names the GitHub columns; it is rebuilt over
    # the tracker identity once the backfill has run.
    _drop_trigger()
    for name in (
        "tracker_kind",
        "tracker_host",
        "tracker_scope_id",
        "tracker_issue_id",
        "tracker_display_key",
        "code_host_kind",
        "code_host_host",
        "repository_project_id",
    ):
        op.add_column(_WORK_ITEMS, _text(name), schema=SCHEMA)
    op.alter_column(_WORK_ITEMS, "repo_full_name", new_column_name="repository_path", schema=SCHEMA)
    op.drop_constraint(
        "work_items_github_installation_id_ck", _WORK_ITEMS, schema=SCHEMA, type_="check"
    )
    op.alter_column(
        _WORK_ITEMS,
        "github_installation_id",
        new_column_name="code_host_installation_id",
        nullable=True,
        schema=SCHEMA,
    )
    op.get_bind().execute(
        sa.text(
            "UPDATE curie.work_items SET tracker_kind = 'github', tracker_host = :host, "
            "tracker_scope_id = github_repository_id::text, "
            "tracker_issue_id = github_issue_number::text, "
            "code_host_kind = 'github', code_host_host = :host, "
            "repository_project_id = github_repository_id::text"
        ),
        {"host": host},
    )
    for name in (
        "tracker_kind",
        "tracker_host",
        "tracker_scope_id",
        "tracker_issue_id",
        "code_host_kind",
        "code_host_host",
        "repository_project_id",
    ):
        op.alter_column(_WORK_ITEMS, name, nullable=False, schema=SCHEMA)
    op.drop_constraint("work_items_github_issue_key", _WORK_ITEMS, schema=SCHEMA, type_="unique")
    for check in ("work_items_github_repository_id_ck", "work_items_github_issue_number_ck"):
        op.drop_constraint(check, _WORK_ITEMS, schema=SCHEMA, type_="check")
    op.drop_column(_WORK_ITEMS, "github_issue_number", schema=SCHEMA)
    op.drop_column(_WORK_ITEMS, "github_repository_id", schema=SCHEMA)
    op.create_unique_constraint(
        "work_items_tracker_issue_key",
        _WORK_ITEMS,
        ["tracker_kind", "tracker_host", "tracker_scope_id", "tracker_issue_id"],
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_identity_text_ck", _WORK_ITEMS, _WORK_ITEM_TEXT_CK, schema=SCHEMA
    )
    op.create_check_constraint(
        "work_items_github_identity_ck", _WORK_ITEMS, _WORK_ITEM_GITHUB_CK, schema=SCHEMA
    )
    op.create_check_constraint(
        "work_items_code_host_installation_id_ck", _WORK_ITEMS, _INSTALLATION_CK, schema=SCHEMA
    )
    _create_trigger(_NEW_IDENTITY)


def _upgrade_lineages(host: str) -> None:
    for name in ("code_host_kind", "code_host_host", "repository_project_id"):
        op.add_column(_LINEAGES, _text(name), schema=SCHEMA)
    op.drop_constraint(
        "thread_publication_lineages_github_identity_ck", _LINEAGES, schema=SCHEMA, type_="check"
    )
    op.drop_index("uq_publication_github_pr_owner", table_name=_LINEAGES, schema=SCHEMA)
    op.drop_index("uq_active_publication_github_conversation", table_name=_LINEAGES, schema=SCHEMA)
    op.alter_column(
        _LINEAGES,
        "github_installation_id",
        new_column_name="code_host_installation_id",
        schema=SCHEMA,
    )
    op.alter_column(
        _LINEAGES, "github_pr_node_id", new_column_name="code_host_pr_id", schema=SCHEMA
    )
    op.get_bind().execute(
        sa.text(
            "UPDATE curie.thread_publication_lineages SET code_host_kind = 'github', "
            "code_host_host = :host, repository_project_id = github_repository_id::text "
            "WHERE github_repository_id IS NOT NULL"
        ),
        {"host": host},
    )
    op.drop_column(_LINEAGES, "github_repository_id", schema=SCHEMA)
    op.create_check_constraint(
        "thread_publication_lineages_code_host_identity_ck",
        _LINEAGES,
        _LINEAGE_IDENTITY_CK,
        schema=SCHEMA,
    )
    op.create_index(
        "uq_publication_code_host_pr_owner",
        _LINEAGES,
        ["code_host_kind", "code_host_host", "repository_project_id", "pr_number"],
        unique=True,
        schema=SCHEMA,
    )
    op.create_index(
        "uq_active_publication_code_host_conversation",
        _LINEAGES,
        [
            "agent_id",
            "conversation_id",
            "code_host_kind",
            "code_host_host",
            "repository_project_id",
        ],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'open'"),
    )


def _upgrade_cursors(host: str) -> None:
    for name in ("tracker_kind", "tracker_host", "tracker_scope_id"):
        op.add_column(_CURSORS, _text(name), schema=SCHEMA)
    bind = op.get_bind()
    bind.execute(sa.text("DELETE FROM curie.factory_poll_cursors WHERE repository_id IS NULL"))
    bind.execute(
        sa.text(
            "DELETE FROM curie.factory_poll_cursors c USING curie.factory_poll_cursors d "
            "WHERE c.repository_id = d.repository_id "
            "AND (c.updated_at, c.repo_full_name) < (d.updated_at, d.repo_full_name)"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE curie.factory_poll_cursors SET tracker_kind = 'github', "
            "tracker_host = :host, tracker_scope_id = repository_id::text"
        ),
        {"host": host},
    )
    op.drop_constraint("factory_poll_cursors_pkey", _CURSORS, schema=SCHEMA, type_="primary")
    op.drop_constraint(
        "factory_poll_cursors_repository_id_ck", _CURSORS, schema=SCHEMA, type_="check"
    )
    op.drop_column(_CURSORS, "repository_id", schema=SCHEMA)
    op.alter_column(_CURSORS, "repo_full_name", new_column_name="scope_path", schema=SCHEMA)
    for name in ("tracker_kind", "tracker_host", "tracker_scope_id"):
        op.alter_column(_CURSORS, name, nullable=False, schema=SCHEMA)
    op.create_primary_key(
        "factory_poll_cursors_pkey",
        _CURSORS,
        ["tracker_kind", "tracker_host", "tracker_scope_id"],
        schema=SCHEMA,
    )


def upgrade() -> None:
    host = _github_host()
    _upgrade_work_items(host)
    _upgrade_lineages(host)
    _upgrade_cursors(host)


def _refuse_foreign_rows() -> None:
    bind = op.get_bind()
    foreign = bind.execute(
        sa.text(
            "SELECT (SELECT count(*) FROM curie.work_items "
            "WHERE tracker_kind <> 'github' OR code_host_kind <> 'github') "
            "+ (SELECT count(*) FROM curie.thread_publication_lineages "
            "WHERE code_host_kind IS NOT NULL AND code_host_kind <> 'github') "
            "+ (SELECT count(*) FROM curie.factory_poll_cursors WHERE tracker_kind <> 'github')"
        )
    ).scalar_one()
    if foreign:
        raise RuntimeError(
            f"0090 downgrade refused: {foreign} row(s) name a tracker or code host "
            "other than GitHub, which revision 0089 cannot store"
        )


def downgrade() -> None:
    _refuse_foreign_rows()
    bind = op.get_bind()

    op.drop_constraint("factory_poll_cursors_pkey", _CURSORS, schema=SCHEMA, type_="primary")
    op.add_column(_CURSORS, sa.Column("repository_id", sa.BigInteger()), schema=SCHEMA)
    bind.execute(
        sa.text("UPDATE curie.factory_poll_cursors SET repository_id = tracker_scope_id::bigint")
    )
    op.alter_column(_CURSORS, "scope_path", new_column_name="repo_full_name", schema=SCHEMA)
    for name in ("tracker_scope_id", "tracker_host", "tracker_kind"):
        op.drop_column(_CURSORS, name, schema=SCHEMA)
    op.create_primary_key("factory_poll_cursors_pkey", _CURSORS, ["repo_full_name"], schema=SCHEMA)
    op.create_check_constraint(
        "factory_poll_cursors_repository_id_ck",
        _CURSORS,
        "repository_id IS NULL OR repository_id > 0",
        schema=SCHEMA,
    )

    op.drop_index(
        "uq_active_publication_code_host_conversation", table_name=_LINEAGES, schema=SCHEMA
    )
    op.drop_index("uq_publication_code_host_pr_owner", table_name=_LINEAGES, schema=SCHEMA)
    op.drop_constraint(
        "thread_publication_lineages_code_host_identity_ck",
        _LINEAGES,
        schema=SCHEMA,
        type_="check",
    )
    op.add_column(_LINEAGES, sa.Column("github_repository_id", sa.BigInteger()), schema=SCHEMA)
    bind.execute(
        sa.text(
            "UPDATE curie.thread_publication_lineages "
            "SET github_repository_id = repository_project_id::bigint "
            "WHERE repository_project_id IS NOT NULL"
        )
    )
    op.alter_column(
        _LINEAGES, "code_host_pr_id", new_column_name="github_pr_node_id", schema=SCHEMA
    )
    op.alter_column(
        _LINEAGES,
        "code_host_installation_id",
        new_column_name="github_installation_id",
        schema=SCHEMA,
    )
    for name in ("repository_project_id", "code_host_host", "code_host_kind"):
        op.drop_column(_LINEAGES, name, schema=SCHEMA)
    op.create_check_constraint(
        "thread_publication_lineages_github_identity_ck",
        _LINEAGES,
        "(github_repository_id IS NULL AND github_installation_id IS NULL "
        "AND github_pr_node_id IS NULL AND base_ref IS NULL) "
        "OR (github_repository_id IS NOT NULL "
        "AND github_repository_id > 0 "
        "AND github_installation_id IS NOT NULL AND github_installation_id > 0 "
        "AND github_pr_node_id IS NOT NULL "
        "AND length(github_pr_node_id) > 0 AND pr_number IS NOT NULL "
        "AND base_ref IS NOT NULL AND length(base_ref) > 0)",
        schema=SCHEMA,
    )
    op.create_index(
        "uq_publication_github_pr_owner",
        _LINEAGES,
        ["github_repository_id", "pr_number"],
        unique=True,
        schema=SCHEMA,
    )
    op.create_index(
        "uq_active_publication_github_conversation",
        _LINEAGES,
        ["agent_id", "conversation_id", "github_repository_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'open'"),
    )

    _drop_trigger()
    for check in (
        "work_items_code_host_installation_id_ck",
        "work_items_github_identity_ck",
        "work_items_identity_text_ck",
    ):
        op.drop_constraint(check, _WORK_ITEMS, schema=SCHEMA, type_="check")
    op.drop_constraint("work_items_tracker_issue_key", _WORK_ITEMS, schema=SCHEMA, type_="unique")
    op.add_column(_WORK_ITEMS, sa.Column("github_repository_id", sa.BigInteger()), schema=SCHEMA)
    op.add_column(_WORK_ITEMS, sa.Column("github_issue_number", sa.Integer()), schema=SCHEMA)
    bind.execute(
        sa.text(
            "UPDATE curie.work_items SET github_repository_id = tracker_scope_id::bigint, "
            "github_issue_number = tracker_issue_id::integer"
        )
    )
    for name in ("github_repository_id", "github_issue_number"):
        op.alter_column(_WORK_ITEMS, name, nullable=False, schema=SCHEMA)
    op.alter_column(
        _WORK_ITEMS,
        "code_host_installation_id",
        new_column_name="github_installation_id",
        nullable=False,
        schema=SCHEMA,
    )
    op.alter_column(_WORK_ITEMS, "repository_path", new_column_name="repo_full_name", schema=SCHEMA)
    for name in (
        "repository_project_id",
        "code_host_host",
        "code_host_kind",
        "tracker_display_key",
        "tracker_issue_id",
        "tracker_scope_id",
        "tracker_host",
        "tracker_kind",
    ):
        op.drop_column(_WORK_ITEMS, name, schema=SCHEMA)
    op.create_unique_constraint(
        "work_items_github_issue_key",
        _WORK_ITEMS,
        ["github_repository_id", "github_issue_number"],
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_github_repository_id_ck",
        _WORK_ITEMS,
        "github_repository_id > 0",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_github_issue_number_ck",
        _WORK_ITEMS,
        "github_issue_number > 0",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_github_installation_id_ck",
        _WORK_ITEMS,
        "github_installation_id > 0",
        schema=SCHEMA,
    )
    _create_trigger(_OLD_IDENTITY)
