"""Bind each new factory request to its configured public Slack destination."""

from collections.abc import Sequence

from alembic import op

revision: str = "0194"
down_revision: str = "0191"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_OLD_KINDS = "'root','phase','pull_request','terminal','card','needs_person','ready','maintenance'"


def upgrade() -> None:
    op.execute("ALTER TABLE curie.agents ADD COLUMN factory_public_slack_channel text")
    op.execute("""
        ALTER TABLE curie.agents ADD CONSTRAINT agents_factory_public_slack_channel_ck CHECK (
            factory_public_slack_channel IS NULL OR
            (factory_slack_channel IS NULL AND factory_public_slack_channel ~ '^[CG][A-Z0-9]{6,}$'))
    """)
    op.drop_constraint("factory_slack_posts_kind_ck", "factory_slack_posts", schema="curie")
    op.create_check_constraint(
        "factory_slack_posts_kind_ck",
        "factory_slack_posts",
        f"kind IN ({_OLD_KINDS},'steering_applied','steering_refused')",
        schema="curie",
    )
    op.execute("""
        CREATE TABLE curie.factory_slack_public_threads (
            LIKE curie.factory_slack_threads INCLUDING DEFAULTS,
            PRIMARY KEY (work_item_id),
            FOREIGN KEY (work_item_id) REFERENCES curie.work_items(id) ON DELETE CASCADE,
            CONSTRAINT factory_slack_public_threads_root_key UNIQUE
                (channel_identity, channel, root_ts),
            CONSTRAINT factory_slack_public_threads_origin_ck
                CHECK (origin IN ('kickoff','configured'))
        )
    """)
    op.execute("""
        CREATE TABLE curie.factory_slack_public_posts (
            LIKE curie.factory_slack_posts INCLUDING DEFAULTS INCLUDING IDENTITY,
            PRIMARY KEY (id),
            FOREIGN KEY (work_item_id) REFERENCES curie.factory_slack_public_threads(work_item_id)
                ON DELETE CASCADE,
            CONSTRAINT factory_slack_public_posts_post_key_key UNIQUE(work_item_id, post_key),
            CONSTRAINT factory_slack_public_posts_kind_ck CHECK (kind IN (
                'root','card','ready','needs_person','steering_queued','steering_applied',
                'steering_refused','maintenance','phase','pull_request','terminal')),
            CONSTRAINT factory_slack_public_posts_payload_ck CHECK (payload = '{}'::jsonb)
        )
    """)
    op.execute("""
        CREATE INDEX ix_factory_slack_public_posts_owed ON curie.factory_slack_public_posts
            (work_item_id, sequence, rank, ordinal) WHERE posted_at IS NULL AND failed_at IS NULL
    """)
    op.execute("""
        CREATE TABLE curie.factory_slack_public_refusal_posts (
            LIKE curie.factory_slack_refusal_posts INCLUDING DEFAULTS INCLUDING IDENTITY,
            PRIMARY KEY (id),
            FOREIGN KEY (notice_id) REFERENCES curie.factory_intake_notices(id) ON DELETE CASCADE,
            CONSTRAINT factory_slack_public_refusal_posts_notice_key UNIQUE (notice_id),
            CONSTRAINT factory_slack_public_refusals_payload_ck CHECK (payload = '{}'::jsonb)
        )
    """)
    op.execute("""
        CREATE INDEX ix_factory_slack_public_refusal_posts_owed
            ON curie.factory_slack_public_refusal_posts
            (created_at, id) WHERE posted_at IS NULL AND failed_at IS NULL
    """)
    op.execute("""
        CREATE VIEW curie.factory_slack_mapped_roots AS
          SELECT work_item_id, channel_identity, channel, root_ts,
                 'public'::text AS destination_role
            FROM curie.factory_slack_public_threads WHERE root_ts IS NOT NULL
          UNION ALL
          SELECT t.work_item_id, t.channel_identity, t.channel, t.root_ts,
                 CASE WHEN a.factory_public_slack_channel = t.channel
                      THEN 'public'::text ELSE 'origin'::text END
            FROM curie.factory_slack_threads t LEFT JOIN curie.agents a ON a.id = t.agent_id
           WHERE t.root_ts IS NOT NULL AND NOT EXISTS (
              SELECT 1 FROM curie.factory_slack_public_threads p
               WHERE (p.channel_identity, p.channel, p.root_ts) =
                     (t.channel_identity, t.channel, t.root_ts))
    """)
    op.execute("""
        CREATE FUNCTION curie.factory_slack_guard_root() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.root_ts IS NULL THEN RETURN NEW; END IF;
          PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(
              NEW.channel_identity, NEW.channel, NEW.root_ts)::text, 0));
          IF EXISTS (
              SELECT 1 FROM curie.factory_slack_mapped_roots r
               WHERE (r.channel_identity, r.channel, r.root_ts) =
                     (NEW.channel_identity, NEW.channel, NEW.root_ts)
                 AND r.work_item_id <> NEW.work_item_id)
          THEN RAISE unique_violation USING MESSAGE = 'factory_slack_root_conflict'; END IF;
          RETURN NEW;
        END $$
    """)
    for table in ("factory_slack_threads", "factory_slack_public_threads"):
        op.execute(f"""
            CREATE TRIGGER factory_slack_guard_root BEFORE INSERT OR UPDATE OF
                channel_identity, channel, root_ts, work_item_id ON curie.{table}
            FOR EACH ROW EXECUTE FUNCTION curie.factory_slack_guard_root()
        """)
    op.execute("""
        CREATE FUNCTION curie.factory_slack_guard_origin_card() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.card_lease_owner IS NOT NULL AND NEW.origin = 'kickoff' AND EXISTS (
              SELECT 1 FROM curie.factory_slack_mapped_roots r
               WHERE (r.work_item_id, r.channel_identity, r.channel, r.root_ts) =
                     (NEW.work_item_id, NEW.channel_identity, NEW.channel, NEW.root_ts)
                 AND r.destination_role = 'public')
          THEN
            NEW.card_lease_owner := NULL;
            NEW.card_lease_expires_at := NULL;
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER factory_slack_guard_origin_card BEFORE UPDATE OF card_lease_owner
        ON curie.factory_slack_threads FOR EACH ROW
        EXECUTE FUNCTION curie.factory_slack_guard_origin_card()
    """)
    op.execute("""
        CREATE FUNCTION curie.factory_slack_guard_origin_post() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.posted_at IS NULL AND NEW.failed_at IS NULL AND EXISTS (
            SELECT 1 FROM curie.factory_slack_threads t
            LEFT JOIN curie.factory_slack_public_threads p
                USING (work_item_id, channel_identity, channel)
            LEFT JOIN curie.agents a ON a.id = t.agent_id
            WHERE t.work_item_id = NEW.work_item_id
              AND ((p.work_item_id IS NOT NULL AND p.root_ts IS NOT DISTINCT FROM t.root_ts)
                   OR (t.origin = 'kickoff' AND t.channel = a.factory_public_slack_channel)))
          THEN
            NEW.failed_at := now();
            NEW.last_error := 'transferred_to_public';
            NEW.payload := '{}'::jsonb;
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER factory_slack_guard_origin_post BEFORE INSERT
        ON curie.factory_slack_posts FOR EACH ROW
        EXECUTE FUNCTION curie.factory_slack_guard_origin_post()
    """)

    op.execute("""
        CREATE FUNCTION curie.reserve_factory_slack_public_root(
            item_id uuid, start_sequence integer, shared_kickoff_only boolean
        ) RETURNS void LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtextextended(item_id::text, 194));
          INSERT INTO curie.factory_slack_public_threads
                (work_item_id, agent_id, channel_identity, channel, first_sequence)
          SELECT w.id, w.agent_id, b.identity, a.factory_public_slack_channel, start_sequence
            FROM curie.work_items w JOIN curie.agents a ON a.id = w.agent_id
            CROSS JOIN LATERAL (
                SELECT min(c.adapter) AS identity FROM curie.agent_channels c
                 WHERE c.agent_id = a.id AND c.kind = 'slack'
                   AND c.address = a.factory_public_slack_channel AND c.adapter IS NOT NULL
                 HAVING count(*) = 1
            ) b
           WHERE w.id = item_id AND a.factory_slack_since IS NOT NULL
             AND (NOT shared_kickoff_only OR EXISTS (
                 SELECT 1 FROM curie.factory_slack_threads t
                  WHERE t.work_item_id = w.id AND t.origin = 'kickoff' AND t.root_ts IS NOT NULL
                    AND t.channel = a.factory_public_slack_channel
                    AND t.channel_identity = b.identity
             ))
          ON CONFLICT (work_item_id) DO NOTHING;

          PERFORM t.work_item_id
            FROM curie.factory_slack_threads t JOIN curie.factory_slack_public_threads p
                 USING (work_item_id, channel_identity, channel)
           WHERE t.work_item_id = item_id AND (p.root_ts IS NULL OR p.root_ts = t.root_ts)
           FOR UPDATE OF t, p;
          IF NOT FOUND THEN RETURN; END IF;

          UPDATE curie.factory_slack_public_threads p
             SET root_ts = COALESCE(p.root_ts, t.root_ts),
                 card_ts = COALESCE(p.card_ts, t.card_ts),
                 origin = t.origin, requester_slack_user = t.requester_slack_user,
                 card_upgrade_cutoff = t.card_upgrade_cutoff
            FROM curie.factory_slack_threads t
           WHERE p.work_item_id = item_id AND t.work_item_id = p.work_item_id;

          INSERT INTO curie.factory_slack_public_posts
                (work_item_id, post_key, kind, sequence, rank, ordinal, payload, attempts,
                 not_before, posted_at, posted_ref, failed_at, created_at)
          SELECT work_item_id, post_key, kind, sequence, rank, ordinal, '{}'::jsonb, attempts,
                 not_before, posted_at, posted_ref,
                 CASE WHEN last_error IS DISTINCT FROM 'transferred_to_public' THEN failed_at END,
                 created_at
            FROM curie.factory_slack_posts WHERE work_item_id = item_id
          ON CONFLICT (work_item_id, post_key) DO NOTHING;

          UPDATE curie.factory_slack_posts
             SET failed_at = now(), last_error = 'transferred_to_public', payload = '{}'::jsonb,
                 lease_owner = NULL, lease_expires_at = NULL
           WHERE work_item_id = item_id AND posted_at IS NULL AND failed_at IS NULL;
        END $$
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE curie.agents, curie.factory_slack_posts, curie.factory_slack_public_threads,
                 curie.factory_slack_public_refusal_posts IN ACCESS EXCLUSIVE MODE
    """)
    op.execute("""
        DO $$ BEGIN IF EXISTS (SELECT 1 FROM curie.factory_slack_public_threads)
        OR EXISTS (SELECT 1 FROM curie.factory_slack_public_refusal_posts)
        OR EXISTS (SELECT 1 FROM curie.agents WHERE factory_public_slack_channel IS NOT NULL)
        OR EXISTS (SELECT 1 FROM curie.factory_slack_posts WHERE kind LIKE 'steering_%')
        THEN RAISE EXCEPTION 'factory_slack_public_state_present'; END IF; END $$
    """)
    op.execute("DROP FUNCTION curie.reserve_factory_slack_public_root(uuid, integer, boolean)")
    op.execute("DROP TRIGGER factory_slack_guard_origin_post ON curie.factory_slack_posts")
    op.execute("DROP FUNCTION curie.factory_slack_guard_origin_post()")
    op.execute("DROP TRIGGER factory_slack_guard_origin_card ON curie.factory_slack_threads")
    op.execute("DROP FUNCTION curie.factory_slack_guard_origin_card()")
    for table in ("factory_slack_threads", "factory_slack_public_threads"):
        op.execute(f"DROP TRIGGER factory_slack_guard_root ON curie.{table}")
    op.execute("DROP FUNCTION curie.factory_slack_guard_root()")
    op.execute("DROP VIEW curie.factory_slack_mapped_roots")
    op.drop_table("factory_slack_public_refusal_posts", schema="curie")
    op.drop_table("factory_slack_public_posts", schema="curie")
    op.drop_table("factory_slack_public_threads", schema="curie")
    op.drop_constraint("factory_slack_posts_kind_ck", "factory_slack_posts", schema="curie")
    op.create_check_constraint(
        "factory_slack_posts_kind_ck",
        "factory_slack_posts",
        f"kind IN ({_OLD_KINDS})",
        schema="curie",
    )
    op.drop_constraint("agents_factory_public_slack_channel_ck", "agents", schema="curie")
    op.drop_column("agents", "factory_public_slack_channel", schema="curie")
