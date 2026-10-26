# 247. Learned skills are a mutable, Markdown-only skill source

Date: 2026-10-20

Status: Accepted

Accepted by the maintainer on 2026-10-19 to land alongside its implementation on epic/self-improving.

The realizing code path is `apps/api/src/curie_api/routers/learned_skills.py`,
`runner/src/curie_runner/learned_skills.py` and
`runner/src/curie_runner/harness/claude/learned_skill_tools.py`.

Tracked in [#4780](https://github.com/curie-eng/curie/issues/4780).
This amends [ADR 0189](0189-deployed-agents-see-only-their-bundles-skills.md).

## Context

People teach agents procedures in conversation: "when a release is cut, do
these five steps". Today the agent can keep a fact or a preference in memory,
but a multi-step procedure has nowhere to live. The only skills a deployed
agent sees are the ones in its bundle, selected by the explicit `skills` list
(ADR 0189), and the bundle is immutable. Keeping a taught procedure would mean
a redeploy by someone with repository access.

The goal is that an agent creator can turn on learned skills per agent, and the
agent then keeps what it was taught, with no redeploy, with a full history, and
without gaining any new power over tools.

## Decision

Learned skills are a second skill source. They are mutable, Markdown only, and
stored with revisions outside the immutable bundle. They are loaded per session.

### Toggle

A per-agent operator toggle, `agents.learned_skills`, defaults to off. It is set
through `PATCH /agents/<id>` and by the CLI `overrides --learned-skills on|off`.
Off means no tools, no guidance and no skills loaded.

### Store

Dedicated Postgres tables hold the skills.

1. `learned_skills` holds the head: slug, description, body, the head revision
   number and a deletion marker. Changes update it with a compare and swap on
   the expected revision, so a stale writer gets 409.
2. `learned_skill_revisions` is insert only. Each row records the action
   (create, patch, revert, delete), the content, the resulting deleted state of
   that revision, and server-stamped provenance: the author from the
   credential's sender claim, the conversation, the turn, the time, and a
   sha256 digest of the content.

Revert to revision N restores both N's content and N's state, so reverting to a
tombstone deletes. "Undo" is revert to the previous revision, and undoing a
create deletes the skill. Undeleting checks the cap and slug uniqueness while
holding a lock on the agent row.

Limits, each answered with 422 naming the limit:

1. body at most 16384 bytes;
2. description at most 1024 characters, one line;
3. at most 32 active skills per agent;
4. slug matching `^[a-z0-9][a-z0-9-]{0,63}$`.

The cap of 32 follows published work on skill libraries (Skill Shadowing,
arXiv 2605.24050), which measured up to 21 percent degradation as libraries
grow, mostly from choosing the wrong skill.

Skills are Markdown only. The runner generates the frontmatter from the slug
and description. A body that opens with its own frontmatter is rejected, so no
tool permission, hook or MCP setting can be authored. There are no scripts,
hooks, MCP config or permission grants.

The raw state namespace `learned_skills` is reserved for every sandbox
credential, on the API and in the runner mirror. Nothing is stored there. The
dedicated router is the only door.

### Loading

The runner reads active skills at boot with its boot read token, from the agent
prefix of `CURIE_STATE_URL`. It materializes a generated local plugin named
`curie-learned` under `/tmp`, because the root filesystem is read-only. Files
are mode 0444 and directories 0555. Qualified names `curie-learned:<slug>` are
unioned into the explicit skills list. This amends ADR 0189: the explicit list
now admits one generated plugin besides the bundle. The name `curie-learned` is
reserved; a bundle that uses it gets no learned skills and an error log.

The snapshot is taken once per session. Re-materializing each turn would change
the system prompt and break the prompt cache affinity of one long-lived
session, so a skill saved in a session takes effect in the next session. The
loaded `slug@revision` list is logged at start. If the store is unreachable the
session starts without learned skills and logs it. With the toggle off the
session has no plugin, no tools and no guidance.

### Tools and authorization

Six in-process tools, `list`, `read`, `create`, `patch`, `delete` and `revert`,
are mounted on their own SDK MCP server only when the toggle is on. `patch` is a
targeted edit against an expected revision: `old_string` must occur exactly once
in the body and is replaced by `new_string`, with no wholesale body field. This
is adapted from Hermes `skill_manage` patch semantics (MIT).

The worker mints its existing per-turn state credential when memory writes or
learned skills is on. It adds the claims `skills` and `conversation`. The
`memory` claim is `read` unless memory writes is on, so a skills-only credential
cannot write facts.

1. Agent writes need the toggle on, `skills == "write"`, an open turn and a
   person as sender. Operator create and patch need the toggle on.
2. Operator delete and revert work while the toggle is off, to repair a store
   before re-enabling it. Nothing loads while it is off.
3. Scheduled runs have a cron author and no person. They can read skills but
   not change them. The API enforces this from the sender claim and the runner
   enforces it on job turns.
4. Responses to sandbox credentials omit the conversation and turn. Operators
   see full provenance.

### Guidance

When the toggle is on the system prompt carries guidance, adapted from the
learning prompt of OpenClaw skill-workshop (MIT). Multi-step procedures become
skills. Facts and preferences stay memory. "Remember how to do this" saves
directly. A procedure the agent notices on its own is offered as "Save this as a
skill: <name>?" and saved only on yes. A correction patches the same skill. After
any save the agent announces the name and that "undo" reverts it.

### Security note

Tool approval stays the boundary. The approval gate is enforced by the
PreToolUse approval hook, with `can_use_tool` as a backstop. A `SKILL.md`
`allowed-tools` rule does not preempt a PreToolUse hook, so a tampered learned
skill cannot widen tool access. The bundle directory and `HOME` are already
writable by the same uid in the sandbox, so a learned plugin under `/tmp` adds
no new exposure class. True immutability of loaded skills needs a separate uid
or a read-only mount, which is chart and daemon work outside this ADR.

## Consequences

1. Agents keep taught procedures with no redeploy, and every change is
   attributable, diffable and revertible from the CLI and API.
2. A new required column and two tables raise the minimum schema for the API.
3. A skill saved in a session is visible to the next session, not the current
   one.
4. Skill text is user-influenced content that the model follows. It can steer
   the agent but not widen what the agent is allowed to do.
5. The 32 skill cap bounds library growth. An agent that needs more must retire
   skills.

## Alternatives considered

1. **Rows in `workflow_state_entries`.** Rejected: its caps are per namespace
   and it keeps no immutable history, so there is nothing to diff or revert.
2. **Skills inside the bundle.** Rejected: it needs a redeploy and repository
   access, which is the problem.
3. **Re-materializing skills every turn.** Rejected: it breaks the prompt cache
   affinity of one long-lived session.
4. **Scripts or hooks in learned skills.** Rejected: executable content authored
   in conversation would widen the agent's power, and Markdown instructions are
   enough for the procedures people teach.
