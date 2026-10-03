# 189. Deployed agents see only their bundle's skills

Date: 2026-10-01

Status: Accepted

Accepted by Brian on 2026-10-02 for v0.12.0. The implementation was
already present in `runner/src/curie_runner/adapter.py`.

Tracked in [#3766](https://github.com/curie-eng/curie/issues/3766), split out
of [#3625](https://github.com/curie-eng/curie/issues/3625).

This ADR builds on two Draft ADRs,
[ADR-0137](0137-coding-tools-are-built-in-and-an-initial-repository-url-selects-the-workspace.md)
and [ADR-0139](0139-bundle-owners-classify-every-vanilla-mcp-tool.md). It
supersedes neither.

## Context

In #3625 a user asked an agent to sign every reply in every channel. The agent
called the `Skill` tool, not the memory tool `remember`. It talked about hooks
and said it had noted a standing instruction. Nothing was saved.

The skill it reached is not from the bundle. It ships inside the Claude Code
CLI. With an empty `HOME` and an empty working directory, the bundled CLI
(2.1.281) still offers the model 17 built-in skills, among them
`update-config`, `code-review`, `simplify`, `loop` and `run`. The listing for
`update-config` says that "from now on" and "each time" requests need hooks in
`settings.json`, and that memory cannot fulfil them. That is the wording of a
"sign every reply" request, and it steers the model away from `remember`.

These skills reach the agent because of how the runner builds its SDK options
(`build_options` in `runner/src/curie_runner/adapter.py`):

- It turns on the full Claude Code tool preset, which includes the `Skill`
  tool.
- It never passes `skills`. The SDK then keeps the CLI's defaults, which list
  every built-in skill.
- It never passes `setting_sources`. Nobody decided that; it is the SDK
  default.

The built-ins make sense for a developer in a repository. A deployed agent
answers in a channel. For it they are a second, wrong place to put "make this
stick" requests, and they describe settings and hooks it cannot configure.

Bundle skills use the same `Skill` tool. A bundle's `skills/<dir>/SKILL.md`
loads as `<manifest name>:<dir>`. So removing the tool would remove every
bundle skill too.

How this relates to the earlier decisions, both still Drafts:

- [ADR-0137](0137-coding-tools-are-built-in-and-an-initial-repository-url-selects-the-workspace.md)
  (Draft) proposes the Claude Code file-tool preset as a platform capability
  of every session. This ADR keeps that. The coding tools and the `Skill` tool
  stay. Only the list of skills the `Skill` tool offers gets narrower. The
  built-in skills are not part of the coding surface the Draft ADR-0137
  describes.
- [ADR-0139](0139-bundle-owners-classify-every-vanilla-mcp-tool.md) (Draft)
  proposes that bundle configuration may add restrictions but may not hollow
  out operator or platform controls. It also keeps harness built-ins outside
  `toolPolicy`, and says that hiding a tool from the model is not
  authorization. This ADR is consistent with both points. The skill list is a
  visibility filter. The `PreToolUse` hook stays the enforcement point. A
  later bundle opt-in that adds built-in skills back would widen what the
  agent can reach, so it needs its own explicit rule (see Consequences).

No earlier ADR mentions `skills`, `setting_sources` or the `Skill` tool.

## Decision

A deployed agent sees only its own bundle's skills. It does not see the Claude
Code CLI's built-in skills.

1. The runner always passes the SDK option `skills` as a list. The list is the
   bundle's own skills, `<manifest name>:<dir>` for each `skills/<dir>/SKILL.md`
   one level down. A bundle with no skills gets `[]`. The runner never leaves
   `skills` unset.
2. The runner always passes an explicit `setting_sources`. The value is
   `["user", "project", "local"]`, which keeps today's settings loading.
3. Both callers of `build_options`, the session entry point and the offline
   MCP load check (`runner/src/curie_runner/check.py`), pass the same list.

## Consequences

- **The model's skill listing has only bundle skills.** On CLI 2.1.281, a test
  that runs the real CLI shows that unlisted skills are not offered to the
  model: the first model request names the bundle's skills and none of the
  built-in ones. A "make this stick" request no longer has `update-config` to
  go to. Whether the `Skill` tool also refuses an unlisted name the model
  guesses was not checked, so this ADR does not rely on it. For listed skills,
  the `PreToolUse` hook is the gate (next point).
- **Listed skills are pre-approved.** With a list, the SDK adds
  `Skill(<name>)` to the allowed tools for each listed skill. Those calls then
  skip `can_use_tool`. The runner has kept `allowed_tools` empty so that no call
  skips it. The `PreToolUse` hook still sees every call, so read-only turns and
  approval gates still apply. This matters only if an operator gates `Skill`
  itself. `assert_gates_not_shadowed` does not see these SDK-added rules. A
  test on the real CLI 2.1.281 confirmed that the SDK's `Skill(<name>)` allow
  rule does skip `can_use_tool`, and that an operator gate on `Skill` still
  stops the call through the hook. So the hook, not `can_use_tool`, is the
  gate for listed skills.
- **`local` settings keep loading.** When `skills` is set and
  `setting_sources` is not, the SDK fills in `["user", "project"]`. That would
  quietly drop `local` settings. Passing `["user", "project", "local"]` keeps
  today's behaviour. Passing `[]` is not chosen here: it would also stop a
  workspace repository's `CLAUDE.md` from loading, which is a separate
  decision.
- **Skill names follow the directory.** The CLI names a skill by its directory,
  not its frontmatter `name`, and does not load nested skill folders. The list
  must use the same one-level rule. The validator and approval code currently
  walk nested folders, so they can see skills the CLI ignores.
- **Folders the SDK cannot name are skipped with a warning.** The bundle
  validator only warns about odd folder names. The SDK, though, refuses to
  build the CLI command for a skill name it cannot carry in a `Skill(<name>)`
  rule, such as one with a comma, a parenthesis or leading or trailing
  whitespace. So that one such folder does not stop the session at connect,
  the runner checks each name with the SDK's own check and leaves out any it
  rejects, logging a warning that names the folder. That skill is then not
  offered to the model.
- **A CLI upgrade could change naming or filtering.** A test that runs the real
  CLI and reads the model's skill listing pins this. The `init` message is not
  enough: it lists every registered skill even when filtered.
- **No bundle opt-in yet.** A bundle cannot ask for a built-in skill back. That
  needs a new field in the frozen `plugin-format` manifest, for example an
  exact-name list, and so its own reviewed contract PR. `toolPolicy` cannot
  express it, since it covers only MCP tools. Because an opt-in widens what the
  agent can reach, its ADR must allow that explicitly. It is left for later.
- **Other skill sources are covered too.** Skills a workspace repository puts
  in `.claude/skills`, or that the agent writes to `~/.claude/skills`, are not
  in the list, so they are hidden as well.

## Alternatives considered

- **Remove the `Skill` tool.** Adding `Skill` to `disallowed_tools` hides the
  built-ins. Rejected: bundle skills use the same tool, so every bundle skill
  would go too.
- **Keep everything visible.** Rely on the memory guidance from #3662 to steer
  "remember" requests. Rejected: this is the #3625 behaviour. The guidance and
  the `update-config` listing would keep pulling in opposite directions, and
  the agent would still be offered settings and hooks it cannot configure.
- **Hide only the settings and hooks family.** Rejected: it needs a list of
  built-in names to block, and every CLI upgrade can add new ones. An allow
  list of the bundle's own skills fails closed.
