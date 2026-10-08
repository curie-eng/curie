# 210. A bundle may declare a secret optional

Date: 2026-10-06

Status: Accepted

Accepted on 2026-10-07 with explicit maintainer approval from Junwon Jung
(jw3329), before any implementation.

This ADR supersedes in part [ADR 0009](0009-per-agent-connector-auth.md)
(Accepted) in one clause: the manifest `secrets` list is "the versioned,
evaluable list of named secrets the bundle requires", and a deploy refuses any
declared name it does not bind. Everything else in ADR 0009 stands: secrets are
declared by name only, values are bound per agent at deploy time, and they
reach the sandbox as environment that the bundle's MCP configuration consumes
as `${VAR}`. It answers
[#4129](https://github.com/curie-eng/curie/issues/4129).

## Context

A bundle declares the secrets its connectors need in the `secrets` field of its
manifest. ADR 0009 made that list the bundle's requirements, and the deploy gate
built for it (#464, `cli/src/commands/deploy.rs`) refuses a deploy that leaves
any declared name unbound: "declares connector secret(s) that were not bound on
deploy". The gate runs in the shared deploy path, so it covers `local deploy`
and `cluster deploy` alike. It exists because a missing binding otherwise
surfaces later as a runtime authentication failure (#429).

That rule has no room for a secret a connector can use but does not need. A
stdio MCP server often works without a credential and does more with one: a
GitHub server reads public repositories anonymously and private ones with a
token. Today a bundle with such a server has two choices, and both are wrong:

- Declare the secret. Every deploy must then bind a value, even where nothing
  needs it. The value can expire, and an expired credential that nothing needed
  breaks the deploy or the agent. A live deployment of an example bundle hit
  exactly this: an expired token it did not need stopped its first run.
- Leave it undeclared. The connector can then never receive it through the
  platform, because only declared names are bound and delivered.

The manifest model is part of the frozen `packages/plugin-format` contract.
`secrets` is typed `list[str] | None` there and in the committed JSON Schema.
How a bundle marks a secret optional is therefore a contract change, and the
semver table in `packages/CLAUDE.md` decides which shapes are compatible.

What reaches the server when the secret is absent also needs a rule. The
runner mounts the bundle's own MCP servers itself
(`runner/src/curie_runner/plugin.py::bundle_mcp_servers`) and leaves every
`${VAR}` other than `${CLAUDE_PLUGIN_ROOT}` for the Claude Code CLI to expand
from the session environment. A server env entry such as
`"GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_PERSONAL_ACCESS_TOKEN}"` whose
variable is unset currently reaches the server as whatever that CLI produces
for a missing variable. Nothing in Curie defines or tests that value, and a
server given an empty string or a literal `${NAME}` as a token typically fails
authentication rather than falling back to anonymous access.

## Decision

1. **A new manifest field, `optionalSecrets`, declares optional secrets.** It is
   a list of names, separate from `secrets`:

   ```json
   "secrets": ["EXAMPLE_SLACK_BOT_TOKEN", "EXAMPLE_SLACK_TEAM_ID"],
   "optionalSecrets": ["GITHUB_PERSONAL_ACCESS_TOKEN"]
   ```

   Each name gets the same validation as a `secrets` name: the env var name
   pattern and the reserved boot env refusal (`plugin_format.reserved_env`). A
   name may not appear in both lists; the validator refuses that bundle. The
   field is optional and absent by default. Under the semver table in
   `packages/CLAUDE.md` a new optional field is the compatible change class, a
   patch bump of the contract. The manifest model already allows extra fields,
   so an older API or CLI ignores `optionalSecrets` and keeps reading `secrets`
   as before.

2. **Deploy accepts an unbound optional secret and still refuses an unbound
   required one.** The deploy gate diffs only `secrets` against the bound set,
   exactly as it does today, with the same message and exit code. A name in
   `optionalSecrets` may be bound with `--secret NAME` or left unbound. A bound
   optional secret is resolved, stored and delivered exactly like a required
   one on every tier.

3. **An unbound optional secret is absent at runtime, never empty.** No tier
   delivers anything for it: no empty value on the local agent record, no
   environment entry in a `curie skill up` runner, and on cluster no key in the
   agent's connector Secret and no `secretKeyRef` for it. In the bundle's MCP
   configuration, a stdio server `env` entry whose entire value is `${NAME}`,
   for a name declared in `optionalSecrets` and not bound, is removed before the
   server is mounted. The server then starts with the variable unset and
   decides for itself what to do without it. The removal happens in the runner's
   in memory projection of the bundle's servers, so the bundle's `.mcp.json`
   stays the unmodified artifact that was deployed, the same rule the hosted
   `Bearer ${NAME}` expansion in `runner/src/curie_runner/connectors.py`
   follows. A `${NAME}` embedded in a longer string (an argument, a header, a
   URL) is not rewritten; the bundle author owns that string, and this ADR
   defines only the whole value form.

4. **`secrets` keeps its meaning.** It stays the list of secrets a bundle
   requires. Moving a name from `secrets` to `optionalSecrets` is a bundle
   change, versioned and reviewed like any other.

## Consequences

- A bundle whose connector works without a credential can deploy without one,
  and no expiring credential that nothing needs can break that deploy.
- A required secret behaves exactly as before. The existing deploy gate tests
  keep passing unchanged, and a new test proves an optional name does not open
  a gap for a required one.
- The runtime absence rule needs a test at the runner seam that mounts the
  bundle's servers, through the same projection the session uses, asserting
  that the entry is gone rather than empty or literal. The skill tier exercises
  that seam with no platform involved, so a skill tier eval of a bundle with an
  unbound optional secret is the end to end proof.
- An older CLI deploying a bundle that uses `optionalSecrets` does not bind or
  require the optional name, which matches this decision. An older runner does
  not remove the unset entry, so the server sees whatever the Claude Code CLI
  produces for a missing variable. A bundle that relies on the absence rule
  therefore needs a runner that implements it.
- The validator, the CLI deploy gate, the per tier delivery and the runner
  projection all read the same declaration. This is a deploy time validator and
  runtime loader pair under the parity seam convention in `AGENTS.md`, so the
  implementation routes both through `plugin_format` rather than a second parse.

## Alternatives considered

- **Object entries inside `secrets`**, such as
  `{"name": "GITHUB_PERSONAL_ACCESS_TOKEN", "optional": true}`. Rejected. It
  changes the type of an existing field, which `packages/CLAUDE.md` classes as
  breaking. The current validator rejects such an entry, so an older API
  refuses the bundle, and the current CLI's `read_declared_secrets`
  (`cli/src/scaffold.rs`) silently drops non string entries, so an older CLI
  would neither require nor report the name. One field with two element shapes
  also makes every reader of `secrets` branch on the shape.
- **Leave the field alone and tell authors to write `${NAME:-}`.** Rejected.
  That makes deploy accept nothing new, since the name still has to be declared
  to be delivered and every declared name is still required. Where the default
  syntax is honored it also yields an empty string, and a server given an empty
  token usually fails authentication instead of falling back to anonymous
  access. The issue asks for the variable to be unset, so the server can tell
  absence from a value.
- **Make every declared secret optional and warn on an unbound one.** Rejected.
  It removes the fail loud gate ADR 0009 and #464 chose on purpose, and turns
  every missing required credential back into a runtime authentication failure.

## Order of work

Each step lands as its own pull request, against `next`, in this order.

1. This Draft ADR, then explicit maintainer acceptance, published as
   `Status: Accepted` with the back link added to ADR 0009 under ADR 0045.
2. A `packages/plugin-format` contract change: the `optionalSecrets` field, its
   validator (name pattern, reserved names, no overlap with `secrets`), the
   regenerated committed schema and generated bindings, and the patch version
   bump.
3. The implementation: the CLI deploy gate accepting unbound optional names,
   delivery of bound optional names on every tier, the runner removal of
   unbound whole value `${NAME}` entries in
   `runner/src/curie_runner/plugin.py::bundle_mcp_servers`, and tests for each,
   including the unchanged refusal of an unbound required name.
4. The example bundle change, tracked in
   [#4129](https://github.com/curie-eng/curie/issues/4129) and not part of this
   decision: `examples/mean-tester` moves `GITHUB_PERSONAL_ACCESS_TOKEN` to
   `optionalSecrets`, bumps its plugin version, and drops its README advice to
   bind a token that reads nothing.
