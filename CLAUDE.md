See [AGENTS.md](AGENTS.md) - the agent instructions for this repo live there.

## One entry point: `curie <command>`

Dev and operator flows go through the `curie` CLI, not loose shell scripts or
bare tool invocations. When you would otherwise add a `./scripts/foo.sh`, tell
someone to run a raw `docker`/`helm`/`uv`/`pnpm` command, or document a
multi-step setup, add or extend a `curie` subcommand instead — a single,
discoverable surface beats a scatter of scripts. The script or tool call can
stay the *implementation*; it just isn't the interface.

- Building the runner image → `curie build` (not a copy-pasted `docker build -f runner/Dockerfile ...`).
- First-run dev bootstrap → `curie install` (or `./get-curie.sh` from a source checkout, which also puts `curie` on PATH the first time by building it).
- Refresh the on-PATH CLI after a code change → `curie update` (rebuilds and `cargo install`s the CLI to `~/.cargo/bin`; `--image` also rebuilds the runner). The per-change loop, so you never re-run the bootstrap script.
- Run the local stack on YOUR checkout → `curie local up --build`. `curie update` covers the CLI and the runner image; the stack's api, worker, dispatcher and ui otherwise come from the registry, so a source-built CLI ends up talking to whatever was last published. That skew does not announce itself — it surfaces as a serde error about a field name, or a missing Python module from inside a container (#1915).
- Contributor/CI scripts (contract codegen, chart render-asserts, the e2e round-trip) → `curie dev <...>`. The `dev` namespace fences off commands that need a **source checkout + dev toolchains**; they error clearly when run from a released binary.
- Operator/product commands stay top-level: `init`, `build`, `skill`, `local`, `cluster`.

New tooling ships as a `curie` subcommand (add the clap surface in
`cli/src/main.rs`, the handler in `cli/src/commands.rs`); a new loose script in
`scripts/` should be the exception with a reason, not the default.

## Release train selection

`main` is the stable v0.6.x line and `next` is the v0.7.0 integration branch.
Before starting work, choose the release train from the branch table in
[`AGENTS.md`](AGENTS.md#release-train-branch-and-commit-conventions), then create
the worktree and PR against that base. Do not assume `main` is the target for
every feature.

## Architecture Decision Records (ADRs)

`docs/adr/` is the system of record for architecture decisions. Each ADR captures
**what was decided and when**, along with the reasoning and the alternatives
weighed at that time. That record is durable even after a decision evolves: ADRs
are immutable once Accepted, so when the thinking changes you add a **new ADR that
supersedes** the old one rather than editing it. The chain of supersessions is the
history of how intent shifted over time (the intent-gap record), not just a
snapshot of the current state.

Workflow for non-trivial work:

- **For an architectural decision that qualifies for an ADR under
  [`AGENTS.md`](AGENTS.md), write a Draft ADR**, numbered sequentially in
  `docs/adr/`. A Draft may merge for discussion but cannot authorize
  implementation.
- **Obtain explicit maintainer acceptance before implementation.** Acceptance
  means the ADR is published with `Status: Accepted`. Normally, acceptance must
  precede implementation. As the coordinated exception, an ADR may land
  `Accepted` alongside implementation only with recorded explicit maintainer
  approval and a named realizing code path.
- **Issues reference their Accepted ADR(s).** A GitHub issue that implements a
  decision links the ADR, so an agent picking up the issue reads the decision's
  intent first and builds to it.
- Division of labor: **ADRs are the "why + when"**, **issues are the "what + track
  the work"**, and **`ARCHITECTURE.md` is the "what talks to what."** Keep decision
  rationale in the ADR, not duplicated across issues. Follow
  [`docs/adr/AGENTS.md`](docs/adr/AGENTS.md) for the complete procedure.
