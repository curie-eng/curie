# 60. The harness is a declared package, not a class

Date: 2026-07-21

Status: Superseded by [ADR 0140](0140-curie-supports-one-model-harness-until-a-second-one-exists.md)

**Supersedes [ADR-0011](0011-opencode-second-harness.md)** ("OpenCode as the
second harness behind the ACI"). Together with
[ADR-0061](0061-out-of-process-harness-boundary.md) it replaces the Proposed
[ADR-0031](0031-harness-neutral-runner-seams.md), whose five decisions survive
in changed form (see Decision 4 below). Per
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
0011 and 0031 carry status-line pointers here rather than edited bodies, and
[ADR-0005](0005-claude-agent-sdk-adapter-and-frozen-aci.md) carries an amendment
banner against its "at zero cost" claim.

## Context

ADR-0005 drew the line: everything inside the runtime boundary is "the harness",
everything outside is "the platform". The seam it built to hold that line is a
class. `runner/src/curie_runner/adapter.py:31-52` declares `ModelSession` as a
five method Protocol (`connect`, `query`, `receive_turn`, `interrupt`, `close`)
whose `receive_turn` returns `AsyncIterator[Any]`, documented as yielding "SDK
messages".

A harness in Curie is not that class. A harness is what that class implies
about the rest of the system, and today those implications are smeared across
nine or more files in two languages:

- `runner/Dockerfile:24` installs the engine
  (`npm install -g @anthropic-ai/claude-code`), and the surrounding comment
  records a Claude specific non root constraint that shapes the image.
- `cli/src/docker.rs:453` hardcodes `RUNNER_IMAGE`, consumed at
  `cli/src/artifacts.rs:132` and `cli/src/commands.rs:462`.
- `apps/worker/src/curie_worker/run.py:126` picks the image again on the
  worker side, defaulting to the same string.
- `apps/worker/src/curie_worker/sandbox/docker.py:310-326` builds the spawn
  environment, carrying a hand copied mirror of the runner's credential rules.
  The comment at `apps/worker/src/curie_worker/sandbox/docker.py:99` admits it:
  it is "a literal mirror of runner/src/curie_runner/sdk_auth.py".
- `runner/src/curie_runner/sdk_auth.py` is 401 lines of auth plus a hard gate
  at `runner/src/curie_runner/sdk_auth.py:377-386` that refuses any non
  Anthropic Messages wire because "the runner speaks the Anthropic Messages wire
  format via claude-agent-sdk".
- `runner/src/curie_runner/__main__.py` encodes boot ordering,
  `runner/src/curie_runner/adapter.py` returns `ClaudeAgentOptions` directly
  from `build_options`, `runner/src/curie_runner/translate.py:84-107` is an
  isinstance chain on SDK types, `runner/src/curie_runner/side_effects.py:28-40`
  hardcodes Claude Code's PascalCase read only tool names, and
  `runner/src/curie_runner/plugin.py` hands the bundle straight to the SDK.

The concrete price of having no declarative install contract is on record. The
withdrawn OpenCode work needed
`runner/src/curie_runner/opencode/installer.py` at 517 lines, plus
`runner/src/curie_runner/opencode/auth.py` (35 lines) and
`runner/src/curie_runner/opencode/__main__.py` (131 lines), none of which is
interface work. It is packaging and wiring, reinvented because there was nowhere
to declare it.

**Omnigent is the shape we are missing.** Omnigent is Databricks' Apache 2.0
"meta harness" (alpha, v0.5.1). A harness there ships as a pip installable
package declaring a Python entry point:

```toml
[project.entry-points."omnigent.community.harness"]
foo = "omnigent.community.harness.foo.plugin:get_contribution"
```

That entry point exports `get_contribution() -> HarnessContribution`, carrying
`name`, `valid_harnesses`, `harness_modules`, `aliases`, `harness_labels`, plus
install and auth metadata, model override environment variables, and per spawn
environment builders. Core refuses plugins that register flat package paths or
try to override a built in harness name. Harness selection is declarative: a
YAML `executor: harness: claude-sdk` field or a `--harness` CLI flag. Omnigent
also documents a scope boundary: direct and headless harnesses only, with
community native TUI harnesses explicitly not pluggable.

The insight, stated plainly: **our harness seam is a class, with everything else
about a harness scattered through core. Omnigent's harness seam is a package
that declares everything about itself.** The unit of a harness should be a
package, not a class.

## Decision

**1. The unit of a harness is an installable package declaring a contribution
manifest.** One object, authored in the harness package, carries: identity,
aliases and labels; the install spec (what the image must install and how); the
auth spec (which credential keys and env shapes the engine wants); the declared
read only tool set; model override env keys; a per spawn environment builder;
and a bundle compile hook. Everything the nine file scatter above encodes
implicitly becomes a field somebody wrote down on purpose.

**2. Registration is by entry point, with Omnigent's guard rules stolen
verbatim.** Core discovers harnesses through a declared entry point group. Core
**refuses** a plugin that registers a flat package path, and **refuses** a plugin
that claims the name of a built in harness. Both refusals are fail closed: an
ambiguous registry is worse than a missing harness, and silent shadowing of the
Claude harness is the single worst failure this registry could have.

**3. Harness selection becomes declarative config.** A `harness:` field plus a
CLI flag, resolved once and carried, replacing today's baked in image assumption
in the Rust constants (`cli/src/docker.rs:453`) and the worker's env default
(`apps/worker/src/curie_worker/run.py:126`). Selecting a harness stops being a
matter of which image string three separate call sites happen to agree on.

**4. ADR-0031's decisions become manifest fields, not separate ports.** Its
decision 2 (harness declared tool identity), decision 3 (a `BundleInstaller`
port) and decision 5 (no options abstraction) all survive, but they stop being
three independent extractions and become three fields on one manifest. Decision
4 (history and resume is descoped, not abstracted) carries forward unchanged: it
still starts with an `aci-protocol` contract change and still needs its own ADR.
Decision 1 (the runner owned `TurnEvent` union) is answered by ADR-0061 and is
retained here only as that ADR's recorded fallback.

**5. Non goal, taken from Omnigent: direct and headless harnesses only.** A
harness that exists only as a native TUI is out of scope for this registry. This
is the line that saves us from chasing Cursor's TUI and calling it a harness
integration. If an engine ships a headless or server mode, it is a candidate; if
it does not, the answer is no, and the answer does not require a spike to
discover.

**6. OpenCode as the second harness is withdrawn, and this is the record of
why.** ADR-0011 gated adoption on a steer spike. **The spike succeeded.** A live
`opencode serve` backed session passed the frozen conformance suite on real
model turns with zero core changes, which is exactly what ADR-0011 asked for.
The work was nevertheless withdrawn: all seven OpenCode PRs (#226 and #315
through #321) were closed unmerged on 2026-07-17 and no OpenCode code exists on
main. The reasons were the synthesis tax that ADR-0031 named (the adapter forged
claude-agent-sdk dataclasses, including dummy `ResultMessage` fields nothing
reads), plus two un priced workstreams: the installer and the bundle compiler.

Recording this matters because ADR-0011's own escape clause only contemplated
supersession if steer proved unreachable. Steer was reachable. Without this ADR,
0011 reads as a still live Accepted decision with its gate satisfied, and the
next person walks the same path and pays the same bill. Eight branches on origin
preserve the work for mining rather than re implementation:
`task/25-opencode-harness-spike`, `task/307-turnevent-message-model`,
`task/308-harness-readonly-toolsets`, `task/309-bundle-installer-port`,
`task/310-opencode-bundle-compiler`,
`task/311-opencode-session-config-parity`, `task/312-opencode-runner-image`,
`task/313-opencode-parity-evals`.

## Alternatives considered

- **Keep the nine file scatter and document it.** A `HARNESS.md` listing every
  place a harness identity leaks costs nothing to write and catches nothing. The
  duplicated credential mirror at
  `apps/worker/src/curie_worker/sandbox/docker.py:99` is already documented, in
  a comment, at the site of the duplication, and it is still a duplication.
- **Extract the four ADR-0031 ports individually.** This is the status quo plan
  and it is not wrong so much as insufficient: four ports still leave the
  install spec, the auth spec, the image selection, and the spawn env
  unrepresented, which is where the 517 line installer actually came from.
- **A config file per harness instead of a package.** A YAML descriptor covers
  identity, install, and env, but not the per spawn env builder or the bundle
  compile hook, both of which are code. Splitting a harness across a descriptor
  and a package reintroduces the scatter at smaller scale.

## Consequences

**Does this shrink second harness work, or only relocate it?** ADR-0031's audit
test, applied honestly.

**It shrinks.** But not for the reason a cleaner seam usually claims. The
evidence is against the interface story: a scripted second implementation passed
the frozen conformance suite five times out of five in a single afternoon with
zero core changes, which means the interface was never the bottleneck. What this
ADR attacks is **packaging and wiring cost**, which is integration cost, not
interface cost: the 517 line installer, the nine file identity scatter, and the
hand copied credential mirror. Those are real, they are per harness, and a
manifest removes them by making them declarations instead of code.

**It does not shrink the bundle compiler, and it never will.** Our bundle is the
Claude Code plugin shape verbatim, which ADR-0005 chose deliberately as the
distribution wedge. Any non Claude engine needs a bundle to native config
compiler, and no registry, manifest, or entry point makes that translation
smaller. ADR-0011 said the same thing in its own words: the bundle translator,
not the ACI server, is the bulk of the work. The manifest gives that compiler a
declared home (the bundle compile hook) and nothing more.

**It does not shrink history and resume.** ADR-0031 decision 4 carries forward
unchanged. The consumer half exists, nothing in production produces a history
ref, and the frozen ACI `final` frame carries no session id. That work still
starts with an `aci-protocol` contract change under
[ADR-0036](0036-aci-semver-and-reader-policy.md)'s semver rules and still owes
its own ADR.

**A Pydantic manifest inherits two existing gates, and one obligation.** If the
manifest is authored as Pydantic inside a frozen package, it gets
[ADR-0017](0017-tri-language-contract-codegen.md)'s drift gate (JSON Schema and
the generated Rust and TS regenerate in CI, and `git diff --exit-code` fails the
build on drift) and ADR-0036's `schema/wire.lock` enforcement (hash changed with
version unchanged fails the build) for free. It also inherits the stop and
escalate rule that comes with a frozen contract: every manifest field addition
becomes a versioned contract change with tri language codegen. That is a real
tax on a surface we expect to iterate on while the first two harnesses teach us
its shape. **Freezing the manifest should be a deliberate choice made when the
shape settles, not an accident of where the file was placed.**

**The Rust and Python sides both have to learn the registry.** Harness selection
crossing from a Rust CLI flag to a Python worker to a container image is exactly
the sort of value ADR-0049's boot env contract governs, so the selected harness
becomes a declared boot env key rather than an ad hoc string.
