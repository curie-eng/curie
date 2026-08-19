# agents.md: the verification contract for driving Curie

Curie is a harness that runs the same immutable bundle and the same eval suite
(the bundle's own evals/cases.json) across three tiers, skill, local, and
cluster, so an agent that worked locally does not silently break once deployed.
Its CLI's primary user is a coding agent driven by a developer, and this file is
the contract that agent works to.

If you are instead working **on** the Curie repository itself, the file you want
is [`AGENTS.md`](../AGENTS.md) at the repo root. This one is about driving the
harness, from a released binary or someone else's bundle.

## The surfaces you can read

- [`llms.txt`](../llms.txt) at the repo root: the curated machine map of every
  doc here, organized around the parity ladder. Start there when you have a
  checkout.
- `curie guide`: the self-contained primer for driving the harness, carried
  inside the binary, so it needs no checkout. `curie guide --json` for the
  structured form.
- `curie schema`: the machine-readable command manifest. **This is the authority
  on what commands exist.** It is hidden from `--help`, and it is real.
- `curie schema-index`: the committed, versioned JSON Schemas for every `--json`
  result, so you can check that a payload field exists before you trust it.

## Ask the human, or do it yourself

**Do it yourself: all non-secret plumbing.** Scaffolding a bundle, bringing a
tier up, deploying, running evals, wiring connectors, tearing down. Do the work
rather than handing the developer a shell checklist.

**Ask the human for the VALUES of secrets and API keys.** Never type a
credential yourself. If a credential already exists in the environment or in the
bundle's own `.env`, use it rather than asking for it again.

**Ask the human to create external applications in a browser**, a Slack app for
example, so its tokens can be minted. You cannot do that part.

**Ask the human before anything that costs money on a real provider**, and
before promoting past a red eval.

## The verification contract

House rule, and the reason it is written down: a command is checked by CI only
when it sits inside backticks or a fenced block in this file. A command written
in bare prose escapes the gate, so every command below is backticked
deliberately, not decoratively.

**Never report success from textual evidence.** A file existing, a string
appearing in output, or a command not erroring is not evidence. Report success
only when a command from this contract exits 0 **and** the stated fact holds in
its `--json` payload.

**After scaffolding a bundle with `curie init`:** `curie skill check --json`
exits 0 and `verdict` is `"green"`. Every server in `declared` has a non-null
`registered` counterpart in `matches` with `connected` true. This runs offline
and needs no credential. A green `--fake-model` run does not cover this: the
fake model never calls MCP tools.

**After `curie skill up`:** `curie skill status --json` exits 0 and reports both
a `url` and a `session`.

**After a deploy, at the tier you deployed to.** Angle-bracket placeholders are
banned in this contract, so all three tiers are written out in full:

- skill tier: `curie skill status --json` then `curie skill eval --json`
- local tier: `curie local status --json` then `curie local eval --json`
- cluster tier: `curie cluster status --json` then `curie cluster eval --json`

At the skill tier the bundle is the session, so there is no separate deploy
step. `eval` runs the bundle's OWN evals/cases.json, the same file at every
tier.

**Success on an eval is `failed` equal to 0 AND `plumbing_ok` equal to 0.**
`plumbing_ok` counts cases that completed on the fake model and were therefore
never graded (ADR-0055), and `passed + failed + plumbing_ok == total`. A run
that is all `plumbing_ok` is not a pass, it is an ungraded run.

**To verify a ticket's acceptance criteria at the skill tier:** `curie scenario
scenarios/ticket-1234.json --json` exits 0 and `verdict` is `"passed"`, with
`identity_verified` true on the `skill` entry in `tiers`. A `verdict` of
`"plumbing_ok"` is an ungraded run, not a pass. The `local` and `cluster` tiers
are refused rather than degraded, because the platform API exposes no way to run
a probe against a CLI-deployed version or to tear one down. The fixed weather
ladder (`curie dev e2e-ladder`) is a separate platform smoke test and does not
answer a ticket's criteria.

**When a command is uncertain, `curie schema` is the authority.** Do not invoke
a command you have not seen resolve there.

## Do not assume

Do not assume any capability of Curie, including an HTTP API, authentication, an
OpenAPI description, an MCP surface, or hosted documentation, unless it is
listed in this file or resolves in `curie schema`.
