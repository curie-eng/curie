# agents.md: the verification contract for driving Curie

Curie is a harness that runs the same immutable bundle and the same eval suite
<!-- doclint:ignore-line -->
(the bundle's own `evals/cases.json` and optional `evals/trajectory.json`)
across three tiers, skill, local, and cluster, so an agent that worked locally
does not silently break once deployed.
Here, `skill` names the runner only tier. An authored bundle skill is the
artifact at `skills/<name>/SKILL.md`.
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

## Supported SRE bundle

The source checkout's `examples/sre-bot` is a supported bundle under ADR-0041.
Its [surface contract](../examples/sre-bot/supported-surface.json) requires the
same capability decisions at skill, local, local-release and cluster tiers.
Externally hosted connectors supply the declared unhosted URLs where the tier
cannot host them. Missing hosting or credentials is an unproved tier, never an
absent capability accepted as parity. Live provider and external integration
observations additionally prove SDK gating, genuine decisions and upstream effects.

From a source checkout, install the checker dependencies and validate declarations:

```bash
python -m pip install -r tools/sre-contract/requirements.txt -e packages/plugin-format
python tools/sre-contract/check.py
python -m pytest -q tools/sre-contract/tests
```

The [bundle schema](../examples/sre-bot/supported-surface.schema.json) governs
the combined manifest, connectors and supported surface. The consumer compares
the [permission map](../examples/sre-bot/docs/PERMISSION-MAP.md) and uses the
product's policy classifier. A static pass proves declaration consistency only.
The supported reads are classified explicitly. The complete image catalog and
eight starter-prompt observations in #2285 remain separate acceptance evidence.

For catalog verification, place every connector's real MCP URL in a protected
JSON object keyed by connector name. Run the following with that file path:

```bash
python tools/sre-contract/check.py --endpoints "$SRE_CONNECTOR_ENDPOINTS"
```

It enumerates every catalog, including image-only connectors, rejects missing or
empty catalogs, checks effective policy and executes the existing
`assert-gates-are-live-tools.py` checker. The Plugin compat workflow runs
the following against the declared images and built connector source:

```bash
python tools/sre-contract/catalog_ci.py
```

That job uses inert credentials and only lists tools; it does
not prove an upstream API call, SDK model visibility, Slack approval or RBAC.
Its prerequisite consistency failure remains red rather than allowing a skip to
count as proof. HTTP fixtures are expressly separate from actual image catalogs.

At each tier, record the exact bundle and eval digest, candidate and command,
then prove a read, pending write, denied unchanged state, fresh approval and
post-action readback. On cluster installs verify both upgrade CronJobs exist
before arming them, and execute the RBAC ceiling refusal. Use the commands and
expected observations in the [SRE demo contract](../examples/sre-bot/DEMO.md).
Run the same applicable cases through `curie skill eval --json`,
`curie local eval --json` and `curie cluster eval --json` with configured real
connectors. A fake evaluation cannot close those observations. Record a causal
mutation failure and restored healthy result for each guard; removing a required
tier, a permission row, connector or gate must fail the source contract as well.

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

**After `curie cluster upgrade --to 0.9.0 --json`:** `status` is `"succeeded"`
only when `convergence.exact` is true and `canary.passed` is true. A failed
attempt reports `previous_serving` or one `fail_forward.command`. Resume by
re-running the same command. `curie cluster status --json` reports the current
upgrade phase and `known_good_version`.

At the skill tier the bundle is the session, so there is no separate deploy
<!-- doclint:ignore-line -->
step. `eval` runs the bundle's OWN `evals/cases.json` at every tier. Default
`skill`, `local`, and `cluster` eval execute that suite without ambient durable
agent memory, so a deployed preference cannot change a committed case. When the
<!-- doclint:ignore-line -->
bundle also carries `evals/trajectory.json`, every case is scored from the
observed tool sequence. Skill reads runner tool frames directly. Local and
cluster trigger the deployed bundle through the platform eval plane and read
the exact triggered matrix stream. Deploy the changed bundle before running
`curie local eval --json` or `curie cluster eval --json`. Those two trajectory
runs refuse `--cases` because a local override cannot alter the deployed
bundle.

The trajectory sidecar maps each `case_id` to an `expected` tool sequence, a
`mode` of `exact`, `in_order`, `any_order`, `precision`, or `recall`, and a
`threshold`. The case file and sidecar are packaged in the same immutable
deployed bundle. The deploy receipt's `bundle_sha256` is the parity identity
for local and cluster. A case with no matching spec fails closed with an
explanatory `detail`; it never passes by omission.

Trajectory records a tool request before that tool executes. A denied or failed
request can therefore satisfy tool identity and order. A trajectory green proves
that the expected request sequence was observed. It does not prove the tool was
reachable or that execution succeeded.

**A parity example case is falsifiable only when it goes RED after the specific
capability under test is removed.** Run the same case after removing that
capability and record the RED result. A null agent run is still a useful
vacuousness control, but it is not sufficient on its own because it removes the
whole agent shape rather than the identity of the capability being proved.
Use an executable control through the real consumer path. One valid example is
to keep a tool declared, route it through an approval gate, supply no approval,
and observe that the tool is denied. Other controls are valid when they make
the named capability unavailable through the surface the case exercises.

Keep capability denial and oracle discrimination as separate observations when
one run cannot prove both. For weather, the live ungranted approval gate made
WebFetch unavailable, but the turn awaited approval and eval failed before
trajectory scoring. A separate focused regression completed a turn with
WebSearch but no WebFetch and observed the committed trajectory oracle go RED.
That regression proves a missing WebFetch request is detected. Do not describe
the approval denial run as a trajectory grade. More generally, a turn that does
not complete can fail before graders run, so one RED result may prove only the
earlier failure and not the named grader.

**Success on an eval is `failed` equal to 0 AND `plumbing_ok` equal to 0.**
`plumbing_ok` counts cases that completed on the fake model and were therefore
never graded (ADR-0055), and `passed + failed + plumbing_ok == total`. Each
case also reports `samples`, `passes`, and `policy` so a one-sample miss is
labeled as one draw, not unexplained tier drift. Default is one sample with
majority aggregation; `curie skill eval --samples 3 --json`,
`curie local eval --samples 3 --json`, and
`curie cluster eval --samples 3 --json` raise N on the in-CLI path. A run
that is all `plumbing_ok` is not a pass, it is an ungraded run.

**An eval's exit code is the gate: 1 at least one selected case failed, 2 the
selector matched no cases, 3 the runner or platform API was unreachable or
timed out (transient, retryable), 4 the selector does not apply to this eval
plane, 0 no case failed.** Exit 0 is not the same as success -- an all
`plumbing_ok` run also has `failed` equal to 0 and so also exits 0; apply the
`failed` equal to 0 AND `plumbing_ok` equal to 0 rule above before reading an
exit code as a pass. `--case-id <ID>` (repeatable, at `skill`, `local`, and `cluster`)
selects which cases run; omitting it runs the whole suite. A value that matches
no case in the suite exits 2 and names the unmatched value, so a mistyped
`--case-id` fails the gate instead of greening an empty run -- including when
only one of several values is mistyped, which would otherwise drop a case
silently. Exit 4 is the honest refusal where a local selection cannot reach the
work: a `local`/`cluster` `--model` sweep and the `local`/`cluster` trajectory
eval both grade the deployed suite server-side from a suite NAME alone, so
`--case-id` is declined there rather than ignored. `curie skill eval --case-id`
IS honored on a `--model` sweep, because that sweep boots a transient local
runner per model and grades in-CLI, so a local selection reaches the run.

**When a command is uncertain, `curie schema` is the authority.** Do not invoke
a command you have not seen resolve there.

## Do not assume

Do not assume any capability of Curie, including an HTTP API, authentication, an
OpenAPI description, an MCP surface, or hosted documentation, unless it is
listed in this file or resolves in `curie schema`.
