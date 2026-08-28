# Agent instructions: test

This is a Curie bundle (a Claude Code plugin shape). The full harness
primer is one command away and is the source of truth:

    curie guide

## The loop

1. `curie skill up --fake-model` -- boot the runner offline, no credential.
   The fake model is plumbing, not a subject under test: it answers every input
   with the same canned reply, so nothing it says is evidence about behavior.
2. Edit `skills/test/SKILL.md` (behavior) and `evals/cases.json` (the contract).
3. `curie skill up --replace --fake-model` -- the runner executes an immutable
   snapshot of the bundle taken at `skill up`, so a `SKILL.md` edit reaches it
   ONLY after a restart. Skip this and step 4 grades the pre-edit bundle with
   no sign anything is stale. (`evals/cases.json` is read live from source, so
   the contract does not need the restart -- only the behavior does.) Confirm
   what is loaded with `curie skill status --json` and its `bundle_digest`.
4. `curie skill eval` under `--fake-model` reports `plumbing_ok` -- it proves
   the turn completed, and grades nothing. Re-run it with a real credential to
   grade the cases; that green is the promotion gate. Merging to main promotes.
5. `curie skill down` when finished.

## What this bundle declares

- `skills/test/SKILL.md` -- behavior. The main thing to edit.
- `evals/cases.json` -- the promotion gate. Make it FALSIFIABLE.
- `connectors.yaml` -- what this agent needs RUNNING (an MCP server Curie
  hosts for it). Scaffolded empty and commented; Curie derives the Deployment,
  Service, hardening, host allowlist, NetworkPolicy, and the URL the agent
  dials, so none of that belongs in this repo. Never hand-write that URL: it
  embeds the release, the agent, and the namespace, so it differs per agent.
- `deploy.yaml` -- WHERE this bundle goes. `curie cluster deploy --target prod`
  reads it, so routing is a reviewable diff instead of flags in CI. The bundle
  is identical across targets; only the binding differs.

## Rules

- Verify before running: `curie schema` lists every real command; never
  invoke one you have not confirmed.
- The eval file is the promotion gate and never changes across tiers
  (skill/local/cluster). Grading, and therefore green and red, is a
  real-credential concept; never deploy on red.
- Landmines: run `curie guide` (or read
  `.claude/skills/using-curie/SKILL.md`) for the full, current list.
- If a step here needs human sign-off, do NOT collect it in the skill. Call the
  built-in `request_approval` tool and end the turn: the platform posts the card,
  authorizes the resolver server-side, and resumes you with a turn beginning
  `[approval resolved]`, which this skill must handle. `curie guide` has the
  section; `docs/approvals.md` in the Curie repo has the full walkthrough.
- The scaffolded eval is a starter smoke test: it only checks the agent named
  itself, so it fails on an empty/errored turn but proves nothing about the
  real work. Replace it with a FALSIFIABLE grader -- one a plausibly-broken
  agent would fail -- as the first authoring step (ADR-0022).
- A bare greeting ("hey", "hi") is answered by the real model by default --
  a full sandbox claim and model turn for something a canned reply could
  handle for free. If this agent gets greeted often, consider a `greeting`
  behavior pack: `GET`/`PUT /agents/{id}/behavior-packs` (no CLI verb yet)
  short-circuits a bare greeting/help request before the model ever runs.
  See `docs/behavior-packs.md`.
