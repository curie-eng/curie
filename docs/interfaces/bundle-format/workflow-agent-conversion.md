# Guide: converting a workflow agent to a bundle

> Part of the #30 epic (bundle format & authoring extensions). Companion to
> [INTERFACE.md](./INTERFACE.md) (the frozen bundle shape).

This guide walks an existing **workflow agent** — a deterministic pipeline with an
LLM at the edges — onto the Curie bundle format end to end: scaffold, map each
piece, wire tools, validate, run locally, add eval cases, and deploy.

The bundle format is the **Claude Code plugin shape verbatim**
(`packages/plugin-format/`); nothing here invents format extensions. If a bundle
validates with `validate_bundle`, every deploy path accepts it.

## The worked example

We'll convert a real workflow agent, **deal-desk**, that today runs as a standalone
script:

1. A Slack message arrives asking to price a deal ("quote 250 seats of Pro, 2-year,
   EDU discount").
2. **LLM (edge in):** parse the free-text ask into structured fields
   (`{product, seats, term_years, segment}`).
3. **Deterministic pipeline (core):** look up list price, apply the segment/volume
   discount rules, compute totals, check the approval threshold. Pure functions and
   a pricing-table lookup — no model in the loop.
4. **LLM (edge out):** write the human-readable quote summary and the caveats.

The shape to preserve: the model handles the *fuzzy edges* (understand the ask,
phrase the answer); the deterministic middle stays deterministic. The bundle format
maps this cleanly — you do **not** turn the pipeline into a prompt.

## The mapping at a glance

| Workflow agent piece | Bundle home | Why |
| --- | --- | --- |
| The agent's job + when to act + how to answer | skills/deal-desk/SKILL.md | The skill is the model-facing contract: trigger + procedure + hard rules. |
| LLM edges (parse the ask, phrase the answer) | Prose steps in `SKILL.md` | These *are* the model's work; they live as instructions, not code. |
| Deterministic pipeline (pricing rules, totals, thresholds) | An MCP tool server in `.mcp.json`, or a `scripts/` helper the skill calls | Keeps determinism out of the prompt; the model *calls* it, never re-derives it. |
| External systems (pricing DB, CRM) | MCP servers in `.mcp.json` | Wire, don't inline; credentials come from env. |
| One-time setup (build the pricing index) | scripts/setup.sh | Directory convention; no schema of its own. |
| Regression cases | evals/cases.json | Feeds `curie skill eval`. |
| Name, version, description | .claude-plugin/plugin.json | The manifest; `name` is the only required field. |

## Step 0 — scaffold the skeleton

```bash
curie init deal-desk        # -> ./deal-desk
```

`curie init` (`cli/src/scaffold.rs`) writes exactly the frozen shape and refuses
to overwrite anything that already exists:

```
deal-desk/
  .claude-plugin/plugin.json   # manifest: {name, description, systemPrompt, version}
  skills/deal-desk/SKILL.md     # skill with YAML frontmatter
  .mcp.json                     # { "mcpServers": {} }
  evals/cases.json              # { name, cases: [{id, input, grader}] }
  .gitignore                    # .curie/
```

The name must be kebab-case (`^[a-z0-9]+(-[a-z0-9]+)*$`) — the same rule the
validator and the scaffolder enforce.

## Step 1 — the manifest

<!-- doclint:ignore-line -->
`.claude-plugin/plugin.json`. `name` is required; `version`, `description`, and the
other Claude Code keys (`author`, `homepage`, `repository`, `license`, `keywords`,
`commands`, `agents`, `hooks`, `mcpServers`) are optional and preserved if present.

```json
{
  "name": "deal-desk",
  "description": "Prices deals from a free-text ask: parses the request, applies the pricing rules, and writes the quote.",
  "version": "0.1.0"
}
```

Unknown keys are accepted (`extra="allow"`), so a manifest authored for Claude Code
carries over unchanged.

## Step 2 — write the skill (the LLM edges)

<!-- doclint:ignore-line -->
`skills/deal-desk/SKILL.md`. The frontmatter needs `name` and `description`;
`allowed-tools` is optional but is how you scope what the model may call. Use the
**real** field name `allowed-tools` (not `tools`), and write its value as the canonical
space-separated string — several tools go on the one line (`allowed-tools: pricing Read`), and a
specifier keeps its own spaces because the value splits at parenthesis depth 0
([ADR-0135](../../adr/0135-a-skill-bundle-has-two-conformance-profiles.md), Draft).

The body is the procedure. Put the deterministic steps behind a tool call — the
model's job is to *gather inputs, call the tool, and phrase the result*, never to do
the arithmetic itself.

```markdown
---
name: deal-desk
description: Price a deal from a free-text ask. Invoke whenever a user asks for a quote, pricing, a discount check, or "how much for N seats".
allowed-tools: pricing            # the MCP tool server wired in .mcp.json (Step 3)
---

# Deal desk

## When to run
The user asks for a price, a quote, a discount, or the cost of a plan/seat count.

## How to answer
1. Parse the ask into fields: product, seats, term (years), segment (edu/gov/none).
   If a required field is missing, ask ONE short question instead of guessing.
2. Call the `pricing.quote` tool with those fields. Do NOT compute prices yourself —
   the tool owns the list price, discount rules, and approval thresholds.
3. Report the quote in 3-4 lines: line price, discount applied, total, and whether
   it needs approval (the tool returns `needs_approval`).
4. If `needs_approval` is true, say so explicitly and name the threshold it crossed.

## Hard rules
- Never invent or round prices; use the tool's numbers verbatim.
- Never apply a discount the tool did not return.
- If the tool errors, report the failure and what you sent — do not fabricate a quote.
```

This is the whole point of the conversion: the fuzzy edges become prose, the
deterministic core stays code the model *invokes*.

## Step 3 — wire the pipeline as tools (`.mcp.json`)

The deterministic pipeline becomes an MCP server the skill calls. `.mcp.json` maps a
server name to either a stdio server (`command`, `args?`, `env?`) or a remote server
(`type`, `url`, `headers?`). The validator requires each server to define either
`command` or `url`.

Stdio (your pricing pipeline packaged as a local command):

```json
{
  "mcpServers": {
    "pricing": {
      "command": "python",
      "args": ["-m", "deal_desk.pricing_mcp"],
      "env": { "PRICING_DB_URL": "${PRICING_DB_URL}" }
    }
  }
}
```

Remote (a pricing service you already run):

```json
{
  "mcpServers": {
    "pricing": {
      "type": "http",
      "url": "https://pricing.internal.example.com/mcp",
      "headers": { "Authorization": "Bearer ${PRICING_TOKEN}" }
    }
  }
}
```

Credentials come from env, never inlined. If your pipeline is a handful of pure
functions rather than a service, the smallest MCP server that exposes `quote` as a
tool is enough — the goal is that the model calls it, so the numbers never live in
the prompt. One-time preparation (building the pricing index) goes in
`scripts/setup.sh` (a directory convention with no schema of its own). <!-- doclint:ignore-line -->

### Porting checklist for the pipeline
- Every deterministic step → a tool (or an argument of one), never a prompt
  instruction.
- Anything the old agent read from a file/DB/CRM → an MCP server + an env-supplied
  credential.
- Anything the old agent *phrased* for a human → prose in `SKILL.md`.
- Keep the tool surface small: expose the pipeline's public operations, not its
  internals.

## Step 4 — validate the shape

Every deploy path calls the single `validate_bundle`; run it before anything else:

```python
from plugin_format import validate_bundle
result = validate_bundle("deal-desk")
for issue in result.errors:
    print(issue.code, issue.location, issue.message)
assert result.valid
```

Fix any path-qualified issue it reports (`manifest.name_invalid`,
`skill.frontmatter_missing`, `mcp.server_incomplete`, `mcp.invalid_json`,
`scripts.not_a_directory`, …). A green `validate_bundle` means the CLI, the bundle
pipeline, and the runner loader will all accept it.

## Step 5 — run it locally (no Slack workspace)

Boot a local runner for the bundle and drive it with synthetic events:

```bash
curie skill up --plugin-dir ./deal-desk --fake-model   # offline, no credential
curie skill send "quote 250 seats of Pro, 2-year, EDU discount"
curie skill status
curie skill down
```

`curie skill up` boots a local runner container for the bundle (`--plugin-dir`
defaults to `.`). `--fake-model` round-trips the ACI events with the scripted fake
model (no credential) so you can exercise the wiring first; drop it and pass
`--model` + a credential for a real run. Use `curie skill up --network
curie_default` to join the dev stack if the pipeline's MCP server needs the
compose services.

## Step 6 — pin behavior with eval cases

Turn the workflow's known inputs/outputs into regression cases in
`evals/cases.json` (a suite `{name, cases: [{id, input, grader}]}`), then: <!-- doclint:ignore-line -->

```bash
curie skill eval          # defaults to evals/cases.json, runs through the runner
```

Start with the `contains` grader the scaffold seeds (assert the reply names the
total and whether approval is needed), and add a case per pricing rule you care
about (volume tier boundaries, the approval threshold, a missing-field ask). These
are your guardrail that the conversion preserved behavior.

## Step 7 — package and deploy

Once it validates, runs, and its evals pass, hand the directory to the bundle
pipeline / deploy path (the runner's bundle loader consumes the same validated
shape). Nothing bundle-specific changes at deploy time — the frozen format is the
contract, and you have already met it.

## Common pitfalls

- **Turning the pipeline into a prompt.** The deterministic middle must stay a tool
  call. If the model is doing arithmetic or applying discount rules from prose,
  you've lost the determinism the workflow guaranteed.
- **`tools:` instead of `allowed-tools:`.** The verbatim Claude Code field is
  `allowed-tools`; `validate_bundle` hard-rejects `tools`/`allowed_tools`/`allowedTools`
  when `allowed-tools` is absent (`skill.tools_confusable`), so this is caught at
  validate time rather than silently granting no tools.
- **Inlined credentials.** Reference env (`${VAR}`) in `.mcp.json`; never commit a
  token.
<!-- doclint:ignore-line -->
- **Manifest in the wrong place.** Canonical is `.claude-plugin/plugin.json`; a bare
  root `plugin.json` is accepted as a fallback but prefer the canonical location.

## Cross-links

- [INTERFACE.md](./INTERFACE.md) — the frozen bundle format and its validator.
- `cli/src/scaffold.rs` — `curie init`, the frozen scaffold this guide starts from.
- `packages/plugin-format/README.md` — the format surface and decisions.
