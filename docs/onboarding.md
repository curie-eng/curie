# Local Dev Onboarding

This is the **contributor** path for engineers working on Curie. To use a
released co-worker, start with [Software Factory](software-factory.md),
[bring your agent](bring-your-agent.md), or the
[quickstart chooser](../QUICKSTART.md#choose-your-starting-point). Installing a
release does not require cloning Curie or building its contributor toolchains.

Use this walkthrough to go from a fresh clone to a verified local turn. Stay on
the `skill` and `local` targets for onboarding. They need no Slack workspace and
no Kubernetes cluster.

## Prerequisites

Install the local path tools:

- `uv`, the Python workspace manager, and Python `3.13`. The workspace requires
  `>=3.13`.
- Docker with Compose v2, for the dev stack and the local runner container.
- A `curie` CLI binary. Download, verify, and install the prebuilt binary for
  your platform by following
  [docs/release-verification.md](release-verification.md#verify-the-cli-before-installing-it).

Contributors working on the CLI can instead build from source with the Rust
stable toolchain (edition `2021`):

```bash
cd cli && cargo build --release
```

The source build writes the binary to `cli/target/release/curie`. Put the
binary on `PATH`; the commands below assume `curie` resolves.

`kubectl` and `helm` are not needed for local dev. They are only for the
`cluster` target.

If you build the CLI from source, build the runner image once from the repo
root with `curie build` (the single build entry point):

```bash
curie build
```

A released binary instead pulls the pinned runner image from `GHCR` on first
run, so only source builds need this.

## Pick a Target

The three targets (`skill`, `local`, `cluster`) and when to reach for each are
described in the [CLI target guide](../cli/README.md#using-different-tiers).
For onboarding, `skill` names the runner only tier; an authored bundle skill is
the artifact at `skills/<name>/SKILL.md`. Start with `skill` for the fastest
proof a bundle answers, then move
to `local` for ticket verification — it puts the full platform in front of the
runner with no Slack or Kubernetes. `curie init` scaffolds a bundle and
targets no environment.

## Fastest First Reply With `skill`

The fastest first reply — `curie init` then `skill up`/`message`/`down` — is
the [QUICKSTART](../QUICKSTART.md) flow.

If `skill up` reports that the container name is already taken, a previous
runner is still around: re-run with `--replace` to remove it and boot fresh. To
clear a leftover runner from a directory that has no recorded state (a bundle
you never ran `skill up` from, or one whose state file is gone), name it
directly with `curie skill down --name <container>`.

## The Real Product Loop With `local`

Bringing up the full compose platform and driving a turn through it —
`local up` / `local deploy` / `local message`, plus `--minimal` for low-RAM
machines — is covered in the
[CLI local walkthrough](../cli/README.md#local-target).
`curie local up` flips to live automatically when a credential
(`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`) is present, so there is no
manual `CURIE_FAKE_MODEL=0` step.

For multi turn work, `local message` prints a `continue this conversation:`
line. Use `curie local message --continue "follow up"` to reuse the last
turn's channel and thread from `.curie/last-turn.json`. Send only the new <!-- doclint:ignore-line -->
text. `--continue` reads the API key again from `$CURIE_API_KEY` and does not
replay transport flags like `--listen-port`, so pass those again if you used
custom values.

## Verify a Ticket End to End

Use this as the verification loop for a code change:

1. Rebuild what changed. Rebuild the runner image with
   `curie build` if you touched runner
   code. Run
   `curie local deploy --plugin-dir . --slack-channel C0123ABCD --api-url http://localhost:28000`
   again to push a changed bundle.
2. Drive a real turn with `curie local message "..."`. It walks the full
   queue, worker, sandboxed runner, and reply path, the same path a Slack mention
   takes.
3. Observe the result three ways:
   - The reply prints to **`stdout`**. Redirect it with
     `curie local message "..." > reply.txt`; progress goes to `stderr`, so it
     pipes cleanly.
   - The `worker` and `runner` log to `stdout`, so
     `docker compose -f compose.dev.yaml logs -f curie-worker`, or the runner
     container, makes the turn observable.
   - On the `full` profile, the turn is traced in `Langfuse`. This is skipped on
     `--minimal`.
4. `curie local status` confirms the compose services are healthy, with
   `docker compose ps` under the hood. `curie local down` stops the stack and
   keeps volumes.

This is a complete end to end verification with no Slack and no cluster, which
is the point of the `local` target.

## Where to Go Next

- [`QUICKSTART.md`](../QUICKSTART.md): first reply in about a minute.
- [`cli/README.md`](../cli/README.md): full command and flag reference for all
  three targets.
- [`docs/operations.md`](operations.md): the `cluster` target runbook and the
  full tier table.
- [`AGENTS.md`](../AGENTS.md): repo rules, verify commands, and dev stack
  gotchas.
- [`docs/vision.md`](vision.md): what Curie is and why.
