# 173. A bundle may layer its own runner image on the platform's

Date: 2026-09-24

Status: Draft

This ADR builds on [ADR-0113](0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md)
and [ADR-0158](0158-a-custom-connector-is-a-bundle-built-http-mcp-server-that-holds-its-own-credential.md).
It supersedes nothing. It records a direction; nothing implements it yet.

## Context

A bundle can use an off-the-shelf stdio MCP server only if the server's
executable is already in the runner image. [ADR-0113](0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md)
says so, and the runner cannot fetch one at start: its rootfs is read-only.
So a server one bundle needs is added to `runner/Dockerfile`, where
`mcp-server-github` already sits under "Add a line here to bless another authed
third-party MCP server". Every agent's runner then carries it.

Review of the mean tester (#3055, ADR-0172 in Draft there)
asked why every runner needs that agent's Slack server. The question was
whether each bot could have its own small Dockerfile that builds on the
runner.

There is one runner image per release today:
- Every agent's SandboxTemplate renders the one `agentSandbox.runner.image`
  (`charts/curie/templates/agent-sandbox.yaml:459`, `:585`, `:688`, on `next`
  at `5f750347`).
- The prewarm DaemonSet pre-pulls exactly that image
  (`charts/curie/templates/runner-prewarm.yaml:72`). A sandbox's cold boot never
  downloads an image: an in-boot pull caused the 2026-07-06 claim-timeout
  incident (`charts/curie/values.yaml:2229` and `:2238`).

Curie's per-bundle extension point today is the hosted connector
([ADR-0086](0086-bundles-declare-connectors-the-platform-hosts-them.md),
ADR-0113, ADR-0158). It is built per bundle and runs for that agent alone. It
fits only a server that speaks HTTP, and it is heavy: every connector of every
agent is its own Deployment, Service and NetworkPolicies, and an example's is
also its own release rows. Hosted connectors should stay limited to servers
that need their own process, or must hold a credential away from the sandbox.

`curie build --plugin-dir <dir> --registry <ref>` already builds a bundle's
declared connector sources. It pushes them and records each digest in
`connectors.lock.yaml` beside the bundle.

## Decision

**A bundle may declare a runner image layered on the platform's runner. That
agent's sandboxes run it, pinned by digest and prewarmed like the platform
runner. Every other agent keeps the platform image.**

1. **Declared in the bundle.** A Dockerfile whose base is the platform runner,
   passed in as a build argument rather than written as a tag. The bundle adds
   layers: typically one pinned `npm install -g` or `pip install` for a stdio
   server.
2. **Built by the Curie CLI, and pinned like a connector.**
   - `curie build --plugin-dir <dir> --registry <ref>` builds the layer too.
   - It records two digests in the bundle's lock: the layered image's, and the
     platform runner's that it was built on.
   - Whoever owns the bundle runs that build. A deploy renders only the
     recorded digest, never a tag.
3. **Only that agent runs it.** That agent's SandboxTemplate renders the
   bundle's digest. Every other template keeps `agentSandbox.runner.image`.
4. **Prewarmed per image.** Cold boot never pulls, so the prewarm covers each
   distinct runner digest a release renders, not only the platform's.
5. **Checked by the CLI, rebuilt by the bundle's owner.** The platform cannot
   rebuild bundles it never sees, since people build their own, so nothing
   rebuilds on their behalf.
   - `curie cluster deploy` refuses a bundle whose recorded base is not the
     installation's runner. Its message is the `curie build` command that
     fixes it.
   - `curie cluster upgrade` names every deployed agent whose layered image
     will stop matching, before it upgrades. Those bundles are rebuilt and
     redeployed by their owners.
   - How such an agent runs between the upgrade and its redeploy is an open
     question for the implementation issue. It must not silently run a runner
     the worker cannot serve.

## Consequences

- A bundle-specific stdio server stops being a platform change. When this is
  implemented, the servers `runner/Dockerfile` blesses for particular bundles
  leave it, along with its "Add a line here" invitation:
  - `mcp-server-github` moves to the bundles that use it (`examples/github-issues`
    and `examples/dark-factory`);
  - the mean tester's Slack server moves to `examples/mean-tester`.
- Each declared image costs registry storage and a prewarm pull on every node.
  It also costs its owner a `curie build` on every platform upgrade.
- The deploy and upgrade checks in decision 5 are new CLI work. They keep a
  layered image from running an old runner under a new worker.
- The bundle's layers run inside the sandbox, under the runner's security rails
  and egress policy. A declared image widens what one agent's sandbox contains,
  not what it can reach.
- Until this is built, a bundle-specific stdio server keeps being blessed in
  `runner/Dockerfile`. ADR-0172's Slack server does exactly that.

## Alternatives considered

- **Keep blessing servers in the one runner image.** This is today's path.
  Every agent carries every blessed server, and adding one is a platform
  change.
- **A hosted connector per server (ADR-0158).** It fits only servers that speak
  HTTP. Each one is another pod, another image with its own release rows, and
  often a shared token between runner and connector.
- **Install at sandbox start.** The rootfs is read-only, and a network fetch at
  boot reintroduces the in-boot download the prewarm exists to prevent.

## Tracking

On acceptance, file an issue each for:
- the bundle declaration and its `curie build` (decisions 1 and 2);
- per-agent template rendering and prewarm (decisions 3 and 4);
- the deploy refusal and the upgrade report (decision 5);
- moving the bundle-specific servers out of `runner/Dockerfile`.
