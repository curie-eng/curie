# 187. The factory polls GitHub for its work, and the platform reads the issue

Date: 2026-10-01

Status: Accepted

**Partially amended by [ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)**
(back-link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the clause that reading the ticket is the bundle's job for the GitHub
factory only, and consequence 6: the platform reads the ticket for every
tracker. Everything else in this ADR stands.

**Partially amended by [ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)**
(back-link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
"The platform reads the issue for the bundle", item 3. The platform reads and
classifies the ticket once at admission and stores only a verdict and a
digest, never ticket content. The sandbox read stays verbatim and unparsed.

Accepted with explicit maintainer approval on 2026-10-01 (Brian Conn),
alongside implementation under ADR 0102.

Tracked in [#3743](https://github.com/curie-eng/curie/issues/3743).

This ADR amends two sections of
[ADR-0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
and one clause of
[ADR-0161](0161-signed-github-issue-events-admit-one-work-item.md):

1. ADR-0145, "There is no polling loop and no task table": the polling half
   only. There is still no task table.
2. ADR-0145, "Reading the ticket is the bundle's job": for the GitHub factory
   only. A tracker that is not GitHub keeps that shape.
3. ADR-0161: a signed `issues.labeled` delivery stops being the only thing
   that admits a WorkItem.

Everything else in both stands. The WorkItem key, the derived request ids,
the review verifier, the write permission check on the sender, bot and App
sender filtering, sticky cancellation, and the rule that Curie stores no
tracker content are all unchanged.

## Context

Factory intake needs a public HTTPS endpoint for GitHub to deliver to. On a
laptop or behind a firewall that means a tunnel, a webhook URL typed into the
App, and a shared webhook secret. The endpoint is also an inbound hole in an
install whose other traffic is outbound. For the people the factory most needs
to reach, a team trying it on their own machine, this is the hardest step in
the setup.

The second hardest step is a fine-grained personal access token. The bundle
reads its ticket through a GitHub MCP server that holds its own credential.
An App installation token cannot replace it, because the token expires after
one hour and a run can last three, and the MCP server does not reload its
environment. Today the operator creates and pastes a PAT.

Two facts make both steps removable.

1. The platform already polls. `reconcile_missed_labels` in
   `apps/api/src/curie_api/factory_label_reconcile.py` lists labelled open
   issues with the App's installation token, reads the issue events to find
   who applied the label, and runs the same `verify_current` and
   `admit_notice` path the webhook uses. It was added as a backstop for missed
   deliveries.
2. The platform already serves scoped reads to a sandbox without giving it a
   credential. [ADR-0174](0174-publication-prechecks-use-an-execution-scoped-capability.md)
   introduced an execution scoped capability that a runner tool presents to an
   API route, and the API answers with its own fresh credential.

## Decision

**Polling is the default factory intake. The platform reads the ticket for
the GitHub factory bundle, so the sandbox holds no GitHub credential.**

### Polling is the default intake

1. The API polls GitHub with the App's installation token for every repository
   the App is installed on and the allowlist admits. It reads:
   1. open issues carrying the configured label, to admit;
   2. closed issues and removed labels on active WorkItems, to cancel;
   3. issue comments since a stored cursor, to admit mention revisions;
   4. review comments and reviews on factory pull requests since a stored
      cursor, for review feedback.
2. Each observation becomes a notice with a stable delivery id derived from
   the GitHub object it describes, and enters the existing `verify_current`
   and `admit_notice` path. The existing delivery table deduplicates it, so a
   repeated poll and a webhook delivery of the same event admit once.
3. Cursors live in Postgres. One API replica polls at a time, under a database
   lock, so a scaled API does not multiply the request rate.
4. Requests are conditional. An unchanged resource answers 304, which GitHub
   does not count against the installation's rate limit. The default interval
   is between 30 and 60 seconds per installation.

### An authenticated read is the proof of origin

On the webhook path the HMAC signature proves a delivery came from GitHub. On
the polling path the platform made the request itself, over TLS, with the
App's own credential, so the response is GitHub's by construction. The
verifier still re-reads the issue or comment and checks the installation, the
allowlist and the sender's write permission before admitting, exactly as
ADR-0161 requires.

### The webhook is optional

1. In polling mode the API does not require a webhook secret to boot, and the
   App needs no webhook URL.
2. An operator may still enable the webhook. Then the signed path is unchanged
   and admits immediately, and polling returns to its backstop role with its
   grace period.

### The platform reads the issue for the bundle

1. The runner exposes a `get_issue` tool to the GitHub factory bundle. The tool
   presents an execution scoped capability, in the ADR-0174 pattern, to an API
   route.
2. The API checks that the capability names this execution and that the
   requested issue is the WorkItem's issue, reads it with the App's
   installation token, and returns the title, body and comments verbatim.
3. The platform does not parse the body, does not model acceptance criteria,
   and does not store the content. The capability is scoped to one issue, so
   the sandbox cannot use it to read anything else.
4. The bundle drops its GitHub MCP server and the PAT it declared.

## Consequences

1. A run starts 15 to 35 seconds after the label is applied instead of one to
   two seconds. That is the price of removing the inbound endpoint.
2. The quickstart needs no tunnel, no webhook URL, no webhook secret and no
   PAT. The operator creates and installs the App and gives the CLI its id and
   private key.
3. The status card image in the factory comment is fetched by GitHub's image
   proxy from a public URL. Without one the comment keeps its checklist and
   result but shows no live image.
4. The API makes a steady stream of conditional reads per installation. At the
   default interval this is a small fraction of the installation's hourly
   budget, and 304 responses cost nothing.
5. The sandbox loses its last GitHub credential, which narrows what a
   compromised run can reach.
6. A second tracker still follows ADR-0145: a different MCP server in the
   bundle with its own credential. Only the GitHub factory uses the platform
   read.

## Alternatives rejected

1. **Keep the webhook as the default and ship a tunnel.** It keeps the inbound
   endpoint and the secret, which is the step this decision exists to remove.
2. **A shared CurieTech App.** Minting its tokens needs our private key. A self
   hosted install cannot hold it, so this needs a hosted token service that
   every install depends on, plus a security review. Deferred.
3. **User tokens from the device flow.** Every pull request would be authored
   by the user, who then cannot approve their own pull request, and the bot
   filtering that stops the factory from looping would no longer apply.
4. **Inject a minted installation token into the sandbox.** It expires before a
   long run ends, the MCP server does not reload it, and it puts a GitHub
   credential back in the sandbox, which ADR-0125 forbids.
5. **Put the issue body in the prompt.** It goes stale when the issue is
   edited, drops comments, and copies tracker content into the platform.

## Realizing code path

1. Polling: `reconcile_missed_labels` in
   `apps/api/src/curie_api/factory_label_reconcile.py`, run from
   `apps/api/src/curie_api/workitem_reconciler.py`, grows into the poller. The
   webhook secret requirement in `apps/api/src/curie_api/config.py` becomes
   conditional on webhook mode.
2. Issue read: a new route beside
   `apps/api/src/curie_api/routers/publication_precheck.py`, a capability in
   the shape of `apps/api/src/curie_api/publication_precheck_token.py`, and a
   runner `get_issue` tool. The bundle change is in `examples/dark-factory/`.
