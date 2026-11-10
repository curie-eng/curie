# 197. The factory reaches code hosts and trackers through two ports

Date: 2026-10-04

Status: Accepted

Accepted 2026-10-05 with explicit maintainer approval from Brian Conn
(TheConnMan), given after review of the publishing pull request
([#3980](https://github.com/curie-eng/curie/pull/3980)), including its
Jira repository binding, review-feedback allowlist fallback, native-tracker
pairing and push-only publication Job decisions.

Amends [ADR-0162](0162-work-items-own-durable-execution-identity.md): the WorkItem
identity stops being a GitHub repository id and issue number and becomes a
tracker-qualified issue identity. Amends
[ADR-0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md),
"Reading the ticket is the bundle's job ... for the GitHub factory only" and its
consequence 6: the platform reads the ticket for every tracker. Amends
[ADR-0161](0161-signed-github-issue-events-admit-one-work-item.md) only where it
names GitHub as the sole source of a notice. Everything else in those ADRs
stands: the delivery table, `verify_current` and `admit_notice`, sticky
cancellation, bot filtering, and storing no tracker content.

Tracked in [#3831](https://github.com/curie-eng/curie/issues/3831).

## Context

The dark factory works only with GitHub. About 25 API modules, four worker
modules, the sandbox template and the runner snapshot check call GitHub REST
directly or assume `github.com`. Teams whose code lives on GitLab or Bitbucket
cannot use it.

Four facts shape the design.

1. The code host and the tracker are not always the same system. Bitbucket
   Cloud removed its issue tracker on 2026-08-20 (its API answers 410), and
   Bitbucket Data Center never had one. Bitbucket teams track work in Jira.
2. Each forge authenticates differently. GitHub mints one-hour App installation
   tokens. GitLab and Bitbucket give a bot a long-lived token that an operator
   rotates. Jira Cloud offers both a long-lived scoped token and OAuth client
   credentials that mint one-hour tokens. Git accepts a different credential
   form on each forge, and GitLab refuses a Bearer header.
3. Identity is the hardest coupling. Unique keys, request ids derived with
   uuid5, advisory-lock keys, the work-items wire contract and the repository
   path validator all assume a GitHub numeric repository id and an `owner/name`
   path. GitLab paths have any number of segments, git serves a renamed GitLab
   project at its old path, and moving a Jira issue changes its key.
4. A factory status reply lands in one of three places: the tracker issue, the
   pull request conversation, or a review thread. With Jira and Bitbucket those
   are two different systems.

## Decision

**The factory calls a code host through a CodeHost port and a tracker through a
Tracker port. Each forge is an adapter. A WorkItem is keyed by its tracker
issue, qualified by tracker kind and host, and its repository is chosen at
admission.**

### Two ports

1. **Tracker**: poll marked tickets since a cursor, re-reading the WorkItems
   already running; verify a notice is current; find who marked the ticket;
   decide whether that actor may start a run; read the ticket for the sandbox as
   markdown; set the state label; produce the closing reference for a pull
   request body; and, optionally, link a pull request back to the ticket and
   report dependencies (filled by the ticket dependency work under ADR-0165).
2. **CodeHost**: resolve a repository from its immutable id; hand back a
   clone-scoped or push-scoped credential with its origin and git header; read a
   branch head and a commit; find, open, update and read a pull or merge
   request; observe CI as normalized checks on the exact head commit; list and
   verify review feedback since a cursor; and, optionally, fetch CI diagnostics,
   rerun failed jobs, and report whether a user may write to the repository.
3. Both ports also expose marked status comments. The core owns the marker
   text. Each adapter decides how to embed it and finds only comments its own
   identity wrote.
4. GitHub and GitLab adapters implement both ports. Jira Cloud implements
   Tracker. Bitbucket Cloud and Bitbucket Data Center are two CodeHost adapters.
   A GitHub or GitLab tracker pairs only with its own forge, because its write
   check says nothing about a repository on another forge. Jira pairs with any
   code host.
5. Each adapter declares every operation as supported, no-op or unsupported.
   One contract test suite runs every adapter, including an in-memory fake, and
   asserts the behavior for supported operations, the absence of writes for
   no-op ones, and the caller's fallback for unsupported ones.
6. The API owns both ports. The worker holds no adapter code. The pull request,
   branch and commit reads it makes today move behind internal API endpoints
   backed by CodeHost, and it receives a credential and an origin as data. The
   publication Job only pushes, with a push-scoped credential. The API checks
   the stored pull request before the Job launches and finds or opens it after
   the push. The force-with-lease push still guards the branch, and a pull
   request merged or closed during the push is recorded from the post-push read.
   The runner reads the allowed origin host from its boot environment and
   accepts a repository path of any depth.

### Identity

1. A WorkItem's unique key is (tracker kind, tracker host, scope id, issue id).
   The scope is the immutable boundary within which the issue id is unique and
   survives the issue being moved: the repository id on GitHub, the project id
   on GitLab, the site (cloudId) on Jira. The issue id is the tracker's
   immutable id as text: the GitHub issue number, the GitLab iid, the Jira
   numeric issue id. A Jira key such as `PROJ-123` changes when the issue moves,
   so it is stored for display only.
2. A repository is bound by (code host kind, code host host, immutable project
   id). Its path is for display. Before cloning, the adapter resolves the path
   from the id and refuses a mismatch.
3. A WorkItem's repository is chosen at admission and frozen on the WorkItem.
   A native tracker's repository is its own project. A Jira binding names a
   default repository alias and may map Jira components to aliases. A
   `repo:<alias>` label may select another alias listed on the same binding. An
   unknown alias, or more than one, is refused with a comment, as an unknown
   `base:` label is.
4. The GitHub adapter keeps its existing request-id, reconcile, feedback
   event-id and issue-lock derivations byte for byte, so rows and locks from
   before the migration still match. A contract test compares old and new
   derivations.
5. Columns, keys, the wire contract, the CLI and the UI are renamed once, with
   no aliases.

### Authority

1. A native tracker keeps today's rule. A ticket marked on GitHub or GitLab
   admits only if the actor has write access to that tracker's repository
   (GitLab: access level 30 or higher, because a Reporter can apply labels).
2. A Jira ticket admits only if the actor is on an allowlist of Jira accounts,
   or in a Jira group, configured for the binding. Curie does not match
   identities across systems.
3. Review feedback on a pull request is acted on only if the code host confirms
   the author may write to the repository. A code host that cannot report
   another user's permission with the configured token (Bitbucket without an
   admin token) falls back to an allowlist of code-host accounts configured for
   the binding.

### Intake and credentials

1. Every adapter polls. No install needs an inbound endpoint (unchanged from
   ADR-0187). Adapters honor each forge's rate-limit headers and do not count
   on 304 responses being free; outside GitHub they are not.
2. Credentials are held by the platform, never by the sandbox. A credential
   provider either re-mints short-lived tokens (the GitHub App, Jira OAuth
   client credentials) or serves a static token. A static token's expiry may be
   unknown. Curie reports each credential's expiry, or that it is unknown, in
   status and doctor output and warns before a known expiry; the operator
   rotates static tokens.

## Consequences

1. GitLab (gitlab.com and self-managed), Bitbucket Cloud, Bitbucket Data Center
   and Jira Cloud become possible factory targets, and a further forge is one
   adapter.
2. A single migration widens the identity keys of about six tables and changes
   the work-items wire contract. The CLI and UI move with it in the same change.
3. Jira-started runs are no longer gated by repository write access; the
   binding's allowlist or group decides who may start them.
4. Outside GitHub, polling spends rate-limit budget even when nothing changed.
   Bitbucket Cloud allows 1,000 requests an hour per token, so a large install
   will eventually want webhooks, which are a later decision.
5. Flaky-job rerun and CI log excerpts are optional CodeHost capabilities.
   GitHub declares both and GitLab can. Bitbucket declares neither, so a
   Bitbucket CI failure reaches the agent as check names, states and links only.
6. The publication Job's pre-push check moves to before the Job launches, so a
   pull request merged or closed while the Job is scheduled can receive one
   stray commit on its branch. The post-push read records the pull request as
   merged or closed, as it does today for a race inside the Job.
7. Curie's own CI check names, hardcoded in the CI gate today, become per-repository
   configuration keyed on the normalized check key.

## Alternatives rejected

1. **One forge port with issues on it.** It cannot express Bitbucket with Jira,
   which is the only way Bitbucket teams track work now.
2. **Adopt a library (PR-Agent providers, ogr).** ogr has no Bitbucket, and
   PR-Agent's providers are built around reviewing an existing pull request and
   read global settings. They are useful references for edge cases.
3. **Match a Jira user to a forge user by email.** Jira Cloud hides emails by
   default, other forges expose them unreliably, and users can edit them.
4. **Map each Jira user to a forge login and keep the repository write check.**
   Someone has to maintain the map, and Bitbucket Data Center reports only
   explicit grants without an admin token.
5. **Pair any tracker with any code host.** A GitHub or GitLab write check on the
   tracker proves nothing about a repository on another forge, so it would need
   the cross-system mapping rejected above.
6. **Key a Jira WorkItem by project and issue key.** Moving the issue changes
   both and would split one ticket into two WorkItems.
7. **Let the worker import adapters.** It would spread forge credentials and
   code into a second process; internal API endpoints keep one owner.
8. **Record this as an amendment to ADR-0165.** ADR-0165 is a Draft about ticket
   dependencies, and its acceptance should not gate this decision.
