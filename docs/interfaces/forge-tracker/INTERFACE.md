---
seam: Forge tracker and code host
kind: CLEAN
impls: GitHub and GitLab pairs, Jira tracker, Bitbucket Cloud and Data Center code hosts, and in-memory pairs
grade: not separately graded
vision_row: null
epics: ["#3831"]
order: 26
---

# INTERFACE: Forge tracker and code host

> Part of the Curie swappable seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** GitHub and GitLab pairs, Jira tracker, Bitbucket Cloud and Data Center code hosts, and in-memory pairs &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol` or typed port class. SOFT = swap through config or wire without a code interface. NONE = not built yet.

## The black line

The dark factory reaches the system where work is tracked through a Tracker
port and the system where code lives through a CodeHost port
([ADR 0197](../../adr/0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)).
The two are separate because they are not always one system: Bitbucket teams
track work in Jira. Each forge is an adapter. The factory core owns the
decisions (what admits a run, when CI is good enough, which feedback becomes a
revision, what a status comment says) and asks the ports only for facts and
the writes those decisions need.

Production adapters implement both ports, with selection owned by
`apps/api/src/curie_api/forges/registry.py`. Some legacy API callers still reach
GitHub directly through `apps/api/src/curie_api/forges/github/`; the import
allowlist below records those remaining callers.

## Current contract

### Tracker

`Tracker` (`apps/api/src/curie_api/forges/ports.py::Tracker`) is where work is
marked and described:

1. `poll_marked` returns every marking since a cursor as `MarkedNotice` values
   (admit, cancel or mention) plus the next cursor, and re-reads the WorkItems
   already running so a marker removed before the cursor still cancels. A
   listing that cannot be read to its end raises `Unavailable` and yields no
   cursor. Events by the adapter's own identity are never notices. Each notice
   carries `occurred_at`, the event time the tracker reported, or None when it
   reported none; admission treats None as unknown.
2. `verify_current` says whether a notice still describes the issue.
3. `marking_actor` names who applied the marker that is on the issue now.
4. `may_start` decides whether that actor may start a run. A native tracker
   checks write access to its own repository. A tracker-only kind checks the
   binding's allowlist or group.
5. `read_ticket` returns the ticket as markdown for the sandbox. Nothing is
   stored.
6. `ticket_labels` returns the issue's current labels and component names,
   which admission reads for repository routing and label conflicts. Sandbox
   acquisition and base reconciliation read them again for the current `base:`
   choice. The adapter stores nothing.
7. `set_state_label` applies the factory state label and drops others.
8. `closing_reference` produces the text a pull request body carries.
9. `publication_references` returns every scope-qualified text by which an
   earlier factory publication body in a repository names exactly this issue:
   GitHub's closing reference, GitLab's `Closes #<iid>` only in the issue's own
   project, and Jira's reference for the current key and every key the issue
   held before a move. An unreadable answer raises `Unavailable`.
10. `issue_url` gives the issue's web URL, which work item links come from.
    It makes no request and raises `NotFound` for an issue outside the tracker.
11. Optional: `link_pull_request`, `dependencies` and `in_group` (group start
    authority).

### CodeHost

`CodeHost` (`apps/api/src/curie_api/forges/ports.py::CodeHost`) is where code,
pull requests, CI and review live: `resolve_repository` from an immutable id;
`credential` for clone or push scope; `branch_head` and `read_commit`;
`find_pull_request`, `open_pull_request`, `update_pull_request`,
`retarget_pull_request` and `read_pull_request`; `list_factory_publications`;
`observe_ci` on an exact head; `list_review_feedback` since a cursor and
`verify_feedback`. Optional:
`ci_diagnostics`, `rerun_failed`, `user_can_write` and `required_checks`
(GitHub only among the shipped code hosts; GitLab and both Bitbucket adapters
raise `Unsupported`).

`retarget_pull_request(ref, *, base_ref, expected_head_sha)` is the mandatory
operation for changing only an open pull request's base. `update_pull_request`
continues to change only its title and body. Retargeting verifies the identity,
unchanged source branch and expected head before and after the update, and
confirms the new target; an already matching target makes no write. Provider
read/write/read checks are optimistic. The API's durable base operation uses
Git `--force-with-lease` with the exact branch and expected head to fence branch
updates.

`list_factory_publications` returns, as `FactoryPublication` values, every pull
request in any state that the adapter's own identity opened in the repository
from a branch of that repository (never a fork) whose last path component is
`publication-<32 hex>`. The whole listing is read: a failed page, an item it
cannot read, or an own identity it cannot read raises `Unavailable`. Bitbucket
Cloud cannot read its own account, so it lists every such pull request of the
repository with `owner_verified` False, as does Bitbucket Data Center when its
answers omit the `X-AUSERID` header. Such publications are not evidence:
rediscovery drops them before matching. Every other adapter lists only its
own, with `owner_verified` True.

`observe_ci` returns a `CiRollup`
(`apps/api/src/curie_api/forges/types.py::CiRollup`) built by
`CiRollup.on_head`, which drops any check reported for another commit. A
green run on an older head therefore never passes a newer one, and a failure
outranks a pending check.

### Marked comments

Both ports expose `MarkedComments`
(`apps/api/src/curie_api/forges/ports.py::MarkedComments`) for status
comments. The core owns the marker text. The adapter decides how to embed it,
and `find_marked` returns only a comment its own identity wrote, so a human
who pastes the marker can neither be found nor overwritten. `upsert_marked`
edits that comment or posts one. `comments_for`
(`apps/api/src/curie_api/forges/ports.py::comments_for`) picks the side for a
`ReplyTarget`: the tracker for an issue, the code host for a pull request
conversation or a review thread.

### Errors

Adapters raise only the classes in `apps/api/src/curie_api/forges/errors.py`:
`Unavailable` (transient, including a listing that failed part way),
`Unauthorized`, `NotFound`, `Unsupported` for an operation declared
unsupported, and `InvalidPairing` for a binding the rules below refuse.

## Capability tiers and pairing rules

Every adapter declares each operation of its port as supported, no-op or
unsupported in a `capabilities` mapping keyed by `Operation`
(`apps/api/src/curie_api/forges/capabilities.py::Operation`). Mandatory
operations must be supported. A no-op is allowed only on
`link_pull_request`, `dependencies` and `rerun_failed`, where doing nothing is
a correct answer. A permission or a log read cannot be answered by doing
nothing, so `user_can_write` and `ci_diagnostics` are supported or not.

`mandatory_capabilities`
(`apps/api/src/curie_api/forges/capabilities.py::mandatory_capabilities`)
returns the set a pairing needs, and `validate_pairing`
(`apps/api/src/curie_api/forges/capabilities.py::validate_pairing`) refuses a
binding that does not meet it. The rules:

1. GitHub and GitLab are native forges. A native tracker pairs only with its
   own forge, because its write check says nothing about a repository on
   another forge.
2. Jira is a tracker-only kind and pairs with any code host.
3. Bitbucket Cloud and Bitbucket Data Center are code-host-only kinds.
4. On a native pairing the code host must support `user_can_write`, since
   the native tracker admits on that same forge's write check.

When `user_can_write` is unsupported, `may_act_on_feedback`
(`apps/api/src/curie_api/forges/authority.py::may_act_on_feedback`) falls back
to the binding's allowlist of code-host accounts. When the code host reports
write access, it decides alone and the allowlist is not consulted.

## Identity

A tracker issue is a `TrackerIssueRef`
(`apps/api/src/curie_api/forges/types.py::TrackerIssueRef`) keyed by tracker
kind, host, scope id and issue id. The scope is the repository id on GitHub,
the project id on GitLab and the site on Jira. A Jira key is carried as
`display_key` and takes no part in equality, because it changes when the
issue moves. A repository is a `RepositoryRef`
(`apps/api/src/curie_api/forges/types.py::RepositoryRef`) keyed by kind, host
and immutable project id; its path is for display.

Request ids, feedback event ids and issue lock keys come from
`apps/api/src/curie_api/forges/identity.py`. For GitHub,
`notice_request_id`, `reconcile_delivery_id`, `feedback_event_id`,
`revision_request_id` and `issue_lock_keys` reproduce the existing
derivations byte for byte, so rows and advisory locks from before the port
still match. Every other kind derives from its full key under a separate
namespace and needs no edit to that module.

## Credentials

`credential` returns a `Credential`
(`apps/api/src/curie_api/forges/types.py::Credential`) holding the origin, the
header form git accepts on that forge (Basic, Bearer or `PRIVATE-TOKEN`), the
scope (clone or push) and the expiry: known with a time, unknown for a static
token whose lifetime the forge does not report, or none. `git_header` renders
the header git sends. The secret is excluded from the value's `repr`.
Credentials stay with the platform and never reach the sandbox.

## The contract suite

One suite under `apps/api/tests/forges/contract/` runs every adapter pair. It
calls only port methods and an `AdapterHarness`
(`apps/api/tests/forge_fakes/contract_harness.py::AdapterHarness`), which
seeds issues, labels, write access, heads, checks, feedback and foreign
comments, injects a page failure and counts writes. One module per behavior:

1. `test_pagination_failure.py`: a failing page raises `Unavailable` and the
   retry from the old cursor sees every marking.
2. `test_duplicate_delivery.py`: one marking polled twice is one request id;
   a relabel is a new one.
3. `test_unauthorized_admission.py`: an actor without authority may not start.
4. `test_cancel_on_label_removal.py`: removing the marker cancels, including a
   removal that predates the cursor.
5. `test_red_ci_continuation.py`: a failing head is failure; a new green head
   is success.
6. `test_stale_head_ci_rejected.py`: checks for an older head are not returned
   for the current one.
7. `test_review_revision.py`: a writer's feedback is listed and actionable; a
   non-writer's is not.
8. `test_marked_comment_ownership.py`: a foreign comment carrying the marker
   is ignored, and an upsert edits only our own.
9. `test_capabilities.py`: a no-op writes nothing, an unsupported operation
   raises `Unsupported`, and the allowlist fallback answers.
10. `test_issue_url.py`: an issue link names the issue on the tracker host,
    writes nothing, and an issue in another scope has no link.

The GitHub identity golden test is
`apps/api/tests/forges/test_identity_golden.py`.

### Registering an adapter

A new adapter pair adds its adapter modules in a package under
`apps/api/src/curie_api/forges/`, a harness that satisfies `AdapterHarness`,
and one entry in `HARNESSES` in
`apps/api/tests/forges/contract/conftest.py`. The vectors are never edited
for an adapter. The Jira Cloud tracker registered this way as the
`jira-github` harness
(`apps/api/tests/forge_fakes/contract_harness.py::JiraGitHubHarness`), paired
with the GitHub code host. Its kind must already be named in the pairing
rules; GitLab, Jira and both Bitbucket kinds are.

## Implementations today

1. **In memory:** `InMemoryTracker`, `InMemoryCodeHost` and
   `InMemoryMarkedComments` in `apps/api/src/curie_api/forges/memory.py`, real
   implementations over dict state. The suite runs them as two pairs: a
   native pair with every operation supported, and a tracker-only pair that
   declares the optional operations no-op or unsupported, the shape a Jira and
   Bitbucket binding has.
2. **GitHub:** `GitHubTracker`
   (`apps/api/src/curie_api/forges/github/tracker.py::GitHubTracker`) and
   `GitHubCodeHost`
   (`apps/api/src/curie_api/forges/github/code_host.py::GitHubCodeHost`) form a
   native pair. Legacy direct callers of the package remain on the import
   allowlist; their existence does not mean the pair is unimplemented.
3. **Jira Cloud:** `JiraTracker`
   (`apps/api/src/curie_api/forges/jira_cloud/tracker.py::JiraTracker`) in
   `apps/api/src/curie_api/forges/jira_cloud/`, a tracker-only kind paired
   with a separately configured code host. It reaches Jira Cloud REST v3 with a static API
   token held only by the API, keeps its status comment marker in the
   `curie.factory` comment property, and links each pull request back to the
   issue with a remote link.
4. **Bitbucket Cloud:** `BitbucketCloudCodeHost` in
   `apps/api/src/curie_api/forges/bitbucket_cloud/code_host.py`, a code host
   only, paired with a Jira Cloud tracker. Every mandatory CodeHost and
   MarkedComments operation is supported; `ci_diagnostics`, `rerun_failed` and
   `user_can_write` are unsupported. Review feedback counts only from accounts
   in the binding's review feedback allowlist.
5. **Bitbucket Data Center:** `BitbucketDcCodeHost` in
   `apps/api/src/curie_api/forges/bitbucket_dc/code_host.py`, with the same
   capabilities and allowlist rule, for Data Center 9.4.0 or newer.
6. **GitLab:** `GitLabTracker`
   (`apps/api/src/curie_api/forges/gitlab/tracker.py::GitLabTracker`) and
   `GitLabCodeHost`
   (`apps/api/src/curie_api/forges/gitlab/code_host.py::GitLabCodeHost`) in
   `apps/api/src/curie_api/forges/gitlab/`, a native pair for gitlab.com and
   self-managed servers 17.0 or newer. It uses a static token with the `api`
   and `write_repository` scopes held only by the API, polls issues, keeps
   marked comments in issue and merge request notes, and supports
   `ci_diagnostics` and `rerun_failed`.

## Known leakage

1. **Callers still reach GitHub directly.** The import gate
   `apps/api/tests/forges/test_import_gate.py` lists every API module outside
   `forges` that imports the GitHub adapter package or carries a GitHub REST
   literal. `apps/worker/tests/test_forge_import_gate.py` does the same for the
   worker, which may not import `curie_api.forges` at all. Both lists may only
   shrink; a listed file that stops offending fails the gate until its entry
   is removed.
2. **A GitHub request identity ignores the host.** To keep existing rows and locks,
   the GitHub derivations use the repository id and issue number only, so two
   GitHub hosts with the same repository id would share request identities.
   WorkItem storage itself has the neutral `tracker_kind`, `tracker_host`,
   `tracker_scope_id` and `tracker_issue_id` key
   (`apps/api/src/curie_api/models.py::WorkItem`); the identity migration is
   already represented in that model.

## Cross-links

1. **Related seam:** [publication-lineage-authority](../publication-lineage-authority/INTERFACE.md). The worker's publication path reaches CodeHost through internal API endpoints, with a separate GitHub authority branch.
2. **Related seam:** [channel-ingress](../channel-ingress/INTERFACE.md). Acknowledge-only GitHub, GitLab and Jira reply adapters sit there; factory status comments go through `MarkedComments`.
