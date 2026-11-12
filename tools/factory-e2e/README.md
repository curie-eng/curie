# Factory end-to-end drivers

This directory holds the factory end-to-end drivers. `factory_e2e.py` is
documented in its module docstring and in `curie dev factory-e2e --help`. This
README covers the single-profile personal subscription acceptance driver,
`personal_profile.py`, which proves issue #5375 against ADR 0268 section 6
rows 3, 4, 6 and 12.

## What the driver proves

One dedicated personal grant, one owner principal and one owned fixture issue
go through trusted factory admission on a throwaway Kubernetes cluster. The
implementer and both reviewers run on the native Claude Code CLI with that
grant, publication parks for approval, an unlisted operator is refused, an
allowed operator rejects one attempt and approves a fresh one, the reviewed
change is published as one ready pull request, and every owned resource is
removed and proven absent.

## Inputs

Every command takes the same flags. `judge` needs only `--candidate`,
`--state-dir` and `--run-id`; `forge-probe` also needs `--app-dir`;
`rehearse` takes no grant, expiry or approval flag.

1. `--candidate` is the full 40 hex commit under test. Images and the chart
   come from `git archive` of that commit, never the working tree.
2. `--context`, `--namespace` and `--release` name a throwaway cluster, a
   namespace that must not exist yet, and a release other than `curie`.
3. `--registry` is a loopback `HOST:PORT` (`127.0.0.1` or `localhost`). The
   driver reads it over plain HTTP on 127.0.0.1, and the cluster nodes pull
   from `localhost:PORT`, so the registry must be reachable at that address
   from both. It must allow manifest deletes
   (`REGISTRY_STORAGE_DELETE_ENABLED=true`), or cleanup reports the run's
   tags as residual.
4. `--docker-context` is a Docker context name, or an `ssh://` or `tcp://`
   host, for the image builds. `--kubeconfig` is the kubeconfig file.
5. `--app-dir` holds a GitHub App kit: `app.json` with `id`,
   `installation_id` and `repo` (the owned fixture repository), and
   `app.pem`. The driver never uses a personal GitHub login.
6. `--grant-file` is the dedicated grant: a regular file you own, mode 0600,
   one ASCII line. `--grant-expires-at` is its expiry as ISO 8601 with an
   offset (`2026-11-11T09:30:00Z`) or epoch seconds. The driver never infers
   expiry from the token.
7. `--state-dir` is an existing directory you own with mode 0700. It holds the
   private ledger `ledger.sqlite3` (mode 0600), a scrubbed command log and the
   run's work directory, which cleanup removes.
8. `--approve-live` must equal the approved ceiling exactly (see below).

The curie binary comes from `$CURIE_BIN`, else `cli/target/debug/curie`.

## Running it

The commands below use placeholder values. Run them from the repository root, with
`$SHA` set to the candidate commit and `$KUBE_CONTEXT` to the throwaway cluster's
context.

```sh
P="uv run python tools/factory-e2e/personal_profile.py"
COMMON="--candidate $SHA --context $KUBE_CONTEXT --namespace ppa-acme-test \
  --release ppa-acme --registry 127.0.0.1:5999 --docker-context acme-builder \
  --kubeconfig $KUBECONFIG_FILE --app-dir $APP_DIR --grant-file $GRANT_FILE \
  --grant-expires-at 2026-11-11T09:30:00Z --state-dir $STATE_DIR --run-id acme-run-1"
CEILING="sessions=8,turns=96,session-turns=32,output-tokens=32768,session-output-tokens=8192,session-seconds=300,live-seconds=2400,cleanup-seconds=300,paid-retries=0"

$P images $COMMON
$P preflight $COMMON --approve-live "$CEILING"
$P forge-probe $COMMON
$P grant-probe --grant-file $GRANT_FILE --grant-expires-at 2026-11-11T09:30:00Z
$P run $COMMON --approve-live "$CEILING" \
  --slack-bot-token-file $SLACK_BOT_TOKEN_FILE --resolution-channel $RESOLUTION_CHANNEL
$P judge --criterion all --candidate $SHA --state-dir $STATE_DIR --run-id acme-run-1
```

1. `images` is unpaid. It builds `curie-api`, `curie-worker`, `curie-runner`
   and `curie-dispatcher` (the channel gateway's image) with the label
   `org.opencontainers.image.revision=<candidate>`, pushes them as
   `<name>:<candidate12>-<run digest>` so each tag belongs to one run ID, then
   runs `curie build` on a private copy of `examples/dark-factory` whose
   manifest routes `mcp__curie__publish_changes` to the `publication` approval
   route and whose hooks add the reservation gate. The layer inherits the
   revision label from the runner it is built on. The layer repository
   (`<bundle>-runner`) must hold no local or registry tag before the build,
   which refuses with `layer_repository_not_owned`; it is recorded first, and
   teardown removes every tag found in it.
2. `preflight` is unpaid. It refuses before any model session and before any
   cluster mutation.
3. `forge-probe` mints an installation token limited to contents, pull
   requests and issues (write) and checks and statuses (read), creates a
   run-owned branch with an empty commit, opens and closes a pull request,
   deletes the branch, and proves both are gone. It touches no other ref.
4. `grant-probe` sends one turn to `claude-haiku-5-5` with the grant as the
   only credential and prints its outcome, cost and error class. It writes no
   ledger. The probe CLI is `claude` from `PATH`, or `--grant-probe-cli`.
   Its child gets a fresh empty temporary `HOME`, `CLAUDE_CONFIG_DIR` and
   working directory and inherits nothing else, so a login on the host cannot
   answer for the grant.
5. `run` performs exactly one live attempt. A factory publication's card goes
   to the `publication` route's fixed Slack channel (ADR 0177), and the outbox
   fails the publication after five failed deliveries. The install therefore
   deploys the channel gateway, with the bot token from
   `--slack-bot-token-file` in a run-owned Secret, and binds the route's
   resolution to `--resolution-channel`. The dispatcher stays off; approvals
   are resolved from the CLI. Each answer waits for its card to be delivered.
   `--output-token-ceiling N` (on `preflight` and `run`) holds one run below
   the approved total output tokens; it can only lower the ceiling, is recorded
   before the first reservation, and bounds reservations, the overrun check and
   `judge --criterion budget`. `--item-output-tokens-spent N` (on `run`) gives
   what the item's earlier live runs actually spent; the watchdog then stops
   paid work when that plus this run's observed output reaches the approved
   32,768, and the budget judge checks the same total. A second `run` with the same run ID refuses with
   `paid_retry_refused`; there is no retry flag. Immediately before the first
   reservation, `run` makes the same probe. A failed probe fails the run
   before any attempt is reserved. Its cost is recorded under `grant_probe` in
   the ledger, apart from the attempt ceiling.
6. `judge --criterion {roles,approval,pod-restart,publication,budget,cleanup,all}`
   reads the ledger only and prints
   `{"criterion", "verdict", "checks", "counts"}` with booleans and counts and
   no identifiers. A missing observation fails its check.

Exit codes: 0 pass, 1 failed run or failed judge, 2 usage error or refusal.
A refusal prints `{"error": <code>, "fix": <text>}` naming variables, never
values.

## Rehearsing the unpaid phases

`rehearse` runs the same `run` phases up to and including `drill`
(refusals, namespace, images, install, setup, forge and drill) against the
real cluster, then the normal finalizer and inventory. The drill creates two
publications shaped like a factory one (reply kind `github`, the
`publication` route and its card channel) through the internal API, waits for
both cards, and requires the unlisted operator to be refused by the explicit
list, the allowed operator to reject the first and approve the second. The
drill's base commit does not exist, so the approved one pushes nothing. A
`run` skips the drill. It never loads a grant,
writes a live start or reserves a session, so the single paid `run` is not
the first execution of the install, setup, forge and cleanup machinery.

```sh
$P rehearse --candidate $SHA --context "$KUBE_CONTEXT" --namespace ppa-acme-rehearsal \
  --release ppa-acme-r --registry 127.0.0.1:5999 --docker-context acme-builder \
  --kubeconfig $KUBECONFIG_FILE --app-dir $APP_DIR --state-dir $STATE_DIR \
  --slack-bot-token-file $SLACK_BOT_TOKEN_FILE --resolution-channel $RESOLUTION_CHANNEL \
  --run-id acme-rehearsal-1
$P judge --criterion cleanup --candidate $SHA --state-dir $STATE_DIR --run-id acme-rehearsal-1
```

1. Passing `--grant-file`, `--grant-expires-at` or `--approve-live` refuses
   with `arguments_invalid`.
2. After minting both operator principals, setup reads every path the live
   phases parse: the pending approvals list (empty), the route listing (the
   `publication` route bound to exactly the allowed subject), the agent's work
   items (empty) and the api's pending approvals (empty). Each result is a
   boolean under `rehearsal`; any false fails the setup phase. The paid `run`
   performs the same reads in its setup.
3. The rehearsal marks its run ID. `run` refuses that ID with
   `rehearsal_run_id`, and `judge` refuses every criterion except `cleanup`
   with `rehearsal_ledger`. The `cleanup` judgment is the unpaid cleanup
   receipt for AC 7: every finalizer step ok, every inventory entry absent,
   foreign resources preserved and no held secret bytes on any channel.

## Refusals

`preflight` and `run` refuse in this order:

1. `service_credentials` when any of `ANTHROPIC_API_KEY`,
   `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_FOUNDRY_API_KEY`,
   `ANTHROPIC_CUSTOM_HEADERS`, `CLAUDE_CODE_OAUTH_TOKEN`,
   `CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`,
   `CLAUDE_CODE_USE_FOUNDRY`, `CURIE_CREDENTIALS`, `CURIE_MODEL_BASE_URL`,
   `CURIE_MODEL_ENV_KEY`, `CURIE_FACTORY_MODEL_API_KEY`, `OPENROUTER_API_KEY`,
   `OPENROUTER_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`, or any
   `ANTHROPIC_*` or `CLAUDE_CODE_*` name ending in `KEY`, `TOKEN` or `SECRET`
   is set and nonempty; `fake_mode` for `CURIE_FAKE_MODEL`. This check runs
   before any file is read.
2. `candidate_invalid`, `state_dir_unsafe` and `live_unapproved`.
3. `grant_missing`, `grant_unsafe`, `expiry_unknown`, then
   `lifetime_insufficient` when less than 2700 seconds remain.
4. `arguments_invalid`, `release_reserved`, `registry_invalid`,
   `app_kit_invalid`.
5. `candidate_image_mismatch` unless the registry serves every required image
   with the exact candidate revision label.

`run` adds `paid_retry_refused`, `namespace_exists`, and after install
`deployed_service_credential` when the deployed settings carry a service
credential, helper, gateway setting or fake model. Before loading the grant
and before every paid start it re-checks the grant lifetime.

## Approved numbers and the budget ledger

Eight sessions; 96 assistant turns in total and 32 per session; 32768 output
tokens in total and 8192 per session; 300 seconds per session; a 2400 second
live window plus 300 seconds of cleanup; zero paid retries. The implementer
model is `claude-sonnet-5-5` and both reviewers use `claude-opus-5-5`; a
provider model with a date or bracketed context suffix counts as the same
model.

1. Before every action that can start model sessions, the run persists a
   reservation in one SQLite transaction. Each attempt reserves four sessions:
   the implementer, the plan reviewer, the diff reviewer and one diff
   re-review round, so two attempts fill the eight approved sessions. Each
   reserved session takes an equal share of the remaining turns and output
   tokens, capped at the per-session bound. The run also sets the agent's
   platform output-token budget to the attempt's reserved total.
2. Usage is settled from the native transcripts. A session whose usage cannot
   be read stays unknown, and unknown usage refuses every later reservation
   with `usage_unknown`. An incomplete harvest leaves the whole attempt
   unknown, and so does an admission answer other than success or a 4xx
   refusal. The fresh attempt replays the conversation history into a new
   native session; records older than its admission or repeating an earlier
   attempt's assistant message id are that replay and are not counted again.
3. A session above any per-session bound, totals above their bounds, or an
   observed session with no reservation mark the budget exceeded; later
   reservations refuse and `judge --criterion budget` fails.
4. The live window starts before the first paid start. No session starts once
   fewer than 300 seconds of it remain or once the grant would expire before
   the window ends.
5. Per-session elapsed time is the span between a session's first and last
   transcript record. The implementer's elapsed time excludes the wall
   intervals covered by its reviewer subagent sessions, which count as the
   reviewers' own seconds.
6. Reviewer starts are limited before any model call. The private bundle copy
   adds a `PreToolUse` hook (`reservation_gate.py`) that counts `Agent` and
   `Task` starts and refuses a second plan review, a third diff review, or any
   other subagent type. It classifies a start with the bundle's own
   `review_gate.reviewer_kind`, so a reviewer inferred from its description
   counts the same way. It counts at invocation, so a review that returns no
   verdict still uses its reservation.
7. A watchdog stops paid work while it runs. Every poll during an attempt
   parses the harvested transcripts and compares each session with its
   reservation. When a session reaches its output tokens, turns or seconds,
   the run fences paid starts and fails. Transcripts are written per completed
   response, so the overshoot is at most the response in flight, which
   `CLAUDE_CODE_MAX_OUTPUT_TOKENS` caps at 4096.
8. The fence reads the grant Secret's identity for the same-grant check, then
   stops the worker, deletes the grant Secret and deletes every runner pod
   before it waits for anything, and confirms no runner container is running. It runs once the approved publication succeeds
   and before the live window closes, because the platform's CI repair can
   queue a continuation for a failing pull request. On every failure path it
   runs before the live window is closed and again early in teardown.
9. Harvest capture is affirmative. The in-pod harvester reports every
   transcript's size and the pod time after each pass. An attempt settles only
   when the last report matches every reassembled file and was taken at or
   after the publication approval's creation, which follows the end of the
   native turn. A failed exec, an empty stream, a stream that ended before
   that moment, bytes after the last report, a malformed transcript line, a
   session with no assistant usage, or an attempt with no implementer session
   leave the attempt's usage unknown. The comparison assumes the API and the
   runner share a node clock, as on a single-node throwaway cluster.

## What each criterion observes

1. `roles`: the first attempt's native transcripts show an implementer session
   and plan reviewer and diff reviewer subagent sessions, each with
   non-synthetic assistant turns, on the configured models; no synthetic
   assistant record in either attempt; at least one `mcp__curie__` tool call;
   the bundle's review gate state files (the hook ran); at most one native
   CLI process at a time; the running images are the pushed candidate digests,
   including each attempt's runner pod, whose running image digest and revision
   label must be the candidate layer's;
   no service credential in the deployed settings; and the owner tuple
   (tenant, mode `personal`, principal, generation 1) from both
   `GET /work-items/{id}` and `curie cluster work-items <id> --json`, and on
   every execution request.
2. `approval`: the run parked on a publication approval; the unlisted
   operator's resolve returned 403, its audit row is `denied` and
   unauthorized by the route's explicit user list (a set that admits nobody
   refuses the listed operator too, so it proves nothing), the approval stayed pending and nothing moved (work item,
   requests, publication, remote branches, owner); the three subjects
   differ; the allowed operator's reject and approve both carry operator audit
   rows; the approved attempt is a fresh approval; the second admission kept
   the work item and owner; and both runner pods read the same grant Secret,
   whose uid and resourceVersion never changed.
3. `pod-restart`: see the next section.
4. `publication`: the rejected attempt left no branch and no pull request;
   exactly one ready pull request exists for the run, and its head is the
   approved publication's own branch; the publication succeeded and the work item reads `published`; the diff
   reviewer approved before approval was requested; the published head's
   files equal the reviewed workspace hashes and carry the nonce; and the
   latest check runs and statuses for that head are all complete and
   successful, with at least one present. Pending or missing checks fail.
5. `budget`: the ledger rules above, the live window, the cleanup budget and
   grant coverage of the live window.
6. `cleanup`: every registered cleanup step succeeded; inventory queries with
   an explicit context found the namespace, release-labelled cluster-scoped
   objects, pushed registry manifests, local image tags, child processes, the
   work directory, the fixture issue, run pull requests and run branches all
   absent; foreign resources (the `kube-system` namespace, the agent-sandbox
   CRDs and controller, reused priority classes) kept their uids; and no
   output channel carried held secret bytes.

## What pod-restart proves on this base

When publication parks, the platform captures the reviewed workspace,
creates the approval and publication, and suspends the sandbox, which deletes
the runner pod. The driver records the workspace (nonce, file hashes, Git
HEAD and status) from the pod before that deletion, confirms the old pod UID
is gone and its process stream ended, and confirms the pending approval
survived with no publication or protected action meanwhile. The next session
runs in a new pod and a fresh native process, started after the deletion,
with the owner and generation unchanged, and the reviewed workspace is
preserved exactly into the published commit.

Restoring the reviewed edits into that fresh process is not a platform
capability on this base (ADR 0251 restoration is not wired), so the driver
does not claim it.

## Secrets and cleanup

The grant is read only inside `run`, after install and setup, and reaches the
cluster only as the Secret `<release>-personal-grant` created through
`kubectl create -f -` stdin. The in-memory value is then cleared, keeping a
digest only for scanning. The App token, API key, internal worker token and
operator principal tokens are held the same way. Child processes receive only
`PATH`, `HOME`, `LANG`, `KUBECONFIG`, `DOCKER_HOST` or `DOCKER_CONTEXT`,
`SSH_AUTH_SOCK` and per-call additions, and a held secret in a child's argv,
environment or input is refused. Every stdout, stderr, child output, HTTP
response, harvested transcript byte, ledger value and evidence line is
scanned in memory, and only booleans and counts are kept. The one exception is
diagnostic and private: every `approvals` CLI call keeps its exit code and
whole stdout and stderr, held secrets redacted, in the run's command log under
the state directory (a mint withholds stdout when it cannot parse the token),
and each `--resolve` records its HTTP status, the API's refusal detail and the
denying authorizer in the ledger. `judge` never prints them.

When a session fails, the evidence names why without keeping its text. A
synthetic or API-error transcript record is kept as an error class: the CLI's
category, the HTTP status, the provider error type and a first line with
credential-shaped values redacted. The ledger scrubs it again and the leak
scan reads it. A failed attempt's message names the class. The harvester also
records whether the runner container and each native CLI process had a
non-empty `CLAUDE_CODE_OAUTH_TOKEN`, as a state and a length; the value never
leaves the pod.

The run owns two kinds of forge refs: those under its random branch prefix,
and each publication's own branch as the API names it. Under the `approve`
policy the platform keeps the historical `curie/` prefix for that branch and
ignores the operator prefix, so the run reads it once each attempt parks.
Evidence, cleanup and the absence checks cover both.

The driver owns a forge object only after creating it succeeds. It closes or
deletes a branch or pull request only while its head is the commit this run
made: the forge probe's own commit, or the commit its publication pushed.
Anything else is left in place and reported by the absence inventory, so a
name collision never removes a foreign branch.

Before the namespace is deleted, the driver records the persistent volume
bound to each of the run's claims. A dynamically provisioned volume carries no
release mark, and one with reclaim policy `Retain` outlives the namespace, so
each recorded volume still claimed by the run is deleted afterwards and its
absence is part of the inventory.

Each resource's cleanup is registered before the resource is created and runs
in reverse order on success, failure, rejection and on SIGINT or SIGTERM.
Transcripts are parsed in memory; the ledger keeps only sanitized per-session
facts, never a raw transcript. A transcript whose final record is unterminated
or unreadable makes that attempt's usage unknown, so the budget judge fails
rather than counting a partial session.

## Two-owner scenario (#5379)

`personal_profile.py --scenario two-owner` proves issue #5379 against ADR 0268
section 6 rows 4, 5, 7, 11 and 12: two independently enrolled owners share one
installation and keep separate grants through execution, approval, CI repair,
recovery and disconnect. Its code is `personal_two_owner.py` (live driver, a
subclass of the single-profile harness) and `personal_two_owner_support.py`
(pure core: ceilings, inputs, manifest, ledger, gate, judges). Every
single-profile flag is refused under `--scenario`; the scenario reads one
private inputs file instead.

### What each criterion proves

1. `owners` and `roles`: both owners run the implementer, plan reviewer, diff
   reviewer, real tools and a bundle hook on their own grant only. WorkItem,
   every request, custody and delivery identity, session and CLI reads agree on
   tenant, immutable owner and generation, and no installation-wide grant exists.
2. `approval` and `publication`: a listed operator rejects A's first
   publication and approves its fresh successor and B's publications, an
   unlisted operator gets 403, nothing is pushed before approval, and approved
   trees equal the reviewed workspace on the branch the API reports.
3. `ci-repair`: a real failing check on B's published head reaches production
   CI ingestion; the production repair keeps B's owner and grant, pushes a
   repaired head and its latest and required checks pass.
4. `restarts`: pod, worker and native-process interruptions each keep the
   WorkItem, conversation, owner and generation, confirm the old runtime gone
   and show a fresh native process continuing on the same owner's grant.
5. `wrong-owner`: B cannot adopt or read A's work before A's authorized
   continuation, which then runs with its original tuple.
6. `disconnect`: A's disconnect during active work fences and tears A down; B
   makes a new tool call after A's confirmed stop and publishes a passing head;
   after a worker restart A cannot launch, reuse or resume.
7. `budget` and `cleanup`: every start was reserved and settled from native
   usage inside the ceiling, and every owned resource is absent with no grant
   bytes in any scanned channel. `rehearsal` judges the unpaid rehearsal ledger.

### Inputs file

`--inputs-file` is one JSON object: a regular file you own, mode 0600, at most
64 KiB, not a symlink, in a directory no group or other user can write. Every
path is absolute, unknown or duplicate keys are refused, and any credential-like
key or token-shaped string is refused as `inline_grant_refused`. Grant bytes
never appear in it; it names grant files, which `check_grant_file` validates.

```json
{
  "target": {"context": "kube-context", "namespace": "pto-demo", "release": "pto-demo",
             "registry": "127.0.0.1:5999", "docker_context": "builder",
             "kubeconfig": "/path/to/kubeconfig"},
  "forge": {"app_dir": "/path/to/app-kit"},
  "gateway": {"resolution_channel": "C0EXAMPLE1",
              "slack_bot_token_file": "/path/to/bot-token-file"},
  "independent_accounts": true,
  "owners": {"a": {"grant_file": "/path/to/grant-a", "usable_until": "2026-11-12T20:00:00Z"},
             "b": {"grant_file": "/path/to/grant-b", "usable_until": 1794000000}},
  "prior_item_output_tokens": 0,
  "probe_cli": "claude"
}
```

1. The setup descriptor (`images`, `rehearse`, `forge-probe`) holds only
   `target`, `forge`, `gateway` and optionally `probe_cli`. Owners,
   `independent_accounts` or any `grant_file` in it is `setup_inputs_carry_grants`.
2. The live descriptor (`preflight`, `run`) must set `independent_accounts` to
   exactly `true` (the caller's independent-enrollment assertion, else
   `independence_unasserted`). The two grant files must differ by resolved
   path, inode and content digest (`duplicate_owner_grant`).
3. `usable_until` is ISO 8601 with an offset or epoch seconds, stated by the
   caller. Each must be at least 5,400 s (live plus cleanup) after now, else
   `lifetime_insufficient`; the run records the window and the budget judge
   checks it covers the live end.
4. `prior_item_output_tokens` is the item's earlier actual spend (0 to 32,768).
   It counts against the aggregate guard and shrinks the schedule's room.
5. `namespace` is at most 56 characters because the driver also owns
   `<namespace>-grants`, the custody namespace, created before helm.

### Commands

```sh
p() {
  python3 tools/factory-e2e/personal_profile.py "$@" \
    --scenario two-owner --candidate "$SHA" \
    --state-dir "$STATE_DIR" --run-id "$RUN_ID"
}
p images --inputs-file "$PRIVATE_SETUP_FILE"
p rehearse --inputs-file "$PRIVATE_SETUP_FILE"
p forge-probe --inputs-file "$PRIVATE_SETUP_FILE"
p preflight --inputs-file "$PRIVATE_LIVE_FILE" --approve-live "$APPROVED_CEILING"
p run --inputs-file "$PRIVATE_LIVE_FILE" --approve-live "$APPROVED_CEILING"
p judge --criterion all
```

1. `images` builds the candidate images and the gated private bundle, and
   mirrors a pinned Dex image into the run's loopback registry. Use a distinct
   rehearsal run ID.
2. `rehearse` is unpaid and takes no grant and no `--approve-live`: namespace,
   install, Dex logins per owner, forge issues, delivered-card controls
   (unlisted 403, authorized reject and approve), wrong-owner admission
   controls with the worker stopped, post-restart refusals, the fence and
   cleanup. It cannot pass a live criterion.
3. `forge-probe` is the single-profile write probe plus read-only CI reads.
4. `preflight` is unpaid and mutates nothing: exact approval string, inputs
   file safety, windows, the schedule check, and image digests. It does not
   authenticate grants or sessions.
5. `run` performs the one live run. A second `run` for the same run ID is
   refused; there is no retry flag.
6. `judge` reads receipts only: no inputs file, no credential, no inference.
   Criteria: `owners`, `roles`, `approval`, `publication`, `ci-repair`,
   `restarts`, `wrong-owner`, `disconnect`, `budget`, `cleanup`, `rehearsal`,
   `all`. Output carries `not_met` for items reported but never failing.

The approved ceiling, exactly as `approved_ceiling()` builds it:

```text
sessions=16,turns=192,session-turns=32,output-tokens=32768,session-output-tokens=8192,owner-output-tokens=16384,session-seconds=300,live-seconds=4800,cleanup-seconds=600,paid-retries=0,probe-model=claude-haiku-5-5,probe-output-tokens=32,probe-turns=1,probe-seconds=30
```

### The 16-start manifest

Each group is reserved and persisted before the native start it covers, in
this order. A group's per-session share is `min(8192, floor(available /
sessions))` with `available` the smaller of the owner's and the aggregate's
remaining output tokens, counting settled actual usage and charging unknown
usage at its full share. Turns per session are `min(32, (192 - used) /
sessions)`.

| Group | Owner | Sessions | Production transition that starts it |
|---|---|---|---|
| a-probe | A | 1 probe | driver grant probe in an empty HOME, before A1 |
| a1 | A | implementer, plan reviewer, diff reviewer | trusted admission, execute turn; reviewers via the Agent tool |
| b-probe | B | 1 probe | driver grant probe, before B1 |
| b1 | B | implementer, plan reviewer, diff reviewer | trusted admission, execute turn |
| a2 | A | implementer, plan reviewer, diff reviewer | admission of the fresh successor after rejection |
| b-r1 | B | implementer, held at its prompt, zero usage | the `factory_ci` round-2 continuation consumed after the worker restart |
| a3 | A | implementer | admission of the authorized follow-up; ended by disconnect |
| b-r2 | B | implementer, diff reviewer, second diff reviewer | kernel runner-error retry on the same request and epoch |

Transitions that start nothing: kernel publish continuation, reviewer start
inside a reserved group, approval resolve and rejection, the publication job,
CI polling and rerun, the owner-lost refusal, pod deletion at a park, the
wrong-owner controls, disconnect terminate and worker restarts. Fenced as
unplanned starts: an owner-lost successor, a round-3 repair, an unpublished
continuation of a CI turn, and any session the harvest cannot map to a
reservation. The unpaid schedule check (G-SESSION-PLAN, run by `preflight` and
`rehearse`) simulates the manifest on planning usage (implementer 2,700, plan
reviewer 1,000, diff reviewer 800, probe 32 output tokens) and requires 16
starts, a share of at least 2,700 for implementer groups and 1,000 for others,
at least 16 turns per session, each owner at most 16,384 and the aggregate at
most 32,768 minus prior spend, and every production transition mapped.

### Live phase order

1. Unpaid: `refusals`, `namespace`, `images`, `install`, `setup` (Dex login and
   enrollment per owner), `forge`. Then `probe-a`, `attempt-a1` (parks on
   card 1), `probe-b`, `attempt-b1` (parks on card 3). Every attempt's pod is
   deleted at its park after terminal capture, so each later start is a fresh
   native process.
2. `wrong-owner`, then `reject-a1` (unlisted operator refused, listed operator
   rejects), `attempt-a2` (fresh successor in a new pod), `approve-a2`.
3. `approve-b1` once A is quiescent, then `worker-stop`: B's real failing check
   queues the round-2 repair while the worker is down.
4. `queue-a3` (A3 admitted with the worker stopped), `worker-start` (a new
   worker pod; B-r1 reserved), `hold-both` (B-r1 held at its prompt, A3 runs one
   tool and is held).
5. `disconnect-a`, `process-interruption` (B-r1's CLI killed and runner PID 1
   signalled; the kernel retries as B-r2), `approve-b2`, `post-disconnect`
   (another worker restart; A cannot launch, reuse or resume, B stays active).
6. The fence (worker to zero, claims and runner pods deleted, active owners
   disconnected), cleanup and inventory.

### Response gate and subagent limits

The private bundle gains `hooks/two_owner_gate.py` on `UserPromptSubmit`,
`PreToolUse` (`*`), `Stop` and `SubagentStop`. Each hook writes a hold file keyed
by its pid and start time and waits up to 240 s (hook timeout 250 s) for a
release; a stop verdict or timeout fails closed. A long-lived in-pod stream
reports holds to the driver, which waits until the harvest has read the
transcript to the reported size, recomputes usage, and releases only if the
session share, per-session cap, turns, 300 s, owner allocation (16,384) and
aggregate guard all hold with one more response (2,048 tokens) in flight.
Otherwise it stops the hold, interrupts the runner, deletes the pod and records
the breach; unknown usage always stops. Before the first prompt release the
driver writes `/tmp/pto-gate/limits.json` with the pod's reviewer allowance from
its group (one plan and one diff reviewer for three-role groups, two diff
reviewers for b-r2, none for probes, a3 and b-r1). An absent file, a missing
type or a spent allowance denies the Agent call.

### CI repair fixture

A's issue asks only for a nonce line, so A's CI passes. B's issue asks for
`unitconv/tests/test_ci_ack.py` with exact content: skipped unless
`GITHUB_HEAD_REF` is set, otherwise it fails unless `CI_ACK.md` at the root has
the line `ci ack: <GITHUB_HEAD_REF>`, printing an `::error file=CI_ACK.md` line.
The head ref (`curie/publication-<hex>`) exists only after the first
publication, so B's first head fails in real Actions CI. The driver observes
the failure, the single rerun (or its forbidden refusal) and the production
`work-item-<request>-ci-2` event, then the repair adds `CI_ACK.md`.

### Ledger and public output

The private SQLite ledger (`ledger.sqlite3` in `$STATE_DIR`, mode 0600) holds
reservations per owner and group, sanitized per-session facts and native error
classes, every observation each criterion judges, cleanup steps, inventories and
the token scan over stdout, stderr, children, http, harvest, ledger and evidence.
Public output is verdicts, booleans, counts and refusal codes: no identifier, no
transcript, no grant, session or client value. Grants travel only by file; the
CLI reads session files the driver writes 0600 and the finalizer removes.

### Interpretations recorded for reviewers

1. Pod recreation lands at the quiescent approval park between A's first and
   second attempts; A1's pod is deleted after its terminal capture and A2 must
   run in a new pod. The process interruption restarts the runner container in
   place, and the same pod, claim, request and epoch continue.
2. B's fourth repair start is a second diff-review round, admitted only when the
   first returns changes, so b-r2 reserves for it either way.
3. Principal sessions are minted during the run through a run-local identity
   provider, so `preflight` validates file safety, windows, schedule and images,
   and the run validates sessions through `/console/principal`.
4. Code-host steering by another owner is admitted under the original owner by
   design and is not driven.
5. `curie cluster message` cannot target a GitHub-issue conversation, so no
   message control is recorded.

### Not met

Restoring uncommitted workspace edits into a fresh or continuing process is Not
met and not claimed. It is tracked in #5541. `judge` lists it under `not_met`
for `restarts` and it never changes a verdict.

## Personal owner enforcement proof

`personal_owner_enforcement.py` is the live proof for issue #5378: personal
Claude subscription execution keeps its owner binding across claims, adoption,
steering and recovery. It builds on #5377's `EnrollmentProof`
(`apps/worker/tests/sandbox/personal_enrollment_cluster_support.py`) and #5374's
exact-image harness. The difference from #5377 is the launch boundary: here the
candidate worker kernel authorizes the owner, reads custody and launches. The
driver only admits through the API's trusted internal admission route
(`POST /v1/internal/work-items/admissions` with `X-Curie-Worker-Token`), seeds
one marker record into a WorkItem's durable transcript through the state API,
interrupts, and observes. The worker runs from the candidate checkout with
`FACTORY_MODEL_AUTH_MODE=personal`, the Kubernetes substrate and the production
custody reader, and holds no model credential.

### Code host and repository workspace

Every WorkItem runs with a real repository workspace against the recorded TLS
GitHub stub (`tools/github-stub/github_stub.py`), run as `tls_code_host.py`
runs it and built from its `stub_manifests`: one Pod in the owned namespace on
the digest-pinned candidate image, a headless Service, and the stub's source
ConfigMap, which also carries a two-file seed tree and an issue body. The stub
generates a per-run test CA and seeds a bare `acme-corp/acme-bot` repository;
only its public certificate leaves the Pod, as the `code-host-ca` ConfigMap.
The driver creates one stub issue per WorkItem (the refused other principal
and each interruption pair) and admits each owner WorkItem against that issue,
the stub's repository id and its installation.

The candidate API and worker are source processes, so they reach the stub
through a `kubectl port-forward` the driver owns on loopback (the stub's
certificate names `127.0.0.1`). The API's GitHub API and clone bases point at
that forward, its App is the stub's App id with a key generated per run that
reaches only the API process environment, and its repository allowlist is the
stub repository. Both processes trust the stub CA plus the public roots through
a private bundle (`SSL_CERT_FILE` for the API, `GIT_SSL_CAINFO` for the
worker's clone). The worker clones, archives and uploads the checkout as in
production, and signs the sandbox's one-object read against the object store
through a relay on the runner API host. The sandbox template gains the chart's
own `workspace-init` program, read from the candidate's `agent-sandbox.yaml`,
the chart's default `emptyDir` workspace at `/workspace`, and the chart's
`codeHostTrust` mount at `/etc/curie/code-host-trust/ca.crt` with
`CURIE_REPO_CA_BUNDLE`, `GIT_SSL_CAINFO`, `SSL_CERT_FILE` and
`REQUESTS_CA_BUNDLE` naming it. Publication is on, as in a factory install
(`CURIE_PUBLICATION_ENABLED=true`, its namespace the owned one): the kernel
binds a publication context before every workspace-backed execute turn and
refuses the turn without one. For a WorkItem with no pull request the API's
context mint answers 204 without a code host read, so the API needs nothing
more. No proof step publishes, and each objective tells the model to answer
the kernel's one unpublished continuation with `No changes needed:` and no
tool, so no publication Job is created.
After each pair and at the end of the audit, the driver reads the stub's record
of unsupported calls and fails with the call's method and path if there is one.

### Runner reachability

#5377 drives its turns itself, so its worker never dials a runner. Here the
worker opens and streams every turn, and runner Service names
(`<sandbox>.<namespace>.svc.cluster.local`) do not resolve outside the cluster:
without help every status read fails closed and the request defers before it
starts. The driver therefore starts the candidate worker through a launcher
that maps exactly that namespace's Service names to a deterministic loopback
address (never `127.0.0.1`) and then runs `curie_worker` as `python -m` would;
every other name resolves as before. On every sweep the driver keeps one
`kubectl port-forward` per Running runner Pod from that address to the Pod's
runner port, replaces it when the Pod is recreated or the forward exits, and
drops it when the Pod goes away. Teardown stops every runner forward and proves
each address closed.

Every observation is scoped to its own WorkItem: a pod counts only when its
claim is the `runtime_claim_name` of a started request of that WorkItem, and
the claim, its `<claim>-tokens` Secret and the Pod are read the moment the pod
is seen, by the claim name the Pod carries. A claim, Secret or Pod that is
already gone is recorded missing, never raised.
Only the start route writes that column and `started_at`, for the owner holding
the request's acquire, and the driver never acquires; `selection` also requires
the request's `runtime_owner`, read while it runs (terminal settlement clears
it), to be the worker's consumer. The owner has one runtime slot (ADR 0268
decision 2), so after each pair the driver waits until every request is
terminal, its Pods are gone and no claim carries the owner's label, and stops
the run, naming it, if a claim outlives its WorkItem.

### Inputs and preconditions

Every target comes from flags and the input file. `--input-file` is a private
JSON object (mode 0600, owned by the caller) with `kubeconfig` and `curie_bin`
(absolute paths), `runner_api_host` (this host's IPv4 address as the cluster
reaches it) and optionally `grant_probe_cli` (default `claude`). `--registry` is
the caller's loopback registry, `127.0.0.1:PORT` or `localhost:PORT`, with
manifest deletes enabled. The Docker context must have an SSH endpoint whose
loopback ports the caller mirrors onto this host's loopback, as #5377 requires.
The cluster runs the agent-sandbox controller. Run the driver under the
repository environment (`uv run`), from a clean checkout at `--candidate`.

### Commands

```bash
uv run python3 tools/factory-e2e/personal_owner_enforcement.py preflight \
  --candidate "$SHA" --context "$CTX" --namespace "$NS" --docker-context "$DCTX" \
  --registry "$REG" --input-file "$INPUT" --state-dir "$STATE" --run-id "$RUN_ID" \
  --grant-file "$GRANT_FILE" --grant-expires-at "$GRANT_EXPIRES_AT"

uv run python3 tools/factory-e2e/personal_owner_enforcement.py rehearse \
  --candidate "$SHA" --context "$CTX" --namespace "$NS" --docker-context "$DCTX" \
  --registry "$REG" --input-file "$INPUT" --state-dir "$STATE" --run-id "$REHEARSAL_ID"

uv run python3 tools/factory-e2e/personal_owner_enforcement.py run \
  --candidate "$SHA" --context "$CTX" --namespace "$NS" --docker-context "$DCTX" \
  --registry "$REG" --input-file "$INPUT" --state-dir "$STATE" --run-id "$RUN_ID" \
  --grant-file "$GRANT_FILE" --grant-expires-at "$GRANT_EXPIRES_AT"

uv run python3 tools/factory-e2e/personal_owner_enforcement.py judge \
  --state-dir "$STATE" --run-id "$RUN_ID" --criterion all
```

`preflight` is read-only and unpaid. It checks, in order: no service
credential or fake mode in the caller environment, the flags, the input file,
the grant file by `stat` only (present, regular, mode 0600, the caller's, 1 to
4097 bytes; never opened), an expiry that leaves at least 1,620 s, the Python
environment, a clean checkout at the SHA, `docker info`, the registry's `/v2/`,
`/readyz` through `--context`, the absence of both namespaces, RBAC for every
verb the run needs, and an unused run ID. A missing prerequisite is a refusal
with exit code 2, never a skip. `rehearse` and `run` repeat it, then re-execute
the driver as a child whose environment holds only `PATH`, `HOME`, `LANG`,
`SSH_AUTH_SOCK` and the names and paths the proof needs, with stdin from
`/dev/null`. Only that child opens the grant file.

A separately authorized attempt uses a new run ID. Pass `--prior-run-id` for
each earlier attempt and `--item-output-token-cap` for the approved combined
ceiling. The item ledger includes every session from those attempts. Settled
native usage is measured; missing settlement is charged its full reservation
share and remains unknown. Reservations and every response check enforce the
combined cap as well as the unchanged per-attempt ceilings. These accounting
flags grant no retry authority.

`--phases remaining --reuse-run-id SOURCE_RUN` runs only rejection,
revalidation, disconnect and the hosted-header audit. The source must be a
finished live run whose selection, restart and fencing judgments passed,
allowing only the tracked #5541 observations. The driver refuses reuse when
any path outside this proof's driver, tests, fixture or README changed. Each
reused criterion names its original run and candidate in the judge output;
its observations are never relabeled as fresh proof.

The remaining-phase bootstrap admits an owner-bound generation-1 WorkItem
without starting a model, seeds the same durable history marker, replaces the
enrollment with the invalid generation-2 control and explicitly revalidates
the waiting request. The real worker executes that request. Generation 3
still replaces the invalid grant, waits for teardown and explicitly
revalidates that same WorkItem before a fresh native process starts. No SQL
completion is forged, no live state is restored from a previous test namespace
and no recovery requirement is dropped. Only the `rejected` and `revalidated`
native sessions are reserved; each retains its original 1,024-token and
four-assistant-turn share, and all attempt and item ceilings still apply.

`--phases post-rejection --reuse-run-id SOURCE_RUN` omits the already-proved
rejection session as well. It requires every rejection check and cleanup to
have passed in that finished live source. The judge reads those rejection
checks from the original ledger, naming their original candidate and run on
each check. Selection, restart and fencing provenance is flattened to its
original source. Revalidation and later checks must come from the new run.
The bootstrap admits a waiting request, seeds durable history, advances to
the invalid generation without starting it, then replaces it with the valid
grant and explicitly revalidates the same waiting request to generation 3.
The workspace is checked against the durable repository before native launch
and again before continued work; the new process is compared with the source
run's old native identity, whose teardown was confirmed. No failed request or
completion is fabricated. Only the valid revalidation session is reserved.

`judge --criterion` takes `selection`, `fencing`, `authentication-required`,
`disconnect`, `restart`, `hosted-header`, `budget`, `cleanup`, `rehearsal` or `all`. It
reads only the private ledger, compares each observation with the expected
value defined in `personal_owner_enforcement_support.py`, and prints every
check's expected and observed value. Exit code 0 is a pass, 1 a failed or
missing observation, 2 a refusal, and 3 a run whose only shortfall is a
recorded Not met. A rehearsal ledger supports `--criterion cleanup` and `--criterion rehearsal`.
The latter retains every reading and marks provider rejection, native tool calls,
native process epoch confirmation and the native response cap as `rehearsal_only`;
it never treats them as live proof.
A fake turn can finish and delete its Pod before log capture, so missing
captures remain visible as `rehearsal_only`. The live confidentiality judge
still requires complete captures. Product and cleanup failures fail the rehearsal judge.

### What rehearse covers

`rehearse` starts zero native model sessions. The worker and every runner use
the runner's offline fake model, the grant is a synthetic value, and any native
CLI process seen in a runner pod is recorded as an unreserved start, fences
every start and fails the rehearsal. Against the real cluster it runs:

1. The candidate image build, its push to the caller's registry under a per-run
   tag, digest pinning, and the revision label and digest check on every pod.
2. Both owned namespaces, the backing containers, the migration, the stub code
   host with its CA, forward, issues and seeded repository, the candidate API
   and worker, the agent deploy, the two OIDC principals and the sentinel
   hosted connector (Pod, Service, bearer Secret).
3. For every WorkItem, workspace selection, the worker's clone of the stub
   repository, the archive upload and the sandbox's `workspace-init` staging.
4. Enrollment through `curie cluster subscription`, the other principal's
   refused admission, the owner's admission and the worker-driven personal
   claim with its owner labels and `<claim>-tokens` grant delivery.
5. The three interruptions against fake-model turns: SIGKILL of every process
   in the runner container except the runner, then SIGTERM to the runner, so
   the container restarts in place; deletion of the runner Pod; SIGKILL of the
   worker followed by a restart. It observes the recovered pod, labels, durable
   history, the stub repository's committed state in the workspace and the
   uncommitted workspace nonce.
6. The generation advance while the worker is down and the claim retained, the
   refused revalidation, the old generation's custody refusal, teardown after
   the worker restarts, revalidation, the later generations' admissions, the
   disconnect with a waiting request and two worker restarts.
7. The full finalizer and the absence proof.

Only `run` covers: the grant probe, native CLI sessions under the owner's
grant, the bundle hooks that hold each session at a Read call and at Stop,
transcript usage settlement against the ledger, the provider's real rejection
of the invalid grant, the Bash scan for the hosted header, the sentinel
challenge and public control through the model, and history replay inside a
fresh native process. Rehearsal turns end on their own, so an interruption can
miss its turn; the rehearsal judge reports the observations and their evidence limits.

### Sessions and reservations

The disk limits are explicit caller inputs: `--disk-start-percent` (default 82)
and `--disk-stop-percent` (default 88). During live runs the driver checks before each reservation
and native launch and samples every 30 seconds, including during the build.
At the stop limit it interrupts the native runtime, deletes its Pod, cancels
owned commands and tears down. Receipts include before, peak and after disk
percentages; the caller records the final reading after removing its registry.

The approved ceiling is 8 native session attempts, 4 assistant turns per session
and 32 in total, including publication continuation, 8,192 output tokens in
total, 180 s per session, 1,440 s live plus 180 s cleanup, zero paid retries,
and `claude-sonnet-5-5`. Each attempt reserves 1,024 tokens and 4 turns in the
private ledger. The candidate fixture wraps the SDK's native executable with
a gate that waits for that committed reservation before executing it; every
permit is consumed once. A retry without a reservation never executes the
native CLI. The original native executable remains unchanged.

Each query has 2 turns and a 512-token runner cap; each response has a
256-token native cap. At each prompt the driver checks that the next query's
worst case fits the session and run allowance. Tool hooks and Stop hold after
responses while the driver harvests their usage, with the output-token total
checked first. Stop is released for continuation only when it fits. Unknown
usage stops the run. The judge and run receipt include ceilings and observed
counts.

An independent watchdog interrupts the native process and deletes its Pod at
the session deadline even if harvesting fails, and fences starts at the live
window deadline. Cleanup uses one 180 s budget with time reserved for absence
checks. Namespaced deletion requires the recorded namespace UID and ownership
label. The capture audit requires complete Pod logs and scans the valid grant,
invalid grant and hosted header. Grant recovery by Bash fails confidentiality.

Immediately before the first reservation, a one-turn grant probe runs on the
cheapest model with an empty `HOME`; if it fails, nothing is reserved.

1. `process-interrupted` and `process-recovered`: the first holds at Read, is
   settled, the second is reserved, and the runner container restarts in place.
2. `pod-interrupted` and `pod-recovered`: the same, with the Pod deleted.
3. `worker-interrupted` and `worker-recovered`: the same, with the worker
   killed. The recovered session is held while the worker is killed again, the
   grant is replaced by a deliberately invalid one (generation 2), and the
   retained claim's fate is observed. The fence aims at the started request
   whose runtime is the retained claim (the recovered session's, else the
   interrupted one's), never at a request that has not started.
4. `rejected`: generation 2 on the same WorkItem, admitted once every earlier
   request is terminal (admission refuses `active_request` while one waits,
   runs or stops); the provider's 401 must end it `authentication_required`
   with the transcript and owner intact.
5. `revalidated`: generation 3 after replace, teardown and revalidation; one
   response calls the Bash scan, the sentinel `challenge` tool and the public
   control tool, then answers.

Unknown usage, an unsettled earlier session, an unterminated final transcript
record or a native start without a reservation stops every later start. Only
records that exactly match the seeded history are excluded from the count.
Every synthetic or zero-usage record keeps a sanitized error class.

### Workspace checks

Before each interruption the driver writes a random nonce into an untracked
file in the sandbox's `/workspace` checkout and reads the checkout's `HEAD`,
the SHA-256 of every tracked file and its porcelain status; after recovery it
reads them again in the recovered pod. The stub's bare repository is the
durable source: `main`'s commit and the SHA-256 of each committed blob. For all
three interruptions `restart` requires `HEAD` and the committed file hashes to
equal the stub's on both sides, the interrupted runtime to be gone (the native
process after a container restart, the Pod UID after deletion, the worker
process after its kill), a fresh native process, and the same owner labels and
generation on the recovered claim. For process interruption, Pod identity and
container restart are observations rather than pass conditions. The driver
records the exact native process identity before signalling it, observes it
gone, and confirms the request still binds the captured runtime epoch and
claim before any successor owner claim appears. It records every driver Pod
delete call and requires none during this interruption. Live-provider recovery used a fresh
Pod, while the fake-model rehearsal restarted the container in place. This is
an observed difference. Fresh-Pod recovery stages the checkout again from its
durable source, so committed state is restored and the uncommitted nonce is
absent. Process interruption and Pod recreation both record this observation
as **Not met: #5541** (blocked by #5092), and neither stops the live scenario.
The judge exits 3 when tracked restoration observations are the only shortfall.
The worker interruption records whether the nonce
survived as information only, since recovery may adopt the retained Pod or
start a new one. The status before and after is recorded for every pair.

### Live condition audit

The live conditions were checked against ACs 2, 3, 5, 6 and 7 before another
paid start. No pass condition requires surviving process memory or an
uncommitted file. The complete set of observations touching those assumptions
is below; all other conditions remain blocking.

| Condition | AC | Disposition |
| --- | --- | --- |
| `process.same_pod` | 7 | Informational; cold-claim recovery may use a fresh Pod. |
| `process.container_restarted` | 7 | Informational; an in-place restart is not required. |
| `process.workspace_nonce_restored` | 7 | Observation, Not met: #5541 when absent. |
| `pod.workspace_nonce_restored` | 7 | Observation, Not met: #5541 when absent. |
| `worker.workspace_nonce_retained_informational` | 2, 7 | Informational; uncommitted restoration remains #5541. |
| Each interruption's `git_status_before` and `git_status_after` | 7 | Observations; a clean recreated checkout is allowed. |

AC 2 retains owner/generation fencing, denial across worker restart, old-claim
and delivery-Secret removal, epoch-confirmed teardown and explicit
revalidation. AC 3 retains original-owner preservation, durable transcript
preservation on provider rejection, no service fallback, disconnect denial
across restart and a fresh revalidated native process. AC 5's HTTP/consumer
suite checks admission and the persisted owner tuple; it imposes no Pod,
memory or uncommitted-file requirement. AC 6 retains actual connector/tool
liveness, public and exposed-marker controls, child credential denials and
complete captures; scanner files are read in the session that wrote them,
without crossing a recovery boundary. AC 7 retains durable history and replay,
committed HEAD and file restoration, owner and authorized generation, fresh
native processes, interruption-specific old-runtime absence, and a new Pod UID
for the Pod-deletion case. Process-only interruption and old-epoch confirmation
before a successor claim binds remain required. The fresh-process and new-Pod
conditions match explicit AC 3 and AC 7 text and are not converted to observations.

### Remaining evidence capture audit

The native CLI does not reliably persist an authentication failure in its
project transcript. In observed SDK-bundled CLI 2.1.317 output, stream-json
assistant/result records expose `api_error_status: 401`; the assistant uses
the CLI classification `authentication_failed`. The provider's distinct
`authentication_error` type is in the native debug response body. The sanitized
fixture is `tests/fixtures/native_auth_refusal.json`.

Every native session now starts a Pod log stream before its launch permit is
released. Only the deliberately invalid control enables `--debug-file
/dev/stderr`, after the driver confirms that Pod's delivery Secret contains
the invalid control. A real grant is refused debug capture. The SDK's normal
stderr sanitizer forwards the response to the already attached stream.
The trusted worker records error/final frames from its actual `TurnStream`
iterator into private SQLite before delivering them to the kernel, reusing
the enrollment driver's ACI observation mechanism. No frame is replaced.
The refusal requires both the provider response and a same-epoch ACI
authentication-required error plus classified-failure final. Missing,
truncated or conflicting evidence stops with a named capture reason.

Attempt 4's retained runner log and actual worker-consumed ACI journal show a
`max-turns` error and classified-failure final at the end of the revalidation
turn. The runner logged that turn end at `10:49:53.236930Z`, before cleanup
started at `10:49:53.626400Z`. This was a terminal failure, not an approval or
publication wait, and it was not a successful Stop. ToolSearch consumed the
first of the fixture's two allowed responses; the second issued Bash and both
MCP calls in parallel. The trace observed only three response-hook gates for
the four tool calls. A shared response holding/release filename allowed those
parallel callbacks to overwrite or remove each other's gates. The executable
regression starts parallel copies of the actual hook and proves that releasing
one cannot release another. Each response hook now owns its PID-specific gate.
The fixture also sets `ENABLE_TOOL_SEARCH=false`, as the unpaid native header
controls do, so the actual catalog is available within the unchanged two-turn
query budget. All session, continuation and response ceilings remain unchanged.
An ACI terminal error now fails revalidation immediately with its classification
instead of waiting for Stop and misreporting the absence as no fresh process.
The sanitized real trace is `tests/fixtures/native_revalidation_end.json`.

Attempt 4's last response reading was 258 output tokens and two assistant turns,
but its ACI failure final carries no usage. Raw native transcript reads had been
omitted from retained command output. That does not certify final usage: its
full 1,024-token reservation remains charged. Subsequent response harvests keep
mode-0600 native records and exact replay seeds in the private evidence directory,
after the held-value scan, so Pod deletion cannot erase the usage evidence.

Attempt 4's retained rejection ACI final also carries no usage, and no retained
rejection transcript certifies its output count. The provider's HTTP 401 and
`authentication_error` plus the matching ACI classification remain real rejection
proof. Re-judging them offline records unknown usage and charges the rejection's
full 1,024-token share, alongside revalidation's full share. The conservative item
total is 6,029 of 16,384 output tokens. The judge reports unknown rejection usage
as an observation, while measured nonzero rejection output fails its zero-output
control. A partial measured count can never charge less than either that count
or its full reservation. Revalidation requires a recorded predecessor identity
before it compares native processes in any phase scope.

The empty-HOME, filtered-environment unpaid probe ran the real bundled CLI
through the launch wrapper and stderr sanitizer with an invalid OAuth token.
It observed HTTP 401, provider type `authentication_error` and zero output
tokens. Neither an empty capture nor seeded history alone is accepted as
measured zero usage. Logs remain available after Pod deletion; the final
audit requires every native log stream to reach successful EOF.

The table names every remaining pass condition and its live evidence source.
Unpaid tests cover parsers, missing/conflicting inputs, capture ordering,
grant selection for debug, stream persistence, EOF, scope provenance,
reservations, response gates and cleanup. The offline rehearsal drives the
real API, worker, backing services, workspace and connector with its declared
fake provider; those readings never become paid-provider proof.

| Phase | Conditions | Evidence on the live path |
| --- | --- | --- |
| Rejection | `rejected.generation` | The current ExecutionRequest row after explicit revalidation or generation-2 admission. |
| Rejection | `rejected.capture_source`, `rejected.http_status`, `rejected.provider_error_type` | Provider response in the actual rejected native process's debug stderr, retained by the pre-launch Pod log stream. |
| Rejection | `rejected.aci_classification` | Actual worker-consumed error/final frames, paired by runner epoch in private SQLite. |
| Rejection | `rejected.terminal_cause` | The worker's terminal ExecutionRequest row, read through the real database. |
| Rejection | `rejected.output_tokens` | The paired ACI final output-token count or retained native transcript usage. Missing usage stays unknown and charges the full reservation; a classified refusal alone never certifies zero. |
| Rejection | `rejected.transcript_preserved` | Durable transcript record IDs before and after the refused request. |
| Rejection | `rejected.owner_preserved` | The immutable WorkItem owner read after the refusal. |
| Rejection | `rejected.service_fallback_claims` | Current claim labels, retained provenance for deleted claims and every observed runner Pod. |
| Revalidation | `revalidated.generation` | The explicit revalidation API response after generation-2 teardown confirmation. |
| Revalidation | `revalidated.fresh_native_process` | Pod UID, native PID and process-start identity compared with the rejected process. |
| Revalidation | `revalidated.committed_head_restored`, `revalidated.committed_files_restored` | Real workspace HEAD and tracked-file hashes before rejection and in the fresh valid process, compared with the durable stub repository. |
| Revalidation | `revalidated.replay_marker_seen` | All retained native transcript files for the recovered session, including any continuation. |
| Revalidation | `revalidated.assistant_turns_positive` | Complete native assistant usage records, excluding only exactly matched seeded records. |
| Hosted header | `sentinel.challenge_returned`, `public_control.returned` | Actual tool-result records across the session's complete captured transcript set. |
| Hosted header | `sentinel.authorized_calls_positive`, `sentinel.unauthorized_calls` | Counters read from the real namespaced sentinel service. |
| Hosted header | `bash.scan_ran`, `bash.negative_control_found` | Scanner output retained at the first Stop before permitting continuation; the deliberately exposed marker must be present. |
| Hosted header | `bash.header_found`, `bash.grant_found` | Exact and encoded held-value scans of the captured Bash scanner output. Missing scanner output remains a failure. |
| Hosted header | `audit.captures_complete` | Every pre-launch native log stream ended successfully; sentinel logs and all declared captures are present. |
| Hosted header | `audit.header_in_captures`, `audit.grant_in_captures`, `audit.invalid_grant_in_captures` | Blocking scans of complete logs, claim/template manifests and events. |
| Hosted header | `audit.header_in_ledger_or_output` | Blocking raw-byte scans before retained command output, ledger entries or driver output are published. |
| Disconnect | `disconnect.generation`, `disconnect.state` | Disconnect API response. |
| Disconnect | `disconnect.waiting_request_started` | The specifically admitted waiting request's `started_at`. |
| Disconnect | `disconnect.custody_secrets` | Custody Secret inventory in the owned grant namespace after reconciliation. |
| Disconnect | `disconnect.claims_after_restarts`, `disconnect.pods_after_restarts`, `disconnect.tokens_secrets_after_restarts` | Claim, Pod and delivery-Secret inventory across both actual worker restarts. |
| Disconnect | `disconnect.native_sessions_after_restarts` | Launch-gated native process identities observed before and after both restarts. |
| Disconnect | `disconnect.admission_code`, `disconnect.revalidation_code`, `disconnect.custody_reader_code`, `disconnect.route` | Real admission/revalidation responses, custody-reader refusal and route lookup. |
| Budget | Recorded session, total-turn and output-token ceilings | The live-window ledger records the same enforced constants used by reservations and query/response gates. |
| Budget | `spend_guard.breach`, `sessions_within_ceiling`, `every_session_within_share`, `unreserved_starts`, `paid_retries` | Durable reservations, native launch observations, per-response harvests and settlements. |
| Budget | `usage_known`, `zero_usage_sessions_classified`, per-session assistant turns, total turns/output tokens | Complete native usage records or measured ACI counts; incomplete usage stops the attempt and charges at least its full share. |
| Budget | `models`, `grant_probe_ok`, `live_seconds_within_ceiling` | Native model metadata, the immediately preceding empty-HOME probe, and measured live/cleanup timestamps. |
| Budget | `item.output_tokens` | All linked attempts, measured usage, full reservation charges for unknown usage and qualification-probe output. |
| Cleanup | `namespace.absent`, `grant_namespace.absent`, `claims.absent`, `pods.absent`, `grant_copies.absent` | Post-teardown Kubernetes absence checks in the owned namespaces. |
| Cleanup | `pvcs.absent`, `pvs.absent` | The pre-teardown ownership inventory and post-teardown volume absence checks. |
| Cleanup | `code_host.absent`, `registry.absent`, `containers.absent`, `port_forwards.absent`, `processes.absent`, `private_dir.absent` | Registered owned-resource finalizers followed by independent absence checks. |
| Cleanup | `foreign_untouched.absent`, `finalizer.steps_ok`, `cleanup.within_bound` | Before/after foreign inventories, each finalizer result and measured cleanup timestamps. |

Only a valid-grant pass can prove successful generation-3 native recovery,
marker recall, positive real assistant usage, actual native hosted-tool/public
control results and native Bash confidentiality with the real grant. The
combined real-provider revalidation/disconnect/header run therefore remains
required. Offline results and a standalone invalid-token probe do not fill
those acceptance rows.

### Known limits

The workspace volume is the chart's default `emptyDir`, so the run creates no
PVC; the inventory still records every PVC's bound volume before deletion and
proves each one absent. The signed workspace read lives 900 s instead of the
chart's 300 s, so a Pod the controller recreates late in a pair can still stage
the checkout. Recovery is whatever the product does on its own (the kernel's
transient retry, or the owner-lost successor after a worker loss); when no
fresh session starts within 150 s, the WorkItem is cancelled and the recovery
checks record false. The restarted worker reuses its consumer name, so its
orphan sweep declares the killed incarnation's running request owner-lost as
it starts (#3076); the terminate chain tears the retained claim down, the
settlement admits the successor, and its execute wake follows on the next 2 s
reconciler pass, so the successor claims within seconds. The API's own
heartbeat path (expiry a full 24 s TTL in the past, 40 to 48 s after the kill)
is only the backstop. The rehearsal's offline turn lasts about 6 s and nothing
holds it, and the runner's fake model has no directive that keeps a turn open,
so the driver runs only the claim snapshot and the nonce write between the
boundary and the interruption; the durable history check is an ordered prefix, because a turn
that ended first appends its own record. Teardown stops the code host and
runner forwards and the object store relay before #5377's cleanup and proves
their ports closed; the stub's Pod, Service and ConfigMaps go with the owned
namespace.
