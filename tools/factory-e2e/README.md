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

The commands below use placeholder values. Run them from the repository root.

```sh
P="python3 tools/factory-e2e/personal_profile.py"
COMMON="--candidate $SHA --context kind-acme-scratch --namespace ppa-acme-test \
  --release ppa-acme --registry 127.0.0.1:5999 --docker-context acme-builder \
  --kubeconfig $KUBECONFIG_FILE --app-dir $APP_DIR --grant-file $GRANT_FILE \
  --grant-expires-at 2026-11-11T09:30:00Z --state-dir $STATE_DIR --run-id acme-run-1"
CEILING="sessions=8,turns=96,session-turns=32,output-tokens=32768,session-output-tokens=8192,session-seconds=300,live-seconds=2400,cleanup-seconds=300,paid-retries=0"

$P images $COMMON
$P preflight $COMMON --approve-live "$CEILING"
$P forge-probe $COMMON
$P run $COMMON --approve-live "$CEILING"
$P judge --criterion all --candidate $SHA --state-dir $STATE_DIR --run-id acme-run-1
```

1. `images` is unpaid. It builds `curie-api`, `curie-worker`,
   `curie-dispatcher` and `curie-runner` with the label
   `org.opencontainers.image.revision=<candidate>`, pushes them as
   `<name>:<candidate12>`, then runs `curie build` on a private copy of
   `examples/dark-factory` whose manifest routes `mcp__curie__publish_changes`
   to the `publication` approval route. The layer inherits the revision label
   from the runner it is built on.
2. `preflight` is unpaid. It refuses before any model session and before any
   cluster mutation.
3. `forge-probe` mints an installation token limited to contents, pull
   requests and issues (write) and checks and statuses (read), creates a
   run-owned branch with an empty commit, opens and closes a pull request,
   deletes the branch, and proves both are gone. It touches no other ref.
4. `run` performs exactly one live attempt. A second `run` with the same run
   ID refuses with `paid_retry_refused`; there is no retry flag.
5. `judge --criterion {roles,approval,pod-restart,publication,budget,cleanup,all}`
   reads the ledger only and prints
   `{"criterion", "verdict", "checks", "counts"}` with booleans and counts and
   no identifiers. A missing observation fails its check.

Exit codes: 0 pass, 1 failed run or failed judge, 2 usage error or refusal.
A refusal prints `{"error": <code>, "fix": <text>}` naming variables, never
values.

## Rehearsing the unpaid phases

`rehearse` runs the same `run` phases up to and including `forge`
(refusals, namespace, images, install, setup and forge) against the real
cluster, then the normal finalizer and inventory. It never loads a grant,
writes a live start or reserves a session, so the single paid `run` is not
the first execution of the install, setup, forge and cleanup machinery.

```sh
$P rehearse --candidate $SHA --context kind-acme-scratch --namespace ppa-acme-rehearsal \
  --release ppa-acme-r --registry 127.0.0.1:5999 --docker-context acme-builder \
  --kubeconfig $KUBECONFIG_FILE --app-dir $APP_DIR --state-dir $STATE_DIR \
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

## What each criterion observes

1. `roles`: the first attempt's native transcripts show an implementer session
   and plan reviewer and diff reviewer subagent sessions, each with
   non-synthetic assistant turns, on the configured models; no synthetic
   assistant record in either attempt; at least one `mcp__curie__` tool call;
   the bundle's review gate state files (the hook ran); at most one native
   CLI process at a time; the running images are the pushed candidate digests;
   no service credential in the deployed settings; and the owner tuple
   (tenant, mode `personal`, principal, generation 1) from both
   `GET /work-items/{id}` and `curie cluster work-items <id> --json`, and on
   every execution request.
2. `approval`: the run parked on a publication approval; the unlisted
   operator's resolve returned 403, its audit row is `denied` and
   unauthorized, the approval stayed pending and nothing moved (work item,
   requests, publication, remote branches, owner); the three subjects
   differ; the allowed operator's reject and approve both carry operator audit
   rows; the approved attempt is a fresh approval; the second admission kept
   the work item and owner; and both runner pods read the same grant Secret,
   whose uid and resourceVersion never changed.
3. `pod-restart`: see the next section.
4. `publication`: the rejected attempt left no branch and no pull request;
   exactly one ready pull request exists for the run's branch prefix; the
   publication succeeded and the work item reads `published`; the diff
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
scanned in memory, and only booleans and counts are kept.

Each resource's cleanup is registered before the resource is created and runs
in reverse order on success, failure, rejection and on SIGINT or SIGTERM.
Transcripts are parsed in memory; the ledger keeps only sanitized per-session
facts, never a raw transcript.
