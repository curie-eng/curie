# CLAUDE.md - apps/api

FastAPI server: agents/versions/deployments CRUD, auth, the plugin bundle
pipeline, the GitHub git-flow engine, and the Langfuse/pod-log observability
proxies. See `../../ARCHITECTURE.md` for how this service fits between the
worker, Postgres, RustFS/S3, Langfuse, and GitHub.

## Load-bearing invariants

- **Administrative auth is the platform key or a live console session (ADR-0083).** `require_api_key`
  (`auth.py`) follows ADR-0083 order: the platform key is checked first via
  `verify_platform_key` with no database read, then a live
  `__Host-curie_console_session` cookie through `live_console_session`.
  `require_platform_key` never accepts a session. This is explicitly MVP-only;
  GitHub-App identity work is expected to replace it eventually. The approval
  resolver is the recorded exception (ADR-0106): the platform key may mint a
  subject-bound operator or console credential but cannot resolve by itself,
  while Slack clicks arrive as approval-bound attestations signed with an
  independent dispatcher/API key. Resolver identity and channel are derived
  from those credentials, never the request body. Do not add another auth
  scheme without an Accepted ADR.
- **The `state` router authenticates differently, on purpose (ADR-0033, #410).**
  `require_state_access` (`routers/state.py`) accepts EITHER the platform key OR a
  scoped, path-`agent_id`-bound `state` token minted by the worker for the
  sandbox, so a sandboxed agent can rehydrate its own memory/transcript without
  holding a resolve-capable platform-wide key. A console session is not accepted
  there, and a state token is still rejected by `require_api_key`. Every OTHER
  router keeps `require_api_key`; a scoped token is rejected everywhere
  else, including `/approvals/{id}/resolve`. Do not collapse the state router
  back onto `require_api_key`, and do not extend scoped-token acceptance to
  another router without a new ADR.
- **The `channels` ingress router is the SECOND such exception (ADR-0096
  decision 3, #1459).** `POST /channels/turns` accepts EITHER the platform key
  OR a `chn` token (`channel_token.py`) scoped to the binding row named in the
  request BODY -- its `channel_id` plus that row's current `generation` -- so an
  ingress adapter can enqueue turns for its own route without holding the
  platform key. `POST /channels/token` bumps that generation on every mint so a
  remint revokes the previous token (#2379). It is a SIBLING credential, never a
  widening of the sandbox token: the two verify against different modules and
  neither authenticates as the other. A `chn` token never mints, and it is
  refused on every other router, `/approvals/{id}/resolve` included. Extending
  it further still takes a new ADR.
- **A channel adapter is a principal (ADR-0154, #2806), the THIRD exception.**
  An `adp` credential (`adapter_principal.py`, header
  `X-Curie-Adapter-Principal`), issued by the platform key at
  `POST /approvals/principals/adapter` and self-rotated at `.../rotate`, is
  accepted by exactly three routes, each checking its binding set:
  `POST /channels/token` (only for a binding it serves; else 403),
  `GET /approvals` (filtered to approvals it serves) and
  `/approvals/{id}/resolve` (with `X-Curie-Approval-Actor`; unserved is 404).
  Served is ONE predicate, `crud.approvals._approval_served`, for both. Presenting it
  with the platform key or another resolver credential is 401, never a
  precedence choice. The resolver kinds are `chat`, `console`, `operator` and
  `adapter`; `operator` resolves only explicit-user routes, and no Slack approver
  set admits `adapter`, since only the dispatcher vouches for a Slack ID
  (ADR-0177's separate finding).
- **Who may start a turn is decided in ONE function (ADR 0175, #3241).**
  `admission.admit` answers for a binding's optional `allowed_callers` list, and
  both entry points call it: `POST /channels/turns` after token verification and
  before any claim (403 with `detail: caller_not_allowed`, frozen in
  `tests/vectors/channel-port-refusal.json`), and the platform-key-only
  `POST /channels/admission` the dispatcher asks. Do not compare caller ids
  anywhere else. The list is written only by
  `PUT /agents/{agent_id}/channels/callers`, which does NOT bump the binding
  generation, so editing it never revokes an adapter's `chn` token.
- **The GitHub webhook is authenticated differently, on purpose.** `/github/webhook`
  verifies the HMAC signature GitHub sends (`x-hub-signature-256` against
  `settings.github_webhook_secret`), not the API key -- GitHub cannot send an
  `X-API-Key` header. It lives outside the `require_api_key` dependency
  deliberately (`routers/github.py`); do not add the API-key dependency to it.
- **Publication comparison is a scoped credential exception (ADR 0174).**
  `POST /publications/precheck` accepts only the API issued `ppc` capability
  in `X-Curie-Publication-Precheck`. Its sole scope is `publication.precheck`.
  The worker mints it through `POST /v1/internal/publications/precheck/context`
  using its internal worker credential. Mint and comparison verify the running
  WorkItem request, runtime epoch, lease, deadline and lineage before and after
  the provider read. Comparison reads one current pull request and writes only
  its shared Valkey attempt budget. It cannot create publication or approval
  state or redeem repository credentials. State and other scoped credentials
  do not authorize comparison, and `ppc` authorizes no other route.
- **The factory issue read is a scoped credential exception (ADR 0187).**
  `POST /work-items/issue-read` accepts only the API issued `wir` capability
  in `X-Curie-Issue-Read`. Its sole scope is `work_item.issue_read`, and it
  names one execution request and its WorkItem's repository and issue. The
  worker mints it at sandbox boot through
  `POST /v1/internal/work-items/issue-read/context` using its internal worker
  credential. The read refuses any other repository or issue, re-checks that
  the request is still running before its deadline and that its WorkItem is
  not cancelled, then reads the issue and its comments with a freshly minted
  App installation token and returns them verbatim. It stores nothing.
  State and other scoped credentials do not authorize it, and `wir`
  authorizes no other route.
- **Channel read is a scoped credential exception (ADR 0100, #2877).**
  `POST /channel-read` accepts only the API issued `chr` capability in
  `X-Curie-Channel-Read`. Its sole audience is `channel.read`, and it names
  one agent, deployment, grant digest, logical turn and generation. The worker
  mints it through `POST /v1/internal/channel-read/context` using its internal
  worker credential. The active ledger key is a 90 second lease that the worker
  renews every 30 seconds with an owner checked refresh; the worker ends the
  turn with an owner tombstone (a late committed open is refused) and an owner
  checked revoke in the shared Valkey ledger
  (`curie_internal.channel_read_ledger`), so revocation holds while the API is
  down and a dead worker's capability lapses within one lease. Every read
  repeats the generation, the grant (read locally from the agent's active
  stored bundle manifest, cached by digest), the binding and provider membership.
  Authorization is surface neutral; a kind is readable only through a
  `channel_read/readers.py` reader that advertises `history-read`, and Slack
  reads live only in `channel_read/slack_reads.py`. A turn may spend
  8 pages, a window is at most 7 days, and `limit` defaults to 50 with a 100
  maximum. Refusals are named `channel_read.*` codes; a 429 carries
  `Retry-After`. It stores no message body. State, issue read
  and other scoped credentials do not authorize it, and `chr` authorizes only
  `/channel-read` and its canvas sibling `/channel-canvas` (ADR 0200). The
  history read also requires the `channelRead` grant carried in the claims, so
  a canvas only bundle never reads history. `/channel-canvas` repeats the same
  generation and digest checks, requires the operation's own grant carried in
  the claims (`canvasList`, `canvasRead`, `canvasEdit`), checks that the canvas
  is shared into one of the agent's bindings with the bot a member before any
  download or edit, charges one page per successful operation, and edits only
  a table cell a read recorded in the same logical turn
  (`channel_read/canvas_sections.py`). Slack canvas calls live only in
  `channel_read/slack_canvas.py`. Each edit is written to
  `channel_canvas_edits` (attempted, then applied or failed; an unconfirmed
  outcome stays attempted) before and after the Slack call. Retrieved canvas
  content is not stored.
- **Git-flow never calls the GitHub API.** `gitflow.py` builds the bundle by
  archiving the pushed sha directly from the repo over the git protocol (bare
  repos in tests, the real remote in production). This keeps the flow
  independent of GitHub API rate limits and scopes. Do not introduce a GitHub
  API client into `gitflow.py` for something the git protocol already gives
  you.
- **The webhook's HMAC signature authenticates the sender, not the payload's
  clone URL.** A valid signature proves the sender holds the shared secret,
  exactly the attacker the threat model assumes, so `clone_and_archive`
  (`gitflow.py`) never hands git `repository.clone_url` from the payload.
  It hands git `trusted_clone_url`, derived from `Settings.github_clone_base`
  plus the `repo_full_name` read from the database row; the payload URL is
  only compared against that derived one, to reject and log a forged push
  (`CloneOriginMismatch`), before being discarded. `repo_full_name` is a
  required keyword-only parameter for this reason and must not get a default.
  The clone also pins `http.followRedirects=false`, since git's default
  follows a redirect on the very request carrying the auth header.
- **Prod push reuses the dev-built bundle; it does not rebuild.** Before any
  clone, a **prod-branch** push looks up a stored bundle for that sha across
  every agent bound to the repository (ADR-0091), fetches its bytes from the
  object store, and reads `deploy.yaml` out of them to route the push; it
  creates the target agent's own `Version` row when needed, pointing to the
  same stored object, and creates a new `Deployment` row. A prod promote of an
  already-bundled sha therefore needs no access to the git remote -- which
  matters because the platform's GitHub credential can expire or be revoked
  without stopping webhook delivery (#1211). A **dev-branch** push is not
  affected by this: it always clones and always validates, including a new
  delivery (a new `X-GitHub-Delivery` id) of the same commit, because the
  clone is also where the ancestry check (#1139) runs. A webhook delivery whose
  id already deployed or promoted answers `push_duplicate` and does nothing
  (#3820); polling has no delivery id and is unaffected.
  If you find yourself rebuilding on promote, that is a bug, not a feature --
  promotion is meant to be "the exact artifact that passed on dev," not a
  fresh build.
- **The plugin bundle validator (`plugin_format.validate_bundle`) is the only
  gate a bundle passes through**, whether it arrives via the CLI's
  `curie local deploy` / `curie cluster deploy`, the UI's create-agent
  modal, or a git push. Do not
  duplicate validation logic in a new entry point; route through
  `bundles.py`.
- **The declared/bound approval-route join has exactly one home
  (`apps/api/src/curie_api/deploy.py`'s `check_approval_route_bindings` family,
  #2436).** It is called in front of every deployment-creation site, every
  bundle attachment onto a version an active deployment already references,
  and the `approval_routes` write. It checks presence of a binding, not its
  validity -- the worker keeps the validity backstop. Do not duplicate this
  check per entry point.
- **Observability endpoints are read-only proxies, not new stores.** The
  `/observability/metrics/*` endpoints compute aggregates from Langfuse's
  public API (`metrics.py` + `langfuse.py`); the runner-pod-log endpoint
  proxies the K8s pod-logs API (`k8s.py`) for the sandbox that served a given
  trace. Neither should grow a local cache or its own persistence -- Langfuse
  and the cluster are the source of truth.
- **The `/observability/runners/.../logs` endpoint has three distinct error
  states by design**: 503 when no kubeconfig is configured, 404 when the pod
  is gone, 502 for any other cluster error. The UI renders each differently
  (`apps/ui/CLAUDE.md`); do not collapse these into a single error shape.

## Migrations: never develop against the shared compose DB

**Alembic migrations must be developed and tested against a disposable
database (or schema), never the shared `compose.dev.yaml` Postgres.** That
Postgres is shared state every other lane's test suite reads on setup;
stamping it with an unmerged revision breaks everyone else's `alembic upgrade
head` (`Can't locate revision ...`) until your branch merges or you unstamp
it. This has already bitten concurrent work in this repo more than once. Spin
up a scratch Postgres (a second compose service, a throwaway container, or a
fresh schema) for migration development; only run migrations against the shared
compose DB once your revision is merged to main.

## Config surface

`Settings` (`config.py`, env-driven) covers the Postgres DSN, the bundle
store (RustFS/S3 endpoint + bucket), the Langfuse public API base + keys, the
GitHub webhook secret, `api_key`, `kube_config_path` (empty = no cluster
configured, the 503 case above), and `metrics_default_window_hours`.

## Verify

```bash
cd apps/api && uv run alembic upgrade head   # apply schema once against compose.dev.yaml
uv run pytest apps/api/tests -q               # from repo root; needs the dev stack up
uv run python -m curie_api.export_openapi   # regenerate committed openapi.json; check for drift
```

`test_openapi_drift.py` fails if the committed OpenAPI spec is stale --
regenerate it after any router signature change, don't hand-edit the JSON.
Integration tests (`test_gitflow_integration.py`, `test_langfuse_integration.py`,
`test_metrics_integration.py`) run against the real Postgres/RustFS from
`compose.dev.yaml`; only Langfuse's own API responses and the GitHub webhook
sender are faked.
