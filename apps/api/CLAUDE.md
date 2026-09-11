# CLAUDE.md - apps/api

FastAPI server: agents/versions/deployments CRUD, auth, the plugin bundle
pipeline, the GitHub git-flow engine, and the Langfuse/pod-log observability
proxies. See `../../ARCHITECTURE.md` for how this service fits between the
worker, Postgres, RustFS/S3, Langfuse, and GitHub.

## Load-bearing invariants

- **Administrative auth is one shared API key today.** `require_api_key`
  (`auth.py`) compares the `X-API-Key` header against `Settings.api_key` with
  `hmac.compare_digest`. This is explicitly MVP-only; GitHub-App identity work
  is expected to replace it eventually. The approval resolver is the recorded
  exception (ADR-0106): the platform key may mint a subject-bound operator or
  console credential but cannot resolve by itself, while Slack clicks arrive as
  approval-bound attestations signed with an independent dispatcher/API key.
  Resolver identity and channel are derived from those credentials, never the
  request body. Do not add another auth scheme without an Accepted ADR.
- **The `state` router authenticates differently, on purpose (ADR-0033, #410).**
  `require_state_access` (`routers/state.py`) accepts EITHER the platform key OR a
  scoped, path-`agent_id`-bound `state` token minted by the worker for the
  sandbox, so a sandboxed agent can rehydrate its own memory/transcript without
  holding a resolve-capable platform-wide key. Every OTHER router keeps
  `require_api_key` (platform key only); a scoped token is rejected everywhere
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
  neither authenticates as the other. `POST /channels/token` (the mint) stays
  platform-key-only, and a `chn` token is refused on every other router,
  `/approvals/{id}/resolve` included. Extending it further still takes a new ADR.
- **The GitHub webhook is authenticated differently, on purpose.** `/github/webhook`
  verifies the HMAC signature GitHub sends (`x-hub-signature-256` against
  `settings.github_webhook_secret`), not the API key -- GitHub cannot send an
  `X-API-Key` header. It lives outside the `require_api_key` dependency
  deliberately (`routers/github.py`); do not add the API-key dependency to it.
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
  still only creates a new `Deployment` row. A prod promote of an
  already-bundled sha therefore needs no access to the git remote -- which
  matters because the platform's GitHub credential can expire or be revoked
  without stopping webhook delivery (#1211). A **dev-branch** push is not
  affected by this: it always clones and always validates, redeliveries
  included, because the clone is also where the ancestry check (#1139) runs.
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
