# Identity E2E on the Docker substrate

[`identity-e2e.md`](identity-e2e.md) drives real HTTP identity turns into a
Kubernetes install. This backend drives the same turns through the product's
local tier instead: an isolated `curie local up` stack whose worker runs each
turn in a runner container on the Docker daemon
(`CURIE_SANDBOX_SUBSTRATE=docker`, `apps/worker/src/curie_worker/sandbox/docker.py`).
Nothing inside the API or the worker is faked. Use it to prove that behavior
which depends on who sent a turn holds on both substrate adapters.

The driver is [`tools/identity-e2e/identity_compose.py`](../../tools/identity-e2e/identity_compose.py),
standard library Python on the host. It reuses the Kubernetes harness's
assets, provider and scenario helpers. The scenario is in
[`tools/identity-e2e/identity_scenarios_docker.py`](../../tools/identity-e2e/identity_scenarios_docker.py).

## What you need

1. A Docker daemon you may start containers on, named by `DOCKER_HOST` or the
   current context. It may be remote: the harness never bind-mounts a local
   path. Every file a container reads arrives in a named volume through a tar
   stream on stdin.
2. `docker` with Compose v2.24 or later (the override uses `!override`),
   `openssl` and `python3`, and a `curie` binary built from the same checkout
   (set `CURIE_BIN`). A source build works; the CLI must accept the
   isolation contract in `cli/src/local.rs`.
3. Loopback reach to the ports the stack publishes: 100 ports from
   `--port-base` (default 27100), all bound to 127.0.0.1 on the Docker host.
   With a remote daemon, forward that block to this host.
4. Images: the published ones (`--image-tag`, default `latest`), candidates
   loaded under your own prefix (`--image-repo-prefix`), or candidates
   `build` makes from a source tarball.

Every action takes `--project` and `--state-dir`. The project is the compose
project and prefixes every container, volume and network the stack owns. The
state directory holds generated keys, ids, channel tokens, the compose
override files and the CLI's private config directory, with mode 0700 and 0600
files; pass an absolute path. No action prints a key, bearer, proof or token,
and a failure tail has every `*KEY=`, `*PASSWORD=`, `*SECRET=`, `*TOKEN=` and
`*CREDENTIALS=` value removed.

## Run it

```bash
H="python3 tools/identity-e2e/identity_compose.py"
T="--project ie2e --state-dir $PWD/.identity-e2e-docker"
```

1. `$H build $T --image-repo-prefix example-cand --image-tag cand1 --build-context src.tar`
   builds `example-cand/curie-api:cand1`, `curie-worker` and `curie-runner`
   from a `git archive` tarball, or from a source directory, which is tarred
   without `.git`, `cli/target`, environments, `.env` files and `*.key`
   files. It refuses a `ghcr.io/curie-eng/` prefix so a candidate never shadows
   a published tag. `up --build-context` does the same before bringing the
   stack up.
2. `$H up $T --image-repo-prefix example-cand --image-tag cand1 --agent agent-a --agent agent-b`
   (or `--api-repo`, `--worker-repo`, `--runner-repo`, `--ui-repo` per
   component; the defaults are the published repositories at `latest`):
   1. writes `compose.override.yaml` into the state directory. It remaps every
      port the base `compose.dev.yaml` publishes into the port block, pins the
      api and migrate images, rebuilds the worker's local overlay
      (`compose/worker-local.Dockerfile`) from the chosen worker image and tags
      it `<project>/curie-worker-local:<tag>`, rebinds the host-network
      worker's endpoints to 127.0.0.1 and the block, names the runner network
      `<project>_runner`, sets `CURIE_FAKE_MODEL=1` and adds
      `CURIE_CONNECTOR_CALLER_SIGNING_KEY`. It also declares two services in
      the harness's own `identity-e2e` profile: the provider and a one-shot
      seeder. The file holds no secret: the signing key and
      `CURIE_ADAPTER_CREDENTIALS` reach compose by interpolation from the
      harness's process environment.
   2. runs `curie local up --minimal` with `COMPOSE_PROJECT_NAME`,
      `COMPOSE_FILE` (base then override) and the matching endpoint
      variables, and a private `CURIE_CONFIG_DIR`, so the stack's generated
      API key and Postgres password are stored apart from any other local
      stack. Model credentials in the shell are not passed on.
   3. seeds the provider's code, roster, vectors, certificate and secrets
      into a named volume and starts `fake_idp.py` from the API image on the
      stack's default network under the alias `<project>-idp`, which its
      certificate names. Its reply sink is published on the block for the
      host-network worker and for the harness.
   4. deploys each missing `--agent` with `curie local deploy` from a copy of
      `--bundle` (default `examples/weather`) without its hosted connectors,
      unless `--keep-connectors`. The local tier would start them as
      containers, and the scenario never dials one (see Known limits).
   5. adopts the tenant's HTTP selection, binds one email address per agent
      with its reply endpoint at the provider's sink, writes a teamless
      access policy where none exists, and writes `api-runtime.json` and
      `provider-ca.pem` (root-owned, mode 0440, group 1000 as the chart's
      Secret mount is) into a second named volume.
   6. runs `curie local up --minimal` again with
      `compose.identity.yaml` added, which mounts that volume read-only at
      `/var/run/curie/http-identity` and sets `CURIE_HTTP_IDENTITY_DIR`, so
      compose recreates only the API. A later `up` (to add an agent) rewrites
      the runtime document and restarts the API. It then mints a channel
      token per binding.
3. `$H send $T --agent agent-a --sender alice@example.test --text hello`
   signs the receiver proof inside the API container and posts
   `/channels/turns`, as the Kubernetes harness does.
4. `$H observe $T --agent agent-a` waits for the newest runner container of
   the agent since its last send, decodes the `CURIE_CONNECTOR_CALLER_TOKEN`
   in that container's environment (`docker inspect`) with the signature
   withheld, prints its claims and `caller_fingerprint` (the first 16 hex of
   the token's sha256), and waits for the turn's reply events.
5. `$H prove $T --evidence handoff.json` runs `requester-handoff-docker`,
   prints the evidence and exits nonzero unless every step passed. It takes
   the same `--flag-agent`, `--control-agent`, `--sender`, `--second-sender`
   and `--wait` as the Kubernetes `prove`, plus `--expect-durable-wait`. The
   flag and control agents default to the first two `--agent`s in the order
   `up` registered them.
6. `$H down $T` force-removes the runner containers and input volumes the
   stack's worker created (labelled `curietech.ai/stack=<project>_runner`),
   runs `docker compose down -v` for the project with every profile, removes
   the runner network and every image `build` or `up` tagged (`--keep-images`
   keeps them), verifies that no container, volume or network named for the
   project or labelled for its runner network remains, and deletes the state.

A container is a claim here. The worker names runner containers
`curie-thread-<thread hash>-<nonce>` and their input volumes
`curie-input-<hex>`; neither name carries the project, so the harness finds
them by the substrate's labels: `curietech.ai/managed-by`,
`curietech.ai/component=claim-runner`, `curietech.ai/stack` (the runner
network) and `curietech.ai/agent`.

## What requester-handoff-docker proves

Two verified humans, A (`--sender`) and B (`--second-sender`), write on one
email conversation to the flag agent, and then on another to the control
agent. Every reply is picked out of the shared conversation by the event id
the ingress receipt returned for its send. The evidence keeps container
names, creation times, principals and token fingerprints, never a token. Each
step needs the one before it, so the first failed step stops the run.

1. D0 checks that the worker runs `CURIE_SANDBOX_SUBSTRATE=docker` with a
   caller signing key, the API reads its HTTP identity directory, the tenant
   has its HTTP selection and both bindings are in the receiver registration.
   It then turns the flag on for the flag agent and off for the control agent,
   each written at its current version.
2. D1: A writes on a new conversation C. A new container, created after the
   send, carries a token that names the flag agent and A's principal, and
   the reply is delivered.
3. D2: B writes on C while it is idle. B's turn runs on a new container
   created after B's send (2 s of clock skew allowed), whose token names B
   and whose fingerprint differs from A's.
4. D3: A writes on C again. The container is replaced back and names A. This
   leaves A's container warm for D4.
5. D4: A sends `[fake:progress-demo]`, which stays live for about 8 s; one
   second later B writes. Both proofs are signed before either post, so
   signing does not widen that gap. Read right after B's send, the container
   A's live turn runs on (D3's warm one, or a replacement A's new delivery
   opened) names A. A's reply is delivered and ends with `all done`,
   so A's turn was not interrupted. The worker log since A's send (read with
   `docker logs --timestamps`) contains `requester handoff pending` and no
   `interrupting before retry`. The step always reports whether the worker
   also logged `capacity wait parked event_id=<B's event> cause=requester_handoff`,
   the durable wait for the deferred delivery, and requires it with
   `--expect-durable-wait`. B's turn then runs on a new container, other than
   A's, created no earlier than A's reply (2 s of skew), whose token names B
   and whose fingerprint differs from A's, and B's reply is delivered. The step waits up to 6 minutes, or `--wait` when longer.
6. D5, the control: on the flag-off agent and a new conversation, A sends a
   warm turn whose token carries no principal, then `[fake:progress-demo]`,
   and B writes one second later. Both turns are delivered, A's reply
   includes `all done`, at most one container is created after A's send, the
   container serving the live turn carries no principal, the worker logged no
   `requester handoff pending`, and B's turn completes before A's, so
   ordinary steering folded B into A's live turn.

As in the Kubernetes scenario, D4 failing only with `deferral not exercised`
reruns D3 and D4 once on a new conversation (`D3-retry`, `D4-retry`), and D5
failing only with `steering not exercised` reruns once as `D5-retry`. A
set-aside first attempt moves to `retried` in the evidence.

## Known limits

1. No caller proxy fronts a local-tier connector, so the proxy's
   `principal=present` decision and the connector-side header are
   Kubernetes-only proofs. This scenario sends no `[fake:connector-call]`
   and reads the token the worker gave each runner container instead.
2. The runner containers' published loopback ports are ephemeral (Docker
   picks them), so they fall outside the port block. The worker dials each
   runner on its runner-network address.
3. Removing an access policy row is not possible through the API; `down`
   removes the whole database volume instead.
4. The provider's record buffer restarts with the provider, which `up`
   recreates on every run.
