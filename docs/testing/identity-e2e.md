# Identity E2E harness

`curie dev identity-e2e` drives real HTTP identity turns against a disposable
Curie install. A turn enters through `POST /channels/turns` with a signed email
receiver proof, the API looks the sender up at a TLS test identity provider,
the worker runs the turn in a sandbox, and the harness reads the connector
caller token that reached the sandbox claim. Nothing inside the API or worker
is faked. Use it to prove any behavior that depends on who sent a turn.

The driver is [`tools/identity-e2e/identity_e2e.py`](../../tools/identity-e2e/identity_e2e.py).
It is standard library Python on the host. Product calls that need signing keys
or the database run inside the release's own API pod, so they use the deployed
build. The provider is [`tools/identity-e2e/fake_idp.py`](../../tools/identity-e2e/fake_idp.py)
and the scenarios are in [`tools/identity-e2e/identity_scenarios.py`](../../tools/identity-e2e/identity_scenarios.py).

## What you need

1. A disposable Curie release on a kube context you own, installed with the
   fake model. Never use a shared or production context.
2. `kubectl`, `helm`, `openssl` and `python3` on the host, and a `curie`
   binary (set `CURIE_BIN`, or have one on `PATH`). A source build works.
3. The release's API, worker and runner images from the code under test.

Every action takes `--context`, `--namespace`, `--release` and `--state-dir`.
There is no default context. The state directory holds generated keys, ids and
channel tokens with mode 0700 and 0600 files; keep it private and pass an
absolute path. No action prints a key, bearer, proof or token.

## Bring up an install

One way, on a kind cluster, with images published for a commit or loaded with
`kind load docker-image`:

```bash
kind create cluster --name identity-e2e
curie cluster up --context kind-identity-e2e --namespace identity-e2e \
  --release identity-e2e --chart charts/curie --dev --fake-model --no-expose \
  --set security.gvisor.mode=off --set dispatcher.deploy=false \
  --set api.image.repository=<api repo> --set api.image.tag=<tag> \
  --set worker.image.repository=<worker repo> --set worker.image.tag=<tag> \
  --set ui.image.repository=<ui repo> --set ui.image.tag=<tag> \
  --set agentSandbox.runner.image=<runner repo> --set agentSandbox.runner.tag=<tag>
```

Do not use the release name `curie` on a cluster another install shares; some
chart objects are cluster scoped and named from the release.

On a single-node kind cluster, scale CoreDNS to one replica before running a
connector scenario:

```bash
kubectl --context kind-identity-e2e -n kube-system scale deploy coredns --replicas=1
```

With two replicas, a sandbox's dual-stack lookup (the A and AAAA queries sent
together from one socket) can lose one reply to a UDP conntrack race, so about
one lookup in three stalls for 3 to 9 seconds or fails. The runner's connector
dial then times out before the connector answers, and `[fake:connector-call]`
reports `failed TimeoutError`. With one replica every lookup returns at once.

## Run it

Set the target once:

```bash
T="--context kind-identity-e2e --namespace identity-e2e --release identity-e2e --state-dir $PWD/.identity-e2e"
```

1. `curie dev identity-e2e -- up $T --agent agent-a --agent agent-b --bundle examples/weather`
   first sets the connector caller key pair when it is unset and rolls the API
   to it (hosted connectors render their verify key at agent deploy time),
   then deploys any missing agent from the bundle, starts the provider, adopts the
   tenant's HTTP selection, binds one email address per agent, writes the
   API's `api-runtime.json` Secret listing those bindings in an email receiver
   registration, and runs `helm upgrade --reuse-values` to mount it and to set
   the email adapter's reply secret. It then rolls the API and worker and mints a channel token per
   binding. It is safe to run again, for example to add an agent. Use
   `examples/weather` for `requester-claim` and `requester-handoff`: its one
   hosted connector, `netpol-probe`, is what the scenarios' turns call. `--bundle` only deploys
   agents that do not exist yet, so an agent deployed earlier from a bundle
   with no hosted connector fails the scenario's preconditions.
2. `curie dev identity-e2e -- send $T --agent agent-a --sender alice@example.test --text hello`
   signs the proof over the exact body and posts the turn, as the mail adapter
   does. It prints the delivery id, conversation id and the API's answer.
   `--conversation <id>` continues a conversation.
3. `curie dev identity-e2e -- observe $T --agent agent-a` waits for the newest
   sandbox claim of that agent since its last send, decodes the
   `CURIE_CONNECTOR_CALLER_TOKEN` in the claim's token Secret, and prints its
   payload claims with `"signature": "redacted"` and a `caller_fingerprint`,
   the first 16 hex characters of the token's sha256. For an email turn it
   also waits for the worker's `turn.completed` reply event and reports the
   outcome and reply text. `--wait` bounds the wait for the token and
   `--reply-wait` (default `--wait`) the wait for the reply, which starts once
   the token is found.
4. `curie dev identity-e2e -- prove $T --scenario requester-claim --evidence out.json`
   runs a scenario, prints the evidence and exits nonzero unless every step
   passed. The flag agent is `--flag-agent` (default: the first agent `up`
   registered) and the control agent `--control-agent` (default: the second).
   `--sender` (default `alice@example.test`) is the human who writes, and
   `requester-handoff` also takes `--second-sender` (default
   `bob@example.test`), a different verified human from the roster. `--wait`
   (default 240) bounds each wait for a claim or a reply.
5. `curie dev identity-e2e -- down $T` deletes the agents and bindings `up`
   created, restores the helm values it changed, deletes the provider and
   runtime objects, removes the tenant HTTP selection row when `up` created it,
   rolls the API and worker, and verifies that nothing it labelled remains.
   Delete the cluster or namespace afterwards as usual.

## The scripted directory

The default roster in `identity_e2e.py` has, all in the reserved
`example.test` domain:

1. `alice@example.test` and `bob@example.test`, active humans in the group
   `identity-e2e-members`.
2. `dana@example.test`, an active human in no group.
3. `carol@example.test`, a disabled human.
4. `robot@example.test`, a service account.

Any other sender is unresolved. `up --roster <file>` replaces the roster; the
format is documented at the top of `fake_idp.py`. Every lookup answer is a copy
of a recorded response from `tests/vectors/http-identity-v1.json` with only the
request echo, issuer, subject, groups and observation times changed. The
provider also serves the email reply endpoint and keeps a record of lookups and
reply events, which `observe` reads.

`up --role-filter` additionally sets an issuer group invocation filter on every
agent. That makes every binding of the agent require HTTP identity, so the
worker then refuses a `curie cluster message` turn to that agent before any
claim. Leave it off unless the scenario needs filtered roles.

## What requester-claim proves

Steps 1 to 3 each send a turn whose text includes `[fake:connector-call]`.
That directive makes the fake model dial every hosted connector of the agent
through the runner's real caller header path and reply with one
`connector-call <name>: ok tools=<n> caller=<fp>` line per connector, where
`<fp>` is the fingerprint of the `X-Curie-Caller` value it sent. Each step then
requires four things. The turn completed with a delivered reply (steps 1 and
2) or the CLI returned reply text (step 3). The reply has an `ok` line for
every hosted connector, and each line's fingerprint equals the fingerprint of
the token decoded from the claim, so the dial used that token. The caller
proxy in each connector pod logged `outcome=admitted` for the agent with
`principal=present` for the flag agent's human turn (step 1) and
`principal=absent` for the control agent (step 2) and for `curie cluster
message` (step 3), and logged no refusal for that agent in the step's window.
The decoded token's claims agree: the principal is the sender in step 1 and
absent in steps 2 and 3.

## What requester-handoff proves

Two verified humans write on one email conversation to an agent with
`connector_requester_identity` on. The second human's connector call carries
the second human's principal, never the first's, and a second human who
writes while the first human's turn is live waits until that turn ends. Run
it with:

```bash
curie dev identity-e2e -- prove $T --scenario requester-handoff --evidence handoff.json
```

A is `--sender` and B is `--second-sender`; both must be verified humans in
the roster `up` installed. Every turn except the progress demos sends
`[fake:connector-call]`, so its reply carries the fingerprint of the caller
token each dial sent. The harness picks each human's reply out of the shared
conversation by the event id the API's ingress receipt returned for that
send, because one conversation carries both humans' replies. The evidence
keeps claim names, creation times, principals and token fingerprints, never a
token.

The steps run in order and each needs the one before it, so the first failed
step stops the run (the evidence records it under `aborted`):

1. H0 checks the same preconditions as `requester-claim` step 0, then turns
   the flag on for the flag agent and off for the control agent, each with
   the version it currently has.
2. H1: A writes on a new conversation C. The claim's caller token names A,
   every `ok` line in A's reply carries that token's fingerprint, and the
   caller proxy logged `outcome=admitted` with `principal=present`.
3. H2: B writes on C while it is idle. B's turn runs on a new claim, created
   after B's send (2 s of clock skew allowed), whose token fingerprint differs
   from A's. That token names B, B's reply carries its fingerprint, and the
   proxy admitted it with `principal=present`.
4. H3: A writes on C again. The claim is replaced back the same way and names
   A. This also leaves A's sandbox warm for H4.
5. H4: A sends `[fake:progress-demo]`, a turn that stays live for about 8 s.
   One second after A's send returns, B sends `[fake:connector-call]`. A's
   reply must be delivered and end with `all done`, so A's turn was not
   interrupted. The worker log since A's send must contain `requester handoff
   pending` (the kernel's deferral of B's delivery) and must not contain
   `interrupting before retry`. B's turn must then run on a new claim created
   no earlier than A's reply (2 s of skew allowed), whose token names B, and
   B's reply must carry that token's fingerprint. B is redelivered about one
   delivery lease TTL (45 s) after A's turn ends, so B's reply usually
   arrives about a minute after B's send. The step waits up to 6 minutes, or
   `--wait` when that is longer.
6. H5 is the control, on the flag-off agent and a new conversation. A first
   sends a warm `[fake:connector-call]` turn, whose token carries no
   principal, so the conversation's sandbox is booted. Then A sends
   `[fake:progress-demo]` and B writes one second later, as in H4. Both turns
   must be delivered, A's reply must include `all done`, at most one claim
   may be created after A's send (A's new delivery may replace the warm
   sandbox, but B never gets a claim of its own), and the worker must have
   logged no `requester handoff pending`. B's turn must complete before A's, which
   shows ordinary steering folded B into A's live turn.

H4 only proves something when B reaches the worker while A's turn is live. If
B arrives after A's turn has already ended, H4 fails with `deferral not
exercised`. That failure alone makes the scenario run H3 and H4 once more on
a new conversation, recorded as `H3-retry` and `H4-retry`. Any other H4
failure stops the run at once. In the same way, H5 runs once more as
`H5-retry` when it fails only with `steering not exercised`. A failed first
attempt moves from `steps` to `retried` in the evidence, so it is kept but
does not fail the run.

H4 and H5 read the worker's log with `kubectl logs --timestamps` from every
pod of the worker Deployment since the first send. For each JSON record they
search the `message` and the last line of any logged `exception`. The
deferral message has to reach the worker's log at its configured level.

## What approver-ticket-creation proves

ADR 0271 decisions 1, 2 and 4 on a live install: a selector registration is
accepted only inside the operator ceiling, the route lock holds in both
directions, and a ticket is verified once, when the approval is created. It
also covers the selector and requester-flag race from ADR 0270. Every
creation request goes through the real HTTP creation boundary,
`POST /v1/internal/identity/approvals`, backed by the API's own Postgres and
Valkey. Run it with:

```bash
curie dev identity-e2e -- prove $T --scenario approver-ticket-creation --evidence ticket.json
```

The scenario runs on `--flag-agent` (default: the first agent `up`
registered). `--sender` is the requester and `--second-sender` the selected
approver; they must differ and both must be active verified humans in the
roster. `--control-agent` (default: the other agent) supplies a Slack binding
for the ordinary legacy control. The agent needs the weather connector, so
bring the install up with `--bundle examples/weather`. `--wait` bounds only
the last step.

Setup records the original requester flag, route map, active deployments and
approver role, and refuses to start if the agent already stores a selector
registration. It then makes four changes:

1. It generates a per-run selector key pair, kept in the state directory.
2. It binds two requesting-surface routes, builds a bundle from
   `examples/weather` whose manifest gates the connector tool on the ticket
   route with an `approvalPolicy`, deploys it, and ends the original active
   deployments, because a registration needs every active version to gate the
   pair.
3. It writes the approver role of the agent's access policy, restricted to the
   roster's active humans by issuer and subject.
4. It sends real email turns from the approver and the requester. The approver
   turn gives the approver a canonical human row in the `principals` table,
   which the ticket's approver list needs. The creation steps then build their
   requests in the API pod from each requester turn's ingress capsule.

The scenario turns the requester flag off and on itself in the early steps.

The steps run in this order, and each records its own evidence:

1. `registration-refused-flag-off`: with the flag off, a registration is refused with 422 and changes nothing.
2. `registration-bundle-gate-refused`: a registration whose serving bundle gates elsewhere, or not at all, is refused with 422.
3. `registration-accepted`: with the flag on, a registration is stored, pinned to the approver role version and versioned by one.
4. `flag-disable-refused-while-registered`: the flag cannot be turned off while a selector is registered.
5. `concurrent-flag-and-registration`: in three rounds, a flag disable and a registration at one version race; exactly one commits.
6. `deploy-breaking-gate-refused`: deploying a version whose bundle moves the registered gate is refused.
7. `creation-valid`: a valid ticket creates one pending dynamic approval.
8. `creation-bad-signature`: another key, a tampered signature or an unregistered key id is refused.
9. `creation-scope-change`: a ticket for another tenant, agent, connector, tool or route is refused.
10. `creation-requester-change`: a ticket naming a human other than the turn's verified requester is refused.
11. `creation-argument-change`: arguments changed after signing are refused.
12. `creation-expired`: an expired or not yet valid preparation is refused.
13. `creation-nonce-replay`: the spent nonce is refused under a second dedupe key, and a fresh nonce then creates.
14. `creation-conflicting-dedupe`: an identical retry returns the original receipt and a conflicting one is refused.
15. `creation-identical-race`: four identical creations at once leave one row and one spent nonce.
16. `legacy-new-policy-refused`: `POST /approvals` cannot create on the registered pair, even with a valid ticket.
17. `legacy-ordinary-unchanged`: ordinary legacy creation is refused on an HTTP-selected binding and still created and idempotent on a legacy one.
18. `route-bypass-refused`: the registered pair cannot be approved off its owning route or without its ticket; an unrelated tool still can.
19. `cluster-message-negative-control`: `curie cluster message` to the agent is refused and creates no approval.

The refusal steps also check that no approval row was stored and no nonce was
spent.

Carried: the ADR 0271 item 2 check of a direct execution call without the
verified-grant header needs the connector proxy header from slice 0271c, so
this scenario does not run it. The evidence lists it under `carried`.

Restore runs in a `finally` block even after a failure, and a failed restore
fails the run. It removes the registration with a versioned route write, puts
the flag back, ends the run's deployments and recreates the original ones,
writes the original route map back, and then checks all four. It restores the
approver role last. A role that existed returns to its original filter. A role
that was absent is removed directly in the API pod's database, under the role
policy's lock order, with a version bump and an audit event, because the
role-policy API cannot delete a role and a `filter: null` role would read as
unrestricted instead of not configured. A configuration stored as SQL NULL goes
back to NULL. The run's agent versions stay, undeployed, and approvals it
created stay pending until they expire. The evidence keeps statuses, refusal
codes, row counts and key ids, never tickets, keys, capsules or tokens.

## Add a scenario

1. Write a function in `identity_scenarios.py` and register it with
   `@scenario("your-name")`. It receives the harness, the parsed `prove`
   arguments and an `Evidence` recorder.
2. Put each claim in its own `with evidence.step("<id>", "<what it proves>") as seen:`
   block. Record what you observed in `seen` and assert with `check(...)`. A
   failed step is recorded and the next step still runs. When later steps
   build on a step, call `require_passed(evidence)` after it to stop the run.
3. Drive the install only through `harness.send`, `harness.observe`,
   `harness.api()`, `harness.target.curie(...)` and `harness.records()`, and
   read connector evidence with `harness.connectors(...)` and
   `harness.proxy_decisions(...)` and worker messages with
   `harness.worker_log(...)`. Use
   `harness.target.sql(...)` only to corrupt state that no product surface can
   write, and restore it in a `finally` block.
4. If the scenario needs other humans, add them to the default roster or ship a
   roster file, and add any new agent with `up --agent`.
5. Add a unit test in `tools/identity-e2e/tests` for any new helper, judged by
   the product code that consumes its output.

## Known limits

1. The chart renders no NetworkPolicy for API egress, so the lookup needs no
   extra rule. The worker must be able to reach the provider's reply port; on
   a cluster that enforces the worker's egress policy, allow it or expect
   reply delivery to fail.
2. `up` refuses a release whose `worker.adapterCredentialsExistingSecret` is
   set, because it cannot add the reply secret to a Secret it does not own.
3. Removing an access policy row is not possible through the API, so `down`
   reports a teamless policy row it wrote on an agent it did not create.
4. The `curie` CLI swaps in a key stored for a local compose stack when it is
   given the dev default key over a loopback port-forward. The harness runs
   `curie` with a private `CURIE_CONFIG_DIR` under the state directory to
   avoid that.
