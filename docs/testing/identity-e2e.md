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
and the scenarios are in [`tools/identity-e2e/identity_scenarios.py`](../../tools/identity-e2e/identity_scenarios.py),
[`tools/identity-e2e/identity_scenarios_requester.py`](../../tools/identity-e2e/identity_scenarios_requester.py)
and [`tools/identity-e2e/identity_scenarios_attestation.py`](../../tools/identity-e2e/identity_scenarios_attestation.py).
The same scenarios on the Docker substrate, against an isolated `curie local up`
stack, are in [Identity E2E on Docker](identity-e2e-docker.md), except
`grant-attestation`, which needs the Kubernetes caller proxy.

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
   `up --reference-connector` is what `requester-effect` needs, and works for
   every scenario; see [The reference connector](#the-reference-connector).
   With no `--flag-agent` or `--control-agent`, `prove` picks the first and
   second agents in the order `up` registered them.
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
   `requester-handoff`, `requester-expiry` and `requester-approval` also take
   `--second-sender` (default `bob@example.test`), a different verified human
   from the roster. `requester-effect` takes `--outsider` (default
   `dana@example.test`), a verified human outside the authority group.
   `--wait` (default 240) bounds each wait for a claim or a reply.
5. `curie dev identity-e2e -- down $T` deletes the agents and bindings `up`
   created, restores the helm values it changed, deletes the provider and
   runtime objects, removes the tenant HTTP selection row when `up` created it,
   rolls the API and worker, deletes the rendered reference bundle, and
   verifies that nothing it labelled remains. Delete the cluster or namespace
   afterwards as usual.

## The reference connector

`up --reference-connector --bundle examples/weather` renders the bundle into
`reference-bundle/` under the state directory with one more hosted connector,
`human-restricted` ([`human_restricted_connector.py`](../../tools/identity-e2e/human_restricted_connector.py)),
and deploys every missing agent from that rendered copy. Its tool
`record_effect` reads the `X-Curie-Principal` its caller proxy set before each
effect and asks the provider whether that exact tenant, issuer and subject may
act now (ADR 0270 decisions 4 and 5). It answers `effect=<n> requester=<fp>` or
`refused <code>`, and logs one line per call, `<tool> outcome=<ok|code>
requester=<fp or -> grant=<fp or ->`, where `<fp>` is the first 16 hex
characters of the sha256 of the header value. It never logs the principal. Its
two protected execution tools, `execute_attested` and `execute_approved`, also
need the proxy's `X-Curie-Grant-Verified` attestation, and the second a valid
approval ticket; [What grant-attestation proves](#what-grant-attestation-proves)
covers them.

The provider then also serves that current authority on its sink listener at
`http://<release>-identity-e2e-idp.<namespace>.svc:8080/authority`. `up`
generates the authority bearer and keeps it in the private state file; the
rendered bundle carries it in the connector's environment, so the rendered
tree is mode 0700 and 0600 as well. The authority allows a verified, active
human of the agents' tenant and the roster issuer who is in the group
`identity-e2e-members`, and denies anyone else. The tenant is known only once
the agents exist, so a first `up` restarts the provider with it after
deploying them. The scenarios change the authority through the provider's
admin endpoints (`/admin/membership`, `/admin/authority`, guarded by the
adapter secret). A membership change also changes what the provider's lookup
answers for that sender, so run `requester-effect` on an `up` without
`--role-filter`.

`--bundle` deploys only missing agents. An agent deployed earlier without the
reference connector fails `requester-effect`'s preconditions; delete it or
choose new agent names. The flag is remembered in the state, so a later `up`
keeps serving the authority.

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
   B sends `[fake:connector-call]` one second after A's send. Both proofs are
   signed first and both posts share one port-forward, so the gap stays near
   one second. A's reply must be delivered and end with `all done`, so A's
   turn was not interrupted. The worker log since A's send must contain
   `requester handoff pending` (the kernel's deferral of B's delivery) and
   `capacity wait parked event_id=<B's event id> cause=requester_handoff` (the
   consumer parked B's delivery in a durable wait), and must not contain
   `interrupting before retry`. B's turn must then run on a new claim created
   no earlier than A's reply (2 s of skew allowed), whose token names B, and
   B's reply must carry that token's fingerprint. The parked wait's due wake
   runs B a few seconds after A's turn ends, with no delivery lease
   redelivery in between. The step waits up to 6 minutes, or `--wait` when
   that is longer.
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

## What requester-effect proves

ADR 0270 acceptance item 5, decisions 4 and 5: a human-restricted effect
happens only for the human the caller proxy attests, and only while the
connector's current downstream authority allows it. It needs
`up --reference-connector`, and no `--role-filter`:

```bash
curie dev identity-e2e -- prove $T --scenario requester-effect --evidence effect.json
```

Every step sends one email turn whose text carries
`[fake:connector-tool human-restricted record_effect {"note":"..."}]`. The fake
model calls that one tool through the runner's real caller header path and
replies `connector-tool human-restricted/record_effect: ok effect=<n>
requester=<fp> caller=<fp>` or `... refused <code> caller=<fp>`. Each step
requires that the claim's token names the expected human (or no one), that the
turn was delivered, that the reply has exactly one such line with the expected
outcome and the claim's token fingerprint, that the connector logged exactly
one outcome line for the call with the same outcome and the fingerprint of the
expected human's `X-Curie-Principal`, and that the caller proxy admitted the
agent with `principal=present` (or `absent`). A refusal therefore applied no
effect. Effects are counted from the connector's own log, read with `kubectl
logs` from its server container, never the proxy's.

1. E0: preconditions. Both agents have the HTTP selection, a registered
   binding and the `human-restricted` connector behind a caller proxy; the
   flag is on for the flag agent and off for the control; `--sender` is in
   `identity-e2e-members` and `--outsider` is not; the provider's authority is
   reset to available with `--sender` a member.
2. E1: `--sender` (alice) gets `ok`, and the effect names alice's fingerprint.
3. E2: `--outsider` (dana, a verified human in no group) is refused
   `not_authorized`.
4. E3: dana's tool arguments name alice's subject, issuer and tenant; still
   `not_authorized`, and the connector logged dana's fingerprint.
5. E4: dana's message also carries `[fake:forge-principal <alice's header>]`,
   so the runner sends a forged `X-Curie-Principal` on that dial. The proxy
   drops it and sets dana's; refused `not_authorized` with dana's fingerprint.
6. E5: the provider revokes alice's membership; alice is refused
   `not_authorized`. The membership is restored in a `finally`, and alice's
   next call is `ok` again.
7. E6: the provider's authority is made unavailable; alice is refused
   `authority_unavailable`. It is restored in a `finally` and checked to allow
   alice again.
8. E7: alice on the flag-off control agent: the token names no one, the proxy
   sets no header, and the connector refuses `missing_requester`.
9. E8: a caller around the caller proxy. From inside the flag agent's
   connector server container, the harness dials the server on the pod's own
   address, as a caller on the connector's `-direct` Service reaches it, with
   alice's valid `X-Curie-Principal` (sent on stdin, never logged). The request
   does not arrive on the proxy's loopback forward, so the connector refuses
   `missing_requester` and logs no requester and no effect.

## What requester-expiry proves

ADR 0270 decision 3 and acceptance items 3 and 4 on the flag agent: a copied
requester token stays usable until its signed expiry, even after a handoff, and
is refused after it; a warm sandbox whose caller token is too short for the
next turn is replaced; and a thread idle past the hour gets a fresh token.

```bash
curie dev identity-e2e -- prove $T --scenario requester-expiry --evidence expiry.json
```

This scenario waits for real time. X6 waits until the C1 token's expiry plus 5
seconds, at most an hour after X1, and prints the time left on stderr every
few minutes. Plan on about 70 minutes. The captured token stays in the
harness's memory only; the evidence keeps fingerprints and `exp` values.

1. X1: A (`--sender`) writes on a new conversation C3, then on a new
   conversation C1, each with `[fake:connector-call]`; each claim's token names
   A. C3 comes first so its token expires no later than C1's. C1's token is
   read from its claim's token Secret and held in memory.
2. X0: the worker logged `requester turn on thread ... bounded to <n>s by the
   requester token window (configured bound <m>s)` for those turns, with every
   `<n>` under 3600 and under `<m>`: no opted-in turn may run past its token.
   It runs after X1 and does not stop the scenario.
3. X2: B (`--second-sender`) writes on C1; the claim is replaced and names B.
4. X3: from inside a `human-restricted` (or the first) connector pod's
   caller-proxy container, the harness posts an MCP `initialize` with A's old
   C1 token to the live proxy on loopback. It is admitted, and the proxy logged
   `outcome=admitted principal=present` for it: the accepted residual window
   after a handoff.
5. X4: in the same container the harness builds the deployed proxy from that
   container's own environment (`ProxyConfig.from_env`, `make_app` with a
   frozen clock) on a free loopback port and sends the same request at the
   token's `exp - 1` (admitted) and at `exp` (403 with the frozen `expired`
   refusal body).
6. X5: A writes on a new conversation C2; then the harness rewrites the C2
   route record's `caller_token_exp` to two minutes from now (see the route
   doctoring exception below) and A writes on C2 again. The turn runs on a new
   claim, created after the send, whose token names A with a later `exp` than
   the warm one. The original value is put back in a `finally` only while the
   record still holds the doctored one, so a route the worker replaced is
   never touched.
7. X6: once C1's token expired, the same replay to the live proxy is refused
   `expired` (403, frozen body) and the proxy logged `outcome=expired`. Then A
   writes on C3 again, idle since X1: a new claim whose token names A and
   expires after the send and after C3's first token.

The token is passed to the in-pod script on stdin, never on argv, and the
script prints only the status, the refusal reason and whether the body is the
frozen one.

## What requester-approval proves

ADR 0270 decision 3's approval resume: an approval one human requested and
another answered resumes on a fresh claim whose token still names the
requester, never the approver.

```bash
curie dev identity-e2e -- prove $T --scenario requester-approval --evidence approval.json
```

1. P0: the flag agent is opted in. The harness binds the approval route
   `identity-e2e-approve` through `PATCH /agents/<id>` with
   `{"resolution": {"mode": "requesting_surface"}, "approvers": {"emails":
   ["<second sender>"]}}`, so the card stays in the asking email thread and
   only that address may answer it (ADR 0177 amendment). It also sets the
   agent's approver role filter to the issuer group `identity-e2e-members`
   through the API's role policy owner in the API pod, so the HTTP approver
   role (ADR 0267) evaluates the answering human on fresh provider facts. It
   reads the stored routes first so it can put them back exactly.
2. P1: A sends `[fake:request-approval:identity-e2e-approve] ...
   [fake:connector-call]`. The paused turn's token names A; the API lists a
   pending approval on that route and conversation whose card channel is the
   reply channel; and the provider's reply sink recorded the card, a
   `reply.post` carrying a `confirm` interaction for that approval id.
3. P2: the harness mints an adapter credential for the flag agent's email
   binding (`POST /approvals/principals/adapter`), signs an email approval
   answer proof for B's `{"decision":"approved"}` inside the API pod with the
   receiver key (as `send` signs a receiver proof), and posts it to
   `/approvals/<id>/resolve` as the mail adapter does. The answer must be 200
   with the approval approved by B, and the audit trail must record B's
   authorized answer by `HttpApprovalAnswer`.
4. P3: the resume runs on a claim created after the answer, different from the
   paused turn's, whose token fingerprint differs and whose principal is A,
   never B. The resume text quotes the approval summary, which carries
   `[fake:connector-call]`, so the resumed turn also dials every connector:
   each `ok` line carries the resumed token's fingerprint and every proxy
   admitted it with `principal=present`.
5. P4: restores the routes and the approver role filter. The role policy has
   no removal, so on an agent that had no approver role before, the role stays
   with no filter, which the answer path reads as unrestricted. Run it on
   agents `up` created, which `down` deletes.

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
verified-grant header needs the connector proxy header, so this scenario does
not run it. The evidence lists it under `carried`, and `grant-attestation` G1
runs it.

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
codes, row counts and key ids, never tickets, keys, capsules or tokens. Each
email turn appears as its event and stream ids, its sender's address and, of
its capsule, whether it carries a principal, the scope's agent id and the
first 16 hex characters of the sha256 of its canonical principal. The
approver's `principals` rows appear as a count, whether one is an active
human, and their types, and the roster's active humans as a count.

## What grant-attestation proves

ADR 0271 decision 7, acceptance item 5 and the carried part of item 2: through
the real worker, caller proxy and reference connector, with the two protected
execution tools held for approval by the runner but absent from the proxy's
gated list, a grant-less call has no effect; the grant the worker mints for an
approved resume, sent with the resumed sandbox's own boot credentials, rides
unspent on unrelated calls, is spent once by the approved call and is attested
to the connector; and replay, forged attestation headers, changed arguments,
batches and tampered tickets refuse, with an uncertain execution never resent.
It needs `up --reference-connector`, two verified humans in
`identity-e2e-members` (`--sender` writes, `--second-sender` approves) and the
worker's connector reconciler off, which is the chart default:

```bash
curie dev identity-e2e -- prove $T --scenario grant-attestation --evidence attestation.json
```

The caller proxy exists only on Kubernetes, so there is no Docker variant, and
the scenario refuses to start on the Docker harness. Every email turn's text
carries `grant-attestation <step> <nonce>` with a nonce for the run. Each
email step reads the proxy's and the connector's logs only from its own send
(2 s of clock skew allowed), and each probe only from its own pod-clock window.
Spends are read from Valkey before and after each step, from inside the worker
container with the worker's own client, as the first 16 hex characters of the
sha256 of each `connector-grant:<jti>` key's `jti`: the same fingerprint the
connector logs as `grant=`.

1. G0: the worker Deployment does not set `CURIE_CONNECTOR_RECONCILE` true, so
   nothing renders the connector again during the run; the release's caller key
   pair is the one `up` set; the flag is turned on and the approver role is set
   to `identity-e2e-members` as in `requester-approval` P0. The harness reads
   the agent's stored `approval_required_tools` with SQL, then adds
   `mcp__human-restricted__execute_attested` and
   `mcp__human-restricted__execute_approved` through `PATCH /agents/<id>`.
   The runner reads that row at every claim, while the caller proxy keeps the
   gated list rendered when the agent was deployed. It generates an Ed25519
   selector key pair for the run, keeps the seed in the private state, sets the
   connector server container's `TICKET_PUBLIC_KEY`, `TICKET_KEY_ID`
   (`identity-e2e-selector`), `TICKET_ROUTE` (`identity-e2e-ticket`) and
   `TICKET_AGENT_ID` with `kubectl set env` and waits for the rollout. The
   serving pod's own `CURIE_CALLER_PROXY_GATED_TOOLS` must match neither tool.
2. G1: `--sender` calls `execute_attested` directly
   (`[fake:connector-tool ...]`) with no approval. The claim carries no grant;
   the proxy admits the call without attesting it; the connector refuses
   `attestation_missing` and logs `grant=-`; nothing is spent.
3. G2: `--sender` trips the runner's gate with
   `[fake:gated-connector-tool human-restricted execute_attested {...}]` on a
   new conversation C. The API lists a pending permission-gate approval
   granting that tool on C, its card reaches the email thread, and nothing
   reached the connector or was spent.
4. G3: `--second-sender` approves by a signed email answer, as in
   `requester-approval` P2. The resume runs on a new claim whose caller token
   names the requester, never the approver, and its turn is delivered. The
   resume claim carries the worker-minted `CURIE_CONNECTOR_TOOL_GRANT` for
   exactly the approved connector, tool, agent and arguments with a canonical
   UUID `jti`; the evidence records only where it was found and the `jti`'s
   fingerprint. The harness looks in the claim's token Secret first, the way
   it reads the caller token, then in the claim's `spec.env`: the worker does
   not count the grant among the claim's scoped tokens, so it arrives there.
   Nothing is spent yet. The harness keeps the grant and the resume claim's
   caller token in memory, both held private, for G4 to G9.
5. G4 to G8 are one probe each, sent with the G9 probe script from inside the
   connector pod's caller-proxy container to the live proxy on loopback. Each
   carries the resumed sandbox's boot credentials: the resume claim's caller
   token as `X-Curie-Caller` and the G3 grant as `X-Curie-Connector-Grant`. No
   probe mints a grant. Each is judged in its own pod-clock window against the
   proxy's decisions, the connector's lines and the spends Valkey shows across
   its step; the probe must report the G3 fingerprint, and in every step but
   G7 the proxy must admit it with `principal=present`. G4: an unrelated
   `record_effect` is forwarded unattested and applied; the proxy attests and
   refuses nothing, the connector logs `ok` under the requester's fingerprint
   with `grant=-`, and nothing is spent. G5: `execute_attested` with the
   approved arguments; the proxy logs `attested=present` once, the step's one
   spend is the G3 fingerprint, and the connector applies the effect under the
   requester's fingerprint with that grant. G6: another unrelated
   `record_effect` after the spend, judged as G4. G7: G5's exact call and
   grant, sent again, are refused `grant_required` (`not_spent`), with no
   connector line and no spend. G8: `execute_attested` with other arguments
   and the grant forwards unattested, spends nothing and is refused
   `attestation_missing`.
6. G9: from inside the connector pod's caller-proxy container, one script
   posts to the live proxy on loopback with the captured caller token, grants
   minted with the install's caller signing key (which `up` set) and tickets
   signed with the selector seed, all passed on stdin. The proxy's gated list
   must still be G0's. Each probe is judged in its own pod-clock window against
   the proxy's decisions, the connector's lines and the spend set: a forged
   `X-Curie-Grant-Verified` in three casings and repeated, with no grant, is
   stripped (`attestation_missing`); a valid ticket on `execute_approved` is
   attested and applied, and its replay is refused `not_spent`; a batch
   carrying the matching call is refused `batched_call` before any spend, and
   the same grant alone is then attested and applied; tickets for another
   requester, another route, another signing key and other arguments are
   attested and then refused `ticket_requester_mismatch`,
   `ticket_scope_mismatch`, `ticket_invalid` and `ticket_arguments_mismatch`
   with no effect; `drop_reply` answers the proxy's 502 with exactly one
   effect and one spend, and its resend is refused `not_spent`; a call whose
   arguments differ from its grant forwards unattested with no spend, and a
   replay of an applied grant afterwards is still refused. The spends G9 made
   must be exactly one per attested probe.
7. G10 always runs. It puts back `approval_required_tools` (checked with SQL),
   the connector's `TICKET_*` settings (checked on the serving pod), the flag
   and the approver role, removes the selector seed from the state, and
   requires the serving pod's gated list to equal G0's. As in
   `requester-approval`, an approver role that had no entry stays with no
   filter.

G4 to G8 are probes, not email turns, because no follow-up turn runs in the
sandbox that holds the grant. Every follow-up turn boots a new sandbox:
`apps/worker/src/curie_worker/kernel/claim.py::_boots_differently` replaces a warm route whose state-token
expiry is earlier than the new boot token's (#3823), and the new token's
expiry, now plus the turn deadline plus the grace, is always the later. So the
probes send that sandbox's own boot credentials, the caller token and the
grant, which are the two headers its runner sends on every hosted connector
call. The runner's handling of those headers did not change when these steps
became probes, so the proxy and the connector see what the runner would send.

No evidence carries a grant, ticket, key, token or principal value. Grants
appear as the fingerprint of their `jti`, humans as the fingerprint of their
`X-Curie-Principal` value, and the probe script prints only statuses, the
first line of each tool result, the proxy's refusal code, the grant
fingerprint and the pod-clock window of each request. G10 checks the whole
evidence last, after its restores and including its own step, and fails on
any object with both an `issuer` and a `subject` key, any `cct.`, `ccg.`,
`ccr.` or `cat1.` envelope, and any private value the run held in memory: both
humans' subjects and `X-Curie-Principal` values, the captured caller token,
every grant it read, the selector seed and the caller signing seed. The
failure names only the JSON path of what it found. A rejected value is
replaced (`[redacted]`, or `[redacted: principal]` for a principal-shaped
object) before the evidence is written or printed, the evidence records how
many under `privacy`, and the run fails. The run holds each private value from
the moment it has it, so a run that stops before G10 is scrubbed the same way
when it finishes.

## What stored-arguments-card proves

ADR 0271 decision 8 and required evidence item 6 on a live install: the card
of a dynamic approval, the requester's pending notice and the API's
`display_summary` describe the arguments the approval stores, through the
existing describer. Neither the bundle's display template nor the opaque
`approval_ticket` replaces them or appears beside them. Run it with:

```bash
curie dev identity-e2e -- prove $T --scenario stored-arguments-card --evidence card.json
```

It needs what `approver-ticket-creation` needs: `--flag-agent` (default: the
first agent `up` registered) brought up with `--bundle examples/weather`, so it
serves the `netpol-probe` connector; `--sender` (the requester) and
`--second-sender` (the selected approver), two different active verified
humans in the roster; and no selector registration already stored on the
agent. The scenario turns the requester flag on itself. `--wait` bounds each
wait for the approval row, the card and the notice.

Each step records its own evidence:

1. S0: the `approver-ticket-creation` setup (both owned routes bound, only the
   gated version served, the approver role restricted to the roster's active
   humans), except that the served gate carries a `summary` template holding a
   per-run template marker and no placeholders, so the runner's own display
   for the held call is that marker. The run's selector is then registered on
   `ticket-review` for `netpol-probe`/`search_dashboards` with the flag on, and
   real email turns from the approver and the requester give both humans an
   active canonical row in `principals`.
2. S1: the API pod signs a ticket with the run's selector seed for the
   requester's verified identity from that turn's capsule, the registered
   route, agent, connector and tool, and the selected approver, with a fresh
   nonce, issued now and valid for 240 s, over `{"query": <stored marker>}`
   by `execution_arguments_sha256`. The requester's email turn carries
   `[fake:gated-connector-tool netpol-probe search_dashboards {"query": "<stored marker>", "approval_ticket": "<ticket>"}]`,
   the runner's gate holds the call, and the worker creates the approval
   through the HTTP creation boundary. The stored row is read from the API's
   Postgres, because the public read model carries neither `selection_mode`
   nor `granted_arguments`: exactly one approval on the conversation grants
   `mcp__netpol-probe__search_dashboards`, and it is dynamic, pending, over the
   stored marker, and stores the ticket the turn carried (matched by
   fingerprint).
3. S2: the card the worker delivered to the email thread (its text and its
   prompt), the requester's pending notice and `GET /approvals/<id>`'s
   `display_summary` each show the stored marker, and none shows the template
   marker, the `cat1.` prefix, any 24 consecutive characters of the ticket or a
   label for the ticket field. The whole card event is checked for the same
   leaks. The route resolves on the requesting surface, so the card lands in
   the email thread and the notice above it is the fixed line
   `Approval requested. See the card below.`, which carries no summary: that
   notice is checked for the leaks, and only a notice that carries the summary
   (`Awaiting approval (<id>): ...`) must also show the stored marker.
   `display_summary` must be at most 2400 characters plus the describer's
   truncation notice.
4. S3 always runs, in a `finally` block, and a failed restore fails the run.
   It runs the `approver-ticket-creation` restore and its check of the
   registration, flag, deployments and routes, then puts back the approver
   role, and removes the selector seed from the state.

The 2400-character truncation itself is proved by unit tests
(`apps/worker/tests/kernel/test_approval_stored_display.py` and
`apps/api/tests/test_approval_presentation.py`), not live: a dynamic
approval's arguments, ticket included, must fit the 4096-character connector
grant, which keeps every live preview under the limit. The S1 approval stays
pending until it expires, after the 24-hour session default because the ticket
route binds no expiry, and its expiry resumes the paused conversation on the
restored deployment. The run's agent version stays, undeployed.

No evidence carries a ticket, key, token or principal value. The ticket stays
in memory: it crosses the API pod's exec pipe and the requester's turn, and the
evidence keeps lengths, booleans and fingerprints (the first 16 hex characters
of a sha256) of the ticket and of each judged text. The S0 turns and
`principals` rows appear as `approver-ticket-creation` records them. The
approval's `summary` field, the runner's machine string of the raw call, is
never read into the evidence. S3 checks the whole evidence last, after its
restore and including its own step, as `grant-attestation` G10 does, with the
two humans' subjects and `X-Curie-Principal` values, the ticket and the
selector seed as the run's private values, and every ticket read back from the
approval rows. As there, a rejected value is replaced before the evidence is
written or printed, and the run fails, also when the run stops before S3.

## Add a scenario

1. Write a function in `identity_scenarios.py` and register it with
   `@scenario("your-name")`. It receives the harness, the parsed `prove`
   arguments and an `Evidence` recorder.
2. Put each claim in its own `with evidence.step("<id>", "<what it proves>") as seen:`
   block. Record what you observed in `seen` and assert with `check(...)`. A
   failed step is recorded and the next step still runs. When later steps
   build on a step, call `require_passed(evidence)` after it to stop the run.
3. Drive the install only through `harness.send` (or `harness.prepare` and
   `harness.post` for sends that must go out close together),
   `harness.observe`, `harness.api()`, `harness.target.curie(...)`,
   `harness.records()` and the provider's admin endpoints
   (`harness.provider_admin(...)`). Read connector evidence with
   `harness.connectors(...)`, `harness.proxy_decisions(...)` and
   `harness.connector_effects(...)`, and worker messages with
   `harness.worker_log(...)`. `harness.capture_caller_token(claim)` returns a
   claim's raw token for `harness.proxy_replay(...)`; keep it in memory and
   record only its fingerprint and `exp`. `harness.connector_tool_grant(claim)`
   returns the grant a claim's runner boots with, under the same rule, and
   `harness.grant_spends()` the spent grants as `jti` fingerprints.
   `harness.connector_server_env(...)` and
   `harness.set_connector_server_env(...)` read and change a connector server
   container's env; put it back in a `finally`.
4. Use `harness.target.sql(...)` or `harness.route_doctor(...)` only to
   corrupt state that no product surface can write, and restore it in a
   `finally` block. `route_doctor` is the route doctoring exception:
   `requester-expiry` X5 needs a warm route whose caller token is too short
   for the next turn, which no product path writes. It finds the worker's
   sandbox route record in Valkey by its claim name (keys
   `curie:sandbox:route:<thread key>`), from inside the worker container with
   the worker's own Valkey client, and rewrites only `caller_token_exp`, with
   the record's own serialization and TTL. Its restore is conditional on the
   field still holding the doctored value. The record also carries the
   runner's bearer, which nothing prints.
5. If the scenario needs other humans, add them to the default roster or ship a
   roster file, and add any new agent with `up --agent`.
6. Add a unit test in `tools/identity-e2e/tests` for any new helper, judged by
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
5. The reference connector runs a digest-pinned `python:3.12-slim` image. The
   cluster must be able to pull it, or have it loaded (`kind load
   docker-image`), before `up --reference-connector` deploys the agents.
6. `requester-expiry` cannot shorten its one-hour wait: X6 proves the live
   proxy's own clock refuses the token, which a controlled clock cannot.
7. `grant-attestation` needs the connector reconciler off. With it on, the
   reconciler may render the connector again from the agent's current
   `approval_required_tools`, so the proxy would gate the protected tools
   itself and the run would no longer show that they hold without the gated
   list.
8. A harness error names only the failed command (or the workload and
   container of an in-pod call) and its exit code, since the output can carry
   a subject, token or query parameter. The raw output tail of the last failed
   command goes to `last-exec-error.txt` in the state directory, mode 0600,
   and the printed error says so when that file was written. A failed restore
   logs only its exception type.
9. `grant-attestation` and `stored-arguments-card` refuse a roster subject (or
   any other private value) shorter than 8 characters, because the evidence
   could not be searched for it reliably. Use longer subjects in a custom
   roster.
