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
build. The provider is [`tools/identity-e2e/test_idp.py`](../../tools/identity-e2e/test_idp.py)
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
   `examples/weather` for `requester-claim`: its one hosted connector,
   `netpol-probe`, is what the scenario's turns call. `--bundle` only deploys
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
   passed.
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
format is documented at the top of `test_idp.py`. Every lookup answer is a copy
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

## Add a scenario

1. Write a function in `identity_scenarios.py` and register it with
   `@scenario("your-name")`. It receives the harness, the parsed `prove`
   arguments and an `Evidence` recorder.
2. Put each claim in its own `with evidence.step("<id>", "<what it proves>") as seen:`
   block. Record what you observed in `seen` and assert with `check(...)`. A
   failed step is recorded and the next step still runs.
3. Drive the install only through `harness.send`, `harness.observe`,
   `harness.api()`, `harness.target.curie(...)` and `harness.records()`, and
   read connector evidence with `harness.connectors(...)` and
   `harness.proxy_decisions(...)`. Use
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
