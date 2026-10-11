---
seam: MCP service-key authentication
kind: SOFT
impls: 1 deployment-owned service identity mode
grade: not separately graded
epics:
  - "#3273"
  - "#3308"
order: 23
---
# INTERFACE: MCP service-key authentication

> Part of the Curie swappable-seam catalog: see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 deployment-owned service identity mode &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

An agent calls an MCP server as itself, with a credential the deployment owns,
not one a user delegated. Two questions are answered on that call: what the
connector server sees as the client credential, and whether the caller is an
agent the connector admits. Under ADR-0009 the first is a named per-agent
secret. Under ADR-0168 decision 7 and ADR-0178 the second is a signed caller
token that a proxy in front of every hosted connector checks before the server
is reached.

This file maps that auth boundary. Where the connector workload runs, and how
its objects are rendered and reconciled, is
[connector-host](../connector-host/INTERFACE.md); the proxy's forwarding and
refusal mechanics are described there and not repeated here.

The seam is SOFT, not CLEAN, because no code `Protocol` draws the line. What a
second identity mode would replace is a set of wires: an env var name that a
`${NAME}` placeholder points at, an `Authorization: Bearer` header the runner
expands in memory, two Ed25519 token formats told apart by prefix, a header
the proxy reads, and a Valkey key it writes. The halves share frozen test
vectors rather than a type. ADR-0088 accepts a per-user delegated OAuth mode as
a separate identity, and no second mode ships today, so nothing has proved the
abstraction. The direction is to keep service auth explicit and fail closed,
with no fallback between the two modes.

## Current contract

1. **The client credential is a named secret.** `_derived_headers`
   (`packages/plugin-format/src/plugin_format/connector_render.py::_derived_headers`)
   gives a hosted connector `Authorization: Bearer ${NAME}`, where `NAME` is
   `bearer_secret` on the `ConnectorSpec`
   (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec`)
   when set, or else the one plain-string secret. A connector whose secrets are
   all `SecretRef` derives no header when `bearer_secret` is absent, because a
   referenced value is the server's own upstream credential and reaches only the
   connector pod. An explicitly selected `bearer_secret` that is a `SecretRef`
   still emits the `Bearer ${NAME}` placeholder, so the runner can diagnose the
   missing sandbox value. The rule is
   frozen in `tests/vectors/connector-derived-bearer.json`, and the runner trims
   an implied header the sandbox could never expand with
   `_without_unreachable_bearer`
   (`runner/src/curie_runner/connectors.py::_without_unreachable_bearer`).
2. **Values cross by name into the sandbox.** `inject_connector_secrets`
   (`apps/worker/src/curie_worker/binding.py::inject_connector_secrets`) writes
   the agent's resolved secrets into the boot env, drops reserved names and
   `SANDBOX_WITHHELD_CONNECTOR_SECRETS`
   (`apps/worker/src/curie_worker/binding.py::SANDBOX_WITHHELD_CONNECTOR_SECRETS`),
   and lists the injected names in `CURIE_CONNECTOR_SECRET_KEYS`. On Kubernetes
   the claim strips those values by that marker and the per-agent template
   delivers them by `secretKeyRef` (`apps/worker/src/curie_worker/sandbox/k8s.py`).
3. **Expanded in memory, then removed from the environment.** At boot,
   `materialize_hosted_bearer_headers`
   (`runner/src/curie_runner/connectors.py::materialize_hosted_bearer_headers`)
   replaces each hosted `Bearer ${NAME}` with the value and drops `NAME` from the
   spawn env and the process env through `drop_connector_secret_names`
   (`runner/src/curie_runner/connectors.py::drop_connector_secret_names`), before
   the SDK session or Bash can read it. A name that never arrived stays a
   placeholder, which the capability probe reports as a missing credential.
   Unrelated secrets stay in the env so ADR-0009 stdio and remote `${VAR}`
   expansion still works.
4. **The caller token, `cct`.** When `CURIE_CONNECTOR_CALLER_SIGNING_KEY` is set,
   `BindingResolver.boot_env`
   (`apps/worker/src/curie_worker/binding.py::BindingResolver.boot_env`) mints one
   per boot with `mint` (`apps/worker/src/curie_worker/caller_token.py::mint`):
   an Ed25519 signature over compact JSON `{agent, exp}`, plus `run` and
   `work_item` together or not at all (ADR-0178), each a lowercase hyphenated
   UUID. Expiry is 24 hours, capped by the run's execution deadline when the
   kernel passes one. The wire is frozen in
   `tests/vectors/connector-caller-token.json`. The signing key is listed in
   `HOST_APPLICATION_CREDENTIAL_ENV_NAMES`
   (`apps/worker/src/curie_worker/sandbox/types.py::HOST_APPLICATION_CREDENTIAL_ENV_NAMES`),
   so it never enters a sandbox.
5. **Presented only to Services Curie created.** `derive_mcp_servers`
   (`runner/src/curie_runner/connectors.py::derive_mcp_servers`) puts the
   `X-Curie-Caller` placeholder (`runner/src/curie_runner/connectors.py::CALLER_HEADER`)
   on hosted entries only, never on a remote or fallback URL.
   `materialize_connector_caller_headers`
   (`runner/src/curie_runner/connectors.py::materialize_connector_caller_headers`)
   expands it in memory and drops the token from every env mapping it was given.
6. **The proxy is mandatory for a hosted render.** The API builds a
   `ConnectorProxy`
   (`packages/plugin-format/src/plugin_format/connector_render.py::ConnectorProxy`)
   from its public keys through `Settings.connector_proxy`
   (`apps/api/src/curie_api/config.py::Settings.connector_proxy`). With no key,
   `render` (`packages/plugin-format/src/plugin_format/connector_render.py::render`)
   raises `hosted_connector_requires_caller_key` and the deploy is a 422, not an
   ungated connector. The proxy refuses to start without a public key or a
   valid `admits` list (`apps/worker/src/curie_connector_proxy/server.py::ProxyConfig.from_env`).
7. **Admission.** `decide` (`apps/worker/src/curie_connector_proxy/caller.py::decide`)
   verifies the signature over the received text before parsing, accepts exactly
   `{agent, exp}` or that set plus the pair (`apps/worker/src/curie_connector_proxy/caller.py::claims`),
   requires `exp` later than now, and matches `agent` exactly against the
   rendered `admits` list. Either configured key may verify, which is how a
   rotation overlaps. The verified identity reaches the server as headers only
   the proxy sets; [connector-host](../connector-host/INTERFACE.md) has the
   forwarding detail.
8. **The tool grant, `ccg`, is a second token.** For a turn resuming an approved
   connector call, `_connector_tool_grant`
   (`apps/worker/src/curie_worker/kernel.py::_connector_tool_grant`) mints one
   with `mint` (`apps/worker/src/curie_worker/connector_grant.py::mint`): claims
   exactly `agent`, `connector`, `tool`, `args` (canonical JSON), `exp` and a
   random `jti`. It uses the same signing key as `cct` and is told apart by
   prefix and claim set. The runner adds `X-Curie-Connector-Grant`
   (`runner/src/curie_runner/connectors.py::GRANT_HEADER`) to hosted entries only
   when a grant is present.
9. **One spend per grant.** For a gated `tools/call`, `_grant_refused`
   (`apps/worker/src/curie_connector_proxy/server.py::_grant_refused`) requires one
   grant that `verify` (`apps/worker/src/curie_connector_proxy/caller.py::verify`)
   accepts, unexpired, naming this agent, connector, tool and canonical
   arguments, then spends its `jti` through `_ValkeyGrantStore`
   (`apps/worker/src/curie_connector_proxy/server.py::_ValkeyGrantStore`) as
   `SET connector-grant:<jti> 1 NX EX`. A replay, a missing store or a failed
   spend refuses with `grant_required`. Approval resolution itself stays in the
   [approval](../approval/INTERFACE.md) seam; the grant only carries its result to
   the connector boundary.

## Implementations today

One mode: a deployment-owned service identity, a per-agent Bearer secret plus
the signed caller token, enforced by the proxy on Kubernetes.

1. **Signer:** the worker, `apps/worker/src/curie_worker/caller_token.py` and
   `apps/worker/src/curie_worker/connector_grant.py`.
2. **Verifier:** the caller proxy, `apps/worker/src/curie_connector_proxy/caller.py`
   and `apps/worker/src/curie_connector_proxy/server.py`, run from the worker image.
3. **Key custody:** `curie cluster up` generates the pair and carries it across
   upgrades (`cli/src/connector_caller.rs`); the chart holds it as
   `connectorCaller` values or a BYO `existingSecret` (`charts/curie/values.yaml`,
   `charts/curie/templates/secrets.yaml`). The worker reads the private half and
   the API the public half (`charts/curie/templates/worker.yaml`,
   `charts/curie/templates/api.yaml`).

The Docker dev tiers are not a second mode: their connectors run with no proxy
and no grant check (`cli/src/docker.rs`, `cli/src/connector_build.rs`).

## Known leakage

1. **Secret delivery spans many hops before the process starts.** A value moves
   from the API's agent record through the worker's boot env, the Kubernetes
   Secret and `secretKeyRef`, the kubelet and the container environment. The
   runner removes it only after boot, so anything that reads the container's
   initial environment, or the pod spec's Secret, still sees it. A `SecretRef`
   never reaches the sandbox, which is why an implicit one derives no header. An
   explicit `bearer_secret` naming one still emits the placeholder, which the
   runner reports as missing.
2. **The grant is not removed from the environment.** Unlike the Bearer value
   and the caller token, `CURIE_CONNECTOR_TOOL_GRANT` stays a `${...}`
   placeholder for the MCP client to expand and is not dropped, so it stays
   readable in the turn's environment. Its exposure is bounded by being single
   use and bound to one tool and one argument set.
3. **The chart ships a published dev pair.** With `security.allowDevDefaults`
   set to `"true"` and neither inline half set, the chart supplies a fixed
   published pair (`charts/curie/templates/_helpers.tpl`) so a dev install can
   render a hosted connector at all. Anyone can mint a token for it. Leaving dev
   mode while the release Secret still holds that public key, with no replacement
   pair or `existingSecret`, fails the render.
4. **Rotation reaches workloads unevenly.** A checksum over the effective public
   key, the previous key and the BYO references rolls the API and the worker. A
   rendered proxy holds keys in its own env and moves only when its agent is next
   deployed or the reconciler next renders it. A BYO Secret rotated in place under
   the same name and keys changes no checksum and needs a manual rollout.
5. **The Docker dev path checks a key it never uses.** Both Docker emitters
   refuse a hosted connector without a caller public key, and the CLI satisfies
   that by generating a throwaway pair on each start (`cli/src/commands.rs`). No
   proxy holds it, so the refusal guards nothing on that path.
6. **Docs and comments still describe the keyless install.** The chart values
   comment and the connector-host file say an install with no key renders no
   proxy and leaves the NetworkPolicy as the only check; `render` now refuses that
   case. Comments in `apps/worker/src/curie_connector_proxy/caller.py` and
   `apps/worker/src/curie_worker/connector_grant.py` say `cct` stays `{agent, exp}`,
   which predates ADR-0178's optional pair.
7. **One key signs two token kinds.** `cct` and `ccg` share the signing key and
   the verifying keys, separated only by prefix and an exact claim set, so a
   rotation or compromise of one is a rotation or compromise of both.

## Cross-links

1. **Related seam:** [connector-host](../connector-host/INTERFACE.md): where hosted connectors run, how the proxy, Services and NetworkPolicies are rendered, and the Docker dev host this auth does not cover.
2. **Related seam:** [sealed-credential](../sealed-credential/INTERFACE.md): another holder shape for a connector secret, refused by validation today, which would feed the same named-secret boundary.
3. **Related seam:** [approval](../approval/INTERFACE.md): decides an approval; this seam only carries the result to the connector as a one-use grant.
4. **Epic(s):** #3273: the worker signs a caller token per sandbox boot; #3308: a caller proxy in front of each hosted connector
5. **Vision doc:** [architecture-vision.md](../../architecture-vision.md): MCP service authentication is not one of the six swap-readiness Jobs; not separately graded
6. **ADR(s):** [ADR-0009](../../adr/0009-per-agent-connector-auth.md): per-agent secrets and connector credentials; [ADR-0086](../../adr/0086-bundles-declare-connectors-the-platform-hosts-them.md): bundles declare connectors, the platform hosts them; [ADR-0094](../../adr/0094-a-bundle-carries-its-own-sealed-connector-keys.md): sealed connector keys; [ADR-0168](../../adr/0168-one-installation-hosts-several-bot-identities.md): decision 7, the admission list and caller token; [ADR-0178](../../adr/0178-the-connector-caller-token-names-the-run-and-work-item.md): the caller token names the run and work item; [ADR-0088](../../adr/0088-per-user-delegated-oauth-for-mcp.md): per-user delegated OAuth, the separate identity mode
