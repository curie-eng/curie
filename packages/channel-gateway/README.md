# Channel gateway

`curie_channel_gateway` owns provider connections, intake, rendering, clicks,
egress and provider reads. This extraction is behavior-neutral. This first
slice defines ports and regression fixtures only; no production composition
uses this package yet. `slack/` and `mail/` are implementation destinations.

## Ports

Import ports from `curie_channel_gateway.ports`; their defining modules are:

| Module | Port or value |
| --- | --- |
| `ports/reply.py` | `AuthenticatedReplyTransport[Route]` |
| `ports/admission.py` | `AdmissionClient`, `AdmissionRequest`, `AdmissionAnswer` |
| `ports/groups.py` | `GroupMembershipSource[Membership]` |
| `ports/reads.py` | `IdentitySelection[Route]`, `FileSource`, `SenderIdentities`, `AddressIdentityLookup`, `ChannelReader[Route, Window, Page, Message]`, `CanvasReader[Route, File, Page, Document]` |

Reply events and acknowledgements remain the existing `channel_protocol.reply`
types: `turn.status`, `reply.update`, `reply.post`, `turn.completed` and `{ref}`.
Wire 1.1 carries progress and delivery identities; wire 1.2 carries literal text
updates. Status and completion retain their 1.0 wire. Authentication, endpoint
selection and credential selectors stay outside that wire.

Generic parameters bind directly to today's worker `TargetRoute`, API
`UserGroupMembership`, `BindingRoute`, `ResolvedWindow`, `ProviderPage`,
`ChannelMessage`, `CanvasFile`, `CanvasPage` and `CanvasDocument`. They preserve
the current typed signatures without importing applications or moving records.
Existing reply and read implementations satisfy their ports structurally.
Lookup errors retain their existing identity and semantics. The admission port
is a wrapper contract: map `evidence` to the API's existing provider evidence
field and preserve `None` as unavailable. It adds no platform endpoint.

## Authority and storage

The API retains admission authority, authorization, verified-principal policy,
approval decisions and resolve-once. Mandatory uncached group resolution stays
an API requirement through `fresh_members`; a provider read grants no authority.
The worker retains resume enqueue, stream consumption, `_ThrottledReply`,
completion/outbox and its neutral `ReplySink`. API admission state, Valkey
dedupe/cache, the worker stream and mail's local SQLite delivery state keep
their current owners. The gateway acquires no Postgres storage or coordination
contract.

Stock Slack still calls `/channels/admission`, claims dedupe, posts a
placeholder, then enqueues. A refusal claims nothing and posts nothing. Mail
retains authenticated `/channels/turns` ingress with its scoped credential and
its standalone entrypoint. AgentMail still cannot authenticate inbound senders;
the positive intake fixture uses the existing verifying test provider.

## Move sequence

1. U2 moves Slack intake and connection code and the mail adapter behind these
   ports, preserving entrypoints, admission order and all existing storage owners.
2. U3 moves Slack egress, cards and clicks and neutral authenticated HTTP delivery.
   Before moving renderers, put the general `Reply` value shape from worker
   `blocks.py` in `channel-protocol` and migrate every caller. The worker must
   never import gateway rendering to obtain that shared value.
3. U4 moves credential-bearing provider reads and pure provider evidence
   validators. The API still executes the validators and authorizes every read
   and approval, including uncached `fresh_members`; then remove provider tokens
   from API and worker. Bind the read ports to the existing API record types.

## Regression proof

From the repository root, run:

```bash
env -u CURIE_SLACK_TRUSTED_ORIGINS CI_REQUIRE_VALKEY_TESTS=1 uv run pytest -q @packages/channel-gateway/behavior.pytest
```

Set `TEST_VALKEY_HOST`, `TEST_VALKEY_PORT` and `TEST_VALKEY_PW` for the isolated
real Valkey instance. Clear ambient development origin trust so it cannot change
the foreign-endpoint negative control. No infrastructure skip counts as proof.
The argument file selects current executable provider paths, the shared reply
corpus, approval action and principal vectors, mail provider conformance, and
positive and negative admission/reply paths. The additional Slack fixture checks ordering
at the real dedupe write, placeholder call and stream append. HTTP fixtures
send the shared corpus through the current worker transport to a real HTTP
receiver. Keep these selectors and assertions working after each move; update
only import wiring when a provider's module location changes.
