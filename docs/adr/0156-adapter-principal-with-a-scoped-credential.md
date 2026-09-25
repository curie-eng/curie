# 156. A channel adapter is a principal, and its credential is scoped to what an adapter does

Date: 2026-09-17

Status: Accepted

Accepted 2026-09-18 with explicit maintainer approval from Brian Conn, recorded on the publishing pull request. The decision was first published on `main` as ADR 0154 in #2807; it is republished here on `next`, the branch that feature work targets, under the next free number because `next` already numbers 0154 and 0155 for other decisions. The body below is that text with one addition: the trust boundary named in decision 3, asked for at acceptance.

## Context

[ADR-0020](0020-message-port-rendering-free-channel-interface.md) made the channel port a rendering-free interface: an adapter outside the platform turns a provider's messages into turns and sends replies back. The credential that came with it is deliberately narrow. A `chn` token, minted by `POST /channels/token`, does exactly one thing: enqueue for the binding in its claims. It lives at most seven days (`ChannelTokenRequest.ttl_s` is capped at 604800), and a re-mint bumps the binding's generation so the token it replaces dies (#2379). Those two properties, the cap and the generation, stand in for the revocation list the design does not build.

Three things an adapter needs sit outside that credential, and all three are behind the platform API key today:

1. **Renewing its own token.** `mint_channel_token` is platform-key-only, and the docstring says why: a token that could mint would defeat both the cap and the generation. The consequence in the field is that something else must mint on a schedule. On one deployment that something is a CronJob holding the platform key, which mints every adapter's token at the cap every three days and patches the Secrets. The most powerful credential in the installation lives in the cluster to perform a seven-day chore.
2. **Carrying approval decisions from the channel.** [ADR-0151](0151-bundle-authored-human-approval-summary.md) and the approval principal work gave resolution an authenticated subject: `ApprovalPrincipalDep` verifies a `chat` or `operator` credential rather than trusting a caller-supplied actor string, and `authorizer.PrincipalKind` is `chat | console | operator`. A channel adapter that authenticates a human at ingress (a mail adapter that has verified the sender's domain alignment, for instance) has no kind to present as. To resolve an approval it would hold the platform key on an inbound-facing pod. A downstream mail adapter has this path built, with tests, and switched off for that reason.
3. **Being named in the audit row.** `approval_audit.principal_kind` is constrained to the three kinds above. An action performed with the platform key names the key, not the adapter that presented it.

Two shapes were considered downstream before this record: an identity object with its own table and credential type, and scopes added to the existing API key. Both were refused with the same observation: each changes what a key may do without giving the adapter an identity, so audit rows still could not say which adapter acted, and a later change of credential mechanism would have to move approvals and routes again.

## Decision

1. **A channel adapter is a principal.** `PrincipalKind` gains `adapter`, beside `chat`, `console` and `operator`. The approval authorizer, the audit constraint and the credential verifier learn the kind. An adapter principal has a subject (the adapter's name) and a set of binding ids it serves.
2. **Its credential is scoped to what an adapter does.** The credential carries three scopes, `channels:token`, `approvals:read` and `approvals:resolve`, each bound to the principal's binding ids. It expires. Issuance is administrative, behind `require_platform_key`, in the same shape as `POST /approvals/principals/operator`: the platform key authorizes issuance once and is not presented by the adapter at runtime.
3. **Three routes accept it, each checking the binding.**
   - `POST /channels/token` accepts an adapter principal whose binding set contains the requested binding, and refuses any other. The seven-day cap and the generation bump are unchanged, so a compromised adapter can renew its own binding's token and nothing else, and a re-mint still revokes the previous one.
   - `GET /approvals` filtered to routes whose approver set names an address the adapter serves.
   - `POST /approvals/{id}/resolve` with the sender the adapter authenticated as the actor, judged by the approver set exactly as a `chat` principal's actor is. The adapter is the transport of the decision, not its author; the audit row names both.
   - **The adapter's authentication of the sender is the trust boundary.** The api takes the actor from an adapter principal on the adapter's word, exactly as it takes a `chat` principal's actor from the chat platform. Everything the adapter checks before it presents a sender (that the message authenticated as coming from the sender's domain, that the address is one the route names as an approver, that the decision is the sender's own words and not a forward or a quote) is the whole of the guarantee behind a resolution carried this way. An adapter that skips those checks can resolve any approval on the routes it serves, and nothing in the api would notice. That is why the credential is bound to the adapter's bindings and the audit row names the adapter: the boundary is per adapter, and a failure of it is attributable to one.
4. **The adapter rotates its own credential.** Before expiry it presents the current credential to obtain the next one for the same subject and bindings. The platform key never mints at runtime. The CronJob pattern is therefore an interim, and an installation using it records the date on which the first adapter credential lands and the job is removed.
5. **Every audit row names the adapter.** Token mints, approval reads and resolutions performed by an adapter principal record `principal_kind = adapter` and the subject.

## Consequences

- The three routes gain a dependency that accepts either the platform key (unchanged behavior) or an adapter principal with the matching scope and binding. Nothing an existing caller does changes.
- A downstream adapter can turn on approvals by channel without holding the platform key, and can drop its rotation job. Until it does, its record names the interim and the date.
- The long-term credential is the Kubernetes projected service account token, an OIDC JWT with an audience the api verifies, which removes both the Secret and the rotation; and an operator-declared per-agent policy evaluated at the same three checks. Bot identities become principals of kind `agent` with the same policy shape. Because the principal exists from this decision, approvals, audit and routes do not migrate when the credential mechanism changes. That work is a later ADR.
- Tests to hold this: an adapter principal is refused a token for a binding it does not serve; it cannot list or resolve an approval outside its routes; its credential expires and rotation before expiry succeeds while rotation after expiry does not; the audit row names the adapter.

## Alternatives considered

- **Scopes on the platform API key, no new principal.** Smaller, and it would have fixed the CronJob's credential. Refused because the audit row would still name a key, not an adapter, and a later move to service-account tokens would have to re-plumb approvals and routes.
- **An adapter identity object with its own table and credential type.** Gives the adapter a name, but as a second identity model beside approval principals; two verifiers, two audit shapes, two rotation stories. The principal kind reuses the one that exists.
- **Raising the `chn` cap so pods keep long-lived tokens.** Trades the revocation stand-in for convenience; refused for the reason `mint_channel_token` already states.
- **Keeping the platform key on the adapter pod "for the demo".** The single thing this record exists to avoid.

## Reference

- Issue: #2806
- Downstream measurement that motivated the shape: a mail adapter whose approvals path is built and off, and whose token renewal runs through a CronJob holding the platform key. Names withheld; nothing here is specific to that installation.
