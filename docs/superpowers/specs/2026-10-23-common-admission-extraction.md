# Common API admission: behavior-preserving extraction

Realizing specification for the first extraction slice of
[#4890](https://github.com/curie-eng/curie/issues/4890), under
[Accepted ADR 0175](../../adr/0175-a-bot-may-limit-who-can-talk-to-it.md) and
[Accepted ADR 0220](../../adr/0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md).
This specification records the existing API behavior that the extraction must
preserve. It does not claim that the extraction is implemented or that the
complete common admission capability is qualified.

## Boundary and shared composition

<!-- @spec COMMON-ADMISSION-EXTRACTION-1 -->
Both existing API entry points, `POST /channels/turns` and
`POST /channels/admission`, delegate invocation-policy composition to one
API-internal service. The service receives the binding already resolved by
that entry point and exactly the caller and identity evidence that entry point
currently supplies. It evaluates the existing binding-list predicate and
invocation-team check without duplicating their rules.

The existing pure list predicate,
`apps/api/src/curie_api/admission.py::admit`, retains its callable contract and
normalization. The shared composition reuses
`apps/api/src/curie_api/team_authorization.py::check_binding` for the invocation
role. Its structured result retains the list decision and, when evaluated,
the team decision, as well as the composed allow and restriction values. This
retained detail permits existing transport-specific refusal handling without
repeating policy evaluation.

The entry points preserve these distinct evidence profiles:

| Entry point | Caller IDs | Identity evidence | Binding absence |
| --- | --- | --- | --- |
| Channel port | Exactly the turn's `author` | The same author as email evidence only for an email binding; no Slack evidence | Existing authenticated missing-binding refusal |
| Admission query | The request's `callers` | Only the request's existing Slack evidence; no email inferred from callers | Allowed and unrestricted, with the existing installation restriction calculation |

The extraction does not synthesize Slack evidence or an initiating principal
from a channel-port author. A Slack channel-port turn with an invocation team
continues to lack the evidence needed to resolve its caller. The admission
query does not acquire email-resolution behavior.

The service is internal composition, not a new credential, wire contract or
swappable provider framework. Existing router request and response models,
status codes, authentication dependencies, token scope and public OpenAPI
remain unchanged. Both consumers must be observed delegating to the same
service; helper-only tests do not establish that integration.

## Existing policy semantics

<!-- @spec COMMON-ADMISSION-EXTRACTION-2 -->
Preserve ADR 0175's list contract and ADR 0220's invocation-team narrowing.
A list allow and a team allow combine by AND. The composed restriction flag
is the OR of the applicable list and team restriction flags. Neither a raw
caller-list entry nor membership in another role's team grants invocation
when the invocation team refuses it.

For legacy `allowed_callers`, null remains unrestricted, a populated list
uses the existing exact per-kind comparison and any matching reported caller
ID, and an empty array remains an invalid write. Existing entry validation,
normalization, duplicate handling, limits, readback and persistence checks do
not change. Editing that list continues to leave binding generation and its
channel token intact.

Preserve the current scope, identity and team predicates, their reason values,
and their order inside `check_binding`. This includes the binding-to-agent
tenant check; the no-invocation-team legacy path; and, when a team applies,
current tenant/agent activity, exact trusted identity resolution, active human
principal and configured issuer, team tenant/source/manager/readiness, and
current projected membership. Do not add a new global-denial rule to a legacy
no-team path as part of this extraction.

Slack resolution continues to use the receiving channel identity and its
installation and namespace authority. Email resolution continues to require
an exact canonical mailbox in a governed domain and the existing active
IdP-owned human link. Display authors, profile email and another tenant's or
issuer's link cannot supply authority. A complete empty team admits nobody;
a team with no completed snapshot is not ready. Invocation and approval roles
retain their existing independent authority.

At the admission query, when the team decision is restricted and Slack
evidence is present, that evidence's `user_id` must also occur in `callers`.
A second listed caller ID cannot bypass that condition.

## Existing observations, cache and transaction effects

<!-- @spec COMMON-ADMISSION-EXTRACTION-3 -->
Preserve each entry point's evaluation and observation order:

- A channel-port list refusal is observed before any team lookup, and no team
  lookup follows it. The existing router logger records one INFO refusal with
  binding, kind and reason, without caller ID or message content. The API
  increments the existing list-refusal counter once and returns exactly
  `403` with `{"detail": "caller_not_allowed"}`. A team-only refusal keeps its
  existing response without acquiring a list-refusal metric, audit or connect
  interaction.
- An admission-query list refusal is logged at DEBUG before team evaluation.
  The team is still evaluated for a bound route even when the list refused.
  The API does not add a refusal counter already owned by the dispatcher.
  Its response keeps the composed allow and restriction values.

Retain the admission query's existing team-restricted refusal audit,
including actor, principal, team, reason, epoch and digest. Existing account
connection is still eligible only for the same unlinked Slack sender, with a
list allow, sender occurrence in callers, enabled OIDC and valid HTTPS
redirect origin; token issuance, public URL construction and response omission
when no URL exists remain unchanged. A list refusal cannot gain a connection
interaction. The existing refusal audit/connect commit remains with the
router, under its current condition. The shared policy service introduces no
commit or rollback and changes no transaction or fence ownership.

Retain `Cache-Control: private, no-store`, the response's
`install_restricted` calculation from the composed restriction, any restricted
binding and any invocation team, and existing evaluation short-circuiting of
that calculation. The dispatcher admission client, identity invalidation and
cache validity behavior remain unchanged. Extracting composition does not
make a prior admission response authority for a later unchecked enqueue.

## Existing ingress ordering and refusal boundary

<!-- @spec COMMON-ADMISSION-EXTRACTION-4 -->
Keep the channel port's bounded-body and parsing steps, stateless credential
verification before database work, identity reader fence before binding read,
credential-to-binding authorization and reply-route validation in their
existing order before shared invocation composition. Keep the admission
query's authentication dependency and its reader fence before route lookup.
Neither route widens accepted credentials, route scope or tenant scope.

The port completes invocation admission before delivery claim, backlog
reservation consumption or queue write. Denial leaves the delivery unclaimed,
backlog usage unchanged and the runs stream unchanged, so a permitted retry
of that same delivery can still enqueue once. The binding-generation reread,
delivery idempotency, quota handling, claim recovery and turn/reply routing
remain with the existing ingress owner and preserve their current order.
The extraction does not assert that the existing generation reread closes a
race it currently only narrows.

Keep identity-writer/reader serialization and dispatcher cache invalidation:
a request waiting behind an identity writer reads the binding after acquiring
its reader fence and cannot retain a pre-lock allow. Team membership and
account-status changes continue to affect the existing authorization paths
through the existing freshness mechanism.

## Required preservation evidence

Tests use owned real Postgres and Valkey and synthetic scoped identities;
external transport responses may be faked. Record the final candidate and
actual commands and outcomes. Existing passing characterization is baseline
evidence, while the new shared-delegation criterion must fail against the
unchanged composition before implementation and pass afterward.

Each criterion has positive and falsifiable negative or independent-path
coverage:

| Criterion | Positive or independent path | Negative or second path |
| --- | --- | --- |
| COMMON-ADMISSION-EXTRACTION-1 | Both real API consumers invoke the same service with their existing binding/evidence profiles | Unbound admission stays open; authenticated unbound port retains refusal; neither surface invents missing identity evidence |
| COMMON-ADMISSION-EXTRACTION-2 | Null/no-team legacy route and listed current invocation-team member remain allowed | Empty-list write rejected without persistence; member/unlisted and listed/nonmember both refused; cross-scope, inactive and nonhuman team-gated principal refused |
| COMMON-ADMISSION-EXTRACTION-3 | Existing allowed response, restriction flags, eligible connection and audit details retained | List refusal preserves per-surface lookup/log/metric order; ineligible connection omitted; team-only port refusal gains no new effects |
| COMMON-ADMISSION-EXTRACTION-4 | Permitted delivery queues once and identity mutation affects the subsequent decision | Refused delivery leaves claim/quota/stream untouched and can be retried after authorization; a fence-waiting request observes the committed denial |

Existing caller-list, team-authorization, identity-connection, channel ingress
and dispatcher/API identity-invalidation tests remain the regression floor.
Check unchanged OpenAPI and import boundaries. A strictly behavior-preserving
refactor follows the repository's explicit refactor exemption from runtime-tier
qualification; it does not turn synthetic identity tests into real directory
or channel qualification. Any behavior expansion requires a new scope decision
and the applicable real-surface evidence before implementation proceeds.

## Exclusions and completion limit

This slice adds no request-time HTTP resolver, provider configuration,
versioned role filters, Slack API-owned enqueue migration, Socket Mode change,
principal-provenance field, worker authorization recheck, approval/undo/recovery
change, email sender-authentication enablement or release/deployment work.
Accepted architecture publication for subsequent architecture-changing slices
is handled separately; this specification does not declare pending ADRs
accepted.

Completion establishes shared API invocation composition with preserved
behavior only. The full capability in #4890 and existing real-directory/channel
qualification in [#4273](https://github.com/curie-eng/curie/issues/4273) remain
separate work. Code merge, artifact publication, provider qualification and
installation activation are distinct claims requiring their own evidence.
