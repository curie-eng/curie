# 261. HTTP identity resolves Slack Connect senders from operator-trusted partner namespaces

Date: 2026-11-02

Status: Accepted

Accepted 2026-11-02 under ADR 0102 coordinated acceptance with explicit
approval by Junwon Jung (jw3329), including merging alongside its realizing
code path in one combined pull request. The realizing code path is
`curie_api.identity.slack.derive_registered_sender`,
`curie_api.identity.http_invocation.ReceiverRegistrationV1.trusted_namespaces`
with `recorded_namespace` and `capture_source`, and the dispatcher evidence
copier `curie_channel_gateway.slack.identity.slack_evidence`. The CODEOWNERS approving review by TheConnMan was
not obtained before merge.

## Context

A registered Slack receiver attributes a sender only where the context
establishes whose namespace the sender belongs to. On an ordinary workspace
receiver that is a channel marked not externally shared (ADR 0201 decision 2).
On an Enterprise Grid workspace receiver it is the organisation, again only in
a channel marked not externally shared ([ADR 0252](0252-http-identity-resolves-enterprise-grid-senders-in-their-organisation.md)
decision 5). Every Slack Connect channel is refused on both.

Teams use Slack Connect channels to work with partner organisations, and an
agent bound in such a channel then cannot use HTTP identity for anyone, its
own organisation's members included. Two things are missing. Nothing says
which external workspaces or organisations the operator is willing to have the
provider answer for. And nothing says how to read a Connect payload's team
fields, which name different things depending on the receiver and the sender:
`user_team` names the sender's home team, `source_team` the team the event
originated from, and `event.team` sometimes the sender's team and sometimes
the receiving one.

## Decision

### 1. Trusted partner namespaces are operator authority

A Slack receiver registration gains an optional list of trusted partner
namespaces. Each entry is a namespace descriptor with empty authority, an
operator-chosen id, and either:

- kind `slack_workspace`, key a workspace team id; or
- kind `slack_enterprise`, key a Grid organisation id. Only an ordinary
  workspace registration may list these.

Keys and ids are unique. No entry repeats the registration's own namespace or
installation account. An absent or empty list is today's registration. The
list lives in the protected startup file and changes only by the coordinated
process replacement every registration change needs. No identity namespace row
is created or read for an entry.

### 2. A Connect mention needs an established home team

A mention with `is_ext_shared_channel` exactly `true` is attributed only when
the user id is present and the sender's home team is established:

- a present `user_team` is the home team, and a present `source_team` must
  equal it;
- with `user_team` absent, `source_team` must be absent too and `event.team`
  must equal the installation account, which is then the home team.

Anything else is refused: an unobserved shape as `subject_unidentified`,
disagreeing team fields as `namespace_conflict`. An absent shared flag stays
refused, as today.

### 3. The home team selects the namespace

On an ordinary workspace registration with no enterprise signal in the
payload:

- the installation account resolves in the registration's own namespace;
- a trusted `slack_workspace` key resolves in that entry;
- anything else is `namespace_conflict`.

On an ordinary registration that trusts at least one organisation, a payload
carrying any enterprise id is attributed to an organisation instead. Every
enterprise id must be the same trusted organisation, the organisation-wide
install signal must be absent, and the home team must not be the installation
account. The sender resolves in that organisation's entry with Slack's
organisation-wide user id, unchanged. A registration that trusts no
organisation keeps refusing every enterprise signal.

On a Grid workspace registration, ADR 0252's enterprise checks come first.
A home team equal to the installation account resolves in the registration's
own organisation; a trusted `slack_workspace` key resolves in that entry;
anything else, including another workspace of the same organisation, is
`namespace_conflict` until a capture shows how Slack names such a member in a
Connect channel.

### 4. Answers follow the same set

Interaction payloads carry no shared flag. An answer keeps the documented
sender field `user.team_id` (ADR 0201) as its home team. Without an
enterprise id it resolves when that team is the installation account or a
trusted workspace key, in the matching namespace. On an ordinary registration
that trusts an organisation, an answer carrying any enterprise id resolves in
that organisation's entry, with the organisation-wide user id unchanged, when
every enterprise id is the same trusted organisation, the organisation-wide
install signal is absent, and `user.team_id` is present and is not the
installation account. A member's `user.team_id` names their own workspace in
the organisation, never the organisation id, so any workspace of that
organisation qualifies. A missing `user.team_id`, the organisation-wide
install signal, or any enterprise id on a registration that trusts no
organisation is `subject_unidentified`; anything else is `namespace_conflict`.

One answer shape on a Grid workspace registration may differ. A partner
member's click on a card in a Connect channel hosted in the organisation is
expected to carry the organisation's enterprise id, but the `view_submission`
of a modal opened from that click is a separate payload whose team fields can
describe the submitter rather than the channel, with no enterprise id at all.
No capture of that shape exists in this repository; its live qualification
remains outstanding. Such a submission resolves in the trusted
`slack_workspace` entry when all of these hold: it is a `view_submission`,
the user id is present, no enterprise id is present, and `user.team_id` is a
trusted workspace key. Every other Grid delivery without an enterprise id
stays refused as `subject_unidentified`, including the same shape from the
installation workspace or from an untrusted team, and every `block_actions`.
The approver role, not this rule, still decides whether that person may
answer.

### 5. Every later check uses the same set

The worker's current check, a sealed answering actor and the original
association accept a recorded namespace only when it equals the
registration's own namespace or one of its trusted entries exactly. Removing
an entry refuses its members' later checks after the next process
replacement.

### 6. Evidence and contract

The receiver's signed Slack evidence gains an optional `event_source_team`,
serialised only when present, so every delivery without it signs exactly the
bytes it signs today. The provider contract does not change: the namespace
stays an opaque descriptor and the native id is passed through unchanged.

## Consequences

An agent in a Slack Connect channel can use HTTP identity for its own
organisation and for partners the operator names, each in its own namespace.
A partner's member reaches the provider in the partner's namespace, never in
the installation's, so a provider that does not know that namespace answers
unresolved rather than matching an unrelated local account.

Trust is per registration and explicit. A partner that the operator has not
listed stays refused, and so does any shape this ADR does not describe.

Interactions from an external workspace have not been captured. The rule for
answers is the narrowest one consistent with the documented field, and live
qualification of it remains open.

## Alternatives considered

1. **Resolve every Connect sender in the installation's namespace.** Would let
   a partner's member match a local account with the same native id, and
   gives the provider no way to tell organisations apart.
2. **Trust every external workspace and let the provider decide.** Sends
   every partner's members to the provider and turns its answer into the
   only boundary. The operator's list keeps the provider's input bounded.
3. **Store trusted namespaces in database rows.** Adds a second declaration
   that can disagree with the startup file and a new write surface.
4. **Read `event.team` as the sender's team in every shape.** In a Connect
   channel it can name the receiving workspace, so it is read only when
   `user_team` and `source_team` are both absent.
