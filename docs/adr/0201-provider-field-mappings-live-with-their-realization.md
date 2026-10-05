# 201. Provider field mappings live with their realization

Date: 2026-10-05

Status: Draft

This ADR amends
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md).

When Accepted, it supersedes exactly two clauses of ADR 0198 decision 6, and
amends ADR 0198 consequence 2's list of what the dispatcher sends (decision 2):

- **In the "Slack is normative now" list,** the field list in the first
  bullet: "keyed by the sending user's own team: the event's `user_team`, or an
  interaction payload's `user.team_id`". Which fields carry the sender's team
  becomes a realization detail (decision 1). The rest of that bullet stands: the
  namespace kind is `slack_workspace`, keyed by the sending user's own team.
- **In property 1,** the phrase "fields the provider documents as describing the
  sending user", and, within a context decision 2 establishes, its prohibition
  on deriving the namespace from fields that describe the receiving
  installation. In such a context the sender's namespace comes from the context
  itself, and a field whose meaning may be the installation's serves only as a
  consistency check. Outside such a context the prohibition stands unchanged.

Every other commitment in ADR 0198 decision 6 stands, including the Slack list's
other bullets:

- canonical global user ids, with Slack's translation layer set to deliver them;
- never using `team_id`, `source_team`, `enterprise_id` or the authorization's
  team;
- `subject_unidentified` and `namespace_conflict`;
- Slack Connect payloads without the sender's team are refused;
- Enterprise Grid users resolve per workspace.

The four properties, the Teams guidance and the email exclusion also stand.

Per [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
ADR 0198 stays `Accepted`. At acceptance it gains a back link immediately after
its `Status:` line, naming these two clauses. Its body is not edited. Its context
table and open questions name fields informatively and remain as written.

## Context

ADR 0198 decision 6 sets four properties for the function that derives a user's
identity namespace from an event: sender-bound, canonical, connection-scoped
authority, and fail-closed. Every provider except Slack is informative until its
realization adopts links. For Slack it also fixed which fields carry the
sender's team.

Before building #2910, a capture recorded the identity-bearing fields of real
Socket Mode deliveries to a Slack app in an ordinary workspace. The workspace
was not part of Enterprise Grid.

| Delivery | Field carrying a team for the sender | Other team fields |
| --- | --- | --- |
| `app_mention` | `event.team` | envelope `team_id`; `authorizations[].team_id`; `is_ext_shared_channel: false`. No `user_team`, no `source_team`, no `enterprise_id` |
| `block_actions` | `user.team_id` | `team.id`, the same value; `enterprise` present but null |
| `view_submission` | `user.team_id` | `team.id`, the same value; `enterprise` present but null |

All three values were the same workspace, the app's own. A direct message was
not captured, because another connection for the same app received every one.

Two problems follow for ADR 0198's text.

1. **The listed field is absent from an ordinary mention.** The captured
   `app_mention` carries no `user_team`. Built exactly as written, the
   realization refuses that mention with `subject_unidentified`, and with it any
   ordinary mention shaped the same way. The interactions are unaffected,
   because they carry `user.team_id`.
2. **The carrying field is undocumented.** It is `event.team`, which Slack's
   reference examples for
   [`app_mention`](https://docs.slack.dev/reference/events/app_mention/) and
   [`message`](https://docs.slack.dev/reference/events/message/) do not show.
   Property 1 requires fields "the provider documents", so using it breaches
   the ADR.

The capture also has a limit. Because every value equals the app's own
workspace, it cannot show whether `event.team` names the sender's team or the
installation's. Observation therefore cannot by itself establish what a field
means.

The underlying error is one of altitude. Which field carries the sender's team
is a fact about a provider's payloads. It is discovered by observing them, and it
can change. The properties are the architecture. Writing one provider's fields
into an Accepted ADR means every correction needs a new ADR, and ADR 0198
already avoided that for every other provider.

## Decision

### 1. Field mappings belong to the realization

The properties in ADR 0198 decision 6 stay normative for every provider. Which
payload fields satisfy them is defined by the provider's realization:

- **the code** that implements the derivation function;
- **tests** pinning each mapping to fixtures (decision 3);
- **a short note** in the provider's interface documentation. It names the
  fields used, the fields refused, the contexts each mapping covers, and the
  evidence for it.

A change to a mapping is reviewed as code against the four properties. It needs
no ADR unless it changes a property or a decision this ADR leaves standing.

### 2. Evidence, and the contexts a mapping may cover

A mapping may use a field only within the contexts its evidence covers.

- **Documented.** The provider documents the field as describing the sending
  user. The mapping covers the contexts the documentation covers.
- **Established by the context.** Some contexts determine the sender's
  namespace by themselves. In a Slack channel that is positively known not to
  be externally shared, outside Enterprise Grid, every member's team is the
  installation's team. The sender's team is then established by that context,
  not read from an installation field. Within such a context, an undocumented
  field observed to carry the team may be used as the sender field, but only
  under these conditions:
  - **Positive evidence.** The context is established from statements that
    positively say so, never from the absence of a marker. For Slack:
    - the event's envelope has `is_ext_shared_channel` present and `false`;
    - the receiving installation is known not to be Enterprise Grid, from the
      `enterprise_id` that Slack's `auth.test` returned for it when the identity
      was attached (#3039). This is a provider statement about the connection,
      not about the event.

    A payload or installation that lacks either statement is outside the
    context.
  - **Agreement.** The field is present and equal to the receiving
    installation's team. If it differs, the answer is `namespace_conflict`.
  - **The same delivery type.** The field's use is limited to the delivery
    types it was observed on. Another delivery type, such as a direct message
    when only a mention was observed, needs its own recorded payload first.
  - **No fallback.** The installation's team is a consistency check, never a
    substitute. A missing field is `subject_unidentified`, as ADR 0198 decision
    6 requires.
- **Anything else** fails closed: a context the evidence does not cover, or one
  the payload does not let the code establish. An observation-only mapping
  extends to a new context, such as a Slack Connect channel, only when a
  capture shows the sender's team distinctly from the installation's and the
  channel's. A documented field covers whatever contexts its documentation
  covers.

The context fields a mapping needs travel with the sender fields to the
derivation function, which is where the checks run. This amends the list in ADR
0198 consequence 2 of what the dispatcher sends: the identity name, the sender's
team fields and the user id, plus each mapping's context fields.

### 3. Fixtures

Recorded payloads become test fixtures only after anonymisation.

- **Placeholders:** every real identifier (team, user, enterprise, channel and
  app ids) is replaced with an obvious, consistent placeholder, such as
  `T_SENDER` or `U_ALICE`.
- **What to keep:** which fields are present, and which values are equal to or
  different from each other.
- **What to remove:** message content, names, tokens and any other private
  metadata.
- **Review:** the exact fixture text is reviewed before it is committed.

### 4. Slack is an ordinary realization

Slack's sender-team field list moves out of ADR 0198 and into #2910's
realization under decisions 1 to 3. This ADR does not fix it. The capture above
is its first evidence.

## Consequences

1. **#2910** defines Slack's mapping in code, with:
   - anonymised fixtures from the capture;
   - executable context checks, with negative tests for each refused context:
     externally shared, a sender team that disagrees, Enterprise Grid, a missing
     field, and a context the payload cannot establish;
   - an interface note.

   It is reviewed against ADR 0198's properties.
2. Under decision 2, the capture supports:
   - `event.team` for `app_mention` deliveries in a channel positively marked as
     not externally shared, outside Enterprise Grid, and only there;
   - interactions using `user.team_id`, which ADR 0198 already names as the
     interaction payload's sender field. They take the documented path and are
     not gated on context.

   Direct messages stay unresolved until a direct-message payload is recorded.
3. ADR 0198 consequence 6, that Slack Connect users resolve when the payload
   carries their own team, is subject to decision 2. They resolve through a
   documented sender field or a capture that shows their team distinctly.
   Otherwise they stay unresolved.
4. Each later provider's mapping, and any correction to Slack's, is a code
   change reviewed against the properties, not a new ADR.
5. ADR 0198 stays Accepted, with a back link naming the two superseded clauses
   and the amended consequence 2.

## Alternatives considered

1. **Amend ADR 0198 with a corrected Slack field list.** This fixes today's
   error. Rejected: the next payload difference, or the first Teams mapping,
   would need another ADR, while the properties already carry the intent. Email
   is separate: ADR 0198 requires its own decision on mailbox evidence, and that
   stands.
2. **Implement `event.team` under ADR 0198 as written.** Rejected: it
   contradicts an Accepted decision's text.
3. **Accept only documented fields.** Rejected: Slack's reference examples omit
   the field that real payloads carry, which leaves ordinary mentions
   unresolvable.
4. **Accept any field once observed.** Rejected: an observation in which the
   sender's, channel's and installation's teams coincide cannot show which one a
   field names. Decision 2 allows the field only in contexts where that
   ambiguity does not matter, and checks the context in code.

## Tracking

Realized by #2910, which defines Slack's mapping under decisions 1 to 3.
