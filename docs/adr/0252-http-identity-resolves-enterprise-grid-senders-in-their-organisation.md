# 252. HTTP identity resolves Enterprise Grid senders in their organisation

Date: 2026-10-27

Status: Accepted

Accepted 2026-10-27 with maintainer approval from jw3329, recorded as a
comment on the publishing pull request
([#5109](https://github.com/curie-eng/curie/pull/5109)).

Tracked in [#4890](https://github.com/curie-eng/curie/issues/4890).

This ADR amends [ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)
and [ADR 0201](0201-provider-field-mappings-live-with-their-realization.md)
for the HTTP identity path of
[ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
only. It supersedes these clauses in that scope:

- **ADR 0198 decision 2, its Slack kinds sentence** ("For Slack, the kind is
  `slack_workspace` and the key is a `T…` id"). Slack gains a second kind,
  `slack_enterprise` (decision 2 below).
- **ADR 0198 decision 6, the Enterprise Grid bullet** of the Slack list, that
  Grid users "resolve in their own workspace's namespace". On the HTTP path a
  Grid sender resolves in the organisation's namespace (decision 2).
- **ADR 0198 open question 1** is settled for the HTTP path by decision 2. It
  stays open for local links.
- **ADR 0201 decision 2's Slack context, and its list of standing commitments,
  in the item "Enterprise Grid users resolve per workspace".** A second
  context, the Grid workspace installation, is added (decisions 4 and 5).

Everything else in ADR 0198 and ADR 0201 stands. That includes the four
properties of ADR 0198 decision 6, canonical global user ids, the
`subject_unidentified` and `namespace_conflict` reasons, the refusal of Slack
Connect payloads without the sender's team, and ADR 0201's rule that a field's
use is limited to the delivery types it was observed on. Local resolution
through identity links, `POST /identity/resolve` and the projection connect
flow keep refusing every Grid signal; this ADR changes none of them.

ADR 0246 is not amended. Its rule that "trusted installation and credential
scope determine the tenant, issuer, binding and sender namespace" is what
decisions 3 to 5 apply to Grid.

## Context

ADR 0246 adds an optional HTTP identity mode. A selected Slack receiver signs
what it saw on its own Socket Mode connection, the API derives the sender from
that evidence and the operator's receiver registration, and an external
provider answers with identity facts for `(namespace, native id)`. The initial
realization (HTTP invocation execution v1) accepts only an ordinary workspace:
every Enterprise Grid signal refuses, at the receiver's startup `auth.test`,
in the registration's namespace check and in the Slack sender derivation.

A workspace that moves into an Enterprise Grid organisation therefore loses
HTTP identity entirely. Two Accepted decisions stand in the way:

- ADR 0198 decision 6 says Grid users resolve per workspace and leaves folding
  an organisation into one namespace as open question 1.
- ADR 0201 decision 2 establishes the sender's team from context only "outside
  Enterprise Grid".

### What Slack says, and what is measured

Slack documents that "within an Enterprise org, all users have a single,
global ID" ([developing for enterprise orgs](https://docs.slack.dev/enterprise/developing-for-enterprise-orgs/)).
That page is not an observation. The behaviour this ADR relies on is measured
in the pinned Slack libraries in the workspace lock, `slack_bolt` 1.30.0 and
`slack_sdk` 3.45.0 (`uv sync --frozen --all-packages`, then reading the
installed sources):

- `slack_bolt/request/internals.py` line 76, `extract_actor_enterprise_id`:
  only when `is_ext_shared_channel` is `True` does Bolt treat the acting user's
  organisation as possibly different from the payload's. Otherwise it returns
  the payload's own enterprise id (line 87). Bolt's model is that outside a
  Slack Connect channel the actor belongs to the receiving organisation.
- Same file, line 126: "The `$.event.user_team` can be an enterprise_id in
  app_mention events", and line 135, from a connected Grid workspace "team_id
  cannot be resolved". A team field on a Grid mention can name an organisation
  rather than a workspace, so it cannot key a workspace namespace.
- Same file, line 99: "With org-wide installations, payload.team in
  interactivity payloads can be None". An organisation-wide install has no
  single receiving workspace.
- Same file, lines 44 and 55: Bolt's context takes `is_enterprise_install` and
  `enterprise_id` from `authorizations[0]` first, then the envelope.
- `slack_bolt/middleware/authorization/single_team_authorization.py` line 66
  and `slack_bolt/middleware/authorization/internals.py` lines 65 to 73: a
  token app's `AuthorizeResult` copies `enterprise_id` and `team_id` from the
  identity's own `auth.test`. `slack_bolt/context/base_context.py` line 177,
  `set_authorize_result`, does not overwrite the payload-derived
  `enterprise_id`, `team_id` or `is_enterprise_install`. `slack_sdk`
  `WebClient.auth_test` (`slack_sdk/web/client.py` line 2530) returns Slack's
  answer unparsed, so its fields are Slack's.

No Enterprise Grid organisation is available here, so no real Grid delivery or
`auth.test` answer has been captured. Every Grid fixture is synthetic, and
live Grid qualification is a Discovery waiver on #4890 until an owned Grid
installation records one.

## Decision

### 1. Scope

This ADR applies to an operator-registered HTTP receiver (ADR 0246) and to the
API's derivation of that receiver's sender. Non-Grid registrations, evidence
and outcomes are unchanged byte for byte. Local link resolution keeps ADR 0198
decision 6 as written, including its refusal of every Grid signal.

### 2. The namespace is the organisation

Under Grid the sender's identity namespace is the Grid organisation:

- kind `slack_enterprise`, authority empty, key the organisation id that the
  receiving identity's own `auth.test` reported;
- native id: the user id from the payload, Slack's organisation-wide id,
  unchanged and unnormalised, as ADR 0198 decision 6 already requires.

This is Slack's uniqueness scope, so no workspace-membership evidence is
needed to fold workspaces together: within the organisation one id is one
person. A per-workspace key is rejected because a Grid mention's team field can
carry an organisation id (Bolt line 126) and because channels can be shared
between workspaces of one organisation.

### 3. Only a workspace installation inside an organisation

A Grid receiver is a workspace-level installation inside one organisation.
Its identity's own `auth.test` must report:

- `team_id`: the registered installation account, a workspace;
- `enterprise_id`: a string, equal to the registered namespace key;
- `is_enterprise_install`: the strict boolean `false`.

An organisation-wide installation (`is_enterprise_install: true`) stays
refused at receiver startup and in every derivation: it receives deliveries for
many workspaces and, per Bolt line 99, interactions without a receiving team.
Supporting it needs its own capture and decision.

### 4. Registration and binding scope

The receiver registration keeps its fields. Its installation account and its
`ProviderInstallation.external_account_id` are the workspace's team id, since
the workspace is what the identity is installed to. Its namespace is the
organisation's `slack_enterprise` namespace, and that namespace's key is the
only place the organisation id is registered. The registered organisation must
differ from the workspace. Binding scope is unchanged: bindings still belong to
one channel identity, so to one workspace installation. A delivery's envelope
`team_id` must still equal the registered workspace.

### 5. Attributing a mention

An `app_mention` from a Grid workspace installation resolves only when:

- **the context establishes the organisation.** The envelope has
  `is_ext_shared_channel` present and `false`, and the installation is
  positively the registered Grid workspace (decision 3). Slack Connect is the
  only way a channel admits members of another organisation, and it sets that
  flag. Every member of such a channel, including one from another workspace
  of the same organisation, is therefore a member of the organisation. This is
  ADR 0201 decision 2's context rule applied at organisation level;
- **every enterprise signal agrees.** At least one enterprise id is present in
  the payload, and every one equals the registered organisation. None present
  is `subject_unidentified`; a different one is `namespace_conflict`; an
  organisation-wide install signal is `subject_unidentified`.

The team fields (`user_team`, `team`) are neither the namespace nor a check on
this path, because under Grid they can name a workspace or the organisation.
A person from another workspace of the same organisation who mentions the bot
in a channel shared between those workspaces is attributed to the organisation
namespace by their organisation-wide id. Other delivery types remain refused
until a Grid payload of that type is recorded.

### 6. Attributing an interaction

Interaction payloads (`block_actions`, `view_submission`) carry no shared
channel flag, so the context of decision 5 cannot be established. An answer
from a Grid workspace installation resolves only when the documented sender
field `user.team_id` equals the registered workspace, which the workspace's
own `auth.test` places in the organisation, and every enterprise signal equals
the registered organisation, with at least one present. A different
`user.team_id` is `namespace_conflict`, even when it may be another workspace
of the same organisation; this version cannot tell that apart from an outside
organisation.

### 7. Outside organisations stay refused

Slack Connect and every other external-organisation sender remain refused on
the Grid path: a mention with `is_ext_shared_channel` true or absent, any
enterprise id other than the registered organisation, and an interaction whose
`user.team_id` is not the registered workspace. ADR 0198 open question 2, how
Slack reports a Connect sender's team, stays open.

### 8. The HTTP facts contract is unchanged

`http-identity-v1` carries `namespace.kind` and `namespace.key` as opaque
strings, so its wire format, version and vectors do not change. A Grid sender
reaches the provider as `{kind: "slack_enterprise", authority: "", key: <the
organisation id>}` with the organisation-wide user id. A provider must map that
pair before a Grid tenant activates; one that does not know the kind answers
unresolved or fails, and either refuses through the existing outcomes.

### 9. Storage

`identity_namespaces.kind` gains `slack_enterprise`, keyed by an `E…` id with
provider `slack`, in an expand-only migration. ADR 0198 decision 2 creates an
installation's home namespace when the installation is connected. For a Grid
workspace installation that home is also the organisation: when an identity's
`auth.test` report names a string `enterprise_id` and `is_enterprise_install`
false, attaching it additionally finds or creates that organisation's
namespace. The workspace namespace is still created and returned, so the
report's answer is unchanged. Creating the row grants nothing: only an
operator's receiver registration names it. The local directory projection
does not accept the new kind. Resolving an event never creates a namespace, as
before.

## Consequences

1. The HTTP invocation execution specification states the Grid registration,
   receiver startup and derivation rules; the HTTP approval continuity
   specification's answer rule uses the same derivation.
2. A Grid tenant's activation additionally needs a provider that maps the
   `slack_enterprise` namespace, and a live Grid capture of a mention, an
   `auth.test` answer and an interaction. Until then live Grid support is
   unqualified (Discovery waiver on #4890).
3. People who mention the bot from another workspace of the organisation
   resolve; people who answer approvals from another workspace do not yet.
4. Existing ordinary installations, local mode and every non-HTTP path behave
   exactly as before.

## Alternatives considered

1. **Key Grid senders by their own workspace, as ADR 0198 decision 6 says.**
   Rejected for the HTTP path: Bolt shows a Grid mention's team field can name
   the organisation, so the workspace is not reliably reported, and channels
   shared between workspaces would refuse their members or misplace them.
2. **Key Grid senders by the receiving workspace.** Rejected: it places a
   sender from another workspace of the organisation in a namespace they may
   not belong to, which is the cross-namespace guess ADR 0198 forbids.
3. **Support organisation-wide installations now.** Rejected: there is no
   single receiving workspace to register, and interactions may carry no team.
4. **Read the organisation from the payload instead of the registration.**
   Rejected by ADR 0198 property 3 and ADR 0246: scope comes from the trusted
   installation; payload enterprise ids are only a consistency check.
5. **Add an `enterprise_id` field to the registration and proof.** Rejected:
   the registered namespace key already names the organisation, and the proof
   already carries every enterprise signal. A new field would change published
   receiver shapes for no new information.

## Open questions

1. Organisation-wide installations.
2. Approval answers from other workspaces of the organisation, which need a
   sender field that names the user's organisation.
3. Direct messages and other delivery types from a Grid installation.
4. Folding a Grid organisation for local identity links.
