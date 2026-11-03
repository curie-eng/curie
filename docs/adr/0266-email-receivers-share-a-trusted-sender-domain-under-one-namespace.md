# 266. Email receivers share a trusted sender domain under one namespace

Date: 2026-11-02

Status: Accepted

Accepted 2026-11-02 under ADR 0102 coordinated acceptance with explicit
approval by Junwon Jung (jw3329), including merging alongside its realizing
code path in one combined pull request. The realizing code path is
`curie_api.identity.http_invocation._email_registrations`, reached through
`curie_api.identity.http_initializer.load_http_identity_runtime`, and the
mail adapter's signing path `curie_mail_adapter.adapter.MailAdapter.post_turn`
with `curie_mail_adapter.config.MailAdapterConfig`. The CODEOWNERS approving
review by TheConnMan was not obtained before merge.

Amends [ADR 0260](0260-http-identity-admits-an-email-sender-only-on-a-signed-email-receiver-proof.md)
decision 1. It supersedes in part exactly one sentence of that decision:
"Keys, domains and ids are unique across the file." Everything else in ADR
0260 stands.

## Context

ADR 0260 registers each mail adapter as an email receiver with its own key,
its own email bindings and one to sixteen trusted sender domains. Each domain
is a namespace descriptor, and the verified sender becomes
`{provider: "email", namespace: <that descriptor>, native_id: <address>}`.
The provider resolves a person by namespace and native id.

ADR 0260 also requires domains and their namespace ids to be unique across the
whole runtime file. An organisation that runs several mailboxes, each its own
adapter, binding and agent, cannot register them. Every mailbox trusts the
same company domains, so the second registration that lists `corp.example.test`
refuses API startup.

Giving each mailbox its own namespace id for the same domain would load, but
it splits one person into one principal per mailbox they write to. Their
identity facts, role matches and approvals would depend on which address they
happened to mail. That is the wrong model. A sender namespace describes the
person's domain. The binding and the receiver describe the mailbox.

The uniqueness rule was not what made admission safe. Admission binds a proof
to its receiver through that receiver's own key, and to its binding through
the registration that lists the binding, before any domain is consulted. A
domain listed by two receivers does not let either receiver's key speak for
the other's bindings.

## Decision

### 1. A domain may be trusted by several email receivers

Two or more email receiver registrations may list the same sender domain. When
they do, every listing carries the same namespace descriptor: the same key and
the same namespace id. A person therefore resolves to one principal whichever
registered mailbox they write to.

### 2. Only contradictions refuse

The runtime file refuses API startup when:

- one domain key is listed with two different namespace ids;
- one namespace id is listed for two different domain keys;
- an email namespace id equals any Slack namespace id, a registration's own or
  one of its trusted entries.

Within one registration, domain keys and namespace ids stay unique.

### 3. Every per-receiver rule stays

Receiver ids, channel identity ids and provider installation ids stay unique
among email registrations and distinct from every Slack registration's. An
email binding id still appears in exactly one email registration and in no
Slack registration. Each email key still differs from every other receiver and
context key and from the platform key. Admission still requires the proof to
verify under the key of the registration that lists the binding, so a shared
domain never lets one mailbox's key admit a turn on another mailbox's binding.

### 4. Curie's own mail adapter signs when configured

Curie's built-in mail adapter takes an optional receiver id, generation and
key. With all three it signs every turn it posts with the shared
`sign_email_receiver_proof` over the exact bytes it sends, as ADR 0260
decision 2 requires. With none it posts exactly as before, so a binding on the
legacy path is unchanged. A partial setting refuses adapter startup.

## Consequences

An organisation can run several mailboxes behind HTTP identity, each with its
own key and bindings, while a sender keeps one identity across all of them.

The operator now keeps one domain to namespace mapping consistent across
registrations. A typo that gives a domain a second namespace id refuses
startup instead of splitting principals silently.

Rotating or removing one mailbox's registration does not touch another's. A
shared domain is only a shared name for the person, never shared authority.

## Alternatives considered

1. **Keep the uniqueness rule and give each mailbox its own namespace ids.**
   Rejected: one person becomes several principals, and their facts and role
   matches depend on which mailbox they write to.
2. **One registration for every mailbox, with all bindings and one key.**
   Rejected: every adapter would hold every mailbox's key, so one compromised
   adapter could admit turns on all of them.
3. **A separate top-level list of trusted email domains that registrations
   reference.** Equivalent in effect, but it changes the runtime file shape
   that operators and ADR 0260 already rely on. Checking consistency across
   the existing per-registration lists keeps the file unchanged.
