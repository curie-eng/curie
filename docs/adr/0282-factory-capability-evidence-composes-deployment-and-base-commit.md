# 282. Factory capability evidence composes the deployment record and base commit

Date: 2026-11-17

Status: Draft

Issue #4485 proposes this decision. Architecture review and explicit maintainer
approval are required before implementation. The intended realizing paths are
the runner image probe and bundle build in `cli`, the deployment record and
authenticated read in `apps/api`, and the existing factory admission seam in
issue #4450.

This decision amends [ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
decision 3 only where it says that the *resolved* verification declaration is
recorded at deployment. It preserves the closed-world manifest, the existing
verification precedence, and [ADR 0204](0204-the-factory-checks-its-environment-before-it-admits-a-ticket.md)'s
environment-unknown disposition. It builds on [ADR 0216](0216-a-factory-repository-binding-selects-its-own-runner-image-built-from-a-tool-catalog.md)'s
repository runner probe contract.
It also composes with [ADR 0286](0286-one-labelled-ticket-the-factory-investigates-asks-and-resumes-on-a-plain-answer.md),
which leaves ADR 0199's deferred viability gate unchanged while defining how a
run asks for a human answer on the same ticket.

## Context

A deployment fixes its bundle; each factory repository binding can select a
different runner image. The ticket's resolved base commit is not known when
the bundle is deployed. If the bundle has no checks,
`.curie/verification.json` belongs to that repository and may differ between
base commits. Recording its resolved contents at deployment would either read a
moving branch or attribute one repository's checks to another. Both break the
immutable evidence required by
ADR 0199.

The repository runner already carries a probed executable manifest at
`/etc/curie/tool-manifest.json`. That probe is evidence about the image, not a
grant of network access or an assertion about repository checkout files.

## Decision

**The capability evidence used for one factory admission is the composition of
an immutable deployment record and, only when needed, a verification declaration
read from that ticket's exact base commit. The composition is recorded with the
admission evidence.**

1. **Deployment record.** Build produces a canonical, versioned capability
   manifest from the actual image probes and declared bundle inputs. It names
   the selected image and bundle digests, executable probe commands and observed
   versions, declared registry hosts, reachable services and connectors,
   bundle-owned check IDs and routes, egress class, and the platform's exact
   `.github/` publication restriction. The deployment record for each active
   factory binding stores the exact manifest bytes and their digest, bound to
   that bundle, image, and runner identity. An authenticated read returns those
   bytes. No other image's tool list can supply missing evidence. A registry
   host is descriptive and never widens egress or credentials.
2. **Verification precedence.** Bundle checks, when present, are part of the
   deployment record and take precedence. Otherwise admission reads
   `.curie/verification.json` from the immutable base commit selected for the
   ticket, through the repository's code-host authority. An absent file means
   no declared checks; an unreadable or malformed file means unknown. The
   effective view records the base SHA and the resolved declaration, including
   each check's route. A later branch tip or runner checkout cannot change it.
3. **Identity and uncertainty.** The read validates schema, digest, deployment
   version, image, bundle, and runner identity against the active binding.
   Missing, malformed, mismatched, or stale evidence is environment unknown.
   Existing deployments remain readable as missing evidence. A new deployment
   record does not change a running execution or an older binding's evidence.
4. **One admission gate.** This record and read supply evidence to the gate in
   issue #4450; they do not start, reject, or label a ticket themselves. The
   platform reports active factory bindings that lack a matching record, with
   a build, render, and redeploy action for each. Rebinding a runner image alone
   does not establish a bundle-bound record.

## Consequences

1. A deployed factory binding has one stable capability record, while
   different tickets can have different repository check declarations without
   rebuilding the bundle.
2. Admission evidence can be reproduced from a deployment identity and exact
   base SHA. Failure to read either source cannot silently become a no-check
   success or a ready verdict.
3. Operators must regenerate and redeploy legacy bindings before the later
   manifest-based admission consumer can judge new tickets ready. Existing
   executions continue under their original version and ADR 0199 revision path.

## Rejected alternatives

1. **Resolve repository checks at deployment.** A factory agent can serve many
   repositories and the ticket's base commit is not yet fixed. A branch-tip
   read would make the evidence mutable and can select the wrong repository.
2. **Read the declaration from the runner checkout.** The checkout may be newer
   than the ticket's base. It would let a later edit change an earlier
   admission decision.
3. **Infer a legacy manifest from a tool list or image name.** These are not
   probes of the deployed image and are not bound to its bundle. They can assert
   capabilities that were never verified.
4. **Add a separate manifest admission gate.** A second gate could disagree
   with ADR 0199's single verdict and duplicate ticket side effects. The
   manifest is evidence for the existing gate.
