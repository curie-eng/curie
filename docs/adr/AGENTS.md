# ADR contributor procedure

[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md), as
amended by [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
defines when an ADR authorizes implementation. [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
defines how Accepted ADR history is preserved.

The ADR requirement applies only to product and platform architecture decisions
that establish or materially change durable system boundaries. CI configuration,
build plumbing, and similar delivery mechanics are outside this requirement
unless they also make such an architectural decision.

An ADR records structure only. The behavior of a single example bundle or
agent (its skill text, prompts, tool policy, test or verdict rules, safety
rules, or operator guidance) is not architecture and never gets an ADR. It
lives in that bundle's README or `docs/` and is tracked in issues. If a bundle
needs a platform change, write the ADR about the platform contract alone, so
that it holds for every bundle.

## Status vocabulary

1. `Draft` is open for discussion and revision. It may merge, but it does not
   authorize implementation.
2. `Accepted` means the decision is published with an Accepted status and has
   explicit maintainer approval. Only this status authorizes implementation.
3. `Superseded` is the terminal historical status for an Accepted decision that
   a later ADR replaces in whole.

## Procedure

1. Create the ADR as `Draft` with the next unused four digit number. Record the
   context, decision, consequences, and rejected alternatives.
2. Merge a Draft when publishing it for discussion is useful. Do not begin
   implementation from a merged Draft.
3. Obtain explicit maintainer approval, then publish the status as `Accepted`.
   Existing code and implementation evidence cannot retroactively accept a
   Draft.
4. Acceptance is the authority for implementation. Normally, publish the ADR as
   `Accepted` before beginning implementation. As the coordinated exception, an
   ADR may land `Accepted` alongside implementation only with recorded explicit
   maintainer approval and a named realizing code path. Without both, acceptance
   must precede implementation. Track the work in a linked GitHub issue and pull
   request.
5. Revise a Draft through review. Once Accepted, preserve its body. Apply only
   the status, back link, pointer repair, and supersession rules in ADR 0045.
6. Run `bash scripts/check-docs.sh` after any ADR change. Commit the regenerated
   `docs/adr/README.md`; never edit that index by hand.
