# 239. Release kind proof reuses a complete CI run on the same tree

Date: 2026-10-21

Status: Accepted

Explicit maintainer approval was recorded in
[#4863](https://github.com/curie-eng/curie/issues/4863) on 2026-10-21.
Under the coordinated exception in
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
this ADR lands Accepted alongside its realizing code path:
`release/kind_ci.py`, called by `release-kind-tier` in
`.github/workflows/release.yaml`.

## Context

Tag publication requires full kind proof. Dispatching CI again repeats that work
when a complete successful run already covers the tagged tree. Overall CI
success alone is insufficient: push runs omit kind and PRs can run only a smoke
upgrade shard. A transient API failure while watching a child run can also fail
the release independently of that child's result.

## Decision

Reuse one completed, successful `.github/workflows/ci.yaml` run only when its
head commit's tree exactly equals the tagged commit's tree and every required
job completed successfully. Different commit SHAs may have the same tree.
Inspect all workflow-run and job pages; never combine proof from multiple runs.
Log the selected run URL and whether proof was reused or dispatched.

The required concrete jobs cover:

1. Rust convergence regressions and the cluster parity ladder.
2. Both chart runtime legs, `chart` and `workload`, and approval resume after
   worker and store restarts.
3. All scripted factory suites: `scenario`, `initial-binding`,
   `static-token-rotation`, and `fresh-install`.
4. All five admitted agent runtime shards.
5. The released chart upgrade negative control, the upgrade shard listing,
   and `E2E required`.
6. Every canonical upgrade matrix shard: currently `s01 s02 s03 s04 s05 s06
   s07 s08 s09 s11 s13 s14 s15 s16 s17 s18 s19 s20 s21`, totaling 19.

Derive the shard set from `cli/scripts/cluster-upgrade-matrix.sh
--list-shards --json` with `CURIE_E2E_SHARDS_OVERRIDE` cleared. Invalid or
unavailable required input is an error. Missing, skipped, incomplete, conflicting,
or unsuccessful required evidence refuses reuse. Nightly-only jobs are outside
the existing no-input tag dispatch contract and may be skipped.

If no run qualifies, preserve the existing `ci.yaml` dispatch POST on the tag,
with no workflow inputs. Poll status GETs for its returned run ID. Retry
transient HTTP 5xx responses within one bounded monotonic deadline; only
completed success passes. Other terminal conclusions and deadline expiry fail.
Do not retry an ambiguous dispatch POST.

Release authorization remains a separate, unchanged gate. This decision does
not change `release/authorize.py`, its ancestry rules, or publication dependencies.

## Consequences

Complete proof can avoid another kind run while retaining its inspectable URL.
The selector must track required concrete job names and the canonical shard set.
Incomplete proof still incurs the full existing dispatch and wait.

## Rejected alternatives

1. Accept overall success or `E2E required` alone. Neither proves every job ran.
2. Require the same commit SHA. Equal trees can occur at different commits.
3. Always dispatch again. This repeats already complete proof.
4. Treat a watcher API error as the child result. Status polling must observe
   the actual terminal conclusion within its deadline.
