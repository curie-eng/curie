# 209. Pause authority after a drain is a lease held by a live upgrade step

Date: 2026-10-07

Status: Accepted

Accepted 2026-10-08.

## Context

The pre-upgrade drain gate (#2010) quiesces the worker fleet with an installation-scoped marker
(#2374), waits for leased deliveries to settle, and on a clean drain writes a roll hold
(`upgrade_quiesce_ttl_s`, capped by the chart at the effective drain wait, 1800 s by default) so
the roll does not interrupt newly claimed work. Only the post-upgrade release clears it. Helm runs
no hook when a pre-upgrade hook fails or when the roll fails, so after a clean drain any later
failure leaves a healthy-looking fleet that claims nothing for the whole roll hold. Round 5 of
the factory resilience run measured exactly 1800 s of zero dispatch after schema-migrate failed.
#3127 already made the waiting phase a renewed lease; this extends the same rule past the drain.

## Decision

1. Hooks that do not depend on the drain run before it. The pre-upgrade variant of
   preflight-gvisor and the mail-adapter-persistence hook resources move to weights below the
   drain (-12 and -11; the RBAC objects keep their relative order). Nothing that can fail for a
   reason unrelated to the drain runs while the fleet is paused.
2. A clean drain hands off a lease, not the roll hold. `await_drained` writes the marker with
   TTL H = 300 s on success instead of `upgrade_quiesce_ttl_s`. 300 s is the chart's existing
   budget for one hook Job's scheduling and worker image pull (attest and release
   `activeDeadlineSeconds`), which is the longest gap no process can renew across.
3. Renewal is renew-only. A new Lua in `upgrade_drain.py` extends the TTL of the authoritative key
   (and the legacy bridge key when applicable) only when it holds a marker of exactly this
   revision; it never creates a marker. A renewal that finds no marker, or a different revision,
   means pause authority was lost and the step refuses the upgrade (exit 1): nothing is rolled
   over a fleet that may have resumed.
4. Every step after the drain renews while it is alive:
   a. attest renews once after a successful attest (renew-only, so it still never creates a
      marker; #3360's rule that attest cannot stand in for a drain holds).
   b. schema-migrate renews from inside the migrate process. The renew-only write and the key
      derivation move to a small shared module installed in both the worker and API images, and
      `curie_api.schema_compat upgrade` runs a renewal task every H/3 = 100 s for its whole life,
      started before the Postgres readiness wait. The chart passes the drain's Valkey wiring,
      installation ID, revision and legacy flag to the migrate Job on upgrade only
      (`.Release.IsUpgrade`); post-install renders nothing new. When the process exits for any
      reason (success, failure, SIGKILL, OOM), renewal stops with it.
5. A new last pre-upgrade hook writes the roll hold. `upgrade-drain-hold` (worker image, weight 10,
   `--mode hold`, backoffLimit 0, activeDeadlineSeconds 300) renew-only extends the marker to
   `upgrade_quiesce_ttl_s`. Absent or foreign marker: exit 1, nothing rolled. The post-upgrade
   release is unchanged.
6. Bound: after a clean drain, a failure in any later pre-upgrade step releases the fleet within
   300 s of that step's last renewal (today 1800 s). A failure after the hold hook (manifest
   apply or `--wait` timeout during the roll) still holds for up to the roll hold; that case is
   recorded as a known residual and a follow-up, because the roll legitimately lasts up to the
   worker termination grace and no chart-owned process outlives it.

## Consequences

1. A slow upgrade whose gap between two steps exceeds 300 s now refuses instead of rolling; the
   operator retries (images are then cached). Fail closed is the intended trade.
2. The API image writes one worker-owned Valkey key, only through the shared renew-only function.
3. Draft ADR 0144 clause 1's description of the roll hold ("only a clean drain extends it to a
   roll hold capped at the drain wait") no longer matches the code; 0144 should be revised before
   it is accepted.

## Alternatives considered

1. Run schema-migrate before the drain. Rejected: forward-only contract or irreversible revisions
   would run while N-1 workers hold in-flight deliveries (ADR 0142 decision 4 only keeps N-1
   serving across an expand); the identity fence's 15 s lock bound in schema-migrate.yaml is
   justified by "the worker is already drained at hook weight -10", and round 4 measured 39.6 s
   worker transactions; and it does not cover attest, preflight or mail hook failures.
2. Native sidecar renewer in the schema-migrate pod (initContainer with restartPolicy Always).
   Cleanest lifecycle, no API image change. Rejected for now: the chart declares
   kubeVersion >=1.25 and already renders for clusters below 1.30
   (`runner-resources-admission.yaml`); native sidecars are default-on from 1.29 only. Viable if
   Brian raises the floor.
3. A keeper container sharing the migrate pod's PID namespace and watching the migrate process.
   Rejected: `shareProcessNamespace` exposes the migrate container's environment (DATABASE_URL)
   to the keeper through /proc.
4. Renew from `curie cluster up`. Rejected: a raw `helm upgrade` (as the chart allows and the
   resilience run used) would lose the hold entirely.
5. One non-renewed hand-off sized to the whole span. Rejected: even with decision 1 the span is
   attest 300 + migrate 600 + hold start 300 = 1200 s, barely better than 1800.
6. Clear on failure. Rejected: Helm 3 has no upgrade-failed hook; `--atomic` rollback hooks run
   only when the caller opts in.
