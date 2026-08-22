# PT-0116: does pre-binding remove the claim deadline?

Throwaway spike behind
[ADR-0116](../../docs/adr/0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md).
Not shipped code, not maintained, and it implements none of the ADR's decisions:
it only measures whether the pre-bind path is reachable at all, and what it costs
when the node is starved.

## What it does

Four timed arms against a live release, plus a controlled neighbour:

| Arm | Claim | Node | Measures |
| --- | --- | --- | --- |
| A | carries per-claim env (today's shape) | quiet | the cold create baseline |
| B | same | saturated | whether `claimTimeoutSeconds` (90s) is reachable |
| C | carries no env, against a version-keyed pool | quiet | the pre-bind path |
| D | same | saturated | whether the deadline is still reachable |

The version-keyed pool is the ADR's decision 3 built by hand: a copy of the
shipped `SandboxTemplate` with the bundle ref and `CURIE_PLUGIN_DIR` baked in
**per pool** instead of injected per claim, plus its own `SandboxWarmPool`. It is
created alongside the shipped objects and never patches them, so a live claim on
the same cluster is undisturbed.

Contention is a Deployment of busy-loop pods that request `200m` each, matching
what ClickHouse requests, against the runner's and bundle containers' `50m`. That
request gap is the amplifier the chart names: under contention the kernel divides
CPU in proportion to requests, not limits.

**On the recorded ratio.** The cast in `docs/demo/` says "4:1", because that is
what the script printed when it was recorded, and the recording is left alone
rather than edited to say something it did not say. The figure is wrong: 4:1 is
the ratio of the declared millicores, while the kernel divides by `cpu.weight`,
and read off a live node that is **11 for 50m against 29 for 200m, so 2.6:1**.
The conversion has a positive intercept, which compresses it. The script's label
has been corrected for future runs; the ADR carries the measured mapping.

## Results (2026-08-20, two full runs)

|                       | quiet node | under contention |
| --------------------- | ---------- | ---------------- |
| today (cold create)   | 4.72s      | **never ready**  |
| pre-bound (ADR-0116)  | **0.17s**  | 7.79s            |

Run 1 measured 4.66s / timed out / 0.14s / 7.86s. Arm B crossed 90s at 91.02s and
was still not ready when the harness gave up at 110s.

The cold baseline is cluster-shaped. This cluster runs the fake model with no
observability stack and a 6,961-byte bundle, so 4-5s is its floor; the same
measurement on a real-model install with Langfuse and ClickHouse resident was
17.39s. The column that carries the argument is the second one.

## Running it

Needs a cluster you own and can saturate. It refuses any kube context whose name
does not start with `curie-demo`, because the kubeconfig this was written against
also held production contexts.

```bash
kubectl config use-context curie-demo
BUNDLE_REF="bundles/<agent-id>/<version-id>.tar.gz" python3 run_demo.py
```

`BUNDLE_REF` comes from `curie cluster deploy`. Recording:

```bash
asciinema rec --command "python3 run_demo.py" --window-size 100x36 demo.cast
agg demo.cast demo.gif --speed 3 --idle-time-limit 1.5 --font-size 15
```

## What it does not establish

- The ACI change itself. Session identity still arrives as pod env here; arms C
  and D pass **no** env, which is why they bind. How a bind is authenticated once
  the runner token stops arriving as pod env is open.
- A clean cold-path wall clock under contention. Arm B is reported as "never
  ready" rather than a number, because the point is the crossing, not the tail.
- Anything about gVisor. `security.gvisor.mode=off` throughout.

## Load harness (`load_lib.py`, `load_run.py`, `load_isolate.py`)

`load_run.py` runs one-shot conversation throughput for both arms under the same
namespace quota. `load_isolate.py` separates the two regimes the first run
conflated. A conversation counts only when a real ACI turn came back with a
terminal frame, so a pod that goes ready and never answers is a failure, not a
pass.

```bash
export BUNDLE_REF=bundles/<agent-id>/<version-id>.tar.gz
export LOAD_CONTEXT=curie-load          # asserted; refuses any other context
N=24 CONC=5 POOLR=3 python3 load_run.py
python3 load_isolate.py
```

Results in `load-results.json` and `load-isolate-results.json`.

The headline is a negative one and it is the reason these files are kept: with a
pool of three against a 24-conversation burst, pre-binding matched the cold path
on throughput (46.7 against 48.3 conversations per minute) and had a worse tail
(p95 11.40s against 7.56s). Only the first three conversations, one per warm pod,
were served sub-second. Sized at or above the burst, or fed arrivals slower than
the refill, every claim came in under 0.22s. Pool depth against arrival rate is
the whole variable.

## Residency harness (`capacity_lib.py`, `capacity_run.py`)

Answers the question the load harness above got wrong by measuring the wrong
baseline: on the shipped defaults, is capacity decided by the claim path or by
residency? Fourteen one-shot conversations, one arriving every 3s, against the
shipped 8-slot quota; the only difference between arms is whether a conversation
keeps its sandbox after the turn.

```bash
export BUNDLE_REF=bundles/<agent-id>/<version-id>.tar.gz
export CAP_CONTEXT=curie-cap            # asserted; refuses any other context
N=14 HOLD=45 INTERVAL=3 python3 capacity_run.py
```

Results in `capacity-results.json`. Measured: holding the sandbox 45s served 6.9
conversations per minute with **6 of 14 blocked by `exceeded quota`**; releasing
at end of turn served 18.8 per minute with **none** blocked. `HOLD` is
`routeTtlSeconds` scaled down by 80, so the shipped ratio is far wider than the
2.7x this prints.

## Demo (`demo_run.py`)

Drives the recording in `docs/demo/`. Five acts on one cluster: a cold create on
a quiet node, the same claim under reproduced contention, a version-keyed pool
warming, an env-free claim binding, and then the capacity arm -- fourteen
one-shot conversations into an 8-slot quota, held versus released.

```bash
export BUNDLE_REF=bundles/<agent-id>/<version-id>.tar.gz
export DEMO_CONTEXT=curie-demo2 CAP_CONTEXT=curie-demo2   # both asserted
python3 demo_run.py
```

Recorded run (`demo-results.json`): cold create 7.12s quiet and 63.50s under
contention, pre-bound 0.16s and 1.41s; holding a sandbox 45s served 12 of 14 with
10 blocked by quota at 0.2/min, releasing served 14 of 14 with none blocked at
19.2/min. Two conversations were dropped outright in the holding arm, which is
the failure this ADR is about rather than a slow one.
