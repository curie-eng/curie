# The static pool substrate

The static pool runs co-worker turns on a fixed set of long-lived shells
instead of Kubernetes sandboxes or local Docker containers. Each shell is one
container (a task on a managed container service, or any host that can run the
image) running the shell supervisor as its first process. The supervisor
serves an authenticated HTTPS API, starts the unchanged runner as a child
process for one claim at a time, proxies the runner's agent control interface
to the worker, and wipes everything the claim left behind before the shell
takes another claim.

Select it on the worker with `CURIE_SANDBOX_SUBSTRATE=static`. The worker
refuses any value other than `kubernetes` (the default), `docker` and `static`.

## How a claim moves through a shell

1. The worker leases a free slot in Valkey with one atomic script. The lease
   records the claim name, the slot, a lease generation that only ever grows
   per slot, the claim's labels, its bundle ref and its creation time, so any
   worker can list, adopt or reap the claim later.
2. The worker prepares the bundle, the workspace and eager attachments the same
   way the Docker substrate does, and uploads them with the claim's boot env
   as one archive to `PUT /_curie/supervisor/v1/claims/<claim>/<generation>`.
3. The supervisor stages the archive into a fresh directory for that claim,
   builds the child environment name by name, starts the runner as an
   unprivileged child in its own process group, and reports ready once the
   runner answers `/healthz`.
4. The worker's route stores `transport: https`, the shell's host and port, and
   the base path `/c/<claim>/<generation>`. Every runner call goes through that
   path with the claim's own runner bearer. The supervisor refuses a call whose
   path names another claim or generation (410) or whose bearer is not the
   claim's (401), so a delayed request from an earlier claim never reaches a
   later one.
5. Follow-up turns reuse the route while it is live. The runner child keeps
   running across turns.
6. An approval pause suspends the claim: the worker asks the shell to stop the
   child and clean up, keeps a suspended record and the route's history ref,
   and returns the slot to the pool. Resuming claims a slot cold, possibly on a
   different shell, under a new generation and a new runner bearer, and the
   runner rehydrates from the stored history. Deleting the suspended record
   never touches a later lease of the same slot.
7. Retirement first refuses new requests for the claim and asks the runner,
   through `/v1/status`, whether it is idle with durable history (any answer
   but a small JSON object saying so counts as unsettled). It then cuts every
   open proxy stream of the claim, stops the child's whole process group,
   removes the claim directory and the workspace and attachment links,
   returns every scratch root to the entries, mode and owner it had at
   startup, removes every System V IPC object created since startup, and
   checks the whole filesystem again. Only then does it answer. The worker
   frees the slot only on that answer. The substrate revokes the claim's boot
   credential through its usual retirement path.

A shell that cannot prove a clean retirement quarantines itself instead of
taking another claim. The reasons are `processes-survived` (a process of the
claim outlived the stop), `cleanup-unverified` (something the claim could
have left was still there after cleanup; the supervisor logs what),
`history-unsettled` (the runner was busy, gave a malformed answer, or its
history was not durable within the settle timeout; the child is still
stopped and its files wiped), `retire-failed` (retirement itself raised) and
`create-unacknowledged` (the control client hung up before a create answered:
the create still finishes, then its claim is stopped and wiped, because that
caller cannot know whether it started; a retry of that create is refused).
Any failure during a create or a retirement ends in quarantine, never in a
shell that reports itself healthy. A create and a retirement run to their
end even if the worker's connection drops. A quarantined shell answers 503 on its health path, so the container
platform replaces it. The worker keeps the slot quarantined until a supervisor
with a new incarnation answers idle on that slot's address, and then returns
the slot to the pool. A shell the worker cannot reach keeps its slot leased
until the shell answers again.

Each fenced turn records its claim and delivery lease before it starts. After
that lease is first observed absent, `CURIE_ABANDONED_TURN_GRACE_S` allows
90 seconds for cleanup, plus the maintenance cadence. Maintenance takes the
thread lock and checks the exact claim again. Settling a delivery removes its
watch only after observing an idle runner with durable history, or confirming
that its claim is gone. Otherwise the watch remains for maintenance, including
when the delivery is terminal. An idle runner with durable history keeps its
route. An active, unreadable or non-durable runner is retired through the
supervisor. Retirement stops the child and verifies cleanup. If the runner
settles before the supervisor checks it, the cleaned shell can be reused;
otherwise the shell is quarantined and its health endpoint returns 503 until
the platform replaces it. This recovery also applies to Docker and Kubernetes
through their normal claim retirement paths. An unreachable shell stays leased
until retirement can be confirmed.

The runner-local `CURIE_RUNNER_CLEANUP_TIMEOUT_S` defaults to 30 seconds,
matching the existing abandoned-turn drain window. Set it on shells through
`CURIE_SUPERVISOR_CHILD_ENV_PUBLIC`. An abandoned stream allows that total time
for a pending timeout interrupt and its own interrupt.
If interruption fails or expires, the runner kills its isolated CLI process
group and allows five seconds to reap the child. It reports the turn ended and
the session unavailable without advancing the history durability fence. The
existing unfinished-usage report adds at most 25 seconds. The default recovery
grace covers these bounds with 30 seconds of margin: 30 + 5 + 25 = 60 seconds.
Increase the grace alongside a larger runner cleanup timeout.

A refused durable handoff waits with exponential backoff inside the same fenced
delivery. The heartbeat retains the PEL entry without increasing its delivery
count. The original delivery deadline bounds the wait across restarts, and an
exhausted deadline produces the ordinary operator-visible delivery failure.
Actual failed deliveries retain the existing maximum-delivery cap.
Recovery of an admitted capacity turn first quiesces its recorded runner epoch,
then removes the exact predecessor watch before starting its replacement turn.
A completed predecessor is recognized through the existing terminal ledger;
other unfinished predecessors must still reach a durable handoff boundary.
The runner retains whether its latest turn emitted a side effect in authenticated
status. Recovery persists that marker before retiring the claim or replacing
an unfinished predecessor's watch for a follow-up. It checks the marker again
after preflight, so an unacknowledged side effect escalates instead of replaying. Each failed retirement retains its watch for the next pass without
preventing other maintenance.

Known gap: turns started through the attachment handoff do not register a
recovery watch. They do not receive this abandoned-turn retirement bound.

When every slot of a pool is leased, a claim fails with
`CapacityExhaustedError` carrying a capacity rejection for one `pods` unit in
that pool, and the kernel parks the turn exactly as it does for a full
Kubernetes quota. Idle-route reclamation only considers routes in a pool that
is exhausted. A live route is never taken from its thread. When Valkey cannot
be read, a claim fails with `AdmissionUnavailableError` instead.

## Worker configuration

| Variable | Meaning |
| --- | --- |
| `CURIE_SANDBOX_SUBSTRATE` | `static` selects this substrate. |
| `CURIE_STATIC_SHELLS` | The shell inventory, a JSON array described below. Required. |
| `CURIE_STATIC_SHELL_CA_FILE` | Path to the PEM bundle of the CA that signs every shell certificate. Required. The worker verifies each shell's certificate and host name against it. |
| `CURIE_STATIC_SHELL_CONTROL_TOKEN` | The control credential the shells accept, at least 32 characters. Required. It is a host credential and never reaches a runner. |
| `CURIE_WARM_POOL` | The generic pool name, `curie-runner-pool` by default. Shells in this pool serve agents without their own pool. |
| `CURIE_AGENT_SANDBOX_POOLS`, `CURIE_AGENT_CONNECTOR_SECRET_POOLS` | Agents with their own shells. Each named agent's pool is derived as on Kubernetes (`<prefix>-agent-<agent>-runner-pool`), and the worker refuses to start when no inventory entry serves it. |
| `CURIE_ROUTE_TTL_SECONDS`, `CURIE_SUSPENDED_ROUTE_TTL_SECONDS`, `CURIE_CLAIM_TIMEOUT_SECONDS` | Route lifetimes and the claim bind timeout, as for the other substrates. |

Leases live in the worker's Valkey under the substrate key prefix. The static
substrate refuses `CURIE_REPO_RUNNER_POOLS` and the Kubernetes admission
settings at startup. Repository publication runs only on Kubernetes.

The inventory names each shell once:

```json
[
  {"slot": "sre-0", "host": "sre-0.shells.internal", "port": 8443, "pool": "curie-agent-sre-runner-pool"},
  {"slot": "shared-0", "host": "shared-0.shells.internal", "pool": "curie-runner-pool"}
]
```

`slot` is a stable DNS label, `host` a DNS name or IPv4 address that the shell's
certificate covers, `port` defaults to 8443, and `pool` is a pool name. Slots
and host and port pairs must be unique.

## Shell supervisor configuration

The supervisor reads its settings from its own environment.

| Variable | Default | Meaning |
| --- | --- | --- |
| `CURIE_SUPERVISOR_SLOT` | required | The slot name this shell serves. |
| `CURIE_SUPERVISOR_CONTROL_TOKEN` or `CURIE_SUPERVISOR_CONTROL_TOKEN_FILE` | required | The control credential, at least 32 characters. The supervisor removes the variable from its environment at startup. |
| `CURIE_SUPERVISOR_TLS_CERT_FILE`, `CURIE_SUPERVISOR_TLS_KEY_FILE` | required | The shell's server certificate chain and key. The supervisor serves TLS 1.2 or later only. |
| `CURIE_SUPERVISOR_INCARNATION` | random | Identifies this supervisor run. A replacement shell must not reuse it. |
| `CURIE_SUPERVISOR_LISTEN_HOST`, `CURIE_SUPERVISOR_PORT` | `0.0.0.0`, `8443` | The control and proxy listener. |
| `CURIE_SUPERVISOR_CHILD_PORT` | `8080` | The runner child's port. |
| `CURIE_SUPERVISOR_RUNNER_COMMAND` | `python -m curie_runner` | The child command line. |
| `CURIE_SUPERVISOR_CHILD_UID`, `CURIE_SUPERVISOR_CHILD_GID` | `1000` | The child's uid and gid, never 0. The supervisor must run as root, or it refuses to start. |
| `CURIE_SUPERVISOR_CHILD_ENV` | empty | Comma separated names of secrets in the shell environment that the child receives, such as the model credential and connector secrets. An executor claim receives only the names its claim allows. |
| `CURIE_SUPERVISOR_CHILD_ENV_PUBLIC` | empty | Comma separated names of non-secret settings the child receives, such as telemetry endpoints or connector endpoint maps set on the shell. |
| `CURIE_SUPERVISOR_STATE_ROOT` | `/var/lib/curie-shell` | Where claim directories are staged. |
| `CURIE_SUPERVISOR_WORKSPACE_PATH`, `CURIE_SUPERVISOR_ATTACHMENTS_PATH` | `/workspace`, `/attachments` | Links the supervisor points at the claim's workspace and attachments. Each must be absent, a link or an empty directory. |
| `CURIE_SUPERVISOR_SCRUB_PATHS` | `/tmp,/var/tmp,/dev/shm,/run/lock,/dev/mqueue` | The scratch roots: the only places outside its claim directory the child may write. Each is returned to its startup contents at every retirement. |
| `CURIE_SUPERVISOR_UNISOLATED_FOR_TESTS` | unset | `1` runs an unprivileged supervisor whose child shares its uid, for tests only. Such a shell reports `isolated: false` and the worker never leases it. |
| `CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS` | `15` | How long retirement waits for an idle runner with durable history. |
| `CURIE_SUPERVISOR_STOP_GRACE_SECONDS` | `10` | How long a stopped child has after SIGTERM before SIGKILL. |
| `CURIE_SUPERVISOR_MAX_STAGE_BYTES`, `CURIE_SUPERVISOR_MAX_STAGE_MEMBERS` | 2 GiB, 200000 | Upload limits. |

The child environment is built, never inherited. It holds the image's
`PATH`, `LANG`, `LC_ALL`, `TZ`, `SSL_CERT_FILE`, `PYTHONUNBUFFERED` and
`CURIE_PROC_DUMPABLE_PRELOAD`, the names listed above, and the claim's boot env
from the worker. The supervisor sets `HOME`, `TMPDIR`, `CURIE_PLUGIN_DIR`,
`CURIE_SANDBOX_ID` and `CURIE_RUNNER_PORT` itself. No `CURIE_SUPERVISOR_`
variable can be forwarded. The claim's boot env passes through unchanged
otherwise, so the caller and route tokens and the connector endpoint variables
(`CURIE_HOSTED_CONNECTOR_ENDPOINTS`, `CURIE_CONNECTOR_UPLOAD_ENDPOINTS`) reach
the runner as they do on the other substrates.

The image is `packages/shell-supervisor/Dockerfile`, which adds the supervisor
to a runner image and runs it as root with a health check on the supervisor's
health path.

## Supervisor API

Every control call carries `Authorization: Bearer <control credential>`.

| Call | Answers |
| --- | --- |
| `GET /_curie/supervisor/healthz` | 200 with `{"state": ...}`, or 503 when quarantined. No credential. |
| `GET /_curie/supervisor/v1/shell` | 200 with the shell status: slot, incarnation, state (`idle`, `staging`, `running`, `stopping`, `quarantined`), the current claim with its generation, readiness and child exit code, the highest generation seen, the quarantine reason and the last retirement. |
| `PUT /_curie/supervisor/v1/claims/<claim>/<generation>` | Body: an uncompressed tar archive with `Content-Length`. 201 when started; 200 when the same claim, generation and body were already started; 409 when another claim holds the shell or the generation is not newer than one already seen; 400 for a refused archive; 411 without a length; 413 over the size limit; 503 when quarantined or when the runner did not start. |
| `DELETE /_curie/supervisor/v1/claims/<claim>/<generation>?mode=retire` or `mode=suspend` | 200 with `outcome` `cleaned`, `quarantined` or `absent`; 409 with `absent` when another claim holds the shell. Retiring a generation the shell never started fences it, so a late create for it is refused. |
| `* /c/<claim>/<generation>/<runner path>` | The runner's own interface, with the claim's runner bearer. 410 for a claim or generation the shell is not serving, 401 for any other bearer, 503 once the child has exited. Responses stream through unbuffered, so NDJSON turns, turn epochs, timeouts and disconnect cancellation behave as they do on a direct connection. |

The archive holds `claim.json` (the manifest: claim, generation, boot env,
executor secret names, withheld names, attachment mode) and the `bundle/`,
`workspace/` and `attachments/` trees. The supervisor deletes the manifest
before the child starts.

## Trust boundaries

The worker holds the control credential and the CA. A shell holds its own
certificate and key, the control credential, and the secrets it passes to its
child by name. The supervisor runs as root and the runner child runs as an
unprivileged uid, so the child cannot read the supervisor's environment,
certificate key or other claims' directories. The runner bearer is per claim
and must differ from the control credential. Each claim gets a new directory,
`HOME` and `TMPDIR`, and its bundle is read only.

Before it serves, the supervisor walks the whole filesystem (virtual
filesystems excepted) and refuses to start, with exit code 78, when the child
could write any path outside the scratch roots, when a setuid or setgid file
exists, when System V IPC objects already exist, or when no mqueue
filesystem is mounted inside a scratch root (POSIX message queues live
there). The child runs with no_new_privs and a zero core file size.
System V IPC objects are removed by a short helper running as the child's
uid, because a container's root usually lacks the capability to remove
objects another uid created.

If the supervisor fails, it kills the child's process group and exits, so the
whole container goes down and the platform replaces it. The child is also
bound to the supervisor with a parent death signal. A shell never serves a
claim after its supervisor is gone.

## Ports

| Port | Exposure |
| --- | --- |
| 8443 (`CURIE_SUPERVISOR_PORT`) | The only ingress. Reachable from the workers only. |
| 8080 (`CURIE_SUPERVISOR_CHILD_PORT`) | The runner binds all interfaces, as it does in every image. The deployment must not expose it outside the container. |

## Deployment requirements

1. One container per inventory slot, each with a stable address the worker can
   reach on port 8443 and nothing else exposed.
2. A server certificate per shell whose names cover its inventory host, signed
   by the CA in `CURIE_STATIC_SHELL_CA_FILE`, and its key, mounted read only
   and readable by root alone. The supervisor refuses to start (exit code 78)
   when the key or a control credential file is owned by the runner's uid,
   readable by its group, or readable by everyone.
3. One control credential shared by the workers and the shells, delivered as a
   secret.
4. The secrets each pool's agents need (the model credential and connector
   secrets) in the shell environment, named in `CURIE_SUPERVISOR_CHILD_ENV`.
5. A health check on `/_curie/supervisor/healthz` that replaces an unhealthy
   container, and a restart policy that starts a new supervisor incarnation.
6. A Valkey shared by every worker.
7. The image built from `packages/shell-supervisor/Dockerfile`, run as root
   with the container runtime's default `/dev/mqueue` mount and its own IPC
   namespace. The Dockerfile hands the runner image's home to root and strips
   setuid and setgid bits, so the startup check passes.

## What the static pool does not support

The static pool has no Kubernetes quota, scheduler or tool daemon. Quota
methods refuse, scheduling diagnostics report nothing, tool daemon profiles and
repository runner pools are refused, the worker refuses to start with the
action executor enabled (its serving attestation reads Kubernetes), and
`runner_resources` are not applied (each shell has the resources its
container was given).

## Verification

`packages/shell-supervisor/tests` runs real supervisors with a stand-in runner
child and covers authentication, streaming, fencing, idempotent create,
teardown, quarantine and fault handling, including a create or retirement
whose client disconnects, a malformed runner status and streams open at
retirement. `packages/shell-supervisor/tests/test_shell_container.py` runs
the built image as a root container (CI runs it in the image proofs job): a
hostile child plants files and symlinks in every directory it can write and
creates named System V and POSIX IPC objects, and the next claim on the
recycled shell finds none of it, with every shared directory's mode and
owner unchanged.
`apps/worker/tests/sandbox/test_static.py` runs the static client against real
Valkey and real supervisors. `apps/worker/tests/sandbox/test_static_qualification.py`
runs real supervisors with the real runner on the fake model through the
substrate and the runner client: a first turn, a follow-up on the retained
route, an approval pause and cold resume on a recycled shell with the same
history, refusal of the paused claim's route and bearer, a worker restart in
the middle of a turn, pool exhaustion and cleanup. It also builds a claim's
environment with the worker's static connector binding and proves the
connector endpoints, caller token, agent name and the shell's connector CA
file reach the runner unchanged.
