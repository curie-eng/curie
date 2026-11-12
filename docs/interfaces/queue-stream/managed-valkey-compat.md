# Managed Valkey on ElastiCache (cluster mode disabled)

> Companion to [INTERFACE.md](./INTERFACE.md). The chart and compose store is
> Valkey 8.1 with AOF fsync every second. This guide is the swap onto node-based
> ElastiCache for Valkey with cluster mode disabled.

The target is AWS ElastiCache for Valkey 8.1, cluster mode disabled, Multi-AZ
with one replica and automatic failover, TLS, a replication-group auth token,
`noeviction`, and daily snapshots. Ordinary API, worker and dispatcher clients
use the shared primary-endpoint recovery policy in
`packages/curie-internal/src/curie_internal/valkey.py`. They remain standalone
clients, never cluster clients. This configuration does not provide durable
acknowledged writes or support the protected commissioning and worker lanes.
Do not attach an RBAC user group: this target uses the default user and an auth
token, and the running services have no username setting.

This guide lives beside the queue seam, the same way the Postgres swap guide
lives beside the relational seam. `docs/operations.md` is a file, so this note
is not under a `docs/operations/` directory.

## Open items

1. **Acknowledged writes can be lost on failover.** This is the recovery point
   the selected cache cannot match. ElastiCache durability (`sync` or `async`)
   requires Valkey 9.0 or later, cluster mode enabled, and Multi-AZ with at
   least one replica. It is not supported for cluster mode disabled
   ([Durability limitations](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Durability.Limitations.html)).
   `appendonly` and `appendfsync` are not modifiable and default off
   ([engine parameters](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/ParameterGroups.Engine.html)).
   The chart and compose store run `appendonly yes` and `appendfsync everysec`
   on a persistent volume. The managed store replicates asynchronously to one
   replica and takes daily snapshots. A failover can drop writes the primary
   already acknowledged, back to the last byte the replica applied, and a
   snapshot does not close that window. The observable failures are listed
   below. Leave durability disabled. Turning it on would require cluster mode,
   which this swap does not take.
2. **Reconnect recovery is implemented; write outcomes remain uncertain.**
   On READONLY, connection errors and `UNBLOCKED` responses from a demoted
   server, ordinary shared clients
   disconnect the pool, including in-use connections. The next command opens
   a new socket and resolves the primary endpoint again. Observational commands
   in the explicit read allowlist retry at most three times after 0.1, 0.2 and
   0.4 seconds. Writes, Lua, transactions and stream delivery operations are
   never automatically retried by this policy; redis-py's generic command
   retry is disabled so a lost reply cannot silently replay an `INCR` or `EVAL`.
   Existing callers receive those errors and retain their own recovery logic.
   The shared stream read loop catches READONLY and `UNBLOCKED` and backs off
   without exiting or canceling its handlers. Timeouts do not reset the pool;
   redis-py disconnects only the timed-out command connection. Other response
   errors still propagate to the existing supervisor, and this recovery does
   not recreate lost stream or group data. The kill subscriber owns a separate
   stop-aware reconnect/resubscribe loop, preserving its subscription state
   across connection errors and its own timeouts without retrying writes.
   DNS convergence is still required, and a pool
   reset may interrupt sibling commands, whose outcomes must be treated as
   unknown. The bounded pressure probe opts out of read retries. Use the
   primary endpoint, never the reader endpoint or a node endpoint.
3. **A named RBAC user is not selectable.** The API builds
   `rediss://:<password>@<host>:<port>/0` unless `VALKEY_URL` is set. The
   worker and the dispatcher pass host, port, password, database, and `ssl`,
   and they have no username field. The connector proxy sends `AUTH` with the
   password only. The chart and Langfuse do the same. An auth token keeps the
   default user on and matches every one of those clients. Named users need
   RBAC user-group configuration, outside this target
   ([RBAC](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Clusters.RBAC.html)).
   Username support across those clients is a later change. It is not required
   for the auth-token configuration, and Langfuse on this chart still would
   not send a username.
4. **Protected commissioning and protected-worker continuity are unsupported.**
   Commissioning needs `ACL SETUSER`, a restricted ElastiCache command.
   Protected workers require named users, which need RBAC and are excluded
   from this auth-token target. They additionally pin TLS peer identity and
   the server `run_id`, and their transport refuses reconnect. Even a separately
   provisioned named user would not establish continuity across promotion to
   a different server identity. The security fences in
   `packages/protected-hooks/src/curie_protected_hooks/worker_transport.py`
   remain unchanged; never route them through ordinary reconnecting clients.
5. **Direct CLI Valkey transport needs a separate managed-cache change.**
   Password construction is fixed, but the current Rust Redis dependency has
   no TLS feature, and cluster message/eval enqueue through an in-chart Valkey
   port-forward using plaintext URLs. Those direct paths are unsupported for
   this TLS-only external cache. Add TLS and an explicit primary-endpoint
   transport design before using them; the Python turn canary can already take
   a `rediss://` URL. The feature inventory was checked with
   `cargo tree --manifest-path cli/Cargo.toml -e features -i redis`.

## Required cache settings

Engine and shape, from the ElastiCache controls this swap depends on:

| Setting | Value |
| --- | --- |
| Engine | Valkey |
| Engine version | 8.1, the newest 8.1 patch AWS offers. Matches the chart image `valkey/valkey:8.1.10-alpine` and the server the proof ran. Valkey 9.0 is available and is not the target: durability cannot be used in this mode, and 9.0 was not the server under test. |
| Mode | Node-based, cluster mode disabled. One shard. |
| Multi-AZ | Enabled, with one replica and automatic failover. |
| Encryption in transit | Required. Clients use TLS on port 6379. |
| Encryption at rest | Enabled. |
| Auth | Replication-group auth token. No Valkey user group. The default user stays on and accepts `AUTH` with the token only. |
| Parameter group family | `valkey8`, the family AWS documents for Valkey 8.1. Create a custom group from the cluster-mode-disabled default. The default groups themselves cannot be edited. |
| `maxmemory-policy` | `noeviction`. The default is `volatile-lru`, which evicts keys that have a TTL. Locks, grants, dedupe keys, and leases are TTL keys. A refused write is the failure this store already chooses. Changes take effect immediately. |
| `notify-keyspace-events` | Leave empty. Curie does not subscribe to keyspace notifications. |
| `appendonly`, `appendfsync` | Not settable. They stay off. See open item 1. |
| `timeout` | Leave the default `0` so an idle blocking read is not disconnected by the server. |
| `databases` | Leave the default. Every Curie client uses database 0. This parameter can be changed only before the group is associated with a cache. |
| Durability | `disabled`. |
| Snapshots | Daily automatic backups. Retain at least 1 day. 7 days is the practical retention so a bad day can be restored. Snapshots do not replace the missing AOF window. |
| Node size | Do not copy the chart's 256Mi container limit. ElastiCache reserves memory for the process, replication, and snapshots, and `noeviction` refuses writes when the data memory is full. Size the node for the stream working set plus the Langfuse queues that share the cache, with headroom under `maxmemory`. A `cache.t4g.small` is only a floor for a small install. Move to a larger node in the m7g or r7g family when stream retention or those queues are large. |

The replication-group AUTH mode has no configurable user access string.
It uses the default user with an auth token. Access strings belong to RBAC
users, which this target excludes. The following string describes only the
local stand-in's permissive user; it is not an ElastiCache AUTH-mode setting:

```
on ~* &* +@all
```

The **local test constraint only** was a tighter user on Valkey 8.1.10:

```
on ~* &* +@all -@admin -@dangerous +info|server
```

That constraint keeps pub/sub channel access and permits `INFO server`, which
protected scripts use for their run-id fence, while denying other `INFO`
sections and admin/dangerous commands. It is neither an AWS AUTH-mode recipe
nor a claim that Langfuse's BullMQ command set works with that constraint.
ElastiCache restricts commands at the engine regardless of the local ACL.
Do not paste the protected worker ACL recipe into an ElastiCache access string;
it contains `reset`, which AWS rejects.

Node-based restricted commands, which the main services do not call
([supported commands](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html)):
`acl setuser`, `acl load`, `acl save`, `acl deluser`, `bgrewriteaof`, `bgsave`,
the mutating `cluster` subcommands (`addslot` through `setslot`), `config`,
`debug`, `migrate`, `psync`, `replicaof`, `save`, `slaveof`, `shutdown`, and
`sync`. Read-only ACL commands such as `ACL WHOAMI`, `ACL LIST`, and
`ACL GETUSER` are supported on the engine. `CLIENT KILL`, `KEYS`, `FLUSHALL`,
and `MONITOR` are restricted on serverless caches, not on this node-based list.
The proof's tighter user denied some of those anyway. That is the ACL, not an
engine ban to copy onto the shared user.

## Client configuration

Set these on the API, the worker, and the dispatcher. The chart renders them
from `valkey.tls`, the primary endpoint, and the auth token. `valkey.tls=true`
with `valkey.deploy=true` fails the render, because the in-chart server has no
TLS listener. For this cache, set `valkey.deploy=false`.

| Env | Value |
| --- | --- |
| `VALKEY_HOST` | The primary endpoint. Not the reader endpoint. |
| `VALKEY_PORT` | `6379` |
| `VALKEY_PASSWORD` | The auth token. |
| `VALKEY_TLS` | `true` |

Leave `VALKEY_URL` unset unless it is a `rediss://` URL for database 0. When it
is set, it replaces the parts above for the API only. Do not put a username in
it for this configuration. The worker and the dispatcher ignore it.

TLS verification uses the system CA bundle. ElastiCache presents a public
certificate that bundle accepts. `ssl_cert_reqs` stays `required` and hostname
checks stay on. The chart does not mount a private CA, and Langfuse is given
the same boundary: `REDIS_HOST`, `REDIS_PORT`, `REDIS_AUTH`, and
`REDIS_TLS_ENABLED`, with `REDIS_TLS_CA` left unset. A private CA or mutual TLS
is outside this knob.

Passwords built into the API DSN are percent-encoded with an empty safe set,
including `#`, `%`, `/`, `?`, `@` and `+`. Worker and dispatcher constructors
pass the password as a separate argument and need no URL encoding.
The CLI local and cluster direct queue builders also encode the password through
one shared URL builder. The
connector proxy receives the API-built DSN and percent-decodes its password;
it does not build a second URL. When supplying `VALKEY_URL` directly, encode
its password yourself.

For auth-token rotation, use AWS `ROTATE` to accept the new token alongside
the old one, update **every** consumer, including Langfuse, API, worker,
dispatcher and any connector proxy grant-store configuration, then restart
those consumers so all pooled connections and issued proxy configurations use
the new token. Verify fresh connections before AWS `SET` removes the old token
([AUTH rotation](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/auth.html)).

The pinned redis-py 8.1.0 defaults to RESP3. With a password it sends
`HELLO 3 AUTH default <password>`, then negotiates
`CLIENT MAINT_NOTIFICATIONS ON moving-endpoint-type ...` in its default `auto`
mode, followed by `CLIENT SETINFO LIB-NAME` and `LIB-VER`. Both sync and async
connection implementations catch a `ResponseError` rejecting maintenance
negotiation in `auto` mode, log at DEBUG and continue connecting. An explicit
`enabled=True` instead propagates that rejection; non-`ResponseError` failures
also propagate. `CLIENT SETINFO` response errors are ignored separately.
This is grounded in redis-py 8.1.0's the sync and async connection modules, especially `on_connect_check_health` and
`_enable_maintenance_notifications`
([pinned source](https://github.com/redis/redis-py/tree/v8.1.0/redis)).
The worker pressure client explicitly disables maintenance notifications.
`SELECT` is sent only for a nonzero database; `health_check_interval` stays 0.
The ordinary worker socket timeout stays above its blocking read by 5 seconds.


## Call paths

Verdicts are for the auth-token, cluster-mode-disabled cache above. "Lost write"
means an acknowledged write that the replica did not have at failover.

| Path | Verdict | Lost write |
| --- | --- | --- |
| API, worker, and dispatcher connections (`Settings.valkey_dsn`, `WorkerConfig.valkey_client_kwargs`, `build_redis`) | Client config: `VALKEY_TLS=true`, primary host, auth token; shared failover clients. | Pool invalidation on READONLY, `UNBLOCKED` and connection errors, excluding timeouts; bounded observational retries. Writes and Lua surface errors. |
| Connector proxy grant spend (`_set_nx_ex` in `apps/worker/src/curie_connector_proxy/server.py`) | Compatible. `rediss://` uses the system CA and TLS 1.2 or newer. `AUTH` is the password only. `SELECT` runs only when the URL path is not 0. Any reply other than `+OK`, including a connection error, is a failed spend. | An acknowledged `SET` that then disappears lets the same grant id be spent again. An error during failover fails closed. |
| Streams and consumer groups (`XADD`, `XGROUP`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, `XCLAIM`, `XPENDING`, `XINFO`, `XRANGE`, `XREVRANGE`, `XLEN`, `XTRIM`) | Compatible. The broker port and the direct producers use these verbs. Multi-key scripts that also touch a stream are safe only because cluster mode stays disabled. | A lost `XADD` drops a turn the producer already treated as queued. A lost `XACK` leaves the entry pending and the group redelivers it, which is the existing at-least-once window. |
| `SET NX EX` dedupe, locks, leases, and single-use grants | Compatible as commands. The parameter group must be `noeviction` or a full node evicts these TTL keys. | The key is gone, so a duplicate event is accepted, a second holder takes a lock or lease, or a grant is spent twice. |
| Lua, including multi-key scripts and `TIME` | Compatible on this mode. Production scripts call the verbs listed below. No script calls `CONFIG`, `DEBUG`, `RANDOMKEY`, or `math.random`. `INFO server` is the protected run-id fence. Multi-key `EVAL` (resume, delivery, channel-read ledger, capacity wait, admission, commissioning, protected markers) hashes to more than one slot and is why cluster mode stays off. `EVAL` is sent directly at most sites. `register_script` uses `EVALSHA` and redis-py recovers `NOSCRIPT` with `SCRIPT LOAD` then `EVALSHA`, including after a new primary with an empty script cache. | A script the primary acknowledged and the replica did not apply is the same class of loss as the writes inside it. A script that errors mid-failover may have applied or not. Callers that retry a non-idempotent script can double the effect. |
| `MULTI` / `EXEC` and pipelines | Compatible. One primary, so a transaction is not a cross-slot error. | A transaction the replica missed is lost as a group. |
| `SCAN` | Compatible. Production iteration uses `scan_iter`. `KEYS` is not the production path. | None. `SCAN` does not create durable state. |
| Pub/sub kill channel | The API publishes and the worker subscribes. The subscriber reconnects and resubscribes after a connection fault or its own timeout; an unrelated command timeout leaves its connection intact. DNS alone does not move a still-connected subscription: a connection fault or shared-pool reset must break it first. | Pub/sub has no replay. Events published during a disconnected gap can be missed, so active turns may require reissuing a kill after resubscription. A surviving kill flag refuses new turns but does not interrupt an already-running turn. A lost kill flag also requires state reconciliation. |
| Blocking `XREADGROUP` | Compatible to connect. The socket timeout sits above the block. | A demotion can reject a new read with READONLY or interrupt an already-blocked read with plain `ResponseError` starting `UNBLOCKED`. Both, and connection errors, invalidate the pool; the shared loop backs off and reconnects through DNS without a supervisor restart. A socket timeout backs off without resetting unrelated connections. Lost replies can leave pending entries for the existing reclaim path; lost stream/group state remains a separate recovery problem. |
| Database index | Compatible while every client stays on 0. The API DSN hardcodes `/0`. | None by itself. |
| `CONFIG`, `DEBUG`, `SAVE`, `BGSAVE`, `FLUSHALL`, `MONITOR`, `REPLICAOF`, `SHUTDOWN`, `MIGRATE`, `SYNC` | The main services do not call them. `cli/scripts/restore-drill.sh` does issue `SAVE`; that Compose-specific restore procedure does not apply to ElastiCache. Use AWS snapshot and restore APIs instead. ElastiCache removes its restricted set at the engine. | None. |
| Protected-hook commissioning (`ACL SETUSER`, `ACL GETUSER`, `ACL WHOAMI`, `CLIENT KILL`) | Unsupported for both commissioning and protected-worker continuity on this configuration. Named users require excluded RBAC, and TLS/run-id/no-reconnect fences reject a replacement server. See open item 4. | Same as the keys those scripts write. |
| CLI direct queue commands and the turn canary | The Rust CLI direct Valkey paths are unsupported for this TLS-only target: the Redis crate enables `tokio-comp` and `streams`, without a TLS feature, and cluster message/eval builds plaintext URLs behind an in-chart Valkey port-forward. Encoding passwords does not add managed-cache routing or TLS. The Python turn canary takes a `rediss://` URL and can use TLS. These operator tools are separate from the ordinary service clients. | Same as the stream and set commands they issue; direct CLI transport needs a separate supported managed-cache design. |
| Langfuse on the same cache | Client config only: the chart's `REDIS_*` values. The default user uses the replication-group auth token; no access string is configurable in AUTH mode. The parameter group uses `noeviction`. | Langfuse's own queue entries can be lost on failover. That is outside Curie's stream contract and still shares this recovery point. |

Lua verbs found in production code under `apps/`, `packages/`, `runner/`,
`cli/`, `examples/`, `tools/`, and `adapters/`, excluding tests: `DECR`, `DEL`,
`EXISTS`, `EXPIRE`, `GET`, `HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`,
`HMGET`, `HSET`, `HSETNX`, `INCR`, `INFO`, `LLEN`, `LRANGE`, `MSET`, `PERSIST`,
`PEXPIRE`, `PEXPIREAT`, `PEXPIRETIME`, `PTTL`, `RPUSH`, `SADD`, `SET`, `SETNX`,
`SISMEMBER`, `SPOP`, `SREM`, `STRLEN`, `TIME`, `TTL`, `TYPE`, `XACK`, `XADD`,
`XCLAIM`, `XINFO`, `XLEN`, `XPENDING`, `XRANGE`, `XREVRANGE`, `XTRIM`, `ZADD`,
`ZCARD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREM`, `ZREMRANGEBYSCORE`, `ZSCORE`.
`INFO` is always the `server` section in those scripts.

## What a lost acknowledged write looks like

Replication loss can roll back deletions and replacements as well as new
writes. Reconnecting restores transport, not the following guarantees:

| Lost acknowledged mutation | Observable effect | Required recovery |
| --- | --- | --- |
| Dedupe, single-use grant or lease acquisition (`SET NX EX/PX`) | A duplicate is accepted, a grant is spent twice, or another holder acquires a live lock or lease. | Reconcile effects against durable identity and external state before replay. Stop or fence prior holders before reacquiring authority; reconnect alone is insufficient. |
| Channel-read revocation `DEL` in `packages/curie-internal/src/curie_internal/channel_read_ledger.py` | A removed active-ledger capability can revive until its lease or token expires. | Treat affected capabilities as potentially live; wait for both validity bounds to expire or revoke through a durable authority before resuming sensitive access. |
| Page-budget increment | The counter rolls back, restoring previously spent pages. | Reconstruct spending from durable evidence or refuse affected capabilities until expiration. The current counter cannot prove the original budget after failover. |
| Affinity replacement, suspension update or route creation | An old route or suspension state reappears; a missing route causes fresh sandbox allocation in `apps/worker/src/curie_worker/sandbox/substrate.py`. Existing live sandboxes may still exist. | Reconcile routes against actual claims and pods, terminate or fence stale owners, then rebuild affinity before permitting duplicate execution. |
| Completion, dedupe or side-effect marker | Replay protection weakens and an externally completed effect may be repeated. | Check durable completion and external effects before retrying; an absent Valkey marker is not evidence of no effect. |
| Resume `XADD` or its marker | A wake disappears or later producer retries can duplicate it. If Postgres `resumed_at` committed first, the existing reconciler never selects that row again: `apps/api/src/curie_api/crud/approvals.py` filters NULL `resumed_at`, and `apps/api/src/curie_api/resumereconciler.py` marks after enqueue. | Identify durable approval obligations with no confirmed terminal execution and safely republish with the original identity. Current reconciliation does not repair this loss. |
| `XACK` | The pending entry can be delivered again. | Use the existing bounded reclaim path, with durable/external verification when side-effect markers may also have rolled back. |
| Ordinary turn `XADD` | The producer observed success but the worker never receives the turn. | Replay from a durable ingress obligation or report the lost turn; a stream alone cannot reconstruct it. |
| Kill flag `SET`, deletion or kill/resume event `PUBLISH` | A lost kill lets new work run; a lost flag deletion restores a stale kill. A lost publish misses interruption of an active turn even if the flag survives. | Reconcile intended kill/resume state and active turns, rewrite the flag and reissue the event as appropriate. |

**Resume repair recommendation, not implemented:** retain a durable outbox with
publication identity and terminal-consumption evidence, and reconcile recently
published obligations after failover. Merely scanning recent `resumed_at` rows
and checking for an absent stream entry is unsafe: stream trimming and completed
consumption can also remove that entry, and dedupe and side-effect markers can
roll back together. A safe repair must preserve the selected HTTP identity and
sealed approval publication rules, respect completed execution, and avoid
reissuing side effects. This crosses more than a small, clearly safe reconciler
patch. The stranded-resume case remains an explicit accepted recovery gap.


## Proof

The stand-in was Valkey 8.1.10 (the chart's image line) with AOF off,
`maxmemory-policy noeviction`, a plaintext port and a TLS port, and the
node-based admin commands renamed away (`CONFIG`, `DEBUG`, `FLUSHALL`,
`FLUSHDB`, `SHUTDOWN`, `REPLICAOF`, `SLAVEOF`, `BGSAVE`, `BGREWRITEAOF`,
`MIGRATE`, `SYNC`, `PSYNC`, `SAVE`, `MODULE`, `MONITOR`). The default user and
a named user used the tighter string above. An inspector user kept `+@all` so
the proof could read `INFO memory` and `INFO persistence`.

Checked-in tests speak plaintext. `connect_or_skip` in
`packages/test-support/src/curie_test_support/valkey.py` builds `redis.Redis`
from host, port, and password, with no TLS and no username. The suites below
used that plaintext port. A separate redis-py client used the TLS port, once
with the named user and once with `rediss://:<password>@.../0`, which
authenticated as `default`.

Results:

1. `tests/test_stream_groups.py`: 3 passed.
2. `apps/worker/tests/kernel/test_threadlock.py`: 6 passed.
3. TLS ping succeeded for the named user and for the password-only default user.
4. `SET NX EX` stored once and refused the second spend. `TIME`, a two-key
   `EVAL`, `MULTI`/`EXEC`, `PUBLISH`/`SUBSCRIBE`, `SCAN`, `CLIENT SETINFO`, and
   `SELECT 0` succeeded.
5. Every Lua verb listed above succeeded under the tighter user, including
   `INFO server`. `INFO` with no section was denied.
6. `XREADGROUP`, `XAUTOCLAIM`, `XCLAIM`, `XACK`, and `XPENDING` succeeded.
7. `CONFIG`, `DEBUG`, and `FLUSHALL` were unknown commands. `ACL SETUSER`,
   `ACL GETUSER`, `CLIENT KILL`, and `KEYS` were denied for the tighter user.
8. After the 32MiB ceiling filled, the next write returned `command not allowed
   when used memory > 'maxmemory'`, and a TTL key written first was still
   present.
9. The inspector saw `aof_enabled` 0 and `maxmemory_policy` `noeviction`.
10. That original command/ACL stand-in used one process with `REPLICAOF`
    renamed away. It did not prove failover. The separate automated promotion
    proof below uses two processes with replication administration enabled.

Python 3.13 rejected a first CA that had no key-usage extension. The CA that
passed had `keyUsage` of certificate sign and CRL sign. ElastiCache's public
certificates already have that. A private CA is still outside the chart.

To re-run, start Valkey 8.1.10 with the shape above, point
`TEST_VALKEY_HOST`, `TEST_VALKEY_PORT`, and `TEST_VALKEY_PW` at the plaintext
listener, and run:

```bash
uv run pytest tests/test_stream_groups.py apps/worker/tests/kernel/test_threadlock.py
```

Then repeat the TLS checks with redis-py against the TLS listener: password-only
`rediss://` as the default user, and a named user only if you are proving the
tighter string. Remove the process when you are done.

## Automated promotion proof

`apps/worker/tests/test_managed_valkey_failover.py` starts isolated Valkey
8.1.10 primary and replica processes with auth, `noeviction`, and AOF disabled.
A repointable TCP port keeps existing connections on their original server.
The proof promotes the replica, demotes the old primary using `REPLICAOF`,
leaves the endpoint stale through twelve READONLY responses, then repoints it.
It asserts that the same stream read task handles and acknowledges a new entry,
the same threadlock acquires and releases again, dispatcher writes recover,
and an authenticated API reply append changes from 503 to 200 and reads back
the expected events. No application client or consumer is restarted.
A second promotion case holds an actual `block=5000` read across demotion,
observes the server's `UNBLOCKED` response, and verifies that the same read task
consumes afterward. A kill-subscription case proves that an unrelated blocking
command timeout and an ordinary NOGROUP error preserve its connection, then
kills the pub/sub socket and confirms resubscription and subsequent kill delivery.
It also drops replies after real writes and checks that `INCR`, Lua and a
transaction are not repeated by either sync or async clients, while an
observational `GET` retries at most three times and then surfaces failure.
The reserved fixture token is passed through the API DSN into the actual connector grant
spender. Fixture teardown verifies removal of its containers and network.

Run with the caller's disposable Docker endpoint configured:

```bash
uv run pytest apps/worker/tests/test_managed_valkey_failover.py apps/api/tests/test_control_unit.py --reruns 0
```

This promotion proof is plaintext and local. The earlier TLS/auth stand-in
proves a separate connection path, not AWS failover, TLS promotion or an AWS
control-plane rehearsal. Live ElastiCache maintenance, DNS TTL behavior,
AWS auth-token rotation and daily snapshot restore remain deployment checks.

## What this guide does not change

Protected transport security fences stay unchanged. Username plumbing,
ElastiCache-native commissioning, direct CLI TLS/routing, durable publication
reconciliation and
recovery of rolled-back authorization or execution state remain recommendations.
Cluster mode stays disabled, so multi-key Lua stays valid and durability stays
unavailable. Ordinary connection recovery does not make acknowledged writes
durable or authorize blind replay after failover.
