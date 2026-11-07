# Managed Valkey on ElastiCache (cluster mode disabled)

> Companion to [INTERFACE.md](./INTERFACE.md). The chart and compose store is
> Valkey 8.1 with AOF fsync every second. This guide is the swap onto node-based
> ElastiCache for Valkey with cluster mode disabled.

Curie can run on that cache with no application change when the cache uses an
auth token, TLS, and a custom parameter group that sets `maxmemory-policy` to
`noeviction`. Clients stay on `redis.Redis` and `redis.asyncio.Redis`. Do not
move them to a cluster client. Do not attach a Valkey RBAC user group: ElastiCache
turns the default user off for that group, and the running services cannot send
a username.

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
2. **A demoted primary is not retried when it returns READONLY.** redis-py 8.1
   retries connection and timeout errors. `ReadOnlyError` is a `ResponseError`,
   so it is not in that set. `StreamConsumer._consume` catches the timeout and
   the connection error around `XREADGROUP` and then retries. A READONLY error
   leaves that loop. A dropped socket is retried, and a new connection resolves
   DNS again. A live socket that keeps returning READONLY is not. Use the
   primary endpoint, not the reader endpoint and not a node endpoint. Blocked
   `XREADGROUP` and in-flight `EVAL` on the old primary fail with one of those
   two errors. This is a reconnect improvement, not a boot blocker.
3. **A named RBAC user is not selectable.** The API builds
   `rediss://:<password>@<host>:<port>/0` unless `VALKEY_URL` is set. The
   worker and the dispatcher pass host, port, password, database, and `ssl`,
   and they have no username field. The connector proxy sends `AUTH` with the
   password only. The chart and Langfuse do the same. An auth token keeps the
   default user on and matches every one of those clients. A Valkey user group
   turns the default user off
   ([RBAC](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Clusters.RBAC.html)).
   Username support across those clients is a later change. It is not required
   for the auth-token configuration, and Langfuse on this chart still would
   not send a username.
4. **Protected-hook commissioning cannot create users on ElastiCache.**
   `ACL SETUSER` is a restricted command. The worker ACL recipe also contains
   `reset`, which an ElastiCache access string rejects. Steady-state protected
   workers can use users that were created in the ElastiCache API. Commissioning
   against this cache cannot. The main API, worker, and dispatcher do not call
   `CONFIG` or `ACL SETUSER`.

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

Access string for the default user that Curie and Langfuse share:

```
on ~* &* +@all
```

`+@all` is the least restrictive string ElastiCache documents. The engine still
rejects the node-based restricted commands, so the string must not name them
and must not contain `reset`. `&*` keeps pub/sub channels open. The kill path
publishes and subscribes. A new user created with key and command grants but
no channel grant cannot do that.

A Curie-only cache, with Langfuse pointed elsewhere, can use a tighter string.
The proof ran this one on Valkey 8.1.10:

```
on ~* &* +@all -@admin -@dangerous +info|server
```

On that server the whole `INFO` command is in `@dangerous`, so `+info|server`
is what puts `INFO server` back. Protected scripts read `run_id` from that
section. `KEYS`, `ACL SETUSER`, `ACL GETUSER`, and `CLIENT KILL` are also in
those categories and stay denied. Do not put `-@dangerous` on the shared
Langfuse user until BullMQ's command set is checked. Do not paste the
protected-hook worker ACL recipe into ElastiCache. It contains `reset`.

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

`redis-py` 8.1 (the version pinned for the worker) sends `AUTH <password>` when
the username is empty, `CLIENT SETINFO` on connect (a `ResponseError` is
ignored), and `SELECT` only when the database is not 0. The protocol stays
RESP2 unless a caller asks for RESP3. Maintenance notifications stay off unless
RESP3 is selected. The worker's pressure client turns them off explicitly.
`health_check_interval` stays 0. The worker sets the socket timeout to the
blocking read plus 5 seconds so an idle `XREADGROUP` is not cut by the client.

## Call paths

Verdicts are for the auth-token, cluster-mode-disabled cache above. "Lost write"
means an acknowledged write that the replica did not have at failover.

| Path | Verdict | Lost write |
| --- | --- | --- |
| API, worker, and dispatcher connections (`Settings.valkey_dsn`, `WorkerConfig.valkey_client_kwargs`, `build_redis`) | Client config: `VALKEY_TLS=true`, primary host, auth token. No code change. | A connect error during failover retries. A READONLY response does not. |
| Connector proxy grant spend (`_set_nx_ex` in `apps/worker/src/curie_connector_proxy/server.py`) | Compatible. `rediss://` uses the system CA and TLS 1.2 or newer. `AUTH` is the password only. `SELECT` runs only when the URL path is not 0. Any reply other than `+OK`, including a connection error, is a failed spend. | An acknowledged `SET` that then disappears lets the same grant id be spent again. An error during failover fails closed. |
| Streams and consumer groups (`XADD`, `XGROUP`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, `XCLAIM`, `XPENDING`, `XINFO`, `XRANGE`, `XREVRANGE`, `XLEN`, `XTRIM`) | Compatible. The broker port and the direct producers use these verbs. Multi-key scripts that also touch a stream are safe only because cluster mode stays disabled. | A lost `XADD` drops a turn the producer already treated as queued. A lost `XACK` leaves the entry pending and the group redelivers it, which is the existing at-least-once window. |
| `SET NX EX` dedupe, locks, leases, and single-use grants | Compatible as commands. The parameter group must be `noeviction` or a full node evicts these TTL keys. | The key is gone, so a duplicate event is accepted, a second holder takes a lock or lease, or a grant is spent twice. |
| Lua, including multi-key scripts and `TIME` | Compatible on this mode. Production scripts call the verbs listed below. No script calls `CONFIG`, `DEBUG`, `RANDOMKEY`, or `math.random`. `INFO server` is the protected run-id fence. Multi-key `EVAL` (resume, delivery, channel-read ledger, capacity wait, admission, commissioning, protected markers) hashes to more than one slot and is why cluster mode stays off. `EVAL` is sent directly at most sites. `register_script` uses `EVALSHA` and redis-py falls back to `EVAL` on `NOSCRIPT`, including after a new primary with an empty script cache. | A script the primary acknowledged and the replica did not apply is the same class of loss as the writes inside it. A script that errors mid-failover may have applied or not. Callers that retry a non-idempotent script can double the effect. |
| `MULTI` / `EXEC` and pipelines | Compatible. One primary, so a transaction is not a cross-slot error. | A transaction the replica missed is lost as a group. |
| `SCAN` | Compatible. Production iteration uses `scan_iter`. `KEYS` is not the production path. | None. `SCAN` does not create durable state. |
| Pub/sub kill channel | Compatible. The API `PUBLISH`es and the worker `SUBSCRIBE`s. `EXISTS` on the kill flag still refuses a new run when the publish was missed. No other production pub/sub subscriber was found. Revocation of grants and channel reads is key state, not pub/sub. | A lost `SET` of the kill flag lets a killed agent accept new turns until the flag is written again. A lost `PUBLISH` with the flag still present does not interrupt a turn that is already running. |
| Blocking `XREADGROUP` | Compatible to connect. The socket timeout sits above the block. | The blocked call fails. A connection error is caught and the loop continues. READONLY is not caught. |
| Database index | Compatible while every client stays on 0. The API DSN hardcodes `/0`. | None by itself. |
| `CONFIG`, `DEBUG`, `SAVE`, `BGSAVE`, `FLUSHALL`, `MONITOR`, `REPLICAOF`, `SHUTDOWN`, `MIGRATE`, `SYNC` | The main services do not call them. ElastiCache removes the restricted set at the engine. | None. |
| Protected-hook commissioning (`ACL SETUSER`, `ACL GETUSER`, `ACL WHOAMI`, `CLIENT KILL`) | Incompatible for creating users. See open item 4. A pre-created user can run the steady-state worker if its access string matches the worker rules without `reset`. | Same as the keys those scripts write. |
| CLI queue commands and the turn canary | Compatible. Both take a URL, so `rediss://` and a username in the URL work for those operator tools. They are not the running service. | Same as the stream and set commands they issue. |
| Langfuse on the same cache | Client config only: the chart's `REDIS_*` values. The shared user stays on `on ~* &* +@all` and `noeviction`. | Langfuse's own queue entries can be lost on failover. That is outside Curie's stream contract and still shares this recovery point. |

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

1. A single-use grant or a dedupe key stored with `SET NX EX` is gone, so the
   same grant or event is accepted again.
2. A resume marker or its `XADD` is gone. The wake is dropped if the producer
   does not retry, and duplicated if it does.
3. A lease or a thread lock is gone, so a second holder can acquire it until
   the TTL would have expired.
4. An `XACK` is gone, so the entry stays in the pending list and is redelivered.
5. An `XADD` of a turn is gone, so the producer observed success and the worker
   never reads the turn.
6. The kill flag `SET` is gone, so a killed agent keeps accepting turns until
   the flag is written again. If only the `PUBLISH` is lost, `EXISTS` still
   refuses new runs, and a turn already in progress is not interrupted.

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
10. Failover was not simulated. The stand-in is one process, and `REPLICAOF`
    was renamed away.

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

## What this guide does not change

No application code. The auth token is enough for the API, the worker, the
dispatcher, the connector proxy, and Langfuse. Username plumbing, READONLY
retry inside `StreamConsumer._consume`, and ElastiCache-native commissioning
are recommendations. Cluster mode stays disabled, so multi-key Lua stays valid
and durability stays unavailable.
