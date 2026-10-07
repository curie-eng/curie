# Protected hook dependency observations

Observed 2026-10-02 against disposable local backing services. This records
measured dependency behavior for ADR 0191. It does not qualify a deployment,
runner, connector or protected delivery implementation. No platform ingress,
model turn, production broker or external message was exercised.

## Versions and reproduction boundary

* Valkey image: `valkey/valkey:8.1.10-alpine`, resolved image digest
  `sha256:d2e18f3410b6f616de1417f570fa55261af2898b9c5b2cfb6781ce2373ea43d1`.
* `admin.info("server")` returned `valkey_version=8.1.10` and the compatibility
  field `redis_version=7.2.4`. The latter is not the Valkey version.
* Python `redis.__version__` returned `8.1.0`.
* `SELECT version()` returned PostgreSQL 16.15, aarch64, Alpine build.

The Python calls below were executed with `redis.Redis` clients named
`admin`, `producer`, `consumer`, and `issuer` against the disposable broker.
They used `decode_responses=True`, a two-second socket timeout and
`Retry(NoBackoff(), 0)`. Supply independently generated disposable credentials
and connections when reproducing; no credentials or local endpoint identities
are recorded here. These are dependency probes, not production commands.

The broker disabled its default user. The administrative probe identity had
all commands and keys. The other identities had these key/command rules,
loaded from an ACL file using `admin.execute_command("ACL", "LOAD")`:

```text
producer:
  %R~proof:* %RW~deliveries:*
  +ping +hello +auth +client|setinfo +client|setname
  +eval +evalsha +script|load +time +get +hget +set +hset +exists +xadd
  (+eval +evalsha %RW~proof:* %RW~deliveries:*)
consumer:
  %RW~deliveries:* %R~proof:*
  +ping +hello +auth +client|setinfo +client|setname
  +xreadgroup +xautoclaim +xclaim +xpending +xack +xgroup +xrange +xlen +get +time
issuer:
  %RW~proof:*
  +ping +hello +auth +client|setinfo +client|setname
  +get +set +del +hget +hset +time
```

Passwords and their hashes are omitted. The role grammar above excludes each
user's `on` and authentication parameters; those were distinct in the fixture.
This inventory is a measured probe configuration, not the complete application
ACL inventory.

## EVAL selectors and atomic broker-time check

Initially, producer had the main rules above without the parenthesized EVAL
selector. Declaring `proof:measurement` in EVAL's KEYS then raised
`NoPermissionError`, although the script only GETs that key. Adding the separate
selector allowed declared-key validation. Inner command checks still refused
proof writes. The exact successful script and calls were:

```python
import json
import time

issuer.set("proof:measurement", json.dumps({
    "generation": "g1", "expiry_ms": int(time.time() * 1000) + 60000,
}))
script = """local proof=redis.call('GET',KEYS[1]); if not proof then return {'refused'} end
local v=cjson.decode(proof); local now=redis.call('TIME'); local ms=now[1]*1000+math.floor(now[2]/1000)
if v.generation~=ARGV[1] or v.expiry_ms<=ms then return {'refused'} end
local claim=redis.call('SET',KEYS[2],'claimed','NX'); if not claim then return {'duplicate'} end
local id=redis.call('XADD',KEYS[3],'*','payload','measurement'); return {'enqueued',id,tostring(ms)}"""
result = producer.eval(script, 3, "proof:measurement", "deliveries:claim",
                       "deliveries:stream", "g1")
assert result[0] == "enqueued"
```

These calls were refused:

| Executed call | Observed exception |
| --- | --- |
| `producer.set("proof:measurement", "forged")` | `NoPermissionError` |
| `consumer.set("proof:measurement", "forged")` | `NoPermissionError` |
| `producer.eval("return redis.call('SET',KEYS[1],'forged')", 1, "proof:measurement")` | `ResponseError` |
| `issuer.xadd("deliveries:stream", {"payload": "forged"})` | `NoPermissionError` |
| `producer.xrange("deliveries:stream")` | `NoPermissionError` |
| `consumer.acl_setuser("forbidden", enabled=True)` | `NoPermissionError` |

The selector admits EVAL's declared keys without granting SET permission on
proof keys to an inner command. Granting both SET and read/write proof keys in
one ordinary selector would lose this separation. This result does not prove
that every future application command is correctly scoped.

The same script returned `['refused']` with no corresponding claim key for:

```python
issuer.delete("proof:measurement")
producer.eval(script, 3, "proof:measurement", "deliveries:claim2",
              "deliveries:stream", "g1")
assert not admin.exists("deliveries:claim2")

issuer.set("proof:measurement", json.dumps({
    "generation": "g2", "expiry_ms": int(time.time() * 1000) + 60000,
}))
producer.eval(script, 3, "proof:measurement", "deliveries:claim3",
              "deliveries:stream", "g1")
assert not admin.exists("deliveries:claim3")

issuer.set("proof:measurement", json.dumps({
    "generation": "g1", "expiry_ms": 1,
}))
producer.eval(script, 3, "proof:measurement", "deliveries:claim4",
              "deliveries:stream", "g1")
assert not admin.exists("deliveries:claim4")
```

## Consumption and retained credential refusal

The ordinary fixture password PINGed its own broker successfully. Connecting
with that password to the independent probe broker, either as its default
user or named consumer, failed with `AuthenticationError`.

The consumer executed these calls successfully:

```python
consumer.xgroup_create("deliveries:stream", "measurement-group", id="0")
rows = consumer.xreadgroup("measurement-group", "first",
                          {"deliveries:stream": ">"}, count=1)
assert rows
reclaimed = consumer.xautoclaim("deliveries:stream", "measurement-group",
                                "second", min_idle_time=0,
                                start_id="0-0", count=1)
assert reclaimed[1]
assert consumer.xack("deliveries:stream", "measurement-group", result[1]) == 1
```

Zero idle here measures command availability only. Product recovery must retain
its specified positive-idle compare-and-claim behavior.

## Password replacement does not terminate authenticated consumption

A separate consumer connection named `live` authenticated before replacement.
A second connection named `blocked` waited in XREADGROUP BLOCK 0 on another
thread. The exact calls included:

```python
live.ping()
live.client_setname("measurement-retained-consumer")
blocked.client_setname("measurement-blocked-consumer")
# Executed on the second thread; CLIENT LIST confirmed its blocked flag.
blocked.xreadgroup("measurement-group", "blocked",
                  {"deliveries:stream": ">"}, count=1, block=0)

# The following calls executed on the administrative thread.
admin.execute_command("ACL", "SETUSER", "consumer", "resetpass",
                      ">" + new_disposable_password)
assert live.xlen("deliveries:stream") == 1
killed = admin.execute_command("CLIENT", "KILL", "USER", "consumer")
assert killed >= 1
```

After SETUSER, the existing authenticated connection still read XLEN. A new
connection with the old password raised `AuthenticationError`. CLIENT KILL
USER killed three consumer connections in this fixture, including the blocked
reader. The blocked call terminated with `ConnectionError`. The retained
client's reconnect with its old password then raised `AuthenticationError`.
A client with the new password successfully read XLEN. Thus changing a password
alone is not live-session revocation. This does not measure orchestrator secret
distribution or prevented image changes.

The fixture restored its original roles with ACL LOAD and deleted its probe
keys afterward. No broker credential or probe payload is published here.

## Metadata permission subset realization

The metadata realization uses Valkey 8.1.10 and redis client 8.1.0. A new
independently owned disposable broker disables only its own default user and
starts distinct source writer, control reader and fixture provisioner
principals. No shared service ACL or production identity is changed.

The permission reset tokens `-@all resetkeys resetchannels clearselectors`
removed prior command, key, channel and selector grants while preserving the
principal's enabled state and password. The source writer's emitted rules
allowed the existing SourceFence reservation, idempotent retry and matching
ordinary publication. A newer reservation closed active authority; stale
publication and conflicting CAS could not replace it.

The reader's main read selectors on `protected:source:*` and
`protected:control:*`, combined with a separate
`(+eval %RW~protected:source:* %RW~protected:control:*)` declared key selector,
allowed EVAL containing GET. Inner SET remained refused with ResponseError;
declaring an admission key refused with NoPermissionError. Direct writes,
payload reads, stream consumption, pubsub and administration also refused.

`+info|server` allowed INFO server and refused INFO clients, memory, all and
the no section form. INFO server clients and INFO server memory returned
only the server section on this version; this is not a general assertion
that arbitrary extra INFO arguments are refused. The operation facade
requests exactly INFO server and TIME. Broker identity and time are separate
observations, not an atomic admission proof.

The actual command `uv run --frozen pytest packages/protected-hooks/tests -q`
passed 830 tests in 2.37 seconds with exit 0 after the separate failing test
commit. Its real broker tests include prior overprivilege removal, ordinary
and default authentication refusal, fixed observation and retained session
revocation: password replacement alone preserved an authenticated connection;
CLIENT KILL USER terminated it and old password reconnect refused. The fixture
removed its exact owned container and credential file afterward.

This measures the two metadata permission subsets. It does not establish the
complete enqueue, worker or verifier ACL inventory, TLS/network isolation,
runtime qualification, source activation, restart recovery or end to end
protected delivery support.

## Reported PostgreSQL transaction-lock observation

The separately reported PostgreSQL 16.15 measurement used this exact statement:

```sql
SELECT pg_advisory_xact_lock(hashtextextended('hook-source:' || $1::text, 0));
```

A concurrent transaction using the same parameter waited; COMMIT of the first
transaction released it. A reproduction with two SQL sessions is:

```sql
-- In both sessions, prepare this statement with the same test-only parameter.
PREPARE probe_lock(text) AS
SELECT pg_advisory_xact_lock(hashtextextended('hook-source:' || $1::text, 0));

-- Session A:
BEGIN;
EXECUTE probe_lock('probe-agent');

-- Session B: the EXECUTE waits.
BEGIN;
EXECUTE probe_lock('probe-agent');

-- Session A releases the transaction-scoped lock:
COMMIT;

-- Session B can then finish:
COMMIT;
```

This report records the supplied exact statement and wait/commit observation;
the reproduction recipe is not claimed as an additional independently executed
campaign. Disconnect recovery, concurrent policy writes, restart durability,
control-plane fencing and the complete application role inventory remain
separate evidence requirements.

## Source-control CAS and restart identity

A supplementary disposable user `sourcewriter` had only
`~protected:source:probe +ping +auth +client|setinfo +eval +get +set`.
These executed calls measured conditional update, not application crash recovery:

```python
sourcewriter.set("protected:source:probe", "1")
cas = "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('SET',KEYS[1],ARGV[2]); return 1"
assert sourcewriter.eval(cas, 1, "protected:source:probe", "1", "2") == 1
assert sourcewriter.eval(cas, 1, "protected:source:probe", "1", "3") == 0
assert sourcewriter.get("protected:source:probe") == "2"
```

Sourcewriter SET of `proof:restart`, sourcewriter GET of a delivery key and
issuer SET of `protected:source:probe` each raised `NoPermissionError`.
For producer, the supplementary read rule `%R~protected:source:probe` and
an EVAL-only selector declaring that key allowed admission reads while direct
SET still raised `NoPermissionError`; SET inside Lua raised `ResponseError`.

`producer.eval("return redis.call('INFO','server')", 0)` initially raised
`ResponseError` because producer lacked INFO permission. After the executed
call below, that EVAL succeeded:

```python
admin.execute_command(
    "ACL", "SETUSER", "producer", "+info", "%R~protected:source:probe",
    "(+eval +evalsha %RW~proof:* %RW~deliveries:* %RW~protected:source:probe)",
)
```

The successful EVAL included `run_id:`. INFO is supported inside Lua on
this measured version; permission must be explicit. The producer then used:

```python
old_run_id = admin.info("server")["run_id"]
issuer.set("proof:restart", json.dumps({"run_id": old_run_id}))
restart_script = """local source=redis.call('GET',KEYS[1]); if source~='2' then return 'source-refused' end
local proof=cjson.decode(redis.call('GET',KEYS[2])); local actual=redis.call('INFO','server')
local runid=string.match(actual,'run_id:([%w]+)'); if proof.run_id~=runid then return 'stale-run-id' end
redis.call('SET',KEYS[3],'accepted'); return 'accepted'"""
assert producer.eval(restart_script, 3, "protected:source:probe", "proof:restart",
                     "deliveries:restart-claim") == "accepted"
admin.delete("deliveries:restart-claim")
```

The disposable instance used AOF, `appendonly yes`, `appendfsync everysec`.
After waiting 1.2 seconds, its container was orderly restarted. Its container
identity remained unchanged; its process `run_id` changed. The host's allocated
port changed, requiring endpoint rediscovery before connecting again. This is
an observation of the disposable local host, not a guaranteed Docker behavior.

After rediscovery and authenticated reconnection, these calls succeeded:

```python
assert admin.info("server")["run_id"] != old_run_id
assert admin.get("protected:source:probe") == "2"
assert json.loads(admin.get("proof:restart"))["run_id"] == old_run_id
assert producer.eval(restart_script, 3, "protected:source:probe", "proof:restart",
                     "deliveries:restart-claim") == "stale-run-id"
assert not admin.exists("deliveries:restart-claim")
```

Thus source data and prior proof survived that orderly AOF restart, while
comparison with actual process identity inside the same script refused stale
proof without a claim. Persisting readiness data is not sufficient to establish
fresh readiness after restart. Bind actual broker process identity alongside
authenticated endpoint identity and provisioning epoch; process run_id alone
does not establish those other identities.

The original ACL file and roles were restored and supplementary keys removed.
AOF crash loss, restore/rollback, failover and application reconciliation were
not measured. These orderly-restart observations cannot establish a general
durability guarantee or qualify production broker admission.

## Lua errors do not roll back earlier writes

A further executed probe used a wrong-type key to produce a runtime error after
writing an intent and an explicit-ID entry:

```python
admin.set("deliveries:wrongtype", "not-a-stream")
partial_script = "redis.call('SET',KEYS[1],'pending:1-0');redis.call('XADD',KEYS[2],'1-0','payload','probe');return redis.call('XADD',KEYS[3],'*','payload','error')"
producer.eval(partial_script, 3, "deliveries:repair-intent",
              "deliveries:repair-stream", "deliveries:wrongtype")
```

EVAL raised `ResponseError`. After that error, both assertions succeeded:

```python
assert admin.get("deliveries:repair-intent") == "pending:1-0"
assert admin.xrange("deliveries:repair-stream", "1-0", "1-0") == [
    ("1-0", {"payload": "probe"}),
]
```

`producer.xadd("deliveries:repair-stream", {"payload": "probe"}, id="1-0")`
then raised `ResponseError`; `admin.xlen("deliveries:repair-stream")` remained
one. Probe keys were deleted afterward. This confirms isolation during a script
is not rollback of prior commands. Expected wrong-type refusals must be checked
before writes. Partial intent recovery must inspect the reserved explicit ID
and committed binding rather than blindly append a second entry.

## Existing legacy counter datatype

Observed 2026-10-03 against disposable local PostgreSQL with `asyncpg` 0.31.0.
The following read-only SQL was executed through an authenticated connection;
no credential, customer identity or local endpoint is reproduced here.

```sql
SHOW server_version;
SELECT data_type, udt_name, column_default, is_nullable
FROM information_schema.columns
WHERE table_schema = 'curie'
  AND table_name = 'agents'
  AND column_name = 'hook_generation';
```

The server reported `16.15`; the column query returned
`('integer', 'int4', '0', 'NO')`. The existing counter is therefore a non-null
Postgres INTEGER with default zero, not the new source policy's BIGINT.
This read measured type/default/nullability only; it did not mutate a counter
or exercise overflow. Source administration must check the positive int4
limit before revocation rather than assume BIGINT allocation or reset a key.

## Enqueue recipe ZRANGE on the quota key

Observed 2026-10-06 against an owned disposable `valkey/valkey:8.1.10-alpine`
container (resolved digest
`sha256:d2e18f3410b6f616de1417f570fa55261af2898b9c5b2cfb6781ce2373ea43d1`),
started with a unique owner label, persistence disabled, loopback only, and
removed by that exact name afterward. `INFO server` returned
`valkey_version=8.1.10`; Python `redis` 8.1.0 with RESP3, a two second timeout
and `Retry(NoBackoff(), 0)`. The default user was disabled and a fixture
provisioner seeded keys. ACL evaluation does not depend on the transport, so
this measurement used plaintext loopback; the TLS transport cases run in
`packages/protected-hooks/tests/test_enqueue_transport.py`. No credential or
endpoint is recorded here.

The candidate enqueue principal was installed with
`ACL SETUSER enqueue reset on >(secret)` and exactly the existing
`admission_acl_rules("enqueue")` recipe, except that the quota selector became
`(+type +zadd +zcard +zrem +zscore +zrange %RW~protected:admission:quota)`.
With members `a`, `b` and `c` at scores 10, 20 and 30 seeded on
`protected:admission:quota`, the enqueue principal executed:

* `ZRANGE protected:admission:quota 0 63` returned `[a, b, c]`, score order.
* `ZRANGE protected:admission:quota 0 -1 WITHSCORES` returned the members with
  scores 10.0, 20.0 and 30.0.
* `EVAL "return redis.call('ZRANGE',KEYS[1],0,-1)" 1 protected:admission:quota`
  returned `[a, b, c]`.
* `ZRANGE` directly on `protected:admission:intent:x`,
  `protected:admission:quota:other`, `curie:runs`, `protected:source:x` and
  `protected:control:x` each refused with `NoPermissionError`.
* The same EVAL declaring each of those keys refused with a NOPERM error
  (inner command refusal, or declared key refusal for
  `protected:admission:quota:other`, which no EVAL selector names).
* `ZRANGESTORE protected:admission:binding:x protected:admission:quota 0 -1`
  and `ZPOPMIN protected:admission:quota` refused with `NoPermissionError`.

ZRANGE therefore reads the quota members in score order and is granted on no
other key. The recipe in `admission_acl.py` adds `+zrange` to the quota
selector only, after this record.
