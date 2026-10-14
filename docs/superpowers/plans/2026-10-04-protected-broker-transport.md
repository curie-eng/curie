# Protected broker authenticated metadata transport

This is the next unwired Task 2 slice of the
[parent plan](2026-10-02-protected-hooks.md), following the
[metadata realization](2026-10-04-protected-broker-metadata.md). Its contract is
[LANE-2/3 transport](../specs/2026-10-02-protected-hook-lane.md#authenticated-metadata-reader-transport).
It targets public main through a dependent branch until its prerequisite
merges. It does not enable protected source activation or delivery.

## Ownership and order

Own only the transport specification, this plan, the new internal
`broker_transport.py`, its tests, a direct cryptography dependency and derived
lock metadata, plus anonymous dependency observations. Do not alter existing
broker metadata behavior, source policy, public authentication, manifest shape,
WorkerConfig, service boot, kernel, consumer, thread locks, markers, ACI,
plugin format, charts or another owner's work.

Commit specification and plan first. An independent test author writes
behavioral tests from the contract. The integration owner observes the failure
against unchanged implementation and commits tests alone. A separate
implementer follows in a later commit. Root alone stages and commits. Review
scope, security and implementation independently before publication.

## Dependency observation and limits

Read only inspection executed on 2026-10-04 used the worktree interpreter:

```python
import inspect
import importlib.metadata
import redis
import redis.connection
print(redis.__version__)
print(importlib.metadata.version("cryptography"))
for method in ("_connect", "_wrap_socket_with_ssl", "connect_check_health",
               "on_connect_check_health"):
    print(inspect.getsource(getattr(redis.connection.SSLConnection, method)))
```

Observed redis 8.1.0 and cryptography 50.0.1, matching the lock. The local
interpreter was Python 3.14.3 with OpenSSL 3.6.2. SSL wrapping uses
`server_hostname=self.host`; no separate server name argument exists. Socket
creation and wrapping precede on connect authentication. RESP2 named AUTH has
a password only compatibility fallback on wrong argument count; explicit
RESP3 uses HELLO AUTH instead. These are source inspection observations, not
a real TLS handshake or Valkey command campaign. Deployment interpreter and
TLS behavior on deployment interpreters remain unmeasured. The package must
declare cryptography directly rather than rely on its present transitive
installation.

A bounded disposable TLS measurement on the same local interpreter used
Valkey 8.1.10 alpine with redis 8.1.0 and cryptography 50.0.1. The probe used
explicit RESP3, required chain and hostname validation, DER leaf SPKI hashing
and a named fixture principal. Its observed order was verified TLS, verified
pin, HELLO, then client handshake and PING/INFO. Wrong pin, wrong hostname and
untrusted CA each refused before HELLO or AUTH. Wrong named authentication
issued one HELLO and no password only AUTH. Plaintext to the TLS only endpoint
refused. INFO server returned the actual Valkey version and canonical run_id;
TIME returned the broker clock. The disposable execution used an explicit
container user, private directory and credential files, and exact container
identity and owner label cleanup. No production transport or application
qualification was exercised. This local observation does not establish Linux
CI file ownership portability; the realizing tests must use the host file
owner explicitly.

## Interfaces and proof

Implement the closed `AuthenticatedMetadataReader.connect(manifest,
credential, ca_pem)` classmethod and frozen, slotted
`MetadataReaderCredential(username, password)` in `broker_transport.py`. The
credential representation redacts both fields. Connect eagerly verifies the
transport and identity; no unverified public constructor is available. CA
input is a string containing PEM certificates only, without private keys.
All public transport validation failures, including SourceFenceInvalid, use
BrokerMetadataUnavailable; existing AuthorityMetadataReader behavior remains
unchanged. Use only the specified metadata
operations. A pin checking SSLConnection subclass rejects after normal TLS
validation and before authentication, closes failed sockets and verifies
manifest run_id after authentication on each connection. Reject unequal
endpoint host and server name before I/O. Use one private serialized connection
and never expose its underlying Redis client.

Use a uniquely owned disposable Valkey 8.1.10 TLS instance, ephemeral fixture
CA/server certificates, disabled default user and distinct named principals.
Keep keys, passwords, certificate private keys and endpoints in ignored private
state. Register cleanup before startup; remove only recorded owned resources.
Never modify another service or inspect production credentials.

Positive tests cover actual source/control reads, validated clock/identity and
reconnection to the same authorized identity. Negative tests cover wrong CA,
hostname, pin, credentials and run_id; prove certificate refusals precede AUTH
and failed named authentication does not fall back to default. Force reconnect
and certificate/broker identity changes using only owned fixtures; verify
refusal before application reads. Cover malformed input and unsupported tuple
without I/O, safe exceptions and terminal close. Record actual command/version
observations before claiming supported dependency behavior. Retained session,
restart, role separation and read results are distinct observations, not
qualification or atomic readiness.

## Verification and remaining boundaries

Required local proof is the real isolated TLS broker campaign and affected
protected hooks regression suite, Ruff, mypy, import and documentation checks.
Classify all seven tiers in the execution record and PR body. Other tiers are
not applicable to this unwired library transport: no service wiring, installed
schema, release path, chart/substrate, runner, provider, external delivery,
factory or bundle behavior changes. The publication guard conservatively maps
any root lock change to local release verification. This change only declares
the already resolved cryptography 50.0.1 as a direct dependency and changes no
resolved version. Record that mapped tier as required with an explicit
discovery waiver linked to open #3603; full protected artifact qualification
remains required before activation. Required repository CI checks still apply.

Source activation, atomic admission/receipts, complete role inventories,
preventive provisioning, guarded worker execution and actual provider/connector
qualification remain parent tasks. Keep #3603 open and the default protected
resolver unavailable. No downstream import, deployment or external message is
part of this slice.
