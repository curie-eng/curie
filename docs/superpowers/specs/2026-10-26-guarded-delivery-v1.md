# Guarded delivery v1

Realization under [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), tracked in [#4890](https://github.com/curie-eng/curie/issues/4890). The existing API delivery owner gains an optional conditional append. This is an unwired primitive. No route, caller, worker, approval consumer, provider, activation, SQL table, release version or permission decision changes.

## 1. Existing owner and fixed surface

<!-- @spec GUARDED-DELIVERY-V1-1 -->

Keep claim acquisition, quota charging, settlement and append in their existing delivery owner. Extend its surface with:

```text
DeliveryValidityWindow(lower_seconds, lower_microseconds,
                       upper_seconds, upper_microseconds)
DeliveryEnqueueGuard(fence_key, expected_stable, validity, reservation)
DeliveryGuardRefusal(code)
await enqueue_owned(..., guard=None) -> tuple[bool, str]
identity_delivery_window(facts, limits) -> DeliveryValidityWindow
```

The generic types, exception and guarded append belong to `curie_api.delivery`. The pure T1 conversion belongs to `curie_api.identity.delivery_validity`, importing the generic window and public facts models. Generic delivery must not import HTTP identity schemas. Reuse the existing append script and permanent receipt argument rather than copying an enqueue implementation. No production call site supplies a guard in this slice.

A guard is conditional write input, never an authorization token. Its caller must already have authenticated the sender, matched the immutable credential/configuration, applied any required lifecycle change with positive commit acknowledgement, and repeated original byte validation and current policy under the participating reader/writer boundary. This primitive does none of that. Its reservation must be the original attempt object fixed before acquisition and charging; possession does not prove a configured quota limit was observed successfully.

## 2. Strict local validation

<!-- @spec GUARDED-DELIVERY-V1-2 -->

The window is frozen. All four fields are actual integers, excluding booleans. Seconds range from `-62135596800` through `253402300799`; microseconds range from zero through `999999`. Compare pairs lexicographically and require lower no later than upper. These are inclusive bounds over the complete existing UTC datetime domain.

The guard is frozen and holds the existing frozen BacklogReservation. Before any datastore call revalidate the guard, window and reservation, including constructed or mutated objects. Require nonempty strings for the fence, claim, stream, marker and counter keys; claim, stream, fence and marker are pairwise distinct, and the counter aliases none of them. Require the existing `pending:` plus 32 lowercase hexadecimal owner form and the original 32 lowercase hexadecimal reservation token. Reservation window is an actual positive integer. Require a `stable:` token with a UUID suffix accepted by the existing fence reader; preserve its exact bytes without UUID normalization. No key or authority comes from the payload.

A guarded call cannot supply hook receipt key/prefix arguments. Reject this combination and malformed caller input before I/O with fixed ValueError text, without the input value or a sensitive exception chain. Existing guardless validation is unchanged. Guard automatic representation reveals no key, token or nested reservation value. These ordinary internal Python objects are not an introspection confidentiality boundary.

## 3. Exact T1 conversion

<!-- @spec GUARDED-DELIVERY-V1-3 -->

The pure helper accepts actual ValidatedFacts and FreshnessLimits, revalidates their ordinary model shape and rejects incoherent outcome/observation/expiry pairs. Resolved facts require identity and membership evidence; inactive and unresolved facts carry identity observation only, with their existing T1 outcome shapes. The caller must supply a successful T1 result and the same limits used to obtain it. Shape validation cannot authenticate coherent fabricated Python values or prove which limit object originally produced an expiry. Invalid or incoherent inputs and empty intersections raise ValueError with fixed text `invalid_identity_delivery_window`, no sensitive input or exception chain, before any I/O.

Use the exact Fraction expiry E already supplied by [T1](2026-10-23-http-identity-facts-v1.md). For an observation with age limit M milliseconds, source lag G milliseconds and skew S milliseconds, its accepted interval is closed:

```text
lower = E - Fraction(M, 1000) + Fraction(G, 1000)
upper = E
```

This equals `[A-S/1000, A+(M-G-S)/1000]` for exact observation instant A. Reject `G+S > M`. Intersect identity and membership intervals whenever membership evidence is present, irrespective of a later role filter. Do not use a private timestamp parser, floating point, rounded up expiry or an additional grace period.

Intersect with the complete UTC datetime domain first. On the server microsecond lattice take `ceil(lower * 1000000)` and `floor(upper * 1000000)` exactly. An empty intersection refuses. Encode with Python floor divmod by one million into signed floor seconds and a nonnegative microsecond limb; minus one half second is `(-1, 500000)`. Preserve submicrosecond input precision through this conservative conversion. The helper decides no identity kind, role match or permission.

## 4. Explicit script protocol and duplicate behavior

<!-- @spec GUARDED-DELIVERY-V1-4 -->

Guardless calls retain exactly the existing two key or three key hook layout and seven arguments. Guarded calls use four keys and an explicit mode, so a guard cannot accidentally activate hook metadata handling:

| Position | Guarded value |
| --- | --- |
| KEYS 1, 2 | Existing claim and stream |
| KEYS 3, 4 | Expected fence key and original reservation marker key |
| ARGV 1 through 7 | Existing arguments, with empty hook receipt prefix |
| ARGV 8 | `identity_guard_v1` |
| ARGV 9, 10 | Exact expected stable token and original reservation token |
| ARGV 11 through 14 | Lower seconds, lower micros, upper seconds, upper micros |

Validate guarded arity, mode, distinct keys and bounded numeric limbs before mutations. Numeric limbs use canonical decimal integer spelling and their specified ranges. Never multiply seconds into an unsafe epoch microsecond Lua double or parse fractional/exponential timestamps. Seconds and microseconds separately fit exact Lua integer valued numbers.

After validation, read the claim. A different current owner or permanent stream receipt returns the existing `(False, current)` result without append. Retained duplicates remain readable after the guard expires, the fence rotates or the marker expires. The caller's authenticated receipt disclosure rules still apply; this return does not grant permission. A caller cannot pass a stream receipt as the expected pending owner.

## 5. Conditional append and settlement

<!-- @spec GUARDED-DELIVERY-V1-5 -->

Both owned pending and absent claim append arms must pass, in this order:

1. Current fence equals the exact expected stable token. Missing, pending or changed fence refuses `fence_changed`.
2. The existing reservation marker equals the original reservation token. Missing, expired, mismatched or refunded marker refuses `reservation_lost`.
3. Sample actual server TIME inside this same script after these reads. Validate its two decimal limbs and supported ranges. Invalid sample refuses `clock_unavailable`; a sample outside the inclusive window refuses `outside_validity`.
4. Only then execute the existing absent claim reacquisition if needed, append once, and publish the existing nonexpiring claim receipt. No second append path bypasses these checks.

Guard refusal mutates no claim, stream, fence, marker or counter. It neither refreshes nor recreates a marker or a window. A later owner uses the unchanged failure settlement with the original reservation. If refund wins before delayed EVAL, marker removal prevents resurrection even after the pending claim disappears. If enqueue wins first, its permanent receipt makes existing settlement preserve the entry and charge, including lost replies and cancellation. A successor pending claim or receipt remains protected. Charging the same old reservation again to rescue it is outside this contract.

The script serializes these checks with its append against other Valkey commands. It is not a cross store transaction or an atomic operating system clock operation. Lua errors do not roll back earlier writes. Existing exceptions, uncertain command outcomes and settlement obligations remain real; no successful duplicate is invented to hide them.

## 6. Bounded refusal and compatibility

<!-- @spec GUARDED-DELIVERY-V1-6 -->

Map script results `-2`, `-3`, `-4`, `-5` to a distinct DeliveryGuardRefusal carrying only `fence_changed`, `outside_validity`, `reservation_lost`, `clock_unavailable` respectively. Existing `-1` hook receipt conflict is unchanged. Refusal text contains no key, token, payload, timestamp or datastore reply. Do not send a guard refusal through duplicate_stream_id. Unexpected script protocol is an internal error; Redis transport/protocol errors and cancellation keep their original exception identity.

Existing successful/current owner tuples, transport metadata, hook receipt arity, orphan evidence rejection, permanent receipts, quota accounting and cleanup remain unchanged. No automatic retry, new counter, SQL ledger, background task or alternate append owner is introduced.

## 7. Pinned engine observations and limits

<!-- @spec GUARDED-DELIVERY-V1-7 -->

On a disposable Valkey **8.1.10** engine, INFO identifies the Valkey version and Redis compatibility **7.2.4**. A redis-py EVAL measurement observed string limbs inside Lua and actual TIME followed by an owned write and GET readback in the same script. Equivalent reproduction commands are:

```bash
valkey-cli INFO server
valkey-cli EVAL "local t=redis.call('TIME'); return {type(t[1]),type(t[2])}" 0
valkey-cli EVAL "local t=redis.call('TIME'); redis.call('SET',KEYS[1],ARGV[1]); return t" 1 "$owned_probe_key" probe
valkey-cli DEL "$owned_probe_key"
```

The probe key belongs only to that disposable test and is removed after readback. Script TIME supplies the execution sample; the client library's TIME conversion is not a substitute for measuring that script value. Separate bounded signed seconds/microsecond comparisons preserve preepoch and year 9999 arithmetic without unsafe large doubles.

These observations do not prove persistent synchronization, real UTC accuracy, or an uncertainty allowance between API and server clocks. Clock relationship, participating writer lifetime, immutable registry selection, original byte revalidation and worker current authority remain prerequisites for a later supported producer. No activation claim follows from this primitive.

## 8. Independent evidence matrix

<!-- @spec GUARDED-DELIVERY-V1-8 -->

Independent failing tests precede implementation. Use actual owned Valkey and production delivery/reservation/settlement functions. Never substitute a datastore or the production server clock, change shared server time/ACLs, or delete keys outside the fixture's recorded namespace.

| Criteria | Required evidence |
| --- | --- |
| 1, 2 | Frozen/strict models, invalid constructed values, alias/owner/stable token/refusal validation before I/O, safe representation, no active caller. |
| 3 | Real T1 synthetic fixtures, all definitive outcomes, independent membership bounds, lag/skew, exact equality, submicrosecond precision, negative epoch, full datetime endpoints, huge limits, empty lattice intersection and incoherent pairs. Compare accepted instants with T1's exact equation. |
| 4, 5 | Real owned and absent append arms; each refusal leaves state identical; original payload/transport and one permanent receipt; receipt/pending duplicates after expiry, rotation and marker loss. |
| 5 | Two real clients and controlled command barriers for refund before delayed EVAL and enqueue before lost reply/cancellation settlement; successor, marker expiry and quota refusal preserve correct entry/charge ownership. |
| 5, 7 | Actual current TIME allows a bounded current interval and refuses clearly future/expired intervals. Full domain/equality comparator cases use a test only arithmetic script and do not claim historical/future runtime clock qualification. |
| 6 | Existing hook receipt, ingress failure recovery, quota/lease and exception/cancellation controls remain green; no fake stream ID or implicit retry. |

Use focused checks locally and the exact final head's required CI. Record unavailable prerequisites as errors, never a pass, skip or waiver for the required mechanism proof.
