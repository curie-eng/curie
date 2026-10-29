# 251. Claim-backed sandbox scratch is bounded by its storage request

Date: 2026-10-29

Status: Draft

This ADR is Draft. It does not authorize implementation. Issue 5158 does.
[ADR 0059](0059-sandbox-is-a-bounded-resource-envelope.md) stays Accepted and
is not edited.

## Context

[ADR 0059](0059-sandbox-is-a-bounded-resource-envelope.md) decision 2 requires
every writable `emptyDir` to declare a positive `sizeLimit`. That bounds node
disk: the kubelet evicts the pod that overruns instead of letting one sandbox
fill the node. The bound is still node ephemeral storage. On a node with about
17Gi of allocatable ephemeral storage, raising the factory workspace or home
ceiling past 15Gi cannot succeed, because the volume remains an `emptyDir` on
that disk.

The eviction notice names the volume (`writable-1`) and the limit. It does not
name the mount path or the values key that produced the size, and it offers no
other backing.

## Decision

1. `scratchVolume.type=emptyDir` stays the default. Every writable `emptyDir`
   still requires a positive `sizeLimit`. ADR 0059 decision 2 still governs
   that kind. A missing `scratchVolume`, including on a release reused from a
   chart that has no such key, means `emptyDir`.
2. `scratchVolume.type=ephemeral` backs only the `workspace` volume and the
   writable volume whose path equals `runner.hardening.home`. Each is a generic
   ephemeral volume (`ephemeral.volumeClaimTemplate`, access mode
   `ReadWriteOnce`). The bound is `requests.storage`, set to the size the
   sandbox envelope already resolved. The volume does not consume node
   ephemeral storage, which is the risk decision 2 guards. The claim dies with
   the pod. An empty `storageClassName` is omitted so the cluster default class
   is used. An explicit empty string is not rendered, because that disables
   dynamic provisioning.
3. `/tmp`, attachments, bundles, and every other writable path stay `emptyDir`
   with a positive `sizeLimit`. Setting `scratchVolume.type=ephemeral` does not
   change them.

An unknown type fails the render and names
`agentSandbox.resourceProfiles.<profile>.scratchVolume.type`. A zero or
negative size still fails for both types and still names the size key that won.

## Consequences

The pod annotation `curie.ai/sandbox-scratch-size-keys` records, for `workspace`
and each `writable-<i>` volume, the values key that won and whether that volume
is eligible for the ephemeral type. Eligibility is not the current type:
`workspace` and the home writable volume are eligible on the default `emptyDir`
render. The worker prints that key when an emptyDir eviction matches the
mounted volume. It appends `scratchVolume.type=ephemeral` only when the stamp
says the volume is eligible. A missing stamp, a volume the pod does not mount,
or a key outside the envelope paths keeps the redacted emptyDir sentence.

Operators opt in per resource profile. The installed default stays `emptyDir`,
so existing clusters keep today's volumes until they change the setting.

## Alternatives rejected

1. Raising the 15Gi `emptyDir` limit. Scheduling uses resource requests, not
   `sizeLimit`, so a higher limit does not keep the pod off the node. The volume
   still uses node ephemeral disk. The kubelet evicts the pod when that disk is
   under pressure or when the volume crosses its `sizeLimit`. On a node with
   about 17Gi of allocatable ephemeral storage, raising the factory ceiling
   leaves the data on that disk.
2. A PVC that outlives the pod. Scratch is claim-scoped. A volume that survives
   the pod would keep another claim's files.
3. Moving a build tool's output directory. That changes what the agent writes,
   not the bound on the volume it already fills.
4. Changing the default type. The installed default must stay `emptyDir` so
   existing clusters keep today's volumes.
5. Editing Accepted ADR 0059. Decision 2 still holds for `emptyDir`. This Draft
   adds a second kind beside it.
