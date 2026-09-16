---
seam: Sealed credential (cluster-sealed connector secrets)
kind: SOFT
impls: 2 halves (Rust sealer, Python opener) over one frozen wire
grade: not separately graded
epics:
  - "#1240"
order: 21
---
# INTERFACE: Sealed credential (cluster-sealed connector secrets)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 2 halves (Rust sealer, Python opener) over one frozen wire &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

A bundle can carry its own connector credential, encrypted to the cluster that
will run it (ADR-0094). The blob is useless without that cluster's private key,
so it lives in the agent repository beside the connector that reads it and an
agent repo becomes self-contained: clone it, point a cluster at it, tools work.

The seam is **a crypto wire format, not a code interface**, and that is
deliberate — which makes it SOFT in exactly the sense the catalog means. The
Rust CLI seals (`cli/src/sealing.rs`) and the Python worker opens
(`apps/worker/src/curie_worker/sealing.py`), and the two halves share no type,
no package and no generated schema. The opener's module docstring names the
constraint outright: **wire compatibility with the CLI is the load-bearing
property**. It is held not by an assumption but by both sides naming the same
primitive rather than assembling their own, because two "compatible" AEAD stacks
that disagree about nonce derivation would fail only at deploy, on a credential
nobody can read out of a log.

A second implementation here is a second sealer (another language, an authoring
tool) or a second opener (a KMS-backed one). Either must satisfy the format
below verbatim; there is nothing else to implement against.

## Current contract

- **Primitive.** A libsodium sealed box, `crypto_box_seal`: X25519 plus
  XSalsa20-Poly1305, anonymous sender. Chosen because "encrypt anonymously to a
  recipient public key" is exactly one named primitive, so there is no nonce
  discipline, key derivation or AEAD pairing for this repository to get wrong.
  A sealed value authenticates nothing about who sealed it, and nothing
  downstream may believe it does.
- **Envelope.** Base64 of the raw sealed box, which is an ephemeral public key
  followed by the ciphertext. Standard alphabet with padding on the Rust side;
  the Python side decodes with validation on. There is **no prefix, no version
  byte and no type tag**. Re-sealing the same value produces a different blob, so
  a repository's history does not leak that two secrets are equal.
- **Keys.** Base64 of exactly 32 raw bytes, both halves. The private key reaches
  the opener as `CURRENT_KEY_ENV`
  (`apps/worker/src/curie_worker/sealing.py::CURRENT_KEY_ENV`), which is
  `CURIE_SEALING_PRIVATE_KEY`.
- **Rotation is two keys, tried in order.** `active_private_keys`
  (`apps/worker/src/curie_worker/sealing.py::active_private_keys`) returns the
  current key first and then `PREVIOUS_KEY_ENV`
  (`apps/worker/src/curie_worker/sealing.py::PREVIOUS_KEY_ENV`),
  `CURIE_SEALING_PREVIOUS_PRIVATE_KEY`, skipping either when blank. ADR-0094
  requires the overlap: without it, rotating the cluster keypair invalidates
  every blob every agent repository has committed, all at once. An empty key set
  is a legitimate state — an install using only operator-supplied credentials
  never needs one.
- **Opening.** `open_sealed`
  (`apps/worker/src/curie_worker/sealing.py::open_sealed`) base64-decodes, then
  tries each active key, skipping a malformed key rather than treating it as
  fatal, and never logs the blob or the plaintext. `open_all`
  (`apps/worker/src/curie_worker/sealing.py::open_all`) is all-or-nothing across
  one connector's values: a connector brought up with three of its four
  credentials is the same silently broken pod as one brought up with none.
- **Failure is fatal by design, and currently unwired.** `SealedSecretError`
  (`apps/worker/src/curie_worker/sealing.py::SealedSecretError`) is intended to
  stop the agent being reconciled, because the alternative — deploy the connector
  without the credential — produces a pod that starts, passes its health check,
  and returns 401 on every call: healthy in `kubectl get pods` and broken for
  every user. Worker production `src` never imports `sealing`; `open_all` /
  `SealedSecretError` exist only in `sealing.py` and its tests, so the opener
  does not currently stop reconcile.
- **Carrier.** `ConnectorSpec.sealed_secrets`
  (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec`), a
  mapping keyed by env var name so it reads like `env` and cannot express two
  blobs for one variable. It is excluded from `ConnectorSpec.resolved_secrets`
  (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec.resolved_secrets`)
  and included in `ConnectorSpec.secret_names`
  (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec.secret_names`):
  the blob **is** the credential, so the deploy path has no value to resolve.

Key custody is asymmetric on purpose. The CLI mints the keypair on
`curie cluster up` and preserves it across upgrades (`cli/src/ops/up.rs`); the chart
never generates one, because it has no lookup-persist and a chart-side random
would mint a new key on every `helm upgrade`
(`charts/curie/templates/secrets.yaml`, `charts/curie/values.yaml`). The previous
key is only ever preserved, never generated, since it exists solely because an
operator deliberately rotated. Only the worker receives the keys, because the
connector reconciler runs there and nothing else in the release decrypts
(`charts/curie/templates/worker.yaml`), asserted by
`charts/curie/ci/sealing-key-assertions.sh`.

## Implementations today

Two halves, one each way, plus the chart that carries the key:

- **Sealer:** `cli/src/sealing.rs` (the `crypto_box` crate, standard base64),
  wrapped by the `curie seal` verb in `cli/src/seal.rs`, which prints a pasteable
  `connectors.yaml` snippet and whose `--json` shape is pinned by
  `cli/schema/seal.schema.json` and `cli/tests/json_contract.rs`. It also holds a
  mirror opener used only by its own tests.
- **Opener:** `apps/worker/src/curie_worker/sealing.py` (PyNaCl `SealedBox`).

The one thing that pins the two together is a **single captured fixture**:
`test_a_blob_sealed_by_the_cli_opens_here`
(`apps/worker/tests/test_sealing.py::test_a_blob_sealed_by_the_cli_opens_here`)
opens a blob produced by the Rust implementation, against a private key derived
from a readable seed rather than pasted as base64 so the file carries no
high-entropy literal a scanner must be told to ignore. It is the only test that
can catch the two sides disagreeing: every other test in that file seals with
PyNaCl and would pass happily while the CLI emitted something it cannot read.

## Known leakage

This seam is a wire contract whose production path is not yet connected, and the
file would be misleading without saying so:

- **The opener has no production caller, and the carrier field is refused.**
  `validate_connectors`
  (`packages/plugin-format/src/plugin_format/connectors.py::validate_connectors`)
  emits `connectors.sealed_secrets_unsupported` and rejects the whole file when
  `sealed_secrets` is present, on the grounds that refusing beats deploying a
  connector without the credential it needs. Nothing in the reconciler imports
  the sealing module. So `curie seal` hands an author a snippet that today makes
  their bundle fail validation. Human output warns that connector validation
  rejects the snippet until #1434 lands. The refusal was briefly lifted and
  then deliberately restored, so this is a live gate rather than an oversight.
- **No version marker, so a format change is indistinguishable from
  corruption.** The envelope is bare base64 with nothing to negotiate on. Any
  future change — a different construction, a compressed payload, additional
  authenticated data binding a blob to one agent — surfaces through
  `open_sealed` as the same generic "does not decrypt with any of this cluster's
  active sealing keys" that a wrong-cluster blob produces.
- **The private key is a plain env var, not a KMS handle.** The chart mounts it
  through a `secretKeyRef` and `active_private_keys` reads the process
  environment, which forecloses envelope encryption, per-decrypt audit,
  key-usage limits and non-exportable keys; anything that can exec into the
  worker pod reads the raw key, and the CLI deliberately reads the private key
  back out of the cluster to derive the public half. ADR-0094 points at the
  chart's `existingSecret` idiom as the mitigation. The shipped values are
  `sealing.privateKeyExistingSecret` and
  `sealing.previousPrivateKeyExistingSecret` (`charts/curie/values.yaml`). A
  plain `curie cluster up` preserves the BYO reference and key without generating
  an unused chart private key (#1801).
- **The conformance pin runs one direction and cannot be regenerated by any
  command in the repo.** Rust to Python is fixed by the captured fixture above;
  Python to Rust is exercised only against blobs Rust itself sealed. The fixture's
  own docstring says regeneration means writing a temporary test in the Rust
  module, so there is no committed generator and no shared vector file under
  `tests/vectors/` the way credential forwarding has one.
- **The base64 alphabet and padding are agreed by coincidence, not assertion.**
  Both sides happen to default to the standard alphabet; nothing seals with the
  URL-safe variant and asserts rejection, and nothing round-trips a padded value.
  The captured fixture happens to be a length that needs no padding, so it does
  not cover the case either.
- **Rotation has no attribution and no reseal command.** The failure text is
  identical whether a blob was sealed to a rotated-out key or to another cluster
  entirely, and the Rust mirror deliberately does not say which key failed.
  ADR-0094 asked for `curie seal` to be able to reseal a repository in one
  command; that half did not ship, so an operator mid-rotation has no way to
  enumerate which repositories still carry stale blobs and no signal until each
  agent's reconcile fails.

## Cross-links

- **Related seam:** [connector-host](../connector-host/INTERFACE.md) — the consumer this credential is for; sealed values are the third holder shape a connector spec can declare.
- **Related seam:** [bundle-format](../bundle-format/INTERFACE.md) — the carrier. `sealed_secrets` is additive and optional, which is what let it land as a backward-compatible change.
- **Related seam:** [model-provider](../model-provider/INTERFACE.md) — the other credential axis, and a separate one: the model credential is resolved by the platform rather than carried by the bundle.
- **Epic(s):** #1240 — a bundle carries its own sealed connector keys, removing the last manual step in onboarding an agent
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — credential sealing is not one of the six swap-readiness Jobs; not separately graded
- **ADR(s):** [ADR-0094](../../adr/0094-a-bundle-carries-its-own-sealed-connector-keys.md) — a bundle carries its own sealed connector keys, with two active keys so a rotation can overlap; [ADR-0009](../../adr/0009-per-agent-connector-auth.md) — per-agent secrets and connector credentials, the mechanism sealed values join rather than replace
