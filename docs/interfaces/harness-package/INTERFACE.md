---
seam: Harness package (declared contribution)
kind: CLEAN
impls: 1 supported built in Claude contribution
grade: not separately graded
epics: ["#3828"]
order: 19
---
# INTERFACE: Harness package (declared contribution)

> Part of the Curie swappable seam catalog. See the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 supported built in Claude contribution &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

This layer declares the contribution that supplies runner boot behavior.
[`harness-modelsession`](../harness-modelsession/INTERFACE.md) describes the
session object used during a turn. The package seam describes identity,
aliases, spawn environment, bundle compilation, tools that are safe to read,
and structured replay support. The frozen ACI wire, session loop, and side
effect classifier remain runner concerns.

[ADR 0140](../../adr/0140-curie-supports-one-model-harness-until-a-second-one-exists.md)
limits supported runner boot to Claude until a real second engine exists.
The retained registry is a guarded declaration mechanism; registering another
contribution does not make it a supported runner engine.

## Current contract

`HarnessContribution`
(`runner/src/curie_runner/harness/contribution.py::HarnessContribution`) is a
frozen dataclass with six fields: `name`, `readonly_tools`, `build_spawn_env`,
`compile_bundle`, `supports_structured_replay`, and `aliases`. Its behavior is
carried by the two callable fields. `supports_structured_replay` explicitly
opts into ordered role/content replay under ADR 0119; recovered history is
refused when that capability is false.

`BundleCompileResult`
(`runner/src/curie_runner/harness/contribution.py::BundleCompileResult`) is the
remaining supporting type. It describes a mounted bundle translated into the
harness session configuration. The manifest has no image, installation, auth,
model override, or label fields. Claude credential resolution and redaction
use `runner/src/curie_runner/sdk_auth.py`, including
`DEFAULT_CREDENTIAL_ENV_KEYS`
(`runner/src/curie_runner/sdk_auth.py::DEFAULT_CREDENTIAL_ENV_KEYS`). Deployment
configuration selects the runner image.

The registry reads entry points in `ENTRY_POINT_GROUP`
(`runner/src/curie_runner/harness/registry.py::ENTRY_POINT_GROUP`), whose value
is `"curie.harness"`. Each entry point resolves to a callable taking no
arguments and returning a contribution. `discover_contributions`
(`runner/src/curie_runner/harness/registry.py::discover_contributions`) returns
contributions keyed by declared names and aliases. `resolve_harness`
(`runner/src/curie_runner/harness/registry.py::resolve_harness`) resolves that
mapping or raises `UnknownHarnessError`
(`runner/src/curie_runner/harness/registry.py::UnknownHarnessError`). The
registry has no mutation API and discovery is not cached.

The registry guards remain independent of supported boot:

1. A flat module path raises `FlatHarnessPackageError`
   (`runner/src/curie_runner/harness/registry.py::FlatHarnessPackageError`).
2. A reserved built in name claimed from another path raises
   `HarnessNameCollisionError`
   (`runner/src/curie_runner/harness/registry.py::HarnessNameCollisionError`).
   `BUILTIN_HARNESS_CANONICAL_PATHS`
   (`runner/src/curie_runner/harness/registry.py::BUILTIN_HARNESS_CANONICAL_PATHS`)
   reserves `claude`, `claude-sdk`, and `claude-code` for the Claude contribution.
3. A key whose exact type is not `str` raises
   `MalformedHarnessContributionError`
   (`runner/src/curie_runner/harness/registry.py::MalformedHarnessContributionError`).
4. Contributions claiming the same key raise `HarnessNameCollisionError`.

`RunnerConfig.from_env`
(`runner/src/curie_runner/config.py::RunnerConfig.from_env`) reads the internal
`CURIE_HARNESS` setting. Unset or empty selects `DEFAULT_HARNESS`
(`runner/src/curie_runner/harness/registry.py::DEFAULT_HARNESS`), `"claude"`.
`_resolve_harness`
(`runner/src/curie_runner/__main__.py::_resolve_harness`) imports the built in
contribution directly for its name and aliases. Every other name raises
`UnsupportedHarnessError`
(`runner/src/curie_runner/harness/registry.py::UnsupportedHarnessError`) before
entry point discovery, including an installed and registered nonClaude name.
No CLI, worker, Compose, or chart selector exposes this internal setting.

## Implementations today

One supported contribution, plus registry test synthetics. The entry point in
`runner/pyproject.toml` points at `get_contribution`
(`runner/src/curie_runner/harness/claude/__init__.py::get_contribution`), which
returns `CLAUDE_CONTRIBUTION`
(`runner/src/curie_runner/harness/claude/__init__.py::CLAUDE_CONTRIBUTION`).
Runner boot calls its environment builder and bundle compiler and supplies its
tool set to `SideEffectClassifier`
(`runner/src/curie_runner/side_effects.py::SideEffectClassifier`).
Runner boot also supplies the tool set to per-turn tool access
(`runner/src/curie_runner/__main__.py::_readonly_tools`) and reads
`harness.auth.credential_env_keys` for held-secret redaction.

The Claude package also owns the SDK approval adapter at
`runner/src/curie_runner/harness/claude/approval.py`. Approval policy stays in
`runner/src/curie_runner/approval.py`. The import rules in `pyproject.toml`
keep the contribution, registry, and policy gate free of SDK imports and
ratchet legacy SDK imports elsewhere in the runner. Only the Claude harness
package is exempt from that runner ratchet.

## Known leakage

The session port still receives SDK message objects, and other runner modules
retain explicit legacy SDK imports. The approval split removes one such edge;
it does not complete a neutral session payload contract. The import ratchet
prevents new exceptions outside the Claude package.

The registry discovers only distributions installed in the runner image.
The sealed virtual environment and read only root filesystem provide no
runtime installation path. A contribution declaration therefore does not
supply image deployment or supported boot for another engine. A working
second engine requires a new ADR under ADR 0140.

## Cross links

1. [Harness ModelSession](../harness-modelsession/INTERFACE.md) describes the
   session port below this declaration.
2. [ACI producer](../aci-producer/INTERFACE.md) describes the frozen process
   wire. `HarnessContribution` is not part of that frozen protocol.
3. [Port adapter service](../port-adapter-service/INTERFACE.md) describes
   deployed adapters for other seams.
4. [Architecture vision](../../architecture-vision.md) places the declaration
   above the session port; it is not a separately graded job.
5. [ADR 0140](../../adr/0140-curie-supports-one-model-harness-until-a-second-one-exists.md)
   replaces the broader programs in ADRs 0060, 0061, and 0062. [#3828](https://github.com/curie-eng/curie/issues/3828)
   tracks its approval and implementation.
