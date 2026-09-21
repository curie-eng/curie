---
seam: Harness package (declared contribution)
kind: CLEAN
impls: 1 (built-in Claude) behind the entry-point registry
grade: not separately graded
epics:
  - "#844"
order: 19
---
# INTERFACE: Harness package (declared contribution)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 (built-in Claude) behind the entry-point registry &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

This is the layer **above** the in-process session port. Where
[`harness-modelsession`](../harness-modelsession/INTERFACE.md) draws the line at
one object the runner talks to during a turn, this seam draws it at the **unit
of distribution**: under ADR-0060 a harness is an installable Python package
that declares one contribution manifest, discovered through a setuptools entry
point group and selected by name at runtime. Identity, aliases and labels, the
install spec, the auth spec, the declared read-only tool set, the model-override
env keys, the per-spawn environment builder and the bundle compile hook stop
being nine files of implicit core knowledge and become fields somebody wrote
down on purpose.

The two lines are swapped independently, which is why they are two files: a
harness package supplies the manifest, and the manifest's consumers may or may
not go on to construct a `ModelSession`. What stays opinionated core is
everything the manifest is fed into — the ACI wire the runner serves, the
session runner, the side-effect classifier.

Discovery is deliberately **fail-closed**. A malformed, colliding, or
name-squatting registration raises rather than being skipped, because an
ambiguous registry is worse than a missing harness and silently shadowing the
built-in Claude harness is the single worst failure this registry could have.

## Current contract

A third party ships a package declaring an entry point in the group
`ENTRY_POINT_GROUP` (`runner/src/curie_runner/harness/registry.py::ENTRY_POINT_GROUP`),
whose value is `"curie.harness"`. The entry point resolves to a zero-argument
callable returning a `HarnessContribution`
(`runner/src/curie_runner/harness/contribution.py::HarnessContribution`), a
frozen dataclass whose eleven fields are, in declaration order: `name`, `image`,
`install`, `auth`, `readonly_tools`, `model_override_env_keys`,
`build_spawn_env`, `compile_bundle`, `supports_structured_replay`, `aliases`, `labels`.
`supports_structured_replay` is an explicit opt-in to ordered role/content replay
(ADR-0119); recovered history is refused when the selected harness leaves it false. It declares **no
methods**; the two behavioral hooks are callable fields, which is the whole
code surface a third party writes.

Three supporting types travel with it, all frozen dataclasses in the same
module: `InstallSpec`
(`runner/src/curie_runner/harness/contribution.py::InstallSpec`) names what the
image must install; `AuthSpec`
(`runner/src/curie_runner/harness/contribution.py::AuthSpec`) names the
credential env keys and OAuth token prefix the engine accepts; and
`BundleCompileResult`
(`runner/src/curie_runner/harness/contribution.py::BundleCompileResult`) is what
`compile_bundle` returns, a mounted bundle translated into that harness's native
session config.

The registry exposes exactly two functions and no `register`, because
registration is packaging metadata rather than a call:

- `discover_contributions`
  (`runner/src/curie_runner/harness/registry.py::discover_contributions`) reads
  `importlib.metadata` entry points for the group and returns every contribution
  keyed by its declared name **and** each of its aliases. Nothing is cached; the
  scan repeats per call unless the caller passes a prepared mapping.
- `resolve_harness`
  (`runner/src/curie_runner/harness/registry.py::resolve_harness`) looks a name
  up in that mapping and raises `UnknownHarnessError`
  (`runner/src/curie_runner/harness/registry.py::UnknownHarnessError`) on a miss.

Four guard rules are fail-closed, and the first two run against entry-point
**metadata before the object is loaded** while the last two run against the keys
the loaded contribution actually claims:

- a flat, top-level package path is refused — `FlatHarnessPackageError`
  (`runner/src/curie_runner/harness/registry.py::FlatHarnessPackageError`);
- a built-in name claimed from any other path is refused —
  `HarnessNameCollisionError`
  (`runner/src/curie_runner/harness/registry.py::HarnessNameCollisionError`),
  checked against `BUILTIN_HARNESS_CANONICAL_PATHS`
  (`runner/src/curie_runner/harness/registry.py::BUILTIN_HARNESS_CANONICAL_PATHS`),
  which reserves `claude`, `claude-sdk` and `claude-code` for one canonical path;
- a non-`str` key is refused — `MalformedHarnessContributionError`
  (`runner/src/curie_runner/harness/registry.py::MalformedHarnessContributionError`).
  The check is an exact `type(key) is not str` rather than `isinstance`,
  because a `str` subclass can override equality and hashing and so still steer
  the dict lookups below it;
- two contributions claiming one key collide, again `HarnessNameCollisionError`.

Selection is a runner-local env read, not a boot-env contract key:
`RunnerConfig.from_env`
(`runner/src/curie_runner/config.py::RunnerConfig.from_env`) reads
`CURIE_HARNESS`, and an empty or unset value selects `DEFAULT_HARNESS`
(`runner/src/curie_runner/harness/registry.py::DEFAULT_HARNESS`), which is
`"claude"`. The default name is declared once, in the registry rather than the
config, so the runner config and the boot path share it. The resolved name is
carried on `RunnerConfig`
(`runner/src/curie_runner/config.py::RunnerConfig`).

## Implementations today

One, plus test-only synthetics. `runner/pyproject.toml` declares the single
in-tree entry point, `claude`, pointing at `get_contribution`
(`runner/src/curie_runner/harness/claude.py::get_contribution`), which returns
`CLAUDE_CONTRIBUTION`
(`runner/src/curie_runner/harness/claude.py::CLAUDE_CONTRIBUTION`). That module
adds no behavior of its own by design: it names what already existed so a second
harness has something to register alongside. No other `pyproject.toml` in the
workspace declares the group, and there is no fake harness *package* — the
`ModelSession`-level fake belongs to the sibling seam, and synthetic
contributions exist only inside `runner/tests/test_harness_registry.py`.

The manifest is consumed in exactly one module, the runner's boot path
(`runner/src/curie_runner/__main__.py`). `_resolve_harness`
(`runner/src/curie_runner/__main__.py::_resolve_harness`) turns a name into a
contribution, `main` (`runner/src/curie_runner/__main__.py::main`) calls
`build_spawn_env`, and `build_runner`
(`runner/src/curie_runner/__main__.py::build_runner`) calls `compile_bundle` and
feeds `readonly_tools` to `SideEffectClassifier`
(`runner/src/curie_runner/side_effects.py::SideEffectClassifier`).

The gate a registration must survive today is the import-linter contract set in
the root `pyproject.toml`, run as `uv run lint-imports` in
`.github/workflows/ci.yaml`. One of its four contracts guards this seam
specifically, forbidding `claude_agent_sdk` inside
`runner/src/curie_runner/harness/contribution.py` and
`runner/src/curie_runner/harness/registry.py` while deliberately exempting
`runner/src/curie_runner/harness/claude.py`, which is the Claude harness itself.
The registry's own behavior is pinned by `runner/tests/test_harness_registry.py`,
`runner/tests/test_harness_contribution.py` and
`runner/tests/test_harness_boot_wiring.py`.

## Known leakage

The registry is CLEAN as a discovery mechanism, and it is a guarded indirection
around one contribution rather than a working plugin distribution channel. Four
concrete gaps:

- **Most of the manifest has no reader.** `image`, `install`, `auth` and
  `model_override_env_keys` are declared and consumed by nothing in production;
  `labels` has no reader anywhere, tests included. Only `build_spawn_env`,
  `compile_bundle`, `supports_structured_replay` and `readonly_tools` are load-bearing, plus `name`/`aliases`
  inside the registry. A second harness that fills the other fields correctly
  changes no behavior.
- **The facts the manifest declares are still hardcoded elsewhere, in two
  languages.** The runner image name is a Rust constant (`cli/src/docker.rs`) and
  a Python default inside `_sandbox_client`
  (`apps/worker/src/curie_worker/run.py::_sandbox_client`), neither of which
  reads `HarnessContribution.image`. The credential mirror that ADR-0060 cited as
  its motivating duplication is still hand-copied in the Docker substrate
  (`apps/worker/src/curie_worker/sandbox/docker.py`) rather than read off
  `AuthSpec`.
- **Nothing can set `CURIE_HARNESS`.** It is explicitly a runner-local knob and
  not a boot-env key, and the boot-env gate's own allowlist records that
  (`apps/worker/tests/binding/test_boot_env_single_declaration.py`). No chart
  template, compose file, worker binding or CLI flag writes it, so selecting a
  non-default harness today means hand-setting container env. ADR-0060 decision 3
  asked for a declarative `harness:` field plus a CLI flag; neither exists.
- **A built-in name never goes through discovery at all, and a third party
  cannot install without rebuilding the image.** `_resolve_harness` short-circuits
  any name in `BUILTIN_HARNESS_CANONICAL_PATHS` to a direct import so a broken
  sibling entry point cannot take the Claude harness down (#865) — which also
  means packaging metadata is never consulted on the default path. Meanwhile
  `importlib.metadata` only sees distributions installed into the runner image's
  sealed virtualenv (`runner/Dockerfile`), and that rootfs is read-only at
  runtime, so a runtime install is not merely unwired but impossible. The
  mechanism buys declaration, not deployment: the distribution story is a derived
  image, and `InstallSpec.packages`, which exists to describe exactly that, is
  read by nobody.

## Cross-links

- **Sibling seam:** [harness-modelsession](../harness-modelsession/INTERFACE.md) — the in-process `ModelSession` port this layer sits above; it covers the turn-time object, this file covers the package and its discovery.
- **Sibling seam:** [aci-producer](../aci-producer/INTERFACE.md) — the frozen cross-process wire. The contribution manifest is deliberately **not** an `aci-protocol` frozen contract type; freezing it is a later choice once a second harness has exercised it.
- **Related seam:** [port-adapter-service](../port-adapter-service/INTERFACE.md) — ADR-0096 generalizes this mechanism, and reserves in-process entry-point groups for the narrow ports that cannot cross a process boundary.
- **Epic(s):** #844 — implement the package-shaped harness redesign (ADR-0060/0061/0062)
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — the declared-package layer sits above the session port; not one of the six swap-readiness Jobs, so not separately graded
- **ADR(s):** [ADR-0060](../../adr/0060-the-harness-is-a-declared-package.md) — the unit of a harness is an installable package declaring a contribution manifest, registered by entry point with fail-closed guard rules; [ADR-0062](../../adr/0062-harness-conformance-has-teeth.md) — a registry proves an object loaded, a conformance kit proves it behaves; the import-linter contracts are the part realized so far
