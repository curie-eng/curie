"""AC2: no boot-env var name is declared twice across the worker and runner lanes.

This is the check that actually enforces #488's thesis. The boot env is a
cross-lane contract: every name the worker writes, the runner reads. Today each
name is typed TWICE, once as a ``*_ENV`` constant in
``apps/worker/src/curie_worker/binding.py`` and once as a bare
``env.get("CURIE_...")`` in ``runner/src/curie_runner/config.py``. Rename
either side and the sandbox boots, runs, and silently drops the feature -- no
import error, no test failure, no log line. After #488 the ONE declaration is
``aci_protocol.BootEnv``, and a string literal of a declared boot key in either
lane's ``src`` is a reintroduction of the drift.

The corpus is every env name in the boot contract's namespace, which is the three
prefixes ``CURIE_``, ``OTEL_EXPORTER_OTLP_`` and ``ANTHROPIC_`` -- the same
three the sibling gates in the other two lanes scan (``cli/tests/
boot_env_contract.rs``, ``charts/curie/ci/render-assertions.sh``). A narrower
``CURIE_``-only corpus would leave ``ANTHROPIC_BASE_URL`` and the OTel trio,
a sixth of the declared keys, invisible to the one gate that claims to enforce
AC2.

The scan is AST-based, not a raw grep, and looks only for DECLARATIONS: a string
literal whose whole value is an env name (``"CURIE_MODEL"``) or the ``NAME=``
head of an assignment, whether the value is interpolated
(``f"CURIE_SANDBOX_ID={name}"``) or static
(``"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf"``) -- both are the docker
substrate's ``-e`` argv form. A name mentioned inside a sentence -- a docstring, a
comment, an operator-facing error message -- is not a declaration: rename the key
and that text goes stale but nothing breaks, so flagging it would be noise that
trains people to pad the allowlist.

Every literal in that corpus is classified into exactly one of three buckets:

* a **declared boot key** (``BootEnv.env_keys()``) -- a violation, unless the
  file carries an explicit exemption below with a stated reason;
* an explicitly allowlisted **non-boot** name -- the worker service's own
  config, the substrate wiring knobs, the SDK's own credential vars, the
  CLI-owned check knob. These are read by a different consumer and are not part
  of the sandbox boot contract;
* anything else -- a violation, because an env in the contract's namespace that
  nobody has classified is exactly how the next straggler is born.

This test lives under ``apps/worker/tests/binding`` rather than a top-level
``tests/`` package because that is the closest lane already collected by both
``pyproject.toml``'s ``testpaths`` and the branch's
``pytest apps/worker/tests/binding runner/tests`` command; it needs no config
change to run in CI. It is deliberately cross-lane despite the location, and it
touches nothing but the filesystem, so it runs in every lane with no fixtures.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from aci_protocol import BootEnv

# The boot contract's whole env namespace, matching the CLI and chart gates.
_PREFIXES = ("CURIE_", "OTEL_EXPORTER_OTLP_", "ANTHROPIC_")

# A declaration, not a mention: the literal IS the name, or is the `NAME=` head
# of an assignment (the docker substrate's `-e` argv form), with the value either
# interpolated away by an f-string or written inline. A name followed by anything
# other than `=` is prose, so an error message naming a var does not match.
_DECLARATION = re.compile(rf"^((?:{'|'.join(_PREFIXES)})[A-Z0-9_]+)(?:=.*)?$", re.DOTALL)


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists() and (parent / "runner").is_dir():
            return parent
    raise AssertionError("could not locate the repo root from the test file path")


_ROOT = _repo_root()

# The two lanes the boot env crosses. Both are scanned as one corpus: a name
# declared in either is a declaration site competing with BootEnv.
_LANES = (
    Path("apps/worker/src"),
    Path("runner/src"),
)

# Names that are NOT part of the sandbox boot env, so a literal is legitimate.
# Each is read by a different consumer than the runner-in-a-sandbox: the worker
# service reading its own process env, the substrate wiring, the CLI, or the SDK
# itself under a name the SDK owns.
_NON_BOOT_ALLOWLIST: frozenset[str] = frozenset(
    {
        # The worker service's own settings (WorkerConfig validation_alias), read
        # from the WORKER's env at worker startup. Never a sandbox boot key.
        "CURIE_BOOTING_TEXT",
        "CURIE_CONSUMER_GROUP",
        "CURIE_CONSUMER_NAME",
        "CURIE_DEAD_LETTER_MAXLEN",
        "CURIE_MAX_ATTEMPTS",
        "CURIE_MAX_DELIVERY",
        "CURIE_SLACK_NO_EDIT_STREAMING",
        # The per-adapter EGRESS credentials (ADR-0096 D4.2), read from the
        # WORKER's env by ``build_reply_sink`` and presented to a channel
        # adapter as ``X-Curie-Adapter-Secret``. Never a sandbox boot key, and
        # emphatically so: these authenticate the PLATFORM to an adapter, so a
        # sandbox running agent-authored code holding them could impersonate
        # the platform to every bound channel.
        "CURIE_ADAPTER_CREDENTIALS",
        # The operator-configured extra Slack origins a per-turn reply endpoint
        # may name (ADR-0096 D4.4), read from the WORKER's env by WorkerConfig
        # and consumed by ``build_reply_sink``. It is an egress-trust decision
        # made on the worker; nothing about it reaches a sandbox.
        "CURIE_SLACK_TRUSTED_ORIGINS",
        # Managed-workspace operator settings, read from the WORKER's env by
        # WorkerConfig and consumed by WorkspacePreparer, WorkspaceObjectStore,
        # and the internal credential client. They govern worker-side clone,
        # archive, upload, and capability-minting policy; none is runner boot
        # configuration.
        "CURIE_INTERNAL_WORKER_TOKEN",
        "CURIE_WORKSPACE_ENABLED",
        "CURIE_WORKSPACE_BUCKET",
        "CURIE_WORKSPACE_OBJECT_PREFIX",
        "CURIE_WORKSPACE_SCRATCH_ROOT",
        "CURIE_WORKSPACE_CLONE_TIMEOUT_SECONDS",
        "CURIE_WORKSPACE_ARCHIVE_TIMEOUT_SECONDS",
        "CURIE_WORKSPACE_UPLOAD_TIMEOUT_SECONDS",
        "CURIE_WORKSPACE_TOTAL_TIMEOUT_SECONDS",
        "CURIE_WORKSPACE_MAX_CHECKOUT_BYTES",
        "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES",
        "CURIE_WORKSPACE_MAX_MEMBERS",
        "CURIE_WORKSPACE_MAX_COMPRESSION_RATIO",
        "CURIE_WORKSPACE_REFERENCE_TTL_SECONDS",
        "CURIE_WORKSPACE_MAX_CONCURRENT_CLONES",
        # WorkspaceClaimCoordinator injects these exact-object handoff values
        # into a claim for the workspace-init container
        # to consume. They are substrate-local delivery inputs, not fields read
        # by the sandbox runner and therefore not part of frozen BootEnv.
        "CURIE_WORKSPACE_REF",
        "CURIE_WORKSPACE_SHA256",
        # Publication-Job operator settings, read from the WORKER's env by
        # WorkerConfig and consumed by PublicationReconcileLoop and its
        # Kubernetes Job builder. They configure the platform-side publisher,
        # never a runner sandbox boot.
        "CURIE_PUBLICATION_ENABLED",
        "CURIE_PUBLICATION_NAMESPACE",
        "CURIE_PUBLICATION_PATCH_MAX_BYTES",
        "CURIE_PUBLICATION_RESULT_MAX_ATTEMPTS",
        "CURIE_PUBLICATION_RECONCILE_MAX_ATTEMPTS",
        "CURIE_PUBLICATION_JOB_ACTIVE_DEADLINE_SECONDS",
        "CURIE_PUBLICATION_GIT_COMMAND_TIMEOUT_SECONDS",
        "CURIE_PUBLICATION_GITHUB_API_URL",
        "CURIE_PUBLICATION_RECONCILE_INTERVAL_SECONDS",
        "CURIE_PUBLICATION_LEASE_SECONDS",
        "CURIE_PUBLICATION_IMAGE_PULL_POLICY",
        "CURIE_PUBLICATION_IMAGE_PULL_SECRETS",
        "CURIE_PUBLICATION_PRIORITY_CLASS_NAME",
        "CURIE_PUBLICATION_SERVICE_ACCOUNT_NAME",
        "CURIE_PUBLICATION_OWNER_NAME",
        "CURIE_PUBLICATION_GIT_USER_NAME",
        "CURIE_PUBLICATION_GIT_USER_EMAIL",
        "CURIE_PUBLICATION_CPU_REQUEST",
        "CURIE_PUBLICATION_CPU_LIMIT",
        "CURIE_PUBLICATION_MEMORY_REQUEST",
        "CURIE_PUBLICATION_MEMORY_LIMIT",
        "CURIE_PUBLICATION_EPHEMERAL_REQUEST",
        "CURIE_PUBLICATION_EPHEMERAL_LIMIT",
        # The Slack shimmer caption. Read from the WORKER's env since #1312 moved
        # the whole shimmer to this side; it reaches Slack, never a sandbox.
        "CURIE_STATUS_TEXT",
        # Which agent this worker mints a fleet-control token FOR (ADR-0125),
        # read from the WORKER's env by WorkerConfig. Emphatically not a boot
        # key: a sandbox that could read the name could learn whether it is
        # privileged, and a sandbox that could SET it would name itself. The
        # boot pair this setting gates (CURIE_CONTROL_URL / CURIE_CONTROL_TOKEN)
        # is declared on BootEnv like every other injected key.
        "CURIE_CONTROL_AGENT",
        # The cluster sealing keys (ADR-0094), read from the WORKER's env at
        # reconcile time. Never a sandbox boot key -- quite the opposite: the
        # decrypted VALUE reaches a connector's Secret, and the private key
        # itself must never leave the worker, least of all into a sandbox that
        # runs agent-authored code.
        "CURIE_SEALING_PRIVATE_KEY",
        "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
        "CURIE_EVAL_CONSUMER_GROUP",
        "CURIE_EVAL_MAX_CONCURRENT_CLAIMS",
        "CURIE_EVAL_STREAM",
        "CURIE_EVAL_STREAM_MAX_AGE_HOURS",
        # The eval harness's own knobs, read by the eval entrypoint, not injected.
        "CURIE_EVAL_SUITE",
        "CURIE_EVAL_TARGET_URL",
        "CURIE_EVAL_VERSION",
        # Multi-sample / variance-aware grading policy (#332), read by the eval
        # entrypoint (run.py::sample_config_from_env), not a sandbox boot key.
        "CURIE_EVAL_SAMPLES",
        "CURIE_EVAL_AGGREGATION",
        "CURIE_EVAL_PASS_AT_K",
        # Substrate wiring: how the worker provisions sandboxes, not what it puts
        # inside one. SubstrateConfig reads these from the worker's env.
        "CURIE_CLAIM_TIMEOUT_SECONDS",
        # How long a thread pins its sandbox, and how long a suspended one waits
        # (#1380). Same family as the claim timeout above: the worker's own
        # provisioning policy, decided before any sandbox exists, so never a
        # boot key.
        "CURIE_ROUTE_TTL_SECONDS",
        "CURIE_SUSPENDED_ROUTE_TTL_SECONDS",
        "CURIE_DOCKER_NETWORK",
        "CURIE_NAMESPACE",
        # The Helm release name (ADR-0086, #1118). Read from the WORKER's env by
        # WorkerConfig, exactly like CURIE_NAMESPACE above, and never injected
        # into a sandbox: what the runner receives is the derived connector
        # scope (CURIE_CONNECTOR_RELEASE), which IS a declared BootEnv key.
        "CURIE_RELEASE",
        # The connector reconciler's own switches (ADR-0090, #1184), read from
        # the WORKER's env by WorkerConfig and consumed by the reconcile loop in
        # the worker process. Nothing here reaches a sandbox: the reconciler
        # writes Kubernetes objects, and what the runner learns about connectors
        # is the derived scope built from CURIE_RELEASE/CURIE_NAMESPACE above.
        # CURIE_CONNECTOR_APP_NAME is the chart's nameOverride, needed only to
        # match the pod selector the connector NetworkPolicy uses.
        "CURIE_CONNECTOR_RECONCILE",
        "CURIE_CONNECTOR_RECONCILE_INTERVAL_S",
        "CURIE_CONNECTOR_APP_NAME",
        "CURIE_RUNNER_IMAGE",
        "CURIE_SANDBOX_SUBSTRATE",
        "CURIE_WARM_POOL",
        # The runner-facing API base (#678): WorkerConfig reads it from the
        # WORKER's env to MINT CURIE_MEMORY_REF/CURIE_HISTORY_REF (which ARE
        # declared boot keys, rendered from the declaration). It is a worker-side
        # knob for what URL those refs carry, never itself a sandbox boot key.
        "CURIE_RUNNER_API_URL",
        # The local-model demo base URL: an operator knob on the WORKER and on the
        # runner's sdk_auth mapping. It is not a BootEnv field; the boot key the
        # worker actually emits from it is ANTHROPIC_BASE_URL.
        "CURIE_MODEL_BASE_URL",
        # CLI-owned: `curie skill check` sets it on its own offline subprocess.
        # The worker never injects it (see the plan's prior-intent note on 20cb18c).
        "CURIE_CHECK_TIMEOUT_S",
        # The claude-agent-sdk's OWN credential vars, whose names the SDK owns, not
        # this contract. sdk_auth resolves CURIE_CREDENTIALS (which IS a declared
        # boot key, and is read from the declaration) onto them; the worker forwards
        # the ambient ones by name. Renaming a BootEnv key cannot move these.
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        # PR #663 operator-tunable Docker runner hardening knobs; the docker
        # substrate reads these from its OWN env, never injected into the runner
        # boot contract.
        "CURIE_RUNNER_HARDENING",
        "CURIE_RUNNER_WRITABLE_PATHS",
        "CURIE_RUNNER_PIDS_LIMIT",
        "CURIE_RUNNER_READ_ONLY",
        "CURIE_RUNNER_CAP_DROP_ALL",
        "CURIE_RUNNER_NO_NEW_PRIVILEGES",
        "CURIE_RUNNER_MEMORY_LIMIT",
        "CURIE_RUNNER_CPU_LIMIT",
        # Standard signal-specific OTel variables are operator-owned process
        # configuration read directly by the SDK exporter. They are not boot
        # keys injected by the worker; the three general fallback keys above
        # remain derived from BootEnv.
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        # runner-local false-completion knob; read by the runner from its own env,
        # not a boot contract key.
        "CURIE_FALSE_COMPLETION_CHECK",
        # runner-local harness selection (ADR-0060, #844); read by the runner from
        # its own env to pick the active harness, unset selects the built-in
        # Claude. Not a boot contract key.
        "CURIE_HARNESS",
        # Delivery budget and ownership lease (ADR-0131, #1971), read from the
        # WORKER's env by WorkerConfig. Never a sandbox boot key: they govern
        # how the worker paces and reclaims its own delivery loop, not
        # anything injected into a runner claim.
        # - CURIE_DELIVERY_BUDGET_S: the overall wall-clock deadline for one
        #   delivery (claim, every runner request, retries, reclaim, cleanup).
        # - CURIE_DELIVERY_LEASE_TTL_S: how long the fenced ownership lease on
        #   an in-flight delivery is valid before it is reclaimable.
        # - CURIE_DELIVERY_LEASE_HEARTBEAT_S: how often the owner renews that
        #   lease while the delivery is still healthy.
        # - CURIE_DELIVERY_SHUTDOWN_RESERVE_S: time reserved near the budget's
        #   end for terminal cleanup instead of another runner attempt.
        # - CURIE_RECLAIM_INTERVAL_S: the maintenance-tick cadence that scans
        #   for expired leases to reclaim; same reclaim family as
        #   CURIE_RECLAIM_MIN_IDLE_MS in the code above, worker-side policy
        #   decided before any sandbox exists.
        "CURIE_DELIVERY_BUDGET_S",
        "CURIE_DELIVERY_LEASE_TTL_S",
        "CURIE_DELIVERY_LEASE_HEARTBEAT_S",
        "CURIE_DELIVERY_SHUTDOWN_RESERVE_S",
        "CURIE_RECLAIM_INTERVAL_S",
        # The platform's voluntary termination grace (ADR-0131, #1971),
        # injected by the chart from the SAME value it renders onto the Pod's
        # ``terminationGracePeriodSeconds`` so the worker's own shutdown
        # validator and the platform can never drift apart. Read from the
        # WORKER's env by WorkerConfig; the sandbox never sees it.
        "CURIE_TERMINATION_GRACE_PERIOD_S",
    }
)

# Declared boot keys that legitimately remain typed in a lane file, each with the
# reason it is not the binding/config drift #488 closes. Every entry is a site
# that happens to SHARE A NAME with a boot key while serving a different producer
# or a different consumer: the worker process reading its own env to decide what
# to inject, or the substrate authoring an identity the worker must never render.
#
# NO ENTRY HERE IS A SAME-CONSUMER SITE, and none may be. If a file reads or
# writes a key for the SAME consumer as the boot contract -- the runner in a
# sandbox -- it must name it from BootEnv.env_key, which fails at import
# everywhere and cannot rot. A pin test is not an acceptable substitute: it is a
# strictly weaker form of derivation, catching a rename only in the lanes where
# it runs and only for as long as someone keeps it alive. Neither is LOUDNESS:
# "a rename would surface as an auth failure" is an argument that the drift gets
# caught downstream by someone else, in an environment, at runtime -- which is
# the thing this gate exists to make unnecessary. Both runner-side literals once
# excused that way (sdk_auth.py's CREDENTIALS, check.py's PLUGIN_DIR) now read
# from BootEnv.env_key instead, at no seam cost: both files already import
# aci_protocol. So did the four exemptions deleted in #488's review (k8s.py's
# BUNDLE_REF / CREDENTIALS / CONNECTOR_SECRET_KEYS, substrate.py's HISTORY_REF /
# SESSION_ID), each justified by a seam cost that importing aci_protocol does not
# actually incur.
#
# This map is the honest floor, not an escape hatch: adding an entry means
# arguing on the record that the site's producer or consumer genuinely differs
# from the boot contract's. If the honest answer is "same consumer, but a rename
# would be noticed", that is not an exemption -- derive the name.
_EXEMPT: dict[tuple[str, str], str] = {
    # WorkerConfig reads the WORKER's env under these names to decide what to
    # inject; the name collision with the boot key it later emits is real but
    # the consumer is the worker process, not the sandbox.
    ("apps/worker/src/curie_worker/config.py", "CURIE_PLUGIN_DIR"): "worker service config",
    ("apps/worker/src/curie_worker/config.py", "CURIE_MODEL"): "worker service config",
    # Same shape as CURIE_MODEL directly above, and the same honest test: the
    # sandbox-side name is derived from the declaration on BOTH real boot sites
    # (binding.THINKING_ENV renders it, the runner reads boot.thinking from
    # BootEnv.from_env), so a rename still moves them. This site is the worker
    # process reading its own env to decide what to inject (#1182, ADR-0098).
    ("apps/worker/src/curie_worker/config.py", "CURIE_THINKING"): "worker service config",
    ("apps/worker/src/curie_worker/config.py", "CURIE_FAKE_MODEL"): "worker service config",
    ("apps/worker/src/curie_worker/config.py", "CURIE_CREDENTIALS"): "worker service config",
    # Same shape as the four above: the operator declares the endpoint's wire
    # protocol and credential key(s) (#514) on the WORKER's env, and WorkerConfig
    # reads them there to decide what to inject. The sandbox-side names are
    # rendered from the declaration (BootEnv.render_worker) and read from it in
    # the runner (sdk_auth), so a rename still moves both real boot sites.
    (
        "apps/worker/src/curie_worker/config.py",
        "CURIE_MODEL_API_BACKEND",
    ): "worker service config",
    ("apps/worker/src/curie_worker/config.py", "CURIE_MODEL_ENV_KEY"): "worker service config",
    ("apps/worker/src/curie_worker/eval/run.py", "CURIE_MODEL"): "eval entrypoint env read",
    # run.py reads the WORKER's own OTel endpoint -- the standard var its own
    # deployment sets to point the worker process at the collector -- to warn when
    # middle mode has none and to hand the docker client a target. The consumer is
    # the worker process; the name it later writes INTO a container is read from
    # the declaration (sandbox/docker.py). Renaming the boot key must not rename
    # the worker's own OTel var, so this site is not the drift.
    (
        "apps/worker/src/curie_worker/run.py",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ): "worker service config",
    # Substrate-authoritative producers. The chart/docker own pod identity and
    # the runner port; the worker must never render them (see BootEnv).
    ("apps/worker/src/curie_worker/run.py", "CURIE_RUNNER_PORT"): "substrate producer",
    (
        "apps/worker/src/curie_worker/sandbox/docker.py",
        "CURIE_SANDBOX_ID",
    ): "substrate producer",
    (
        "apps/worker/src/curie_worker/sandbox/docker.py",
        "CURIE_RUNNER_PORT",
    ): "substrate producer",
}


def _literals(path: Path) -> list[tuple[int, str]]:
    """Every CURIE_ name DECLARED by a string literal, with its line number.

    Docstrings are excluded (they discuss these names by design) and comments
    never reach the AST at all.
    """

    tree = ast.parse(path.read_text(), filename=str(path))
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ) and (doc := ast.get_docstring(node, clean=False)):
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                assert first.value.value == doc
                docstrings.add(id(first.value))

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        if id(node) in docstrings:
            continue
        if match := _DECLARATION.match(node.value):
            found.append((node.lineno, match.group(1)))
    return found


def _scan() -> list[tuple[str, int, str]]:
    hits: list[tuple[str, int, str]] = []
    for lane in _LANES:
        lane_dir = _ROOT / lane
        assert lane_dir.is_dir(), f"lane {lane} not found under {_ROOT}"
        for path in sorted(lane_dir.rglob("*.py")):
            rel = path.relative_to(_ROOT).as_posix()
            for lineno, name in _literals(path):
                hits.append((rel, lineno, name))
    return hits


def test_boot_env_keys_are_declared_once_in_aci_protocol() -> None:
    # The WHOLE declared surface, not the CURIE_ slice of it: a scan that could
    # not see ANTHROPIC_BASE_URL or the OTel trio would call a redeclaration of
    # one of them "unclassified" instead of naming it the drift it is.
    boot_keys = set(BootEnv.env_keys())
    assert boot_keys, "BootEnv declares no keys; the scan would be vacuous"
    assert not boot_keys - {
        k for k in boot_keys if k.startswith(_PREFIXES)
    }, "a declared boot key sits outside the scanned prefixes; widen _PREFIXES"

    redeclared: list[str] = []
    unclassified: list[str] = []
    for rel, lineno, name in _scan():
        if name in boot_keys:
            if (rel, name) not in _EXEMPT:
                redeclared.append(f"  {rel}:{lineno}  {name}")
            continue
        if name not in _NON_BOOT_ALLOWLIST:
            unclassified.append(f"  {rel}:{lineno}  {name}")

    problems: list[str] = []
    if redeclared:
        problems.append(
            "These lines retype a boot-env key that aci_protocol.BootEnv already\n"
            "declares. That is the exact drift #488 closes: rename the key on one\n"
            "side and the sandbox still boots, still runs, and silently drops the\n"
            "feature. Read the key from the BootEnv declaration (render it via\n"
            "BootEnv.render_worker on the worker side, parse it via BootEnv.from_env\n"
            "on the runner side) instead of retyping the literal:\n"
            + "\n".join(sorted(redeclared))
        )
    if unclassified:
        problems.append(
            "These lines name an env in the boot contract's namespace that is\n"
            "neither a declared BootEnv key nor an allowlisted non-boot name. Every\n"
            "one of them is one or the other: if the sandbox reads it, declare it on\n"
            "BootEnv; if some other process reads it, add it to _NON_BOOT_ALLOWLIST\n"
            "in this file with the consumer named. An unclassified one is how the\n"
            "next straggler is born:\n"
            + "\n".join(sorted(unclassified))
        )

    assert not problems, "\n\n".join(problems)


def test_exemptions_and_allowlist_are_live() -> None:
    """A stale exemption is a hole nobody is watching. Fail when one goes unused.

    Without this, deleting a write site leaves its exemption behind, silently
    re-permitting the literal when someone reintroduces it later.
    """

    hits = _scan()
    seen_pairs = {(rel, name) for rel, _, name in hits}
    seen_names = {name for _, _, name in hits}

    stale_exempt = sorted(f"{rel} {name}" for rel, name in _EXEMPT if (rel, name) not in seen_pairs)
    stale_allow = sorted(name for name in _NON_BOOT_ALLOWLIST if name not in seen_names)

    assert not stale_exempt, (
        "These _EXEMPT entries match nothing any more; the literal is gone, so "
        "drop the exemption rather than leaving a hole open:\n  " + "\n  ".join(stale_exempt)
    )
    assert not stale_allow, (
        "These _NON_BOOT_ALLOWLIST entries match nothing any more; drop them:\n  "
        + "\n  ".join(stale_allow)
    )


def test_agent_id_is_not_declared_anywhere_in_the_lanes() -> None:
    """CURIE_AGENT_ID is written by the worker and read by nobody (#488, AC4)."""

    hits = [f"{rel}:{lineno}" for rel, lineno, name in _scan() if name == "CURIE_AGENT_ID"]
    assert not hits, (
        "CURIE_AGENT_ID is injected into every sandbox boot env and no consumer "
        "ever reads it. Delete the write site rather than declaring it:\n  "
        + "\n  ".join(hits)
    )
