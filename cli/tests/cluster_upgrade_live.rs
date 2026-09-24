//! `curie cluster upgrade` against the recording process boundary (#2299 / #2301).
//!
//! `cluster_upgrade.rs` drives `FakeUpgradeHost`, which satisfies the lifecycle
//! contract by construction. These tests instead drive the real binary against
//! recording `helm`/`kubectl` processes, so they pin what `LiveHost` actually
//! issues and observes. They do not prove real Kubernetes concurrency. The
//! separate cluster acceptance run owns that evidence.
//!
//! Fixture shapes follow the Kubernetes workload/pod references used by
//! `cluster_convergence.rs`. `PATH` is process-global, so every child gets its
//! own `PATH` via `Command::env` and every test its own TempDir root.
//!
//! Non-inertness is the design constraint. `convergence::observe` compares live
//! containers against Helm's INSTALLED manifest, not against `--to`, so a
//! fixture that moves the manifest and the live pods together can never make
//! `images` false however the flag is wired. Every convergence fixture here
//! therefore diverges exactly one facet and asserts that the OTHER named
//! sub-flags stay true: a binding that collapses every flag onto
//! `issues.is_empty()` fails these tests just as loudly as a hardcoded `true`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const HOLDER_ANNOTATION: &str = "curietech.ai/upgrade-holder";
const ACTION_ANNOTATION: &str = "curietech.ai/upgrade-action";
const CREATE_WINNER: &str = "00000000-0000-4000-8000-000000000701";
const PATCH_WINNER: &str = "00000000-0000-4000-8000-000000000702";
const RELEASE_WINNER: &str = "00000000-0000-4000-8000-000000000703";

/// The exact resource selector `convergence::workloads_command` issues. The
/// recording `kubectl` serves ONLY this string, so a narrower `deploy,sts,ds`
/// read fails outright instead of quietly satisfying a fixture.
const WORKLOADS: &str = "deployments,statefulsets,daemonsets,pods,jobs";

/// The named convergence sub-flags, excluding `exact`.
const FACETS: [&str; 7] = [
    "images",
    "generations",
    "replicas",
    "unavailable_zero",
    "hooks_healthy",
    "queues_drained",
    "manifest_matches",
];

/// One isolated recording harness holding `helm`, `kubectl`, the argv log, and
/// every captured payload.
struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new(retained: Option<&str>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        for name in ["helm", "kubectl"] {
            let path = temp.path().join(name);
            fs::write(&path, include_str!("data/upgrade-driver.py")).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        if let Some(retained) = retained {
            fs::write(
                temp.path().join("retained.json"),
                strip_annotation(retained),
            )
            .unwrap();
        }
        Self(temp)
    }

    /// Seed the complete upgrade checkpoint ConfigMap `kubectl get` returns.
    fn seed_checkpoint(&self, record: &Value) {
        self.seed_config_map(&checkpoint_config_map("41", None, Some(record)));
    }

    fn seed_config_map(&self, config_map: &Value) {
        fs::write(
            self.0.path().join("checkpoint.json"),
            serde_json::to_string(config_map).unwrap(),
        )
        .unwrap();
    }

    fn checkpoint(self, record: &Value) -> Self {
        self.seed_checkpoint(record);
        self
    }

    fn config_map(self, config_map: &Value) -> Self {
        self.seed_config_map(config_map);
        self
    }

    fn run(&self, scenario: &str, to: &str, chart: &str) -> Output {
        self.run_with(scenario, to, chart, &[])
    }

    /// `run`, plus any extra flags (`--dry-run`) after the fixed argument set.
    fn run_with(&self, scenario: &str, to: &str, chart: &str, extra: &[&str]) -> Output {
        self.run_with_env(scenario, to, chart, extra, &[])
    }

    /// `run_with`, plus env vars that must reach the `curie` binary.
    fn run_with_env(
        &self,
        scenario: &str,
        to: &str,
        chart: &str,
        extra: &[&str],
        extra_env: &[(&str, &str)],
    ) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args([
                "--json",
                "cluster",
                "upgrade",
                "--to",
                to,
                "--namespace",
                "ns",
                "--release",
                "rel",
                "--chart",
                chart,
                "--yes",
            ])
            .args(extra)
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap(),
            )
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env("UPGRADE_DRIVER_ROOT", self.0.path())
            .env("UPGRADE_DRIVER_SCENARIO", scenario);
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.output().unwrap()
    }

    /// Drive the public command with no `--chart`, from a caller-selected cwd.
    ///
    /// `release_channel` uses the debug-only channel switch to make the
    /// resolver branch durable regression coverage. It does not stand in for
    /// the separate build-stamped, checkout-free release-binary acceptance run.
    fn run_without_chart_with(
        &self,
        scenario: &str,
        to: &str,
        current_dir: &Path,
        release_channel: bool,
        extra: &[&str],
    ) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args([
                "--color=never",
                "--json",
                "cluster",
                "upgrade",
                "--to",
                to,
                "--namespace",
                "ns",
                "--release",
                "rel",
                "--yes",
            ])
            .args(extra)
            .current_dir(current_dir)
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env("UPGRADE_DRIVER_ROOT", self.0.path())
            .env("UPGRADE_DRIVER_SCENARIO", scenario)
            .env("XDG_CACHE_HOME", self.cache_home());
        if release_channel {
            command.env("CURIE_TEST_ARTIFACT_CHANNEL", "release");
        } else {
            command.env_remove("CURIE_TEST_ARTIFACT_CHANNEL");
        }
        command.output().unwrap()
    }

    /// Drive an explicit `--chart` in either artifact channel with an isolated,
    /// initially absent cache. This is the override-precedence control for the
    /// no-flag resolver tests above.
    fn run_override_in_channel(
        &self,
        scenario: &str,
        to: &str,
        chart: &str,
        release_channel: bool,
    ) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args([
                "--color=never",
                "--json",
                "cluster",
                "upgrade",
                "--to",
                to,
                "--namespace",
                "ns",
                "--release",
                "rel",
                "--chart",
                chart,
                "--yes",
            ])
            .current_dir(self.0.path())
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env("UPGRADE_DRIVER_ROOT", self.0.path())
            .env("UPGRADE_DRIVER_SCENARIO", scenario)
            .env("XDG_CACHE_HOME", self.cache_home());
        if release_channel {
            command.env("CURIE_TEST_ARTIFACT_CHANNEL", "release");
        } else {
            command.env_remove("CURIE_TEST_ARTIFACT_CHANNEL");
        }
        command.output().unwrap()
    }

    fn cache_home(&self) -> PathBuf {
        self.0.path().join("cache")
    }

    fn target_chart_cache(&self, to: &str) -> PathBuf {
        self.cache_home()
            .join("curie")
            .join(format!("v{to}"))
            .join(format!("curie-{to}.tgz"))
    }

    /// Make the recording Helm process behave like real Helm for one explicit
    /// local archive that is absent. The delegate still records the attempted
    /// consumer command before the wrapper returns Helm's refusal.
    fn reject_missing_chart(&self) -> PathBuf {
        let helm = self.0.path().join("helm");
        let recorder_dir = self.0.path().join("recorder");
        fs::create_dir(&recorder_dir).unwrap();
        let recorder = recorder_dir.join("helm");
        fs::rename(&helm, &recorder).unwrap();
        fs::write(
            &helm,
            r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "$UPGRADE_DRIVER_ROOT/missing-curie.tgz" ]; then
    "$UPGRADE_DRIVER_ROOT/recorder/helm" "$@" >/dev/null 2>&1
    echo "Error: chart $arg not found" >&2
    exit 1
  fi
done
exec "$UPGRADE_DRIVER_ROOT/recorder/helm" "$@"
"#,
        )
        .unwrap();
        fs::set_permissions(&helm, fs::Permissions::from_mode(0o755)).unwrap();
        self.0.path().join("missing-curie.tgz")
    }

    fn local(&self, scenario: &str) -> Output {
        self.run(scenario, "0.9.0", "charts/curie")
    }

    fn local_env(&self, scenario: &str, extra_env: &[(&str, &str)]) -> Output {
        self.run_with_env(scenario, "0.9.0", "charts/curie", &[], extra_env)
    }

    /// Every recorded invocation, in order.
    fn argv(&self) -> Vec<Vec<String>> {
        let log = self.0.path().join("argv.log");
        match fs::read_to_string(log) {
            Ok(text) => text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn issued(&self, prefix: &[&str]) -> bool {
        self.argv()
            .iter()
            .any(|call| call.len() >= prefix.len() && call[..prefix.len()] == *prefix)
    }

    /// The `helm upgrade` invocations. Empty means nothing was mutated.
    fn helm_upgrades(&self) -> Vec<Vec<String>> {
        self.argv()
            .into_iter()
            .filter(|call| {
                call.first().map(String::as_str) == Some("helm")
                    && call.get(1).map(String::as_str) == Some("upgrade")
            })
            .collect()
    }

    /// The chart operand from every Helm command that consumes the target
    /// chart: local metadata inspection, schema metadata render, and Apply.
    fn consumed_charts(&self) -> Vec<String> {
        self.argv()
            .into_iter()
            .filter_map(|call| {
                if call.first().map(String::as_str) != Some("helm") {
                    return None;
                }
                match call.get(1).map(String::as_str) {
                    Some("show") if call.get(2).map(String::as_str) == Some("chart") => {
                        call.get(3).cloned()
                    }
                    Some("template" | "upgrade") => call.get(3).cloned(),
                    _ => None,
                }
            })
            .collect()
    }

    /// The n-th values document (1-based) helm was handed via `-f`.
    fn values(&self, index: usize) -> String {
        fs::read_to_string(self.0.path().join(format!("values-{index}.yaml")))
            .unwrap_or_else(|error| panic!("values-{index}.yaml: {error}"))
    }

    fn captured_paths(&self, prefix: &str) -> Vec<std::path::PathBuf> {
        let prefix = format!("{prefix}-");
        let mut paths: Vec<_> = fs::read_dir(self.0.path())
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name()?.to_string_lossy().into_owned();
                name.starts_with(&prefix).then_some(path)
            })
            .collect();
        paths.sort_by_key(|path| {
            path.file_stem()
                .and_then(|stem| {
                    stem.to_string_lossy()
                        .rsplit('-')
                        .next()?
                        .parse::<u32>()
                        .ok()
                })
                .unwrap_or(0)
        });
        paths
    }

    fn persisted_payloads(&self) -> String {
        let mut all = String::new();
        for path in self.captured_paths("patch") {
            all.push_str(&fs::read_to_string(&path).unwrap());
        }
        all
    }

    fn patches(&self) -> Vec<Value> {
        self.captured_paths("patch")
            .iter()
            .map(|path| serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap())
            .collect()
    }

    fn created(&self) -> Vec<Value> {
        self.captured_paths("created")
            .iter()
            .map(|path| serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap())
            .collect()
    }

    /// Every persisted `UpgradeRecord`, parsed from the sole JSON Patch path.
    fn records(&self) -> Vec<Value> {
        self.patches()
            .iter()
            .flat_map(|patch| patch.as_array().into_iter().flatten())
            .filter_map(|operation| match operation["path"].as_str()? {
                "/data" => operation.pointer("/value/record")?.as_str(),
                "/data/record" => operation["value"].as_str(),
                _ => None,
            })
            .filter_map(|record| serde_json::from_str(record).ok())
            .collect()
    }

    fn last_record(&self) -> Value {
        self.records()
            .pop()
            .unwrap_or_else(|| panic!("no checkpoint record was ever persisted"))
    }

    fn config_map_state(&self) -> Value {
        serde_json::from_str(
            &fs::read_to_string(self.0.path().join("checkpoint.json"))
                .expect("checkpoint ConfigMap state"),
        )
        .expect("checkpoint ConfigMap JSON")
    }

    fn sentinel_record(&self) -> String {
        fs::read_to_string(self.0.path().join("sentinel-record.txt"))
            .expect("external sentinel record")
    }

    fn acquired_holder(&self) -> String {
        for created in self.created() {
            if let Some(holder) = created
                .pointer("/metadata/annotations/curietech.ai~1upgrade-holder")
                .and_then(Value::as_str)
            {
                return holder.to_string();
            }
        }
        for patch in self.patches() {
            for operation in patch.as_array().into_iter().flatten() {
                if operation["op"] != "add" {
                    continue;
                }
                if operation["path"] == "/metadata/annotations" {
                    if let Some(holder) = operation
                        .pointer("/value/curietech.ai~1upgrade-holder")
                        .and_then(Value::as_str)
                    {
                        return holder.to_string();
                    }
                }
                if operation["path"] == "/metadata/annotations/curietech.ai~1upgrade-holder" {
                    return operation["value"]
                        .as_str()
                        .expect("holder value")
                        .to_string();
                }
            }
        }
        panic!("no ownership acquisition was captured")
    }

    fn acquired_action(&self) -> String {
        for created in self.created() {
            if let Some(action) = created
                .pointer("/metadata/annotations/curietech.ai~1upgrade-action")
                .and_then(Value::as_str)
            {
                return action.to_string();
            }
        }
        for patch in self.patches() {
            for operation in patch.as_array().into_iter().flatten() {
                if operation["op"] != "add" {
                    continue;
                }
                if operation["path"] == "/metadata/annotations" {
                    if let Some(action) = operation
                        .pointer("/value/curietech.ai~1upgrade-action")
                        .and_then(Value::as_str)
                    {
                        return action.to_string();
                    }
                }
                if operation["path"] == "/metadata/annotations/curietech.ai~1upgrade-action" {
                    return operation["value"]
                        .as_str()
                        .expect("action value")
                        .to_string();
                }
            }
        }
        panic!("no ownership action was captured")
    }
}

/// Drop the `_fixture` block the committed overlays carry.
///
/// It is harness annotation -- provenance prose naming the released chart and
/// the migration class under test -- not Helm values, and `config_migrate.rs`
/// removes it the same way before migrating. A real release never retains it,
/// so feeding it to the fake `helm get values` would put prose (including the
/// very env-var names these tests assert are gone) into the `-f` payload and
/// make whole-file assertions lie in both directions.
fn strip_annotation(retained: &str) -> String {
    match serde_json::from_str::<Value>(retained) {
        Ok(Value::Object(mut map)) => {
            map.remove("_fixture");
            serde_json::to_string(&Value::Object(map)).unwrap()
        }
        _ => retained.to_owned(),
    }
}

/// The `-f` document helm was handed, parsed. Helm values are YAML, and JSON is
/// YAML, so this reads whichever the migration emits. Structural assertions
/// beat substring ones here: `extraEnv` promotion has to be judged on the list
/// itself, not on whether a name appears anywhere in the file.
fn values_doc(raw: &str) -> Value {
    serde_norway::from_str(raw).unwrap_or_else(|error| panic!("values payload ({error}): {raw}"))
}

/// Every `name` across all four `extraEnv` lists the migration walks.
fn extra_env_names(values: &Value) -> Vec<String> {
    ["worker", "api", "dispatcher"]
        .iter()
        .map(|owner| format!("/{owner}/extraEnv"))
        .chain(std::iter::once("/agentSandbox/runner/extraEnv".to_string()))
        .filter_map(|pointer| values.pointer(&pointer)?.as_array())
        .flatten()
        .filter_map(|entry| entry.get("name")?.as_str().map(ToOwned::to_owned))
        .collect()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn visible(output: &Output) -> String {
    format!("{}{}", stdout(output), stderr(output))
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "invalid JSON ({error}): {} / {}",
            stdout(output),
            stderr(output)
        )
    })
}

/// Assert that exactly the named convergence facets are false. The "every
/// other flag is still true" half is what stops a binding that simply mirrors
/// `issues.is_empty()` onto all seven from passing a single-facet fixture.
fn only_false(json: &Value, expected_false: &[&str]) {
    let conv = &json["convergence"];
    assert!(
        conv.is_object(),
        "no convergence payload was reported: {json}"
    );
    for facet in FACETS {
        let want = !expected_false.contains(&facet);
        assert_eq!(
            conv[facet],
            Value::Bool(want),
            "convergence.{facet} must be {want}; only {expected_false:?} may be false: {json}"
        );
    }
    assert_eq!(conv["exact"], false, "{json}");
}

/// Assert the observer -- not the old `kubectl get deploy,sts,ds` stub --
/// produced this convergence payload.
fn observed_for_real(fixture: &Fixture) {
    assert!(
        fixture.issued(&["helm", "get", "manifest"]),
        "Helm's installed target manifest must be read: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.issued(&["kubectl", "get", WORKLOADS]),
        "convergence must read the full workload/pod/job set, not `deploy,sts,ds`: {:?}",
        fixture.argv()
    );
}

const V084: &str = include_str!("data/upgrade-config/v0.8.4-user-values.json");
const V085: &str = include_str!("data/upgrade-config/v0.8.5-user-values.json");

/// The inline Slack credential carried by the v0.8.4 fixture. Split so the
/// literal never appears contiguously in source: a repository secret scanner
/// matches `xox[baprs]-` plus ten characters on any added line, and this file
/// asserts on the value precisely because it must never be emitted.
const SLACK_TOKEN: &str = concat!("xoxb", "-test-token-must-not-leak");

/// A checkpoint record whose durable phases are already complete through Apply.
fn applied_checkpoint(drain_completed: bool) -> Value {
    checkpoint_through(
        &[
            "plan",
            "validate",
            "drain_preflight",
            "checkpoint",
            "migrate",
            "apply",
        ],
        drain_completed,
    )
}

fn checkpoint_through(completed: &[&str], drain_completed: bool) -> Value {
    serde_json::json!({
        "target_version": "0.9.0",
        "from_version": "0.8.6",
        "known_good_version": "0.8.6",
        "completed": completed,
        "status": "in_progress",
        "plan": ["upgrade to 0.9.0"],
        "drain_completed": drain_completed,
        "convergence": null,
        "canary": null,
        "fail_forward": null,
        "resumed": false,
    })
}

fn assert_only_consumed_chart(fixture: &Fixture, expected: &str, context: &str) {
    let charts = fixture.consumed_charts();
    assert!(
        !charts.is_empty(),
        "{context}: fixture must observe at least one chart-consuming Helm command: {:?}",
        fixture.argv()
    );
    assert!(
        charts.iter().all(|chart| chart == expected),
        "{context}: every chart-consuming Helm command must use {expected}: {:?}",
        fixture.argv()
    );
}

/// #2593 -- compiled-CLI fixture proof for the release resolver branch. The
/// target archive is deliberately preseeded because this recording fixture is
/// offline; the build-stamped release-binary run remains separate acceptance
/// evidence. Running outside the checkout ensures the old literal fallback
/// cannot accidentally resolve to a real local chart. The target must differ
/// from the running CLI version so this test can distinguish target keyed cache
/// resolution from CLI version keyed resolution.
#[test]
fn release_channel_default_uses_target_chart_cache_outside_checkout() {
    const TO: &str = "0.8.9";

    assert_ne!(
        TO,
        env!("CARGO_PKG_VERSION"),
        "the target must differ from the running CLI version"
    );
    let fixture = Fixture::new(None);
    let target = fixture.target_chart_cache(TO);
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(
        &target,
        "offline fixture archive; Helm is recorded, not executed\n",
    )
    .unwrap();

    let output =
        fixture.run_without_chart_with("release-cache-prior", TO, fixture.0.path(), true, &[]);
    assert!(
        output.status.success(),
        "preseeded target archive must complete the recording fixture: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let target = target.to_string_lossy().into_owned();
    assert_only_consumed_chart(&fixture, &target, "release default");

    let argv = fixture.argv();
    let schema = argv
        .iter()
        .position(|call| is_schema_compat_template(call))
        .unwrap_or_else(|| panic!("schema compatibility must render the resolved chart: {argv:?}"));
    let apply = argv
        .iter()
        .position(|call| {
            call.first().map(String::as_str) == Some("helm")
                && call.get(1).map(String::as_str) == Some("upgrade")
        })
        .unwrap_or_else(|| panic!("one Helm upgrade must be recorded: {argv:?}"));
    assert!(
        schema < apply,
        "schema compatibility must be evaluated before mutation: {argv:?}"
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "the cached target must be applied exactly once: {argv:?}"
    );

    let cli_version_cache = fixture
        .cache_home()
        .join("curie")
        .join(format!("v{}", env!("CARGO_PKG_VERSION")))
        .join(format!("curie-{}.tgz", env!("CARGO_PKG_VERSION")))
        .to_string_lossy()
        .into_owned();
    assert!(
        !argv.iter().flatten().any(|arg| arg == &cli_version_cache),
        "upgrade resolution must key the cache by --to, not CLI version: {argv:?}"
    );
    assert!(
        !argv.iter().flatten().any(|arg| arg == "charts/curie"),
        "a release-channel default outside a checkout must not use the dev chart: {argv:?}"
    );
}

/// #2593 -- the dev-channel positive control keeps the checkout-local default,
/// including the #2588 schema render before the single Apply.
#[test]
fn dev_channel_default_uses_local_chart_from_checkout() {
    let fixture = Fixture::new(None);
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output =
        fixture.run_without_chart_with("schema-compatible", "0.9.0", repo_root, false, &[]);
    assert!(
        output.status.success(),
        "dev default must complete from the source checkout: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert_only_consumed_chart(&fixture, "charts/curie", "dev default");
    assert!(
        fixture
            .argv()
            .iter()
            .any(|call| is_schema_compat_template(call)),
        "schema compatibility must still render the local target chart: {:?}",
        fixture.argv()
    );
    assert_eq!(fixture.helm_upgrades().len(), 1, "{:?}", fixture.argv());
    assert!(
        !fixture.cache_home().exists(),
        "the dev local default must not consult or create the release cache"
    );
}

/// #2593 -- a dev binary invoked outside a checkout has no implicit chart to
/// validate or apply. It must refuse with the resolver's actionable remedy
/// before reaching either recording process.
#[test]
fn dev_channel_without_local_default_refuses_before_helm() {
    let fixture = Fixture::new(None);
    let output =
        fixture.run_without_chart_with("schema-compatible", "0.9.0", fixture.0.path(), false, &[]);
    assert!(
        !output.status.success(),
        "dev without charts/curie must refuse: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let message = visible(&output);
    assert!(
        message.contains("dev build")
            && message.contains("charts/curie")
            && message.contains("--chart"),
        "refusal must name the missing default and explicit remedy: {message}"
    );
    assert!(
        fixture.argv().is_empty(),
        "resolution must refuse before any Helm/Kubectl call: {:?}",
        fixture.argv()
    );
}

/// #2593 -- an explicit local archive is authoritative in both channels. The
/// release-channel loop is fixture-only branch coverage, and the absent cache
/// proves the override did not fall through to target artifact resolution.
#[test]
fn explicit_local_chart_wins_in_both_channels_without_cache() {
    for release_channel in [false, true] {
        let fixture = Fixture::new(None);
        let chart = fixture.0.path().join("explicit-curie.tgz");
        fs::write(
            &chart,
            "offline fixture archive; Helm is recorded, not executed\n",
        )
        .unwrap();
        let chart = chart.to_string_lossy().into_owned();
        let output =
            fixture.run_override_in_channel("schema-compatible", "0.9.0", &chart, release_channel);
        let channel = if release_channel { "release" } else { "dev" };
        assert!(
            output.status.success(),
            "explicit local chart must complete in {channel}: {} / {}",
            stdout(&output),
            stderr(&output)
        );
        assert_only_consumed_chart(&fixture, &chart, channel);
        assert!(
            !fixture.cache_home().exists(),
            "explicit {channel} override must not consult or create the target cache"
        );
    }
}

/// #2593 / #2594 -- an explicit Helm-resolved ref also wins in both channels,
/// stays uncached, and carries `--version <to>` through target schema render
/// and Apply.
#[test]
fn explicit_chart_ref_wins_in_both_channels_without_cache() {
    const CHART: &str = "oci://example.invalid/curie";
    for release_channel in [false, true] {
        let fixture = Fixture::new(None);
        let output =
            fixture.run_override_in_channel("schema-compatible", "0.9.0", CHART, release_channel);
        let channel = if release_channel { "release" } else { "dev" };
        assert!(
            output.status.success(),
            "explicit chart ref must complete in {channel}: {} / {}",
            stdout(&output),
            stderr(&output)
        );
        assert_only_consumed_chart(&fixture, CHART, channel);

        let chart_calls: Vec<_> = fixture
            .argv()
            .into_iter()
            .filter(|call| {
                is_schema_compat_template(call)
                    || (call.first().map(String::as_str) == Some("helm")
                        && call.get(1).map(String::as_str) == Some("upgrade"))
            })
            .collect();
        assert_eq!(
            chart_calls.len(),
            2,
            "{channel} must render schema metadata and Apply: {:?}",
            fixture.argv()
        );
        for call in chart_calls {
            assert!(
                call.windows(2)
                    .any(|pair| pair[0] == "--version" && pair[1] == "0.9.0"),
                "{channel} must pin the explicit ref to --to: {call:?}"
            );
        }
        assert!(
            !fixture.cache_home().exists(),
            "explicit {channel} ref must not consult or create the target cache"
        );
    }
}

/// #2593 -- a cold release-channel dry-run is network-free and cannot inspect
/// the absent target archive yet. It must say those target checks are pending,
/// while still running the independent retained-configuration migration.
#[test]
fn release_channel_dry_run_plans_target_cache_and_url_without_fetching() {
    let fixture = Fixture::new(Some(V084));
    let target = fixture.target_chart_cache("0.9.0");
    let url = "https://github.com/curie-eng/curie/releases/download/v0.9.0/curie-0.9.0.tgz";
    let output = fixture.run_without_chart_with(
        "schema-compatible",
        "0.9.0",
        fixture.0.path(),
        true,
        &["--dry-run"],
    );
    assert!(
        output.status.success(),
        "release dry-run must plan without downloading: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let target = target.to_string_lossy().into_owned();
    let output_text = visible(&output);
    assert!(
        output_text.contains(url) && output_text.contains(&target),
        "dry-run must report the target release URL and cache path: {output_text}"
    );
    assert!(
        !fixture.cache_home().exists(),
        "dry-run must not create the cache or fetch the target archive"
    );
    let plan = json(&output)["plan"].as_array().unwrap().clone();
    let helm_line = plan
        .iter()
        .filter_map(Value::as_str)
        .find(|line| line.starts_with("helm upgrade "))
        .unwrap_or_else(|| panic!("cold dry-run has no Helm plan line: {plan:?}"));
    assert_eq!(
        helm_line,
        format!("helm upgrade rel {target} -n ns --wait --timeout 15m -f <retained-values>"),
        "the downloaded archive is a local chart at Apply, so the cold plan must omit --version"
    );
    assert!(
        plan.iter()
            .filter_map(Value::as_str)
            .any(|line| line.contains("pending")
                && line.contains("chart metadata")
                && line.contains("schema compatibility")),
        "cold dry-run must name the unavailable target checks as pending: {plan:?}"
    );
    assert!(
        plan.iter()
            .filter_map(Value::as_str)
            .any(|line| line.contains("config schema: 0.8.6 -> 0.9.0")),
        "cold dry-run must still migrate retained configuration: {plan:?}"
    );
    assert_eq!(
        fixture
            .argv()
            .iter()
            .filter(|call| call.len() >= 3 && call[..3] == ["helm", "get", "values"])
            .count(),
        1,
        "retained configuration must still be read exactly once: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.consumed_charts().is_empty(),
        "an absent archive cannot be inspected or rendered during a network-free dry-run: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "dry-run must not mutate: {:?}",
        fixture.argv()
    );
}

/// #2593 -- a cached release archive is available during dry-run, so the
/// local metadata pin and target schema validation must both execute. Neither
/// command nor the plan may carry the Helm version flag for a local archive.
#[test]
fn release_channel_cached_dry_run_checks_target_without_version_flag() {
    let fixture = Fixture::new(Some(V084));
    let target = fixture.target_chart_cache("0.9.0");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(
        &target,
        "offline fixture archive; Helm is recorded, not executed\n",
    )
    .unwrap();

    let output = fixture.run_without_chart_with(
        "schema-compatible",
        "0.9.0",
        fixture.0.path(),
        true,
        &["--dry-run"],
    );
    assert!(
        output.status.success(),
        "cached release dry-run must complete its checks: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let target = target.to_string_lossy().into_owned();
    assert_only_consumed_chart(&fixture, &target, "cached release dry-run");
    let chart_calls: Vec<_> = fixture
        .argv()
        .into_iter()
        .filter(|call| {
            (call.get(1).map(String::as_str) == Some("show")
                && call.get(2).map(String::as_str) == Some("chart"))
                || is_schema_compat_template(call)
        })
        .collect();
    assert_eq!(
        chart_calls.len(),
        2,
        "cached dry-run must inspect chart metadata and render target schema metadata: {:?}",
        fixture.argv()
    );
    assert!(
        chart_calls
            .iter()
            .all(|call| !call.iter().any(|arg| arg == "--version")),
        "local archive checks must not carry a version flag Helm ignores: {chart_calls:?}"
    );
    let plan = json(&output)["plan"].as_array().unwrap().clone();
    assert!(
        !plan
            .iter()
            .filter_map(Value::as_str)
            .any(|line| line.contains("pending")),
        "available target checks must not be reported as pending: {plan:?}"
    );
    assert!(
        plan.iter().filter_map(Value::as_str).any(|line| {
            line == format!(
                "helm upgrade rel {target} -n ns --wait --timeout 15m -f <retained-values>"
            ) && !line.contains("--version")
        }),
        "cached plan must use the exact local-archive command head: {plan:?}"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "cached dry-run must not mutate: {:?}",
        fixture.argv()
    );
}

/// #2593 -- pending target checks do not excuse an independent retained-value
/// conflict. A cold dry-run must still surface the same Validate refusal while
/// keeping chart inspection pending and the release cache absent.
#[test]
fn release_channel_cold_dry_run_still_reports_config_conflict() {
    let fixture = Fixture::new(Some(&conflict_values("999").to_string()));
    let output = fixture.run_without_chart_with(
        "schema-compatible",
        "0.9.0",
        fixture.0.path(),
        true,
        &["--dry-run"],
    );
    assert!(
        !output.status.success(),
        "a refusing dry-run reports the refusal in its plan and fails (#2862): {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let plan = json(&output)["plan"].to_string();
    assert!(
        plan.contains("refusal at validate")
            && plan.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S")
            && plan.contains("worker.runnerTotalTimeoutSeconds"),
        "cold dry-run must preserve the retained-configuration refusal: {plan}"
    );
    assert!(
        plan.contains("pending")
            && plan.contains("chart metadata")
            && plan.contains("schema compatibility"),
        "the unrelated target checks must remain explicitly pending: {plan}"
    );
    assert!(
        fixture.issued(&["helm", "get", "values"]),
        "the conflict must come from the real retained-values consumer: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.consumed_charts().is_empty(),
        "cold target checks must not execute against an absent archive: {:?}",
        fixture.argv()
    );
    assert!(!fixture.cache_home().exists());
    assert!(fixture.helm_upgrades().is_empty());
}

/// #2593 -- an explicit operand never inherits the release resolver's pending
/// download state. Real Helm refuses this absent local archive during target
/// schema rendering, and that refusal must remain visible before mutation.
#[test]
fn explicit_missing_chart_refuses_without_becoming_pending_release_download() {
    let fixture = Fixture::new(None);
    let missing = fixture.reject_missing_chart();
    let missing = missing.to_string_lossy().into_owned();
    let output = fixture.run_with("schema-compatible", "0.9.0", &missing, &["--dry-run"]);
    assert!(
        !output.status.success(),
        "dry-run refusal belongs in the plan and fails the dry run (#2862)"
    );
    let plan = json(&output)["plan"].to_string();
    assert!(
        plan.contains("refusal at validate")
            && plan.contains("could not render target schema compatibility metadata")
            && plan.contains(&missing),
        "the target consumer's missing-chart refusal must reach the plan: {plan}"
    );
    assert!(
        !plan.contains("pending"),
        "an explicit operand must never masquerade as a pending release asset: {plan}"
    );
    assert!(
        !fixture.cache_home().exists(),
        "an explicit operand must not consult or populate release cache"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "missing explicit chart must refuse before mutation: {:?}",
        fixture.argv()
    );
}

fn checkpoint_config_map(
    resource_version: &str,
    annotations: Option<Value>,
    record: Option<&Value>,
) -> Value {
    let mut metadata = serde_json::json!({
        "name": "rel-upgrade-checkpoint",
        "namespace": "ns",
        "resourceVersion": resource_version,
        "labels": {
            "app.kubernetes.io/managed-by": "curie",
            "curietech.ai/upgrade": "checkpoint",
        },
    });
    if let Some(annotations) = annotations {
        metadata["annotations"] = annotations;
    }
    let mut config_map = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
    });
    if let Some(record) = record {
        config_map["data"] = serde_json::json!({
            "record": serde_json::to_string(record).unwrap(),
        });
    }
    config_map
}

fn is_checkpoint_get(call: &[String]) -> bool {
    call.len() >= 3
        && call[0] == "kubectl"
        && call[1] == "get"
        && call[2] == "configmap"
        && call.iter().any(|arg| arg == "rel-upgrade-checkpoint")
}

fn argv_starts(call: &[String], prefix: &[&str]) -> bool {
    call.len() >= prefix.len()
        && call
            .iter()
            .zip(prefix)
            .all(|(actual, expected)| actual == expected)
}

fn is_checkpoint_patch(call: &[String]) -> bool {
    call.len() >= 3
        && call[0] == "kubectl"
        && call[1] == "patch"
        && call[2] == "configmap"
        && call.iter().any(|arg| arg == "rel-upgrade-checkpoint")
}

fn patch_has(patch: &Value, operation: &str, path: &str) -> bool {
    patch.as_array().is_some_and(|operations| {
        operations
            .iter()
            .any(|item| item["op"] == operation && item["path"] == path)
    })
}

fn is_record_patch(patch: &Value) -> bool {
    patch_has(patch, "add", "/data")
        || patch_has(patch, "add", "/data/record")
        || patch_has(patch, "replace", "/data/record")
}

fn is_release_patch(patch: &Value) -> bool {
    patch_has(
        patch,
        "remove",
        "/metadata/annotations/curietech.ai~1upgrade-holder",
    )
}

// T1(a) -- #2301 "requires no direct Helm command or merge-flag choice", as
// amended by driver Ruling 2: a local chart path is never pinned with
// `--version` (Helm silently ignores it there).
#[test]
fn local_chart_upgrade_does_not_pass_a_version_flag() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    let upgrades = fixture.helm_upgrades();
    assert_eq!(
        upgrades.len(),
        1,
        "one mutation expected: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        !upgrades[0].iter().any(|arg| arg == "--version"),
        "a local chart path must not carry a --version pin Helm ignores: {:?}",
        upgrades[0]
    );
}

/// Helm keeps release status and chart metadata on separate command surfaces.
/// The v3.16.4 sources are:
/// https://github.com/helm/helm/blob/v3.16.4/cmd/helm/status.go
/// https://github.com/helm/helm/blob/v3.16.4/cmd/helm/get_metadata.go
/// A prior runtime observation returned metadata with string version `0.9.0`,
/// numeric revision `2`, chart `acme-upgrade`, and appVersion `0.9.0`.
#[test]
fn installed_chart_version_comes_from_helm_metadata() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    assert!(
        output.status.success(),
        "the real metadata shape must complete the upgrade: {:?} / {}",
        fixture.argv(),
        visible(&output)
    );
    let expected = ["helm", "get", "metadata", "rel", "-n", "ns", "-o", "json"];
    let reads: Vec<_> = fixture
        .argv()
        .into_iter()
        .filter(|call| call.len() >= 3 && call[..3] == ["helm", "get", "metadata"])
        .collect();
    assert!(!reads.is_empty(), "chart metadata was never read");
    assert!(
        reads
            .iter()
            .all(|call| { call.iter().map(String::as_str).collect::<Vec<_>>() == expected }),
        "chart version reads must use the real Helm metadata command: {reads:?}"
    );
    let result = json(&output);
    assert_eq!(result["from_version"], "0.8.6", "{result}");
    assert_eq!(result["known_good_version"], "0.9.0", "{result}");
}

/// Helm metadata's top-level chart version is a string. A numeric value cannot
/// become the installed chart version.
#[test]
fn numeric_metadata_version_is_not_accepted_as_the_chart_version() {
    let fixture = Fixture::new(None);
    let output = fixture.local("numeric-metadata-version");
    assert!(
        fixture.issued(&["helm", "get", "metadata"]),
        "the version decision must read Helm metadata: {:?}",
        fixture.argv()
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Apply must run before the postcondition rejects numeric metadata: {:?}",
        fixture.argv()
    );
    assert!(
        !output.status.success(),
        "a numeric metadata version must not satisfy target 0.9.0: {}",
        stdout(&output)
    );
    assert!(
        visible(&output).contains("release reports no version"),
        "numeric metadata must preserve the absent version behavior: {}",
        visible(&output)
    );
    let records = fixture.records();
    assert!(
        !records.is_empty(),
        "the run must persist pre Apply phases before the postcondition fails"
    );
    for record in records {
        assert_ne!(
            record["known_good_version"], "0.9.0",
            "known good must not advance from numeric metadata: {record}"
        );
        assert!(
            !record["completed"]
                .as_array()
                .is_some_and(|done| done.iter().any(|phase| phase == "commit")),
            "Commit must not run after numeric metadata was rejected: {record}"
        );
    }
}

/// Kubernetes documents ConfigMap metadata and optional data here:
/// https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/config-map-v1/
/// Kubernetes documents resourceVersion conflict detection here:
/// https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
/// The recording server omits empty annotations and data just as the observed
/// Kubernetes API did. Real race acceptance remains in the cluster run.
#[test]
fn ownership_create_precedes_every_upgrade_snapshot_and_has_server_state() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    assert!(
        output.status.success(),
        "healthy create path must complete: {:?} / {}",
        fixture.argv(),
        visible(&output)
    );

    let argv = fixture.argv();
    assert!(
        is_checkpoint_get(&argv[0]),
        "ownership read must be first: {argv:?}"
    );
    assert!(
        argv[0].windows(2).any(|pair| pair == ["-o", "json"])
            && !argv[0].iter().any(|arg| arg.contains("jsonpath")),
        "acquisition must read the complete ConfigMap object: {:?}",
        argv[0]
    );
    let first_snapshot = argv
        .iter()
        .position(|call| {
            argv_starts(call, &["helm", "get", "metadata"])
                || argv_starts(call, &["helm", "get", "values"])
                || argv_starts(call, &["helm", "show", "chart"])
                || argv_starts(call, &["helm", "template"])
                || (call.first().map(String::as_str) == Some("kubectl")
                    && call.iter().any(|arg| arg == "exec"))
        })
        .expect("upgrade snapshot command");
    let create_position = argv
        .iter()
        .position(|call| argv_starts(call, &["kubectl", "create"]))
        .expect("ownership create");
    assert!(
        create_position < first_snapshot,
        "the server must return ownership before any metadata, values, or schema snapshot: {argv:?}"
    );

    let created = fixture.created();
    assert_eq!(created.len(), 1, "one create payload expected: {argv:?}");
    let annotations = created[0]["metadata"]["annotations"]
        .as_object()
        .expect("ownership annotations");
    assert_eq!(
        annotations.len(),
        2,
        "create must carry only ownership annotations"
    );
    assert!(annotations.contains_key(HOLDER_ANNOTATION));
    assert!(
        annotations[ACTION_ANNOTATION]
            .as_str()
            .is_some_and(|action| action.contains("0.9.0")),
        "the redacted action must name the requested target: {annotations:?}"
    );
    assert!(
        created[0].get("data").is_none(),
        "the API can omit empty data, so acquisition must not invent it: {}",
        created[0]
    );
    assert_eq!(
        created[0]["metadata"]["labels"],
        serde_json::json!({
            "app.kubernetes.io/managed-by": "curie",
            "curietech.ai/upgrade": "checkpoint",
        }),
        "the created owner must retain the checkpoint labels"
    );
    assert!(
        created[0].pointer("/metadata/resourceVersion").is_none(),
        "resourceVersion must come from the server response: {}",
        created[0]
    );
    assert!(
        argv[create_position]
            .windows(2)
            .any(|pair| pair == ["-o", "json"]),
        "create must request the complete server object: {:?}",
        argv[create_position]
    );
    assert!(
        fixture
            .helm_upgrades()
            .iter()
            .all(|call| !call.iter().any(|arg| arg == "--create-namespace")),
        "ownership cannot be established in a namespace Helm creates later: {argv:?}"
    );
    assert!(
        !argv
            .iter()
            .any(|call| argv_starts(call, &["kubectl", "apply"])),
        "checkpoint writes have one JSON Patch path: {argv:?}"
    );
}

#[test]
fn existing_unowned_checkpoint_with_omitted_annotations_adds_the_complete_map() {
    let fixture = Fixture::new(None).config_map(&checkpoint_config_map("41", None, None));
    let output = fixture.local("healthy");
    assert!(output.status.success(), "{}", visible(&output));
    let holder = fixture.acquired_holder();
    let action = fixture.acquired_action();
    assert_eq!(
        fixture.patches()[0],
        serde_json::json!([
            {"op": "test", "path": "/metadata/resourceVersion", "value": "41"},
            {
                "op": "add",
                "path": "/metadata/annotations",
                "value": {
                    (HOLDER_ANNOTATION): holder,
                    (ACTION_ANNOTATION): action,
                }
            }
        ]),
        "an omitted parent map requires one complete map add"
    );
}

#[test]
fn existing_unowned_checkpoint_tests_the_full_annotation_map_before_child_adds() {
    let observed = serde_json::json!({
        "example.com/keep": "yes",
        "example.com/second": "still-here",
    });
    let fixture =
        Fixture::new(None).config_map(&checkpoint_config_map("57", Some(observed.clone()), None));
    let output = fixture.local("healthy");
    assert!(output.status.success(), "{}", visible(&output));
    let holder = fixture.acquired_holder();
    let action = fixture.acquired_action();
    assert_eq!(
        fixture.patches()[0],
        serde_json::json!([
            {"op": "test", "path": "/metadata/resourceVersion", "value": "57"},
            {"op": "test", "path": "/metadata/annotations", "value": observed},
            {
                "op": "add",
                "path": "/metadata/annotations/curietech.ai~1upgrade-holder",
                "value": holder,
            },
            {
                "op": "add",
                "path": "/metadata/annotations/curietech.ai~1upgrade-action",
                "value": action,
            },
        ]),
        "a present map requires a complete observed map test before escaped child adds"
    );
    assert_eq!(
        fixture.config_map_state()["metadata"]["annotations"],
        serde_json::json!({
            "example.com/keep": "yes",
            "example.com/second": "still-here",
        }),
        "normal release must preserve unrelated annotations"
    );
}

#[test]
fn malformed_annotations_fail_closed_before_snapshots_or_writes() {
    let fixture = Fixture::new(None).config_map(&checkpoint_config_map(
        "61",
        Some(serde_json::json!("malformed")),
        None,
    ));
    let output = fixture.local("healthy");
    assert!(
        !output.status.success(),
        "malformed annotations must refuse"
    );
    assert!(
        visible(&output).to_lowercase().contains("annotation"),
        "the malformed field must be named: {}",
        visible(&output)
    );
    assert!(
        fixture.patches().is_empty(),
        "no patch is safe: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.created().is_empty(),
        "the existing object must not be recreated"
    );
    assert!(
        !fixture
            .argv()
            .iter()
            .any(|call| call.first().map(String::as_str) == Some("helm")),
        "ownership parsing must precede all Helm snapshots: {:?}",
        fixture.argv()
    );
}

#[test]
fn existing_holder_refusal_names_holder_and_action_without_helm_writes() {
    let holder = "00000000-0000-4000-8000-000000000601";
    let action = "upgrade to 0.9.0 by another current CLI";
    let fixture = Fixture::new(None).config_map(&checkpoint_config_map(
        "63",
        Some(serde_json::json!({
            (HOLDER_ANNOTATION): holder,
            (ACTION_ANNOTATION): action,
        })),
        None,
    ));
    let output = fixture.local("healthy");
    let message = visible(&output);
    assert!(!output.status.success(), "an existing holder must refuse");
    assert!(
        message.contains(holder),
        "refusal must name the holder: {message}"
    );
    assert!(
        message.contains(action),
        "refusal must name its action: {message}"
    );
    assert!(
        message.contains("wait") || message.contains("stopped"),
        "refusal must give a safe next action: {message}"
    );
    assert!(fixture.patches().is_empty(), "the loser must write nothing");
    assert!(
        fixture.created().is_empty(),
        "the loser must create nothing"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "the loser must not reach Helm mutation"
    );
}

#[test]
fn create_race_loser_reads_one_diagnostic_and_names_the_winner() {
    let fixture = Fixture::new(None);
    let output = fixture.local("acquire-create-conflict");
    let message = visible(&output);
    assert!(!output.status.success(), "the create loser must refuse");
    assert!(
        message.contains(CREATE_WINNER),
        "winner holder missing: {message}"
    );
    assert!(
        message.contains("upgrade to 0.9.0"),
        "winner action missing: {message}"
    );
    assert!(
        message.contains("wait") || message.contains("stopped"),
        "the loser needs safe recovery guidance: {message}"
    );
    assert_eq!(fixture.created().len(), 1, "create must not retry");
    assert_eq!(
        fixture
            .argv()
            .iter()
            .filter(|call| is_checkpoint_get(call))
            .count(),
        2,
        "one initial read and one diagnostic read are allowed: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.patches().is_empty(),
        "the create loser must not switch to patch"
    );
    assert!(
        !fixture
            .argv()
            .iter()
            .any(|call| call.first().map(String::as_str) == Some("helm")),
        "the loser must stop before Helm snapshots: {:?}",
        fixture.argv()
    );
}

#[test]
fn patch_race_loser_reads_one_diagnostic_and_never_retries() {
    let fixture = Fixture::new(None).config_map(&checkpoint_config_map("70", None, None));
    let output = fixture.local("acquire-patch-conflict");
    let message = visible(&output);
    assert!(!output.status.success(), "the patch loser must refuse");
    assert!(
        message.contains(PATCH_WINNER),
        "winner holder missing: {message}"
    );
    assert!(
        message.contains("upgrade to 0.9.0"),
        "winner action missing: {message}"
    );
    assert!(
        message.contains("wait") || message.contains("stopped"),
        "the loser needs safe recovery guidance: {message}"
    );
    assert_eq!(
        fixture.patches().len(),
        1,
        "acquisition patch must not retry"
    );
    assert_eq!(
        fixture
            .argv()
            .iter()
            .filter(|call| is_checkpoint_get(call))
            .count(),
        2,
        "one initial read and one diagnostic read are allowed: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.created().is_empty(),
        "the patch loser must not switch to create"
    );
    assert!(
        !fixture
            .argv()
            .iter()
            .any(|call| call.first().map(String::as_str) == Some("helm")),
        "the loser must stop before Helm snapshots: {:?}",
        fixture.argv()
    );
}

/// kubectl sends JSON Patch as documented here:
/// https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/
#[test]
fn record_and_release_patches_chain_server_resource_versions_and_holder() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    assert!(output.status.success(), "{}", visible(&output));
    let holder = fixture.acquired_holder();
    let patches = fixture.patches();
    assert!(
        patches.len() > 3,
        "the lifecycle must persist several phases: {patches:?}"
    );
    let argv = fixture.argv();
    let patch_calls: Vec<_> = argv
        .iter()
        .filter(|call| is_checkpoint_patch(call))
        .collect();
    assert_eq!(
        patch_calls.len(),
        patches.len(),
        "every captured patch needs one command"
    );
    assert!(
        patch_calls.iter().all(|call| {
            call.iter().any(|arg| arg == "--type=json")
                && call.iter().any(|arg| arg == "--patch-file")
                && call.windows(2).any(|pair| pair == ["-o", "json"])
        }),
        "every write must request one server returned JSON object: {patch_calls:?}"
    );

    for (index, patch) in patches.iter().enumerate() {
        let operations = patch.as_array().expect("JSON Patch array");
        assert_eq!(
            operations[0],
            serde_json::json!({
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": (100 + index).to_string(),
            }),
            "each write must use the prior server returned resourceVersion: {patch}"
        );
        assert_eq!(
            operations[1],
            serde_json::json!({
                "op": "test",
                "path": "/metadata/annotations/curietech.ai~1upgrade-holder",
                "value": holder,
            }),
            "each write must test this invocation's holder: {patch}"
        );
    }

    let record_patches: Vec<_> = patches
        .iter()
        .filter(|patch| is_record_patch(patch))
        .collect();
    assert!(!record_patches.is_empty(), "no record patch captured");
    assert_eq!(
        record_patches[0].as_array().unwrap()[2]["op"],
        "add",
        "an omitted data parent requires add"
    );
    assert_eq!(
        record_patches[0].as_array().unwrap()[2]["path"],
        "/data",
        "the record cannot be added beneath an omitted parent"
    );
    assert!(
        record_patches.iter().skip(1).all(|patch| {
            let operation = &patch.as_array().unwrap()[2];
            operation["path"] == "/data/record"
                && (operation["op"] == "add" || operation["op"] == "replace")
        }),
        "once data exists, later writes must target only data.record: {record_patches:?}"
    );

    let releases: Vec<_> = patches
        .iter()
        .filter(|patch| is_release_patch(patch))
        .collect();
    assert_eq!(releases.len(), 1, "normal completion releases exactly once");
    let release = releases[0].as_array().unwrap();
    assert_eq!(
        release.len(),
        4,
        "release is two tests and two removes: {release:?}"
    );
    assert!(release.iter().any(|operation| {
        operation["op"] == "remove"
            && operation["path"] == "/metadata/annotations/curietech.ai~1upgrade-action"
    }));
    assert!(release.iter().any(|operation| {
        operation["op"] == "remove"
            && operation["path"] == "/metadata/annotations/curietech.ai~1upgrade-holder"
    }));
    assert!(
        fixture.config_map_state()["metadata"]
            .get("annotations")
            .is_none(),
        "the API omits the empty annotations map after normal release"
    );
}

#[test]
fn stale_record_cas_preserves_the_external_sentinel_without_retry_or_reacquire() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-record-cas");
    assert!(!output.status.success(), "stale persistence must fail");
    assert!(
        visible(&output).contains("resourceVersion") || visible(&output).contains("checkpoint"),
        "the stale checkpoint failure must surface: {}",
        visible(&output)
    );
    let patches = fixture.patches();
    assert_eq!(
        patches.len(), 2,
        "one stale record attempt and one release attempt are the complete write budget: {patches:?}"
    );
    assert_eq!(
        patches
            .iter()
            .filter(|patch| is_record_patch(patch))
            .count(),
        1,
        "a stale record write must never retry: {patches:?}"
    );
    assert_eq!(
        patches
            .iter()
            .filter(|patch| is_release_patch(patch))
            .count(),
        1,
        "ordinary error cleanup gets one CAS release attempt: {patches:?}"
    );
    assert_eq!(
        fixture.created().len(),
        1,
        "ownership must not be reacquired"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "stale persistence precedes Helm mutation"
    );
    assert_eq!(
        fixture.config_map_state()["data"]["record"],
        fixture.sentinel_record(),
        "the external record must remain byte identical"
    );
}

#[test]
fn release_conflict_after_success_is_nonzero_and_reports_the_observed_holder() {
    let fixture = Fixture::new(None);
    let output = fixture.local("release-conflict");
    let holder = fixture.acquired_holder();
    let message = visible(&output);
    assert!(
        !output.status.success(),
        "a failed release cannot report success"
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "the lifecycle itself must complete"
    );
    assert!(
        message.contains("release"),
        "release failure missing: {message}"
    );
    assert!(
        message.contains(RELEASE_WINNER),
        "diagnostic must name the holder actually observed: {message}"
    );
    assert!(
        !message.contains(&format!("holder {holder} remains"))
            && !message.contains(&format!("{holder} still holds")),
        "the diagnostic must not claim the original holder remains: {message}"
    );
    assert_eq!(
        fixture.config_map_state()["metadata"]["annotations"][HOLDER_ANNOTATION],
        RELEASE_WINNER
    );
    assert_eq!(
        fixture
            .patches()
            .iter()
            .filter(|patch| is_release_patch(patch))
            .count(),
        1,
        "release must not retry"
    );
    assert_eq!(
        fixture
            .argv()
            .iter()
            .filter(|call| is_checkpoint_get(call))
            .count(),
        2,
        "release failure permits one diagnostic read"
    );
}

#[test]
fn release_failure_preserves_the_structured_lifecycle_failure() {
    let fixture = Fixture::new(None);
    let output = fixture.local("converge-then-release-conflict");
    assert!(
        !output.status.success(),
        "both failures require a nonzero exit"
    );
    let report = json(&output);
    assert_eq!(report["status"], "failed", "{report}");
    assert_eq!(report["phase"], "converge", "{report}");
    assert_eq!(report["convergence"]["images"], false, "{report}");
    assert!(report["fail_forward"].is_object(), "{report}");
    assert!(
        visible(&output).contains(RELEASE_WINNER),
        "the release diagnostic must accompany the lifecycle report: {}",
        visible(&output)
    );
}

#[test]
fn absent_namespace_guides_cluster_up_without_a_namespace_read_or_helm_write() {
    let fixture = Fixture::new(None);
    let output = fixture.local("namespace-absent");
    assert!(!output.status.success(), "an absent namespace must refuse");
    let report = json(&output);
    let error = report["error"]
        .as_str()
        .expect("namespace refusal error string");
    assert!(
        error.contains("curie cluster up"),
        "the recovery guidance must establish the install first: {error}"
    );
    assert!(
        error.contains("namespaces \"ns\" not found"),
        "the real ConfigMap GET failure must remain visible: {error}"
    );
    let argv = fixture.argv();
    assert!(
        !argv.iter().any(|call| {
            argv_starts(call, &["kubectl", "get", "namespace"])
                || argv_starts(call, &["kubectl", "get", "namespaces"])
        }),
        "ownership must not add a cluster scoped Namespace read: {argv:?}"
    );
    assert!(
        fixture.created().is_empty(),
        "a namespace NotFound from ConfigMap GET must not be mistaken for an absent object"
    );
    assert!(fixture.patches().is_empty(), "no object exists to patch");
    assert!(
        !argv
            .iter()
            .any(|call| call.first().map(String::as_str) == Some("helm")),
        "namespace refusal must precede all Helm snapshots: {argv:?}"
    );
    assert!(
        !argv.iter().flatten().any(|arg| arg == "--create-namespace"),
        "cluster upgrade must never ask Helm to create the namespace: {argv:?}"
    );
}

#[test]
fn namespace_disappearing_before_create_guides_cluster_up_without_helm_write() {
    let fixture = Fixture::new(None);
    let output = fixture.local("namespace-disappears");
    assert!(
        !output.status.success(),
        "a disappeared namespace must refuse"
    );
    let report = json(&output);
    let message = report["error"]
        .as_str()
        .expect("namespace disappearance error string");
    assert!(
        message.contains("Error from server (NotFound): error when creating \""),
        "the real create failure prefix must remain visible: {message}"
    );
    assert!(
        message.contains("curie cluster up"),
        "the recovery guidance must establish the install first: {message}"
    );
    assert!(
        message.contains("error when creating") && message.contains("namespaces \"ns\" not found"),
        "the real create failure shape must remain visible: {message}"
    );
    let argv = fixture.argv();
    assert!(
        !argv.iter().any(|call| {
            argv_starts(call, &["kubectl", "get", "namespace"])
                || argv_starts(call, &["kubectl", "get", "namespaces"])
        }),
        "ownership must not add a cluster scoped Namespace read: {argv:?}"
    );
    assert_eq!(fixture.created().len(), 1, "create must run exactly once");
    assert!(fixture.patches().is_empty(), "no object exists to patch");
    assert!(
        !argv
            .iter()
            .any(|call| call.first().map(String::as_str) == Some("helm")),
        "namespace refusal must precede all Helm snapshots: {argv:?}"
    );
}

#[test]
fn dry_run_does_not_acquire_or_mutate_cluster_ownership() {
    let fixture = Fixture::new(None);
    let output = fixture.run_with("healthy", "0.9.0", "charts/curie", &["--dry-run"]);
    assert!(output.status.success(), "{}", visible(&output));
    let argv = fixture.argv();
    assert!(
        !argv.iter().any(|call| is_checkpoint_get(call)),
        "dry run must not enter the ownership protocol: {argv:?}"
    );
    assert!(
        fixture.created().is_empty(),
        "dry run must not create ownership"
    );
    assert!(
        fixture.patches().is_empty(),
        "dry run must not patch ownership"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "dry run must not mutate Helm"
    );
    assert!(
        !argv
            .iter()
            .any(|call| argv_starts(call, &["kubectl", "apply"])),
        "dry run must issue no legacy checkpoint apply: {argv:?}"
    );
}

// T1(a) -- Ruling 2: the local chart's own metadata is the pin. A chart whose
// `helm show chart` version is not `--to` must refuse BEFORE any mutation.
#[test]
fn local_chart_version_mismatch_refuses_before_mutation() {
    let fixture = Fixture::new(None);
    let output = fixture.local("local-chart-mismatch");
    assert!(
        !output.status.success(),
        "a 0.8.7 chart must not satisfy --to 0.9.0: {}",
        stdout(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "refusal must precede mutation: {:?}",
        fixture.argv()
    );
    let message = visible(&output);
    assert!(
        message.contains("0.8.7") && message.contains("0.9.0"),
        "refusal must name the chart's version and the requested version: {message}"
    );
}

// T1(b) -- #2301: a non-local ref IS resolved by Helm, so `--version <to>` must
// reach the command.
#[test]
fn remote_chart_ref_pins_the_target_version() {
    let fixture = Fixture::new(None);
    let output = fixture.run("healthy", "0.9.0", "oci://example.invalid/curie");
    let upgrades = fixture.helm_upgrades();
    assert_eq!(
        upgrades.len(),
        1,
        "one mutation expected: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let position = upgrades[0]
        .iter()
        .position(|arg| arg == "--version")
        .unwrap_or_else(|| panic!("--version absent from {:?}", upgrades[0]));
    assert_eq!(
        upgrades[0].get(position + 1).map(String::as_str),
        Some("0.9.0"),
        "--version must carry the requested target: {:?}",
        upgrades[0]
    );
}

/// #2588 -- remote `helm template` for schema metadata must pin `--version`
/// the same way `helm upgrade` does, or Validate reads the latest chart.
#[test]
fn remote_chart_schema_template_pins_the_target_version() {
    let fixture = Fixture::new(None);
    let output = fixture.run("schema-compatible", "0.9.0", "oci://example.invalid/curie");
    let templates: Vec<_> = fixture
        .argv()
        .into_iter()
        .filter(|call| is_schema_compat_template(call))
        .collect();
    assert!(
        !templates.is_empty(),
        "schema Validate must render target metadata: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let position = templates[0]
        .iter()
        .position(|arg| arg == "--version")
        .unwrap_or_else(|| panic!("--version absent from helm template {:?}", templates[0]));
    assert_eq!(
        templates[0].get(position + 1).map(String::as_str),
        Some("0.9.0"),
        "--version must pin helm template to the requested target: {:?}",
        templates[0]
    );
    let upgrades = fixture.helm_upgrades();
    assert_eq!(
        upgrades.len(),
        1,
        "compatible remote chart must still upgrade: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let position = upgrades[0]
        .iter()
        .position(|arg| arg == "--version")
        .unwrap_or_else(|| panic!("--version absent from {:?}", upgrades[0]));
    assert_eq!(
        upgrades[0].get(position + 1).map(String::as_str),
        Some("0.9.0"),
        "--version must carry the requested target: {:?}",
        upgrades[0]
    );
}

// Helm v3.20 status removes release chart metadata and exposes its numeric
// revision as `version`, while get metadata maps the installed chart version
// to a string. These references pin the external boundary this test exercises:
// https://github.com/helm/helm/blob/v3.20.0/cmd/helm/status.go
// https://github.com/helm/helm/blob/v3.20.0/pkg/action/get_metadata.go
#[test]
fn metadata_version_drives_startup_apply_canary_and_commit() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    assert!(output.status.success(), "{}", visible(&output));
    let result = json(&output);
    assert_eq!(result["status"], "succeeded", "{result}");
    assert_eq!(result["phase"], "commit", "{result}");
    assert_eq!(result["from_version"], "0.8.6", "{result}");
    assert_eq!(result["target_version"], "0.9.0", "{result}");
    assert_eq!(result["known_good_version"], "0.9.0", "{result}");

    let calls = fixture.argv();
    let upgrade = calls
        .iter()
        .position(|call| {
            call.first().map(String::as_str) == Some("helm")
                && call.get(1).map(String::as_str) == Some("upgrade")
        })
        .expect("Apply must invoke helm upgrade");
    let workloads = calls
        .iter()
        .position(|call| {
            call.first().map(String::as_str) == Some("kubectl")
                && call.iter().any(|arg| arg == WORKLOADS)
        })
        .expect("Converge must observe workloads");
    let metadata_reads: Vec<_> = calls
        .iter()
        .enumerate()
        .filter_map(|(index, call)| {
            call.iter()
                .map(String::as_str)
                .eq(["helm", "get", "metadata", "rel", "-n", "ns", "-o", "json"])
                .then_some(index)
        })
        .collect();
    assert_eq!(metadata_reads.len(), 4, "source, Apply, Canary, Commit");
    assert!(metadata_reads[0] < upgrade, "source observation");
    assert!(
        upgrade < metadata_reads[1] && metadata_reads[1] < workloads,
        "Apply observation"
    );
    assert!(workloads < metadata_reads[2], "Canary observation");
    assert!(metadata_reads[2] < metadata_reads[3], "Commit observation");

    assert_unusable_metadata_after_apply("stale-version", "still reports 0.8.6");
    for scenario in [
        "metadata-malformed-after",
        "metadata-missing-after",
        "metadata-numeric-after",
    ] {
        assert_unusable_metadata_after_apply(scenario, "release reports no version");
    }
}

fn assert_unusable_metadata_after_apply(scenario: &str, expected: &str) {
    let fixture = Fixture::new(None);
    let output = fixture.local(scenario);
    assert!(!output.status.success(), "{scenario} must fail");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Apply must run: {scenario}"
    );
    assert!(visible(&output).contains(expected));
    for record in fixture.records() {
        assert_ne!(record["known_good_version"], "0.9.0", "{record}");
        assert!(!record["completed"]
            .as_array()
            .is_some_and(|done| { done.iter().any(|phase| phase == "commit") }));
    }
}

// T2 -- #2301 "never reports success for an unconverged release": the observed
// chart version, not the requested string, is the authority.
//
// The fixture's `helm show chart` reports 0.9.0, so Ruling 2's pre-mutation
// refusal does NOT fire and Apply is genuinely entered -- asserted below,
// because a test that refuses at Validate proves nothing about the
// post-condition read.
#[test]
fn observed_version_divergence_fails_apply() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-version");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Validate must pass and Apply must run, or this proves nothing: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        !output.status.success(),
        "a release still on 0.8.6 must not report success: {}",
        stdout(&output)
    );
    let message = visible(&output);
    assert!(
        message.contains("0.8.6") && message.contains("0.9.0"),
        "the failure must name the requested and the observed version: {message}"
    );
    for record in fixture.records() {
        assert_ne!(
            record["known_good_version"], "0.9.0",
            "known-good must not advance to an unobserved version: {record}"
        );
        assert!(
            !record["completed"]
                .as_array()
                .is_some_and(|done| done.iter().any(|phase| phase == "commit")),
            "Commit must not run once Apply's post-condition read failed: {record}"
        );
    }
}

// T3 -- #2301 "a target-version canary". Apply's own post-condition read is
// satisfied (`helm get metadata` reports 0.9.0 after the upgrade) and
// convergence is exact, THEN the release slips back to 0.8.6 before the canary.
// Only a canary that re-reads the live version can catch that; a canary that
// trusts `self.current` -- which Apply set from `--to` -- passes. This is the
// R4 tautology guard, and it deliberately does not share T2's fixture.
#[test]
fn canary_fails_when_the_observed_version_is_not_the_target() {
    let fixture = Fixture::new(None);
    let output = fixture.local("canary-version-drift");
    let json = json(&output);
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Apply must have succeeded for the canary to be the phase under test: {:?}",
        fixture.argv()
    );
    for facet in FACETS {
        assert_eq!(
            json["convergence"][facet], true,
            "fixture must isolate the canary from convergence: {json}"
        );
    }
    assert_eq!(json["convergence"]["exact"], true, "{json}");
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_ne!(json["known_good_version"], "0.9.0", "{json}");
    let record = fixture.last_record();
    assert!(
        !record["completed"]
            .as_array()
            .is_some_and(|done| done.iter().any(|phase| phase == "commit")),
        "Commit must not run after a failed canary: {record}"
    );
}

// T4 -- #2301 "exact target image convergence".
//
// The release is deployed, `helm get manifest` renders the 0.9.0 images, and
// the live pods still run 0.8.6 at full ready counts with generations
// observed, hooks healthy and selectors matching. Divergence between the
// INSTALLED manifest and the live containers is the only thing `observe` can
// see -- a fixture whose manifest and pods agree reports `images: true` no
// matter how the flag is wired.
//
// `ProgressDeadlineExceeded` is stamped so the observer stops on its first
// pass instead of retrying to its 300-second deadline. It is a Rollout-facet
// issue and must not be the reason any NAMED sub-flag goes false, which is
// exactly what `only_false` pins.
#[test]
fn stale_images_fail_convergence() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-images");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["images"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T5 -- #2301 "healthy hooks". `helm status -o json` reports the pre-upgrade
// Job hook with `last_run.phase: Failed` AND the live Job carries a `Failed`
// condition, so `hooks_healthy` goes false because the hook state was read --
// not because `observe` errored out on a fixture that never served hooks.
//
// The hook that fails here is `rel-schema-migrate`, NOT the drain gate, and
// `only_false` asserts `queues_drained` stays true. Paired with T6 (where the
// drain gate fails and `hooks_healthy` stays true) this proves the two facets
// are genuinely distinct rather than two names for the same hook walk.
#[test]
fn failed_hook_fails_convergence() {
    let fixture = Fixture::new(None);
    let output = fixture.local("failed-hook");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["hooks_healthy"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T6 -- #2301 "drained queues" (guards #2010), per driver Ruling 13.
//
// Ruling 7 derived this from phase bookkeeping and was withdrawn: Drain is
// always executed-or-skipped-and-pushed onto `completed` before Converge runs,
// so that predicate was always true and could not fail. The observable fact is
// the #2010 gate itself -- a Helm PRE-UPGRADE hook Job
// (`charts/curie/templates/worker-upgrade-drain.yaml`) that fires during Apply,
// which is why the Drain phase cannot see its verdict and Converge can.
//
// `queues_drained` binds to that one hook by name; `hooks_healthy` covers every
// other hook. Both branches are reachable, so this test is a fixture and its
// own mutation control.
#[test]
fn failed_drain_hook_is_the_only_source_of_queues_drained() {
    // The gate refused: accepted work was still in flight when the roll began.
    let gate = Fixture::new(None);
    let output = gate.local("failed-drain-hook");
    let refused = json(&output);
    observed_for_real(&gate);
    only_false(&refused, &["queues_drained"]);
    assert_eq!(refused["status"], "failed", "{refused}");
    assert_eq!(refused["phase"], "converge", "{refused}");

    // The control: the same fixture with the drain gate succeeding. One hook
    // phase is the whole difference between these two runs.
    let drained = Fixture::new(None);
    let output = drained.local("healthy");
    let control = json(&output);
    observed_for_real(&drained);
    assert_eq!(
        control["convergence"]["queues_drained"], true,
        "a drain gate that exited 0 is a drained queue: {control}"
    );
    assert_eq!(control["convergence"]["exact"], true, "{control}");
    assert_eq!(control["status"], "succeeded", "{control}");
}

// T6 (cont.) -- the two paths that reach Converge with the Drain PHASE
// legitimately skipped must still report drained queues. This is the
// regression the withdrawn ruling was protecting, and it survives the rebind
// because the hook, not the phase record, is now the source: a fresh install
// runs no drain hook and a same-version rerun applies no revision, so neither
// produces a Drain facet.
#[test]
fn skipped_drain_phases_still_report_drained_queues() {
    // Fresh install: no release yet, so Drain is skipped for having nothing in
    // flight (`from.is_none()`).
    let fresh = Fixture::new(None);
    let output = fresh.local("fresh-install");
    let installed = json(&output);
    assert_eq!(
        installed["convergence"]["queues_drained"], true,
        "a first install has nothing in flight and is not undrained: {installed}"
    );
    assert_eq!(installed["convergence"]["exact"], true, "{installed}");
    assert_eq!(installed["status"], "succeeded", "{installed}");
    assert_eq!(
        fresh.helm_upgrades().len(),
        1,
        "a fresh install still installs: {:?}",
        fresh.argv()
    );

    // Same-version rerun: DrainPreflight/Checkpoint/Migrate/Apply are all skipped.
    let same = Fixture::new(None);
    let output = same.local("resumed-applied");
    let rerun = json(&output);
    assert_eq!(rerun["unchanged"], true, "{rerun}");
    assert_eq!(
        rerun["convergence"]["queues_drained"], true,
        "a same-version rerun skips DrainPreflight but is not undrained: {rerun}"
    );
    assert_eq!(rerun["convergence"]["exact"], true, "{rerun}");
    assert_eq!(rerun["status"], "succeeded", "{rerun}");
    assert!(
        same.helm_upgrades().is_empty(),
        "a same-version rerun must not apply a new revision: {:?}",
        same.argv()
    );

    // A resume past Apply: #2010's exactly-once flag is not a convergence fact
    // either way, so neither setting of it may move the drained verdict.
    for drain_completed in [false, true] {
        let fixture = Fixture::new(None).checkpoint(&applied_checkpoint(drain_completed));
        let output = fixture.local("resumed-applied");
        let resumed = json(&output);
        assert_eq!(
            resumed["convergence"]["queues_drained"], true,
            "drain_completed={drain_completed} must not decide the drained facet: {resumed}"
        );
        assert_eq!(resumed["status"], "succeeded", "{resumed}");
    }
}

// T20 -- #2639: live DrainPreflight must return the worker Deployment probe,
// not a hardcoded success. A non-NotFound probe failure that stays pending
// through the phase budget fails the preflight, keeps the previous version
// serving, and never issues helm upgrade.
//
// #2830: the phase's own name and its refusal text must not claim it observed
// a drain -- it only confirmed the worker Deployment was unreachable. The
// real #2010 drain gate is the chart's pre-upgrade Helm hook Job, not this
// probe.
#[test]
fn undrained_deploy_fails_drain_preflight_before_mutation() {
    let fixture = Fixture::new(None);
    let output = fixture.local_env(
        "undrained-deploy",
        &[("CURIE_UPGRADE_DRAIN_TIMEOUT_SECS", "0")],
    );
    let json = json(&output);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "drain_preflight", "{json}");
    assert_eq!(json["previous_serving"], true, "{json}");
    assert!(
        fixture.helm_upgrades().is_empty(),
        "DrainPreflight refusal must precede mutation: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.issued(&["kubectl", "get", "deploy", "rel-worker"]),
        "DrainPreflight must probe the worker Deployment: {:?}",
        fixture.argv()
    );
    let reason = json["fail_forward"]["reason"]
        .as_str()
        .unwrap_or_else(|| panic!("no fail_forward reason: {json}"));
    assert!(
        reason.contains("reachable"),
        "fail-forward must describe the unreachable worker probe: {reason}"
    );
    assert!(
        !reason.contains("in flight"),
        "fail-forward must not claim it observed in-flight delivery work it never watched: {reason}"
    );
}

// T7 -- #2301 "compare Helm's retained target manifest with live owned objects
// before committing": a live selector that differs from the target manifest.
// The images agree here, so `manifest_matches` is the only facet that may go
// false -- a binding that maps every issue onto every flag fails this.
#[test]
fn manifest_mismatch_fails_convergence_on_the_live_path() {
    let fixture = Fixture::new(None);
    let output = fixture.local("selector-drift");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["manifest_matches"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T8(a) -- #2301 "persist phase/checkpoint state sufficient to resume": the
// FIRST checkpoint write, before any mutation, must fail the command rather
// than being discarded by `let _ = persist_record(..)`.
#[test]
fn checkpoint_persist_failure_fails_the_command() {
    let fixture = Fixture::new(None);
    let output = fixture.local("persist-fails");
    assert!(
        !output.status.success(),
        "a lost checkpoint must not report success: {}",
        stdout(&output)
    );
    assert!(
        visible(&output).contains("checkpoint"),
        "the persist failure must surface: {}",
        visible(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "a checkpoint lost before Apply must stop short of mutation: {:?}",
        fixture.argv()
    );
}

// T8(b) -- plan edge 10. Only the checkpoint write that FOLLOWS a failed
// Converge fails. Every earlier write succeeds, so the run reaches Converge,
// fails it on stale images, and then loses the failure checkpoint.
//
// The convergence failure is the verdict the operator must act on. A persist
// error raised with `?` from the `PhaseOutcome::Failed` arm would replace it
// with a checkpoint message and drop the `ClusterUpgradeOutput` entirely --
// no phase, no convergence payload, no fail_forward. This asserts the phase
// failure is still the structured verdict and the persist error travels
// beside it.
#[test]
fn persist_failure_after_converge_does_not_mask_the_phase_failure() {
    let fixture = Fixture::new(None);
    let output = fixture.local("converge-then-persist-fails");
    let json = json(&output);
    assert_eq!(
        json["status"], "failed",
        "the phase failure is the verdict: {json}"
    );
    assert_eq!(
        json["phase"], "converge",
        "the persist error must not rename the failing phase: {json}"
    );
    assert_eq!(
        json["convergence"]["images"], false,
        "the convergence payload must survive the lost checkpoint: {json}"
    );
    assert!(
        json["fail_forward"].is_object(),
        "the operator still needs one bounded recovery path: {json}"
    );
    assert!(
        visible(&output).contains("checkpoint"),
        "the persist failure must travel alongside the phase failure: {}",
        visible(&output)
    );
}

// T9 -- #2299 "reject ambiguous conflicts before cluster mutation". The two
// cases differ ONLY in the extraEnv value: 999 contradicts the first-class
// `worker.runnerTotalTimeoutSeconds: 120` and is unresolvable; 120 agrees with
// it and migrates cleanly. Without the second case this test would prove
// nothing beyond "Validate always refuses".
#[test]
fn ambiguous_config_conflict_refuses_before_mutation() {
    let conflicting = Fixture::new(Some(&conflict_values("999").to_string()));
    let output = conflicting.local("healthy");
    assert!(
        !output.status.success(),
        "an ambiguous conflict must refuse: {}",
        stdout(&output)
    );
    assert!(
        conflicting.helm_upgrades().is_empty(),
        "refusal must precede mutation: {:?}",
        conflicting.argv()
    );

    // The control: same shape, no contradiction.
    let agreeing = Fixture::new(Some(&conflict_values("120").to_string()));
    let output = agreeing.local("healthy");
    assert_eq!(
        agreeing.helm_upgrades().len(),
        1,
        "a legacy extraEnv that AGREES with its successor must still upgrade: {:?} / {}",
        agreeing.argv(),
        stderr(&output)
    );
    let values = agreeing.values(1);
    assert!(
        !values.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S"),
        "the redundant extraEnv entry must be dropped, not carried forward: {values}"
    );
}

fn conflict_values(extra_env_value: &str) -> Value {
    serde_json::json!({
        "config": {"schemaVersion": "0.8.4"},
        "worker": {
            "runnerTotalTimeoutSeconds": 120,
            "extraEnv": [{"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": extra_env_value}]
        }
    })
}

// T10 -- #2299 "migrate legacy extraEnv entries" and "merge new target defaults
// without dropping prior operator overrides": the values file helm is HANDED is
// the evidence, not an in-process return value.
#[test]
fn retained_values_are_migrated_before_helm_sees_them() {
    let fixture = Fixture::new(Some(V084));
    let output = fixture.local("healthy");
    assert!(
        !fixture.helm_upgrades().is_empty(),
        "nothing was applied: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let values = values_doc(&fixture.values(1));
    assert_eq!(
        values.pointer("/worker/runnerTotalTimeoutSeconds"),
        Some(&serde_json::json!(120)),
        "legacy extraEnv must become the first-class key, carrying the operator's own value: {values}"
    );
    assert!(
        !extra_env_names(&values).contains(&"CURIE_RUNNER_TOTAL_TIMEOUT_S".to_string()),
        "the promoted entry must be gone from every extraEnv list: {values}"
    );
    assert!(
        extra_env_names(&values).contains(&"PROVIDER_BASE_URL".to_string()),
        "an unrelated operator override must survive the merge: {values}"
    );
    assert!(
        values.pointer("/config/schemaVersion").is_some(),
        "the migrated overlay must persist the configuration schema version: {values}"
    );
}

// T11 -- #2299 "preserve external Secret names byte-for-byte" / "never restore
// an inline value".
#[test]
fn external_secret_references_survive_byte_for_byte() {
    let fixture = Fixture::new(Some(V084));
    fixture.local("healthy");
    let values = values_doc(&fixture.values(1));
    assert_eq!(
        values.pointer("/dispatcher/slack/botTokenExistingSecret"),
        Some(&Value::String("acme-slack".into())),
        "the external Secret name must survive byte-for-byte: {values}"
    );
    assert_eq!(
        values.pointer("/dispatcher/slack/botTokenExistingSecretKey"),
        Some(&Value::String("botToken".into())),
        "the external Secret key must survive byte-for-byte: {values}"
    );
    assert!(
        !fixture.values(1).contains(SLACK_TOKEN),
        "an inline credential must never be restored beside its Secret ref: {values}"
    );
}

// T12 -- #2299 "plan/apply idempotent", as a RESUME rather than two runs of the
// same input (plan edge 11).
//
// The first run migrates the retained v0.8.5 overlay and hands helm the result.
// The recording helm then RETAINS that result, so the second run's
// `helm get values` returns the first run's own migrated `-f` payload -- and
// the checkpoint seeded between the runs stops Apply from being skipped as a
// same-version no-op. Re-migrating already migrated output must change nothing.
// Two runs against an unchanged fixture would go green even if migration were
// not idempotent at all, because both would migrate the same original input.
#[test]
fn resumed_upgrade_remigrates_its_own_output_without_change() {
    let fixture = Fixture::new(Some(V085));
    let first = fixture.local("healthy");
    let once = fixture.values(1);
    assert!(
        values_doc(&once)
            .pointer("/worker/runnerTotalTimeoutSeconds")
            .is_some(),
        "the first run must actually have migrated something: {once} / {}",
        stderr(&first)
    );

    // Resume with Apply still outstanding, so the second run re-reads and
    // re-migrates the retained (already migrated) overlay. This deliberately
    // seeds the pre-#2830 `"drain"` phase name (rather than the current
    // `"drain_preflight"`) so a full resume through the real binary also pins
    // that a checkpoint an older binary wrote still resumes correctly.
    fixture.seed_checkpoint(&checkpoint_through(
        &["plan", "validate", "drain", "checkpoint", "migrate"],
        true,
    ));
    let second = fixture.local("healthy");
    assert_eq!(
        fixture.helm_upgrades().len(),
        2,
        "the resume must reach Apply again: {:?} / {}",
        fixture.argv(),
        stderr(&second)
    );
    let twice = fixture.values(2);
    assert_eq!(
        once, twice,
        "re-migrating an already migrated document must change nothing"
    );
}

// T13 -- #2299/#2300 redaction. Ruling 10: `--dry-run` exposing
// `config.schemaVersion` on the plan is DEFERRED out of this PR, so this test
// asserts only credential absence. The overlay's persisted schema version is
// pinned by T10, where the migration output is the subject.
#[test]
fn upgrade_output_contains_no_credential_value() {
    let fixture = Fixture::new(Some(V084));
    let output = fixture.local("healthy");
    let reachable = format!("{}{}", visible(&output), fixture.persisted_payloads());
    for secret in [SLACK_TOKEN, "sk-ant-test-must-not-leak"] {
        assert!(
            !reachable.contains(secret),
            "credential value reached output or the persisted record: {reachable}"
        );
    }
}

// T14 -- pins `generations` to an observed controller generation. The live
// Deployment's `status.observedGeneration` lags `metadata.generation`; images,
// replicas, hooks and selectors all agree, so a hardcoded `generations: true`
// (or a `generations` bound to any other facet) fails here.
#[test]
fn stale_generation_fails_only_the_generations_facet() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-generation");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["generations"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T15 -- pins `replicas`, and pins that it is NOT `unavailable_zero`.
// `updatedReplicas` lags desired while `unavailableReplicas` is genuinely 0,
// so `replicas` must be false and `unavailable_zero` must stay true. A
// hardcoded `replicas: true`, or the old `unavailable_zero: replicas` alias,
// fails on one half or the other.
#[test]
fn replica_count_mismatch_fails_only_the_replicas_facet() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-replicas");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["replicas"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T16 -- the mirror of T15 and the reason `unavailable_zero` is its own facet:
// updated == ready == total == desired, but one replica is unavailable.
// `replicas` stays true and `unavailable_zero` alone goes false, so neither
// flag can be a literal or an alias of the other.
#[test]
fn unavailable_replicas_fail_only_the_unavailable_facet() {
    let fixture = Fixture::new(None);
    let output = fixture.local("unavailable-replicas");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["unavailable_zero"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T17 -- an observation that could not be MADE determines NO named property.
// The workloads read exits non-zero (a command failure, not a terminal
// cluster condition), so every named sub-flag must read false -- `holds` is
// closed-world, and leaving the facets untagged would report seven observed
// truths off a read that never happened. The read's own error text, not the
// generic converge message, must reach `fail_forward.reason`.
#[test]
fn an_unmakeable_observation_determines_no_facet() {
    let fixture = Fixture::new(None);
    let output = fixture.local("workloads-unreadable");
    let json = json(&output);
    only_false(&json, &FACETS);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
    let reason = json["fail_forward"]["reason"]
        .as_str()
        .unwrap_or_else(|| panic!("no fail_forward reason: {json}"));
    assert!(
        reason.contains("read target workloads and pods"),
        "the failed read must be named, not replaced by the generic converge message: {reason}"
    );
    assert!(
        reason.contains("inspect Helm/Kubernetes access and retry"),
        "the observer's recovery hint must survive into fail_forward: {reason}"
    );
}

// T18 -- F4: `helm get values` failing with stderr that merely CONTAINS
// "not found" (here a missing namespace) leaves the retained overlay unknown,
// not absent. Proceeding would run `helm upgrade` with no `-f` and silently
// drop every retained operator value, so this must fail closed before any
// mutation.
#[test]
fn an_unreadable_retained_overlay_fails_closed() {
    let fixture = Fixture::new(None);
    let output = fixture.local("values-read-fails");
    assert!(
        !output.status.success(),
        "an unknown retained overlay must not report success: {}",
        stdout(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "nothing may be mutated while the retained values are unknown: {:?}",
        fixture.argv()
    );
    assert!(
        visible(&output).contains("retained helm values"),
        "the refusal must name the unreadable retained values: {}",
        visible(&output)
    );
}

// T19 -- R7: the overlay is read ONCE, before the lifecycle, and Apply hands
// helm that stored document. The fake serves a DIFFERENT values document on
// every `helm get values` after the first, so a re-read inside Apply would
// visibly swap the payload. Both halves are asserted: the `-f` document is the
// one read at Validate, and `helm get values` appears exactly once in the argv
// log.
#[test]
fn the_overlay_is_read_once_and_apply_uses_the_stored_one() {
    let fixture = Fixture::new(None);
    let output = fixture.local("values-drift");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Apply must run for this to prove anything: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let values = fixture.values(1);
    let names = extra_env_names(&values_doc(&values));
    assert!(
        names.contains(&"FIRST_READ_ONLY".to_string()),
        "helm must be handed the overlay read at Validate: {values}"
    );
    assert!(
        !names.contains(&"SECOND_READ_MUST_NOT_WIN".to_string()),
        "a second `helm get values` must not be the source of the -f payload: {values}"
    );
    let reads = fixture
        .argv()
        .into_iter()
        .filter(|call| call.len() >= 3 && call[..3] == ["helm", "get", "values"])
        .count();
    assert_eq!(
        reads,
        1,
        "the retained overlay must be read exactly once: {:?}",
        fixture.argv()
    );
}

/// #2301 -- `--dry-run` must plan the refusal the real run would hit. Both
/// pre-mutation inputs are read-only, so a dry run computes them; a clean
/// nine-phase plan for an upgrade that cannot happen is the defect.
#[test]
fn dry_run_plans_the_chart_refusal_without_mutating() {
    let fixture = Fixture::new(None);
    let output = fixture.run_with(
        "local-chart-mismatch",
        "0.9.0",
        "charts/curie",
        &["--dry-run"],
    );
    let plan = json(&output)["plan"].to_string();
    assert!(
        plan.contains("refusal") && plan.contains("0.8.7") && plan.contains("0.9.0"),
        "the dry-run plan must surface the Validate refusal: {plan}"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "a dry run must mutate nothing: {:?}",
        fixture.argv()
    );
}

/// #2299 -- the redacted plan must carry the configuration schema version it
/// migrates from and to, and must never carry a credential value.
#[test]
fn dry_run_plan_carries_the_config_schema_version() {
    let fixture = Fixture::new(Some(V084));
    let output = fixture.run_with("healthy", "0.9.0", "charts/curie", &["--dry-run"]);
    let plan = json(&output)["plan"].to_string();
    assert!(
        plan.contains("config schema: 0.8.6 -> 0.9.0"),
        "the plan must name the source and target configuration schema: {plan}"
    );
    assert!(
        !plan.contains(SLACK_TOKEN),
        "the plan must stay redacted: {plan}"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "a dry run must mutate nothing: {:?}",
        fixture.argv()
    );
}

/// #2301 -- the printed plan's helm line must BE the command. Compares the
/// dry-run plan's helm line against the argv the mutating run records, for a
/// local chart (no `--version`) and a resolvable ref (`--version` in both).
#[test]
fn dry_run_helm_line_matches_the_recorded_upgrade_argv() {
    for chart in ["charts/curie", "oci://example.invalid/curie"] {
        let planner = Fixture::new(None);
        let planned = json(&planner.run_with("healthy", "0.9.0", chart, &["--dry-run"]));
        let line = planned["plan"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .find(|line| line.starts_with("helm upgrade "))
            .unwrap_or_else(|| panic!("no helm line in {planned}"))
            .to_string();

        let mutator = Fixture::new(None);
        let output = mutator.run("healthy", "0.9.0", chart);
        let upgrades = mutator.helm_upgrades();
        assert_eq!(
            upgrades.len(),
            1,
            "{:?} / {}",
            mutator.argv(),
            stderr(&output)
        );
        // #2863: the planned line is the whole command. Only the `-f`
        // tempfile path differs, and the plan names it by placeholder.
        let mut executed = upgrades[0].clone();
        if let Some(at) = executed.iter().position(|arg| arg == "-f") {
            executed[at + 1] = "<retained-values>".into();
        }
        assert_eq!(
            line,
            executed.join(" "),
            "planned line must be the executed command: {:?}",
            upgrades[0]
        );
        let pinned = line.contains("--version 0.9.0");
        assert_eq!(
            pinned,
            chart.starts_with("oci://"),
            "--version must show exactly when passed: {line}"
        );
    }
}

fn is_alembic_current(call: &[String]) -> bool {
    call.first().map(String::as_str) == Some("kubectl")
        && call.iter().any(|arg| arg == "exec")
        && call.iter().any(|arg| arg == "alembic")
        && call.iter().any(|arg| arg == "current")
}

fn is_schema_compat_template(call: &[String]) -> bool {
    if call.first().map(String::as_str) != Some("helm")
        || call.get(1).map(String::as_str) != Some("template")
    {
        return false;
    }
    call.windows(2)
        .any(|pair| pair[0] == "--show-only" && pair[1] == "templates/schema-compat.yaml")
        || call
            .iter()
            .any(|arg| arg == "--show-only=templates/schema-compat.yaml")
}

fn is_schema_compat_configmap_get(call: &[String]) -> bool {
    call.len() >= 3
        && call[0] == "kubectl"
        && call[1] == "get"
        && call[2] == "configmap"
        && call.iter().any(|arg| arg.contains("schema-compat"))
}

fn overlay_sets_forward_only(fixture: &Fixture) -> bool {
    let upgrades = fixture.helm_upgrades();
    if upgrades.is_empty() {
        return false;
    }
    let argv = upgrades[0].join(" ");
    if argv.contains("forwardOnly") || argv.contains("forward-only") {
        return true;
    }
    fs::read_to_string(fixture.0.path().join("values-1.yaml"))
        .map(|text| text.contains("forwardOnly") || text.contains("forward-only"))
        .unwrap_or(false)
}

/// #2588 -- an unknown live revision refuses at Validate with zero `helm
/// upgrade`. Current LiveHost ignores schema, so this fails until the bind.
#[test]
fn incompatible_live_revision_refuses_before_helm_upgrade() {
    let fixture = Fixture::new(None);
    let output = fixture.local("schema-incompatible");
    assert!(
        !output.status.success(),
        "live revision 0099 must refuse the target schema: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "schema refusal must precede mutation: {:?}",
        fixture.argv()
    );
    let message = visible(&output).to_lowercase();
    assert!(
        message.contains("compatibility")
            || message.contains("schema")
            || message.contains("revision"),
        "refusal must name compatibility/schema/revision: {}",
        visible(&output)
    );
}

/// #2588 -- pending contract 0041 refuses and names `--forward-only`.
#[test]
fn pending_contract_refuses_and_names_forward_only() {
    let fixture = Fixture::new(None);
    let output = fixture.local("schema-contract");
    assert!(
        !output.status.success(),
        "pending contract 0041 must refuse without --forward-only: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "contract refusal must precede mutation: {:?}",
        fixture.argv()
    );
    assert!(
        visible(&output).contains("--forward-only"),
        "refusal must name --forward-only: {}",
        visible(&output)
    );
}

/// #2588 -- a resume that already completed Validate still refuses a freshly
/// computed contract migration and must not skip to Apply.
#[test]
fn resume_after_validate_still_refuses_fresh_schema_contract() {
    let fixture = Fixture::new(None).checkpoint(&checkpoint_through(&["plan", "validate"], true));
    let output = fixture.local("schema-contract");
    assert!(
        !output.status.success(),
        "resume without --forward-only must refuse a pending contract: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "fresh schema refusal must precede mutation: {:?}",
        fixture.argv()
    );
    assert!(
        visible(&output).contains("--forward-only"),
        "refusal must name --forward-only: {}",
        visible(&output)
    );
}

/// #2588 -- the same pending contract proceeds once `--forward-only` is set.
/// Clap does not accept the flag on `cluster upgrade` yet, so this fails today.
#[test]
fn forward_only_allows_pending_contract_to_reach_helm_upgrade() {
    let fixture = Fixture::new(None);
    let output = fixture.run_with(
        "schema-contract",
        "0.9.0",
        "charts/curie",
        &["--forward-only"],
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "--forward-only must reach helm upgrade: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        overlay_sets_forward_only(&fixture),
        "Apply overlay must carry api.migrate.forwardOnly: {:?} / {}",
        fixture.helm_upgrades(),
        fs::read_to_string(fixture.0.path().join("values-1.yaml")).unwrap_or_default()
    );
}

/// #2588 -- already at head is the positive control: helm upgrade without the
/// flag. LiveHost currently ignores schema, so this may already pass.
#[test]
fn compatible_already_at_head_reaches_helm_upgrade_without_forward_only() {
    let fixture = Fixture::new(None);
    let output = fixture.local("schema-compatible");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "compatible already-at-head must still upgrade the chart: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        !fixture
            .argv()
            .iter()
            .any(|call| call.iter().any(|arg| arg == "--forward-only")),
        "the compatible path must not pass --forward-only: {:?}",
        fixture.argv()
    );
}

/// #2588 -- a v0.8.x install has no schema-compat ConfigMap. Source window
/// comes from the catalog; target window from `helm template --show-only`.
#[test]
fn v08x_source_window_comes_from_catalog_not_configmap() {
    let fixture = Fixture::new(None);
    let output = fixture.local("schema-compatible");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "compatible v0.8.6 source must still upgrade: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        fixture.argv().iter().any(|call| is_alembic_current(call)),
        "live revision must be read with kubectl exec alembic current: {:?}",
        fixture.argv()
    );
    assert!(
        fixture
            .argv()
            .iter()
            .any(|call| is_schema_compat_template(call)),
        "target window must come from helm template --show-only templates/schema-compat.yaml: {:?}",
        fixture.argv()
    );
    assert!(
        !fixture
            .argv()
            .iter()
            .any(|call| is_schema_compat_configmap_get(call)),
        "v0.8.x source window must not be read from a schema-compat ConfigMap: {:?}",
        fixture.argv()
    );
}

/// #2588 -- an existing release whose alembic probe fails is not an empty-DB
/// install. Current LiveHost never execs, so the upgrade still succeeds today.
#[test]
fn unreadable_live_revision_on_existing_release_refuses_empty_db_shortcut() {
    let fixture = Fixture::new(None);
    let output = fixture.local("schema-probe-fails");
    assert!(
        !output.status.success(),
        "a failed alembic probe on an existing release must refuse: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "an unreadable live revision must not mutate: {:?}",
        fixture.argv()
    );
}

/// #2590 -- LiveHost env hooks refuse soak identities before helm mutates.
#[test]
fn fail_at_apply_against_soak_namespace_is_refused_without_helm() {
    let fixture = Fixture::new(None);
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    let output = command
        .args([
            "--json",
            "cluster",
            "upgrade",
            "--to",
            "0.9.0",
            "--namespace",
            "curie",
            "--release",
            "t2590",
            "--chart",
            "charts/curie",
            "--yes",
        ])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap(),
        )
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", fixture.0.path().display()),
        )
        .env("UPGRADE_DRIVER_ROOT", fixture.0.path())
        .env("UPGRADE_DRIVER_SCENARIO", "happy")
        .env("CURIE_UPGRADE_TEST_FAIL_AT", "apply")
        .output()
        .expect("run soak FAIL_AT");
    assert!(
        !output.status.success(),
        "soak FAIL_AT must exit nonzero: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let text = visible(&output);
    assert!(
        text.contains("soak") && text.contains("CURIE_UPGRADE_TEST_FAIL_AT"),
        "refusal must name the soak hook: {text}"
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "soak FAIL_AT must not call helm upgrade: {:?}",
        fixture.argv()
    );
}

#[test]
fn fail_at_plan_on_owned_namespace_fails_before_helm() {
    let fixture = Fixture::new(None);
    let output = fixture.run_with_env(
        "healthy",
        "0.9.0",
        "charts/curie",
        &[],
        &[("CURIE_UPGRADE_TEST_FAIL_AT", "plan")],
    );
    let payload = json(&output);
    assert_eq!(
        payload["status"],
        "failed",
        "owned FAIL_AT=plan must be structured failed: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    assert_eq!(payload["phase"], "plan", "{payload}");
    assert!(
        fixture.helm_upgrades().is_empty(),
        "owned FAIL_AT=plan must not call helm upgrade: {:?}",
        fixture.argv()
    );
}

// T#2741 -- boolean-shaped string scalars must survive the retained-values
// round trip.
//
// Mechanism: Helm parses values with Go's go-yaml, which is YAML **1.1**, where
// the bare tokens `off`/`on`/`yes`/`no`/`y`/`n`/`true`/`false` are booleans.
// `serde_norway` emits YAML **1.2**, where only `true`/`false` are, so it writes
// the JSON string "off" as the bare scalar `off`. Helm then reads a BOOLEAN
// false. An install carrying `security.gvisor.mode: "off"` silently flips on
// upgrade and the absent `gvisor` RuntimeClass becomes required.
//
// The assertion therefore has to be made on the emitted BYTES with YAML 1.1
// eyes: re-parsing with `serde_norway` (YAML 1.2) would hand the string back and
// prove nothing.

/// The YAML 1.1 boolean tokens that YAML 1.2 does NOT share. `true`/`false` are
/// deliberately absent: both versions read them as booleans, so a bare `true` is
/// a correct emission for a genuine boolean, not a leak. Helm's go-yaml also
/// accepts the capitalised and all-caps spellings, which `to_lowercase` folds in.
const YAML11_ONLY_BOOLEAN_WORDS: [&str; 6] = ["y", "yes", "n", "no", "on", "off"];

/// The retained overlay used by every #2741 test: one boolean-shaped string at
/// each of three nesting depths, so a fix that only special-cases the known
/// gvisor key does not pass.
fn boolean_shaped_retained() -> String {
    serde_json::json!({
        "config": {"schemaVersion": "0.8.4"},
        "security": {"gvisor": {"mode": "off"}},
        "api": {"logStructured": "yes"},
        "uiFlag": "n",
    })
    .to_string()
}

/// Lines of a block-style YAML document whose scalar value is an UNQUOTED YAML
/// 1.1 boolean token. Non-empty means Helm would read a boolean where the
/// operator wrote a string.
///
/// Block style only, which is what `serde_norway::to_string` emits; a flow-style
/// emitter would need this widened rather than weakened.
fn unquoted_yaml11_booleans(raw: &str) -> Vec<String> {
    raw.lines()
        .filter(|line| {
            let Some((key, value)) = line.split_once(": ") else {
                return false;
            };
            if key.trim_start().starts_with('#') {
                return false;
            }
            let value = value.trim();
            YAML11_ONLY_BOOLEAN_WORDS.contains(&value.to_lowercase().as_str())
        })
        .map(|line| line.trim().to_string())
        .collect()
}

/// #2741 -- the ordinary retained-values path. `retained_overlay()` re-serializes
/// with `serde_norway::to_string` (YAML 1.2), so the strings "off", "yes" and "n"
/// reach Helm's YAML 1.1 parser as bare booleans.
#[test]
fn boolean_shaped_retained_strings_stay_strings_in_the_apply_overlay() {
    let fixture = Fixture::new(Some(&boolean_shaped_retained()));
    let output = fixture.local("healthy");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "nothing was applied: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let raw = fixture.values(1);
    assert!(
        unquoted_yaml11_booleans(&raw).is_empty(),
        "Helm parses YAML 1.1, so these bare scalars become booleans: {:?}\n{raw}",
        unquoted_yaml11_booleans(&raw)
    );
    let values = values_doc(&raw);
    for (pointer, expected) in [
        ("/security/gvisor/mode", "off"),
        ("/api/logStructured", "yes"),
        ("/uiFlag", "n"),
    ] {
        assert_eq!(
            values.pointer(pointer),
            Some(&Value::String(expected.into())),
            "{pointer} must still be the string {expected:?}: {raw}"
        );
    }
}

/// #2741 -- the `--forward-only` path re-serializes through a SECOND
/// `serde_norway::to_string` site (`merge_forward_only()`), so it needs its own
/// coverage. The paired assertion is the control: a genuine boolean
/// (`api.migrate.forwardOnly`) must stay a boolean, so the fix cannot be
/// "quote everything".
#[test]
fn forward_only_overlay_preserves_boolean_shaped_strings_and_real_booleans() {
    let fixture = Fixture::new(Some(&boolean_shaped_retained()));
    let output = fixture.run_with(
        "schema-contract",
        "0.9.0",
        "charts/curie",
        &["--forward-only"],
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "--forward-only must reach helm upgrade: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let raw = fixture.values(1);
    assert!(
        unquoted_yaml11_booleans(&raw).is_empty(),
        "forward-only overlay leaks YAML 1.1 booleans: {:?}\n{raw}",
        unquoted_yaml11_booleans(&raw)
    );
    let values = values_doc(&raw);
    assert_eq!(
        values.pointer("/security/gvisor/mode"),
        Some(&Value::String("off".into())),
        "the forward-only path must keep the string \"off\": {raw}"
    );
    assert_eq!(
        values.pointer("/api/migrate/forwardOnly"),
        Some(&Value::Bool(true)),
        "a real boolean must stay a boolean, not be stringified: {raw}"
    );
}

/// #2741 -- the negative control. Quoting is not allowed to be blanket: genuine
/// booleans, integers and nulls in the retained values must reach Helm with
/// their own YAML 1.2/1.1-agreeing types.
#[test]
fn genuine_scalar_types_are_not_coerced_to_strings() {
    let retained = serde_json::json!({
        "config": {"schemaVersion": "0.8.4"},
        "ui": {"deploy": false},
        "worker": {"runnerTotalTimeoutSeconds": 120, "replicas": 3},
        "api": {"nodeSelector": null},
    })
    .to_string();
    let fixture = Fixture::new(Some(&retained));
    let output = fixture.local("healthy");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "nothing was applied: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let raw = fixture.values(1);
    let values = values_doc(&raw);
    assert_eq!(
        values.pointer("/ui/deploy"),
        Some(&Value::Bool(false)),
        "a genuine boolean must not become a string: {raw}"
    );
    assert_eq!(
        values.pointer("/worker/runnerTotalTimeoutSeconds"),
        Some(&serde_json::json!(120)),
        "a genuine integer must not become a string: {raw}"
    );
    assert_eq!(
        values.pointer("/worker/replicas"),
        Some(&serde_json::json!(3)),
        "a genuine integer must not become a string: {raw}"
    );
    assert_eq!(
        values.pointer("/api/nodeSelector"),
        Some(&Value::Null),
        "a genuine null must not become a string: {raw}"
    );
}

fn retained_dotted_maps_and_scalars() -> String {
    serde_json::json!({
        "config": {"schemaVersion": "0.8.4"},
        "security": {
            "otelCollectorNetworkPolicy": {
                "metricsIngress": [{
                    "namespaceSelector": {
                        "matchLabels": {
                            "kubernetes.io/metadata.name": "observability"
                        }
                    },
                    "podSelector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": "prometheus"
                        }
                    }
                }]
            }
        },
        "independentLabels": {
            "app.kubernetes.io/name": "retained",
            "example.com/tier": "metrics"
        },
        "ordinary": {
            "mode": "off",
            "affirmative": "yes",
            "short": "n",
            "numeric": "00123",
            "enabled": true,
            "replicas": 3
        }
    })
    .to_string()
}

fn assert_retained_dotted_maps_and_scalars(values: &Value) {
    let expected = serde_json::json!({
        "metricsIngress": [{
            "namespaceSelector": {
                "matchLabels": {
                    "kubernetes.io/metadata.name": "observability"
                }
            },
            "podSelector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "prometheus"
                }
            }
        }],
        "independentLabels": {
            "app.kubernetes.io/name": "retained",
            "example.com/tier": "metrics"
        },
        "ordinary": {
            "mode": "off",
            "affirmative": "yes",
            "short": "n",
            "numeric": "00123",
            "enabled": true,
            "replicas": 3
        }
    });
    assert_eq!(
        values.pointer("/security/otelCollectorNetworkPolicy/metricsIngress"),
        expected.pointer("/metricsIngress"),
        "nested array label maps changed: {values}"
    );
    assert_eq!(
        values.pointer("/independentLabels"),
        expected.pointer("/independentLabels"),
        "independent dotted label map changed: {values}"
    );
    assert_eq!(
        values.pointer("/ordinary"),
        expected.pointer("/ordinary"),
        "ordinary scalar types changed: {values}"
    );
}

#[test]
fn retained_dotted_maps_and_scalars_stay_exact_in_normal_upgrade_json() {
    let fixture = Fixture::new(Some(&retained_dotted_maps_and_scalars()));
    let output = fixture.local("healthy");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "normal upgrade did not reach Helm: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let raw = fixture.values(1);
    let values: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("normal retained overlay is not JSON ({error}): {raw}"));
    assert_retained_dotted_maps_and_scalars(&values);
}

#[test]
fn retained_dotted_maps_and_scalars_stay_exact_in_forward_only_upgrade_json() {
    let fixture = Fixture::new(Some(&retained_dotted_maps_and_scalars()));
    let output = fixture.run_with(
        "schema-contract",
        "0.9.0",
        "charts/curie",
        &["--forward-only"],
    );
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "forward only upgrade did not reach Helm: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let raw = fixture.values(1);
    let values: Value = serde_json::from_str(&raw).unwrap_or_else(|error| {
        panic!("forward only retained overlay is not JSON ({error}): {raw}")
    });
    assert_retained_dotted_maps_and_scalars(&values);
    assert_eq!(
        values.pointer("/api/migrate/forwardOnly"),
        Some(&Value::Bool(true)),
        "forward only control must stay a boolean: {values}"
    );
}

/// Every call that writes: Helm apply or rollback, and any kubectl create,
/// patch, apply, replace or delete.
fn mutating_calls(fixture: &Fixture) -> Vec<Vec<String>> {
    fixture
        .argv()
        .into_iter()
        .filter(|call| {
            match (
                call.first().map(String::as_str),
                call.get(1).map(String::as_str),
            ) {
                (Some("helm"), Some(verb)) => {
                    matches!(verb, "upgrade" | "install" | "rollback" | "uninstall")
                }
                (Some("kubectl"), Some(verb)) => {
                    matches!(verb, "create" | "patch" | "apply" | "replace" | "delete")
                }
                _ => false,
            }
        })
        .collect()
}

/// #2862: a dry run that ends in a Validate refusal exits with the same
/// nonzero class as the identical real run, and issues no mutating call.
/// Covers the chart identity refusal and the schema graph refusal.
#[test]
fn dry_run_refusal_exits_like_the_real_run_without_mutating() {
    for (scenario, needle) in [
        ("local-chart-mismatch", "declares version"),
        ("schema-incompatible", "refusal at validate"),
    ] {
        let dry = Fixture::new(None);
        let output = dry.run_with(scenario, "0.9.0", "charts/curie", &["--dry-run"]);
        assert!(
            !output.status.success(),
            "{scenario}: a refusing dry run must exit nonzero: {}",
            visible(&output)
        );
        assert!(
            visible(&output).contains(needle),
            "{scenario}: the refusal must stay visible: {}",
            visible(&output)
        );
        assert!(
            mutating_calls(&dry).is_empty(),
            "{scenario}: a dry run must not mutate: {:?}",
            dry.argv()
        );

        let real = Fixture::new(None);
        let real_output = real.run(scenario, "0.9.0", "charts/curie");
        assert!(
            !real_output.status.success(),
            "{scenario}: real run refuses"
        );
        assert_eq!(
            output.status.code(),
            real_output.status.code(),
            "{scenario}: dry run and real run must share an exit class"
        );
        assert!(real.helm_upgrades().is_empty(), "{scenario}");
    }
}

/// #2862 negative control: a plan with no refusal still succeeds and still
/// mutates nothing.
#[test]
fn valid_dry_run_succeeds_without_mutating() {
    let fixture = Fixture::new(None);
    let output = fixture.run_with("schema-compatible", "0.9.0", "charts/curie", &["--dry-run"]);
    assert!(output.status.success(), "{}", visible(&output));
    assert!(!visible(&output).contains("refusal at validate"));
    assert!(mutating_calls(&fixture).is_empty(), "{:?}", fixture.argv());
}

/// #2863: the apply line a dry run prints is the argv the real run executes,
/// with only the retained values tempfile path replaced by a placeholder. The
/// overlay's contents never reach the plan.
#[test]
fn printed_apply_line_matches_executed_helm_argv() {
    let overlay = r#"{"worker":{"replicas":2},"marker":"overlay-value-must-not-print"}"#;
    for (scenario, install) in [("schema-compatible", false), ("fresh-install", true)] {
        let dry = Fixture::new(Some(overlay));
        let output = dry.run_with(scenario, "0.9.0", "charts/curie", &["--dry-run"]);
        assert!(output.status.success(), "{scenario}: {}", visible(&output));
        assert!(
            !visible(&output).contains("overlay-value-must-not-print"),
            "{scenario}: values contents must not be printed: {}",
            visible(&output)
        );
        let printed: Vec<String> = json(&output)["plan"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .find(|line| line.starts_with("helm upgrade "))
            .unwrap_or_else(|| panic!("{scenario}: no apply line: {}", visible(&output)))
            .split(' ')
            .map(str::to_owned)
            .collect();

        let real = Fixture::new(Some(overlay));
        let real_output = real.run(scenario, "0.9.0", "charts/curie");
        let upgrades = real.helm_upgrades();
        assert_eq!(upgrades.len(), 1, "{scenario}: {}", visible(&real_output));
        let mut executed = upgrades[0].clone();
        let values_at = executed
            .iter()
            .position(|arg| arg == "-f")
            .unwrap_or_else(|| panic!("{scenario}: apply passed no values: {executed:?}"));
        executed[values_at + 1] = "<retained-values>".into();

        assert_eq!(
            printed, executed,
            "{scenario}: printed plan drifted from apply"
        );
        assert_eq!(
            printed.iter().any(|arg| arg == "--install"),
            install,
            "{scenario}: --install only for a first install: {printed:?}"
        );
    }
}
