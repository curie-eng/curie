//! Local runner state and message turn context.
//!
//! Written to `.curie/runner.json` in the project directory so `message`,
//! `eval`, `cluster status`, and `cluster down` find the runner without flags. The file is
//! local workstation state, never committed (init scaffolds the ignore rule).
//! Message verbs also write `.curie/last-turn.json` with the last successful
//! turn's non-secret continuation context.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const STATE_DIR: &str = ".curie";
pub const STATE_FILE: &str = "runner.json";
pub const TURN_FILE: &str = "last-turn.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunnerState {
    pub container_id: String,
    pub container_name: String,
    pub image: String,
    pub port: u16,
    pub base_url: String,
    pub session_id: String,
    pub plugin_dir: String,
    pub fake_model: bool,
    #[serde(default)]
    pub ollama_container: Option<String>,
    #[serde(default)]
    pub network: Option<String>,
    #[serde(default)]
    pub model_base_url: Option<String>,
    /// sha256 of the tar.gz this runner's bundle was packed into (#1087),
    /// computed exactly the way `apps/api` computes it on deploy: sha256 of the
    /// packed archive bytes. It identifies THESE bytes from THIS packer, not the
    /// source tree in the abstract -- the tar carries per-file mtime/uid/gid, so
    /// the same content freshly cloned elsewhere digests differently. `None`
    /// only for a record written before #1087.
    #[serde(default)]
    pub bundle_digest: Option<String>,
    /// The materialized snapshot directory this runner has mounted, recorded
    /// (never re-derived) so teardown removes exactly what boot created.
    #[serde(default)]
    pub bundle_snapshot_dir: Option<String>,
    /// The connector containers this boot created, recorded (never re-derived)
    /// so `skill down` reaps exactly what `skill up` started. Without the
    /// record a run that dies mid-turn leaves labeled containers holding a
    /// resolved credential -- the failure class #747 fixed once for the runner.
    #[serde(default)]
    pub connector_containers: Vec<String>,
    /// The network the connectors and the runner share, when this boot made
    /// one. `None` for a bundle that declares no hosted connector.
    #[serde(default)]
    pub connector_network: Option<String>,
}

fn state_path(dir: &Path) -> PathBuf {
    dir.join(STATE_DIR).join(STATE_FILE)
}

fn turn_state_path(dir: &Path) -> PathBuf {
    dir.join(STATE_DIR).join(TURN_FILE)
}

pub fn save(dir: &Path, state: &RunnerState) -> Result<()> {
    let path = state_path(dir);
    std::fs::create_dir_all(path.parent().expect("state path has a parent"))?;
    let body = serde_json::to_string_pretty(state)?;
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

pub fn load(dir: &Path) -> Result<Option<RunnerState>> {
    let path = state_path(dir);
    if !path.is_file() {
        return Ok(None);
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let state = serde_json::from_str(&body)
        .with_context(|| format!("{} is not a valid runner state file", path.display()))?;
    Ok(Some(state))
}

pub fn remove(dir: &Path) -> Result<()> {
    let path = state_path(dir);
    if path.is_file() {
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TurnVerb {
    Local,
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnContext {
    pub verb: TurnVerb,
    pub channel: String,
    pub thread_ts: String,
    pub namespace: String,
    pub release: String,
    pub chart: String,
    pub listen_host: Option<String>,
    pub timeout_secs: u64,
    pub api_url: Option<String>,
    pub api_key_env: Option<String>,
}

impl TurnContext {
    pub fn from_turn(
        opts: &crate::message::MessageOpts,
        verb: TurnVerb,
        channel: &str,
        thread_ts: &str,
        env_api_key: Option<String>,
    ) -> TurnContext {
        TurnContext {
            verb,
            channel: channel.to_string(),
            thread_ts: thread_ts.to_string(),
            namespace: opts.namespace.clone(),
            release: opts.release.clone(),
            chart: opts.chart.clone(),
            listen_host: opts.listen_host.clone(),
            timeout_secs: opts.timeout_secs,
            api_url: opts.api_url.clone(),
            api_key_env: env_api_key
                .filter(|value| value == &opts.api_key)
                .map(|_| "CURIE_API_KEY".to_string()),
        }
    }
}

pub fn save_turn(dir: &Path, ctx: &TurnContext) -> Result<()> {
    let path = turn_state_path(dir);
    std::fs::create_dir_all(path.parent().expect("state path has a parent"))?;
    let body = serde_json::to_string_pretty(ctx)?;
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

pub fn load_turn(dir: &Path) -> Result<Option<TurnContext>> {
    let path = turn_state_path(dir);
    if !path.is_file() {
        return Ok(None);
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let state = serde_json::from_str(&body)
        .with_context(|| format!("{} is not a valid turn state file", path.display()))?;
    Ok(Some(state))
}

pub struct CliTurnArgs {
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub namespace: Option<String>,
    pub release: Option<String>,
    pub chart: Option<String>,
    pub listen_host: Option<String>,
    pub timeout_secs: Option<u64>,
    pub api_url: Option<String>,
    pub api_key: String,
}

pub struct ResolvedTurnArgs {
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub namespace: String,
    pub release: String,
    pub chart: Option<String>,
    pub listen_host: Option<String>,
    pub timeout_secs: u64,
    pub api_url: Option<String>,
    pub api_key: String,
}

pub fn apply_continue(
    verb: TurnVerb,
    cli: CliTurnArgs,
    state: Option<TurnContext>,
    env_api_key: Option<String>,
) -> Result<ResolvedTurnArgs> {
    if let Some(state) = state.as_ref() {
        if state.verb != verb {
            anyhow::bail!(
                "the last turn was '{} message'; re-run that verb with --continue, or drop --continue",
                turn_verb_name(state.verb)
            );
        }
    }

    let api_key = if cli.api_key != crate::message::DEFAULT_API_KEY {
        cli.api_key
    } else if let Some(name) = state
        .as_ref()
        .and_then(|state| state.api_key_env.as_deref())
        .filter(|_| env_api_key.is_none())
    {
        anyhow::bail!(
            "this conversation was started with the api key from ${name}, which is not set now; re-export it or pass --api-key"
        );
    } else {
        cli.api_key
    };

    Ok(ResolvedTurnArgs {
        channel: cli
            .channel
            .or_else(|| state.as_ref().map(|state| state.channel.clone())),
        thread: cli
            .thread
            .or_else(|| state.as_ref().map(|state| state.thread_ts.clone())),
        namespace: cli
            .namespace
            .or_else(|| state.as_ref().map(|state| state.namespace.clone()))
            .unwrap_or_else(|| "curie".to_string()),
        release: cli
            .release
            .or_else(|| state.as_ref().map(|state| state.release.clone()))
            .unwrap_or_else(|| "curie".to_string()),
        chart: cli
            .chart
            .or_else(|| state.as_ref().map(|state| state.chart.clone())),
        listen_host: cli
            .listen_host
            .or_else(|| state.as_ref().and_then(|state| state.listen_host.clone())),
        timeout_secs: cli
            .timeout_secs
            .or_else(|| state.as_ref().map(|state| state.timeout_secs))
            .unwrap_or(crate::message::DEFAULT_TIMEOUT_SECS),
        api_url: cli
            .api_url
            .or_else(|| state.as_ref().and_then(|state| state.api_url.clone())),
        api_key,
    })
}

fn turn_verb_name(verb: TurnVerb) -> &'static str {
    match verb {
        TurnVerb::Local => "local",
        TurnVerb::Cluster => "cluster",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{MessageOpts, DEFAULT_API_KEY, DEFAULT_TIMEOUT_SECS};

    fn sample() -> RunnerState {
        RunnerState {
            container_id: "abc123".into(),
            container_name: "curie-runner-local".into(),
            image: "curie-runner".into(),
            port: 7245,
            base_url: "http://localhost:7245".into(),
            session_id: "local-1".into(),
            plugin_dir: "/tmp/deal-desk".into(),
            fake_model: true,
            ollama_container: None,
            network: None,
            model_base_url: None,
            bundle_digest: None,
            bundle_snapshot_dir: None,
            connector_containers: Vec::new(),
            connector_network: None,
        }
    }

    #[test]
    fn round_trips_through_the_state_file() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
        save(dir.path(), &sample()).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(sample()));
        remove(dir.path()).unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
    }

    #[test]
    fn corrupt_state_is_an_error_not_a_silent_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(STATE_DIR)).unwrap();
        std::fs::write(dir.path().join(STATE_DIR).join(STATE_FILE), "not json").unwrap();
        assert!(load(dir.path()).is_err());
    }

    #[test]
    fn round_trip_preserves_local_model_runner_fields() {
        let dir = tempfile::tempdir().unwrap();
        let state = RunnerState {
            ollama_container: Some("curie-ollama".into()),
            network: Some("curie_default".into()),
            model_base_url: Some("http://ollama:11434".into()),
            ..sample()
        };
        save(dir.path(), &state).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(state));
    }

    #[test]
    fn load_accepts_older_state_without_local_model_keys() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(STATE_DIR)).unwrap();
        std::fs::write(
            dir.path().join(STATE_DIR).join(STATE_FILE),
            r#"{
  "container_id": "abc123",
  "container_name": "curie-runner-local",
  "image": "curie-runner",
  "port": 7245,
  "base_url": "http://localhost:7245",
  "session_id": "local-1",
  "plugin_dir": "/tmp/deal-desk",
  "fake_model": true
}"#,
        )
        .unwrap();
        let loaded = load(dir.path()).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().ollama_container, None);
    }

    /// #1087: what boot created, recorded so teardown removes exactly that --
    /// asserted on the bytes that survive a real write/read round trip through
    /// `.curie/runner.json`, not on the in-memory struct alone.
    #[test]
    fn round_trip_preserves_the_bundle_snapshot_fields() {
        let dir = tempfile::tempdir().unwrap();
        let digest = "a".repeat(64);
        let snapshot_dir = format!("/tmp/deal-desk/.curie/snapshots/{digest}");
        let state = RunnerState {
            bundle_digest: Some(digest.clone()),
            bundle_snapshot_dir: Some(snapshot_dir.clone()),
            ..sample()
        };
        save(dir.path(), &state).unwrap();

        let loaded = load(dir.path()).unwrap().expect("the record round trips");
        assert_eq!(loaded.bundle_digest, Some(digest.clone()));
        assert_eq!(loaded.bundle_snapshot_dir, Some(snapshot_dir.clone()));
        // The file itself is the contract a later `skill down` and `skill
        // status` read, so pin the keys on disk rather than only in memory.
        let raw = std::fs::read_to_string(dir.path().join(STATE_DIR).join(STATE_FILE)).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["bundle_digest"], serde_json::json!(digest));
        assert_eq!(
            parsed["bundle_snapshot_dir"],
            serde_json::json!(snapshot_dir)
        );
    }

    /// A `.curie/runner.json` written before #1087 must still load: the file
    /// outlives the CLI version that wrote it, and `load` treats a parse failure
    /// as a hard error (see `corrupt_state_is_an_error_not_a_silent_none`), so a
    /// missing key without `serde(default)` would break `skill down` on every
    /// bundle upgraded in place.
    #[test]
    fn load_accepts_older_state_without_bundle_keys() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(STATE_DIR)).unwrap();
        std::fs::write(
            dir.path().join(STATE_DIR).join(STATE_FILE),
            r#"{
  "container_id": "abc123",
  "container_name": "curie-runner-local",
  "image": "curie-runner",
  "port": 7245,
  "base_url": "http://localhost:7245",
  "session_id": "local-1",
  "plugin_dir": "/tmp/deal-desk",
  "fake_model": true,
  "ollama_container": null,
  "network": null,
  "model_base_url": null
}"#,
        )
        .unwrap();

        let loaded = load(dir.path())
            .expect("a pre-#1087 record must parse, not error")
            .expect("a pre-#1087 record is a record, not an absent one");
        assert_eq!(loaded.bundle_digest, None);
        assert_eq!(loaded.bundle_snapshot_dir, None);
        assert_eq!(loaded.container_name, "curie-runner-local");
    }

    fn default_cli_turn_args() -> CliTurnArgs {
        CliTurnArgs {
            channel: None,
            thread: None,
            namespace: None,
            release: None,
            chart: None,
            listen_host: None,
            timeout_secs: None,
            api_url: None,
            api_key: DEFAULT_API_KEY.into(),
        }
    }

    fn persisted_turn(verb: TurnVerb) -> TurnContext {
        TurnContext {
            verb,
            channel: "C-persisted".into(),
            thread_ts: "9.9".into(),
            namespace: "persisted-ns".into(),
            release: "persisted-release".into(),
            chart: "charts/persisted".into(),
            listen_host: Some("127.0.0.1".into()),
            api_key_env: Some("CURIE_API_KEY".into()),
            timeout_secs: 777,
            api_url: Some("http://persisted-api".into()),
        }
    }

    #[test]
    fn turn_context_round_trips_through_the_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = TurnContext {
            verb: TurnVerb::Cluster,
            channel: "C1".into(),
            thread_ts: "9.9".into(),
            namespace: "curie-turns".into(),
            release: "curie-turns".into(),
            chart: "charts/curie-turns".into(),
            listen_host: Some("127.0.0.1".into()),
            api_key_env: Some("CURIE_API_KEY".into()),
            timeout_secs: 321,
            api_url: Some("http://turn-api".into()),
        };

        save_turn(dir.path(), &state).unwrap();
        assert_eq!(load_turn(dir.path()).unwrap(), Some(state));

        let empty = tempfile::tempdir().unwrap();
        assert_eq!(load_turn(empty.path()).unwrap(), None);
    }

    #[test]
    fn corrupt_turn_state_is_an_error_not_a_silent_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(STATE_DIR)).unwrap();
        std::fs::write(dir.path().join(STATE_DIR).join(TURN_FILE), "not json").unwrap();
        assert!(load_turn(dir.path()).is_err());
    }

    #[test]
    fn turn_state_never_contains_secret_values() {
        let dir = tempfile::tempdir().unwrap();
        let opts = MessageOpts {
            text: "hi".into(),
            channel: None,
            thread: None,
            namespace: "curie".into(),
            release: "curie".into(),
            chart: "charts/curie".into(),
            listen_host: None,
            listen_port: 8155,
            valkey_local_port: 56381,
            valkey_password: "vk-secret".into(),
            api_local_port: 8123,
            api_key: "sk-super-secret".into(),
            user: "U".into(),
            stream: "s".into(),
            timeout_secs: 300,
            dry_run: false,
            local: false,
            api_url: None,
        };

        let turn = TurnContext::from_turn(
            &opts,
            TurnVerb::Cluster,
            "C1",
            "9.9",
            Some("sk-super-secret".into()),
        );
        save_turn(dir.path(), &turn).unwrap();

        let raw = std::fs::read_to_string(dir.path().join(STATE_DIR).join(TURN_FILE)).unwrap();
        assert!(!raw.contains("sk-super-secret"));
        assert!(!raw.contains("vk-secret"));
        assert!(raw.contains("CURIE_API_KEY"));
    }

    #[test]
    fn from_turn_records_env_source_only_when_env_value_matches() {
        let opts = MessageOpts {
            text: "hi".into(),
            channel: None,
            thread: None,
            namespace: "curie".into(),
            release: "curie".into(),
            chart: "charts/curie".into(),
            listen_host: None,
            listen_port: 8155,
            valkey_local_port: 56381,
            valkey_password: "vk-secret".into(),
            api_local_port: 8123,
            api_key: "sk-super-secret".into(),
            user: "U".into(),
            stream: "s".into(),
            timeout_secs: 300,
            dry_run: false,
            local: false,
            api_url: None,
        };

        let from_equal = TurnContext::from_turn(
            &opts,
            TurnVerb::Cluster,
            "C1",
            "9.9",
            Some("sk-super-secret".into()),
        );
        assert_eq!(from_equal.api_key_env.as_deref(), Some("CURIE_API_KEY"));

        let from_different = TurnContext::from_turn(
            &opts,
            TurnVerb::Cluster,
            "C1",
            "9.9",
            Some("different".into()),
        );
        assert_eq!(from_different.api_key_env, None);

        let from_none = TurnContext::from_turn(&opts, TurnVerb::Cluster, "C1", "9.9", None);
        assert_eq!(from_none.api_key_env, None);
    }

    #[test]
    fn turn_state_coexists_with_runner_state() {
        let dir = tempfile::tempdir().unwrap();
        let runner = sample();
        let turn = persisted_turn(TurnVerb::Cluster);

        save(dir.path(), &runner).unwrap();
        save_turn(dir.path(), &turn).unwrap();

        assert_eq!(load(dir.path()).unwrap(), Some(runner));
        assert_eq!(load_turn(dir.path()).unwrap(), Some(turn));
    }

    #[test]
    fn continue_verb_mismatch_errors() {
        assert!(apply_continue(
            TurnVerb::Local,
            default_cli_turn_args(),
            Some(persisted_turn(TurnVerb::Cluster)),
            None,
        )
        .is_err());
    }

    #[test]
    fn explicit_flags_beat_persisted_state() {
        let mut cli = default_cli_turn_args();
        cli.channel = Some("C-explicit".into());
        cli.thread = Some("1.2".into());
        cli.namespace = Some("explicit-ns".into());
        cli.release = Some("explicit-release".into());
        cli.chart = Some("charts/explicit".into());
        cli.listen_host = Some("192.0.2.10".into());
        cli.timeout_secs = Some(12);
        cli.api_url = Some("http://explicit-api".into());
        cli.api_key = "cli-explicit-key".into();

        let resolved = apply_continue(
            TurnVerb::Cluster,
            cli,
            Some(persisted_turn(TurnVerb::Cluster)),
            None,
        )
        .unwrap();

        assert_eq!(resolved.channel.as_deref(), Some("C-explicit"));
        assert_eq!(resolved.thread, Some("1.2".into()));
        assert_eq!(resolved.namespace, "explicit-ns");
        assert_eq!(resolved.release, "explicit-release");
        assert_eq!(resolved.chart.as_deref(), Some("charts/explicit"));
        assert_eq!(resolved.listen_host.as_deref(), Some("192.0.2.10"));
        assert_eq!(resolved.timeout_secs, 12);
        assert_eq!(resolved.api_url.as_deref(), Some("http://explicit-api"));
        assert_eq!(resolved.api_key, "cli-explicit-key");
    }

    #[test]
    fn persisted_state_beats_builtin_defaults() {
        let cli = default_cli_turn_args();

        let persisted = TurnContext {
            verb: TurnVerb::Cluster,
            channel: "C-persisted".into(),
            thread_ts: "9.9".into(),
            namespace: "persisted-ns".into(),
            release: "persisted-release".into(),
            chart: "charts/persisted".into(),
            listen_host: Some("127.0.0.1".into()),
            api_key_env: None,
            timeout_secs: 777,
            api_url: Some("http://persisted-api".into()),
        };

        let resolved = apply_continue(TurnVerb::Cluster, cli, Some(persisted), None).unwrap();

        assert_eq!(resolved.channel.as_deref(), Some("C-persisted"));
        assert_eq!(resolved.thread, Some("9.9".into()));
        assert_eq!(resolved.namespace, "persisted-ns");
        assert_eq!(resolved.release, "persisted-release");
        assert_eq!(resolved.chart.as_deref(), Some("charts/persisted"));
        assert_eq!(resolved.listen_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(resolved.timeout_secs, 777);
        assert_eq!(resolved.api_url.as_deref(), Some("http://persisted-api"));
    }

    #[test]
    fn absent_continue_ignores_existing_state() {
        let cli = default_cli_turn_args();

        let resolved = apply_continue(TurnVerb::Cluster, cli, None, None).unwrap();

        assert_eq!(resolved.channel, None);
        assert_eq!(resolved.thread, None);
        assert_eq!(resolved.namespace, "curie");
        assert_eq!(resolved.release, "curie");
        assert_eq!(resolved.chart, None);
        assert_eq!(resolved.timeout_secs, DEFAULT_TIMEOUT_SECS);
    }

    #[test]
    fn api_key_env_recorded_but_unset_now_errors() {
        let cli = default_cli_turn_args();

        assert!(apply_continue(
            TurnVerb::Cluster,
            cli,
            Some(persisted_turn(TurnVerb::Cluster)),
            None,
        )
        .is_err());
    }

    #[test]
    fn api_key_explicit_value_wins_over_recorded_env() {
        let mut cli = default_cli_turn_args();
        cli.api_key = "sk-live-flag".into();

        let resolved = apply_continue(
            TurnVerb::Cluster,
            cli,
            Some(persisted_turn(TurnVerb::Cluster)),
            None,
        )
        .unwrap();

        assert_eq!(resolved.api_key, "sk-live-flag");
    }

    // @spec ADR-0168 d8
    #[test]
    fn continue_carries_the_agent_the_last_turn_selected() {
        let mut saved = TurnContext::from_turn(
            &MessageOpts {
                agent: Some("ops".into()),
                ..MessageOpts::default()
            },
            TurnVerb::Local,
            "C0EXAMPLE1",
            "1.0",
            None,
        );
        assert_eq!(saved.agent.as_deref(), Some("ops"));
        let cli = || CliTurnArgs {
            channel: None,
            thread: None,
            namespace: None,
            release: None,
            chart: None,
            listen_host: None,
            timeout_secs: None,
            api_url: None,
            api_key: DEFAULT_API_KEY.into(),
            agent: None,
        };
        let resolved = apply_continue(TurnVerb::Local, cli(), Some(saved.clone()), None).unwrap();
        assert_eq!(resolved.agent.as_deref(), Some("ops"));
        saved.agent = None;
        let older: TurnContext = serde_json::from_str(
            &serde_json::to_string(&saved)
                .unwrap()
                .replace(",\"agent\":null", ""),
        )
        .expect("a state file written before the field still loads");
        assert_eq!(older.agent, None);
    }

    #[test]
    fn api_key_env_recorded_and_still_set_is_reread_without_error() {
        // Turn 1 recorded api_key_env = Some("CURIE_API_KEY"); on continue the env is still
        // set, so clap re-reads it into cli.api_key (!= default) and apply_continue must succeed
        // using that live value, never erroring and never touching the stored source name.
        let mut cli = default_cli_turn_args();
        cli.api_key = "sk-live-from-env".into(); // what clap's `env = CURIE_API_KEY` re-read

        let resolved = apply_continue(
            TurnVerb::Cluster,
            cli,
            Some(persisted_turn(TurnVerb::Cluster)), // api_key_env = Some("CURIE_API_KEY")
            Some("sk-live-from-env".into()),         // env still set
        )
        .unwrap();

        assert_eq!(resolved.api_key, "sk-live-from-env");
    }
}
