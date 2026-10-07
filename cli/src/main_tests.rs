use crate::args::*;
use crate::dispatch::*;
use clap::{CommandFactory, Parser};

#[test]
fn clap_surface_is_valid() {
    on_parse_stack(|| Cli::command().debug_assert());
}

// @spec ADR-0168 d8
#[test]
fn the_drivers_take_an_agent_selector() {
    for argv in [
        ["curie", "local", "message", "--agent", "ops", "hi"],
        ["curie", "cluster", "message", "--agent", "ops", "hi"],
    ] {
        assert!(try_parse_from(argv).is_ok(), "{argv:?}");
    }
    for argv in [
        ["curie", "local", "eval", "--agent", "ops"],
        ["curie", "cluster", "eval", "--agent", "ops"],
    ] {
        assert!(try_parse_from(argv).is_ok(), "{argv:?}");
    }
}

// #3619 C4: the webhook secret never reaches argv, so no value flag exists.
#[test]
fn cluster_factory_has_no_webhook_secret_value_flag() {
    let error = match try_parse_from([
        "curie",
        "cluster",
        "factory",
        "--webhook-secret",
        "x",
        "--dry-run",
    ]) {
        Ok(_) => panic!("--webhook-secret <value> must not parse"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    assert!(error.to_string().contains("--webhook-secret"), "{error}");
    assert!(
        try_parse_from([
            "curie",
            "cluster",
            "factory",
            "--webhook-secret-file",
            "/tmp/s",
            "--label",
            "factory",
            "--dry-run"
        ])
        .is_ok(),
        "the file form is the supported path"
    );
}

/// clap's derived parser is deep enough that debug bin tests overflow the
/// default thread stack once apply/diff/doctor grew `--context`. The
/// released binary still parses on the process stack; only the test
/// harness needs the extra room.
fn on_parse_stack<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("cli-parse".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(f)
        .expect("spawn cli parse thread")
        .join()
        .expect("cli parse thread")
}

fn try_parse_from<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T> + Send + 'static,
    T: Into<std::ffi::OsString> + Clone + Send + 'static,
{
    on_parse_stack(move || Cli::try_parse_from(args))
}

fn message_value_flags(path: &[&str]) -> std::collections::BTreeSet<String> {
    let root = Cli::command();
    let mut command = &root;
    let mut flags = std::collections::BTreeSet::new();

    for (index, name) in path.iter().enumerate() {
        let is_leaf = index + 1 == path.len();
        for arg in command.get_arguments() {
            if (!is_leaf && !arg.is_global_set()) || !arg.get_action().takes_values() {
                continue;
            }
            if let Some(long) = arg.get_long() {
                flags.insert(format!("--{long}"));
            }
        }
        command = command
            .find_subcommand(name)
            .unwrap_or_else(|| panic!("missing command path component {name:?}"));
    }

    for arg in command.get_arguments() {
        if arg.get_action().takes_values() {
            if let Some(long) = arg.get_long() {
                flags.insert(format!("--{long}"));
            }
        }
    }

    flags
}

fn message_value_flags_from_source() -> std::collections::BTreeSet<String> {
    let source = include_str!("message.rs");
    let body = source
        .split("const MESSAGE_VALUE_FLAGS: &[&str] = &[")
        .nth(1)
        .and_then(|rest| rest.split_once("];"))
        .map(|(body, _)| body)
        .expect("message.rs must contain MESSAGE_VALUE_FLAGS");

    body.lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix('"')
                .and_then(|flag| flag.strip_suffix("\","))
                .map(str::to_string)
        })
        .collect()
}

#[test]
fn message_preflight_value_flags_match_clap_command_graph() {
    on_parse_stack(|| {
        let mut derived = message_value_flags(&["local", "message"]);
        derived.extend(message_value_flags(&["cluster", "message"]));
        let source = message_value_flags_from_source();
        let missing: Vec<_> = derived.difference(&source).cloned().collect();
        let stale: Vec<_> = source.difference(&derived).cloned().collect();

        assert!(
            missing.is_empty() && stale.is_empty(),
            "message value flag inventory drifted from clap: missing={missing:?}, stale={stale:?}, derived={derived:?}, source={source:?}"
        );
    });
}

/// Serializes the `cluster_connector_bind_values` cases that mutate the
/// process environment, for the same reason and with the same limits as
/// `GITHUB_TOKEN_ENV_LOCK` below: `set_var` is not thread-safe against a
/// concurrent `getenv` from a test that does not take this lock. The
/// precedent in this crate is `cli/src/slack.rs`.
static BIND_VALUES_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn bind_values_env_lock() -> std::sync::MutexGuard<'static, ()> {
    BIND_VALUES_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn bind_values_binds_a_connector_secret_with_no_explicit_flag() {
    // #2503: with zero `--secret` flags the connector's own declared name
    // still reaches the sandbox bind map, carrying the value the connector
    // plan already resolved for THIS cluster scope (#1913). No `--secret`
    // means `resolve_named_secrets` iterates nothing, so neither the
    // environment nor the host vault is consulted at all.
    let values = cluster_connector_bind_values(
        &[],
        &["GH".to_string()],
        &std::collections::BTreeMap::from([("GH".to_string(), "scoped".to_string())]),
    )
    .expect("an owned connector value needs no local resolution");
    assert_eq!(
        values,
        std::collections::BTreeMap::from([("GH".to_string(), "scoped".to_string())])
    );
}

#[test]
fn bind_values_prefers_the_scoped_connector_value_over_the_environment() {
    // The overlap case: the operator also passed `--secret GH` and the
    // environment holds a DIFFERENT credential. The connector pod
    // authenticates with the cluster-scoped value, so the sandbox must get
    // that one -- #1913 is one cluster, one credential, and a split would
    // 401 exactly the derived Bearer header #2503 exists to make work.
    let _guard = bind_values_env_lock();
    let previous = std::env::var("CURIE_TEST_BIND_GH").ok();
    std::env::set_var("CURIE_TEST_BIND_GH", "env-sentinel");

    let values = cluster_connector_bind_values(
        &["CURIE_TEST_BIND_GH".to_string()],
        &["CURIE_TEST_BIND_GH".to_string()],
        &std::collections::BTreeMap::from([(
            "CURIE_TEST_BIND_GH".to_string(),
            "scoped-sentinel".to_string(),
        )]),
    );

    match previous {
        Some(value) => std::env::set_var("CURIE_TEST_BIND_GH", value),
        None => std::env::remove_var("CURIE_TEST_BIND_GH"),
    }

    let values = values.expect("an env-resolvable --secret must not error");
    assert_eq!(
        values.get("CURIE_TEST_BIND_GH").map(String::as_str),
        Some("scoped-sentinel"),
        "the connector-scoped value must win over the environment value"
    );
    assert_eq!(values.len(), 1);
}

#[test]
fn bind_values_with_nothing_to_bind_is_empty() {
    // Baseline: no `--secret` and no connector-owned values binds nothing,
    // preserving the pre-#2503 behavior for an ordinary bundle.
    let values = cluster_connector_bind_values(&[], &[], &std::collections::BTreeMap::new())
        .expect("nothing to resolve");
    assert!(values.is_empty());
    // A declared connector name with no resolved value adds nothing
    // either: the bind map never invents a value for a name.
    let values =
        cluster_connector_bind_values(&[], &["GH".to_string()], &std::collections::BTreeMap::new())
            .expect("nothing to resolve");
    assert!(values.is_empty());
}

#[test]
fn skill_approvals_accepts_list_and_resolve_to_decline_them() {
    // The flags exist so the skill tier DECLINES them with a reason
    // (ADR-0077), not clap-erroring like an unknown-flag typo.
    try_parse_from(["curie", "skill", "approvals", "--list"])
        .expect("skill approvals --list should parse");
    try_parse_from(["curie", "skill", "approvals", "--resolve", "abc"])
        .expect("skill approvals --resolve should parse");
}

/// Serializes the two `cluster up` GitHub-credential cases below.
///
/// `cluster_up_clap_accepts_the_token_from_the_environment_only` has to arm
/// the input through `CURIE_GITHUB_TOKEN` alone, and clap reads an `env =`
/// binding from the PROCESS environment at parse time, with no injection
/// seam, so the variable is set on this process and restored afterwards.
///
/// What this lock DOES guarantee, precisely: the two tests that take it
/// never run concurrently, so neither observes the other's mutation and
/// neither leaves `CURIE_GITHUB_TOKEN` behind.
///
/// What it does NOT guarantee: `setenv` is not thread-safe against a
/// concurrent `getenv` from a thread that does not take this lock, and cargo
/// runs this binary's tests in parallel. Other tests call `std::env::var`
/// and `std::env::temp_dir()` (which reads `TMPDIR`) while these two mutate
/// the environment. That is a latent data race, not a race this `Mutex` can
/// close -- it is exactly why Rust 2024 made `std::env::set_var` `unsafe`;
/// this crate is `edition = "2021"`, so it compiles. The precedent already
/// in the tree is `cli/src/slack.rs`. The real fix is a clap-level
/// injection seam (parsing from an explicit env source rather than the
/// process environment), which is a production change.
static GITHUB_TOKEN_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A failing assertion panics while holding the lock, which poisons it. The
/// data is `()`, so there is nothing to corrupt: recover the guard rather
/// than let the first red case cascade into bogus `PoisonError` failures.
fn github_token_env_lock() -> std::sync::MutexGuard<'static, ()> {
    GITHUB_TOKEN_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn cluster_up_clap_accepts_the_token_from_the_environment_only() {
    // #1124 AC1, armed through the SECONDARY path: the environment variable
    // alone, never the command line. The env var is the input that keeps the
    // credential out of shell history, so it has to work with no flag
    // present. `Option<String>` with no `default_value` is what makes
    // "absent" distinguishable from "empty"; an empty variable is covered by
    // the resolver's `empty_value_preserves_and_never_clears`.
    //
    // The name is CURIE_GITHUB_TOKEN, never GITHUB_TOKEN: the latter is
    // exported in the shells of most people who use `gh` and in most CI
    // runners, so binding to it would silently capture a personal PAT into a
    // persistent cluster Secret (#496 canonicalized the CURIE_ namespace).
    let _guard = github_token_env_lock();
    let previous = std::env::var("CURIE_GITHUB_TOKEN").ok();

    std::env::set_var("CURIE_GITHUB_TOKEN", "ghp-SENTINEL-1124-leak-canary"); // gitleaks:allow -- test leak canary, not a real token
    let from_env = try_parse_from(["curie", "cluster", "up"])
        .expect("cluster up should parse with only the env var set");
    std::env::remove_var("CURIE_GITHUB_TOKEN");
    let without = try_parse_from(["curie", "cluster", "up"])
        .expect("cluster up should parse with nothing set");
    match previous {
        Some(value) => std::env::set_var("CURIE_GITHUB_TOKEN", value),
        None => std::env::remove_var("CURIE_GITHUB_TOKEN"),
    }

    match from_env.command {
        Some(Command::Cluster {
            action: ClusterAction::Up { github_token, .. },
            ..
        }) => assert_eq!(
            github_token.as_deref(),
            Some("ghp-SENTINEL-1124-leak-canary")
        ),
        _ => panic!("expected cluster up"),
    }
    match without.command {
        Some(Command::Cluster {
            action: ClusterAction::Up { github_token, .. },
            ..
        }) => assert_eq!(
            github_token, None,
            "an unset variable must be absence, not an empty credential"
        ),
        _ => panic!("expected cluster up"),
    }
}

#[test]
fn cluster_upgrade_requires_to_and_reads_namespace_env() {
    let parsed = try_parse_from(["curie", "cluster", "upgrade", "--to", "0.9.0", "--dry-run"])
        .expect("cluster upgrade --to should parse");
    match parsed.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Upgrade {
                    to,
                    namespace,
                    dry_run,
                    ..
                },
            ..
        }) => {
            assert_eq!(to, "0.9.0");
            assert_eq!(namespace, "curie");
            assert!(dry_run);
        }
        _ => panic!("expected cluster upgrade"),
    }
    let missing = try_parse_from(["curie", "cluster", "upgrade"]);
    assert!(missing.is_err(), "--to is required");
}

#[test]
fn clap_rejects_flag_and_clear_together() {
    // #1124 AC4, armed through the PARSER rather than the resolver: the
    // fourth, invalid state (set and clear at once) never reaches
    // `resolve_github_token` at all. This also catches `conflicts_with`
    // naming clap's arg ID wrongly -- a spelling like "clear-github-token"
    // compiles and then panics at runtime on the first parse.
    let _guard = github_token_env_lock();
    let previous = std::env::var("CURIE_GITHUB_TOKEN").ok();
    std::env::remove_var("CURIE_GITHUB_TOKEN");

    let both = try_parse_from([
        "curie",
        "cluster",
        "up",
        "--github-token",
        "ghp-SENTINEL-1124-leak-canary",
        "--clear-github-token",
    ]);
    // Each alone still parses, so the rejection is the conflict and not a
    // broken flag.
    let set_only = try_parse_from([
        "curie",
        "cluster",
        "up",
        "--github-token",
        "ghp-SENTINEL-1124-leak-canary",
    ]);
    let clear_only = try_parse_from(["curie", "cluster", "up", "--clear-github-token"]);

    if let Some(value) = previous {
        std::env::set_var("CURIE_GITHUB_TOKEN", value);
    }

    assert!(
        both.is_err(),
        "--github-token with --clear-github-token must be a clap conflict"
    );
    assert!(set_only.is_ok(), "--github-token alone must parse");
    assert!(clear_only.is_ok(), "--clear-github-token alone must parse");
}

#[test]
fn clap_rejects_migrate_store_with_allow_stateful_removal() {
    // #1351: the pair states contradictory intent (carry the object store's
    // data across the upgrade, versus proceed WITHOUT it). Refused at the
    // parser, so the contradiction never reaches the code that silently
    // picked one and took the data destroying path with exit 0.
    //
    // Both orderings are asserted rather than trusting that a
    // `conflicts_with` declared on one arg is mutual. Each flag alone must
    // still parse: a conflict naming an arg id that does not exist panics
    // at parse time, and the "alone" arms are what catch that.
    let both = try_parse_from([
        "curie",
        "apply",
        "--migrate-store",
        "--allow-stateful-removal",
    ]);
    let reversed = try_parse_from([
        "curie",
        "apply",
        "--allow-stateful-removal",
        "--migrate-store",
    ]);
    let migrate_only = try_parse_from(["curie", "apply", "--migrate-store"]);
    let allow_only = try_parse_from(["curie", "apply", "--allow-stateful-removal"]);

    assert!(
        both.is_err(),
        "--migrate-store with --allow-stateful-removal must be a clap conflict"
    );
    assert!(
        reversed.is_err(),
        "the conflict must hold in either argument order"
    );
    assert!(migrate_only.is_ok(), "--migrate-store alone must parse");
    assert!(
        allow_only.is_ok(),
        "--allow-stateful-removal alone must parse"
    );
}

#[test]
fn build_platform_requires_registry() {
    assert!(
        try_parse_from([
            "curie",
            "build",
            "--plugin-dir",
            "x",
            "--platform",
            "linux/arm64"
        ])
        .is_err(),
        "--platform without --registry must be refused"
    );
    assert!(try_parse_from([
        "curie",
        "build",
        "--plugin-dir",
        "x",
        "--registry",
        "r",
        "--platform",
        "linux/arm64"
    ])
    .is_ok());
}

#[test]
fn build_defaults_tag_and_accepts_override() {
    let cli = try_parse_from(["curie", "build"]).expect("build should parse");
    match cli.command {
        Some(Command::Build { tag, .. }) => assert_eq!(tag, "curie-runner"),
        _ => panic!("expected build command"),
    }
    let cli = try_parse_from(["curie", "build", "--tag", "my-runner:dev"])
        .expect("build --tag should parse");
    match cli.command {
        Some(Command::Build { tag, .. }) => assert_eq!(tag, "my-runner:dev"),
        _ => panic!("expected build command"),
    }
}

#[test]
fn list_agents_parses() {
    let cli = try_parse_from(["curie", "list-agents"]).expect("list-agents should parse");
    assert!(matches!(cli.command, Some(Command::ListAgents)));
}

#[test]
fn deploy_local_parses_the_folder_positional_and_defaults() {
    let cli = try_parse_from(["curie", "deploy-local", "revenue-leak"])
        .expect("deploy-local should parse");
    match cli.command {
        Some(Command::DeployLocal {
            folder,
            slack_channel,
            secret,
            ..
        }) => {
            assert_eq!(folder, "revenue-leak");
            assert_eq!(slack_channel, None);
            assert!(secret.is_empty());
        }
        _ => panic!("expected deploy-local command"),
    }
}

#[test]
fn no_subcommand_defaults_to_interactive() {
    let cli = try_parse_from(["curie"]).expect("bare curie should parse");
    assert!(cli.command.is_none());
}

#[test]
fn install_parses() {
    let cli = try_parse_from(["curie", "install"]).expect("install should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Install { update: false })
    ));
}

#[test]
fn install_update_parses() {
    let cli = try_parse_from(["curie", "install", "--update"]).expect("install should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Install { update: true })
    ));
}

#[test]
fn skill_eval_model_is_repeatable_for_a_sweep() {
    // No --model -> drive the running runner (empty models vec).
    match try_parse_from(["curie", "skill", "eval"])
        .expect("skill eval should parse")
        .command
    {
        Some(Command::Skill {
            action: SkillAction::Eval { model, .. },
        }) => assert!(model.is_empty()),
        _ => panic!("expected skill eval"),
    }
    // Repeated --model collects into the sweep list.
    match try_parse_from([
        "curie",
        "skill",
        "eval",
        "--model",
        "claude-haiku-4-5",
        "--model",
        "claude-sonnet-5",
    ])
    .expect("skill eval sweep should parse")
    .command
    {
        Some(Command::Skill {
            action: SkillAction::Eval { model, .. },
        }) => assert_eq!(model, vec!["claude-haiku-4-5", "claude-sonnet-5"]),
        _ => panic!("expected skill eval sweep"),
    }
}

#[test]
fn local_and_cluster_eval_model_are_repeatable_for_a_sweep() {
    // local eval --model repeats into the sweep list (#526).
    match try_parse_from([
        "curie", "local", "eval", "--model", "opus", "--model", "sonnet",
    ])
    .expect("local eval sweep should parse")
    .command
    {
        Some(Command::Local {
            action: LocalAction::Eval { model, .. },
        }) => assert_eq!(model, vec!["opus", "sonnet"]),
        _ => panic!("expected local eval sweep"),
    }
    // Bare local eval -> no models (the in-CLI parity gate).
    match try_parse_from(["curie", "local", "eval"])
        .expect("local eval should parse")
        .command
    {
        Some(Command::Local {
            action: LocalAction::Eval { model, .. },
        }) => assert!(model.is_empty()),
        _ => panic!("expected local eval"),
    }
    // cluster eval --model likewise.
    match try_parse_from(["curie", "cluster", "eval", "--model", "opus"])
        .expect("cluster eval sweep should parse")
        .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Eval { model, .. },
            ..
        }) => assert_eq!(model, vec!["opus"]),
        _ => panic!("expected cluster eval sweep"),
    }
}

#[test]
fn eval_case_id_is_repeatable_at_every_tier_and_empty_when_absent() {
    // #2007: `--case-id` is the eval case SELECTOR (distinct from `--cases`,
    // the suite FILE). Absent means the whole suite; repeated means a subset,
    // and a value matching nothing exits 2 rather than greening an empty run.
    match try_parse_from([
        "curie",
        "skill",
        "eval",
        "--case-id",
        "greets-the-user",
        "--case-id",
        "escalates",
    ])
    .expect("skill eval --case-id should parse")
    .command
    {
        Some(Command::Skill {
            action: SkillAction::Eval { case_id, .. },
        }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
        _ => panic!("expected skill eval with a selector"),
    }
    match try_parse_from(["curie", "skill", "eval"])
        .expect("skill eval should parse")
        .command
    {
        Some(Command::Skill {
            action: SkillAction::Eval { case_id, .. },
        }) => assert!(case_id.is_empty(), "no selector -> the whole suite"),
        _ => panic!("expected skill eval"),
    }
    match try_parse_from([
        "curie",
        "local",
        "eval",
        "--case-id",
        "greets-the-user",
        "--case-id",
        "escalates",
    ])
    .expect("local eval --case-id should parse")
    .command
    {
        Some(Command::Local {
            action: LocalAction::Eval { case_id, .. },
        }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
        _ => panic!("expected local eval with a selector"),
    }
    match try_parse_from(["curie", "local", "eval"])
        .expect("local eval should parse")
        .command
    {
        Some(Command::Local {
            action: LocalAction::Eval { case_id, .. },
        }) => assert!(case_id.is_empty()),
        _ => panic!("expected local eval"),
    }
    match try_parse_from([
        "curie",
        "cluster",
        "eval",
        "--case-id",
        "greets-the-user",
        "--case-id",
        "escalates",
    ])
    .expect("cluster eval --case-id should parse")
    .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Eval { case_id, .. },
            ..
        }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
        _ => panic!("expected cluster eval with a selector"),
    }
    match try_parse_from(["curie", "cluster", "eval"])
        .expect("cluster eval should parse")
        .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Eval { case_id, .. },
            ..
        }) => assert!(case_id.is_empty()),
        _ => panic!("expected cluster eval"),
    }
}

#[test]
fn update_parses_with_and_without_image() {
    let bare = try_parse_from(["curie", "update"]).expect("update should parse");
    assert!(matches!(
        bare.command,
        Some(Command::Update { image: false })
    ));
    let with_image = try_parse_from(["curie", "update", "--image"]).expect("update should parse");
    assert!(matches!(
        with_image.command,
        Some(Command::Update { image: true })
    ));
}

#[test]
fn interactive_parses_with_aliases() {
    let cli = try_parse_from(["curie", "interactive"]).expect("interactive should parse");
    assert!(matches!(cli.command, Some(Command::Interactive)));
    let cli = try_parse_from(["curie", "ui"]).expect("ui alias should parse");
    assert!(matches!(cli.command, Some(Command::Interactive)));
    let cli = try_parse_from(["curie", "tui"]).expect("tui alias should parse");
    assert!(matches!(cli.command, Some(Command::Interactive)));
}

#[test]
fn secrets_subcommands_parse() {
    let cli = try_parse_from(["curie", "secrets", "set", "GITHUB_TOKEN"])
        .expect("secrets set should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Secrets {
            action: SecretsAction::Set { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "secrets",
        "set",
        "GITHUB_TOKEN",
        "--from-env",
        "TMP_TOKEN",
    ])
    .expect("secrets set --from-env should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Secrets {
            action: SecretsAction::Set {
                from_env: Some(_),
                ..
            }
        })
    ));
    let cli = try_parse_from(["curie", "secrets", "list"]).expect("secrets list should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Secrets {
            action: SecretsAction::List
        })
    ));
    let cli = try_parse_from(["curie", "secrets", "unset", "GITHUB_TOKEN"])
        .expect("secrets unset should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Secrets {
            action: SecretsAction::Unset { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "secrets",
        "set",
        "K8S_WRITE_KUBECONFIG",
        "--from-env",
        "K8S_WRITE_KUBECONFIG",
        "--cluster-identity",
        "ca:a",
        "--release",
        "curie",
        "--namespace",
        "curie-test",
        "--expected-version",
        "1",
    ])
    .expect("scoped secrets set should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Secrets {
            action: SecretsAction::Set {
                cluster_identity: Some(_),
                expected_version: Some(1),
                ..
            }
        })
    ));
}

#[test]
fn dev_subcommands_parse() {
    let cli = try_parse_from(["curie", "dev", "contracts"]).expect("dev contracts should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::Contracts
        })
    ));
    let cli =
        try_parse_from(["curie", "dev", "chart-check"]).expect("dev chart-check should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::ChartCheck
        })
    ));
    let cli = try_parse_from(["curie", "dev", "e2e"]).expect("dev e2e should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::E2e
        })
    ));
    let cli = try_parse_from(["curie", "dev", "docs-lint"]).expect("dev docs-lint should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::DocsLint
        })
    ));
    let cli =
        try_parse_from(["curie", "dev", "agent-skills"]).expect("dev agent-skills should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::AgentSkills
        })
    ));
    let cli = try_parse_from(["curie", "dev", "eval-falsifiability"])
        .expect("dev eval-falsifiability should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::EvalFalsifiability
        })
    ));
    let cli = try_parse_from(["curie", "dev", "e2e-ladder"]).expect("dev e2e-ladder should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::E2eLadder
        })
    ));
    let cli = try_parse_from(["curie", "dev", "two-release-approval-e2e"])
        .expect("dev two-release-approval-e2e should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::TwoReleaseApprovalE2e
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "factory-e2e",
        "run",
        "--scenario",
        "revision",
    ])
    .expect("dev factory-e2e should pass its mode and flags through");
    match cli.command {
        Some(Command::Dev {
            action: DevAction::FactoryE2e { args },
        }) => assert_eq!(args, ["run", "--scenario", "revision"]),
        _ => panic!("dev factory-e2e parsed as another command"),
    }
    assert!(try_parse_from(["curie", "dev", "factory-e2e"]).is_err());
    let cli = try_parse_from(["curie", "dev", "chart-runtime-e2e"])
        .expect("dev chart-runtime-e2e should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::ChartRuntimeE2e { force: false }
        })
    ));
    // The script's context guard names `--force` as its only override, so the
    // flag has to survive the `curie dev` hop or the guard is unoverridable
    // through the documented entry point.
    let cli = try_parse_from(["curie", "dev", "chart-runtime-e2e", "--force"])
        .expect("dev chart-runtime-e2e --force should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::ChartRuntimeE2e { force: true }
        })
    ));
    let cli =
        try_parse_from(["curie", "dev", "restore-drill"]).expect("dev restore-drill should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::RestoreDrill {
                check_backup: None,
                supplied_config: None,
                negative: None,
            }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "restore-drill",
        "--check-backup",
        "/tmp/backup",
        "--supplied-config",
        "/tmp/supplied.json",
        "--negative",
        "bundles",
    ])
    .expect("dev restore-drill flags should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::RestoreDrill {
                check_backup: Some(_),
                supplied_config: Some(_),
                negative: Some(_),
            }
        })
    ));
    let cli = try_parse_from(["curie", "dev", "recovery-drill"])
        .expect("dev recovery-drill should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::RecoveryDrill { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "recovery-drill",
        "--surface",
        "cluster",
        "--scenario",
        "worker-death",
        "--bound-seconds",
        "120",
        "--force",
    ])
    .expect("dev recovery-drill flags should parse");
    match cli.command {
        Some(Command::Dev {
            action:
                DevAction::RecoveryDrill {
                    surface,
                    scenario,
                    bound_seconds,
                    force,
                },
        }) => {
            assert_eq!(surface, "cluster");
            assert_eq!(scenario, "worker-death");
            assert_eq!(bound_seconds, 120);
            assert!(force);
        }
        _ => panic!("expected recovery-drill"),
    }
    let cli = try_parse_from(["curie", "dev", "lease-expiry-cluster-proof"])
        .expect("dev lease-expiry-cluster-proof should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::LeaseExpiryClusterProof { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "lease-expiry-cluster-proof",
        "--force",
        "--keep",
        "--self-test",
    ])
    .expect("dev lease-expiry-cluster-proof flags should parse");
    match cli.command {
        Some(Command::Dev {
            action:
                DevAction::LeaseExpiryClusterProof {
                    force,
                    keep,
                    self_test,
                },
        }) => {
            assert!(force);
            assert!(keep);
            assert!(self_test);
        }
        _ => panic!("expected lease-expiry-cluster-proof"),
    }
    let cli =
        try_parse_from(["curie", "dev", "upgrade-drill"]).expect("dev upgrade-drill should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::UpgradeDrill { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "upgrade-drill",
        "--scenario",
        "incompatible-rollback",
        "--also-predecessor",
        "--force",
        "--keep",
        "--self-test",
    ])
    .expect("dev upgrade-drill flags should parse");
    match cli.command {
        Some(Command::Dev {
            action:
                DevAction::UpgradeDrill {
                    scenario,
                    also_predecessor,
                    force,
                    keep,
                    self_test,
                },
        }) => {
            assert_eq!(scenario, "incompatible-rollback");
            assert!(also_predecessor);
            assert!(force);
            assert!(keep);
            assert!(self_test);
        }
        _ => panic!("expected upgrade-drill"),
    }
    let cli = try_parse_from(["curie", "dev", "release-accept"])
        .expect("dev release-accept should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::ReleaseAccept {
                ledger: None,
                self_test: false
            }
        })
    ));
    let cli = try_parse_from(["curie", "dev", "release-accept", "--self-test"])
        .expect("dev release-accept --self-test should parse");
    match cli.command {
        Some(Command::Dev {
            action: DevAction::ReleaseAccept { ledger, self_test },
        }) => {
            assert!(ledger.is_none());
            assert!(self_test);
        }
        _ => panic!("expected release-accept"),
    }
    let cli = try_parse_from(["curie", "dev", "release-accept", "--ledger", "ledger.json"])
        .expect("dev release-accept --ledger should parse");
    match cli.command {
        Some(Command::Dev {
            action: DevAction::ReleaseAccept { ledger, self_test },
        }) => {
            assert_eq!(ledger.as_deref(), Some(std::path::Path::new("ledger.json")));
            assert!(!self_test);
        }
        _ => panic!("expected release-accept"),
    }
    let cli = try_parse_from(["curie", "dev", "cluster-upgrade-matrix"])
        .expect("dev cluster-upgrade-matrix should parse");
    assert!(matches!(
        cli.command,
        Some(Command::Dev {
            action: DevAction::ClusterUpgradeMatrix { .. }
        })
    ));
    let cli = try_parse_from([
        "curie",
        "dev",
        "cluster-upgrade-matrix",
        "--scenario",
        "fail-every-phase",
        "--force",
        "--keep",
        "--self-test",
    ])
    .expect("dev cluster-upgrade-matrix flags should parse");
    match cli.command {
        Some(Command::Dev {
            action:
                DevAction::ClusterUpgradeMatrix {
                    scenario,
                    force,
                    keep,
                    self_test,
                },
        }) => {
            assert_eq!(scenario, "fail-every-phase");
            assert!(force);
            assert!(keep);
            assert!(self_test);
        }
        _ => panic!("expected cluster-upgrade-matrix"),
    }
}

// Proves the `dev` verb set is closed: an unrecognized verb must fail to
// parse rather than silently falling through. Without this negative case,
// a typo'd verb name (e.g. a future rename that missed a call site) could
// ship without ever being caught by the positive-path tests above.
#[test]
fn dev_unknown_subcommand_rejected() {
    let result = try_parse_from(["curie", "dev", "e2e-ladder-typo"]);
    assert!(result.is_err());
}

#[test]
fn local_message_accepts_api_key() {
    let cli = try_parse_from(["curie", "local", "message", "--api-key", "K", "hi"])
        .expect("local message --api-key should parse");
    match cli.command {
        Some(Command::Local {
            action: LocalAction::Message { api_key, .. },
        }) => assert_eq!(api_key, "K"),
        _ => panic!("expected local message command"),
    }
}

/// `cluster message` carries no dev-default sentinel for either credential
/// (#786): an omitted flag must parse to `None`, not a bound default, so the
/// handler discovers the release's own Secret instead.
#[test]
fn cluster_message_credentials_default_to_discovery() {
    let cli = try_parse_from(["curie", "cluster", "message", "hi"])
        .expect("cluster message should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Message {
                    api_key,
                    valkey_password,
                    ..
                },
            ..
        }) => {
            assert_eq!(api_key, None, "an omitted --api-key must not default");
            assert_eq!(
                valkey_password, None,
                "an omitted --valkey-password must not default"
            );
        }
        _ => panic!("expected cluster message command"),
    }
}

#[test]
fn cluster_message_accepts_explicit_credentials() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "message",
        "--api-key",
        "K",
        "--valkey-password",
        "P",
        "hi",
    ])
    .expect("cluster message with explicit credentials should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Message {
                    api_key,
                    valkey_password,
                    ..
                },
            ..
        }) => {
            assert_eq!(api_key, Some("K".to_string()));
            assert_eq!(valkey_password, Some("P".to_string()));
        }
        _ => panic!("expected cluster message command"),
    }
}

/// `cluster eval` had the identical defect (#790): it still bound the
/// dev-default sentinel for both credentials after #786 fixed `message`, so
/// mirror the same "no default, resolves to None" contract here.
#[test]
fn cluster_eval_credentials_default_to_discovery() {
    let cli = try_parse_from(["curie", "cluster", "eval"]).expect("cluster eval should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Eval {
                    api_key,
                    valkey_password,
                    ..
                },
            ..
        }) => {
            assert_eq!(api_key, None, "an omitted --api-key must not default");
            assert_eq!(
                valkey_password, None,
                "an omitted --valkey-password must not default"
            );
        }
        _ => panic!("expected cluster eval command"),
    }
}

#[test]
fn cluster_eval_accepts_explicit_credentials() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "eval",
        "--api-key",
        "K",
        "--valkey-password",
        "P",
    ])
    .expect("cluster eval with explicit credentials should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Eval {
                    api_key,
                    valkey_password,
                    ..
                },
            ..
        }) => {
            assert_eq!(api_key, Some("K".to_string()));
            assert_eq!(valkey_password, Some("P".to_string()));
        }
        _ => panic!("expected cluster eval command"),
    }
}

/// #1908: a red cluster eval used to leak a kubectl child bound to the
/// fixed 56381 default, so the next eval selected that same occupied port.
/// Omitted eval ports must request kernel-assigned 0, matching `cluster
/// message` (#1652 / #1740).
#[test]
fn cluster_eval_omitted_ports_default_to_ephemeral() {
    let cli = try_parse_from(["curie", "cluster", "eval"]).expect("cluster eval should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Eval {
                    listen_port,
                    valkey_local_port,
                    api_local_port,
                    ..
                },
            ..
        }) => {
            assert_eq!(listen_port, 0, "an omitted --listen-port must request 0");
            assert_eq!(
                valkey_local_port, 0,
                "an omitted --valkey-local-port must request 0"
            );
            assert_eq!(
                api_local_port, 0,
                "an omitted --api-local-port must request 0"
            );
        }
        _ => panic!("expected cluster eval command"),
    }
}

/// #1533 symptom 2: `cluster deploy` hardcoded `DEFAULT_API_LOCAL_PORT`
/// (8123) for its self-plumbed tunnel, so two concurrent deploys collided
/// and anything already holding 8123 broke the deploy. `cluster message`
/// and `cluster eval` were fixed by #1740 / #1652; deploy was the verb that
/// PR did not reach. An omitted flag must request a kernel-assigned port.
#[test]
fn cluster_deploy_defaults_api_local_port_to_zero() {
    let cli = try_parse_from(["curie", "cluster", "deploy"]).expect("cluster deploy should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Deploy { api_local_port, .. },
            ..
        }) => assert_eq!(
            api_local_port, 0,
            "an omitted --api-local-port must request a kernel-assigned port"
        ),
        _ => panic!("expected cluster deploy command"),
    }
}

/// The escape hatch stays exact: an explicit port is an override, not a
/// hint, so an operator can still pin a tunnel port (and get the #1739
/// occupied-port refusal when it is squatted).
#[test]
fn an_explicit_api_local_port_is_honoured() {
    let cli = try_parse_from(["curie", "cluster", "deploy", "--api-local-port", "18123"])
        .expect("cluster deploy with an explicit port should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Deploy { api_local_port, .. },
            ..
        }) => assert_eq!(api_local_port, 18123),
        _ => panic!("expected cluster deploy command"),
    }
}

#[test]
fn cluster_eval_preserves_explicit_port_overrides() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "eval",
        "--listen-port",
        "18155",
        "--valkey-local-port",
        "18156",
        "--api-local-port",
        "18157",
    ])
    .expect("cluster eval with explicit ports should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Eval {
                    listen_port,
                    valkey_local_port,
                    api_local_port,
                    ..
                },
            ..
        }) => {
            assert_eq!(listen_port, 18155);
            assert_eq!(valkey_local_port, 18156);
            assert_eq!(api_local_port, 18157);
        }
        _ => panic!("expected cluster eval command"),
    }
}

#[test]
fn cluster_deploy_defaults_to_proxy_discovery() {
    let cli = try_parse_from(["curie", "cluster", "deploy"]).expect("cluster deploy should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Deploy {
                    api_url,
                    namespace,
                    release,
                    ..
                },
            ..
        }) => {
            assert_eq!(api_url, None);
            assert_eq!(namespace, "curie");
            assert_eq!(release, "curie");
        }
        _ => panic!("expected cluster deploy command"),
    }
}

#[test]
fn cluster_deploy_accepts_explicit_api_url() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "deploy",
        "--api-url",
        "http://h:30080/api",
    ])
    .expect("cluster deploy --api-url should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Deploy { api_url, .. },
            ..
        }) => assert_eq!(api_url.as_deref(), Some("http://h:30080/api")),
        _ => panic!("expected cluster deploy command"),
    }
}

#[test]
fn cluster_deploy_captures_namespace_and_release() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "deploy",
        "--namespace",
        "ns1",
        "--release",
        "rel1",
    ])
    .expect("cluster deploy --namespace --release should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Deploy {
                namespace, release, ..
            },
            ..
        }) => {
            assert_eq!(namespace, "ns1");
            assert_eq!(release, "rel1");
        }
        _ => panic!("expected cluster deploy command"),
    }
}

// @spec ADR-0168 d8
#[test]
fn deploy_takes_an_identity_on_both_tiers() {
    for tier in ["local", "cluster"] {
        let cli = try_parse_from(["curie", tier, "deploy", "--identity", "ops-bot"]).unwrap();
        let identity = match cli.command {
            Some(Command::Local {
                action: LocalAction::Deploy { identity, .. },
            }) => identity,
            Some(Command::Cluster {
                action: ClusterAction::Deploy { identity, .. },
                ..
            }) => identity,
            _ => panic!("expected {tier} deploy"),
        };
        assert_eq!(identity.as_deref(), Some("ops-bot"));
    }
    assert!(
        try_parse_from([
            "curie",
            "cluster",
            "deploy",
            "--all-targets",
            "--identity",
            "x"
        ])
        .is_err(),
        "every target states its own identity"
    );
}

#[test]
fn local_short_file_flag_parses_for_all_verbs() {
    let cases = [
        (["curie", "local", "up", "-f", "custom.yaml"], "up"),
        (["curie", "local", "down", "-f", "custom.yaml"], "down"),
        (["curie", "local", "status", "-f", "custom.yaml"], "status"),
    ];

    for (argv, verb) in cases {
        let cli = try_parse_from(argv).expect("local verb accepts -f");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::Up { files, .. },
            }) => {
                assert_eq!(verb, "up");
                assert_eq!(files, vec!["custom.yaml"]);
            }
            Some(Command::Local {
                action: LocalAction::Down { files, .. },
            }) => {
                assert_eq!(verb, "down");
                assert_eq!(files, vec!["custom.yaml"]);
            }
            Some(Command::Local {
                action: LocalAction::Status { files, .. },
            }) => {
                assert_eq!(verb, "status");
                assert_eq!(files, vec!["custom.yaml"]);
            }
            _ => panic!("expected the local subcommand"),
        }
    }
}

#[test]
fn local_up_parses_minimal_flag() {
    let cli = try_parse_from(["curie", "local", "up", "--minimal"])
        .expect("local up --minimal should parse");
    match cli.command {
        Some(Command::Local {
            action: LocalAction::Up { minimal, .. },
        }) => assert!(minimal),
        _ => panic!("expected local up command"),
    }
}

#[test]
fn local_up_parses_slack_flag() {
    let cli =
        try_parse_from(["curie", "local", "up", "--slack"]).expect("local up --slack should parse");
    match cli.command {
        Some(Command::Local {
            action: LocalAction::Up { slack, .. },
        }) => assert!(slack),
        _ => panic!("expected local up command"),
    }
}

#[test]
fn local_comms_parses_slack_disconnect_and_app_token() {
    let cli = try_parse_from([
        "curie",
        "local",
        "comms",
        "--slack",
        "--disconnect",
        "--app-token",
        "X",
    ])
    .expect("local comms flags should parse");
    match cli.command {
        Some(Command::Local {
            action:
                LocalAction::Comms {
                    slack,
                    disconnect,
                    app_token,
                    ..
                },
        }) => {
            assert!(slack);
            assert!(disconnect);
            assert_eq!(app_token, "X");
        }
        _ => panic!("expected local comms command"),
    }
}

#[tokio::test]
async fn resolve_cluster_conn_prefers_explicit_over_discovery() {
    // #524: an explicit --api-url/--api-key (or env) wins and short-circuits
    // discovery entirely -- no kubectl is shelled, so this resolves with no
    // cluster. (The discovery branch self-plumbs a loopback tunnel through
    // commands::deploy_api_tunnel.)
    let conn = ClusterConn {
        api_url: Some("https://api.example.test".into()),
        api_key: Some("real-release-key".into()),
        namespace: "curie".into(),
        release: "curie".into(),
    };
    let (url, key, port_forward) = resolve_cluster_conn(conn, false)
        .await
        .expect("explicit conn resolves");
    assert_eq!(url, "https://api.example.test");
    assert_eq!(key, "real-release-key");
    assert!(port_forward.is_none());
}

#[test]
fn cluster_governance_verbs_take_namespace_and_release() {
    // The discovery flags exist on a cluster governance verb so an omitted
    // --api-url/--api-key can be resolved from the named release (#524).
    let cli = try_parse_from([
        "curie",
        "cluster",
        "versions",
        "demo",
        "--namespace",
        "prod",
        "--release",
        "acme",
    ])
    .expect("cluster versions with --namespace/--release should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Versions { target },
            ..
        }) => {
            assert_eq!(target.agent, "demo");
            assert_eq!(target.conn.namespace, "prod");
            assert_eq!(target.conn.release, "acme");
            assert!(
                target.conn.api_url.is_none(),
                "omitted --api-url stays None for discovery"
            );
        }
        _ => panic!("expected cluster versions"),
    }
}

#[test]
fn cluster_kill_parses_agent_and_yes() {
    let cli = try_parse_from(["curie", "cluster", "kill", "deal-desk", "--yes"])
        .expect("cluster kill should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Kill { agent, yes, .. },
            ..
        }) => {
            assert_eq!(agent, "deal-desk");
            assert!(yes);
        }
        _ => panic!("expected cluster kill command"),
    }
}

#[test]
fn cluster_kill_defaults_yes_and_dry_run_off() {
    let cli = try_parse_from(["curie", "cluster", "kill", "a"])
        .expect("cluster kill without flags should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Kill {
                    agent,
                    yes,
                    dry_run,
                    ..
                },
            ..
        }) => {
            assert_eq!(agent, "a");
            assert!(!yes);
            assert!(!dry_run);
        }
        _ => panic!("expected cluster kill command"),
    }
}

#[test]
fn cluster_resume_parses_agent_and_dry_run() {
    let cli = try_parse_from(["curie", "cluster", "resume", "a", "--dry-run"])
        .expect("cluster resume should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Resume { agent, dry_run, .. },
            ..
        }) => {
            assert_eq!(agent, "a");
            assert!(dry_run);
        }
        _ => panic!("expected cluster resume command"),
    }
}

#[test]
fn cluster_budget_parses_agent_and_limit() {
    let cli = try_parse_from(["curie", "cluster", "budget", "a", "--limit", "12.5"])
        .expect("cluster budget should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Budget {
                    agent,
                    limit,
                    output_tokens,
                    ..
                },
            ..
        }) => {
            assert_eq!(agent, "a");
            assert_eq!(limit, Some(12.5));
            assert_eq!(output_tokens, None);
        }
        _ => panic!("expected cluster budget command"),
    }
}

#[test]
fn cluster_reset_thread_parses_agent_thread_key_and_yes() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "reset-thread",
        "deal-desk",
        "--thread-key",
        "1234.5678",
        "--yes",
    ])
    .expect("cluster reset-thread should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::ResetThread {
                    agent,
                    thread_key,
                    yes,
                    ..
                },
            ..
        }) => {
            assert_eq!(agent, "deal-desk");
            assert_eq!(thread_key, "1234.5678");
            assert!(yes);
        }
        _ => panic!("expected cluster reset-thread command"),
    }
}

#[test]
fn cluster_reset_thread_defaults_yes_and_dry_run_off() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "reset-thread",
        "a",
        "--thread-key",
        "t1",
    ])
    .expect("cluster reset-thread without flags should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::ResetThread {
                    agent,
                    thread_key,
                    yes,
                    dry_run,
                    ..
                },
            ..
        }) => {
            assert_eq!(agent, "a");
            assert_eq!(thread_key, "t1");
            assert!(!yes);
            assert!(!dry_run);
        }
        _ => panic!("expected cluster reset-thread command"),
    }
}

#[test]
fn cluster_reset_thread_requires_thread_key() {
    // --thread-key has no default; omitting it must be a parse error.
    assert!(try_parse_from(["curie", "cluster", "reset-thread", "a", "--yes"]).is_err());
}

#[test]
fn local_platform_verbs_parse() {
    // The inspection/governance verbs mirrored onto the local tier.
    assert!(matches!(
        try_parse_from(["curie", "local", "versions", "gh"])
            .expect("local versions")
            .command,
        Some(Command::Local {
            action: LocalAction::Versions { .. }
        })
    ));
    assert!(matches!(
        try_parse_from(["curie", "local", "memory", "gh"])
            .expect("local memory")
            .command,
        Some(Command::Local {
            action: LocalAction::Memory { .. }
        })
    ));
    assert!(matches!(
        try_parse_from(["curie", "local", "observability"])
            .expect("local observability")
            .command,
        Some(Command::Local {
            action: LocalAction::Observability { .. }
        })
    ));
    // local budget/kill/resume are the mirrored lifecycle verbs.
    assert!(try_parse_from(["curie", "local", "budget", "gh", "--limit", "1"]).is_ok());
    assert!(try_parse_from(["curie", "local", "kill", "gh", "--yes"]).is_ok());
}

#[test]
fn local_memory_add_parses_agent_and_content() {
    let cli = try_parse_from([
        "curie",
        "local",
        "memory",
        "translation-bot",
        "--add",
        "ask before translating to French",
    ])
    .expect("local memory --add should parse");
    match cli.command {
        Some(Command::Local {
            action: LocalAction::Memory { target, add, .. },
        }) => {
            assert_eq!(target.agent, "translation-bot");
            assert_eq!(add.as_deref(), Some("ask before translating to French"));
        }
        _ => panic!("expected local memory"),
    }
}

#[test]
fn cluster_memory_add_parses_agent_and_content() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "memory",
        "translation-bot",
        "--add",
        "ask before translating to French",
    ])
    .expect("cluster memory --add should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Memory { target, add, .. },
            ..
        }) => {
            assert_eq!(target.agent, "translation-bot");
            assert_eq!(add.as_deref(), Some("ask before translating to French"));
        }
        _ => panic!("expected cluster memory"),
    }
}

#[test]
fn local_memory_list_still_parses_without_add() {
    assert!(matches!(
        try_parse_from(["curie", "local", "memory", "translation-bot"])
            .expect("local memory list")
            .command,
        Some(Command::Local {
            action: LocalAction::Memory { add: None, .. }
        })
    ));
}

#[test]
fn local_reset_thread_parses_agent_thread_key_and_yes() {
    let cli = try_parse_from([
        "curie",
        "local",
        "reset-thread",
        "gh",
        "--thread-key",
        "1234.5678",
        "--yes",
    ])
    .expect("local reset-thread should parse");
    match cli.command {
        Some(Command::Local {
            action:
                LocalAction::ResetThread {
                    agent,
                    thread_key,
                    yes,
                    dry_run,
                    ..
                },
        }) => {
            assert_eq!(agent, "gh");
            assert_eq!(thread_key, "1234.5678");
            assert!(yes);
            assert!(!dry_run);
        }
        _ => panic!("expected local reset-thread command"),
    }
}

#[test]
fn local_reset_thread_dry_run_skips_yes_requirement_at_parse_time() {
    // --dry-run parses fine without --yes; the refusal-without-yes check
    // happens in commands::reset_thread, not at the clap layer.
    let cli = try_parse_from([
        "curie",
        "local",
        "reset-thread",
        "gh",
        "--thread-key",
        "t1",
        "--dry-run",
    ])
    .expect("local reset-thread --dry-run should parse");
    match cli.command {
        Some(Command::Local {
            action: LocalAction::ResetThread { dry_run, yes, .. },
        }) => {
            assert!(dry_run);
            assert!(!yes);
        }
        _ => panic!("expected local reset-thread command"),
    }
}

// -----------------------------------------------------------------------
// Observability twin (issue #460): the `--open` gate is agent-first, so it
// must default OFF on both tiers, and `--json` is the global flag from #456.
// -----------------------------------------------------------------------

#[test]
fn local_observability_open_flag_defaults_off_and_parses() {
    // Bare `local observability` must NOT open a browser (agent-first default).
    match try_parse_from(["curie", "local", "observability"])
        .expect("local observability")
        .command
    {
        Some(Command::Local {
            action: LocalAction::Observability { open, .. },
        }) => assert!(!open, "--open must default to false"),
        _ => panic!("expected local observability command"),
    }
    // `--open` is the explicit human opt-in.
    match try_parse_from(["curie", "local", "observability", "--open"])
        .expect("local observability --open")
        .command
    {
        Some(Command::Local {
            action: LocalAction::Observability { open, .. },
        }) => assert!(open, "--open must parse to true"),
        _ => panic!("expected local observability command"),
    }
}

#[test]
fn cluster_observability_parses_with_namespace_release_defaults() {
    match try_parse_from(["curie", "cluster", "observability"])
        .expect("cluster observability")
        .command
    {
        Some(Command::Cluster {
            action:
                ClusterAction::Observability {
                    namespace,
                    release,
                    dry_run,
                    open,
                    ..
                },
            ..
        }) => {
            assert_eq!(namespace, "curie");
            assert_eq!(release, "curie");
            assert!(!dry_run);
            assert!(!open, "--open must default to false");
        }
        _ => panic!("expected cluster observability command"),
    }
}

#[test]
fn cluster_observability_accepts_the_global_json_flag() {
    // `--json` is a GLOBAL flag on `Cli` (issue #456), not a subcommand flag,
    // so it parses onto the top-level struct while the subcommand still binds.
    let cli = try_parse_from(["curie", "cluster", "observability", "--json"])
        .expect("cluster observability --json");
    assert!(cli.json, "--json must set the global json flag");
    assert!(matches!(
        cli.command,
        Some(Command::Cluster {
            action: ClusterAction::Observability { .. },
            ..
        })
    ));
}

#[test]
fn cluster_observability_parses_open_and_dry_run_together() {
    match try_parse_from(["curie", "cluster", "observability", "--open", "--dry-run"])
        .expect("cluster observability --open --dry-run")
        .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Observability { dry_run, open, .. },
            ..
        }) => {
            assert!(dry_run, "--dry-run must parse to true");
            assert!(open, "--open must parse to true");
        }
        _ => panic!("expected cluster observability command"),
    }
}

#[test]
fn approvals_parses_repeatable_gate_and_clear() {
    let cli = try_parse_from([
        "curie",
        "local",
        "approvals",
        "gh",
        "--gate",
        "Bash",
        "--gate",
        "mcp__x__y",
    ])
    .expect("local approvals should parse");
    match cli.command {
        Some(Command::Local {
            action:
                LocalAction::Approvals {
                    target,
                    gate,
                    clear,
                    ..
                },
        }) => {
            assert_eq!(target.agent, "gh");
            assert_eq!(gate, vec!["Bash".to_string(), "mcp__x__y".to_string()]);
            assert!(!clear);
        }
        _ => panic!("expected local approvals command"),
    }
    // --clear parses on both tiers.
    assert!(try_parse_from(["curie", "cluster", "approvals", "gh", "--clear"]).is_ok());
}

#[test]
fn budget_requires_limit_or_output_tokens_on_both_tiers() {
    for tier in ["local", "cluster"] {
        assert!(try_parse_from(["curie", tier, "budget", "a"]).is_err());
    }
}

#[test]
fn budget_parses_selected_limits_on_both_tiers() {
    for tier in ["local", "cluster"] {
        for (flags, expected_limit, expected_tokens) in [
            (vec!["--limit", "12.5"], Some(12.5), None),
            (vec!["--output-tokens", "96000"], None, Some(96000)),
            (
                vec!["--limit", "12.5", "--output-tokens", "96000"],
                Some(12.5),
                Some(96000),
            ),
        ] {
            let mut args = vec!["curie", tier, "budget", "a"];
            args.extend(flags);
            let cli = try_parse_from(args).expect("selected budget limits should parse");
            let (agent, limit, output_tokens) = match cli.command {
                Some(Command::Local {
                    action:
                        LocalAction::Budget {
                            agent,
                            limit,
                            output_tokens,
                            ..
                        },
                })
                | Some(Command::Cluster {
                    action:
                        ClusterAction::Budget {
                            agent,
                            limit,
                            output_tokens,
                            ..
                        },
                    ..
                }) => (agent, limit, output_tokens),
                _ => panic!("expected budget command"),
            };
            assert_eq!(agent, "a");
            assert_eq!(limit, expected_limit);
            assert_eq!(output_tokens, expected_tokens);
        }
    }
}

#[test]
fn cluster_delete_parses_agent_and_yes() {
    let cli = try_parse_from(["curie", "cluster", "delete", "a", "--yes"])
        .expect("cluster delete should parse");
    match cli.command {
        Some(Command::Cluster {
            action: ClusterAction::Delete { agent, yes, .. },
            ..
        }) => {
            assert_eq!(agent, "a");
            assert!(yes);
        }
        _ => panic!("expected cluster delete command"),
    }
}

#[test]
fn skill_approvals_parses_plugin_dir_and_repeatable_gate() {
    let cli = try_parse_from([
        "curie",
        "skill",
        "approvals",
        "--plugin-dir",
        "/tmp/bundle",
        "--gate",
        "A",
        "--gate",
        "B",
    ])
    .expect("skill approvals should parse");
    match cli.command {
        Some(Command::Skill {
            action:
                SkillAction::Approvals {
                    plugin_dir,
                    gate,
                    clear,
                    ..
                },
        }) => {
            assert_eq!(plugin_dir, std::path::PathBuf::from("/tmp/bundle"));
            assert_eq!(gate, vec!["A".to_string(), "B".to_string()]);
            assert!(!clear);
        }
        _ => panic!("expected skill approvals command"),
    }
}

#[test]
fn skill_approvals_parses_clear() {
    let cli = try_parse_from(["curie", "skill", "approvals", "--clear"])
        .expect("skill approvals --clear should parse");
    match cli.command {
        Some(Command::Skill {
            action: SkillAction::Approvals { gate, clear, .. },
        }) => {
            assert!(clear);
            assert!(gate.is_empty());
        }
        _ => panic!("expected skill approvals command"),
    }
}

#[test]
fn skill_approvals_clear_and_gate_parse_ok_at_clap_layer() {
    // The --clear + --gate conflict is a RUNTIME usage error (asserted in the
    // commands handler tests), not a clap parse error.
    assert!(try_parse_from(["curie", "skill", "approvals", "--clear", "--gate", "X"]).is_ok());
}

#[test]
fn skill_versions_parses_as_a_known_verb() {
    // The verb EXISTS at the skill tier (answered, not a clap unknown
    // subcommand): parsing succeeds and the runtime reports unavailability.
    assert!(matches!(
        try_parse_from(["curie", "skill", "versions"])
            .expect("skill versions should parse")
            .command,
        Some(Command::Skill {
            action: SkillAction::Versions
        })
    ));
}

#[test]
fn cluster_deploy_accepts_secret_flag() {
    // `--secret` must PARSE (not error like a typo). Cluster delivery is
    // implemented (#1488); this lock is the clap surface, not the helm path.
    match try_parse_from([
        "curie",
        "cluster",
        "deploy",
        "--secret",
        "GITHUB_PERSONAL_ACCESS_TOKEN",
    ])
    .expect("cluster deploy --secret should parse")
    .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Deploy { secret, .. },
            ..
        }) => assert_eq!(secret, vec!["GITHUB_PERSONAL_ACCESS_TOKEN"]),
        _ => panic!("expected cluster deploy"),
    }
    // Bare cluster deploy still parses with no secrets.
    match try_parse_from(["curie", "cluster", "deploy"])
        .expect("bare cluster deploy should parse")
        .command
    {
        Some(Command::Cluster {
            action: ClusterAction::Deploy { secret, .. },
            ..
        }) => assert!(secret.is_empty()),
        _ => panic!("expected cluster deploy"),
    }
}

#[test]
fn skill_memory_parses_as_a_known_verb() {
    assert!(matches!(
        try_parse_from(["curie", "skill", "memory"])
            .expect("skill memory should parse")
            .command,
        Some(Command::Skill {
            action: SkillAction::Memory
        })
    ));
}

#[test]
fn cluster_comms_parses_slack_disconnect_and_app_token() {
    let cli = try_parse_from([
        "curie",
        "cluster",
        "comms",
        "--slack",
        "--disconnect",
        "--app-token",
        "X",
    ])
    .expect("cluster comms flags should parse");
    match cli.command {
        Some(Command::Cluster {
            action:
                ClusterAction::Comms {
                    slack,
                    disconnect,
                    app_token,
                    ..
                },
            ..
        }) => {
            assert!(slack);
            assert!(disconnect);
            assert_eq!(app_token, "X");
        }
        _ => panic!("expected cluster comms command"),
    }
}

#[test]
fn developer_model_script_passes_provider_flags_through() {
    let cli = try_parse_from([
        "curie",
        "dev",
        "model-script",
        "serve",
        "--transcript",
        "transcript.json",
    ])
    .expect("dev model-script should pass its flags through");
    match cli.command {
        Some(Command::Dev {
            action: DevAction::ModelScript { args },
        }) => assert_eq!(args, ["serve", "--transcript", "transcript.json"]),
        _ => panic!("dev model-script parsed as another command"),
    }
    assert!(try_parse_from(["curie", "dev", "model-script"]).is_err());
}
