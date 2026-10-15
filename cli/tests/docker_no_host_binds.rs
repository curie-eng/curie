use std::path::PathBuf;

use curie::docker::{CheckSpec, StartSpec};

fn client_host_binds(args: &[String]) -> Vec<String> {
    if !matches!(args.first().map(String::as_str), Some("run" | "create")) {
        return Vec::new();
    }
    let mut violations = Vec::new();
    let mut index = 1;
    while index < args.len() {
        let arg = args[index].as_str();
        let (kind, value) = if matches!(arg, "-v" | "--volume" | "--mount") {
            index += 1;
            (arg, args.get(index).map(String::as_str).unwrap_or(""))
        } else if let Some(value) = arg.strip_prefix("--volume=") {
            ("--volume", value)
        } else if let Some(value) = arg.strip_prefix("--mount=") {
            ("--mount", value)
        } else if let Some(value) = arg.strip_prefix("-v").filter(|value| !value.is_empty()) {
            ("-v", value)
        } else {
            index += 1;
            continue;
        };
        let forbidden = if kind == "--mount" {
            let fields: Vec<(&str, &str)> = value
                .split(',')
                .filter_map(|field| field.split_once('='))
                .collect();
            let mount_type = fields.iter().find(|(key, _)| *key == "type");
            let source = fields
                .iter()
                .find(|(key, _)| matches!(*key, "src" | "source"));
            match mount_type.map(|(_, value)| *value) {
                Some("bind") => !source.is_some_and(|(_, source)| daemon_path(source)),
                Some("tmpfs") => false,
                _ => source.is_some_and(|(_, source)| !named_volume(source)),
            }
        } else {
            value.split_once(':').is_some_and(|(source, _)| {
                !named_volume(source) && !daemon_path(source)
            })
        };
        if forbidden {
            violations.push(value.to_string());
        }
        index += 1;
    }
    violations
}

fn named_volume(source: &str) -> bool {
    !source.is_empty()
        && source.starts_with(|c: char| c.is_ascii_alphanumeric())
        && source
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn daemon_path(source: &str) -> bool {
    matches!(source, "/var/run/docker.sock" | "/dev/kmsg")
}

fn runner_spec() -> StartSpec {
    StartSpec {
        image: "curie-runner".into(),
        container_name: "curie-runner-acme-test".into(),
        host_port: 7245,
        plugin_dir: PathBuf::from("/tmp/acme-bot/.curie/snapshots/example"),
        session_id: "acme-session".into(),
        sandbox_id: "acme-sandbox".into(),
        budget_json: "{}".into(),
        fake_model: true,
        network: None,
        otel_endpoint: None,
        model_base_url: None,
        model: None,
        passthrough_env: Vec::new(),
        docker_env: Vec::new(),
    }
}

#[test]
fn default_runner_argv_has_no_client_host_bind() {
    let args = runner_spec().run_args();
    assert!(client_host_binds(&args).is_empty(), "client bind in {args:?}");
}

#[test]
fn default_check_argv_has_no_client_host_bind() {
    let args = CheckSpec {
        image: "curie-runner".into(),
        plugin_dir: "/tmp/acme-bot/.curie/snapshots/example".into(),
        timeout_s: 30,
    }
    .run_args();
    assert!(client_host_binds(&args).is_empty(), "client bind in {args:?}");
}

#[test]
fn argv_guard_rejects_absolute_and_relative_client_paths() {
    for mount in [
        "/tmp/acme-bot:/plugin:ro",
        "/home/acme/checkout:/plugin:ro",
        "./acme-bot:/plugin:ro",
        "../acme-bot:/plugin:ro",
        "bundles/acme-bot:/plugin:ro",
    ] {
        for flag in ["-v", "--volume"] {
            let args = ["run", flag, mount, "curie-runner"].map(String::from);
            assert_eq!(client_host_binds(&args), vec![mount], "{args:?}");
        }
        for arg in [format!("--volume={mount}"), format!("-v{mount}")] {
            let args = vec!["create".into(), arg, "curie-runner".into()];
            assert_eq!(client_host_binds(&args), vec![mount], "{args:?}");
        }
    }
    for mount in [
        "type=bind,src=/tmp/acme-bot,dst=/plugin,readonly",
        "source=./acme-bot,target=/plugin,type=bind",
        "type=volume,source=../acme-bot,target=/plugin",
    ] {
        let args = ["run", "--mount", mount, "curie-runner"].map(String::from);
        assert_eq!(client_host_binds(&args), vec![mount], "{args:?}");
        let args = vec!["run".into(), format!("--mount={mount}"), "curie-runner".into()];
        assert_eq!(client_host_binds(&args), vec![mount], "{args:?}");
    }
}

#[test]
fn argv_guard_accepts_named_volumes_tmpfs_and_daemon_devices() {
    for mount in [
        "curie-snapshot-acme:/plugin:ro",
        "acme.model_cache-1:/root/.ollama",
        "/var/run/docker.sock:/var/run/docker.sock",
        "/dev/kmsg:/dev/kmsg",
        "/plugin",
    ] {
        let args = ["run", "-v", mount, "curie-runner"].map(String::from);
        assert!(client_host_binds(&args).is_empty(), "{args:?}");
    }
    for mount in [
        "type=volume,source=curie-snapshot-acme,target=/plugin,readonly",
        "type=tmpfs,target=/tmp",
        "type=bind,src=/dev/kmsg,dst=/dev/kmsg",
        "type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock",
    ] {
        let args = ["run", "--mount", mount, "curie-runner"].map(String::from);
        assert!(client_host_binds(&args).is_empty(), "{args:?}");
    }
    let args = ["run", "--device", "/dev/kmsg:/dev/kmsg", "curie-runner"].map(String::from);
    assert!(client_host_binds(&args).is_empty());
    let args = ["compose", "down", "-v"].map(String::from);
    assert!(client_host_binds(&args).is_empty());
}
