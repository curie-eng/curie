use std::fs;
use std::process::Command;

use curie::factory_capabilities::{
    bundle_input_digest, manifest_from_observations, CapabilityInputs, Executable, ProbeSpec,
    PROBE_SCRIPT,
};

#[test]
fn manifest_requires_every_declared_command_and_keeps_subcommands_distinct() {
    let inputs = CapabilityInputs {
        required_executables: vec![
            ProbeSpec {
                name: "cargo".into(),
                command: vec!["cargo".into(), "--version".into()],
            },
            ProbeSpec {
                name: "cargo fmt".into(),
                command: vec!["cargo".into(), "fmt".into(), "--version".into()],
            },
        ],
        registry_hosts: vec!["index.crates.io".into()],
        services: vec![],
        egress_class: "restricted".into(),
    };
    let observed = vec![Executable {
        name: "cargo".into(),
        version: "1.95.0".into(),
        path: "/usr/local/cargo/bin/cargo".into(),
        command: vec!["cargo".into(), "--version".into()],
    }];
    let error = manifest_from_observations(
        &inputs,
        observed,
        "acme/runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "b".repeat(64),
        vec![],
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("cargo fmt"));
    assert!(error.to_string().contains("cargo fmt --version"));
}

#[test]
fn bundle_digest_excludes_only_the_generated_manifest() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("verification")).unwrap();
    fs::write(dir.path().join("plugin.json"), "first").unwrap();
    fs::write(dir.path().join("verification/capabilities.json"), "old").unwrap();
    let initial = bundle_input_digest(dir.path()).unwrap();
    fs::write(dir.path().join("verification/capabilities.json"), "new").unwrap();
    assert_eq!(initial, bundle_input_digest(dir.path()).unwrap());
    fs::write(dir.path().join("plugin.json"), "second").unwrap();
    assert_ne!(initial, bundle_input_digest(dir.path()).unwrap());
}

#[test]
fn bundle_digest_matches_shared_cross_language_vector() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/vectors/factory-capability-bundle.json"
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    for (name, text) in vector["files"].as_object().unwrap() {
        let path = dir.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text.as_str().unwrap()).unwrap();
    }
    assert_eq!(
        bundle_input_digest(dir.path()).unwrap(),
        vector["expected_sha256"]
    );
}

#[test]
fn image_probe_script_observes_real_binary_and_refuses_missing_one() {
    let observed = vec![ProbeSpec {
        name: "python3".into(),
        command: vec!["python3".into(), "--version".into()],
    }];
    let run = |probes: &[ProbeSpec]| {
        Command::new("python3")
            .args(["-c", PROBE_SCRIPT, &serde_json::to_string(probes).unwrap()])
            .output()
            .unwrap()
    };
    let success = run(&observed);
    assert!(
        success.status.success(),
        "{}",
        String::from_utf8_lossy(&success.stderr)
    );
    let result: Vec<Executable> = serde_json::from_slice(&success.stdout).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "python3");
    let missing = ProbeSpec {
        name: "missing".into(),
        command: vec!["curie-absent-tool".into(), "--version".into()],
    };
    let failure = run(&[observed[0].clone(), missing]);
    assert!(!failure.status.success());
    let stderr = String::from_utf8_lossy(&failure.stderr);
    assert!(stderr.contains("missing"));
    assert!(stderr.contains("curie-absent-tool --version"));
}

#[test]
fn manifest_keeps_declared_probe_order_for_api_validation() {
    let commands = [
        ProbeSpec {
            name: "z".into(),
            command: vec!["python3".into(), "--version".into()],
        },
        ProbeSpec {
            name: "a".into(),
            command: vec!["node".into(), "--version".into()],
        },
    ];
    let inputs = CapabilityInputs {
        required_executables: commands.to_vec(),
        registry_hosts: vec![],
        services: vec![],
        egress_class: "restricted".into(),
    };
    let observed = commands
        .iter()
        .map(|item| Executable {
            name: item.name.clone(),
            version: "1.0".into(),
            path: "/usr/bin/tool".into(),
            command: item.command.clone(),
        })
        .collect();
    let manifest = manifest_from_observations(
        &inputs,
        observed,
        "acme/runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "b".repeat(64),
        vec![],
        None,
    )
    .unwrap();
    let value = serde_json::to_value(manifest).unwrap();
    assert_eq!(value["executables"][0]["name"], "z");
    assert_eq!(value["executables"][1]["name"], "a");
}
