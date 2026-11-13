//! Offline CLI rendering and target selection for the investigation bundle.

use std::process::Command;

use serde_json::Value;

#[test]
fn renders_exact_bundle_with_explicit_target_and_one_json_object() {
    let temporary = tempfile::tempdir().unwrap();
    let out = temporary.path().join("bundle");
    let result = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["--json", "example", "factory-triage", "render", "--out"])
        .arg(&out)
        .args(["--implementation-label", "ready"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let receipt: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(receipt["rendered"], true);
    assert_eq!(receipt["implementation_label"], "ready");
    assert_eq!(receipt["bundle_dir"], out.to_str().unwrap());
    for (name, embedded) in curie::examples::FACTORY_TRIAGE_BUNDLE_FILES {
        let rendered = std::fs::read(out.join(name)).unwrap();
        if *name == "workflow/result.json" {
            let mut expected: Value = serde_json::from_slice(embedded).unwrap();
            expected["implementation_label"] = Value::String("ready".into());
            assert_eq!(
                serde_json::from_slice::<Value>(&rendered).unwrap(),
                expected
            );
        } else {
            assert_eq!(rendered, *embedded, "{name}");
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(out.join("hooks/reproduction_gate.py"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0o111
        );
    }
}

#[test]
fn requires_label_and_refuses_nonempty_directory_without_overwriting() {
    let temporary = tempfile::tempdir().unwrap();
    let sentinel = temporary.path().join("keep");
    std::fs::write(&sentinel, "preserved").unwrap();
    for label in [None, Some("ready"), Some(""), Some(" ")] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args(["--json", "example", "factory-triage", "render", "--out"])
            .arg(temporary.path());
        if let Some(label) = label {
            command.args(["--implementation-label", label]);
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "preserved");
    }
}

#[test]
fn every_source_file_is_embedded_and_source_bytes_match() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/factory-triage");
    fn files(root: &std::path::Path, current: &std::path::Path, names: &mut Vec<String>) {
        for entry in std::fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.file_name().unwrap() == "__pycache__" {
                continue;
            }
            if path.is_dir() {
                files(root, &path, names);
            } else {
                names.push(path.strip_prefix(root).unwrap().to_str().unwrap().into());
            }
        }
    }
    let mut source = Vec::new();
    files(&root, &root, &mut source);
    let embedded = curie::examples::FACTORY_TRIAGE_BUNDLE_FILES;
    assert_eq!(source.len(), embedded.len());
    for name in source {
        let (_, bytes) = embedded
            .iter()
            .find(|(entry, _)| *entry == name)
            .expect("embedded source file");
        assert_eq!(std::fs::read(root.join(name)).unwrap(), *bytes);
    }
}
