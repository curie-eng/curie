use std::process::Command;

#[test]
fn hook_wake_shared_cases_through_real_build() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/vectors/hook-wake-declarations.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".claude-plugin")).unwrap();
        std::fs::write(
            dir.path().join(".claude-plugin/plugin.json"),
            serde_json::to_vec(&case["manifest"]).unwrap(),
        )
        .unwrap();
        if let Some(connectors) = case.get("connectors") {
            std::fs::write(
                dir.path().join("connectors.yaml"),
                serde_json::to_vec(connectors).unwrap(),
            )
            .unwrap();
        }
        let output = Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(["build", "--plugin-dir"])
            .arg(dir.path())
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            case["valid"].as_bool().unwrap(),
            "case {}: stdout={}, stderr={}",
            case["id"],
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if output.status.success() {
            let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(receipt["connectors"], serde_json::json!([]));
        } else {
            let code = case["codes"][0].as_str().unwrap();
            assert!(
                String::from_utf8_lossy(&output.stdout).contains(code)
                    || String::from_utf8_lossy(&output.stderr).contains(code),
                "case {} did not refuse with {}",
                case["id"],
                code
            );
        }
        println!("{}: {}", case["id"], output.status.code().unwrap());
    }
}
