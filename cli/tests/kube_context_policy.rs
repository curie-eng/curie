use std::process::Command;

#[test]
fn context_policy_exercises_literal_rejection_and_variable_acceptance_through_cli() {
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["dev", "kube-context-check", "--self-test"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .output()
        .expect("run kube-context policy through the CLI");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("literals rejected"), "{stdout}");
    assert!(
        stdout.contains("variables/placeholders accepted"),
        "{stdout}"
    );
}
