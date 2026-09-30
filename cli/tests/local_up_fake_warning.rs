#[path = "support/executable.rs"]
mod test_executable;

use std::fs;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

#[test]
fn local_up_without_credentials_warns_about_fake_model() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    let tools = temp.path().join("tools");
    fs::create_dir(&tools).expect("create tools directory");
    test_executable::install_in(&tools, "docker", "#!/bin/sh\nexit 0\n");

    let compose_file = temp.path().join("compose.yaml");
    fs::write(&compose_file, "services: {}\n").expect("write compose file");

    let mut paths = vec![tools];
    if let Some(path) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&path));
    }
    let path = std::env::join_paths(paths).expect("join PATH");

    let output = Command::new(bin())
        .current_dir(temp.path())
        .args([
            "--color=never",
            "local",
            "up",
            "--minimal",
            "-f",
            compose_file.to_str().expect("compose path is UTF 8"),
        ])
        .env("PATH", path)
        .env("LC_ALL", "C")
        .env("CURIE_CONFIG_DIR", temp.path().join("config"))
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .env_remove("CURIE_FAKE_MODEL")
        .output()
        .expect("run curie local up");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "local up must succeed\nstdout: {}\nstderr: {stderr}",
        String::from_utf8_lossy(&output.stdout)
    );

    let guidance = "Running the fake model (no credential set). Provide a credential (ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN / CURIE_CREDENTIALS) or --local-model to go live.";
    assert!(
        stderr.contains(guidance),
        "stderr must retain the fake model recovery guidance\n{stderr}"
    );

    let expected_warning = format!("! warn  {guidance}");
    let warning_count = stderr
        .lines()
        .filter(|line| *line == expected_warning.as_str())
        .count();
    assert_eq!(
        warning_count, 1,
        "stderr must report the fake model fallback as one warning\n{stderr}"
    );
}
