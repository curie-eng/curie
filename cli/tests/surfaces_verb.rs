//! Integration: the `surfaces` verb at BOTH tiers (ADR-0116, #1525).
//!
//! An agent may hold more than one channel binding, so the CLI needs a verb
//! that adds and removes one binding at a time. These tests drive the BUILT
//! BINARY against an in-process mock of the platform API, so they cover the
//! whole path an operator actually runs: clap parsing, the flag-conflict gate,
//! the `ApiClient` call, and the `CliOutput` that comes back.
//!
//! Why the built binary rather than `commands::channels(...)` directly: the
//! defect class this file exists for is clap wiring (`cli/tests/overrides_clap_wiring.rs`
//! documents the same gap for `overrides` -- swapping two arguments at the call
//! site left that whole suite green). A direct call cannot see which flag became
//! which argument, nor that `--add` and `--remove` conflict at parse time.
//!
//! BOTH tiers are covered because the local/cluster verb pair is a parity seam
//! (AGENTS.md): a verb landed on one tier and missed on the other is the drift
//! the seam registry exists to catch. Per that rule the secondary path
//! (cluster) additionally gets a case that arms a behavior through it ALONE --
//! `cluster_surfaces_rejects_add_and_remove_before_resolving_the_connection`,
//! which is meaningless at the local tier because only the cluster tier has a
//! connection to discover.
//!
//! FAIL-FIRST until S4.8a-S4.8f land: the verb does not exist, so clap answers
//! every one of these with "unrecognized subcommand". The usage test below is
//! written to distinguish THAT exit 2 from the exit 2 it actually asserts.

mod support;

use std::process::Command;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "44444444-4444-4444-4444-444444444444";
const AGENT_NAME: &str = "deal-desk";
/// The binding the fixture agent already holds.
const BOUND: &str = "C0EXAMPLE1";
/// A second binding, the one `--add` asks for.
const OTHER: &str = "C0EXAMPLE2";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn channels_path() -> String {
    format!("/agents/{AGENT_ID}/channels")
}

/// One agent's wire JSON with its plural `channels` array (ADR-0116).
fn agent_json(channels: &[&str]) -> String {
    let bindings = channels
        .iter()
        .map(|c| format!(r#"{{"kind":"slack","address":"{c}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[{bindings}],"created_at":"2026-07-05T00:00:00Z"}}"#
    )
}

/// A mock platform API that answers the agent read and whichever binding write
/// the test expects. `write` is consulted first, so a test can override the
/// subresource's answer (a 409, say) without restating the read.
fn api(write: impl Fn(&str, &str) -> Option<Response> + Send + Sync + 'static) -> MockServer {
    serve(move |req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        if let Some(response) = write(m, p) {
            return response;
        }
        match (m, p) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json(&[BOUND]))),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => {
                Response::json(200, &agent_json(&[BOUND]))
            }
            // Answered rather than panicked, so a write the CLI should NOT have
            // issued is RECORDED and the assertion about it is what fails. A
            // panicking handler never records the request, which makes every
            // "no write happened" check vacuous (see `cli/tests/api_deploy.rs`).
            _ => Response::json(200, &agent_json(&[BOUND, OTHER])),
        }
    })
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn output(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Run the binary with `argv`. `CURIE_API_URL`/`CURIE_API_KEY` are removed from
/// the child's environment so an operator's shell cannot change what these
/// tests assert.
fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// Every request the CLI issued, as `METHOD PATH BODY`.
fn traffic(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .map(|r| {
            format!(
                "{} {} {}",
                r.method,
                r.path,
                String::from_utf8_lossy(&r.body)
            )
        })
        .collect()
}

/// Requests that MUTATE a binding: the subresource POST/PATCH/DELETE, plus any
/// PATCH of the agent row (`AgentUpdate.channel` is retired, so a binding write
/// that lands there is a defect, not an alternative spelling).
fn writes(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| {
            format!(
                "{} {} {}",
                r.method,
                r.path,
                String::from_utf8_lossy(&r.body)
            )
        })
        .collect()
}

// --- local tier ---------------------------------------------------------------

#[test]
fn local_surfaces_add_posts_the_pair_to_the_subresource() {
    let server = api(|m, p| {
        (m == "POST" && p == channels_path())
            .then(|| Response::json(201, &agent_json(&[BOUND, OTHER])))
    });

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--add",
        &format!("slack={OTHER}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_eq!(run.code, 0, "add must succeed: {}", run.output());

    let posts: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path == channels_path())
        .collect();
    assert_eq!(
        posts.len(),
        1,
        "exactly one binding add: {:?}",
        traffic(&server)
    );
    let body: serde_json::Value =
        serde_json::from_slice(&posts[0].body).expect("the add body should be JSON");
    // Exact equality: a stray key, or a bare address string standing in for the
    // pair, both fail. The kind is never inferred -- that is the point of a
    // channel-neutral binding.
    assert_eq!(
        body,
        serde_json::json!({"kind": "slack", "address": OTHER}),
        "the POST carries the pair and nothing else"
    );
}

#[test]
fn local_surfaces_add_forwards_a_write_only_reply_route() {
    let server = api(|m, p| {
        (m == "POST" && p == channels_path())
            .then(|| Response::json(201, &agent_json(&[BOUND, OTHER])))
    });

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--add",
        "discord=111111111111111111",
        "--endpoint",
        "https://discord-adapter.example.com/replies",
        "--adapter",
        "discord-main",
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_eq!(
        run.code,
        0,
        "Discord surface add must succeed: {}",
        run.output()
    );

    let post = server
        .recorded()
        .into_iter()
        .find(|request| request.method == "POST" && request.path == channels_path())
        .expect("one surface POST");
    let body: serde_json::Value = serde_json::from_slice(&post.body).expect("JSON body");
    assert_eq!(
        body,
        serde_json::json!({
            "kind": "discord",
            "address": "111111111111111111",
            "endpoint": "https://discord-adapter.example.com/replies",
            "adapter": "discord-main",
        })
    );
}

#[test]
fn local_surfaces_remove_deletes_the_pair() {
    let server = api(|m, p| {
        (m == "DELETE" && p.starts_with(&channels_path())).then(|| Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        })
    });

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--remove",
        &format!("slack={BOUND}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_eq!(run.code, 0, "remove must succeed: {}", run.output());

    let deletes: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "DELETE")
        .collect();
    assert_eq!(
        deletes.len(),
        1,
        "exactly one binding removal: {:?}",
        traffic(&server)
    );
    assert!(
        deletes[0].path.starts_with(&channels_path()),
        "the removal targets the agent's channel subresource: {}",
        deletes[0].path
    );
    // The pair identifies WHICH binding to drop. Whether it travels in the
    // query string or the body is the implementation's call; that both halves
    // travel is not, because a removal that names only the agent would drop the
    // wrong binding on a multi-binding agent -- the exact case this verb exists
    // for.
    let selector = format!(
        "{} {}",
        deletes[0].path,
        String::from_utf8_lossy(&deletes[0].body)
    );
    assert!(
        selector.contains("slack") && selector.contains(BOUND),
        "the removal must name the pair, got {selector}"
    );
}

#[test]
fn local_surfaces_list_reads_without_writing() {
    let server = api(|_, _| None);

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_eq!(run.code, 0, "a list must succeed: {}", run.output());
    assert!(
        run.stdout.contains(BOUND),
        "the list must show the bound channel: {}",
        run.output()
    );
    // No flags means INSPECT. A list that writes would make `channels <agent>`
    // unsafe to run for a look, which is the one thing an operator does most.
    assert!(
        writes(&server).is_empty(),
        "a list must issue no write, got {:?}",
        writes(&server)
    );
}

#[test]
fn local_surfaces_json_carries_the_agent_and_its_bindings() {
    let server = api(|m, p| {
        (m == "POST" && p == channels_path())
            .then(|| Response::json(201, &agent_json(&[BOUND, OTHER])))
    });

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--add",
        &format!("slack={OTHER}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
        "--json",
    ]);
    assert_eq!(run.code, 0, "add --json must succeed: {}", run.output());

    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "--json must emit exactly one JSON object: {e}; stdout: {}",
            run.stdout
        )
    });
    assert_eq!(value["agent"], AGENT_NAME);
    // `channels` is an ARRAY OF PAIRS, not a string and not a list of bare
    // addresses: an agent consumer must be able to read the kind without
    // guessing it, which is the whole shape ADR-0116 introduced.
    assert_eq!(
        value["surfaces"],
        serde_json::json!([
            {"kind": "slack", "address": BOUND},
            {"kind": "slack", "address": OTHER},
        ]),
        "the emitted bindings must be the ones the API stored: {value}"
    );
    // `changed` tells an agent consumer "this is what it NOW is" apart from
    // "this is what it is", without diffing.
    assert_eq!(value["changed"], serde_json::Value::Bool(true));
}

#[test]
fn local_surfaces_surfaces_a_409_when_removing_the_last_binding() {
    // An agent with no binding answers nowhere, so the API refuses to remove
    // the last one. That refusal must reach the operator with its reason
    // intact: a bare "failed" here sends them hunting for a CLI bug.
    let server = api(|m, p| {
        (m == "DELETE" && p.starts_with(&channels_path())).then(|| {
            Response::json(
                409,
                r#"{"detail":"an agent must keep at least one channel binding"}"#,
            )
        })
    });

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--remove",
        &format!("slack={BOUND}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_ne!(run.code, 0, "a refused removal must not exit 0");
    let out = run.output();
    assert!(
        out.contains("at least one channel binding"),
        "the API's reason must survive to the operator: {out}"
    );
}

#[test]
fn surfaces_rejects_add_and_remove_together_with_exit_2_and_no_http() {
    // Exactly one `--add` OR one `--remove` per invocation. Two mutations in
    // one command would be a transaction the API has no batch endpoint for, so
    // a half-applied run would leave the operator guessing what took. clap's
    // `conflicts_with` refuses it at PARSE time, which is why no request is
    // issued: an operator who mistyped should not wait on a network round trip
    // to be told so.
    let server = api(|_, _| None);

    let run = run(&[
        "local",
        "surfaces",
        AGENT_NAME,
        "--add",
        &format!("slack={OTHER}"),
        "--remove",
        &format!("slack={BOUND}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);

    assert_eq!(run.code, 2, "a contradictory invocation is a USAGE error");
    let out = run.output();
    // An unrecognized subcommand ALSO exits 2 with no HTTP, so without this the
    // test would pass against a binary that has no `surfaces` verb at all --
    // green while asserting nothing.
    assert!(
        !out.contains("unrecognized subcommand") && !out.contains("unexpected argument"),
        "exit 2 must come from the flag conflict, not from an unknown verb: {out}"
    );
    assert!(
        out.contains("--add") && out.contains("--remove"),
        "the usage error must name both flags: {out}"
    );
    assert!(
        server.recorded().is_empty(),
        "a usage error must precede every network call, got {:?}",
        traffic(&server)
    );
}

// --- cluster tier -------------------------------------------------------------

#[test]
fn cluster_surfaces_add_posts_the_pair_to_the_subresource() {
    // The parity twin of the local add. `channels` is an agent-lifecycle verb
    // like `overrides`/`kill`/`delete`, so it talks through the API connection
    // rather than self-plumbing a tunnel; an explicit `--api-url`/`--api-key`
    // wins over discovery, so no cluster is touched here.
    let server = api(|m, p| {
        (m == "POST" && p == channels_path())
            .then(|| Response::json(201, &agent_json(&[BOUND, OTHER])))
    });

    let run = run(&[
        "cluster",
        "surfaces",
        AGENT_NAME,
        "--add",
        &format!("slack={OTHER}"),
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
    ]);
    assert_eq!(run.code, 0, "cluster add must succeed: {}", run.output());

    let posts: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path == channels_path())
        .collect();
    assert_eq!(
        posts.len(),
        1,
        "exactly one binding add: {:?}",
        traffic(&server)
    );
    let body: serde_json::Value =
        serde_json::from_slice(&posts[0].body).expect("the add body should be JSON");
    assert_eq!(body, serde_json::json!({"kind": "slack", "address": OTHER}));
}

#[test]
fn cluster_surfaces_rejects_add_and_remove_before_resolving_the_connection() {
    // CLUSTER-ONLY, and the reason the cluster tier is not merely a copy: with
    // no `--api-url`/`--api-key`, this verb DISCOVERS them from the release by
    // shelling out to kubectl. Resolving the flag conflict first is what keeps
    // a typo from costing a cluster round trip (and, with no cluster reachable,
    // from being reported as a connection failure instead of the typo it is).
    //
    // `KUBECONFIG` points at a path that cannot exist, so discovery cannot
    // quietly succeed: if the conflict were checked second, the failure would
    // be about the cluster and this assertion would catch it.
    let output = Command::new(bin())
        .args([
            "cluster",
            "surfaces",
            AGENT_NAME,
            "--add",
            &format!("slack={OTHER}"),
            "--remove",
            &format!("slack={BOUND}"),
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .expect("run curie cluster surfaces");
    let code = output.status.code().unwrap_or(-1);
    let text = String::from_utf8_lossy(&output.stdout).into_owned()
        + &String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        code, 2,
        "a contradictory invocation is a USAGE error: {text}"
    );
    assert!(
        !text.contains("unrecognized subcommand") && !text.contains("unexpected argument"),
        "exit 2 must come from the flag conflict, not from an unknown verb: {text}"
    );
    assert!(
        text.contains("--add") && text.contains("--remove"),
        "the usage error must name both flags: {text}"
    );
    assert!(
        !text.contains("kubectl") && !text.contains("kubeconfig"),
        "the flag conflict must be settled before any cluster lookup: {text}"
    );
}
