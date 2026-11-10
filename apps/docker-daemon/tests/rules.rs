//! One refused case and at least one allowed neighbor per policy rule, driven
//! through `authorize_parts` (and `authorize` for request-level rules).

use curie_docker_authz::policy::{authorize, authorize_parts, Decision};
use serde_json::{json, Value};

const CREATE: &str = "/v1.56/containers/create";
const EXEC: &str = "/v1.56/containers/c1/exec";
const NET_CREATE: &str = "/v1.56/networks/create";
const NET_CONNECT: &str = "/v1.56/networks/n1/connect";
const VOL_CREATE: &str = "/v1.56/volumes/create";

// ---------------------------------------------------------------- helpers

#[track_caller]
fn assert_refused(d: Decision, rule: &str) {
    assert!(
        !d.allow && d.rule == rule,
        "expected refusal by rule {rule}, got {d:?}"
    );
}

#[track_caller]
fn assert_allowed(d: Decision) {
    assert!(d.allow, "expected allow, got {d:?}");
}

fn parts(method: &str, uri: &str, body: Option<&[u8]>) -> Decision {
    authorize_parts(method, uri, body)
}

fn get(uri: &str) -> Decision {
    parts("GET", uri, None)
}

fn post(uri: &str) -> Decision {
    parts("POST", uri, None)
}

fn post_raw(uri: &str, body: &str) -> Decision {
    parts("POST", uri, Some(body.as_bytes()))
}

fn post_json(uri: &str, body: &Value) -> Decision {
    let bytes = serde_json::to_vec(body).unwrap();
    parts("POST", uri, Some(&bytes))
}

/// Container create with the given HostConfig.
fn create(host_config: Value) -> Decision {
    post_json(
        CREATE,
        &json!({"Image": "alpine:3.20", "Cmd": ["true"], "HostConfig": host_config}),
    )
}

/// Container create from a raw body string, for duplicate and folded keys
/// that a serde_json map cannot represent.
fn create_raw(body: &str) -> Decision {
    post_raw(CREATE, body)
}

/// Container create whose HostConfig object is the given raw JSON text.
fn create_hc_raw(host_config: &str) -> Decision {
    create_raw(&format!(
        r#"{{"Image":"alpine:3.20","HostConfig":{host_config}}}"#
    ))
}

fn mount(m: Value) -> Decision {
    create(json!({"Mounts": [m]}))
}

fn merge(base: Value, patch: Value) -> Value {
    let mut base = base;
    let obj = base.as_object_mut().unwrap();
    for (k, v) in patch.as_object().unwrap() {
        obj.insert(k.clone(), v.clone());
    }
    base
}

/// Body the CLI sends for `docker exec` (recorded cli-and-compose index 12).
fn exec_base() -> Value {
    json!({"User": "", "Privileged": false, "Tty": false, "AttachStdin": false,
           "AttachStderr": true, "AttachStdout": true, "DetachKeys": "",
           "Env": null, "WorkingDir": "", "Cmd": ["true"]})
}

fn exec(patch: Value) -> Decision {
    post_json(EXEC, &merge(exec_base(), patch))
}

/// Body the CLI sends for `docker network create n1` (recorded index 37).
fn net_base() -> Value {
    json!({"Name": "n1", "Driver": "bridge", "Scope": "",
           "IPAM": {"Driver": "default", "Options": {}, "Config": []},
           "Internal": false, "Attachable": false, "Ingress": false,
           "ConfigOnly": false, "ConfigFrom": null, "Options": {}, "Labels": {}})
}

fn net_create(patch: Value) -> Decision {
    post_json(NET_CREATE, &merge(net_base(), patch))
}

/// Body the CLI sends for `docker network connect n1 c1` (recorded index 38).
fn connect_base() -> Value {
    json!({"Container": "c1", "EndpointConfig": {"IPAMConfig": {}, "Links": null,
           "Aliases": [], "DriverOpts": {}, "GwPriority": 0, "NetworkID": "",
           "EndpointID": "", "Gateway": "", "IPAddress": "", "MacAddress": "",
           "IPPrefixLen": 0, "IPv6Gateway": "", "GlobalIPv6Address": "",
           "GlobalIPv6PrefixLen": 0, "DNSNames": null}})
}

fn connect(patch: Value) -> Decision {
    post_json(NET_CONNECT, &merge(connect_base(), patch))
}

fn vol_create(patch: Value) -> Decision {
    post_json(
        VOL_CREATE,
        &merge(json!({"Driver": "local", "Name": "v1"}), patch),
    )
}

fn b64_encode(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            A[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

// ---------------------------------------------------- request-unparseable

#[test]
fn request_unparseable_envelopes_are_refused() {
    for raw in [
        &b"not json"[..],
        b"",
        b"[]",
        b"\"GET\"",
        b"{}",
        br#"{"RequestUri":"/_ping"}"#,
        br#"{"RequestMethod":"GET"}"#,
        br#"{"RequestMethod":1,"RequestUri":"/_ping"}"#,
        br#"{"RequestMethod":"GET","RequestUri":["/_ping"]}"#,
        br#"{"RequestMethod":"POST","RequestUri":"/v1.56/containers/create","RequestBody":"@@@@"}"#,
        br#"{"RequestMethod":"GET","RequestUri":"/_ping"} trailing"#,
    ] {
        let d = authorize(raw);
        assert!(
            !d.allow && d.rule == "request-unparseable",
            "{:?} -> {d:?}",
            String::from_utf8_lossy(raw)
        );
    }
}

#[test]
fn request_unparseable_neighbors_are_allowed() {
    assert_allowed(authorize(
        br#"{"RequestMethod":"HEAD","RequestUri":"/_ping"}"#,
    ));
    // The shape dockerd really sends: headers present, no User fields on tcp.
    assert_allowed(authorize(
        br#"{"RequestMethod":"GET","RequestUri":"/v1.56/info","RequestHeaders":{"User-Agent":"Docker-Client/29.9.0 (linux)"}}"#,
    ));
    let body = br#"{"Image":"alpine:3.20","HostConfig":{"Privileged":false}}"#;
    let env = json!({"RequestMethod": "POST", "RequestUri": CREATE,
                     "RequestHeaders": {"Content-Type": "application/json"},
                     "RequestBody": b64_encode(body)});
    assert_allowed(authorize(&serde_json::to_vec(&env).unwrap()));
}

#[test]
fn authorize_decodes_request_body_before_policy() {
    let body = br#"{"Image":"alpine:3.20","HostConfig":{"Privileged":true}}"#;
    let env = json!({"RequestMethod": "POST", "RequestUri": CREATE,
                     "RequestBody": b64_encode(body)});
    assert_refused(authorize(&serde_json::to_vec(&env).unwrap()), "privileged");
    // No RequestBody on a json endpoint is body-required, not unparseable.
    let env = json!({"RequestMethod": "POST", "RequestUri": CREATE});
    assert_refused(
        authorize(&serde_json::to_vec(&env).unwrap()),
        "body-required",
    );
}

// ------------------------------------------------------------ uri-invalid

#[test]
fn uri_invalid_bad_percent_encoding() {
    assert_refused(get("/v1.56/containers/%zz/json"), "uri-invalid");
    assert_refused(get("/v1.56/containers/c1%/json"), "uri-invalid");
    assert_refused(get("/v1.56/containers/c1%2/json"), "uri-invalid");
    // Decodes to a byte sequence that is not UTF-8.
    assert_refused(get("/v1.56/containers/%ff%fe/json"), "uri-invalid");
}

#[test]
fn uri_invalid_unclean_paths() {
    for uri in [
        "/v1.56/containers//json",
        "/v1.56/containers/c1/../../plugins",
        "/v1.56/images/alpine/./json",
        "/v1.56/containers/c1/.",
        "/v1.56/containers/c1/..",
        "/v1.56/containers/c1/%2e%2e/json",
        "/v1.56/containers/c1%2f%2e%2e%2fjson",
    ] {
        let d = get(uri);
        assert!(!d.allow && d.rule == "uri-invalid", "{uri} -> {d:?}");
    }
}

#[test]
fn uri_invalid_query() {
    assert_refused(get("/v1.56/containers/json?all=1;x=2"), "uri-invalid");
    // Old Go ParseQuery split on `;`; never let it smuggle a second value.
    assert_refused(
        post("/v1.56/images/create?fromImage=alpine;fromSrc=-"),
        "uri-invalid",
    );
    assert_refused(get("/v1.56/containers/json?all=%zz"), "uri-invalid");
}

#[test]
fn uri_neighbors_are_allowed() {
    assert_allowed(get("/v1.56/containers/json?all=1"));
    assert_allowed(get("/v1.56/containers/c1/json"));
    assert_allowed(get(
        "/v1.56/containers/json?all=1&filters=%7B%22label%22%3A%7B%22com.docker.compose.project%3Dp1%22%3Atrue%7D%7D",
    ));
    // Dots inside a segment are not path traversal.
    assert_allowed(get("/v1.56/images/alpine:3.20/json"));
    assert_allowed(get("/v1.56/volumes/my..vol"));
}

#[test]
fn version_prefix_is_optional() {
    assert_allowed(parts("HEAD", "/_ping", None));
    assert_allowed(get("/_ping"));
    assert_allowed(get("/v1.56/_ping"));
    assert_allowed(get("/info"));
    assert_allowed(get("/v1.56/info"));
    assert_allowed(get("/v1.41/containers/json"));
    assert_allowed(post_raw("/containers/create", r#"{"Image":"alpine:3.20"}"#));
    assert_refused(
        post_raw(
            "/containers/create",
            r#"{"HostConfig":{"Privileged":true}}"#,
        ),
        "privileged",
    );
    assert_refused(
        post_raw(
            "/v1.24/containers/create",
            r#"{"HostConfig":{"Privileged":true}}"#,
        ),
        "privileged",
    );
    assert_refused(get("/plugins"), "endpoint-not-allowed");
    assert_refused(get("/v1.56/plugins"), "endpoint-not-allowed");
}

#[test]
fn percent_encoded_path_is_decoded_before_matching() {
    // Moby's router matches the decoded path (plan wire fact 3), so
    // /containers/%63reate IS container create and must be body checked.
    assert_refused(
        post_raw(
            "/v1.56/containers/%63reate",
            r#"{"HostConfig":{"Privileged":true}}"#,
        ),
        "privileged",
    );
    assert_refused(post("/v1.56/containers/%63reate"), "body-required");
    assert_refused(post("/containers/%63reate"), "body-required");
    assert_allowed(post_raw(
        "/v1.56/containers/%63reate",
        r#"{"Image":"alpine:3.20"}"#,
    ));
    assert_refused(post("/v1.56/networks/%63reate"), "body-required");
    // An encoded slash turns this into exec create.
    assert_refused(
        post_json(
            "/v1.56/containers/c1%2Fexec",
            &merge(exec_base(), json!({"Privileged": true})),
        ),
        "exec-privileged",
    );
    assert_refused(get("/v1.56/%70lugins"), "endpoint-not-allowed");
}

// --------------------------------------------------- endpoint-not-allowed

#[test]
fn every_endpoint_table_row_is_allowed() {
    let rows: &[(&str, &str)] = &[
        ("GET", "/_ping"),
        ("HEAD", "/_ping"),
        ("GET", "/version"),
        ("GET", "/info"),
        ("GET", "/events?since=1791834787&until=1791834907"),
        ("GET", "/containers/json"),
        ("GET", "/containers/c1/json"),
        ("GET", "/containers/c1/logs?stderr=1&stdout=1"),
        ("GET", "/containers/c1/top"),
        ("GET", "/containers/c1/stats?stream=false"),
        ("GET", "/containers/c1/archive?path=%2Ftmp%2Ff"),
        ("HEAD", "/containers/c1/archive?path=%2Ftmp%2Fin.txt"),
        (
            "PUT",
            "/containers/c1/archive?noOverwriteDirNonDir=true&path=%2Ftmp",
        ),
        ("POST", "/containers/c1/start"),
        ("POST", "/containers/c1/stop?t=1"),
        ("POST", "/containers/c1/kill"),
        ("POST", "/containers/c1/restart?t=1"),
        ("POST", "/containers/c1/wait?condition=next-exit"),
        ("POST", "/containers/c1/attach?stderr=1&stdout=1&stream=1"),
        ("POST", "/containers/c1/resize?h=24&w=80"),
        ("DELETE", "/containers/c1?force=1&v=1"),
        ("POST", "/exec/65ca4250efec/start"),
        ("POST", "/exec/65ca4250efec/resize?h=24&w=80"),
        ("GET", "/exec/65ca4250efec/json"),
        ("POST", "/images/create?fromImage=alpine&tag=3.20"),
        ("POST", "/session"),
        ("POST", "/grpc"),
        ("GET", "/images/json?manifests=1"),
        ("GET", "/images/t1/json"),
        (
            "POST",
            "/images/t1/tag?repo=docker.io%2Flibrary%2Ft2&tag=latest",
        ),
        ("DELETE", "/images/t2"),
        ("GET", "/networks"),
        ("GET", "/networks/n1"),
        ("POST", "/networks/n1/disconnect"),
        ("DELETE", "/networks/n1"),
        ("GET", "/volumes"),
        ("GET", "/volumes/v1"),
        ("DELETE", "/volumes/v1?force=1"),
    ];
    for &(m, path) in rows {
        for prefix in ["", "/v1.56"] {
            let uri = format!("{prefix}{path}");
            let d = parts(m, &uri, None);
            assert!(d.allow, "{m} {uri} -> {d:?}");
        }
    }
}

#[test]
fn endpoints_outside_the_table_are_refused() {
    let rows: &[(&str, &str)] = &[
        ("GET", "/plugins"),
        ("GET", "/plugins/does-not-exist/json"),
        ("POST", "/plugins/pull?remote=x"),
        ("GET", "/system/df"),
        ("POST", "/containers/prune"),
        ("POST", "/images/prune"),
        ("POST", "/volumes/prune"),
        ("POST", "/networks/prune"),
        ("POST", "/build/prune"),
        ("POST", "/build"),
        ("POST", "/build?dockerfile=Dockerfile&q=1&version=1"),
        ("POST", "/build/cancel?id=x"),
        ("POST", "/commit?container=c1&repo=x"),
        ("POST", "/containers/c1/update"),
        ("GET", "/containers/c1/export"),
        ("GET", "/containers/c1/changes"),
        ("POST", "/images/x/push"),
        ("GET", "/images/get?names=x"),
        ("POST", "/images/load"),
        ("POST", "/auth"),
        ("GET", "/swarm"),
        ("POST", "/swarm/init"),
        ("GET", "/nodes"),
        ("GET", "/services"),
        ("GET", "/secrets"),
        ("GET", "/configs"),
        ("GET", "/tasks"),
        ("GET", "/distribution/alpine/json"),
        ("OPTIONS", "/_ping"),
        ("POST", "/_ping"),
        ("DELETE", "/_ping"),
        ("PUT", "/containers/create"),
        ("GET", "/containers/create"),
        ("POST", "/containers/c1/json"),
        ("GET", "/"),
        ("GET", ""),
    ];
    for &(m, path) in rows {
        let uri = if path.is_empty() {
            String::new()
        } else {
            format!("/v1.56{path}")
        };
        let d = parts(m, &uri, None);
        assert!(!d.allow, "{m} {uri} -> {d:?}");
        if !path.is_empty() && path != "/" {
            assert_eq!(d.rule, "endpoint-not-allowed", "{m} {uri} -> {d:?}");
        }
    }
}

#[test]
fn endpoint_refusal_message_names_method_and_path() {
    let d = get("/v1.56/system/df");
    assert_refused(d.clone(), "endpoint-not-allowed");
    assert!(
        d.msg.starts_with("refused by rule endpoint-not-allowed"),
        "{d:?}"
    );
    assert!(d.msg.contains("GET"), "{d:?}");
    assert!(d.msg.contains("/system/df"), "{d:?}");
}

#[test]
fn plugins_refusal_message_mentions_type_flag() {
    // `docker inspect <missing>` falls through to /plugins (wire fact 5).
    for uri in ["/v1.56/plugins/does-not-exist/json", "/v1.56/plugins"] {
        let d = get(uri);
        assert_refused(d.clone(), "endpoint-not-allowed");
        assert!(d.msg.contains("--type"), "{uri} -> {d:?}");
    }
}

#[test]
fn literal_paths_win_over_wildcards() {
    assert_allowed(get("/v1.56/containers/json"));
    assert_refused(post("/v1.56/networks/create"), "body-required");
    assert_refused(post("/v1.56/volumes/create"), "body-required");
    assert_refused(post("/v1.56/containers/create"), "body-required");
}

#[test]
fn image_names_with_slashes() {
    // Image names travel unescaped with `/` (wire fact 3).
    assert_allowed(get("/v1.56/images/docker.io/library/alpine:3.20/json"));
    assert_allowed(get("/v1.56/images/ghcr.io/org/team/img:1/json?manifests=1"));
    assert_allowed(get("/v1.56/images/sha256:8932d529d1eaeb4dc3c62f554a8a12f0bcb64a48037701170d727a743fde6e82/json"));
    assert_allowed(post(
        "/v1.56/images/ghcr.io/org/img:1/tag?repo=docker.io%2Flibrary%2Ft2&tag=latest",
    ));
    assert_allowed(parts("DELETE", "/v1.56/images/ghcr.io/org/img:1", None));
    assert_allowed(parts("DELETE", "/v1.56/images/x:1", None));
    // A slash-bearing image name does not open push.
    assert_refused(
        post("/v1.56/images/ghcr.io/org/img/push"),
        "endpoint-not-allowed",
    );
}

// ---------------------------------------------------------- body-required

#[test]
fn body_required_on_each_json_endpoint() {
    for uri in [CREATE, EXEC, NET_CREATE, NET_CONNECT, VOL_CREATE] {
        let d = parts("POST", uri, None);
        assert!(!d.allow && d.rule == "body-required", "{uri} None -> {d:?}");
        let d = parts("POST", uri, Some(b""));
        assert!(
            !d.allow && d.rule == "body-required",
            "{uri} empty -> {d:?}"
        );
    }
}

#[test]
fn body_required_neighbors_with_realistic_bodies_are_allowed() {
    assert_allowed(create(json!({"NetworkMode": "default"})));
    assert_allowed(exec(json!({})));
    assert_allowed(net_create(json!({})));
    assert_allowed(connect(json!({})));
    assert_allowed(vol_create(json!({})));
    // An empty object is valid for every json endpoint (Docker then rejects
    // a create with no Image itself).
    for uri in [CREATE, EXEC, NET_CREATE, NET_CONNECT, VOL_CREATE] {
        let d = post_raw(uri, "{}");
        assert!(d.allow, "{uri} {{}} -> {d:?}");
    }
}

#[test]
fn non_json_endpoints_accept_empty_and_absent_bodies() {
    for (m, uri) in [
        ("POST", "/v1.56/containers/c1/start"),
        (
            "POST",
            "/v1.56/containers/c1/attach?stderr=1&stdout=1&stream=1",
        ),
        ("PUT", "/v1.56/containers/c1/archive?path=%2Ftmp"),
        ("POST", "/grpc"),
        ("POST", "/session"),
        ("POST", "/v1.56/networks/n1/disconnect"),
    ] {
        let d = parts(m, uri, None);
        assert!(d.allow, "{m} {uri} None -> {d:?}");
        let d = parts(m, uri, Some(b""));
        assert!(d.allow, "{m} {uri} empty -> {d:?}");
    }
    // exec start carries a small JSON body the policy does not inspect
    // (recorded cli-and-compose index 13).
    assert_allowed(post_raw(
        "/v1.56/exec/abc/start",
        r#"{"Detach":false,"Tty":false}"#,
    ));
    assert_allowed(post_raw(
        "/v1.56/networks/n1/disconnect",
        r#"{"Container":"c1","Force":false}"#,
    ));
}

// -------------------------------------------------------- body-unparseable

#[test]
fn body_unparseable_invalid_json() {
    for body in [
        "{",
        "not json",
        r#"{"Image":"alpine"} trailing"#,
        r#"{"Image":"alpine"}{}"#,
        r#"{"Image":"alpine"},"#,
        "[]",
        "\"x\"",
    ] {
        let d = create_raw(body);
        assert!(!d.allow && d.rule == "body-unparseable", "{body} -> {d:?}");
    }
    for uri in [EXEC, NET_CREATE, NET_CONNECT, VOL_CREATE] {
        let d = post_raw(uri, r#"{"Name":"x"} {"#);
        assert!(!d.allow && d.rule == "body-unparseable", "{uri} -> {d:?}");
    }
}

#[test]
fn body_unparseable_invalid_utf8() {
    let d = parts("POST", CREATE, Some(b"{\"Image\":\"\xff\xfe\"}"));
    assert_refused(d, "body-unparseable");
    let d = parts("POST", VOL_CREATE, Some(b"{\"Name\":\"v\xc0\"}"));
    assert_refused(d, "body-unparseable");
}

#[test]
fn body_unparseable_wrong_types() {
    for hc in [
        json!({"Privileged": 1}),
        json!({"Privileged": 0}),
        json!({"Privileged": "true"}),
        json!({"Privileged": "false"}),
        json!({"CapAdd": "SYS_ADMIN"}),
        json!({"CapAdd": [1]}),
        json!({"Binds": "/:/host"}),
        json!({"NetworkMode": 5}),
        json!({"Mounts": {"Type": "bind"}}),
        json!({"SecurityOpt": "label=disable"}),
        json!({"LogConfig": "syslog"}),
    ] {
        let d = create(hc.clone());
        assert!(!d.allow && d.rule == "body-unparseable", "{hc} -> {d:?}");
    }
    assert_refused(create(json!("privileged")), "body-unparseable");
    assert_refused(create(json!([{"Privileged": true}])), "body-unparseable");
    assert_refused(exec(json!({"Privileged": 1})), "body-unparseable");
    assert_refused(net_create(json!({"Driver": 1})), "body-unparseable");
    assert_refused(net_create(json!({"Options": "x"})), "body-unparseable");
    assert_refused(vol_create(json!({"Driver": ["nfs"]})), "body-unparseable");
}

#[test]
fn body_unparseable_neighbors_are_allowed() {
    assert_allowed(create(json!({"Privileged": false})));
    assert_allowed(create(json!({"Privileged": null})));
    assert_allowed(create(
        json!({"CapAdd": null, "Binds": null, "Mounts": null}),
    ));
    assert_allowed(create(Value::Null));
    assert_allowed(create_raw(r#"{"Image":"alpine:3.20"}"#));
    // Whitespace around the document is valid JSON.
    assert_allowed(create_raw(" {\"Image\":\"alpine:3.20\"}\n"));
    // Non-ASCII UTF-8 in an uninspected value is fine.
    assert_allowed(create_raw(
        r#"{"Image":"alpine:3.20","Labels":{"note":"caf\u00e9 ☕"}}"#,
    ));
}

// --------------------------------------------------------- field-ambiguous

#[test]
fn field_ambiguous_exact_duplicates() {
    assert_refused(
        create_hc_raw(r#"{"Memory":0,"Memory":1}"#),
        "field-ambiguous",
    );
    // Go keeps the last duplicate (wire fact 6); a first-wins reader would
    // see the harmless one.
    assert_refused(
        create_raw(r#"{"Image":"a","HostConfig":{"Privileged":true},"HostConfig":{}}"#),
        "field-ambiguous",
    );
    assert_refused(
        create_hc_raw(r#"{"Mounts":[{"Type":"volume","Type":"volume","Target":"/d"}]}"#),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(
            EXEC,
            r#"{"Cmd":["true"],"Privileged":false,"Privileged":false}"#,
        ),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(NET_CREATE, r#"{"Name":"n","Name":"m"}"#),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(
            NET_CREATE,
            r#"{"Name":"n","IPAM":{"Driver":"default","Driver":"default"}}"#,
        ),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(NET_CONNECT, r#"{"Container":"c1","Container":"c2"}"#),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(VOL_CREATE, r#"{"Name":"v","Name":"w"}"#),
        "field-ambiguous",
    );
}

#[test]
fn field_ambiguous_folded_duplicates() {
    assert_refused(
        create_hc_raw(r#"{"Memory":0,"memory":1}"#),
        "field-ambiguous",
    );
    assert_refused(
        create_raw(r#"{"Image":"a","HostConfig":{},"hostconfig":{}}"#),
        "field-ambiguous",
    );
    assert_refused(
        create_raw(r#"{"Image":"a","HostConfig":{},"hostconfig":{"Privileged":true}}"#),
        "field-ambiguous",
    );
    // U+212A KELVIN SIGN folds to k and U+017F LONG S folds to s in Go.
    assert_refused(
        create_hc_raw("{\"Links\":null,\"Lin\u{212A}s\":null}"),
        "field-ambiguous",
    );
    assert_refused(
        create_hc_raw("{\"ShmSize\":0,\"\u{17F}hmSize\":0}"),
        "field-ambiguous",
    );
    // The same long s written as a JSON escape.
    assert_refused(
        create_hc_raw(r#"{"ShmSize":0,"\u017fhmSize":0}"#),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(
            EXEC,
            r#"{"Cmd":["true"],"Privileged":false,"PRIVILEGED":false}"#,
        ),
        "field-ambiguous",
    );
    assert_refused(
        post_raw(VOL_CREATE, r#"{"Driver":"local","driver":"local"}"#),
        "field-ambiguous",
    );
}

#[test]
fn field_ambiguous_neighbors_maps_are_not_collision_checked() {
    assert_allowed(create_hc_raw(r#"{"Memory":0}"#));
    assert_allowed(create_raw(
        r#"{"Image":"a","Labels":{"a":"1","A":"2","a":"3"},"HostConfig":{}}"#,
    ));
    assert_allowed(create_hc_raw(
        r#"{"LogConfig":{"Type":"json-file","Config":{"max-size":"1m","MAX-SIZE":"2m"}}}"#,
    ));
    assert_allowed(create_hc_raw(r#"{"Annotations":{"x":"1","X":"2"}}"#));
    assert_allowed(create_hc_raw(
        r#"{"PortBindings":{"80/tcp":[{"HostIp":"","HostPort":"8080"}],"80/TCP":[]}}"#,
    ));
    assert_allowed(create_hc_raw(r#"{"Tmpfs":{"/run":"","/RUN":""}}"#));
    assert_allowed(create_raw(
        r#"{"Image":"a","NetworkingConfig":{"EndpointsConfig":{"n1":{},"N1":{}}}}"#,
    ));
    assert_allowed(post_raw(
        VOL_CREATE,
        r#"{"Name":"v","Labels":{"k":"1","K":"2"}}"#,
    ));
}

// ----------------------------------------------------------- field-unknown

#[test]
fn field_unknown_keys_are_refused() {
    assert_refused(create(json!({"Bogus": true})), "field-unknown");
    // U+0131 dotless i does not fold to i in Go, so this is not Privileged.
    assert_refused(
        create_hc_raw("{\"Pr\u{131}vileged\":true}"),
        "field-unknown",
    );
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/d", "Bogus": 1})),
        "field-unknown",
    );
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/d", "ClusterOptions": {}})),
        "field-unknown",
    );
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/d", "VolumeOptions": {"Bogus": 1}})),
        "field-unknown",
    );
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/d",
                     "VolumeOptions": {"DriverConfig": {"Name": "local", "Bogus": 1}}})),
        "field-unknown",
    );
    assert_refused(exec(json!({"Bogus": 1})), "field-unknown");
    assert_refused(net_create(json!({"Bogus": 1})), "field-unknown");
    assert_refused(
        net_create(json!({"IPAM": {"Driver": "default", "Bogus": 1}})),
        "field-unknown",
    );
    assert_refused(connect(json!({"Bogus": 1})), "field-unknown");
    assert_refused(vol_create(json!({"Bogus": 1})), "field-unknown");
}

#[test]
fn field_unknown_neighbors_are_allowed() {
    // Folded spellings of known keys are known keys.
    assert_allowed(create_raw(
        r#"{"Image":"a","hostconfig":{"privileged":false,"capadd":null}}"#,
    ));
    assert_allowed(create_hc_raw("{\"Lin\u{212A}s\":null}"));
    assert_allowed(create(
        json!({"Init": true, "ReadonlyRootfs": true, "PidsLimit": 100,
                                 "Memory": 67108864, "Ulimits": null, "ShmSize": 0}),
    ));
    assert_allowed(mount(
        json!({"Type": "volume", "Source": "data", "Target": "/d",
                                "ReadOnly": false, "Consistency": "", "ClusterOptions": null,
                                "BindOptions": null, "TmpfsOptions": null,
                                "VolumeOptions": {"NoCopy": true, "Labels": {"a": "b"},
                                                  "Subpath": "x",
                                                  "DriverConfig": {"Name": "local", "Options": {}}}}),
    ));
    // Top-level container Config keys are not key-allowlisted.
    assert_allowed(create_raw(
        r#"{"Image":"a","SomethingNew":1,"HostConfig":{}}"#,
    ));
    assert_allowed(exec(
        json!({"ConsoleSize": [24, 80], "Env": ["A=1"], "User": "1000",
                               "WorkingDir": "/w", "Tty": true, "DetachKeys": "ctrl-p"}),
    ));
    assert_allowed(net_create(
        json!({"CheckDuplicate": true, "EnableIPv4": true,
                                     "EnableIPv6": false, "Internal": true, "Attachable": true,
                                     "Labels": {"a": "b"},
                                     "IPAM": {"Driver": "default", "Options": null,
                                              "Config": [{"Subnet": "10.9.0.0/24"}]}}),
    ));
    assert_allowed(vol_create(json!({"Labels": {"a": "b"}, "DriverOpts": null,
                                     "ClusterVolumeSpec": null})));
}

// --------------------------------------------------------------- privileged

#[test]
fn privileged_refused() {
    assert_refused(create(json!({"Privileged": true})), "privileged");
    assert_refused(create(json!({"privileged": true})), "privileged");
    assert_refused(create(json!({"PRIVILEGED": true})), "privileged");
    // A lone lower-cased `hostconfig` is HostConfig to Go's decoder.
    assert_refused(
        create_raw(r#"{"Image":"a","hostconfig":{"privileged":true}}"#),
        "privileged",
    );
    assert_refused(
        create_raw(r#"{"Image":"a","HOSTCONFIG":{"Privileged":true}}"#),
        "privileged",
    );
}

#[test]
fn privileged_neighbors_allowed() {
    assert_allowed(create(json!({"Privileged": false})));
    assert_allowed(create(json!({"privileged": false})));
    assert_allowed(create(json!({})));
}

#[test]
fn refusal_message_format() {
    for (d, rule) in [
        (create(json!({"Privileged": true})), "privileged"),
        (create(json!({"Sysctls": {"kernel.x": "1"}})), "sysctls"),
        (post("/v1.56/images/create?fromSrc=-"), "image-import"),
        (get("/v1.56/system/df"), "endpoint-not-allowed"),
    ] {
        assert_refused(d.clone(), rule);
        assert!(
            d.msg.starts_with(&format!("refused by rule {rule}")),
            "{rule}: {d:?}"
        );
    }
    assert_eq!(
        create(json!({})).rule,
        "",
        "allowed decision carries no rule"
    );
}

// ------------------------------------------------------------------ cap-add

#[test]
fn cap_add_refused() {
    for caps in [
        json!(["ALL"]),
        json!(["all"]),
        json!(["SYS_ADMIN"]),
        json!(["CAP_SYS_ADMIN"]),
        json!(["cap_sys_admin"]),
        json!(["NET_ADMIN"]),
        json!(["SYS_PTRACE"]),
        json!(["CHOWN", "SYS_MODULE"]),
        json!(["CAP_ALL"]),
    ] {
        let d = create(json!({"CapAdd": caps.clone()}));
        assert!(!d.allow && d.rule == "cap-add", "{caps} -> {d:?}");
    }
}

#[test]
fn cap_add_default_set_allowed() {
    assert_allowed(create(json!({"CapAdd": ["net_bind_service"]})));
    assert_allowed(create(json!({"CapAdd": ["CAP_CHOWN"]})));
    assert_allowed(create(json!({"CapAdd": ["cap_chown"]})));
    assert_allowed(create(json!({"CapAdd": [
        "CHOWN", "DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "NET_RAW", "SETGID",
        "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL",
        "AUDIT_WRITE"]})));
    // Re-adding a default cap after dropping all (recorded hardened run).
    assert_allowed(create(
        json!({"CapAdd": ["CAP_NET_BIND_SERVICE"], "CapDrop": ["ALL"]}),
    ));
    assert_allowed(create(json!({"CapAdd": [], "CapDrop": ["ALL"]})));
    assert_allowed(create(json!({"CapAdd": null})));
}

// ----------------------------------------------------------- host-namespace

#[test]
fn host_namespace_refused() {
    for hc in [
        json!({"UsernsMode": "host"}),
        json!({"PidMode": "host"}),
        json!({"IpcMode": "host"}),
        json!({"UTSMode": "host"}),
        json!({"NetworkMode": "host"}),
        json!({"NetworkMode": "HOST"}),
        json!({"NetworkMode": "Host"}),
        json!({"CgroupnsMode": "host"}),
        json!({"Cgroup": "host"}),
        json!({"utsmode": "host"}),
    ] {
        let d = create(hc.clone());
        assert!(!d.allow && d.rule == "host-namespace", "{hc} -> {d:?}");
    }
    // U+212A KELVIN SIGN folds to k: this key is NetworkMode to Go.
    assert_refused(
        create_hc_raw("{\"Networ\u{212A}Mode\":\"host\"}"),
        "host-namespace",
    );
}

#[test]
fn host_namespace_neighbors_allowed() {
    for hc in [
        json!({"UsernsMode": ""}),
        json!({"PidMode": ""}),
        json!({"PidMode": "container:c1"}),
        json!({"IpcMode": ""}),
        json!({"IpcMode": "none"}),
        json!({"IpcMode": "private"}),
        json!({"IpcMode": "shareable"}),
        json!({"IpcMode": "container:c1"}),
        json!({"UTSMode": ""}),
        json!({"NetworkMode": "default"}),
        json!({"NetworkMode": "bridge"}),
        json!({"NetworkMode": "none"}),
        json!({"NetworkMode": "p1_default"}),
        json!({"NetworkMode": "container:c1"}),
        json!({"CgroupnsMode": ""}),
        json!({"CgroupnsMode": "private"}),
        json!({"Cgroup": ""}),
    ] {
        let d = create(hc.clone());
        assert!(d.allow, "{hc} -> {d:?}");
    }
}

// ------------------------------------------------------------------ devices

#[test]
fn devices_each_field_refused() {
    for hc in [
        json!({"Devices": [{"PathOnHost": "/dev/null", "PathInContainer": "/dev/null",
                            "CgroupPermissions": "rwm"}]}),
        json!({"DeviceRequests": [{"Driver": "", "Count": -1, "DeviceIDs": null,
                                   "Capabilities": [["gpu"]], "Options": {}}]}),
        json!({"DeviceCgroupRules": ["c 1:3 mr"]}),
        json!({"BlkioWeightDevice": [{"Path": "/dev/sda", "Weight": 100}]}),
        json!({"BlkioDeviceReadBps": [{"Path": "/dev/sda", "Rate": 1}]}),
        json!({"BlkioDeviceWriteBps": [{"Path": "/dev/sda", "Rate": 1}]}),
        json!({"BlkioDeviceReadIOps": [{"Path": "/dev/sda", "Rate": 1}]}),
        json!({"BlkioDeviceWriteIOps": [{"Path": "/dev/sda", "Rate": 1}]}),
    ] {
        let d = create(hc.clone());
        assert!(!d.allow && d.rule == "devices", "{hc} -> {d:?}");
    }
    assert_refused(
        create_hc_raw("{\"Device\u{17F}\":[{\"PathOnHost\":\"/dev/sda\",\"PathInContainer\":\"/dev/sda\",\"CgroupPermissions\":\"rwm\"}]}"),
        "devices",
    );
}

#[test]
fn devices_empty_fields_allowed() {
    // The CLI sends [] for some of these and null for others (recorded).
    assert_allowed(create(json!({
        "Devices": [], "DeviceRequests": null, "DeviceCgroupRules": null,
        "BlkioWeightDevice": [], "BlkioDeviceReadBps": [], "BlkioDeviceWriteBps": null,
        "BlkioDeviceReadIOps": [], "BlkioDeviceWriteIOps": null, "BlkioWeight": 0})));
}

// ------------------------------------------------------------------ sysctls

#[test]
fn sysctls_refused() {
    assert_refused(
        create(json!({"Sysctls": {"net.ipv4.ip_forward": "1"}})),
        "sysctls",
    );
    assert_refused(
        create(json!({"sysctls": {"kernel.shm_rmid_forced": "1"}})),
        "sysctls",
    );
    assert_refused(
        create_hc_raw("{\"\u{17F}ysctls\":{\"net.ipv4.ip_forward\":\"1\"}}"),
        "sysctls",
    );
    assert_refused(
        create_hc_raw(r#"{"\u017Fysctls":{"net.ipv4.ip_forward":"1"}}"#),
        "sysctls",
    );
}

#[test]
fn sysctls_empty_allowed() {
    assert_allowed(create(json!({"Sysctls": {}})));
    assert_allowed(create(json!({"Sysctls": null})));
}

// ---------------------------------------------------------------- host-bind

#[test]
fn host_bind_binds_refused() {
    for bind in [
        "/:/host",
        "/var/run/docker.sock:/var/run/docker.sock",
        "/etc:/etc:ro",
        "./x:/y",
        "~/x:/y",
        "rel/x:/y",
        "../x:/y",
        ".hidden:/y",
        "a:/y",
        "-x:/y",
        ":/y",
    ] {
        let d = create(json!({"Binds": [bind]}));
        assert!(!d.allow && d.rule == "host-bind", "{bind} -> {d:?}");
    }
    assert_refused(
        create(json!({"Binds": ["data:/data", "/:/host"]})),
        "host-bind",
    );
}

#[test]
fn host_bind_binds_allowed() {
    for bind in [
        "/data",
        "name:/dst:ro",
        "p1_data:/data:rw",
        "my.vol-2_x:/d",
        "v1:/v",
    ] {
        let d = create(json!({"Binds": [bind]}));
        assert!(d.allow, "{bind} -> {d:?}");
    }
    assert_allowed(create(json!({"Binds": []})));
    assert_allowed(create(json!({"Binds": null})));
}

#[test]
fn host_bind_mounts_refused() {
    assert_refused(
        mount(json!({"Type": "bind", "Source": "/", "Target": "/host"})),
        "host-bind",
    );
    assert_refused(
        mount(json!({"Type": "npipe", "Source": "x", "Target": "/x"})),
        "host-bind",
    );
    assert_refused(
        mount(json!({"Type": "cluster", "Source": "x", "Target": "/x"})),
        "host-bind",
    );
    assert_refused(
        mount(json!({"Type": "image", "Source": "alpine", "Target": "/x"})),
        "host-bind",
    );
    // moby compares mount types case sensitively.
    assert_refused(
        mount(json!({"Type": "Volume", "Source": "v", "Target": "/x"})),
        "host-bind",
    );
    assert_refused(
        mount(json!({"Type": "", "Source": "/", "Target": "/x"})),
        "host-bind",
    );
    assert_refused(mount(json!({"Source": "/", "Target": "/x"})), "host-bind");
    assert_refused(
        mount(json!({"Type": "volume", "Source": "v", "Target": "/x", "BindOptions": {}})),
        "host-bind",
    );
    assert_refused(
        mount(json!({"Type": "volume", "Source": "v", "Target": "/x",
                     "BindOptions": {"Propagation": "rshared"}})),
        "host-bind",
    );
}

#[test]
fn host_bind_mounts_allowed() {
    assert_allowed(mount(json!({"Type": "tmpfs", "Target": "/cache"})));
    assert_allowed(mount(json!({"Type": "tmpfs", "Target": "/cache",
                                "TmpfsOptions": {"SizeBytes": 1048576, "Mode": 1777}})));
    assert_allowed(mount(
        json!({"Type": "volume", "Source": "data", "Target": "/data"}),
    ));
    assert_allowed(mount(json!({"Type": "volume", "Target": "/anon"})));
    assert_allowed(mount(
        json!({"Type": "volume", "Source": "v", "Target": "/x",
                                "BindOptions": null}),
    ));
    assert_allowed(create(json!({"Mounts": []})));
}

// ------------------------------------------------------------ volume-driver

#[test]
fn volume_driver_refused() {
    assert_refused(create(json!({"VolumeDriver": "convoy"})), "volume-driver");
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/d",
                     "VolumeOptions": {"DriverConfig": {"Name": "nfs"}}})),
        "volume-driver",
    );
    assert_refused(vol_create(json!({"Driver": "nfs"})), "volume-driver");
    assert_refused(
        vol_create(json!({"ClusterVolumeSpec": {}})),
        "volume-driver",
    );
}

#[test]
fn volume_driver_local_allowed() {
    assert_allowed(create(json!({"VolumeDriver": "local"})));
    assert_allowed(create(json!({"VolumeDriver": ""})));
    assert_allowed(mount(json!({"Type": "volume", "Target": "/d",
                                "VolumeOptions": {"DriverConfig": {"Name": "local"}}})));
    assert_allowed(mount(json!({"Type": "volume", "Target": "/d",
                                "VolumeOptions": {"DriverConfig": {"Name": ""}}})));
    assert_allowed(vol_create(json!({"Driver": "local"})));
    assert_allowed(vol_create(json!({"Driver": ""})));
    assert_allowed(post_raw(VOL_CREATE, r#"{"Name":"v1"}"#));
}

// ---------------------------------------------------- volume-driver-options

#[test]
fn volume_driver_options_refused() {
    assert_refused(
        mount(json!({"Type": "volume", "Target": "/h",
                     "VolumeOptions": {"DriverConfig": {"Name": "local",
                         "Options": {"device": "/", "o": "bind", "type": "none"}}}})),
        "volume-driver-options",
    );
    assert_refused(
        vol_create(json!({"DriverOpts": {"device": "/", "o": "bind", "type": "none"}})),
        "volume-driver-options",
    );
}

#[test]
fn volume_driver_options_empty_allowed() {
    assert_allowed(mount(json!({"Type": "volume", "Target": "/h",
                                "VolumeOptions": {"DriverConfig": {"Name": "local", "Options": {}}}})));
    assert_allowed(mount(json!({"Type": "volume", "Target": "/h",
                                "VolumeOptions": {"DriverConfig": {"Name": "local", "Options": null}}})));
    assert_allowed(vol_create(json!({"DriverOpts": {}})));
    assert_allowed(vol_create(json!({"DriverOpts": null})));
}

// ------------------------------------------------------------- security-opt

#[test]
fn security_opt_refused() {
    for opt in [
        "seccomp=unconfined",
        "seccomp:unconfined",
        "apparmor=unconfined",
        "label=disable",
        "label:disable",
        "systempaths=unconfined",
        "no-new-privileges:yes",
        "no-new-privileges=1",
        "NO-NEW-PRIVILEGES",
    ] {
        let d = create(json!({"SecurityOpt": [opt]}));
        assert!(!d.allow && d.rule == "security-opt", "{opt} -> {d:?}");
    }
    assert_refused(
        create(json!({"SecurityOpt": ["no-new-privileges:true", "label=disable"]})),
        "security-opt",
    );
    // `--security-opt systempaths=unconfined` arrives as MaskedPaths: [].
    assert_refused(create(json!({"MaskedPaths": []})), "security-opt");
    assert_refused(
        create(json!({"MaskedPaths": ["/proc/kcore"]})),
        "security-opt",
    );
    assert_refused(create(json!({"ReadonlyPaths": []})), "security-opt");
    assert_refused(
        create_hc_raw("{\"\u{17F}ecurityOpt\":[\"label=disable\"]}"),
        "security-opt",
    );
    assert_refused(create_hc_raw("{\"Ma\u{17F}kedPaths\":[]}"), "security-opt");
}

#[test]
fn security_opt_allowed() {
    for opt in [
        "no-new-privileges",
        "no-new-privileges:true",
        "no-new-privileges:false",
        "no-new-privileges=true",
        "no-new-privileges=false",
    ] {
        let d = create(json!({"SecurityOpt": [opt]}));
        assert!(d.allow, "{opt} -> {d:?}");
    }
    assert_allowed(create(
        json!({"SecurityOpt": null, "MaskedPaths": null, "ReadonlyPaths": null}),
    ));
    assert_allowed(create(json!({"SecurityOpt": []})));
}

// ------------------------------------------------------------ cgroup-parent

#[test]
fn cgroup_parent_refused() {
    assert_refused(create(json!({"CgroupParent": "/x"})), "cgroup-parent");
    assert_refused(create(json!({"cgroupparent": "/"})), "cgroup-parent");
}

#[test]
fn cgroup_parent_empty_allowed() {
    assert_allowed(create(json!({"CgroupParent": ""})));
}

// --------------------------------------------------------------- log-driver

#[test]
fn log_driver_refused() {
    for t in [
        "syslog",
        "fluentd",
        "journald",
        "gelf",
        "JSON-FILE",
        "splunk",
    ] {
        let d = create(json!({"LogConfig": {"Type": t, "Config": {}}}));
        assert!(!d.allow && d.rule == "log-driver", "{t} -> {d:?}");
    }
}

#[test]
fn log_driver_allowed() {
    for t in ["", "json-file", "local", "none"] {
        let d = create(json!({"LogConfig": {"Type": t, "Config": {"max-size": "10m"}}}));
        assert!(d.allow, "{t} -> {d:?}");
    }
    assert_allowed(create(json!({"LogConfig": {"Type": "", "Config": null}})));
    assert_allowed(create(json!({"LogConfig": null})));
}

// ------------------------------------------------------------------ runtime

#[test]
fn runtime_refused() {
    assert_refused(create(json!({"Runtime": "nvidia"})), "runtime");
    assert_refused(create(json!({"Runtime": "runsc"})), "runtime");
    assert_refused(create(json!({"Isolation": "hyperv"})), "runtime");
    assert_refused(create(json!({"StorageOpt": {"size": "10G"}})), "runtime");
}

#[test]
fn runtime_defaults_allowed() {
    assert_allowed(create(json!({"Runtime": ""})));
    assert_allowed(create(json!({"Runtime": "runc"})));
    assert_allowed(create(json!({"Isolation": ""})));
    assert_allowed(create(json!({"Isolation": "default"})));
    assert_allowed(create(json!({"StorageOpt": {}})));
    assert_allowed(create(json!({"StorageOpt": null})));
}

// --------------------------------------------------- network-driver-options

#[test]
fn network_driver_options_refused() {
    assert_refused(
        create_raw(
            r#"{"Image":"a","HostConfig":{"NetworkMode":"n1"},"NetworkingConfig":{"EndpointsConfig":{"n1":{"DriverOpts":{"com.docker.network.endpoint.sysctls":"net.ipv4.conf.IFNAME.forwarding=1"}}}}}"#,
        ),
        "network-driver-options",
    );
    assert_refused(
        net_create(json!({"Options": {"com.docker.network.bridge.name": "br-x"}})),
        "network-driver-options",
    );
    assert_refused(
        net_create(json!({"IPAM": {"Driver": "default", "Options": {"x": "y"}, "Config": []}})),
        "network-driver-options",
    );
    assert_refused(
        connect(json!({"EndpointConfig": {"DriverOpts": {"x": "y"}}})),
        "network-driver-options",
    );
}

#[test]
fn network_driver_options_empty_allowed() {
    assert_allowed(create_raw(
        r#"{"Image":"a","HostConfig":{"NetworkMode":"n1"},"NetworkingConfig":{"EndpointsConfig":{"n1":{"Aliases":["web"],"DriverOpts":null}}}}"#,
    ));
    assert_allowed(create_raw(
        r#"{"Image":"a","NetworkingConfig":{"EndpointsConfig":{"n1":{"DriverOpts":{}}}}}"#,
    ));
    assert_allowed(create_raw(r#"{"Image":"a","NetworkingConfig":null}"#));
    assert_allowed(net_create(json!({"Options": {}})));
    assert_allowed(net_create(json!({"Options": null})));
    assert_allowed(connect(json!({"EndpointConfig": {"DriverOpts": null}})));
    assert_allowed(connect(json!({"EndpointConfig": null})));
}

// ---------------------------------------------------------- exec-privileged

#[test]
fn exec_privileged_refused() {
    assert_refused(exec(json!({"Privileged": true})), "exec-privileged");
    assert_refused(
        post_raw(EXEC, r#"{"Cmd":["sh"],"privileged":true}"#),
        "exec-privileged",
    );
    assert_refused(
        post_json(
            "/containers/c1/exec",
            &merge(exec_base(), json!({"Privileged": true})),
        ),
        "exec-privileged",
    );
}

#[test]
fn exec_unprivileged_allowed() {
    assert_allowed(exec(json!({"Privileged": false})));
    assert_allowed(exec(json!({"Privileged": null})));
    assert_allowed(post_raw(EXEC, r#"{"Cmd":["sh","-c","echo hi"]}"#));
}

// ----------------------------------------------------------- network-driver

#[test]
fn network_driver_refused() {
    for patch in [
        json!({"Driver": "macvlan"}),
        json!({"Driver": "ipvlan"}),
        json!({"Driver": "overlay"}),
        json!({"Driver": "host"}),
        json!({"IPAM": {"Driver": "dhcp", "Options": {}, "Config": []}}),
        json!({"Ingress": true}),
        json!({"ConfigOnly": true}),
        json!({"ConfigFrom": {"Network": "cfg"}}),
        json!({"Scope": "swarm"}),
        json!({"Scope": "global"}),
    ] {
        let d = net_create(patch.clone());
        assert!(!d.allow && d.rule == "network-driver", "{patch} -> {d:?}");
    }
}

#[test]
fn network_driver_bridge_allowed() {
    assert_allowed(net_create(json!({"Driver": "bridge"})));
    assert_allowed(net_create(json!({"Driver": ""})));
    assert_allowed(net_create(json!({"Scope": "local"})));
    assert_allowed(net_create(json!({"IPAM": {"Driver": "", "Config": []}})));
    assert_allowed(net_create(json!({"IPAM": null})));
    assert_allowed(post_raw(
        NET_CREATE,
        r#"{"Name":"p1_default","Labels":{"com.docker.compose.project":"p1"}}"#,
    ));
}

// ------------------------------------------------------- build endpoint

/// POST /build reads networkmode, cgroupparent, remote, ... with Go's
/// r.FormValue, which also parses a form body. dockerd forwards the LAST
/// Content-Type value to the plugin while Go's parser uses the FIRST, so a
/// duplicated header hides a form body the plugin cannot see. The classic
/// builder endpoint is therefore refused outright; BuildKit builds (Docker CLI
/// 29, compose, buildx) go over POST /grpc and /session instead.
#[test]
fn build_endpoint_refused_for_every_query() {
    for q in [
        "",
        "?dockerfile=Dockerfile&q=1&version=1",
        "?networkmode=default",
        "?networkmode=bridge",
        "?networkmode=none",
        "?networkmode=host",
        "?networkmode=HOST",
        "?networkmode=ho%73t",
        "?networkmode=container%3Ac1",
        "?networkmode=default&networkmode=host",
        "?cgroupparent=",
        "?cgroupparent=%2Fx",
        "?cgroupparent=&cgroupparent=/x",
        "?remote=https%3A%2F%2Fexample.invalid%2Fctx.tar",
        "?version=2",
    ] {
        for prefix in ["", "/v1.24", "/v1.41", "/v1.56", "/v9.99"] {
            let uri = format!("{prefix}/build{q}");
            let d = post(&uri);
            assert!(
                !d.allow && d.rule == "endpoint-not-allowed",
                "{uri} -> {d:?}"
            );
            let d = parts("POST", &uri, Some(b""));
            assert!(
                !d.allow && d.rule == "endpoint-not-allowed",
                "{uri} empty body -> {d:?}"
            );
        }
    }
}

#[test]
fn build_cancel_endpoint_refused() {
    for uri in [
        "/build/cancel",
        "/build/cancel?id=x",
        "/v1.56/build/cancel?id=x",
    ] {
        let d = post(uri);
        assert!(
            !d.allow && d.rule == "endpoint-not-allowed",
            "{uri} -> {d:?}"
        );
    }
}

#[test]
fn build_refusal_message_names_the_endpoint() {
    let d = post("/v1.56/build?networkmode=host");
    assert_refused(d.clone(), "endpoint-not-allowed");
    assert!(d.msg.contains("POST"), "{d:?}");
    assert!(d.msg.contains("/build"), "{d:?}");
}

#[test]
fn buildkit_endpoints_stay_allowed() {
    // What `docker build` (CLI 29), compose and buildx actually call.
    for uri in ["/grpc", "/session", "/v1.56/grpc", "/v1.56/session"] {
        assert_allowed(post(uri));
        assert_allowed(parts("POST", uri, Some(b"")));
    }
}

// ------------------------------------------------------------- image-import

#[test]
fn image_import_refused() {
    for q in [
        "fromSrc=-",
        "fromSrc=http%3A%2F%2Fexample.invalid%2Fx.tar",
        "fromImage=alpine&fromSrc=-",
        "fromImage=",
        "tag=3.20",
    ] {
        let d = post(&format!("/v1.56/images/create?{q}"));
        assert!(!d.allow && d.rule == "image-import", "{q} -> {d:?}");
    }
    assert_refused(post("/v1.56/images/create"), "image-import");
}

#[test]
fn image_pull_allowed() {
    assert_allowed(post(
        "/v1.56/images/create?fromImage=docker.io%2Flibrary%2Falpine&tag=3.20",
    ));
    assert_allowed(post("/v1.56/images/create?fromImage=alpine"));
    assert_allowed(post(
        "/images/create?fromImage=ghcr.io%2Forg%2Fimg&tag=1&platform=linux%2Famd64",
    ));
}
