//! Allowlist decision for one Docker API request.
//!
//! Anything the policy cannot classify or parse is refused. Rule ids are
//! stable: they appear in `Decision.rule`, in the refusal message dockerd
//! shows the client, and in the plugin's log.
//!
//! moby reads query parameters with Go's `r.FormValue`, which also reads a
//! form body, and dockerd forwards only JSON bodies to the plugin. So a form
//! Content-Type is refused on every endpoint before the endpoint table is
//! consulted. That is not enough on its own: dockerd forwards only the LAST
//! value of a repeated Content-Type header, while Go's form parser reads the
//! FIRST. `POST /images/create` is the one endpoint whose query carries
//! privilege (`fromSrc`), so it cannot rely on the forwarded value and
//! refuses any Content-Type (a real pull sends none). `POST /build` is not in
//! the table at all: a `/build` query can be smuggled in a form body, and
//! BuildKit builds use `POST /grpc` and `POST /session`.

use crate::json::{refuse, Fields, Node, Refusal, Res};
use serde_json::Value;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub allow: bool,
    pub rule: &'static str,
    pub msg: String,
}

impl Decision {
    pub fn allowed() -> Self {
        Decision {
            allow: true,
            rule: "",
            msg: String::new(),
        }
    }

    pub fn refused(rule: &'static str, msg: impl Into<String>) -> Self {
        Decision {
            allow: false,
            rule,
            msg: msg.into(),
        }
    }
}

impl From<Refusal> for Decision {
    fn from(r: Refusal) -> Self {
        Decision::refused(r.rule, format!("refused by rule {}: {}", r.rule, r.detail))
    }
}

fn decision(r: Res<()>) -> Decision {
    r.map_or_else(Decision::from, |()| Decision::allowed())
}

/// Run a decision so that a panic refuses instead of unwinding into the
/// server (which must never answer Allow true by accident).
fn guarded(f: impl FnOnce() -> Decision) -> Decision {
    catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| refuse("internal-error", "the policy failed on this request").into())
}

/// Decide one AuthZReq. `raw` is the exact JSON body dockerd POSTs to
/// /AuthZPlugin.AuthZReq. Never panics on any input; refuses what it cannot parse.
pub fn authorize(raw: &[u8]) -> Decision {
    authorize_logged(raw).0
}

/// `authorize`, plus the request method and query-less URI for the log line
/// (None when the envelope itself is unreadable).
pub(crate) fn authorize_logged(raw: &[u8]) -> (Decision, Option<(String, String)>) {
    let mut seen = None;
    let d = guarded(|| match envelope(raw) {
        Err(r) => r.into(),
        Ok(Envelope {
            method,
            uri,
            content_type,
            body,
        }) => {
            let path = path_of(&uri).to_owned();
            seen = Some((method.clone(), path));
            match content_type.as_deref().filter(|ct| is_form(ct)) {
                Some(ct) => refuse(
                    "form-body",
                    format!(
                        "Content-Type {ct:?} is a form body; moby reads form fields as \
                         query parameters, which this plugin cannot see"
                    ),
                )
                .into(),
                None => decision(decide(
                    &method,
                    &uri,
                    content_type.as_deref(),
                    body.as_deref(),
                )),
            }
        }
    });
    (d, seen)
}

struct Envelope {
    method: String,
    uri: String,
    /// The forwarded Content-Type value, if the header is present.
    content_type: Option<String>,
    body: Option<Vec<u8>>,
}

/// AuthZReq keys as dockerd sends them: RequestMethod, RequestUri,
/// RequestHeaders (a string map, may be absent), RequestBody (base64, absent
/// when dockerd forwarded no body).
fn envelope(raw: &[u8]) -> Res<Envelope> {
    let bad = |detail: String| refuse("request-unparseable", detail);
    let v: Value =
        serde_json::from_slice(raw).map_err(|e| bad(format!("AuthZReq is not JSON: {e}")))?;
    let obj = v
        .as_object()
        .ok_or_else(|| bad("AuthZReq is not an object".into()))?;
    let field = |k: &str| {
        obj.get(k)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| bad(format!("{k} missing or not a string")))
    };
    let (method, uri) = (field("RequestMethod")?, field("RequestUri")?);
    let mut content_type = None;
    match obj.get("RequestHeaders") {
        None | Some(Value::Null) => {}
        Some(Value::Object(headers)) => {
            for (k, v) in headers {
                if !k.eq_ignore_ascii_case("content-type") {
                    continue;
                }
                let ct = v
                    .as_str()
                    .ok_or_else(|| bad("Content-Type header is not a string".into()))?;
                // Keep a form value if one of several case variants is a form.
                if content_type.as_deref().is_none_or(|prev| !is_form(prev)) {
                    content_type = Some(ct.to_owned());
                }
            }
        }
        Some(_) => return Err(bad("RequestHeaders is not an object".into())),
    }
    let body = match obj.get("RequestBody") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            Some(b64_decode(s).ok_or_else(|| bad("RequestBody is not valid base64".into()))?)
        }
        Some(_) => return Err(bad("RequestBody is not a string".into())),
    };
    Ok(Envelope {
        method,
        uri,
        content_type,
        body,
    })
}

/// A media type Go's form parser reads as request parameters.
fn is_form(content_type: &str) -> bool {
    let media = content_type.split(';').next().unwrap_or("").trim();
    media.eq_ignore_ascii_case("application/x-www-form-urlencoded")
        || media.eq_ignore_ascii_case("multipart/form-data")
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    }
    let b = s.as_bytes();
    if !b.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 4 * 3);
    let chunks = b.len() / 4;
    for (i, chunk) in b.chunks(4).enumerate() {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && i + 1 != chunks) {
            return None;
        }
        let mut n = 0u32;
        for &c in &chunk[..4 - pad] {
            n = n << 6 | val(c)?;
        }
        n <<= 6 * pad;
        out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8][..3 - pad]);
    }
    Some(out)
}

/// Same decision from parts. `body` is the DECODED request body (None when
/// dockerd sent no RequestBody).
pub fn authorize_parts(method: &str, request_uri: &str, body: Option<&[u8]>) -> Decision {
    guarded(|| decision(decide(method, request_uri, None, body)))
}

// ------------------------------------------------------------ URI handling

fn hex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

/// The request target without its query: the text before the first `?`.
pub(crate) fn path_of(uri: &str) -> &str {
    uri.split_once('?').map_or(uri, |(p, _)| p)
}

/// Percent-decode to UTF-8; `plus` turns `+` into a space (query form).
fn percent_decode(s: &str, plus: bool) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let h = b.get(i + 1..i + 3)?;
                out.push(hex(h[0])? << 4 | hex(h[1])?);
                i += 3;
            }
            b'+' if plus => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn is_clean(p: &str) -> bool {
    p.starts_with('/')
        && !p.contains("//")
        && !p.contains("/./")
        && !p.contains("/../")
        && !p.ends_with("/.")
        && !p.ends_with("/..")
}

/// Strip `^/v[0-9.]+` when a `/` follows it, as moby's versioned routes do.
fn strip_version(p: &str) -> &str {
    if let Some(rest) = p.strip_prefix("/v") {
        let n = rest
            .bytes()
            .take_while(|c| c.is_ascii_digit() || *c == b'.')
            .count();
        if n > 0 && rest[n..].starts_with('/') {
            return &rest[n..];
        }
    }
    p
}

/// Split on `&`; a pair containing `;` is refused because older Go
/// ParseQuery also split on it, which could smuggle a second value.
fn parse_query(q: &str) -> Option<Vec<(String, String)>> {
    q.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            if pair.contains(';') {
                return None;
            }
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            Some((percent_decode(k, true)?, percent_decode(v, true)?))
        })
        .collect()
}

// --------------------------------------------------------- endpoint table

#[derive(Clone, Copy)]
enum Kind {
    Plain,
    ContainerCreate,
    ExecCreate,
    NetworkCreate,
    NetworkConnect,
    VolumeCreate,
    ImagePull,
}

use Kind::*;

/// `{*}` is one or more characters, slashes allowed, anchored by the literal
/// prefix and suffix (moby matches `{name:.*}` against the decoded path, and
/// image names carry `/` unescaped).
const ENDPOINTS: &[(&[&str], &str, Kind)] = &[
    (&["GET", "HEAD"], "/_ping", Plain),
    (&["GET"], "/version", Plain),
    (&["GET"], "/info", Plain),
    (&["GET"], "/events", Plain),
    (&["GET"], "/containers/json", Plain),
    (&["POST"], "/containers/create", ContainerCreate),
    (&["GET"], "/containers/{*}/json", Plain),
    (&["GET"], "/containers/{*}/logs", Plain),
    (&["GET"], "/containers/{*}/top", Plain),
    (&["GET"], "/containers/{*}/stats", Plain),
    (&["GET", "HEAD", "PUT"], "/containers/{*}/archive", Plain),
    (&["POST"], "/containers/{*}/start", Plain),
    (&["POST"], "/containers/{*}/stop", Plain),
    (&["POST"], "/containers/{*}/kill", Plain),
    (&["POST"], "/containers/{*}/restart", Plain),
    (&["POST"], "/containers/{*}/wait", Plain),
    (&["POST"], "/containers/{*}/attach", Plain),
    (&["POST"], "/containers/{*}/resize", Plain),
    (&["POST"], "/containers/{*}/exec", ExecCreate),
    (&["DELETE"], "/containers/{*}", Plain),
    (&["POST"], "/exec/{*}/start", Plain),
    (&["POST"], "/exec/{*}/resize", Plain),
    (&["GET"], "/exec/{*}/json", Plain),
    (&["POST"], "/images/create", ImagePull),
    (&["POST"], "/session", Plain),
    (&["POST"], "/grpc", Plain),
    (&["GET"], "/images/json", Plain),
    (&["GET"], "/images/{*}/json", Plain),
    (&["POST"], "/images/{*}/tag", Plain),
    (&["DELETE"], "/images/{*}", Plain),
    (&["GET"], "/networks", Plain),
    (&["GET"], "/networks/{*}", Plain),
    (&["POST"], "/networks/create", NetworkCreate),
    (&["POST"], "/networks/{*}/connect", NetworkConnect),
    (&["POST"], "/networks/{*}/disconnect", Plain),
    (&["DELETE"], "/networks/{*}", Plain),
    (&["GET"], "/volumes", Plain),
    (&["GET"], "/volumes/{*}", Plain),
    (&["POST"], "/volumes/create", VolumeCreate),
    (&["DELETE"], "/volumes/{*}", Plain),
];

fn matches(pattern: &str, path: &str) -> bool {
    match pattern.split_once("{*}") {
        None => pattern == path,
        Some((pre, suf)) => {
            path.len() > pre.len() + suf.len() && path.starts_with(pre) && path.ends_with(suf)
        }
    }
}

/// Literal rows win over wildcard rows (`/containers/json` is list,
/// `/networks/create` is create).
fn classify(method: &str, path: &str) -> Option<Kind> {
    let pass = |wildcard: bool| {
        ENDPOINTS
            .iter()
            .find(|(methods, pat, _)| {
                pat.contains("{*}") == wildcard && methods.contains(&method) && matches(pat, path)
            })
            .map(|row| row.2)
    };
    pass(false).or_else(|| pass(true))
}

/// `content_type` is the forwarded Content-Type header, already known not to
/// be a form type.
fn decide(method: &str, uri: &str, content_type: Option<&str>, body: Option<&[u8]>) -> Res<()> {
    let raw_path = path_of(uri);
    let query = uri[raw_path.len()..].strip_prefix('?').unwrap_or("");
    let path = percent_decode(raw_path, false)
        .filter(|p| is_clean(p))
        .ok_or_else(|| refuse("uri-invalid", "path has bad encoding or is not clean"))?;
    let path = strip_version(&path);
    let query = parse_query(query)
        .ok_or_else(|| refuse("uri-invalid", "query has bad encoding or a `;`"))?;
    let Some(kind) = classify(method, path) else {
        let mut detail = format!("{method} {path} is not an allowed endpoint");
        if path == "/plugins" || path.starts_with("/plugins/") {
            detail.push_str("; a generic docker inspect of a missing name ends here; pass --type");
        }
        return Err(refuse("endpoint-not-allowed", detail));
    };
    let check: fn(&Node) -> Res<()> = match kind {
        Plain => return Ok(()),
        ImagePull => return image_pull(&query, content_type),
        ContainerCreate => container_create,
        ExecCreate => exec_create,
        NetworkCreate => network_create,
        NetworkConnect => network_connect,
        VolumeCreate => volume_create,
    };
    let body = body.filter(|b| !b.is_empty()).ok_or_else(|| {
        refuse(
            "body-required",
            format!("{method} {path} needs a JSON body"),
        )
    })?;
    let root = Node::parse(body).map_err(|e| {
        refuse(
            "body-unparseable",
            format!("body is not one JSON document: {e}"),
        )
    })?;
    check(&root)
}

// ------------------------------------------------------------ query rules

fn values<'q>(query: &'q [(String, String)], key: &'q str) -> impl Iterator<Item = &'q str> {
    query
        .iter()
        .filter(move |(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn image_pull(query: &[(String, String)], content_type: Option<&str>) -> Res<()> {
    if let Some(ct) = content_type {
        return Err(refuse(
            "form-body",
            format!(
                "images/create carries Content-Type {ct:?}; dockerd forwards only the last \
                 Content-Type value while Go's form parser reads the first, so a form body \
                 could hide fromSrc from this plugin (a pull sends no Content-Type)"
            ),
        ));
    }
    if values(query, "fromSrc").next().is_some() {
        return Err(refuse(
            "image-import",
            "images/create with fromSrc imports a tarball",
        ));
    }
    let mut from = values(query, "fromImage").peekable();
    if from.peek().is_none() || from.any(str::is_empty) {
        return Err(refuse(
            "image-import",
            "images/create needs a non-empty fromImage",
        ));
    }
    Ok(())
}

// ------------------------------------------------------------- body rules

#[rustfmt::skip]
const HOST_CONFIG_KEYS: &[&str] = &[
    "Binds", "ContainerIDFile", "LogConfig", "NetworkMode", "PortBindings", "RestartPolicy",
    "AutoRemove", "VolumeDriver", "VolumesFrom", "ConsoleSize", "Annotations", "CapAdd",
    "CapDrop", "CgroupnsMode", "Dns", "DnsOptions", "DnsSearch", "ExtraHosts", "GroupAdd",
    "IpcMode", "Cgroup", "Links", "OomScoreAdj", "PidMode", "Privileged", "PublishAllPorts",
    "ReadonlyRootfs", "SecurityOpt", "StorageOpt", "Tmpfs", "UTSMode", "UsernsMode", "ShmSize",
    "Sysctls", "Runtime", "Isolation", "CpuShares", "Memory", "NanoCpus", "CgroupParent",
    "BlkioWeight", "BlkioWeightDevice", "BlkioDeviceReadBps", "BlkioDeviceWriteBps",
    "BlkioDeviceReadIOps", "BlkioDeviceWriteIOps", "CpuPeriod", "CpuQuota", "CpuRealtimePeriod",
    "CpuRealtimeRuntime", "CpusetCpus", "CpusetMems", "Devices", "DeviceCgroupRules",
    "DeviceRequests", "KernelMemoryTCP", "MemoryReservation", "MemorySwap", "MemorySwappiness",
    "OomKillDisable", "PidsLimit", "Ulimits", "CpuCount", "CpuPercent", "IOMaximumIOps",
    "IOMaximumBandwidth", "Mounts", "MaskedPaths", "ReadonlyPaths", "Init",
];
#[rustfmt::skip]
const MOUNT_KEYS: &[&str] = &[
    "Type", "Source", "Target", "ReadOnly", "Consistency", "BindOptions", "VolumeOptions",
    "TmpfsOptions", "ClusterOptions",
];
const VOLUME_OPTIONS_KEYS: &[&str] = &["NoCopy", "Labels", "Subpath", "DriverConfig"];
const DRIVER_CONFIG_KEYS: &[&str] = &["Name", "Options"];
#[rustfmt::skip]
const EXEC_KEYS: &[&str] = &[
    "AttachStdin", "AttachStdout", "AttachStderr", "ConsoleSize", "DetachKeys", "Tty", "Env",
    "Cmd", "Privileged", "User", "WorkingDir",
];
#[rustfmt::skip]
const NETWORK_KEYS: &[&str] = &[
    "Name", "CheckDuplicate", "Driver", "Scope", "EnableIPv4", "EnableIPv6", "IPAM", "Internal",
    "Attachable", "Ingress", "ConfigOnly", "ConfigFrom", "Options", "Labels",
];
const IPAM_KEYS: &[&str] = &["Driver", "Options", "Config"];
const CONNECT_KEYS: &[&str] = &["Container", "EndpointConfig"];
const VOLUME_KEYS: &[&str] = &[
    "Name",
    "Driver",
    "DriverOpts",
    "Labels",
    "ClusterVolumeSpec",
];

/// Docker's default capability set; re-adding one is a no-op privilege-wise.
#[rustfmt::skip]
const DEFAULT_CAPS: &[&str] = &[
    "CHOWN", "DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "NET_RAW", "SETGID", "SETUID",
    "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE",
];
#[rustfmt::skip]
const DEVICE_FIELDS: &[&str] = &[
    "Devices", "DeviceRequests", "DeviceCgroupRules", "BlkioWeightDevice", "BlkioDeviceReadBps",
    "BlkioDeviceWriteBps", "BlkioDeviceReadIOps", "BlkioDeviceWriteIOps",
];
const SECURITY_OPTS: &[&str] = &[
    "no-new-privileges",
    "no-new-privileges:true",
    "no-new-privileges:false",
    "no-new-privileges=true",
    "no-new-privileges=false",
];

/// Refuse with `rule` unless string field `name` is one of `ok`.
fn one_of(f: &Fields, name: &str, rule: &'static str, ok: &[&str]) -> Res<()> {
    let v = f.str(name)?;
    if ok.contains(&v) {
        Ok(())
    } else {
        Err(refuse(rule, format!("{} is {v:?}", f.what(name))))
    }
}

fn require(cond: bool, rule: &'static str, detail: impl FnOnce() -> String) -> Res<()> {
    if cond {
        Ok(())
    } else {
        Err(refuse(rule, detail()))
    }
}

fn top<'a>(root: &'a Node, what: &str, keys: Option<&[&str]>) -> Res<Fields<'a>> {
    Fields::of(root, what, keys)?
        .ok_or_else(|| refuse("body-unparseable", format!("{what} must be a JSON object")))
}

/// Top-level create keys are container Config (no host privilege) and are not
/// allowlisted, but the object is collision checked so `hostconfig` beside
/// `HostConfig` is ambiguous, and a lone `hostconfig` is read as HostConfig.
fn container_create(root: &Node) -> Res<()> {
    let body = top(root, "body", None)?;
    if let Some(hc) = body.sub("HostConfig", Some(HOST_CONFIG_KEYS))? {
        host_config(&hc)?;
    }
    if let Some(nc) = body.sub("NetworkingConfig", None)? {
        for (name, ep) in nc.map("EndpointsConfig")? {
            let what = format!("{}[{name:?}]", nc.what("EndpointsConfig"));
            if let Some(ep) = Fields::of(ep, &what, None)? {
                let opts = ep.map("DriverOpts")?;
                require(opts.is_empty(), "network-driver-options", || {
                    format!("{} is set", ep.what("DriverOpts"))
                })?;
            }
        }
    }
    Ok(())
}

fn host_config(hc: &Fields) -> Res<()> {
    require(!hc.bool("Privileged")?, "privileged", || {
        "HostConfig.Privileged is true".into()
    })?;
    for cap in hc.strs("CapAdd")? {
        let upper = cap.to_ascii_uppercase();
        let name = upper.strip_prefix("CAP_").unwrap_or(&upper);
        require(DEFAULT_CAPS.contains(&name), "cap-add", || {
            format!("HostConfig.CapAdd {cap:?} is outside Docker's default set")
        })?;
    }
    host_namespaces(hc)?;
    for field in DEVICE_FIELDS {
        require(hc.list(field)?.is_empty(), "devices", || {
            format!("{} is set", hc.what(field))
        })?;
    }
    require(hc.map("Sysctls")?.is_empty(), "sysctls", || {
        "HostConfig.Sysctls is set".into()
    })?;
    binds_and_mounts(hc)?;
    one_of(hc, "VolumeDriver", "volume-driver", &["", "local"])?;
    for opt in hc.strs("SecurityOpt")? {
        require(SECURITY_OPTS.contains(&opt), "security-opt", || {
            format!("HostConfig.SecurityOpt {opt:?}")
        })?;
    }
    // `--security-opt systempaths=unconfined` arrives as `MaskedPaths: []`.
    for field in ["MaskedPaths", "ReadonlyPaths"] {
        require(!hc.present(field), "security-opt", || {
            format!("{} is set", hc.what(field))
        })?;
    }
    one_of(hc, "CgroupParent", "cgroup-parent", &[""])?;
    if let Some(log) = hc.sub("LogConfig", None)? {
        one_of(
            &log,
            "Type",
            "log-driver",
            &["", "json-file", "local", "none"],
        )?;
    }
    one_of(hc, "Runtime", "runtime", &["", "runc"])?;
    one_of(hc, "Isolation", "runtime", &["", "default"])?;
    require(hc.map("StorageOpt")?.is_empty(), "runtime", || {
        "HostConfig.StorageOpt is set".into()
    })
}

fn host_namespaces(hc: &Fields) -> Res<()> {
    let rule = "host-namespace";
    let container = |v: &str| v.starts_with("container:");
    one_of(hc, "UsernsMode", rule, &[""])?;
    one_of(hc, "UTSMode", rule, &[""])?;
    one_of(hc, "CgroupnsMode", rule, &["", "private"])?;
    one_of(hc, "Cgroup", rule, &[""])?;
    let pid = hc.str("PidMode")?;
    require(pid.is_empty() || container(pid), rule, || {
        format!("HostConfig.PidMode is {pid:?}")
    })?;
    let ipc = hc.str("IpcMode")?;
    require(
        ["", "none", "private", "shareable"].contains(&ipc) || container(ipc),
        rule,
        || format!("HostConfig.IpcMode is {ipc:?}"),
    )?;
    let net = hc.str("NetworkMode")?;
    require(!net.eq_ignore_ascii_case("host"), rule, || {
        format!("HostConfig.NetworkMode is {net:?}")
    })
}

fn is_volume_name(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 2
        && b[0].is_ascii_alphanumeric()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(c))
}

fn binds_and_mounts(hc: &Fields) -> Res<()> {
    for bind in hc.strs("Binds")? {
        if let Some((source, _)) = bind.split_once(':') {
            require(is_volume_name(source), "host-bind", || {
                format!("HostConfig.Binds {bind:?} has a host path source")
            })?;
        }
    }
    for (i, node) in hc.list("Mounts")?.iter().enumerate() {
        let what = format!("HostConfig.Mounts[{i}]");
        let m = Fields::of(node, &what, Some(MOUNT_KEYS))?
            .ok_or_else(|| refuse("host-bind", format!("{what} is null")))?;
        // moby compares mount types case sensitively.
        let ty = m.str("Type")?;
        require(ty == "volume" || ty == "tmpfs", "host-bind", || {
            format!("{what}.Type is {ty:?}")
        })?;
        require(!m.present("BindOptions"), "host-bind", || {
            format!("{what}.BindOptions is set")
        })?;
        require(!m.present("ClusterOptions"), "field-unknown", || {
            format!("{what}.ClusterOptions is set")
        })?;
        if let Some(vo) = m.sub("VolumeOptions", Some(VOLUME_OPTIONS_KEYS))? {
            if let Some(dc) = vo.sub("DriverConfig", Some(DRIVER_CONFIG_KEYS))? {
                one_of(&dc, "Name", "volume-driver", &["", "local"])?;
                require(
                    dc.map("Options")?.is_empty(),
                    "volume-driver-options",
                    || format!("{} is set", dc.what("Options")),
                )?;
            }
        }
    }
    Ok(())
}

fn exec_create(root: &Node) -> Res<()> {
    let exec = top(root, "exec", Some(EXEC_KEYS))?;
    require(!exec.bool("Privileged")?, "exec-privileged", || {
        "exec Privileged is true".into()
    })
}

fn network_create(root: &Node) -> Res<()> {
    let net = top(root, "network", Some(NETWORK_KEYS))?;
    let rule = "network-driver";
    one_of(&net, "Driver", rule, &["", "bridge"])?;
    one_of(&net, "Scope", rule, &["", "local"])?;
    for field in ["Ingress", "ConfigOnly"] {
        require(!net.bool(field)?, rule, || {
            format!("network {field} is true")
        })?;
    }
    require(!net.present("ConfigFrom"), rule, || {
        "network ConfigFrom is set".into()
    })?;
    let ipam = net.sub("IPAM", Some(IPAM_KEYS))?;
    if let Some(ipam) = &ipam {
        one_of(ipam, "Driver", rule, &["", "default"])?;
    }
    require(
        net.map("Options")?.is_empty(),
        "network-driver-options",
        || "network Options is set".into(),
    )?;
    match ipam {
        Some(ipam) => require(
            ipam.map("Options")?.is_empty(),
            "network-driver-options",
            || "network.IPAM Options is set".into(),
        ),
        None => Ok(()),
    }
}

fn network_connect(root: &Node) -> Res<()> {
    let connect = top(root, "connect", Some(CONNECT_KEYS))?;
    match connect.sub("EndpointConfig", None)? {
        Some(ep) => require(
            ep.map("DriverOpts")?.is_empty(),
            "network-driver-options",
            || "connect.EndpointConfig.DriverOpts is set".into(),
        ),
        None => Ok(()),
    }
}

fn volume_create(root: &Node) -> Res<()> {
    let vol = top(root, "volume", Some(VOLUME_KEYS))?;
    one_of(&vol, "Driver", "volume-driver", &["", "local"])?;
    require(!vol.present("ClusterVolumeSpec"), "volume-driver", || {
        "volume ClusterVolumeSpec is set".into()
    })?;
    require(
        vol.map("DriverOpts")?.is_empty(),
        "volume-driver-options",
        || "volume DriverOpts is set".into(),
    )
}
