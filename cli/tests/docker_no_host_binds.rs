use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use curie::docker::{model_probe_args, ollama_run_args, CheckSpec, StartSpec};
use proc_macro2::{TokenStream, TokenTree};
use syn::visit::Visit;

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
            value
                .split_once(':')
                .is_some_and(|(source, _)| !named_volume(source) && !daemon_path(source))
        };
        if forbidden {
            violations.push(value.to_string());
        }
        index += 1;
    }
    violations
}

pub(crate) fn assert_no_client_host_binds(args: &[String]) {
    assert!(
        client_host_binds(args).is_empty(),
        "client bind in {args:?}"
    );
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
    let args = runner_spec().run_args("curie-snapshot-acme-test");
    assert!(
        client_host_binds(&args).is_empty(),
        "client bind in {args:?}"
    );
}

#[test]
fn default_check_argv_has_no_client_host_bind() {
    let args = CheckSpec {
        image: "curie-runner".into(),
        plugin_dir: "/tmp/acme-bot/.curie/snapshots/example".into(),
        timeout_s: 30,
    }
    .run_args("curie-snapshot-acme-test");
    assert!(
        client_host_binds(&args).is_empty(),
        "client bind in {args:?}"
    );
}

#[test]
fn model_sidecar_and_cache_probe_argv_have_no_client_host_binds() {
    let commands = [
        ollama_run_args("acme-ollama", "acme-runner", "ollama/ollama:0.24.0"),
        model_probe_args("acme-model-cache", "ollama/ollama:0.24.0", "qwen3:4b")
            .expect("valid model reference"),
    ];
    for args in commands {
        assert!(
            client_host_binds(&args).is_empty(),
            "client bind in {args:?}"
        );
    }
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
        let args = vec![
            "run".into(),
            format!("--mount={mount}"),
            "curie-runner".into(),
        ];
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
    for command in ["rm", "container"] {
        let args = if command == "container" {
            vec!["container", "rm", "-f", "-v", "acme-runner"]
        } else {
            vec!["rm", "-f", "-v", "acme-runner"]
        }
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
        assert!(client_host_binds(&args).is_empty(), "{args:?}");
    }
}

fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        matches!(&attr.meta, syn::Meta::List(list)
            if list.path.is_ident("cfg") && list.tokens.to_string() == "test")
    })
}

fn expression_string(expression: &syn::Expr) -> Option<String> {
    match expression {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) => Some(value.value()),
        syn::Expr::MethodCall(call) => expression_string(&call.receiver),
        _ => None,
    }
}

#[derive(Default)]
struct MountSites {
    implementation: Option<String>,
    function: Option<String>,
    sites: BTreeMap<String, usize>,
}

impl MountSites {
    fn literal(&mut self, value: &str) {
        if matches!(value, "-v" | "--volume" | "--mount")
            || value.starts_with("--volume=")
            || value.starts_with("--mount=")
        {
            let function = self.function.as_deref().unwrap_or("<module>");
            *self.sites.entry(function.to_string()).or_default() += 1;
        }
    }

    fn tokens(&mut self, tokens: TokenStream) {
        let first = tokens.clone().into_iter().next();
        let removal = matches!(first, Some(TokenTree::Literal(literal))
            if syn::parse_str::<syn::LitStr>(&literal.to_string())
                .is_ok_and(|literal| literal.value() == "rm"));
        for token in tokens {
            match token {
                TokenTree::Group(group) => self.tokens(group.stream()),
                TokenTree::Literal(literal) => {
                    if let Ok(literal) = syn::parse_str::<syn::LitStr>(&literal.to_string()) {
                        let value = literal.value();
                        if !(removal && value == "-v") {
                            self.literal(&value);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn enter_function(&mut self, name: &syn::Ident) -> Option<String> {
        self.function.replace(match &self.implementation {
            Some(owner) => format!("{owner}::{name}"),
            None => name.to_string(),
        })
    }
}

impl<'ast> Visit<'ast> for MountSites {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !test_only(&node.attrs) {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if test_only(&node.attrs) {
            return;
        }
        let previous = self.implementation.take();
        if let syn::Type::Path(owner) = node.self_ty.as_ref() {
            self.implementation = owner
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string());
        }
        syn::visit::visit_item_impl(self, node);
        self.implementation = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if test_only(&node.attrs) {
            return;
        }
        let previous = self.enter_function(&node.sig.ident);
        syn::visit::visit_item_fn(self, node);
        self.function = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if test_only(&node.attrs) {
            return;
        }
        let previous = self.enter_function(&node.sig.ident);
        syn::visit::visit_impl_item_fn(self, node);
        self.function = previous;
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        self.literal(&node.value());
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.tokens(node.tokens.clone());
    }

    fn visit_expr_array(&mut self, node: &'ast syn::ExprArray) {
        let removal = node
            .elems
            .first()
            .and_then(expression_string)
            .is_some_and(|command| command == "rm");
        for element in &node.elems {
            if !(removal && expression_string(element).as_deref() == Some("-v")) {
                self.visit_expr(element);
            }
        }
    }
}

fn mount_sites(source: &str) -> BTreeMap<String, usize> {
    let parsed = syn::parse_file(source).expect("Docker argv inventory must parse production Rust");
    let mut visitor = MountSites::default();
    visitor.visit_file(&parsed);
    visitor.sites
}

fn module_sources(items: &[syn::Item], directory: &Path, sources: &mut Vec<PathBuf>) {
    for item in items {
        let syn::Item::Mod(module) = item else {
            continue;
        };
        if test_only(&module.attrs) {
            continue;
        }
        if let Some((_, items)) = &module.content {
            module_sources(items, &directory.join(module.ident.to_string()), sources);
            continue;
        }
        let explicit = module.attrs.iter().find_map(|attr| {
            let syn::Meta::NameValue(value) = &attr.meta else {
                return None;
            };
            value.path.is_ident("path").then(|| {
                expression_string(&value.value).expect("module path must be a string literal")
            })
        });
        let path = match explicit {
            Some(path) => directory.join(path),
            None => {
                let path = directory.join(format!("{}.rs", module.ident));
                if path.is_file() {
                    path
                } else {
                    directory.join(module.ident.to_string()).join("mod.rs")
                }
            }
        };
        let source = fs::read_to_string(&path).expect("read declared production Rust module");
        let parsed = syn::parse_file(&source).expect("parse declared production Rust module");
        let children = if path.file_name().is_some_and(|name| name == "mod.rs") {
            path.parent().unwrap().to_path_buf()
        } else {
            path.with_extension("")
        };
        module_sources(&parsed.items, &children, sources);
        sources.push(path);
    }
}

#[test]
fn mount_inventory_detects_new_builder_sites_and_ignores_test_modules() {
    let source = r#"
        fn new_builder() { let args = vec!["run", "--mount", "type=bind,src=/tmp/acme,dst=/a"]; }
        struct Builder;
        impl Builder { fn args() { args.push("-v"); } }
        #[cfg(test)] mod tests { fn fixture() { vec!["-v", "/tmp/acme:/a"]; } }
        #[cfg(test)] fn fixture() { vec!["-v", "/tmp/acme:/a"]; }
        #[cfg(test)] impl Builder { fn fixture() { vec!["--mount"]; } }
        fn removal() {
            let args = vec!["rm".into(), "-f".into(), "-v".into(), "acme-runner".into()];
            let args = ["rm".into(), "-f".into(), "-v".into(), "acme-runner".into()];
        }
        fn mixed() {
            let remove = vec!["rm", "-v", "acme-runner"];
            let start = vec!["run", "-v", "/tmp/acme:/a", "curie-runner"];
        }
    "#;
    assert_eq!(
        mount_sites(source),
        BTreeMap::from([
            ("Builder::args".into(), 1),
            ("mixed".into(), 1),
            ("new_builder".into(), 1),
        ])
    );
}

#[test]
fn mount_inventory_does_not_open_external_test_modules() {
    let source = r#"
        #[cfg(test)] mod absent_test_fixture;
        mod inline {
            #[cfg(test)]
            #[path = "absent_fixture.rs"]
            mod fixture;
        }
    "#;
    let parsed = syn::parse_file(source).expect("parse module declarations");
    let mut sources = Vec::new();
    module_sources(&parsed.items, Path::new("/absent/acme-src"), &mut sources);
    assert!(sources.is_empty());
}

#[test]
fn every_rust_mount_builder_is_reviewed_for_remote_docker() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found = BTreeMap::new();
    let mut sources = Vec::new();
    for entry in ["lib.rs", "main.rs"] {
        let path = root.join("src").join(entry);
        let source = fs::read_to_string(&path).expect("read CLI crate root");
        let parsed = syn::parse_file(&source).expect("parse CLI crate root");
        module_sources(&parsed.items, &root.join("src"), &mut sources);
        sources.push(path);
    }
    sources.sort();
    sources.dedup();
    for path in sources {
        let source = fs::read_to_string(&path).expect("read production CLI Rust source");
        for (function, count) in mount_sites(&source) {
            let relative = path.strip_prefix(root).expect("source belongs to CLI");
            found.insert(format!("{}::{function}", relative.display()), count);
        }
    }
    let reviewed = BTreeMap::from([
        ("src/docker.rs::CheckSpec::run_args".into(), 1),
        ("src/docker.rs::ConnectorStartSpec::run_args".into(), 1),
        ("src/docker.rs::StartSpec::run_args".into(), 1),
        ("src/docker.rs::model_probe_args".into(), 1),
        ("src/docker.rs::ollama_run_args".into(), 1),
        ("src/docker.rs::seed_volume".into(), 1),
        ("src/local.rs::down_command".into(), 1),
    ]);
    assert_eq!(
        found, reviewed,
        "A Docker mount builder changed. Exercise its emitted argv with client_host_binds \
         before registering the new site. Compose down -v removes volumes and is not a mount."
    );
}
