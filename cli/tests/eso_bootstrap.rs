//! External Secrets install, reuse, refusal, and teardown.
//!
//! These tests script kubectl and helm. They do not need the aws CLI, a
//! cluster, or a live External Secrets controller.

use std::sync::Mutex;

use curie::provider::bootstrap::{
    decide, dry_run_lines, ensure, minor_version, remove_owned_controller, ControllerTeardown,
    Decision, EnsureOutcome, InstallRef, InstallationView, ESO_CHART, OWNERSHIP_CONFIGMAP,
};
use curie::provider::eso::{Kubectl, KubectlOutput, StoreSpec, ESO_VERSION};
use serde_json::json;

struct Scripted {
    calls: Mutex<Vec<String>>,
    helm_body: String,
    deploy_body: String,
    external_crd: String,
    push_crd: String,
    ownership: String,
    store_body: String,
    fail_writes: bool,
}

impl Scripted {
    fn absent() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            helm_body: "[]".to_string(),
            deploy_body: "Error from server (NotFound): deployments.apps \"external-secrets\" not found\n".to_string(),
            external_crd: "Error from server (NotFound): customresourcedefinitions.apiextensions.k8s.io \"externalsecrets.external-secrets.io\" not found\n".to_string(),
            push_crd: "Error from server (NotFound): customresourcedefinitions.apiextensions.k8s.io \"pushsecrets.external-secrets.io\" not found\n".to_string(),
            ownership: "Error from server (NotFound): configmaps \"curie-eso-install\" not found\n".to_string(),
            store_body: ready_store(),
            fail_writes: false,
        }
    }

    fn note(&self, kind: &str, args: &[String]) {
        self.calls
            .lock()
            .expect("calls")
            .push(format!("{kind}: {}", args.join(" ")));
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

impl curie::provider::bootstrap::Helm for Scripted {
    fn run(&self, args: &[String]) -> anyhow::Result<KubectlOutput> {
        self.note("helm", args);
        let joined = args.join(" ");
        if joined.contains("upgrade") {
            assert!(!self.fail_writes, "helm upgrade must not run");
            return Ok(ok("release installed"));
        }
        if joined.contains("uninstall") {
            assert!(!self.fail_writes, "helm uninstall must not run");
            return Ok(ok("release uninstalled"));
        }
        if joined.contains("list") {
            return Ok(ok(&self.helm_body));
        }
        Ok(fail("unexpected helm command"))
    }
}

impl Kubectl for Scripted {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> anyhow::Result<KubectlOutput> {
        self.note("kubectl", args);
        let joined = args.join(" ");
        if joined.contains(" apply ") || joined.ends_with(" apply") || joined.contains("apply ") {
            assert!(!self.fail_writes, "kubectl apply must not run: {joined}");
            let _ = stdin;
            return Ok(ok("applied"));
        }
        if joined.contains("get deploy") || joined.contains("get deployment") {
            return if self.deploy_body.contains("NotFound") {
                Ok(fail(&self.deploy_body))
            } else {
                Ok(ok(&self.deploy_body))
            };
        }
        if joined.contains("externalsecrets.external-secrets.io") {
            return if self.external_crd.contains("NotFound") {
                Ok(fail(&self.external_crd))
            } else {
                Ok(ok(&self.external_crd))
            };
        }
        if joined.contains("pushsecrets.external-secrets.io") {
            return if self.push_crd.contains("NotFound") {
                Ok(fail(&self.push_crd))
            } else {
                Ok(ok(&self.push_crd))
            };
        }
        if joined.contains(OWNERSHIP_CONFIGMAP) {
            return if self.ownership.contains("NotFound") || self.ownership.contains("unreachable")
            {
                Ok(fail(&self.ownership))
            } else {
                Ok(ok(&self.ownership))
            };
        }
        if joined.contains("get secretstore") {
            return Ok(ok(&self.store_body));
        }
        if joined.contains("delete") {
            assert!(!self.fail_writes, "kubectl delete must not run: {joined}");
            return Ok(ok("deleted"));
        }
        Ok(fail("unexpected kubectl command"))
    }
}

fn ok(stdout: &str) -> KubectlOutput {
    KubectlOutput {
        success: true,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

fn fail(stderr: &str) -> KubectlOutput {
    KubectlOutput {
        success: false,
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}

fn spec() -> StoreSpec {
    StoreSpec {
        name: "acme-harness-aws".to_string(),
        namespace: "acme-harness".to_string(),
        region: "us-east-1".to_string(),
        service_account: "acme-harness-aws-eso".to_string(),
        role_arn: "arn:aws:iam::000000000000:role/acme-harness".to_string(),
    }
}

fn ready_store() -> String {
    json!({
        "status": {"conditions": [{"type": "Ready", "status": "True", "reason": "Valid"}]}
    })
    .to_string()
}

fn compatible_deploy(image: &str, watch: Option<&str>) -> String {
    let mut args = vec!["--loglevel=info"];
    if let Some(namespace) = watch {
        args.push("--scoped-namespace");
        args.push(namespace);
    }
    json!({
        "spec": {"template": {"spec": {"containers": [{
            "name": "external-secrets",
            "image": image,
            "args": args,
        }]}}}
    })
    .to_string()
}

fn crd(version: &str) -> String {
    json!({"spec": {"versions": [
        {"name": version, "served": true},
        {"name": "v1beta1", "served": false}
    ]}})
    .to_string()
}

fn owned_map() -> String {
    json!({"data": {
        "installed-by": "curie",
        "chart-version": ESO_VERSION,
        "install-namespace": "acme-harness",
        "install-release": "acme-harness",
    }})
    .to_string()
}

fn this_install() -> InstallRef {
    InstallRef {
        namespace: "acme-harness".to_string(),
        release: "acme-harness".to_string(),
    }
}

fn compatible(script: &mut Scripted) {
    script.helm_body =
        json!([{"name": "external-secrets", "chart": "external-secrets-2.11.0"}]).to_string();
    script.deploy_body =
        compatible_deploy("ghcr.io/external-secrets/external-secrets:v2.11.0", None);
    script.external_crd = crd("v1");
    script.push_crd = crd("v1alpha1");
}

#[test]
fn minor_versions_match_only_the_pinned_line() {
    assert_eq!(minor_version("2.11.0"), Some((2, 11)));
    assert_eq!(minor_version("v2.11.3"), Some((2, 11)));
    assert_eq!(
        minor_version("ghcr.io/external-secrets/external-secrets:v2.11.0"),
        Some((2, 11))
    );
    assert_eq!(minor_version("external-secrets-2.11.1"), Some((2, 11)));
    assert_eq!(minor_version("2.10.0"), Some((2, 10)));
    assert_eq!(minor_version("2.12.0"), Some((2, 12)));
}

#[test]
fn an_empty_cluster_installs_and_a_match_reuses() {
    let empty = InstallationView {
        helm_chart: None,
        image: None,
        controller_present: false,
        external_secret_served: vec![],
        push_secret_served: vec![],
        watch_namespace: None,
    };
    assert_eq!(decide(&empty, "acme-harness"), Decision::Install);
    let compatible = InstallationView {
        helm_chart: Some("external-secrets-2.11.0".to_string()),
        image: Some("ghcr.io/external-secrets/external-secrets:v2.11.0".to_string()),
        controller_present: true,
        external_secret_served: vec!["v1".to_string()],
        push_secret_served: vec!["v1alpha1".to_string()],
        watch_namespace: None,
    };
    assert_eq!(decide(&compatible, "acme-harness"), Decision::Reuse);
    let scoped = InstallationView {
        watch_namespace: Some("other".to_string()),
        ..compatible
    };
    assert!(matches!(
        decide(&scoped, "acme-harness"),
        Decision::Refuse(_)
    ));
}

#[test]
fn ensure_installs_when_nothing_is_present_and_is_idempotent_on_reuse() {
    let install = Scripted::absent();
    let outcome = ensure(&install, &install, &spec(), &this_install(), None).expect("install");
    assert_eq!(outcome, EnsureOutcome::Installed);
    let calls = install.calls();
    assert!(
        calls
            .iter()
            .any(|call| call.contains("helm: upgrade --install")),
        "{calls:?}"
    );
    assert!(
        calls
            .iter()
            .any(|call| call.contains(ESO_CHART) && call.contains(ESO_VERSION)),
        "{calls:?}"
    );
    assert!(
        calls.iter().any(|call| call.contains("secretstore")),
        "{calls:?}"
    );

    let mut reuse = Scripted::absent();
    compatible(&mut reuse);
    reuse.ownership =
        "Error from server (NotFound): configmaps \"curie-eso-install\" not found\n".to_string();
    let outcome = ensure(&reuse, &reuse, &spec(), &this_install(), None).expect("reuse");
    assert_eq!(outcome, EnsureOutcome::Reused);
    let calls = reuse.calls();
    assert!(
        calls.iter().all(|call| !call.contains("upgrade")),
        "reuse must not reinstall: {calls:?}"
    );
    assert!(calls.iter().any(|call| call.contains("apply")));
}

#[test]
fn an_incompatible_controller_refuses_before_any_write() {
    let mut script = Scripted::absent();
    compatible(&mut script);
    script.deploy_body =
        compatible_deploy("ghcr.io/external-secrets/external-secrets:v2.10.0", None);
    script.fail_writes = true;
    let error = ensure(&script, &script, &spec(), &this_install(), None).expect_err("must refuse");
    let text = format!("{error:#}");
    assert!(text.contains("Nothing was changed"), "{text}");
    assert!(
        text.contains("2.10.0") || text.contains("v2.10.0"),
        "{text}"
    );
    assert!(
        script
            .calls()
            .iter()
            .all(|call| !call.contains("apply") && !call.contains("upgrade")),
        "{:?}",
        script.calls()
    );
}

#[test]
fn teardown_removes_only_a_controller_curie_installed() {
    let mut absent = Scripted::absent();
    absent.fail_writes = true;
    assert_eq!(
        remove_owned_controller(&absent, &absent, &this_install()).expect("absent"),
        ControllerTeardown::Retained
    );

    let mut foreign = Scripted::absent();
    foreign.ownership = json!({"data": {"installed-by": "someone-else"}}).to_string();
    foreign.fail_writes = true;
    assert_eq!(
        remove_owned_controller(&foreign, &foreign, &this_install()).expect("foreign"),
        ControllerTeardown::Retained
    );

    let mut other_install = Scripted::absent();
    other_install.ownership = owned_map();
    other_install.fail_writes = true;
    let other = InstallRef {
        namespace: "other-ns".to_string(),
        release: "other-release".to_string(),
    };
    assert_eq!(
        remove_owned_controller(&other_install, &other_install, &other).expect("other install"),
        ControllerTeardown::Retained
    );

    let mut unreadable = Scripted::absent();
    unreadable.ownership = "unreachable\n".to_string();
    unreadable.fail_writes = true;
    assert_eq!(
        remove_owned_controller(&unreadable, &unreadable, &this_install()).expect("unreadable"),
        ControllerTeardown::Unproven
    );

    let mut owned = Scripted::absent();
    owned.ownership = owned_map();
    assert_eq!(
        remove_owned_controller(&owned, &owned, &this_install()).expect("owned"),
        ControllerTeardown::Removed
    );
    let calls = owned.calls();
    assert!(
        calls.iter().any(|call| call.contains("uninstall")),
        "{calls:?}"
    );
    assert!(
        calls
            .iter()
            .any(|call| call.contains("delete") && call.contains(OWNERSHIP_CONFIGMAP)),
        "{calls:?}"
    );
}

#[test]
fn dry_run_names_the_pinned_chart_and_the_store() {
    let lines = dry_run_lines(&spec());
    let text = lines.join("\n");
    assert!(text.contains(ESO_VERSION));
    assert!(text.contains("secretstore acme-harness-aws"));
    assert!(text.contains("arn:aws:iam::000000000000:role/acme-harness"));
    assert!(!text.contains("sk-"));
}
