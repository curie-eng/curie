"""Kind-rung driver for the scripted factory scenario (#3814).

`curie dev factory-e2e scripted` installs the candidate chart into a namespace
on a disposable kind cluster, points GitHub at the #3815 stub, and points the
worker at a scripted Messages endpoint. The fixture tree is unitconv, not
this repository. The process deletes only the namespace it creates.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
import re
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import factory_e2e as fe
import scripted_scenario as scenario

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tools" / "factory-e2e" / "fixtures" / "unitconv"
TRANSCRIPT = REPO_ROOT / "tools" / "model-script" / "transcripts" / "unitconv-issue.json"


class ScriptedScenarioError(RuntimeError):
    """The kind scenario could not prove the fixture run."""


def fixture_bundle(workdir: Path) -> Path:
    """Keep factory behavior, using platform Python for this stdlib fixture.

    The original layer adds repository-specific uv/Rust/pnpm toolchains. This
    fixture does not claim qualification of that default release artifact.
    """
    target = workdir / "fixture-bundle"
    shutil.copytree(
        REPO_ROOT / "examples" / "dark-factory",
        target,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    (target / "connectors.yaml").write_text("connectors: {}\n")
    return target


def refuse_context(context: str) -> None:
    """Production and the shared k8 context are never this scenario's cluster."""

    if not context.startswith("kind-"):
        raise ScriptedScenarioError(
            f"refusing kube context {context!r}; create a disposable kind cluster"
        )


def scripted_values(
    *,
    model_base_url: str,
    github_api: str,
    clone_base: str,
    ca_configmap: str,
) -> dict[str, Any]:
    """Helm values for one scripted install. The model host is a pod IP."""

    config = fe.FactoryConfig(
        kube_context="kind-local",
        app_id="51",
        installation_id=5501,
        private_key_file=Path("app.pem"),
        repo="acme-corp/acme-bot",
        label="factory",
        mention="curie-factory-bot",
        cloudflared="cloudflared",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="scripted-webhook-secret",
        actor_token="scripted",
        model="scripted",
        model_context_tokens=None,
        model_base_url=model_base_url,
    )
    values = fe.install_values(
        config,
        candidate="local",
        app_key_secret="factory-app",
        consumer_controller=True,
        local_images=True,
    )
    values["api"]["githubApiUrl"] = github_api
    values["api"]["githubCloneBase"] = clone_base
    values["api"]["githubStubCaConfigMap"] = ca_configmap
    values["api"]["githubRepoAllowlist"] = ["acme-corp/acme-bot"]
    return values


def assert_observed(preflight_outcome: str, publication_paths: list[str]) -> None:
    """The two fixture facts the scenario exists to protect."""

    scenario.assert_preflight_outcome(preflight_outcome)
    scenario.assert_publication_paths(publication_paths)


def fixture_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ScriptedPreflight(fe.Preflight):
    """The existing driver, with only its external fixture endpoints adapted."""

    # Contains the chart name: canonical curie.fullname equals this release.
    release = "curie-factory-scripted"

    def __init__(self, *args: Any, github_stub: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.github_stub = github_stub
        self.github_html_base = github_stub.base_url
        self.ci_observations: list[dict[str, Any]] = []

    def work_item_detail(self, work_item_id: str) -> dict[str, Any] | None:
        detail = super().work_item_detail(work_item_id)
        if detail is not None and isinstance(detail.get("ci"), dict):
            self.ci_observations.append(dict(detail["ci"]))
        return detail

    def github(self, method: str, path: str, *, token: str, body: Any = None) -> tuple[int, Any]:
        request = Request(
            self.github_stub.base_url + "/api/v3" + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        context = ssl.create_default_context(cafile=str(self.github_stub.ca_file))
        try:
            with urlopen(request, context=context, timeout=30) as response:
                status, raw = response.status, response.read()
        except HTTPError as exc:
            status, raw = exc.code, exc.read()
        return status, json.loads(raw) if raw else None

    def github_diff(self, number: int) -> tuple[int, str]:
        # The fixture's bare repository is the actual received Git push.
        pull = self.github_stub._get("pull", number)
        if pull is None:
            return 404, ""
        fresh = self.github_stub._fresh_pull(pull)
        return 200, self.github_stub._run(
            "git",
            "--git-dir",
            str(self.github_stub._repository),
            "diff",
            fresh["base"]["sha"],
            fresh["head"]["sha"],
        ).decode()

    def check_images(self) -> None:
        nodes = json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]
        identities: dict[str, dict[str, str]] = {}
        for node in nodes:
            name = node["metadata"]["name"]
            identities[name] = {}
            for image in (
                "curie-api:local",
                "curie-worker:local",
                "curie-ui:local",
                "curie-runner:latest",
            ):
                inspected = json.loads(
                    fe.run(["docker", "exec", name, "crictl", "inspecti", image])
                )
                identities[name][image] = inspected["status"]["id"]
        if not identities:
            raise fe.PreflightFailed("kind has no nodes with candidate images")
        self.evidence["loaded_images"] = identities

    def extract_chart(self) -> Path:
        return self.repo_root / "charts" / "curie"

    def sweep_stale_namespaces(self) -> None:
        # This execution never acquires cleanup ownership of previous runs.
        self.step("existing namespaces preserved")

    def egress_cidrs(self) -> list[str]:
        return []

    def model_usage(self) -> float | None:
        # Provider authentication and billing remain outside the product box.
        return None

    def install(self) -> None:
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "configmap",
            "github-stub-ca",
            f"--from-file=ca.pem={self.github_stub.ca_file}",
        )
        super().install()

    def installation_values(self, config: fe.FactoryConfig, **kwargs: Any) -> dict[str, Any]:
        values = fe.install_values(config, **kwargs, local_images=True)
        values["api"].update(
            {
                "githubApiUrl": self.github_stub.base_url + "/api/v3",
                "githubCloneBase": self.github_stub.base_url,
                "githubStubCaConfigMap": "github-stub-ca",
            }
        )
        values["dispatcher"]["deploy"] = False
        values["security"]["networkPolicy"]["allowedEgress"].append(
            fe.model_proxy_egress(self.github_stub.base_url)
        )
        return values

    def tunnel(self) -> None:
        # Both external fixtures are on this CI host; the real API remains in kind.
        self.ensure_api()
        self.tunnel_url = self.api_url
        self.step("external fixture reaches owned API port-forward")

    def ensure_tunnel(self) -> None:
        self.ensure_api()
        if self.tunnel_url != self.api_url:
            self.tunnel()
            self.point_card_at_tunnel()
            self._patch_webhook(self.tunnel_url + "/github/webhook")


def run_scenario(preflight: ScriptedPreflight) -> dict[str, Any]:
    result = fe.issue_to_pr(preflight)
    request = str(uuid.UUID(preflight.evidence["execution_request_id"]))
    work_item = str(uuid.UUID(preflight.evidence["work_item_id"]))
    lineage = preflight.sql(
        "SELECT l.head_sha FROM thread_publication_lineages l "
        "JOIN work_items w ON w.publication_lineage_id = l.id "
        f"WHERE w.id = '{work_item}'"
    )
    if len(lineage) != 1 or not lineage[0][0]:
        raise ScriptedScenarioError("no actual published lineage head")
    scenario.assert_ci_completion(result, preflight.ci_observations, lineage[0][0])
    result["ci_observations"] = preflight.ci_observations
    observations = preflight.sql(
        "SELECT note FROM execution_request_phase_reports "
        f"WHERE execution_request_id = '{request}' AND phase = 'verification_preflight' ORDER BY id"
    )
    records = [json.loads(row[0]) for row in observations]
    matching = [r for r in records if r.get("command") == " ".join(scenario.FIXTURE_CHECK)]
    if not matching:
        raise ScriptedScenarioError("no actual declared fixture preflight observation")
    paths = preflight.sql(
        "SELECT changed_paths::text FROM publications "
        f"WHERE execution_request_id = '{request}' ORDER BY created_at"
    )
    changed = [path for row in paths for path in json.loads(row[0])]
    for record in matching:
        assert_observed(record["outcome"], changed)
    result["fixture_preflight"] = matching
    result["publication_paths"] = changed
    return result


def collect_failure_diagnostics(preflight: ScriptedPreflight) -> dict[str, Any]:
    """Read bounded status and hook logs before deleting this run's namespace.

    Never fetch Secrets, pod specs, environment values, or another namespace.
    A missing or mismatched ownership marker refuses every workload query.
    """
    base = ["kubectl", "--context", preflight.config.kube_context, "--request-timeout=10s"]

    def capture(args: list[str]) -> dict[str, Any]:
        try:
            result = subprocess.run(
                [*base, *args], capture_output=True, text=True, check=False, timeout=15
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"error": type(exc).__name__}
        output = result.stdout[-12000:] if result.returncode == 0 else ""
        for name in ("model_api_key", "actor_token", "webhook_secret"):
            secret = getattr(preflight.config, name, None)
            if secret:
                output = output.replace(secret, "[redacted]")
        output = re.sub(r"(\w+://)[^\s/@]+:[^\s/@]+@", r"\1[redacted]@", output)
        return {"exit_code": result.returncode, "output": output}

    marker = capture(
        [
            "get",
            "namespace",
            preflight.namespace,
            "--ignore-not-found",
            "-o",
            "jsonpath={.metadata.annotations.curie\\.dev/factory-e2e-run}",
        ]
    )
    if marker.get("output", "").strip() != preflight.run_id:
        return {"ownership": "unverified", "read_error": marker.get("error")}
    prefix = ["-n", preflight.namespace]
    queries = {
        "pods": [
            "get",
            "pods",
            "-o",
            "custom-columns=NAME:.metadata.name,PHASE:.status.phase,REASON:.status.reason,"
            "NODE:.spec.nodeName,WAITING:.status.containerStatuses[*].state.waiting.reason,"
            "TERMINATED:.status.containerStatuses[*].state.terminated.reason,"
            "RESTARTS:.status.containerStatuses[*].restartCount",
        ],
        "jobs": [
            "get",
            "jobs",
            "-o",
            "custom-columns=NAME:.metadata.name,ACTIVE:.status.active,SUCCEEDED:.status.succeeded,"
            "FAILED:.status.failed,REASONS:.status.conditions[*].reason",
        ],
        "events": [
            "get",
            "events",
            "--sort-by=.metadata.creationTimestamp",
            "-o",
            "custom-columns=OBJECT:.involvedObject.name,REASON:.reason,MESSAGE:.message",
        ],
        "schema_log": [
            "logs",
            f"job/{preflight.release}-schema-migrate",
            "--all-containers=true",
            "--tail=80",
            "--limit-bytes=12000",
            "--pod-running-timeout=5s",
        ],
    }
    return {
        "ownership": "verified",
        **{name: capture([*prefix, *args]) for name, args in queries.items()},
    }


def finish_run(
    preflight: ScriptedPreflight | None,
    fixtures: fe.Teardown,
    model: Any,
    *,
    code: int,
    record: bool,
) -> tuple[int, list[dict[str, Any]]]:
    if preflight is not None and code != 0:
        try:
            diagnostics = collect_failure_diagnostics(preflight)
        except BaseException as exc:  # noqa: BLE001 - diagnostics cannot prevent owned cleanup
            diagnostics = {"error": type(exc).__name__}
        preflight.evidence["failure_diagnostics"] = diagnostics
        print(f"factory-e2e: failure diagnostics {json.dumps(diagnostics)}", file=sys.stderr)
    cleanup = preflight.teardown.run() if preflight is not None else []
    cleanup += fixtures.run()
    if not all(result["ok"] for result in cleanup) or (model is not None and not model._exchanges):
        code = fe.EXIT_FAILED
    if preflight is not None:
        preflight.evidence.update(
            result="passed" if code == 0 else "failed",
            teardown=cleanup,
            teardown_clean=all(r["ok"] for r in cleanup),
            finished_at=dt.datetime.now(dt.UTC).isoformat(),
            model_mode="provider recording" if record else "strict replay",
            model_exchanges=len(model._exchanges),
        )
        preflight.write_evidence()
        shutil.rmtree(preflight.workdir, ignore_errors=True)
    for result in cleanup:
        print(
            f"factory-e2e: cleanup {result['step']}: {'ok' if result['ok'] else 'FAILED'}",
            file=sys.stderr,
        )
    return code, cleanup


def main(args: argparse.Namespace) -> int:
    record = getattr(args, "record", False)
    transcript = Path(args.transcript).resolve()
    try:
        refuse_context(args.context)
        namespace = fe.validate_namespace(args.namespace)
        if args.model_base_url:
            raise fe.ConfigError(
                "scripted owns its model proxy; use --listen-host for pod reachability"
            )
        if not record:
            recorded = json.loads(transcript.read_text())
            if recorded.get("version") != 1 or not recorded.get("exchanges"):
                raise fe.ConfigError("the factory transcript contains no recorded exchanges")
        credential = os.environ.get("CURIE_FACTORY_MODEL_API_KEY") if record else None
        if record and not credential:
            raise fe.ConfigError("recording needs CURIE_FACTORY_MODEL_API_KEY on the fixture host")
        if record and transcript.exists():
            raise fe.ConfigError("recording refuses to overwrite an existing transcript")
        host = getattr(args, "listen_host", None) or os.environ.get("CURIE_E2E_LISTEN_HOST")
        if not host:
            raise fe.ConfigError("--listen-host must name the kind-reachable fixture host IP")
        # Validate the actual network-policy endpoint before any fixture or namespace starts.
        fe.model_proxy_egress(f"http://{host}:1")
    except (fe.ConfigError, ScriptedScenarioError, OSError, ValueError, AttributeError) as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return fe.EXIT_CONFIG
    stub_module = fixture_module(
        "factory_github_stub", REPO_ROOT / "tools/github-stub/github_stub.py"
    )
    model_module = fixture_module(
        "factory_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    stage = REPO_ROOT / ".projects" / "factory-scripted"
    stage.mkdir(parents=True, exist_ok=True)
    evidence = getattr(args, "evidence", None) or stage / f"{namespace}.json"
    code = fe.EXIT_FAILED
    preflight: ScriptedPreflight | None = None
    model: Any = None
    fixtures = fe.Teardown()
    previous_signals = {}

    def terminate(signum: int, _frame: Any) -> None:
        raise SystemExit(128 + signum)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous_signals[sig] = signal.signal(sig, terminate)
    with tempfile.TemporaryDirectory(prefix="fixture-", dir=stage) as temporary:
        workdir = Path(temporary)
        try:
            bundle = fixture_bundle(workdir)
            key = workdir / "app.pem"
            fe.run(["openssl", "genrsa", "-out", str(key), "2048"])
            os.chmod(key, 0o600)
            github_recording = json.loads(
                (REPO_ROOT / "tools/github-stub/recordings/curie-pr-3400.json").read_text()
            )
            stub = stub_module.GithubStub(
                workdir / "github",
                github_recording,
                host=host,
                bind="0.0.0.0",
                seed_tree=FIXTURE,
                actor_token="example-actor-token",
                webhook_secret="example-webhook-secret",
                clock_scale=20,
            )
            fixtures.push("close GitHub fixture", lambda: stub.close() or {"closed": True})
            stub.start()
            # Preserve recorded timing relative to the first actual pull request.
            model = model_module.ModelScript(
                transcript,
                record=record,
                require_consumed=True,
                upstream=getattr(args, "upstream", "https://openrouter.ai/api"),
                upstream_api_key=credential,
                host="0.0.0.0",
            )
            fixtures.push("close model fixture", lambda: model.close() or {"closed": True})
            model.start()
            config = fe.FactoryConfig(
                kube_context=args.context,
                app_id="51",
                installation_id=5501,
                private_key_file=key,
                repo="acme-corp/acme-bot",
                label="factory",
                mention="example-app",
                cloudflared="kubectl",
                priority_classes=("curie-platform", "curie-sandbox"),
                restore_webhook_url=None,
                webhook_secret="example-webhook-secret",
                actor_token="example-actor-token",
                operator_login="example-operator",
                model_api_key="example-proxy-token",
                model=getattr(args, "model", None) or fe.DEFAULT_MODEL,
                model_context_tokens=None,
                model_base_url=f"http://{host}:{model.port}",
                curie_bin=os.environ.get("CURIE_BIN", "curie"),
                bundle_dir=bundle,
            )
            candidate = fe.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"]).strip()
            preflight = ScriptedPreflight(
                config,
                github_stub=stub,
                repo_root=REPO_ROOT,
                candidate=candidate,
                namespace=namespace,
                evidence_path=Path(evidence),
                admission_timeout=120,
                issue_spec=(
                    "Add Kelvin temperature conversion",
                    "Support k alongside c and f in unitconv/convert.py. "
                    "Add tests in unitconv/tests/test_convert.py for 0 c = 273.15 k, "
                    "32 f = 273.15 k, and Kelvin round trips. Keep length conversions "
                    "and category rejection unchanged. Run the repository's declared "
                    "unittest command and publish both changed Python files.",
                ),
                expect="pr",
                scenario_name="issue-to-pr",
            )
            preflight.evidence["mode"] = "scripted:record" if record else "scripted:replay"
            preflight.evidence["image_tag"] = "kind-loaded-candidate"
            preflight.evidence["github_fixture_clock_scale"] = stub.clock_scale
            preflight.evidence["fixture_bundle_difference"] = (
                "connectors.yaml: repository toolchain runner declaration removed"
            )
            digest = hashlib.sha256()
            for path in sorted(bundle.rglob("*")):
                if path.is_file():
                    digest.update(
                        str(path.relative_to(bundle)).encode() + b"\0" + path.read_bytes()
                    )
            preflight.evidence["fixture_bundle_sha256"] = digest.hexdigest()
            preflight.run(run_scenario)
            preflight.evidence["result"] = "passed"
            code = 0
        except BaseException as exc:  # noqa: BLE001 - owned cleanup runs on interrupts too
            print(f"factory-e2e: scripted failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            if preflight is not None:
                preflight.evidence.update(result="failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            code, _ = finish_run(preflight, fixtures, model, code=code, record=record)
            for sig, handler in previous_signals.items():
                signal.signal(sig, handler)
    return code


if __name__ == "__main__":
    raise SystemExit(main(fe.parse_args(sys.argv[1:])))
