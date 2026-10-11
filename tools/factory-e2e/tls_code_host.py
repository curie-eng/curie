"""The tls-code-host factory scenario (#4398 decision 2).

`curie dev factory-e2e run --scenario tls-code-host` runs the recorded TLS
GitHub stub (tools/github-stub/github_stub.py) as one Pod in the test
namespace, on the candidate worker image (it carries python3, git and openssl).
The stub generates a private test CA; only its public certificate leaves the
Pod, as the ConfigMap `code-host-ca` the install trusts through
`codeHostTrust.caBundle.configMapRef`. The api's GitHub API and clone bases
point at the stub's Service name, sandbox and publication egress allow only the
stub Pod and the model provider, and a real model runs the default
dark-factory bundle.

The ticket is the stub's seeded issue 3815, labelled `factory` by its human
fixture actor. An issue the stub creates through its API is labelled by the
App's bot identity, which factory admission ignores by design, so the driver
rewrites 3815's title and body instead. It keeps 3815 closed until the bundle
is deployed, so the poller cannot admit it early, then reopens it.

The judge reads: the admitted WorkItem request reached running and ended
completed, the sandbox claim carried CURIE_REPO_CA_BUNDLE at the code host trust
path, the install trusted the stub only through codeHostTrust, the request's
publication record ended succeeded, the stub's bare repository holds a pushed
branch and exactly one pull request, the stub recorded no unsupported request,
and no api, worker, publication or stub log shows a TLS verification failure.
A publication Job observed by the best-effort watch must mount the
`code-host-trust` volume from `code-host-ca`; one never observed does not fail
the run (judge_tls_code_host has the reasoning).

Nothing here reaches real GitHub: no App JWT, actor token, webhook, tunnel,
fixture repository or GitHub /meta call. The App private key the chart needs is
generated per run and never leaves the run's private directory and the
namespace Secret. Standard library only.
"""

from __future__ import annotations

import copy
import dataclasses
import ipaddress
import json
import os
import re
import secrets
import shutil
import subprocess
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import factory_e2e as fe

SCENARIO = "tls-code-host"
STUB_NAME = "github-stub"
STUB_SOURCE_CONFIG_MAP = "github-stub-source"
STUB_COMPONENT_LABEL = "app.kubernetes.io/name"
STUB_PORT_DEFAULT = 8443
# The publication Job's egress policy opens TCP 443 only, and a headless
# Service does no port mapping, so the live stub listens on 443 itself.
STUB_LIVE_PORT = 443
STUB_SOURCE = Path(__file__).resolve().parents[1] / "github-stub" / "github_stub.py"
STUB_RECORDING = (
    Path(__file__).resolve().parents[1] / "github-stub" / "recordings" / "curie-pr-3400.json"
)
STUB_STATE = "/state"
STUB_CA_FILE = f"{STUB_STATE}/ca.pem"
STUB_DATABASE = f"{STUB_STATE}/state.sqlite3"

# The stub fixture's own identities (tools/github-stub/README.md).
STUB_REPO = "acme-corp/acme-bot"
STUB_APP_ID = "51"
STUB_INSTALLATION_ID = 5501
STUB_ISSUE = 3815
STUB_LABEL = "factory"
STUB_MENTION = "example-app"
STUB_BARE_REPO = f"{STUB_STATE}/repositories/{STUB_REPO}.git"

CA_CONFIG_MAP = "code-host-ca"
CA_KEY = "ca.crt"
TRUST_VOLUME = "code-host-trust"
CA_PATH = "/etc/curie/code-host-trust/ca.crt"
SYSTEM_ROOTS = "/etc/ssl/certs/ca-certificates.crt"
STUB_START_TIMEOUT_SECONDS = 600
TERMINAL_STATUSES = frozenset({"completed", "failed", "expired", "cancelled"})
# The worker names a publication's Job after its publication id
# (publication_k8s.publication_resource_names): the first 20 hex digits.
_PUBLICATION_JOB_NAME = re.compile(r"^curie-publication-[0-9a-f]{20}$")

DEFAULT_TLS_ISSUE = (
    "Add a greet function to example.py",
    "In `example.py`, add a function `greet(name: str) -> str` that returns "
    '`f"Hello, {name}!"`. Keep the existing `example()` function unchanged.\n\n'
    "Add `test_example.py` using the standard library `unittest` module with two "
    'tests: `greet("Curie")` returns `"Hello, Curie!"`, and `example()` still '
    'returns `"example"`. Run `python3 -m unittest` and make sure both pass before '
    "publishing.",
)

_TLS_FAILURE = re.compile(
    r"SSL certificate problem"
    r"|CERTIFICATE_VERIFY_FAILED"
    r"|certificate verify failed"
    r"|server certificate verification failed"
    r"|SSLCertVerificationError"
    r"|unable to get local issuer certificate"
    r"|self[- ]signed certificate in certificate chain"
    r"|alert unknown ca",
    re.IGNORECASE,
)
_UNSUPPORTED_LINE = re.compile(r"unsupported_request: ([A-Z]+ \S+)")

_OWNER_KEY, _OWNER_VALUE = fe.OWNER_LABEL.split("=", 1)


# --------------------------------------------------------------------------
# Pure pieces
# --------------------------------------------------------------------------


def stub_host(namespace: str) -> str:
    """The stub Service's in-cluster DNS name, the name its certificate carries."""

    return f"{STUB_NAME}.{namespace}.svc.cluster.local"


def stub_manifests(
    namespace: str, image: str, port: int = STUB_PORT_DEFAULT
) -> list[dict[str, Any]]:
    """The stub's ConfigMap (source and recording), Pod and headless Service. Pure
    except for reading the stub source and recording from this checkout.

    The Pod runs once on ``image`` as a non-root user with no service account
    token. Its CA key, server key, SQLite state and bare repository live in an
    emptyDir only it mounts. The Service is headless, so its name resolves to
    the Pod IP and the sandbox and publication egress rules can name that IP.
    """

    labels = {_OWNER_KEY: _OWNER_VALUE}
    selector = {STUB_COMPONENT_LABEL: STUB_NAME}
    source = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": STUB_SOURCE_CONFIG_MAP, "namespace": namespace, "labels": labels},
        "data": {
            "github_stub.py": STUB_SOURCE.read_text(),
            STUB_RECORDING.name: STUB_RECORDING.read_text(),
        },
    }
    pod_security: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "runAsGroup": 1000,
        "fsGroup": 1000,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if port < 1024:
        # A safe, namespaced sysctl: lets the non-root stub bind a low port.
        pod_security["sysctls"] = [{"name": "net.ipv4.ip_unprivileged_port_start", "value": "0"}]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": STUB_NAME,
            "namespace": namespace,
            "labels": {**labels, **selector},
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "securityContext": pod_security,
            "containers": [
                {
                    "name": STUB_NAME,
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python3", "/stub/github_stub.py", "serve"],
                    "args": [
                        "--root",
                        STUB_STATE,
                        "--recording",
                        f"/stub/{STUB_RECORDING.name}",
                        "--bind",
                        "0.0.0.0",
                        "--host",
                        stub_host(namespace),
                        "--port",
                        str(port),
                    ],
                    "ports": [{"name": "https", "containerPort": port, "protocol": "TCP"}],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {
                        "requests": {"cpu": "50m", "memory": "128Mi"},
                        "limits": {"memory": "512Mi"},
                    },
                    "volumeMounts": [
                        {"name": "stub", "mountPath": "/stub", "readOnly": True},
                        {"name": "state", "mountPath": STUB_STATE},
                    ],
                }
            ],
            "volumes": [
                {"name": "stub", "configMap": {"name": STUB_SOURCE_CONFIG_MAP}},
                {"name": "state", "emptyDir": {}},
            ],
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": STUB_NAME, "namespace": namespace, "labels": labels},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "None",
            "selector": selector,
            "ports": [{"name": "https", "port": port, "targetPort": port, "protocol": "TCP"}],
        },
    }
    return [source, pod, service]


def _host_cidr(address: str) -> str:
    parsed = ipaddress.ip_address(address)
    return f"{parsed}/{parsed.max_prefixlen}"


def tls_values(
    base: Mapping[str, Any],
    *,
    namespace: str,
    ca_config_map: str,
    stub_cluster_ip: str,
    port: int,
    model_cidrs: Sequence[str],
) -> dict[str, Any]:
    """Install values for the stub code host, layered on ``fe.install_values``. Pure.

    ``stub_cluster_ip`` is the address the stub's Service name resolves to (the
    Pod IP behind the headless Service). Sandbox egress becomes exactly that
    address on ``port`` plus each model CIDR on 443, and the publication Job's
    code host egress is that address alone.
    """

    values: dict[str, Any] = copy.deepcopy(dict(base))
    origin = f"https://{stub_host(namespace)}:{port}"
    stub_cidr = _host_cidr(stub_cluster_ip)
    api = values.setdefault("api", {})
    api.update(
        {
            "githubApiUrl": f"{origin}/api/v3",
            "githubCloneBase": origin,
            "githubRepoAllowlist": [STUB_REPO],
            "githubAppId": STUB_APP_ID,
            "githubFactoryIntake": "poll",
            "githubFactoryIngressEnabled": True,
            # Trust comes only through codeHostTrust, never the legacy stub path.
            "githubStubCaConfigMap": "",
        }
    )
    values["codeHostTrust"] = {"caBundle": {"configMapRef": {"name": ca_config_map, "key": CA_KEY}}}
    egress = [{"cidr": stub_cidr, "ports": [{"protocol": "TCP", "port": port}]}]
    egress += [{"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}]} for cidr in model_cidrs]
    security = values.setdefault("security", {})
    security["networkPolicy"] = {**(security.get("networkPolicy") or {}), "allowedEgress": egress}
    worker = values.setdefault("worker", {})
    worker["publication"] = {**(worker.get("publication") or {}), "githubHttpsCidrs": [stub_cidr]}
    return values


def install_trust(values: Mapping[str, Any]) -> dict[str, Any]:
    """The code host trust settings out of install values, for the judge. Pure."""

    return {
        "codeHostTrust": copy.deepcopy(values.get("codeHostTrust")),
        "githubStubCaConfigMap": (values.get("api") or {}).get("githubStubCaConfigMap"),
    }


def parse_stub_startup(log_text: str) -> dict[str, str]:
    """The stub's one JSON startup object, found among its other output."""

    for line in log_text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if not isinstance(parsed, dict):
            continue
        found = {key: parsed.get(key) for key in ("base_url", "clone_url", "ca_file")}
        if not all(isinstance(value, str) and value for value in found.values()):
            continue
        if not (
            str(found["base_url"]).startswith("https://")
            and str(found["clone_url"]).startswith("https://")
        ):
            continue
        return {key: str(value) for key, value in found.items()}
    raise fe.PreflightFailed(
        "the GitHub stub printed no HTTPS startup object (base_url, clone_url, ca_file)"
    )


def load_tls_config(env: Mapping[str, str], *, context: str | None) -> fe.FactoryConfig:
    """The scenario's inputs: a model key and a kube context, nothing GitHub.

    The App identity is the stub fixture's. The App private key is generated
    per run (``private_key_file`` is replaced then), and the webhook secret is a
    fresh random value no delivery ever uses.
    """

    missing: list[str] = []
    kube_context = context or env.get("CURIE_FACTORY_KUBE_CONTEXT", "")
    if not kube_context:
        missing.append("CURIE_FACTORY_KUBE_CONTEXT (or --context)")
    model_api_key = env.get("CURIE_FACTORY_MODEL_API_KEY", "")
    if not model_api_key:
        missing.append(
            "CURIE_FACTORY_MODEL_API_KEY (the stub run needs a real model to open a pull request)"
        )
    bundle_dir = (
        Path(env["CURIE_FACTORY_BUNDLE_DIR"]).expanduser()
        if env.get("CURIE_FACTORY_BUNDLE_DIR")
        else fe.DEFAULT_BUNDLE
    )
    if not bundle_dir.is_dir():
        missing.append("CURIE_FACTORY_BUNDLE_DIR (a plugin bundle directory)")
    model = env.get("CURIE_FACTORY_MODEL") or fe.DEFAULT_MODEL
    context_tokens: int | None = (
        fe.DEFAULT_MODEL_CONTEXT_TOKENS if model == fe.DEFAULT_MODEL else None
    )
    raw_tokens = env.get("CURIE_FACTORY_MODEL_CONTEXT_TOKENS", "")
    if raw_tokens:
        if raw_tokens.isdigit() and int(raw_tokens) > 0:
            context_tokens = int(raw_tokens)
        else:
            missing.append("CURIE_FACTORY_MODEL_CONTEXT_TOKENS (a positive integer)")
    priority_classes: tuple[str, str] | None = None
    if env.get("CURIE_FACTORY_PRIORITY_CLASSES"):
        parts = [part.strip() for part in env["CURIE_FACTORY_PRIORITY_CLASSES"].split(",")]
        if len(parts) == 2 and all(parts):
            priority_classes = (parts[0], parts[1])
        else:
            missing.append("CURIE_FACTORY_PRIORITY_CLASSES ('<platform>,<sandbox>')")
    if missing:
        raise fe.ConfigError(
            f"{SCENARIO} needs "
            + "; ".join(missing)
            + ". It serves its own TLS GitHub stub, so it reads no GitHub App, webhook, "
            "actor or fixture repository setting."
        )
    return fe.FactoryConfig(
        kube_context=kube_context,
        app_id=STUB_APP_ID,
        installation_id=STUB_INSTALLATION_ID,
        private_key_file=Path("generated-per-run.pem"),
        repo=STUB_REPO,
        label=STUB_LABEL,
        mention=STUB_MENTION,
        cloudflared="cloudflared",
        priority_classes=priority_classes,
        restore_webhook_url=None,
        webhook_secret=secrets.token_hex(24),
        actor_token="",
        model_api_key=model_api_key,
        model=model,
        model_context_tokens=context_tokens,
        bundle_dir=bundle_dir,
        curie_bin=env.get("CURIE_FACTORY_CURIE_BIN") or "curie",
        layer_registry=env.get("CURIE_FACTORY_LAYER_REGISTRY") or None,
        operator_login=None,
        model_base_url=None,
    )


def _job_status_summary(job: Mapping[str, Any]) -> str:
    status = job.get("status") or {}
    conditions = [
        f"{c.get('type')}={c.get('status')}"
        for c in status.get("conditions") or []
        if isinstance(c, dict)
    ]
    return (
        f"succeeded={status.get('succeeded') or 0} failed={status.get('failed') or 0} "
        f"conditions={conditions or 'none'}"
    )


def _watch_events(text: str) -> list[dict[str, Any]]:
    """The watch events in a `kubectl get --watch --output-watch-events -o json` stream.

    kubectl writes one indented JSON object per event, back to back. Anything
    that does not decode (a torn final object, a stray stderr line) is skipped
    a line at a time, so one bad event never hides the ones after it.
    """

    decoder = json.JSONDecoder()
    events: list[dict[str, Any]] = []
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        try:
            value, index = decoder.raw_decode(text, index)
        except ValueError:
            newline = text.find("\n", index)
            index = len(text) if newline < 0 else newline + 1
            continue
        if (
            isinstance(value, dict)
            and isinstance(value.get("type"), str)
            and isinstance(value.get("object"), dict)
        ):
            events.append(value)
    return events


def publication_job_from_watch(text: str) -> dict[str, Any] | None:
    """The first publication Job a Job watch reported, reduced to what the judge reads.

    The worker creates the Job and deletes it as soon as it settles the
    outcome, so the Job may exist only between two polls. A watch still
    reports it (ADDED, then DELETED with the final object), and any of those
    events carries the immutable pod template, so its volumes come from
    whichever event has them. Status is the last one reported, for the
    failure message only; completion is judged from the publication record.
    Jobs not named like a publication Job are ignored, and only the first
    publication Job reported is this run's.
    """

    name: str | None = None
    volumes: list[Any] = []
    status: dict[str, Any] = {}
    namespace: Any = None
    for event in _watch_events(text):
        job = event["object"]
        if job.get("kind") not in (None, "Job"):
            continue
        meta = job.get("metadata") or {}
        job_name = str(meta.get("name") or "")
        if not _PUBLICATION_JOB_NAME.match(job_name):
            continue
        if name is None:
            name = job_name
        elif job_name != name:
            continue
        namespace = meta.get("namespace") or namespace
        template = ((job.get("spec") or {}).get("template") or {}).get("spec") or {}
        if template.get("volumes"):
            volumes = list(template["volumes"])
        if job.get("status"):
            status = dict(job["status"])
    if name is None:
        return None
    return {
        "kind": "Job",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"template": {"spec": {"volumes": volumes}}},
        "status": status,
    }


def tls_failure_lines(text: str) -> list[str]:
    """Distinct log lines that report a failed TLS verification."""

    found: list[str] = []
    for line in text.splitlines():
        if _TLS_FAILURE.search(line):
            trimmed = line.strip()[:300]
            if trimmed not in found:
                found.append(trimmed)
    return found


def judge_tls_code_host(obs: Mapping[str, Any]) -> list[str]:
    """Every way the run missed the TLS code host contract; empty means passed.

    The proof that publication trusted the stub through codeHostTrust is
    behavioral and durable, not the publication Job object, which the worker
    deletes as soon as it settles the outcome. It is the conjunction of:

    1. the install values set ``codeHostTrust.caBundle.configMapRef`` to
       ``code-host-ca`` and left ``api.githubStubCaConfigMap`` empty, so
       codeHostTrust is the only path by which any component can trust the
       stub's CA, which the stub generated for this run alone;
    2. the publication record for the admitted request ended ``succeeded``,
       which the worker writes once the Job's post-push marker reports a
       verified push and the pull request was read back from the stub over
       TLS;
    3. the stub's bare repository holds a pushed non-main branch;
    4. no observed api, worker, publication or stub log reports a TLS
       verification failure.

    Together these mean publication spoke TLS to the stub and verified it,
    which only the codeHostTrust CA could make succeed. The Job object is a
    diagnostic: when a publication Job was observed it must carry the
    code-host-trust volume from ``code-host-ca`` (a contradiction fails), and
    not observing one is recorded as ``publication_job_observed: false``
    without failing.
    """

    failures: list[str] = []
    if not obs.get("work_item_id"):
        failures.append("no WorkItem was admitted for the stub's labelled issue")

    statuses = [str(status) for status in obs.get("request_statuses") or []]
    if not statuses:
        failures.append("no execution request status was observed")
    else:
        if "running" not in statuses:
            failures.append(
                f"the request never reached running (statuses: {', '.join(statuses)}), "
                "so no sandbox cloned the stub repository"
            )
        if statuses[-1] != "completed":
            failures.append(f"the request did not end completed: it ended {statuses[-1]}")

    claim_env = obs.get("claim_env") or {}
    bundle = claim_env.get("CURIE_REPO_CA_BUNDLE")
    if not bundle:
        failures.append(
            "the sandbox claim env has no CURIE_REPO_CA_BUNDLE, so the snapshot clone did "
            "not record the code host trust"
        )
    elif bundle != CA_PATH:
        failures.append(f"the sandbox claim's CURIE_REPO_CA_BUNDLE is {bundle}, not {CA_PATH}")

    trust = obs.get("install_trust") or {}
    config_map_ref = ((trust.get("codeHostTrust") or {}).get("caBundle") or {}).get(
        "configMapRef"
    ) or {}
    if config_map_ref.get("name") != CA_CONFIG_MAP or config_map_ref.get("key") != CA_KEY:
        failures.append(
            "the install values did not trust the stub through codeHostTrust "
            f"(caBundle.configMapRef: {config_map_ref or 'none'}, "
            f"expected {CA_CONFIG_MAP}/{CA_KEY})"
        )
    if trust.get("githubStubCaConfigMap") != "":
        failures.append(
            "the install values set api.githubStubCaConfigMap to "
            f"{trust.get('githubStubCaConfigMap')!r}, so codeHostTrust was not the only "
            "trust path for the stub"
        )

    job = obs.get("publication_job")
    if job:
        volumes = ((job.get("spec") or {}).get("template") or {}).get("spec", {}).get(
            "volumes"
        ) or []
        trust_volume = next(
            (v for v in volumes if isinstance(v, dict) and v.get("name") == TRUST_VOLUME), None
        )
        if trust_volume is None:
            failures.append(f"the observed publication Job has no {TRUST_VOLUME} volume")
        else:
            source = (trust_volume.get("configMap") or {}).get("name")
            if source != CA_CONFIG_MAP:
                failures.append(
                    f"the observed publication Job's {TRUST_VOLUME} volume is ConfigMap "
                    f"{source!r}, not {CA_CONFIG_MAP}"
                )

    # Completion is the durable publication record, not the Job's status:
    # the worker deletes the Job as soon as it settles the outcome.
    record = obs.get("publication_record") or {}
    status = record.get("status")
    if status != "succeeded":
        reason = (
            f"its publication record is {status!r}"
            + (f" ({record['error']})" if record.get("error") else "")
            if record
            else "no publication record exists for the request"
        )
        seen = f" (last Job status seen: {_job_status_summary(job)})" if job else ""
        failures.append(f"the publication did not succeed: {reason}{seen}")

    branches = [str(branch) for branch in obs.get("stub_branches") or []]
    if not [branch for branch in branches if branch != "main"]:
        failures.append(
            f"the stub repository has no pushed branch (branches: {branches or 'none'})"
        )

    pulls = list(obs.get("stub_pull_requests") or [])
    if len(pulls) != 1:
        failures.append(f"the stub shows {len(pulls)} pull requests, expected exactly 1")

    for request in obs.get("stub_unsupported") or []:
        failures.append(f"the stub recorded an unsupported request: {request}")

    for line in tls_failure_lines(str(obs.get("logs") or ""))[:5]:
        failures.append(f"TLS verification failure in the logs: {line}")
    return failures


# --------------------------------------------------------------------------
# The live driver
# --------------------------------------------------------------------------

# Run inside the stub Pod: one REST call over loopback, verified against the
# stub's CA (its certificate names 127.0.0.1 too). argv: method, path, body, port.
_STUB_CLIENT = """
import json, ssl, sys, urllib.error, urllib.request
method, path, body, port = sys.argv[1:5]
context = ssl.create_default_context(cafile="@CA_FILE@")
request = urllib.request.Request(
    "https://127.0.0.1:" + port + "/api/v3" + path,
    data=body.encode() if body else None,
    method=method,
    headers={"Accept": "application/vnd.github+json", "Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(request, context=context, timeout=30) as response:
        status, raw = response.status, response.read()
except urllib.error.HTTPError as error:
    status, raw = error.code, error.read()
print(json.dumps({"status": status, "body": json.loads(raw or b"null")}))
""".replace("@CA_FILE@", STUB_CA_FILE)

_STUB_UNSUPPORTED = (
    "import json, sqlite3\n"
    f"connection = sqlite3.connect('{STUB_DATABASE}')\n"
    "print(json.dumps([row[0] for row in connection.execute('SELECT request FROM unsupported')]))\n"
)


class TlsCodeHostRun:
    """One live run against the stub code host, inside the Preflight's teardown."""

    def __init__(self, p: fe.Preflight) -> None:
        self.p = p
        self.port = STUB_LIVE_PORT
        self.stub_ip = ""
        self.startup: dict[str, str] = {}
        self.repository_id = 0
        self.request_id = ""
        self.claim_env: dict[str, str] = {}
        self.publication_job: dict[str, Any] | None = None
        self.job_watch: subprocess.Popen[bytes] | None = None
        self.job_watch_file: Path | None = None
        self.job_watch_restarts = 0
        self.publication_record: dict[str, Any] | None = None
        self.statuses: list[str] = []
        self.started_at: str | None = None
        self.obs: dict[str, Any] = {"stub": {}, "timeline": []}

    # --- small helpers ------------------------------------------------

    @property
    def ns(self) -> str:
        return self.p.namespace

    def _timeline(self, event: str, **facts: Any) -> None:
        self.obs["timeline"].append({"at": fe._now_iso(), "event": event, **facts})

    def _exec(self, *argv: str, check: bool = True) -> str:
        return self.p.kubectl("-n", self.ns, "exec", STUB_NAME, "--", *argv, check=check)

    def stub_api(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        out = self._exec(
            "python3",
            "-c",
            _STUB_CLIENT,
            method,
            path,
            json.dumps(body) if body is not None else "",
            str(self.port),
        )
        reply = json.loads(out.strip().splitlines()[-1])
        return int(reply["status"]), reply["body"]

    def stub_phase(self) -> str:
        return self.p.kubectl(
            "-n",
            self.ns,
            "get",
            "pod",
            STUB_NAME,
            "--ignore-not-found",
            "-o",
            "jsonpath={.status.phase}",
            check=False,
        ).strip()

    def stub_running(self) -> bool:
        return self.stub_phase() == "Running"

    def _private_file(self, name: str, text: str) -> Path:
        path = self.p.workdir / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
        return path

    # --- steps ----------------------------------------------------------

    def check_tools(self) -> None:
        wanted = ["kubectl", "helm", "git", "openssl", self.p.config.curie_bin]
        absent = [tool for tool in wanted if shutil.which(tool) is None]
        if absent:
            raise fe.ConfigError(f"required tools not on PATH: {', '.join(absent)}")
        bundle = self.p.config.bundle_dir
        if (
            fe.bundle_declares_runner_layer(bundle)
            and not (bundle / "connectors.lock.yaml").is_file()
            and self.p.config.layer_registry is None
        ):
            raise fe.ConfigError(
                f"{SCENARIO} deploys a bundle that declares a runner layer and carries no lock; "
                "set CURIE_FACTORY_LAYER_REGISTRY to a registry the cluster can pull from"
            )

    def stub_image(self) -> str:
        images = self.p.candidate_images
        if images is not None:
            return images["curie-worker"].pinned
        registry = fe.GHCR.removeprefix("https://")
        return f"{registry}/{fe.IMAGE_OWNER}/curie-worker:sha-{self.p.candidate}"

    def start_stub(self) -> None:
        image = self.stub_image()
        listing = {
            "apiVersion": "v1",
            "kind": "List",
            "items": stub_manifests(self.ns, image, self.port),
        }
        fe.run(
            ["kubectl", "--context", self.p.config.kube_context, "create", "-f", "-"],
            input_text=json.dumps(listing),
        )
        deadline = time.monotonic() + STUB_START_TIMEOUT_SECONDS
        while True:
            phase = self.stub_phase()
            logs = self.p.kubectl("-n", self.ns, "logs", STUB_NAME, check=False)
            try:
                self.startup = parse_stub_startup(logs)
                break
            except fe.PreflightFailed:
                if phase in ("Succeeded", "Failed") or time.monotonic() > deadline:
                    tail = logs.strip()[-800:]
                    raise fe.PreflightFailed(
                        f"the GitHub stub did not start (phase {phase or 'unknown'}): {tail}"
                    ) from None
            time.sleep(3)
        self.stub_ip = self.p.kubectl(
            "-n", self.ns, "get", "pod", STUB_NAME, "-o", "jsonpath={.status.podIP}"
        ).strip()
        ipaddress.ip_address(self.stub_ip)
        expected = f"https://{stub_host(self.ns)}:{self.port}"
        if self.startup["base_url"] != expected:
            raise fe.PreflightFailed(
                f"the stub advertises {self.startup['base_url']}, expected {expected}"
            )
        self.obs["stub"] = {**self.startup, "pod_ip": self.stub_ip, "image": image}
        self.p.step("GitHub stub serving over TLS", base_url=self.startup["base_url"], image=image)

    def prepare_ticket(self) -> None:
        """Rewrite the human-labelled seed issue to the scenario ticket and park it."""

        status, repository = self.stub_api("GET", f"/repos/{STUB_REPO}")
        if status != 200 or not isinstance(repository, dict):
            raise fe.PreflightFailed(f"the stub did not serve {STUB_REPO} (HTTP {status})")
        self.repository_id = int(repository["id"])
        title, body = self.p.issue_spec or DEFAULT_TLS_ISSUE
        status, issue = self.stub_api(
            "PATCH",
            f"/repos/{STUB_REPO}/issues/{STUB_ISSUE}",
            {"title": title, "body": body, "state": "closed"},
        )
        if status != 200 or not isinstance(issue, dict) or issue.get("state") != "closed":
            raise fe.PreflightFailed(f"the stub refused the ticket rewrite (HTTP {status})")
        self.p.issue_number = STUB_ISSUE
        self.p.evidence["fixture_repository_id"] = self.repository_id
        self.p.step("stub ticket written and parked", issue=STUB_ISSUE, title=title)

    def publish_ca(self) -> None:
        """The stub's public CA, plus the public roots the sandbox still needs."""

        ca = self._exec("cat", STUB_CA_FILE)
        if "CERTIFICATE" not in ca or "PRIVATE" in ca:
            raise fe.PreflightFailed("the stub CA file is not one public certificate")
        roots = self._exec("cat", SYSTEM_ROOTS)
        # The chart makes this bundle the sandbox's whole SSL_CERT_FILE store, so
        # the public roots ride along for the model provider (README, Code host
        # CA bundle). The stub CA comes first.
        bundle = self._private_file("code-host-ca.crt", ca.rstrip("\n") + "\n" + roots)
        self._create_ca_config_map(self.ns, bundle)
        self.obs["stub"]["ca_config_map"] = CA_CONFIG_MAP
        self.p.step("stub CA published", config_map=CA_CONFIG_MAP, namespace=self.ns)

    def _create_ca_config_map(self, namespace: str, bundle: Path) -> None:
        self.p.kubectl(
            "-n",
            namespace,
            "create",
            "configmap",
            CA_CONFIG_MAP,
            f"--from-file={CA_KEY}={bundle}",
        )
        self.p.kubectl("-n", namespace, "label", "configmap", CA_CONFIG_MAP, fe.OWNER_LABEL)

    def publish_ca_to_publication_namespace(self) -> None:
        namespace = self.p.publication_namespace()
        if namespace == self.ns:
            return
        if not self.p.kubectl(
            "get", "namespace", namespace, "--ignore-not-found", "-o", "name"
        ).strip():
            raise fe.PreflightFailed(f"the install created no publication namespace {namespace}")

        def remove() -> dict[str, Any]:
            self.p.kubectl(
                "-n", namespace, "delete", "configmap", CA_CONFIG_MAP, "--ignore-not-found"
            )
            left = self.p.kubectl(
                "-n",
                namespace,
                "get",
                "configmap",
                CA_CONFIG_MAP,
                "--ignore-not-found",
                "-o",
                "name",
                check=False,
            ).strip()
            if left:
                raise fe.PreflightFailed(f"{namespace}/{CA_CONFIG_MAP} is still present")
            return {"deleted": f"{namespace}/{CA_CONFIG_MAP}", "verified_absent": True}

        self.p.teardown.push("delete publication namespace CA ConfigMap", remove)
        self._create_ca_config_map(namespace, self.p.workdir / "code-host-ca.crt")
        self.p.step("stub CA published", config_map=CA_CONFIG_MAP, namespace=namespace)

    def generate_app_key(self) -> None:
        """A throwaway App key for the chart's App secret; the stub checks no JWT."""

        key = self.p.workdir / "app.pem"
        fe.run(
            [
                "openssl",
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                str(key),
            ]
        )
        key.chmod(0o600)
        self.p.config = dataclasses.replace(self.p.config, private_key_file=key)

    def install(self) -> None:
        model_cidrs = self.p.model_provider_cidrs()
        self.obs["model_cidrs"] = len(model_cidrs)

        def layer(base: dict[str, Any]) -> dict[str, Any]:
            values = tls_values(
                base,
                namespace=self.ns,
                ca_config_map=CA_CONFIG_MAP,
                stub_cluster_ip=self.stub_ip,
                port=self.port,
                model_cidrs=model_cidrs,
            )
            # The last layer before helm, so this is the trust the install used.
            self.obs["install_trust"] = install_trust(values)
            return values

        self.p.install(egress_cidrs=model_cidrs, extra_values=layer)

    def open_ticket(self) -> None:
        status, issue = self.stub_api(
            "PATCH", f"/repos/{STUB_REPO}/issues/{STUB_ISSUE}", {"state": "open"}
        )
        if status != 200 or not isinstance(issue, dict) or issue.get("state") != "open":
            raise fe.PreflightFailed(f"the stub refused to reopen the ticket (HTTP {status})")
        status, events = self.stub_api(
            "GET", f"/repos/{STUB_REPO}/issues/{STUB_ISSUE}/events?per_page=100"
        )
        event_id = fe.last_label_event_id(events if isinstance(events, list) else [], STUB_LABEL)
        if status != 200 or event_id is None:
            raise fe.PreflightFailed(f"the stub ticket has no {STUB_LABEL} label event")
        self.p.labelled_at = time.time()
        self.request_id = str(fe.request_id_for(self.repository_id, STUB_ISSUE, str(event_id)))
        self.obs["issue_number"] = STUB_ISSUE
        self.obs["request_id"] = self.request_id
        self._timeline("ticket reopened", label_event_id=event_id)
        self.p.step("stub ticket opened", issue=STUB_ISSUE, label_event_id=event_id)

    def await_admission(self) -> str | None:
        timeout = max(self.p.admission_timeout, 300.0)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            request = self.p.execution_request(self.request_id)
            if request is not None and request.get("work_item_id"):
                work_item_id = str(request["work_item_id"])
                self.p.evidence["execution_request_id"] = self.request_id
                self.p.evidence["work_item_id"] = work_item_id
                self._record_status(request)
                self.p.step("WorkItem admitted", work_item_id=work_item_id)
                return work_item_id
            if not self.stub_running():
                return None
            time.sleep(5)
        return None

    def _record_status(self, request: Mapping[str, Any]) -> str:
        status = str(request.get("status") or "")
        if status and (not self.statuses or self.statuses[-1] != status):
            # A request that started but was first seen past running still ran.
            if (
                status in TERMINAL_STATUSES
                and request.get("started_at")
                and "running" not in self.statuses
            ):
                self.statuses.append("running")
            self.statuses.append(status)
            self._timeline("request status", status=status)
        if request.get("started_at"):
            self.started_at = str(request["started_at"])
        return status

    def _capture_claim_env(self) -> None:
        if self.claim_env:
            return
        raw = self.p.kubectl(
            "-n",
            self.ns,
            "get",
            "sandboxclaims.extensions.agents.x-k8s.io",
            "-o",
            "json",
            check=False,
        )
        try:
            items = json.loads(raw).get("items") or []
        except ValueError:
            return
        for claim in items:
            entries = (claim.get("spec") or {}).get("env") or []
            names = {
                str(e.get("name")): str(e.get("value"))
                for e in entries
                if isinstance(e, dict) and not e.get("containerName")
            }
            if "CURIE_REPO_ORIGIN" in names or "CURIE_REPO_PATH" in names:
                # Only the code host facts; nothing else on the claim is needed.
                self.claim_env = {k: v for k, v in names.items() if k.startswith("CURIE_REPO_")}
                self.obs["claim_name"] = (claim.get("metadata") or {}).get("name")
                self._timeline("sandbox claim observed", keys=sorted(self.claim_env))
                return

    def start_job_watch(self) -> None:
        """Watch the publication namespace's Jobs before the ticket opens: a diagnostic.

        The worker creates the publication Job and deletes it right after it
        settles the outcome (publication_loop._terminalize, then
        cleanup_terminal), so a fast run can create and delete it between two
        polls. A watch usually still reports it (ADDED, then DELETED), and
        events stream to a private file in the run's directory parsed by
        publication_job_from_watch. It is best-effort: a restart can miss a
        Job, so an unobserved Job does not fail the run. The judge's proof is
        the durable publication record and the stub's own state.
        """

        self.job_watch_file = self._private_file("publication-jobs.watch.json", "")
        self._spawn_job_watch()

        def stop() -> dict[str, Any]:
            return {"stopped": self.stop_job_watch()}

        self.p.teardown.push("stop publication Job watch", stop)
        self.p.step("publication Job watch started", namespace=self.p.publication_namespace())

    def _spawn_job_watch(self) -> None:
        assert self.job_watch_file is not None
        with open(self.job_watch_file, "ab") as sink:
            # The child holds its own descriptor; the file stays 0600.
            self.job_watch = subprocess.Popen(
                [
                    "kubectl",
                    "--context",
                    self.p.config.kube_context,
                    "-n",
                    self.p.publication_namespace(),
                    "get",
                    "jobs",
                    "-l",
                    "curietech.ai/component=publication",
                    "--watch",
                    "--output-watch-events",
                    "-o",
                    "json",
                ],
                stdout=sink,
                stderr=subprocess.DEVNULL,
            )

    def _keep_job_watch(self) -> None:
        """Restart the watch if the API server closed it; a restart relists as ADDED."""

        if self.job_watch is not None and self.job_watch.poll() is not None:
            self.job_watch_restarts += 1
            self._timeline("publication Job watch restarted", restarts=self.job_watch_restarts)
            self._spawn_job_watch()

    def stop_job_watch(self) -> bool:
        if self.job_watch is None:
            return True
        return fe._stop(self.job_watch)

    def _capture_publication_job(self) -> None:
        """Read the publication Job, if any, from everything the watch has reported."""

        if self.job_watch_file is None:
            return
        try:
            text = self.job_watch_file.read_text(errors="replace")
        except OSError:
            return
        job = publication_job_from_watch(text)
        if job is None:
            return
        if self.publication_job is None:
            self._timeline("publication Job observed", name=job["metadata"]["name"])
        self.publication_job = job

    def _capture_publication_record(self) -> None:
        """The admitted request's publication record, read from Postgres.

        Chosen over the Job's own status or its pod because it is the one
        completion fact that survives the Job's deletion: the worker writes the
        record terminal (succeeded once the Job's post-push marker reports a
        verified push and the pull request was read back) before it deletes
        the Job, and never deletes the
        record. It is keyed by the execution request, so it needs no Job to
        have been observed.
        """

        if not self.request_id:
            return
        if (self.publication_record or {}).get("status") in ("succeeded", "failed"):
            return
        try:
            rows = self.p.sql(
                "SELECT id, status, execution_request_id, "
                "left(translate(coalesce(error, ''), E'\\t\\n\\r', '   '), 500) "
                "FROM publications "
                f"WHERE execution_request_id = '{uuid.UUID(self.request_id)}' "
                "ORDER BY created_at DESC LIMIT 1"
            )
        except fe.PreflightFailed:
            # A failed read is retried on the next poll; the record persists.
            return
        if len(rows) != 1:
            return
        publication_id, status, request_id, error = (rows[0] + ["", "", "", ""])[:4]
        record = {
            "id": publication_id,
            "status": status,
            "execution_request_id": request_id or None,
            "error": error or None,
        }
        if record != self.publication_record:
            self._timeline("publication record", status=status)
        self.publication_record = record

    def await_ending(self) -> bool:
        """Poll the admitted request to its end, capturing claim and Job as they appear."""

        since = self.p.labelled_at
        while True:
            request = self.p.execution_request(self.request_id)
            status = self._record_status(request) if request is not None else ""
            self._capture_claim_env()
            self._keep_job_watch()
            self._capture_publication_job()
            self._capture_publication_record()
            if status in TERMINAL_STATUSES:
                return True
            if not self.stub_running():
                self._timeline("stub stopped", phase=self.stub_phase())
                return False
            started = fe._parse_time(self.started_at)
            if started is not None:
                give_up = (
                    started.timestamp()
                    + fe.EXECUTION_BOUND_SECONDS
                    + fe.PUBLICATION_ALLOWANCE_SECONDS
                )
            else:
                give_up = since + fe.NEVER_STARTED_CAP_SECONDS
            if time.time() > give_up:
                fe.log(f"{SCENARIO}: the run did not end within the wait; judging what exists")
                return False
            time.sleep(fe.POLL_SECONDS)

    def collect_stub(self) -> tuple[list[str], list[dict[str, Any]], list[str], str]:
        stub_log = self.p.kubectl("-n", self.ns, "logs", STUB_NAME, check=False)
        if not self.stub_running():
            unsupported = list(dict.fromkeys(_UNSUPPORTED_LINE.findall(stub_log)))
            return [], [], unsupported, stub_log
        refs = self._exec(
            "git",
            "--git-dir",
            STUB_BARE_REPO,
            "for-each-ref",
            "--format=%(refname:short)",
            "refs/heads/",
        )
        branches = [line.strip() for line in refs.splitlines() if line.strip()]
        status, pulls = self.stub_api("GET", f"/repos/{STUB_REPO}/pulls?state=all&per_page=100")
        rows = [
            {
                "number": pr.get("number"),
                "state": pr.get("state"),
                "head": (pr.get("head") or {}).get("ref"),
                "base": (pr.get("base") or {}).get("ref"),
            }
            for pr in (pulls if status == 200 and isinstance(pulls, list) else [])
            if isinstance(pr, dict)
        ]
        unsupported = json.loads(self._exec("python3", "-c", _STUB_UNSUPPORTED) or "[]")
        return branches, rows, [str(r) for r in unsupported], stub_log

    def collect_logs(self, stub_log: str) -> str:
        parts = [stub_log]
        for selector in ("app.kubernetes.io/component=api", fe.WORKER_SELECTOR):
            parts.append(
                self.p.kubectl(
                    "-n",
                    self.ns,
                    "logs",
                    "-l",
                    selector,
                    "--all-containers",
                    "--tail=-1",
                    "--max-log-requests=20",
                    check=False,
                )
            )
        parts.append(
            self.p.kubectl(
                "-n",
                self.p.publication_namespace(),
                "logs",
                "-l",
                "curietech.ai/component=publication",
                "--all-containers",
                "--tail=-1",
                check=False,
            )
        )
        return "\n".join(parts)

    def drive(self) -> dict[str, Any]:
        p = self.p
        p.evidence["scenario"] = self.obs
        self.check_tools()
        p._lock(f"context-{p.config.kube_context}", "this kube context")
        p.check_images()
        if p.candidate_image_mode == "build":
            p.build_candidate_images()
        p.sweep_stale_namespaces()
        p.create_namespace()
        self.start_stub()
        self.prepare_ticket()
        self.publish_ca()
        self.generate_app_key()
        self.install()
        self.publish_ca_to_publication_namespace()
        p.port_forward()
        p.bind_agent(github_token=False)
        self.start_job_watch()
        self.open_ticket()
        work_item_id = self.await_admission()
        ended = self.await_ending() if work_item_id else False
        self.stop_job_watch()
        self._capture_claim_env()
        self._capture_publication_job()
        self._capture_publication_record()
        branches, pulls, unsupported, stub_log = self.collect_stub()
        logs = self.collect_logs(stub_log)
        self.obs.update(
            {
                "work_item_id": work_item_id,
                "request_statuses": list(self.statuses),
                "started_at": self.started_at,
                "ended": ended,
                "claim_env": dict(self.claim_env),
                "publication_job": self.publication_job,
                "publication_job_observed": self.publication_job is not None,
                "publication_job_watch_restarts": self.job_watch_restarts,
                "publication_record": self.publication_record,
                "stub_branches": branches,
                "stub_pull_requests": pulls,
                "stub_unsupported": unsupported,
            }
        )
        secrets_seen = [p.config.model_api_key, p.api_key, p.worker_token]
        failures = [
            fe._redact(failure, secrets_seen)
            for failure in judge_tls_code_host({**self.obs, "logs": logs})
        ]
        self.obs["verdict"] = "passed" if not failures else "failed"
        self.obs["failures"] = failures
        p.evidence["scenario"] = self.obs
        if failures:
            raise fe.PreflightFailed("; ".join(failures))
        return self.obs


def drive(p: fe.Preflight) -> dict[str, Any]:
    """The registered scenario driver: the whole stub code host run."""

    return TlsCodeHostRun(p).drive()
