import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml

from .engine import PROFILES, audit_chart

REPO = Path(__file__).resolve().parents[4]
CHART = REPO / "charts" / "curie"


@pytest.fixture
def chart(tmp_path: Path) -> Path:
    return Path(shutil.copytree(CHART, tmp_path / "curie"))


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    assert text.count(old) == 1, f"Expected one mutation anchor in {path.name}: {old!r}"
    path.write_text(text.replace(old, new, 1))


def set_value(path: Path, keys: tuple[str, ...], value: Any) -> None:
    values = yaml.safe_load(path.read_text())
    target = values
    for key in keys[:-1]:
        target = target.setdefault(key, {})
    target[keys[-1]] = value
    path.write_text(yaml.safe_dump(values, sort_keys=False))


def rejected(errors: list[str], rule: str, workload: str, container: str) -> None:
    assert any(
        error.startswith(rule) and workload in error and f"container={container}" in error
        for error in errors
    ), errors


def audit(chart: Path, repo: Path = REPO, **kwargs: Any) -> list[str]:
    return audit_chart(chart, repo, profiles=PROFILES, **kwargs)


def container(image: str = "busybox:1.36.1", name: str = "policy-main") -> dict[str, Any]:
    return {
        "name": name,
        "image": image,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000},
        "readinessProbe": {"tcpSocket": {"port": 8080}, "initialDelaySeconds": 1},
        "livenessProbe": {
            "tcpSocket": {"port": 8080},
            "initialDelaySeconds": 30,
            "timeoutSeconds": 5,
        },
    }


def workload(
    kind: str,
    main: dict[str, Any],
    *,
    name: str = "policy-added",
    init: dict[str, Any] | None = None,
    hook: bool = False,
    pod_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pod: dict[str, Any] = {"containers": [main]}
    if init is not None:
        pod["initContainers"] = [init]
    if pod_context is not None:
        pod["securityContext"] = pod_context
    template = {"metadata": {"labels": {"app": name}}, "spec": pod}
    if kind == "Pod":
        spec = pod
    elif kind == "SandboxTemplate":
        spec = {"podTemplate": template}
    elif kind == "CronJob":
        spec = {"schedule": "0 * * * *", "jobTemplate": {"spec": {"template": template}}}
    else:
        spec = {"template": template}
    metadata: dict[str, Any] = {"name": name}
    if hook:
        metadata["annotations"] = {"helm.sh/hook": "pre-install"}
    return {"apiVersion": "v1", "kind": kind, "metadata": metadata, "spec": spec}


def add_workloads(chart: Path, *documents: dict[str, Any]) -> None:
    (chart / "templates" / "policy-mutation.yaml").write_text(yaml.safe_dump_all(documents))


def copied_repo(tmp_path: Path) -> Path:
    target = tmp_path / "repo"
    target.mkdir()
    shutil.copy2(REPO / "compose.dev.yaml", target / "compose.dev.yaml")
    for source in [*(REPO / "apps").glob("*/Dockerfile"), REPO / "runner" / "Dockerfile"]:
        destination = target / source.relative_to(REPO)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return target


@pytest.mark.parametrize("image", ["busybox", "busybox:latest", "busybox:1.36"])
def test_p1_rejects_unpinned_rendered_image(chart: Path, image: str) -> None:
    add_workloads(chart, workload("Deployment", container(image)))
    rejected(audit(chart), "P1", "Deployment/policy-added", "policy-main")


def test_p1_dev_local_exemption_does_not_allow_latest(chart: Path) -> None:
    set_value(chart / "values-dev.yaml", ("api", "image", "tag"), "latest")
    rejected(audit(chart), "P1", "Deployment/curie-api", "api")


@pytest.mark.parametrize(
    "image",
    ["registry.example.com:5000/tool:v1.2.3", f"busybox@sha256:{'a' * 64}"],
)
def test_p1_accepts_full_patch_and_digest_images(chart: Path, image: str) -> None:
    add_workloads(chart, workload("Deployment", container(image)))
    assert audit(chart) == []


def test_p1_rejects_malformed_digest(chart: Path) -> None:
    add_workloads(chart, workload("Pod", container("busybox@sha256:abc")))
    rejected(audit(chart), "P1", "Pod/policy-added", "policy-main")


@pytest.mark.parametrize("image", ["nicolaka/netshoot", "nicolaka/netshoot:latest"])
def test_p1_checks_image_values_in_unrendered_overlays(chart: Path, image: str) -> None:
    overlay = chart / "values-policy-negative.yaml"
    overlay.write_text(yaml.safe_dump({"security": {"probes": {"netshootImage": image}}}))
    errors = audit(chart)
    assert any(
        error.startswith("P1") and overlay.name in error and "netshootImage" in error
        for error in errors
    ), errors


def test_p1_requires_postgres_digest_in_every_overlay(chart: Path) -> None:
    overlay = chart / "values-policy-negative.yaml"
    overlay.write_text("postgres:\n  image: postgres:16.15-alpine\n")
    errors = audit(chart)
    assert any(
        error.startswith("P1") and overlay.name in error and "postgres.image" in error
        for error in errors
    ), errors


def test_p1_checks_third_party_compose_images(chart: Path, tmp_path: Path) -> None:
    repo = copied_repo(tmp_path)
    set_value(repo / "compose.dev.yaml", ("services", "valkey", "image"), "valkey/valkey:8")
    errors = audit(chart, repo)
    assert any(
        error.startswith("P1") and "compose.dev.yaml" in error and "valkey" in error
        for error in errors
    ), errors


@pytest.mark.parametrize("probe", ["readinessProbe", "livenessProbe"])
def test_p2_requires_langfuse_worker_probes(chart: Path, probe: str) -> None:
    if probe == "readinessProbe":
        block = (
            "          readinessProbe:\n"
            "            httpGet:\n"
            "              path: /api/ready\n"
            "              port: 3030\n"
            "            {{- toYaml .Values.langfuse.worker.readinessProbe | nindent 12 }}\n"
        )
    else:
        block = (
            "          livenessProbe:\n"
            "            tcpSocket:\n"
            "              port: 3030\n"
            "            {{- toYaml .Values.langfuse.worker.livenessProbe | nindent 12 }}\n"
        )
    replace_once(chart / "templates" / "langfuse.yaml", block, "")
    rejected(audit(chart), "P2", "Deployment/curie-langfuse-worker", "langfuse-worker")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("              path: /api/ready\n", "              path: /api/health\n"),
        (
            "            tcpSocket:\n              port: 3030\n",
            "            tcpSocket:\n              port: 3000\n",
        ),
        (
            "            tcpSocket:\n              port: 3030\n",
            "            httpGet:\n              path: /api/health\n              port: 3030\n",
        ),
        ("              containerPort: 3030\n", "              containerPort: 3000\n"),
    ],
    ids=["readiness-path", "liveness-port", "store-coupled-liveness", "declared-port"],
)
def test_p2_preserves_worker_probe_handlers(chart: Path, old: str, new: str) -> None:
    replace_once(chart / "templates" / "langfuse.yaml", old, new)
    rejected(audit(chart), "P2", "Deployment/curie-langfuse-worker", "langfuse-worker")


def test_p2_preserves_worker_default_cadence(chart: Path) -> None:
    set_value(
        chart / "values.yaml",
        ("langfuse", "worker", "readinessProbe", "periodSeconds"),
        9,
    )
    rejected(audit(chart), "P2", "Deployment/curie-langfuse-worker", "langfuse-worker")


@pytest.mark.parametrize("kind", ["Deployment", "StatefulSet"])
def test_p2_requires_probes_on_new_persistent_workloads(chart: Path, kind: str) -> None:
    main = container()
    del main["livenessProbe"]
    add_workloads(chart, workload(kind, main))
    rejected(audit(chart), "P2", f"{kind}/policy-added", "policy-main")


def test_p3_rejects_liveness_cutoff_before_readiness(chart: Path) -> None:
    errors = audit(
        chart,
        helm_args=(
            "--set",
            "postgres.livenessProbe.failureThreshold=3",
            "--set",
            "langfuse.web.livenessProbe.failureThreshold=1",
            "--set",
            "langfuse.worker.livenessProbe.failureThreshold=1",
            "--set",
            "api.livenessProbe.failureThreshold=1",
        ),
    )
    for kind, name in [
        ("StatefulSet", "postgres"),
        ("Deployment", "langfuse-web"),
        ("Deployment", "langfuse-worker"),
        ("Deployment", "api"),
    ]:
        rejected(errors, "P3", f"{kind}/curie-{name}", name)


def test_p3_uses_kubelet_defaults_for_omitted_cadence(chart: Path) -> None:
    main = container()
    main["readinessProbe"]["initialDelaySeconds"] = 30
    del main["livenessProbe"]["initialDelaySeconds"]
    add_workloads(chart, workload("Pod", main))
    rejected(audit(chart), "P3", "Pod/policy-added", "policy-main")


def test_p3_requires_explicit_liveness_timeout(chart: Path) -> None:
    main = container()
    del main["livenessProbe"]["timeoutSeconds"]
    add_workloads(chart, workload("Pod", main))
    rejected(audit(chart), "P3", "Pod/policy-added", "policy-main")


def test_p3_requires_postgres_timeout_of_at_least_five_seconds(chart: Path) -> None:
    set_value(chart / "values.yaml", ("postgres", "livenessProbe", "timeoutSeconds"), 1)
    rejected(audit(chart), "P3", "StatefulSet/curie-postgres", "postgres")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("              path: /ready\n", "              path: /health\n"),
        ("              path: /health\n", "              path: /ready\n"),
    ],
    ids=["readiness-health", "liveness-ready"],
)
def test_p3_preserves_api_probe_routing(chart: Path, old: str, new: str) -> None:
    replace_once(chart / "templates" / "api.yaml", old, new)
    rejected(audit(chart), "P3", "Deployment/curie-api", "api")


@pytest.mark.parametrize("uid", [None, 0, True, "1001", 1002])
def test_p4_preserves_langfuse_web_numeric_uid(chart: Path, uid: Any) -> None:
    set_value(
        chart / "values.yaml", ("langfuse", "web", "containerSecurityContext", "runAsUser"), uid
    )
    errors = audit(chart)
    rejected(errors, "P4", "Deployment/curie-langfuse-web", "langfuse-web")


@pytest.mark.parametrize("uid", [None, 0, True, "1001", 1002])
def test_p4_preserves_langfuse_postgres_gate_numeric_uid(chart: Path, uid: Any) -> None:
    context: dict[str, Any] = {"runAsNonRoot": True}
    if uid is not None:
        context["runAsUser"] = uid
    rendered_context = "".join(
        f"    {line}" for line in yaml.safe_dump(context, sort_keys=False).splitlines(keepends=True)
    )
    replace_once(
        chart / "templates" / "_helpers.tpl",
        "  securityContext:\n    {{- toYaml $readinessSecurityContext | nindent 4 }}\n",
        f"  securityContext:\n{rendered_context}",
    )
    rejected(audit(chart), "P4", "Deployment/curie-langfuse-web", "wait-for-postgres")


def test_p4_preserves_langfuse_worker_nonroot_context(chart: Path) -> None:
    set_value(
        chart / "values.yaml",
        ("langfuse", "worker", "containerSecurityContext", "runAsNonRoot"),
        False,
    )
    rejected(audit(chart), "P4", "Deployment/curie-langfuse-worker", "langfuse-worker")


@pytest.mark.parametrize("uid", [0, True, "1000"])
def test_p4_container_context_overrides_pod_context(chart: Path, uid: Any) -> None:
    main = container()
    main["securityContext"]["runAsUser"] = uid
    add_workloads(
        chart,
        workload("Pod", main, pod_context={"runAsNonRoot": True, "runAsUser": 1000}),
    )
    rejected(audit(chart), "P4", "Pod/policy-added", "policy-main")


def test_p4_accepts_effective_pod_security_context(chart: Path) -> None:
    main = container()
    del main["securityContext"]
    add_workloads(
        chart,
        workload("Pod", main, pod_context={"runAsNonRoot": True, "runAsUser": 1000}),
    )
    assert audit(chart) == []


@pytest.mark.parametrize("user", ["1000:1000", "root", "0", "named-user"])
def test_p4_uses_final_numeric_dockerfile_user(chart: Path, tmp_path: Path, user: str) -> None:
    repo = copied_repo(tmp_path)
    dockerfile = repo / "apps" / "api" / "Dockerfile"
    dockerfile.write_text(f"FROM scratch AS builder\nUSER 1000\nFROM scratch\nUSER {user}\n")
    errors = audit(chart, repo)
    if user == "1000:1000":
        assert errors == []
    else:
        rejected(errors, "P4", "Deployment/curie-api", "api")


@pytest.mark.parametrize("image", ["ghcr.io/curie-eng/curie-unknown:1.2.3", "curie-unknown:1.2.3"])
def test_p5_discovers_unknown_first_party_deployment(chart: Path, image: str) -> None:
    add_workloads(chart, workload("Deployment", container(image, "unknown-adapter")))
    rejected(audit(chart), "P5", "Deployment/policy-added", "unknown-adapter")


@pytest.mark.parametrize("field", ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"])
def test_p5_requires_both_telemetry_variables(chart: Path, field: str) -> None:
    main = container("curie-api:1.2.3")
    main["env"] = [
        {
            "name": "OTEL_EXPORTER_OTLP_ENDPOINT",
            "valueFrom": {"secretKeyRef": {"name": "acme-otel", "key": "endpoint"}},
        },
        {"name": "OTEL_EXPORTER_OTLP_PROTOCOL", "value": "http/protobuf"},
    ]
    main["env"] = [entry for entry in main["env"] if entry["name"] != field]
    add_workloads(chart, workload("Pod", main))
    rejected(audit(chart), "P5", "Pod/policy-added", "policy-main")


def test_p5_accepts_telemetry_value_from(chart: Path) -> None:
    main = container("curie-api:1.2.3")
    main["env"] = [
        {"name": name, "valueFrom": {"secretKeyRef": {"name": "acme-otel", "key": key}}}
        for name, key in [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "endpoint"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "protocol"),
        ]
    ]
    add_workloads(chart, workload("Pod", main))
    assert audit(chart) == []


def test_p5_checks_existing_workload_instrumentation(chart: Path) -> None:
    replace_once(
        chart / "templates" / "api.yaml",
        '            {{- include "curie.env.otel" '
        '(dict "root" . "extraEnv" .Values.api.extraEnv) | nindent 12 }}\n',
        "",
    )
    rejected(audit(chart), "P5", "Deployment/curie-api", "api")


def test_p5_rejects_wrong_collector_endpoint(chart: Path) -> None:
    main = container("curie-api:1.2.3")
    main["env"] = [
        {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "https://wrong.example.com:4318"},
        {"name": "OTEL_EXPORTER_OTLP_PROTOCOL", "value": "http/protobuf"},
    ]
    add_workloads(chart, workload("Pod", main))
    rejected(audit(chart), "P5", "Pod/policy-added", "policy-main")


def test_p5_class_exemptions_cover_bootstrap_hook_prewarm_and_spa(chart: Path) -> None:
    bootstrap = container("curie-api:1.2.3", "policy-bootstrap")
    prewarm = container("curie-api:1.2.3", "policy-prewarm")
    prewarm["command"] = ["sleep", "infinity"]
    add_workloads(
        chart,
        workload("Pod", container(), name="policy-init", init=bootstrap),
        workload("Job", container("curie-api:1.2.3"), name="policy-hook", hook=True),
        workload("DaemonSet", prewarm, name="policy-prewarm"),
        workload("Deployment", container("curie-ui:1.2.3"), name="policy-spa"),
    )
    assert audit(chart) == []


def test_enumeration_checks_all_supported_kinds_init_and_hooks(chart: Path) -> None:
    kinds = ["Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Pod", "SandboxTemplate"]
    documents = [
        workload(kind, container("busybox:latest"), name=f"policy-{kind.lower()}") for kind in kinds
    ]
    documents.append(
        workload(
            "Job",
            container(),
            name="policy-hook-init",
            init=container("busybox:latest", "policy-init"),
            hook=True,
        )
    )
    add_workloads(chart, *documents)
    errors = audit(chart)
    for kind in kinds:
        rejected(errors, "P1", f"{kind}/policy-{kind.lower()}", "policy-main")
    rejected(errors, "P1", "Job/policy-hook-init", "policy-init")


@pytest.mark.parametrize("empty", [False, True], ids=["containers", "empty-template"])
def test_unknown_kind_with_pod_template_fails_closed(chart: Path, empty: bool) -> None:
    document = workload("UnknownWorkload", container())
    if empty:
        document["spec"]["template"]["spec"] = {}
    add_workloads(chart, document)
    errors = audit(chart)
    assert any(
        "enumeration" in error.lower() and "UnknownWorkload/policy-added" in error
        for error in errors
    ), errors


def test_unknown_kind_without_pod_template_is_valid(chart: Path) -> None:
    add_workloads(
        chart,
        {
            "apiVersion": "example.com/v1",
            "kind": "UnknownConfiguration",
            "metadata": {"name": "policy-configuration"},
            "spec": {"enabled": True},
        },
    )
    assert audit(chart) == []


@pytest.mark.parametrize("state", ["missing", "malformed"])
def test_required_compose_input_fails_closed(chart: Path, tmp_path: Path, state: str) -> None:
    repo = copied_repo(tmp_path)
    compose = repo / "compose.dev.yaml"
    if state == "missing":
        compose.unlink()
    else:
        compose.write_text("services: [\n")
    errors = audit(chart, repo)
    assert any("compose.dev.yaml" in error for error in errors), errors
