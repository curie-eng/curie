from __future__ import annotations

import re
import subprocess
from collections.abc import Iterator
from copy import deepcopy
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import yaml

from .exemptions import EXEMPTIONS, Exemption

PROFILES = (None, "values-dev.yaml", "values-external.yaml", "values-e2e-harness.yaml")
POD_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Pod", "SandboxTemplate"}
FIRST_PARTY_PREFIX = "ghcr.io/curie-eng/curie-"
DOCKERFILES = {
    **{
        f"curie-{name}": f"apps/{name}/Dockerfile"
        for name in ("api", "dispatcher", "mail-adapter", "worker", "ui", "docker-daemon")
    },
    "curie-runner": "runner/Dockerfile",
}
CADENCE = {
    "initialDelaySeconds": 0,
    "periodSeconds": 10,
    "timeoutSeconds": 1,
    "failureThreshold": 3,
}
WORKER_CADENCE = {
    "readinessProbe": (20, 10, 5, 30),
    "livenessProbe": (90, 20, 5, 15),
}


def _load(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ValueError(f"required input {path.name} is missing, unreadable or malformed") from exc
    if not isinstance(data, dict):
        raise ValueError(f"required input {path.name} must contain a YAML mapping")
    return data


def _merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = deepcopy(value)


def _settings(
    base: dict[str, Any], overlay: dict[str, Any], helm_args: tuple[str, ...]
) -> dict[str, Any]:
    """Resolve YAML overlays and simple dotted scalar overrides used by this suite."""
    values = deepcopy(base)
    _merge(values, overlay)
    index = 0
    while index < len(helm_args):
        flag = helm_args[index]
        option, _, inline = flag.partition("=")
        if option in {"-f", "--values", "--set", "--set-string"}:
            index += 1
            argument = inline if "=" in flag else helm_args[index]
            if inline:
                index -= 1
            if option in {"-f", "--values"}:
                _merge(values, _load(Path(argument)))
            else:
                for assignment in argument.split(","):
                    key, separator, raw = assignment.partition("=")
                    if not separator or "[" in key:
                        continue
                    target = values
                    parts = key.split(".")
                    for part in parts[:-1]:
                        target = target.setdefault(part, {})
                    target[parts[-1]] = raw if option == "--set-string" else yaml.safe_load(raw)
        index += 1
    return values


def _image_repository(image: str) -> str:
    reference = image.split("@", 1)[0]
    parts = reference.split("/")
    parts[-1] = parts[-1].split(":", 1)[0]
    return "/".join(parts)


def _first_party(image: str) -> bool:
    repository = _image_repository(image)
    return repository.startswith(FIRST_PARTY_PREFIX) or (
        "/" not in repository and repository.startswith("curie-")
    )


def _image_problem(image: str) -> str | None:
    if not image:
        return "image is empty"
    if "@" in image:
        if not re.fullmatch(r".+@sha256:[0-9a-fA-F]{64}", image):
            return "malformed sha256 digest"
        return None
    final = image.rsplit("/", 1)[-1]
    if ":" not in final:
        return "image has no tag"
    tag = final.rsplit(":", 1)[-1]
    if not re.match(r"^v?[0-9]+\.[0-9]+\.[0-9]+", tag):
        return "image needs a full numeric patch tag or sha256 digest"
    return None


def _image_values(data: Any, path: str) -> Iterator[tuple[str, str]]:
    if isinstance(data, dict):
        for key, value in data.items():
            child = f"{path}.{key}"
            if str(key).endswith("Image") and isinstance(value, str) and value.strip():
                yield child, value.strip()
            yield from _image_values(value, child)
    elif isinstance(data, list):
        for index, value in enumerate(data):
            yield from _image_values(value, f"{path}[{index}]")


def _input_problems(values: dict[str, dict[str, Any]], compose: dict[str, Any]) -> list[str]:
    errors = []
    for filename, data in values.items():
        for where, image in _image_values(data, filename):
            if reason := _image_problem(image):
                errors.append(f"P1 {where}: {reason}")
        postgres_image = (data.get("postgres") or {}).get("image")
        if postgres_image and (
            not isinstance(postgres_image, str)
            or not re.fullmatch(r".+@sha256:[0-9a-fA-F]{64}", postgres_image)
        ):
            errors.append(
                f"P1 {filename} postgres.image must be digest-pinned for fsGroup identity"
            )
    services = compose.get("services")
    if not isinstance(services, dict) or not services:
        errors.append("P1 compose.dev.yaml must contain services")
        return errors
    for name, service in services.items():
        if not isinstance(service, dict):
            errors.append(f"P1 compose.dev.yaml services.{name} must be a mapping")
            continue
        compose_image = service.get("image")
        if not compose_image or FIRST_PARTY_PREFIX in str(compose_image):
            continue
        if reason := _image_problem(str(compose_image).strip()):
            errors.append(f"P1 compose.dev.yaml services.{name}: {reason}")
    return errors


def _contains_pod(data: Any) -> bool:
    if isinstance(data, dict):
        if any(
            key in data
            for key in ("containers", "initContainers", "template", "podTemplate", "jobTemplate")
        ):
            return True
        return any(_contains_pod(value) for value in data.values())
    if isinstance(data, list):
        return any(_contains_pod(value) for value in data)
    return False


def _pod_spec(document: dict[str, Any]) -> dict[str, Any] | None:
    kind = document.get("kind")
    spec = document.get("spec") or {}
    if kind not in POD_KINDS:
        if _contains_pod(spec):
            raise ValueError("unsupported kind carries a pod spec")
        return None
    if kind == "Pod":
        pod = spec
    elif kind == "CronJob":
        pod = spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec")
    elif kind == "SandboxTemplate":
        pod = spec.get("podTemplate", {}).get("spec")
    else:
        pod = spec.get("template", {}).get("spec")
    if not isinstance(pod, dict) or not isinstance(pod.get("containers"), list):
        raise ValueError("missing or malformed pod containers")
    return pod


def _exempt(
    exemptions: tuple[Exemption, ...],
    matched: set[int],
    rule: str,
    workload: str,
    container: str,
    profile: str,
    classification: str | None = None,
) -> bool:
    found = False
    for index, entry in enumerate(exemptions):
        if (
            entry.rule_id == rule
            and fnmatchcase(workload, entry.workload)
            and fnmatchcase(container, entry.container)
            and (entry.profile is None or entry.profile == profile)
            and (entry.classification is None or entry.classification == classification)
        ):
            matched.add(index)
            found = True
    return found


def _probe_problems(container: dict[str, Any], where: str) -> list[str]:
    errors = []
    ready, live = container.get("readinessProbe"), container.get("livenessProbe")
    if container.get("name") == "api":
        for probe, path in ((ready, "/ready"), (live, "/health")):
            target = (probe or {}).get("httpGet") or {}
            if (target.get("path"), target.get("port")) != (path, 8000):
                errors.append(f"P3 {where}: API probe must target httpGet {path}:8000")
    if isinstance(live, dict):
        if "timeoutSeconds" not in live:
            errors.append(f"P3 {where}: livenessProbe must declare timeoutSeconds explicitly")
        elif container.get("name") == "postgres":
            timeout = live["timeoutSeconds"]
            if type(timeout) is not int or timeout < 5:
                errors.append(f"P3 {where}: postgres liveness timeoutSeconds must be at least 5")
    if not isinstance(ready, dict) or not isinstance(live, dict):
        return errors
    cutoffs = []
    for label, probe in (("readinessProbe", ready), ("livenessProbe", live)):
        cadence = {key: probe.get(key, default) for key, default in CADENCE.items()}
        if any(type(value) is not int for value in cadence.values()):
            errors.append(f"P3 {where}: {label} cadence fields must be integers")
            return errors
        cutoffs.append(
            cadence["initialDelaySeconds"]
            + (cadence["failureThreshold"] - 1) * cadence["periodSeconds"]
        )
    if cutoffs[1] <= cutoffs[0]:
        errors.append(
            f"P3 {where}: liveness cutoff must exceed readiness cutoff; "
            f"live={cutoffs[1]}s ready={cutoffs[0]}s"
        )
    return errors


def _worker_problems(container: dict[str, Any], where: str, defaults: bool) -> list[str]:
    errors = []
    if 3030 not in [port.get("containerPort") for port in container.get("ports") or []]:
        errors.append(f"P2 {where}: langfuse-worker must declare containerPort 3030")
    ready = container.get("readinessProbe") or {}
    live = container.get("livenessProbe") or {}
    handler = ready.get("httpGet") or {}
    if (handler.get("path"), handler.get("port")) != ("/api/ready", 3030):
        errors.append(f"P2 {where}: readinessProbe must target httpGet /api/ready:3030")
    if "httpGet" in live or (live.get("tcpSocket") or {}).get("port") != 3030:
        errors.append(f"P2 {where}: livenessProbe must use tcpSocket:3030 without httpGet")
    if defaults:
        for name, expected in WORKER_CADENCE.items():
            probe = container.get(name) or {}
            if tuple(probe.get(key) for key in CADENCE) != expected:
                errors.append(f"P2 {where}: {name} default cadence must be {expected}")
    return errors


def _numeric_user(repo: Path, image: str) -> bool:
    if not _first_party(image):
        return False
    relative = DOCKERFILES.get(_image_repository(image).rsplit("/", 1)[-1])
    if relative is None:
        return False
    try:
        source = (repo / relative).read_text()
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"required Dockerfile {relative} is missing or unreadable") from exc
    user = ""
    for line in source.splitlines():
        instruction = line.strip().split(maxsplit=1)
        if instruction and instruction[0].upper() == "FROM":
            user = ""
        elif len(instruction) == 2 and instruction[0].upper() == "USER":
            user = instruction[1].split("#", 1)[0].strip()
    return bool(re.fullmatch(r"[0-9]+(?::[0-9]+)?", user) and int(user.split(":")[0]) > 0)


def _user_problems(
    repo: Path,
    pod: dict[str, Any],
    container: dict[str, Any],
    image: str,
    where: str,
    strict: bool,
) -> list[str]:
    context = container.get("securityContext") or {}
    if strict and (
        context.get("runAsNonRoot") is not True
        or type(context.get("runAsUser")) is not int
        or context["runAsUser"] != 1001
    ):
        return [f"P4 {where}: Langfuse container must declare runAsNonRoot=true and runAsUser=1001"]
    effective = {**(pod.get("securityContext") or {}), **context}
    if effective.get("runAsNonRoot") is not True:
        return []
    if "runAsUser" in effective:
        uid = effective["runAsUser"]
        valid = type(uid) is int and uid > 0
    else:
        try:
            valid = _numeric_user(repo, image)
        except ValueError as exc:
            return [f"P4 {where}: {exc}"]
    if not valid:
        return [
            f"P4 {where}: non-root user needs a positive integer runAsUser or final Dockerfile USER"
        ]
    return []


def _classification(
    container: dict[str, Any],
    image: str,
    is_init: bool,
    hook: bool,
    profile: str,
    endpoint: str,
) -> str | None:
    if is_init:
        return "init-bootstrap"
    if hook:
        return "helm-hook"
    if container.get("command") == ["sleep", "infinity"]:
        return "prewarm-sleep"
    if _image_repository(image).rsplit("/", 1)[-1] == "curie-ui":
        return "spa"
    if profile == "values-e2e-harness.yaml" and not endpoint:
        return "harness-no-collector"
    return None


def _telemetry_problems(container: dict[str, Any], endpoint: str, where: str) -> list[str]:
    errors = []
    env = {item.get("name"): item for item in container.get("env") or []}
    for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"):
        entry = env.get(name) or {}
        value = str(entry.get("value") or "").strip()
        if not value and not entry.get("valueFrom"):
            errors.append(f"P5 {where}: missing {name}")
        elif name.endswith("_ENDPOINT") and value and value != endpoint:
            errors.append(f"P5 {where}: {name} differs from chart-owned collector endpoint")
    return errors


def _endpoint(documents: list[dict[str, Any]], values: dict[str, Any]) -> str:
    otel = values.get("otelCollector") or {}
    if otel.get("deploy"):
        for document in documents:
            if document.get("kind") != "Service":
                continue
            spec = document.get("spec") or {}
            if (spec.get("selector") or {}).get("app.kubernetes.io/component") != "otel-collector":
                continue
            for port in spec.get("ports") or []:
                if port.get("name") == "otlp-http":
                    return f"http://{document['metadata']['name']}:{port['port']}"
        raise ValueError("P5 collector Service with otlp-http port is missing")
    return "" if otel.get("telemetryDisabled") else str(otel.get("endpoint") or "").strip()


def _required_telemetry(values: dict[str, Any], endpoint: str) -> set[str]:
    required = {
        f"curie-{component}"
        for component in ("api", "worker")
        if (values.get(component) or {}).get("deploy")
    }
    dispatcher = values.get("dispatcher") or {}
    slack = dispatcher.get("slack") or {}
    if dispatcher.get("deploy") and slack.get("appToken") and slack.get("botToken"):
        required.add("curie-dispatcher")
    if (values.get("mailAdapter") or {}).get("deploy"):
        required.add("curie-mail-adapter")
    if (values.get("agentSandbox") or {}).get("deploy") and endpoint:
        required.add("curie-runner")
    return required


def audit_chart(
    chart: Path,
    repo: Path,
    profiles: tuple[str | None, ...] = PROFILES,
    helm_args: tuple[str, ...] = (),
    exemptions: tuple[Exemption, ...] = EXEMPTIONS,
) -> list[str]:
    try:
        paths = {"values.yaml": chart / "values.yaml"}
        paths.update({path.name: path for path in sorted(chart.glob("values-*.yaml"))})
        paths.update({profile: chart / profile for profile in profiles if profile is not None})
        values = {name: _load(path) for name, path in paths.items()}
        compose = _load(repo / "compose.dev.yaml")
    except ValueError as exc:
        return [f"P1 {exc}"]
    errors = _input_problems(values, compose)
    matched: set[int] = set()
    requested = {profile or "default" for profile in profiles}
    for profile in profiles:
        label = profile or "default"
        command = ["helm", "template", "curie", str(chart)]
        if profile is not None:
            command.extend(["-f", str(chart / profile)])
        try:
            rendered = subprocess.run(
                [*command, *helm_args],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
            if rendered.returncode:
                errors.append(
                    f"enumeration [{label}]: helm template failed (exit {rendered.returncode})"
                )
                continue
            documents = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc is not None]
            if not documents or any(not isinstance(doc, dict) for doc in documents):
                raise ValueError("enumeration: render must contain YAML objects")
            settings = _settings(
                values["values.yaml"], values[profile] if profile is not None else {}, helm_args
            )
            endpoint = _endpoint(documents, settings)
        except (OSError, subprocess.TimeoutExpired, ValueError, yaml.YAMLError) as exc:
            errors.append(f"enumeration [{label}]: {exc}")
            continue
        targets: list[tuple[str, str]] = []
        first_party_seen = 0
        instrumented: set[str] = set()
        instrumented_kinds: set[str] = set()
        for document in documents:
            kind = document.get("kind")
            metadata = document.get("metadata") or {}
            workload = f"{kind}/{metadata.get('name', '<unnamed>')}"
            try:
                pod = _pod_spec(document)
            except ValueError as exc:
                errors.append(f"enumeration [{label}] {workload}: {exc}")
                continue
            if pod is None:
                continue
            hook = bool((metadata.get("annotations") or {}).get("helm.sh/hook"))
            for group in ("containers", "initContainers"):
                for container in pod.get(group) or []:
                    if not isinstance(container, dict):
                        errors.append(f"enumeration [{label}] {workload}: malformed {group} entry")
                        continue
                    cname = str(container.get("name") or "<unnamed>")
                    image = str(container.get("image") or "").strip()
                    where = f"[{label}] {workload} container={cname}"
                    is_init = group == "initContainers"
                    targets.append((workload, cname))
                    local = image in {"curie-api:local", "curie-worker:local", "curie-ui:local"}
                    local_class = (
                        f"offline-local-{_image_repository(image).removeprefix('curie-')}"
                        if local and container.get("imagePullPolicy") == "Never"
                        else None
                    )
                    if not _exempt(
                        exemptions,
                        matched,
                        "P1",
                        workload,
                        cname,
                        label,
                        local_class,
                    ):
                        if reason := _image_problem(image):
                            errors.append(f"P1 {where}: {reason}")
                    if kind in {"Deployment", "StatefulSet"} and not is_init:
                        if not _exempt(exemptions, matched, "P2", workload, cname, label):
                            for probe in ("readinessProbe", "livenessProbe"):
                                if (
                                    not isinstance(container.get(probe), dict)
                                    or not container[probe]
                                ):
                                    errors.append(f"P2 {where}: missing {probe}")
                            if cname == "langfuse-worker":
                                errors.extend(
                                    _worker_problems(
                                        container, where, profile is None and not helm_args
                                    )
                                )
                    errors.extend(_probe_problems(container, where))
                    strict = kind == "Deployment" and (
                        (
                            workload.endswith("-langfuse-web")
                            and cname in {"langfuse-web", "wait-for-postgres"}
                        )
                        or (workload.endswith("-langfuse-worker") and cname == "langfuse-worker")
                    )
                    if not _exempt(exemptions, matched, "P4", workload, cname, label):
                        errors.extend(_user_problems(repo, pod, container, image, where, strict))
                    if _first_party(image):
                        first_party_seen += 1
                        classification = _classification(
                            container, image, is_init, hook, label, endpoint
                        )
                        if not _exempt(
                            exemptions, matched, "P5", workload, cname, label, classification
                        ):
                            instrumented.add(_image_repository(image).rsplit("/", 1)[-1])
                            instrumented_kinds.add(str(kind))
                            errors.extend(_telemetry_problems(container, endpoint, where))
        if not first_party_seen:
            errors.append(f"P5 [{label}]: no first-party containers found")
        for missing in sorted(_required_telemetry(settings, endpoint) - instrumented):
            errors.append(
                f"P5 [{label}] workload image={missing} "
                f"container={missing.removeprefix('curie-')}: "
                "required instrumentable container missing"
            )
        if (
            "curie-runner" in _required_telemetry(settings, endpoint)
            and "SandboxTemplate" not in instrumented_kinds
        ):
            errors.append(
                f"P5 [{label}] SandboxTemplate/curie-runner container=runner: "
                "required instrumentable kind missing"
            )
        if profile is None and not helm_args:
            for suffix, cname in (
                ("web", "langfuse-web"),
                ("web", "wait-for-postgres"),
                ("worker", "langfuse-worker"),
            ):
                expected = (f"Deployment/curie-langfuse-{suffix}", cname)
                count = sum(
                    workload.startswith("Deployment/")
                    and workload.endswith(f"-langfuse-{suffix}")
                    and name == cname
                    for workload, name in targets
                )
                if count != 1:
                    errors.append(
                        f"P4 [{label}] {expected[0]} container={cname}: "
                        f"expected exactly one Langfuse container, found {count}"
                    )
    for index, entry in enumerate(exemptions):
        if entry.profile is not None and entry.profile not in requested:
            continue
        if index not in matched:
            errors.append(
                f"exemption {entry.rule_id} {entry.workload} container={entry.container}: "
                f"matches no container in requested profiles ({entry.reference})"
            )
    return errors
