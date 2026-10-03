"""image_build: build a public commit in the run's namespace (#3246, ADR 0176 decision 6).

Contract: build a commit fetchable by SHA from an allowlisted https host. One
Job in the run's own namespace runs three containers, so repository content
never runs beside the push credential:

1. Init ``source`` (git) fetches the commit into ``/workspace``. No credential.
2. Init ``build`` (kaniko) builds to ``/out/image.tar`` with ``--no-push``. It
   mounts only the optional cache credential, which Dockerfile ``RUN`` steps
   can reach, so that credential must be scoped to the cache repository.
3. Main ``push`` (crane) runs a fixed script that pushes the tarball under a
   unique staging tag, applies the run labels with ``crane mutate`` under a
   unique final tag, and writes the post mutate digest to its termination
   message. It is the only container that mounts the push credential, and it
   never executes repository content.

The target repository is recorded in the ``e2e-images`` ledger before any
Secret or Job exists, so teardown retention always knows what to delete, and a
ledger whose ``closing_at`` is set admits nothing.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import urllib.parse
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from curie_e2e_connector.contract import (
    BUILD_CACHE_K8S_SECRET_PREFIX,
    BUILD_EGRESS_POLICY,
    BUILD_LABEL,
    BUILD_PUSH_K8S_SECRET_PREFIX,
    FINAL_TAG_PREFIX,
    POD_SECURITY_LABEL,
    PUSH_SCRIPT,
    REFUSAL_BUILD_ARGUMENT,
    REFUSAL_BUILD_FAILED,
    REFUSAL_BUILD_NO_DIGEST,
    REFUSAL_BUILD_POD_SECURITY,
    REFUSAL_BUILD_TIMEOUT,
    REFUSAL_ENVIRONMENT_CLOSING,
    REFUSAL_MISCONFIGURED,
    REFUSAL_REGISTRY_NOT_CONFIGURED,
    STAGING_TAG_PREFIX,
)
from curie_e2e_connector.kube import (
    ClusterApi,
    ClusterError,
    checked_request,
    container_statuses,
    delete_quietly,
    ensure_object,
    job_pods,
    job_state,
    terminated_state,
)
from curie_e2e_connector.namespace import (
    Caller,
    Install,
    namespace_name,
    require_caller,
    require_environment,
    run_labels,
)
from curie_e2e_connector.registry import (
    DIGEST,
    NAME_COMPONENT,
    RegistryError,
    RegistrySettings,
    host_port,
    namespace_repository,
    parse_docker_config,
    split_repository,
    valid_host,
    validate_prefix,
)
from curie_e2e_connector.retention import (
    LEDGER_ATTEMPTS,
    configmap_body,
    configmaps_path,
    ledger_path,
    ledger_text,
    parse_ledger,
)

logger = logging.getLogger(__name__)

DEFAULT_SOURCE_HOSTS = ("github.com",)
_MIN_TIMEOUT = 60
_MAX_TIMEOUT = 3600
# Polling stops this long after the Job's own activeDeadlineSeconds.
_DEADLINE_GRACE_S = 30
_JOB_TTL_S = 600
_REASON_LIMIT = 1000
_MIN_REDACTED = 4

_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9._-]+$")
_PLATFORM = re.compile(r"^[a-z0-9]+/[a-z0-9]+(/[a-z0-9]+)?$")
_SOURCE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_BUILD_ID = re.compile(r"^[a-z0-9]{1,16}$")


def _misconfigured(reason: str) -> ClusterError:
    return ClusterError(f"{REFUSAL_MISCONFIGURED}: {reason}")


@dataclass(frozen=True)
class BuildConfig:
    registry: RegistrySettings
    cache_repo: str
    builder_image: str
    git_image: str
    push_image: str
    timeout_seconds: int = 1200
    source_hosts: tuple[str, ...] = DEFAULT_SOURCE_HOSTS
    poll_seconds: float = 5

    def validate(self) -> None:
        if not _MIN_TIMEOUT <= self.timeout_seconds <= _MAX_TIMEOUT:
            raise _misconfigured(f"build timeout is outside {_MIN_TIMEOUT}..{_MAX_TIMEOUT}")
        for label, value in (("registry", self.registry.prefix), ("cache repo", self.cache_repo)):
            if not value:
                continue
            try:
                validate_prefix(value)
            except RegistryError:
                raise _misconfigured(f"{label} is not a <host>[/path] repository") from None
        for entry in self.registry.token_hosts:
            if not valid_host(entry):
                raise _misconfigured("a registry token host is not host[:port]")
        if not self.source_hosts or not all(valid_host(host) for host in self.source_hosts):
            raise _misconfigured("source hosts must be host names")
        if not (self.builder_image and self.git_image and self.push_image):
            raise _misconfigured("a build image is empty")
        if self.poll_seconds <= 0:
            raise _misconfigured("build poll interval must be positive")


def _refuse(field_name: str, reason: str) -> ClusterError:
    return ClusterError(f"{REFUSAL_BUILD_ARGUMENT}: {field_name} {reason}")


def _relative_path(field_name: str, value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise _refuse(field_name, "must be a relative path of 1 to 256 characters")
    if value.startswith("/") or "\\" in value:
        raise _refuse(field_name, "must be a relative path with forward slashes")
    for segment in value.split("/"):
        if segment == ".." or not _PATH_SEGMENT.fullmatch(segment):
            raise _refuse(field_name, "segments must match [A-Za-z0-9._-]+ and not be ..")
    return value


@dataclass(frozen=True)
class BuildRequest:
    context: str
    dockerfile: str
    platform: str | None
    repository: str
    commit: str
    name: str

    @classmethod
    def parse(
        cls,
        context: str,
        dockerfile: str | None,
        platforms: list[str] | None,
        repository: str,
        commit: str,
        name: str | None,
        *,
        source_hosts: Iterable[str] = DEFAULT_SOURCE_HOSTS,
    ) -> BuildRequest:
        context = "." if context == "." else _relative_path("context", context)
        dockerfile = _relative_path("dockerfile", dockerfile or "Dockerfile")
        platform: str | None = None
        if platforms is not None:
            if not isinstance(platforms, list) or len(platforms) > 1:
                raise _refuse("platforms", "accepts at most one platform")
            if platforms:
                candidate = platforms[0]
                if not isinstance(candidate, str) or not _PLATFORM.fullmatch(candidate):
                    raise _refuse("platforms", "entry must look like linux/amd64")
                platform = candidate
        _check_source(repository, tuple(source_hosts))
        if not isinstance(commit, str) or not _COMMIT.fullmatch(commit):
            raise _refuse("commit", "must be a full 40 character lowercase hex SHA")
        name = "app" if name is None else name
        if (
            not isinstance(name, str)
            or not NAME_COMPONENT.fullmatch(name)
            or not _DNS_LABEL.fullmatch(name)
        ):
            raise _refuse("name", "must be a lowercase DNS label")
        return cls(
            context=context,
            dockerfile=dockerfile,
            platform=platform,
            repository=repository,
            commit=commit,
            name=name,
        )


def _check_source(repository: str, source_hosts: tuple[str, ...]) -> None:
    if not isinstance(repository, str) or "?" in repository or "#" in repository:
        raise _refuse("repository", "must be an https URL without a query or fragment")
    try:
        parts = urllib.parse.urlsplit(repository)
    except ValueError:
        raise _refuse("repository", "is not a URL") from None
    if parts.scheme != "https" or "@" in parts.netloc:
        raise _refuse("repository", "must be an https URL without credentials")
    if parts.netloc not in source_hosts:
        raise _refuse("repository", "host is not an allowed source host")
    path = parts.path
    if len(path) > 512 or not _SOURCE_PATH.fullmatch(path) or ".." in path.split("/"):
        raise _refuse("repository", "path must match [A-Za-z0-9._/-] within 512 characters")


def build_image(
    cluster: ClusterApi,
    install: Install,
    config: BuildConfig,
    caller: Caller,
    request: BuildRequest,
    *,
    push_config_text: str | None,
    cache_config_text: str | None,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    build_id: str,
    on_poll: Callable[[float], None],
) -> dict[str, Any]:
    """Build ``request`` and return ``{"images": [{"name", "digest"}]}``."""

    # Refusals before any cluster call.
    install.validate()
    config.validate()
    if not config.registry.prefix:
        raise ClusterError(
            f"{REFUSAL_REGISTRY_NOT_CONFIGURED}: this installation has no build registry"
        )
    if install.pod_security == "restricted":
        raise ClusterError(
            f"{REFUSAL_BUILD_POD_SECURITY}: image_build needs baseline Pod Security; "
            "this installation enforces restricted"
        )
    _check_source(request.repository, config.source_hosts)
    # Label values reach crane only through env, but are re-validated anyway.
    require_caller(caller.run, caller.work_item)
    if not _BUILD_ID.fullmatch(build_id):
        raise _misconfigured("build id is not a short lowercase token")
    push_text = push_config_text if push_config_text and push_config_text.strip() else None
    cache_text = cache_config_text if cache_config_text and cache_config_text.strip() else None
    parse_docker_config(push_text)
    parse_docker_config(cache_text)
    caching = bool(config.cache_repo) and cache_text is not None
    secrets_to_redact = _credential_strings(push_text) | _credential_strings(cache_text)

    namespace = namespace_name(install, caller)
    _require_build_environment(cluster, install, caller)
    repo = namespace_repository(config.registry, namespace, request.name)
    labels = run_labels(install, caller)
    _admit(cluster, namespace, repo, labels)
    ensure_object(
        cluster,
        f"/apis/networking.k8s.io/v1/namespaces/{namespace}/networkpolicies",
        _egress_policy(config, labels, caching=caching),
    )

    job = f"e2e-build-{build_id}"
    build_labels = {**labels, BUILD_LABEL: job}
    push_secret = f"{BUILD_PUSH_K8S_SECRET_PREFIX}{build_id}" if push_text else None
    cache_secret = f"{BUILD_CACHE_K8S_SECRET_PREFIX}{build_id}" if caching else None
    created: list[str] = []
    # A Secret whose POST raised may still have been persisted (a lost response),
    # or may be another build's (a 409). Cleanup reads it back and deletes it
    # only when it carries this build's label.
    uncertain: list[str] = []
    delete_job = False
    try:
        for secret_name, text in ((push_secret, push_text), (cache_secret, cache_text)):
            if secret_name is None or text is None:
                continue
            try:
                _create_secret(cluster, namespace, secret_name, text, build_labels)
            except ClusterError:
                uncertain.append(secret_name)
                raise
            created.append(secret_name)
        body = _job_body(
            job,
            config,
            install,
            caller,
            request,
            repo=repo,
            build_id=build_id,
            labels=build_labels,
            push_secret=push_secret,
            cache_secret=cache_secret,
        )
        code, _payload = checked_request(
            cluster, "POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", body
        )
        if code not in (200, 201):
            raise ClusterError(f"the test cluster refused the build Job ({code})")
        delete_job = True
        _confirm_admission(cluster, namespace)
        delete_job = False
        status = _poll(cluster, namespace, job, config, clock=clock, sleep=sleep, on_poll=on_poll)
        if status is None:
            delete_job = True
            raise ClusterError(
                f"{REFUSAL_BUILD_TIMEOUT}: the build did not finish within "
                f"{config.timeout_seconds}s"
            )
        succeeded, reason, message = status
        pods = job_pods(cluster, namespace, job)
        if succeeded:
            digest = _pushed_digest(pods, repo)
            return {"images": [{"name": repo, "digest": digest}]}
        if reason == "DeadlineExceeded":
            raise ClusterError(
                f"{REFUSAL_BUILD_TIMEOUT}: the build did not finish within "
                f"{config.timeout_seconds}s"
            )
        detail = _failure_reason(pods) or message or reason or "the build Job failed"
        raise ClusterError(f"{REFUSAL_BUILD_FAILED}: {_redact(detail, secrets_to_redact)}")
    finally:
        if delete_job:
            delete_quietly(
                cluster,
                f"/apis/batch/v1/namespaces/{namespace}/jobs/{job}?propagationPolicy=Background",
                "the build Job",
            )
        for secret_name in created:
            delete_quietly(
                cluster, f"/api/v1/namespaces/{namespace}/secrets/{secret_name}", "a build Secret"
            )
        for secret_name in uncertain:
            _delete_if_built_by(cluster, namespace, secret_name, job)


def _require_build_environment(cluster: ClusterApi, install: Install, caller: Caller) -> None:
    metadata = require_environment(cluster, install, caller, "image_build")
    labels = metadata.get("labels")
    if isinstance(labels, dict) and labels.get(POD_SECURITY_LABEL) == "restricted":
        raise ClusterError(
            f"{REFUSAL_BUILD_POD_SECURITY}: image_build needs baseline Pod Security; "
            "this namespace enforces restricted"
        )


def _closing() -> ClusterError:
    return ClusterError(
        f"{REFUSAL_ENVIRONMENT_CLOSING}: teardown has closed this environment to new builds"
    )


def _admit(cluster: ClusterApi, namespace: str, repo: str, labels: dict[str, str]) -> None:
    """Record ``repo`` in the ledger, refusing once teardown has closed it."""

    path = ledger_path(namespace)
    for _attempt in range(LEDGER_ATTEMPTS):
        code, payload = checked_request(cluster, "GET", path)
        if code == 404:
            body = configmap_body(ledger_text([repo]), labels=labels)
            created, _payload = checked_request(cluster, "POST", configmaps_path(namespace), body)
            if created in (200, 201):
                return
            if created == 409:
                continue
            raise ClusterError(f"the test cluster refused to create the image ledger ({created})")
        if code != 200:
            raise ClusterError(f"the test cluster did not answer the image ledger read ({code})")
        ledger = parse_ledger(payload)
        if ledger.closing_at is not None:
            raise _closing()
        if repo in ledger.repositories:
            return
        body = configmap_body(
            ledger_text([*ledger.repositories, repo]),
            labels=ledger.labels,
            resource_version=ledger.resource_version,
        )
        written, _payload = checked_request(cluster, "PUT", path, body)
        if written == 200:
            return
        if written == 409:
            continue
        raise ClusterError(f"the test cluster refused the image ledger update ({written})")
    raise ClusterError(
        f"the image ledger kept changing after {LEDGER_ATTEMPTS} attempts; call image_build again"
    )


def _confirm_admission(cluster: ClusterApi, namespace: str) -> None:
    """Re-read the ledger right after the Job POST; a close since admission refuses."""

    code, payload = checked_request(cluster, "GET", ledger_path(namespace))
    if code == 404:
        raise _closing()
    if code != 200:
        raise ClusterError(f"the test cluster did not answer the image ledger read ({code})")
    if parse_ledger(payload).closing_at is not None:
        raise _closing()


def _egress_policy(config: BuildConfig, labels: dict[str, str], *, caching: bool) -> dict[str, Any]:
    insecure = config.registry.insecure
    ports = [443]
    hosts = [split_repository(config.registry.prefix)[0]]
    if caching:
        hosts.append(split_repository(config.cache_repo)[0])
    # Source and token hosts add a port only when they name one explicitly.
    hosts += [host for host in (*config.source_hosts, *config.registry.token_hosts) if ":" in host]
    for host in hosts:
        port = host_port(host, insecure=insecure)
        if port not in ports:
            ports.append(port)
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": BUILD_EGRESS_POLICY, "labels": labels},
        "spec": {
            "podSelector": {"matchExpressions": [{"key": BUILD_LABEL, "operator": "Exists"}]},
            "policyTypes": ["Egress"],
            "egress": [
                {"ports": [{"protocol": "TCP", "port": port} for port in ports]},
                {"ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]},
            ],
        },
    }


def _create_secret(
    cluster: ClusterApi, namespace: str, name: str, text: str, labels: dict[str, str]
) -> None:
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "kubernetes.io/dockerconfigjson",
        "metadata": {"name": name, "labels": labels},
        "data": {".dockerconfigjson": base64.b64encode(text.encode("utf-8")).decode("ascii")},
    }
    code, _payload = checked_request(
        cluster, "POST", f"/api/v1/namespaces/{namespace}/secrets", body
    )
    if code not in (200, 201):
        raise ClusterError(f"the test cluster refused to create Secret {name} ({code})")


def _security(*, drop_all: bool = False) -> dict[str, Any]:
    context: dict[str, Any] = {"privileged": False}
    if drop_all:
        context["allowPrivilegeEscalation"] = False
        context["capabilities"] = {"drop": ["ALL"]}
    return context


def _resources(cpu: str, memory: str, cpu_limit: str, memory_limit: str) -> dict[str, Any]:
    return {
        "requests": {"cpu": cpu, "memory": memory},
        "limits": {"cpu": cpu_limit, "memory": memory_limit},
    }


def _job_body(
    job: str,
    config: BuildConfig,
    install: Install,
    caller: Caller,
    request: BuildRequest,
    *,
    repo: str,
    build_id: str,
    labels: dict[str, str],
    push_secret: str | None,
    cache_secret: str | None,
) -> dict[str, Any]:
    insecure = config.registry.insecure
    context_dir = "/workspace" if request.context == "." else f"/workspace/{request.context}"
    args = [
        f"--context=dir://{context_dir}",
        f"--dockerfile={context_dir}/{request.dockerfile}",
        f"--destination={repo}:{FINAL_TAG_PREFIX}{build_id}",
        "--no-push",
        "--tar-path=/out/image.tar",
    ]
    if cache_secret is not None:
        args += ["--cache=true", f"--cache-repo={config.cache_repo}"]
    else:
        args.append("--cache=false")
    if request.platform is not None:
        args.append(f"--custom-platform={request.platform}")
    if insecure:
        registry_host = split_repository(config.registry.prefix)[0]
        args.append(f"--insecure-registry={registry_host}")
        if cache_secret is not None:
            cache_host = split_repository(config.cache_repo)[0]
            if cache_host != registry_host:
                args.append(f"--insecure-registry={cache_host}")

    volumes: list[dict[str, Any]] = [
        {"name": "workspace", "emptyDir": {}},
        {"name": "out", "emptyDir": {}},
    ]
    build_mounts: list[dict[str, Any]] = [
        {"name": "workspace", "mountPath": "/workspace"},
        {"name": "out", "mountPath": "/out"},
    ]
    push_mounts: list[dict[str, Any]] = [{"name": "out", "mountPath": "/out", "readOnly": True}]
    push_env = [
        {"name": "DEST", "value": repo},
        {"name": "STAGING_TAG", "value": f"{STAGING_TAG_PREFIX}{build_id}"},
        {"name": "BUILD_TAG", "value": f"{FINAL_TAG_PREFIX}{build_id}"},
        {"name": "LABEL_OWNER", "value": install.owner_label_value},
        {"name": "LABEL_RUN", "value": caller.run},
        {"name": "LABEL_WORK_ITEM", "value": caller.work_item},
    ]
    if insecure:
        push_env.append({"name": "CRANE_INSECURE", "value": "1"})
    if push_secret is not None:
        volumes.append(_config_volume("push-config", push_secret))
        push_mounts.append({"name": "push-config", "mountPath": "/docker-config", "readOnly": True})
        push_env.append({"name": "DOCKER_CONFIG", "value": "/docker-config"})
    if cache_secret is not None:
        volumes.append(_config_volume("cache-config", cache_secret))
        build_mounts.append(
            {"name": "cache-config", "mountPath": "/kaniko/.docker", "readOnly": True}
        )

    source = {
        "name": "source",
        "image": config.git_image,
        "command": [
            "sh",
            "-ec",
            'git init -q /workspace && cd /workspace && git fetch -q --depth 1 "$1" "$2" '
            "&& git checkout -q FETCH_HEAD",
            "fetch",
            request.repository,
            request.commit,
        ],
        "env": [{"name": "GIT_TERMINAL_PROMPT", "value": "0"}],
        "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}],
        "resources": _resources("100m", "128Mi", "500m", "256Mi"),
        "securityContext": _security(),
        "terminationMessagePolicy": "FallbackToLogsOnError",
    }
    build = {
        "name": "build",
        "image": config.builder_image,
        "args": args,
        "volumeMounts": build_mounts,
        "resources": _resources("500m", "1Gi", "2", "2Gi"),
        "securityContext": _security(),
        "terminationMessagePolicy": "FallbackToLogsOnError",
    }
    push = {
        "name": "push",
        "image": config.push_image,
        "command": ["sh", "-c", PUSH_SCRIPT],
        "env": push_env,
        "volumeMounts": push_mounts,
        "resources": _resources("100m", "128Mi", "500m", "512Mi"),
        "securityContext": _security(drop_all=True),
        "terminationMessagePolicy": "FallbackToLogsOnError",
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job, "labels": labels},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": config.timeout_seconds,
            "ttlSecondsAfterFinished": _JOB_TTL_S,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "initContainers": [source, build],
                    "containers": [push],
                    "volumes": volumes,
                },
            },
        },
    }


def _config_volume(name: str, secret: str) -> dict[str, Any]:
    return {
        "name": name,
        "secret": {
            "secretName": secret,
            "items": [{"key": ".dockerconfigjson", "path": "config.json"}],
        },
    }


def _poll(
    cluster: ClusterApi,
    namespace: str,
    job: str,
    config: BuildConfig,
    *,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    on_poll: Callable[[float], None],
) -> tuple[bool, str, str] | None:
    """Return ``(succeeded, reason, message)``, or None at the connector deadline."""

    start = clock()
    limit = start + config.timeout_seconds + _DEADLINE_GRACE_S
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs/{job}"
    while True:
        code, payload = checked_request(cluster, "GET", path)
        on_poll(clock() - start)
        if code == 404:
            raise ClusterError(f"{REFUSAL_BUILD_FAILED}: the environment was removed")
        if code != 200:
            raise ClusterError(f"the test cluster did not answer a build Job read ({code})")
        state = job_state(payload.get("status"))
        if state is not None:
            return state
        if clock() >= limit:
            return None
        sleep(config.poll_seconds)


def _pushed_digest(pods: list[dict[str, Any]], repo: str) -> str:
    for pod in pods:
        for container in container_statuses(pod, "containerStatuses"):
            if container.get("name") != "push":
                continue
            terminated = terminated_state(container)
            message = terminated.get("message") if terminated else None
            if not isinstance(message, str):
                continue
            name, sep, digest = message.strip().rpartition("@")
            if sep and name == repo and DIGEST.fullmatch(digest):
                return digest
    raise ClusterError(
        f"{REFUSAL_BUILD_NO_DIGEST}: the push container did not report a digest for {repo}"
    )


def _failure_reason(pods: list[dict[str, Any]]) -> str:
    by_name: dict[str, dict[str, Any]] = {}
    for pod in pods:
        for key in ("initContainerStatuses", "containerStatuses"):
            for container in container_statuses(pod, key):
                terminated = terminated_state(container)
                name = container.get("name")
                if terminated is not None and isinstance(name, str):
                    by_name.setdefault(name, terminated)
    for name in ("build", "source", "push"):
        terminated = by_name.get(name)
        if terminated is None or terminated.get("exitCode") in (0, None):
            continue
        detail = terminated.get("message") or terminated.get("reason") or ""
        return f"{name}: {detail}" if detail else f"{name} exited {terminated.get('exitCode')}"
    return ""


def _credential_strings(text: str | None) -> set[str]:
    """Every credential value a docker config holds, for redaction."""

    if not text:
        return set()
    found: set[str] = set()
    for user, password in parse_docker_config(text).values():
        found.update({password, f"{user}:{password}"})
    try:
        parsed = json.loads(text)
    except ValueError:
        return found
    auths = parsed.get("auths") if isinstance(parsed, dict) else None
    for entry in auths.values() if isinstance(auths, dict) else []:
        if not isinstance(entry, dict):
            continue
        for key in ("auth", "password", "identitytoken", "registrytoken"):
            value = entry.get(key)
            if isinstance(value, str):
                found.add(value)
    return {value for value in found if len(value) >= _MIN_REDACTED}


def _redact(text: str, secrets: set[str]) -> str:
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, "[redacted]")
    return text[-_REASON_LIMIT:]


def _delete_if_built_by(cluster: ClusterApi, namespace: str, name: str, job: str) -> None:
    """Delete Secret ``name`` only if it exists and carries this build's label.

    Best effort like ``delete_quietly``: never masks the error being raised.
    Logs never name the Secret, only that cleanup of one failed.
    """

    path = f"/api/v1/namespaces/{namespace}/secrets/{name}"
    try:
        code, payload = cluster.request("GET", path)
    except ClusterError:
        logger.warning("e2e build cleanup could not read a build Secret in namespace=%s", namespace)
        return
    if code == 404:
        return
    if code != 200:
        logger.warning(
            "e2e build cleanup was refused reading a build Secret in namespace=%s (%s)",
            namespace,
            code,
        )
        return
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    if not isinstance(labels, dict) or labels.get(BUILD_LABEL) != job:
        logger.warning(
            "e2e build cleanup left a Secret in namespace=%s: it is not this build's", namespace
        )
        return
    delete_quietly(cluster, path, "a build Secret")
