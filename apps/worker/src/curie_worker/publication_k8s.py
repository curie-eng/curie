"""Deterministic, secret-minimizing Kubernetes publication resources.

The patch is carried as ConfigMap ``binaryData`` and the short-lived push
credential lives only in a Secret volume.  Neither value appears in Job argv,
environment, labels, or logs.  Names are publication-id-derived so a worker
restart adopts the same resource set instead of starting a second push.

The Job only pushes (ADR 0197, "Two ports" item 6). It clones the origin the
API named, applies the approved patch as one marked commit, and pushes it with
a lease on the expected remote head. It calls no code host API: the worker asks
the API to check the stored pull request before the Job launches and to find
or open it after the push.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import posixpath
import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, get_args
from urllib.parse import urlsplit

from aci_protocol import BootEnv
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

if TYPE_CHECKING:
    from .publication_loop import PublicationJobObservation

MAX_PATCH_BYTES = 900_000
_AUTHORIZATION_LOG = re.compile(
    r"(?:Authorization|PRIVATE-TOKEN):\s*(?:(?:Basic|Bearer|token)\s+)?[^\s]+", re.IGNORECASE
)
_URL_USERINFO = re.compile(r"(https?://)[^/\s@]+@", re.IGNORECASE)
_MARKER_LOG_LINE = re.compile(r"^CURIE_[A-Z_]+=")
# Failed Job text leaves the cluster into thread history; keep it bounded.
_MAX_JOB_ERROR = 1500
_REPOSITORY_SEGMENT = re.compile(r"[A-Za-z0-9._-]+")

# How git sends the push credential, as the API names it (ADR 0197). GitHub
# is ``authorization_basic`` with ``x-access-token``; GitLab refuses a Bearer
# header, so the form travels with the credential instead of being assumed.
HeaderForm = Literal["authorization_basic", "authorization_bearer", "private_token"]
HEADER_FORMS: frozenset[str] = frozenset(get_args(HeaderForm))
# The Job's env names for the transport facts, read back on adoption.
_ORIGIN_ENV = "CODE_HOST_ORIGIN"
_HEADER_FORM_ENV = "CODE_HOST_HEADER_FORM"
# The same boot contract key the sandbox reads, named from its one declaration.
_CA_BUNDLE_ENV = BootEnv.env_key("repo_ca_bundle")
_CA_VOLUME = "code-host-trust"


def _redact(text: str) -> str:
    text = _AUTHORIZATION_LOG.sub("Authorization: [REDACTED]", text)
    return _URL_USERINFO.sub(r"\1[REDACTED]@", text)


def _field(obj: Any, name: str, attr: str | None = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, attr or name, None)


class PublicationResourceError(RuntimeError):
    """A publication cannot be represented by the hardened Job contract."""


@dataclass(frozen=True)
class PublicationTransport:
    """Where and how the Job pushes, as the API's credential named it.

    None of these is secret. ``origin`` is the code host's scheme, host and
    optional base path; the clone URL is the origin plus the repository path.
    ``ca_bundle_ref`` is the path of a PEM bundle mounted in the Job, or None
    for the public trust store.
    """

    origin: str
    header_form: HeaderForm
    ca_bundle_ref: str | None = None


def clean_origin(origin: str) -> bool:
    """Whether ``origin`` is a credential-free HTTPS origin with no trailing slash."""

    try:
        parsed = urlsplit(origin)
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and not origin.endswith("/")
    )


def valid_repository_path(path: str) -> bool:
    """A repository path of two or more plain segments (``owner/name`` or deeper)."""

    segments = path.split("/")
    return len(segments) >= 2 and all(
        _REPOSITORY_SEGMENT.fullmatch(segment) and segment not in {".", ".."}
        for segment in segments
    )


@dataclass(frozen=True)
class PublicationJobSettings:
    namespace: str
    runner_image: str
    image_pull_policy: str
    image_pull_secrets: tuple[str, ...]
    priority_class_name: str
    service_account_name: str
    owner_name: str
    git_user_name: str
    git_user_email: str
    cpu_request: str
    cpu_limit: str
    memory_request: str
    memory_limit: str
    ephemeral_request: str
    ephemeral_limit: str
    owner_uid: str | None = None
    active_deadline_seconds: int = 300
    git_timeout_seconds: int = 60
    # The ConfigMap holding the operator's code host CA bundle
    # (codeHostTrust.caBundle.configMapRef), mounted read-only at the path the
    # credential's ``ca_bundle_ref`` names. Empty means none is configured.
    ca_bundle_config_map: str = ""
    ca_bundle_key: str = "ca.crt"


@dataclass(frozen=True)
class PublicationPayload:
    publication_id: uuid.UUID
    revision_id: uuid.UUID
    revision_number: int
    repo_full_name: str
    clean_clone_url: str
    base_sha: str
    expected_prior_head: str
    expected_remote_head: str | None
    patch: bytes
    branch: str
    title: str
    transport: PublicationTransport
    branch_prefix: str | None = None


@dataclass(frozen=True)
class PublicationResourceNames:
    config_map: str
    secret: str
    job: str


@dataclass(frozen=True)
class PublicationResources:
    names: PublicationResourceNames
    config_map: dict[str, Any]
    secret: dict[str, Any]
    job: dict[str, Any]
    owner_uid: str


def deterministic_publication_branch(publication_id: uuid.UUID) -> str:
    return f"curie/publication-{publication_id.hex}"


def publication_resource_names(publication_id: uuid.UUID) -> PublicationResourceNames:
    suffix = publication_id.hex[:20]
    return PublicationResourceNames(
        config_map=f"curie-publication-{suffix}",
        secret=f"curie-publication-{suffix}",
        job=f"curie-publication-{suffix}",
    )


_PUBLISH_SCRIPT = r"""#!/bin/bash
set -euo pipefail
umask 077
if [[ -n "${PUBLICATION_BRANCH_PREFIX:-}" && "$BRANCH" != "${PUBLICATION_BRANCH_PREFIX}"* ]]; then
  echo "publication branch does not carry the required prefix" >&2
  exit 1
fi

redact() {
  # Credentials are never deliberately logged. This filter is defence in depth
  # for diagnostics git returns: it drops userinfo from any URL, including the
  # configured origin's, and any credential header.
  auth_scheme='([Bb][Aa][Ss][Ii][Cc]|[Bb][Ee][Aa][Rr][Ee][Rr]|'
  auth_scheme+='[Tt][Oo][Kk][Ee][Nn])'
  sed -E -e 's#(https?://)[^/@[:space:]]*@#\1#g' \
      -e "s#(Authorization|PRIVATE-TOKEN):[[:space:]]*(${auth_scheme}[[:space:]]+)?"\
"[^[:space:]]+#Authorization: [REDACTED]#gI"
}

git_with_timeout() {
  # Git must not forward an operator credential to a redirected origin. Keep
  # this invocation-scoped so no credential or transport setting is persisted
  # in the checkout configuration.
  timeout --signal=TERM "${GIT_TIMEOUT_SECONDS}s" git -c http.followRedirects=false \
    -c include.path=/tmp/curie-git-auth.config "$@"
}

cleanup_auth() {
  rm -f /tmp/curie-git-user /tmp/curie-git-pass /tmp/curie-askpass \
    /tmp/curie-git-auth.config
}
trap cleanup_auth EXIT

credential_path="${CURIE_CREDENTIAL_PATH:-/credentials/credential}"
patch_path="${CURIE_PATCH_PATH:-/publication/changes.patch}"
work_dir="${CURIE_WORK_DIR:-/work}"
export CURIE_CREDENTIAL_PATH="$credential_path"

if [[ "$CLEAN_CLONE_URL" != "$CODE_HOST_ORIGIN/"* ]]; then
  echo "publication clone URL is not under the code host origin" >&2
  exit 1
fi

# The header form decides how git presents the credential (ADR 0197). Basic is
# a username and password through askpass; any other form is one extra header
# scoped to the origin, written to an include file so it never enters argv or
# the environment.
python - <<'PY'
import base64
import os
from pathlib import Path

value = Path(os.environ["CURIE_CREDENTIAL_PATH"]).read_text().strip()
form = os.environ["CODE_HOST_HEADER_FORM"]
origin = os.environ["CODE_HOST_ORIGIN"]
user, password, header = "", "", ""
if form == "authorization_basic":
    scheme, _, encoded = value.partition(" ")
    if scheme.lower() != "basic":
        raise SystemExit("publication credential is not Basic authorization")
    try:
        user, password = base64.b64decode(encoded, validate=True).decode().split(":", 1)
    except (ValueError, UnicodeError):
        raise SystemExit("publication credential is not valid Basic authorization")
elif form == "authorization_bearer":
    if not value.lower().startswith("bearer "):
        raise SystemExit("publication credential is not Bearer authorization")
    header = f"Authorization: {value}"
elif form == "private_token":
    header = f"PRIVATE-TOKEN: {value}"
else:
    raise SystemExit("publication credential header form is unknown")
if any(char in value for char in "\r\n\0\"\\"):
    raise SystemExit("publication credential contains a forbidden character")
Path("/tmp/curie-git-user").write_text(user)
Path("/tmp/curie-git-pass").write_text(password)
config = f'[http "{origin}/"]\n\textraHeader = "{header}"\n' if header else ""
Path("/tmp/curie-git-auth.config").write_text(config)
PY

cat >/tmp/curie-askpass <<'ASKPASS'
#!/bin/sh
case "$1" in
  *sername*) cat /tmp/curie-git-user ;;
  *) cat /tmp/curie-git-pass ;;
esac
ASKPASS
chmod 0700 /tmp/curie-askpass
export GIT_ASKPASS=/tmp/curie-askpass
export GIT_TERMINAL_PROMPT=0

if [[ ! -s "$patch_path" ]]; then
  echo "publication Job requires a non-empty patch" >&2
  exit 1
fi

mkdir -p "$work_dir"
cd "$work_dir"
git_with_timeout clone "$CLEAN_CLONE_URL" repo 2> >(redact >&2)
cd repo
git_with_timeout remote set-url origin "$CLEAN_CLONE_URL"
git_with_timeout config --get remote.origin.url | grep -Fx "$CLEAN_CLONE_URL" >/dev/null
if ! git_with_timeout cat-file -e "${BASE_SHA}^{commit}" 2>/dev/null; then
  git_with_timeout fetch --depth=1 origin "$BASE_SHA" 2> >(redact >&2)
fi
git_with_timeout checkout --detach "$BASE_SHA" 2> >(redact >&2)
git_with_timeout switch -c "$BRANCH" 2> >(redact >&2)
remote_head=$(git_with_timeout ls-remote origin "refs/heads/$BRANCH" \
  2> >(redact >&2) | awk '{print $1}')
if [[ "$remote_head" != "$EXPECTED_REMOTE_HEAD" ]]; then
  echo "publication branch head conflict" >&2
  exit 1
fi
git_with_timeout apply --check --binary "$patch_path"
git_with_timeout apply --binary "$patch_path"
git_with_timeout add --all
if git_with_timeout diff --cached --quiet; then
  echo "publication patch produced no changes" >&2
  exit 1
fi
git_with_timeout -c user.name="$GIT_USER_NAME" -c user.email="$GIT_USER_EMAIL" commit \
  -m "$COMMIT_TITLE" -m "Curie-Revision: $REVISION_ID"
commit_sha=$(git_with_timeout rev-parse HEAD)
# The lease is the branch guard: the push lands only if the remote branch still
# holds the head this revision was approved against.
git_with_timeout push \
  --force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD \
  origin "HEAD:refs/heads/$BRANCH" 2> >(redact >&2)
echo "CURIE_COMMIT_SHA=$commit_sha"
"""


def _owner_reference(settings: PublicationJobSettings, owner_uid: str) -> list[dict[str, Any]]:
    return [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "name": settings.owner_name,
            "uid": owner_uid,
            "controller": False,
            "blockOwnerDeletion": False,
        }
    ]


def publication_branch_is_valid(branch: str, branch_prefix: str | None) -> bool:
    """Accept a stored lineage branch without renaming it.

    Historical branches stay under ``curie/``. A platform-named automatic
    branch (``<prefix>publication-<hex>``) stays valid after the operator
    clears that prefix. An optional prefix, when still recorded, must match.
    Components that git itself rejects (``.lock``, a trailing dot, ``..``)
    never pass.
    """

    if (
        ".." in branch
        or branch.startswith(("/", "."))
        or "//" in branch
        or branch.endswith("/")
    ):
        return False
    parts = branch.split("/")
    if any(not part or part.endswith(".lock") or part.endswith(".") for part in parts):
        return False
    if branch_prefix:
        rest = branch.removeprefix(branch_prefix)
        if not (
            branch.startswith(branch_prefix)
            and rest != ""
            and re.fullmatch(r"[A-Za-z0-9._/-]+", rest) is not None
        ):
            return False
    historical = re.fullmatch(r"curie/[A-Za-z0-9._/-]+", branch) is not None
    named = (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}/publication-[0-9a-f]+", branch)
        is not None
    )
    return historical or named


def _valid_publication_branch(payload: PublicationPayload) -> bool:
    return publication_branch_is_valid(payload.branch, payload.branch_prefix)


def _check_transport(
    payload: PublicationPayload, settings: PublicationJobSettings
) -> tuple[str, str] | None:
    """Refuse a transport the Job cannot honor; the CA mount directory and file, or None."""

    transport = payload.transport
    if not clean_origin(transport.origin):
        raise PublicationResourceError("publication code host origin is not clean HTTPS")
    if transport.header_form not in HEADER_FORMS:
        raise PublicationResourceError("publication credential header form is unknown")
    if not valid_repository_path(payload.repo_full_name):
        raise PublicationResourceError("publication repository path is invalid")
    parsed_clone = urlsplit(payload.clean_clone_url)
    if (
        parsed_clone.username is not None
        or parsed_clone.password is not None
        or payload.clean_clone_url != f"{transport.origin}/{payload.repo_full_name}.git"
    ):
        raise PublicationResourceError(
            "publication clone URL does not match the requested repository"
        )
    ref = transport.ca_bundle_ref
    if ref is None:
        return None
    directory, filename = posixpath.split(posixpath.normpath(ref))
    if (
        not ref.startswith("/")
        or posixpath.normpath(ref) != ref
        or not filename
        or directory in {"", "/"}
        or directory.startswith(("/publication", "/credentials", "/work", "/tmp"))
    ):
        raise PublicationResourceError("publication CA bundle reference is not a mountable path")
    if not settings.ca_bundle_config_map:
        raise PublicationResourceError(
            "the credential names a code host CA bundle but no codeHostTrust "
            "ConfigMap is configured for the publication Job"
        )
    return directory, filename


def build_publication_resources(
    payload: PublicationPayload,
    *,
    credential: str,
    settings: PublicationJobSettings,
) -> PublicationResources:
    if len(payload.patch) > MAX_PATCH_BYTES:
        raise PublicationResourceError(
            f"publication patch exceeds the {MAX_PATCH_BYTES} raw-byte limit"
        )
    if not payload.patch:
        # A metadata-only revision has nothing to push; the API applies it.
        raise PublicationResourceError("the publication Job only pushes a non-empty patch")
    if not _valid_publication_branch(payload):
        raise PublicationResourceError("publication branch is not a valid stored lineage branch")
    if re.fullmatch(r"[0-9a-f]{40,64}", payload.base_sha) is None:
        raise PublicationResourceError(
            "publication base SHA must be 40-64 lowercase hexadecimal characters"
        )
    if re.fullmatch(r"[0-9a-f]{40,64}", payload.expected_prior_head) is None:
        raise PublicationResourceError(
            "publication expected prior head must be 40-64 lowercase hexadecimal characters"
        )
    if payload.base_sha != payload.expected_prior_head:
        raise PublicationResourceError(
            "publication base SHA does not match expected prior head"
        )
    if payload.expected_remote_head is not None and (
        re.fullmatch(r"[0-9a-f]{40,64}", payload.expected_remote_head) is None
        or payload.expected_remote_head != payload.expected_prior_head
    ):
        raise PublicationResourceError("publication expected remote head is invalid")
    if payload.revision_number <= 0:
        raise PublicationResourceError("publication revision number must be positive")
    ca_mount = _check_transport(payload, settings)
    if not credential.strip():
        raise PublicationResourceError("publication credential is empty")
    if min(settings.active_deadline_seconds, settings.git_timeout_seconds) <= 0:
        raise PublicationResourceError("publication timeouts must be positive")
    names = publication_resource_names(payload.publication_id)
    owner_uid = settings.owner_uid or str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"curie:{settings.namespace}:{settings.owner_name}")
    )
    labels = {
        "app.kubernetes.io/managed-by": "curie",
        "curietech.ai/component": "publication",
        "curietech.ai/publication-id": str(payload.publication_id),
    }
    contract = {
        "publication_id": str(payload.publication_id),
        "revision_id": str(payload.revision_id),
        "revision_number": payload.revision_number,
        "repo_full_name": payload.repo_full_name,
        "clean_clone_url": payload.clean_clone_url,
        "origin": payload.transport.origin,
        "header_form": payload.transport.header_form,
        "ca_bundle_ref": payload.transport.ca_bundle_ref,
        "ca_bundle_config_map": settings.ca_bundle_config_map if ca_mount else None,
        "ca_bundle_key": settings.ca_bundle_key if ca_mount else None,
        "base_sha": payload.base_sha,
        "expected_prior_head": payload.expected_prior_head,
        "expected_remote_head": payload.expected_remote_head,
        "patch_sha256": hashlib.sha256(payload.patch).hexdigest(),
        "branch": payload.branch,
        "title": payload.title,
        "branch_prefix": payload.branch_prefix,
        "runner_image": settings.runner_image,
        "service_account_name": settings.service_account_name,
        "active_deadline_seconds": settings.active_deadline_seconds,
        "git_timeout_seconds": settings.git_timeout_seconds,
    }
    annotations = {
        "curietech.ai/publication-contract-sha256": hashlib.sha256(
            json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    }
    metadata: dict[str, Any] = {
        "namespace": settings.namespace,
        "labels": labels,
        "annotations": annotations,
        "ownerReferences": _owner_reference(settings, owner_uid),
    }
    config_map = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {**metadata, "name": names.config_map},
        "immutable": True,
        "binaryData": {
            "changes.patch": base64.b64encode(payload.patch).decode("ascii"),
        },
        "data": {"publish.sh": _PUBLISH_SCRIPT},
    }
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {**metadata, "name": names.secret},
        "immutable": True,
        "type": "Opaque",
        "stringData": {"credential": credential},
    }
    env = [
        {"name": "REPO_FULL_NAME", "value": payload.repo_full_name},
        {"name": "CLEAN_CLONE_URL", "value": payload.clean_clone_url},
        {"name": _ORIGIN_ENV, "value": payload.transport.origin},
        {"name": _HEADER_FORM_ENV, "value": payload.transport.header_form},
        {"name": "BASE_SHA", "value": payload.base_sha},
        {"name": "BRANCH", "value": payload.branch},
        {"name": "REVISION_ID", "value": str(payload.revision_id)},
        {"name": "REVISION_NUMBER", "value": str(payload.revision_number)},
        {"name": "EXPECTED_PRIOR_HEAD", "value": payload.expected_prior_head},
        {"name": "EXPECTED_REMOTE_HEAD", "value": payload.expected_remote_head or ""},
        {"name": "COMMIT_TITLE", "value": payload.title},
        {"name": "PUBLICATION_BRANCH_PREFIX", "value": payload.branch_prefix or ""},
        {"name": "GIT_USER_NAME", "value": settings.git_user_name},
        {"name": "GIT_USER_EMAIL", "value": settings.git_user_email},
        {"name": "GIT_TIMEOUT_SECONDS", "value": str(settings.git_timeout_seconds)},
    ]
    volume_mounts: list[dict[str, Any]] = [
        {"name": "publication", "mountPath": "/publication", "readOnly": True},
        {"name": "credentials", "mountPath": "/credentials", "readOnly": True},
        {"name": "work", "mountPath": "/work"},
        {"name": "tmp", "mountPath": "/tmp"},
    ]
    volumes: list[dict[str, Any]] = [
        {"name": "publication", "configMap": {"name": names.config_map}},
        {"name": "credentials", "secret": {"secretName": names.secret}},
        {"name": "work", "emptyDir": {"sizeLimit": "2Gi"}},
        {"name": "tmp", "emptyDir": {"sizeLimit": "16Mi"}},
    ]
    if ca_mount is not None:
        # One trust bundle for git and any HTTP client, at the path the
        # credential named (ADR 0197). Unset, nothing here changes.
        ca_directory, ca_file = ca_mount
        ca_path = posixpath.join(ca_directory, ca_file)
        env.extend(
            {"name": name, "value": ca_path}
            for name in (_CA_BUNDLE_ENV, "GIT_SSL_CAINFO", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")
        )
        volume_mounts.append({"name": _CA_VOLUME, "mountPath": ca_directory, "readOnly": True})
        volumes.append(
            {
                "name": _CA_VOLUME,
                "configMap": {
                    "name": settings.ca_bundle_config_map,
                    "items": [{"key": settings.ca_bundle_key, "path": ca_file}],
                },
            }
        )
    pod_spec: dict[str, Any] = {
        "serviceAccountName": settings.service_account_name,
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "priorityClassName": settings.priority_class_name,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 1000,
            "runAsGroup": 1000,
            "fsGroup": 1000,
            "fsGroupChangePolicy": "OnRootMismatch",
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "imagePullSecrets": [{"name": name} for name in settings.image_pull_secrets],
        "containers": [
            {
                "name": "publish",
                "image": settings.runner_image,
                "imagePullPolicy": settings.image_pull_policy,
                "command": ["/bin/bash", "/publication/publish.sh"],
                "env": env,
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "readOnlyRootFilesystem": True,
                },
                "resources": {
                    "requests": {
                        "cpu": settings.cpu_request,
                        "memory": settings.memory_request,
                        "ephemeral-storage": settings.ephemeral_request,
                    },
                    "limits": {
                        "cpu": settings.cpu_limit,
                        "memory": settings.memory_limit,
                        "ephemeral-storage": settings.ephemeral_limit,
                    },
                },
                "volumeMounts": volume_mounts,
            }
        ],
        "volumes": volumes,
    }
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {**metadata, "name": names.job},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": settings.active_deadline_seconds,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": dict(labels)},
                "spec": pod_spec,
            },
        },
    }
    return PublicationResources(
        names=names,
        config_map=config_map,
        secret=secret,
        job=job,
        owner_uid=owner_uid,
    )


def _as_serialized_mapping(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return copy.deepcopy(obj)
    serialized = k8s_client.ApiClient().sanitize_for_serialization(obj)
    if not isinstance(serialized, dict):
        raise PublicationResourceError("Kubernetes returned a non-object publication resource")
    return serialized


def _expected_projection(observed: Any, expected: Any) -> Any:
    """Project API-defaulted state onto the immutable submitted contract.

    The apiserver omits empty strings, lists and maps (omitempty), so an
    absent observed field matches an exactly-empty expected one. Absent and
    empty are identical to the kubelet; ``0``/``False`` never qualify.
    """

    if observed is None and (
        (type(expected) is str and expected == "")
        or (type(expected) is list and len(expected) == 0)
        or (type(expected) is dict and len(expected) == 0)
    ):
        return expected

    if isinstance(expected, dict):
        if not isinstance(observed, dict):
            return observed
        return {
            key: _expected_projection(observed.get(key), value)
            for key, value in expected.items()
        }
    if isinstance(expected, list):
        if not isinstance(observed, list) or len(observed) != len(expected):
            return observed
        return [
            _expected_projection(observed_value, expected_value)
            for observed_value, expected_value in zip(observed, expected, strict=True)
        ]
    return observed


def validate_adopted_resource(
    kind: str,
    expected: dict[str, Any],
    observed: Any,
    *,
    compare_secret_value: bool = True,
) -> None:
    """Fail closed unless an exact-name object carries our immutable contract.

    Kubernetes defaults fields after creation, so Jobs compare the submitted
    spec projection plus explicit pod escape hatches. Secret credential bytes
    are compared without logging while a matching Job exists. If its Job no
    longer exists, ``apply`` uses only an exact-name metadata/key-shape GET
    (never a list), then UID-replaces the immutable Secret with the freshly
    redeemed credential before creating another Job.
    """

    actual = _as_serialized_mapping(observed)
    expected_metadata = expected["metadata"]
    metadata_contract = {
        key: expected_metadata[key]
        for key in ("name", "namespace", "labels", "annotations", "ownerReferences")
    }
    if _expected_projection(actual.get("metadata"), metadata_contract) != metadata_contract:
        raise PublicationResourceError(
            f"refusing to adopt {kind} {expected_metadata['name']!r}: metadata contract mismatch"
        )

    if kind == "ConfigMap":
        contract = {
            "immutable": expected["immutable"],
            "binaryData": expected["binaryData"],
            "data": expected["data"],
        }
        if _expected_projection(actual, contract) != contract:
            raise PublicationResourceError(
                f"refusing to adopt ConfigMap {expected_metadata['name']!r}: payload mismatch"
            )
        return

    if kind == "Secret":
        actual_keys = set((actual.get("data") or actual.get("stringData") or {}).keys())
        if (
            actual.get("immutable") is not True
            or actual.get("type") != expected["type"]
            or actual_keys != {"credential"}
        ):
            raise PublicationResourceError(
                f"refusing to adopt Secret {expected_metadata['name']!r}: immutable shape mismatch"
            )
        if compare_secret_value:
            encoded = (actual.get("data") or {}).get("credential")
            if not isinstance(encoded, str) or not encoded:
                raise PublicationResourceError(
                    f"refusing to adopt Secret {expected_metadata['name']!r}: credential mismatch"
                )
            try:
                actual_credential = base64.b64decode(encoded, validate=True).decode()
            except (TypeError, ValueError, UnicodeError) as exc:
                raise PublicationResourceError(
                    f"refusing to adopt Secret {expected_metadata['name']!r}: credential mismatch"
                ) from exc
            expected_credential = str(expected["stringData"]["credential"])
            if not hmac.compare_digest(actual_credential, expected_credential):
                raise PublicationResourceError(
                    f"refusing to adopt Secret {expected_metadata['name']!r}: credential mismatch"
                )
        return

    if kind != "Job":
        raise PublicationResourceError(f"unknown publication resource kind {kind!r}")
    if _expected_projection(actual.get("spec"), expected["spec"]) != expected["spec"]:
        raise PublicationResourceError(
            f"refusing to adopt Job {expected_metadata['name']!r}: spec mismatch"
        )
    pod_spec = ((actual.get("spec") or {}).get("template") or {}).get("spec") or {}
    for container in pod_spec.get("containers") or []:
        # The expected-shape projection ignores keys the expected side never
        # set, so an omitted literal env "value" tolerates any actual key on
        # that item -- including a planted valueFrom that redirects the
        # container to read an attacker-chosen Secret/ConfigMap value. Refuse
        # any env entry outside name/value, and any envFrom, explicitly.
        for env_item in container.get("env") or []:
            if set(env_item) - {"name", "value"}:
                raise PublicationResourceError(
                    f"refusing to adopt Job {expected_metadata['name']!r}: spec mismatch"
                )
        if container.get("envFrom"):
            raise PublicationResourceError(
                f"refusing to adopt Job {expected_metadata['name']!r}: spec mismatch"
            )
    forbidden = {
        "hostNetwork": pod_spec.get("hostNetwork"),
        "hostPID": pod_spec.get("hostPID"),
        "hostIPC": pod_spec.get("hostIPC"),
        "shareProcessNamespace": pod_spec.get("shareProcessNamespace"),
        "initContainers": pod_spec.get("initContainers"),
        "ephemeralContainers": pod_spec.get("ephemeralContainers"),
    }
    if any(value not in (None, False, []) for value in forbidden.values()):
        raise PublicationResourceError(
            f"refusing to adopt Job {expected_metadata['name']!r}: pod security mismatch"
        )


def job_transport(job: Any) -> PublicationTransport | None:
    """The transport facts an existing Job was built with, from its env.

    Adoption rebuilds the expected resources from these and validates the
    whole Job against them, so a Job whose facts were altered fails the same
    contract check as any other mismatch. None when the Job names none.
    """

    serialized = _as_serialized_mapping(job)
    pod_spec = (((serialized.get("spec") or {}).get("template") or {}).get("spec")) or {}
    containers = pod_spec.get("containers") or []
    if len(containers) != 1:
        return None
    env = {
        item.get("name"): item.get("value")
        for item in containers[0].get("env") or []
        if isinstance(item, dict)
    }
    origin, header_form = env.get(_ORIGIN_ENV), env.get(_HEADER_FORM_ENV)
    ca_bundle_ref = env.get(_CA_BUNDLE_ENV)
    if not isinstance(origin, str) or header_form not in HEADER_FORMS:
        return None
    return PublicationTransport(
        origin=origin,
        header_form=cast(HeaderForm, header_form),
        ca_bundle_ref=ca_bundle_ref if isinstance(ca_bundle_ref, str) else None,
    )


class KubernetesPublicationCluster:
    """Create-or-adopt the deterministic resource set on a real apiserver."""

    def __init__(self, namespace: str, *, kubeconfig: str | None = None) -> None:
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config(config_file=kubeconfig)
        self.namespace = namespace
        self._core = k8s_client.CoreV1Api()
        self._batch = k8s_client.BatchV1Api()

    def owner_uid(self, owner_name: str) -> str:
        owner = self._core.read_namespaced_config_map(owner_name, self.namespace)
        if isinstance(owner, dict):
            uid = (owner.get("metadata") or {}).get("uid")
        else:
            uid = getattr(getattr(owner, "metadata", None), "uid", None)
        if not uid:
            raise PublicationResourceError(
                f"publication owner ConfigMap {owner_name!r} has no UID"
            )
        return str(uid)

    @staticmethod
    def _is_not_found(exc: k8s_client.ApiException) -> bool:
        return bool(exc.status == 404)

    def _read_existing(self, kind: str, obj: dict[str, Any]) -> Any | None:
        name = str(obj["metadata"]["name"])
        api = self._batch if kind == "Job" else self._core
        suffix = {"ConfigMap": "config_map", "Secret": "secret", "Job": "job"}[kind]
        try:
            return getattr(api, f"read_namespaced_{suffix}")(name, self.namespace)
        except k8s_client.ApiException as exc:
            if not self._is_not_found(exc):
                raise
            return None

    def _create(self, kind: str, obj: dict[str, Any]) -> None:
        api = self._batch if kind == "Job" else self._core
        suffix = {"ConfigMap": "config_map", "Secret": "secret", "Job": "job"}[kind]
        getattr(api, f"create_namespaced_{suffix}")(self.namespace, body=obj)

    def apply(self, resources: PublicationResources) -> None:
        # Replace the builder's deterministic test UID with the live owner UID
        # before anything reaches the apiserver. The owner object is Helm-owned,
        # so uninstall garbage-collects a worker crash's leftovers.
        owner_name = resources.config_map["metadata"]["ownerReferences"][0]["name"]
        live_uid = self.owner_uid(str(owner_name))
        objects = [
            copy.deepcopy(resources.config_map),
            copy.deepcopy(resources.secret),
            copy.deepcopy(resources.job),
        ]
        for obj in objects:
            obj["metadata"]["ownerReferences"][0]["uid"] = live_uid
        existing = {
            str(obj["kind"]): self._read_existing(str(obj["kind"]), obj) for obj in objects
        }
        # Validate every collision before making any mutation. This prevents a
        # matching label on a hostile or stale object from authorizing partial
        # adoption. Reads are exact-name GETs; publication RBAC needs no Secret
        # list permission.
        for obj in objects:
            kind = str(obj["kind"])
            if existing[kind] is not None:
                validate_adopted_resource(
                    kind,
                    obj,
                    existing[kind],
                    # A stale immutable Secret is replaced by UID below when
                    # no Job exists, so reading/comparing credential bytes is
                    # unnecessary in that recovery shape.
                    compare_secret_value=not (
                        kind == "Secret" and existing["Job"] is None
                    ),
                )

        config_map, secret, job = objects
        if existing["ConfigMap"] is None:
            self._create("ConfigMap", config_map)

        if existing["Secret"] is not None and existing["Job"] is None:
            # A finished Job may be removed by its TTL while its immutable
            # credential Secret survives a worker crash. Replace by UID before
            # starting another Job; never silently reuse unknown credential
            # bytes with a newly redeemed installation token.
            serialized_secret = _as_serialized_mapping(existing["Secret"])
            secret_uid = (serialized_secret.get("metadata") or {}).get("uid")
            if not secret_uid:
                raise PublicationResourceError(
                    f"refusing to replace Secret {resources.names.secret!r} without a stable UID"
                )
            self._core.delete_namespaced_secret(
                resources.names.secret,
                self.namespace,
                body={"preconditions": {"uid": secret_uid}},
            )
            existing["Secret"] = None
        if existing["Secret"] is None:
            self._create("Secret", secret)
        if existing["Job"] is None:
            self._create("Job", job)

    def validate_existing(self, resources: PublicationResources) -> None:
        """Validate an in-flight deterministic resource set without mutation."""

        owner_name = resources.config_map["metadata"]["ownerReferences"][0]["name"]
        live_uid = self.owner_uid(str(owner_name))
        objects = [
            copy.deepcopy(resources.config_map),
            copy.deepcopy(resources.secret),
            copy.deepcopy(resources.job),
        ]
        for obj in objects:
            obj["metadata"]["ownerReferences"][0]["uid"] = live_uid
        for obj in objects:
            kind = str(obj["kind"])
            observed = self._read_existing(kind, obj)
            if observed is None:
                raise PublicationResourceError(
                    f"in-flight publication is missing deterministic {kind}"
                )
            validate_adopted_resource(
                kind,
                obj,
                observed,
                compare_secret_value=kind != "Secret",
            )

    def observe(self, job_name: str) -> PublicationJobObservation:
        # Local import avoids a module import cycle: publication_loop owns the
        # neutral observation DTO while this module owns Kubernetes shapes.
        from .publication_loop import PublicationJobObservation

        try:
            job = self._batch.read_namespaced_job(job_name, self.namespace)
        except k8s_client.ApiException as exc:
            if self._is_not_found(exc):
                return PublicationJobObservation(
                    phase="pending",
                    commit_sha=None,
                    logs="",
                    error=None,
                    exists=False,
                )
            raise
        metadata = job.get("metadata") if isinstance(job, dict) else getattr(job, "metadata", None)
        job_uid = (
            metadata.get("uid")
            if isinstance(metadata, dict)
            else getattr(metadata, "uid", None)
        )
        if not job_uid:
            raise PublicationResourceError(
                f"publication Job {job_name!r} has no stable UID"
            )
        status = job.get("status") if isinstance(job, dict) else getattr(job, "status", None)

        def status_value(name: str, default: Any = None) -> Any:
            if isinstance(status, dict):
                return status.get(name, default)
            return getattr(status, name, default)

        phase = "running"
        error: str | None = None
        if status_value("succeeded", 0):
            phase = "succeeded"
        elif status_value("failed", 0):
            phase = "failed"
            conditions = status_value("conditions", []) or []
            seen_condition_texts: set[str] = set()
            condition_text_parts = []
            for condition in conditions:
                if _field(condition, "status") != "True":
                    continue
                text = ": ".join(
                    part
                    for part in (
                        str(_field(condition, "reason") or ""),
                        str(_field(condition, "message") or ""),
                    )
                    if part
                )
                if text and text not in seen_condition_texts:
                    seen_condition_texts.add(text)
                    condition_text_parts.append(text)
            condition_text = "; ".join(condition_text_parts)

        logs = ""
        selected: Any = None
        logs_read = False
        try:
            pods = self._core.list_namespaced_pod(
                self.namespace, label_selector=f"job-name={job_name}"
            )
            items = (
                pods.get("items")
                if isinstance(pods, dict)
                else getattr(pods, "items", None)
            ) or []
            owned = []
            for pod in items:
                pod_metadata = (
                    pod.get("metadata")
                    if isinstance(pod, dict)
                    else getattr(pod, "metadata", None)
                )
                references = (
                    pod_metadata.get("ownerReferences", [])
                    if isinstance(pod_metadata, dict)
                    else getattr(pod_metadata, "owner_references", None) or []
                )
                if any(
                    (
                        reference.get("kind") == "Job"
                        and str(reference.get("uid")) == str(job_uid)
                    )
                    if isinstance(reference, dict)
                    else (
                        getattr(reference, "kind", None) == "Job"
                        and str(getattr(reference, "uid", None)) == str(job_uid)
                    )
                    for reference in references
                ):
                    owned.append(pod)
            if owned:
                owned.sort(
                    key=lambda pod: str(
                        (pod.get("metadata") or {}).get("name")
                        if isinstance(pod, dict)
                        else getattr(getattr(pod, "metadata", None), "name", "")
                    )
                )
                selected = owned[0]
                selected_metadata = (
                    selected.get("metadata")
                    if isinstance(selected, dict)
                    else getattr(selected, "metadata", None)
                )
                pod_name = str(
                    selected_metadata.get("name")
                    if isinstance(selected_metadata, dict)
                    else getattr(selected_metadata, "name", "")
                )
                if not pod_name:
                    raise PublicationResourceError(
                        f"publication Job {job_name!r} owns a pod without a name"
                    )
                raw_log = self._core.read_namespaced_pod_log(
                    pod_name,
                    self.namespace,
                    tail_lines=200,
                    limit_bytes=65_536,
                    _preload_content=False,
                )
                data = getattr(raw_log, "data", raw_log)
                logs = (
                    data.decode("utf-8", errors="replace")
                    if isinstance(data, bytes)
                    else str(data)
                )
                logs = _redact(logs)
                logs_read = True
        except k8s_client.ApiException as exc:
            if exc.status not in (400, 404):
                raise
        if phase == "failed":
            parts = [condition_text] if condition_text else []
            statuses = _field(_field(selected, "status"), "containerStatuses", "container_statuses")
            for container in statuses or []:
                terminated = _field(_field(container, "state"), "terminated")
                if terminated is None:
                    continue
                exit_code = _field(terminated, "exitCode", "exit_code")
                reason = _field(terminated, "reason")
                parts.append(
                    "container exited"
                    + (f" ({reason})" if reason else "")
                    + (f" with exit code {exit_code}" if exit_code is not None else "")
                )
            if logs_read:
                # Keep the tail: git and shell failures print their cause last.
                meaningful = [
                    line.strip()
                    for line in logs.splitlines()
                    if line.strip() and not _MARKER_LOG_LINE.match(line.strip())
                ]
                if meaningful:
                    parts.append("; ".join(meaningful[-5:]))
            else:
                parts.append("pod logs were unavailable")
            error = _redact("; ".join(parts) or "publication Job failed")
            if len(error) > _MAX_JOB_ERROR:
                # Truncate after redaction; keep the head (reason) and the log tail.
                head = _MAX_JOB_ERROR // 3
                tail = _MAX_JOB_ERROR - head - len(" ... ")
                error = error[:head] + " ... " + error[-tail:]
        commit_match = re.search(
            r"^CURIE_COMMIT_SHA=([0-9a-f]{40,64})$", logs, re.MULTILINE
        )
        return PublicationJobObservation(
            phase=phase,
            commit_sha=commit_match.group(1) if commit_match else None,
            logs=logs,
            error=error,
            transport=job_transport(job),
        )

    def cleanup_credentials(self, names: PublicationResourceNames) -> None:
        try:
            self._core.delete_namespaced_secret(names.secret, self.namespace)
        except k8s_client.ApiException as exc:
            if not self._is_not_found(exc):
                raise

    def cleanup_terminal(self, names: PublicationResourceNames) -> None:
        operations: tuple[tuple[Any, str, dict[str, Any]], ...] = (
            (self._batch.delete_namespaced_job, names.job, {"propagation_policy": "Background"}),
            (self._core.delete_namespaced_config_map, names.config_map, {}),
        )
        for delete, name, kwargs in operations:
            try:
                delete(name, self.namespace, **kwargs)
            except k8s_client.ApiException as exc:
                if not self._is_not_found(exc):
                    raise
