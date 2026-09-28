"""Deterministic, secret-minimizing Kubernetes publication resources.

The patch is carried as ConfigMap ``binaryData`` and the short-lived GitHub
credential lives only in a Secret volume.  Neither value appears in Job argv,
environment, labels, or logs.  Names are publication-id-derived so a worker
restart adopts the same resource set instead of starting a second push.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

if TYPE_CHECKING:
    from .publication_loop import PublicationJobObservation

MAX_PATCH_BYTES = 900_000
_AUTHORIZATION_LOG = re.compile(
    r"Authorization:\s*(?:Basic|Bearer|token)\s+[^\s]+", re.IGNORECASE
)
_URL_USERINFO = re.compile(r"(https?://)[^/\s@]+@", re.IGNORECASE)
_MARKER_LOG_LINE = re.compile(r"^CURIE_[A-Z_]+=")
# Failed Job text leaves the cluster into thread history; keep it bounded.
_MAX_JOB_ERROR = 1500


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
    github_timeout_seconds: int = 30
    github_api_url: str = "https://api.github.com"


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
    pr_number: int | None
    pr_url: str | None
    title: str
    body: str
    observed_title_sha256: str | None
    observed_body_sha256: str | None
    github_repository_id: int | None
    github_pr_node_id: str | None
    open_as_draft: bool = False
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
  # for diagnostics returned by git or GitHub.
  auth_scheme='([Bb][Aa][Ss][Ii][Cc]|[Bb][Ee][Aa][Rr][Ee][Rr]|'
  auth_scheme+='[Tt][Oo][Kk][Ee][Nn])'
  sed -E -e 's#https://[^/@[:space:]]*@github\.com#https://github.com#g' \
      -e "s#Authorization:[[:space:]]*${auth_scheme}"\
"[[:space:]]+[^[:space:]]+#Authorization: [REDACTED]#gI"
}

git_with_timeout() {
  # Git must not forward an operator credential to a redirected origin. Keep
  # this invocation-scoped so no credential or transport setting is persisted
  # in the checkout configuration.
  timeout --signal=TERM "${GIT_TIMEOUT_SECONDS}s" git -c http.followRedirects=false "$@"
}

cleanup_auth() {
  rm -f /tmp/curie-git-user /tmp/curie-git-pass /tmp/curie-askpass \
    /tmp/curie-github.py /tmp/curie-pr-facts.json
}
trap cleanup_auth EXIT

credential_path="${CURIE_CREDENTIAL_PATH:-/credentials/credential}"
patch_path="${CURIE_PATCH_PATH:-/publication/changes.patch}"
work_dir="${CURIE_WORK_DIR:-/work}"
export CURIE_CREDENTIAL_PATH="$credential_path"

python - <<'PY'
import base64
import os
from pathlib import Path

value = Path(os.environ["CURIE_CREDENTIAL_PATH"]).read_text().strip()
user, password = "x-access-token", value
if value.lower().startswith("basic "):
    try:
        decoded = base64.b64decode(value.split(None, 1)[1]).decode()
        user, password = decoded.split(":", 1)
    except (ValueError, UnicodeError):
        raise SystemExit("publication credential is not valid Basic authorization")
elif value.lower().startswith(("bearer ", "token ")):
    password = value.split(None, 1)[1]
Path("/tmp/curie-git-user").write_text(user)
Path("/tmp/curie-git-pass").write_text(password)
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
if [[ -s "$patch_path" ]]; then
  git_with_timeout apply --check --binary "$patch_path"
  git_with_timeout apply --binary "$patch_path"
  git_with_timeout add --all
  if git_with_timeout diff --cached --quiet; then
    echo "publication patch produced no changes" >&2
    exit 1
  fi
  git_with_timeout -c user.name="$GIT_USER_NAME" -c user.email="$GIT_USER_EMAIL" commit \
    -m "$PR_TITLE" -m "Curie-Revision: $REVISION_ID"
  commit_sha=$(git_with_timeout rev-parse HEAD)
else
  if [[ -z "$PR_NUMBER" || -z "$PR_URL" ]]; then
    echo "metadata-only publication requires a stored pull request" >&2
    exit 1
  fi
  commit_sha="$BASE_SHA"
fi

cat >/tmp/curie-github.py <<'PY'
import base64
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

repo = os.environ["REPO_FULL_NAME"]
branch = os.environ["BRANCH"]
owner = repo.split("/", 1)[0]
github_api = os.environ["GITHUB_API_URL"].rstrip("/")
repo_api = f"{github_api}/repos/{repo}"
api = f"{repo_api}/pulls"
credential_path = Path(os.environ.get("CURIE_CREDENTIAL_PATH", "/credentials/credential"))
facts_path = Path(os.environ.get("CURIE_PR_FACTS_PATH", "/tmp/curie-pr-facts.json"))
phase = os.environ["CURIE_GITHUB_PHASE"]
if phase not in {"pre-push", "post-push", "metadata-only"}:
    raise SystemExit("invalid publication GitHub validation phase")
expected_head = os.environ.get("CURIE_EXPECTED_HEAD", "")
if len(expected_head) not in range(40, 65) or any(
    char not in "0123456789abcdef" for char in expected_head
):
    raise SystemExit("expected publication commit is invalid")
raw = credential_path.read_text().strip()
if raw.lower().startswith(("basic ", "bearer ", "token ")):
    authorization = raw
else:
    authorization = f"Bearer {raw}"
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": authorization,
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "curie-publication-job",
}
class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

opener = build_opener(_NoRedirect())

def request(method, url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(url, data=data, headers=headers, method=method)
    with opener.open(req, timeout=int(os.environ["GITHUB_TIMEOUT_SECONDS"])) as response:
        return json.load(response)

def validate_pull(
    row,
    expected_base,
    *,
    require_metadata,
    expected_number=None,
    expected_url=None,
    require_merged=False,
):
    if not isinstance(row, dict):
        raise SystemExit("GitHub returned an invalid pull request")
    head = row.get("head")
    base = row.get("base")
    expected = {
        "head_ref": branch,
        "head_repo": repo,
        "base_ref": expected_base,
        "base_repo": repo,
    }
    actual = {
        "head_ref": head.get("ref") if isinstance(head, dict) else None,
        "head_repo": (
            (head.get("repo") or {}).get("full_name")
            if isinstance(head, dict) and isinstance(head.get("repo"), dict)
            else None
        ),
        "head_sha": head.get("sha") if isinstance(head, dict) else None,
        "base_ref": base.get("ref") if isinstance(base, dict) else None,
        "base_repo": (
            (base.get("repo") or {}).get("full_name")
            if isinstance(base, dict) and isinstance(base.get("repo"), dict)
            else None
        ),
    }
    repo_fields = ("head_repo", "base_repo")
    if require_metadata and (
        row.get("title") != os.environ["PR_TITLE"]
        or row.get("body") != os.environ["PR_BODY"]
    ):
        raise SystemExit(
            "GitHub pull request does not match the approved publication contract"
        )
    if os.environ.get("PUBLICATION_OPEN_AS_DRAFT") == "true" and row.get("draft") is not True:
        raise SystemExit("GitHub pull request is not the required draft")
    if any(
        not isinstance(actual[field], str)
        or actual[field].casefold() != expected[field].casefold()
        for field in repo_fields
    ) or any(
        actual[field] != expected[field]
        for field in expected
        if field not in repo_fields
    ):
        raise SystemExit(
            "GitHub pull request does not match the approved publication contract"
        )
    if actual["head_sha"] != expected_head:
        raise SystemExit(
            "GitHub pull request head does not match the expected publication commit"
        )
    url = row.get("html_url")
    prefix = f"https://github.com/{repo}/pull/"
    if not isinstance(url, str) or not url.casefold().startswith(prefix.casefold()):
        raise SystemExit("GitHub did not return a usable pull request URL")
    url_number = url[len(prefix):]
    number = row.get("number")
    if (
        not url_number.isdigit()
        or isinstance(number, bool)
        or not isinstance(number, int)
        or number <= 0
        or number != int(url_number)
        or (expected_number is not None and number != expected_number)
        or (
            expected_url is not None
            and url.casefold() != expected_url.casefold()
        )
    ):
        raise SystemExit("GitHub did not return a usable pull request URL")
    state = row.get("state")
    merged = row.get("merged")
    merged_at = row.get("merged_at")
    if require_merged and not isinstance(merged, bool):
        raise SystemExit("GitHub returned an invalid pull request state")
    if merged is True or merged_at is not None:
        print(f"CURIE_PR_URL={url}")
        print(f"CURIE_PR_NUMBER={number}")
        print(f"CURIE_COMMIT_SHA={actual['head_sha']}")
        print("CURIE_PR_STATE=merged", flush=True)
        raise SystemExit("stored pull request is merged")
    if state != "open":
        if state == "closed":
            print(f"CURIE_PR_URL={url}")
            print(f"CURIE_PR_NUMBER={number}")
            print(f"CURIE_COMMIT_SHA={actual['head_sha']}")
            print("CURIE_PR_STATE=closed", flush=True)
            raise SystemExit("stored pull request is closed")
        raise SystemExit("GitHub returned an invalid pull request state")
    return url, number

def existing(expected_base):
    head=quote(f"{owner}:{branch}", safe="")
    rows = request("GET", f"{api}?state=all&head={head}")
    if not isinstance(rows, list):
        raise SystemExit("GitHub deterministic-head lookup returned an invalid response")
    if len(rows) > 1:
        raise SystemExit("GitHub deterministic-head lookup returned multiple pull requests")
    return validate_pull(rows[0], expected_base, require_metadata=True) if rows else None

pr_number = os.environ.get("PR_NUMBER")
if pr_number:
    try:
        expected_number = int(pr_number)
    except ValueError:
        raise SystemExit("stored pull request number is invalid") from None
    expected_url = os.environ.get("PR_URL")
    if not expected_url:
        raise SystemExit("stored pull request URL is missing")
    if phase in {"pre-push", "metadata-only"}:
        repository = request("GET", repo_api)
        default_base = repository.get("default_branch")
        if not isinstance(default_base, str) or not default_base:
            raise SystemExit("GitHub repository has no default branch")
        current_pull = request("GET", f"{api}/{pr_number}")
        url, number = validate_pull(
            current_pull,
            default_base,
            require_metadata=False,
            expected_number=expected_number,
            expected_url=expected_url,
            require_merged=True,
        )
        if phase == "metadata-only":
            expected_repository_id = int(os.environ["EXPECTED_GITHUB_REPOSITORY_ID"])
            expected_pr_node_id = os.environ["EXPECTED_GITHUB_PR_NODE_ID"]
            if repository.get("id") != expected_repository_id or (
                current_pull.get("node_id") != expected_pr_node_id
            ):
                raise SystemExit("stored pull request identity changed")
            current_body = current_pull.get("body")
            if current_body is not None and not isinstance(current_body, str):
                raise SystemExit("GitHub pull request body is invalid")
            current_body = current_body or ""
            expected_title = os.environ["OBSERVED_TITLE_SHA256"]
            expected_body = os.environ["OBSERVED_BODY_SHA256"]
            if any(
                len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
                for value in (expected_title, expected_body)
            ):
                raise SystemExit("observed pull request metadata is missing")
            if (
                hashlib.sha256(str(current_pull.get("title", "")).encode()).hexdigest()
                != expected_title
                or hashlib.sha256(current_body.encode()).hexdigest()
                != expected_body
            ):
                raise SystemExit("pull request metadata changed after publication approval")
            if (
                current_pull.get("title") == os.environ["PR_TITLE"]
                and current_body == os.environ["PR_BODY"]
            ):
                raise SystemExit("pull request metadata already matches the proposal")
            update = {}
            if current_pull.get("title") != os.environ["PR_TITLE"]:
                update["title"] = os.environ["PR_TITLE"]
            if current_body != os.environ["PR_BODY"]:
                update["body"] = os.environ["PR_BODY"]
            try:
                updated_pull = request(
                    "PATCH", f"{api}/{pr_number}", update,
                )
            except (HTTPError, URLError):
                raise SystemExit("GitHub pull request metadata update was not confirmed") from None
            validate_pull(
                updated_pull, default_base, require_metadata=True,
                expected_number=expected_number, expected_url=expected_url,
                require_merged=True,
            )
            if updated_pull.get("node_id") != expected_pr_node_id:
                raise SystemExit("stored pull request identity changed")
            updated_at = updated_pull.get("updated_at")
            try:
                timestamp = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            except (AttributeError, ValueError):
                raise SystemExit("GitHub pull request update time is invalid") from None
            if timestamp.tzinfo is None:
                raise SystemExit("GitHub pull request update time is invalid")
            print(f"CURIE_PR_UPDATED_AT={timestamp.astimezone(UTC).isoformat()}")
            print(f"CURIE_PR_URL={url}")
            print(f"CURIE_PR_NUMBER={number}")
            raise SystemExit(0)
        facts_path.write_text(json.dumps({"base": default_base, "url": url, "number": number}))
        raise SystemExit(0)
    try:
        facts = json.loads(facts_path.read_text())
    except (OSError, ValueError):
        raise SystemExit("stored pull request validation facts are missing") from None
    if not isinstance(facts, dict):
        raise SystemExit("stored pull request validation facts are invalid")
    default_base = facts.get("base")
    if (
        not isinstance(default_base, str)
        or facts.get("url") != expected_url
        or facts.get("number") != expected_number
    ):
        raise SystemExit("stored pull request validation facts are invalid")
    url, number = validate_pull(
        request("GET", f"{api}/{pr_number}"),
        default_base,
        require_metadata=False,
        expected_number=expected_number,
        expected_url=expected_url,
        require_merged=True,
    )
else:
    if phase != "post-push":
        raise SystemExit("new pull request cannot be validated before push")
    # Query the deterministic head before POST, and again after an ambiguous
    # REST failure. This is the idempotency boundary for a lost response.
    repository = request("GET", repo_api)
    default_base = repository.get("default_branch")
    if not isinstance(default_base, str) or not default_base:
        raise SystemExit("GitHub repository has no default branch")
    pull = existing(default_base)
    if not pull:
        try:
            pull_body = {
                "title": os.environ["PR_TITLE"],
                "head": branch,
                "base": default_base,
                "body": os.environ["PR_BODY"],
            }
            if os.environ.get("PUBLICATION_OPEN_AS_DRAFT") == "true":
                pull_body["draft"] = True
            created = request("POST", api, pull_body)
            pull = validate_pull(created, default_base, require_metadata=True)
        except (HTTPError, URLError):
            pull = existing(default_base)
            if not pull:
                raise
    url, number = pull
print(f"CURIE_PR_URL={url}")
print(f"CURIE_PR_NUMBER={number}")
PY

if [[ ! -s "$patch_path" ]]; then
  CURIE_EXPECTED_HEAD="$BASE_SHA" \
    CURIE_GITHUB_PHASE=metadata-only python /tmp/curie-github.py
  echo "CURIE_COMMIT_SHA=$BASE_SHA"
  exit 0
fi
if [[ -n "$PR_NUMBER" ]]; then
  CURIE_EXPECTED_HEAD="$EXPECTED_PRIOR_HEAD" \
    CURIE_GITHUB_PHASE=pre-push python /tmp/curie-github.py
fi
git_with_timeout push \
  --force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD \
  origin "HEAD:refs/heads/$BRANCH" 2> >(redact >&2)
CURIE_EXPECTED_HEAD="$commit_sha" \
  CURIE_GITHUB_PHASE=post-push python /tmp/curie-github.py
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
    if not payload.patch and (payload.pr_number is None or payload.pr_url is None):
        raise PublicationResourceError(
            "metadata-only publication requires a stored pull request"
        )
    if not payload.patch and (
        payload.observed_title_sha256 is None or payload.observed_body_sha256 is None
    ):
        raise PublicationResourceError("metadata-only publication requires observed metadata")
    if not payload.patch and (
        isinstance(payload.github_repository_id, bool)
        or not isinstance(payload.github_repository_id, int)
        or payload.github_repository_id <= 0
        or not isinstance(payload.github_pr_node_id, str)
        or not payload.github_pr_node_id.strip()
    ):
        raise PublicationResourceError("metadata-only publication requires stored GitHub identity")
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
    if (payload.pr_number is None) != (payload.pr_url is None):
        raise PublicationResourceError("publication pull request identity is incomplete")
    if payload.pr_number is not None and payload.pr_number <= 0:
        raise PublicationResourceError("publication pull request number must be positive")
    if payload.clean_clone_url.casefold() != (
        f"https://github.com/{payload.repo_full_name}.git".casefold()
    ):
        raise PublicationResourceError(
            "publication clone URL does not match the requested repository"
        )
    if not credential.strip():
        raise PublicationResourceError("publication credential is empty")
    if min(
        settings.active_deadline_seconds,
        settings.git_timeout_seconds,
        settings.github_timeout_seconds,
    ) <= 0:
        raise PublicationResourceError("publication timeouts must be positive")
    if not settings.github_api_url.startswith("https://"):
        raise PublicationResourceError("publication GitHub API URL must use HTTPS")
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
        "base_sha": payload.base_sha,
        "expected_prior_head": payload.expected_prior_head,
        "expected_remote_head": payload.expected_remote_head,
        "patch_sha256": hashlib.sha256(payload.patch).hexdigest(),
        "branch": payload.branch,
        "pr_number": payload.pr_number,
        "pr_url": payload.pr_url,
        "title": payload.title,
        "body": payload.body,
        "observed_title_sha256": payload.observed_title_sha256,
        "observed_body_sha256": payload.observed_body_sha256,
        "github_repository_id": payload.github_repository_id,
        "github_pr_node_id": payload.github_pr_node_id,
        "open_as_draft": payload.open_as_draft,
        "branch_prefix": payload.branch_prefix,
        "runner_image": settings.runner_image,
        "service_account_name": settings.service_account_name,
        "active_deadline_seconds": settings.active_deadline_seconds,
        "git_timeout_seconds": settings.git_timeout_seconds,
        "github_timeout_seconds": settings.github_timeout_seconds,
        "github_api_url": settings.github_api_url,
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
        {"name": "BASE_SHA", "value": payload.base_sha},
        {"name": "BRANCH", "value": payload.branch},
        {"name": "REVISION_ID", "value": str(payload.revision_id)},
        {"name": "REVISION_NUMBER", "value": str(payload.revision_number)},
        {"name": "EXPECTED_PRIOR_HEAD", "value": payload.expected_prior_head},
        {"name": "EXPECTED_REMOTE_HEAD", "value": payload.expected_remote_head or ""},
        {"name": "PR_NUMBER", "value": str(payload.pr_number or "")},
        {"name": "PR_URL", "value": payload.pr_url or ""},
        {"name": "PR_TITLE", "value": payload.title},
        {"name": "PR_BODY", "value": payload.body},
        {"name": "OBSERVED_TITLE_SHA256", "value": payload.observed_title_sha256 or ""},
        {"name": "OBSERVED_BODY_SHA256", "value": payload.observed_body_sha256 or ""},
        {
            "name": "EXPECTED_GITHUB_REPOSITORY_ID",
            "value": str(payload.github_repository_id or ""),
        },
        {"name": "EXPECTED_GITHUB_PR_NODE_ID", "value": payload.github_pr_node_id or ""},
        {
            "name": "PUBLICATION_OPEN_AS_DRAFT",
            "value": "true" if payload.open_as_draft else "false",
        },
        {"name": "PUBLICATION_BRANCH_PREFIX", "value": payload.branch_prefix or ""},
        {"name": "GIT_USER_NAME", "value": settings.git_user_name},
        {"name": "GIT_USER_EMAIL", "value": settings.git_user_email},
        {"name": "GIT_TIMEOUT_SECONDS", "value": str(settings.git_timeout_seconds)},
        {"name": "GITHUB_TIMEOUT_SECONDS", "value": str(settings.github_timeout_seconds)},
        {"name": "GITHUB_API_URL", "value": settings.github_api_url},
    ]
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
                "volumeMounts": [
                    {"name": "publication", "mountPath": "/publication", "readOnly": True},
                    {"name": "credentials", "mountPath": "/credentials", "readOnly": True},
                    {"name": "work", "mountPath": "/work"},
                    {"name": "tmp", "mountPath": "/tmp"},
                ],
            }
        ],
        "volumes": [
            {"name": "publication", "configMap": {"name": names.config_map}},
            {"name": "credentials", "secret": {"secretName": names.secret}},
            {"name": "work", "emptyDir": {"sizeLimit": "2Gi"}},
            {"name": "tmp", "emptyDir": {"sizeLimit": "16Mi"}},
        ],
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
                    pr_url=None,
                    pr_number=None,
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
        match = re.search(
            r"^CURIE_PR_URL=(https://github\.com/[^\s]+/pull/\d+)$",
            logs,
            re.MULTILINE,
        )
        number_match = re.search(r"^CURIE_PR_NUMBER=([1-9][0-9]*)$", logs, re.MULTILINE)
        commit_match = re.search(
            r"^CURIE_COMMIT_SHA=([0-9a-f]{40,64})$", logs, re.MULTILINE
        )
        state_match = re.search(
            r"^CURIE_PR_STATE=(closed|merged)$", logs, re.MULTILINE
        )
        return PublicationJobObservation(
            phase=phase,
            pr_url=match.group(1) if match else None,
            pr_number=int(number_match.group(1)) if number_match else None,
            commit_sha=commit_match.group(1) if commit_match else None,
            pr_state=(
                cast(Literal["closed", "merged"], state_match.group(1))
                if state_match
                else None
            ),
            logs=logs,
            error=error,
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
