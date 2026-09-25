"""Secret-free deterministic Kubernetes publication resources."""

from __future__ import annotations

import base64
import importlib
import json
import os
import shutil
import subprocess
import threading
import uuid
from copy import deepcopy
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

import pytest
from kubernetes.client import ApiException

PUBLICATION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
WRITE_CREDENTIAL = "publication-write-credential-value"
CLEAN_URL = "https://github.com/acme-corp/acme-bot.git"
PR_API_URL = "https://api.github.com/repos/acme-corp/acme-bot/pulls"
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
LINEAGE_BRANCH = "curie/thread-lineage-example"
PRIOR_HEAD = "b" * 40
REVISION_HEAD = "d" * 40


@pytest.fixture
def publication_k8s() -> Any:
    return importlib.import_module("curie_worker.publication_k8s")


def _settings(module: Any) -> Any:
    return module.PublicationJobSettings(
        namespace="curie",
        runner_image="ghcr.io/curie-eng/curie-runner:v0.7.0",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=("registry-creds",),
        priority_class_name="curie-platform-critical",
        service_account_name="curie-publication",
        owner_name="curie-publication-owner",
        git_user_name="Curie Publisher",
        git_user_email="publisher@example.com",
        cpu_request="100m",
        cpu_limit="1",
        memory_request="256Mi",
        memory_limit="1Gi",
        ephemeral_request="1Gi",
        ephemeral_limit="4Gi",
    )


def _payload(module: Any, patch: bytes = b"diff --git a/a b/a\n") -> Any:
    return module.PublicationPayload(
        publication_id=PUBLICATION_ID,
        revision_id=REVISION_ID,
        revision_number=1,
        repo_full_name="acme-corp/acme-bot",
        clean_clone_url=CLEAN_URL,
        base_sha="a" * 40,
        expected_prior_head="a" * 40,
        expected_remote_head=None,
        patch=patch,
        branch=LINEAGE_BRANCH,
        pr_number=None,
        pr_url=None,
        title="Update repository",
        body="Approved platform publication.",
    )


def _resources(module: Any, patch: bytes = b"diff --git a/a b/a\n") -> Any:
    return module.build_publication_resources(
        _payload(module, patch),
        credential=WRITE_CREDENTIAL,
        settings=_settings(module),
    )


def _job_env(resources: Any) -> dict[str, str]:
    container = resources.job["spec"]["template"]["spec"]["containers"][0]
    return {item["name"]: item["value"] for item in container["env"]}


def _lineage_resources(module: Any) -> Any:
    payload = module.PublicationPayload(
        publication_id=PUBLICATION_ID,
        revision_id=REVISION_ID,
        revision_number=2,
        repo_full_name="acme-corp/acme-bot",
        clean_clone_url=CLEAN_URL,
        base_sha=PRIOR_HEAD,
        expected_prior_head=PRIOR_HEAD,
        expected_remote_head=PRIOR_HEAD,
        patch=b"diff --git a/a b/a\n",
        branch=LINEAGE_BRANCH,
        pr_number=123,
        pr_url="https://github.com/acme-corp/acme-bot/pull/123",
        title="Update repository",
        body="Approved platform publication.",
    )
    return module.build_publication_resources(
        payload,
        credential=WRITE_CREDENTIAL,
        settings=_settings(module),
    )


def test_branch_prefix_mismatch_exits_before_any_git_or_github_call(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    payload = replace(
        _payload(publication_k8s),
        branch="factory/publication-abc",
        branch_prefix="factory/",
    )
    resources = publication_k8s.build_publication_resources(
        payload,
        credential=WRITE_CREDENTIAL,
        settings=_settings(publication_k8s),
    )
    script = tmp_path / "publish.sh"
    script.write_text(resources.config_map["data"]["publish.sh"])
    completed = subprocess.run(
        ["bash", str(script)],
        env={**os.environ, **_job_env(resources), "BRANCH": "curie/publication-abc"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 1
    assert "required prefix" in completed.stderr


def test_a_cleared_prefix_still_accepts_the_stored_platform_branch(
    publication_k8s: Any,
) -> None:
    payload = replace(
        _payload(publication_k8s),
        branch="factory/publication-abc",
        branch_prefix=None,
    )
    resources = publication_k8s.build_publication_resources(
        payload,
        credential=WRITE_CREDENTIAL,
        settings=_settings(publication_k8s),
    )
    assert _job_env(resources)["BRANCH"] == "factory/publication-abc"
    assert _job_env(resources)["PUBLICATION_BRANCH_PREFIX"] == ""

    unsafe = replace(_payload(publication_k8s), branch="release/v1", branch_prefix=None)
    with pytest.raises(publication_k8s.PublicationResourceError, match="lineage branch"):
        publication_k8s.build_publication_resources(
            unsafe,
            credential=WRITE_CREDENTIAL,
            settings=_settings(publication_k8s),
        )


def test_draft_publication_refuses_a_non_draft_pull_request(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    payload = replace(_payload(publication_k8s), open_as_draft=True)
    resources = publication_k8s.build_publication_resources(
        payload,
        credential=WRITE_CREDENTIAL,
        settings=_settings(publication_k8s),
    )
    created = _pull_response()
    created["draft"] = False
    refused, _ = _run_github_guard(
        tmp_path,
        resources,
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, []),
            (201, {}, created),
        ],
    )
    assert refused.returncode != 0
    assert "required draft" in refused.stderr

    accepted = _pull_response()
    accepted["draft"] = True
    opened, requests = _run_github_guard(
        tmp_path,
        resources,
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, []),
            (201, {}, accepted),
        ],
    )
    assert opened.returncode == 0, opened.stderr
    assert any(path.endswith("/pulls") for path, _auth in requests)


def test_900000_raw_patch_bytes_fit_binary_data_and_900001_is_refused(
    publication_k8s: Any,
) -> None:
    patch = b"x" * 900_000
    resources = _resources(publication_k8s, patch)
    encoded = resources.config_map["binaryData"]["changes.patch"]

    assert base64.b64decode(encoded) == patch
    assert len(base64.b64decode(encoded)) == 900_000
    with pytest.raises(publication_k8s.PublicationResourceError, match="900000"):
        _resources(publication_k8s, b"x" * 900_001)


@pytest.mark.parametrize(
    "base_sha",
    ["abc", "A" * 40, "g" * 40, "a" * 65, "a" * 39, "a" * 40 + ";touch /tmp/x"],
)
def test_publication_base_sha_is_revalidated_before_entering_job_argv(
    publication_k8s: Any,
    base_sha: str,
) -> None:
    payload = _payload(publication_k8s)
    invalid = publication_k8s.PublicationPayload(
        **{**payload.__dict__, "base_sha": base_sha}
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="base SHA"):
        publication_k8s.build_publication_resources(
            invalid,
            credential=WRITE_CREDENTIAL,
            settings=_settings(publication_k8s),
        )


def test_publication_resource_names_and_stored_lineage_branch_are_deterministic(
    publication_k8s: Any,
) -> None:
    first = _resources(publication_k8s)
    second = _resources(publication_k8s)

    assert first.names == second.names
    assert first.job == second.job
    assert first.config_map == second.config_map
    assert first.secret["metadata"]["name"] == first.names.secret
    assert _job_env(first)["BRANCH"] == LINEAGE_BRANCH


def test_built_job_is_bounded_secret_free_and_outside_sandbox_selectors(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)
    job = resources.job
    pod = job["spec"]["template"]
    pod_spec = pod["spec"]
    container = pod_spec["containers"][0]
    serialized_public = json.dumps(
        {"job": job, "config_map": resources.config_map}, sort_keys=True
    )

    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 300
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["serviceAccountName"] == "curie-publication"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["priorityClassName"] == "curie-platform-critical"
    assert pod_spec["imagePullSecrets"] == [{"name": "registry-creds"}]
    assert container["image"] == "ghcr.io/curie-eng/curie-runner:v0.7.0"
    assert container["command"] == ["/bin/bash", "/publication/publish.sh"]
    env_by_name = {item["name"]: item["value"] for item in container["env"]}
    assert {
        ("GIT_TIMEOUT_SECONDS", "60"),
        ("GITHUB_TIMEOUT_SECONDS", "30"),
        ("GITHUB_API_URL", "https://api.github.com"),
    } <= set(env_by_name.items())
    assert container["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "1Gi"},
        "limits": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "4Gi"},
    }
    assert pod["metadata"]["labels"].get("curietech.ai/component") == "publication"
    assert pod["metadata"]["labels"].get("sandbox.agent-sandbox.io/runner") is None
    assert WRITE_CREDENTIAL not in serialized_public
    assert "secretKeyRef" not in json.dumps(container.get("env", []))
    assert resources.secret["stringData"]["credential"] == WRITE_CREDENTIAL


def test_publish_script_uses_clean_remote_file_askpass_rest_and_redacted_marker(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)
    script = resources.config_map["data"]["publish.sh"]
    serialized_job = json.dumps(resources.job)

    assert "GIT_ASKPASS" in script
    assert "/credentials/credential" in script
    assert "git_with_timeout clone \"$CLEAN_CLONE_URL\"" in script
    assert "git_with_timeout remote set-url origin \"$CLEAN_CLONE_URL\"" in script
    assert (
        "git_with_timeout -c user.name=\"$GIT_USER_NAME\" "
        "-c user.email=\"$GIT_USER_EMAIL\" commit" in script
    )
    assert "git_with_timeout apply --check" in script
    assert "git_with_timeout push" in script
    assert (
        'timeout --signal=TERM "${GIT_TIMEOUT_SECONDS}s" '
        'git -c http.followRedirects=false "$@"' in script
    )
    assert 'timeout=int(os.environ["GITHUB_TIMEOUT_SECONDS"])' in script
    assert PR_API_URL not in script, "repository-specific URLs must be derived at runtime"
    assert 'github_api = os.environ["GITHUB_API_URL"].rstrip("/")' in script
    assert 'repo_api = f"{github_api}/repos/{repo}"' in script
    assert "https://api.github.com" not in script
    assert "gh " not in script
    assert "CURIE_PR_URL=" in script
    assert "redact" in script.lower()
    assert "set -x" not in script
    assert WRITE_CREDENTIAL not in script
    assert WRITE_CREDENTIAL not in serialized_job


def test_lineage_revision_job_marks_one_commit_and_uses_exact_head_occupancy_cas(
    publication_k8s: Any,
) -> None:
    """Revision two may advance only the stored head of the stable lineage branch."""

    resources = _lineage_resources(publication_k8s)
    script = resources.config_map["data"]["publish.sh"]
    container = resources.job["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item["value"] for item in container["env"]}

    assert env["BRANCH"] == LINEAGE_BRANCH
    assert env["EXPECTED_PRIOR_HEAD"] == PRIOR_HEAD
    assert env["EXPECTED_REMOTE_HEAD"] == PRIOR_HEAD
    assert env["REVISION_ID"] == str(REVISION_ID)
    assert env["PR_NUMBER"] == "123"
    assert env["PR_URL"] == "https://github.com/acme-corp/acme-bot/pull/123"
    assert "Curie-Revision: $REVISION_ID" in script
    assert (
        '--force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD' in script
    )
    assert "CURIE_COMMIT_SHA=" in script
    assert "CURIE_PR_NUMBER=" in script
    assert "git push --force " not in script
    assert f"publication-{PUBLICATION_ID.hex}" not in LINEAGE_BRANCH
    preflight = script.index("CURIE_GITHUB_PHASE=pre-push python")
    push = script.index("git_with_timeout push")
    postflight = script.index("CURIE_GITHUB_PHASE=post-push python")
    success = script.index('echo "CURIE_COMMIT_SHA=$commit_sha"')
    assert preflight < push < postflight < success


def test_lineage_job_refuses_every_remote_head_except_prior_or_own_marked_commit(
    publication_k8s: Any,
) -> None:
    script = _lineage_resources(publication_k8s).config_map["data"]["publish.sh"]

    assert "ls-remote" in script
    assert "EXPECTED_REMOTE_HEAD" in script
    assert "REVISION_ID" in script
    assert "Curie-Revision:" in script
    assert "publication branch head conflict" in script
    assert "force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD" in script


def test_lineage_revision_payload_rejects_checkout_and_expected_head_disagreement(
    publication_k8s: Any,
) -> None:
    payload = publication_k8s.PublicationPayload(
        publication_id=PUBLICATION_ID,
        revision_id=REVISION_ID,
        revision_number=2,
        repo_full_name="acme-corp/acme-bot",
        clean_clone_url=CLEAN_URL,
        base_sha="c" * 40,
        expected_prior_head=PRIOR_HEAD,
        expected_remote_head=PRIOR_HEAD,
        patch=b"diff --git a/a b/a\n",
        branch=LINEAGE_BRANCH,
        pr_number=123,
        pr_url="https://github.com/acme-corp/acme-bot/pull/123",
        title="Update repository",
        body="Approved platform publication.",
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="expected prior head"):
        publication_k8s.build_publication_resources(
            payload,
            credential=WRITE_CREDENTIAL,
            settings=_settings(publication_k8s),
        )


def test_publish_job_refuses_redirects_for_git_and_github_rest(
    publication_k8s: Any,
) -> None:
    """Neither credential-bearing transport may follow an attacker-controlled redirect."""

    script = _resources(publication_k8s).config_map["data"]["publish.sh"]

    assert 'git -c http.followRedirects=false "$@"' in script
    assert "from urllib.request import HTTPRedirectHandler, Request, build_opener" in script
    assert "class _NoRedirect(HTTPRedirectHandler):" in script
    assert "opener = build_opener(_NoRedirect())" in script
    assert "opener.open(req, timeout=int(os.environ[\"GITHUB_TIMEOUT_SECONDS\"]))" in script
    assert "urlopen(req" not in script


def _pull_response(
    *,
    state: str = "open",
    merged: bool = False,
    number: int = 123,
    url: str = "https://github.com/acme-corp/acme-bot/pull/123",
    head: str = LINEAGE_BRANCH,
    head_sha: str | None = None,
    base: str = "main",
) -> dict[str, Any]:
    head_payload: dict[str, Any] = {
        "ref": head,
        "repo": {"full_name": "acme-corp/acme-bot"},
    }
    if head_sha is not None:
        head_payload["sha"] = head_sha
    return {
        "number": number,
        "html_url": url,
        "state": state,
        "merged": merged,
        "title": "Update repository",
        "body": "Approved platform publication.",
        "head": head_payload,
        "base": {"ref": base, "repo": {"full_name": "acme-corp/acme-bot"}},
    }


def _terminal_markers(state: str, head_sha: str) -> str:
    return (
        "CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123\n"
        "CURIE_PR_NUMBER=123\n"
        f"CURIE_COMMIT_SHA={head_sha}\n"
        f"CURIE_PR_STATE={state}\n"
    )


class _GitHubApiHandler(BaseHTTPRequestHandler):
    queued_responses: list[tuple[int, dict[str, str], Any]] = []
    requests: list[tuple[str, str | None]] = []
    post_count = 0

    def _respond(self) -> None:
        type(self).requests.append((self.path, self.headers.get("Authorization")))
        status, headers, payload = type(self).queued_responses.pop(0)
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        if payload is not None:
            body = json.dumps(payload).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
        else:
            body = b""
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self._respond()

    def do_POST(self) -> None:
        type(self).post_count += 1
        content_length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(content_length)
        self._respond()

    def log_message(self, _format: str, *args: object) -> None:
        return


def _embedded_github_script(resources: Any) -> str:
    script = resources.config_map["data"]["publish.sh"]
    marker = "cat >/tmp/curie-github.py <<'PY'\n"
    return cast(str, script.split(marker, 1)[1].split("\nPY\n", 1)[0])


def _run_github_guard(
    tmp_path: Path,
    resources: Any,
    *,
    mode: str,
    responses: list[tuple[int, dict[str, str], Any]],
    expected_head: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[tuple[str, str | None]]]:
    credential = tmp_path / "credential"
    credential.write_text(WRITE_CREDENTIAL)
    facts = tmp_path / "pr-facts.json"
    expected_head = expected_head or (
        PRIOR_HEAD if mode == "pre-push" else REVISION_HEAD
    )
    prepared = deepcopy(responses)
    for _status, _headers, payload in prepared:
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("head"), dict):
                row["head"].setdefault("sha", expected_head)
    _GitHubApiHandler.queued_responses = prepared
    _GitHubApiHandler.requests = []
    _GitHubApiHandler.post_count = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _GitHubApiHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        completed = subprocess.run(
            ["python", "-c", _embedded_github_script(resources)],
            env={
                **os.environ,
                **_job_env(resources),
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "CURIE_GITHUB_PHASE": mode,
                "CURIE_EXPECTED_HEAD": expected_head,
                "CURIE_CREDENTIAL_PATH": str(credential),
                "CURIE_PR_FACTS_PATH": str(facts),
            },
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    return completed, list(_GitHubApiHandler.requests)


@pytest.mark.parametrize(
    ("state", "merged", "marker"),
    [("closed", False, "closed"), ("closed", True, "merged")],
)
def test_revision_guard_refuses_closed_or_merged_pull_before_push(
    publication_k8s: Any,
    tmp_path: Path,
    state: str,
    merged: bool,
    marker: str,
) -> None:
    resources = _lineage_resources(publication_k8s)
    completed, requests = _run_github_guard(
        tmp_path,
        resources,
        mode="pre-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response(state=state, merged=merged)),
        ],
    )

    assert completed.returncode != 0
    assert completed.stdout == _terminal_markers(marker, PRIOR_HEAD)
    assert requests == [
        ("/repos/acme-corp/acme-bot", f"Bearer {WRITE_CREDENTIAL}"),
        ("/repos/acme-corp/acme-bot/pulls/123", f"Bearer {WRITE_CREDENTIAL}"),
    ]


@pytest.mark.parametrize(
    ("state", "merged", "marker"),
    [("closed", False, "closed"), ("closed", True, "merged")],
)
def test_revision_guard_refuses_closed_or_merged_pull_after_push_without_success(
    publication_k8s: Any,
    tmp_path: Path,
    state: str,
    merged: bool,
    marker: str,
) -> None:
    resources = _lineage_resources(publication_k8s)
    before, _ = _run_github_guard(
        tmp_path,
        resources,
        mode="pre-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response()),
        ],
    )
    assert before.returncode == 0

    after, requests = _run_github_guard(
        tmp_path,
        resources,
        mode="post-push",
        responses=[(200, {}, _pull_response(state=state, merged=merged))],
    )

    assert after.returncode != 0
    assert after.stdout == _terminal_markers(marker, REVISION_HEAD)
    assert requests == [
        ("/repos/acme-corp/acme-bot/pulls/123", f"Bearer {WRITE_CREDENTIAL}")
    ]


@pytest.mark.parametrize("mode", ["pre-push", "post-push"])
@pytest.mark.parametrize(
    "mismatch",
    [
        {"number": 124},
        {"url": "https://github.com/acme-corp/acme-bot/pull/124"},
        {"head": "curie/different-lineage"},
        {"base": "different-base"},
    ],
)
def test_revision_guard_refuses_pull_identity_mismatch_at_both_boundaries(
    publication_k8s: Any,
    tmp_path: Path,
    mode: str,
    mismatch: dict[str, Any],
) -> None:
    resources = _lineage_resources(publication_k8s)
    if mode == "post-push":
        healthy, _ = _run_github_guard(
            tmp_path,
            resources,
            mode="pre-push",
            responses=[
                (200, {}, {"default_branch": "main"}),
                (200, {}, _pull_response()),
            ],
        )
        assert healthy.returncode == 0
        responses: list[tuple[int, dict[str, str], Any]] = [
            (200, {}, _pull_response(**mismatch))
        ]
    else:
        responses = [
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response(**mismatch)),
        ]

    completed, _ = _run_github_guard(
        tmp_path, resources, mode=mode, responses=responses
    )

    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "CURIE_PR_URL=" not in completed.stdout


def test_revision_guard_does_not_follow_or_forward_auth_on_redirect(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    resources = _lineage_resources(publication_k8s)
    target_requests: list[str | None] = []

    class TargetHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            target_requests.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.end_headers()

        def log_message(self, _format: str, *args: object) -> None:
            return

    target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)
    target_thread = threading.Thread(target=target.serve_forever)
    target_thread.start()
    try:
        completed, requests = _run_github_guard(
            tmp_path,
            resources,
            mode="pre-push",
            responses=[
                (200, {}, {"default_branch": "main"}),
                (
                    302,
                    {"Location": f"http://127.0.0.1:{target.server_port}/capture"},
                    None,
                ),
            ],
        )
    finally:
        target.shutdown()
        target_thread.join()
        target.server_close()

    assert completed.returncode != 0
    assert target_requests == []
    assert requests[-1][0] == "/repos/acme-corp/acme-bot/pulls/123"


def test_revision_guard_revalidates_healthy_stable_pull_before_and_after_push(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    resources = _lineage_resources(publication_k8s)
    before, _ = _run_github_guard(
        tmp_path,
        resources,
        mode="pre-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response()),
        ],
    )
    assert before.returncode == 0
    assert before.stdout == ""

    after, requests = _run_github_guard(
        tmp_path,
        resources,
        mode="post-push",
        responses=[(200, {}, _pull_response())],
    )

    assert after.returncode == 0
    assert after.stdout == (
        "CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123\n"
        "CURIE_PR_NUMBER=123\n"
    )
    assert requests == [
        ("/repos/acme-corp/acme-bot/pulls/123", f"Bearer {WRITE_CREDENTIAL}")
    ]


def test_post_push_guard_rejects_concurrent_branch_replacement_before_markers(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    resources = _lineage_resources(publication_k8s)
    before, _ = _run_github_guard(
        tmp_path,
        resources,
        mode="pre-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response()),
        ],
    )
    assert before.returncode == 0

    completed, requests = _run_github_guard(
        tmp_path,
        resources,
        mode="post-push",
        responses=[(200, {}, _pull_response(head_sha="c" * 40))],
        expected_head=REVISION_HEAD,
    )

    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "expected publication commit" in completed.stderr
    assert requests == [
        ("/repos/acme-corp/acme-bot/pulls/123", f"Bearer {WRITE_CREDENTIAL}")
    ]


def test_embedded_github_client_executes_healthy_two_revision_path(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    first_revision = _resources(publication_k8s)
    first, first_requests = _run_github_guard(
        tmp_path,
        first_revision,
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, []),
            (201, {}, _pull_response()),
        ],
    )
    assert first.returncode == 0
    assert first.stdout == (
        "CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123\n"
        "CURIE_PR_NUMBER=123\n"
    )
    assert [path.split("?", 1)[0] for path, _auth in first_requests] == [
        "/repos/acme-corp/acme-bot",
        "/repos/acme-corp/acme-bot/pulls",
        "/repos/acme-corp/acme-bot/pulls",
    ]

    second_revision = _lineage_resources(publication_k8s)
    before, _ = _run_github_guard(
        tmp_path,
        second_revision,
        mode="pre-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, _pull_response()),
        ],
    )
    after, _ = _run_github_guard(
        tmp_path,
        second_revision,
        mode="post-push",
        responses=[(200, {}, _pull_response())],
    )

    assert before.returncode == 0
    assert after.returncode == 0
    assert after.stdout.endswith("CURIE_PR_NUMBER=123\n")


def test_first_revision_recovers_pull_after_ambiguous_create_failure(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    completed, requests = _run_github_guard(
        tmp_path,
        _resources(publication_k8s),
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, []),
            (500, {}, {"message": "ambiguous failure"}),
            (200, {}, [_pull_response()]),
        ],
    )

    assert completed.returncode == 0
    assert completed.stdout.endswith("CURIE_PR_NUMBER=123\n")
    assert len(requests) == 4


@pytest.mark.parametrize(
    ("state", "merged", "marker"),
    [("closed", False, "closed"), ("closed", True, "merged")],
)
def test_first_revision_recognizes_terminal_pull_without_posting(
    publication_k8s: Any,
    tmp_path: Path,
    state: str,
    merged: bool,
    marker: str,
) -> None:
    completed, requests = _run_github_guard(
        tmp_path,
        _resources(publication_k8s),
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, [_pull_response(state=state, merged=merged)]),
        ],
    )

    assert completed.returncode != 0
    assert completed.stdout == _terminal_markers(marker, REVISION_HEAD)
    assert [path.split("?", 1)[0] for path, _auth in requests] == [
        "/repos/acme-corp/acme-bot",
        "/repos/acme-corp/acme-bot/pulls",
    ]
    assert "state=all" in requests[-1][0]


def test_first_revision_lost_create_response_recognizes_terminal_without_repost(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    completed, requests = _run_github_guard(
        tmp_path,
        _resources(publication_k8s),
        mode="post-push",
        responses=[
            (200, {}, {"default_branch": "main"}),
            (200, {}, []),
            (500, {}, {"message": "response lost after create"}),
            (200, {}, [_pull_response(state="closed")]),
        ],
    )

    assert completed.returncode != 0
    assert completed.stdout == _terminal_markers("closed", REVISION_HEAD)
    assert [method_path.split("?", 1)[0] for method_path, _auth in requests].count(
        "/repos/acme-corp/acme-bot/pulls"
    ) == 3
    assert _GitHubApiHandler.post_count == 1


class _LocalGitHubState:
    def __init__(self, *, git: str, remote: Path) -> None:
        self.git = git
        self.remote = remote
        self.pr_number: int | None = None
        self.requests: list[tuple[str, str, dict[str, list[str]]]] = []
        self.post_payloads: list[dict[str, Any]] = []

    def branch_head(self) -> str:
        return subprocess.run(
            [
                self.git,
                "--git-dir",
                str(self.remote),
                "rev-parse",
                f"refs/heads/{LINEAGE_BRANCH}",
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()

    def pull(self) -> dict[str, Any]:
        assert self.pr_number is not None
        return _pull_response(
            number=self.pr_number,
            url=f"https://github.com/acme-corp/acme-bot/pull/{self.pr_number}",
            head_sha=self.branch_head(),
        )


def _local_github_handler(state: _LocalGitHubState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            state.requests.append(("GET", parsed.path, query))
            if parsed.path == "/repos/acme-corp/acme-bot":
                self._send(200, {"default_branch": "main"})
                return
            if parsed.path == "/repos/acme-corp/acme-bot/pulls":
                self._send(200, [] if state.pr_number is None else [state.pull()])
                return
            if parsed.path == "/repos/acme-corp/acme-bot/pulls/123":
                self._send(200, state.pull())
                return
            self._send(404, {"message": "not found"})

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            state.requests.append(("POST", parsed.path, {}))
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
            assert isinstance(payload, dict)
            state.post_payloads.append(payload)
            if parsed.path != "/repos/acme-corp/acme-bot/pulls":
                self._send(404, {"message": "not found"})
                return
            if state.pr_number is not None:
                self._send(422, {"message": "pull request already exists"})
                return
            state.pr_number = 123
            self._send(201, state.pull())

        def log_message(self, _format: str, *args: object) -> None:
            return

    return Handler


def _run_generated_publish_script(
    tmp_path: Path,
    resources: Any,
    *,
    remote: Path,
    api_url: str,
    ordinal: str,
    path_prefix: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    root = tmp_path / f"run-{ordinal}"
    root.mkdir()
    credential = root / "credential"
    credential.write_text("Bearer local-test-token")
    patch = root / "changes.patch"
    patch.write_bytes(
        base64.b64decode(resources.config_map["binaryData"]["changes.patch"])
    )
    script = root / "publish.sh"
    script.write_text(resources.config_map["data"]["publish.sh"])
    remote_url = remote.resolve().as_uri()
    env = {
        **os.environ,
        **_job_env(resources),
        "GITHUB_API_URL": api_url,
        "CURIE_CREDENTIAL_PATH": str(credential),
        "CURIE_PATCH_PATH": str(patch),
        "CURIE_WORK_DIR": str(root / "work"),
        "GIT_ALLOW_PROTOCOL": "file",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": f"url.{remote_url}.insteadOf",
        "GIT_CONFIG_VALUE_0": CLEAN_URL,
    }
    if path_prefix is not None:
        env["PATH"] = f"{path_prefix}{os.pathsep}{env['PATH']}"
    if extra_env is not None:
        env.update(extra_env)
    return subprocess.run(
        ["/bin/bash", str(script)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _publication_resources_for_commit(
    module: Any,
    *,
    publication_id: uuid.UUID,
    revision_id: uuid.UUID,
    revision_number: int,
    base_sha: str,
    expected_remote_head: str | None,
    pr_number: int | None,
    patch: bytes,
) -> Any:
    return module.build_publication_resources(
        module.PublicationPayload(
            publication_id=publication_id,
            revision_id=revision_id,
            revision_number=revision_number,
            repo_full_name="acme-corp/acme-bot",
            clean_clone_url=CLEAN_URL,
            base_sha=base_sha,
            expected_prior_head=base_sha,
            expected_remote_head=expected_remote_head,
            patch=patch,
            branch=LINEAGE_BRANCH,
            pr_number=pr_number,
            pr_url=(
                "https://github.com/acme-corp/acme-bot/pull/123"
                if pr_number is not None
                else None
            ),
            title="Update repository",
            body="Approved platform publication.",
        ),
        credential="Bearer local-test-token",
        settings=_settings(module),
    )


def test_generated_publish_script_keeps_one_pr_lineage_and_refuses_lease_race(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    git = shutil.which("git")
    assert git is not None
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    subprocess.run([git, "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run([git, "init", "-b", "main", str(seed)], check=True, capture_output=True)
    subprocess.run([git, "-C", str(seed), "config", "user.name", "Test Publisher"], check=True)
    subprocess.run(
        [git, "-C", str(seed), "config", "user.email", "publisher@example.com"],
        check=True,
    )
    (seed / "README.md").write_text("base\n")
    subprocess.run([git, "-C", str(seed), "add", "README.md"], check=True)
    subprocess.run([git, "-C", str(seed), "commit", "-m", "Base"], check=True)
    base_sha = subprocess.run(
        [git, "-C", str(seed), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        [git, "-C", str(seed), "remote", "add", "origin", remote.resolve().as_uri()],
        check=True,
    )
    subprocess.run([git, "-C", str(seed), "push", "origin", "main"], check=True)
    subprocess.run(
        [git, "--git-dir", str(remote), "symbolic-ref", "HEAD", "refs/heads/main"],
        check=True,
    )

    state = _LocalGitHubState(git=git, remote=remote)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _local_github_handler(state))
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    api_url = f"http://127.0.0.1:{server.server_port}"
    try:
        first_resources = _publication_resources_for_commit(
            publication_k8s,
            publication_id=PUBLICATION_ID,
            revision_id=REVISION_ID,
            revision_number=1,
            base_sha=base_sha,
            expected_remote_head=None,
            pr_number=None,
            patch=(
                b"diff --git a/README.md b/README.md\n"
                b"--- a/README.md\n+++ b/README.md\n"
                b"@@ -1 +1,2 @@\n base\n+first\n"
            ),
        )
        first = _run_generated_publish_script(
            tmp_path,
            first_resources,
            remote=remote,
            api_url=api_url,
            ordinal="one",
        )
        assert first.returncode == 0, first.stderr
        first_head = state.branch_head()
        assert f"CURIE_COMMIT_SHA={first_head}" in first.stdout
        assert "CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123" in first.stdout
        assert "CURIE_PR_NUMBER=123" in first.stdout
        first_parents = subprocess.run(
            [git, "--git-dir", str(remote), "rev-list", "--parents", "-n", "1", first_head],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.split()
        assert first_parents == [first_head, base_sha]

        second_revision = uuid.UUID("77777777-7777-4777-8777-777777777777")
        second_resources = _publication_resources_for_commit(
            publication_k8s,
            publication_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
            revision_id=second_revision,
            revision_number=2,
            base_sha=first_head,
            expected_remote_head=first_head,
            pr_number=123,
            patch=(
                b"diff --git a/README.md b/README.md\n"
                b"--- a/README.md\n+++ b/README.md\n"
                b"@@ -1,2 +1,3 @@\n base\n first\n+second\n"
            ),
        )
        second = _run_generated_publish_script(
            tmp_path,
            second_resources,
            remote=remote,
            api_url=api_url,
            ordinal="two",
        )
        assert second.returncode == 0, second.stderr
        second_head = state.branch_head()
        assert f"CURIE_COMMIT_SHA={second_head}" in second.stdout
        assert "CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123" in second.stdout
        assert "CURIE_PR_NUMBER=123" in second.stdout
        second_parents = subprocess.run(
            [git, "--git-dir", str(remote), "rev-list", "--parents", "-n", "1", second_head],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.split()
        assert second_parents == [second_head, first_head]
        commit_count = subprocess.run(
            [
                git,
                "--git-dir",
                str(remote),
                "rev-list",
                "--count",
                f"{base_sha}..{second_head}",
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        assert commit_count == "2"
        assert len(state.post_payloads) == 1
        assert state.post_payloads[0]["head"] == LINEAGE_BRANCH
        assert all(
            query.get("state") == ["all"]
            for method, path, query in state.requests
            if method == "GET" and path.endswith("/pulls")
        )

        contender = tmp_path / "contender"
        subprocess.run(
            [git, "clone", remote.resolve().as_uri(), str(contender)],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [git, "-C", str(contender), "fetch", "origin", LINEAGE_BRANCH],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [git, "-C", str(contender), "checkout", "--detach", second_head],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [git, "-C", str(contender), "config", "user.name", "Race Writer"],
            check=True,
        )
        subprocess.run(
            [git, "-C", str(contender), "config", "user.email", "race@example.com"],
            check=True,
        )
        (contender / "race.txt").write_text("replacement\n")
        subprocess.run([git, "-C", str(contender), "add", "race.txt"], check=True)
        subprocess.run([git, "-C", str(contender), "commit", "-m", "Race"], check=True)
        replacement_head = subprocess.run(
            [git, "-C", str(contender), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        subprocess.run(
            [
                git,
                "-C",
                str(contender),
                "push",
                "origin",
                f"{replacement_head}:refs/curie-test/race-candidate",
            ],
            check=True,
            capture_output=True,
        )

        wrapper_dir = tmp_path / "git-wrapper"
        wrapper_dir.mkdir()
        wrapper = wrapper_dir / "git"
        wrapper.write_text(
            """#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

args = sys.argv[1:]
real_git = os.environ["CURIE_TEST_REAL_GIT"]
marker = Path(os.environ["CURIE_TEST_RACE_MARKER"])
if "push" in args and not marker.exists():
    marker.touch()
    subprocess.run(
        [
            real_git,
            "--git-dir",
            os.environ["CURIE_TEST_REMOTE"],
            "update-ref",
            os.environ["CURIE_TEST_RACE_REF"],
            os.environ["CURIE_TEST_RACE_SHA"],
        ],
        check=True,
    )
os.execv(real_git, [real_git, *args])
"""
        )
        wrapper.chmod(0o700)
        third_resources = _publication_resources_for_commit(
            publication_k8s,
            publication_id=uuid.UUID("88888888-8888-4888-8888-888888888888"),
            revision_id=uuid.UUID("99999999-9999-4999-8999-999999999999"),
            revision_number=3,
            base_sha=second_head,
            expected_remote_head=second_head,
            pr_number=123,
            patch=(
                b"diff --git a/README.md b/README.md\n"
                b"--- a/README.md\n+++ b/README.md\n"
                b"@@ -1,3 +1,4 @@\n base\n first\n second\n+third\n"
            ),
        )
        raced = _run_generated_publish_script(
            tmp_path,
            third_resources,
            remote=remote,
            api_url=api_url,
            ordinal="race",
            path_prefix=wrapper_dir,
            extra_env={
                "CURIE_TEST_REAL_GIT": git,
                "CURIE_TEST_REMOTE": str(remote),
                "CURIE_TEST_RACE_REF": f"refs/heads/{LINEAGE_BRANCH}",
                "CURIE_TEST_RACE_SHA": replacement_head,
                "CURIE_TEST_RACE_MARKER": str(tmp_path / "race-fired"),
            },
        )
        assert raced.returncode != 0
        assert "CURIE_PR_URL=" not in raced.stdout
        assert "CURIE_PR_NUMBER=" not in raced.stdout
        assert "CURIE_COMMIT_SHA=" not in raced.stdout
        assert (tmp_path / "race-fired").is_file()
        assert state.branch_head() == replacement_head
        assert len(state.post_payloads) == 1
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_job_injects_a_non_default_github_api_base_without_baking_it_into_script(
    publication_k8s: Any,
) -> None:
    api_base = "https://github.example.com/api/v3"
    resources = publication_k8s.build_publication_resources(
        _payload(publication_k8s),
        credential=WRITE_CREDENTIAL,
        settings=replace(_settings(publication_k8s), github_api_url=api_base),
    )
    container = resources.job["spec"]["template"]["spec"]["containers"][0]
    env_by_name = {item["name"]: item["value"] for item in container["env"]}
    script = resources.config_map["data"]["publish.sh"]

    assert env_by_name["GITHUB_API_URL"] == api_base
    assert api_base not in script
    assert WRITE_CREDENTIAL not in script
    assert WRITE_CREDENTIAL not in json.dumps(resources.job)


def _assert_script_redacts_authorization(script: str) -> None:
    redact_function = script[
        script.index("redact() {") : script.index("\n}\n\ngit_with_timeout") + 2
    ]
    sensitive = (
        "Authorization: Basic dXNlcjp3cml0ZS10b2tlbg==\n"
        "authorization: Bearer github_pat_sensitive\n"
    )
    completed = subprocess.run(
        ["/bin/bash", "-c", f"{redact_function}\nredact"],
        input=sensitive,
        text=True,
        capture_output=True,
        check=True,
    )
    assert completed.stdout == "Authorization: [REDACTED]\nAuthorization: [REDACTED]\n"
    assert "dXNlc" not in completed.stdout
    assert "github_pat_sensitive" not in completed.stdout


def test_publish_script_executes_redaction_and_the_assertion_catches_a_mutation(
    publication_k8s: Any,
) -> None:
    script = _resources(publication_k8s).config_map["data"]["publish.sh"]

    subprocess.run(["/bin/bash", "-n"], input=script, text=True, check=True)
    _assert_script_redacts_authorization(script)
    mutation = script.replace("[Bb][Ee][Aa][Rr][Ee][Rr]", "[Xx][Ee][Aa][Rr][Ee][Rr]")
    with pytest.raises(AssertionError):
        _assert_script_redacts_authorization(mutation)


def test_absent_pull_job_queries_stored_lineage_head_before_posting_once(
    publication_k8s: Any,
) -> None:
    script = _resources(publication_k8s).config_map["data"]["publish.sh"]
    lookup = script.index("head=")
    post = script.index("POST")

    assert lookup < post
    assert "CURIE_PR_URL=" in script
    assert "urllib" in script or "http.client" in script


def test_every_dynamic_resource_has_the_helm_owner_reference(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)
    for obj in (resources.config_map, resources.secret, resources.job):
        refs = obj["metadata"]["ownerReferences"]
        assert refs == [
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "name": "curie-publication-owner",
                "uid": resources.owner_uid,
                "controller": False,
                "blockOwnerDeletion": False,
            }
        ]


class _FakeCoreApi:
    def __init__(self, owner_name: str) -> None:
        self.config_maps: dict[str, dict[str, Any]] = {
            owner_name: {"metadata": {"name": owner_name, "uid": "live-owner-uid"}}
        }
        self.secrets: dict[str, dict[str, Any]] = {}
        self.created: list[tuple[str, str]] = []
        self.secret_deletes: list[tuple[str, dict[str, Any]]] = []

    def _read(self, rows: dict[str, dict[str, Any]], name: str) -> dict[str, Any]:
        if name not in rows:
            raise ApiException(status=404)
        return deepcopy(rows[name])

    def read_namespaced_config_map(self, name: str, namespace: str) -> dict[str, Any]:
        return self._read(self.config_maps, name)

    def read_namespaced_secret(self, name: str, namespace: str) -> dict[str, Any]:
        return self._read(self.secrets, name)

    def create_namespaced_config_map(self, namespace: str, body: dict[str, Any]) -> None:
        value = deepcopy(body)
        value["metadata"]["uid"] = f"uid-{body['metadata']['name']}"
        self.config_maps[body["metadata"]["name"]] = value
        self.created.append(("ConfigMap", body["metadata"]["name"]))

    def create_namespaced_secret(self, namespace: str, body: dict[str, Any]) -> None:
        value = deepcopy(body)
        value["metadata"]["uid"] = f"uid-{body['metadata']['name']}"
        value["data"] = {
            key: base64.b64encode(raw.encode()).decode()
            for key, raw in value.pop("stringData").items()
        }
        self.secrets[body["metadata"]["name"]] = value
        self.created.append(("Secret", body["metadata"]["name"]))

    def delete_namespaced_secret(
        self, name: str, namespace: str, *, body: dict[str, Any]
    ) -> None:
        self.secret_deletes.append((name, deepcopy(body)))
        del self.secrets[name]


class _FakeBatchApi:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.created: list[str] = []

    def read_namespaced_job(self, name: str, namespace: str) -> dict[str, Any]:
        if name not in self.jobs:
            raise ApiException(status=404)
        return deepcopy(self.jobs[name])

    def create_namespaced_job(self, namespace: str, body: dict[str, Any]) -> None:
        value = deepcopy(body)
        value["metadata"]["uid"] = f"uid-{body['metadata']['name']}"
        self.jobs[body["metadata"]["name"]] = value
        self.created.append(body["metadata"]["name"])


def _fake_cluster(module: Any) -> tuple[Any, _FakeCoreApi, _FakeBatchApi]:
    cluster = object.__new__(module.KubernetesPublicationCluster)
    cluster.namespace = "curie"
    core = _FakeCoreApi("curie-publication-owner")
    batch = _FakeBatchApi()
    cluster._core = core
    cluster._batch = batch
    return cluster, core, batch


def test_create_or_adopt_validates_immutable_spec_and_live_owner_before_mutating(
    publication_k8s: Any,
) -> None:
    cluster, core, batch = _fake_cluster(publication_k8s)
    resources = _resources(publication_k8s)
    cluster.apply(resources)
    first_creates = (list(core.created), list(batch.created))

    cluster.apply(resources)
    assert (core.created, batch.created) == first_creates

    batch.jobs[resources.names.job]["spec"]["backoffLimit"] = 1
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        cluster.apply(resources)
    assert (core.created, batch.created) == first_creates

    batch.jobs[resources.names.job]["spec"]["backoffLimit"] = 0
    original_credential = core.secrets[resources.names.secret]["data"]["credential"]
    core.secrets[resources.names.secret]["data"]["credential"] = base64.b64encode(
        b"collision-credential"
    ).decode()
    with pytest.raises(publication_k8s.PublicationResourceError, match="credential mismatch"):
        cluster.apply(resources)
    core.secrets[resources.names.secret]["data"]["credential"] = original_credential

    core.config_maps[resources.names.config_map]["metadata"]["ownerReferences"][0][
        "uid"
    ] = "different-owner-uid"
    with pytest.raises(publication_k8s.PublicationResourceError, match="metadata contract"):
        cluster.apply(resources)


def test_stale_immutable_secret_is_uid_replaced_only_when_job_is_gone(
    publication_k8s: Any,
) -> None:
    cluster, core, batch = _fake_cluster(publication_k8s)
    resources = _resources(publication_k8s)
    cluster.apply(resources)
    old_uid = core.secrets[resources.names.secret]["metadata"]["uid"]
    del batch.jobs[resources.names.job]
    rotated = publication_k8s.build_publication_resources(
        _payload(publication_k8s),
        credential="rotated-publication-write-credential",
        settings=_settings(publication_k8s),
    )

    cluster.apply(rotated)

    assert core.secret_deletes == [
        (resources.names.secret, {"preconditions": {"uid": old_uid}})
    ]
    assert batch.created == [resources.names.job, resources.names.job]
    assert base64.b64decode(
        core.secrets[resources.names.secret]["data"]["credential"]
    ).decode() == "rotated-publication-write-credential"


def test_observe_reads_logs_only_from_a_pod_owned_by_the_exact_job(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-123"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: SimpleNamespace(
            metadata=SimpleNamespace(uid=job_uid),
            status=SimpleNamespace(succeeded=1, failed=0, conditions=[]),
        )
    )
    log_reads: list[str] = []
    hostile = SimpleNamespace(
        metadata=SimpleNamespace(
            name="hostile-pod",
            owner_references=[SimpleNamespace(kind="Job", uid="different-job")],
        )
    )
    owned = SimpleNamespace(
        metadata=SimpleNamespace(
            name="owned-pod",
            owner_references=[SimpleNamespace(kind="Job", uid=job_uid)],
        )
    )

    def read_log(name: str, *_args: object, **_kwargs: object) -> str:
        log_reads.append(name)
        if name == "hostile-pod":
            return "CURIE_PR_URL=https://github.com/attacker/repo/pull/1"
        return _terminal_markers("closed", REVISION_HEAD)

    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: SimpleNamespace(
            items=[hostile, owned]
        ),
        read_namespaced_pod_log=read_log,
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert log_reads == ["owned-pod"]
    assert observed.pr_url == "https://github.com/acme-corp/acme-bot/pull/123"
    assert observed.pr_number == 123
    assert observed.commit_sha == REVISION_HEAD
    assert observed.pr_state == "closed"


def test_observe_reads_terminal_status_from_dict_shaped_kubernetes_objects(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: {
            "metadata": {"uid": "job-uid-123"},
            "status": {
                "succeeded": 0,
                "failed": 1,
                "conditions": [
                    {"status": "True", "reason": "DeadlineExceeded"}
                ],
            },
        }
    )
    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: {"items": []}
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.phase == "failed"
    assert observed.error == "DeadlineExceeded; pod logs were unavailable"


def _real_client_pod_log(raw: bytes) -> Any:
    """Mimic kubernetes-python 36.0.3's read_namespaced_pod_log shape.

    Without ``_preload_content=False`` the generated client decodes the
    urllib3 response body with ``str(bytes_body)``, i.e. the Python repr of
    the raw bytes. With ``_preload_content=False`` it hands back the raw
    HTTPResponse-like object whose ``.data`` is the actual bytes.
    """

    def read_log(
        _name: str,
        _namespace: str,
        *_args: object,
        _preload_content: bool = True,
        **_kwargs: object,
    ) -> Any:
        if _preload_content is False:
            return SimpleNamespace(data=raw)
        return repr(raw)

    return read_log


def _owned_pod(name: str, job_uid: str) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            owner_references=[SimpleNamespace(kind="Job", uid=job_uid)],
        )
    )


def test_observe_parses_pr_markers_from_the_real_clients_pod_log_shape(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-success"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: SimpleNamespace(
            metadata=SimpleNamespace(uid=job_uid),
            status=SimpleNamespace(succeeded=1, failed=0, conditions=[]),
        )
    )
    raw = (
        b"Cloning into 'repo'...\n"
        b"CURIE_PR_URL=https://github.com/acme-corp/acme-bot/pull/123\n"
        b"CURIE_PR_NUMBER=123\n"
        + f"CURIE_COMMIT_SHA={REVISION_HEAD}\n".encode()
    )
    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: SimpleNamespace(
            items=[_owned_pod("owned-pod", job_uid)]
        ),
        read_namespaced_pod_log=_real_client_pod_log(raw),
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.pr_url == "https://github.com/acme-corp/acme-bot/pull/123"
    assert observed.pr_number == 123
    assert observed.commit_sha == REVISION_HEAD


def test_observe_failed_job_error_is_not_a_bytes_repr_on_the_real_client_shape(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-failed"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: SimpleNamespace(
            metadata=SimpleNamespace(uid=job_uid),
            status=SimpleNamespace(
                succeeded=0,
                failed=1,
                conditions=[
                    SimpleNamespace(
                        type="FailureTarget",
                        status="True",
                        reason="BackoffLimitExceeded",
                        message="Job has reached the specified backoff limit",
                    ),
                    SimpleNamespace(
                        type="Failed",
                        status="True",
                        reason="BackoffLimitExceeded",
                        message="Job has reached the specified backoff limit",
                    ),
                ],
            ),
        )
    )
    raw = (
        b"Cloning into 'repo'...\n"
        b"fatal: Authentication failed for 'https://github.com/o/r.git/'\n"
    )
    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: SimpleNamespace(
            items=[_owned_pod("owned-pod", job_uid)]
        ),
        read_namespaced_pod_log=_real_client_pod_log(raw),
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.error is not None
    assert "fatal: Authentication failed" in observed.error
    assert 'b"' not in observed.error
    assert "b'" not in observed.error
    assert "\\n" not in observed.error
    assert observed.error.count("BackoffLimitExceeded") == 1


def test_resource_builder_binds_clone_url_to_publication_repository(
    publication_k8s: Any,
) -> None:
    payload = _payload(publication_k8s)
    payload = publication_k8s.PublicationPayload(
        **{
            **payload.__dict__,
            "clean_clone_url": "https://github.com/other-corp/other-repo.git",
        }
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="clone URL"):
        publication_k8s.build_publication_resources(
            payload,
            credential="publication-write-credential",
            settings=_settings(publication_k8s),
        )


def test_publication_cluster_has_no_legacy_combined_cleanup_shim(
    publication_k8s: Any,
) -> None:
    assert not hasattr(publication_k8s.KubernetesPublicationCluster, "cleanup")


def _omitempty(value: Any) -> Any:
    """Serialize like the apiserver: drop empty strings, lists and maps."""

    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            projected = _omitempty(item)
            if projected in ("", [], {}, None):
                continue
            result[key] = projected
        return result
    if isinstance(value, list):
        return [_omitempty(item) for item in value]
    return value


def _apiserver_minimal_resources(module: Any) -> Any:
    return module.build_publication_resources(
        _payload(module),
        credential=WRITE_CREDENTIAL,
        settings=replace(
            _settings(module), priority_class_name="", image_pull_secrets=()
        ),
    )


def test_apiserver_omitempty_serialized_own_job_is_adopted(
    publication_k8s: Any,
) -> None:
    resources = _apiserver_minimal_resources(publication_k8s)
    observed = _omitempty(deepcopy(resources.job))
    pod_spec = observed["spec"]["template"]["spec"]
    assert "priorityClassName" not in pod_spec
    assert "imagePullSecrets" not in pod_spec
    env = pod_spec["containers"][0]["env"]
    assert any("value" not in item for item in env)

    publication_k8s.validate_adopted_resource("Job", resources.job, observed)
    publication_k8s.validate_adopted_resource(
        "ConfigMap",
        resources.config_map,
        _omitempty(deepcopy(resources.config_map)),
    )


def test_omitempty_tolerance_still_refuses_non_empty_where_empty_expected(
    publication_k8s: Any,
) -> None:
    resources = _apiserver_minimal_resources(publication_k8s)

    planted_env = _omitempty(deepcopy(resources.job))
    for item in planted_env["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "PR_URL":
            item["value"] = "https://evil.example/pull/1"
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted_env)

    planted_priority = _omitempty(deepcopy(resources.job))
    planted_priority["spec"]["template"]["spec"][
        "priorityClassName"
    ] = "system-node-critical"
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted_priority)

    planted_pull = _omitempty(deepcopy(resources.job))
    planted_pull["spec"]["template"]["spec"]["imagePullSecrets"] = [{"name": "x"}]
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted_pull)


def test_omitempty_tolerance_refuses_differing_or_emptied_non_empty_values(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)

    differing = deepcopy(resources.job)
    differing["spec"]["template"]["spec"]["priorityClassName"] = "other-class"
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, differing)

    emptied = deepcopy(resources.job)
    emptied["spec"]["template"]["spec"]["imagePullSecrets"] = []
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, emptied)

    absent = deepcopy(resources.job)
    del absent["spec"]["template"]["spec"]["imagePullSecrets"]
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, absent)

    backoff_absent = deepcopy(resources.job)
    del backoff_absent["spec"]["backoffLimit"]
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, backoff_absent)


def test_omitempty_tolerance_refuses_valueFrom_secretKeyRef_on_omitted_env(
    publication_k8s: Any,
) -> None:
    resources = _apiserver_minimal_resources(publication_k8s)
    secret_name = resources.secret["metadata"]["name"]

    planted = _omitempty(deepcopy(resources.job))
    container = planted["spec"]["template"]["spec"]["containers"][0]
    env = container["env"]
    empty_items = [item for item in env if "value" not in item]
    assert empty_items, "expected at least one omitted-value env entry to attack"
    empty_items[0]["valueFrom"] = {
        "secretKeyRef": {"name": secret_name, "key": "credential"}
    }

    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted)


def test_omitempty_tolerance_refuses_valueFrom_configMapKeyRef_on_omitted_env(
    publication_k8s: Any,
) -> None:
    resources = _apiserver_minimal_resources(publication_k8s)

    planted = _omitempty(deepcopy(resources.job))
    container = planted["spec"]["template"]["spec"]["containers"][0]
    env = container["env"]
    empty_items = [item for item in env if "value" not in item]
    assert empty_items, "expected at least one omitted-value env entry to attack"
    empty_items[0]["valueFrom"] = {
        "configMapKeyRef": {"name": "attacker-configmap", "key": "credential"}
    }

    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted)


def test_omitempty_tolerance_refuses_added_envFrom(
    publication_k8s: Any,
) -> None:
    resources = _apiserver_minimal_resources(publication_k8s)
    secret_name = resources.secret["metadata"]["name"]

    planted = _omitempty(deepcopy(resources.job))
    container = planted["spec"]["template"]["spec"]["containers"][0]
    container["envFrom"] = [{"secretRef": {"name": secret_name}}]

    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted)


def _failed_job_cluster(
    module: Any, *, pods: list[Any], logs: str, reason: str = "BackoffLimitExceeded"
) -> Any:
    cluster = object.__new__(module.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: {
            "metadata": {"uid": "job-uid-123"},
            "status": {
                "succeeded": 0,
                "failed": 1,
                "conditions": [
                    {
                        "type": "Failed",
                        "status": "True",
                        "reason": reason,
                        "message": "Job has reached the specified backoff limit",
                    }
                ],
            },
        }
    )
    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: {"items": pods},
        read_namespaced_pod_log=lambda *_args, **_kwargs: logs,
    )
    return cluster


def _failed_owned_pod(exit_code: int = 128) -> dict[str, Any]:
    return {
        "metadata": {
            "name": "owned-pod",
            "ownerReferences": [{"kind": "Job", "uid": "job-uid-123"}],
        },
        "status": {
            "containerStatuses": [
                {
                    "name": "publish",
                    "state": {
                        "terminated": {"reason": "Error", "exitCode": exit_code}
                    },
                }
            ]
        },
    }


def test_observe_failed_job_names_reason_exit_code_and_redacted_git_error(
    publication_k8s: Any,
) -> None:
    logs = "\n".join(
        [
            "CURIE_PUBLICATION_STAGE=clone",
            "Cloning into 'repo'...",
            "Authorization: Bearer SECRETTOKEN",
            "fatal: repository 'https://x-access-token:SECRETTOKEN@github.com/o/r.git/'"
            " not found",
            "CURIE_PUBLICATION_STAGE=failed",
        ]
    )
    cluster = _failed_job_cluster(
        publication_k8s, pods=[_failed_owned_pod(128)], logs=logs
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.phase == "failed"
    error = observed.error or ""
    assert "BackoffLimitExceeded" in error
    assert "exit code 128" in error
    assert "fatal: repository 'https://" in error
    assert "github.com/o/r.git/' not found" in error
    assert "CURIE_PUBLICATION_STAGE" not in error
    assert "SECRETTOKEN" not in error
    assert "SECRETTOKEN" not in observed.logs
    assert len(error) <= 1500


def test_observe_failed_job_error_is_bounded_for_huge_logs(
    publication_k8s: Any,
) -> None:
    logs = "\n".join(f"fatal: line {index} " + "x" * 200 for index in range(60))
    cluster = _failed_job_cluster(
        publication_k8s, pods=[_failed_owned_pod(1)], logs=logs
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    error = observed.error or ""
    assert "BackoffLimitExceeded" in error
    assert "fatal: line 59" in error
    assert len(error) <= 1500


def test_observe_failed_job_without_pod_says_logs_were_unavailable(
    publication_k8s: Any,
) -> None:
    cluster = _failed_job_cluster(
        publication_k8s, pods=[], logs="", reason="DeadlineExceeded"
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    error = observed.error or ""
    assert "DeadlineExceeded" in error
    assert "pod logs were unavailable" in error
