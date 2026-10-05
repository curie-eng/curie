"""Secret-free deterministic Kubernetes publication resources."""

from __future__ import annotations

import base64
import importlib
import json
import os
import re
import shutil
import subprocess
import uuid
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from kubernetes.client import (
    ApiClient,
    ApiException,
    V1Job,
    V1JobCondition,
    V1JobStatus,
    V1ObjectMeta,
)

PUBLICATION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
WRITE_CREDENTIAL = "publication-write-credential-value"
ORIGIN = "https://github.com"
CLEAN_URL = f"{ORIGIN}/acme-corp/acme-bot.git"
GITLAB_ORIGIN = "https://gitlab.example.com/forge"
GITLAB_REPO = "group/sub/acme-bot"
GITLAB_URL = f"{GITLAB_ORIGIN}/{GITLAB_REPO}.git"
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
LINEAGE_BRANCH = "curie/thread-lineage-example"
PRIOR_HEAD = "b" * 40
REVISION_HEAD = "d" * 40
CA_REF = "/etc/curie/code-host-trust/ca.crt"
BASIC_TEST_CREDENTIAL = "Basic " + base64.b64encode(b"x-access-token:local-test-token").decode()


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


def _transport(
    module: Any,
    *,
    origin: str = ORIGIN,
    header_form: str = "authorization_basic",
    ca_bundle_ref: str | None = None,
) -> Any:
    return module.PublicationTransport(
        origin=origin, header_form=header_form, ca_bundle_ref=ca_bundle_ref
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
        title="Update repository",
        transport=_transport(module),
    )


def _gitlab_payload(module: Any, header_form: str = "private_token") -> Any:
    return replace(
        _payload(module),
        repo_full_name=GITLAB_REPO,
        clean_clone_url=GITLAB_URL,
        transport=_transport(module, origin=GITLAB_ORIGIN, header_form=header_form),
    )


def _resources(module: Any, patch: bytes = b"diff --git a/a b/a\n") -> Any:
    return module.build_publication_resources(
        _payload(module, patch),
        credential=WRITE_CREDENTIAL,
        settings=_settings(module),
    )


def _build(module: Any, payload: Any, settings: Any | None = None) -> Any:
    return module.build_publication_resources(
        payload,
        credential=WRITE_CREDENTIAL,
        settings=settings or _settings(module),
    )


def _ca_settings(module: Any) -> Any:
    return replace(
        _settings(module),
        ca_bundle_config_map="curie-code-host-trust",
        ca_bundle_key="corporate-root.pem",
    )


def _container(resources: Any) -> dict[str, Any]:
    container: dict[str, Any] = resources.job["spec"]["template"]["spec"]["containers"][0]
    return container


def _job_env(resources: Any) -> dict[str, str]:
    return {item["name"]: item["value"] for item in _container(resources)["env"]}


def _lineage_resources(module: Any) -> Any:
    payload = replace(
        _payload(module),
        revision_number=2,
        base_sha=PRIOR_HEAD,
        expected_prior_head=PRIOR_HEAD,
        expected_remote_head=PRIOR_HEAD,
    )
    return _build(module, payload)


def test_branch_prefix_mismatch_exits_before_any_git_call(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    payload = replace(
        _payload(publication_k8s),
        branch="factory/publication-abc",
        branch_prefix="factory/",
    )
    resources = _build(publication_k8s, payload)
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
    resources = _build(publication_k8s, payload)
    assert _job_env(resources)["BRANCH"] == "factory/publication-abc"
    assert _job_env(resources)["PUBLICATION_BRANCH_PREFIX"] == ""

    unsafe = replace(_payload(publication_k8s), branch="release/v1", branch_prefix=None)
    with pytest.raises(publication_k8s.PublicationResourceError, match="lineage branch"):
        _build(publication_k8s, unsafe)


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


def test_an_empty_patch_builds_no_job(publication_k8s: Any) -> None:
    with pytest.raises(publication_k8s.PublicationResourceError, match="non-empty patch"):
        _resources(publication_k8s, b"")


@pytest.mark.parametrize(
    "base_sha",
    ["abc", "A" * 40, "g" * 40, "a" * 65, "a" * 39, "a" * 40 + ";touch /tmp/x"],
)
def test_publication_base_sha_is_revalidated_before_entering_job_argv(
    publication_k8s: Any,
    base_sha: str,
) -> None:
    invalid = replace(_payload(publication_k8s), base_sha=base_sha)

    with pytest.raises(publication_k8s.PublicationResourceError, match="base SHA"):
        _build(publication_k8s, invalid)


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
    serialized_public = json.dumps({"job": job, "config_map": resources.config_map}, sort_keys=True)

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
        ("CODE_HOST_ORIGIN", ORIGIN),
        ("CODE_HOST_HEADER_FORM", "authorization_basic"),
        ("CLEAN_CLONE_URL", CLEAN_URL),
    } <= set(env_by_name.items())
    assert not [name for name in env_by_name if name.startswith(("GITHUB_", "PR_"))]
    assert container["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "1Gi"},
        "limits": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "4Gi"},
    }
    assert pod["metadata"]["labels"].get("curietech.ai/component") == "publication"
    assert pod["metadata"]["labels"].get("sandbox.agent-sandbox.io/runner") is None
    assert WRITE_CREDENTIAL not in serialized_public
    assert "secretKeyRef" not in json.dumps(container.get("env", []))
    assert resources.secret["stringData"]["credential"] == WRITE_CREDENTIAL


def test_publish_script_only_pushes_and_calls_no_code_host_api(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)
    script = resources.config_map["data"]["publish.sh"]
    serialized_job = json.dumps(resources.job)

    assert "GIT_ASKPASS" in script
    assert "/credentials/credential" in script
    assert 'git_with_timeout clone "$CLEAN_CLONE_URL"' in script
    assert 'git_with_timeout remote set-url origin "$CLEAN_CLONE_URL"' in script
    assert (
        'git_with_timeout -c user.name="$GIT_USER_NAME" '
        '-c user.email="$GIT_USER_EMAIL" commit' in script
    )
    assert "git_with_timeout apply --check" in script
    assert "git_with_timeout push" in script
    assert (
        'timeout --signal=TERM "${GIT_TIMEOUT_SECONDS}s" git -c http.followRedirects=false \\\n'
        '    -c include.path=/tmp/curie-git-auth.config "$@"' in script
    )
    # No REST client, no forge URL, no pull request marker: the API owns all of it.
    for forbidden in (
        "urllib",
        "http.client",
        "api.github.com",
        "GITHUB_",
        "CURIE_PR_",
        "/pulls",
        "set -x",
    ):
        assert forbidden not in script, forbidden
    assert re.search(r"(^|[\s;|(])gh\s", script) is None, "no gh CLI"
    assert "redact" in script.lower()
    assert WRITE_CREDENTIAL not in script
    assert WRITE_CREDENTIAL not in serialized_job


def test_lineage_revision_job_marks_one_commit_and_uses_exact_head_occupancy_cas(
    publication_k8s: Any,
) -> None:
    """Revision two may advance only the stored head of the stable lineage branch."""

    resources = _lineage_resources(publication_k8s)
    script = resources.config_map["data"]["publish.sh"]
    env = _job_env(resources)

    assert env["BRANCH"] == LINEAGE_BRANCH
    assert env["EXPECTED_PRIOR_HEAD"] == PRIOR_HEAD
    assert env["EXPECTED_REMOTE_HEAD"] == PRIOR_HEAD
    assert env["REVISION_ID"] == str(REVISION_ID)
    assert "Curie-Revision: $REVISION_ID" in script
    assert "--force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD" in script
    assert "git push --force " not in script
    assert f"publication-{PUBLICATION_ID.hex}" not in LINEAGE_BRANCH
    head_check = script.index('git_with_timeout ls-remote origin "refs/heads/$BRANCH"')
    apply = script.index('git_with_timeout apply --binary "$patch_path"')
    commit = script.index("commit \\\n")
    push = script.index("git_with_timeout push")
    success = script.index('echo "CURIE_COMMIT_SHA=$commit_sha"')
    assert head_check < apply < commit < push < success
    assert script.rstrip().endswith('echo "CURIE_COMMIT_SHA=$commit_sha"')


def test_lineage_job_refuses_every_remote_head_except_the_expected_one(
    publication_k8s: Any,
) -> None:
    script = _lineage_resources(publication_k8s).config_map["data"]["publish.sh"]

    assert "ls-remote" in script
    assert '"$remote_head" != "$EXPECTED_REMOTE_HEAD"' in script
    assert "publication branch head conflict" in script
    assert "force-with-lease=refs/heads/$BRANCH:$EXPECTED_REMOTE_HEAD" in script


def test_lineage_revision_payload_rejects_checkout_and_expected_head_disagreement(
    publication_k8s: Any,
) -> None:
    payload = replace(
        _payload(publication_k8s),
        revision_number=2,
        base_sha="c" * 40,
        expected_prior_head=PRIOR_HEAD,
        expected_remote_head=PRIOR_HEAD,
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="expected prior head"):
        _build(publication_k8s, payload)


def test_publish_job_refuses_redirects_for_git(
    publication_k8s: Any,
) -> None:
    """The credential-bearing transport may not follow an attacker-controlled redirect."""

    script = _resources(publication_k8s).config_map["data"]["publish.sh"]

    assert "git -c http.followRedirects=false" in script
    # Every git call goes through the wrapper, so none can skip the flag.
    git_calls = [
        line.strip()
        for line in script.splitlines()
        if not line.strip().startswith("#")
        and (line.strip().startswith("git ") or " git " in f" {line.strip()}")
    ]
    assert git_calls, "the script must call git"
    for line in git_calls:
        assert line.startswith(("git_with_timeout", "timeout --signal=TERM")), line


# Transport: origin, header form, CA bundle (ADR 0197).


@pytest.mark.parametrize(
    ("clone_url", "repo"),
    [
        pytest.param("https://gitlab.example.com/acme-corp/acme-bot.git", None, id="other-host"),
        pytest.param("http://github.com/acme-corp/acme-bot.git", None, id="plain-http"),
        pytest.param(
            "https://x-access-token:tok@github.com/acme-corp/acme-bot.git",
            None,
            id="userinfo",
        ),
        pytest.param("https://github.com/acme-corp/acme-bot", None, id="no-dot-git"),
        pytest.param("https://github.com.evil.example/acme-corp/acme-bot.git", None, id="suffix"),
        pytest.param(CLEAN_URL, "acme-corp/../acme-bot", id="dot-dot-repo"),
    ],
)
def test_builder_refuses_a_clone_url_outside_the_origin(
    publication_k8s: Any, clone_url: str, repo: str | None
) -> None:
    payload = replace(_payload(publication_k8s), clean_clone_url=clone_url)
    if repo is not None:
        payload = replace(payload, repo_full_name=repo)

    with pytest.raises(publication_k8s.PublicationResourceError, match="clone URL|repository path"):
        _build(publication_k8s, payload)


@pytest.mark.parametrize(
    "origin",
    [
        "http://github.com",
        "https://github.com/",
        "https://user:pw@github.com",
        "https://github.com?x=1",
        "https://",
    ],
)
def test_builder_refuses_an_unclean_origin(publication_k8s: Any, origin: str) -> None:
    payload = replace(
        _payload(publication_k8s),
        clean_clone_url=f"{origin}/acme-corp/acme-bot.git",
        transport=_transport(publication_k8s, origin=origin),
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="origin"):
        _build(publication_k8s, payload)


def test_builder_refuses_an_unknown_header_form(publication_k8s: Any) -> None:
    payload = replace(
        _payload(publication_k8s),
        transport=_transport(publication_k8s, header_form="cookie"),
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="header form"):
        _build(publication_k8s, payload)


def test_script_refuses_a_clone_url_outside_the_origin_before_reading_the_credential(
    publication_k8s: Any, tmp_path: Path
) -> None:
    resources = _build(publication_k8s, _gitlab_payload(publication_k8s))
    script = tmp_path / "publish.sh"
    script.write_text(resources.config_map["data"]["publish.sh"])
    missing_credential = tmp_path / "never-read"

    completed = subprocess.run(
        ["/bin/bash", str(script)],
        env={
            **os.environ,
            **_job_env(resources),
            # A same-prefix host must not pass as "under" the origin.
            "CLEAN_CLONE_URL": "https://gitlab.example.com/forgery/group/acme-bot.git",
            "CURIE_CREDENTIAL_PATH": str(missing_credential),
            "CURIE_WORK_DIR": str(tmp_path / "work"),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 1
    assert "not under the code host origin" in completed.stderr
    assert not (tmp_path / "work").exists()


def test_a_non_github_origin_with_a_nested_repository_path_builds(
    publication_k8s: Any,
) -> None:
    resources = _build(publication_k8s, _gitlab_payload(publication_k8s))
    env = _job_env(resources)

    assert env["CODE_HOST_ORIGIN"] == GITLAB_ORIGIN
    assert env["CODE_HOST_HEADER_FORM"] == "private_token"
    assert env["CLEAN_CLONE_URL"] == GITLAB_URL
    assert env["REPO_FULL_NAME"] == GITLAB_REPO
    assert "github" not in json.dumps(resources.job).lower()


def test_a_ca_bundle_mounts_the_configmap_read_only_and_points_every_client_at_it(
    publication_k8s: Any,
) -> None:
    payload = replace(
        _payload(publication_k8s),
        transport=_transport(publication_k8s, ca_bundle_ref=CA_REF),
    )
    resources = _build(publication_k8s, payload, _ca_settings(publication_k8s))
    pod_spec = resources.job["spec"]["template"]["spec"]
    container = _container(resources)
    env = _job_env(resources)

    trust_volumes = [v for v in pod_spec["volumes"] if v["name"] == "code-host-trust"]
    assert trust_volumes == [
        {
            "name": "code-host-trust",
            "configMap": {
                "name": "curie-code-host-trust",
                "items": [{"key": "corporate-root.pem", "path": "ca.crt"}],
            },
        }
    ]
    trust_mounts = [m for m in container["volumeMounts"] if m["name"] == "code-host-trust"]
    assert trust_mounts == [
        {
            "name": "code-host-trust",
            "mountPath": "/etc/curie/code-host-trust",
            "readOnly": True,
        }
    ]
    for name in ("CURIE_REPO_CA_BUNDLE", "GIT_SSL_CAINFO", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
        assert env[name] == CA_REF, name
    # The bundle is public trust material, never the credential's Secret.
    assert "secret" not in trust_volumes[0]


def test_no_ca_bundle_adds_no_volume_mount_or_trust_env(publication_k8s: Any) -> None:
    # A configured ConfigMap alone does not mount anything; the credential's ref does.
    for settings in (_settings(publication_k8s), _ca_settings(publication_k8s)):
        resources = _build(publication_k8s, _payload(publication_k8s), settings)
        pod_spec = resources.job["spec"]["template"]["spec"]
        container = _container(resources)
        env = _job_env(resources)

        assert [v["name"] for v in pod_spec["volumes"]] == [
            "publication",
            "credentials",
            "work",
            "tmp",
        ]
        assert [m["name"] for m in container["volumeMounts"]] == [
            "publication",
            "credentials",
            "work",
            "tmp",
        ]
        for name in (
            "CURIE_REPO_CA_BUNDLE",
            "GIT_SSL_CAINFO",
            "SSL_CERT_FILE",
            "REQUESTS_CA_BUNDLE",
        ):
            assert name not in env, name
    # The two builds are identical, so the operator setting cannot drift a running Job.
    assert (
        _build(publication_k8s, _payload(publication_k8s)).job
        == _build(publication_k8s, _payload(publication_k8s), _ca_settings(publication_k8s)).job
    )


def test_a_ca_bundle_ref_without_a_configmap_is_refused(publication_k8s: Any) -> None:
    payload = replace(
        _payload(publication_k8s),
        transport=_transport(publication_k8s, ca_bundle_ref=CA_REF),
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="no codeHostTrust"):
        _build(publication_k8s, payload)


@pytest.mark.parametrize(
    "ref",
    [
        "relative/ca.crt",
        "/ca.crt",
        "/etc/curie/../ca.crt",
        "/etc/curie/trust/",
        "/tmp/trust/ca.crt",
        "/credentials/ca.crt",
        "/publication/ca.crt",
        "/work/ca.crt",
    ],
)
def test_a_ca_bundle_ref_that_cannot_be_mounted_safely_is_refused(
    publication_k8s: Any, ref: str
) -> None:
    payload = replace(
        _payload(publication_k8s),
        transport=_transport(publication_k8s, ca_bundle_ref=ref),
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="CA bundle"):
        _build(publication_k8s, payload, _ca_settings(publication_k8s))


@pytest.mark.parametrize(
    ("origin", "header_form", "ca_bundle_ref"),
    [
        (ORIGIN, "authorization_basic", None),
        (GITLAB_ORIGIN, "private_token", None),
        (GITLAB_ORIGIN, "authorization_bearer", CA_REF),
    ],
)
def test_job_transport_round_trips_what_the_builder_wrote(
    publication_k8s: Any, origin: str, header_form: str, ca_bundle_ref: str | None
) -> None:
    transport = _transport(
        publication_k8s, origin=origin, header_form=header_form, ca_bundle_ref=ca_bundle_ref
    )
    repo = "acme-corp/acme-bot" if origin == ORIGIN else GITLAB_REPO
    payload = replace(
        _payload(publication_k8s),
        repo_full_name=repo,
        clean_clone_url=f"{origin}/{repo}.git",
        transport=transport,
    )
    resources = _build(publication_k8s, payload, _ca_settings(publication_k8s))

    assert publication_k8s.job_transport(resources.job) == transport
    # The real client hands back a V1Job model, not a dict.
    model = ApiClient().deserialize(SimpleNamespace(data=json.dumps(resources.job)), "V1Job")
    assert isinstance(model, V1Job)
    assert publication_k8s.job_transport(model) == transport
    # The rebuilt resources match the adopted Job exactly.
    rebuilt = _build(
        publication_k8s,
        replace(payload, transport=publication_k8s.job_transport(model)),
        _ca_settings(publication_k8s),
    )
    publication_k8s.validate_adopted_resource("Job", rebuilt.job, model)


def test_job_transport_is_none_when_the_job_names_no_valid_transport(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)

    no_origin = deepcopy(resources.job)
    container = no_origin["spec"]["template"]["spec"]["containers"][0]
    container["env"] = [e for e in container["env"] if e["name"] != "CODE_HOST_ORIGIN"]
    assert publication_k8s.job_transport(no_origin) is None

    unknown_form = deepcopy(resources.job)
    for item in unknown_form["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "CODE_HOST_HEADER_FORM":
            item["value"] = "cookie"
    assert publication_k8s.job_transport(unknown_form) is None

    two_containers = deepcopy(resources.job)
    pod_spec = two_containers["spec"]["template"]["spec"]
    pod_spec["containers"].append(deepcopy(pod_spec["containers"][0]))
    assert publication_k8s.job_transport(two_containers) is None


def test_an_adopted_job_with_an_altered_transport_fails_the_contract(
    publication_k8s: Any,
) -> None:
    resources = _resources(publication_k8s)
    altered = deepcopy(resources.job)
    for item in altered["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "CODE_HOST_ORIGIN":
            item["value"] = "https://evil.example"

    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, altered)

    bearer = _build(
        publication_k8s,
        replace(
            _payload(publication_k8s),
            transport=_transport(publication_k8s, header_form="authorization_bearer"),
        ),
    )
    annotation = "curietech.ai/publication-contract-sha256"
    assert (
        bearer.job["metadata"]["annotations"][annotation]
        != resources.job["metadata"]["annotations"][annotation]
    )
    with pytest.raises(publication_k8s.PublicationResourceError, match="metadata contract"):
        publication_k8s.validate_adopted_resource(
            "ConfigMap", bearer.config_map, resources.config_map
        )


# Executing the generated script against local bare repositories.


def _git() -> str:
    git = shutil.which("git")
    assert git is not None
    return git


def _seed_remote(tmp_path: Path) -> tuple[Path, str]:
    git = _git()
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
    subprocess.run([git, "-C", str(seed), "commit", "-m", "Base"], check=True, capture_output=True)
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
    subprocess.run(
        [git, "-C", str(seed), "push", "origin", "main"], check=True, capture_output=True
    )
    subprocess.run(
        [git, "--git-dir", str(remote), "symbolic-ref", "HEAD", "refs/heads/main"],
        check=True,
    )
    return remote, base_sha


def _branch_head(remote: Path) -> str:
    return subprocess.run(
        [_git(), "--git-dir", str(remote), "rev-parse", f"refs/heads/{LINEAGE_BRANCH}"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()


def _run_generated_publish_script(
    tmp_path: Path,
    resources: Any,
    *,
    remote: Path,
    ordinal: str,
    credential: str = BASIC_TEST_CREDENTIAL,
    path_prefix: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    root = tmp_path / f"run-{ordinal}"
    root.mkdir()
    credential_file = root / "credential"
    credential_file.write_text(credential)
    patch = root / "changes.patch"
    patch.write_bytes(base64.b64decode(resources.config_map["binaryData"]["changes.patch"]))
    # In the pod /tmp is a private emptyDir. Here it is the host's shared /tmp,
    # so each run gets its own directory for the auth files; nothing else changes.
    private_tmp = root / "tmp"
    private_tmp.mkdir()
    script_text = resources.config_map["data"]["publish.sh"]
    assert "/tmp/curie-git-auth.config" in script_text
    script = root / "publish.sh"
    script.write_text(script_text.replace("/tmp/curie-", f"{private_tmp}/curie-"))
    env = _job_env(resources)
    env = {
        **os.environ,
        **env,
        "CURIE_CREDENTIAL_PATH": str(credential_file),
        "CURIE_PATCH_PATH": str(patch),
        "CURIE_WORK_DIR": str(root / "work"),
        "GIT_ALLOW_PROTOCOL": "file",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": f"url.{remote.resolve().as_uri()}.insteadOf",
        "GIT_CONFIG_VALUE_0": env["CLEAN_CLONE_URL"],
    }
    if path_prefix is not None:
        env["PATH"] = f"{path_prefix}{os.pathsep}{env['PATH']}"
    if extra_env is not None:
        env.update(extra_env)
    completed = subprocess.run(
        ["/bin/bash", str(script)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    # The EXIT trap leaves no credential material behind, on success or failure.
    assert sorted(path.name for path in private_tmp.iterdir()) == []
    return completed


def _publication_resources_for_commit(
    module: Any,
    *,
    publication_id: uuid.UUID,
    revision_id: uuid.UUID,
    revision_number: int,
    base_sha: str,
    expected_remote_head: str | None,
    patch: bytes,
    transport: Any | None = None,
    repo_full_name: str = "acme-corp/acme-bot",
) -> Any:
    transport = transport or _transport(module)
    return module.build_publication_resources(
        module.PublicationPayload(
            publication_id=publication_id,
            revision_id=revision_id,
            revision_number=revision_number,
            repo_full_name=repo_full_name,
            clean_clone_url=f"{transport.origin}/{repo_full_name}.git",
            base_sha=base_sha,
            expected_prior_head=base_sha,
            expected_remote_head=expected_remote_head,
            patch=patch,
            branch=LINEAGE_BRANCH,
            title="Update repository",
            transport=transport,
        ),
        credential=BASIC_TEST_CREDENTIAL,
        settings=_settings(module),
    )


_FIRST_PATCH = (
    b"diff --git a/README.md b/README.md\n"
    b"--- a/README.md\n+++ b/README.md\n"
    b"@@ -1 +1,2 @@\n base\n+first\n"
)


def _commit_message(remote: Path, sha: str) -> str:
    return subprocess.run(
        [_git(), "--git-dir", str(remote), "log", "-1", "--format=%B", sha],
        text=True,
        capture_output=True,
        check=True,
    ).stdout


def test_generated_publish_script_keeps_one_lineage_and_refuses_lease_race(
    publication_k8s: Any,
    tmp_path: Path,
) -> None:
    git = _git()
    remote, base_sha = _seed_remote(tmp_path)

    first_resources = _publication_resources_for_commit(
        publication_k8s,
        publication_id=PUBLICATION_ID,
        revision_id=REVISION_ID,
        revision_number=1,
        base_sha=base_sha,
        expected_remote_head=None,
        patch=_FIRST_PATCH,
    )
    first = _run_generated_publish_script(tmp_path, first_resources, remote=remote, ordinal="one")
    assert first.returncode == 0, first.stderr
    first_head = _branch_head(remote)
    assert first.stdout.strip().splitlines()[-1] == f"CURIE_COMMIT_SHA={first_head}"
    assert "CURIE_PR_" not in first.stdout
    first_parents = subprocess.run(
        [git, "--git-dir", str(remote), "rev-list", "--parents", "-n", "1", first_head],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.split()
    assert first_parents == [first_head, base_sha]
    assert _commit_message(remote, first_head).rstrip("\n") == (
        f"Update repository\n\nCurie-Revision: {REVISION_ID}"
    )
    author = subprocess.run(
        [git, "--git-dir", str(remote), "log", "-1", "--format=%an <%ae>", first_head],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    assert author == "Curie Publisher <publisher@example.com>"

    second_revision = uuid.UUID("77777777-7777-4777-8777-777777777777")
    second_resources = _publication_resources_for_commit(
        publication_k8s,
        publication_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
        revision_id=second_revision,
        revision_number=2,
        base_sha=first_head,
        expected_remote_head=first_head,
        patch=(
            b"diff --git a/README.md b/README.md\n"
            b"--- a/README.md\n+++ b/README.md\n"
            b"@@ -1,2 +1,3 @@\n base\n first\n+second\n"
        ),
    )
    second = _run_generated_publish_script(tmp_path, second_resources, remote=remote, ordinal="two")
    assert second.returncode == 0, second.stderr
    second_head = _branch_head(remote)
    assert f"CURIE_COMMIT_SHA={second_head}" in second.stdout
    second_parents = subprocess.run(
        [git, "--git-dir", str(remote), "rev-list", "--parents", "-n", "1", second_head],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.split()
    assert second_parents == [second_head, first_head]
    assert f"Curie-Revision: {second_revision}" in _commit_message(remote, second_head)
    commit_count = subprocess.run(
        [git, "--git-dir", str(remote), "rev-list", "--count", f"{base_sha}..{second_head}"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    assert commit_count == "2"

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
    subprocess.run([git, "-C", str(contender), "config", "user.name", "Race Writer"], check=True)
    subprocess.run(
        [git, "-C", str(contender), "config", "user.email", "race@example.com"],
        check=True,
    )
    (contender / "race.txt").write_text("replacement\n")
    subprocess.run([git, "-C", str(contender), "add", "race.txt"], check=True)
    subprocess.run(
        [git, "-C", str(contender), "commit", "-m", "Race"], check=True, capture_output=True
    )
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
    assert "CURIE_COMMIT_SHA=" not in raced.stdout
    assert (tmp_path / "race-fired").is_file()
    assert _branch_head(remote) == replacement_head


@pytest.mark.parametrize(
    "expected_is_none",
    [
        pytest.param(True, id="branch-exists-but-expected-absent"),
        pytest.param(False, id="branch-moved-past-expected"),
    ],
)
def test_generated_publish_script_refuses_an_unexpected_remote_head_before_committing(
    publication_k8s: Any, tmp_path: Path, expected_is_none: bool
) -> None:
    remote, base_sha = _seed_remote(tmp_path)
    first = _run_generated_publish_script(
        tmp_path,
        _publication_resources_for_commit(
            publication_k8s,
            publication_id=PUBLICATION_ID,
            revision_id=REVISION_ID,
            revision_number=1,
            base_sha=base_sha,
            expected_remote_head=None,
            patch=_FIRST_PATCH,
        ),
        remote=remote,
        ordinal="one",
    )
    assert first.returncode == 0, first.stderr
    head = _branch_head(remote)

    stale = _publication_resources_for_commit(
        publication_k8s,
        publication_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
        revision_id=uuid.UUID("77777777-7777-4777-8777-777777777777"),
        revision_number=2,
        base_sha=base_sha,
        expected_remote_head=None if expected_is_none else base_sha,
        patch=_FIRST_PATCH,
    )
    refused = _run_generated_publish_script(tmp_path, stale, remote=remote, ordinal="stale")

    assert refused.returncode == 1
    assert "publication branch head conflict" in refused.stderr
    assert "CURIE_COMMIT_SHA=" not in refused.stdout
    assert _branch_head(remote) == head


_AUTH_CAPTURE_WRAPPER = """#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
from pathlib import Path

args = sys.argv[1:]
real_git = os.environ["CURIE_TEST_REAL_GIT"]
capture = Path(os.environ["CURIE_TEST_CAPTURE"])
if "clone" in args and not capture.exists():
    capture.mkdir()
    include = next(a.split("=", 1)[1] for a in args if a.startswith("include.path="))
    shutil.copy(include, capture / "auth.config")
    (capture / "argv").write_text("\\n".join(args))
    origin = os.environ["CODE_HOST_ORIGIN"]
    for prompt, name in (
        (f"Username for '{origin}': ", "askpass-user"),
        (f"Password for '{origin}': ", "askpass-pass"),
    ):
        answer = subprocess.run(
            [os.environ["GIT_ASKPASS"], prompt], capture_output=True, text=True, check=True
        )
        (capture / name).write_text(answer.stdout)
os.execv(real_git, [real_git, *args])
"""


def _run_with_auth_capture(
    module: Any, tmp_path: Path, *, header_form: str, credential: str
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    remote, base_sha = _seed_remote(tmp_path)
    wrapper_dir = tmp_path / "git-wrapper"
    wrapper_dir.mkdir()
    (wrapper_dir / "git").write_text(_AUTH_CAPTURE_WRAPPER)
    (wrapper_dir / "git").chmod(0o700)
    capture = tmp_path / "capture"
    resources = _publication_resources_for_commit(
        module,
        publication_id=PUBLICATION_ID,
        revision_id=REVISION_ID,
        revision_number=1,
        base_sha=base_sha,
        expected_remote_head=None,
        patch=_FIRST_PATCH,
        transport=_transport(module, origin=GITLAB_ORIGIN, header_form=header_form),
        repo_full_name=GITLAB_REPO,
    )
    completed = _run_generated_publish_script(
        tmp_path,
        resources,
        remote=remote,
        ordinal="auth",
        credential=credential,
        path_prefix=wrapper_dir,
        extra_env={"CURIE_TEST_REAL_GIT": _git(), "CURIE_TEST_CAPTURE": str(capture)},
    )
    return completed, capture, remote


def _url_matched_header(config: Path, url: str) -> str | None:
    """What git itself would send for ``url`` from the generated include file."""

    found = subprocess.run(
        [_git(), "config", "--file", str(config), "--get-urlmatch", "http.extraHeader", url],
        text=True,
        capture_output=True,
        check=False,
    )
    return found.stdout.strip() if found.returncode == 0 else None


def test_basic_header_form_answers_askpass_and_writes_no_header(
    publication_k8s: Any, tmp_path: Path
) -> None:
    completed, capture, remote = _run_with_auth_capture(
        publication_k8s,
        tmp_path,
        header_form="authorization_basic",
        credential=BASIC_TEST_CREDENTIAL + "\n",
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().endswith(f"CURIE_COMMIT_SHA={_branch_head(remote)}")
    assert (capture / "askpass-user").read_text() == "x-access-token"
    assert (capture / "askpass-pass").read_text() == "local-test-token"
    assert (capture / "auth.config").read_text() == ""
    argv = (capture / "argv").read_text().splitlines()
    assert "http.followRedirects=false" in argv
    assert argv[-2:] == [GITLAB_URL, "repo"]
    assert "local-test-token" not in completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("header_form", "credential", "header"),
    [
        pytest.param(
            "authorization_bearer",
            "Bearer glpat-local-token",
            "Authorization: Bearer glpat-local-token",
            id="bearer",
        ),
        pytest.param(
            "private_token",
            "glpat-local-token",
            "PRIVATE-TOKEN: glpat-local-token",
            id="private-token",
        ),
    ],
)
def test_header_forms_scope_one_extra_header_to_the_origin_only(
    publication_k8s: Any, tmp_path: Path, header_form: str, credential: str, header: str
) -> None:
    completed, capture, remote = _run_with_auth_capture(
        publication_k8s, tmp_path, header_form=header_form, credential=credential
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().endswith(f"CURIE_COMMIT_SHA={_branch_head(remote)}")
    config = capture / "auth.config"
    assert config.read_text() == f'[http "{GITLAB_ORIGIN}/"]\n\textraHeader = "{header}"\n'
    # Real git URL matching: the header goes to the origin's repositories only.
    assert _url_matched_header(config, GITLAB_URL) == header
    assert _url_matched_header(config, "https://gitlab.example.com/other/repo.git") is None
    assert _url_matched_header(config, "https://evil.example/forge/group/repo.git") is None
    # Askpass has nothing to give, so git cannot fall back to sending the token as Basic.
    assert (capture / "askpass-user").read_text() == ""
    assert (capture / "askpass-pass").read_text() == ""
    assert "glpat-local-token" not in completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("header_form", "credential", "message"),
    [
        pytest.param(
            "authorization_basic", "Bearer abc", "not Basic authorization", id="basic-got-bearer"
        ),
        pytest.param(
            "authorization_basic", "Basic !!!not-base64", "not valid Basic", id="basic-garbage"
        ),
        pytest.param(
            "authorization_basic",
            "Basic " + base64.b64encode(b"no-colon").decode(),
            "not valid Basic",
            id="basic-no-colon",
        ),
        pytest.param(
            "authorization_bearer", BASIC_TEST_CREDENTIAL, "not Bearer", id="bearer-got-basic"
        ),
        pytest.param(
            "private_token", 'tok"\n[http]\n\textraHeader = x', "forbidden", id="config-injection"
        ),
        pytest.param("private_token", "tok\\x", "forbidden", id="backslash"),
    ],
)
def test_a_credential_that_does_not_fit_its_header_form_is_refused_before_clone(
    publication_k8s: Any, tmp_path: Path, header_form: str, credential: str, message: str
) -> None:
    completed, capture, remote = _run_with_auth_capture(
        publication_k8s, tmp_path, header_form=header_form, credential=credential
    )

    assert completed.returncode != 0
    assert message in completed.stderr
    assert not capture.exists(), "git clone must not run"
    assert "CURIE_COMMIT_SHA=" not in completed.stdout
    if "abc" in credential or "tok" in credential:
        assert credential not in completed.stderr


# Redaction.


def _assert_script_redacts_credentials(script: str) -> None:
    redact_function = script[
        script.index("redact() {") : script.index("\n}\n\ngit_with_timeout") + 2
    ]
    sensitive = (
        "Authorization: Basic dXNlcjp3cml0ZS10b2tlbg==\n"
        "authorization: Bearer github_pat_sensitive\n"
        "PRIVATE-TOKEN: glpat-sensitive\n"
        "fatal: unable to access "
        "'https://oauth2:glpat-userinfo@gitlab.example.com/forge/group/acme-bot.git/'\n"
    )
    completed = subprocess.run(
        ["/bin/bash", "-c", f"{redact_function}\nredact"],
        input=sensitive,
        text=True,
        capture_output=True,
        check=True,
    )
    assert completed.stdout == (
        "Authorization: [REDACTED]\n"
        "Authorization: [REDACTED]\n"
        "Authorization: [REDACTED]\n"
        "fatal: unable to access 'https://gitlab.example.com/forge/group/acme-bot.git/'\n"
    )
    for secret in ("dXNlc", "github_pat_sensitive", "glpat-sensitive", "glpat-userinfo"):
        assert secret not in completed.stdout


@pytest.mark.parametrize(
    ("original", "mutated"),
    [
        pytest.param("[Bb][Ee][Aa][Rr][Ee][Rr]", "[Xx][Ee][Aa][Rr][Ee][Rr]", id="bearer"),
        pytest.param("|PRIVATE-TOKEN):", "|PRIVATE-XOKEN):", id="private-token"),
        pytest.param("s#(https?://)[^/@", "s#(xttps?://)[^/@", id="userinfo"),
    ],
)
def test_publish_script_executes_redaction_and_the_assertion_catches_a_mutation(
    publication_k8s: Any, original: str, mutated: str
) -> None:
    script = _resources(publication_k8s).config_map["data"]["publish.sh"]

    subprocess.run(["/bin/bash", "-n"], input=script, text=True, check=True)
    _assert_script_redacts_credentials(script)
    assert original in script
    with pytest.raises(AssertionError):
        _assert_script_redacts_credentials(script.replace(original, mutated))


def test_worker_side_redaction_scrubs_a_non_github_origin(publication_k8s: Any) -> None:
    text = (
        "fatal: unable to access "
        "'https://oauth2:glpat-userinfo@gitlab.example.com/forge/group/acme-bot.git/'\n"
        "PRIVATE-TOKEN: glpat-header\n"
        "private-token:glpat-lower\n"
        "Authorization: Bearer glpat-bearer\n"
    )

    redacted = publication_k8s._redact(text)

    for secret in ("glpat-userinfo", "oauth2", "glpat-header", "glpat-lower", "glpat-bearer"):
        assert secret not in redacted, secret
    assert "https://[REDACTED]@gitlab.example.com/forge/group/acme-bot.git/" in redacted


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

    def delete_namespaced_secret(self, name: str, namespace: str, *, body: dict[str, Any]) -> None:
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

    core.config_maps[resources.names.config_map]["metadata"]["ownerReferences"][0]["uid"] = (
        "different-owner-uid"
    )
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

    assert core.secret_deletes == [(resources.names.secret, {"preconditions": {"uid": old_uid}})]
    assert batch.created == [resources.names.job, resources.names.job]
    assert (
        base64.b64decode(core.secrets[resources.names.secret]["data"]["credential"]).decode()
        == "rotated-publication-write-credential"
    )


def _v1_job(
    uid: str,
    *,
    succeeded: int = 0,
    failed: int = 0,
    conditions: list[V1JobCondition] | None = None,
) -> V1Job:
    """The model the real BatchV1Api.read_namespaced_job returns."""

    return V1Job(
        metadata=V1ObjectMeta(uid=uid),
        status=V1JobStatus(succeeded=succeeded, failed=failed, conditions=conditions),
    )


def test_observe_reads_logs_only_from_a_pod_owned_by_the_exact_job(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-123"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: _v1_job(job_uid, succeeded=1)
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
            return f"CURIE_COMMIT_SHA={'e' * 40}\n"
        return f"CURIE_COMMIT_SHA={REVISION_HEAD}\n"

    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: SimpleNamespace(items=[hostile, owned]),
        read_namespaced_pod_log=read_log,
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert log_reads == ["owned-pod"]
    assert observed.phase == "succeeded"
    assert observed.commit_sha == REVISION_HEAD
    assert observed.error is None


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
                "conditions": [{"status": "True", "reason": "DeadlineExceeded"}],
            },
        }
    )
    cluster._core = SimpleNamespace(list_namespaced_pod=lambda *_args, **_kwargs: {"items": []})

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.phase == "failed"
    assert observed.error == "DeadlineExceeded; pod logs were unavailable"
    assert observed.transport is None


def test_observe_reports_the_transport_the_existing_job_was_built_with(
    publication_k8s: Any,
) -> None:
    payload = replace(
        _gitlab_payload(publication_k8s, "authorization_bearer"),
        transport=_transport(
            publication_k8s,
            origin=GITLAB_ORIGIN,
            header_form="authorization_bearer",
            ca_bundle_ref=CA_REF,
        ),
    )
    resources = _build(publication_k8s, payload, _ca_settings(publication_k8s))
    live = deepcopy(resources.job)
    live["metadata"]["uid"] = "job-uid-live"
    live["status"] = {"active": 1}
    model = ApiClient().deserialize(SimpleNamespace(data=json.dumps(live)), "V1Job")
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie"
    cluster._batch = SimpleNamespace(read_namespaced_job=lambda *_args: model)
    cluster._core = SimpleNamespace(list_namespaced_pod=lambda *_args, **_kwargs: {"items": []})

    observed = cluster.observe(resources.names.job)

    assert observed.phase == "running"
    assert observed.commit_sha is None
    assert observed.transport == payload.transport


def test_observe_of_a_missing_job_reports_not_existing(publication_k8s: Any) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie"

    def missing(*_args: object) -> None:
        raise ApiException(status=404)

    cluster._batch = SimpleNamespace(read_namespaced_job=missing)

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.exists is False
    assert observed.phase == "pending"
    assert observed.transport is None


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


def test_observe_parses_the_commit_marker_from_the_real_clients_pod_log_shape(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-success"
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: _v1_job(job_uid, succeeded=1)
    )
    raw = (
        b"Cloning into 'repo'...\n"
        b"[curie/thread-lineage-example 0123456] Update repository\n"
        + f"CURIE_COMMIT_SHA={REVISION_HEAD}\n".encode()
    )
    cluster._core = SimpleNamespace(
        list_namespaced_pod=lambda *_args, **_kwargs: SimpleNamespace(
            items=[_owned_pod("owned-pod", job_uid)]
        ),
        read_namespaced_pod_log=_real_client_pod_log(raw),
    )

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.commit_sha == REVISION_HEAD
    assert observed.logs.endswith(f"CURIE_COMMIT_SHA={REVISION_HEAD}\n")


def test_observe_failed_job_error_is_not_a_bytes_repr_on_the_real_client_shape(
    publication_k8s: Any,
) -> None:
    cluster = object.__new__(publication_k8s.KubernetesPublicationCluster)
    cluster.namespace = "curie-publications"
    job_uid = "job-uid-failed"
    condition = {
        "status": "True",
        "reason": "BackoffLimitExceeded",
        "message": "Job has reached the specified backoff limit",
    }
    cluster._batch = SimpleNamespace(
        read_namespaced_job=lambda *_args: _v1_job(
            job_uid,
            failed=1,
            conditions=[
                V1JobCondition(type="FailureTarget", **condition),
                V1JobCondition(type="Failed", **condition),
            ],
        )
    )
    raw = (
        b"Cloning into 'repo'...\n"
        b"fatal: Authentication failed for 'https://gitlab.example.com/o/r.git/'\n"
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
    payload = replace(
        _payload(publication_k8s),
        clean_clone_url="https://github.com/other-corp/other-repo.git",
    )

    with pytest.raises(publication_k8s.PublicationResourceError, match="clone URL"):
        _build(publication_k8s, payload)


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
    return _build(
        module,
        _payload(module),
        replace(_settings(module), priority_class_name="", image_pull_secrets=()),
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
    planted = False
    for item in planted_env["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "EXPECTED_REMOTE_HEAD":
            assert "value" not in item
            item["value"] = "e" * 40
            planted = True
    assert planted
    with pytest.raises(publication_k8s.PublicationResourceError, match="spec mismatch"):
        publication_k8s.validate_adopted_resource("Job", resources.job, planted_env)

    planted_priority = _omitempty(deepcopy(resources.job))
    planted_priority["spec"]["template"]["spec"]["priorityClassName"] = "system-node-critical"
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
    empty_items[0]["valueFrom"] = {"secretKeyRef": {"name": secret_name, "key": "credential"}}

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
                    "state": {"terminated": {"reason": "Error", "exitCode": exit_code}},
                }
            ]
        },
    }


@pytest.mark.parametrize(
    ("leak_lines", "kept"),
    [
        pytest.param(
            [
                "Authorization: Bearer SECRETTOKEN",
                "fatal: repository 'https://x-access-token:SECRETTOKEN@github.com/o/r.git/'"
                " not found",
            ],
            "github.com/o/r.git/' not found",
            id="github",
        ),
        pytest.param(
            [
                "PRIVATE-TOKEN: SECRETTOKEN",
                "fatal: repository "
                "'https://oauth2:SECRETTOKEN@gitlab.example.com/forge/o/r.git/' not found",
            ],
            "gitlab.example.com/forge/o/r.git/' not found",
            id="non-github",
        ),
    ],
)
def test_observe_failed_job_names_reason_exit_code_and_redacted_git_error(
    publication_k8s: Any, leak_lines: list[str], kept: str
) -> None:
    logs = "\n".join(
        [
            "CURIE_PUBLICATION_STAGE=clone",
            "Cloning into 'repo'...",
            *leak_lines,
            "CURIE_PUBLICATION_STAGE=failed",
        ]
    )
    cluster = _failed_job_cluster(publication_k8s, pods=[_failed_owned_pod(128)], logs=logs)

    observed = cluster.observe("curie-publication-22222222222242228222")

    assert observed.phase == "failed"
    error = observed.error or ""
    assert "BackoffLimitExceeded" in error
    assert "exit code 128" in error
    assert "fatal: repository 'https://" in error
    assert kept in error
    assert "CURIE_PUBLICATION_STAGE" not in error
    assert "SECRETTOKEN" not in error
    assert "SECRETTOKEN" not in observed.logs
    assert len(error) <= 1500


def test_observe_failed_job_error_is_bounded_for_huge_logs(
    publication_k8s: Any,
) -> None:
    logs = "\n".join(f"fatal: line {index} " + "x" * 200 for index in range(60))
    cluster = _failed_job_cluster(publication_k8s, pods=[_failed_owned_pod(1)], logs=logs)

    observed = cluster.observe("curie-publication-22222222222242228222")

    error = observed.error or ""
    assert "BackoffLimitExceeded" in error
    assert "fatal: line 59" in error
    assert len(error) <= 1500


def test_observe_failed_job_without_pod_says_logs_were_unavailable(
    publication_k8s: Any,
) -> None:
    cluster = _failed_job_cluster(publication_k8s, pods=[], logs="", reason="DeadlineExceeded")

    observed = cluster.observe("curie-publication-22222222222242228222")

    error = observed.error or ""
    assert "DeadlineExceeded" in error
    assert "pod logs were unavailable" in error
