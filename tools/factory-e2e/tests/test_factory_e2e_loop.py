"""Unit contract for the candidate-image path, the forge-neutral loop and the forge registry.

The live paths need a cluster, the dedicated test GitHub App and fixture
repository, so they are proved by running the command. These pin what must
hold before then: built images are what the install points at, a running pod
must carry the built digest, kind loads while other clusters push, the loop
judge names every shortfall, every forge entry file registers itself, the
forge-loop scenario observes through the driver `--forge` names, and a forge
whose driver is not installed is refused before anything is installed.
"""

from __future__ import annotations

import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SHA = "c" * 40
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64


def _app_dir(tmp_path: Path) -> Path:
    app = tmp_path / "app"
    app.mkdir(parents=True)
    meta = {"id": 42, "slug": "example-factory", "installation_id": 7, "repo": "acme/fixture"}
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    return app


def _config(tmp_path: Path, **env: str) -> fe.FactoryConfig:
    base = {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(_app_dir(tmp_path)),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }
    base.update(env)

    def no_gh(user: str) -> str:
        raise AssertionError(user)

    return fe.load_config(base, context=None, gh_token=no_gh)


def _preflight(tmp_path: Path, **kwargs: Any) -> fe.Preflight:
    context = kwargs.pop("context", "scratch")
    return fe.Preflight(
        _config(tmp_path, CURIE_FACTORY_KUBE_CONTEXT=context),
        repo_root=REPO_ROOT,
        candidate=SHA,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
        **kwargs,
    )


def _images(prefix: str = "registry.example/factory/factory-e2e-ab") -> dict[str, Any]:
    return {
        name: fe.CandidateImage(name, f"{prefix}/{name}", "cand-cccccccccccc", (D1,))
        for name in fe.CANDIDATE_BUILDS
    }


# --- candidate image values --------------------------------------------------


def test_candidate_builds_cover_every_chart_component_and_the_runner() -> None:
    assert set(fe.CANDIDATE_BUILDS) == {*fe.CHART_COMPONENTS.values(), fe.RUNNER_IMAGE}
    for context, dockerfile in fe.CANDIDATE_BUILDS.values():
        assert context == "."
        assert (REPO_ROOT / dockerfile).is_file(), dockerfile


def test_install_values_point_every_component_and_the_runner_at_built_images(
    tmp_path: Path,
) -> None:
    images = _images()
    values = fe.install_values(
        _config(tmp_path),
        candidate=SHA,
        app_key_secret="k",
        consumer_controller=False,
        candidate_images=images,
    )
    for component, name in fe.CHART_COMPONENTS.items():
        assert values[component]["image"] == {
            "repository": f"registry.example/factory/factory-e2e-ab/{name}",
            "tag": "cand-cccccccccccc",
        }
    runner = values["agentSandbox"]["runner"]
    assert runner["image"] == "registry.example/factory/factory-e2e-ab/curie-runner"
    assert runner["tag"] == "cand-cccccccccccc"
    assert "prewarm" not in runner
    # Everything else the install sets is untouched.
    assert values["factory"]["enabled"] is True
    assert values["agentSandbox"]["controller"]["deploy"] is True


def test_kind_values_never_pull_a_loaded_image(tmp_path: Path) -> None:
    values = fe.install_values(
        _config(tmp_path),
        candidate=SHA,
        app_key_secret="k",
        consumer_controller=False,
        candidate_images=_images("factory-e2e-ab"),
        kind=True,
    )
    for component in fe.CHART_COMPONENTS:
        assert values[component]["image"]["pullPolicy"] == "IfNotPresent"
    runner = values["agentSandbox"]["runner"]
    assert runner["imagePullPolicy"] == "IfNotPresent"
    assert runner["prewarm"] == {"imagePullPolicy": "IfNotPresent"}


def test_published_values_are_unchanged_without_candidate_images(tmp_path: Path) -> None:
    values = fe.install_values(
        _config(tmp_path), candidate=SHA, app_key_secret="k", consumer_controller=False
    )
    assert values["api"]["image"] == {"tag": f"sha-{SHA}"}
    assert values["agentSandbox"]["runner"]["tag"] == f"sha-{SHA}"
    assert "image" not in values["agentSandbox"]["runner"]


def test_candidate_tag_and_repository_prefix() -> None:
    assert fe.candidate_image_tag(SHA) == "cand-cccccccccccc"
    assert fe.candidate_repository_prefix("reg.example/x/", False, "ab") == (
        "reg.example/x/factory-e2e-ab"
    )
    assert fe.candidate_repository_prefix(None, True, "ab") == "factory-e2e-ab"
    with pytest.raises(fe.ConfigError, match="CURIE_FACTORY_LAYER_REGISTRY"):
        fe.candidate_repository_prefix(None, False, "ab")


# --- kind vs push --------------------------------------------------------------


def test_kind_is_chosen_by_context_prefix_or_flag() -> None:
    assert fe.kind_cluster_name("kind-dev") == "dev"
    assert fe.kind_cluster_name("k8") is None
    assert fe.kind_cluster_name("k8", named="local") == "local"
    assert fe.kind_cluster_name("kind-dev", named="other") == "other"


def test_preflight_only_detects_kind_in_build_mode(tmp_path: Path) -> None:
    assert _preflight(tmp_path, context="kind-dev").kind_cluster is None
    built = _preflight(tmp_path / "b", context="kind-dev", candidate_images="build")
    assert built.kind_cluster == "dev"
    assert built.evidence["image_tag"] == "cand-cccccccccccc"
    with pytest.raises(fe.ConfigError, match="--candidate-images"):
        _preflight(tmp_path / "c", candidate_images="local")


def test_build_argv_uses_the_named_builder_and_never_selects_one(tmp_path: Path) -> None:
    pushed = fe.candidate_build_argv(
        builder="ci-builder",
        ref="reg/x/curie-api:cand-1",
        dockerfile=tmp_path / "apps/api/Dockerfile",
        context_dir=tmp_path,
        platforms=["linux/amd64", "linux/arm64"],
        load=False,
        metadata_file=tmp_path / "m.json",
    )
    assert pushed[:5] == ["docker", "buildx", "build", "--builder", "ci-builder"]
    assert "--use" not in pushed and "use" not in pushed
    assert pushed[pushed.index("--platform") + 1] == "linux/amd64,linux/arm64"
    assert "--push" in pushed and "--load" not in pushed
    assert pushed[pushed.index("--metadata-file") + 1] == str(tmp_path / "m.json")
    assert pushed[-1] == str(tmp_path)
    loaded = fe.candidate_build_argv(
        builder="ci-builder",
        ref="factory-e2e-ab/curie-api:cand-1",
        dockerfile=tmp_path / "Dockerfile",
        context_dir=tmp_path,
        platforms=["linux/amd64"],
        load=True,
    )
    assert "--load" in loaded and "--push" not in loaded
    with pytest.raises(fe.ConfigError, match="one platform"):
        fe.candidate_build_argv(
            builder="b",
            ref="r",
            dockerfile=tmp_path,
            context_dir=tmp_path,
            platforms=["linux/amd64", "linux/arm64"],
            load=True,
        )


def test_platforms_follow_the_nodes() -> None:
    assert fe.candidate_platforms(["amd64", "amd64"]) == ["linux/amd64"]
    assert fe.candidate_platforms(["arm64", "amd64"]) == ["linux/amd64", "linux/arm64"]
    with pytest.raises(fe.PreflightFailed):
        fe.candidate_platforms([])


def test_pushed_digest_requires_a_real_digest() -> None:
    assert fe.pushed_digest({"containerimage.digest": D1}) == D1
    with pytest.raises(fe.PreflightFailed):
        fe.pushed_digest({"containerimage.digest": "latest"})


def test_build_mode_needs_a_named_builder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BUILDX_BUILDER", raising=False)
    with pytest.raises(fe.ConfigError, match="BUILDX_BUILDER"):
        _preflight(tmp_path, candidate_images="build").check_images()


class _FakeBuild:
    """Records docker calls; writes buildx metadata as a push would."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def subprocess_run(self, argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        if "--metadata-file" in argv:
            path = Path(argv[argv.index("--metadata-file") + 1])
            path.write_text(json.dumps({"containerimage.digest": D1}))
        if argv[:3] == ["docker", "exec", "dev-control-plane"]:
            return subprocess.CompletedProcess(
                argv, 0, json.dumps({"status": {"id": D2, "repoDigests": []}}), ""
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    def run(self, argv: list[str], **_: Any) -> str:
        self.calls.append(list(argv))
        return D2 if argv[:3] == ["docker", "image", "inspect"] else ""


def _build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, context: str
) -> tuple[fe.Preflight, _FakeBuild]:
    monkeypatch.setenv("BUILDX_BUILDER", "ci-builder")
    preflight = _preflight(tmp_path, context=context, candidate_images="build")
    fake = _FakeBuild()
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(preflight, "export_source", lambda: source)
    monkeypatch.setattr(preflight, "kubectl", lambda *a, **k: "amd64 amd64")
    monkeypatch.setattr(fe.subprocess, "run", fake.subprocess_run)
    monkeypatch.setattr(fe, "run", fake.run)
    preflight.build_candidate_images()
    return preflight, fake


def test_push_mode_pushes_every_image_under_a_fresh_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preflight, fake = _build(tmp_path, monkeypatch, context="k8")
    builds = [c for c in fake.calls if c[:3] == ["docker", "buildx", "build"]]
    assert len(builds) == len(fe.CANDIDATE_BUILDS)
    assert all("--push" in c and "--load" not in c for c in builds)
    assert all(c[c.index("--platform") + 1] == "linux/amd64" for c in builds)
    assert not [c for c in fake.calls if c[0] == "kind"]
    images = preflight.candidate_images
    assert images is not None
    prefixes = {image.repository.rsplit("/", 1)[0] for image in images.values()}
    assert len(prefixes) == 1
    (prefix,) = prefixes
    assert prefix.startswith("registry.example/factory/factory-e2e-")
    assert all(image.digests == (D1,) for image in images.values())
    # The layer builds FROM the built runner, pinned by digest, on the same platforms.
    assert preflight._layer_base == f"{prefix}/curie-runner@{D1}"
    evidence = preflight.evidence["candidate_images"]
    assert evidence["delivery"] == "registry push"
    assert evidence["images"]["curie-api"]["digests"] == [D1]


def test_kind_mode_loads_images_and_pushes_only_the_layer_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preflight, fake = _build(tmp_path, monkeypatch, context="kind-dev")
    builds = [c for c in fake.calls if c[:3] == ["docker", "buildx", "build"]]
    loaded = [c for c in builds if "--load" in c]
    pushed = [c for c in builds if "--push" in c]
    assert len(loaded) == len(fe.CANDIDATE_BUILDS)
    assert len(pushed) == 1 and "/curie-runner:" in pushed[0][pushed[0].index("-t") + 1]
    kind_loads = [c for c in fake.calls if c[:3] == ["kind", "load", "docker-image"]]
    assert len(kind_loads) == len(fe.CANDIDATE_BUILDS)
    assert all(c[-2:] == ["--name", "dev"] for c in kind_loads)
    images = preflight.candidate_images
    assert images is not None
    assert all(image.repository.startswith("factory-e2e-") for image in images.values())
    assert images["curie-api"].digests == (D2,)
    assert preflight._layer_base.startswith("registry.example/factory/factory-e2e-")
    assert preflight._layer_base.endswith(f"/curie-runner@{D1}")


def test_layer_is_built_on_the_candidate_runner_for_its_platforms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preflight, _ = _build(tmp_path, monkeypatch, context="k8")
    calls: list[list[str]] = []

    def fake_run(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(fe.subprocess, "run", fake_run)
    preflight.build_runner_layer(fe.DEFAULT_BUNDLE)
    (argv,) = calls
    assert argv[argv.index("--runner-image") + 1] == preflight._layer_base
    assert "ghcr.io" not in preflight._layer_base
    assert argv[-2:] == ["--platform", "linux/amd64"]


# --- running image ids ---------------------------------------------------------


def _pod(name: str, image: str, image_id: str) -> dict[str, Any]:
    return {
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "main", "image": image}]},
        "status": {"containerStatuses": [{"name": "main", "imageID": image_id}]},
    }


def test_running_images_match_the_built_digests() -> None:
    images = _images("reg.example:5000/x/factory-e2e-ab")
    pods = {
        "items": [
            _pod(
                "api-1",
                "reg.example:5000/x/factory-e2e-ab/curie-api:cand-cccccccccccc",
                f"reg.example:5000/x/factory-e2e-ab/curie-api@{D1}",
            ),
            _pod("pg-0", "postgres:16", "docker.io/library/postgres@" + D2),
        ]
    }
    records, failures = fe.judge_running_images(pods, images, required=["curie-api"])
    assert failures == []
    assert records == [
        {
            "pod": "api-1",
            "container": "main",
            "image": "reg.example:5000/x/factory-e2e-ab/curie-api:cand-cccccccccccc",
            "image_id": f"reg.example:5000/x/factory-e2e-ab/curie-api@{D1}",
            "expected": [D1],
        }
    ]


def test_running_images_mismatch_and_absence_fail() -> None:
    images = _images("reg/x")
    pods = {"items": [_pod("api-1", "reg/x/curie-api:cand-cccccccccccc", f"reg/x/curie-api@{D2}")]}
    _, failures = fe.judge_running_images(pods, images, required=["curie-api", "curie-worker"])
    assert any("api-1/main runs " + D2 in f for f in failures)
    assert any("no running container uses the built curie-worker" in f for f in failures)
    pending = {"items": [_pod("api-1", "reg/x/curie-api:cand-cccccccccccc", "")]}
    _, failures = fe.judge_running_images(pending, images, required=[])
    assert failures == ["api-1/main reports no image id yet"]


def test_required_images_come_from_the_workloads() -> None:
    images = _images("reg/x")
    workloads = {
        "items": [
            {"spec": {"template": {"spec": {"containers": [{"image": "reg/x/curie-api:t"}]}}}},
            {"spec": {"template": {"spec": {"containers": [{"image": "postgres:16"}]}}}},
            {
                "spec": {
                    "template": {
                        "spec": {"initContainers": [{"image": f"reg/x/curie-runner@{D1}"}]}
                    }
                }
            },
        ]
    }
    assert fe.candidate_workload_images(workloads, images) == ["curie-api", "curie-runner"]


def test_assert_running_images_records_evidence_and_fails_on_mismatch(
    tmp_path: Path,
) -> None:
    preflight = _preflight(tmp_path)
    preflight.candidate_images = _images("reg/x")
    workloads = {
        "items": [
            {"spec": {"template": {"spec": {"containers": [{"image": "reg/x/curie-api:t"}]}}}}
        ]
    }
    pods = {"items": [_pod("api-1", "reg/x/curie-api:t", f"reg/x/curie-api@{D2}")]}

    def kubectl(*args: str, check: bool = True) -> str:
        return json.dumps(pods if "pods" in args else workloads)

    preflight.kubectl = kubectl  # type: ignore[method-assign]
    with pytest.raises(fe.PreflightFailed, match="running images differ"):
        preflight.assert_running_images()
    assert preflight.evidence["running_images"][0]["image_id"] == f"reg/x/curie-api@{D2}"
    pods["items"] = [_pod("api-1", "reg/x/curie-api:t", f"reg/x/curie-api@{D1}")]
    preflight.assert_running_images()


# --- github-loop judge -----------------------------------------------------------


def _loop_obs() -> dict[str, Any]:
    return {
        "forge": "github",
        "change_number": 7,
        "head_initial": "a" * 40,
        "commits_initial": 1,
        "red_ci": {"sha": "a" * 40, "context": "factory-e2e/loop-gate", "state": "failure"},
        "first_run_statuses": ["completed"],
        "head_after_ci": "b" * 40,
        "commits_after_ci": 2,
        "ci_after_fix": {"sha": "b" * 40, "state": "passing"},
        "review_delivery_status_code": 200,
        "review_delivery_api_status": "factory_admitted",
        "request_statuses": ["completed", "completed"],
        "head_after_review": "d" * 40,
        "commits_after_review": 3,
        "work_item_change_number": 7,
        "change_numbers": [7],
        "default_branch_moved_before_merge": False,
        "merge": {"status_code": 200, "merged": True, "method": "merge"},
        "forge_merged": True,
        "work_item_pr_status": "merged",
        "cli_pr_status": "merged",
        "cli_failures": [],
    }


def test_github_loop_passes_on_a_complete_loop() -> None:
    assert fe.judge_github_loop(_loop_obs()) == []


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"red_ci": {"sha": "a" * 40, "state": "success"}}, "CI was not set red"),
        ({"red_ci": {"sha": "z" * 40, "state": "failure"}}, "CI was not set red"),
        ({"first_run_statuses": ["failed"]}, "first request ended 'failed'"),
        ({"first_run_statuses": []}, "first request ended None"),
        ({"head_after_ci": "a" * 40}, "no CI fix commit"),
        ({"commits_after_ci": 1}, "the CI fix added no commit"),
        ({"ci_after_fix": {"state": "failing"}}, "checks on the CI fix head read 'failing'"),
        ({"ci_after_fix": None}, "checks on the CI fix head read None"),
        ({"review_delivery_status_code": 500}, "review delivery got HTTP 500"),
        ({"review_delivery_api_status": "factory_ignored"}, "review was not admitted"),
        ({"request_statuses": ["completed"]}, "has 1 request(s), expected 2"),
        ({"request_statuses": ["completed", "failed"]}, "review request ended 'failed'"),
        ({"head_after_review": "b" * 40}, "review revision pushed no commit"),
        ({"commits_after_review": 2}, "review revision added no commit"),
        ({"work_item_change_number": 8}, "WorkItem records pull request #8"),
        ({"change_numbers": [7, 8]}, "pull requests [7, 8] were opened"),
        ({"default_branch_moved_before_merge": True}, "default branch moved before the merge"),
        ({"merge": {"status_code": 405, "merged": False}}, "merge failed (HTTP 405)"),
        ({"forge_merged": False}, "forge does not read the pull request as merged"),
        ({"work_item_pr_status": "open"}, "WorkItem api reads the pull request 'open'"),
        ({"cli_pr_status": None}, "`curie cluster work-items` reads the pull request None"),
        ({"cli_failures": ["after merge: exited 2"]}, "cli: after merge: exited 2"),
    ],
)
def test_github_loop_names_each_shortfall(change: dict[str, Any], expected: str) -> None:
    obs = {**_loop_obs(), **change}
    failures = fe.judge_github_loop(obs)
    assert len(failures) == 1, failures
    assert expected in failures[0]


def test_github_loop_without_a_pull_request_stops_at_that() -> None:
    obs = {"forge": "github", "cli_failures": ["unknown id: exited 0"]}
    assert fe.judge_github_loop(obs) == [
        "the run opened no pull request",
        "cli: unknown id: exited 0",
    ]


def test_cli_pr_status_reads_the_work_item_pr() -> None:
    body = json.dumps({"item": {"state": "published", "pr": {"number": 7, "status": "merged"}}})
    assert fe.work_item_cli_pr_status(body) == "merged"
    assert fe.work_item_cli_pr_status(json.dumps({"item": {"pr": None}})) is None
    assert fe.work_item_cli_pr_status("not json") is None


def test_github_loop_needs_a_model_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(_app_dir(tmp_path)),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_API_KEY", raising=False)
    assert fe.main(["run", "--scenario", "github-loop"]) == fe.EXIT_CONFIG


# --- the GitHub entry -----------------------------------------------------------


def _github_entry() -> Any:
    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.github"]


def test_github_ci_state_reads_runs_and_statuses() -> None:
    gh = _github_entry()
    ok = {"status": "completed", "conclusion": "success"}
    assert gh.github_ci_state([], []) == "none"
    assert gh.github_ci_state([ok], [{"state": "success"}]) == "passing"
    assert gh.github_ci_state([ok, {"status": "in_progress"}], []) == "pending"
    assert gh.github_ci_state([ok], [{"state": "pending"}]) == "pending"
    assert gh.github_ci_state([{"status": "completed", "conclusion": "failure"}], []) == "failing"
    assert gh.github_ci_state([ok], [{"state": "failure"}]) == "failing"
    assert len(gh.LOOP_GATE_DESCRIPTION) <= 140


def test_review_delivery_matches_the_review_and_repository() -> None:
    gh = _github_entry()

    def delivery(guid: str, review_id: int, repo: str) -> dict[str, Any]:
        return {
            "guid": guid,
            "event": "pull_request_review",
            "action": "submitted",
            "request": {
                "payload": {"review": {"id": review_id}, "repository": {"full_name": repo}}
            },
        }

    deliveries = [
        delivery("a", 5, "acme/fixture"),
        delivery("b", 6, "acme/fixture"),
        delivery("c", 5, "acme/other"),
        {**delivery("d", 5, "acme/fixture"), "action": "edited"},
    ]
    assert gh.match_review_delivery(deliveries, review_id=5, repo="acme/fixture")["guid"] == "a"
    assert gh.match_review_delivery(deliveries, review_id=9, repo="acme/fixture") is None


class _Actor:
    def __init__(self, answers: list[tuple[int, Any]]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, str, Any]] = []

    def __call__(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        self.calls.append((method, path, body))
        return self.answers.pop(0)


def test_github_entry_sets_red_ci_and_merges_with_an_allowed_method(tmp_path: Path) -> None:
    gh = _github_entry()
    preflight = _preflight(tmp_path)
    actor = _Actor(
        [
            (201, {"state": "failure"}),
            (405, {"message": "Merge commits are not allowed"}),
            (200, {"merged": True, "sha": "e" * 40}),
        ]
    )
    preflight.as_actor = actor  # type: ignore[method-assign]
    loop = gh.GitHubDriver(preflight)
    red = loop.set_ci_red("a" * 40)
    assert red == {"sha": "a" * 40, "context": gh.LOOP_GATE_CONTEXT, "state": "failure"}
    method, path, body = actor.calls[0]
    assert (method, path) == ("POST", f"/repos/acme/fixture/statuses/{'a' * 40}")
    assert body["state"] == "failure" and body["context"] == gh.LOOP_GATE_CONTEXT
    merged = loop.merge(fe.ChangeRef(7, "https://github.com/acme/fixture/pull/7"))
    assert merged == {"status_code": 200, "merged": True, "method": "squash", "sha": "e" * 40}
    assert [c[2] for c in actor.calls[1:]] == [
        {"merge_method": "merge"},
        {"merge_method": "squash"},
    ]


def test_github_driver_opens_a_labelled_issue_and_returns_the_admitted_work_item(
    tmp_path: Path,
) -> None:
    gh = _github_entry()
    preflight = _preflight(tmp_path)
    admitted: list[tuple[int, float]] = []

    def assert_admission(number: int, since: float) -> None:
        admitted.append((number, since))
        preflight.evidence["work_item_id"] = "wi-17"

    preflight.open_labelled_issue = lambda: 17  # type: ignore[method-assign]
    preflight.assert_admission = assert_admission  # type: ignore[method-assign]
    driver = gh.GitHubDriver(preflight)
    assert driver.open_and_label_ticket("Title", "Body", 123.0) == "wi-17"
    assert preflight.issue_spec == ("Title", "Body")
    assert admitted == [(17, 123.0)]


# --- forge driver protocol ---------------------------------------------------------

FORGE_DIR = Path(__file__).resolve().parents[1] / "forges"
DRIVER_METHODS = (
    "open_and_label_ticket",
    "await_change",
    "change_head",
    "set_ci_red",
    "ci_state",
    "post_review",
    "merge",
    "change_merged",
    "changes_opened",
    "default_branch_head",
    "reset_fixture",
    "pin_fixture_base",
    "tracker_evidence",
    "judge_tracker",
    "teardowns",
)
# Every forge entry in forges/ is installed since GitLab's driver landed (#4399);
# the not-installed path is pinned on a reserved entry registered by the test.
RESERVED_FORGE = "reservedforge"


def _with_reserved_forge(monkeypatch: pytest.MonkeyPatch) -> None:
    fe.load_forge_entries()
    entry = fe.ForgeEntry(
        name=RESERVED_FORGE, driver=fe.not_installed_driver(RESERVED_FORGE), installed=False
    )
    monkeypatch.setattr(fe, "FORGES", {**fe.FORGES, RESERVED_FORGE: entry})


def test_base_defines_the_driver_protocol_without_importing_the_harness(
    tmp_path: Path,
) -> None:
    probe = (
        "import importlib.util, json, sys\n"
        "spec = importlib.util.spec_from_file_location('forge_base_probe', sys.argv[1])\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "sys.modules['forge_base_probe'] = module\n"
        "spec.loader.exec_module(module)\n"
        "ref = module.ChangeRef(7, 'https://forge.example/change/7')\n"
        "print(json.dumps({\n"
        "    'harness_imported': 'factory_e2e' in sys.modules,\n"
        "    'methods': sorted(n for n in dir(module.ForgeDriver) if not n.startswith('_')),\n"
        "    'not_installed_is_exception': issubclass(module.ForgeNotInstalled, Exception),\n"
        "    'change': [ref.number, ref.url],\n"
        "}))\n"
    )
    # -I with an unrelated cwd: an import of factory_e2e would fail outright.
    result = subprocess.run(
        [sys.executable, "-I", "-c", probe, str(FORGE_DIR / "base.py")],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    facts = json.loads(result.stdout)
    assert facts["harness_imported"] is False
    assert set(DRIVER_METHODS) <= set(facts["methods"])
    assert "open_ticket" not in facts["methods"] and "mark_ticket" not in facts["methods"]
    assert facts["not_installed_is_exception"] is True
    assert facts["change"] == [7, "https://forge.example/change/7"]


def test_the_harness_reuses_base_change_ref_and_drops_loop_driver() -> None:
    source = inspect.getsourcefile(fe.ChangeRef)
    assert source is not None and Path(source).resolve() == (FORGE_DIR / "base.py").resolve()
    assert not hasattr(fe, "LoopDriver")


def test_github_driver_implements_every_protocol_method() -> None:
    gh = _github_entry()
    assert not hasattr(gh, "GitHubLoop")
    for method in DRIVER_METHODS:
        assert callable(getattr(gh.GitHubDriver, method, None)), method


def test_github_driver_has_no_tracker_evidence_and_no_extra_teardown(tmp_path: Path) -> None:
    gh = _github_entry()
    assert gh.GitHubDriver.forge == "github"
    assert gh.GitHubDriver.pairing == "github+github"
    driver = gh.GitHubDriver(_preflight(tmp_path))
    change = fe.ChangeRef(7, "https://github.com/acme/fixture/pull/7")
    assert driver.tracker_evidence(change, "wi-1") == {}
    assert driver.tracker_evidence(None, "wi-1") == {}
    assert driver.judge_tracker({}) == []
    assert tuple(driver.teardowns()) == ()


def test_the_jira_driver_implements_every_protocol_method() -> None:
    fe.load_forge_entries()
    jira = sys.modules["factory_e2e_forges.jira_cloud"]
    assert jira.JiraCloudDriver.forge == "jira_cloud"
    assert jira.JiraCloudDriver.pairing == "jira_cloud+github"
    for method in DRIVER_METHODS:
        assert callable(getattr(jira.JiraCloudDriver, method, None)), method


# --- forge registry --------------------------------------------------------------


def test_forge_entries_register_github_jira_and_gitlab_installed() -> None:
    loaded = fe.load_forge_entries()
    for name in ("github", "jira_cloud", "gitlab"):
        assert name in loaded, name
        assert fe.FORGES[name].name == name
    assert fe.FORGES["github"].installed is True
    assert fe.FORGES["github"].driver is _github_entry().GitHubDriver
    assert fe.FORGES["github"].setup is None
    assert fe.FORGES["jira_cloud"].installed is True
    jira = sys.modules["factory_e2e_forges.jira_cloud"]
    assert fe.FORGES["jira_cloud"].setup is jira.install_jira
    gitlab = sys.modules["factory_e2e_forges.gitlab"]
    assert fe.FORGES["gitlab"].installed is True
    assert fe.FORGES["gitlab"].setup is gitlab.install_gitlab
    assert not [name for name, entry in fe.FORGES.items() if not entry.installed]


def test_a_not_installed_driver_raises_not_installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _with_reserved_forge(monkeypatch)
    with pytest.raises(Exception, match="not installed") as raised:
        fe.FORGES[RESERVED_FORGE].driver(_preflight(tmp_path))
    assert type(raised.value).__name__ == "ForgeNotInstalled"


def test_forge_loop_and_github_loop_are_both_scenarios() -> None:
    fe.load_forge_entries()
    for name in ("forge-loop", "github-loop"):
        assert name in fe.SCENARIO_NAMES
        assert callable(fe.SCENARIOS[name])
        assert callable(fe.resolve_scenario(name))


def test_a_new_entry_file_registers_without_shared_edits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fe, "FORGES", dict(fe.FORGES))
    monkeypatch.setattr(fe, "SCENARIOS", dict(fe.SCENARIOS))
    (tmp_path / "_helpers.py").write_text("raise AssertionError('private modules are skipped')\n")
    (tmp_path / "exampleforge.py").write_text(
        "import factory_e2e as fe\n"
        "fe.register_forge(fe.ForgeEntry(name='exampleforge', "
        "driver=lambda p: None, installed=True))\n"
    )
    monkeypatch.delitem(sys.modules, "factory_e2e_forges.exampleforge", raising=False)
    try:
        assert fe.load_forge_entries(tmp_path) == ["exampleforge"]
        assert fe.FORGES["exampleforge"].installed is True
        args = fe.parse_args(["run", "--scenario", "forge-loop", "--forge", "exampleforge"])
        assert (args.scenario, args.forge) == ("forge-loop", "exampleforge")
    finally:
        sys.modules.pop("factory_e2e_forges.exampleforge", None)


def test_forge_loop_resolves_the_github_forge() -> None:
    fe.load_forge_entries()
    args = fe.parse_args(["run", "--scenario", "forge-loop", "--forge", "github"])
    assert (args.scenario, args.forge) == ("forge-loop", "github")


# --- the loop goes through the driver only -----------------------------------------

# Every Preflight method that reads or writes GitHub directly. The loop must not
# touch any of them: it observes and mutates the forge only through its driver.
GITHUB_DIRECT = (
    "github",
    "as_actor",
    "as_app",
    "_paged",
    "check_app",
    "default_branch_head",
    "pin_fixture_base",
    "restore_fixture_base",
    "move_default_branch",
    "reset_fixture",
    "seed_failing_test_commit",
    "open_labelled_issue",
    "assert_admission",
    "label_admission_event_id",
    "await_delivery",
    "github_text_file",
    "pr_head",
    "new_branches",
    "remove_label",
    "post_pr_comment",
)


class _FakeDriver:
    """A ForgeDriver that records each call and serves a complete loop."""

    forge = "fakeforge"
    pairing = "faketracker+fakeforge"

    def __init__(self, _p: Any = None) -> None:
        self.calls: list[str] = []
        self.heads = iter([("a" * 40, 1), ("b" * 40, 2), ("d" * 40, 3)])

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        self.calls.append("open_and_label_ticket")
        return "wi-1"

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        self.calls.append("await_change")
        return fe.ChangeRef(7, "https://forge.example/change/7")

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        self.calls.append("change_head")
        return next(self.heads)

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        self.calls.append("set_ci_red")
        return {"sha": sha, "state": "failure"}

    def ci_state(self, sha: str) -> dict[str, Any]:
        self.calls.append("ci_state")
        return {"sha": sha, "state": "failing" if sha == "a" * 40 else "passing"}

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        self.calls.append("post_review")
        return {"id": 5, "delivery_status_code": 200, "delivery_api_status": "factory_admitted"}

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        self.calls.append("merge")
        return {"status_code": 200, "merged": True}

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        self.calls.append("change_merged")
        return True

    def changes_opened(self) -> list[int]:
        self.calls.append("changes_opened")
        return [7]

    def default_branch_head(self) -> str:
        self.calls.append("default_branch_head")
        return "f" * 40

    def reset_fixture(self) -> dict[str, Any]:
        self.calls.append("reset_fixture")
        return {"reset": "fakeforge"}

    def pin_fixture_base(self) -> None:
        self.calls.append("pin_fixture_base")

    def tracker_evidence(self, change: fe.ChangeRef | None, work_item_id: str) -> dict[str, Any]:
        self.calls.append("tracker_evidence")
        return {"key": "T-1", "status_comments": 1}

    def judge_tracker(self, evidence: Any) -> list[str]:
        self.calls.append("judge_tracker")
        return []

    def teardowns(self) -> tuple[tuple[str, Any], ...]:
        return (("fake tracker cleanup", lambda: {"cleaned": True}),)


def _forbid_github(preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in GITHUB_DIRECT:
        assert callable(getattr(fe.Preflight, name, None)), name

        def forbidden(*_a: Any, _name: str = name, **_k: Any) -> Any:
            raise AssertionError(f"the loop touched GitHub directly through Preflight.{_name}")

        monkeypatch.setattr(preflight, name, forbidden)

    def no_pr_numbers(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("the loop listed pull requests on GitHub directly")

    monkeypatch.setattr(fe, "_scenario_pr_numbers", no_pr_numbers)


def _loop_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, driver: _FakeDriver
) -> fe.Preflight:
    monkeypatch.setitem(
        fe.FORGES,
        "fakeforge",
        fe.ForgeEntry(name="fakeforge", driver=lambda _p: driver, installed=True),
    )
    preflight = _preflight(tmp_path, scenario_name="forge-loop")
    preflight.evidence["forge"] = "fakeforge"
    _forbid_github(preflight, monkeypatch)
    return preflight


def test_run_loop_observes_and_mutates_the_forge_only_through_the_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _FakeDriver()
    preflight = _loop_preflight(tmp_path, monkeypatch, driver)
    preflight.head_before = "f" * 40
    one = {"status": "completed", "started_at": "2026-10-11T00:00:00Z"}
    details = iter(
        [
            {"state": "published", "requests": [one], "pr": {"number": 7}},
            {"state": "published", "requests": [one, one], "pr": {"number": 7}},
        ]
    )
    merged = {"state": "published", "requests": [one, one], "pr": {"number": 7, "status": "merged"}}

    def work_item_detail(_work_item_id: str) -> dict[str, Any]:
        return next(details, merged)

    def no_cli_judgement(*_a: Any, **_k: Any) -> None:
        return None

    preflight.work_item_detail = work_item_detail  # type: ignore[method-assign]
    preflight.cli_check = no_cli_judgement  # type: ignore[method-assign]
    preflight.cli_not_found_check = no_cli_judgement  # type: ignore[method-assign]
    cli_body = json.dumps({"item": {"state": "published", "pr": {"number": 7, "status": "merged"}}})
    preflight.cli_work_items_raw = lambda _id: (0, cli_body)  # type: ignore[method-assign]
    # The pre-teardown reads (#4866) are not what this test observes.
    preflight.work_item_usage = lambda _id: {"cost_complete": True}  # type: ignore[method-assign]
    preflight.cli_cluster_status = lambda: (0, json.dumps({"credentials": []}))  # type: ignore[method-assign]

    def opened(title: str, body: str, since: float) -> str:
        preflight.evidence["work_item_id"] = "wi-1"
        return _FakeDriver.open_and_label_ticket(driver, title, body, since)

    driver.open_and_label_ticket = opened  # type: ignore[method-assign]
    monkeypatch.setattr(fe.time, "sleep", lambda _s: None)

    obs = fe.forge_loop(preflight)

    assert obs["verdict"] == "passed", obs["failures"]
    assert obs["forge"] == "fakeforge"
    assert obs["pairing"] == "faketracker+fakeforge"
    assert obs["tracker"] == {"key": "T-1", "status_comments": 1}
    assert driver.calls[:2] == ["open_and_label_ticket", "pin_fixture_base"]
    for method in ("set_ci_red", "post_review", "merge", "change_merged", "changes_opened"):
        assert method in driver.calls, method
    assert "default_branch_head" in driver.calls
    # Tracker evidence is read once, after the merge is observed, then judged.
    assert driver.calls.count("tracker_evidence") == 1
    assert driver.calls.index("tracker_evidence") > driver.calls.index("changes_opened")
    assert "judge_tracker" in driver.calls


def test_loop_records_usage_and_status_before_teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#4866: the loop records the per-run usage, the cluster status credentials
    and the work-items tracker link while the install is still up.

    The reads answer only through the port-forward the driver already holds
    and the release the CLI reads, so they happen after the last request ends
    and before ``main()`` tears the install down (its ``finally``), never
    after: the evidence carries the cost and the credential rows the
    acceptance table asserts.
    """

    driver = _FakeDriver()
    preflight = _loop_preflight(tmp_path, monkeypatch, driver)
    preflight.head_before = "f" * 40
    one = {"status": "completed", "started_at": "2026-10-11T00:00:00Z"}
    details = iter(
        [
            {"state": "published", "requests": [one], "pr": {"number": 7}},
            {"state": "published", "requests": [one, one], "pr": {"number": 7}},
        ]
    )
    merged = {"state": "published", "requests": [one, one], "pr": {"number": 7, "status": "merged"}}
    preflight.work_item_detail = lambda _id: next(details, merged)  # type: ignore[method-assign]
    preflight.cli_check = lambda *_a, **_k: None  # type: ignore[method-assign]
    preflight.cli_not_found_check = lambda *_a, **_k: None  # type: ignore[method-assign]
    tracker = {
        "kind": "github",
        "host": "https://forge.example",
        "scope_id": "acme/fixture",
        "issue_id": "7",
        "display_key": "acme/fixture#7",
        "url": "https://forge.example/change/7",
    }
    cli_body = json.dumps(
        {
            "item": {
                "state": "published",
                "pr": {"number": 7, "status": "merged"},
                "tracker": tracker,
            }
        }
    )
    work_items_calls: list[str] = []

    def cli_work_items_raw(work_item_id: str) -> tuple[int, str]:
        work_items_calls.append(work_item_id)
        return 0, cli_body

    preflight.cli_work_items_raw = cli_work_items_raw  # type: ignore[method-assign]
    usage = {"estimated_cost_usd": 0.42, "cost_complete": True, "requests_without_usage": 0}
    usage_calls: list[str] = []

    def work_item_usage(work_item_id: str) -> dict[str, Any]:
        usage_calls.append(work_item_id)
        return dict(usage)

    preflight.work_item_usage = work_item_usage  # type: ignore[method-assign]
    credentials = [
        {
            "section": "code_host",
            "name": "actor",
            "type": "pat",
            "authentication": "accepted",
            "expiry": "known",
            "expires_at": "2026-10-23T00:00:00Z",
        }
    ]
    status_calls: list[None] = []

    def cli_cluster_status() -> tuple[int, str]:
        status_calls.append(None)
        return 0, json.dumps({"credentials": credentials})

    preflight.cli_cluster_status = cli_cluster_status  # type: ignore[method-assign]
    teardown_runs: list[None] = []
    monkeypatch.setattr(
        fe.Teardown, "run", lambda *_a, **_k: (teardown_runs.append(None), [])[1]
    )

    def opened(title: str, body: str, since: float) -> str:
        preflight.evidence["work_item_id"] = "wi-1"
        return _FakeDriver.open_and_label_ticket(driver, title, body, since)

    driver.open_and_label_ticket = opened  # type: ignore[method-assign]
    monkeypatch.setattr(fe.time, "sleep", lambda _s: None)

    obs = fe.forge_loop(preflight)

    assert obs["verdict"] == "passed", obs["failures"]
    # The reads land in the step evidence, complete.
    assert obs["usage"] == usage
    assert obs["cluster_status"] == {"credentials": credentials}
    assert obs["work_items"] == [{"tracker": tracker}]
    assert preflight.evidence["scenario"] is obs
    # Each read names the loop's one work item.
    assert usage_calls == ["wi-1"]
    assert len(status_calls) == 1
    assert work_items_calls[-1] == "wi-1"
    # They happen before the teardown step: main() runs teardown only in its
    # finally, after the scenario returns, so nothing tore down while the
    # loop recorded them.
    assert teardown_runs == []


def test_preflight_setup_resets_and_baselines_a_loop_fixture_through_the_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _FakeDriver()
    preflight = _loop_preflight(tmp_path, monkeypatch, driver)
    # The install and webhook steps are not the forge boundary under test.
    for step in (
        "check_tools",
        "check_images",
        "sweep_stale_namespaces",
        "create_namespace",
        "install",
        "port_forward",
        "bind_agent",
        "ensure_label",
        "tunnel",
        "point_card_at_tunnel",
        "repoint_webhook",
    ):
        monkeypatch.setattr(preflight, step, lambda *_a, **_k: None)
    monkeypatch.setattr(preflight, "_lock", lambda *_a, **_k: None)
    # check_app reads the App; it is GitHub App setup, not the fixture.
    monkeypatch.setattr(preflight, "check_app", lambda *_a, **_k: None)
    seen: list[Any] = []

    preflight.run(lambda p: seen.append(p.loop_driver()) or {"ok": True})

    assert seen == [driver]
    assert driver.calls == ["reset_fixture", "default_branch_head"]
    assert preflight.head_before == "f" * 40
    names = [name for name, _step in preflight.teardown._steps]
    assert "reset fixture repository" in names
    reset_step = dict(preflight.teardown._steps)["reset fixture repository"]
    assert reset_step() == {"reset": "fakeforge"}
    # The driver's own teardowns are pushed right after the fixture reset.
    assert names[names.index("reset fixture repository") + 1] == "fake tracker cleanup"


def _setup_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, setup: Any
) -> tuple[fe.Preflight, list[str]]:
    driver = _FakeDriver()
    monkeypatch.setitem(
        fe.FORGES,
        "fakeforge",
        fe.ForgeEntry(name="fakeforge", driver=lambda _p: driver, installed=True, setup=setup),
    )
    preflight = _preflight(tmp_path, scenario_name="forge-loop")
    preflight.evidence["forge"] = "fakeforge"
    order: list[str] = []
    for step in (
        "check_tools",
        "check_images",
        "check_app",
        "sweep_stale_namespaces",
        "create_namespace",
        "install",
        "port_forward",
        "bind_agent",
        "ensure_label",
        "tunnel",
        "point_card_at_tunnel",
        "repoint_webhook",
    ):

        def stub(*_a: Any, _step: str = step, **_k: Any) -> None:
            order.append(_step)

        monkeypatch.setattr(preflight, step, stub)
    monkeypatch.setattr(preflight, "_lock", lambda *_a, **_k: None)
    preflight.run(lambda _p: {"ok": True})
    return preflight, order


def test_an_entry_setup_runs_after_the_port_forward_instead_of_bind_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[Any] = []

    def setup(p: fe.Preflight) -> None:
        seen.append(p)

    preflight, order = _setup_run(tmp_path, monkeypatch, setup)
    assert seen == [preflight]
    assert "bind_agent" not in order
    assert order.index("port_forward") < order.index("ensure_label")


def test_an_entry_without_setup_binds_the_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _preflight_run, order = _setup_run(tmp_path, monkeypatch, None)
    assert "bind_agent" in order
    assert order.index("port_forward") < order.index("bind_agent")


# --- main: forge-loop selection happens before any install ------------------------


class _Reached(Exception):
    """The run got as far as constructing the install."""


def _main_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, model_key: bool) -> list[Any]:
    for key, value in {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(_app_dir(tmp_path)),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example/factory",
    }.items():
        monkeypatch.setenv(key, value)
    if model_key:
        monkeypatch.setenv("CURIE_FACTORY_MODEL_API_KEY", "placeholder-model-key")
    else:
        monkeypatch.delenv("CURIE_FACTORY_MODEL_API_KEY", raising=False)
    monkeypatch.setattr(fe, "_resolve_candidate", lambda *_a, **_k: SHA)
    constructed: list[Any] = []

    def preflight(*args: Any, **kwargs: Any) -> Any:
        constructed.append(kwargs)
        raise _Reached

    monkeypatch.setattr(fe, "Preflight", preflight)
    return constructed


def test_forge_loop_without_a_forge_is_a_config_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    constructed = _main_env(tmp_path, monkeypatch, model_key=True)
    assert fe.main(["run", "--scenario", "forge-loop"]) == fe.EXIT_CONFIG
    assert "--forge" in capsys.readouterr().err
    assert constructed == []


def test_forge_loop_with_the_github_forge_reaches_the_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    constructed = _main_env(tmp_path, monkeypatch, model_key=True)
    with pytest.raises(_Reached):
        fe.main(["run", "--scenario", "forge-loop", "--forge", "github"])
    assert len(constructed) == 1


def test_github_loop_still_reaches_the_install_without_a_forge_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    constructed = _main_env(tmp_path, monkeypatch, model_key=True)
    with pytest.raises(_Reached):
        fe.main(["run", "--scenario", "github-loop"])
    assert len(constructed) == 1


def test_forge_loop_needs_a_model_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    constructed = _main_env(tmp_path, monkeypatch, model_key=False)
    assert fe.main(["run", "--scenario", "forge-loop", "--forge", "github"]) == fe.EXIT_CONFIG
    assert constructed == []


def test_a_not_installed_forge_is_refused_before_any_install(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _with_reserved_forge(monkeypatch)
    name = RESERVED_FORGE

    def no_config(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("load_config must not run for a forge that is not installed")

    def no_preflight(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("nothing may be installed for a forge that is not installed")

    monkeypatch.setattr(fe, "load_config", no_config)
    monkeypatch.setattr(fe, "Preflight", no_preflight)
    monkeypatch.setattr(fe, "_resolve_candidate", no_config)
    assert fe.main(["run", "--scenario", "forge-loop", "--forge", name]) == fe.EXIT_SCENARIO
    assert "not installed" in capsys.readouterr().err


# --- accept is gone ------------------------------------------------------------------


def test_accept_is_no_longer_a_mode() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(["accept"])
    with pytest.raises(SystemExit):
        fe.parse_args(["accept", "--forge", "github"])
