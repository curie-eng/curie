"""Unit contract for the candidate-image path, the full GitHub loop and the forge registry.

The live paths need a cluster, the dedicated test GitHub App and fixture
repository, so they are proved by running the command. These pin what must
hold before then: built images are what the install points at, a running pod
must carry the built digest, kind loads while other clusters push, the loop
judge names every shortfall, and the acceptance runner drives every entry the
forges directory registers.
"""

from __future__ import annotations

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
    assert values["api"]["githubFactoryIngressEnabled"] is True
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
    loop = gh.GitHubLoop(preflight)
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


# --- forge registry and acceptance runner -----------------------------------------


def test_forge_entries_register_github_and_its_loop_scenario() -> None:
    assert "github" in fe.load_forge_entries()
    entry = fe.FORGES["github"]
    assert entry.scenario == "github-loop"
    assert callable(fe.SCENARIOS["github-loop"])
    assert "github-loop" in fe.SCENARIO_NAMES


def test_a_new_entry_file_registers_without_shared_edits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fe, "FORGES", dict(fe.FORGES))
    monkeypatch.setattr(fe, "SCENARIOS", dict(fe.SCENARIOS))
    (tmp_path / "_helpers.py").write_text("raise AssertionError('private modules are skipped')\n")
    (tmp_path / "exampleforge.py").write_text(
        "import factory_e2e as fe\n"
        "fe.register_forge(fe.ForgeEntry(name='exampleforge', scenario='exampleforge-loop', "
        "driver=lambda p: None))\n"
    )
    monkeypatch.delitem(sys.modules, "factory_e2e_forges.exampleforge", raising=False)
    try:
        assert fe.load_forge_entries(tmp_path) == ["exampleforge"]
        assert fe.FORGES["exampleforge"].scenario == "exampleforge-loop"
        assert callable(fe.SCENARIOS["exampleforge-loop"])
        args = fe.parse_args(["accept", "--forge", "exampleforge"])
        assert args.forge == ["exampleforge"]
    finally:
        sys.modules.pop("factory_e2e_forges.exampleforge", None)


def test_accept_runs_every_registered_forge_with_the_shared_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        fe,
        "FORGES",
        {
            "github": fe.FORGES["github"],
            "other": fe.ForgeEntry("other", "other-loop", fe.FORGES["github"].driver),
        },
    )
    args = fe.parse_args(
        [
            "accept",
            "--context",
            "k8",
            "--candidate-images",
            "build",
            "--evidence",
            str(tmp_path / "accept.json"),
        ]
    )
    runs = fe.accept_runs(args)
    assert [name for name, _ in runs] == ["github", "other"]
    argv = dict(runs)["github"]
    assert argv[:3] == ["run", "--scenario", "github-loop"]
    assert argv[argv.index("--candidate-images") + 1] == "build"
    assert argv[argv.index("--context") + 1] == "k8"
    assert argv[argv.index("--evidence") + 1].startswith(str(tmp_path / "accept-github-"))
    assert dict(runs)["other"][2] == "other-loop"

    seen: list[list[str]] = []

    def run_one(run_argv: list[str]) -> int:
        seen.append(run_argv)
        return 0 if "github-loop" in run_argv else fe.EXIT_FAILED

    assert fe.accept(args, run_one=run_one) == fe.EXIT_FAILED
    assert len(seen) == 2
    summary = json.loads((tmp_path / "accept.json").read_text())
    assert summary["result"] == "failed"
    assert [(f["forge"], f["exit_code"]) for f in summary["forges"]] == [
        ("github", 0),
        ("other", fe.EXIT_FAILED),
    ]


def test_accept_with_no_entries_is_a_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fe, "FORGES", {})
    args = fe.parse_args(["accept"])
    assert fe.accept(args, run_one=lambda _argv: 0) == fe.EXIT_CONFIG
