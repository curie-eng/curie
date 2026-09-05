"""Executable contract for deterministic end to end CI selection."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
# NOT `select.py`. Python puts a script's own directory first on sys.path, so a
# module named `select` shadows the stdlib one -- and this script imports
# subprocess, which imports selectors, which imports select. That made every
# test here fail on macOS while CI stayed green (#1878).
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
UPGRADE_MUTANT_BUILDER = REPO_ROOT / "charts" / "curie" / "ci" / "make-upgrade-mutants.py"

TIERS = ("skill", "local", "local-release", "cluster", "released-upgrade")
BASE_TIERS = TIERS[:-1]
OUTPUT_KEYS = {
    "skill": "skill",
    "local": "local",
    "local-release": "local_release",
    "cluster": "cluster",
    "released-upgrade": "released_upgrade",
}
APPROVED_ROOT_DOCS = (
    "AGENTS.md",
    "ARCHITECTURE.md",
    "CLAUDE.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "NOTICE",
    "QUICKSTART.md",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
    "TRADEMARKS.md",
    "llms.txt",
)


def _invoke_selector(
    tmp_path: Path,
    *paths: str,
    registry: Path = REGISTRY,
    push: bool = False,
    base: str | None = None,
    head: str | None = None,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], str]:
    output_path = tmp_path / f"github-output-{len(list(tmp_path.glob('github-output-*')))}"
    command = [sys.executable, str(SELECTOR), "--registry", str(registry)]
    if push:
        command.append("--push")
    if base is not None:
        command.extend(("--base", base))
    if head is not None:
        command.extend(("--head", head))
    for path in paths:
        command.extend(("--path", path))

    environment = os.environ.copy()
    environment["GITHUB_OUTPUT"] = str(output_path)
    completed = subprocess.run(
        command,
        cwd=cwd or tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = output_path.read_text() if output_path.exists() else ""
    return completed, output


def _expected_output(*selected: str, pytest_needed: bool = True) -> str:
    selected_tiers = set(selected)
    lines = [
        f"{OUTPUT_KEYS[tier]}={'true' if tier in selected_tiers else 'false'}"
        for tier in TIERS
    ]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected_tiers)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    return "\n".join(lines) + "\n"


def _assert_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
    *,
    pytest_needed: bool = True,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(*selected, pytest_needed=pytest_needed)


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        ("runner/example.py", BASE_TIERS),
        ("compose.dev.yaml", ("local",)),
        ("compose/generated.py", ("local-release",)),
        ("charts/curie/values.yaml", ("cluster", "released-upgrade")),
        ("apps/api/example.py", ("local", "local-release", "cluster")),
        ("apps/worker/example.py", ("local", "local-release", "cluster")),
        ("otel/collector.yaml", ("local", "local-release")),
        ("cli/example.rs", BASE_TIERS),
        ("cli/src/main.rs", BASE_TIERS),
        ("packages/example.py", BASE_TIERS),
        ("packages/aci-protocol/src/aci_protocol/wire.py", BASE_TIERS),
        ("packages/plugin-format/src/plugin_format/manifest.py", BASE_TIERS),
        ("pyproject.toml", BASE_TIERS),
        ("uv.lock", BASE_TIERS),
    ],
)
def test_registry_maps_each_known_surface(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


@pytest.mark.parametrize(
    "path",
    [
        ".github/e2e-selection.yaml",
        ".github/workflows/ci.yaml",
        "tools/e2e-ci-selection/select_tiers.py",
    ],
)
def test_enforcement_paths_select_every_tier(tmp_path: Path, path: str) -> None:
    _assert_selection(tmp_path, path, TIERS)


def test_weather_fixture_does_not_select_released_upgrade(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "examples/weather/evals/cases.json", BASE_TIERS)


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/values.yaml",
        "charts/curie/templates/secrets.yaml",
        "apps/api/src/curie_api/migrations/versions/example.py",
        "apps/worker/src/curie_worker/config.py",
        "apps/worker/src/curie_worker/approval_cards.py",
        "apps/worker/src/curie_worker/consumer_liveness.py",
        "apps/worker/src/curie_worker/workspace.py",
    ],
)
def test_released_upgrade_selects_upgrade_state_owners(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "true"


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/routers/agents.py",
        "apps/worker/src/curie_worker/binding.py",
        "apps/worker/src/curie_worker/state/config.py",
        "apps/ui/src/main.tsx",
        "docs/guides/getting-started.md",
    ],
)
def test_released_upgrade_does_not_select_unrelated_paths(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "false"


@pytest.mark.parametrize(
    "path",
    [*APPROVED_ROOT_DOCS, "docs/example.md", "docs/guides/getting-started.md"],
)
def test_genuine_documentation_only_selects_no_runtime_e2e_tiers(
    tmp_path: Path,
    path: str,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=False)


@pytest.mark.parametrize(
    ("path", "pytest_needed"),
    [
        ("apps/ui/package.json", True),
        ("apps/ui/pnpm-lock.yaml", True),
        ("apps/dispatcher/src/curie_dispatcher/app.py", True),
        ("scripts/README.md", False),
        ("scripts/check-docs.sh", False),
        ("scripts/check-pr-body.sh", False),
        (".github/workflows/pr-body.yaml", False),
        ("packages/test-support/src/curie_test_support/valkey.py", True),
        ("examples/coder/evals/cases.json", False),
    ],
)
def test_known_non_runtime_paths_select_no_e2e_tiers(
    tmp_path: Path,
    path: str,
    pytest_needed: bool,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=pytest_needed)


def test_unapproved_markdown_fallback_selects_all_base_tiers(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "UNAPPROVED.md", BASE_TIERS)


def test_charts_curie_still_selects_cluster(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "charts/curie/values.yaml")
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["cluster"] == "true"


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        (".github/e2e-selection.yaml", TIERS),
        (".github/workflows/ci.yaml", TIERS),
        (".github/workflows/README.md", BASE_TIERS),
        (".github/action.yml", BASE_TIERS),
        ("apps/api/README.md", ("local", "local-release", "cluster")),
        ("apps/api/runtime-config.yaml", ("local", "local-release", "cluster")),
        ("packages/plugin-format/README.md", BASE_TIERS),
        ("packages/plugin-format/plugin.yaml", BASE_TIERS),
        ("examples/weather/README.md", BASE_TIERS),
        ("examples/weather/skill-config.yaml", BASE_TIERS),
        ("tests/README.md", BASE_TIERS),
        ("tests/selector-config.yaml", BASE_TIERS),
        ("UNAPPROVED.md", BASE_TIERS),
    ],
)
def test_non_allowlisted_paths_never_bypass_runtime_e2e_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


def test_mixed_root_documentation_and_runtime_diff_selects_runtime_tiers(
    tmp_path: Path,
) -> None:
    completed, output = _invoke_selector(tmp_path, "ARCHITECTURE.md", "apps/api/main.py")
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output("local", "local-release", "cluster")


def test_unknown_and_union_selection_are_deterministic(tmp_path: Path) -> None:
    unknown, unknown_output = _invoke_selector(tmp_path, "new-surface/module.py")
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(*BASE_TIERS)

    forward, forward_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
        "compose.dev.yaml",
        "otel/collector.yaml",
    )
    reverse, reverse_output = _invoke_selector(
        tmp_path,
        "otel/collector.yaml",
        "compose.dev.yaml",
        "charts/curie/values.yaml",
    )
    assert forward.returncode == 0, forward.stderr
    assert reverse.returncode == 0, reverse.stderr
    assert forward_output == reverse_output == _expected_output(
        "local",
        "local-release",
        "cluster",
        "released-upgrade",
    )


def test_push_selects_every_tier_without_a_repository(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(*TIERS)


def test_revisions_select_changed_paths_and_unknown_fallback(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repository,
        check=True,
    )

    known_path = repository / "compose.dev.yaml"
    known_path.write_text("version: one\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    known_path.write_text("version: two\n")
    subprocess.run(["git", "commit", "-am", "known"], cwd=repository, check=True)
    known_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    known, known_output = _invoke_selector(
        tmp_path,
        base=base,
        head=known_head,
        cwd=repository,
    )
    assert known.returncode == 0, known.stderr
    assert known_output == _expected_output("local")

    (repository / "new-surface.txt").write_text("new\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "unknown"], cwd=repository, check=True)
    unknown_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    unknown, unknown_output = _invoke_selector(
        tmp_path,
        base=known_head,
        head=unknown_head,
        cwd=repository,
    )
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(*BASE_TIERS)


VALID_REGISTRY = """
version: 1
fallback: [skill, local, local-release, cluster]
rules:
  exact:
    compose.dev.yaml: [local]
    apps/worker/src/curie_worker/config.py: [released-upgrade]
  prefixes:
    charts: [cluster]
    charts/curie: [released-upgrade]
  ignored_prefixes:
    docs: []
"""


@pytest.mark.parametrize(
    "registry_text",
    [
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [unknown]"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [cluster, cluster]"),
        VALID_REGISTRY.replace("compose.dev.yaml: [local]", "compose.dev.yaml: []"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: []"),
        VALID_REGISTRY.replace("version: 1", "version: true"),
        VALID_REGISTRY.replace(
            "    charts: [cluster]",
            "    charts: [cluster]\n    charts: [local]",
        ),
        VALID_REGISTRY.replace("  exact:\n    compose.dev.yaml: [local]", "  exact: []"),
        VALID_REGISTRY.replace(
            "fallback: [skill, local, local-release, cluster]",
            "fallback: [skill, local]",
        ),
        VALID_REGISTRY.replace("    docs: []", "    charts: []"),
    ],
    ids=(
        "unknown_tier",
        "duplicate_tier",
        "empty_exact_tiers",
        "empty_prefix_tiers",
        "boolean_version",
        "duplicate_rule",
        "malformed_entry",
        "invalid_fallback",
        "ignored_overlap",
    ),
)
def test_selector_rejects_invalid_registries(
    tmp_path: Path,
    registry_text: str,
) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(registry_text)
    completed, _output = _invoke_selector(tmp_path, "charts/example.yaml", registry=registry)
    assert completed.returncode != 0


def test_more_specific_ignored_child_of_selected_prefix_is_allowed(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(
        VALID_REGISTRY.replace("    docs: []", "    docs: []\n    charts/ci: []")
    )
    ignored, ignored_output = _invoke_selector(
        tmp_path, "charts/ci/probe.sh", registry=registry
    )
    assert ignored.returncode == 0, ignored.stderr
    assert ignored_output == _expected_output(pytest_needed=False)

    selected, selected_output = _invoke_selector(
        tmp_path, "charts/curie/values.yaml", registry=registry
    )
    assert selected.returncode == 0, selected.stderr
    assert selected_output == _expected_output("cluster", "released-upgrade")


AGGREGATE_EXPRESSIONS = {
    "changes_result": "${{ needs.changes.result }}",
    "skill_selected": "${{ needs.changes.outputs.skill }}",
    "local_selected": "${{ needs.changes.outputs.local }}",
    "local_release_selected": "${{ needs.changes.outputs.local_release }}",
    "cluster_selected": "${{ needs.changes.outputs.cluster }}",
    "released_upgrade_selected": "${{ needs.changes.outputs.released_upgrade }}",
    "skill_local_result": "${{ needs.e2e-ladder.result }}",
    "local_release_result": "${{ needs.e2e-ladder-release.result }}",
    "cluster_result": "${{ needs.e2e-ladder-cluster.result }}",
    "released_upgrade_result": "${{ needs.e2e-released-upgrade.result }}",
}


def test_workflow_consumes_each_selection_output_exactly() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    assert jobs["changes"]["outputs"] == {
        "skill": "${{ steps.filter.outputs.skill }}",
        "local": "${{ steps.filter.outputs.local }}",
        "local_release": "${{ steps.filter.outputs.local_release }}",
        "cluster": "${{ steps.filter.outputs.cluster }}",
        "released_upgrade": "${{ steps.filter.outputs.released_upgrade }}",
        "skill_local_tiers": "${{ steps.filter.outputs.skill_local_tiers }}",
    }

    skill_local = jobs["e2e-ladder"]
    assert skill_local["if"] == (
        "${{ needs.changes.outputs.skill == 'true' || "
        "needs.changes.outputs.local == 'true' }}"
    )
    ladder_steps = [
        step
        for step in skill_local["steps"]
        if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(ladder_steps) == 1
    assert ladder_steps[0]["env"]["CURIE_E2E_TIERS"] == (
        "${{ needs.changes.outputs.skill_local_tiers }}"
    )

    assert jobs["e2e-ladder-release"]["if"] == (
        "${{ needs.changes.outputs.local_release == 'true' }}"
    )
    assert jobs["e2e-ladder-cluster"]["if"] == (
        "${{ needs.changes.outputs.cluster == 'true' }}"
    )
    assert jobs["e2e-released-upgrade"]["if"] == (
        "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    )


def test_released_upgrade_workflow_pins_issue_2194_runtime_contract() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    job = workflow["jobs"]["e2e-released-upgrade"]
    assert set(job["needs"]) == {"rust-build", "changes"}

    named_steps = {
        step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)
    }
    assert len(named_steps) == sum("name" in step for step in job["steps"])

    candidate_images = {
        "api": ("apps/api/Dockerfile", "curie-api:upgrade-candidate"),
        "dispatcher": (
            "apps/dispatcher/Dockerfile",
            "curie-dispatcher:upgrade-candidate",
        ),
        "worker": ("apps/worker/Dockerfile", "curie-worker:upgrade-candidate"),
        "ui": ("apps/ui/Dockerfile", "curie-ui:upgrade-candidate"),
        "runner": ("runner/Dockerfile", "curie-runner:upgrade-candidate"),
    }
    for component, (dockerfile, tag) in candidate_images.items():
        step = named_steps[f"Build the candidate {component} image locally"]
        assert step["uses"].startswith("docker/build-push-action@")
        assert step["with"]["context"] == "."
        assert step["with"]["file"] == dockerfile
        assert step["with"]["tags"] == tag
        assert step["with"]["push"] is False
        assert step["with"]["load"] is True

    load_run = named_steps["Load candidate images into the kind cluster"]["run"]
    for _component, (_dockerfile, tag) in candidate_images.items():
        assert f"  {tag} \\" in load_run or f"  {tag}; do" in load_run
    assert 'kind load docker-image "$image" --name curie-upgrade' in load_run

    download_run = named_steps["Download and verify the exact public v0.8.2 chart"][
        "run"
    ]
    assert (
        "https://github.com/curie-eng/curie/releases/download/"
        "v0.8.2/curie-0.8.2.tgz"
    ) in download_run
    assert 'sha256sum --check --strict' in download_run
    assert 'test "$(helm show chart "$released_chart"' in download_run
    assert ')" = "0.8.2"' in download_run

    fixture_run = named_steps["Write the legacy retained values fixture"]["run"]
    for exact_line in (
        "  placeholderText: legacy-retained",
        "    appToken: xapp-example-upgrade",
        "    botToken: xoxb-example-upgrade",
        '    mode: "off"',
        "placement: null",
    ):
        assert exact_line in fixture_run

    image_overrides = (
        "--set api.image.repository=curie-api --set api.image.tag=upgrade-candidate "
        "--set api.image.digest= --set api.image.pullPolicy=Never",
        "--set dispatcher.image.repository=curie-dispatcher "
        "--set dispatcher.image.tag=upgrade-candidate "
        "--set dispatcher.image.digest= --set dispatcher.image.pullPolicy=Never",
        "--set worker.image.repository=curie-worker "
        "--set worker.image.tag=upgrade-candidate "
        "--set worker.image.digest= --set worker.image.pullPolicy=Never",
        "--set ui.image.repository=curie-ui --set ui.image.tag=upgrade-candidate "
        "--set ui.image.digest= --set ui.image.pullPolicy=Never",
        "--set agentSandbox.runner.image=curie-runner "
        "--set agentSandbox.runner.tag=upgrade-candidate "
        "--set agentSandbox.runner.digest= "
        "--set agentSandbox.runner.imagePullPolicy=Never",
    )
    runner_prewarm_override = (
        "--set agentSandbox.runner.prewarm.imagePullPolicy=Never"
    )
    retained_projection = (
        "placeholderText: .dispatcher.placeholderText",
        "slackAppToken: .dispatcher.slack.appToken",
        "slackBotToken: .dispatcher.slack.botToken",
        "gvisorMode: .security.gvisor.mode",
        "placement: .placement",
    )

    install_run = named_steps["Install the exact public v0.8.2 release"]["run"]
    assert 'helm get values curie -n curie -o json' in install_run
    for projection in retained_projection:
        assert projection in install_run
    for predicate in (
        '.placeholderText == "legacy-retained"',
        '.slackAppToken == "xapp-example-upgrade"',
        '.slackBotToken == "xoxb-example-upgrade"',
        '.gvisorMode == "off"',
        ".placement == null",
    ):
        assert predicate in install_run

    first_upgrade = named_steps["Upgrade the legacy release to the candidate chart"][
        "run"
    ]
    second_upgrade = named_steps[
        "Upgrade a second time and preserve the generated attester"
    ]["run"]
    for upgrade, snapshot in (
        (first_upgrade, "first-retained-values.json"),
        (second_upgrade, "second-retained-values.json"),
    ):
        assert "helm upgrade curie charts/curie -n curie" in upgrade
        assert "--reset-then-reuse-values" in upgrade
        for override in image_overrides:
            assert override in upgrade
        assert runner_prewarm_override in upgrade
        assert 'helm get values curie -n curie -o json' in upgrade
        for projection in retained_projection:
            assert projection in upgrade
        assert snapshot in upgrade
        assert (
            f'cmp "$RUNNER_TEMP/retained-values.json" '
            f'"$RUNNER_TEMP/{snapshot}"'
        ) in upgrade
        assert "kubectl rollout status deploy/curie-api" in upgrade
        assert "kubectl rollout status deploy/curie-dispatcher" in upgrade

    assert (
        'printf \'%s\' "$first_attester" | sha256sum | cut -d\' \' -f1 '
        '> "$RUNNER_TEMP/first-attester.sha256"'
    ) in first_upgrade
    assert (
        'first_attester="$(cat "$RUNNER_TEMP/first-attester.sha256")"'
    ) in second_upgrade
    assert (
        'test "$(printf \'%s\' "$second_attester" | sha256sum | '
        'cut -d\' \' -f1)" = "$first_attester"'
    ) in second_upgrade

    verifier_step = named_steps["Write the managed attester verifier"]
    verifier_run = verifier_step["run"]
    assert 'test -n "$attester"' in verifier_run
    assert 'test "$attester" != "$api_key"' in verifier_run
    verifier_call = '"$RUNNER_TEMP/verify-managed-attester.sh"'
    assert verifier_call in first_upgrade

    negative_run = named_steps["Nil unsafe helper negative control"]["run"]
    placement_mutant_setup = '''placement_mutant="$RUNNER_TEMP/nil-unsafe-placement-chart"
cp -a charts/curie "$placement_mutant"'''
    assert placement_mutant_setup in negative_run
    assert "python3 charts/curie/ci/make-upgrade-mutants.py" in negative_run
    mutant_builder = UPGRADE_MUTANT_BUILDER.read_text()
    assert "match = block_re.search(text)" in mutant_builder
    assert 'if match is None or "| default dict" not in match.group(0):' in mutant_builder
    assert (
        'mutated = match.group(0).replace("| default dict", "", 1)'
    ) in mutant_builder
    assert 'start_marker = \'{{- define "curie.managedSecret" -}}\'' in mutant_builder
    placement_upgrade = '''if helm upgrade curie-negative "$placement_mutant" -n curie-negative \\
    --reset-then-reuse-values --timeout 15m; then
  echo "nil-unsafe placement mutant unexpectedly upgraded legacy placement:null values" >&2
  exit 1
fi'''
    assert placement_upgrade in negative_run
    placement_rejection = (
        'echo "Released-upgrade rung rejected the nil-unsafe placement mutant as expected"'
    )
    assert placement_rejection in negative_run
    managed_secret_mutation = (
        "kubectl patch secret curie-negative-secrets -n curie-negative --type merge"
    )
    assert negative_run.index(placement_upgrade) < negative_run.index(placement_rejection)
    assert negative_run.index(placement_rejection) < negative_run.index(
        managed_secret_mutation
    )
    assert verifier_call in negative_run
    assert f"if {verifier_call}" in negative_run
    assert 'test -z "$negative_attester"' in negative_run
    assert "unexpectedly passed" in negative_run

    health_run = named_steps["Require healthy API after the candidate upgrade"]["run"]
    assert "curl -fsS http://127.0.0.1:28000/health" in health_run
    assert 'grep -q \'"status":"ok"\'' in health_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    for override in image_overrides:
        assert override in smoke["run"]
    assert runner_prewarm_override in smoke["run"]

    existing_cluster_steps = jobs["e2e-ladder-cluster"]["steps"]
    existing_smoke = [
        step
        for step in existing_cluster_steps
        if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(existing_smoke) == 1
    assert existing_smoke[0]["env"]["CURIE_E2E_TIERS"] == "cluster"

    assert named_steps["Tear down the disposable upgrade cluster"]["if"] == "always()"


def test_released_upgrade_candidate_images_opt_into_forward_only_migrations() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["e2e-released-upgrade"]["steps"]
    named_steps = {
        step["name"]: step for step in steps if isinstance(step.get("name"), str)
    }

    candidate_upgrade_steps = (
        "Upgrade the legacy release to the candidate chart",
        "Upgrade a second time and preserve the generated attester",
        "Upgrade the v0.8.4 release to the candidate chart",
        "Existing cluster rung smoke on the upgraded candidate",
    )
    for name in candidate_upgrade_steps:
        run = named_steps[name]["run"]
        assert "helm upgrade curie charts/curie -n curie" in run
        assert "--set api.image.tag=upgrade-candidate" in run
        assert "--set api.migrate.forwardOnly=true" in run


def test_released_upgrade_workflow_pins_issue_2097_live_manifest_parity() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-released-upgrade"]
    named_steps = {
        step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)
    }

    download_run = named_steps["Download and verify the exact public v0.8.4 chart"][
        "run"
    ]
    assert (
        "https://github.com/curie-eng/curie/releases/download/"
        "v0.8.4/curie-0.8.4.tgz"
    ) in download_run
    assert (
        "fee20ab73c05d7a888165f980fb82d25150fcba509f19218bafc4c187a9044bb"
    ) in download_run
    assert 'sha256sum --check --strict' in download_run
    assert 'test "$(helm show chart "$released_chart_084"' in download_run
    assert ')" = "0.8.4"' in download_run

    fixture_run = named_steps["Write the v0.8.4 retained timeout values fixture"]["run"]
    for exact_line in (
        "worker:",
        "  extraEnv:",
        "    - name: CURIE_RUNNER_TOTAL_TIMEOUT_S",
        '      value: "1700"',
        '    mode: "off"',
    ):
        assert exact_line in fixture_run

    published_image_pins = (
        "--set api.image.tag=0.8.4 --set api.image.digest="
        "sha256:8804a35d0c96e9bb2ed0d4f6a990015aab9e9343f6c1ae0b5dc7307e37b50aaf",
        "--set dispatcher.image.tag=0.8.4 --set dispatcher.image.digest="
        "sha256:ab6766db1f7d211e86f6bd816bb5c73ebfe6ee6cbcdf3dc9de9f9a26848bef47",
        "--set worker.image.tag=0.8.4 --set worker.image.digest="
        "sha256:c3117c30ac0a4cdd626c1170a8c976911da601f55781c9ffea6f1d85d4f02656",
        "--set ui.image.tag=0.8.4 --set ui.image.digest="
        "sha256:7e22c984e53478df9d70e96ec1fd3c73601396a23b378f7b9483ef8bf498477b",
        "--set agentSandbox.runner.tag=0.8.4 --set agentSandbox.runner.digest="
        "sha256:4e19b285d4161d4667145ce9ea6e3efd11c1a9e22b1f1f2f49167994515b0b88",
    )
    install_run = named_steps["Install the exact public v0.8.4 release"]["run"]
    assert "helm install" in install_run
    for pin in published_image_pins:
        assert pin in install_run
    assert "$RELEASED_V084_VALUES" in install_run

    upgrade_run = named_steps["Upgrade the v0.8.4 release to the candidate chart"]["run"]
    assert "kubectl delete deploy/curie-worker -n curie" in upgrade_run
    assert "helm upgrade curie charts/curie -n curie" in upgrade_run
    assert "--reset-then-reuse-values" in upgrade_run
    assert (
        "--set worker.image.repository=curie-worker "
        "--set worker.image.tag=upgrade-candidate"
    ) in upgrade_run

    parity_run = named_steps[
        "Verify live-manifest parity and target-version convergence"
    ]["run"]
    assert "charts/curie/ci/live_manifest_parity.py" in parity_run
    assert "helm get manifest" in parity_run
    assert "kubectl get deploy" in parity_run
    assert "helm get metadata" in parity_run
    assert "charts/curie/Chart.yaml" in parity_run
    assert "CURIE_RUNNER_TOTAL_TIMEOUT_S" in parity_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    install_names = [step.get("name") for step in job["steps"]]
    assert install_names.index("Install the exact public v0.8.4 release") < (
        install_names.index("Existing cluster rung smoke on the upgraded candidate")
    )
    assert "Nil unsafe helper negative control" in named_steps

    helm_ci = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml").read_text()
    )
    helm_runs = "\n".join(
        step.get("run", "")
        for step in helm_ci["jobs"]["helm"]["steps"]
        if isinstance(step.get("run"), str)
    )
    assert "charts/curie/ci/live-manifest-parity-assertions.sh" in helm_runs


def _aggregate_contract() -> tuple[str, dict[str, str]]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-required"]
    assert job["name"] == "E2E required"
    assert set(job["needs"]) == {
        "changes",
        "e2e-ladder",
        "e2e-ladder-release",
        "e2e-ladder-cluster",
        "e2e-released-upgrade",
    }
    assert job["if"] == "${{ !cancelled() }}"

    candidates = [
        step
        for step in job["steps"]
        if isinstance(step, dict)
        and isinstance(step.get("run"), str)
        and isinstance(step.get("env"), dict)
        and AGGREGATE_EXPRESSIONS["changes_result"] in step["env"].values()
    ]
    assert len(candidates) == 1
    step = candidates[0]
    bindings: dict[str, str] = {}
    for semantic_name, expression in AGGREGATE_EXPRESSIONS.items():
        environment_names = [
            name for name, value in step["env"].items() if value == expression
        ]
        assert len(environment_names) == 1, expression
        bindings[semantic_name] = environment_names[0]
    return step["run"], bindings


def _run_aggregate(
    *,
    script_transform: Callable[[str], str] | None = None,
    **overrides: str,
) -> subprocess.CompletedProcess[str]:
    script, bindings = _aggregate_contract()
    if script_transform is not None:
        script = script_transform(script)
    state = {
        "changes_result": "success",
        "skill_selected": "false",
        "local_selected": "false",
        "local_release_selected": "false",
        "cluster_selected": "false",
        "released_upgrade_selected": "false",
        "skill_local_result": "skipped",
        "local_release_result": "skipped",
        "cluster_result": "skipped",
        "released_upgrade_result": "skipped",
    }
    state.update(overrides)
    environment = os.environ.copy()
    environment.update({bindings[name]: value for name, value in state.items()})
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_e2e_required_validates_docs_only_ladder_skips(tmp_path: Path) -> None:
    selected, output = _invoke_selector(tmp_path, "ARCHITECTURE.md")
    assert selected.returncode == 0, selected.stderr
    assert output == _expected_output(pytest_needed=False)
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())

    skipped = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
    )
    assert skipped.returncode == 0, skipped.stdout + skipped.stderr

    unexpected_result = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
        skill_local_result="success",
    )
    assert unexpected_result.returncode != 0


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"skill_selected": "true", "skill_local_result": "success"},
        {"local_selected": "true", "skill_local_result": "success"},
        {
            "skill_selected": "true",
            "local_selected": "true",
            "local_release_selected": "true",
            "cluster_selected": "true",
            "skill_local_result": "success",
            "local_release_result": "success",
            "cluster_result": "success",
        },
    ],
)
def test_aggregate_accepts_exact_selected_outcomes(state: dict[str, str]) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    "state",
    [
        {"changes_result": "failure"},
        {"skill_selected": "true", "skill_local_result": "skipped"},
        {"local_selected": "true", "skill_local_result": "cancelled"},
        {"local_release_selected": "true", "local_release_result": "failure"},
        {"cluster_selected": "true", "cluster_result": "skipped"},
        {"cluster_selected": "true", "cluster_result": "cancelled"},
        {"skill_local_result": "success"},
        {"local_release_result": "success"},
        {"cluster_result": "success"},
    ],
)
def test_aggregate_rejects_inconsistent_outcomes(state: dict[str, str]) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode != 0


def test_aggregate_negative_control_runs_before_real_results() -> None:
    completed = _run_aggregate()
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    negative_control = "Selected and skipped negative control passed"
    real_result = "E2E required passed"
    assert negative_control in output
    assert real_result in output
    assert output.index(negative_control) < output.index(real_result)


def test_negative_control_rejects_selected_skipped_outcome_when_validator_mutates() -> None:
    unmutated = _run_aggregate(
        skill_selected="true",
        skill_local_result="success",
    )
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    skill_result_check = '"$SKILL_LOCAL_RESULT" != "$skill_local_expected" ||'

    def accept_selected_skipped(script: str) -> str:
        assert script.count(skill_result_check) == 1
        return script.replace(skill_result_check, "", 1)

    completed = _run_aggregate(
        script_transform=accept_selected_skipped,
        skill_selected="true",
        skill_local_result="skipped",
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_selector_directory_has_no_stdlib_shadowing_modules() -> None:
    # Structural, not behavioral: on Linux `select` is a builtin module (its
    # __file__ is None), so a directly-executed script's own directory on
    # sys.path never wins and the shadow bug cannot be reproduced here. On
    # macOS `select` is a dynamic extension in lib-dynload, which loses to
    # sys.path[0] and crashes. See issue #1878.
    selector_dir = SELECTOR.parent
    for candidate in selector_dir.glob("*.py"):
        assert candidate.stem not in sys.stdlib_module_names, (
            f"{candidate.name} shadows the stdlib module '{candidate.stem}': "
            "Python puts a directly-executed script's own directory first on "
            "sys.path, so this basename shadows the stdlib module for every "
            "import in this process. That's harmless on platforms where the "
            "shadowed module is a builtin (e.g. Linux's `select`), but breaks "
            "the script on platforms where it's a dynamic extension instead "
            "(e.g. macOS's `select`), per issue #1878."
        )
