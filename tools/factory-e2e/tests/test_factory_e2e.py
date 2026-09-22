"""Unit contract for the `curie dev factory-e2e` driver (#2966).

The live path needs a cluster, a GitHub App and a fixture repository, so it is
proved by running the command, not here. These tests pin the parts that must
hold before anything live is touched: credentials are named when missing,
scenario hooks refuse before an install, the App JWT verifies, the WorkItem
request id matches the api's derivation, delivery matching picks the labelled
issue, and teardown runs every step in reverse even after a failure.
"""

from __future__ import annotations

import base64
import json
import subprocess
import uuid
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _app_dir(tmp_path: Path, **overrides: Any) -> Path:
    app = tmp_path / "app"
    app.mkdir()
    meta = {"id": 42, "slug": "example-factory", "installation_id": 7, "repo": "acme/fixture"}
    meta.update(overrides)
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    return app


def _env(app: Path) -> dict[str, str]:
    return {
        "CURIE_FACTORY_KUBE_CONTEXT": "scratch",
        "CURIE_FACTORY_APP_DIR": str(app),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
    }


def _no_gh(user: str) -> str:
    raise AssertionError(f"gh must not be consulted for {user}")


def test_config_loads_from_app_dir(tmp_path: Path) -> None:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    assert config.kube_context == "scratch"
    assert config.app_id == "42"
    assert config.installation_id == 7
    assert config.repo == "acme/fixture"
    assert config.label == "curie-factory"
    assert config.mention == "example-factory"
    assert config.webhook_secret == "not-the-dev-default"


def test_env_overrides_app_dir_and_context_flag_wins(tmp_path: Path) -> None:
    env = _env(_app_dir(tmp_path))
    env.update(
        {
            "CURIE_FACTORY_REPO": "acme/other",
            "CURIE_FACTORY_LABEL": "factory",
            "CURIE_FACTORY_MENTION": "somebody",
        }
    )
    config = fe.load_config(env, context="flagged", gh_token=_no_gh)
    assert (config.kube_context, config.repo, config.label, config.mention) == (
        "flagged",
        "acme/other",
        "factory",
        "somebody",
    )


def test_missing_credentials_are_all_named(tmp_path: Path) -> None:
    with pytest.raises(fe.ConfigError) as refused:
        fe.load_config({}, context=None, gh_token=_no_gh)
    message = str(refused.value)
    for name in (
        "CURIE_FACTORY_KUBE_CONTEXT",
        "CURIE_FACTORY_APP_ID",
        "CURIE_FACTORY_INSTALLATION_ID",
        "CURIE_FACTORY_APP_PRIVATE_KEY_FILE",
        "CURIE_FACTORY_WEBHOOK_SECRET_FILE",
        "CURIE_FACTORY_REPO",
        "CURIE_FACTORY_ACTOR_TOKEN",
    ):
        assert name in message


def test_missing_key_file_is_refused_without_its_contents(tmp_path: Path) -> None:
    app = _app_dir(tmp_path)
    (app / "app.pem").unlink()
    with pytest.raises(fe.ConfigError) as refused:
        fe.load_config(_env(app), context=None, gh_token=_no_gh)
    assert "CURIE_FACTORY_APP_PRIVATE_KEY_FILE" in str(refused.value)


def test_actor_token_resolves_through_gh_user(tmp_path: Path) -> None:
    env = _env(_app_dir(tmp_path))
    del env["CURIE_FACTORY_ACTOR_TOKEN"]
    env["CURIE_FACTORY_ACTOR_GH_USER"] = "someone"
    seen: list[str] = []

    def gh(user: str) -> str:
        seen.append(user)
        return "from-gh"

    config = fe.load_config(env, context=None, gh_token=gh)
    assert config.actor_token == "from-gh"
    assert seen == ["someone"]


def test_secrets_stay_out_of_repr(tmp_path: Path) -> None:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    text = repr(config)
    assert "not-the-dev-default" not in text
    assert "actor-token" not in text


def test_cli_exits_2_with_clear_message_when_credentials_missing(tmp_path: Path) -> None:
    result = subprocess.run(
        ["python3", str(REPO_ROOT / "tools/factory-e2e/factory_e2e.py"), "preflight"],
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "missing required factory credential" in result.stderr
    assert "CURIE_FACTORY_APP_ID" in result.stderr


@pytest.mark.parametrize("name", fe.SCENARIO_NAMES)
def test_every_named_scenario_parses(name: str) -> None:
    args = fe.parse_args(["run", "--scenario", name])
    assert args.scenario == name


def test_unknown_scenario_is_rejected_by_the_parser() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(["run", "--scenario", "merge-it"])


def test_unwritten_scenario_refuses_before_config_is_read(tmp_path: Path) -> None:
    unwritten = [name for name in fe.SCENARIO_NAMES if fe.SCENARIOS[name] is None]
    assert unwritten, "every scenario has a driver; drop this test"
    result = subprocess.run(
        [
            "python3",
            str(REPO_ROOT / "tools/factory-e2e/factory_e2e.py"),
            "run",
            "--scenario",
            unwritten[0],
        ],
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 3
    assert unwritten[0] in result.stderr
    assert "missing required factory credential" not in result.stderr


def _b64(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def test_app_jwt_is_rs256_and_verifies(tmp_path: Path) -> None:
    app = _app_dir(tmp_path)
    token = fe.app_jwt("42", app / "app.pem", now=1_000_000)
    header, payload, signature = token.split(".")
    assert json.loads(_b64(header)) == {"alg": "RS256", "typ": "JWT"}
    claims = json.loads(_b64(payload))
    assert claims["iss"] == "42"
    assert claims["iat"] < 1_000_000 < claims["exp"] <= 1_000_000 + 600
    pub = tmp_path / "pub.pem"
    subprocess.run(
        ["openssl", "rsa", "-in", str(app / "app.pem"), "-pubout", "-out", str(pub)],
        check=True,
        capture_output=True,
    )
    sig = tmp_path / "sig"
    sig.write_bytes(_b64(signature))
    verified = subprocess.run(
        ["openssl", "dgst", "-sha256", "-verify", str(pub), "-signature", str(sig)],
        input=f"{header}.{payload}".encode(),
        capture_output=True,
    )
    assert verified.returncode == 0, verified.stderr


def test_request_id_matches_api_derivation() -> None:
    expected = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/factory/label/123/9")
    assert fe.request_id_for(123, 9) == expected


def _delivery(guid: str, number: int, repo: str, *, action: str = "labeled") -> dict[str, Any]:
    return {
        "guid": guid,
        "event": "issues",
        "action": action,
        "status_code": 200,
        "request": {"payload": {"issue": {"number": number}, "repository": {"full_name": repo}}},
        "response": {"payload": json.dumps({"status": "factory_admitted"})},
    }


def test_delivery_match_picks_the_labelled_issue() -> None:
    found = fe.match_delivery(
        [
            _delivery("a", 9, "acme/fixture", action="opened"),
            _delivery("b", 8, "acme/fixture"),
            _delivery("c", 9, "acme/other"),
            _delivery("d", 9, "acme/fixture"),
        ],
        issue_number=9,
        repo="acme/fixture",
    )
    assert found is not None and found["guid"] == "d"


def test_delivery_match_is_none_without_a_labelled_delivery() -> None:
    assert (
        fe.match_delivery(
            [_delivery("a", 9, "acme/fixture", action="opened")],
            issue_number=9,
            repo="acme/fixture",
        )
        is None
    )


def test_delivery_result_reads_the_api_status() -> None:
    assert fe.delivery_api_status(_delivery("d", 9, "acme/fixture")) == "factory_admitted"
    broken = _delivery("d", 9, "acme/fixture")
    broken["response"]["payload"] = "<html>bad gateway</html>"
    assert fe.delivery_api_status(broken) is None


def test_teardown_runs_in_reverse_and_survives_a_failing_step() -> None:
    order: list[str] = []
    teardown = fe.Teardown()
    teardown.push("first", lambda: order.append("first"))

    def broken() -> None:
        order.append("broken")
        raise RuntimeError("step failed")

    teardown.push("broken", broken)
    teardown.push("last", lambda: order.append("last"))
    results = teardown.run()
    assert order == ["last", "broken", "first"]
    assert [(r["step"], r["ok"]) for r in results] == [
        ("last", True),
        ("broken", False),
        ("first", True),
    ]
    assert "step failed" in results[1]["detail"]
    assert teardown.run() == []


def test_namespace_is_owned_and_rfc1123() -> None:
    assert fe.default_namespace("ABCDEF0123456789") == "test-factory-abcdef01"
    with pytest.raises(fe.ConfigError):
        fe.validate_namespace("default")
    with pytest.raises(fe.ConfigError):
        fe.validate_namespace("test-factory-Bad_Name")
    assert fe.validate_namespace("test-factory-x1") == "test-factory-x1"


def test_install_values_pin_every_image_and_enable_factory_ingress(tmp_path: Path) -> None:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    values = fe.install_values(
        config,
        candidate="c" * 40,
        app_key_secret="factory-app",
        consumer_controller=True,
    )
    tag = "sha-" + "c" * 40
    for component in ("api", "worker", "dispatcher", "mailAdapter", "ui"):
        assert values[component]["image"]["tag"] == tag
    assert values["agentSandbox"]["runner"]["tag"] == tag
    assert values["agentSandbox"]["controller"]["deploy"] is False
    api = values["api"]
    assert api["githubFactoryIngressEnabled"] is True
    assert api["githubFactoryLabel"] == "curie-factory"
    assert api["githubFactoryMention"] == "example-factory"
    assert api["githubAppId"] == "42"
    assert api["githubAppExistingSecret"] == "factory-app"
    assert api["githubRepoAllowlist"] == ["acme/fixture"]
    assert api["githubWebhookSecret"] == "not-the-dev-default"


def test_namespace_undo_is_registered_before_the_create_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    preflight = fe.Preflight(
        config,
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "e.json",
        admission_timeout=1,
    )
    calls: list[list[str]] = []

    def fake_run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        calls.append(argv)
        if "create" in argv:
            manifest = json.loads(input_text or "{}")
            assert manifest["metadata"]["annotations"][fe.RUN_ANNOTATION] == preflight.run_id
            raise fe.PreflightFailed("connection lost after the server applied it")
        return ""

    monkeypatch.setattr(fe, "run", fake_run)
    with pytest.raises(fe.PreflightFailed):
        preflight.create_namespace()
    assert [name for name, _ in preflight.teardown._steps] == ["delete namespaces"]


def test_existing_publication_namespace_refuses_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    preflight = fe.Preflight(
        config,
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "e.json",
        admission_timeout=1,
    )

    def fake_run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        if "test-factory-unit-curie-publication" in argv:
            return "namespace/test-factory-unit-curie-publication"
        return ""

    monkeypatch.setattr(fe, "run", fake_run)
    with pytest.raises(fe.ConfigError):
        preflight.create_namespace()
    assert preflight.teardown.run() == []


def test_a_second_run_on_the_same_app_is_refused_before_it_mutates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fe, "LOCK_DIR", tmp_path / "locks")
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)

    def make() -> fe.Preflight:
        return fe.Preflight(
            config,
            repo_root=REPO_ROOT,
            candidate="c" * 40,
            namespace="test-factory-unit",
            evidence_path=tmp_path / "e.json",
            admission_timeout=1,
        )

    first, second = make(), make()
    first._lock("app-42", "this App")
    with pytest.raises(fe.ConfigError, match="holds this App"):
        second._lock("app-42", "this App")
    assert first.teardown.run()[0]["ok"]
    second._lock("app-42", "this App")
    second.teardown.run()


def test_tunnel_url_skips_the_cloudflared_control_host() -> None:
    assert (
        fe.quick_tunnel_url("INF Requesting new quick Tunnel on https://api.trycloudflare.com...")
        is None
    )
    assert (
        fe.quick_tunnel_url("|  https://contribute-cookie-mode-newman.trycloudflare.com  |")
        == "https://contribute-cookie-mode-newman.trycloudflare.com"
    )


# --------------------------------------------------------------------------
# #2576: the default dark-factory bundle, a real model, and issue-to-pr.
# --------------------------------------------------------------------------


def _config(tmp_path: Path, **extra: str) -> Any:
    env = _env(_app_dir(tmp_path))
    env.update(extra)
    return fe.load_config(env, context=None, gh_token=_no_gh)


def test_defaults_name_the_model_and_the_bundle() -> None:
    assert fe.DEFAULT_MODEL == "z-ai/glm-5.3"
    assert fe.DEFAULT_BUNDLE == REPO_ROOT / "examples" / "dark-factory"


def test_config_defaults_model_bundle_and_curie_bin(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.model == fe.DEFAULT_MODEL
    assert config.bundle_dir == fe.DEFAULT_BUNDLE
    assert config.curie_bin == "curie"


def test_config_env_overrides_model_bundle_and_curie_bin(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    config = _config(
        tmp_path,
        CURIE_FACTORY_MODEL="vendor/other-model",
        CURIE_FACTORY_BUNDLE_DIR=str(bundle),
        CURIE_FACTORY_CURIE_BIN="/opt/bin/curie",
    )
    assert config.model == "vendor/other-model"
    assert isinstance(config.bundle_dir, Path)
    assert config.bundle_dir == bundle
    assert config.curie_bin == "/opt/bin/curie"


def test_bundle_dir_that_is_not_a_directory_is_refused(tmp_path: Path) -> None:
    with pytest.raises(fe.ConfigError, match="CURIE_FACTORY_BUNDLE_DIR"):
        _config(tmp_path, CURIE_FACTORY_BUNDLE_DIR=str(tmp_path / "missing"))


def test_model_key_is_read_and_kept_out_of_repr(tmp_path: Path) -> None:
    config = _config(tmp_path, CURIE_FACTORY_MODEL_API_KEY="model-key-value")
    assert config.model_api_key == "model-key-value"
    assert "model-key-value" not in repr(config)


def _values(config: Any) -> dict[str, Any]:
    return fe.install_values(
        config,
        candidate="c" * 40,
        app_key_secret="factory-app",
        consumer_controller=False,
        egress_cidrs=["1.2.3.4/32"],
    )


def test_install_values_without_a_model_key_stay_fake(tmp_path: Path) -> None:
    values = _values(_config(tmp_path))
    sandbox = values["agentSandbox"]
    assert "credentials" not in sandbox
    assert sandbox.get("fakeModel") is not False


def test_install_values_with_a_model_key_run_the_real_model(tmp_path: Path) -> None:
    config = _config(tmp_path, CURIE_FACTORY_MODEL_API_KEY="model-key-value")
    values = _values(config)
    sandbox = values["agentSandbox"]
    assert sandbox["fakeModel"] is False
    assert sandbox["model"] == config.model
    assert sandbox["credentials"] == "model-key-value"
    worker = values["worker"]
    assert worker["deliveryBudgetSeconds"] >= 1800
    assert worker["runnerTotalTimeoutSeconds"] >= 1800
    assert worker["runnerTotalTimeoutSeconds"] <= worker["deliveryBudgetSeconds"]
    assert {"cidr": "1.2.3.4/32", "ports": [{"protocol": "TCP", "port": 443}]} in values[
        "security"
    ]["networkPolicy"]["allowedEgress"]
    assert values["security"]["gvisor"]["mode"] == "off"
    tag = "sha-" + "c" * 40
    for component in ("api", "worker", "dispatcher", "mailAdapter", "ui"):
        assert values[component]["image"]["tag"] == tag
    assert sandbox["runner"]["tag"] == tag
    api = values["api"]
    assert api["githubFactoryIngressEnabled"] is True
    assert api["githubAppId"] == "42"
    assert api["githubAppExistingSecret"] == "factory-app"
    assert api["githubRepoAllowlist"] == ["acme/fixture"]


def test_issue_file_parses_title_and_body(tmp_path: Path) -> None:
    path = tmp_path / "issue.md"
    path.write_text("\n\n##  Add a greeting  \n\nThe body line.\n\n- criterion\n\n")
    assert fe.parse_issue_file(path) == ("Add a greeting", "The body line.\n\n- criterion")


@pytest.mark.parametrize("content", ["", "   \n\n", "# Only a title\n\n"])
def test_issue_file_without_a_body_is_refused(tmp_path: Path, content: str) -> None:
    path = tmp_path / "issue.md"
    path.write_text(content)
    with pytest.raises(fe.ConfigError):
        fe.parse_issue_file(path)


def test_missing_issue_file_is_refused(tmp_path: Path) -> None:
    with pytest.raises(fe.ConfigError):
        fe.parse_issue_file(tmp_path / "absent.md")


def _pr(**overrides: Any) -> dict[str, Any]:
    pr = {"number": 5, "files": ["src/app.py", "tests/test_app.py"], "diff": "+print('hi')\n"}
    pr.update(overrides)
    return pr


def _outcome(**overrides: Any) -> dict[str, Any]:
    outcome = {
        "terminal": True,
        "pull_requests": [_pr()],
        "terminus_comments": 0,
        "default_branch_moved": False,
        "elapsed_seconds": 900.0,
    }
    outcome.update(overrides)
    return outcome


def test_clean_single_pr_passes() -> None:
    assert fe.judge_outcome(_outcome(), "pr") == []


def test_non_terminal_run_fails() -> None:
    assert fe.judge_outcome(_outcome(terminal=False), "any")


def test_more_than_one_pr_fails() -> None:
    assert fe.judge_outcome(_outcome(pull_requests=[_pr(), _pr(number=6)]), "any")


def test_pr_and_comment_together_fail() -> None:
    assert fe.judge_outcome(_outcome(terminus_comments=1), "any")


def test_neither_pr_nor_comment_fails() -> None:
    assert fe.judge_outcome(_outcome(pull_requests=[], terminus_comments=0), "any")


def test_workflow_file_in_pr_fails() -> None:
    pr = _pr(files=["src/app.py", ".github/workflows/ci.yml"])
    assert fe.judge_outcome(_outcome(pull_requests=[pr]), "pr")


@pytest.mark.parametrize(
    "secret",
    [
        "ghp_" + "a1B2" * 9,
        "github_pat_" + "11ABCDEFG0" + "x" * 30,
        "ghs_" + "Z9y8" * 9,
        "sk-or-v1-" + "0f" * 32,
        "-----BEGIN RSA PRIVATE KEY-----",
    ],
)
def test_credential_in_diff_fails(secret: str) -> None:
    pr = _pr(diff=f"+TOKEN = '{secret}'\n")
    assert fe.judge_outcome(_outcome(pull_requests=[pr]), "pr")


def test_moved_default_branch_fails() -> None:
    assert fe.judge_outcome(_outcome(default_branch_moved=True), "pr")


def test_overrunning_the_bound_fails() -> None:
    assert fe.judge_outcome(_outcome(elapsed_seconds=2100.5), "pr")
    assert fe.judge_outcome(_outcome(elapsed_seconds=2100.0), "pr") == []


def test_expect_pr_requires_a_pr() -> None:
    comment = _outcome(pull_requests=[], terminus_comments=1)
    assert fe.judge_outcome(comment, "pr")


def test_expect_comment_requires_a_comment_and_no_pr() -> None:
    comment = _outcome(pull_requests=[], terminus_comments=1, ending_cause="no_pull_request")
    assert fe.judge_outcome(comment, "comment") == []
    assert fe.judge_outcome(_outcome(), "comment")


def test_expect_any_accepts_either() -> None:
    assert fe.judge_outcome(_outcome(), "any") == []
    comment = _outcome(pull_requests=[], terminus_comments=1, ending_cause="no_pull_request")
    assert fe.judge_outcome(comment, "any") == []


def test_unknown_expect_raises() -> None:
    with pytest.raises(ValueError):
        fe.judge_outcome(_outcome(), "merged")


def test_issue_to_pr_has_a_driver_and_the_rest_do_not() -> None:
    assert callable(fe.SCENARIOS["issue-to-pr"])
    for name, driver in fe.SCENARIOS.items():
        if name != "issue-to-pr":
            assert driver is None, name


def test_run_parses_issue_file_and_expect() -> None:
    args = fe.parse_args(
        ["run", "--scenario", "issue-to-pr", "--issue-file", "x.md", "--expect", "comment"]
    )
    assert args.issue_file == Path("x.md")
    assert args.expect == "comment"


def test_expect_defaults_to_any() -> None:
    args = fe.parse_args(["run", "--scenario", "issue-to-pr", "--issue-file", "x.md"])
    assert args.expect == "any"


def test_invalid_expect_is_rejected_by_the_parser() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(
            ["run", "--scenario", "issue-to-pr", "--issue-file", "x.md", "--expect", "merged"]
        )


def test_issue_to_pr_without_issue_file_refuses_before_any_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    env = _env(_app_dir(tmp_path))
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    def refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("must refuse before any subprocess or git call")

    monkeypatch.setattr(fe, "run", refuse)
    monkeypatch.setattr(fe, "_resolve_candidate", refuse)
    assert fe.main(["run", "--scenario", "issue-to-pr"]) == fe.EXIT_CONFIG
    assert "--issue-file" in capsys.readouterr().err


# --- review round: cause, uniqueness, .github, notice matching, elapsed ---


def _comment_ending(**overrides: Any) -> dict[str, Any]:
    base = {"pull_requests": [], "terminus_comments": 1, "ending_cause": "no_pull_request"}
    base.update(overrides)
    return _outcome(**base)


def test_expect_comment_refuses_a_runner_crash() -> None:
    assert fe.judge_outcome(_comment_ending(ending_cause="runner_failed"), "comment")
    assert fe.judge_outcome(_comment_ending(), "comment") == []


def test_expect_comment_honours_explicit_causes() -> None:
    crash = _comment_ending(ending_cause="runner_failed")
    assert fe.judge_outcome(crash, "comment", expect_causes={"runner_failed"}) == []
    assert fe.judge_outcome(_comment_ending(), "comment", expect_causes={"runner_failed"})


@pytest.mark.parametrize(
    "cause",
    [
        "runner_failed",
        "runner_escalated",
        "owner_lost",
        "capacity_wait_expired",
        "publication_failed",
    ],
)
def test_expect_any_refuses_failure_causes(cause: str) -> None:
    assert fe.judge_outcome(_comment_ending(ending_cause=cause), "any")


def test_expect_any_accepts_refusal_and_deadline() -> None:
    assert fe.judge_outcome(_comment_ending(), "any") == []
    assert fe.judge_outcome(_comment_ending(ending_cause="execution_deadline"), "any") == []
    assert fe.judge_outcome(_comment_ending(ending_cause=None), "any")


def test_expect_cause_parses_and_rejects_unknown() -> None:
    args = fe.parse_args(
        [
            "run",
            "--scenario",
            "issue-to-pr",
            "--issue-file",
            "x.md",
            "--expect-cause",
            "runner_failed",
            "--expect-cause",
            "no_pull_request",
        ]
    )
    assert set(args.expect_cause) == {"runner_failed", "no_pull_request"}
    with pytest.raises(SystemExit):
        fe.parse_args(
            ["run", "--scenario", "issue-to-pr", "--issue-file", "x.md", "--expect-cause", "x"]
        )


@pytest.mark.parametrize("expect", ["comment", "any"])
def test_two_terminus_comments_fail(expect: str) -> None:
    assert fe.judge_outcome(_comment_ending(terminus_comments=2), expect)


def test_any_dot_github_path_in_pr_fails() -> None:
    pr = _pr(files=["src/app.py", ".github/CODEOWNERS"])
    assert fe.judge_outcome(_outcome(pull_requests=[pr]), "pr")


_RID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _notice(cause: str, rid: uuid.UUID = _RID, **overrides: Any) -> dict[str, Any]:
    comment: dict[str, Any] = {
        "user": {"login": "factory[bot]", "type": "Bot"},
        "performed_via_github_app": {"id": 42},
        "created_at": "2026-01-01T00:10:00Z",
        "body": f"This factory run cannot continue.\nCause: {cause}\n\n"
        f"<!-- curie-execution-request:{rid} -->\n",
    }
    comment.update(overrides)
    return comment


def test_terminus_matcher_requires_app_author_and_marker() -> None:
    other_bot = _notice(
        "no_pull_request",
        user={"login": "other[bot]", "type": "Bot"},
        performed_via_github_app=None,
    )
    other_app = _notice(
        "no_pull_request", user={"login": "x", "type": "User"}, performed_via_github_app={"id": 7}
    )
    wrong_request = _notice("no_pull_request", rid=uuid.uuid4())
    no_marker = _notice("no_pull_request", body="This factory run cannot continue.")
    good = _notice("runner_failed")
    by_login = _notice("no_pull_request", performed_via_github_app=None)
    matched = fe.match_terminus_comments(
        [other_bot, other_app, wrong_request, no_marker, good, by_login],
        mention="factory",
        app_id="42",
        request_ids=[str(_RID)],
    )
    assert [m["cause"] for m in matched] == ["runner_failed", "no_pull_request"]
    assert matched[0]["created_at"] == "2026-01-01T00:10:00Z"


def test_elapsed_runs_to_the_observed_ending_not_terminal_at() -> None:
    request = {
        "started_at": "2026-01-01T00:00:00Z",
        "terminal_at": "2026-01-01T00:30:00Z",
    }
    elapsed, execution = fe.ending_times(request, labelled_at=0.0, ended_at="2026-01-01T00:38:20Z")
    assert elapsed == 2300.0
    assert execution == 1800.0
    assert fe.judge_outcome(_outcome(elapsed_seconds=elapsed), "pr")


def test_final_reply_is_the_last_turn_assistant_text() -> None:
    value = [
        {"type": "turn", "assistant": "first"},
        {"type": "summary", "text": "s"},
        {"type": "turn", "assistant": "x" * 5000},
    ]
    assert fe.final_agent_reply(value) == "x" * 4000
    assert fe.final_agent_reply([]) is None
    assert fe.final_agent_reply("junk") is None
