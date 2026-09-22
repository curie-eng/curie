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

import pytest

import factory_e2e as fe

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
