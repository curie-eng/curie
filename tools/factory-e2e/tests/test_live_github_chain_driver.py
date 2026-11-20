"""Offline proof of the live-github-chain driver and its hooks into factory_e2e (#5808).

Every test name carries its selector word: ``wiring`` (the production caller,
``main`` or ``Preflight.run``, drives the scenario), ``readiness`` (the blocker
check on real captured GraphQL answers), ``shape`` and ``handoff`` (a real
producer's output, or a documented GitHub response shape, fed through the
driver's builder into the judge, with a named counterexample).

GitHub shapes follow the REST docs cited beside each builder in
``live_github_chain.py``; identifiers are obviously fake.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import subprocess
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import factory_e2e as fe
import live_github_chain as lgc
import live_github_chain_judge as judge
import personal_profile_support as pps
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "live-github-chain"
READINESS = FIXTURES / "readiness"
PENDING = FIXTURES / "pending"
PASSING: dict[str, Any] = json.loads((FIXTURES / "record-pass.json").read_text())

REPO = "example-org/unitconv"
WRITER = {"login": "octo-writer", "id": 1001, "type": "User"}
READER = {"login": "octo-reader", "id": 1002, "type": "User"}
REPOSITORY = {
    "id": 2001,
    "full_name": REPO,
    "html_url": f"https://github.com/{REPO}",
    "default_branch": "main",
}
APP = {"login": "fixture-app[bot]", "id": 9000003, "type": "Bot"}
WRITER_PRINCIPAL = "00000000-0000-4000-8000-00000000000a"
READER_PRINCIPAL = "00000000-0000-4000-8000-00000000000b"
TENANT = "00000000-0000-4000-8000-0000000000aa"
AGENT = "00000000-0000-4000-8000-0000000000bb"


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def isolated_log_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fe, "LOG_FILTERS", [])


def _app_dir(tmp_path: Path) -> Path:
    app = tmp_path / "app"
    app.mkdir()
    meta = {"id": 4242, "slug": "fixture-app", "installation_id": 777, "repo": REPO}
    (app / "app.json").write_text(json.dumps(meta))
    subprocess.run(
        ["openssl", "genrsa", "-out", str(app / "app.pem"), "2048"],
        check=True,
        capture_output=True,
    )
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    return app


def _env(app: Path, **extra: str) -> dict[str, str]:
    return {
        "CURIE_FACTORY_KUBE_CONTEXT": "ctx-fake",
        "CURIE_FACTORY_APP_DIR": str(app),
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example.invalid:5000/factory",
        **extra,
    }


def _no_gh(user: str) -> str:
    raise AssertionError(f"gh must not be consulted for {user}")


def _preflight(tmp_path: Path, **options: Any) -> fe.Preflight:
    config = fe.load_config(_env(_app_dir(tmp_path)), context=None, gh_token=_no_gh)
    p = fe.Preflight(
        config,
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence" / "e.json",
        admission_timeout=1,
        scenario_name=lgc.SCENARIO,
    )
    p.scenario_options.update(options)
    return p


def _captured(name: str) -> Any:
    return json.loads((READINESS / name).read_text())


def _fake_graphql(
    blockers: dict[int, Any] | None = None,
) -> Callable[[str, str, Any], Any]:
    body = _captured("chain-issue-5808.json")
    nodes = blockers if blockers is not None else _captured("blockers-open.json")

    def query(_token: str, text: str, variables: Any) -> Any:
        if text == lgc.BODY_QUERY:
            return body
        return nodes[str(variables["number"])]

    return query


def _main_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **extra: str) -> None:
    for key, value in _env(_app_dir(tmp_path), **extra).items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_API_KEY", raising=False)
    monkeypatch.delenv(lgc.READER_ENV, raising=False)
    for key, value in extra.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(fe, "_repo_root", lambda: REPO_ROOT)
    monkeypatch.setattr(fe, "_resolve_candidate", lambda *a, **k: "c" * 40)
    monkeypatch.setattr(fe.signal, "signal", lambda *a: None)


class _Recorder:
    """Records every fe.run argv and every lock; refuses kubectl and helm."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.argv: list[list[str]] = []
        self.locks: list[str] = []
        monkeypatch.setattr(fe, "run", self.run)
        monkeypatch.setattr(fe.Preflight, "_lock", lambda _p, name, holds: self.lock(name, holds))

    def run(self, argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        self.argv.append(list(argv))
        return ""

    def lock(self, name: str, holds: str) -> None:
        self.locks.append(name)

    def cluster_calls(self) -> list[list[str]]:
        return [a for a in self.argv if a and a[0] in {"kubectl", "helm"}]


def _not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lgc, "graphql", _fake_graphql())
    monkeypatch.setattr(lgc, "git_ancestry", lambda _root, _candidate: lambda _oid: False)


def _github(
    monkeypatch: pytest.MonkeyPatch, *, open_items: list[dict[str, Any]] | None = None
) -> list[tuple[str, str]]:
    """The actor token's GitHub for ``main``: the three identity reads and the open items."""

    calls: list[tuple[str, str]] = []

    def as_actor(_self: fe.Preflight, method: str, path: str, body: Any = None) -> Any:
        calls.append((method, path))
        if method == "GET" and path == "/user":
            return 200, WRITER
        if method == "GET" and path == f"/user/{READER['id']}":
            return 200, READER
        if method == "GET" and path == f"/repos/{REPO}":
            return 200, {**REPOSITORY, "permissions": {"push": True}}
        if method == "GET" and path.startswith(f"/repos/{REPO}/issues?state=open&"):
            return 200, list(open_items or [])
        raise AssertionError(f"unexpected GitHub call {method} {path}")

    monkeypatch.setattr(fe.Preflight, "as_actor", as_actor)
    return calls


DEFAULT_NAMESPACE = fe.default_namespace("c" * 40)


# --------------------------------------------------------------------------
# wiring: main and Preflight.run
# --------------------------------------------------------------------------


def test_wiring_full_mode_unready_candidate_exits_config_before_any_cluster_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _main_env(
        monkeypatch,
        tmp_path,
        CURIE_FACTORY_MODEL_API_KEY="fake-mk",
        CURIE_FACTORY_READER_ACCOUNT_ID="1002",
    )
    _not_ready(monkeypatch)
    _github(monkeypatch)
    recorder = _Recorder(monkeypatch)
    evidence = tmp_path / "evidence.json"

    code = fe.main(["run", "--scenario", lgc.SCENARIO, "--evidence", str(evidence)])

    assert code == fe.EXIT_CONFIG
    assert recorder.cluster_calls() == []
    assert recorder.locks == []
    err = capsys.readouterr().err
    assert "#5788: no merged pull request into next closes it" in err
    written = json.loads(evidence.read_text())
    assert written["result"] == "refused"
    assert written["live_chain_readiness"]["passed"] is False


def test_wiring_stop_mode_records_the_unready_candidate_and_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    _not_ready(monkeypatch)
    _github(monkeypatch)
    _Recorder(monkeypatch)
    reached: list[str] = []

    def check_tools(_p: fe.Preflight) -> None:
        reached.append("check_tools")
        raise fe.PreflightFailed("stopped by the test after readiness")

    monkeypatch.setattr(fe.Preflight, "check_tools", check_tools)
    evidence = tmp_path / "evidence.json"

    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )

    assert reached == ["check_tools"]
    assert code == fe.EXIT_FAILED
    written = json.loads(evidence.read_text())
    assert written["live_chain_readiness"]["passed"] is False
    assert len(written["live_chain_readiness"]["missing"]) == 6
    assert any("readiness not met" in s["step"] for s in written["steps"])


def test_wiring_a_run_failing_before_drive_prints_and_writes_no_raw_identifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A live stop-mode run failed building the bundle's runner layer, after the
    # install and before drive: its logs named the kube context and the App id.
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    _not_ready(monkeypatch)
    _github(monkeypatch)
    _Recorder(monkeypatch)
    seen: dict[str, str] = {}

    def check_tools(p: fe.Preflight) -> None:
        seen["namespace"] = p.namespace
        fe.log(f"lock context-{p.config.kube_context} and app-{p.config.app_id} for {REPO}")
        raise fe.PreflightFailed(f"curie build failed in {p.namespace} on {p.config.kube_context}")

    monkeypatch.setattr(fe.Preflight, "check_tools", check_tools)
    evidence = tmp_path / "evidence.json"

    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )

    assert code == fe.EXIT_FAILED
    err = capsys.readouterr().err
    written = evidence.read_text()
    raw = [seen["namespace"], "ctx-fake", "4242", REPO, "fixture-app"]
    for text in (err, written):
        assert not [value for value in raw if value in text], text
    assert "lock context-<context> and app-<app> for <fixture-repo>" in err
    assert "curie build failed in <namespace> on <context>" in json.loads(written)["error"]
    assert len(fe.LOG_FILTERS) == 1


def test_wiring_stop_mode_refuses_a_labelled_open_issue_before_any_lock_or_cluster_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A leftover labelled issue could be admitted by poll intake between the
    # agent binding and the fixture reset, with the model key still installed.
    _main_env(
        monkeypatch,
        tmp_path,
        CURIE_FACTORY_MODEL_API_KEY="fake-mk",
        CURIE_FACTORY_READER_ACCOUNT_ID="1002",
    )
    _not_ready(monkeypatch)
    leftover = {"number": 5, "labels": [{"name": "factory"}], "user": WRITER}
    _github(monkeypatch, open_items=[leftover])
    recorder = _Recorder(monkeypatch)
    reached: list[str] = []
    monkeypatch.setattr(fe.Preflight, "check_tools", lambda _p: reached.append("check_tools"))
    evidence = tmp_path / "evidence.json"

    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )

    assert code == fe.EXIT_FAILED
    assert reached == [] and recorder.locks == [] and recorder.cluster_calls() == []
    assert "1 open issues or pull requests" in json.loads(evidence.read_text())["error"]


def test_wiring_an_empty_fixture_goes_on_past_the_open_item_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Liveness: an empty fixture repository proceeds to the shared preflight.
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    _not_ready(monkeypatch)
    calls = _github(monkeypatch, open_items=[])
    _Recorder(monkeypatch)
    reached: list[str] = []

    def check_tools(_p: fe.Preflight) -> None:
        reached.append("check_tools")
        raise fe.PreflightFailed("stopped by the test after the open item check")

    monkeypatch.setattr(fe.Preflight, "check_tools", check_tools)
    evidence = tmp_path / "evidence.json"
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )
    assert code == fe.EXIT_FAILED and reached == ["check_tools"]
    assert any(path.startswith(f"/repos/{REPO}/issues?state=open") for _m, path in calls)
    steps = [s["step"] for s in json.loads(evidence.read_text())["steps"]]
    assert "live chain fixture has no open item" in steps


def _install_through_main(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> list[tuple[str, dict[str, Any]]]:
    """Stub the cluster edges of Preflight.run so main runs the real install, then stop."""

    chart = tmp_path / "chart"
    (chart / "crds").mkdir(parents=True)
    for name in ("check_tools", "check_images", "check_app", "sweep_stale_namespaces"):
        monkeypatch.setattr(fe.Preflight, name, lambda _p: None)
    monkeypatch.setattr(fe.Preflight, "create_namespace", lambda _p: None)
    monkeypatch.setattr(fe.Preflight, "extract_chart", lambda _p: chart)
    monkeypatch.setattr(fe.Preflight, "existing_sandbox_controller", lambda _p: False)
    monkeypatch.setattr(fe.Preflight, "assert_running_images", lambda _p: None)
    monkeypatch.setattr(fe.Preflight, "egress_cidrs", lambda _p: [])
    monkeypatch.setattr(fe.Preflight, "kubectl", lambda _p, *_a, **_k: "")
    monkeypatch.setattr(fe.Preflight, "_lock", lambda *_a, **_k: None)

    def stop(_p: fe.Preflight) -> None:
        raise fe.PreflightFailed("stopped by the test after the install")

    monkeypatch.setattr(fe.Preflight, "port_forward", stop)
    helm_values: list[tuple[str, dict[str, Any]]] = []

    def fake_run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        if argv[0] == "helm":
            values_file = argv[argv.index("-f") + 1]
            helm_values.append((argv[3], json.loads(Path(values_file).read_text())))
        return ""

    monkeypatch.setattr(fe, "run", fake_run)
    return helm_values


def test_wiring_stop_mode_with_a_model_key_installs_the_fake_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _main_env(
        monkeypatch,
        tmp_path,
        CURIE_FACTORY_MODEL_API_KEY="fake-mk-stop-mode",
        CURIE_FACTORY_MODEL_BASE_URL="https://model-proxy.example.invalid/v1",
        CURIE_FACTORY_READER_ACCOUNT_ID="1002",
    )
    _not_ready(monkeypatch)
    _github(monkeypatch, open_items=[])
    helm_values = _install_through_main(monkeypatch, tmp_path)
    evidence = tmp_path / "evidence.json"

    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )

    assert code == fe.EXIT_FAILED
    assert [verb for verb, _ in helm_values] == ["install"]
    values = helm_values[0][1]
    text = json.dumps(values)
    assert "fake-mk-stop-mode" not in text and "model-proxy.example.invalid" not in text
    assert values["agentSandbox"]["runner"].get("fakeModel") is not False
    assert "credentials" not in values["agentSandbox"]["runner"]
    steps = {s["step"]: s for s in json.loads(evidence.read_text())["steps"]}
    assert steps["installed"]["model"] == "fake"
    assert steps["live chain stop mode installs the fake model"]["model_settings_dropped"]


def test_wiring_an_invalid_reader_id_writes_no_raw_namespace_or_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="abc")
    _Recorder(monkeypatch)
    evidence = tmp_path / "evidence.json"
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )
    assert code == fe.EXIT_CONFIG
    written = evidence.read_text()
    err = capsys.readouterr().err
    for text in (written, err):
        assert DEFAULT_NAMESPACE not in text and "ctx-fake" not in text, text
    parsed = json.loads(written)
    assert parsed["namespace"] == "<namespace>" and parsed["kube_context"] == "<context>"
    assert lgc.READER_ENV in parsed["error"]


def test_wiring_a_failure_right_after_check_app_writes_and_prints_no_raw_identifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _main_env(
        monkeypatch,
        tmp_path,
        CURIE_FACTORY_READER_ACCOUNT_ID="1002",
        CURIE_FACTORY_OPERATOR_LOGIN="octo-operator",
    )
    _not_ready(monkeypatch)
    _github(monkeypatch, open_items=[])
    _Recorder(monkeypatch)
    for name in ("check_tools", "check_images"):
        monkeypatch.setattr(fe.Preflight, name, lambda _p: None)
    installation = {"id": 777, "permissions": {"checks": "read", "statuses": "read"}}
    monkeypatch.setattr(
        fe.Preflight, "as_app", lambda _p, method, path, body=None: (200, installation)
    )

    def sweep(p: fe.Preflight) -> None:
        # check_app ran for real: it read the repository id and the actor login.
        raise fe.PreflightFailed(
            f"sweep failed for repository {p.repository_id} by {p.evidence['actor_login']} "
            f"in {p.namespace} on {p.config.kube_context} for App {p.config.app_id}"
        )

    monkeypatch.setattr(fe.Preflight, "sweep_stale_namespaces", sweep)
    evidence = tmp_path / "evidence.json"
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )

    assert code == fe.EXIT_FAILED
    written = evidence.read_text()
    err = capsys.readouterr().err
    raw = ["2001", "octo-writer", DEFAULT_NAMESPACE, "ctx-fake", "4242"]
    for text in (written, err):
        assert not [value for value in raw if value in text], text
    parsed = json.loads(written)
    assert parsed["actor_login"] == "<writer>"
    assert parsed["fixture_repository_id"] == "<fixture-repo>"
    assert "repository <fixture-repo> by <writer> in <namespace> on <context>" in parsed["error"]


def test_wiring_a_serializer_refusal_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    monkeypatch.setattr(fe, "SCENARIO_HOOKS", {})

    def refuse(_evidence: dict[str, Any]) -> str:
        raise judge.EvidenceLeak([("writer", "$/steps[0]")])

    def run(self: fe.Preflight, _driver: Any) -> None:
        self.evidence_serializer = refuse

    monkeypatch.setattr(fe.Preflight, "run", run)
    evidence = tmp_path / "e.json"
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )
    assert code == fe.EXIT_FAILED
    written = json.loads(evidence.read_text())
    assert written["result"] == "failed" and "refused" in written["error"]


def _stub_preflight_steps(
    p: fe.Preflight, monkeypatch: pytest.MonkeyPatch, order: list[str]
) -> None:
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
        "record_baseline",
    ):

        def stub(*_a: Any, _step: str = step, **_k: Any) -> None:
            order.append(_step)

        monkeypatch.setattr(p, step, stub)
    monkeypatch.setattr(p, "_lock", lambda *_a, **_k: None)
    monkeypatch.setattr(p, "reset_fixture", lambda: {"reset": True})


def test_wiring_preflight_run_never_opens_a_seed_issue_for_the_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _preflight(tmp_path)
    order: list[str] = []
    _stub_preflight_steps(p, monkeypatch, order)
    monkeypatch.setattr(lgc, "before_run", lambda _p: order.append("before_run"))
    monkeypatch.setattr(lgc, "install_values", lambda _p: lambda values: values)

    def seed(*_a: Any, **_k: Any) -> int:
        raise AssertionError("the chain opens its own tickets")

    monkeypatch.setattr(p, "open_labelled_issue", seed)
    monkeypatch.setattr(p, "assert_admission", seed)

    p.run(lambda _p: {"ran": True})

    assert order[0] == "before_run"
    assert order.index("create_namespace") < order.index("install")
    assert p.evidence["scenario"] == {"ran": True}


def _oidc_entries(values: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        e
        for e in values.get("api", {}).get("extraEnv") or []
        if str(e.get("name", "")).startswith("CURIE_OIDC_")
    ]


def test_wiring_install_and_card_upgrade_values_both_carry_the_oidc_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _preflight(tmp_path)
    order: list[str] = []
    _stub_preflight_steps(p, monkeypatch, order)
    # install and the card upgrade run for real; only their cluster edges are fakes.
    for real in ("install", "point_card_at_tunnel"):
        monkeypatch.setattr(p, real, getattr(fe.Preflight, real).__get__(p))
    chart = tmp_path / "chart"
    (chart / "crds").mkdir(parents=True)
    monkeypatch.setattr(p, "extract_chart", lambda: chart)
    monkeypatch.setattr(p, "existing_sandbox_controller", lambda: False)
    monkeypatch.setattr(p, "assert_running_images", lambda: None)
    monkeypatch.setattr(p, "egress_cidrs", lambda: [])
    monkeypatch.setattr(p, "ensure_api", lambda: None)
    monkeypatch.setattr(p, "ensure_agent_pool", lambda: None)
    monkeypatch.setattr(p, "kubectl", lambda *_a, **_k: "")
    monkeypatch.setattr(lgc, "before_run", lambda _p: None)
    helm_values: list[tuple[str, dict[str, Any]]] = []
    created: list[str] = []

    def fake_run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        if argv[0] == "helm":
            values_file = argv[argv.index("-f") + 1]
            helm_values.append((argv[3], json.loads(Path(values_file).read_text())))
        elif input_text is not None:
            created.append(json.loads(input_text)["kind"])
        return ""

    monkeypatch.setattr(fe, "run", fake_run)

    p.run(lambda _p: {})

    assert [verb for verb, _ in helm_values] == ["install", "upgrade"]
    issuer = f"http://{lgc.DEX_NAME}.test-factory-unit.svc.cluster.local:5556/dex"
    for _verb, values in helm_values:
        entries = {e["name"]: e for e in _oidc_entries(values)}
        assert set(entries) == {
            "CURIE_OIDC_ISSUER",
            "CURIE_OIDC_AUDIENCE",
            "CURIE_OIDC_JWKS_URL",
            "CURIE_OIDC_CLIENT_SECRET",
            "CURIE_OIDC_REDIRECT_URI",
            "CURIE_OIDC_SCOPES",
            "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED",
        }
        assert entries["CURIE_OIDC_ISSUER"]["value"] == issuer
        assert entries["CURIE_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"] == {
            "name": lgc.OIDC_SECRET_NAME,
            "key": lgc.OIDC_SECRET_KEY,
        }
        assert entries["CURIE_OIDC_ADMIT_ALL_AUTHENTICATED"]["value"] == "true"
    assert created == ["Secret", "List"]
    assert lgc.DEX_STEP in [name for name, _ in p.teardown._steps]


def test_wiring_oidc_values_replace_old_entries_and_touch_nothing_else() -> None:
    base = {
        "api": {
            "extraEnv": [
                {"name": "KEEP", "value": "1"},
                {"name": "CURIE_OIDC_ISSUER", "value": "old"},
            ]
        },
        "worker": {"extraEnv": [{"name": "W", "value": "2"}]},
    }
    result = lgc.oidc_values(
        base,
        issuer="http://dex.ns.svc.cluster.local:5556/dex",
        redirect_uri="http://api.ns.svc.cluster.local:8000/console/oidc/callback",
    )
    names = [e["name"] for e in result["api"]["extraEnv"]]
    assert names.count("CURIE_OIDC_ISSUER") == 1 and names[0] == "KEEP"
    assert result["worker"] == base["worker"]
    assert base["api"]["extraEnv"][1]["value"] == "old"  # the input is not changed
    assert "FACTORY_MODEL_AUTH_MODE" not in json.dumps(result)


EXPECTED_SEED = {
    "issue-to-pr": True,
    "revision": True,
    "cancel-waiting": True,
    "cancel-running": True,
    "evaluation": False,
    "quiesce": True,
    "forge-loop": False,
    "github-loop": False,
    "tls-code-host": False,
    "repo-runner": False,
    "docker": False,
    "live-github-chain": False,
}


def test_wiring_seed_issue_table_covers_every_scenario() -> None:
    assert set(fe.SCENARIO_NAMES) == set(EXPECTED_SEED)
    assert {name: fe.scenario_opens_seed_issue(name) for name in fe.SCENARIO_NAMES} == (
        EXPECTED_SEED
    )
    assert callable(fe.SCENARIOS[lgc.SCENARIO])
    assert "live-github-chain opens its own" in (fe.scenario_opens_seed_issue.__doc__ or "")


def test_wiring_module_docstring_describes_the_scenario() -> None:
    doc = fe.__doc__ or ""
    paragraph = doc[doc.index("`live-github-chain`") :].split("\n\n")[0]
    for needle in ("--stop-after links", "--live-chain-attempt", "CURIE_FACTORY_READER_ACCOUNT_ID"):
        assert needle in paragraph


@pytest.mark.parametrize(
    ("flag", "value"), [("--stop-after", "links"), ("--live-chain-attempt", "2")]
)
def test_wiring_chain_flags_are_refused_for_another_scenario(
    flag: str, value: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("refused before config or any action")

    monkeypatch.setattr(fe, "load_config", refuse)
    monkeypatch.setattr(fe.Preflight, "run", refuse)
    assert fe.main(["run", "--scenario", "cancel-waiting", flag, value]) == fe.EXIT_CONFIG
    assert flag in capsys.readouterr().err


def test_wiring_attempt_flag_accepts_only_one_or_two() -> None:
    assert (
        fe.parse_args(
            ["run", "--scenario", lgc.SCENARIO, "--live-chain-attempt", "2"]
        ).live_chain_attempt
        == 2
    )
    with pytest.raises(SystemExit):
        fe.parse_args(["run", "--scenario", lgc.SCENARIO, "--live-chain-attempt", "3"])


def test_wiring_full_mode_needs_a_model_key_and_stop_mode_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    ran: list[dict[str, Any]] = []
    monkeypatch.setattr(
        fe.Preflight, "run", lambda self, _driver: ran.append(dict(self.scenario_options))
    )
    evidence = str(tmp_path / "e.json")
    assert fe.main(["run", "--scenario", lgc.SCENARIO, "--evidence", evidence]) == fe.EXIT_CONFIG
    assert "CURIE_FACTORY_MODEL_API_KEY" in capsys.readouterr().err
    assert ran == []
    # Liveness: the stop mode needs no model key; its options reach the Preflight.
    monkeypatch.setattr(fe, "SCENARIO_HOOKS", {})
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", evidence]
    )
    assert code == 0
    assert ran == [{"stop_after": "links", "attempt": 1}]


@pytest.mark.parametrize("value", [None, "", "abc", "12a", "0", "-5", "1e3"])
def test_wiring_reader_id_missing_or_not_numeric_is_refused_before_readiness(
    value: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    extra = {} if value is None else {lgc.READER_ENV: value}
    _main_env(monkeypatch, tmp_path, **extra)
    recorder = _Recorder(monkeypatch)

    def no_query(*_a: Any) -> Any:
        raise AssertionError("readiness must not run without a reader id")

    monkeypatch.setattr(lgc, "graphql", no_query)
    code = fe.main(
        [
            "run",
            "--scenario",
            lgc.SCENARIO,
            "--stop-after",
            "links",
            "--evidence",
            str(tmp_path / "e.json"),
        ]
    )
    assert code == fe.EXIT_CONFIG
    assert lgc.READER_ENV in capsys.readouterr().err
    assert recorder.cluster_calls() == []


def _actor_api(open_items: list[dict[str, Any]]) -> Callable[..., tuple[int, Any]]:
    def as_actor(method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if path == "/user":
            return 200, WRITER
        if path == f"/user/{READER['id']}":
            return 200, READER
        if path == f"/repos/{REPO}":
            return 200, REPOSITORY
        raise AssertionError(f"unexpected GitHub call {method} {path}")

    return as_actor


def _chain_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    open_items: list[dict[str, Any]],
    stop: bool = True,
) -> fe.Preflight:
    p = _preflight(tmp_path, stop_after="links" if stop else None, attempt=1)
    chain = lgc.chain_of(p)
    chain.reader_id = READER["id"]
    chain.dex = {
        "issuer": "http://live-chain-dex.test-factory-unit.svc.cluster.local:5556/dex",
        "redirect_uri": "http://unit-curie-api.test-factory-unit.svc.cluster.local:8000/console/oidc/callback",
    }
    p.api_url = "http://127.0.0.1:18000"
    p.api_key = "fake-pk"
    p.evidence["agent_id"] = AGENT
    monkeypatch.setattr(p, "as_actor", _actor_api(open_items))
    monkeypatch.setattr(p, "_paged", lambda path: list(open_items))
    monkeypatch.setattr(
        lgc,
        "cluster_cli",
        lambda _p, _args, **_k: (
            0,
            json.dumps({"items": [], "limit": 50, "truncated": False}),
            0.1,
        ),
    )
    return p


def test_wiring_an_open_pull_request_stops_the_run_before_any_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    open_pull = {
        "number": 3,
        "pull_request": {"url": f"https://api.github.com/repos/{REPO}/pulls/3"},
    }
    p = _chain_preflight(tmp_path, monkeypatch, open_items=[open_pull])

    def no_login() -> Any:
        raise AssertionError("no OIDC login before the first check passes")

    monkeypatch.setattr(lgc, "make_opener", no_login)
    monkeypatch.setattr(p, "kubectl", lambda *_a, **_k: no_login())

    with pytest.raises(fe.PreflightFailed, match="open issue or pull request"):
        lgc.drive(p)
    assert lgc.chain_of(p).record["preflight"]["open_items"] == [
        {"number": 3, "pull_request": True}
    ]


def _session_answers(kind: str) -> dict[str, tuple[int, Any]]:
    from curie_api.routers.console import _operator_session
    from curie_api.schemas.console import ConsoleSessionOut, ConsoleSessionPrincipal

    expires = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
    answers: dict[str, tuple[int, Any]] = {}
    for role, principal in (("writer", WRITER_PRINCIPAL), ("reader", READER_PRINCIPAL)):
        if kind == "operator":
            session = _operator_session(f"operator:{role}", expires)
        else:
            session = ConsoleSessionOut(
                subject=f"principal:{principal}",
                expires_at=expires,
                kind="principal",
                principal=ConsoleSessionPrincipal(
                    id=uuid.UUID(principal), display_name=role, email=f"{role}@example.com"
                ),
            )
        answers[role] = (200, session.model_dump(mode="json"))
    return answers


def _drive_to_links(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, kind: str, revoke_status: int = 200
) -> tuple[fe.Preflight, list[tuple[str, str, Any]]]:
    from curie_api.schemas.agent_access import AgentAccessPolicyOut
    from curie_api.schemas.console import PrincipalOut

    p = _chain_preflight(tmp_path, monkeypatch, open_items=[])
    monkeypatch.setattr(p, "kubectl", lambda *_a, **_k: "")
    monkeypatch.setattr(lgc, "_dex_port_forward", lambda _p: 15556)
    monkeypatch.setattr(lgc, "make_opener", lambda: "opener")
    held = {"writer@example.com": "writer-session", "reader@example.com": "reader-session"}
    monkeypatch.setattr(
        lgc.pto,
        "oidc_login",
        lambda _opener, **kw: pps.HeldSecret(held[kw["email"]]),
    )
    sessions = _session_answers(kind)
    calls: list[tuple[str, str, Any]] = []

    def api(method: str, path: str, *, headers: Any, body: Any = None) -> tuple[int, Any]:
        calls.append((method, path, body))
        cookie = headers.get("Cookie", "")
        role = "writer" if "writer-session" in cookie else "reader"
        principal = WRITER_PRINCIPAL if role == "writer" else READER_PRINCIPAL
        if path == "/console/session":
            return sessions[role]
        if path == "/console/principal":
            if kind == "operator":
                return 401, {"detail": "a principal session is required"}
            out = PrincipalOut(
                id=uuid.UUID(principal),
                tenant_id=uuid.UUID(TENANT),
                idp_subject=role,
                display_name=role,
                email=f"{role}@example.com",
                status="active",
            )
            return 200, out.model_dump(mode="json")
        if path.endswith("/access-policy"):
            policy = AgentAccessPolicyOut(
                agent_id=uuid.UUID(AGENT),
                tenant_id=uuid.UUID(TENANT),
                invocation_team_id=None,
                approver_team_id=None,
                version=0,
                updated_at=None,
            )
            return 200, policy.model_dump(mode="json")
        if path.endswith("/code-host-links"):
            return 201, {"link_id": str(uuid.uuid4()), "identity_namespace_id": str(uuid.uuid4())}
        if path.endswith("/revoke"):
            if revoke_status != 200:
                return revoke_status, {"detail": "unavailable"}
            return 200, {"link_id": path.split("/")[-2], "revoked_at": "2030-01-01T00:00:00Z"}
        raise AssertionError(f"unexpected API call {method} {path}")

    monkeypatch.setattr(p, "api", api)
    return p, calls


def test_wiring_an_operator_session_stops_before_any_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, calls = _drive_to_links(tmp_path, monkeypatch, kind="operator")
    with pytest.raises(fe.PreflightFailed, match="not a principal session"):
        lgc.drive(p)
    assert not [c for c in calls if c[0] == "POST"]
    # The session files exist only for the CLI and are private.
    for path in lgc.chain_of(p).session_files.values():
        assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(("revoke_status", "passed"), [(503, False), (200, True)])
def test_wiring_a_failed_link_revoke_fails_finish(
    revoke_status: int, passed: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, calls = _drive_to_links(tmp_path, monkeypatch, kind="principal", revoke_status=revoke_status)
    evidence = lgc.drive(p)
    assert evidence["stop_after"] == "links" and evidence["links"] == 2
    links = [c for c in calls if c[1].endswith("/code-host-links")]
    assert [c[2]["account_id"] for c in links] == ["1001", "1002"]
    assert {c[2]["host"] for c in links} == {"github.com"}
    results = p.teardown.run()
    # passed=True is the liveness case: verified revokes and deleted session files pass.
    assert lgc.finish(p, results) is passed
    teardown = next(b for b in p.evidence["live_chain"]["beats"] if b["beat"] == "teardown")
    assert teardown["passed"] is passed
    assert all(not path.exists() for path in lgc.chain_of(p).session_files.values())


def test_wiring_a_crashed_finish_fails_main(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _main_env(monkeypatch, tmp_path, CURIE_FACTORY_READER_ACCOUNT_ID="1002")
    monkeypatch.setattr(fe.Preflight, "run", lambda self, _driver: None)
    monkeypatch.setitem(
        fe.SCENARIO_HOOKS, lgc.SCENARIO, fe.ScenarioHooks(finish=lambda _p, _r: False)
    )
    evidence = tmp_path / "e.json"
    code = fe.main(
        ["run", "--scenario", lgc.SCENARIO, "--stop-after", "links", "--evidence", str(evidence)]
    )
    assert code == fe.EXIT_FAILED
    assert json.loads(evidence.read_text())["result"] == "failed"


RAW_VALUES = [
    "test-factory-unit",
    "ctx-fake",
    REPO,
    "unitconv",
    "octo-writer",
    "octo-reader",
    "1001",
    "1002",
    "2001",
    "registry.example.invalid",
]


def test_wiring_written_evidence_holds_placeholders_and_no_raw_identifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _chain_preflight(tmp_path, monkeypatch, open_items=[])
    lgc.stage_identities(p, lgc.chain_of(p))
    p.step("pushed", url=f"https://github.com/{REPO}/pull/12", actor="octo-writer", id=1001)
    p.evidence["reader"] = {"login": "octo-reader", "id": 1002}
    p.write_evidence()
    text = p.evidence_path.read_text()
    assert not [raw for raw in RAW_VALUES if raw in text], text
    for placeholder in ("<namespace>", "<context>", "<fixture-repo>", "<writer>", "<reader>"):
        assert placeholder in text
    assert json.loads(text)["release"] == "<namespace>"


def test_wiring_log_lines_are_sanitized_once_the_filter_is_installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    p = _chain_preflight(tmp_path, monkeypatch, open_items=[])
    fe.log("before: test-factory-unit")
    assert "test-factory-unit" in capsys.readouterr().err
    lgc.stage_identities(p, lgc.chain_of(p))
    fe.log(f"namespace test-factory-unit on ctx-fake for {REPO} by octo-writer")
    err = capsys.readouterr().err
    assert "<namespace> on <context> for <fixture-repo> by <writer>" in err
    assert not [raw for raw in RAW_VALUES if raw in err]
    assert fe.filtered("ctx-fake") == "<context>"


def test_wiring_identities_refuse_the_actor_as_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _chain_preflight(tmp_path, monkeypatch, open_items=[])
    lgc.chain_of(p).reader_id = WRITER["id"]
    monkeypatch.setattr(
        p,
        "as_actor",
        lambda m, path, b=None: (200, WRITER) if "user" in path else (200, REPOSITORY),
    )
    with pytest.raises(fe.PreflightFailed, match="the reader account is the actor"):
        lgc.stage_identities(p, lgc.chain_of(p))
    assert fe.LOG_FILTERS == []


# --------------------------------------------------------------------------
# wiring: the beats of the paid path, each gated before its next side effect
# --------------------------------------------------------------------------


class _Opened(Exception):
    """Raised by the fake open_and_label: a ticket would have been opened."""


def _beat_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[fe.Preflight, lgc.ChainRun, list[str]]:
    p = _chain_preflight(tmp_path, monkeypatch, open_items=[], stop=False)
    chain = lgc.chain_of(p)
    chain.record = {
        key: copy.deepcopy(value)
        for key, value in PASSING.items()
        if not key.startswith("beat_") and key not in ("usage", "teardown")
    }
    chain.session_files = {role: tmp_path / f"{role}.session" for role in lgc.ROLES}
    monkeypatch.setattr(p, "model_usage", lambda: 10.0)
    opened: list[str] = []

    def open_and_label(_p: Any, _chain: Any, fixture: str, _label: str) -> Any:
        opened.append(fixture)
        raise _Opened(fixture)

    monkeypatch.setattr(lgc, "open_and_label", open_and_label)
    return p, chain, opened


def _overrides(proposals: Any) -> str:
    output = json.loads((PENDING / "overrides-response.json").read_text())["output"]
    return json.dumps({**output, "memory_rule_proposals": proposals})


def _fake_cli(monkeypatch: pytest.MonkeyPatch, answer: Callable[[list[str]], Any]) -> None:
    monkeypatch.setattr(lgc, "cluster_cli", lambda _p, args, **_k: answer(list(args)))


@pytest.mark.parametrize("read", [_overrides(False), _overrides(None), "not json"])
def test_wiring_beat_a_opens_no_issue_unless_the_proposals_read_shows_true(
    read: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, chain, opened = _beat_preflight(tmp_path, monkeypatch)
    _fake_cli(monkeypatch, lambda args: (0, "{}" if "on" in args else read, 0.1))
    with pytest.raises(lgc.ChainStopped, match="rule proposals on"):
        lgc.beat_a(p, chain)
    assert opened == []


@pytest.mark.parametrize("usage", [None, float("nan"), float("inf")])
def test_wiring_beat_a_opens_no_issue_without_a_finite_usage_baseline(
    usage: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, chain, opened = _beat_preflight(tmp_path, monkeypatch)
    monkeypatch.setattr(p, "model_usage", lambda: usage)
    _fake_cli(monkeypatch, lambda args: (0, "{}" if "on" in args else _overrides(True), 0.1))
    with pytest.raises(lgc.ChainStopped, match="could not be read at before_a"):
        lgc.beat_a(p, chain)
    assert opened == []


def test_wiring_beat_a_opens_ticket_a_with_proposals_on_and_a_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Liveness: proposals read true and a finite baseline open ticket A.
    p, chain, opened = _beat_preflight(tmp_path, monkeypatch)
    _fake_cli(monkeypatch, lambda args: (0, "{}" if "on" in args else _overrides(True), 0.1))
    with pytest.raises(_Opened):
        lgc.beat_a(p, chain)
    assert opened == ["issue-a.md"]
    assert chain.record["usage"]["samples"] == [{"label": "before_a", "usage": 10.0}]


@pytest.mark.parametrize(("exit_code", "opens"), [(1, False), (0, True)])
def test_wiring_a_failed_instruction_write_never_opens_ticket_b(
    exit_code: int, opens: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # exit 0 is the liveness case.
    p, chain, opened = _beat_preflight(tmp_path, monkeypatch)
    _fake_cli(monkeypatch, lambda args: (exit_code, "{}", 0.1))
    with pytest.raises(_Opened if opens else lgc.ChainStopped):
        lgc.beat_f(p, chain)
    assert opened == (["issue-b.md"] if opens else [])


def _removal(state: str) -> str:
    rule_id = PASSING["beat_e"]["rule_before"]["id"]
    return json.dumps({"rule": {"id": rule_id, "state": state}, "applies_from": "next_ticket"})


@pytest.mark.parametrize(
    ("removal", "opens"),
    [((1, "{}"), False), ((0, _removal("accepted")), False), ((0, _removal("removed")), True)],
)
def test_wiring_a_failed_removal_never_opens_ticket_c(
    removal: tuple[int, str],
    opens: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The removed state is the liveness case.
    p, chain, opened = _beat_preflight(tmp_path, monkeypatch)
    chain.record["beat_e"] = copy.deepcopy(PASSING["beat_e"])
    rule_id = PASSING["beat_e"]["rule_before"]["id"]
    listing = json.dumps({"rules": [{"id": rule_id, "version": 2, "state": "accepted"}]})

    def answer(args: list[str]) -> Any:
        if "--remove" in args:
            return (*removal, 0.2)
        return 0, listing, 0.1

    _fake_cli(monkeypatch, answer)
    with pytest.raises(_Opened if opens else lgc.ChainStopped):
        lgc.beat_g(p, chain)
    assert opened == (["issue-c.md"] if opens else [])


def _recorded_beat(name: str, section: Any = None) -> Callable[[Any, Any], None]:
    def beat(_p: Any, chain: lgc.ChainRun) -> None:
        chain.record[name] = copy.deepcopy(PASSING[name] if section is None else section)

    return beat


@pytest.mark.parametrize("beat_c_passes", [False, True])
def test_wiring_a_failed_beat_c_never_merges(
    beat_c_passes: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # beat_c_passes=True is the liveness case: a passing beat (c) reaches the merge.
    p, chain, _opened = _beat_preflight(tmp_path, monkeypatch)
    failing = {**copy.deepcopy(PASSING["beat_c"]), "pull_comments": []}
    monkeypatch.setattr(lgc, "beat_a", _recorded_beat("beat_a"))
    monkeypatch.setattr(lgc, "beat_b", _recorded_beat("beat_b"))
    monkeypatch.setattr(lgc, "beat_c", _recorded_beat("beat_c", None if beat_c_passes else failing))
    monkeypatch.setattr(p, "default_branch_head", lambda: "a" * 40)
    _fake_cli(monkeypatch, lambda args: (0, json.dumps({"rules": []}), 0.1))
    calls: list[tuple[str, str]] = []

    def as_actor(method: str, path: str, body: Any = None) -> Any:
        calls.append((method, path))
        raise _Opened("merge")

    monkeypatch.setattr(p, "as_actor", as_actor)
    with pytest.raises(_Opened if beat_c_passes else lgc.ChainStopped) as raised:
        lgc.run_beats(p, chain)
    merges = [c for c in calls if c[0] == "PUT" and c[1].endswith("/merge")]
    assert len(merges) == (1 if beat_c_passes else 0)
    if not beat_c_passes:
        assert "beat_c failed" in str(raised.value)
        assert "no App reply on the pull request" in str(raised.value)


def test_wiring_a_missing_usage_sample_after_a_beat_stops_before_the_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, chain, _opened = _beat_preflight(tmp_path, monkeypatch)
    chain.record["usage"] = {"samples": [{"label": "before_a", "usage": 10.0}]}
    monkeypatch.setattr(p, "model_usage", lambda: None)

    def beat_a(_p: Any, run: lgc.ChainRun) -> None:
        run.record["beat_a"] = copy.deepcopy(PASSING["beat_a"])
        lgc.sample_usage(p, run, "after_a")

    started: list[str] = []
    monkeypatch.setattr(lgc, "beat_a", beat_a)
    monkeypatch.setattr(lgc, "beat_b", lambda *_a: started.append("beat_b"))
    with pytest.raises(lgc.ChainStopped, match="could not be read at after_a"):
        lgc.run_beats(p, chain)
    assert started == []
    # The spend beat of the verdict fails on the missing sample.
    assert not judge.judge_beat("spend", chain.record).passed


@pytest.mark.parametrize(("read_back", "ok"), [("b" * 40, False), ("a" * 40, True)])
def test_wiring_the_default_branch_restore_raises_when_the_head_does_not_read_back(
    read_back: str, ok: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ok=True is the liveness case: the branch moves back and reads back.
    p, chain, _opened = _beat_preflight(tmp_path, monkeypatch)
    for beat in ("beat_a", "beat_c"):
        chain.record[beat] = copy.deepcopy(PASSING[beat])
    heads = iter(["a" * 40, "b" * 40, read_back])
    monkeypatch.setattr(p, "default_branch_head", lambda: next(heads))
    moved: list[str] = []
    monkeypatch.setattr(p, "move_default_branch", moved.append)

    def stop(_p: Any) -> Any:
        raise lgc.ChainStopped("stopped by the test after the restore was pushed")

    monkeypatch.setattr(lgc, "_rules", stop)
    with pytest.raises(lgc.ChainStopped):
        lgc.beat_d(p, chain)
    results = p.teardown.run()
    step = next(r for r in results if r["step"] == lgc.DEFAULT_BRANCH_STEP)
    assert moved == ["a" * 40]
    assert step["ok"] is ok
    assert lgc.teardown_section(results, namespace_gone=True)["verified"] is ok


@pytest.mark.parametrize(
    ("delete_status", "read_status", "ok"), [(500, 404, False), (204, 200, False), (204, 404, True)]
)
def test_wiring_the_label_undo_raises_unless_the_label_reads_back_gone(
    delete_status: int,
    read_status: int,
    ok: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # (204, 404) is the liveness case.
    p, chain, _opened = _beat_preflight(tmp_path, monkeypatch)
    p.run_id = "run1"

    def as_actor(method: str, path: str, body: Any = None) -> Any:
        if method == "POST":
            return 201, {"name": "factory-unbound-run1"}
        if method == "DELETE":
            return delete_status, None
        return read_status, {"name": "factory-unbound-run1"}

    monkeypatch.setattr(p, "as_actor", as_actor)
    with pytest.raises(_Opened):
        lgc.beat_h(p, chain)
    results = p.teardown.run()
    step = next(r for r in results if r["step"] == lgc.LABEL_STEP)
    assert step["ok"] is ok


# --------------------------------------------------------------------------
# readiness: real captured GraphQL answers
# --------------------------------------------------------------------------


def _body() -> str:
    return str(lgc.graphql_issue(_captured("chain-issue-5808.json"))["body"])


def _open_nodes() -> dict[int, Any]:
    return {int(k): lgc.graphql_issue(v) for k, v in _captured("blockers-open.json").items()}


def test_readiness_parser_reads_the_real_graphql_answers() -> None:
    assert lgc.blocker_numbers(_body()) == (5788, 5790, 5794, 5799, 5800, 5807)
    node = lgc.graphql_issue(_captured("blocker-merged-into-next.json"))
    assert lgc.closing_pulls(5787, node) == [
        {
            "number": 5849,
            "state": "MERGED",
            "merged": True,
            "base": "next",
            "merge_commit": "77d3d60a66087e251ec791d6b0d2a55d826f232e",
        }
    ]
    with pytest.raises(ValueError):
        lgc.graphql_issue({"errors": [{"message": "Could not resolve"}], "data": None})


def test_readiness_refuses_todays_six_open_blockers_naming_all_six() -> None:
    verdict = lgc.readiness_verdict(_body(), _open_nodes(), lambda _oid: True)
    assert not verdict.passed
    assert verdict.missing == tuple(
        f"#{n}: no merged pull request into next closes it"
        for n in (5788, 5790, 5794, 5799, 5800, 5807)
    )


def test_readiness_refuses_a_blocker_merged_into_next_outside_the_candidate() -> None:
    body = "## Blocked by\n\n1. #5787.\n"
    node = lgc.graphql_issue(_captured("blocker-merged-into-next.json"))
    verdict = lgc.readiness_verdict(body, {5787: node}, lambda _oid: False)
    assert verdict.missing == (
        "#5787: pull request #5849 merge commit is not an ancestor of the candidate",
    )


def test_readiness_refuses_a_blocker_closed_by_a_merge_into_main() -> None:
    body = "## Blocked by\n\n1. #5814.\n"
    node = lgc.graphql_issue(_captured("blocker-merged-into-main.json"))
    verdict = lgc.readiness_verdict(body, {5814: node}, lambda _oid: True)
    assert verdict.missing == ("#5814: no merged pull request into next closes it",)


def _merged_into_next_for(number: int) -> Any:
    node = copy.deepcopy(lgc.graphql_issue(_captured("blocker-merged-into-next.json")))
    node["number"] = number
    for item in node["timelineItems"]["nodes"]:
        source = item.get("source") or {}
        if source.get("__typename") == "PullRequest":
            source["body"] = str(source["body"]).replace("#5787", f"#{number}")
    return node


def test_readiness_passes_when_every_blocker_is_merged_into_next_and_in_the_candidate() -> None:
    # Liveness, built from the real merged-into-next shape.
    nodes = {n: _merged_into_next_for(n) for n in lgc.blocker_numbers(_body())}
    asked: list[str] = []
    verdict = lgc.readiness_verdict(_body(), nodes, lambda oid: asked.append(oid) or True)
    assert verdict.passed, verdict.missing
    assert len(asked) == 6


def test_readiness_refuses_a_fork_pull_request_into_its_own_next() -> None:
    # The real captured #5787 node, with its closing pull request's base
    # repository changed to a fork name: a fork's pull request into the fork's
    # own next that says `Closes #5787` merges nothing into the product.
    node = _merged_into_next_for(5787)
    for item in node["timelineItems"]["nodes"]:
        source = item.get("source") or {}
        if source.get("__typename") == "PullRequest":
            assert source["baseRepository"] == {"nameWithOwner": "curie-eng/curie"}
            source["baseRepository"] = {"nameWithOwner": "someone/curie"}
    body = "## Blocked by\n\n1. #5787.\n"
    verdict = lgc.readiness_verdict(body, {5787: node}, lambda _o: True)
    assert verdict.missing == ("#5787: no merged pull request into next closes it",)
    # Liveness: the unchanged capture is counted.
    real = lgc.readiness_verdict(body, {5787: _merged_into_next_for(5787)}, lambda _o: True)
    assert real.passed, real.missing


def test_readiness_query_reads_each_pull_requests_base_repository() -> None:
    assert lgc.BLOCKER_QUERY.count("baseRepository { nameWithOwner }") == 2


def test_readiness_does_not_count_a_reference_without_a_closing_keyword() -> None:
    node = _merged_into_next_for(5787)
    for item in node["timelineItems"]["nodes"]:
        source = item.get("source") or {}
        if source.get("__typename") == "PullRequest":
            source["body"] = "Part of #5787."
    verdict = lgc.readiness_verdict("## Blocked by\n\n1. #5787.\n", {5787: node}, lambda _o: True)
    assert verdict.missing == ("#5787: no merged pull request into next closes it",)


def test_readiness_check_reads_through_the_injected_query_and_ancestry() -> None:
    nodes = {
        str(n): {"data": {"repository": {"issue": _merged_into_next_for(n)}}}
        for n in lgc.blocker_numbers(_body())
    }
    seen: list[tuple[Path, str]] = []

    def ancestry(root: Path, candidate: str) -> Callable[[str], bool]:
        seen.append((root, candidate))
        return lambda _oid: True

    verdict = lgc.check_readiness(
        "token",
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        query=_fake_graphql(nodes),
        ancestry=ancestry,
    )
    assert verdict.passed and seen == [(REPO_ROOT, "c" * 40)]
    unreadable = lgc.check_readiness(
        "token",
        repo_root=REPO_ROOT,
        candidate="c" * 40,
        query=lambda *_a: {"errors": [{"message": "rate limited"}]},
        ancestry=ancestry,
    )
    assert not unreadable.passed and "could not be read" in unreadable.missing[0]


def test_readiness_git_ancestry_treats_an_unknown_object_as_not_an_ancestor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def rev(name: str) -> str:
        argv = ["git", "-C", str(REPO_ROOT), "rev-parse", name]
        return subprocess.run(argv, capture_output=True, text=True, check=True).stdout.strip()

    head, parent = rev("HEAD"), rev("HEAD~1")
    real_run = subprocess.run

    def no_fetch(argv: list[str], *a: Any, **k: Any) -> Any:
        if "fetch" in argv:
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        return real_run(argv, *a, **k)

    monkeypatch.setattr(lgc.subprocess, "run", no_fetch)
    check = lgc.git_ancestry(REPO_ROOT, head)
    assert check(parent) is True
    assert check("f" * 40) is False
    assert check("not-a-sha") is False


# --------------------------------------------------------------------------
# handoff: real product producers into the judge
# --------------------------------------------------------------------------


def _record_with(beat: str, section: dict[str, Any]) -> dict[str, Any]:
    record = copy.deepcopy(PASSING)
    record[beat] = section
    return record


def _app_comment(body: str, created: str, updated: str) -> dict[str, Any]:
    return {"id": 77, "body": body, "created_at": created, "updated_at": updated, "user": APP}


def test_handoff_answer_acknowledgment_from_the_real_status_body() -> None:
    from curie_api import factory_notices as notices
    from curie_api.factory_questions import ANSWER_RECEIVED_LINE

    def body(lead: str) -> str:
        return notices.status_body(
            request_id=uuid.uuid4(),
            card_url=None,
            pill_label="RUNNING",
            phase_view=None,
            result=None,
            lead_line=lead,
        )

    def judged(text: str) -> judge.BeatVerdict:
        section = copy.deepcopy(PASSING["beat_b"])
        section["ticket_comments"] = lgc.comments_record(
            [_app_comment(text, "2030-01-01T00:00:20Z", "2030-01-01T00:10:40Z")]
        )
        return judge.judge_beat("beat_b", _record_with("beat_b", section))

    assert judged(body(ANSWER_RECEIVED_LINE)).passed
    refused = judged(body(notices.INVESTIGATING_LINE))
    assert not refused.passed
    assert any("acknowledges the answer" in r for r in refused.reasons)


# The revision reply and the merge notice are produced inside the API's async
# status renderer (``_render`` in apps/api/src/curie_api/factory_notices.py),
# which needs the API database, so a tools test cannot drive it. These tests
# build the bodies from the product's own pure helpers (``status_body``,
# ``result_section``, ``code_span``) and the format strings pinned against the
# product source; the producer itself is exercised by the paid live run (#5808
# criterion 1). The renderer posts on the request's stored reply target: the
# pull request conversation for a review revision, ticket A for the
# implementation request whose pull request merged.


def test_handoff_revision_reply_from_the_real_status_body_and_code_span() -> None:
    from curie_api import factory_notices as notices

    feedback = "https://github.com/example-org/unitconv/pull/12#pullrequestreview-7001"
    result = notices.result_section("completed", pr_url=None, feedback_url=feedback)

    def reply(sha: str) -> str:
        body = notices.status_body(
            request_id=uuid.uuid4(),
            card_url=None,
            pill_label="COMPLETED",
            phase_view=None,
            result=result,
        )
        # factory_notices appends this line to the status body once checks pass.
        return body + (
            f"\n\nChecks passed on head {notices.code_span(sha)}, "
            "observed at 2030-01-01 00:41:00 UTC after the run ended."
        )

    def judged(pull_conversation: list[dict[str, Any]]) -> judge.BeatVerdict:
        # beat_c reads the pull request's conversation (issue comments of the pull number).
        section = copy.deepcopy(PASSING["beat_c"])
        section["pull_comments"] = lgc.comments_record(pull_conversation)
        return judge.judge_beat("beat_c", _record_with("beat_c", section))

    def comment(text: str) -> dict[str, Any]:
        return _app_comment(text, "2030-01-01T00:40:30Z", "2030-01-01T00:41:00Z")

    assert judged([comment(reply("b" * 40))]).passed
    old_head = judged([comment(reply("a" * 40))])
    assert any("names the revision commit" in r for r in old_head.reasons)
    # Counterexample: the reply posted on ticket A instead. The pull request's
    # conversation then holds only ticket A's own investigating status comment.
    investigating = notices.status_body(
        request_id=uuid.uuid4(),
        card_url=None,
        pill_label="RUNNING",
        phase_view=None,
        result=None,
        lead_line=notices.INVESTIGATING_LINE,
    )
    on_ticket_a = judged([comment(investigating)])
    assert any("names the revision commit" in r for r in on_ticket_a.reasons)
    source = Path(notices.__file__).read_text()
    assert "Checks passed on head {code_span(observation.head_sha)}, " in source


def test_handoff_merge_notice_from_the_real_result_section() -> None:
    from curie_api import factory_notices as notices

    url = PASSING["beat_b"]["open_pulls"][0]["html_url"]
    source = Path(notices.__file__).read_text()
    assert 'result += f"Pull request merged: {pr_url}\\n"' in source
    assert judge.MERGED_NOTICE_PREFIX == "Pull request merged: "

    def notice(pr_url: str) -> dict[str, Any]:
        text = (
            notices.result_section("completed", pr_url=pr_url) + f"Pull request merged: {pr_url}\n"
        )
        return _app_comment(text, "2030-01-01T00:00:20Z", "2030-01-01T00:45:40Z")

    def judged(ticket_a: list[dict[str, Any]]) -> judge.BeatVerdict:
        # beat_d reads ticket A's issue comments after the merge.
        section = copy.deepcopy(PASSING["beat_d"])
        section["ticket_comments_after_merge"] = lgc.comments_record(ticket_a)
        return judge.judge_beat("beat_d", _record_with("beat_d", section))

    assert judged([notice(url)]).passed
    assert not judged([notice(url.replace("/pull/12", "/pull/99"))]).passed
    # Counterexample: the notice posted on the pull request instead. Ticket A
    # then holds only its completed result without the merged line.
    completed = notices.result_section("completed", pr_url=url)
    refused = judged([_app_comment(completed, "2030-01-01T00:00:20Z", "2030-01-01T00:40:00Z")])
    assert any("names the merged pull request" in r for r in refused.reasons)


def test_handoff_session_probe_from_the_real_console_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api.schemas.agent_access import AgentAccessPolicyOut
    from curie_api.schemas.console import PrincipalOut

    def probe(kind: str) -> judge.BeatVerdict:
        sessions = _session_answers(kind)
        sides = {}
        for role, principal in (("writer", WRITER_PRINCIPAL), ("reader", READER_PRINCIPAL)):
            out = PrincipalOut(
                id=uuid.UUID(principal),
                tenant_id=uuid.UUID(TENANT),
                idp_subject=role,
                display_name=None,
                email=None,
                status="active",
            ).model_dump(mode="json")
            sides[role] = lgc.session_side(*sessions[role], 200, out)
        policy = AgentAccessPolicyOut(
            agent_id=uuid.UUID(AGENT),
            tenant_id=uuid.UUID(TENANT),
            invocation_team_id=None,
            approver_team_id=None,
            version=0,
            updated_at=None,
        ).model_dump(mode="json")
        record = {"session_probe": sides, "agent": {"tenant_id": lgc.agent_tenant(200, policy)}}
        return judge.judge_beat("session_probe", record)

    assert probe("principal").passed
    refused = probe("operator")
    assert any("not a principal session" in r for r in refused.reasons)


def test_shape_link_body_is_a_valid_code_host_link() -> None:
    from curie_api.schemas.identity import CodeHostLinkIn
    from pydantic import ValidationError

    body = lgc.code_host_link_body(
        principal_id=WRITER_PRINCIPAL, account_id=1001, created_by=WRITER_PRINCIPAL
    )
    assert CodeHostLinkIn.model_validate(body).account_id == "1001"
    for account in (1001, "octo-writer"):
        with pytest.raises(ValidationError):
            CodeHostLinkIn.model_validate({**body, "account_id": account})
    for bad in (True, 0, -3):
        with pytest.raises(ValueError):
            lgc.code_host_link_body(
                principal_id=WRITER_PRINCIPAL, account_id=bad, created_by=WRITER_PRINCIPAL
            )


def test_shape_code_host_matches_the_api_github_host() -> None:
    from curie_api.config import Settings
    from curie_api.forges.hosts import github_host

    assert github_host(Settings()) == lgc.CODE_HOST


# --------------------------------------------------------------------------
# shape: each beat builder fed GitHub's documented response shapes
# --------------------------------------------------------------------------


def _gh_comment(
    cid: int, body: str, user: dict[str, Any], created: str, updated: str | None = None
) -> dict[str, Any]:
    # https://docs.github.com/en/rest/issues/comments#list-issue-comments
    return {
        "id": cid,
        "node_id": f"IC_{cid}",
        "url": f"https://api.github.com/repos/{REPO}/issues/comments/{cid}",
        "html_url": f"https://github.com/{REPO}/issues/11#issuecomment-{cid}",
        "body": body,
        "user": {"login": user["login"], "id": user["id"], "type": user["type"]},
        "created_at": created,
        "updated_at": updated or created,
        "author_association": "NONE",
    }


def _row(
    rid: str, status: str, created: str, terminal: str | None, objective: str = ""
) -> dict[str, Any]:
    # One row_to_json of execution_requests: Postgres timestamps with an offset.
    return {
        "id": rid,
        "work_item_id": "work-item-a",
        "status": status,
        "created_at": created.replace("Z", ".123456+00:00"),
        "terminal_at": terminal.replace("Z", ".5+00:00") if terminal else None,
        "objective": objective,
    }


def _gh_pull(number: int, body: str, created: str, sha: str, ref: str) -> dict[str, Any]:
    # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
    return {
        "number": number,
        "state": "open",
        "html_url": f"https://code-host.example.invalid/fixture-owner/fixture-repo/pull/{number}",
        "body": body,
        "created_at": created,
        "user": APP,
        "head": {"ref": ref, "sha": sha, "label": f"fixture-owner:{ref}"},
        "base": {"ref": "main", "sha": "0" * 40},
    }


def _judged(beat: str, section: dict[str, Any]) -> judge.BeatVerdict:
    return judge.judge_beat(beat, _record_with(beat, section))


def _beat_a(
    status: str = "completed", terminal: str | None = "2030-01-01T00:05:05Z"
) -> dict[str, Any]:
    app = {"login": "fixture-app[bot]", "id": 9000003, "type": "Bot"}
    question = PASSING["beat_a"]["comments"][1]["body"]
    return lgc.beat_a_record(
        issue_number=11,
        labelled_at="2030-01-01T00:00:00Z",
        comments=[
            _gh_comment(
                1,
                "Investigating this ticket. The factory will open a pull request "
                "or post its questions here.",
                app,
                "2030-01-01T00:00:20Z",
            ),
            _gh_comment(2, question, app, "2030-01-01T00:05:00Z"),
        ],
        labels=[
            {"id": 1, "name": "factory", "color": "ededed"},
            {"id": 2, "name": "curie-factory:questions", "color": "ededed"},
        ],
        quiet_rows=[_row("request-1", status, "2030-01-01T00:00:25Z", terminal)],
        observed_at="2030-01-01T00:06:10Z",
        work_item_id="work-item-a",
    )


def test_shape_beat_a_builder_into_the_judge() -> None:
    assert _judged("beat_a", _beat_a()).passed
    assert not _judged("beat_a", _beat_a("waiting", None)).passed


def _beat_b(ack: str = judge.ANSWER_RECEIVED_LINE) -> dict[str, Any]:
    app = {"login": "fixture-app[bot]", "id": 9000003, "type": "Bot"}
    writer = {"login": "fixture-writer", "id": 9000001, "type": "User"}
    body = PASSING["beat_b"]["open_pulls"][0]["body"]
    return lgc.beat_b_record(
        answer=_gh_comment(3, judge.ANSWER_TEXT, writer, "2030-01-01T00:10:00Z"),
        issue_number=11,
        request_rows=[
            _row("request-1", "completed", "2030-01-01T00:00:25Z", "2030-01-01T00:05:05Z"),
            _row("request-2", "completed", "2030-01-01T00:10:30Z", "2030-01-01T00:24:00Z"),
        ],
        open_pulls=[_gh_pull(12, body, "2030-01-01T00:25:00Z", "a" * 40, "curie/issue-11")],
        issues=[
            {"number": 11, "user": writer},
            {"number": 12, "user": app, "pull_request": {"url": "https://api.github.com/x"}},
        ],
        ticket_comments=[
            _gh_comment(
                1, f"{ack}\n\nStatus: RUNNING", app, "2030-01-01T00:00:20Z", "2030-01-01T00:10:40Z"
            )
        ],
    )


def test_shape_beat_b_builder_into_the_judge() -> None:
    assert _judged("beat_b", _beat_b()).passed
    assert not _judged("beat_b", _beat_b(ack="Investigating this ticket.")).passed


PATCH = (
    "@@ -38,2 +38,6 @@ class ConvertTest(unittest.TestCase):\n"
    "     def test_c(self):\n"
    "         pass\n"
    "+    def test_rankine(self):\n"
    "+        x = 1\n"
    "+        y = 2\n"
    "+        z = 3"
)


def _beat_c(proposals: list[str] | None = None, conclusion: str = "success") -> dict[str, Any]:
    app = {"login": "fixture-app[bot]", "id": 9000003, "type": "Bot"}
    revision = judge.revision_request_id(9100001, 12, 7001)
    rule = judge.REVIEW_RULE_TEXT
    path = "unitconv/tests/test_convert.py"
    posted = lgc.review_body(
        "a" * 40, *lgc.review_target(lgc.added_lines_by_path([{"filename": path, "patch": PATCH}]))
    )
    sha = "b" * 40
    return lgc.beat_c_record(
        repo="fixture-owner/fixture-repo",
        pull_number=12,
        pull_before=_gh_pull(12, "", "2030-01-01T00:25:00Z", "a" * 40, "curie/issue-11"),
        posted=posted,
        # https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
        review={
            "id": 7001,
            "user": {"login": "fixture-writer", "id": 9000001},
            "body": "",
            "state": "COMMENTED",
            "html_url": "https://code-host.example.invalid/fixture-owner/fixture-repo/pull/12#pullrequestreview-7001",
            "pull_request_url": "https://api.code-host.example.invalid/repos/fixture-owner/fixture-repo/pulls/12",
            "submitted_at": "2030-01-01T00:30:00Z",
            "commit_id": "a" * 40,
        },
        # https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
        review_comments=[
            {
                "id": 5,
                "pull_request_review_id": 7001,
                "path": path,
                "line": 40,
                "body": rule,
                "commit_id": "a" * 40,
            }
        ],
        files=[
            {"filename": path, "status": "modified", "additions": 4, "deletions": 0, "patch": PATCH}
        ],
        request_rows=[
            _row("request-1", "completed", "2030-01-01T00:00:25Z", "2030-01-01T00:05:05Z"),
            _row("request-2", "completed", "2030-01-01T00:10:30Z", "2030-01-01T00:24:00Z"),
            _row(
                revision,
                "completed",
                "2030-01-01T00:30:20Z",
                "2030-01-01T00:39:00Z",
                f"Review feedback on {path} line 40: {rule}",
            ),
        ],
        proposals=[revision] if proposals is None else proposals,
        pull_after=_gh_pull(12, "", "2030-01-01T00:25:00Z", sha, "curie/issue-11"),
        committed_at="2030-01-01T00:40:00Z",
        branches=[
            {"name": "main", "protected": True},
            {"name": "curie/issue-11", "protected": False},
        ],
        pull_comments=[
            _gh_comment(
                21,
                f"Checks passed on head `{sha}`, "
                "observed at 2030-01-01 00:41:00 UTC after the run ended.",
                app,
                "2030-01-01T00:40:30Z",
                "2030-01-01T00:41:00Z",
            )
        ],
        # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
        check_runs={
            "total_count": 1,
            "check_runs": [
                {
                    "id": 31,
                    "name": "test",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": conclusion,
                }
            ],
        },
        # https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
        combined_status={"state": "pending", "sha": sha, "total_count": 0, "statuses": []},
    )


def test_shape_beat_c_builder_into_the_judge() -> None:
    section = _beat_c()
    assert section["post"]["body"]["comments"][0]["line"] == 40
    assert _judged("beat_c", section).passed
    assert not _judged("beat_c", _beat_c(proposals=[])).passed
    assert not _judged("beat_c", _beat_c(conclusion="failure")).passed


def _rules_vector() -> dict[str, Any]:
    return json.loads((PENDING / "memory-rules-responses.json").read_text())


def _promoted_list(state: str = "proposed") -> dict[str, Any]:
    listing = _rules_vector()["list"]
    rule = {
        **listing["rules"][0],
        "state": state,
        "source_url": "https://code-host.example.invalid/fixture-owner/fixture-repo/pull/12#pullrequestreview-7001",
    }
    return {**listing, "rules": [rule]}


def _beat_d(issue_state: str = "closed") -> dict[str, Any]:
    app = {"login": "fixture-app[bot]", "id": 9000003, "type": "Bot"}
    url = PASSING["beat_b"]["open_pulls"][0]["html_url"]
    return lgc.beat_d_record(
        rules_before={**_rules_vector()["list"], "rules": []},
        pull_merged={
            **_gh_pull(12, "", "2030-01-01T00:25:00Z", "b" * 40, "curie/issue-11"),
            "merged": True,
            "merged_at": "2030-01-01T00:45:00Z",
            "merged_by": {"login": "fixture-writer", "id": 9000001},
        },
        rules_after=_promoted_list(),
        observed_at="2030-01-01T00:45:30Z",
        # https://docs.github.com/en/rest/issues/issues#get-an-issue
        issue_after={
            "number": 11,
            "state": issue_state,
            "closed_at": "2030-01-01T00:45:05Z" if issue_state == "closed" else None,
        },
        ticket_comments=[
            _gh_comment(
                1,
                f"Completed: {url}\nPull request merged: {url}\n",
                app,
                "2030-01-01T00:00:20Z",
                "2030-01-01T00:45:40Z",
            )
        ],
    )


def test_shape_beat_d_builder_into_the_judge() -> None:
    record = _record_with("beat_d", _beat_d())
    record["beat_c"]["review"]["id"] = 7001
    assert judge.judge_beat("beat_d", record).passed
    assert not _judged("beat_d", _beat_d(issue_state="open")).passed


def _accept_output(principal: str) -> str:
    accept = _rules_vector()["accept"]
    accept["rule"]["id"] = "rule-id-1"
    accept["rule"]["accepted_by"]["principal_id"] = principal
    return json.dumps(accept)


def _beat_e(reader_exit: int = 1) -> dict[str, Any]:
    refusal = json.dumps(
        {
            "error": "memory_rules.write_access_required: You cannot write to this repository.",
            "fix": "Ask a maintainer of the repository to decide this rule.",
        }
    )
    return lgc.beat_e_record(
        # https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
        reader_permission={
            "permission": "read",
            "role_name": "read",
            "user": {"login": "fixture-reader"},
        },
        rule_before={"id": "rule-id-1", "version": 1, "state": "proposed"},
        reader=(reader_exit, refusal, 1.2),
        rule_after_reader={"id": "rule-id-1", "version": 1},
        writer=(0, _accept_output("principal-writer"), 1.5),
    )


def test_shape_beat_e_builder_into_the_judge() -> None:
    assert _judged("beat_e", _beat_e()).passed
    assert not _judged("beat_e", _beat_e(reader_exit=0)).passed


def _beat_f(extra: str = "") -> dict[str, Any]:
    pending = json.loads((PENDING / "work-item-memory-rules.json").read_text())
    body = pending["pull_body_annotated"]
    patch = (
        "@@ -1,0 +1,4 @@\n"
        "+    def test_delisle(self):\n"
        "+        self.assertAlmostEqual(\n"
        "+            convert(100, 'c', 'de'), 0.0, places=6\n"
        "+        )" + extra
    )
    return lgc.beat_f_record(
        issue_number=13,
        instruction_set=True,
        labelled_at="2030-01-01T01:00:00Z",
        open_pulls=[_gh_pull(14, body, "2030-01-01T01:20:00Z", "c" * 40, "curie/issue-13")],
        work_item={"id": "work-item-b", "memory_rules": pending["selected"]},
        files=[{"filename": "unitconv/tests/test_convert.py", "patch": patch}],
    )


def test_shape_beat_f_builder_into_the_judge() -> None:
    assert _judged("beat_f", _beat_f()).passed
    refused = _judged(
        "beat_f", _beat_f('\n+        self.assertEqual(convert(1, "C", "De"), 148.5)')
    )
    assert any("float assertions" in r for r in refused.reasons)


def _beat_g(state: str = "absent") -> dict[str, Any]:
    pending = json.loads((PENDING / "work-item-memory-rules.json").read_text())
    removed = _rules_vector()["list"]["rules"][3]
    rule = {
        **removed,
        "id": "rule-id-1",
        "removed_at": "2030-01-01T01:28:00Z",
        "removed_by": {"principal_id": "principal-writer", "display_name": "Fixture Writer"},
    }
    return lgc.beat_g_record(
        issue_number=15,
        labelled_at="2030-01-01T01:30:00Z",
        rule_id="rule-id-1",
        removal=(0, json.dumps({"rule": rule, "applies_from": "next_ticket"}), 1.1),
        open_pulls=[
            _gh_pull(
                16,
                "Closes #15\n\nRequester preferences: applied; sha256:" + "f" * 64,
                "2030-01-01T01:50:00Z",
                "d" * 40,
                "curie/issue-15",
            )
        ],
        work_item={
            "memory_rules": pending[state]
            if state in pending
            else {**pending["selected"], "state": state}
        },
    )


def test_shape_beat_g_builder_into_the_judge() -> None:
    assert _judged("beat_g", _beat_g()).passed
    assert not _judged("beat_g", _beat_g("selected")).passed


# What `curie cluster deploy --json` prints when the API refuses the bundle
# upload: cli/src/main.rs:63 emits cli/src/exit.rs:280 `error_json`, which is
# {"error": format!("{err:#}"), "fix": <classify fix, none here>}; the error is
# cli/src/api.rs:3055 `bail!("{what} failed with {status}: {}", body.trim())`
# with what = "uploading the bundle" (cli/src/api.rs:3977), and the body is
# FastAPI's {"detail": {"code", "message"}} of a 422 refusal
# (apps/api/src/curie_api/routers/bundles.py:178). The code is #5804's (ASSUMED).
RETIRED_REFUSAL = json.dumps(
    {
        "error": "uploading the bundle failed with 422 Unprocessable Entity: "
        + json.dumps(
            {
                "detail": {
                    "code": "bundle.result_declaration_retired",
                    "message": "workflow/result.json is retired; remove it from the bundle",
                }
            },
            separators=(",", ":"),
        ),
        "fix": None,
    }
)
# The connector lock preflight's refusal of a bundle without its runner lock,
# before any upload (cli/src/commands/connector_images.rs:507, rebuild_hint at
# :438): the copy of the source bundle got this instead of the API's refusal.
MISSING_LOCK_REFUSAL = json.dumps(
    {
        "error": "connectors.yaml declares a runner layer, but connectors.lock.yaml records "
        "no runner image for it. Build it before deploying.",
        "fix": "run `curie build --plugin-dir retired-bundle` and redeploy",
    }
)


def _beat_h(exit_code: int = 1, refusal: str = RETIRED_REFUSAL) -> dict[str, Any]:
    writer = {"login": "fixture-writer", "id": 9000001}
    return lgc.beat_h_record(
        label="factory-unbound-run1",
        observed_at="2030-01-01T02:03:30Z",
        work_items=[],
        comments=[],
        # https://docs.github.com/en/rest/issues/events#list-issue-events
        events=[
            {
                "id": 1,
                "event": "labeled",
                "actor": writer,
                "label": {"name": "factory-unbound-run1", "color": "ededed"},
                "created_at": "2030-01-01T02:00:00Z",
            },
            {"id": 2, "event": "renamed", "actor": writer, "created_at": "2030-01-01T02:00:01Z"},
        ],
        deploy=(exit_code, refusal),
        version_before="version-3",
        version_after="version-3",
    )


def test_shape_beat_h_builder_into_the_judge() -> None:
    assert _judged("beat_h", _beat_h()).passed
    assert not _judged("beat_h", _beat_h(exit_code=0)).passed
    missing_lock = _judged("beat_h", _beat_h(refusal=MISSING_LOCK_REFUSAL))
    assert any("deploy refusal is not" in r for r in missing_lock.reasons)


@pytest.mark.parametrize(
    ("refusal", "passed"), [(RETIRED_REFUSAL, True), (MISSING_LOCK_REFUSAL, False)]
)
def test_handoff_retired_declaration_deploy_uses_the_deployed_bundle_copy(
    refusal: str, passed: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The deploy_bundle the preflight ran records the prepared copy it uploaded
    # (with the runner lock `curie build` wrote); beat (h) deploys a copy of
    # that one plus workflow/result.json, and the judge reads the CLI's real
    # refusal envelope. The missing lock refusal is the counterexample.
    p, chain, _opened = _beat_preflight(tmp_path, monkeypatch)
    prepared = tmp_path / "prepared"
    (prepared / ".claude-plugin").mkdir(parents=True)
    (prepared / ".claude-plugin" / "plugin.json").write_text('{"name": "b", "version": "1"}')
    (prepared / "connectors.lock.yaml").write_text("runner: {}\n")
    p.chart_dir = tmp_path
    monkeypatch.setattr(p, "build_runner_layer", lambda _bundle: prepared)
    monkeypatch.setattr(p, "_write_kubeconfig", lambda: tmp_path / "kubeconfig")
    monkeypatch.setattr(p, "_curie", lambda *_a: None)
    p.deploy_bundle(github_token=False)
    assert p.deployed_bundle == prepared

    p.run_id = "run1"
    label = "factory-unbound-run1"
    writer = {"login": "fixture-writer", "id": PASSING["identities"]["writer_id"]}
    monkeypatch.setattr(
        lgc,
        "open_and_label",
        lambda *_a: ({"number": 21, "id": 99}, "2000-01-01T00:00:00Z"),
    )
    monkeypatch.setattr(p, "as_actor", lambda *_a, **_k: (201, {}))
    events = [
        {
            "event": "labeled",
            "label": {"name": label},
            "actor": writer,
            "created_at": "2000-01-01T00:00:00Z",
        }
    ]
    monkeypatch.setattr(p, "_paged", lambda path: events if path.endswith("/events") else [])
    monkeypatch.setattr(lgc, "work_item_for_issue", lambda *_a: None)
    monkeypatch.setattr(lgc, "_in_force_version", lambda _p: "v3")
    seen: dict[str, bool] = {}

    def answer(args: list[str]) -> Any:
        bundle = Path(args[args.index("--plugin-dir") + 1])
        seen["lock"] = (bundle / "connectors.lock.yaml").is_file()
        seen["result"] = (bundle / "workflow" / "result.json").is_file()
        return 1, refusal, 3.0

    _fake_cli(monkeypatch, answer)
    lgc.beat_h(p, chain)
    assert seen == {"lock": True, "result": True}
    verdict = judge.judge_beat("beat_h", chain.record)
    assert verdict.passed is passed, verdict.reasons


def test_shape_teardown_section_from_teardown_results() -> None:
    ok = [
        {"step": name, "ok": True, "detail": {}}
        for name in (lgc.DEX_STEP, lgc.SESSION_STEP, lgc.LINKS_STEP)
    ]
    section = lgc.teardown_section(ok, namespace_gone=True)
    assert judge.judge_beat("teardown", {"teardown": section}).passed
    broken = [*ok[:2], {"step": lgc.LINKS_STEP, "ok": False, "detail": "x"}]
    refused = judge.judge_beat(
        "teardown", {"teardown": lgc.teardown_section(broken, namespace_gone=True)}
    )
    assert any("code_host_links_removed" in r for r in refused.reasons)
    assert not judge.judge_beat(
        "teardown", {"teardown": lgc.teardown_section(ok, namespace_gone=False)}
    ).passed


def test_shape_usage_breach_stops_at_the_cap() -> None:
    assert (
        lgc.usage_breach(
            [{"label": "before_a", "usage": 10.0}, {"label": "after_a", "usage": 59.99}]
        )
        is None
    )
    assert (
        lgc.usage_breach(
            [{"label": "before_a", "usage": 10.0}, {"label": "after_a", "usage": 60.0}]
        )
        == 50.0
    )


def test_shape_evidence_serializer_on_a_real_preflight_evidence(tmp_path: Path) -> None:
    p = _preflight(tmp_path)
    p.step("namespace created", namespace=p.namespace)
    p.evidence["image_ref"] = "registry.example.invalid:5000/factory-e2e-abc/curie-api:cand-1"
    ids = lgc.sanitizer_identifiers(
        actor=WRITER,
        reader=READER,
        repository=REPOSITORY,
        app_id=p.config.app_id,
        app_slug=p.config.mention,
        installation_id=p.config.installation_id,
        namespace=p.namespace,
        release=p.release,
        kube_context=p.config.kube_context,
        registry=p.config.layer_registry,
    )
    text = lgc.evidence_serializer(ids)(p.evidence)
    assert not [raw for raw in RAW_VALUES if raw in text]
    parsed = json.loads(text)
    assert parsed["kube_context"] == "<context>"
    assert parsed["image_ref"].startswith("<registry>/")
    assert parsed["started_at"] == p.evidence["started_at"]


def _iterate(values: dict[str, list[str | int]]) -> Iterator[str]:
    for raws in values.values():
        for raw in raws:
            yield str(raw)


def test_shape_sanitizer_identifiers_name_every_role_and_keep_the_owner_as_writer() -> None:
    owned = {**REPOSITORY, "full_name": "octo-writer/unitconv"}
    ids = lgc.sanitizer_identifiers(
        actor=WRITER,
        reader=READER,
        repository=owned,
        app_id="4242",
        app_slug="fixture-app",
        installation_id=777,
        namespace="ns",
        release="rel",
        kube_context="ctx",
        registry=None,
    )
    assert set(ids) == {"fixture-repo", "writer", "reader", "app", "namespace", "context"}
    assert "octo-writer" not in ids["fixture-repo"]
    assert (
        judge.sanitize_log("octo-writer/unitconv by octo-writer", ids)
        == "<fixture-repo> by <writer>"
    )
    assert "fixture-app[bot]" in set(_iterate(ids))
