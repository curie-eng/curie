"""Exercise the maintenance fixture's external stimuli without starting a cluster."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import candidate_install as ci
import certifi
import factory_e2e as fe
import maintenance
import pytest

TOOLS = Path(__file__).resolve().parents[2]


@pytest.fixture
def fixture(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(TOOLS / "github-stub"))
    monkeypatch.syspath_prepend(str(TOOLS / "model-script"))
    import maintenance_fixture

    monkeypatch.setattr(maintenance_fixture, "DATABASE", tmp_path / "checks.sqlite3")
    maintenance_fixture.initialize("a" * 40)
    return maintenance_fixture


def test_check_attempts_are_independent_by_head_and_job(fixture, tmp_path):
    issue = tmp_path / "issue.md"
    issue.write_text("Example")
    stub = fixture.MaintenanceGitHub(tmp_path, {"version": 1}, seed_tree=tmp_path, issue_file=issue)
    base, head = "a" * 40, "b" * 40
    (failed_id,) = fixture.control("set-checks", [json.dumps([[head, "failure", fixture.CHECK]])])

    def check(sha):
        _, listing = stub._rest("GET", f"/repositories/4401/commits/{sha}/check-runs", {}, {})
        return next(r for r in listing["check_runs"] if r["name"] == fixture.CHECK)

    failed = check(head)
    assert failed["head_sha"] == head and failed["id"] == failed_id
    assert failed["conclusion"] == "failure"
    assert check(base)["conclusion"] == "success"
    status, _ = stub._rest(
        "POST", f"/repositories/4401/actions/runs/{failed_id}/rerun-failed-jobs", {}, {}
    )
    assert status == 201
    assert check(head)["conclusion"] is None
    fixture.control("set-checks", [json.dumps([[head, "failure", fixture.CHECK]])])
    assert check(head)["conclusion"] == "failure" and check(head)["id"] != failed_id
    assert check(base)["conclusion"] == "success"
    fixture.control("mode", ["no-ci"])
    assert stub._checks(head) == []


def test_leased_human_push_and_restore_through_https_forge(fixture, tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    issue = seed / "issue.md"
    issue.write_text("Example\n")
    forge = fixture.github_stub.GithubStub(
        tmp_path, {"version": 1}, seed_tree=seed, issue_file=issue
    )
    branch = "factory/change"
    ref = "refs/heads/" + branch
    try:
        forge.start()
        ca = str(forge.ca_file)

        def git(*args):
            return subprocess.run(
                ["git", "-c", "http.sslVerify=true", "-c", "http.sslCAInfo=" + ca, *args],
                cwd=seed,
                capture_output=True,
                text=True,
                check=True,
                timeout=15,
            ).stdout.strip()

        git("clone", "--no-checkout", forge.clone_url, "publisher")
        original = git("-C", "publisher", "rev-parse", "HEAD")
        git("-C", "publisher", "push", "origin", "HEAD:" + ref)

        def counts():
            result = fixture.control("counts", [original])
            return result["factory_push"], result["human_push"]

        def tip():
            return git("ls-remote", "--refs", forge.clone_url, ref).split()[0]

        def stimulus(operation, expected, value, trusted_ca=ca):
            return fixture.control(
                operation, [forge.clone_url, trusted_ca, branch, expected, value]
            )

        assert counts() == (1, 0) and tip() == original
        with pytest.raises(subprocess.CalledProcessError) as untrusted:
            stimulus(
                "commit",
                original,
                '{"human.txt":"untrusted"}',
                certifi.where(),
            )
        assert "certificate" in untrusted.value.stderr.lower()
        assert counts() == (1, 0) and tip() == original
        foreign = stimulus("commit", original, '{"human.txt":"human edit"}')
        assert foreign != original and tip() == foreign and counts() == (1, 1)
        git("-C", "publisher", "fetch", "origin", ref)
        assert git("-C", "publisher", "show", "FETCH_HEAD:human.txt") == "human edit"
        assert git("-C", "publisher", "rev-parse", "FETCH_HEAD^") == original
        with pytest.raises(subprocess.CalledProcessError) as stale:
            stimulus("commit", original, '{"human.txt":"stale write"}')
        assert "stale info" in stale.value.stderr
        with pytest.raises(subprocess.CalledProcessError) as stale_restore:
            stimulus("restore", original, original)
        assert "stale info" in stale_restore.value.stderr
        assert tip() == foreign and counts() == (1, 1)
        assert stimulus("restore", foreign, original) == original
        assert tip() == original and counts() == (1, 2)
    finally:
        forge.close()


def test_model_edits_verifies_then_publishes_a_fresh_turn(fixture, tmp_path):
    (tmp_path / "revision.py").write_text("REVISION = 0\n")
    (tmp_path / "test_revision.py").write_text(
        "import unittest\nfrom revision import REVISION\n"
        "class RevisionTest(unittest.TestCase):\n"
        "    def test_real_revision(self): self.assertEqual(REVISION, 1)\n"
    )
    messages = [{"role": "user", "content": "Increase the example revision."}]
    (edit,) = fixture.model_response(messages)["message"]["content"]
    result = subprocess.run(
        ["sh", "-c", edit["input"]["command"]],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert (
        "Ran 1 test" in result.stderr and (tmp_path / "revision.py").read_text() == "REVISION = 1\n"
    )
    messages.extend(
        [
            {"role": "assistant", "content": [edit]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": edit["id"]}]},
        ]
    )
    (publish,) = fixture.model_response(messages)["message"]["content"]
    assert publish["name"] == "mcp__curie__publish_changes"
    messages.append({"role": "user", "content": "Repair the newly failed check."})
    assert fixture.model_response(messages)["message"]["content"][0]["name"] == "Bash"


def test_fixture_can_publish_conflict_specific_files_and_verify_them(fixture, tmp_path):
    check = "from pathlib import Path; assert Path('nested/result.txt').read_text() == 'new value'"
    plan = {"files": {"nested/result.txt": "new value"}, "verification": ["python3", "-c", check]}
    assert fixture.control("plan", [json.dumps(plan)])
    response = fixture.model_response([{"role": "user", "content": "Repair"}], plan=plan)
    command = response["message"]["content"][0]["input"]["command"]
    subprocess.run(["sh", "-c", command], cwd=tmp_path, timeout=10, check=True)
    assert (tmp_path / "nested/result.txt").read_text() == "new value"


def test_lightweight_candidate_values_and_pod_entrypoints(tmp_path):
    inputs = ci.Inputs(
        "throwaway",
        "test-example",
        "curie-example",
        "/example/curie",
        {k: f"registry.example.com/{k}:candidate" for k in ci.COMPONENTS},
        "Always",
    )
    run = maintenance.MaintenanceInstall(
        inputs, tmp_path, app_key=tmp_path / "app.pem", stub_ip="192.0.2.1"
    )
    values = run.maintenance_values()
    assert values["factory"]["maintenanceRepairEnabled"]
    assert not values["agentSandbox"]["runner"]["fakeModel"]
    assert values["agentSandbox"]["runner"]["workspace"]["enabled"]
    model = [
        e["value"] for e in values["worker"]["extraEnv"] if e["name"] == "CURIE_MODEL_BASE_URL"
    ]
    assert model == ["http://192.0.2.1:8123"]
    assert fe.model_proxy_egress(model[0]) in values["security"]["networkPolicy"]["allowedEgress"]
    for install, script in [
        (run, "maintenance_fixture.py"),
        (ci.CandidateInstall(inputs, tmp_path), "github_stub.py"),
    ]:
        manifests = install.stub_manifests({"revision.py": "REVISION = 0"}, "Example issue.")
        by_kind = {m["kind"]: m for m in manifests}
        assert by_kind["Pod"]["spec"]["containers"][0]["args"][:2] == ["/stub/" + script, "serve"]
        assert script in by_kind["ConfigMap"]["data"]


@pytest.mark.parametrize("case", ["active", "late", "recovery"])
def test_cli_selects_each_maintenance_case(case):
    args = fe.parse_args(["maintenance", "--case", case])
    assert args.case == case and args.mode == "maintenance"
