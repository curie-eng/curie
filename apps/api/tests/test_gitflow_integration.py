"""Git-flow end to end against real Postgres + RustFS with local bare repos.

No github.com and no network: the "remote" is a local bare repository and the
webhook payloads are HMAC-signed exactly as GitHub signs them. This exercises
the real deploy/promote path (git archive -> validate -> store -> deploy row).

The bare repositories live under a per-test `file://` clone base laid out as
`<base>/<owner>/<name>.git`, and `GITHUB_CLONE_BASE` points at that base, so the
URL git is handed is the derived origin the production code computes -- the same
way a GitHub Enterprise operator points the setting at their own host. Nothing
here is a test-only code branch.
"""

import asyncio
import hashlib
import hmac
import io
import json
import os
import subprocess
import tarfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api import bundles, crud
from curie_api.config import get_settings
from curie_test_support.scaffold import scaffolded_deploy_yaml
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

SECRET = get_settings().github_webhook_secret
REPO = "octo/demo-agent"

VALID_FILES = {
    ".claude-plugin/plugin.json": '{"name": "demo-plugin", "version": "0.1.0"}',
    "skills/alpha/SKILL.md": "---\nname: alpha\ndescription: does alpha\n---\n",
    "skills/beta/SKILL.md": "---\nname: beta\ndescription: does beta\n---\n",
}


@pytest.fixture
def trusted_clone_base(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """Point `GITHUB_CLONE_BASE` at a per-test `file://` base and yield it.

    `routers/github.py` calls `get_settings()` per request rather than through a
    cached dependency, so the app does not need rebuilding. The clear-AFTER-undo
    ordering at teardown is load-bearing: clearing first would let the still-set
    env value be re-cached and leak into a later test.
    """

    base = tmp_path / "remotes"
    base.mkdir()
    monkeypatch.setenv("GITHUB_CLONE_BASE", f"file://{base}")
    get_settings.cache_clear()
    yield base
    monkeypatch.undo()
    get_settings.cache_clear()


def _git(*args: str, cwd: Path | None = None) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    out = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def _build_bare_repo(base_dir: Path, repo_full_name: str, files: dict[str, str]) -> tuple[str, str]:
    """Create a bare repo at `<base_dir>/<repo_full_name>.git`. Returns (url, sha).

    The returned URL is byte-identical to the origin the API derives from
    `GITHUB_CLONE_BASE` plus the agent's stored `repo_full_name`, so these tests
    exercise the derivation instead of bypassing it.
    """

    work = base_dir / "_work" / repo_full_name
    work.mkdir(parents=True)
    _git("init", "-q", "-b", "dev", cwd=work)
    for rel, content in files.items():
        path = work / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    _git("add", "-A", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    sha = _git("rev-parse", "HEAD", cwd=work)
    bare = base_dir / f"{repo_full_name}.git"
    bare.parent.mkdir(parents=True, exist_ok=True)
    _git("clone", "--quiet", "--bare", str(work), str(bare))
    return f"file://{bare}", sha


def _post(client: Any, event: str, payload: dict[str, Any], secret: str = SECRET) -> Any:
    body = json.dumps(payload).encode()
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": event,
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )


def _push_payload(ref: str, sha: str, clone_url: str) -> dict[str, Any]:
    return {
        "ref": ref,
        "after": sha,
        "repository": {"full_name": REPO, "clone_url": clone_url},
    }


def _insert_partial_version(agent_id: str, sha: str) -> None:
    """Insert a version row with commit_sha set but no bundle stored.

    Mimics the residue of a prior push that committed the row and then failed
    before the bundle was stored.
    """

    async def _run() -> None:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            await crud.create_version_row(
                session,
                uuid.UUID(agent_id),
                version_label=sha[:12],
                created_by="git-flow",
                commit_sha=sha,
            )
        await engine.dispose()

    asyncio.run(_run())


def _register_agent(client: Any, headers: dict[str, str]) -> str:
    agent = client.post(
        "/agents",
        json={
            "name": "gitflow-agent",
            "channel": {"kind": "slack", "address": "C000000G01"},
            "repo_full_name": REPO,
        },
        headers=headers,
    ).json()
    return str(agent["id"])


def test_dev_push_deploys_dev_bot(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "deployed"
    assert body["environment"] == "dev"
    assert body["commit_sha"] == sha

    # The version was built from the commit and its bundle is stored + fetchable.
    version = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()[0]
    assert version["commit_sha"] == sha
    assert version["bundle_ref"] is not None
    bundle = client.get(f"/agents/{agent_id}/versions/{version['id']}/bundle", headers=auth_headers)
    assert bundle.status_code == 200
    assert len(bundle.content) > 0

    # The deployment routes to the dev bot.
    deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert len(deployments) == 1
    assert deployments[0]["environment"] == "dev"


def test_patched_repo_binding_routes_a_push(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    """A binding applied by PATCH must actually route that repository's pushes.

    Before migration 0018, create-time was the only place `repo_full_name`
    could ever be set, so an agent bound after creation had no way to become
    reachable by git-flow. This drives the push through the single-agent
    fallback in `resolve_target_agent` (no `deploy.yaml` in the pushed files,
    exactly one agent bound to the repository), which is the path an agent
    bound entirely by PATCH depends on.

    Asserting that `AgentUpdate` has the field proves none of that. Only a real
    signed push landing a deployment on the patched agent does.
    """

    created = client.post(
        "/agents",
        json={"name": "patched-agent", "channel": {"kind": "slack", "address": "C000000G02"}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    assert created.json()["repo_full_name"] is None, "created deliberately unbound"

    patched = client.patch(
        f"/agents/{agent_id}",
        json={"repo_full_name": REPO},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["repo_full_name"] == REPO

    # Read back through a separate request, so the assertion cannot be satisfied
    # by the PATCH response echoing its own input.
    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["repo_full_name"] == REPO

    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)
    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "deployed", body
    assert body["environment"] == "dev"
    assert body["commit_sha"] == sha

    # The push reached THIS agent, which is the whole point of the binding: a
    # push succeeding somewhere else would prove nothing.
    deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert len(deployments) == 1, deployments
    assert deployments[0]["environment"] == "dev"


def test_patch_omitting_repo_full_name_leaves_the_binding_intact(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A PATCH that omits repo_full_name must leave an existing binding alone.

    This is the guarantee a channel-only redeploy depends on: the CLI writes the
    channel by itself whenever --repo was not passed, and that must never wipe
    the repository the agent is already bound to.

    Since ADR-0116 the channel write is a different REQUEST (the channels
    subresource), so the sibling assertion needs two calls to say what one used
    to: the channel write must take effect, and the repo binding must survive
    it. Both are kept, because dropping either turns this into a test of one
    endpoint rather than of the interaction between them.
    """

    created = client.post(
        "/agents",
        json={
            "name": "omit-repo-agent",
            "channel": {"kind": "slack", "address": "C0EXAMPLE3"},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    assert created.json()["repo_full_name"] == REPO, "bound at creation"

    patched = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE3"},
        json={"kind": "slack", "address": "C0EXAMPLE4"},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text

    # Read back through a separate request, so the assertion cannot be
    # satisfied by the write response echoing stale input.
    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["repo_full_name"] == REPO, (
        "a channel write must leave the repository binding unchanged"
    )
    assert fetched.json()["channels"] == [{"kind": "slack", "address": "C0EXAMPLE4"}], (
        "the channel write must still take effect"
    )


def test_main_push_promotes_and_reuses_the_built_version(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    dev = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url)).json()
    prod = _post(client, "push", _push_payload("refs/heads/main", sha, clone_url)).json()

    assert prod["status"] == "promoted"
    assert prod["environment"] == "prod"
    # Promote reuses the already-built bundle rather than rebuilding.
    assert prod["version_id"] == dev["version_id"]

    envs = {
        d["environment"]
        for d in client.get(
            "/deployments", params={"agent_id": agent_id}, headers=auth_headers
        ).json()
    }
    assert envs == {"dev", "prod"}


def test_partial_version_is_rebuilt_not_reused(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)
    _insert_partial_version(agent_id, sha)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200
    assert resp.json()["status"] == "deployed"

    versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
    # The partial row was repaired in place (no duplicate) and now has a bundle.
    assert len(versions) == 1
    assert versions[0]["commit_sha"] == sha
    assert versions[0]["bundle_ref"] is not None


def test_dev_push_does_not_reuse_a_cli_bundle_with_the_same_commit_sha(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    """A git push must build its own artifact and fan out its normal eval."""

    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    cli_version = client.post(
        f"/agents/{agent_id}/versions",
        json={
            "version_label": "cli-working-tree",
            "created_by": "cli",
            "commit_sha": sha,
        },
        headers=auth_headers,
    )
    assert cli_version.status_code == 201, cli_version.text
    cli_version_id = cli_version.json()["id"]

    cli_files = {
        **VALID_FILES,
        "skills/alpha/SKILL.md": "---\nname: alpha\ndescription: cli working tree\n---\n",
    }
    cli_archive = io.BytesIO()
    with tarfile.open(fileobj=cli_archive, mode="w:gz") as archive:
        for rel, content in cli_files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"cli-working-tree/{rel}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    upload = client.put(
        f"/agents/{agent_id}/versions/{cli_version_id}/bundle",
        files={"file": ("cli-working-tree.tar.gz", cli_archive.getvalue())},
        headers=auth_headers,
    )
    assert upload.status_code == 201, upload.text
    cli_bundle_ref = upload.json()["bundle_ref"]

    response = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "deployed"

    versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
    git_versions = [version for version in versions if version["created_by"] == "git-flow"]
    assert len(git_versions) == 1, versions
    git_version = git_versions[0]
    assert git_version["id"] != cli_version_id
    assert git_version["bundle_ref"] != cli_bundle_ref

    stored = client.get(
        f"/agents/{agent_id}/versions/{git_version['id']}/bundle", headers=auth_headers
    )
    assert stored.status_code == 200, stored.text
    assert stored.content != cli_archive.getvalue()

    stream = redis.from_url(get_settings().valkey_dsn())
    try:
        eval_jobs = [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in stream.xrevrange("curie:evals", count=200)
            if json.loads(fields[STREAM_PAYLOAD_FIELD.encode()]).get("agent_id") == agent_id
        ]
    finally:
        stream.close()
    assert len(eval_jobs) == 1, eval_jobs
    assert eval_jobs[0]["version_id"] == git_version["id"]
    assert eval_jobs[0]["sha"] == sha


def test_invalid_signature_is_401(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    body = json.dumps(_push_payload("refs/heads/dev", "a" * 40, "file:///x")).encode()
    resp = client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": "sha256=deadbeef",
        },
    )
    assert resp.status_code == 401


def test_ping_event_pongs(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    resp = _post(client, "ping", {"zen": "hi"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "pong"


def test_unknown_repo_is_ignored(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # No agent registered for REPO.
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)
    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


def test_non_deploy_branch_is_ignored(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)
    resp = _post(client, "push", _push_payload("refs/heads/feature-x", sha, clone_url))
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


def test_malformed_bundle_push_is_rejected(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent_id = _register_agent(client, auth_headers)
    files = dict(VALID_FILES)
    files["skills/beta/SKILL.md"] = "# no frontmatter\n"
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, files)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "rejected"
    codes = {e["code"] for e in body["errors"]}
    assert "skill.frontmatter_missing" in codes

    # Nothing was deployed for a rejected push.
    deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert deployments == []


def test_signed_push_with_a_foreign_clone_url_is_rejected(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # The ticket's attack driven through the real consumer path: a correctly
    # HMAC-signed push naming the registered repository, with an attacker's
    # clone URL. A real bare repo exists at the trusted path and the same push
    # deploys fine when the URL matches (test_dev_push_deploys_dev_bot), so the
    # rejection here is the origin pin and nothing else.
    agent_id = _register_agent(client, auth_headers)
    _url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    resp = _post(
        client,
        "push",
        _push_payload("refs/heads/dev", sha, f"https://evil.example/{REPO}.git"),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "rejected"
    codes = {e["code"] for e in body["errors"]}
    assert "git.origin_mismatch" in codes

    # Nothing was built and nothing was deployed.
    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []
    assert (
        client.get("/deployments", params={"agent_id": agent_id}, headers=auth_headers).json() == []
    )


def test_push_using_the_repository_url_fallback_still_deploys(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # `repository.url` is the rarely-executed sibling of `repository.clone_url`
    # at gitflow.py:189. GitHub's push payload carries `clone_url` WITH the .git
    # suffix and `url` WITHOUT it, so this arms the guard via the secondary path
    # only and proves it agrees with the primary path (AGENTS.md parity-seam
    # rule).
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    resp = _post(
        client,
        "push",
        {
            "ref": "refs/heads/dev",
            "after": sha,
            "repository": {
                "full_name": REPO,
                "url": clone_url.removesuffix(".git"),
            },
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "deployed"

    deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert len(deployments) == 1


def test_signed_push_with_a_foreign_url_fallback_is_rejected(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # The fallback must not be an unguarded back door: the same comparison
    # applies whichever payload field supplied the URL.
    agent_id = _register_agent(client, auth_headers)
    _url, sha = _build_bare_repo(trusted_clone_base, REPO, VALID_FILES)

    resp = _post(
        client,
        "push",
        {
            "ref": "refs/heads/dev",
            "after": sha,
            "repository": {
                "full_name": REPO,
                "url": f"https://evil.example/{REPO}",
            },
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "rejected"
    assert "git.origin_mismatch" in {e["code"] for e in body["errors"]}

    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []
    assert (
        client.get("/deployments", params={"agent_id": agent_id}, headers=auth_headers).json() == []
    )


# --------------------------------------------------------------------------- #
# One repository, two agents (ADR-0091, #1070)
# --------------------------------------------------------------------------- #
DEPLOY_YAML = """
targets:
  dev:
    agent: two-agent-dev
    env: dev
    slack_channel: C000000D01
  prod:
    agent: two-agent-prod
    env: prod
    slack_channel: C000000E01
"""

TWO_TARGET_FILES = {**VALID_FILES, "deploy.yaml": DEPLOY_YAML}


def _register(client: Any, headers: dict[str, str], name: str, channel: str) -> str:
    resp = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": channel},
            "repo_full_name": REPO,
        },
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def test_one_repo_binds_two_agents(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Before 0018 the second create was a 409 from the unique index, which is
    # what forced the first adopting agent repo to make its second agent out of
    # band.
    _register(client, auth_headers, "two-agent-dev", "C000000D01")
    _register(client, auth_headers, "two-agent-prod", "C000000E01")


def test_dev_push_and_main_push_reach_different_agents(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    """The thing #1070 is for: dev -> the dev bot, main -> the prod bot."""

    dev_id = _register(client, auth_headers, "two-agent-dev", "C000000D01")
    prod_id = _register(client, auth_headers, "two-agent-prod", "C000000E01")
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, TWO_TARGET_FILES)

    dev = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url)).json()
    assert dev["status"] == "deployed", dev
    assert dev["agent_id"] == dev_id, "a dev push must reach the dev agent"

    prod = _post(client, "push", _push_payload("refs/heads/main", sha, clone_url)).json()
    assert prod["status"] == "promoted", prod
    assert prod["agent_id"] == prod_id, "a main push must reach the prod agent"

    # Each agent holds exactly its own deployment, in its own environment.
    for agent_id, expected in ((dev_id, "dev"), (prod_id, "prod")):
        deployments = client.get(
            "/deployments", params={"agent_id": agent_id}, headers=auth_headers
        ).json()
        assert [d["environment"] for d in deployments] == [expected]


def test_prod_promotes_the_exact_artifact_dev_validated(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # Bundle-once, bind-many. The two agents get their own Version ROWS (the
    # worker's binding join requires a version to belong to its agent), but both
    # rows point at the SAME stored object -- so prod promotes not merely
    # identical bytes but the same artifact, by construction rather than by
    # discipline.
    dev_id = _register(client, auth_headers, "two-agent-dev", "C000000D01")
    prod_id = _register(client, auth_headers, "two-agent-prod", "C000000E01")
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, TWO_TARGET_FILES)

    _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    _post(client, "push", _push_payload("refs/heads/main", sha, clone_url))

    def only_version(agent_id: str) -> dict[str, Any]:
        versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
        assert len(versions) == 1, versions
        return dict(versions[0])

    dev_version, prod_version = only_version(dev_id), only_version(prod_id)
    assert dev_version["id"] != prod_version["id"], "each agent owns its own version row"
    assert dev_version["bundle_ref"] == prod_version["bundle_ref"], (
        "prod must promote the artifact dev validated, not a re-upload of it"
    )
    assert dev_version["commit_sha"] == prod_version["commit_sha"] == sha


def test_dev_push_does_not_reuse_a_cli_bundle_from_a_sibling_agent(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    dev_id = _register(client, auth_headers, "two-agent-dev", "C000000D01")
    prod_id = _register(client, auth_headers, "two-agent-prod", "C000000E01")
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, TWO_TARGET_FILES)

    cli_version = client.post(
        f"/agents/{prod_id}/versions",
        json={
            "version_label": "cli-sibling-working-tree",
            "created_by": "cli",
            "commit_sha": sha,
        },
        headers=auth_headers,
    )
    assert cli_version.status_code == 201, cli_version.text
    cli_version_id = cli_version.json()["id"]

    cli_files = {
        **TWO_TARGET_FILES,
        "skills/alpha/SKILL.md": "---\nname: alpha\ndescription: cli sibling tree\n---\n",
    }
    cli_archive = io.BytesIO()
    with tarfile.open(fileobj=cli_archive, mode="w:gz") as archive:
        for rel, content in cli_files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"cli-sibling/{rel}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    upload = client.put(
        f"/agents/{prod_id}/versions/{cli_version_id}/bundle",
        files={"file": ("cli-sibling.tar.gz", cli_archive.getvalue())},
        headers=auth_headers,
    )
    assert upload.status_code == 201, upload.text
    cli_bundle_ref = upload.json()["bundle_ref"]

    response = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "deployed"

    versions = client.get(f"/agents/{dev_id}/versions", headers=auth_headers).json()
    assert len(versions) == 1, versions
    git_version = versions[0]
    assert git_version["created_by"] == "git-flow"
    assert git_version["bundle_ref"] != cli_bundle_ref

    stored = client.get(
        f"/agents/{dev_id}/versions/{git_version['id']}/bundle", headers=auth_headers
    )
    assert stored.status_code == 200, stored.text
    with tarfile.open(fileobj=io.BytesIO(stored.content), mode="r:*") as archive:
        skill = archive.extractfile("skills/alpha/SKILL.md")
        assert skill is not None
        assert skill.read().decode() == VALID_FILES["skills/alpha/SKILL.md"]


def test_a_target_naming_another_repos_agent_is_rejected_end_to_end(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # ADR-0091's sharpest edge, through the real webhook: `foreign-bot` belongs
    # to somebody else's repository, so this push must not deploy over it.
    _register(client, auth_headers, "two-agent-dev", "C000000D01")
    client.post(
        "/agents",
        json={
            "name": "foreign-bot",
            "channel": {"kind": "slack", "address": "C000000F01"},
            "repo_full_name": "someone-else/their-repo",
        },
        headers=auth_headers,
    )
    files = {
        **VALID_FILES,
        "deploy.yaml": (
            "targets:\n  prod:\n    agent: foreign-bot\n"
            "    env: prod\n    slack_channel: C000000F01\n"
        ),
    }
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, files)

    body = _post(client, "push", _push_payload("refs/heads/main", sha, clone_url)).json()
    assert body["status"] == "rejected", body
    assert body["errors"][0]["code"] == "deploy.agent_bound_elsewhere"

    # And nothing was deployed to the foreign agent.
    foreign = next(
        a for a in client.get("/agents", headers=auth_headers).json() if a["name"] == "foreign-bot"
    )
    assert (
        client.get("/deployments", params={"agent_id": foreign["id"]}, headers=auth_headers).json()
        == []
    )


def test_commit_poller_retries_after_routing_topology_is_repaired(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A rejected sha deploys on the next pass after its target is created."""
    from curie_api.commitpoller import CommitPoller
    from curie_api.schemas import AgentCreate, ChannelBinding

    _register(client, auth_headers, "bootstrap_agent", "C000000R01")
    files = {
        **VALID_FILES,
        "deploy.yaml": (
            "targets:\n  dev:\n    agent: repairedagent\n"
            "    env: dev\n    slack_channel: C000000R02\n"
        ),
    }
    _, sha = _build_bare_repo(trusted_clone_base, REPO, files)
    settings = get_settings()

    class Tips:
        def sha_for(self, repo_full_name: str, branch: str) -> str | None:
            assert repo_full_name == REPO
            return sha if branch == settings.dev_branch else None

    class NoopEvalQueue:
        async def enqueue(self, _job: Any) -> None:
            pass

    async def exercise() -> str:
        engine = create_async_engine(settings.database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        poller = CommitPoller(
            session_factory=maker,
            store=client.app.state.bundle_store,
            settings=settings,
            eval_queue=NoopEvalQueue(),
            tips=Tips(),
            interval_seconds=60,
        )
        try:
            first = await poller.poll_once()
            assert [move.sha for move in first] == [sha]
            assert "deploy.unknown_agent" in caplog.text

            async with maker() as session:
                assert await crud.list_deployments(session) == []
                repaired = await crud.create_agent(
                    session,
                    AgentCreate(
                        name="repairedagent",
                        channel=ChannelBinding(kind="slack", address="C000000R02"),
                        repo_full_name=REPO,
                    ),
                )

            second = await poller.poll_once()
            assert [move.sha for move in second] == [sha]
            return str(repaired.id)
        finally:
            await engine.dispose()

    repaired_id = asyncio.run(exercise())
    deployments = client.get(
        "/deployments", params={"agent_id": repaired_id}, headers=auth_headers
    ).json()
    assert len(deployments) == 1, deployments
    assert deployments[0]["commit_sha"] == sha
    assert deployments[0]["environment"] == "dev"


# --------------------------------------------------------------------------- #
# A scaffolded deploy.yaml declares nothing (#1210)
# --------------------------------------------------------------------------- #
# `curie init` writes a commented-out example plus a literal empty map, so the
# value below is the payload a fresh repository actually pushes, read from
# `DEPLOY_YAML` in cli/src/scaffold.rs rather than restated here.
SCAFFOLDED_DEPLOY_YAML = scaffolded_deploy_yaml()

SCAFFOLD_FILES = {**VALID_FILES, "deploy.yaml": SCAFFOLDED_DEPLOY_YAML}


def test_a_scaffolded_deploy_yaml_still_deploys(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    # #1210 through the real webhook: `curie init` scaffolds `targets: {}`, so
    # every first push from a scaffolded repository carried a deploy.yaml that
    # declared no routing, and the resolver ignored the push instead of
    # deploying to the one agent the repository binds. The unit tests reproduce
    # the deploy.yaml read by hand; this one runs the production read path
    # (gitflow._read_targets -> bundles.read_deploy_targets).
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, SCAFFOLD_FILES)

    body = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url)).json()
    assert body["status"] == "deployed", body
    assert body["deployment_id"] is not None
    assert body["agent_id"] == agent_id


def test_a_scaffolded_deploy_yaml_reads_back_as_an_empty_map(tmp_path: Path) -> None:
    # The premise the whole of #1210 rests on, pinned directly. None from
    # `read_deploy_targets` means the file is ABSENT; a non-null file whose
    # `targets` map is empty means it is PRESENT and declares nothing.
    # Conflating the two is the bug: had a scaffolded deploy.yaml read back as
    # None, the old `if targets is None:` gate would already have deployed it.
    for rel, content in SCAFFOLD_FILES.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    targets = bundles.read_deploy_targets(tmp_path)
    assert targets is not None, "a present deploy.yaml must not read back as absent"
    assert targets.targets == {}


# --------------------------------------------------------------------------- #
# A source-built connector arriving by git push (ADR 0113)
#
# VERIFY ONLY: nothing in gitflow.py changes. The push path routes through
# `bundles.extract_and_validate`, so the intake rules added to
# `plugin_format.validate.py` cover it with no second implementation -- which is
# the whole reason they live in `validate_bundle` rather than in the CLI upload
# router (apps/api/CLAUDE.md: the validator is the ONE gate a bundle passes
# through, whatever entry point it arrives by). These tests prove that rather
# than assuming it, and they assert on the ABSENCE of the Version row, not on a
# log line: `gitflow.py` creates the Version and stores the bundle only after
# validation passes, so a rule that fired but did not block would still leave a
# row behind.
# --------------------------------------------------------------------------- #
BUILT_CONNECTORS = (
    "connectors:\n"
    "  k8s-write:\n"
    "    build:\n"
    "      context: connectors/k8s-write\n"
    "      platforms: [linux/amd64, linux/arm64]\n"
)

_BUILT_FILES = {
    **VALID_FILES,
    "connectors.yaml": BUILT_CONNECTORS,
    "connectors/k8s-write/Dockerfile": "FROM scratch\nCOPY server.py /server.py\n",
    "connectors/k8s-write/server.py": "print('acme')\n",
}

_REGISTRY_IMAGE = (
    "ghcr.io/acme-corp/acme-bot-k8s-write-mcp@sha256:"
    "0000000000000000000000000000000000000000000000000000000000000000"
)


def _lock_text(source_digest: str) -> str:
    return (
        "version: 1\n"
        "connectors:\n"
        "  k8s-write:\n"
        f"    image: {_REGISTRY_IMAGE}\n"
        "    delivery: registry\n"
        "    platforms: [linux/amd64, linux/arm64]\n"
        f"    source_digest: {source_digest}\n"
    )


def _source_digest(tmp_path: Path, files: dict[str, str]) -> str:
    """Hash the build context exactly as the validator will after extraction."""

    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    for rel, content in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    build = ConnectorBuild.model_validate(
        {"context": "connectors/k8s-write", "platforms": ["linux/amd64", "linux/arm64"]}
    )
    return connector_lock.source_digest_of(tmp_path / "connectors" / "k8s-write", build)


def test_a_git_push_of_a_locked_build_bundle_deploys(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
    tmp_path: Path,
) -> None:
    # The control. Without it, a rule that rejected every build: bundle outright
    # would pass both negatives below.
    agent_id = _register_agent(client, auth_headers)
    files = dict(_BUILT_FILES)
    files["connectors.lock.yaml"] = _lock_text(_source_digest(tmp_path / "ctx", _BUILT_FILES))
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, files)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "deployed"
    versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
    assert [v["commit_sha"] for v in versions] == [sha]


def test_a_git_push_of_a_lockless_build_bundle_creates_no_version(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent_id = _register_agent(client, auth_headers)
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, _BUILT_FILES)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "rejected"
    assert "connectors.lock_missing" in {e["code"] for e in body["errors"]}

    # The row itself, not the response: this is what stops the deployment going
    # active while the runner derives a hosted URL for a connector nobody built.
    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []
    assert (
        client.get("/deployments", params={"agent_id": agent_id}, headers=auth_headers).json() == []
    )


def test_a_git_push_of_a_stale_build_bundle_creates_no_version(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
    tmp_path: Path,
) -> None:
    # The push path is the one an author uses without ever running `curie
    # build`, so it is where a stale lock actually reaches production: the
    # previous digest is activated and the deployed connector silently stops
    # matching the reviewed source.
    agent_id = _register_agent(client, auth_headers)
    files = dict(_BUILT_FILES)
    files["connectors.lock.yaml"] = _lock_text(_source_digest(tmp_path / "ctx", _BUILT_FILES))
    files["connectors/k8s-write/server.py"] = "print('acme, but different')\n"
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO, files)

    resp = _post(client, "push", _push_payload("refs/heads/dev", sha, clone_url))
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "rejected"
    assert "connectors.lock_stale" in {e["code"] for e in body["errors"]}
    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []
