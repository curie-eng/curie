"""Eval fan-out seam against real Valkey.

Asserts an enqueued job lands on the stream with exactly the agreed payload
shape, and that a dev-branch push fans out an eval job while a prod push does not.
"""

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import uuid
from pathlib import Path
from typing import Any

import redis
import redis.asyncio as aioredis
from aci_protocol import STREAM_PAYLOAD_FIELD, EvalJob
from curie_api.config import get_settings
from curie_api.evalqueue import EvalQueue, from_stream_fields, now_iso

SECRET = get_settings().github_webhook_secret
REPO = "octo/k1-fanout"
VALID_FILES = {
    ".claude-plugin/plugin.json": '{"name": "demo-plugin", "version": "0.1.0"}',
    "skills/alpha/SKILL.md": "---\nname: alpha\ndescription: does alpha\n---\n",
}


def test_enqueue_lands_with_exact_shape() -> None:
    stream = f"curie:evals:test-{secrets.token_hex(4)}"
    agent_id, version_id = uuid.uuid4(), uuid.uuid4()
    request = EvalJob(
        agent_id=agent_id,
        version_id=version_id,
        sha="deadbeef",
        suite="default",
        bundle_ref="bundles/x/y.tar.gz",
        requested_at=now_iso(),
    )

    async def _enqueue() -> None:
        client = aioredis.from_url(get_settings().valkey_dsn())
        try:
            await EvalQueue(client, stream=stream).enqueue(request)
        finally:
            await client.aclose()

    asyncio.run(_enqueue())

    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        entries = sync.xrange(stream)
        assert len(entries) == 1
        _entry_id, fields = entries[0]
        payload = json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
        assert payload == {
            "agent_id": str(agent_id),
            "version_id": str(version_id),
            "sha": "deadbeef",
            "suite": "default",
            "bundle_ref": "bundles/x/y.tar.gz",
            "target_url": None,
            "model": None,
            "requested_at": request.requested_at,
        }
    finally:
        sync.delete(stream)
        sync.close()


def test_from_stream_fields_tolerates_an_unknown_field() -> None:
    """The reader side of the seam ignores fields it does not model.

    ``from_stream_fields`` is a wire READ, so a newer producer's optional field
    must decode, not raise. Asserted at the call site: the tolerance lives in
    which decode the function calls, not in the model.
    """

    payload = {
        "agent_id": str(uuid.uuid4()),
        "version_id": str(uuid.uuid4()),
        "sha": "deadbeef",
        "suite": "default",
        "bundle_ref": "bundles/x/y.tar.gz",
        "requested_at": now_iso(),
        "future_field": "from a newer producer",
    }
    job = from_stream_fields({STREAM_PAYLOAD_FIELD: json.dumps(payload)})
    assert job.sha == "deadbeef"
    assert not hasattr(job, "future_field")


# --- end to end: a dev push fans out, a prod push does not -----------------


def _git(*args: str, cwd: Path | None = None) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    out = subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    )
    return out.stdout.strip()


def _build_bare_repo(tmp_path: Path) -> tuple[str, str]:
    work = tmp_path / "work"
    work.mkdir()
    _git("init", "-q", "-b", "dev", cwd=work)
    for rel, content in VALID_FILES.items():
        path = work / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    _git("add", "-A", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    sha = _git("rev-parse", "HEAD", cwd=work)
    bare = tmp_path / "bare.git"
    _git("clone", "--quiet", "--bare", str(work), str(bare))
    return f"file://{bare}", sha


def _post_push(client: Any, ref: str, sha: str, clone_url: str) -> Any:
    body = json.dumps(
        {"ref": ref, "after": sha, "repository": {"full_name": REPO, "clone_url": clone_url}}
    ).encode()
    sig = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )


def _eval_entry_for(sha: str) -> dict[str, Any] | None:
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        for _id, fields in sync.xrevrange("curie:evals", count=50):
            payload = json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            if payload.get("sha") == sha:
                return payload
    finally:
        sync.close()
    return None


def _count_eval_entries_for_agent(agent_id: str) -> int:
    # Scope by agent_id (a fresh UUID per test) rather than sha or total count:
    # the shared curie:evals stream is never cleaned between tests, and two
    # tests building identical repo content can collide on sha.
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        return sum(
            1
            for _id, fields in sync.xrevrange("curie:evals", count=200)
            if json.loads(fields[STREAM_PAYLOAD_FIELD.encode()]).get("agent_id") == agent_id
        )
    finally:
        sync.close()


def test_dev_push_fans_out_prod_push_does_not(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path
) -> None:
    agent = client.post(
        "/agents",
        json={"name": "k1-fanout", "slack_channel": "C000000K01", "repo_full_name": REPO},
        headers=auth_headers,
    ).json()
    clone_url, sha = _build_bare_repo(tmp_path)

    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == ("deployed")
    entry = _eval_entry_for(sha)
    assert entry is not None, "dev push should fan out an eval job"
    assert entry["agent_id"] == agent["id"]
    assert entry["suite"] == "default"

    # A prod push (same sha) promotes but must NOT add another eval entry.
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        before = len(sync.xrange("curie:evals"))
    finally:
        sync.close()
    assert _post_push(client, "refs/heads/main", sha, clone_url).json()["status"] == ("promoted")
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        after = len(sync.xrange("curie:evals"))
    finally:
        sync.close()
    assert after == before


def test_redelivered_dev_push_does_not_refan_out(
    client: Any, auth_headers: dict[str, str], clean_db: None, tmp_path: Path
) -> None:
    agent = client.post(
        "/agents",
        json={"name": "k1-redeliver", "slack_channel": "C000000K01", "repo_full_name": REPO},
        headers=auth_headers,
    ).json()
    clone_url, sha = _build_bare_repo(tmp_path)

    # First delivery builds the bundle and fans out exactly one eval job.
    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == ("deployed")
    assert _count_eval_entries_for_agent(agent["id"]) == 1

    # GitHub redelivers the same push. The version already has a stored bundle,
    # so the build is skipped and no second eval job may be enqueued.
    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == ("deployed")
    assert _count_eval_entries_for_agent(agent["id"]) == 1
