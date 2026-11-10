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
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import STREAM_PAYLOAD_FIELD, EvalJob
from curie_api.config import get_settings
from curie_api.evalqueue import EvalQueue, from_stream_fields, now_iso
from curie_worker.config import WorkerConfig

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


def test_default_queue_enqueues_onto_the_worker_consumer_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One CURIE_EVAL_STREAM value moves the API producer (as main.py
    constructs it) and the worker consumer together (#3565)."""
    stream = f"curie:evals:test-{secrets.token_hex(4)}"
    monkeypatch.setenv("CURIE_EVAL_STREAM", stream)
    get_settings.cache_clear()
    request = EvalJob(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        sha="deadbeef",
        suite="default",
        bundle_ref="bundles/x/y.tar.gz",
        requested_at=now_iso(),
    )
    try:
        dsn = get_settings().valkey_dsn()

        async def _enqueue() -> None:
            client = aioredis.from_url(dsn)
            try:
                await EvalQueue(client).enqueue(request)
            finally:
                await client.aclose()

        asyncio.run(_enqueue())

        sync = redis.from_url(dsn)
        try:
            assert sync.xlen(stream) == 1
            assert stream == WorkerConfig().eval_stream
        finally:
            sync.delete(stream)
            sync.close()
    finally:
        get_settings.cache_clear()


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


@pytest.fixture
def trusted_clone_base(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[Path]:
    """Point `GITHUB_CLONE_BASE` at a per-test `file://` base and yield it.

    This file carries its own copy rather than importing
    `test_gitflow_integration`'s. `routers/github.py` calls `get_settings()` per
    request, so the app needs no rebuild; the clear-AFTER-undo ordering at
    teardown is load-bearing, since clearing first would re-cache the still-set
    env value into a later test.
    """

    base = tmp_path / "remotes"
    base.mkdir()
    monkeypatch.setenv("GITHUB_CLONE_BASE", f"file://{base}")
    get_settings.cache_clear()
    yield base
    monkeypatch.undo()
    get_settings.cache_clear()


def _build_bare_repo(base_dir: Path, repo_full_name: str) -> tuple[str, str]:
    """Create a bare repo at `<base_dir>/<repo_full_name>.git`. Returns (url, sha).

    The returned URL is byte-identical to the origin the API derives from
    `GITHUB_CLONE_BASE` plus the agent's stored `repo_full_name`, so the fan-out
    path exercises the derivation instead of bypassing it.
    """

    work = base_dir / "_work" / repo_full_name
    work.mkdir(parents=True)
    _git("init", "-q", "-b", "dev", cwd=work)
    for rel, content in VALID_FILES.items():
        path = work / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    _git("add", "-A", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    sha = _git("rev-parse", "HEAD", cwd=work)
    _git("branch", "main", cwd=work)
    bare = base_dir / f"{repo_full_name}.git"
    bare.parent.mkdir(parents=True, exist_ok=True)
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
            "X-GitHub-Delivery": str(uuid.uuid4()),
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
            if json.loads(fields[STREAM_PAYLOAD_FIELD.encode()]).get("agent_id")
            == agent_id
        )
    finally:
        sync.close()


def test_dev_push_fans_out_prod_push_does_not(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent = client.post(
        "/agents",
        json={
            "name": "k1-fanout",
            "channel": {"kind": "slack", "address": "C000000K01"},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    ).json()
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO)

    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == (
        "deployed"
    )
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
    assert _post_push(client, "refs/heads/main", sha, clone_url).json()["status"] == (
        "promoted"
    )
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        after = len(sync.xrange("curie:evals"))
    finally:
        sync.close()
    assert after == before


def test_redelivered_dev_push_does_not_refan_out(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    trusted_clone_base: Path,
) -> None:
    agent = client.post(
        "/agents",
        json={
            "name": "k1-redeliver",
            "channel": {"kind": "slack", "address": "C000000K01"},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    ).json()
    clone_url, sha = _build_bare_repo(trusted_clone_base, REPO)

    # First delivery builds the bundle and fans out exactly one eval job.
    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == (
        "deployed"
    )
    assert _count_eval_entries_for_agent(agent["id"]) == 1

    # GitHub redelivers the same push. The version already has a stored bundle,
    # so the build is skipped and no second eval job may be enqueued.
    assert _post_push(client, "refs/heads/dev", sha, clone_url).json()["status"] == (
        "deployed"
    )
    assert _count_eval_entries_for_agent(agent["id"]) == 1
