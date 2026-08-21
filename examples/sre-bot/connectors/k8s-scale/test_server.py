"""Tests for the scale connector.

The load-bearing one is `test_a_failed_read_refuses_instead_of_scaling`. This
tool's reason to exist is that its reply can be trusted as a snapshot, so a path
where the write lands but the prior state does not is worse than no write at all:
the platform would hold an action it believes happened and cannot undo.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest
import yaml

_MODULE_NAME = "sre_bot_k8s_scale_server"
_SERVER_PY = Path(__file__).parent / "server.py"

GOOD_KUBECONFIG = {
    "clusters": [{"cluster": {"server": "https://k8s.example:6443"}}],
    "users": [{"user": {"token": "scale-token"}}],
}


def _load(tmp_path, kubeconfig=GOOD_KUBECONFIG, allowlist="public/api", ceiling="50"):
    cfg = tmp_path / "kubeconfig"
    cfg.write_text(yaml.safe_dump(kubeconfig), encoding="utf-8")
    os.environ["KUBECONFIG_PATH"] = str(cfg)
    os.environ["K8S_SCALE_ALLOWLIST"] = allowlist
    os.environ["K8S_SCALE_MAX_REPLICAS"] = ceiling
    sys.modules.pop(_MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SERVER_PY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


class _Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


class _FakeClient:
    """Records what the tool asked the API server to do."""

    def __init__(self, seen, get=(200, {"spec": {"replicas": 3}}), patch=(200, {})):
        self.seen = seen
        self._get = get
        self._patch = patch

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, path):
        self.seen["get_path"] = path
        return _Response(*self._get)

    def patch(self, path, body):
        self.seen["patch_path"] = path
        self.seen["patch_body"] = body
        return _Response(*self._patch)


def test_the_reply_carries_the_replica_count_read_before_the_patch(tmp_path, monkeypatch):
    """Without prior state the action is not undoable, which is the whole point."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    result = json.loads(srv.scale_deployment("public", "api", 10))
    assert result["ok"] is True
    assert result["prior"] == {"spec": {"replicas": 3}}
    assert result["target"] == {"namespace": "public", "name": "api"}
    assert "from 3 to 10" in result["summary"]


def test_it_writes_through_the_scale_subresource(tmp_path, monkeypatch):
    """The narrow grant is the security argument; patching the Deployment is not it."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    srv.scale_deployment("public", "api", 10)
    assert seen["get_path"] == "/apis/apps/v1/namespaces/public/deployments/api/scale"
    assert seen["patch_path"] == "/apis/apps/v1/namespaces/public/deployments/api/scale"


def test_the_patch_body_only_ever_sets_replicas(tmp_path, monkeypatch):
    """Caller input reaches exactly one integer and nothing else."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    srv.scale_deployment("public", "api", 7)
    assert seen["patch_body"] == {"spec": {"replicas": 7}}


def test_a_failed_read_refuses_instead_of_scaling(tmp_path, monkeypatch):
    """No trustworthy prior state means no write: an un-undoable action is worse."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, get=(403, {})))
    result = json.loads(srv.scale_deployment("public", "api", 10))
    assert result["ok"] is False
    assert result["prior"] is None
    assert "patch_body" not in seen


def test_a_read_with_no_replica_count_refuses(tmp_path, monkeypatch):
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, get=(200, {"spec": {}})))
    result = json.loads(srv.scale_deployment("public", "api", 10))
    assert result["ok"] is False
    assert "prior state" in result["summary"]
    assert "patch_body" not in seen


@pytest.mark.parametrize("ns,name", [("public", "not-listed"), ("platform", "api")])
def test_a_target_outside_the_allowlist_never_reaches_the_api(tmp_path, monkeypatch, ns, name):
    srv = _load(tmp_path)
    def explode():
        raise AssertionError("a client must never be built for a refused target")
    monkeypatch.setattr(srv, "_client", explode)
    result = json.loads(srv.scale_deployment(ns, name, 10))
    assert result["ok"] is False
    assert "allowlist" in result["summary"]


def test_the_ceiling_refuses_before_a_client_is_built(tmp_path, monkeypatch):
    """Scale to ten thousand is a denial of service with an approval on it."""
    srv = _load(tmp_path, ceiling="50")
    def explode():
        raise AssertionError("a client must never be built for a refused target")
    monkeypatch.setattr(srv, "_client", explode)
    result = json.loads(srv.scale_deployment("public", "api", 10_000))
    assert result["ok"] is False
    assert "ceiling" in result["summary"]


@pytest.mark.parametrize("bad", [True, "3", 3.5, None])
def test_a_non_integer_replica_count_is_refused(tmp_path, monkeypatch, bad):
    """bool is an int in Python; a caller passing True must not scale to 1."""
    srv = _load(tmp_path)
    def explode():
        raise AssertionError("a client must never be built for a refused target")
    monkeypatch.setattr(srv, "_client", explode)
    result = json.loads(srv.scale_deployment("public", "api", bad))
    assert result["ok"] is False
    assert "integer" in result["summary"]


def test_every_return_path_is_the_same_shape(tmp_path, monkeypatch):
    """A caller never has to guess which keys are present."""
    srv = _load(tmp_path)
    seen = {}
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen))
    ok = json.loads(srv.scale_deployment("public", "api", 4))
    monkeypatch.setattr(srv, "_client", lambda: _FakeClient(seen, get=(500, {})))
    bad = json.loads(srv.scale_deployment("public", "api", 4))
    assert set(ok) == set(bad) == {"ok", "summary", "prior", "target"}


def test_insecure_skip_tls_verify_is_refused(tmp_path, monkeypatch):
    srv = _load(tmp_path, kubeconfig={
        "clusters": [{"cluster": {"server": "https://k8s.example:6443",
                                  "insecure-skip-tls-verify": True}}],
        "users": [{"user": {"token": "scale-token"}}],
    })
    result = json.loads(srv.scale_deployment("public", "api", 3))
    assert result["ok"] is False
    assert "insecure-skip-tls-verify" in result["summary"]
