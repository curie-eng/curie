"""Production request protocol checks. @spec SRE-CREDS-5 SRE-CREDS-8"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT = Path(__file__).parents[1] / "sre-bot" / "observability" / "adapter-credentials" / "sync.py"


# @spec SRE-CREDS-5 SRE-CREDS-8
def test_service_account_transport_uses_tls_timeout_and_merge_patch(
    tmp_path: Path, monkeypatch: Any
) -> None:
    spec = importlib.util.spec_from_file_location("adapter_credentials_transport_check", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    (tmp_path / "token").write_text("synthetic-token\n")
    (tmp_path / "ca.crt").write_text("synthetic-ca")
    monkeypatch.setenv("SA_DIR", str(tmp_path))
    tls_context = object()
    ca_paths: list[str] = []
    observations: list[tuple[Any, Any]] = []
    handlers: list[Any] = []

    def create_context(*, cafile: str) -> object:
        ca_paths.append(cafile)
        return tls_context

    class Opener:
        def open(self, request: Any, data: Any = None, timeout: Any = None) -> io.BytesIO:
            observations.append((request, timeout))
            return io.BytesIO(b'{"kind":"Secret"}')

    def build_opener(*given_handlers: Any) -> Opener:
        handlers.extend(given_handlers)
        return Opener()

    monkeypatch.setattr(module.ssl, "create_default_context", create_context)
    monkeypatch.setattr(module.urllib.request, "build_opener", build_opener)
    client = module.ServiceAccountRequest()
    patch = {"metadata": {"resourceVersion": "4"}, "data": {"map": "e30="}}
    assert client("PATCH", "/api/v1/namespaces/acme/secrets/target", patch) == {"kind": "Secret"}
    assert ca_paths == [str(tmp_path / "ca.crt")]
    request, timeout = observations[0]
    assert (
        request.full_url == "https://kubernetes.default.svc/api/v1/namespaces/acme/secrets/target"
    )
    assert request.get_method() == "PATCH"
    assert request.get_header("Authorization") == "Bearer synthetic-token"
    assert request.get_header("Content-type") == "application/merge-patch+json"
    assert json.loads(request.data) == patch
    assert handlers[0]._context is tls_context
    assert isinstance(handlers[1], module.RejectRedirect)
    assert (
        handlers[1].redirect_request(None, None, 302, "redirect", {}, "https://elsewhere") is None
    )
    assert isinstance(timeout, (int, float)) and 0 < timeout < 60

    assert client("GET", "/api/v1/namespaces/acme/secrets/target") == {"kind": "Secret"}
    get_request = observations[1][0]
    assert get_request.get_method() == "GET"
    assert get_request.data is None
    assert get_request.get_header("Content-type") is None
