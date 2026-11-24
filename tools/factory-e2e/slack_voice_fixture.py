"""Provision the offline weights and two authenticated media files for Slack voice replay."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripted_kind import ScriptedRun

MODEL_CLAIM = "slack-voice-model"
MODEL_NAME = "ggml-base.en.bin"
MODEL_SHA256 = "a03779c86df3323075f5e796cb2ce5029f00ec8869eee3fdfb897afe36c6d002"
MEDIA = {"F0EXAMPLE1": "retry-label.m4a", "F0EXAMPLE2": "silence.wav"}
_MEDIA_SERVICE = "slack-voice-media"
_TLS = "slack-voice-media-tls"
_TRUST = "slack-voice-media-ca"
_SEED = "slack-voice-model-loader"


def _model_path(run: ScriptedRun) -> Path:
    value = run.env.get("CURIE_FACTORY_VOICE_MODEL_PATH", "")
    if not value:
        raise ValueError("set CURIE_FACTORY_VOICE_MODEL_PATH to the local pinned model")
    return Path(value).expanduser().resolve()


def validate_model(run: ScriptedRun) -> None:
    """Check the operator-supplied bytes before building images or creating a cluster."""
    path = _model_path(run)
    try:
        if not path.is_file() or path.stat().st_size != 147_964_211:
            raise ValueError("the local Slack voice model must be the pinned base.en weights")
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
    except OSError:
        raise ValueError("the local Slack voice model could not be read") from None
    if digest != MODEL_SHA256:
        raise ValueError("the local Slack voice model has the wrong SHA-256")


def _fixture_pod(
    run: ScriptedRun,
    name: str,
    command: list[str],
    volumes: dict[str, tuple[dict[str, Any], str]],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": {"app": name}},
        "spec": {
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "restartPolicy": "Never",
            "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "fsGroup": 1000},
            "containers": [
                {
                    "name": "fixture",
                    "image": run.image_refs[0],
                    "imagePullPolicy": "Never",
                    "command": command,
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                    },
                    "volumeMounts": [
                        {
                            "name": key,
                            "mountPath": path,
                            "readOnly": "persistentVolumeClaim" not in src,
                        }
                        for key, (src, path) in volumes.items()
                    ],
                }
            ],
            "volumes": [{"name": key, **source} for key, (source, _) in volumes.items()],
        },
    }


def prepare(run: ScriptedRun) -> None:
    """Create namespace-owned fixtures after the candidate API image is loaded into kind."""
    from scripted_kind import SCRIPTED_SLACK_BOT_TOKEN

    assert run.workdir is not None
    vectors = Path(__file__).resolve().parents[2] / "tests/vectors/factory-voice"

    def kubectl(*args: str) -> str:
        return run.kubectl("-n", run.namespace, *args)

    with TemporaryDirectory(dir=run.workdir) as scratch:
        root = Path(scratch)
        ca_key, ca = root / "ca.key", root / "ca.crt"
        key, cert, request = root / "server.key", root / "server.crt", root / "server.csr"
        run.command(
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=Slack fixture CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca),
        )
        run.command(
            "openssl",
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=files.slack.com",
            "-keyout",
            str(key),
            "-out",
            str(request),
        )
        extensions = root / "extensions"
        extensions.write_text(
            "subjectAltName=DNS:files.slack.com\nbasicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
            "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid\n"
        )
        run.command(
            "openssl",
            "x509",
            "-req",
            "-days",
            "1",
            "-in",
            str(request),
            "-CA",
            str(ca),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-extfile",
            str(extensions),
            "-out",
            str(cert),
        )
        kubectl("create", "secret", "tls", _TLS, f"--cert={cert}", f"--key={key}")
        kubectl("create", "configmap", _TRUST, f"--from-file=ca.crt={ca}")
        kubectl(
            "create",
            "configmap",
            _MEDIA_SERVICE,
            f"--from-file=fixture.py={Path(__file__).resolve()}",
            *(f"--from-file={name}={vectors / name}" for name in MEDIA.values()),
        )
        claim = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": MODEL_CLAIM},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "256Mi"}},
            },
        }
        seed = _fixture_pod(
            run,
            _SEED,
            ["python", "-c", "import signal; signal.pause()"],
            {"model": ({"persistentVolumeClaim": {"claimName": MODEL_CLAIM}}, "/model")},
        )
        media = _fixture_pod(
            run,
            _MEDIA_SERVICE,
            ["python", "/media/fixture.py"],
            {
                "data": ({"configMap": {"name": _MEDIA_SERVICE}}, "/media"),
                "tls": ({"secret": {"secretName": _TLS, "defaultMode": 0o440}}, "/tls"),
            },
        )
        server = media["spec"]["containers"][0]
        server["env"] = [{"name": "FIXTURE_BOT_TOKEN", "value": SCRIPTED_SLACK_BOT_TOKEN}]
        server["readinessProbe"] = {"tcpSocket": {"port": 8443}}
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": _MEDIA_SERVICE},
            "spec": {
                "selector": {"app": _MEDIA_SERVICE},
                "ports": [{"name": "https", "port": 443, "targetPort": 8443}],
            },
        }
        manifest = root / "fixture.json"
        manifest.write_text(
            json.dumps({"apiVersion": "v1", "kind": "List", "items": [claim, seed, media, service]})
        )
        kubectl("create", "-f", str(manifest))
        kubectl("wait", "--for=condition=Ready", f"pod/{_SEED}", "--timeout=180s")
        kubectl("cp", "--no-preserve", str(_model_path(run)), f"{_SEED}:/model/{MODEL_NAME}")
        kubectl(
            "exec",
            _SEED,
            "--",
            "python",
            "-c",
            "from curie_api.factory_voice_model import pinned_model\n"
            f"with open('/model/{MODEL_NAME}', 'rb') as weights:\n"
            "    raise SystemExit(0 if pinned_model(weights) else 1)",
        )
        kubectl("delete", "pod", _SEED, "--wait=true", "--timeout=120s")
        kubectl("wait", "--for=condition=Ready", f"pod/{_MEDIA_SERVICE}", "--timeout=180s")


def attach_gateway(run: ScriptedRun) -> None:
    """Bind only this scripted install's gateway to the disposable media service."""
    from scripted_kind import SCRIPTED_SLACK_BOT_TOKEN

    if not run.slack or run.context != f"kind-{run.cluster}":
        raise ValueError("the media fixture requires the run's isolated kind gateway")
    gateway = f"deployment/{run.release}-channel-gateway"

    def kubectl(*args: str) -> str:
        return run.kubectl("-n", run.namespace, *args)

    kubectl("rollout", "status", gateway, "--timeout=300s")
    kubectl(
        "exec",
        gateway,
        "--",
        "python",
        "-c",
        "import hmac, os, sys\n"
        "from curie_channel_gateway.slack.tokens import SlackProviderConfig, slack_bot_tokens\n"
        "tokens = slack_bot_tokens(SlackProviderConfig())\n"
        "ok = (set(tokens) == {'default'} and hmac.compare_digest(tokens['default'], sys.argv[1])"
        " and os.environ.get('SLACK_API_BASE_URL') == sys.argv[2])\n"
        "raise SystemExit(0 if ok else 1)",
        SCRIPTED_SLACK_BOT_TOKEN,
        run.slack.base_url,
    )
    address = kubectl("get", "svc", _MEDIA_SERVICE, "-o", "jsonpath={.spec.clusterIP}")
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "hostAliases": [
                        {
                            "ip": str(ipaddress.ip_address(address.strip())),
                            "hostnames": ["files.slack.com"],
                        }
                    ],
                    "volumes": [{"name": "voice-fixture-ca", "configMap": {"name": _TRUST}}],
                    "containers": [
                        {
                            "name": "channel-gateway",
                            "volumeMounts": [
                                {
                                    "name": "voice-fixture-ca",
                                    "mountPath": "/voice-fixture-ca",
                                    "readOnly": True,
                                }
                            ],
                            "env": [
                                {"name": "SSL_CERT_FILE", "value": "/voice-fixture-ca/ca.crt"},
                                {"name": "NO_PROXY", "value": "*"},
                            ],
                        }
                    ],
                }
            }
        }
    }
    kubectl("patch", gateway, "--type=strategic", "-p", json.dumps(patch))
    kubectl("rollout", "status", gateway, "--timeout=300s")
    kubectl(
        "exec",
        gateway,
        "--",
        "python",
        "-c",
        "import socket, sys\n"
        "ips = {row[4][0] for row in socket.getaddrinfo('files.slack.com', 443)}\n"
        "raise SystemExit(0 if ips == {sys.argv[1]} else 1)",
        address.strip(),
    )


class MediaHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args: Any) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        expected = "Bearer " + os.environ["FIXTURE_BOT_TOKEN"]
        if not hmac.compare_digest(
            self.headers.get("Authorization", "").encode(), expected.encode()
        ):
            self.send_error(403)
            return
        name = MEDIA.get(self.path.removeprefix("/"))
        if name is None:
            self.send_error(404)
            return
        data = Path("/media", name).read_bytes()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Type", "audio/mp4" if name.endswith(".m4a") else "audio/wav")
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
    with ThreadingHTTPServer(("0.0.0.0", 8443), MediaHandler) as server:
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.serve_forever()
