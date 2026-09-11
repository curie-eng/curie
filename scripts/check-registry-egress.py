#!/usr/bin/env python3
"""Prove the chart's runner registry egress deny/admit/revoke boundary.

This creates its own kind cluster and binds a chart-rendered SandboxTemplate to
a probe Pod.  It intentionally does not run the sandbox controller, create a
SandboxClaim, call a model, or prove a hostname-stable allowlist.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import yaml

RUNNER_IMAGE = (
    "ghcr.io/curie-eng/curie-runner@"
    "sha256:c31e9f585d3f74a6a554b575042c0d71421de9603c038ced27ae5b393e0dc272"
)
KIND_IMAGE = "kindest/node@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93"
CALICO_URL = "https://raw.githubusercontent.com/projectcalico/calico/v3.29.3/manifests/calico.yaml"
CALICO_SHA256 = "9a575859428b822a224dedafc4238555b6b0f910f2abf12983f20f871860914e"
PACKAGE = "packaging==25.0"


class Check:
    def __init__(self, output: Path, runner_image: str) -> None:
        self.output = output
        self.repo = Path(__file__).resolve().parent.parent
        self.runner_image = runner_image
        suffix = uuid.uuid4().hex[:10]
        self.cluster = f"curie-registry-{suffix}"
        self.release = f"registry-{suffix}"
        self.namespace = self.release
        self.pod = "registry-egress-probe"
        self.commands: list[dict[str, Any]] = []
        self.phases: dict[str, dict[str, int]] = {}
        self.cluster_attempted = False
        self.chart_sha256 = ""
        self.temp: tempfile.TemporaryDirectory[str] | None = None
        self.kubeconfig: Path | None = None

    def write_json(self, name: str, value: Any) -> None:
        (self.output / name).write_text(json.dumps(value, indent=2) + "\n")

    def run(
        self, name: str, args: list[str], timeout: int = 120, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        started = time.monotonic()
        try:
            result = subprocess.run(
                args, cwd=self.repo, text=True, capture_output=True, timeout=timeout
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            result = subprocess.CompletedProcess(
                args, 124, stdout, stderr + f"\ntimeout after {timeout}s\n"
            )
        record = {
            "name": name,
            "command": args,
            "exit_code": result.returncode,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        self.commands.append(record)
        self.write_json(f"command-{len(self.commands):02d}-{name}.json", record)
        if check and result.returncode != 0:
            raise RuntimeError(f"{name} exited {result.returncode}; see command log")
        return result

    def kubectl(self, *args: str) -> list[str]:
        assert self.kubeconfig is not None
        return ["kubectl", "--kubeconfig", str(self.kubeconfig), *args]

    def render(self, values: Path, artifact: str) -> list[dict[str, Any]]:
        result = self.run(
            f"render-{artifact}",
            [
                "helm",
                "template",
                self.release,
                "charts/curie",
                "--namespace",
                self.namespace,
                "-f",
                str(values),
                "-s",
                "templates/agent-sandbox.yaml",
                "-s",
                "templates/security-networkpolicy.yaml",
                "-s",
                "templates/priorityclass.yaml",
            ],
        )
        (self.output / f"rendered-{artifact}.yaml").write_text(result.stdout)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def probe(self, phase: str, ip: str) -> None:
        tcp_code = (
            "import socket,sys; s=socket.socket(); s.settimeout(3); "
            "\ntry: s.connect((sys.argv[1],443)); print('TCP443_CONNECTED')"
            "\nexcept OSError as e: print('TCP443_REFUSED',type(e).__name__); sys.exit(1)"
        )
        tcp = self.run(
            f"tcp-{phase}",
            self.kubectl(
                "exec",
                "-n",
                self.namespace,
                self.pod,
                "-c",
                "runner",
                "--",
                "python",
                "-c",
                tcp_code,
                ip,
            ),
            timeout=15,
            check=False,
        )
        venv = f"/workspace/.venv-{phase}"
        pip_script = f"""set -eu
python -m venv {venv}
{venv}/bin/python -c "import importlib.util; assert importlib.util.find_spec('packaging') is None"
set +e
PIP_CONFIG_FILE=/dev/null {venv}/bin/python -m pip --isolated install \
  --disable-pip-version-check --no-cache-dir --index-url https://pypi.org/simple \
  --retries 0 --timeout 5 --only-binary=:all: --no-deps \
  --report /workspace/report-{phase}.json {PACKAGE}
rc=$?
set -e
if [ "$rc" -eq 0 ]; then
  {venv}/bin/python -c "import packaging; assert packaging.__version__ == '25.0'"
else
  {venv}/bin/python -c "import importlib.util; assert importlib.util.find_spec('packaging') is None"
fi
exit "$rc"
"""
        pip = self.run(
            f"pip-{phase}",
            self.kubectl(
                "exec",
                "-n",
                self.namespace,
                self.pod,
                "-c",
                "runner",
                "--",
                "sh",
                "-c",
                pip_script,
            ),
            timeout=100,
            check=False,
        )
        expected = 0 if phase == "admitted" else 1
        self.phases[phase] = {
            "tcp_exit_code": tcp.returncode,
            "pip_exit_code": pip.returncode,
        }
        if tcp.returncode != expected or pip.returncode != expected:
            raise AssertionError(
                f"{phase}: expected TCP and pip exit {expected}, got "
                f"{tcp.returncode} and {pip.returncode}"
            )

    def execute(self) -> None:
        for tool in ("docker", "kind", "kubectl", "helm"):
            if shutil.which(tool) is None:
                raise RuntimeError(f"required executable not found: {tool}")
        image_match = re.fullmatch(r"(.+)@(sha256:[0-9a-f]{64})", self.runner_image)
        if not image_match:
            raise ValueError("--runner-image must be an immutable sha256 image reference")
        chart_hash = hashlib.sha256()
        for path in sorted((self.repo / "charts/curie").rglob("*")):
            if path.is_file():
                relative = path.relative_to(self.repo).as_posix()
                chart_hash.update(relative.encode() + b"\0" + path.read_bytes() + b"\0")
        self.chart_sha256 = chart_hash.hexdigest()

        self.temp = tempfile.TemporaryDirectory(prefix="curie-registry-egress-")
        self.kubeconfig = Path(self.temp.name) / "kubeconfig"
        kind_config = self.output / "kind.yaml"
        kind_config.write_text(
            "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n"
            "  disableDefaultCNI: true\n  podSubnet: 192.168.0.0/16\n"
            "nodes:\n  - role: control-plane\n"
        )
        calico = self.output / "calico.yaml"
        with urllib.request.urlopen(CALICO_URL, timeout=30) as response:
            calico.write_bytes(response.read())
        digest = hashlib.sha256(calico.read_bytes()).hexdigest()
        if digest != CALICO_SHA256:
            raise AssertionError(f"Calico manifest digest mismatch: {digest}")

        self.cluster_attempted = True
        self.run(
            "kind-create",
            [
                "kind",
                "create",
                "cluster",
                "--name",
                self.cluster,
                "--image",
                KIND_IMAGE,
                "--config",
                str(kind_config),
                "--kubeconfig",
                str(self.kubeconfig),
            ],
            timeout=360,
        )
        self.run("calico-apply", self.kubectl("apply", "-f", str(calico)), timeout=120)
        self.run(
            "calico-ready",
            self.kubectl(
                "rollout", "status", "daemonset/calico-node", "-n", "kube-system", "--timeout=300s"
            ),
            timeout=320,
        )
        self.run(
            "nodes-ready",
            self.kubectl("wait", "--for=condition=Ready", "nodes", "--all", "--timeout=300s"),
            timeout=320,
        )

        base_values = {
            "agentSandbox": {
                "controller": {"deploy": False},
                "runner": {
                    "image": image_match.group(1),
                    "digest": image_match.group(2),
                    "bundleFetch": {"enabled": False},
                    "serviceAccount": {"create": False},
                },
            },
            "security": {"networkPolicy": {"allowedEgress": []}},
        }
        denied_values = self.output / "values-denied.yaml"
        denied_values.write_text(yaml.safe_dump(base_values, sort_keys=False))
        denied_docs = self.render(denied_values, "denied")
        sandbox = next(doc for doc in denied_docs if doc.get("kind") == "SandboxTemplate")
        if sandbox["spec"].get("networkPolicyManagement") != "Unmanaged":
            raise AssertionError("SandboxTemplate is not bound to chart-managed NetworkPolicy")
        self.write_json("sandbox-template.json", sandbox)

        spec = copy.deepcopy(sandbox["spec"]["podTemplate"]["spec"])
        runner = next(
            container for container in spec["containers"] if container["name"] == "runner"
        )
        if runner["image"] != self.runner_image:
            raise AssertionError(f"rendered runner image is not pinned: {runner['image']}")
        runner["command"] = ["sleep", "3600"]
        runner.pop("args", None)
        for key in ("readinessProbe", "livenessProbe", "startupProbe"):
            runner.pop(key, None)
        spec["restartPolicy"] = "Never"
        spec["automountServiceAccountToken"] = False
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self.pod,
                "namespace": self.namespace,
                "labels": sandbox["spec"]["podTemplate"]["metadata"]["labels"],
            },
            "spec": spec,
        }
        self.write_json("pod.json", pod)
        policies = [doc for doc in denied_docs if doc.get("kind") == "NetworkPolicy"]
        (self.output / "policies-denied.yaml").write_text(
            yaml.safe_dump_all(policies, sort_keys=False)
        )
        classes = [doc for doc in denied_docs if doc.get("kind") == "PriorityClass"]
        for item in classes:
            item["value"] = int(float(item["value"]))
        (self.output / "priorityclasses.yaml").write_text(
            yaml.safe_dump_all(classes, sort_keys=False)
        )

        self.run("namespace-create", self.kubectl("create", "namespace", self.namespace))
        self.run(
            "priorityclasses-apply",
            self.kubectl("apply", "-f", str(self.output / "priorityclasses.yaml")),
        )
        self.run(
            "policies-apply",
            self.kubectl(
                "apply",
                "-n",
                self.namespace,
                "-f",
                str(self.output / "policies-denied.yaml"),
            ),
        )
        self.run(
            "pod-apply",
            self.kubectl("apply", "-n", self.namespace, "-f", str(self.output / "pod.json")),
        )
        self.run(
            "pod-ready",
            self.kubectl(
                "wait",
                "--for=condition=Ready",
                f"pod/{self.pod}",
                "-n",
                self.namespace,
                "--timeout=300s",
            ),
            timeout=320,
        )

        dns_code = (
            "import json,socket; h=['pypi.org','files.pythonhosted.org']; "
            "print(json.dumps({x:sorted({r[4][0] for r in socket.getaddrinfo(x,443,"
            "socket.AF_INET,socket.SOCK_STREAM)}) for x in h}))"
        )
        dns_result = self.run(
            "dns",
            self.kubectl(
                "exec",
                "-n",
                self.namespace,
                self.pod,
                "-c",
                "runner",
                "--",
                "python",
                "-c",
                dns_code,
            ),
            timeout=30,
        )
        answers = json.loads(dns_result.stdout)
        if any(not answers.get(host) for host in ("pypi.org", "files.pythonhosted.org")):
            raise AssertionError("both registry hostnames must have live IPv4 answers")
        self.write_json("dns.json", {"hostnames": answers})
        ips = sorted({ip for values in answers.values() for ip in values})
        control_ip = ips[0]
        self.probe("denied", control_ip)

        admitted_values_data = copy.deepcopy(base_values)
        admitted_values_data["security"]["networkPolicy"]["allowedEgress"] = [
            {"cidr": f"{ip}/32", "ports": [{"protocol": "TCP", "port": 443}]} for ip in ips
        ]
        admitted_values = self.output / "values-admitted.yaml"
        admitted_values.write_text(yaml.safe_dump(admitted_values_data, sort_keys=False))
        admitted_docs = self.render(admitted_values, "admitted")
        allow = next(
            doc
            for doc in admitted_docs
            if doc.get("kind") == "NetworkPolicy"
            and doc["metadata"]["name"].endswith("runner-allow-egress")
        )
        (self.output / "allow.yaml").write_text(yaml.safe_dump(allow, sort_keys=False))
        self.run(
            "allow-apply",
            self.kubectl("apply", "-n", self.namespace, "-f", str(self.output / "allow.yaml")),
        )
        time.sleep(3)
        self.probe("admitted", control_ip)
        pip_report = self.run(
            "pip-report-admitted",
            self.kubectl(
                "exec",
                "-n",
                self.namespace,
                self.pod,
                "-c",
                "runner",
                "--",
                "cat",
                "/workspace/report-admitted.json",
            ),
        )
        (self.output / "pip-report-admitted.json").write_text(pip_report.stdout)
        self.run(
            "allow-delete",
            self.kubectl("delete", "-n", self.namespace, "-f", str(self.output / "allow.yaml")),
        )
        time.sleep(3)
        self.probe("revoked", control_ip)
        live_pod = self.run(
            "live-pod",
            self.kubectl("get", "pod", self.pod, "-n", self.namespace, "-o", "json"),
        )
        (self.output / "live-pod.json").write_text(live_pod.stdout)
        phase_order = ("denied", "admitted", "revoked")
        if [self.phases[p]["pip_exit_code"] for p in phase_order] != [1, 0, 1]:
            raise AssertionError("pip sequence was not [1, 0, 1]")
        if [self.phases[p]["tcp_exit_code"] for p in phase_order] != [1, 0, 1]:
            raise AssertionError("TCP sequence was not [1, 0, 1]")

    def cleanup(self) -> None:
        if not self.cluster_attempted:
            return
        assert self.kubeconfig is not None
        deleted = self.run(
            "kind-delete",
            [
                "kind",
                "delete",
                "cluster",
                "--name",
                self.cluster,
                "--kubeconfig",
                str(self.kubeconfig),
            ],
            timeout=180,
            check=False,
        )
        inventory = self.run(
            "kind-inventory-after",
            ["kind", "get", "clusters"],
            timeout=30,
            check=False,
        )
        still_exists = self.cluster in inventory.stdout.splitlines()
        if deleted.returncode != 0 or inventory.returncode != 0 or still_exists:
            raise RuntimeError(f"cleanup failed for owned cluster {self.cluster}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="new or empty directory for manifests, command logs, and results",
    )
    parser.add_argument(
        "--runner-image",
        default=RUNNER_IMAGE,
        help="public immutable runner image (default: release 0.8.7)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        print(f"output directory must be empty: {output}", file=sys.stderr)
        return 2
    check = Check(output, args.runner_image)
    error: str | None = None
    cleanup_error: str | None = None
    try:
        check.execute()
    except Exception as exc:  # Keep partial evidence for diagnosis.
        error = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            check.cleanup()
        except Exception as exc:
            cleanup_error = f"{type(exc).__name__}: {exc}"
        if check.temp is not None:
            check.temp.cleanup()
        result = {
            "status": "passed" if error is None and cleanup_error is None else "failed",
            "completed_utc": dt.datetime.now(dt.UTC).isoformat(),
            "cluster": check.cluster,
            "runner_image": check.runner_image,
            "chart_sha256": check.chart_sha256,
            "kind_image": KIND_IMAGE,
            "calico": {"url": CALICO_URL, "sha256": CALICO_SHA256},
            "scope": "chart-rendered bound Pod only; no controller, claim, model, or publication",
            "phases": check.phases,
            "error": error,
            "cleanup_error": cleanup_error,
        }
        check.write_json("results.json", result)
    if error or cleanup_error:
        print(error or cleanup_error, file=sys.stderr)
        return 1
    print(f"registry egress check passed; evidence: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
