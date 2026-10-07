"""Run the generated rules in Prometheus. @spec SRE-SCHEDULED-JOBS c1, c2, c3, c5"""

from __future__ import annotations

import json
import pathlib
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scheduled_job_alerts import scheduled_job_group


def run() -> None:
    """@spec SRE-SCHEDULED-JOBS c1, c2, c3, c5"""
    namespace = "acme-system"
    group = scheduled_job_group(namespace, "Maintenance job health")
    metrics = (
        "\n".join(
            [
                f'kube_cronjob_spec_suspend{{namespace="{namespace}",cronjob="suspended"}} 1',
                f'kube_cronjob_spec_suspend{{namespace="{namespace}",cronjob="healthy"}} 0',
                "kube_cronjob_status_last_schedule_time"
                f'{{namespace="{namespace}",cronjob="failing"}} 200',
                "kube_cronjob_status_last_successful_time"
                f'{{namespace="{namespace}",cronjob="failing"}} 100',
                "kube_cronjob_status_last_schedule_time"
                f'{{namespace="{namespace}",cronjob="never-success"}} 200',
                "kube_cronjob_status_last_schedule_time"
                f'{{namespace="{namespace}",cronjob="healthy"}} 200',
                "kube_cronjob_status_last_successful_time"
                f'{{namespace="{namespace}",cronjob="healthy"}} 200',
                'kube_cronjob_spec_suspend{namespace="other-system",cronjob="excluded"} 1',
            ]
        )
        + "\n"
    )

    class Metrics(BaseHTTPRequestHandler):
        """@spec SRE-SCHEDULED-JOBS c1, c2"""

        def do_GET(self) -> None:
            """@spec SRE-SCHEDULED-JOBS c1, c2"""
            body = metrics.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            """@spec SRE-SCHEDULED-JOBS c4"""

    server = ThreadingHTTPServer(("0.0.0.0", 0), Metrics)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    container_id = None
    try:
        with tempfile.TemporaryDirectory(prefix="scheduled-job-prometheus-") as temporary:
            directory = pathlib.Path(temporary)
            directory.chmod(0o755)
            (directory / "alerts.json").write_text(json.dumps({"groups": [group]}))
            config = {
                "global": {"scrape_interval": "1s", "evaluation_interval": "1s"},
                "rule_files": ["/verification/alerts.json"],
                "scrape_configs": [
                    {
                        "job_name": "verification",
                        "static_configs": [
                            {"targets": [f"host.docker.internal:{server.server_port}"]}
                        ],
                    }
                ],
            }
            (directory / "prometheus.json").write_text(json.dumps(config))
            container_id = subprocess.check_output(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--label",
                    "curie.verification=scheduled-job-alerts",
                    "--publish",
                    "127.0.0.1::9090",
                    "--add-host",
                    "host.docker.internal:host-gateway",
                    "--volume",
                    f"{directory}:/verification:ro",
                    "prom/prometheus:v3.5.0",
                    "--config.file=/verification/prometheus.json",
                ],
                text=True,
            ).strip()
            print("owned Prometheus container:", container_id, flush=True)
            port = subprocess.check_output(["docker", "port", container_id, "9090/tcp"], text=True)
            base = "http://" + port.strip()

            def query(path: str) -> dict:
                """@spec SRE-SCHEDULED-JOBS c1, c2, c5"""
                with urllib.request.urlopen(base + path, timeout=3) as response:
                    return json.load(response)

            deadline = time.monotonic() + 90
            suspension_fired = False
            while time.monotonic() < deadline:
                try:
                    rules = query("/api/v1/rules")["data"]["groups"][0]["rules"]
                    assert all(not rule.get("lastError") for rule in rules), rules
                    results = [
                        query("/api/v1/query?" + urllib.parse.urlencode({"query": rule["expr"]}))[
                            "data"
                        ]["result"]
                        for rule in group["rules"]
                    ]
                    jobs = [{item["metric"]["cronjob"] for item in result} for result in results]
                    if jobs != [{"suspended"}, {"failing", "never-success"}]:
                        time.sleep(1)
                        continue
                    suspension = next(
                        rule for rule in rules if rule["name"] == "CurieScheduledJobSuspended"
                    )
                    if any(alert["state"] == "firing" for alert in suspension["alerts"]):
                        suspension_fired = True
                        print(
                            "Prometheus: suspended firing; failed and never-success selected; "
                            "healthy and other namespace excluded"
                        )
                        break
                except (OSError, KeyError, IndexError):
                    pass
                time.sleep(1)
            assert suspension_fired, (
                "Prometheus did not fire the generated suspension rule within 90 seconds"
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if container_id:
            label = subprocess.check_output(
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{{index .Config.Labels "curie.verification"}}',
                    container_id,
                ],
                text=True,
            ).strip()
            assert label == "scheduled-job-alerts", "refusing cleanup of an unowned container"
            subprocess.run(
                ["docker", "rm", "--force", container_id], check=True, capture_output=True
            )
            remaining = subprocess.check_output(
                ["docker", "ps", "--all", "--quiet", "--filter", f"id={container_id}"], text=True
            )
            assert not remaining.strip(), "owned container remains after cleanup"
            print("owned Prometheus container removed")


if __name__ == "__main__":
    run()
